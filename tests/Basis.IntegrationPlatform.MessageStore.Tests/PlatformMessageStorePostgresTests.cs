using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Microsoft.Extensions.DependencyInjection;

namespace Basis.IntegrationPlatform.MessageStore.Tests;

[TestClass]
public sealed class PlatformMessageStorePostgresTests
{
    private static string RequireConnectionString()
    {
        var connectionString = Environment.GetEnvironmentVariable("POSTGRES_TEST_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Assert.Inconclusive("Set POSTGRES_TEST_CONNECTION_STRING to run the Postgres adapter schema tests.");
        }

        return connectionString!;
    }

    private static PlatformMessageStoreOptions CreateOptions(string connectionString)
    {
        var options = new PlatformMessageStoreOptions
        {
            ApplicationName = "integration-e2e-dotnet",
            EnvironmentName = "dev",
            Tenant = "demo",
            KafkaBootstrapServers = "kafka:9092",
            InputTopic = "bip.integration-e2e-dotnet.in",
            DlqTopic = "bip.message.dlq",
            ReplayInputTopicPattern = "bip.%s.in",
            PayloadMode = PlatformMessageStoreOptions.PayloadModePostgresInline,
            PayloadMaxBytes = 1_048_576,
            PayloadRetentionDays = 30,
            PostgresConnectionString = connectionString,
        };
        options.Validate();
        return options;
    }

    private static PlatformMessage CreateMessage(string messageId, PlatformMessageStoreOptions options, IReadOnlyDictionary<string, string>? headers = null, byte[]? payload = null)
    {
        return new PlatformMessage(
            messageId,
            messageId,
            options.ApplicationName,
            options.EnvironmentName,
            options.Tenant,
            "test-source",
            options.ApplicationName,
            "application/json",
            payload ?? """{"messageType":"e2e.order","runtime":"dotnet","outcome":"SUCCESS"}"""u8.ToArray(),
            headers ?? new Dictionary<string, string>());
    }

    private static (PlatformMessageStore Store, PlatformStepExecutor Steps, PlatformStepObservationCollector Observations, LogRecorder Logs) CreateStore(
        NpgsqlDataSource dataSource, PlatformMessageStoreOptions options)
    {
        var observations = new PlatformStepObservationCollector();
        var logs = new LogRecorder();
        var store = new PlatformMessageStore(
            dataSource,
            observations,
            Options.Create(options),
            logs.For<PlatformMessageStore>());
        var steps = new PlatformStepExecutor(dataSource, observations, NullLogger<PlatformStepExecutor>.Instance);
        return (store, steps, observations, logs);
    }

    [TestMethod]
    public async Task BusinessDatasourceDoesNotReplaceAdapterJournalDatasource()
    {
        var connectionString = RequireConnectionString();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddBasisMessageStoreAdapter<SyntheticHandler>(options =>
        {
            options.PostgresConnectionString = connectionString;
            options.KafkaBootstrapServers = "unused:9092";
        });
        // This deliberately unusable application datasource must never be used
        // by either adapter persistence service.
        var business = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=business;Username=synthetic;Timeout=1");
        services.AddSingleton(business);
        await using var provider = services.BuildServiceProvider();
        Assert.AreSame(business, provider.GetRequiredService<NpgsqlDataSource>());
        var options = CreateOptions(connectionString);
        var message = CreateMessage("datasource-test-"+Guid.NewGuid().ToString("N"), options);
        var steps = provider.GetRequiredService<IPlatformStepExecutor>();
        var step = await steps.ExecuteAsync(message, "synthetic", "https://example.test/action",
            options.ApplicationName, _ => Task.CompletedTask, CancellationToken.None);
        Assert.AreEqual("DELIVERED", step.Status);
        await provider.GetRequiredService<PlatformMessageStore>().RecordAttemptAsync(
            message, "synthetic", "COMPLETED", null, CancellationToken.None);
        await using var connection = await provider.GetRequiredKeyedService<NpgsqlDataSource>(
            PlatformMessageStoreExtensions.MessageStoreDataSourceKey).OpenConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT status FROM bip_message_log WHERE message_id=@id", connection);
        command.Parameters.AddWithValue("id", message.MessageId);
        Assert.AreEqual("COMPLETED", await command.ExecuteScalarAsync());
    }

    [TestMethod]
    public void RegisteringTheAdapterAddsNothingToTheHttpClientPipeline()
    {
        static string[] HttpClientRegistrations(bool withAdapter)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddHttpClient("target-system");
            if (withAdapter)
            {
                services.AddBasisMessageStoreAdapter<SyntheticHandler>(options => options.KafkaBootstrapServers = "unused:9092");
            }

            return services
                .Where(service => service.ServiceType == typeof(Microsoft.Extensions.Http.IHttpMessageHandlerBuilderFilter)
                    || service.ServiceType == typeof(IConfigureOptions<Microsoft.Extensions.Http.HttpClientFactoryOptions>)
                    || service.ServiceType == typeof(IPostConfigureOptions<Microsoft.Extensions.Http.HttpClientFactoryOptions>)
                    || service.ServiceType == typeof(DelegatingHandler))
                .Select(service => $"{service.ServiceType.Name} {service.ImplementationType?.FullName ?? service.ImplementationInstance?.GetType().FullName ?? "factory"}")
                .ToArray();
        }

        CollectionAssert.AreEqual(HttpClientRegistrations(withAdapter: false), HttpClientRegistrations(withAdapter: true));
    }

    [TestMethod]
    public async Task ConsumedCredentialHeadersReachNoHttpReceiver()
    {
        var connectionString = RequireConnectionString();
        var receiver = new RecordingReceiver();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHttpClient("target-system", client => client.BaseAddress = new Uri("https://target.example/"))
            .ConfigurePrimaryHttpMessageHandler(() => receiver);
        services.AddBasisMessageStoreAdapter<ReadmePatternHandler>(options =>
        {
            options.PostgresConnectionString = connectionString;
            options.KafkaBootstrapServers = "unused:9092";
            options.ApplicationName = "integration-e2e-dotnet";
            options.InputTopic = "bip.integration-e2e-dotnet.in";
        });
        await using var provider = services.BuildServiceProvider();
        var consumer = provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>().OfType<PlatformMessageConsumerService>().Single();
        var options = provider.GetRequiredService<IOptions<PlatformMessageStoreOptions>>().Value;
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";
        var consumed = new Message<string, byte[]>
        {
            Key = messageId,
            Value = "{}"u8.ToArray(),
            Headers = new Headers
            {
                { "x-bip-message-id", Encoding.UTF8.GetBytes(messageId) },
                { "Authorization", Encoding.UTF8.GetBytes("Bearer synthetic-consumed-token") },
                { "x-api-key", Encoding.UTF8.GetBytes("synthetic-consumed-key") },
                { "Cookie", Encoding.UTF8.GetBytes("session=synthetic-consumed-session") },
                { "x-custom-trace", Encoding.UTF8.GetBytes("synthetic-consumed-trace") },
            },
        };

        await consumer.ProcessAsync(PlatformMessageConsumerService.ToPlatformMessage(consumed, options), () => { }, CancellationToken.None);

        var request = receiver.Requests.Single();
        var consumedNames = consumed.Headers.Select(header => header.Key).ToHashSet(StringComparer.OrdinalIgnoreCase);
        Assert.IsFalse(request.Headers.Any(header => consumedNames.Contains(header.Name)), string.Join(", ", request.Headers.Select(header => header.Name)));
        Assert.IsFalse(request.Headers.Any(header => header.Value.Contains("synthetic-consumed", StringComparison.Ordinal)));
        Assert.IsFalse(request.Uri.Contains("synthetic-consumed", StringComparison.Ordinal));
        Assert.AreEqual("COMPLETED", await LogStatusAsync(provider.GetRequiredKeyedService<NpgsqlDataSource>(PlatformMessageStoreExtensions.MessageStoreDataSourceKey), messageId));
    }

    [TestMethod]
    public async Task ReplayRecordCarriesOnlyTheEnvelopeHeaders()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, _, _) = CreateStore(dataSource, options);
        var consumed = new Message<string, byte[]>
        {
            Key = messageId,
            Value = "{}"u8.ToArray(),
            Headers = new Headers
            {
                { "x-bip-message-id", Encoding.UTF8.GetBytes(messageId) },
                { "x-bip-correlation-id", Encoding.UTF8.GetBytes(messageId) },
                { "Authorization", Encoding.UTF8.GetBytes("Bearer synthetic-consumed-token") },
                { "x-api-key", Encoding.UTF8.GetBytes("synthetic-consumed-key") },
                { "x-custom-trace", Encoding.UTF8.GetBytes("synthetic-consumed-trace") },
                { "x-bip-unknown", Encoding.UTF8.GetBytes("synthetic-consumed-envelope") },
            },
        };
        await store.RecordAttemptAsync(PlatformMessageConsumerService.ToPlatformMessage(consumed, options), "dotnet-handler", "COMPLETED", null, CancellationToken.None);
        var producer = new FakeProducer();

        var replay = await store.ReplayAsync(messageId, new MessageReplayRequest("operator", "resend"), producer, CancellationToken.None);

        Assert.AreEqual("PUBLISHED", replay.Status);
        var headers = producer.Produced.Single().Message.Headers;
        CollectionAssert.AreEquivalent(
            new[]
            {
                "x-bip-message-id", "x-bip-correlation-id", "x-bip-schema-version", "x-bip-content-type", "content-type",
                "x-bip-source-system", "x-bip-target-system", "x-bip-replay", "x-bip-attempt", "x-bip-redo-steps",
            },
            headers.Select(header => header.Key).ToArray());
        Assert.IsFalse(headers.Any(header => Encoding.UTF8.GetString(header.GetValueBytes()).Contains("synthetic-consumed", StringComparison.Ordinal)));
    }

    /// <summary>The README's handler: one step around a named client's call.</summary>
    private sealed class ReadmePatternHandler(IPlatformStepExecutor steps, IHttpClientFactory httpClients) : IPlatformMessageHandler
    {
        public async Task<PlatformMessageResult> HandleAsync(PlatformMessage message, CancellationToken cancellationToken)
        {
            await steps.ExecuteAsync(
                message,
                stepId: "target-system",
                endpointUri: "https://target.example/api/orders",
                targetRef: message.TargetSystem,
                operation: async ct =>
                {
                    using var content = new ByteArrayContent(message.Payload);
                    content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse(message.ContentType);
                    using var response = await httpClients.CreateClient("target-system").PostAsync("api/orders", content, ct);
                    response.EnsureSuccessStatusCode();
                },
                cancellationToken);
            return PlatformMessageResult.Completed();
        }
    }

    private sealed class RecordingReceiver : HttpMessageHandler
    {
        public List<(string Uri, List<(string Name, string Value)> Headers)> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var headers = request.Headers.Concat(request.Content?.Headers ?? Enumerable.Empty<KeyValuePair<string, IEnumerable<string>>>())
                .SelectMany(header => header.Value.Select(value => (header.Key, value)))
                .ToList();
            Requests.Add((request.RequestUri!.ToString(), headers));
            return Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.OK));
        }
    }

    private sealed class SyntheticHandler : IPlatformMessageHandler
    {
        public Task<PlatformMessageResult> HandleAsync(PlatformMessage message, CancellationToken cancellationToken)
            => Task.FromResult(PlatformMessageResult.Completed());
    }

    [TestMethod]
    public async Task RecordAttemptWritesAdapterV1Schema()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";

        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, steps, _, _) = CreateStore(dataSource, options);

        var message = CreateMessage(messageId, options);

        var delivered = await steps.ExecuteAsync(
            message,
            "target-system",
            "https://target.example/api/orders",
            options.ApplicationName,
            _ => Task.CompletedTask,
            CancellationToken.None);
        Assert.AreEqual("DELIVERED", delivered.Status);

        var skippedOperationRan = false;
        var skipped = await steps.ExecuteAsync(
            message,
            "target-system",
            "https://target.example/api/orders",
            options.ApplicationName,
            _ =>
            {
                skippedOperationRan = true;
                return Task.CompletedTask;
            },
            CancellationToken.None);
        Assert.AreEqual("SKIPPED", skipped.Status);
        Assert.IsTrue(skipped.Skipped);
        Assert.IsFalse(skippedOperationRan);

        await store.RecordAttemptAsync(
            message,
            new PlatformMessageAttemptMetadata("dotnet-handler", true, 1, 2),
            "COMPLETED",
            null,
            CancellationToken.None);

        var failedMessageId = $"dotnet-test-{Guid.NewGuid():N}";
        var failedMessage = message with
        {
            MessageId = failedMessageId,
            CorrelationId = failedMessageId,
            Payload = """{"messageType":"e2e.order","runtime":"dotnet","outcome":"ERROR"}"""u8.ToArray(),
        };

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => steps.ExecuteAsync(
            failedMessage,
            "target-system",
            "https://target.example/api/orders",
            options.ApplicationName,
            _ => throw new InvalidOperationException("simulated step failure"),
            CancellationToken.None));

        await store.RecordAttemptAsync(failedMessage, "dotnet-handler", "DLQ", "simulated failure", CancellationToken.None);

        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            """
            select status, schema_version, integration, payload_storage, replayable, error,
                   auto_redelivered, auto_redelivery_count, auto_redelivery_max,
                   payload_expires_at, replay_disabled_reason
            from bip_message_log
            where message_id = @message_id
            """,
            connection);
        command.Parameters.AddWithValue("message_id", messageId);

        await using var reader = await command.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync(), "Expected bip_message_log row to be written.");
        Assert.AreEqual("COMPLETED", reader.GetString(0));
        Assert.AreEqual(1, reader.GetInt32(1));
        Assert.AreEqual(options.ApplicationName, reader.GetString(2));
        Assert.AreEqual("postgres_inline", reader.GetString(3));
        // Replayable follows payload availability, not status.
        Assert.IsTrue(reader.GetBoolean(4), "COMPLETED message with captured payload must be replayable.");
        Assert.IsTrue(reader.IsDBNull(5));
        Assert.IsTrue(reader.GetBoolean(6));
        Assert.AreEqual(1, reader.GetInt32(7));
        Assert.AreEqual(2, reader.GetInt32(8));
        Assert.IsFalse(reader.IsDBNull(9), "Captured payload must have payload_expires_at set.");
        Assert.IsTrue(reader.IsDBNull(10));
        await reader.DisposeAsync();

        await using var attemptCommand = new NpgsqlCommand(
            """
            select status,
                   auto_redelivered,
                   auto_redelivery_count,
                   auto_redelivery_max,
                   jsonb_array_length(steps_json),
                   steps_json->0->>'step',
                   steps_json->0->>'status',
                   steps_json->0->>'idempotencyKey',
                   steps_json->0->>'endpointUri',
                   steps_json->0->>'targetRef',
                   steps_json->0->>'occurredAt',
                   steps_json->1->>'status',
                   steps_json->1->>'idempotencyKey',
                   correlation_id,
                   content_type,
                   schema_version
            from bip_message_attempt
            where message_id = @message_id and attempt = 1
            """,
            connection);
        attemptCommand.Parameters.AddWithValue("message_id", messageId);

        await using var attemptReader = await attemptCommand.ExecuteReaderAsync();
        Assert.IsTrue(await attemptReader.ReadAsync(), "Expected bip_message_attempt row to be written.");
        Assert.AreEqual("COMPLETED", attemptReader.GetString(0));
        Assert.IsTrue(attemptReader.GetBoolean(1));
        Assert.AreEqual(1, attemptReader.GetInt32(2));
        Assert.AreEqual(2, attemptReader.GetInt32(3));
        // steps_json records what THIS attempt observed, including
        // the dedup-skipped second execution.
        Assert.AreEqual(2, attemptReader.GetInt32(4));
        Assert.AreEqual("target-system", attemptReader.GetString(5));
        Assert.AreEqual("DELIVERED", attemptReader.GetString(6));
        Assert.AreEqual($"{messageId}:target-system", attemptReader.GetString(7));
        Assert.AreEqual("https://target.example/api/orders", attemptReader.GetString(8));
        Assert.AreEqual(options.ApplicationName, attemptReader.GetString(9));
        Assert.IsFalse(attemptReader.IsDBNull(10), "occurredAt must be recorded on step observations.");
        Assert.AreEqual("SKIPPED", attemptReader.GetString(11));
        Assert.AreEqual($"{messageId}:target-system", attemptReader.GetString(12));
        // Cross-runtime parity: the Camel adapter writes correlation_id and
        // content_type on attempt rows; the .NET adapter must match.
        Assert.AreEqual(messageId, attemptReader.GetString(13));
        Assert.AreEqual("application/json", attemptReader.GetString(14));
        Assert.AreEqual(1, attemptReader.GetInt32(15));
        await attemptReader.DisposeAsync();

        await using var failedCommand = new NpgsqlCommand(
            """
            select log.status, log.schema_version, log.integration, log.payload_storage, log.replayable, log.error,
                   jsonb_array_length(attempt.steps_json), attempt.steps_json->0->>'status', attempt.steps_json->0->>'error'
            from bip_message_log log
            join bip_message_attempt attempt using (message_id)
            where message_id = @message_id and attempt.attempt = 1
            """,
            connection);
        failedCommand.Parameters.AddWithValue("message_id", failedMessageId);

        await using var failedReader = await failedCommand.ExecuteReaderAsync();
        Assert.IsTrue(await failedReader.ReadAsync(), "Expected failed bip_message_log row to be written.");
        Assert.AreEqual("DLQ", failedReader.GetString(0));
        Assert.AreEqual(1, failedReader.GetInt32(1));
        Assert.AreEqual(options.ApplicationName, failedReader.GetString(2));
        Assert.AreEqual("postgres_inline", failedReader.GetString(3));
        Assert.IsTrue(failedReader.GetBoolean(4), "DLQ message with captured payload must be replayable.");
        Assert.AreEqual("simulated failure", failedReader.GetString(5));
        Assert.AreEqual(1, failedReader.GetInt32(6));
        Assert.AreEqual("FAILED", failedReader.GetString(7));
        Assert.AreEqual("simulated step failure", failedReader.GetString(8));
        await failedReader.DisposeAsync();

        await using var failedStepCommand = new NpgsqlCommand(
            """
            select status, error
            from bip_step_outcome
            where idempotency_key = @idempotency_key
            """,
            connection);
        failedStepCommand.Parameters.AddWithValue("idempotency_key", $"{failedMessageId}:target-system");

        await using var failedStepReader = await failedStepCommand.ExecuteReaderAsync();
        Assert.IsTrue(await failedStepReader.ReadAsync(), "Expected failed bip_step_outcome row to be written.");
        Assert.AreEqual("FAILED", failedStepReader.GetString(0));
        Assert.AreEqual("simulated step failure", failedStepReader.GetString(1));
    }

    [TestMethod]
    public async Task AttemptNumberComesFromHeaderAndRedeliveryUpserts()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";

        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, _, _) = CreateStore(dataSource, options);

        // The attempt number comes from x-bip-attempt, never the DB.
        var replayDelivery = CreateMessage(messageId, options, new Dictionary<string, string>
        {
            ["x-bip-attempt"] = "3",
            ["x-bip-replay"] = "true",
        });

        await store.RecordAttemptAsync(replayDelivery, "dotnet-handler", "DLQ", "still failing", CancellationToken.None);
        // Infra redelivery of the same delivery re-records the SAME attempt row.
        await store.RecordAttemptAsync(replayDelivery, "dotnet-handler", "COMPLETED", null, CancellationToken.None);

        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            """
            select count(*)::int,
                   min(attempt),
                   max(attempt),
                   (select status from bip_message_attempt where message_id = @message_id and attempt = 3),
                   (select last_attempt from bip_message_log where message_id = @message_id),
                   (select status from bip_message_log where message_id = @message_id)
            from bip_message_attempt
            where message_id = @message_id
            """,
            connection);
        command.Parameters.AddWithValue("message_id", messageId);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(1, reader.GetInt32(0), "Redelivery must upsert the same (message_id, attempt) row.");
        Assert.AreEqual(3, reader.GetInt32(1));
        Assert.AreEqual(3, reader.GetInt32(2));
        Assert.AreEqual("COMPLETED", reader.GetString(3));
        Assert.AreEqual(3, reader.GetInt32(4));
        Assert.AreEqual("COMPLETED", reader.GetString(5));
    }

    [TestMethod]
    public async Task OlderAttemptCannotOverwriteAnySummaryColumnButLatestRedeliveryCan()
    {
        var options = CreateOptions(RequireConnectionString());
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, _, _) = CreateStore(dataSource, options);
        var first = CreateMessage($"dotnet-test-{Guid.NewGuid():N}", options, payload: "first"u8.ToArray());
        await store.RecordAttemptAsync(first, "first-route", "FAILED", "first error", CancellationToken.None);
        var latest = first with
        {
            Headers = new Dictionary<string, string> { ["x-bip-attempt"] = "2" },
            Payload = "latest payload"u8.ToArray(),
            SchemaVersion = 2,
        };
        var latestMetadata = new PlatformMessageAttemptMetadata("latest-route", true, 2, 3);
        await store.RecordAttemptAsync(latest, latestMetadata, "DLQ", "latest error", CancellationToken.None);

        await using var connection = await dataSource.OpenConnectionAsync();
        await using var summary = new NpgsqlCommand(
            "select to_jsonb(l)::text from bip_message_log l where message_id = @id", connection);
        summary.Parameters.AddWithValue("id", first.MessageId);
        var before = (string)(await summary.ExecuteScalarAsync())!;
        using var snapshot = JsonDocument.Parse(before);
        Assert.AreEqual(2, snapshot.RootElement.GetProperty("last_attempt").GetInt32());
        Assert.AreEqual("DLQ", snapshot.RootElement.GetProperty("status").GetString());
        Assert.AreEqual("latest error", snapshot.RootElement.GetProperty("error").GetString());
        Assert.AreEqual("\\x" + Convert.ToHexString(latest.Payload).ToLowerInvariant(),
            snapshot.RootElement.GetProperty("payload_bytes").GetString());
        Assert.AreEqual(JsonValueKind.String, snapshot.RootElement.GetProperty("payload_expires_at").ValueKind);

        // Even a delivery with a different capture policy must leave EVERY
        // summary column, including updated_at and replay state, untouched.
        options.PayloadMode = PlatformMessageStoreOptions.PayloadModeMetadataOnly;
        await store.RecordAttemptAsync(first with { SchemaVersion = 3 }, "late-route", "COMPLETED", null, CancellationToken.None);
        Assert.AreEqual(before, await summary.ExecuteScalarAsync());
        await using (var attempts = new NpgsqlCommand(
            """
            select attempt, status, route_id, schema_version, payload_storage, error
            from bip_message_attempt where message_id = @id order by attempt
            """, connection))
        {
            attempts.Parameters.AddWithValue("id", first.MessageId);
            await using var reader = await attempts.ExecuteReaderAsync();
            Assert.IsTrue(await reader.ReadAsync());
            Assert.AreEqual(1, reader.GetInt32(0));
            Assert.AreEqual("COMPLETED", reader.GetString(1));
            Assert.AreEqual("late-route", reader.GetString(2));
            Assert.AreEqual(3, reader.GetInt32(3));
            Assert.AreEqual("metadata_only", reader.GetString(4));
            Assert.IsTrue(reader.IsDBNull(5));
            Assert.IsTrue(await reader.ReadAsync());
            Assert.AreEqual(2, reader.GetInt32(0));
            Assert.AreEqual("DLQ", reader.GetString(1));
            Assert.AreEqual("latest-route", reader.GetString(2));
            Assert.IsFalse(await reader.ReadAsync());
        }

        options.PayloadMode = PlatformMessageStoreOptions.PayloadModePostgresInline;
        await store.RecordAttemptAsync(latest with { Payload = "redelivered"u8.ToArray(), SchemaVersion = 4 },
            "latest-route", "COMPLETED", null, CancellationToken.None);
        await using var updated = new NpgsqlCommand(
            """
            select last_attempt, l.status, l.error, l.schema_version, l.payload_bytes,
                   l.payload_size_bytes, l.replayable, l.replay_disabled_reason,
                   l.auto_redelivered, l.auto_redelivery_count, l.auto_redelivery_max,
                   l.payload_expires_at = a.payload_expires_at
            from bip_message_log l join bip_message_attempt a using (message_id)
            where message_id = @id and a.attempt = 2
            """, connection);
        updated.Parameters.AddWithValue("id", first.MessageId);
        await using var updatedReader = await updated.ExecuteReaderAsync();
        Assert.IsTrue(await updatedReader.ReadAsync());
        Assert.AreEqual(2, updatedReader.GetInt32(0));
        Assert.AreEqual("COMPLETED", updatedReader.GetString(1));
        Assert.IsTrue(updatedReader.IsDBNull(2));
        Assert.AreEqual(4, updatedReader.GetInt32(3));
        CollectionAssert.AreEqual("redelivered"u8.ToArray(), (byte[])updatedReader[4]);
        Assert.AreEqual(11L, updatedReader.GetInt64(5));
        Assert.IsTrue(updatedReader.GetBoolean(6));
        Assert.IsTrue(updatedReader.IsDBNull(7));
        Assert.IsFalse(updatedReader.GetBoolean(8));
        Assert.IsTrue(updatedReader.IsDBNull(9));
        Assert.IsTrue(updatedReader.IsDBNull(10));
        Assert.IsTrue(updatedReader.GetBoolean(11));
    }

    [TestMethod]
    public async Task ReplayRecordIsKeyedByTheCorrelationId()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, _, _) = CreateStore(dataSource, options);
        var producer = new FakeProducer();
        var withHeader = $"dotnet-test-{Guid.NewGuid():N}";
        var withoutHeader = $"dotnet-test-{Guid.NewGuid():N}";
        var withoutKey = $"dotnet-test-{Guid.NewGuid():N}";
        foreach (var (messageId, key, correlationId) in new[]
        {
            (withHeader, "record-key-1", "correlation-1"),
            (withoutHeader, "record-key-2", null),
            (withoutKey, null, null),
        })
        {
            var headers = new Headers { { "x-bip-message-id", Encoding.UTF8.GetBytes(messageId) } };
            if (correlationId is not null)
            {
                headers.Add("x-bip-correlation-id", Encoding.UTF8.GetBytes(correlationId));
            }

            var consumed = new Message<string, byte[]> { Key = key!, Value = "{}"u8.ToArray(), Headers = headers };
            await store.RecordAttemptAsync(
                PlatformMessageConsumerService.ToPlatformMessage(consumed, options), "dotnet-handler", "COMPLETED", null, CancellationToken.None);
            Assert.AreEqual("PUBLISHED", (await store.ReplayAsync(messageId, new MessageReplayRequest("operator", "resend"), producer, CancellationToken.None)).Status);
        }

        // x-bip-correlation-id, else the original record's key, else the message id.
        CollectionAssert.AreEqual(
            new[] { "correlation-1", "record-key-2", withoutKey },
            producer.Produced.Select(produced => produced.Message.Key).ToArray());
        // The replay topic is the pattern with the integration name.
        Assert.IsTrue(producer.Produced.All(produced => produced.Topic == "bip.integration-e2e-dotnet.in"));
    }

    [TestMethod]
    public async Task SchemaVersionFromProducerIsStoredAndReemittedOnReplay()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";

        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, _, _) = CreateStore(dataSource, options);

        // End to end: producer declares x-bip-schema-version=7 ⇒ the
        // store persists 7 in both tables ⇒ replay re-emits the stored value.
        var kafkaMessage = new Message<string, byte[]>
        {
            Key = messageId,
            Value = """{"messageType":"e2e.order","runtime":"dotnet","outcome":"SUCCESS"}"""u8.ToArray(),
            Headers = new Headers
            {
                { "x-bip-message-id", Encoding.UTF8.GetBytes(messageId) },
                { "x-bip-correlation-id", Encoding.UTF8.GetBytes(messageId) },
                { "x-bip-schema-version", Encoding.UTF8.GetBytes("7") },
                { "x-bip-content-type", Encoding.UTF8.GetBytes("application/json") },
                { "x-bip-source-system", Encoding.UTF8.GetBytes("test-source") },
            },
        };
        var message = PlatformMessageConsumerService.ToPlatformMessage(kafkaMessage, options);
        Assert.AreEqual(7, message.SchemaVersion);

        await store.RecordAttemptAsync(message, "dotnet-handler", "COMPLETED", null, CancellationToken.None);

        await using (var connection = await dataSource.OpenConnectionAsync())
        {
            await using var command = new NpgsqlCommand(
                """
                select log.schema_version, attempt.schema_version
                from bip_message_log log
                join bip_message_attempt attempt using (message_id)
                where message_id = @message_id and attempt.attempt = 1
                """,
                connection);
            command.Parameters.AddWithValue("message_id", messageId);
            await using var reader = await command.ExecuteReaderAsync();
            Assert.IsTrue(await reader.ReadAsync(), "Expected message rows to be written.");
            Assert.AreEqual(7, reader.GetInt32(0), "bip_message_log.schema_version must capture x-bip-schema-version.");
            Assert.AreEqual(7, reader.GetInt32(1), "bip_message_attempt.schema_version must capture x-bip-schema-version.");
        }

        var producer = new FakeProducer();
        var result = await store.ReplayAsync(messageId, new MessageReplayRequest("operator", "re-run"), producer, CancellationToken.None);

        Assert.AreEqual("PUBLISHED", result.Status);
        Assert.AreEqual(1, producer.Produced.Count);
        var headers = PlatformMessageConsumerService.ReadHeaders(producer.Produced[0].Message.Headers);
        Assert.AreEqual("7", headers["x-bip-schema-version"], "Replay must re-emit the stored schema version.");
    }

    [TestMethod]
    public async Task DlqPublishFailureLeavesNoDlqRowAndNoCommitUntilRetrySucceeds()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        options.MaxHandlerAttempts = 2;
        options.HandlerRetryDelayMs = 0;
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";

        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var observations = new PlatformStepObservationCollector();
        var store = new PlatformMessageStore(
            dataSource,
            observations,
            Options.Create(options),
            NullLogger<PlatformMessageStore>.Instance);

        var failDlqPublish = true;
        var producer = new FakeProducer((_, _) =>
        {
            if (failDlqPublish)
            {
                throw new KafkaException(new Error(ErrorCode.Local_AllBrokersDown, "dlq broker down"));
            }

            return Task.CompletedTask;
        });
        var handler = new RecordingFailingHandler();
        var service = new PlatformMessageConsumerService(
            handler,
            store,
            observations,
            producer,
            Options.Create(options),
            NullLogger<PlatformMessageConsumerService>.Instance);

        var message = CreateMessage(messageId, options);
        var commits = 0;

        // Publish to DLQ first, then record DLQ status, then commit.
        // A failed DLQ publish must leave NO DLQ status row and NO committed offset.
        await Assert.ThrowsExactlyAsync<KafkaException>(() => service.ProcessAsync(message, () => commits++, CancellationToken.None));
        Assert.AreEqual(0, commits, "The offset must not be committed when the DLQ publish fails.");
        Assert.AreEqual(0, producer.Produced.Count);
        Assert.AreEqual(0, await CountRowsAsync(dataSource, "bip_message_log", messageId), "No status row may exist until the DLQ publish succeeds.");
        Assert.AreEqual(0, await CountRowsAsync(dataSource, "bip_message_attempt", messageId), "No attempt row may exist until the DLQ publish succeeds.");

        // The seek/redelivery path re-processes the same delivery; once the DLQ
        // topic is back the message is parked, recorded and the offset committed.
        failDlqPublish = false;
        await service.ProcessAsync(message, () => commits++, CancellationToken.None);

        Assert.AreEqual(1, commits, "The offset must be committed after the DLQ publish and DLQ row succeed.");
        Assert.AreEqual(1, producer.Produced.Count);
        var (topic, produced) = producer.Produced[0];
        Assert.AreEqual(options.DlqTopic, topic);
        var dlqHeaders = PlatformMessageConsumerService.ReadHeaders(produced.Headers);
        Assert.AreEqual("simulated handler failure", dlqHeaders["x-bip-error"]);
        Assert.AreEqual(messageId, dlqHeaders["x-bip-message-id"]);
        // No invented wire headers leak onto DLQ records — in-process retry
        // diagnostics belong to the auto_redelivery_* columns only.
        Assert.IsFalse(dlqHeaders.ContainsKey("x-bip-redelivered"), "x-bip-redelivered must not leak onto DLQ records.");
        Assert.IsFalse(dlqHeaders.ContainsKey("x-bip-redelivery-count"), "x-bip-redelivery-count must not leak onto DLQ records.");

        // Two ProcessAsync calls x MaxHandlerAttempts=2 in-process attempts,
        // each seeing the consumed message without invented retry headers.
        Assert.AreEqual(4, handler.SeenHeaderSnapshots.Count);
        Assert.IsTrue(
            handler.SeenHeaderSnapshots.All(headers =>
                !headers.ContainsKey("x-bip-redelivered") && !headers.ContainsKey("x-bip-redelivery-count")),
            "The handler must never observe invented redelivery wire headers.");

        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            """
            select log.status, log.auto_redelivered, log.auto_redelivery_count, log.auto_redelivery_max,
                   attempt.status, attempt.auto_redelivered, attempt.auto_redelivery_count, attempt.auto_redelivery_max
            from bip_message_log log
            join bip_message_attempt attempt using (message_id)
            where message_id = @message_id and attempt.attempt = 1
            """,
            connection);
        command.Parameters.AddWithValue("message_id", messageId);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync(), "Expected the DLQ outcome rows after the successful retry.");
        Assert.AreEqual("DLQ", reader.GetString(0));
        // Retry diagnostics keep flowing into the columns (derived from the
        // retry loop state, not from headers).
        Assert.IsTrue(reader.GetBoolean(1));
        Assert.AreEqual(1, reader.GetInt32(2));
        Assert.AreEqual(1, reader.GetInt32(3));
        Assert.AreEqual("DLQ", reader.GetString(4));
        Assert.IsTrue(reader.GetBoolean(5));
        Assert.AreEqual(1, reader.GetInt32(6));
        Assert.AreEqual(1, reader.GetInt32(7));
    }

    [TestMethod]
    public async Task PayloadOverCapDegradesToMetadataOnly()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        options.PayloadMaxBytes = 16;
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";

        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, _, _) = CreateStore(dataSource, options);

        var payload = new byte[64];
        Array.Fill(payload, (byte)'x');
        var message = CreateMessage(messageId, options, payload: payload);
        await store.RecordAttemptAsync(message, "dotnet-handler", "COMPLETED", null, CancellationToken.None);

        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            """
            select payload_storage, payload_size_bytes, payload_bytes is null, payload_expires_at is null,
                   replayable, replay_disabled_reason, status
            from bip_message_log
            where message_id = @message_id
            """,
            connection);
        command.Parameters.AddWithValue("message_id", messageId);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual("metadata_only", reader.GetString(0));
        Assert.AreEqual(64L, reader.GetInt64(1));
        Assert.IsTrue(reader.GetBoolean(2), "metadata_only rows must store no payload bytes.");
        Assert.IsTrue(reader.GetBoolean(3), "metadata_only rows must not carry payload_expires_at.");
        Assert.IsFalse(reader.GetBoolean(4));
        Assert.AreEqual("payload exceeds max inline size", reader.GetString(5));
        Assert.AreEqual("COMPLETED", reader.GetString(6), "Over-cap payload must not fail message processing.");
    }

    [TestMethod]
    public async Task MetadataOnlyModeIsNotReplayable()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        options.PayloadMode = PlatformMessageStoreOptions.PayloadModeMetadataOnly;
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";

        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, _, _) = CreateStore(dataSource, options);

        await store.RecordAttemptAsync(CreateMessage(messageId, options), "dotnet-handler", "DLQ", "boom", CancellationToken.None);

        var producer = new FakeProducer();
        var result = await store.ReplayAsync(messageId, new MessageReplayRequest("operator", "retry"), producer, CancellationToken.None);

        Assert.AreEqual("FAILED", result.Status);
        Assert.AreEqual(MessageReplayErrorCodes.NotReplayable, result.ErrorCode);
        Assert.AreEqual("payload storage mode is metadata-only", result.Error);
        Assert.IsNull(result.ReplayId, "Non-replayable requests must be rejected before any audit row.");
        Assert.AreEqual(0, producer.Produced.Count, "Non-replayable requests must never produce.");
        Assert.AreEqual(0, await CountAuditRowsAsync(dataSource, messageId));
    }

    [TestMethod]
    public async Task ExpiredPayloadIsNotReplayable()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";

        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, _, _) = CreateStore(dataSource, options);

        await store.RecordAttemptAsync(CreateMessage(messageId, options), "dotnet-handler", "COMPLETED", null, CancellationToken.None);
        await using (var connection = await dataSource.OpenConnectionAsync())
        {
            await using var expire = new NpgsqlCommand(
                "update bip_message_log set payload_expires_at = now() - interval '1 day' where message_id = @message_id",
                connection);
            expire.Parameters.AddWithValue("message_id", messageId);
            await expire.ExecuteNonQueryAsync();
        }

        var producer = new FakeProducer();
        var result = await store.ReplayAsync(messageId, new MessageReplayRequest("operator", "retry"), producer, CancellationToken.None);

        Assert.AreEqual(MessageReplayErrorCodes.NotReplayable, result.ErrorCode);
        Assert.AreEqual("Payload has expired.", result.Error);
        Assert.AreEqual(0, producer.Produced.Count);
        Assert.AreEqual(0, await CountAuditRowsAsync(dataSource, messageId));
    }

    [TestMethod]
    public async Task ReplayUnknownMessageReturnsNotFoundWithoutAudit()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        var messageId = $"dotnet-missing-{Guid.NewGuid():N}";

        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, _, _) = CreateStore(dataSource, options);

        var producer = new FakeProducer();
        var result = await store.ReplayAsync(messageId, new MessageReplayRequest("operator", "retry"), producer, CancellationToken.None);

        Assert.AreEqual(MessageReplayErrorCodes.NotFound, result.ErrorCode);
        Assert.AreEqual(0, producer.Produced.Count);
        Assert.AreEqual(0, await CountAuditRowsAsync(dataSource, messageId));
    }

    [TestMethod]
    public async Task ReplayCommitsRequestedAuditBeforeProduceAndCompletedMessagesAreResendable()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";

        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, _, _) = CreateStore(dataSource, options);

        // COMPLETED outcome: still resendable while the payload is available.
        await store.RecordAttemptAsync(CreateMessage(messageId, options), "dotnet-handler", "COMPLETED", null, CancellationToken.None);

        string? auditStatusDuringProduce = null;
        var producer = new FakeProducer(async (topic, message) =>
        {
            // The REQUESTED audit row must already be committed and visible from
            // a separate connection while the produce is in flight.
            await using var connection = await dataSource.OpenConnectionAsync();
            await using var command = new NpgsqlCommand(
                "select status from bip_message_replay_audit where message_id = @message_id order by requested_at desc limit 1",
                connection);
            command.Parameters.AddWithValue("message_id", messageId);
            auditStatusDuringProduce = (string?)await command.ExecuteScalarAsync();
        });

        var result = await store.ReplayAsync(messageId, new MessageReplayRequest("operator", "re-run"), producer, CancellationToken.None);

        Assert.AreEqual("PUBLISHED", result.Status);
        Assert.IsNull(result.ErrorCode);
        Assert.IsNotNull(result.ReplayId);
        Assert.AreEqual(2, result.Attempt);
        Assert.AreEqual("bip.integration-e2e-dotnet.in", result.TargetTopic);
        Assert.AreEqual("REQUESTED", auditStatusDuringProduce, "Audit row must be committed as REQUESTED before the produce.");

        Assert.AreEqual(1, producer.Produced.Count);
        var (topic, produced) = producer.Produced[0];
        Assert.AreEqual("bip.integration-e2e-dotnet.in", topic);
        var headers = PlatformMessageConsumerService.ReadHeaders(produced.Headers);
        Assert.AreEqual(messageId, headers["x-bip-message-id"]);
        Assert.AreEqual("2", headers["x-bip-attempt"]);
        Assert.AreEqual("true", headers["x-bip-replay"]);

        await using var connection2 = await dataSource.OpenConnectionAsync();
        await using var auditCommand = new NpgsqlCommand(
            "select status, published_at is not null, replay_attempt, target_topic from bip_message_replay_audit where replay_id = @replay_id",
            connection2);
        auditCommand.Parameters.AddWithValue("replay_id", Guid.Parse(result.ReplayId!));
        await using var auditReader = await auditCommand.ExecuteReaderAsync();
        Assert.IsTrue(await auditReader.ReadAsync());
        Assert.AreEqual("PUBLISHED", auditReader.GetString(0));
        Assert.IsTrue(auditReader.GetBoolean(1));
        Assert.AreEqual(2, auditReader.GetInt32(2));
        Assert.AreEqual("bip.integration-e2e-dotnet.in", auditReader.GetString(3));
    }

    [TestMethod]
    public async Task ReplayPublishFailureMarksAuditFailed()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";

        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, _, _) = CreateStore(dataSource, options);

        await store.RecordAttemptAsync(CreateMessage(messageId, options), "dotnet-handler", "DLQ", "boom", CancellationToken.None);

        var producer = new FakeProducer((_, _) => throw new KafkaException(new Error(ErrorCode.Local_AllBrokersDown, "broker down")));
        var result = await store.ReplayAsync(messageId, new MessageReplayRequest("operator", "retry"), producer, CancellationToken.None);

        Assert.AreEqual("FAILED", result.Status);
        Assert.AreEqual(MessageReplayErrorCodes.PublishFailed, result.ErrorCode);
        Assert.IsNotNull(result.ReplayId, "PUBLISH_FAILED must reference the audit row written before the produce.");

        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            "select status, error from bip_message_replay_audit where replay_id = @replay_id",
            connection);
        command.Parameters.AddWithValue("replay_id", Guid.Parse(result.ReplayId!));
        await using var reader = await command.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual("FAILED", reader.GetString(0));
        StringAssert.Contains(reader.GetString(1), "broker down");
    }

    [TestMethod]
    public async Task VerifyContractFailsFastWhenGateCannotRead()
    {
        // Unreadable bip_contract (permissions/connectivity) retries transient
        // conditions, then FAILS startup - fail fast over masked errors (safe
        // under rolling updates).
        var options = Microsoft.Extensions.Options.Options.Create(new PlatformMessageStoreOptions
        {
            ApplicationName = "gate-unreadable",
            KafkaBootstrapServers = "unused:9092",
            PostgresConnectionString = "Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x;Timeout=1",
            ContractGateMaxAttempts = 2,
            ContractGateRetryDelayMs = 10,
        });
        await using var dataSource = Npgsql.NpgsqlDataSource.Create(options.Value.PostgresConnectionString);
        var store = new PlatformMessageStore(
            dataSource,
            new PlatformStepObservationCollector(),
            options,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<PlatformMessageStore>.Instance);

        var ex = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => store.VerifyContractAsync(CancellationToken.None));
        StringAssert.Contains(ex.Message, "bip_contract");
    }

    [TestMethod]
    public async Task VerifyContractPassesOnSupportedMajor()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);

        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        await using (var connection = await dataSource.OpenConnectionAsync())
        {
            await using var seed = new NpgsqlCommand(
                """
                create table if not exists bip_contract (
                    contract_major integer not null,
                    contract_version text not null,
                    applied_at timestamptz not null default current_timestamp
                );
                insert into bip_contract (contract_major, contract_version)
                select 1, '1.0.0-rc2'
                where not exists (select 1 from bip_contract where contract_major = 1);
                """,
                connection);
            await seed.ExecuteNonQueryAsync();
        }

        var (store, _, _, _) = CreateStore(dataSource, options);
        await store.VerifyContractAsync(CancellationToken.None);
    }

    [TestMethod]
    public async Task VerifyContractFailsFastOnWrongMajor()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);

        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        await using (var connection = await dataSource.OpenConnectionAsync())
        {
            await using var seed = new NpgsqlCommand(
                """
                create table if not exists bip_contract (
                    contract_major integer not null,
                    contract_version text not null,
                    applied_at timestamptz not null default current_timestamp
                );
                insert into bip_contract (contract_major, contract_version) values (2, '2.0.0-test');
                """,
                connection);
            await seed.ExecuteNonQueryAsync();
        }

        try
        {
            var (store, _, _, _) = CreateStore(dataSource, options);
            var ex = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => store.VerifyContractAsync(CancellationToken.None));
            StringAssert.Contains(ex.Message, "contract major 2");
        }
        finally
        {
            await using var connection = await dataSource.OpenConnectionAsync();
            await using var cleanup = new NpgsqlCommand("delete from bip_contract where contract_major = 2", connection);
            await cleanup.ExecuteNonQueryAsync();
        }
    }

    [TestMethod]
    public async Task VerifyContractWarnsAndContinuesWhenTableMissing()
    {
        var connectionString = RequireConnectionString();
        var schemaName = $"bip_gate_{Guid.NewGuid():N}";
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { SearchPath = schemaName };
        var options = CreateOptions(builder.ConnectionString);

        await using var adminDataSource = NpgsqlDataSource.Create(connectionString);
        await using (var connection = await adminDataSource.OpenConnectionAsync())
        {
            await using var create = new NpgsqlCommand($"create schema \"{schemaName}\"", connection);
            await create.ExecuteNonQueryAsync();
        }

        try
        {
            await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
            var (store, _, _, logs) = CreateStore(dataSource, options);
            await store.VerifyContractAsync(CancellationToken.None);
            Assert.IsTrue(
                logs.Warnings.Any(warning => warning.Contains("bip_contract", StringComparison.Ordinal)),
                "A missing bip_contract table must produce a loud warning.");
        }
        finally
        {
            await using var connection = await adminDataSource.OpenConnectionAsync();
            await using var drop = new NpgsqlCommand($"drop schema \"{schemaName}\" cascade", connection);
            await drop.ExecuteNonQueryAsync();
        }
    }

    private static async Task<int> CountAuditRowsAsync(NpgsqlDataSource dataSource, string messageId)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            "select count(*)::int from bip_message_replay_audit where message_id = @message_id",
            connection);
        command.Parameters.AddWithValue("message_id", messageId);
        return (int)(await command.ExecuteScalarAsync())!;
    }

    private static async Task<int> CountRowsAsync(NpgsqlDataSource dataSource, string table, string messageId)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            $"select count(*)::int from {table} where message_id = @message_id",
            connection);
        command.Parameters.AddWithValue("message_id", messageId);
        return (int)(await command.ExecuteScalarAsync())!;
    }

    [TestMethod]
    public async Task FanOutThroughOneStepIdSkipsLaterItemsAndLogsAnError()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var observations = new PlatformStepObservationCollector();
        var logs = new LogRecorder();
        var steps = new PlatformStepExecutor(dataSource, observations, logs.For<PlatformStepExecutor>());
        var message = CreateMessage($"dotnet-test-{Guid.NewGuid():N}", options);
        observations.BeginHandlerInvocation(message.MessageId);

        // The key is <message-id>:<step-id>, so items sent through one step id
        // share a key: the first is delivered, the rest are skipped.
        var sent = new List<string>();
        foreach (var item in new[] { "line-1", "line-2", "line-3" })
        {
            await steps.ExecuteAsync(message, "post-line", "https://target.example/lines", "target",
                _ => { sent.Add(item); return Task.CompletedTask; }, CancellationToken.None);
        }

        CollectionAssert.AreEqual(new[] { "line-1" }, sent);
        Assert.AreEqual(2, logs.Errors.Count, "One ERROR per skipped call.");
        Assert.IsTrue(logs.Errors.All(error => error.Contains("Step post-line already ran in this handler invocation", StringComparison.Ordinal)
            && error.Contains($"messageId={message.MessageId}", StringComparison.Ordinal)
            && error.Contains("distinct step id per item", StringComparison.Ordinal)));
        Assert.AreEqual(0, logs.Warnings.Count);
        CollectionAssert.AreEqual(
            new[] { "DELIVERED", "SKIPPED", "SKIPPED" },
            observations.Snapshot(message.MessageId).Select(observation => observation.Status).ToArray());

        // A step id per item delivers every item.
        sent.Clear();
        foreach (var item in new[] { "line-1", "line-2", "line-3" })
        {
            await steps.ExecuteAsync(message, $"post-line-{item}", "https://target.example/lines", "target",
                _ => { sent.Add(item); return Task.CompletedTask; }, CancellationToken.None);
        }

        CollectionAssert.AreEqual(new[] { "line-1", "line-2", "line-3" }, sent);
    }

    [TestMethod]
    public async Task InProcessRetryOfADeliveredStepDoesNotWarnAboutReuse()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var observations = new PlatformStepObservationCollector();
        var logs = new LogRecorder();
        var steps = new PlatformStepExecutor(dataSource, observations, logs.For<PlatformStepExecutor>());
        var message = CreateMessage($"dotnet-test-{Guid.NewGuid():N}", options);

        observations.BeginHandlerInvocation(message.MessageId);
        await steps.ExecuteAsync(message, "deliver", "https://target.example/orders", "target",
            _ => Task.CompletedTask, CancellationToken.None);
        // The consumer starts a new handler invocation for each in-process retry.
        observations.BeginHandlerInvocation(message.MessageId);
        var retried = await steps.ExecuteAsync(message, "deliver", "https://target.example/orders", "target",
            _ => Task.CompletedTask, CancellationToken.None);

        Assert.IsTrue(retried.Skipped);
        Assert.AreEqual(0, logs.Warnings.Count);
        Assert.AreEqual(0, logs.Errors.Count);
    }

    [TestMethod]
    public async Task ReplaySkippingAStepAnEarlierAttemptDeliveredLogsNoError()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, steps, _, _) = CreateStore(dataSource, options);
        var message = CreateMessage($"dotnet-test-{Guid.NewGuid():N}", options);
        await steps.ExecuteAsync(message, "deliver", "https://target.example/orders", "target",
            _ => Task.CompletedTask, CancellationToken.None);
        await store.RecordAttemptAsync(message, "dotnet-handler", "COMPLETED", null, CancellationToken.None);

        var observations = new PlatformStepObservationCollector();
        var logs = new LogRecorder();
        var replaySteps = new PlatformStepExecutor(dataSource, observations, logs.For<PlatformStepExecutor>());
        var replay = message with { Headers = new Dictionary<string, string> { ["x-bip-attempt"] = "2", ["x-bip-replay"] = "true" } };
        observations.BeginHandlerInvocation(replay.MessageId);
        var skipped = await replaySteps.ExecuteAsync(replay, "deliver", "https://target.example/orders", "target",
            _ => Task.CompletedTask, CancellationToken.None);

        Assert.IsTrue(skipped.Skipped);
        Assert.AreEqual(0, logs.Errors.Count);
        Assert.AreEqual(0, logs.Warnings.Count);

        // A handler that repeats the key in the replay: which item the earlier
        // attempt sent is unknown, so each repeat keeps the warning.
        for (var call = 0; call < 2; call++)
        {
            Assert.IsTrue((await replaySteps.ExecuteAsync(replay, "deliver", "https://target.example/orders", "target",
                _ => Task.CompletedTask, CancellationToken.None)).Skipped);
        }

        Assert.AreEqual(0, logs.Errors.Count);
        Assert.AreEqual(2, logs.Warnings.Count);
    }

    [TestMethod]
    public async Task RepeatedKeyWithoutAHandlerInvocationKeepsTheWarning()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var observations = new PlatformStepObservationCollector();
        var logs = new LogRecorder();
        var steps = new PlatformStepExecutor(dataSource, observations, logs.For<PlatformStepExecutor>());
        var message = CreateMessage($"dotnet-test-{Guid.NewGuid():N}", options);

        // Called outside the consumer, so no handler invocation is known.
        await steps.ExecuteAsync(message, "deliver", "https://target.example/orders", "target",
            _ => Task.CompletedTask, CancellationToken.None);
        var repeated = await steps.ExecuteAsync(message, "deliver", "https://target.example/orders", "target",
            _ => Task.CompletedTask, CancellationToken.None);

        Assert.IsTrue(repeated.Skipped);
        Assert.AreEqual(0, logs.Errors.Count);
        Assert.AreEqual(1, logs.Warnings.Count);
    }

    [TestMethod]
    public async Task SharedMessageIdAcrossIntegrationsSkipsTheSecondDeliveryAndWarns()
    {
        var connectionString = RequireConnectionString();
        var first = CreateOptions(connectionString);
        first.ApplicationName = "integration-first";
        var second = CreateOptions(connectionString);
        second.ApplicationName = "integration-second";
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";
        await using var dataSource = NpgsqlDataSource.Create(connectionString);

        var (firstStore, firstSteps, _, _) = CreateStore(dataSource, first);
        var firstMessage = CreateMessage(messageId, first);
        await firstSteps.ExecuteAsync(firstMessage, "deliver", "https://first.example/orders", "first",
            _ => Task.CompletedTask, CancellationToken.None);
        await firstStore.RecordAttemptAsync(firstMessage, "dotnet-handler", "COMPLETED", null, CancellationToken.None);

        // The message id travelled on to a second integration on the same
        // Message Store. bip_step_outcome is not scoped by integration, so an
        // equal step id matches the first integration's delivery.
        var logs = new LogRecorder();
        var secondSteps = new PlatformStepExecutor(dataSource, new PlatformStepObservationCollector(), logs.For<PlatformStepExecutor>());
        var secondMessage = CreateMessage(messageId, second);
        var secondRan = false;
        var collided = await secondSteps.ExecuteAsync(secondMessage, "deliver", "https://second.example/orders", "second",
            _ => { secondRan = true; return Task.CompletedTask; }, CancellationToken.None);

        Assert.IsTrue(collided.Skipped);
        Assert.IsFalse(secondRan);
        Assert.AreEqual(1, logs.Warnings.Count);
        StringAssert.Contains(logs.Warnings[0], "journaled by integration integration-first");

        // A step id unique across the integrations is delivered.
        var distinct = await secondSteps.ExecuteAsync(secondMessage, "second-deliver", "https://second.example/orders", "second",
            _ => Task.CompletedTask, CancellationToken.None);
        Assert.AreEqual("DELIVERED", distinct.Status);
    }

    [TestMethod]
    public async Task FailedJournalLookupDoesNotFailASkippedStep()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        await using var admin = NpgsqlDataSource.Create(connectionString);
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";
        var (_, adminSteps, _, _) = CreateStore(admin, options);
        var message = CreateMessage(messageId, options);
        await adminSteps.ExecuteAsync(message, "deliver", "https://target.example/orders", "target",
            _ => Task.CompletedTask, CancellationToken.None);

        // A role that can use the step ledger but cannot read bip_message_log,
        // so the lookup behind the shared-key warning fails with 42501.
        var role = $"bip_steps_only_{Guid.NewGuid():N}";
        const string password = "synthetic-test-only";
        try
        {
            await using (var connection = await admin.OpenConnectionAsync())
            {
                await using var create = new NpgsqlCommand(
                    $"create role {role} login password '{password}'; grant select, insert, update on bip_step_outcome to {role};",
                    connection);
                await create.ExecuteNonQueryAsync();
            }
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            Assert.Inconclusive("The test connection cannot create roles.");
        }

        try
        {
            var restricted = new NpgsqlConnectionStringBuilder(connectionString) { Username = role, Password = password, Pooling = false };
            await using var dataSource = NpgsqlDataSource.Create(restricted.ConnectionString);
            var logs = new LogRecorder();
            var steps = new PlatformStepExecutor(dataSource, new PlatformStepObservationCollector(), logs.For<PlatformStepExecutor>());
            var ran = false;

            var result = await steps.ExecuteAsync(message, "deliver", "https://target.example/orders", "target",
                _ => { ran = true; return Task.CompletedTask; }, CancellationToken.None);

            Assert.IsTrue(result.Skipped);
            Assert.AreEqual("SKIPPED", result.Status);
            Assert.IsFalse(ran);
            Assert.AreEqual(0, logs.Warnings.Count);
        }
        finally
        {
            await using var connection = await admin.OpenConnectionAsync();
            await using var drop = new NpgsqlCommand($"drop owned by {role}; drop role {role};", connection);
            await drop.ExecuteNonQueryAsync();
        }
    }

    [TestMethod]
    public async Task ReplayRefreshesInlineExpiryUntilFirstReceiptCap()
    {
        var options = CreateOptions(RequireConnectionString());
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var firstReceipt = new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new CaptureClock { Now = firstReceipt };
        var store = new PlatformMessageStore(dataSource, new PlatformStepObservationCollector(),
            Options.Create(options), NullLogger<PlatformMessageStore>.Instance) { Clock = clock };
        var message = CreateMessage($"dotnet-test-{Guid.NewGuid():N}", options);
        await store.RecordAttemptAsync(message, "dotnet-handler", "COMPLETED", null, CancellationToken.None);
        await using var connection = await dataSource.OpenConnectionAsync();
        await using (var age = new NpgsqlCommand(
            "update bip_message_attempt set occurred_at = @first where message_id = @id", connection))
        {
            age.Parameters.AddWithValue("first", firstReceipt);
            age.Parameters.AddWithValue("id", message.MessageId);
            await age.ExecuteNonQueryAsync();
        }

        // Just before expiry, at the renewal boundary, near the cap, at the cap,
        // and even a delayed delivery beyond it: no processing extends the cap.
        var attempt = 1;
        foreach (var days in new[] { 29, 30, 31, 59, 60, 61 })
        {
            clock.Now = firstReceipt.AddDays(days);
            var replay = message with
            {
                Headers = new Dictionary<string, string>
                {
                    ["x-bip-attempt"] = (++attempt).ToString(System.Globalization.CultureInfo.InvariantCulture),
                    ["x-bip-replay"] = "true",
                },
            };
            await store.RecordAttemptAsync(replay, "dotnet-handler", "COMPLETED", null, CancellationToken.None);
            await using var read = new NpgsqlCommand(
                """
                select l.payload_expires_at, a.payload_expires_at,
                       (select payload_expires_at from bip_message_attempt where message_id = @id and attempt = 1)
                from bip_message_log l join bip_message_attempt a using (message_id)
                where message_id = @id and a.attempt = @attempt
                """, connection);
            read.Parameters.AddWithValue("id", message.MessageId);
            read.Parameters.AddWithValue("attempt", attempt);
            await using var reader = await read.ExecuteReaderAsync();
            Assert.IsTrue(await reader.ReadAsync());
            var expected = firstReceipt.AddDays(Math.Min(days + 30, 60));
            Assert.AreEqual(expected, reader.GetFieldValue<DateTimeOffset>(0), $"Replay on day {days}");
            Assert.AreEqual(expected, reader.GetFieldValue<DateTimeOffset>(1));
            Assert.AreEqual(firstReceipt.AddDays(30), reader.GetFieldValue<DateTimeOffset>(2),
                "Replaying must not extend historical payload expiry.");
        }
    }

    [TestMethod]
    [DataRow(0)]
    [DataRow(30)]
    public async Task InlineExpiryUsesOnlyThisIntegrationsHistoryAndHandlesZeroRetention(int retentionDays)
    {
        var options = CreateOptions(RequireConnectionString());
        options.PayloadRetentionDays = retentionDays;
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var firstReceipt = new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new CaptureClock { Now = firstReceipt };
        var message = CreateMessage($"dotnet-test-{Guid.NewGuid():N}", options);
        var other = CreateOptions(options.PostgresConnectionString);
        other.ApplicationName = "integration-other";
        var (otherStore, _, _, _) = CreateStore(dataSource, other);
        await otherStore.RecordAttemptAsync(message, "other-handler", "COMPLETED", null, CancellationToken.None);
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var age = new NpgsqlCommand(
            "update bip_message_attempt set occurred_at = @first where message_id = @id and attempt = @attempt", connection);
        age.Parameters.AddWithValue("first", firstReceipt.AddYears(-1));
        age.Parameters.AddWithValue("id", message.MessageId);
        age.Parameters.AddWithValue("attempt", 1);
        await age.ExecuteNonQueryAsync();

        var store = new PlatformMessageStore(dataSource, new PlatformStepObservationCollector(),
            Options.Create(options), NullLogger<PlatformMessageStore>.Instance) { Clock = clock };
        await store.RecordAttemptAsync(message with
        {
            Headers = new Dictionary<string, string> { ["x-bip-attempt"] = "2" },
        }, "dotnet-handler", "COMPLETED", null, CancellationToken.None);
        await using var read = new NpgsqlCommand(
            "select payload_expires_at from bip_message_log where message_id = @id", connection);
        read.Parameters.AddWithValue("id", message.MessageId);
        Assert.AreEqual(firstReceipt.AddDays(retentionDays).UtcDateTime, await read.ExecuteScalarAsync(),
            "Without this integration's history, capture time anchors the expiry.");

        age.Parameters["first"].Value = firstReceipt;
        age.Parameters["attempt"].Value = 2;
        await age.ExecuteNonQueryAsync();
        clock.Now = firstReceipt.AddDays(90);
        await store.RecordAttemptAsync(message with
        {
            Headers = new Dictionary<string, string> { ["x-bip-attempt"] = "3", ["x-bip-replay"] = "true" },
        }, "dotnet-handler", "COMPLETED", null, CancellationToken.None);
        Assert.AreEqual(firstReceipt.AddDays(2 * retentionDays).UtcDateTime, await read.ExecuteScalarAsync());
    }

    private sealed class CaptureClock : TimeProvider
    {
        public DateTimeOffset Now { get; set; }
        public override DateTimeOffset GetUtcNow() => Now;
    }

    [TestMethod]
    public async Task HandlerTimeoutIsRetriedThenParkedAndConsumptionContinues()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        options.MaxHandlerAttempts = 3;
        options.HandlerRetryDelayMs = 0;
        var timedOutId = $"dotnet-test-{Guid.NewGuid():N}";
        var nextId = $"dotnet-test-{Guid.NewGuid():N}";
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, observations, _) = CreateStore(dataSource, options);
        var producer = new FakeProducer();
        // An HttpClient timeout surfaces as TaskCanceledException while the
        // stopping token is still active.
        var handler = new DelegateHandler(async (message, _) =>
        {
            await Task.Yield();
            if (message.MessageId == timedOutId)
            {
                throw new TaskCanceledException(
                    "The request was canceled due to the configured HttpClient.Timeout of 100 seconds elapsing.",
                    new TimeoutException());
            }

            return PlatformMessageResult.Completed();
        });
        var service = CreateConsumerService(handler, store, observations, producer, options);
        using var stopping = new CancellationTokenSource();
        var consumer = new FakeConsumer(stopping, ConsumeResultFor(timedOutId, 0, options), ConsumeResultFor(nextId, 1, options));

        await service.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.AreEqual(3, handler.Calls.Count(id => id == timedOutId), "A timeout goes through the in-process retry.");
        Assert.AreEqual(1, producer.Produced.Count, "The timed-out message is parked once.");
        var (topic, parked) = producer.Produced[0];
        Assert.AreEqual(options.DlqTopic, topic);
        var dlqHeaders = PlatformMessageConsumerService.ReadHeaders(parked.Headers);
        Assert.AreEqual(timedOutId, dlqHeaders["x-bip-message-id"]);
        StringAssert.Contains(dlqHeaders["x-bip-error"], "HttpClient.Timeout");
        Assert.AreEqual("DLQ", await LogStatusAsync(dataSource, timedOutId));
        Assert.AreEqual("COMPLETED", await LogStatusAsync(dataSource, nextId), "Consumption continues after the parked message.");
        CollectionAssert.AreEqual(new long[] { 0, 1 }, consumer.Commits);
        Assert.AreEqual(0, consumer.Seeks.Count);
    }

    [TestMethod]
    public async Task ShutdownDuringTheHandlerStopsWithoutParkingTheMessage()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        options.MaxHandlerAttempts = 3;
        options.HandlerRetryDelayMs = 0;
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, observations, _) = CreateStore(dataSource, options);
        var producer = new FakeProducer();
        using var stopping = new CancellationTokenSource();
        var handler = new DelegateHandler((_, cancellationToken) =>
        {
            stopping.Cancel();
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(PlatformMessageResult.Completed());
        });
        var service = CreateConsumerService(handler, store, observations, producer, options);
        var consumer = new FakeConsumer(stopping, ConsumeResultFor(messageId, 0, options));

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => service.ConsumeAsync(consumer, stopping.Token));

        Assert.AreEqual(1, handler.Calls.Count, "Shutdown is not retried.");
        Assert.AreEqual(0, producer.Produced.Count, "Shutdown never parks the message.");
        Assert.AreEqual(0, consumer.Commits.Count, "The delivery is consumed again after restart.");
        Assert.AreEqual(0, await CountRowsAsync(dataSource, "bip_message_log", messageId));
    }

    [TestMethod]
    public async Task RecoverableConsumeErrorDoesNotStopConsumption()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, observations, _) = CreateStore(dataSource, options);
        var handler = new DelegateHandler((_, _) => Task.FromResult(PlatformMessageResult.Completed()));
        var service = CreateConsumerService(handler, store, observations, new FakeProducer(), options);
        using var stopping = new CancellationTokenSource();
        var consumer = new FakeConsumer(
            stopping,
            new ConsumeException(new ConsumeResult<byte[], byte[]>(), new Error(ErrorCode.UnknownTopicOrPart, "Subscribed topic not available")),
            ConsumeResultFor(messageId, 0, options));

        await service.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.AreEqual("COMPLETED", await LogStatusAsync(dataSource, messageId));
        CollectionAssert.AreEqual(new long[] { 0 }, consumer.Commits);
    }

    [TestMethod]
    public async Task FatalConsumeErrorStillStopsTheConsumer()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, observations, _) = CreateStore(dataSource, options);
        var handler = new DelegateHandler((_, _) => Task.FromResult(PlatformMessageResult.Completed()));
        var service = CreateConsumerService(handler, store, observations, new FakeProducer(), options);
        using var stopping = new CancellationTokenSource();
        var consumer = new FakeConsumer(
            stopping,
            new ConsumeException(new ConsumeResult<byte[], byte[]>(), new Error(ErrorCode.Local_Fatal, "synthetic fatal error", true)),
            ConsumeResultFor($"dotnet-test-{Guid.NewGuid():N}", 0, options));

        await Assert.ThrowsExactlyAsync<ConsumeException>(() => service.ConsumeAsync(consumer, stopping.Token));
        Assert.AreEqual(0, handler.Calls.Count);
    }

    [TestMethod]
    public async Task StepTimeoutIsRecordedAsAFailedStep()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (_, steps, observations, _) = CreateStore(dataSource, options);
        var message = CreateMessage(messageId, options);

        await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => steps.ExecuteAsync(
            message, "deliver", "https://target.example/orders", "target",
            _ => throw new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout of 100 seconds elapsing."),
            CancellationToken.None));

        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            "select status from bip_step_outcome where idempotency_key = @idempotency_key",
            connection);
        command.Parameters.AddWithValue("idempotency_key", $"{messageId}:deliver");
        Assert.AreEqual("FAILED", await command.ExecuteScalarAsync());
        var observed = observations.Snapshot(messageId).Single();
        Assert.AreEqual("FAILED", observed.Status);
        StringAssert.Contains(observed.Error, "HttpClient.Timeout");
    }

    [TestMethod]
    public async Task StepFailureCaughtOutsideTheStepStaysFailedAndRunsAgainOnResend()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        options.MaxHandlerAttempts = 2;
        options.HandlerRetryDelayMs = 0;
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, steps, observations, _) = CreateStore(dataSource, options);
        var producer = new FakeProducer();
        var receiverFails = true;
        var sends = 0;

        // A helper that is not itself a step catches the failed send, and the
        // handler parks the message. The step recorded its own failure before
        // the catch, so it stays FAILED and a resend sends again.
        async Task<string?> PostOrderAsync(PlatformMessage message, CancellationToken cancellationToken)
        {
            try
            {
                await steps.ExecuteAsync(message, "post-order", "https://target.example/orders", "target", _ =>
                {
                    sends++;
                    return receiverFails
                        ? throw new HttpRequestException("Response status code does not indicate success: 500 (Internal Server Error).")
                        : Task.CompletedTask;
                }, cancellationToken);
                return null;
            }
            catch (HttpRequestException ex)
            {
                return ex.Message;
            }
        }

        var handler = new DelegateHandler(async (message, cancellationToken) =>
            await PostOrderAsync(message, cancellationToken) is { } error
                ? PlatformMessageResult.Failed(error)
                : PlatformMessageResult.Completed());
        var service = CreateConsumerService(handler, store, observations, producer, options);

        await service.ProcessAsync(CreateMessage(messageId, options), () => { }, CancellationToken.None);

        Assert.AreEqual(2, sends, "The in-process retry sends again.");
        Assert.AreEqual("DLQ", await LogStatusAsync(dataSource, messageId));
        Assert.AreEqual("FAILED", await StepStatusAsync(dataSource, $"{messageId}:post-order"));
        CollectionAssert.AreEqual(new[] { "post-order FAILED", "post-order FAILED" }, await ObservedStepsAsync(dataSource, messageId, 1));

        var replay = await store.ReplayAsync(messageId, new MessageReplayRequest("operator", "resend"), producer, CancellationToken.None);
        Assert.AreEqual("PUBLISHED", replay.Status);
        receiverFails = false;
        var resent = producer.Produced.Single(produced => produced.Topic == options.InputTopic).Message;
        await service.ProcessAsync(PlatformMessageConsumerService.ToPlatformMessage(resent, options), () => { }, CancellationToken.None);

        Assert.AreEqual(3, sends, "The resend runs the failed step again.");
        Assert.AreEqual("COMPLETED", await LogStatusAsync(dataSource, messageId));
        Assert.AreEqual("DELIVERED", await StepStatusAsync(dataSource, $"{messageId}:post-order"));
        CollectionAssert.AreEqual(new[] { "post-order DELIVERED" }, await ObservedStepsAsync(dataSource, messageId, 2));
    }

    [TestMethod]
    public async Task StepEndpointCredentialsAreMaskedInTheLedgerAndTheAttempt()
    {
        const string endpoint =
            "https://svc-user:synthetic-password@target.example/api/orders"
            + "?code=synthetic-function-key&api_key=synthetic-api-key&access_token=synthetic-token&sig=synthetic-signature&orderId=42";
        const string masked =
            "https://xxxxxx@target.example/api/orders?code=xxxxxx&api_key=xxxxxx&access_token=xxxxxx&sig=xxxxxx&orderId=42";
        // A network-path reference, resolved against the HTTP client's base address.
        const string relativeEndpoint = "//svc-user:synthetic-password@target.example/notify?sig=synthetic-signature";
        const string relativeMasked = "//xxxxxx@target.example/notify?sig=xxxxxx";
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";
        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, steps, observations, _) = CreateStore(dataSource, options);
        var message = CreateMessage(messageId, options);
        var called = 0;

        // The operation is the application's own call to the destination; the
        // adapter only records the URI.
        await steps.ExecuteAsync(message, "deliver", endpoint, "target", _ => { called++; return Task.CompletedTask; }, CancellationToken.None);
        var skipped = await steps.ExecuteAsync(message, "deliver", endpoint, "target", _ => { called++; return Task.CompletedTask; }, CancellationToken.None);
        await steps.ExecuteAsync(message, "notify", relativeEndpoint, "target", _ => { called++; return Task.CompletedTask; }, CancellationToken.None);

        Assert.AreEqual(2, called);
        Assert.IsTrue(skipped.Skipped);
        CollectionAssert.AreEqual(
            new[] { masked, masked, relativeMasked },
            observations.Snapshot(messageId).Select(observation => observation.EndpointUri).ToArray());

        await store.RecordAttemptAsync(message, "dotnet-handler", "COMPLETED", null, CancellationToken.None);
        await using var connection = await dataSource.OpenConnectionAsync();
        await using (var ledger = new NpgsqlCommand(
            "select step_id, endpoint_uri from bip_step_outcome where message_id = @message_id order by step_id",
            connection))
        {
            ledger.Parameters.AddWithValue("message_id", messageId);
            await using var rows = await ledger.ExecuteReaderAsync();
            var recorded = new List<string>();
            while (await rows.ReadAsync())
            {
                recorded.Add($"{rows.GetString(0)} {rows.GetString(1)}");
            }

            CollectionAssert.AreEqual(new[] { $"deliver {masked}", $"notify {relativeMasked}" }, recorded);
        }

        await using var attempt = new NpgsqlCommand(
            """
            select steps_json -> 0 ->> 'endpointUri',
                   steps_json -> 1 ->> 'endpointUri',
                   steps_json -> 2 ->> 'endpointUri',
                   steps_json::text
            from bip_message_attempt
            where message_id = @message_id and attempt = 1
            """,
            connection);
        attempt.Parameters.AddWithValue("message_id", messageId);
        await using var reader = await attempt.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync());
        Assert.AreEqual(masked, reader.GetString(0));
        Assert.AreEqual(masked, reader.GetString(1));
        Assert.AreEqual(relativeMasked, reader.GetString(2));
        Assert.IsFalse(reader.GetString(3).Contains("synthetic-", StringComparison.Ordinal), "steps_json must not carry the credentials.");
    }

    [TestMethod]
    public async Task ReplayRefusesAMessageOfAnotherIntegration()
    {
        var connectionString = RequireConnectionString();
        var first = CreateOptions(connectionString);
        first.ApplicationName = "integration-first";
        var second = CreateOptions(connectionString);
        second.ApplicationName = "integration-second";
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        var (firstStore, _, _, _) = CreateStore(dataSource, first);
        var (secondStore, _, _, _) = CreateStore(dataSource, second);
        await secondStore.RecordAttemptAsync(CreateMessage(messageId, second), "dotnet-handler", "COMPLETED", null, CancellationToken.None);
        var producer = new FakeProducer();

        var refused = await firstStore.ReplayAsync(messageId, new MessageReplayRequest("operator", "re-run"), producer, CancellationToken.None);

        Assert.AreEqual("FAILED", refused.Status);
        Assert.AreEqual(MessageReplayErrorCodes.NotReplayable, refused.ErrorCode);
        Assert.AreEqual("Message is not replayable by this integration.", refused.Error);
        Assert.IsNull(refused.ReplayId, "A refused replay writes no audit row.");
        Assert.AreEqual(0, producer.Produced.Count, "A refused replay never produces.");
        Assert.AreEqual(0, await CountAuditRowsAsync(dataSource, messageId));
        Assert.AreEqual(1, await CountRowsAsync(dataSource, "bip_message_attempt", messageId));

        // The owning integration still replays it to its own input topic.
        var replayed = await secondStore.ReplayAsync(messageId, new MessageReplayRequest("operator", "re-run"), producer, CancellationToken.None);
        Assert.AreEqual("PUBLISHED", replayed.Status);
        Assert.AreEqual("bip.integration-second.in", producer.Produced.Single().Topic);
    }

    private static PlatformMessageConsumerService CreateConsumerService(
        IPlatformMessageHandler handler,
        PlatformMessageStore store,
        PlatformStepObservationCollector observations,
        IProducer<string, byte[]> producer,
        PlatformMessageStoreOptions options)
    {
        return new PlatformMessageConsumerService(
            handler,
            store,
            observations,
            producer,
            Options.Create(options),
            NullLogger<PlatformMessageConsumerService>.Instance)
        {
            FailureBackoff = TimeSpan.Zero,
        };
    }

    private static ConsumeResult<string, byte[]> ConsumeResultFor(string messageId, long offset, PlatformMessageStoreOptions options)
    {
        return new ConsumeResult<string, byte[]>
        {
            TopicPartitionOffset = new TopicPartitionOffset(options.InputTopic, new Partition(0), new Offset(offset)),
            Message = new Message<string, byte[]>
            {
                Key = messageId,
                Value = """{"messageType":"e2e.order","runtime":"dotnet"}"""u8.ToArray(),
                Headers = new Headers { { "x-bip-message-id", Encoding.UTF8.GetBytes(messageId) } },
            },
        };
    }

    private static async Task<string?> LogStatusAsync(NpgsqlDataSource dataSource, string messageId)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand("select status from bip_message_log where message_id = @message_id", connection);
        command.Parameters.AddWithValue("message_id", messageId);
        return await command.ExecuteScalarAsync() as string;
    }

    private static async Task<string?> StepStatusAsync(NpgsqlDataSource dataSource, string idempotencyKey)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand("select status from bip_step_outcome where idempotency_key = @idempotency_key", connection);
        command.Parameters.AddWithValue("idempotency_key", idempotencyKey);
        return await command.ExecuteScalarAsync() as string;
    }

    /// <summary>The steps_json entries of one attempt, in order, as "step status".</summary>
    private static async Task<string[]> ObservedStepsAsync(NpgsqlDataSource dataSource, string messageId, int attempt)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            """
            select (observed.step ->> 'step') || ' ' || (observed.step ->> 'status')
            from bip_message_attempt,
                 jsonb_array_elements(steps_json) with ordinality as observed(step, position)
            where message_id = @message_id and attempt = @attempt
            order by observed.position
            """,
            connection);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("attempt", attempt);
        var steps = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            steps.Add(reader.GetString(0));
        }

        return [.. steps];
    }

    private sealed class DelegateHandler(Func<PlatformMessage, CancellationToken, Task<PlatformMessageResult>> handle) : IPlatformMessageHandler
    {
        public List<string> Calls { get; } = [];

        public Task<PlatformMessageResult> HandleAsync(PlatformMessage message, CancellationToken cancellationToken)
        {
            Calls.Add(message.MessageId);
            return handle(message, cancellationToken);
        }
    }

    private sealed class RecordingFailingHandler : IPlatformMessageHandler
    {
        public List<IReadOnlyDictionary<string, string>> SeenHeaderSnapshots { get; } = [];

        public Task<PlatformMessageResult> HandleAsync(PlatformMessage message, CancellationToken cancellationToken)
        {
            SeenHeaderSnapshots.Add(new Dictionary<string, string>(message.Headers, StringComparer.OrdinalIgnoreCase));
            return Task.FromResult(PlatformMessageResult.Failed("simulated handler failure"));
        }
    }

    private sealed class LogRecorder
    {
        public List<string> Warnings { get; } = [];

        public List<string> Errors { get; } = [];

        public ILogger<T> For<T>() => new Recorder<T>(this);

        private sealed class Recorder<T>(LogRecorder parent) : ILogger<T>
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (logLevel == LogLevel.Warning)
                {
                    parent.Warnings.Add(formatter(state, exception));
                }
                else if (logLevel == LogLevel.Error)
                {
                    parent.Errors.Add(formatter(state, exception));
                }
            }
        }
    }

    /// <summary>
    /// Hands out a scripted sequence of deliveries and consume errors, then
    /// requests shutdown the way the host does.
    /// </summary>
    private sealed class FakeConsumer(CancellationTokenSource stopping, params object[] steps) : IConsumer<string, byte[]>
    {
        private readonly Queue<object> script = new(steps);

        public List<long> Commits { get; } = [];

        public List<long> Seeks { get; } = [];

        public ConsumeResult<string, byte[]> Consume(CancellationToken cancellationToken = default)
        {
            if (script.Count == 0)
            {
                stopping.Cancel();
                cancellationToken.ThrowIfCancellationRequested();
            }

            return script.Dequeue() switch
            {
                Exception exception => throw exception,
                ConsumeResult<string, byte[]> result => result,
                var other => throw new InvalidOperationException($"Unexpected script step {other}."),
            };
        }

        public ConsumeResult<string, byte[]> Consume(int millisecondsTimeout) => throw new NotSupportedException();

        public ConsumeResult<string, byte[]> Consume(TimeSpan timeout) => throw new NotSupportedException();

        public void Commit(ConsumeResult<string, byte[]> result) => Commits.Add(result.Offset.Value);

        public List<TopicPartitionOffset> Commit() => throw new NotSupportedException();

        public void Commit(IEnumerable<TopicPartitionOffset> offsets) => throw new NotSupportedException();

        public void Seek(TopicPartitionOffset tpo) => Seeks.Add(tpo.Offset.Value);

        public string MemberId => "fake-member";

        public List<TopicPartition> Assignment => [];

        public List<string> Subscription => [];

        public IConsumerGroupMetadata ConsumerGroupMetadata => throw new NotSupportedException();

        public Handle Handle => throw new NotSupportedException();

        public string Name => "fake-consumer";

        public void Subscribe(IEnumerable<string> topics)
        {
        }

        public void Subscribe(string topic)
        {
        }

        public void Unsubscribe()
        {
        }

        public void Assign(TopicPartition partition) => throw new NotSupportedException();

        public void Assign(TopicPartitionOffset partition) => throw new NotSupportedException();

        public void Assign(IEnumerable<TopicPartitionOffset> partitions) => throw new NotSupportedException();

        public void Assign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions) => throw new NotSupportedException();

        public void IncrementalAssign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public void IncrementalUnassign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public void Unassign() => throw new NotSupportedException();

        public void StoreOffset(ConsumeResult<string, byte[]> result) => throw new NotSupportedException();

        public void StoreOffset(TopicPartitionOffset offset) => throw new NotSupportedException();

        public void Pause(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public void Resume(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public List<TopicPartitionOffset> Committed(TimeSpan timeout) => throw new NotSupportedException();

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout) => throw new NotSupportedException();

        public Offset Position(TopicPartition partition) => throw new NotSupportedException();

        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
            => throw new NotSupportedException();

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition) => throw new NotSupportedException();

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) => throw new NotSupportedException();

        public void Close()
        {
        }

        public int AddBrokers(string brokers) => 0;

        public void SetSaslCredentials(string username, string password)
        {
        }

        public void Dispose()
        {
        }
    }

    private sealed class FakeProducer : IProducer<string, byte[]>
    {
        private readonly Func<string, Message<string, byte[]>, Task>? onProduce;

        public FakeProducer(Func<string, Message<string, byte[]>, Task>? onProduce = null)
        {
            this.onProduce = onProduce;
        }

        public List<(string Topic, Message<string, byte[]> Message)> Produced { get; } = [];

        public Handle Handle => throw new NotSupportedException();

        public string Name => "fake-producer";

        public async Task<DeliveryResult<string, byte[]>> ProduceAsync(
            string topic,
            Message<string, byte[]> message,
            CancellationToken cancellationToken = default)
        {
            if (onProduce is not null)
            {
                await onProduce(topic, message);
            }

            Produced.Add((topic, message));
            return new DeliveryResult<string, byte[]>
            {
                Topic = topic,
                Message = message,
                Status = PersistenceStatus.Persisted,
            };
        }

        public Task<DeliveryResult<string, byte[]>> ProduceAsync(
            TopicPartition topicPartition,
            Message<string, byte[]> message,
            CancellationToken cancellationToken = default)
        {
            return ProduceAsync(topicPartition.Topic, message, cancellationToken);
        }

        public void Produce(string topic, Message<string, byte[]> message, Action<DeliveryReport<string, byte[]>>? deliveryHandler = null)
            => throw new NotSupportedException();

        public void Produce(TopicPartition topicPartition, Message<string, byte[]> message, Action<DeliveryReport<string, byte[]>>? deliveryHandler = null)
            => throw new NotSupportedException();

        public int Poll(TimeSpan timeout) => 0;

        public int Flush(TimeSpan timeout) => 0;

        public void Flush(CancellationToken cancellationToken = default)
        {
        }

        public void InitTransactions(TimeSpan timeout) => throw new NotSupportedException();

        public void BeginTransaction() => throw new NotSupportedException();

        public void CommitTransaction(TimeSpan timeout) => throw new NotSupportedException();

        public void CommitTransaction() => throw new NotSupportedException();

        public void AbortTransaction(TimeSpan timeout) => throw new NotSupportedException();

        public void AbortTransaction() => throw new NotSupportedException();

        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
            => throw new NotSupportedException();

        public int AddBrokers(string brokers) => 0;

        public void SetSaslCredentials(string username, string password)
        {
        }

        public void Dispose()
        {
        }
    }
}
