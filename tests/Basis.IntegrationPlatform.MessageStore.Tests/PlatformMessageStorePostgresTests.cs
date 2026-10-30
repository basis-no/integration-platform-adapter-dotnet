using System.Text;
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
        // DECISION D3: replayable follows payload availability, not status.
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
        // DECISION D4: steps_json records what THIS attempt observed, including
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

        // DECISION D2: the attempt number comes from x-bip-attempt, never the DB.
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
    public async Task SchemaVersionFromProducerIsStoredAndReemittedOnReplay()
    {
        var connectionString = RequireConnectionString();
        var options = CreateOptions(connectionString);
        var messageId = $"dotnet-test-{Guid.NewGuid():N}";

        await using var dataSource = NpgsqlDataSource.Create(options.PostgresConnectionString);
        var (store, _, _, _) = CreateStore(dataSource, options);

        // §3 / S20 end to end: producer declares x-bip-schema-version=7 ⇒ the
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

        // S12 (§8): publish to DLQ first, then record DLQ status, then commit.
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
        // §3: no invented wire headers leak onto DLQ records — in-process retry
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

        // COMPLETED outcome: still resendable per DECISION D3.
        await store.RecordAttemptAsync(CreateMessage(messageId, options), "dotnet-handler", "COMPLETED", null, CancellationToken.None);

        string? auditStatusDuringProduce = null;
        var producer = new FakeProducer(async (topic, message) =>
        {
            // The REQUESTED audit row must already be committed and visible from
            // a separate connection while the produce is in flight (§9).
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
        // Certification S19b: unreadable bip_contract (permissions/connectivity)
        // retries transient conditions, then FAILS startup - fail fast over
        // masked errors (safe under rolling updates).
        var options = Microsoft.Extensions.Options.Options.Create(new PlatformMessageStoreOptions
        {
            ApplicationName = "cert-s19b",
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
            }
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
