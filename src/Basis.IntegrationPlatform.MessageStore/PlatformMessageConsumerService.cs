using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Basis.IntegrationPlatform.MessageStore;

internal sealed class PlatformMessageConsumerService : BackgroundService
{
    internal const string MaskedHeaderValue = "xxxxxx";

    private static readonly HashSet<string> CredentialHeaders = new(StringComparer.Ordinal)
    {
        "authorization", "proxy-authorization", "cookie", "set-cookie", "www-authenticate", "proxy-authenticate",
    };

    private static readonly string[] CredentialNameParts =
    [
        "secret", "token", "password", "passwd", "credential", "session", "signature",
        "apikey", "api-key", "api_key", "subscription-key", "private-key", "auth",
    ];

    private readonly IPlatformMessageHandler handler;
    private readonly PlatformMessageStore store;
    private readonly PlatformStepObservationCollector stepObservations;
    private readonly IProducer<string, byte[]> producer;
    private readonly PlatformMessageStoreOptions options;
    private readonly ILogger<PlatformMessageConsumerService> logger;
    private readonly PlatformTrafficMetrics? traffic;
    private readonly PlatformReplayEndpoints? replayEndpoints;
    private readonly IHostApplicationLifetime? lifetime;

    public PlatformMessageConsumerService(
        IPlatformMessageHandler handler,
        PlatformMessageStore store,
        PlatformStepObservationCollector stepObservations,
        IProducer<string, byte[]> producer,
        IOptions<PlatformMessageStoreOptions> options,
        ILogger<PlatformMessageConsumerService> logger,
        PlatformTrafficMetrics? traffic = null,
        PlatformReplayEndpoints? replayEndpoints = null,
        IHostApplicationLifetime? lifetime = null)
    {
        this.handler = handler;
        this.store = store;
        this.stepObservations = stepObservations;
        this.producer = producer;
        this.options = options.Value;
        this.logger = logger;
        this.replayEndpoints = replayEndpoints;
        this.lifetime = lifetime;
        this.traffic = traffic;
        if (producer is PlatformTrafficProducer instrumented) instrumented.Selected();
        else traffic?.InvalidatePublication();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        options.Validate();
        // Startup gate: verify the store schema major in bip_contract, then
        // that the store has every table and column of the shipped schema that
        // this configuration uses, before consuming anything. Wrong major or a
        // missing table or column fails fast; a missing bip_contract table
        // warns.
        await store.VerifyContractAsync(stoppingToken);
        // Whether the resend endpoint is mapped is known once the host has
        // started: a Startup class maps its endpoints while the web host
        // starts, after this service.
        if (lifetime is not null && replayEndpoints is not null)
        {
            await WhenStartedAsync(lifetime, stoppingToken);
        }

        await store.VerifyStoreColumnsAsync(replayEndpoints?.Mapped == true, stoppingToken);
        var config = new ConsumerConfig(options.CreateKafkaClientConfig())
        {
            GroupId = options.ConsumerGroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        using var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        consumer.Subscribe(options.InputTopic);
        logger.LogInformation("Message-store adapter consuming topic {Topic} group {GroupId}", options.InputTopic, options.ConsumerGroupId);
        await ConsumeAsync(consumer, stoppingToken);
    }

    private static Task WhenStartedAsync(IHostApplicationLifetime lifetime, CancellationToken stoppingToken)
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lifetime.ApplicationStarted.Register(() => started.TrySetResult());
        return started.Task.WaitAsync(stoppingToken);
    }

    internal TimeSpan FailureBackoff { get; init; } = TimeSpan.FromSeconds(5);

    internal async Task ConsumeAsync(IConsumer<string, byte[]> consumer, CancellationToken stoppingToken)
    {
        traffic?.ConsumerStarted();
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]> result;
                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (ConsumeException ex) when (!ex.Error.IsFatal)
                {
                    // The client reports recoverable errors, such as a subscribed
                    // topic that does not exist yet, and keeps going; so does the
                    // loop. The deserializers cannot fail (keys are decoded as UTF-8
                    // with replacement, values are raw bytes), so no record is
                    // skipped. A fatal error still ends the consumer.
                    logger.LogWarning("Message-store adapter consume error, retrying code={ErrorCode} reason={Reason}", ex.Error.Code, ex.Error.Reason);
                    await Task.Delay(FailureBackoff, stoppingToken);
                    continue;
                }

                if (result.IsPartitionEOF || result.Message is null) continue;
                var message = ToPlatformMessage(result.Message, options);
                traffic?.RecordAdmitted(result.Topic, result.Partition.Value, result.Offset.Value);
                try
                {
                    await ProcessAsync(message, () => consumer.Commit(result), stoppingToken);
                }
                catch (Exception ex) when (!IsShutdown(ex, stoppingToken))
                {
                    logger.LogError(ex, "Message-store adapter failed to persist or publish message result messageId={MessageId}", message.MessageId);
                    consumer.Seek(result.TopicPartitionOffset);
                    await Task.Delay(FailureBackoff, stoppingToken);
                }
            }
        }
        finally
        {
            traffic?.ConsumerStopped();
        }
    }

    /// <summary>
    /// Processes one consumed delivery end to end: handler invocation (with
    /// bounded in-process retry), outcome persistence and offset commit.
    /// Ordering on give-up: publish to the DLQ topic FIRST, then record the
    /// attempt with status DLQ, then commit the Kafka offset. A failed DLQ
    /// publish therefore leaves NO DLQ status row and NO committed offset, so
    /// the seek/redelivery path re-processes the same delivery until the park
    /// succeeds.
    /// </summary>
    internal async Task ProcessAsync(PlatformMessage message, Action commitOffset, CancellationToken cancellationToken)
    {
        // Reset the per-message step observations for this delivery so the
        // attempt row records exactly what THIS processing attempt observed.
        stepObservations.Clear(message.MessageId);
        var handled = await HandleWithRetryAsync(message, cancellationToken);
        // In-process retry diagnostics flow ONLY into the auto_redelivered /
        // auto_redelivery_count / auto_redelivery_max columns, derived from the
        // retry-loop state. They are never invented as x-bip-* wire headers.
        var metadata = new PlatformMessageAttemptMetadata(
            options.RouteId,
            handled.HandlerAttempts > 1,
            handled.HandlerAttempts > 1 ? handled.HandlerAttempts - 1 : null,
            options.MaxHandlerAttempts > 1 ? options.MaxHandlerAttempts - 1 : null);

        if (handled.Result.Succeeded)
        {
            await store.RecordAttemptAsync(message, metadata, "COMPLETED", null, cancellationToken);
            commitOffset();
            return;
        }

        var error = handled.Result.Error ?? "Handler returned failure.";
        await PublishDlqAsync(message, error, cancellationToken);
        await store.RecordAttemptAsync(message, metadata, "DLQ", error, cancellationToken);
        commitOffset();
    }

    private async Task<HandledMessage> HandleWithRetryAsync(PlatformMessage message, CancellationToken cancellationToken)
    {
        var maxAttempts = Math.Max(1, options.MaxHandlerAttempts);
        PlatformMessageResult result = PlatformMessageResult.Failed("Handler was not invoked.");

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                // The handler sees the consumed message unchanged on every
                // in-process retry: retry diagnostics are recorded in the
                // auto_redelivery_* columns, never invented as wire headers.
                stepObservations.BeginHandlerInvocation(message.MessageId);
                result = await handler.HandleAsync(message, cancellationToken);
            }
            catch (Exception ex) when (!IsShutdown(ex, cancellationToken))
            {
                logger.LogError(ex, "Unhandled .NET integration message failure messageId={MessageId} handlerAttempt={HandlerAttempt}", message.MessageId, attempt);
                result = PlatformMessageResult.Failed(ex.Message);
            }

            if (result.Succeeded || attempt == maxAttempts)
            {
                return new HandledMessage(result, attempt);
            }

            logger.LogWarning(
                ".NET integration message handler failed; retrying messageId={MessageId} handlerAttempt={HandlerAttempt} maxHandlerAttempts={MaxHandlerAttempts}",
                message.MessageId,
                attempt,
                maxAttempts);

            if (options.HandlerRetryDelayMs > 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(options.HandlerRetryDelayMs), cancellationToken);
            }
        }

        return new HandledMessage(result, maxAttempts);
    }

    // Only the host's stopping token means shutdown. Any other cancellation,
    // such as the TaskCanceledException of an HttpClient timeout, is a handler
    // failure: retried, then parked, never a reason to stop consuming.
    private static bool IsShutdown(Exception exception, CancellationToken stoppingToken)
    {
        return exception is OperationCanceledException && stoppingToken.IsCancellationRequested;
    }

    internal static PlatformMessage ToPlatformMessage(Message<string, byte[]> message, PlatformMessageStoreOptions options)
    {
        var headers = ReadHeaders(message.Headers);

        var messageId = Header(headers, "x-bip-message-id") ?? Guid.NewGuid().ToString();
        var correlationId = Header(headers, "x-bip-correlation-id") ?? message.Key ?? messageId;
        return new PlatformMessage(
            messageId,
            correlationId,
            options.ApplicationName,
            options.EnvironmentName,
            options.Tenant,
            Header(headers, "x-bip-source-system") ?? "unknown",
            Header(headers, "x-bip-target-system") ?? options.ApplicationName,
            ResolveContentType(headers),
            message.Value ?? [],
            headers,
            SchemaVersionFrom(headers));
    }

    internal static int SchemaVersionFrom(IReadOnlyDictionary<string, string> headers)
    {
        // x-bip-schema-version is an integer string declared by the producer.
        // Absent or unparseable values default to schema version 1 (parity
        // with the Camel adapter's header conversion).
        var raw = Header(headers, "x-bip-schema-version");
        return raw is not null
            && int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var schemaVersion)
            ? schemaVersion
            : 1;
    }

    private Task PublishDlqAsync(PlatformMessage message, string error, CancellationToken cancellationToken)
    {
        return producer.ProduceAsync(
            options.DlqTopic,
            new Message<string, byte[]>
            {
                Key = message.MessageId,
                Value = message.Payload,
                Headers = CreateDlqHeaders(message, error),
            },
            cancellationToken);
    }

    internal static IReadOnlyDictionary<string, string> ReadHeaders(Headers? source)
    {
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (source is null)
        {
            return headers;
        }

        foreach (var header in source)
        {
            headers[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes() ?? []);
        }

        return headers;
    }

    internal static string ResolveContentType(IReadOnlyDictionary<string, string> headers)
    {
        return Header(headers, "x-bip-content-type")
            ?? Header(headers, "content-type")
            ?? "application/octet-stream";
    }

    internal static Headers CreateDlqHeaders(PlatformMessage message, string error)
    {
        // Only the x-bip-* envelope is required on the DLQ record. The other
        // consumed headers are kept for diagnosis, but a producer can forward
        // credentials (Authorization, cookies, API keys) onto the input topic,
        // and the DLQ topic is shared: keep their names, mask the values.
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var header in message.Headers)
        {
            values[header.Key] = IsCredentialHeader(header.Key) ? MaskedHeaderValue : header.Value;
        }

        var envelope = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["x-bip-message-id"] = message.MessageId,
            ["x-bip-correlation-id"] = message.CorrelationId,
            ["x-bip-source-system"] = message.SourceSystem,
            ["x-bip-target-system"] = message.TargetSystem,
            ["x-bip-content-type"] = message.ContentType,
            ["x-bip-error"] = error,
            ["x-bip-dlq-at"] = DateTimeOffset.UtcNow.ToString("O"),
        };
        foreach (var header in envelope)
        {
            values[header.Key] = header.Value;
        }

        var headers = new Headers();
        foreach (var header in values)
        {
            headers.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
        }

        return headers;
    }

    // Same rule as the Camel adapter's DLQ parking (0.3.6), so both adapters
    // mask the same header names on the shared DLQ topic. A -key or _key
    // suffix covers vendor keys such as x-functions-key; Idempotency-Key is
    // a delivery identifier, not a credential.
    internal static bool IsCredentialHeader(string name)
    {
        var lower = name.ToLowerInvariant();
        if (lower.StartsWith("x-bip-", StringComparison.Ordinal) || lower == "idempotency-key")
        {
            return false;
        }

        return CredentialHeaders.Contains(lower)
            || CredentialNameParts.Any(part => lower.Contains(part, StringComparison.Ordinal))
            || lower.EndsWith("-key", StringComparison.Ordinal)
            || lower.EndsWith("_key", StringComparison.Ordinal);
    }

    private static string? Header(IReadOnlyDictionary<string, string> headers, string name)
    {
        return headers.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value) ? value : null;
    }

    private sealed record HandledMessage(PlatformMessageResult Result, int HandlerAttempts);
}
