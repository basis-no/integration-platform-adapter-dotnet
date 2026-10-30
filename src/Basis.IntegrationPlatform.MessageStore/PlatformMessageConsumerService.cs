using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Basis.IntegrationPlatform.MessageStore;

internal sealed class PlatformMessageConsumerService : BackgroundService
{
    private readonly IPlatformMessageHandler handler;
    private readonly PlatformMessageStore store;
    private readonly PlatformStepObservationCollector stepObservations;
    private readonly IProducer<string, byte[]> producer;
    private readonly PlatformMessageStoreOptions options;
    private readonly ILogger<PlatformMessageConsumerService> logger;

    public PlatformMessageConsumerService(
        IPlatformMessageHandler handler,
        PlatformMessageStore store,
        PlatformStepObservationCollector stepObservations,
        IProducer<string, byte[]> producer,
        IOptions<PlatformMessageStoreOptions> options,
        ILogger<PlatformMessageConsumerService> logger)
    {
        this.handler = handler;
        this.store = store;
        this.stepObservations = stepObservations;
        this.producer = producer;
        this.options = options.Value;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        options.Validate();
        // Contract v1 §10 startup gate: verify the provisioned contract major
        // before consuming anything. Wrong major fails fast; missing table warns.
        await store.VerifyContractAsync(stoppingToken);
        var config = new ConsumerConfig(options.CreateKafkaClientConfig())
        {
            GroupId = options.ConsumerGroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        using var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        consumer.Subscribe(options.InputTopic);
        logger.LogInformation("Basis message-store adapter consuming topic {Topic} group {GroupId}", options.InputTopic, options.ConsumerGroupId);

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

            var message = ToPlatformMessage(result.Message, options);
            try
            {
                await ProcessAsync(message, () => consumer.Commit(result), stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Basis message-store adapter failed to persist or publish message result messageId={MessageId}", message.MessageId);
                consumer.Seek(result.TopicPartitionOffset);
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    /// <summary>
    /// Processes one consumed delivery end to end: handler invocation (with
    /// bounded in-process retry), outcome persistence and offset commit.
    /// Contract v1 §8 ordering on give-up (scenario S12): publish to the DLQ
    /// topic FIRST, then record the attempt with status DLQ, then commit the
    /// Kafka offset. A failed DLQ publish therefore leaves NO DLQ status row
    /// and NO committed offset, so the seek/redelivery path re-processes the
    /// same delivery until the park succeeds.
    /// </summary>
    internal async Task ProcessAsync(PlatformMessage message, Action commitOffset, CancellationToken cancellationToken)
    {
        // Reset the per-message step observations for this delivery so the
        // attempt row records exactly what THIS processing attempt observed (D4).
        stepObservations.Clear(message.MessageId);
        var handled = await HandleWithRetryAsync(message, cancellationToken);
        // In-process retry diagnostics flow ONLY into the auto_redelivered /
        // auto_redelivery_count / auto_redelivery_max columns, derived from the
        // retry-loop state. They are never invented as x-bip-* wire headers (§3).
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
                result = await handler.HandleAsync(message, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
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
        // Contract v1 §3: x-bip-schema-version is an integer string declared by
        // the producer. Absent or unparseable values default to schema version 1
        // (parity with the Camel adapter's header conversion).
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
        var values = new Dictionary<string, string>(message.Headers, StringComparer.OrdinalIgnoreCase)
        {
            ["x-bip-message-id"] = message.MessageId,
            ["x-bip-correlation-id"] = message.CorrelationId,
            ["x-bip-source-system"] = message.SourceSystem,
            ["x-bip-target-system"] = message.TargetSystem,
            ["x-bip-content-type"] = message.ContentType,
            ["x-bip-error"] = error,
            ["x-bip-dlq-at"] = DateTimeOffset.UtcNow.ToString("O"),
        };

        var headers = new Headers();
        foreach (var header in values)
        {
            headers.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
        }

        return headers;
    }

    private static string? Header(IReadOnlyDictionary<string, string> headers, string name)
    {
        return headers.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value) ? value : null;
    }

    private sealed record HandledMessage(PlatformMessageResult Result, int HandlerAttempts);
}
