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

    /// <summary>Above this many bytes, a stored text envelope value is refused at the boundary.</summary>
    internal const int MaxEnvelopeValueBytes = 1_024;

    /// <summary>After this many records in a row parked for their own data, the next one is held back instead.</summary>
    internal const int MaxRecordDataInARow = 3;

    // Bounds what is kept per partition or record, as the Camel adapter does.
    private const int MaxTrackedRecords = 1_024;

    // The record headers the Message Store keeps as text, two of them in
    // indexes (the step key adds the step id to the message id;
    // source_system and target_system share one).
    private static readonly string[] StoredTextHeaders =
    [
        "x-bip-message-id", "x-bip-correlation-id", "x-bip-content-type", "x-bip-source-system", "x-bip-target-system",
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
    private readonly PlatformUnrecordedDeliveries marks;
    // The consume loop handles one record at a time, so these need no lock.
    // Per partition: the offset of a held-back record that came without
    // x-bip-message-id, and the id generated for it, so that its next read
    // keeps the id and its marks match.
    private readonly Dictionary<(string Topic, int Partition), (long Offset, string MessageId)> generatedIds = new();
    // Per partition: how many records in a row took the terminal path because
    // the store refused their data. Records the boundary check refused do not
    // count: they say nothing about the store's settings or schema.
    private readonly Dictionary<(string Topic, int Partition), int> recordDataInARow = new();
    // Records parked for their own data in this process, so that one whose
    // reduced outcome cannot be written is not parked again on every read.
    private readonly HashSet<PlatformRecordPosition> parkedForData = new();

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
        marks = new PlatformUnrecordedDeliveries(logger);
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
            logger.LogInformation("Waiting for the host to start before checking the Message Store tables.");
            try
            {
                await WhenStartedAsync(lifetime, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // The host stopped before it started, for another reason.
                return;
            }
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
                var record = ReadRecord(result);
                traffic?.RecordAdmitted(result.Topic, result.Partition.Value, result.Offset.Value);
                try
                {
                    await ProcessAsync(record, () => consumer.Commit(result), stoppingToken);
                }
                catch (Exception ex) when (!IsShutdown(ex, stoppingToken))
                {
                    LogNotCommitted(ex, record);
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
    internal Task ProcessAsync(PlatformMessage message, Action commitOffset, CancellationToken cancellationToken) =>
        ProcessAsync(new ConsumedRecord(message, null, null), commitOffset, cancellationToken);

    internal async Task ProcessAsync(ConsumedRecord record, Action commitOffset, CancellationToken cancellationToken)
    {
        var message = record.Message;
        // Reset the per-message step observations for this delivery so the
        // attempt row records exactly what THIS processing attempt observed.
        stepObservations.Clear(message.MessageId);
        if (record.Refusal is not null)
        {
            // The handler does not run, so nothing is sent for a record the
            // store cannot hold.
            await ParkForItsOwnDataAsync(record, Metadata(0), record.Refusal, completed: false, refusedByStore: false, parked: false, commitOffset, cancellationToken);
            return;
        }

        var delivery = new PlatformDelivery(record.Position, marks);
        HandledMessage handled;
        delivery.Begin();
        try
        {
            handled = await HandleWithRetryAsync(message, delivery, cancellationToken);
        }
        finally
        {
            delivery.End();
        }

        var metadata = Metadata(handled.HandlerAttempts);

        // A step outcome check or write failed, whatever the handler did with
        // the exception: a step may not have run, or its delivery isn't
        // recorded. Process the delivery again instead of recording an
        // outcome, and park nothing, unless every such failure was the
        // record's own data, which no read again can store.
        if (delivery.StoreFailure is { } storeFailure)
        {
            // On shutdown, stop as a shutdown rather than as a store failure.
            cancellationToken.ThrowIfCancellationRequested();
            if (delivery.StoreFailedOnlyForRecordData)
            {
                await ParkForItsOwnDataAsync(record, metadata, StepDataReason(delivery, storeFailure), completed: false, refusedByStore: true, parked: false, commitOffset, cancellationToken);
                return;
            }

            throw new PlatformRecordHeldBackException("a step outcome was not checked or recorded, so it is consumed again", storeFailure);
        }

        if (handled.Result.Succeeded)
        {
            if (await RecordOutcomeAsync(record, metadata, "COMPLETED", null, parked: false, commitOffset, cancellationToken))
            {
                commitOffset();
                Committed(record.Position, normal: true);
            }

            return;
        }

        var error = PlatformStoreFailures.Storable(handled.Result.Error ?? "Handler returned failure.");
        await PublishDlqAsync(message, error, cancellationToken);
        if (await RecordOutcomeAsync(record, metadata, "DLQ", error, parked: true, commitOffset, cancellationToken))
        {
            commitOffset();
            Committed(record.Position, normal: true);
        }
    }

    // In-process retry diagnostics flow ONLY into the auto_redelivered /
    // auto_redelivery_count / auto_redelivery_max columns, derived from the
    // retry-loop state. They are never invented as x-bip-* wire headers.
    private PlatformMessageAttemptMetadata Metadata(int handlerAttempts) => new(
        options.RouteId,
        handlerAttempts > 1,
        handlerAttempts > 1 ? handlerAttempts - 1 : null,
        options.MaxHandlerAttempts > 1 ? options.MaxHandlerAttempts - 1 : null);

    /// <summary>
    /// Records the outcome. False when the store refused it for the record's
    /// own data and the terminal path took the record instead.
    /// </summary>
    private async Task<bool> RecordOutcomeAsync(
        ConsumedRecord record,
        PlatformMessageAttemptMetadata metadata,
        string status,
        string? error,
        bool parked,
        Action commitOffset,
        CancellationToken cancellationToken)
    {
        try
        {
            await store.RecordAttemptAsync(record.Message, metadata, status, error, cancellationToken);
            return true;
        }
        catch (Exception ex) when (PlatformStoreFailures.IsRecordData(ex))
        {
            // Every step outcome is durable here; the outcome alone cannot be
            // stored.
            await ParkForItsOwnDataAsync(
                record,
                metadata,
                $"The Message Store cannot hold the record's outcome (SQLSTATE {PlatformStoreFailures.SqlState(ex)})",
                completed: status == "COMPLETED",
                refusedByStore: true,
                parked,
                commitOffset,
                cancellationToken);
            return false;
        }
        catch (Exception ex) when (!IsShutdown(ex, cancellationToken))
        {
            throw new PlatformRecordHeldBackException("its outcome could not be recorded, so it is consumed again", ex);
        }
    }

    private static string StepDataReason(PlatformDelivery delivery, Exception storeFailure)
    {
        var sent = delivery.UnrecordedSends;
        return $"The Message Store cannot hold a step outcome of the record (SQLSTATE {PlatformStoreFailures.SqlState(storeFailure)}); "
            + (sent.Count == 0
                ? "no send of the attempt had returned without a recorded outcome"
                : $"the send of step {string.Join(", ", sent)} had returned, its outcome not recorded");
    }

    /// <summary>
    /// The terminal path for a record whose own data the Message Store cannot
    /// hold: park it, unless it was parked already or its handler completed,
    /// record a reduced outcome without the envelope's text values and steps,
    /// and commit only after both are durable. If either fails, the record is
    /// read again and the path runs again. After
    /// <see cref="MaxRecordDataInARow"/> such records in a row on a partition,
    /// the next one is held back instead, since a setting or column that fails
    /// every record must not drain the topic into the DLQ.
    /// </summary>
    private async Task ParkForItsOwnDataAsync(
        ConsumedRecord record,
        PlatformMessageAttemptMetadata metadata,
        string reason,
        bool completed,
        bool refusedByStore,
        bool parked,
        Action commitOffset,
        CancellationToken cancellationToken)
    {
        var message = record.Message;
        var position = record.Position;
        if (refusedByStore && position is not null
            && recordDataInARow.GetValueOrDefault(PartitionOf(position)) >= MaxRecordDataInARow)
        {
            throw new PlatformRecordHeldBackException(
                $"{MaxRecordDataInARow} records in a row on this partition could not be stored for their own data, so this one is held back; check the Message Store's settings and schema. {reason}",
                null,
                position,
                PlatformStoreErrorClass.Schema);
        }

        // The parked copy and the row name the record: the DLQ copy carries no
        // Kafka position, and a derived id is traced back to the record only
        // through it.
        var stored = position is null ? reason : $"{reason} (record {position})";
        if (!completed && !parked && (position is null || !parkedForData.Contains(position)))
        {
            try
            {
                await PublishDlqAsync(message, stored, cancellationToken);
            }
            catch (Exception ex) when (!IsShutdown(ex, cancellationToken))
            {
                throw new PlatformRecordHeldBackException($"it could not be parked for its own data; {reason}", ex, position, PlatformStoreErrorClass.RecordData);
            }

            if (position is not null)
            {
                Keep(parkedForData, position);
            }
        }

        try
        {
            await store.RecordReducedAttemptAsync(message, metadata, completed ? "COMPLETED" : "DLQ", stored, cancellationToken);
        }
        catch (Exception ex) when (!IsShutdown(ex, cancellationToken))
        {
            throw new PlatformRecordHeldBackException("its reduced outcome could not be recorded", ex, position);
        }

        // A failed commit keeps the record in parkedForData, so that its next
        // read records and commits without parking it again.
        commitOffset();
        if (position is not null)
        {
            parkedForData.Remove(position);
            Committed(position, normal: false);
            if (refusedByStore)
            {
                recordDataInARow[PartitionOf(position)] = recordDataInARow.GetValueOrDefault(PartitionOf(position)) + 1;
            }
        }

        // Parked and recorded: the consumer goes on with the next record.
        logger.LogError(
            completed
                ? "Message {MessageId} from record {Position} was recorded without its envelope values: {Reason} storeError=record-data"
                : "Message {MessageId} from record {Position} was parked: {Reason} storeError=record-data",
            message.MessageId,
            position?.ToString() ?? "unknown",
            reason);
    }

    /// <summary>
    /// What a committed record ends on its partition: the marks and kept id;
    /// and, unless it took the terminal path, the run of records parked for
    /// their own data.
    /// </summary>
    private void Committed(PlatformRecordPosition? position, bool normal)
    {
        if (position is null)
        {
            return;
        }

        marks.Supersede(position, committed: true);
        generatedIds.Remove(PartitionOf(position));
        if (normal)
        {
            recordDataInARow.Remove(PartitionOf(position));
        }
    }

    private static (string Topic, int Partition) PartitionOf(PlatformRecordPosition position) => (position.Topic, position.Partition);

    private static void Keep<T>(HashSet<T> set, T value)
    {
        if (set.Count >= MaxTrackedRecords && !set.Contains(value))
        {
            set.Remove(set.First());
        }

        set.Add(value);
    }

    private void LogNotCommitted(Exception failure, ConsumedRecord record)
    {
        if (failure is not PlatformRecordHeldBackException heldBack)
        {
            logger.LogError(failure, "Message-store adapter failed to persist or publish message result messageId={MessageId}", record.Message.MessageId);
            return;
        }

        // The line ends with the store error class, and the SQLSTATE when
        // known, so an alert can tell a read-only store or a lost grant from
        // an outage.
        logger.LogError(
            heldBack.InnerException,
            "Message {MessageId}{AtRecord} is not committed: {Reason} storeError={StoreError}{SqlStateSuffix}",
            record.Message.MessageId,
            heldBack.Position is { } position ? $" at {position}" : string.Empty,
            heldBack.Reason,
            heldBack.ErrorClass.Label(),
            heldBack.SqlState is { } sqlState ? $" sqlState={sqlState}" : string.Empty);
    }

    private async Task<HandledMessage> HandleWithRetryAsync(PlatformMessage message, PlatformDelivery delivery, CancellationToken cancellationToken)
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

            // A store failure, in any class, ends the attempt at once: a
            // handler retry would only fail before its steps are sent.
            if (result.Succeeded || attempt == maxAttempts || delivery.StoreFailure is not null)
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

    /// <summary>
    /// Reads a consumed record. For a record without <c>x-bip-message-id</c>
    /// the id generated on its first read is kept while the same offset is
    /// read again in this process, so the marks of its sends match. A record
    /// whose envelope values the Message Store cannot hold is refused before
    /// anything else, and recorded under the id derived from its position when
    /// its own id is refused.
    /// </summary>
    internal ConsumedRecord ReadRecord(ConsumeResult<string, byte[]> result)
    {
        var position = new PlatformRecordPosition(result.Topic, result.Partition.Value, result.Offset.Value);
        // A later record on the partition supersedes the marks of the one
        // before it.
        marks.Supersede(position, committed: false);
        var headers = ReadHeaders(result.Message.Headers);
        var (refusal, idRefused) = RefuseUnstorableEnvelope(result.Message, headers);
        // A refused id is replaced by the derived one, so the record has an id
        // of its own, as one that carries x-bip-message-id has.
        var generated = KeepGeneratedId(position, idRefused || Header(headers, "x-bip-message-id") is not null);
        var messageId = idRefused
            ? PlatformRecordIdentity.Derive(options.ApplicationName, position.Topic, position.Partition, position.Offset)
            : generated;
        return new ConsumedRecord(ToPlatformMessage(result.Message, headers, options, messageId), position, refusal);
    }

    /// <summary>
    /// The id generated for a record without one, kept per partition with the
    /// record's offset: the same offset read again gets it back, and any other
    /// record on the partition drops it. Null for a record with its own id.
    /// </summary>
    private string? KeepGeneratedId(PlatformRecordPosition position, bool hasMessageId)
    {
        var partition = PartitionOf(position);
        if (generatedIds.TryGetValue(partition, out var kept))
        {
            if (kept.Offset != position.Offset)
            {
                generatedIds.Remove(partition);
            }
            else if (!hasMessageId)
            {
                return kept.MessageId;
            }
        }

        if (hasMessageId)
        {
            return null;
        }

        var generated = Guid.NewGuid().ToString();
        if (generatedIds.Count >= MaxTrackedRecords)
        {
            generatedIds.Remove(generatedIds.Keys.First());
        }

        generatedIds[partition] = (position.Offset, generated);
        return generated;
    }

    /// <summary>
    /// Refuses a record whose text envelope values the Message Store cannot
    /// hold: a value with a NUL byte, which PostgreSQL text cannot hold, or
    /// longer than <see cref="MaxEnvelopeValueBytes"/> bytes, which a btree
    /// index entry (at most 2704 bytes) might not. The check is on the
    /// header's raw bytes, so both adapters agree on values that are not
    /// valid UTF-8; the last header of a name counts, as when it is read. The
    /// record key, which becomes the correlation id when the record has no
    /// usable x-bip-correlation-id, is checked the same way.
    /// </summary>
    private static (string? Refusal, bool IdRefused) RefuseUnstorableEnvelope(
        Message<string, byte[]> message, IReadOnlyDictionary<string, string> headers)
    {
        var raw = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
        foreach (var header in message.Headers ?? [])
        {
            if (StoredTextHeaders.Contains(header.Key, StringComparer.OrdinalIgnoreCase))
            {
                raw[header.Key] = header.GetValueBytes() ?? [];
            }
        }

        var refused = new List<string>();
        var idRefused = false;
        foreach (var name in StoredTextHeaders)
        {
            if (raw.TryGetValue(name, out var bytes) && WhyUnstorable(bytes) is { } why)
            {
                refused.Add($"{name} {why}");
                idRefused |= name == "x-bip-message-id";
            }
        }

        if (Header(headers, "x-bip-correlation-id") is null && message.Key is not null
            && WhyUnstorable(Encoding.UTF8.GetBytes(message.Key)) is { } keyWhy)
        {
            refused.Add($"the record key, which is the correlation id, {keyWhy}");
        }

        return refused.Count == 0
            ? (null, false)
            : ($"The Message Store cannot hold the record's envelope: {string.Join(", ", refused)}; the handler did not run", idRefused);
    }

    private static string? WhyUnstorable(byte[] value) =>
        value.Contains((byte)0) ? "contains a NUL byte"
        : value.Length > MaxEnvelopeValueBytes ? $"is {value.Length} bytes, over the {MaxEnvelopeValueBytes}-byte limit"
        : null;

    internal static PlatformMessage ToPlatformMessage(Message<string, byte[]> message, PlatformMessageStoreOptions options) =>
        ToPlatformMessage(message, ReadHeaders(message.Headers), options, null);

    private static PlatformMessage ToPlatformMessage(
        Message<string, byte[]> message, IReadOnlyDictionary<string, string> headers, PlatformMessageStoreOptions options, string? messageIdOverride)
    {
        var messageId = messageIdOverride ?? Header(headers, "x-bip-message-id") ?? Guid.NewGuid().ToString();
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

/// <summary>A consumed record as the consumer processes it: the message, its position, and why the boundary check refused it.</summary>
internal sealed record ConsumedRecord(PlatformMessage Message, PlatformRecordPosition? Position, string? Refusal);

/// <summary>
/// A record that is not committed and is read again, with what the log line
/// says about it: the reason and the store error class, with its SQLSTATE
/// when known.
/// </summary>
internal sealed class PlatformRecordHeldBackException : Exception
{
    internal PlatformRecordHeldBackException(
        string reason, Exception? storeFailure, PlatformRecordPosition? position = null, PlatformStoreErrorClass? errorClass = null)
        : base($"The message is not committed: {reason}", storeFailure)
    {
        Reason = reason;
        Position = position;
        ErrorClass = errorClass ?? PlatformStoreFailures.Classify(storeFailure);
        SqlState = errorClass is null ? PlatformStoreFailures.SqlState(storeFailure) : null;
    }

    internal string Reason { get; }

    internal PlatformRecordPosition? Position { get; }

    internal PlatformStoreErrorClass ErrorClass { get; }

    internal string? SqlState { get; }
}
