using System.Data;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace Basis.IntegrationPlatform.MessageStore;

public sealed class PlatformMessageStore
{
    internal const int SupportedContractMajor = 1;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly NpgsqlDataSource dataSource;
    private readonly PlatformStepObservationCollector stepObservations;
    private readonly PlatformMessageStoreOptions options;
    private readonly ILogger<PlatformMessageStore> logger;

    internal TimeProvider Clock { get; init; } = TimeProvider.System;

    public PlatformMessageStore(
        NpgsqlDataSource dataSource,
        PlatformStepObservationCollector stepObservations,
        IOptions<PlatformMessageStoreOptions> options,
        ILogger<PlatformMessageStore> logger)
    {
        this.dataSource = dataSource;
        this.stepObservations = stepObservations;
        this.options = options.Value;
        this.logger = logger;
    }

    /// <summary>
    /// Startup gate: read the store schema major (bip_contract.contract_major)
    /// before consuming. A missing or empty table is a loud warning (treated as
    /// major 1, for stores provisioned without it); a different major fails fast.
    /// </summary>
    public async Task VerifyContractAsync(CancellationToken cancellationToken)
    {
        // An absent table (42P01) warns and continues; any other failure to
        // obtain an answer gets bounded retries (cold pools) and then FAILS
        // startup - fail fast over masked errors. A mismatched schema major
        // always fails startup.
        object? result;
        Exception? lastFailure = null;
        var attempt = 0;
        while (true)
        {
            attempt++;
            try
            {
                await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
                await using var command = new NpgsqlCommand("select max(contract_major) from bip_contract", connection);
                result = await command.ExecuteScalarAsync(cancellationToken);
                break;
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                logger.LogWarning(
                    "bip_contract table is missing; assuming message-store contract major {ContractMajor}. " +
                    "Provision the contract metadata (004_contract_metadata.sql) so the adapter can verify the schema before consuming.",
                    SupportedContractMajor);
                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                lastFailure = ex;
                if (attempt >= options.ContractGateMaxAttempts)
                {
                    // Fail fast over masked errors. Startup failure is safe
                    // under rolling updates (previous pods keep serving) and an
                    // adapter that cannot reach its journal must not consume
                    // anyway.
                    throw new InvalidOperationException(
                        $"Message-store contract gate could not read bip_contract after {attempt} attempts " +
                        $"({lastFailure.Message}). Refusing to start: fix grants/connectivity " +
                        "(the runtime role needs select on bip_contract).",
                        lastFailure);
                }
                logger.LogInformation(
                    "Contract check attempt {Attempt}/{MaxAttempts} failed ({Message}); retrying in {DelayMs} ms.",
                    attempt,
                    options.ContractGateMaxAttempts,
                    ex.Message,
                    options.ContractGateRetryDelayMs);
                await Task.Delay(TimeSpan.FromMilliseconds(options.ContractGateRetryDelayMs), cancellationToken);
            }
        }

        if (result is null or DBNull)
        {
            logger.LogWarning(
                "bip_contract table has no rows; assuming message-store contract major {ContractMajor}.",
                SupportedContractMajor);
            return;
        }

        var contractMajor = Convert.ToInt32(result, CultureInfo.InvariantCulture);
        if (contractMajor != SupportedContractMajor)
        {
            throw new InvalidOperationException(
                $"The provisioned message-store schema declares contract major {contractMajor}, but this adapter " +
                $"implements contract major {SupportedContractMajor}. Refusing to start: writing a mismatched schema " +
                "shape would corrupt the message store. Deploy an adapter version matching the provisioned contract.");
        }

        logger.LogInformation("Message-store contract major {ContractMajor} verified against bip_contract.", contractMajor);
    }

    /// <summary>
    /// Startup check after the contract major: every table of the shipped store
    /// schema that this configuration uses, with every column, resolves
    /// through the connection's search_path, as the adapter's statements
    /// resolve them. The message tables are always required, bip_step_outcome
    /// with the Postgres step outcome store and bip_message_replay_audit when
    /// the resend endpoint is mapped. Anything missing fails startup, naming it
    /// with the statement that adds it; a failure to read gets the gate's
    /// bounded retries.
    /// </summary>
    internal async Task VerifyStoreColumnsAsync(bool replay, CancellationToken cancellationToken)
    {
        var required = PlatformMessageStoreSchema.Required(options.UsesStepLedger, replay);
        var present = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                present.Clear();
                await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
                // pg_attribute lists every column of a table the search_path
                // resolves, also one the role has no privilege on; to_regclass
                // gives null for a table that does not resolve.
                await using var command = new NpgsqlCommand(
                    """
                    select t.name, a.attname
                    from unnest(@tables) as t(name)
                    join pg_attribute a on a.attrelid = to_regclass(t.name) and a.attnum > 0 and not a.attisdropped
                    """,
                    connection);
                command.Parameters.Add("tables", NpgsqlDbType.Array | NpgsqlDbType.Text).Value =
                    required.Select(table => table.Name).ToArray();
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var table = reader.GetString(0);
                    if (!present.TryGetValue(table, out var columns))
                    {
                        present[table] = columns = new HashSet<string>(StringComparer.Ordinal);
                    }

                    columns.Add(reader.GetString(1));
                }

                break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                if (attempt >= options.ContractGateMaxAttempts)
                {
                    throw new InvalidOperationException(
                        $"Message Store column check could not read the store's tables after {attempt} attempts ({ex.Message}). " +
                        "Refusing to start: fix grants/connectivity.",
                        ex);
                }

                logger.LogInformation(
                    "Message Store column check attempt {Attempt}/{MaxAttempts} failed ({Message}); retrying in {DelayMs} ms.",
                    attempt,
                    options.ContractGateMaxAttempts,
                    ex.Message,
                    options.ContractGateRetryDelayMs);
                await Task.Delay(TimeSpan.FromMilliseconds(options.ContractGateRetryDelayMs), cancellationToken);
            }
        }

        var missing = new List<string>();
        var statements = new List<string>();
        var followUps = new List<string>();
        foreach (var table in required)
        {
            if (!present.TryGetValue(table.Name, out var columns))
            {
                missing.Add($"table {table.Name}");
                statements.Add(table.CreateStatement);
                statements.AddRange(table.IndexStatements);
                continue;
            }

            foreach (var (column, definition) in table.Columns.Where(column => !columns.Contains(column.Key)))
            {
                missing.Add($"{table.Name}.{column}");
                var (statement, followUp) = PlatformMessageStoreSchema.AddColumnStatement(table.Name, definition);
                statements.Add(statement);
                if (followUp is not null)
                {
                    followUps.Add(followUp);
                }
            }
        }

        if (missing.Count > 0)
        {
            throw new InvalidOperationException(
                $"The Message Store lacks {string.Join(", ", missing)}, which this adapter uses. Refusing to start: " +
                "each message would fail to record and be consumed again. The store was provisioned for an earlier " +
                "adapter version, or the connection's search_path or schema USAGE grant hides it; check those first, " +
                "so the statements below don't create a second set of tables in another schema. Bring the store to the " +
                $"store schema for contract major {SupportedContractMajor} before starting: {string.Join("; ", statements)}." +
                (followUps.Count > 0
                    ? $" Then give existing rows a value in each added column and run: {string.Join("; ", followUps)}."
                    : string.Empty));
        }

        logger.LogInformation("Message Store tables and columns verified.");
    }

    public Task RecordAttemptAsync(PlatformMessage message, string routeId, string status, string? error, CancellationToken cancellationToken)
    {
        return RecordAttemptAsync(
            message,
            new PlatformMessageAttemptMetadata(routeId),
            status,
            error,
            cancellationToken);
    }

    public async Task RecordAttemptAsync(
        PlatformMessage message,
        PlatformMessageAttemptMetadata metadata,
        string status,
        string? error,
        CancellationToken cancellationToken)
    {
        // The attempt number comes from the x-bip-attempt header of the
        // consumed message (absent => 1) and is never derived from the database.
        var attempt = AttemptFrom(message);
        var payload = CapturePayload(message);
        // steps_json records what THIS processing attempt observed, not a
        // cumulative snapshot of bip_step_outcome.
        var stepsJson = JsonSerializer.Serialize(stepObservations.Snapshot(message.MessageId), JsonOptions);

        // The outcome is kept here and its idempotent write retried in place,
        // so a short store failure, or a commit the store could not confirm as
        // durable, doesn't make the delivery run again.
        for (var write = 1; ; write++)
        {
            try
            {
                await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
                using var confirmation = PlatformCommitConfirmation.Watch(connection);
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
                var capped = await CapPayloadExpiryAsync(connection, transaction, message.MessageId, payload, cancellationToken);
                await UpsertAttemptAsync(connection, transaction, message, metadata, status, attempt, capped, error, stepsJson, cancellationToken);
                await UpsertLogAsync(connection, transaction, message, status, attempt, capped, error, metadata, cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                confirmation.ThrowIfUnconfirmed();
                break;
            }
            catch (Exception ex) when (write < OutcomeWriteAttempts && !(ex is OperationCanceledException && cancellationToken.IsCancellationRequested))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }

                logger.LogWarning(
                    "Message Store outcome write failed messageId={MessageId} status={Status} write={Write} ({ExceptionType}); retrying.",
                    message.MessageId,
                    status,
                    write,
                    ex.GetType().Name);
                await Task.Delay(OutcomeWriteRetryDelay * write, cancellationToken);
            }
        }

        stepObservations.Clear(message.MessageId);
    }

    /// <summary>Attempts of an outcome write before its store failure is thrown.</summary>
    internal int OutcomeWriteAttempts { get; init; } = 3;

    /// <summary>Delay before the second outcome write attempt; it grows with each attempt.</summary>
    internal TimeSpan OutcomeWriteRetryDelay { get; init; } = TimeSpan.FromMilliseconds(500);

    public async Task<MessageReplayResult> ReplayAsync(
        string messageId,
        MessageReplayRequest request,
        IProducer<string, byte[]> producer,
        CancellationToken cancellationToken)
    {
        var stored = await LoadStoredMessageAsync(messageId, cancellationToken);
        if (stored is null)
        {
            return new MessageReplayResult(
                "FAILED",
                messageId,
                options.ApplicationName,
                ResolveReplayTopic(options.ApplicationName),
                0,
                Error: "Message not found.",
                ErrorCode: MessageReplayErrorCodes.NotFound);
        }

        if (!string.Equals(stored.Integration, options.ApplicationName, StringComparison.Ordinal))
        {
            // A shared Message Store holds other integrations' messages too.
            // Only the integration that journaled a message may replay it, so
            // refuse before any audit row or produce, as the Camel adapter does.
            return new MessageReplayResult(
                "FAILED",
                messageId,
                options.ApplicationName,
                ResolveReplayTopic(options.ApplicationName),
                0,
                Error: "Message is not replayable by this integration.",
                ErrorCode: MessageReplayErrorCodes.NotReplayable);
        }

        var targetTopic = ResolveReplayTopic(stored.Integration);
        var nextAttempt = stored.LastAttempt + 1;
        var replayValidationError = ReplayValidationError(stored);
        if (replayValidationError is not null)
        {
            // Not replayable: rejected before any audit row or produce.
            return new MessageReplayResult(
                "FAILED",
                messageId,
                stored.Integration,
                targetTopic,
                nextAttempt,
                Error: replayValidationError,
                ErrorCode: MessageReplayErrorCodes.NotReplayable);
        }

        if (!options.UsesStepLedger && request.RedoSteps?.Any(step => !string.IsNullOrWhiteSpace(step)) == true)
        {
            // With the noop step store every step runs on every replay, so a
            // redo request would promise a protection that does not exist.
            // Refused before any audit row or produce, as the Camel adapter does.
            return new MessageReplayResult(
                "FAILED",
                messageId,
                stored.Integration,
                targetTopic,
                nextAttempt,
                Error: "redoSteps needs the postgres step outcome store; with BASIS_MESSAGE_STORE_STEP_OUTCOME_STORE=noop every step runs on every replay. Resend without redoSteps.",
                ErrorCode: MessageReplayErrorCodes.NotReplayable);
        }

        // Audit ordering: commit the REQUESTED audit row on its own
        // connection/transaction BEFORE the Kafka produce. No database lock or
        // open transaction spans the produce call.
        var replayId = Guid.NewGuid().ToString();
        await InsertReplayAuditAsync(stored.Integration, messageId, replayId, nextAttempt, request, targetTopic, cancellationToken);

        try
        {
            await producer.ProduceAsync(
                targetTopic,
                new Message<string, byte[]>
                {
                    Key = stored.CorrelationId,
                    Value = stored.PayloadBytes!,
                    Headers = CreateReplayHeaders(stored, request, nextAttempt),
                },
                cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "Replay publish failed messageId={MessageId} replayId={ReplayId}", messageId, replayId);
            await UpdateReplayAuditAsync(replayId, "FAILED", ex.Message, cancellationToken);
            return new MessageReplayResult(
                "FAILED",
                messageId,
                stored.Integration,
                targetTopic,
                nextAttempt,
                replayId,
                ex.Message,
                MessageReplayErrorCodes.PublishFailed);
        }

        // Post-produce status update runs on a separate connection/transaction.
        // A crash between produce and this update leaves an honest REQUESTED row.
        await UpdateReplayAuditAsync(replayId, "PUBLISHED", null, cancellationToken);
        return new MessageReplayResult("PUBLISHED", messageId, stored.Integration, targetTopic, nextAttempt, replayId);
    }

    internal static int AttemptFrom(PlatformMessage message)
    {
        if (message.Headers.TryGetValue("x-bip-attempt", out var raw)
            && int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var attempt)
            && attempt >= 1)
        {
            return attempt;
        }

        return 1;
    }

    internal PayloadCapture CapturePayload(PlatformMessage message)
    {
        // Payload policy, identical semantics to the Camel adapter.
        var sizeBytes = (long)message.Payload.Length;
        if (options.PayloadMode == PlatformMessageStoreOptions.PayloadModeMetadataOnly)
        {
            return PayloadCapture.MetadataOnly(sizeBytes, "payload storage mode is metadata-only");
        }

        if (sizeBytes > options.PayloadMaxBytes)
        {
            return PayloadCapture.MetadataOnly(sizeBytes, "payload exceeds max inline size");
        }

        return PayloadCapture.Inline(message.Payload, sizeBytes, Clock.GetUtcNow().AddDays(options.PayloadRetentionDays));
    }

    private async Task<PayloadCapture> CapPayloadExpiryAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string messageId,
        PayloadCapture payload,
        CancellationToken cancellationToken)
    {
        if (payload.ExpiresAt is not { } requested)
        {
            return payload;
        }

        // Read before rewriting this attempt: the oldest recorded receipt for
        // this integration bounds every replay at twice the retention window.
        await using var command = new NpgsqlCommand(
            """
            select min(occurred_at) from bip_message_attempt
            where message_id = @message_id and integration = @integration
            """, connection, transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("integration", options.ApplicationName);
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow, cancellationToken);
        await reader.ReadAsync(cancellationToken);
        var firstReceipt = requested.AddDays(-options.PayloadRetentionDays);
        if (!reader.IsDBNull(0) && reader.GetFieldValue<DateTimeOffset>(0) < firstReceipt)
        {
            firstReceipt = reader.GetFieldValue<DateTimeOffset>(0);
        }
        var cap = firstReceipt.AddDays(2.0 * options.PayloadRetentionDays);
        return payload with { ExpiresAt = requested < cap ? requested : cap };
    }

    internal static Headers CreateReplayHeaders(ReplayableStoredMessage message, MessageReplayRequest request, int nextAttempt)
    {
        var headers = new Headers
        {
            { "x-bip-message-id", HeaderValue(message.MessageId) },
            { "x-bip-correlation-id", HeaderValue(message.CorrelationId) },
            { "x-bip-schema-version", HeaderValue(message.SchemaVersion.ToString(CultureInfo.InvariantCulture)) },
            { "x-bip-content-type", HeaderValue(message.ContentType) },
            { "content-type", HeaderValue(message.ContentType) },
            { "x-bip-source-system", HeaderValue(message.SourceSystem) },
            { "x-bip-target-system", HeaderValue(message.TargetSystem) },
            { "x-bip-replay", HeaderValue("true") },
            { "x-bip-attempt", HeaderValue(nextAttempt.ToString(CultureInfo.InvariantCulture)) },
            { "x-bip-redo-steps", HeaderValue(RedoStepsJson(request)) },
        };
        return headers;
    }

    private static byte[] HeaderValue(string value) => Encoding.UTF8.GetBytes(value);

    private static string? ReplayValidationError(ReplayableStoredMessage message)
    {
        // Replayable = payload captured AND not expired AND not deleted —
        // independent of terminal status.
        if (message.PayloadDeletedAt is not null)
        {
            return "Payload was deleted.";
        }

        if (message.PayloadExpiresAt is not null && message.PayloadExpiresAt.Value < DateTimeOffset.UtcNow)
        {
            return "Payload has expired.";
        }

        if (!message.Replayable)
        {
            return string.IsNullOrWhiteSpace(message.ReplayDisabledReason)
                ? "Payload is not replayable."
                : message.ReplayDisabledReason;
        }

        return message.PayloadBytes is null ? "Payload bytes are missing." : null;
    }

    private string ResolveReplayTopic(string integration)
    {
        if (!string.IsNullOrWhiteSpace(options.ReplayInputTopicPattern))
        {
            return options.ReplayInputTopicPattern.Contains("%s", StringComparison.Ordinal)
                ? options.ReplayInputTopicPattern.Replace("%s", integration, StringComparison.Ordinal)
                : options.ReplayInputTopicPattern;
        }

        return string.IsNullOrWhiteSpace(options.InputTopic) ? $"bip.{integration}.in" : options.InputTopic;
    }

    private async Task<ReplayableStoredMessage?> LoadStoredMessageAsync(string messageId, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var load = new NpgsqlCommand(
            """
            select message_id,
                   correlation_id,
                   content_type,
                   source_system,
                   target_system,
                   integration,
                   schema_version,
                   last_attempt,
                   payload_bytes,
                   replayable,
                   replay_disabled_reason,
                   payload_expires_at,
                   payload_deleted_at
            from bip_message_log
            where message_id = @message_id
            """,
            connection);
        load.Parameters.AddWithValue("message_id", messageId);
        await using var reader = await load.ExecuteReaderAsync(CommandBehavior.SingleRow, cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new ReplayableStoredMessage(
            reader.GetString(0),
            reader.IsDBNull(1) ? messageId : reader.GetString(1),
            reader.IsDBNull(2) ? "application/octet-stream" : reader.GetString(2),
            reader.IsDBNull(3) ? "unknown" : reader.GetString(3),
            reader.IsDBNull(4) ? options.ApplicationName : reader.GetString(4),
            reader.GetString(5),
            reader.GetInt32(6),
            reader.GetInt32(7),
            reader.IsDBNull(8) ? null : (byte[])reader[8],
            reader.GetBoolean(9),
            reader.IsDBNull(10) ? null : reader.GetString(10),
            reader.IsDBNull(11) ? null : reader.GetFieldValue<DateTimeOffset>(11),
            reader.IsDBNull(12) ? null : reader.GetFieldValue<DateTimeOffset>(12));
    }

    private async Task UpsertLogAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        PlatformMessage message,
        string status,
        int attempt,
        PayloadCapture payload,
        string? error,
        PlatformMessageAttemptMetadata metadata,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            insert into bip_message_log (
              message_id, correlation_id, integration, schema_version, status, last_attempt,
              occurred_at, updated_at, source_system, target_system, content_type, payload_storage,
              payload_size_bytes, payload_bytes, payload_expires_at, replayable,
              replay_disabled_reason, auto_redelivered, auto_redelivery_count,
              auto_redelivery_max, error
            )
            values (
              @message_id, @correlation_id, @integration, @schema_version, @status, @attempt,
              now(), now(), @source_system, @target_system, @content_type, @payload_storage,
              @payload_size_bytes, @payload_bytes, @payload_expires_at, @replayable,
              @replay_disabled_reason, @auto_redelivered, @auto_redelivery_count,
              @auto_redelivery_max, @error
            )
            on conflict (message_id) do update set
              schema_version = excluded.schema_version,
              status = excluded.status,
              last_attempt = excluded.last_attempt,
              updated_at = now(),
              payload_storage = excluded.payload_storage,
              payload_size_bytes = excluded.payload_size_bytes,
              payload_bytes = excluded.payload_bytes,
              payload_expires_at = excluded.payload_expires_at,
              replayable = excluded.replayable,
              replay_disabled_reason = excluded.replay_disabled_reason,
              auto_redelivered = excluded.auto_redelivered,
              auto_redelivery_count = excluded.auto_redelivery_count,
              auto_redelivery_max = excluded.auto_redelivery_max,
              error = excluded.error
            where excluded.last_attempt >= bip_message_log.last_attempt
            """,
            connection,
            transaction);
        AddMessageParameters(command, message);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("attempt", attempt);
        // schema_version is the producer-declared x-bip-schema-version
        // (default 1), never a hardcoded constant.
        command.Parameters.AddWithValue("schema_version", message.SchemaVersion);
        AddPayloadParameters(command, payload);
        command.Parameters.AddWithValue("auto_redelivered", metadata.AutoRedelivered);
        AddNullableInt(command, "auto_redelivery_count", metadata.AutoRedeliveryCount);
        AddNullableInt(command, "auto_redelivery_max", metadata.AutoRedeliveryMax);
        AddNullableText(command, "error", error);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task UpsertAttemptAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        PlatformMessage message,
        PlatformMessageAttemptMetadata metadata,
        string status,
        int attempt,
        PayloadCapture payload,
        string? error,
        string stepsJson,
        CancellationToken cancellationToken)
    {
        // Infra redeliveries re-record the same (message_id, attempt) row
        // instead of appending a new attempt or failing on the primary key.
        await using var command = new NpgsqlCommand(
            """
            insert into bip_message_attempt (
              message_id, attempt, schema_version, correlation_id, integration, route_id, status,
              occurred_at, source_system, target_system, content_type, payload_size_bytes,
              payload_storage, payload_bytes, payload_expires_at, replayable,
              replay_disabled_reason, auto_redelivered, auto_redelivery_count,
              auto_redelivery_max, steps_json, error
            )
            values (
              @message_id, @attempt, @schema_version, @correlation_id, @integration, @route_id, @status,
              now(), @source_system, @target_system, @content_type, @payload_size_bytes,
              @payload_storage, @payload_bytes, @payload_expires_at, @replayable,
              @replay_disabled_reason, @auto_redelivered, @auto_redelivery_count,
              @auto_redelivery_max, @steps_json, @error
            )
            on conflict (message_id, attempt) do update set
              schema_version = excluded.schema_version,
              correlation_id = excluded.correlation_id,
              content_type = excluded.content_type,
              status = excluded.status,
              route_id = excluded.route_id,
              occurred_at = excluded.occurred_at,
              payload_storage = excluded.payload_storage,
              payload_size_bytes = excluded.payload_size_bytes,
              payload_bytes = excluded.payload_bytes,
              payload_expires_at = excluded.payload_expires_at,
              replayable = excluded.replayable,
              replay_disabled_reason = excluded.replay_disabled_reason,
              auto_redelivered = excluded.auto_redelivered,
              auto_redelivery_count = excluded.auto_redelivery_count,
              auto_redelivery_max = excluded.auto_redelivery_max,
              steps_json = excluded.steps_json,
              error = excluded.error
            """,
            connection,
            transaction);
        AddMessageParameters(command, message);
        command.Parameters.AddWithValue("attempt", attempt);
        command.Parameters.AddWithValue("route_id", metadata.RouteId);
        command.Parameters.AddWithValue("status", status);
        // schema_version is the producer-declared x-bip-schema-version
        // (default 1), never a hardcoded constant.
        command.Parameters.AddWithValue("schema_version", message.SchemaVersion);
        AddPayloadParameters(command, payload);
        command.Parameters.AddWithValue("auto_redelivered", metadata.AutoRedelivered);
        AddNullableInt(command, "auto_redelivery_count", metadata.AutoRedeliveryCount);
        AddNullableInt(command, "auto_redelivery_max", metadata.AutoRedeliveryMax);
        command.Parameters.Add("steps_json", NpgsqlDbType.Jsonb).Value = stepsJson;
        AddNullableText(command, "error", error);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task InsertReplayAuditAsync(
        string integration,
        string messageId,
        string replayId,
        int replayAttempt,
        MessageReplayRequest request,
        string targetTopic,
        CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        // The REQUESTED row must be durable before the produce.
        using var confirmation = PlatformCommitConfirmation.Watch(connection);
        await using var command = new NpgsqlCommand(
            """
            insert into bip_message_replay_audit (
              replay_id, message_id, integration, replay_attempt, requested_by, reason,
              redo_steps, target_topic, status, requested_at, error
            )
            values (
              @replay_id, @message_id, @integration, @replay_attempt, @requested_by, @reason,
              @redo_steps, @target_topic, 'REQUESTED', now(), null
            )
            """,
            connection);
        command.Parameters.AddWithValue("replay_id", Guid.Parse(replayId));
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("integration", integration);
        command.Parameters.AddWithValue("replay_attempt", replayAttempt);
        AddNullableText(command, "requested_by", request.RequestedBy);
        AddNullableText(command, "reason", request.Reason);
        command.Parameters.Add("redo_steps", NpgsqlDbType.Jsonb).Value = RedoStepsJson(request);
        command.Parameters.AddWithValue("target_topic", targetTopic);
        await command.ExecuteNonQueryAsync(cancellationToken);
        confirmation.ThrowIfUnconfirmed();
    }

    private async Task UpdateReplayAuditAsync(string replayId, string status, string? error, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            update bip_message_replay_audit
            set status = @status, published_at = case when @status = 'PUBLISHED' then now() else published_at end, error = @error
            where replay_id = @replay_id
            """,
            connection);
        command.Parameters.AddWithValue("replay_id", Guid.Parse(replayId));
        command.Parameters.AddWithValue("status", status);
        AddNullableText(command, "error", error);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string RedoStepsJson(MessageReplayRequest request)
    {
        return JsonSerializer.Serialize(request.RedoSteps ?? [], JsonOptions);
    }

    private void AddMessageParameters(NpgsqlCommand command, PlatformMessage message)
    {
        command.Parameters.AddWithValue("message_id", message.MessageId);
        command.Parameters.AddWithValue("correlation_id", message.CorrelationId);
        command.Parameters.AddWithValue("integration", options.ApplicationName);
        command.Parameters.AddWithValue("source_system", message.SourceSystem);
        command.Parameters.AddWithValue("target_system", message.TargetSystem);
        command.Parameters.AddWithValue("content_type", message.ContentType);
    }

    private static void AddPayloadParameters(NpgsqlCommand command, PayloadCapture payload)
    {
        command.Parameters.AddWithValue("payload_storage", payload.Storage);
        command.Parameters.AddWithValue("payload_size_bytes", payload.SizeBytes);
        command.Parameters.Add("payload_bytes", NpgsqlDbType.Bytea).Value = payload.Bytes is null ? DBNull.Value : payload.Bytes;
        command.Parameters.Add("payload_expires_at", NpgsqlDbType.TimestampTz).Value =
            payload.ExpiresAt.HasValue ? payload.ExpiresAt.Value : DBNull.Value;
        command.Parameters.AddWithValue("replayable", payload.Replayable);
        AddNullableText(command, "replay_disabled_reason", payload.ReplayDisabledReason);
    }

    internal static void AddNullableText(NpgsqlCommand command, string name, string? value)
    {
        command.Parameters.Add(name, NpgsqlDbType.Text).Value =
            string.IsNullOrWhiteSpace(value) ? DBNull.Value : value;
    }

    private static void AddNullableInt(NpgsqlCommand command, string name, int? value)
    {
        command.Parameters.Add(name, NpgsqlDbType.Integer).Value = value.HasValue ? value.Value : (object)DBNull.Value;
    }
}

public sealed record PlatformMessageAttemptMetadata(
    string RouteId,
    bool AutoRedelivered = false,
    int? AutoRedeliveryCount = null,
    int? AutoRedeliveryMax = null);

/// <summary>
/// Result of the payload policy for one recorded message.
/// metadata_only rows store no payload bytes and are never replayable.
/// </summary>
internal sealed record PayloadCapture(
    string Storage,
    long SizeBytes,
    byte[]? Bytes,
    DateTimeOffset? ExpiresAt,
    bool Replayable,
    string? ReplayDisabledReason)
{
    public static PayloadCapture Inline(byte[] bytes, long sizeBytes, DateTimeOffset expiresAt) =>
        new(PlatformMessageStoreOptions.PayloadModePostgresInline, sizeBytes, bytes, expiresAt, true, null);

    public static PayloadCapture MetadataOnly(long sizeBytes, string reason) =>
        new(PlatformMessageStoreOptions.PayloadModeMetadataOnly, sizeBytes, null, null, false, reason);
}

internal sealed record ReplayableStoredMessage(
    string MessageId,
    string CorrelationId,
    string ContentType,
    string SourceSystem,
    string TargetSystem,
    string Integration,
    int SchemaVersion,
    int LastAttempt,
    byte[]? PayloadBytes,
    bool Replayable,
    string? ReplayDisabledReason,
    DateTimeOffset? PayloadExpiresAt,
    DateTimeOffset? PayloadDeletedAt);
