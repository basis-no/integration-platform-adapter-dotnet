using System.Text.Json;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Basis.IntegrationPlatform.MessageStore;

public interface IPlatformStepExecutor
{
    Task<PlatformStepExecutionResult> ExecuteAsync(
        PlatformMessage message,
        string stepId,
        string endpointUri,
        string? targetRef,
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken);
}

public sealed record PlatformStepExecutionResult(
    string IdempotencyKey,
    string StepId,
    string Status,
    bool Skipped);

public sealed class PlatformStepExecutor : IPlatformStepExecutor
{
    private const string Delivered = "DELIVERED";
    private const string Failed = "FAILED";
    private const string SkippedStatus = "SKIPPED";
    private const string RedoStepsHeader = "x-bip-redo-steps";

    private readonly NpgsqlDataSource dataSource;
    private readonly PlatformStepObservationCollector observations;
    private readonly ILogger<PlatformStepExecutor> logger;

    public PlatformStepExecutor(
        NpgsqlDataSource dataSource,
        PlatformStepObservationCollector observations,
        ILogger<PlatformStepExecutor> logger)
    {
        this.dataSource = dataSource;
        this.observations = observations;
        this.logger = logger;
    }

    public async Task<PlatformStepExecutionResult> ExecuteAsync(
        PlatformMessage message,
        string stepId,
        string endpointUri,
        string? targetRef,
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(stepId);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpointUri);
        ArgumentNullException.ThrowIfNull(operation);

        var idempotencyKey = IdempotencyKey(message.MessageId, stepId);
        if (!ShouldRedo(message, stepId, idempotencyKey, logger) && await WasDeliveredAsync(idempotencyKey, cancellationToken))
        {
            await WarnAboutSharedStepKeyAsync(message, stepId, idempotencyKey, cancellationToken);
            Observe(message.MessageId, stepId, SkippedStatus, idempotencyKey, endpointUri, targetRef, null);
            return new PlatformStepExecutionResult(idempotencyKey, stepId, SkippedStatus, true);
        }

        try
        {
            await operation(cancellationToken);
            await RecordAsync(message.MessageId, idempotencyKey, stepId, Delivered, endpointUri, targetRef, null, cancellationToken);
            Observe(message.MessageId, stepId, Delivered, idempotencyKey, endpointUri, targetRef, null);
            return new PlatformStepExecutionResult(idempotencyKey, stepId, Delivered, false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await RecordAsync(message.MessageId, idempotencyKey, stepId, Failed, endpointUri, targetRef, ex.Message, cancellationToken);
            Observe(message.MessageId, stepId, Failed, idempotencyKey, endpointUri, targetRef, ex.Message);
            throw;
        }
    }

    internal static string IdempotencyKey(string messageId, string stepId) => $"{messageId}:{stepId}";

    /// <summary>
    /// The key is <c>messageId:stepId</c> (Contract v1 §7) and bip_step_outcome
    /// has no integration column, so a DELIVERED row may not be this call's own
    /// delivery. Warn on the two ways that happens; the skip itself is unchanged.
    /// </summary>
    private async Task WarnAboutSharedStepKeyAsync(
        PlatformMessage message,
        string stepId,
        string idempotencyKey,
        CancellationToken cancellationToken)
    {
        if (observations.CompletedInCurrentInvocation(message.MessageId, idempotencyKey))
        {
            logger.LogWarning(
                "Step {StepId} already ran in this handler invocation and is skipped messageId={MessageId}. "
                + "The step key is <message-id>:<step-id>; a handler that sends several items through one step needs a distinct step id per item.",
                stepId,
                message.MessageId);
        }

        string? journaledBy;
        try
        {
            journaledBy = await JournaledIntegrationAsync(message.MessageId, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The lookup only feeds a warning: a failed read must not turn a
            // correct skip into a step failure.
            logger.LogDebug(ex, "Shared step key check skipped messageId={MessageId} step={StepId}", message.MessageId, stepId);
            return;
        }

        if (journaledBy is not null && !string.Equals(journaledBy, message.ApplicationName, StringComparison.Ordinal))
        {
            logger.LogWarning(
                "Step {StepId} is skipped as already delivered, but messageId={MessageId} is journaled by integration {JournaledBy}, not {Integration}. "
                + "Step outcomes are not scoped by integration, so the delivery may be the other integration's. "
                + "Use a new x-bip-message-id per integration, or step ids unique across integrations.",
                stepId,
                message.MessageId,
                journaledBy,
                message.ApplicationName);
        }
    }

    private void Observe(string messageId, string stepId, string status, string idempotencyKey, string endpointUri, string? targetRef, string? error)
    {
        observations.Record(messageId, new PlatformStepObservation(
            stepId,
            status,
            idempotencyKey,
            endpointUri,
            targetRef,
            error,
            DateTimeOffset.UtcNow));
    }

    internal static bool ShouldRedo(PlatformMessage message, string stepId, string idempotencyKey, ILogger logger)
    {
        if (!message.Headers.TryGetValue(RedoStepsHeader, out var redoSteps) || string.IsNullOrWhiteSpace(redoSteps))
        {
            return false;
        }

        try
        {
            var steps = JsonSerializer.Deserialize<List<string>>(redoSteps);
            return steps is not null
                && steps.Any(step => string.Equals(step, stepId, StringComparison.Ordinal)
                    || string.Equals(step, idempotencyKey, StringComparison.Ordinal));
        }
        catch (JsonException ex)
        {
            logger.LogWarning(
                ex,
                "Malformed {Header} header value treated as empty (expected a JSON array of step ids) messageId={MessageId} value={Value}",
                RedoStepsHeader,
                message.MessageId,
                redoSteps);
            return false;
        }
    }

    private async Task<bool> WasDeliveredAsync(string idempotencyKey, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            select 1
            from bip_step_outcome
            where idempotency_key = @idempotency_key
              and status = 'DELIVERED'
            """,
            connection);
        command.Parameters.AddWithValue("idempotency_key", idempotencyKey);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is not null;
    }

    private async Task<string?> JournaledIntegrationAsync(string messageId, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            select integration
            from bip_message_log
            where message_id = @message_id
            """,
            connection);
        command.Parameters.AddWithValue("message_id", messageId);
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    private async Task RecordAsync(
        string messageId,
        string idempotencyKey,
        string stepId,
        string status,
        string endpointUri,
        string? targetRef,
        string? error,
        CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            insert into bip_step_outcome (
                idempotency_key,
                message_id,
                step_id,
                status,
                endpoint_uri,
                target_ref,
                error,
                occurred_at,
                updated_at
            ) values (
                @idempotency_key,
                @message_id,
                @step_id,
                @status,
                @endpoint_uri,
                @target_ref,
                @error,
                now(),
                now()
            )
            on conflict (idempotency_key) do update
            set message_id = excluded.message_id,
                step_id = excluded.step_id,
                status = excluded.status,
                endpoint_uri = excluded.endpoint_uri,
                target_ref = excluded.target_ref,
                error = excluded.error,
                occurred_at = excluded.occurred_at,
                updated_at = current_timestamp
            where bip_step_outcome.status <> 'DELIVERED'
               or excluded.status = 'DELIVERED'
            """,
            connection);
        command.Parameters.AddWithValue("idempotency_key", idempotencyKey);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("step_id", stepId);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("endpoint_uri", endpointUri);
        PlatformMessageStore.AddNullableText(command, "target_ref", targetRef);
        PlatformMessageStore.AddNullableText(command, "error", error);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
