using System.Collections.Concurrent;

namespace Basis.IntegrationPlatform.MessageStore;

/// <summary>
/// One step outcome as observed by the current processing attempt
/// (contract v1 DECISION D4). Serialized into bip_message_attempt.steps_json
/// with the exact camelCase field set of spec/schemas/steps-json.schema.json.
/// </summary>
public sealed record PlatformStepObservation(
    string Step,
    string Status,
    string IdempotencyKey,
    string? EndpointUri,
    string? TargetRef,
    string? Error,
    DateTimeOffset? OccurredAt);

/// <summary>
/// Collects, per in-flight message, the step outcomes observed by the current
/// processing attempt. The step executor reports every DELIVERED/FAILED/SKIPPED
/// outcome here; the attempt-record path snapshots the collection into the
/// bip_message_attempt.steps_json column and clears it after a successful write.
/// This intentionally does NOT mirror the cumulative bip_step_outcome ledger.
/// </summary>
public sealed class PlatformStepObservationCollector
{
    private readonly ConcurrentDictionary<string, List<PlatformStepObservation>> observations = new(StringComparer.Ordinal);
    // Index of the first observation made by the current handler invocation.
    // In-process retries re-run the handler within one processing attempt, so
    // a step seen again in a later invocation is a retry, not a reused id.
    private readonly ConcurrentDictionary<string, int> invocationStarts = new(StringComparer.Ordinal);

    public void Record(string messageId, PlatformStepObservation observation)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentNullException.ThrowIfNull(observation);
        var list = observations.GetOrAdd(messageId, _ => []);
        lock (list)
        {
            list.Add(observation);
        }
    }

    public IReadOnlyList<PlatformStepObservation> Snapshot(string messageId)
    {
        if (!observations.TryGetValue(messageId, out var list))
        {
            return [];
        }

        lock (list)
        {
            return list.ToArray();
        }
    }

    public void Clear(string messageId)
    {
        observations.TryRemove(messageId, out _);
        invocationStarts.TryRemove(messageId, out _);
    }

    internal void BeginHandlerInvocation(string messageId)
    {
        invocationStarts[messageId] = Snapshot(messageId).Count;
    }

    /// <summary>
    /// True when the current handler invocation already delivered or skipped
    /// this idempotency key, so a further call with it is skipped as well.
    /// </summary>
    internal bool CompletedInCurrentInvocation(string messageId, string idempotencyKey)
    {
        var start = invocationStarts.TryGetValue(messageId, out var index) ? index : 0;
        return Snapshot(messageId)
            .Skip(start)
            .Any(observation => string.Equals(observation.IdempotencyKey, idempotencyKey, StringComparison.Ordinal)
                && observation.Status is "DELIVERED" or "SKIPPED");
    }
}
