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
    }
}
