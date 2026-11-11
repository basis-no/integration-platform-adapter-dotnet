using System.Globalization;
using Microsoft.Extensions.Logging;

namespace Basis.IntegrationPlatform.MessageStore;

/// <summary>A consumed record's topic, partition and offset.</summary>
internal sealed record PlatformRecordPosition(string Topic, int Partition, long Offset)
{
    public override string ToString() =>
        string.Create(CultureInfo.InvariantCulture, $"{Topic}/{Partition}@{Offset}");
}

/// <summary>
/// The processing of one consumed record, its in-process handler retries
/// included, as the step executor sees it. The consumer makes it current
/// around the handler, and it flows with the handler's async calls, so steps
/// run in parallel and a step the handler runs with a message id of its own
/// belong to it too. A step executor call outside it, such as one from an
/// application's own endpoint, sees none.
/// </summary>
internal sealed class PlatformDelivery
{
    private static readonly AsyncLocal<PlatformDelivery?> CurrentDelivery = new();

    private readonly object gate = new();
    private readonly HashSet<string> marksTaken = new(StringComparer.Ordinal);
    private readonly List<string> unrecordedSends = [];
    private Exception? storeFailure;
    private bool onlyRecordData = true;
    private volatile bool callerCancelled;
    private volatile bool ended;

    internal PlatformDelivery(PlatformRecordPosition? position, PlatformUnrecordedDeliveries? marks)
    {
        Position = position;
        Marks = marks;
    }

    /// <summary>The processing the calling code belongs to, or null outside one or after it ended.</summary>
    internal static PlatformDelivery? Current => CurrentDelivery.Value is { ended: false } delivery ? delivery : null;

    /// <summary>The record's position; null when the record was not consumed by the adapter.</summary>
    internal PlatformRecordPosition? Position { get; }

    internal PlatformUnrecordedDeliveries? Marks { get; }

    /// <summary>The first step outcome check or write of this processing that failed.</summary>
    internal Exception? StoreFailure
    {
        get
        {
            lock (gate)
            {
                return storeFailure;
            }
        }
    }

    /// <summary>
    /// Whether every step outcome failure of this processing was the record's
    /// own data; the first one alone does not tell, since parallel steps can
    /// fail for other reasons.
    /// </summary>
    internal bool StoreFailedOnlyForRecordData
    {
        get
        {
            lock (gate)
            {
                return storeFailure is not null && onlyRecordData;
            }
        }
    }

    /// <summary>The steps whose send returned in this processing, or in an earlier read, without a written outcome.</summary>
    internal IReadOnlyList<string> UnrecordedSends
    {
        get
        {
            lock (gate)
            {
                return unrecordedSends.ToArray();
            }
        }
    }

    /// <summary>Makes this processing current for the calling async flow.</summary>
    internal void Begin() => CurrentDelivery.Value = this;

    /// <summary>Ends it, also for a step the handler started and did not await.</summary>
    internal void End()
    {
        ended = true;
        CurrentDelivery.Value = null;
    }

    internal void NoteStoreFailure(Exception failure)
    {
        lock (gate)
        {
            storeFailure ??= failure;
            onlyRecordData &= PlatformStoreFailures.IsRecordData(failure);
        }
    }

    /// <summary>
    /// Whether, in the current handler invocation, a step's ledger check was
    /// not made because the handler's own cancellation token was already
    /// cancelled: no store failure, but that step did not run.
    /// </summary>
    internal bool CallerCancelled => callerCancelled;

    internal void NoteCallerCancellation() => callerCancelled = true;

    /// <summary>A handler retry runs every step again, so what a cancelled token stopped before no longer counts.</summary>
    internal void BeginHandlerInvocation() => callerCancelled = false;

    internal void NoteUnrecordedSend(string stepId)
    {
        lock (gate)
        {
            if (!unrecordedSends.Contains(stepId))
            {
                unrecordedSends.Add(stepId);
            }
        }
    }

    /// <summary>
    /// Takes a mark's send for this processing, in one step with
    /// <paramref name="taken"/>, so that of several calls with the mark's step
    /// key, also parallel ones, only the first is the send from the mark.
    /// </summary>
    internal bool TryTakeMark(string idempotencyKey, Action taken)
    {
        lock (gate)
        {
            if (!marksTaken.Add(idempotencyKey))
            {
                return false;
            }

            taken();
            return true;
        }
    }
}

/// <summary>
/// A send that returned, but whose <c>DELIVERED</c> outcome could not be
/// written: the outcome the write would have stored, and its record.
/// </summary>
internal sealed record PlatformUnrecordedDelivery(
    PlatformRecordPosition Position,
    string MessageId,
    string StepId,
    string IdempotencyKey,
    string EndpointUri,
    string? TargetRef,
    DateTimeOffset SentAt);

/// <summary>
/// Sends that returned but whose <c>DELIVERED</c> outcome could not be
/// written, kept in memory: a mark per record position and step key. A later
/// read of the same record writes the outcome from its mark instead of sending
/// again. A mark stays until a record at or after its offset is committed, or a
/// record at another offset begins on its partition; writing it again is
/// harmless. Marks are lost on a restart or a rebalance to another process.
/// </summary>
internal sealed class PlatformUnrecordedDeliveries(ILogger logger)
{
    internal const int MaxMarks = 10_000;

    private readonly object gate = new();
    // Oldest first, so the bound drops the oldest mark.
    private readonly LinkedList<PlatformUnrecordedDelivery> order = new();
    private readonly Dictionary<(PlatformRecordPosition Position, string IdempotencyKey), LinkedListNode<PlatformUnrecordedDelivery>> marks = new();

    internal int Count
    {
        get
        {
            lock (gate)
            {
                return marks.Count;
            }
        }
    }

    internal void Mark(PlatformUnrecordedDelivery delivery)
    {
        lock (gate)
        {
            var key = (delivery.Position, delivery.IdempotencyKey);
            if (marks.Remove(key, out var previous))
            {
                order.Remove(previous);
            }

            marks[key] = order.AddLast(delivery);
            while (marks.Count > MaxMarks && order.First is { } oldest)
            {
                order.RemoveFirst();
                marks.Remove((oldest.Value.Position, oldest.Value.IdempotencyKey));
                logger.LogWarning(
                    "Dropped the unwritten DELIVERED outcome of step {StepId} for record {Position}: more than {MaxMarks} such outcomes are kept; that record's next read sends the step again",
                    oldest.Value.StepId,
                    oldest.Value.Position,
                    MaxMarks);
            }
        }
    }

    /// <summary>The mark of this record and step key, if an earlier read left one.</summary>
    internal PlatformUnrecordedDelivery? Find(PlatformRecordPosition position, string idempotencyKey)
    {
        lock (gate)
        {
            return marks.TryGetValue((position, idempotencyKey), out var node) ? node.Value : null;
        }
    }

    /// <summary>
    /// Drops the marks of the record's partition that it supersedes: on a
    /// commit, those at or before the committed offset; when a record begins,
    /// those at any other offset.
    /// </summary>
    internal void Supersede(PlatformRecordPosition position, bool committed)
    {
        lock (gate)
        {
            for (var node = order.First; node is not null;)
            {
                var next = node.Next;
                var mark = node.Value.Position;
                if (mark.Topic == position.Topic && mark.Partition == position.Partition
                    && (committed ? mark.Offset <= position.Offset : mark.Offset != position.Offset))
                {
                    order.Remove(node);
                    marks.Remove((mark, node.Value.IdempotencyKey));
                }

                node = next;
            }
        }
    }
}
