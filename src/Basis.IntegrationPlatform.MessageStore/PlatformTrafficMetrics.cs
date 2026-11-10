using System.Diagnostics;
using System.Diagnostics.Metrics;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Basis.IntegrationPlatform.MessageStore;

internal sealed class PlatformTrafficMetrics : IDisposable
{
    internal const string MeterName = "Basis.IntegrationPlatform.MessageStore";
    internal const string Admitted = "record_admitted";
    internal const string Published = "record_published";
    private const long MaxExact = (1L << 53) - 1;

    private readonly Meter meter = new(MeterName);
    private readonly ILogger logger;
    private readonly Signal admitted;
    private readonly Signal published;
    private readonly HashSet<string> inputTopics;
    private readonly string consumerTopic;
    private int consumerReady;
    private int producerReady;
    private int instrumentsReady;

    internal PlatformTrafficMetrics(PlatformMessageStoreOptions options, ILogger<PlatformTrafficMetrics>? logger = null)
    {
        this.logger = logger ?? NullLogger<PlatformTrafficMetrics>.Instance;
        var selected = Split(options.TrafficSignals);
        var valid = selected.All(s => s is Admitted or Published);
        admitted = new Signal(Admitted, valid && selected.Contains(Admitted));
        published = new Signal(Published, valid && selected.Contains(Published));
        inputTopics = Split(options.TrafficInputTopics);
        consumerTopic = options.InputTopic;
        if (!valid)
        {
            this.logger.LogWarning(
                "BASIS_TRAFFIC_SIGNALS selects an unknown signal; Kafka activity coverage is unknown. Use record_admitted, record_published or both.");
        }

        if (inputTopics.Count == 0 || inputTopics.Any(t => !ValidTopic(t))) Invalidate(published, "BASIS_TRAFFIC_INPUT_TOPICS is empty or names an invalid topic");
        if (selected.Count == 0)
        {
            // Disabled export registers no instruments, as in the Camel adapter.
            return;
        }

        try
        {
            meter.CreateObservableCounter<long>("integration_traffic_events_total", () => new[]
            {
                new Measurement<long>(Interlocked.Read(ref admitted.Total), Tag(Admitted)),
                new Measurement<long>(Interlocked.Read(ref published.Total), Tag(Published)),
            });
            meter.CreateObservableGauge<int>("integration_traffic_signal_supported", () => new[]
            {
                new Measurement<int>(Supported(admitted, Volatile.Read(ref consumerReady)) ? 1 : 0, Tag(Admitted)),
                new Measurement<int>(Supported(published, Volatile.Read(ref producerReady)) ? 1 : 0, Tag(Published)),
            });
            using var process = Process.GetCurrentProcess();
            var started = new DateTimeOffset(process.StartTime.ToUniversalTime()).ToUnixTimeMilliseconds() / 1000.0;
            meter.CreateObservableGauge<double>("integration_traffic_process_start_time_seconds", () => started);
            instrumentsReady = 1;
        }
        catch (Exception ex)
        {
            // Telemetry registration must not prevent processing. An absent
            // series, like supported=0, means that traffic is unknown.
            instrumentsReady = 0;
            this.logger.LogWarning("Kafka activity metrics could not be registered ({ExceptionType}); coverage is unknown.", ex.GetType().Name);
        }
    }

    internal void ConsumerStarted() => Volatile.Write(ref consumerReady, 1);
    internal void ConsumerStopped() => Volatile.Write(ref consumerReady, 0);
    internal void ProducerStarted() => Volatile.Write(ref producerReady, 1);
    internal void ProducerStopped() => Volatile.Write(ref producerReady, 0);
    internal void InvalidatePublication() => Invalidate(published, "a send's acknowledgement is unknown, a transaction was used, or the registered producer was replaced");

    internal void RecordAdmitted(string topic, int partition, long offset)
    {
        if (!Supported(admitted, Volatile.Read(ref consumerReady))) return;
        if (!string.Equals(topic, consumerTopic, StringComparison.Ordinal) || partition < 0 || offset < 0)
        {
            Invalidate(admitted, "a record came from a topic other than the input topic, or without a position");
            return;
        }
        Increment(admitted);
    }

    internal void PublicationUncertain(string topic)
    {
        if (inputTopics.Contains(topic)) InvalidatePublication();
    }

    internal void RecordPublished(string topic, PersistenceStatus status, int partition, long offset)
    {
        if (!Supported(published, Volatile.Read(ref producerReady)) || !inputTopics.Contains(topic)) return;
        if (status == PersistenceStatus.PossiblyPersisted)
        {
            Invalidate(published, "a send was possibly persisted");
            return;
        }
        if (status != PersistenceStatus.Persisted) return;
        if (partition < 0 || offset < 0)
        {
            Invalidate(published, "an acknowledgement had no position");
            return;
        }
        Increment(published);
    }

    private bool Supported(Signal signal, int ready) => Volatile.Read(ref instrumentsReady) != 0
        && signal.Selected && Volatile.Read(ref signal.Invalid) == 0 && ready != 0;

    // Coverage of a signal, once unknown, stays unknown for the process; the
    // first time, a selected signal logs why (never a topic or message).
    private void Invalidate(Signal signal, string reason)
    {
        if (Interlocked.Exchange(ref signal.Invalid, 1) == 0 && signal.Selected)
        {
            logger.LogWarning("Kafka activity signal {Signal} coverage is unknown from now on: {Reason}.", signal.Name, reason);
        }
    }

    private void Increment(Signal signal)
    {
        for (;;)
        {
            var previous = Interlocked.Read(ref signal.Total);
            if (previous == MaxExact)
            {
                Invalidate(signal, "the count reached its exact limit");
                return;
            }
            if (Interlocked.CompareExchange(ref signal.Total, previous + 1, previous) == previous) return;
        }
    }

    private static HashSet<string> Split(string? value) => string.IsNullOrWhiteSpace(value)
        ? new(StringComparer.Ordinal)
        : value.Split(',').Select(s => s.Trim()).ToHashSet(StringComparer.Ordinal);

    private static bool ValidTopic(string topic) => topic.Length is > 0 and <= 249 && topic is not ("." or "..")
        && topic.All(c => c is >= 'a' and <= 'z' or >= 'A' and <= 'Z' or >= '0' and <= '9' or '.' or '_' or '-');

    private static KeyValuePair<string, object?> Tag(string signal) => new("signal", signal);

    public void Dispose()
    {
        Volatile.Write(ref instrumentsReady, 0);
        try { meter.Dispose(); }
        catch (Exception) { /* A telemetry listener must not break shutdown. */ }
    }

    private sealed class Signal(string name, bool selected)
    {
        internal readonly string Name = name;
        internal readonly bool Selected = selected;
        internal long Total;
        internal int Invalid;
    }
}
