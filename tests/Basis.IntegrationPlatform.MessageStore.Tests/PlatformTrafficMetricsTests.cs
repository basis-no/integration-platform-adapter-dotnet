using System.Diagnostics.Metrics;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Basis.IntegrationPlatform.MessageStore.Tests;

[TestClass]
public sealed class PlatformTrafficMetricsTests
{
    private static PlatformMessageStoreOptions Options(string signals = "record_admitted", string topics = "") => new()
    {
        TrafficSignals = signals, TrafficInputTopics = topics, InputTopic = "orders.in",
        KafkaBootstrapServers = "127.0.0.1:1", PostgresConnectionString = "Host=unused;Username=synthetic",
    };

    [TestMethod]
    public void MetricsHaveFixedNamesLabelsAndProcessStartTime()
    {
        using var capture = new TrafficCapture();
        using var metrics = new PlatformTrafficMetrics(Options());
        capture.Collect();
        Assert.AreEqual(0, capture.Value("integration_traffic_signal_supported", "record_admitted"));
        metrics.ConsumerStarted();
        metrics.RecordAdmitted("orders.in", 0, 7);
        metrics.RecordAdmitted("orders.in", 0, 7); // Redelivery is activity too.
        capture.Collect();
        Assert.AreEqual(2, capture.Value("integration_traffic_events_total", "record_admitted"));
        Assert.AreEqual(0, capture.Value("integration_traffic_events_total", "record_published"));
        Assert.AreEqual(1, capture.Value("integration_traffic_signal_supported", "record_admitted"));
        Assert.AreEqual(0, capture.Value("integration_traffic_signal_supported", "record_published"));
        Assert.IsTrue(capture.Value("integration_traffic_process_start_time_seconds") > 0);
        Assert.IsTrue(capture.Value("integration_traffic_process_start_time_seconds") <= DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 1);
        Assert.AreEqual(5, capture.Values.Count);
        Assert.IsTrue(capture.Values.All(v => v.Tags.Length == (v.Name.EndsWith("start_time_seconds", StringComparison.Ordinal) ? 0 : 1)));
        Assert.IsTrue(capture.Values.SelectMany(v => v.Tags).All(t => t.Key == "signal" && t.Value is "record_admitted" or "record_published"));
        metrics.ConsumerStopped();
        capture.Collect();
        Assert.AreEqual(0, capture.Value("integration_traffic_signal_supported", "record_admitted"));
    }

    [TestMethod]
    public void DisabledExportRegistersNoInstruments()
    {
        using var capture = new TrafficCapture();
        using var metrics = new PlatformTrafficMetrics(Options("", "orders.in"));
        metrics.ConsumerStarted();
        metrics.ProducerStarted();
        metrics.RecordAdmitted("orders.in", 0, 1);
        capture.Collect();
        Assert.AreEqual(0, capture.Values.Count);
    }

    [TestMethod]
    public void UnknownCoverageIsLoggedOncePerSignalWithoutTopics()
    {
        var logger = new ListLogger();
        using var metrics = new PlatformTrafficMetrics(Options("record_admitted,record_published", "secret-topic.*"), logger);
        metrics.ConsumerStarted();
        metrics.RecordAdmitted("other.in", 0, 1);
        metrics.RecordAdmitted("other.in", 0, 2);
        metrics.InvalidatePublication();

        Assert.AreEqual(2, logger.Messages.Count, string.Join(" | ", logger.Messages));
        StringAssert.Contains(logger.Messages[0], "record_published");
        StringAssert.Contains(logger.Messages[1], "record_admitted");
        Assert.IsFalse(logger.Messages.Any(m => m.Contains("secret-topic", StringComparison.Ordinal) || m.Contains("other.in", StringComparison.Ordinal)));

        var invalid = new ListLogger();
        using var typo = new PlatformTrafficMetrics(Options("record_admited"), invalid);
        StringAssert.Contains(invalid.Messages.Single(), "BASIS_TRAFFIC_SIGNALS");
    }

    [TestMethod]
    public void TheDefaultRegistrationCountsPublicationThroughTheRegisteredProducer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddBasisMessageStoreAdapter<SyntheticHandler>(o =>
        {
            o.KafkaBootstrapServers = "127.0.0.1:1";
            o.PostgresConnectionString = "Host=unused;Username=synthetic";
            o.TrafficSignals = "record_admitted,record_published";
            o.TrafficInputTopics = "orders.in";
        });
        using var provider = services.BuildServiceProvider();
        using var capture = new TrafficCapture();
        Assert.IsInstanceOfType<PlatformTrafficProducer>(provider.GetRequiredService<IProducer<string, byte[]>>());
        provider.GetServices<IHostedService>().Single();
        capture.Collect();
        Assert.AreEqual(1, capture.Value("integration_traffic_signal_supported", "record_published"));
    }

    [TestMethod]
    public void DisposingTheProducerEndsPublicationCoverageAndDisposesTheClient()
    {
        using var capture = new TrafficCapture();
        using var metrics = new PlatformTrafficMetrics(Options("record_published", "orders.in"));
        var inner = new SyntheticProducer();
        var producer = new PlatformTrafficProducer(inner, metrics);
        producer.Selected();
        capture.Collect();
        Assert.AreEqual(1, capture.Value("integration_traffic_signal_supported", "record_published"));

        producer.Dispose();

        capture.Collect();
        Assert.AreEqual(0, capture.Value("integration_traffic_signal_supported", "record_published"));
        Assert.IsTrue(inner.Disposed);
    }

    [TestMethod]
    [DataRow("Record_admitted", "orders.in", 0, 0)]
    [DataRow("record_admitted,unknown", "orders.in", 0, 0)]
    [DataRow("record_admitted,record_published", "", 1, 0)]
    [DataRow("record_admitted,record_published", "orders.*", 1, 0)]
    [DataRow("record_admitted,record_published", "orders.in,", 1, 0)]
    [DataRow("record_admitted,record_published", "..", 1, 0)]
    [DataRow("record_published", "orders.in, other.in", 0, 1)]
    public void UnsupportedConfigurationReportsUnknown(string signals, string topics, int admitted, int published)
    {
        using var capture = new TrafficCapture();
        using var metrics = new PlatformTrafficMetrics(Options(signals, topics));
        metrics.ConsumerStarted();
        metrics.ProducerStarted();
        capture.Collect();
        Assert.AreEqual(admitted, capture.Value("integration_traffic_signal_supported", "record_admitted"));
        Assert.AreEqual(published, capture.Value("integration_traffic_signal_supported", "record_published"));
    }

    [TestMethod]
    public void InvalidRecordCoordinatesMakeAdmissionUnknown()
    {
        using var capture = new TrafficCapture();
        using var metrics = new PlatformTrafficMetrics(Options());
        metrics.ConsumerStarted();
        metrics.RecordAdmitted("orders.in", 0, -1);
        metrics.RecordAdmitted("orders.in", 0, 1);
        capture.Collect();
        Assert.AreEqual(0, capture.Value("integration_traffic_events_total", "record_admitted"));
        Assert.AreEqual(0, capture.Value("integration_traffic_signal_supported", "record_admitted"));
    }

    [TestMethod]
    public async Task ProducerCountsOnlyAcknowledgedListedRecordsForEveryOverload()
    {
        using var capture = new TrafficCapture();
        using var metrics = new PlatformTrafficMetrics(Options("record_published", "orders.in"));
        using var inner = new SyntheticProducer();
        using var producer = new PlatformTrafficProducer(inner, metrics);
        producer.Selected();
        var message = new Message<string, byte[]> { Key = "synthetic-private-key", Value = "synthetic-private-payload"u8.ToArray() };
        var acknowledgement = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        inner.Delay = acknowledgement;
        var pending = producer.ProduceAsync("orders.in", message);
        capture.Collect();
        Assert.AreEqual(0, capture.Value("integration_traffic_events_total", "record_published"));
        acknowledgement.SetResult();
        await pending;
        inner.Delay = null;
        await producer.ProduceAsync(new TopicPartition("orders.in", 0), message);
        var callbacks = 0;
        producer.Produce("orders.in", message, _ => callbacks++);
        producer.Produce(new TopicPartition("orders.in", 0), message, _ => callbacks++);
        await producer.ProduceAsync("unlisted.in", message);
        producer.Produce("unlisted.in", message);
        inner.Status = PersistenceStatus.NotPersisted;
        await producer.ProduceAsync("orders.in", message);
        inner.Error = new Error(ErrorCode.Local_QueueFull);
        await Assert.ThrowsExactlyAsync<ProduceException<string, byte[]>>(() => producer.ProduceAsync("orders.in", message));
        producer.Produce("orders.in", message, _ => callbacks++);
        capture.Collect();
        Assert.AreEqual(4, capture.Value("integration_traffic_events_total", "record_published"));
        Assert.AreEqual(1, capture.Value("integration_traffic_signal_supported", "record_published"));
        Assert.AreEqual(3, callbacks);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task ProducerCompletionDoesNotNeedTheCallersSynchronizationContext(bool usePartition)
    {
        using var metrics = new PlatformTrafficMetrics(Options("", ""));
        var acknowledgement = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var inner = new SyntheticProducer { Delay = acknowledgement };
        using var producer = new PlatformTrafficProducer(inner, metrics);
        var context = new NonPumpingContext();
        var invocation = Task.Run(() =>
        {
            SynchronizationContext.SetSynchronizationContext(context);
            try
            {
                var send = usePartition
                    ? producer.ProduceAsync(new TopicPartition("orders.in", 0), new Message<string, byte[]>())
                    : producer.ProduceAsync("orders.in", new Message<string, byte[]>());
                acknowledgement.SetResult();
                return send.GetAwaiter().GetResult();
            }
            finally { SynchronizationContext.SetSynchronizationContext(null); }
        });
        try
        {
            await invocation.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.IsTrue(context.Work.IsEmpty, "Disabled telemetry must not add a caller-context continuation.");
        }
        finally
        {
            // A regression must fail finitely without leaving a blocked worker.
            while (context.Work.TryDequeue(out var work)) work.Callback(work.State);
            await invocation.WaitAsync(TimeSpan.FromSeconds(5));
        }
    }

    private sealed class NonPumpingContext : SynchronizationContext
    {
        internal System.Collections.Concurrent.ConcurrentQueue<(SendOrPostCallback Callback, object? State)> Work { get; } = new();
        public override void Post(SendOrPostCallback d, object? state) => Work.Enqueue((d, state));
    }

    [TestMethod]
    public async Task AmbiguousOrMissingAcknowledgementMetadataMakesPublicationUnknown()
    {
        using var capture = new TrafficCapture();
        using var metrics = new PlatformTrafficMetrics(Options("record_published", "orders.in"));
        using var inner = new SyntheticProducer { Status = PersistenceStatus.PossiblyPersisted };
        using var producer = new PlatformTrafficProducer(inner, metrics);
        producer.Selected();
        await producer.ProduceAsync("orders.in", new Message<string, byte[]>());
        inner.Status = PersistenceStatus.Persisted;
        await producer.ProduceAsync("orders.in", new Message<string, byte[]>());
        capture.Collect();
        Assert.AreEqual(0, capture.Value("integration_traffic_events_total", "record_published"));
        Assert.AreEqual(0, capture.Value("integration_traffic_signal_supported", "record_published"));
    }

    [TestMethod]
    public async Task MissingAcknowledgementCoordinatesMakePublicationUnknown()
    {
        using var capture = new TrafficCapture();
        using var metrics = new PlatformTrafficMetrics(Options("record_published", "orders.in"));
        using var inner = new SyntheticProducer { AckOffset = -1 };
        using var producer = new PlatformTrafficProducer(inner, metrics);
        producer.Selected();
        await producer.ProduceAsync("orders.in", new Message<string, byte[]>());
        capture.Collect();
        Assert.AreEqual(0, capture.Value("integration_traffic_events_total", "record_published"));
        Assert.AreEqual(0, capture.Value("integration_traffic_signal_supported", "record_published"));
    }

    [TestMethod]
    [DataRow("orders.in", 0)]
    [DataRow("unlisted.in", 1)]
    public async Task CanceledSendAddsNothingAndOnlyAffectsListedCoverage(string topic, int supported)
    {
        using var capture = new TrafficCapture();
        using var metrics = new PlatformTrafficMetrics(Options("record_published", "orders.in"));
        using var inner = new SyntheticProducer { Delay = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
        using var producer = new PlatformTrafficProducer(inner, metrics);
        producer.Selected();
        using var cancellation = new CancellationTokenSource();
        var pending = producer.ProduceAsync(topic, new Message<string, byte[]>(), cancellation.Token);
        cancellation.Cancel();
        await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => pending);
        capture.Collect();
        Assert.AreEqual(0, capture.Value("integration_traffic_events_total", "record_published"));
        Assert.AreEqual(supported, capture.Value("integration_traffic_signal_supported", "record_published"));
    }

    [TestMethod]
    public void TransactionalPublicationCannotClaimBrokerAcknowledgementAsCommit()
    {
        using var capture = new TrafficCapture();
        using var metrics = new PlatformTrafficMetrics(Options("record_published", "orders.in"));
        using var producer = new PlatformTrafficProducer(new SyntheticProducer(), metrics);
        producer.Selected();
        producer.BeginTransaction();
        producer.Produce("orders.in", new Message<string, byte[]>());
        capture.Collect();
        Assert.AreEqual(0, capture.Value("integration_traffic_events_total", "record_published"));
        Assert.AreEqual(0, capture.Value("integration_traffic_signal_supported", "record_published"));
    }

    [TestMethod]
    public void PublicationCoverageBeginsWhenTheConsumerSelectsTheInstrumentedProducer()
    {
        var options = Options("record_published", "orders.in");
        using var capture = new TrafficCapture();
        using var metrics = new PlatformTrafficMetrics(options);
        using var producer = new PlatformTrafficProducer(new SyntheticProducer(), metrics);
        capture.Collect();
        Assert.AreEqual(0, capture.Value("integration_traffic_signal_supported", "record_published"));
        using var dataSource = Npgsql.NpgsqlDataSource.Create(options.PostgresConnectionString);
        var observations = new PlatformStepObservationCollector();
        var store = new PlatformMessageStore(dataSource, observations, Microsoft.Extensions.Options.Options.Create(options),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<PlatformMessageStore>.Instance);
        using var consumer = new PlatformMessageConsumerService(new SyntheticHandler(), store, observations, producer,
            Microsoft.Extensions.Options.Options.Create(options),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<PlatformMessageConsumerService>.Instance, metrics);
        capture.Collect();
        Assert.AreEqual(1, capture.Value("integration_traffic_signal_supported", "record_published"));
        producer.Produce("orders.in", new Message<string, byte[]>());
        capture.Collect();
        Assert.AreEqual(1, capture.Value("integration_traffic_events_total", "record_published"));
    }

    [TestMethod]
    public void ReplacingTheRegisteredProducerLeavesPublicationUnsupported()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddBasisMessageStoreAdapter<SyntheticHandler>(o =>
        {
            o.KafkaBootstrapServers = "127.0.0.1:1";
            o.PostgresConnectionString = "Host=unused;Username=synthetic";
            o.TrafficSignals = "record_published";
            o.TrafficInputTopics = "orders.in";
        });
        services.AddSingleton<IProducer<string, byte[]>>(new SyntheticProducer());
        using var provider = services.BuildServiceProvider();
        using var capture = new TrafficCapture();
        // Enumeration constructs the shadowed adapter wrapper too. That must
        // not claim coverage of the custom producer selected by normal DI.
        Assert.AreEqual(2, provider.GetServices<IProducer<string, byte[]>>().Count());
        Assert.IsInstanceOfType<SyntheticProducer>(provider.GetRequiredService<IProducer<string, byte[]>>());
        provider.GetServices<IHostedService>().Single();
        provider.GetRequiredService<PlatformTrafficMetrics>();
        capture.Collect();
        Assert.AreEqual(0, capture.Value("integration_traffic_signal_supported", "record_published"));
    }

    [TestMethod]
    public void AThrowingTelemetryListenerDoesNotBreakActivityOrShutdown()
    {
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, _) =>
            {
                if (instrument.Meter.Name == PlatformTrafficMetrics.MeterName) throw new InvalidOperationException("synthetic exporter failure");
            },
        };
        listener.Start();
        using var metrics = new PlatformTrafficMetrics(Options("record_admitted,record_published", "orders.in"));
        metrics.ConsumerStarted();
        metrics.ProducerStarted();
        metrics.RecordAdmitted("orders.in", 0, 1);
        metrics.RecordPublished("orders.in", PersistenceStatus.Persisted, 0, 1);
    }

    private sealed class SyntheticHandler : IPlatformMessageHandler
    {
        public Task<PlatformMessageResult> HandleAsync(PlatformMessage message, CancellationToken cancellationToken)
            => Task.FromResult(PlatformMessageResult.Completed());
    }

    private sealed class SyntheticProducer : IProducer<string, byte[]>
    {
        internal TaskCompletionSource? Delay { get; set; }
        internal PersistenceStatus Status { get; set; } = PersistenceStatus.Persisted;
        internal long AckOffset { get; set; } = 7;
        internal Error Error { get; set; } = new(ErrorCode.NoError);
        public Handle Handle => throw new NotSupportedException();
        public string Name => "synthetic-producer";
        public async Task<DeliveryResult<string, byte[]>> ProduceAsync(string topic, Message<string, byte[]> message, CancellationToken cancellationToken = default)
        {
            if (Delay is not null) await Delay.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            var result = Result(topic, message);
            if (Error.IsError) throw new ProduceException<string, byte[]>(Error, result);
            return result;
        }
        public Task<DeliveryResult<string, byte[]>> ProduceAsync(TopicPartition topicPartition, Message<string, byte[]> message, CancellationToken cancellationToken = default)
            => ProduceAsync(topicPartition.Topic, message, cancellationToken);
        private DeliveryResult<string, byte[]> Result(string topic, Message<string, byte[]> message) => new()
        {
            Topic = topic, Partition = 0, Offset = AckOffset, Status = Status, Message = message,
        };
        public void Produce(string topic, Message<string, byte[]> message, Action<DeliveryReport<string, byte[]>>? deliveryHandler = null)
            => deliveryHandler?.Invoke(new DeliveryReport<string, byte[]> { Topic = topic, Partition = 0, Offset = AckOffset, Status = Status, Error = Error, Message = message });
        public void Produce(TopicPartition topicPartition, Message<string, byte[]> message, Action<DeliveryReport<string, byte[]>>? deliveryHandler = null)
            => Produce(topicPartition.Topic, message, deliveryHandler);
        public int Poll(TimeSpan timeout) => 0;
        public int Flush(TimeSpan timeout) => 0;
        public void Flush(CancellationToken cancellationToken = default) { }
        public int AddBrokers(string brokers) => 0;
        public void SetSaslCredentials(string username, string password) { }
        public void InitTransactions(TimeSpan timeout) { }
        public void BeginTransaction() { }
        public void CommitTransaction(TimeSpan timeout) { }
        public void CommitTransaction() { }
        public void AbortTransaction(TimeSpan timeout) { }
        public void AbortTransaction() { }
        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout) { }
        internal bool Disposed { get; private set; }
        public void Dispose() => Disposed = true;
    }

    private sealed class ListLogger : Microsoft.Extensions.Logging.ILogger<PlatformTrafficMetrics>
    {
        internal List<string> Messages { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;
        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter) => Messages.Add(formatter(state, exception));
    }
}

internal sealed class TrafficCapture : IDisposable
{
    private readonly MeterListener listener = new();
    internal List<(string Name, double Value, KeyValuePair<string, object?>[] Tags)> Values { get; } = [];

    internal TrafficCapture()
    {
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == PlatformTrafficMetrics.MeterName) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((i, value, tags, _) => Values.Add((i.Name, value, tags.ToArray())));
        listener.SetMeasurementEventCallback<int>((i, value, tags, _) => Values.Add((i.Name, value, tags.ToArray())));
        listener.SetMeasurementEventCallback<double>((i, value, tags, _) => Values.Add((i.Name, value, tags.ToArray())));
        listener.Start();
    }

    internal void Collect()
    {
        Values.Clear();
        listener.RecordObservableInstruments();
    }

    internal double Value(string name, string? signal = null) => Values.Single(v => v.Name == name
        && (signal is null ? v.Tags.Length == 0 : v.Tags.Single().Value as string == signal)).Value;

    public void Dispose() => listener.Dispose();
}
