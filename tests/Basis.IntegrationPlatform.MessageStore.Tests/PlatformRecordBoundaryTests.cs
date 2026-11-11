using System.Reflection;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Basis.IntegrationPlatform.MessageStore.Tests;

[TestClass]
public sealed class PlatformRecordBoundaryTests
{
    private const string Topic = "bip.order-pipeline.in";

    [TestMethod]
    [DataRow("order-pipeline", "bip.order-pipeline.in", 0, 42L, "d7431daf-3b19-51d8-968b-020679b335a5")]
    [DataRow("order-pipeline", "bip.order-pipeline.in", 3, 0L, "3421dcd0-bbee-5491-8683-1fc2b8cbfd84")]
    [DataRow("invoice-\u00e9", "bip.invoice.in", 11, 9007199254740993L, "f3b505ba-d509-5a06-9d6f-fd595e79c844")]
    public void TheDerivedIdMatchesTheSharedTestVectors(string integration, string topic, int partition, long offset, string expected)
    {
        Assert.AreEqual(expected, PlatformRecordIdentity.Derive(integration, topic, partition, offset));
    }

    [TestMethod]
    [DataRow("x-bip-message-id")]
    [DataRow("x-bip-correlation-id")]
    [DataRow("x-bip-content-type")]
    [DataRow("x-bip-source-system")]
    [DataRow("x-bip-target-system")]
    [DataRow("X-BIP-Correlation-Id")]
    public void ARecordWithANulInAStoredEnvelopeHeaderIsRefused(string header)
    {
        using var service = CreateService();
        var record = service.ReadRecord(Record(7, ("x-bip-message-id", "order-1"), (header, "before\0after")));

        StringAssert.Contains(record.Refusal, $"{header.ToLowerInvariant()} contains a NUL byte");
        StringAssert.StartsWith(record.Refusal, "The Message Store cannot hold the record's envelope: ");
        StringAssert.EndsWith(record.Refusal, "; the handler did not run");
        Assert.IsFalse(record.Refusal!.Contains("before", StringComparison.Ordinal), "The refused value is not copied into the reason.");
        Assert.AreEqual(
            header.Equals("x-bip-message-id", StringComparison.OrdinalIgnoreCase)
                ? PlatformRecordIdentity.Derive("order-pipeline", Topic, 0, 7)
                : "order-1",
            record.Message.MessageId);
    }

    [TestMethod]
    public void AValueOf1024BytesIsAcceptedAndOneOf1025IsRefused()
    {
        using var service = CreateService();

        Assert.IsNull(service.ReadRecord(Record(1, ("x-bip-correlation-id", new string('c', 1_024)))).Refusal);
        var refused = service.ReadRecord(Record(2, ("x-bip-correlation-id", new string('c', 1_025))));
        StringAssert.Contains(refused.Refusal, "x-bip-correlation-id is 1025 bytes, over the 1024-byte limit");

        // Bytes, not characters: 512 two-byte characters are 1,024 bytes.
        Assert.IsNull(service.ReadRecord(Record(3, ("x-bip-source-system", new string('\u00e9', 512)))).Refusal);
        Assert.IsNotNull(service.ReadRecord(Record(4, ("x-bip-source-system", new string('\u00e9', 513)))).Refusal);
    }

    [TestMethod]
    public void ARefusedMessageIdIsReplacedByTheIdDerivedFromThePosition()
    {
        using var service = CreateService();

        var record = service.ReadRecord(Record(42, ("x-bip-message-id", new string('m', 2_000))));

        StringAssert.Contains(record.Refusal, "x-bip-message-id is 2000 bytes, over the 1024-byte limit");
        Assert.AreEqual(PlatformRecordIdentity.Derive("order-pipeline", Topic, 0, 42), record.Message.MessageId);
        Assert.AreEqual(new PlatformRecordPosition(Topic, 0, 42), record.Position);
    }

    [TestMethod]
    public void TheCheckIsOnTheRawBytesAndTheLastHeaderOfANameCounts()
    {
        using var service = CreateService();
        var record = new ConsumeResult<string, byte[]>
        {
            TopicPartitionOffset = new TopicPartitionOffset(Topic, new Partition(0), new Offset(5)),
            Message = new Message<string, byte[]>
            {
                Key = "key",
                Value = [],
                Headers = new Headers
                {
                    { "x-bip-message-id", "order-1"u8.ToArray() },
                    // 1,024 bytes that are not valid UTF-8: decoded, each one
                    // would be three bytes of U+FFFD.
                    { "x-bip-source-system", Enumerable.Repeat((byte)0xff, 1_024).ToArray() },
                    { "x-bip-target-system", "first\0"u8.ToArray() },
                    { "X-Bip-Target-System", "second"u8.ToArray() },
                },
            },
        };

        Assert.IsNull(service.ReadRecord(record).Refusal);

        record.Message.Headers.Add("x-bip-target-system", "third\0"u8.ToArray());
        StringAssert.Contains(service.ReadRecord(record).Refusal, "x-bip-target-system contains a NUL byte");
    }

    [TestMethod]
    public void TheRecordKeyIsCheckedWhenItBecomesTheCorrelationId()
    {
        using var service = CreateService();

        var keyed = service.ReadRecord(Record(1, "order\0key", ("x-bip-message-id", "order-1")));
        StringAssert.Contains(keyed.Refusal, "the record key, which is the correlation id, contains a NUL byte");

        var withCorrelation = service.ReadRecord(Record(2, "order\0key", ("x-bip-message-id", "order-1"), ("x-bip-correlation-id", "corr-1")));
        Assert.IsNull(withCorrelation.Refusal, "The key is not stored when the record has its own correlation id.");
    }

    [TestMethod]
    public void TheContentTypeHeaderIsCheckedWhenItBecomesTheContentType()
    {
        using var service = CreateService();

        StringAssert.Contains(
            service.ReadRecord(Record(1, ("x-bip-message-id", "order-1"), ("Content-Type", "text/plain\0"))).Refusal,
            "content-type, which is the content type, contains a NUL byte");
        Assert.IsNull(
            service.ReadRecord(Record(2, ("x-bip-message-id", "order-1"), ("x-bip-content-type", "application/json"), ("content-type", "text/plain\0"))).Refusal,
            "Not stored when the record has its own x-bip-content-type.");
    }

    [TestMethod]
    public void ARecordWithoutAnIdKeepsItsGeneratedIdWhileTheSameOffsetIsReadAgain()
    {
        using var service = CreateService();

        var first = service.ReadRecord(Record(10));
        var again = service.ReadRecord(Record(10));
        Assert.AreEqual(first.Message.MessageId, again.Message.MessageId);

        // Another record on the partition supersedes the kept id.
        service.ReadRecord(Record(11, ("x-bip-message-id", "order-11")));
        Assert.AreNotEqual(first.Message.MessageId, service.ReadRecord(Record(10)).Message.MessageId);

        // A blank id is no id.
        var blank = service.ReadRecord(Record(12, ("x-bip-message-id", "  ")));
        Assert.AreEqual(blank.Message.MessageId, service.ReadRecord(Record(12, ("x-bip-message-id", "  "))).Message.MessageId);
    }

    [TestMethod]
    public void AMarkIsDroppedByACommitAtOrAfterItsOffsetOrByARecordAtAnotherOffset()
    {
        var marks = new PlatformUnrecordedDeliveries(NullLogger.Instance);
        PlatformUnrecordedDelivery Mark(long offset, int partition = 0) =>
            new(new PlatformRecordPosition(Topic, partition, offset), "m", "deliver", "m:deliver", "https://target.example", null, DateTimeOffset.UtcNow);

        marks.Mark(Mark(5));
        marks.Mark(Mark(5, partition: 1));
        marks.Supersede(new PlatformRecordPosition(Topic, 0, 5), committed: false);
        Assert.IsNotNull(marks.Find(new PlatformRecordPosition(Topic, 0, 5), "m:deliver"), "The same record read again keeps its mark.");

        marks.Supersede(new PlatformRecordPosition(Topic, 0, 4), committed: true);
        Assert.AreEqual(2, marks.Count, "A commit before the mark's offset keeps it.");
        marks.Supersede(new PlatformRecordPosition(Topic, 0, 6), committed: true);
        Assert.IsNull(marks.Find(new PlatformRecordPosition(Topic, 0, 5), "m:deliver"));
        Assert.IsNotNull(marks.Find(new PlatformRecordPosition(Topic, 1, 5), "m:deliver"), "Another partition's mark stays.");

        marks.Supersede(new PlatformRecordPosition(Topic, 1, 3), committed: false);
        Assert.AreEqual(0, marks.Count, "A record at another offset drops the partition's marks.");
    }

    [TestMethod]
    public void TheOldestMarkIsDroppedAboveTheBound()
    {
        var marks = new PlatformUnrecordedDeliveries(NullLogger.Instance);
        for (var offset = 0; offset <= PlatformUnrecordedDeliveries.MaxMarks; offset++)
        {
            marks.Mark(new(new PlatformRecordPosition(Topic, offset % 7, offset), "m", "deliver", $"m{offset}:deliver", "https://target.example", null, DateTimeOffset.UtcNow));
        }

        Assert.AreEqual(PlatformUnrecordedDeliveries.MaxMarks, marks.Count);
        Assert.IsNull(marks.Find(new PlatformRecordPosition(Topic, 0, 0), "m0:deliver"));
    }

    [TestMethod]
    public void AnUnconfirmedCommitHasItsOwnSqlStateWhateverTheWarnings()
    {
        // A warning with a class 22 SQLSTATE must not make the write count as
        // the record's own data.
        var failure = new PlatformCommitUnconfirmedException("22P05");

        Assert.AreEqual("08007", PlatformStoreFailures.SqlState(failure));
        Assert.AreEqual(PlatformStoreErrorClass.Unavailable, PlatformStoreFailures.Classify(failure));
        StringAssert.Contains(failure.Message, "SQLSTATE 22P05");
    }

    [TestMethod]
    public void AFailureWithoutASqlStateIsUnavailable()
    {
        Assert.AreEqual(PlatformStoreErrorClass.Unavailable, PlatformStoreFailures.Classify(new TimeoutException()));
        Assert.AreEqual(PlatformStoreErrorClass.Unavailable, PlatformStoreFailures.Classify(new OperationCanceledException()));
        Assert.AreEqual(PlatformStoreErrorClass.Unavailable, PlatformStoreFailures.Classify(new NpgsqlException("connection lost")));
        Assert.IsNull(PlatformStoreFailures.SqlState(new NpgsqlException("connection lost")));
    }

    [TestMethod]
    public void AStoredFailureTextHasNoNul()
    {
        Assert.AreEqual("bad \ufffd byte", PlatformStoreFailures.Storable("bad \0 byte"));
    }

    private static PlatformMessageConsumerService CreateService()
    {
        var options = new PlatformMessageStoreOptions
        {
            ApplicationName = "order-pipeline",
            EnvironmentName = "dev",
            Tenant = "demo",
            KafkaBootstrapServers = "kafka:9092",
            InputTopic = Topic,
            PostgresConnectionString = "Host=postgres.example;Database=store",
        };
        var observations = new PlatformStepObservationCollector();
        var store = new PlatformMessageStore(
            NpgsqlDataSource.Create(options.PostgresConnectionString), observations, Options.Create(options), NullLogger<PlatformMessageStore>.Instance);
        return new PlatformMessageConsumerService(
            new NoHandler(), store, observations, DispatchProxy.Create<IProducer<string, byte[]>, NoProducer>(),
            Options.Create(options), NullLogger<PlatformMessageConsumerService>.Instance);
    }

    private static ConsumeResult<string, byte[]> Record(long offset, params (string Name, string Value)[] headers) =>
        Record(offset, "key", headers);

    private static ConsumeResult<string, byte[]> Record(long offset, string key, params (string Name, string Value)[] headers)
    {
        var kafkaHeaders = new Headers();
        foreach (var (name, value) in headers)
        {
            kafkaHeaders.Add(name, Encoding.UTF8.GetBytes(value));
        }

        return new ConsumeResult<string, byte[]>
        {
            TopicPartitionOffset = new TopicPartitionOffset(Topic, new Partition(0), new Offset(offset)),
            Message = new Message<string, byte[]> { Key = key, Value = [], Headers = kafkaHeaders },
        };
    }

    // Reading a record publishes nothing.
    public class NoProducer : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
            throw new AssertFailedException($"The producer is not used here ({targetMethod?.Name}).");
    }

    private sealed class NoHandler : IPlatformMessageHandler
    {
        public Task<PlatformMessageResult> HandleAsync(PlatformMessage message, CancellationToken cancellationToken) =>
            throw new AssertFailedException("The handler is not called here.");
    }
}
