using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Basis.IntegrationPlatform.MessageStore.Tests;

[TestClass]
public sealed class PlatformMessageConsumerServiceTests
{
    [TestMethod]
    public void ResolveContentTypePrefersPlatformHeader()
    {
        var headers = new Headers
        {
            { "content-type", Encoding.UTF8.GetBytes("application/octet-stream") },
            { "x-bip-content-type", Encoding.UTF8.GetBytes("application/json") },
        };

        var values = PlatformMessageConsumerService.ReadHeaders(headers);

        Assert.AreEqual("application/json", PlatformMessageConsumerService.ResolveContentType(values));
    }

    [TestMethod]
    public void ReadHeadersAllowsDuplicateKeys()
    {
        var headers = new Headers
        {
            { "x-bip-message-id", Encoding.UTF8.GetBytes("first") },
            { "X-BIP-Message-Id", Encoding.UTF8.GetBytes("second") },
        };

        var values = PlatformMessageConsumerService.ReadHeaders(headers);

        Assert.AreEqual("second", values["x-bip-message-id"]);
    }

    [TestMethod]
    public void CreateDlqHeadersPreservesMessageContract()
    {
        var message = new PlatformMessage(
            "message-1",
            "correlation-1",
            "integration-e2e-dotnet",
            "dev",
            "demo",
            "source",
            "target",
            "application/json",
            """{"ok":false}"""u8.ToArray(),
            new Dictionary<string, string>
            {
                ["x-demo-outcome"] = "ERROR",
                ["x-bip-foo"] = "custom",
            });

        var headers = PlatformMessageConsumerService.CreateDlqHeaders(message, "simulated failure");

        Assert.AreEqual("ERROR", Header(headers, "x-demo-outcome"));
        Assert.AreEqual("custom", Header(headers, "x-bip-foo"));
        Assert.AreEqual("message-1", Header(headers, "x-bip-message-id"));
        Assert.AreEqual("correlation-1", Header(headers, "x-bip-correlation-id"));
        Assert.AreEqual("application/json", Header(headers, "x-bip-content-type"));
        Assert.AreEqual("simulated failure", Header(headers, "x-bip-error"));
        Assert.IsFalse(string.IsNullOrWhiteSpace(Header(headers, "x-bip-dlq-at")));
        // §3: in-process retry diagnostics live in the auto_redelivery_* columns
        // only — the adapter must never invent non-contract wire headers.
        Assert.IsFalse(headers.TryGetLastBytes("x-bip-redelivered", out _), "x-bip-redelivered is not a contract header and must not appear on DLQ records.");
        Assert.IsFalse(headers.TryGetLastBytes("x-bip-redelivery-count", out _), "x-bip-redelivery-count is not a contract header and must not appear on DLQ records.");
    }

    [TestMethod]
    public void CreateDlqHeadersMasksCredentialValuesButKeepsTheirNames()
    {
        var message = Message(new Dictionary<string, string>
        {
            // Credentials a producer forwarded onto the input topic.
            ["Authorization"] = "Bearer synthetic-token",
            ["client_secret"] = "synthetic-client-secret",
            ["Ocp-Apim-Subscription-Key"] = "synthetic-subscription-key",
            ["X-API-Key"] = "synthetic-key",
            ["Cookie"] = "SESSION=synthetic-session",
            ["X-Request-Token-Echo"] = "synthetic-echoed-token",
            // Envelope and diagnostics that must stay readable.
            ["x-bip-attempt"] = "2",
            ["x-bip-redo-steps"] = "[\"deliver\"]",
            ["Idempotency-Key"] = "message-1:deliver",
            ["content-type"] = "application/json",
            ["x-demo-outcome"] = "ERROR",
        });

        var headers = PlatformMessageConsumerService.CreateDlqHeaders(message, "simulated failure");

        foreach (var name in new[] { "Authorization", "client_secret", "Ocp-Apim-Subscription-Key", "X-API-Key", "Cookie", "X-Request-Token-Echo" })
        {
            Assert.AreEqual("xxxxxx", Header(headers, name), name);
        }

        Assert.AreEqual("2", Header(headers, "x-bip-attempt"));
        Assert.AreEqual("[\"deliver\"]", Header(headers, "x-bip-redo-steps"));
        Assert.AreEqual("message-1:deliver", Header(headers, "Idempotency-Key"));
        Assert.AreEqual("application/json", Header(headers, "content-type"));
        Assert.AreEqual("ERROR", Header(headers, "x-demo-outcome"));
        Assert.AreEqual("message-1", Header(headers, "x-bip-message-id"));
        Assert.AreEqual("simulated failure", Header(headers, "x-bip-error"));
    }

    [TestMethod]
    public void ToPlatformMessageReadsSchemaVersionHeader()
    {
        var options = new PlatformMessageStoreOptions
        {
            ApplicationName = "integration-e2e-dotnet",
            EnvironmentName = "dev",
            Tenant = "demo",
        };
        var kafkaMessage = new Message<string, byte[]>
        {
            Key = "correlation-1",
            Value = """{"ok":true}"""u8.ToArray(),
            Headers = new Headers
            {
                { "x-bip-message-id", Encoding.UTF8.GetBytes("message-1") },
                { "x-bip-schema-version", Encoding.UTF8.GetBytes("7") },
                { "x-bip-content-type", Encoding.UTF8.GetBytes("application/json") },
            },
        };

        var message = PlatformMessageConsumerService.ToPlatformMessage(kafkaMessage, options);

        Assert.AreEqual("message-1", message.MessageId);
        Assert.AreEqual(7, message.SchemaVersion);
    }

    [TestMethod]
    public void SchemaVersionDefaultsToOneWhenAbsentOrInvalid()
    {
        Assert.AreEqual(7, PlatformMessageConsumerService.SchemaVersionFrom(
            new Dictionary<string, string> { ["x-bip-schema-version"] = "7" }));
        Assert.AreEqual(1, PlatformMessageConsumerService.SchemaVersionFrom(
            new Dictionary<string, string>()));
        Assert.AreEqual(1, PlatformMessageConsumerService.SchemaVersionFrom(
            new Dictionary<string, string> { ["x-bip-schema-version"] = "not-a-number" }));
        Assert.AreEqual(1, PlatformMessageConsumerService.SchemaVersionFrom(
            new Dictionary<string, string> { ["x-bip-schema-version"] = " " }));
    }

    [TestMethod]
    public async Task ReadReplayRequestHonorsChunkedBodyWithoutContentLength()
    {
        // §9: a chunked-transfer POST has no Content-Length; the redoSteps body
        // must still be honored — ignoring it would be a silent redo no-op.
        var json = """{"requestedBy":"operator","reason":"redo","redoSteps":["target-system"]}"""u8.ToArray();
        await using var body = new NonSeekableReadStream(json);

        var (request, error) = await PlatformMessageStoreExtensions.ReadReplayRequestAsync(body, CancellationToken.None);

        Assert.IsNull(error);
        Assert.IsNotNull(request);
        Assert.AreEqual("operator", request!.RequestedBy);
        Assert.AreEqual("redo", request.Reason);
        CollectionAssert.AreEqual(new[] { "target-system" }, request.RedoSteps.ToArray());
    }

    [TestMethod]
    public async Task ReadReplayRequestEmptyBodyYieldsNoRequestAndNoError()
    {
        await using var body = new NonSeekableReadStream([]);

        var (request, error) = await PlatformMessageStoreExtensions.ReadReplayRequestAsync(body, CancellationToken.None);

        Assert.IsNull(request);
        Assert.IsNull(error);
    }

    [TestMethod]
    public async Task ReadReplayRequestMalformedBodyYieldsError()
    {
        await using var body = new NonSeekableReadStream("{not-json"u8.ToArray());

        var (request, error) = await PlatformMessageStoreExtensions.ReadReplayRequestAsync(body, CancellationToken.None);

        Assert.IsNull(request);
        Assert.IsNotNull(error);
        StringAssert.Contains(error, "Malformed replay request body");
    }

    [TestMethod]
    public void AttemptFromHeaderDrivesAttemptNumber()
    {
        Assert.AreEqual(2, PlatformMessageStore.AttemptFrom(Message(new Dictionary<string, string> { ["x-bip-attempt"] = "2" })));
        Assert.AreEqual(1, PlatformMessageStore.AttemptFrom(Message(new Dictionary<string, string>())));
        Assert.AreEqual(1, PlatformMessageStore.AttemptFrom(Message(new Dictionary<string, string> { ["x-bip-attempt"] = "not-a-number" })));
        Assert.AreEqual(1, PlatformMessageStore.AttemptFrom(Message(new Dictionary<string, string> { ["x-bip-attempt"] = "0" })));
    }

    [TestMethod]
    public void CreateReplayHeadersMatchesRuntimeContract()
    {
        var stored = new ReplayableStoredMessage(
            "message-1",
            "correlation-1",
            "application/json",
            "source",
            "target",
            "integration-e2e-dotnet",
            1,
            1,
            """{"ok":true}"""u8.ToArray(),
            true,
            null,
            null,
            null);
        var request = new MessageReplayRequest("operator", "retry", ["target-system"]);

        var headers = PlatformMessageStore.CreateReplayHeaders(stored, request, 2);

        Assert.AreEqual("message-1", Header(headers, "x-bip-message-id"));
        Assert.AreEqual("correlation-1", Header(headers, "x-bip-correlation-id"));
        Assert.AreEqual("1", Header(headers, "x-bip-schema-version"));
        Assert.AreEqual("application/json", Header(headers, "x-bip-content-type"));
        Assert.AreEqual("application/json", Header(headers, "content-type"));
        Assert.AreEqual("source", Header(headers, "x-bip-source-system"));
        Assert.AreEqual("target", Header(headers, "x-bip-target-system"));
        Assert.AreEqual("true", Header(headers, "x-bip-replay"));
        Assert.AreEqual("2", Header(headers, "x-bip-attempt"));
        Assert.AreEqual("[\"target-system\"]", Header(headers, "x-bip-redo-steps"));
    }

    [TestMethod]
    public void MessageReplayRequestAcceptsLegacyBooleanRedoSteps()
    {
        var request = JsonSerializer.Deserialize<MessageReplayRequest>(
            """{"requestedBy":"operator","reason":"retry","redoSteps":true}""");

        Assert.IsNotNull(request);
        Assert.AreEqual("operator", request!.RequestedBy);
        Assert.AreEqual("retry", request.Reason);
        Assert.AreEqual(0, request.RedoSteps.Count);
    }

    [TestMethod]
    public void StepExecutorRedoHeaderMatchesStepOrIdempotencyKey()
    {
        var message = Message(new Dictionary<string, string>
        {
            ["x-bip-redo-steps"] = "[\"message-1:target-system\"]",
        });

        Assert.IsTrue(PlatformStepExecutor.ShouldRedo(message, "target-system", "message-1:target-system", NullLogger.Instance));
    }

    [TestMethod]
    public void StepExecutorMalformedRedoHeaderWarnsAndTreatsAsEmpty()
    {
        // Contract v1 DECISION D1: malformed x-bip-redo-steps is a WARNING and
        // treated as empty, never a processing failure.
        var message = Message(new Dictionary<string, string>
        {
            ["x-bip-redo-steps"] = "target-system,other-step",
        });
        var logger = new RecordingLogger();

        Assert.IsFalse(PlatformStepExecutor.ShouldRedo(message, "target-system", "message-1:target-system", logger));
        Assert.AreEqual(1, logger.Warnings.Count);
        StringAssert.Contains(logger.Warnings[0], "x-bip-redo-steps");
    }

    [TestMethod]
    public void StepExecutorNonStringRedoArrayWarnsAndTreatsAsEmpty()
    {
        var message = Message(new Dictionary<string, string>
        {
            ["x-bip-redo-steps"] = "[1,2]",
        });
        var logger = new RecordingLogger();

        Assert.IsFalse(PlatformStepExecutor.ShouldRedo(message, "target-system", "message-1:target-system", logger));
        Assert.AreEqual(1, logger.Warnings.Count);
    }

    private static PlatformMessage Message(Dictionary<string, string> headers)
    {
        return new PlatformMessage(
            "message-1",
            "correlation-1",
            "integration-e2e-dotnet",
            "dev",
            "demo",
            "source",
            "target",
            "application/json",
            """{"ok":true}"""u8.ToArray(),
            headers);
    }

    private static string Header(Headers headers, string name)
    {
        return Encoding.UTF8.GetString(headers.GetLastBytes(name));
    }

    /// <summary>
    /// A forward-only stream mimicking a chunked-transfer request body:
    /// no Length, no seeking — data is only discoverable by reading.
    /// </summary>
    private sealed class NonSeekableReadStream(byte[] data) : Stream
    {
        private readonly MemoryStream inner = new(data, writable: false);

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inner.Dispose();
            }

            base.Dispose(disposing);
        }
    }

    private sealed class RecordingLogger : ILogger
    {
        public List<string> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                Warnings.Add(formatter(state, exception));
            }
        }
    }
}
