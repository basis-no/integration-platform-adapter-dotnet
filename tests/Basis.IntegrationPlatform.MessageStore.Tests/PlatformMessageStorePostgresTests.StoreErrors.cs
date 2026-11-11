using System.Security.Cryptography;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Basis.IntegrationPlatform.MessageStore.Tests;

// A store that can be read but not written, and records whose own values the
// store cannot hold, through the consume loop: each read of a record that is
// not committed hands out the same record again, as Kafka does after a seek.
public sealed partial class PlatformMessageStorePostgresTests
{
    private const string DeliverUri = "https://target.example/orders";

    [TestMethod]
    [DataRow("read-only", true)]
    [DataRow("read-only", false)]
    [DataRow("revoked", true)]
    [DataRow("revoked", false)]
    [DataRow("synchronous-standby", true)]
    public async Task AStepWhoseDeliveredWriteFailsIsSentOnceAndCompletesWhenTheStoreIsBack(string fault, bool withMessageId)
    {
        await WithIsolatedStoreAsync(async store =>
        {
            var options = store.Options(fault == "synchronous-standby" ? 1 : null);
            options.HandlerRetryDelayMs = 0;
            var logs = new LogRecorder();
            var (service, steps, messageStore, producer) = store.Consumer(options, logs);
            var sends = 0;
            var handler = SendingHandler(steps, () => sends++);
            using var consumerService = service(handler);

            store.Break(fault);
            var record = RecordAt(0, options, withMessageId ? $"dotnet-test-{Guid.NewGuid():N}" : null);
            using var stopping = new CancellationTokenSource();
            // Two reads while the store refuses the write, then it is back.
            var consumer = new ReadAgainConsumer(stopping, [record], maxReads: 4, beforeRead: read =>
            {
                if (read == 3)
                {
                    store.Repair(fault);
                }
            });

            try
            {
                await consumerService.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(120));
            }
            finally
            {
                store.Repair(fault);
            }

            Assert.AreEqual(1, sends, "The step is sent once, not once per read.");
            CollectionAssert.AreEqual(new long[] { 0 }, consumer.Commits);
            Assert.AreEqual(0, producer.Produced.Count, "Nothing is parked while the store refuses writes.");
            var messageId = handler.Calls.Distinct().Single();
            Assert.AreEqual(3, handler.Calls.Count, "Read three times, each time without a handler retry.");
            Assert.AreEqual("COMPLETED", await LogStatusAsync(store.Admin, messageId));
            Assert.AreEqual("DELIVERED", await StepStatusAsync(store.Admin, $"{messageId}:deliver"));
            CollectionAssert.AreEqual(new[] { "deliver DELIVERED" }, await ObservedStepsAsync(store.Admin, messageId, 1));
            Assert.AreEqual(1, logs.Warnings.Count(warning => warning.Contains("was sent in an earlier read of record", StringComparison.Ordinal)));
            var storeError = fault switch
            {
                "read-only" => "storeError=read-only sqlState=25006",
                "revoked" => "storeError=privilege sqlState=42501",
                _ => "storeError=unavailable sqlState=08007",
            };
            Assert.AreEqual(2, logs.Errors.Count(error => error.EndsWith(
                $"is not committed: a step outcome was not checked or recorded, so it is consumed again {storeError}", StringComparison.Ordinal)));

            // A resend that names the step in redoSteps still sends it.
            var replay = await messageStore.ReplayAsync(messageId, new MessageReplayRequest("operator", "redo", ["deliver"]), producer, CancellationToken.None);
            Assert.AreEqual("PUBLISHED", replay.Status);
            var replayed = producer.Produced.Single().Message;
            using var replayStopping = new CancellationTokenSource();
            var replayConsumer = new ReadAgainConsumer(replayStopping, [RecordAt(1, replayed)], maxReads: 1);
            await consumerService.ConsumeAsync(replayConsumer, replayStopping.Token).WaitAsync(TimeSpan.FromSeconds(30));
            Assert.AreEqual(2, sends);
            CollectionAssert.AreEqual(new long[] { 1 }, replayConsumer.Commits);
        });
    }

    [TestMethod]
    public async Task ADeliveredWriteThatFailsThreeTimesIsWrittenOnItsFourthTryInPlace()
    {
        await WithFailingLedgerAsync(failures: 3, async (dataSource, options) =>
        {
            options.HandlerRetryDelayMs = 0;
            var observations = new PlatformStepObservationCollector();
            var store = new PlatformMessageStore(dataSource, observations, Options.Create(options), NullLogger<PlatformMessageStore>.Instance);
            var sends = 0;
            var handler = SendingHandler(LedgerTestExecutor(dataSource, observations), () => sends++);
            using var service = CreateConsumerService(handler, store, observations, new FakeProducer(), options);
            using var stopping = new CancellationTokenSource();
            var consumer = new ReadAgainConsumer(stopping, [RecordAt(0, options, $"dotnet-test-{Guid.NewGuid():N}")], maxReads: 3);

            await service.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(60));

            Assert.AreEqual(1, sends);
            Assert.AreEqual(1, consumer.Reads, "Written in place: the record is not read again.");
            CollectionAssert.AreEqual(new long[] { 0 }, consumer.Commits);
        });
    }

    [TestMethod]
    public async Task AfterACaughtStoreFailureTheNextStepOfTheAttemptIsNotSent()
    {
        await WithIsolatedStoreAsync(async store =>
        {
            var options = store.Options();
            options.HandlerRetryDelayMs = 0;
            var logs = new LogRecorder();
            var (service, steps, _, producer) = store.Consumer(options, logs);
            var first = 0;
            var second = 0;
            var handler = new DelegateHandler(async (message, ct) =>
            {
                try
                {
                    await steps.ExecuteAsync(message, "first", DeliverUri, "target", _ => { first++; return Task.CompletedTask; }, ct);
                }
                catch (PostgresException)
                {
                    // An optional step, as the handler sees it.
                }

                await steps.ExecuteAsync(message, "second", DeliverUri, "target", _ => { second++; return Task.CompletedTask; }, ct);
                return PlatformMessageResult.Completed();
            });
            using var consumerService = service(handler);
            await store.FailWritesAsync("bip_step_outcome", "57P03", "$1->>'idempotency_key' like '%:first'");
            using var stopping = new CancellationTokenSource();
            var consumer = new ReadAgainConsumer(stopping, [RecordAt(0, options, $"dotnet-test-{Guid.NewGuid():N}")], maxReads: 1);

            await consumerService.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(60));

            Assert.AreEqual(1, first);
            Assert.AreEqual(0, second, "Nothing the attempt does can be committed, so the second step is not sent.");
            Assert.AreEqual(1, handler.Calls.Count, "No handler retry after a store failure.");
            Assert.AreEqual(0, consumer.Commits.Count);
            Assert.AreEqual(0, producer.Produced.Count);
            Assert.IsTrue(logs.Errors.Any(error => error.EndsWith(
                "is not committed: a step outcome was not checked or recorded, so it is consumed again storeError=unavailable sqlState=57P03", StringComparison.Ordinal)));
        });
    }

    [TestMethod]
    public async Task AStepOnAFlowThatDoesNotCarryTheProcessingIsStillHeldBackAndMarked()
    {
        // The handler runs its step on a worker of its own, without the
        // consumer's execution context; the processing is found by the
        // message id.
        await WithIsolatedStoreAsync(async store =>
        {
            var options = store.Options();
            options.HandlerRetryDelayMs = 0;
            var (service, steps, _, producer) = store.Consumer(options, new LogRecorder());
            var sends = 0;
            var handler = new DelegateHandler(async (message, ct) =>
            {
                Task step;
                using (ExecutionContext.SuppressFlow())
                {
                    step = Task.Run(() => steps.ExecuteAsync(message, "deliver", DeliverUri, "target", _ => { sends++; return Task.CompletedTask; }, ct));
                }

                try
                {
                    await step;
                }
                catch (PostgresException)
                {
                    // An optional step, as the handler sees it.
                }

                return PlatformMessageResult.Completed();
            });
            using var consumerService = service(handler);
            await store.FailWritesAsync("bip_step_outcome", "57P03");
            var messageId = $"dotnet-test-{Guid.NewGuid():N}";
            using var stopping = new CancellationTokenSource();
            var consumer = new ReadAgainConsumer(stopping, [RecordAt(0, options, messageId)], maxReads: 3, beforeRead: read =>
            {
                if (read == 2)
                {
                    store.RepairWrites();
                }
            });

            await consumerService.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(60));

            Assert.AreEqual(1, sends);
            Assert.AreEqual(2, consumer.Reads, "Not committed on the first read.");
            CollectionAssert.AreEqual(new long[] { 0 }, consumer.Commits);
            Assert.AreEqual(0, producer.Produced.Count);
            Assert.AreEqual("DELIVERED", await StepStatusAsync(store.Admin, $"{messageId}:deliver"));
        });
    }

    [TestMethod]
    [DataRow("failed-write")]
    [DataRow("ledger-check")]
    [DataRow("ledger-check-swallowed")]
    [DataRow("ledger-check-then-retry")]
    public async Task AStepWhoseLedgerCallTheHandlersCancelledTokenStopsIsAHandlerFailure(string path)
    {
        // The store is healthy; the handler's own timeout has already fired
        // when the ledger call starts. That is the handler's failure: retried
        // and parked, not a store failure that holds the record back.
        await WithFailingLedgerAsync(failures: 0, async (dataSource, options) =>
        {
            options.MaxHandlerAttempts = 2;
            options.HandlerRetryDelayMs = 0;
            var observations = new PlatformStepObservationCollector();
            var store = new PlatformMessageStore(dataSource, observations, Options.Create(options), NullLogger<PlatformMessageStore>.Instance);
            var steps = LedgerTestExecutor(dataSource, observations);
            var sends = 0;
            var invocations = 0;
            var handler = new DelegateHandler(async (message, ct) =>
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                if (path == "ledger-check-then-retry")
                {
                    // Only the first invocation's timeout has fired; the retry
                    // sends and completes.
                    if (++invocations == 1)
                    {
                        timeout.Cancel();
                    }

                    await steps.ExecuteAsync(message, "deliver", DeliverUri, "target", _ => { sends++; return Task.CompletedTask; }, timeout.Token);
                    return PlatformMessageResult.Completed();
                }

                if (path != "failed-write")
                {
                    timeout.Cancel();
                }

                try
                {
                    await steps.ExecuteAsync(message, "deliver", DeliverUri, "target", _ =>
                    {
                        sends++;
                        // The operation turns its timeout into a failure of its own.
                        timeout.Cancel();
                        throw new TimeoutException("The call to the receiver timed out.");
                    }, timeout.Token);
                }
                catch (OperationCanceledException) when (path == "ledger-check-swallowed")
                {
                    return PlatformMessageResult.Completed();
                }

                return PlatformMessageResult.Completed();
            });
            var producer = new FakeProducer();
            using var service = CreateConsumerService(handler, store, observations, producer, options);
            var message = CreateMessage($"dotnet-test-{Guid.NewGuid():N}", options);
            var commits = 0;

            if (path == "ledger-check-swallowed")
            {
                // The step did not run and the handler returned success: not
                // recorded COMPLETED.
                var heldBack = await Assert.ThrowsExactlyAsync<PlatformRecordHeldBackException>(
                    () => service.ProcessAsync(message, () => commits++, CancellationToken.None));
                Assert.IsNull(heldBack.ErrorClass, "No store failure, so the line names no store error.");
                Assert.AreEqual(0, commits);
                Assert.IsNull(await LogStatusAsync(dataSource, message.MessageId));
                return;
            }

            await service.ProcessAsync(message, () => commits++, CancellationToken.None);

            Assert.AreEqual(2, handler.Calls.Count, "Retried in process.");
            if (path == "ledger-check-then-retry")
            {
                Assert.AreEqual(1, sends);
                Assert.AreEqual(1, commits);
                Assert.AreEqual(0, producer.Produced.Count);
                Assert.AreEqual("COMPLETED", await LogStatusAsync(dataSource, message.MessageId));
                return;
            }

            Assert.AreEqual(path == "failed-write" ? 2 : 0, sends);
            Assert.AreEqual(1, producer.Produced.Count, "Parked.");
            Assert.AreEqual(1, commits);
            Assert.AreEqual("DLQ", await LogStatusAsync(dataSource, message.MessageId));
            Assert.IsNull(await StepStatusAsync(dataSource, $"{message.MessageId}:deliver"), "The FAILED write did not reach the store; the step runs again anyway.");
        });
    }

    [TestMethod]
    public async Task ARecordParkedForItsDataIsParkedAgainAfterAnotherOffsetOfItsPartitionBegan()
    {
        await WithIsolatedStoreAsync(async store =>
        {
            var options = store.Options();
            var (service, steps, _, producer) = store.Consumer(options, new LogRecorder());
            using var consumerService = service(SendingHandler(steps, () => { }));
            // The reduced outcome cannot be written, so the parked record is
            // held back.
            await store.FailWritesAsync("bip_message_attempt", "57P03", "$1->>'correlation_id' is null");
            var refused = RecordAt(0, options, $"dotnet-test-{Guid.NewGuid():N}", ("x-bip-source-system", "nul\0"));

            await Assert.ThrowsExactlyAsync<PlatformRecordHeldBackException>(
                () => consumerService.ProcessAsync(consumerService.ReadRecord(refused), () => { }, CancellationToken.None));
            await Assert.ThrowsExactlyAsync<PlatformRecordHeldBackException>(
                () => consumerService.ProcessAsync(consumerService.ReadRecord(refused), () => { }, CancellationToken.None));
            Assert.AreEqual(1, producer.Produced.Count, "Read again at its own offset, it is not parked again.");

            // Offsets moved: another record of the partition begins, and the
            // one at offset 0 is a record of its own again.
            consumerService.ReadRecord(RecordAt(1, options, $"dotnet-test-{Guid.NewGuid():N}"));
            await Assert.ThrowsExactlyAsync<PlatformRecordHeldBackException>(
                () => consumerService.ProcessAsync(consumerService.ReadRecord(refused), () => { }, CancellationToken.None));
            Assert.AreEqual(2, producer.Produced.Count);
        });
    }

    [TestMethod]
    public async Task ParallelStepsWhoseWritesFailAreEachSentOnce()
    {
        await WithIsolatedStoreAsync(async store =>
        {
            var options = store.Options();
            options.HandlerRetryDelayMs = 0;
            var logs = new LogRecorder();
            var (service, steps, _, _) = store.Consumer(options, logs);
            var sends = new int[2];
            // Both calls return before either write fails, so both are in
            // flight when the first store failure is noted.
            var arrived = 0;
            var bothSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            async Task SendAsync(int part)
            {
                Interlocked.Increment(ref sends[part]);
                if (Interlocked.Increment(ref arrived) == 2)
                {
                    bothSent.SetResult();
                }

                await bothSent.Task.WaitAsync(TimeSpan.FromSeconds(10));
            }

            var handler = new DelegateHandler(async (message, ct) =>
            {
                await Task.WhenAll(
                    Task.Run(() => steps.ExecuteAsync(message, "part-a", DeliverUri, "target", _ => SendAsync(0), ct)),
                    Task.Run(() => steps.ExecuteAsync(message, "part-b", DeliverUri, "target", _ => SendAsync(1), ct)));
                return PlatformMessageResult.Completed();
            });
            using var consumerService = service(handler);
            await store.FailWritesAsync("bip_step_outcome", "57P03");
            var messageId = $"dotnet-test-{Guid.NewGuid():N}";
            using var stopping = new CancellationTokenSource();
            var consumer = new ReadAgainConsumer(stopping, [RecordAt(0, options, messageId)], maxReads: 4, beforeRead: read =>
            {
                if (read == 3)
                {
                    store.RepairWrites();
                }
            });

            await consumerService.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(60));

            CollectionAssert.AreEqual(new[] { 1, 1 }, sends);
            CollectionAssert.AreEqual(new long[] { 0 }, consumer.Commits);
            CollectionAssert.AreEquivalent(new[] { "part-a DELIVERED", "part-b DELIVERED" }, await ObservedStepsAsync(store.Admin, messageId, 1));
            Assert.AreEqual(2, logs.Warnings.Count(warning => warning.Contains("was sent in an earlier read of record", StringComparison.Ordinal)), "Each part kept its own mark.");
        });
    }

    [TestMethod]
    public async Task OfTwoMessagesWithTheirOwnIdsTheFailedOneIsSentAgainAndTheUnwrittenOneIsWrittenFromItsMark()
    {
        await WithIsolatedStoreAsync(async store =>
        {
            var options = store.Options();
            options.HandlerRetryDelayMs = 0;
            var (service, steps, _, _) = store.Consumer(options, new LogRecorder());
            var sends = new int[2];
            var read = 0;
            var handler = new DelegateHandler(async (message, ct) =>
            {
                read++;
                try
                {
                    await steps.ExecuteAsync(message with { MessageId = $"{message.MessageId}-1" }, "deliver", DeliverUri, "target", _ =>
                    {
                        sends[0]++;
                        return read == 1 ? throw new HttpRequestException("synthetic receiver failure") : Task.CompletedTask;
                    }, ct);
                }
                catch (HttpRequestException)
                {
                    // Handled: the part is optional for this handler.
                }

                await steps.ExecuteAsync(message with { MessageId = $"{message.MessageId}-2" }, "deliver", DeliverUri, "target", _ => { sends[1]++; return Task.CompletedTask; }, ct);
                return PlatformMessageResult.Completed();
            });
            using var consumerService = service(handler);
            await store.FailWritesAsync("bip_step_outcome", "57P03", "$1->>'idempotency_key' like '%-2:deliver'");
            var messageId = $"dotnet-test-{Guid.NewGuid():N}";
            using var stopping = new CancellationTokenSource();
            var consumer = new ReadAgainConsumer(stopping, [RecordAt(0, options, messageId)], maxReads: 3, beforeRead: next =>
            {
                if (next == 2)
                {
                    store.RepairWrites();
                }
            });

            await consumerService.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(60));

            CollectionAssert.AreEqual(new[] { 2, 1 }, sends, "The failed part is sent again; the part whose outcome was lost is not.");
            CollectionAssert.AreEqual(new long[] { 0 }, consumer.Commits);
            Assert.AreEqual("DELIVERED", await StepStatusAsync(store.Admin, $"{messageId}-1:deliver"));
            Assert.AreEqual("DELIVERED", await StepStatusAsync(store.Admin, $"{messageId}-2:deliver"));
        });
    }

    [TestMethod]
    public async Task RecordsWhoseEnvelopeTheStoreCannotHoldAreParkedWithoutRunningTheHandler()
    {
        await WithIsolatedStoreAsync(async store =>
        {
            var options = store.Options();
            var (service, steps, _, producer) = store.Consumer(options, new LogRecorder());
            var sends = 0;
            var handler = SendingHandler(steps, () => sends++);
            using var consumerService = service(handler);
            var id = $"dotnet-test-{Guid.NewGuid():N}";
            var records = new List<ConsumeResult<string, byte[]>>
            {
                RecordAt(0, options, "nul\0id"),
                RecordAt(1, options, $"{id}-1", ("x-bip-correlation-id", "nul\0correlation")),
                RecordAt(2, options, $"{id}-2", ("x-bip-content-type", "application/json\0")),
                RecordAt(3, options, $"{id}-3", ("x-bip-source-system", "source\0")),
                RecordAt(4, options, $"{id}-4", ("x-bip-target-system", "target\0")),
                RecordAt(5, options, $"{id}-5", key: "key\0with-nul"),
                RecordAt(6, options, new string('m', 2_000)),
                RecordAt(7, options, $"{id}-7", ("x-bip-correlation-id", new string('c', 1_025))),
                RecordAt(8, options, $"{id}-8", ("content-type", "application/json\0")),
                RecordAt(9, options, $"{id}-9", ("x-bip-correlation-id", new string('c', 1_024))),
                RecordAt(10, options, $"{id}-10"),
            };
            using var stopping = new CancellationTokenSource();
            var consumer = new ReadAgainConsumer(stopping, records, maxReads: 20);

            await consumerService.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(60));

            CollectionAssert.AreEqual(new long[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, consumer.Commits, "Each record is committed once, and the partition goes on.");
            CollectionAssert.AreEqual(new[] { $"{id}-9", $"{id}-10" }, handler.Calls, "Only the records the store can hold run their handler.");
            Assert.AreEqual(2, sends);
            Assert.AreEqual(9, producer.Produced.Count, "One DLQ copy per refused record.");
            for (var offset = 0; offset < 9; offset++)
            {
                var parked = PlatformMessageConsumerService.ReadHeaders(producer.Produced[offset].Message.Headers);
                var expectedId = offset is 0 or 6 ? PlatformRecordIdentity.Derive(options.ApplicationName, options.InputTopic, 0, offset) : $"{id}-{offset}";
                Assert.AreEqual(expectedId, parked["x-bip-message-id"]);
                StringAssert.StartsWith(parked["x-bip-error"], "The Message Store cannot hold the record's envelope: ");
                StringAssert.EndsWith(parked["x-bip-error"], $"; the handler did not run (record {options.InputTopic}/0@{offset})");
                Assert.AreEqual(1, await CountRowsAsync(store.Admin, "bip_message_attempt", expectedId));
                var (status, correlation, contentType, source, target, error) = await ReducedRowAsync(store.Admin, expectedId);
                Assert.AreEqual("DLQ", status);
                Assert.IsNull(correlation);
                Assert.IsNull(contentType);
                Assert.IsNull(source);
                Assert.IsNull(target);
                Assert.AreEqual(parked["x-bip-error"], error);
                CollectionAssert.AreEqual(Array.Empty<string>(), await ObservedStepsAsync(store.Admin, expectedId, 1));
            }

            Assert.AreEqual("nul\0correlation", PlatformMessageConsumerService.ReadHeaders(producer.Produced[1].Message.Headers)["x-bip-correlation-id"], "Other refused values stay as they are in the copy.");
            StringAssert.Contains(PlatformMessageConsumerService.ReadHeaders(producer.Produced[6].Message.Headers)["x-bip-error"], "x-bip-message-id is 2000 bytes, over the 1024-byte limit");
            StringAssert.Contains(PlatformMessageConsumerService.ReadHeaders(producer.Produced[8].Message.Headers)["x-bip-error"], "content-type, which is the content type, contains a NUL byte");
            Assert.AreEqual("COMPLETED", await LogStatusAsync(store.Admin, $"{id}-9"));
        });
    }

    [TestMethod]
    public async Task TenRefusedRecordsInARowAreAllParkedAndThePartitionGoesOn()
    {
        await WithIsolatedStoreAsync(async store =>
        {
            var options = store.Options();
            var (service, steps, _, producer) = store.Consumer(options, new LogRecorder());
            var handler = SendingHandler(steps, () => { });
            using var consumerService = service(handler);
            var id = $"dotnet-test-{Guid.NewGuid():N}";
            var records = Enumerable.Range(0, 10)
                .Select(offset => RecordAt(offset, options, $"{id}-{offset}", ("x-bip-correlation-id", "nul\0")))
                .Append(RecordAt(10, options, $"{id}-10"))
                .ToList();
            using var stopping = new CancellationTokenSource();
            var consumer = new ReadAgainConsumer(stopping, records, maxReads: 20);

            await consumerService.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(60));

            CollectionAssert.AreEqual(Enumerable.Range(0, 11).Select(offset => (long)offset).ToArray(), consumer.Commits);
            Assert.AreEqual(10, producer.Produced.Count);
            CollectionAssert.AreEqual(new[] { $"{id}-10" }, handler.Calls);
        });
    }

    [TestMethod]
    public async Task AReducedOutcomeTheStoreRefusesTooIsWrittenWithoutThePayload()
    {
        await WithIsolatedStoreAsync(async store =>
        {
            var options = store.Options();
            var (service, steps, _, _) = store.Consumer(options, new LogRecorder());
            using var consumerService = service(SendingHandler(steps, () => { }));
            await store.FailWritesAsync("bip_message_attempt", "22000", "$1->>'correlation_id' is null and $1->>'payload_bytes' is not null");
            var messageId = $"dotnet-test-{Guid.NewGuid():N}";
            using var stopping = new CancellationTokenSource();
            var consumer = new ReadAgainConsumer(stopping, [RecordAt(0, options, messageId, ("x-bip-source-system", "nul\0"))], maxReads: 3);

            await consumerService.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(60));

            CollectionAssert.AreEqual(new long[] { 0 }, consumer.Commits);
            await using var connection = await store.Admin.OpenConnectionAsync();
            await using var command = new NpgsqlCommand(
                "select status, payload_storage, payload_bytes is null, replayable, replay_disabled_reason from bip_message_log where message_id = @id", connection);
            command.Parameters.AddWithValue("id", messageId);
            await using var reader = await command.ExecuteReaderAsync();
            Assert.IsTrue(await reader.ReadAsync());
            Assert.AreEqual("DLQ", reader.GetString(0));
            Assert.AreEqual("metadata_only", reader.GetString(1));
            Assert.IsTrue(reader.GetBoolean(2));
            Assert.IsFalse(reader.GetBoolean(3));
            Assert.AreEqual("the record's envelope values could not be stored", reader.GetString(4));
        });
    }

    [TestMethod]
    [DataRow("publish")]
    [DataRow("commit")]
    public async Task ARefusedRecordWhoseParkingOrCommitFailsOnceIsParkedOnce(string failure)
    {
        await WithIsolatedStoreAsync(async store =>
        {
            var options = store.Options();
            var logs = new LogRecorder();
            var publishes = 0;
            var (service, steps, _, producer) = store.Consumer(options, logs, (_, _) =>
                ++publishes == 1 && failure == "publish"
                    ? throw new KafkaException(new Error(ErrorCode.Local_AllBrokersDown, "dlq broker down"))
                    : Task.CompletedTask);
            using var consumerService = service(SendingHandler(steps, () => { }));
            var messageId = $"dotnet-test-{Guid.NewGuid():N}";
            using var stopping = new CancellationTokenSource();
            var commits = 0;
            var consumer = new ReadAgainConsumer(stopping, [RecordAt(0, options, messageId, ("x-bip-target-system", "nul\0"))], maxReads: 3)
            {
                FailCommit = () => ++commits == 1 && failure == "commit" ? new KafkaException(new Error(ErrorCode.Local_Transport, "broker down")) : null,
            };

            await consumerService.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(60));

            Assert.AreEqual(2, consumer.Reads, "Read again once, after the failure.");
            CollectionAssert.AreEqual(new long[] { 0 }, consumer.Commits);
            Assert.AreEqual(1, producer.Produced.Count, "Parked once.");
            Assert.AreEqual("DLQ", await LogStatusAsync(store.Admin, messageId));
            if (failure == "publish")
            {
                Assert.IsTrue(logs.Errors.Any(error => error.Contains(
                    $"Message {messageId} at {options.InputTopic}/0@0 is not committed: it could not be parked for its own data; The Message Store cannot hold the record's envelope: x-bip-target-system contains a NUL byte; the handler did not run storeError=record-data",
                    StringComparison.Ordinal)));
            }
        });
    }

    [TestMethod]
    [DataRow("too-long")]
    [DataRow("nul")]
    public async Task AnIdTheHandlerSetsThatTheStoreCannotHoldParksTheMessage(string kind)
    {
        await WithIsolatedStoreAsync(async store =>
        {
            var options = store.Options();
            options.HandlerRetryDelayMs = 0;
            var (service, steps, _, producer) = store.Consumer(options, new LogRecorder());
            var sends = 0;
            // Random, so that PostgreSQL cannot compress it to fit the index.
            var stepMessageId = kind == "too-long" ? Convert.ToBase64String(RandomNumberGenerator.GetBytes(2_250)) : "own\0id";
            var handler = new DelegateHandler(async (message, ct) =>
            {
                await steps.ExecuteAsync(message with { MessageId = stepMessageId }, "deliver", DeliverUri, "target", _ => { sends++; return Task.CompletedTask; }, ct);
                return PlatformMessageResult.Completed();
            });
            using var consumerService = service(handler);
            var messageId = $"dotnet-test-{Guid.NewGuid():N}";
            using var stopping = new CancellationTokenSource();
            var consumer = new ReadAgainConsumer(stopping, [RecordAt(0, options, messageId)], maxReads: 3);

            await consumerService.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(60));

            Assert.AreEqual(kind == "too-long" ? 1 : 0, sends);
            Assert.AreEqual(1, handler.Calls.Count);
            CollectionAssert.AreEqual(new long[] { 0 }, consumer.Commits);
            var parked = PlatformMessageConsumerService.ReadHeaders(producer.Produced.Single().Message.Headers);
            Assert.AreEqual(messageId, parked["x-bip-message-id"]);
            Assert.AreEqual(
                kind == "too-long"
                    ? $"The Message Store cannot hold a step outcome of the record (SQLSTATE 54000); the send of step deliver had returned, its outcome not recorded (record {options.InputTopic}/0@0)"
                    : $"The Message Store cannot hold a step outcome of the record (SQLSTATE 22021); no send of the attempt had returned without a recorded outcome (record {options.InputTopic}/0@0)",
                parked["x-bip-error"]);
            Assert.AreEqual("DLQ", await LogStatusAsync(store.Admin, messageId));
        });
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task AfterThreeRecordsInARowWhoseOutcomeTheStoreRefusedTheFourthIsHeldBack(bool handlerFails)
    {
        await WithIsolatedStoreAsync(async store =>
        {
            var options = store.Options();
            options.MaxHandlerAttempts = 1;
            var logs = new LogRecorder();
            var (service, steps, _, producer) = store.Consumer(options, logs);
            var handler = handlerFails
                ? new DelegateHandler((_, _) => Task.FromResult(PlatformMessageResult.Failed("synthetic handler failure")))
                : SendingHandler(steps, () => { });
            using var consumerService = service(handler);
            // A setting or column that refuses every full outcome row.
            await store.FailWritesAsync("bip_message_attempt", "22000", "$1->>'correlation_id' is not null");
            var id = $"dotnet-test-{Guid.NewGuid():N}";
            var records = Enumerable.Range(0, 5).Select(offset => RecordAt(offset, options, $"{id}-{offset}")).ToList();
            using var stopping = new CancellationTokenSource();
            var consumer = new ReadAgainConsumer(stopping, records, maxReads: 5);

            await consumerService.ConsumeAsync(consumer, stopping.Token).WaitAsync(TimeSpan.FromSeconds(60));

            CollectionAssert.AreEqual(new long[] { 0, 1, 2 }, consumer.Commits);
            CollectionAssert.AreEqual(new[] { $"{id}-0", $"{id}-1", $"{id}-2", $"{id}-3", $"{id}-3" }, handler.Calls);
            // A failed handler's message was parked before its DLQ outcome was
            // refused, and is not parked again on the next read.
            Assert.AreEqual(handlerFails ? 4 : 0, producer.Produced.Count, "A completed message is not parked.");
            Assert.AreEqual(handlerFails ? "DLQ" : "COMPLETED", await LogStatusAsync(store.Admin, $"{id}-0"));
            Assert.IsNull((await ReducedRowAsync(store.Admin, $"{id}-0")).Correlation);
            Assert.AreEqual(3, logs.Errors.Count(error => error.Contains(
                handlerFails
                    ? "was parked: The Message Store cannot hold the record's outcome (SQLSTATE 22000) storeError=record-data"
                    : "was recorded without its envelope values: The Message Store cannot hold the record's outcome (SQLSTATE 22000) storeError=record-data",
                StringComparison.Ordinal)));
            Assert.AreEqual(2, logs.Errors.Count(error => error.StartsWith($"Message {id}-3 at {options.InputTopic}/0@3 is not committed: 3 records in a row on this partition could not be stored for their own data", StringComparison.Ordinal)
                && error.EndsWith("storeError=schema", StringComparison.Ordinal)));
        });
    }

    private static DelegateHandler SendingHandler(IPlatformStepExecutor steps, Action send) =>
        new(async (message, ct) =>
        {
            await steps.ExecuteAsync(message, "deliver", DeliverUri, "target", _ => { send(); return Task.CompletedTask; }, ct);
            return PlatformMessageResult.Completed();
        });

    private static ConsumeResult<string, byte[]> RecordAt(
        long offset, PlatformMessageStoreOptions options, string? messageId, params (string Name, string Value)[] headers) =>
        RecordAt(offset, options, messageId, messageId ?? "key", headers);

    private static ConsumeResult<string, byte[]> RecordAt(
        long offset, PlatformMessageStoreOptions options, string? messageId, string key, params (string Name, string Value)[] headers)
    {
        var kafkaHeaders = new Headers();
        if (messageId is not null)
        {
            kafkaHeaders.Add("x-bip-message-id", Encoding.UTF8.GetBytes(messageId));
        }

        foreach (var (name, value) in headers)
        {
            kafkaHeaders.Add(name, Encoding.UTF8.GetBytes(value));
        }

        return new ConsumeResult<string, byte[]>
        {
            TopicPartitionOffset = new TopicPartitionOffset(options.InputTopic, new Partition(0), new Offset(offset)),
            Message = new Message<string, byte[]> { Key = key, Value = """{"messageType":"e2e.order"}"""u8.ToArray(), Headers = kafkaHeaders },
        };
    }

    private static ConsumeResult<string, byte[]> RecordAt(long offset, Message<string, byte[]> message) => new()
    {
        TopicPartitionOffset = new TopicPartitionOffset("bip.integration-e2e-dotnet.in", new Partition(0), new Offset(offset)),
        Message = message,
    };

    private static async Task<(string Status, string? Correlation, string? ContentType, string? Source, string? Target, string? Error)> ReducedRowAsync(
        NpgsqlDataSource dataSource, string messageId)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            """
            select attempt.status, attempt.correlation_id, attempt.content_type, attempt.source_system, attempt.target_system, log.error
            from bip_message_attempt attempt join bip_message_log log using (message_id)
            where message_id = @id and attempt.attempt = 1
            """,
            connection);
        command.Parameters.AddWithValue("id", messageId);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.IsTrue(await reader.ReadAsync(), $"No outcome for {messageId}.");
        string? Text(int column) => reader.IsDBNull(column) ? null : reader.GetString(column);
        return (reader.GetString(0), Text(1), Text(2), Text(3), Text(4), Text(5));
    }

    /// <summary>
    /// A Message Store of its own: a schema with the store's tables, a login
    /// that can only use it, and rules that make its writes fail. The
    /// adapter's connections are not pooled, so a role setting reaches the
    /// next one.
    /// </summary>
    private sealed class IsolatedStore(string schema, string role, string adapterConnectionString, NpgsqlDataSource admin, NpgsqlDataSource superuser)
    {
        public NpgsqlDataSource Admin => admin;

        public PlatformMessageStoreOptions Options(int? commandTimeoutSeconds = null)
        {
            var builder = new NpgsqlConnectionStringBuilder(adapterConnectionString);
            if (commandTimeoutSeconds is { } seconds)
            {
                builder.CommandTimeout = seconds;
            }

            return CreateOptions(builder.ConnectionString);
        }

        public (Func<IPlatformMessageHandler, PlatformMessageConsumerService> Service, PlatformStepExecutor Steps, PlatformMessageStore Store, FakeProducer Producer) Consumer(
            PlatformMessageStoreOptions options, LogRecorder logs, Func<string, Message<string, byte[]>, Task>? onProduce = null)
        {
            var dataSource = NpgsqlDataSource.Create(PlatformStoreTimeouts.Apply(options.PostgresConnectionString));
            var observations = new PlatformStepObservationCollector();
            var store = new PlatformMessageStore(dataSource, observations, Microsoft.Extensions.Options.Options.Create(options), logs.For<PlatformMessageStore>())
            {
                OutcomeWriteRetryDelay = TimeSpan.Zero,
            };
            var steps = new PlatformStepExecutor(dataSource, observations, logs.For<PlatformStepExecutor>())
            {
                DeliveredWriteRetryWaits = [TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero],
            };
            var producer = new FakeProducer(onProduce);
            return (handler => new PlatformMessageConsumerService(
                handler, store, observations, producer, Microsoft.Extensions.Options.Options.Create(options), logs.For<PlatformMessageConsumerService>())
            {
                FailureBackoff = TimeSpan.Zero,
            }, steps, store, producer);
        }

        public void Break(string fault) => Fault(fault, broken: true);

        public void Repair(string fault) => Fault(fault, broken: false);

        public Task FailWritesAsync(string table, string sqlState, string condition = "true") => ExecuteAsync(
            $"insert into {schema}.synthetic_failure (table_name, sql_state, condition) values ('{table}', '{sqlState}', '{condition.Replace("'", "''")}')");

        public void RepairWrites() => ExecuteAsync($"delete from {schema}.synthetic_failure").GetAwaiter().GetResult();

        private void Fault(string fault, bool broken)
        {
            switch (fault)
            {
                case "read-only":
                    ExecuteAsync(broken
                        ? $"alter role {role} set default_transaction_read_only = on"
                        : $"alter role {role} reset default_transaction_read_only").GetAwaiter().GetResult();
                    break;
                case "revoked":
                    ExecuteAsync(broken
                        ? $"revoke insert, update on {schema}.bip_step_outcome from {role}"
                        : $"grant insert, update on {schema}.bip_step_outcome to {role}").GetAwaiter().GetResult();
                    break;
                case "synchronous-standby":
                    SetStandby(broken ? "'unanswered_synthetic_standby'" : null);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(fault), fault, null);
            }
        }

        // Makes the server wait for a synchronous standby that does not exist,
        // as WithUnansweredSynchronousStandbyAsync does; the test's own
        // sessions opt out with synchronous_commit=local.
        private void SetStandby(string? standby)
        {
            ExecuteAsync(standby is null
                ? "alter system reset synchronous_standby_names; select pg_reload_conf()"
                : $"alter system set synchronous_standby_names = {standby}; select pg_reload_conf()").GetAwaiter().GetResult();
            for (var i = 0; i < 50; i++)
            {
                using var connection = superuser.OpenConnection();
                using var show = new NpgsqlCommand("show synchronous_standby_names", connection);
                if ((string?)show.ExecuteScalar() == (standby?.Trim('\'') ?? string.Empty))
                {
                    return;
                }

                Thread.Sleep(100);
            }
        }

        private async Task ExecuteAsync(string sql)
        {
            await using var connection = await superuser.OpenConnectionAsync();
            await using var command = new NpgsqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    private static async Task WithIsolatedStoreAsync(Func<IsolatedStore, Task> test)
    {
        var connectionString = RequireConnectionString();
        var id = Guid.NewGuid().ToString("N");
        var schema = $"bip_store_{id}";
        var role = $"bip_store_{id}";
        var password = $"synthetic-{id}";
        var local = new NpgsqlConnectionStringBuilder(connectionString) { Options = "-c synchronous_commit=local" };
        await using var superuser = NpgsqlDataSource.Create(local.ConnectionString);
        await using (var connection = await superuser.OpenConnectionAsync())
        {
            await using var create = new NpgsqlCommand(
                $"""
                create role {role} login password '{password}';
                create schema {schema};
                create table {schema}.bip_message_log (like public.bip_message_log including all);
                create table {schema}.bip_message_attempt (like public.bip_message_attempt including all);
                create table {schema}.bip_step_outcome (like public.bip_step_outcome including all);
                create table {schema}.bip_message_replay_audit (like public.bip_message_replay_audit including all);
                grant usage on schema {schema} to {role};
                grant select, insert, update on all tables in schema {schema} to {role};
                create table {schema}.synthetic_failure (table_name text not null, sql_state text not null, condition text not null);
                create function {schema}.synthetic_failure() returns trigger language plpgsql security definer as $$
                declare
                    rule record;
                    matched boolean;
                begin
                    for rule in select * from {schema}.synthetic_failure where table_name = tg_table_name loop
                        execute 'select ' || rule.condition using to_jsonb(new) into matched;
                        if matched then
                            raise exception 'synthetic store failure' using errcode = rule.sql_state;
                        end if;
                    end loop;
                    return new;
                end $$;
                create trigger synthetic_failure before insert or update on {schema}.bip_step_outcome
                    for each row execute function {schema}.synthetic_failure();
                create trigger synthetic_failure before insert or update on {schema}.bip_message_attempt
                    for each row execute function {schema}.synthetic_failure();
                create trigger synthetic_failure before insert or update on {schema}.bip_message_log
                    for each row execute function {schema}.synthetic_failure();
                """,
                connection);
            try
            {
                await create.ExecuteNonQueryAsync();
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
            {
                Assert.Inconclusive("Needs a login that can create roles.");
            }
        }

        try
        {
            var adapter = new NpgsqlConnectionStringBuilder(connectionString)
            {
                Username = role,
                Password = password,
                SearchPath = schema,
                Pooling = false,
            };
            await using var admin = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(local.ConnectionString) { SearchPath = schema }.ConnectionString);
            await test(new IsolatedStore(schema, role, adapter.ConnectionString, admin, superuser));
        }
        finally
        {
            await using var connection = await superuser.OpenConnectionAsync();
            await using var drop = new NpgsqlCommand($"drop schema {schema} cascade; drop role {role}", connection);
            await drop.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Hands out records from one partition and, after a seek, the record at
    /// the offset sought, as Kafka does; requests shutdown after the last
    /// record or the given number of reads.
    /// </summary>
    private sealed class ReadAgainConsumer(
        CancellationTokenSource stopping, IReadOnlyList<ConsumeResult<string, byte[]>> records, int maxReads, Action<int>? beforeRead = null)
        : IConsumer<string, byte[]>
    {
        private int next;

        public int Reads { get; private set; }

        public List<long> Commits { get; } = [];

        public Func<Exception?>? FailCommit { get; init; }

        public ConsumeResult<string, byte[]> Consume(CancellationToken cancellationToken = default)
        {
            if (next >= records.Count || Reads >= maxReads)
            {
                stopping.Cancel();
                cancellationToken.ThrowIfCancellationRequested();
            }

            Reads++;
            beforeRead?.Invoke(Reads);
            return records[next++];
        }

        public void Commit(ConsumeResult<string, byte[]> result)
        {
            if (FailCommit?.Invoke() is { } failure)
            {
                throw failure;
            }

            Commits.Add(result.Offset.Value);
        }

        public void Seek(TopicPartitionOffset tpo) =>
            next = records.Select((record, index) => (record, index)).First(entry => entry.record.Offset == tpo.Offset).index;

        public ConsumeResult<string, byte[]> Consume(int millisecondsTimeout) => throw new NotSupportedException();

        public ConsumeResult<string, byte[]> Consume(TimeSpan timeout) => throw new NotSupportedException();

        public List<TopicPartitionOffset> Commit() => throw new NotSupportedException();

        public void Commit(IEnumerable<TopicPartitionOffset> offsets) => throw new NotSupportedException();

        public string MemberId => "read-again-member";

        public List<TopicPartition> Assignment => [];

        public List<string> Subscription => [];

        public IConsumerGroupMetadata ConsumerGroupMetadata => throw new NotSupportedException();

        public Handle Handle => throw new NotSupportedException();

        public string Name => "read-again-consumer";

        public void Subscribe(IEnumerable<string> topics)
        {
        }

        public void Subscribe(string topic)
        {
        }

        public void Unsubscribe()
        {
        }

        public void Assign(TopicPartition partition) => throw new NotSupportedException();

        public void Assign(TopicPartitionOffset partition) => throw new NotSupportedException();

        public void Assign(IEnumerable<TopicPartitionOffset> partitions) => throw new NotSupportedException();

        public void Assign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions) => throw new NotSupportedException();

        public void IncrementalAssign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public void IncrementalUnassign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public void Unassign() => throw new NotSupportedException();

        public void StoreOffset(ConsumeResult<string, byte[]> result) => throw new NotSupportedException();

        public void StoreOffset(TopicPartitionOffset offset) => throw new NotSupportedException();

        public void Pause(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public void Resume(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

        public List<TopicPartitionOffset> Committed(TimeSpan timeout) => throw new NotSupportedException();

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout) => throw new NotSupportedException();

        public Offset Position(TopicPartition partition) => throw new NotSupportedException();

        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
            => throw new NotSupportedException();

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition) => throw new NotSupportedException();

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) => throw new NotSupportedException();

        public void Close()
        {
        }

        public int AddBrokers(string brokers) => 0;

        public void SetSaslCredentials(string username, string password)
        {
        }

        public void Dispose()
        {
        }
    }
}
