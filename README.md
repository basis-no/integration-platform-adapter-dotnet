# .NET Message Store Adapter

.NET runtime adapter for message auditing, Kafka recovery and controlled
replay. The adapter is self-contained: this README describes the tables, topics
and headers it uses, and the schema it expects (store schema major 1) is in
`tests/fixtures/postgres/`. The
[Camel Quarkus adapter](https://github.com/basis-no/integration-platform-adapter-camel-quarkus)
implements the same message store.

Package (NuGet.org):

```text
Basis.IntegrationPlatform.MessageStore
```

The adapter owns the message-store mechanics of a .NET integration:

- consumes the input topic `APP_KAFKA_INPUT_TOPIC`
  (default `bip.<APP_NAME>.in`)
- calls application code through `IPlatformMessageHandler`
- takes the attempt number from the `x-bip-attempt` header
  (absent means attempt 1) and upserts the `(message_id, attempt)` row so
  infrastructure redeliveries re-record the same attempt
- records terminal status in `bip_message_log` and `bip_message_attempt`
- records per-attempt step observations (`DELIVERED`/`FAILED`/`SKIPPED`)
  in `bip_message_attempt.steps_json`
- records idempotent external step outcomes in `bip_step_outcome`. The
  recorded endpoint URI, there and in `steps_json`, has its user info and the
  values of credential-like query parameters masked as `xxxxxx`, at least as
  the Camel adapter masks it (see the step section below). Only the recorded
  copy is masked
- stores replayable payload bytes in Postgres under the payload policy
  (`replayable` = payload captured, not expired, not deleted — independent
  of message status)
- retries handler failures before terminal `COMPLETED` or `DLQ`. A
  cancellation is shutdown only when the host is stopping; any other, such as
  an `HttpClient` timeout, is a handler failure, and a timed-out step is
  recorded as `FAILED`. Recoverable Kafka consume errors are logged and
  retried; a fatal one stops the consumer
- publishes failed messages to `APP_KAFKA_DLQ_TOPIC`
  (default `bip.message.dlq`) with `x-bip-error` and `x-bip-dlq-at`. The DLQ
  record also carries the other consumed headers; the values of
  credential-like ones are masked as `xxxxxx`: `Authorization`, `Cookie`,
  `Set-Cookie`, `WWW-Authenticate`, and names containing secret, token,
  password, credential, session, signature, auth or API/subscription key, and
  names ending in `-key` or `_key` (`x-functions-key`). Their names, `x-bip-*`
  and `Idempotency-Key` stay readable
- exposes `POST /admin/messages/{messageId}/resend` for messages this
  integration journaled. A message of another integration on the same
  Message Store is refused with `409 NOT_REPLAYABLE` before any audit row or
  produce
- republishes replay payloads back to the integration input topic
  (`bip.<integration>.in`), committing the `REQUESTED` audit row before the
  Kafka produce
- verifies the store schema major in `bip_contract` at startup and refuses
  to start on a mismatch (a missing or empty table logs a loud warning and
  the adapter assumes major 1)

## Integration Usage

```csharp
builder.Services.AddHttpClient("target-system", client => client.BaseAddress = new Uri("https://target.example/"));
builder.Services.AddBasisMessageStoreAdapter<MyMessageHandler>();

app.MapBasisMessageStoreAdminEndpoints();
```

Application code implements:

```csharp
public sealed class MyMessageHandler : IPlatformMessageHandler
{
    private readonly IPlatformStepExecutor steps;
    private readonly IHttpClientFactory httpClients;

    public MyMessageHandler(IPlatformStepExecutor steps, IHttpClientFactory httpClients)
    {
        this.steps = steps;
        this.httpClients = httpClients;
    }

    public async Task<PlatformMessageResult> HandleAsync(
        PlatformMessage message,
        CancellationToken cancellationToken)
    {
        await steps.ExecuteWithContextAsync(
            message,
            stepId: "target-system",
            endpointUri: "https://target.example/api/orders",
            targetRef: message.TargetSystem,
            operation: async (step, ct) =>
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, "api/orders")
                {
                    Content = new ByteArrayContent(message.Payload),
                };
                request.Content.Headers.ContentType = MediaTypeHeaderValue.Parse(message.ContentType);
                // The step's stable key lets the receiver deduplicate. The
                // Camel adapter sends it under both names.
                request.Headers.Add("Idempotency-Key", step.IdempotencyKey);
                request.Headers.Add("x-bip-idempotency-key", step.IdempotencyKey);
                using var response = await httpClients.CreateClient("target-system").SendAsync(request, ct);
                // HttpClient returns a 4xx or 5xx response without throwing.
                response.EnsureSuccessStatusCode();
            },
            cancellationToken);

        return PlatformMessageResult.Completed();
    }
}
```

The step is recorded `DELIVERED` when its operation returns and `FAILED` when
it throws, so the operation must throw when the send fails. `HttpClient` does
not throw on a 4xx or 5xx response by itself: without
`EnsureSuccessStatusCode()`, or a status check that throws, a 500 is recorded
`DELIVERED`, the message completes, and a resend skips the step, so the
receiver never gets the message. The same happens when the operation catches
its own failure, and with any client that reports a failed send in its result
instead of throwing. To park the message on a failed send, let the exception
leave the operation: the handler can catch it and return
`PlatformMessageResult.Failed`, or let it propagate, and the adapter retries
the handler and then parks the message.

The step's own record in the Message Store can fail too. An operation that
returned is never recorded `FAILED`: the executor retries the `DELIVERED`
write in place (three attempts), and if it still fails, it throws
the store's exception without writing `FAILED`; the step then runs again on
the next delivery, so send its idempotency key to the receiver. A failed
ledger read before the step throws without running the operation. Either way
the adapter notes the store failure outside the exception: if the handler
catches it and returns success anyway, the adapter does not record the message
`COMPLETED` or commit its offset, but processes the delivery again after the
failure backoff, without parking it. This includes a ledger call cut short by
the handler's own cancellation token, such as a timeout. A ledger write that
fails every time for one message, with a handler that swallows it, therefore
blocks its partition and calls the receiver again on each pass; the error log
names the message and step.

The step key is `<message-id>:<step-id>`, and a step whose key is already
`DELIVERED` in `bip_step_outcome` is skipped, unless the message's
`x-bip-redo-steps` header (a JSON array of step ids or step keys) names it.
`ExecuteWithContextAsync` passes the operation a `PlatformStepExecutionContext`
before the call, with `StepId` and `IdempotencyKey`, the key
`bip_step_outcome` records. The key is stable across redelivery, replay and
explicit redo. Send it to the receiver, as in the example, so the receiver can
deduplicate a call it already applied: after a crash between the call and the
ledger write, and on a redo, which sends the same key. `ExecuteAsync`, whose
operation receives only the cancellation token, works as before.

Choose step ids with that in mind:

- Give every external call its own step id. A handler that sends several items
  of one message through one step id, such as a loop over order lines, delivers
  the first item and skips the rest. Put the item's stable key in the step id
  (`post-line-{lineId}`).
- The step outcome table has no integration column, so the lookup is not scoped
  by integration. If a message id travels to another integration on the same
  Message Store and both use the same step id, the second integration's step is
  skipped as already delivered. Use step ids unique across integrations, and
  when one integration produces for another, give the new message its own
  `x-bip-message-id` and carry continuity in `x-bip-correlation-id`, which
  then also has to be the record key if replays should keep its partition
  (see Replay order).

When the step executor skips a step that the same handler invocation already
delivered, it logs an ERROR, once per skipped call, with the step id and the
message id: that call is not sent, yet the message completes. A skip of a step
that an earlier attempt delivered, on a redelivery or a replay, logs no error;
a further call with the same step id in that invocation logs a warning, since
the adapter cannot tell which item the earlier attempt sent. When the skipped
message id is journaled by another integration, the adapter logs a warning.
That warning reads `bip_message_log`. Where row-level security limits
each integration to its own rows, the other integration's row is hidden, so that
warning never fires there.
If the read fails, the step is still skipped and the check is logged at debug.

The endpoint URI a step records is masked first the way the Camel adapter
masks it, so both adapters record the same value for every credential that
adapter masks:

- the password in the user info (`https://user:xxxxxx@host`);
- the value of a query parameter whose name contains, in any case, one of the
  option names Camel 4.18 treats as sensitive, such as `password`,
  `passphrase`, `secret`, `token`, `user`, `login`, `clientid`, `apikey`,
  `api_key` or `privatekey`. The value is masked up to the next `&`, a
  fragment included, and a `RAW(...)` or `RAW{...}` value as a whole.

This adapter then masks more: the whole user info (`https://xxxxxx@host`, also
when it holds only a token), and the values of query parameters named by the
DLQ header rule above, named `key`, `code`, `sig` or `pwd`, or containing
passphrase, passcode, privatekey, accesskey, accountkey or connectionstring.
A URI the patterns cannot mask within a second is recorded as `xxxxxx`. The
operation calls the URI it was given; rows written earlier are unchanged.

## Required Runtime Environment

The deployment sets these environment variables:

```text
APP_NAME
APP_ENVIRONMENT
APP_TENANT
KAFKA_BOOTSTRAP_SERVERS
APP_KAFKA_INPUT_TOPIC
APP_KAFKA_DLQ_TOPIC
POSTGRES_JDBC_URL
POSTGRES_USERNAME
POSTGRES_PASSWORD
BASIS_MESSAGE_STORE_REPLAY_INPUT_TOPIC_PATTERN
```

Startup fails without `KAFKA_BOOTSTRAP_SERVERS` and a Postgres connection:
either `POSTGRES_CONNECTION_STRING`, or `POSTGRES_JDBC_URL` with the
credentials in `POSTGRES_USERNAME` and `POSTGRES_PASSWORD`. `APP_NAME`
defaults to `dotnet-integration`; set it, because it also names the Kafka
consumer group and the default input topic. The input and DLQ topics default
as listed above, and the replay topic pattern defaults to `bip.%s.in`.

## Step outcome store

| Environment variable | Default | Meaning |
|---|---|---|
| `BASIS_MESSAGE_STORE_STEP_OUTCOME_STORE` | `postgres` | `postgres` or `noop` |

With `postgres`, delivered steps are kept in `bip_step_outcome` and a replay
skips them, as described under Integration Usage. With `noop`, nothing is read
from or written to `bip_step_outcome`: every step runs on every delivery,
replays included, and `steps_json` still records what each attempt observed.
A resend whose `redoSteps` names a step is then refused with
`409 NOT_REPLAYABLE` before any audit row or produce, because every step runs
anyway; a resend without `redoSteps` works as usual. The value is trimmed and
read case-insensitively; an empty value means the default, and any other value
fails startup. The setting reaches the step executor through
`AddBasisMessageStoreAdapter`; a `PlatformStepExecutor` constructed directly
uses `bip_step_outcome`. The variable has the same name and values as in the
Camel adapter, whose library default is `noop`; this adapter's default stays
`postgres`.

Switching from `noop` back to `postgres` keeps the rows written before the
`noop` period: a step delivered before the `noop` period is skipped on a later
replay, even if it ran again and failed under `noop`. Name such a step in `redoSteps` to run it.

## Payload policy

The same semantics as the Camel Quarkus adapter:

| Environment variable | Default | Meaning |
|---|---|---|
| `BASIS_MESSAGE_STORE_PAYLOAD_MODE` | `postgres_inline` | `postgres_inline` or `metadata_only` |
| `BASIS_MESSAGE_STORE_PAYLOAD_MAX_BYTES` | `1048576` | Payloads larger than this degrade to `metadata_only` with a `replay_disabled_reason` |
| `BASIS_MESSAGE_STORE_PAYLOAD_RETENTION_DAYS` | `30` | Sets `payload_expires_at` on captured payloads |

Inline capture sets expiry to the earlier of capture time plus retention and
first receipt plus twice retention, matching the Camel adapter. First receipt
is the earliest `occurred_at` in this integration's attempt history, or capture
time when there is none. A replay shortly before expiry can renew it up to
that cap; repeated replays never extend it beyond the cap. The summary and
new attempt store the same expiry, and earlier attempts keep theirs. Consuming
attempt 1 again rewrites its `occurred_at`, so the cap can move when that row
was the earliest receipt; replays write new attempts and do not move it.

`metadata_only` rows store no `payload_bytes` and are `replayable=false` with
a reason. Replayability is a property of payload availability, never of the
message outcome: `COMPLETED` messages are resendable as long as the payload
is captured, unexpired, and undeleted.

## Replay order

A resend publishes a new record to the replay topic; it never returns to the
original offset. The replay topic is
`BASIS_MESSAGE_STORE_REPLAY_INPUT_TOPIC_PATTERN` with `%s` replaced by the
integration name (default `bip.%s.in`). If `APP_KAFKA_INPUT_TOPIC` names
another topic, set the pattern to match, or the replay is published to a topic
this integration does not consume. The record's Kafka key is the message's
correlation id: `x-bip-correlation-id`, else the original record's key when it
has one (an empty key is kept as is), else the message id. A replay is
appended after everything already published, and Kafka orders records only
within a partition, so:

- records for the same key that were published after the original may already
  have been processed. Replaying an old failure after a newer update for the
  same entity can restore stale data at the receiver;
- resending several messages has no overall order. Resend one key's messages
  oldest first, one request at a time, waiting for each response before the
  next, and do not resend a failure that a newer message for the same key has
  superseded;
- a replay reliably lands on the original's partition only when it goes to the
  same topic, the partition count is unchanged, the original record had the
  same non-empty key and no explicit partition, and both producers hash keys
  the same way. Keep `x-bip-correlation-id` equal to the record key, or leave
  it out. This adapter's producer uses the .NET Kafka client's default
  partitioner (`consistent_random`, CRC32), which can place a key on a
  different partition than the Java client's default (`murmur2`).

## Kafka activity metrics

From 0.4, admission activity is enabled by default. The adapter exports through
`System.Diagnostics.Metrics` under the meter name
`Basis.IntegrationPlatform.MessageStore`. Subscribe to that meter in the
application's exporter (for example OpenTelemetry); the adapter adds no metrics
endpoint or exporter dependency.

| Metric | Labels | Meaning |
| --- | --- | --- |
| `integration_traffic_events_total` | `signal` | Process-local count for `record_admitted` or `record_published` while supported |
| `integration_traffic_signal_supported` | `signal` | `1` for a selected, supported signal; `0` means unknown coverage |
| `integration_traffic_process_start_time_seconds` | none | Process start time as Unix seconds |

`record_admitted` counts every input record handed to processing, including
Kafka redeliveries, once per consumed delivery. In-process handler retries do
not add admissions. This measures Kafka activity, not business completion or
unique messages.

`BASIS_TRAFFIC_SIGNALS` is a comma-separated selection of `record_admitted` and
`record_published`; its default is `record_admitted`. Set it to an empty string
to disable both; the adapter then registers no instruments. Publication is
opt-in: select `record_published` and set `BASIS_TRAFFIC_INPUT_TOPICS` to the
exact, comma-separated destination topics that count (no wildcards). Only
broker-acknowledged records sent through the adapter's registered
`IProducer<string, byte[]>` count, including callback-based sends. Failed and
unacknowledged sends add nothing. Application-owned producers are outside this
coverage, and so is a producer built from the registered producer's `Handle`
(`DependentProducerBuilder`); replacing the registered producer leaves
publication unsupported. DLQ and replay sends count only if their destination
is listed.

Unselected signals, invalid signal names or topic lists, a missing producer or
consumer boundary, and ambiguous acknowledgement metadata report `supported=0`.
Transactional sends and cancellation before an observed ACK also make
publication coverage unknown, and so does admission of a record from a topic
other than `APP_KAFKA_INPUT_TOPIC` (as with a regex subscription). Coverage
that becomes unknown stays unknown until the process restarts, and the adapter
logs a warning naming the signal the first time. Unknown coverage and absent
metrics must never be interpreted as zero traffic. Counters reset when the
process restarts; no topic, message, integration, payload or header values
become metric labels. Options `TrafficSignals` and `TrafficInputTopics` can
also be set in registration.

## Publish

Releases are published to NuGet.org as `Basis.IntegrationPlatform.MessageStore`
by `.github/workflows/publish-nuget.yml` when a `vX.Y.Z` tag is pushed (NuGet
Trusted Publishing); the version comes from the tag. Pre-releases for
acceptance testing, `X.Y.Z-preview.<date>.<run>`, are published by the manual
`publish-prerelease.yml` workflow and never tagged. A pre-release is a test
build: use one only by pinning its exact version, never as a production
dependency. `docs/releasing.md` in the repository describes both lanes.
NuGet.org is the canonical channel.

## License

Apache-2.0. Issues and pull requests are welcome and handled best-effort.

## Turning payload capture off

The Kafka consumer, the Postgres lifecycle journal, the step-outcome ledger,
retry/DLQ handling and the resend endpoint stay active independently of
payload storage. `BASIS_MESSAGE_STORE_PAYLOAD_MODE=metadata_only` processes
messages but records no replay payload: lifecycle rows remain, with
`replayable=false` and a reason. `postgres_inline` restores payload capture
for subsequent deliveries subject to the existing size and retention policy.
The hyphenated spellings `metadata-only` and `postgres-inline` are accepted too.

This is not a no-storage mode: Kafka input/DLQ records retain their payloads
under Kafka retention, and Postgres retains lifecycle and step metadata.
Switching modes neither purges earlier history nor captures messages
retroactively. Existing replayable history is evaluated per message by the
adapter; a replay processed with capture off records a metadata-only new
attempt. Earlier attempts and the replay audit remain historical evidence.
The latest lifecycle row follows the latest attempt's capture policy.
Switching modes needs no schema change.

To stop processing, stop the deployment's replicas. Turning payload capture
off does not relax recording: durable recording before commit remains
mandatory, and missing or failed journal persistence still prevents successful
acknowledgement.

## Operator-owned runtime security

Existing deployments keep plaintext Kafka unless configured. For authenticated
TLS, set these through your deployment's secret and runtime configuration:

| Environment variable | Default / purpose |
| --- | --- |
| `BASIS_MESSAGE_STORE_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT`; use `SASL_SSL` for authenticated TLS (`SSL`, `SASL_PLAINTEXT` also recognized) |
| `BASIS_MESSAGE_STORE_KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512`; also `SCRAM-SHA-256` or `PLAIN` |
| `BASIS_MESSAGE_STORE_KAFKA_SASL_USERNAME` | Required for SASL; the approved principal |
| `BASIS_MESSAGE_STORE_KAFKA_SASL_PASSWORD` | Required for SASL; supply it from a secret only |
| `BASIS_MESSAGE_STORE_KAFKA_SSL_CA_LOCATION` | Mounted broker CA file; system trust if unset |
| `BASIS_MESSAGE_STORE_POSTGRES_MAX_POOL_SIZE` | Optional bounded pool size for JDBC-derived settings; previous Npgsql default if unset |
| `BASIS_MESSAGE_STORE_POSTGRES_SSL_MODE` | Optional Npgsql TLS mode, e.g. `VerifyFull` |
| `BASIS_MESSAGE_STORE_POSTGRES_ROOT_CERTIFICATE` | Mounted PostgreSQL CA file |

The same Kafka client configuration covers consumption, DLQ and replay. TLS
certificate and hostname verification stay enabled. Missing SASL credentials
fail validation; the client never silently falls back to plaintext. Keep
credentials out of source, endpoint URLs and logs. An explicit
`POSTGRES_CONNECTION_STRING` takes precedence and must carry its own pool/TLS
settings. Plaintext modes are retained for trusted local compatibility.

Business persistence belongs in a separate application database. The adapter
uses its keyed datasource `PlatformMessageStoreExtensions.MessageStoreDataSourceKey`
for both journal and step persistence. A default datasource is still registered
for backwards compatibility; registering another application datasource will
not redirect adapter writes. Keep application schema migrations in a separate
execution with a scoped migration login, never the Message Store login.

## How this project is developed

Most code and documentation here is written with AI coding agents, directed
and reviewed by Basis maintainers. Every change goes through a pull request,
human review and CI before it is merged. [`AGENTS.md`](AGENTS.md) contains the
rules the agents follow; human contributors follow the same rules.

See [CONTRIBUTING.md](CONTRIBUTING.md) for branches and pull requests.

## Release history

This repository starts from a fresh history. Versions up to 0.3.1 were
developed in an earlier private repository, so this repository has no tags or
release pages for them. Their notes are in [CHANGELOG.md](CHANGELOG.md).
