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
  values of credential-like query parameters masked as `xxxxxx` (the DLQ
  header rule below, plus `key`, `code`, `sig`, `pwd`, passphrase, passcode
  and private, access or account key). Only the recorded copy is masked
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
        await steps.ExecuteAsync(
            message,
            stepId: "target-system",
            endpointUri: "https://target.example/api/orders",
            targetRef: message.TargetSystem,
            operation: async ct =>
            {
                using var content = new ByteArrayContent(message.Payload);
                content.Headers.ContentType = MediaTypeHeaderValue.Parse(message.ContentType);
                using var response = await httpClients.CreateClient("target-system")
                    .PostAsync("api/orders", content, ct);
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

The step key is `<message-id>:<step-id>`, and a step whose key is already
`DELIVERED` in `bip_step_outcome` is skipped, unless the message's
`x-bip-redo-steps` header (a JSON array of step ids or step keys) names it.
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
  `x-bip-message-id` and carry continuity in `x-bip-correlation-id`.

The adapter logs a warning when it skips a step that the same handler invocation
already ran, and when the skipped message id is journaled by another integration.
The second warning reads `bip_message_log`. Where row-level security limits
each integration to its own rows, the other integration's row is hidden, so that
warning never fires there.
If the read fails, the step is still skipped and the check is logged at debug.

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
read case-insensitively; any other value fails startup. The setting reaches the
step executor through `AddBasisMessageStoreAdapter`; a `PlatformStepExecutor`
constructed directly uses `bip_step_outcome`. The variable has the same name and values as in the Camel adapter,
whose library default is `noop`; this adapter's default stays `postgres`.

## Payload policy

The same semantics as the Camel Quarkus adapter:

| Environment variable | Default | Meaning |
|---|---|---|
| `BASIS_MESSAGE_STORE_PAYLOAD_MODE` | `postgres_inline` | `postgres_inline` or `metadata_only` |
| `BASIS_MESSAGE_STORE_PAYLOAD_MAX_BYTES` | `1048576` | Payloads larger than this degrade to `metadata_only` with a `replay_disabled_reason` |
| `BASIS_MESSAGE_STORE_PAYLOAD_RETENTION_DAYS` | `30` | Sets `payload_expires_at` on captured payloads |

`metadata_only` rows store no `payload_bytes` and are `replayable=false` with
a reason. Replayability is a property of payload availability, never of the
message outcome: `COMPLETED` messages are resendable as long as the payload
is captured, unexpired, and undeleted.

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
