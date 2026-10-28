# Basis .NET Message Store Adapter

Implements message-store contract v1.0.0-rc2. The adapter is self-contained:
this README describes the tables, topics and headers it uses, and the schema it
expects is in `tests/fixtures/postgres/`.

NuGet package source for `.NET` integrations that need the Basis platform
message-store contract.

Package (NuGet.org):

```text
Basis.IntegrationPlatform.MessageStore
```

The adapter owns platform mechanics for .NET integrations:

- consumes the platform input topic `APP_KAFKA_INPUT_TOPIC`
  (default `bip.<APP_NAME>.in`)
- calls application code through `IPlatformMessageHandler`
- takes the platform attempt number from the `x-bip-attempt` header
  (absent means attempt 1) and upserts the `(message_id, attempt)` row so
  infrastructure redeliveries re-record the same attempt
- records terminal status in `bip_message_log` and `bip_message_attempt`
- records per-attempt step observations (`DELIVERED`/`FAILED`/`SKIPPED`)
  in `bip_message_attempt.steps_json`
- records idempotent external step outcomes in `bip_step_outcome`
- stores replayable payload bytes in Postgres under the payload policy
  (`replayable` = payload captured, not expired, not deleted — independent
  of message status)
- retries handler failures before terminal `COMPLETED` or `DLQ`
- publishes failed messages to `APP_KAFKA_DLQ_TOPIC`
  (default `bip.message.dlq`) with `x-bip-error` and `x-bip-dlq-at`. The DLQ
  record also carries the other consumed headers; the values of
  credential-like ones are masked as `xxxxxx`: `Authorization`, `Cookie`,
  `Set-Cookie`, `WWW-Authenticate`, and names containing secret, token,
  password, credential, session, signature, auth or API/subscription key, and
  names ending in `-key` or `_key` (`x-functions-key`). Their names, `x-bip-*`
  and `Idempotency-Key` stay readable
- exposes `POST /admin/messages/{messageId}/resend`
- republishes replay payloads back to the integration input topic
  (`bip.<integration>.in`), committing the `REQUESTED` audit row before the
  Kafka produce
- verifies the provisioned contract major in `bip_contract` at startup and
  refuses to start on a mismatch (missing table logs a loud warning during
  the v1.0 rollout)

## Integration Usage

```csharp
builder.Services.AddBasisMessageStoreAdapter<MyMessageHandler>();

app.MapBasisMessageStoreAdminEndpoints();
```

Application code implements:

```csharp
public sealed class MyMessageHandler : IPlatformMessageHandler
{
    private readonly IPlatformStepExecutor steps;

    public MyMessageHandler(IPlatformStepExecutor steps)
    {
        this.steps = steps;
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
                await SendToTargetSystem(message, ct);
            },
            cancellationToken);

        return PlatformMessageResult.Completed();
    }
}
```

The step key is `<message-id>:<step-id>`, and a step whose key is already
`DELIVERED` in `bip_step_outcome` is skipped. Choose step ids with that in mind:

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
The second warning reads `bip_message_log`. Under runtime isolation, row-level
security hides the other integration's row, so that warning never fires there.
If the read fails, the step is still skipped and the check is logged at debug.

## Required Runtime Environment

The shared `integration-app` chart injects these when the catalog enables both
Kafka and Postgres:

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
BASIS_MESSAGE_STORE_SINK
BASIS_MESSAGE_STORE_STEP_OUTCOME_STORE
BASIS_MESSAGE_STORE_REPLAY_INPUT_TOPIC_PATTERN
```

## Payload policy

Identical semantics across runtime adapters (contract v1 §6):

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
`publish-prerelease.yml` workflow and never tagged. A pre-release is an
uncertified test build: use one only by pinning its exact version, never as a
production dependency. `docs/releasing.md` in the repository describes both
lanes. Per ADR-003 the public registry is the canonical channel;
tenant-registry fan-out remains an opt-in overlay for restricted environments
and is operated from the platform's internal infrastructure.

## License and support

Apache-2.0. The adapter is developed as part of the Basis integration
platform; production use is supported for platform customers through their
platform agreement. Issues and pull requests from other users are welcome and
handled best-effort.

## Managed payload capture profile

The managed runtime keeps the v1 Kafka consumer, Postgres lifecycle journal,
step-outcome ledger, retry/DLQ handling and admin adapter active independently
of payload storage. `BASIS_MESSAGE_STORE_PAYLOAD_MODE=metadata-only` processes
messages but records no replay payload: lifecycle rows remain, with
`replayable=false` and a reason. `postgres-inline` restores payload capture
for subsequent deliveries subject to the existing size and retention policy.

This is not a no-storage mode: Kafka input/DLQ records retain their payloads
under Kafka retention, and Postgres retains lifecycle and step metadata.
Switching modes neither purges earlier history nor captures messages
retroactively. Existing replayable history is evaluated per message by the
adapter; a replay processed with capture off records a metadata-only new
attempt. Earlier attempts and the replay audit remain historical evidence.
The latest lifecycle row follows the latest attempt's capture policy.

Runtime stop/start remains deployment replica state. Do not disable the sink
or step ledger to implement payload capture off: durable recording before
commit remains mandatory. Missing or failed journal persistence must still
prevent successful acknowledgement. No new adapter package or database schema
is required for this profile; published .NET 0.3.0 and Camel 0.3.1 already
implement the required payload modes. A coordinated managed projection release
is required; do not infer fleet adoption from this documentation change.

## Operator-owned runtime security

Existing deployments keep plaintext Kafka unless configured. For authenticated
TLS, inject these settings through the platform's secret/runtime bindings:

| Environment variable | Default / purpose |
| --- | --- |
| `BASIS_MESSAGE_STORE_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT`; use `SASL_SSL` for authenticated TLS (`SSL`, `SASL_PLAINTEXT` also recognized) |
| `BASIS_MESSAGE_STORE_KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512`; also `SCRAM-SHA-256` or `PLAIN` |
| `BASIS_MESSAGE_STORE_KAFKA_SASL_USERNAME` | Required for SASL; inject approved principal |
| `BASIS_MESSAGE_STORE_KAFKA_SASL_PASSWORD` | Required for SASL; secret binding only |
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
