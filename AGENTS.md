# .NET adapter

This repository is the .NET Message Store adapter, published on NuGet.org as
`Basis.IntegrationPlatform.MessageStore`. Integrations depend on it to consume their input topic,
record every message in the Message Store (the `bip_*` Postgres tables), commit Kafka offsets only
after that record is durable, park failures in the DLQ and serve resend. Each integration pins an
exact package version. This file is for contributors and the coding agents working with them.
`CLAUDE.md` is a symlink to it.

The Message Store contract is what this adapter shares with the
[Camel adapter](https://github.com/basis-no/integration-platform-adapter-camel-quarkus): headers,
statuses, attempts, steps, DLQ records, resend and the SQL schema. `README.md` describes it,
`tests/fixtures/postgres/` holds the schema, and `SupportedContractMajor` is the store schema major
the adapter accepts. This repository owns the .NET implementation, its configuration and its tests.

Treat this file as good defaults, not hard law. The developer's instructions override it; when a
rule here fights the task, say so plainly and get a human decision.

## What we never compromise on

### 1. The contract defines behavior, in both adapters

Semantics come from the contract as `README.md` and the fixture schema describe it, not from this
code; when they disagree, the code is wrong. The Camel adapter writes the same rows and headers and
readers of the store rely on both behaving alike, so a semantic change is a deliberate contract
change made in both adapters, never here alone.

### 2. Deployed integrations keep working

Integrations upgrade by an explicit change to their package pin. A release accepts the code and
configuration that worked with the previous one: public types and signatures, the environment
variables deployments set, headers, request bodies and defaults. Existing deployments keep
their behavior until they opt in; Kafka stays plaintext until security is configured. Published
packages are immutable; fixes ship as new versions.

### 3. Nothing is acknowledged before it is recorded

An offset is committed only after the attempt is recorded. On give-up the original payload goes
to the DLQ first, then the `DLQ` attempt is recorded, then the offset is committed. If recording
or the DLQ publish fails, the consumer seeks back and processes the same delivery again. The
guarantee is at-least-once; external calls rely on the step ledger and the receiver's idempotency.

### 4. Fail closed

Startup refuses an unknown store schema major in `bip_contract`, and an unreadable `bip_contract`
after bounded retries. `PlatformMessageStoreOptions.Validate` rejects missing required settings,
unknown payload modes or security values, and Kafka SASL without credentials. Kafka TLS keeps
certificate and hostname verification on.

### 5. This repository is public

No customer names, environments, private hosts or registries, topology, payloads, credentials or
incident details in code, tests, docs, commits or PRs. Examples stay provider-neutral
(`target.example`). The adapter is generic open source: describe what it does in its own terms,
never the products, deployments or internal documents built around it.

## The ways to hurt yourself

1. **Changing the Message Store from this side.** The SQL in `PlatformMessageStore.cs` and
   `PlatformStepExecutor.cs`, the `x-bip-*` headers, the status strings and
   `SupportedContractMajor` are contract surface. The adapter never creates or migrates the
   `bip_*` tables; deployments provision them. `tests/fixtures/postgres/` is the schema the adapter
   expects: change it only as part of a deliberate schema change in both adapters, never to pass a
   test. Do not widen the gate's grace: a missing or empty `bip_contract` only warns.
2. **Reordering the delivery path.** Recording before the DLQ publish, committing before
   recording, or holding a transaction or lock across a Kafka produce breaks the contract. Resend
   commits its `REQUESTED` audit row on its own connection before the produce.
3. **Leaking data.** Never put payloads, application headers, passwords or connection strings in
   logs or exception messages. Handler error text lands in the `error` columns and the
   `x-bip-error` DLQ header, where operators read it. Log message ids, never message content.
4. **Breaking pinned consumers.** Renaming a public type, method, options property or environment
   variable breaks integrations on upgrade. Add an overload or keep a compatibility path, as the
   `RecordAttemptAsync(message, routeId, ...)` overload and the default datasource registration do.
5. **Publishing.** NuGet.org never deletes a version, so both lanes publish permanently. A pushed
   `v*` tag runs `publish-nuget.yml`, which tests, packs with the version from the tag and pushes;
   tag only when asked. A manual run of `publish-prerelease.yml` publishes the selected branch as
   `X.Y.Z-preview.<date>.<run>`; run it only when asked. The csproj `<Version>` is the next
   intended release with a `-preview` label: the pre-release takes `X.Y.Z` from it and the tag
   sets the release version. `docs/releasing.md` is the procedure.

## How it works

`AddBasisMessageStoreAdapter<THandler>()` registers the options, a keyed Npgsql datasource
(`MessageStoreDataSourceKey`) for all adapter persistence, `PlatformMessageStore`,
`IPlatformStepExecutor`, a Kafka producer and the hosted `PlatformMessageConsumerService`. On start
the consumer validates options, runs the contract gate, then consumes the input topic with
auto-commit off. Each message goes to the application's `IPlatformMessageHandler` with bounded
in-process retry; the attempt number comes from the `x-bip-attempt` header. Success upserts
`bip_message_log` and `bip_message_attempt` in one transaction and commits; give-up parks the
message as above. `IPlatformStepExecutor.ExecuteAsync` wraps each external call: a step already
`DELIVERED` in `bip_step_outcome` is skipped unless the replay names it in `x-bip-redo-steps`.
`MapBasisMessageStoreAdminEndpoints()` maps `POST /admin/messages/{messageId}/resend`, which
republishes the stored payload to the input topic with the next attempt number.

## Where code lives

- `src/Basis.IntegrationPlatform.MessageStore/`: the package. `PlatformMessageStoreExtensions.cs`
  registration and the resend endpoint; `PlatformMessageConsumerService.cs` consume, retry, DLQ and
  commit; `PlatformMessageStore.cs` the contract gate, recording, payload policy and replay;
  `PlatformStepExecutor.cs` and `PlatformStepObservations.cs` the step ledger and `steps_json`;
  `PlatformMessageStoreOptions.cs` configuration; `PlatformMessage.cs` the public message types.
- `tests/Basis.IntegrationPlatform.MessageStore.Tests/`: MSTest. `tests/fixtures/postgres/`: the
  store schema the Postgres tests run against.
- `Directory.Build.props` (nullable on, warnings as errors), `Directory.Packages.props` (central
  package versions; add versions there, not in a csproj).
- `.github/workflows/`: `test.yml` (PRs and `main`), `publish-nuget.yml` (tags) and
  `publish-prerelease.yml` (manual pre-releases).

## Verifying

- .NET 8 SDK. There is no solution file. Smallest proof, for the tests you touched:
  `dotnet test tests/Basis.IntegrationPlatform.MessageStore.Tests --filter "ClassName~<TestClass>"`
- `PlatformMessageStorePostgresTests` reports inconclusive without
  `POSTGRES_TEST_CONNECTION_STRING`. When you touch SQL or persistence, run it against a
  disposable Postgres with `tests/fixtures/postgres/*.sql` applied in order, as `test.yml` does.
- The `Test` workflow runs the whole suite against real Postgres on every PR; do not run the whole
  suite locally unless asked. CI tests only this adapter: a contract-facing change also needs the
  matching change, and its tests, in the Camel adapter.
- Tests use synthetic data. Report what you ran and what you did not.

## Pull requests

- Small, from a branch off freshly fetched `origin/main` at
  `github.com/basis-no/integration-platform-adapter-dotnet`. Push and open PRs only when asked.
- Title: a plain imperative sentence, like the history ("Ignore local VS Code settings").
- Body: the problem in a sentence or two, the change, and what you verified. Say when the change
  affects compatibility, defaults, limitations or the store schema major, and whether the Camel
  adapter needs the same change. No AI or agent attribution in commits or PR text.
- Stage paths explicitly; never `git add -A`.

## Documentation

Most code changes need no documentation change. `README.md` is the user reference and ships inside
the NuGet package: a change to usage, environment variables or defaults updates it, and a change
users will notice gets a line in `CHANGELOG.md`.

- Keep the store schema major in the README in step with `SupportedContractMajor`.
- Rewrite text in place. Never append dated updates, status sections or evidence logs.

## Plans and work artifacts

- Do not commit implementation plans, research notes, acceptance evidence or scratch files.
- Track active work in the issue or PR that owns it.

## Taste

- Keep the public surface small: helpers are `internal` and tested through `InternalsVisibleTo`.
- Configuration comes from environment variables in `PlatformMessageStoreOptions`; `Validate()`
  fails with a message that names the variable to fix.
- SQL is inline and parameterized; never build it from strings.
- Comments sit next to the code and explain why in terms of what the adapter does. They do not
  cite documents outside this repository by section, decision or ADR number.
- Prove persistence against real Postgres rather than mocks.
