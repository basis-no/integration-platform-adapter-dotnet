# Changelog

## Unreleased

- A step whose operation returned is never recorded `FAILED`. When the
  `DELIVERED` write fails, the step executor retries it in place, and if it
  keeps failing, throws the store failure without writing `FAILED`. Before,
  a store error after a successful call recorded the step `FAILED`, with the
  store's error text as the step's error, so a later attempt or replay sent
  the call again.

- A handler that catches a step ledger failure and returns success no longer
  gets the message recorded `COMPLETED`: the adapter notes the store failure
  next to the step observations and processes the delivery again, without
  parking it, also when the ledger call was cancelled by the handler's own
  token. Before, a step that never ran could be recorded as completed.

- `IPlatformStepExecutor.ExecuteWithContextAsync` passes the operation a
  `PlatformStepExecutionContext` with the step's idempotency key and step id
  before the call. Send the key as `Idempotency-Key` and
  `x-bip-idempotency-key`, as the Camel adapter does, so the receiver can
  deduplicate a redelivery, a replay or a redo of a call it already applied.
  `ExecuteAsync`, and implementations of `IPlatformStepExecutor` written
  against it, keep working unchanged.

- `BASIS_MESSAGE_STORE_STEP_OUTCOME_STORE` selects the step outcome store, as
  in the Camel adapter: `postgres` (the default, as before) or `noop`. With
  `noop`, every step runs on every delivery and replay, nothing is read from
  or written to `bip_step_outcome`, and a resend with `redoSteps` is refused
  with `409 NOT_REPLAYABLE`. Earlier versions ignored the variable and always
  used `bip_step_outcome`; a deployment that sets it to `noop` now gets the
  `noop` behaviour, and an unknown value fails startup.

- A late, older attempt no longer overwrites the message summary. When attempt
  1 was consumed again after its replay, attempt 2, had been recorded,
  `bip_message_log` took attempt 1's status, error, payload and expiry while
  `last_attempt` still said 2, and a resend read the older payload. The attempt
  row is now written first and the summary only when the attempt is at least as
  new as the recorded one, as the Camel adapter does; the older attempt still
  updates its own `bip_message_attempt` row, and a redelivery of the latest
  attempt still updates the summary. All 0.3.x releases are affected.

- The DLQ record no longer repeats credentials from the consumed message in
  clear text. It carries every consumed header, so an `Authorization`, cookie,
  client secret or API/subscription key a producer put on the input topic
  reached the shared DLQ topic. Their values, and those of headers ending in
  `-key` or `_key` such as `x-functions-key`, are now masked as `xxxxxx`;
  header names, `x-bip-*` and `Idempotency-Key` are kept. The rule matches the
  Camel adapter. Version 0.3.1 forwards them unmasked.

- Log when the step executor skips a step for a reason the integration
  probably did not intend. When the same handler invocation already delivered
  that step id (several items sent through one step id deliver only the first),
  each skipped call logs an ERROR, as the Camel adapter does. A repeat of a step
  id an earlier attempt delivered, and a message id journaled by another
  integration, log a warning. Step keys are
  `<message-id>:<step-id>` and `bip_step_outcome` has no integration column, so
  a message id shared with another integration that uses an equal step id skips
  the delivery. The skip is unchanged; the README describes how to choose step
  ids. All 0.3.x releases are affected.

- A handler timeout no longer stops the consumer. Cancellation counted as
  shutdown whatever its cause, so the `TaskCanceledException` of an
  `HttpClient` timeout escaped the consume loop, and by default the host
  stopped. Cancellation is now shutdown only when the host is stopping; any
  other is retried and then parked in the DLQ like a handler failure, and a
  timed-out step is recorded as `FAILED`. Recoverable Kafka consume errors,
  such as a subscribed topic that does not exist yet, are logged and retried
  instead of ending the consumer; a fatal one still ends it. All 0.3.x
  releases are affected.

- Step endpoint URIs are recorded without credentials. `bip_step_outcome` and
  `steps_json` kept the URI given to the step executor as is. They are now
  masked first by the Camel adapter's rule, so both adapters record the same
  value for every credential that adapter masks, including the user name,
  login and client id parameters it treats as sensitive. This adapter also
  masks the whole user info and the values of further credential-like query
  parameters, such as `code` and `sig`, as `xxxxxx`; only the recorded copy is
  masked. Rows written earlier are unchanged. All 0.3.x releases are affected.

- Resend refuses a message journaled by another integration. On a shared
  Message Store, `POST /admin/messages/{messageId}/resend` replayed any stored
  message, to the input topic of the integration that journaled it. It now answers
  `409 NOT_REPLAYABLE` ("Message is not replayable by this integration.")
  before any audit row or produce, as the Camel adapter does. All 0.3.x
  releases are affected.

- Replay renews inline payload expiry only up to first receipt plus twice the
  configured retention, matching the Camel adapter. Both the summary and new
  attempt use the cap; earlier attempts keep their expiry. First receipt is
  the earliest attempt recorded by this integration. Re-consuming that earliest
  attempt rewrites its receipt time and can move the cap; replay cannot.

## 0.3.0

First public release, on NuGet.org, under the package id
`Basis.IntegrationPlatform.MessageStore`. Expects store schema major 1.
Earlier versions (up to 0.2.2) were internal to Basis under the id
`no.basis.integrationplatform.integration-platform-adapter-dotnet`.
