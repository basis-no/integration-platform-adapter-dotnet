# Changelog

## Unreleased

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

- Warn when the step executor skips a step for a reason the integration
  probably did not intend: the same handler invocation already ran that step id
  (several items sent through one step id deliver only the first), or the
  message id is journaled by another integration. Step keys are
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
  `steps_json` kept the URI given to the step executor as is. Its user info
  and the values of credential-like query parameters, such as `code`, `sig`,
  `api_key` or `access_token`, are now masked as `xxxxxx`; only the recorded
  copy is masked. Rows written earlier are unchanged. The Camel adapter masks
  step endpoint URIs too. All 0.3.x releases are affected.

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
