# Changelog

## Unreleased

- The DLQ record no longer repeats credentials from the consumed message in
  clear text. It carries every consumed header, so an `Authorization`, cookie,
  client secret or API/subscription key a producer put on the input topic
  reached the shared DLQ topic. Their values, and those of headers ending in
  `-key` or `_key` such as `x-functions-key`, are now masked as `xxxxxx`;
  header names, `x-bip-*` and `Idempotency-Key` are kept. The rule matches the
  Camel adapter. Version 0.3.1 forwards them unmasked.

- Log when the step executor skips a step for a reason the integration
  probably did not intend. When the same handler invocation already ran that
  step id (several items sent through one step id deliver only the first), each
  skipped call logs an ERROR, as the Camel adapter does; when the message id is
  journaled by another integration, a warning. Step keys are
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

- A replay records a new inline payload expiry, so each replay extends payload
  retention from the time it was processed. This is current behaviour, now
  pinned by a test.

## 0.3.0

First public release, on NuGet.org, under the package id
`Basis.IntegrationPlatform.MessageStore`. Expects store schema major 1.
Earlier versions (up to 0.2.2) were internal to Basis under the id
`no.basis.integrationplatform.integration-platform-adapter-dotnet`.
