# Changelog

## Unreleased

- The DLQ record no longer repeats credentials from the consumed message in
  clear text. It carries every consumed header, so an `Authorization`, cookie,
  client secret or API/subscription key a producer put on the input topic
  reached the shared DLQ topic. Their values are now masked as `xxxxxx`;
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

- A replay records a new inline payload expiry, so each replay extends payload
  retention from the time it was processed. This is current behaviour, now
  pinned by a test.

## 0.3.0

First public release, on NuGet.org, under the package id
`Basis.IntegrationPlatform.MessageStore`. Implements message-store contract
v1.0.0-rc2. Earlier versions (up to 0.2.2) were internal to the Basis
platform under the id
`no.basis.integrationplatform.integration-platform-adapter-dotnet`.
