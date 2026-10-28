# Changelog

## Unreleased

- The DLQ record no longer repeats credentials from the consumed message in
  clear text. It carries every consumed header, so an `Authorization`, cookie,
  client secret or API/subscription key a producer put on the input topic
  reached the shared DLQ topic. Their values are now masked as `xxxxxx`;
  header names, `x-bip-*` and `Idempotency-Key` are kept. The rule matches the
  Camel adapter. Version 0.3.1 forwards them unmasked.

## 0.3.0

First public release, on NuGet.org, under the package id
`Basis.IntegrationPlatform.MessageStore`. Implements message-store contract
v1.0.0-rc2. Earlier versions (up to 0.2.2) were internal to the Basis
platform under the id
`no.basis.integrationplatform.integration-platform-adapter-dotnet`.
