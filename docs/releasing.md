# Releasing

The adapter has two publication lanes, both to NuGet.org through this
repository's workflows with NuGet Trusted Publishing. Pre-releases let an
unreleased change be tested in a real integration before its final release;
users depend only on final releases.

| Lane | Workflow | Trigger | Approval | Version |
| --- | --- | --- | --- | --- |
| Pre-release | `publish-prerelease.yml` | manual run | environment `prerelease` | `X.Y.Z-preview.<UTC date>.<run number>` |
| Final release | `publish-nuget.yml` | pushed `vX.Y.Z` tag | environment `release` | `X.Y.Z` |

Both workflows keep project code away from the publishing identity. The job
that runs `dotnet test` and `dotnet pack` has a read-only token, no secrets and
no OIDC token. The job that pushes runs in a protected environment, never checks
out or runs repository code, and is the only one that can request the OIDC token
that NuGet/login exchanges for a 1-hour API key. The nuget.org Trusted
Publishing policy for each workflow is bound to its environment, so a run
without the approval gets no key.

NuGet.org keeps every version it accepts. Deleting a version only unlists it,
and an unlisted version still restores by its exact version, so every run of
either lane is permanent.

## Pre-release for acceptance

1. One time, the package owner adds a Trusted Publishing policy on NuGet.org
   for this repository with the workflow file `publish-prerelease.yml` and
   environment `prerelease`; the release policy covers only `publish-nuget.yml`
   with environment `release`. A new policy can start as temporarily active for
   seven days and becomes permanent with its first publish.
2. Keep the csproj `<Version>` on the next intended release with a `-preview`
   label, for example `0.4.0-preview.1`. The workflow takes `X.Y.Z` from it and
   refuses a version without a label or one that NuGet.org has already
   released.
3. Run the **Publish NuGet pre-release** workflow from the branch to test:

   ```sh
   gh workflow run publish-prerelease.yml --ref <branch>
   ```

   The branch must contain the workflow. Its `build` job runs the test suite
   against Postgres with the vendored contract schema and packs the commit as,
   for example, `0.4.0-preview.20261017.3`. A reviewer of environment
   `prerelease` other than the person who started the run then approves the
   `push` job, which pushes exactly that package. The run summary shows the
   version and source commit. It creates no Git tag and no GitHub release.
4. Pin that exact version in the validation integration, for example in
   `Directory.Packages.props`. NuGet.org validates and indexes a new version
   before restore can find it, usually within minutes. Pre-releases do not
   appear in default search or `dotnet add package`, so nobody receives one
   without pinning it.
5. Build and deploy the integration, then record the adapter commit, the
   pre-release version and the image digest in the acceptance evidence. The
   version names one immutable package, so a rebuild restores the same bytes.

A pre-release is an uncertified test build. The platform builds and runs it in
development and test environments with a warning and refuses it for
production. After the final release, superseded pre-releases may be unlisted;
exact pins keep restoring.

## Final release

Release the accepted commit, after a `CHANGELOG.md` section for the version and
with the README's contract version in step with the code:

```sh
git tag v0.4.0
git push origin v0.4.0
```

Only release managers can create `v*` tags (tag ruleset), and the tag must be
on `main`. `publish-nuget.yml` starts only for `vX.Y.Z` tags and runs three
jobs:

1. `build` checks the tag, runs the test suite against Postgres and packs with
   the version from the tag. It has a read-only token and no OIDC token.
2. `attest` runs no repository code. It records the package's SHA-256 and
   creates GitHub build provenance for it, which is public on a public
   repository.
3. `push` waits for approval in environment `release` by a required reviewer
   other than the person who pushed the tag. It checks that the environment is
   still protected and that the tag still points at the built commit, verifies
   the package against the recorded checksum and provenance, then logs in with
   Trusted Publishing and pushes that package. It never checks out the
   repository. A version that already exists on NuGet.org fails the run.

Review the `build` and `attest` results and the package checksum in the run
summary before approving. Check provenance yourself with
`gh attestation verify <nupkg> --repo basis-no/integration-platform-adapter-dotnet`.
Afterwards move the csproj `<Version>` to the next intended release, such as
`0.4.1-preview.1`. A defect in a published version is fixed in a new version;
NuGet.org never replaces one.

A failed `push` job may be re-run; it needs a new approval. If the version
already reached NuGet.org, the push fails on the duplicate and changes nothing.
