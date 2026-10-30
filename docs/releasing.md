# Releasing

The adapter has two publication lanes, both to NuGet.org through this
repository's workflows with NuGet Trusted Publishing. Pre-releases let an
unreleased change be tested in a real integration before its final release;
users depend only on final releases.

| Lane | Workflow | Trigger | Version |
| --- | --- | --- | --- |
| Pre-release | `publish-prerelease.yml` | manual run | `X.Y.Z-preview.<UTC date>.<run number>` |
| Final release | `publish-nuget.yml` | pushed `vX.Y.Z` tag | `X.Y.Z` |

NuGet.org keeps every version it accepts. Deleting a version only unlists it,
and an unlisted version still restores by its exact version, so every run of
either lane is permanent.

## Pre-release for acceptance

1. One time, the package owner adds a Trusted Publishing policy on NuGet.org
   for this repository with the workflow file `publish-prerelease.yml`; the
   release policy covers only `publish-nuget.yml`. A new policy can start as
   temporarily active for seven days and becomes permanent with its first
   publish.
2. Keep the csproj `<Version>` on the next intended release with a `-preview`
   label, for example `0.4.0-preview.1`. The workflow takes `X.Y.Z` from it and
   refuses a version without a label or one that NuGet.org has already
   released.
3. Run the **Publish NuGet pre-release** workflow from the branch to test:

   ```sh
   gh workflow run publish-prerelease.yml --ref <branch>
   ```

   The branch must contain the workflow. It runs the test suite against
   Postgres with the vendored contract schema, packs the commit as, for example,
   `0.4.0-preview.20261017.3` and pushes it. The run summary shows the version
   and source commit. It creates no Git tag and no GitHub release.
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

`publish-nuget.yml` accepts only `vX.Y.Z` tags. It tests, packs with the
version from the tag and pushes. Afterwards move the csproj `<Version>` to the
next intended release, such as `0.4.1-preview.1`. A defect in a published
version is fixed in a new version; NuGet.org never replaces one.
