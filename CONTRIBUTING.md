# Contributing

## How this project is developed

Most code and documentation here is written with AI coding agents, directed
and reviewed by Basis maintainers. Every change goes through a pull request,
human review and CI before it is merged. [`AGENTS.md`](AGENTS.md) contains the
rules the agents follow; human contributors follow the same rules.

## Branches

- `main` is the development line for the next minor version.
- `release/X.Y` branches carry maintenance releases of an older line.
- Work happens on short-lived branches named `<type>/<topic>`, where the type
  is `feat`, `fix`, `docs`, `test`, `ci` or `chore`. Names describe the change.
  They never contain customer, environment or tool names.
- Fix on `main` first, then backport to the release branch with
  `git cherry-pick -x`, unless the fix applies only to the release line.

## Pull requests

- One topic per pull request. Stack pull requests only when one really depends
  on another.
- Pull requests are squash-merged, and the pull-request title becomes the
  commit message. Write it as a short sentence in the imperative mood.
- Before asking for review, run `dotnet test`, update `CHANGELOG.md` for
  user-visible changes and update the documentation.
- This adapter and the Camel Quarkus adapter implement the same Message Store
  contract. Say in the pull request whether the other adapter needs the same
  change.
- Never put customer names, internal hostnames, credentials, personal data or
  test-run evidence in code, documentation, commits or pull requests.

## Security

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md),
not in a public issue.
