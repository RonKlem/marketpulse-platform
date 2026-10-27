# Security Policy

## Scope

MarketPulse is a portfolio and demonstration platform. It is **not** intended
for production use, and it processes only public market and economic data —
there are no credentials, no PII, and no trading execution path.

Reports are still welcome. A vulnerability here is a defect in engineering
practice even where the blast radius is a demo.

## Reporting

Open a [private security advisory](https://github.com/ronklem/marketpulse-platform/security/advisories/new).
Do not open a public issue for an unfixed vulnerability.

Please include the version or commit, reproduction steps, and the impact as you
see it. Expect an acknowledgement within a week.

## What is already enforced

These are checked in CI rather than left to review:

- **SQL injection.** Values are bound as prepared-statement parameters.
  Identifiers cannot be parameterised — DuckDB interpolates them into a view
  definition — so they are restricted to `[A-Za-z_][A-Za-z0-9_]*`, ASCII, under
  128 characters. Restricting the charset removes the injection path rather than
  relying on quoting being correct at every call site.
- **Path traversal.** Dataset names reach the filesystem from config files and
  CLI arguments, so `..` and path separators are rejected centrally in
  `StorageConfig.dataset_path` rather than at each caller.
- **Static analysis.** Ruff's `flake8-bandit` (`S`) rules run on every pull
  request.
- **Secret commits.** `detect-private-key` runs in pre-commit, and `.env`,
  `*.pem`, and `*.key` are git-ignored.
- **Dependency updates.** Dependabot runs weekly. Engine major versions are
  held for deliberate review, since they can invalidate the measurements in
  ADR-0002.

## Known limitations, stated deliberately

- **`marketpulse query` executes arbitrary SQL.** That is its purpose. It is a
  local developer tool against a local DuckDB instance, and it should not be
  exposed to untrusted input or wrapped in a network service as written.
- **No authentication or authorisation.** Access control arrives with Unity
  Catalog in Phase 3; the local layer has none.
- **No encryption at rest.** The local lakehouse is plain Parquet.

## Secrets

Secrets belong in environment variables, never in a config file. Every
`config/*.toml` layer is committed except `config/override.toml`, and even that
is the wrong place for a credential — it is git-ignored, not encrypted.

From Phase 3 onward, secrets come from Databricks secret scopes or AWS Secrets
Manager; that decision is still open and recorded in
[`architecture.md`](architecture.md#13-open-questions).
