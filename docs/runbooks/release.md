# Release Runbook

Use the release smoke gate before promoting a release candidate and as the CI
release safety check. The gate is local: it validates the checked-out revision,
the configured PostgreSQL database, generated OpenAPI artifact consistency, and
the conformance ownership table for published OpenAPI paths, runs the live
manifest-drift audit, and checks the API process readiness endpoint. It does not
deploy, contact external RPC providers, contact GitHub or Fly, or validate a
remote production target.

## Command

Run the standard local release smoke gate:

```sh
scripts/release-smoke
```

Run the CI-compatible no-network subset:

```sh
scripts/release-smoke --no-network
```

Show the supported arguments and environment inputs:

```sh
scripts/release-smoke --help
```

## Local Prerequisites

- `cargo`, `diff`, `seq`, and `curl` are available on `PATH`.
- A PostgreSQL URL is available through `BIGNAME_DATABASE_URL` or
  `DATABASE_URL`. If neither is set, the script defaults to
  `postgres://bigname:bigname@127.0.0.1:5432/bigname`.
  Point this at the local PostgreSQL database used for smoke checks; migrations,
  the manifest-drift audit, and readiness all require that database even when
  `--no-network` is passed.
- The API bind address is free. Set `BIGNAME_SMOKE_API_BIND_ADDR` when
  `127.0.0.1:3000` is already in use.
- `BIGNAME_SMOKE_API_HEALTH_URL` is reachable from the operator host. By
  default it is derived from `BIGNAME_SMOKE_API_BIND_ADDR` as
  `http://<bind_addr>/healthz`.
- For `--no-network`, Cargo dependencies must already be cached locally.

The script loads `.env` when it exists, then uses the environment values above.

## Gate Coverage

`scripts/release-smoke` performs these checks, in order:

1. Validates no-network constraints when `--no-network` is passed.
2. Runs `cargo run --locked -p bigname-api -- print-openapi` and compares the
   result to `docs/api-v1.openapi.json`.
3. Runs `cargo test --locked --manifest-path tests/conformance/Cargo.toml
   openapi` as the OpenAPI conformance-owner smoke guard. This guard reads only
   the checked-in `docs/api-v1.openapi.json` artifact and the conformance owner
   table in the conformance harness; it is no-network and no-Postgres.
4. Runs `cargo test --locked --manifest-path tests/conformance/Cargo.toml
   capability_cutover_evidence` as the focused capability cutover evidence
   guard.
5. Runs `cargo run --locked -p bigname-worker -- migrate` against the configured
   database.
6. Runs `cargo run --locked -p bigname-worker -- manifest-drift audit --json`
   against the configured database.
7. Starts `cargo run --locked -p bigname-api -- serve --bind-addr
   <BIGNAME_SMOKE_API_BIND_ADDR>` and probes `/healthz` until it returns
   `200` with `"status":"ready"`.

With `--no-network`, the script also sets `CARGO_NET_OFFLINE=true`, passes
`--offline` to Cargo, and rejects non-loopback smoke bind or health URLs. The
gate does not contact external network services or external RPC providers, but
the configured local PostgreSQL database must still be available.

## Pass Criteria

Treat the release smoke gate as passing only when the script exits `0` and logs
`release smoke gate passed`.

A passing gate means:

- the checked-in OpenAPI JSON matches the API generator output for this
  revision;
- every published OpenAPI public path has an explicit conformance harness owner
  or an explicit private/out-of-scope reason in the conformance owner table;
- the focused capability cutover evidence guard passes for this revision;
- the checked-in migrations apply to the configured local database;
- the manifest-drift audit command exits successfully against the configured
  local database;
- the API process can start from this revision; and
- the private readiness endpoint reports ready against that database.

## Failure Criteria

Any non-zero exit blocks the release candidate until triaged.

- OpenAPI drift failure: the generated artifact and checked-in artifact disagree.
  Do not promote the candidate until the API contract and checked-in artifact are
  intentionally reconciled.
- OpenAPI conformance-owner failure: a published public path in
  `docs/api-v1.openapi.json` lacks a conformance harness owner, an owner entry is
  blank, or an out-of-scope entry lacks an explicit reason. Do not promote until
  the route has an owning conformance harness or a deliberate private/out-of-scope
  reason.
- Migration failure: the configured database cannot apply the checked-in
  migrations. Do not promote until the migration or database precondition is
  fixed.
- Manifest-drift audit failure: the audit command returned non-zero against the
  configured local database. Do not promote until the local manifest/discovery
  state, audit inputs, or database precondition is triaged. This is not
  production monitoring or external RPC coverage.
- Readiness failure: the API did not stay up or `/healthz` did not report
  ready. Do not promote until the API logs and database reachability explain the
  failure.
- No-network failure: the gate was not fully local, the bind or health URL was
  not loopback, or Cargo could not build from its local cache. Fix the operator
  environment before treating it as a release failure.

## Rollback Decision Points

Before promotion, a release smoke failure is a stop-the-line release block, not
a rollback trigger. Fix the release candidate or the local prerequisites, then
rerun the gate.

After promotion, start the rollback runbook when the promoted revision is
already serving traffic and the failure is service-impacting or cannot be
resolved quickly by correcting operator configuration. Use
`docs/runbooks/rollback.md` to validate the rollback candidate locally before
or alongside the operational rollback procedure.

Do not use this gate as proof of external integration health. It intentionally
does not exercise deploy commands, external RPC, GitHub, Fly, or remote
production endpoints.

## CI Behavior

CI runs this gate as `release smoke gate (no network)` with:

```sh
./scripts/release-smoke --no-network
```

The CI no-network subset preserves the existing OpenAPI drift and migration
checks while adding the no-Postgres OpenAPI conformance-owner guard, focused
capability cutover evidence guard, live manifest-drift audit, and local API
readiness. It uses loopback-only smoke URLs, offline Cargo execution, and the
configured local PostgreSQL database for migrations, manifest-drift audit, and
readiness. A CI failure has the same release-blocking meaning as a local
non-zero exit, except that missing cached dependencies are a CI environment issue
rather than a product regression.
