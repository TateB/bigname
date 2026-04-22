# Rollback Runbook

Use the rollback smoke gate when preparing or validating a rollback candidate.
The gate is local: it validates the checked-out rollback revision, the
configured PostgreSQL database, generated OpenAPI artifact consistency,
migration idempotence, the conformance ownership table for published OpenAPI
paths, and the API process readiness endpoint. It does not perform the
production rollback, deploy, contact external RPC providers, contact GitHub or
Fly, or validate a remote production target.

## Command

Run the standard local rollback smoke gate:

```sh
scripts/rollback-smoke
```

Run the CI-compatible no-network subset:

```sh
scripts/rollback-smoke --no-network
```

Show the supported arguments and environment inputs:

```sh
scripts/rollback-smoke --help
```

## Local Prerequisites

- `cargo`, `diff`, `seq`, and `curl` are available on `PATH`.
- A PostgreSQL URL is available through `BIGNAME_DATABASE_URL` or
  `DATABASE_URL`. If neither is set, the script defaults to
  `postgres://bigname:bigname@127.0.0.1:5432/bigname`.
- The API bind address is free. Set `BIGNAME_SMOKE_API_BIND_ADDR` when
  `127.0.0.1:3000` is already in use.
- `BIGNAME_SMOKE_API_HEALTH_URL` is reachable from the operator host. By
  default it is derived from `BIGNAME_SMOKE_API_BIND_ADDR` as
  `http://<bind_addr>/healthz`.
- For `--no-network`, Cargo dependencies must already be cached locally.

The script loads `.env` when it exists, then uses the environment values above.

## Gate Coverage

`scripts/rollback-smoke` performs these checks, in order:

1. Validates no-network constraints when `--no-network` is passed.
2. Runs `cargo run --locked -p bigname-worker -- migrate` against the configured
   database.
3. Runs the same migration command a second time to catch non-idempotent
   migration behavior in the rollback checkout.
4. Runs `cargo run --locked -p bigname-api -- print-openapi` and compares the
   result to `docs/api-v1.openapi.json`.
5. Runs `cargo test --locked --manifest-path tests/conformance/Cargo.toml
   openapi` as the OpenAPI conformance-owner smoke guard. This guard reads only
   the checked-in `docs/api-v1.openapi.json` artifact and the conformance owner
   table in the conformance harness; it is no-network and no-Postgres.
6. Starts `cargo run --locked -p bigname-api -- serve --bind-addr
   <BIGNAME_SMOKE_API_BIND_ADDR>` and probes `/healthz` until it returns
   `200` with `"status":"ready"`.

With `--no-network`, the script also sets `CARGO_NET_OFFLINE=true`, passes
`--offline` to Cargo, and rejects non-loopback smoke bind or health URLs.

## Pass Criteria

Treat the rollback smoke gate as passing only when the script exits `0` and logs
`rollback smoke gate passed`.

A passing gate means:

- the rollback checkout's checked-in migrations can be run twice against the
  configured local database without failing;
- the checked-in OpenAPI JSON matches the rollback checkout's API generator
  output;
- every published OpenAPI public path has an explicit conformance harness owner
  or an explicit private/out-of-scope reason in the conformance owner table;
- the API process can start from the rollback checkout; and
- the private readiness endpoint reports ready against that database.

## Failure Criteria

Any non-zero exit blocks automatic rollback promotion until triaged.

- First migration failure: the rollback checkout cannot apply its checked-in
  migrations to the configured database. Stop and inspect the database state and
  migration expectations before continuing.
- Second migration failure: the rollback checkout's migration command is not
  idempotent for the current database state. Do not proceed with an automatic
  rollback until the idempotence problem is understood.
- OpenAPI drift failure: the generated artifact and checked-in artifact disagree
  in the rollback checkout. Do not promote that checkout until the artifact and
  contract state are reconciled.
- OpenAPI conformance-owner failure: a published public path in
  `docs/api-v1.openapi.json` lacks a conformance harness owner, an owner entry is
  blank, or an out-of-scope entry lacks an explicit reason. Do not promote the
  rollback checkout until the route has an owning conformance harness or a
  deliberate private/out-of-scope reason.
- Readiness failure: the rollback API did not stay up or `/healthz` did not
  report ready. Do not treat the rollback as service-restoring until the API
  logs and database reachability explain the failure.
- No-network failure: the gate was not fully local, the bind or health URL was
  not loopback, or Cargo could not build from its local cache. Fix the operator
  environment before treating it as a rollback-candidate failure.

## Rollback Decision Points

Start rollback execution when the current release is already promoted, the
current release is unhealthy or unsafe, and a rollback candidate is expected to
restore service faster than a forward fix.

Run `scripts/rollback-smoke` against the rollback checkout before promotion when
there is time to validate locally. For urgent incidents, run it in parallel with
operational rollback preparation and use any failure as a reason to pause
automatic promotion and escalate to the owning engineer.

After the operational rollback, rerun the gate against the revision and database
state that represent the rolled-back service when local access is available. A
passing local gate is not a substitute for production health checks; it confirms
only the local migration, artifact, and readiness behaviors covered above.

Do not use this gate as proof of external integration health. It intentionally
does not exercise deploy commands, external RPC, GitHub, Fly, or remote
production endpoints.

## CI Behavior

CI runs this gate as `rollback smoke gate (no network)` with:

```sh
./scripts/rollback-smoke --no-network
```

The CI no-network subset preserves the existing OpenAPI drift and migration
checks while adding the double migration idempotence check, the no-Postgres
OpenAPI conformance-owner guard, and local API readiness. It uses loopback-only
smoke URLs and offline Cargo execution. A CI failure has the same
rollback-blocking meaning as a local non-zero exit, except that missing cached
dependencies are a CI environment issue rather than a product regression.
