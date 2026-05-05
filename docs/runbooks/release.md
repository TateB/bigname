# Release runbook

The release smoke gate is a local check before promoting a release candidate, and the same script runs in CI as the no-network release safety check. It validates the checked-out revision, the configured Postgres, local pinned upstream refs, generated OpenAPI artifact consistency, the conformance ownership table for published OpenAPI paths, focused conformance guards, the live manifest-drift audit, the runtime watch plan, and the API readiness endpoint from a prebuilt local binary.

The gate does **not** deploy, contact external RPC providers, contact GitHub or Fly, or validate a remote production target.

## Command

```sh
scripts/release-smoke              # standard local gate
scripts/release-smoke --no-network # CI-compatible subset
scripts/release-smoke --help       # arguments and env inputs
```

## Prerequisites

- `cargo`, `diff`, `seq`, and `curl` on `PATH`.
- A Postgres URL via `BIGNAME_DATABASE_URL` or `DATABASE_URL`. Default: `postgres://bigname:bigname@127.0.0.1:5432/bigname`. The reorg-chaos and dynamic-resolver-profile guards create, migrate, and drop temporary databases here. Migrations, the manifest-drift audit, watch-plan inspection, and readiness all run against this database — even with `--no-network`.
- The migration that creates `manifest_alert_observations` has run before manifest-drift smoke checks. The smoke gate runs migrations before the audit; for hand runs of `manifest-drift audit --json` or `inspect manifest-drift --json`, run the worker migration first.
- The API bind address is free. Set `BIGNAME_SMOKE_API_BIND_ADDR` if `127.0.0.1:3000` is in use.
- `BIGNAME_SMOKE_API_HEALTH_URL` is reachable. Default: `http://<bind_addr>/healthz`.
- The readiness check builds `bigname-api` before the probe window, then runs the compiled binary directly. Slow local compilation completes (or fails) before readiness polling begins; the 30 one-second probes measure server startup and health only.
- For `--no-network`, Cargo dependencies must already be cached locally.

The script loads `.env` when present.

## Gate steps

In order:

1. Validate `--no-network` constraints if passed.
2. `scripts/sync-refs --check` — local pinned upstream-ref verification (reads `.refs/`; doesn't fetch or rotate).
3. `cargo test … reorg_chaos_drill_conformance_job` — focused reorg chaos guard. Uses the local Postgres for temporary databases.
4. `cargo run -p bigname-api -- print-openapi` and diff against `docs/api-v1.openapi.json`.
5. `cargo test … openapi` — OpenAPI conformance-owner guard. Reads only the checked-in JSON and the conformance owner table.
6. `cargo test … capability_cutover_evidence` — capability cutover evidence guard.
7. `cargo test … dynamic_resolver_profile` — dynamic resolver-profile guard. Local Postgres temporary databases.
8. `cargo run -p bigname-worker -- migrate` against the configured database, including the `manifest_alert_observations` migration.
9. `cargo run -p bigname-worker -- manifest-drift audit --json` — live drift, persistence, render of the durable observation set.
10. `cargo run -p bigname-worker -- inspect watch-plan --json` — read-only watch-plan inspection.
11. `cargo build -p bigname-api --bin bigname-api` — keep API compile time outside the readiness probe window.
12. Start the compiled binary and probe `/healthz` until `200` with `"status":"ready"`.

`--no-network` also sets `CARGO_NET_OFFLINE=true`, passes `--offline` to Cargo, and rejects non-loopback smoke bind or health URLs. The local pinned-ref check and the Cargo-backed conformance guards run from the local dependency cache. The configured Postgres still has to be available.

### Manifest-drift behaviour

- `manifest-drift audit --json` persists live alert candidates into `manifest_alert_observations`, then renders the durable observation set. The JSON reports persisted counts and `actionable_persisted_alert_count`; live candidate counts are diagnostic.
- `--fail-on-alert` (used outside the smoke script) fails on actionable persisted alerts. It's not a gate on transient live candidates that weren't persisted.
- `inspect manifest-drift --json` is read-only over the same storage.
- Neither command fixes drift or mutates manifest truth, discovery edges, source-family admission, watch plans, or normalised events. Remediation is explicit manifest, discovery, or source-family work.

## Pass criteria

A passing gate exits `0` and logs `release smoke gate passed`. That means:

- the checked-in OpenAPI JSON matches the API generator output for this revision
- local `.refs/` checkouts match the pinned upstream-ref manifest
- the focused reorg chaos guard passes using local Postgres temporary databases
- every published OpenAPI path has an explicit conformance harness owner or a deliberate private/out-of-scope reason
- the capability cutover evidence guard passes
- the dynamic resolver-profile guard passes using local Postgres temporary databases
- migrations apply to the configured local database, including manifest alert observation storage
- the manifest-drift audit succeeds, persists worker-owned alerts, and renders the persisted set
- the watch-plan inspection succeeds and renders JSON
- the API binary builds locally
- the API process starts from that binary
- `/healthz` reports ready against the database

## Failure handling

Any non-zero exit blocks the release candidate.

| Failure | Action |
|---|---|
| OpenAPI drift | Don't promote until the API contract and checked-in artifact are intentionally reconciled. |
| Pinned upstream-ref mismatch | Don't promote until local pinned-ref state is restored. (This check is local only — it doesn't fetch.) |
| Reorg chaos / dynamic resolver-profile / capability-cutover guard | Triage the conformance failure or the local Postgres precondition. |
| OpenAPI conformance-owner | A published path lacks an owner or out-of-scope reason. Add one or document it. |
| Migration | Database can't apply checked-in migrations. Manifest-drift checks can't proceed until this is fixed. |
| Manifest-drift audit | Triage local manifest/discovery state, audit inputs, persistence path, migration state, or DB precondition. The audit doesn't auto-remediate. |
| Manifest-drift `--fail-on-alert` rerun | Persisted observations contain actionable alerts. Inspect, then fix manifest/discovery/source-family before rerun. |
| Watch-plan inspection | Triage DB reachability, manifest/discovery state, or the inspection failure. |
| API prebuild | Triage compile failure or missing offline cache. |
| Readiness | API didn't stay up or `/healthz` didn't report ready. Check API logs and DB reachability. |
| `--no-network` violation | Bind/health URL wasn't loopback or Cargo couldn't build from cache. Fix the operator environment. |

## Decision points

Before promotion, a smoke failure is a stop-the-line block — not a rollback trigger. Fix the candidate or local prerequisites, then rerun the gate.

After promotion, if the promoted revision is already serving traffic and the failure is service-impacting or can't be resolved by correcting operator config, switch to [`rollback.md`](rollback.md).

This gate is not proof of external integration health. It intentionally doesn't exercise deploy commands, external RPC, GitHub, Fly, or remote production endpoints.

## CI behaviour

CI runs the gate as `release smoke gate (no network)`:

```sh
./scripts/release-smoke --no-network
```

The CI subset preserves the existing OpenAPI-drift and migration checks while adding the local pinned-ref check, focused reorg chaos guard, no-Postgres OpenAPI conformance-owner guard, focused capability-cutover guard, focused dynamic resolver-profile guard, live manifest-drift audit with worker-owned alert persistence, runtime watch-plan inspection, and local API prebuild plus readiness. It uses loopback-only smoke URLs, offline Cargo execution, the checked-out `.refs/` state, and the configured local Postgres for the relevant temporary databases. A CI failure has the same release-blocking meaning as a local non-zero exit, except missing cached dependencies are a CI environment issue rather than a product regression.
