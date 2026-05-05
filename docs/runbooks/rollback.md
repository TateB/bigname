# Rollback runbook

The rollback smoke gate validates a rollback candidate before or alongside an operational rollback. It's the same shape as the [release gate](release.md) — local, no remote contact — with one extra step: the migration command runs twice to catch non-idempotent migrations under the rollback checkout.

The gate does **not** perform the production rollback, deploy, contact external RPC providers, contact GitHub or Fly, or validate a remote production target.

## Command

```sh
scripts/rollback-smoke              # standard local gate
scripts/rollback-smoke --no-network # CI-compatible subset
scripts/rollback-smoke --help       # arguments and env inputs
```

## Prerequisites

Same as the release gate. See [`release.md`](release.md) § Prerequisites.

## Gate steps

Same shape as the release gate, with double migration:

1. Validate `--no-network` constraints if passed.
2. `scripts/sync-refs --check` — local pinned upstream-ref verification.
3. `cargo test … reorg_chaos_drill_conformance_job` — focused reorg chaos guard.
4. `cargo run -p bigname-worker -- migrate` — first migration pass.
5. `cargo run -p bigname-worker -- migrate` — **second pass to catch non-idempotent migrations under the rollback checkout.**
6. `cargo run -p bigname-api -- print-openapi` and diff against `docs/api-v1.openapi.json`.
7. `cargo test … openapi` — OpenAPI conformance-owner guard.
8. `cargo test … capability_cutover_evidence` — capability cutover evidence guard.
9. `cargo test … dynamic_resolver_profile` — dynamic resolver-profile guard.
10. `cargo run -p bigname-worker -- manifest-drift audit --json` — live drift, persistence, render of the durable observation set.
11. `cargo run -p bigname-worker -- inspect watch-plan --json` — read-only inspection.
12. `cargo build -p bigname-api --bin bigname-api` — prebuild outside the probe window.
13. Start the compiled binary and probe `/healthz` until ready.

`--no-network` semantics match the release gate.

Manifest-drift behaviour matches [`release.md`](release.md) § Manifest-drift behaviour.

## Pass criteria

A passing gate exits `0` and logs `rollback smoke gate passed`. That means everything in the release pass criteria, plus:

- the rollback checkout's checked-in migrations can be run **twice** against the configured local database without failing (idempotent under the rollback checkout's expected DB state)

## Failure handling

| Failure | Action |
|---|---|
| First migration | Rollback checkout can't apply checked-in migrations. Inspect DB state and migration expectations. Manifest-drift checks can't proceed until fixed. |
| Second migration | Migration command isn't idempotent for the current DB state. Don't proceed with automatic rollback until the idempotence problem is understood. |
| Anything else | Same as the release gate (see [`release.md`](release.md) § Failure handling). |

## Decision points

Start rollback execution when the current release is already promoted, the current release is unhealthy or unsafe, and a rollback candidate is expected to restore service faster than a forward fix.

Run `scripts/rollback-smoke` against the rollback checkout before promotion when there's time to validate locally. For urgent incidents, run it in parallel with operational rollback preparation; any failure is a reason to pause automatic promotion and escalate to the owning engineer.

After the operational rollback, rerun the gate against the revision and database state that represent the rolled-back service when local access is available. A passing local gate is not a substitute for production health checks — it confirms only the local migration, artifact, pinned-ref, conformance, manifest-drift, watch-plan, API prebuild, and readiness behaviours covered above.

## CI behaviour

```sh
./scripts/rollback-smoke --no-network
```

Same shape as the release gate's `--no-network` subset, with the double-migration idempotence check added. CI failure has the same rollback-blocking meaning as a local non-zero exit, except missing cached dependencies are a CI environment issue rather than a product regression.
