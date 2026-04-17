# bigname

Status: Phase 1 bootstrap

`bigname` is a replayable, auditable indexing and read platform for ENSv1, ENSv2, and Basenames.

The docs remain the source of truth for product semantics, storage boundaries, API shape, and rollout criteria. The checked-in Rust workspace now provides the bootstrap needed to start implementation against those frozen interfaces.

## Read Order

1. `docs/architecture.md`
2. `docs/development-plan.md`
3. `docs/chain-intake.md`
4. `docs/api-v1.md`
5. `docs/storage.md`
6. `docs/manifests.md`
7. `docs/projections.md`
8. `docs/execution.md`
9. `docs/consumer-capabilities.md`
10. `docs/workstreams.md`
11. `docs/adrs/`

## Workspace

- `apps/api`: native `v1` read API bootstrap with an internal `/healthz` route
- `apps/indexer`: intake and adapter process bootstrap
- `apps/worker`: replay, backfill, execution, and migration entrypoint bootstrap
- `crates/domain`: narrow shared domain bootstrap surface
- `crates/storage`: PostgreSQL connection and migration support
- `crates/manifests`: manifest and discovery bootstrap surface
- `crates/adapters`: adapter bootstrap surface
- `crates/execution`: verified execution bootstrap surface
- `tests/conformance`: reserved for the TypeScript conformance harness

## Quickstart

1. Copy `.env.example` to `.env` if you want to customize local ports or credentials.
2. Start local dependencies with `docker compose up -d`.
3. Apply the checked-in migration with `./scripts/migrate`.
4. Boot all three processes with `./scripts/dev-up`.

Useful cargo aliases:

- `cargo api -- serve`
- `cargo indexer -- run`
- `cargo worker -- run`
- `cargo worker -- migrate`

## Guardrails

- treat the Phase 0 docs as the interface freeze
- update docs first when changing public semantics, shared IDs, storage invariants, or manifest meaning
- use `docs/workstreams.md` to split work without shared-interface drift
- keep adapter output limited to identity rows and normalized events
- keep the API read-only over projections and execution output
