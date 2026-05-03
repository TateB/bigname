# Parallel Workstreams

Internal reference. Companion to [`development-plan.md`](./development-plan.md). Describes how implementation work splits so multiple engineers can move concurrently without fighting over semantics or storage boundaries.

## 1. Freeze Before Forking Work

These docs are the minimum interface freeze:

- `architecture.md`
- `chain-intake.md`
- `api-v1.md`
- `storage.md`
- `manifests.md`
- `consumer-capabilities.md`
- `docs/adrs/0001-stack.md`
- `docs/adrs/0002-surface-resource-identity.md`

After these are accepted, parallel work can start.

## 2. Workstream Matrix

| Workstream | Owns | Can start after | Main outputs |
| --- | --- | --- | --- |
| Platform and DevEx | workspace bootstrap, CI, local dev, config loading | ADR 0001 | Rust workspace, compose/dev env, CI gates |
| Storage and Domain | migrations, IDs, storage traits, domain types | `storage.md`, `chain-intake.md`, ADR 0002 | schema, migration harness, shared types |
| Manifests and Discovery | manifest loader, discovery graph, capability registry | `manifests.md`, ADR 0001 | manifest crate, discovery persistence, admission logic |
| Intake and Adapters | chain intake, ENSv1/ENSv2/Basenames adapters, normalized events | `storage.md`, `chain-intake.md`, `manifests.md` | raw fact intake, per-chain provider availability handling, adapter routing, normalized events |
| Projections and API | current-state projections, read handlers, OpenAPI output | `api-v1.md`, `projections.md`, `storage.md` | read models, API routes, contract tests |
| Verified Execution | resolution execution, primary verification, trace persistence | `execution.md`, `manifests.md`, `storage.md` | execution crate, invalidation, explain traces |
| Conformance and Fixtures | fixtures, replay tests, consumer capability checks | `api-v1.md`, `execution.md`, `consumer-capabilities.md` | golden fixtures, contract tests, replay suites, capability mapping |

## 3. Hard Boundaries

- adapters write normalized events, not projection rows
- API code reads projections and execution output, not raw-fact tables
- execution code consumes declared topology and manifests through stable interfaces, not adapter internals
- manifest/discovery code decides what is authoritative; adapters consume that decision
- changes to shared IDs, enums, or coverage semantics require doc updates before code merges

## 4. Repository Ownership

Initial ownership should map to directories:

- `apps/api`: Projections and API
- `apps/indexer`: Intake and Adapters, including selected-profile provider setup and automatic bootstrap job creation
- `apps/worker`: Projections, replay, bounded backfill, finalized catch-up, capacity-guarded chunk execution, execution jobs
- `crates/domain`: Storage and Domain
- `crates/storage`: Storage and Domain
- `crates/manifests`: Manifests and Discovery
- `crates/adapters`: Intake and Adapters
- `crates/execution`: Verified Execution
- `crates/test-support`: Conformance and Fixtures (dev-only crate, if added)
- `tests/conformance`: Conformance and Fixtures

`crates/domain` is the highest-conflict area. Keep it narrow and change it deliberately.

## 5. Recommended Startup Sequence

Week 1:

1. bootstrap the Rust workspace, CI, config, and dev services
2. land the storage skeleton and migrations
3. land the manifest loader and discovery schema

In parallel once the storage/domain interfaces are merged:

1. build Ethereum and Base intake
2. build the first ENSv1 adapter slice
3. build the first projection worker and `GET /v1/names/{namespace}/{name}`
4. build execution-trace persistence and the first verified resolution flow

## 6. Merge Discipline

- prefer short-lived branches scoped to one workstream
- do not share write ownership of migration files without coordination
- treat fixture updates as cross-workstream review points
- if a workstream needs a new shared field or enum, update the relevant doc first
- bootstrap backfill caps and finalized catch-up progress are operational readiness signals only; they must not be used by any workstream as route-coverage or consumer-replacement evidence without the full admitted-history and conformance gates in `consumer-capabilities.md`
