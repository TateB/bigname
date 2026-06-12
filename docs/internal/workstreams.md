# Ownership Boundaries

Internal reference for splitting implementation work. `AGENTS.md` is the process rulebook; this file only maps review ownership and high-conflict surfaces.

## Boundaries

- Adapters write identity rows and normalized events, not projection rows.
- Projection workers own projection tables and rebuild behavior.
- API code reads projections and execution output, except explicit audit endpoints.
- Execution uses declared topology and manifests, not adapter internals.
- Manifest and discovery code decides what is authoritative.

## Ownership Map

| Surface | Owner | Notes |
| --- | --- | --- |
| `apps/api`, `docs/api-v1.md`, `docs/api-v1-routes.md`, `docs/api-v1.openapi.json` | Projections and API | Public route shape, OpenAPI, response contracts, API tests |
| `apps/indexer`, `crates/adapters`, `docs/chain-intake.md` | Intake and Adapters | Raw intake, adapter normalization, provider/backfill behavior |
| `apps/worker`, projection modules, `docs/projections.md` | Projections and API | Projection apply/rebuild, current read models, worker-owned operational commands |
| `crates/storage`, `migrations`, `docs/storage.md` | Storage and Domain | Schema, canonicality, migrations, storage helpers |
| `crates/domain` | Storage and Domain | Narrow normalization helpers only; persisted identity types live in `crates/storage/src/identity/types.rs` |
| `crates/manifests`, `manifests/**`, `docs/manifests.md` | Manifests and Discovery | Source authority, discovery, capability flags, watch-plan inputs |
| `crates/execution`, `docs/execution.md` | Verified Execution | Resolution/primary execution, traces, invalidation |
| `tests/conformance`, checked-in fixtures | Conformance and Fixtures | Capability evidence, replay/conformance suites, golden fixtures |
| `docs/consumer-capabilities.md` | Conformance and Fixtures | Replacement meaning, rollout/rollback evidence |
| `.refs/MANIFEST.toml`, `docs/upstream.md` | Upstream Evidence | Pin rotation, citations, known divergences |
| `.agents/**`, `.codex/agents/**`, `.codex/rules/**`, `.codex/config.toml`, `.codex/hooks/**`, `AGENTS.md` | Agent Process | Skills, subagent definitions, hooks, automation, repo-local process rules |
| `scripts/**`, `.github/**`, root `Cargo.toml`, `Cargo.lock` | Platform and DevEx | Tooling, CI, workspace-wide dependency changes |

## High-Conflict Rules

- Migrations, fixtures, manifest schemas, `crates/domain`, and process definitions are serialized review points.
- Shared public semantics, coverage meaning, source authority, replay behavior, or replacement meaning require docs in the same change.
- Parallel work should split by ownership boundary, with one integrator responsible for final consistency.
- Before staging, inspect dirty state and stage explicit paths only.
