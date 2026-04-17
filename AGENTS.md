# bigname

`bigname` is in bootstrap. The checked-in docs are the source of truth for semantics.

## Guardrails

- Minimum shared-interface freeze: `docs/architecture.md`, `docs/api-v1.md`, `docs/storage.md`, `docs/manifests.md`, `docs/consumer-capabilities.md`, `docs/adrs/0001-stack.md`, and `docs/adrs/0002-surface-resource-identity.md`.
- If a task changes public semantics, shared IDs or enums, coverage meaning, manifest schema, workstream ownership, or consumer-replacement meaning, update the relevant docs first or in the same change.
- Prefer thin end-to-end slices. Do not build disguised legacy API parity or new planning docs unless semantics changed.

## Boundaries

- Adapters write identity rows and normalized events, not projection rows.
- API code reads projections and execution output only, except explicit audit endpoints.
- Execution code uses declared topology and manifests, not adapter internals.
- Manifest and discovery code decides what is authoritative.
- Raw facts are immutable. Projections are rebuildable. Canonicality is explicit. Execution artifacts are durable. Unsupported behavior must be explicit.

## High Conflict

- Keep `crates/domain` narrow.
- Coordinate migrations carefully.
- Treat fixture updates as cross-workstream review points.

## Local Skills

- `$change-gate`: classify doc-first vs implementation-only work.
- `$manifest-rollout`: review manifests, discovery, capability flags, and invalidation.
- `$capability-slice`: map consumer capabilities to routes, projections, tests, and rollout criteria.
- `$replay-boundaries`: review replay, canonicality, projection, and execution-boundary work.
- `$parallel-pickup`: take a broad task, split it into safe owned slices, and parallelize with subagents.
