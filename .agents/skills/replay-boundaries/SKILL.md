---
name: replay-boundaries
description: Review bigname storage, replay, projection, and execution-boundary work. Use whenever a task touches canonicality, normalized events, projection rebuilds, reorg repair, invalidation, execution traces, or storage-family write ownership.
---

# Replay Boundaries

Start with:

- `docs/storage.md`
- `docs/projections.md`
- `docs/execution.md`
- `docs/workstreams.md`

## What to verify

For any design, implementation, or review touching replayable state, check:

1. storage layer affected
2. write owner
3. primary keys or identity anchors
4. canonicality behavior under reorg
5. rebuild path
6. invalidation triggers

## Non-negotiable rules

- Raw facts stay immutable.
- Canonicality is explicit; reorg repair marks rows noncanonical rather than deleting truth.
- Projection rows are disposable and rebuildable.
- Only projection workers write projection tables.
- Invalidation is deterministic and key-scoped, not broad polling.
- Execution artifacts and traces are durable facts.

## Boundary enforcement

Correct designs that drift into any of these:

- adapters mutating projections
- API reading raw-fact tables for normal reads
- execution code bypassing declared topology or manifest interfaces
- latest-row-wins logic instead of canonicality-aware replay

## Change guidance

- Append-only tables should prefer additive schema changes.
- Projection tables may be recreated only when the rebuild path already exists.
- If a change affects shared ownership or replay semantics, route it back through `$change-gate`.

Keep the review concrete. Name the affected tables, keys, invalidation sources, and rebuild modes.
