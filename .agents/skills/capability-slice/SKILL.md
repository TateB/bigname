---
name: capability-slice
description: Define or implement a thin consumer-replacement slice for bigname. Use whenever a task maps a first-party consumer capability to routes, projections, execution behavior, tests, or rollout criteria, or when someone claims parity or replacement.
---

# Capability Slice

Start with `docs/consumer-capabilities.md`. Then read only the contract docs needed for the capability:

- `docs/api-v1.md` for routes and query semantics
- `docs/projections.md` for declared-state reads
- `docs/execution.md` for verified behavior
- `docs/development-plan.md` for milestone fit

## Scope one capability at a time

Prefer one capability group per slice. If a request spans multiple groups, split it into smaller deliverables unless the docs already freeze them together.

## Produce this mapping

For the requested capability, map:

1. consumer capability
2. native `v1` route or routes
3. declared vs verified responsibility
4. required projections or execution outputs
5. coverage and exhaustiveness expectations
6. contract tests
7. rollout and rollback criteria

## Guardrails

- Measure replacement by consumer capability coverage, not legacy schema parity.
- Prefer thin end-to-end slices over broad scaffolding.
- Do not claim replacement or parity until the capability has:
  - concrete app call-site mapping
  - contract tests
  - rollout and rollback criteria

## Output style

Be specific about what is included in the slice and what is intentionally deferred. If coverage is partial, say so explicitly.
