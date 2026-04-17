---
name: change-gate
description: Classify a bigname change before coding. Use whenever a task may affect API semantics, coverage or exhaustiveness, shared IDs or enums, manifests, migrations, workstream ownership, `crates/domain`, or any claim of consumer replacement/parity.
---

# Change Gate

Use this skill to decide whether a task is a frozen-interface change or an implementation-only change.

## Read only the docs you need

- API, query, coverage, or route semantics: `docs/api-v1.md`
- Surface/resource identity or binding behavior: `docs/adrs/0002-surface-resource-identity.md`
- Storage, canonicality, migrations, or write ownership: `docs/storage.md`
- Manifests, discovery, or capability flags: `docs/manifests.md`
- Projection families or collection semantics: `docs/projections.md`
- Verified execution or explain traces: `docs/execution.md`
- Consumer replacement or parity claims: `docs/consumer-capabilities.md`
- Delivery order or workstream ownership: `docs/development-plan.md`, `docs/workstreams.md`

## Classify the change

Produce a short gate with:

1. `change_class`: `semantic`, `shared-interface`, or `implementation-only`
2. `docs_to_update`: exact files that must change first or alongside code
3. `write_owner`: the owning workstream or directory
4. `parallel_risk`: whether the task can run in parallel safely

## Force doc-first treatment when any of these change

- route shape, defaults, pagination, error semantics, or coverage meaning
- shared IDs, enums, or identity anchors
- manifest schema, rollout semantics, or capability-flag meaning
- storage ownership boundaries or canonicality rules
- what counts as consumer replacement or parity

## Hard stops

Do not treat a task as implementation-only if it would cause any of:

- adapters writing projection rows
- API code reading raw facts directly
- execution code depending on adapter internals
- hidden unsupported behavior instead of explicit coverage or typed failure

## Parallel work

If the task is substantial, assign explicit owned paths before parallel work starts. Be conservative around:

- `crates/domain`
- migration files
- fixtures
- manifest schema

Keep the output concise. The goal is to unblock implementation without shared-interface drift.
