---
name: parallel-pickup
description: Take a substantial bigname task, split it into safe owned slices, and parallelize it with subagents. Use whenever the user asks to pick up a task, fan work out, parallelize implementation, or coordinate multiple workstreams.
---

# Parallel Pickup

Use this skill when the user wants execution, not just planning, and the task is large enough to benefit from parallel work.

Start with `docs/workstreams.md`. Read `docs/development-plan.md` if milestone order matters. If the task may touch shared semantics, manifests, migrations, `crates/domain`, or parity claims, run `$change-gate` first.

## Orchestration rules

1. Form a short top-level plan.
2. Keep the immediate blocking task local whenever the next step depends on it.
3. Delegate only concrete sidecar or parallel work with disjoint write ownership.
4. Split work along the repo boundaries in `docs/workstreams.md`:
   - `apps/api`
   - `apps/indexer`
   - `apps/worker`
   - `crates/domain`
   - `crates/storage`
   - `crates/manifests`
   - `crates/adapters`
   - `crates/execution`
   - `tests/conformance`
5. Tell each worker exactly which paths it owns.
6. Tell each worker it is not alone in the codebase and must not revert others' edits.
7. Ask each worker to report changed files and any residual risks.

## Do not parallelize these casually

- `crates/domain`
- migration files
- fixtures
- manifest schema
- any unresolved shared-interface change

## Good delegated tasks

- implement one adapter slice in `crates/adapters`
- add one projection or route in `apps/api` or `apps/worker`
- add conformance tests in `tests/conformance`
- wire manifest loading in `crates/manifests`

## Bad delegated tasks

- "figure out the architecture"
- "change whatever is needed"
- overlapping edits to the same migration or shared type files
- blocking semantic decisions that should stay local

Keep the orchestration concise. The goal is to turn one broad request into a few safe owned slices and then integrate them.
