---
name: parallel-pickup
description: Take a substantial bigname task, split it into safe owned slices, and parallelize it with subagents. Use whenever the user asks to pick up a task, fan work out, parallelize implementation, or coordinate multiple workstreams.
metadata:
  kind: coordination
---

# Parallel Pickup

Broad-mode wrapper for `$orchestrate`. Use when execution is wanted (not just planning) and the task is large enough to benefit from parallel workers.

Apply `$orchestrate` with the fan-out extras below.

## Extras

1. Form a short top-level plan before delegating.
2. Keep the immediate blocking task local whenever the next step depends on it.
3. Run `$change-gate` first if the task may touch shared semantics, manifests, migrations, `crates/domain`, or parity claims.
4. Split work along `docs/workstreams.md` boundaries.
5. Log each dispatched slice via the orchestrate slice-log helper (`./.agents/skills/orchestrate/scripts/slice-log '{…}'`) so continuation loops see in-flight work.

See `AGENTS.md` High Conflict for surfaces that must not be parallelized casually, plus any unresolved shared-interface change.

## Good delegated tasks

Each bullet is one worker's remit — bundle related code, tests, and wiring into the same task rather than fanning out every layer.

- implement one adapter slice in `crates/adapters` together with its conformance tests
- add one projection or route end-to-end in `apps/api` or `apps/worker`, including manifest wiring and rollout entries
- wire manifest loading in `crates/manifests` with the discovery and capability-flag paths it unlocks
- add a coherent batch of conformance tests in `tests/conformance` for an existing capability

## Bad delegated tasks

- "figure out the architecture"
- "change whatever is needed"
- overlapping edits to shared type files or the same migration
- blocking semantic decisions that should stay local
