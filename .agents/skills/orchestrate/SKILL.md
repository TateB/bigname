---
name: orchestrate
description: Run broad bigname execution work in orchestration mode. Use whenever the task is large, multi-slice, parallelizable, or should be delegated instead of implemented directly in the current session. This skill makes the current session orchestrate the work. Read docs, classify the change, create bounded tasks, spawn subagents, steer them, and integrate their results without doing most implementation locally.
metadata:
  kind: coordination
---

# Orchestrate

`$phased-continuation` and `$parallel-pickup` are thin mode wrappers around this skill. Their bodies defer here.

## Core role

- Delegate implementation — including docs, task breakdown, and analysis. Don't edit product code, tests, manifests, fixtures, migrations, or docs unless delegation is blocked.
- Keep your own work to scoping, task design, delegation, follow-up, conflict resolution, synthesis.

## Repo rules

See `AGENTS.md` for Guardrails, Boundaries, and High Conflict. Apply them on every delegation.

Split work along `docs/workstreams.md` boundaries when possible.

## Modes

- **default** — bounded task. Parallelize subtasks whenever they have approximately disjoint work zones; only serialize when the task genuinely has one unit of work.
- **broad** (triggered by `$parallel-pickup` or explicit fan-out requests) — split a large task into owned slices and run workers concurrently. With the parallel-first defaults below, this is now a thin wrapper on `default`; keep it as an entry-point name for discoverability.
- **continuation** (triggered by `$phased-continuation` or "keep going") — run the scheduler loop: research while executing, refill in-flight queue from the pending pool, commit after each completed slice. See that skill for loop-specific rules.

## Subagents

Shared subagents live in `.codex/agents/*.toml`. Dispatch as follows:

- `next_slice_researcher` (read-only) — picks the next viable slices. Use for "what should we work on next?" and at the top of continuation loops. Emits a ranked list of slice envelopes (typically 1–3); primary first.
- `task_designer` (read-only) — decomposes one or more chosen slices into owned subagent tasks. Consumes a single envelope or a ranked envelope list; produces a unified task set with explicit file ownership and cross-slice dependencies.
- `docs_writer` — writes or updates docs, task writeups, doc-first semantic changes. Not read-only because its job is to edit under `docs/` and task notes.
- `verification_reviewer` (read-only) — cross-slice review for correctness, boundary compliance, missing validation. Use when risk is high or multiple workers edited adjacent surfaces.
- built-in `worker` — bounded implementation task. Give it owned paths, outcome, and validation.

## Dispatching subagents

Typed subagents (anything defined in `.codex/agents/*.toml`) take a scoped prompt, not a fork of the parent thread. Compose each call with only what the subagent needs: the slice envelope or the relevant fields, owned paths, the exact deliverable, success signal. Do not try to inherit full conversation context — that dispatch mode is rejected and wastes a round-trip on retry.

Pass task-relevant facts only. Do not leak orchestration metadata ("user invoked `$phased-continuation`", the current skill name, what mode you are in). The subagent does not need to know why you are dispatching it — only what to produce. Mentioning a skill in the prompt can make the subagent re-invoke it and load irrelevant instructions, bloating its context.

## Waiting on subagents

Subagents take minutes, not seconds. Slow is not stuck. Once work is dispatched, waiting is the job.

- While any subagent is live, do not start local implementation, read code "in preparation", draft docs, or spawn agents to fill time. Planned parallel fan-out is expected; ad-hoc spawning because you feel idle is the failure mode. Patience is the default, not the fallback.
- Do not cancel or restart a live subagent because it feels slow. Cancellation requires concrete evidence of being stuck: silence for several minutes AND no sign of work in the latest output. Reading files, running searches, and writing code are work, not silence.
- Do not spawn duplicates of in-flight work. Before spawning, read `.agents/state/slices.jsonl`.
- If a live subagent seems off-track, ask it for a status update. Do not kill and restart.

## Playbook routing

Dispatch to a playbook skill when the change touches its surface. Playbooks are libraries, not entry points:

- `$change-gate` — classify shared-interface vs implementation-only. Run first whenever semantics, IDs/enums, manifests, migrations, ownership, or parity claims may change. Output fills the envelope's `change_class`, `docs_to_update`, `write_owner`.
- `$capability-slice` — consumer-capability mapping to routes, projections, tests, rollout criteria.
- `$manifest-rollout` — manifest, discovery, capability-flag, invalidation changes.
- `$replay-boundaries` — replay, canonicality, projection rebuilds, execution-boundary work.

## Slice envelope

All research, design, and review subagents communicate via a shared schema. See `references/slice-envelope.md` for the canonical fields and how they compose with `$change-gate` output. Extend subagent prompts to emit or consume it rather than inventing per-agent output shapes.

## Slice log

When fanning out or looping, append to `.agents/state/slices.jsonl` — one JSON line per event:

```
{"slice_id": "...", "status": "picked|in_flight|completed|blocked", "ts": "...", "owned_paths": [...], "subagent": "...", "notes": "..."}
```

`next_slice_researcher` must read the log before picking so in-flight or completed slices are not re-picked. The log is gitignored and ephemeral; one line per state transition is enough.

## Task template

For each delegated task, specify:

- exact outcome
- files or directories owned (disjoint from other in-flight slices)
- surfaces not to touch
- validation or tests to run if editing code
- report back: changed files, evidence gathered, unresolved risks, assumptions

Bounded deliverables only — no vague goals like "figure it out". Tell each subagent it is not alone in the codebase and must not revert others' edits.

## Preferred fan-out pattern

Pipeline the read-only stages. Parallelize within every stage that allows it. Workers at task level stay bundled (code + tests + wiring per task); the parallelism sits in how many tasks run concurrently, not in slicing a single capability across workers.

1. If the real question is what to do next, delegate slice selection to `next_slice_researcher`. Expect 2–4 envelopes back.
2. For every returned envelope, fire `$change-gate` and `task_designer` in parallel — they are read-only, share no write ownership, and there is no correctness reason to serialize them.
3. Dispatch workers from `task_designer`'s `concurrent_wave` groupings up to `max_threads=6` across the whole in-flight set. Append every dispatched slice to the slice log.
4. As soon as workers are dispatched, if more slices may be available, spawn another `next_slice_researcher` so the next cycle's pending pool is ready when workers drain. Research runs *during* execution; waiting until workers return is a cache miss.
5. Use `docs_writer` for doc and task-writeup changes.
6. Use `verification_reviewer` for cross-slice checking when `parallel_risk` was `coordinated` or when multiple in-flight slices actually touched adjacent surfaces.
7. Steer live agents: answer blockers, redirect unclear work, request tighter follow-up. Close agents when their work is integrated.
8. Synthesize results for the user without taking over implementation.

## Final response

- Summarize which subagents were used and what each produced.
- Distinguish subagent results from your own synthesis.
- Call out conflicts, unresolved risks, missing validation, and places where user direction is needed.
