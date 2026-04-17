---
name: phased-continuation
description: Continue phased development in bigname. Use whenever the user asks to keep going, continue implementation, pick the next work, suggest the next slice, continue phased development, or just keep shipping. Runs $orchestrate in continuation mode as a parallel scheduler — refilling an in-flight queue from next_slice_researcher, pipelining design stages, and committing after each completed slice until blocked or redirected.
metadata:
  kind: coordination
---

# Phased Continuation

Continuation-mode wrapper for `$orchestrate`. Use when the user wants steady forward progress without re-specifying the next task.

Apply `$orchestrate` with the extras below. Do not drift into direct implementation because the next slice looks obvious. Do not fall back to serial execution because the scheduler shape is new — the loop is designed around concurrent work and only degrades to a single slice when research genuinely finds nothing else viable.

## Scheduler loop

Maintain two things across the loop:

- an **in-flight queue** — slices whose workers are dispatched but not yet complete
- a **pending pool** — slices returned by the most recent `next_slice_researcher` pass that have not yet been dispatched

On every iteration:

1. Read `.agents/state/slices.jsonl` so in-flight and completed slices are visible.
2. If both the in-flight queue and the pending pool are empty, spawn `next_slice_researcher` to refill the pool. Pass the log contents so it does not re-pick live work. Expect 2–4 envelopes back.
3. For every envelope in the pending pool that is not yet classified, fire `$change-gate` and `task_designer` in parallel (both read-only — do not serialize them). Log each slice as `picked`.
4. Drain the pending pool into the in-flight queue by dispatching waves from `task_designer` output up to `max_threads=6` concurrent workers across the whole in-flight set. Log each dispatched slice as `in_flight`.
5. As soon as workers are dispatched, if the pending pool is empty, spawn `next_slice_researcher` again for cycle N+1 so research runs concurrently with execution. Do not wait for workers to finish before re-researching.
6. When any slice completes, commit it (see below), log it `completed`, and return to step 4 — refill the in-flight queue from the pending pool or from fresh research.

Research, design, dispatch, and commit are the orchestrator's work. Implementation is the workers'.

## Commit after each completed slice

After each slice reaches its success signal, the orchestrator commits directly — do not batch multiple slices into one commit, and do not defer commits to a later cycle:

- `git add` the slice's `owned_paths` plus any docs the slice touched.
- `git commit -m` with a subject that references the `slice_id` and a one-line description of the capability that shipped. Follow the repo's existing commit style from `git log`.
- One commit per completed slice — the log stays bisection-friendly and `next_slice_researcher` gets clean ground truth on what has shipped.
- Do not push; pushing stays a user action.

`git add` and `git commit` are auto-approved for the orchestrator via `.codex/rules/default.rules`. Any other git command still requires approval.

## Do not decide the next slice locally

If `next_slice_researcher` can answer "what's next", let it. The loop exists so research runs concurrently with execution — there is no speed advantage to shortcutting it, and the researcher's access to `slices.jsonl` keeps picks coherent across cycles.

## Stop conditions

Stop and report when:

- the repo is blocked by unresolved semantics or shared-interface work and no unblocker slice exists
- a prerequisite slice is missing and should become the real next slice (blocked envelope in the pool)
- `next_slice_researcher` returns an empty list twice in a row
- the user interrupts or redirects
- the current phase exit criteria are satisfied and the next milestone needs explicit user confirmation
