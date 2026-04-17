# Slice envelope

Canonical schema for communicating about a slice across research, design, review, and execution subagents. Emit or consume the full envelope — do not invent per-agent shapes.

## Fields

- `slice_id` — stable free-form identifier; used in the slice log. Must be unique per live slice.
- `current_phase` — phase or milestone from `docs/development-plan.md` the slice belongs to.
- `next_slice` — one-sentence statement of what this slice accomplishes end-to-end.
- `why_now` — why this slice unblocks the most useful next work.
- `owned_paths` — approximate work zone (directories or files the implementation expects to write to). Best-effort disjoint from other in-flight slices; minor overlap is tolerated because the orchestrator reconciles and runs `verification_reviewer` post-wave when zones actually collide.
- `blocking_risks` — prerequisite work, doc-first requirements, semantic conflicts.
- `docs_to_touch` — docs that must change in the same commit or before.
- `parallel_risk` — `safe` (default), `coordinated`, or `serial`. `safe` slices dispatch freely; `coordinated` slices trigger a post-wave `verification_reviewer` pass because overlap is likely; `serial` slices block others and should be rare.
- `coordination_note` — optional free-form note from the researcher when benign overlap is expected (e.g. "both slices append to `lib.rs` but at different modules; reviewer should merge import order").
- `success_signal` — concrete outcome that marks the slice meaningfully complete (test passes, route returns, projection rebuilds).
- `change_class` — `semantic`, `shared-interface`, or `implementation-only`. Produced by `$change-gate`.
- `docs_to_update` — exact docs that must change first or alongside code. Produced by `$change-gate`. May overlap with `docs_to_touch`; treat `docs_to_update` as the stricter set.
- `write_owner` — owning workstream or directory from `docs/workstreams.md`.

## Roles

- `next_slice_researcher` emits a ranked list of complete envelopes (primary first, then viable follow-ons — typically 1–3 entries). Returns an empty list and names the smallest unblocker when no viable slice exists.
- `$change-gate` fills `change_class`, `docs_to_update`, `write_owner`. Run it before or alongside research when shared-interface risk is present. With a multi-envelope list, classify each envelope independently unless they genuinely share a single shared-interface change.
- `task_designer` consumes one envelope or a ranked list of envelopes and produces a unified task set. Every task references its originating envelope's `slice_id`, inherits a subset of that envelope's `owned_paths`, and respects its `change_class`. Cross-slice dependencies are marked explicitly.
- `verification_reviewer` reads the envelope(s) for context and flags gaps between declared fields and what the diff actually changed.
- Worker subagents inherit `owned_paths`, `success_signal`, `docs_to_update` as guardrails from their task's originating envelope.

## Slice log

Envelope state transitions append to `.agents/state/slices.jsonl`:

```
{"slice_id": "...", "status": "picked|in_flight|completed|blocked", "ts": "...", "owned_paths": [...], "subagent": "...", "notes": "..."}
```

- `picked` — researcher chose the slice; envelope exists but no worker is assigned.
- `in_flight` — at least one worker has started on the slice.
- `completed` — integration done; success signal observed.
- `blocked` — work paused pending unblocker described in `notes`.

Research reads the log before picking to avoid re-picking in-flight or completed slices. The log is gitignored; one line per state transition is enough.
