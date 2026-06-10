# Remediation Orchestration Process — 2026-06 (TEMPORARY)

**This doc is temporary.** It exists only to drive the remediation tracked in [`remediation-2026-06.md`](remediation-2026-06.md). Delete all three `remediation-2026-06*` files (or move a short summary into the changelog/ADR if anything changed public semantics) once every P0/P1 checkbox is closed and the P2/P3 leftovers are either done or explicitly re-triaged into normal backlog.

**Audience:** an orchestrating agent (or human) with no prior context. Everything you need is referenced from here.

## Read first, in order

1. `AGENTS.md` — process rulebook. Binding: doc-first rules, upstream citation rules (`.refs/` + commit), file-size gate, staging discipline.
2. [`workstreams.md`](workstreams.md) — ownership boundaries and high-conflict rules.
3. [`remediation-2026-06.md`](remediation-2026-06.md) — the work itself: 8 workstreams (WS-A … WS-H), each a checklist with priorities, evidence paths, and acceptance criteria.
4. [`remediation-2026-06-appendix.md`](remediation-2026-06-appendix.md) — the full subsystem review reports the checklists were condensed from. Reference material: use it when re-verifying an item (it carries the full reasoning chain) and before working any P2/P3 "bundle" item (the named specifics live only there). It also records which three findings were already hand-verified and which areas were verified-good.

## Mission

Close every **P0 and P1** checkbox in `remediation-2026-06.md` via reviewed, tested PRs to `main`. P2/P3 items are fast-follows on the same workstream branches when cheap, or re-triaged at the end. Do not expand scope beyond the listed items; new discoveries get a new checkbox (with evidence) rather than silent inclusion.

## Standing rules (apply to every item)

- **Re-verify before fixing.** Every item is a review claim, not gospel. First reproduce it: a failing test, a query plan, or direct code reading that confirms the behavior. If a claim turns out wrong, do NOT "fix" it — strike the checkbox with a one-line rebuttal and evidence (`~~item~~ — rejected: <reason, file:line>`). The review was thorough but automated; expect a small number of rejects.
- **Failing test first** wherever the item is a behavior bug (the acceptance criteria usually name the test). Repair-SQL and adapter-semantics items especially: the absence of a rejection/edge test is part of the finding.
- **Doc-first** (per `AGENTS.md`): if an item changes public semantics, shared IDs/enums, coverage meaning, manifest schema, or replacement meaning, the owning doc updates in the same PR. Items marked "doc-first" in the plan are decisions (implement vs descope) — make the decision explicit in the doc either way.
- **Upstream claims need citations.** Any PR justifying behavior by ENSv1/ENSv2/Basenames semantics cites pinned sources as `(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)`. Never paraphrase upstream from memory.
- **Use the repo skills** at the right moments: `$contract-impact` before starting any item touching public semantics; `$replay-safety` for anything in WS-A/WS-C/WS-D/WS-F that touches raw facts, normalized events, canonicality, rebuilds, or migrations; `$manifest-authority` for WS-E manifest items; `$verify-loop` before declaring a workstream's PR ready.
- **Stage explicit paths only.** Never stage unrelated work. Inspect dirty state first.

## Execution order

1. **WS-H first, alone.** It adds the CI gates (conformance suite, publish gating) that protect every later landing. Nothing else merges before WS-H's two P0s are on `main`.
2. **WS-A items 1–3** are merge gates for the pre-existing branch `fix/ens-v1-registry-owner-authority`. Land them on that branch (or a branch off it), then merge it. Remaining WS-A items branch off `main` afterwards.
3. **All other streams run in parallel** once WS-H is merged. Recommended concurrency: 3–4 streams at a time; more adds coordination cost faster than throughput. Suggested wave order by risk: WS-C + WS-D + WS-E first (production-correctness P0/P1s), then WS-B + WS-F + WS-G.

## Per-workstream loop

For each workstream:

1. Create branch `fix/ws-<letter>-<slug>` off `main` (e.g. `fix/ws-c-projection-integrity`). One workstream = one branch = one PR (or a small stacked chain if the stream's P0s should merge before its P3s).
2. Work items top-to-bottom (they are priority-ordered). For each: re-verify → failing test → fix → acceptance criteria met.
3. Respect the stream's **file scope** as listed in the plan. If an item genuinely requires editing another stream's files, see Coordination below — do not just edit them.
4. Run the full local gate before PR: `cargo fmt --check`, `cargo clippy --workspace --all-targets`, `cargo test` for touched packages, `scripts/check-rust-file-size`, plus the conformance suite (`cargo test --manifest-path tests/conformance/Cargo.toml --locked`) for anything touching public contracts, projections, or replay.
5. Run `$verify-loop` on the result.
6. PR description: list the checkbox items closed (copy their text), evidence of each acceptance criterion, and any rejected items with rebuttals.
7. On merge: tick the checkboxes in `remediation-2026-06.md` and update the status table below — in the same PR or an immediate follow-up commit, so the tracking doc never lags.

## Coordination (the only ways streams may touch each other)

- **Migrations are serialized through WS-F.** Any stream needing a migration (known: WS-C dead-letter state; WS-F's own index/trigger work; possibly WS-A repair tests) writes the migration in its own branch but: (a) announces it in the status table's Notes column first, (b) gets WS-F review, (c) renumbers the timestamp at merge time to be the newest. Never two unmerged migrations with interleaving timestamps.
- **Shared `crates/storage` files are pre-assigned:** WS-D owns `backfill_jobs/` + `lineage/`; WS-G owns `history.rs`; WS-A owns `normalized_events/upsert/repair/`; all other storage files are WS-F. If your item needs a file you don't own, ask the owning stream to expose what you need (or sequence behind its merge).
- **Conformance fixtures**: new/changed fixtures get review from a second stream's perspective (per `AGENTS.md` — fixtures are cross-workstream review points).
- **Docs**: WS-H owns broad sweeps of `storage.md`/`architecture.md`; other streams edit only the single section their item names. If WS-H's sweep and a stream's targeted edit collide, the targeted (semantic) edit wins; WS-H rebases.
- **WS-E ↔ WS-G seam**: primary-name route *semantics* (coverage class, ResultStatus, coin_type) belong to WS-E even though files live under `apps/api`. WS-G does not touch `primary_name_lookup.rs` or primary-name response builders.

## Orchestrator responsibilities

- Maintain the status table below; it is the single live view.
- Enforce execution order and the migration serialization rule.
- When spawning sub-agents per workstream: give each agent (a) this doc, (b) its single workstream section from `remediation-2026-06.md`, (c) its file scope, and (d) the standing rules above. Bounded work with a clear output contract only — no open-ended "keep shipping" loops (per `AGENTS.md`).
- Re-triage at the end: remaining P2/P3 items either get done, moved to normal backlog with an owner, or explicitly dropped with a line of rationale appended to `remediation-2026-06.md`.
- Tear-down: when the mission definition is met, delete this file and either delete `remediation-2026-06.md` or trim it to a closed-out record, whichever the repo owner prefers at that time.

## Status

| Stream | Status | Branch | PR | Notes |
| --- | --- | --- | --- | --- |
| WS-H Safety net & docs | PR open | `fix/ws-h-safety-net-docs` | [#13](https://github.com/TateB/bigname/pull/13) | WS-H P0 checkboxes are closed on the branch; approved cross-scope healthcheck command patch touches indexer/worker; Docker publishes only from successful CI on main, with release-tag publishing deferred rather than left as an ungated path; no other stream merges to main before WS-H P0s land |
| WS-A ENSv1 authority | not started | `fix/ens-v1-registry-owner-authority` | — | Items 1–3 gate the existing fix branch; may be prepared there but must not merge to main before WS-H |
| WS-B ENSv2 + preimage | not started | — | — | |
| WS-C Projection pipeline | not started | — | — | Will need a migration (dead-letter) — WS-F review |
| WS-D Intake resilience | not started | — | — | Owns storage `backfill_jobs/`, `lineage/` |
| WS-E Verified execution & primary names | not started | — | — | Owns `primary_name_lookup.rs` seam |
| WS-F Storage write-path & perf | not started | — | — | Migration reviewer for all streams |
| WS-G API contract & pagination | not started | — | — | Owns storage `history.rs` |

Status values: `not started` → `in progress` → `PR open` → `merged` → (`re-triaged` for leftovers).
