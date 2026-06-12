# 2026-06 Remediation Post-Mortem

## Summary

The 2026-06 remediation started from a full-codebase review run on 2026-06-10 at commit `602d3f1` on `fix/ens-v1-registry-owner-authority`. The review produced eight file-disjoint workstreams, WS-A through WS-H, mapped to the repository ownership boundaries so the work could proceed in parallel without unmanaged file conflicts.

Every P0/P1 item from that review was closed through reviewed and tested pull requests merged to `main` between 2026-06-10 and 2026-06-12. The final P0/P1 close was PR #62, merged as `a964c60`. All remaining P2/P3 items were re-triaged to normal backlog issues #30-#61 on 2026-06-12.

The orchestration model was explicit: Codex implemented and maintained the tracking records, Claude supervised and reviewed adversarially, and the repo owner retained merge authority and semantic/product-decision authority.

## What Shipped, Per Workstream

### WS-H - Safety net and docs truth

- PR #13 (`2891acd`) added the conformance suite to CI, restored the full conformance baseline to green, and gated Docker image publishing on successful CI. Compose healthchecks were added for API/indexer/worker readiness.
- PR #62 (`a964c60`) closed the docs corrections sweep. MinIO/object-storage claims were removed everywhere, including ADR-0001, because no object-store layer existed in code. The conformance docs were corrected to Rust, stale 400-requirement text was fixed, code-read environment variables were added to examples, the `crates/domain` ownership map was corrected, the fuse-masking doc claim was verified already fixed by PR #19, and two compose env-forwarding lines for ops-catchup guards were added from bot triage.

### WS-A - ENSv1 authority lifecycle

- PR #15 (`a878609`, later shipped to `main` through gate PR #16 `12bcea0`) closed the three gate P0s: repair-SQL validation moved out of a non-filtering `LEFT JOIN ... ON` shape, ENSv1 registry `Transfer(bytes32,address)` was decoded, and unwrap reactivated the prior registrar lease/resource lineage. The owner-divergence/convergence behavior was documented in the identity ADR/upstream docs.
- PR #26 (`5a09222`) closed the remaining WS-A P1 pair: NameWrapper `TransferBatch` topic admission and ABI decode with per-id fan-out, and stale wrapper authority clearing on strict post-grace lease release or fresh unwrapped re-registration. The same PR also closed the grace-boundary off-by-one P2 by moving release to the first block strictly after expiry plus grace.

### WS-B - ENSv2 and preimage coverage

- PR #22 (`f29f48e`) admitted ENSv2 registry/root `EACRolesChanged`, emitted `PermissionChanged` and `RootPermissionChanged` with source-family role vocabularies, expanded preimage intake for post-2023 controller signatures, reconciled the documented taxonomy, and changed malformed `ReverseClaimed` / `AliasChanged` handling so malformed single logs did not fail whole batches.
- PR #24 (`92da58f`) consumed `RootPermissionChanged` and registry-scope permission events in projections. `permissions_current` consumed both event kinds with the registry vocabulary, `registry_root` mapped to root scope, derive/invalidation widened, and root-derivation provenance through `inheritance_path=registry_root_fallback` was pinned by tests.
- PR #25 (`a200fee`) bumped `CURRENT_PROJECTION_REPLAY_VERSION` from 4 to 5 after PR #24 widened the permissions input set, forcing bootstrap replay for deployments with stale version-4 markers.
- PR #28 (`4a00d70`) implemented the owner product decision from 2026-06-12: name-level ENSv2 role reads pre-compose root fallback. The route now performs read-time composition with adapter-equivalent root-anchor derivation, fail-closes on malformed provenance, narrows `resource_id` filters coherently, and shipped docs plus regenerated OpenAPI.

### WS-C - Projection pipeline integrity

- PR #19 (`74ea76d`) implemented wrapper fuse masking of `effective_powers` in `permissions_current` and `address_names`, including resolver/address_names invalidation fan-out. The same PR added dead-letter handling, manifest-driven invalidation, per-key serialization plus whole-batch heartbeat for multi-worker stale-overwrite protection, staged replacement full rebuilds with marker hygiene, and database-scoped replay-readiness index probing.

### WS-D - Intake resilience

- PR #18 (`36ab81a`) fixed backfill lease refresh and the stranded-job race, fixed reorg-during-backfill orphan preservation/revival boundaries, and added RPC resilience for retries, provider log-limit patterns, and safe/finalized tag degradation.
- PR #27 (`f1003be`) unified the inline, sparse, and Coinbase materialization paths behind one shared stage. Sparse bootstrap was widened to live semantics for transaction-sibling logs and code observations, a three-path parity test was added, and `docs/chain-intake.md` documented the upgrade-path caveat for pre-unification ranges.

### WS-E - Verified execution and primary names

- PR #17 (`5521105`) accepted `https://` RPC endpoints, fixed EIP-3668 behavior, and corrected API primary-name classification semantics. It partially closed verified-primary work by documenting the producer as backfill-fed and landing readback coin-type canonicalization.
- PR #23 (`2d005fb`) fully closed verified-primary wiring: claim-change invalidation was wired, including hydration paths; full cache-identity write/read symmetry was enforced; post-upsert canonical validation was added; and drifted cache identities read back as misses instead of stale hits.

### WS-F - Storage write-path and performance

- PR #20 (`dc83c79`) added reorg-orphaning indexes, made row-path identity writes atomic with row locks, and set the repair policy to code-first. It partially closed the trigger/write-amplification bundle by gating count/feed sidecar triggers on readability-class transitions.

### WS-G - API contract and pagination

- PR #21 (`89bd97a`) moved `/v1/events` and `/v1/history/*` to SQL keyset pagination, gated `/v1/names` total counts on `include=total_count`, and closed the input-validation consistency P2. The same PR advanced contract polish by landing empty-enum defaults and documenting input behavior changes.

## Defects Caught Pre-Merge

The table lists 33 pre-merge defect classes that were found by review, CI, bot triage, or adversarial passes before the corresponding PR reached `main`.

| # | PR | Defect caught before merge | Mechanism |
| --- | --- | --- | --- |
| 1 | #15 | Indexer integration fixtures did not include the new `label_preimages` / projection support required by the branch, producing red indexer shards. | CI shard plus supervisor review |
| 2 | #15 | Worker fixtures still seeded placeholder labelhashes that violated `children_current_labelhash_check`. | CI shard plus supervisor review |
| 3 | #15 | The divergence docs described registry-side convergence restore that the code did not yet perform. | Review blocker, hand verification |
| 4 | #15 | NewOwner routing changed stored-history/replay behavior for fresh registrations. | Review blocker, hand verification |
| 5 | #15 | Block-hash restricted preload reconstructed diverged registrar state as current instead of superseded, so live reconciliation did not restore convergence and mis-attributed diverged intervals. | Post-approval adversarial pass plus red-test audit |
| 6 | #16 | Restart handoff required target coverage and would full-rebuild all current projection families on ordinary restart. | Fable adversarial pass plus supervisor hand-verification |
| 7 | #16 | Owner-preservation repair for Known-stored/Null-incoming cases was non-idempotent and refired on every replay/canonicality presentation. | Fable adversarial pass plus red-test audit |
| 8 | #16 | The handoff check retained a replay-readiness gate whose cluster-wide `pg_stat_progress_create_index` probe caused CI flake and a restart-stall risk. | CI worker shard plus scoped adversarial pass |
| 9 | #17 | `raw_claim_name` documentation still said it always came from `primary_names_current`, but the new ENS/60 on-demand invalid-name fallback used a live reverse-RPC claim. | Supervisor review, doc-code audit |
| 10 | #17 | Verified-primary persistence accepted non-canonical `coin_type` metadata that API readback compared verbatim, causing accept-then-500 behavior. | Supervisor review plus red-test audit |
| 11 | #18 | Generic lineage/raw upserts could revive stored orphaned blocks during backfill instead of preserving orphan state. | Supervisor review plus red-test audit |
| 12 | #18 | AwaitingAncestor persistence routed unproven raw blocks through revival-capable writes and erased orphan markers. | Re-review blocker plus red-test audit |
| 13 | #18 | Concurrent completion of the last two backfill ranges could strand a job in `running` with no reservable ranges. | Supervisor hand-verification plus deterministic red test |
| 14 | #19 | `PARENT_CANNOT_CONTROL` incorrectly masked owner `resource_control` rows for ordinary wrapped `.eth` names. | Supervisor review plus upstream check and red-test audit |
| 15 | #19 | `resolver_control` was not masked on `CANNOT_SET_RESOLVER`. | Supervisor review plus red-test audit |
| 16 | #19 | Missing or malformed fuse state published unmasked powers instead of failing closed. | Supervisor review plus red-test audit |
| 17 | #19 | `address_names_current` bypassed the shared fuse masking path and kept burned-fuse controller rows. | Codex-bot review, supervisor verification, red-test audit |
| 18 | #19 | Fuse burns did not invalidate `resolver_current`, leaving resolver summaries stale. | Codex-bot review, supervisor verification, red-test audit |
| 19 | #20 | Concurrent index builds lacked invalid-index self-heal; a failed `CREATE INDEX CONCURRENTLY IF NOT EXISTS` could leave an invalid index that future runs skipped. | Supervisor migration review |
| 20 | #21 | Public 400 behavior changes for malformed addresses, unnormalized path names, and empty enums were not documented. | Supervisor review, doc-first gate |
| 21 | #21 | The history keyset index migration needed invalid-index self-heal and a timestamp renumber because it collided with the WS-F migration block. | WS-F migration-owner review |
| 22 | #22 | ENSv2 registry/root role bitmaps were decoded with the resolver role vocabulary, producing wrong public `effective_powers`. | Supervisor review plus pinned-upstream audit and red-test audit |
| 23 | #22 | Malformed-log drop paths lacked operator-visible warning/identity context. | Supervisor review blocker |
| 24 | #23 | Legacy reverse-resolver hydration writes/deletes bypassed verified-primary claim-change invalidation. | Supervisor review plus red-test audit |
| 25 | #23 | Verified-primary write gates accepted trace identity drift that readback would later reject or miss. | Supervisor review plus red-test audit |
| 26 | #23 | Producer-side verified-primary persistence could accept storage-normalized noncanonical cache identity artifacts. | Codex-bot triage plus red-test audit |
| 27 | #24 | Test fixtures did not mirror real adapter emission for registry/root permission events, leaving root-derivation provenance unpinned. | Supervisor test-fidelity review |
| 28 | #25 | PR #24 widened `permissions_current` inputs without bumping the projection replay version, so upgraded deployments could skip historical root-permission grants. | Codex-bot triage plus supervisor hand-verification |
| 29 | #26 | Indexer manifest fixtures omitted the newly admitted `TransferBatch` ABI signature, failing the indexer shard. | CI shard plus supervisor verification |
| 30 | #27 | The widened materialization/fact-retention semantics lacked an upgrade-path note for pre-unification ranges. | Supervisor review, doc-first gate |
| 31 | #28 | Name-role root fallback composition needed coherent `resource_id` filter narrowing so filtered reads did not mix composed and direct rows incorrectly. | Codex-bot triage plus red-test audit |
| 32 | #28 | A follow-on filter asymmetry remained after the first fix commit and required a second narrowed fix. | Scoped adversarial pass plus supervisor verification |
| 33 | #62 | Compose did not forward `POSTGRES_MAX_BYTES` to the container path that code read. | Codex-bot triage plus supervisor rendered-config verification |

## Process Lessons

- Bounded test forms were necessary on shared boxes. Unbounded `cargo test --workspace` and unbounded conformance runs exhausted local Postgres pools in ways CI did not reproduce; the bounded `RUST_TEST_THREADS=4 ./scripts/test-db -- ...` forms became the reliable local gate.
- Projection replay versions had to bump whenever a projection's consumed input set widened. The PR #24 / PR #25 incident showed that green projection tests were not enough if old replay markers could skip historical inputs.
- Upgrade-path documentation was required whenever retention or fact-set semantics widened. PR #27 changed the durable backfill fact set, and the merge blocker was the missing operator truth for pre-unification ranges.
- Migrations were serialized through WS-F and renumbered at merge. This avoided timestamp collisions and forced concurrent-index failure modes to be reviewed before merge.
- Manifest changes required the indexer shard locally. Fixture-backed manifest validation caught real omissions, including the `TransferBatch` ABI fixture gap in PR #26.
- Codex-bot review comments became a standing triage step. They were not accepted blindly, but validated or refuted with evidence; validated comments found real P1/P2 defects.
- Adversarial passes on fix commits caught real defects three times: PR #15's replay/live convergence mismatch, PR #16's restart-handoff/readiness regression, and PR #28's filter asymmetry.
- The never-green-base rule was necessary. Branches with no prior CI history required full local workspace/test-shard coverage before review claims could be trusted.
- Doc-first discipline kept the public contract honest. Docs were changed to describe what code actually did, and semantic changes landed with the owning docs instead of retroactive cleanup.

## Residual Work

All remaining P2/P3 work was moved to normal backlog on 2026-06-12. Public-semantics changes that were made during the remediation were recorded doc-first in their owning docs during the work itself.

### WS-A - ENSv1 authority lifecycle

- #30 - Convergence-preload follow-up tests.
- #31 - Basenames AuthorityTransferred owner Known<->Null parity.
- #32 - Convergence-preload polish.
- #33 - Robustness bundle.
- #34 - Released-resource provenance fidelity.

### WS-B - ENSv2 and preimage coverage

- #35 - Projection-consumption polish.
- #36 - Root-fallback route polish.
- #37 - WS-B robustness leftovers.

### WS-C - Projection pipeline integrity

- #38 - ENSv2 `ParentChanged` to `children_current` invalidation.
- #39 - Tooling: range rebuild, rewind, missing inspect subcommands.
- #40 - Efficiency bundle.
- #41 - Dead-letter operational polish.
- #42 - Apply-lock hardening.
- #43 - Converge the two staged-publish mechanisms.

### WS-D - Intake resilience

- #44 - Bootstrap code-observation cost cliff.
- #45 - Materialization residual seams.
- #46 - Branch-proof safe/finalized head revival.
- #47 - Align child payload-table canonicality CASEs with the preserve contract.
- #48 - Provider-resilience refinements.
- #49 - Robustness bundle.
- #50 - RPC-cost bundle.

### WS-E - Verified execution and primary names

- #51 - Verified-primary polish.
- #52 - Record-selector coin_type canonicalization.
- #53 - Typed failure taxonomy and trace completeness.
- #54 - Manifests polish.

### WS-F - Storage write-path and performance

- #55 - Trigger/write-amplification bundle.
- #56 - Consistency bundle.
- #57 - Write-path hardening.

### WS-G - API contract and pagination

- #58 - Contract polish bundle.
- #59 - Efficiency bundle.
- #60 - API polish.

### WS-H - Safety net and docs truth

- #61 - Test-matrix holes.

## Timeline

- 2026-06-10: The whole-codebase review at `602d3f1` produced the remediation plan. WS-H landed first through PR #13 to put CI/conformance and publish gates in place.
- 2026-06-11 to early 2026-06-12: Wave 1 landed WS-C, WS-D, and WS-E through PRs #17, #18, and #19, after the WS-A gate branch landed through PRs #15 and #16.
- 2026-06-12: Wave 2 landed WS-B, WS-F, WS-G, and WS-E wiring through PRs #20, #21, #22, and #23.
- 2026-06-12: The follow-up wave landed projection consumption, replay-version bump, WS-A wrapper authority, WS-D materialization unification, and root-fallback pre-composition through PRs #24, #25, #26, #27, and #28.
- 2026-06-12: Endgame re-triaged every remaining P2/P3 to issues #30-#61, then PR #62 closed the final open P1. The mission definition was met at `a964c60`.
