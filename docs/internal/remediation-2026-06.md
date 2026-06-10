# Remediation Plan — 2026-06 Full-Codebase Review

Internal tracking doc for the 2026-06-10 whole-repo review (reviewed at commit `602d3f1` on `fix/ens-v1-registry-owner-authority`, all subsystems, security explicitly out of scope; line numbers reference that commit). Findings are grouped into eight workstreams aligned with the ownership map in [`workstreams.md`](workstreams.md) and chosen to be file-disjoint so they can run in parallel. Each workstream is one branch, one PR chain, one owner.

The checklists below are condensed. The full subsystem review reports — including minor findings, named specifics behind the P2/P3 "bundle" items, reasoning chains, and verified-good areas — are archived in [`remediation-2026-06-appendix.md`](remediation-2026-06-appendix.md). Consult the owning report before working a bundle item.

Priorities: **P0** = production-breaking or merge-gating, **P1** = correctness gaps to schedule next, **P2** = important, **P3** = polish/perf. Evidence paths are repo-relative; line numbers are as of the review commit.

## Cross-stream rules

- **Migrations are serialized.** Any stream needing a migration declares it in its section below before writing it; WS-F owns migration review; timestamps are assigned at merge time, not authoring time.
- **Conformance fixtures are cross-workstream review points** (per `AGENTS.md`); new/changed fixtures get a second-stream reviewer.
- **Doc-first items** update the owning doc in the same PR (`AGENTS.md` rule). WS-H owns broad doc sweeps; other streams make targeted single-section edits only.
- **Shared `crates/storage` files**: WS-D owns `backfill_jobs/` and `lineage/` for the duration; WS-G owns `history.rs`; WS-A owns `normalized_events/upsert/repair/`; everything else storage-side is WS-F.
- One **integrator** watches the merge order and final consistency (per `workstreams.md` § High-Conflict Rules).

## Sequencing

1. **WS-H first** — cheap, no code conflicts, and the CI gates protect every other stream's landing.
2. **WS-A items 1–3 gate the `fix/ens-v1-registry-owner-authority` merge.**
3. All other streams start in parallel at will. WS-C item 1 (fuse masking) has no dependency on WS-A — the adapter already emits `PermissionScopeChanged`.

---

## WS-A · ENSv1 authority lifecycle (owner: Intake and Adapters)

Files: `crates/adapters/src/ens_v1_unwrapped_authority/**`, `crates/adapters/src/registry_migration_cache.rs`, `crates/storage/src/normalized_events/upsert/repair/**`. Items 1–3 block the current branch.

- [ ] **P0 — Repair-SQL validation never filters (LEFT JOIN ON-clause).** All resource-anchor proofs in `crates/storage/src/normalized_events/upsert/repair/ens_v1_registry_event_time.rs:208-633` hang off `LEFT JOIN resources new_resource ON ...`; LEFT JOIN ON-predicates cannot remove rows, and the `updated` CTE repoints `resource_id` from `input.new_resource_id` regardless, so the fail-closed gate (`docs/storage.md` repair rules) cannot fire. Siblings (`ens_v1_renewal.rs`, `ens_v1_registry_event_time_null_resource.rs`) use INNER JOIN/WHERE correctly. Accept: validation in WHERE; failing test proving rejection of an orphaned/wrong-name/wrong-labelhash anchor.
- [ ] **P0 — Decode registry `Transfer(bytes32,address)`.** Selected (`constants.rs:58`), suppression-checked, never decoded; `RegistryOwnerChanged` only comes from `NewOwner` (`observation.rs:129-155`). All `setOwner`/`setRecord` owner moves invisible, incl. NameWrapper wrap/unwrap registry handovers (upstream: `.refs/ens_v1/contracts/registry/ENSRegistry.sol:63-69`). Accept: namehash-targeted decode branch, migration suppression still applied.
- [ ] **P0 — Unwrap fails to reactivate the prior registrar lease** (depends on the item above). Wrap demotes via the registry-owner-supersedes heuristic (`apply_registrar.rs:407-447`); unwrap restore compares stale `current_registry_owner` → wrap→unwrap binds registry-only instead of reactivating the registrar resource/lineage (violates `docs/architecture.md` rebind table). The owner-divergence rotation itself is an undocumented divergence — doc-first: document in `docs/upstream.md` or restrict. Accept: round-trip test reactivates prior resource + lineage.
- [ ] **P1 — Watch + decode NameWrapper `TransferBatch`** (`event_topics.rs:50-56` has `TransferSingle` only; subgraph handles batch). Accept: per-id fan-out, multi-id test.
- [ ] **P1 — Clear stale wrapper authority on lapse + re-registration.** Nothing clears `current_wrapper_key` except observed `NameUnwrapped` (`apply_registrar.rs:303-305`); expired-past-grace + re-registered names keep the dead wrapper anchor. Accept: wrapper authority expires on lease release or fresh `RegistrationGranted` without same-tx `NameWrapped`.
- [ ] **P2 — Grace-boundary off-by-one.** Release fires at `timestamp == expiry+90d`; upstream allows renewal at exactly that timestamp (`BaseRegistrarImplementation.sol:161`). Accept: release strictly greater; boundary-renewal test.
- [ ] **P3 — Robustness bundle:** reorged-away migration markers stay suppressed until restart (`registry_migration_cache.rs:131-166`); zero-length binding segments drop without `SurfaceUnbound` (`transition.rs:16-57`); `RegistrationGranted.before_state.registrant` always null (`apply_registrar.rs:54`); record events from non-current resolver dropped (`apply_resolver.rs:109-111`); two diverging migration-guard impls — share one.

## WS-B · ENSv2 + preimage coverage (owner: Intake and Adapters)

Files: `crates/adapters/src/ens_v2_*`, `crates/adapters/src/block_derived_normalized_events/**`, `crates/adapters/src/ens_v1_reverse_claim/`, taxonomy section of `docs/architecture.md`. Disjoint from WS-A modules.

- [ ] **P1 — Consume ENSv2 registry/root `EACRolesChanged`.** Only the resolver family's role events are read (`ens_v2_permissions/load.rs:32`); registry/root EAC events — the core v2 permission surface — watched nowhere; `RootPermissionChanged` never emitted despite the taxonomy. Accept: registry EAC watched; `PermissionChanged`/`RootPermissionChanged` per docs.
- [ ] **P1 — Preimage intake for post-2023 controller signatures.** `block_derived_normalized_events/constants.rs:21-44` covers legacy controller events only; wrapped/unwrapped controller variants and Basenames controllers feed nothing into `label_preimages`. Decode logic duplicated and already diverged (lost NUL check vs `preimage_observation.rs:9-11`). Accept: all admitted signatures produce observations; one shared decode helper.
- [ ] **P2 — Taxonomy reconciliation (doc-first).** Never emitted: `RecordDeleted`, `CommitmentMade`, `DelegateRetainedAfterTransfer` (+ `RootPermissionChanged`, covered above). Emitted but undocumented: `RegistrarNameRegistered` (`ens_v2_registrar.rs:29`). ENSv2 ERC-1155 `TransferSingle/Batch` unwatched (owners never update post-registration; sepolia scope). Accept: each kind implemented or removed from the taxonomy with rationale.
- [ ] **P3 — One malformed log fails the batch:** `ReverseClaimed` node mismatch `bail!`s the whole sync (`ens_v1_reverse_claim/events.rs:61-71`); same for malformed `AliasChanged` (`block_derived_normalized_events/event_builders.rs:199-210`). Drop/flag the single log instead.

## WS-C · Projection pipeline integrity (owner: Projections and API)

Files: `apps/worker/**`, projection-queue migrations (declare to WS-F: dead-letter state column/enum).

- [ ] **P0 — Implement wrapper fuse masking of `effective_powers`.** Docs promise fail-closed fuse folding (`docs/api-v1-routes.md:460`, `docs/projections.md:115-119`); projection loads only `PermissionChanged` (`apps/worker/src/permissions/load.rs:82-86`) and copies powers verbatim; `PermissionScopeChanged` consumed nowhere. Found independently twice. Accept: latest canonical scope modifier applied; burned-fuse end-to-end test (also closes a test-matrix hole).
- [ ] **P1 — Dead-letter handling.** `attempt_count` written, never read (`projection_apply/apply.rs:323`); a failing key retries forever every 60s and blocks primary hydration. Accept: max-attempt threshold → dead-letter state with operator visibility. *(Needs migration — coordinate with WS-F.)*
- [ ] **P1 — Manifest-driven invalidation producer.** Docs list manifest/capability changes as triggers; no producer enqueues them — projections reading manifest state at rebuild stay stale until an unrelated event lands. Accept: manifest sync enqueues affected family keys.
- [ ] **P1 — Close the multi-worker stale-overwrite window.** Generation bump clears the in-flight claim (`derive_queries.rs:52-55`), so a slow gen-G rebuild can publish last over gen-G+1 with the queue row gone; only the actively-applied row of a 25-row claim is heartbeated (`apply.rs:162-175`). Accept: per-key serialization + whole-batch heartbeat; concurrent-worker test.
- [ ] **P1 — Staged-replacement full rebuilds + marker hygiene.** Five families wipe-then-repopulate (permissions, children, record_inventory, resolver, primary_name), exposing empty reads and crash-truncated state that bootstrap markers never repair; per-family CLI commands skip the documented marker-clearing rule. `name_current`/`address_names` already correct. Accept: staged atomic publish everywhere; commands clear `current_projection_replay_status` first.
- [ ] **P2 — ENSv2 `ParentChanged` → `children_current` invalidation.** Emitted with `logical_name_id = None` (`ens_v2_registry/events.rs:465-487`); both derive branches miss it while the rebuild consumes it. Accept: derive key from payload registry/parent fields.
- [ ] **P2 — Tooling: range rebuild, rewind, missing inspect subcommands** (docs promise point/range/full + rewind; only point/full exist, `cli.rs:230-522`; inspect lacks surface-binding, resolver-topology, declared-vs-verified diff). Implement or doc-first descope.
- [ ] **P3 — Efficiency bundle:** claim query defeats the pending partial index; per-task children block cache never hits (`children.rs:98-108`); inconsistent NULL `log_index` tie-breaking across four families (one shared sort key); duplicated derive loops; non-transactional upsert+stale-delete in point rebuilds; cross-family starvation needs age escalation.

## WS-D · Intake resilience (owner: Intake and Adapters)

Files: `apps/indexer/**`, `crates/storage/src/backfill_jobs/**`, `crates/storage/src/lineage/**` (WS-D owns these storage files for the duration).

- [ ] **P0 — Refresh backfill leases during range execution.** 300s lease never refreshed (`crates/storage/src/backfill_jobs/lease.rs:197-213`) vs 50k-block ranges; reservation prefers stealing expired ranges → `abort_all` kills bootstrap; crash-loops any deployment where a range exceeds 300s. Accept: `advance_backfill_range` extends the lease; concurrent-bootstrap test.
- [ ] **P0 — Reorg-during-backfill re-canonicalizes orphaned blocks.** Evidence loaded once per range; `chain_lineage_contains_ancestor` ignores canonicality (`lineage/reads.rs:182-235`); lineage upsert revives `orphaned → canonical` (`lineage/upserts.rs:127-128`). Accept: per-chunk evidence reload and/or ancestry proofs reject orphaned paths; mid-backfill reorg test.
- [ ] **P1 — RPC resilience.** Zero retry in the primary client (`provider/request.rs:177-221`), batch→sequential fallback amplifies 429s; log-range bisection recognizes one provider's limit message only (`logs_receipts.rs:480-483`); providers erroring on `safe`/`finalized` tags break head-following. Accept: bounded retry/backoff (model: the Coinbase client); multi-provider limit patterns; tag errors degrade to `None`.
- [ ] **P1 — Unify the three materialization pipelines** (inline / sparse / Coinbase). Already drifted: sparse hardcodes code observations off (`fetching/sparse.rs:213`) and is the default bootstrap path → bootstrap history has no `raw_code_hashes`; live retains tx-sibling logs, backfill selected-emitter only (replay sees different fact sets per admission path). Accept: one stage implementation; identical retention/observation behavior (doc-first if intentionally narrowed).
- [ ] **P2 — Robustness bundle:** poll loop exits on transient DB errors (`runtime/poll_loop.rs:356-374`); unbounded non-checkpointed gap-fill up to 131k blocks (hand to bounded backfill machinery); no escalation past the reorg walk limit; one-block admission isn't one transaction; object-cache budget preflight hardcoded false; bootstrap serial-and-fail-fast with live intake.
- [ ] **P3 — RPC-cost bundle:** `eth_getCode` for the full watch plan per reconciled block (restrict to log-emitters); ops-catchup 32-block default chunks (~600k jobs full-history); inline mode full bundles where logs-first works; moving-tail chunk redone per follow iteration; per-row INSERT loop for event-silent observations; batch concurrency default 1.

## WS-E · Verified execution & primary names (owners: Verified Execution + Manifests and Discovery)

Files: `crates/execution/**`, `crates/manifests/**`, `manifests/**`, `apps/api/src/support/primary_name_lookup.rs` + primary-name response files (coordinate the API surface with WS-G; semantics owned here).

- [ ] **P0 — Accept `https://` RPC endpoints.** `crates/execution/src/rpc.rs:80-84` bails on non-`http`; hosted providers are https-only, so live verified resolution and the ENS/60 fallback are unusable outside a local node. Also share the `reqwest::Client`.
- [ ] **P1 — Finish the verified-primary path.** No production caller for `persist_ens_verified_primary_name`; readback selects newest by request key only, ignoring chain positions/manifest versions/boundaries (`apps/api/src/support/primary_name_lookup.rs:78-104`); claim-change invalidation hooks are dead code → stale verified answers survive claim changes. Accept: claim-change invalidation wired; readback validates full cache identity; producer implemented or class documented backfill-fed.
- [ ] **P1 — EIP-3668 conformance.** GET chosen for `{sender}`-only templates and calldata never sent; `{sender}` not substituted on POST (`ens_resolution_ccip.rs:237-249`); 4xx retried against spec; `OffchainLookup`-required reverts persisted as cacheable `execution_failed` instead of documented `unsupported`; transient transport failures frozen into the cache (`ens_resolution_call.rs:100-117`).
- [ ] **P1 — API primary-names classification.** Route coverage keys off "persisted verified outcome present" instead of tuple-class membership (`apps/api/src/responses/resolution.rs:467-507`); in-class verified misses return `unsupported` where docs say `not_found`; `coin_type` not canonicalized (`"060"` forms a distinct tuple key and disables the ENS/60 fallback — same bug in `crates/execution/src/json_helpers.rs:79-95`); ENS/60 fallback returns `not_found` for unnormalizable claims where docs require `invalid_name`.
- [ ] **P2 — Typed failure taxonomy + trace completeness.** Nearly everything collapses to `resolver_call_failed` (typed flags exist unused); `latency_ms` always null; ENS-side CCIP recorded only as digest, never as documented step rows; manifest-change cache invalidation is CLI-only — wire to manifest events.
- [ ] **P3 — Manifests polish:** version-tag vs `manifest_version` mismatch silently collides (`repository.rs:83-114`); duplicate contract roles unchecked; `normalizer_version` unvalidated; UR proxy declared `proxy_kind="none"` so upgrade-drift detection can't fire on the contract anchoring every verified answer; documented edge kinds (`parent`/`alias`/`metadata`/`transport`/discovered proxy) never produced; version bumps deactivate discovered edges until next replay; Basenames verified executor readback-only despite `supported` flag (doc-first decision).

## WS-F · Storage write-path & performance (owner: Storage and Domain)

Files: `crates/storage/src/identity/**`, trigger/index migrations. **Owns migration review for all streams.**

- [ ] **P1 — `(chain_id, block_hash)` indexes for reorg orphaning.** Orphaning UPDATEs on `normalized_events` + four identity tables have no supporting index (`normalized_events/orphaning.rs:33-46`, `identity/orphan.rs:156-183`) — the most latency-critical write path seq-scans the largest tables. Accept: indexes added `CONCURRENTLY`, or predicates constrained by block range.
- [ ] **P1 — Atomic merges on row-path identity writes.** Reload-merge-update without `FOR UPDATE` (`identity/write_rows/surface_binding.rs:71-117` + three sites in `write_rows.rs`) can re-extend tightened intervals or revive orphaned rows; bulk path already proves the merge in SQL. Accept: row paths adopt the bulk pattern or lock the reload.
- [ ] **P2 — Trigger/write-amplification bundle:** count/feed/invalidation triggers fire on read-equivalent `canonical→safe→finalized` promotions (gate on readability-class transitions); feed trigger lacks `WHEN`/`OF` (full per-address recompute on every projection rewrite, migration `20260521160000:476-479`); `projection_normalized_event_changes` has no pruning path; non-`CONCURRENTLY` index on `normalized_events` (`20260608160000`); four redundant indexes beyond the prior cleanup.
- [ ] **P2 — Repair policy: code-first.** ~14 ENSv1 repair migrations duplicate a ~150-line jsonb rewrite block, couple to `_sqlx_migrations.installed_on`/wall-clock cutoffs, three extend/reopen closed intervals against the tighten-only rule, three rewrite `event_identity` without the collision guards the Rust framework has. Accept: policy note (repairs land in the Rust framework); doc carve-out or correction for the interval rule.
- [ ] **P3 — Consistency bundle:** readable-universe predicate restated 6+ ways (call the existing SQL function everywhere); `change_kind='canonicality_update'` overloaded for payload repairs; `primary_names_current` missing documented projection-row metadata columns; execution-cache reorg invalidation loads every outcome into memory; history same-block ordering uses `transaction_hash` before `log_index`; per-event loop over a batch-capable upsert; `pg_trgm` installed but `contains` filters have no trgm index.

## WS-G · API contract & pagination (owner: Projections and API)

Files: `apps/api/**` (minus primary-name semantics → WS-E), `crates/storage/src/history.rs`.

- [ ] **P1 — SQL keyset pagination for `/v1/events` + `/v1/history/*`.** These load the entire filtered universe per request and paginate in memory (no LIMIT, `crates/storage/src/history.rs:95-244`; O(n) cursor scan) — the clearest scalability cliff. Gate `/v1/names`' unconditional `COUNT(*)` on `include=total_count` (`name_current/list.rs:187`).
- [ ] **P2 — Input-validation consistency.** Exact-name path segments skip ENSIP-15 (unnormalizable → 404, docs say 400, `handlers/exact_name.rs:3-27`); address validation differs across three route families; empty enum values inconsistent. One shared boundary, documented behavior for unnormalized input.
- [ ] **P2 — Contract polish bundle:** `meta=full` and `include=record_summaries` accepted-but-inert on names/roles (implement or disclose + doc); hardcoded coverage on `/v1/addresses/{address}/names` (`responses/collections.rs:52-58`); declared provenance emits `execution_trace_id: null` where docs say omit; `invalid_cursor` vs `invalid_input` doc inconsistency; undocumented `"stale"` from `/v1/status` + short-circuited degraded check; null-collision on unrequested compact-record sections; startup warm-up `bail!` → warning (`records_warmup.rs:31-42`).
- [ ] **P3 — Efficiency bundle:** N+1 `load_name_surface` in children compact view (`handlers/collections.rs:244-278`); O(n²) `Vec::contains` dedupe; per-request `JsonRpcHttpClient` in the primary-name fallback; sequential awaits that could join.

## WS-H · Safety net & docs truth (owners: Platform and DevEx + Conformance and Fixtures)

Files: `.github/**`, `docker-compose*`, `docker/`, `.env*.example`, `docs/**` sweep, `tests/conformance/**`. **Land first.**

- [ ] **P0 — Run the conformance suite in CI.** ~85 of ~95 consumer-contract tests gated nowhere (`tests/conformance` not a workspace member; only four filters run in smoke scripts). Accept: CI job runs `cargo test --manifest-path tests/conformance/Cargo.toml --locked` against the existing Postgres service.
- [ ] **P0 — Gate image publishing on CI; healthchecks.** `docker.yml` publishes `:latest` on any push to main with no CI dependency; production compose pulls `:latest`; no healthchecks on api/indexer/worker despite `/healthz` + curl in-image; `public-proxy` fronts an unready API.
- [ ] **P1 — Docs corrections sweep:** `docs/storage.md` partitioning + object storage describe nonexistent layers (remove dead MinIO services/env vars or implement); fuse-masking docs vs WS-C item 1 status; "TypeScript conformance harness" is Rust; conformance README describes a removed 400-requirement; `.env*.example` missing ~15 env vars the code reads; `crates/domain` ownership map vs reality (identity types live in `crates/storage/src/identity/types.rs`).
- [ ] **P2 — Test-matrix holes** (each test ideally lands with its owning stream; tracked here): fuse-changes-altering-control (WS-C), ENSv2 delegate-retained / shared-subregistry / alias-derived (WS-B), CCIP failure + offchain gateway mismatch (WS-E), Basenames primary unset (WS-E), wrapped-name route coverage (WS-G), proxy-implementation-change + manifest-version-change conformance (WS-E).
