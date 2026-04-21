# Chain Intake

Status: Phase 0 baseline

This document freezes the chain-intake contract for the shipped mainnet deployment profile and the profile-selection rule that later alternate deployments must follow.

## 1. Mental Model

- chain intake is a canonical-chain reconciliation system with a fact log attached
- subscriptions, filters, and provider notifications are latency hints only
- raw facts are append-only; canonicality and head promotion are explicit state
- block hash is identity; block number is position
- live ingestion and backfill must converge on the same raw-fact, normalized-event, and projection pipeline
- a deployment selects one chain profile at a time; mainnet and Sepolia facts do not share the same canonical corpus, checkpoints, or projection state
- the ENSv2 `sepolia-dev` profile selects `manifests-sepolia-dev/` as a whole alternate profile; it must not be loaded beside `manifests/` in the same intake runtime, watch plan, discovery graph, or projection set

## 2. Scope Boundary

Initial truth-core intake covers:

- blocks and lineage metadata
- transactions
- receipts
- logs
- code-hash observations
- block-anchored call snapshots used by verified execution or enrichment

Out of scope for the initial intake contract:

- mempool or pending-transaction indexing
- node-local txpool APIs
- client-specific trace or state-diff indexing as a correctness dependency
- historical state reconstruction from non-archive upstreams

These may exist later as separate capabilities, but they must not leak into the core correctness model for declared-state indexing.

## 3. ENSv2 Phase 5 Adapter Intake Boundary

The Phase 5 ENSv2 `sepolia-dev` intake starts from the four admitted source families `ens_v2_root_l1`, `ens_v2_registry_l1`, `ens_v2_registrar_l1`, and `ens_v2_resolver_l1` under `manifests-sepolia-dev/ens/...`. Initial direct watched roots come from the pinned upstream `sepolia-dev` deployment metadata for `RootRegistry`, `ETHRegistry`, and `ETHRegistrar`; `PermissionedResolverImpl` is implementation metadata for discovered or admitted resolver instances, and resolver instances enter the watch plan only through manifest admission or discovery edges (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/PermissionedResolverImpl.json:L2 @ ens_v2@554c309).

ENSv2 adapters normalize log-derived facts after raw block admission:

- upstream `TokenResource(tokenId, resource)` becomes `TokenResourceLinked`; upstream `TokenRegenerated(oldTokenId, newTokenId)` becomes `TokenRegenerated` and must not be treated as a new resource by intake or projection workers (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309).
- upstream `SubregistryUpdated`, `ResolverUpdated`, and `ParentUpdated` become graph and topology events after their endpoint addresses resolve to current `contract_instance_id` values for the selected profile (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L59 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309).
- upstream `AliasChanged` becomes `AliasChanged` on admitted resolver instances, and upstream `EACRolesChanged` becomes resource-, root-, or resolver-scoped Permission events after the adapter resolves the upstream EAC resource to bigname identity (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309).

Any ENSv2 enrichment call used to repair or disambiguate a log-derived fact, such as `getResource(anyId)`, `getTokenId(anyId)`, `getState(anyId)`, `getAlias(fromName)`, or EAC role reads, must be anchored to the same block identity as the raw log. The upstream interfaces expose these reads, but the intake correctness model remains hash-first and log-derived state must not be rewritten through ambiguous number-only calls (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L57 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L67 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L72 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L56 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L100 @ ens_v2@554c309).

## 4. Upstream Requirements

For each chain source in the selected deployment profile, the intake plane must have access to:

- block fetch by hash
- block fetch by number or canonical tag
- log fetch by exact block identity
- receipt fetch for a whole block when the upstream supports it, or a bounded fallback path
- code and call reads at pinned chain positions
- safe and finalized head visibility

Rules:

- production correctness depends on `safe` and `finalized` support; sources that cannot surface those checkpoints are bootstrap or shadow sources only
- if the platform self-hosts on post-Merge Ethereum, it must operate an execution client and a consensus client together
- historical state-heavy enrichment and state rewrites require archive-capable upstreams or a separately retained local corpus
- upstream history retention must be treated as bounded; intake must retain its own raw corpus for deterministic replay and rewrites

## 5. Head Model And Recent Window

Per chain, intake tracks these persisted checkpoints:

- `canonical_head`
- `safe_head`
- `finalized_head`

API consistency maps onto them directly:

- `consistency=head` reads from the current canonical head
- `consistency=safe` reads from the safe checkpoint
- `consistency=finalized` reads from the finalized checkpoint

The intake plane also keeps a recent reconciled window keyed by `(chain_id, block_hash)` with at least:

- `parent_hash`
- `block_number`
- `timestamp`
- `logs_bloom`
- `transactions_root`
- `receipts_root`
- `state_root` when the upstream exposes it

This window exists to:

- detect parent mismatch immediately
- walk back to a common ancestor on reorg
- backfill short parent gaps
- answer recent canonicality disputes and audits

Number-to-hash mappings inside this window are derived views only. The primary key is always block hash.

## 6. Block Identity And Storage Rules

Lineage and raw facts must preserve enough information to rebuild canonicality without re-scraping chain history.

Rules:

- block hash is the identity anchor for every block-scoped object
- `parent_hash` is required in lineage storage
- every raw fact row that comes from chain data carries `chain_id`, `block_number`, and `block_hash`
- caches are keyed by block hash first; block number may be used only as a secondary lookup or pagination aid
- if a downstream key needs "current block number," it must resolve that number to a block hash before reading block-scoped data

## 7. Notification And Fetch Contract

Subscriptions, filters, and polling are allowed only as low-latency triggers.

They must not be treated as durable truth because:

- subscriptions are tied to a live connection
- filters are node-side state and may expire
- duplicate heights and replayed logs can happen during reorgs
- connection loss cannot imply data loss or canonical confirmation

The live path is:

1. receive a head notification from polling or subscription
2. fetch the referenced block or header by hash when possible
3. reconcile `parent_hash` against the recent window
4. fetch exact block-scoped data
5. persist one block admission unit atomically
6. advance canonical, safe, and finalized checkpoints only after reconciliation

For exact block-scoped data:

- logs must be fetched by `blockHash`, not just block number, whenever the upstream supports it
- receipts should be fetched block-scoped first; transaction-by-transaction receipt fan-out is a fallback, not the preferred primitive
- live ingestion must not rely on subscription payloads alone as the persisted source of truth

## 8. Backfill Contract

Backfill may use either:

- logs-centric range scans
- block-centric receipt or block scans

Backfill is scheduled as persisted, bounded jobs. A job is scoped to one selected deployment profile, chain, source selector, scan mode, and explicit block range. The source selector mode is `whole_active_watched_chain` by default when no selector is supplied, `source_family`, or an explicit `watched_target_set`. The job range must be finite at creation time; open-ended tail following remains live intake, not a backfill job.

Backfill jobs use a bounded lifecycle:

- `pending`: the job or range exists but no worker currently owns it
- `reserved`: a worker has a lease for the next bounded range checkpoint
- `running`: the reserved worker is advancing the range checkpoint through the shared intake path
- `completed`: every range checkpoint for the job reached its declared end
- `failed`: the job or range stopped with recorded failure metadata and can be retried by creating or reserving explicit remaining work

The resumable backfill runner command is indexer/backfill-owned operational tooling exposed through `bigname-indexer backfill` over this persisted job model. Each invocation supplies or reuses an idempotency key for one immutable job shape: selected deployment profile, chain, source selector, scan mode, finite range start, and finite range end. If the idempotency key already names that exact job shape, the command reuses the existing job and ranges. If the same key is presented with a different job shape, the command must fail with an explicit conflict instead of widening the range, changing source identity, resetting checkpoints, or reclassifying already admitted facts.

The source-scoped backfill runner selector has three mutually exclusive modes:

- `whole_active_watched_chain`: the default when no source selector is supplied. The selected targets are every active watched target for the selected deployment profile and chain whose active watch range intersects the finite job range at job creation time.
- `source_family`: selected by `--source-family <family>`. The selected targets are only the active watched targets in that source family for the selected deployment profile and chain whose active watch range intersects the finite job range. Unknown families or families with no matching active targets fail before job creation rather than falling back to whole-chain backfill.
- `watched_target_set`: selected by an explicit watched-target set. The request identifies watched targets by `contract_instance_id`; raw addresses alone are not accepted as durable target identity. The selected targets are exactly the supplied watched target identities after validation against the selected deployment profile, chain, and finite job range. The runner must not expand an explicit set to sibling targets, other targets in the same source family, or the whole active watch plan.

The persisted source identity for any selector is the resolved target set, not the CLI spelling that produced it. It is stable and sorted by `source_family`, `contract_instance_id`, normalized address, effective target range start, and effective target range end. Duplicate target identities must collapse only when the full canonical target tuple matches; if the same selector resolves conflicting metadata for the same target identity, job creation fails with an explicit source identity conflict. For idempotency-key reuse, the runner compares the persisted selector mode and resolved source identity. If the active watch plan has changed such that the same CLI selector now resolves to a different target set, the same idempotency key conflicts instead of mutating the existing job.

Backfill intake for a source-scoped job is selected-target-only and hash-pinned. The runner may use block-number ranges to enumerate candidate blocks, but every persisted block-scoped fact or enrichment must be anchored to the resolved block hash before admission through the shared intake path. The job may persist minimal lineage/header anchors needed for that hash pinning, but target-scoped log admission, call snapshots, normalized events, and downstream projection invalidation must be limited to the selected targets. A source-scoped job must not opportunistically admit unselected watched targets merely because they appear in the same block, receipt batch, source family, or chain range.

Storage helpers own lifecycle mutation. They must be idempotent:

- `create_backfill_job` inserts a new bounded job or returns the existing job for the same idempotency key and immutable job shape without widening or narrowing its range, changing source identity, or replacing child range bounds
- `reserve_backfill_range` atomically claims pending or reclaimable work with a lease owner, lease token, and lease expiry; duplicate calls by the same active lease holder return the same reservation, and expired leases can be reclaimed without duplicating range work
- `advance_backfill_range` requires the current lease and moves the persisted range checkpoint forward monotonically, never below the prior checkpoint and never beyond the declared range end
- `complete_backfill_range` and `complete_backfill_job` are no-ops when already complete and must require all child range checkpoints to reach their declared ends
- `fail_backfill_range` and `fail_backfill_job` record bounded failure state and failure metadata without rewinding completed checkpoints, clearing completed ranges, or mutating raw facts

Range checkpoints are owned by the backfill job substrate. They record operational progress for fetch/resume only and must not be reused as chain checkpoints, projection replay checkpoints, or API consistency checkpoints. The runner must not call chain checkpoint advancement as a side effect of creating, reserving, advancing, completing, failing, or reusing a backfill job, regardless of whether the selector is whole-chain, source-family scoped, or an explicit watched-target set.

Rules:

- backfill and live ingestion share the same downstream normalization and projection path after raw fetch
- receipt-rich indexing should prefer block-scoped receipt ingestion when available
- backfill jobs must be resumable, idempotent, and bounded by explicit checkpoints
- backfill completion is not proof of finality; canonical, safe, and finalized promotion still follow the lineage model
- backfill job and range checkpoint updates must not mutate or promote `canonical_head`, `safe_head`, or `finalized_head`

## 9. Batch And Retry Rules

Batching is allowed only for independent work.

Good batch targets:

- many block fetches for historical backfill
- many exact block-scoped log fetches
- many receipt lookups inside a bounded fallback path
- many code-hash or ABI lookups

Rules:

- later pipeline stages must not assume earlier batched results are canonical until reconciliation finishes
- every batch item must be retryable independently
- partial batch failure must not corrupt intake ordering
- batch size must stay bounded and measurable

## 10. State Enrichment Rules

If intake or execution enriches facts with state reads such as calls, storage, or balances:

- anchor the read to the exact block hash whenever the RPC surface supports it
- otherwise treat the enriched result as provisional until the source block is at least `safe`
- never attach number-based enrichment to a block-scoped fact as though it were reorg-proof

Historical state-heavy enrichment is an archive requirement, not a best-effort full-node feature.

## 11. Reconciliation Algorithm

Reorg handling is an explicit unwind and replay algorithm.

For each candidate canonical block:

1. if the block is already known, update checkpoint promotion state only
2. if `parent_hash` matches the current canonical head, append it
3. if the parent is missing, backfill parents until continuity or an existing checkpoint is reached
4. if the parent conflicts with the current canonical head, walk backward through the recent window to a common ancestor
5. mark the losing branch as `orphaned`
6. emit deterministic invalidation for normalized events and `execution_cache_outcomes` rows derived from orphaned block identities
7. admit the winning branch in canonical order
8. move the canonical head pointer last
9. promote blocks under the safe and finalized checkpoints asynchronously and monotonically

Reconciliation must never depend on ad hoc deletes or "latest row wins" semantics.

Execution-cache invalidation emitted by reorg repair is hash-scoped. It invalidates `execution_cache_outcomes` rows for verified resolution and verified primary-name outcomes when their dependency set contains an orphaned `(chain_id, block_hash)` or a boundary resolved through one. It must not delete execution traces, execution steps, raw facts, or normalized events; those remain durable replay and audit inputs.

Cache dependencies must be tied to explicit block-hash-bearing chain positions or boundaries before a verified outcome can be treated as reorg-safe. Number-only, tag-only, or dependency-free verified resolution and verified primary-name rows fail closed and cannot be served from cache after a reorg check; rows for request types explicitly documented outside this Phase 9 invalidation surface remain out of scope. This reorg/replay foundation does not promote ENSv2 exact-name support or any manifest capability.

## 12. Raw-Fact Normalized-Event Replay Runner

Raw-fact normalized-event replay is bounded operational tooling over already persisted canonical raw facts. A replay request selects a finite deployment profile, chain, and block range or explicit block-hash set. For selected blocks, canonical raw facts are rows whose block identity is `canonical`, `safe`, or `finalized`; `observed` and `orphaned` facts are excluded unless a later audit-only contract explicitly admits them.

The raw-fact normalized-event replay runner performs an upsert-only adapter resync by invoking the same adapter-owned `normalized_events` boundary used after live or backfill raw admission. It must read persisted raw facts, lineage state, and the already persisted manifest/source identity needed to route those facts. It must not perform RPC fetches, re-open live intake, create or reserve backfill ranges, advance backfill range checkpoints, mutate backfill jobs, promote `canonical_head`, `safe_head`, or `finalized_head`, rebuild projections, write public API state, or expose a public `v1` route.

Replay does not delete stale `normalized_events`, purge rows derived from selected blocks, or replace existing payloads for an already persisted normalized-event identity. Existing normalized-event identities can only be refreshed through the storage upsert canonicality path; stale conflicting payloads remain a hard storage mismatch rather than being rewritten by replay. Raw facts and lineage remain immutable, projection rebuild remains downstream worker-owned, and API responses continue to read projections and execution output rather than the replay runner.

## 13. Atomicity Boundary

The raw admission transaction boundary is one block.

That transaction writes:

- lineage rows for the admitted block
- raw block, transaction, receipt, and log facts
- any block-scoped call snapshots captured in intake
- normalized events emitted from those facts
- invalidation signals required by downstream workers

The canonical head pointer is written last inside that admission unit.

Projection workers remain downstream and asynchronous, but they must consume deterministic block-scoped invalidation and replay inputs so that reorg repair is reproducible.

## 14. Traces, Pending, And Other Optional Capabilities

Pending and mempool indexing are a separate product surface.

Trace and internal-call indexing are a separate capability plane because they depend on non-standard, client-specific APIs and different operational budgets.

Rules:

- the declared-state truth core must not require traces to be correct
- if traces are enabled later, they persist as their own raw facts with the same block-hash anchoring and reorg semantics
- intake planning must not assume all providers expose the same trace APIs

## 15. Observability And Test Requirements

Minimum chain-intake metrics:

- lag to canonical, safe, and finalized heads
- reorg depth histogram
- orphaned block rate
- RPC latency and error rate by method
- partial batch failure rate
- recent-window cache hit and miss rate
- backlog depth
- replay and rewrite duration
- raw-fact normalized-event replay duration and selected canonical block count

Required failure drills:

- dropped subscription connection during a reorg
- duplicate headers at the same height
- missing parent gap that requires parent backfill
- partial batch failures
- crash and resume from a persisted checkpoint
- crash and resume from a persisted backfill job range checkpoint
- raw-fact normalized-event replay restart over the same bounded canonical selection as an upsert-only adapter resync without RPC fetch or checkpoint promotion
- safe or finalized promotion lagging canonical intake

## 16. Acceptance Rules

The intake contract is acceptable for the first implementation milestone only if:

- live notifications can be lost without losing correctness
- the system can reconcile short forks by hash and parent hash alone
- block-scoped data ingestion never depends on ambiguous number-only reads when a hash-scoped primitive exists
- raw facts are sufficient to rebuild canonical declared state after a reorg or decoder rewrite
- backfill reuses the same downstream semantics as live ingestion
- raw-fact normalized-event replay upserts normalized events only from persisted canonical raw facts without payload replacement, stale-row purge, RPC fetch, projection rebuild, public API exposure, or chain/backfill checkpoint mutation
