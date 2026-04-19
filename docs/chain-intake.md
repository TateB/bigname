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

## 3. Upstream Requirements

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

## 4. Head Model And Recent Window

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

## 5. Block Identity And Storage Rules

Lineage and raw facts must preserve enough information to rebuild canonicality without re-scraping chain history.

Rules:

- block hash is the identity anchor for every block-scoped object
- `parent_hash` is required in lineage storage
- every raw fact row that comes from chain data carries `chain_id`, `block_number`, and `block_hash`
- caches are keyed by block hash first; block number may be used only as a secondary lookup or pagination aid
- if a downstream key needs "current block number," it must resolve that number to a block hash before reading block-scoped data

## 6. Notification And Fetch Contract

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

## 7. Backfill Contract

Backfill may use either:

- logs-centric range scans
- block-centric receipt or block scans

Rules:

- backfill and live ingestion share the same downstream normalization and projection path after raw fetch
- receipt-rich indexing should prefer block-scoped receipt ingestion when available
- backfill jobs must be resumable, idempotent, and bounded by explicit checkpoints
- backfill completion is not proof of finality; canonical, safe, and finalized promotion still follow the lineage model

## 8. Batch And Retry Rules

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

## 9. State Enrichment Rules

If intake or execution enriches facts with state reads such as calls, storage, or balances:

- anchor the read to the exact block hash whenever the RPC surface supports it
- otherwise treat the enriched result as provisional until the source block is at least `safe`
- never attach number-based enrichment to a block-scoped fact as though it were reorg-proof

Historical state-heavy enrichment is an archive requirement, not a best-effort full-node feature.

## 10. Reconciliation Algorithm

Reorg handling is an explicit unwind and replay algorithm.

For each candidate canonical block:

1. if the block is already known, update checkpoint promotion state only
2. if `parent_hash` matches the current canonical head, append it
3. if the parent is missing, backfill parents until continuity or an existing checkpoint is reached
4. if the parent conflicts with the current canonical head, walk backward through the recent window to a common ancestor
5. mark the losing branch as `orphaned`
6. emit deterministic invalidation for normalized events and execution cache entries derived from orphaned blocks
7. admit the winning branch in canonical order
8. move the canonical head pointer last
9. promote blocks under the safe and finalized checkpoints asynchronously and monotonically

Reconciliation must never depend on ad hoc deletes or "latest row wins" semantics.

## 11. Atomicity Boundary

The raw admission transaction boundary is one block.

That transaction writes:

- lineage rows for the admitted block
- raw block, transaction, receipt, and log facts
- any block-scoped call snapshots captured in intake
- normalized events emitted from those facts
- invalidation signals required by downstream workers

The canonical head pointer is written last inside that admission unit.

Projection workers remain downstream and asynchronous, but they must consume deterministic block-scoped invalidation and replay inputs so that reorg repair is reproducible.

## 12. Traces, Pending, And Other Optional Capabilities

Pending and mempool indexing are a separate product surface.

Trace and internal-call indexing are a separate capability plane because they depend on non-standard, client-specific APIs and different operational budgets.

Rules:

- the declared-state truth core must not require traces to be correct
- if traces are enabled later, they persist as their own raw facts with the same block-hash anchoring and reorg semantics
- intake planning must not assume all providers expose the same trace APIs

## 13. Observability And Test Requirements

Minimum chain-intake metrics:

- lag to canonical, safe, and finalized heads
- reorg depth histogram
- orphaned block rate
- RPC latency and error rate by method
- partial batch failure rate
- recent-window cache hit and miss rate
- backlog depth
- replay and rewrite duration

Required failure drills:

- dropped subscription connection during a reorg
- duplicate headers at the same height
- missing parent gap that requires parent backfill
- partial batch failures
- crash and resume from a persisted checkpoint
- safe or finalized promotion lagging canonical intake

## 14. Acceptance Rules

The intake contract is acceptable for the first implementation milestone only if:

- live notifications can be lost without losing correctness
- the system can reconcile short forks by hash and parent hash alone
- block-scoped data ingestion never depends on ambiguous number-only reads when a hash-scoped primitive exists
- raw facts are sufficient to rebuild canonical declared state after a reorg or decoder rewrite
- backfill reuses the same downstream semantics as live ingestion
