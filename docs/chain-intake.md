# Chain Intake

Status: Phase 0 baseline

This document freezes the chain-intake contract for the shipped mainnet deployment profile and the profile-selection rule that later alternate deployments must follow.

## 1. Mental Model

- chain intake is a canonical-chain reconciliation system with a fact log attached
- subscriptions, filters, and provider notifications are latency hints only
- raw facts are append-only; canonicality and head promotion are explicit state
- Postgres is the hot indexed/replay store for replay-critical facts and payload-cache metadata, not the archive for every fetched block-scoped body
- block hash is identity; block number is position
- live ingestion and backfill must converge on the same raw-fact, normalized-event, and projection pipeline
- a deployment selects one chain profile at a time; mainnet and Sepolia facts do not share the same canonical corpus, checkpoints, or projection state
- the ENSv2 `sepolia-dev` profile selects `manifests-sepolia-dev/` as a whole alternate profile; it must not be loaded beside `manifests/` in the same intake runtime, watch plan, discovery graph, or projection set

## 2. Scope Boundary

Initial truth-core intake covers durable replay facts and cache metadata for:

- blocks and lineage metadata
- selected/admitted target logs
- transaction, receipt, and block fields needed to decode selected logs or rebuild retained normalized events and execution outputs
- code-hash observations
- block-anchored call snapshots used by verified execution or enrichment
- optional cache metadata or digests for large/full block, transaction, or receipt bodies fetched outside the hot replay set; hash-addressed cold pointers are required only for payload classes explicitly declared durable

Exact block-anchored `raw_call_snapshots` remain intake-owned raw facts even when verified execution supplied the candidate request/response pair. The admitted handoff is narrow: execution may hand off only snapshots anchored to the resolved requested chain position and only for a persistence path that already admits those snapshots. That handoff does not create a general execution-owned raw-fact write surface.

Out of scope for the initial intake contract:

- mempool or pending-transaction indexing
- node-local txpool APIs
- client-specific trace or state-diff indexing as a correctness dependency
- historical state reconstruction from non-archive upstreams

These may exist later as separate capabilities, but they must not leak into the core correctness model for declared-state indexing.

## 3. ENSv1 And Basenames Resolver Discovery Boundary

ENSv1 old-registry intake is migration-aware historical admission, not a second current registry stream. If `ENSRegistryOld` is admitted, it stays under `ens_v1_registry_l1` as an allow-listed migration-epoch input at `0x314159265dd8dbb310642f98f50c066173c1259b` with `start_block = 3327417` from the pinned subgraph. The current registry `startBlock: 9380380` remains only the current registry's pinned-subgraph start, not original ENS history (upstream: .refs/ens_subgraph/subgraph.yaml:L10 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L42 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L44 @ ens_subgraph@723f1b6).

Old-registry raw facts must retain their emitter identity and pass a migration guard before they can normalize into current topology. A current-registry `NewOwner` marks the affected subnode migrated; once migrated, later old-registry `NewOwner`, `Transfer`, `NewTTL`, and non-root `NewResolver` observations for that node are retained as facts but must not overwrite the current owner, resolver, TTL, child edge, resolver-discovery edge, or projection input. The root resolver is the only exception: old-registry `NewResolver(ROOT_NODE, resolver)` may still update the root resolver binding and feed `ens_v1_resolver_l1` discovery. The pinned subgraph's old-registry handlers encode the same migrated-node guard and root resolver exception (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L134 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L230 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L252 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L259 @ ens_subgraph@723f1b6).

ENSv1 and Basenames declared record indexing must not stop at the statically admitted resolver deployments. For the shipped mainnet profile, registry-level resolver changes are discovery inputs:

- ENSv1 registry `NewResolver(node, resolver)` logs from admitted `ens_v1_registry_l1` emitters must produce resolver discovery observations for `ens_v1_resolver_l1`; nonzero resolver addresses create or refresh node-to-resolver bindings and resolver contract instances, while zero-address resolver changes close only the affected node-to-resolver binding (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f).
- Basenames registry `NewResolver(node, resolver)` logs from admitted `basenames_base_registry` emitters must produce resolver discovery observations for `basenames_base_resolver`; nonzero Base-side resolver addresses create or refresh node-to-resolver bindings and resolver contract instances, while zero-address resolver changes close only the affected node-to-resolver binding (upstream: .refs/basenames/src/L2/Registry.sol:L19 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc).

The resolver address observed in declared topology is therefore not sufficient by itself. Contract-instance admission, node-to-resolver binding state, generic event intake, and supported resolver-profile admission are separate. For ENSv1, retained generic resolver-local record/version events such as `AddrChanged`, `AddressChanged`, `TextChanged`, and `VersionChanged` may feed observed selector/cache and version-boundary facts when the emitter and node match the selected resolver binding; unobserved selectors remain explicit gaps or `resolver_family_pending` rather than silently becoming absent or complete. Generic resolver-topic intake is topic-first and may encounter non-ENS emitters or incompatible event declarations; a raw log whose payload cannot ABI-decode to the upstream resolver event shape is retained but must not emit an observed selector/cache or version-boundary fact (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L10 @ ens_v1@91c966f). Resolver-profile admission is still required before bigname claims complete record-family coverage, resolver overview completeness, resolver-local authorization semantics, latest-only behavior, or event-to-onchain-call parity (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IVersionableResolver.sol:L5 @ ens_v1@91c966f). Basenames resolver-local record, permission, alias, and resolver-overview facts remain governed by the separate `L2Resolver`-compatible profile gate.

For ENSv1, the first dynamic resolver-profile admission is limited to explicit ENS Labs PublicResolver-generation profiles for the relevant complete fact families. The profile gate may use direct manifest admission, first-party known-resolver admission, stored code-hash observations, proxy / implementation edges, or another explicit non-schema admission rule, but registry `NewResolver` observation alone is not enough. Unknown dynamic ENSv1 resolvers keep explicit `pending` or `unsupported` resolver-profile state for profile-gated behavior, and older admitted generations expose only the families listed for their profile. PublicResolver-generation compatibility is anchored to the upstream PublicResolver profile mixins, ERC165 support, and ResolverBase record-versioning (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L21 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L22 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L23 @ ens_v1@91c966f). This ENSv1 profile admission does not widen Basenames resolver-profile support (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc).

## 4. ENSv2 Phase 5 Adapter Intake Boundary

The Phase 5 ENSv2 `sepolia-dev` intake starts from the four admitted source families `ens_v2_root_l1`, `ens_v2_registry_l1`, `ens_v2_registrar_l1`, and `ens_v2_resolver_l1` under `manifests-sepolia-dev/ens/...`. Initial direct watched roots come from the pinned upstream `sepolia-dev` deployment metadata for `RootRegistry`, `ETHRegistry`, and `ETHRegistrar`; `PermissionedResolverImpl` is implementation metadata for discovered or admitted resolver instances, and resolver instances enter the watch plan only through manifest admission or discovery edges (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/PermissionedResolverImpl.json:L2 @ ens_v2@554c309).

ENSv2 adapters normalize log-derived facts after raw block admission:

- upstream `TokenResource(tokenId, resource)` becomes `TokenResourceLinked`; upstream `TokenRegenerated(oldTokenId, newTokenId)` becomes `TokenRegenerated` and must not be treated as a new resource by intake or projection workers (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309).
- upstream `SubregistryUpdated`, `ResolverUpdated`, and `ParentUpdated` become graph and topology events after their endpoint addresses resolve to current `contract_instance_id` values for the selected profile (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L59 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309).
- upstream `AliasChanged` becomes `AliasChanged` on admitted resolver instances, and upstream `EACRolesChanged` becomes resource-, root-, or resolver-scoped Permission events after the adapter resolves the upstream EAC resource to bigname identity (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309).

Any ENSv2 enrichment call used to repair or disambiguate a log-derived fact, such as `getResource(anyId)`, `getTokenId(anyId)`, `getState(anyId)`, `getAlias(fromName)`, or EAC role reads, must be anchored to the same block identity as the raw log. The upstream interfaces expose these reads, but the intake correctness model remains hash-first and log-derived state must not be rewritten through ambiguous number-only calls (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L57 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L67 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L72 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L56 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L100 @ ens_v2@554c309).

## 5. Upstream Requirements

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
- historical state-heavy enrichment and state rewrites require archive-capable upstreams, a separately retained durable replay corpus, or explicit fail-closed behavior when the relevant cache-fill path cannot satisfy its block-hash-scoped fetch and retained-digest checks
- upstream history retention must be treated as bounded; intake must retain its own durable hot replay facts for deterministic replay and treat provider re-fetch as a cache-fill path, not as a substitute for selected replay facts

### Local Runtime Provider Configuration

The local `bigname-indexer run` command selects one manifest root with
`BIGNAME_INDEXER_MANIFESTS_ROOT` and reads provider sources from
`BIGNAME_INDEXER_CHAIN_RPC_URLS` and
`BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES`. Each setting is a comma-delimited list
of `<chain>=<value>` entries, and each chain name must match an active watched
chain produced by the selected manifest/watch state. JSON-RPC values are
provider URLs. Reth DB values are local Reth data directories for deployments
that have a same-host Reth database/static-file store. The checked-in local
default selects `manifests`; the ENSv2 Sepolia dev profile is selected by
setting `BIGNAME_INDEXER_MANIFESTS_ROOT=manifests-sepolia-dev`.

Header audit retention is an explicit operational mode. By default,
`bigname-indexer run`, `backfill`, and `ops-catchup` persist minimal block
anchors only: block hash, parent hash, number, timestamp, and canonicality
state. Passing `--retain-header-audit-fields` or setting
`BIGNAME_INDEXER_RETAIN_HEADER_AUDIT_FIELDS=true` retains nullable
`logs_bloom`, `transactions_root`, `receipts_root`, and `state_root` when the
provider returns them. Minimal replay may coexist with those retained fields
without clearing them; auditable replay may fill a previously minimal row, and
conflicting non-null audit fields remain an identity mismatch.

At most one provider source may be configured for a chain. JSON-RPC remains the
portable source. A Reth DB source is an optional intake source, not a protocol
adapter: ENS/Basenames adapters still consume bigname raw facts and append
adapter-owned normalized events. The checked-in Reth DB reader currently targets
Ethereum Mainnet Reth datadirs; other chains fail closed until they have a
chain-specific reader. A Reth-backed reader must satisfy the same
block-hash-first provider contract as JSON-RPC for heads, exact block payloads,
selected logs, receipts, and block-anchored code/state observations. It must
fail closed when the local Reth store is unavailable, pruned, inconsistent with
the selected chain, or cannot surface the requested safe/finalized checkpoint or
exact historical payload. Live execution, trace production, and fresh API-time
reads remain outside this source boundary.

The provider source list is an operational input, not a manifest admission rule.
An unset provider source list leaves manifest sync, watch-plan rebuild, and
checkpoint row creation available, but provider-backed head fetch and live
ingestion remain idle for every active watched chain. A selected manifest root
may therefore declare active watched chains that are fully synchronized into
manifest, discovery, watch-plan, and checkpoint setup while automatic bootstrap
and live provider work stay idle until that chain has a configured provider
source. Current bootstrap JSON-RPC support accepts `http://` endpoints only.

Local Reth database row keys, static-file offsets, or table handles must not
replace bigname raw fact identities. They may be retained only as operational
source metadata or evictable cache metadata. Durable selected logs,
transactions, receipts, block/header anchors, code-hash observations, and
normalized-event `raw_fact_ref` values remain keyed by bigname's chain/block/log
identity and live in Postgres according to the storage contract.

Provider availability is evaluated per selected profile and per active watched
chain. A Base provider source is not a global startup prerequisite: an
Ethereum-only profile must start without a Base provider, and a profile whose
Base chain has no configured provider must leave Base provider-backed intake,
automatic bootstrap, backfill catch-up, and live head following idle with an
explicit operational `unavailable` / `no_provider` reason rather than failing
startup for other configured chains. A configured provider for a chain outside
the selected profile remains invalid because the runtime must not ingest across
profiles.

## 6. Head Model And Recent Window

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

## 7. Block Identity And Storage Rules

Lineage and raw facts must preserve enough information to rebuild canonicality without re-scraping chain history.

Rules:

- block hash is the identity anchor for every block-scoped object
- `parent_hash` is required in lineage storage
- lineage ancestry repair requires only block hash, parent hash, block number,
  timestamp, and canonicality state. Header audit fields such as logs bloom,
  transaction root, receipt root, and state root are retained only in an
  explicit auditable-header mode; otherwise they are nullable and omitted from
  the hot sparse-backfill path.
- every raw fact row that comes from chain data carries `chain_id`, `block_number`, and `block_hash`
- full block, transaction, and receipt payloads may be fetched during live indexing, but Postgres retains only replay-critical hot facts and optional cache metadata for non-critical full bodies
- cache metadata should be stable enough to explain which payload was fetched; any metadata that may authorize later byte use must include a retained digest, and that digest must be verified before a cached, object-backed, or provider-refetched payload is used
- caches are keyed by block hash first; block number may be used only as a secondary lookup or pagination aid
- if a downstream key needs "current block number," it must resolve that number to a block hash before reading block-scoped data

## 8. Notification And Fetch Contract

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

- logs must be fetched by `blockHash`, not just block number, for exact block-scoped ingestion; providers that cannot support that contract are not acceptable for that path
- receipts should be fetched block-scoped first; transaction-by-transaction receipt fan-out is a fallback, not the preferred primitive
- live ingestion must not rely on subscription payloads alone as the persisted source of truth
- live ingestion may fetch full block-scoped payloads to derive selected facts, but the persisted Postgres admission unit should keep only replay-critical hot rows and optional cache metadata for non-critical full payloads

## 9. Backfill Contract

Backfill may use either:

- logs-centric range scans
- block-centric receipt or block scans

Backfill is scheduled as persisted, bounded jobs. A job is scoped to one selected deployment profile, chain, source selector, scan mode, and explicit block range. The source selector mode is `whole_active_watched_chain` by default when no selector is supplied, `source_family`, or an explicit `watched_target_set`. The job range must be finite at creation time; open-ended tail following remains live intake, not a backfill job.

Full historical backfill means covering the entire admitted history for the
selected deployment profile, chain, and selected targets. The start is the
manifest/discovery admitted start for each selected target, not an arbitrary
recent window. A recent chunk, a startup bootstrap range, or a partially
completed source-family conformance run is not complete history and must not be
used as consumer-replacement or route-coverage evidence.

### Automatic Bootstrap Backfill

`phase9-indexer-run-auto-backfill-bootstrap` is a shared-interface,
doc-first Phase 9 contract for `bigname-indexer run`. Automatic bootstrap is
allowed to create historical backfill work from the selected manifest root and
materialized watch plan, but only as finite persisted backfill jobs. It must not
run an implicit unbounded scanner, tail follower, or address-only fetch path.

Automatic bootstrap follows these rules:

- it runs after manifest sync, discovery admission, watch-plan materialization, and per-chain checkpoint row setup for the selected deployment profile
- active watched chains without configured providers remain idle after that setup; bootstrap must not create jobs for a chain whose provider cannot supply a finite bootstrap end
- automatic bootstrap covers each eligible target from its manifest/discovery admitted start through the finite provider head observed at job creation time. It must not cap the start to an arbitrary recent window; full admitted history is the default startup work for configured chains.
- each candidate target is the resolved watched target keyed by `contract_instance_id`, source family, chain, normalized address, and effective range; raw address is never accepted as durable source identity
- automatic bootstrap groups all eligible targets whose finite ranges overlap into the same raw-fact job segment by default. It must not run one full-chain pass per source family merely because source-scoped repair exists. Source-scoped jobs remain an explicit operational targeting mode for repair, conformance, or manual backfill.
- automatic bootstrap may partition a finite job segment into multiple contiguous child `backfill_ranges` for internal worker leases. The child ranges preserve the same immutable job source identity, declared job bounds, and raw-fact ownership; partitioning does not create source-family-specific jobs, widen coverage, promote checkpoints, or change replay/projection ownership.
- the persisted source identity for an automatically created job is the sorted resolved target set with effective range start and effective range end, matching the same canonical target tuple used by source-scoped backfill; very large selected target sets may use the compact digest form described below instead of embedding every target tuple in one JSONB value. If the segment includes ENSv1 generic resolver-event intake, the source identity must also carry `generic_topic_scans` for `ens_v1_resolver_l1`; resolver profile/admission addresses are not the address filter for those generic event facts.
- a target with declared `start_block` is eligible only from that inclusive block, further narrowed by its active watch range and the finite bootstrap range end resolved at job creation time
- a target with omitted `start_block` has unknown historical start and must be skipped explicitly with that reason; bootstrap must not infer the target start from block zero, the current job range start, manifest activation, provider history, or any default range
- every created job must have finite declared range start and finite declared range end before insertion into `backfill_jobs`; open-ended historical catch-up remains live intake or a later explicit job, not automatic bootstrap
- creating, reusing, reserving, advancing, completing, or failing an automatic bootstrap job follows the same backfill lifecycle as manual jobs and must not mutate or promote `canonical_head`, `safe_head`, or `finalized_head`

Automatic bootstrap is operational intake readiness only. It does not add or
widen public API routes, route-level coverage, manifest capability flags,
additional ENSv2 profile support, or consumer-replacement meaning.

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

The persisted source identity for any selector is the resolved target set, not the CLI spelling that produced it. It is stable and sorted by `source_family`, `contract_instance_id`, normalized address, effective target range start, and effective target range end. Duplicate target identities must collapse only when the full canonical target tuple matches; if the same selector resolves conflicting metadata for the same target identity, job creation fails with an explicit source identity conflict. For idempotency-key reuse, the runner compares the persisted selector mode and resolved source identity. If the active watch plan has changed such that the same CLI selector now resolves to a different target set, the same idempotency key conflicts instead of mutating the existing job. When a selected target set is too large to retain safely as one JSONB payload, the persisted identity may use `source_identity_payload_format=selected_targets_digest_v1`: selector fields, requested target identities, selected target count, a digest algorithm, a digest of the sorted selected target tuples, a first/last target audit sample, and `source_identity_hash`. The sorted canonical target tuple remains the digest input; the runner does not downgrade to raw-address identity or make the selector mutable.

Backfill intake for a source-scoped job is selected-target-only and block-hash-scoped. The runner may use block-number ranges to enumerate candidate blocks, but every persisted block-scoped fact or enrichment must be anchored to the resolved block hash before admission through the shared intake path. The job may persist minimal lineage/header anchors needed for that block-hash-scoped admission, but target-scoped log admission, call snapshots, normalized events, and downstream projection invalidation must be limited to the selected targets. A source-scoped job must not opportunistically admit unselected watched targets merely because they appear in the same block, receipt batch, source family, or chain range. This selected-target-only rule applies to explicit repair jobs; automatic full bootstrap uses the combined segment rule above.

For ENSv1 resolver events, source-scoped or explicit per-target backfill is an operational repair and targeting mode over persisted watched targets. It is not the default semantic model for generic resolver-local event intake and must not turn ENS Labs PublicResolver-generation profile admission into the address set for baseline `AddrChanged`, `AddressChanged`, `TextChanged`, or `VersionChanged` observations. Full bootstrap and whole-active-watched-chain backfill may combine the generic resolver topic scan with address-scoped source families in one raw-fact range: resolver events are topic-scanned across all emitters, while non-resolver families keep their address-scoped filters. Topic matches whose indexed fields or ABI payload do not match the ENSv1 resolver declaration are retained raw facts but are not selector/cache evidence. Replay and projection must continue to distinguish observed selector/cache facts from profile-gated complete-family and parity claims.

When historical backfill admits finalized or safe historical ranges, persisted
lineage, raw facts, and normalized events must carry the best canonicality state
supported by available checkpoint evidence: `finalized` for ranges proven below
the finalized checkpoint, `safe` for ranges proven below the safe checkpoint,
and `canonical` for reconciled canonical ranges that are not yet safe. They
must not remain `observed` merely because they entered through backfill. If the
provider or retained lineage cannot prove the required checkpoint relationship,
the runner must fail closed or persist the weaker explicit state and report the
gap; backfill lifecycle transitions themselves still must not promote
`canonical_head`, `safe_head`, or `finalized_head`.

Source-scoped backfill must avoid retaining unselected block-wide transaction,
receipt, or full block bodies in Postgres. If the runner fetches broader
block-scoped payloads to locate or verify selected target facts, the Postgres
hot store keeps selected-target logs/facts, minimal lineage/header anchors,
replay-required enrichments, and any cache metadata needed for block-hash-scoped
admission or audit. Historical blocks with no selected target facts or
replay-required enrichments retain only one `chain_lineage` header anchor per
observed block identity for ancestry repair and checkpoint accounting. Optional
header audit fields are retained in `chain_header_audit` only when the
auditable-header mode is explicitly enabled for the run; full payload cache
entries, receipt bundles, transaction bundles, or block bodies are not retained
by default. Unselected full bodies are
evictable cache unless an explicit doc-first retention policy declares that
payload class durable; otherwise the selected replay contract must not depend on
them.

Source-family backfill conformance intake for the shipped mainnet profile is limited to proving that the source selector, resolved `source_identity`, bounded job lifecycle, shared raw-fact intake, and later raw-fact normalized-event replay coexist for already admitted targets. The initial conformance families are:

- `ens_v1_wrapper_l1`: the active watched target is the admitted mainnet NameWrapper contract instance; conformance may exercise wrapper-local event intake such as `NameWrapped`, `NameUnwrapped`, `FusesSet`, and `ExpiryExtended` under that family without admitting wrapper migration history or route coverage (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L200 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L219 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L238 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L275 @ ens_v1@91c966f).
- `ens_v1_resolver_l1`: the active watched targets are admitted PublicResolver-family resolver contract instances; conformance may exercise resolver-record, resolver-version, and resolver-local authorization event intake such as `ABIChanged`, `AddrChanged`, `AddressChanged`, `Approved`, `ContenthashChanged`, `TextChanged`, and `VersionChanged` without claiming full resolver corpus replacement or route coverage (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L57 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L76 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L101 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L157 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L176 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L357 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L376 @ ens_v1@91c966f).
- `basenames_l1_compat`: the active watched target is the Ethereum Mainnet Basenames L1 Resolver as compatibility transport for the `base.eth` 2LD; conformance keeps this source family separate from execution even when the normalized address is the same (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc).
- `basenames_execution`: the active watched target is the same Ethereum Mainnet Basenames L1 Resolver as verified-resolution entrypoint selection; conformance may exercise the entrypoint boundary that routes `base.eth` through the root resolver and wildcard names through `OffchainLookup` / `resolveWithProof`, but the family remains shadow until a separate doc-first route and capability graduation lands (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc).

For these conformance families, `source_identity` is the canonical resolved target tuple persisted by the job substrate, or the compact digest form for large source-family target sets. Full payloads must include the selector mode plus the sorted selected targets, and each target identity is keyed by `source_family`, `contract_instance_id`, normalized address, effective target range start, and effective target range end. Compact payloads must digest that same sorted target tuple and include the selector mode, source family, selected target count, digest metadata, and first/last target audit sample. Same-address targets in `basenames_l1_compat` and `basenames_execution` are therefore distinct source identities, while repeated selection of the same full tuple remains idempotent. Replay coexistence means a completed source-family backfill job and a later raw-fact normalized-event replay over the same canonical facts can both upsert through their owned storage boundaries without mutating each other's checkpoints, raw facts, or public read surfaces.

Source-family backfill conformance is a non-graduation test. Passing it does not add or widen a public route, change route-level coverage, promote manifest capabilities from `shadow` or `unsupported`, add a capability group, graduate ENSv2 exact-name support, claim wrapper / migration history support, admit a fallback primary-name source, or change consumer-replacement meaning. It proves selector correctness, source-identity stability, bounded lifecycle persistence, selected-target-only intake, and replay coexistence only.

### Operational Finalized Catch-up

Operational catch-up to the finalized head is a sequence of bounded backfill
jobs, not a hidden unbounded scanner. Each catch-up chunk has an immutable job
shape, an idempotency key, a finite start, and a finite end no greater than the
finalized head observed for that chain when the chunk is created. Following the
finalized head means repeatedly creating the next finite chunk after the prior
chunk is complete or safely resumable; live intake remains responsible for the
open-ended tail.

Before reserving or running every catch-up chunk, the worker must check current
Postgres size, writable free disk, and any configured object-cache budget against
the chunk's estimated write amplification. If capacity is below the configured
minimum, or if the estimate would exceed the budget, the chunk must pause or
fail with explicit capacity metadata before starting new range work. Capacity
failure must not widen the job, drop retained replay facts, downgrade
canonicality, or silently switch to retaining fewer selected facts.

Catch-up uses the same selected-target retention contract as other backfill:
durable selected facts, lineage/header anchors, selected target logs, and
replay-required enrichments are retained, while empty historical blocks and
unselected full payloads remain cache/metadata-only or absent. Catch-up progress
does not change route coverage or consumer-replacement meaning until the full
admitted history for the relevant capability has completed and the normal
capability conformance gates pass.

Storage helpers own lifecycle mutation. They must be idempotent:

- `create_backfill_job` inserts a new bounded job or returns the existing job for the same idempotency key and immutable job shape without widening or narrowing its range, changing source identity, or replacing child range bounds
- `reserve_backfill_range` atomically claims pending or reclaimable work with a lease owner, lease token, and lease expiry; duplicate calls by the same active lease holder return the same reservation, and expired leases can be reclaimed without duplicating range work
- `advance_backfill_range` requires the current lease and moves the persisted range checkpoint forward monotonically, never below the prior checkpoint and never beyond the declared range end
- `complete_backfill_range` and `complete_backfill_job` are no-ops when already complete and must require all child range checkpoints to reach their declared ends
- `fail_backfill_range` and `fail_backfill_job` record bounded failure state and failure metadata without rewinding completed checkpoints, clearing completed ranges, or mutating raw facts

Range checkpoints are owned by the backfill job substrate. They record operational progress for fetch/resume only and must not be reused as chain checkpoints, projection replay checkpoints, or API consistency checkpoints. The runner must not call chain checkpoint advancement as a side effect of creating, reserving, advancing, completing, failing, or reusing a backfill job, regardless of whether the selector is whole-chain, source-family scoped, or an explicit watched-target set.
Bootstrap planning may use completed range checkpoints and expired or failed
range checkpoints as lower bounds for the next bounded bootstrap job with the
same resolved source identity. It must not use an unexpired active lease as
coverage, and this bootstrap resume shortcut still does not promote chain heads
or mutate canonicality outside the raw-fact write path.

Rules:

- backfill and live ingestion share the same downstream normalization and projection path after raw fetch
- receipt-rich indexing should prefer block-scoped receipt ingestion when available
- backfill jobs must be resumable, idempotent, and bounded by explicit checkpoints
- backfill completion is not proof of finality; canonical, safe, and finalized promotion still follow the lineage model
- backfill job and range checkpoint updates must not mutate or promote `canonical_head`, `safe_head`, or `finalized_head`

## 10. Batch And Retry Rules

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

## 11. State Enrichment Rules

If intake or execution enriches facts with state reads such as calls, storage, or balances:

- anchor the read to the exact block hash whenever the RPC surface supports it
- otherwise treat the enriched result as provisional until the source block is at least `safe`
- never attach number-based enrichment to a block-scoped fact as though it were reorg-proof

Historical state-heavy enrichment is an archive requirement, not a best-effort full-node feature.

## 12. Reconciliation Algorithm

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

Execution-cache invalidation emitted by reorg repair is block-hash-scoped. It invalidates `execution_cache_outcomes` rows for verified resolution and verified primary-name outcomes when their dependency set contains an orphaned `(chain_id, block_hash)` or a boundary resolved through one. It must not delete execution traces, execution steps, raw facts, or normalized events; those remain durable replay and audit inputs.

Cache dependencies must be tied to explicit block-hash-bearing chain positions or boundaries before a verified outcome can be treated as reorg-safe. Number-only, tag-only, or dependency-free verified resolution and verified primary-name rows fail closed and cannot be served from cache after a reorg check; rows for request types explicitly documented outside this Phase 9 invalidation surface remain out of scope. This reorg/replay foundation does not promote ENSv2 exact-name support or any manifest capability.

## 13. Raw-Fact Normalized-Event Replay Runner

Raw-fact normalized-event replay is bounded operational tooling over already persisted canonical raw facts. A replay request selects a finite deployment profile, chain, and block range or explicit block-hash set. For selected blocks, canonical raw facts are rows whose block identity is `canonical`, `safe`, or `finalized`; `observed` and `orphaned` facts are excluded unless a later audit-only contract explicitly admits them.

The raw-fact normalized-event replay runner performs an upsert-only adapter resync by invoking the same adapter-owned `normalized_events` boundary used after live or backfill raw admission. It must read persisted raw facts, lineage state, optional header-audit state when retained, and the already persisted manifest/source identity needed to route those facts. It may advance its own indexer-owned `normalized_replay_*` operational cursor so automatic replay can resume after restart. It may use a retained durable cold payload only when the retained replay contract requires that payload. For block-scoped payloads, it may use provider re-fetch only through an explicit block-hash-scoped, retained-digest-checked, fail-closed cache-fill path; if no retained digest exists, the payload cannot satisfy that contract. Provider re-fetch must not replace selected replay facts that the docs require Postgres to retain. It must not re-open live intake, create or reserve backfill ranges, advance backfill range checkpoints, mutate backfill jobs, promote `canonical_head`, `safe_head`, or `finalized_head`, rebuild projections, write public API state, or expose a public `v1` route.

Automatic normalized-event replay catch-up uses a single all-source chain cursor over persisted canonical raw facts and replays selected blocks in block order. It must not split catch-up into per-source-family cursors: cross-family adapters may need registry, registrar, wrapper, resolver, and reverse-claim facts in the same chronological stream to produce non-overlapping identity intervals. Source-scoped replay remains an explicit repair/backfill selector for bounded target sets, not the automatic catch-up default.

Selected-target replay scopes are operational scan bounds. For ENSv1 generic resolver-local events, replay may narrow which persisted raw logs are resubmitted to the adapter, but the scope does not graduate coverage, mutate resolver profiles, suppress otherwise retained generic resolver observations, or make profile state the source of truth for observed selector/cache facts.

Replay does not delete stale `normalized_events`, purge rows derived from selected blocks, or replace existing payloads for an already persisted normalized-event identity. Existing normalized-event identities can only be refreshed through the storage upsert canonicality path; stale conflicting payloads remain a hard storage mismatch rather than being rewritten by replay. Raw facts and lineage remain immutable, projection rebuild remains downstream worker-owned, and API responses continue to read projections and execution output rather than the replay runner.

## 14. Atomicity Boundary

The raw admission transaction boundary is one block.

That transaction writes:

- one `chain_lineage` header-anchor row for the admitted block
- optional `chain_header_audit` fields when auditable header retention is enabled
- hot raw transaction, receipt, and log facts needed for selected replay contracts
- optional cache metadata or digests for non-critical full block-scoped payloads when the selected retention contract keeps them
- any block-scoped call snapshots captured through that intake-owned raw-fact handoff
- normalized events emitted from those facts
- invalidation signals required by downstream workers

The canonical head pointer is written last inside that admission unit.

Projection workers remain downstream and asynchronous, but they must consume deterministic block-scoped invalidation and replay inputs so that reorg repair is reproducible.

## 15. Traces, Pending, And Other Optional Capabilities

Pending and mempool indexing are a separate product surface.

Trace and internal-call indexing are a separate capability plane because they depend on non-standard, client-specific APIs and different operational budgets.

Rules:

- the declared-state truth core must not require traces to be correct
- if traces are enabled later, they persist as their own raw facts with the same block-hash anchoring and reorg semantics
- intake planning must not assume all providers expose the same trace APIs

## 16. Observability And Test Requirements

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
- raw-fact normalized-event replay restart over the same bounded canonical selection as an upsert-only adapter resync whose selected replay facts come from persisted canonical raw facts; any explicit provider cache refill is block-hash-scoped, retained-digest-checked, fail-closed, and performs no checkpoint promotion
- safe or finalized promotion lagging canonical intake

## 17. Acceptance Rules

The intake contract is acceptable for the first implementation milestone only if:

- live notifications can be lost without losing correctness
- the system can reconcile short forks by hash and parent hash alone
- block-scoped data ingestion never depends on ambiguous number-only reads when a block-hash-scoped primitive exists
- raw facts are sufficient to rebuild canonical declared state after a reorg or decoder rewrite
- backfill reuses the same downstream semantics as live ingestion
- raw-fact normalized-event replay upserts normalized events only from persisted canonical selected replay facts without payload replacement, stale-row purge, projection rebuild, public API exposure, or chain/backfill checkpoint mutation
- any explicit replay cache refill uses provider re-fetch only as a block-hash-scoped, retained-digest-checked, fail-closed cache-fill path; missing digests, mismatched bytes, or unavailable historical payloads fail closed, and selected replay facts never depend on provider history
