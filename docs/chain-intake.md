# Chain Intake

Chain intake is canonical-chain reconciliation with a fact log attached. Subscriptions, filters, and provider notifications are latency hints; raw facts are append-only; canonicality and head promotion are explicit state. Block hash is identity; block number is position. Live ingestion and backfill share one downstream pipeline.[^bigname-canonicality-source]

A deployment selects one chain profile at a time. Mainnet and Sepolia facts do not share a canonical corpus, checkpoints, or projection state. The default profile selects `manifests/mainnet/`; the ENSv2 Sepolia profile selects `manifests/sepolia/`. A runtime must not load both profile roots into the same intake runtime, watch plan, discovery graph, or projection set.[^bigname-profile-source]

This document covers reconciliation, fetch, notification, backfill, and replay. The model and read contract live in [`architecture.md`](architecture.md). Persistence rules live in [`storage.md`](storage.md). Manifest and discovery rules live in [`manifests.md`](manifests.md).

## Scope

Truth-core intake covers durable replay facts and cache metadata for the raw-fact, payload-cache, and execution handoff boundary:[^bigname-storage-scope]

- blocks and lineage metadata
- selected and admitted target logs
- transaction, receipt, and block fields needed to decode selected logs or rebuild retained normalized events and execution outputs
- code-hash observations
- block-anchored call snapshots used by verified execution or enrichment
- optional cache metadata or digests for large block, transaction, or receipt bodies fetched outside the hot replay set; hash-addressed cold pointers are required only for payload classes explicitly declared durable

Block-anchored `raw_call_snapshots` remain intake-owned raw facts even when verified execution supplied the candidate request and response pair. Execution may hand off only snapshots anchored to the resolved requested chain position and only for a persistence path that already admits those snapshots. That handoff does not create a general execution-owned raw-fact write surface.[^bigname-execution-snapshot-source]

Out of scope: mempool or pending-transaction indexing, node-local txpool APIs, client-specific trace or state-diff indexing as a correctness dependency, historical state reconstruction from non-archive upstreams. Any of these may exist later as separate capabilities; they do not enter the core correctness model.[^bigname-support-boundary]

## ENSv1 and Basenames resolver discovery

ENSv1 old-registry intake is migration-aware historical admission, not a second current-registry stream. `ENSRegistryOld` stays under `ens_v1_registry_l1` as an allow-listed migration-epoch input at `0x314159265dd8dbb310642f98f50c066173c1259b` with `start_block = 3327417`. The current registry `startBlock: 9380380` is the current registry's pinned start, not original ENS history.[^subgraph-l10][^subgraph-l15][^subgraph-l39][^subgraph-l42][^subgraph-l44]

Old-registry raw facts retain their emitter identity and pass a migration guard before they normalize into current topology. A current-registry `NewOwner` marks the affected subnode migrated; later old-registry `NewOwner`, `Transfer`, `NewTTL`, and non-root `NewResolver` observations for that node are retained as facts but do not overwrite the current owner, resolver, TTL, child edge, resolver-discovery edge, or projection input. The root resolver is the single exception: old-registry `NewResolver(ROOT_NODE, resolver)` may still update the root resolver binding and feed `ens_v1_resolver_l1` discovery. The pinned subgraph's old-registry handlers encode the same migrated-node guard and root-resolver exception.[^subgraph-ts-l134][^subgraph-ts-l230][^subgraph-ts-l238][^subgraph-ts-l246][^subgraph-ts-l252][^subgraph-ts-l259]

Resolver discovery feeds declared record indexing for ENSv1 and Basenames; static manifest admission is not enough. Registry-level resolver changes are discovery inputs:[^bigname-discovery-source]

- ENSv1 `NewResolver(node, resolver)` from admitted `ens_v1_registry_l1` emitters produces resolver discovery observations for `ens_v1_resolver_l1`. Nonzero resolver addresses create or refresh the node-to-resolver binding and the resolver contract instance; the zero address closes the affected binding.[^v1-ens-l12][^v1-ensreg-l89][^v1-ensreg-l174]
- Basenames `NewResolver(node, resolver)` from admitted `basenames_base_registry` emitters produces resolver discovery observations for `basenames_base_resolver`. The same nonzero/zero rules apply on Base.[^bn-registry-l19][^bn-registry-l132][^bn-registry-l223]

The resolver address observed in declared topology is not enough by itself. Contract-instance admission, node-to-resolver binding state, generic event intake, and supported resolver-profile admission are separate.[^bigname-discovery-source]

For ENSv1, retained generic resolver-local record and version events such as `AddrChanged`, `AddressChanged`, `TextChanged`, and `VersionChanged` feed observed selector/cache and version-boundary facts when the emitter and node match the selected resolver binding. Unobserved selectors stay explicit gaps or `resolver_family_pending` rather than silently going absent or complete. Generic resolver-topic intake is topic-first: a raw log whose payload cannot ABI-decode to the upstream resolver event shape is retained but does not emit an observed selector/cache or version-boundary fact.[^v1-iaddr-l6][^v1-iname-resolver-l5][^v1-itext-l5][^v1-itext-l10]

Resolver-profile admission gates complete record-family coverage, resolver-overview completeness, resolver-local authorization semantics, latest-only behavior, and event-to-onchain-call parity.[^v1-iaddr2-l6][^v1-iaddress-l6][^v1-itext2-l5][^v1-iversionable-l5] Basenames resolver-local record, permission, alias, and resolver-overview facts remain governed by the separate `L2Resolver`-compatible profile gate.[^bn-l2resolver-l22][^bigname-discovery-source]

The first dynamic resolver-profile admission for ENSv1 is limited to ENS Labs PublicResolver-generation profiles for the relevant complete fact families. The profile gate may use direct manifest admission, first-party known-resolver admission, stored code-hash observations, proxy/implementation edges, or another explicit non-schema admission rule. Registry `NewResolver` observation alone is not enough. Unknown dynamic resolvers keep explicit `pending` or `unsupported` profile state; older admitted generations expose only the families listed for their profile. PublicResolver-generation compatibility anchors to the upstream PublicResolver mixins, ERC165 support, and `ResolverBase` record-versioning.[^v1-pres-l20][^v1-pres-l31][^v1-pres-l131][^v1-pres-l150][^v1-resolverbase-l17][^v1-resolverbase-l21][^v1-resolverbase-l22][^v1-resolverbase-l23] ENSv1 profile admission does not widen Basenames resolver-profile support.[^bn-l2resolver-l22][^bn-registry-l132]

## ENSv2 Sepolia Adapter Intake

The ENSv2 Sepolia intake starts from four admitted source families: `ens_v2_root_l1`, `ens_v2_registry_l1`, `ens_v2_registrar_l1`, and `ens_v2_resolver_l1` under `manifests/sepolia/ethereum/ens/...`. Direct watched roots come from the pinned upstream `sepolia-dev` deployment metadata for `RootRegistry`, `ETHRegistry`, and `ETHRegistrar`. `PermissionedResolverImpl` is implementation metadata for discovered or admitted resolver instances; resolver instances enter the watch plan only through manifest admission or discovery edges.[^v2-deploy-root][^v2-deploy-ethreg][^v2-deploy-ethrc][^v2-deploy-pres]

ENSv2 adapters normalize log-derived facts after raw block admission:

- `TokenResource(tokenId, resource)` becomes `TokenResourceLinked`. `TokenRegenerated(oldTokenId, newTokenId)` becomes `TokenRegenerated` and is not treated as a new resource.[^v2-iperm-l34][^v2-events-l69][^v2-pr-l451]
- `SubregistryUpdated`, `ResolverUpdated`, and `ParentUpdated` become graph and topology events after their endpoint addresses resolve to current `contract_instance_id` values for the selected profile.[^v2-events-l49][^v2-events-l59][^v2-events-l75]
- `AliasChanged` becomes `AliasChanged` on admitted resolver instances. `EACRolesChanged` becomes resource-, root-, or resolver-scoped Permission events after the adapter resolves the upstream EAC resource to bigname identity.[^v2-iperm-resolver-l14][^v2-eac-l19]

Any ENSv2 enrichment call used to repair or disambiguate a log-derived fact — `getResource(anyId)`, `getTokenId(anyId)`, `getState(anyId)`, `getAlias(fromName)`, EAC role reads — anchors to the same block identity as the raw log. Log-derived state is never rewritten through ambiguous number-only calls.[^v2-iperm-l57][^v2-iperm-l67][^v2-iperm-l72][^v2-iperm-resolver-l56][^v2-eac-l100]

## Upstream requirements

For each chain source in the selected deployment profile, the intake plane has access to hash-addressed fetch and checkpoint primitives needed by the storage contract:[^bigname-provider-source]

- block fetch by hash
- block fetch by number or canonical tag
- log fetch by exact block identity
- receipt fetch for a whole block when supported, with a bounded fallback path
- code and call reads at pinned chain positions
- safe and finalized head visibility

Production correctness depends on `safe` and `finalized` support. Sources that cannot surface those checkpoints are bootstrap or shadow sources only. A self-hosted post-Merge Ethereum upstream operates an execution client and a consensus client together. Historical state-heavy enrichment and state rewrites require archive-capable upstreams, a separately retained durable replay corpus, or explicit fail-closed behavior when the relevant cache-fill path cannot satisfy its block-hash-scoped fetch and retained-digest checks. Upstream history retention is bounded; intake retains its own durable hot replay facts for deterministic replay and treats provider re-fetch as a cache-fill path, not a substitute for selected replay facts.[^reth-readme-l23][^bigname-cache-source]

### Reference anchors

Repo-wide citation rules live in [`AGENTS.md`](../AGENTS.md#upstream-anchors) and [`upstream.md`](upstream.md). The refs below are chain-intake anchors: Ponder, graph-node, and Reth are comparison sources for reorg, rewind, and node behavior; they do not replace bigname's storage contract.

- Reth tracks blocks by hash and number, admits multiple hashes at one height, records parent-child links, and resolves `safe` / `finalized` tags to block hashes. Its RPC call path converts non-pending block targets to block hash so related provider calls hit the same block.[^reth-tree-l61][^reth-state-l25][^reth-state-l31][^reth-state-l35][^reth-block-id-l81][^reth-block-id-l90][^reth-call-l326]
- Exact fetch is hash-first. Ponder keeps a parent-linked unfinalized window and fetches live blocks/logs by block hash; graph-node uses EIP-1898 hash block IDs when available, fetches blocks and block receipts by hash, and rejects trace/receipt data whose block hash does not match the requested block.[^ponder-unfinalized-l71][^ponder-unfinalized-l118][^ponder-logs-l310][^graph-blockptr-l596][^graph-block-l1301][^graph-receipts-l2334][^graph-trace-l1086][^graph-receipt-l2486]
- Reorg repair is rewind plus replay, not latest-row wins. Ponder rolls database changes back to the common ancestor and reprocesses canonical data; graph-node's time-travel rows roll back future versions; Reth ExEx notifications revert a noncanonical head and then backfill the canonical chain.[^ponder-reorg-l41][^ponder-reorg-l44][^graph-time-l112][^reth-notify-l342][^reth-notify-l742]
- Storage is bounded but must stay behind the rewind horizon. Ponder drops finalized transaction-log data and keeps unfinalized data out of its RPC cache; graph-node pruning restricts time travel and keeps retained history beyond the reorg threshold; Reth finalizes its WAL and uses finished heights to decide what is safe to prune.[^ponder-reorg-l47][^ponder-cache-l82][^ponder-cache-l87][^graph-prune-l3][^graph-prune-l24][^graph-prune-l28][^reth-wal-l35][^reth-finished-l14]

### Local runtime provider configuration

`bigname-indexer run` selects one manifest profile root with `BIGNAME_INDEXER_MANIFESTS_ROOT` and reads provider sources from `BIGNAME_INDEXER_CHAIN_RPC_URLS` and `BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES`. Each setting is a comma-delimited list of `<chain>=<value>` entries; each chain name matches an active watched chain produced by the selected manifest and watch state. JSON-RPC values are provider URLs; Reth DB values are local Reth data directories for deployments with a same-host Reth database and static-file store. The checked-in default selects `manifests/mainnet`; the ENSv2 Sepolia profile loads with `BIGNAME_INDEXER_MANIFESTS_ROOT=manifests/sepolia`.[^bigname-deployment-profile-source]

Header audit retention is an explicit operational mode. By default, `bigname-indexer run`, `backfill`, and `ops-catchup` persist minimal block anchors only: block hash, parent hash, number, timestamp, and canonicality state. Passing `--retain-header-audit-fields` or setting `BIGNAME_INDEXER_RETAIN_HEADER_AUDIT_FIELDS=true` retains nullable `logs_bloom`, `transactions_root`, `receipts_root`, and `state_root` when the provider returns them. Minimal replay coexists with retained fields without clearing them; auditable replay may fill a previously minimal row, and conflicting non-null audit fields stay an identity mismatch.[^bigname-lineage-source]

At most one provider source may be configured for a chain. JSON-RPC is the portable source. A Reth DB source is an optional intake source, not a protocol adapter: ENS and Basenames adapters still consume bigname raw facts and append adapter-owned normalized events. The checked-in Reth DB reader targets Ethereum Mainnet Reth datadirs; other chains fail closed until they have a chain-specific reader. A Reth-backed reader satisfies the same block-hash-first provider contract as JSON-RPC for heads, exact block payloads, selected logs, receipts, and block-anchored code/state observations. It fails closed when the local Reth store is unavailable, pruned, inconsistent with the selected chain, or cannot surface the requested safe/finalized checkpoint or exact historical payload. Live execution, trace production, and fresh API-time reads remain outside this source boundary.[^bigname-deployment-provider-source][^reth-readme-l23]

The provider source list is an operational input, not a manifest admission rule. An unset list leaves manifest sync, watch-plan rebuild, and checkpoint row creation available, but provider-backed head fetch and live ingestion stay idle for every active watched chain. A manifest root may declare active watched chains that are fully synchronized into manifest, discovery, watch-plan, and checkpoint setup while automatic bootstrap and live provider work stay idle until the chain has a configured provider source. Bootstrap JSON-RPC support accepts `http://` endpoints only.[^bigname-deployment-provider-source]

Local Reth row keys, static-file offsets, and table handles do not replace bigname raw fact identities. They are retained as operational source metadata or evictable cache metadata only. Durable selected logs, transactions, receipts, block and header anchors, code-hash observations, and normalized-event `raw_fact_ref` values stay keyed by bigname's chain/block/log identity in Postgres according to the storage contract.[^bigname-cache-source]

Provider availability is evaluated per selected profile and per active watched chain. A Base provider is not a global startup prerequisite: an Ethereum-only profile starts without a Base provider, and a profile whose Base chain has no configured provider leaves Base provider-backed intake, automatic bootstrap, backfill catch-up, and live head following idle with an explicit `unavailable` / `no_provider` reason. Startup does not fail for other configured chains. A configured provider for a chain outside the selected profile remains invalid; the runtime never ingests across profiles.[^bigname-deployment-provider-source]

## Head model and recent window

Per chain, intake tracks three persisted checkpoints:[^bigname-head-source]

- `canonical_head`
- `safe_head`
- `finalized_head`

API consistency maps directly: `consistency=head` reads from canonical, `consistency=safe` from safe, `consistency=finalized` from finalized.[^bigname-head-source]

The intake plane keeps a recent reconciled window keyed by `(chain_id, block_hash)` with at least `parent_hash`, `block_number`, `timestamp`, `logs_bloom`, `transactions_root`, `receipts_root`, and `state_root` when the upstream exposes it. The window detects parent mismatch immediately, walks back to a common ancestor on reorg, backfills short parent gaps, and answers recent canonicality disputes and audits. Number-to-hash mappings inside this window are derived views; the primary key is always block hash.[^bigname-lineage-source][^reth-tree-l61][^reth-state-l25][^reth-state-l31]

## Block identity and storage rules

Lineage and raw facts preserve enough information to rebuild canonicality without re-scraping chain history.[^bigname-lineage-source]

- Block hash is the identity anchor for every block-scoped object.
- `parent_hash` is required in lineage storage.
- Lineage ancestry repair requires only block hash, parent hash, block number, timestamp, and canonicality state. Header audit fields (logs bloom, transaction root, receipt root, state root) are retained only in auditable-header mode; otherwise they are nullable and omitted from the hot sparse-backfill path.
- Every chain-derived raw fact row carries `chain_id`, `block_number`, and `block_hash`.
- Live indexing may fetch full block, transaction, and receipt payloads, but Postgres retains only replay-critical hot facts and optional cache metadata for non-critical full bodies.
- Cache metadata is stable enough to explain which payload was fetched. Metadata that may authorize later byte use carries a retained digest, and the digest is verified before any cached, object-backed, or provider-refetched payload is used.
- Caches key by block hash first; block number is a secondary lookup or pagination aid.
- A downstream key that needs "current block number" resolves it to a block hash before reading block-scoped data.

## Notification and fetch contract

Subscriptions, filters, and polling are low-latency triggers, not durable truth. Connection loss does not imply data loss or canonical confirmation; durable correctness comes from exact block fetch, parent reconciliation, and persisted lineage.[^bigname-provider-source][^ponder-unfinalized-l71][^graph-blockptr-l596][^reth-call-l326]

The live path:

1. Receive a head notification from polling or subscription.
2. Fetch the referenced block or header by hash when possible.
3. Reconcile `parent_hash` against the recent window.
4. Fetch exact block-scoped data.
5. Persist one block admission unit atomically.
6. Advance canonical, safe, and finalized checkpoints only after reconciliation.

For exact block-scoped data: logs are fetched by `blockHash`, not just block number; providers that cannot support that contract are not acceptable for that path. Receipts are fetched block-scoped first; transaction-by-transaction receipt fan-out is a fallback. Live ingestion never relies on subscription payloads alone as the persisted source of truth. Live ingestion may fetch full block-scoped payloads to derive selected facts, but the persisted Postgres admission unit keeps replay-critical hot rows and optional cache metadata for non-critical full payloads.[^ponder-logs-l310][^graph-receipts-l2334][^bigname-cache-source]

## Backfill contract

Backfill uses logs-centric range scans or block-centric receipt or block scans. It runs as persisted, bounded jobs scoped to one selected deployment profile, chain, source selector, scan mode, and explicit block range. The source selector mode is `whole_active_watched_chain` by default when no selector is supplied, `source_family`, or an explicit `watched_target_set`. The job range is finite at creation time; open-ended tail following remains live intake.[^bigname-backfill-source]

Manual Base backfill may use Coinbase CDP SQL as a historical log candidate source by setting `--backfill-source coinbase-sql` or by setting `--backfill-source auto` with a Base chain and a configured Coinbase SQL URL. Coinbase SQL is not a normal `ChainProvider`: it never supplies live heads, safe/finalized evidence, canonicality assignment, code observations, repair payloads, ops catch-up, or live polling. A normal validation provider for the same chain remains required; that provider resolves block numbers to block hashes, fetches headers, supplies canonicality evidence, fills exact raw log payload bytes for Coinbase SQL log identities when the SQL schema exposes identity fields only, fetches code observations, and fills or validates selected transaction/receipt facts before raw-fact persistence. In both supported validation modes for the first slice, `full` and `sample`, the validation provider scans the same selected address/topic span and fails the range if Coinbase SQL omits or adds a selected log identity. Coinbase SQL jobs use the distinct scan mode `coinbase_sql_hash_pinned_logs_v1` and still advance `backfill_ranges.checkpoint_block_number` only after the bounded window has been fetched, block-hash validated, persisted through the existing raw-fact path, and adapter-synced or explicitly skipped.[^bigname-backfill-source][^bigname-head-source]

Full historical backfill covers the entire admitted history for the selected deployment profile, chain, and selected targets. The start is the manifest/discovery admitted start for each selected target, not an arbitrary recent window. A recent chunk, a startup bootstrap range, or a partial source-family conformance run is not complete history and is not used as consumer-replacement or route-coverage evidence.[^bigname-coverage-source]

### Automatic bootstrap

`bigname-indexer run` creates historical backfill work from the selected manifest root and materialized watch plan as finite persisted backfill jobs. It does not run an implicit unbounded scanner, tail follower, or address-only fetch path.[^bigname-deployment-bootstrap-source]

Automatic bootstrap follows these rules:

- It runs after manifest sync, discovery admission, watch-plan materialization, and per-chain checkpoint row setup for the selected deployment profile.
- Active watched chains without configured providers stay idle. Bootstrap does not create jobs for a chain whose provider cannot supply a finite bootstrap end.
- Bootstrap covers each eligible target from its manifest/discovery admitted start through the finite provider head observed at job creation time. It does not cap the start to an arbitrary recent window; full admitted history is the default startup work for configured chains.
- Each candidate target is the resolved watched target keyed by `contract_instance_id`, source family, chain, normalized address, and effective range. Raw address is never accepted as durable source identity.
- Bootstrap groups eligible targets whose finite ranges overlap into the same raw-fact job segment by default. It does not run one full-chain pass per source family merely because source-scoped repair exists; source-scoped jobs remain an explicit operational targeting mode for repair, conformance, or manual backfill.
- A finite job segment may partition into multiple contiguous child `backfill_ranges` for internal worker leases. Child ranges preserve the same immutable job source identity, declared bounds, and raw-fact ownership. Partitioning does not create source-family-specific jobs, widen coverage, promote checkpoints, or change replay/projection ownership.
- The persisted source identity is the sorted resolved target set with effective range start and end, matching the same canonical target tuple used by source-scoped backfill. Very large selected target sets may use the compact digest form below instead of embedding every target tuple in one JSONB value. If the segment includes ENSv1 generic resolver-event intake, the source identity carries `generic_topic_scans` for `ens_v1_resolver_l1`; resolver profile/admission addresses are not the address filter for those generic event facts.
- A target with declared `start_block` is eligible from that inclusive block, narrowed by its active watch range and the finite bootstrap end resolved at job creation.
- A target with omitted `start_block` has unknown historical start and is skipped explicitly with that reason. Bootstrap does not infer the target start from block zero, the current job range start, manifest activation, provider history, or any default range.
- Every created job has finite declared range start and end before insertion into `backfill_jobs`.
- Creating, reserving, advancing, completing, or failing an automatic bootstrap job follows the same backfill lifecycle as manual jobs and never mutates `canonical_head`, `safe_head`, or `finalized_head`.

Automatic bootstrap is operational intake readiness only. It does not add or widen public API routes, route-level coverage, manifest capability flags, additional ENSv2 profile support, or consumer-replacement meaning.

### Job lifecycle

Backfill jobs use a bounded lifecycle:[^bigname-backfill-source]

- `pending` — the job or range exists, no worker owns it.
- `reserved` — a worker has a lease for the next bounded range checkpoint.
- `running` — the reserved worker is advancing the range checkpoint through the shared intake path.
- `completed` — every range checkpoint reached its declared end.
- `failed` — the job or range stopped with recorded failure metadata; retries create or reserve explicit remaining work.

The resumable backfill runner is indexer/backfill-owned operational tooling exposed through `bigname-indexer backfill` over this persisted job model. Each invocation supplies or reuses an idempotency key for one immutable job shape: selected deployment profile, chain, source selector, scan mode, finite range start, and finite range end. If the key already names that exact shape, the command reuses the existing job and ranges. If the same key is presented with a different shape, the command fails with an explicit conflict instead of widening the range, changing source identity, resetting checkpoints, or reclassifying already admitted facts.[^bigname-backfill-source]

### Selector modes

The source-scoped backfill runner has three mutually exclusive selector modes:[^bigname-backfill-source]

- `whole_active_watched_chain` — default when no selector is supplied. Selected targets are every active watched target for the selected deployment profile and chain whose active watch range intersects the finite job range at job creation.
- `source_family` — `--source-family <family>`. Selected targets are the active watched targets in that family whose active watch range intersects the finite job range. Unknown families or families with no matching active targets fail before job creation rather than falling back to whole-chain backfill.
- `watched_target_set` — explicit watched-target set. Targets are identified by `contract_instance_id`; raw addresses alone are not accepted. Selected targets are exactly the supplied identities after validation against the selected deployment profile, chain, and finite range. The runner does not expand an explicit set to sibling targets, other targets in the same source family, or the whole active watch plan.

The persisted source identity for any selector is the resolved target set, not the CLI spelling. It is stable and sorted by `source_family`, `contract_instance_id`, normalized address, effective range start, and effective range end. Duplicate target identities collapse only when the full canonical target tuple matches; conflicting metadata for the same target identity fails job creation with an explicit source-identity conflict. For idempotency-key reuse, the runner compares persisted selector mode and resolved source identity. If the active watch plan has shifted such that the same CLI selector now resolves to a different target set, the same idempotency key conflicts instead of mutating the existing job. When a selected target set is too large to retain safely as one JSONB payload, the persisted identity uses `source_identity_payload_format=selected_targets_digest_v1`: selector fields, requested target identities, selected target count, digest algorithm, digest of the sorted selected target tuples, a first/last target audit sample, and `source_identity_hash`. The sorted canonical target tuple is the digest input; the runner does not downgrade to raw-address identity or make the selector mutable.[^bigname-backfill-source]

### Selected-target intake

Backfill intake for a source-scoped job is selected-target-only and block-hash-scoped. The runner may use block-number ranges to enumerate candidate blocks, but every persisted block-scoped fact or enrichment is anchored to the resolved block hash before admission through the shared intake path. The job may persist minimal lineage and header anchors needed for that admission; target-scoped log admission, call snapshots, normalized events, and downstream projection invalidation stay limited to the selected targets. A source-scoped job does not opportunistically admit unselected watched targets merely because they appear in the same block, receipt batch, source family, or chain range. Automatic full bootstrap uses the combined-segment rule above.[^bigname-backfill-source][^bigname-cache-source]

For ENSv1 resolver events, source-scoped or per-target backfill is an operational repair and targeting mode over persisted watched targets. It is not the default semantic model for generic resolver-local event intake, and PublicResolver-generation profile admission is not the address set for baseline `AddrChanged`, `AddressChanged`, `TextChanged`, or `VersionChanged` observations. Full bootstrap and whole-active-watched-chain backfill may combine the generic resolver topic scan with address-scoped source families in one raw-fact range: resolver events are topic-scanned across all emitters, while non-resolver families keep their address-scoped filters. Topic matches whose indexed fields or ABI payload do not match the ENSv1 resolver declaration are retained raw facts but are not selector/cache evidence. Replay and projection continue to distinguish observed selector/cache facts from profile-gated complete-family and parity claims.[^v1-iaddr-l6][^v1-iaddress-l6][^v1-itext-l5][^v1-itext-l10][^v1-iversionable-l5][^bigname-discovery-source]

### Canonicality at admission

When historical backfill admits finalized or safe historical ranges, persisted lineage, raw facts, and normalized events carry the best canonicality state supported by available checkpoint evidence: `finalized` for ranges proven below the finalized checkpoint, `safe` for ranges proven below the safe checkpoint, and `canonical` for reconciled canonical ranges that are not yet safe. They do not stay `observed` merely because they entered through backfill. If the provider or retained lineage cannot prove the required relationship, the runner fails closed or persists the weaker explicit state and reports the gap. Backfill lifecycle transitions still do not promote `canonical_head`, `safe_head`, or `finalized_head`.[^bigname-head-source][^bigname-backfill-source]

Source-scoped backfill avoids retaining unselected block-wide transaction, receipt, or full block bodies in Postgres. If the runner fetches broader block-scoped payloads to locate or verify selected target facts, the Postgres hot store keeps selected-target logs and facts, minimal lineage and header anchors, replay-required enrichments, and any cache metadata needed for block-hash-scoped admission or audit. Historical blocks with no selected target facts or replay-required enrichments retain only one `chain_lineage` header anchor per observed block identity for ancestry repair and checkpoint accounting. Optional header audit fields land in `chain_header_audit` only when the auditable-header mode is enabled for the run; full payload cache entries, receipt bundles, transaction bundles, and block bodies are not retained by default. Unselected full bodies are evictable cache unless an explicit doc-first retention policy declares the payload class durable.[^bigname-cache-source]

### Source-family conformance

Source-family backfill conformance for the shipped mainnet profile proves selector correctness, source-identity stability, bounded lifecycle persistence, selected-target-only intake, and replay coexistence. The conformance families are:[^bigname-backfill-source][^bigname-coverage-source]

- `ens_v1_wrapper_l1` — admitted Mainnet NameWrapper. Conformance exercises wrapper-local event intake (`NameWrapped`, `NameUnwrapped`, `FusesSet`, `ExpiryExtended`) without admitting wrapper migration history or route coverage.[^v1-namewrapper-deploy][^v1-nw-deploy-l200][^v1-nw-deploy-l219][^v1-nw-deploy-l238][^v1-nw-deploy-l275]
- `ens_v1_resolver_l1` — admitted PublicResolver-family resolver instances. Conformance exercises resolver-record, resolver-version, and resolver-local authorization events (`ABIChanged`, `AddrChanged`, `AddressChanged`, `Approved`, `ContenthashChanged`, `TextChanged`, `VersionChanged`) without claiming full resolver corpus replacement or route coverage.[^v1-publicresolver-deploy][^v1-pres-deploy-l57][^v1-pres-deploy-l76][^v1-pres-deploy-l101][^v1-pres-deploy-l157][^v1-pres-deploy-l176][^v1-pres-deploy-l357][^v1-pres-deploy-l376]
- `basenames_l1_compat` — Ethereum Mainnet Basenames L1 Resolver as compatibility transport for `base.eth`. Conformance keeps this family separate from execution even when the normalized address is the same.[^bn-readme-l22][^bn-readme-l69][^bn-l1resolver-l13]
- `basenames_execution` — same Ethereum Mainnet Basenames L1 Resolver as verified-resolution entrypoint selection. Conformance exercises the active v2 exact-surface transport-assisted direct-path class, including the entrypoint that routes `base.eth` through the root resolver and wildcard names through `OffchainLookup` / `resolveWithProof`; other Basenames verified path classes remain unsupported.[^bn-readme-l22][^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]

For these families, `source_identity` is the canonical resolved target tuple (or compact digest form for large source-family target sets). Full payloads include the selector mode plus the sorted selected targets; each target identity keys by `source_family`, `contract_instance_id`, normalized address, effective target range start, and effective target range end. Compact payloads digest the same sorted target tuple and include the selector mode, source family, selected target count, digest metadata, and first/last target audit sample. Same-address targets in `basenames_l1_compat` and `basenames_execution` are distinct source identities; repeated selection of the same full tuple stays idempotent. Replay coexistence means a completed source-family backfill job and a later raw-fact normalized-event replay over the same canonical facts both upsert through their owned storage boundaries without mutating each other's checkpoints, raw facts, or public read surfaces.[^bigname-backfill-source][^bigname-replay-source]

Source-family conformance is non-graduation. Passing it does not add or widen a public route, change route-level coverage, promote manifest capabilities from `shadow` or `unsupported`, add a capability group, graduate ENSv2 exact-name support, claim wrapper or migration history support, admit a fallback primary-name source, or change consumer-replacement meaning.[^bigname-coverage-source]

### Operational finalized catch-up

Operational catch-up to the finalized head is a sequence of bounded backfill jobs, not a hidden unbounded scanner. Each chunk has an immutable job shape, an idempotency key, a finite start, and a finite end no greater than the finalized head observed for that chain when the chunk is created. Following the finalized head means repeatedly creating the next finite chunk after the prior chunk completes or becomes safely resumable. Live intake remains responsible for the open-ended tail.[^bigname-deployment-bootstrap-source]

Before reserving or running a catch-up chunk, the worker checks current Postgres size, writable free disk, and any configured object-cache budget against the chunk's estimated write amplification. If capacity is below the configured minimum or the estimate would exceed the budget, the chunk pauses or fails with explicit capacity metadata before starting new range work. Capacity failure does not widen the job, drop retained replay facts, downgrade canonicality, or silently switch to retaining fewer selected facts.[^bigname-deployment-capacity-source]

Catch-up uses the same selected-target retention contract as other backfill: durable selected facts, lineage and header anchors, selected target logs, and replay-required enrichments are retained, while empty historical blocks and unselected full payloads stay cache or metadata only or absent. Catch-up progress does not change route coverage or consumer-replacement meaning until full admitted history for the relevant capability has completed and the normal capability conformance gates pass.[^bigname-cache-source][^bigname-coverage-source]

### Storage helpers

Storage helpers own lifecycle mutation and are idempotent:[^bigname-backfill-source]

- `create_backfill_job` inserts a new bounded job or returns the existing job for the same idempotency key and immutable shape without widening or narrowing range, changing source identity, or replacing child range bounds.
- `reserve_backfill_range` atomically claims pending or reclaimable work with a lease owner, lease token, and lease expiry. Duplicate calls by the same active lease holder return the same reservation; expired leases reclaim without duplicating range work.
- `advance_backfill_range` requires the current lease and moves the persisted range checkpoint forward monotonically. The checkpoint records the last completed block, initializes to one block before the child range start, and never advances beyond the declared range end.
- `complete_backfill_range` and `complete_backfill_job` are no-ops when already complete and require all child range checkpoints to reach their declared ends.
- `fail_backfill_range` and `fail_backfill_job` record bounded failure state and metadata without rewinding completed checkpoints, clearing completed ranges, or mutating raw facts.

Range checkpoints belong to the backfill job substrate. They record operational fetch and resume progress only and are never reused as chain checkpoints, projection replay checkpoints, or API consistency checkpoints. Retrying a range resumes at `checkpoint_block_number + 1`, so a successfully advanced boundary block is not fetched again. The runner does not advance chain checkpoints as a side effect of creating, reserving, advancing, completing, failing, or reusing a backfill job, regardless of selector. Bootstrap planning may use completed range checkpoints and expired or failed range checkpoints as lower bounds for the next bounded bootstrap job with the same resolved source identity. It does not use an unexpired active lease as coverage; the bootstrap resume shortcut still does not promote chain heads or mutate canonicality outside the raw-fact write path.[^bigname-backfill-source][^bigname-head-source]

### Shared rules

- Backfill and live ingestion share the same downstream normalization and projection path after raw fetch.[^bigname-canonicality-source]
- Receipt-rich indexing prefers block-scoped receipt ingestion when available.[^graph-receipts-l2334][^bigname-provider-source]
- Backfill jobs are resumable, idempotent, and bounded by explicit checkpoints.[^bigname-backfill-source]
- Backfill completion is not proof of finality; canonical, safe, and finalized promotion follow the lineage model.[^bigname-head-source]
- Backfill job and range checkpoint updates never mutate or promote `canonical_head`, `safe_head`, or `finalized_head`.[^bigname-backfill-source]

## Batch and retry rules

Batching applies only to independent work: many block fetches for historical backfill, many exact block-scoped log fetches, many receipt lookups inside a bounded fallback, many code-hash or ABI lookups.[^bigname-provider-source][^bigname-backfill-source]

- Later pipeline stages do not assume earlier batched results are canonical until reconciliation finishes.
- Every batch item is retryable independently.
- Partial batch failure does not corrupt intake ordering.
- Batch size stays bounded and measurable.

## State enrichment

When intake or execution enriches facts with state reads (calls, storage, balances):[^bigname-execution-snapshot-source]

- Anchor the read to the exact block hash whenever the RPC surface supports it.
- Otherwise treat the enriched result as provisional until the source block is at least `safe`.
- Never attach number-based enrichment to a block-scoped fact as if it were reorg-proof.

Historical state-heavy enrichment is an archive requirement, not a best-effort full-node feature.[^bigname-cache-source]

## Reconciliation algorithm

Reorg handling is an explicit unwind and replay. For each candidate canonical block:[^bigname-reorg-source]

1. If the block is already known, update checkpoint promotion state only.
2. If `parent_hash` matches the current canonical head, append it.
3. If the parent is missing, backfill parents until continuity or an existing checkpoint.
4. If the parent conflicts with the current canonical head, walk back through the recent window to a common ancestor.
5. Mark the losing branch `orphaned`.
6. Emit deterministic invalidation for normalized events and `execution_cache_outcomes` rows derived from orphaned block identities.
7. Admit the winning branch in canonical order.
8. Move the canonical head pointer last.
9. Promote blocks under the safe and finalized checkpoints asynchronously and monotonically.

Reconciliation never depends on ad hoc deletes or "latest row wins" semantics.[^bigname-reorg-source]

Execution-cache invalidation emitted by reorg repair is block-hash-scoped. It invalidates `execution_cache_outcomes` rows for verified resolution and verified primary-name outcomes when their dependency set contains an orphaned `(chain_id, block_hash)` or a boundary resolved through one. It does not delete execution traces, execution steps, raw facts, or normalized events; those remain durable replay and audit inputs.[^bigname-execution-cache-source]

Cache dependencies tie to explicit block-hash-bearing chain positions or boundaries before a verified outcome can be treated as reorg-safe. Number-only, tag-only, or dependency-free verified resolution and verified primary-name rows fail closed and cannot be served from cache after a reorg check.[^bigname-execution-cache-source]

The bigname rule is the reliability constraint, not those storage layouts: keep enough hash-addressed lineage, selected replay facts, normalized events, identity intervals, and invalidation state to unwind and replay deterministically, while keeping full payload bodies and non-critical staging rows evictable.[^bigname-reorg-source][^bigname-cache-source]

## Rewind contract

Rewind is the shared primitive for reorg repair, operator repair, and historical snapshot materialization. A rewind request selects one deployment profile, one chain, and either an exact ancestor `(chain_id, block_number, block_hash)` or a finite set/range of known block identities. Block number alone is never a rewind target.[^bigname-rewind-source]

The indexer exposes the exact-ancestor repair form as `bigname-indexer rewind --deployment-profile <profile> --chain <chain> --ancestor-block-number <number> --ancestor-block-hash <hash> [--from-block-hash <hash>]`. When `from_block_hash` is omitted, the stored canonical checkpoint is the rewind start. The command rejects ancestors that are not on the stored parent-hash path from the selected start.

The rewind path marks affected lineage, raw facts, identity rows, normalized events, projection inputs, and reusable execution-cache outcomes noncanonical or invalid before replaying the winning canonical selection. It emits key-scoped projection invalidations and block-hash-scoped execution invalidations. It does not delete durable audit facts, mutate manifests, widen backfill jobs, promote chain checkpoints, or let API handlers read raw facts directly.[^bigname-rewind-source]

Time-travel reads use the same hash-first rewind/rebuild machinery. If a requested `at` or `chain_positions` snapshot has not been materialized into projection/execution rows with matching chain-position context, the route returns `stale`; it does not serve newer current rows, provider `latest`, or raw-fact scans as a substitute.[^bigname-time-travel-source]

## Raw-fact normalized-event replay

Replay is bounded operational tooling over already persisted canonical raw facts. A replay request selects a finite deployment profile, chain, and block range or explicit block-hash set. Canonical raw facts are rows whose block identity is `canonical`, `safe`, or `finalized`; `observed` and `orphaned` facts are excluded unless a later audit-only contract explicitly admits them.[^bigname-replay-source]

The runner performs an upsert-only adapter resync by invoking the same adapter-owned `normalized_events` boundary used after live or backfill raw admission. It reads persisted raw facts, lineage state, optional header-audit state when retained, and the persisted manifest/source identity needed to route those facts. It may advance its own indexer-owned `normalized_replay_*` operational cursor so automatic replay resumes after restart. It may use a retained durable cold payload only when the retained replay contract requires that payload. For block-scoped payloads, it uses provider re-fetch only through an explicit block-hash-scoped, retained-digest-checked, fail-closed cache-fill path; if no retained digest exists, the payload cannot satisfy that contract. Provider re-fetch never replaces selected replay facts that the docs require Postgres to retain. The runner does not re-open live intake, create or reserve backfill ranges, advance backfill range checkpoints, mutate backfill jobs, promote `canonical_head`, `safe_head`, or `finalized_head`, rebuild projections, write public API state, or expose a public `v1` route.[^bigname-replay-source][^bigname-cache-source]

Automatic normalized-event replay catch-up uses a single all-source chain cursor over persisted canonical raw facts and replays selected blocks in block order. It does not split catch-up into per-source-family cursors: cross-family adapters need registry, registrar, wrapper, resolver, and reverse-claim facts in the same chronological stream to produce non-overlapping identity intervals. Replay orchestration classifies every normalized-event producer as `stateless_raw_fact`, `stateful_closure_required`, or `contextual_dependency_required` before invoking adapter sync. `stateless_raw_fact` adapters may use block-hash/source-scoped replay because each emitted row is a pure function of the selected canonical raw fact, immutable manifest/source metadata for the range, chain lineage/canonicality, and deterministic decoder constants. For stateful and contextual adapters, automatic catch-up treats batching as physical IO only: log-count caps, scan guards, and chunk-block settings do not create restartable semantic boundaries for adapter history or dependency closure. Implemented full-closure replay uses canonical raw-log event candidate-count-bounded physical pages when those pages preserve whole-block boundaries and the global cursor still advances only after the latched closure target completes; adapter routing may further filter each page to watched or generic source events. Any larger database scan guard is only an implementation throughput guard and is not a fixed replay window. Without a documented durable adapter-state snapshot, a restarted stateful replay must resume from the retained closure boundary rather than from the last physical page. While this automatic replay cursor is incomplete, live polling may persist raw facts without running adapter-owned normalization so a single replay owner remains responsible for the selected closure. After the automatic replay cursor is complete for provider-configured chains, the indexer first runs a bounded post-replay live-adapter backlog pass over canonical raw-log blocks already persisted after the latched replay target, then live polling resumes adapter-owned normalization for newly persisted raw facts. That backlog pass derives live adapter scope from the persisted raw-log emitters in the selected block hashes, owns only its operational cursor and adapter-owned upserts, and does not widen the completed full-closure replay target. Source-scoped live and backlog adapter passes do not perform a full-source discovery carry-forward per selected block; discovery mutation is bounded to observations touched by the selected raw facts plus any descendant branch made unreachable by those touched observations. Repair remains replayable because those live writes are still deterministic upserts from retained raw facts, and a later closure replay can rederive the same normalized event identities. Source-scoped replay remains an explicit repair/backfill selector for bounded target sets, not the automatic catch-up default, and it is not deterministic state regeneration for stateful/contextual adapters unless the selected facts are closure-complete. The current runner fails closed for `stateful_closure_required` and `contextual_dependency_required` raw-fact replay unless a documented closure/dependency replay session exists.[^bigname-replay-source]

Selected-target replay scopes are operational scan bounds. For ENSv1 generic resolver-local events, replay may narrow which persisted raw logs the adapter resubmits, but the scope does not graduate coverage, mutate resolver profiles, suppress otherwise retained generic resolver observations, or make profile state the source of truth for observed selector/cache facts.[^bigname-replay-source][^bigname-coverage-source]

Replay does not delete stale `normalized_events`, purge rows derived from selected blocks, or replace existing payloads for an already persisted normalized-event identity. Existing identities refresh only through the storage upsert canonicality path; stale conflicting payloads stay a hard storage mismatch rather than being rewritten by replay. Raw facts and lineage stay immutable, projection rebuild stays downstream worker-owned, and API responses keep reading projections and execution output rather than the replay runner.[^bigname-replay-source][^bigname-projection-source]

## Atomicity boundary

The raw admission transaction boundary is one block. That transaction writes:[^bigname-atomicity-source]

- one `chain_lineage` header-anchor row for the admitted block
- optional `chain_header_audit` fields when auditable header retention is enabled
- hot raw transaction, receipt, and log facts needed for selected replay contracts
- optional cache metadata or digests for non-critical full block-scoped payloads when the retention contract keeps them
- any block-scoped call snapshots captured through the intake-owned raw-fact handoff
- normalized events emitted from those facts
- invalidation signals required by downstream workers

The canonical head pointer writes last inside that admission unit. Projection workers stay downstream and asynchronous, but they consume deterministic block-scoped invalidation and replay inputs through `projection_normalized_event_changes` and `projection_invalidations` so reorg repair stays reproducible. Normalized-event inserts and canonicality-state updates both append projection-consumable changes.[^bigname-atomicity-source][^bigname-projection-source]

## Traces, pending, optional capabilities

Pending and mempool indexing are a separate product surface. Trace and internal-call indexing are a separate capability plane: they depend on client-specific APIs and different operational budgets.[^bigname-support-boundary]

- The declared-state truth core does not require traces to be correct.
- If traces enable later, they persist as their own raw facts with the same block-hash anchoring and reorg semantics.
- Intake planning does not assume all providers expose the same trace APIs.

## Observability and tests

Minimum metrics: lag to canonical, safe, and finalized heads; reorg depth histogram; orphaned block rate; RPC latency and error rate by method; partial batch failure rate; recent-window cache hit and miss rate; backlog depth; replay and rewrite duration; raw-fact normalized-event replay duration and selected canonical block count.[^bigname-reorg-source][^bigname-replay-source]

Required failure drills: dropped subscription connection during a reorg; duplicate headers at the same height; missing parent gap that requires parent backfill; partial batch failures; crash and resume from a persisted checkpoint; crash and resume from a persisted backfill job range checkpoint; raw-fact normalized-event replay restart over the same bounded canonical selection as an upsert-only adapter resync whose selected replay facts come from persisted canonical raw facts (any explicit provider cache refill is block-hash-scoped, retained-digest-checked, fail-closed, and performs no checkpoint promotion); safe or finalized promotion lagging canonical intake.[^bigname-reorg-source][^bigname-backfill-source][^bigname-replay-source]

## Acceptance rules

The intake contract is acceptable when the storage, projection, API, and execution contracts below can all hold under reorg, replay, and bounded-storage operation:[^bigname-acceptance-source]

- Live notifications can be lost without losing correctness.
- The system reconciles short forks by hash and parent hash alone.
- Block-scoped data ingestion never depends on ambiguous number-only reads when a block-hash-scoped primitive exists.
- Raw facts are sufficient to rebuild canonical declared state after a reorg or decoder rewrite.
- Backfill reuses the same downstream semantics as live ingestion.
- Raw-fact normalized-event replay upserts normalized events only from persisted canonical selected replay facts without payload replacement, stale-row purge, projection rebuild, public API exposure, or chain/backfill checkpoint mutation.
- Any explicit replay cache refill uses provider re-fetch only as a block-hash-scoped, retained-digest-checked, fail-closed cache-fill path; missing digests, mismatched bytes, or unavailable historical payloads fail closed, and selected replay facts never depend on provider history.

---

[^bigname-canonicality-source]: Internal source: [`architecture.md`](architecture.md), [`storage.md` Invariants](storage.md#invariants), [`storage.md` Canonicality model](storage.md#canonicality-model), [`storage.md` Reorg repair](storage.md#reorg-repair), [`projections.md` Rules](projections.md#rules).
[^bigname-profile-source]: Internal source: [`manifests.md` File format and location](manifests.md#file-format-and-location), [`deployment.md` Fresh Server Compose](deployment.md#fresh-server-compose).
[^bigname-storage-scope]: Internal source: [`storage.md` Storage layers](storage.md#storage-layers), [`storage.md` Raw-log retention modes](storage.md#raw-log-retention-modes), [`storage.md` Evictable payload cache](storage.md#evictable-payload-cache), [`storage.md` Execution storage](storage.md#execution-storage).
[^bigname-execution-snapshot-source]: Internal source: [`execution.md` Resolution flow](execution.md#resolution-flow), [`execution.md` Cache identity and invalidation](execution.md#cache-identity-and-invalidation), [`storage.md` Execution storage](storage.md#execution-storage).
[^bigname-support-boundary]: Internal source: [`execution.md` Support boundary](execution.md#support-boundary), [`consumer-capabilities.md` Explicitly out of scope](consumer-capabilities.md#explicitly-out-of-scope), [`storage.md` Read-only inspection tooling](storage.md#read-only-inspection-tooling).
[^bigname-discovery-source]: Internal source: [`manifests.md` Discovery admission](manifests.md#discovery-admission), [`manifests.md` Watch-plan expansion](manifests.md#watch-plan-expansion), [`manifests.md` Capability policy](manifests.md#capability-policy), [`projections.md` Resolution](projections.md#resolution).
[^bigname-provider-source]: Internal source: [`deployment.md` Fresh Server Compose](deployment.md#fresh-server-compose), [`storage.md` Evictable payload cache](storage.md#evictable-payload-cache), [`storage.md` Canonicality model](storage.md#canonicality-model).
[^bigname-cache-source]: Internal source: [`storage.md` Evictable payload cache](storage.md#evictable-payload-cache), [`storage.md` Raw-log retention modes](storage.md#raw-log-retention-modes), [`storage.md` Replay semantics](storage.md#replay-semantics), [`storage.md` Backfill persistence](storage.md#backfill-persistence).
[^bigname-deployment-profile-source]: Internal source: [`deployment.md` Fresh Server Compose](deployment.md#fresh-server-compose), [`manifests.md` File format and location](manifests.md#file-format-and-location).
[^bigname-deployment-provider-source]: Internal source: [`deployment.md` Fresh Server Compose](deployment.md#fresh-server-compose), [`storage.md` Evictable payload cache](storage.md#evictable-payload-cache), [`storage.md` Repository ownership](storage.md#repository-ownership).
[^bigname-deployment-bootstrap-source]: Internal source: [`deployment.md` Fresh Server Compose](deployment.md#fresh-server-compose), [`storage.md` Backfill persistence](storage.md#backfill-persistence), [`storage.md` Backfill range checkpoint vs chain checkpoint](storage.md#backfill-range-checkpoint-vs-chain-checkpoint).
[^bigname-deployment-capacity-source]: Internal source: [`deployment.md` Fresh Server Compose](deployment.md#fresh-server-compose), [`storage.md` Backfill persistence](storage.md#backfill-persistence), [`storage.md` Evictable payload cache](storage.md#evictable-payload-cache).
[^bigname-lineage-source]: Internal source: [`storage.md` Canonicality model](storage.md#canonicality-model), [`storage.md` Reorg repair](storage.md#reorg-repair), [`storage.md` Raw-log retention modes](storage.md#raw-log-retention-modes).
[^bigname-head-source]: Internal source: [`storage.md` Canonicality model](storage.md#canonicality-model), [`storage.md` Backfill range checkpoint vs chain checkpoint](storage.md#backfill-range-checkpoint-vs-chain-checkpoint), [`api-v1.md` Snapshot selection](api-v1.md#snapshot-selection).
[^bigname-backfill-source]: Internal source: [`storage.md` Backfill persistence](storage.md#backfill-persistence), [`storage.md` Backfill range checkpoint vs chain checkpoint](storage.md#backfill-range-checkpoint-vs-chain-checkpoint), [`deployment.md` Fresh Server Compose](deployment.md#fresh-server-compose).
[^bigname-coverage-source]: Internal source: [`consumer-capabilities.md` Coverage notes](consumer-capabilities.md#coverage-notes), [`manifests.md` Capability policy](manifests.md#capability-policy), [`api-v1-routes.md` route catalog](api-v1-routes.md).
[^bigname-replay-source]: Internal source: [`storage.md` Replay semantics](storage.md#replay-semantics), [`projections.md` Replay status tracking](projections.md#replay-status-tracking), [`deployment.md` Fresh Server Compose](deployment.md#fresh-server-compose).
[^bigname-projection-source]: Internal source: [`projections.md` Invalidation](projections.md#invalidation), [`projections.md` Rebuild](projections.md#rebuild), [`storage.md` Projection storage rules](storage.md#projection-storage-rules).
[^bigname-reorg-source]: Internal source: [`storage.md` Canonicality model](storage.md#canonicality-model), [`storage.md` Reorg repair](storage.md#reorg-repair), [`projections.md` Invalidation](projections.md#invalidation), [`execution.md` Cache identity and invalidation](execution.md#cache-identity-and-invalidation).
[^bigname-execution-cache-source]: Internal source: [`execution.md` Cache identity and invalidation](execution.md#cache-identity-and-invalidation), [`storage.md` Execution storage](storage.md#execution-storage).
[^bigname-rewind-source]: Internal source: [`storage.md` Reorg repair](storage.md#reorg-repair), [`storage.md` Replay semantics](storage.md#replay-semantics), [`projections.md` Rewind and historical snapshots](projections.md#rewind-and-historical-snapshots), [`execution.md` Cache identity and invalidation](execution.md#cache-identity-and-invalidation).
[^bigname-time-travel-source]: Internal source: [`api-v1.md` Snapshot selection](api-v1.md#snapshot-selection), [`storage.md` Projection storage rules](storage.md#projection-storage-rules), [`projections.md` Rewind and historical snapshots](projections.md#rewind-and-historical-snapshots).
[^bigname-atomicity-source]: Internal source: [`storage.md` Table families and write ownership](storage.md#table-families-and-write-ownership), [`storage.md` Projection storage rules](storage.md#projection-storage-rules), [`projections.md` Invalidation](projections.md#invalidation).
[^bigname-acceptance-source]: Internal source: [`storage.md`](storage.md), [`projections.md`](projections.md), [`api-v1.md`](api-v1.md), [`execution.md`](execution.md).

[^subgraph-l10]: (upstream: .refs/ens_subgraph/subgraph.yaml:L10 @ ens_subgraph@723f1b6)
[^subgraph-l15]: (upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)
[^subgraph-l39]: (upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)
[^subgraph-l42]: (upstream: .refs/ens_subgraph/subgraph.yaml:L42 @ ens_subgraph@723f1b6)
[^subgraph-l44]: (upstream: .refs/ens_subgraph/subgraph.yaml:L44 @ ens_subgraph@723f1b6)
[^subgraph-ts-l134]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L134 @ ens_subgraph@723f1b6)
[^subgraph-ts-l230]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L230 @ ens_subgraph@723f1b6)
[^subgraph-ts-l238]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6)
[^subgraph-ts-l246]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6)
[^subgraph-ts-l252]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L252 @ ens_subgraph@723f1b6)
[^subgraph-ts-l259]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L259 @ ens_subgraph@723f1b6)

[^v1-ens-l12]: (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)
[^v1-ensreg-l89]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)
[^v1-ensreg-l174]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f)

[^v1-iaddr-l6]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f)
[^v1-iname-resolver-l5]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L5 @ ens_v1@91c966f)
[^v1-itext-l5]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f)
[^v1-itext-l10]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L10 @ ens_v1@91c966f)
[^v1-iaddr2-l6]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f)
[^v1-iaddress-l6]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f)
[^v1-itext2-l5]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f)
[^v1-iversionable-l5]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IVersionableResolver.sol:L5 @ ens_v1@91c966f)

[^v1-pres-l20]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^v1-pres-l31]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f)
[^v1-pres-l131]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f)
[^v1-pres-l150]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f)
[^v1-resolverbase-l17]: (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f)
[^v1-resolverbase-l21]: (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L21 @ ens_v1@91c966f)
[^v1-resolverbase-l22]: (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L22 @ ens_v1@91c966f)
[^v1-resolverbase-l23]: (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L23 @ ens_v1@91c966f)

[^v1-namewrapper-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f)
[^v1-nw-deploy-l200]: (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L200 @ ens_v1@91c966f)
[^v1-nw-deploy-l219]: (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L219 @ ens_v1@91c966f)
[^v1-nw-deploy-l238]: (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L238 @ ens_v1@91c966f)
[^v1-nw-deploy-l275]: (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L275 @ ens_v1@91c966f)

[^v1-publicresolver-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f)
[^v1-pres-deploy-l57]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L57 @ ens_v1@91c966f)
[^v1-pres-deploy-l76]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L76 @ ens_v1@91c966f)
[^v1-pres-deploy-l101]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L101 @ ens_v1@91c966f)
[^v1-pres-deploy-l157]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L157 @ ens_v1@91c966f)
[^v1-pres-deploy-l176]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L176 @ ens_v1@91c966f)
[^v1-pres-deploy-l357]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L357 @ ens_v1@91c966f)
[^v1-pres-deploy-l376]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L376 @ ens_v1@91c966f)

[^bn-readme-l22]: (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc)
[^bn-readme-l69]: (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)
[^bn-l1resolver-l13]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
[^bn-l1resolver-l154]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)
[^bn-l1resolver-l173]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc)
[^bn-l1resolver-l191]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)
[^bn-registry-l19]: (upstream: .refs/basenames/src/L2/Registry.sol:L19 @ basenames@1809bbc)
[^bn-registry-l132]: (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
[^bn-registry-l223]: (upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc)
[^bn-l2resolver-l22]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)

[^v2-deploy-root]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2 @ ens_v2@554c309)
[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309)
[^v2-deploy-ethrc]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)
[^v2-deploy-pres]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/PermissionedResolverImpl.json:L2 @ ens_v2@554c309)

[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309)
[^v2-iperm-l57]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L57 @ ens_v2@554c309)
[^v2-iperm-l67]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L67 @ ens_v2@554c309)
[^v2-iperm-l72]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L72 @ ens_v2@554c309)
[^v2-events-l49]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309)
[^v2-events-l59]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L59 @ ens_v2@554c309)
[^v2-events-l69]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309)
[^v2-events-l75]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309)
[^v2-pr-l451]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309)

[^v2-iperm-resolver-l14]: (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309)
[^v2-iperm-resolver-l56]: (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L56 @ ens_v2@554c309)

[^v2-eac-l19]: (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309)
[^v2-eac-l100]: (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L100 @ ens_v2@554c309)

[^ponder-reorg-l41]: (upstream: .refs/ponder/docs/pages/docs/indexing/overview.mdx:L41 @ ponder@c8f6935)
[^ponder-reorg-l44]: (upstream: .refs/ponder/docs/pages/docs/indexing/overview.mdx:L44 @ ponder@c8f6935)
[^ponder-reorg-l47]: (upstream: .refs/ponder/docs/pages/docs/indexing/overview.mdx:L47 @ ponder@c8f6935)
[^ponder-unfinalized-l71]: (upstream: .refs/ponder/packages/core/src/sync-realtime/index.ts:L71 @ ponder@c8f6935)
[^ponder-unfinalized-l118]: (upstream: .refs/ponder/packages/core/src/sync-realtime/index.ts:L118 @ ponder@c8f6935)
[^ponder-logs-l310]: (upstream: .refs/ponder/packages/core/src/sync-realtime/index.ts:L310 @ ponder@c8f6935)
[^ponder-cache-l82]: (upstream: .refs/ponder/docs/pages/docs/api-reference/ponder/database.mdx:L82 @ ponder@c8f6935)
[^ponder-cache-l87]: (upstream: .refs/ponder/docs/pages/docs/api-reference/ponder/database.mdx:L87 @ ponder@c8f6935)
[^graph-time-l112]: (upstream: .refs/graph_node/docs/implementation/time-travel.md:L112 @ graph_node@aefe173)
[^graph-prune-l3]: (upstream: .refs/graph_node/docs/implementation/pruning.md:L3 @ graph_node@aefe173)
[^graph-prune-l24]: (upstream: .refs/graph_node/docs/implementation/pruning.md:L24 @ graph_node@aefe173)
[^graph-prune-l28]: (upstream: .refs/graph_node/docs/implementation/pruning.md:L28 @ graph_node@aefe173)
[^graph-blockptr-l596]: (upstream: .refs/graph_node/chain/ethereum/src/ethereum_adapter.rs:L596 @ graph_node@aefe173)
[^graph-block-l1301]: (upstream: .refs/graph_node/chain/ethereum/src/ethereum_adapter.rs:L1301 @ graph_node@aefe173)
[^graph-receipts-l2334]: (upstream: .refs/graph_node/chain/ethereum/src/ethereum_adapter.rs:L2334 @ graph_node@aefe173)
[^graph-trace-l1086]: (upstream: .refs/graph_node/chain/ethereum/src/ethereum_adapter.rs:L1086 @ graph_node@aefe173)
[^graph-receipt-l2486]: (upstream: .refs/graph_node/chain/ethereum/src/ethereum_adapter.rs:L2486 @ graph_node@aefe173)
[^reth-tree-l61]: (upstream: .refs/reth/crates/engine/tree/src/lib.rs:L61 @ reth@194ece2)
[^reth-state-l25]: (upstream: .refs/reth/crates/engine/tree/src/tree/state.rs:L25 @ reth@194ece2)
[^reth-state-l31]: (upstream: .refs/reth/crates/engine/tree/src/tree/state.rs:L31 @ reth@194ece2)
[^reth-state-l35]: (upstream: .refs/reth/crates/engine/tree/src/tree/state.rs:L35 @ reth@194ece2)
[^reth-block-id-l81]: (upstream: .refs/reth/crates/storage/storage-api/src/block_id.rs:L81 @ reth@194ece2)
[^reth-block-id-l90]: (upstream: .refs/reth/crates/storage/storage-api/src/block_id.rs:L90 @ reth@194ece2)
[^reth-call-l326]: (upstream: .refs/reth/crates/rpc/rpc-eth-api/src/helpers/call.rs:L326 @ reth@194ece2)
[^reth-notify-l342]: (upstream: .refs/reth/crates/exex/exex/src/notifications.rs:L342 @ reth@194ece2)
[^reth-notify-l742]: (upstream: .refs/reth/crates/exex/exex/src/notifications.rs:L742 @ reth@194ece2)
[^reth-wal-l35]: (upstream: .refs/reth/crates/exex/exex/src/wal/mod.rs:L35 @ reth@194ece2)
[^reth-finished-l14]: (upstream: .refs/reth/crates/exex/types/src/finished_height.rs:L14 @ reth@194ece2)
[^reth-readme-l23]: (upstream: .refs/reth/README.md:L23 @ reth@194ece2)
