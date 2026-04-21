# Storage Strategy

Status: Phase 0 baseline

This document freezes the internal persistence strategy enough for storage, intake, projection, and execution work to proceed in parallel.

## 1. Invariants

- raw facts are immutable
- projections are disposable and rebuildable
- canonicality is explicit, never inferred from "latest row wins"
- execution traces and execution steps are durable audit artifacts; cache outcomes are reusable only while their dependencies remain canonical
- one write owner exists per storage family

## 2. Storage Layers

The system of record is split into six layers:

1. `chain_lineage`: block ancestry, fork points, hash-first reconciliation, head promotion
2. `raw_facts`: blocks, transactions, receipts, logs, code hashes, fetched call snapshots
3. `manifests_and_discovery`: source manifests, discovered edges, rollout flags
4. `identity_and_events`: `NameSurface`, `SurfaceBinding`, resources, token lineage, normalized events
5. `projections`: current-state and collection read models
6. `execution`: durable traces and steps, `execution_cache_outcomes`, invalidation records

Only layers 1 through 5 are required to rebuild current declared state. Layer 6 is required to replay verified answers and explain them.

## 3. ID Strategy

### Deterministic text IDs

- `logical_name_id = "<namespace>:<normalized_name>"`

This is stable, human-auditable, and can be derived without database lookup.

### Opaque stable IDs

Use `uuid` for:

- `resource_id`
- `token_lineage_id`
- `contract_instance_id`
- `surface_binding_id`
- `execution_trace_id`

Rules:

- UUID values are internal identities, not user-generated strings
- `resource_id` and `token_lineage_id` must survive projection rebuilds
- token IDs, node hashes, and resolver addresses are attributes, not identity anchors

ENSv1 continuity rules for adapters:

- mint one `resource_id` per distinct ENSv1 authority anchor and reuse it if that exact anchor becomes authoritative again after a gap
- for this slice, direct registry-only control, registrar-backed registration, and wrapper-backed control are distinct ENSv1 authority anchors
- direct registry-only control has no active `token_lineage_id`
- mint one `token_lineage_id` per distinct tokenized ENSv1 anchor and reuse it if that same tokenized anchor becomes authoritative again
- transfer, renewal, fuse updates, expiry / grace changes, and permission or scope changes inside the same current anchor append normalized events against the current `resource_id` but do not mint new `resource_id`, `token_lineage_id`, or `surface_binding_id` rows
- wrap, unwrap, and re-registration close the old binding range only when the authoritative anchor changes; unwrap back to the same still-live pre-wrap registrar lease reuses the prior registrar `resource_id` and `token_lineage_id`
- when authority moves to a different anchor, close or reactivate binding continuity as above and attach subsequent authority- and permission-family normalized events to the successor `resource_id`
- adapters do not rewrite predecessor-resource normalized events or infer successor-resource permissions from them; any successor effective permission state must come from normalized events attached to that successor `resource_id`
- for ENSv1 direct-authority cases in this slice, write `SurfaceBinding.binding_kind = declared_registry_path`; do not use a different binding kind merely because the authority anchor changed between registry, registrar, and wrapper control
- `ens_v1_wrapper_l1` identity input is the admitted NameWrapper contract instance plus the wrapped node. Wrapper ownership, fuse, expiry, wrap, unwrap, resolver, and TTL observations update the active wrapper-backed resource or close/reactivate bindings according to the authority-anchor rules above; they do not create a separate migration resource unless a later doc-first wrapper / migration history slice admits that behavior (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L666 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L676 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022 @ ens_v1@91c966f).
- `ens_v1_resolver_l1` identity input is the admitted PublicResolver contract instance plus the node and record selector. Resolver addresses remain source-graph contract instances or time-ranged attributes, not `resource_id`s; resolver-local approvals and delegates attach to the active resolver-scoped permission rows and do not backfill registry or wrapper authority (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L38 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L44 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L78 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L97 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f).

ENSv2 continuity rules for adapters:

- key `resource_id` by `(chain_id, registry_contract_instance_id, upstream_eac_resource)` after observing the upstream permissioned-registry resource, not by current ERC1155 token ID; upstream exposes both `getResource(anyId)` and `getTokenId(anyId)` and emits `TokenResource(tokenId, resource)` when the token is associated with the resource (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L67 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L72 @ ens_v2@554c309)
- normalize `TokenResource(tokenId, resource)` as `TokenResourceLinked` and use it to attach token-lineage state to the current resource; absence of this event means the adapter must not guess a token/resource link from token ID shape alone (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L216 @ ens_v2@554c309)
- normalize `TokenRegenerated(oldTokenId, newTokenId)` as `TokenRegenerated`; keep the same `resource_id`, `token_lineage_id`, and `surface_binding_id`, update the current token ID attribute, and append the event against the already linked resource because upstream regenerates the token after role grants or revokes while leaving the resource unchanged (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L429 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461 @ ens_v2@554c309)
- mint a new `resource_id` when upstream creates a new EAC resource version for a label after unregister / re-register; upstream constructs resources from `eacVersionId` and token IDs from `tokenVersionId`, and unregister / re-register increments both counters rather than preserving the old permission scope (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L28 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L203 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L237 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L536 @ ens_v2@554c309)
- resolver-scoped permission resources belong to the admitted resolver contract instance plus upstream resolver EAC resource; `PermissionedResolver` creates name-, text-key-, and coin-type-specific EAC resources for setter permissions, so storage must preserve resolver scope instead of folding those grants into registry resources (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L70 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L239 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L257 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L282 @ ens_v2@554c309)
- store ENSv2 preimage observations only in adapter-owned identity/preimage and normalized-event families; name-bearing registry `LabelRegistered`, `LabelReserved`, and `ParentUpdated` events, registrar `NameRegistered` and `NameRenewed` events, and resolver `AliasChanged`, `NamedResource`, `NamedTextResource`, and `NamedAddrResource` events may append observations, but those observations must not insert projection rows, mutate manifest capability state, or promote the ENSv2 `sepolia-dev` exact-name profile beyond `status=unsupported`, `exhaustiveness=not_applicable`, and `unsupported_reason="ensv2 sepolia-dev exact-name profile is shadow-only"` (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L30 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L132 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L142 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L153 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)

`contract_instance_id` rules:

- mint a new `contract_instance_id` when a manifest-declared contract or discovery-admitted contract first enters the canonical source graph
- one admitted contract address on one chain maps to one stable `contract_instance_id` across all admission epochs
- reuse the same `contract_instance_id` when the same contract address remains admitted on the same chain and only manifest version, rollout state, code-hash observations, or edge activity changes
- if the same admitted contract address becomes active again after an inactive gap, reuse the prior `contract_instance_id` and record a new non-overlapping active range instead of minting another ID
- model proxy contracts and implementation contracts as separate contract instances; proxy implementation churn mutates discovery edges and active ranges, not the proxy ID
- if the watched contract's own admitted address changes, close the old instance active range and mint a new `contract_instance_id`; do not reuse the prior ID to represent the successor deployment, and represent any continuity between distinct instances with `migration` edges
- store contract addresses as time-ranged attributes for raw-fact lookup, log routing, and watch-plan materialization; addresses are never the primary key of the source graph
- roots use the same `contract_instance_id` rules as ordinary manifest-declared and discovery-admitted contracts

### Append-only event IDs

Use `bigint generated always as identity` for:

- raw fact rows
- normalized event rows
- projection job rows

## 4. Table Families And Write Ownership

| Family | Write owner | Notes |
| --- | --- | --- |
| `chain_*` | intake | lineage and canonical block graph |
| `raw_*` | intake | immutable blockchain and execution inputs |
| `backfill_*` | worker/backfill substrate | persisted backfill jobs, bounded range leases, and resumable range checkpoints; not chain head checkpoints |
| `manifest_*` | manifests/discovery | source manifests, declared contract admission, capability versions |
| `discovery_*` | manifests/discovery | canonical reachable contract graph and watch-plan expansion keyed by `contract_instance_id` |
| `name_surfaces`, `surface_bindings`, `resources`, `token_lineages` | adapters | stable identity anchors |
| `normalized_events` | adapters | append-only normalized protocol events |
| `projection_*` | projection workers | disposable read models |
| `execution_*` | execution workers; synchronous indexer/reorg repair for orphan-block cache outcome deletes only | durable traces and steps, normal `execution_cache_outcomes` writes, invalidation records |

The API process is read-only against storage.

Within the `execution_*` family, the only non-execution-worker write owner is synchronous indexer/reorg repair during chain reconciliation. That path may delete or invalidate reusable `execution_cache_outcomes` rows whose dependency set includes an orphaned block identity; it does not write execution traces, execution steps, normal execution outcomes, projections, API state, or manifest state.

For ENSv1 identity rows and normalized authority / permission events, adapters are responsible for minting and reusing `resource_id`, `token_lineage_id`, and `surface_binding_id` according to the continuity rules above and for attaching normalized events to the authoritative `resource_id` in effect at that chain position. Projection workers consume those identity rows and normalized events; they do not infer alternate continuity or synthesize cross-resource permission carry on their own.

For ENSv1 wrapper and resolver Phase 4 rows, adapters may append normalized identity, authority, permission, resolver-change, record, preimage, fuse, and expiry events from the admitted NameWrapper and PublicResolver families. They must not write projection rows, mutate manifest capability state, infer route coverage graduation, or persist wrapper upgrade / migration history from the wrapper upgrade path without a later doc-first admission of that history surface (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L479 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L500 @ ens_v1@91c966f).

For ENSv2 identity rows and normalized event rows, adapters own the same boundary: they mint and reuse `resource_id`, `token_lineage_id`, and `surface_binding_id`, append `TokenResourceLinked`, `TokenRegenerated`, `SubregistryChanged`, `ParentChanged`, `AliasChanged`, permission events, and preimage observations from name-bearing events, and never write projection rows. Projection workers consume those events; they do not infer token-resource links, subregistry reachability, alias targets, wildcard coverage, EAC-derived effective powers, or exact-name support directly from raw logs, preimage observations, or manifest presence (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L30 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309).

Raw-fact normalized-event replay does not introduce a new storage owner. The indexer-owned operational runner may select bounded canonical raw facts and ask the adapter-owned `normalized_events` boundary to perform an upsert-only resync for the corresponding rows; it must not let storage helpers, projections, API code, or inspection tooling synthesize normalized events directly. Replay does not delete stale `normalized_events` or replace existing payloads for an already persisted normalized-event identity; the storage upsert path inserts absent rows and refreshes canonicality for matching identities, while conflicting payloads remain mismatches. Replay must not mutate `chain_*`, `raw_*`, `backfill_*`, `projection_*`, `execution_*`, manifests, discovery rows, public API state, or checkpoint promotion state.

At minimum, manifests/discovery persistence must carry:

- `contract_instances`: one row per stable `contract_instance_id` with chain, contract kind, and provenance; roots use the same identity family as other contract instances
- `contract_instance_addresses`: time-ranged address attributes keyed by `contract_instance_id` for lookup from raw facts and watch targets to source-graph identity; one `contract_instance_id` may carry multiple non-overlapping active ranges when the same address is re-admitted after an inactive gap
- `discovery_edges`: edges keyed by `edge_id` with `from_contract_instance_id`, `to_contract_instance_id`, `edge_kind`, active range, provenance, and canonicality
- any materialized watch-plan table keyed by `contract_instance_id` plus chain and range, including root start nodes keyed by the root `contract_instance_id`; raw address is a derived watch target, not the durable identity

At minimum, backfill persistence must carry:

- `backfill_jobs`: one row per bounded backfill job with selected profile, chain, selector kind, resolved source identity, scan mode, declared range start and end, idempotency key, lifecycle status, failure metadata, and timestamp metadata
- `backfill_ranges`: child rows or equivalent range records with declared range bounds, next checkpoint, lease owner, lease token, lease expiry, attempt counters, lifecycle status, failure metadata, and timestamp metadata
- monotonic helper-owned checkpoint fields that allow a worker to resume after crash without widening the original range or reclassifying already admitted facts

Backfill source selector storage freezes the job identity fields used by the source-scoped runner:

- `selector_kind`: one of `whole_active_watched_chain`, `source_family`, or `watched_target_set`
- `source_family`: the requested family string for `selector_kind=source_family`, otherwise `null`
- `requested_watched_targets`: the caller-supplied watched target identities for `selector_kind=watched_target_set`, otherwise an empty array
- `requested_watched_targets[*].contract_instance_id`
- `selected_targets`: the resolved materialized target set sorted by `source_family`, `contract_instance_id`, normalized address, effective target range start, and effective target range end
- `selected_targets[*].source_family`
- `selected_targets[*].contract_instance_id`
- `selected_targets[*].address`
- `selected_targets[*].effective_from_block`
- `selected_targets[*].effective_to_block`
- `source_identity_hash`: a digest of `selector_kind`, `source_family`, `requested_watched_targets`, and `selected_targets`; the canonical selector payload remains authoritative if a hash collision or payload mismatch is detected

The selected target range fields are the intersection of the watched target's active range with the job's finite declared block range. `effective_to_block` is finite for every persisted selected target because backfill jobs are finite at creation time.

Backfill job and range checkpoint rows are operational state. They do not replace `chain_lineage`, do not define canonicality, and do not promote `canonical_head`, `safe_head`, or `finalized_head`.

## 5. Partitioning Baseline

Start with partitioning on the highest-volume append-only tables:

- `raw_blocks`
- `raw_transactions`
- `raw_receipts`
- `raw_logs`
- `normalized_events`
- `execution_steps`

Partition keys:

- `chain_id`
- block-number range

Current-state projection tables start unpartitioned unless measurements prove otherwise.

## 6. Canonicality Model

`chain_lineage` persists the recent reconciled block window keyed by `(chain_id, block_hash)` and carries the fields needed to recover canonical ancestry:

- `parent_hash`
- `block_number`
- `timestamp`
- checkpoint-promotion state
- integrity fields needed for audits and replay

Every fact-derived row that can be invalidated by reorg carries:

- `chain_id`
- `block_number`
- `block_hash`
- `canonicality_state`
- `observed_at`

`canonicality_state` values:

- `observed`
- `canonical`
- `safe`
- `finalized`
- `orphaned`

Rules:

- block hash is the identity anchor; block number is position only
- fork detection marks affected rows `orphaned`; it does not delete them
- projection rebuilds read rows that are `canonical`, `safe`, or `finalized` by default
- history and audit tools may opt into `observed` and `orphaned` rows explicitly
- safe and finalized promotion is monotonic per chain

Execution cache rows follow the same hash-first canonicality rule. When reorg repair marks a block identity `orphaned`, synchronous indexer/reorg repair invalidates or deletes any reusable `execution_cache_outcomes` row for verified resolution or verified primary-name readback whose dependency set includes that `(chain_id, block_hash)` or a boundary resolved through it. The invalidation makes the cached outcome ineligible for reuse; it must not delete raw facts, execution traces, execution steps, trace attachments, or any execution-owned audit artifact.

Reusable `execution_cache_outcomes` rows must carry dependencies tied to explicit block-hash-bearing chain positions or boundaries. Rows for verified resolution or verified primary-name readback that lack those dependencies fail closed; rows for request types explicitly documented as outside this reorg invalidation surface remain out of scope rather than being treated as reorg-safe by omission.

Backfill range checkpoints are separate from canonicality checkpoints. Advancing or completing a backfill job records only that bounded fetch/resume work reached a position in its declared range; it must not change any `canonicality_state` value and must not advance `canonical_head`, `safe_head`, or `finalized_head`.

Read-only canonicality inspection uses storage audit helpers over `chain_lineage`, raw fact tables, and `normalized_events`. The worker single-block inspection contract remains `bigname-worker inspect canonicality --chain-id <id> --block-hash <hash>` and resolves one `(chain_id, block_hash)`. For that requested block hash, helpers may report whether a stored lineage row exists and, for stored rows, block lineage, parent hash, block number, canonicality state, raw fact counts, and normalized-event counts.

Read-only stored lineage range inspection is worker-owned operational tooling over `chain_lineage`. The bounded command `bigname-worker inspect stored-lineage-range` lists only lineage rows already stored for the requested chain and finite block range, ordered stably by `(block_number, block_hash)` unless a later doc-first contract adds another explicit order. It renders stable JSON per observed block with chain id, block number, block hash, parent hash, canonicality state, timestamp, and any stored promotion markers; nullable stored fields render as `null` rather than disappearing. It must not infer missing heights, gaps, span-wide canonicality, aggregate finality, or completeness for the requested range. It must not mutate lineage, raw facts, normalized events, projections, execution cache rows, backfill jobs, backfill range checkpoints, or `canonical_head`, `safe_head`, or `finalized_head`.

Read-only backfill job inspection uses storage audit helpers over `backfill_jobs` and `backfill_ranges`. The worker inspection contract is job-id only: `bigname-worker inspect backfill-job --backfill-job-id <id>` resolves one persisted job and its child ranges. It renders stable JSON with the job lifecycle, declared range, selector kind, resolved source identity, idempotency key, timestamp metadata, failure metadata, and a `ranges` array sorted by range bounds and range id. Each range object includes lifecycle status, declared bounds, range checkpoint, lease owner/token/expiry, attempt count, timestamp metadata, and failure metadata. Nullable lease, completion, and failure fields must render as `null` or an empty metadata object rather than disappearing. The command is read-only: it must not reserve ranges, refresh leases, advance checkpoints, complete or fail jobs, mutate chain lineage, mutate raw facts, update projections, update execution cache rows, or promote `canonical_head`, `safe_head`, or `finalized_head`.

## 7. Projection Storage Rules

Every current-state projection row carries:

- provenance pointers
- manifest version
- relevant chain positions
- canonicality summary
- last recomputed timestamp

Projection tables may be truncated and rebuilt from canonical facts plus normalized events.

## 8. Execution Artifact Storage

Persist small execution payloads inline in Postgres:

- request metadata
- response digests
- decoded final values
- failure reasons

Persist large payloads in object storage addressed by SHA-256 digest:

- CCIP payload bodies
- large metadata responses
- trace attachments

Postgres stores the digest, size, content type, and object key.

The execution storage boundary separates durable audit artifacts from cache reuse. `execution_traces` and `execution_steps` preserve what was executed and why; normal `execution_cache_outcomes` writes record whether a verified outcome can be reused under its request key, manifest versions, and block-hash-bearing dependency boundaries. Phase 9 reorg invalidation updates cache eligibility only through the synchronous indexer/reorg repair exception and does not promote ENSv2 exact-name support, widen verified execution support, or graduate any manifest capability.

Worker-owned execution trace inspection reads only persisted `execution_traces`, `execution_steps`, and trace attachment metadata needed to render stable operational JSON for one stored trace. Inspection helpers must not execute fresh calls, read adapter internals, synthesize declared topology, mutate `execution_cache_outcomes`, write projections, update manifests or discovery rows, or expose a public `v1` route.

## 9. Migration Rules

- schema changes land through checked-in migrations only
- append-only tables prefer additive changes over destructive rewrites
- backfill job and range checkpoint storage lands as additive `backfill_*` tables or additive columns; it must not overload `chain_lineage`, projection job state, or public API tables
- projection tables may be recreated when the rebuild path already exists
- migrations that change a shared interface require the companion doc update first

## 10. Repository Ownership Implications

To keep parallel work safe:

- storage owns migrations and query primitives
- storage owns backfill job/range helper primitives for idempotent create, reserve, advance, complete, and fail transitions
- worker/backfill code owns operational writes to `backfill_*` through those helpers
- adapters own inserts into identity and normalized-event tables
- projection workers own materialized read models
- execution workers own trace and step writes plus normal cache outcome writes
- synchronous indexer/reorg repair owns only `execution_cache_outcomes` deletes or invalidations tied to orphaned block dependencies
- raw-fact normalized-event replay is indexer-owned orchestration over the adapter-owned `normalized_events` boundary; it reads persisted canonical raw facts and may upsert only the corresponding `normalized_events` without stale-row purge or payload replacement
- API code must not query raw-fact tables directly except for explicit audit endpoints
- canonicality, raw-fact, and stored lineage range inspection tooling is worker-owned, read-only operational tooling over storage audit helpers; it does not create a public `v1` route, infer missing lineage, or bypass the API boundary for user-facing reads
- backfill job inspection tooling is worker-owned, read-only operational tooling over `backfill_*`; it does not create a public `v1` route, mutate operational state, or bypass API read boundaries for user-facing data
- execution trace inspection tooling is worker-owned, read-only operational tooling over `execution_*`; it does not create a public `v1` route, execute fresh verification, mutate cache eligibility, or bypass the public explain-route boundary
