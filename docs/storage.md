# Storage Strategy

Status: Phase 0 baseline

This document freezes the internal persistence strategy enough for storage, intake, projection, and execution work to proceed in parallel.

## 1. Invariants

- raw facts are immutable
- projections are disposable and rebuildable
- canonicality is explicit, never inferred from "latest row wins"
- verified execution artifacts are durable facts, not ephemeral cache only
- one write owner exists per storage family

## 2. Storage Layers

The system of record is split into six layers:

1. `chain_lineage`: block ancestry, fork points, hash-first reconciliation, head promotion
2. `raw_facts`: blocks, transactions, receipts, logs, code hashes, fetched call snapshots
3. `manifests_and_discovery`: source manifests, discovered edges, rollout flags
4. `identity_and_events`: `NameSurface`, `SurfaceBinding`, resources, token lineage, normalized events
5. `projections`: current-state and collection read models
6. `execution`: traces, cache entries, persisted verified outcomes

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
| `manifest_*` | manifests/discovery | source manifests, declared contract admission, capability versions |
| `discovery_*` | manifests/discovery | canonical reachable contract graph and watch-plan expansion keyed by `contract_instance_id` |
| `name_surfaces`, `surface_bindings`, `resources`, `token_lineages` | adapters | stable identity anchors |
| `normalized_events` | adapters | append-only normalized protocol events |
| `projection_*` | projection workers | disposable read models |
| `execution_*` | execution workers | traces, cached answers, invalidations |

The API process is read-only against storage.

For ENSv1 identity rows and normalized authority / permission events, adapters are responsible for minting and reusing `resource_id`, `token_lineage_id`, and `surface_binding_id` according to the continuity rules above and for attaching normalized events to the authoritative `resource_id` in effect at that chain position. Projection workers consume those identity rows and normalized events; they do not infer alternate continuity or synthesize cross-resource permission carry on their own.

For ENSv2 identity rows and normalized event rows, adapters own the same boundary: they mint and reuse `resource_id`, `token_lineage_id`, and `surface_binding_id`, append `TokenResourceLinked`, `TokenRegenerated`, `SubregistryChanged`, `ParentChanged`, `AliasChanged`, permission events, and preimage observations from name-bearing events, and never write projection rows. Projection workers consume those events; they do not infer token-resource links, subregistry reachability, alias targets, wildcard coverage, EAC-derived effective powers, or exact-name support directly from raw logs, preimage observations, or manifest presence (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L30 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309).

At minimum, manifests/discovery persistence must carry:

- `contract_instances`: one row per stable `contract_instance_id` with chain, contract kind, and provenance; roots use the same identity family as other contract instances
- `contract_instance_addresses`: time-ranged address attributes keyed by `contract_instance_id` for lookup from raw facts and watch targets to source-graph identity; one `contract_instance_id` may carry multiple non-overlapping active ranges when the same address is re-admitted after an inactive gap
- `discovery_edges`: edges keyed by `edge_id` with `from_contract_instance_id`, `to_contract_instance_id`, `edge_kind`, active range, provenance, and canonicality
- any materialized watch-plan table keyed by `contract_instance_id` plus chain and range, including root start nodes keyed by the root `contract_instance_id`; raw address is a derived watch target, not the durable identity

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

## 9. Migration Rules

- schema changes land through checked-in migrations only
- append-only tables prefer additive changes over destructive rewrites
- projection tables may be recreated when the rebuild path already exists
- migrations that change a shared interface require the companion doc update first

## 10. Repository Ownership Implications

To keep parallel work safe:

- storage owns migrations and query primitives
- adapters own inserts into identity and normalized-event tables
- projection workers own materialized read models
- execution workers own trace and cache tables
- API code must not query raw-fact tables directly except for explicit audit endpoints
