# Storage Strategy

Status: Phase 0 baseline

This document freezes the internal persistence strategy enough for storage, intake, projection, and execution work to proceed in parallel.

## 1. Invariants

- durable raw facts are immutable; evictable full-payload cache entries are not system-of-record facts
- projections are disposable and rebuildable
- canonicality is explicit, never inferred from "latest row wins"
- execution traces and execution steps are durable audit artifacts; cache outcomes are reusable only while their dependencies remain canonical
- one write owner exists per storage family

## 2. Storage Layers

The system of record is split into six layers:

1. `chain_lineage`: block ancestry, fork points, hash-first reconciliation, head promotion
2. `raw_facts`: hot indexed replay facts, selected/admitted target logs, code hashes, fetched call snapshots, and compact payload-cache metadata
3. `manifests_and_discovery`: source manifests, discovered edges, rollout flags
4. `identity_and_events`: `NameSurface`, `SurfaceBinding`, resources, token lineage, normalized events
5. `projections`: current-state and collection read models
6. `execution`: durable traces and steps, `execution_cache_outcomes`, invalidation records

Only layers 1 through 5 are required to rebuild current declared state. Layer 6 is required to replay verified answers and explain them.

Worker-owned manifest/proxy alert observations are an operational persistence family alongside those truth layers. They record audit findings for drift and proxy implementation changes, but they are not manifest truth, discovery admission, projection state, public API state, or adapter-owned normalized events.

Postgres is the hot indexed and replay-focused store, not an archive-style raw corpus. It retains durable replay and audit facts:

- lineage and header anchors needed to reconcile forks, prove ancestry, promote checkpoints, and audit canonicality
- selected/admitted target logs and the minimal transaction, receipt, and block fields needed to decode those logs, route them through adapters, rebuild normalized events, or reproduce execution outputs
- block-scoped call snapshots and enrichments only when they are part of a retained replay contract for normalized events, projections, or execution artifacts
- code-hash observations and discovery/proxy evidence needed by manifests, adapter routing, or audit tooling
- compact metadata and optional digests for full payloads that were fetched as cache but are not replay-critical hot rows

Large/full block payloads and non-indexed transaction, receipt, or block bodies are evictable cache by default once the durable replay facts have been extracted. They may live inline during a hot window, in local/provider cache, in hash-addressed object storage, or not be retained at all. Hash-addressed cold storage is required only for payload classes that a doc-first policy explicitly declares durable. If metadata is retained for an evictable payload, it should be stable enough to explain what was fetched, such as payload kind, chain id, block hash/number where block-scoped, optional digest, size, content type or encoding, source observation metadata, observed time, and canonicality state when applicable.

Historical backfill does not turn empty blocks into hot payload archives. For a
block with no selected target logs/facts and no replay-required enrichment, the
durable storage contract is limited to lineage/header anchors and any compact
audit metadata required by the selected retention policy. Full block bodies,
receipt bundles, transaction bundles, and payload-cache bytes for those empty
historical blocks remain evictable or absent unless a later doc-first policy
declares that payload class durable.

Provider re-fetch is an explicit, fallible cache-fill path. For block-scoped payloads, it must be block-hash-scoped, verify the retained digest before any bytes are used, and fail closed if the digest is absent, the digest mismatches, or the provider cannot serve the exact historical payload. Provider re-fetch is not a substitute for retaining selected replay facts, lineage, normalized events, execution artifacts, or orphaned-branch audit truth.

Local execution-client storage, including a same-host Reth database/static-file
store, is a provider/cache substrate rather than a new storage family. Client
table keys, row cursors, static-file offsets, or data-directory paths may appear
only in operational source metadata or evictable cache metadata. They are not
durable `raw_fact_ref` identities, normalized-event provenance, projection
inputs, or replacements for selected raw logs, minimal transaction/receipt
facts, lineage/header anchors, code-hash observations, call snapshots,
execution artifacts, or orphaned-branch audit truth retained in Postgres.

Retention windows and compaction cadence are operational policy. They do not change route coverage, API consistency semantics, manifest capability flags, rollout status, or consumer-replacement graduation.

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
- `ENSRegistryOld` identity input, if admitted, is a migration-aware input to `ens_v1_registry_l1`, not a separate current registry owner and not a latest-log-wins stream. Adapters may attach old-registry owner, child, TTL, and resolver observations to current topology only while the target node remains unmigrated; once a current-registry `NewOwner` marks that subnode migrated, later old-registry observations for that node remain raw/audit facts and must not rewrite the active `resource_id`, `surface_binding_id`, node resolver binding, TTL, child edge, or projection input. The root resolver exception may update only the `ROOT_NODE` resolver binding and resolver-discovery input. The current registry fallback contract reads old owner, resolver, and TTL only when the current registry has no record, while the pinned subgraph marks current `NewOwner` output as migrated and suppresses old-registry handlers except for root resolver (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L29 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L40 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L153 @ ens_v1@91c966f) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L134 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6).

ENSv2 continuity rules for adapters:

- key `resource_id` by `(chain_id, registry_contract_instance_id, upstream_eac_resource)` after observing the upstream permissioned-registry resource, not by current ERC1155 token ID; upstream exposes both `getResource(anyId)` and `getTokenId(anyId)` and emits `TokenResource(tokenId, resource)` when the token is associated with the resource (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L67 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L72 @ ens_v2@554c309)
- normalize `TokenResource(tokenId, resource)` as `TokenResourceLinked` and use it to attach token-lineage state to the current resource; absence of this event means the adapter must not guess a token/resource link from token ID shape alone (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L216 @ ens_v2@554c309)
- normalize `TokenRegenerated(oldTokenId, newTokenId)` as `TokenRegenerated`; keep the same `resource_id`, `token_lineage_id`, and `surface_binding_id`, update the current token ID attribute, and append the event against the already linked resource because upstream regenerates the token after role grants or revokes while leaving the resource unchanged (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L429 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461 @ ens_v2@554c309)
- mint a new `resource_id` when upstream creates a new EAC resource version for a label after unregister / re-register; upstream constructs resources from `eacVersionId` and token IDs from `tokenVersionId`, and unregister / re-register increments both counters rather than preserving the old permission scope (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L28 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L203 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L237 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L536 @ ens_v2@554c309)
- resolver-scoped permission resources belong to the admitted resolver contract instance plus upstream resolver EAC resource; `PermissionedResolver` creates name-, text-key-, and coin-type-specific EAC resources for setter permissions, so storage must preserve resolver scope instead of folding those grants into registry resources (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L70 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L239 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L257 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L282 @ ens_v2@554c309)
- store ENSv2 preimage observations only in adapter-owned identity/preimage and normalized-event families; name-bearing registry `LabelRegistered`, `LabelReserved`, and `ParentUpdated` events, registrar `NameRegistered` and `NameRenewed` events, and resolver `AliasChanged`, `NamedResource`, `NamedTextResource`, and `NamedAddrResource` events may append observations, but those observations must not insert projection rows, mutate manifest capability state, or promote public exact-name support. ENSv2 exact-name support may be read as supported only from the selected `sepolia-dev` manifest root when `ens_v2_registrar_l1` carries `exact_name_profile = "supported"`; raw logs, preimage facts, active rollout, and backfill completion do not graduate any other profile or capability (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L30 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L132 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L142 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L153 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)

`contract_instance_id` rules:

- mint a new `contract_instance_id` when a manifest-declared contract or discovery-admitted contract first enters the canonical source graph
- one admitted contract address on one chain maps to one stable `contract_instance_id` across all admission epochs
- reuse the same `contract_instance_id` when the same contract address remains admitted on the same chain and only manifest version, rollout state, code-hash observations, or edge activity changes
- if the same admitted contract address becomes active again after an inactive gap, reuse the prior `contract_instance_id` and record a new non-overlapping active range instead of minting another ID
- model proxy contracts and implementation contracts as separate contract instances; proxy implementation churn mutates discovery edges and active ranges, not the proxy ID
- if the watched contract's own admitted address changes, close the old instance active range and mint a new `contract_instance_id`; do not reuse the prior ID to represent the successor deployment, and represent any continuity between distinct instances with `migration` edges
- store contract addresses as time-ranged attributes for raw-fact lookup, log routing, and watch-plan materialization; addresses are never the primary key of the source graph
- roots use the same `contract_instance_id` rules as ordinary manifest-declared and discovery-admitted contracts
- ENSv1 and Basenames dynamic resolver discovery must keep contract admission separate from node resolver binding state. Each canonical nonzero registry-observed resolver address resolves to a stable `contract_instance_id` under the relevant resolver source family when admitted by the resolver discovery rule, while node-to-resolver bindings record which nodes currently select that resolver. Zero-address resolver observations close only the affected node-to-resolver binding; they do not delete prior contract instances or resolver-local facts, and they close a resolver discovery edge or derived watch target only when no active canonical node binding or direct manifest admission still requires that resolver. Resolver addresses remain source-graph contract instances or time-ranged attributes, not `resource_id`s (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc).

### Append-only event IDs

Use `bigint generated always as identity` for:

- raw fact rows
- normalized event rows
- projection job rows

## 4. Table Families And Write Ownership

| Family | Write owner | Notes |
| --- | --- | --- |
| `chain_*` | intake | lineage and canonical block graph |
| `raw_*` | intake | immutable hot replay facts plus payload-cache metadata for blockchain and execution inputs |
| `backfill_*` | worker/backfill substrate | persisted backfill jobs, bounded range leases, and resumable range checkpoints; not chain head checkpoints |
| `normalized_replay_*` | indexer/replay orchestration | operational replay cursors only; not canonicality, backfill, adapter, projection, or API state |
| `manifest_*` | manifests/discovery | source manifests, declared contract admission, capability versions |
| `discovery_*` | manifests/discovery | canonical reachable contract graph and watch-plan expansion keyed by `contract_instance_id` |
| `manifest_alert_*` | worker/audit | persisted manifest-drift and proxy-alert observations; operational only, not manifest truth or public API state |
| `name_surfaces`, `surface_bindings`, `resources`, `token_lineages` | adapters | stable identity anchors |
| `normalized_events` | adapters | append-only normalized protocol events |
| `projection_*` | projection workers | disposable read models |
| `execution_*` | execution workers; synchronous indexer/reorg repair for orphan-block cache outcome deletes only | durable traces and steps, normal `execution_cache_outcomes` writes, invalidation records |

The API process is read-only against storage.

Within the `execution_*` family, the only non-execution-worker write owner is synchronous indexer/reorg repair during chain reconciliation. That path may delete or invalidate reusable `execution_cache_outcomes` rows whose dependency set includes an orphaned block identity; it does not write execution traces, execution steps, normal execution outcomes, projections, API state, or manifest state.

For ENSv1 identity rows and normalized authority / permission events, adapters are responsible for minting and reusing `resource_id`, `token_lineage_id`, and `surface_binding_id` according to the continuity rules above and for attaching normalized events to the authoritative `resource_id` in effect at that chain position. Projection workers consume those identity rows and normalized events; they do not infer alternate continuity or synthesize cross-resource permission carry on their own.

For ENSv1 old-registry admission, adapters own the migration guard before a raw old-registry log can become a topology-changing normalized event. Storage helpers, projection workers, and API reads must not merge old and current registry rows by block order alone; canonicality-aware replay re-applies the same migrated-node rule and root resolver exception before writing current-state projections (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6).

For ENSv1 wrapper and resolver Phase 4 rows, adapters may append normalized identity, authority, permission, resolver-change, record, preimage, fuse, and expiry events from the admitted NameWrapper and PublicResolver families. They must not write projection rows, mutate manifest capability state, infer route coverage graduation, or persist wrapper upgrade / migration history from the wrapper upgrade path without a later doc-first admission of that history surface (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L479 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L500 @ ens_v1@91c966f).

For ENSv1 and Basenames dynamic resolver discovery, manifests/discovery owns the `contract_instance_id` and discovery-edge admission created from registry `NewResolver` observations, while adapter/projection state owns the node-to-resolver binding lifetime. Resolver adapters may append typed resolver-local normalized events only after the emitter address resolves to a direct manifest-admitted or resolver-discovery-admitted contract instance in the relevant resolver source family and that instance has supported resolver-profile admission for the emitted fact family; projection workers and API reads must surface explicit unsupported or gap state when the current resolver target has not reached those boundaries (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc).

ENSv1 resolver-profile state is separate from contract-instance admission. For discovered ENSv1 resolver instances, Phase 4 supports only the PublicResolver-compatible profile for the relevant record, record-version, and resolver-local authorization fact families. Profile admission state is keyed conceptually by `contract_instance_id`, source family, profile name, fact family, and active range; it records whether the instance is `supported`, `pending`, or `unsupported`, plus provenance such as the stored code-hash fact or proxy / implementation edge used for the decision. This freeze does not require a new profile-fact table: until a later doc-first storage family exists, the state may be derived from existing discovery provenance, normalized resolver-discovery events, and code-hash / proxy-edge facts. Unknown dynamic resolvers remain admitted watch targets only and must retain explicit derived `pending` or `unsupported` profile state until a later doc-first admission supports them. PublicResolver compatibility is anchored to the upstream PublicResolver mixin surface, ERC165 support, and ResolverBase record-versioning; Basenames profile admission is frozen separately below (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L23 @ ens_v1@91c966f).

Basenames resolver-profile state is also separate from contract-instance admission. For discovered Base-side resolver instances, Phase 8 supports only the `L2Resolver`-compatible profile for the relevant resolver-local record and authorization fact families. Profile admission state uses the same conceptual key shape as ENSv1, keyed by `contract_instance_id`, source family, profile name, fact family, and active range, and it remains derived from existing discovery provenance, normalized resolver-discovery events, stored code-hash / proxy-edge facts, ERC165 evidence, ABI-family admission, or supported resolver-event evidence until a later doc-first storage family exists. This freeze does not require a new profile-fact table, storage migration, shared enum, manifest schema field, or API route widening. Unknown dynamic Base resolvers remain admitted watch targets only and must retain explicit derived `pending` or `unsupported` profile state until a later doc-first admission supports them. The Basenames gate is separate from ENSv1 PublicResolver-compatible admission, the Ethereum Mainnet `L1Resolver` transport / execution families, and offchain-gateway admission (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc).

For ENSv2 identity rows and normalized event rows, adapters own the same boundary: they mint and reuse `resource_id`, `token_lineage_id`, and `surface_binding_id`, append `TokenResourceLinked`, `TokenRegenerated`, `SubregistryChanged`, `ParentChanged`, `AliasChanged`, permission events, and preimage observations from name-bearing events, and never write projection rows. Projection workers consume those events; they do not infer token-resource links, subregistry reachability, alias targets, wildcard coverage, EAC-derived effective powers, or exact-name support directly from raw logs, preimage observations, or manifest presence (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L30 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309).

Raw-fact normalized-event replay does not introduce a new event storage owner. The indexer-owned operational runner may select bounded canonical raw facts and ask the adapter-owned `normalized_events` boundary to perform an upsert-only resync for the corresponding rows; it may advance only its own `normalized_replay_*` operational cursor while doing so. It must not let storage helpers, projections, API code, or inspection tooling synthesize normalized events directly. Replay reads canonical durable hot facts first. It may use a retained durable cold payload only when an explicitly retained replay contract requires that payload. For block-scoped payloads, it may use provider re-fetch only through an explicit block-hash-scoped, retained-digest-checked, fail-closed cache-fill path; if no retained digest exists, the payload cannot satisfy that contract. Provider re-fetch must not replace selected replay facts that the docs require Postgres to retain. Replay does not delete stale `normalized_events` or replace existing payloads for an already persisted normalized-event identity; the storage upsert path inserts absent rows and refreshes canonicality for matching identities, while conflicting payloads remain mismatches. Replay must not mutate `chain_*`, `raw_*`, `backfill_*`, `projection_*`, `execution_*`, manifests, discovery rows, public API state, or checkpoint promotion state.

At minimum, manifests/discovery persistence must carry:

- `contract_instances`: one row per stable `contract_instance_id` with chain, contract kind, and provenance; roots use the same identity family as other contract instances
- `contract_instance_addresses`: time-ranged address attributes keyed by `contract_instance_id` for lookup from raw facts and watch targets to source-graph identity; one `contract_instance_id` may carry multiple non-overlapping active ranges when the same address is re-admitted after an inactive gap, and manifest-declared address ranges may carry nullable inclusive `start_block` metadata where the manifest supplied it
- `discovery_edges`: edges keyed by `edge_id` with `from_contract_instance_id`, `to_contract_instance_id`, `edge_kind`, active range, provenance, and canonicality
- resolver-profile admission state, when present: status and provenance keyed conceptually by `contract_instance_id`, source family, supported profile, fact family, and active range; it may be derived from existing discovery / normalized-event / code-hash / proxy-edge material until a later doc-first storage family exists, and it gates resolver-local normalized-event consumption but is not manifest truth, a capability flag, or public coverage state
- any materialized watch-plan table keyed by `contract_instance_id` plus chain and range, including root start nodes keyed by the root `contract_instance_id`; raw address is a derived watch target, not the durable identity, and an omitted `start_block` is persisted as unknown/null rather than converted to block zero or a job start

The worker-owned `manifest_alert_*` family persists manifest/proxy alert observations produced by the audit job. At minimum it must carry an observation identity, observation kind (`manifest_drift` or `proxy_implementation_drift`), lifecycle status, manifest version, source family, chain, contract-instance references, nullable proxy / implementation edge references, expected and observed code-hash or implementation-edge material, derived watch-plan metadata, first/last observed timestamps, and nullable remediation metadata. Writing this family must not write adapter-owned `normalized_events`, mutate manifest truth, mutate discovery admission, admit contracts, rewrite discovery edges, change capability flags, update watch-plan inputs, write projections, expose public API state, or claim consumer replacement. A proxy implementation observation preserves the proxy `contract_instance_id`; implementation churn is represented by an observed or admitted proxy / implementation edge, not by minting a replacement proxy identity.

Read-only manifest-drift and proxy-alert inspection reads the persisted worker-owned alert observations and renders operational JSON only. Inspection helpers may join stored alert rows to manifest/discovery identifiers, code-hash facts, proxy / implementation edges, and derived watch-target metadata, but they must not fetch fresh chain state, create alert rows, mutate alert lifecycle state, mutate manifest truth, mutate discovery admission, admit contracts, change capability flags, rewrite discovery edges, update watch-plan inputs, write projections, expose public API state, or claim consumer replacement.

At minimum, backfill persistence must carry:

- `backfill_jobs`: one row per bounded backfill job with selected profile, chain, selector kind, resolved source identity, scan mode, declared range start and end, idempotency key, lifecycle status, failure metadata, and timestamp metadata
- `backfill_ranges`: child rows or equivalent range records with declared range bounds, next checkpoint, lease owner, lease token, lease expiry, attempt counters, lifecycle status, failure metadata, and timestamp metadata
- monotonic helper-owned checkpoint fields that allow a worker to resume after crash without widening the original range or reclassifying already admitted facts

Operational finalized catch-up uses these same `backfill_*` families. It may
create many finite chunks, but each chunk must preserve one immutable job shape
and idempotency key. Capacity checks are part of the chunk lifecycle: before
range work starts, the worker must check current Postgres size, writable free
disk, and any configured object-cache budget, and record an explicit failure or
paused state in existing lifecycle/failure metadata when capacity is
insufficient. Capacity failures must not mutate raw facts, canonicality state,
selected target identity, chain checkpoints, projections, manifests, discovery,
or public API state.

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

Very large source-family jobs may persist compact selector identity instead of a
full `selected_targets` array in the job row. Compact identity sets
`source_identity_payload_format=selected_targets_digest_v1` and carries
`selected_target_count`, `selected_targets_digest_algorithm`,
`selected_targets_digest`, a first/last `selected_targets_sample`, and
`source_identity_hash`. The digest input is still the sorted canonical
`selected_targets` tuple above; compact storage avoids making one operational
JSONB row scale with every resolver instance while preserving immutable
idempotency-key comparison.

The selected target range fields are the intersection of the watched target's active range with the job's finite declared block range. `effective_to_block` is finite for every persisted selected target because backfill jobs are finite at creation time.

Automatic bootstrap backfill uses the same `backfill_jobs`,
`backfill_ranges`, and source-identity payloads as explicitly requested
backfill. It must persist finite declared range bounds before job creation and
store selected targets keyed by `contract_instance_id` plus effective range.
For configured chains, bootstrap ranges start at each eligible target's
manifest/discovery admitted start and end at the finite provider head observed
at job creation time; bootstrap must not replace that start with an arbitrary
recent-window cap. A watched target whose manifest-declared `start_block` is
unknown is skipped by bootstrap and leaves no synthetic block-zero,
provider-history, recent-window, or job-start range in `backfill_*`.

Backfill job and range checkpoint rows are operational state. They do not replace `chain_lineage`, do not define canonicality, and do not promote `canonical_head`, `safe_head`, or `finalized_head`.

Source-family backfill conformance for the admitted ENSv1 wrapper and resolver families and the admitted Basenames families is evidence over completed `backfill_*` job/range lifecycle state plus replayed existing shipped consumer-capability responses. It does not add a storage family, write projection rows, mutate manifests or discovery, graduate route coverage, expand public API routes, promote additional ENSv2 profiles, claim wrapper / migration history support, or change consumer-replacement meaning (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc).

## 5. Partitioning Baseline

Start with partitioning on the highest-volume append-only tables:

- `raw_blocks`
- `raw_transactions`
- `raw_receipts`
- `raw_logs`
- `normalized_events`
- `execution_steps`

For `raw_blocks`, `raw_transactions`, and `raw_receipts`, partitioning applies to hot replay facts and any payload-cache metadata retained in Postgres. It is not a requirement to keep full block, transaction, or receipt bodies inline after those payloads are outside the hot reorg/replay window or were not selected for admitted target replay.

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
- reorg repair preserves lineage and selected replay facts for losing branches as audit truth; evictable payload-cache bytes may be absent, but their absence must not erase canonicality or replay-critical evidence
- projection rebuilds read rows that are `canonical`, `safe`, or `finalized` by default
- history and audit tools may opt into `observed` and `orphaned` rows explicitly
- safe and finalized promotion is monotonic per chain

Execution cache rows follow the same hash-first canonicality rule. When reorg repair marks a block identity `orphaned`, synchronous indexer/reorg repair invalidates or deletes any reusable `execution_cache_outcomes` row for verified resolution or verified primary-name readback whose dependency set includes that `(chain_id, block_hash)` or a boundary resolved through it. The invalidation makes the cached outcome ineligible for reuse; it must not delete raw facts, execution traces, execution steps, trace attachments, or any execution-owned audit artifact.

Reusable `execution_cache_outcomes` rows must carry dependencies tied to explicit block-hash-bearing chain positions or boundaries. Rows for verified resolution or verified primary-name readback that lack those dependencies fail closed; rows for request types explicitly documented as outside this reorg invalidation surface remain out of scope rather than being treated as reorg-safe by omission.

Backfill range checkpoints are separate from canonicality checkpoints. Advancing or completing a backfill job records only that bounded fetch/resume work reached a position in its declared range; it must not change any `canonicality_state` value and must not advance `canonical_head`, `safe_head`, or `finalized_head`.

Backfill raw admission still writes canonicality for the facts it admits. When
the admitted historical range is already proven canonical, safe, or finalized by
retained lineage or provider checkpoint evidence, new lineage, raw-fact, and
normalized-event rows must use `canonical`, `safe`, or `finalized` as
appropriate instead of leaving those rows `observed` solely because the source
was backfill. If the evidence is absent, the storage layer must preserve the
weaker explicit state and leave the gap visible to audit and coverage tooling.

Read-only canonicality inspection uses storage audit helpers over `chain_lineage`, raw fact tables, retained payload-cache metadata, and `normalized_events`. The worker single-block inspection contract remains `bigname-worker inspect canonicality --chain-id <id> --block-hash <hash>` and resolves one `(chain_id, block_hash)`. For that requested block hash, helpers may report whether a stored lineage row exists and, for stored rows, block lineage, parent hash, block number, canonicality state, raw fact counts, payload-cache metadata counts or digests where retained, and normalized-event counts.

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

Exact-name snapshot selection is a storage read boundary, not a new storage family. The API resolves `at`, explicit `chain_positions`, and `consistency` to one concrete `ChainPositions` object, then reads only projection rows and execution outputs eligible for that exact object. `name_current`, `coverage_current`, `surface_bindings_current`, `permissions_current`, and `record_inventory_current` must retain enough chain-position context for the API to reject mismatched joins rather than combine rows from different snapshots.

If the selected exact-name positions are valid but no eligible projection or persisted execution output exists, the serving path returns the documented `stale`, `unsupported`, or `not_found` API state for that route and mode. It must not read raw facts, adapter-owned identity/event rows, or provider data directly to fill the public exact-name, coverage, topology, explain, or verified-readback response.

## 8. Raw Payload Cache and Object Storage

Persist durable raw replay facts inline in Postgres when they are needed for indexed lookup, adapter replay, projection rebuild, execution-output rebuild, canonicality audit, or selected-target backfill proof. Treat large/full raw payload bytes as cache unless a doc-first policy declares that payload class durable:

- full block bodies outside the hot reorg/replay window
- non-indexed transaction bodies
- non-indexed receipt bodies
- block-scoped payload batches fetched by live ingestion or backfill but not selected for admitted target replay
- full payload cache or block bundles for historical blocks with no selected target facts or replay-required enrichments

Evictable payload cache may be inline, local, object-backed, provider-refetchable, or absent after durable facts are extracted. Postgres stores only the metadata needed by the selected retention contract, such as payload kind, block identity fields, digest when the payload may later be dereferenced or refetched, size, content type or encoding, object key when object-backed, and observation metadata. Cache metadata without a retained digest may describe what was fetched, but it cannot authorize later byte use. Reads that dereference object-backed cache or re-fetch from a provider must verify the retained digest before use and fail closed on missing digest, mismatch, or unavailable historical data.

Hash-addressed object storage is a durability boundary only for raw payload classes explicitly declared durable. It is an implementation detail for evictable cache otherwise and must not be required for every fetched full block, transaction, or receipt body.

Persist small execution payloads inline in Postgres:

- request metadata
- response digests
- decoded final values
- failure reasons

Persist large payloads in object storage addressed by SHA-256 digest:

- CCIP payload bodies
- large metadata responses
- trace attachments

Postgres stores the digest, size, content type, and object key for execution attachments as well.

The execution storage boundary separates durable audit artifacts from cache reuse. `execution_traces` and `execution_steps` preserve what was executed and why; normal `execution_cache_outcomes` writes record whether a verified outcome can be reused under its request key, manifest versions, and block-hash-bearing dependency boundaries. Phase 9 reorg invalidation updates cache eligibility only through the synchronous indexer/reorg repair exception and does not change the selected ENSv2 exact-name support boundary, widen verified execution support, promote additional deployment profiles, or graduate any manifest capability.

Exact block-anchored `raw_call_snapshots` used by verified resolution stay in the intake-owned `raw_*` family rather than `execution_*`. Execution may hand off candidate snapshots for persistence only through that raw-fact boundary, only for the exact requested chain position, and only for support classes that explicitly admit them; `execution_traces`, `execution_steps`, and `execution_cache_outcomes` do not own those rows.

Before a verified-resolution selector may persist as a supported reusable outcome, execution must reload from storage the exact manifest versions for the request, the same declared topology snapshot the mixed route would serve for the same request and chain positions, and any resolver-profile admission state already required by the participating resolver-local fact families. The frozen support class is derived from those stored inputs and must match the persisted trace and cache key. If the stored inputs are absent or do not re-establish one frozen supported class, the trace may remain a durable audit artifact but the selector must not persist as a supported reusable outcome.

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
- worker/backfill code owns operational writes to `backfill_*` through those helpers, including finalized catch-up chunk creation and capacity pause/failure metadata
- adapters own inserts into identity and normalized-event tables
- projection workers own materialized read models
- execution workers own trace and step writes plus normal cache outcome writes
- synchronous indexer/reorg repair owns only `execution_cache_outcomes` deletes or invalidations tied to orphaned block dependencies
- raw-fact normalized-event replay is indexer-owned orchestration over the adapter-owned `normalized_events` boundary; it reads persisted canonical raw facts and may upsert only the corresponding `normalized_events` without stale-row purge or payload replacement
- normalized replay cursor storage is indexer-owned operational state used only to resume bounded raw-fact normalized-event replay; it does not define canonicality, widen backfill jobs, or change adapter event ownership
- intake owns durable hot raw-fact writes plus optional payload-cache metadata for block-scoped payloads; replay and inspection tooling may dereference object-backed cache or re-fetch provider payloads only through an explicit block-hash-scoped, retained-digest-checked, fail-closed boundary and must not refetch provider history as a substitute for retained replay inputs
- API code must not query raw-fact tables directly except for explicit audit endpoints
- canonicality, raw-fact, and stored lineage range inspection tooling is worker-owned, read-only operational tooling over storage audit helpers; it does not create a public `v1` route, infer missing lineage, or bypass the API boundary for user-facing reads
- backfill job inspection tooling is worker-owned, read-only operational tooling over `backfill_*`; it does not create a public `v1` route, mutate operational state, or bypass API read boundaries for user-facing data
- execution trace inspection tooling is worker-owned, read-only operational tooling over `execution_*`; it does not create a public `v1` route, execute fresh verification, mutate cache eligibility, or bypass the public explain-route boundary
- manifest drift and proxy alerting tooling is worker-owned observation over `manifest_*`, `discovery_*`, code-hash facts, proxy / implementation edges, and derived watch targets; its live audit path may write only the worker-owned `manifest_alert_*` observation family, and its read-only inspection path renders those stored alert observations as operational JSON without writing adapter-owned `normalized_events`, mutating manifest truth, discovery admission, discovery edges, capability flags, watch-plan inputs, projections, alert lifecycle state, or public API state
