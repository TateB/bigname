# ADR 0002: Surface And Resource Identity

Status: Accepted
Date: 2026-04-16

## Context

Legacy ENS indexing tends to conflate public name text, node identity, token identity, resolver instance, and control history. ENSv2 and Basenames both break that simplification:

- one public surface may rebind across time
- one resource may appear under multiple public surfaces
- token identifiers may change while backing authority does not
- resolver aliasing and wildcard behavior may create observable surfaces without direct registry entries

## Decision

Use four distinct identity anchors:

- `logical_name_id`: deterministic on-chain name identity, stored as `<namespace>:<namehash>` where the hash is the lowercase `0x`-prefixed 32-byte node
- `resource_id`: opaque stable identity for the backing authority object
- `token_lineage_id`: opaque stable identity for tokenized ownership history
- `contract_instance_id`: opaque stable identity for registry, registrar, resolver, wrapper, or transport instances

Contract-instance rules:

- mint a `contract_instance_id` when a manifest-declared contract or discovery-admitted contract first enters the canonical source graph
- one admitted contract address on one chain maps to one `contract_instance_id` across all manifest and discovery epochs
- reuse the same `contract_instance_id` while the same admitted contract address remains authoritative on the same chain
- if the same admitted contract address becomes active again after an inactive gap, reuse the prior `contract_instance_id` and record a new non-overlapping active range
- treat a change to the watched contract's own admitted address as a new contract instance; close the predecessor's active range and mint a successor ID instead of reusing the old one
- roots follow the same contract-instance rules as ordinary manifest-declared and discovery-admitted contracts
- model proxy contracts and implementation contracts as separate contract instances linked by time-ranged proxy / implementation edges
- represent continuity between distinct contract instances with `migration` edges in the manifest/discovery graph
- resolve discovery and watch-plan lookup from `(chain, address, point in time)` to `contract_instance_id`; raw addresses are attributes used for lookup, not graph identity

### Discovery-edge observation identity

- [`logical_edge_identity`](../glossary.md#logical-discovery-edge-identity) is
  the rebuild-stable identity of one fact-derived discovery-edge epoch. A
  sequence-assigned database `discovery_edge_id` is only
  a local join key and never enters this identity.
- For `registry_announcement`, encode the following ordered text fields:
  `chain_id`, `edge_kind`, canonical lower-case hyphenated
  `from_contract_instance_id`, canonical lower-case hyphenated
  `to_contract_instance_id`, `discovery_source`, `admission_basis`, the source
  manifest's `namespace`, `source_family`, `chain_id`, `deployment_label`, and
  decimal `manifest_version`, followed by `observation_key` and decimal
  `active_from_block_number`, lower-case `active_from_block_hash`, decimal
  transaction index, and decimal log index. Decimal integers have no leading
  zero except the value zero itself.
- Serialize each field as its four-byte unsigned big-endian UTF-8 byte length
  followed by those UTF-8 bytes. Prefix the concatenation with the ASCII domain
  separator `bigname:discovery-edge:v1\0`, hash it with Keccak-256, and render
  the result as lower-case `0x`-prefixed 32-byte hex. This value is
  `logical_edge_identity`.
- The five-field source-manifest tuple is the schema's unique semantic manifest
  key. Using it rather than sequence-assigned `source_manifest_id` makes the
  manifest component stable across an empty-schema rebuild. A replay of the same
  edge observation therefore reproduces the same logical identity even when its
  local database IDs change.

Public identity rules:

- exact lookup is surface-first and keyed by `logical_name_id`
- raw labels and their hash path are retained verbatim; normalized label or display text is never an identity input
- normalization is a versioned visibility gate, so unnormalizable names retain deactivated shadow rows and normalizer-version changes never rotate `logical_name_id`
- permissions and control are resource-first and keyed by `resource_id`
- token IDs are never treated as logical identity
- a time-ranged `SurfaceBinding` joins `logical_name_id` to `resource_id`
- a canonical ENSv2 registry/root `PreimageObserved`, or a resolver
  `AliasChanged` preimage observation whose DNS name passes normalization, keeps
  its [name surface](../glossary.md#surface-name-surface) known after its active
  binding and resource end; later node-scoped records keep `logical_name_id`
  but do not inherit the ended `resource_id`, and alias restoration never
  creates a resource binding; this cross-run rule has one known pre-existing
  exception described below

ENSv1 authority-anchor rules:

- `resource_id` is anchored to the current ENSv1 authority object, not to the surface text and not to the current holder address
- for this slice, the relevant ENSv1 authority anchors are direct registry-only control, registrar-backed registration, and wrapper-backed control
- keep the active `resource_id` while the same ENSv1 authority anchor stays authoritative across transfer, renewal, expiry, grace, fuse, resolver changes, or controller changes that do not diverge from the current tokenized holder
- rotate the active `resource_id` when authority moves to a different ENSv1 anchor; wrap, unwrap, re-registration, and live registrar registry-owner divergence are the important cases
- if the exact prior ENSv1 authority anchor becomes authoritative again, reuse its prior `resource_id`; exact means the prior anchor itself, not a different holder / controller and not a released or re-registered lease
- if no prior registrar identity was materialized because the [deployment profile](../glossary.md#deployment-profile) retains numeric BaseRegistrar registration events only as evidence for a candidate [ENSv1→ENSv2 migration boundary](../glossary.md#migration-boundary), the ordered `NameUnwrapped` then BaseRegistrar `Transfer` establishes the registrar `resource_id` and `token_lineage_id` at that transfer; later transfers and replay reuse them, without retroactively creating an ordinary registration row. A completed `syncWrapper` [ENSv1→ENSv2 migration correlation group](../glossary.md#migration-correlation-group) may refine the registrar expiry used only when that transfer first materializes the missing identity; multiple completed groups retain the monotone maximum correlated expiry, and the retained correlated state does not update ordinary NameWrapper normalized events or NameWrapper state. That maximum is safe across full lapse and re-registration because BaseRegistrar accepts a successor registration only after the predecessor expiry plus grace, then writes a strictly later `block.timestamp + duration` expiry. (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L103 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L168 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L104-L111 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L318-L337 @ ens_v1@91c966f)
- effective permissions and permission history are keyed to the authoritative `resource_id`, not to the surface text
- when the same ENSv1 anchor remains authoritative, resource-centric permission continuity stays on that `resource_id`; when authority moves to a different anchor, resource-centric permission reads do not merge predecessor and successor resources
- direct registry-only control has no active `token_lineage_id`
- registrar-backed and wrapper-backed ENSv1 anchors each carry their own `token_lineage_id`
- keep the active `token_lineage_id` while the same tokenized ENSv1 anchor stays authoritative; rotate it when authority moves to a different tokenized anchor
- if authority returns to the exact prior tokenized anchor, reuse its prior `token_lineage_id`; exact prior-anchor reuse does not resurrect token lineage after release or across mismatched holder / controller authority
- ordinary ENSv1 registry-only control, registrar registration, wrap, unwrap, expiry / grace, transfer, and re-registration all use `SurfaceBinding.binding_kind = declared_registry_path`
- a standalone registry-owner observation for a node without a materialized [name surface](../glossary.md#surface-name-surface) creates the node-scoped direct-registry `resource_id` but no name surface or `SurfaceBinding`; an observation attributed to a live registrar lease, including ownership setup reconciled within the registration transaction, remains retained interpreter state without a separate direct-registry resource, surface, or binding, and if that lease later releases while the retained owner is nonzero, the direct-registry `resource_id` and replacement `SurfaceBinding` must be materialized together at the release boundary; when `NameRenewed` or `NameWrapped` first supplies the active name surface for a retained registry resolver, reuse the retained registry resource for an additive [state-derived normalized event](../glossary.md#state-derived-normalized-event) without rewriting the earlier event or opening registry control while registrar or wrapper authority remains current
- registrar release is a block-boundary transition because upstream availability compares stored expiry plus the 90-day grace period with `block.timestamp`, rather than emitting a lease-expiry log (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L103 @ ens_v1@91c966f); the retained registry owner survives release because ENS stores it independently until another registry ownership write replaces it (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L7 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L13 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L170 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L171 @ ens_v1@91c966f)

Resource-centric convenience rule:

- when a resource view needs a single display surface, rank bindings in this order:
  `declared_registry_path`
  `linked_subregistry_path`
  `resolver_alias_path`
  `observed_wildcard_path`
  `observed_only`
- ties break by earliest active binding, then lexical `normalized_name`

## Consequences

- address collections return surfaces by default
- clients may opt into `dedupe_by=resource`, but that is never the default truth model
- history must support `scope=surface|resource|both`
- wrapping, migration, token regeneration, and aliasing can be represented without identity distortion

## Worked Examples

### ENSv1 authority-anchor lifecycle

| Case | Continuity result |
| --- | --- |
| Registry-only control for `sub.alice.eth` | mint one registry-anchored `resource_id`; keep it across registry-owner or controller changes; no active `token_lineage_id`; `binding_kind` is `declared_registry_path` |
| Registrar registration for `alice.eth` | mint one registrar-anchored `resource_id` and one registrar `token_lineage_id`; keep both while that same lease remains authoritative; `binding_kind` is `declared_registry_path` |
| Registry owner diverges from the live registrar holder for `alice.eth` | close the registrar binding; mint one registry-anchored `resource_id` with no active `token_lineage_id`; the successor binding is still `declared_registry_path` |
| Diverged registry owner returns to the same live registrar holder before release via registry-side `setOwner` or registrar `reclaim` | keep `logical_name_id`; close the registry-only binding; reactivate the prior registrar `resource_id` and prior registrar `token_lineage_id`; the successor binding is still `declared_registry_path` |
| Registry owner returns after release or to a different holder / controller | keep `logical_name_id`; do not reactivate the prior registrar `resource_id` or prior registrar `token_lineage_id`; the active authority remains on a distinct registry-only or later registrar anchor; `binding_kind` is `declared_registry_path` (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L7 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L13 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L170 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L171 @ ens_v1@91c966f) |
| Wrap `alice.eth` | keep `logical_name_id`; close the registrar binding; mint a wrapper-anchored `resource_id` and wrapper `token_lineage_id`; the successor binding is still `declared_registry_path` |
| Unwrap `alice.eth` before the lease ends | keep `logical_name_id`; close the wrapper binding; reactivate the prior registrar `resource_id` and prior registrar `token_lineage_id`; if that registrar identity was never materialized, establish it at the ordered BaseRegistrar transfer instead; the successor binding is still `declared_registry_path` |
| `alice.eth` enters expiry or grace while the same authority anchor remains in force | keep the current `resource_id` and current `token_lineage_id`; only status and expiry facts change; `binding_kind` stays `declared_registry_path` |
| The registrar lease for `alice.eth` releases while its retained registry owner is nonzero | close the registrar binding; materialize the direct-registry `resource_id` with no token lineage and open its `declared_registry_path` binding in the same boundary batch |
| `alice.eth` transfers while the same authority anchor remains in force | keep the current `resource_id` and current `token_lineage_id`; no new binding row is needed if the anchor did not change; `binding_kind` stays `declared_registry_path` |
| `alice.eth` fully lapses and is later re-registered | keep `logical_name_id`; once the old authority ends, its binding closes; a later registration mints a new registrar `resource_id` and a new registrar `token_lineage_id`; the new binding is `declared_registry_path` |

Resource-centric permissions follow the same lifecycle: while one ENSv1 authority anchor remains authoritative, effective permission continuity stays on that anchor's `resource_id`; wrap, unwrap, or re-registration do not cause the API to stitch different `resource_id` values into one permission collection. Exact prior-anchor reuse applies to that prior anchor becoming authoritative again, such as unwrap or registry-side convergence back to the same live unreleased registrar lease, not to post-release resurrection or convergence through a different holder / controller.

### ENSv2 linked surfaces

Two public surfaces may bind to the same `resource_id`. Permissions and role history stay attached to the resource; surface-specific reads keep their own binding provenance.

A retained canonical registry/root `PreimageObserved`, or a resolver
`AliasChanged` preimage observation whose DNS name passes normalization,
rebuilds the known name surface during replay even after registration release
or expiry closed its binding. Alias evidence never creates or restores a
resource binding. A later resolver `NameChanged` or `VersionChanged` for that node
remains attributed to the surface without an active `resource_id`. The
known exception is when a resolver-emitted resource equals `namehash(N)`:
named-resource and alias preimages can share one retained [interpreter state
key](../glossary.md#interpreter-state-key), so resumed interpretation can lose
the named-resource resolver hint and diverge from a fresh walk
([#560](https://github.com/ensdomains/bigname/issues/560); evidence is checked
in as an ignored collision probe). The resolver stores records by node and
version. `setName` passes part zero,
selecting the node-specific, any-part permission resource; the cited
authorization path reads EnhancedAccessControl role mappings and contains no
current registry-registration lookup. (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L127-L133 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L77-L85 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L178-L186 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L467-L472 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L247-L254 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L66-L78 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L185-L192 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L374-L382 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L443-L455 @ ens_v2@a971bd64)

An ownerless version-zero ENSv2 reservation establishes an unbound registry-entry
`resource_id` and token-lineage identity before any token mint. Their existence
alone is not a registration, current authority, or `SurfaceBinding`. A later
successful claim for that registry entry reuses the identities, and its
`TokenResource` emission confirms the resource. A reservation whose version
bits prevent deriving that resource remains resource-less.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L25-L34 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L428-L471 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L632-L650 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/utils/LibLabel.sol:L11-L17 @ ens_v2@a971bd64)

### Token regeneration with stable authority

Token regeneration does not change `logical_name_id`, and it does not require a new `resource_id` when the backing authority is the same. Token attributes change within the token-lineage history rather than becoming the primary identity.

### Proxy implementation upgrade

The proxy contract keeps the same `contract_instance_id`. The old proxy / implementation edge closes and a new edge opens to the implementation contract instance for the new implementation address. If a prior implementation address returns later, its prior `contract_instance_id` is reused.

### Declared contract replacement

If a manifest changes a watched contract's own address, the prior contract instance ends and a new `contract_instance_id` begins for the successor deployment. Any continuity is represented with a `migration` edge, not by reusing the predecessor's ID. If the predecessor address returns later, its prior `contract_instance_id` is reused with a new active range.
