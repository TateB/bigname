# Architecture

Status: Phase 0 baseline

Normative scope: Sections 2 through 31 are normative for the first implementation pass. Section 32 is informative only.

This document replaces the prior architecture baseline for `bigname`.

It defines the native architecture for a versioned naming-data and verification platform for:

- `ens`
- `basenames`

It is intentionally **not** a legacy-compatibility spec for the ENSv1 subgraph or any existing GraphQL surface. The goal is to replace those systems in our stack by serving the required capabilities through a cleaner native contract.

Implementation sequencing lives in [Development Plan](./development-plan.md).
Wire format, chain intake, storage, manifests, projections, execution, and parallel delivery boundaries live in the companion docs in this folder.

---

## 1. Objective

Build a replayable, auditable, reorg-safe platform that can answer, for any supported name or address:

- what public name surface exists
- what backing resource or authority object it currently binds to
- what the declared state is
- what the verified resolution is
- who controls it, and in what way
- what permissions exist
- whether a primary-name claim is merely claimed or actually verified
- what coverage and exhaustiveness guarantees apply
- how the answer was derived
- how the answer changed over time

Every answer must be:

- point-in-time
- replayable
- auditable
- explicit about provenance
- explicit about coverage
- explicit about finality / consistency
- safe under chain reorgs
- safe under source-graph expansion

---

## 2. Decision Summary

The architecture is centered on the following decisions:

1. **Native public contract, not legacy API parity**
   - `bigname` exposes its own versioned `v1` read contract.
   - Functional supersession is measured by capability coverage for real consumers, not by preserving the ENSv1 subgraph schema.

2. **Public name surfaces and backing resources are separate**
   - A public name string is not always the same thing as the authority object that owns control history.
   - Multiple public surfaces may bind to one backing resource.
   - One public surface may rebind across time.

3. **Declared and verified state are separate first-class answer modes**
   - Declared state comes from enumerable onchain / source-managed facts.
   - Verified state comes from deterministic execution of resolution and primary-name algorithms.

4. **Coverage and exhaustiveness are contractual**
   - Exact lookup, address enumeration, child enumeration, and record inventory each have independent coverage semantics.
   - Wildcard and offchain-derived names are never silently treated as globally enumerable.

5. **Source manifests and dynamic discovery are part of the truth model**
   - Contract discovery is not an implementation detail.
   - Every watched contract must be explained by a manifest or a reachable discovery edge.

6. **Permissions, resolver topology, and history are first-class**
   - Consumers should not reconstruct authority from raw role bitmaps or low-level logs.
   - Resolver- and account-centric views are explicit read models.

7. **Preimage observation is a first-class fact stream**
   - Human-readable labels and names must be derived from durable observed facts, not transient execution-side guesses.

---

## 3. Goals And Non-Goals

### Goals

- unify ENSv1, ENSv2, and Basenames behind one native truth model
- support point-in-time answers at head, safe, and finalized consistency levels
- support deterministic replay from immutable facts
- support exact-name lookup with authoritative coverage for supported source classes
- support verified resolution, wildcard traversal, alias traversal, and CCIP-enabled flows
- support primary-name answers as claim vs verified result
- support account, resource, and resolver-centric views
- support future capability growth through manifests rather than public-contract churn

### Non-Goals

- preserving the ENSv1 subgraph schema
- preserving GraphQL field-level parity with existing indexers
- pretending wildcard, alias-derived, or offchain names are globally enumerable
- treating token IDs as stable logical identity
- treating resolution as event-only
- hiding unsupported source classes behind silent fallback

---

## 4. Product Boundary And Namespace Policy

The public namespaces are exactly:

- `ens`
- `basenames`

Rules:

- `ens` is one public product.
- ENSv1 and ENSv2 are internal authority epochs, not separate public namespaces.
- `basenames` is a separate public product for Basenames-issued `*.base.eth` names on Base (upstream: .refs/basenames/README.md:L8 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L14 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc).
- `base.eth` itself is not treated as an end-user Basename; upstream treats `base.eth` as the root domain handled by the Ethereum Mainnet `L1Resolver`, while Basenames are the `*.base.eth` subdomains managed on Base (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc).
- public namespace assignment is explicit and versioned in an internal `NamespaceRegistry`
- a technically ENS-backed name may still belong to a different public namespace product
- no public name may exist twice across public namespaces
- deployment profile is separate from public namespace assignment; it chooses which admitted chain set backs those same public namespaces
- the shipped baseline uses the mainnet deployment profile
- later Sepolia support is an alternate deployment profile for the same public namespaces, not a new namespace or a concurrent truth set

Implication:

- `alice.base.eth` may be ENS-compatible internally, but publicly it belongs to `basenames` (upstream: .refs/basenames/README.md:L14 @ basenames@1809bbc)

### `NamespaceRegistry`

`NamespaceRegistry` is a versioned internal policy table that decides which public namespace owns a surface.

Each rule records:

- `namespace`
- `match_kind`
- `match_value`
- `priority`
- `active_from`
- `active_to`

`match_kind` values:

- `exact_name`
- `suffix`
- `authority_root`

Resolution rules:

1. highest-priority `exact_name`
2. highest-priority `suffix`
3. highest-priority `authority_root`
4. otherwise `unsupported`

Initial registry policy:

- exact `base.eth` belongs to `ens` because upstream treats `base.eth` as the L1 root domain handled by the Ethereum Mainnet `L1Resolver`, while Basenames are the `*.base.eth` subdomains managed on Base (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- suffix `*.base.eth` belongs to `basenames` (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- other supported ENS surfaces belong to `ens`

Conflicts reject canonical admission until the registry is updated. Namespace assignment happens before `logical_name_id` is minted.

---

## 5. Public Contract And Compatibility Policy

`bigname` publishes a versioned native `v1` contract from day one.

This `v1` contract is the compatibility boundary.

It does **not** preserve:

- ENSv1 subgraph entity names
- ENSNode schema shapes
- historical GraphQL query structure
- legacy distinctions that exist only because of old indexer internals

Instead, it preserves capabilities that consumers actually need.

Breaking semantic changes require `v2`.

### Public `v1` resource families

Top-level `v1` resource families are:

- `Namespace`
- `Name`
- `Address`
- `Resolver`
- `Resolution`
- `PrimaryName`
- `Permissions`
- `History`
- `Explain`
- `SourceManifest`
- `Coverage`

`Registration` is a stable subdocument of `Name`, not a top-level `v1` resource family in the initial contract.

Every externally visible read supports some combination of:

- `namespace`
- `name`
- `address`
- `coin_type`
- `at`
- `chain_positions`
- `consistency=head|safe|finalized`
- `mode=declared|verified|both`
- `include=*` for optional expansions
- pagination where collection semantics require it

The exact JSON and route layout belong in `docs/api-v1.md`. This architecture document defines semantics, not wire format.
Point-in-time selection rules for `at`, `chain_positions`, and cross-chain consistency are defined in `docs/api-v1.md`.

---

## 6. Public Answer Model

Every externally visible answer returns, directly or by expansion:

- `declared_state`
- `verified_state`
- `provenance`
- `coverage`
- `chain_positions`
- `consistency`
- `last_updated`

### Rules

- `declared_state` is authoritative for enumerable, source-managed facts.
- `verified_state` is authoritative for resolution and primary-name answers that require execution.
- `provenance` must identify source facts and any execution traces used to derive the answer.
- `coverage` must explain completeness and exhaustiveness, not merely freshness.
- `chain_positions` must be explicit whenever an answer depends on multiple chains or execution checkpoints.
- `consistency` is caller-visible and not inferred implicitly.
- mixed declared+verified routes keep the same top-level envelope shape as declared-only routes; `mode=declared|verified|both` decides which of `declared_state` and `verified_state` is populated and the unrequested section becomes `null`
- when one mixed route carries both declared and verified material, top-level provenance is a route summary and section-local provenance may be attached to preserve the declared-vs-execution boundary explicitly

### Coverage status vocabulary

Expected `coverage.status` values:

- `full`
- `partial`
- `observed_only`
- `unsupported`
- `stale`

### Exhaustiveness vocabulary

Expected `coverage.exhaustiveness` values vary by surface, but must explicitly distinguish:

- `authoritative`
- `best_effort`
- `observed_only`
- `non_enumerable`
- `not_applicable`

### `ResultStatus` Vocabulary

Mixed resolution and primary-name routes reuse one per-result `ResultStatus` vocabulary:

- `success`
- `not_found`
- `mismatch`
- `unsupported`
- `invalid_name`
- `execution_failed`

Rules:

- every route-local result object always carries `status`
- `unsupported_reason` is required when `status=unsupported`
- `failure_reason` may refine `not_found`, `mismatch`, `invalid_name`, or `execution_failed`
- only `success` guarantees a concrete record value or concrete name target
- not every status applies to every result object; the route contract defines the valid subset

---

## 7. Core Identity Model

The architecture separates four identity layers:

### 7.1 `logical_name_id`

Stable identity for a **public name surface** within a namespace across time.

Examples:

- `ens:test.eth`
- `ens:wallet.linked.parent.eth`
- `basenames:alice.base.eth`

A `logical_name_id` is the durable identity for the public surface, even if:

- the backing resource changes
- the token ID changes
- the name is unregistered and later re-registered
- the name resolves through aliasing or wildcard behavior at different times

### 7.2 `resource_id`

Stable identity for the **backing authority / control object**.

This is the anchor for:

- permission lineage
- control lineage
- token lineage
- resolver-scoped resource permissions
- resource-level role history

For ENSv2, `resource_id` maps to the upstream permissioned-registry EAC resource, not the current ERC1155 token ID. The registry exposes both `getResource(anyId)` and `getTokenId(anyId)`, emits `TokenResource(tokenId, resource)` when a registered label is linked to its EAC resource, and emits `TokenRegenerated(oldTokenId, newTokenId)` when role changes burn and mint a replacement token while leaving the resource unchanged (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L67 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L72 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309).
For ENSv1, `resource_id` is the stable internal identity for the authority object represented by the registry / wrapper / registration state.  
For Basenames, `resource_id` anchors the Base-side authority object, even when L1 compatibility transport is involved, because upstream keeps the authority stack on Base while cross-chain compatibility enters through the separate Ethereum Mainnet `L1Resolver` (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc).

ENSv1 continuity rule:

- treat each distinct ENSv1 authority anchor as its own `resource_id`
- for this slice, the relevant anchor classes are direct registry-only control, registrar-backed registration, and wrapper-backed control
- keep the same `resource_id` while the same anchor remains authoritative and only holder, controller, resolver, expiry, grace, fuse, or status facts change
- rotate the active `resource_id` when authority moves to a different anchor, including registry-only to registrar, registrar to wrapper, wrapper back to registrar or registry-only, and full lapse followed by later re-registration
- if the exact prior ENSv1 anchor becomes authoritative again, reuse that prior `resource_id` instead of minting another one; unwrap back to the still-live pre-wrap registrar lease is the canonical case
- resource-anchored permission truth follows the active `resource_id`; authority and permission continuity stay on that resource while the same anchor remains authoritative
- when authority moves to a different ENSv1 anchor, the successor `resource_id` has its own effective-permission truth; public reads do not silently merge predecessor and successor resources

### 7.3 `token_lineage_id`

Stable identity for tokenized ownership history.

This is required because token IDs can change or be replaced while the backing resource remains the same.

ENSv1 continuity rule:

- direct registry-only control has no active `token_lineage_id`
- mint a `token_lineage_id` when the authoritative ENSv1 anchor is tokenized through a registrar registration or wrapper position
- keep that `token_lineage_id` across transfer, renewal, expiry, and grace-period changes while the same tokenized anchor stays authoritative
- rotate the active `token_lineage_id` when authority moves to a different tokenized anchor, including registrar-to-wrapper transitions and a later re-registration after the old registration has fully ended
- if authority returns to the exact prior tokenized anchor, reuse that anchor's prior `token_lineage_id`; unwrap back to the same still-live registrar lease reactivates the prior registrar lineage

ENSv2 continuity rule:

- keep the same `resource_id` and `token_lineage_id` across `TokenRegenerated` events; update the current token ID attribute and append the normalized `TokenRegenerated` event instead of rebinding the surface or minting a successor resource (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L429 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461 @ ens_v2@554c309)
- mint or reactivate resource identity by upstream EAC resource, because the registry constructs resources from `eacVersionId` and token IDs from `tokenVersionId`; unregister / re-register increments both counters, while token regeneration increments only the token version (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L28 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L203 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L237 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L536 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L545 @ ens_v2@554c309)

### 7.4 `contract_instance_id`

Stable identity for registry, registrar, resolver, wrapper, or compatibility transport instances.

Mint a new `contract_instance_id` when a manifest-declared contract or a discovery-admitted contract is first added to the canonical source graph.

One admitted contract address on one chain maps to one `contract_instance_id` across all manifest and discovery epochs.

A contract instance keeps its ID when:

- the same admitted contract address on the same chain remains authoritative across manifest versions or discovery refresh
- the same admitted contract address on the same chain returns after an inactive gap; record a new non-overlapping active range on the existing `contract_instance_id`
- only code-hash observations, ABI metadata, rollout state, or active ranges change
- a proxy stays the watched contract while its implementation edge changes

A contract instance rotates when the watched contract itself is replaced by a different admitted contract address. In that case:

- close the prior instance's active range
- mint a new `contract_instance_id` for the successor address
- represent continuity, if any, with a `migration` edge in the manifest/discovery graph rather than ID reuse

Contract addresses are time-ranged attributes used for raw-fact matching and chain watching. Implementation addresses are modeled as separate contract instances linked by time-ranged proxy / implementation edges.

These rules apply equally to manifest `[[roots]]`, manifest `[[contracts]]`, and discovery-admitted contracts.

### Rule

A current token ID is never treated as stable logical identity.

---

## 8. Name Surface Model

The architecture adds a first-class surface layer so public names and backing resources do not get conflated.

### 8.1 `NameSurface`

Represents the canonical stored public-surface row for a normalized name in a namespace.

There is one `NameSurface` row per `logical_name_id`.

It stores the admitted surface identity and one canonical representative normalization result for that surface. It does not collapse every observed spelling or normalization attempt into additional `NameSurface` rows.

Persist on the `NameSurface` row:

- `logical_name_id`
- `namespace`
- `input_name`
- `canonical_display_name`
- `normalized_name`
- `dns_encoded_name`
- `namehash`
- `labelhashes`
- `normalizer_version`
- normalization warnings / errors

On `NameSurface`, `input_name` is the single representative source string whose pinned normalization output populates that row.

### 8.2 `SurfaceBinding`

Represents how a public name surface binds to a backing resource through time.

Persist:

- `surface_binding_id`
- `logical_name_id`
- `resource_id`
- `binding_kind`
- `active_from`
- `active_to`
- provenance
- canonicality state

### 8.3 Binding kinds

At minimum:

- `declared_registry_path`
- `linked_subregistry_path`
- `resolver_alias_path`
- `observed_wildcard_path`
- `migration_rebind`
- `observed_only`

ENSv1 authority-anchor rule:

- use `declared_registry_path` whenever the current ENSv1 binding is directly justified by canonical L1 registry, registrar, or wrapper facts
- registry-only control, registrar registration, wrapped control, unwrapped control, expiry / grace, transfer, and later re-registration all remain `declared_registry_path`
- these lifecycle changes only require a new `SurfaceBinding` row when the bound `resource_id` changes; transfer and expiry / grace within the same anchor do not change `binding_kind`
- do not encode ordinary ENSv1 wrap, unwrap, or re-registration transitions as `migration_rebind`; the identity change is carried by the `resource_id` and `token_lineage_id`, not by inventing a different binding kind

### 8.4 ENSv1 continuity examples

| Case | Current authoritative anchor | `resource_id` rule | `token_lineage_id` rule | `binding_kind` |
| --- | --- | --- | --- | --- |
| Registry-only control for `sub.alice.eth` | direct ENS registry control for the subname | mint one registry-anchored `resource_id`; keep it across registry-owner or controller changes until authority moves elsewhere | none while control stays registry-only | `declared_registry_path` |
| Registrar registration for `alice.eth` | ENSv1 registrar-backed lease | mint one registrar-anchored `resource_id`; keep it across renewals and registrar-owner transfers while the same lease remains authoritative | mint one registrar `token_lineage_id`; keep it while that same lease remains authoritative | `declared_registry_path` |
| Wrap `alice.eth` | ENSv1 NameWrapper-backed control | close the registrar binding and open a wrapper-anchored `resource_id` because the authority anchor changed | mint a wrapper `token_lineage_id` because the authoritative tokenized anchor changed | `declared_registry_path` |
| Unwrap `alice.eth` before the lease ends | same pre-wrap registrar lease becomes authoritative again | close the wrapper binding and reactivate the prior registrar `resource_id` instead of minting a new registrar resource | reactivate the prior registrar `token_lineage_id` instead of minting a new registrar lineage | `declared_registry_path` |
| Expiry or grace for `alice.eth` | same registrar or wrapper anchor, now with expired or grace-period status | keep the current `resource_id`; only status and expiry facts change until the old authority actually ends | keep the current `token_lineage_id` while the same tokenized anchor remains authoritative | `declared_registry_path` |
| Transfer of `alice.eth` | same current anchor, new holder or controller | keep the current `resource_id`; do not open a new binding row when the authority anchor did not change | keep the current `token_lineage_id` | `declared_registry_path` |
| Re-registration of `alice.eth` after full lapse | new registrar lease after the prior authority ended | once the old authority ends, close its binding; mint a new registrar `resource_id` for the new lease | mint a new registrar `token_lineage_id` for the new lease | `declared_registry_path` |

### Why this exists

This is required to correctly represent cases where:

- one backing resource appears under multiple public names
- one public name resolves via aliasing without a direct registry entry
- wildcard-derived names exist as observed answers rather than declared registry children
- names rebind across time without losing public-surface history

---

## 9. Normalization And Preimage Observation

Normalization is version-pinned.

For each `logical_name_id`, the canonical `NameSurface` row persists one representative normalized surface.

When sources reveal additional spellings or presentations of that same surface, those per-observation `input_name` and normalization details persist in immutable name/preimage observation facts and their normalized events rather than in additional `NameSurface` rows.

Persist on the canonical `NameSurface` representation:

- `input_name`
- `canonical_display_name`
- `normalized_name`
- `dns_encoded_name`
- `namehash`
- `labelhashes`
- normalization warnings
- normalization errors
- `normalizer_version`

Rules:

- invalid input is never silently coerced into a valid identity
- normalization output and the normalizer version used to produce it are both persisted
- per-observation name text and normalization provenance remain attributable through immutable observation facts and normalized events
- normalization provenance is part of the audit surface

### Preimage observation is first-class

The system must persist immutable facts for human-readable name revelation, including:

- `PreimageObserved`

These facts may come from:

- registrar events with explicit labels
- registry events with explicit labels
- wrapper events with human-readable names
- reverse / primary flows that explicitly reveal names
- metadata or source-specific discovery surfaces when allowed by manifest policy

Rules:

- unhashed labels and names must remain attributable to the source that revealed them
- the system must distinguish between a known surface and an unknown-but-hashed placeholder
- historical name quality must not depend on transient cache state

ENSv2 observation-only rules:

- admitted ENSv2 registry, registrar, and resolver name-bearing events may produce adapter-owned preimage observations: registry `LabelRegistered`, `LabelReserved`, and `ParentUpdated` expose label text, registrar `NameRegistered` and `NameRenewed` expose `.eth` labels, and resolver `AliasChanged`, `NamedResource`, `NamedTextResource`, and `NamedAddrResource` expose DNS-encoded names used by resolver topology and permission scopes (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L30 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L132 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L142 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L153 @ ens_v2@554c309).
- those observations are adapter/intake truth only: they may create identity rows, immutable preimage observation facts, and normalized events, but they do not create projection rows, do not mutate manifest capability state, and do not by themselves promote public exact-name support. For ENSv2, exact-name profile support is graduated only by `exact_name_profile = "supported"` on `ens_v2_registrar_l1` in the selected `sepolia-dev` manifest root; other ENSv2 profiles or capability states remain unsupported or shadow as declared by their active manifest (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309).

---

## 10. Canonicality, Authority, And Epochs

Rules:

- for `ens`, authoritative registration and control come from Ethereum L1
- `authority_epoch` may be `ens_v1` or `ens_v2` per name and time
- `authority_epoch` and `resolution_epoch` are separate concepts
- for `basenames`, authoritative registration and control live on Base (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- the Basenames L1 resolver path is compatibility transport, not a competing authority source (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
- primary names are canonical only after the verification algorithm succeeds for the requested `coinType`
- reverse claims alone are insufficient; verification must resolve the claimed name back to the requested address before the primary name is authoritative (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L217 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L226 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L263 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L269 @ ens_v1@91c966f)

Design consequence:

- the system must be able to show a declared answer and a separately verified answer, each with independent provenance

---

## 11. Source Families

### ENS source families

- `ens_v1_registry_l1`
- `ens_v1_registrar_l1`
- `ens_v1_wrapper_l1`
- `ens_v1_resolver_l1`
- `ens_v1_reverse_l1`
- `ens_dns_l1`
- `ens_offchain_metadata`
- `ens_v2_root_l1`
- `ens_v2_registry_l1`
- `ens_v2_registrar_l1`
- `ens_v2_resolver_l1`
- `ens_execution`

Current admitted ENSv1 Phase 4 split:

- `ens_v1_wrapper_l1` owns the Ethereum Mainnet NameWrapper source family for wrapper-backed declared authority, wrapper-token holder facts, fuse / expiry changes, wrapper-revealed names, and wrapper-originated resolver / TTL changes for admitted ENSv1 names. The upstream mainnet NameWrapper deployment is `0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401`; the wrapper emits wrap / unwrap / fuse / expiry events and mutates registry ownership or resolver / TTL state through wrapper methods (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L240 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L377 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L637 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L666 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L676 @ ens_v1@91c966f).
- `ens_v1_resolver_l1` owns the Ethereum Mainnet PublicResolver source family for declared resolver record state, resolver record-version observations, and resolver-local authorization facts. The upstream mainnet PublicResolver deployment is `0xF29100983E058B709F3D539b0c765937B804AC15`; its contract composes ABI, address, contenthash, data, DNS, interface, name, pubkey, and text resolver profiles, and its authorization path accounts for the ENS registry owner, wrapped owner, trusted controllers, approved operators, and approved delegates (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L13 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L66 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f).

This Phase 4 admission freezes adapter inputs and ownership only. It does not admit wrapper upgrade / migration history, does not add public routes, does not graduate exact-name, history, resolver overview, primary-name, or verified-resolution coverage, and does not claim consumer replacement. Those remain separate doc-first changes.

ENSv1 dynamic resolver discovery is nevertheless required before declared record reads can claim consumer replacement. The statically admitted PublicResolver is an initial seed, not the full resolver corpus. The admitted manifest/discovery rule treats canonical nonzero `NewResolver(node, resolver)` observations from admitted ENSv1 registry emitters as node-to-resolver binding updates and resolver contract instances for `ens_v1_resolver_l1`; zero-address resolver observations close only the affected node-to-resolver binding. The ENSv1 registry declares `NewResolver`, emits it from `setResolver`, and emits it from `_setResolverAndTTL` when record or subnode-record calls change resolver state (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f). Resolver contract admission does not by itself admit a supported resolver profile; typed record, version, and authorization facts require a separate supported-profile gate. The first dynamic ENSv1 supported-profile gate is PublicResolver-compatible only, using explicit profile admission evidence such as stored code-hash facts, proxy / implementation edge facts, or another non-schema admission rule; unknown dynamic resolvers remain watched targets with explicit `pending` or `unsupported` profile state and must not feed record inventory, record cache, or resolver overview projections. Basenames resolver-profile admission is frozen separately in the Basenames section below (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L23 @ ens_v1@91c966f).

`ENSRegistryOld` admission stays inside `ens_v1_registry_l1` as migration-aware historical input. The old registry address and `start_block = 3327417` come from the pinned subgraph, while `9380380` is only the pinned-subgraph start for the current ENS registry, not original ENS history (upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L42 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L44 @ ens_subgraph@723f1b6). Old-registry logs must not be unioned with current-registry logs by latest block: current-registry `NewOwner` marks the affected subnode migrated, later old-registry owner / resolver / TTL updates for that node are suppressed from current topology, and only old-registry root resolver updates retain the frozen exception (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L134 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L230 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6). This admission does not create a separate registry source family, widen capability flags, or claim consumer replacement.

Current admitted ENSv2 `sepolia-dev` split:

- `ens_v2_root_l1` owns the `RootRegistry` manifest root for the alternate `sepolia-dev` profile (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2 @ ens_v2@554c309).
- `ens_v2_registry_l1` owns the `ETHRegistry` root and discovered `UserRegistry` registry instances reached through ENSv2 registry graph expansion (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L15 @ ens_v2@554c309).
- `ens_v2_registrar_l1` owns the `ETHRegistrar` source for `.eth` commit, registration, and renewal facts; registered-name resource identity remains anchored to the permissioned registry resource linked by the registry (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L30 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L173 @ ens_v2@554c309).
- `ens_v2_resolver_l1` owns `PermissionedResolver` record, alias, version, and resolver-scoped permission facts for admitted resolver instances; the initial `sepolia-dev` implementation artifact is `PermissionedResolverImpl` (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/PermissionedResolverImpl.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L38 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L70 @ ens_v2@554c309).

The `sepolia-dev` profile uses `manifests-sepolia-dev/<namespace>/<source_family>/v1.toml` with the same manifest schema. It is an alternate selected profile, not an extension of the shipped mainnet profile; mainnet and Sepolia / `sepolia-dev` facts must not share a canonical corpus, watch plan, discovery graph, or projection set.

The ENSv2 `sepolia-dev` exact-name profile is promoted only inside the selected `sepolia-dev` deployment profile. When that runtime selects `manifests-sepolia-dev/` and `ens_v2_registrar_l1` declares `exact_name_profile = "supported"`, exact-name profile reads may use the admitted `ETHRegistry` resource/token state and the admitted `ETHRegistrar` lifecycle events for declared `.eth` profile coverage (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L22 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309). That promotion does not apply to the shipped mainnet profile, does not apply to other Sepolia profiles, and does not graduate resolver profiles, universal resolver / execution, reverse, DNS, wrapper, migration, verified resolution, primary-name, or consumer-replacement support.

### Basenames source families

- `basenames_base_registry`
- `basenames_base_registrar`
- `basenames_base_resolver`
- `basenames_base_primary`
- `basenames_l1_compat`
- `basenames_execution`
- `basenames_offchain`

Current admitted Basenames split:

- `basenames_base_registry`
- `basenames_base_registrar`
- `basenames_base_resolver`
- `basenames_base_primary`
- `basenames_l1_compat`
- `basenames_execution`

This admitted Basenames split matches the upstream deployed Registry, BaseRegistrar, L2Resolver, ReverseRegistrar, and L1Resolver contracts on Base Mainnet and Ethereum Mainnet; `basenames_offchain` remains reserved for later explicit gateway admission and is not part of the current admitted split (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc).

Basenames dynamic resolver discovery is required for the same declared-record reason. The statically admitted Base `L2Resolver` is the default resolver seed, not the full Base-side resolver corpus. The admitted manifest/discovery rule treats canonical nonzero `NewResolver(node, resolver)` observations from admitted `basenames_base_registry` emitters as node-to-resolver binding updates and Base resolver contract instances for `basenames_base_resolver`; zero-address resolver observations close only the affected node-to-resolver binding. The upstream Basenames registry stores resolver addresses per node and emits `NewResolver` from both direct resolver changes and record/subnode-record resolver changes (upstream: .refs/basenames/src/L2/Registry.sol:L19 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc). Resolver contract admission does not by itself admit a supported resolver profile; typed record and resolver-local authorization facts require a separate supported-profile gate such as code-hash / implementation allow-listing, ERC165 interface probing, ABI-family admission, or supported resolver-event observation. The static `L2Resolver` support profile is the current cited profile seed, with profile mixins, extended-resolution and ERC165 support, and the resolver-profile authorization override visible in the pinned `L2Resolver` file (upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc). The `L2Resolver`-compatible profile gate is separate from the ENSv1 PublicResolver-compatible gate and remains derived from existing discovery, code-hash / proxy-edge, ERC165, ABI-family, or event evidence; it does not require a manifest schema change, storage migration, shared enum, or API route widening. Pending or unsupported discovery-admitted Base resolvers remain watch targets only and must not feed resolver-local record, cache, or overview support. This discovery and profile rule is Base-side only; the Ethereum Mainnet L1 resolver remains explicitly owned by `basenames_l1_compat` and `basenames_execution`, and offchain gateways remain outside current admission.

### Shared families

- `shared_manifests`
- `shared_normalization_rules`
- `shared_capability_registry`

---

## 12. Source Manifests And Capability Registry

Each source family is pinned by a versioned manifest containing:

- root contracts
- chain
- proxy addresses
- implementation addresses
- code hashes
- ABI / schema hashes
- deployment epoch
- normalization version
- capability flags
- rollout status

Manifest changes are first-class normalized events:

- `SourceManifestUpdated`
- `ProxyImplementationChanged`
- `CapabilityChanged`

Rules:

- root manifests bootstrap the canonical contract graph through root `contract_instance_id` nodes
- a discovered contract becomes authoritative only if it is reachable from a canonical root or explicitly admitted by a manifest
- alternate-profile manifests such as `manifests-sepolia-dev/ens/ens_v2_registry_l1/v1.toml` use the same schema and are selected as a whole profile; they are not loaded beside `manifests/` in the same runtime
- manifest-declared roots and contracts admit `contract_instance_id` nodes; declared addresses are lookup attributes for those nodes, not the source-graph identity
- re-declaring the same root or contract address on the same chain, including after an inactive gap, carries forward the existing `contract_instance_id` and records a new non-overlapping active range
- changing the declared root or contract address mints a new contract instance and closes the old active range; any continuity to the predecessor is represented with a `migration` edge
- declared proxy implementations resolve to separate implementation `contract_instance_id` nodes; a proxy implementation change updates the proxy / implementation edge, not the proxy identity
- manifest versions are carried forward into normalized events and projections
- capability ownership attaches to the declaring `source_family`; it is never implied by a different family's presence alone
- ENSv2 Phase 5 `sepolia-dev` capability ownership is frozen to `ens_v2_root_l1`, `ens_v2_registry_l1`, `ens_v2_registrar_l1`, and `ens_v2_resolver_l1`; upstream deployment artifacts outside those families, including universal resolver, reverse, DNS, wrapper, migration, factory, oracle, and mock-payment deployments, remain outside current admission until a later doc-first source-family update (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/UniversalResolverV2.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ReverseRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/DNSAliasResolver.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/WrapperRegistryImpl.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/LockedMigrationController.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/HCAFactory.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/StandardRentPriceOracle.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/MockUSDC.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/MockDAI.json:L2 @ ens_v2@554c309)
- within that ENSv2 Phase 5 profile, `ens_v2_registrar_l1` owns the profile-scoped `exact_name_profile` capability and may promote it to `supported` only for the selected `sepolia-dev` manifest root; no other `sepolia-dev` family implies exact-name profile support, and `rollout_status=active`, registry admission, registrar admission, resolver-family admission, preimage observations, or backfill completion remain intake readiness rather than support graduation for any other profile or capability (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309)
- ENS verified resolution on Ethereum Mainnet belongs to `ens_execution`, whose canonical contract role is `universal_resolver` at the official ENS Universal Resolver proxy address `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`, not to `ens_v1_registry_l1`; that ownership freeze does not by itself widen public verified support beyond the separately frozen exact-surface ENS direct-path class, the already frozen exact-surface alias-only non-direct class, and the first additive exact-surface wildcard-derived class (official ENS docs: https://docs.ens.domains/resolvers/universal/) (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f)
- ENSv1 `.eth` registrar label intake belongs to `ens_v1_registrar_l1`: BaseRegistrar remains the tokenized registrar authority, while legacy, wrapped, and current registrar-controller contracts are admitted inside that same source family only for label-bearing registration and renewal observations needed to bind labelhashes to exact-name surfaces. This does not create a separate registrar-controller source-family owner or graduate the registrar capability flags beyond the existing shadow exact-name intake (upstream: .refs/ens_subgraph/subgraph.yaml:L145 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L170 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L226 @ ens_subgraph@723f1b6) (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L116 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L133 @ ens_v1@91c966f)
- ENS declared reverse-claim intake on Ethereum Mainnet belongs to `ens_v1_reverse_l1`, whose canonical contract role is `reverse_registrar` at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb` on the Ethereum `addr.reverse` Reverse Registrar, not to `ens_v1_registry_l1` or `ens_v1_resolver_l1` (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L19 @ ens_v1@91c966f)
- that ENS reverse-family ownership freezes only the current reverse-only declared claim surface; later fallback claim-setting surfaces, if admitted, require their own source-family owner and a later doc-first contract update (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
- for ENS primary-name reads in Phase 7, that reverse-family ownership admits only the reverse-claim tuple; it does not authorize combining reverse-only claim precedence with resolver-backed or execution-derived name identity to manufacture richer `claimed_primary_name` payloads (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
- Basenames declared authority on the shipped mainnet profile is split across `basenames_base_registry` through contract role `registry` at `0xb94704422c2a1e396835a571837aa5ae53285a95`, `basenames_base_registrar` through contract role `registrar` at `0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a`, and `basenames_base_resolver` through contract role `resolver` at `0xC6d566A56A1aFf6508b41f6c90ff131615583BCD` (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L10 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L15 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
- ENSv1 and Basenames declared record support requires dynamic resolver discovery and resolver-profile admission before consumer replacement can be claimed: resolver addresses observed through admitted registry `NewResolver` logs must become admitted resolver contract instances through resolver discovery edges, and resolver-local facts may be consumed only after supported profile admission for the relevant fact family; for ENSv1, the first dynamic supported profile is PublicResolver-compatible only, while for Basenames the first dynamic supported profile is Base-side `L2Resolver`-compatible only. Unknown dynamic resolvers in either namespace remain explicit `pending` or `unsupported`; the Basenames gate is separate from L1 transport / execution and offchain-gateway admission (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)
- ENSv1 old-registry admission is migration-aware and source-family-local to `ens_v1_registry_l1`: it may seed pre-migration topology and the root resolver exception, but it does not make `9380380` original ENS history, admit a second current registry owner, or graduate exact-name, children, history, resolver overview, or consumer-replacement coverage (upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L44 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6)
- Basenames declared primary-claim intake on the shipped mainnet profile belongs to `basenames_base_primary`, whose canonical contract role is `reverse_registrar` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282` (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
- that six-family Basenames split freezes the first declared read-plane boundary: exact-name, address-name, and children reads take declared truth from the Base registry / registrar / resolver families, while `basenames_base_primary` stays a separate claim-intake family (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
- Basenames L1 compatibility transport on the shipped mainnet profile belongs to `basenames_l1_compat`, whose canonical contract role is `l1_resolver` at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
- Basenames verified resolution on the shipped mainnet profile belongs to `basenames_execution`, whose canonical contract role is `l1_resolver` at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31`; `basenames_execution` keeps `verified_resolution=shadow` until the mixed and execution-explain routes both serve the exact-surface transport-assisted direct-path class, and the shared L1 Resolver address does not collapse transport ownership into execution ownership (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)
- `basenames_offchain` remains reserved for later explicit gateway admission; the current admitted Basenames family split is the six families above, not seven (upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc)
- that freeze does not create separate current source-family owners for registrar-controller, oracle, migration, proxy-admin, or offchain-gateway deployment artifacts; ENSv1 registrar-controller label intake above remains source-family-local to `ens_v1_registrar_l1`
- draft or optional features may be enabled behind manifest flags without changing the public contract

---

## 13. Source Discovery Graph

Discovery expands the canonical graph through time-versioned reachability edges such as:

- resolver changes
- subregistry changes
- parent changes
- alias changes
- metadata changes
- proxy / implementation changes
- migration edges
- transport edges

Persist a source graph with:

- `edge_id`
- `from_contract_instance_id`
- `to_contract_instance_id`
- `discovered_by`
- `edge_kind`
- `active_from`
- `active_to`
- provenance
- canonicality state

Endpoint rules:

- manifest-declared and discovered edges share the same endpoint model: each endpoint is a `contract_instance_id`
- discovery first resolves `(chain, address, point in time)` to the active `contract_instance_id`; if the address was admitted previously on that chain and is admitted again after an inactive gap, discovery reuses the historical `contract_instance_id` and records a new active range; only an address that has never been admitted on that chain mints a new `contract_instance_id`
- addresses, implementation addresses, code hashes, and roles remain attributes or provenance on the endpoint instances; they are never the primary key of the graph

ENSv2 graph expansion rules:

- upstream `SubregistryUpdated(tokenId, subregistry, sender)` maps to normalized `SubregistryChanged`; non-null `subregistry` endpoints are resolved to `contract_instance_id` before the discovery edge is stored (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L131 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L222 @ ens_v2@554c309).
- upstream `ParentUpdated(parent, label, sender)` maps to normalized `ParentChanged` and updates the parent edge for the registry instance that emitted it (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L151 @ ens_v2@554c309).
- upstream `ResolverUpdated(tokenId, resolver, sender)` updates the resolver edge for the current registry resource; admitted resolver endpoints then belong to `ens_v2_resolver_l1` (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L59 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L141 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L225 @ ens_v2@554c309).

Watch-plan expansion rules:

- watch-plan expansion starts from active root `contract_instance_id`s admitted from `[[roots]]` and traverses active discovery edges by `contract_instance_id`
- the watch target for intake is the address range attached to each active contract instance at the requested time
- address-only watch rows are derived execution detail and must remain explainable back to a manifest root or discovery edge through `contract_instance_id`

This graph is part of the truth model and audit surface. It is not a throwaway implementation detail.

---

## 14. Intake Architecture

Run three major intake planes for one selected deployment profile at a time:

- blockchain intake for Ethereum L1 in the shipped mainnet profile
- blockchain intake for Base in the shipped mainnet profile
- execution intake for verified reads and CCIP flows

Profile rules:

- the shipped baseline profile is `ethereum-mainnet` plus `base-mainnet`
- later Sepolia support is additive as an alternate deployment profile, not a concurrent expansion of the same canonical corpus
- one deployment must choose exactly one profile at a time; it must not ingest, reconcile, or answer across mainnet and Sepolia in the same truth set
- provider availability is per selected profile and active watched chain. A Base RPC endpoint is not required for an Ethereum-only run, and a selected profile with no configured Base provider must mark Base provider-backed intake idle / unavailable rather than failing startup for configured chains.

Shared stages:

1. block lineage intake
2. transaction, receipt, and log intake
3. hot raw fact persistence plus payload-cache metadata persistence
4. manifest and discovery updates
5. adapter routing
6. normalized event persistence
7. projection updates
8. execution-cache invalidation

Historical backfill enters through persisted, bounded jobs and range checkpoints, then uses the same raw fact, adapter, normalized-event, and projection stages as live intake. Backfill checkpoint state is operational worker state; it does not promote canonical, safe, or finalized chain checkpoints.

Full backfill covers the entire admitted history for the selected profile and
selected targets. Startup bootstrap follows the same admitted-history rule for
configured chains by creating finite jobs from each eligible target's admitted
start through the provider head observed at job creation time. Completing those
intake jobs still does not define route coverage, manifest support, or
consumer-replacement graduation without the matching projection, route,
conformance, and rollout evidence.

Postgres is the hot indexed and replay-focused store for this path. Live ingestion and backfill may fetch full block-scoped payloads, but Postgres retains replay-critical facts, lineage/header anchors, selected/admitted target logs, replay-required call snapshots/enrichments, and optional payload-cache metadata. Large/full block payloads and non-indexed transaction or receipt bodies are evictable cache by default once durable replay facts have been extracted; hash-addressed cold storage is required only for payload classes explicitly declared durable. Empty historical blocks retain lineage/header anchors and optional audit metadata only; they must not force full payload cache or block-bundle retention.

Exact lineage, fetch, notification, and reconciliation rules for this plane live in `docs/chain-intake.md`.

Protocol-specific logic belongs in:

- adapters
- manifest logic
- execution drivers

It must not leak into the public contract.

---

## 15. Immutable Facts And Rebuildable State

### Immutable facts

- blocks
- transactions
- receipts
- logs
- contract code hashes
- manifests
- discovery edges
- normalized events
- normalization results
- preimage observations
- selected `eth_call` snapshots
- CCIP request and response digests
- verification outcomes
- metadata responses
- sync cursors

For large/full chain payloads, the durable fact retained in Postgres may be only selected replay fields plus optional cache metadata or a digest, not the full body. This does not weaken immutability: compaction may evict non-critical inline payload bytes after durable replay facts are extracted, while lineage anchors, selected replay facts, normalized events, execution artifacts, and retained metadata remain immutable and canonicality-bearing.

### Rebuildable state

- current name-surface snapshot
- current surface binding snapshot
- current authority / registration snapshot
- current control snapshot
- current permissions snapshot
- current resolver topology
- current record inventory
- current record cache
- current primary-name snapshot
- reverse and address indexes
- resource-role indexes
- resolver indexes
- history materializations
- coverage snapshots
- execution cache
- subscriptions / feeds

Every projected row carries:

- provenance pointers
- manifest version
- canonicality state
- chain position context

---

## 16. Internal Domain Model

Core internal objects:

- `NameSurface`
- `SurfaceBinding`
- `BackingResource`
- `NameClass`
- `RegistrationSnapshot`
- `AuthoritySnapshot`
- `ControlVector`
- `PermissionSnapshot`
- `ResolutionTopology`
- `RecordInventory`
- `RecordCache`
- `PrimaryNameSnapshot`
- `SourceProvenance`
- `CoverageSnapshot`
- `TokenLineage`
- `ExecutionResult`

### `ControlVector`

`ControlVector` is never a single owner field. It includes:

- `token_holder`
- `registrant`
- `effective_controller`
- `record_manager`
- `delegates`
- `reverse_manager`
- `resolved_address_target`
- `status`
- `expiry`
- `authority_epoch`
- `resolution_epoch`

### `Registration.kind`

Source-specific values include:

- `lease`
- `subname_assignment`
- `reservation`
- `dns_control`
- `offchain_policy`
- `observed_only`

### Rules

- permissions and control are anchored to `resource_id`, not merely to surface name text
- `logical_name_id -> surface_binding -> resource_id -> token_lineage` must remain reconstructible through time
- multiple surfaces may map to one resource without duplicating control history

---

## 17. Normalized Event Taxonomy

### Identity, preimage, and discovery

- `PreimageObserved`
- `NameClassified`
- `SurfaceBound`
- `SurfaceUnbound`
- `ContractDiscovered`
- `MetadataChanged`
- `SourceManifestUpdated`

### Registration and authority

- `RegistrationReserved`
- `RegistrationGranted`
- `RegistrationRenewed`
- `RegistrationReleased`
- `ExpiryChanged`
- `AuthorityTransferred`
- `AuthorityEpochChanged`
- `MigrationApplied`
- `CommitmentMade`
- `PricingPolicyChanged`

### Lineage and control

- `TokenResourceLinked`
- `TokenRegenerated`
- `TokenControlTransferred`
- `ResolutionEpochChanged`

### Topology and resolution

- `ResolverChanged`
- `SubregistryChanged`
- `ParentChanged`
- `AliasChanged`
- `WildcardCoverageChanged`
- `RecordChanged`
- `RecordDeleted`
- `RecordVersionChanged`
- `RecordInventoryObserved`

### Permissions

- `PermissionChanged`
- `RootPermissionChanged`
- `DelegateRetainedAfterTransfer`
- `PermissionScopeChanged`

ENSv2 adapter mappings:

- `TokenResourceLinked` is emitted from upstream `TokenResource(tokenId, resource)` and is the only adapter event that links the current token ID to the upstream EAC resource (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L216 @ ens_v2@554c309).
- `TokenRegenerated` is emitted from upstream `TokenRegenerated(oldTokenId, newTokenId)` and must preserve the existing `resource_id`, `token_lineage_id`, and active surface binding unless a separate registry event changes those anchors (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309).
- `PreimageObserved` rows may be appended from ENSv2 name-bearing registry, registrar, and resolver events, but only as preimage/normalization observations tied to adapter-owned identity rows; they are not projection writes and they do not change public coverage or manifest capability state (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L30 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L132 @ ens_v2@554c309).
- `SubregistryChanged` and `ParentChanged` are the normalized graph events for upstream `SubregistryUpdated` and `ParentUpdated` respectively (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309).
- `AliasChanged` is the normalized topology event for upstream `PermissionedResolver.AliasChanged`, and the alias path stores source and destination DNS-encoded names from the unindexed event data (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L230 @ ens_v2@554c309).
- `PermissionChanged` and `RootPermissionChanged` are derived from upstream `EACRolesChanged(resource, account, oldRoleBitmap, newRoleBitmap)`; root-resource permissions stay distinguishable because EAC root roles are checked separately and also satisfy resource-level checks through root fallback (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L176 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L181 @ ens_v2@554c309).

ENSv1 Phase 4 adapter mappings:

- `PreimageObserved`, `SurfaceBound`, `SurfaceUnbound`, `AuthorityTransferred`, `ExpiryChanged`, `TokenControlTransferred`, `ResolverChanged`, `PermissionChanged`, and `RecordChanged` may be appended from the admitted NameWrapper and PublicResolver source families. The wrapper source maps its `NameWrapped`, `NameUnwrapped`, `FusesSet`, and `ExpiryExtended` event surfaces and wrapper registry-mutating methods into identity / authority / permission / resolver events; PublicResolver's admitted profile surface and authorization events map into resolver record and resolver-local permission events (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1034 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L51 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L58 @ ens_v1@91c966f).
- Those mappings are adapter-owned normalized events only. They do not write projections, synthesize cross-resource permission carry, mutate manifest capabilities, or convert upstream wrapper upgrade support into bigname wrapper / migration history support (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L479 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L500 @ ens_v1@91c966f).

### Reverse and primary

- `ReverseChanged`
- `PrimaryNameClaimed`
- `PrimaryNameVerified`
- `PrimaryNameInvalidated`

### Execution and coverage

- `VerifiedResolutionObserved`
- `VerifiedResolutionInvalidated`
- `CoverageChanged`

`CoverageChanged` captures a change to the shared single-name coverage summary used both by exact-name inline coverage and by `GET /v1/coverage/{namespace}/{name}`.

Every normalized event must carry:

- namespace
- `logical_name_id` when applicable
- `resource_id` when applicable
- source family
- manifest version
- chain position
- raw fact reference
- derivation kind
- canonicality flag
- before / after state where possible

---

## 18. Resolution Model

`Resolution` is one mixed route envelope with three declared sections and one verified section:

- declared `topology`
- declared `record_inventory`
- declared `record_cache`
- verified `verified_queries`

### 18.1 `topology`

`topology` is a fixed declared object with:

- `registry_path`
- `subregistry_path`
- `resolver_path`
- `wildcard`
- `alias`
- `version_boundaries`
- `transport`

Field semantics:

- `registry_path` is an ordered array of `NameRef` rows from the requested surface toward the declared registry authority and is never empty when `topology` is supported
- `subregistry_path` is an ordered array of `NameRef` rows from the requested surface toward the nearest declared subregistry ancestor and is empty when no subregistry delegation participates
- `resolver_path` is an ordered array of resolver hops; each hop carries `logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `resource_id`, `chain_id`, `address`, and `latest_event_kind`
- `wildcard` is an object with `source` and `matched_labels`
- `alias` is an object with `final_target` and `hops`
- `version_boundaries` is an object with `topology_version_boundary` and `record_version_boundary`
- `transport` is an object with `source_chain_id`, `target_chain_id`, `contract_address`, and `latest_event_kind`
- when compatibility transport participates, `transport.source_chain_id` names the declared-authority chain and `transport.target_chain_id` names the compatibility-entrypoint chain; for the frozen Basenames promotion-target class that freezes to `base-mainnet -> ethereum-mainnet` through the Basenames L1 Resolver because upstream deploys the Basenames authority stack on Base and the `L1Resolver` on Ethereum Mainnet (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)

Rules:

- `wildcard.source=null` with `matched_labels=[]` means wildcard traversal did not participate
- `alias.final_target=null` with `hops=[]` means alias rewriting did not participate
- for ENSv2, `alias` is declared topology only when admitted `PermissionedResolver` state provides an `AliasChanged` mapping; `PermissionedResolver` resolves aliases by longest suffix match and rewrites the resolver calldata node before dispatching the profile call (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L56 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L412 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L650 @ ens_v2@554c309)
- for ENSv2, `wildcard` is observed topology, not manifest admission: it is populated only when resolution or explain input identifies an ancestor/source resolver and matched labels; resolver deployment or alias presence alone must not synthesize wildcard coverage (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L38 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L412 @ ens_v2@554c309)
- all `transport` fields are `null` when no compatibility transport participates
- each version-boundary object carries `logical_name_id`, `resource_id`, `normalized_event_id`, `event_kind`, and `chain_position`
- `version_boundaries.record_version_boundary` must match the section-local `record_version_boundary` exposed by both `record_inventory` and `record_cache` for the same declared answer

### 18.2 `record_inventory`

`record_inventory` is the public contract for “what record space is known to exist”.

It is a fixed declared object with:

- `record_version_boundary`
- `enumeration_basis`
- `selectors`
- `explicit_gaps`
- `unsupported_families`
- `last_change`

Field semantics:

- `record_version_boundary` uses the same version-boundary object shape as `topology.version_boundaries.record_version_boundary`
- `enumeration_basis` is an object with `observed_selectors`, `capability_declared_families`, and `globally_enumerable`
- each selector row carries `record_key`, `record_family`, `selector_key`, and `cacheable`
- each explicit-gap row carries `record_key`, `record_family`, `selector_key`, and `gap_reason`
- each unsupported-family row carries `record_family` and `unsupported_reason`
- `last_change` is a history-pointer summary of the canonical event that last changed the admitted selector space, or `null` if no retained pointer exists

Rules:

- record inventory is not the same thing as canonical global enumeration
- record inventory is usually observed or capability-driven
- record inventory defines the stable record-selector space admitted by the route, including explicit gaps and unsupported families
- `selector_key` is `null` for scalar families and a string for parameterized families; when it is present, `record_key` is the round-trip string `record_family + ":" + selector_key`
- numeric selector domains such as coin types still use string `selector_key` values so `record_key` remains stable text
- selectors and explicit gaps are sorted by `record_key` ascending; unsupported families are sorted by `record_family` ascending
- version changes invalidate record inventory and cached record values for the prior version boundary

### 18.3 `record_cache`

`record_cache` is a declared-state cache of the last known value for supported records.

It is a fixed declared object with:

- `record_version_boundary`
- `entries`

Each cache entry carries:

- `record_key`
- `record_family`
- `selector_key`
- `status`
- `value`
- `unsupported_reason`

Rules:

- `record_cache` is keyed by node and version boundary
- `record_cache` is the declared last-known-value view over the same selector space and version boundary defined by `record_inventory`
- `record_cache` is capability-driven, not resolver-family hardcoded
- `record_version_boundary` must match both `record_inventory.record_version_boundary` and `topology.version_boundaries.record_version_boundary` for the same declared answer
- cache-entry `status` reuses the shared `ResultStatus` vocabulary, but declared cache entries use only `success`, `not_found`, and `unsupported`
- cache entries echo the same selector identity tuple `(record_key, record_family, selector_key)` surfaced by `record_inventory`
- `value` appears only when `status=success` and uses the family-native JSON shape for that selector
- `unsupported_reason` appears only when `status=unsupported` and is required then
- if callers request an explicit selector subset, entry order follows request order; otherwise entries are sorted by `record_key` ascending
- callers may request an explicit selector subset without changing the route envelope or inventing a second declared-state truth system
- unsupported records must remain requestable through verified execution where possible

### 18.4 `verified_queries`

`verified_queries` are execution-derived answers for explicit record requests.

Rules:

- verified queries return one result object per requested record selector and reuse the shared `ResultStatus` vocabulary
- explicit record reads may succeed even when inventory is partial
- verified queries do not backfill `record_inventory` or `record_cache` inside the same response; they are the execution-derived counterpart to those declared sections
- public verified support is narrower than the full resolution model: the shipped Phase 7 slice supports `ens` exact-surface direct-path requests first, the already frozen exact-surface alias-only non-direct class, and the first additive exact-surface wildcard-derived class
- for that support check, use the same declared topology snapshot that would populate the mixed route's declared `topology`; a request is direct-path only when `resolver_path[0].logical_name_id` equals the route surface `logical_name_id`, `wildcard.source=null` with `matched_labels=[]`, `alias.final_target=null` with `hops=[]`, and all `transport` fields are `null`
- the already frozen ENS alias-only non-direct support class is the exact-surface class where that same declared topology snapshot keeps `resolver_path[0].logical_name_id` equal to the route surface `logical_name_id`, `alias.final_target` non-`null` with `hops` non-empty, `wildcard.source=null` with `matched_labels=[]`, and all `transport` fields are `null`
- the first additive ENS wildcard-derived support class is the exact-surface class where `wildcard.source` is non-`null` with `matched_labels` non-empty, `resolver_path[0].logical_name_id` equals `wildcard.source.logical_name_id`, `alias.final_target=null` with `hops=[]`, `subregistry_path=[]`, and all `transport` fields are `null`
- ENS verified paths outside the direct-path, alias-only, and wildcard-derived classes, including other non-alias ancestor-selected paths, linked-subregistry ancestor-selected paths, any transport-assisted path, and any request whose persisted execution used CCIP-Read, remain deferred and return explicit selector-local `unsupported` results rather than silently widening support
- while `basenames_execution` remains `shadow`, public Basenames verified reads stay explicit `unsupported`; the first Basenames verified-resolution class frozen for promotion to `supported` is the exact-surface transport-assisted direct-path class where `resolver_path[0].logical_name_id` equals the route surface `logical_name_id`, `wildcard.source=null` with `matched_labels=[]`, `alias.final_target=null` with `hops=[]`, `subregistry_path=[]`, `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, and `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"`; that keeps declared authority on Base while publishing the separate L1 compatibility hop in the same declared topology snapshot (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- when `basenames_execution` graduates from `shadow` to `supported`, that frozen Basenames class includes CCIP-participating traces rather than selector-local `unsupported` because the upstream `L1Resolver` initiates `OffchainLookup` for non-`base.eth` requests and completes them through `resolveWithProof` (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)
- after that promotion, other Basenames verified path classes remain explicit selector-local `unsupported` until a later doc-first contract update broadens support and keeps future gateway admission separate from the frozen Base-authority-plus-L1Resolver slice (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc)
- verified answers must persist an execution trace
- wildcard traversal, alias rewriting, and CCIP flows must be explainable end-to-end

### `ExplainResolution`

`ExplainResolution` must show:

- resolver selection
- wildcard traversal
- alias rewriting
- record version boundary
- CCIP steps
- the source event or execution result that last changed the answer

Rules:

- the shipped explain route stays coupled to the same public verified-support boundary and explains persisted supported answers only; it does not fabricate trace-shaped public responses for deferred ENS non-direct paths or for Basenames paths outside the frozen transport-assisted direct class (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- for the supported ENS alias-only and wildcard-derived classes, and for the supported Basenames transport-assisted direct class, explainability must stay trace-backed and exact-surface-scoped: the persisted explain payload makes the participating alias rewrite, wildcard traversal, or Basenames CCIP transport explicit while other transport-assisted, CCIP-participating, non-alias ancestor-selected, or linked-subregistry paths remain outside the shipped public explain surface (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)

For Basenames, resolution must expose both:

- Base-native authority / state (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- L1 compatibility transport context (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)

---

## 19. Permissions Model

Permissions are first-class projections and first-class explain views.

Track grants by scope:

- root
- registry
- resource
- resolver
- record manager / operator
- migration-derived
- transport-derived where relevant

Track for each grant:

- grant source
- revocation source
- inheritance path
- transfer behavior
- scope
- effective powers

Public reads must expose `effective_powers` directly so callers do not reconstruct authority from raw role bitmaps or low-level assignments.

The first public declared-state permissions route is resource-centric: `GET /v1/resources/{resource_id}/permissions`.
Name-, address-, and resolver-centric permission views summarize or filter the same resource-anchored truth model; they do not introduce separate grant systems.

For ENSv2 registry resources, upstream `PermissionedRegistry` translates every labelhash, token ID, or resource input through `getResource(anyId)` before delegating EAC role reads and writes, so public permissions must be keyed by the bigname `resource_id` linked to that upstream resource rather than by token ID (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L57 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L261 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L351 @ ens_v2@554c309).

For ENSv2 resolver-scoped permissions, `PermissionedResolver` uses name-, text-key-, and coin-type-specific EAC resources for resolver setters; these permissions remain rows in the same resource-anchored permission model with resolver scope metadata, and resolver overview may summarize them without creating a separate grant system (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L70 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L159 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L239 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L257 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L282 @ ens_v2@554c309).

Required indexes include:

- permissions by resource
- permissions by account
- permissions by resolver
- permission history by resource
- permission history by account

---

## 20. Primary And Reverse Name Model

`PrimaryName` is address- and `coinType`-centric, not just a reverse-record projection.

Persist:

- `claimed_primary_name`
- `verified_primary_name`
- `reverse_namespace`
- `coin_type`
- `resolver`
- provenance
- coverage

Rules:

- `claimed_primary_name` and `verified_primary_name` are separate route-local result objects under the shared mixed-route envelope
- both objects reuse the shared `ResultStatus` vocabulary; `mismatch` and `execution_failed` apply only to `verified_primary_name`
- `claimed_primary_name` is the declared candidate only and never implies that verification succeeded
- `verified_primary_name` is authoritative only when `status=success`
- if the raw claim exists but cannot be normalized, the route surfaces `status=invalid_name` instead of silently dropping the claim
- verified primary names require the verification algorithm to succeed
- reverse claims alone are insufficient; verification must resolve the claimed name back to the requested address before the primary name is authoritative (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L217 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L226 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L263 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L269 @ ens_v1@91c966f)
- for ENS on Ethereum Mainnet, the current declared claim precedence is reverse-only through `ens_v1_reverse_l1` and its `reverse_registrar` entrypoint at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb` (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
- for Basenames on the shipped mainnet profile, the admitted declared primary-claim family is `basenames_base_primary` through contract role `reverse_registrar` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282`; it remains claim intake only and does not replace the Base registry / registrar / resolver families as the declared truth for exact-name, address-name, or children reads because upstream exposes reverse-name claims through the dedicated ReverseRegistrar rather than the Base authority stack (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
- missing or unsupported ENS reverse claims do not trigger fallback to registry-, resolver-, or other claim-setting surfaces in this phase; the admitted ENS claim source is the reverse registrar tuple only (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
- any fallback beyond that reverse-only ENS claim surface remains deferred and requires a later doc-first contract update; manifest presence alone does not widen claim precedence
- `claimed_primary_name.name`, when present, comes only from the exact requested `primary_names_current(address, coin_type, namespace)` row's declared normalized claim-identity source for that same tuple, aligned with the currently admitted reverse-only claim precedence (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
- it must not be synthesized or backfilled from manifest presence, resolver-backed identity, verified execution identity, tuple presence alone, a different tuple, or any fallback claim source
- `claimed_primary_name.name` remains distinct from execution-derived `verified_primary_name.name`; this clarification does not change when `verified_primary_name.name` appears, and it does not widen route-level primary-name coverage beyond the exact-tuple persisted-readback classes frozen below
- for Basenames as well as ENS, admitted claim intake does not collapse `claimed_primary_name` and `verified_primary_name`: claim-local state stays separate from execution-local state, and Base authority reads plus the separate Ethereum Mainnet `L1Resolver` execution owner do not let one backfill the other (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
- in Phase 7, that reverse-only ENS claim precedence does not combine with resolver-backed or execution-derived name data to enrich `claimed_primary_name`; `claimed_primary_name.name` stays limited to that exact requested row's declared normalized claim-identity source, `claimed_primary_name.provenance` is limited to exact-tuple declared row provenance, and richer fallback-expanded claimed payloads remain blocked (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
- the exact-tuple verified-primary support class uses stable execution identity `request_type=verified_primary_name` with request-key identity `{namespace}:{normalized_address}:{coin_type}` for the exact route tuple; the shipped ENS slice and the frozen first Basenames slice both use that cache identity, and claimed text, normalized name identity, verified target address, result status, and section-local provenance stay outside it
- the matching `primary_names_current(address, coin_type, namespace)` row is the only admitted claim-side lookup / invalidation anchor for that verified request; the projection may carry claim-local lookup and invalidation inputs only, and it does not become a second verified ledger
- for Basenames on the shipped mainnet profile, that exact-tuple persisted verified-primary support class stays execution-derived under `basenames_execution` rather than `basenames_base_primary`: upstream keeps reverse-name writes on the Base ReverseRegistrar while verified resolution enters through the separate Ethereum Mainnet `L1Resolver`, so this freeze does not add a dedicated manifest capability flag or widen route-level coverage beyond exact-tuple persisted readback (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
- `claimed_primary_name.provenance` is the first public claim-local section provenance on this route: exact-tuple declared-only provenance from the requested `primary_names_current(address, coin_type, namespace)` row, stripped of `verified_primary_name_lookup` / `verified_primary_name_invalidation`, and with no `execution_trace_id`
- `verified_primary_name.provenance` is part of the shipped public field boundary and is limited to `{execution_trace_id, manifest_versions}`: it is a strict verification-local refinement for the exact tuple, `verified_primary_name.provenance.execution_trace_id` must equal top-level `provenance.execution_trace_id`, `verified_primary_name.provenance.manifest_versions` must narrow that same persisted verification trace, and it must not publish `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material, restate claimed-row provenance, or publish other `Provenance` fields at this section-local boundary
- top-level route provenance joins declared claim inputs with any persisted verification trace; `claimed_primary_name.provenance` stays row-scoped and declared-only, and `verified_primary_name.provenance`, when present, stays verification-local within that same top-level `execution_trace_id`
- Basenames claim-setting operations affect the claim surface, but the read contract still distinguishes claim from verified primary name because upstream keeps reverse-name writes on the ReverseRegistrar while verified resolution enters through the separate `L1Resolver` (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)

---

## 21. Collection Semantics

Collection semantics are part of the public contract and must be frozen before implementation.

This is where the architecture replaces “legacy API compatibility” with explicit native semantics.

### 21.1 Exact name lookup

Lookup by name resolves a `NameSurface`.

The answer must include:

- normalized surface identity
- current surface binding
- declared summary sections for registration, authority, control, resolver, record inventory, and history
- provenance / coverage

Exact lookup is authoritative for supported source classes.

Rules:

- route-level exact-lookup coverage and subdocument support are separate concerns
- for `namespace=ens` under the selected ENSv2 `sepolia-dev` profile, exact-name profile coverage may be supported only when `ens_v2_registrar_l1` declares `exact_name_profile = "supported"` in that selected profile; the support class is limited to declared profile state derived from admitted registry and registrar facts, and it does not imply resolver-profile, verified-resolution, primary-name, history, or consumer-replacement graduation (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L22 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309)
- each declared summary section is always present as an object
- any declared summary section that is not yet projected must return an explicit unsupported object instead of disappearing silently
- for `namespace=basenames`, exact-name declared truth stays on the admitted Base authority split `basenames_base_registry`, `basenames_base_registrar`, and `basenames_base_resolver`; `basenames_base_primary` remains primary-claim intake only, and neither `basenames_l1_compat` nor `basenames_execution` widens this declared route because upstream keeps the registry / registrar / resolver stack on Base while reverse claims and the Ethereum Mainnet `L1Resolver` remain separate surfaces (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
- `authority` may fall back to the current binding identifiers when the binding is known but a richer authority summary is not yet projected
- exact-name `control` is a narrow current-control summary for the bound `resource_id`; in the initial contract it carries `registrant`, `registry_owner`, and `latest_event_kind`, and it stays narrower than both the internal `ControlVector` and the dedicated resource-permissions truth family
- exact-name `control` may repeat the current `registrant` already visible in `registration` when the same canonical facts drive both summaries; that duplication is intentional and does not create a second control truth system
- exact-name `resolver` is a narrow current-resolver summary; in the initial contract it carries `chain_id`, `address`, and `latest_event_kind`, and `chain_id=null` plus `address=null` mean the current resource has no declared resolver rather than that resolver reads are unsupported
- exact-name `resolver` does not inline alias traversal, wildcard traversal, transport context, record inventory, or resolver-overview subdocuments; those remain on `Resolution.topology`, `Resolution.record_inventory`, and resolver-centric reads
- exact-name `history` is a pair of head pointers into the canonical name-history contract rather than embedded rows; it carries `surface_head` and `resource_head`, each meaning “the first canonical row under `chain_position_desc` for the matching scope”
- exact-name `history` intentionally omits a `both_head` field; the dedicated history route keeps the `scope=both` union contract, row shape, and pagination behavior
- Phase 6 does not add a separate exact-name history-explain route; the shipped history routes are the history explain surface, and exact-name `history` only links into that declared answer with `surface_head` and `resource_head`
- for the same exact-name target and snapshot, the top-level `coverage` object matches the shared `Coverage` summary returned by `GET /v1/coverage/{namespace}/{name}`
- verified resolution remains a separate route family; exact-name lookup does not inline verified execution in the declared-state baseline

### 21.2 Address → names

Address-to-name reads return **surfaces**, not backing resources.

Each item must include:

- `logical_name_id`
- stable surface identity
- `resource_id`
- relation facets (`registrant`, `token_holder`, `effective_controller` in the first declared-state slice)
- binding kind
- provenance / coverage

Rules:

- callers may request de-duplicated results by `resource_id`, but surface-first semantics remain the default
- default sort is `display_name_asc`
- exhaustiveness is only authoritative for source classes with enumerable ownership / assignment surfaces
- wildcard- and offchain-derived names are never silently treated as exhaustive enumeration results
- for `namespace=basenames`, address-to-name membership and relation facets derive from the admitted Base authority split rather than reverse-claim or transport state; `basenames_base_primary`, `basenames_l1_compat`, and `basenames_execution` do not add rows or widen relation semantics on this route because upstream separates Base-side name ownership / resolver state from reverse claims and the Ethereum Mainnet `L1Resolver` transport (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
- role-summary expansion is additive; it must not change item identity, supported filters, grouping semantics, default sort, cursor behavior, or coverage meaning

### 21.3 Address → names with `include=role_summary`

This is an additive expansion of the same address-to-name collection, not a separate route and not application-side joining.

When requested, each item adds:

- `role_summary`
- `subname_count`
- `record_count`
- `status`
- `expiry`

`role_summary` is a per-resource summary object. In the first shipped slice it carries one `subjects[*]` entry per distinct current permission subject for the same `resource_id`, and each subject entry keeps the current `scope` plus `effective_powers` pairs from the resource-permissions collection. Row-granular grant and revocation lineage stays on the dedicated resource-permissions route.

Rules:

- `include=role_summary` keeps the base `Address.names` query contract unchanged: `namespace`, `relation`, and `dedupe_by` keep the same meaning and defaults
- the default sort stays `display_name_asc`
- surface-first enumeration remains the default and `dedupe_by=resource` remains grouping-only behavior
- the added fields above are expansion-only fields; they do not replace the required surface identity, binding, or relation facets
- `role_summary` derives from the same resource-anchored effective-permission truth family used by `GET /v1/resources/{resource_id}/permissions`; it does not create a second address-role ledger
- `subname_count` reuses the declared direct-children rule from `Name.children`, so it counts declared direct child surfaces only
- `status` and `expiry` come directly from the current `ControlVector` for that `resource_id`; they are not recomputed from the address relation
- `record_count` is the count of distinct stable declared record selectors for that `resource_id` at the current version boundary, using the same declared record-inventory semantics as `Resolution.record_inventory`; it is not a separate address-list counter, a raw resolver-slot count, or a verified execution count

### 21.4 Name → children

Child enumeration returns **declared direct child surfaces by default**.

This keeps counts stable and explainable.

The public contract may optionally expose additional buckets for:

- linked-subregistry children
- alias-derived children
- observed wildcard children

Rules:

- `subname_count` in the main name summary means declared direct children only
- linked, alias-derived, and wildcard-observed child counts are separate metrics
- a child can appear in multiple public surfaces when backing resources are shared
- for `namespace=basenames`, declared direct child surfaces come from the admitted Base authority split only; `basenames_base_primary` claim intake, `basenames_l1_compat` transport, and shadow `basenames_execution` do not create child rows or widen supported child buckets because upstream places `*.base.eth` subdomain registration on the Base registry / registrar stack while reverse claims and the L1 resolver stay separate (upstream: .refs/basenames/README.md:L8 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)

### 21.5 Resource → permissions

The first declared permissions collection is resource-centric.

One current row represents the effective permission state for a `(resource_id, subject, scope)` key.

Rules:

- the truth anchor is `resource_id`
- subject- or resolver-centric summaries may be projected for display, but they derive from the same resource-anchored effective grant rows
- `Address.names?include=role_summary` is one such display summary: it groups the current resource-anchored rows by `subject`, keeps each grouped subject's `scope` plus `effective_powers`, and leaves grant lineage on this route
- resolver-scoped permissions are part of this collection through scope detail, not a separate permission ledger
- if one surface rebinds across ENSv1 authority anchors, resource-centric permission reads stay partitioned by `resource_id` rather than stitching old and new anchors into one collection

### 21.6 History

History is first-class and queryable by scope:

- `surface`
- `resource`
- `both`

This is required because some changes affect the public name surface, while others affect only the backing resource.

Examples:

- a token transfer affects the backing resource
- an alias bind may affect the surface answer
- a resolver version change affects resolution but not public naming text

Rules:

- history reads are canonical normalized-event reads, not separate denormalized truth tables
- `scope` selects anchor sets, not alternate storage families
- name-history `resource` scope includes every resource ever bound to the requested surface
- resource-history `surface` scope includes every surface ever bound to the requested resource
- `Address.history` composes address anchor resolution with the same normalized-event history contract, using the existing address relation vocabulary and `scope=surface|resource|both` across current and historical matches rather than introducing a separate address-history truth system

### 21.7 Resolver overview

Resolvers are first-class read targets.

A resolver overview must be able to answer:

- which current surfaces / resources point at this resolver
- alias mappings
- resolver-scoped permissions
- role holders
- resolver events
- counts for nodes, aliases, and role holders

This is not a derived debug-only view; it is part of the product surface.

Rules:

- resolver overview is a declared-state route in the initial contract
- bindings, alias mappings, permissions, role-holder detail, and event/count summaries are separate declared summary sections
- supported alias mappings reuse the same `{status, count, items}` summary envelope as resolver bindings, but `items` is only the current `binding_kind=resolver_alias_path` subset of the same resolver-linked binding rows
- resolver alias mappings are sourced from current resolver-linked bindings only; they do not create a separate alias ledger or history family
- ENSv1 and Basenames resolver overview may summarize only direct manifest-admitted or resolver-discovery-admitted resolver contract instances with supported resolver-profile admission for the relevant overview section; for ENSv1 discovered resolver targets, that dynamic support is PublicResolver-compatible only, and for Basenames discovered Base-side resolver targets it is `L2Resolver`-compatible only. A current resolver target observed from registry state alone or left in `pending` / `unsupported` profile state remains a topology fact and must not create supported resolver-local overview sections (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)
- any such summary that is not yet projected must remain explicit through an unsupported object
- detailed effective permission rows still live on the resource-centric permissions route

### 21.8 Surface-binding explain by exact name

`GET /v1/explain/names/{namespace}/{name}/surface-binding` is the exact-name-scoped declared-state explain route for the current surface binding.

Rules:

- it is scoped to the same exact-name target and point-in-time snapshot rules as exact-name lookup
- its top-level `coverage` field matches the exact-name lookup answer for the same target and snapshot
- its declared-state detail is a thin view over the current `SurfaceBinding` plus the same exact-name history head pointers already defined for the exact-name route
- it reuses `surface_bindings_current` together with the shared normalized-event history contract; it does not introduce a second explain ledger or a binding-only history family
- it remains a declared-state route; it does not introduce verified execution semantics or collection semantics

### 21.9 Authority / control explain by exact name

`GET /v1/explain/names/{namespace}/{name}/authority-control` is the exact-name-scoped declared-state explain route for current authority and control.

Rules:

- it is scoped to the same exact-name target and point-in-time snapshot rules as exact-name lookup
- its top-level `coverage` field matches the exact-name lookup answer for the same target and snapshot
- its declared-state detail reuses the same exact-name `authority` and `control` summaries rather than widening those objects for a separate explain surface
- detailed permission lineage stays on the resource-centric permissions route, so this explain route does not become a second control or permissions ledger
- it reuses `name_current` plus the existing resource-anchored permissions truth family; it does not introduce a second authority or control truth system
- it remains a declared-state route; it does not introduce verified execution semantics or collection semantics

### 21.10 Coverage / explain by exact name

`GET /v1/coverage/{namespace}/{name}` is a single-name declared-state coverage / explain route.

It exists to explain the coverage contract for one exact public surface without introducing a second coverage truth model.

Rules:

- it is scoped to the same exact-name target and point-in-time snapshot rules as exact-name lookup
- its top-level `coverage` field is the same shared `Coverage` object returned inline by `GET /v1/names/{namespace}/{name}` for that target and snapshot
- its declared-state detail explains `coverage.status`, `coverage.exhaustiveness`, `coverage.source_classes_considered`, `coverage.enumeration_basis`, and `coverage.unsupported_reason`
- it remains a declared-state route; it does not introduce verified execution semantics or collection semantics

---

## 22. Coverage And Exhaustiveness Rules

Coverage is contractual, not incidental.

Rules:

- exact name lookup must be authoritative for supported source classes
- ENSv1 Phase 4 NameWrapper and PublicResolver admission does not by itself make wrapper-backed history, wrapper migration history, resolver overview, exact-name profile, primary-name, or verified-resolution coverage supported; primary-name coverage is supported only for the exact-tuple persisted-readback class below, and other unsupported or unprojected sections remain explicit until a later doc-first coverage update
- ENSv1 and Basenames declared record and resolver-overview coverage must stay explicit about unindexed or unsupported-profile current resolvers: a resolver target observed from registry state is not enough to claim supported records unless that resolver address is directly manifest-admitted or discovery-admitted into the relevant resolver source family and has supported profile admission for the relevant fact family (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
- ENSv2 exact-name profile coverage is supported only for the selected `sepolia-dev` manifest root when `ens_v2_registrar_l1` declares `exact_name_profile = "supported"`; other profiles or shadow/unsupported capability states must remain explicit unsupported or shadow rather than `full`, `partial`, or `observed_only` public support (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)
- exact-name route coverage may still be authoritative when some declared summary subdocuments are explicitly unsupported
- the dedicated single-name coverage route is the explain surface for the same shared `Coverage` object used inline on exact-name lookup
- address-to-name enumeration is exhaustive only for source classes with enumerable ownership / assignment surfaces
- wildcard and offchain name classes are not globally enumerable in general
- record inventory is `best_effort` unless a resolver family exposes explicit enumeration or the platform has a source-specific index
- child enumeration is authoritative only for declared direct children unless the caller explicitly opts into other surface classes
- the shipped primary-name route has local exact-tuple coverage only for the frozen ENS and Basenames persisted-readback classes: supported tuples return route-level `coverage.status=partial`, `exhaustiveness=non_enumerable`, `enumeration_basis=primary_name_lookup`, namespace-local `source_classes_considered`, and `unsupported_reason=null`
- for `namespace=ens`, that exact-tuple class uses `source_classes_considered=["ens_v1_reverse_l1","ens_execution"]`; for `namespace=basenames`, it uses `source_classes_considered=["basenames_base_primary","basenames_execution"]` (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc)
- out-of-class primary-name tuples, fallback claim sources, fresh verified-primary execution, address-wide primary-name coverage, namespace-wide coverage, and external app parity remain explicit `unsupported` or out of scope; tuple presence and resolver-backed verification detail do not by themselves unlock richer ENS claimed-payload fields or widen the local exact-tuple coverage class

Every response includes:

- `coverage.status`
- `coverage.exhaustiveness`
- `coverage.source_classes_considered`
- `coverage.unsupported_reason`
- `coverage.enumeration_basis`

---

## 23. Deterministic Execution And Verification Plane

Verified execution is a required subsystem.

Default verified resolution paths:

- ENS uses `ens_execution` with contract role `universal_resolver` at the official ENS Universal Resolver proxy address `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` as the canonical verified-resolution entrypoint on Ethereum Mainnet; the shipped public verified slice covers exact-surface direct-path requests, the already frozen exact-surface alias-only non-direct class, and the first additive exact-surface wildcard-derived class, using the same route-level support check over declared topology (official ENS docs: https://docs.ens.domains/resolvers/universal/) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L90 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L106 @ ens_v1@91c966f)
- Basenames uses `basenames_execution` with contract role `l1_resolver` at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` as the canonical verified-resolution entrypoint on Ethereum Mainnet; `basenames_l1_compat` owns that same L1 Resolver address as compatibility transport, and while `basenames_execution` remains `shadow`, public Basenames verified / explain reads stay explicit `unsupported`. The first Basenames verified / explain class frozen for promotion to `supported` is the exact-surface transport-assisted direct-path class where `resolver_path[0].logical_name_id` equals the route surface, `wildcard.source=null` with `matched_labels=[]`, `alias.final_target=null` with `hops=[]`, `subregistry_path=[]`, `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, and `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- that frozen Basenames verified-support target does not suppress the separate declared read plane: exact-name, address-name, children, and declared exact-name explain reads stay on Base-side declared truth while only Basenames paths outside the frozen transport-assisted direct class remain deferred after promotion (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)

The execution engine must support:

- onchain calls
- wildcard resolution
- alias-aware execution
- nested CCIP-Read
- batch / multicall execution
- proof and verification persistence

Persist an `ExecutionTrace` for every verified answer with:

- entrypoint
- resolver discovery path
- contracts called
- gateway URLs or digests
- proof and callback checks
- final result
- errors
- chain positions

Cache verified answers by:

- request
- chain positions
- manifest versions
- relevant topology / version boundaries

Invalidate the cache on:

- reorg
- manifest change
- relevant topology change
- relevant record change
- relevant alias / wildcard change

---

## 24. Reorg, Replay, And Backfill

The system stores block lineage per chain.

On divergence:

- detect the fork point
- mark affected facts orphaned
- invalidate dependent normalized events
- invalidate dependent execution cache entries
- rebuild projections deterministically

The exact unwind and replay algorithm lives in `docs/chain-intake.md`.

Reorg repair preserves the audit trail for the losing branch. It marks lineage, selected raw facts, normalized events, and retained payload metadata `orphaned` where applicable; it must not delete replay/audit truth needed to explain or rebuild state. Evictable full-payload cache bytes may be absent, but their absence must not erase canonicality or replay-critical evidence.

Backfills use the same path as live ingestion:

- hot raw facts and retained payload-cache metadata
- manifests / discovery
- normalized events
- projections

Backfill scheduling is persisted as bounded jobs with resumable range checkpoints. The shared substrate provides idempotent create, reserve, advance, complete, and fail transitions so workers can crash and resume without widening source ranges, duplicating range ownership, or rewriting admitted facts.

Backfill range checkpoint ownership is separate from chain checkpoint ownership. Completing a backfill job means the declared range work reached its stored end; it does not make any block canonical, safe, or finalized and does not change API consistency semantics.

Source-scoped backfill is selected-target-only. It may retain selected target logs/facts, minimal lineage/header anchors, replay-required enrichments, and cache metadata needed for block-hash-scoped admission or audit, but it must not turn unselected block-wide transaction, receipt, or block bodies into Postgres hot rows merely because they were fetched during scanning.

Historical backfill over finalized or safe ranges must persist selected facts
with the strongest canonicality supported by available checkpoint evidence:
`finalized`, `safe`, or `canonical` rather than `observed` when that evidence
exists. The job lifecycle still remains separate from checkpoint promotion.

Operational catch-up follows finalized head by chaining finite idempotent
backfill chunks whose ends are no greater than the finalized head observed at
chunk creation. Each chunk must pass DB-size, writable-free-disk, and configured
object-cache budget checks before range work starts. Capacity failures pause or
fail the chunk with explicit metadata; they must not discard durable selected
facts, widen source identity, downgrade canonicality, or force full payload
retention for empty historical blocks.

Required backfills include:

- ENSv1 historical state
- ENSv1 wrapper / migration history
- ENSv1 DNS and offchain discovery where supported
- ENS reverse / primary history
- ENSv2 historical registration, topology, permissions, and alias history
- Basenames historical registration, control, primary, and resolution history

Constraint:

- wildcard and offchain names cannot be assumed exhaustively enumerable
- backfill for those classes is discovery-based and observed-answer-based

---

## 25. Operational And Audit Requirements

Metrics:

- chain lag
- safe / finalized lag
- reorg depth
- adapter failure rate
- manifest drift
- proxy upgrade detection
- execution latency
- CCIP error rate
- verification failure rate
- coverage partial rate
- replay duration
- backfill catch-up chunk capacity checks, including Postgres size and writable free disk

Required tooling:

- replay from checkpoint
- backfill source range
- follow finalized head with bounded idempotent catch-up jobs
- inspect backfill job and range checkpoints
- rerun projections from normalized events
- inspect persisted execution trace
- inspect stored manifest drift and proxy alert observations
- inspect raw facts
- inspect manifest versions
- diff declared vs verified answers
- invalidate execution cache
- inspect canonicality disputes
- inspect canonicality and raw facts for one `(chain_id, block_hash)` by lineage, parent/number, raw fact counts, normalized-event counts, and stored canonicality state
- inspect surface bindings
- inspect resolver topology

Canonicality and raw-fact inspection is worker-owned operational tooling over read-only storage audit helpers. The worker inspection surface is the single-block command `bigname-worker inspect canonicality --chain-id <id> --block-hash <hash>` and resolves only one `(chain_id, block_hash)` at a time. It may report whether that block hash has a stored lineage row and, for stored rows, the lineage, canonicality state, parent hash, block number, raw fact counts, payload-cache metadata counts or digests where retained, and normalized-event counts for that block. Range-oriented storage helpers, where present, are observed/stored lineage listings for known rows only; they do not infer missing heights, gaps, or range-level canonicality status. The tooling must not expose a public `v1` route, mutate storage, dereference object-backed cache or provider-refetched block-scoped payloads unless the fetch is block-hash-scoped and the retained digest verifies, treat payload metadata without a retained digest as reusable bytes, or let user-facing API code bypass the projection and execution-read boundaries.

Execution trace inspection is worker-owned operational tooling over persisted `execution_traces` and `execution_steps`. It returns stable JSON for one stored trace and its persisted step summaries; it does not execute a fresh resolution or primary-name request, expose raw execution payload APIs, synthesize topology, mutate caches or projections, mutate manifests or discovery, or change the public `GET /v1/explain/resolutions/{namespace}/{name}/execution` boundary.

Manifest drift and proxy-alert inspection is worker-owned operational tooling over already stored alert observations. It returns stable JSON for existing observations and their source manifest, contract-instance, proxy / implementation edge, code-hash, watched-target, timestamp, lifecycle, and nullable remediation metadata. It must not perform fresh chain comparison, create observations, mutate alert lifecycle state, change manifest truth, rewrite discovery edges, update watch plans, write projections, expose a public `v1` route, or claim consumer replacement.

Audit expectations:

- periodic live-call sampling against canonical manifests
- projection vs live-state diffing
- alerting on unexpected code-hash or implementation changes
- consumer capability conformance checks

Live manifest drift and proxy-upgrade alerting is a worker-owned operational observation loop over admitted manifests, stored code-hash facts, proxy / implementation edges, and derived watch-plan state. The producer computes operational audit output and must not write adapter-owned `normalized_events` or a new alert table unless a later doc-first worker-owned persistence family exists. Any persisted alert observation must preserve the proxy `contract_instance_id` when only implementation observations change, and implementation churn remains an edge observation until an explicit manifest or discovery update changes source truth. Alerting does not silently admit contracts, mutate manifest truth, change capability flags, rewrite discovery edges, mutate watch plans, write projections, expose public API responses, or claim consumer replacement.

---

## 26. Consumer Capability Contract

Supersession is defined by capability coverage, not by schema parity.

The checked-in baseline lives in `docs/consumer-capabilities.md`. The summary below is the condensed replacement contract for first-party consumers.

| Capability | Existing app examples | Native `v1` responsibility |
| --- | --- | --- |
| exact name profile | profile pages, record editing, registration views | `Name.registration` + `Resolution` |
| names owned / controlled by address | dashboard and search flows | `Address.names` |
| names owned / controlled by address with role summary | dashboard lists | `Address.names` with `include=role_summary` |
| declared child subnames and child counts | subname pages and creation flows | `Name.children` |
| record inventory for editing | profile / records screens | `Resolution.record_inventory` + `Resolution.record_cache` |
| verified record reads | profile / send / address resolution | `Resolution.verified_queries` |
| name history | profile history pages | `History(scope=both)` |
| address history across names | address activity views | `Address.history` |
| role holders for a name / resource | roles pages | `Permissions.by_resource` |
| role change history | roles history pages | `History(filter=permissions)` |
| resolver-centric overview | resolver pages | `Resolver` |
| claimed vs verified primary names | dashboard / profile | `PrimaryName.claimed_primary_name` + `PrimaryName.verified_primary_name` |

This matrix is the replacement contract for first-party consumers and must be frozen in phase 0.

---

## 27. Minimum Test Matrix

### ENSv1 and wrapper cases

- ENSv1-only name
- wrapped ENSv1 name
- wrapped expiry / grace-period edge
- fuse changes that alter control semantics
- wrapped owner differing from registrant
- reverse claim vs verified primary mismatch

### ENSv2-specific cases

- root-scope role grant
- delegate retained after transfer
- token regeneration on role change without ownership change
- shared subregistry creating multiple surfaces for one backing resource
- alias-derived surface with no direct registry entry
- subregistry swap replacing a subtree
- re-registration with same resource and new token ID

### DNS / wildcard / offchain cases

- imported DNS name
- gasless DNS or metadata-discovered name where supported
- wildcard-derived subname
- CCIP success
- CCIP failure
- offchain gateway mismatch

### Basenames cases

- NFT-only transfer
- management-only transfer
- address-resolution change
- full transfer
- primary-name set / unset
- L1 compatibility resolution
- current single-address capability
- future capability flag off for multi-address support

### Operational cases

- reorg across authority events
- reorg across verified execution cache
- replay determinism from raw facts
- replay determinism from normalized events
- proxy implementation change
- manifest version change

Validate each scenario at four layers:

- raw facts
- normalized events
- execution traces
- public API output

---

## 28. Acceptance Criteria

The system is done for its first production milestone when:

- every supported exact name lookup returns surface identity, declared state, provenance, coverage, and point-in-time chain positions
- every resolution answer can be served in `declared`, `verified`, or `both` mode
- every primary-name answer distinguishes claim from verified result
- ENS names transition across ENSv1 and ENSv2 without duplicating public-surface identity
- multiple public surfaces can bind to one backing resource without duplicating control history
- Basenames remain a separate public namespace even though they reuse ENS-style infrastructure (upstream: .refs/basenames/README.md:L14 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- no answer silently drops unsupported source classes or resolver types
- projections rebuild deterministically from canonical facts
- reorg recovery requires no manual projection patching
- manifests and discovery edges are observable and explainable
- first-party consumer capabilities are fully served through the native `v1` contract

---

## 29. Non-Negotiable Constraints

- versioned native public contract from day one
- namespace is first-class and explicit
- public surface identity is distinct from backing resource, token, resolver instance, and reverse namespace identity
- provenance, coverage, and finality are first-class
- resolution is not modeled as event-only
- verified execution is a required subsystem
- permissions are first-class
- source manifests are first-class
- preimage observation is first-class
- projections are disposable and rebuildable
- protocol-specific logic lives in adapters and execution drivers, not in the public contract
- no silent cross-source fallback
- every fallback must appear in provenance / explain
- no requirement to preserve the ENSv1 indexer API surface

---

## 30. Initial Implementation Direction

Recommended baseline:

- Rust modular monolith for the first production version
- PostgreSQL as the primary hot indexed/replay system of record for lineage, replay-critical raw facts, projections, execution metadata, and retained payload-cache metadata
- hash-addressed object storage for execution artifacts and for raw payload classes explicitly declared durable; otherwise large raw payload bytes are evictable cache
- Rust workers for ingestion, projection, replay, and execution
- Rust HTTP API for the public `v1` surface
- small TypeScript conformance harness for protocol and consumer capability tests

Suggested repo shape:

- `apps/api` for the read API
- `apps/indexer` for chain intake and adapter routing
- `apps/worker` for replay, backfill, and projection jobs
- `crates/domain` for public and internal domain types
- `crates/storage` for raw facts, normalized events, and projections
- `crates/execution` for verified resolution, primary verification, and trace persistence
- `crates/adapters` for ENSv1, ENSv2, DNS, reverse, and Basenames logic
- `crates/manifests` for source manifests and capability registry logic
- `docs/` for evolving specs, ADRs, and operational notes

Parallel implementation sequencing and ownership live in `docs/workstreams.md`.

First implementation priorities:

1. source-manifest schema
2. raw-fact schema
3. chain-intake contract
4. surface / resource identity schema
5. normalized-event schema
6. chain-lineage model
7. replay-safe projection interfaces
8. execution-trace schema
9. minimal `Name`, `Address`, and `PrimaryName` read paths

---

## 31. Open Decisions

These items are intentionally left open for the next ADR / spec pass:

- exact Postgres schema and partitioning strategy
- exact cache invalidation granularity for verified queries
- which execution artifacts stay inline in Postgres vs object storage
- exact raw-payload cache retention windows, compaction cadence, and which payload classes are durable rather than cache
- whether subscriptions ship in the first `v1` release or after the first stable read milestone

---

## 32. References

- `architecture.md` and `development-plan.md` in this workspace
- `chain-intake.md` in this workspace
- `consumer-capabilities.md` in this workspace
- <https://github.com/ensdomains/contracts-v2>
- <https://raw.githubusercontent.com/ensdomains/contracts-v2/main/docs/indexing-ensv2-events.md>
- <https://raw.githubusercontent.com/ensdomains/contracts-v2/main/docs/indexing-test-names.md>
- <https://github.com/ensdomains/ens-contracts>
- <https://github.com/ensdomains/ens-subgraph>
- <https://github.com/namehash/ensnode>
- <https://docs.ens.domains/web/ensv2-readiness/>
- <https://docs.ens.domains/contracts/ensv2/overview/>
- <https://docs.ens.domains/wrapper/expiry>
- <https://docs.ens.domains/wrapper/fuses>
- <https://docs.base.org/base-account/basenames/basenames-faq>
- <https://docs.base.org/base-account/basenames/basename-transfer>
