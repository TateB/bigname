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
- `basenames` is a separate public product for Basenames-issued `*.base.eth` names on Base.
- `base.eth` itself is not treated as an end-user Basename.
- public namespace assignment is explicit and versioned in an internal `NamespaceRegistry`
- a technically ENS-backed name may still belong to a different public namespace product
- no public name may exist twice across public namespaces

Implication:

- `alice.base.eth` may be ENS-compatible internally, but publicly it belongs to `basenames`

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

- exact `base.eth` belongs to `ens`
- suffix `*.base.eth` belongs to `basenames`
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
- `provenance` must identify both source facts and execution traces used to derive the answer.
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

For ENSv2, `resource_id` maps to the stable resource / canonical ID within a registry and survives token regeneration.  
For ENSv1, `resource_id` is the stable internal identity for the authority object represented by the registry / wrapper / registration state.  
For Basenames, `resource_id` anchors the Base-side authority object, even when L1 compatibility transport is involved.

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

- `LabelObserved`
- `NameObserved`
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

---

## 10. Canonicality, Authority, And Epochs

Rules:

- for `ens`, authoritative registration and control come from Ethereum L1
- `authority_epoch` may be `ens_v1` or `ens_v2` per name and time
- `authority_epoch` and `resolution_epoch` are separate concepts
- for `basenames`, authoritative registration and control live on Base
- the Basenames L1 resolver path is compatibility transport, not a competing authority source
- primary names are canonical only after the verification algorithm succeeds for the requested `coinType`
- reverse claims alone are insufficient

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

### Basenames source families

- `basenames_base_registry`
- `basenames_base_registrar`
- `basenames_base_resolver`
- `basenames_base_primary`
- `basenames_l1_compat`
- `basenames_execution`
- `basenames_offchain`

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
- manifest-declared roots and contracts admit `contract_instance_id` nodes; declared addresses are lookup attributes for those nodes, not the source-graph identity
- re-declaring the same root or contract address on the same chain, including after an inactive gap, carries forward the existing `contract_instance_id` and records a new non-overlapping active range
- changing the declared root or contract address mints a new contract instance and closes the old active range; any continuity to the predecessor is represented with a `migration` edge
- declared proxy implementations resolve to separate implementation `contract_instance_id` nodes; a proxy implementation change updates the proxy / implementation edge, not the proxy identity
- manifest versions are carried forward into normalized events and projections
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

Watch-plan expansion rules:

- watch-plan expansion starts from active root `contract_instance_id`s admitted from `[[roots]]` and traverses active discovery edges by `contract_instance_id`
- the watch target for intake is the address range attached to each active contract instance at the requested time
- address-only watch rows are derived execution detail and must remain explainable back to a manifest root or discovery edge through `contract_instance_id`

This graph is part of the truth model and audit surface. It is not a throwaway implementation detail.

---

## 14. Intake Architecture

Run three major intake planes:

- blockchain intake for Ethereum L1
- blockchain intake for Base
- execution intake for verified reads and CCIP flows

Shared stages:

1. block lineage intake
2. transaction, receipt, and log intake
3. raw fact persistence
4. manifest and discovery updates
5. adapter routing
6. normalized event persistence
7. projection updates
8. execution-cache invalidation

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

- `LabelObserved`
- `NameObserved`
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

Rules:

- `wildcard.source=null` with `matched_labels=[]` means wildcard traversal did not participate
- `alias.final_target=null` with `hops=[]` means alias rewriting did not participate
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

For Basenames, resolution must expose both:

- Base-native authority / state
- L1 compatibility transport context

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
- reverse claims alone are insufficient
- Basenames claim-setting operations affect the claim surface, but the read contract still distinguishes claim from verified primary name

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
- each declared summary section is always present as an object
- any declared summary section that is not yet projected must return an explicit unsupported object instead of disappearing silently
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
- exact-name route coverage may still be authoritative when some declared summary subdocuments are explicitly unsupported
- the dedicated single-name coverage route is the explain surface for the same shared `Coverage` object used inline on exact-name lookup
- address-to-name enumeration is exhaustive only for source classes with enumerable ownership / assignment surfaces
- wildcard and offchain name classes are not globally enumerable in general
- record inventory is `best_effort` unless a resolver family exposes explicit enumeration or the platform has a source-specific index
- child enumeration is authoritative only for declared direct children unless the caller explicitly opts into other surface classes

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

- ENS uses the canonical Universal Resolver path on Ethereum Mainnet
- Basenames uses the L1 compatibility path plus Base-native state, with provenance showing both transport and Base authority surfaces

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

Backfills use the same path as live ingestion:

- raw facts
- manifests / discovery
- normalized events
- projections

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

Required tooling:

- replay from checkpoint
- backfill source range
- rerun projections from normalized events
- inspect explain trace
- inspect raw facts
- inspect manifest versions
- diff declared vs verified answers
- invalidate execution cache
- inspect canonicality disputes
- inspect surface bindings
- inspect resolver topology

Audit expectations:

- periodic live-call sampling against canonical manifests
- projection vs live-state diffing
- alerting on unexpected code-hash or implementation changes
- consumer capability conformance checks

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
- Basenames remain a separate public namespace even though they reuse ENS-style infrastructure
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
- PostgreSQL as primary system of record
- object storage for large raw payloads and execution artifacts
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
