# Development Plan

Archived: this document is historical planning context, not active implementation
guidance. Current semantics live in the top-level `docs/` files, current ADRs live in
`docs/adrs/`, and current operational checks live in the runbooks.

Internal reference. This is the phased implementation plan that drove bigname's bootstrap and first vertical slices. It is preserved as historical record and forward-looking structure for work that hasn't shipped yet. The shipped behavior of the system is described in the top-level `docs/` files; this plan is not the source of truth for what works today.

It orders work around the decisions that are hardest to change later:

- public semantics
- surface / resource identity
- manifest model
- raw fact model
- replay model
- verified execution model

The plan assumes a modular monolith for the first production version. Parallel execution and ownership boundaries live in [`workstreams.md`](../workstreams.md).

---

## 1. Principles

- freeze public semantics before optimizing internals
- freeze collection semantics before building projections
- freeze surface / resource identity before building APIs
- build live ingestion and backfill through the same pipeline
- keep raw facts immutable and projections disposable
- treat verified execution as a first-class subsystem from the start
- measure replacement by consumer capability coverage, not legacy API parity
- prefer cohesive end-to-end slices over broad scaffolding with undefined semantics
- freeze cross-workstream interfaces before parallel implementation starts
- keep chain deployment profiles explicit and mutually exclusive within one canonical corpus

---

## 2. Delivery Milestones

The plan is organized around four milestone types:

### Milestone A: Truth Core

The system can ingest, normalize, bind public surfaces to backing resources, and replay deterministically.

### Milestone B: Declared-State Product Slice

The system can serve native `v1` reads for exact names, address collections, child collections, roles, and history from declared state.

### Milestone C: Verified Product Slice

The system can serve verified resolution and primary-name answers with full provenance.

### Milestone D: Consumer Replacement Slice

The first-party apps can switch to the native `v1` contract without relying on the existing ENSv1/v2 indexer API shape.

Implementation should use the workstream overlay in `docs/internal/workstreams.md` once the Phase 0 docs and ADRs are frozen.

---

## 3. Phase 0: Scope Freeze

### Goal

Turn the revised architecture into implementation-ready semantics for the first build.

### Deliverables

- versioned `v1` resource definitions
- explicit compatibility policy: no legacy subgraph / GraphQL parity requirement
- query semantics for `namespace`, `at`, `chain_positions`, `consistency`, and `mode`
- frozen coverage and exhaustiveness definitions
- frozen canonicality rules
- frozen namespace assignment rules
- frozen `NameSurface` and `SurfaceBinding` taxonomy
- frozen collection semantics for:
  - exact name lookup
  - address → names
  - address → names with role summary
  - name → children
  - history scopes
  - resolver overview
- frozen `RecordInventory` semantics
- frozen preimage observation model
- frozen chain-intake contract for hash-first lineage, checkpoint promotion, and reorg handling
- frozen deployment-profile rule: ship the mainnet profile first, with later Sepolia support as a separate single-profile option rather than a concurrent chain set
- frozen consumer capability matrix in `docs/consumer-capabilities.md`
- frozen workstream boundaries and ownership for shared crates
- initial ADRs for stack and repo layout

### Exit Criteria

- no unresolved ambiguity in the first `Name`, `Address`, `Resolution`, `PrimaryName`, and `Resolver` reads
- no unresolved ambiguity in surface-vs-resource behavior
- no unresolved ambiguity about what belongs to `ens` vs `basenames`
- no unresolved ambiguity in what “replacement” means for first-party consumers

---

## 4. Phase 1: Repository Skeleton

### Goal

Create the implementation structure without committing to too much behavior yet.

### Deliverables

- Rust workspace
- initial API, indexer, and worker binaries
- shared crates for domain, storage, manifests, adapters, and execution
- local development environment
- migration framework
- baseline observability and config loading
- CI for formatting, linting, tests, and migrations

### Exit Criteria

- the repo can boot an API process, an indexer process, and a worker process
- local development can create the database schema and run tests
- docs and ADR folders are wired into the repo structure
- repo ownership matches `docs/internal/workstreams.md`

---

## 5. Phase 2: Storage Foundation

### Goal

Make the raw-fact, identity, and replay model concrete.

### Deliverables

- chain-lineage schema
- single chain header-anchor storage, optional header-audit storage, and raw transaction, receipt, and log tables
- manifest tables
- source-discovery tables
- `NameSurface` / `SurfaceBinding` / `resource_id` schema
- normalized-event tables
- preimage observation tables
- canonicality model
- provenance references
- execution-trace schema
- replay checkpoints

### Exit Criteria

- one chain segment can be ingested into raw facts and replayed into normalized events
- a public name surface can be bound to a backing resource in storage
- orphaned facts can be marked and excluded from canonical projections
- execution traces can be persisted even before full verified execution is enabled

---

## 6. Phase 3: Intake, Manifests, And Discovery

### Goal

Ingest the shipped mainnet profile (`ethereum-mainnet` plus `base-mainnet`) and build the canonical source graph.

### Deliverables

- Ethereum intake
- Base intake
- code-hash capture
- manifest bootstrapping
- contract discovery graph
- proxy / implementation tracking
- discovery-edge provenance
- watch-list expansion from manifests and discovery edges

### Exit Criteria

- the system can follow both chains forward
- every watched contract is explainable through a manifest or discovery edge
- manifest changes propagate into normalized-event context
- later additive Sepolia support remains a separate profile choice; the runtime does not ingest mainnet and Sepolia simultaneously

### Planned Follow-On

After the shipped mainnet profile is stable, add Sepolia support as an alternate deployment profile using `ethereum-sepolia` plus `base-sepolia`.

Requirements for that follow-on:

- reuse the same manifest, intake, discovery, and API semantics as the mainnet profile
- make runtime profile selection explicit so one deployment chooses either the mainnet profile or the Sepolia profile
- keep mainnet and Sepolia mutually exclusive within one canonical corpus, watch plan, and projection set

---

## 7. Phase 4: ENSv1 Adapter Slice

### Goal

Translate ENSv1 raw facts into stable internal events and serve the first useful declared-state answers.

### Deliverables

- ENSv1 registry adapter
- ENS `.eth` registrar adapter
- Name Wrapper adapter
- resolver adapter for supported record families
- dynamic resolver discovery from admitted ENSv1 registry `NewResolver` observations (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)
- reverse / primary adapter
- preimage observation from ENSv1 name-bearing events
- first normalized-event rules for:
  - registration
  - renewal
  - expiry
  - wrapper ownership / expiry / fuse-relevant changes
  - resolver changes
  - record changes
  - reverse changes
- first declared projections:
  - name snapshot
  - address names
  - record inventory
  - history
  - primary claim snapshot

### Exit Criteria

- a minimal ENSv1 `.eth` name can be looked up by surface name
- wrapped and unwrapped ENSv1 names can be represented without identity confusion
- declared child enumeration works for direct declared children in supported ENSv1 cases
- history reconstructs from normalized events without manual patching
- ENSv1 declared record and resolver-overview support does not claim consumer replacement until resolver addresses observed through registry state are admitted into the resolver source family, watched dynamically, and admitted as supported resolver profiles for the relevant fact families (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)

### Shared-Interface Freezes

- `phase4-ensv1-wrapper-resolver-source-family-admission`: ENSv1 Phase 4 admits `ens_v1_wrapper_l1` as the NameWrapper source-family owner for current wrapper-backed authority, wrapper-token holder, fuse, expiry, wrapper-revealed name, and wrapper-driven registry resolver / TTL observations, and admits `ens_v1_resolver_l1` as the PublicResolver source-family owner for declared resolver record state, record-version observations, and resolver-local authorization facts; those admissions are adapter input boundaries only and do not add wrapper / migration history support, public routes, route coverage graduation, primary-name fallback sources, verified execution widening, or consumer-replacement claims (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L666 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L676 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L13 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f).
- `phase4-ensv1-dynamic-resolver-discovery-contract`: ENSv1 declared record support admits dynamic resolver discovery from canonical `NewResolver(node, resolver)` observations on admitted ENSv1 registry emitters; a nonzero resolver creates or refreshes the node-to-resolver binding and resolver contract instance under `ens_v1_resolver_l1`, while a zero-address resolver closes only the affected node-to-resolver binding. Contract admission / watch lifetime, node binding lifetime, and supported resolver-profile admission are separate gates: resolver-local record / version / authorization facts may be consumed only after direct manifest or resolver-edge admission plus supported profile admission for the relevant fact family. Static PublicResolver admission remains a seed, not the complete consumer-replacement resolver corpus (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f).
- `phase4-ensv1-publicresolver-discovery-profile-admission`: ENSv1 discovered resolver instances can graduate from watched-target-only state to resolver-local fact consumption only through explicit PublicResolver-compatible supported-profile admission for the relevant fact families. Unknown dynamic resolvers remain admitted watch targets with explicit derived `pending` or `unsupported` resolver-profile state; they must not populate record inventory, record cache, or resolver overview supported sections. This is not a manifest schema change, does not require a new storage migration in this freeze, and leaves Basenames resolver-profile admission to its separate Phase 8 freeze (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L23 @ ens_v1@91c966f).

---

## 8. Phase 5: ENSv2 Adapter Slice

### Goal

Add ENSv2-native discovery, resources, permissions, and surface-binding behavior.

### Deliverables

- RootRegistry / ETHRegistry adapter
- UserRegistry adapter
- ETHRegistrar adapter
- PermissionedResolver adapter
- ENSv2 preimage observation rules
- ENSv2 resource / token-lineage handling
- `TokenRegenerated` handling
- `TokenResourceLinked` handling
- `SubregistryChanged` / `ParentChanged` graph expansion
- alias and wildcard topology events
- role and permissions projections
- resolver index projection
- surface-binding projection for:
  - declared registry paths
  - linked subregistries
  - alias-derived surfaces where representable
  - observed wildcard surfaces

### Exit Criteria

- ENSv2 names can be indexed without treating token IDs as stable identity
- one backing resource can surface under multiple public names
- linked-subregistry behavior is represented explicitly
- resolver-centric and resource-centric permissions reads work for the first supported slice

### Shared-Interface Freezes

- `phase5-ensv2-sepolia-dev-ref-profile-manifest-admission`: the first ENSv2 alternate-profile manifests live at `manifests/sepolia/ethereum/<namespace>/<source_family>/v1.toml`, reuse the same manifest schema, and are loaded as one selected profile at a time; the initial Sepolia source-family split admits `ens_v2_root_l1` for `RootRegistry`, `ens_v2_registry_l1` for `ETHRegistry` and discovered user registries, `ens_v2_registrar_l1` for `ETHRegistrar`, and `ens_v2_resolver_l1` for `PermissionedResolver` resolver state, while additional upstream `sepolia-dev` artifacts stay outside current admission until a later doc-first expansion (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/UserRegistryImpl.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/PermissionedResolverImpl.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/UniversalResolverV2.json:L2 @ ens_v2@554c309).
- `phase5-ensv2-adapter-event-contract-clarification`: ENSv2 adapters now have frozen normalized event/resource semantics for `TokenResourceLinked`, `TokenRegenerated`, `SubregistryChanged`, `ParentChanged`, `AliasChanged`, and Permission events: registry resources are EAC resources rather than token IDs, token regeneration updates token attributes without rebinding the resource or surface, subregistry and parent logs expand the discovery graph, alias logs populate resolver topology, wildcard topology is observation-backed rather than manifest-inferred, and registry/resolver EAC role deltas feed the resource-centric permission model (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309).
- `phase5-ensv2-preimage-observation-completion`: ENSv2 registry, registrar, and resolver name-bearing events may produce adapter-owned preimage observations: registry `LabelRegistered`, `LabelReserved`, and `ParentUpdated`; registrar `NameRegistered` and `NameRenewed`; and resolver `AliasChanged`, `NamedResource`, `NamedTextResource`, and `NamedAddrResource`. This is intake truth only: it persists identity/preimage facts and normalized events, writes no projection rows, does not change manifest capability state, and does not by itself promote public exact-name support; exact-name profile support is now promoted only through the selected `sepolia-dev` profile's `ens_v2_registrar_l1 exact_name_profile = "supported"` capability (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L30 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L132 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L142 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L153 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309).
- `phase5-ensv2-exact-name-profile-support-promotion`: the ENSv2 exact-name profile is promoted to `supported` only for the selected `sepolia-dev` deployment profile and only through `ens_v2_registrar_l1` declaring `exact_name_profile = "supported"`; the support class covers declared exact-name profile reads backed by admitted `ETHRegistry` and `ETHRegistrar` facts, and it does not apply to mainnet, other Sepolia profiles, resolver-profile support, universal resolver / execution, reverse, DNS, wrapper, migration, verified resolution, primary-name support, history coverage, or consumer replacement for other capability groups (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L22 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309).
- `phase5-ensv2-history-readback-lock`: ENSv2 history readback coverage is locked to the existing shared history routes: `GET /v1/history/names/{namespace}/{name}`, `GET /v1/history/resources/{resource_id}`, and `GET /v1/history/addresses/{address}`. The coverage reads canonical normalized events through the existing surface/resource identity and address-relation anchors, preserves the shipped `surface` / `resource` / `both` scope behavior and replay-stable paging, and keeps the shared empty `declared_state` history envelope. This does not change public API semantics, add an ENSv2-specific history ledger, graduate additional manifest capabilities, add verified execution or universal resolver support, widen the selected `sepolia-dev` exact-name support promotion, or promote exact-name support for any other profile.

---

## 9. Phase 6: Declared-State API Slice

### Goal

Expose the first stable native `v1` surface for declared-state product reads.

### Deliverables

- `Name` read
- `Address` read
- `Resolver` read
- `History` read
- `Permissions` read
- declared `Resolution` read with topology + record inventory + record cache
- explain views for:
  - `GET /v1/explain/names/{namespace}/{name}/surface-binding`
  - `GET /v1/explain/names/{namespace}/{name}/authority-control`
  - `GET /v1/coverage/{namespace}/{name}`
- shipped shared history routes plus exact-name `declared_state.history.{surface_head,resource_head}` satisfy the Phase 6 history-explain deliverable; no separate exact-name history-explain route is introduced in this phase
- machine-readable contract output frozen to the publication location `docs/api-v1.openapi.json`; when generated, it covers only the routes currently shipped by `apps/api/src/main.rs`
- replay-stable `cursor` / `page_size` plus frozen default sorts for shipped collection reads
- resolver overview alias summary sourced from current resolver-linked bindings

### Minimum supported reads

- exact name lookup
- `Address.names`
- `Address.names` with `include=role_summary`
- name → children
- name / resource → role holders
- name history
- resource history
- address history
- resolver overview

`Address.names` with `include=role_summary` stays the same declared-state address collection with an additive expansion, and `Address.history` stays the address-derived variant of the shared normalized-event history contract rather than a second route family.

### Exit Criteria

- first-party app data requirements that rely only on declared state can be served from native `v1`
- every response includes provenance, coverage, and chain-position context
- collection semantics match the frozen phase 0 definitions

### Current contract-freeze status

- `phase6-surface-binding-authority-explain-contract-clarification`: `GET /v1/explain/names/{namespace}/{name}/surface-binding` and `GET /v1/explain/names/{namespace}/{name}/authority-control` are frozen in the shared docs and shipped by `apps/api/src/main.rs` as exact-name-scoped declared-state explain routes over existing truth families
- `phase6-shipped-read-pagination`: `cursor` and `page_size` are frozen for `GET /v1/addresses/{address}/names`, `GET /v1/names/{namespace}/{name}/children`, `GET /v1/resources/{resource_id}/permissions`, `GET /v1/history/addresses/{address}`, `GET /v1/history/names/{namespace}/{name}`, and `GET /v1/history/resources/{resource_id}`; no other shipped route honors those query parameters in the initial contract
- `phase6-openapi-contract-output-clarification`: machine-readable contract output is frozen to the publication location `docs/api-v1.openapi.json`; when generated, it covers only the routes currently shipped by `apps/api/src/main.rs`, including the shipped `GET /v1/primary-names/{address}` route
- `phase6-resolver-overview-alias-summary-support`: supported `declared_state.aliases` on `GET /v1/resolvers/{chain_id}/{resolver_address}` is shipped as the `binding_kind=resolver_alias_path` subset of current resolver-linked bindings with the shared `{status, count, items}` summary envelope, including `count=0` with `items=[]` when no current alias binding exists

---

## 10. Phase 7: Verified Execution Slice

### Goal

Add the execution plane that turns declared state into verified answers.

### Deliverables

- ENS Universal Resolver execution
- alias-aware execution
- wildcard handling
- CCIP-Read support
- Basenames compatibility execution scaffolding
- primary-name verification engine
- execution trace persistence
- cache and invalidation rules
- `Resolution` reads in `verified` and `both` modes
- namespace-inferred resolution convenience route: `GET /v1/resolve/{name}`
- `PrimaryName` reads in `verified` and `both` modes
- `GET /v1/explain/resolutions/{namespace}/{name}/execution` explain view over persisted resolution execution traces

### Exit Criteria

- explicit record reads work in `verified` mode for the initial supported slice
- primary-name reads distinguish claim from verified result
- every verified answer produces traceable provenance and execution traces
- relevant topology or record changes invalidate cached verified answers deterministically

### Current contract-freeze and shipped progress

- `phase7-resolution-execution-explain-contract-clarification`: `GET /v1/explain/resolutions/{namespace}/{name}/execution` is shipped in the shared docs and machine-readable contract as a verified-state explain route for the same current exact surface and explicit selector set as `GET /v1/resolutions/{namespace}/{name}`; it reads persisted execution traces only, stays route-local instead of becoming a raw trace dump, and the current published handler contract exposes path parameters plus required `records` only
- `phase7-verified-resolution-support-boundary-clarification`: the shipped mixed `GET /v1/resolutions/{namespace}/{name}` contract and shipped `GET /v1/explain/resolutions/{namespace}/{name}/execution` envelope stay stable, and public verified support remains frozen to ENS exact-surface direct-path reads, the already frozen alias-only non-direct class, and the first additive exact-surface wildcard-derived class, using the same declared-topology support check as the route contract; other ENS non-alias ancestor-selected paths, linked-subregistry ancestor-selected paths, transport-assisted ENS paths, and CCIP-participating ENS traces remain explicit unsupported until a later doc-first contract update broadens the ENS slice
- `phase7-namespace-inferred-resolution-route`: `GET /v1/resolve/{name}` now ships in the handler and machine-readable publication as a namespace-inferred convenience route for the same `ResolutionResponse` envelope as canonical `GET /v1/resolutions/{namespace}/{name}`; the shipped handler currently exposes only `mode` and `records`; exact `base.eth` infers `namespace=ens`, names matching `*.base.eth` infer `namespace=basenames`, other supported ENS names infer `namespace=ens`, and the response still exposes canonical namespaced identity through `data.namespace` and `data.logical_name_id`; namespace inference is separate from verified support, so inferred Basenames selectors use Basenames-local support and return selector-local `unsupported` instead of falling back to ENS outside the frozen Basenames direct transport-assisted verified class
- `phase7-ens-verified-resolution-wildcard-path-contract-clarification`: the first additive ENS wildcard-derived verified-resolution support class is now frozen on both the mixed and explain routes: it admits persisted ENS exact-surface answers only when `wildcard.source` is non-`null` with `matched_labels` non-empty, `resolver_path[0].logical_name_id` equals `wildcard.source.logical_name_id`, `alias.final_target=null` with `hops=[]`, `subregistry_path=[]`, and all `transport` fields are `null`; alias-only support stays as already frozen, while transport-assisted ENS paths, CCIP-participating ENS traces, and other non-alias ancestor-selected or linked-subregistry ENS paths remain unsupported and machine-readable publication in `docs/api-v1.openapi.json` stays unchanged
- `phase7-primary-name-route-envelope-bootstrap`: `GET /v1/primary-names/{address}` is shipped as a head-only bootstrap on the already frozen mixed declared+verified route contract and enters machine-readable publication scope; it currently honors `namespace`, `coin_type`, and optional `mode`, while richer claim/verified payloads and broader Phase 7 verified execution deliverables remain pending
- `phase7-primary-name-claim-status-contract-clarification`: the shipped declared `claimed_primary_name` surface on `GET /v1/primary-names/{address}` is frozen to the exact `primary_names_current(address, coin_type, namespace)` tuple as a bootstrap declared contract; the route does not trigger fresh reverse-claim lookup while serving that bootstrap readback, the admitted ENS claim state remains reverse-only through `ens_v1_reverse_l1`, tuple-present declared reads stay limited to the exact tuple with exact-tuple declared `claimed_primary_name.name`, exact-tuple declared `claimed_primary_name.provenance`, and the separately frozen exact-tuple `invalid_name` `raw_claim_name` allowance beyond `status`, while route-level coverage is now governed by the later exact-tuple persisted-readback coverage contract
- `phase7-primary-name-claim-status-readback-bootstrap`: the shipped route now reads back the exact tuple's declared `claimed_primary_name.status` from `primary_names_current` in `mode=declared|both`; exact-tuple declared normalized claimed identity is governed by the separate claimed-name readback contract freeze, broader coverage remains outside this status-only slice, and any admitted `raw_claim_name` publication is governed by the separate exact-tuple `claim_status=invalid_name` contract freeze
- `phase7-primary-name-raw-claim-name-readback`: the prose contract for `GET /v1/primary-names/{address}` now admits `claimed_primary_name.raw_claim_name` only for the exact requested `(address, namespace, coin_type)` tuple and only when declared `claim_status=invalid_name`, copied verbatim from `primary_names_current.raw_claim_name`; `claimed_primary_name.name` is separately frozen to the same exact requested row's declared normalized claim-identity source, fallback claim sources beyond the admitted reverse-only ENS surface remain deferred, and route-level primary-name coverage is now limited to the exact-tuple persisted-readback contract
- `phase7-primary-name-claim-provenance-contract-clarification`: the prose contract for `GET /v1/primary-names/{address}` now admits `claimed_primary_name.provenance` as the first public claim-local section provenance: exact-tuple declared-only provenance from the requested `primary_names_current(address, coin_type, namespace)` row, stripped of `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material, and with no `execution_trace_id`; `claimed_primary_name.name` is separately frozen to that same exact requested row's declared normalized claim-identity source, must not be backfilled from manifest presence, resolver-backed identity, verified execution identity, tuple presence alone, or fallback claim sources, and remains distinct from execution-derived `verified_primary_name.name` without widening the exact-tuple persisted-readback coverage contract
- `phase7-primary-name-claimed-name-readback-contract-clarification`: the prose contract for `GET /v1/primary-names/{address}` now admits `claimed_primary_name.name` only from the exact requested `primary_names_current(address, coin_type, namespace)` row's declared normalized claim-identity source for that same tuple, aligned with the currently admitted reverse-only ENS claim precedence; manifest presence, resolver-backed identity, verified execution identity, tuple presence alone, different-tuple state, and fallback claim sources must not synthesize or backfill it; this clarification does not change when `verified_primary_name.name` appears, and it does not widen the exact-tuple persisted-readback coverage contract
- `phase7-primary-name-capability-ownership-clarification`: the shipped ENS primary-name slices do not introduce dedicated `claimed_primary_name` or `verified_primary_name` manifest capability flags; `ens_v1_reverse_l1` remains active with empty capability flags for declared reverse-claim intake, `ens_execution` remains the execution owner without a separate verified-primary flag, and any later primary-name-specific manifest gating would be a doc-first additive change
- `phase7-verified-primary-support-boundary-clarification`: the first additive ENS `verified_primary_name` slice is frozen to persisted/readback verified results for the same `(address, namespace, coin_type)` tuple as `GET /v1/primary-names/{address}`; it uses stable execution identity `request_type=verified_primary_name` and request-key identity `{namespace}:{normalized_address}:{coin_type}`, may use only `primary_names_current(address, coin_type, namespace)` as the claim-side lookup / invalidation anchor, keeps top-level provenance separate from claim-local and verification-local section provenance, supplies the ENS side of the exact-tuple persisted-readback coverage contract, and does not admit richer ENS tuple-present claimed or verified payloads without a later doc-first contract update
- `phase7-verified-primary-provenance-contract-clarification`: the shared contract docs now ship `verified_primary_name.provenance` as a strict verification-local refinement for the exact requested tuple under the same top-level `provenance.execution_trace_id`, with published shape limited to `execution_trace_id` plus `manifest_versions`; `execution_trace_id` must equal top-level `provenance.execution_trace_id`, `manifest_versions` must narrow the same persisted verification trace, and the field must not publish `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material, restate claimed-row provenance, widen request-key or invalidation identity, imply primary-name coverage beyond the exact-tuple persisted-readback contract, or publish other `Provenance` fields at this section-local boundary
- `phase7-ens-verified-primary-route-readback-bootstrap`: the shipped `GET /v1/primary-names/{address}` route now reads back persisted ENS `verified_primary_name` results for the exact requested tuple in `mode=verified|both`, using the already frozen mixed-route envelope and the same machine-readable publication in `docs/api-v1.openapi.json`, including the shipped verification-local `verified_primary_name.provenance` object `{execution_trace_id, manifest_versions}`; declared claim behavior remains bootstrap exact-tuple readback under the separately frozen `status`, `claimed_primary_name.name`, `claimed_primary_name.provenance`, and `raw_claim_name` boundaries, tuple-present fallback `unsupported` behavior still applies outside the ENS persisted-readback support class, and route-level primary-name coverage participates in the exact-tuple partial coverage contract
- `phase7-phase8-primary-name-coverage-graduation-contract-and-readback`: the local `GET /v1/primary-names/{address}` coverage contract now graduates only the frozen ENS and Basenames exact-tuple persisted-readback classes: supported tuples return route-level `coverage.status=partial`, `exhaustiveness=non_enumerable`, `enumeration_basis=primary_name_lookup`, namespace-local `source_classes_considered`, and `unsupported_reason=null`; tuples outside those frozen classes remain explicit `coverage.status=unsupported` with `unsupported_reason="primary-name exact-tuple persisted readback is not supported for the requested tuple"`, and this is not an external app parity or first-party app replacement claim

---

## 11. Phase 8: Basenames Slice

### Goal

Add Basenames as a first-class public namespace, using the same architecture rather than a sidecar model.

### Deliverables

- Base-side registry / registrar / resolver adapters
- dynamic Base-side resolver discovery from admitted Basenames registry `NewResolver` observations (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
- Basenames primary-name support
- Base authority projections
- L1 compatibility transport context in resolution
- Basenames-specific control facets:
  - token ownership
  - management rights
  - address-resolution target
- Basenames-specific coverage and explain rules
- Basenames history support

### Exit Criteria

- `basenames` reads are served through the same native `v1` contract
- Base authority and L1 transport are clearly separated in provenance
- Basenames transfer scenarios map correctly onto `ControlVector`
- Basenames declared record and resolver-overview support does not claim consumer replacement until Base-side resolver addresses observed through registry state are admitted into `basenames_base_resolver`, watched dynamically, and admitted as supported resolver profiles for the relevant fact families (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)

### Shared-Interface Freezes

- `phase8-basenames-read-plane-boundary-clarification`: the first public Basenames read-plane boundary is frozen across the shared docs, aligned to the existing six-family manifest split: exact-name, address-name, and children reads take declared truth from `basenames_base_registry`, `basenames_base_registrar`, and `basenames_base_resolver`; `basenames_base_primary` remains claim intake only; `claimed_primary_name` and `verified_primary_name` stay distinct route-local objects; and `Resolution.topology` publishes the separate Base-authority-plus-L1-transport split as Base-side `registry_path` / `resolver_path` plus `transport = {source_chain_id=\"base-mainnet\", target_chain_id=\"ethereum-mainnet\", contract_address=\"0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31\", ...}` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- `phase8-basenames-dynamic-resolver-discovery-contract`: Basenames declared record support admits dynamic Base-side resolver discovery from canonical `NewResolver(node, resolver)` observations on admitted `basenames_base_registry` emitters; a nonzero resolver creates or refreshes the node-to-resolver binding and resolver contract instance under `basenames_base_resolver`, while a zero-address resolver closes only the affected node-to-resolver binding. Contract admission / watch lifetime, node binding lifetime, and supported resolver-profile admission are separate gates: resolver-local record and authorization facts may be consumed only after direct manifest or resolver-edge admission plus supported profile admission for the relevant fact family. Static `L2Resolver` admission remains the default resolver seed, not the complete consumer-replacement resolver corpus, and this rule does not discover the Ethereum Mainnet L1 resolver or offchain gateways (upstream: .refs/basenames/src/L2/Registry.sol:L19 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc).
- `phase8-basenames-l2resolver-discovery-profile-admission`: Basenames discovered Base-side resolver instances can graduate from watched-target-only state to resolver-local fact consumption only through explicit `L2Resolver`-compatible supported-profile admission for the relevant fact families. Unknown dynamic Base resolvers remain admitted watch targets with explicit derived `pending` or `unsupported` resolver-profile state; they must not populate record inventory, record cache, or resolver overview supported sections. This Basenames gate is separate from the ENSv1 PublicResolver-compatible gate and from Basenames L1 transport / execution, is not a manifest schema change, and does not require a new storage migration, shared enum, or API route widening in this freeze (upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc).
- `phase4-phase8-registry-resolver-discovery-bootstrap`: ENSv1 and Basenames registry `NewResolver` discovery admission is frozen as source-graph admission and node-binding state only. It admits watched resolver contract instances under the relevant resolver source family, but resolver-profile support, typed record facts, resolver overview coverage, route coverage graduation, and consumer replacement remain blocked until the specific resolver instance has supported profile admission for the relevant fact family (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc).
- `phase8-basenames-verified-resolution-support-boundary-clarification`: the public Basenames verified / explain path class is now supported only for the exact-surface transport-assisted direct-path class where `resolver_path[0].logical_name_id` equals the route surface, `wildcard.source=null` with `matched_labels=[]`, `alias.final_target=null` with `hops=[]`, `subregistry_path=[]`, `transport.source_chain_id=\"base-mainnet\"`, `transport.target_chain_id=\"ethereum-mainnet\"`, and `transport.contract_address=\"0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31\"`; CCIP-participating traces for that class are part of the supported public surface because the upstream `L1Resolver` initiates `OffchainLookup` for non-`base.eth` requests and completes them through `resolveWithProof`, while other Basenames verified path classes remain unsupported until a later doc-first expansion (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)
- `phase8-basenames-verified-resolution-promotion-doc-lock`: active `basenames_execution` v2 carries `verified_resolution=supported` for only that exact transport-assisted direct-path class; historical v1 shadow state is no longer the current shared-interface rule, and the promotion does not move transport ownership away from `basenames_l1_compat`, widen route-level primary-name coverage beyond the separate exact-tuple persisted-readback contract, or add a dedicated primary-name manifest flag (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
- `phase8-basenames-verified-primary-readback-support-class-clarification`: the first Basenames `verified_primary_name` support class on `GET /v1/primary-names/{address}` is frozen to exact-tuple persisted readback for the requested `(address, namespace=basenames, coin_type)` tuple only; it uses stable execution identity `request_type=verified_primary_name` and request-key identity `{namespace}:{normalized_address}:{coin_type}`, keeps `primary_names_current(address, coin_type, namespace)` as the only claim-side lookup / invalidation anchor, limits `verified_primary_name.provenance` to `{execution_trace_id, manifest_versions}` under the same top-level `provenance.execution_trace_id`, stays execution-derived under `basenames_execution` rather than `basenames_base_primary`, adds no dedicated primary-name manifest flag, and supplies the Basenames side of the exact-tuple persisted-readback coverage contract while preserving the declared / verified split because upstream keeps reverse-name writes on the Base ReverseRegistrar while verified resolution enters through the separate Ethereum Mainnet `L1Resolver` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)

### Current contract-freeze and shipped progress

- `phase8-basenames-verified-primary-readback-bootstrap`: the shipped `GET /v1/primary-names/{address}` route now reads back persisted Basenames `verified_primary_name` results for the exact requested `(address, namespace=basenames, coin_type)` tuple in `mode=verified|both`, reusing the already-frozen support boundary and the same machine-readable publication in `docs/api-v1.openapi.json`, including the verification-local `verified_primary_name.provenance` object `{execution_trace_id, manifest_versions}`; declared claim behavior remains exact-tuple bootstrap readback under the separate claimed-primary boundary, tuple-present fallback `unsupported` still applies outside the Basenames persisted-readback support class, and route-level primary-name coverage participates in the exact-tuple partial coverage contract

---

## 12. Phase 9: Reorg, Replay, And Backfill

### Goal

Harden the system around historical correctness.

### Deliverables

- fork detection
- canonical invalidation
- deterministic replay tooling
- historical backfill tooling
- `phase9-reorg-execution-cache-invalidation`: reorg repair invalidates `execution_cache_outcomes` for verified resolution and verified primary-name outcomes that depend on orphaned block identities; execution traces and execution steps remain durable audit artifacts; rows without explicit block-hash-bearing dependencies fail closed unless they are explicitly out of scope; this is a reorg/replay foundation and does not change the selected ENSv2 exact-name support boundary, promote additional deployment profiles, or graduate any manifest capability
- `phase9-backfill-job-checkpoint-substrate`: persisted backfill jobs are bounded by explicit profile, chain, source selector, scan mode, and finite block range; storage helpers provide idempotent create, reserve, advance, complete, and fail transitions over resumable range checkpoints; those checkpoints are operational backfill progress only and never promote canonical, safe, or finalized chain checkpoints
- `phase9-resumable-backfill-job-runner`: contract frozen for the indexer/backfill-owned `bigname-indexer backfill` command to create or reuse bounded jobs by idempotency key, reserve leased ranges, advance range checkpoints monotonically, complete only when all range checkpoints reach declared ends, and fail with recorded metadata without mutating or promoting `canonical_head`, `safe_head`, or `finalized_head`; this does not claim broader replay, finality, manifest, public API, additional ENSv2 profile, or consumer-replacement support
- `phase9-source-scoped-backfill-runner`: contract frozen for `bigname-indexer backfill` selector semantics: default whole-active-watched-chain selection, `--source-family <family>` selection, and explicit watched-target-set selection resolve to a stable sorted source identity; idempotency-key reuse conflicts if the selector shape or resolved target set drifts; selected-target-only intake remains block-hash-scoped and never promotes canonical, safe, or finalized chain checkpoints
- `phase9-indexer-run-auto-backfill-bootstrap`: shared-interface/doc-first contract frozen for `bigname-indexer run` automatic bootstrap to create finite persisted backfill jobs from manifest-declared `start_block` values only; omitted `start_block` means unknown and must be skipped explicitly, provider-missing active watched chains remain idle after manifest/watch/checkpoint setup, source identity is resolved watched targets keyed by `contract_instance_id` plus effective range rather than raw address, overlapping eligible source families share raw-fact job segments by default, ENSv1 generic resolver events are represented as a `generic_topic_scans` source-identity component rather than as an address filter, and bootstrap lifecycle does not mutate `canonical_head`, `safe_head`, or `finalized_head`; this does not graduate API coverage, manifest capabilities, or consumer-replacement support
- `phase9-canonicality-inspection-tooling`: read-only worker-owned canonicality inspection tooling uses storage audit helpers for the single-block command `bigname-worker inspect canonicality --chain-id <id> --block-hash <hash>`; it reports whether one requested `(chain_id, block_hash)` has a stored lineage row and, for stored rows, lineage, canonicality state, parent/number, raw fact counts, and normalized-event counts; it does not inspect spans or infer absent heights/gaps, does not expose a public `v1` API, does not let API code bypass projection/execution read boundaries, and does not change the selected ENSv2 exact-name support boundary, promote additional deployment profiles, or graduate any manifest capability
- `phase9-backfill-job-inspection-cli`: contract frozen for the read-only worker command `bigname-worker inspect backfill-job --backfill-job-id <id>` to render stable JSON for one persisted job plus child ranges, including lifecycle, lease, range checkpoint, attempt count, timestamp, and failure metadata; it does not mutate storage, expose a public `v1` API, promote chain heads, or claim broader replay, finality, manifest, additional ENSv2 profile, or consumer-replacement support
- `phase9-replay-json-summary-output`: contract frozen for worker-owned operational output only from `bigname-worker replay all-current-projections --json`; stdout is a stable summary with `command`, ordered `projections` entries carrying `projection`, `requested`, `upserted`, and `deleted`, plus `totals` carrying summed `requested`, `upserted`, and `deleted`; non-JSON behavior is unchanged and this does not expose public `v1` API support, graduate manifest capabilities, or claim consumer replacement
- `phase9-raw-fact-normalized-event-replay-runner`: contract frozen for a bounded operational runner that performs an upsert-only adapter resync of `normalized_events` from already persisted canonical raw facts through the adapter-owned `normalized_events` boundary; it does not delete stale rows or replace existing payloads, does not use provider history as a substitute for persisted selected replay facts, and performs no projection rebuild, public `v1` exposure, manifest capability mutation, chain checkpoint promotion, or backfill job/range checkpoint mutation
- `phase9-stored-lineage-range-inspection-cli`: contract frozen for read-only worker-owned stored lineage range inspection over a finite chain/block range; it lists only stored lineage rows in stable `(block_number, block_hash)` order and renders stable JSON per observed block, without inferring missing heights, gaps, span-wide canonicality, aggregate finality, or completeness and without mutating storage
- `phase9-wrapper-resolver-source-family-backfill-lock`: doc-first conformance meaning frozen to prove that completed ENSv1 wrapper, ENSv1 resolver, and Basenames source-family job lifecycle state can coexist with replayed existing shipped consumer-capability responses and guard the non-graduation boundary; it does not prove synthetic jobs admitted route data, graduate unsupported coverage, widen the selected ENSv2 exact-name support promotion to other profiles, claim wrapper/migration history, change manifest capabilities, add public API routes, or create new consumer-replacement semantics (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc)
- dispute and inspection tooling
- backfill jobs for:
  - ENSv1
  - wrapper / migration history
  - ENSv2
  - Basenames
- consumer capability conformance jobs against backfilled data

### Exit Criteria

- a simulated reorg rebuilds correct current answers
- historical backfill reuses the same adapter and projection path as live ingestion
- replay determinism holds from raw facts and from normalized events
- execution cache invalidation makes orphaned-block-dependent `execution_cache_outcomes` ineligible for reuse without deleting execution traces or execution steps
- backfill job and range helpers are resumable, idempotent, bounded, and separate from canonical, safe, and finalized chain checkpoint promotion
- the resumable backfill runner reuses jobs by idempotency key, leases bounded ranges, advances range checkpoints monotonically, completes or fails with recorded metadata, and leaves `canonical_head`, `safe_head`, and `finalized_head` untouched
- source-scoped backfill selection is deterministic across the three frozen selector modes, persists stable sorted target identity, conflicts on idempotency-key selector drift, and admits only selected target facts through block-hash-scoped intake without promoting chain heads
- automatic bootstrap creates only finite persisted backfill jobs for resolved watched targets with known inclusive `start_block`; it skips unknown-start targets, leaves provider-missing active watched chains idle after setup, persists `contract_instance_id` plus effective range source identity, groups overlapping eligible source families into shared raw-fact job segments by default, carries ENSv1 generic resolver-event intake as `generic_topic_scans`, and never promotes canonical, safe, or finalized chain checkpoints
- canonicality inspection reports stored lineage, parent/number, canonicality state, raw fact counts, and normalized-event counts for one requested `(chain_id, block_hash)` through worker-owned read-only tooling without adding a public API surface or making the single-block command infer range gaps
- backfill job inspection reports one persisted job and its ranges as stable read-only JSON with lifecycle, lease, checkpoint, attempt, timestamp, and failure metadata without adding a public API surface or mutating job, range, chain, projection, or execution state
- replay JSON reports all current projection families in stable order with per-projection and total `requested`, `upserted`, and `deleted` counts, while non-JSON replay output remains unchanged and operational-only
- raw-fact normalized-event replay is bounded to persisted canonical selected replay facts, preserves adapter ownership of `normalized_events`, and does not perform stale-row purge, payload replacement, provider-history substitution for selected replay facts, projection rebuilds, public API exposure, or chain/backfill checkpoint mutation
- stored lineage range inspection lists only stored lineage rows as stable JSON per observed block and remains read-only, with no missing-height, gap, span-wide canonicality, aggregate finality, or completeness inference
- wrapper/resolver/Basenames source-family backfill conformance locks completed source-family job state alongside replayed existing shipped responses and guards non-graduation; it does not prove synthetic jobs admitted route data or change unsupported coverage, manifest capabilities, additional ENSv2 profile support, wrapper/migration history, public API routes, or consumer-replacement semantics (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc)

---

## 13. Phase 10: Consumer Replacement And Hardening

### Goal

Replace the current indexer dependencies in first-party apps with the native `v1` contract and harden operations.

### Deliverables

- consumer adapter / SDK layer for the apps monorepo
- app-by-app migration plan
- capability-by-capability cutover checklist
- parity tests at the capability level, not the schema level
- dashboards and SLOs
- live audit jobs
- chaos / reorg drills
- manifest drift and proxy-upgrade alerting
- release and rollback runbooks

### Exit Criteria

- first-party apps no longer require the existing ENSv1/v2 indexer API surface
- remaining gaps are explicitly tracked as unsupported native `v1` capabilities
- operations can detect drift before it becomes user-visible

### Current contract-freeze status

- `phase10-execution-trace-inspection-cli`: worker-owned `bigname-worker inspect execution-trace --execution-trace-id <id> --json` is frozen as read-only operational JSON over persisted `execution_traces` and `execution_steps`; it does not create a public `v1` route, expose raw execution or gateway payload APIs, execute fresh resolution or primary-name verification, synthesize topology, mutate cache/projection/manifest/discovery state, or widen the public execution-explain route boundary.
- `phase10-live-manifest-drift-audit-job`: live manifest-drift and proxy-alert production is frozen as worker-owned operational observation over admitted manifests, stored code-hash facts, stored proxy / implementation edges, and derived watch-plan state; it may write only the worker-owned persisted alert observation family, preserves the proxy `contract_instance_id` while implementation churn remains an edge observation, and does not write adapter-owned `normalized_events`, mutate manifest truth, discovery admission, discovery edges, capability flags, watch-plan inputs, projections, public API state, or consumer-replacement meaning.
- `phase10-manifest-drift-alert-persistence-foundation`: the worker-owned persisted alert observation family is the only durable Phase 10 storage target for manifest/proxy drift observations; it records observation kind, lifecycle status, manifest version, source family, chain, contract-instance references, proxy / implementation edge references, expected and observed code-hash or implementation-edge material, derived watched-target metadata, timestamps, and nullable remediation metadata, without becoming manifest truth, discovery admission, watch-plan input, projection state, public API state, adapter-owned `normalized_events`, or a capability flag.
- `phase10-manifest-drift-alert-inspection-cli`: read-only manifest-drift and proxy-alert inspection is frozen as operational JSON over persisted worker-owned alert observations, including their source manifest, source family, chain, contract-instance references, proxy / implementation edge references, expected and observed code-hash material, derived watched-target metadata, timestamps, lifecycle status, and nullable remediation metadata; it does not perform fresh chain comparison, create observations, mutate alert lifecycle state, mutate manifest truth, mutate discovery admission, admit contracts, change capability flags, rewrite discovery edges, mutate watch-plan inputs, write projections, expose public API state, or claim consumer replacement.
- `phase10-runtime-watch-plan-inspection-cli`: worker-owned `bigname-worker inspect watch-plan --json` is frozen as read-only operational JSON over existing admitted watch-plan state, exposing active watched contracts / watch-plan entries with source kind (`manifest_root`, `manifest_contract`, or `discovery_edge`), source families, contract instance IDs, chain addresses, source manifest IDs when available, and active block ranges. It uses existing manifest/discovery state only and does not perform fresh chain comparison, admit contracts, mutate discovery edges, change capability flags, update watch-plan inputs, write projections, expose a public `v1` route, or claim consumer replacement.
- `phase10-reorg-chaos-drill-conformance-job`: conformance status is frozen around the focused command `cargo test --manifest-path tests/conformance/Cargo.toml reorg_chaos_drill_conformance_job`; the job uses the local per-test Postgres harness, seeds the existing replay-style stale current corpus, applies shipped reorg orphaning helpers, runs shipped raw-fact normalized-event replay over a deterministic canonical raw-log probe, runs all-current projection replay, and reuses existing consumer-response convergence and losing-branch absence assertions. When local Postgres is unavailable, the focused job is a no-run fallback rather than evidence of a route contract failure. This is a Phase 10 hardening drill over shipped route contracts only; it does not widen route coverage or semantics, graduate unsupported coverage, change the selected ENSv2 exact-name support boundary, promote additional deployment profiles, change manifest capabilities, add public API routes, or claim consumer replacement.
- `phase10-raw-retention-cache-policy`: raw-fact retention is frozen as a storage/replay policy in which Postgres remains the hot indexed store for durable replay facts, lineage/header anchors, selected/admitted target logs, replay-required call snapshots/enrichments, and retained payload-cache metadata, while large/full block payloads and non-indexed transaction/receipt/block bodies are evictable cache by default after durable replay facts are extracted. Hash-addressed cold storage is required only for raw payload classes explicitly declared durable; otherwise object storage is just one possible cache backend. Provider re-fetch may refill cache for block-scoped payloads only through an explicit block-hash-scoped, retained-digest-checked, fail-closed path; payloads without retained digests cannot satisfy that cache-fill contract, and provider re-fetch must not replace selected replay facts that Postgres is required to retain. Non-goals: no public `v1` route change, no manifest capability or rollout-status change, no consumer-replacement graduation, no exact retention-window value, no compaction job implementation, no default object-store requirement for every fetched payload, and no schema/migration promise beyond the documented metadata boundary. Success signal: implementation can identify durable hot facts versus evictable cache payloads for live intake and source-scoped backfill, reorg repair preserves orphaned audit truth without deleting replay inputs, and replay/conformance tests can prove selected replay facts do not depend on provider history while explicit cache re-fetch paths fail closed on missing digests, mismatched bytes, or missing historical data.
- `phase10-consumer-capability-cutover-evidence-bootstrap`: local cutover evidence is frozen as a capability-group table in `docs/consumer-capabilities.md` tying each shipped group to route owner, conformance owner, rollout gate, and rollback gate. It is local bigname evidence only; it does not claim external app parity or first-party app call-site replacement before the imported apps are mapped separately.
- `phase10-consumer-capability-golden-fixture-pack`: deterministic native bigname golden fixtures are accepted only as local cutover evidence for the capability groups in `docs/consumer-capabilities.md`; they do not prove external app parity, imported app call-site replacement, first-party app replacement, legacy schema parity, or consumer replacement beyond the locally frozen route/conformance boundaries.

---

## 14. Recommended First Vertical Slice

If we optimize for learning rather than scaffolding, the first complete slice should be:

1. exact ENS name lookup for a small canonical subset
2. surface binding + backing resource summary
3. declared authority and control
4. declared resolution topology
5. declared record inventory
6. verified address resolution
7. claimed vs verified primary-name answer
8. provenance and coverage in every response

This validates the central architecture before wider resolver-family expansion or long-tail backfills.

---

## 15. Recommended Second Vertical Slice

The second slice should prove that the architecture handles the parts the legacy model cannot express cleanly:

1. ENSv2 dynamic subregistries
2. linked-subregistry surfaces
3. token regeneration with stable resource identity
4. resource-centric permissions
5. resolver overview
6. role history
7. alias-aware verified resolution

If this slice works cleanly, the architecture is confirmed.

---

## 16. Consumer Capability Migration Plan

The apps monorepo should be migrated by capability group, not by endpoint mirroring.

### Group 1: Name profile and records

- exact name profile
- registration / expiry summary
- record inventory
- verified record reads

### Group 2: Collections

- address → names
- address → names with role summary
- name → children
- child counts

### Group 3: History and permissions

- name history
- address history
- role holders
- role history
- resolver overview

### Group 4: Primary names

- claimed primary
- verified primary
- failure states

Each group should have:

- contract tests
- app integration tests
- explain / provenance checks
- rollout and rollback criteria

---

## 17. Test Strategy

Tests must operate at four layers:

- raw facts
- normalized events
- projections / API
- verified execution traces

### Required suites

- fixture-based protocol tests
- forked-chain integration tests
- replay determinism tests
- reorg tests
- manifest drift tests
- consumer capability conformance tests
- performance tests for collection reads
- execution cache invalidation tests

---

## 18. Supersession Criteria

The system supersedes existing indexers for our stack when all of the following are true:

- the first-party apps use native `v1` reads for all required naming capabilities
- exact lookup, collections, permissions, history, resolution, and primary-name answers are served with explicit coverage / provenance
- ENSv2-native features are represented without legacy identity distortions
- Basenames are served as a first-class public namespace
- operational replay, reorg handling, and drift detection are production-hardened

This is a capability replacement, not a schema impersonation.

---

## 19. Companion Docs Required Before Coding Starts

These docs should exist and be treated as the interface baseline before repository bootstrap turns into parallel implementation:

- `docs/api-v1.md` for exact response shapes
- `docs/chain-intake.md` for lineage, fetch, and reconciliation rules
- `docs/storage.md` for schema strategy
- `docs/manifests.md` for manifest structure and capability flags
- `docs/projections.md` for collection semantics and indexes
- `docs/execution.md` for verified resolution and primary-name verification
- `docs/consumer-capabilities.md` for the checked-in consumer capability baseline
- `docs/internal/workstreams.md` for parallel delivery boundaries and ownership
- `docs/adrs/0001-stack.md` for the implementation stack decision
- `docs/adrs/0002-surface-resource-identity.md` for the surface / resource split
