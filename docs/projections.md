# Projections

Status: Phase 0 baseline

This document freezes the read-model boundaries between normalized events, current-state projections, and API reads.

The exact-name explain routes for surface-binding and authority-control now ship in the API binary. The primary-name route family also ships there as a mixed route: tuple-backed declared `claimed_primary_name` readback is wired, the admitted public claimed-local surfaces are exact-tuple declared `claimed_primary_name.name`, exact-tuple declared `claimed_primary_name.provenance`, and exact-tuple `raw_claim_name` for `invalid_name`, and route-level coverage is `partial` only for the frozen exact-tuple ENS/Basenames persisted-readback classes while out-of-class tuples remain `unsupported`. Its projection boundaries are nevertheless normative here so later support can land without changing the shared contract: `primary_names_current` stays claim-local, `claimed_primary_name.name` stays limited to the exact requested tuple row's declared normalized claim-identity source under the current reverse-only claim precedence, and the frozen `verified_primary_name.provenance` invariant stays verification-local under the same top-level `execution_trace_id` rather than becoming a projection-owned join. No separate history-explain route is queued: the shipped history routes remain the declared history answer, and exact-name `history` only stores head pointers into that contract. ENS reverse-only claim precedence is anchored to the Ethereum Mainnet Reverse Registrar and its reverse-record write path (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f).

## 1. Projection Rules

- projections are rebuildable from canonical facts and normalized events
- projections exist to serve stable reads, not to invent semantics
- every projection row carries provenance, manifest version, and chain position context
- only projection workers write projection tables
- exact-name public reads resolve `at`, explicit `chain_positions`, and `consistency` before reading projections; the selected positions then key one coherent `name_current`, `coverage_current`, `surface_bindings_current`, `permissions_current`, `record_inventory_current`, and execution-output join for that response
- projection readers must fail closed with the API's selector error state when the selected positions cannot be served from current projection or execution-output rows; they must not patch a missing snapshot by reading raw facts, adapter internals, or a newer projection row
- a current-state row with an older stored chain-position context may serve a later selected snapshot only when the projection reader can prove that no newer canonical projection input for that row's keys exists through the selected positions; otherwise the row is stale and must be rebuilt by the projection worker

## 2. Projection Families

| Projection | Primary key | Primary read | Source events |
| --- | --- | --- | --- |
| `name_current` | `logical_name_id` | exact name lookup | identity, registration, control, resolver, history heads, coverage |
| `surface_bindings_current` | `surface_binding_id` | exact lookup, explain | `SurfaceBound`, `SurfaceUnbound`, migration events |
| `address_names_current` | `(address, logical_name_id, relation)` | address collections (shipped) | authority, control, reverse, primary claim events |
| `children_current` | `(parent_logical_name_id, child_logical_name_id, surface_class)` | child collections | registration, subregistry, alias, wildcard events |
| `permissions_current` | `(resource_id, subject, scope)` | resource permissions reads (shipped) | permission and transfer events |
| `resolver_current` | `(chain_id, resolver_address)` | resolver overview (shipped) | resolver, alias, permission, inventory events |
| `record_inventory_current` | `(resource_id, version_boundary)` | declared resolution inventory + cache | record and version-boundary events |
| `primary_names_current` | `(address, coin_type, namespace)` | exact-tuple declared `claimed_primary_name` readback + exact-tuple declared normalized claim identity for `claimed_primary_name.name` + exact-tuple declared claim provenance + optional exact-tuple `invalid_name` `raw_claim_name` + invalidation context | reverse, primary claim, verified primary events |
| `coverage_current` | `logical_name_id` | exact-name inline coverage, dedicated single-name coverage/explain reads | `CoverageChanged`, capability changes |

History reads use normalized events plus thin cursor support rather than a separate denormalized history truth table. The shipped address-history view composes address anchor selection across current and historical matches with the same normalized-event history family rather than introducing a separate history projection or ledger.

## 3. Collection Semantics

### Exact name lookup

- keyed by `logical_name_id`
- authoritative for supported source classes
- exact-name lookup is snapshot-selected: current binding, registration, authority, control, resolver, record-inventory summary, history pointers, provenance, coverage, chain positions, and consistency must all describe the same selected `ChainPositions` object for `GET /v1/names/{namespace}/{name}` and the exact-name explain routes
- `name_current` rows are the API join root for exact-name reads; API handlers may join other projection families by the selected exact-name identity and positions, but must not rebuild exact-name state from normalized events or raw facts
- for `namespace=ens` on the selected ENSv2 `sepolia-dev` profile, `name_current` may publish supported declared exact-name profile rows only from the admitted `ens_v2_registry_l1` and `ens_v2_registrar_l1` source classes. The registry projection inputs are the upstream label state, token-resource link, label lifecycle, resolver-target, and token-regeneration surfaces; the registrar projection inputs are the upstream `.eth` registration and renewal lifecycle surfaces (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L21 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L41 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L47 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L63 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L21 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L45 @ ens_v2@554c309)
- that ENSv2 `sepolia-dev` exact-name projection support does not create rows for the mainnet profile, reverse or primary-name reads, wrapper-derived authority, migration history, universal-resolver entrypoints, verified resolution, execution explain, or resolver-local record/cache/overview facts that are outside the exact-name profile support class
- for `namespace=basenames`, `name_current` remains a Base-authority read family: exact-name declared truth comes from `basenames_base_registry`, `basenames_base_registrar`, and `basenames_base_resolver`; `basenames_base_primary` is claim-intake-only, and `basenames_l1_compat` plus `basenames_execution` do not become alternate exact-name truth because upstream keeps the registry / registrar / resolver stack on Base while reverse claims stay on the separate ReverseRegistrar surface (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
- returns the current binding plus fixed declared summary sections for registration, authority, control, resolver, record inventory, and history
- unsupported declared summary sections stay explicit in the read model; they are not omitted silently
- authority may fall back to binding identifiers when a richer authority summary is not yet projected
- `control` is the exact-name summary form of current resource-anchored control facts and, in the initial contract, carries only `registrant`, `registry_owner`, and `latest_event_kind`; it is narrower than both the internal `ControlVector` and `permissions_current`
- `resolver` is the exact-name summary form of the current resolver target and, in the initial contract, carries `chain_id`, `address`, and `latest_event_kind`; `chain_id=null` and `address=null` mean the current binding has no declared resolver rather than that the resolver summary itself is unsupported
- for ENSv1 and Basenames, the exact-name resolver summary identifies the declared target only; resolver-local record, cache, or resolver-overview facts may be consumed only after the resolver address is direct manifest-admitted or resolver discovery-admitted into the relevant resolver source family and admitted as a supported resolver profile for the relevant record family (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
- `history` is a pair of head pointers derived from canonical normalized events: `surface_head` and `resource_head`, each pointing at the first row the dedicated name-history route would return for the same target under `scope=surface` or `scope=resource`
- `history` summary stays in `name_current` only as these scope-specific head pointers; paginated history rows and `scope=both` union ordering remain on the dedicated history reads and do not create a separate history projection
- Phase 6 does not add an explain-only history route or projection family; shipped history routes remain the explainable declared answer, and `name_current.history` only links callers into those rows
- the shipped explain routes `GET /v1/explain/names/{namespace}/{name}/surface-binding` and `GET /v1/explain/names/{namespace}/{name}/authority-control` are thin reads over the same exact-name target, `surface_bindings_current`, `name_current`, and `permissions_current` truth families; they do not add explain-specific projection families or ledgers

### Coverage by exact name

- keyed by `logical_name_id`
- serves the shared `Coverage` object for both `GET /v1/names/{namespace}/{name}` inline coverage and `GET /v1/coverage/{namespace}/{name}`
- coverage reads use the same exact-name snapshot selector as exact-name lookup; for the same `{namespace, name}` and selected `ChainPositions`, `GET /v1/coverage/{namespace}/{name}` and the inline exact-name `coverage` field must read the same `coverage_current` answer
- for the promoted ENSv2 `sepolia-dev` exact-name profile, `coverage_current` stores the shared supported summary for that exact-name profile class: `status=full`, `exhaustiveness=authoritative`, `source_classes_considered=["ens_v2_registry_l1","ens_v2_registrar_l1"]`, `enumeration_basis=exact_name_profile`, and `unsupported_reason=null` (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)
- that coverage row is scoped to declared exact-name profile support only and does not store supported coverage for mainnet, reverse or primary-name reads, wrapper-derived authority, migration history, universal-resolver entrypoints, verified resolution, execution explain, or resolver-local sections outside the promoted exact-name profile class
- the dedicated coverage route adds declared explain detail for that same single-name answer; it does not introduce separate coverage enums or defaults
- `CoverageChanged` updates this shared single-name coverage state; capability changes may invalidate or recompute it, but do not create a second coverage truth system

### Address to names

- default unit is the surface, not the resource
- for `namespace=basenames`, `address_names_current` membership and relation facets derive from the same Base-side authority/control truth as exact-name lookup; reverse-claim intake and L1 compatibility transport do not create membership rows or widen relation facets because upstream separates Base-side name ownership / resolver state from reverse claims and the Ethereum Mainnet `L1Resolver` transport (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
- the initial declared-state relation vocabulary is `registrant`, `token_holder`, and `effective_controller`
- callers may request `dedupe_by=resource`
- default sort is `display_name_asc`
- `include=role_summary` is additive and adds only `role_summary`, `subname_count`, `record_count`, `status`, and `expiry`
- `include=role_summary` does not change supported filters, default grouping, default sort, cursor semantics, or route-level coverage meaning
- `role_summary` derives from the current item `resource_id` plus the existing resource-permissions truth family; it does not require a second address-role projection or ledger
- `role_summary` is the per-resource summary form of `permissions_current`: group current rows by `subject`, keep each grouped subject's `scope` plus `effective_powers`, and leave row-granular grant lineage on `permissions_current`
- `subname_count` reuses `children_current` under the declared direct-child surface rule; linked, alias-derived, and wildcard-observed child buckets stay separate
- `status` and `expiry` are resource-derived control fields for the current `resource_id`; they mirror the current `ControlVector` values rather than any address-list-local state
- `record_count` counts the distinct stable declared record selectors for the current `resource_id` at its current version boundary, using the same declared inventory semantics as `Resolution.record_inventory`; it must not be implemented as an address-list-only counter, a raw slot count, or a verified-query count

### Name to children

- default unit is declared direct child surfaces
- for `namespace=ens` on the ENSv2 `sepolia-dev` profile, declared direct child and linked-subregistry buckets are driven by normalized `SubregistryChanged` and `ParentChanged` graph events, not by token ID enumeration alone; upstream emits `SubregistryUpdated(tokenId, subregistry, sender)` and `ParentUpdated(parent, label, sender)` as registry events (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L131 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L151 @ ens_v2@554c309)
- for `namespace=basenames`, `children_current` remains a Base-authority family: declared direct child rows come from the admitted Base registry / registrar / resolver split, not from primary-claim intake or L1 compatibility transport because upstream places `*.base.eth` subdomain registration on the Base registry / registrar stack while reverse claims and the L1 resolver stay separate (upstream: .refs/basenames/README.md:L8 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
- linked, alias-derived, and wildcard-observed children are separate `surface_class` buckets
- default sort is `display_name_asc`

### History

- default sort is `chain_position_desc`
- `scope=surface|resource|both` maps onto normalized-event filters, not different truth systems
- name-history resource scope resolves across every resource ever bound to the requested surface
- resource-history surface scope resolves across every surface ever bound to the requested resource
- shipped `Address.history` resolves address-derived surface and resource anchor sets across current and historical matches first, then applies the same `scope=surface|resource|both` history contract over normalized events

### Resource permissions

- keyed by `(resource_id, subject, scope)`
- default unit is the effective permission row for one resource-anchored subject and scope
- resolver-scoped permissions remain rows in this family; resolver overview reads summarize them but do not replace them
- for ENSv2, `permissions_current` consumes normalized permission events derived from upstream `EACRolesChanged(resource, account, oldRoleBitmap, newRoleBitmap)` and must retain whether an effective power is resource-specific or root-derived because upstream root roles satisfy resource-level `hasRoles` checks through root fallback (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L176 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L181 @ ens_v2@554c309)
- ENSv2 registry permissions are keyed to the bigname `resource_id` linked to the upstream registry EAC resource; `TokenRegenerated` updates token attributes and invalidates token-facing summaries without moving permission rows to a successor resource (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309)
- ENSv2 resolver-scoped permissions are keyed by resolver contract instance plus the upstream resolver EAC resource for a whole name, text key, or coin type, as emitted by `NamedResource`, `NamedTextResource`, and `NamedAddrResource` before the corresponding EAC role change (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L132 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L137 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L149 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L239 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L257 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L282 @ ens_v2@554c309)

### Resolver overview

- keyed by `(chain_id, resolver_address)`
- serves declared summary sections for bindings, aliases, permissions, role holders, and event/count summaries
- supported `aliases` reuses the same supported `{status, count, items}` envelope as `bindings`, with `items` sourced only from current resolver-linked bindings whose `binding_kind=resolver_alias_path`
- resolver-overview alias support stays within `resolver_current`; it does not add an alias-only projection family, historical alias ledger, or second resolver-binding truth system
- for ENSv2, resolver alias mappings are sourced from normalized `AliasChanged` events emitted by admitted `PermissionedResolver` instances; the resolver rewrites by longest matching suffix, so `resolver_current.aliases.items` must preserve both the source and final target DNS-encoded names used to build the public path (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L56 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L230 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L650 @ ens_v2@554c309)
- for ENSv1 and Basenames, `resolver_current` may summarize a resolver only after that resolver address is direct manifest-admitted or resolver discovery-admitted into the relevant resolver source family and admitted as a supported resolver profile for the relevant overview section; a current topology edge observed from registry state alone does not create a supported resolver overview (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
- unsupported declared summary sections stay explicit until the corresponding overview detail is projected

### Resolution

- `GET /v1/resolutions/{namespace}/{name}` uses the same exact-name snapshot selector for its exact-name `data`, declared topology, route-level coverage, record-inventory/cache joins, and verified readback support checks
- persisted verified resolution output may join the public response only when its stored requested chain positions exactly match the selected exact-name `ChainPositions`; output for a different snapshot remains in execution storage but is not eligible for this response
- declared `topology` freezes the fixed subdocument `{registry_path, subregistry_path, resolver_path, wildcard, alias, version_boundaries, transport}`; it remains part of the resolution read contract rather than a second topology ledger
- for ENSv2, `topology.subregistry_path` and `topology.registry_path` consume normalized `SubregistryChanged` and `ParentChanged`, `topology.alias` consumes normalized `AliasChanged`, and `topology.wildcard` is populated only by observed extended-resolution evidence with a concrete source resolver and matched labels; manifest presence or resolver deployment alone must not synthesize wildcard coverage (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L412 @ ens_v2@554c309)
- for `namespace=basenames`, declared `topology` keeps Base-side authority on `registry_path` and `resolver_path` while publishing the separate compatibility hop in `transport`; the current Basenames `verified_resolution=supported` class is the exact-surface transport-assisted direct-path class where `resolver_path[0].logical_name_id` equals the route surface, `wildcard.source=null` with `matched_labels=[]`, `alias.final_target=null` with `hops=[]`, `subregistry_path=[]`, `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, and `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- for ENSv1 and Basenames, `record_inventory_current` and `record_cache` may consume retained resolver-local record events from the current resolver as event-evidenced selector/cache facts even while resolver-profile admission is still pending; unobserved selectors in that family remain explicit `resolver_family_pending` unsupported family coverage rather than being treated as absent or complete (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L61 @ basenames@1809bbc)
- for ENSv1 discovered resolver instances, the only complete Phase 4 supported dynamic profile is PublicResolver-compatible. Projection workers may promote complete family coverage, resolver-overview, resolver-authorization, and non-retained resolver-local facts only when the resolver-profile state for the resolver `contract_instance_id` is `supported` for the emitted fact family; `pending` or `unsupported` profile state keeps non-event-evidenced family coverage partial. PublicResolver compatibility is anchored to the upstream PublicResolver mixin surface, ERC165 support, and ResolverBase record-versioning; Basenames projections use the separate Base-side resolver-profile gate in the next bullet (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L23 @ ens_v1@91c966f)
- for Basenames discovered Base-side resolver instances, the only complete Phase 8 supported dynamic profile is `L2Resolver`-compatible. Projection workers may promote complete family coverage and resolver-authorization facts only when the resolver-profile state for the resolver `contract_instance_id` is `supported` for the emitted fact family; `pending` or `unsupported` profile state keeps non-event-evidenced family coverage partial. `L2Resolver` compatibility is anchored to the upstream `L2Resolver` resolver-profile mixins, extended-resolution and ERC165 support, and authorization model, and this rule does not reuse the ENSv1 PublicResolver-compatible gate or widen Basenames L1 transport / execution projections (upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)
- `record_inventory_current` is keyed by `(resource_id, version_boundary)` and serves both declared `record_inventory` and declared `record_cache`
- `record_inventory` and `record_cache` are two declared subdocuments over the same selector space and version boundary; they are not separate truth systems
- every version-boundary object exposed by declared resolution uses the fixed fields `{logical_name_id, resource_id, normalized_event_id, event_kind, chain_position}`
- `topology.version_boundaries.record_version_boundary`, `record_inventory.record_version_boundary`, and `record_cache.record_version_boundary` must stay identical for the same declared answer
- `record_inventory` carries the fixed fields `{record_version_boundary, enumeration_basis, selectors, explicit_gaps, unsupported_families, last_change}`
- `record_inventory.enumeration_basis` is the fixed object `{observed_selectors, capability_declared_families, globally_enumerable}`
- `record_inventory.selectors[*]` and `record_cache.entries[*]` share the selector identity tuple `{record_key, record_family, selector_key}`; callers round-trip `record_key` in `records`
- `selector_key` is `null` for scalar families and a string for parameterized families, so numeric selector domains such as coin types stay textual on the wire
- `record_inventory` carries selector space, explicit gaps, and unsupported families
- `record_cache` carries the fixed fields `{record_version_boundary, entries}` and each entry uses `{record_key, record_family, selector_key, status}` plus conditional `value` or `unsupported_reason` keyed by `status`
- `record_cache.entries[*]` use the `ResultStatus` subset `success|not_found|unsupported`; if narrowed by `records`, entry order follows request order, otherwise `record_key` ascending
- `record_cache` carries last-known values for cacheable selectors at that same boundary and may be narrowed to requested selectors without changing the projection family
- `verified_queries` remain execution output keyed by the explicit selector request; projection rows do not become a second verified-resolution ledger
- for that supported Basenames class, execution output may carry persisted CCIP-participating traces because the upstream `L1Resolver` initiates `OffchainLookup` for non-`base.eth` requests and completes them through `resolveWithProof`; those traces join the public verified surface only for the exact transport-assisted direct class, while other Basenames path classes remain execution-local `unsupported` until a later contract update broadens support (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)

### Primary names

- keyed by `(address, coin_type, namespace)`
- serves the exact-tuple declared claim anchor plus the invalidation context needed for current exact-tuple handling and any later additive claimed-local readback
- for ENS on Ethereum Mainnet, the current declared claim precedence is reverse-only through `ens_v1_reverse_l1`; missing or unsupported reverse claims do not trigger fallback to registry-, resolver-, or other claim-setting surfaces, and admitting those fallback sources remains deferred (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
- for Basenames on the shipped mainnet profile, `basenames_base_primary` is the declared primary-claim intake owner only; `primary_names_current(address, coin_type, namespace)` may carry claim-local lookup and invalidation inputs for that intake, but it does not become the declared truth family for exact-name, address-name, or children reads because upstream exposes reverse-name claims through the dedicated ReverseRegistrar rather than the Base authority stack (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
- the route-level `claimed_primary_name` and `verified_primary_name` objects share the API `ResultStatus` vocabulary, but they do not collapse declared claim state and verified execution state into one projection-owned field
- for Basenames as well as ENS, projection-owned claim state and execution-owned verification state stay distinct: Base authority projections do not synthesize public primary-name payloads from exact-name/address-name/children truth, and `primary_names_current` does not persist or backfill `verified_primary_name` because upstream keeps reverse-name writes on the Base ReverseRegistrar while verified resolution enters through the separate Ethereum Mainnet `L1Resolver` (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
- projection-owned `claimed_primary_name` is limited to the declared subset `success|not_found|unsupported|invalid_name`; richer claimed payload fields remain additive-only except exact-tuple declared `claimed_primary_name.name`, exact-tuple declared `claimed_primary_name.provenance`, and the exact-tuple `invalid_name` `raw_claim_name` allowance
- for ENS on Ethereum Mainnet in Phase 7, the shipped projection is the exact-tuple claim anchor plus declared claim-side inputs only: reverse tuple admission supplies lookup and invalidation state, and it does not join resolver-backed or execution-derived name identity into public `claimed_primary_name` fields (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
- `primary_names_current(address, coin_type, namespace)` is the frozen exact-tuple claim-side lookup / invalidation anchor only for this route family; it owns the declared claim-side inputs and invalidation context for the requested tuple, not verified result publication
- that identity stays tied to the exact requested tuple and may own `claim_status`, `raw_claim_name`, the declared normalized claim-identity source for `claimed_primary_name.name`, and the claim-local provenance inputs for `claimed_primary_name.provenance` plus request-matching invalidation hooks; it does not own fallback-source selection beyond the admitted reverse-only surface, execution `request_type`, execution `request key`, `execution_trace_id`, verified status, verified name identity, verification-local failure payloads, or the route-level join between claim-side and verification-side provenance (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
- primary-name route coverage is a route-local API contract, not a `coverage_current` exact-name row; `primary_names_current` may supply claim-side inputs used by the exact-tuple readback class, but it does not persist a coverage row or make coverage exhaustive
- the frozen first Basenames exact-tuple `verified_primary_name` support class stays execution-derived under `basenames_execution`: it uses the same route tuple, the exact request-key string `{namespace}:{normalized_address}:{coin_type}`, and execution identity `request_type=verified_primary_name`, keeps the matching `primary_names_current(address, coin_type, namespace)` row as the only claim-side lookup / invalidation anchor, does not require a second primary-name manifest capability flag, and still does not let projection rows own `execution_trace_id`, verified status, verified name identity, or verification-local provenance because upstream keeps reverse-name writes on the Base ReverseRegistrar while verified resolution enters through the separate Ethereum Mainnet `L1Resolver` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
- the exact-tuple persisted-readback class is the only primary-name coverage support class in this freeze: ENS uses `source_classes_considered=["ens_v1_reverse_l1","ens_execution"]` and Basenames uses `source_classes_considered=["basenames_base_primary","basenames_execution"]`; both publish route-level `status=partial`, `exhaustiveness=non_enumerable`, `enumeration_basis=primary_name_lookup`, and `unsupported_reason=null` only for the requested tuple (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc)
- out-of-class primary-name tuples, fallback claim sources, fresh verified-primary execution, and broader address or namespace coverage stay explicit `unsupported` with `source_classes_considered=[]` and do not derive support from tuple presence or from `coverage_current`
- the admitted public claimed-local fields beyond bare status are exact-tuple declared `claimed_primary_name.name`, exact-tuple declared `claimed_primary_name.provenance`, and `raw_claim_name`
- when the route publishes `claimed_primary_name.provenance`, it is exact-tuple declared-only provenance sourced from the requested `primary_names_current(address, coin_type, namespace)` row's claim-local provenance inputs; it must strip any `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material before publication and must omit `execution_trace_id`
- when the route publishes `raw_claim_name`, it is copied verbatim from `primary_names_current.raw_claim_name` for the same exact `(address, coin_type, namespace)` tuple and only when `claim_status=invalid_name`
- `claimed_primary_name.name`, when present, comes only from the exact requested `primary_names_current(address, coin_type, namespace)` row's declared normalized claim-identity source for that same tuple, aligned with the currently admitted reverse-only claim precedence (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
- it must not be synthesized or backfilled from manifest presence, resolver-backed identity, verified execution identity, tuple presence alone, a different tuple, or any fallback claim source
- `claimed_primary_name.name` remains distinct from execution-derived `verified_primary_name.name`; this clarification does not change when `verified_primary_name.name` appears, and it does not by itself widen the exact-tuple primary-name coverage contract above
- tuple presence is a lookup and invalidation hook only; it does not by itself widen claim precedence, admit fallback sources, change route-level coverage outside the exact-tuple persisted-readback support class, or imply richer tuple-present claimed payload support beyond exact-tuple declared `claimed_primary_name.name`, exact-tuple declared `claimed_primary_name.provenance`, and the exact-tuple `invalid_name` `raw_claim_name` allowance (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
- `raw_claim_name` is projection-owned claim state only; it exists to preserve the declared raw input when normalization fails and must not be copied into `verified_primary_name`
- projection rows do not own verified-only states or failure payloads: `mismatch`, `execution_failed`, and verification-local `failure_reason` stay execution-derived even when the tuple row exists
- claim-local provenance inputs remain projection-owned lookup / invalidation material; the public `claimed_primary_name.provenance` surface is the exact-tuple declared-only projection-backed slice of those inputs, stripped of `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material and with no `execution_trace_id`
- the now-frozen `verified_primary_name.provenance` invariant is additive to public publication rather than projection-owned state: when admitted on the exact-tuple persisted-readback class, it reuses `Provenance` as a verification-local section refinement for the same exact tuple over execution output only
- `verified_primary_name.provenance` may publish only `execution_trace_id` plus `manifest_versions` for that same persisted verification trace; `verified_primary_name.provenance.execution_trace_id` must equal top-level `provenance.execution_trace_id`
- `verified_primary_name.provenance` must not publish `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material, restate `claimed_primary_name.provenance`, introduce a second lookup / invalidation identity for the tuple, or publish other `Provenance` fields
- top-level `provenance` remains the only route-level join between declared claim inputs and the persisted verification trace; projection rows do not own that join, verified status, or verified payload publication
- `verified_primary_name` in `mode=verified|both`, including the frozen first Basenames exact-tuple persisted-readback support class and any later `verified_primary_name.provenance` publication, remains execution-derived even when verified-primary normalized events are also projected for lookup and invalidation support

## 4. Invalidation Rules

Projection invalidation happens on:

- canonicality change
- manifest version change that affects a consumed capability
- normalized event insertion for a relevant key
- execution invalidation signals where a projection stores declared cache summaries

Invalidation must be deterministic and key-scoped. No projection is refreshed by broad time-based polling alone.

## 5. Rebuild Strategy

Every projection supports:

- point rebuild by key
- range rebuild by chain position
- full rebuild from canonical events

Required worker modes:

- continuous apply
- backfill apply
- reorg repair
- one-shot rebuild

### Replay operational summary

`bigname-worker replay all-current-projections` is worker-owned operational tooling for projection rebuilds. Its `--json` summary is worker-owned operational output only. It does not expose a public `v1` API, publish a machine-readable API contract, graduate manifest capabilities, change route-level coverage, or claim consumer replacement.

When invoked without `--json`, the command keeps its existing non-JSON behavior. Human-readable progress, logging, and terminal formatting remain operational output with no stable machine-readable contract.

When invoked as `bigname-worker replay all-current-projections --json`, stdout is one JSON object with this stable shape:

```json
{
  "command": "all-current-projections",
  "projections": [
    {
      "projection": "address_names_current",
      "requested": 0,
      "upserted": 0,
      "deleted": 0
    }
  ],
  "totals": {
    "requested": 0,
    "upserted": 0,
    "deleted": 0
  }
}
```

Rules:

- `command` is always `all-current-projections`
- `projections` contains one object per current projection family in stable projection identifier order: `address_names_current`, `children_current`, `coverage_current`, `name_current`, `permissions_current`, `primary_names_current`, `record_inventory_current`, `resolver_current`, `surface_bindings_current`
- each projection object has exactly `projection`, `requested`, `upserted`, and `deleted`
- `requested`, `upserted`, and `deleted` are non-negative integer counts for that projection replay attempt
- `totals` has exactly `requested`, `upserted`, and `deleted`, and each value is the sum of the corresponding per-projection counts
- the JSON summary describes the completed worker command attempt only; it is not stored as projection truth and is not a replay checkpoint

## 6. Index Baseline

Start with indexes that match the public contract:

- `name_current(logical_name_id)`
- `address_names_current(address, namespace, canonical_display_name, logical_name_id)`
- `children_current(parent_logical_name_id, surface_class, canonical_display_name, child_logical_name_id)`
- `permissions_current(resource_id, subject, scope)`
- `resolver_current(chain_id, resolver_address)`
- `primary_names_current(address, coin_type, namespace)`

Add more only after measured query evidence.

## 7. Ownership Boundaries

- adapters emit normalized events and never write projection rows directly
- projection workers read normalized events and manifests
- API handlers read projections and execution output only
- execution workers may publish invalidation signals but do not mutate declared projections outside their owned cache summaries
