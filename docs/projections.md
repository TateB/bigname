# Projections

Status: Phase 0 baseline

This document freezes the read-model boundaries between normalized events, current-state projections, and API reads.

The exact-name explain routes for surface-binding and authority-control now ship in the API binary. The primary-name route family also ships there, but only as a bootstrap mixed route: tuple-backed declared `claimed_primary_name` readback is wired, the admitted public claimed-local surfaces are exact-tuple declared provenance plus exact-tuple `raw_claim_name` for `invalid_name`, and `claimed_primary_name.name`, deferred fallback policy, public publication of `verified_primary_name.provenance`, and graduated coverage remain additive. Its projection boundaries are nevertheless normative here so later support can land without changing the shared contract: `primary_names_current` stays claim-local, while the now-frozen `verified_primary_name.provenance` invariant stays verification-local under the same top-level `execution_trace_id` rather than becoming a projection-owned join. No separate history-explain route is queued: the shipped history routes remain the declared history answer, and exact-name `history` only stores head pointers into that contract.

## 1. Projection Rules

- projections are rebuildable from canonical facts and normalized events
- projections exist to serve stable reads, not to invent semantics
- every projection row carries provenance, manifest version, and chain position context
- only projection workers write projection tables

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
| `primary_names_current` | `(address, coin_type, namespace)` | bootstrap declared `claimed_primary_name` readback + exact-tuple declared claim provenance + optional exact-tuple `invalid_name` `raw_claim_name` + invalidation context | reverse, primary claim, verified primary events |
| `coverage_current` | `logical_name_id` | exact-name inline coverage, dedicated single-name coverage/explain reads | `CoverageChanged`, capability changes |

History reads use normalized events plus thin cursor support rather than a separate denormalized history truth table. The shipped address-history view composes address anchor selection across current and historical matches with the same normalized-event history family rather than introducing a separate history projection or ledger.

## 3. Collection Semantics

### Exact name lookup

- keyed by `logical_name_id`
- authoritative for supported source classes
- returns the current binding plus fixed declared summary sections for registration, authority, control, resolver, record inventory, and history
- unsupported declared summary sections stay explicit in the read model; they are not omitted silently
- authority may fall back to binding identifiers when a richer authority summary is not yet projected
- `control` is the exact-name summary form of current resource-anchored control facts and, in the initial contract, carries only `registrant`, `registry_owner`, and `latest_event_kind`; it is narrower than both the internal `ControlVector` and `permissions_current`
- `resolver` is the exact-name summary form of the current resolver target and, in the initial contract, carries `chain_id`, `address`, and `latest_event_kind`; `chain_id=null` and `address=null` mean the current binding has no declared resolver rather than that the resolver summary itself is unsupported
- `history` is a pair of head pointers derived from canonical normalized events: `surface_head` and `resource_head`, each pointing at the first row the dedicated name-history route would return for the same target under `scope=surface` or `scope=resource`
- `history` summary stays in `name_current` only as these scope-specific head pointers; paginated history rows and `scope=both` union ordering remain on the dedicated history reads and do not create a separate history projection
- Phase 6 does not add an explain-only history route or projection family; shipped history routes remain the explainable declared answer, and `name_current.history` only links callers into those rows
- the shipped explain routes `GET /v1/explain/names/{namespace}/{name}/surface-binding` and `GET /v1/explain/names/{namespace}/{name}/authority-control` are thin reads over the same exact-name target, `surface_bindings_current`, `name_current`, and `permissions_current` truth families; they do not add explain-specific projection families or ledgers

### Coverage by exact name

- keyed by `logical_name_id`
- serves the shared `Coverage` object for both `GET /v1/names/{namespace}/{name}` inline coverage and `GET /v1/coverage/{namespace}/{name}`
- the dedicated coverage route adds declared explain detail for that same single-name answer; it does not introduce separate coverage enums or defaults
- `CoverageChanged` updates this shared single-name coverage state; capability changes may invalidate or recompute it, but do not create a second coverage truth system

### Address to names

- default unit is the surface, not the resource
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

### Resolver overview

- keyed by `(chain_id, resolver_address)`
- serves declared summary sections for bindings, aliases, permissions, role holders, and event/count summaries
- supported `aliases` reuses the same supported `{status, count, items}` envelope as `bindings`, with `items` sourced only from current resolver-linked bindings whose `binding_kind=resolver_alias_path`
- resolver-overview alias support stays within `resolver_current`; it does not add an alias-only projection family, historical alias ledger, or second resolver-binding truth system
- unsupported declared summary sections stay explicit until the corresponding overview detail is projected

### Resolution

- declared `topology` freezes the fixed subdocument `{registry_path, subregistry_path, resolver_path, wildcard, alias, version_boundaries, transport}`; it remains part of the resolution read contract rather than a second topology ledger
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

### Primary names

- keyed by `(address, coin_type, namespace)`
- serves the exact-tuple declared claim anchor plus the invalidation context needed for current bootstrap handling and any later additive claimed-local readback
- for ENS on Ethereum Mainnet, the current declared claim precedence is reverse-only through `ens_v1_reverse_l1`; missing or unsupported reverse claims do not trigger fallback to registry-, resolver-, or other claim-setting surfaces, and admitting those fallback sources remains deferred
- the route-level `claimed_primary_name` and `verified_primary_name` objects share the API `ResultStatus` vocabulary, but they do not collapse declared claim state and verified execution state into one projection-owned field
- projection-owned `claimed_primary_name` is limited to the declared subset `success|not_found|unsupported|invalid_name`; richer claimed payload fields remain additive-only except exact-tuple declared `claimed_primary_name.provenance` and the exact-tuple `invalid_name` `raw_claim_name` allowance
- for ENS on Ethereum Mainnet in Phase 7, the shipped projection is the exact-tuple claim anchor plus declared claim-side inputs only: reverse tuple admission supplies lookup and invalidation state, and it does not join resolver-backed or execution-derived name identity into public `claimed_primary_name` fields
- `primary_names_current(address, coin_type, namespace)` is the frozen exact-tuple claim-side lookup / invalidation anchor only for this route family; it owns the declared claim-side inputs and invalidation context for the requested tuple, not verified result publication
- that identity stays tied to the exact requested tuple and may own `claim_status`, `raw_claim_name`, and the claim-local provenance inputs for `claimed_primary_name.provenance` plus request-matching invalidation hooks; it does not own fallback-source selection beyond the admitted reverse-only surface, execution `request_type`, execution `request key`, `execution_trace_id`, verified status, verified name identity, verification-local failure payloads, or the route-level join between claim-side and verification-side provenance
- the admitted public claimed-local fields beyond bare status are exact-tuple declared `claimed_primary_name.provenance` and `raw_claim_name`
- when the route publishes `claimed_primary_name.provenance`, it is exact-tuple declared-only provenance sourced from the requested `primary_names_current(address, coin_type, namespace)` row's claim-local provenance inputs; it must strip any `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material before publication and must omit `execution_trace_id`
- when the route publishes `raw_claim_name`, it is copied verbatim from `primary_names_current.raw_claim_name` for the same exact `(address, coin_type, namespace)` tuple and only when `claim_status=invalid_name`
- `claimed_primary_name.name` remains blocked until a later doc-first contract update freezes an honest declared normalized claim-identity source that does not collapse into resolver-backed or execution-derived truth
- tuple presence is a bootstrap lookup and invalidation hook only; it does not by itself widen claim precedence, admit fallback sources, graduate route-level coverage, or imply richer tuple-present claimed payload support beyond exact-tuple declared `claimed_primary_name.provenance` and the exact-tuple `invalid_name` `raw_claim_name` allowance
- `raw_claim_name` is projection-owned claim state only; it exists to preserve the declared raw input when normalization fails and must not be copied into `verified_primary_name`
- projection rows do not own verified-only states or failure payloads: `mismatch`, `execution_failed`, and verification-local `failure_reason` stay execution-derived even when the tuple row exists
- claim-local provenance inputs remain projection-owned lookup / invalidation material; the public `claimed_primary_name.provenance` surface is the exact-tuple declared-only projection-backed slice of those inputs, stripped of `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material and with no `execution_trace_id`
- the now-frozen `verified_primary_name.provenance` invariant is additive to public publication rather than projection-owned state: when admitted, it reuses `Provenance` as a verification-local section refinement for the same exact tuple over execution output only
- `verified_primary_name.provenance` may publish only `execution_trace_id` plus any narrower `normalized_event_ids`, `raw_fact_refs`, `manifest_versions`, or `derivation_kind` for that same persisted verification trace; `verified_primary_name.provenance.execution_trace_id` must equal top-level `provenance.execution_trace_id`
- `verified_primary_name.provenance` must not publish `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material, restate `claimed_primary_name.provenance`, or introduce a second lookup / invalidation identity for the tuple
- top-level `provenance` remains the only route-level join between declared claim inputs and the persisted verification trace; projection rows do not own that join, verified status, or verified payload publication
- `verified_primary_name` in `mode=verified|both`, including any later `verified_primary_name.provenance` publication, remains execution-derived even when verified-primary normalized events are also projected for lookup and invalidation support

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
