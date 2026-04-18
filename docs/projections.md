# Projections

Status: Phase 0 baseline

This document freezes the read-model boundaries between normalized events, current-state projections, and API reads.

Some declared-state route families are still queued in the API binary. Their projection boundaries are nevertheless normative here so the read contract can freeze before those handlers ship.

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
| `address_names_current` | `(address, logical_name_id, relation)` | address collections (queued) | authority, control, reverse, primary claim events |
| `children_current` | `(parent_logical_name_id, child_logical_name_id, surface_class)` | child collections | registration, subregistry, alias, wildcard events |
| `permissions_current` | `(resource_id, subject, scope)` | resource permissions reads (queued) | permission and transfer events |
| `resolver_current` | `(chain_id, resolver_address)` | resolver overview (queued) | resolver, alias, permission, inventory events |
| `record_inventory_current` | `(resource_id, version_boundary)` | declared resolution inventory + cache | record and version-boundary events |
| `primary_names_current` | `(address, coin_type, namespace)` | declared primary claim + verification lookup key | reverse, primary claim, verified primary events |
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
- the queued explain routes `GET /v1/explain/names/{namespace}/{name}/surface-binding` and `GET /v1/explain/names/{namespace}/{name}/authority-control` are thin reads over the same exact-name target, `surface_bindings_current`, `name_current`, and `permissions_current` truth families; they do not add explain-specific projection families or ledgers

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
- queued `Address.history` resolves address-derived surface and resource anchor sets across current and historical matches first, then applies the same `scope=surface|resource|both` history contract over normalized events

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
- serves declared `claimed_primary_name` plus the invalidation and provenance hooks needed to locate request-matching verified execution output
- the route-level `claimed_primary_name` and `verified_primary_name` objects share the API `ResultStatus` vocabulary, but they do not collapse declared claim state and verified execution state into one projection-owned field
- `verified_primary_name` in `mode=verified|both` remains execution-derived even when verified-primary normalized events are also projected for lookup and invalidation support

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
