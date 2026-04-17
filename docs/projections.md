# Projections

Status: Phase 0 baseline

This document freezes the read-model boundaries between normalized events, current-state projections, and API reads.

## 1. Projection Rules

- projections are rebuildable from canonical facts and normalized events
- projections exist to serve stable reads, not to invent semantics
- every projection row carries provenance, manifest version, and chain position context
- only projection workers write projection tables

## 2. Projection Families

| Projection | Primary key | Primary read | Source events |
| --- | --- | --- | --- |
| `name_current` | `logical_name_id` | exact name lookup | identity, registration, control, resolver, coverage |
| `surface_bindings_current` | `surface_binding_id` | exact lookup, explain | `SurfaceBound`, `SurfaceUnbound`, migration events |
| `address_names_current` | `(address, logical_name_id, relation)` | address collections | authority, control, reverse, primary claim events |
| `children_current` | `(parent_logical_name_id, child_logical_name_id, surface_class)` | child collections | registration, subregistry, alias, wildcard events |
| `permissions_current` | `(resource_id, subject, scope)` | permissions reads | permission and transfer events |
| `resolver_current` | `(chain_id, resolver_address)` | resolver overview | resolver, alias, permission, inventory events |
| `record_inventory_current` | `(resource_id, version_boundary)` | declared resolution | record and version-boundary events |
| `primary_names_current` | `(address, coin_type, namespace)` | primary-name reads | reverse, primary claim, verified primary events |
| `coverage_current` | route-specific key | coverage reads | `CoverageChanged`, capability changes |

History reads use normalized events plus thin cursor support rather than a separate denormalized history truth table.

## 3. Collection Semantics

### Exact name lookup

- keyed by `logical_name_id`
- authoritative for supported source classes
- returns the current binding and current declared summary

### Address to names

- default unit is the surface, not the resource
- callers may request `dedupe_by=resource`
- default sort is `display_name_asc`

### Name to children

- default unit is declared direct child surfaces
- linked, alias-derived, and wildcard-observed children are separate `surface_class` buckets
- default sort is `display_name_asc`

### History

- default sort is `chain_position_desc`
- `scope=surface|resource|both` maps onto normalized-event filters, not different truth systems

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

