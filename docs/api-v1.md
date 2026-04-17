# API v1 Contract

Status: Phase 0 baseline

This document freezes the external `v1` read contract strongly enough for API, projection, and SDK work to proceed in parallel.

## 1. Conventions

- all routes live under `/v1`
- responses are JSON with `snake_case` keys
- timestamps are RFC 3339 UTC strings
- semantic identities are strings; opaque internal IDs are never inferred by clients
- `namespace` is always explicit for name-based reads
- names in path segments are normalized names, URL-encoded as plain text
- every externally visible answer includes provenance, coverage, chain position context, and consistency

### Common query parameters

- `at`: point-in-time selector, either an RFC 3339 timestamp or a chain-position token
- `consistency`: `head`, `safe`, or `finalized`
- `mode`: `declared`, `verified`, or `both`
- `include`: comma-separated expansions; default is route-specific
- `cursor`: opaque pagination cursor
- `page_size`: default `50`, max `200`

Defaults:

- `consistency=head`
- `mode=declared`

### Snapshot Selection Rules

| Inputs | Rule |
| --- | --- |
| `chain_positions` only | use the supplied positions exactly |
| `at` only | resolve per-chain positions at the requested `consistency` |
| neither | use the latest available positions at the requested `consistency` |
| both `at` and `chain_positions` | reject with `invalid_input` |

Validation rules:

- if `chain_positions` is supplied, every chain required by the route must be present
- if `chain_positions` is supplied, unsupported chain keys for that route are rejected
- if `consistency` is supplied with explicit `chain_positions`, the server validates that each supplied position satisfies that consistency floor or returns `conflict`

Cross-chain rules:

- ENS authoritative positions are selected on Ethereum L1
- Basenames authoritative positions are selected on Base
- when a route also needs an auxiliary chain, choose the auxiliary position at the same requested consistency with timestamp less than or equal to the authoritative-chain timestamp
- verified execution runs against the resolved positions only; it does not advance to a newer head mid-request

## 2. Shared Response Envelope

Single-resource reads return:

```json
{
  "data": {},
  "declared_state": {},
  "verified_state": null,
  "provenance": {},
  "coverage": {},
  "chain_positions": {},
  "consistency": "head",
  "last_updated": "2026-04-16T00:00:00Z"
}
```

Collection reads replace `data` with an array and add:

```json
{
  "page": {
    "cursor": null,
    "next_cursor": null,
    "page_size": 50,
    "sort": "display_name_asc"
  }
}
```

Rules:

- `declared_state` is present whenever the route has declared semantics
- `verified_state` is present only when `mode=verified|both` and the route supports verified execution
- `coverage` explains completeness and enumeration basis, not just freshness
- `chain_positions` may contain multiple chains for cross-chain answers

## 3. Shared Objects

### `NameRef`

- `logical_name_id`
- `namespace`
- `normalized_name`
- `canonical_display_name`
- `namehash`
- `resource_id`
- `binding_kind`

### `ResourceRef`

- `resource_id`
- `authority_epoch`
- `token_lineage_id`
- `current_resolver`

### `Coverage`

- `status`: `full`, `partial`, `observed_only`, `unsupported`, `stale`
- `exhaustiveness`: `authoritative`, `best_effort`, `observed_only`, `non_enumerable`, `not_applicable`
- `source_classes_considered`
- `enumeration_basis`
- `unsupported_reason`

### `Provenance`

- `normalized_event_ids`
- `raw_fact_refs`
- `manifest_versions`
- `execution_trace_id`
- `derivation_kind`

### `ChainPositions`

- `ethereum`
- `base`
- `execution_checkpoint`

Each position object contains:

- `chain_id`
- `block_number`
- `block_hash`
- `timestamp`

## 4. Initial Route Set

These routes define the baseline `v1` surface. Later additions must be additive within `v1`.

| Route | Purpose | First milestone |
| --- | --- | --- |
| `GET /v1/namespaces/{namespace}` | Namespace metadata and support status | B |
| `GET /v1/names/{namespace}/{name}` | Exact name lookup | B |
| `GET /v1/names/{namespace}/{name}/children` | Declared child collection by default | B |
| `GET /v1/addresses/{address}/names` | Address-to-surface collection | B |
| `GET /v1/resources/{resource_id}/permissions` | Resource-centric effective permissions | B |
| `GET /v1/resolvers/{chain_id}/{resolver_address}` | Resolver overview | B |
| `GET /v1/resolutions/{namespace}/{name}` | Resolution topology, inventory, and verified reads | B/C |
| `GET /v1/primary-names/{address}` | Claimed and verified primary-name answer | C |
| `GET /v1/history/names/{namespace}/{name}` | Surface or combined history | B |
| `GET /v1/history/resources/{resource_id}` | Resource history | B |
| `GET /v1/manifests/{namespace}` | Active manifest versions and capabilities | B |
| `GET /v1/coverage/{namespace}/{name}` | Coverage and explain-oriented coverage details | B |

## 5. Route-Level Semantics

### `GET /v1/namespaces/{namespace}`

Returns manifest-backed metadata for one public namespace.

`declared_state` includes:

- `active_manifest_count`
- `active_source_families`
- `chains`
- `normalizer_versions`

Rules:

- return `200` with empty lists and `active_manifest_count=0` when the namespace is public but has no active manifests yet
- return `404 not_found` when the namespace is not a supported public namespace
- use `GET /v1/manifests/{namespace}` for per-manifest capability flags and manifest-version detail

### `GET /v1/names/{namespace}/{name}`

Returns:

- normalized surface identity
- current binding to `resource_id`
- registration and authority summary
- control summary
- resolver summary
- record inventory summary
- history pointers

Optional includes:

- `resolution`
- `permissions`
- `history`
- `primary_name`

### `GET /v1/addresses/{address}/names`

Returns surfaces, not backing resources.

Supported filters:

- `namespace`
- `relation`
- `dedupe_by=surface|resource`
- `include=role_summary`

Each item includes:

- `logical_name_id`
- `resource_id`
- `binding_kind`
- `relation_facets`
- `expiry`
- `status`
- `summary_counts`

### `GET /v1/names/{namespace}/{name}/children`

Defaults to declared direct children only.

Optional query parameters:

- `surface_classes=declared,linked,alias,wildcard`
- `include=counts`

### `GET /v1/resolutions/{namespace}/{name}`

`declared_state` includes:

- `topology`
- `record_inventory`
- `record_cache`

`verified_state` includes:

- explicit record answers for requested record keys
- execution trace reference
- failure state when verification cannot succeed

### `GET /v1/primary-names/{address}`

Supports:

- `coin_type`
- `namespace`

Returns both:

- `claimed_primary_name`
- `verified_primary_name`

Verification statuses are:

- `verified`
- `claimed_only`
- `mismatch`
- `unnormalized`
- `not_found`
- `unsupported`

## 6. Sorting And Pagination Defaults

- address collections default to `display_name_asc`
- child collections default to `display_name_asc`
- history reads default to `chain_position_desc`
- ties break on `logical_name_id` for surfaces and `resource_id` for resource views

Cursor pagination must be stable under replay for the same requested chain positions.

## 7. Error Model

Every non-2xx response returns:

```json
{
  "error": {
    "code": "unsupported",
    "message": "verified mode is not yet available for this namespace",
    "details": {}
  }
}
```

`error.code` values:

- `invalid_input`
- `not_found`
- `unsupported`
- `stale`
- `verification_failed`
- `conflict`
- `internal_error`

## 8. Versioning Rules

- new optional fields are additive within `v1`
- new routes are additive within `v1`
- changing enum meaning, default sort, coverage semantics, or required fields requires `v2`
- if a capability is unsupported for a namespace or source class, return it explicitly in `coverage` or `error`, never through silent omission
