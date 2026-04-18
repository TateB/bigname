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

Routes declare which subset of these parameters they honor. Unlisted parameters are reserved for additive route support and do not widen the shipped declared-state contract.

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
- `verified_state` is always present in the response envelope; routes without verified semantics return `null`
- routes that support verified execution populate `verified_state` only when the request asks for verified output
- `coverage` explains completeness and enumeration basis, not just freshness
- `chain_positions` may contain multiple chains for cross-chain answers
- route-level `coverage` and subdocument support are separate: a read may be authoritative for exact lookup while one or more declared summary sections still return explicit unsupported objects

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

### `UnsupportedSummary`

- `status`: always `unsupported`
- `unsupported_reason`

Use this object when a declared-state subdocument is part of the route contract but is not yet projected. The field stays present; unsupported detail is never omitted silently.

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

The current API binary ships only the declared-state subset below. Queued routes remain part of the frozen `v1` contract so projection and SDK work can proceed without changing wire semantics later.

| Route | Purpose | Contract state |
| --- | --- | --- |
| `GET /v1/namespaces/{namespace}` | Namespace metadata and support status | shipped declared-state |
| `GET /v1/names/{namespace}/{name}` | Exact name lookup | shipped declared-state |
| `GET /v1/names/{namespace}/{name}/children` | Declared child collection by default | shipped declared-state |
| `GET /v1/history/names/{namespace}/{name}` | Surface or combined history | shipped declared-state |
| `GET /v1/history/resources/{resource_id}` | Resource history | shipped declared-state |
| `GET /v1/manifests/{namespace}` | Active manifest versions and capabilities | shipped declared-state |
| `GET /v1/addresses/{address}/names` | Address-to-surface collection | queued declared-state |
| `GET /v1/resources/{resource_id}/permissions` | Resource-centric effective permissions | queued declared-state |
| `GET /v1/resolvers/{chain_id}/{resolver_address}` | Resolver overview | queued declared-state |
| `GET /v1/resolutions/{namespace}/{name}` | Resolution topology, inventory, and verified reads | queued mixed declared+verified |
| `GET /v1/primary-names/{address}` | Claimed and verified primary-name answer | queued mixed declared+verified |
| `GET /v1/coverage/{namespace}/{name}` | Coverage and explain-oriented coverage details | queued declared-state |

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

- `data` surface identity: `logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `namehash`
- `data` binding identifiers: `resource_id`, `token_lineage_id`, `binding_kind`
- `declared_state.registration`
- `declared_state.authority`
- `declared_state.control`
- `declared_state.resolver`
- `declared_state.record_inventory`
- `declared_state.history`

Rules:

- the exact-name route is authoritative for supported source classes even when one or more declared summary sections are still unsupported
- every declared summary section above is always present as an object
- if a section is not yet projected, it returns `UnsupportedSummary`
- `declared_state.authority` may fall back to `{resource_id, token_lineage_id, binding_kind}` when a dedicated authority summary is not yet projected but the current binding is known
- the shipped exact-name route does not support `include` expansions; history, permissions, resolution, and primary-name reads stay on their dedicated routes
- `verified_state` is `null` for the shipped exact-name route

### `GET /v1/addresses/{address}/names`

This route is queued but frozen now to unblock address-read work.

Returns surfaces, not backing resources.

Supported filters in the first declared-state contract:

- `namespace`
- `relation=registrant|token_holder|effective_controller`
- `dedupe_by=surface|resource`

Each item includes:

- `logical_name_id`
- `namespace`
- `normalized_name`
- `canonical_display_name`
- `namehash`
- `resource_id`
- `binding_kind`
- `relation_facets`

Rules:

- `dedupe_by=surface` is the default truth model
- `dedupe_by=resource` changes grouping only; it does not change coverage semantics or turn the route into a resource collection
- a later `include=role_summary` expansion is additive and must not change enumeration basis, default sort, or item identity
- later additive fields such as expiry, status, or counts must not replace the required surface identity and relation facets

### `GET /v1/resources/{resource_id}/permissions`

This route is queued but frozen now to unblock resource-centric declared reads.

Returns current effective permission rows anchored to one `resource_id`.

Supported filters:

- `subject`
- `scope`

Each item includes:

- `resource_id`
- `subject`
- `scope`
- `effective_powers`
- `grant_source`
- `revocation_source`
- `inheritance_path`
- `transfer_behavior`

Rules:

- `resource_id` is the truth anchor; surface names or resolver addresses may appear only as explanatory context
- resolver-scoped permissions remain rows in this same collection with resolver-specific scope detail; they are not a separate truth system
- this route is declared-state only and `verified_state` remains `null`

### `GET /v1/names/{namespace}/{name}/children`

Defaults to declared direct children only.

Optional query parameters:

- `surface_classes=declared`
- `include=counts`

Rules:

- requesting `linked`, `alias`, or `wildcard` surface classes is reserved for additive expansion and currently returns `unsupported`

### `GET /v1/history/names/{namespace}/{name}`

Returns canonical normalized-event history for one logical name anchor.

Supported query parameters:

- `scope=surface|resource|both` with default `both`

Rules:

- `scope=surface` returns events anchored by the requested `logical_name_id`
- `scope=resource` returns events anchored by any `resource_id` ever bound to that surface
- `scope=both` returns the union of those anchor sets
- observed and orphaned events are excluded from the shipped history routes
- `declared_state` is `{}` for history routes; the normalized-event rows themselves are the declared answer

### `GET /v1/history/resources/{resource_id}`

Returns canonical normalized-event history for one resource anchor.

Supported query parameters:

- `scope=surface|resource|both` with default `both`

Rules:

- `resource_id` must be a UUID or the route returns `400 invalid_input`
- `scope=resource` returns events anchored by the requested `resource_id`
- `scope=surface` returns events anchored by any `logical_name_id` ever bound to that resource
- `scope=both` returns the union of those anchor sets
- observed and orphaned events are excluded from the shipped history routes
- no dedicated address-history route is frozen in the shipped subset; queued address activity views must reuse these same anchor and coverage semantics rather than invent a second history contract

### `GET /v1/resolvers/{chain_id}/{resolver_address}`

This route is queued but frozen now to unblock resolver-overview reads.

`data` identifies the resolver target. `declared_state` groups:

- current bindings
- alias mappings
- resolver-scoped permissions
- role-holder summary
- resolver event summary

Rules:

- resolver overview is declared-state only and `verified_state` remains `null`
- counts for nodes, aliases, and role holders live inside those declared summaries rather than as a separate truth system
- any declared summary that is not yet projected returns `UnsupportedSummary`

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
