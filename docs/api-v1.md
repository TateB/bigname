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

Routes declare which subset of these parameters they honor. Section 6 freezes the exact shipped collection routes that honor `cursor` and `page_size`. Unlisted parameters are reserved for additive route support and do not widen the shipped declared-state contract.

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

- `declared_state` and `verified_state` are always present in the response envelope
- routes without declared or verified semantics return `null` for that top-level section
- routes that support both declared and verified semantics use `mode` to decide which sections are populated:
  `declared` populates `declared_state` and returns `verified_state=null`
  `verified` populates `verified_state` and returns `declared_state=null`
  `both` populates both sections
- `coverage` explains completeness and enumeration basis, not just freshness
- `chain_positions` may contain multiple chains for cross-chain answers
- route-level `coverage` and subdocument support are separate: a read may be authoritative for exact lookup while one or more declared summary sections still return explicit unsupported objects
- top-level `provenance` is a route-level summary; mixed declared+verified routes may add section-local `provenance` objects where declared and execution derivations differ

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

### `RoleSummary`

- `subjects`
- `subjects[*].subject`
- `subjects[*].scopes`
- `subjects[*].scopes[*].scope`
- `subjects[*].scopes[*].effective_powers`

Use this object for the `role_summary` expansion on `GET /v1/addresses/{address}/names`. It is the per-resource summary view of the current effective permission rows for the same `resource_id`. Row-granular permission lineage such as `grant_source`, `revocation_source`, `inheritance_path`, and `transfer_behavior` stays on `GET /v1/resources/{resource_id}/permissions`.

### `ExactNameControlSummary`

- `registrant`
- `registry_owner`
- `latest_event_kind`

Use this object for `declared_state.control` on `GET /v1/names/{namespace}/{name}`. It is the exact-name summary form of the current resource-anchored control facts for the route's current `resource_id`, not a second permissions ledger and not a full `ControlVector` dump. When this summary is supported, these keys stay present; values may be `null` when the current authority epoch does not expose that subject or there is no retained control-change pointer yet.

### `ExactNameResolverSummary`

- `chain_id`
- `address`
- `latest_event_kind`

Use this object for `declared_state.resolver` on `GET /v1/names/{namespace}/{name}`. It identifies the current resolver target for the bound resource only; it does not inline `Resolution.topology`, wildcard or alias traversal detail, or resolver-overview subdocuments. When this summary is supported, `chain_id` and `address` are both `null` if the current resource has no declared resolver at the requested snapshot, and `latest_event_kind` may be `null` when the summary has no retained resolver-change pointer.

### `HistoryPointer`

- `normalized_event_id`
- `event_kind`
- `chain_position`

Use this object for summary links into the dedicated history routes. `chain_position` reuses the per-chain position object shape from `ChainPositions` and points at the same canonical normalized-event row that would appear in the dedicated history route under the matching scope and default sort.

### `ExactNameHistorySummary`

- `surface_head`
- `resource_head`

Use this object for `declared_state.history` on `GET /v1/names/{namespace}/{name}`. `surface_head` is the first canonical row that `GET /v1/history/names/{namespace}/{name}?scope=surface` would return under the shared default sort, and `resource_head` is the same pointer for `scope=resource`. Either field may be `null` when that anchor set has no canonical rows. This summary intentionally does not add a `both_head` field; callers that need union ordering or pagination use the dedicated history route with its existing `scope=both` default.

### `SurfaceBindingExplainSummary`

- `surface_binding_id`
- `binding_kind`

Use this object for `declared_state.surface_binding` on `GET /v1/explain/names/{namespace}/{name}/surface-binding`. It identifies the current `SurfaceBinding` row for the same exact-name answer returned by `GET /v1/names/{namespace}/{name}` at the requested snapshot. `binding_kind` repeats the current binding classification intentionally so this thin explain view can stand alone, while `resource_id` and `token_lineage_id` remain on top-level `data`. This route does not expose historical binding rows or a second binding-history ledger.

### `ResolutionResolverHop`

- `logical_name_id`
- `namespace`
- `normalized_name`
- `canonical_display_name`
- `resource_id`
- `chain_id`
- `address`
- `latest_event_kind`

Use this object for `declared_state.topology.resolver_path[*]` on `GET /v1/resolutions/{namespace}/{name}`. The array is ordered from the surface or ancestor that contributed resolver selection to the final declared resolver target. When `topology` is supported, `resolver_path` is never empty. The last hop is the resolver selected for the requested snapshot. `chain_id` and `address` are both `null` only when the path terminates in “no declared resolver”, and `latest_event_kind` may be `null` when the path has no retained resolver-change pointer.

### `VersionBoundary`

- `logical_name_id`
- `resource_id`
- `normalized_event_id`
- `event_kind`
- `chain_position`

Use this object for `declared_state.topology.version_boundaries.topology_version_boundary`, `declared_state.topology.version_boundaries.record_version_boundary`, `declared_state.record_inventory.record_version_boundary`, and `declared_state.record_cache.record_version_boundary`. `logical_name_id` and `resource_id` identify the surface and resource that last changed the relevant boundary and may differ from the route `data` when alias or wildcard traversal selects an ancestor. `normalized_event_id` and `event_kind` may be `null` only when the retained boundary is pinned by `chain_position` but there is no retained canonical boundary-event pointer.

### `ResolutionTopology`

- `registry_path`
- `subregistry_path`
- `resolver_path`
- `wildcard`
- `alias`
- `version_boundaries`
- `transport`

Use this object for `declared_state.topology` on `GET /v1/resolutions/{namespace}/{name}`.

Rules:

- `registry_path` is an array of `NameRef`, ordered from the requested surface toward the declared registry authority, and is never empty when `topology` is supported
- `subregistry_path` is an array of `NameRef`, ordered from the requested surface toward the nearest declared subregistry ancestor, and is empty when no subregistry delegation participates
- `resolver_path` is an array of `ResolutionResolverHop`
- `wildcard` is an object with `source` and `matched_labels`; `source` is `NameRef | null` and `matched_labels` is an array of label strings
- `alias` is an object with `final_target` and `hops`; `final_target` is `NameRef | null` and `hops` is an ordered array of `NameRef` alias targets after the requested surface
- `version_boundaries` is an object with `topology_version_boundary` and `record_version_boundary`, both using `VersionBoundary`
- `transport` is an object with `source_chain_id`, `target_chain_id`, `contract_address`, and `latest_event_kind`
- `wildcard.source=null` with `matched_labels=[]` means wildcard traversal did not participate
- `alias.final_target=null` with `hops=[]` means alias rewriting did not participate
- all `transport` fields are `null` when no compatibility transport participates
- `version_boundaries.record_version_boundary` must equal `record_inventory.record_version_boundary` and `record_cache.record_version_boundary` when those sections are supported in the same response

### `ResolutionRecordSelector`

- `record_key`
- `record_family`
- `selector_key`
- `cacheable`

Use this object for `declared_state.record_inventory.selectors[*]`. `record_key` is the stable round-trip selector token used in the `records` query parameter. `selector_key` is `null` for scalar families and a string for parameterized families. When `selector_key` is not `null`, `record_key` is `record_family + ":" + selector_key`; callers should round-trip the surfaced `record_key` instead of rebuilding it. Numeric selector domains such as coin types remain strings inside `selector_key` so `record_key` stays stable text.

### `ResolutionRecordGap`

- `record_key`
- `record_family`
- `selector_key`
- `gap_reason`

Use this object for `declared_state.record_inventory.explicit_gaps[*]`. `selector_key=null` means the explicit gap applies to the scalar family key itself rather than a parameterized member.

### `ResolutionUnsupportedRecordFamily`

- `record_family`
- `unsupported_reason`

Use this object for `declared_state.record_inventory.unsupported_families[*]`.

### `ResolutionRecordInventory`

- `record_version_boundary`
- `enumeration_basis`
- `selectors`
- `explicit_gaps`
- `unsupported_families`
- `last_change`

Use this object for `declared_state.record_inventory` on `GET /v1/resolutions/{namespace}/{name}` and `GET /v1/names/{namespace}/{name}`.

Rules:

- `record_version_boundary` uses `VersionBoundary`
- `enumeration_basis` is an object with `observed_selectors`, `capability_declared_families`, and `globally_enumerable`
- `selectors` is an array of `ResolutionRecordSelector`
- `explicit_gaps` is an array of `ResolutionRecordGap`
- `unsupported_families` is an array of `ResolutionUnsupportedRecordFamily`
- `last_change` is `HistoryPointer | null`
- `selectors` and `explicit_gaps` are sorted by `record_key` ascending
- `unsupported_families` is sorted by `record_family` ascending
- this object may be authoritative for exact lookup while `enumeration_basis.globally_enumerable` remains `false`

### `ResolutionRecordCacheEntry`

- `record_key`
- `record_family`
- `selector_key`
- `status`
- `value`
- `unsupported_reason`

Use this object for `declared_state.record_cache.entries[*]`.

Rules:

- `status` uses the shared `ResultStatus` vocabulary, but declared cache entries use only `success`, `not_found`, and `unsupported`
- `selector_key` follows the same scalar-vs-parameterized rule as `ResolutionRecordSelector`
- `value` appears only when `status=success` and uses the family-native JSON shape for that selector
- `unsupported_reason` appears only when `status=unsupported` and is required then

### `ResolutionRecordCache`

- `record_version_boundary`
- `entries`

Use this object for `declared_state.record_cache` on `GET /v1/resolutions/{namespace}/{name}`.

Rules:

- `record_version_boundary` uses `VersionBoundary`
- `entries` is an array of `ResolutionRecordCacheEntry`
- if `records` is omitted, `entries` contains every cacheable selector visible at the current `record_version_boundary` and is sorted by `record_key` ascending
- if `records` is supplied, `entries` contains exactly one item per requested `record_key` and follows request order
- `record_version_boundary` must equal `record_inventory.record_version_boundary` when both declared sections are supported in the same response

### `ExecutionStepSummary`

- `step_index`
- `step_kind`
- `input_digest`
- `output_digest`
- `latency`
- `canonicality_dependency`

Use this object for `verified_state.execution.steps[*]` on `GET /v1/explain/resolutions/{namespace}/{name}/execution`. It mirrors the persisted execution step list for one verified resolution answer without exposing raw calldata, raw return bodies, or a second trace family.

### `ResolutionExecutionExplainSummary`

- `execution_trace_id`
- `selected_entrypoint`
- `resolver_discovery_path`
- `wildcard`
- `alias`
- `steps`
- `finished_at`

Use this object for `verified_state.execution` on `GET /v1/explain/resolutions/{namespace}/{name}/execution`.

Rules:

- `execution_trace_id` equals top-level `provenance.execution_trace_id`
- `resolver_discovery_path` is an ordered array of `ResolutionResolverHop`
- `wildcard` reuses the same object shape as `ResolutionTopology.wildcard`
- `alias` reuses the same object shape as `ResolutionTopology.alias`
- `steps` is an ordered array of `ExecutionStepSummary`
- `steps` summarize only the persisted trace; they do not expose raw calldata, raw gateway payloads, or unpersisted retry state
- CCIP-Read participation is expressed through persisted `steps[*].step_kind` rather than a second gateway transcript

### `UnsupportedSummary`

- `status`: always `unsupported`
- `unsupported_reason`

Use this object when a declared-state subdocument is part of the route contract but is not yet projected. The field stays present; unsupported detail is never omitted silently.

### `ResolverOverviewBindingItem`

- `logical_name_id`
- `canonical_display_name`
- `normalized_name`
- `namehash`
- `resource_id`
- `surface_binding_id`
- `binding_kind`

Use this object for `declared_state.bindings.items[*]` and `declared_state.aliases.items[*]` on `GET /v1/resolvers/{chain_id}/{resolver_address}`. Items are ordered by `canonical_display_name`, then `logical_name_id`, then `surface_binding_id`.

### `ResolverOverviewBindingSummary`

- `status`: always `supported`
- `count`
- `items`

Use this object for supported `declared_state.bindings` and `declared_state.aliases` on `GET /v1/resolvers/{chain_id}/{resolver_address}`. `count` equals `items.length`. `status=supported` with `count=0` and `items=[]` is valid. `declared_state.aliases` reuses this exact shape but narrows `items` to the `binding_kind=resolver_alias_path` subset of the same current resolver-linked binding rows.

### `ResultStatus`

- `success`
- `not_found`
- `mismatch`
- `unsupported`
- `invalid_name`
- `execution_failed`

Use this status vocabulary for:

- `declared_state.record_cache.entries[*]`
- `verified_state.verified_queries[*]`
- `declared_state.claimed_primary_name`
- `verified_state.verified_primary_name`

Rules:

- every result object above always includes `status`
- route-specific request identity fields stay present even when `status` is not `success`
- `unsupported_reason` is required when `status=unsupported`
- `failure_reason` may appear for `not_found`, `mismatch`, `invalid_name`, or `execution_failed`
- value and identity fields appear only when the route established a concrete record value or concrete name target for that result
- not every status applies to every result object; routes document the subset they use

### `Coverage`

- `status`: `full`, `partial`, `observed_only`, `unsupported`, `stale`
- `exhaustiveness`: `authoritative`, `best_effort`, `observed_only`, `non_enumerable`, `not_applicable`
- `source_classes_considered`
- `enumeration_basis`
- `unsupported_reason`

This shared object is the route-level `coverage` summary on every response. For the same exact-name target and snapshot, `GET /v1/names/{namespace}/{name}` and `GET /v1/coverage/{namespace}/{name}` return the same top-level `Coverage` object.

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

The current API binary ships the routes marked `shipped` below. Queued routes remain part of the frozen `v1` contract so projection and SDK work can proceed without changing wire semantics later.

| Route | Purpose | Contract state |
| --- | --- | --- |
| `GET /v1/namespaces/{namespace}` | Namespace metadata and support status | shipped declared-state |
| `GET /v1/names/{namespace}/{name}` | Exact name lookup | shipped declared-state |
| `GET /v1/explain/names/{namespace}/{name}/surface-binding` | Current surface-binding explain view for one exact name | shipped declared-state |
| `GET /v1/explain/names/{namespace}/{name}/authority-control` | Current authority/control explain view for one exact name | shipped declared-state |
| `GET /v1/names/{namespace}/{name}/children` | Declared child collection by default | shipped declared-state |
| `GET /v1/history/names/{namespace}/{name}` | Surface or combined history | shipped declared-state |
| `GET /v1/history/resources/{resource_id}` | Resource history | shipped declared-state |
| `GET /v1/history/addresses/{address}` | Address activity across related surfaces and resources | shipped declared-state |
| `GET /v1/manifests/{namespace}` | Active manifest versions and capabilities | shipped declared-state |
| `GET /v1/addresses/{address}/names` | Address-to-surface collection | shipped declared-state |
| `GET /v1/resources/{resource_id}/permissions` | Resource-centric effective permissions | shipped declared-state |
| `GET /v1/resolvers/{chain_id}/{resolver_address}` | Resolver overview | shipped declared-state |
| `GET /v1/resolutions/{namespace}/{name}` | Resolution topology, inventory, and verified reads | shipped mixed declared+verified |
| `GET /v1/explain/resolutions/{namespace}/{name}/execution` | Persisted verified execution explain for one exact-name resolution request | shipped verified-state explain |
| `GET /v1/primary-names/{address}` | Bootstrap claimed and verified primary-name answer | shipped bootstrap mixed declared+verified |
| `GET /v1/coverage/{namespace}/{name}` | Single-name coverage and explain details | shipped declared-state |

### Machine-Readable Contract Publication

Phase 6 freezes `docs/api-v1.openapi.json` as the publication location for future machine-readable contract output.

When generated, that artifact covers only the `v1` routes currently shipped by `apps/api/src/main.rs`.

Queued routes stay prose-frozen in this document until their handlers ship.

`GET /v1/explain/resolutions/{namespace}/{name}/execution` is now shipped and published in `docs/api-v1.openapi.json`. Its generated contract matches the current handler surface: path parameters `{namespace}` and `{name}` plus required `records`.

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
- `declared_state.control`: `ExactNameControlSummary | UnsupportedSummary`
- `declared_state.resolver`: `ExactNameResolverSummary | UnsupportedSummary`
- `declared_state.record_inventory`: `ResolutionRecordInventory | UnsupportedSummary`
- `declared_state.history`: `ExactNameHistorySummary | UnsupportedSummary`

Rules:

- the exact-name route is authoritative for supported source classes even when one or more declared summary sections are still unsupported
- every declared summary section above is always present as an object
- if a section is not yet projected, it returns `UnsupportedSummary`
- `declared_state.authority` may fall back to `{resource_id, token_lineage_id, binding_kind}` when a dedicated authority summary is not yet projected but the current binding is known
- `declared_state.control` is the narrow current-`resource_id` control summary only; it does not inline full resource permissions, role-holder detail, or the entire internal `ControlVector`
- supported `declared_state.resolver` uses `chain_id` plus `address` as the same resolver target key used by `GET /v1/resolvers/{chain_id}/{resolver_address}` when a resolver exists; `chain_id=null` and `address=null` mean no declared current resolver rather than unsupported projection
- supported `declared_state.record_inventory` reuses the same `ResolutionRecordInventory` object shape as `GET /v1/resolutions/{namespace}/{name}` and, for the same snapshot, must expose the same `record_version_boundary`
- supported `declared_state.history.surface_head` and `declared_state.history.resource_head` point at the first canonical rows of the dedicated name-history route under `scope=surface` and `scope=resource`; the exact-name route does not add `both_head`, pagination state, or a second history truth system
- for the same `{namespace}`, `{name}`, and snapshot selection, the top-level `coverage` object matches `GET /v1/coverage/{namespace}/{name}`
- the only exact-name explain routes in Phase 6 are `GET /v1/explain/names/{namespace}/{name}/surface-binding` and `GET /v1/explain/names/{namespace}/{name}/authority-control`; they are thin views over this same exact-name target, current binding, and declared summary contract, while history explanation stays on the shipped `GET /v1/history/...` routes plus `declared_state.history.{surface_head,resource_head}` and does not introduce a separate exact-name history-explain endpoint or truth system
- the shipped exact-name route does not support `include` expansions; history, permissions, resolution, and primary-name reads stay on their dedicated routes
- `verified_state` is `null` for the shipped exact-name route

### `GET /v1/coverage/{namespace}/{name}`

This route ships as the single-name coverage and explain surface.

Returns the declared-state coverage answer for one exact public surface.

`data` identifies the same single surface and current binding as `GET /v1/names/{namespace}/{name}`.

`declared_state` carries explain-oriented detail for that same single-name coverage answer.

Supported query parameters:

- `at`
- `consistency`

Rules:

- this route honors only `at` and `consistency` from the common query set; if `at` is omitted, the common snapshot defaults apply and the route reads the latest available positions at `consistency=head` unless the caller supplies another supported `consistency`
- this route is declared-state only and `verified_state` is `null`
- the top-level `coverage` field is the shared `Coverage` object for the requested name and snapshot
- for the same `{namespace}`, `{name}`, and snapshot selection, that top-level `coverage` object must match the inline `coverage` returned by `GET /v1/names/{namespace}/{name}`
- `declared_state` explains `coverage.status`, `coverage.exhaustiveness`, `coverage.source_classes_considered`, `coverage.enumeration_basis`, and `coverage.unsupported_reason`; it does not redefine them
- the initial contract defines no `include` expansions for this route

### `GET /v1/explain/names/{namespace}/{name}/surface-binding`

This route ships as the exact-name surface-binding explain read.

Returns the declared-state binding explanation for one exact public surface.

`data` identifies the same single surface and current binding as `GET /v1/names/{namespace}/{name}`.

`declared_state` includes:

- `surface_binding`: `SurfaceBindingExplainSummary`
- `history`: `ExactNameHistorySummary | UnsupportedSummary`

Supported query parameters:

- `at`
- `consistency`

Rules:

- this route is scoped to the same exact-name target and point-in-time snapshot rules as `GET /v1/names/{namespace}/{name}`
- this route is declared-state only and `verified_state` is `null`
- `declared_state.surface_binding.surface_binding_id` identifies the current `SurfaceBinding` row whose `binding_kind` matches the exact-name answer's current binding; this route does not return historical binding rows or pagination state
- `declared_state.history` reuses the exact-name history head-pointer contract and does not create a binding-only history ledger
- for the same `{namespace}`, `{name}`, and snapshot selection, the top-level `coverage` object matches `GET /v1/names/{namespace}/{name}`
- this route reuses `surface_bindings_current` together with the existing exact-name and normalized-event history truth families; it does not introduce a second explain ledger
- the initial contract defines no `include` expansions for this route

### `GET /v1/explain/names/{namespace}/{name}/authority-control`

This route ships as the exact-name authority/control explain read.

Returns the declared-state authority and control explanation for one exact public surface.

`data` identifies the same single surface and current binding as `GET /v1/names/{namespace}/{name}`.

`declared_state` includes:

- `authority`
- `control`: `ExactNameControlSummary | UnsupportedSummary`

Supported query parameters:

- `at`
- `consistency`

Rules:

- this route is scoped to the same exact-name target and point-in-time snapshot rules as `GET /v1/names/{namespace}/{name}`
- this route is declared-state only and `verified_state` is `null`
- `declared_state.authority` uses the same object shape and fallback rule as the exact-name route; it does not widen authority semantics for the explain view
- `declared_state.control` uses the same exact-name summary object as `GET /v1/names/{namespace}/{name}` and remains narrower than both the internal `ControlVector` and the dedicated resource-permissions collection
- row-granular permission lineage stays on `GET /v1/resources/{resource_id}/permissions`
- for the same `{namespace}`, `{name}`, and snapshot selection, the top-level `coverage` object matches `GET /v1/names/{namespace}/{name}`
- this route reuses `name_current` plus the existing resource-anchored permissions truth family; it does not introduce a second authority or control ledger
- the initial contract defines no `include` expansions for this route

### `GET /v1/addresses/{address}/names`

This route ships as the address-read collection.

Returns surfaces, not backing resources.

Supported query parameters in the first declared-state contract:

- `namespace`
- `relation=registrant|token_holder|effective_controller`
- `dedupe_by=surface|resource`
- `include=role_summary`
- `cursor`
- `page_size`

Each item includes:

- `logical_name_id`
- `namespace`
- `normalized_name`
- `canonical_display_name`
- `namehash`
- `resource_id`
- `binding_kind`
- `relation_facets`

When `include=role_summary` is requested, each item also adds:

- `role_summary`: `RoleSummary`
- `subname_count`
- `record_count`
- `status`
- `expiry`

Rules:

- `dedupe_by=surface` is the default truth model
- `dedupe_by=resource` changes grouping only; it does not change coverage semantics or turn the route into a resource collection
- the default sort remains `display_name_asc`
- `cursor` and `page_size` page over the frozen default sort only; they do not alter item shape, grouping semantics, supported filters, or coverage meaning
- `include=role_summary` is additive; it does not change supported filters, default `dedupe_by`, enumeration basis, route-level coverage meaning, default sort, cursor behavior, or item identity
- the `role_summary` expansion derives from the current item `resource_id` plus the existing resource-permissions truth family; it does not introduce a separate address-role ledger
- `role_summary` groups the current `GET /v1/resources/{resource_id}/permissions` rows by `subject`; each grouped subject keeps the current `(scope, effective_powers)` pairs for that `resource_id`, while row-granular grant and revocation detail stays on the dedicated permissions route
- `subname_count` counts the same declared direct child surfaces returned by `GET /v1/names/{namespace}/{name}/children` by default; it does not include linked, alias-derived, or wildcard-observed child buckets
- `status` and `expiry` mirror the current `ControlVector.status` and `ControlVector.expiry` values for the item `resource_id`
- `record_count` counts the distinct stable declared record selectors for the item `resource_id` at its current version boundary; in the first shipped slice this is the number of selectors that belong to the same declared record-inventory answer shape used by `Resolution.record_inventory`, not a count of raw resolver slots, cached values, or verified query results
- the added fields `role_summary`, `subname_count`, `record_count`, `status`, and `expiry` are optional expansion fields only and do not replace the required surface identity and relation facets

### `GET /v1/resources/{resource_id}/permissions`

This route ships as the resource-centric declared permissions read.

Returns current effective permission rows anchored to one `resource_id`.

Supported query parameters:

- `subject`
- `scope`
- `cursor`
- `page_size`

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
- `GET /v1/addresses/{address}/names?include=role_summary` is the per-resource summary form of this same collection: it groups current rows by `subject`, retains each grouped subject's `scope` plus `effective_powers`, and leaves row-granular lineage on this dedicated route
- `cursor` and `page_size` page over the frozen `subject_scope_asc` order only; they do not alter row shape, supported filters, or route-level coverage meaning
- this route is declared-state only and `verified_state` remains `null`

### `GET /v1/names/{namespace}/{name}/children`

Defaults to declared direct children only.

Optional query parameters:

- `surface_classes=declared`
- `include=counts`
- `cursor`
- `page_size`

Rules:

- requesting `linked`, `alias`, or `wildcard` surface classes is reserved for additive expansion and currently returns `unsupported`
- `cursor` and `page_size` page over the frozen `display_name_asc` order only; they do not alter supported `surface_classes`, row shape, or coverage meaning

### `GET /v1/history/names/{namespace}/{name}`

Returns canonical normalized-event history for one logical name anchor.

Supported query parameters:

- `scope=surface|resource|both` with default `both`
- `cursor`
- `page_size`

Rules:

- `scope=surface` returns events anchored by the requested `logical_name_id`
- `scope=resource` returns events anchored by any `resource_id` ever bound to that surface
- `scope=both` returns the union of those anchor sets
- observed and orphaned events are excluded from the shipped history routes
- `cursor` and `page_size` page over the frozen `chain_position_desc` order only; they do not alter row shape, scope semantics, or coverage meaning
- `declared_state` is `{}` for history routes; the normalized-event rows themselves are the declared answer

### `GET /v1/history/resources/{resource_id}`

Returns canonical normalized-event history for one resource anchor.

Supported query parameters:

- `scope=surface|resource|both` with default `both`
- `cursor`
- `page_size`

Rules:

- `resource_id` must be a UUID or the route returns `400 invalid_input`
- `scope=resource` returns events anchored by the requested `resource_id`
- `scope=surface` returns events anchored by any `logical_name_id` ever bound to that resource
- `scope=both` returns the union of those anchor sets
- observed and orphaned events are excluded from the shipped history routes
- `cursor` and `page_size` page over the frozen `chain_position_desc` order only; they do not alter row shape, scope semantics, or coverage meaning
- `GET /v1/history/addresses/{address}` reuses these same anchor and coverage semantics rather than inventing a second history contract

### `GET /v1/history/addresses/{address}`

This route ships as the address activity history read.

Returns canonical normalized-event history for one address-derived anchor set.

Supported query parameters:

- `namespace`
- `relation=registrant|token_holder|effective_controller`
- `scope=surface|resource|both` with default `both`
- `cursor`
- `page_size`

Rules:

- address history reuses the existing normalized-event history contract; it does not introduce a separate address-history ledger or projection family
- `namespace` and `relation` filter which related surfaces and resources contribute anchors for the requested address across current and historical matches; they do not change history row shape, ordering, or coverage meaning
- `scope=surface` returns events anchored by any `logical_name_id` selected for the requested address across current and historical matches under the active filters
- `scope=resource` returns events anchored by any `resource_id` selected for the requested address across current and historical matches under the active filters
- `scope=both` returns the union of those anchor sets
- observed and orphaned events are excluded from this route
- this route follows the shared history default sort `chain_position_desc`
- `cursor` and `page_size` page over that frozen default sort only; they do not alter row shape, anchor semantics, or coverage meaning
- `declared_state` is `{}` for history routes; the normalized-event rows themselves are the declared answer

### `GET /v1/resolvers/{chain_id}/{resolver_address}`

This route ships as the resolver-overview read.

`data` identifies the resolver target. `declared_state` groups:

- current bindings: `ResolverOverviewBindingSummary | UnsupportedSummary`
- alias mappings: `ResolverOverviewBindingSummary`
- resolver-scoped permissions
- role-holder summary
- resolver event summary

Supported query parameters:

- none in the initial contract

Rules:

- resolver overview is declared-state only and `verified_state` remains `null`
- supported `declared_state.bindings` includes every current resolver-linked binding whose current resolver target matches the route target, regardless of `binding_kind`
- supported `declared_state.aliases` ships in the initial resolver-overview contract and reuses the same `{status, count, items}` summary envelope as `bindings`, but `items` is only the current `binding_kind=resolver_alias_path` subset of those same resolver-linked bindings
- `declared_state.aliases` is sourced from current resolver-linked bindings only; it does not enumerate historical alias rows or create a second alias ledger
- when no current alias binding exists for the target resolver, `declared_state.aliases` returns `{status:"supported", count:0, items:[]}`
- counts for nodes, aliases, and role holders live inside those declared summaries rather than as a separate truth system
- any other declared summary that is not yet projected returns `UnsupportedSummary`

### `GET /v1/resolutions/{namespace}/{name}`

This route ships one mixed declared+verified envelope for resolution reads.

Supported query parameters:

- `at`
- `consistency`
- `mode=declared|verified|both`
- `records`

`data` identifies the same surface and current binding as `GET /v1/names/{namespace}/{name}` for the requested snapshot.

When `declared_state` is populated, it includes:

- `topology`: `ResolutionTopology | UnsupportedSummary`
- `record_inventory`: `ResolutionRecordInventory | UnsupportedSummary`
- `record_cache`: `ResolutionRecordCache | UnsupportedSummary`

When `verified_state` is populated, it includes:

- `verified_queries`

When all declared sections are supported, they use this exact field structure:

```json
{
  "topology": {
    "registry_path": [
      {
        "logical_name_id": "ens:alice.eth",
        "namespace": "ens",
        "normalized_name": "alice.eth",
        "canonical_display_name": "alice.eth",
        "namehash": "0x...",
        "resource_id": "00000000-0000-0000-0000-000000000000",
        "binding_kind": "declared_registry_path"
      }
    ],
    "subregistry_path": [],
    "resolver_path": [
      {
        "logical_name_id": "ens:alice.eth",
        "namespace": "ens",
        "normalized_name": "alice.eth",
        "canonical_display_name": "alice.eth",
        "resource_id": "00000000-0000-0000-0000-000000000000",
        "chain_id": "ethereum-mainnet",
        "address": "0x0000000000000000000000000000000000000000",
        "latest_event_kind": "ResolverChanged"
      }
    ],
    "wildcard": {
      "source": null,
      "matched_labels": []
    },
    "alias": {
      "final_target": null,
      "hops": []
    },
    "version_boundaries": {
      "topology_version_boundary": {
        "logical_name_id": "ens:alice.eth",
        "resource_id": "00000000-0000-0000-0000-000000000000",
        "normalized_event_id": null,
        "event_kind": null,
        "chain_position": {
          "chain_id": "ethereum-mainnet",
          "block_number": 0,
          "block_hash": "0x0",
          "timestamp": "2026-04-16T00:00:00Z"
        }
      },
      "record_version_boundary": {
        "logical_name_id": "ens:alice.eth",
        "resource_id": "00000000-0000-0000-0000-000000000000",
        "normalized_event_id": null,
        "event_kind": null,
        "chain_position": {
          "chain_id": "ethereum-mainnet",
          "block_number": 0,
          "block_hash": "0x0",
          "timestamp": "2026-04-16T00:00:00Z"
        }
      }
    },
    "transport": {
      "source_chain_id": null,
      "target_chain_id": null,
      "contract_address": null,
      "latest_event_kind": null
    }
  },
  "record_inventory": {
    "record_version_boundary": {
      "logical_name_id": "ens:alice.eth",
      "resource_id": "00000000-0000-0000-0000-000000000000",
      "normalized_event_id": null,
      "event_kind": null,
      "chain_position": {
        "chain_id": "ethereum-mainnet",
        "block_number": 0,
        "block_hash": "0x0",
        "timestamp": "2026-04-16T00:00:00Z"
      }
    },
    "enumeration_basis": {
      "observed_selectors": true,
      "capability_declared_families": true,
      "globally_enumerable": false
    },
    "selectors": [],
    "explicit_gaps": [],
    "unsupported_families": [],
    "last_change": null
  },
  "record_cache": {
    "record_version_boundary": {
      "logical_name_id": "ens:alice.eth",
      "resource_id": "00000000-0000-0000-0000-000000000000",
      "normalized_event_id": null,
      "event_kind": null,
      "chain_position": {
        "chain_id": "ethereum-mainnet",
        "block_number": 0,
        "block_hash": "0x0",
        "timestamp": "2026-04-16T00:00:00Z"
      }
    },
    "entries": []
  }
}
```

Rules:

- `topology`, `record_inventory`, and `record_cache` are always present as objects when `declared_state` is populated; any declared section that is not yet projected returns `UnsupportedSummary`
- callers must round-trip the surfaced `record_key` strings in `records`; `record_family` and `selector_key` are explanatory fields, not alternate request identity
- `record_inventory` defines the known record-selector space, explicit gaps, and the current version boundary for the requested surface; it does not imply global record enumeration
- `record_cache` is the declared last-known-value view over that same selector space and version boundary; it never implies that verified execution was run
- `topology.version_boundaries.record_version_boundary` must equal `record_inventory.record_version_boundary` and `record_cache.record_version_boundary` when those sections are supported together
- selector-level declared cache results live in `record_cache.entries`
- `record_cache.entries[*]` and `verified_queries[*]` always echo the applicable `record_key`, even when the selector status is not `success`
- `records` is a comma-separated list of explicit record selectors; selectors use the stable `record_key` strings surfaced by `record_inventory`, and the contract permits additive selector families without changing the envelope shape
- in `mode=declared`, `records` is optional; if supplied, `record_cache` narrows to the requested selectors, otherwise it returns every cacheable selector visible at the current version boundary
- in `mode=verified` or `mode=both`, `records` is required and duplicate selectors are rejected with `400 invalid_input`
- malformed selector syntax returns `400 invalid_input`
- if the exact surface does not exist for the requested namespace and snapshot, return `404 not_found`
- `verified_queries` returns one result object per requested selector in request order
- `verified_queries[*].status` uses the shared `ResultStatus` vocabulary; the initial resolution contract uses `success`, `not_found`, `unsupported`, and `execution_failed`
- unsupported selector families, unsupported resolver families, or namespaces without a verified entrypoint return `200` with `verified_queries[*].status=unsupported`; they do not silently downgrade to declared cache values
- supported verified queries that execute but do not produce a trustworthy answer return `status=execution_failed` with `failure_reason`
- for `mode=verified` or `mode=both`, top-level `provenance` includes the request-scoped execution trace summary and each `verified_queries[*]` item may carry narrower provenance for the specific selector result
- deeper execution explanation stays on the shipped `GET /v1/explain/resolutions/{namespace}/{name}/execution` route; `GET /v1/resolutions/{namespace}/{name}` does not inline ordered step lists or a raw trace dump
- route-level `coverage` explains declared completeness for topology, inventory, and cache at the requested snapshot; per-selector verified misses or failures do not change that shared route-level `coverage` object by themselves

### `GET /v1/explain/resolutions/{namespace}/{name}/execution`

This route is the shipped exact-name resolution execution explain read.

Returns the verified explain view for one exact-name resolution request backed by a persisted execution trace.

Supported query parameters:

- `records`

`records` is required and uses the same stable `record_key` selector tokens as `GET /v1/resolutions/{namespace}/{name}`.

`data` identifies the same current surface and current binding as `GET /v1/resolutions/{namespace}/{name}`.

When `verified_state` is populated, it includes:

- `execution`: `ResolutionExecutionExplainSummary`
- `verified_queries`

Rules:

- `declared_state` is `null`
- this route is verified-state only; it does not duplicate `declared_state.topology`, `record_inventory`, or `record_cache`
- the shipped route publishes path parameters plus required `records` only; `at` and `consistency` are not part of this route contract
- duplicate `records` selectors are rejected with `400 invalid_input`, and malformed selector syntax returns `400 invalid_input`, using the same parsing rules as `GET /v1/resolutions/{namespace}/{name}`
- this route is keyed by the same current exact surface and explicit selector set as `GET /v1/resolutions/{namespace}/{name}`; it explains the persisted verified answer that the mixed route would surface for those same inputs
- `verified_state.verified_queries` reuses the same selector-scoped result objects, request order, and `ResultStatus` subset as the mixed resolution route
- `verified_state.execution.execution_trace_id` must equal top-level `provenance.execution_trace_id`
- top-level `provenance` anchors the response to the persisted execution trace, and any `verified_queries[*].provenance` objects must stay within that same `execution_trace_id` rather than creating a second provenance system
- `verified_state.execution.resolver_discovery_path`, `wildcard`, and `alias` explain the runtime path selected for that persisted trace; they do not widen declared topology into a second truth model
- `verified_state.execution.steps` is the ordered persisted step summary for the trace and must not be treated as raw calldata, raw gateway payloads, or a replayable execution dump
- the route does not trigger fresh execution and does not synthesize explanation from declared topology alone; if no persisted verified resolution answer exists for the requested current surface and explicit selector set, return `404 not_found`
- for the same `{namespace}`, `{name}`, and `records` request, the top-level `coverage` object matches `GET /v1/resolutions/{namespace}/{name}`
- the initial contract defines no `include` expansions for this route

### `GET /v1/primary-names/{address}`

Supported query parameters:

- `mode=declared|verified|both`
- `coin_type`
- `namespace`

This route is keyed by one `(address, namespace, coin_type)` tuple. `namespace` and `coin_type` are required.

`data` identifies the requested tuple:

- `address`
- `namespace`
- `coin_type`

When `declared_state` is populated, it returns:

- `claimed_primary_name`

When `verified_state` is populated, it returns:

- `verified_primary_name`

Rules:

- the shipped bootstrap route is head-only; it does not honor `at` or `consistency`, and additive snapshot-selector support remains pending
- for ENS on Ethereum Mainnet, `claimed_primary_name` currently consults only the reverse-claim surface owned by `ens_v1_reverse_l1` through contract role `reverse_registrar` at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`
- missing or unsupported ENS reverse claims do not trigger fallback to registry-, resolver-, or other claim-setting surfaces in the current contract
- any fallback beyond that reverse-only ENS claim surface remains deferred and requires a later doc-first contract update; manifest presence alone does not widen the shipped precedence rule
- in Phase 7, that reverse-only ENS claim precedence does not combine with resolver-backed name data or verification-derived identity to populate richer `claimed_primary_name` fields
- `claimed_primary_name` is the declared claim candidate only; it never implies that the requested address actually verifies to that name
- `claimed_primary_name.status` uses the shared `ResultStatus` vocabulary; the initial declared contract uses `success`, `not_found`, `unsupported`, and `invalid_name`
- `verified_primary_name.status` uses the same `ResultStatus` vocabulary; the initial verified contract uses `success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, and `execution_failed`
- `claimed_primary_name` and `verified_primary_name` always include `status` when their containing section is populated
- if a later additive slice ships richer tuple-present claimed payloads, `claimed_primary_name` is frozen to the field boundary `{status, name?, raw_claim_name?, unsupported_reason?, provenance?}`; `name`, when later shipped, carries the normalized claim identity using the shared `NameRef` shape, `raw_claim_name` appears only for `status=invalid_name`, and `failure_reason` is not used on this declared object
- if a later additive slice ships richer tuple-present verified payloads, `verified_primary_name` is frozen to the field boundary `{status, name?, unsupported_reason?, failure_reason?, provenance?}`; its `name`, when later shipped, uses that same `NameRef` shape, and `raw_claim_name` never appears on this execution-derived object
- in the shipped Phase 7 bootstrap slice, richer ENS tuple-present payloads remain blocked: reverse tuple presence, reverse-only claim precedence, or verification establishing a concrete normalized name target do not by themselves populate `claimed_primary_name.name`, `claimed_primary_name.raw_claim_name`, or claim-local section provenance
- when those additive fields later ship, `claimed_primary_name.name` appears only for `status=success`; `verified_primary_name.name` appears only for `status=success` or `status=mismatch`, where the route established a concrete normalized name target for verification
- the richer tuple-present fields above are prose-frozen only until additive handler support ships; the current bootstrap handler and current `docs/api-v1.openapi.json` publication remain limited to the already shipped bootstrap envelope
- `verified_primary_name` is authoritative only when `status=success`
- `status=mismatch` applies only to `verified_primary_name` and remains reserved for the additive verified result shape where the claim normalizes and resolves for the requested `coin_type`, but the verified target address does not equal the requested `{address}`
- `verified_primary_name.failure_reason` is verification-local and may appear only for `status=mismatch`, `status=invalid_name`, or `status=execution_failed`; it must not be used to restate declared claim identity or to duplicate `raw_claim_name`
- when both sections are populated and the raw claim exists but cannot be normalized, `claimed_primary_name.status=invalid_name` may carry `raw_claim_name` while `verified_primary_name.status=invalid_name` remains limited to verification-local fields
- invalid address syntax, missing required `namespace` or `coin_type`, or a malformed query tuple returns `400 invalid_input`
- an unsupported public namespace returns `404 not_found`
- no declared or verified primary-name answer for the requested tuple returns `200` with `status=not_found`; it does not turn the route into `404`
- unsupported claim surfaces or unsupported verified entrypoints return `200` with the corresponding object `status=unsupported`
- top-level `provenance` summarizes the declared claim inputs and, when an execution-derived verified answer is present, the verification trace
- `claimed_primary_name.provenance`, when later shipped, is claim-local declared provenance and must not introduce a second `execution_trace_id`
- `verified_primary_name.provenance`, when later shipped, is verification-local provenance and must stay within top-level `provenance.execution_trace_id`
- in the shipped bootstrap handler, route-level `coverage` remains bootstrap-only for primary-name lookup: `status=unsupported`, `exhaustiveness=not_applicable`, `source_classes_considered=[]`, `enumeration_basis=primary_name_lookup`, and `unsupported_reason="primary-name coverage is not yet supported"`
- tuple presence, tuple absence, a verified mismatch, or resolver-backed verification detail does not by itself graduate that bootstrap primary-name coverage summary

## 6. Sorting And Pagination Defaults

- `GET /v1/addresses/{address}/names` defaults to `display_name_asc` and honors replay-stable `cursor` and `page_size`
- `GET /v1/names/{namespace}/{name}/children` defaults to `display_name_asc` and honors replay-stable `cursor` and `page_size`
- `GET /v1/resources/{resource_id}/permissions` defaults to `subject_scope_asc` and honors replay-stable `cursor` and `page_size`
- `GET /v1/history/names/{namespace}/{name}` defaults to `chain_position_desc` and honors replay-stable `cursor` and `page_size`
- `GET /v1/history/resources/{resource_id}` defaults to `chain_position_desc` and honors replay-stable `cursor` and `page_size`
- `GET /v1/history/addresses/{address}` defaults to `chain_position_desc` and honors replay-stable `cursor` and `page_size`
- no other shipped route honors `cursor` or `page_size` in the initial contract
- ties break on `logical_name_id` for surface-first views and `resource_id` for resource-grouped address views

Cursor pagination must be stable under replay for the same requested chain positions. `page.cursor` echoes the applied cursor or `null` on the first page, and `page.next_cursor=null` means there are no further rows at the requested snapshot.

## 7. Error Model

Every non-2xx response returns:

```json
{
  "error": {
    "code": "unsupported",
    "message": "the requested route option is not supported",
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

Rules:

- use non-2xx `unsupported` only when the request cannot produce the route contract at all for the requested shape
- when a mixed route can produce the envelope but one declared or verified subsection is unsupported, return `200` and surface that state through `UnsupportedSummary` or the shared `ResultStatus` vocabulary instead of raising a route-level `unsupported` error

## 8. Versioning Rules

- new optional fields are additive within `v1`
- new routes are additive within `v1`
- changing enum meaning, default sort, coverage semantics, or required fields requires `v2`
- if a capability is unsupported for a namespace or source class, return it explicitly in `coverage` or `error`, never through silent omission
