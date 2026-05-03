# API v1 Contract

Status: Phase 0 baseline

This document freezes the external `v1` read contract strongly enough for API, projection, and SDK work to proceed in parallel.

## 1. Conventions

- all routes live under `/v1`
- responses are JSON with `snake_case` keys
- timestamps are RFC 3339 UTC strings
- semantic identities are strings; opaque internal IDs are never inferred by clients
- `namespace` is always explicit for canonical name-based reads; any documented namespace-inferred convenience route still returns the inferred namespace in identity fields
- names in path segments are normalized names, URL-encoded as plain text
- full-envelope answers include provenance, coverage, chain position context, and consistency; compact app-facing routes document narrower default metadata and keep full provenance/coverage opt-in

### Common query parameters

- `at`: point-in-time selector, either an RFC 3339 timestamp or a chain-position token
- `chain_positions`: explicit point-in-time selector, encoded as one URL query value containing a JSON object with the same per-position object shape as `ChainPositions`
- `consistency`: `head`, `safe`, or `finalized`
- `mode`: `declared`, `verified`, or `both`
- `include`: comma-separated expansions; default is route-specific
- `view`: `compact` or `full`; default is route-specific
- `meta`: `none`, `summary`, or `full`; default is route-specific
- `sort`: route-specific stable sort key
- `order`: `asc` or `desc`
- `cursor`: opaque pagination cursor
- `page_size`: default `50`, max `200`

Routes declare which subset of these parameters they honor. Section 6 freezes the exact shipped collection routes that honor `cursor` and `page_size`. Unlisted parameters are reserved for additive route support and do not widen the shipped declared-state contract.

Defaults:

- `consistency=head`
- `mode=declared`
- no `at` and no `chain_positions` selects `consistency=head` and the latest stored checkpoint for each chain required by the route; any on-demand verified execution targets those selected block positions, not a provider's newer head

### Snapshot Selection Rules

Snapshot selection resolves caller input to a concrete `ChainPositions` object before any route-specific read is performed. `chain_positions` query values use route-declared position slot keys and per-position `{chain_id, block_number, block_hash, timestamp}` objects; they are not a second selector vocabulary. The selected object is echoed in the response as `chain_positions`.

| Inputs | Rule |
| --- | --- |
| `chain_positions` only | use the supplied positions exactly |
| `at` only | resolve per-chain positions at the requested `consistency` |
| neither | use the latest available positions at the requested `consistency` |
| both `at` and `chain_positions` | reject with `invalid_input` |

Validation rules:

- if `chain_positions` is supplied, every chain required by the route must be present
- if `chain_positions` is supplied, unsupported position slots for that route are rejected with `invalid_input`
- if `chain_positions` is malformed, has duplicate position slots, mixes deployment profiles, or names a `chain_id` that does not match the selected deployment profile, reject with `invalid_input`
- if `chain_positions` is supplied, the server validates that each supplied position satisfies the requested `consistency` floor, including the default `consistency=head`, or returns `conflict`
- if a supplied `(chain_id, block_number, block_hash)` does not match stored lineage, is orphaned for the requested floor, or cannot be reconciled with the other supplied positions as one route snapshot, return `conflict`
- if a valid selector resolves to positions for which the required projection rows have not been built, return `stale` rather than reading raw facts or silently advancing to a newer projection
- if matching persisted execution output is absent, persisted-readback routes and entrypoints return their documented stale or not-found state; the documented exception is supported ENS verified resolution on `GET /v1/resolutions/{namespace}/{name}` and `GET /v1/resolve/{name}`, which may perform on-demand execution after snapshot selection and persist the outcome before returning
- current-state projection rows may serve a later selected snapshot only when the projection's stored chain-position context is on the same required chain set and no newer canonical projection input for that row's keys exists at or before the selected positions; otherwise the route returns `stale`

Cross-chain rules:

- in the shipped mainnet profile, ENS authoritative positions are selected on `ethereum-mainnet`
- in the shipped mainnet profile, Basenames authoritative positions are selected on `base-mainnet` because upstream deploys the Basenames registry / registrar / resolver system on Base rather than Ethereum Mainnet (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- when a route also needs an auxiliary chain, choose the auxiliary position at the same requested consistency with timestamp less than or equal to the authoritative-chain timestamp
- verified execution runs against the resolved positions only; it does not advance to a newer head mid-request

Deployment-profile rules:

- later Sepolia support may reuse the same route semantics with a different admitted chain set
- one deployment answers under exactly one profile at a time; responses and explicit `chain_positions` must not mix mainnet and Sepolia chain keys
- the promoted ENSv2 `sepolia-dev` exact-name profile is supported only when that deployment profile is selected, and only for declared exact-name profile reads backed by the admitted `ETHRegistry` and `ETHRegistrar` source classes (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)
- this promotion does not admit the mainnet profile, reverse or primary-name reads, wrapper-derived authority, migration history, universal-resolver entrypoints, verified resolution, or execution-explain surfaces

### Exact-Name Snapshot Selector

The exact-name snapshot selector applies to:

- `GET /v1/names/{namespace}/{name}`
- `GET /v1/coverage/{namespace}/{name}`
- `GET /v1/explain/names/{namespace}/{name}/surface-binding`
- `GET /v1/explain/names/{namespace}/{name}/authority-control`
- `GET /v1/resolutions/{namespace}/{name}` for its exact-name data row, declared topology, declared record inventory/cache joins, route-level coverage, verified support checks, and verified execution target selection

Full-envelope rules:

- resolve `at`, explicit `chain_positions`, and `consistency` once for the request, before exact-name lookup, coverage lookup, declared topology construction, explain construction, persisted execution lookup, or on-demand execution
- all exact-name route sections in the same response use that one selected `ChainPositions` object; a response must not combine the current binding from one snapshot with coverage, topology, resolver summaries, record inventory/cache, permission summaries, history pointers, or execution output from another snapshot
- an exact-name `name_current` row whose stored position is older than the selected snapshot remains eligible only when the worker-owned invalidation check finds no newer canonical normalized event or surface-binding input for that `logical_name_id` or current `resource_id` through the selected positions
- a `record_inventory_current` row whose stored position is older than the selected snapshot remains eligible only when its chain-position context is on the same required chain set, both endpoints are canonical lineage members, and the worker-owned invalidation check finds no newer canonical `RecordChanged`, `RecordVersionChanged`, or `ResolverChanged` input for that `logical_name_id` or `resource_id` through the selected positions
- `GET /v1/coverage/{namespace}/{name}` returns the same top-level `coverage` object as `GET /v1/names/{namespace}/{name}` for the same `{namespace, name}` and selected snapshot
- the surface-binding and authority-control explain routes identify the same current `logical_name_id`, `resource_id`, `token_lineage_id`, `surface_binding_id`, and `binding_kind` that the exact-name route selects at that snapshot, subject to each route's documented field shape
- `GET /v1/resolutions/{namespace}/{name}` uses the same selected exact-name snapshot for `data`, `declared_state.topology`, `declared_state.record_inventory`, `declared_state.record_cache`, route-level `coverage`, verified support checks, and any on-demand verified execution
- in `mode=verified|both`, verified output may join the response only when its request chain positions exactly match the selected snapshot; if matching output is absent for a supported ENS verified-resolution selector, the API executes that selector against the selected exact-name snapshot, persists the trace and outcome, and returns the newly persisted output
- verified execution never advances the selected positions mid-request; unsupported selector families, unsupported verified path classes, and persisted-readback-only entrypoints keep their documented unsupported, stale, or not-found behavior rather than reading raw facts or declared cache values
- when no `at` or `chain_positions` selector is supplied, the selected exact-name snapshot is `consistency=head` at the latest stored checkpoint, and live execution targets that selected chain position
- live ENS verified resolution requires an API Ethereum RPC provider configured for the API process; if the provider is missing or cannot serve the selected block, supported selectors return `409 stale` with a configuration message rather than falling back to `declared_state.record_cache`
- API handlers serve these public responses from projections and execution outputs after selector resolution; they do not synthesize exact-name answers, coverage, topology, explain detail, or verified readback directly from raw facts or adapter-owned normalized events
- `GET /v1/resolve/{name}` remains the namespace-inferred convenience route for resolution and currently exposes only its documented `mode` and `records` query parameters; after namespace inference, its exact-name joins use the canonical route's default snapshot selector and do not admit `at`, `chain_positions`, or `consistency` on this convenience route in the shipped contract

## 2. Shared Response Envelope

Full-envelope single-resource reads return:

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

Full-envelope collection reads replace `data` with an array and add:

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

Compact app-facing reads are a narrower response view for Manager / Explorer indexer replacement work. Routes that declare `view=compact` default to `meta=summary` unless they say otherwise. Compact responses keep `data` and, for collections, `page`; they do not include `declared_state`, `verified_state`, top-level `provenance`, full `coverage`, internal projection identifiers, or raw normalized-event IDs by default.

```json
{
  "data": [],
  "page": {
    "cursor": null,
    "next_cursor": null,
    "page_size": 50,
    "sort": "name_asc"
  },
  "meta": {
    "support_status": "partial",
    "unsupported_filters": [],
    "unsupported_fields": [],
    "total_count": null
  }
}
```

Compact metadata rules:

- `meta=none` omits `meta`; collection `page` stays present because pagination is part of the data contract. App-facing `data` must still stay compact and must not carry provenance, source summaries, unsupported field lists, or projection bookkeeping as a substitute for `meta`.
- `meta=summary` may include only route-level support state, unsupported filters or fields, count metadata, and selected snapshot summary; it must not include raw facts or full provenance
- `meta=full` is opt-in and may include the same top-level `coverage`, `chain_positions`, `consistency`, `last_updated`, and route-level `provenance` summaries used by full-envelope routes
- `view=full` returns the route's full envelope when the route documents one; otherwise it is reserved and returns `400 invalid_input`
- explain/audit detail remains on the explicit explain/audit routes; compact app-facing routes must not expose hidden projection internals through default metadata

Full-envelope rules:

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

For ENSv1 and Basenames, this exact-name resolver summary is topology only. It does not prove complete resolver-local record, cache, or resolver-overview coverage. For ENSv1, retained generic resolver-local record events may still produce observed selector-level cache successes while profile state is pending; complete family coverage, resolver overview completeness, latest-only behavior, and onchain-call parity claims require the resolver address to be admitted to the relevant ENS Labs PublicResolver-generation profile for the requested family (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IVersionableResolver.sol:L5 @ ens_v1@91c966f). Basenames complete family coverage still requires the resolver address to be direct manifest-admitted or discovery-admitted into the relevant resolver source family and admitted as a supported `L2Resolver`-compatible profile for the relevant record family (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L61 @ basenames@1809bbc).

For ENSv1, resolver `NameChanged` text observed through an admitted reverse / primary claim path may reveal the forward-name preimage used by exact-name and resolution projections. That text does not by itself make `GET /v1/resolve/{name}` found, does not prove primary-name truth, and does not populate resolver records unless replay also has matching forward-node registry / resolver observations for the computed namehash (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L10 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L18 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f).

For ENSv1 discovered resolver targets, the first complete supported dynamic profile set is ENS Labs PublicResolver-generation-compatible and profile-exact. A resolver target whose profile state is `pending` or `unsupported` may still expose retained event-evidenced selector/cache entries, but it must not make complete record family coverage or resolver overview appear supported from topology alone. A resolver target admitted as an older PublicResolver generation exposes only that generation's supported sections; it does not inherit latest-only name-wrapper awareness, default coin-type fallback, VersionableResolver boundaries, DNS records, text, contenthash, ABI, name, interface, pubkey, or `DataResolver` support. Latest app-known PublicResolver compatibility is anchored to the upstream PublicResolver profile mixins, ERC165 support, and ResolverBase record-versioning, but only the app-known resolver interfaces admitted in `docs/manifests.md` graduate public coverage (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L23 @ ens_v1@91c966f).

ENSv1 observed resolver events do not by themselves graduate public coverage. Pubkey evidence is ignored for this API version until a doc-first pubkey admission exists. `DataResolver` evidence is retained as a known resolver-family signal, but known PublicResolver-generation profiles report it as unsupported and unknown resolver implementations report it as pending / unknown; neither case may inherit support from generic `resolver_record` observation.

For Basenames discovered Base-side resolver targets, the first complete supported dynamic profile is `L2Resolver`-compatible only. A Base resolver target whose contract instance is admitted for watching but whose profile state is `pending` or `unsupported` must not make complete record family coverage or resolver overview appear supported from topology alone. `L2Resolver` compatibility is separate from the ENSv1 PublicResolver-generation profile gate and from Basenames L1 transport / execution; it is anchored to the upstream `L2Resolver` profile mixins, extended-resolution and ERC165 support, and authorization model (upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc).

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
- when compatibility transport participates, `transport.source_chain_id` names the declared-authority chain and `transport.target_chain_id` names the compatibility-entrypoint chain; for the frozen Basenames promotion-target class that freezes to `base-mainnet -> ethereum-mainnet` through the Basenames L1 Resolver because upstream deploys the Basenames authority stack on Base and the `L1Resolver` on Ethereum Mainnet (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
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
- when `topology.resolver_path` terminates in the explicit no-declared-resolver hop (`chain_id=null`, `address=null`), `record_inventory` is supported with an empty selector set at the exact-name record boundary, and requested `record_cache.entries[*]` return `status="not_found"` rather than a projection-missing unsupported object
- for ENSv1 and Basenames, resolver-local selector and cache facts may be populated from retained current-resolver record events even while resolver-profile admission is pending; for ENSv1 those retained facts can come from generic resolver-event topics rather than only individually profile-admitted resolver targets. A retained generic resolver-topic collision whose indexed fields or ABI payload do not decode as the upstream ENSv1 resolver event shape does not create a route-visible selector/cache fact or coverage signal. Unobserved selectors in that family still surface explicit gaps or unsupported families such as `unsupported_reason="resolver_family_pending"` rather than silently appearing absent or complete (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L10 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L61 @ basenames@1809bbc)
- for ENSv1 discovered resolvers, `unsupported_reason="resolver_family_pending"` is the required route-visible state for a resolver whose ENS Labs PublicResolver-generation profile state is still `pending` and has no retained current-resolver event for the requested selector; `unsupported` profile state and unsupported interfaces on admitted legacy generations must remain explicit as `unsupported_reason="resolver_family_unsupported"` for non-event-evidenced coverage until a later doc-first profile admission supports that resolver family (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f)
- for Basenames discovered Base-side resolvers, `unsupported_reason="resolver_family_pending"` is the required route-visible state for a watched resolver whose `L2Resolver`-compatible profile state is still `pending`; `unsupported` profile state must remain explicit as `unsupported_reason="resolver_family_unsupported"` until a later doc-first profile admission supports that resolver family (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)

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

### `CompactDomainSummary`

- `namespace`
- `name`
- `normalized_name`
- `namehash`
- `labelhash`
- `token_id`
- `owner`
- `registrant`
- `created_at`
- `registration_date`
- `expiry_date`
- `resolver_address`
- `record_summaries`
- `subname_count`
- `record_count`

Use this object for app-facing name collections and exact compact lookup on `GET /v1/names`. `name` is the display name selected by the projection, and `normalized_name` is the normalized lookup value. `labelhash` and `token_id` are present only when the namespace projection exposes a stable namespace-local token identity. `record_summaries`, `subname_count`, and `record_count` are optional compact fields; when a requested field is not projected, the field is `null` or omitted according to its route rule and `meta.unsupported_fields` must identify the unsupported field unless `meta=none`.

This object intentionally omits full provenance, full coverage, `logical_name_id`, `resource_id`, `surface_binding_id`, projection version, and raw normalized-event identifiers by default. Routes that need those fields use `view=full` where documented, the canonical exact-name route, or an explain/audit route.

### `CompactRecordSummary`

- `resolver_address`
- `text_records`
- `known_text_keys`
- `avatar`
- `content_hash`
- `coin_addresses`

Use this object for `GET /v1/names/{namespace}/{name}/records` and namespace-inferred `GET /v1/resolve/{name}/records`. `known_text_keys` is declared inventory/cache metadata, never a verified enumeration result. `text_records`, `avatar`, `content_hash`, and `coin_addresses` use the selected compact value source recorded in `meta.value_source`: declared cache for `mode=declared`, verified resolution output for `mode=verified`, and automatic declared-or-verified selection for `mode=auto`. Declared ENSv1 text records are selector-specific when the resolver event carries a key, for example `avatar` is backed by `text:avatar` rather than a generic `text` selector (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L21 @ ens_v1@91c966f). Record value objects expose only compact `status`, `value`, and failure/unsupported reason fields; selector identity, inventory, provenance, and source bookkeeping stay out of `data`. When `mode=auto|verified|both` has no declared record selectors to work from, compact routes may probe the bounded basic app profile set (`addr:60`, `avatar`, `contenthash`, and text keys `description`, `url`, `email`) so a current UI read can still return useful verified records; fallback text keys that resolve to `not_found` are omitted from `text_records` unless the caller requested them explicitly.

### `CompactHistoryEvent`

- `type`
- `name`
- `namespace`
- `resource_id`
- `block_number`
- `timestamp`
- `transaction_hash`
- `log_index`
- `data`

Use this object for `view=compact` on history routes and for `GET /v1/events`. `data` is a route-owned compact payload for the event type; raw log bodies, raw calldata, and full normalized-event rows stay out of the compact default.

### `RoleRow`

- `account`
- `resource_hex`
- `resource_id`
- `name`
- `role_bitmap`
- `effective_powers`
- `provenance`

Use this object for app-facing role reads. `resource_id` is the stable internal resource identity surfaced by the API; clients must treat it as opaque. `resource_hex` is nullable and appears only when a stable projected resource hex exists for the row. `provenance` is compact section provenance, not the full normalized-event lineage; full lineage remains on `GET /v1/resources/{resource_id}/permissions` or explain/audit routes.

### `ResolverOverviewCompact`

- `chain_id`
- `resolver_address`
- `counts`
- `nodes`
- `aliases`
- `roles`
- `events`

Use this object for `GET /v1/resolvers/{chain_id}/{resolver_address}/overview`. `counts` reports only sections backed by `resolver_current` or another declared projection family named by the route. Unsupported sections are named in `meta.unsupported_fields`; unsupported projected fan-in must not be rendered as a supported zero count.

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

Use this object for supported enumerable `declared_state.bindings` and `declared_state.aliases` on `GET /v1/resolvers/{chain_id}/{resolver_address}`. `count` equals `items.length`. `status=supported` with `count=0` and `items=[]` is valid. `declared_state.aliases` reuses this exact shape but narrows `items` to the `binding_kind=resolver_alias_path` subset of the same current resolver-linked binding rows.

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

Rules:

- `execution_trace_id` appears only when the provenance includes execution-derived material
- declared-only provenance objects, including `claimed_primary_name.provenance`, omit `execution_trace_id`

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

The current API binary ships the routes marked `shipped` below. Compact app-facing routes that were previously prose-frozen are now implemented; future route additions must remain additive within `v1`.

| Route | Purpose | Contract state |
| --- | --- | --- |
| `GET /v1/namespaces/{namespace}` | Namespace metadata and support status | shipped declared-state |
| `GET /v1/names` | App-facing compact name search, exact lookup, address relation lists, and suggestions | shipped compact declared-state |
| `GET /v1/names/{namespace}/{name}` | Exact name lookup | shipped declared-state |
| `GET /v1/explain/names/{namespace}/{name}/surface-binding` | Current surface-binding explain view for one exact name | shipped declared-state |
| `GET /v1/explain/names/{namespace}/{name}/authority-control` | Current authority/control explain view for one exact name | shipped declared-state |
| `GET /v1/names/{namespace}/{name}/children` | Compact declared child collection by default, with full envelope available through `view=full` | shipped compact/full declared-state |
| `GET /v1/names/{namespace}/{name}/records` | App-facing compact resolver records over declared inventory/cache and optional verified selectors | shipped compact mixed declared+verified |
| `GET /v1/names/{namespace}/{name}/roles` | App-facing role rows for a name's current resource | shipped compact declared-state |
| `GET /v1/history/names/{namespace}/{name}` | Surface or combined history | shipped declared-state |
| `GET /v1/history/resources/{resource_id}` | Resource history | shipped declared-state |
| `GET /v1/history/addresses/{address}` | Address activity across related surfaces and resources | shipped declared-state |
| `GET /v1/events` | App-facing compact event search across name, address, resource, type, and block filters | shipped compact declared-state |
| `GET /v1/manifests/{namespace}` | Active manifest versions and capabilities | shipped declared-state |
| `GET /v1/addresses/{address}/names` | Address-to-surface collection | shipped declared-state |
| `GET /v1/addresses/{address}/names/count` | App-facing count for address relation filters | shipped compact declared-state |
| `GET /v1/roles` | App-facing role rows by account, resource, or name lookup filters | shipped compact declared-state |
| `GET /v1/resources/lookup` | App-facing lookup from namespace/name to the current opaque resource identity | shipped compact declared-state |
| `GET /v1/resources/{resource_id}/permissions` | Resource-centric effective permissions | shipped declared-state |
| `GET /v1/resolvers/{chain_id}/{resolver_address}` | Resolver overview | shipped declared-state |
| `GET /v1/resolvers/{chain_id}/{resolver_address}/overview` | App-facing compact resolver overview with projected counts, nested lists, and events | shipped compact declared-state |
| `GET /v1/resolutions/{namespace}/{name}` | Resolution topology, inventory, and verified reads | shipped mixed declared+verified |
| `GET /v1/resolve/{name}` | Namespace-inferred convenience route for the same resolution response | shipped mixed declared+verified |
| `GET /v1/resolve/{name}/records` | Namespace-inferred compact resolver records over declared inventory/cache and optional verified selectors | shipped compact mixed declared+verified |
| `GET /v1/explain/resolutions/{namespace}/{name}/execution` | Persisted verified execution explain for one exact-name resolution request | shipped verified-state explain |
| `GET /v1/primary-names/{address}` | Exact-tuple claimed and verified primary-name answer | shipped mixed declared+verified with local exact-tuple coverage |
| `GET /v1/coverage/{namespace}/{name}` | Single-name coverage and explain details | shipped declared-state |

### Machine-Readable Contract Publication

Phase 6 freezes `docs/api-v1.openapi.json` as the publication location for future machine-readable contract output.

When generated, that artifact covers only the `v1` routes currently shipped by `apps/api/src/main.rs`.

The running API also serves helper documentation routes outside the frozen `v1` contract: `GET /openapi.json` returns the same generated OpenAPI document, and `GET /docs` renders that document as a browser-readable OpenAPI page. These helper routes are operational publication surfaces for the contract, not `v1` business routes, and they must not be inserted into `docs/api-v1.openapi.json` as path entries.

No prose-only app-facing REST routes remain in the table above. The app-facing compact handlers have shipped; this document remains the source of truth for route names, defaults, DTO fields, coverage, unsupported behavior, and pagination.

`GET /v1/explain/resolutions/{namespace}/{name}/execution` is now shipped and published in `docs/api-v1.openapi.json`. Its generated contract matches the current handler surface: path parameters `{namespace}` and `{name}` plus required `records`.

`GET /v1/primary-names/{address}` remains published in `docs/api-v1.openapi.json` on the same route envelope. The shipped ENS exact-tuple persisted `verified_primary_name` readback slice and the now-shipped Basenames exact-tuple persisted `verified_primary_name` readback slice keep published query parameters and the top-level response shell shape stable; they now also freeze route-level coverage as a local exact-tuple primary-name coverage object: `partial` only for the supported persisted-readback classes and `unsupported` outside those classes.

`GET /v1/resolve/{name}` and `GET /v1/resolve/{name}/records` are shipped and published in `docs/api-v1.openapi.json` as namespace-inferred convenience routes for the same resolution and compact records contracts.

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

Supported query parameters:

- `at`
- `chain_positions`
- `consistency`

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

- this route uses the exact-name snapshot selector; exact-name lookup, binding identifiers, declared summary sections, route-level `coverage`, provenance, and response `chain_positions` must describe one coherent snapshot
- the exact-name route is authoritative for supported source classes even when one or more declared summary sections are still unsupported
- for `namespace=ens` on the selected ENSv2 `sepolia-dev` profile, the promoted exact-name profile is supported for declared exact-name lookup only. It is backed by `ens_v2_registry_l1` registry state, token-resource links, label lifecycle events, and resolver-target events, plus `ens_v2_registrar_l1` `.eth` registration and renewal lifecycle events from the admitted `ETHRegistry` and `ETHRegistrar` deployments (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L21 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L41 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L47 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L63 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L21 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L45 @ ens_v2@554c309)
- the shared coverage object for that ENSv2 `sepolia-dev` exact-name profile class is `status=full`, `exhaustiveness=authoritative`, `source_classes_considered=["ens_v2_registry_l1","ens_v2_registrar_l1"]`, `enumeration_basis=exact_name_profile`, and `unsupported_reason=null`
- that supported exact-name profile coverage does not widen the shipped mainnet profile, reverse or primary-name routes, wrapper-derived authority, migration history, universal-resolver entrypoints, verified resolution, execution explain, or any resolver-local section that is still returned as `UnsupportedSummary`
- every declared summary section above is always present as an object
- if a section is not yet projected, it returns `UnsupportedSummary`
- `declared_state.authority` may fall back to `{resource_id, token_lineage_id, binding_kind}` when a dedicated authority summary is not yet projected but the current binding is known
- for `namespace=basenames`, exact-name declared truth stays on the admitted Base authority split: `basenames_base_registry`, `basenames_base_registrar`, and `basenames_base_resolver`; `basenames_base_primary` remains primary-claim intake only, and neither `basenames_l1_compat` nor `basenames_execution` widens this declared route because upstream keeps the registry / registrar / resolver stack on Base while the reverse registrar writes a separate reverse-name claim surface (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
- `declared_state.control` is the narrow current-`resource_id` control summary only; it does not inline full resource permissions, role-holder detail, or the entire internal `ControlVector`
- supported `declared_state.resolver` uses `chain_id` plus `address` as the same resolver target key used by `GET /v1/resolvers/{chain_id}/{resolver_address}` when a resolver exists; `chain_id=null` and `address=null` mean no declared current resolver rather than unsupported projection
- supported `declared_state.record_inventory` reuses the same `ResolutionRecordInventory` object shape as `GET /v1/resolutions/{namespace}/{name}` and, for the same snapshot, must expose the same `record_version_boundary`
- supported `declared_state.history.surface_head` and `declared_state.history.resource_head` point at the first canonical rows of the dedicated name-history route under `scope=surface` and `scope=resource`; the exact-name route does not add `both_head`, pagination state, or a second history truth system
- for the same `{namespace}`, `{name}`, and snapshot selection, the top-level `coverage` object matches `GET /v1/coverage/{namespace}/{name}`
- the only exact-name explain routes in Phase 6 are `GET /v1/explain/names/{namespace}/{name}/surface-binding` and `GET /v1/explain/names/{namespace}/{name}/authority-control`; they are thin views over this same exact-name target, current binding, and declared summary contract, while history explanation stays on the shipped `GET /v1/history/...` routes plus `declared_state.history.{surface_head,resource_head}` and does not introduce a separate exact-name history-explain endpoint or truth system
- the shipped exact-name route does not support `include` expansions; history, permissions, resolution, and primary-name reads stay on their dedicated routes
- `verified_state` is `null` for the shipped exact-name route

### `GET /v1/names`

This shipped route is the compact app-facing collection for Manager / Explorer indexer replacement. It covers exact name lookup, address-owned lists, owner / registrant relation lists, name search, and suggestions without exposing full projection lineage by default.

Supported query parameters:

- `namespace`
- `name`
- `prefix`
- `contains`
- `contains_nocase`
- `owner`
- `account`
- `registrant`
- `resolver`
- `resolved_address`
- `relation=token_holder|registrant|effective_controller|any`
- `sort=name|expiry_date|registration_date|created_at`
- `order=asc|desc`
- `include=record_summaries,total_count`
- `view=compact|full`
- `meta=none|summary|full`
- `cursor`
- `page_size`

Defaults:

- `view=compact`
- `meta=summary`
- `relation=any` when `account` is supplied
- `sort=name`
- `order=asc`

Each compact item is `CompactDomainSummary`.

Rules:

- `namespace` limits the collection to one public namespace; when omitted, the route may return rows across supported public namespaces and every item must include `namespace`
- `name` is exact lookup by normalized name; when paired with `namespace`, the collection contains zero or one item for that namespace
- `prefix`, `contains`, and `contains_nocase` are mutually compatible with `namespace`, address relation filters, and pagination; they are not availability checks
- `owner` is the token-holder / owner address filter and is equivalent to `account` with `relation=token_holder`; supplying both `owner` and `account` returns `400 invalid_input`
- `registrant` filters the projected registrant relation directly
- `account` filters by the requested `relation`; `relation=any` returns the union of token-holder, registrant, and effective-controller matches and dedupes by `(namespace, normalized_name)`
- `resolver` filters by the current declared resolver address where the exact-name resolver summary is projected
- `resolved_address` is supported only when the implementation has a declared, replay-stable record-value equality projection for the requested namespace and selector family; otherwise the route returns a non-2xx `unsupported` error for that filter rather than scanning raw facts or verified execution output
- `sort=expiry_date`, `sort=registration_date`, and `sort=created_at` sort by projected timestamp fields; rows with `null` sort values are ordered after non-null values for `asc` and before non-null values for `desc`
- all sort orders break ties by `(namespace, normalized_name, namehash)`
- `include=record_summaries` may add compact record counts, known text-key hints, avatar/content-hash presence, and known coin-type hints from declared inventory/cache; it must not run verified execution
- `include=total_count` asks the API to return `meta.total_count` for the filtered set before cursor slicing when the filter set is count-supported; unsupported count combinations leave `total_count=null` and add `total_count` to `meta.unsupported_fields`
- compact responses omit provenance, full coverage, internal projection metadata, `logical_name_id`, and `resource_id` by default
- `view=full` returns a full-envelope collection only after a later implementation documents the full item shape; until then `view=full` is reserved and returns `400 invalid_input`

### `GET /v1/coverage/{namespace}/{name}`

This route ships as the single-name coverage and explain surface.

Returns the declared-state coverage answer for one exact public surface.

`data` identifies the same single surface and current binding as `GET /v1/names/{namespace}/{name}`.

`declared_state` carries explain-oriented detail for that same single-name coverage answer.

Supported query parameters:

- `at`
- `chain_positions`
- `consistency`

Rules:

- this route honors only `at`, `chain_positions`, and `consistency` from the common query set; if `at` and `chain_positions` are both omitted, the common snapshot defaults apply and the route reads the latest available positions at `consistency=head` unless the caller supplies another supported `consistency`
- this route uses the exact-name snapshot selector and must select the same `data`, binding identifiers, `coverage`, provenance, and response `chain_positions` as `GET /v1/names/{namespace}/{name}` for the same request selector
- this route is declared-state only and `verified_state` is `null`
- the top-level `coverage` field is the shared `Coverage` object for the requested name and snapshot
- for `namespace=ens` under the selected ENSv2 `sepolia-dev` profile, the shared coverage object follows the same promoted exact-name profile rule as `GET /v1/names/{namespace}/{name}`: `status=full`, `exhaustiveness=authoritative`, `source_classes_considered=["ens_v2_registry_l1","ens_v2_registrar_l1"]`, `enumeration_basis=exact_name_profile`, and `unsupported_reason=null` for the admitted registry and registrar source classes (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)
- this coverage route does not report support for ENSv2 mainnet, reverse or primary-name reads, wrapper-derived authority, migration history, universal-resolver entrypoints, verified resolution, execution explain, or resolver-local sections outside the supported exact-name profile class
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
- `chain_positions`
- `consistency`

Rules:

- this route is scoped to the same exact-name target and exact-name snapshot selector as `GET /v1/names/{namespace}/{name}`
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
- `chain_positions`
- `consistency`

Rules:

- this route is scoped to the same exact-name target and exact-name snapshot selector as `GET /v1/names/{namespace}/{name}`
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
- for `namespace=basenames`, address-name membership and relation facets derive from the admitted Base authority split rather than reverse-claim or transport state; `basenames_base_primary`, `basenames_l1_compat`, and `basenames_execution` do not add rows or widen relation semantics on this route because upstream separates Base-side name ownership / resolver state from reverse claims and from the Ethereum Mainnet `L1Resolver` transport (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
- the `role_summary` expansion derives from the current item `resource_id` plus the existing resource-permissions truth family; it does not introduce a separate address-role ledger
- `role_summary` groups the current `GET /v1/resources/{resource_id}/permissions` rows by `subject`; each grouped subject keeps the current `(scope, effective_powers)` pairs for that `resource_id`, while row-granular grant and revocation detail stays on the dedicated permissions route
- `subname_count` counts the same declared direct child surfaces returned by `GET /v1/names/{namespace}/{name}/children` by default; it does not include linked, alias-derived, or wildcard-observed child buckets
- `status` and `expiry` mirror the current `ControlVector.status` and `ControlVector.expiry` values for the item `resource_id`
- `record_count` counts the distinct stable declared record selectors for the item `resource_id` at its current version boundary; in the first shipped slice this is the number of selectors that belong to the same declared record-inventory answer shape used by `Resolution.record_inventory`, not a count of raw resolver slots, cached values, or verified query results
- the added fields `role_summary`, `subname_count`, `record_count`, `status`, and `expiry` are optional expansion fields only and do not replace the required surface identity and relation facets

### `GET /v1/addresses/{address}/names/count`

This shipped route returns the count used by app dashboard owned-name lists and registrant lists. It is the count-only companion to address relation filters on `GET /v1/names`.

Supported query parameters:

- `namespace`
- `relation=token_holder|registrant|effective_controller|any`
- `prefix`
- `contains`
- `contains_nocase`
- `resolver`

Returns:

```json
{
  "data": {
    "address": "0x0000000000000000000000000000000000000000",
    "namespace": "ens",
    "relation": "token_holder",
    "count": 0
  },
  "meta": {
    "support_status": "partial",
    "unsupported_filters": []
  }
}
```

Rules:

- `relation=any` counts the deduped union of token-holder, registrant, and effective-controller matches for the address
- count filters use the same support and unsupported semantics as `GET /v1/names`; unsupported filters return a non-2xx `unsupported` error rather than scanning raw facts
- `count` is computed over the filtered set before any cursor pagination that would be applied by `GET /v1/names`
- this route does not expose item rows, provenance, coverage detail, or projection internals by default

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
- `effective_powers` is the server-computed, post-scope-modifier result. Clients must not apply NameWrapper fuse masks themselves or treat raw ownership / grant evidence as an unmasked permission.
- resolver-scoped permissions remain rows in this same collection with resolver-specific scope detail; they are not a separate truth system
- For ENSv1 wrapper-backed resources, current NameWrapper fuses are folded into `effective_powers`: a burned fuse removes any public power that depends on the prohibited wrapper operation, and a subject/scope row whose powers are all masked is omitted from the collection. Upstream exposes wrapper fuse values in `NameWrapped` and `FusesSet`, and gates wrapper operations such as resolver-target mutation, transfer, unwrap, subname creation, TTL mutation, fuse burning, and approval through those fuse bits (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L31 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L421 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L427 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L647 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L666 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L669 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L679 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L723 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L827 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1023 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L132 @ ens_v1@91c966f).
- A wrapper-backed permission answer may be `full` only when the current fuse modifier for the selected resource snapshot has been applied. If the projection cannot prove the current fuse state, the API must fail closed rather than return unmasked powers with full coverage.
- `GET /v1/addresses/{address}/names?include=role_summary` is the per-resource summary form of this same collection: it groups current rows by `subject`, retains each grouped subject's `scope` plus `effective_powers`, and leaves row-granular lineage on this dedicated route
- `cursor` and `page_size` page over the frozen `subject_scope_asc` order only; they do not alter row shape, supported filters, or route-level coverage meaning
- this route is declared-state only and `verified_state` remains `null`

### `GET /v1/resources/lookup`

This shipped route resolves app-facing name identity to the current opaque resource identity used by role and permission routes.

Supported query parameters:

- `namespace`
- `name`
- `view=compact|full`
- `meta=none|summary|full`

Returns:

```json
{
  "data": {
    "namespace": "ens",
    "name": "alice.eth",
    "normalized_name": "alice.eth",
    "resource_id": "00000000-0000-0000-0000-000000000000",
    "resource_hex": null
  }
}
```

Rules:

- `namespace` and `name` are required
- `resource_id` is opaque and is the stable API key for resource-scoped roles and permissions
- `resource_hex` is deferred unless an existing stable projected field is explicitly documented for the namespace; callers must not derive `resource_hex` from `resource_id`, `namehash`, token ID, or calldata
- the route reads the same current exact-name projection as `GET /v1/names/{namespace}/{name}` and must not synthesize identities from raw facts

### `GET /v1/roles`

This shipped route returns compact app-facing role rows by account, resource, or name lookup filters.

Supported query parameters:

- `account`
- `resource_id`
- `namespace`
- `name`
- `role_bitmap`
- `view=compact|full`
- `meta=none|summary|full`
- `cursor`
- `page_size`

Defaults:

- `view=compact`
- `meta=summary`
- default sort `account_resource_scope_asc`

Each compact item is `RoleRow`.

Rules:

- at least one of `account`, `resource_id`, or the pair `{namespace, name}` is required
- `{namespace, name}` first resolves through `GET /v1/resources/lookup` semantics, then reads current effective permission rows for that resource
- `account` filters by effective permission subject; it does not search owner, registrant, or address-name relation rows unless those subjects also exist in `permissions_current`
- `role_bitmap` filters only when the projection exposes `role_bitmap`; otherwise the route returns a non-2xx `unsupported` error for that filter
- `effective_powers` remains the API-owned post-scope effective power list; clients must not infer powers from `role_bitmap` alone
- `provenance` is compact section provenance; row-granular grant lineage remains on `GET /v1/resources/{resource_id}/permissions`

### `GET /v1/names/{namespace}/{name}/roles`

This shipped route is the name-scoped compact role collection for the current resource behind one exact name.

Supported query parameters:

- `account`
- `role_bitmap`
- `view=compact|full`
- `meta=none|summary|full`
- `cursor`
- `page_size`

Rules:

- the route resolves the current `resource_id` for `{namespace, name}` using the exact-name snapshot and returns `RoleRow` items for that resource
- if the name exists but role projection support for its current resource is unavailable, the compact response returns an empty `data` array only when the route can prove there are no current rows; otherwise it returns a non-2xx `unsupported` or `409 stale` according to the shared error model
- `resource_hex` follows the same nullable/deferred rule as `GET /v1/resources/lookup`
- this route is a compact view of `GET /v1/resources/{resource_id}/permissions`, not a second permissions ledger

### `GET /v1/names/{namespace}/{name}/children`

Defaults to declared direct children only.

Optional query parameters:

- `surface_classes=declared`
- `include=counts`
- `view=compact|full`
- `meta=none|summary|full`
- `cursor`
- `page_size`

Each compact item includes:

- `name`
- `normalized_name`
- `label_name`
- `labelhash`
- `namehash`
- `owner`
- `registrant`
- `subname_count`

Rules:

- `view=compact` is the app-facing default for this route; noisy provenance and coverage detail stay suppressed unless requested through `meta=full`, `view=full`, or existing explain/audit routes
- `view=full` returns the existing full-envelope declared child collection shape for callers that need provenance, coverage, chain position context, or consistency metadata
- `name` is the child display name, `normalized_name` is the child normalized name, and `label_name` is the single child label relative to the requested parent
- `labelhash` appears when the child projection carries a stable label hash; otherwise it is `null`
- `owner` and `registrant` are included when projected for the child surface; missing projected values are `null` and do not imply route-level unsupported
- `include=counts` adds `subname_count` for each child where the direct-child count is projected; if a count is not projected, the field is `null` and `meta.unsupported_fields` includes `subname_count` unless `meta=none`
- requesting `linked`, `alias`, or `wildcard` surface classes is reserved for additive expansion and currently returns `unsupported`
- for `namespace=basenames`, declared direct child surfaces come from the admitted Base authority split only; `basenames_base_primary` claim intake, `basenames_l1_compat` transport, and `basenames_execution` verified-resolution support do not create child rows or widen supported `surface_classes` because upstream places `*.base.eth` subdomain registration on the Base registry / registrar stack while the reverse registrar and L1 resolver remain separate surfaces (upstream: .refs/basenames/README.md:L8 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
- `cursor` and `page_size` page over the frozen `display_name_asc` order only; they do not alter supported `surface_classes`, row shape, or coverage meaning

### `GET /v1/names/{namespace}/{name}/records`

This shipped route is the compact app-facing resolver record endpoint. It is a convenience read over current exact-name resolver summary projections, declared record inventory/cache projections, and optional verified selector execution. Unlike `GET /v1/resolutions/{namespace}/{name}`, this compact route does not select a later checkpoint and then prove projection catch-up by scanning normalized events. `GET /v1/resolve/{name}/records` is the namespace-inferred counterpart for callers that do not already have a namespace.

Supported query parameters:

- `mode=auto|declared|verified|both`
- `texts`
- `known_text_keys=true|false`
- `avatar=true|false`
- `content_hash=true|false`
- `coin_types`
- `include=resolver_address,known_text_keys,avatar,content_hash,coins`
- `view=compact|full`
- `meta=none|summary|full`

Defaults:

- `mode=declared`
- `view=compact`
- `meta=summary`
- `include=resolver_address`

Returns `CompactRecordSummary` in `data` for `view=compact`.

Rules:

- `resolver_address` is the current declared resolver target from exact-name state; `null` means no declared resolver at the selected snapshot, not a verified failure
- `texts` is a comma-separated list of requested text keys; requested text keys are returned in `text_records` from the selected compact value source
- `known_text_keys=true` returns the projected known text-key inventory; it is inventory/cache metadata and must not be represented as a verified enumeration
- `avatar=true` is a compact alias for requesting the `avatar` text key and may also populate the top-level `avatar` convenience field from declared cache
- `content_hash=true` requests the declared content-hash selector
- `coin_types` is a comma-separated list of textual coin-type selector keys; numeric selector domains stay textual on the wire
- in `mode=declared`, values come from `record_cache` and inventory comes from `record_inventory`; no live execution is performed
- in `mode=verified|both`, requested selectors follow the same supported verified-resolution boundary, selector ordering, and unsupported result semantics as `GET /v1/resolutions/{namespace}/{name}`; supported ENS cache misses may execute live through the configured API Ethereum RPC provider using the provider `latest` block tag for this compact UI route
- in `mode=auto`, an authoritative declared resolver profile uses local `record_inventory_current` / record cache values; observed ENSv1 PublicResolver-compatible text selectors may be worker-hydrated into that declared cache after rebuild. Otherwise supported requested selectors use verified resolution output, including non-persisted on-demand Universal Resolver execution with provider `latest` when no persisted exact-snapshot output exists
- when no declared record selectors are available and `mode=auto|verified|both` requests app-facing record sections, the compact route probes only the bounded basic app profile set (`addr:60`, `avatar`, `contenthash`, and text keys `description`, `url`, `email`); this is a convenience fallback, not verified record enumeration
- compact route on-demand `latest` calls return inline selector results and do not create exact-snapshot execution cache rows or exact block-anchored `raw_call_snapshots`; callers that need persisted exact-block provenance use `GET /v1/resolutions/{namespace}/{name}` and explain/audit routes
- selector-specific record history is not part of this route; callers use `GET /v1/events` or history routes with event-type filters, and selector-exact record history remains deferred until a projection-backed selector-history contract is added

### `GET /v1/history/names/{namespace}/{name}`

Returns canonical normalized-event history for one logical name anchor.

Supported query parameters:

- `scope=surface|resource|both` with default `both`
- `view=compact|full`
- `meta=none|summary|full`
- `cursor`
- `page_size`

Rules:

- `scope=surface` returns events anchored by the requested `logical_name_id`
- `scope=resource` returns events anchored by any `resource_id` ever bound to that surface
- `scope=both` returns the union of those anchor sets
- observed and orphaned events are excluded from the shipped history routes
- `view=compact` returns `CompactHistoryEvent` rows with default `meta=summary`; `view=full` returns the existing normalized-event history row shape
- `cursor` and `page_size` page over the frozen `chain_position_desc` order only; they do not alter row shape, scope semantics, or coverage meaning
- `declared_state` is `{}` for history routes; the normalized-event rows themselves are the declared answer

### `GET /v1/history/resources/{resource_id}`

Returns canonical normalized-event history for one resource anchor.

Supported query parameters:

- `scope=surface|resource|both` with default `both`
- `view=compact|full`
- `meta=none|summary|full`
- `cursor`
- `page_size`

Rules:

- `resource_id` must be a UUID or the route returns `400 invalid_input`
- `scope=resource` returns events anchored by the requested `resource_id`
- `scope=surface` returns events anchored by any `logical_name_id` ever bound to that resource
- `scope=both` returns the union of those anchor sets
- observed and orphaned events are excluded from the shipped history routes
- `view=compact` returns `CompactHistoryEvent` rows with default `meta=summary`; `view=full` returns the existing normalized-event history row shape
- `cursor` and `page_size` page over the frozen `chain_position_desc` order only; they do not alter row shape, scope semantics, or coverage meaning
- `GET /v1/history/addresses/{address}` reuses these same anchor and coverage semantics rather than inventing a second history contract

### `GET /v1/history/addresses/{address}`

This route ships as the address activity history read.

Returns canonical normalized-event history for one address-derived anchor set.

Supported query parameters:

- `namespace`
- `relation=registrant|token_holder|effective_controller`
- `scope=surface|resource|both` with default `both`
- `view=compact|full`
- `meta=none|summary|full`
- `cursor`
- `page_size`

Rules:

- address history reuses the existing normalized-event history contract; it does not introduce a separate address-history ledger or projection family
- `namespace` and `relation` filter which related surfaces and resources contribute anchors for the requested address across current and historical matches; they do not change history row shape, ordering, or coverage meaning
- `scope=surface` returns events anchored by any `logical_name_id` selected for the requested address across current and historical matches under the active filters
- `scope=resource` returns events anchored by any `resource_id` selected for the requested address across current and historical matches under the active filters
- `scope=both` returns the union of those anchor sets
- observed and orphaned events are excluded from this route
- `view=compact` returns `CompactHistoryEvent` rows with default `meta=summary`; `view=full` returns the existing normalized-event history row shape
- this route follows the shared history default sort `chain_position_desc`
- `cursor` and `page_size` page over that frozen default sort only; they do not alter row shape, anchor semantics, or coverage meaning
- `declared_state` is `{}` for history routes; the normalized-event rows themselves are the declared answer

### `GET /v1/events`

This shipped route is the app-facing compact event search across name, address, resource, type, relation, and block range filters. It reuses the normalized-event history truth family and does not introduce a second event ledger.

Supported query parameters:

- `namespace`
- `name`
- `address`
- `resource`
- `resource_id`
- `type`
- `relation=token_holder|registrant|effective_controller|any`
- `from_block`
- `to_block`
- `view=compact|full`
- `meta=none|summary|full`
- `cursor`
- `page_size`

Defaults:

- `view=compact`
- `meta=summary`
- default sort `chain_position_desc`

Each compact row is `CompactHistoryEvent`.

Rules:

- `name` is interpreted with `namespace`; supplying `name` without `namespace` returns `400 invalid_input`
- `address` selects events whose projected surface or resource anchor is related to the address under the requested relation filter; it follows the same anchor selection as `GET /v1/history/addresses/{address}`
- `resource` and `resource_id` select events anchored to the requested opaque resource ID; supplying both returns `400 invalid_input`, and this route does not accept `resource_hex`
- `type` filters by normalized event type or route-owned compact type alias; unsupported type aliases return a non-2xx `unsupported` error
- `from_block` and `to_block` apply to the event's canonical chain position and must not force raw fact scans
- observed and orphaned events are excluded from the default app-facing route
- `view=full` returns the existing normalized-event history row shape only after that full shape is documented for this route; until then it is reserved and returns `400 invalid_input`

### `GET /v1/resolvers/{chain_id}/{resolver_address}`

This route ships as the resolver-overview read.

`data` identifies the resolver target. `declared_state` groups:

- current bindings: `ResolverOverviewBindingSummary | UnsupportedSummary`
- alias mappings: `ResolverOverviewBindingSummary | UnsupportedSummary`
- resolver-scoped permissions
- role-holder summary
- resolver event summary

Supported query parameters:

- none in the initial contract

Rules:

- resolver overview is declared-state only and `verified_state` remains `null`
- supported enumerable `declared_state.bindings` includes every current resolver-linked binding whose current resolver target matches the route target, regardless of `binding_kind`
- supported enumerable `declared_state.aliases` ships in the initial resolver-overview contract and reuses the same `{status, count, items}` summary envelope as `bindings`, but `items` is only the current `binding_kind=resolver_alias_path` subset of those same resolver-linked bindings
- `declared_state.aliases` is sourced from current resolver-linked bindings only; it does not enumerate historical alias rows or create a second alias ledger
- for enumerable resolver targets, when no current alias binding exists for the target resolver, `declared_state.aliases` returns `{status:"supported", count:0, items:[]}`
- for ENSv1 PublicResolver-generation targets admitted through the supported resolver-profile gate, `declared_state.bindings`, `declared_state.aliases`, and resolver event fan-in summaries return `UnsupportedSummary` with `unsupported_reason="resolver_binding_enumeration_not_projected"` rather than enumerating all names currently pointing at that shared resolver address; exact-name resolver state remains available on exact-name and resolution routes
- for ENSv1, supported resolver overview over a dynamically discovered target requires that target's resolver-profile state be `supported` for the requested summary family on an admitted ENS Labs PublicResolver-generation profile; a watched resolver with `pending` or `unsupported` profile state, or an admitted legacy generation without the requested summary family, returns explicit `UnsupportedSummary` sections rather than zero-count latest-PublicResolver summaries (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f)
- for Basenames, supported resolver overview over a dynamically discovered Base-side target requires that target's resolver-profile state be `L2Resolver`-compatible and `supported` for the requested summary family; a watched resolver with `pending` or `unsupported` profile state returns explicit `UnsupportedSummary` sections rather than zero-count supported summaries, and the ENSv1 PublicResolver-generation profile gate, Ethereum Mainnet `L1Resolver` transport, and offchain gateways do not satisfy this Base-side resolver-profile gate (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)
- counts for nodes, aliases, and role holders live inside those declared summaries rather than as a separate truth system
- any other declared summary that is not yet projected returns `UnsupportedSummary`

### `GET /v1/resolvers/{chain_id}/{resolver_address}/overview`

This shipped route is the compact app-facing resolver overview. It uses the same declared resolver target and `resolver_current` ownership boundary as `GET /v1/resolvers/{chain_id}/{resolver_address}`, but defaults to a compact DTO suitable for Explorer resolver pages.

Supported query parameters:

- `include=nodes,aliases,roles,events`
- `view=compact|full`
- `meta=none|summary|full`

Defaults:

- `view=compact`
- `meta=summary`
- `include=nodes,aliases,roles,events`

Returns `ResolverOverviewCompact` in `data` for `view=compact`.

Rules:

- `counts.nodes`, `counts.aliases`, `counts.role_holders`, and `counts.events` are present only when the corresponding section is projected; unsupported sections are named in `meta.unsupported_fields` unless `meta=none`
- `nodes` is the compact current-name binding list when resolver binding fan-in is projected; if resolver binding fan-in is not projected, `nodes` is `null` and `meta.unsupported_fields` includes `nodes` unless `meta=none`
- `aliases` is the compact current alias list when alias fan-in is projected; if alias fan-in is not projected, `aliases` is `null` and `meta.unsupported_fields` includes `aliases` unless `meta=none`
- `roles` is the compact role-holder list derived from resolver-scoped permission rows when projected; row-granular lineage remains on permissions routes
- `events` is a compact event list derived from canonical normalized events for the resolver target when projected; selector-specific record history remains deferred unless a later route documents selector-backed event filters
- unsupported projected fan-in returns explicit unsupported metadata and must not be rendered as a supported zero count
- `view=full` delegates to the existing `GET /v1/resolvers/{chain_id}/{resolver_address}` envelope when supported; otherwise it is reserved and returns `400 invalid_input`

### `GET /v1/resolutions/{namespace}/{name}`

This route ships one mixed declared+verified envelope for resolution reads.

This is the canonical namespaced resolution route. `namespace` remains part of the public resource identity and is the stable key for storage, execution, provenance, and cache semantics.

Supported query parameters:

- `at`
- `chain_positions`
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

- this route uses the exact-name snapshot selector for `data`, declared topology, declared record inventory/cache, route-level `coverage`, verified support checks, and verified execution target selection
- in `mode=verified|both`, persisted verified output is eligible only when its stored request chain positions exactly match the selected `chain_positions`; no verified selector may be satisfied from execution output produced for another snapshot
- when matching persisted output is absent for a supported ENS Universal Resolver selector, the route performs on-demand execution against the selected exact-name snapshot, persists the request-scoped trace and selector outcome, and returns that persisted outcome in the same response (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L44 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L52 @ ens_v1@91c966f)
- `topology`, `record_inventory`, and `record_cache` are always present as objects when `declared_state` is populated; any declared section that is not yet projected returns `UnsupportedSummary`
- callers must round-trip the surfaced `record_key` strings in `records`; `record_family` and `selector_key` are explanatory fields, not alternate request identity
- `record_inventory` defines the known record-selector space, explicit gaps, and the current version boundary for the requested surface; it does not imply global record enumeration
- `record_cache` is the declared last-known-value view over that same selector space and version boundary; it never implies that verified execution was run
- for ENSv1 and Basenames, a current resolver target in `topology.resolver_path` is not enough to claim complete `record_inventory`, `record_cache`, or resolver-overview support; retained resolver-local record events may produce selector-level `record_cache` successes, but complete family coverage still requires supported resolver-profile admission for the relevant record family (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L61 @ basenames@1809bbc)
- for ENSv1, the supported discovered-resolver profile set is ENS Labs PublicResolver-generation-compatible and per-generation; unknown dynamic resolvers keep explicit `pending` or `unsupported` profile state and cannot produce complete family or resolver-overview support from topology alone. Admitted legacy generations also produce explicit unsupported coverage for families they do not support, such as missing DNS record support, no VersionableResolver boundaries, no name-wrapper awareness, or no default coin-type fallback, instead of inheriting latest-only capabilities (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f)
- for Basenames, the supported discovered-resolver profile is Base-side `L2Resolver`-compatible only; unknown dynamic resolvers remain watched topology targets with explicit `pending` or `unsupported` profile state and cannot produce complete family or resolver-overview support from topology alone. This is independent of the ENSv1 PublicResolver-generation profile gate and does not promote Basenames L1 transport / execution or offchain-gateway support (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)
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
- unsupported selector families, unsupported verified path classes, or namespaces without a verified entrypoint return `200` with `verified_queries[*].status=unsupported`; they do not silently downgrade to declared cache values
- declared resolver-profile gaps such as `resolver_family_pending` remain visible in `declared_state.record_inventory` and `declared_state.record_cache`, but they do not by themselves suppress verified execution for an otherwise supported Universal Resolver path; in `mode=verified|both`, supported ENS selectors either read matching persisted execution output or execute on demand at the selected snapshot, then persist and return the outcome (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L44 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L52 @ ens_v1@91c966f)
- public verified resolution support is narrower than the full declared topology model: in the shipped Phase 7 slice, `namespace=ens` exact-surface direct-path requests are supported first, the already frozen alias-only non-direct class remains in scope, and the first additive wildcard-derived class is exact-surface ENS wildcard-derived paths
- for that support check, use the same declared topology snapshot that would populate `declared_state.topology` under `mode=declared|both`; a request is direct-path only when `resolver_path[0].logical_name_id` equals top-level `data.logical_name_id`, `wildcard.source=null` with `matched_labels=[]`, `alias.final_target=null` with `hops=[]`, and all `transport` fields are `null`
- the already frozen ENS alias-only non-direct support class is the exact-surface class where alias rewriting participates on that same declared topology snapshot, `alias.final_target` is non-`null` with `hops` non-empty, `wildcard.source=null` with `matched_labels=[]`, and all `transport` fields are `null`
- the first additive ENS wildcard-derived support class is the exact-surface class where `wildcard.source` is non-`null` with `matched_labels` non-empty, `resolver_path[0].logical_name_id` equals `wildcard.source.logical_name_id`, `alias.final_target=null` with `hops=[]`, `subregistry_path=[]`, and all `transport` fields are `null`
- ENS verified requests outside the direct-path, alias-only, and wildcard-derived classes, including other non-alias ancestor-selected paths, linked-subregistry ancestor-selected paths, any transport-assisted path, and any request whose persisted execution used CCIP-Read, remain deferred and return `200` with `verified_queries[*].status=unsupported` for every requested selector
- for `namespace=basenames`, public verified resolution is supported only for the exact-surface transport-assisted direct-path class: `resolver_path[0].logical_name_id` equals top-level `data.logical_name_id`, `wildcard.source=null` with `matched_labels=[]`, `alias.final_target=null` with `hops=[]`, `subregistry_path=[]`, `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, and `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"`; that keeps declared authority on Base while publishing the separate L1 compatibility hop in the same declared topology snapshot (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- that supported Basenames class includes CCIP-participating traces rather than selector-local `unsupported` because the upstream `L1Resolver` initiates `OffchainLookup` for non-`base.eth` requests and completes them through `resolveWithProof` (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)
- other `namespace=basenames` verified requests, including alias-participating, wildcard-derived, linked-subregistry, transport-free, or later offchain-gateway path classes, remain explicit `unsupported` until a later doc-first contract change broadens the slice; this first Basenames support class does not widen ENS support classes and keeps future gateway admission separate from the frozen Base-authority-plus-L1Resolver slice (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc)
- that supported Basenames verified-resolution class does not change the declared read plane: exact-name, address-name, and children reads remain on the separate Base-side declared contract above, and `basenames_base_primary` stays claim intake only (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
- supported verified queries that execute but do not produce a trustworthy answer return `status=execution_failed` with `failure_reason`
- when a supported ENS verified selector needs on-demand execution, the API Ethereum RPC provider is required to serve the selected Ethereum block; missing provider configuration or provider inability to serve that block returns `409 stale` with a configuration message and never falls back to `declared_state.record_cache`
- for `mode=verified` or `mode=both`, top-level `provenance` includes the request-scoped execution trace summary and each `verified_queries[*]` item may carry narrower provenance for the specific selector result
- the additive alias-only and wildcard-derived support classes, and the remaining support-boundary `unsupported` results, do not change the mixed route envelope, selector order, query parameters, the shared route-level `coverage` object, or the already-published machine-readable route shape in `docs/api-v1.openapi.json`
- deeper execution explanation stays on the shipped `GET /v1/explain/resolutions/{namespace}/{name}/execution` route; `GET /v1/resolutions/{namespace}/{name}` does not inline ordered step lists or a raw trace dump
- route-level `coverage` explains declared completeness for topology, inventory, and cache at the requested snapshot; per-selector verified misses or failures do not change that shared route-level `coverage` object by themselves

### `GET /v1/resolve/{name}`

This route is a namespace-inferred convenience alias for the canonical resolution contract.

Supported query parameters:

- `mode=declared|verified|both`
- `records`

The shipped route currently exposes only `mode` and `records`. It returns the same `ResolutionResponse` envelope as `GET /v1/resolutions/{namespace}/{name}` after namespace inference. It does not define a second response shape, a second selector vocabulary, or a second execution truth system.

Namespace inference runs on the normalized `{name}` path value:

- exact `base.eth` resolves as `namespace=ens`
- names matching `*.base.eth` resolve as `namespace=basenames`
- other supported ENS names resolve as `namespace=ens`

Rules:

- `GET /v1/resolutions/{namespace}/{name}` remains the canonical route; clients that already know the namespace should use it, and all persisted identity continues to be namespaced
- the response must surface the inferred namespace through the existing identity fields, including `data.namespace` and `data.logical_name_id`
- after inference, `mode`, `records`, declared-state, verified-state, provenance, coverage, and error semantics follow the canonical route for the inferred `{namespace, name}` tuple within this shipped query surface
- because this route does not expose `at`, `chain_positions`, or `consistency`, the inferred canonical tuple uses the default exact-name snapshot: `consistency=head` at the latest stored checkpoint, and any supported ENS on-demand execution targets that selected chain position
- selector identity is namespace-local after inference: for `*.base.eth`, `records` is interpreted against the inferred `basenames` record selector space, not the ENS selector space
- namespace inference and verified support are separate gates; `*.base.eth` requests do not fall back to `namespace=ens` outside the Basenames exact-surface transport-assisted direct-path support class
- for inferred `namespace=basenames`, `mode=verified|both` uses the same Basenames verified-support boundary frozen for the canonical route above; outside the exact-surface transport-assisted direct-path class, each requested verified selector returns `status=unsupported`
- exact `base.eth` follows the inferred `namespace=ens` canonical route and therefore uses ENS support and selector semantics
- inferred ENS requests in `mode=verified|both` share the canonical route's cache-or-live-execute behavior for supported verified-resolution selectors and the same fail-closed API Ethereum RPC provider requirement

### `GET /v1/resolve/{name}/records`

This route is the namespace-inferred compact records convenience endpoint. Unlike the namespaced endpoint, its default mode is `auto`: if the current resolver profile is authoritative, values come from local declared record inventory/cache, including worker-hydrated ENSv1 PublicResolver text values for observed selectors when that post-rebuild step has run; otherwise supported requested selectors use verified resolution output from persisted execution or on-demand Universal Resolver execution. The route is a current-projection read: it must not perform normalized-event catch-up scans to prove a newer chain checkpoint before returning compact UI records.

Supported query parameters:

- `mode=auto|declared|verified|both`
- `texts`
- `known_text_keys=true|false`
- `avatar=true|false`
- `content_hash=true|false`
- `coin_types`
- `include=resolver_address,known_text_keys,avatar,content_hash,coins`
- `view=compact|full`
- `meta=none|summary|full`

Defaults:

- `mode=auto`
- `view=compact`
- `meta=summary`
- `include=resolver_address,known_text_keys,avatar,content_hash,coins`

Rules:

- namespace inference is identical to `GET /v1/resolve/{name}`: exact `base.eth` uses `namespace=ens`, names matching `*.base.eth` use `namespace=basenames`, and other supported ENS names use `namespace=ens`
- after inference, the route returns the same `CompactRecordSummary` contract and verified support boundary as `GET /v1/names/{namespace}/{name}/records`; its `mode=auto` default also turns on the common app-facing sections so one request can return resolver address, known text-key inventory, avatar, content hash, and known coin-address records when those selectors are available
- if the inferred current name or wildcard source has no declared record selectors, the default `mode=auto` read probes the bounded basic app profile set and returns successful fallback text rows plus the ETH coin row when available; it does not claim `known_text_keys` inventory support from those verified probes
- this convenience route does not expose `at`, `chain_positions`, or `consistency`; the inferred canonical tuple uses current exact-name and record-inventory projections rather than the full route's selectable snapshot join
- supported ENS verified fallback for this convenience route uses the provider `latest` block tag and returns non-persisted selector results inline; it does not create exact-snapshot execution cache rows or exact block-anchored `raw_call_snapshots`
- persisted identity, support status, unsupported fields, and error messages remain namespace-local after inference; the route must not fall back from Basenames to ENS when the inferred Basenames tuple is missing or unsupported

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
- the shipped public explain surface follows the same verified-resolution support boundary as the mixed route: in Phase 7, persisted ENS exact-surface direct-path verified answers are in scope first, the already frozen alias-only non-direct class remains in scope, and the first additive wildcard-derived class is persisted ENS exact-surface wildcard-derived paths under that same envelope
- `verified_state.verified_queries` reuses the same selector-scoped result objects, request order, and `ResultStatus` subset as the mixed resolution route
- `verified_state.execution.execution_trace_id` must equal top-level `provenance.execution_trace_id`
- top-level `provenance` anchors the response to the persisted execution trace, and any `verified_queries[*].provenance` objects must stay within that same `execution_trace_id` rather than creating a second provenance system
- `verified_state.execution.resolver_discovery_path`, `wildcard`, and `alias` explain the runtime path selected for that persisted trace; they do not widen declared topology into a second truth model
- the first additive ENS wildcard-derived support class uses the same exact-surface predicate as the mixed route: `wildcard.source` is non-`null` with `matched_labels` non-empty, `resolver_path[0].logical_name_id` equals `wildcard.source.logical_name_id`, `alias.final_target=null` with `hops=[]`, `subregistry_path=[]`, and all `transport` fields are `null`
- `verified_state.execution.steps` is the ordered persisted step summary for the trace and must not be treated as raw calldata, raw gateway payloads, or a replayable execution dump
- `namespace=basenames` execution-explain support is limited to the same exact-surface transport-assisted direct-path class as the mixed route: `resolver_path[0].logical_name_id` equals top-level `data.logical_name_id`, `wildcard.source=null` with `matched_labels=[]`, `alias.final_target=null` with `hops=[]`, `subregistry_path=[]`, `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, and `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- that supported Basenames explain class includes persisted CCIP-Read steps rather than route-level `unsupported` because the upstream `L1Resolver` uses `OffchainLookup` for non-`base.eth` requests and verifies the callback through `resolveWithProof` (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)
- ENS paths outside the direct-path, alias-only, and wildcard-derived classes, and Basenames paths outside that frozen transport-assisted direct class, remain outside the shipped public explain surface until a later doc-first contract change broadens verified support; this route does not synthesize trace-shaped `unsupported` responses from declared topology or bootstrap execution scaffolding (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc)
- for `namespace=basenames`, that frozen explain-support target applies to execution explain only; the separate declared exact-name explain routes remain on the Base-side declared read plane (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
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

- the shipped route is head-only; it does not honor `at` or `consistency`, and additive snapshot-selector support remains pending
- for ENS on Ethereum Mainnet, exact-tuple declared `claimed_primary_name` handling is keyed by the exact `primary_names_current(address, coin_type, namespace)` row for the requested tuple; the route does not trigger a fresh reverse-claim lookup while serving that declared status-shaped response
- for ENS on Ethereum Mainnet, the admitted claim state behind that row remains reverse-only through `ens_v1_reverse_l1` and contract role `reverse_registrar` at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb` (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L19 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f)
- for Basenames on the shipped mainnet profile, the admitted primary-claim family is `basenames_base_primary` through contract role `reverse_registrar` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282`; it remains claim intake only and does not replace the Base registry / registrar / resolver families as the declared truth for exact-name, address-name, or children reads because upstream exposes reverse-name claims through the dedicated ReverseRegistrar rather than the Base authority stack (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
- missing or unsupported ENS reverse claims do not trigger fallback to registry-, resolver-, or other claim-setting surfaces in the current contract; the admitted ENS claim source is the reverse registrar tuple only (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
- any fallback beyond that reverse-only ENS claim surface remains deferred and requires a later doc-first contract update; manifest presence alone does not widen the shipped precedence rule (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
- in Phase 7, that reverse-only ENS claim precedence and exact-tuple projection readback do not combine with resolver-backed name data or verification-derived identity to populate richer `claimed_primary_name` fields (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
- `claimed_primary_name` is the declared claim candidate only; it never implies that the requested address actually verifies to that name
- `claimed_primary_name.status` uses the shared `ResultStatus` vocabulary; the initial declared contract uses `success`, `not_found`, `unsupported`, and `invalid_name`
- `verified_primary_name.status` uses the same `ResultStatus` vocabulary; the initial verified contract uses `success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, and `execution_failed`
- `claimed_primary_name` and `verified_primary_name` always include `status` when their containing section is populated
- the declared `claimed_primary_name` contract stays exact-tuple and claim-local: `status` is always present, the admitted claimed-local fields beyond bare status are exact-tuple declared `claimed_primary_name.name`, exact-tuple declared `claimed_primary_name.provenance`, and `claimed_primary_name.raw_claim_name` for exact-tuple `status=invalid_name`
- when `claimed_primary_name.status=invalid_name` for the exact requested `(address, namespace, coin_type)` tuple, the route may publish `claimed_primary_name.raw_claim_name`; when present, it is copied verbatim from `primary_names_current.raw_claim_name` for that same tuple and must not be synthesized from normalized name identity, resolver-backed name data, verification-derived identity, or a different tuple's stored claim text
- blank or whitespace-only raw claim names are declared `not_found`; `invalid_name` is limited to nonblank raw claim names that fail normalization
- for every other declared status, and for every tuple other than the exact requested `(address, namespace, coin_type)`, the route does not publish `claimed_primary_name.raw_claim_name`
- `claimed_primary_name.provenance` is the first public claim-local section provenance on this route: it reuses `Provenance` as exact-tuple declared provenance from the requested `primary_names_current(address, coin_type, namespace)` row, stays limited to that row's claim-side inputs for the requested tuple, and must not be synthesized from fallback claim sources, resolver-backed name data, verification-derived identity, or a different tuple
- `claimed_primary_name.provenance` must strip any `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material before publication and must omit `execution_trace_id`
- `claimed_primary_name.name`, when present, comes only from the exact requested `primary_names_current(address, coin_type, namespace)` row's declared normalized claim-identity source for that same tuple, aligned with the currently admitted reverse-only claim precedence (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
- the route must not synthesize or backfill `claimed_primary_name.name` from manifest presence, resolver-backed name data, verified execution identity, tuple presence alone, a different tuple's stored identity, or any fallback claim source
- `verified_primary_name` is frozen to the shipped mixed-route field boundary `{status, name?, unsupported_reason?, failure_reason?, provenance?}`; `name`, when present on persisted verified-primary readback, uses the shared `NameRef` shape, `raw_claim_name` never appears on this execution-derived object, and `verified_primary_name.provenance`, when present, is the shipped section-local verification object `{execution_trace_id, manifest_versions}` for that same exact requested tuple
- `verified_primary_name.provenance` is a strict verification-local refinement under the same top-level `provenance.execution_trace_id`: `verified_primary_name.provenance.execution_trace_id` must equal top-level `provenance.execution_trace_id`, and `verified_primary_name.provenance.manifest_versions` must narrow that same persisted verification trace rather than widen it
- `verified_primary_name.provenance` must not publish `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material, restate `claimed_primary_name.provenance`, introduce a second lookup / invalidation identity for the tuple, or publish other `Provenance` fields at this section-local boundary
- in the shipped Phase 7 bootstrap slice, richer ENS tuple-present claimed payloads remain tightly bounded: reverse tuple presence or verification establishing a concrete normalized name target do not by themselves populate `claimed_primary_name.name` beyond that exact requested row's declared normalized claim-identity source, widen `claimed_primary_name.provenance` beyond exact-tuple declared row provenance, or widen the exact-tuple `status=invalid_name` gate for `claimed_primary_name.raw_claim_name` (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
- the exact-tuple `verified_primary_name` support class is limited to persisted readback for the same requested tuple and stable execution identity `request_type=verified_primary_name`; the shipped ENS slice and the frozen first Basenames slice both use it, and this mixed route does not become a fresh execution trigger
- for that exact-tuple persisted-readback support class, the verified execution `request key` identity is the exact normalized route tuple `{namespace}:{normalized_address}:{coin_type}`, where `normalized_address` uses the same lowercase normalization as the route lookup; claimed text, normalized name identity, verified target address, result status, and section-local provenance do not participate in that key
- `primary_names_current(address, coin_type, namespace)` is the only admitted claim-side lookup / invalidation anchor for the current exact-tuple declared handling and for the verified tuple; persisted claim-local inputs there may publish `claimed_primary_name.name` only through that exact requested row's declared normalized claim-identity source, and they still do not admit fallback-expanded claim sources or publish `verified_primary_name` fields beyond the separately frozen verified readback slice
- in the exact-tuple persisted-readback support class, `verified_primary_name.name` appears only for `status=success` or `status=mismatch`, where the route established a concrete normalized name target for verification
- `claimed_primary_name.name` remains distinct from execution-derived `verified_primary_name.name`; this clarification does not change when `verified_primary_name.name` appears, and it does not by itself widen the exact-tuple primary-name coverage contract below
- for `namespace=basenames`, claim intake does not collapse `claimed_primary_name` and `verified_primary_name` into one truth system: declared claim state stays route-local and claim-local, while verified state stays execution-local and must not be backfilled from Base authority reads because upstream keeps reverse-name writes on the dedicated ReverseRegistrar while verified resolution enters through the separate Ethereum Mainnet `L1Resolver` (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
- for `namespace=basenames`, the shipped `GET /v1/primary-names/{address}` readback now returns persisted `verified_primary_name` results for the exact requested `(address, namespace, coin_type)` tuple in `mode=verified|both`; it stays execution-derived under `basenames_execution`, uses the same `request_type=verified_primary_name` and exact tuple request-key identity `{namespace}:{normalized_address}:{coin_type}`, keeps `primary_names_current(address, coin_type, namespace)` as the only claim-side lookup / invalidation anchor, keeps `verified_primary_name.provenance` limited to `{execution_trace_id, manifest_versions}` under the same top-level `provenance.execution_trace_id`, and does not add a dedicated manifest capability flag; it participates in the same exact-tuple primary-name coverage contract below, while the declared / verified split remains required because upstream keeps reverse-name writes on the Base ReverseRegistrar while verified resolution enters through the separate Ethereum Mainnet `L1Resolver` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
- the mixed route continues to use the same already-published envelope in `docs/api-v1.openapi.json`; this claimed-primary clarification does not widen query parameters, add snapshot handling, or admit fallback claim sources, and route-level coverage is governed only by the exact-tuple persisted-readback rules below
- `verified_primary_name` is authoritative only when `status=success`
- `status=mismatch` applies only to `verified_primary_name` and remains reserved for the shipped persisted-readback verified result shape where the claim normalizes and resolves for the requested `coin_type`, but the verified target address does not equal the requested `{address}`
- `verified_primary_name.failure_reason` is verification-local and may appear only for `status=mismatch`, `status=invalid_name`, or `status=execution_failed`; it must not be used to restate declared claim identity or to duplicate `raw_claim_name`
- when the admitted declared claim cannot be normalized, `claimed_primary_name` may still return `status=invalid_name`; `raw_claim_name` is the only claim-local field admitted on the declared object in that case, while `verified_primary_name.status=invalid_name` remains limited to verification-local fields
- invalid address syntax, missing required `namespace` or `coin_type`, or a malformed query tuple returns `400 invalid_input`
- an unsupported public namespace returns `404 not_found`
- no declared or verified primary-name answer for the requested tuple returns `200` with `status=not_found`; it does not turn the route into `404`
- unsupported claim surfaces or unsupported verified entrypoints return `200` with the corresponding object `status=unsupported`; for `verified_primary_name`, that same bootstrap fallback still applies to tuple-present reads outside the current namespace-local exact-tuple persisted-readback support class
- top-level `provenance` summarizes the declared claim inputs and, when an execution-derived verified answer is present, the verification trace
- top-level `provenance` is the only response-wide join between claim-side inputs and any persisted verification trace; `claimed_primary_name.provenance` and `verified_primary_name.provenance`, when present, must remain strict refinements of that top-level identity rather than a second route-level truth system
- `claimed_primary_name.provenance` is exact-tuple declared provenance from the requested `primary_names_current(address, coin_type, namespace)` row; it must strip any `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material and must not include `execution_trace_id`
- `verified_primary_name.provenance`, when present, is verification-local provenance for that same exact tuple and is limited to `execution_trace_id` plus `manifest_versions`; `verified_primary_name.provenance.execution_trace_id` must equal top-level `provenance.execution_trace_id`, `verified_primary_name.provenance.manifest_versions` must narrow that same persisted verification trace, and it must not publish claim-side lookup / invalidation hook material or other `Provenance` fields
- route-level `coverage` for this route is local to the requested `(address, namespace, coin_type)` tuple and is not the single-name `coverage_current` object used by `GET /v1/coverage/{namespace}/{name}`; it reports whether the route can serve the frozen primary-name readback class for that tuple, not address-wide or namespace-wide primary-name completeness
- for `namespace=ens` on the shipped Ethereum Mainnet profile, the frozen exact-tuple persisted-readback support class returns `coverage.status=partial`, `exhaustiveness=non_enumerable`, `source_classes_considered=["ens_v1_reverse_l1","ens_execution"]`, `enumeration_basis=primary_name_lookup`, and `unsupported_reason=null` (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)
- for `namespace=basenames` on the shipped mainnet profile, the frozen exact-tuple persisted-readback support class returns `coverage.status=partial`, `exhaustiveness=non_enumerable`, `source_classes_considered=["basenames_base_primary","basenames_execution"]`, `enumeration_basis=primary_name_lookup`, and `unsupported_reason=null`
- this partial coverage means only exact-tuple declared claim readback plus persisted verified-primary readback are frozen for the requested tuple; it does not admit fresh execution, fallback claim sources, address-wide primary-name enumeration, other deployment profiles, or external app parity
- requests against supported public namespaces whose tuple, deployment profile, or verified entrypoint falls outside the namespace-local exact-tuple persisted-readback support class return route-level `coverage.status=unsupported`, `exhaustiveness=not_applicable`, `source_classes_considered=[]`, `enumeration_basis=primary_name_lookup`, and `unsupported_reason="primary-name exact-tuple persisted readback is not supported for the requested tuple"`; out-of-class verified objects still use `verified_primary_name.status=unsupported`
- tuple presence, tuple absence, a verified mismatch, or resolver-backed verification detail does not by itself change these coverage states; support-class membership chooses route-level coverage, while result-object `status` describes the tuple answer inside that class

### App-Facing Request Examples

Dashboard owned names:

`GET /v1/names?namespace=ens&account=0x0000000000000000000000000000000000000000&relation=token_holder&contains=ali&sort=expiry_date&order=asc&page_size=50`

Name search suggestions:

`GET /v1/names?namespace=ens&prefix=alic&sort=name&order=asc&page_size=10&meta=none`

Exact compact profile lookup:

`GET /v1/names?namespace=ens&name=alice.eth&include=record_summaries`

Subnames list:

`GET /v1/names/ens/alice.eth/children?include=counts&page_size=50`

Resolver records:

`GET /v1/resolve/alice.eth/records`

Targeted resolver records:

`GET /v1/resolve/alice.eth/records?include=resolver_address,known_text_keys,avatar,content_hash,coins&texts=avatar,com.twitter&coin_types=60,0`

Name history:

`GET /v1/history/names/ens/alice.eth?view=compact&scope=both&page_size=25`

Address history:

`GET /v1/events?address=0x0000000000000000000000000000000000000000&relation=any&namespace=ens&page_size=25`

Roles by account:

`GET /v1/roles?account=0x0000000000000000000000000000000000000000&page_size=50`

Roles by resource:

`GET /v1/roles?resource_id=00000000-0000-0000-0000-000000000000&page_size=50`

Roles by name:

`GET /v1/names/ens/alice.eth/roles?page_size=50`

Resolver overview:

`GET /v1/resolvers/ethereum-mainnet/0x0000000000000000000000000000000000000000/overview?include=nodes,aliases,roles,events`

### Apps-Monorepo Indexer Coverage Report

| Area | Coverage class | Route behavior |
| --- | --- | --- |
| exact full profile lookup | covered by existing routes | `GET /v1/names/{namespace}/{name}` remains the canonical full-envelope profile route with provenance, coverage, chain position context, and declared summary sections |
| full resolution inventory/cache and verified selectors | covered by existing routes | `GET /v1/resolutions/{namespace}/{name}` remains the full mixed declared+verified route; compact record reads do not replace its full envelope |
| direct child collections, address-name collections, resource permissions, resolver overview, and primary-name exact tuple | covered by existing routes | existing shipped routes remain the full-envelope source of truth for those capabilities |
| exact compact profile lookup | covered by implemented compact route | `GET /v1/names?namespace=...&name=...` returns `CompactDomainSummary`; canonical full detail remains on `GET /v1/names/{namespace}/{name}` |
| dashboard owned names and owner / registrant / effective-controller lists | covered by implemented compact route over existing address relation projections | `GET /v1/names` supports `owner`, `account`, `registrant`, and `relation`; `GET /v1/addresses/{address}/names/count` provides count-only readback |
| name search suggestions | covered by implemented compact route | `prefix`, `contains`, and `contains_nocase` are search filters, not availability or pricing checks |
| resolver records | covered by implemented compact records routes plus existing resolution route | `GET /v1/resolve/{name}/records` infers namespace for app callers and defaults to `mode=auto`, choosing authoritative declared cache or verified execution for requested supported selectors; `GET /v1/names/{namespace}/{name}/records` remains available when namespace is already known and defaults to declared cache |
| child rows and counts | covered by expanded children route | compact child rows include label/name identity, owner/registrant where projected, and direct-child counts where projected; `view=full` keeps the existing full-envelope route shape |
| name/address history and compact event search | covered by expanded history routes and implemented events route | history routes support `view=compact`, and `GET /v1/events` exposes compact canonical events without raw normalized-event payloads |
| roles by account/resource/name | covered by implemented compact roles routes over permissions | `RoleRow` exposes `resource_id`, nullable `resource_hex`, `role_bitmap` where projected, and `effective_powers` |
| resolver overview | covered by implemented compact overview route where `resolver_current` has sections | unsupported resolver fan-in sections are explicit and are not represented as zero counts |
| resolved-address listing | deferred due projection gap | `resolved_address` is accepted only where a declared record-value equality projection exists; otherwise the filter is unsupported |
| `resource_hex` lookup | deferred unless a stable projected field is documented | `resource_hex` is nullable on role/resource lookup responses and must not be derived by clients |
| selector-specific record history beyond event filters | deferred due projection gap | event type filters are supported; selector-exact record history waits for a selector-history contract |
| linked, alias, and wildcard child buckets | deferred | `surface_classes=linked|alias|wildcard` stays unsupported until those buckets are projection-backed |
| unprojected resolver fan-in | deferred per resolver section | compact overview sections backed by unprojected fan-in return explicit unsupported metadata rather than supported zero counts |
| direct-chain and local app services | out of scope | favorites/local services, availability, pricing, direct contract workflows, DNSSEC, app images, faucet, and direct reverse checks are not part of this indexer coverage unless backed by projections |

## 6. Sorting And Pagination Defaults

- `GET /v1/names` defaults to `name_asc` and honors route-specific `sort`, `order`, `cursor`, and `page_size`
- `GET /v1/addresses/{address}/names` defaults to `display_name_asc` and honors replay-stable `cursor` and `page_size`
- `GET /v1/names/{namespace}/{name}/children` defaults to `display_name_asc` and honors replay-stable `cursor` and `page_size`
- `GET /v1/resources/{resource_id}/permissions` defaults to `subject_scope_asc` and honors replay-stable `cursor` and `page_size`
- `GET /v1/roles` defaults to `account_resource_scope_asc` and honors replay-stable `cursor` and `page_size`
- `GET /v1/names/{namespace}/{name}/roles` defaults to `account_scope_asc` and honors replay-stable `cursor` and `page_size`
- `GET /v1/history/names/{namespace}/{name}` defaults to `chain_position_desc` and honors replay-stable `cursor` and `page_size`
- `GET /v1/history/resources/{resource_id}` defaults to `chain_position_desc` and honors replay-stable `cursor` and `page_size`
- `GET /v1/history/addresses/{address}` defaults to `chain_position_desc` and honors replay-stable `cursor` and `page_size`
- `GET /v1/events` defaults to `chain_position_desc` and honors replay-stable `cursor` and `page_size`
- no other route honors `cursor` or `page_size` in this contract
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
- for snapshot selectors, malformed `chain_positions`, unsupported position slots, missing required position slots, mixed deployment-profile positions, and combined `at` plus `chain_positions` return `400 invalid_input`
- a syntactically valid selector whose supplied block hash, block number, canonicality floor, or cross-chain reconciliation cannot form one route snapshot returns `409 conflict`
- a syntactically valid selector that resolves to a coherent snapshot but cannot yet be served from the required projection rows returns `409 stale`
- persisted-readback routes or entrypoints that require matching execution output and cannot find it return the route's documented stale or not-found state; supported ENS verified resolution instead attempts on-demand execution, and returns `409 stale` with a configuration message when the API Ethereum RPC provider is not configured or cannot serve the selected block

## 8. Versioning Rules

- new optional fields are additive within `v1`
- new routes are additive within `v1`
- changing enum meaning, default sort, coverage semantics, or required fields requires `v2`
- if a capability is unsupported for a namespace or source class, return it explicitly in `coverage` or `error`, never through silent omission
