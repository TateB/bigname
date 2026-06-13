# API v1

The bigname public read contract. Wire format for the model defined in [`architecture.md`](architecture.md). Per-route reference in [`api-v1-routes.md`](api-v1-routes.md). Machine-readable contract in [`api-v1.openapi.json`](api-v1.openapi.json).

## Conventions

- All routes live under `/v1`.
- JSON, `snake_case` keys.
- Timestamps are RFC 3339 UTC.
- Semantic identities are strings; opaque internal IDs (UUIDs) are returned as-is and are never derived by clients.
- `namespace` is explicit on canonical name routes. Convenience routes that infer namespace still echo it back in identity fields.
- Path-segment names are ENSIP-15-normalized names, URL-encoded. Convenience routes that accept unnormalized names normalize them with the same `ensip15@ens-normalize-0.1.1` boundary used by adapters before namespace inference. Unnormalizable names reject as `invalid_input` or, on the identity façade, return `unnormalizable_input` where documented.

### Common query parameters

| Parameter | Meaning |
| --- | --- |
| `at` | Point-in-time selector: RFC 3339 timestamp or chain-position token. |
| `chain_positions` | Explicit selector: one URL value containing a JSON object with the same shape as `ChainPositions`. |
| `consistency` | `head`, `safe`, or `finalized`. |
| `mode` | `declared`, `verified`, or `both`. |
| `include` | Comma-separated route-specific expansions. |
| `view` | `compact` or `full`. |
| `meta` | `none`, `summary`, or `full`. |
| `sort` | Route-specific stable sort key. |
| `order` | `asc` or `desc`. |
| `cursor` | Opaque pagination cursor. |
| `page_size` | Default `50`, max `200`. |

Each route documents the subset it honors. Defaults: `consistency=head`, `mode=declared`, no `at` and no `chain_positions` selects the latest stored checkpoint per required chain. On-demand verified execution targets the same selected positions, never a provider's newer head.

## Snapshot selection

Snapshot selection resolves caller input to one `ChainPositions` object before any route-specific read. The selected object is echoed in the response.

| Inputs | Result |
| --- | --- |
| `chain_positions` only | use them exactly |
| `at` only | resolve per-chain positions at `consistency` |
| neither | latest available positions at `consistency` |
| both | reject with `invalid_input` |

Validation:

- Every chain required by the route must appear in `chain_positions` and use a position slot the route accepts.
- Malformed input, duplicate slots, mixed deployment profiles, or a `chain_id` that doesn't match the active profile rejects with `invalid_input`.
- Positions that don't satisfy the requested `consistency` floor return `conflict`.
- A `(chain_id, block_number, block_hash)` that isn't on stored canonical lineage, or that can't be reconciled across chains as one snapshot, returns `conflict`.
- A coherent selector whose required projection rows aren't built yet returns `stale` rather than reading raw facts.
- Persisted-readback routes return `stale` or `not_found` when matching output is absent. The exception is supported ENS verified selectors on `GET /v1/profiles/names/{name}` and `GET /v1/names/{namespace}/{name}/records`, which may execute on demand against the selected snapshot, persist the outcome, and return it. The profile route has no caller-selected `records` parameter: it selects profile records server-side from declared inventory selectors, explicit gaps, and record-cache entries that parse into bigname's public selector grammar. It falls back to the bounded app profile set only when a supported declared inventory exists but its public selector set is empty after non-public selector facts are omitted; missing, stale, or explicitly unsupported inventory does not use fallback records.
- A current-state row may serve a later selected snapshot only when its stored chain context covers the same required chains and no newer canonical input exists for that row through the selected positions; otherwise `stale`.
- Historical `at` and explicit `chain_positions` reads require projection or execution rows materialized for the exact selected positions. If rewind/rebuild has not produced that snapshot, the route returns `stale`; it never serves provider `latest`, raw facts, or newer current rows as a substitute.

Cross-chain rules:

- ENS authoritative positions are selected on `ethereum-mainnet` (mainnet profile).
- Basenames authoritative positions are selected on `base-mainnet` because upstream deploys the Basenames stack on Base.[^bn-readme-l70]
- An auxiliary chain position is chosen at the same `consistency` with timestamp at or before the authoritative-chain timestamp.
- Verified execution does not advance positions mid-request.

Deployment profiles:

- One runtime serves one profile at a time. Responses and explicit `chain_positions` must not mix mainnet and Sepolia chain keys.
- The ENSv2 `sepolia-dev` profile, when selected, supports declared exact-name profile reads against the admitted `ETHRegistry` and `ETHRegistrar` deployments[^v2-deploy-ethreg][^v2-deploy-ethrc]. It does not enable mainnet, reverse/primary, wrapper authority, migration, Universal Resolver, verified resolution, or execution-explain surfaces.

### Exact-name snapshot

These routes share the exact-name snapshot:

- `GET /v1/names/{namespace}/{name}`
- `GET /v1/coverage/{namespace}/{name}`
- `GET /v1/explain/names/{namespace}/{name}/surface-binding`
- `GET /v1/explain/names/{namespace}/{name}/authority-control`
- `GET /v1/profiles/names/{name}` (data, declared topology, inventory/cache, server-selected verified profile records, execution target)

Rules:

- Resolve `at`, `chain_positions`, and `consistency` once before any lookup, topology build, explain build, or execution.
- Every section in the response uses that one snapshot. Don't combine current binding from one snapshot with topology, inventory, or execution from another.
- A `name_current` or `record_inventory_current` row whose stored position predates the selected snapshot stays eligible only when no newer canonical input exists for the same `logical_name_id` or `resource_id` through the selected positions. Stored rows may carry auxiliary chain positions outside the selected snapshot; they do not make the row ineligible as long as every selected chain position is covered and fresh.
- `coverage` for `{namespace, name}` matches between `GET /v1/names/{namespace}/{name}` and `GET /v1/coverage/{namespace}/{name}`.
- The two explain routes resolve the same `logical_name_id`, `resource_id`, `token_lineage_id`, `surface_binding_id`, and `binding_kind` as the exact-name route at the same snapshot.
- `mode=verified|both`: persisted verified output joins only when its stored chain positions exactly match the selected snapshot. `GET /v1/profiles/names/{name}` executes every server-selected profile record derived from declared state; it never accepts a caller-supplied selector subset. If matching output is missing for a supported ENS selector, the route executes against the selected snapshot, persists the outcome, and returns it. Verified execution never advances positions mid-request.
- Without `at` or `chain_positions`, the snapshot is `consistency=head` at the latest stored checkpoint, and live execution targets that.
- Live ENS verified resolution requires an Ethereum RPC provider on the API process. If unconfigured or unable to serve the selected block, supported selectors return `409 stale` with a configuration message; declared cache is not a fallback.
- Handlers serve from projections and execution output. Raw facts and adapter-owned normalized events are never read directly.
- The slim API does not expose namespace-inferred resolution aliases; callers pass `{namespace}` explicitly on name routes.

## Response envelope

Full-envelope single-resource:

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

Full-envelope collection adds:

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

Compact envelope (app-facing routes):

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

Rules:

- `declared_state` and `verified_state` are always present in the full envelope. A route without one of those semantics returns `null` for that section.
- `mode=declared` populates `declared_state` and sets `verified_state=null`. `mode=verified` does the inverse. `mode=both` populates both.
- `coverage` is route-level completeness and enumeration basis, not freshness.
- `chain_positions` may carry multiple chains for cross-chain answers.
- Route-level `coverage` and per-section support are independent: a read may be authoritative while one declared section returns `UnsupportedSummary`.
- Top-level `provenance` is optional and reserved for explicit diagnostic/full metadata paths. Product routes omit it by default; mixed declared+verified routes may add section-local `provenance` where derivations differ.
- `meta=none` omits `meta` (collection `page` stays). `meta=summary` includes route-level support, unsupported filters/fields, count metadata, and snapshot summary. `meta=full` adds the full-envelope `coverage`, `chain_positions`, `consistency`, `last_updated`, and route-level `provenance` summaries where a compact route documents those diagnostics; compact names and role collections currently keep the same compact `meta` object as `meta=summary`.
- `GET /v1/profiles/names/{name}` is the app full-profile exception to the ordinary full-envelope default: `meta=summary` and `meta=none` return compact profile `data` without internal IDs or routine `normalized_name`, omit top-level coverage/chain/provenance fields, and strip per-query execution provenance. `meta=full` is required for diagnostic exact-name data and envelope metadata.
- `view=full` returns the full envelope only when the route documents a full view. Compact-only routes keep `view=full` as a reserved input that returns `400 invalid_input`; OpenAPI advertises only `view=compact` for those routes.
- Compact responses never expose raw facts, full provenance, or projection internals as a substitute for `meta`. Explain detail belongs on explain/audit routes.

## Identity

`POST /v1/identity:lookup` is the native slim app-facing route for partner-style indexed identity reads. It flattens existing current projections into native `IdentityRecord` DTOs, with result-level `input` and `normalization` metadata for name inputs. Native records use `name` as the canonical normalized name string and do not include routine `normalized_name` or `corrected_input_normalization` peers.

The identity route has three read profiles:

- Feed profile: `POST /v1/identity:lookup` with `profile=feed` returns at most one compact identity row per input address, plus `total_count`. It is the partner-1 latency path for feeds and timelines and intentionally excludes full `IdentityRecord` fields, record inventory, text records, coin-address maps, and deep provenance.
- Profile/detail profile: `POST /v1/identity:lookup` with `profile=detail` returns full native `IdentityRecord` rows for profile aggregation and account switchers. It may carry larger payloads and pagination.
- Shadow profile: `POST /v1/identity:lookup` with `profile=shadow` follows the detail response shape for deterministic migration comparison.

Identity lookup normalizes names before lookup. Identity reads are projection-backed by default and do not run live verified execution. Production ENSv2/L2 manifest admission remains a separate workstream; this route does not widen the documented ENSv2 `sepolia-dev` support boundary.

Identity not-found behavior is adapter-compatible rather than core-route `404` behavior:

- Name inputs return `record=null` with `status=not_found` for a miss.
- Unnormalizable name inputs return `status=unnormalizable_input`, `record=null`, and result-level `normalization.reason`.
- Address misses return `records=[]`, `status=success`, and `total_count=0`.
- Explicit `namespace=ens|basenames` filters are currently supported for name inputs only; address inputs use public namespace semantics and reject explicit namespace filters.
- The route preserves input order and returns one result object per input.
- Batch limits default to `1000` inputs and may be configured with `BIGNAME_API_IDENTITY_BATCH_LIMIT`.
- Address inputs default to `page_size=1` unless the input item asks for a larger page.

`IdentityRecord` includes canonical `name`, `namespace`, `namehash`, optional owner/manager/primary/record fields where projected, `network`, optional primary/relation facets for reverse reads, `status`, and `unsupported_fields`. Name result statuses can use `unnormalizable_input` before a record exists; nested `IdentityRecord.status` uses `success`, `not_found`, `unsupported`, or `stale`. Optional fields are omitted when no backed value is available. `unsupported_fields` lists fields that the identity route could not prove from the current projections without inventing a value.

Reverse identity pagination and feed results always include `total_count`. The count is read from the indexed `address_names_current_identity_counts` sidecar maintained with `address_names_current` and readable `name_current` eligibility, so the default feed path does not run an exact count scan and the count matches the reachable reverse page universe. The compact feed identity row is read from `address_names_current_identity_feed`, a sidecar that materializes the same readable first-row ordering per address, role set, and primary-name coin type.

## Shared objects

### `NameRef`

`logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `namehash`, `resource_id`, `binding_kind`.

### `ResourceRef`

`resource_id`, `authority_epoch`, `token_lineage_id`, `current_resolver`.

### `ChainPositions`

```
ethereum, base, execution_checkpoint
```

Each position: `chain_id`, `block_number`, `block_hash`, `timestamp`.

### `Provenance`

`normalized_event_ids`, `raw_fact_refs`, `manifest_versions`, `execution_trace_id`, `derivation_kind`. `execution_trace_id` appears only when execution-derived material participates. Declared-only provenance (including `claimed_primary_name.provenance`) omits it.

### `Coverage`

- `status`: `full`, `partial`, `observed_only`, `unsupported`, `stale`
- `exhaustiveness`: `authoritative`, `best_effort`, `observed_only`, `non_enumerable`, `not_applicable`
- `source_classes_considered`
- `enumeration_basis`
- `unsupported_reason`

For the same exact name and snapshot, `GET /v1/names/{namespace}/{name}` and `GET /v1/coverage/{namespace}/{name}` return the same `Coverage` object.

### `ResultStatus`

`success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, `execution_failed`. Used on `record_cache.entries`, `verified_queries`, `claimed_primary_name`, `verified_primary_name`. Each route documents the subset it uses.

Rules:

- `status` is always present.
- Request-identity fields stay present even when `status != success`.
- `unsupported_reason` is required when `status=unsupported`.
- `failure_reason` may appear on `not_found`, `mismatch`, `invalid_name`, or `execution_failed`.
- Concrete value/identity fields appear only when the route established a concrete answer.

### `UnsupportedSummary`

```
status: "unsupported", unsupported_reason
```

Used when a documented declared subdocument exists but isn't projected. The field stays present.

### `ExactNameControlSummary`

`registrant`, `registry_owner`, `latest_event_kind`. The narrow `declared_state.control` for one resource. Not a `ControlVector` dump or a permissions ledger. Keys stay present when supported; values may be `null` when the authority epoch doesn't expose that subject or no retained pointer exists.

### `ExactNameResolverSummary`

`chain_id`, `address`, `latest_event_kind`. Topology-only target identity. `chain_id=null, address=null` means no declared resolver, not unsupported.

For ENSv1, complete family coverage and resolver-overview support require admission to an ENS Labs PublicResolver-generation profile.[^v1-pres-l20][^v1-pres-l31][^v1-pres-l66][^v1-pres-l114] Retained generic resolver-local events may produce observed cache successes while profile state is `pending`. ENSv1 resolver `NameChanged` text observed via reverse/primary paths is preimage only — it doesn't make exact-name reads found, doesn't prove primary truth, and doesn't populate records without matching forward-node observations.[^v1-namechanged-l10][^v1-namechanged-l18][^v1-revreg-l129][^v1-revreg-l130]

For Basenames, complete family coverage requires a discovered Base resolver to be `L2Resolver`-compatible and admitted as `supported`.[^bn-l2resolver-l22][^bn-l2resolver-l182][^bn-l2resolver-l193] The ENSv1 profile gate, L1 transport, and offchain gateways don't satisfy this.

### `RoleSummary`

`subjects[*]` with `subject`, `scopes[*].scope`, `scopes[*].effective_powers`. Per-resource summary view of current effective permission rows. Row-granular lineage stays on `GET /v1/resources/{resource_id}/permissions`.

### `HistoryPointer`

`normalized_event_id`, `event_kind`, `chain_position`. Points at the first canonical row from the matching dedicated history route under default sort.

### `ExactNameHistorySummary`

`surface_head`, `resource_head`. Either may be `null` when the anchor set has no canonical rows. No `both_head` — use the dedicated history route with `scope=both` for that.

### `SurfaceBindingExplainSummary`

`surface_binding_id`, `binding_kind`. Identifies the current `SurfaceBinding` row matching the exact-name route's binding. `binding_kind` repeats so this thin view stands alone; `resource_id` and `token_lineage_id` remain on top-level `data`.

### `ResolutionResolverHop`

`logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `resource_id`, `chain_id`, `address`, `latest_event_kind`. Ordered from the contributing surface to the final resolver target.

### `VersionBoundary`

`logical_name_id`, `resource_id`, `normalized_event_id`, `event_kind`, `chain_position`. The surface and resource that last changed the boundary may differ from `data` when alias or wildcard selects an ancestor.

### `ResolutionTopology`

- `registry_path`: ordered `NameRef` array from the requested surface toward registry authority. Never empty when `topology` is supported.
- `subregistry_path`: ordered `NameRef` toward the nearest declared subregistry ancestor. Empty when none participates.
- `resolver_path`: ordered `ResolutionResolverHop` array. Last hop is the selected resolver.
- `wildcard`: `{source: NameRef|null, matched_labels: string[]}`. `null/[]` means wildcard didn't participate.
- `alias`: `{final_target: NameRef|null, hops: NameRef[]}`. `null/[]` means alias didn't participate.
- `version_boundaries`: `{topology_version_boundary, record_version_boundary}` — both `VersionBoundary`.
- `transport`: `{source_chain_id, target_chain_id, contract_address, latest_event_kind}`. All `null` means no transport. For Basenames, supported transport is `base-mainnet → ethereum-mainnet` through the L1 Resolver.[^bn-readme-l22][^bn-readme-l28][^bn-readme-l29][^bn-readme-l34][^bn-readme-l69][^bn-readme-l70]

`record_version_boundary` must equal `record_inventory.record_version_boundary` and `record_cache.record_version_boundary` when those sections are supported.

### `ResolutionRecordSelector`

`record_key`, `record_family`, `selector_key`, `cacheable`. `record_key` is the round-trip token used in `records`. `selector_key` is `null` for scalar families, a string otherwise. When non-null, `record_key = record_family + ":" + selector_key`. Numeric domains (coin types) stay textual on the wire; `addr:<coin_type>` selectors are canonicalized to unsigned 64-bit decimal text, so `addr:060` and `addr:60` address the same selector/cache identity. This is a bigname public-selector narrowing relative to upstream resolver `uint256 coinType` APIs `(upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L14 @ ens_v1@91c966f)` `(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L93 @ basenames@1809bbc)`; see `docs/upstream.md` § Known divergences.

### `ResolutionRecordGap`

`record_key`, `record_family`, `selector_key`, `gap_reason`. `selector_key=null` means the gap covers the scalar family key.

### `ResolutionUnsupportedRecordFamily`

`record_family`, `unsupported_reason`.

### `ResolutionRecordInventory`

- `record_version_boundary`: `VersionBoundary`
- `enumeration_basis`: `{observed_selectors, capability_declared_families, globally_enumerable}`
- `selectors`: `ResolutionRecordSelector[]`, sorted by `record_key` ascending
- `explicit_gaps`: `ResolutionRecordGap[]`, sorted by `record_key` ascending
- `unsupported_families`: `ResolutionUnsupportedRecordFamily[]`, sorted by `record_family` ascending
- `last_change`: `HistoryPointer | null`

May be authoritative for exact lookup while `globally_enumerable=false`. When `topology.resolver_path` ends in the explicit no-resolver hop, inventory is supported with empty selectors and requested `record_cache.entries[*]` return `status="not_found"` rather than unsupported.

For ENSv1 and Basenames, retained current-resolver record events may populate selectors and cache while resolver-profile admission is `pending`. Generic-topic collisions whose payload doesn't decode as the upstream event don't create selector facts. Unobserved selectors in a pending family surface explicit gaps or `unsupported_reason="resolver_family_pending"`. Admitted-`unsupported` profile state surfaces `unsupported_reason="resolver_family_unsupported"`.

### `ResolutionRecordCacheEntry`

`record_key`, `record_family`, `selector_key`, `status`, `value`, `unsupported_reason`. Declared cache uses `success`, `not_found`, `unsupported` only. `value` appears only on `success`, family-native JSON shape. `unsupported_reason` required when `status=unsupported`.

### `ResolutionRecordCache`

`record_version_boundary`, `entries`. If `records` is omitted, `entries` carries every cacheable selector at the current boundary, sorted by `record_key`. If supplied, one item per requested key in request order.

### `ExecutionStepSummary`

`step_index`, `step_kind`, `input_digest`, `output_digest`, `latency`, `canonicality_dependency`. Mirrors the persisted execution step list without exposing raw calldata or return bodies.

### `ResolutionExecutionExplainSummary`

`execution_trace_id`, `selected_entrypoint`, `resolver_discovery_path`, `wildcard`, `alias`, `steps`, `finished_at`. `execution_trace_id` equals top-level `provenance.execution_trace_id`. CCIP-Read participation is expressed through `steps[*].step_kind`.

### `CompactDomainSummary`

`namespace`, `name`, `normalized_name`, `namehash`, `labelhash`, `token_id`, `owner`, `registrant`, `created_at`, `registration_date`, `expiry_date`, `resolver_address`, `record_summaries`, `subname_count`, `record_count`. Used by `GET /v1/names`. `labelhash` and `token_id` appear only when the namespace exposes stable namespace-local token identity. Compact: omits provenance, full coverage, `logical_name_id`, `resource_id`, `surface_binding_id`, projection version, and normalized-event IDs.

### `CompactRecordSummary`

`resolver_address`, `text_records`, `known_text_keys`, `avatar`, `content_hash`, `coin_addresses`. The v1 compact record object keeps those keys present for schema stability; unrequested sections are `null`, and requested sections may also be `null` when the section has no backed value or is unsupported for the selected metadata mode. `known_text_keys` is declared inventory metadata, not verified enumeration. Value source for `text_records`, `avatar`, `content_hash`, `coin_addresses` follows `mode`: declared cache, verified output, or auto. `mode=auto` uses declared cache only when it can satisfy the requested values from replayable state; explicit declared gaps, unretained declared values, or missing declared selectors fall through to verified output for supported selectors. ENSv1 text records are selector-keyed (e.g. `avatar` is `text:avatar`).[^v1-pres-l20] When `mode=auto|verified|both` has no declared selectors to work from, compact routes may probe the basic app profile set: `addr:60`, `avatar`, `contenthash`, and text keys `description`, `url`, `email`. Fallback text keys that resolve to `not_found` are omitted unless requested explicitly.

### `CompactHistoryEvent`

`type`, `name`, `namespace`, `resource_id`, `block_number`, `timestamp`, `transaction_hash`, `log_index`, `data`. `data` is a route-owned compact payload; raw log bodies and full normalized-event rows stay out.

### `RoleRow`

`account`, `resource_hex`, `resource_id`, `name`, `role_bitmap`, `effective_powers`. `resource_id` is opaque and stable; clients treat it as such. `resource_hex` is nullable and appears only when a stable projected hex exists. Row-granular grant lineage is exposed by `GET /v1/resources/{resource_id}/permissions`, not compact role rows.

### `ResolverOverviewCompact`

`chain_id`, `resolver_address`, `counts`, `nodes`, `aliases`, `roles`, `events`. `counts` reports only sections backed by `resolver_current` or another declared family the route names. Unsupported sections appear in `meta.unsupported_fields` and are never rendered as supported zero counts.

### `ResolverOverviewBindingItem`

`namespace`, `name`, `normalized_name`, `namehash`. Used in compact resolver `nodes` and `aliases` arrays. `name` is the route-owned surface string for display/order, and `namehash` is the stable name identity key.

### `ResolverOverviewRoleItem`

`subject`, `resource_count`, `permission_row_count`, `effective_powers`, `resource_ids`. Row-granular grant and revocation lineage stays on `GET /v1/resources/{resource_id}/permissions`.

## Route catalog

The actual published routes are listed below. Per-route semantics are in [`api-v1-routes.md`](api-v1-routes.md). The grouping is normative for product guidance: native slim identity routes are the latency-critical partner/feed surface, canonical product routes are the preferred public API for app and explorer reads, and diagnostics carry coverage/provenance detail. Removed convenience aliases such as `/v1/resolve*` and `/v1/resolutions*` are not part of the slim native surface.

### Native slim identity

| Route | Purpose |
| --- | --- |
| `POST /v1/identity:lookup` | Native slim identity lookup. `profile=feed` is the partner-1 latency path; `profile=detail` is profile aggregation. |
| `GET /v1/status` | Public projection/indexing readiness by chain. |

### Canonical product reads

| Route | Purpose |
| --- | --- |
| `GET /v1/names` | Compact name search, exact-name filter, address relations, suggestions. |
| `GET /v1/names/{namespace}/{name}` | Exact name lookup without routine deep provenance. |
| `GET /v1/profiles/names/{name}` | App-facing full profile path with declared topology/cache and server-selected verified profile record results. |
| `GET /v1/names/{namespace}/{name}/children` | Direct children, compact by default, full via `view=full`. |
| `GET /v1/names/{namespace}/{name}/records` | Compact resolver records over declared inventory/cache and verified selectors; compact view only. |
| `GET /v1/addresses/{address}/names` | Address-to-surface collection. |
| `GET /v1/primary-names/{address}` | App primary-name lookup for an address; defaults to ENS coin type 60, with route-local on-demand reverse claim and forward verification when the persisted tuple is missing. |
| `GET /v1/resources/{resource_id}/permissions` | Resource-centric effective permissions. |
| `GET /v1/events` | Compact event search across name, address, resource, type, block filters; compact view only. |

### Metadata and control plane

| Route | Purpose |
| --- | --- |
| `GET /v1/namespaces/{namespace}` | Namespace metadata. |
| `GET /v1/manifests/{namespace}` | Active manifest versions and capabilities. |
| `GET /healthz` | Liveness check. Not part of the `v1` contract. |

### Diagnostics and provenance

| Route | Purpose |
| --- | --- |
| `GET /v1/coverage/{namespace}/{name}` | Single-name completeness and support surface. |
| `GET /v1/explain/names/{namespace}/{name}/surface-binding` | Current surface-binding explain view. |
| `GET /v1/explain/names/{namespace}/{name}/authority-control` | Current authority/control explain view. |
| `GET /v1/explain/resolutions/{namespace}/{name}/execution` | Persisted verified execution explain. |

### Specialist adjuncts

| Route | Purpose |
| --- | --- |
| `GET /v1/names/{namespace}/{name}/roles` | Compact role rows for the name's current resource plus ENSv2 root fallback rows when applicable; compact view only.[^v2-eac-l56][^v2-eac-l187] |
| `GET /v1/roles` | Compact role rows by account, resource, or name; compact view only. |
| `GET /v1/resources/lookup` | Compact lookup from `{namespace, name}` to current `resource_id`; compact view only. |
| `GET /v1/resolvers/{chain_id}/{resolver_address}/overview` | Compact resolver overview. |
| `GET /v1/history/names/{namespace}/{name}` | Surface or combined history. |
| `GET /v1/history/resources/{resource_id}` | Resource history. |
| `GET /v1/history/addresses/{address}` | Address activity history. |

The running API also serves `GET /`, `GET /docs`, and `GET /openapi.json` as helpers. They aren't `v1` business routes and don't appear in `docs/api-v1.openapi.json` as path entries.

## Sorting and pagination

| Route | Default sort |
| --- | --- |
| `GET /v1/names` | `name_asc` |
| `GET /v1/addresses/{address}/names` | `display_name_asc` |
| `GET /v1/names/{namespace}/{name}/children` | `display_name_asc` |
| `GET /v1/resources/{resource_id}/permissions` | `subject_scope_asc` |
| `GET /v1/roles` | `account_resource_scope_asc` |
| `GET /v1/names/{namespace}/{name}/roles` | `account_resource_scope_asc` |
| `GET /v1/history/names/{namespace}/{name}` | `chain_position_desc` |
| `GET /v1/history/resources/{resource_id}` | `chain_position_desc` |
| `GET /v1/history/addresses/{address}` | `chain_position_desc` |
| `GET /v1/events` | `chain_position_desc` |

Other routes don't honor `cursor` or `page_size`. Surface-first views break ties on `logical_name_id`; resource-grouped address views break on `resource_id`. `page.cursor` echoes the applied cursor or `null` on the first page; `page.next_cursor=null` means no further rows at the requested snapshot. Cursors are stable under replay for the same chain positions. Operational label-preimage imports and retained-fact backfills may repair unknown-label child display names without changing chain positions; cursors captured before that repair may become stale and return `400 invalid_input`.

Deploy note: name-role pagination now orders the `account_resource_scope_asc` text components with bytewise PostgreSQL `COLLATE "C"`, and the cursor envelope version is `2`. Cursors issued before that ordering change fail with `400 invalid_input` after rollout; clients must restart pagination from the first page.

## Error model

```json
{
  "error": {
    "code": "unsupported",
    "message": "the requested route option is not supported",
    "details": {}
  }
}
```

Codes: `invalid_input`, `not_found`, `unsupported`, `stale`, `verification_failed`, `conflict`, `internal_error`.

Rules:

- Use `unsupported` only when the request can't produce the route contract at all. When a route can produce the envelope but a subsection is unsupported, return `200` and surface the state through `UnsupportedSummary` or `ResultStatus.unsupported`.
- Malformed snapshot selectors, unsupported position slots, missing required slots, mixed-profile positions, and `at` plus `chain_positions` together return `400 invalid_input`.
- A coherent selector that can't be served from current projections returns `409 stale`.
- A selector whose supplied lineage, canonicality floor, or cross-chain reconciliation can't form one snapshot returns `409 conflict`.
- Persisted-readback routes return their documented stale or not-found state when matching output is missing. Supported ENS verified selectors on the resolution and compact records routes instead execute on demand, then return `409 stale` with a configuration message if the Ethereum RPC provider is unconfigured or can't serve the selected block.

## Versioning

- New optional fields and new routes are additive within `v1`.
- Changing enum meaning, default sort, coverage semantics, or required fields requires `v2`.
- An unsupported capability is reported through `coverage` or `error`. Never silent omission.

## Footnotes

[^bn-readme-l22]: (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc)
[^bn-readme-l28]: (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc)
[^bn-readme-l29]: (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc)
[^bn-readme-l34]: (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc)
[^bn-readme-l69]: (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)
[^bn-readme-l70]: (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)

[^bn-l2resolver-l22]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
[^bn-l2resolver-l182]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc)
[^bn-l2resolver-l193]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)

[^v1-pres-l20]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^v1-pres-l31]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f)
[^v1-pres-l66]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L66 @ ens_v1@91c966f)
[^v1-pres-l114]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f)

[^v1-namechanged-l10]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L10 @ ens_v1@91c966f)
[^v1-namechanged-l18]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L18 @ ens_v1@91c966f)
[^v1-revreg-l129]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f)
[^v1-revreg-l130]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)

[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309)
[^v2-deploy-ethrc]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)
[^v2-eac-l56]: (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L56 @ ens_v2@554c309)
[^v2-eac-l187]: (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L187 @ ens_v2@554c309)
