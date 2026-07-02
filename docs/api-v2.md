# API v2

Development-time contract for the API surface accepted in
[ADR 0006](adrs/0006-api-v2-product-surface.md). Per-route reference lives in
[`api-v2-routes.md`](api-v2-routes.md). The generated OpenAPI document is not
part of this docs-only rollout step; it is generated from the route table in
the implementation step.

## Contract Principles

`v2` is designed around three rules:

1. **One vocabulary.** Every domain concept has exactly one wire name, drawn
   from common ENS/blockchain usage, defined in the naming dictionary below,
   and used identically on every route.
2. **One envelope.** Every route returns `data`, plus `page` on collections,
   plus `meta`. Field budgets may subset fields but never rename, retype, or
   restructure them.
3. **Three tiers.** Lookup primitives, product reads, and diagnostics are
   separate route families. The route path decides the tier; a query parameter
   never switches a route into another tier.

## Versioning

`v2` is a development designation only. The new surface is built under `/v2`
alongside the frozen `v1`, passes the one-time parity gate, and then ships as
the re-baselined `v1`: old `v1` routes are deleted and the `/v2` prefix is
renamed to `/v1` in the same release. No permanent `/v2` prefix ships as the
public contract.

Until that switch, `docs/api-v1.md` and `docs/api-v1-routes.md` are frozen
except for corrections. These `api-v2` docs are the development-time contract
that step 3 implements.

## Naming Dictionary

Normative one-name-per-concept dictionary from ADR 0006, extended with the
step-3-gate vocabulary needed by the route schemas:

| `v2` name | Meaning | Replaces (`v1`) |
| --- | --- | --- |
| `name` | the ENSIP-15 normalized name string | `normalized_name`, `logical_name_id` (derivable as `namespace:name`) |
| `display_name` | display form of the name | `canonical_display_name` |
| `namespace` | public namespace slug used to resolve a name or filter a route, such as `ens` or `basenames` | `namespace` path segment/query usage (unchanged; now echoed consistently) |
| `namehash` | ENS namehash hex string | `namehash` (unchanged) |
| `token_id` | decimal-string token id for tokenized registrations/names | `token_id` (unchanged; now defined consistently) |
| `owner` | token/registry owner | `token_holder`, `owner`, `owner_address`, `registry_owner` |
| `manager` | controller/manager | `effective_controller`, `manager_address` |
| `registrant` | registrant | `registrant` (unchanged) |
| `relation` | address-to-name relation filter: one or more of `owner`, `manager`, `registrant` (comma-separated set); `any` = all three | four divergent relation/role enums incl. `owned`/`managed`/`both` (partner `BOTH` = `owner,manager`) |
| `relations` | address-to-name relations that matched a row, using `owner`, `manager`, and `registrant` values | `relation_facets`, role-specific match arrays |
| `expires_at` | expiry, RFC 3339 | `expiry_date`, `expiration` (unix), `expiry` |
| `registered_at` | current registration start, RFC 3339 | `registration_date` |
| `created_at` | first observation of the name, RFC 3339 | `created_at` (now defined and distinguished from `registered_at`) |
| `registration_status` | registration/control lifecycle label: `active`, `wrapped`, `registered`, `released`, or `unregistered` | `ControlVector.status`, role-summary `status` |
| `primary_name` | primary name selected or claimed for an address/coin tuple | `claimed_primary_name`, `verified_primary_name` when surfaced as the selected name |
| `primary_address` | primary/default address value for a name | `primary_address` (unchanged) |
| `is_primary` | whether an address-name row is the selected primary answer for that address/coin tuple | `is_primary` (unchanged) |
| `addresses` | coin-type-to-address map, string keys | `coin_addresses`, `coin_type_addresses` |
| `address` | EVM address used as a subject, filter, or single-address answer | `account`, `subject`, single-address fields named `address` |
| `coin_type` | ENS/SLIP-44 coin type number | `coin_type` (unchanged; now used consistently for reverse and record lookups) |
| `text_records` | text-key-to-value map | `text_records` (unchanged) |
| `content_hash` | contenthash value | `content_hash` (unchanged) |
| `resolver` | `{chain_id, address}` | `resolver_address`, `current_resolver`, declared resolver summaries |
| `chain_id` | numeric EVM chain id (`1`, `8453`); string-keyed in maps | string chain ids (`"ethereum-mainnet"`), position slot keys |
| `network` | display slug (`ethereum`, `base`) | `network` (unchanged, display-only) |
| `registration_id` | the one opaque stable handle for a registration lifecycle | `resource_id`, `resource_hex`, `resource`, `token_lineage_id`, `surface_binding_id` |
| `input` | caller-supplied lookup input echoed in a result | `input` (unchanged; now specified as result echo, not a parallel DTO family) |
| `normalization` | name-normalization result for an input | `corrected_input_normalization`, `unnormalizable_input` status detail |
| `finality` | `latest`, `safe`, `finalized` (JSON-RPC block-tag vocabulary) | `consistency` = `head`/`safe`/`finalized` |
| `source` | answer origin `indexed` or `verified` (the records route adds request value `auto`) | `mode` = `declared`/`verified`/`both`/`auto`; `declared_state`/`verified_state` |
| `as_of` | readable per-chain `{block_number, block_hash, timestamp}`, keyed by `chain_id` | `chain_positions` (and the `execution_checkpoint` pseudo-slot is diagnostics-only) |
| `as_of_token` | opaque URL-safe snapshot token for replaying the exact served positions with `at` | reconstructing `at` from `chain_positions` |
| `at` | snapshot selector parameter for routes that support point-in-time reads | `chain_positions` query parameter and timestamp-specific ad hoc selectors |
| `include` | route-documented expansion allowlist | comma-separated expansion flags, `meta` knobs, and route-specific include flags |
| `sort` | route-documented sort field | `sort` (unchanged; allowed fields are now route-documented) |
| `order` | sort direction, `asc` or `desc` | `order` (unchanged) |
| `scope` (history) | `name`, `registration`, `both` | `surface`, `resource`, `both` |
| `grant_scope` | the protocol scope of a permission row (`root`, `registry`, `registration`, resolver-scoped, and derived grants) | permission-row `scope` (renamed so history `scope` and permission scope are two names for two concepts) |
| `verification` | typed checked-answer summary for claimed-vs-verified answers | `verified_state`, `verified_primary_name` section wrappers |
| `status` | one result vocabulary: `ok`, `not_found`, `invalid_name`, `mismatch`, `unsupported`, `stale`, `failed` | `ResultStatus`, `IdentityStatus`, `NameRecordStatus`, `unnormalizable_input` (folds into `invalid_name`); `mismatch` kept for verification results |
| `unsupported_reason` | reason code or short reason string required with `status=unsupported` | `coverage.unsupported_reason`, route-specific unsupported details |
| `failure_reason` | reason code or short reason string for `failed`, `stale`, `not_found`, or `mismatch` details | route-specific failure detail fields |
| `completeness` | `full`, `partial`, `unsupported` | `coverage.status` on product routes (full taxonomy moves to diagnostics) |
| `powers` | effective permission powers; storage `resource_control` is exposed as `registration_control` | `effective_powers` |
| `unsupported_fields` | fields or expansions that could not be served or proved for a response item | `unsupported_filters`, coverage-derived unsupported field lists |
| `keys` | comma-separated resolver record-key allowlist | `records` query parameter, selector token lists in record diagnostics |
| `page` | pagination object on top-level collections, per-input lookup results, and the resolver overview `bound_names` nested collection | pagination sections with divergent field subsets |
| `cursor` | opaque request cursor for the current page | `cursor` (unchanged; now opaque and versioned) |
| `next_cursor` | opaque cursor for the next page, or `null` | `next_cursor` (unchanged) |
| `page_size` | requested or served page size | `page_size` (unchanged) |
| `total_count` | nullable total item count when cheap or explicitly requested | `total_count` (unchanged; now nullable and budgeted) |
| `has_more` | whether another page is available | `has_more` (unchanged) |
| `meta` | response metadata object for snapshot, completeness, unsupported, and source details | `provenance`, `coverage`, `chain_positions`, `consistency`, `last_updated` top-level peers |
| `subname_count` | count of direct subnames when requested | `subname_count` (unchanged; now the only count name for child rows) |
| `record_count` | count of known record keys when requested | `record_count` (unchanged) |
| `role_summary` | grouped permission powers for dashboard-style name rows | `role_summary` (unchanged; rewritten to dictionary field names inside) |
| `capabilities` | product-facing summary of supported namespace capabilities | capability flag summaries when exposed to product routes |
| `type` | product event category label | `event_kind`, compact event `type` aliases |
| `by_type` | map of product event `type` values to counts | event summary `by_kind` maps keyed by raw event kind |
| `block_number` | EVM block number | block-number fields inside chain-position objects |
| `block_hash` | EVM block hash | block-hash fields inside chain-position objects |
| `timestamp` | RFC 3339 event or block timestamp | event timestamps and chain-position timestamps |
| `transaction_hash` | EVM transaction hash | `transaction_hash` (unchanged) |
| `log_index` | EVM log index within a transaction | `log_index` (unchanged) |
| `from_block` | inclusive lower block-number filter | `from_block` (unchanged) |
| `to_block` | inclusive upper block-number filter | `to_block` (unchanged) |
| `data` | envelope root payload, and event-row payload when nested inside an event row | compact event payload objects |

Rules:

- Timestamps are RFC 3339 UTC everywhere, including the lookup route.
- JSON map keys are strings (`"60"`, `"8453"`); `chain_id` as an object field
  is a JSON number.
- `token_id` stays a decimal string.
- Pipeline vocabulary (`projection`, `sidecar`, `manifest`, `normalized event`,
  `raw fact`, table names) must not appear in product-route field names, enum
  values, or error messages.

## Envelope

One success shape applies to every route:

```json
{
  "data": {},
  "page": {
    "cursor": null,
    "next_cursor": "opaque-token",
    "page_size": 50,
    "total_count": 123,
    "has_more": true
  },
  "meta": {
    "as_of": {
      "1": {
        "block_number": 19000000,
        "block_hash": "0x...",
        "timestamp": "2026-06-10T00:00:00Z"
      }
    },
    "as_of_token": "opaque-token",
    "completeness": "partial",
    "unsupported_fields": ["role_summary"],
    "unsupported_reason": "not_supported_for_namespace",
    "source": "indexed"
  }
}
```

Rules:

- `data` is an object on single-resource routes and an array on collections.
- Top-level `page` appears on collection routes only. Per-input pagination on
  `POST /v2/lookup` and the nested resolver-overview `bound_names` collection
  use the same object inside their containing result/object.
- `total_count` is nullable. It is populated where a precomputed count makes
  it cheap or where a route explicitly documents `include=total_count`. Routes
  must not run unconditional full counts on the request path to fill it.
- `meta` is always present. Routes that read chain-derived state include
  `meta.as_of` and `meta.as_of_token` when they can attribute at least one
  served snapshot-pinned chain position; control-plane routes (`/v2/status`,
  `/v2/namespaces/{namespace}`), verified name-profile responses served by the
  route-local on-demand fallback, and primary-name responses served by the
  route-local on-demand fallback omit both. `meta.as_of` is human-readable
  staleness attribution. `meta.as_of_token` is opaque and is the value to pass
  to `at` when a route supports snapshot replay. `meta.completeness`,
  `meta.unsupported_fields`, and `meta.unsupported_reason` appear only when the
  read is not clean. `meta.source` appears when the route supports `source`.
- `meta.unsupported_fields` names response-level sections or expansions the
  route could not serve. Record-level `unsupported_fields` names data fields
  the index could not prove for that record. One unsupported field is not
  duplicated at both levels in one response.
- There is no `meta` query parameter and no stripped envelope variant.
- There are no `declared_state`/`verified_state` parallel trees and no `both`
  mode.

## Field Budgets

`include` is a route-documented expansion allowlist. It may add documented
sections or route-documented expensive metadata. No route supports
`include=total_count` unless that route's parameter list says so.

`profile=feed` on `POST /v2/lookup` is a field budget over the same record
shape used by `profile=detail`. Feed returns fewer fields; every feed field has
the same name and type as its detail counterpart. Feed does not change reverse
lookup pagination semantics: `cursor`, `page_size`, `next_cursor`, and
`has_more` mean the same thing as detail.

Flat record optional fields are omitted when there is no backed value. Routes
do not serialize permanently-null placeholders for optionals such as `manager`.
Known-empty maps on detail records, such as `addresses` and `text_records`,
serialize as `{}`; omission means the field is outside the requested field
budget or unsupported by the served source.

## Tiers

### Tier 1: Lookup Primitives

Lookup primitives serve the partner latency path and current indexing status:

- `POST /v2/lookup`
- `GET /v2/status`

The lookup route uses the common record shape and in-band per-result statuses.
`GET /v2/status` is the only route with the ops status vocabulary
`ready`, `degraded`, `stale`.

### Tier 2: Product Reads

Product routes serve app and public read workflows. They must use only product
vocabulary in field names, enum values, and error messages. Product routes may
expose simplified `completeness`, `unsupported_fields`, and per-item `status`,
but they must not expose pipeline internals.

The product-route denylist includes pipeline terms such as `projection`,
`sidecar`, `manifest`, `normalized event`, `raw fact`, storage table names,
`logical_name_id`, `resource_id`, `token_lineage_id`,
`surface_binding_id`, `binding_kind`, `normalized_event_id`,
`raw_fact_refs`, `manifest_versions`, `derivation_kind`,
`exhaustiveness`, `enumeration_basis`, `source_classes_considered`, and the
`execution_checkpoint` pseudo-chain slot. If a product capability needs that
detail, it belongs on a diagnostics route instead.

### Tier 3: Diagnostics

Diagnostics are the only public routes that may carry pipeline vocabulary.
They expose coverage taxonomy, binding and authority explanations, record
inventory/cache internals, persisted execution explanations, active manifests,
and raw normalized-event rows.

## Parameters

Common parameter rules:

| Parameter | Applies to | Values |
| --- | --- | --- |
| `at` | Tier-2 snapshot reads: names, records, subnames, history, permissions, address name/history collections, search, events, and resolver overview; diagnostics exact-name snapshot/explain routes; not lookup, status, primary-name, or namespace metadata | RFC 3339 timestamp, or the URL-safe opaque snapshot token from `meta.as_of_token` |
| `finality` | snapshot-read routes and diagnostics exact-name snapshot/explain routes; not lookup, status, primary-name, or namespace metadata | `latest` (default), `safe`, `finalized` |
| `source` | names, records, primary-name | names and records use `indexed` (default) or `verified`; the records route also accepts `auto`; primary-name omits `source` to return all supported source answers and may use `indexed` or `verified` to request a subset |
| `namespace` | name-inferred, address-anchored, and collection routes | explicit override or filter |
| `include` | route-documented expansions | per-route allowlist |
| `sort`, `order` | paginated routes that declare a sort set | route-documented field set plus `asc`/`desc` |
| `cursor`, `page_size` | every paginated route | opaque cursor; default 50, max 200 |

No advertised-but-rejected parameters are part of the `v2` contract.
Unknown or undocumented query parameters are rejected with `400 invalid_input`
on every `v2` route.
Snapshot-pinned reads require the ADR 0003 slice-3 snapshot-service enabler;
ADR 0006 rollout step 3 includes that read-layer work.

## Status Vocabulary

One result-status vocabulary is used everywhere except the `/v2/status` ops
route:

- `ok`
- `not_found`
- `invalid_name`
- `mismatch`
- `unsupported`
- `stale`
- `failed`

Rules:

- `unsupported_reason` is required when `status=unsupported`.
- `failure_reason` is permitted on `failed`, `stale`, `not_found`, and
  `mismatch`.
- `mismatch` is the verification state where a claimed answer verifies to a
  different value.
- `completeness` is `full`, `partial`, or `unsupported`.
- Empty arrays and empty maps mean known-empty, not unknown.

## Finality And Snapshots

`finality` values are `latest`, `safe`, and `finalized`. Snapshot selection is
uniform across snapshot-read routes. Each chain-derived response carries
`meta.as_of`, keyed by stringified `chain_id`, and `meta.as_of_token`, an
opaque token that can round-trip as `at` to pin exact per-chain positions.
Tokens must cover every required slot in the target route's snapshot scope and
must not carry extra slots outside that scope. A single-chain token from a
namespace-scoped read is rejected by namespace-omitted `/v2/search`, because
the union search scope requires every public namespace slot. A multi-chain
token from namespace-omitted search replays on the same union search and is
rejected by single-namespace routes whose scope does not include every token
slot.
Event, history, and other collection routes use `at`/`finality` to resolve
`meta.as_of`/`meta.as_of_token`, but their row sets currently read the latest
projection or normalized-event rows unless the route section explicitly says
otherwise. True row-bounding by `at`, `safe`, or `finalized` is deferred to
storage follow-up work.

`POST /v2/lookup` is a current-state read. It does not accept `at` or
`finality`; when a served head is available, its `meta.as_of` and
`meta.as_of_token` record the served positions for staleness attribution and
shadow-diff correlation. Lookup rejects partial scoped heads instead of
emitting a token that cannot replay on a compatible snapshot-read route.

`GET /v2/addresses/{address}/primary-name` is also a current-state read. It
does not accept `at` or `finality`; when a served head is available, its
`meta.as_of` and `meta.as_of_token` record the served positions for staleness
attribution and shadow-diff correlation. When the ENS/60 route-local on-demand
fallback supplies the answer instead of persisted snapshot state, the response
omits `meta.as_of` and `meta.as_of_token`. Basenames responses that serve a
persisted verified answer include both the Base authority position and the
Ethereum resolution-auxiliary position; indexed-only responses and missing
persisted verified outcomes remain Base-scoped.

The `chain_positions` query parameter from `v1` does not exist in `v2`.

## Cursors And Pagination

Cursors are opaque and versioned. They are not bound to the route path string,
so route evolution does not invalidate outstanding cursors. Cursors remain
stable under replay for the same snapshot.

Every collection uses `cursor`, `next_cursor`, `page_size`, nullable
`total_count`, and `has_more`. Default `page_size` is 50; maximum is 200.

## Error Model

Error envelope:

```json
{
  "error": {
    "code": "unsupported",
    "message": "the requested route option is not supported",
    "details": {}
  }
}
```

Uniform mapping:

| Code | HTTP | Meaning |
| --- | --- | --- |
| `invalid_input` | 400 | malformed input, unnormalizable path name, bad parameter combination |
| `not_found` | 404 | single-resource GET with no answer |
| `unsupported` | 422 | the route cannot produce its contract for this input |
| `stale` | 409 | coherent selector not yet served for the selected snapshot |
| `conflict` | 409 | selector cannot form one canonical snapshot |
| `internal_error` | 500 | unexpected failure |

Rules:

- `unsupported` is `422`.
- Verified-execution failures surface as `status: "failed"` on the affected
  section with `failure_reason`, or as `stale` when the RPC provider cannot
  serve the selected block.
- Single-resource GETs return `404 not_found` when no answer exists.
- Collections return `200` with empty `data`.
- Batch lookup results carry in-band `status` per input; a batch never returns
  `404` for one missing input.
- The primary-name route is the documented exception to single-resource `404`:
  a valid `{address, coin_type, namespace}` tuple with no claim or an
  unsupported/mismatched verification returns `200` with in-band `status`.
- Error messages must not name internal storage or pipeline components.
