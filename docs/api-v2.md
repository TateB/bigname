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

Normative one-name-per-concept dictionary from ADR 0006:

| `v2` name | Meaning | Replaces (`v1`) |
| --- | --- | --- |
| `name` | the ENSIP-15 normalized name string | `normalized_name`, `logical_name_id` (derivable as `namespace:name`) |
| `display_name` | display form of the name | `canonical_display_name` |
| `owner` | token/registry owner | `token_holder`, `owner`, `owner_address`, `registry_owner` |
| `manager` | controller/manager | `effective_controller`, `manager_address` |
| `registrant` | registrant | `registrant` (unchanged) |
| `relation` | address-to-name relation filter: one or more of `owner`, `manager`, `registrant` (comma-separated set); `any` = all three | four divergent relation/role enums incl. `owned`/`managed`/`both` (partner `BOTH` = `owner,manager`) |
| `expires_at` | expiry, RFC 3339 | `expiry_date`, `expiration` (unix), `expiry` |
| `registered_at` | current registration start, RFC 3339 | `registration_date` |
| `created_at` | first observation of the name, RFC 3339 | `created_at` (now defined and distinguished from `registered_at`) |
| `addresses` | coin-type-to-address map, string keys | `coin_addresses`, `coin_type_addresses` |
| `text_records` | text-key-to-value map | `text_records` (unchanged) |
| `content_hash` | contenthash value | `content_hash` (unchanged) |
| `resolver` | `{chain_id, address}` | `resolver_address`, `current_resolver`, declared resolver summaries |
| `chain_id` | numeric EVM chain id (`1`, `8453`); string-keyed in maps | string chain ids (`"ethereum-mainnet"`), position slot keys |
| `network` | display slug (`ethereum`, `base`) | `network` (unchanged, display-only) |
| `registration_id` | the one opaque stable handle for a registration lifecycle | `resource_id`, `resource_hex`, `resource`, `token_lineage_id`, `surface_binding_id` |
| `finality` | `latest`, `safe`, `finalized` (JSON-RPC block-tag vocabulary) | `consistency` = `head`/`safe`/`finalized` |
| `source` | `indexed`, `verified` (the records route adds `auto`) | `mode` = `declared`/`verified`/`both`/`auto`; `declared_state`/`verified_state` |
| `as_of` | per-chain `{block_number, block_hash, timestamp}`, keyed by `chain_id` | `chain_positions` (and the `execution_checkpoint` pseudo-slot is diagnostics-only) |
| `scope` (history) | `name`, `registration`, `both` | `surface`, `resource`, `both` |
| `grant_scope` | the protocol scope of a permission row (root/registry/resolver-scoped grants) | permission-row `scope` (renamed so history `scope` and permission scope are two names for two concepts) |
| `status` | one result vocabulary: `ok`, `not_found`, `invalid_name`, `mismatch`, `unsupported`, `stale`, `failed` | `ResultStatus`, `IdentityStatus`, `NameRecordStatus`, `unnormalizable_input` (folds into `invalid_name`); `mismatch` kept for verification results |
| `completeness` | `full`, `partial`, `unsupported` | `coverage.status` on product routes (full taxonomy moves to diagnostics) |
| `powers` | effective permission powers | `effective_powers` |

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
    "completeness": "partial",
    "unsupported_fields": ["role_summary"],
    "unsupported_reason": "not_supported_for_namespace",
    "source": "indexed"
  }
}
```

Rules:

- `data` is an object on single-resource routes and an array on collections.
- `page` appears on collections only. Per-input pagination on `POST /v2/lookup`
  uses the same object inside each result.
- `total_count` is nullable. It is populated where an indexed count sidecar
  makes it cheap or where the caller opts in via `include=total_count`.
  Routes must not run unconditional full counts on the request path to fill it.
- `meta` is always present. Routes that read chain-derived state include
  `meta.as_of`; control-plane routes (`/v2/status`,
  `/v2/namespaces/{namespace}`) omit it. `meta.completeness`,
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
sections or opt into documented expensive metadata such as `total_count`, but it
must not change the envelope shape.

`profile=feed` on `POST /v2/lookup` is a field budget over the same record
shape used by `profile=detail`. Feed returns fewer fields; every feed field has
the same name and type as its detail counterpart.

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
| `at` | Tier-2 projection reads, except lookup primitives | RFC 3339 timestamp, or a URL-safe opaque snapshot token round-tripped from `meta.as_of` |
| `finality` | projection-read routes | `latest` (default), `safe`, `finalized` |
| `source` | names, records, primary-name | `indexed` (default), `verified`; the records route also accepts `auto` |
| `namespace` | name-inferred, address-anchored, and collection routes | explicit override or filter |
| `include` | route-documented expansions | per-route allowlist |
| `sort`, `order` | every paginated route | route-documented field set plus `asc`/`desc` |
| `cursor`, `page_size` | every paginated route | opaque cursor; default 50, max 200 |

No advertised-but-rejected parameters are part of the `v2` contract.

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
- `failure_reason` is permitted on `failed`, `not_found`, and `mismatch`.
- `mismatch` is the verification state where a claimed answer verifies to a
  different value.
- `completeness` is `full`, `partial`, or `unsupported`.
- Empty arrays mean known-empty, not unknown.

## Finality And Snapshots

`finality` values are `latest`, `safe`, and `finalized`. Snapshot selection is
uniform across projection-read routes. Each chain-derived response carries
`meta.as_of`, keyed by stringified `chain_id`, and that response metadata can
round-trip as an `at` snapshot token to pin exact per-chain positions.

`POST /v2/lookup` is a current-state read. It does not accept `at` or
`finality`; its `meta.as_of` records the served positions for staleness
attribution and shadow-diff correlation.

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
| `stale` | 409 | coherent selector not yet served by projections |
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
- Error messages must not name internal tables, projections, or sidecars.
