# ADR 0006: API v2 Product Surface

Status: Accepted
Date: 2026-06-10
Accepted: 2026-06-12

## Context

A holistic review of the `v1` API (2026-06-10) confirmed the fragmentation that
ADR 0003 diagnosed but never resolved: the scope worksheet behind it
(`docs/internal/api-surface-flattening-scope-decisions.md`) still has an empty
"Final Direction" section, and the vocabulary/envelope decisions it deferred were
never made. Measured against the implementation, the `v1` contract today has:

- 24 routes producing 8 distinct response envelope shapes in practice (full
  single, full collection, compact collection, compact single, the profile-route
  hybrid whose arity changes with `meta`, the identity batch shape, the status
  wrapper, and `/healthz`).
- 54 OpenAPI component schemas, 37 used exactly once, 2 orphaned, and
  about 10 documented shapes that exist only as untyped `JsonObject`.
- The same concept spelled differently per route: the name string as `name` /
  `normalized_name` / `canonical_display_name` (with `name` itself meaning
  display-name on some routes and normalized-name on others); controlling
  addresses as `owner` / `owner_address` / `registrant` / `registry_owner` /
  `account` / `subject` / `manager_address`; expiry as `expiry_date` (RFC 3339)
  vs `expiration` (unix integer) vs `expiry`; four relation enums; nine status
  vocabularies; six sort vocabularies in two styles; two pagination objects.
- Indexer-internal vocabulary on every default read: `logical_name_id`,
  `resource_id`, `token_lineage_id`, `surface_binding_id`, `binding_kind`,
  `normalized_event_id`, `raw_fact_refs`, `manifest_versions`,
  `derivation_kind`, `exhaustiveness`, `enumeration_basis`,
  `source_classes_considered`, the `execution_checkpoint` pseudo-chain slot, and
  projection table names leaking into `meta.value_source` and error messages.
- Contract rot: `verification_failed` and `authority_epoch` are documented but
  never emitted; `resource_hex` and `role_bitmap` are hardcoded `null` on the
  wire; `CompactDomainSummary` documents fields the builder never produces;
  `/v1/events` declares roughly 11 query parameters that exist only to be
  rejected; seven routes advertise a `view` enum with one legal value.

The two driving consumers need the opposite of this:

- partner-1 (`docs/partners/partner-1-indexing-requirements.md`) needs two
  primitives — forward name-to-record and reverse address-plus-coin-type-to-names,
  batched — with a flat record shape in common vocabulary, `has_more` /
  `total_count` / cursor pagination, per-response staleness metadata, defined
  empty-vs-not-found semantics, no caller-side namespace fan-out, and p95 under
  10 ms. `POST /v1/identity:lookup` already satisfies this and meets a local
  server-side proxy for the latency target through 250 inputs
  (`docs/partners/partner-1-identity-facade-benchmarks.md`); the final AWS
  `us-east` vantage measurement is outstanding and is rerun at the parity
  gate.
- The first-party app (capability scan in ADR 0003) needs profile, records,
  owned-names-with-role-summary, subnames, search, and history reads — and does
  not call the `v1` API yet.

That last fact is the strategic window: there are approximately zero live
consumers of the current contract. partner-1 is pre-shim and the app is on
ENSJS/GraphQL. Breaking the API costs nothing today and becomes permanently
expensive at partner cutover.

The review also confirmed what is worth keeping: snapshot-pinned reads, the
indexed-vs-onchain-verified distinction, and explicit unsupported semantics are
real differentiators — partner-1's shadow-comparison migration leans on
exactly these properties, the explicit semantics and per-response staleness
attribution in particular. The defect is that they leak on every route by
default, under invented names, in overlapping vocabularies. And one route —
`identity:lookup` — already demonstrates the target ergonomics: flat DTOs,
common vocabulary, sane batch semantics, fastest path in the system. This ADR
makes the exception the rule.

## Decision

Publish a new product contract — developed as `v2`, shipped as the
re-baselined `v1` — designed around three rules:

1. **One vocabulary.** Every domain concept has exactly one wire name, drawn
   from common ENS/blockchain usage, defined in the naming dictionary below, and
   used identically on every route.
2. **One envelope.** Every route returns `data`, plus `page` on collections,
   plus `meta`. No full/compact dual shapes and no query-time envelope or
   arity switching; documented field budgets (`include=`, the lookup feed
   profile) may subset fields but never rename, retype, or restructure them.
3. **Three tiers.** Lookup primitives (the partner path), product reads (the app
   path), and diagnostics (the only home of pipeline vocabulary). Product
   responses carry zero indexer internals; the route path — not a query knob —
   decides which tier a response belongs to.

`v2` is a development designation only. The new surface is built under the
`/v2` prefix alongside the frozen `v1`, passes a one-time parity validation,
and then ships as the new `v1`: the old `v1` routes are deleted and the `/v2`
prefix is renamed to `/v1` in the same release. The public API stays at `v1`;
no permanent `/v2` prefix exists and there is no coexistence or deprecation
window (see Rollout).

### Naming dictionary (normative)

One name per concept, applied on every `v2` route:

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
| `as_of` | readable per-chain `{block_number, block_hash, timestamp}`, keyed by `chain_id` | `chain_positions` (and the `execution_checkpoint` pseudo-slot is diagnostics-only) |
| `as_of_token` | opaque URL-safe snapshot token for replaying the exact served positions with `at` | reconstructing `at` from `chain_positions` |
| `scope` (history) | `name`, `registration`, `both` | `surface`, `resource`, `both` |
| `grant_scope` | the protocol scope of a permission row (root/registry/resolver-scoped grants) | permission-row `scope` (renamed so history `scope` and permission scope are two names for two concepts) |
| `status` | one result vocabulary: `ok`, `not_found`, `invalid_name`, `mismatch`, `unsupported`, `stale`, `failed` | `ResultStatus`, `IdentityStatus`, `NameRecordStatus`, `unnormalizable_input` (folds into `invalid_name`); `mismatch` kept for verification results |
| `completeness` | `full`, `partial`, `unsupported` | `coverage.status` on product routes (full taxonomy moves to diagnostics) |
| `powers` | effective permission powers | `effective_powers` |

Rules:

- Timestamps are RFC 3339 UTC everywhere, including the lookup route. partner-1
  requested unix-seconds `expiration`; the partner shim performs that format
  mapping. Field semantics, not field format, are the requirement per the
  partner document.
- JSON map keys are strings (`"60"`, `"8453"`); `chain_id` as an object field is
  a JSON number.
- `token_id` stays a decimal string (uint256).
- Pipeline vocabulary (`projection`, `sidecar`, `manifest`, `normalized event`,
  `raw fact`, table names) must not appear in product-route field names, enum
  values, or error messages.

### Route catalog

Routes below carry the `/v2` development prefix; at the switch the prefix
becomes `/v1` (see Rollout). The development prefix is what allows old and new
surfaces to coexist in one binary for parity validation — several new paths
(e.g. `GET /v2/names/{name}`) would be ambiguous against old-`v1` grammar
(`GET /v1/names/{namespace}/{name}`) at the same prefix.

Name-shaped routes infer the namespace from the name itself (the existing
`profiles/` and `identity:lookup` inference: exact `base.eth` is `ens` because
upstream treats it as the L1 root domain handled by the Mainnet
L1Resolver (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc);
`*.base.eth` is `basenames`, the Base-issued subdomain
space (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc); other
supported names are `ens`), accept an optional `?namespace=` override, and
always echo the resolved `namespace` in the response. This collapses the `v1` dual grammar of `/v1/names/{ns}/{name}` vs
`/v1/profiles/names/{name}`.

Tier 1 — lookup primitives:

| Route | Purpose |
| --- | --- |
| `POST /v2/lookup` | Batched forward (name) and reverse (address + coin type) resolution. `profile=feed` is the latency path; `profile=detail` returns full records. Replaces `POST /v1/identity:lookup`. |
| `GET /v2/status` | Per-chain indexing readiness: `chains: {<chain_id>: {latest_block, indexed_block, safe_block, finalized_block, lag_blocks, lag_seconds, status}}`. `status` here is the ops vocabulary `ready\|degraded\|stale` — the one non-result status enum, scoped to this route. |

Tier 2 — product reads:

| Route | Purpose |
| --- | --- |
| `GET /v2/names/{name}` | Name profile: the flat record shape plus registration summary. Replaces `/v1/names/{ns}/{name}` + `/v1/profiles/names/{name}`. |
| `GET /v2/names/{name}/records` | Resolver records. `?source=indexed\|verified\|auto` (`auto` keeps `v1`'s replay-safe-cache-with-verified-fallback policy), `?keys=` selector filter. `?include=inventory` adds the known selector space and unset keys (the record-editing capability) in product vocabulary; deep inventory internals stay on diagnostics. |
| `GET /v2/names/{name}/subnames` | Direct subnames. `?include=counts`. Replaces `children`. Lists children from the latest projection; as-of child enumeration is deferred to a storage follow-up. |
| `GET /v2/names/{name}/history` | Name history. `?scope=name\|registration\|both`. |
| `GET /v2/permissions` | Permission rows by `?name=`, `?registration_id=`, or `?address=` (at least one required; combinable), including registrations that are no longer a name's current one. `?include=lineage` adds per-row grant/revocation lineage and inheritance/transfer behavior. A flat filterable collection in the same style as `/v2/events`. Replaces `/v1/resources/{id}/permissions`, `/v1/roles`, `/v1/names/.../roles`, `/v1/resources/lookup`. |
| `GET /v2/addresses/{address}/names` | Names related to an address. Implemented in ADR 0006 step 3. `?relation=` uses the v2 product vocabulary `owner\|manager\|registrant` and maps storage token-holder -> `owner`, effective-controller -> `manager`, and registrant -> `registrant`; `?q=` is prefix text filtering, `?sort=name\|expires_at\|registered_at`, `?dedupe=name\|registration`, and `?include=role_summary` keeps the dashboard combination of address relation + text filter + sort. |
| `GET /v2/addresses/{address}/primary-name` | Primary name. `?coin_type=` (default `60`), `?namespace=` (default `ens`), `?source=`. Replaces `/v1/primary-names/{address}` with the same `{address, coin_type, namespace}` tuple selection. Returns one answer per `source` plus a typed `verification` summary (`{status, name}`, `status` incl. `mismatch`) whenever a persisted or on-demand verified outcome exists — claimed-vs-verified stays one call without parallel state trees. |
| `GET /v2/addresses/{address}/history` | Address activity history. `?relation=` set filter, `?scope=name\|registration\|both` — keeps `v1`'s anchor selection for separating name-surface events from registration-lifecycle events. |
| `GET /v2/search` | Name search and suggestions: `?q=` with `?match=prefix\|contains` (default `prefix`), `?namespace=`; paginates with the standard `cursor`/`page_size` like every collection. Split out of `/v1/names`; no availability or pricing semantics. |
| `GET /v2/events` | Compact event search across name, address, registration, type, and block filters. |
| `GET /v2/resolvers/{chain_id}/{address}` | Resolver overview (numeric `chain_id`). Replaces `/v1/resolvers/.../overview` and, through its paginated bound-names section, the `/v1/names?resolver=` filter. |
| `GET /v2/namespaces/{namespace}` | Namespace metadata: supported-capability summary in product vocabulary. |

Tier 3 — diagnostics (the only routes carrying pipeline vocabulary):

| Route | Purpose |
| --- | --- |
| `GET /v2/diagnostics/names/{name}/coverage` | Full coverage taxonomy: `exhaustiveness`, `enumeration_basis`, `source_classes_considered`, `unsupported_reason` detail. |
| `GET /v2/diagnostics/names/{name}/binding` | Surface-binding explain (binding ids, binding kind, anchors). |
| `GET /v2/diagnostics/names/{name}/authority` | Authority/control explain (token lineage, control vectors, permission lineage). |
| `GET /v2/diagnostics/names/{name}/records` | Record inventory and cache internals: selectors, explicit gaps, unsupported families, version boundaries, value sources, and indexed-vs-verified side-by-side comparison (the former `mode=both`). |
| `GET /v2/diagnostics/names/{name}/execution` | Persisted verified-execution explain: trace id, steps, digests, CCIP participation. |
| `GET /v2/diagnostics/namespaces/{namespace}/manifests` | Active manifest versions, source families, deployment epochs, capability flags. Replaces `/v1/manifests/{namespace}`. |
| `GET /v2/diagnostics/events` | Raw normalized-event rows: upstream event kinds, event identity, full provenance. Same filters as `/v2/events`. This is the home of `v1` history `view=full`, resolving ADR 0003's open history full-view decision: the full row shape survives as a diagnostics contract, not a product one. |

`GET /healthz`, `GET /`, `GET /docs`, and `GET /openapi.json` remain non-contract
helpers.

Deleted from the public catalog (capability absorbed as noted): the `profiles/`
prefix, `/v1/coverage/*` and `/v1/explain/*` (moved under diagnostics),
`/v1/resources/*` and the roles routes (merged into the flat
`GET /v2/permissions`, where `registration_id` remains a first-class filter
and response field),
`/v1/manifests/*` (moved to diagnostics — manifest vocabulary is pipeline
internals and stays off product routes), and exact-name filtering via
`/v1/names?name=` (owned by `GET /v2/names/{name}`).

### Envelope

One success shape for every route:

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
    "as_of_token": "opaque-snapshot-token",
    "completeness": "partial",
    "unsupported_fields": ["role_summary"],
    "unsupported_reason": "not_supported_for_namespace",
    "source": "indexed"
  }
}
```

Rules:

- `data` is an object on single-resource routes and an array on collections.
- `page` appears on collections only and is identical everywhere, including
  partner-1's `total_count` and `has_more`. Per-input pagination on
  `POST /v2/lookup` uses this same object inside each result.
- `total_count` is nullable. It is populated where an indexed count sidecar
  makes it cheap (the reverse-lookup count path) or where the caller opts in
  via `include=total_count`; routes must not run unconditional full counts on
  the request path to fill it.
- `meta` is always present: `as_of` and `as_of_token` on routes that read
  snapshot-pinned chain-derived state; control-plane routes (`/v2/status`,
  `/v2/namespaces/{namespace}`), verified name-profile responses served by the
  route-local on-demand fallback, and primary-name responses served by the
  route-local on-demand fallback omit both. `completeness`,
  `unsupported_fields`, and `unsupported_reason` only when the read is not clean;
  `source` when the route supports
  `?source=`. There is no `meta` query parameter — no `meta=full` (deeper
  detail is a diagnostics route, not a query knob) and no stripped variant,
  so the envelope never changes shape per request. `meta` is one small
  response-level object (not per-record); the feed latency path tolerates it.
- `unsupported_fields` appears at two levels with disjoint meanings and no
  duplication: `meta.unsupported_fields` names response-level sections or
  expansions the route could not serve (e.g. an `include=` section);
  record-level `unsupported_fields` names data fields the index could not
  prove for that record. An entry never appears at both levels for one
  response.
- There are no `declared_state`/`verified_state` parallel trees and no
  `both` mode. `source=indexed` returns indexed values; `source=verified`
  returns the same shape from verified execution, with `meta.source`
  identifying the answer's origin. The records route also accepts
  `source=auto` — `v1`'s `mode=auto` value-source policy, kept for record
  panels: indexed values where replay-safe, verified execution fills explicit
  gaps and unretained values for supported selectors. Same response shape;
  `meta.source` reports `auto`, and per-key origin lives on the diagnostics
  records route. Indexed-vs-verified side-by-side
  comparison is a diagnostics read
  (`GET /v2/diagnostics/names/{name}/records`), not a product-route mode —
  a third top-level member only in one mode would break the one-envelope
  guarantee. No permanently-null required fields.
- On `GET /v2/names/{name}`, `source=verified` applies to the flat shape's
  resolver-record-backed fields (`addresses`, `text_records`, `content_hash`,
  and `primary_address`). Registration and identity summary fields remain the
  indexed projection values because verified resolution executes record
  selectors, not registration state.
- `view` does not exist in `v2`.

The flat record shape (used by `/v2/lookup` detail results, `/v2/names/{name}`,
and as the row shape on subname and address-name collections):

```
name, display_name, namespace, namehash, chain_id, network,
owner, manager, registrant, expires_at, registered_at, created_at,
resolver: {chain_id, address}, primary_name, primary_address,
addresses: {"60": "0x..."}, text_records: {...}, content_hash,
token_id, registration_id, status, unsupported_reason, failure_reason,
unsupported_fields
```

`primary_address` is the relevant primary coin-type address from `addresses`
for the row context — coin type `60` on name-profile reads and the requested
primary-name coin type on reverse/detail reads. It is a partner-1 required
field carried by `v1`'s `IdentityRecord` and kept here.

Reverse results add `is_primary` and `relations` (the subset of
`owner`/`manager`/`registrant` that matched). Optional fields are omitted when
no backed value exists; `unsupported_fields` lists fields the index could not
prove without inventing a value — the explicit-unsupported guarantee is
unchanged, only its spelling.

`profile=feed` on the lookup route is a field budget over this same record
shape, not a second DTO: it returns the record object restricted to a
documented core-field subset (identity fields, `is_primary`/`relations`,
`status`), and every feed field is identical in name and type to its detail
counterpart. Reverse feed and detail requests use the same cursor and
`page_size` semantics; the latency contract is preserved by returning fewer
fields, not different ones.

Name-history rows use a dedicated lean product shape:
`{type, name, namespace, registration_id, block_number, timestamp,
transaction_hash, log_index}`. They do not include before/after state, raw
normalized-event payloads, or a `data` change object. Product event rows use
the friendly `type` vocabulary (`registration` from `RegistrationGranted` and
`LabelRegistered`; `renewal` from `RegistrationRenewed`; `release` from
`RegistrationReleased`; `expiry` from `ExpiryChanged`; `transfer` from
`TokenControlTransferred`; `authority` from `AuthorityTransferred` and
`AuthorityEpochChanged`; `resolver` from `ResolverChanged`; `record` from
`RecordChanged` and
`RecordVersionChanged`; `primary_name` from `ReverseChanged`; `permission`
from `PermissionChanged`, `PermissionScopeChanged`, `RolesChanged`, and
`EACRolesChanged`). Raw upstream event kinds are diagnostics-only.
Permission rows use `{address, grant_scope, powers, registration_id, name}`;
`?include=lineage` adds grant/revocation lineage and inheritance/transfer
behavior per row.

### Parameters

| Parameter | Applies to | Values |
| --- | --- | --- |
| `at` | Tier-2 projection reads (not the lookup primitive — see below) | RFC 3339 timestamp (selects the snapshot at or before it), or the URL-safe opaque snapshot token from a previous response's `meta.as_of_token` (pins exact per-chain positions) |
| `finality` | projection-read routes | `latest` (default), `safe`, `finalized` |
| `source` | names, records, primary-name | names and records use `indexed` (default) or `verified`; the records route also accepts `auto`; primary-name omits `source` to return all supported source answers and may use `indexed` or `verified` to request a subset |
| `namespace` | name-inferred, address-anchored, and collection routes | explicit override / filter |
| `include` | route-documented expansions | per-route allowlist |
| `sort`, `order` | every paginated route | route-documented field set + `asc`/`desc`; one style |
| `cursor`, `page_size` | every paginated route | opaque cursor; default 50, max 200 |

Rules:

- Snapshot selection (`at` + `finality`) is uniform across projection-read
  routes. Exact multi-chain block pinning stays on product routes: every
  chain-derived response carries `meta.as_of_token`, so snapshot reads can be
  replayed at exactly the positions they were served from (the determinism tool
  for the parity diff harness and shadow comparison). `meta.as_of` remains the
  readable per-chain attribution object; callers do not reconstruct tokens from
  it. What dies is `v1`'s separate `chain_positions` query parameter — one
  selector parameter, not two.
- The first paginated collection routes, `GET /v2/names/{name}/subnames` and
  `GET /v2/names/{name}/history`, accept `at` and `finality` and use them to
  resolve the parent name plus `meta.as_of`/`meta.as_of_token`, but the
  collection rows currently read the latest projection/history. True as-of
  child and history enumeration is deferred to storage follow-up work.
- `GET /v2/addresses/{address}/names` also accepts `at` and `finality` only
  for `meta.as_of`/`meta.as_of_token` resolution. The address anchor has no
  parent resource to resolve or 404, and collection rows read the latest
  address-name projection; true as-of address-name enumeration is deferred to
  storage follow-up work.
- Cursors are opaque and versioned but not bound to the route path string, so
  route evolution does not invalidate outstanding cursors. Cursors remain
  stable under replay for the same snapshot.
- No advertised-but-rejected parameters. If a filter is unimplemented it is
  absent from the contract and listed under deferred capabilities in
  `docs/consumer-capabilities.md`, not reserved in the schema.
- `POST /v2/lookup` body: `{inputs: [...], profile, namespace?}`, where each
  input is `{id?, name}` or
  `{id?, address, coin_type?, relation?, page_size?, cursor?}`.
  Reverse inputs default to `coin_type=60` when omitted. Input order is
  preserved, one result per input; each result echoes its `input` including
  the caller-supplied correlation `id`, and, for name inputs, carries
  `normalization` metadata (`changed`, `input_name`, `reason`) — both
  preserved from `v1`'s result-level contract. The lookup
  primitive is a current-state read: it does not accept `at`/`finality`, and
  `meta.as_of`/`meta.as_of_token` record the served positions for staleness
  attribution and shadow-diff correlation. Batch limit 1000
  (configurable via `BIGNAME_API_LOOKUP_BATCH_LIMIT`, which renames
  `BIGNAME_API_IDENTITY_BATCH_LIMIT`). `profile=shadow` is
  removed from the public contract; it survives as an undocumented
  compatibility alias of `detail` until partner-1 shadow comparison completes.
- `GET /v2/addresses/{address}/primary-name` is also a current-state read. It
  does not accept `at` or `finality`; its `meta.as_of`/`meta.as_of_token`
  record the served positions for staleness attribution and shadow-diff
  correlation when the answer comes from persisted snapshot state. When the
  ENS/60 route-local on-demand fallback supplies the answer, the response omits
  `meta.as_of` and `meta.as_of_token`. Basenames responses that serve a
  persisted verified answer include both the Base authority position and the
  Ethereum resolution-auxiliary position; indexed-only responses and missing
  persisted verified outcomes remain Base-scoped.

### Error model and statuses

The error envelope is unchanged: `{"error": {"code", "message", "details"}}`.
The mapping is fixed and uniform:

| Code | HTTP | Meaning |
| --- | --- | --- |
| `invalid_input` | 400 | malformed input, unnormalizable path name, bad parameter combination |
| `not_found` | 404 | single-resource GET with no answer |
| `unsupported` | 422 | the route cannot produce its contract for this input |
| `stale` | 409 | coherent selector not yet served by projections |
| `conflict` | 409 | selector cannot form one canonical snapshot |
| `internal_error` | 500 | unexpected failure |

Rules:

- `unsupported` moves from 400 to 422; 409 carries two distinct codes.
- `verification_failed` is deleted (it was never emitted). Verified-execution
  failures surface as `status: "failed"` on the affected section with a
  `failure_reason`, or `stale` when the RPC provider cannot serve the selected
  block.
- One not-found philosophy: single-resource GETs return 404; collections return
  200 with empty `data`; batch lookup results carry in-band `status` per input
  (a batch never 404s). Empty arrays and empty maps mean known-empty, never
  unknown. The
  primary-name route is the documented exception to the 404 rule: a valid
  `{address, coin_type, namespace}` tuple with no claim, or an
  unsupported/mismatched verification, is an answer about that tuple rather
  than a missing resource — it returns 200 with in-band `status`
  (`not_found`, `unsupported`, `mismatch`) on the answer and verification
  sections, matching `v1`'s conformance-tested behavior.
- One result-status vocabulary everywhere: `ok`, `not_found`, `invalid_name`,
  `mismatch`, `unsupported`, `stale`, `failed`, with `unsupported_reason`
  required when `unsupported` and `failure_reason` permitted on
  `failed`/`stale`/`not_found`/`mismatch`. `mismatch` is the verification state
  where a claimed answer verifies to a different value (claimed-vs-verified
  primary names) — kept from `v1`, where dropping it would misreport as `not_found`
  or `failed`.
- Error messages must not name internal tables, projections, or sidecars.

### Deleted wire surface

Gone from `v2` with no replacement: `resource_hex` and `role_bitmap` (hardcoded
`null` in `v1`), `authority_epoch` (documented, never emitted),
`verification_failed` (documented, never emitted), reserved `view=full`
parameters, the `resolved_address` and `role_bitmap` advertised-but-rejected
filters, the reserved `/v1/events` parameter block, the `resource` vs
`resource_id` alias pair, `contains_nocase`, top-level and section
`provenance` on product routes (diagnostics-only), and routine
`normalized_name`/`canonical_display_name` peers.

### What this ADR deliberately keeps

- Snapshot-pinned reads, multi-chain coherence, and `stale`/`conflict`
  semantics — renamed (`finality`, `as_of`), not removed.
- Verified execution, CCIP-Read support, Basenames L1-transport-assisted
  verified reads (`base-mainnet` → `ethereum-mainnet` through the L1
  Resolver (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)
  (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc)), and
  on-demand execution on the records/profile/primary-name paths — behind
  `source=`. Implementation note: v2 `/records` wires on-demand verified
  execution behind `source=verified` and verified fallback from `source=auto`;
  v2 `/names/{name}` uses the same verified execution path for its
  resolver-record-backed flat fields.
- Explicit unsupported semantics — as `meta.completeness`,
  `unsupported_fields`, and per-item `status`, with the full taxonomy intact on
  diagnostics routes. "Never silent omission" is unchanged.
- History, events, subnames, permissions, resolver overview, namespace
  metadata — reshaped, not cut.
- Namespace inference with explicit override, per ADR 0003's namespace rules,
  now uniform instead of route-exceptional.

This answers the open items in
`docs/internal/api-surface-flattening-scope-decisions.md`: provenance public —
no, diagnostics-only (Q3); coverage public — simplified on product routes, full
taxonomy on diagnostics (Q4); one envelope — yes (Q5); record cache metadata —
diagnostics-only (Q8); selector-state distinctions — kept, via the unified
status vocabulary (Q9); execution traces — diagnostics-only (Q12); search/exact
split — yes (Q20); typed unsupported inside 200s — yes, as `meta` +
per-item `status` (Q31); `view=full` — deleted (Q33); `mode` public — renamed
`source`, only on routes where verified execution is first-class (Q34);
canonicality explicit in public behavior — yes, via `stale`/`conflict` (Q39);
coherent multi-chain snapshots — yes, with exact pinning preserved on product
routes via `meta.as_of_token` and the `at` snapshot token
(Q40); normalized events as public semantics — no: history and events are
route-owned compact DTOs, and raw normalized-event rows and kinds stay
diagnostics-only (Q37). It also supersedes Q32's earlier `Yes`: `meta=full`
does not survive on product routes; deep metadata is a diagnostics route.
Q36 (raw-fact retention) is storage policy, not API surface — this ADR
deliberately leaves it open.

## Upstream anchors

This ADR introduces no new claims about ENSv1, ENSv2, or Basenames behavior. It
renames and restructures bigname-owned API vocabulary only. The ENSIP-15
normalization boundary, namespace inference rules, resolver-profile gating, and
all upstream-anchored semantics in `docs/api-v1.md` and
`docs/consumer-capabilities.md` carry forward unchanged with their existing
citations. The `finality` vocabulary adopts the Ethereum JSON-RPC block-tag
terms already used by `consistency`'s `safe`/`finalized` values.

Two existing claims are restated and cited in place. The namespace-inference
rule (§ Route catalog): exact `base.eth` resolves under `ens` because
upstream handles it through the Mainnet L1Resolver
(upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc),
while `*.base.eth` is the Base-issued `basenames` space
(upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) — the same
anchors carried by `docs/architecture.md` § Namespaces. And in § "What this
ADR deliberately keeps": Basenames verified reads use the L1 transport path from
`base-mainnet` to `ethereum-mainnet` through the L1 Resolver
(upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)
(upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) — L69 states
the L1 resolver's cross-chain role for the `base.eth` 2LD and L22 pins the
deployed L1Resolver. The same claim is anchored more broadly in
`docs/api-v1.md`. No divergence is created.

## Consequences

Positive:

- One vocabulary and one envelope: a consumer learns the record shape and the
  `data`/`page`/`meta` contract once. The partner record shape and the app
  profile shape become the same object.
- The partner path is the front door (`POST /v2/lookup`), not a side-route
  exception, and its proven latency characteristics are unaffected (the
  feed-profile read path and sidecars do not change).
- Pipeline internals stop leaking: product DTOs carry no projection, manifest,
  or lineage vocabulary; diagnostics routes own all of it explicitly.
- Contract rot is eliminated structurally: no reserved parameters, no
  documented-but-never-emitted fields or codes, and generated OpenAPI continues
  to derive from the route table.
- `docs/api-v1.md` ceases to grow; the new contract docs start from the
  dictionary and one envelope instead of per-route exceptions, and take over
  the `api-v1` names at the switch.
- The public API stays at `v1` permanently — no `/v2` prefix encoding the
  migration history of zero clients into every future URL.

Negative / trade-offs:

- The old `v1` keeps serving unchanged until the new surface passes the
  parity gate, then the switch replaces it in one release — not a migration
  window. Anyone who integrates against the old `v1` in the interim breaks at
  the switch; accepted because the consumer count is ~zero and the old `v1`
  docs stop being advertised at ADR acceptance.
- The one-time parity gate is the only safety check before the old contract
  disappears; there is no fallback window to catch a missed capability after
  the switch. The gate's three checks (capability mapping, same-snapshot value
  equivalence, partner latency) are therefore hard requirements, not advisory.
- Some field names originate in projection rows in `crates/storage`
  (`declared_summary` passthroughs); `v2` requires a mapping layer at the API
  boundary or projection-side renames, coordinated with Storage and Domain.
- Diagnostics routes become load-bearing for operators and shadow comparison;
  they need the same contract discipline as product routes, just a different
  audience.
- Folding four roles/permissions routes into one flat `GET /v2/permissions`
  narrows specialist query shapes; name-, registration-, and account-anchored
  reads become filters on that route (plus
  `/v2/addresses/{address}/names?include=role_summary`), which must be
  validated against the roles-page capability before `/v1/roles` is removed.

New failure modes:

- A single envelope serializer bug affects every route at once; envelope
  conformance tests are required from the first slice.
- Namespace inference on all name routes makes the inference table
  correctness-critical; it needs exhaustive tests including the `base.eth`
  exact-match exception.
- Re-using the `v1` label means two different `v1` contracts exist across the
  repo's history; any cached or forked copy of the old docs silently describes
  a dead contract. Mitigated by archiving the old docs with a pointer to this
  ADR and the switch date.

## Rollout

Doc-first, then code-concurrent slices. Ownership follows
`docs/internal/workstreams.md`: Projections and API own routes, DTOs, OpenAPI,
and contract tests; Storage and Domain review the boundary mapping for
projection-originated field names; Verified Execution reviews `source=` and
diagnostics execution surfaces; Conformance and Fixtures own capability-mapping
tests.

1. Accept or revise this ADR; record the outcome as the Final Direction in
   `docs/internal/api-surface-flattening-scope-decisions.md`.
2. Write the new contract docs from the dictionary and route catalog above —
   maintained as `docs/api-v2.md` / `docs/api-v2-routes.md` during development
   and renamed to the `api-v1` names at the switch; generate the OpenAPI from
   the route table. The existing `docs/api-v1.md` is frozen except for
   corrections until then.
3. Implement `v2` routes over the existing read layer (the shared exact-name
   funnel in `apps/api/src/support/snapshots.rs` and the route-definition
   table). ADR 0003 slices 3–6 (snapshot service, record read model,
   support-state consolidation) remain valid implementation work and become
   `v2` enablers; this ADR supplies the target model those slices were missing.
4. Add envelope-conformance, dictionary-conformance (no banned `v1` spellings
   on `v2` routes), and product-route denylist tests (no pipeline vocabulary)
   alongside the existing OpenAPI assertions.
5. One-time parity validation, with old and new surfaces registered in the
   same binary (old `/v1`, new `/v2`): every capability row in
   `docs/consumer-capabilities.md` is served by a mapped new route; a diff
   harness reads both surfaces at the same snapshot and proves value
   equivalence under the dictionary mapping; the partner latency benchmarks
   are rerun against the new lookup route. The gate checks against the
   capability matrix as frozen before this ADR (not the remapped one),
   includes error-path parity (status codes, not-found philosophy) alongside
   value equivalence, and the harness's dictionary mapping is itself
   review-gated before its results count. Results are recorded once — this
   gate is not a standing dual-serving arrangement, and it is the only
   safety check before the old contract disappears.
6. The switch, in one release: delete the old `v1` routes, rename the `/v2`
   prefix to `/v1` (route table, docs, and generated OpenAPI follow), and
   retire the old `v1` docs to an archived record pointing at this ADR. The
   `/v2` prefix never ships as a public contract; anything still reading
   old-`v1` semantics breaks at the switch by design.
7. Point the partner-1 shim and the first app integration at the re-baselined
   `v1`.

Sequencing with the 2026-06 remediation
(`docs/internal/remediation-2026-06-postmortem.md`, the closed-out record):
the remediation completes before `v2` implementation begins (planning decision,
2026-06-10). Steps 1–2 (docs only) conflict with nothing and may proceed
during the remediation; step 3 starts after the remediation closes out, so
`v2` inherits the corrected semantics (primary-name status classification,
ENSIP-15 input validation at the path boundary, SQL keyset pagination)
instead of re-fixing them. Because the remediation runs first, three
execution notes follow. The first two are advisory, not gates: remediation
work that `v2` later replaces is an accepted cost (planning decision,
2026-06-10), and the remediation does not wait on this ADR's acceptance.

1. WS-G's "accepted-but-inert `meta=full` / `include=record_summaries`" item
   may take either branch; the disclose-and-document branch is cheaper since
   `v2` deletes both knobs.
2. WS-G's pagination/cursor rewrite ideally targets the `v2` cursor rules
   (opaque, versioned, not route-path-bound); if it ships route-bound cursors
   instead, `v2` re-cuts them.
3. At remediation teardown, remaining P2/P3 `v1`-contract polish items are
   re-triaged against `v2` supersession: work that only reconciles `v1` knobs
   or docs that `v2` deletes is dropped with a rationale line, not done.

`v1` behavior fixes that converge implementation on the documented `v1`
contract are corrections, not contract changes — the `v1` freeze does not
block them.

## Alternatives considered

**Keep a permanent `/v2` prefix.** The conventional reading of
`docs/api-v1.md` § Versioning is that this change requires a `v2`. Rejected as
the end state: version prefixes exist for consumers, and the old `v1` has
none — shipping the new contract under `/v2` forever would encode the
migration history of zero clients into every future URL. The policy's intent
(no silent breaking changes for integrators) is honored by the parity gate and
by the fact that nobody integrated; this re-baseline is a one-time exception
recorded here, and the versioning policy applies unchanged to the re-baselined
`v1` from the switch onward.

**Reshape `v1` incrementally in place.** Cheapest on prefixes but worst on
truth: the frozen `v1` docs would be wrong for the whole transition, there is
no single parity gate, partial states would be observable, and several new
paths are grammatically ambiguous against old routes at the same prefix.
Rejected in favor of the wholesale switch, which keeps the docs accurate at
every moment — the old contract until the switch, the new one after.

**Keep the dual full/compact envelope model and only rename fields.** Renaming
alone fixes vocabulary but preserves eight envelope shapes, the `meta`-dependent
arity changes, and the `view`/`mode`/`meta` knob matrix — the structural half of
the fragmentation. Rejected because the envelope count, not the spelling, is
what forces consumers to learn three API philosophies.

**Adopt GraphQL for the product surface.** partner-1 accepts either transport,
and GraphQL would solve field selection. Rejected for now: the latency path is
already met with REST + sidecars, the app capability set is small and
enumerable, and a second query engine is a larger complexity budget than the
problem justifies. The flat record shape ports to GraphQL later if field
selection becomes a real need.

**Keep `resource_id` routes public.** Resource-anchored routes
(`/v1/resources/*`) expose the indexer's internal identity model as API
structure. Rejected: `registration_id` survives as a field and filter for the
stability guarantees consumers actually need (dedupe, permission joins), without
requiring callers to navigate an ontology the partner and app never asked for.

**Drop diagnostics from the public API entirely.** Largest simplification, but
it would delete the explain/coverage surfaces that shadow comparison and
operational debugging rely on, and ADR 0003 already rejected hiding
auditability. Rejected in favor of confining, not deleting.

## References

- `docs/adrs/0003-api-surface-flattening-plan.md` (target model this ADR
  completes; its compatibility-preservation policy is superseded by the
  replacement model here, while its implementation slices 3–6 remain valid
  enablers)
- `docs/adrs/0004-conceptual-deduplication-gate.md`
- `docs/internal/api-surface-flattening-scope-decisions.md`
- `docs/partners/partner-1-indexing-requirements.md`
- `docs/partners/partner-1-identity-facade-benchmarks.md`
- `docs/api-v1.md`, `docs/api-v1-routes.md`, `docs/consumer-capabilities.md`
