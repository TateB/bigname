# API v2 Routes

Per-route reference for the development-time `/v2` surface accepted in
[ADR 0006](adrs/0006-api-v2-product-surface.md). Contract principles,
dictionary, envelope, status vocabulary, finality rules, cursor rules, and
error shape live in [`api-v2.md`](api-v2.md).

Routes below use the `/v2` development prefix. At the switch, the prefix
becomes `/v1`; no permanent public `/v2` prefix ships.

`GET /healthz`, `GET /`, `GET /docs`, and `GET /openapi.json` remain
non-contract helpers.

## Shared Route Rules

Name-shaped routes infer the namespace from the name itself: exact `base.eth`
is `ens` because upstream treats it as the L1 root domain handled by the
Mainnet L1Resolver (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc);
`*.base.eth` is `basenames`, the Base-issued subdomain space
(upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc); other
supported names are `ens`. Name-shaped routes accept optional `namespace` as
an override and always echo the resolved `namespace` in the response.

Single-resource GETs return `404 not_found` when no answer exists. Collections
return `200` with empty `data` when the result set is known-empty. Batch lookup
uses in-band result `status` and never returns `404` for one missing input. The
primary-name route is the documented single-resource exception: valid tuples
with no claim, unsupported verification, or mismatched verification return
`200` with in-band `status`.

All collection routes use the standard `page` object: `cursor`,
`next_cursor`, `page_size`, nullable `total_count`, and `has_more`.

OPEN QUESTION (step-3 gate): ADR 0006's route catalog and shape snippets name
some route-local fields that are not entries in the naming dictionary. Step 3
must either classify them as route-local fields in the route-table schema or
extend the dictionary before generated OpenAPI is accepted. The record-shaped
fields are `namespace`, `namehash`, `primary_name`, `primary_address`,
`token_id`, `unsupported_fields`, `is_primary`, and `relations`.

OPEN QUESTION (step-3 gate): Lookup result fields outside the dictionary need
route-table ownership: `id`, `input`, `normalization`, `changed`,
`input_name`, `reason`, `coin_type`, `profile`, `inputs`, `records`, and
the primary-name `verification` summary.

OPEN QUESTION (step-3 gate): Route-specific parameter names outside the
dictionary need route-table ownership: `keys`, `q`, `match`, and `dedupe`.

OPEN QUESTION (step-3 gate): Event row fields outside the dictionary need
route-table ownership: `type`, `block_number`, `timestamp`,
`transaction_hash`, `log_index`, `data`, and `address`.

OPEN QUESTION (step-3 gate): The `/v2/status` ops shape is route-local and
uses fields outside the dictionary: `chains`, `latest_block`, `indexed_block`,
`safe_block`, `finalized_block`, `lag_blocks`, and `lag_seconds`.

## Tier 1: Lookup Primitives

### `POST /v2/lookup`

- Method/path: `POST /v2/lookup`
- Tier: lookup primitive.
- Purpose: batched forward name-to-record and reverse address-plus-coin-type
  resolution. `profile=feed` is the latency path; `profile=detail` returns
  full records.
- Request parameters: body `{inputs, profile, namespace?}`. Each input is
  `{id?, name}` or `{id?, address, coin_type?, relation?, page_size?, cursor?}`.
  Reverse inputs default to `coin_type=60` when omitted. Batch limit is 1000
  and is configurable with `BIGNAME_API_LOOKUP_BATCH_LIMIT`.
- Response shape: the common envelope. `data` contains one result per input in
  caller order. Each result echoes its `input`, including the optional
  correlation `id`. Name inputs may carry `normalization` metadata. Name
  results carry one record object. Reverse results carry record rows with
  `is_primary` and `relations` in addition to the shared record fields.
  `profile=feed` returns a documented core-field subset of the same record
  object; it does not introduce another DTO.
- Pagination behavior: top-level `page` is absent. Reverse inputs use the
  standard `page` object inside each result.
- Status semantics: per-result `status` uses the common result vocabulary.
  Name misses are in-band `not_found`; invalid names are in-band
  `invalid_name`. Reverse misses return an empty record array for the input.
- Replaces (v1): `POST /v1/identity:lookup`.

OPEN QUESTION (step-3 gate): ADR 0006 requires the one envelope but does not
state whether lookup `data` is an array of result objects or an object
containing a result array. Step 3 must choose one shape before OpenAPI
generation.

### `GET /v2/status`

- Method/path: `GET /v2/status`
- Tier: lookup primitive.
- Purpose: per-chain indexing readiness.
- Request parameters: none.
- Response shape: `data.status` plus `data.chains`, keyed by `chain_id`.
  Each chain entry carries `latest_block`, `indexed_block`, `safe_block`,
  `finalized_block`, `lag_blocks`, `lag_seconds`, and route-local ops
  `status`.
- Pagination behavior: none.
- Status semantics: route-local ops `status` is `ready`, `degraded`, or
  `stale`. This is the only non-result `status` enum in `v2`.
- Replaces (v1): `GET /v1/status`.

## Tier 2: Product Reads

### `GET /v2/names/{name}`

- Method/path: `GET /v2/names/{name}`
- Tier: product read.
- Purpose: name profile, using the flat record shape plus registration summary.
- Request parameters: path `name`; query `namespace`, `at`, `finality`,
  `source`.
- Response shape: `data` is one record object using dictionary fields for
  ownership, resolver, records, registration handle, timestamps, and status.
  It also echoes the resolved `namespace` per the shared namespace inference
  rule.
- Pagination behavior: none.
- Status semantics: valid names with no profile return `404 not_found`.
  Invalid path names return `400 invalid_input`.
- Replaces (v1): `GET /v1/names/{namespace}/{name}` and
  `GET /v1/profiles/names/{name}`.

OPEN QUESTION (step-3 gate): ADR 0006 says this route returns a registration
summary but does not enumerate the exact registration-summary fields beyond the
dictionary fields such as `registration_id`, `registered_at`, `expires_at`,
`owner`, `manager`, and `registrant`.

### `GET /v2/names/{name}/records`

- Method/path: `GET /v2/names/{name}/records`
- Tier: product read.
- Purpose: resolver records.
- Request parameters: path `name`; query `namespace`, `at`, `finality`,
  `source=indexed|verified|auto`, `keys`, `include=inventory`.
- Response shape: `data` returns resolver record values using `resolver`,
  `addresses`, `text_records`, and `content_hash`. `source=auto` keeps the
  replay-safe-cache-with-verified-fallback policy. `include=inventory` adds the
  known selector space and unset keys in product vocabulary; deep inventory
  internals stay on diagnostics.
- Pagination behavior: none.
- Status semantics: a missing name returns `404 not_found`. Missing, unset, or
  unsupported requested record values are reported with the common result
  `status` vocabulary inside the record answer rather than by changing the
  envelope.
- Replaces (v1): `GET /v1/names/{namespace}/{name}/records` and record
  sections of `GET /v1/profiles/names/{name}`.

OPEN QUESTION (step-3 gate): ADR 0006 does not define the exact per-key record
answer shape for `keys` or `include=inventory`; step 3 must define it without
reintroducing `declared_state`, `verified_state`, or `mode=both`.

### `GET /v2/names/{name}/subnames`

- Method/path: `GET /v2/names/{name}/subnames`
- Tier: product read.
- Purpose: direct subnames.
- Request parameters: path `name`; query `namespace`, `at`, `finality`,
  `include=counts`, `cursor`, `page_size`.
- Response shape: `data` is an array of record-shaped subname rows in
  dictionary vocabulary. `include=counts` adds documented count fields where
  supported.
- Pagination behavior: standard collection pagination.
- Status semantics: no direct subnames returns `200` with empty `data`.
  Missing parent names return `404 not_found`.
- Replaces (v1): `GET /v1/names/{namespace}/{name}/children`.

OPEN QUESTION (step-3 gate): ADR 0006 preserves child counts but does not name
the count field in the dictionary. Step 3 must define the `include=counts`
field without reusing v1-only spellings accidentally.

### `GET /v2/names/{name}/history`

- Method/path: `GET /v2/names/{name}/history`
- Tier: product read.
- Purpose: name history.
- Request parameters: path `name`; query `namespace`, `at`, `finality`,
  `scope=name|registration|both`, `cursor`, `page_size`.
- Response shape: `data` is an array of compact event rows with friendly
  `type` vocabulary: `registration`, `renewal`, `transfer`, `authority`,
  `resolver`, `record`, `primary_name`, `permission`.
- Pagination behavior: standard collection pagination.
- Status semantics: no matching history returns `200` with empty `data`.
  Missing names return `404 not_found`.
- Replaces (v1): `GET /v1/history/names/{namespace}/{name}`.

OPEN QUESTION (step-3 gate): ADR 0006 maps history `scope=registration` to the
old resource-oriented concept but does not state whether this route fully
replaces direct `GET /v1/history/resources/{resource_id}` for registrations no
longer reachable from a name.

### `GET /v2/permissions`

- Method/path: `GET /v2/permissions`
- Tier: product read.
- Purpose: flat permission rows by name, registration, or address, including
  registrations that are no longer a name's current one.
- Request parameters: at least one of `name`, `registration_id`, or `address`;
  filters are combinable. Query `namespace`, `at`, `finality`,
  `include=lineage`, `cursor`, `page_size`.
- Response shape: `data` is an array of permission rows
  `{address, grant_scope, powers, registration_id, name}`. `include=lineage`
  adds per-row grant/revocation lineage and inheritance/transfer behavior.
- Pagination behavior: standard collection pagination.
- Status semantics: no matching permission rows returns `200` with empty
  `data`. Unsupported filter combinations return `422 unsupported`.
- Replaces (v1): `GET /v1/resources/{resource_id}/permissions`,
  `GET /v1/roles`, `GET /v1/names/{namespace}/{name}/roles`, and
  `GET /v1/resources/lookup`.

OPEN QUESTION (step-3 gate): ADR 0006 names the lineage expansion but does not
pin exact field names for grant/revocation lineage, inheritance, or transfer
behavior.

### `GET /v2/addresses/{address}/names`

- Method/path: `GET /v2/addresses/{address}/names`
- Tier: product read.
- Purpose: names related to an address.
- Request parameters: path `address`; query `namespace`, `at`, `finality`,
  `relation`, `q`, `sort=name|expires_at|registered_at`,
  `dedupe=name|registration`, `include=role_summary`, `cursor`, `page_size`.
- Response shape: `data` is an array of record-shaped rows. Reverse rows add
  `is_primary` and `relations`, where `relations` is the subset of
  `owner`, `manager`, and `registrant` that matched. `include=role_summary`
  adds the documented role summary in product vocabulary.
- Pagination behavior: standard collection pagination.
- Status semantics: no related names returns `200` with empty `data`.
  Malformed addresses return `400 invalid_input`.
- Replaces (v1): `GET /v1/addresses/{address}/names` and address-relation
  uses of `GET /v1/names`.

OPEN QUESTION (step-3 gate): ADR 0006 keeps the role-summary expansion but
does not enumerate its exact v2 fields beyond the dictionary terms
`grant_scope` and `powers`.

### `GET /v2/addresses/{address}/primary-name`

- Method/path: `GET /v2/addresses/{address}/primary-name`
- Tier: product read.
- Purpose: primary name for an address.
- Request parameters: path `address`; query `coin_type` default `60`,
  `namespace` default `ens`, and `source`.
- Response shape: `data` returns one answer per `source` plus a typed
  `verification` summary `{status, name}` whenever a persisted or on-demand
  verified outcome exists. Claimed-vs-verified remains one call without
  parallel state trees.
- Pagination behavior: none.
- Status semantics: valid tuples with no claim return `200` with in-band
  `status=not_found`. Unsupported and mismatched verification outcomes also
  return `200` with in-band `status`. Malformed addresses return
  `400 invalid_input`.
- Replaces (v1): `GET /v1/primary-names/{address}`.

OPEN QUESTION (step-3 gate): ADR 0006 says the route returns one answer per
`source` but does not define the containing field name or array/object shape
for those answers.

### `GET /v2/addresses/{address}/history`

- Method/path: `GET /v2/addresses/{address}/history`
- Tier: product read.
- Purpose: address activity history.
- Request parameters: path `address`; query `namespace`, `at`, `finality`,
  `relation`, `scope=name|registration|both`, `cursor`, `page_size`.
- Response shape: `data` is an array of compact event rows using the shared
  friendly `type` vocabulary.
- Pagination behavior: standard collection pagination.
- Status semantics: no matching activity returns `200` with empty `data`.
  Malformed addresses return `400 invalid_input`.
- Replaces (v1): `GET /v1/history/addresses/{address}`.

### `GET /v2/search`

- Method/path: `GET /v2/search`
- Tier: product read.
- Purpose: name search and suggestions. No availability or pricing semantics.
- Request parameters: query `q`, `match=prefix|contains` default `prefix`,
  `namespace`, `cursor`, `page_size`.
- Response shape: `data` is an array of record-shaped name search results in
  dictionary vocabulary.
- Pagination behavior: standard collection pagination.
- Status semantics: no matches returns `200` with empty `data`.
- Replaces (v1): search, suggestion, and exact-name-filter uses of
  `GET /v1/names`; exact name profiles move to `GET /v2/names/{name}`.

### `GET /v2/events`

- Method/path: `GET /v2/events`
- Tier: product read.
- Purpose: compact event search across name, address, registration, type, and
  block filters.
- Request parameters: query filters for name, address, registration, event
  type, and block range; `cursor`, `page_size`.
- Response shape: `data` is an array of compact event rows with friendly
  `type` vocabulary. Raw upstream event kinds are diagnostics-only.
- Pagination behavior: standard collection pagination.
- Status semantics: no matching events returns `200` with empty `data`.
  Malformed filters return `400 invalid_input`.
- Replaces (v1): `GET /v1/events` compact event search.

OPEN QUESTION (step-3 gate): ADR 0006 says this route supports name, address,
registration, type, and block filters, but it does not restate the exact
registration or block filter parameter names.

### `GET /v2/resolvers/{chain_id}/{address}`

- Method/path: `GET /v2/resolvers/{chain_id}/{address}`
- Tier: product read.
- Purpose: resolver overview for numeric `chain_id` and resolver `address`.
- Request parameters: path `chain_id`, `address`; query `include` for
  route-documented sections, `cursor`, `page_size`.
- Response shape: `data` is a resolver overview in product vocabulary. The
  route includes a paginated bound-names section that replaces resolver-based
  name filtering.
- Pagination behavior: standard collection pagination applies to the
  bound-names section.
- Status semantics: no bound names returns `200` with an empty bound-names
  section. Malformed `chain_id` or `address` returns `400 invalid_input`.
- Replaces (v1): `GET /v1/resolvers/{chain_id}/{resolver_address}/overview`
  and the `GET /v1/names?resolver=...` filter.

OPEN QUESTION (step-3 gate): ADR 0006 does not define whether an otherwise
valid resolver with no overview row is `404 not_found` or `200` with empty
sections.

### `GET /v2/namespaces/{namespace}`

- Method/path: `GET /v2/namespaces/{namespace}`
- Tier: product read.
- Purpose: namespace metadata and supported-capability summary in product
  vocabulary.
- Request parameters: path `namespace`.
- Response shape: `data` is the namespace metadata summary. Control-plane
  metadata omits `meta.as_of`.
- Pagination behavior: none.
- Status semantics: unsupported public namespaces return `404 not_found`.
- Replaces (v1): `GET /v1/namespaces/{namespace}`. Manifest internals move to
  `GET /v2/diagnostics/namespaces/{namespace}/manifests`.

OPEN QUESTION (step-3 gate): ADR 0006 does not enumerate the product
capability-summary fields for namespace metadata.

## Tier 3: Diagnostics

Diagnostics are the only routes that may carry pipeline vocabulary. Product
route vocabulary restrictions do not apply to the diagnostic payloads below.

OPEN QUESTION (step-3 gate): ADR 0006 does not say whether diagnostic
projection reads accept `at` and `finality`, or whether they carry
`meta.as_of`. Step 3 must decide this consistently before route-table
implementation.

### `GET /v2/diagnostics/names/{name}/coverage`

- Method/path: `GET /v2/diagnostics/names/{name}/coverage`
- Tier: diagnostics.
- Purpose: full coverage taxonomy.
- Request parameters: path `name`; query `namespace`. Snapshot parameters are
  covered by the diagnostics open question above.
- Response shape: `data` includes `exhaustiveness`, `enumeration_basis`,
  `source_classes_considered`, and `unsupported_reason` detail.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`; unsupported coverage
  classes return diagnostic detail rather than product simplification.
- Replaces (v1): `GET /v1/coverage/{namespace}/{name}`.

### `GET /v2/diagnostics/names/{name}/binding`

- Method/path: `GET /v2/diagnostics/names/{name}/binding`
- Tier: diagnostics.
- Purpose: surface-binding explain.
- Request parameters: path `name`; query `namespace`. Snapshot parameters are
  covered by the diagnostics open question above.
- Response shape: `data` includes binding ids, binding kind, and anchors.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`.
- Replaces (v1): `GET /v1/explain/names/{namespace}/{name}/surface-binding`.

### `GET /v2/diagnostics/names/{name}/authority`

- Method/path: `GET /v2/diagnostics/names/{name}/authority`
- Tier: diagnostics.
- Purpose: authority/control explain.
- Request parameters: path `name`; query `namespace`. Snapshot parameters are
  covered by the diagnostics open question above.
- Response shape: `data` includes token lineage, control vectors, and
  permission lineage.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`.
- Replaces (v1): `GET /v1/explain/names/{namespace}/{name}/authority-control`.

### `GET /v2/diagnostics/names/{name}/records`

- Method/path: `GET /v2/diagnostics/names/{name}/records`
- Tier: diagnostics.
- Purpose: record inventory and cache internals.
- Request parameters: path `name`; query `namespace`. Snapshot parameters are
  covered by the diagnostics open question above.
- Response shape: `data` includes selectors, explicit gaps, unsupported
  families, version boundaries, value sources, and indexed-vs-verified
  side-by-side comparison.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`.
- Replaces (v1): record inventory/cache diagnostics formerly embedded in
  `GET /v1/profiles/names/{name}` and
  `GET /v1/names/{namespace}/{name}/records`, including the former
  `mode=both` comparison.

OPEN QUESTION (step-3 gate): ADR 0006 names the diagnostic record sections but
does not pin their exact field-level schema.

### `GET /v2/diagnostics/names/{name}/execution`

- Method/path: `GET /v2/diagnostics/names/{name}/execution`
- Tier: diagnostics.
- Purpose: persisted verified-execution explain.
- Request parameters: path `name`; query `namespace`. Snapshot parameters and
  selector request shape are covered by open questions.
- Response shape: `data` includes trace id, steps, digests, and CCIP
  participation.
- Pagination behavior: none.
- Status semantics: missing persisted execution artifacts return
  `404 not_found`.
- Replaces (v1): `GET /v1/explain/resolutions/{namespace}/{name}/execution`.

OPEN QUESTION (step-3 gate): ADR 0006 does not define how callers select the
persisted execution artifact when more than one selector set or source exists.

### `GET /v2/diagnostics/namespaces/{namespace}/manifests`

- Method/path: `GET /v2/diagnostics/namespaces/{namespace}/manifests`
- Tier: diagnostics.
- Purpose: active manifest versions, source families, deployment epochs, and
  capability flags.
- Request parameters: path `namespace`.
- Response shape: `data` is the active manifest summary in diagnostics
  vocabulary.
- Pagination behavior: none.
- Status semantics: unsupported public namespaces return `404 not_found`.
- Replaces (v1): `GET /v1/manifests/{namespace}`.

### `GET /v2/diagnostics/events`

- Method/path: `GET /v2/diagnostics/events`
- Tier: diagnostics.
- Purpose: raw normalized-event rows: upstream event kinds, event identity, and
  full provenance.
- Request parameters: same filter concepts as `GET /v2/events`; `cursor`,
  `page_size`.
- Response shape: `data` is an array of raw normalized-event rows in
  diagnostics vocabulary.
- Pagination behavior: standard collection pagination.
- Status semantics: no matching rows returns `200` with empty `data`.
- Replaces (v1): `view=full` on `GET /v1/history/names/{namespace}/{name}`,
  `GET /v1/history/resources/{resource_id}`, and
  `GET /v1/history/addresses/{address}`.

OPEN QUESTION (step-3 gate): ADR 0006 says this route has the same filters as
`/v2/events` but does not restate the exact diagnostic event filter names or
raw row schema.
