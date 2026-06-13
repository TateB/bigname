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

Field ownership:

- Shared record, lookup, primary-name, event, and count concepts are dictionary
  fields in `api-v2.md`.
- Lookup-only transport fields are route-local: `id` is caller correlation
  inside the echoed `input`, `kind` is the result discriminator, `profile` and
  `inputs` are request controls, `record` holds a single name result, `records`
  holds reverse result rows, and `changed`, `input_name`, and `reason` live
  inside `normalization` only.
- Name-filter request fields are route-local: `q` is shared by search and
  address-name collections, `match` is search-only, and `dedupe` is
  address-name-only.
- Records-route containers are route-local: `records`, `inventory`,
  `known_keys`, `unset_keys`, `unsupported_keys`, and `value` are the per-key
  answer and inventory shape for one resolver-record route, not shared domain
  vocabulary.
- Permission lineage containers are route-local: `lineage`, `grant`,
  `revocation`, `inheritance_path`, and `transfer_behavior` exist only on
  `include=lineage` for `/v2/permissions`.
- Primary-name containers are route-local: `answers` holds the returned
  source answer entries, and `raw_claim_name` preserves an invalid reverse
  claim exactly as observed for that tuple.
- Role-summary containers are route-local: `grants` groups
  `{grant_scope, powers}` entries under one `address` inside
  `role_summary`.
- Namespace metadata containers are route-local: `networks` is the
  product-facing list of public chain mappings for one namespace.
- Resolver overview containers are route-local: `bound_names` is the nested
  names collection inside one resolver overview object.
- Ops status containers are route-local: `/v2/status` owns `chains`,
  `latest_block`, `indexed_block`, `safe_block`, `finalized_block`,
  `lag_blocks`, and `lag_seconds`.
- Diagnostic-only field names are route-local to diagnostics unless they are
  already dictionary fields. Diagnostics may use pipeline vocabulary because
  their tier is explicitly separate from product reads.

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
- Response shape: the common envelope. `data` is an array of result objects,
  not an object wrapper. The array contains one result per input in caller
  order. Each result is `{input, kind, status, normalization?, record?,
  records?, page?}`. `input` echoes the caller-supplied input, including `id`
  when supplied. `kind` is `name` or `address`. Name results use `record` for
  the single record object. Reverse results use `records` for zero or more
  record rows with `is_primary` and `relations` in addition to the shared
  record fields. `profile=feed` returns a documented core-field subset of the
  same record object; it does not introduce another DTO.
- Pagination behavior: top-level `page` is absent. Reverse inputs use the
  standard `page` object inside each result.
- Status semantics: per-result `status` uses the common result vocabulary.
  Name misses are in-band `not_found`; invalid names are in-band
  `invalid_name`. Reverse misses return `status=ok` with an empty `records`
  array for the input.
- Replaces (v1): `POST /v1/identity:lookup`.

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
- Response shape: `data` is one flat record object using dictionary fields.
  The registration summary is not nested; it is represented by
  `registration_id`, `token_id`, `owner`, `manager`, `registrant`,
  `registered_at`, `created_at`, `expires_at`, and `registration_status` on
  the same object. The profile portion uses `name`, `display_name`, `namespace`,
  `namehash`, `resolver`, `addresses`, `text_records`, `content_hash`,
  `primary_name`, `primary_address`, `chain_id`, `network`, `status`, and
  `unsupported_fields` when those fields are served. On a `200` profile,
  `status` is the flat-record result: `ok` for clean indexed reads; `failed`
  or `stale` may appear only when `source=verified` cannot serve the verified
  sections; `not_found` and `invalid_name` are unreachable in-record.
- Pagination behavior: none.
- Status semantics: valid names with no profile return `404 not_found`.
  Invalid path names return `400 invalid_input`.
- Replaces (v1): `GET /v1/names/{namespace}/{name}` and
  `GET /v1/profiles/names/{name}`.

### `GET /v2/names/{name}/records`

- Method/path: `GET /v2/names/{name}/records`
- Tier: product read.
- Purpose: resolver records.
- Request parameters: path `name`; query `namespace`, `at`, `finality`,
  `source=indexed|verified|auto`, `keys`, `include=inventory`.
- Response shape: `data` returns resolver record values using `resolver`,
  `addresses`, `text_records`, and `content_hash`. `keys` is a comma-separated
  record-key allowlist using the existing app key grammar: `addr:<coin_type>`,
  `text:<key>`, `avatar`, and `contenthash`. Requested-key outcomes are also
  returned in route-local `records`, keyed by the requested key; each value is
  `{status, value?, unsupported_reason?, failure_reason?}`. `source=auto` uses
  indexed values when they satisfy the requested keys and otherwise falls back
  to verified values for supported keys. `include=inventory` adds route-local
  `inventory: {known_keys, unset_keys, unsupported_keys}`. Deep inventory
  internals stay on diagnostics.
- Pagination behavior: none.
- Status semantics: a missing name returns `404 not_found`. Missing, unset, or
  unsupported requested record values are reported with the common result
  `status` vocabulary inside the record answer rather than by changing the
  envelope.
- Replaces (v1): `GET /v1/names/{namespace}/{name}/records` and record
  sections of `GET /v1/profiles/names/{name}`.

### `GET /v2/names/{name}/subnames`

- Method/path: `GET /v2/names/{name}/subnames`
- Tier: product read.
- Purpose: direct subnames.
- Request parameters: path `name`; query `namespace`, `at`, `finality`,
  `include=counts`, `cursor`, `page_size`.
- Response shape: `data` is an array of record-shaped subname rows in
  dictionary vocabulary. `include=counts` adds `subname_count` where
  supported.
- Pagination behavior: standard collection pagination.
- Status semantics: no direct subnames returns `200` with empty `data`.
  Missing parent names return `404 not_found`.
- Replaces (v1): `GET /v1/names/{namespace}/{name}/children`.

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
  Registration-id anchored history from `GET /v1/history/resources/{resource_id}`
  moves to `GET /v2/events?registration_id=...`. `scope=registration` on this
  route is limited to registration lifecycles associated with the requested
  name.

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
  adds route-local `lineage` per row:
  `{grant, revocation?, inheritance_path?, transfer_behavior?}`. `grant` and
  `revocation` identify the event or permission row that created or removed
  the powers; `inheritance_path` lists inherited grant scopes in order; and
  `transfer_behavior` describes whether the powers follow a registration
  transfer.
- Pagination behavior: standard collection pagination.
- Status semantics: no matching permission rows returns `200` with empty
  `data`. Unsupported filter combinations return `422 unsupported`.
- Replaces (v1): `GET /v1/resources/{resource_id}/permissions`,
  `GET /v1/roles`, `GET /v1/names/{namespace}/{name}/roles`, and
  `GET /v1/resources/lookup`.

### `GET /v2/addresses/{address}/names`

- Method/path: `GET /v2/addresses/{address}/names`
- Tier: product read.
- Purpose: names related to an address.
- Request parameters: path `address`; query `namespace`, `at`, `finality`,
  `relation`, `q`, `sort=name|expires_at|registered_at`, `order=asc|desc`,
  `dedupe=name|registration`, `include=role_summary`, `cursor`, `page_size`.
  `q` applies prefix matching to the dictionary `name` field; this route does
  not accept `match`.
- Response shape: `data` is an array of record-shaped rows. Reverse rows add
  `is_primary` and `relations`, where `relations` is the subset of
  `owner`, `manager`, and `registrant` that matched. `include=role_summary`
  adds `role_summary: [{address, grants}]`, where each `grants` entry is
  `{grant_scope, powers}`. The same expansion may add `subname_count`,
  `record_count`, `registration_status`, and `expires_at` when those fields
  are already available for the row.
- Pagination behavior: standard collection pagination.
- Status semantics: no related names returns `200` with empty `data`.
  Malformed addresses return `400 invalid_input`.
- Replaces (v1): `GET /v1/addresses/{address}/names` and address-relation
  uses of `GET /v1/names`.

### `GET /v2/addresses/{address}/primary-name`

- Method/path: `GET /v2/addresses/{address}/primary-name`
- Tier: product read.
- Purpose: primary name for an address.
- Request parameters: path `address`; query `coin_type` default `60`,
  `namespace` default `ens`, and `source`. This is a current-state read and
  does not accept `at` or `finality`.
- Response shape: `data` is
  `{address, coin_type, namespace, answers, verification?}`. `answers` is an
  array of `{source, status, name?, raw_claim_name?, unsupported_reason?,
  failure_reason?}` entries. When `source` is omitted, the route returns one
  entry for each answer source in stable `indexed`, then `verified` order;
  unsupported sources are represented by an entry with `status=unsupported`,
  not omitted.
  Supplying `source=indexed` or `source=verified` narrows the `answers` array
  to that source for single-source callers. `verification` is
  `{status, name?, unsupported_reason?, failure_reason?}` and appears whenever
  a persisted or on-demand verified outcome exists. The `verified` answer
  entry is the source-specific payload; `verification` is the typed comparison
  summary and must not contradict that entry. Claimed-vs-verified remains one
  call without `declared_state`/`verified_state`.
- Pagination behavior: none.
- Status semantics: answer entries use in-band `status`. Valid tuples with no
  indexed claim return an `indexed` entry with `status=not_found`. Unsupported,
  not-found, failed, and mismatched verified outcomes return `200` with the
  corresponding `verified` entry status. Malformed addresses return
  `400 invalid_input`.
- Replaces (v1): `GET /v1/primary-names/{address}`.

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
  `namespace`, `at`, `finality`, `cursor`, `page_size`.
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
- Request parameters: query `namespace`, `name`, `address`,
  `registration_id`, `type`, `from_block`, `to_block`, `at`, `finality`,
  `cursor`, and `page_size`.
- Response shape: `data` is an array of compact event rows with friendly
  `type` vocabulary. Raw upstream event kinds are diagnostics-only.
- Pagination behavior: standard collection pagination.
- Status semantics: no matching events returns `200` with empty `data`.
  Malformed filters return `400 invalid_input`.
- Replaces (v1): `GET /v1/events` compact event search.

### `GET /v2/resolvers/{chain_id}/{address}`

- Method/path: `GET /v2/resolvers/{chain_id}/{address}`
- Tier: product read.
- Purpose: resolver overview for numeric `chain_id` and resolver `address`.
- Request parameters: path `chain_id`, `address`; query `include` for
  route-documented sections, `at`, `finality`, `cursor`, `page_size`.
- Response shape: `data` is a resolver overview in product vocabulary. The
  route includes route-local `bound_names: {data, page}`, a nested collection
  of record-shaped name rows that replaces resolver-based name filtering.
- Pagination behavior: standard collection pagination applies to the
  nested `bound_names.page` object. The top-level response has no `page`.
- Status semantics: an otherwise valid resolver with no overview row returns
  `404 not_found`. A resolver overview with no bound names returns `200` with
  an empty bound-names section. Malformed `chain_id` or `address` returns
  `400 invalid_input`.
- Replaces (v1): `GET /v1/resolvers/{chain_id}/{resolver_address}/overview`
  and the `GET /v1/names?resolver=...` filter.

### `GET /v2/namespaces/{namespace}`

- Method/path: `GET /v2/namespaces/{namespace}`
- Tier: product read.
- Purpose: namespace metadata and supported-capability summary in product
  vocabulary.
- Request parameters: path `namespace`.
- Response shape: `data` is `{namespace, capabilities, networks}`.
  `capabilities` is a product-facing object keyed by capability name; each
  value is `{completeness, unsupported_reason?}` using the common
  completeness vocabulary. `networks` is an array of `{network, chain_id?}`
  entries when the namespace has public chain mappings. Control-plane metadata
  omits `meta.as_of`.
- Pagination behavior: none.
- Status semantics: unsupported public namespaces return `404 not_found`.
- Replaces (v1): `GET /v1/namespaces/{namespace}`. Operational namespace
  internals move to the diagnostics namespace route documented below.

## Tier 3: Diagnostics

Diagnostics are the only routes that may carry pipeline vocabulary. Product
route vocabulary restrictions do not apply to the diagnostic payloads below.

Diagnostic snapshot rules:

- `/v2/diagnostics/names/{name}/coverage`,
  `/v2/diagnostics/names/{name}/binding`,
  `/v2/diagnostics/names/{name}/authority`,
  `/v2/diagnostics/names/{name}/records`,
  `/v2/diagnostics/names/{name}/execution`, and `/v2/diagnostics/events`
  accept `at` and `finality` and carry `meta.as_of` because they explain the
  same selected snapshot as product reads.
- Diagnostics execution selection uses the exact name, `keys`, and selected
  snapshot. Omitting `at` selects the latest persisted execution artifact.
  RFC 3339 `at` selects the newest persisted artifact whose requested chain
  positions are at or before the selected positions. If multiple artifacts
  match, the deterministic tie-break is newest `finished_at`, then greatest
  `execution_trace_id`.
- `/v2/diagnostics/namespaces/{namespace}/manifests` omits `meta.as_of`; it is
  control-plane metadata.

### `GET /v2/diagnostics/names/{name}/coverage`

- Method/path: `GET /v2/diagnostics/names/{name}/coverage`
- Tier: diagnostics.
- Purpose: full coverage taxonomy.
- Request parameters: path `name`; query `namespace`, `at`, `finality`.
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
- Request parameters: path `name`; query `namespace`, `at`, `finality`.
- Response shape: `data` includes binding ids, binding kind, and anchors.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`.
- Replaces (v1): `GET /v1/explain/names/{namespace}/{name}/surface-binding`.

### `GET /v2/diagnostics/names/{name}/authority`

- Method/path: `GET /v2/diagnostics/names/{name}/authority`
- Tier: diagnostics.
- Purpose: authority/control explain.
- Request parameters: path `name`; query `namespace`, `at`, `finality`.
- Response shape: `data` includes token lineage, control vectors, and
  permission lineage.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`.
- Replaces (v1): `GET /v1/explain/names/{namespace}/{name}/authority-control`.

### `GET /v2/diagnostics/names/{name}/records`

- Method/path: `GET /v2/diagnostics/names/{name}/records`
- Tier: diagnostics.
- Purpose: record inventory and cache internals.
- Request parameters: path `name`; query `namespace`, `at`, `finality`.
- Response shape: `data` is
  `{record_inventory, record_cache, value_sources, comparison}`.
  `record_inventory` is
  `{record_version_boundary, enumeration_basis, selectors, explicit_gaps,
  unsupported_families, last_change}` using the existing diagnostic selector
  row fields `record_key`, `record_family`, `selector_key`, and `cacheable`.
  `record_cache` is `{record_version_boundary, entries}` where each entry is
  `{record_key, record_family, selector_key, status, value?,
  unsupported_reason?, failure_reason?}`. `value_sources` summarizes the
  indexed or verified origin per key. `comparison` is keyed by `record_key` and
  carries side-by-side `{indexed, verified}` record answers for the former
  `mode=both` workflow.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`.
- Replaces (v1): record inventory/cache diagnostics formerly embedded in
  `GET /v1/profiles/names/{name}` and
  `GET /v1/names/{namespace}/{name}/records`, including the former
  `mode=both` comparison.

### `GET /v2/diagnostics/names/{name}/execution`

- Method/path: `GET /v2/diagnostics/names/{name}/execution`
- Tier: diagnostics.
- Purpose: persisted verified-execution explain.
- Request parameters: path `name`; query `namespace`, `at`, `finality`, and
  required `keys`.
  `keys` uses the same record-key grammar as `/v2/names/{name}/records`. The
  route is verified-only; callers select the persisted artifact by exact name,
  requested keys, and selected snapshot. The route rejects duplicate or
  malformed keys with `400 invalid_input`.
- Response shape: `data` includes trace id, steps, digests, and CCIP
  participation.
- Pagination behavior: none.
- Status semantics: missing persisted execution artifacts return
  `404 not_found`.
- Replaces (v1): `GET /v1/explain/resolutions/{namespace}/{name}/execution`.

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
- Request parameters: query `namespace`, `name`, `address`,
  `registration_id`, `type`, `from_block`, `to_block`, `at`, `finality`,
  `cursor`, and `page_size`.
- Response shape: `data` is an array of raw normalized-event rows in
  diagnostics vocabulary:
  `{normalized_event_id, event_identity, namespace, name?, registration_id?,
  event_kind, source_family, manifest_version?, source_manifest_id?,
  chain_position, transaction_hash, log_index, raw_fact_ref, derivation_kind,
  canonicality_state, before_state?, after_state?, provenance, coverage}`.
- Pagination behavior: standard collection pagination.
- Status semantics: no matching rows returns `200` with empty `data`.
- Replaces (v1): `view=full` on `GET /v1/history/names/{namespace}/{name}`,
  `GET /v1/history/resources/{resource_id}`, and
  `GET /v1/history/addresses/{address}`.
