# API v2 Routes

Per-route reference for the development-time `/v2` surface accepted in
[ADR 0006](adrs/0006-api-v2-product-surface.md). Contract principles,
dictionary, envelope, status vocabulary, finality rules, cursor rules, and
error shape live in [`api-v2.md`](api-v2.md).

Routes below use the `/v2` prefix. C2 removes the former `/v1` API without
renaming these routes; the public edge remains a separate C3 change.

`GET /healthz` remains the unversioned operator health contract outside the
versioned product routes. `GET /`, `GET /docs`, and `GET /openapi.json` are not
served.

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

The top-level latest-state collections are `GET /v2/names/{name}/subnames`,
`GET /v2/names/{name}/history`, `GET /v2/permissions`,
`GET /v2/addresses/{address}/names`,
`GET /v2/addresses/{address}/history`, `GET /v2/search`, `GET /v2/events`, and
`GET /v2/diagnostics/events`. They omit `meta.as_of` and
`meta.as_of_token`, except that search reports request-scoped `meta.as_of` and
`meta.as_of_completeness` for staleness and suppression disclosure while still
omitting `meta.as_of_token`. Their cursors bind the collection anchor, namespace,
filters, and sort without claiming a frozen snapshot. Newly issued cursors
carry no snapshot token; a legacy cursor's snapshot component is ignored. They
accept omitted or explicit `finality=latest`. An `at` selector returns `400 invalid_input` with
`at is not supported because collection routes read latest state`;
`finality=safe` or `finality=finalized` returns `400 invalid_input` with
`finality must be latest because collection routes read latest state`.
Issue #188 option 1 remains the storage follow-up: revision-bound cursors with
explicit cursor-expired semantics. These restrictions lift when that storage
contract exists.

At the planned ENSv1→ENSv2 [re-derivation
boundary](glossary.md#re-derivation-boundary), slices 1 and 2 deploy
together with [PR #391](https://github.com/ensdomains/bigname/pull/391) under one
[interpreter content hash](glossary.md#interpreter-content-hash), one full
source re-walk, and one
[Project publication](glossary.md#projection) decision for
`ethereum-sepolia`. Production makes only that activated Project publication;
candidate-versus-activated behavior remains a replay and
acceptance-test distinction, not a production serving interval. Other chains
retain their ordinary independent publication decisions.

In the test environment, the slice-1 acceptance gate saves each normalized-
event-backed route's `next_cursor` at a fixed readable chain head, performs and
publishes the full Interpret and Project re-walk, and submits that old cursor to
the post-re-walk test publication. The control and candidate test runs hold
every other shared-boundary input constant, including PR #391's topology
serializer. For
`/v2/events`, name history, address history, and every other product cursor
surface backed by normalized-event row identity, it must
resume from the same normalized-event keyset anchor with identical remaining
product rows, pages, fields, `has_more`, and summary behavior. Because that
anchor may be an unmapped event absent from the response, the corpus places an
unmapped normalized event at a product-page boundary and proves no visible row
is skipped or duplicated. This default product-event exception does not remove
an explicitly requested `type` from cursor anchor validation.
`/v2/diagnostics/events` must accept its old cursor
and continue from the same stable normalized-event anchor, but its remaining
rows and fields may include the expected new candidate diagnostics.
The numeric `normalized_event_id` of a pre-existing diagnostic row may change
across the re-walk; its `event_identity` and pre-existing semantic fields remain
stable, apart from the explicitly allowed candidate diagnostic additions.
Implementations may preserve numeric normalized-event IDs or resolve the old
token through stable `event_identity` plus its stored sort tuple; these are
alternative storage strategies. Freshly issued cursor bytes may differ. The
gate separately verifies fresh post-re-walk cursors on every covered route.

That identical-product continuation rule applies to a re-walk whose declared
contract preserves product behavior. An intentional [interpreter content
hash](glossary.md#interpreter-content-hash) change may instead have a documented
field and route-membership delta. For the
[#348](https://github.com/ensdomains/bigname/issues/348) and
[#529](https://github.com/ensdomains/bigname/issues/529) changes, an existing late
ENSv2 resolver `RecordChanged` or `RecordVersionChanged` keeps its
`event_identity`, gains `logical_name_id`, keeps `resource_id=null`, and updates
the corresponding `raw_fact_ref.interpreter_state_key` attribution field. Its
`before_state` may also become the preceding `after_state` from the
logical-name/resource-null state stream that the event now joins. Issue #348
retains the surface from registry/root evidence; issue #529 retains a surface
observed only by resolver `AliasChanged` before a batch boundary. Those events
may consequently enter name-filtered diagnostics and product history. An
outstanding cursor has no continuation guarantee across this behavior-changing
boundary and may be rejected. Consumers must discard pre-#348/#529 cursors and
restart from the first page; fresh post-publication cursors continue normally.

The [#613](https://github.com/ensdomains/bigname/issues/613) interpreter change
keeps the original [pre-surface](glossary.md#pre-surface) ENSv1 registry `ResolverChanged` row unchanged,
then adds a name- and resource-linked, [state-derived](glossary.md#state-derived-normalized-event) `ResolverChanged` when the
first active [name surface](glossary.md#surface-name-surface) is learned. Product
events or name history may therefore gain one historical resolver row, while
diagnostics may gain each linked resource copy. A cursor issued before this
change has no continuation guarantee and may be rejected. Consumers must
discard pre-#613 cursors and restart from the first page; fresh post-publication
cursors continue normally.

These boundaries do not claim fresh/resumed parity for the known pre-existing
exception: when a resolver-emitted resource equals `namehash(N)`,
named-resource and alias preimages can share one retained [interpreter state
key](glossary.md#interpreter-state-key), so resumed interpretation can lose the
named-resource resolver hint and diverge from a fresh walk
([#560](https://github.com/ensdomains/bigname/issues/560); evidence is checked
in as an ignored collision probe).
If an ended resource retains a resolver pointer to the emitter, its rebuildable
record-inventory projection may change. The event remains resource-less and
does not restore `name_current.resource_id`, so the released or expired name's
name and record routes still expose no current record inventory.

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
  `lag_blocks`, `lag_seconds`, `pending_invalidation_count`,
  `pending_invalidation_count_capped`, `dead_letter_count`, `network_block`, `network_head_observed_at`,
  `network_head_age_seconds`, `network_head_status`,
  `ingestion_lag_blocks`, and `ingestion_lag_seconds`.
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
  Reverse inputs default to `coin_type=60` when omitted. Reverse `relation`
  accepts a comma-separated set of `owner`, `manager`, and `registrant`; `any`
  is the normalized all-three set. Reverse rows match when any listed relation
  matches. Batch limit is 1000 and is configurable with
  `BIGNAME_API_LOOKUP_BATCH_LIMIT`.
- Response shape: the common envelope. `data` is an array of result objects,
  not an object wrapper. The array contains one result per input in caller
  order. Each result is `{input, kind, status, unsupported_reason?,
  failure_reason?, normalization?, record?, records?, page?}`. `input` echoes
  the caller-supplied input, including `id` when supplied; omitted `id` is not
  synthesized. `kind` is `name` or `address`. Name results use `record` for the
  single record object. Reverse results use `records` for zero or more record
  rows with `is_primary` and `relations` in addition to the shared record
  fields. Reverse `input.relation` echoes the normalized relation set; `any`
  serializes as `owner,manager,registrant` and reordered sets use canonical
  dictionary order. `profile=feed` returns a documented core-field subset of
  the same record object; it does not introduce another DTO.
  A name result classified as `registration_status=unregistered` always omits
  `registration_id`. It also omits `resolver` and resolver-record fields unless
  it is
  an ownerless ENSv1 or Basenames registry row whose current registry resolver
  pointer is retained (a [serving resource](glossary.md#serving-resource)).
  That classified row serves its resolver without acquiring registration
  identity or control. Indexed records are served when its serving resource has
  inventory.
  See [registration status](api-v2.md#status-vocabulary) for the upstream
  basis.
  An ownerless ENSv2 reservation does not meet this exception, even if identity
  attached to a resource or record inventory was retained for audit. This
  intentionally differs from ENSv2, which stores and returns a reservation
  resolver until expiry.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @ ens_v2@a971bd64)
- Pagination behavior: top-level `page` is absent. Reverse inputs use the
  standard `page` object inside each result. Detail and feed use identical
  pagination semantics; feed only reduces returned fields. Reverse inputs
  default `page_size` to 50 and use the common max of 200. A reverse cursor
  binds the deployment-derived public namespace set and is rejected if that
  set changes. Relation filters that cannot be satisfied by one storage role
  (including exact `owner`, exact `registrant`, and partial relation sets such
  as `owner,manager`) may require multiple broad candidate batches to assemble
  one response page. The API retains the selected [projection
  generation](glossary.md#projection-generation) across those batches. Before
  issuing a second or later broad batch, it revalidates that generation and
  returns retryable `409 stale` if it changed. The API may return an as-filled
  page with `has_more=true` when it reaches the bounded post-filter scan cap;
  clients continue with the returned `next_cursor`.
- Status semantics: per-result `status` uses the common result vocabulary.
  Name misses are in-band `not_found`; invalid names are in-band
  `invalid_name`. Name-only and exact-scope latest reads return retryable `409
  stale` when the selected namespace is undergoing an Interpret redo. Reverse
  misses return `status=ok` with an empty `records`
  array for the input. Lookup record-level reason values are mapped to product
  vocabulary before serialization; current values include `read_failed`,
  `exact_name_profile_not_supported`, `mixed_exact_name_corpus`, and
  `unsupported_reason_missing`. The contracted per-name authority replacement
  is documented in
  [`architecture.md`](architecture.md#ensv1ensv2-current-authority). When its
  exact-name consumer slice is activated, `conflicting_current_ens_authority`
  covers Mainnet overlap without a provable boundary.
  `independent_ens_deployments_overlap` covers
  ordinary Sepolia overlap without a proven ENSv1→ENSv2 migration boundary; a proven
  Sepolia boundary follows the same per-name authority rule. The exact
  [shared ENS infrastructure](glossary.md#shared-ens-infrastructure) names—root,
  `eth`, `reverse`, and `addr.reverse`—instead select ENSv2 when the ENSv2 arm is
  current and ENSv1 evidence, current or historical, exists without proof.
  When that shared-infrastructure rule selects ENSv2, it overrides the ordinary
  no-proof handling below, so those names carry neither Mainnet's
  `conflicting_current_ens_authority` nor Sepolia's
  `independent_ens_deployments_overlap`.
  Historical ENSv2 evidence alone does not qualify, and `.reverse` descendants
  do not inherit the exception. These values replace the
  blanket mixed-corpus reason; intake from the planned [ENSv2 migration source
  family](glossary.md#source-family) alone does not add them. An address lookup
  returns `409 conflict` when the deployment has no ready public namespace.
  After the authority replacement is activated, an unsupported mixed-history
  name result retains `input`, `kind`, and a `record` containing only `name`,
  `display_name`, `namespace`, `namehash`, `status`, and
  `unsupported_reason`. It omits registration, control, lifecycle, resolver,
  record, relation, permission, and primary-name fields from both source
  families rather than presenting either binding as current.
- Snapshot behavior: lookup selects the current schema-v2 phase head and reads
  `bigname_phase` name, inventory, and address-name projections published for
  one completed projection-phase generation. For each reverse result, the
  readable name fetched with the candidate row is the common source for the
  emitted normalized and display names, label-derived fields, primary-name
  ordering, the `is_primary` result, and the reverse cursor. Public reverse
  lookup with no explicit namespace derives its snapshot scope from the
  namespaces served by the deployment, excluding a namespace
  while its selected authority chain has Interpret `redo_in_progress=true`,
  regardless of redo mode. A running Interpret redo rewrites previously served
  identity history batch by batch, so a page read during the redo can be
  incomplete even while Project still reports its prior completed head. Because
  projection publication is incremental, an unchanged
  row target may precede the selected head; it may not be ahead, and a
  same-height target must match the selected hash. Lookup revalidates both
  `chain_heads` and that generation after the read. Before that check, public
  reverse lookup also reloads the active manifest declarations and Interpret
  redo state captured during namespace derivation. An active manifest
  declaration change returns `409 conflict`. A redo that begins mid-request, an
  invalid target, phase lag, readiness change, or other mid-request
  head/projection change returns the existing retryable `409 stale`, never a
  partial page. For a bare public reverse request, the request scope contains
  every active public namespace's chains even when the readable namespace set
  excludes one of them. Readable chains appear in `meta.as_of`; an excluded
  request-scope chain appears in `meta.as_of_completeness` with
  `completeness=unsupported` and
  `unsupported_reason=temporarily_unavailable`. Name-only lookup does not use
  public namespace derivation: its inferred or explicit namespace determines
  the chain scope, and out-of-scope chains are absent from both disclosure maps.
  In a mixed name-and-address batch, reverse suppression takes precedence over
  a real `meta.as_of` entry for the same chain. `meta.as_of_token` can still
  contain the position actually used by the name input.
- Replaces (v1): `POST /v1/identity:lookup`.

### `GET /v2/status`

- Method/path: `GET /v2/status`
- Tier: lookup primitive.
- Purpose: per-chain indexing readiness.
- Request parameters: none.
- Response shape: `data.status` plus `data.chains`, keyed by `chain_id`.
  The schema-v2 project phase does not use the legacy invalidation queue, so
  `data.pending_invalidation_count` is `0`,
  `data.pending_invalidation_count_capped` is `false`, and
  `data.dead_letter_count` is `0`. Each chain entry carries `latest_block`,
  `indexed_block`, `safe_block`,
  `finalized_block`, `lag_blocks`, `lag_seconds`, `network_block`,
  `network_head_observed_at`, `network_head_age_seconds`,
  `network_head_status`, `ingestion_lag_blocks`, `ingestion_lag_seconds`, and
  route-local ops `status`.
- Storage mapping: the chain set is the union of
  `bigname_phase.chain_heads` and `bigname_phase.chain_phase_state`.
  `latest_block`, `safe_block`, and `finalized_block` map to the corresponding
  `chain_heads` positions. `indexed_block` maps to the `project` phase's
  `current_block_number`. `lag_seconds` compares the timestamps of the exact
  latest-head and project-current hashes in `bigname_phase.chain_lineage`.
  Missing head, project, or lineage rows preserve the existing nullable fields.
  If the phase schema has not been created yet, API startup uses an empty
  expected-chain set and this route returns the same empty, `degraded` status
  shape instead of preventing the API process from starting.
  When the phase schema is present, API startup checks every phase-schema
  relation, function, and type its serving paths read: relations by name, both
  guarded [verified lookup](glossary.md#verified-lookup) functions by exact
  signature, and the `canonicality_state` type. If any are missing, the API
  refuses to start and its diagnostic names every missing identity.
- The existing per-chain `status` field also maps the `project` phase
  lifecycle and redo marker, the Interpret `redo_in_progress` marker, and the
  newest per-chain
  `bigname_phase.service_heartbeats` timestamp. A phase row that startup
  settled while its chain was unconfigured is not eligible for `ready` until
  genuine phase completion or completed-state revalidation clears that marker.
  It reports `degraded` unless a stronger `stale` condition applies, such as a
  genuinely failed phase or an expired heartbeat. An active Interpret redo
  makes the chain `degraded`; `data.status` continues to report the worst
  readiness across all chains.
  Ethereum Sepolia is ineligible for `ready` unless its `ingest` phase
  remains `completed` and its [verification](glossary.md#verification-level)
  phase is `completed` at a known level at or above the `quick_synced` floor; unknown stored levels fail closed. A failed Ingest or Verify, or an ordinary completed Verify without that
  evidence, is `stale`; an idle, running, paused, or missing Ingest or Verify is
  `degraded`. An expired runner heartbeat remains `stale` while either required
  phase is incomplete. Chains without this requirement omit those Ingest and
  Verify evidence checks but still honor the settlement marker and the
  Project-lifecycle rule below.
  The most recent completed `project` publication remains the indexed position
  while the next live-follow pass is running. A running phase is therefore
  eligible for `ready` when that completed publication is present and its
  block and time lag are within `BIGNAME_API_STATUS_MAX_BLOCK_LAG` and
  `BIGNAME_API_STATUS_MAX_LAG_SECS`. Readiness also requires the publication's
  interpreter content hash to match this API build and, at the same height as
  the stored head, its block hash to match exactly. A generation mismatch is
  `degraded`. A running phase without a completed publication is `degraded`;
  lag beyond either threshold is `stale`. A failed
  `project` phase is `stale`, while a paused or redoing phase is `degraded`.
  Missing phase-runner heartbeat evidence is `degraded`; a heartbeat older
  than `BIGNAME_API_PHASE_HEARTBEAT_MAX_AGE_SECS` is `stale`. This phase-only
  threshold defaults to 60 seconds so a long database statement between
  five-second runner heartbeat opportunities.
- `lag_blocks` and `lag_seconds` are independently nonnegative. Each field
  clamps its own canonical-versus-projected difference at `0`.
- Pagination behavior: none.
- Status semantics: route-local ops `status` is `ready`, `degraded`, or
  `stale`. This is the only non-result `status` enum in `v2`. `project`
  publication lag beyond either configured threshold, or a fresh provider
  comparison beyond either configured ingestion-lag threshold, is `stale`.
  Missing stored readiness or a provider observation
  whose `network_head_status` is `stale`, `unavailable`, `pending`, or
  `unconfigured` is `degraded` when its projection is within the configured
  thresholds. Excessive projection lag takes precedence and is `stale`;
  `network_head_status` still reports the provider state. The provider head is
  refreshed asynchronously
  under a timeout and cache TTL; this route reads only the cache and never
  waits for provider I/O. If the latest refresh fails after a successful one,
  `network_head_status` becomes `unavailable` immediately while the last head,
  observation time, age, and lag values remain as cached evidence.
- Replaces (v1): `GET /v1/status`.

## Tier 2: Product Reads

### `GET /v2/names/{name}`

- Method/path: `GET /v2/names/{name}`
- Tier: product read.
- Purpose: name-profile read, using the flat record shape plus registration summary.
- Request parameters: path `name`; query `namespace`, `at`, `finality`,
  `source`. `source` accepts `indexed` or `verified`; omitting it is identical
  to `source=indexed`. This name-profile route does not accept `source=auto`.
- Response shape: `data` is one flat record object using dictionary fields.
  The registration summary is not nested; it is represented by
  `registration_id`, `token_id`, `owner`, `manager`, `registrant`,
  `registered_at`, `created_at`, `expires_at`, and `registration_status` on
  the same object when backed. An ENSv1 wrapper-backed row also carries
  `wrapper_state` with the current [`wrapped`](glossary.md#wrapped-namewrapper-state),
  [`emancipated`](glossary.md#emancipated-namewrapper-state), or
  [`locked`](glossary.md#locked-namewrapper-state) lifecycle value and the typed
  `wrapper_fuses` object defined in [`api-v2.md`](api-v2.md#naming-dictionary).
  The tristate is bigname vocabulary derived from the enforcing NameWrapper
  guards, not an upstream enum. Both fields are omitted after an emancipated or
  locked wrapper position expires; a plain wrapped position remains `wrapped`
  with `wrapper_fuses.fuses=0` and every named boolean false because expiry
  clears its fuses without clearing its owner.
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L848 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L849 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f)
  Fuse-effect gating accepts the full upstream `uint64` expiry domain. A valid
  `MAX_EXPIRY` therefore keeps the lifecycle value active at representable
  served block timestamps even when `expires_at` is omitted because that public
  timestamp cannot be represented.
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L57 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L848 @ ens_v1@91c966f)
  A `.eth` second-level name keeps its lifecycle value during the registrar
  grace period, even though owner modification and transfer powers stop at the
  earlier grace boundary; `wrapper_state` is not itself a complete permission
  summary. (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L48 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L218 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L221 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L820 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L825 @ ens_v1@91c966f)
  `manager` is omitted when no forward-read source can derive it; it is not
  emitted as a permanent null placeholder. The
  name-profile portion uses `name`, `display_name`, `namespace`, `namehash`, `resolver`,
  `addresses`, `text_records`, `content_hash`,
  `primary_name`, `primary_address`, `chain_id`, `network`, `status`, and
  `unsupported_reason`/`failure_reason`/`unsupported_fields` when those fields
  are served. With `source=verified`, the resolver-record-backed fields
  `addresses`, `text_records`, `content_hash`, and `primary_address` are built
  by a fresh schema-v2 lookup at the current readable position, using the same
  verified path as `/v2/names/{name}/records`; indexed resolver-record values
  are not substituted into those fields. The registration and identity summary
  fields (`registration_id`, `token_id`, `owner`, `manager`, `registrant`, dates,
  `registration_status`, `wrapper_state`, `wrapper_fuses`, `name`, `display_name`, `namespace`, `namehash`,
  `resolver`, `primary_name`, `chain_id`, and `network`) remain indexed
  projection values because they are not resolver records. Verified responses
  include `meta.as_of`/`meta.as_of_token` for the positions used by the fresh
  lookup. For cross-chain resolution, the authoritative position is admitted
  from the selected product snapshot and the auxiliary position is the
  canonical execution position retained by the projected row; it may be older
  than the newest `chain_heads` position selected for that chain, but never
  newer; an anchor at the same height must have the same block hash. They create
  no legacy trace or reusable execution outcome. Provider connect, DNS, TLS,
  connection-reset, and other transport failures abort the whole request with
  `500 internal_error`; they are not flat-record `status=stale` results. On a
  `200` name-profile response,
  `status` is the flat-record result: `ok` for clean indexed reads; `failed`
  and `stale` may appear only when `source=verified` cannot serve the verified
  sections. `unsupported` is keyed on the projected row's own coverage status,
  not on a list of reasons: any row whose `coverage.status=unsupported` returns
  `200` with `status=unsupported` and the minimal identity-only object below.
  The single exception is `current_authority_not_projected`, which keeps the
  ratified partial `status=ok` described at the end of this section. Every other
  unsupported reason downgrades, including
  `conflicting_current_ens_authority` and
  `independent_ens_deployments_overlap` for a mixed-history read with no
  provable current authority, and `ensv2_exact_name_profile_shadow`, which
  reaches consumers as `exact_name_profile_not_supported`. The rule fails closed
  at both edges: an unsupported row that names no reason downgrades, and so does
  an unsupported reason this build does not recognize, so a reason added to the
  projection later serves `unsupported` by default rather than silently serving
  `ok` ([#487](https://github.com/ensdomains/bigname/issues/487)). A proven
  migration boundary returns the selected ENSv2 registration and `status=ok`;
  it does not expose the retained ENSv1 registration as current. `failure_reason`
  or `unsupported_reason` carries the product reason when available;
  `not_found` and `invalid_name` are unreachable in-record. The unsupported
  object retains only `name`, `display_name`, `namespace`,
  `namehash`, `status`, and `unsupported_reason`; registration, control,
  lifecycle, resolver, record, relation, permission, and primary-name fields
  from both source families are omitted.
  A row classified as `registration_status=unregistered` always omits
  `registration_id`. It also omits `resolver` and resolver-record fields unless
  it is
  an ownerless ENSv1 or Basenames registry row whose current registry resolver
  pointer is retained (a [serving resource](glossary.md#serving-resource)).
  For that classified row, indexed name detail serves the resolver and records
  present in its serving resource's inventory. `source=verified` executes lookup
  through the surviving resolver when the ordinary lookup capability supports
  it. Neither path acquires registration identity or control. An ownerless ENSv2
  reservation does not meet this exception, even if identity attached to a
  resource or record inventory was retained for audit. This intentionally
  differs from ENSv2, which stores and returns a reservation resolver until
  expiry.
  See [registration status](api-v2.md#status-vocabulary) for the upstream
  basis.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @ ens_v2@a971bd64)
  For `source=indexed`, a row classified as
  `current_authority_not_projected` remains `status=ok` for the identity and
  registration fields that can be served, but omits `resolver`; retained
  resolver-pointer evidence is not presented as current authority.
  An ownerless ENSv1 or Basenames registry row with a zero [getter-visible
  owner](glossary.md#getter-visible-owner) is instead supported and unregistered.
  When a current event-linked nonzero registry resolver pointer survives, name
  detail includes that resolver while registration and control fields remain
  absent.
- Pagination behavior: none.
- Status semantics: valid names with no name-profile data return `404 not_found`.
  Invalid path names return `400 invalid_input`.
- Replaces (v1): `GET /v1/names/{namespace}/{name}` and
  `GET /v1/profiles/names/{name}`.

### `GET /v2/names/{name}/records`

- Method/path: `GET /v2/names/{name}/records`
- Tier: product read.
- Purpose: resolver records.
- Request parameters: path `name`; query `namespace`, `at`, `finality`,
  `source=indexed|verified|auto`, `keys`, `include=inventory`.
- Response shape: `data` returns resolver record values using `namespace`,
  `resolver`, `addresses`, `text_records`, and `content_hash`. `keys` is a
  comma-separated
  record-key allowlist using the existing app key grammar: `addr:<coin_type>`,
  `text:<key>`, `avatar`, and `contenthash`. Requested-key outcomes are also
  returned in route-local `records`, keyed by the requested key; each value is
  `{status, value?, unsupported_reason?, failure_reason?, meta?}`. Exact indexed
  and verified answers omit `meta`. An indexed ENSIP-19 answer derived from the
  projected default-address rule includes
  `meta={basis:"derived", rule:"ensip19_default_address",
  source_record_key:"addr:2147483648"}` for both `ok` and authoritative
  `not_found`. Values-only convenience maps continue to contain only values.
  `source=verified`
  and verified fallback from `source=auto` execute a fresh schema-v2 lookup on
  every request. They do not read or write the legacy execution cache. A direct
  live/indexed disagreement may update the guarded resolution divergence
  ledger; restored agreement may clear a matching active row, while wildcard
  and CCIP-Read answers do not mutate it. A selected position
  that the provider cannot serve is reported per key as `status=stale` with a
  `failure_reason`; a completed provider response timeout or malformed result
  remains an in-band `status=failed` result.
  `meta.as_of` and `meta.as_of_token` report the authoritative and actual
  hash-pinned execution positions returned by the lookup engine. On a
  cross-chain path, the execution position may be an older canonical projected
  position than the auxiliary `chain_heads` position initially selected by the
  route. It may not be newer, and a position at the same height must have the
  same block hash; violations are stale before provider execution. Provider
  connect, DNS, TLS, connection-reset, and other transport
  failures abort the whole request with `500 internal_error`; they are not
  per-key stale answers and `source=auto` does not return a partial blend.
  A name with no current registration returns no declared resolver or retained
  record values and does not execute verified lookup unless it is
  an ownerless ENSv1 or Basenames registry row whose current registry resolver
  pointer is retained (a [serving resource](glossary.md#serving-resource)).
  That classified row serves its resolver and any records present in its serving
  resource's inventory. Verified lookup runs through the surviving resolver
  when the ordinary lookup capability supports it. An ownerless ENSv2
  reservation does not meet this exception. `include=inventory` does not expose
  inventory retained for a former or audit-only resource. This intentionally
  omits the resolver that ENSv2 can store and return for an unexpired
  reservation.
  See [registration status](api-v2.md#status-vocabulary) for the upstream
  basis.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @ ens_v2@a971bd64)
  Product records use product reason vocabulary: retained-selector misses use
  `value_not_retained`, and phase-unsupported record families use
  `record_family_not_supported`.

  Representative keyed answers and convenience fields are:

  ```json
  {
    "addresses": {
      "0": "0x001122"
    },
    "content_hash": "0xe3010170",
    "records": {
      "contenthash": {
        "status": "ok",
        "value": "0xe3010170"
      },
      "addr:0": {
        "status": "ok",
        "value": "0x001122"
      },
      "addr:2": {
        "status": "not_found"
      }
    }
  }
  ```

  Coin types in record keys and the `addresses` map are decimal strings.
  Contenthash and address answers are scalar lowercase, `0x`-prefixed hex.
  This route flattens both projected `{encoding,bytes}` address values and
  projected scalar address values to that same public string; the exact-name
  detail route, `profile=detail` lookup, and GraphQL resolver address fields
  apply the same normalization.
  A zero-length byte payload makes the exact stored answer `not_found` and
  omits `value`. Cleared exact values are absent from `addresses` and
  `content_hash` unless a documented derived-record rule supplies a replacement
  answer. The ENSIP-19 default-address rule below is one such rule.
  `source=auto` blends per key: indexed answers are used where they satisfy the
  requested key, and only the remaining supported keys fall back to verified
  lookup. [Universal Resolver ancestor
  discovery](glossary.md#universal-resolver-ancestor-discovery) applies when a
  readable ENS name on Ethereum Mainnet has a null projected exact resolver, a
  projected name identity and DNS wire name, no alias, linked-subregistry,
  projected wildcard, or cross-chain transport path, and an admitted Universal
  Resolver manifest entrypoint. This makes the indexed null-resolver miss
  unsatisfying. `source=auto` therefore executes the requested
  keys through verified lookup, and `source=verified` uses the same route. The
  Universal Resolver walks to the nearest nonzero ancestor resolver and accepts
  an ancestor only when it implements ENSIP-10
  `(upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L25-L38 @ ens_v1@91c966f)`
  `(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L63-L88 @ ens_v1@91c966f)`.
  These responses use `meta.source=verified`; `data.resolver` remains null
  because it reports the exact registry resolver, not the ancestor used during
  live execution. `ResolverNotFound(bytes)` is a chain-proven per-key
  `not_found` with `failure_reason=resolver_not_found` only when its embedded
  DNS name equals the request name; it covers both no resolver and a nearest
  non-extended ancestor. Other reverts remain failed. Every successfully
  decoded call for one name and block must return the same effective resolver.
  A `ResolverNotFound` outcome cannot coexist with a successfully decoded
  effective resolver; either inconsistency fails the request closed. Ordinary
  selector-local failed or unsupported results remain mixed per key. This route
  has no indexed comparison: success and
  live `not_found` write and clear no divergence rows, while CCIP-required
  answers remain `unsupported` with `offchain_lookup_required` and likewise
  write nothing. ENS verified record resolution continues not to follow CCIP-Read.
  Basenames and other chains and namespaces do not enter this route. Outside this
  null-exact-resolver class, exact indexed `ok` answers and authoritative
  ENSIP-19 derived answers satisfy auto without a provider request. Within this
  class, all requested keys execute through verified lookup because retained
  exact inventory predates the resolver-clear boundary. If no admitted
  Universal Resolver entrypoint is available at execution time, the requested
  keys are explicitly `unsupported`; auto never turns that inability to execute
  into an indexed null-resolver `not_found`. For
  `addr:<coin_type>`, exact `ok`
  wins. An exact entry normalized to `not_found`, including empty address
  bytes, or a missing exact entry may read projected
  `addr:2147483648` only when the selected resolver's manifest-authorized
  [resolver read feature](glossary.md#resolver-read-feature) is present and
  `chainFromCoinType(coin_type) > 0`; coin type `2147483648` itself never
  recurses. A derived answer is normalized through the requested getter's
  verified decode: for coin type `60`, a 20-byte zero default becomes derived
  `not_found`; for EVM-range multicoin selectors, the same non-empty bytes remain
  an `ok` value. Exact stored records keep their existing behavior. Other
  default-source `ok` values yield the requested-key value, while authoritative
  absence yields derived `not_found`. An unsupported or
  non-authoritative source leaves auto unsatisfied and triggers ordinary
  verified lookup. Explicit `source=indexed` reports that case as
  `unsupported`.
  (upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L9-L38 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L68-L85 @ ens_v1@91c966f)
  (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L685-L697 @ ens_v2_sepolia_20260629@ccaeb58)
  The admitted legacy Basenames resolver is unflagged because its exact-storage
  getter does not implement that fallback. The fallback-bearing upgradeable
  Basenames resolver proxy is not yet admitted.
  (upstream: .refs/basenames/test/Fork/BaseMainnetConstants.sol:L9-L14 @ basenames@1809bbc)
  (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/AddrResolver.sol:L35-L61 @ basenames@1809bbc)
  A non-derived indexed `not_found` is admitted only from a record inventory
  whose coverage carries no unsupported reason and whose coverage `status` is
  `full` or `projected`. `projected` is admitted
  because a supported schema-v2 inventory is complete for its current resolver
  over retained supported facts: the project phase includes both record events
  already linked to the name and ENSv1 resolver events whose `logical_name_id`
  is null but which match the same chain, node hash, and emitting resolver. It
  then keeps everything
  strictly after that resolver last bumped the node's record version and thereby
  cleared its earlier records (the `record_version_boundary` reported below).
  Resolver selection time is not a lower bound on writes. These rules follow
  the registry's current-resolver lookup
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137 @ ens_v1@91c966f)
  and the resolver's version-, node-, and key-scoped text storage
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L28 @ ens_v1@91c966f).
  Once that inventory exists, a key's absence is therefore absence from the
  retained attributable history rather than an unfinished build. When an
  ENSv1 registry resolver selection predates the
  [name surface](glossary.md#surface-name-surface), first-surface
  materialization supplies the linked pointer without requiring a repeated
  selection; a latest zero-address selection remains a clear. The row's
  `exhaustiveness: not_asserted` disclaims a claim about complete *history*,
  which is a weaker statement than `full` and does not weaken this admission.
  Node-keyed `ens_v1_resolver_l1` records written before the name surface
  existed enter that attributable history only when the selected pointer's
  source family is `ens_v1_registry_l1`, `ens_v1_registrar_l1`, or
  `ens_v1_wrapper_l1`. A selected `ens_v2_registry_l1` or `ens_v2_root_l1`
  pointer may also admit them when its target resolver's final classification
  is supported `ens_v1_resolver_l1` from an applicable exact declaration and
  the classifying manifest's namespace matches the pointer's namespace. Under
  that guard, absence from the projected inventory is authoritative
  `not_found`. A node-keyed `basenames_base_resolver` row with no
  `logical_name_id` is likewise attributable only through a
  `basenames_base_registry` pointer on the same chain, node, and resolver
  emitter. Basenames keeps the current resolver by node, authorizes its
  registrar controller and reverse registrar independently of the node owner,
  and stores text by record version, node, and key.
  (upstream: .refs/basenames/src/L2/Registry.sol:L173-L180 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193-L199 @ basenames@1809bbc)
  (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/ResolverBase.sol:L7-L24 @ basenames@1809bbc)
  (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/TextResolver.sol:L7-L36 @ basenames@1809bbc)
  Other ENSv2-family pointers do not attribute this node-keyed history.
  An inventory in any other coverage state is not authoritative, and the
  request falls through to verified lookup or an explicit unsupported answer
  rather than reporting absence from the index as absence on chain.
  A Basenames auto read remains Base-scoped when no fallback key
  remains; it selects the Ethereum resolution-auxiliary position only when it
  will attempt that verified fallback. If projection movement removes the last
  fallback key while the expanded snapshot is being selected, the request
  returns `409 stale` so a retry can return an indexed Base-scoped response.
  Explicit `keys` and the inventory-derived default verified selector set are
  both limited to 200 record keys. When omitted `keys` would derive more than
  200 keys, `source=verified` returns `422 unsupported` before any provider call;
  callers can supply `keys` to select a smaller set. For the verified flat
  name-profile, the limit applies to inventory-derived selectors before the
  route adds its synthetic primary-address request. A 200-selector inventory
  may therefore produce 201 provider keys when `addr:60` was absent; an
  inventory with more than 200 selectors still returns `422 unsupported`.
  `include=inventory` adds route-local
  `inventory: {known_keys, unset_keys, unsupported_keys}`. Deep inventory
  internals stay on diagnostics.
- Pagination behavior: none.
- Status semantics: a missing name returns `404 not_found`. Missing, unset, or
  unsupported requested record values are reported with the common result
  `status` vocabulary inside the record answer rather than by changing the
  envelope. A proven migration uses
  only the selected ENSv2 resolver; the retained ENSv1 resolver is historical.
  A name whose exact-name projection is unsupported exposes no resolver values.
  Every unsupported reason except `current_authority_not_projected`
  short-circuits `source=indexed`, `source=verified`, and `source=auto` before
  provider execution and reports each requested or inventory-derived key as
  `status=unsupported` with the name-level reason:
  `conflicting_current_ens_authority` or
  `independent_ens_deployments_overlap` for a mixed-history name with no
  provable current authority, and otherwise the same public reason name detail
  serves for that row. The reason reaches this route through the shared
  name-level vocabulary name detail uses, so one projection reason yields one
  public reason on every route. Verified execution does not choose a resolver
  for an unsupported name. `current_authority_not_projected` also
  short-circuits those three sources before provider execution, but keeps its
  documented behavior: the response has no resolver values and reports each
  requested or inventory-derived key as `status=unsupported`
  with `inventory_not_available`.
  A supported ownerless registry name does not enter that short circuit merely because its control
  state is unregistered. Indexed reads use the [serving resource](glossary.md#serving-resource)'s inventory, verified reads select
  the surviving resolver, and `source=auto` follows the ordinary indexed/verified blend. Owner zero
  or registry-self alone therefore does not produce `inventory_not_available`.
  Direct verified lookup compares against the same exact-or-derived indexed
  evaluator before the guarded resolution-divergence-ledger write. Agreement
  can therefore clear an older exact-key false miss; provider output remains
  request-scoped and is never copied into inventory or another projection.
  When projection changes a formerly direct resolver to null, projection
  publication retires active observations for that old direct resolver as stale
  evidence. This cleanup does not compare a live ancestor-served answer with the
  former indexed miss and is not a wildcard divergence write.
- Replaces (v1): `GET /v1/names/{namespace}/{name}/records` and record
  sections of `GET /v1/profiles/names/{name}`.

#### Public record-field completeness

The product record-key grammar is deliberately closed. The same grammar applies
to the product and record-diagnostic routes; a family outside it is rejected as
`400 invalid_input`, not returned with an invented or incomplete value.

| Registry or resolver field family | Public status | Contract |
| --- | --- | --- |
| Address records | Served | `addr:<coin_type>`, with the coin type limited to an unsigned 64-bit integer; see the [coin-type selector divergence](upstream.md#verified-resolution-addr-coin-type-selector-narrowing). ENS defines both the legacy Ethereum-address getter and the multicoin getter. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L4-L11 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L4-L15 @ ens_v1@91c966f) |
| Text records | Served | `text:<key>`, with the key limited to the closed selector grammar: non-empty, no ASCII whitespace, no commas (commas separate multiple record keys in request parameters). Each request item is trimmed of boundary whitespace before the grammar check, so `text:display ` selects the `display` key; a key that fails the grammar after that trim is rejected as `400 invalid_input`, and an on-chain text key containing whitespace or commas is not requestable through this route; see the [text selector-key divergence](upstream.md#verified-resolution-text-selector-key-narrowing). ENS defines text records by node and unconstrained string key. (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L4-L19 @ ens_v1@91c966f) |
| Avatar | Served | `avatar`, as the dedicated public selector for the `avatar` text key. (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L4-L19 @ ens_v1@91c966f) |
| Content hash | Served | `contenthash`. The current Basenames admission has a narrower event family; see the [Basenames contenthash divergence](upstream.md#basenames-contenthash-admission-narrowing). (upstream: .refs/ens_v1/contracts/resolvers/profiles/IContentHashResolver.sol:L4-L10 @ ens_v1@91c966f) |
| Registry TTL | Validated and discarded | `NewTTL` is decoded to validate admitted logs but produces no normalized event or public record key. The LLL-era low-byte validation exception is documented in the [registry-word divergence](upstream.md#ensv1-lll-era-registry-word-decoding). ENS declares the TTL event and getter as `uint64`. (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L14-L15 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L49-L57 @ ens_v1@91c966f) |
| Registry owner | Served outside the grammar | No record key. `NewOwner` and `Transfer` are retained as normalized authority events. `GET /v2/names/{name}` carries the selected current owner in its optional `owner` field; its history route exposes a retained authority change as `type=authority`. Ownership is never requestable as a record key. (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L6-L9 @ ens_v1@91c966f) |
| Registry resolver | Served outside the grammar | No record key. `NewResolver` is retained as the node's resolver-binding event. `GET /v2/names/{name}` carries a serveable current binding in its optional `resolver` object (`chain_id` and `address`); its history route exposes a retained change as `type=resolver`. A resolver address is not itself a requestable record. (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L11-L12 @ ens_v1@91c966f) |
| ABI records | Outside the grammar | No public key. ENS defines ABI records by node and accepted content-type mask. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IABIResolver.sol:L4-L16 @ ens_v1@91c966f) |
| Public keys | Outside the grammar | No public key. ENS defines a secp256k1 public-key record. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IPubkeyResolver.sol:L4-L12 @ ens_v1@91c966f) |
| Interface declarations | Outside the grammar | No public key. ENS defines an interface-ID-to-implementer lookup. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IInterfaceResolver.sol:L4-L22 @ ens_v1@91c966f) |
| Reverse-claim name records | Served outside the grammar | No record key. For indexed claim intake, only a `RecordChanged` row whose `primary_claim_source` was produced when the reverse-registrar adapter interpreted `NameForAddrChanged` is attributed to a reverse claim. Among indexed `RecordChanged` rows, only those attributed rows contribute claim values to the primary-name projection; a `ReverseChanged` event for the same address, coin type, and namespace must also exist, and the indexed claim attaches to that event's key. ENSv1's standalone reverse registrar emits `NameForAddrChanged` when it stores an address's name. (upstream: .refs/ens_v1/contracts/reverseRegistrar/StandaloneReverseRegistrar.sol:L28-L30 @ ens_v1@91c966f) Mainnet ENS reverse resolution instead uses the separate [event-silent](glossary.md#event-silent) reverse-resolver [hydration](glossary.md#hydration) or request-scoped [verified lookup](glossary.md#verified-lookup) path, not indexed `NameForAddrChanged` claim intake; its reverse registrar emits `ReverseClaimed` and calls the selected resolver to set the name. (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L76-L84 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123-L131 @ ens_v1@91c966f) |
| General resolver name records | History only; outside the grammar | No public key or current value surface. Every resolver-family `NameChanged` is retained as an unattributed normalized `RecordChanged` in the `name` family, regardless of resolver or node type; a write for an `<addr>.addr.reverse` node therefore remains unattributed. When the row is associated with a materialized name, `GET /v2/names/{name}/history` exposes the change as `type=record` without its stored name value. The record routes reject the `name` family, and the primary-name projection ignores these rows because they have no `primary_claim_source`. ENSv1 defines `NameChanged` generically by node and name. (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L4-L11 @ ens_v1@91c966f) |
| Resolver record versions | Outside the grammar | No public key. ENS keeps a per-node record version on the resolver and bumps it on `clearRecords`, emitting `VersionChanged`; the indexed record inventory retains that event as the boundary that invalidates older record values, but the version number itself is not served. (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L8 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L20-L22 @ ens_v1@91c966f) |
| DNS record sets | Outside the grammar | No public key. ENS defines DNS record-set update/delete events and a wire-format getter. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDNSRecordResolver.sol:L4-L24 @ ens_v1@91c966f) |
| DNS zone hashes | Outside the grammar | No public key. ENS defines a DNS zone-hash update event and getter. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDNSZoneResolver.sol:L4-L15 @ ens_v1@91c966f) |
| Legacy content and multihash | Outside the grammar | No public key. ENS retains these getters and setters as deprecated resolver functions. (upstream: .refs/ens_v1/contracts/resolvers/Resolver.sol:L86-L93 @ ens_v1@91c966f) |
| ENSv1 arbitrary data records | Outside the grammar | No public key. ENS defines string-keyed arbitrary byte data. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDataResolver.sol:L5-L21 @ ens_v1@91c966f) The pinned ENSv1 `PublicResolver` source composes `DataResolver`. (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20-L30 @ ens_v1@91c966f) bigname's ENSv1 resolver-family manifest admits and normalizes `DataChanged`, but its Mainnet PublicResolver admission rows include `DataResolver` in none of their declared resolver compositions; see [ENS mainnet admission](manifests.md#ens-mainnet). This is an admitted-generation composition limit and a grammar limit, not an event-admission limit. |
| ENSv2 generic data resources | Outside the grammar | No public key. The admitted archived Sepolia resolver ABI exposes `DataChanged` and `NamedDataResource`; their normalized-event exclusion is documented in the [ENSv2 admission divergence](upstream.md#ensv2-data-event-admission-narrowing). (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L360-L375 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L505-L519 @ ens_v2@a971bd64) |

### `GET /v2/names/{name}/subnames`

- Method/path: `GET /v2/names/{name}/subnames`
- Tier: product read.
- Purpose: direct subnames.
- Request parameters: path `name`; query `namespace`, `include=counts`,
  `cursor`, `page_size`, and optional `finality=latest`. `at` and historical
  `finality` values are rejected by the shared latest-state collection rule.
- Response shape: `data` is an array of dedicated subname rows in dictionary
  vocabulary: `name`, `display_name`, `namespace`, `namehash`, `labelhash`,
  `owner`, `registrant`, `registration_status`, `registered_at`,
  `created_at`, and `expires_at`. Registry events prove the child node and its
  labelhash but not the label, so two [non-name
  forms](glossary.md#non-name-form) are reachable here. A child whose label has
  never been observed carries `[<labelhash-without-0x>].<parent-name>` in
  `display_name` and the same stored bytes in `name`. The same placeholder serves
  a child whose label was observed — from the chain or the proof-checked rainbow
  import — but whose text fails ENSIP-15 normalization: the node commits to the
  raw label bytes, so serving the text as a name would name a different node,
  and escaping the valid string would reproduce the same misleading text. When a
  label is observed as
  bytes that are not valid UTF-8, or that contain a NUL, the row instead carries
  the whole stored child name — those label bytes, a dot, and the parent's —
  encoded by PostgreSQL's `escape` rule: a NUL as `\000`, each byte above `0x7f`
  as a backslash and three octal digits, a backslash doubled, and every other
  byte verbatim. The rule runs over the whole string, so a non-ASCII parent
  portion is octal-escaped along with the label. Neither form is reserved syntax
  — a label really spelled `[<64 hex digits>].<parent>`, or really spelled like
  escape output such as `\377bad`, produces the same string — so distinguish
  rows by `namehash` and `labelhash` rather than by parsing the served text.
  Both forms come from ENSv1 and Basenames registry edges; an ENSv2 child
  bigname cannot name is absent from the page instead. Neither form is
  addressable, and neither may be fed
  back into a name-shaped route. Resolver records are not included here;
  use `GET /v2/names/{name}/records` for `resolver`, `addresses`,
  `text_records`, and `content_hash`.
  `include=counts` adds `subname_count`, the row's direct subname count.
- Pagination behavior: standard collection pagination by
  `display_name` ascending.
- Snapshot behavior: the parent and subname rows are selected from current
  state. The response omits `meta.as_of` and `meta.as_of_token`, and its cursor
  carries no snapshot validity claim. True as-of child enumeration is deferred
  to the revision-bound storage follow-up.
- Status semantics: no direct subnames returns `200` with empty `data`.
  Missing parent names return `404 not_found`. Each child appears at most once,
  from the relation its own selected authority names. ENSv1 relations that are
  unreachable through the parent's ENSv1→ENSv2 migration path are omitted. A
  parent on the `unwrapped`, `unlocked_wrapped`, or `emancipated_child` path
  retains no ENSv1 children. A parent on the `locked_wrapped` or `locked_child`
  path retains only a [migratable child](glossary.md#migratable-child): one
  whose label has never had a reserved, registered, or renewed entry in that
  parent's [migration `WrapperRegistry`](glossary.md#migration-registry-wrapperregistry), whose current
  expiry-effective fuse word has `PARENT_CANNOT_CONTROL` set and `IS_DOT_ETH`
  clear, and whose current ENSv1 registry owner is nonzero. The wrapper fuse
  and expiry evidence remains effective across an ENSv1 binding rotation.
  (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L276-L277 @ ens_v1@91c966f)
  The child's own
  [authority arm](glossary.md#authority-epoch) still chooses between the remaining ENSv1 and ENSv2 candidates.
  An unknown activated migration-path value blocks the Project generation as a
  data-integrity failure instead of silently hiding relations. A child whose
  arms disagree with no authority proof is omitted entirely. On Mainnet, an
  ENSv1 relation that survives parent reachability and
  was asserted after a proven ENSv2 child authority began blocks Project
  publication for that generation,
  though a positive ENSv2 registration in a locked parent's migration registry
  is itself entry history and therefore filters the ENSv1 relation before this
  assertion. The dual-current assertion remains a defensive generation check:
  an unmigrated parent can expose this contradiction, but no ordinary on-chain
  parent-and-child ENSv1→ENSv2 shape reaches it after parent reachability and
  migration-registry history are applied.
  Sepolia publishes the proof-selected child relation; extending the
  publication guardrail there is deferred until the connected Interpret→Project
  path is proven.
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L146-L164 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L293-L307 @ ens_v2@a971bd64)
  This route therefore never chooses one
  by recency, emits two rows for one logical
  child, or adds a row-local unsupported shape.
  A V1 child with getter-visible owner zero is omitted unless a current
  event-linked nonzero resolver independently establishes read reachability.
  Such a row has owner zero and no registrant or control registration; clearing
  the resolver removes it.
- Replaces (v1): `GET /v1/names/{namespace}/{name}/children`.

### `GET /v2/names/{name}/history`

- Method/path: `GET /v2/names/{name}/history`
- Tier: product read.
- Purpose: name history.
- Request parameters: path `name`; query `namespace`,
  `scope=name|registration|both`, `cursor`, `page_size`, and optional
  `finality=latest`. `at` and historical `finality` values are rejected by the
  shared latest-state collection rule.
- Response shape: `data` is an array of dedicated lean event rows:
  `{type, name, namespace, registration_id, block_number, timestamp,
  transaction_hash, log_index}`. `registration_id` carries actual registration
  lifecycle identity and is `null` when the event is not associated with a
  registration; reservation facts never carry one. The shared event-identity
  contract, including the committed companion change that adds `resource_id`
  for the underlying resource identity, is documented under
  [`GET /v2/events`](#get-v2events). Rows never include before/after
  state, raw normalized-event payloads, or a `data` change object. Friendly
  `type` vocabulary: `registration`, `renewal`, `release`, `expiry`,
  `transfer`, `authority`, `resolver`, `record`, `primary_name`, `permission`.
  Raw upstream or pipeline event kinds are diagnostics-only and are not emitted
  by this product route. Slice 1 excludes every correlation-dependent normalized
  row with `consumer_visibility=candidate`, including a familiar event kind whose
  existence depends on correlation under an existing source family; diagnostics
  may expose those rows. An [independently admitted
  event](glossary.md#independently-admitted-event) remains byte-for-byte
  activated and product-visible. Its separate
  candidate association is diagnostics-only and cannot suppress, duplicate, or
  reclassify that ordinary row. Only slice 2 consumer activation enables the
  per-source-log mapping specified for [`GET /v2/events`](#get-v2events) when an
  activated event is associated with the requested name or registration.
  Manifest or schema-vocabulary activation alone changes no name-history
  response. Candidate visibility is filtered in
  storage before summary calculation, keyset pagination, page-size limiting,
  cursor construction, or the later product-type mapping; candidate rows cannot
  consume a page slot or move an existing cursor. When one V1 registry resolver
  log is linked to both the registry resource retained for reads and a distinct
  control resource, this product route returns the control-resource row once.
  The additional normalized row that retains the registry resource link remains available from
  `GET /v2/diagnostics/events`; product suppression happens before cursor
  validation, summary calculation, and pagination. Without a distinct control
  resource, the sole registry-resource row remains product-visible.
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L94 @ ens_v1@91c966f)
- Pagination behavior: standard newest-first keyset pagination by chain
  position. The cursor is bound to the resolved namespace, parent name, scope,
  and sort. Product event-type filtering is applied before page construction,
  so `page_size`, `next_cursor`, and `has_more` describe product-visible
  events. A nonterminal page contains `page_size` rows; only the terminal page
  may be shorter.
- Scope behavior: `scope=name` reads name-surface events only,
  `scope=registration` reads registration-resource events associated with the
  requested name, and `scope=both` reads both sets. `scope` defaults to `both`.
  A V1 ownerless row linked only to the registry resource retained for reads is
  visible through name history with `registration_id=null` when it carries the
  name's `logical_name_id`. Name history returns a
  [pre-surface](glossary.md#pre-surface) owner row on a
  registry resource that was ever bound to the name under `scope=both` or
  `scope=registration`, even when the row was stored before the
  [name surface](glossary.md#surface-name-surface) existed and carries no name
  attribution. `scope=name` returns only rows carrying the name's
  `logical_name_id`. A row on a resource that was never bound to the name is
  reachable through `GET /v2/diagnostics/events` via the registry resource
  recorded internally at
  `name_current.provenance.read_reachability.serving_resource_id`.
- Snapshot behavior: the parent anchor and history rows are selected from
  current state. The response omits `meta.as_of` and `meta.as_of_token`, and
  its cursor carries no snapshot validity claim. True as-of history
  enumeration is deferred to the revision-bound storage follow-up.
- Status semantics: no product-visible matches return `200` with empty `data`,
  `page.next_cursor=null`, and `page.has_more=false`. Missing names return `404
  not_found`. Request and cursor-binding validation precede the first
  `redo_in_progress` check, so malformed requests retain `400`. The route
  captures the collection-wide check before parent lookup, then checks it again
  inside the repeatable-read page transaction. A missing parent returns `404`
  only when no redo is active and the captured generations are unchanged;
  otherwise the route returns retryable `409 stale`. Because the check is
  collection-wide, an active Interpret redo on any chain returns `409 stale`
  regardless of the requested namespace or name. The same response applies
  when either check sees an active redo or a redo began between the checks. A
  well-formed cursor whose event anchor is gone returns `400 invalid_input` when
  no redo intervened and `stale` when the redo check takes precedence.
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
  filters are combinable. Query `namespace`, `include=lineage`, `cursor`,
  `page_size`, and optional `finality=latest`. `at` and historical `finality`
  values are rejected by the shared latest-state collection rule.
- Response shape: `data` is an array of permission rows
  `{address, grant_scope, powers, registration_id, name?, authority_context,
  wrapper_state?, wrapper_fuses?}`. The two wrapper fields use the same atomic,
  [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word)
  contract as name detail and appear only for a returned current ENSv1 wrapper
  registration. Their presence does not widen wrapper-holder enumeration;
  request-relative completeness metadata below remains authoritative.
  `authority_context` is required on every row and records how that row was
  admitted under the per-name ownership rule.
  `include=lineage`
  adds route-local `lineage` per row:
  `{grant, revocation?, inheritance_path?, transfer_behavior?}`. Product lineage
  is a bounded summary; deep provenance stays on diagnostics authority/events
  routes. Lineage objects expose only allowlisted fields: `kind`,
  `registration_id`, `resolver: {chain_id, address}`, and `powers` when those
  fields apply. `kind` values are `event`, `permission`,
  `registration_authority`, `registration_rebound`, `ens_v1_authority`,
  `resolver_root_fallback`, and `registry_root_fallback`. Diagnostics-only
  storage keys such as event provenance, upstream/root resources,
  contract-instance ids, changed powers, and manifest versions are omitted.
  `grant_scope` is `{kind, detail}`. Detail is `{}` for `root`, `registry`,
  and `registration`;
  `{resolver: {chain_id, address}}` for `resolver` with numeric `chain_id`;
  and `{chain_id, manager}` for `record_manager`.
- Pagination behavior: standard collection pagination.
- Snapshot behavior: a `name` filter resolves its current registration anchor,
  and permission rows come from current state. The response omits `meta.as_of`
  and `meta.as_of_token`; completeness metadata remains available. Its cursor
  carries no snapshot validity claim. True as-of permission enumeration is
  deferred to the revision-bound storage follow-up.
- Status semantics: no matching permission rows returns `200` with empty
  `data`, including when a `name` filter has no registration anchor in the
  current state. Unsupported filter combinations return `422 unsupported`;
  pairing `name` with a `registration_id` that is not that name's selected
  current registration is not one of them. It is a supported query that selects
  nothing, so it returns `200` with empty `data`.
  A supplied `name` that is missing or unrecognized, whose current name is
  marked unsupported, or that resolves to a current name not bound to a
  registration resource cannot select a supported current registration. Its
  request-relative empty result returns `meta.completeness=partial` with
  `unsupported_reason=permission_support_unknown`; it does not prove that the
  name has no permission rows. By contrast, a resolved current name paired with
  an explicitly different `registration_id` is a supported, proven-empty
  selection, so its empty page has no `completeness` or `unsupported_reason`.
  The route reads current permission rows and summaries without claiming a
  request-wide immutable projection generation; current-state generation changes
  do not produce `409 stale`. When `name` or `registration_id` binds the read to a
  registration, the projection-owned
  per-registration permission summary classifies the result. Independently
  proven full support adds no completeness metadata. A non-wrapper resource
  whose standard operator, token-approval, or resolver-delegation paths are not
  fully served returns `meta.completeness=partial` with
  `unsupported_reason=approval_and_delegation_permissions_not_supported`.
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L118 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L78-L103 @ ens_v1@91c966f) A
  wrapper-only resource returns `meta.completeness=unsupported` with
  `unsupported_reason=wrapper_holder_permissions_not_supported`. Missing or
  unrecognized summary metadata returns `meta.completeness=partial` with
  `unsupported_reason=permission_support_unknown` and takes precedence. A mixed
  wrapper/non-wrapper request uses the approval/delegation partial reason. An
  address-only read is always at least `partial` with the approval/delegation
  reason, including for zero rows, unless missing or unrecognized summary
  metadata wins. Returned rows do not define the request denominator: zero rows
  do not prove that no account can mutate the selected name or registration.
  Projected rows are not suppressed by these classifications and remain useful,
  but neither the page nor a role summary is an authoritative permission
  enumeration while the partial marker is present. NameWrapper holder
  enumeration remains separately unsupported, and ENSv2 registry operator
  approval remains separately narrowed until indexed.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L575-L592 @ ens_v2@a971bd64) A `name` filter
  resolves only the selected current registration: a migrated name returns its
  ENSv2 permission rows, while an explicit `registration_id` can still select a
  retained historical ENSv1 registration for audit. An ENSv2 reservation does
  not select permission rows by `name` because current-registration
  classification has no owner evidence: the owner-zero branch emits
  `LabelReserved`, while minting the owner token and granting resource roles
  occur only in the registered branch.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464-L472 @ ens_v2@a971bd64)
  If bigname retains permission evidence attached to a resource for audit, an explicit
  `registration_id` read remains available with `resource_audit`; that marker
  does not claim the evidence is live for the reserved name. Every
  permission row carries the required `authority_context` field.
  `current_for_name` means a `name` filter selected the row's current
  registration for that requested name. A row admitted without a `name` filter,
  including an explicit-`registration_id` or address-filtered resource read, is
  `resource_audit` and makes no current-name claim even when it has an optional
  display `name`. Combining `name` with `registration_id` returns rows only when
  that registration is the selected current registration; a superseded pair is
  empty. Rows carrying `resource_audit` remain available in this collection.
  The marker changes only how that response may be interpreted; it does not
  change the registration's eligibility in a separate name-scoped view. The
  per-name ownership rule independently decides which registration contributes
  current authority, address relations, and role summaries, so a superseded
  ENSv1 registration is never selected while a current registration queried by
  resource can still contribute elsewhere. The collection adds no row-local
  coverage status or unsupported-reason vocabulary. A `name` filter whose
  exact-name projection is unsupported therefore selects no registration and
  returns `200` with empty `data`; callers use name detail or batch lookup for
  the explicit reason.
- Replaces (v1): `GET /v1/resources/{resource_id}/permissions`,
  `GET /v1/roles`, `GET /v1/names/{namespace}/{name}/roles`, and
  `GET /v1/resources/lookup`.

### `GET /v2/addresses/{address}/names`

- Method/path: `GET /v2/addresses/{address}/names`
- Tier: product read.
- Purpose: names related to an address.
- Request parameters: path `address`; query `namespace`, `relation`, `q`,
  `sort=name|expires_at|registered_at`, `order=asc|desc`,
  `dedupe=name|registration`, `include=role_summary`, `cursor`, `page_size`,
  and optional `finality=latest`. `at` and historical `finality` values are
  rejected by the shared latest-state collection rule.
  `q` applies prefix matching to the dictionary `name` field. The API treats
  the complete `q` value as an ENSIP-15 name prefix and normalizes it with the
  same normalizer used for indexed names before comparing it directly with the
  stored normalized name. An empty `q` is treated as absent. A partial final
  label is accepted when it is valid as a standalone ENSIP-15 label, so `q=AL`
  normalizes to `al`. When `q` has exactly one trailing dot, the preceding
  nonempty name is normalized and the dot is then restored, so
  `q=ALICE.` matches `alice.eth` but not `alicex.eth`. An empty name before the
  boundary marker, multiple trailing dots, an empty interior label, or any
  other input rejected by bigname's name validation atop ENSIP-15 returns
  `400 invalid_input`. This route does not accept `match`.
  `relation` accepts a comma-separated set of v2 vocabulary values
  `owner`, `manager`, and `registrant`; `any` normalizes to all three values.
  Rows match when any listed relation matches. The storage relations map as
  token-holder -> `owner`, effective-controller -> `manager`, and
  registrant -> `registrant`. `dedupe=name` groups by name surface and is the
  default; `dedupe=registration` groups by registration resource.
- Response shape: `data` is an array of record-shaped rows with `name`,
  `display_name`, `namespace`, `namehash`, `owner`, `registrant`,
  `registration_status`, `registered_at`, `created_at`, and `expires_at`.
  Address-name rows add `is_primary` and `relations`, where `relations` is the
  subset of `owner`, `manager`, and `registrant` that matched. `is_primary` is
  evaluated against that row namespace's coin-type-60 primary-name claim, not a
  route-wide namespace shortcut. The claim is compared in the same normalized
  form the indexed answer from `GET /v2/addresses/{address}/primary-name`
  publishes, so a successful claim recorded in a non-normalized spelling still
  marks its name primary. A spelling the projection already recorded as its
  normalized form is instead compared verbatim, so such a claim marks a row
  primary only where the published spelling is exactly that row's name. A
  successful claim whose stored spelling does not normalize likewise marks no
  row primary, and the primary-name route reports it as `invalid_name`.
  Resolver records are not included; use `GET /v2/names/{name}/records` for
  resolver data.
  `include=role_summary` adds
  `role_summary: [{address, grants: [{grant_scope, powers}]}]` grouped by the
  permission subject address and `record_count` when record inventory exists
  for the row. `record_count` counts the known record selectors for the name's
  current registration, including unsupported-family selectors and excluding
  explicit gaps. `grant_scope` uses the same shape documented for
  `GET /v2/permissions`.
- Pagination behavior: standard collection pagination. Cursors are bound to
  address, optional namespace filter, normalized relation set, `q`, dedupe
  mode, sort, and order.
- Snapshot behavior: address-name rows come from current state. The response
  omits `meta.as_of` and `meta.as_of_token`; completeness metadata for
  `include=role_summary` remains available. Its cursor carries no snapshot
  validity claim. True as-of address-name enumeration is deferred to the
  revision-bound storage follow-up.
- Status semantics: no related names returns `200` with empty `data`.
  Malformed addresses return `400 invalid_input`. Unsupported public namespaces
  return `404 not_found`. `include=role_summary`
  does not claim a request-wide immutable projection generation, and current-state
  generation changes do not produce `409 stale`. The expansion batch-loads
  projection-owned permission summaries for every
  registration on the served page. If all are independently proven full, no
  completeness metadata is added. A non-wrapper approval/delegation limitation
  returns `meta.completeness=partial`,
  `meta.unsupported_fields=["role_summary"]`, and
  `unsupported_reason=approval_and_delegation_permissions_not_supported`. An
  ENSv1 wrapper-only summary uses the same `partial` response classification and
  unsupported field with
  `unsupported_reason=wrapper_holder_permissions_not_supported`. Projected
  grants remain in `role_summary`, but the expansion is non-authoritative;
  therefore an empty summary is not a proven empty permission set. A mixed
  wrapper/non-wrapper page uses the approval/delegation reason. Missing or
  unrecognized summary metadata takes precedence and uses
  `permission_support_unknown`.
  Current address relations and
  `role_summary` are built only from the selected registration. A migrated
  name therefore stops relating its superseded ENSv1 holder or controller to
  the current name row. This collection adds no row-local mixed-authority
  status. When a current address relation is provable, the standing exception
  in [`api-v2.md`](api-v2.md#cursors-and-pagination) still lists the row even if other name
  coverage is unsupported. When no current authority can be proven, no current
  address relation can be established and the name is structurally absent;
  callers use name detail or batch lookup for its explicit coverage reason.
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
  `{address, coin_type, namespace, answers, verification?}` with `coin_type`
  serialized as a JSON number. `answers` is an
  array of `{source, status, name?, raw_claim_name?, unsupported_reason?,
  failure_reason?}` entries. When `source` is omitted, the route returns one
  entry for each answer source in stable `indexed`, then `verified` order;
  unsupported sources are represented by an entry with `status=unsupported`,
  not omitted.
  Supplying `source=indexed` or `source=verified` narrows the `answers` array
  to that source for single-source callers; every indexed entry comes from
  `bigname_phase.primary_names_current`, regardless of source selection. A
  successful stored raw claim is normalized for the indexed product name even
  when its raw spelling was not already normalized, unless the projection
  already recorded that spelling as its normalized form, in which case the
  stored spelling is published unchanged. `verification` is
  `{status, name?, unsupported_reason?, failure_reason?}` and appears when the
  fresh lookup produces a verification outcome. As an explicit exception,
  it also appears when the request includes the `verified` source and the
  live reverse claim fails the pre-forward normalization gate. That gate result
  represents the fresh hash-pinned reverse lookup and normalization decision,
  not a forward resolver call or a persisted execution trace.
  An indexed-only response never includes `verification`. The `verified` answer
  entry is the source-specific payload; `verification` is the typed comparison
  summary and must not contradict that entry. Claimed-vs-verified remains one
  call without `declared_state`/`verified_state`. When a served head is
  available, `meta.as_of` and `meta.as_of_token` record the served positions
  for staleness attribution and shadow-diff correlation. ENS/60 verification
  uses the schema-v2 lookup engine's current readable Ethereum position and
  pins its reverse and optional forward calls to that block hash. It persists
  neither a legacy trace/outcome nor a divergence row. When `source` is omitted,
  the indexed claim is read from `bigname_phase.primary_names_current` and
  returned beside the verified answer only when the current `chain_heads`
  position and exact completed `project` publication generation match the
  lookup before verified execution and remain unchanged after the indexed
  read; otherwise the request returns `409 stale`. Live results never change
  the indexed answer. Basenames verified primary-name lookup is unsupported;
  indexed Basenames responses remain Base-scoped.
- Pagination behavior: none.
- Snapshot behavior: current-state read over chain-derived primary-name state.
  The route does not accept `at` or `finality`. Successful responses carry
  `meta.as_of` and `meta.as_of_token` for indexed state or the current readable
  Ethereum position used by fresh ENS/60 verification. No metadata field
  implies cache reuse or a persisted execution identity. Provider transport
  failures abort the request with `500 internal_error`; they are not verified
  answer entries with `status=stale`. The projected-claim reads that decide
  whether a claimed name may be verified at all fail closed the same way: if
  those reads error the request returns `500 internal_error` rather than
  proceeding to live resolution, because a failed read is not evidence that no
  claim exists. The one exception is a missing exact-name projection: the
  verified source then reports the same in-band `unsupported` answer the indexed
  source reports for it, rather than failing, since neither source can speak for
  a projection that is not deployed.
- Verifiable authority: forward verification is refused for a name whose selected
  authority is an arm this deployment declares no execution entrypoint for. The
  refusal is an in-band `verified` entry with `status=unsupported` and
  `unsupported_reason=exact_name_authority_not_verifiable`, and no forward
  resolver call is dispatched. The refusal follows the name, not the source that
  named it: it applies both to a projected claim and to a name the live reverse
  leg returns, and in the live case the check runs after the reverse leg and
  before the forward call, so a refused name costs no forward dispatch and no
  CCIP-read follow. A present supported `name_current` row without its selected
  [authority arm](glossary.md#authority-epoch) is a projection anomaly and
  receives the same refusal. A name for which the current projection read finds
  no exact-name row in `name_current` is instead admitted to live forward
  verification. The projection cannot state which authority applies to a name
  outside indexed coverage, and live verification is the only answer path for
  it. A subname known only through offchain or wildcard live resolution has no
  exact-name row because a live provider answer does not materialize one;
  separately observed wildcard names can have projected surfaces.

  A missing readable row cannot generally hide a later authority arm from the
  execution this route performs. Live ENS/60 primary-name verification currently
  executes only against Mainnet, whose [deployment
  profile](glossary.md#deployment-profile) admits no ENSv2 registry, so
  there is no later arm to hide. Sepolia currently has no route execution
  entrypoint; the Sepolia evidence below explains its projected authority and
  the expected ENSv1-path behavior, not an active Sepolia verified route. An
  unwrapped ENSv1→ENSv2 migration clears the migrated node's ENSv1 resolver
  `(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111-L118 @ ens_v2@a971bd64)`,
  an unlocked wrapped ENSv1→ENSv2 migration does the same
  `(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146 @ ens_v2@a971bd64)`,
  and the locked path clears it when the name permits that change
  `(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L135-L144 @ ens_v2_sepolia_20260629@ccaeb58)`.
  The deployment script installs the ENSv2-backed wildcard resolver at the
  ENSv1 `eth` node
  `(upstream: .refs/ens_v2_sepolia_20260629/contracts/deploy/00_ENSV2Resolver.ts:L60-L81 @ ens_v2_sepolia_20260629@ccaeb58)`
  `(upstream: .refs/ens_v2/contracts/src/resolver/ENSV2Resolver.sol:L13-L14 @ ens_v2@a971bd64)`.
  The ENSv1 Universal Resolver walks up to an ancestor resolver
  `(upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L25-L38 @ ens_v1@91c966f)`
  and accepts that resolver through ENSIP-10
  `(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L63-L87 @ ens_v1@91c966f)`,
  so the ENSv1 path for a migrated name serves live ENSv2 state. Upstream tests
  prove that path directly; the end-to-end suite defines the shared ENSv1 and
  ENSv2 resolution check and invokes it before and after ENSv1→ENSv2 migration
  of unwrapped, unlocked wrapped, and locked names
  `(upstream: .refs/ens_v2_sepolia_20260629/contracts/test/integration/ENSV2Resolver.test.ts:L94-L124 @ ens_v2_sepolia_20260629@ccaeb58)`
  `(upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L133-L147 @ ens_v2@a971bd64)`
  `(upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L454-L458 @ ens_v2@a971bd64)`
  `(upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L546-L553 @ ens_v2@a971bd64)`
  `(upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L606-L613 @ ens_v2@a971bd64)`.
  The `eth`-node redirect is scripted intent plus a deployed Sepolia resolver in
  the pinned checkout
  `(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ENSV2Resolver.json:L2 @ ens_v2@a971bd64)`;
  the [`ens_v2` pin](../.refs/MANIFEST.toml) is scoped to the admitted
  2026-06-29 Sepolia deployment's archived evidence (upstream's 2026-07-30
  redeploy is not admitted) and does not establish a Mainnet redirect
  `(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/.deployment.json:L4 @ ens_v2@a971bd64)`
  `(upstream: .refs/ens_v2/contracts/deployments/sepolia/.deployment.json:L4 @ ens_v2@a971bd64)`.

  There is one known narrow exception. A locked name migrated with
  `CANNOT_SET_RESOLVER` burned keeps its ENSv1 resolver entry because the
  ENSv1→ENSv2 migration cannot clear it. When that entry names a listed
  PublicResolver, the ENSv1 side continues to serve its retained records while
  ENSv2 selects the replacement resolver
  `(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L137-L175 @ ens_v2_sepolia_20260629@ccaeb58)`.
  The ENSv1 PublicResolver derives ordinary write authority from the registry or
  wrapped token owner and their approvals, while separately authorizing its
  trusted ETH controller and reverse registrar
  `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114-L128 @ ens_v1@91c966f)`,
  requires that authority for record writes
  `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L65 @ ens_v1@91c966f)`,
  and keeps existing records readable without that write check
  `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L68-L84 @ ens_v1@91c966f)`.
  Moving the wrapped token to the graveyard therefore freezes ordinary writes
  by the former name owner, but it does not revoke those separately trusted
  callers.
  This locked-name `CANNOT_SET_RESOLVER` case is not reachable through the
  current Mainnet-only execution path. If a deployment with the Sepolia redirect
  gains a verified route entrypoint, a name outside indexed coverage would be
  admitted and could verify those retained records. Inside coverage, the name's
  `ens_v2`-selected row would cause the refusal above, so the exposure would close
  as indexing coverage completes.
- Status semantics: answer entries use in-band `status`. Valid tuples with no
  indexed claim return an `indexed` entry with `status=not_found`. A stored
  successful claim whose spelling does not normalize returns an `indexed` entry
  with `status=invalid_name` and `raw_claim_name`, the same answer the
  projection's own `invalid_name` classification produces; that row marks no
  name primary on the address-name and reverse-lookup collections. Unsupported,
  not-found, failed, and mismatched verified outcomes return `200` with the
  corresponding `verified` entry status. When the requested output includes the
  verified source, a successful live claim whose raw spelling
  differs from its normalized form produces a verified answer and
  `verification` with `status=not_found` and
  `failure_reason=claim_not_normalized`. An unnormalizable live claim
  leaves a missing indexed tuple as `status=not_found` and produces a verified
  answer plus `verification` with `status=not_found` and
  `failure_reason=claim_name_not_normalizable`. A completed JSON-RPC failure,
  malformed response, or configured provider or CCIP-Read gateway response
  timeout produces an in-band verified `status=failed` result. Missing provider
  configuration or a selected-block rejection returns whole-request `409
  stale`. A provider or gateway connect-phase timeout, DNS failure, TLS
  failure, connection reset, or other transport failure returns whole-request
  `500 internal_error`; no trace or outcome is persisted, so the next read
  retries. Malformed addresses return `400 invalid_input`.
  `source=indexed` does not enter verified-execution rate or concurrency
  admission; omitted `source` and `source=verified` do because they run the
  fresh lookup. Forward verification requires the claimed name's selected
  exact-name authority and declines in three cases. A claim whose exact-name
  projection is unsupported returns the in-band unsupported result carrying
  that projection's own public reason, the same reason name detail serves for
  the row. A present supported row with no selected authority arm is a projection
  anomaly and returns `exact_name_authority_not_verifiable`. A claim the
  projection supports whose selected authority is the ENSv2 arm returns that
  same reason: no manifest declares an ENSv2 execution entrypoint, so this route
  has no ENSv2 forward-resolution path and declines rather than resolving the
  name through the ENSv1 universal resolver its own authority selection has
  already ruled out. The ENSv2 case needs a deployment profile that can support
  an ENSv2 selection at all; where the deployment profile shadows the ENSv2 arm,
  the name is already unsupported and takes the first case instead. None of the
  three cases dispatches a forward resolver call. A live reverse claim has
  already used its two reverse-leg provider calls before the name-level refusal
  is known. A consumer reads `unsupported_reason` to distinguish a projected
  coverage refusal from the shared authority-not-verifiable refusal.
- Replaces (v1): `GET /v1/primary-names/{address}`.

### `GET /v2/addresses/{address}/history`

- Method/path: `GET /v2/addresses/{address}/history`
- Tier: product read.
- Purpose: address activity history.
- Request parameters: path `address`; query `namespace`, `relation`,
  `scope=name|registration|both`, `cursor`, `page_size`, and optional
  `finality=latest`. `at` and historical `finality` values are rejected by the
  shared latest-state collection rule.
  `namespace` defaults to `ens` when omitted. `relation` accepts a
  comma-separated set of `owner`, `manager`, and `registrant`; `any`
  normalizes to all three values. Rows match when any listed relation matches.
- Response shape: `data` is an array of compact event rows using the shared
  friendly `type` vocabulary and the event-identity contract documented under
  [`GET /v2/events`](#get-v2events). The correlation-scoped candidate
  visibility rule from name history also applies here: slice 1 changes no
  address-history row,
  and slice 2 consumer activation admits correlation-dependent rows. An
  independently admitted ordinary row remains visible while its candidate
  association remains diagnostics-only. Storage applies
  the visibility predicate before deriving address anchors from ownership or
  control events, constructing selectors, validating cursors, calculating
  summaries, or selecting and paginating final rows. A candidate row therefore
  cannot expose an older activated row by broadening the anchor set. If those
  anchors reach both rows emitted for one V1 registry resolver log, product
  history keeps the control-resource row and suppresses the additional row
  carrying the registry resource link before cursor validation and pagination; raw diagnostics keeps
  both. Without a distinct control resource, the sole registry-resource row
  remains visible.
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L94 @ ens_v1@91c966f)
- Snapshot behavior: address-history rows come from current state. The response
  omits `meta.as_of` and `meta.as_of_token`, and its cursor carries no snapshot
  validity claim. True as-of/finality row-bounding is deferred to the
  revision-bound storage follow-up.
- Pagination behavior: product event-type filtering runs before newest-first
  keyset page construction, so `page_size`, `next_cursor`, and `has_more`
  describe product-visible events. A nonterminal page contains `page_size`
  rows; only the terminal page may be shorter.
- Status semantics: no product-visible matches return `200` with empty `data`,
  `page.next_cursor=null`, and `page.has_more=false`. Address, namespace, and
  cursor-binding validation precede the first `redo_in_progress` check, so
  malformed addresses retain `400 invalid_input` and unsupported public
  namespaces retain `404 not_found`. The route checks `redo_in_progress` before
  deriving address relation anchors and rechecks it inside the repeatable-read
  page transaction. After the transaction commits, the route resolves display
  names and revalidates the captured redo state before returning data. This
  check is collection-wide: an active Interpret redo on
  any chain returns retryable `409 stale` with no `data` page, regardless of the
  requested namespace. The same response applies when a redo began between the
  checks. A well-formed cursor
  whose event anchor is gone returns `400 invalid_input` when no redo intervened
  and `stale` when the redo check takes precedence.
- Replaces (v1): `GET /v1/history/addresses/{address}`.

### `GET /v2/search`

- Method/path: `GET /v2/search`
- Tier: product read.
- Purpose: name search and suggestions. No availability or pricing semantics.
- Request parameters: query `q`, `match=prefix|contains` default `prefix`,
  `namespace`, `cursor`, `page_size`, and optional `finality=latest`. `at` and
  historical `finality` values are rejected by the shared latest-state
  collection rule.
- Response shape: `data` is an array of record-shaped name search results in
  dictionary vocabulary. Each result is built only from the selected current
  registration: a migrated name uses its ENSv2 owner, registrant, status, and
  expiry. A name whose exact-name projection is unsupported is omitted from
  search results whatever the reason, including a mixed-history name with no
  provable current authority. Search carries no row-local status or
  unsupported-reason field, so it omits such a name rather than serving
  registration fields no selected authority backs; callers use name detail or
  batch lookup when they need an omitted name's explicit coverage reason. The
  omission is applied before paging, so returned counts, page order, and cursor
  continuation all reflect the same filtered set.
- Pagination behavior: standard collection pagination. Without an explicit
  namespace, the cursor binds the deployment-derived namespace set and is
  rejected if that set changes.
- Snapshot behavior: search rows come from current state. The response reports
  readable request-scope positions in `meta.as_of` and reports each suppressed
  request-scope chain in `meta.as_of_completeness` with
  `completeness=unsupported` and
  `unsupported_reason=temporarily_unavailable`. A bare request accounts for
  every active public namespace's chains. An explicit namespace accounts only
  for that namespace's chains, even when another public namespace is readable.
  Returned search rows do not change that denominator. The response omits
  `meta.as_of_token`, and its cursor carries no snapshot validity claim. True
  as-of search enumeration is deferred to the
  revision-bound storage follow-up. Bare search reloads the active manifest
  declarations, selected authority chain heads, project generations, and
  Interpret redo state after reading its page; a redo that begins mid-request
  or another captured-state change returns the existing retryable `409 conflict`,
  never a partial page. Explicit-namespace search likewise captures its
  request-scope metadata before reading the page and reloads it afterward; a
  head, completed publication generation, or readiness change returns `409
  conflict` rather than attributing the page to a later position. An explicit
  namespace returns retryable `409 stale` whenever an Interpret redo is among
  the failed admission terms, including when the redo begins during the read.
  A Project publication that becomes ready between admission reads with no
  redo involved is instead a readiness change and returns `409 conflict`.
- Status semantics: no matches returns `200` with empty `data`. `q` is
  required; a missing or empty `q` returns `400 invalid_input`. The API treats
  `q` as an ENSIP-15 name fragment, normalizes it, and then applies the selected
  `match=prefix|contains` byte comparison directly to stored names. Invalid
  normalization returns `400 invalid_input`. A single trailing dot remains
  preserved as a label boundary after the preceding nonempty name is normalized,
  matching the address-names `q` behavior documented above. With
  `match=contains`, one leading dot is also accepted when it is followed by a
  nonempty fragment that does not begin with another dot. The following fragment
  is normalized as usual, and the leading dot is preserved for matching. Thus
  `.eth`, `eth.`, `.eth.`, and `th.e` are accepted contains fragments, while `.`
  and `..` return `400 invalid_input`. Leading-boundary support is specific to
  `match=contains`; `match=prefix` fragment behavior is unchanged and does not
  accept a leading dot. An explicit
  recognized namespace bypasses public namespace derivation and reads its
  current rows without a deployment-readiness gate, while its metadata still
  discloses a request-scope chain suppressed by that gate. A disclosure change
  during the read returns `409 conflict`. Bare search excludes a namespace
  while its selected authority chain has Interpret
  `redo_in_progress=true`, regardless of redo mode, and returns `409 conflict`
  when no public namespace is ready or when its captured deployment state
  changes during the read.
- Replaces (v1): search, suggestion, and exact-name-filter uses of
  `GET /v1/names`; exact name profiles move to `GET /v2/names/{name}`.

### `GET /v2/events`

- Method/path: `GET /v2/events`
- Tier: product read.
- Purpose: compact event search across name, address, registration, type, and
  block filters.
- Request parameters: query `namespace`, `name`, `address`,
  `registration_id`, `type`, `from_block`, `to_block`, `cursor`, `page_size`,
  and optional `finality=latest`. `at` and historical `finality` values are
  rejected by the shared latest-state collection rule. When `name` is present
  and `namespace` is omitted, namespace is inferred from the name; `namespace`
  defaults to `ens` only when there is no name filter.
- Response shape: `data` is an array of compact event rows with friendly
  `type` vocabulary. Raw upstream event kinds are diagnostics-only. Event-row
  identity uses two fields with distinct meanings. `registration_id` is actual
  registration lifecycle identity: it is set only when the event is associated
  with a registration lifecycle and is `null` otherwise. `resource_id` carries
  the underlying resource identity, including non-registration resources such
  as reserved entries. A reservation fact carries `resource_id` but never
  `registration_id`; resource identity is not registration authority. The
  product `registration_id` filter likewise excludes V1 ownerless rows linked
  only to the registry resource retained for reads. Raw diagnostics keeps that
  resource attribution. The filter still returns resource-less events for names
  bound to the requested registration. The served API currently exposes the old single-field shape, with
  `registration_id` only; the field change is the committed contract and lands
  in an immediate companion change. The
  slice-2 consumer activation contract maps each
  [migration correlation group's](glossary.md#migration-correlation-group)
  renewal-bridge and correlated registrar normalized rows to `renewal` and
  `expiry`, Graveyard claims to `release`, and controller membership changes
  to `permission`. Mapping is per normalized source event and resource anchor,
  not per transaction. A synchronized renewal emits one row for each real
  lifecycle owner/resource participating in renewal, so one synchronized
  renewal transaction contains two `renewal` rows — the renewal-bridge arm and
  the ENSv1-registrar arm — and two `expiry` rows. Reservation-scoped state
  changes remain reservation/resource facts and do not produce registration
  renewal rows; no synthetic collapsed renewal is created. The candidate
  `MigrationApplied` and
  `ContractDiscovered` kinds have no product event type. During slice 1, every
  correlation-dependent row carrying `consumer_visibility=candidate` is excluded
  even if its familiar event kind would otherwise map above or its source family
  is `ens_v2_registry_l1`. An event admitted independently by an existing family
  stays byte-for-byte activated and product-visible; its candidate
  `migration_event_associations` row is diagnostics-only and never changes the
  ordinary event's inclusion or multiplicity. Diagnostics may expose candidate
  rows and associations immediately. Schema-vocabulary,
  manifest-family, backfill, and interpretation activation alone change no
  `/v2/events` or product-history response; only slice 2 changes visibility.
  The shared visibility predicate runs before keyset pagination, page-size
  limiting, cursor construction, and product-type mapping. A V1 registry
  resolver log linked to both the registry resource retained for reads and a
  distinct control resource appears once in this product route: the
  additional row carrying the registry resource link is suppressed before cursor validation and
  pagination, while the control-resource row remains visible and raw
  diagnostics retain both normalized rows. Without a distinct control resource,
  the sole registry-resource row remains product-visible.
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L94 @ ens_v1@91c966f)
  (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/AbstractETHRegistrar.sol:L84 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/AbstractETHRegistrar.sol:L91 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/AbstractETHRegistrar.sol:L92 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/AbstractETHRegistrar.sol:L93 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L212 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L226 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L227 @ ens_v2@a971bd64) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L106 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L107 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L111 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L132 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L134 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L8 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L9 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L157 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L160 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L162 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L169 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRenewerV1.json:L110-L158 @ ens_v2@a971bd64)
- Pagination behavior: standard newest-first keyset pagination. Product
  event-type filtering runs before page construction, so `page_size`,
  `next_cursor`, and `has_more` describe product-visible events. A nonterminal
  page contains `page_size` rows; only the terminal page may be shorter.
- Snapshot behavior: event rows come from current state. The response omits
  `meta.as_of` and `meta.as_of_token`, and its cursor carries no snapshot
  validity claim. True as-of/finality row-bounding is deferred to the
  revision-bound storage follow-up.
- Status semantics: no product-visible matches return `200` with empty `data`,
  `page.next_cursor=null`, and `page.has_more=false`. Filter and cursor-binding
  validation precede the first `redo_in_progress` check, so malformed requests
  retain `400 invalid_input`. The route checks `redo_in_progress` before deriving
  name, registration, or address anchors and rechecks it inside the
  repeatable-read page transaction. After that transaction commits, the route
  resolves display names and revalidates the captured redo state before
  returning data. This check is collection-wide: an active
  Interpret redo on any chain returns retryable `409 stale` with no `data` page,
  regardless of the requested filters or namespace. The same response applies
  when a redo began between the checks. A
  well-formed cursor whose event anchor is gone returns `400 invalid_input` when
  no redo intervened and `stale` when the redo check takes precedence.
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
  Those rows use the same optional, atomic `wrapper_state` and `wrapper_fuses`
  contract as exact-name detail; the fields are present only for a current
  ENSv1 NameWrapper registration at the served projection timestamp.
  Once exact-name authority is activated, `bound_names` includes a logical
  name only under the resolver selected by its current registration. A
  migrated name is absent from its superseded ENSv1 resolver's listing; a
  mixed-history name with no provable current authority is omitted from all
  resolver listings rather than forced to `ok`. This nested collection adds no
  row-local mixed-authority status, so callers use name detail or batch lookup
  for the explicit coverage reason. A row classified as
  `current_authority_not_projected` is also absent from `bound_names`; retained
  resolver-pointer evidence does not establish listing membership.
  An ownerless ENSv2 reservation is likewise absent: a retained reservation
  resolver or former-resource pointer is not a resolver selected by a current
  registration. This intentionally narrows ENSv2, which stores and returns a
  reservation resolver until expiry.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @ ens_v2@a971bd64)
  A positively classified ownerless ENSv1 or Basenames registry row is
  different: its event-derived resolver
  binding is eligible for `bound_names` only where that resolver family's
  existing binding-enumeration capability is supported.
  `counts.nodes`, `counts.aliases`, and `counts.role_holders` are total counts,
  while the corresponding `include=nodes`, `include=aliases`, and
  `include=roles` arrays are deterministic samples of at most 100 items. A
  count greater than the returned array length means that sample is truncated;
  omitted binding rows remain available through paginated name-side routes,
  and omitted permission rows remain available through permission routes.
  Resolver alias-event mappings have no exhaustive product collection: when
  their sample is truncated, the total count reports the omitted mappings but
  clients cannot page through them on this route. Binding samples sort by name
  and stable identity, alias samples place current binding aliases before
  current alias-event rows and preserve each group’s stable order, and
  role-holder samples sort by address.
  `include=roles` items are `{address, registration_count, permission_count,
  powers}`. `registration_count` is the number of distinct registrations with
  resolver-scoped permission rows for the role address. `permission_count` is
  the number of those permission rows, not the number of powers expanded from
  them; a row granting multiple powers counts once. The former embedded
  `registration_ids` list is omitted because it was itself unbounded, and
  permission rows remain queryable through `GET /v2/permissions` using the
  returned addresses. The resolver arrays are not independently pageable. This
  bounded-sample contract is
  the consumer-visible resolver-overview shape change delivered with
  [issue #401](https://github.com/ensdomains/bigname/issues/401); clients must
  not interpret an included array as exhaustive when its total count is
  larger.
  `include=aliases` exposes binding rows as `{namespace, name, display_name,
  namehash}` and resolver alias rows as `{namespace, from_name, to_name,
  from_display_name?, to_display_name?, state, resolver: {chain_id, address},
  to_registration_id?}`. `to_name` is `null` when the latest alias state is
  `removed` or `unknown`. `include=events` exposes `{count, by_type}` where
  `by_type` aggregates raw resolver event kinds that map to the same friendly
  `type` vocabulary as `GET /v2/events`; raw kinds without a product event type
  remain included in `count` but are excluded from `by_type`.
- Pagination behavior: standard collection pagination applies to the
  nested `bound_names.page` object. The top-level response has no `page`.
- Snapshot behavior: the resolver overview and bound names read
  `bigname_phase` projections from one completed projection-phase generation. A row
  target may precede the selected position when the row was unchanged by later
  incremental publications; it may not be ahead, and a same-height target must
  match the selected hash. The projection-phase generation is revalidated after the
  read, and an invalid target or changed generation returns `409 stale`.
- Status semantics: only a request without `at` and with `finality=latest`
  applies the latest served-head Interpret-redo check and returns retryable `409
  stale` while its selected chain is undergoing a redo. Historical `at` reads
  and requests with `finality=safe|finalized` retain their existing generation
  validation without that redo check.
- An otherwise valid current/latest resolver with no overview
  row returns `404 not_found`. For `at`, `safe`, or `finalized`, a missing
  current projection cannot prove historical absence and returns `409 stale`.
  Bound-name listings under `at`, `safe`, or `finalized` are drawn from
  current-state projections: a name bound at the requested position but
  unbound since is absent from the listing rather than flagged, consistent
  with the coverage `exhaustiveness: not_asserted` disclosure.
  A resolver overview with no bound names returns `200` with an empty
  bound-names section. Malformed `chain_id` or `address` returns `400
  invalid_input`.
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
  omits `meta.as_of` and `meta.as_of_token`. Under the Sepolia deployment
  profile, ENS `name_profile` completeness is `partial`: the ENSv2 registrar
  declaration is supported while the admitted ENSv1 registrar declaration is
  shadow because registrar-controller label coverage is absent.
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
  and `/v2/diagnostics/names/{name}/records` accept `at` and `finality` and
  carry `meta.as_of`/`meta.as_of_token` because they explain one selected
  snapshot.
- `/v2/diagnostics/events` follows the shared latest-state collection rule: it
  omits snapshot metadata and rejects `at` and historical `finality`.
- `/v2/diagnostics/namespaces/{namespace}/manifests` omits `meta.as_of` and
  `meta.as_of_token`; it is control-plane metadata.

`GET /v2/diagnostics/names/{name}/execution` is removed. The persisted-explain
capability it served is retired with the C2 cutover, not deferred to a later
slice: the execution traces, steps, and cache outcomes it read no longer exist
and no replacement route is planned. Verified resolution now runs per request,
so there is no persisted artifact to explain. See
[`execution.md`](execution.md#removed-legacy-artifacts).

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
- Request parameters: path `name`; query `namespace`, `at`, `finality`, and
  optional `keys`. `keys` uses the same record-key grammar as
  `/v2/names/{name}/records`: `addr:<coin_type>`, `text:<key>`, `avatar`, and
  `contenthash`.
- Response shape: `data` is
  `{record_inventory, record_cache, value_sources, comparison,
  comparison_explicit_gaps?}`.
  `record_inventory` is
  `{record_version_boundary, enumeration_basis, selectors, explicit_gaps,
  unsupported_families, last_change}` using the existing diagnostic selector
  row fields `record_key`, `record_family`, `selector_key`, and `cacheable`.
  `record_cache` is `{record_version_boundary, entries}` where each entry is
  `{record_key, record_family, selector_key, status, value?,
  unsupported_reason?, failure_reason?}`. `value_sources` summarizes the
  indexed or verified origin per key. `comparison` is keyed by `record_key` and
  carries side-by-side `{indexed, verified}` record answers for the former
  `mode=both` workflow. Without `keys`, `comparison` defaults to the first 16
  inventory-derived supported record keys in deterministic order. The indexed
  `record_inventory` and `record_cache` sections remain complete, including
  retained audit state that product name routes omit when no current
  registration exists. When more
  than 16 default comparison keys are available, `comparison_explicit_gaps`
  lists each uncompared selector as
  `{record_key, record_family, selector_key, gap_reason}` with
  `gap_reason=diagnostics_comparison_default_limit_exceeded`. With `keys`, the
  comparison is scoped exactly to the requested keys. On-demand verified
  comparison execution is chunked to at most 4 in-flight selector RPCs per
  diagnostics request burst. Identity objects in these diagnostics use dictionary
  spellings (`namespace`, `name`, `display_name`, `registration_id`), while
  pipeline-only identifiers such as `normalized_event_id` keep their pipeline
  names per the tier-3 rule.
  `record_cache.entries` is an internal projected representation, not the
  product-route answer envelope. A retained contenthash entry uses
  `status="success"` with
  `value={"encoding":"hex","bytes":"0x..."}`; a retained address entry uses
  `status="success"` with scalar `value="0x..."`. A cleared contenthash or
  address entry uses `status="not_found"` and omits `value`. Normalized fields
  such as `contenthash_hex` and `address_bytes_hex` are not product
  record-answer fields; they can appear inside the complete normalized
  `after_state` returned by the raw event diagnostics route below.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`.
- Replaces (v1): record inventory/cache diagnostics formerly embedded in
  `GET /v1/profiles/names/{name}` and
  `GET /v1/names/{namespace}/{name}/records`, including the former
  `mode=both` comparison.

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
  `registration_id`, `type`, `from_block`, `to_block`, `cursor`, `page_size`,
  and optional `finality=latest`. `at` and historical `finality` values are
  rejected by the shared latest-state collection rule. When `name` is present
  and `namespace` is omitted, namespace is inferred from the name; `namespace`
  defaults to `ens` only when there is no name filter.
- Response shape: `data` is an array of raw normalized-event rows in
  diagnostics vocabulary:
  `{normalized_event_id, event_identity, namespace, name?, registration_id?,
  event_kind, source_family, manifest_version?, source_manifest_id?,
  chain_position, transaction_hash, log_index, raw_fact_ref, derivation_kind,
  canonicality_state, before_state?, after_state?, provenance, coverage}`. The
  slice-1 diagnostics extension adds `consumer_visibility`,
  `migration_correlation_ids`, and `migration_associations?`; each
  `migration_associations` entry is
  `{migration_correlation_ids, correlation_kind, consumer_visibility}`. A
  correlation-dependent normalized row reports its marker in the top-level
  fields. An independently admitted ordinary row reports top-level
  `consumer_visibility=activated` and an empty ID set; its separate candidate or
  activated correlation relationships appear only in `migration_associations`.
  `migration_associations` is raw diagnostic evidence: each association remains
  anchored to the chain lineage where it was derived, while the lookup attaches
  every association with the same `event_identity`. The top-level
  `canonicality_state` applies only to the returned normalized-event row; it does
  not filter its associations. Retained associations from replaced forks can
  therefore appear beside a canonical event. Consumers that require canonical-only
  correlation must not treat association presence as a current relationship.
  When interpretation links one V1 registry resolver log to both the registry
  resource retained for reads and a distinct control resource, diagnostics
  returns both normalized rows and permits cursors anchored to either row;
  product event and name-history routes return that on-chain log once through
  the control-resource row. Without a distinct control resource, the sole
  registry-resource row remains product-visible.
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L94 @ ens_v1@91c966f)
  When `address` is present, diagnostics derives its name/resource anchor set
  from both activated and candidate address-relation evidence. Candidate
  evidence never contributes anchors to `/v2/events` or product history routes.
  A behavior-preserving full re-walk may assign a different numeric
  `normalized_event_id` to a pre-existing row while its `event_identity` and
  pre-existing semantic fields remain stable; the numeric ID change and the
  candidate fields are explicit diagnostic-only deltas. The intentional
  #348/#529 interpreter changes are the documented exception above:
  `RecordChanged` and
  `RecordVersionChanged` can gain `logical_name_id`, keep `resource_id=null`,
  update `raw_fact_ref.interpreter_state_key`, and rethread `before_state` on
  the same resolver event identity.
- Pagination behavior: standard collection pagination.
- Snapshot behavior: diagnostic event rows come from current state, but their
  `migration_associations` are the raw lineage evidence described above, not
  assertions of current correlation. The response omits `meta.as_of` and
  `meta.as_of_token`, and its cursor carries no snapshot validity claim. True
  as-of/finality row-bounding is deferred to the revision-bound storage
  follow-up.
- Status semantics: no matching rows returns `200` with empty `data`.
- Replaces (v1): `view=full` on `GET /v1/history/names/{namespace}/{name}`,
  `GET /v1/history/resources/{resource_id}`, and
  `GET /v1/history/addresses/{address}`.
