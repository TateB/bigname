# API v2

Development-time contract for the API surface accepted in
[ADR 0006](adrs/0006-api-v2-product-surface.md). Per-route reference lives in
[`api-v2-routes.md`](api-v2-routes.md). This surface has no generated OpenAPI
artifact.

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

The binary serves this contract under `/v2`; the old v1 REST surface has been
deleted. The production edge does not expose `/v2` until the maintainer-gated
C3 edge flip. These docs define the currently served internal REST contract,
not the public-edge rollout state.

## GraphQL compatibility

`POST /graphql` is governed by
[`consumer-capabilities.md` § GraphQL compatibility](consumer-capabilities.md#graphql-compatibility),
including its generated-style roots, local extensions, and explicit unsupported
behavior. This document does not define a second GraphQL contract.

The generated `Domain_filter` serves the complete upstream ID and name operator
families with conjunctive semantics, direct comparison against the served
`Domain.id` and `Domain.name` values, and separate case-sensitive and nocase
pattern operators. Raw-name comparisons and name ordering use expression-local
PostgreSQL `COLLATE "C"`; canonical fixed-width namehash operands use the
database-collation index, while noncanonical ID ranges retain C semantics on a
linear path. Pattern input retains SQL `%`, `_`, and backslash semantics. For
the generated ID/name families, explicit null equality is
distinct from omission, while explicit null on the other operators is rejected.
Generated order ties use the Domain ID in the requested direction. The generated
`Domain_orderBy` exposes only the exact values listed
in the capability contract. The legacy `DomainFilter` behavior and local
`registrationDate` ordering remain separate extensions.
Graph Node generates these ID and String operator families (upstream:
.refs/graph_node/graph/src/schema/api.rs:L872-L912 @ graph_node@aefe173), and the
pinned ENS subgraph declares `Domain.id` as `ID!` and `Domain.name` as `String`
(upstream: .refs/ens_subgraph/schema.graphql:L1-L7 @ ens_subgraph@723f1b6).

## Naming Dictionary

Normative one-name-per-concept dictionary from ADR 0006, extended with the
step-3-gate vocabulary needed by the route schemas:

| `v2` name | Meaning | Replaces (`v1`) |
| --- | --- | --- |
| `name` | the ENSIP-15 normalized name string, except on routes that document an explicit [non-name form](glossary.md#non-name-form) for a label bigname cannot state as a name — today only `GET /v2/names/{name}/subnames` | `normalized_name`, `logical_name_id` (derivable as `namespace:name`) |
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
| `wrapper_state` | bigname's current ENSv1 NameWrapper lifecycle value: [`wrapped`](glossary.md#wrapped-namewrapper-state), [`emancipated`](glossary.md#emancipated-namewrapper-state), or [`locked`](glossary.md#locked-namewrapper-state); omitted when the current name is not in one of those states | raw NameWrapper fuse bitmap |
| `wrapper_fuses` | typed summary of the current [expiry-effective NameWrapper fuse word](glossary.md#expiry-effective-namewrapper-fuse-word); present exactly when `wrapper_state` is present | raw NameWrapper fuse bitmap |
| `fuses` | uint32 fuse word nested in `wrapper_fuses`; it is zero after wrapper expiry even though normalized events retain their expiry-unadjusted interpreted word | raw NameWrapper fuse bitmap |
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
| `as_of_completeness` | per-chain positions suppressed from `as_of`, keyed by `chain_id`, with `{completeness, unsupported_reason}` | inferring request coverage from whichever rows happened to be returned |
| `as_of_token` | opaque URL-safe snapshot token for replaying the exact served positions with `at` | reconstructing `at` from `chain_positions` |
| `at` | snapshot selector parameter for routes that support point-in-time reads | `chain_positions` query parameter and timestamp-specific ad hoc selectors |
| `include` | route-documented expansion allowlist | comma-separated expansion flags, `meta` knobs, and route-specific include flags |
| `sort` | route-documented sort field | `sort` (unchanged; allowed fields are now route-documented) |
| `order` | sort direction, `asc` or `desc` | `order` (unchanged) |
| `scope` (history) | `name`, `registration`, `both` | `surface`, `resource`, `both` |
| `grant_scope` | the protocol scope of a permission row: `root`, `registry`, `registration`, `resolver`, or `record_manager` | permission-row `scope` (renamed so history `scope` and permission scope are two names for two concepts) |
| `verification` | typed checked-answer summary for claimed-vs-verified answers | `verified_state`, `verified_primary_name` section wrappers |
| `status` | one result vocabulary: `ok`, `not_found`, `invalid_name`, `mismatch`, `unsupported`, `stale`, `failed` | `ResultStatus`, `IdentityStatus`, `NameRecordStatus`, `unnormalizable_input` (folds into `invalid_name`); `mismatch` kept for verification results |
| `unsupported_reason` | reason code or short reason string required with `status=unsupported` | `coverage.unsupported_reason`, route-specific unsupported details |
| `failure_reason` | reason code or short reason string for `failed`, `stale`, `not_found`, or `mismatch` details | route-specific failure detail fields |
| `completeness` | `full`, `partial`, `unsupported` | `coverage.status` on product routes (full taxonomy moves to diagnostics) |
| `powers` | effective permission powers; storage `resource_control` is exposed as `registration_control`; ENSv2 registry `was_reserved` is a non-authorizing history marker retained here so marker-only transitions remain visible (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L47-L48 @ ens_v2@a971bd64) | `effective_powers` |
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
| `authority_context` | required permission-row marker from the [per-name ownership rule](consumer-capabilities.md#ensv1ensv2-mixed-history-ownership); [`current_for_name`](glossary.md#current-for-name-authority-context) means a `name` filter selected the current registration, while [`resource_audit`](glossary.md#resource-audit-context) makes no current-name claim | new in v2 |
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

`GET /v2/permissions` and `GET /v2/addresses/{address}/names?include=role_summary`
read current permission rows and per-resource permission summaries. Canonical
identity checks exclude rows from an orphaned chain lineage. These routes do
not claim a request-wide immutable projection generation, and their cursors carry
no snapshot-validity claim. The base v2 address-name collection remains available
without the expansion.

Permission-backed v2 reads also classify the served resources from the typed
projection-owned per-resource permission summary. For a resource-bound
`GET /v2/permissions` read, a non-wrapper summary whose standard operator,
token-approval, or resolver-delegation paths are not fully served produces
`meta.completeness=partial` with
`approval_and_delegation_permissions_not_supported`. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L118 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L78-L103 @ ens_v1@91c966f) An ENSv1 wrapper-only
summary instead produces `meta.completeness=unsupported` with
`wrapper_holder_permissions_not_supported`. A missing or unrecognized summary
produces `meta.completeness=partial` with `permission_support_unknown`, which
takes precedence over both known limitations.

A supplied `name` that is missing or unrecognized, whose current name is marked
unsupported, or that resolves to a current name not bound to a registration
resource returns an empty result relative to that request with `meta.completeness=partial` and
`meta.unsupported_reason=permission_support_unknown`. This establishes only
that the API could not select a supported current registration; it does not
establish that the name has no permission rows. A supplied current name paired
with an explicitly different `registration_id` is instead a supported,
proven-empty selection. Its empty page has neither `meta.completeness` nor
`meta.unsupported_reason`.

An address-only permissions read is always at least `partial` with
`approval_and_delegation_permissions_not_supported`, including when it returns
zero rows, because returned registrations cannot establish the request's full
permission set.
For `include=role_summary`, any non-full resource summary makes the overall
address-name response `partial`, lists `role_summary` in
`meta.unsupported_fields`, and uses the same product reason mapping. Projected
permission rows remain visible, but an empty or populated expansion is not
authoritative when that metadata is present. A page containing both a wrapper
summary and a non-wrapper approval/delegation limitation uses the latter partial
reason; missing or unrecognized summary metadata still takes precedence. A
synthetic or future resource summary that independently proves full coverage
adds no completeness metadata on a resource-bound request.

These classifications are request-relative. `/v2/permissions` continues to
serve known permission rows that apply to each resource, but those rows and the
derived role summaries are not authoritative enumerations while the coverage
described above remains partial. Zero returned rows therefore
do not prove that no account can mutate the selected name or registration.
NameWrapper holder enumeration remains a separate known unsupported class, and
ENSv2 registry operator approval remains separately narrowed until indexed.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L575-L592 @ ens_v2@a971bd64)

`wrapper_fuses` has one stable shape on name detail, resolver `bound_names`,
and permission rows:

```json
{
  "fuses": 196609,
  "cannot_unwrap": true,
  "cannot_burn_fuses": false,
  "cannot_transfer": false,
  "cannot_set_resolver": false,
  "cannot_set_ttl": false,
  "cannot_create_subdomain": false,
  "cannot_approve": false,
  "parent_cannot_control": true,
  "is_dot_eth": true,
  "can_extend_expiry": false
}
```

The booleans name the ten fuse bits declared by NameWrapper; mask constants and
the zero sentinel are not fuse booleans.
(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L24 @ ens_v1@91c966f)
The word and booleans use the served block timestamp: when wrapper expiry is
earlier, `fuses` and every boolean are cleared. An expired plain wrapped name
keeps `wrapper_state="wrapped"` with the cleared summary; expired emancipated
and locked names expose neither wrapper field because NameWrapper also clears
their owner.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L856 @ ens_v1@91c966f)
Each returned item either has both `wrapper_state` and `wrapper_fuses` or has
neither. Collection completeness remains request-relative: metadata on returned
permission rows does not make zero-row permission enumeration complete, so the
`meta.completeness` and unsupported-reason rules above still apply.

During `.eth` registrar grace, bigname keeps the existing approve-only policy
interpretation for projected wrapper-holder powers: it removes owner
modification and transfer powers except `approve` and `approve_wrapper`, then
still applies `CANNOT_APPROVE`. Upstream's `canModifyName` rejects owner/operator
modification during grace, while per-token `approve` routes through the
ERC-1155-fuse owner/operator authorization path rather than that helper. This
approve exception is bigname policy, not an upstream lifecycle state.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L222 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L37 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L47 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L127 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L135 @ ens_v1@91c966f)

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
    "as_of_completeness": {
      "8453": {
        "completeness": "unsupported",
        "unsupported_reason": "temporarily_unavailable"
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
- `total_count` is nullable. Reverse address results from `POST /v2/lookup`
  populate it by counting the same readable current name/address rows used by
  the page query when the requested relation set maps directly to a stored role
  group. Relation sets that require post-filtering retain `total_count=null`.
  Other routes populate it only where a precomputed count makes it cheap or
  where they explicitly document `include=total_count`; they must not otherwise
  run unconditional full counts on the request path.
- `meta` is always present. Single-resource routes that read chain-derived state
  include `meta.as_of` and `meta.as_of_token` when they can attribute at least
  one served snapshot-pinned chain position. Top-level collection routes omit
  both because their mutable latest-state rows are not bound to one snapshot,
  except that `/v2/search` reports request-scoped `meta.as_of` positions as
  human-readable staleness attribution and still omits `meta.as_of_token`.
  Control-plane routes (`/v2/status`, `/v2/namespaces/{namespace}`) omit both.
  Verified name and record responses keep the same metadata shape as their
  indexed peers. The authoritative position identifies the projection snapshot
  admitted for the lookup. For a cross-chain path, the auxiliary position is
  the canonical execution position retained by that projected row, even when it
  is older than the newest `chain_heads` marker for that chain. The lookup
  engine returns both positions, and `meta.as_of`/`meta.as_of_token` expose
  those actual lookup positions rather than implying execution at the newer
  marker. The engine independently requires its [`project`
  phase](architecture.md#intake-architecture) to be at the current readable
  authoritative head before executing. After the live calls it revalidates the
  exact project generation, projected name topology, selected manifest
  declarations, and canonical positions. A concurrent replacement returns the
  existing `409 stale` response and performs no ledger mutation. `meta.as_of` is
  human-readable staleness attribution on routes
  that provide it. `meta.as_of_token` is opaque and is the value to pass to
  `at` when a route supports snapshot replay. `meta.completeness`,
  `meta.unsupported_fields`, and `meta.unsupported_reason` appear only when the
  read is not clean. `meta.source` appears when the route supports `source`.
- Public reverse requests to `POST /v2/lookup` and all `GET /v2/search`
  requests disclose the chain scope selected by the request. A public reverse
  request or search without an explicit `namespace` accounts for the chains of
  every active public namespace; an explicit search namespace accounts only
  for that namespace's chains. Name-only lookup retains its inferred or
  explicit namespace scope. A chain with a readable position appears under
  `meta.as_of`. A chain suppressed by the deployment-readiness check instead
  appears under `meta.as_of_completeness` as
  `{completeness:"unsupported", unsupported_reason:"temporarily_unavailable"}`.
  The two maps have disjoint keys, and their union is exactly the request's
  chain scope; returned rows never reduce that denominator. The sibling map is
  omitted when no in-scope chain is suppressed. A suppressed chain is not added
  to `meta.as_of_token` solely for disclosure. In a mixed lookup batch, the
  token can still contain that chain when another input actually uses its
  snapshot position; the suppression entry takes precedence over a
  human-readable `meta.as_of` entry because some requested data was withheld.
  That precedence is the general rule for every route that emits these maps:
  whenever one chain is both readable in one part of the request's scope and
  suppressed in another, the suppression entry wins and the chain is removed
  from `meta.as_of`.
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
Rows classified as `registration_status=unregistered`, including ownerless
ENSv2 reservations, have no current registration, so product name detail and
batch lookup always omit `registration_id`. Resolver and record fields are also
omitted, the records route exposes no resolver, record values, or audit-only
inventory, and resolver `bound_names` omits the row unless it is
an ownerless ENSv1 or Basenames registry row whose current registry resolver
pointer is retained (a [serving resource](glossary.md#serving-resource)). That
retained pointer permits resolver and record reads without acquiring registration
identity or control. Indexed records require retained inventory;
routes with source selection let verified and auto records follow the ordinary
lookup capability, and resolver `bound_names` remains subject to the resolver
family's binding-enumeration capability.
See [registration status](#status-vocabulary) for the upstream basis.
The ENSv2 rule is an intentional product
narrowing: ENSv2 stores a nonzero resolver supplied for an ownerless
reservation and returns it until expiry. (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @
ens_v2@a971bd64) (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @
ens_v2@a971bd64)

## Tiers

### Tier 1: Lookup Primitives

Lookup primitives serve the partner latency path and current indexing status:

- `POST /v2/lookup`
- `GET /v2/status`

The lookup route uses the common record shape and in-band per-result statuses.
`GET /v2/status` is the only route with the ops status vocabulary
`ready`, `degraded`, `stale`. It reads the chain set from
`bigname_phase.chain_heads` and `bigname_phase.chain_phase_state`. The stored
head and finality fields come from `chain_heads`; indexed progress is the
`project` phase's most recent completed publication. Readiness also uses that
phase's lifecycle state and redo marker, the Interpret `redo_in_progress`
marker, and newest per-chain heartbeat in
`service_heartbeats`. A phase row that startup settled while its chain was
unconfigured is not eligible for `ready` until genuine phase completion or
completed-state revalidation clears that marker. It reports `degraded` unless
a stronger `stale` condition applies, such as a genuinely failed phase or an
expired heartbeat. Ethereum Sepolia readiness requires its `ingest`
phase to remain `completed` and its
[verification](glossary.md#verification-level) phase to be `completed` with
a known level at or above the `quick_synced` floor: `quick_synced`, `cross_checked`, and `node_checked` qualify, while an unknown stored level fails closed.
A failed Ingest or Verify, or an ordinary completed Verify without that
evidence, maps to `stale`. An idle, running, paused, or missing Ingest or Verify
maps to `degraded`. An expired runner heartbeat remains `stale` while either
required phase is incomplete. Chains without this requirement omit those
Ingest and Verify evidence checks. A
failed Project or expired heartbeat maps to `stale`; an active Interpret redo
or a paused, redoing, or missing-heartbeat Project maps
to `degraded`. A running Project with a completed publication remains eligible
for `ready` when its block and time lag are within the configured thresholds,
its interpreter content hash matches this API build, and a same-height
publication has the stored head's exact block hash. A generation mismatch or
running without a completed publication is `degraded`. The schema-v2 project phase has no
invalidation queue or dead-letter table, so the retained response fields map
to `pending_invalidation_count=0`,
`pending_invalidation_count_capped=false`, and `dead_letter_count=0`.
Cached network-head comparison evidence is unchanged. Provider refresh runs
asynchronously under a timeout and cache TTL, so the route never waits for a
provider. A failed latest refresh degrades readiness immediately while keeping
the last successful head comparison visible as cached evidence.

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

`GET /v2/names/{name}?source=verified` and
`GET /v2/names/{name}/records` with a verified source execute through the
schema-v2 lookup engine on every request. Response fields and per-record status
meaning stay unchanged, but there is no reusable outcome, durable execution
trace, or execution-cache readback. A direct live answer that disagrees
with the indexed exact entry or manifest-authorized derived read used for
comparison writes the guarded
[resolution divergence ledger](glossary.md#resolution-divergence-ledger).
Agreement creates no divergence but may clear a matching active row, wildcard
lookup without an exact comparison row writes nothing, and an answer that used
CCIP-Read never writes or clears the ledger. For cross-chain resolution, the
selected product snapshot must admit
the current authoritative position and include the execution chain, while the
canonical projected row supplies the exact hash-pinned execution position. The
response metadata reports that actual position, which may be older than the
generic auxiliary checkpoint initially selected by the route, but never newer;
a position at the same height must have the same block hash. A newer or
same-height incompatible position makes the verified answer stale before any
provider call or ledger write. The current
lookup engine does not replay historical `at`, `safe`, or `finalized`
authoritative execution: if the selected product snapshot does not admit the
engine's current readable authoritative position, the verified section is
`stale` rather than being executed at a different authoritative position.
Provider connect, DNS, TLS, connection-reset, and other transport failures
abort a verified name or record request with `500 internal_error`; they are not
reported as selector-local stale answers, and `source=auto` does not return a
partial blend after such a failure.
Explicit record `keys` and the inventory-derived default verified selector set
are each limited to 200 keys. An oversized server-derived set returns `422
unsupported` before provider execution; the compact records caller can narrow
the request with `keys`. For the verified flat name-profile, the limit applies
before its synthetic `addr:60` request is added, so a 200-selector inventory
may issue 201 provider keys when the primary-address selector was absent; more
than 200 inventory-derived selectors still returns the same error.

`GET /v2/addresses/{address}/primary-name` keeps its documented `answers` and
typed `verification` shapes. Every indexed answer reads
`bigname_phase.primary_names_current`; `source` selection only narrows the
answer list and does not select a different indexed projection. A successful
stored raw claim is normalized for the indexed product name even when its raw
spelling was not already normalized. The verified producer is a fresh ENS/60
lookup at the current readable Ethereum position. It applies the raw-claim
normalization gate before forward resolution and persists neither a legacy
execution outcome nor a divergence row. When `source` is omitted, the route
returns the indexed and verified answers together only if the current Ethereum
`chain_heads` position and exact completed `project` publication generation
match that lookup before verified execution and remain unchanged after reading
the indexed tuple from
`bigname_phase.primary_names_current`; otherwise the whole
request returns `409 stale` instead of assigning answers from different
positions to one `meta.as_of`. The indexed answer depends only on the projected
tuple: a live reverse claim or live lookup failure changes only the verified
answer. Other verified primary-name tuples are explicit `unsupported`; indexed
answers remain available where their projection supports the requested tuple.
Provider transport failures abort this route with `500 internal_error` rather
than producing a verified answer entry with `status=stale`.
The post-call guard also revalidates the Ethereum project generation and both
selected ENS manifest declarations; a concurrent replacement returns `409
stale` and no verified answer.

The exact-head and post-call generation fences on verified reads fail safe
under schema-v2 projection lag. If head following or project publication
remains behind the readable chain head, verified reads degrade to `409 stale`
instead of executing against mixed generations. Snapshot selection and
verified lookup now read the same `chain_heads` and `chain_phase_state` project
row, so this exact-head fence detects only a concurrent head, rewind, or
project-publication change between their reads. A fast-moving chain can still
advance during a provider or CCIP round trip, so the post-call generation
checks remain necessary.

Indexed lookup names, record inventories, address-name relations, resolver
overviews, and resolver bound names now come from `bigname_phase` projections.
Projection publication is incremental, so an unchanged row retains the target of
the last projection-phase run that rebuilt it. A row target may therefore be earlier
than the selected position; it may not be later, and a target at the same
height must carry the selected hash. The API captures and revalidates the
completed projection-phase generation around each indexed read. Current lookup and
latest resolver reads also bind that generation to the selected
`chain_heads` position; historical resolver reads bind to the current
generation while admitting only rows at or before the requested position.

This single-source projection consistency check rejects
future or same-height wrong-fork rows while serving unchanged rows after an
unrelated head advance. A real phase lag,
interpreter-hash mismatch, concurrent publication change, or row ahead of the
selected position still fails closed.

### Tier 3: Diagnostics

Diagnostics are the only public routes that may carry pipeline vocabulary.
They expose coverage taxonomy, binding and authority explanations, record
inventory and indexed-value internals, active manifests, and raw
normalized-event rows.
The diagnostics records route drives the same verified lookup engine and can
write or clear rows in the
[resolution divergence ledger](glossary.md#resolution-divergence-ledger).

## Parameters

Common parameter rules:

| Parameter | Applies to | Values |
| --- | --- | --- |
| `at` | Tier-2 single-resource snapshot reads: names, records, and resolver overview; diagnostics exact-name snapshot/explain routes. Top-level collection routes recognize `at` only to return the temporary latest-state limitation error. Lookup, status, primary-name, and namespace metadata do not accept it. | RFC 3339 timestamp, or the URL-safe opaque snapshot token from `meta.as_of_token` |
| `finality` | Single-resource snapshot reads and diagnostics exact-name snapshot/explain routes accept `latest` (default), `safe`, and `finalized`. Top-level collection routes accept only omitted or explicit `latest`. Lookup, status, primary-name, and namespace metadata do not accept it. | `latest` (default), `safe`, `finalized` where supported |
| `source` | names, records, primary-name | names and records use `indexed` (default) or `verified`; the records route also accepts `auto`; primary-name omits `source` to return all supported source answers and may use `indexed` or `verified` to request a subset |
| `namespace` | name-inferred, address-anchored, and collection routes | explicit override or filter |
| `include` | route-documented expansions | per-route allowlist |
| `sort`, `order` | paginated routes that declare a sort set | route-documented field set plus `asc`/`desc` |
| `cursor`, `page_size` | every paginated route | opaque cursor; default 50, max 200 |

For a cross-namespace read with no explicit `namespace`, the API accounts for
every recognized public namespace with active
[source manifests](manifests.md). It separately determines which of those
namespaces may serve rows: their active manifests must have a completed
projection publication at the current head of the namespace's authority chain
in the selected deployment. A namespace may not serve rows while its selected
authority chain has Interpret
`redo_in_progress=true`, regardless of redo mode. An Interpret redo rewrites
previously served identity history batch by batch, so a page read during the
redo can be incomplete even while Project still reports its prior completed
head.
Bare search and public reverse lookup filter current rows and counts to exactly
the eligible namespaces, and public reverse lookup builds its snapshot scope
from the same authority chains. After reading a bare search page, the API reloads the active
manifest declarations, selected authority chain heads, and completed projection
publication generations captured during derivation, and confirms that no
selected authority chain began an Interpret redo. Any change returns the
existing retryable `409 conflict` instead of serving a response assembled
across deployment states.
Public reverse lookup reloads its captured active manifest declarations before
the route's existing head and projection-publication check, including the same
Interpret redo check: a manifest change returns `409 conflict`, while a redo,
head, or publication change returns the existing retryable `409 stale`. A redo
that begins after derivation therefore never exposes a partial page through
either route.
Explicit-namespace search captures its request-scope metadata before reading
the page and reloads it afterward. A head, completed publication generation, or
readiness change returns the same retryable `409 conflict` instead of
attributing the page to a position selected after the rows were read.
Publication becoming ready between admission reads is such a readiness change,
not evidence of an Interpret redo.
Their namespace-omitted cursors bind that derived namespace list and fail closed if it
changes. Search with an explicit recognized `namespace` bypasses public
namespace derivation. It still requires the selected namespace's
`redo_in_progress` value to be false and returns `409 stale` while an Interpret
redo is active. Name-only lookup likewise keeps its existing name snapshot
selection and does not derive the public set; only address inputs invoke public
reverse derivation.
The chains accounted for by `meta.as_of` and `meta.as_of_completeness` are
selected by the namespace parameter and input kinds, not by the namespaces
eligible to serve rows or the rows returned. An explicit namespace does not
account for another public namespace's chains. A bare cross-namespace read
returns `409 conflict` when no public namespace may serve rows; when at least
one may serve rows, every other in-scope chain is disclosed through
`meta.as_of_completeness`.

Unknown or undocumented query parameters are rejected with `400 invalid_input`
on every `v2` route. As a documented temporary exception, latest-state
collection routes recognize `at`, `finality=safe`, and `finality=finalized` so
they can return the limitation errors defined below instead of implying
snapshot support.
Snapshot-pinned reads require the ADR 0003 slice-3 snapshot-service enabler;
ADR 0006 rollout step 3 includes that read-layer work.

## Status Vocabulary

`unregistered` describes the absence of current registration or control; it
does not assert that resolver data is absent. A supported row may therefore have
`registration_status=unregistered` when it is
an ownerless ENSv1 or Basenames registry row whose current registry resolver
pointer is retained (a [serving resource](glossary.md#serving-resource)). It
serves indexed records when retained inventory exists; routes with source
selection can also serve verified records under the ordinary lookup capability.
The current ENSv1 registry and the Basenames registry emit the supplied owner
from `setOwner`.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L68 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/Registry.sol:L100-L103 @ basenames@1809bbc)
Both map registry self-ownership to zero in `owner()`.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/Registry.sol:L165-L170 @ basenames@1809bbc)
In both registries, the resolver write and read use the resolver field,
separately from the owner field.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L7-L10 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L94 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137-L140 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/Registry.sol:L16-L22 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/Registry.sol:L132-L134 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/Registry.sol:L178-L180 @ basenames@1809bbc)
The internal reason
`current_authority_not_projected` is reserved for authority selection that is
unresolved or unsupported, not for a registry event stream that positively
proves current authority is absent.

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
- A read over a projected row keys `unsupported` on that row's own coverage
  status, not on a list of known reasons: an unsupported row serves
  `status=unsupported` even when it names no reason or names a reason the build
  does not recognize. Exceptions are per-route and named there, such as the
  name-detail partial serve for `current_authority_not_projected` in
  [`api-v2-routes.md`](api-v2-routes.md).
- When an unsupported projected row names a reason that this build does not
  recognize and that cannot cross the serving boundary as public vocabulary,
  the public `unsupported_reason` is `unsupported_reason_unrecognized`.
- `failure_reason` is permitted on `failed`, `stale`, `not_found`, and
  `mismatch`.
- `mismatch` is the verification state where a claimed answer verifies to a
  different value.
- `completeness` is `full`, `partial`, or `unsupported`.
- Empty arrays and empty maps mean known-empty, not unknown.

### Resolver record answers and values

A keyed resolver record answer contains `status`. It contains `value` only
when `status` is `ok`; `unsupported_reason`, `failure_reason`, and `meta` are
present only where the route contract permits them.

For a successful `contenthash` answer, `value` is a lowercase,
`0x`-prefixed hex string containing the bytes returned by the resolver. The API
does not decode those bytes into a URI or media-type-specific representation.
For a successful `addr:<coin_type>` answer, both `<coin_type>` and the selector
are decimal strings and `value` is a lowercase, `0x`-prefixed hex string. For
multicoin records, that string contains the resolver-returned native binary
address bytes without chain-specific textual re-encoding.

ENSv1 and Basenames store the supplied contenthash and address byte payloads
verbatim and emit the same bytes. Contenthash reads, and address reads when no
default-address fallback applies, return the stored bytes. An empty payload is
therefore the stored value after a clear, and those reads return the same empty
bytes.
(upstream: .refs/ens_v1/contracts/resolvers/profiles/ContentHashResolver.sol:L14-L28 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L85 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/resolver/ContentHashResolver.sol:L32-L43 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L57-L99 @ basenames@1809bbc)
Bigname represents that zero-length exact stored contenthash or address answer
as `{"status":"not_found"}` and omits `value`. A records route may then apply a
documented derived-record rule, such as the ENSIP-19 default-address rule; when
it does, the final keyed answer and convenience field follow that derived
answer and carry its metadata. A clear is distinct from any non-empty all-zero
byte payload; this contract does not define a new meaning for a non-empty
20-byte all-zero `addr:60` value.

The `addresses` convenience map uses the same scalar hex string for each
decimal coin type, and `content_hash` uses the same contenthash scalar string.
Cleared exact values are omitted from both convenience fields unless a
documented derived-record rule supplies a replacement answer. Diagnostics and
Project use the internal status `success` for a retained value; product routes
publish that status as `ok`.

The exact-name detail route, resolver-records route, `profile=detail` lookup, and
GraphQL resolver address fields flatten both projected `{encoding,bytes}`
address values and projected scalar address values to the same scalar hex
string. The supported internal shapes therefore do not create a second public
multicoin-address shape.

## Finality And Snapshots

`finality` values are `latest`, `safe`, and `finalized`. Snapshot selection is
uniform across single-resource snapshot-read routes. Each such chain-derived
response carries `meta.as_of`, keyed by stringified `chain_id`, and
`meta.as_of_token`, an opaque token that can round-trip as `at` to pin exact
per-chain positions. Tokens must cover every required slot in the target
route's snapshot scope and must not carry extra slots outside that scope.
For lookup and search responses that account for chains selected by the
request, the target route's served snapshot scope can be narrower than that
chain scope; every additional in-scope chain is reported under
`meta.as_of_completeness` and is not added to the token.

Chain-position timestamps are RFC 3339 instants. Inputs and stored projection
positions may use `Z` or a numeric UTC offset (`+HH:MM` or `-HH:MM`) and may
carry one to nine fractional-second digits; readers normalize the instant to
UTC before comparison. Because `+` is decoded as a space in query strings,
clients must percent-encode it as `%2B` in an `at=` query value. For example,
`at=2025-06-15T17:37:42%2B02:30` selects the same instant as
`at=2025-06-15T15:07:42Z`. Different accepted spellings of the same instant do
not make a projection stale.

Successful v2 metadata and snapshot tokens serialize timestamps in UTC with
`Z`. They retain non-zero fractional seconds; whole-second timestamps keep the
existing `YYYY-MM-DDTHH:MM:SSZ` spelling.

The API selects current `latest`, `safe`, and `finalized` positions from
`bigname_phase.chain_heads` and obtains their timestamps from readable
`bigname_phase.chain_lineage`. Every selection is available only when the current
`project` phase is completed at the exact latest head with the API's compiled
interpreter content hash. Timestamp `at` selection and opaque-token replay
still choose historical positions: every supplied or resolved position must
exist in `bigname_phase.chain_lineage` and satisfy the requested finality
floor, and an authoritative cross-chain selection bounds auxiliary positions
by its timestamp. The current project-publication check also applies to those
historical selectors.
A token or timestamp that selects a block absent from readable phase lineage
returns `409 conflict`.

API startup discovers the status chain set from the union of
`bigname_phase.chain_heads` and `bigname_phase.chain_phase_state`. `/v2/status` uses
those same relations for its chain set and reads stored head/finality positions
from `chain_heads`, project progress from the `project` row in
`chain_phase_state`, and both timestamps from the matching readable
`chain_lineage` rows.

Top-level collections page over mutable latest-state tables. They therefore
omit `meta.as_of` and `meta.as_of_token`, except that search reports
request-scoped `meta.as_of` for staleness attribution while still omitting
`meta.as_of_token`. Their cursors do not claim a snapshot bound. Newly issued collection cursors carry no snapshot token; a
legacy cursor's snapshot component is ignored rather than treated as a
validity condition. Omitted `finality` and explicit `finality=latest` are accepted.
An `at` selector returns `400 invalid_input` with
`at is not supported because collection routes read latest state`.
`finality=safe` and `finality=finalized` return `400 invalid_input` with
`finality must be latest because collection routes read latest state`.

This is issue #188 option 2. Option 1 is the storage follow-up: bind every page
to an immutable publication revision and return explicit cursor-expired
semantics when that revision is no longer available. Once revision-bound
cursors and row reads land, the collection `at` and historical `finality`
restrictions lift and collection snapshot metadata can be restored.

`POST /v2/lookup` is a current-state read. It does not accept `at` or
`finality`; when a served head is available, its `meta.as_of` and
`meta.as_of_token` record the served positions for staleness attribution and
shadow-diff correlation. Lookup rejects partial scoped heads instead of
emitting a token that cannot replay on a compatible snapshot-read route. Each
returned forward or reverse phase row must have a projection target at or before
the selected position; a target at the same height must have the same hash. The
selected `chain_heads` rows and completed schema-v2 projection generations must
remain unchanged across the read. An ahead, same-height wrong-hash, or
publication-generation mismatch returns `409 stale`.
Name-only and exact-scope lookup, explicit-namespace search, and resolver reads
without `at` at `finality=latest` apply the same `redo_in_progress` readiness
check and return `409 stale`. Historical resolver reads selected with `at` and
resolver reads at `finality=safe|finalized` retain their existing generation
validation without that redo check.

`GET /v2/addresses/{address}/primary-name` is also a current-state read. It
does not accept `at` or `finality`; when a served head is available, its
`meta.as_of` and `meta.as_of_token` record the served positions for staleness
attribution and shadow-diff correlation. For an ENS/60 verified answer, both
metadata fields identify the current readable Ethereum position that pins the
fresh lookup. An omitted-source ENS/60 response fences the indexed claim to
that same schema-v2 position and project publication generation across the
verified and indexed reads and returns `409 stale` if either changes.
There is no persisted trace or verified-outcome cache. Indexed-only Basenames
responses remain Base-scoped; Basenames verified primary-name lookup is
currently unsupported.

The `chain_positions` query parameter from `v1` does not exist in `v2`.

## Cursors And Pagination

Cursors are opaque and versioned. They are not bound to the route path string,
so route evolution does not invalidate outstanding cursors. Top-level
collection cursors bind the collection anchor, namespace, filters, and sort,
but not a snapshot. A bare search cursor uses the request's derived namespace
set as its namespace anchor and fails closed if that set has changed. Cursors
preserve keyset position across requests without claiming that the mutable
dataset is frozen. A legacy collection cursor's snapshot component is ignored.
Snapshot-bound cursor semantics remain on single-resource responses with nested
pagination where documented.

The `/v2/events`, name-history, and address-history collections use a
collection-wide `redo_in_progress` check. An active Interpret redo on any chain
returns retryable `409 stale` for all three collections, regardless of the
requested namespace or name. Events validates the request and cursor binding
before its first check, while address history also validates its namespace
first. Name history validates the request and cursor binding, then captures the
check before parent lookup. A missing parent returns `404
not_found` only when no redo is active and the captured generations are
unchanged; otherwise it returns `409 stale`. Each route checks before deriving
identity and event anchors written by Interpret and checks again inside the
repeatable-read page transaction. Events and address history then resolve
display names and revalidate the captured redo state before returning data;
name history has no post-transaction display-name read. An active redo at any
check, or a redo
that began between them, returns `409 stale` instead of exposing a partially
reconstructed normalized-event range. A
well-formed cursor whose event anchor is gone therefore returns `stale` when the
redo check takes precedence and `400 invalid_input` otherwise. Product
event-type filtering precedes keyset pagination, so page rows and continuation
metadata describe only product-visible events. For requests without an explicit
`type`, cursor anchor validation omits the implicit product event-type filter,
so a still-existing non-product anchor continues to the next product-visible
row. An explicit `type` remains part of anchor validation. Cursor bytes remain
unstable.

A full Interpret and Project re-walk that must not change product behavior at a
fixed readable chain head does not invalidate an outstanding collection cursor
merely because an internal normalized-event row ID changes. On `/v2/events`,
name history, address history, and every other product cursor surface backed by
normalized-event row identity, a cursor issued before the re-walk must resume
after publication from the same underlying normalized-event keyset anchor, with
identical remaining product rows, pages, fields, `has_more`, and summary
behavior, including when the anchor is a non-product event. The diagnostic-events
route must also accept its pre-re-walk cursor and continue from the same stable
normalized-event anchor, but its remaining diagnostic rows and fields may
reflect newly admitted candidate data. A pre-existing diagnostic row's numeric
`normalized_event_id` may change, while its `event_identity` and pre-existing
semantic fields remain stable apart from those allowed candidate additions.
Implementations may preserve numeric
normalized-event IDs or resolve an old token through stable `event_identity`
plus its stored sort tuple; these are alternative storage strategies. Freshly
issued cursor bytes may differ. The boundary acceptance gate exercises both the
product and diagnostic continuation contracts, then separately verifies fresh
post-re-walk cursors.

That identical-product continuation rule applies only when the declared
[re-derivation boundary](glossary.md#re-derivation-boundary) preserves product
semantics. The intentional
[#348](https://github.com/ensdomains/bigname/issues/348) and
[#529](https://github.com/ensdomains/bigname/issues/529) interpreter changes keep
an ENSv2 resolver `RecordChanged.event_identity` or
`RecordVersionChanged.event_identity`, changes `logical_name_id` from null to
the retained canonical [name surface](glossary.md#surface-name-surface), keeps
`resource_id` null, and updates the attribution embedded in
`raw_fact_ref.interpreter_state_key`. Its `before_state` may also become the
preceding `after_state` from the logical-name/resource-null state stream that
the event now joins. Issue #348 retains the surface from registry/root
evidence; issue #529 retains a surface observed only by resolver
`AliasChanged` before a batch boundary. The event may therefore newly enter name-filtered
diagnostics and product history. A cursor issued before that change has no
continuation guarantee and may be rejected. Consumers must discard
pre-#348/#529 cursors and restart from the first page; fresh post-publication cursors
continue normally.

The [#613](https://github.com/ensdomains/bigname/issues/613) interpreter change
keeps the original [pre-surface](glossary.md#pre-surface) ENSv1 registry `ResolverChanged` row unchanged,
then adds a name- and resource-linked, [state-derived](glossary.md#state-derived-normalized-event) `ResolverChanged` when the
first active [name surface](glossary.md#surface-name-surface) is learned. Product
events or name history may therefore gain one historical resolver row, while
diagnostics may gain each linked resource copy. When current-registry ownership
ends old-registry fallback, its resource-specific resolver-clear copies represent
one ownership-log/node transition. Product history selects the lexically first
stable event identity among the activated copies matching the request and its
canonicality filters. Selection happens before pagination and is shared by
counts, summaries, and cursor validation. A resource-only request therefore
retains its matching clear even when another resource has the globally first
copy; a sole matching clear is never suppressed. All normalized copies remain
available to diagnostics, projection, and replay.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L24 @ ens_v1@91c966f)
A cursor issued before this
change has no continuation guarantee and may be rejected. Consumers must
discard pre-#613 cursors and restart from the first page; fresh post-publication
cursors continue normally.

These boundaries do not claim fresh/resumed parity for the
known pre-existing exception: when a resolver-emitted resource equals
`namehash(N)`, named-resource and alias preimages can share one retained
[interpreter state key](glossary.md#interpreter-state-key), so resumed
interpretation can lose the named-resource resolver hint and diverge from a
fresh walk ([#560](https://github.com/ensdomains/bigname/issues/560); evidence
is checked in as an ignored collision probe). If an ended resource still
retains a resolver pointer to the emitter, its rebuildable record-inventory
projection may change too. The
resource-less late event does not restore `name_current.resource_id`, so name
and record reads for the released or expired name continue to expose no current
record inventory.

Every collection uses `cursor`, `next_cursor`, `page_size`, nullable
`total_count`, and `has_more`. Default `page_size` is 50; maximum is 200.
For reverse address inputs to `POST /v2/lookup` whose relation set maps directly
to a stored role group, `total_count` is the exact distinct-name count from the
same current joins and readability filters as the page, and `has_more` compares
against that live count on the one-row first-page path. Relation sets that need
post-filtering retain `total_count=null`; feed and detail profiles use the same
count and pagination semantics.
For a name with multiple relation rows, a readable row admits the name and the
returned `is_primary` is computed from the current name and primary-name claim
even when a different primary-matching relation row is unreadable. The retired
v1 page/sidecar pair disagreed on this case; the v2 live page and count joins
share the same eligibility rule.
Reverse address results from `POST /v2/lookup` additionally require the
projection rows behind a name to be [readable](glossary.md#readable--read-safe)
*and* supported. Both the name row and the address-relation row that admits it
must carry a supported support status; a name whose current name row or whose
matching relation row is unsupported is absent from both the page and
`total_count` rather than listed with an unsupported reason. Reverse lookup
results therefore answer which supported names an address holds. This
deliberately narrows earlier behavior, which listed unsupported names and left
the caller to read the reason; per-name unsupported detail now lives on the
name-shaped routes and diagnostics, which read the row directly.
`GET /v2/addresses/{address}/names` is the exception: it lists an unsupported
row when the matching current address relation is provable but other coverage
for that name is unsupported. Once the [per-name ownership
rule](consumer-capabilities.md#ensv1ensv2-mixed-history-ownership) is activated,
a name with no provable current authority has no provable current address
relation and is therefore structurally absent from this collection.
Listed unsupported rows do not carry a per-row reason; read the reason from the
name-shaped routes or diagnostics for the name in question.

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
| `request_timeout` | 408 | the whole request exceeded the configured deadline |
| `rate_limited` | 429 | the enabled client limit, keyed by an IPv4 address or IPv6 `/64`, rejected a route that can trigger verified execution |
| `overloaded` | 503 | the process-wide, health-specific, or verified-execution in-flight ceiling was exhausted |
| `internal_error` | 500 | unexpected failure |

Rules:

- `unsupported` is `422`.
- Verified record-resolution failures surface as `status: "failed"` on the
  affected section with `failure_reason`, or as `stale` when the RPC provider
  cannot serve the selected block. Provider response timeouts for that path use
  the existing in-band execution-failure behavior; they are not whole-request
  `408` responses. Provider connect-phase timeouts and other transport failures
  during verified record resolution return whole-request `500 internal_error`;
  no execution outcome is cached for any v2 lookup. ENS/60 primary-name
  verification uses the same transport
  split with its CCIP-Read gateway leg: configured provider or gateway response
  timeouts remain in-band failures for that response, while provider or gateway
  connect-phase timeouts, DNS failures, TLS failures, connection resets, and
  other transport failures return whole-request `500 internal_error`. Neither
  result is persisted by the v2 serving path.
- Every route has a whole-request deadline. `/healthz` and `/v2/status` retain
  that deadline as their final backstop. `/healthz` bypasses
  the process-wide concurrency limiter and load shedding, uses a reserved
  one-connection database pool with a two-second check limit, and has a small
  independent health ceiling. HTTP-concurrency saturation and request-pool
  exhaustion therefore do not queue the probe; a failed or timed-out readiness
  connection reports the database as unreachable. The status routes retain
  global admission because their aggregate database query is not a liveness
  probe. A successful `/healthz` database check also returns a one-way identity
  token scoped to the currently running PostgreSQL postmaster, database OID,
  and server listener used by the connection, without exposing their raw
  values. The token changes when PostgreSQL restarts or the connection reaches
  a different listener. Alternate paths to the same postmaster, such as a Unix
  socket and TCP or different listen addresses, can therefore produce different
  tokens. It is populated only when bounded probes of the serving and
  reserved-readiness pools identify the same token.
- The verified-execution rate limit, when enabled, and all in-flight ceilings
  reject work before it waits for execution capacity. The rate-limit key is an
  IPv4 address or IPv6 `/64`; `/healthz` passes only through the health-specific
  ceiling. `GET /v2/names/{name}/records?source=auto` with omitted or empty
  `keys` and `GET /v2/addresses/{address}/primary-name?source=indexed` are
  indexed reads and do not enter verified-execution admission.
- Single-resource GETs return `404 not_found` when no answer exists.
- Collections return `200` with empty `data`.
- Batch lookup results carry in-band `status` per input; a batch never returns
  `404` for one missing input.
- The primary-name route is the documented exception to single-resource `404`:
  a valid `{address, coin_type, namespace}` tuple with no claim or an
  unsupported/mismatched verification returns `200` with in-band `status`.
- Error messages must not name internal storage or pipeline components.
