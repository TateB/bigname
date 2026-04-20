# Conformance Harness

Bootstrap supported-read contract harness for already-shipped routes and collections:

- `GET /v1/namespaces/{namespace}`
- `GET /v1/manifests/{namespace}`
- `GET /v1/names/{namespace}/{name}/children`
- `GET /v1/addresses/{address}/names`
- `GET /v1/names/{namespace}/{name}`
- `GET /v1/resolutions/{namespace}/{name}`
- `GET /v1/coverage/{namespace}/{name}`
- `GET /v1/primary-names/{address}`
- `GET /v1/resources/{resource_id}/permissions`
- `GET /v1/resolvers/{chain_id}/{resolver_address}`
- `GET /v1/history/addresses/{address}`
- `GET /v1/history/names/{namespace}/{name}`
- `GET /v1/history/resources/{resource_id}`

The harness is a standalone Rust package rooted in this directory so it can run without changing the workspace root.

Smoke test:

```sh
cargo test shipped_api::conformance::smoke_supported_reads_contract_bootstrap -- --exact
```

Run the full route set:

```sh
cargo test
```

Execution notes:

- uses `BIGNAME_DATABASE_URL` when set
- otherwise falls back to the bootstrap default `postgres://bigname:bigname@127.0.0.1:5432/bigname`
- each test creates, migrates, and drops its own temporary database
- the child collection contract seeds `children_current` rows and covers both the base
  `GET /v1/names/{namespace}/{name}/children` response and the shipped `include=counts`
  variant; the harness also asserts that unsupported non-`declared` `surface_classes` are
  rejected. Standalone ENSv2 coverage reads back a declared direct child from the same collection
  envelope with `surface_class=declared`, `enumeration_basis=declared_direct_children`, ENSv2
  registry provenance, and no broader linked-child surface-class support
- the address-name collection contract seeds `address_names_current` rows plus the backing
  surfaces, resources, token lineage, and bindings; the harness covers the base
  `GET /v1/addresses/{address}/names` response plus shipped `namespace`, `relation`,
  `dedupe_by`, and additive `include=role_summary` handling. Standalone ENSv2 address-name
  readback coverage rebuilds `address_names_current` from normalized-event inputs and asserts
  that `include=role_summary` remains projection-backed and additive: the base collection row is
  unchanged, while `status`, `expiry`, `record_count`, `subname_count`, and `role_summary` read
  from the existing supporting projections. This does not claim public exact-name support,
  manifest capability graduation, linked child support, verified execution, or universal
  resolver support
- the exact-name contract seeds raw blocks, identity rows, and normalized events, rebuilds
  `name_current` through the worker, and asserts only the frozen `control`, `resolver`, and
  `history` declared-state sections on `GET /v1/names/{namespace}/{name}`, with
  `declared_state.history.{surface_head,resource_head}` acting as the exact-name links into the
  shipped history routes that satisfy the Phase 6 history-explain deliverable
- the resolution contract reuses the exact-name rebuild seed and asserts the shipped mixed-route
  envelope on `GET /v1/resolutions/{namespace}/{name}` across `declared`, `verified`, and `both`
  modes, including required and invalid `records` handling, the supported declared `topology`,
  `record_inventory`, and `record_cache` sections, the shared record-version-boundary invariant
  across those declared sections, selector identity and cache-subset invariants between
  `record_inventory` and `record_cache`, explicit unsupported verified entries when no shipped
  verified answer applies, mixed-route readback of a persisted ENS exact-surface direct-path
  `contenthash` answer, shipped persisted verified `avatar` readback only for ENS exact-surface
  direct-path and `resolver_alias_path` alias-only cases on the mixed route, and persisted ENS
  wildcard-derived `addr:60` readback on `mode=both` with projected wildcard topology while
  declared `record_inventory` / `record_cache` remain unsupported for that lane, plus the
  Basenames deferred-path lock: shipped direct transport-assisted Basenames readback remains
  supported, while alias-participating, wildcard-derived, linked-subregistry, transport-free,
  and reserved offchain-gateway Basenames path classes stay selector-local `unsupported` with
  `provenance.execution_trace_id=null` on the mixed route; beyond those shipped lanes, other
  transport-assisted, other non-alias ancestor-selected, and broader non-wildcard non-direct
  verified support remain out of scope. Standalone ENSv2 declared record-inventory coverage
  reads normalized resolver events into `record_inventory` and `record_cache` only: supported
  selector inventory is limited to retained selector identity and the shared
  `record_version_boundary`, requested `addr:60` and `text` cache entries remain
  `unsupported` because values are not retained in normalized events, missing `contenthash`
  returns `not_found`, unsupported `pubkey` stays `unsupported`, and this does not imply
  verified resolution or universal resolver execution support
- the coverage contract reuses the same exact-name rebuild seed and asserts that
  `GET /v1/coverage/{namespace}/{name}` keeps the same single-name `data` and top-level
  `coverage` object as exact-name lookup while exposing the explain-only coverage block in
  `declared_state`
- the primary-name bootstrap contract covers `GET /v1/primary-names/{address}` against the same
  migrated per-test databases as the rest of the harness, while keeping the shipped route
  bootstrap-only: the row-present cases seed a canonical ENS `ReverseChanged` normalized event,
  run the worker's targeted `primary-names-current rebuild` for the
  `(address, namespace, coin_type)` tuple, and then assert that all three
  `mode=declared`, `mode=verified`, and `mode=both` responses keep the same
  `{address, namespace, coin_type}` `data`; `declared` returns only
  `declared_state.claimed_primary_name`, `verified` returns only
  `verified_state.verified_primary_name`, and `both` combines those sections. The tuple-miss case
  leaves the migrated `primary_names_current` table without that tuple and asserts per-mode
  `status=not_found`; the tuple-present rebuild path reads back only the status-shaped declared
  `claimed_primary_name` from that rebuilt tuple while keeping the verified
  `verified_primary_name` section bootstrap `unsupported` until a persisted exact-tuple verified
  answer exists. A separate exact-tuple declared-claim readback case inserts
  `claim_provenance` plus normalized claim names directly and asserts exact-tuple
  `claimed_primary_name.name` readback for the requested success tuple on `mode=declared` and on
  the declared section of `mode=both`, while
  `claimed_primary_name.provenance` returns only the shipped declared fields for that tuple
  (`source_family`, `contract_role`, `contract_instance_id`, and `emitting_address`), omitting
  execution- and verified-lookup metadata and without implying any fallback claim-source payload
  or broader primary-name coverage. The exact-tuple invalid-name case asserts
  `claimed_primary_name.status=invalid_name` with the tuple-scoped `raw_claim_name` and
  provenance read back from that tuple only, and explicitly guards against implying
  `claimed_primary_name.name`. The harness also seeds persisted verified execution outcomes and
  asserts exact-tuple readback for `verified_state.verified_primary_name` on `mode=verified` /
  `mode=both` only when that exact `(address, namespace, coin_type)` tuple has a cached answer;
  the route still keeps public coverage bootstrap `unsupported`, and that persisted-verified
  fixture's `mode=both` response still pairs the verified readback with the tuple-backed
  status-shaped declared `claimed_primary_name` section instead of implying a broader claimed or
  verified contract. Exact-tuple invalidation coverage then evicts only the targeted persisted
  verified answer across manifest, topology-boundary, and
  record-boundary cases, confirms sibling tuple outcomes remain readable, and confirms the evicted
  tuple falls back to the same tuple-backed declared status plus bootstrap verified unsupported
  section. It also requires both
  `namespace` and `coin_type`, asserts `400 invalid_input` for missing `namespace` / `coin_type`,
  malformed addresses, and malformed non-decimal `coin_type` values, `404 not_found` for
  unsupported namespaces, and the shared bootstrap provenance invariant
  (`normalized_event_ids`, `raw_fact_refs`, and `manifest_versions` empty,
  `execution_trace_id=null`, `derivation_kind=primary_name_route_bootstrap`) plus the same
  unsupported coverage invariant (`status=unsupported`,
  `exhaustiveness=not_applicable`, `source_classes_considered=[]`,
  `enumeration_basis=primary_name_lookup`,
  `unsupported_reason="primary-name coverage is not yet supported"`), empty `chain_positions`,
  `consistency=head`, and a UTC `last_updated` timestamp; persisted verified readback keeps that
  same route-level coverage and `derivation_kind`, while swapping in the persisted
  `manifest_versions`, `execution_trace_id`, and execution `finished_at` as `last_updated`
- the resource-permissions contract seeds `permissions_current` rows and covers both the base
  `GET /v1/resources/{resource_id}/permissions` collection response and the shipped `subject` and
  `scope` query filters. Standalone ENSv2 readback coverage includes resource-scope and
  resolver-scope permission rows from `ens_v2_resolver_l1`, omits fully revoked rows from the
  current collection, and leaves `verified_state` absent
- the resolver-overview contract seeds `resolver_current` rows and asserts the shipped declared
  summary sections, including the supported `{status, count, items}` alias envelope narrowed to
  current `binding_kind=resolver_alias_path` bindings, plus projection provenance, coverage, and
  lowercase address normalization for `GET /v1/resolvers/{chain_id}/{resolver_address}`.
  Standalone ENSv2 resolver-overview coverage reads resolver binding, permission, role-holder,
  and event-summary counts from the declared summary while keeping the permission item shape to
  `resource_id`, `subject`, `effective_powers`, `grant_source`, and `revocation_source`; it does
  not expand the public permission ledger or graduate manifest capabilities
- the address-history contract seeds `address_names_current` anchors plus the backing surfaces,
  resources, token lineage, bindings, and canonical normalized events; the harness covers the
  base `GET /v1/history/addresses/{address}` response with the shipped empty `declared_state`,
  normalized-event provenance, and default `both` scope behavior, plus the shipped
  `namespace=ens&relation=registrant` filter combination and
  `relation=effective_controller` with `scope=surface`, `scope=resource`, and `scope=both`
- the ENSv2 history readback contract seeds canonical ENSv2 normalized-event rows plus the
  existing surface, resource, token-lineage, binding, and address-name anchors, then asserts
  readback through the shared history envelopes for
  `GET /v1/history/names/{namespace}/{name}`,
  `GET /v1/history/resources/{resource_id}`, and
  `GET /v1/history/addresses/{address}`. Name and resource history stay scoped to canonical
  normalized events for the requested `surface`, `resource`, or `both` scope; address history
  uses the same normalized-event history route with the shipped `namespace=ens`,
  `relation=registrant`, `relation=effective_controller`, and replay-stable paging behavior. The
  route contract and row shape remain unchanged with empty `declared_state`; this does not claim
  public exact-name support, manifest capability graduation, verified execution, universal
  resolver support, an ENSv2-specific history ledger, or execution-trace history. The ENSv2
  `sepolia-dev` exact-name profile remains shadow-only with
  `unsupported_reason="ensv2 sepolia-dev exact-name profile is shadow-only"`
- the Basenames history readback contract seeds canonical Basenames rows and asserts they read
  back through the existing shared history envelopes for
  `GET /v1/history/names/{namespace}/{name}`,
  `GET /v1/history/resources/{resource_id}`, and
  `GET /v1/history/addresses/{address}`; address-history matching includes historical
  Basenames matches even when the matched resource has `token_lineage_id = NULL`, while the route
  contract and row shape remain unchanged with no Basenames-only history ledger or
  execution-trace history
- collection-route conformance asserts no-param behavior plus replay-stable `cursor` /
  `page_size` paging for the six shipped collection routes:
  `GET /v1/addresses/{address}/names`,
  `GET /v1/names/{namespace}/{name}/children`,
  `GET /v1/resources/{resource_id}/permissions`,
  `GET /v1/history/addresses/{address}`,
  `GET /v1/history/names/{namespace}/{name}`, and
  `GET /v1/history/resources/{resource_id}` while preserving the frozen default `page.sort`
  invariants
- exact-name explain-route conformance reuses the same exact-name rebuild seed and covers
  `GET /v1/explain/names/{namespace}/{name}/surface-binding` and
  `GET /v1/explain/names/{namespace}/{name}/authority-control`, asserting declared-only
  responses that preserve the exact-name `data`, provenance, coverage, and frozen summary
  sections; the `surface-binding` explain contract also verifies that `declared_state.history`
  remains the shipped exact-name head-pointer link into the dedicated history routes instead of a
  separate history explain endpoint
- resolution execution explain conformance reuses that exact-name rebuild seed, inserts persisted
  execution trace/outcome rows directly, and covers
  `GET /v1/explain/resolutions/{namespace}/{name}/execution`, asserting shared top-level envelope
  invariants with `GET /v1/resolutions/{namespace}/{name}`, request-order preservation for
  `verified_queries`, presence of the persisted execution summary, explain-route readback of the
  same persisted ENS exact-surface direct-path `contenthash` answer for the requested selector
  set, shipped persisted verified `avatar` explain-route readback only for ENS exact-surface
  direct-path and `resolver_alias_path` alias-only cases, persisted ENS wildcard-derived
  `addr:60` explain-route readback with the same envelope plus a persisted execution summary, and
  shipped direct transport-assisted Basenames explain-route readback; beyond those shipped lanes,
  other transport-assisted, other non-alias ancestor-selected, and broader non-wildcard
  non-direct lanes remain out of scope. It also asserts `404 not_found` when the current exact
  surface has no persisted answer for the requested selector set, that deferred transport-assisted
  and other non-alias ancestor-selected requests stay outside the shipped public explain surface
  even when persisted outcomes exist, and that the Basenames alias-participating,
  wildcard-derived, linked-subregistry, transport-free, and reserved offchain-gateway deferred
  path classes stay `404 not_found` on the execution explain route, then reuses the shipped
  execution-outcome invalidation APIs to assert that exact manifest,
  topology-boundary, and record-boundary
  invalidation evicts the persisted ENS verified-resolution answer from the mixed
  `GET /v1/resolutions/{namespace}/{name}?mode=both` route and the execution explain route
