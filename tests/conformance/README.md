# Conformance Harness

Bootstrap supported-read contract harness for already-shipped routes and collections:

- `GET /v1/namespaces/{namespace}`
- `GET /v1/manifests/{namespace}`
- `GET /v1/names`
- `GET /v1/names/{namespace}/{name}/children`
- `GET /v1/names/{namespace}/{name}/records`
- `GET /v1/names/{namespace}/{name}/roles`
- `GET /v1/addresses/{address}/names`
- `GET /v1/names/{namespace}/{name}`
- `GET /v1/profiles/names/{name}`
- `GET /v1/events`
- `GET /v1/roles`
- `GET /v1/resources/lookup`
- `GET /v1/explain/names/{namespace}/{name}/surface-binding`
- `GET /v1/explain/names/{namespace}/{name}/authority-control`
- `GET /v1/explain/resolutions/{namespace}/{name}/execution`
- `GET /v1/coverage/{namespace}/{name}`
- `GET /v1/primary-names/{address}`
- `GET /v1/resources/{resource_id}/permissions`
- `GET /v1/resolvers/{chain_id}/{resolver_address}/overview`
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

Focused OpenAPI publication coverage guard, from the repository root:

```sh
cargo test --manifest-path tests/conformance/Cargo.toml openapi --locked
```

This no-Postgres guard reads `docs/api-v1.openapi.json` and fails if a published
public path lacks either a conformance harness owner or an explicit private/out-of-scope reason;
private `/healthz` remains out of scope.

Focused backfilled-data consumer conformance job, from the repository root:

```sh
cargo test --manifest-path tests/conformance/Cargo.toml backfill
```

Focused source-family backfill conformance lock, from the repository root:

```sh
cargo test --manifest-path tests/conformance/Cargo.toml backfill_sources_source_family
```

Focused automatic-bootstrap backfill conformance lock, from the repository root:

```sh
cargo test --manifest-path tests/conformance/Cargo.toml backfill_sources_auto_bootstrap -- --nocapture
```

Focused reorg chaos drill conformance job, from the repository root:

```sh
cargo test --manifest-path tests/conformance/Cargo.toml reorg_chaos_drill_conformance_job
```

Focused dynamic resolver profile conformance coverage, from the repository root:

```sh
cargo test --manifest-path tests/conformance/Cargo.toml dynamic_resolver_profile -- --nocapture
```

Focused primary-name route contract coverage, from the repository root:

```sh
cargo test --manifest-path tests/conformance/Cargo.toml primary_names_contract -- --nocapture
```

Focused Basenames verified-resolution promotion and deferred-path coverage, from the repository
root:

```sh
cargo test --manifest-path tests/conformance/Cargo.toml basenames_transport -- --nocapture
cargo test --manifest-path tests/conformance/Cargo.toml basenames_deferred_path_classes -- --nocapture
```

Focused capability golden fixture pack guards, from the repository root:

```sh
cargo test --manifest-path tests/conformance/Cargo.toml capability --locked
cargo test --manifest-path tests/conformance/Cargo.toml capability_golden_response_fixtures --locked
```

Execution notes:

- uses `BIGNAME_DATABASE_URL` when set, then `DATABASE_URL` when set
- otherwise falls back to the bootstrap default `postgres://bigname:bigname@127.0.0.1:5432/bigname`
- each test creates, migrates, and drops its own temporary database
- replay, backfill, and chaos-drill conformance jobs expect that configured Postgres server to be
  local and reachable with privileges to create and drop per-test databases; when no local
  Postgres is available, standalone backfill jobs and the focused chaos drill may be treated as
  no-run fallback instead of route contract failures, and should be rerun in an environment with
  Postgres before relying on them
- the capability golden fixture guards are static no-Postgres checks; route readback, replay,
  backfill, chaos-drill, and Basenames verified-resolution commands use the configured local
  PostgreSQL expectations above whenever they create temporary databases
- the golden fixture pack under `tests/conformance/fixtures/capabilities` is deterministic local
  bigname cutover evidence only. The harness asserts that the checked-in fixture set is exactly
  the local cutover evidence set, each fixture keeps `scope=local_cutover_evidence`, fixture IDs
  match `fixtures/capabilities/{fixture_id}.json`, requests are `GET` read routes, responses
  include `data` and `coverage`, and route, conformance, rollout, and rollback owners match the
  native conformance table. This is a local-only/no app-parity guard: the fixtures are not
  imported app call-site replacement, external app parity, first-party app replacement, legacy
  schema parity, or consumer-replacement evidence. The static pack now includes focused local
  route evidence for the implemented compact names, count, records, events, roles, resource
  lookup, and compact resolver overview routes; full first-party cutover still needs app call-site
  mapping
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
- the profile and records contracts reuse the exact-name rebuild seed and assert the shipped
  profile envelope on `GET /v1/profiles/names/{name}` across `declared`, `verified`, and `both`
  modes plus selector-specific behavior on `GET /v1/names/{namespace}/{name}/records`, including
  rejection of `records` on the profile route, the supported declared `topology`,
  `record_inventory`, and `record_cache` sections, the shared record-version-boundary invariant
  across those declared sections, selector identity and cache-subset invariants between
  `record_inventory` and `record_cache`, explicit unsupported verified entries when no shipped
  verified answer applies, mixed-route readback of a persisted ENS exact-surface direct-path
  `contenthash` answer, shipped persisted verified `avatar` readback only for ENS exact-surface
  direct-path and `resolver_alias_path` alias-only cases on the mixed route, and persisted ENS
  wildcard-derived `addr:60` readback on `mode=both` with projected wildcard topology while
  declared `record_inventory` / `record_cache` remain unsupported for that lane, plus the
  Basenames promoted exact-surface transport-assisted direct-path lock: active
  `basenames_execution` v2 supports only the direct class whose persisted topology keeps the
  resolver path on the route surface, has no wildcard source, alias hops, or linked subregistry,
  and uses the Base-to-Ethereum L1 resolver transport
  (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc)
  (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc).
  The alias-participating, wildcard-derived, linked-subregistry, transport-free, and
  offchain-gateway Basenames path classes stay explicitly deferred as selector-local
  `unsupported` with `provenance.execution_trace_id=null` on the mixed route; beyond those shipped
  lanes, other transport-assisted, other non-alias ancestor-selected, and broader non-wildcard
  non-direct verified support remain out of scope. The namespace-inferred app full-profile route
  `GET /v1/profiles/names/{name}` is locked to the canonical `{namespace, name}` selection for
  `base.eth` as ENS and `alice.base.eth` as Basenames, including server-owned profile record
  selection and Basenames verified unsupported selector-local behavior even when an ENS row for
  the same name has a persisted verified answer. Standalone ENSv2 declared record-inventory coverage
  reads normalized resolver events into `record_inventory` and `record_cache` only: supported
  selector inventory is limited to retained selector identity and the shared
  `record_version_boundary`, requested `addr:60` and `text` cache entries remain
  `unsupported` because values are not retained in normalized events, missing `contenthash`
  returns `not_found`, unsupported `pubkey` stays `unsupported`, and this does not imply
  verified resolution or universal resolver execution support
- the dynamic resolver profile lock is focused with
  `cargo test --manifest-path tests/conformance/Cargo.toml dynamic_resolver_profile -- --nocapture`.
  It uses the same local configured Postgres and per-test temporary database expectations as the
  rest of the route harness, with no live RPC or chain intake. The profile covers the local
  positive readback lane whose fixture is labeled Basenames `L2Resolver`-compatible for supported
  resolver-profile answers, with the fixture label anchored to upstream `L2Resolver`
  (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/L2Resolver.sol:L23 @ basenames@1809bbc). Pending or
  pending dynamic resolver targets remain explicit as `resolver_family_pending`, unsupported dynamic resolver targets remain explicit as `resolver_family_unsupported`; the lock does
  not widen resolver support, route coverage semantics, Basenames path classes, manifest
  capabilities, verified execution support, or consumer replacement
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
  section. It also defaults missing `namespace` / `coin_type` to `ens` / `60`, asserts
  `400 invalid_input` for malformed addresses and malformed non-decimal `coin_type` values, `404 not_found` for
  unsupported namespaces, and the shared bootstrap provenance invariant
  (`normalized_event_ids`, `raw_fact_refs`, and `manifest_versions` empty,
  `execution_trace_id=null`, `derivation_kind=primary_name_route_bootstrap`) plus the same
  unsupported coverage invariant (`status=unsupported`,
  `exhaustiveness=not_applicable`, `source_classes_considered=[]`,
  `enumeration_basis=primary_name_lookup`,
  `unsupported_reason="primary-name coverage is not yet supported"`), empty `chain_positions`,
  `consistency=head`, and a UTC `last_updated` timestamp; persisted verified readback keeps that
  same route-level coverage and `derivation_kind`, while swapping in the persisted
  `manifest_versions`, `execution_trace_id`, and execution `finished_at` as `last_updated`.
  The focused `primary_names_contract` filter is the practical lock for this route: it covers
  local exact-tuple primary-name route readback for ENS and Basenames persisted fixtures. This is
  local route coverage only, not external app parity or first-party app replacement
- the resource-permissions contract seeds `permissions_current` rows and covers both the base
  `GET /v1/resources/{resource_id}/permissions` collection response and the shipped `subject` and
  `scope` query filters. Standalone ENSv2 readback coverage includes resource-scope and
  resolver-scope permission rows from `ens_v2_resolver_l1`, omits fully revoked rows from the
  current collection, and leaves `verified_state` absent
- the resolver-overview contract seeds `resolver_current` rows and asserts the shipped compact
  overview sections, including current `binding_kind=resolver_alias_path` alias rows, role-holder
  summaries, projection provenance, coverage, and lowercase address normalization for
  `GET /v1/resolvers/{chain_id}/{resolver_address}/overview`.
  Standalone ENSv2 resolver-overview coverage reads resolver binding, permission, role-holder,
  and event-summary counts from the declared summary while keeping compact role items to
  `subject`, `resource_count`, `permission_row_count`, `effective_powers`, and `resource_ids`; it does
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
  history support, verified execution, universal resolver support, an ENSv2-specific history
  ledger, or execution-trace history. ENSv2 `sepolia-dev` exact-name profile support is covered
  only by the separate exact-name contract and remains scoped to admitted exact-name reads
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
  invariants with the app full-profile route where applicable, request-order preservation for
  `verified_queries`, presence of the persisted execution summary, explain-route readback of the
  same persisted ENS exact-surface direct-path `contenthash` answer for the requested selector
  set, shipped persisted verified `avatar` explain-route readback only for ENS exact-surface
  direct-path and `resolver_alias_path` alias-only cases, persisted ENS wildcard-derived
  `addr:60` explain-route readback with the same envelope plus a persisted execution summary, and
  shipped direct transport-assisted Basenames explain-route readback for the same
  `basenames_execution` exact-surface direct class described above; beyond those shipped lanes,
  other transport-assisted, other non-alias ancestor-selected, and broader non-wildcard
  non-direct lanes remain out of scope. It also asserts `404 not_found` when the current exact
  surface has no persisted answer for the requested selector set, that deferred transport-assisted
  and other non-alias ancestor-selected requests stay outside the shipped public explain surface
  even when persisted outcomes exist, and that the Basenames alias-participating,
  wildcard-derived, linked-subregistry, transport-free, and offchain-gateway deferred path classes
  stay `404 not_found` on the execution explain route, then reuses the shipped
  execution-outcome invalidation APIs to assert that exact manifest,
  topology-boundary, and record-boundary
  invalidation evicts the persisted ENS verified-resolution answer from the mixed
  `GET /v1/profiles/names/{name}?mode=both` route and the execution explain route
- replay capability conformance seeds a per-test supported-read corpus from existing source-truth
  fixtures, snapshots exact-name, child and address-name collection, name/resource/address
  history, resolution, resource-permissions, resolver-overview, and primary-name route payloads,
  runs `bigname-worker replay all-current-projections` against that same database, and asserts the
  route payloads remain byte-for-byte JSON stable. The focused filter is `replay`; current replay
  coverage includes resolver-profile-gated answers in the supported-read corpus, so the lock
  proves those local answers survive all-current projection replay. Reorg/current-answer replay
  coverage also
  seeds stale losing-branch current projections, marks the losing-branch `normalized_events` and
  `raw_blocks` source rows `orphaned`, inserts canonical winning-branch source rows, proves stale
  current answers exist before replay, runs `bigname-worker replay all-current-projections`, and
  asserts shipped route payloads rebuild to canonical winning-branch truth while losing-branch
  address, resolver, and history data disappear. This locks replay idempotence and canonical-only
  rebuild behavior over the shipped route contracts only; it does not widen route support,
  coverage semantics, verified execution support, manifest capabilities, ENSv2 exact-name support,
  Basenames path classes, or consumer replacement
- `backfilled_data_consumer_conformance_job` is the standalone backfilled-data consumer
  conformance entry point. It seeds a synthetic local persisted backfill job with completed child
  ranges, replays all current projections, and asserts the existing shipped consumer capability
  route families for exact name, children, address names, name/resource/address history,
  resolution, resource permissions, resolver overview, and primary name. The job also keeps the
  replay negative checks for losing-branch address-name, address-history, and resolver answers, so
  backfilled data is validated against canonical current projection behavior without widening the
  shipped route contracts
- the source-family backfill conformance lock is focused with
  `cargo test --manifest-path tests/conformance/Cargo.toml backfill_sources_source_family`
  and, in the worker run that introduced the lock, passed as one selected test with 73 tests
  filtered out. The test uses the same local Postgres and per-test temporary database expectations
  as the rest of the harness, seeds synthetic/local completed source-family jobs, and exercises no
  live RPC or chain intake. It persists one synthetic completed job record for each source family
  currently locked by the conformance slice: ENSv1 `ens_v1_registry_l1`, `ens_v1_registrar_l1`, and
  `ens_v1_reverse_l1`; ENSv2 shadow `ens_v2_root_l1`, `ens_v2_registry_l1`,
  `ens_v2_registrar_l1`, and `ens_v2_resolver_l1`; and Basenames
  `basenames_base_registry`, `basenames_base_registrar`, `basenames_base_resolver`, and
  `basenames_base_primary`. Each job persists `selector_kind=source_family`, one synthetic
  selected target, `scan_mode=hash_pinned_block`, and two completed child ranges alongside the
  separately seeded shipped route inputs; replay then rebuilds existing current projections before
  the harness asserts the already shipped route responses, resolver-profile-gated answers, and
  losing-branch negative checks. This proves completed source-family job lifecycle state can
  coexist with replayed existing consumer-capability responses, including the local supported
  resolver-profile answers, after source-family backfill; it does not prove those synthetic jobs
  admitted the route data or
  graduate unsupported coverage, ENSv2 exact-name support, wrapper/migration history, manifest
  capabilities, public API routes, or consumer-replacement semantics
- the automatic-bootstrap backfill conformance lock is focused with
  `cargo test --manifest-path tests/conformance/Cargo.toml backfill_sources_auto_bootstrap -- --nocapture`.
  It uses the same local Postgres and per-test temporary database expectations as the nearby
  backfill conformance entries, persists automatic-bootstrap job identity and effective ranges,
  covers the conformance-only Basenames unknown-start exclusion case, and keeps unsupported
  coverage non-graduated. It exercises no live RPC scheduling or drain; those remain covered by
  `apps/indexer` unit tests
- `reorg_chaos_drill_conformance_job` is the standalone reorg chaos drill conformance entry
  point. It is focused with
  `cargo test --manifest-path tests/conformance/Cargo.toml reorg_chaos_drill_conformance_job`.
  The drill seeds the existing replay-style stale current corpus, applies shipped reorg orphaning
  helpers to losing-branch raw blocks and normalized events, runs the shipped indexer raw-fact
  normalized-event replay against a deterministic canonical raw-log probe, runs
  `bigname-worker replay all-current-projections`, and reuses existing consumer-response
  convergence and losing-branch absence assertions. It uses the same local Postgres and per-test
  temporary database expectations as the rest of the harness; when no local Postgres is available,
  the focused chaos drill may be treated as a no-run fallback instead of a route contract failure,
  and should be rerun in an environment with Postgres before relying on it. This validates
  reorg/replay hardening over shipped route contracts only; it does not widen route support or
  route coverage semantics, graduate unsupported coverage, change verified execution support,
  manifest capabilities, ENSv2 exact-name support, Basenames path classes, public API routes, or
  consumer-replacement semantics
