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
  rejected
- the address-name collection contract seeds `address_names_current` rows plus the backing
  surfaces, resources, token lineage, and bindings; the harness covers the base
  `GET /v1/addresses/{address}/names` response plus shipped `namespace`, `relation`,
  `dedupe_by`, and additive `include=role_summary` handling
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
  `record_inventory` and `record_cache`, and verified query entries that remain explicitly
  unsupported until their own slice lands
- the coverage contract reuses the same exact-name rebuild seed and asserts that
  `GET /v1/coverage/{namespace}/{name}` keeps the same single-name `data` and top-level
  `coverage` object as exact-name lookup while exposing the explain-only coverage block in
  `declared_state`
- the primary-name bootstrap contract covers `GET /v1/primary-names/{address}` with explicit
  `mode=declared`, `mode=verified`, and `mode=both` envelopes for the bootstrap no-table path:
  all three keep the same `{address, namespace, coin_type}` `data`, `declared` returns only
  `declared_state.claimed_primary_name`, `verified` returns only
  `verified_state.verified_primary_name`, and `both` combines those sections; it requires both
  `namespace` and `coin_type`, asserts `400 invalid_input` for missing `namespace` / `coin_type`,
  malformed addresses, and malformed non-decimal `coin_type` values, `404 not_found` for
  unsupported namespaces, per-mode `status=not_found` tuple results when
  `primary_names_current` exists but the `(address, namespace, coin_type)` row is absent, and the
  explicit declared `claimed_primary_name` / verified `verified_primary_name` unsupported sections
  when the bootstrap path or a tuple-present row is returned; every mode also shares the bootstrap
  provenance invariant (`normalized_event_ids`, `raw_fact_refs`, and `manifest_versions` empty,
  `execution_trace_id=null`, `derivation_kind=primary_name_route_bootstrap`) plus the same
  unsupported coverage invariant (`status=unsupported`, `exhaustiveness=not_applicable`,
  `source_classes_considered=[]`, `enumeration_basis=primary_name_lookup`,
  `unsupported_reason="primary-name coverage is not yet supported"`), empty `chain_positions`,
  `consistency=head`, and a UTC `last_updated` timestamp
- the resource-permissions contract seeds `permissions_current` rows and covers both the base
  `GET /v1/resources/{resource_id}/permissions` collection response and the shipped `subject` and
  `scope` query filters
- the resolver-overview contract seeds `resolver_current` rows and asserts the shipped declared
  summary sections, including the supported `{status, count, items}` alias envelope narrowed to
  current `binding_kind=resolver_alias_path` bindings, plus projection provenance, coverage, and
  lowercase address normalization for `GET /v1/resolvers/{chain_id}/{resolver_address}`
- the address-history contract seeds `address_names_current` anchors plus the backing surfaces,
  resources, token lineage, bindings, and canonical normalized events; the harness covers the
  base `GET /v1/history/addresses/{address}` response with the shipped empty `declared_state`,
  normalized-event provenance, and default `both` scope behavior, plus the shipped
  `namespace=ens&relation=registrant` filter combination and
  `relation=effective_controller` with `scope=surface`, `scope=resource`, and `scope=both`
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
