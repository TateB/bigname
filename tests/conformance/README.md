# Conformance Harness

Bootstrap supported-read contract harness for already-shipped declared-state routes and
collections:

- `GET /v1/namespaces/{namespace}`
- `GET /v1/manifests/{namespace}`
- `GET /v1/names/{namespace}/{name}/children`
- `GET /v1/addresses/{address}/names`
- `GET /v1/names/{namespace}/{name}`
- `GET /v1/resolutions/{namespace}/{name}`
- `GET /v1/coverage/{namespace}/{name}`
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
  `history` declared-state sections on `GET /v1/names/{namespace}/{name}`
- the resolution contract reuses the exact-name rebuild seed and asserts the shipped mixed-route
  envelope on `GET /v1/resolutions/{namespace}/{name}` across `declared`, `verified`, and `both`
  modes, including required and invalid `records` handling plus the explicitly unsupported
  declared `topology` / `record_inventory` / `record_cache` sections and unsupported verified
  query entries that remain after the bootstrap slice
- the coverage contract reuses the same exact-name rebuild seed and asserts that
  `GET /v1/coverage/{namespace}/{name}` keeps the same single-name `data` and top-level
  `coverage` object as exact-name lookup while exposing the explain-only coverage block in
  `declared_state`
- the resource-permissions contract seeds `permissions_current` rows and covers both the base
  `GET /v1/resources/{resource_id}/permissions` collection response and the shipped `subject` and
  `scope` query filters
- the resolver-overview contract seeds `resolver_current` rows and asserts the shipped declared
  summary sections, projection provenance, coverage, and lowercase address normalization for
  `GET /v1/resolvers/{chain_id}/{resolver_address}`
- the address-history contract seeds `address_names_current` anchors plus the backing surfaces,
  resources, token lineage, bindings, and canonical normalized events; the harness covers the
  base `GET /v1/history/addresses/{address}` response with the shipped empty `declared_state`,
  normalized-event provenance, and default `both` scope behavior, plus the shipped
  `namespace=ens&relation=registrant` filter combination and
  `relation=effective_controller` with `scope=surface`, `scope=resource`, and `scope=both`
- collection-route conformance currently asserts the shared page envelope, returned default
  `page.sort`, and returned `page.page_size`; dedicated request-side `cursor` / `page_size`
  coverage is still needed for `GET /v1/addresses/{address}/names`,
  `GET /v1/names/{namespace}/{name}/children`,
  `GET /v1/resources/{resource_id}/permissions`,
  `GET /v1/history/addresses/{address}`,
  `GET /v1/history/names/{namespace}/{name}`, and
  `GET /v1/history/resources/{resource_id}`
- the shipped resolver-overview harness currently asserts `declared_state.aliases` as
  `UnsupportedSummary`; when alias support lands, that section must switch to the shared supported
  `{status, count, items}` envelope backed only by current `binding_kind=resolver_alias_path`
  bindings for the same resolver target
- exact-name explain-route conformance for
  `GET /v1/explain/names/{namespace}/{name}/surface-binding` and
  `GET /v1/explain/names/{namespace}/{name}/authority-control` is not covered yet
