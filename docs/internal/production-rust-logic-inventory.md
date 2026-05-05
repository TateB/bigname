# Production Rust logic inventory

Status: initial audit snapshot, 2026-05-05.

Gate: implementation-only inventory. Follow-up cleanup slices stay
implementation-only when they preserve API semantics, manifest semantics, storage
ownership, shared IDs, and coverage meaning. If a slice changes those public
rules, update the public contract docs in the same change before code lands.

Scope: production Rust under `apps/` and `crates/`, excluding tests and generated
output. This document records where logic currently lives so related cleanup can
be grouped instead of handled as one-off file splits.

## Maintenance rules

- When adding or moving production logic, update the relevant inventory row here.
- When deleting a duplication family, keep the row and mark it `done` with the
  replacement path.
- Do not put ENSv1, ENSv2, or Basenames behavioral claims in this file without
  the required `.refs/` upstream citation. This document should inventory local
  implementation shape, not restate upstream semantics.
- Keep `crates/domain` narrow. Prefer crate-local helpers or existing owner
  crates unless a type is truly cross-crate and contract-like.

## Size snapshot

Production Rust snapshot from the working tree:

| Area | Production files | LOC |
| --- | ---: | ---: |
| `crates/storage` | 142 | 26,143 |
| `crates/adapters` | 101 | 21,082 |
| `apps/indexer` | 65 | 17,036 |
| `apps/api` | 64 | 14,185 |
| `apps/worker` | 71 | 13,163 |
| `crates/manifests` | 31 | 7,279 |
| `crates/execution` | 36 | 6,293 |
| `crates/test-support` | 1 | 194 |
| `crates/domain` | 1 | 6 |
| Total | 512 | 105,381 |

The current file-size gate flags these oversized production files as the first
places to revisit after logic dedupe:

- `crates/adapters/src/ens_v1_unwrapped_authority/preload.rs` at 1,762 LOC.
- `apps/api/src/responses/app_facing/records_declared_values.rs` at 783 LOC.
- `crates/adapters/src/ens_v1_unwrapped_authority/pipeline.rs` at 635 LOC.
- `apps/indexer/src/main/repair.rs` at 615 LOC.
- `crates/adapters/src/ens_v1_unwrapped_authority/pipeline/apply.rs` at 612 LOC.
- `crates/adapters/src/ens_v1_unwrapped_authority/observation.rs` at 608 LOC.
- `crates/manifests/src/lib/views/resolver_profiles/ens_v1.rs` at 602 LOC.
- `crates/adapters/src/ens_v1_unwrapped_authority/loading/raw_logs.rs` at 594 LOC.
- `crates/manifests/src/lib/views/watched.rs` at 587 LOC.

Addressed slices:

- `crates/adapters/src/ens_v1_unwrapped_authority/materialization.rs`
  dropped from 645 LOC to 523 LOC by moving token-lineage/resource builders
  into `materialization/lineage.rs`.
- `apps/worker/src/projection_json.rs` now owns worker timestamp formatting,
  JSON path reads, and JSON value dedupe. Existing worker projection modules
  re-export the helpers where tests or sibling modules already used those paths.
- `crates/adapters/src/normalized_event_support.rs` now owns adapter
  normalized-event identity loading and by-kind inserted/synced counters.
- `crates/adapters/src/evm_abi.rs` now owns adapter Keccak, event-signature
  topic hashes, prefixed/non-prefixed hex strings, namehash, and child-namehash
  helpers, backed by Alloy primitives.
- `crates/adapters/src/ens_v2_registrar/decoding.rs` is the first adapter
  decoder converted from manual ABI word walking to `alloy-sol-types`
  `sol_data` tuple decoding.
- `crates/storage/src/projection_helpers.rs` now owns the storage keyset
  page split/truncate/next-cursor pattern used by address names, children, and
  permissions.
- `apps/api/src/support/cursors.rs` now uses the maintained `hex` crate for
  cursor bytes instead of local nibble parsing.
- `crates/storage/src/sql_row.rs` now owns the common "required SQL column with
  missing-column context" helper used by storage row decoders, replacing the
  first 350 exact `PgRow::try_get(...).context("missing ...")?` call sites.
- `apps/worker/src/sql_row.rs` mirrors that owner-local required-column helper
  for worker projection loaders and replaced the first 70+ exact worker row
  decoder call sites.
- `crates/adapters/src/sql_row.rs` now owns the same required-column helper for
  adapter loaders, replacing the first 199 exact adapter row decoder call sites.
- `crates/execution/src/sql_row.rs` now owns the same helper for execution
  revalidation storage reads, replacing 30 exact decoder call sites.
- `apps/indexer/src/provider/decode.rs` now owns local JSON-RPC object,
  required-field, optional-hex, and normalization helpers for provider payload
  decoding instead of repeating object walking at every field.

## Highest leverage cleanup map

| Logic family | Current locations | Replace or centralize with | Expected payoff |
| --- | --- | --- | --- |
| EVM ABI words, event topics, hex, hashes | `crates/adapters/src/evm_abi.rs` now owns shared adapter ABI-word, Keccak, topic-hash, hex, namehash, and child-namehash helpers; `ens_v2_registrar/decoding.rs` now uses `alloy-sol-types` for event data decoding. Remaining duplicates are in adapter decode/event builders, `crates/execution/src/ens_resolution_abi.rs`, `apps/indexer/src/provider/decode.rs`, and `apps/indexer/src/main/reconciliation/payload.rs` | Keep using `alloy-primitives` for `Address`, `B256`, `U256`, `Bytes`, `FixedBytes`, `hex`, `keccak256`; continue replacing manual ABI word walking with `alloy-sol-types` `sol!`, `SolCall`, `SolEvent`, and `SolValue` where the event shape is stable | Large LOC reduction in adapters, fewer hand-rolled offset/word parsers, less duplicated topic hashing |
| Provider JSON-RPC typed decoding | `apps/indexer/src/provider/decode.rs` now has local helpers for JSON-RPC object/field reads, optional hex parsing, and normalized optional hashes/addresses; remaining manual decoding lives in provider transport/bundle readers and `reth_db` conversion boundaries | Keep current transport initially, continue shrinking repeated `serde_json::Value` object walking, then deserialize into `alloy-rpc-types-eth` block, transaction, receipt, log, filter, and block-id types before converting to storage DTOs | Removes brittle `serde_json::Value` object walking and custom hex parsing in provider code |
| Address/hash normalization | Adapter hash/hex/namehash helpers are centralized in `evm_abi`; `normalize_address` still appears in API, indexer, worker, adapters, manifests, storage, and execution path validation | One storage-format helper per owner crate: parse with Alloy where EVM-shaped, return canonical lower `0x` strings; expose narrow helpers from adapters/execution/provider modules | Prevents drift between "lowercase only" and "validated EVM address/hash" call sites |
| Canonicality and binding-kind parsing/rank | First slice landed: `CanonicalityState::rank`, `CanonicalityState::weakest`, and public `SurfaceBindingKind::parse` now cover indexer/adapters/storage/worker call sites with the canonical storage ordering; projection summaries with intentionally different ordering remain local | Continue replacing wrappers where semantics match; leave summary-specific rank orders local until their meaning is documented | Deletes repeated match blocks and reduces risk when enum variants change |
| Projection JSON summaries | `apps/worker/src/projection_json.rs` now covers repeated worker timestamp formatting, JSON path reads, and JSON value dedupe; remaining repeated worker families are provenance envelopes, chain-position maps, summary-specific canonicality ranks, and chain slots. API still has response-side JSON helpers | Continue growing worker-local `projection_json` with provenance, chain-position, and canonicality primitives where semantics match; consider storage helpers only for projection-shared public row shapes | Reduces repeated `serde_json` assembly and makes coverage/provenance mistakes easier to spot |
| SQL row decoding boilerplate | `crates/storage/src/sql_row.rs` covers the first storage-local required-column helper and 350 exact storage decoder call sites; `apps/worker/src/sql_row.rs` covers the matching worker helper for 70+ exact loader call sites; `crates/adapters/src/sql_row.rs` covers the adapter helper for 199 exact loader call sites; `crates/execution/src/sql_row.rs` covers 30 execution revalidation storage call sites; manual `PgRow::try_get(...).context(...)` decoders remain across storage edge cases, manifests, adapter custom-context loaders, worker custom-context loaders, and API/indexer support; almost no production `query_as`/`FromRow` usage | Continue replacing same-semantics row reads with owner-local helpers; use `sqlx::FromRow` for plain rows; add small helper wrappers for contextual field reads and non-negative conversions where dynamic SQL prevents derive | Cuts a large amount of repetitive error text and makes row shape changes easier |
| Keyset pagination and cursors | `crates/storage/src/projection_helpers.rs` covers shared storage page-size checks and keyset page split/truncate/cursor selection for address names, children, and permissions; API cursor envelope helpers remain in `apps/api/src/support/cursors.rs` and now use `hex` for cursor bytes | Continue with shared cursor envelope helpers in API; storage keyset helper for `(field1, field2, ...) > (...)` | Lower API/storage paging LOC and fewer subtle cursor-field validation variants |
| Adapter active-emitter and source-scope flow | `crates/adapters/src/ens_v2_common.rs`, `ens_v2_*`, `ens_v1_reverse_claim`, `ens_v1_subregistry_discovery`, `ens_v1_unwrapped_authority`, `block_derived_normalized_events`, plus indexer replay/backfill source-scope builders | Adapter-local support modules for normalized source-scope targets, emitter interval overlap, active-at-block lookup, scoped ranges, and summaries. Event identity loading and by-kind counters are now in `normalized_event_support` | Removes repeated range-overlap and source-family filtering logic across adapter families |
| Normalized-event builders and persistence summaries | `crates/adapters/src/normalized_event_support.rs` covers shared event identity loading and by-kind counters; remaining duplication lives in `crates/adapters/src/*/normalized.rs`, `events.rs`, `event_building.rs`, `persistence_summary.rs`, and manifest event identity/raw fact builders | Continue with shared `NormalizedEventBuilder`/summary helpers inside `crates/adapters`, with adapter-specific state supplied as data | Reduces repeated event identity, raw fact ref, by-kind count, and inserted count code |
| OpenAPI schema/parameter JSON | `apps/api/src/openapi/{schemas.rs,parameters.rs,app_facing_parameters.rs,responses.rs,route_operations.rs}` | First centralize schema builders and parameter builders; later evaluate `utoipa`, `schemars`, or `aide` only if DTO derive-based schemas match public docs without obscuring contract review | Good LOC reduction, but public-contract risk is higher than internal helper cleanup |
| Compact app-facing response transforms | `apps/api/src/responses/app_facing/records_declared_values.rs`, `handlers/app_facing/*.rs`, `responses/projections*.rs` | Extract typed compact record/role/event builders before considering a schema library; share selector parsing and record-key helpers with execution/storage support | Shrinks the largest API response file and improves reviewability |
| ENSv1 restricted replay preload pipeline | `crates/adapters/src/ens_v1_unwrapped_authority/{preload.rs,pipeline.rs,pipeline/apply.rs,materialization.rs,observation.rs,loading/raw_logs.rs}` | Split by responsibility after the helper work above: preload queries, selected state before replay, resolver state preload, provenance decoding, history mutation, identity materialization | Most LOC impact, but should happen after shared helpers land to avoid pure file shuffling |

## EVM and Alloy inventory

The codebase already uses Alloy in `crates/execution`, `crates/adapters`, and
`apps/indexer`, but use is uneven:

- `crates/execution/src/ens_text_records.rs` and
  `crates/execution/src/ens_resolution_ccip.rs` already use `sol!`, `SolCall`,
  and typed ABI encode/decode.
- `crates/execution/src/ens_resolution_abi.rs` uses Alloy primitive ABI helpers
  but still keeps local selector constants, DNS/namehash helpers, and hex helpers.
- `crates/adapters/src/evm_abi.rs` centralizes some manual ABI word parsing for
  adapters, but many adapter modules still match topic0 strings and decode
  topics/data field-by-field.
- `apps/indexer/src/provider/reth_db/convert.rs` uses Alloy/Reth primitives for
  DB-backed provider data, while `apps/indexer/src/provider/decode.rs` manually
  walks JSON-RPC `serde_json::Value` objects.

Near-term replacement candidates:

- Replace manual event data decoders in `crates/adapters/src/ens_v2_registry/decode.rs`,
  `crates/adapters/src/ens_v2_resolver/decode.rs`,
  `crates/adapters/src/ens_v2_permissions/decode.rs`, and
  `crates/adapters/src/ens_v2_registrar/decoding.rs` with local `sol!` event
  definitions and `SolEvent` decoding.
- Replace topic hash functions like `keccak_signature_hex`, `*_topic0`, and
  signature arrays with constants derived from the `sol!` event types where the
  generated type exposes the selector/topic.
- Keep adapter output types string-shaped for storage compatibility, but parse
  EVM values through `Address`, `B256`, `U256`, and `Bytes` first.
- Move shared `namehash`, `child_namehash`, `dns_encode`, `dns_decode`, `hex_32`,
  and `hex_string` helpers into one adapter/execution support module. Do not put
  ENS behavior claims in that module without upstream citations.

Provider-side candidates:

- Introduce typed JSON-RPC response structs from `alloy-rpc-types-eth` while
  preserving the current `JsonRpcProvider` request/batch transport.
- Normalize once at conversion boundaries: `alloy` typed response to existing
  `ProviderBlock`, `ProviderTransaction`, `ProviderReceipt`, and `ProviderLog`.
- After typed decoding is stable, consider whether `alloy-provider` can replace
  custom transport pieces. This is a second slice because the current transport
  preserves payload-cache and hash-pinned revalidation behavior.

Dependency note:

- `cargo tree -d --workspace` does not show duplicate Alloy major/minor versions
  in the resolved tree, but direct version specs are split across manifests. Move
  Alloy deps to `[workspace.dependencies]` with one set of versions before doing
  broader Alloy migrations.

## Internal helper inventory

### Address, hex, hash, and namehash helpers

Repeated names from the production function inventory:

- `normalize_address`: 12 local definitions.
- `hex_string`: 9 local definitions.
- `keccak256_hex`: 6 local definitions.
- `namehash_hex`: 4 local definitions.
- `normalize_hex_32`: 5 local definitions.
- `decode_hex_32`: 3 local definitions.

Preferred shape:

- `crates/adapters`: one `evm` helper module for adapter-side EVM primitives.
- `crates/execution`: either use the adapter helper only if that dependency is
  already intended, or keep an execution-local helper with the same public
  behavior and tests.
- `apps/indexer/provider`: provider conversion helpers should parse with Alloy
  and return existing provider DTOs.
- API and manifest string normalization should avoid importing Alloy unless the
  field is explicitly EVM-shaped; otherwise keep simple string normalization
  owner-local.

### Canonicality and binding kind helpers

Current status:

- Implemented canonical storage helpers:
  `CanonicalityState::rank`, `CanonicalityState::weakest`, and public
  `SurfaceBindingKind::parse`.
- Replaced matching duplicate rank/parse matches in storage row decoders,
  indexer reconciliation payload handling, execution revalidation, worker name
  current/address-name/resolver loading, and ENSv1 materialization.
- Left `record_inventory`, `permissions`, and resolver summary ranking local
  because those summaries currently order observed/canonical states differently
  than storage promotion logic.

Preferred shape:

- Keep new helpers in storage; do not reintroduce local parse/rank matches for
  the storage canonical ordering.
- Replace remaining wrappers only when the ordering matches storage semantics.
- Avoid widening `crates/domain`; these types already live in storage.

### Projection JSON helpers

Repeated worker helpers include:

- `build_provenance`
- `build_chain_positions`
- `build_canonicality_summary`
- `chain_slot`

Preferred shape:

- Continue using worker-local `apps/worker/src/projection_json.rs`, because most
  projection-summary assembly is worker-owned.
- Done in the second cleanup slice: `format_timestamp`, `json_str`, `json_i64`,
  and `dedupe_json_values` are centralized in `projection_json`.
- Use tiny input structs or traits such as `ProjectionEventRef` instead of
  forcing all projection event rows into one enum.
- Keep family-specific declared-state details in their current modules. Only
  move the invariant envelope pieces: normalized event IDs, raw fact refs,
  manifest versions, chain-position maps, canonicality summaries, timestamp
  formatting, and JSON dedupe.
- If API response code needs the same formatting, either expose the helper from a
  storage support module or create a parallel API helper. Do not create a broad
  domain crate just for formatting.

### SQL row decoding

Hotspots from pattern counts include:

- `crates/manifests/src/lib/views/drift.rs`
- `crates/adapters/src/ens_v1_unwrapped_authority/preload.rs`
- `crates/storage/src/address_names/decode.rs`
- `apps/worker/src/name_current/decode.rs`
- `crates/storage/src/identity/read.rs`
- `crates/execution/src/revalidation/storage.rs`
- `apps/worker/src/resolver/target_loading.rs`

Current status:

- `crates/storage/src/sql_row.rs` centralizes storage required-column reads for
  exact `missing <column>` contexts. The first pass replaced 350 storage call
  sites without changing custom contextual errors or dynamic-column decoders.
- `apps/worker/src/sql_row.rs` does the same for worker-owned projection loaders
  and command query decoders, replacing the first 70+ exact call sites.
- `crates/adapters/src/sql_row.rs` does the same for adapter loaders and source
  queries, replacing the first 199 exact call sites.
- `crates/execution/src/sql_row.rs` does the same for execution revalidation
  storage row reads, replacing 30 exact call sites.

Preferred shape:

- For static row shapes, derive or implement `sqlx::FromRow` and use
  `query_as::<_, RowType>`.
- For dynamic SQL, add tiny helpers like `required(row, "field")`,
  `optional(row, "field")`, and `non_negative_i64_to_u64(row, "field")`.
- Add `TryFrom<String>` or public parse helpers for storage enums that are
  decoded repeatedly.
- Avoid a macro-heavy abstraction until the same helper has removed boilerplate
  from at least two modules.

### Pagination and cursor helpers

Current pagination has two layers:

- API cursor envelope and validation in `apps/api/src/support/cursors.rs`.
- Storage keyset SQL in `crates/storage/src/*/paging.rs`, `address_names`, and
  `name_current`.

Preferred shape:

- Keep the API cursor envelope in API, but replace manual hex nibble code with a
  direct dependency on a maintained encoding crate or an already-owned shared
  helper.
- Introduce a storage-side helper for "page size to SQL limit", "truncate
  sentinel row", and tuple keyset expressions. Start with the simple tuple cases
  in children, permissions, and address names before tackling name-current
  timestamp sorting.
- Keep route-specific cursor field validation near API handlers until a shared
  typed cursor trait naturally appears.

## Adapter source-scope and normalized-event inventory

Repeated adapter flow:

1. Normalize optional source scope into source-family, address, from-block, and
   to-block targets.
2. Load watched/active emitters.
3. Intersect emitters with scoped ranges.
4. Load raw logs for block hashes or ranges.
5. Build normalized events.
6. Load existing event identities.
7. Count synced and inserted events by kind.

Current locations:

- `crates/adapters/src/ens_v2_common.rs` already covers some ENSv2 helpers.
- `crates/adapters/src/ens_v1_reverse_claim.rs` still keeps local existing-event
  identity and count helpers.
- `crates/adapters/src/block_derived_normalized_events/persistence.rs` has
  similar helpers.
- `crates/adapters/src/ens_v1_subregistry_discovery/{scope.rs,loader/*.rs}` and
  `crates/adapters/src/ens_v1_unwrapped_authority/{scope.rs,loading/*.rs}` share
  interval and active-emitter shapes.
- `apps/indexer/src/main/reconciliation/{adapter_sync.rs,replay.rs}` and
  `apps/indexer/src/main/backfill/fetching.rs` build matching source-scope
  tuples for adapter calls.

Preferred shape:

- First move event-identity and by-kind count helpers to a common adapters
  support module.
- Then factor source-scope target normalization and interval-overlap helpers.
- Only after those are stable, consider a shared `ActiveEmitter` type. Some
  families carry slightly different manifest/discovery metadata, so a trait or
  generic helper may be less disruptive than one mega struct.

## Oversized-file notes

### `crates/adapters/src/ens_v1_unwrapped_authority/preload.rs`

This is the largest production file and mixes several different responsibilities:

- selected name discovery for restricted replay
- registrar state before replay
- wrapper state before replay
- resolver state before replay
- record-version preload
- name metadata decoding
- preloaded history mutation
- provenance field parsing
- release-boundary block loading

Best cleanup order:

1. Extract row decoders and provenance parsers after the SQL row helper exists.
2. Extract each preload family into submodules: registrar, wrapper, resolver,
   record versions, metadata.
3. Leave the orchestration function in `preload.rs` once its helpers are small.

### `crates/adapters/src/ens_v1_unwrapped_authority/{observation.rs,ids.rs,abi.rs}`

These files are good candidates for Alloy `sol!` event definitions and shared
hash/namehash helpers. Do this before splitting observation logic so the split
does not preserve the current manual ABI surface.

### `apps/api/src/responses/app_facing/records_declared_values.rs`

The file currently owns compact requested-record selection, declared cache
mapping, verified cache mapping, known-text fallback, and response shaping.

Best cleanup order:

1. Extract record-key request selection and dedupe.
2. Extract declared versus verified entry lookup.
3. Extract final compact response assembly.
4. Only then evaluate schema/OpenAPI generation helpers.

### `apps/indexer/src/main.rs`

This is a wiring file above the target. Prefer extracting command-mode runners
and startup/runtime setup, not changing behavior. Do not bury CLI semantics in
generic helpers.

## Suggested cleanup slices

1. Done: add storage enum helpers:
   `CanonicalityState::rank`, `CanonicalityState::weakest`, public
   `SurfaceBindingKind::parse`, and replace local duplicate matches.
2. Partially done: add worker `projection_json` helpers for timestamp, JSON path
   reads, and JSON value dedupe across address names, permissions, record
   inventory, resolver, children, inspect, and name current.
3. Partially done: move adapter normalized-event identity loading and by-kind
   counters to `crates/adapters/src/normalized_event_support.rs`.
4. Partially done: consolidate adapter `hex_string`, `keccak256_hex`,
   `namehash_hex`, `hex_32`, and `child_namehash` in `evm_abi`; address
   normalization and execution/indexer helper consolidation remain.
5. Done for first pattern: convert ENSv2 registrar data decoding to
   `alloy-sol-types`; continue with small ENSv2 resolver/permission decoders
   before touching the large ENSv1 observation files.
6. Convert provider JSON-RPC response decoding from manual `serde_json::Value`
   walking to `alloy-rpc-types-eth`, while keeping existing provider DTOs.
7. Add storage keyset pagination helpers for simple tuple cursors, then migrate
   children and permissions before name-current. Page split/truncate/cursor
   selection is done for address names, children, and permissions.
8. Extract `apps/api/src/responses/app_facing/records_declared_values.rs` into
   request-selection, entry-lookup, and response-assembly submodules.
9. Split ENSv1 preload by family after row/provenance helpers land.
10. Re-run `scripts/check-rust-file-size` and remove allowlist entries for files
    that drop below threshold.
