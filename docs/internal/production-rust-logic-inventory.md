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
| `crates/storage` | 144 | 25,984 |
| `crates/adapters` | 106 | 21,377 |
| `apps/indexer` | 65 | 17,003 |
| `apps/api` | 65 | 14,218 |
| `apps/worker` | 69 | 12,491 |
| `crates/manifests` | 33 | 7,602 |
| `crates/execution` | 36 | 6,386 |
| `crates/domain` | 1 | 6 |
| Total | 519 | 105,067 |

The current file-size gate has no hard-failing production Rust files.

Additional advisory-only warnings remain above 500 LOC, including
`crates/adapters/src/block_derived_normalized_events/event_builders.rs` at 593
LOC, `crates/adapters/src/ens_v1_unwrapped_authority.rs` at 572 LOC, and
`crates/storage/src/raw_code.rs` at 562 LOC. `apps/indexer/src/main.rs` is no
longer a hard failure, but remains a wiring-file advisory at 493 LOC.
`crates/manifests/src/lib/views/resolver_profiles/ens_v1.rs` is now advisory
only at 577 LOC. `apps/indexer/src/main/repair.rs` is now advisory only at 583
LOC. `crates/manifests/src/lib/model.rs` is now advisory only at 578 LOC.
`crates/adapters/src/ens_v1_unwrapped_authority/pipeline/apply.rs` is now
advisory only at 584 LOC.
`apps/api/src/responses/app_facing/records_declared_values.rs` is now advisory
only at 595 LOC.
`crates/adapters/src/ens_v1_unwrapped_authority/preload.rs` is now below the
advisory threshold at 347 LOC, and its largest preload helper module is
`preload/registrar_state.rs` at 411 LOC.

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
- `crates/adapters/src/ens_v2_{registry,permissions,resolver}/decode.rs`
  now use `alloy-sol-types` `sol_data` tuple decoding for event bodies while
  preserving existing topic handling and storage-facing normalized output
  shapes.
- `crates/storage/src/projection_helpers.rs` now owns the storage keyset
  page split/truncate/next-cursor pattern used by address names, children, and
  permissions.
- `apps/api/src/support/cursors.rs` now uses the maintained `hex` crate for
  cursor bytes instead of local nibble parsing.
- `crates/storage/src/sql_row.rs` now owns the common "required SQL column with
  missing-column context" helper used by storage row decoders, replacing the
  first 350 exact `PgRow::try_get(...).context("missing ...")?` call sites.
- `crates/storage/src/sql_row.rs` is now the shared required-column SQL row
  helper for storage, worker, adapter, and execution decoders; the duplicate
  worker, adapter, and execution helper modules were removed.
- `apps/indexer/src/provider/decode.rs` now owns local JSON-RPC object,
  required-field, optional-hex, and normalization helpers for provider payload
  decoding instead of repeating object walking at every field.
- `apps/indexer/src/provider/decode.rs` now decodes provider block,
  transaction, receipt, and log payloads through typed serde DTOs backed by
  Alloy primitives for quantities, addresses, hashes, and bytes. Block hash/root
  strings stay permissive at the provider boundary to preserve sparse fixture
  and cache-fill behavior.
- `crates/execution/src/ens_resolution_abi.rs` now defines Universal Resolver
  and resolver selector calls with Alloy `sol!`/`SolCall`, deriving selector
  constants, calldata, and return decoding from generated call types.
- `crates/adapters/src/ens_v1_unwrapped_authority/abi.rs` now decodes
  registrar, resolver, wrapper, and registry address event bodies through Alloy
  `sol_data` parameter decoding. The manual dynamic-string/bytes offset readers
  and registrar expiry word-offset helpers were removed.
- Workspace `Cargo.toml` now owns direct Alloy dependency versions for adapters,
  execution, and indexer crates.
- `apps/api/src/routes/parameters.rs` and
  `apps/api/src/routes/parameters/app.rs` now own route parameter lists as route
  catalog data. The duplicate OpenAPI-side parameter helper files and
  `ApiRouteParameters` switchboard were removed; OpenAPI now renders the route
  catalog instead of owning parameter construction.
- `apps/api/src/responses/app_facing/records_declared_values.rs` now reuses
  record-key indexing helpers for verified/declaration entries and selector
  family lookups.
- `crates/manifests` now has a first-class manifest ABI surface for
  Alloy-parseable event/function fragments, with loader validation for fragment
  shape, duplicate canonical signatures, and declared contract roles. This is
  the landing zone for moving adapter event constants and execution call ABI
  declarations out of production code.
- The ENSv2 Sepolia registry, registrar, and resolver manifests now declare the
  event ABI fragments currently consumed by the ENSv2 adapters, with upstream
  citations next to the manifest declarations.
- `crates/manifests` now derives canonical event signatures, event topic0s,
  and function selectors from manifest ABI fragments through Alloy, and exposes
  active manifest ABI events from persisted manifest payloads for adapter use.
- `crates/adapters/src/ens_v2_registrar` now loads `NameRegistered` and
  `NameRenewed` topic0s from active manifest ABI events instead of owning
  local event signature constants.
- `crates/adapters/src/ens_v2_registry` now loads the registry/root registry
  event topic0s from active manifest ABI events. The root manifest declares the
  same registry event fragments so scoped root-registry runs do not fall back to
  code-owned signature strings.
- `crates/adapters/src/ens_v2_resolver` and
  `crates/adapters/src/ens_v2_permissions` now load resolver and permission
  event topic0s from active manifest ABI events. Production ENSv2 adapter
  decoders no longer own event signature strings for direct adapter matching.
- Direct ENSv2 registry, registrar, resolver, and permissions decoders now use
  `sol!`/`SolEvent` to validate and decode full logs, including indexed topics
  and event bodies, after the manifest-owned canonical signature gate matches.
- `crates/adapters/src/block_derived_normalized_events` now loads ENSv1
  wrapper/registrar and ENSv2 registry/registrar/resolver preimage event
  topic0s from active manifest ABI events by `source_manifest_id` and canonical
  signature. Duplicate event names are matched with signature keys, and event
  payloads now decode through generated `sol!`/`SolEvent` types instead of
  manual dynamic offsets or tuple-only body decoders. Source-family dispatch now
  prevents the old try-each-builder fallback path.
- `crates/adapters/src/ens_v1_unwrapped_authority` now loads authority
  registrar, registry, resolver, and wrapper topic0s from active manifest ABI
  events by canonical signature. Duplicate event names are matched through
  signature-keyed manifest lookup for observation, resolver-gate,
  migration-guard, generic resolver raw-log filtering, and restricted replay
  preload paths; stable event-body decoding for those observation paths now uses
  Alloy tuple decoding.
- `crates/adapters/src/ens_v1_unwrapped_authority/pipeline.rs` dropped below
  the hard oversized-file threshold by moving binding-authority identity-row
  staging helpers into `pipeline/identity.rs`.
- `crates/storage/src/evm_primitives.rs` now owns storage-side EVM address and
  hash normalization through Alloy `Address`/`B256`, while preserving existing
  lowercase fallback behavior for non-standard sentinels and placeholder values.
- `apps/api/src/support/query_parsing.rs` now normalizes standard EVM addresses
  through Alloy and keeps the prior permissive lowercase fallback at the API
  ingestion boundary.
- `apps/api/src/responses/resolution_verified/readback.rs` now names the
  compact-selector cache probe and the full-selector fallback explicitly, so
  the only remaining two-shape cache lookup documents why it exists.
- `crates/adapters/src/ens_v1_unwrapped_authority/abi.rs` now routes
  exact-signature Alloy decode misses through explicit skip helpers instead of
  ad hoc `ok()` handling at each observation site.
- `crates/adapters/src/ens_v1_unwrapped_authority/abi.rs` now decodes
  authority registrar, resolver, and wrapper observation logs through generated
  `sol!`/`SolEvent` types instead of tuple-only event-body decoders. The
  preload-only owner-address payload uses the shared ABI word address helper.
- `crates/manifests/src/lib/views/abi.rs` now reads manifest identity and
  source-family context from persisted view columns while decoding only ABI
  fragments from `manifest_payload`, so manifest-derived topic lookup works for
  older minimal payload rows without restoring code-owned ABI constants.
- `crates/adapters/src/adapter_manifest.rs` no longer carries the unused
  name-keyed manifest event topic loader. Active adapter topic matching now goes
  through canonical signature keys only.
- Generic ENSv1 resolver record observation now decodes ABIChanged, content,
  DNS, interface, and data logs through generated `sol!`/`SolEvent` types,
  removing the local per-field event-body/topic skip helpers.
- The worker-local `evm.rs` shim was removed. Worker address/hash normalization
  call sites in address names, primary names, record inventory, name current,
  execution, permissions, resolver target loading, and manifest drift now import
  the shared storage EVM normalizers directly.
- ENSv1 reverse-claim topic matching now derives the `ReverseClaimed` topic0
  from an Alloy-generated `sol!`/`SolEvent` event ref instead of owning a
  hand-written signature string plus Keccak helper call.
- `apps/worker/src/replay/rebuild.rs` now centralizes all-current projection
  replay skip/mark/summary wiring behind one helper, leaving the ordered
  projection list as data-shaped calls instead of seven duplicated branches.
- `bigname_storage::{CanonicalityState, SurfaceBindingKind}` now decode
  directly from SQL text through SQLx, allowing worker projection loaders to
  use `query_as`/`FromRow`. The name-current decoder module and address-name,
  permission, and record-inventory event decode functions were removed.
- Storage row decoders now read `CanonicalityState` and `SurfaceBindingKind`
  through the shared SQLx decoders instead of repeatedly loading strings and
  reparsing them at each raw/identity/history/projection decode site.
- Adapter raw-log and preload row loaders now share the same SQLx enum decode
  path for canonicality and binding-kind fields. The ENSv2 common
  canonicality parse shim and the unused ENSv1 authority ABI parse helper were
  removed.
- `crates/adapters/src/ens_v1_unwrapped_authority/preload.rs` now centralizes
  its canonical/safe/finalized SQL filter and collapses selected registrar
  replay reference aggregation to one JSON payload, removing repeated reference
  column aggregates and hand-decoded optional reference columns.
- `apps/indexer/src/main.rs` dropped below the hard wiring-file threshold by
  moving the run-mode unit test into the existing `main/tests` include tree.
- `crates/manifests/src/lib/views/resolver_profiles/ens_v1.rs` dropped below
  the hard oversized-file threshold by reusing the shared resolver-profile
  admission sort helper.
- `apps/indexer/src/main/repair.rs` dropped below the hard oversized-file
  threshold by reusing the storage normalized-event JSONB serializer instead of
  carrying a duplicate NUL-safe JSON sanitizer.
- `crates/manifests/src/lib/model.rs` dropped below the hard oversized-file
  threshold by replacing its hand-written optional `start_block` Serde visitor
  with direct optional integer deserialization plus the same non-negative
  conversion check.
- `crates/adapters/src/ens_v1_unwrapped_authority/pipeline/apply.rs` dropped
  below the hard oversized-file threshold by removing the one-use
  `apply_authority_raw_logs` wrapper and letting the restricted replay caller
  own the matched-log counting loop directly.
- `apps/api/src/responses/app_facing/records_declared_values.rs` dropped below
  the hard oversized-file threshold by moving compact record inventory lookup,
  inventory-derived payload, and inventory-source helpers into
  `records_declared_inventory.rs`.
- `crates/adapters/src/ens_v1_unwrapped_authority/preload.rs` dropped below the
  hard oversized-file threshold by splitting restricted replay preload support
  into `preload/{registrar_state,registrar_history,resolver,support,wrapper_registry}.rs`
  while preserving adapter-owned history preload and canonicality filters.

## Highest leverage cleanup map

| Logic family | Current locations | Replace or centralize with | Expected payoff |
| --- | --- | --- | --- |
| EVM ABI words, event topics, hex, hashes | `crates/adapters/src/evm_abi.rs` now owns shared adapter topic/static-word normalization, Keccak, topic-hash, hex, namehash, child-namehash, Alloy tuple decode, address formatting, and `U256` formatting helpers; ENSv2 registrar, registry, permissions, resolver, ENSv1 authority observation, and block-derived preimage event bodies now decode with generated `sol!`/`SolEvent` types. `crates/adapters/src/adapter_manifest.rs` now loads manifest-owned event topic0s for direct ENSv2 adapters, block-derived ENSv1/ENSv2 preimage matching, and ENSv1 unwrapped-authority matching keyed by canonical event signature. `crates/execution/src/ens_resolution_abi.rs` now uses Alloy `sol!`/`SolCall` for resolver selectors, calldata, and return decoding. `crates/manifests` now accepts Alloy-validated event/function fragments so manifest versions can become the authoritative ABI inventory. Remaining duplicates are in generated/dynamic ABI lookup from manifests, execution DNS/namehash helpers, `apps/indexer/src/provider/decode.rs`, and `apps/indexer/src/main/reconciliation/payload.rs` | Keep using `alloy-primitives` for `Address`, `B256`, `U256`, `Bytes`, `FixedBytes`, `hex`, `keccak256`; continue replacing code-owned ABI shape lists with `sol!`, `SolCall`, `SolEvent`, and manifest-backed `alloy-json-abi`/`alloy-dyn-abi` lookup where that does not obscure review; keep indexed-topic normalization narrow and explicit | Large LOC reduction in adapters, fewer hand-rolled offset/word parsers, less duplicated topic hashing |
| Provider JSON-RPC typed decoding | `apps/indexer/src/provider/decode.rs` now uses typed serde DTOs with Alloy `U256`, `Address`, `B256`, and `Bytes` for quantities, transaction/receipt/log hashes, addresses, topics, byte blobs, and log data. Block hash/root strings remain normalized strings because existing provider fixtures and raw-payload cache-fill paths intentionally accept sparse or placeholder values. Remaining manual decoding lives in provider transport/bundle readers, request filter construction, and `reth_db` conversion boundaries | Keep current transport initially; evaluate narrower typed request/filter structs next, and only move to full `alloy-rpc-types-eth` block/receipt/log types if cache payloads and fixture contracts can tolerate their stricter headers | Removes brittle `serde_json::Value` object walking and custom hex parsing in provider code while avoiding accidental behavior tightening |
| Address/hash normalization | Adapter hash/hex/namehash helpers are centralized in `evm_abi`; storage now owns shared `evm_primitives` for Alloy-backed `Address`/`B256` canonicalization with lowercase fallback, and worker ingestion imports those helpers directly instead of duplicate parsing. API query parsing still owns a small Alloy-backed address helper. Remaining local normalization appears in indexer, manifests, adapters, and execution path validation | Keep one helper per owner crate where fallback semantics differ; parse with Alloy where EVM-shaped, return canonical lower `0x` strings, and preserve sentinels only at boundaries that already accepted them | Prevents drift between "lowercase only" and "validated EVM address/hash" call sites |
| Canonicality and binding-kind parsing/rank | First slice landed: `CanonicalityState::rank`, `CanonicalityState::weakest`, and public `SurfaceBindingKind::parse` now cover indexer/adapters/storage/worker call sites with the canonical storage ordering; projection summaries with intentionally different ordering remain local | Continue replacing wrappers where semantics match; leave summary-specific rank orders local until their meaning is documented | Deletes repeated match blocks and reduces risk when enum variants change |
| Projection JSON summaries | `apps/worker/src/projection_json.rs` now covers repeated worker timestamp formatting, JSON path reads, and JSON value dedupe; remaining repeated worker families are provenance envelopes, chain-position maps, summary-specific canonicality ranks, and chain slots. API still has response-side JSON helpers | Continue growing worker-local `projection_json` with provenance, chain-position, and canonicality primitives where semantics match; consider storage helpers only for projection-shared public row shapes | Reduces repeated `serde_json` assembly and makes coverage/provenance mistakes easier to spot |
| SQL row decoding boilerplate | `crates/storage/src/sql_row.rs` covers the shared required-column helper for storage, worker, adapter, and execution decoders: 350 exact storage call sites, 70+ worker call sites, 199 adapter call sites, and 30 execution revalidation call sites; manual `PgRow::try_get(...).context(...)` decoders remain across storage edge cases, manifests, adapter custom-context loaders, worker custom-context loaders, and API/indexer support; almost no production `query_as`/`FromRow` usage | Continue replacing same-semantics row reads with the shared helper where dependent crates already use storage; use `sqlx::FromRow` for plain rows; add small helper wrappers for contextual field reads and non-negative conversions where dynamic SQL prevents derive | Cuts a large amount of repetitive error text and makes row shape changes easier |
| Keyset pagination and cursors | `crates/storage/src/projection_helpers.rs` covers shared storage page-size checks and keyset page split/truncate/cursor selection for address names, children, and permissions; API cursor envelope helpers remain in `apps/api/src/support/cursors.rs` and now use `hex` for cursor bytes | Continue with shared cursor envelope helpers in API; storage keyset helper for `(field1, field2, ...) > (...)` | Lower API/storage paging LOC and fewer subtle cursor-field validation variants |
| Adapter active-emitter and source-scope flow | `crates/adapters/src/ens_v2_common.rs`, `ens_v2_*`, `ens_v1_reverse_claim`, `ens_v1_subregistry_discovery`, `ens_v1_unwrapped_authority`, `block_derived_normalized_events`, plus indexer replay/backfill source-scope builders | Adapter-local support modules for normalized source-scope targets, emitter interval overlap, active-at-block lookup, scoped ranges, and summaries. Event identity loading and by-kind counters are now in `normalized_event_support` | Removes repeated range-overlap and source-family filtering logic across adapter families |
| Normalized-event builders and persistence summaries | `crates/adapters/src/normalized_event_support.rs` covers shared event identity loading and by-kind counters; remaining duplication lives in `crates/adapters/src/*/normalized.rs`, `events.rs`, `event_building.rs`, `persistence_summary.rs`, and manifest event identity/raw fact builders | Continue with shared `NormalizedEventBuilder`/summary helpers inside `crates/adapters`, with adapter-specific state supplied as data | Reduces repeated event identity, raw fact ref, by-kind count, and inserted count code |
| OpenAPI schema/parameter JSON | Route definitions now own parameter lists through `apps/api/src/routes/parameters.rs` and `apps/api/src/routes/parameters/app.rs`; `apps/api/src/openapi/route_operations.rs` renders those route-owned parameter specs. Larger schema/response JSON remains in `schemas.rs`, `responses.rs`, and the route operation renderer | Continue deleting OpenAPI-owned route semantics; evaluate schema generation only if DTO-derived schemas match public docs without obscuring contract review | Removes the duplicate parameter-list owner while preserving generated OpenAPI output |
| Compact app-facing response transforms | `apps/api/src/responses/app_facing/records_declared_values.rs` now shares record-key indexing helpers for verified/declaration entries and selector-family lookups; remaining repeated transforms live in `handlers/app_facing/*.rs`, `responses/projections*.rs`, and compact record/role/event helpers | Extract typed compact record/role/event builders before considering a schema library; share selector parsing and record-key helpers with execution/storage support | Shrinks the largest API response file and improves reviewability |
| ENSv1 restricted replay preload pipeline | `crates/adapters/src/ens_v1_unwrapped_authority/{preload.rs,preload/*.rs,pipeline.rs,pipeline/apply.rs,materialization.rs,observation.rs,loading/raw_logs.rs}` | `preload.rs` now owns restricted replay orchestration while `preload/*.rs` owns registrar state/history, resolver state, row/provenance support, and wrapper/registry history helpers. Continue with behavior-preserving dedupe in pipeline/materialization/loading before changing semantics | Most LOC impact, but should stay as narrow adapter-owned slices to avoid replay-boundary drift |

## EVM and Alloy inventory

The codebase already uses Alloy in `crates/execution`, `crates/adapters`, and
`apps/indexer`, but use is uneven:

- `crates/execution/src/ens_text_records.rs` and
  `crates/execution/src/ens_resolution_ccip.rs` already use `sol!`, `SolCall`,
  and typed ABI encode/decode.
- `crates/execution/src/ens_resolution_abi.rs` uses Alloy `sol!`/`SolCall`
  generated types for Universal Resolver and resolver selector calldata/return
  decoding. It still owns execution-local DNS/namehash and hex helpers.
- `crates/adapters/src/evm_abi.rs` centralizes indexed-topic/static-word
  normalization, Alloy tuple decoding, and `SolEvent` log decoding for adapters.
  Direct ENSv2 registrar, registry, permissions, resolver, ENSv1 authority
  observation, and block-derived preimage logs decode through generated `sol!`
  event types. Topic matching for the direct ENSv2, block-derived, and ENSv1
  authority adapters is
  manifest-derived, and block-derived dispatch is source-family keyed instead
  of builder probing.
- `crates/storage/src/evm_primitives.rs` centralizes storage-side and worker
  EVM address/hash parsing with Alloy while leaving legacy non-standard values
  as lowercase strings at the same ingestion boundaries.
- `apps/indexer/src/provider/reth_db/convert.rs` uses Alloy/Reth primitives for
  DB-backed provider data. `apps/indexer/src/provider/decode.rs` now uses
  typed serde DTOs backed by Alloy primitives for JSON-RPC quantities,
  transaction/receipt/log hashes, addresses, topics, and byte fields while
  preserving string-normalized block hash/root compatibility.

Near-term replacement candidates:

- Replace remaining code-owned ABI shape declarations with generated `sol!`
  event types or manifest-backed dynamic ABI decoding where the generated or
  dynamic surface stays easy to review. Direct ENSv2 adapters, ENSv1 authority
  observation, and block-derived preimage builders use `SolEvent`; remaining
  ABI shape declarations should move toward manifest-backed lookup where that
  stays reviewable.
- Move source-family event/call definitions into manifest `[[abi.events]]` and
  `[[abi.calls]]`, then consume them through `alloy-json-abi` and
  `alloy-dyn-abi` lookup helpers. ENSv2 Sepolia registry/registrar/resolver
  fragments are seeded; ENSv1 authority and block-derived topic matching now
  consume manifest-owned canonical signatures, leaving call fragments and
  generated/dynamic event-body lookup as the next ABI inventory targets.
- Replace topic hash functions like `keccak_signature_hex`, `*_topic0`, and
  signature arrays with constants derived from the `sol!` event types where the
  generated type exposes the selector/topic.
- Keep adapter output types string-shaped for storage compatibility, but parse
  EVM values through `Address`, `B256`, `U256`, and `Bytes` first.
- Move shared `namehash`, `child_namehash`, `dns_encode`, `dns_decode`, `hex_32`,
  and `hex_string` helpers into one adapter/execution support module. Do not put
  ENS behavior claims in that module without upstream citations.

Provider-side candidates:

- Continue typed JSON-RPC response structs around the current
  `JsonRpcProvider` request/batch transport; prefer narrow DTOs where
  `alloy-rpc-types-eth` full block or receipt types would reject sparse cached
  payloads.
- Normalize once at conversion boundaries: `alloy` typed response to existing
  `ProviderBlock`, `ProviderTransaction`, `ProviderReceipt`, and `ProviderLog`.
- After typed decoding is stable, consider whether `alloy-provider` can replace
  custom transport pieces. This is a second slice because the current transport
  preserves payload-cache and hash-pinned revalidation behavior.

Dependency note:

- `cargo tree -d --workspace` does not show duplicate Alloy major/minor versions
  in the resolved tree. Direct Alloy version specs now live in
  `[workspace.dependencies]`; member crates should use `.workspace = true`.

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

- `crates/storage/src/sql_row.rs` centralizes required-column reads for exact
  `missing <column>` contexts across storage and dependent production crates.
  The first passes replaced 350 storage call sites, 70+ worker call sites, 199
  adapter call sites, and 30 execution call sites without changing custom
  contextual errors or dynamic-column decoders.

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

These files are good candidates for generated `sol!` event definitions or
manifest-backed dynamic ABI lookup, plus shared hash/namehash helpers. Do this
before splitting observation logic so the split does not preserve code-owned ABI
shape lists.

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
   `namehash_hex`, `hex_32`, and `child_namehash` in `evm_abi`; storage, API,
   and worker standard EVM address/hash helpers now use Alloy with owner-local
   fallback semantics. Execution/indexer helper consolidation remains.
5. Partially done: direct ENSv2 registrar, registry, resolver, and permissions
   topic matching now uses active manifest ABI topic0s and full log decoding is
   `sol!`/`SolEvent` based. ENSv1 generic resolver records also use full
   `SolEvent` log decoding. Block-derived ENSv1/ENSv2 preimage matching plus
   ENSv1 authority adapter matching use manifest-owned topics keyed by canonical
   signatures. Block-derived preimage event bodies and ENSv1 authority
   observation logs now use full `SolEvent` log decoding; continue by moving
   call fragments and the remaining code-owned event shapes to manifest-backed
   ABI lookup where the dynamic surface stays reviewable.
6. Partially done: provider JSON-RPC response decoding now uses typed serde DTOs
   backed by Alloy primitives for hashes, addresses, quantities, topics, and
   byte blobs while keeping existing provider DTOs. Continue only where stricter
   `alloy-rpc-types-eth` types do not break sparse cached payload behavior.
7. Add storage keyset pagination helpers for simple tuple cursors, then migrate
   children and permissions before name-current. Page split/truncate/cursor
   selection is done for address names, children, and permissions.
8. Extract `apps/api/src/responses/app_facing/records_declared_values.rs` into
   request-selection, entry-lookup, and response-assembly submodules.
9. Split ENSv1 preload by family after row/provenance helpers land.
10. Re-run `scripts/check-rust-file-size` and remove allowlist entries for files
    that drop below threshold.
