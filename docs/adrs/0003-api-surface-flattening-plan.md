# ADR 0003: API Surface Flattening Plan

Status: Proposed
Date: 2026-05-06

## Context

The codebase has accumulated parallel ways to describe the same domain ideas. Some of
that complexity is necessary because bigname serves ENS, ENSv2, and Basenames across
declared projection reads, verified execution, historical replay, and app-facing
compact routes. Some of it is accidental.

The accidental complexity shows up as duplicated vocabulary and duplicated ownership:

- full, compact, declared, verified, app-facing, and legacy routes often describe the
  same name, resolver, record, coverage, snapshot, and support concepts separately
- compact routes sometimes inherit full-route knobs even when the compact route should
  expose only current app data
- OpenAPI output manually mirrors route, query, and schema concepts already present in
  the handlers and response builders
- exact-name snapshot selection, record-inventory boundary lookup, and compact record
  reads have historically been assembled in multiple places
- adapter scoped replay and raw-log loading repeat source-selection, block loading,
  ordering, and summary construction across adapter files
- public API and operational/audit concerns are too easy to blur, especially around raw
  facts, provenance, execution traces, and normalized events

This plan keeps the broad product scope. It does not delete history, record inventory,
typed unsupported objects, multi-chain snapshots, live verified execution, CCIP support,
Basenames transport, roles, children, primary-name behavior, ENSv2, or Basenames. The
goal is to delete parallel representations and make each capability have one owner.

The plan incorporates the following scope decisions:

- `consistency` stays unless a later implementation proves it is the main complexity
  source. It means the selected finality floor: `head`, `safe`, or `finalized`.
- historical events stay public; history is served through compact event DTOs and
  history routes, not raw normalized-event internals.
- typed unsupported objects stay public for documented sections.
- multi-chain snapshots stay public and explicit.
- raw facts stay immutable and rebuildable, but the public API does not expose them
  except through explicit audit or explain surfaces.
- namespace inference stays. The target default for namespace-omitted app-facing
  convenience reads is `ens`, but only after route-specific compatibility bridges are
  documented. Basenames is selected only by explicit namespace or documented convenience
  inference.
- compact responses must not carry irrelevant full-route or audit data.

This ADR is a proposed refactor plan. It does not by itself change the frozen public
API contract in `docs/api-v1.md`, `docs/api-v1-routes.md`, or
`docs/consumer-capabilities.md`. Each implementation slice that changes semantics must
update those docs in the same change.

## Decision

Adopt a compatibility-preserving flattening plan with one target model:

1. Keep one canonical full model for exact resources.
2. Keep compact app-facing routes as current UI reads with a small allowlisted payload.
3. Keep audit, explain, and operational data on explicit audit/explain/worker surfaces.
4. Generate OpenAPI from route definitions rather than hand-authoring duplicate schema
   and operation trees.
5. Move snapshot, record-inventory, support-state, and adapter replay concepts behind
   single owners.
6. Preserve compatibility while deprecating accidental knobs and vocabulary.

### Complexity budget

This ADR is a complexity-reduction plan, not an abstraction plan. A slice is successful
only when the system becomes easier to understand, easier to change, and smaller in
hand-written production code. The expected shape is lower cognitive load first, with LOC
falling naturally as duplicated concepts and duplicate code disappear.

Rules:

- do not add a new layer unless it replaces and deletes more route-local, adapter-local,
  or schema-local complexity than it introduces
- do not hide complexity behind a generic helper while preserving every old concept at
  the call sites
- compatibility shims must be named, documented, tested, and paired with a removal or
  acceptance decision
- generated OpenAPI must reduce hand-written public-shape code; generation that adds a
  second hard-to-debug model is a failed slice
- a slice that increases hand-written production LOC must explain why the increase is
  temporary, what later deletion it unlocks, and how that deletion will be measured
- prefer fewer public-ish structs, fewer parser branches, fewer response builders, and
  fewer route-local modes over clever reuse
- if two names mean the same thing, pick one name and delete or deprecate the other

The review question for every slice is: "Can a maintainer understand this capability
with fewer concepts and fewer files open than before?" If the answer is no, the slice is
not de-slopping even if the code looks more organized.

ADR 0004 adds a retroactive completion gate for this plan. A slice does not count as
completed ADR 0003 work merely because it introduces a shared helper, central table, or
compatibility scaffold. It must delete or collapse the duplicated subsystem concept
that made the capability hard to understand, or explicitly name the remaining
scaffolding as debt with a paired deletion target.

### Public route families

All public `v1` routes should fit one of these route families.

| Family | Examples | Snapshot behavior | Payload rule |
| --- | --- | --- | --- |
| Canonical full resources | `GET /v1/names/{namespace}/{name}`, `GET /v1/resolutions/{namespace}/{name}`, `GET /v1/resources/{resource_id}/permissions` | Uses documented snapshot selection: `at`, `chain_positions`, `consistency` | Full envelope; may include coverage, provenance, declared state, verified state |
| Current compact app reads | `GET /v1/names`, `GET /v1/names/{namespace}/{name}/records`, `GET /v1/resolve/{name}/records`, compact history/events | Current projection or route-local current execution policy | Compact envelope; allowlisted DTO fields only; no raw facts, full provenance, execution trace, projection IDs, or raw normalized-event bodies |
| Public explain and audit | `GET /v1/explain/...` | Reads a selected snapshot or persisted artifact as documented by the surface | Explicit trace/provenance detail only where the route name says explain or audit |

The route path should decide the shape. Query parameters should refine behavior within
that family, not switch the route into a different conceptual API.

Operational surfaces such as `bigname-worker inspect execution-trace` are outside the
public `v1` route catalog and must not be generated into OpenAPI. They can share the
same audit boundary rules, but they are worker-owned operational commands, not public
routes.

### Namespace rules

Namespace handling must be boring and visible in one place:

- canonical routes require `{namespace}` in the path
- `GET /v1/resolve/{name}` and `GET /v1/resolve/{name}/records` infer before lookup:
  - exact `base.eth` resolves as `namespace=ens`
  - `*.base.eth` resolves as `namespace=basenames`
  - other supported ENS names resolve as `namespace=ens`
- inferred Basenames requests never retry as ENS
- multi-namespace collection behavior must be explicit if it exists; omitted namespace
  must not silently create a cross-namespace or cross-chain query

Target state:

- app-facing convenience routes without an explicit namespace default to `namespace=ens`
  only after the route's current no-namespace contract has a documented bridge
- first-party app mappings should pass explicit namespace whenever the app knows it
- multi-namespace collection reads should require an explicit opt-in such as
  `namespace=all` if they remain public

Compatibility bridge:

- keep accepting currently documented inputs until the matching public docs are updated
  and a deprecation window is chosen
- the frozen `GET /v1/names` docs currently say that omitting `namespace` spans
  supported public namespaces; Slice 2 must preserve that behavior unless it ships a
  semantic migration with `docs/api-v1.md`, `docs/api-v1-routes.md`,
  `docs/consumer-capabilities.md`, and generated OpenAPI updates
- acceptable bridges for `/v1/names` are:
  - keep omitted namespace as cross-namespace and require app replacement code to pass
    `namespace=ens`
  - add an explicit `namespace=all` compatibility spell, then deprecate omitted
    namespace before changing its default
  - require explicit namespace for new app-facing clients while preserving old omitted
    namespace behavior for existing clients during the deprecation window
- no implementation slice may silently narrow `/v1/names` no-namespace results from
  all supported public namespaces to ENS-only

### Exact lookup and `/v1/names`

`GET /v1/names/{namespace}/{name}` is the canonical exact-name profile route. It owns
the full exact-name envelope, coverage, provenance, and snapshot behavior.

`GET /v1/names` remains a compact collection route for:

- address-owned or address-controlled names
- compact name search
- suggestions backed by projected names
- compatibility exact filtering with `name=...`

The compatibility `name=...` filter should be treated as a collection filter that
returns zero or one `CompactDomainSummary`, not as the conceptual owner of exact-name
profile semantics. First-party app replacement should map profile pages to
`GET /v1/names/{namespace}/{name}` and records panels to record routes, not to
`GET /v1/names?name=...`.

### Compact payload budget

Every compact DTO must have an explicit allowlist. Compact routes may include:

- display identity: `namespace`, `name`, `normalized_name`, `namehash` where useful
- app-facing ownership or registration summary fields
- app-facing record summary or record values requested by the route
- compact support metadata under `meta`
- pagination under `page`

Compact routes must not include these fields unless the route docs explicitly name them
as part of that route's compact contract:

- `logical_name_id`, `resource_id`, `surface_binding_id`, or projection version IDs
- raw facts
- raw normalized-event rows
- full provenance
- execution traces
- internal replay status
- record-cache internals that are not needed to render the compact records surface
- full coverage objects unless `meta=full` is documented for that route

Current compact identity exceptions are intentional:

- `GET /v1/addresses/{address}/names` exposes `logical_name_id` and `resource_id`
  because `dedupe_by=resource`, role summaries, and permissions links need the stable
  public identity anchors.
- `GET /v1/resources/lookup`, `GET /v1/roles`, and
  `GET /v1/names/{namespace}/{name}/roles` expose `resource_id` because resource
  identity is the route's purpose.
- resolver-overview binding and alias items may expose `logical_name_id`,
  `resource_id`, and `surface_binding_id` because they are route-owned resolver fan-in
  identities.

If a compact route needs a field for an app screen, it should add that field directly to
the compact DTO instead of tunneling a full subdocument through `meta`.

### `view=full`

`view=full` should not be a general escape hatch.

Rules:

- do not add new `view=full` behavior to compact routes
- remove `view=full` from OpenAPI and docs where it is only reserved or invalid
- where `view=full` already returns a documented full-envelope collection, keep
  compatibility until a canonical full route exists or the behavior is explicitly
  accepted
- prefer distinct route families over query-time shape switching

This is a cleanup target, not permission to silently break a shipped working route.

### `mode`

`mode` should exist only where declared and verified are both first-class semantics.

Rules:

- keep `mode=declared|verified|both` on canonical mixed routes such as
  `GET /v1/resolutions/{namespace}/{name}`
- do not expose `mode` globally as a common route concept for routes that only read
  projections
- compact records may keep a small value-source policy only if it is needed by the app
  surface; the implementation should use one route-owned policy function instead of
  scattering mode branches across handlers, response builders, readback, and execution
- compact current reads must not persist exact-snapshot execution artifacts unless the
  route is documented as a canonical verified route

### Support and unsupported states

Unsupported behavior must stay explicit.

Rules:

- documented unsupported sections return typed `UnsupportedSummary` objects
- selector-level unsupported records use one shared `ResultStatus` and
  `unsupported_reason`
- unsupported filters may be non-2xx errors when the route cannot execute the requested
  filter
- empty arrays mean known empty results, not unknown or unsupported
- coverage/support metadata is shared vocabulary, not route-local prose

The flattening target is one support-state vocabulary used by exact routes, compact
routes, record inventory, and resolver overview.

### Raw facts, normalized events, and history

Raw facts remain immutable and projections remain rebuildable, but raw facts are not a
public API concept.

Rules:

- adapters write identity rows and normalized events, not projection rows
- API code reads projections and execution output, not raw facts
- history routes read canonical normalized events and return route-owned event DTOs
- `GET /v1/events` and compact history routes may expose compact event payloads, but
  they must not expose raw normalized-event rows as their public contract
- audit/explain surfaces may expose attributable summaries, never broad raw-fact dumps

### History full view

The current public route docs document history `view=full` as the existing
normalized-event history row shape. That behavior is not reserved or invalid, so it
cannot be removed by the generic compact-route cleanup.

Decision required before implementation:

- either accept a route-owned full history DTO that preserves the existing full history
  behavior while treating it as a stable public DTO rather than raw normalized-event
  internals
- or add a semantic migration slice that deprecates and removes history `view=full`
  with matching updates to `docs/api-v1.md`, `docs/api-v1-routes.md`,
  `docs/consumer-capabilities.md`, and generated OpenAPI

Until that decision is made, history `view=full` stays compatible and out of scope for
reserved/invalid `view=full` cleanup. `GET /v1/events?view=full` remains a separate
case because the current docs reserve it.

### OpenAPI

OpenAPI must stop being a second hand-authored API implementation.

Target model:

- one route catalog owns method, path, operation id, tags, handler binding, path
  parameters, query parameters, and response family
- schema components are generated from small route/schema definitions or shared DTO
  builders
- tests compare generated route presence, parameter presence, important schema refs,
  and selected golden snippets
- the first OpenAPI slice should preserve output as much as possible before semantic
  cleanup begins

OpenAPI generation is the first high-value deletion target because it removes duplicate
public-shape code without reducing capability scope.

## App usage mapping

A first-party app scan on 2026-05-06 found that the app reference does not literally
call bigname's `/v1/names` route yet. It uses ENSJS, GraphQL indexer queries, and
route-local hooks. Therefore bigname replacement must be measured by capability mapping,
not by preserving existing app URL calls.

Scan scope:

- source archive: `.refs/apps-monorepo-main.zip`
- extracted path: `/tmp/bigname-apps-monorepo/apps-monorepo-main`
- searched file types: TypeScript, TSX, JavaScript, JSX, MJS, Markdown
- search command:
  `rg -n "(/v1/names|v1/names|names\\?|getNamesForAddress|getProfile|getSubnames|useSearch|profile|records|resolver|indexer|GraphQL)" /tmp/bigname-apps-monorepo/apps-monorepo-main -g '*.ts' -g '*.tsx' -g '*.js' -g '*.jsx' -g '*.mjs' -g '*.md'`

Important call-site evidence:

- profile pages use `getProfileQueryOptions` and owner hooks rather than bigname
  `/v1/names?name=...` (`apps/portal/src/features/profile/hooks/useProfile.ts`,
  `apps/portal/src/routes/$name/index.tsx`)
- records and edit-records pages use profile/record hooks rather than bigname compact
  records today (`apps/portal/src/routes/$name/records.tsx`,
  `apps/portal/src/routes/$name/edit-records.tsx`)
- dashboard owned-name views use ENSJS and GraphQL address-owned-name queries
  (`apps/portal/src/features/dashboard/hooks/useV1NamesForAddress.ts`,
  `apps/portal/src/features/dashboard/hooks/useV2NamesForAddress.ts`,
  `apps/manager/src/features/dashboard/components/MyNamesList.tsx`)
- subname pages use ENSJS or GraphQL subname hooks
  (`apps/portal/src/features/profile/hooks/useSubnames.ts`,
  `apps/portal/src/routes/$name/subnames.tsx`)
- search suggestions are assembled from route-local suggestion logic, owner lookups,
  availability checks, and owned-name queries
  (`apps/portal/src/features/dashboard/hooks/useSearchResults.ts`)

| Current app need | bigname target | Planning implication |
| --- | --- | --- |
| profile page loads owner, profile records, registration data | `GET /v1/names/{namespace}/{name}` plus compact records route | exact profile should not be centered on `/v1/names?name=...` |
| records page and edit-records page need resolver address, text keys, coin values, contenthash, ABI-like support | `GET /v1/names/{namespace}/{name}/records` and canonical resolution route when full verified detail is needed | compact records must avoid provenance and cache internals while still exposing app fields |
| dashboard owned names | `GET /v1/names?account=...&relation=...` or `GET /v1/addresses/{address}/names` | keep address collection semantics and pagination |
| dashboard names with roles, subname counts, record counts | `GET /v1/addresses/{address}/names?include=role_summary` | role summaries are an additive expansion, not a separate truth model |
| subnames page | `GET /v1/names/{namespace}/{name}/children` | keep children and counts public |
| search suggestions | `GET /v1/names?prefix=...` or `contains=...` | search is projected-name search only; availability stays out of bigname |
| resolved-address listing | deferred `resolved_address` filter | keep explicit unsupported until a declared equality projection exists |
| name availability and registration flows | out of scope | do not add availability or pricing into bigname |

This scan is planning evidence, not a parity claim. Before claiming replacement or
parity, rerun the app scan against the active app repo and record concrete call-site
mappings in `docs/consumer-capabilities.md`.

## Refactor slices

### Slice 0: Accept or revise this ADR

Change class: semantic planning.

Owner:

- docs and API contract reviewers

Files:

- `docs/adrs/0003-api-surface-flattening-plan.md`

Output:

- accepted target model or revised decisions
- explicit approval for which slices can begin

Exit criteria:

- the ADR status is accepted or the planned first implementation slice is explicitly
  approved as a proposed-plan implementation

### Slice 1: Generate OpenAPI without changing the public contract

Change class: shared-interface if output changes; implementation-only if output is
preserved byte-for-byte except deterministic formatting.

Owner:

- Projections and API

Primary files:

- `apps/api/src/openapi.rs`
- `apps/api/src/openapi/route_operations.rs`
- `apps/api/src/openapi/schemas.rs`
- `apps/api/src/openapi/responses.rs`
- `apps/api/src/routes.rs`
- `apps/api/src/routes/parameters.rs`
- `apps/api/src/routes/parameters/app.rs`
- `apps/api/src/tests/openapi.rs`
- `docs/api-v1.openapi.json`

Plan:

1. Introduce a route-definition table that names each route once.
2. Move route tags, operation ids, path params, query params, and response family into
   the route definition.
3. Generate OpenAPI `paths` from that route definition.
4. Keep schema components initially compatible.
5. Replace per-route hand-built operation code with data-driven route definitions.
6. Keep tests that assert important route/query/schema facts.

Deletion targets:

- duplicated operation builders
- duplicated query parameter lists
- duplicated route descriptions that can be derived from route definitions
- manual OpenAPI path registration separate from `apps/api/src/routes.rs`

Exit criteria:

- generated OpenAPI contains all existing public routes
- existing OpenAPI tests pass
- no public route disappears
- public docs still agree with generated route catalog

Estimated production LOC reduction:

- 400 to 800 lines once the route table replaces operation boilerplate

### Slice 2: Compact route knob cleanup

Change class: semantic/shared-interface.

Docs to update in the same slice:

- `docs/api-v1.md`
- `docs/api-v1-routes.md`
- `docs/consumer-capabilities.md`
- generated `docs/api-v1.openapi.json`

Owner:

- Projections and API

Plan:

1. Make compact route docs say exactly which routes support `view`, `mode`, and
   `meta=full`.
2. Remove `view=full` from docs/OpenAPI where the route currently rejects it or where no
   full shape is documented.
3. Keep parser compatibility by continuing to reject reserved invalid values with the
   existing error shape until a deprecation decision says otherwise.
4. Reclassify `/v1/names?name=...` as a compatibility exact filter, not the conceptual
   exact-profile API.
5. Decide the `/v1/names` no-namespace bridge before changing behavior. Preserve the
   current cross-namespace default until one of the bridges in "Namespace rules" ships.
6. Make omitted namespace default to `ens` only for routes whose current contract allows
   it or after a documented semantic migration. Prefer explicit namespace in app
   replacement mappings.
7. Add explicit tests that compact routes do not leak full-only fields, with route-owned
   identity exceptions for address-name, resource lookup, roles, and resolver-overview
   routes.
8. Keep history `view=full` out of this cleanup until the history full-view decision is
   resolved.

Deletion targets:

- route-local parser branches for reserved full views
- OpenAPI parameters that advertise unsupported values
- response-shaping code that carries full-route metadata into compact paths

Exit criteria:

- compact route docs list only supported knobs
- `view=full` is not advertised for invalid compact routes
- compact responses denylisted fields are absent in tests
- app-facing no-namespace behavior is explicit, compatibility-preserving, and tested
- history full-view behavior is either unchanged or covered by a separate semantic
  migration

Estimated production LOC reduction:

- 200 to 500 lines from parser/docs/schema simplification

### Slice 3: One snapshot-selection service

Change class: implementation-only if semantics are preserved; semantic if behavior
changes.

Owner:

- Projections and API, with Storage and Domain review

Primary files:

- `apps/api/src/support/snapshots.rs`
- exact-name handlers
- resolution handlers
- coverage handlers
- explain handlers
- compact handlers that read exact-name projections
- `crates/storage` snapshot/projection read helpers

Plan:

1. Define one API support type for selected route snapshot:
   - requested namespace/name or inferred namespace/name
   - selected `ChainPositions`
   - `consistency`
   - profile/deployment context
   - route family
2. Make exact-name, coverage, resolution, and explain routes call that selector first.
3. Make storage expose route-ready projection reads for a selected snapshot instead of
   forcing handlers to probe boundaries.
4. Keep multi-chain snapshot behavior explicit and shared.
5. Add tests proving same snapshot selection across exact name, coverage, resolution,
   and explain.

Deletion targets:

- handler-local chain-position parsing
- handler-local exact-name snapshot probing
- duplicated stale/not_found/conflict mapping
- scattered current-vs-selected snapshot checks

Exit criteria:

- exact-name snapshot selection is visible in one support module
- canonical full routes use the same selected snapshot object
- compact current routes do not accidentally opt into historical snapshot behavior

Estimated production LOC reduction:

- 250 to 700 lines after handlers stop assembling the same snapshot pieces

### Slice 4: One record read model

Change class: implementation-only if response shape is preserved; shared-interface if
compact record fields change.

Owner:

- Projections and API, Verified Execution review

Primary files:

- `apps/api/src/support/records.rs`
- `apps/api/src/responses/resolution_verified/readback/record_inventory.rs`
- `apps/api/src/responses/app_facing/records.rs`
- `apps/api/src/handlers/app_facing/records.rs`
- `apps/api/src/handlers/resolution.rs`
- record tests

Plan:

1. Represent record reads with one internal model:
   - resolver identity
   - selector identity
   - declared inventory state
   - declared cache value
   - optional verified value
   - support status
   - value source
2. Build both full resolution response sections and compact record DTOs from that model.
3. Keep `record_inventory` public on canonical/full routes.
4. Keep compact records free of irrelevant inventory/cache metadata.
5. Keep typed unsupported selectors and gaps explicit.
6. Keep text-value hydration as a worker/projection concern, not a compact route sidecar.

Deletion targets:

- duplicate selector parsing
- duplicate record-cache entry construction
- duplicate unsupported-family construction
- compact route snapshot/execution work when verified values are not requested
- tuple-style selector plumbing through unrelated layers

Exit criteria:

- one code path loads record inventory/cache boundary data
- compact route tests prove no full-only record metadata leaks
- full resolution tests prove inventory/cache/verified outputs retain semantics

Estimated production LOC reduction:

- 200 to 500 lines

### Slice 5: Coverage and support-state consolidation

Change class: semantic/shared-interface if names or meanings change.

Owner:

- Projections and API, Storage and Domain review

Docs to update if meanings change:

- `docs/api-v1.md`
- `docs/projections.md`
- `docs/consumer-capabilities.md`

Plan:

1. Define one internal support-state object that can render as:
   - `Coverage`
   - `UnsupportedSummary`
   - selector-level `ResultStatus`
   - compact `meta.unsupported_fields`
2. Keep public coverage where the product needs completeness.
3. Delete route-specific coverage JSON construction when it just restates the shared
   object.
4. Standardize unsupported reason names.

Deletion targets:

- per-route unsupported JSON builders
- coverage/status string duplication
- divergent unsupported reason fields

Exit criteria:

- one support-state vocabulary is used across exact routes, records, resolver overview,
  and compact metadata
- typed unsupported objects stay public where documented

Estimated production LOC reduction:

- 200 to 500 lines

### Slice 6: Adapter replay engine

Change class: implementation-only if normalized events, raw facts, and projections are
unchanged.

Owner:

- Intake and Adapters, Storage and Domain review

Primary files:

- `apps/indexer/src/main/reconciliation/adapter_sync.rs`
- `crates/adapters/src/*`
- raw-log loader modules
- adapter tests and replay tests

Plan:

1. Replace per-adapter scoped raw-log loading with a shared adapter replay engine.
2. Make adapters declare:
   - source family
   - event families
   - block/log filters
   - observation builders
   - normalized event builders
3. Let the shared engine own:
   - source-scope filtering
   - block-hash loading
   - canonical ordering
   - raw fact lookup
   - replay summaries
   - per-source progress
4. Remove tuple-style scoped sync parameters from adapter public APIs.
5. Keep raw facts immutable and rebuildability unchanged.

Deletion targets:

- repeated raw-log loader files
- repeated `(source_family, source_id, from, to)` tuple plumbing
- repeated scoped sync dispatch in `adapter_sync.rs`
- per-adapter block-hash and ordering logic

Exit criteria:

- normalized event output is unchanged for existing fixtures
- adapters no longer hand-roll source-scope replay
- replay summaries are produced by the engine

Estimated production LOC reduction:

- 600 to 1,000 lines

### Slice 7: Audit/explain boundary cleanup

Change class: semantic/shared-interface if public trace or provenance surfaces change.

Owner:

- Verified Execution, Projections and API review

Docs to update if public semantics change:

- `docs/api-v1.md`
- `docs/api-v1-routes.md`
- `docs/execution.md`
- `docs/storage.md`

Plan:

1. Keep execution traces durable.
2. Keep public execution explain on explicit explain routes.
3. Keep worker inspection operational and not part of public `v1`.
4. Remove trace/provenance construction from compact responses.
5. Ensure API handlers do not read raw facts except explicit audit endpoints.
6. Add guard tests or static checks for compact response denylisted fields.

Deletion targets:

- compact provenance shims
- duplicate trace summaries outside explain/readback paths
- route-level raw-fact joins

Exit criteria:

- compact routes cannot expose raw facts, raw normalized events, or execution traces
- explain routes remain attributable and tested

Estimated production LOC reduction:

- 200 to 600 lines unless a larger public trace surface is deleted later

### Slice 8: Test and fixture de-duplication

Change class: implementation-only if test semantics remain unchanged.

Owner:

- Conformance and Fixtures

Plan:

1. Introduce fixture builders for repeated exact-name, record, address-name, and history
   shapes.
2. Convert tests to assert capability outcomes instead of large hand-built JSON setup
   where possible.
3. Keep golden fixtures only where wire contract stability matters.
4. Do not count fixture deletion as product-code de-slopping.

Deletion targets:

- repeated raw fixture insertion boilerplate
- repeated JSON expected payload construction
- duplicated helper structs in test files

Exit criteria:

- product tests stay readable
- production LOC measurements are reported separately from test LOC

Estimated LOC reduction:

- potentially large in tests, but not a production de-slop metric

## Implementation order

Recommended order:

1. Accept or revise this ADR.
2. Generate OpenAPI while preserving the current contract.
3. Clean compact route knobs and namespace defaults in docs and OpenAPI.
4. Move exact-name snapshot selection behind one support service.
5. Finish record read-model consolidation.
6. Consolidate support-state and coverage shaping.
7. Rewrite adapter replay behind one engine.
8. Tighten audit/explain boundaries.
9. De-duplicate tests and fixtures.

Reasoning:

- OpenAPI generation removes duplicate public-shape code with the lowest semantic risk.
- Compact route cleanup removes invalid advertised choices before deeper handler work.
- Snapshot and records unification make later API work simpler.
- Adapter replay is the largest implementation-only deletion, but it is riskier and
  should run after API contract direction is settled.

## Change gate summary

| Slice | Change class | Docs required |
| --- | --- | --- |
| ADR acceptance | semantic planning | this ADR |
| OpenAPI generation, output preserved | implementation-only | none |
| OpenAPI generation, output changed | shared-interface | `docs/api-v1.openapi.json`, route docs if behavior changes |
| Compact knob cleanup | semantic/shared-interface | `docs/api-v1.md`, `docs/api-v1-routes.md`, `docs/consumer-capabilities.md` |
| Snapshot service, behavior preserved | implementation-only | none |
| Snapshot service, behavior changed | semantic | `docs/api-v1.md`, `docs/projections.md` |
| Record read model, shape preserved | implementation-only | none |
| Record read model, shape changed | shared-interface | route docs and OpenAPI |
| Support-state consolidation | semantic/shared-interface if vocabulary changes | `docs/api-v1.md`, `docs/consumer-capabilities.md` |
| Adapter replay engine | implementation-only | none if emitted facts/events are unchanged |
| Audit boundary cleanup | semantic/shared-interface if public fields change | `docs/api-v1.md`, `docs/execution.md`, route docs |

## Compatibility policy

This plan preserves compatibility by default.

Allowed without a deprecation window:

- deleting code that produces identical public behavior
- generating OpenAPI that preserves the same public paths and schemas
- removing internal helper layers
- consolidating storage reads behind equivalent APIs
- moving route-local logic into shared support modules

Requires docs and explicit review:

- changing default namespace behavior
- removing an advertised query value
- changing `view`, `mode`, `meta`, or `include` behavior
- changing support or coverage vocabulary
- moving public trace/provenance fields
- changing error status or error code for existing inputs

Compatibility bridge for deprecated knobs:

- docs stop advertising the accidental knob first
- OpenAPI stops advertising it in the same change
- handlers may keep accepting or keep rejecting the old value with the existing error
  shape until a removal window is chosen
- contract tests pin the bridge behavior

## Measurement

Production de-slopping should be measured separately from tests and generated files.
LOC is a trailing indicator, not the sole goal: when conceptual complexity goes down,
hand-written production LOC should usually go down too. A slice that claims to simplify
the system while increasing production LOC needs an explicit follow-up deletion plan.

Track these metrics before and after each slice:

- hand-written production Rust LOC
- hand-written production Rust file count
- files over the advisory Rust size threshold
- number of concepts/vocabulary terms used for one capability across API, storage,
  execution, and adapters
- number of files a maintainer must inspect to understand one capability end to end
- number of public route definitions
- number of hand-authored OpenAPI operation/schema builders
- number of duplicate parser functions for `view`, `mode`, `meta`, namespace, and
  selector handling
- number of adapter raw-log loader implementations

Expected production LOC reduction while preserving compatibility:

| Area | Expected reduction |
| --- | --- |
| OpenAPI generation | 400 to 800 |
| Compact route knob cleanup | 200 to 500 |
| Snapshot selection and projection-read service | 250 to 700 |
| Record read model | 200 to 500 |
| Support-state consolidation | 200 to 500 |
| Adapter replay engine | 600 to 1,000 |
| Audit/explain cleanup | 200 to 600 |

Expected total:

- realistic compatibility-preserving reduction: 1,800 to 4,000 production LOC
- larger reductions require public contract cuts, especially removing route families or
  broad provenance/explain surfaces

The goal is reduced complexity, reduced cognitive load, and reduced hand-written
production LOC, in that order. A slice counts as successful when it reduces the number
of public-ish concepts a maintainer must understand to change one capability. LOC should
fall as a natural consequence of that simplification; if it does not, the slice needs a
clear explanation and a follow-up deletion target.

## Tests and acceptance criteria

Every implementation slice needs focused tests.

Required API tests:

- OpenAPI route/parameter/schema assertions
- compact route denylist tests for full-only fields
- namespace default and inference tests
- exact-name snapshot consistency tests across exact name, coverage, resolution, and
  explain
- record inventory/cache/verified response parity tests
- unsupported section tests proving typed objects stay present
- unsupported filter tests proving non-2xx errors stay explicit

Required storage/projection tests:

- selected snapshot read eligibility
- stale vs not_found vs conflict mapping
- record boundary lookup through one service
- support-state rendering
- address-name count parity with `GET /v1/names`

Required adapter/replay tests:

- fixture-level normalized event parity before and after replay-engine migration
- source-scope filtering
- block-hash/canonical ordering
- replay summary construction
- reorg repair compatibility

Required checks:

- `cargo fmt --check`
- `cargo check -p bigname-api`
- package-specific tests for touched crates
- `scripts/check-rust-file-size`
- production LOC measurement before and after substantial slices

## Consequences

Positive:

- fewer public-ish route shapes and helper layers
- one owner for snapshot selection
- one owner for record inventory/cache/verified read assembly
- one owner for adapter replay mechanics
- compact app-facing routes stay small and app-oriented
- OpenAPI stops mirroring handler semantics by hand
- raw facts and traces stay behind explicit audit/explain boundaries
- future app replacement work can map capabilities instead of copying legacy schemas

Tradeoffs:

- the first slices are docs and public-contract sensitive
- compatibility preservation limits immediate LOC deletion
- OpenAPI generation can be risky if it changes schema details accidentally
- adapter replay unification is a larger rewrite and requires strong fixture parity tests
- namespace default changes require careful client communication if any client relied on
  cross-namespace defaults

New failure modes:

- generated OpenAPI may hide a route parameter if route definitions are incomplete
- a too-small compact DTO allowlist may omit a field the app actually needs
- a shared snapshot service bug can affect several routes at once
- a shared replay engine bug can affect multiple adapters at once

Mitigations:

- preserve-output OpenAPI tests first
- compact denylist plus app capability tests
- route-by-route snapshot parity tests
- adapter fixture parity before deleting old loaders
- stage high-risk rewrites behind old/new comparison paths when practical

## Rollout

This is a doc-first plan.

Rollout sequence:

1. Review and accept or revise this ADR.
2. Land OpenAPI generation without semantic changes.
3. Land compact route docs and OpenAPI cleanup for unsupported/reserved knobs.
4. Land implementation-only unifications behind existing public behavior.
5. Run app capability mapping against the current first-party app before claiming
   replacement.
6. Only after compatibility bridges are documented, decide whether to remove any old
   parser behavior.

Workstream ownership follows `docs/internal/workstreams.md`:

- Projections and API owns route handlers, responses, OpenAPI, compact DTOs, and API
  contract tests.
- Storage and Domain reviews snapshot and projection-read services.
- Intake and Adapters owns adapter replay engine work.
- Verified Execution reviews mode, verified readback, and explain/audit boundaries.
- Conformance and Fixtures owns fixture parity and consumer capability mapping tests.

## Alternatives considered

### Delete compact routes and keep only full routes

This would remove a lot of API surface, but it would make app replacement worse. The
app needs small current UI reads, not full audit envelopes everywhere. Rejected because
it cuts product ergonomics instead of accidental duplication.

### Keep `/v1/names` as the universal app API

This would make one route look simpler at the edge, but it would force exact profile,
search, address collections, suggestions, record summaries, namespace behavior, and
pagination into one overloaded handler. Rejected because it moves complexity into one
large route instead of flattening concepts.

### Remove `consistency`

This would reduce snapshot-selection vocabulary, but it would also remove an explicit
finality floor that matters for replay, multi-chain reads, and stale/conflict behavior.
Rejected for now because the expected simplification is not large enough.

### Expose normalized events as the history API

This would avoid route-owned compact event DTOs, but it would make internal replay
event shape a public contract. Rejected because history should be stable for consumers
while normalized events remain an implementation/rebuild layer.

### Treat raw facts as public audit output

This would make rebuildability visible, but it would couple public API shape to intake
storage. Rejected because auditability is better served by explicit explain and worker
inspection surfaces.

### Hand-author OpenAPI forever

This keeps docs easy to edit locally, but it preserves one of the clearest duplicate
public-shape implementations in the codebase. Rejected because generated OpenAPI can
preserve reviewability while deleting boilerplate.

### Rewrite adapters before API cleanup

This would produce a larger immediate production LOC decrease, but it does not settle
the public model. Rejected as the first step because API semantics should be boring
before the largest implementation-only rewrite begins.

## Upstream anchors

This ADR does not introduce new claims about ENSv1, ENSv2, or Basenames contract
behavior. It reshapes bigname API ownership and implementation order only. Slices that
add or change upstream behavior claims must cite `.refs/` sources in the docs they
touch.

## References

- `docs/api-v1.md`
- `docs/api-v1-routes.md`
- `docs/consumer-capabilities.md`
- `docs/projections.md`
- `docs/execution.md`
- `docs/storage.md`
- `docs/internal/workstreams.md`
- `docs/adrs/0001-stack.md`
- `docs/adrs/0002-surface-resource-identity.md`
- `docs/adrs/0004-conceptual-deduplication-gate.md`
