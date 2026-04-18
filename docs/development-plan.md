# Development Plan

Status: Phase 0 baseline

Normative scope: milestone goals, phase deliverables, exit criteria, and the companion docs referenced here are normative for repository bootstrap and the first vertical slices.

This document translates the revised architecture into an implementation sequence.

It keeps early work ordered around the decisions that are hardest to change later:

- public semantics
- surface / resource identity
- manifest model
- raw fact model
- replay model
- verified execution model

The plan assumes a modular monolith for the first production version.
Parallel execution and ownership boundaries live in [Parallel Workstreams](./workstreams.md).

Implementation detail should stay subordinate to one rule: build the native `v1` contract we actually want, not a disguised legacy indexer.

---

## 1. Principles

- freeze public semantics before optimizing internals
- freeze collection semantics before building projections
- freeze surface / resource identity before building APIs
- build live ingestion and backfill through the same pipeline
- keep raw facts immutable and projections disposable
- treat verified execution as a first-class subsystem from the start
- measure replacement by consumer capability coverage, not legacy API parity
- prefer cohesive end-to-end slices over broad scaffolding with undefined semantics
- freeze cross-workstream interfaces before parallel implementation starts

---

## 2. Delivery Milestones

The plan is organized around four milestone types:

### Milestone A: Truth Core

The system can ingest, normalize, bind public surfaces to backing resources, and replay deterministically.

### Milestone B: Declared-State Product Slice

The system can serve native `v1` reads for exact names, address collections, child collections, roles, and history from declared state.

### Milestone C: Verified Product Slice

The system can serve verified resolution and primary-name answers with full provenance.

### Milestone D: Consumer Replacement Slice

The first-party apps can switch to the native `v1` contract without relying on the existing ENSv1/v2 indexer API shape.

Implementation should use the workstream overlay in `docs/workstreams.md` once the Phase 0 docs and ADRs are frozen.

---

## 3. Phase 0: Scope Freeze

### Goal

Turn the revised architecture into implementation-ready semantics for the first build.

### Deliverables

- versioned `v1` resource definitions
- explicit compatibility policy: no legacy subgraph / GraphQL parity requirement
- query semantics for `namespace`, `at`, `chain_positions`, `consistency`, and `mode`
- frozen coverage and exhaustiveness definitions
- frozen canonicality rules
- frozen namespace assignment rules
- frozen `NameSurface` and `SurfaceBinding` taxonomy
- frozen collection semantics for:
  - exact name lookup
  - address → names
  - address → names with role summary
  - name → children
  - history scopes
  - resolver overview
- frozen `RecordInventory` semantics
- frozen preimage observation model
- frozen chain-intake contract for hash-first lineage, checkpoint promotion, and reorg handling
- frozen consumer capability matrix in `docs/consumer-capabilities.md`
- frozen workstream boundaries and ownership for shared crates
- initial ADRs for stack and repo layout

### Exit Criteria

- no unresolved ambiguity in the first `Name`, `Address`, `Resolution`, `PrimaryName`, and `Resolver` reads
- no unresolved ambiguity in surface-vs-resource behavior
- no unresolved ambiguity about what belongs to `ens` vs `basenames`
- no unresolved ambiguity in what “replacement” means for first-party consumers

---

## 4. Phase 1: Repository Skeleton

### Goal

Create the implementation structure without committing to too much behavior yet.

### Deliverables

- Rust workspace
- initial API, indexer, and worker binaries
- shared crates for domain, storage, manifests, adapters, and execution
- local development environment
- migration framework
- baseline observability and config loading
- CI for formatting, linting, tests, and migrations

### Exit Criteria

- the repo can boot an API process, an indexer process, and a worker process
- local development can create the database schema and run tests
- docs and ADR folders are wired into the repo structure
- repo ownership matches `docs/workstreams.md`

---

## 5. Phase 2: Storage Foundation

### Goal

Make the raw-fact, identity, and replay model concrete.

### Deliverables

- chain-lineage schema
- raw block, transaction, receipt, and log tables
- manifest tables
- source-discovery tables
- `NameSurface` / `SurfaceBinding` / `resource_id` schema
- normalized-event tables
- preimage observation tables
- canonicality model
- provenance references
- execution-trace schema
- replay checkpoints

### Exit Criteria

- one chain segment can be ingested into raw facts and replayed into normalized events
- a public name surface can be bound to a backing resource in storage
- orphaned facts can be marked and excluded from canonical projections
- execution traces can be persisted even before full verified execution is enabled

---

## 6. Phase 3: Intake, Manifests, And Discovery

### Goal

Ingest Ethereum L1 and Base and build the canonical source graph.

### Deliverables

- Ethereum intake
- Base intake
- code-hash capture
- manifest bootstrapping
- contract discovery graph
- proxy / implementation tracking
- discovery-edge provenance
- watch-list expansion from manifests and discovery edges

### Exit Criteria

- the system can follow both chains forward
- every watched contract is explainable through a manifest or discovery edge
- manifest changes propagate into normalized-event context

---

## 7. Phase 4: ENSv1 Adapter Slice

### Goal

Translate ENSv1 raw facts into stable internal events and serve the first useful declared-state answers.

### Deliverables

- ENSv1 registry adapter
- ENS `.eth` registrar adapter
- Name Wrapper adapter
- resolver adapter for supported record families
- reverse / primary adapter
- preimage observation from ENSv1 name-bearing events
- first normalized-event rules for:
  - registration
  - renewal
  - expiry
  - wrapper ownership / expiry / fuse-relevant changes
  - resolver changes
  - record changes
  - reverse changes
- first declared projections:
  - name snapshot
  - address names
  - record inventory
  - history
  - primary claim snapshot

### Exit Criteria

- a minimal ENSv1 `.eth` name can be looked up by surface name
- wrapped and unwrapped ENSv1 names can be represented without identity confusion
- declared child enumeration works for direct declared children in supported ENSv1 cases
- history reconstructs from normalized events without manual patching

---

## 8. Phase 5: ENSv2 Adapter Slice

### Goal

Add ENSv2-native discovery, resources, permissions, and surface-binding behavior.

### Deliverables

- RootRegistry / ETHRegistry adapter
- UserRegistry adapter
- ETHRegistrar adapter
- PermissionedResolver adapter
- ENSv2 preimage observation rules
- ENSv2 resource / token-lineage handling
- `TokenRegenerated` handling
- `TokenResourceLinked` handling
- `SubregistryChanged` / `ParentChanged` graph expansion
- alias and wildcard topology events
- role and permissions projections
- resolver index projection
- surface-binding projection for:
  - declared registry paths
  - linked subregistries
  - alias-derived surfaces where representable
  - observed wildcard surfaces

### Exit Criteria

- ENSv2 names can be indexed without treating token IDs as stable identity
- one backing resource can surface under multiple public names
- linked-subregistry behavior is represented explicitly
- resolver-centric and resource-centric permissions reads work for the first supported slice

---

## 9. Phase 6: Declared-State API Slice

### Goal

Expose the first stable native `v1` surface for declared-state product reads.

### Deliverables

- `Name` read
- `Address` read
- `Resolver` read
- `History` read
- `Permissions` read
- declared `Resolution` read with topology + record inventory + record cache
- explain views for:
  - `GET /v1/explain/names/{namespace}/{name}/surface-binding`
  - `GET /v1/explain/names/{namespace}/{name}/authority-control`
  - `GET /v1/coverage/{namespace}/{name}`
- shipped shared history routes plus exact-name `declared_state.history.{surface_head,resource_head}` satisfy the Phase 6 history-explain deliverable; no separate exact-name history-explain route is introduced in this phase
- machine-readable contract output frozen to the publication location `docs/api-v1.openapi.json`; when generated, it covers only the routes currently shipped by `apps/api/src/main.rs`
- replay-stable `cursor` / `page_size` plus frozen default sorts for shipped collection reads
- resolver overview alias summary sourced from current resolver-linked bindings

### Minimum supported reads

- exact name lookup
- `Address.names`
- `Address.names` with `include=role_summary`
- name → children
- name / resource → role holders
- name history
- resource history
- address history
- resolver overview

`Address.names` with `include=role_summary` stays the same declared-state address collection with an additive expansion, and `Address.history` stays the address-derived variant of the shared normalized-event history contract rather than a second route family.

### Exit Criteria

- first-party app data requirements that rely only on declared state can be served from native `v1`
- every response includes provenance, coverage, and chain-position context
- collection semantics match the frozen phase 0 definitions

### Current contract-freeze status

- `phase6-surface-binding-authority-explain-contract-clarification`: `GET /v1/explain/names/{namespace}/{name}/surface-binding` and `GET /v1/explain/names/{namespace}/{name}/authority-control` are frozen in the shared docs and shipped by `apps/api/src/main.rs` as exact-name-scoped declared-state explain routes over existing truth families
- `phase6-shipped-read-pagination`: `cursor` and `page_size` are frozen for `GET /v1/addresses/{address}/names`, `GET /v1/names/{namespace}/{name}/children`, `GET /v1/resources/{resource_id}/permissions`, `GET /v1/history/addresses/{address}`, `GET /v1/history/names/{namespace}/{name}`, and `GET /v1/history/resources/{resource_id}`; no other shipped route honors those query parameters in the initial contract
- `phase6-openapi-contract-output-clarification`: machine-readable contract output is frozen to the publication location `docs/api-v1.openapi.json`; when generated, it covers only the routes currently shipped by `apps/api/src/main.rs`, including the shipped `GET /v1/primary-names/{address}` route
- `phase6-resolver-overview-alias-summary-support`: supported `declared_state.aliases` on `GET /v1/resolvers/{chain_id}/{resolver_address}` is shipped as the `binding_kind=resolver_alias_path` subset of current resolver-linked bindings with the shared `{status, count, items}` summary envelope, including `count=0` with `items=[]` when no current alias binding exists

---

## 10. Phase 7: Verified Execution Slice

### Goal

Add the execution plane that turns declared state into verified answers.

### Deliverables

- ENS Universal Resolver execution
- alias-aware execution
- wildcard handling
- CCIP-Read support
- Basenames compatibility execution scaffolding
- primary-name verification engine
- execution trace persistence
- cache and invalidation rules
- `Resolution` reads in `verified` and `both` modes
- `PrimaryName` reads in `verified` and `both` modes
- `GET /v1/explain/resolutions/{namespace}/{name}/execution` explain view over persisted resolution execution traces

### Exit Criteria

- explicit record reads work in `verified` mode for the initial supported slice
- primary-name reads distinguish claim from verified result
- every verified answer produces traceable provenance and execution traces
- relevant topology or record changes invalidate cached verified answers deterministically

### Current contract-freeze and shipped progress

- `phase7-resolution-execution-explain-contract-clarification`: `GET /v1/explain/resolutions/{namespace}/{name}/execution` is shipped in the shared docs and machine-readable contract as a verified-state explain route for the same current exact surface and explicit selector set as `GET /v1/resolutions/{namespace}/{name}`; it reads persisted execution traces only, stays route-local instead of becoming a raw trace dump, and the current published handler contract exposes path parameters plus required `records` only
- `phase7-verified-resolution-support-boundary-clarification`: the shipped mixed `GET /v1/resolutions/{namespace}/{name}` contract and shipped `GET /v1/explain/resolutions/{namespace}/{name}/execution` envelope stay stable, but public verified support is currently only ENS exact-surface direct-path reads, using the same declared-topology support check as the route contract; ENS non-direct paths and all Basenames verified reads remain explicit unsupported until a later doc-first contract update broadens the slice after Base authority plus L1 compatibility transport are wired
- `phase7-primary-name-route-envelope-bootstrap`: `GET /v1/primary-names/{address}` is shipped as a head-only bootstrap on the already frozen mixed declared+verified route contract and enters machine-readable publication scope; it currently honors `namespace`, `coin_type`, and optional `mode`, and tuple-present reads may still surface explicit `unsupported` result objects while richer claim/verified payloads and broader Phase 7 verified execution deliverables remain pending
- `phase7-verified-primary-support-boundary-clarification`: the first additive ENS `verified_primary_name` slice is frozen to persisted/readback verified results for the same `(address, namespace, coin_type)` tuple as `GET /v1/primary-names/{address}`; it uses stable execution identity `request_type=verified_primary_name` and request-key identity `{namespace}:{normalized_address}:{coin_type}`, may use only `primary_names_current(address, coin_type, namespace)` as the claim-side lookup / invalidation anchor, keeps top-level provenance separate from claim-local and verification-local section provenance, leaves route-level primary-name coverage bootstrap `unsupported`, and does not admit richer ENS tuple-present claimed or verified payloads without a later doc-first contract update

---

## 11. Phase 8: Basenames Slice

### Goal

Add Basenames as a first-class public namespace, using the same architecture rather than a sidecar model.

### Deliverables

- Base-side registry / registrar / resolver adapters
- Basenames primary-name support
- Base authority projections
- L1 compatibility transport context in resolution
- Basenames-specific control facets:
  - token ownership
  - management rights
  - address-resolution target
- Basenames-specific coverage and explain rules
- Basenames history support

### Exit Criteria

- `basenames` reads are served through the same native `v1` contract
- Base authority and L1 transport are clearly separated in provenance
- Basenames transfer scenarios map correctly onto `ControlVector`

---

## 12. Phase 9: Reorg, Replay, And Backfill

### Goal

Harden the system around historical correctness.

### Deliverables

- fork detection
- canonical invalidation
- deterministic replay tooling
- historical backfill tooling
- execution-cache invalidation on reorg
- dispute and inspection tooling
- backfill jobs for:
  - ENSv1
  - wrapper / migration history
  - ENSv2
  - Basenames
- consumer capability conformance jobs against backfilled data

### Exit Criteria

- a simulated reorg rebuilds correct current answers
- historical backfill reuses the same adapter and projection path as live ingestion
- replay determinism holds from raw facts and from normalized events

---

## 13. Phase 10: Consumer Replacement And Hardening

### Goal

Replace the current indexer dependencies in first-party apps with the native `v1` contract and harden operations.

### Deliverables

- consumer adapter / SDK layer for the apps monorepo
- app-by-app migration plan
- capability-by-capability cutover checklist
- parity tests at the capability level, not the schema level
- dashboards and SLOs
- live audit jobs
- chaos / reorg drills
- manifest drift and proxy-upgrade alerting
- release and rollback runbooks

### Exit Criteria

- first-party apps no longer require the existing ENSv1/v2 indexer API surface
- remaining gaps are explicitly tracked as unsupported native `v1` capabilities
- operations can detect drift before it becomes user-visible

---

## 14. Recommended First Vertical Slice

If we optimize for learning rather than scaffolding, the first complete slice should be:

1. exact ENS name lookup for a small canonical subset
2. surface binding + backing resource summary
3. declared authority and control
4. declared resolution topology
5. declared record inventory
6. verified address resolution
7. claimed vs verified primary-name answer
8. provenance and coverage in every response

This validates the central architecture before wider resolver-family expansion or long-tail backfills.

---

## 15. Recommended Second Vertical Slice

The second slice should prove that the architecture handles the parts the legacy model cannot express cleanly:

1. ENSv2 dynamic subregistries
2. linked-subregistry surfaces
3. token regeneration with stable resource identity
4. resource-centric permissions
5. resolver overview
6. role history
7. alias-aware verified resolution

If this slice works cleanly, the architecture is confirmed.

---

## 16. Consumer Capability Migration Plan

The apps monorepo should be migrated by capability group, not by endpoint mirroring.

### Group 1: Name profile and records

- exact name profile
- registration / expiry summary
- record inventory
- verified record reads

### Group 2: Collections

- address → names
- address → names with role summary
- name → children
- child counts

### Group 3: History and permissions

- name history
- address history
- role holders
- role history
- resolver overview

### Group 4: Primary names

- claimed primary
- verified primary
- failure states

Each group should have:

- contract tests
- app integration tests
- explain / provenance checks
- rollout and rollback criteria

---

## 17. Test Strategy

Tests must operate at four layers:

- raw facts
- normalized events
- projections / API
- verified execution traces

### Required suites

- fixture-based protocol tests
- forked-chain integration tests
- replay determinism tests
- reorg tests
- manifest drift tests
- consumer capability conformance tests
- performance tests for collection reads
- execution cache invalidation tests

---

## 18. Supersession Criteria

The system supersedes existing indexers for our stack when all of the following are true:

- the first-party apps use native `v1` reads for all required naming capabilities
- exact lookup, collections, permissions, history, resolution, and primary-name answers are served with explicit coverage / provenance
- ENSv2-native features are represented without legacy identity distortions
- Basenames are served as a first-class public namespace
- operational replay, reorg handling, and drift detection are production-hardened

This is a capability replacement, not a schema impersonation.

---

## 19. Companion Docs Required Before Coding Starts

These docs should exist and be treated as the interface baseline before repository bootstrap turns into parallel implementation:

- `docs/api-v1.md` for exact response shapes
- `docs/chain-intake.md` for lineage, fetch, and reconciliation rules
- `docs/storage.md` for schema strategy
- `docs/manifests.md` for manifest structure and capability flags
- `docs/projections.md` for collection semantics and indexes
- `docs/execution.md` for verified resolution and primary-name verification
- `docs/consumer-capabilities.md` for the checked-in consumer capability baseline
- `docs/workstreams.md` for parallel delivery boundaries and ownership
- `docs/adrs/0001-stack.md` for the implementation stack decision
- `docs/adrs/0002-surface-resource-identity.md` for the surface / resource split
