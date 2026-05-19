# ADR 0004: Conceptual Deduplication Gate

Status: Accepted
Date: 2026-05-06

## Context

ADR 0003 set out to flatten the API surface and related implementation paths by
deleting parallel representations of the same subsystem concepts. The intended goal
was not cleaner naming by itself, and not helper extraction by itself. The goal was
lower cognitive load, fewer public-ish concepts, fewer files required to understand
one capability, and less hand-written production code.

The first ADR 0003 implementation pass exposed a failure mode in that plan: a slice
can centralize some metadata, introduce shared helpers, preserve compatibility, and
still leave the old conceptual owners in place. That kind of change can be useful
preparation, but it is not the same as de-slopping the subsystem. It can even increase
production LOC while making the code look more organized.

Examples of this failure mode:

- a route catalog exists, but OpenAPI operation registration and query-parameter
  construction still maintain route semantics in parallel
- an ABI decoding helper exists, but each adapter still owns event-family branching,
  source-scope loading, replay selection, observation construction, and persistence
  summaries separately
- a snapshot request type exists, but handlers and readback code still assemble
  selected-snapshot behavior independently
- a normalized-event persistence helper exists, but scoped replay and raw-log loading
  remain per-adapter concepts

ADR 0003 remains the product and compatibility plan. This ADR adds the completion
gate: an ADR 0003 slice is not done until it deletes or collapses the duplicated
concepts that made the subsystem hard to understand.

## Decision

Adopt a conceptual deduplication gate for ADR 0003 and later de-slopping work.

A slice that claims to simplify a subsystem must satisfy at least one of these
conditions:

- delete a duplicate conceptual owner, such as a second route-shape catalog, second
  snapshot selector, second support-state vocabulary, or second adapter replay path
- collapse two or more public-ish vocabulary terms into one named model, with the old
  term removed, deprecated, or isolated behind a compatibility boundary
- remove a route-local, adapter-local, or schema-local implementation path because a
  shared owner now performs that behavior
- make a capability understandable through fewer files and fewer independent branches
  than before, and show the specific files or branches that disappeared

Helper extraction alone does not satisfy the gate. A new helper counts only when it
also removes the old call-site concept or when it is paired with an immediate follow-up
deletion slice. If the old concept still exists at every call site, the change is
scaffolding, not conceptual deduplication.

Compatibility preservation remains required, but compatibility shims must be explicit:

- name the shim and the old behavior it preserves
- state whether the shim is temporary or accepted permanent compatibility
- keep the shim at the boundary, not in the internal target model
- measure the target model separately from the shim

Production LOC is not the only metric, but it is a sanity check. When a slice claims to
reduce complexity, hand-written production LOC should normally decrease. A slice that
increases production LOC must state:

- the old duplicated owner that still needs to be deleted
- the follow-up slice that deletes it
- the expected production LOC and file-count reduction
- why the temporary increase reduces risk enough to justify the extra code

If that follow-up deletion does not happen, the scaffolding should be considered debt,
not completed ADR 0003 work.

## Completion bars by subsystem

### OpenAPI and route catalog

Not enough:

- a central route list exists while OpenAPI keeps independent route registration,
  parameter construction, and response-family mapping
- generated OpenAPI exists as a second model that must be kept in sync by hand

Done means:

- one route definition owns method, path, operation id, tags, handler binding, path
  parameters, query parameters, and response family
- OpenAPI path generation consumes that route definition instead of re-stating it in a
  separate switchboard
- compact/full/audit route-family differences are visible in the route definition, not
  scattered across parameter helpers and response builders
- the hand-written OpenAPI operation/schema code is smaller after generated output and
  tests are accounted for

### Compact and canonical API models

Not enough:

- compact DTOs have denylist tests while full-route fields still leak through shared
  metadata paths
- `view`, `mode`, `meta`, namespace, and include behavior are parsed in several route
  families under different local policies

Done means:

- each public route family has one route-owned shape rule
- compact app-facing routes use allowlisted DTOs and do not carry irrelevant full,
  audit, projection, or replay data
- compatibility behavior is isolated at request parsing or response adaptation
  boundaries
- old query knobs are removed from docs/OpenAPI when they are unsupported, or named as
  explicit compatibility

### Snapshot selection and projection reads

Not enough:

- handlers create a shared request object but still perform boundary probing, stale
  mapping, or exact-snapshot selection locally

Done means:

- one snapshot-selection service returns the selected snapshot and route-ready
  projection reads
- exact-name, coverage, resolution, compact-record, and explain paths use that service
  instead of re-assembling snapshot behavior
- stale, not-found, conflict, and unsupported mappings are shared vocabulary, not
  handler-local prose

### Record read model

Not enough:

- compact records and verified/full readback call one helper but still build selector,
  cache, inventory, unsupported, and value-source objects independently

Done means:

- one internal record read model represents selector identity, resolver identity,
  inventory state, cache value, optional verified value, support status, and value
  source
- full and compact responses render from that model with route-specific projection
  rules
- tuple-style selector plumbing and duplicate unsupported-family builders disappear

### Support and coverage states

Not enough:

- unsupported objects remain typed but every route keeps local status strings and JSON
  construction

Done means:

- exact routes, compact routes, record inventory, resolver overview, and coverage use
  one support-state vocabulary
- public DTO differences are render decisions, not separate support concepts
- empty, unknown, unsupported, stale, and partial states have one owner

### Adapter replay

Not enough:

- ABI helpers decode events cleanly while each adapter still hand-rolls source-scope
  filtering, raw-log loading, block-hash ordering, observation construction, and replay
  summaries
- normalized-event persistence is shared while replay selection remains duplicated

Done means:

- adapters declare source families, event families, filters, observation builders, and
  normalized event builders
- one shared replay engine owns source-scope filtering, raw-log loading, canonical
  ordering, raw-fact lookup, event identity, persistence, and replay summaries
- tuple-style scoped sync parameters disappear from adapter public APIs
- fixture-level normalized event output is unchanged

### Audit and explain boundaries

Not enough:

- compact response tests deny raw fields while compact builders still know about trace,
  provenance, raw fact, or replay internals

Done means:

- raw facts, raw normalized events, execution traces, and provenance live only behind
  explicit audit, explain, or worker inspection surfaces
- compact builders cannot accidentally render those internals because the data is not
  present on the compact path
- public explain DTOs are route-owned and do not expose internal storage rows as the
  contract

## Measurement

Each substantial de-slopping slice must report before and after values for:

- hand-written production Rust LOC
- hand-written production Rust file count
- files over the advisory Rust size threshold
- number of separate owners for the capability being changed
- number of route/query/parser/schema maps that must be updated for one public change
- number of adapter replay/raw-log loading paths, for adapter work

LOC should be reported separately for production code, tests, generated files, and
docs. Test and fixture deletion can be valuable, but it does not prove production
conceptual simplification.

A slice may still be accepted with a temporary production LOC increase, but the
increase must be treated as an explicit debt item until the paired deletion lands.

## Rollout

This ADR is a retroactive gate for work claiming to implement ADR 0003.

Future ADR 0003 implementation slices should start with a short gate summary:

- the duplicated concept being deleted
- the old owner and the new owner
- compatibility shims that remain
- expected deletion target
- before/after production LOC measurement

The first follow-up work should re-evaluate any ADR 0003 scaffolding that centralized
behavior without deleting the old owner. The highest-value candidates are:

- route catalog and OpenAPI parameter generation
- adapter replay and source-scope loading
- snapshot selection and route-ready projection reads
- record inventory, cache, and verified value readback

If a future slice changes route semantics, compatibility behavior, support vocabulary,
or public payload fields, it must update `docs/api-v1.md`,
`docs/api-v1-routes.md`, `docs/consumer-capabilities.md`, and generated OpenAPI in the
same change.

## Consequences

Positive:

- prevents helper extraction from being mistaken for subsystem simplification
- makes LOC movement a useful warning signal without turning LOC into the only goal
- forces compatibility shims to stay at boundaries instead of becoming the new model
- gives reviewers a concrete way to reject complexity that merely moved around

Tradeoffs:

- some compatibility-preserving preparation slices will no longer count as completed
  de-slopping work
- large subsystem rewrites need clearer measurement and follow-up discipline
- reviewers may block abstraction-heavy patches that look cleaner locally but preserve
  the old global model

New failure mode:

- teams may chase deletion too aggressively and accidentally hide necessary product
  distinctions

Mitigation:

- the gate deletes duplicated concepts only after the docs name which distinctions are
  intentional product behavior
- public compatibility remains protected by ADR 0003, public API docs, and the normal
  change gate

## Alternatives considered

### Treat ADR 0003 as complete because the planned slices landed

Rejected. A slice can land the named artifact from ADR 0003 while preserving the old
conceptual owner. That makes implementation status look better than maintainer
experience or LOC movement.

### Measure only production LOC

Rejected. LOC is a useful warning signal, but a small increase can be justified when it
enables a safe compatibility bridge or parity harness. The important rule is that such
increases remain explicit debt until the paired deletion lands.

### Continue accepting helper-only simplification

Rejected. Helper-only changes can improve local readability, but they do not complete
ADR 0003 unless they remove the duplicated subsystem concept at the call sites.

## Upstream anchors

This ADR does not introduce new claims about ENSv1, ENSv2, or Basenames contract
behavior. It defines bigname implementation and review criteria only.

## References

- `docs/adrs/0003-api-surface-flattening-plan.md`
- `docs/api-v1.md`
- `docs/api-v1-routes.md`
- `docs/consumer-capabilities.md`
- `docs/internal/workstreams.md`
