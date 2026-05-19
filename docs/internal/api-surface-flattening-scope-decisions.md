# API Surface Flattening Scope Decisions

Working sheet behind ADR 0003. These answers informed the API surface flattening plan; the accepted contract remains the checked-in public docs and ADRs.

Legend:

- `Yes` means keep or adopt the capability.
- `No` means delete, hide, or make it internal.
- `Unsure` means decide before implementation.

## Core Product Shape

### 1. Should bigname expose only current/latest state?

- [ ] Yes
  - Keeps/adopts: delete public `at`, `chain_positions`, `consistency`; keep only latest checkpoint semantics; large snapshot/test savings.
- [X] No
  - Deletes/narrows: keep time-travel/current-at-snapshot API; snapshot selection remains a core product surface.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

Time-travel queries are good, what is `consistency` though?


### 2. Should bigname expose historical events?

- [X] Yes
  - Keeps/adopts: keep `/history/*` and `/events`; normalized events stay public-facing.
- [ ] No
  - Deletes/narrows: normalized events become internal replay data only; delete public history/event handlers and docs.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 3. Should bigname expose audit/provenance data publicly?

- [ ] Yes
  - Keeps/adopts: keep route provenance and derivation detail in responses.
- [ ] No
  - Deletes/narrows: move provenance to internal/audit tooling; public DTOs shrink substantially.
- [X] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

What audit/provenance data is there? What does it do?


### 4. Should bigname expose coverage/exhaustiveness publicly?

- [ ] Yes
  - Keeps/adopts: keep coverage/exhaustiveness/support taxonomy as public contract.
- [ ] No
  - Deletes/narrows: collapse to ordinary nulls/errors/availability; removes lots of support-shaping code.
- [X] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

What coverage/exhaustiveness policies do we have?


### 5. Should bigname have exactly one public response envelope?

- [ ] Yes
  - Keeps/adopts: pick one envelope for all public routes; deletes full/compact dual response paths.
- [X] No
  - Deletes/narrows: continue carrying compact app DTOs plus full/audit envelopes.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


## Records

### 6. Should records be indexed-only?

- [ ] Yes
  - Keeps/adopts: records come only from indexed projections/cache; no API execution needed for values.
- [X] No
  - Deletes/narrows: keep verified/live/persisted execution as part of records product.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

What is the persisted execution used for? Where?


### 7. Should the public API return record inventory?

- [X] Yes
  - Keeps/adopts: record selector inventory remains visible to clients.
- [ ] No
  - Deletes/narrows: inventory becomes internal; public API returns values/status only.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 8. Should the public API return record cache metadata?

- [ ] Yes
  - Keeps/adopts: cache boundaries/status/provenance stay visible.
- [ ] No
  - Deletes/narrows: cache metadata becomes internal; public record output is value-oriented.
- [X] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

What is in the record cache metadata?


### 9. Should unknown, unobserved, and unsupported record selectors remain distinct publicly?

- [ ] Yes
  - Keeps/adopts: keep precise `not_found` vs `pending` vs `unsupported` semantics.
- [ ] No
  - Deletes/narrows: collapse these to missing/unavailable; simpler client and server model.
- [X] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

What does this mean?

### 10. Should text-value hydration exist?

- [X] Yes
  - Keeps/adopts: keep or fold hydration into rebuild; supports filling observed ENSv1 text values.
- [ ] No
  - Deletes/narrows: delete hydration sidecar and CLI knobs; record cache only reflects indexed values.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


## Execution

### 11. Should API requests ever call RPC providers live?

- [X] Yes
  - Keeps/adopts: API can perform live RPC work; needs provider config, error/stale paths, latency concerns.
- [ ] No
  - Deletes/narrows: API is read-only over stored projections/execution output; simpler and more predictable.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

We should also have RPC fallback/etc, for failover, redundancy, and optionally, correctness.


### 12. Should execution traces be public?

- [ ] Yes
  - Keeps/adopts: keep public explain/execution trace routes.
- [ ] No
  - Deletes/narrows: execution traces become internal debugging artifacts only.
- [X] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

What is included in the execution traces? Where are they public?

### 13. Should CCIP-Read be a public supported path?

- [X] Yes
  - Keeps/adopts: keep CCIP semantics in public verified resolution.
- [ ] No
  - Deletes/narrows: CCIP paths become unsupported/internal; less transport/execution complexity.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 14. Should Basenames L1 Resolver transport be publicly supported?

- [X] Yes
  - Keeps/adopts: keep Basenames L1 resolver transport-assisted verified reads.
- [ ] No
  - Deletes/narrows: Basenames public data is Base-side indexed facts only.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


## Resolvers

### 15. Should resolver overview be public?

- [X] Yes
  - Keeps/adopts: keep resolver-centric routes and projections.
- [ ] No
  - Deletes/narrows: exact-name resolver address only; delete resolver overview/fan-in product.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 16. Should resolver aliases be public?

- [X] Yes
  - Keeps/adopts: keep alias sections and fan-in distinctions.
- [ ] No
  - Deletes/narrows: aliases are internal topology only or omitted from public API.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 17. Should resolver profile completeness be public?

- [X] Yes
  - Keeps/adopts: publish supported/pending/unsupported profile completeness.
- [ ] No
  - Deletes/narrows: profile admission is internal; public API exposes observed/current values only.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

Didn't you already ask this question?


### 18. Should shared ENSv1 PublicResolver fan-in be modeled publicly?

- [X] Yes
  - Keeps/adopts: model shared resolver fan-in and explicit unsupported sections.
- [ ] No
  - Deletes/narrows: do not expose shared PublicResolver fan-in; avoids unbounded enumeration semantics.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


## Names

### 19. Should exact-name full profile be public?

- [X] Yes
  - Keeps/adopts: keep rich `/v1/names/{namespace}/{name}` full profile.
- [ ] No
  - Deletes/narrows: exact name returns the app-facing name DTO only.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 20. Should `/v1/names` handle both search and exact lookup?

- [ ] Yes
  - Keeps/adopts: one collection route does search plus exact compact lookup.
- [ ] No
  - Deletes/narrows: split exact lookup from search; simpler route semantics.
- [X] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

Check against current app usage, is this okay to split?

### 21. Should children/subnames be public?

- [X] Yes
  - Keeps/adopts: keep child/subname collection API.
- [ ] No
  - Deletes/narrows: delete children projections/routes from public product.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 22. Should child counts be public?

- [X] Yes
  - Keeps/adopts: keep projected child counts.
- [ ] No
  - Deletes/narrows: children, if kept, are rows only; no count guarantees.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 23. Should address-to-name relations be public?

- [X] Yes
  - Keeps/adopts: keep owner/registrant/controller address relation reads.
- [ ] No
  - Deletes/narrows: exact lookup/search only; no address-to-name membership product.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 24. Should primary names be public?

- [X] Yes
  - Keeps/adopts: keep primary-name claim/verification API.
- [ ] No
  - Deletes/narrows: delete primary-name route/projections/execution from public product.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


## Permissions And Roles

### 25. Should roles be public?

- [X] Yes
  - Keeps/adopts: keep compact role rows.
- [ ] No
  - Deletes/narrows: delete roles route; permissions may still exist internally.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 26. Should resource permissions be public?

- [X] Yes
  - Keeps/adopts: keep resource permission lineage API.
- [ ] No
  - Deletes/narrows: permissions are internal inputs for ownership/control only.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 27. Should ENSv2/Basenames role bitmaps be public?

- [X] Yes
  - Keeps/adopts: expose protocol role bitmaps/effective powers.
- [ ] No
  - Deletes/narrows: collapse public permissions to owner/controller-style summaries.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


## Namespaces

### 28. Should ENSv2 be first-class now?

- [X] Yes
  - Keeps/adopts: ENSv2 remains a first-class documented profile.
- [ ] No
  - Deletes/narrows: park ENSv2 as experimental/internal until the product needs it.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 29. Should Basenames have parity with ENS?

- [X] Yes
  - Keeps/adopts: Basenames tries to match ENS capability breadth.
- [ ] No
  - Deletes/narrows: define a smaller Basenames subset; likely big transport/primary/resolver savings.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 30. Should namespace inference exist?

- [X] Yes
  - Keeps/adopts: routes may infer/default namespace, with ENS implied when unspecified.
- [ ] No
  - Deletes/narrows: require explicit namespace everywhere; less magic but more client burden.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


## API Contract

### 31. Should unsupported sub-sections return `200` with typed unsupported objects?

- [ ] Yes
  - Keeps/adopts: keep typed unsupported objects inside `200` responses.
- [ ] No
  - Deletes/narrows: use normal errors/nulls; greatly shrinks support taxonomy.
- [X] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

Which is actually better?


### 32. Should `meta=full` exist?

- [X] Yes
  - Keeps/adopts: compact routes can expose deeper metadata.
- [ ] No
  - Deletes/narrows: compact remains compact; no metadata escape hatch.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

meta attachment handler should be shared across all routes though right?


### 33. Should `view=full` exist on compact routes?

- [ ] Yes
  - Keeps/adopts: compact routes may delegate to full responses.
- [ ] No
  - Deletes/narrows: one route, one shape; no `view=full` branches.
- [X] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

What does `view=full` mean?


### 34. Should `mode=declared|verified|both` exist publicly?

- [ ] Yes
  - Keeps/adopts: clients choose declared/verified/both.
- [ ] No
  - Deletes/narrows: one source policy per route; delete mixed-mode response logic.
- [X] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

Is there a usecase for having it public?


### 35. Should OpenAPI be exhaustive and hand-maintained?

- [ ] Yes
  - Keeps/adopts: keep detailed hand-authored OpenAPI.
- [X] No
  - Deletes/narrows: generate or shrink OpenAPI; likely direct LOC win.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

Generate it.


## Storage And Indexing

### 36. Should raw facts be retained forever?

- [ ] Yes
  - Keeps/adopts: raw facts are permanent audit/rebuild source.
- [ ] No
  - Deletes/narrows: allow pruning/compaction; much bigger storage/process change.
- [X] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

Don't we already have a mode that doesn't store raw facts? Only the resulted normalized events? Unsure

### 37. Should normalized events be part of public semantics?

- [ ] Yes
  - Keeps/adopts: events/history semantics are part of public API.
- [ ] No
  - Deletes/narrows: normalized events are internal replay/projection input only.
- [X] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

How might history/etc be consumed without public normalized events?


### 38. Should projections be rebuildable from raw facts?

- [X] Yes
  - Keeps/adopts: full rebuild/replay remains a core guarantee.
- [ ] No
  - Deletes/narrows: prefer simpler materialized current state; large architectural simplification.
- [ ] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:


### 39. Should canonicality/reorg repair be explicit in public behavior?

- [ ] Yes
  - Keeps/adopts: public errors expose stale/conflict/canonicality behavior.
- [ ] No
  - Deletes/narrows: hide reorg/canonicality behind latest indexed state.
- [X] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

Can you clarify what you mean by this?


### 40. Should multi-chain snapshots be coherent across chains?

- [ ] Yes
  - Keeps/adopts: cross-chain answers share one coherent selected snapshot.
- [ ] No
  - Deletes/narrows: each namespace/route reads its own latest authoritative chain state.
- [X] Unsure
  - Decide before coding; this affects docs and implementation scope.

Notes:

How does this behaviour work as-is?


## Candidate Scope Bundles

### Smallest App API

- [ ] Latest/current state only
- [ ] Compact or standard DTO only
- [ ] Indexed records only
- [ ] No public provenance
- [ ] No public coverage
- [ ] No public explain routes
- [ ] No public resolver overview
- [ ] No public roles/permissions
- [ ] No public history/events

Notes:


### Current Index Plus History

- [ ] Latest/current state only
- [ ] Keep history/events
- [ ] Indexed records only
- [ ] No public provenance
- [ ] No public coverage
- [ ] No live RPC from API

Notes:


### Audit Platform

- [ ] Keep snapshots
- [ ] Keep coverage
- [ ] Keep provenance
- [ ] Keep explain routes
- [ ] Keep history/events
- [ ] Keep typed unsupported semantics

Notes:


## Final Direction

Chosen product shape:


Must keep:


Can delete:


Needs docs-first change:


Implementation order:
