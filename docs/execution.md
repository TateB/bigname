# Verified Execution

Status: Phase 0 baseline

This document freezes the verified execution plane for resolution and primary-name verification.

## 1. Supported Entry Points

Initial verified entry points:

- explicit record resolution by name
- verified primary-name lookup by address and `coin_type`

The execution plane consumes:

- declared topology snapshots
- manifest versions
- requested chain positions

It does not read adapter-specific internals directly.

Mixed resolution and primary-name routes reuse one shared `ResultStatus` vocabulary:

- `success`
- `not_found`
- `mismatch`
- `unsupported`
- `invalid_name`
- `execution_failed`

Execution uses `ResultStatus` for verified route-local result objects, and the same vocabulary is reused by the paired declared primary-name claim object. The route contract decides which subset applies to each object.

## 2. Resolution Flow

Verified resolution follows this sequence:

1. load the declared topology for the requested surface and chain positions
2. choose the namespace-specific execution entrypoint
3. resolve resolver selection, alias rewrites, and wildcard traversal
4. execute onchain calls
5. follow CCIP-Read when allowed by the manifest and resolver family
6. persist the execution trace and final answer

For the namespace-inferred convenience route `GET /v1/resolve/{name}`, inference happens before step 1 and produces the canonical `{namespace, name}` tuple used by the rest of the flow:

- exact `base.eth` resolves as `namespace=ens`
- names matching `*.base.eth` resolve as `namespace=basenames`
- other supported ENS names resolve as `namespace=ens`

The inferred namespace is not execution-local metadata. It selects the declared topology, execution entrypoint, trace namespace, request key, provenance, and cache identity exactly as if the caller had used `GET /v1/resolutions/{namespace}/{name}`.

For ENS on Ethereum Mainnet, step 2 is frozen to the `ens_execution` source family. Its canonical manifest-declared execution entrypoint is the ENS Universal Resolver proxy: `[[contracts]] role = "universal_resolver"` at `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` (official ENS docs: https://docs.ens.domains/resolvers/universal/). The pinned ENSv1 deployment artifact remains the implementation / ABI anchor behind that source family rather than the route-facing proxy address (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f).

For Basenames on the shipped mainnet profile, step 2 is frozen to the `basenames_execution` source family. Its canonical manifest-declared execution entrypoint is the Basenames L1 Resolver: `[[contracts]] role = "l1_resolver"` at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc).

That Basenames execution owner shares the same L1 Resolver address with `basenames_l1_compat`, but the ownership split stays explicit: `basenames_l1_compat` owns transport attribution, while active `basenames_execution` v2 owns verified-resolution entrypoint selection with `verified_resolution=supported` only for one exact-surface transport-assisted direct-path class. The supported class requires `resolver_path[0].logical_name_id` to equal top-level `data.logical_name_id`, `wildcard.source=null` with `matched_labels=[]`, `alias.final_target=null` with `hops=[]`, `subregistry_path=[]`, `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, and `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"`; all other Basenames verified / explain path classes remain explicit `unsupported`, and transport ownership stays with `basenames_l1_compat` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc).

The declared read plane stays separate from that Basenames execution / transport pairing: exact-name, address-name, and children reads remain sourced from the admitted Base registry / registrar / resolver families, while `basenames_base_primary` stays claim intake only (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc).

Rules:

- every step is attributable in provenance
- one verified resolution request may cover multiple explicit record selectors under one request-scoped execution trace
- execution returns one `verified_queries` result object per requested selector and uses the shared `ResultStatus` vocabulary
- execution entrypoint selection is attributable to the manifest-declared `source_family` and `role`; it is not implied by registry-family presence alone
- wildcard traversal and alias rewriting must be explicit in the trace
- namespace inference and verified support are separate gates: inferred `namespace=basenames` requests never retry as `namespace=ens` outside the Basenames exact-surface transport-assisted direct-path support class
- unsupported record families stay explicit as `status=unsupported`; they do not silently degrade to declared cache values
- supported selector requests that cannot produce a trustworthy answer return `status=execution_failed` with a typed `failure_reason`

## 3. Primary-Name Verification Flow

Primary verification follows this sequence:

1. load the claimed name from the currently admitted declared claim surface
2. normalize the claimed name using the recorded normalizer version
3. resolve the claimed name for the requested `coin_type`
4. compare the resolved target with the requested address
5. persist both the claim state and verification result

Rules:

- the route keeps claimed state separate from the execution-derived verification result
- `claimed_primary_name` and `verified_primary_name` both use the shared `ResultStatus` vocabulary
- `claimed_primary_name` is limited to `success`, `not_found`, `unsupported`, and `invalid_name`; `verified_primary_name` is limited to `success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, and `execution_failed`
- a raw claim that cannot be normalized surfaces `status=invalid_name`; it is not silently treated as missing
- `raw_claim_name` is claim-local state: it may be preserved to explain `claimed_primary_name.status=invalid_name`, but it does not migrate into `verified_primary_name`
- `mismatch` and `execution_failed` are verified-only outcomes; when emitted, any `failure_reason` stays verification-local and does not duplicate declared claim identity
- `mismatch` means the claim normalized, resolved for the requested `coin_type`, and produced a concrete target address that did not equal the requested address
- when verification establishes a concrete normalized name target, `verified_primary_name` may carry that name identity for `status=success` or `status=mismatch`; it omits that identity for `status=not_found`, `status=unsupported`, `status=invalid_name`, and `status=execution_failed`
- claim-local provenance and verification-local provenance may both contribute to the route, but the claim-local side is exact-tuple declared provenance from the requested `primary_names_current(address, coin_type, namespace)` row and the shipped verification-local side is `verified_primary_name.provenance = {execution_trace_id, manifest_versions}` under the same top-level `provenance.execution_trace_id` for that exact tuple
- for ENS on Ethereum Mainnet in the current contract, the admitted declared claim surface is reverse-only: `ens_v1_reverse_l1` through contract role `reverse_registrar` at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb` (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L19 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f)
- for Basenames on the shipped mainnet profile, the admitted declared primary-claim family is `basenames_base_primary` through contract role `reverse_registrar` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282`; it remains claim intake only, so exact-name, address-name, and children declared truth stays on the Base registry / registrar / resolver families because upstream exposes reverse-name writes through the dedicated ReverseRegistrar rather than the Base authority stack (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
- for ENS on Ethereum Mainnet, the verification step for that claimed name reuses the `ens_execution` source family and its manifest-declared `universal_resolver` proxy entrypoint at `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`; declared claim ownership and verified execution ownership stay separate (official ENS docs: https://docs.ens.domains/resolvers/universal/) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L183 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L199 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L205 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L263 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L269 @ ens_v1@91c966f)
- for Basenames as well as ENS, `claimed_primary_name` and `verified_primary_name` stay separate route-local objects: declared claim intake does not backfill verified identity, and Base authority reads plus the separate Ethereum Mainnet `L1Resolver` execution owner do not collapse them into one truth system (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
- `claimed_primary_name.name`, when present, comes only from the exact requested `primary_names_current(address, coin_type, namespace)` row's declared normalized claim-identity source for that same tuple, aligned with the currently admitted reverse-only claim precedence (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
- it must not be synthesized or backfilled from manifest presence, resolver-backed identity, verified execution identity, tuple presence alone, a different tuple, or any fallback claim source
- `claimed_primary_name.name` remains distinct from execution-derived `verified_primary_name.name`; this clarification does not change when `verified_primary_name.name` appears, and it does not by itself widen the exact-tuple primary-name coverage contract
- that declared-vs-verified split means Phase 7 does not synthesize richer ENS `claimed_primary_name` payloads by combining reverse tuple intake with resolver-backed or execution-derived name identity; `claimed_primary_name.provenance` stays limited to exact-tuple declared row provenance, while deferred fallback-source expansion remains blocked
- the exact-tuple verified-primary support class is persisted readback only for the exact route tuple; the shipped ENS slice and the frozen first Basenames slice both use it, and the read path does not become a fresh execution entrypoint
- that exact-tuple support class uses stable execution identity `request_type=verified_primary_name`
- its `request key` identity is the exact normalized route tuple `{namespace}:{normalized_address}:{coin_type}`, where `normalized_address` uses the same lowercase normalization as `GET /v1/primary-names/{address}`; claimed text, normalized claim or verified name identity, verified target address, result status, and section-local provenance do not participate in that key
- that persisted-readback support class is also the only route-level primary-name coverage support class: ENS and Basenames exact tuples may publish `coverage.status=partial` with `exhaustiveness=non_enumerable` using the route's namespace-local claim and execution source families; route tuples outside the frozen classes remain explicit `unsupported` instead of inheriting coverage from manifest rollout, tuple presence, or verified-resolution support (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)
- `primary_names_current(address, coin_type, namespace)` is the claim-side lookup / invalidation anchor for that same tuple; projection-owned claim state may explain tuple admission or claim invalidation, but it must not persist `execution_trace_id` or `verified_primary_name`
- the public `claimed_primary_name.provenance` surface is exact-tuple declared-only provenance from that requested row; it must strip `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material and omit `execution_trace_id`
- the shipped `verified_primary_name.provenance` surface is limited to `execution_trace_id` and `manifest_versions`: it is a strict verification-local refinement for the same exact tuple, `verified_primary_name.provenance.execution_trace_id` must equal top-level `provenance.execution_trace_id`, and `verified_primary_name.provenance.manifest_versions` must narrow that same persisted verification trace
- `verified_primary_name.provenance` must not publish `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material, restate claimed-row provenance, or introduce a second lookup / invalidation identity for the tuple
- the shipped Phase 7 ENS primary-name path still does not require dedicated manifest capability flags such as `claimed_primary_name` or `verified_primary_name`; reverse claim admission stays owned by the active `ens_v1_reverse_l1` manifest, while verified-primary readback stays execution-derived under the already frozen `ens_execution` owner (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f)
- the frozen first Basenames exact-tuple verified-primary slice likewise does not require a dedicated manifest capability flag; reverse claim admission stays owned by `basenames_base_primary`, while verified-primary readback stays execution-derived under the already frozen `basenames_execution` owner because upstream keeps reverse-name writes on the Base ReverseRegistrar while the separate Ethereum Mainnet `L1Resolver` remains the execution entrypoint (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
- top-level route provenance joins claim-side and verification-side context; section-local provenance stays narrower, `claimed_primary_name.provenance` stays row-scoped and declared-only, and `verified_primary_name.provenance`, when present, stays verification-local under that same persisted `execution_trace_id`
- missing or unsupported ENS reverse claims do not trigger fallback to registry-, resolver-, or other claim-setting surfaces in this phase; the admitted ENS claim source is the reverse registrar tuple only (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
- manifest rollout and capability state remain source-family-local inputs only: they may admit reverse claim intake or shadow execution traces and cache ownership, but they do not by themselves widen ENS claim precedence, widen route-level primary-name coverage beyond the exact-tuple persisted-readback class, or ship richer tuple-present `claimed_primary_name` or `verified_primary_name` payloads (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
- introducing any dedicated primary-name manifest capability flag would therefore be a later additive contract change, not a prerequisite for the shipped persisted-readback or reverse-claim slices
- the shipped route may still return explicit verified `status=unsupported` outside the frozen exact-tuple persisted-readback class without surfacing richer tuple-present claimed or verified payloads

## 4. Trace Schema

Each verified answer persists:

- `execution_trace_id`
- request type
- request key
- namespace
- chain positions
- manifest versions
- step list
- contracts called
- gateway digests
- final value
- failure reason
- finished timestamp

For resolution, one persisted answer may include multiple selector-scoped outputs under the same `execution_trace_id`.

For the exact-tuple verified-primary support class, one persisted answer covers exactly one `{address, namespace, coin_type}` tuple under `request_type=verified_primary_name`.

Each step records:

- step index
- step kind
- input digest
- output digest
- latency
- canonicality dependency

Execution traces and execution steps are durable audit artifacts. Reorg-driven cache invalidation must not delete `execution_traces`, `execution_steps`, object-store attachments, or the trace-local step list; it only changes whether a persisted verified outcome can be reused as a cache hit.

### Worker Trace Inspection

`bigname-worker inspect execution-trace --execution-trace-id <id> --json` is the worker-owned operational inspection surface for one persisted execution trace.

The stable JSON output is limited to already persisted trace and step state:

- `command`
- `execution_trace_id`
- request metadata already stored on the trace
- request type and request key
- namespace
- chain positions
- manifest versions
- trace status, final value digest, failure reason, and finished timestamp
- ordered `steps` entries with step index, step kind, input digest, output digest, latency, canonicality dependency, and attachment digest metadata where present

Rules:

- the command reads `execution_traces`, `execution_steps`, and trace attachment metadata only
- it does not execute or re-execute resolution, primary-name verification, CCIP calls, or topology discovery
- it does not expose a public `v1` route, raw execution API, raw gateway transcript API, or batch trace dump
- it does not synthesize declared topology, resolver paths, wildcard paths, alias paths, or transport paths from non-trace storage
- it does not mutate `execution_cache_outcomes`, projections, manifests, discovery edges, watch plans, or normalized events
- it preserves the public explain boundary: `GET /v1/explain/resolutions/{namespace}/{name}/execution` remains the route-local explain view over persisted supported resolution traces, while this command is operational read-only inspection

## 5. Cache Key And Invalidation

Persisted verified outcomes are cached in `execution_cache_outcomes` by:

- request key
- requested chain positions
- manifest versions
- topology version boundary
- record version boundary

Invalidate on:

- reorg
- manifest change
- resolver change
- alias or wildcard topology change
- relevant record change
- primary claim change

For resolution, `request key` includes the normalized explicit selector set so the cache boundary matches `verified_queries`.

For `GET /v1/resolve/{name}`, the resolution request key is built from the inferred namespace, normalized name, and normalized explicit selector set. A namespace-inferred request and the equivalent canonical `GET /v1/resolutions/{namespace}/{name}` request therefore share cache identity after inference; the raw convenience path string is not a separate cache namespace.

For verified primary-name, `request key` is the normalized tuple string `{namespace}:{normalized_address}:{coin_type}`. The matching `primary_names_current(address, coin_type, namespace)` row is the only admitted claim-side lookup / invalidation anchor for that key; projection updates for that row may invalidate request-matching verified answers, but the projection does not persist verified result payloads or trace IDs.

Phase 9 reorg invalidation rules:

- reorg repair invalidates any `execution_cache_outcomes` row for verified resolution or verified primary-name readback whose dependency set contains an orphaned block identity
- cache dependencies must be tied to explicit block-hash-bearing chain positions or boundaries; block numbers, `latest` / `head` tags, manifest versions, topology versions, and record versions are not sufficient unless they resolve to one or more block hashes or to source rows that carry block hashes
- verified resolution and verified primary-name rows without explicit block-hash-bearing dependencies fail closed and are ineligible for cache reuse after a reorg check; request types that are documented as not depending on chain state remain explicitly out of scope rather than implicitly safe
- invalidation affects cache eligibility only; execution traces, execution steps, and trace attachments remain durable audit artifacts
- this is a reorg/replay foundation only: it does not promote ENSv2 exact-name support, widen any verified support class, or graduate any manifest capability

## 6. Explain Requirements

Every verified answer must be explainable through:

- selected entrypoint
- resolver discovery path
- wildcard traversal
- alias rewriting
- CCIP steps
- final comparison or returned record value

For resolution, the shipped explain surface is `GET /v1/explain/resolutions/{namespace}/{name}/execution`.

Rules:

- it is keyed by the same current exact surface and explicit selector set as `GET /v1/resolutions/{namespace}/{name}`
- it reads the persisted execution trace and selector-scoped results already stored for that request; it does not re-execute the request or synthesize explain detail from declared topology alone
- top-level provenance and any selector-local provenance stay anchored to the same persisted `execution_trace_id`
- the route surfaces the selected entrypoint, resolver discovery path, wildcard traversal, alias rewriting, and the ordered persisted step summary; CCIP-Read participation appears through persisted step kinds rather than a raw gateway transcript
- it does not become a global trace-inspection API, a raw trace dump, or a second provenance / truth system
- it is shipped and published in `docs/api-v1.openapi.json`; the current handler contract exposes path parameters plus required `records` only
- public explain support stays coupled to the same verified-resolution support boundary as the mixed route; deferred unsupported path classes do not gain a synthetic trace-shaped public contract
- for Basenames, the public execution-explain support boundary applies only to execution explain; the separate declared exact-name explain routes stay on the Base-side declared read plane (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)

## 7. Initial Support Boundary

For the shipped Phase 7 slice:

- ENS verified resolution on Ethereum Mainnet uses `ens_execution` with contract role `universal_resolver` at the official ENS Universal Resolver proxy address `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`; the shipped public verified slice covers exact-surface direct-path requests first, the already frozen exact-surface alias-only non-direct class, and the first additive exact-surface wildcard-derived class (official ENS docs: https://docs.ens.domains/resolvers/universal/) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L90 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L106 @ ens_v1@91c966f)
- for that support check, use the same declared topology snapshot as the mixed route: a request is direct-path only when `resolver_path[0].logical_name_id` equals top-level `data.logical_name_id`, `wildcard.source` is `null` with `matched_labels=[]`, `alias.final_target` is `null` with `hops=[]`, and all `transport` fields are `null`
- the already frozen ENS alias-only non-direct support class is the exact-surface class where that same declared topology snapshot keeps `resolver_path[0].logical_name_id` equal to top-level `data.logical_name_id`, `alias.final_target` is non-`null` with `hops` non-empty, `wildcard.source` is `null` with `matched_labels=[]`, and all `transport` fields are `null`
- the first additive ENS wildcard-derived support class is the exact-surface class where `wildcard.source` is non-`null` with `matched_labels` non-empty, `resolver_path[0].logical_name_id` equals `wildcard.source.logical_name_id`, `alias.final_target` is `null` with `hops=[]`, `subregistry_path=[]`, and all `transport` fields are `null`
- supported direct-path, alias-only, and wildcard-derived answers remain attributable through the same persisted execution trace and explain contract: the public explain route must surface the selected entrypoint, resolver discovery path, ordered persisted steps, and the participating alias or wildcard detail for that persisted answer without inventing a second trace family
- ENS verified requests outside the direct-path, alias-only, and wildcard-derived classes, including other non-alias ancestor-selected paths, linked-subregistry ancestor-selected paths, any transport-assisted path, and any request whose persisted execution used CCIP-Read, remain deferred and return explicit selector-local `status=unsupported` on the mixed route; the shipped explain route does not synthesize public traces for them
- Basenames verified resolution on the shipped mainnet profile uses active `basenames_execution` v2 with contract role `l1_resolver` at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31`; `basenames_l1_compat` owns that same L1 Resolver address as compatibility transport, and public Basenames verified / explain support is limited to the exact-surface transport-assisted direct-path class rather than a transport-free or authority-replacing class (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- that supported Basenames class uses the same declared-topology snapshot as the mixed route: `resolver_path[0].logical_name_id` equals top-level `data.logical_name_id`, `wildcard.source` is `null` with `matched_labels=[]`, `alias.final_target` is `null` with `hops=[]`, `subregistry_path=[]`, `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, and `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
- CCIP-participating traces are eligible for that supported Basenames class rather than selector-local `status=unsupported` because the upstream `L1Resolver` initiates `OffchainLookup` for non-`base.eth` requests and completes them through `resolveWithProof`; the explain route must therefore surface the resulting persisted CCIP steps for that class without inventing a second trace family (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)
- Basenames paths outside that frozen transport-assisted direct class remain explicit `unsupported`, and the verified-resolution support class still does not widen route-level primary-name coverage beyond the separate exact-tuple persisted-readback class or add a new manifest flag; the first Basenames `verified_primary_name` support class on `GET /v1/primary-names/{address}` is instead that exact-tuple class under the same reverse-intake / execution split (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
- `GET /v1/resolve/{name}` does not widen this support boundary: exact `base.eth` is inferred as `namespace=ens`, names matching `*.base.eth` are inferred as `namespace=basenames`, and inferred Basenames verified selectors return selector-local `status=unsupported` unless the requested snapshot satisfies the same frozen transport-assisted direct-path Basenames support class
- ENS and Basenames primary-name coverage has graduated only for the local exact-tuple persisted-readback class: supported tuples may return route-level `coverage.status=partial` with `exhaustiveness=non_enumerable`; unfrozen tuples, fallback claim sources, richer claimed payloads, fresh verified-primary execution, and namespace-wide or app-parity claims remain explicit `unsupported` or out of scope. Manifest rollout, manifest capability state, reverse tuple lookup, and resolver-backed verification detail do not by themselves widen that exact-tuple public contract, and any fallback beyond the currently admitted claim surface remains deferred (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc)
- unsupported resolver families remain requestable but must return explicit `status=unsupported` results unless the route cannot attribute any section-level answer at all
