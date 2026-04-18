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

For ENS on Ethereum Mainnet, step 2 is frozen to the `ens_execution` source family. Its canonical manifest-declared execution entrypoint is the ENS Universal Resolver: `[[contracts]] role = "universal_resolver"` at `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`.

Rules:

- every step is attributable in provenance
- one verified resolution request may cover multiple explicit record selectors under one request-scoped execution trace
- execution returns one `verified_queries` result object per requested selector and uses the shared `ResultStatus` vocabulary
- execution entrypoint selection is attributable to the manifest-declared `source_family` and `role`; it is not implied by registry-family presence alone
- wildcard traversal and alias rewriting must be explicit in the trace
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
- claim-local provenance and verification-local provenance may both contribute to the route, but only the verification-local side is anchored to the persisted `execution_trace_id`
- for ENS on Ethereum Mainnet in the current contract, the admitted declared claim surface is reverse-only: `ens_v1_reverse_l1` through contract role `reverse_registrar` at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`
- for ENS on Ethereum Mainnet, the verification step for that claimed name reuses the `ens_execution` source family and its manifest-declared `universal_resolver` entrypoint at `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`; declared claim ownership and verified execution ownership stay separate
- that declared-vs-verified split means Phase 7 does not synthesize richer ENS `claimed_primary_name` payloads by combining reverse tuple intake with resolver-backed or execution-derived name identity; those richer claimed payloads remain blocked
- missing or unsupported ENS reverse claims do not trigger fallback to registry-, resolver-, or other claim-setting surfaces in this phase
- manifest rollout and capability state remain source-family-local inputs only: they may admit reverse claim intake or shadow execution traces and cache ownership, but they do not by themselves widen ENS claim precedence, graduate route-level primary-name coverage, or ship richer tuple-present `claimed_primary_name` or `verified_primary_name` payloads
- the shipped bootstrap route may still return explicit verified `status=unsupported` without surfacing the richer tuple-present claimed or verified payloads above

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

Each step records:

- step index
- step kind
- input digest
- output digest
- latency
- canonicality dependency

## 5. Cache Key And Invalidation

Verified answers are cached by:

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

## 6. Explain Requirements

Every verified answer must be explainable through:

- selected entrypoint
- resolver discovery path
- wildcard traversal
- alias rewriting
- CCIP steps
- final comparison or returned record value

For resolution, the queued explain surface is `GET /v1/explain/resolutions/{namespace}/{name}/execution`.

Rules:

- it is keyed by the same exact surface, snapshot selection, and explicit selector set as `GET /v1/resolutions/{namespace}/{name}`
- it reads the persisted execution trace and selector-scoped results already stored for that request; it does not re-execute the request or synthesize explain detail from declared topology alone
- top-level provenance and any selector-local provenance stay anchored to the same persisted `execution_trace_id`
- the route surfaces the selected entrypoint, resolver discovery path, wildcard traversal, alias rewriting, and the ordered persisted step summary; CCIP-Read participation appears through persisted step kinds rather than a raw gateway transcript
- it does not become a global trace-inspection API, a raw trace dump, or a second provenance / truth system
- until a handler ships, the route remains prose-frozen only and stays outside `docs/api-v1.openapi.json`

## 7. Initial Support Boundary

For the first implementation slice:

- ENS verified resolution on Ethereum Mainnet uses `ens_execution` with contract role `universal_resolver` at `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`; freezing this entrypoint does not by itself ship public verified-resolution reads
- Basenames verified execution is scaffolded but may initially expose partial coverage until Base-side authority and L1 transport are both wired
- ENS primary-name support remains bootstrap-only: the public route may be present and the owning source families may be admitted, but route-level coverage stays in its bootstrap unsupported state; manifest rollout, manifest capability state, reverse tuple lookup, and resolver-backed verification detail do not by themselves graduate that public contract or unlock richer ENS claimed payloads, and any fallback beyond the reverse-only claim surface remains deferred
- unsupported resolver families remain requestable but must return explicit `status=unsupported` results unless the route cannot attribute any section-level answer at all
