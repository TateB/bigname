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

## 2. Resolution Flow

Verified resolution follows this sequence:

1. load the declared topology for the requested surface and chain positions
2. choose the namespace-specific execution entrypoint
3. resolve resolver selection, alias rewrites, and wildcard traversal
4. execute onchain calls
5. follow CCIP-Read when allowed by the manifest and resolver family
6. persist the execution trace and final answer

Rules:

- every step is attributable in provenance
- wildcard traversal and alias rewriting must be explicit in the trace
- unsupported record families fail explicitly rather than silently degrading

## 3. Primary-Name Verification Flow

Primary verification follows this sequence:

1. load the claimed reverse or primary setting
2. normalize the claimed name using the recorded normalizer version
3. resolve the claimed name for the requested `coin_type`
4. compare the resolved target with the requested address
5. persist both the claim state and verification result

Verification statuses:

- `verified`
- `claimed_only`
- `mismatch`
- `unnormalized`
- `not_found`
- `unsupported`

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

## 6. Explain Requirements

Every verified answer must be explainable through:

- selected entrypoint
- resolver discovery path
- wildcard traversal
- alias rewriting
- CCIP steps
- final comparison or returned record value

## 7. Initial Support Boundary

For the first implementation slice:

- ENS uses the canonical Universal Resolver path on Ethereum L1
- Basenames verified execution is scaffolded but may initially expose partial coverage until Base-side authority and L1 transport are both wired
- unsupported resolver families remain requestable but must return explicit unsupported coverage or typed verification failure

