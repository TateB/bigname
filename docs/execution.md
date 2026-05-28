# Verified execution

Verified execution covers two read paths: explicit record resolution by name, and primary-name verification by `(address, coin_type)`. Both consume declared topology snapshots, manifest versions, and the requested chain positions; neither reads adapter-specific internals directly. Mixed routes return per-result `ResultStatus` from one shared vocabulary: `success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, `execution_failed`. Verified-only outcomes are `mismatch` and `execution_failed`. Companion docs: [`architecture.md`](architecture.md), [`api-v1.md`](api-v1.md), [`storage.md`](storage.md).

## Resolution flow

A verified resolution request runs:

1. load the declared topology for the requested surface and chain positions
2. select the namespace's execution entrypoint
3. resolve resolver selection, alias rewrites, and wildcard traversal
4. execute onchain calls
5. follow CCIP-Read where the manifest and resolver family allow it
6. hand any admitted exact block-anchored call snapshots to intake-owned raw facts; persist the trace and final answer

Every step is attributable in provenance. One request may cover multiple explicit selectors under one request-scoped trace, returning one `verified_queries` entry per selector. Wildcard traversal and alias rewriting appear explicitly in the trace. Entrypoint selection is attributable to a manifest-declared `source_family` and `role` — registry-family presence alone does not imply it. Admitted exact block-anchored `raw_call_snapshots` stay intake-owned; execution may hand them off as a narrow persistence step but they are not trace rows.

Before persisting a selector-local result as a supported, cache-eligible outcome, execution reloads from storage the manifest versions, the same declared topology snapshot the mixed route would serve, and any resolver-profile admission state required by the participating resolver-local fact families. The namespace support class is derived from those stored inputs, not from transient trace shape. If revalidation cannot re-establish a frozen supported class, audit material may persist but supported-outcome persistence fails closed.

Unsupported record families surface explicit `status=unsupported`; they never silently degrade to declared cache values. Supported requests that cannot produce a trustworthy answer return `status=execution_failed` with a typed `failure_reason`.

### Namespaces and entrypoints

For ENS on Ethereum Mainnet, the entrypoint is `ens_execution` with contract role `universal_resolver` at the official ENS Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`.[^ens-docs-univ] The pinned ENSv1 deployment artifact is the implementation/ABI anchor behind that source family rather than the route-facing proxy.[^v1-ur-deploy][^v1-ursol-l8]

For Basenames on the shipped mainnet profile, the entrypoint is active `basenames_execution` v2 with contract role `l1_resolver` at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` for the exact-surface transport-assisted direct-path class only.[^bn-readme-l22][^bn-l1resolver-l13] The same L1 Resolver address is also referenced by `basenames_l1_compat`, but ownership stays split: `basenames_l1_compat` owns transport attribution; `basenames_execution` owns verified-resolution entrypoint selection. Declared exact-name, address-name, and children reads remain on the Base registry/registrar/resolver families. `basenames_base_primary` is claim intake only.[^bn-readme-l70][^bn-revreg-l12][^bn-revreg-l150]

### On-demand execution

`GET /v1/profiles/names/{name}` with `mode=verified` or `mode=both`, and `GET /v1/names/{namespace}/{name}/records` with verified selectors, are cache-or-live-execute reads for supported Universal Resolver selectors.[^v1-iur-l44][^v1-iur-l52] The profile route does not accept a selector query; it executes every server-derived profile selector from declared inventory selectors, explicit gaps, and record-cache entries for the selected snapshot. If that derived set is non-empty, it is complete for the profile route. The bounded app profile set is used only when a supported declared inventory exists but has no declared selector/gap/cache records; missing, stale, or unsupported inventory does not trigger defaults. The compact records route is the selector-specific app path. Each route first looks for matching persisted execution output at the selected exact-name snapshot. On miss, the API performs Universal Resolver execution against that selected chain position, persists the trace and outcome, and returns the persisted outcome in the same response.

Live-execution rules:

- the execution target is the exact `ChainPositions` selected by the route before any verified-support check; absent `at` and `chain_positions`, this is `consistency=head` and the latest stored checkpoint for the required chain
- full resolution and explain/audit execution never retarget to provider latest, a newer checkpoint, or a different snapshot mid-request
- the API Ethereum RPC provider must be configured (`BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<url>`) and able to serve the selected Ethereum block; missing configuration or provider unavailability fails closed with `409 stale` and a configuration message rather than falling back to declared cache
- unsupported selector families and unsupported verified path classes stay selector-local `status=unsupported`; on-demand execution does not widen the support boundary
- `GET /v1/explain/resolutions/{namespace}/{name}/execution` is persisted-trace readback only

The compact records route `GET /v1/names/{namespace}/{name}/records` uses the same supported-selector boundary and selected stored snapshot as the full profile route. When it needs on-demand ENS verified values, it executes against that snapshot, persists the trace and outcome, and fails closed with `409 stale` when the provider cannot serve the selected block. It never targets provider `latest` independently of the selected snapshot.

### Namespace inference

For `GET /v1/profiles/names/{name}`, inference happens before step 1 and produces the canonical `{namespace, name}` tuple:

- exact `base.eth` resolves as `namespace=ens`
- `*.base.eth` resolves as `namespace=basenames`
- other supported ENS names resolve as `namespace=ens`

The inferred namespace selects topology, entrypoint, trace namespace, request key, provenance, and cache identity exactly as if the caller had used the canonical route. Namespace inference and verified support are separate gates: an inferred `namespace=basenames` request never retries as `namespace=ens`.

## Primary-name verification

A verification request runs:

1. load the claimed name from the currently admitted declared claim surface
2. normalize the claimed name using the recorded normalizer version
3. resolve the claimed name for the requested `coin_type`
4. compare the resolved target to the requested address
5. persist both the claim state and the verification result

The route keeps claim state separate from the execution-derived verification result. Both `claimed_primary_name` and `verified_primary_name` use `ResultStatus`. `claimed_primary_name` is limited to `success`, `not_found`, `unsupported`, `invalid_name`. `verified_primary_name` adds `mismatch` and `execution_failed`.

`mismatch` means the claim normalized, resolved for the requested `coin_type`, and produced a concrete target address that did not equal the requested one. A nonblank raw claim that cannot be normalized surfaces `invalid_name`; blank or whitespace-only is `not_found`. `raw_claim_name` is claim-local — it may be preserved to explain `claimed_primary_name.status=invalid_name` but does not migrate into `verified_primary_name`. When verification establishes a concrete normalized target, `verified_primary_name` may carry that name identity for `success` or `mismatch`; it is omitted otherwise.

### Claim sources

For ENS on Ethereum Mainnet, declared claim intake is reverse-only through `ens_v1_reverse_l1` at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`.[^v1-revreg-deploy][^v1-revreg-l15][^v1-revreg-l100][^v1-revreg-l123] Verification reuses `ens_execution` and the Universal Resolver proxy; declared claim ownership and verified execution ownership stay separate.[^v1-aur-l217][^v1-aur-l263][^v1-aur-l269]

For Basenames, declared claim intake is `basenames_base_primary` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282`.[^bn-readme-l33][^bn-revreg-l12][^bn-revreg-l150] It stays claim intake only — exact-name, address-name, and children declared truth remain on the Base registry/registrar/resolver families because upstream exposes reverse-name writes through the dedicated `ReverseRegistrar` rather than the Base authority stack. Verification runs through `basenames_execution` against the Mainnet `L1Resolver`; declared and verified ownership do not collapse.[^bn-readme-l22][^bn-l1resolver-l13][^bn-revreg-l193]

`claimed_primary_name.name`, when present from persisted state, comes from the exact requested `primary_names_current(address, coin_type, namespace)` row's declared normalized claim-identity source for that same tuple, including projection-owned legacy reverse-resolver hydration for configured event-silent ENSv1 reverse resolvers. Resolver-edge-only legacy hydration may create that exact row only when the hydrated reverse name resolves forward for `addr:60` through the ENS Universal Resolver at the same hash-pinned checkpoint to an ETH address whose computed `addr.reverse` node matches the candidate node; that forward check is claim-side address recovery, not persisted verified-primary execution.[^v1-revreg-l137][^v1-registry-l137][^v1-nameresolver-l7][^v1-iaddrres-l11][^v1-iur-l44][^v1-iur-l52] The app default tuple (`namespace=ens`, `coin_type=60`) may use an on-demand Ethereum Mainnet reverse RPC fallback when that persisted tuple is missing; the fallback builds the current `addr.reverse` node, reads its ENS registry resolver, calls resolver `name(bytes32)`, normalizes the value, and marks provenance as `ens_reverse_rpc`.[^v1-registry-deploy][^v1-revreg-l137][^v1-registry-l137][^v1-nameresolver-l7][^v1-nameresolverimpl-l25] In `mode=verified|both`, that route-local fallback also verifies the claimed name by executing `addr:60` through the ENS Universal Resolver proxy at provider `latest`; it does not persist an execution trace or populate `primary_names_current`.[^v1-ur-deploy][^v1-iur-l44][^v1-iur-l52] Other tuple claim sources are not synthesized from manifest presence, resolver-backed forward identity outside that resolver-edge recovery guard, verified execution identity, tuple presence alone, or a different tuple.[^v1-revreg-l74][^v1-revreg-l83][^v1-revreg-l84]

### Coverage and provenance

The exact-tuple verified-primary support class remains persisted readback for materialized tuples. Both the ENS slice and the first Basenames slice use it. The ENS/60 app fallback is the narrow route-local exception: it performs current reverse claim lookup and, for verified modes, current forward `addr:60` verification without creating `request_type=verified_primary_name` cache rows. Stable persisted execution identity is still `request_type=verified_primary_name`; the request key is the normalized tuple `{namespace}:{normalized_address}:{coin_type}`, where `normalized_address` uses the same lowercase normalization as `GET /v1/primary-names/{address}`. Claimed text, normalized identity, verified target, status, and section-local provenance are not part of the key.

Supported tuples may publish `coverage.status=partial` with `exhaustiveness=non_enumerable`. Tuples outside the frozen class remain explicit `unsupported`; they do not inherit coverage from manifest rollout, tuple presence, or verified-resolution support.

`primary_names_current(address, coin_type, namespace)` is the claim-side lookup and invalidation anchor for that tuple. Projection-owned claim state may explain tuple admission or claim invalidation but must not persist `execution_trace_id` or `verified_primary_name`.

Section-local provenance:

- `claimed_primary_name.provenance` is exact-tuple declared-only provenance from the requested row, including projection-owned legacy reverse-resolver hydration metadata when present, or route-local `ens_reverse_rpc` resolver provenance for the ENS/60 on-demand fallback. Persisted declared provenance strips `verified_primary_name_lookup` / `verified_primary_name_invalidation` hook material and omits `execution_trace_id`.
- `verified_primary_name.provenance`, when present, is `{execution_trace_id, manifest_versions}` for persisted readback. Its `execution_trace_id` must equal the top-level `provenance.execution_trace_id`; its `manifest_versions` must narrow that same persisted trace. ENS/60 on-demand verification omits this field because it has no persisted trace.
- Top-level route provenance joins claim-side and verification-side context. `verified_primary_name.provenance` does not publish lookup/invalidation hook material, restate claimed-row provenance, or introduce a second lookup/invalidation identity.

The shipped ENS and Basenames primary-name paths do not require dedicated manifest capability flags. Reverse claim admission stays under `ens_v1_reverse_l1` / `basenames_base_primary`; persisted verified-primary readback stays execution-derived under `ens_execution` / `basenames_execution`; the ENS/60 route-local fallback reports `ens_reverse_rpc` and `ens_execution_rpc` source classes when it performs live verification. Adding a dedicated capability flag would be additive, not a prerequisite.

## Trace schema

Each verified answer persists into `execution_traces`:

- `execution_trace_id`
- request type, request key
- namespace, chain positions, manifest versions
- step list
- contracts called, gateway digests
- final value, failure reason, finished timestamp

For resolution, one persisted answer may carry multiple selector-scoped outputs under the same `execution_trace_id`. For exact-tuple verified primary, one persisted answer covers exactly one `{address, namespace, coin_type}` tuple under `request_type=verified_primary_name`.

Each step row in `execution_steps` records:

- step index, step kind
- input digest, output digest
- latency
- canonicality dependency

Admitted exact block-anchored `raw_call_snapshots` are not part of this schema. They remain intake-owned raw facts keyed by exact block identity even when verified-resolution persistence hands them off alongside the trace.

Execution traces and steps are durable audit artifacts. Reorg-driven cache invalidation does not delete `execution_traces`, `execution_steps`, object-store attachments, or the trace-local step list — it only changes whether a persisted outcome is reusable as a cache hit.

### Worker inspection

`bigname-worker inspect execution-trace --execution-trace-id <id> --json` is the worker-owned operational read for one persisted trace. The JSON output is limited to already persisted state: `command`, `execution_trace_id`, request metadata, request type and key, namespace, chain positions, manifest versions, trace status, final value digest, failure reason, finished timestamp, and ordered `steps` entries with index, kind, input digest, output digest, latency, canonicality dependency, and attachment digest metadata.

The command reads `execution_traces`, `execution_steps`, and trace attachment metadata only. It does not execute or re-execute resolution, primary-name verification, CCIP calls, or topology discovery; it does not expose a public `v1` route, raw execution API, raw gateway transcript, or batch trace dump; it does not synthesize topology or resolver/wildcard/alias/transport paths from non-trace storage; it does not mutate cache, projections, manifests, discovery, watch plans, or normalized events. The public explain boundary stays intact: `GET /v1/explain/resolutions/{namespace}/{name}/execution` remains the route-local explain view; this command is operational read-only inspection.

## Cache identity and invalidation

Persisted outcomes live in `execution_cache_outcomes`, keyed by:

- request key
- requested chain positions
- manifest versions
- topology version boundary
- record version boundary

For resolution, the request key includes the normalized explicit selector set so the cache boundary matches `verified_queries`. For `GET /v1/profiles/names/{name}`, the resolution request key is built from the inferred namespace, normalized name, and normalized selector set. Namespace inference does not create a separate cache namespace.

For verified primary, the request key is the normalized tuple `{namespace}:{normalized_address}:{coin_type}`. The matching `primary_names_current(address, coin_type, namespace)` row is the only admitted claim-side lookup/invalidation anchor; projection updates may invalidate request-matching answers but the projection does not persist verified payloads or trace IDs.

Invalidate on:

- reorg
- manifest change
- resolver change
- alias or wildcard topology change
- relevant record change
- primary claim change

Reorg invalidation rules: reorg repair invalidates any `execution_cache_outcomes` row whose dependency set contains an orphaned block identity. Cache dependencies must tie to explicit block-hash-bearing chain positions or boundaries; block numbers, `latest`/`head` tags, manifest versions, topology versions, and record versions are not sufficient unless they resolve to one or more block hashes or to source rows that carry block hashes. Verified resolution and verified primary-name rows without explicit block-hash-bearing dependencies fail closed and are ineligible for cache reuse after a reorg check. Request types documented as not depending on chain state remain explicitly out of scope rather than implicitly safe. Invalidation affects cache eligibility only; traces, steps, and attachments stay durable.

## Explain

Every verified answer must be explainable through the selected entrypoint, resolver discovery path, wildcard traversal, alias rewriting, CCIP steps, and the final comparison or returned record value. The shipped explain surface for resolution is `GET /v1/explain/resolutions/{namespace}/{name}/execution`.

It is keyed by the same current exact surface and explicit selector set as the mixed route, reads the persisted trace and selector-scoped results, and does not re-execute or synthesize from declared topology alone. Top-level provenance and any selector-local provenance anchor to the same persisted `execution_trace_id`. The route surfaces the selected entrypoint, resolver discovery path, wildcard traversal, alias rewriting, and the ordered persisted step summary; CCIP-Read participation appears through persisted step kinds, not a raw gateway transcript. It is published in `docs/api-v1.openapi.json`. The current handler exposes path parameters plus required `records` only.

Public explain support stays coupled to the same verified-resolution support boundary as the mixed route; deferred unsupported path classes do not gain a synthetic trace-shaped public contract. For Basenames, the public execution-explain boundary applies only to execution explain; the separate declared exact-name explain routes stay on the Base-side declared read plane.[^bn-readme-l70]

## Support boundary

ENS verified resolution on Ethereum Mainnet uses `ens_execution` at the Universal Resolver proxy.[^ens-docs-univ][^v1-aur-l90][^v1-aur-l106] Public verified support covers three exact-surface path classes against the same declared topology snapshot used by the mixed route:

- **Direct path** — `resolver_path[0].logical_name_id` equals top-level `data.logical_name_id`; `wildcard.source` is `null` with `matched_labels=[]`; `alias.final_target` is `null` with `hops=[]`; all `transport` fields are `null`.
- **Alias-only non-direct** — same shape, except `alias.final_target` is non-`null` with non-empty `hops`.
- **Wildcard-derived** — `wildcard.source` is non-`null` with non-empty `matched_labels`; `resolver_path[0].logical_name_id` equals `wildcard.source.logical_name_id`; `alias.final_target` is `null` with `hops=[]`; `subregistry_path=[]`; all `transport` fields are `null`.

All three flow through the same persisted execution trace and explain contract: explain surfaces the selected entrypoint, resolver discovery path, ordered persisted steps, and any participating alias or wildcard detail without a second trace family.

ENS requests outside these classes — including non-alias ancestor-selected paths, linked-subregistry ancestor-selected paths, any transport-assisted path, and any request whose persisted execution used CCIP-Read — return selector-local `status=unsupported`. The explain route does not synthesize public traces for them.

Basenames verified resolution on the shipped mainnet profile uses active `basenames_execution` v2 at the L1 Resolver for the exact-surface transport-assisted direct-path class:[^bn-readme-l22][^bn-readme-l69][^bn-readme-l70][^bn-l1resolver-l13]

- `resolver_path[0].logical_name_id` equals top-level `data.logical_name_id`
- `wildcard.source=null`, `matched_labels=[]`
- `alias.final_target=null`, `hops=[]`
- `subregistry_path=[]`
- `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"`

CCIP-participating traces are eligible for that class rather than `unsupported`, because upstream `L1Resolver` initiates `OffchainLookup` for non-`base.eth` requests and completes them through `resolveWithProof`.[^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191] Explain surfaces the resulting persisted CCIP steps without inventing a second trace family. Other Basenames paths remain `unsupported`. The verified-resolution boundary does not widen route-level primary-name coverage beyond the exact-tuple persisted-readback class and does not add manifest flags.

`GET /v1/profiles/names/{name}` does not widen this boundary. Inferred Basenames verified selectors return `unsupported` unless the requested snapshot satisfies the same frozen Basenames class.

ENS and Basenames primary-name coverage is graduated for the exact-tuple persisted-readback class, including projection-owned legacy reverse-resolver hydration for configured event-silent ENSv1 reverse resolvers, and the ENS/60 app fallback is graduated for on-demand reverse RPC claim lookup plus route-local forward `addr:60` verification when verified mode requests it. Supported classes return `coverage.status=partial` with `exhaustiveness=non_enumerable`. Out-of-class tuples, richer claimed payloads, non-ENS/60 fresh verified-primary execution, and namespace-wide claims remain `unsupported` or out of scope. Manifest rollout, capability state, reverse-tuple lookup, and resolver-backed verification detail do not by themselves widen the verified-readback contract.

Declared resolver-profile gaps remain requestable and explicit on the declared read plane; they do not by themselves make a supported verified-resolution path unsupported. Supported Universal Resolver selectors read matching persisted output or execute on demand at the selected snapshot, then persist and return the outcome.[^v1-iur-l44][^v1-iur-l52]

---

[^ens-docs-univ]: <https://docs.ens.domains/resolvers/universal/> (official Universal Resolver proxy)

[^v1-ur-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)
[^v1-ursol-l8]: (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f)
[^v1-iur-l44]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L44 @ ens_v1@91c966f)
[^v1-iur-l52]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L52 @ ens_v1@91c966f)
[^v1-iaddrres-l11]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L11 @ ens_v1@91c966f)
[^v1-aur-l90]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L90 @ ens_v1@91c966f)
[^v1-aur-l106]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L106 @ ens_v1@91c966f)
[^v1-aur-l217]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L217 @ ens_v1@91c966f)
[^v1-aur-l263]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L263 @ ens_v1@91c966f)
[^v1-aur-l269]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L269 @ ens_v1@91c966f)

[^v1-revreg-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-revreg-l15]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f)
[^v1-revreg-l74]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f)
[^v1-revreg-l83]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f)
[^v1-revreg-l84]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
[^v1-revreg-l100]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f)
[^v1-revreg-l123]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f)
[^v1-registry-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ENSRegistry.json:L2 @ ens_v1@91c966f)
[^v1-revreg-l137]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L137 @ ens_v1@91c966f)
[^v1-registry-l137]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137 @ ens_v1@91c966f)
[^v1-nameresolver-l7]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L7 @ ens_v1@91c966f)
[^v1-nameresolverimpl-l25]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L25 @ ens_v1@91c966f)

[^bn-readme-l22]: (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc)
[^bn-readme-l33]: (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc)
[^bn-readme-l69]: (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)
[^bn-readme-l70]: (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
[^bn-l1resolver-l13]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
[^bn-l1resolver-l154]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)
[^bn-l1resolver-l173]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc)
[^bn-l1resolver-l191]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)
[^bn-revreg-l12]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
[^bn-revreg-l150]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
[^bn-revreg-l193]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
