# Consumer Capability Baseline

Status: Phase 0 baseline

This document is the checked-in replacement contract for first-party consumers until the apps monorepo is imported and mapped call-site by call-site.

## 1. Capability Groups

| Capability | Example consumer surface | Native `v1` responsibility |
| --- | --- | --- |
| exact name profile | profile pages, record editing, registration views | `Name.registration` + `Resolution` |
| names owned / controlled by address | dashboards and search flows | `Address.names` |
| names owned / controlled by address with role summary | dashboard lists | `Address.names` with `include=role_summary` |
| declared child subnames and counts | subname pages and creation flows | `Name.children` |
| record inventory for editing | profile and records screens | `Resolution.record_inventory` + `Resolution.record_cache` |
| verified record reads | profile, send, and address-resolution flows | `Resolution.verified_queries` |
| name history | profile history pages | `History(scope=both)` |
| address history across names | address activity views | `Address.history` |
| role holders for a resource | roles pages | `Permissions.by_resource` |
| role change history | roles history pages | `History(filter=permissions)` |
| resolver-centric overview | resolver pages | `Resolver` |
| claimed vs verified primary name | dashboard and profile | `PrimaryName.claimed_primary_name` + `PrimaryName.verified_primary_name` |

## 2. Current Status

- this is the working baseline for Phase 0
- `Address.names` with `include=role_summary` is an additive expansion of the same address-to-surface collection; it is not a separate route or replacement surface
- `Address.history` is the declared-state address activity read over address-derived surface and resource anchors; it reuses the shared history contract rather than introducing a separate truth system
- `Resolution` is one mixed route: `record_inventory` defines the known record-selector space, `record_cache` is the declared last-known-value view over that same selector space, and `verified_queries` is the explicit request-bound execution answer set
- `GET /v1/resolve/{name}` is a convenience entry to the same `Resolution` capability, not a separate consumer capability: exact `base.eth` infers `namespace=ens`, names matching `*.base.eth` infer `namespace=basenames`, other supported ENS names infer `namespace=ens`, and the response still exposes canonical namespaced identity through fields such as `data.namespace` and `data.logical_name_id`
- namespace-inferred resolution does not change verified-support meaning: inferred Basenames requests use Basenames-local selector and topology support, return selector-local `unsupported` outside the exact-surface transport-assisted direct-path support class, and do not fall back to ENS
- ENSv2 exact-name profile support is promoted only for the selected `sepolia-dev` deployment profile when `ens_v2_registrar_l1` declares `exact_name_profile = "supported"`; that support class covers declared exact-name profile reads from the admitted `ETHRegistry` and `ETHRegistrar` sources, and it does not graduate resolver-profile support, verified resolution, primary-name support, history coverage, other deployment profiles, or consumer replacement for unrelated capability groups (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309).
- ENSv1 and Basenames `Resolution.record_inventory`, `Resolution.record_cache`, and resolver-centric overview are not consumer-replacement complete merely because resolver discovery admits registry-observed resolver addresses. A current resolver target in registry state is insufficient by itself; resolver-local facts must come from a direct manifest-admitted or discovery-admitted resolver contract whose supported profile is admitted for the relevant record family (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc).
- ENSv1 dynamic resolver-profile admission is PublicResolver-compatible only in this phase. A discovered resolver that is watched but has `pending` or `unsupported` profile state remains topology-only and does not satisfy `Resolution.record_inventory`, `Resolution.record_cache`, or resolver-centric overview cutover evidence. Basenames resolver-profile admission is governed by the separate Phase 8 Basenames gate below (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f).
- Basenames dynamic resolver-profile admission is Base-side `L2Resolver`-compatible only in this phase. A discovered Base resolver that is watched but has `pending` or `unsupported` profile state remains topology-only and does not satisfy `Resolution.record_inventory`, `Resolution.record_cache`, or resolver-centric overview cutover evidence. This gate is separate from ENSv1 PublicResolver-compatible admission and from Basenames L1 transport / execution, so the Ethereum Mainnet `L1Resolver`, active `basenames_execution` verified-resolution support, and any offchain gateway do not satisfy declared resolver-profile evidence (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc).
- `PrimaryName` is one mixed route: `claimed_primary_name` is the declared claim candidate and `verified_primary_name` is the execution-derived verification result
- `PrimaryName` local route coverage is graduated only for the frozen ENS/Basenames exact-tuple persisted-readback classes: route-level coverage is `partial` for those tuples and explicit `unsupported` outside them; this is local bigname evidence, not external app parity or first-party app call-site replacement
- deterministic native bigname golden fixtures are local cutover evidence only. They can prove that the native route/conformance contract is stable for a capability group, but they do not prove external app parity, imported app call-site replacement, first-party app replacement, legacy schema parity, or consumer replacement before app call sites are mapped separately.
- both mixed routes reuse the same `ResultStatus` vocabulary: `success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, `execution_failed`
- wrapper/resolver/Basenames source-family backfill conformance proves only that completed source-family job lifecycle state for the admitted ENSv1 NameWrapper, ENSv1 PublicResolver, and Basenames source families can coexist with replayed existing shipped consumer-capability responses. It does not prove that synthetic jobs admitted route data, add a capability group, graduate unsupported coverage, expand the selected ENSv2 exact-name support promotion to other profiles, claim wrapper/migration history support, change manifest capabilities, add public API routes, or change consumer-replacement meaning (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc).
- when the apps monorepo is imported, add app-by-app call-site mappings rather than replacing this table
- any capability required by a first-party consumer that is not covered here must be added here before code claims parity

## 3. Exit Condition For Consumer Cutover

Before first-party cutover:

- each capability must map to one or more concrete app call sites
- each capability must have contract tests
- each capability must have rollout and rollback criteria

## 4. Local Cutover Evidence Model

This model records local bigname evidence only. It is necessary for cutover planning, but it is not an external app parity claim and does not prove imported app call-site replacement or first-party app replacement until the imported app call sites are mapped separately.

The golden fixture pack is deterministic native bigname evidence for this table. It must stay tied to the listed native route owner, conformance owner, rollout gate, and rollback gate; adding or refreshing those fixtures does not create app parity, imported app call-site replacement, first-party app replacement, legacy schema parity, or consumer-replacement claims beyond the local route/conformance boundary.

| Capability group | Route owner | Conformance owner | Rollout gate | Rollback gate |
| --- | --- | --- | --- | --- |
| exact name profile | Projections and API: `GET /v1/names/{namespace}/{name}` and `GET /v1/coverage/{namespace}/{name}` | Conformance and Golden Fixtures: exact-name and coverage capability checks | OpenAPI route owner guard, focused exact-name conformance, release smoke, and matching manifest support state where applicable | rollback smoke plus route envelope returning the prior supported / unsupported coverage state |
| names owned / controlled by address | Projections and API: `GET /v1/addresses/{address}/names` | Conformance and Golden Fixtures: address-name collection checks | OpenAPI route owner guard, focused collection conformance, and release smoke | rollback smoke plus stable cursor / response-shape rollback for the address collection |
| names owned / controlled by address with role summary | Projections and API: `GET /v1/addresses/{address}/names?include=role_summary` | Conformance and Golden Fixtures: address-name role-summary checks | OpenAPI route owner guard, role-summary conformance, and release smoke | rollback smoke plus preserving the base address collection without role-summary widening |
| declared child subnames and counts | Projections and API: `GET /v1/names/{namespace}/{name}/children` | Conformance and Golden Fixtures: children collection checks | OpenAPI route owner guard, focused children conformance, and release smoke | rollback smoke plus stable child collection pagination / unsupported behavior |
| record inventory for editing | Projections and API: `GET /v1/resolutions/{namespace}/{name}` declared `record_inventory` | Conformance and Golden Fixtures: resolution inventory checks | OpenAPI route owner guard, focused resolution conformance, supported resolver-profile evidence, and release smoke | rollback smoke plus explicit unsupported / pending resolver-family state rather than silent omission |
| verified record reads | Projections and API plus Verified Execution: `GET /v1/resolutions/{namespace}/{name}` verified queries and execution explain | Conformance and Golden Fixtures: verified resolution and execution-explain checks | OpenAPI route owner guard, focused verified-resolution conformance, execution trace persistence checks, and release smoke | rollback smoke plus execution cache invalidation / unsupported verified-query behavior remaining stable |
| name history | Projections and API: `GET /v1/history/names/{namespace}/{name}` | Conformance and Golden Fixtures: name history checks | OpenAPI route owner guard, focused history conformance, replay stability checks, and release smoke | rollback smoke plus stable cursor and canonical-only history behavior |
| address history across names | Projections and API: `GET /v1/history/addresses/{address}` | Conformance and Golden Fixtures: address history checks | OpenAPI route owner guard, focused address-history conformance, replay stability checks, and release smoke | rollback smoke plus stable address-anchor selection and pagination behavior |
| role holders for a resource | Projections and API: `GET /v1/resources/{resource_id}/permissions` | Conformance and Golden Fixtures: resource permissions checks | OpenAPI route owner guard, focused permissions conformance, and release smoke | rollback smoke plus stable permission envelope and explicit unsupported summaries |
| role change history | Projections and API: `GET /v1/history/resources/{resource_id}` with permission events | Conformance and Golden Fixtures: resource history / permissions-history checks | OpenAPI route owner guard, focused history conformance, and release smoke | rollback smoke plus stable resource-history cursor behavior |
| resolver-centric overview | Projections and API: `GET /v1/resolvers/{chain_id}/{resolver_address}` | Conformance and Golden Fixtures: resolver overview checks | OpenAPI route owner guard, focused resolver conformance, supported resolver-profile evidence, and release smoke | rollback smoke plus `UnsupportedSummary` for pending / unsupported resolver profiles |
| claimed vs verified primary name | Projections and API plus Verified Execution: `GET /v1/primary-names/{address}` | Conformance and Golden Fixtures: primary-name checks | OpenAPI route owner guard, focused primary-name conformance, persisted execution readback checks, and release smoke | rollback smoke plus stable partial exact-tuple coverage, explicit unsupported out-of-class behavior, and execution-derived verification rollback |
