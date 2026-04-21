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
- namespace-inferred resolution does not change verified-support meaning: inferred Basenames requests use Basenames-local selector and topology support, return selector-local `unsupported` when that support is unavailable, and do not fall back to ENS
- ENSv2 exact-name profile support is promoted only for the selected `sepolia-dev` deployment profile when `ens_v2_registrar_l1` declares `exact_name_profile = "supported"`; that support class covers declared exact-name profile reads from the admitted `ETHRegistry` and `ETHRegistrar` sources, and it does not graduate resolver-profile support, verified resolution, primary-name support, history coverage, other deployment profiles, or consumer replacement for unrelated capability groups (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309).
- ENSv1 and Basenames `Resolution.record_inventory`, `Resolution.record_cache`, and resolver-centric overview are not consumer-replacement complete merely because resolver discovery admits registry-observed resolver addresses. A current resolver target in registry state is insufficient by itself; resolver-local facts must come from a direct manifest-admitted or discovery-admitted resolver contract whose supported profile is admitted for the relevant record family (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc).
- `PrimaryName` is one mixed route: `claimed_primary_name` is the declared claim candidate and `verified_primary_name` is the execution-derived verification result
- both mixed routes reuse the same `ResultStatus` vocabulary: `success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, `execution_failed`
- wrapper/resolver/Basenames source-family backfill conformance proves only that completed source-family job lifecycle state for the admitted ENSv1 NameWrapper, ENSv1 PublicResolver, and Basenames source families can coexist with replayed existing shipped consumer-capability responses. It does not prove that synthetic jobs admitted route data, add a capability group, graduate unsupported coverage, expand the selected ENSv2 exact-name support promotion to other profiles, claim wrapper/migration history support, change manifest capabilities, add public API routes, or change consumer-replacement meaning (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc).
- when the apps monorepo is imported, add app-by-app call-site mappings rather than replacing this table
- any capability required by a first-party consumer that is not covered here must be added here before code claims parity

## 3. Exit Condition For Consumer Cutover

Before first-party cutover:

- each capability must map to one or more concrete app call sites
- each capability must have contract tests
- each capability must have rollout and rollback criteria
