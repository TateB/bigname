# Source Manifests

Status: Phase 0 baseline

This document freezes how `bigname` represents source manifests, capability flags, and discovery admission.

## 1. Purpose

Manifests make watched contracts, capability support, and rollout state explicit. They are part of the truth model, not deploy-time configuration only.

## 2. File Format And Location

Manifests live in the repository as TOML:

```text
manifests/<namespace>/<source_family>/<version>.toml
```

Alternate deployment profiles use the same schema under a profile-specific repository root. The first ENSv2 alternate profile is `sepolia-dev`:

```text
manifests-sepolia-dev/<namespace>/<source_family>/v1.toml
```

One runtime selects exactly one manifest root at startup, such as `manifests/` for the shipped mainnet profile or `manifests-sepolia-dev/` for the ENSv2 Sepolia dev profile. Profile selection is not a manifest schema change, and a selected runtime must not load both roots into one canonical corpus, watch plan, discovery graph, or projection set.

Reasons for TOML:

- deterministic diffs
- easy hand-editing during protocol bootstrap
- straightforward Rust parsing

## 3. Required Fields

Each manifest contains:

- `manifest_version`
- `namespace`
- `source_family`
- `chain`
- `deployment_epoch`
- `rollout_status`
- `normalizer_version`
- `capability_flags`
- `roots`
- `contracts`
- `discovery_rules`

### `rollout_status`

- `draft`
- `shadow`
- `active`
- `deprecated`

### `capability_flags`

Capabilities are named and versioned. Each flag records:

- capability name
- status: `unsupported`, `shadow`, `supported`
- optional notes

### `chain`

- `chain` names the authority chain for that manifest within the selected deployment profile
- the shipped baseline uses mainnet chain IDs such as `ethereum-mainnet` and `base-mainnet`
- later Sepolia and `sepolia-dev` support is additive as a separate manifest root and chain-ID set, not a concurrent extension of the same runtime
- one runtime loads manifests from exactly one deployment profile at a time; it must not combine mainnet and Sepolia / `sepolia-dev` manifests in one canonical corpus, watch plan, discovery graph, or projection set

## 4. Example Shape

```toml
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "uts46-v1"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000000"
code_hash = "sha256:..."
abi_ref = "abis/ens_v2_root_registry.json"

[[contracts]]
role = "registry"
address = "0x0000000000000000000000000000000000000000"
proxy_kind = "none"
# Omit `implementation` when `proxy_kind = "none"`.

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

[capability_flags]
declared_children = "supported"
```

Capability ownership is source-family specific. For ENS verified resolution on Ethereum Mainnet, the authoritative execution manifest is `ens_execution`, not `ens_v1_registry_l1`. Its canonical contract entry is the ENS Universal Resolver.

Relevant manifest fields for that execution family:

```toml
source_family = "ens_execution"
chain = "ethereum-mainnet"

[[contracts]]
role = "universal_resolver"
address = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"

[capability_flags]
verified_resolution = "shadow"
```

That freeze attaches `verified_resolution` ownership to `ens_execution`. It uses the official ENS Universal Resolver proxy address for the manifest entrypoint (official ENS docs: https://docs.ens.domains/resolvers/universal/). The pinned ENSv1 deployment artifact remains the implementation / ABI anchor for the source family, not the route-facing proxy address. The freeze allows shadow execution traces and cache ownership without implying that public verified-resolution reads are already shipped (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f).

The shipped Phase 7 ENS primary-name slices do not add a second execution capability flag here. `ens_execution` remains the execution owner for exact-tuple persisted `verified_primary_name` readback, but the route-level coverage contract is frozen in the API, execution, and projection docs rather than through a dedicated `verified_primary_name` manifest flag. The existing `verified_resolution = "shadow"` flag admits the shared shadow execution substrate only; it does not silently widen into a second manifest capability or address-wide primary-name coverage. If a later milestone needs dedicated manifest gating for verified primary-name reads, that flag would be an additive doc-first change.

ENS reverse-claim intake follows the same source-family discipline. For Ethereum Mainnet, declared primary-claim intake is anchored to `ens_v1_reverse_l1`, not `ens_v1_registry_l1` or `ens_v1_resolver_l1`. Its canonical contract entry is the Ethereum `addr.reverse` Reverse Registrar (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L19 @ ens_v1@91c966f).

Relevant manifest fields for that reverse family:

```toml
source_family = "ens_v1_reverse_l1"
chain = "ethereum-mainnet"

[[contracts]]
role = "reverse_registrar"
address = "0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb"
```

That freeze fixes the authoritative reverse entrypoint, source-family owner, and reverse-only intake precedence for ENS primary-claim support. It does not define a new capability flag, does not add manifest schema, does not authorize fallback to registry-, resolver-, or other claim-setting surfaces, and does not by itself widen public primary-name reads beyond the exact-tuple persisted-readback contract frozen at the route level (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f).

Within the claimed-vs-verified primary-name contract, that reverse family owns only the declared claim intake. The truth split stays explicit: `ens_v1_reverse_l1` admits the authoritative reverse claim source, while any verified primary-name result remains execution-derived through the execution owner already frozen above. The current reverse manifest may therefore be `rollout_status = "active"` with no dedicated primary-name capability flag at all. That combination is sufficient for the namespace-local ENS exact-tuple persisted-readback coverage class when paired with persisted `ens_execution` readback, but it must not widen coverage beyond that requested tuple, imply address-wide or app-parity primary-name support, admit fallback claim sources, or combine the admitted reverse tuple with resolver-backed or execution-derived name identity to fill richer ENS `claimed_primary_name` payloads (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f).

That absence is intentional for the shipped Phase 7 route: `ens_v1_reverse_l1` does not need a dedicated `claimed_primary_name`, `primary_name_claim`, or similar capability flag to admit the declared reverse-claim tuple, and `ens_execution` does not need a dedicated `verified_primary_name` flag for persisted exact-tuple readback. Later primary-name capability flagging, if ever needed, would be additive and would have to preserve the existing truth split between reverse-owned declared intake, execution-derived verification, and route-local exact-tuple coverage.

ENSv1 Phase 4 NameWrapper and PublicResolver admission is frozen to two source-family owners on the shipped mainnet profile:

- `ens_v1_wrapper_l1` owns the Ethereum Mainnet NameWrapper contract role `name_wrapper` at `0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401`, wrapper-backed authority observations, wrapper owner / fuse / expiry facts, wrapper-revealed DNS-encoded names, and wrapper-driven registry owner / resolver / TTL changes for currently admitted ENSv1 names. The upstream wrapper exposes `NameWrapped`, `NameUnwrapped`, `FusesSet`, and `ExpiryExtended` events, plus wrap, unwrap, fuse, subnode, resolver, TTL, `ownerOf`, and `getData` functions; those surfaces are admitted as adapter input only (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L54 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L80 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L90 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L102 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L138 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L140 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L142 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L148 @ ens_v1@91c966f).
- `ens_v1_resolver_l1` owns the Ethereum Mainnet PublicResolver contract role `public_resolver` at `0xF29100983E058B709F3D539b0c765937B804AC15`, declared resolver record state, resolver record-version observations, and resolver-local authorization facts for admitted ENSv1 names. The upstream resolver composes the ABI, address, contenthash, data, DNS, interface, name, pubkey, and text resolver profiles, stores its NameWrapper dependency in the constructor, and authorizes either trusted controllers / reverse registrar, the registry owner, the wrapped owner, an approved operator, or an approved delegate (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L7 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L8 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L9 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L10 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L11 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L13 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L66 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f).

Those admissions are Phase 4 declared-state input boundaries only. `ens_v1_wrapper_l1` may append identity, preimage, authority, fuse, expiry, permission, resolver-change, and wrapper-token lineage normalized events; `ens_v1_resolver_l1` may append resolver record, record-version, and resolver authorization normalized events. Neither family writes projection rows, mutates manifest capability state from observed logs, graduates route coverage, admits wrapper upgrade / migration history, adds a public route, or claims consumer replacement by source-family presence alone. Any later wrapper migration / upgrade support, fallback primary-claim source, route coverage graduation, or new public read surface is additive and doc-first.

ENSv1 dynamic resolver indexing is required for consumer-usable declared record coverage. The mainnet PublicResolver manifest entry is a bootstrap seed, not the complete ENSv1 resolver universe. The admitted resolver discovery rule from the ENSv1 registry source to `ens_v1_resolver_l1` treats each canonical nonzero `NewResolver(node, resolver)` observation from an admitted registry emitter as both a node-to-resolver binding update and resolver contract-instance admission through a discovery edge; a zero-address resolver observation closes only that node-to-resolver binding. The upstream registry exposes `NewResolver(bytes32 indexed node, address resolver)`, emits it from `setResolver`, and also emits it when `setRecord` / `setSubnodeRecord` change resolver state through `_setResolverAndTTL` (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f). Resolver discovery admission is not resolver-profile support: a registry-selected arbitrary address may be watched and represented as a `contract_instance_id`, but typed record, record-version, and resolver-local authorization facts may be appended only after a supported resolver profile is admitted for that instance and record family.

The Phase 4 dynamic ENSv1 supported-profile gate is narrow and PublicResolver-compatible only. A discovered resolver instance may move from pending / unsupported profile state to supported profile state only when the resolver-profile admission logic explicitly matches it to the PublicResolver-compatible profile for the relevant fact families through stored code-hash facts, implementation-edge facts, or another explicit non-schema admission rule. The profile is compatible with the upstream PublicResolver surface that composes ABI, address, contenthash, data, DNS, interface, name, pubkey, and text resolver mixins; stores NameWrapper and trusted-controller dependencies; exposes ERC165 support over those mixins; and inherits `ResolverBase` record-versioning (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L13 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L66 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L75 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L23 @ ens_v1@91c966f). Unknown dynamic resolvers remain admitted watch targets only: their resolver-profile state must stay explicit `pending` or `unsupported`, and ENSv1 record inventory, record cache, and resolver overview must not claim consumer replacement for names whose current resolver lacks supported PublicResolver-compatible profile admission. Basenames resolver-profile admission is governed by the separate Phase 8 Basenames gate in this section.

ENSv2 Phase 5 source-family ownership for the `sepolia-dev` alternate profile is frozen to four admitted families under `manifests-sepolia-dev/ens/...`:

- `ens_v2_root_l1` owns the `RootRegistry` manifest root at `0x3a3e15a5d27ff6f05c844313312f2e72096d3ed3` and the root-registry event surface needed to seed registry discovery and parent graph state; the upstream registry is a tokenized, resource-scoped permissioned registry with separate resource and token version counters (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L22 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L28 @ ens_v2@554c309).
- `ens_v2_registry_l1` owns the `ETHRegistry` manifest root at `0x796fff2e907449be8d5921bcc215b1b76d89d080` plus discovered `UserRegistry` proxy instances admitted through registry discovery edges; the checked-in `UserRegistryImpl` deployment at `0xea93aff7375e8176053ab6ab36b57cab53cbf702` is implementation metadata for proxy-backed user registries, not a separate public source-family owner (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/UserRegistryImpl.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L15 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L59 @ ens_v2@554c309).
- `ens_v2_registrar_l1` owns the `.eth` registrar deployment `ETHRegistrar` at `0x68586418353b771cf2425ed14a07512aa880c532`; registrar events and commit/renew facts stay in this family, while the actual registered-name resource identity is linked back to the permissioned registry resource emitted by the registry (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L49 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L173 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309).
- `ens_v2_resolver_l1` owns `PermissionedResolver` resolver state, alias events, record-version events, and resolver-scoped EAC permission facts; `PermissionedResolverImpl` at `0xe566a1fbaf30ff7c39828fe99f955fc55544cb9c` is the initial implementation artifact for discovered resolver instances, while resolver instances themselves enter the source graph through manifest or discovery admission (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/PermissionedResolverImpl.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L38 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L70 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L121 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L221 @ ens_v2@554c309).

The ENSv2 exact-name profile promotion is scoped to the selected `sepolia-dev` deployment profile only. In that profile, `ens_v2_registrar_l1` may carry `exact_name_profile = "supported"` to graduate the declared exact-name profile read for `.eth` names backed by the admitted `ETHRegistry` and `ETHRegistrar` sources; the registry deployment supplies the resource/token state and token-resource linkage, while the registrar deployment supplies commit, registration, renewal, and label-bearing lifecycle facts (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L22 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L19 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309). The promotion does not apply to the shipped mainnet profile, any other Sepolia profile, or any runtime that has not selected the `sepolia-dev` manifest root. It also does not graduate ENSv2 resolver-profile support, universal-resolver / execution support, reverse support, DNS support, wrapper support, migration support, history coverage, verified resolution, primary-name support, or consumer replacement for those capabilities. `rollout_status = "active"`, registry admission, registrar admission, resolver-family admission, preimage observations, and backfill completion are intake readiness inputs; outside an explicit `exact_name_profile = "supported"` capability on `ens_v2_registrar_l1` for the selected `sepolia-dev` profile, exact-name profile reads must remain unsupported or shadow as declared by the active manifest.

The Phase 5 split maps upstream `TokenResource(tokenId, resource)` logs to normalized `TokenResourceLinked`, upstream `TokenRegenerated(oldTokenId, newTokenId)` logs to normalized `TokenRegenerated`, upstream `SubregistryUpdated` logs to normalized `SubregistryChanged`, upstream `ParentUpdated` logs to normalized `ParentChanged`, upstream `AliasChanged` logs to normalized `AliasChanged`, and upstream `EACRolesChanged` logs to resource- or resolver-scoped permission events. Those mappings are adapter semantics, not manifest schema fields (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309).

This freeze does not create current source-family owners for the additional `sepolia-dev` deployment artifacts such as the ENSv2 universal resolver, reverse registries, DNS resolvers, wrapper implementation, migration controllers, factory contracts, oracle contracts, batch registrars, or payment mocks. Later admission of those surfaces would be additive and doc-first (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/UniversalResolverV2.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ReverseRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/DNSAliasResolver.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/WrapperRegistryImpl.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/LockedMigrationController.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/HCAFactory.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/StandardRentPriceOracle.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/BatchRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/MockUSDC.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/MockDAI.json:L2 @ ens_v2@554c309).

Basenames source-family ownership on the shipped mainnet profile is frozen across six admitted families (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc):

- `basenames_base_registry` owns registry-controlled declared authority through contract role `registry` at `0xb94704422c2a1e396835a571837aa5ae53285a95` on Base Mainnet because the upstream Registry stores per-node owner / resolver / ttl state and authorizes owner-controlled subnode and resolver updates there (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L10 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L100 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L113 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
- `basenames_base_registrar` owns tokenized registrar authority through contract role `registrar` at `0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a` on Base Mainnet because the upstream BaseRegistrar owns `base.eth`, mints `*.base.eth` subdomains, and can reclaim Base registry ownership after token transfers (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L15 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L17 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L237 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L327 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L423 @ basenames@1809bbc)
- `basenames_base_resolver` owns Base-native declared resolver state through contract role `resolver` at `0xC6d566A56A1aFf6508b41f6c90ff131615583BCD` on Base Mainnet because the upstream `L2Resolver` is the default Base resolver and authorizes the registrar controller, reverse registrar, and name owners / delegates to modify resolver records (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L49 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L52 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)
- `basenames_base_primary` owns declared primary-claim intake through contract role `reverse_registrar` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282` on Base Mainnet because the upstream ReverseRegistrar establishes reverse records and writes the claimed `name()` value there (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
- `basenames_l1_compat` owns L1 compatibility transport through contract role `l1_resolver` at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` on Ethereum Mainnet because upstream assigns cross-chain resolution for the `base.eth` domain to the Ethereum Mainnet `L1Resolver` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
- `basenames_execution` owns verified-resolution entrypoint selection through contract role `l1_resolver` at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` on Ethereum Mainnet with active v2 `verified_resolution = "supported"` only for the exact-surface transport-assisted direct-path class because the same upstream `L1Resolver` is the execution entrypoint that initiates `OffchainLookup` and completes the verified callback through `resolveWithProof` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)

That freeze maps declared authority to the Base registry / registrar / resolver families, declared primary to `basenames_base_primary`, compatibility transport to `basenames_l1_compat`, and execution entrypoint selection to `basenames_execution` (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc).

Basenames dynamic resolver indexing follows the same manifest/discovery rule on Base Mainnet. `basenames_base_resolver` starts with the upstream default `L2Resolver`, but it must not be treated as the only possible Base-side resolver for consumer replacement. The admitted resolver discovery rule from `basenames_base_registry` to `basenames_base_resolver` treats each canonical nonzero registry `NewResolver(node, resolver)` observation as both a node-to-resolver binding update and Base resolver contract-instance admission through a discovery edge; a zero-address resolver observation closes only that node-to-resolver binding. Upstream Basenames registry stores resolver addresses per node, emits `NewResolver` from `setResolver`, and emits it when `setRecord` / `setSubnodeRecord` change resolver state through `_setResolverAndTTL` (upstream: .refs/basenames/src/L2/Registry.sol:L19 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc). Resolver discovery admission is not resolver-profile support: a registry-selected arbitrary address may be watched and represented as a `contract_instance_id`, but typed record and resolver-local authorization facts may be appended only after a supported resolver profile is admitted for that instance and record family through an explicit mechanism such as code-hash / implementation allow-listing, ERC165 interface probing, ABI-family admission, or supported resolver-event observation. The static `L2Resolver` support profile is the current cited profile seed (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc). This Base-side discovery rule does not discover or widen the separate Ethereum Mainnet L1 resolver used by `basenames_l1_compat` / `basenames_execution`, and it does not admit offchain gateways. Basenames record inventory, record cache, and resolver overview must not claim consumer replacement for names whose current Base-side resolver lacks supported profile admission, even when the resolver address itself has been discovery-admitted and watched.

The Phase 8 Basenames supported-profile gate is separate from the ENSv1 PublicResolver-compatible gate and is `L2Resolver`-compatible only for Base-side resolver facts. A Base resolver instance may move from pending / unsupported profile state to supported profile state only when resolver-profile admission explicitly matches it to the Basenames `L2Resolver`-compatible profile for the relevant fact families through stored code-hash facts, implementation-edge facts, ERC165 probing, ABI-family admission, or supported resolver-event evidence. That profile is compatible with the upstream `L2Resolver` surface that composes ABI, address, contenthash, DNS, interface, name, pubkey, text, multicall, and extended-resolution profiles; stores the Base registry, registrar-controller, and reverse-registrar dependencies; authorizes the registrar controller, reverse registrar, registry owner, approved operator, or approved delegate for record writes; and exposes ERC165 support over those profiles (upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L46 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L49 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L52 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc). Unknown Base-side dynamic resolvers remain admitted watch targets only: their resolver-profile state must stay explicit `pending` or `unsupported`, and Basenames typed records, resolver-local authorization facts, record inventory, record cache, and resolver overview must not claim consumer replacement for names whose current Base resolver lacks supported `L2Resolver`-compatible profile admission. This profile gate does not reuse the ENSv1 PublicResolver-compatible profile, does not admit Ethereum Mainnet `L1Resolver` transport or execution behavior, does not admit offchain gateways, and does not change manifest schema, capability flag vocabulary, storage shape, shared enums, API route shape, or route-level coverage by itself.

Active `basenames_execution` v2 promotes only the exact-surface transport-assisted direct-path class where `resolver_path[0].logical_name_id` equals the route surface, `wildcard.source=null` with `matched_labels=[]`, `alias.final_target=null` with `hops=[]`, `subregistry_path=[]`, `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, and `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"`. Historical v1 was the pre-promotion shadow manifest state; v2 does not move transport ownership away from `basenames_l1_compat` and does not support alias-participating, wildcard-derived, linked-subregistry, transport-free, or offchain-gateway path classes (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc).

The same Ethereum Mainnet L1 Resolver address may therefore be declared in both `basenames_l1_compat` and `basenames_execution`. That duplication is intentional: transport ownership remains with `basenames_l1_compat`, while execution entrypoint ownership and supported verified-resolution routing remain with `basenames_execution` (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc).

The first Basenames `verified_primary_name` support class does not add a second execution capability flag either. `basenames_execution` remains the execution owner for exact-tuple persisted `verified_primary_name` readback on `GET /v1/primary-names/{address}` under stable execution identity `request_type=verified_primary_name` and request-key identity `{namespace}:{normalized_address}:{coin_type}`. The matching `primary_names_current(address, coin_type, namespace)` row remains the only claim-side lookup / invalidation anchor, and the public `verified_primary_name.provenance` surface stays limited to `{execution_trace_id, manifest_versions}`; `verified_primary_name.provenance.execution_trace_id` must equal top-level `provenance.execution_trace_id`, and `manifest_versions` must narrow that same persisted verification trace. The active `verified_resolution = "supported"` flag remains the single execution capability flag for the exact transport-assisted resolution class; it does not create a second primary-name manifest capability. Route-level exact-tuple coverage support is frozen over `basenames_base_primary` plus `basenames_execution` and must not widen beyond exact-tuple persisted readback, because upstream keeps reverse-name writes on the Base ReverseRegistrar while the separate Ethereum Mainnet `L1Resolver` remains the execution owner (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc) (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc).

`basenames_offchain` remains a reserved catalog family for later explicit gateway admission and is not part of the current admitted Basenames split; upstream documents the off-chain gateway as a separate category from the shipped L1 resolver and Base authority contracts (upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc).

This freeze does not create separate current source-family owners for registrar-controller, oracle, migration, proxy-admin, or offchain-gateway deployment artifacts. Later admission of those surfaces would be additive and doc-first.

## 5. Contract Instance Admission And Continuity

Manifest loading admits source-graph nodes as `contract_instance_id`s, not as raw addresses.

A manifest-declared root or contract instance keeps its ID only while the same declared contract address remains admitted on the same chain.

Rules:

- each active `[[roots]]` and `[[contracts]]` entry resolves to one admitted `contract_instance_id`
- `[[roots]]` seed canonical graph expansion and watch-plan expansion, but otherwise follow the same identity and continuity rules as `[[contracts]]`
- reusing the same declared address on the same chain across manifest versions, including after an inactive gap, carries forward the existing `contract_instance_id` and records a new non-overlapping active range
- changing a root or contract entry's own declared address closes the prior instance active range and admits a new `contract_instance_id`; any continuity to the predecessor is expressed by a `migration` edge, not by ID reuse
- when `proxy_kind != "none"`, the proxy address and the `implementation` address refer to separate contract instances linked by a time-ranged proxy / implementation edge
- changing only `implementation` keeps the proxy's `contract_instance_id`; reuse the prior implementation instance if that implementation address reappears, otherwise mint a new implementation instance

Contract addresses remain stored as time-ranged attributes for raw-fact matching and watch-plan expansion.

## 6. Discovery Admission Rules

A discovered contract becomes authoritative only if one of the following is true:

- it is declared directly in an active manifest
- it is reachable from an active manifest root through an allowed discovery rule
- it is explicitly allow-listed by manifest version for a migration epoch

Every admitted discovery edge stores:

- `from_contract_instance_id`
- `to_contract_instance_id`
- source manifest version
- edge kind
- discovery source
- active range
- provenance

Discovery uses raw addresses only as lookup inputs. It resolves `(chain, address, point in time)` to endpoint `contract_instance_id`s before storing the canonical edge.

If discovery re-admits an address that was admitted previously on the same chain and later became inactive, it reuses the prior `contract_instance_id` and records a new non-overlapping active range. It mints a new `contract_instance_id` only when that address has never been admitted on that chain.

Manifest-declared and discovered proxy / implementation links use the same edge and active-range rules.

## 7. Manifest Change Propagation

Manifest changes produce normalized events:

- `SourceManifestUpdated`
- `ProxyImplementationChanged`
- `CapabilityChanged`

They also:

- update discovery admission
- invalidate relevant execution cache entries
- trigger projection recomputation where capability boundaries change

Live manifest drift and proxy-upgrade alerting is worker-owned operational observation in Phase 10. The worker-owned audit job may compute and persist manifest/proxy alert observations from admitted manifests, stored code-hash facts, stored proxy / implementation edges, and derived watch-plan state into the worker-owned alert observation family. It must not write adapter-owned `normalized_events`, mutate manifest truth, mutate discovery admission, admit contracts, rewrite discovery edges, change capability flags, update watch-plan inputs, write projections, expose public API state, or claim consumer replacement. Alert state is derived from the already admitted source graph; remediation stays an explicit manifest or discovery change that produces the normal `SourceManifestUpdated`, `ProxyImplementationChanged`, or `CapabilityChanged` event.

Read-only manifest-drift and proxy-alert inspection is operational JSON over persisted worker-owned alert observations. The inspection surface may list stored drift or proxy alert rows with their manifest version, source family, chain, contract-instance references, expected and observed code-hash or implementation-edge material, derived watch-plan metadata, timestamps, lifecycle status, and nullable remediation metadata. `manifest-drift audit --json` remains read-only inspection and exits successfully after rendering alerts. `manifest-drift audit --fail-on-alert --json` changes only the process exit behavior: it renders the same JSON and returns a nonzero status when `counts.total` is greater than zero for actionable alerts. The audit inspection modes must not perform fresh chain comparison, create alert observations, mutate alert lifecycle state, mutate manifest truth, mutate discovery admission, admit contracts, change capability flags, rewrite discovery edges, update watch-plan inputs, write projections, write normalized events, expose a public `v1` route, or claim consumer replacement.

## 8. Watch-Plan Expansion

Watch-plan expansion starts from active manifest roots by `contract_instance_id` and traverses active discovery edges by `contract_instance_id`.

Rules:

- the chain-intake watch target is the address range attached to each active contract instance at the requested time
- watch rows may denormalize address and code-hash state, but their durable explanation path is `manifest root -> discovery edge(s) -> contract_instance_id`
- address-only watch state is derived and may be rebuilt from manifests, contract-instance address attributes, and active discovery edges

Read-only runtime watch-plan inspection is operational JSON over existing admitted watch-plan state through `bigname-worker inspect watch-plan --json`. The inspection surface exposes active watched contracts / watch-plan entries with their source kind (`manifest_root`, `manifest_contract`, or `discovery_edge`), source families, contract instance IDs, chain addresses, source manifest IDs when available, and active block ranges. It uses existing manifest/discovery state only and must not perform fresh chain comparison, admit contracts, mutate discovery edges, change capability flags, update watch-plan inputs, write projections, expose a public `v1` route, or claim consumer replacement.

## 9. Capability Policy

Capabilities gate behavior, not public-contract existence.

Rules:

- an unsupported capability must surface as `coverage.unsupported_reason` or a typed error
- shadow capabilities may write facts and traces without being enabled for general reads
- capability ownership attaches to the manifest-declared `source_family`; it is never implied by another family's presence
- ENSv1 Phase 4 wrapper and resolver admission is source-family-local: `ens_v1_wrapper_l1` admission does not imply resolver support, `ens_v1_resolver_l1` admission does not imply wrapper authority support, and neither admission graduates exact-name, history, resolver overview, primary-name, or verified-resolution public coverage without a later doc-first API / projection / capability update
- ENSv1 resolver discovery admission is source-family-local: registry `NewResolver` observations may admit resolver contract instances and node bindings, but they do not graduate resolver-profile support, typed resolver records, resolver overview coverage, or consumer replacement without explicit PublicResolver-compatible supported-profile admission for the emitted fact family; unknown dynamic resolvers remain watched targets with explicit `pending` or `unsupported` profile state (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f)
- Basenames resolver discovery admission is source-family-local and is not widened by the ENSv1 PublicResolver-compatible profile gate: registry `NewResolver` observations may admit resolver contract instances and node bindings, but they do not graduate resolver-profile support, typed resolver records, resolver overview coverage, or consumer replacement without explicit Base-side `L2Resolver`-compatible supported-profile admission for the emitted fact family; that gate is separate from Basenames L1 transport / execution and from offchain-gateway admission (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)
- ENSv2 Phase 5 capability ownership on the `sepolia-dev` alternate profile is source-family-local: registry admission does not imply registrar support, registrar admission does not imply resolver support, and resolver admission does not imply universal-resolver / execution support
- ENSv2 `sepolia-dev` exact-name profile support is profile-scoped: only `exact_name_profile = "supported"` on `ens_v2_registrar_l1` in the selected `sepolia-dev` manifest root graduates that profile's exact-name declared read; active rollout, raw preimage observations, resolver admission, backfill completion, or the presence of `ETHRegistry` / `ETHRegistrar` contracts do not graduate any other profile or capability (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309)
- ENS verified resolution on Ethereum Mainnet is owned by `ens_execution` through contract role `universal_resolver` at the official ENS Universal Resolver proxy address `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`, not by `ens_v1_registry_l1` (official ENS docs: https://docs.ens.domains/resolvers/universal/) (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f)
- ENS reverse-claim intake on Ethereum Mainnet is anchored to `ens_v1_reverse_l1` through contract role `reverse_registrar` at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`, not by `ens_v1_registry_l1` or `ens_v1_resolver_l1` (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L19 @ ens_v1@91c966f)
- ENS primary-name truth on Ethereum Mainnet is intentionally split across those owners: `ens_v1_reverse_l1` owns declared reverse-claim intake, while verification stays execution-derived rather than becoming a second manifest-owned claim surface (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f)
- that reverse-family ownership freezes only the current reverse-only ENS claim surface; any later fallback claim-setting surface would need its own manifest-owned source family and a later doc-first contract update (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
- the shipped Phase 7 ENS primary-name route does not require dedicated `claimed_primary_name` or `verified_primary_name` capability flags on either owner; reverse admission plus execution-owned persisted readback are enough for the exact-tuple persisted-readback contract
- `rollout_status` and `capability_flags` are source-family-local readiness inputs; they do not by themselves widen ENS claim precedence, combine reverse tuple intake with resolver-backed name payloads, collapse claimed and verified primary-name truth into one manifest capability, or widen public coverage beyond local exact-tuple persisted readback (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
- adding a new capability is additive if it does not change prior semantics

## 10. Ownership And Workflow

- manifest/discovery owners maintain the TOML files
- adapter owners consume manifest versions as inputs, not hidden configuration
- execution owners depend on manifest versions for cache keys and invalidation
- any manifest schema change requires a doc-first update to this file
