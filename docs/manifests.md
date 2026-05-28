# Source Manifests

Manifests pin watched contracts, capability ownership, and rollout state per source family. They are part of the truth model, not deploy-time configuration. The wider model lives in [`architecture.md`](architecture.md); persistence in [`storage.md`](storage.md); intake in [`chain-intake.md`](chain-intake.md); execution in [`execution.md`](execution.md).

## File format and location

Manifests are TOML files at:

```text
manifests/<profile>/<chain_combo>/<namespace>/<source_family>/<version>.toml
```

Profiles select a corpus, and chain-combo directories partition the chains inside that corpus:

```text
manifests/mainnet/ethereum/<namespace>/<source_family>/v1.toml
manifests/mainnet/base/<namespace>/<source_family>/v1.toml
manifests/sepolia/ethereum/<namespace>/<source_family>/v1.toml
manifests/sepolia/base/<namespace>/<source_family>/v1.toml
```

One runtime selects exactly one manifest profile root at startup — `manifests/mainnet/` for the shipped mainnet profile, or `manifests/sepolia/` for the ENSv2 Sepolia profile. Profile selection is not a manifest schema change. A runtime never loads two profile roots into the same canonical corpus, watch plan, discovery graph, or projection set.

Within a selected profile root, the first directory component is the chain combo. It must match the leading component of each manifest `chain` ID: `ethereum-mainnet` lives under `ethereum/`, `base-mainnet` under `base/`, and `ethereum-sepolia` under `ethereum/`.

TOML is chosen for deterministic diffs, hand-editing, and straightforward Rust parsing.

## Required fields

Each manifest contains:

- `manifest_version`
- `namespace`
- `source_family`
- `chain`
- `deployment_epoch`
- `rollout_status` — `draft` | `shadow` | `active` | `deprecated`
- `normalizer_version`
- `capability_flags`
- `roots`
- `contracts`
- `discovery_rules`

Each `[[roots]]` and `[[contracts]]` entry may declare an optional `start_block`. `start_block` is the inclusive first historical block for that target. Omitted means unknown — adapters preserve that state rather than inferring zero, the current job range, the manifest activation height, or any other fallback.

For `[[contracts]]`, `proxy_kind` is required. `proxy_kind = "none"` omits `implementation`. Any non-`none` `proxy_kind` includes `implementation` as the current implementation address for that manifest version.

For `[[discovery_rules]]`, the only authorable `admission` value is `reachable_from_root` — the discovered edge is authoritative while its `from_role` endpoint remains reachable from an active manifest root under an allowed rule. Internal labels like `manifest_declared` and `manifest_successor` are storage tags, not authored values.

`[abi]` is optional. When present, it declares the Solidity ABI fragments that this manifest version authorizes for adapter, execution, or watch-plan use. ABI entries are source-family metadata; they do not by themselves graduate public capability support.

### `capability_flags`

Each flag carries a name, a status (`unsupported` | `shadow` | `supported`), and optional notes.

### `chain`

`chain` names the authority chain for that manifest within the selected profile. Mainnet manifests use chain IDs like `ethereum-mainnet` and `base-mainnet`. Sepolia support is additive as a separate manifest profile root and chain-ID set.

### `abi`

ABI entries use Alloy-parseable human-readable Solidity fragments, not handwritten selectors or topic hashes. The loader validates each fragment with Alloy and derives event topic0 values, function selectors, canonical signatures, indexed parameters, inputs, and outputs from the fragment. Authored selector/topic fields are intentionally absent.

`[[abi.events]]` entries contain:

- `name` — must match the parsed event name.
- `fragment` — a human-readable event fragment such as `event ResolverUpdated(uint256 indexed node, address resolver, address sender)`.
- `emitter_roles` — optional `[[contracts]].role` values that may emit the event.
- `normalized_events` — optional normalized event kinds produced from the event.
- `status` — optional `unsupported` | `shadow` | `supported` marker for the ABI entry.
- `notes` — optional reviewer-facing context.

`[[abi.calls]]` entries contain:

- `name` — must match the parsed function name.
- `fragment` — a human-readable function fragment such as `function resolver(bytes32 node) view returns (address)`.
- `target_roles` — optional `[[contracts]].role` values that may be called.
- `status` — optional `unsupported` | `shadow` | `supported` marker for the ABI entry.
- `notes` — optional reviewer-facing context.

ABI fragments should cite upstream in nearby manifest comments or in the public doc section that admits the source family. If an adapter still has an in-code selector or `sol!` definition for a manifest-declared fragment, that code is a compatibility bridge until the adapter consumes the manifest ABI directly.

`normalizer_version` is currently `ensip15@ens-normalize-0.1.1` for all admitted ENS, ENSv2, and Basenames source families. Runtime code treats this as one shared normalization boundary, not a per-source-family choice.

## Example shape

```toml
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000000"
code_hash = "sha256:..."
abi_ref = "abis/ens_v2_root_registry.json"
start_block = 123456

[[contracts]]
role = "registry"
address = "0x0000000000000000000000000000000000000000"
proxy_kind = "none"
start_block = 123456

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

[[abi.events]]
name = "ResolverUpdated"
fragment = "event ResolverUpdated(uint256 indexed node, address resolver, address sender)"
emitter_roles = ["registry"]
normalized_events = ["ResolverChanged"]
status = "supported"

[[abi.calls]]
name = "resolver"
fragment = "function resolver(bytes32 node) view returns (address)"
target_roles = ["registry"]
status = "shadow"

[capability_flags]
declared_children = "supported"
```

## Capability ownership

Capability ownership attaches to the declaring `source_family`. It is never implied by another family's presence.

### ENS mainnet

`ens_execution` owns verified resolution at the ENS Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` with `verified_resolution = "shadow"`.[^ens-docs-univ][^v1-ur-deploy][^v1-ursol-l8] The pinned `.refs/` artifact is the implementation/ABI anchor; the route-facing entry is the proxy address. The shadow flag records manifest ownership for the execution substrate; public ENS verified-resolution support is gated by the route-level support classes in `docs/api-v1-routes.md` and `docs/execution.md`, not by widening this manifest flag.

The ENS primary-name route does not introduce a second manifest capability. `ens_execution` remains the execution owner for exact-tuple persisted `verified_primary_name` readback under the same execution-owner manifest, without turning `verified_resolution = "shadow"` into a route-level primary-name support flag.

`ens_v1_reverse_l1` owns declared reverse-claim intake at the Mainnet `addr.reverse` Reverse Registrar `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`.[^v1-revreg-deploy][^v1-revreg-l15][^v1-revreg-l19] No dedicated `claimed_primary_name` flag is needed for the exact-tuple persisted-readback contract.

`ens_v1_registry_l1` owns the current ENS registry at `0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E` with `start_block = 9380380`,[^subgraph-l15] plus `ENSRegistryOld` at `0x314159265dd8dbb310642f98f50c066173c1259b` with `start_block = 3327417` as a migration-aware input.[^subgraph-l39][^subgraph-l44] Old-registry logs do not union with current logs by latest block: a current-registry `NewOwner` marks the node migrated; later old-registry `NewOwner`, `Transfer`, `NewTTL`, and non-root `NewResolver` updates for that node are suppressed.[^subgraph-ts-l134][^subgraph-ts-l230][^subgraph-ts-l238][^subgraph-ts-l246] Root-resolver updates from the old registry are the one frozen exception.[^v1-ensregfb-l40]

`ens_v1_registrar_l1` owns `.eth` BaseRegistrar at `start_block = 9380410`[^subgraph-l122] plus the legacy, wrapped, and current ETHRegistrarController contracts as label-bearing intake (LegacyEthRegistrarController `9380471`,[^subgraph-l145] WrappedETHRegistrarController `16925618`,[^v1-wrapethrc-l640] current ETHRegistrarController `22764821`).[^v1-ethrc-l706] Controllers do not split into a separate source-family owner.

`ens_v1_wrapper_l1` owns NameWrapper at `0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401` with `start_block = 16925608`,[^v1-namewrapper-deploy] for wrapper authority, fuse/expiry, wrapper-revealed names, and wrapper-driven registry changes.[^v1-iname-l27][^v1-iname-l35][^v1-iname-l37][^v1-iname-l38]

`ens_v1_resolver_l1` owns ENS Labs PublicResolver-generation profile admission. The seed entry is the latest PublicResolver at `0xF29100983E058B709F3D539b0c765937B804AC15` with `start_block = 22764828`.[^v1-publicresolver-deploy] Resolver-profile admission is the gate for complete record-family coverage, resolver-overview support, latest-only behavior, and event-to-call parity. Unadmitted resolvers stay `pending` or `unsupported`.

Admitted ENS Labs PublicResolver generations on Ethereum Mainnet (first-party app-known data):[^v1-app-resolvers]

| Address | Profile | Limitations |
| --- | --- | --- |
| `0xF29100983E058B709F3D539b0c765937B804AC15` | latest: address, multicoin, default coin-type fallback, name, ABI, text, contenthash, DNS, interface, name-wrapper-aware, VersionableResolver | no pubkey or `DataResolver` |
| `0x231b0Ee14048e9dCcD1d247744d114a4EB5E8E63` | as latest minus default coin-type fallback | no pubkey or `DataResolver` |
| `0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41` | address, multicoin, name, ABI, text, contenthash, DNS, interface | no name-wrapper, no fallback, no Versionable, no pubkey/`DataResolver` |
| `0xDaaF96c344f63131acadD0Ea35170E7892d3dfBA` | same as `0x4976...` | same |
| `0x226159d592E2b063810a10Ebf6dcbADA94Ed68b8` | legacy: address, multicoin, name, ABI, text, contenthash, interface | no DNS, no name-wrapper, no fallback, no Versionable, no pubkey/`DataResolver` |
| `0x5FfC014343cd971B7eb70732021E26C35B744cc4` | older legacy: ETH-address, name, ABI, text, interface | no multicoin, contenthash, DNS, name-wrapper, fallback, Versionable, pubkey/`DataResolver` |
| `0x1da022710dF5002339274AaDEe8D58218e9D6AB5` | oldest legacy: ETH-address, name, ABI, interface | no text, contenthash, multicoin, DNS, name-wrapper, fallback, Versionable, pubkey/`DataResolver` |

Older rows do not inherit latest-only behavior. Unsupported interfaces and pending profiles surface explicitly through `coverage`, `UnsupportedSummary`, `resolver_family_pending`, or `resolver_family_unsupported`. They are never reported as absent records.

Address-specific resolver `start_block`s come from ENSNode datasource pins where available: `0x1da0...` `3648359`, `0x5FfC...` `3733668`, `0x2261...` `8659893`, `0x4976...` `9412610`, `0x231b...` `16925619`.[^ensnode-mainnet] `0xDaaF...` has no pinned datasource; it uses the current ENSRegistry epoch `9380380` as a conservative bootstrap basis. The OffchainDNSResolver and ExtendedDNSResolver app-known maps remain deferred — they are not PublicResolver-generation profile admissions.

Resolver discovery from registry `NewResolver(node, resolver)` admits the resolver as a `contract_instance_id` in `ens_v1_resolver_l1` and updates the node-to-resolver binding.[^v1-ens-l12][^v1-ensreg-l89][^v1-ensreg-l174] Zero-address observations close only that binding. Generic resolver-local events (`AddrChanged`, `AddressChanged`, `TextChanged`, `VersionChanged`) feed observed selector/cache facts; they do not graduate profile support.

`PubkeyChanged` is ignored by the current admission model. `DataResolver`-shaped events are unsupported on admitted generations and `pending` on unknown profiles. The generic `resolver_record` fact is an observation bucket; it does not act as a catch-all for unknown families.

### ENSv2 (`sepolia` profile)

The `sepolia` profile admits four ENSv2 Sepolia dev families under `manifests/sepolia/ethereum/ens/`:[^v2-deploy-root][^v2-deploy-ethreg][^v2-deploy-ethrc][^v2-deploy-pres]

- `ens_v2_root_l1` — `RootRegistry` at `0x3a3e15a5d27ff6f05c844313312f2e72096d3ed3`, `start_block = 10462881`. Tokenized, resource-scoped permissioned registry seed for discovery and parent graph state.[^v2-pr-l22][^v2-pr-l28]
- `ens_v2_registry_l1` — `ETHRegistry` at `0x796fff2e907449be8d5921bcc215b1b76d89d080`, `start_block = 10462895`, plus discovered `UserRegistry` proxy instances. `UserRegistryImpl` at `0xea93aff7375e8176053ab6ab36b57cab53cbf702` is implementation metadata, not a separate owner.[^v2-userreg-l15]
- `ens_v2_registrar_l1` — `ETHRegistrar` at `0x68586418353b771cf2425ed14a07512aa880c532`, `start_block = 10462909`. Registrar events and commit/renew facts; registered-name resource identity links back to the registry resource.[^v2-ethrc-l49][^v2-ethrc-l173]
- `ens_v2_resolver_l1` — `PermissionedResolver` resolver state, alias events, record-version events, resolver-scoped EAC permissions. `PermissionedResolverImpl` at `0xe566a1fbaf30ff7c39828fe99f955fc55544cb9c` is the initial implementation artifact.[^v2-pres-l38][^v2-pres-l70]

Exact-name profile promotion is profile-scoped: only `exact_name_profile = "supported"` on `ens_v2_registrar_l1` in the `sepolia` root graduates `.eth` exact-name declared reads, backed by `ETHRegistry` resource/token state and `ETHRegistrar` lifecycle facts.[^v2-iperm-l22][^v2-events-l15][^v2-iethreg-l32] The promotion does not apply to mainnet, other Sepolia profiles, or any runtime that has not selected `manifests/sepolia`. Active rollout, raw preimage observations, resolver admission, or backfill completion do not graduate any other capability.

Upstream events map to normalized adapter output: `TokenResource` → `TokenResourceLinked`, `TokenRegenerated` → `TokenRegenerated`, `SubregistryUpdated` → `SubregistryChanged`, `ParentUpdated` → `ParentChanged`, `AliasChanged` → `AliasChanged`, `EACRolesChanged` → resource- or resolver-scoped permission events.[^v2-iperm-l34][^v2-events-l49][^v2-events-l69][^v2-events-l75][^v2-iperm-resolver-l14][^v2-eac-l19] These are adapter semantics, not manifest schema fields.

Other `sepolia-dev` artifacts (`UniversalResolverV2`, `ReverseRegistry`, `DNSAliasResolver`, `WrapperRegistryImpl`, `LockedMigrationController`, `HCAFactory`, `StandardRentPriceOracle`, `BatchRegistrar`, `MockUSDC`, `MockDAI`) remain outside admission until a doc-first update.

### Basenames mainnet

Basenames mainnet admits six families:[^bn-readme-l22][^bn-readme-l28][^bn-readme-l29][^bn-readme-l33][^bn-readme-l34][^bn-readme-l69][^bn-readme-l70]

- `basenames_base_registry` — `registry` at `0xb94704422c2a1e396835a571837aa5ae53285a95` (Base). Per-node owner/resolver/ttl state.[^bn-registry-l10][^bn-registry-l100][^bn-registry-l113][^bn-registry-l132]
- `basenames_base_registrar` — `registrar` at `0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a` (Base). Tokenized authority owning `base.eth`, minting `*.base.eth` subdomains.[^bn-baseregistrar-l15][^bn-baseregistrar-l17][^bn-baseregistrar-l237][^bn-baseregistrar-l327]
- `basenames_base_resolver` — `resolver` at `0xC6d566A56A1aFf6508b41f6c90ff131615583BCD` (Base). Default `L2Resolver` profile seed.[^bn-l2resolver-l22][^bn-l2resolver-l49][^bn-l2resolver-l52][^bn-l2resolver-l193]
- `basenames_base_primary` — `reverse_registrar` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282` (Base). Declared primary-claim intake only.[^bn-revreg-l12][^bn-revreg-l150][^bn-revreg-l193]
- `basenames_l1_compat` — `l1_resolver` at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` (Ethereum). L1 compatibility transport for the `base.eth` domain.[^bn-l1resolver-l13]
- `basenames_execution` — `l1_resolver` at the same Ethereum address with `verified_resolution = "supported"` for the exact-surface transport-assisted direct-path class only. Execution entrypoint that initiates `OffchainLookup` and completes through `resolveWithProof`.[^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]

The L1 Resolver address appears in both `basenames_l1_compat` and `basenames_execution`. Transport ownership stays with `basenames_l1_compat`; execution entrypoint and verified-resolution routing stay with `basenames_execution`.

`basenames_execution` v2 promotes only the path class where `resolver_path[0].logical_name_id` equals the route surface, `wildcard.source = null`, `alias.final_target = null`, `subregistry_path = []`, `transport.source_chain_id = "base-mainnet"`, `transport.target_chain_id = "ethereum-mainnet"`, and `transport.contract_address = "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"`. Alias-participating, wildcard-derived, linked-subregistry, transport-free, and offchain-gateway classes return selector-local `unsupported`.[^bn-readme-l71]

`verified_primary_name` for Basenames runs through `basenames_execution` under the same flag. The matching `primary_names_current(address, coin_type, namespace)` row is the only claim-side anchor; `verified_primary_name.provenance` carries `{execution_trace_id, manifest_versions}` matching the top-level `execution_trace_id`.

Basenames Base-side resolver discovery from registry `NewResolver` admits resolver instances and updates bindings. Resolver-local fact consumption requires `L2Resolver`-compatible profile admission for the emitted family. The Base-side discovery rule does not discover the L1 Resolver and does not admit offchain gateways.[^bn-registry-l19][^bn-registry-l223][^bn-l2resolver-l4][^bn-l2resolver-l16][^bn-l2resolver-l29][^bn-l2resolver-l182][^bn-l2resolver-l209][^bn-l2resolver-l225]

`basenames_offchain` is reserved for later gateway admission. It is not part of the current split.

## Contract instance admission and continuity

Manifest loading admits source-graph nodes as `contract_instance_id`s, not raw addresses. Each active `[[roots]]` and `[[contracts]]` entry resolves to one admitted instance.

- `[[roots]]` seed canonical graph and watch-plan expansion; otherwise they follow the same identity rules as `[[contracts]]`.
- Reusing the same address on the same chain across manifest versions, even across an inactive gap, carries forward the existing `contract_instance_id` and appends a new non-overlapping active range.
- Changing a declared address closes the prior active range and admits a new instance. Continuity to the predecessor uses a `migration` edge, not ID reuse.
- `proxy_kind = "none"` resolves the declared address directly; `implementation` is omitted.
- `proxy_kind != "none"` requires `implementation`. The proxy and implementation are separate instances linked by a time-ranged proxy/implementation edge.
- Changing only `implementation` keeps the proxy's identity. The implementation instance is reused if its address reappears, otherwise a new one is minted.

Contract addresses persist as time-ranged attributes for raw-fact matching and watch-plan expansion.

## Discovery admission

A discovered contract is authoritative when one of these holds:

- it is declared directly in an active manifest
- it is reachable from an active manifest root through an allowed `discovery_rules` edge
- it is explicitly allow-listed by a manifest version for a migration epoch

Each admitted edge stores `from_contract_instance_id`, `to_contract_instance_id`, source manifest version, edge kind, discovery source, active range, and provenance.

Discovery resolves `(chain, address, point in time)` to endpoint `contract_instance_id`s before storing the edge. Re-admitting an address that was previously admitted on the same chain reuses the prior `contract_instance_id` and appends a new range; a new ID is minted only for addresses never admitted on that chain. Manifest-declared and discovered proxy/implementation links share the same edge and active-range rules.

## Manifest change propagation

Manifest changes produce normalized events: `SourceManifestUpdated`, `ProxyImplementationChanged`, `CapabilityChanged`. They update discovery admission, invalidate execution cache entries, and trigger projection recomputation where capability boundaries change.

Live manifest drift and proxy-upgrade alerting is a worker-owned operational loop. The worker computes drift candidates from admitted manifests, code-hash facts, proxy/implementation edges, and watch-plan state, and persists them to the worker-owned alert observation family. The worker does not write `normalized_events`, mutate manifests, mutate discovery admission, change capability flags, write projections, or expose a public route. Remediation is an explicit manifest or discovery change that produces the normal events above.

`bigname-worker manifest-drift audit --json` computes candidates, persists alert observations, and renders the persisted view alongside live counts. `--fail-on-alert --json` returns nonzero when actionable persisted alerts remain. `bigname-worker inspect manifest-drift --json` is read-only over already persisted observations.

## Watch-plan expansion

Watch-plan expansion starts from active manifest roots by `contract_instance_id` and traverses active discovery edges by ID.

- The chain-intake watch target is the address range attached to each active contract instance at the requested time.
- If a manifest target carries `start_block`, the materialized watch range starts at that inclusive block unless a later active-range boundary narrows it.
- If `start_block` is omitted, the historical start is unknown. Live watch may still produce a target; automatic historical bootstrap treats it as unbootstrapable until a finite start is declared.
- Watch rows may denormalize address and code-hash state, but their durable explanation path is `manifest root → discovery edge(s) → contract_instance_id`.
- Address-only watch state is rebuildable from manifests, instance attributes, and active discovery edges.

`bigname-worker inspect watch-plan --json` exposes active watched contracts with source kind (`manifest_root`, `manifest_contract`, `discovery_edge`), source families, contract instance IDs, chain addresses, source manifest IDs, and active block ranges. It is read-only over existing state.

## Capability policy

Capabilities gate behavior, not public-contract existence. An unsupported capability surfaces as `coverage.unsupported_reason` or a typed error. Shadow capabilities write facts and traces without enabling general reads. Adding a new capability is additive only when it does not change prior semantics.

## Ownership

- Manifest/discovery owners maintain the TOML files.
- Adapter owners consume manifest versions as inputs.
- Execution owners depend on manifest versions for cache keys and invalidation.
- Schema changes require a doc-first update to this file.

---

## Bootstrap `start_block` provenance

Known historical starts cite a pinned upstream source. Targets without a pinned source omit `start_block`; automatic bootstrap skips them rather than inventing values. Basenames mainnet families and the ENS Universal Resolver remain unknown.

| Target | `start_block` | Source |
| --- | --- | --- |
| ENSv1 ENSRegistry | `9380380` | [^subgraph-l15] |
| ENSv1 ENSRegistryOld | `3327417` | [^subgraph-l39] |
| ENSv1 BaseRegistrar | `9380410` | [^subgraph-l122] |
| LegacyEthRegistrarController | `9380471` | [^subgraph-l145] |
| WrappedETHRegistrarController | `16925618` | [^v1-wrapethrc-l640] |
| ETHRegistrarController | `22764821` | [^v1-ethrc-l706] |
| ENSv1 NameWrapper | `16925608` | [^v1-namewrapper-deploy] |
| ENSv1 PublicResolver (latest) | `22764828` | [^v1-publicresolver-deploy] |
| ENSv1 ReverseRegistrar | `16925606` | [^v1-revreg-deploy-l379] |
| ENSv2 RootRegistry (`sepolia-dev`) | `10462881` | [^v2-deploy-root] |
| ENSv2 ETHRegistry (`sepolia-dev`) | `10462895` | [^v2-deploy-ethreg] |
| ENSv2 ETHRegistrar (`sepolia-dev`) | `10462909` | [^v2-deploy-ethrc] |

---

[^ens-docs-univ]: <https://docs.ens.domains/resolvers/universal/>
[^v1-app-resolvers]: (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L32 @ ens_app_v3@7175858)
[^ensnode-mainnet]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L343 @ ensnode@9b8f590)

[^v1-ens-l12]: (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)
[^v1-ensreg-l89]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)
[^v1-ensreg-l174]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f)
[^v1-ensregfb-l40]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L40 @ ens_v1@91c966f)

[^v1-iname-l27]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)
[^v1-iname-l35]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f)
[^v1-iname-l37]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f)
[^v1-iname-l38]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f)

[^v1-namewrapper-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L1498 @ ens_v1@91c966f)
[^v1-publicresolver-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L1104 @ ens_v1@91c966f)
[^v1-revreg-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-revreg-deploy-l379]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L379 @ ens_v1@91c966f)
[^v1-ur-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)
[^v1-ursol-l8]: (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f)

[^v1-wrapethrc-l640]: (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L640 @ ens_v1@91c966f)
[^v1-ethrc-l706]: (upstream: .refs/ens_v1/deployments/mainnet/ETHRegistrarController.json:L706 @ ens_v1@91c966f)

[^v1-revreg-l15]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f)
[^v1-revreg-l19]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L19 @ ens_v1@91c966f)

[^subgraph-l15]: (upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)
[^subgraph-l39]: (upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)
[^subgraph-l44]: (upstream: .refs/ens_subgraph/subgraph.yaml:L44 @ ens_subgraph@723f1b6)
[^subgraph-l122]: (upstream: .refs/ens_subgraph/subgraph.yaml:L122 @ ens_subgraph@723f1b6)
[^subgraph-l145]: (upstream: .refs/ens_subgraph/subgraph.yaml:L145 @ ens_subgraph@723f1b6)
[^subgraph-ts-l134]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L134 @ ens_subgraph@723f1b6)
[^subgraph-ts-l230]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L230 @ ens_subgraph@723f1b6)
[^subgraph-ts-l238]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6)
[^subgraph-ts-l246]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6)

[^v2-deploy-root]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2617 @ ens_v2@554c309)
[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2617 @ ens_v2@554c309)
[^v2-deploy-ethrc]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L1922 @ ens_v2@554c309)
[^v2-deploy-pres]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/PermissionedResolverImpl.json:L2 @ ens_v2@554c309)

[^v2-userreg-l15]: (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L15 @ ens_v2@554c309)
[^v2-ethrc-l49]: (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L49 @ ens_v2@554c309)
[^v2-ethrc-l173]: (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L173 @ ens_v2@554c309)

[^v2-pr-l22]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L22 @ ens_v2@554c309)
[^v2-pr-l28]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L28 @ ens_v2@554c309)

[^v2-pres-l38]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L38 @ ens_v2@554c309)
[^v2-pres-l70]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L70 @ ens_v2@554c309)

[^v2-iperm-l22]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L22 @ ens_v2@554c309)
[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309)
[^v2-iperm-resolver-l14]: (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309)
[^v2-iethreg-l32]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309)

[^v2-events-l15]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309)
[^v2-events-l49]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309)
[^v2-events-l69]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309)
[^v2-events-l75]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309)

[^v2-eac-l19]: (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309)

[^bn-readme-l22]: (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc)
[^bn-readme-l28]: (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc)
[^bn-readme-l29]: (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc)
[^bn-readme-l33]: (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc)
[^bn-readme-l34]: (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc)
[^bn-readme-l69]: (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)
[^bn-readme-l70]: (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
[^bn-readme-l71]: (upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc)

[^bn-l1resolver-l13]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
[^bn-l1resolver-l154]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)
[^bn-l1resolver-l173]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc)
[^bn-l1resolver-l191]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)

[^bn-registry-l10]: (upstream: .refs/basenames/src/L2/Registry.sol:L10 @ basenames@1809bbc)
[^bn-registry-l19]: (upstream: .refs/basenames/src/L2/Registry.sol:L19 @ basenames@1809bbc)
[^bn-registry-l100]: (upstream: .refs/basenames/src/L2/Registry.sol:L100 @ basenames@1809bbc)
[^bn-registry-l113]: (upstream: .refs/basenames/src/L2/Registry.sol:L113 @ basenames@1809bbc)
[^bn-registry-l132]: (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
[^bn-registry-l223]: (upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc)

[^bn-baseregistrar-l15]: (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L15 @ basenames@1809bbc)
[^bn-baseregistrar-l17]: (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L17 @ basenames@1809bbc)
[^bn-baseregistrar-l237]: (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L237 @ basenames@1809bbc)
[^bn-baseregistrar-l327]: (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L327 @ basenames@1809bbc)

[^bn-l2resolver-l4]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc)
[^bn-l2resolver-l16]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc)
[^bn-l2resolver-l22]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
[^bn-l2resolver-l29]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc)
[^bn-l2resolver-l49]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L49 @ basenames@1809bbc)
[^bn-l2resolver-l52]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L52 @ basenames@1809bbc)
[^bn-l2resolver-l182]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc)
[^bn-l2resolver-l193]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)
[^bn-l2resolver-l209]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc)
[^bn-l2resolver-l225]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)

[^bn-revreg-l12]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
[^bn-revreg-l150]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
[^bn-revreg-l193]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
