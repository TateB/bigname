# Source manifests

Manifests pin watched contracts, capability ownership, and rollout state per source family. They are part of the truth model — not deploy-time configuration. Architecture model in [`architecture.md`](architecture.md); persistence in [`storage.md`](storage.md); intake in [`chain-intake.md`](chain-intake.md); execution in [`execution.md`](execution.md); upstream pins and divergences in [`upstream.md`](upstream.md).

## File layout

```
manifests/<namespace>/<source_family>/<version>.toml
```

Alternate deployment profiles live under a profile-specific root. The first ENSv2 alternate profile is `sepolia-dev`:

```
manifests-sepolia-dev/<namespace>/<source_family>/v1.toml
```

One runtime selects exactly one root at startup (`manifests/` for the shipped mainnet profile, `manifests-sepolia-dev/` for the dev profile). Profile selection is operational, not a schema field. A runtime never loads two roots into the same canonical corpus, watch plan, discovery graph, or projection set.

TOML is chosen for deterministic diffs, hand-editing, and straightforward Rust parsing.

## Schema

Each manifest file:

```toml
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"          # draft | shadow | active | deprecated
normalizer_version = "uts46-v1"

[[roots]]
name = "RootRegistry"
address = "0x..."
code_hash = "sha256:..."
abi_ref = "abis/ens_v2_root_registry.json"
start_block = 123456                # optional inclusive bootstrap metadata

[[contracts]]
role = "registry"
address = "0x..."
proxy_kind = "none"                 # required; non-"none" requires `implementation`
start_block = 123456

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"   # the only authorable value

[capability_flags]
declared_children = "supported"     # unsupported | shadow | supported
```

Required top-level fields: `manifest_version`, `namespace`, `source_family`, `chain`, `deployment_epoch`, `rollout_status`, `normalizer_version`, `capability_flags`, `roots`, `contracts`, `discovery_rules`.

Notes:

- `start_block` on `[[roots]]` and `[[contracts]]` is optional inclusive bootstrap metadata. Omitted means unknown — adapters preserve that state rather than inferring zero, the manifest activation height, or the current job range start.
- `proxy_kind = "none"` omits `implementation`. Any non-`none` proxy kind includes `implementation` as the current implementation address.
- `discovery_rules.admission`: only `reachable_from_root` is authorable. The internal labels `manifest_declared` and `manifest_successor` are storage tags.
- `chain` names the authority chain for the manifest within the selected profile (`ethereum-mainnet`, `base-mainnet`, etc.). Sepolia and `sepolia-dev` support is additive as a separate manifest root and chain set.

## Capability ownership — ENS mainnet

Capability ownership attaches to the declaring `source_family`. It's never implied by another family's presence.

### `ens_v1_registry_l1`

Owns the current ENS registry plus migration-aware `ENSRegistryOld` input.

| Contract | Address | `start_block` | Source |
|---|---|---|---|
| `ENSRegistry` (current) | `0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E` | `9380380` | [^subgraph-l15] |
| `ENSRegistryOld` (migration epoch) | `0x314159265dd8dbb310642f98f50c066173c1259b` | `3327417` | [^subgraph-l39] |

Old-registry logs do **not** union with current logs. A current-registry `NewOwner` marks a node migrated; later old-registry `NewOwner`/`Transfer`/`NewTTL`/non-root `NewResolver` updates for that node are retained as raw facts but suppressed from topology[^subgraph-ts-l134][^subgraph-ts-l230][^subgraph-ts-l238][^subgraph-ts-l246]. Root-resolver updates from the old registry are the one frozen exception[^v1-ensregfb-l40].

### `ens_v1_registrar_l1`

Owns `.eth` BaseRegistrar plus the legacy/wrapped/current ETHRegistrarController contracts as label-bearing intake. Controllers don't split into a separate source-family owner.

| Contract | Address | `start_block` | Source |
|---|---|---|---|
| BaseRegistrar | `0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85` | `9380410` | [^subgraph-l122] |
| LegacyEthRegistrarController | `0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5` | `9380471` | [^subgraph-l145] |
| WrappedETHRegistrarController | `0x253553366Da8546fC250F225fe3d25d0C782303b` | `16925618` | [^v1-wrapethrc-l640] |
| ETHRegistrarController | `0x59E16fcCd424Cc24e280Be16E11Bcd56fb0CE547` | `22764821` | [^v1-ethrc-l706] |

### `ens_v1_wrapper_l1`

Owns the Mainnet NameWrapper.

| Contract | Address | `start_block` | Source |
|---|---|---|---|
| NameWrapper | `0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401` | `16925608` | [^v1-namewrapper-deploy] |

Covers wrapper authority, fuses/expiry, wrapper-revealed names, and wrapper-driven registry changes[^v1-iname-l27].

### `ens_v1_resolver_l1`

Owns ENS Labs PublicResolver-generation profile admission. Admission is the gate for complete record-family coverage, resolver-overview support, latest-only behaviour, and event-to-call parity.

The seed entry is the current PublicResolver at `0xF29100983E058B709F3D539b0c765937B804AC15` (`start_block = 22764828`)[^v1-publicresolver-deploy]. Other admitted ENS Labs PublicResolver generations on Ethereum Mainnet (first-party app-known data[^v1-app-resolvers]):

| Address | Profile | Limitations |
|---|---|---|
| `0xF29100983E058B709F3D539b0c765937B804AC15` | latest: address, multicoin, default coin-type fallback, name, ABI, text, contenthash, DNS, interface, name-wrapper-aware, VersionableResolver | no pubkey or DataResolver |
| `0x231b0Ee14048e9dCcD1d247744d114a4EB5E8E63` | as latest minus default coin-type fallback | no pubkey or DataResolver |
| `0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41` | address, multicoin, name, ABI, text, contenthash, DNS, interface | no name-wrapper, no fallback, no Versionable, no pubkey/DataResolver |
| `0xDaaF96c344f63131acadD0Ea35170E7892d3dfBA` | same as `0x4976…` | same |
| `0x226159d592E2b063810a10Ebf6dcbADA94Ed68b8` | legacy: address, multicoin, name, ABI, text, contenthash, interface | no DNS, no name-wrapper, no fallback, no Versionable, no pubkey/DataResolver |
| `0x5FfC014343cd971B7eb70732021E26C35B744cc4` | older legacy: ETH-address, name, ABI, text, interface | no multicoin, contenthash, DNS, name-wrapper, fallback, Versionable, pubkey/DataResolver |
| `0x1da022710dF5002339274AaDEe8D58218e9D6AB5` | oldest legacy: ETH-address, name, ABI, interface | no text, contenthash, multicoin, DNS, name-wrapper, fallback, Versionable, pubkey/DataResolver |

Older rows do not inherit latest-only behaviour. Address-specific `start_block`s come from ENSNode datasource pins where available[^ensnode-mainnet]: `0x1da0…` `3648359`, `0x5FfC…` `3733668`, `0x2261…` `8659893`, `0x4976…` `9412610`, `0x231b…` `16925619`. `0xDaaF…` has no pinned datasource — it uses the current ENSRegistry epoch `9380380` as a conservative bootstrap basis. `OffchainDNSResolver` and `ExtendedDNSResolver` app-known maps are not PublicResolver-generation profile admissions and remain deferred.

Discovery from registry `NewResolver(node, resolver)` admits the resolver as a `contract_instance_id` in `ens_v1_resolver_l1` and updates the node-to-resolver binding[^v1-ensreg-l89]. Zero-address closes only that binding. Generic resolver-local events (`AddrChanged`, `AddressChanged`, `TextChanged`, `VersionChanged`) feed observed selector/cache facts; they don't graduate profile support. `PubkeyChanged` is ignored. `DataResolver`-shaped events stay `unsupported` on admitted generations and `pending` on unknown profiles. The generic `resolver_record` fact is an observation bucket, not a catch-all for unknown families.

### `ens_v1_reverse_l1`

Owns declared reverse-claim intake at the Mainnet `addr.reverse` Reverse Registrar.

| Contract | Address | `start_block` | Source |
|---|---|---|---|
| ReverseRegistrar | `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb` | `16925606` | [^v1-revreg-deploy-l379] |

No dedicated `claimed_primary_name` flag is needed for the exact-tuple persisted-readback contract.

### `ens_execution`

Owns verified resolution at the official ENS Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`[^ens-docs-univ] with `verified_resolution = "shadow"`. The pinned ENSv1 deployment artifact is the implementation/ABI anchor[^v1-ur-deploy][^v1-ursol-l8]; the route-facing entry is the proxy. The `shadow` flag records manifest ownership of the execution substrate; public verified-resolution support is gated by the route-level support classes in [`api-v1-routes.md`](api-v1-routes.md) and [`execution.md`](execution.md), not by widening this flag.

The ENS primary-name route does not introduce a second manifest capability. `ens_execution` remains the execution owner for exact-tuple persisted `verified_primary_name` readback under the same manifest.

## Capability ownership — ENSv2 (`sepolia-dev` profile)

The `sepolia-dev` profile admits four families under `manifests-sepolia-dev/ens/`. Other deployment artifacts are listed in [`upstream.md`](upstream.md) § ENSv2 `sepolia-dev` source-family narrowing.

| Family | Contract | Address | `start_block` |
|---|---|---|---|
| `ens_v2_root_l1` | `RootRegistry` | `0x3a3e15a5d27ff6f05c844313312f2e72096d3ed3` | `10462881`[^v2-deploy-root] |
| `ens_v2_registry_l1` | `ETHRegistry` (+ discovered `UserRegistry`) | `0x796fff2e907449be8d5921bcc215b1b76d89d080` | `10462895`[^v2-deploy-ethreg] |
| `ens_v2_registrar_l1` | `ETHRegistrar` | `0x68586418353b771cf2425ed14a07512aa880c532` | `10462909`[^v2-deploy-ethrc] |
| `ens_v2_resolver_l1` | `PermissionedResolverImpl` (implementation metadata) | `0xe566a1fbaf30ff7c39828fe99f955fc55544cb9c` | n/a[^v2-deploy-pres] |

`UserRegistryImpl` at `0xea93aff7375e8176053ab6ab36b57cab53cbf702` is implementation metadata, not a separate owner[^v2-userreg-l15].

Exact-name profile promotion is profile-scoped: only `exact_name_profile = "supported"` on `ens_v2_registrar_l1` in the `sepolia-dev` root graduates `.eth` exact-name declared reads, backed by `ETHRegistry` resource/token state and `ETHRegistrar` lifecycle facts[^v2-iperm-l22][^v2-events-l15][^v2-iethreg-l32]. The promotion does not apply to mainnet, other Sepolia profiles, or any runtime that hasn't selected `sepolia-dev`. Active rollout, raw preimage observations, resolver admission, or backfill completion do not graduate any other capability.

Upstream events map to normalised adapter output: `TokenResource` → `TokenResourceLinked`, `TokenRegenerated` → `TokenRegenerated`, `SubregistryUpdated` → `SubregistryChanged`, `ParentUpdated` → `ParentChanged`, `AliasChanged` → `AliasChanged`, `EACRolesChanged` → resource- or resolver-scoped permission events. These are adapter semantics, not manifest schema fields.

## Capability ownership — Basenames mainnet

Basenames mainnet admits six families.

| Family | Contract | Address | Chain |
|---|---|---|---|
| `basenames_base_registry` | `registry` | `0xb94704422c2a1e396835a571837aa5ae53285a95` | Base[^bn-registry-l10] |
| `basenames_base_registrar` | `registrar` | `0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a` | Base[^bn-baseregistrar-l15] |
| `basenames_base_resolver` | `resolver` | `0xC6d566A56A1aFf6508b41f6c90ff131615583BCD` | Base[^bn-l2resolver-l22] |
| `basenames_base_primary` | `reverse_registrar` (claim intake) | `0x79ea96012eea67a83431f1701b3dff7e37f9e282` | Base[^bn-revreg-l12] |
| `basenames_l1_compat` | `l1_resolver` (transport) | `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` | Ethereum[^bn-l1resolver-l13] |
| `basenames_execution` | `l1_resolver` (verified-resolution entrypoint) | same as above | Ethereum[^bn-l1resolver-l154] |

The L1 Resolver address appears in both `basenames_l1_compat` and `basenames_execution`. Transport ownership stays with `basenames_l1_compat`; execution entrypoint and verified-resolution routing stay with `basenames_execution`. `basenames_offchain` is reserved for later gateway admission and not part of the current split.

`basenames_execution` v2 promotes one path class: `resolver_path[0].logical_name_id` equals the route surface, `wildcard.source = null`, `alias.final_target = null`, `subregistry_path = []`, `transport.source_chain_id = "base-mainnet"`, `transport.target_chain_id = "ethereum-mainnet"`, `transport.contract_address = "0xde9049…f31"`. Alias-participating, wildcard-derived, linked-subregistry, transport-free, and offchain-gateway classes return selector-local `unsupported`[^bn-readme-l71].

`verified_primary_name` for Basenames runs through `basenames_execution` under the same flag. The matching `primary_names_current(address, coin_type, namespace)` row is the only claim-side anchor.

Base-side resolver discovery from registry `NewResolver` admits resolver instances and updates bindings[^bn-registry-l132]. Resolver-local fact consumption requires `L2Resolver`-compatible profile admission for the emitted family. The Base-side discovery rule does not discover the L1 Resolver and does not admit offchain gateways.

## Contract instance admission and continuity

Manifest loading admits source-graph nodes as `contract_instance_id`s, not raw addresses. Each active `[[roots]]` and `[[contracts]]` entry resolves to one admitted instance.

- `[[roots]]` seed canonical graph and watch-plan expansion; otherwise they follow the same identity rules as `[[contracts]]`.
- Reusing the same address on the same chain across manifest versions, even across an inactive gap, carries forward the existing `contract_instance_id` and appends a new non-overlapping active range.
- Changing a declared address closes the prior active range and admits a new instance. Continuity to the predecessor uses a `migration` edge — never id reuse.
- `proxy_kind = "none"` resolves the declared address directly; `implementation` is omitted.
- `proxy_kind != "none"` requires `implementation`. Proxy and implementation are separate instances linked by a time-ranged proxy/implementation edge.
- Changing only `implementation` keeps the proxy's identity. The implementation instance is reused if its address reappears, otherwise a new one is minted.

Contract addresses persist as time-ranged attributes for raw-fact matching and watch-plan expansion.

## Discovery admission

A discovered contract is authoritative when one of these holds:

- it's declared directly in an active manifest
- it's reachable from an active manifest root through an allowed `discovery_rules` edge
- it's explicitly allow-listed by a manifest version for a migration epoch

Each admitted edge stores `from_contract_instance_id`, `to_contract_instance_id`, source manifest version, edge kind, discovery source, active range, and provenance.

Discovery resolves `(chain, address, point in time)` to endpoint `contract_instance_id`s before storing the edge. Re-admitting an address previously admitted on the same chain reuses the prior `contract_instance_id` with a new active range. Manifest-declared and discovered proxy/implementation links share the same edge and active-range rules.

## Manifest change propagation

Manifest changes produce normalised events: `SourceManifestUpdated`, `ProxyImplementationChanged`, `CapabilityChanged`. They update discovery admission, invalidate execution cache entries, and trigger projection recomputation where capability boundaries change.

Live manifest-drift / proxy-upgrade alerting is a worker-owned operational loop. The worker computes drift candidates from admitted manifests, code-hash facts, proxy/implementation edges, and watch-plan state, and persists them to the worker-owned `manifest_alert_*` family. The worker does not write `normalized_events`, mutate manifests, mutate discovery admission, change capability flags, write projections, or expose a public route. Remediation is an explicit manifest or discovery change that produces the normal events above.

`bigname-worker manifest-drift audit --json` computes candidates, persists alert observations, and renders the persisted view alongside live counts. `--fail-on-alert --json` returns non-zero when actionable persisted alerts remain. `bigname-worker inspect manifest-drift --json` is read-only over already persisted observations.

## Watch-plan expansion

Watch-plan expansion starts from active manifest roots by `contract_instance_id` and traverses active discovery edges by id.

- The chain-intake watch target is the address range attached to each active contract instance at the requested time.
- If a manifest target carries `start_block`, the materialised watch range starts at that inclusive block unless a later active-range boundary narrows it.
- If `start_block` is omitted, the historical start is unknown. Live watch may still produce a target; automatic historical bootstrap treats it as unbootstrapable until a finite start is declared.
- Watch rows may denormalise address and code-hash state, but their durable explanation path is `manifest root → discovery edge(s) → contract_instance_id`.
- Address-only watch state is rebuildable from manifests, instance attributes, and active discovery edges.

`bigname-worker inspect watch-plan --json` exposes active watched contracts with source kind (`manifest_root`, `manifest_contract`, `discovery_edge`), source families, contract instance ids, chain addresses, source manifest ids, and active block ranges. Read-only.

## Capability policy

Capabilities gate behaviour, not public-contract existence. An unsupported capability surfaces as `coverage.unsupported_reason` or a typed error. Shadow capabilities write facts and traces without enabling general reads. Adding a new capability is additive only when it doesn't change prior semantics.

## Ownership

- Manifest/discovery owners maintain the TOML files.
- Adapter owners consume manifest versions as inputs.
- Execution owners depend on manifest versions for cache keys and invalidation.
- Schema changes require a doc-first update to this file.

## Bootstrap `start_block` provenance

Known historical starts cite a pinned upstream source. Targets without a pinned source omit `start_block`; automatic bootstrap skips them rather than inventing values. Basenames mainnet families and the ENS Universal Resolver remain unknown.

| Target | `start_block` | Source |
|---|---|---|
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

## Footnotes

[^ens-docs-univ]: <https://docs.ens.domains/resolvers/universal/>
[^v1-app-resolvers]: (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L32 @ ens_app_v3@7175858)
[^ensnode-mainnet]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L343 @ ensnode@9b8f590)

[^subgraph-l15]: (upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)
[^subgraph-l39]: (upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)
[^subgraph-l122]: (upstream: .refs/ens_subgraph/subgraph.yaml:L122 @ ens_subgraph@723f1b6)
[^subgraph-l145]: (upstream: .refs/ens_subgraph/subgraph.yaml:L145 @ ens_subgraph@723f1b6)
[^subgraph-ts-l134]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L134 @ ens_subgraph@723f1b6)
[^subgraph-ts-l230]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L230 @ ens_subgraph@723f1b6)
[^subgraph-ts-l238]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6)
[^subgraph-ts-l246]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6)

[^v1-iname-l27]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)
[^v1-namewrapper-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f)
[^v1-publicresolver-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f)
[^v1-revreg-deploy-l379]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L379 @ ens_v1@91c966f)
[^v1-ur-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)
[^v1-ursol-l8]: (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f)
[^v1-wrapethrc-l640]: (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L640 @ ens_v1@91c966f)
[^v1-ethrc-l706]: (upstream: .refs/ens_v1/deployments/mainnet/ETHRegistrarController.json:L706 @ ens_v1@91c966f)
[^v1-ensregfb-l40]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L40 @ ens_v1@91c966f)
[^v1-ensreg-l89]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)

[^v2-deploy-root]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2617 @ ens_v2@554c309)
[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2617 @ ens_v2@554c309)
[^v2-deploy-ethrc]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L1922 @ ens_v2@554c309)
[^v2-deploy-pres]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/PermissionedResolverImpl.json:L2 @ ens_v2@554c309)
[^v2-userreg-l15]: (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L15 @ ens_v2@554c309)
[^v2-iperm-l22]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L22 @ ens_v2@554c309)
[^v2-iethreg-l32]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309)
[^v2-events-l15]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309)

[^bn-registry-l10]: (upstream: .refs/basenames/src/L2/Registry.sol:L10 @ basenames@1809bbc)
[^bn-registry-l132]: (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
[^bn-baseregistrar-l15]: (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L15 @ basenames@1809bbc)
[^bn-l2resolver-l22]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
[^bn-revreg-l12]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
[^bn-l1resolver-l13]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
[^bn-l1resolver-l154]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)
[^bn-readme-l71]: (upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc)
