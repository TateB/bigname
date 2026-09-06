# Source Manifests

Manifests pin watched contracts, capability ownership, and rollout state per [source family](glossary.md). They are part of the truth model, not deploy-time configuration. The wider model lives in [`architecture.md`](architecture.md); persistence in [`storage.md`](storage.md); intake in [`chain-intake.md`](chain-intake.md); execution in [`execution.md`](execution.md).

## File format and location

Manifests are TOML files at:

```text
manifests/<profile>/<chain_combo>/<namespace>/<source_family>/<version>.toml
```

[Deployment profiles](glossary.md) select a corpus, and chain-combo directories partition the chains inside that corpus:

```text
manifests/mainnet/ethereum/<namespace>/<source_family>/v1.toml
manifests/mainnet/base/<namespace>/<source_family>/v1.toml
manifests/sepolia/ethereum/<namespace>/<source_family>/v1.toml
manifests/sepolia/base/<namespace>/<source_family>/v1.toml
```

One runtime selects exactly one manifest profile root at startup — `manifests/mainnet/` for the shipped mainnet profile, or `manifests/sepolia/` for the Sepolia profile. Deployment-profile selection is not a manifest schema change. A runtime never loads two profile roots into the same canonical corpus, [watch plan](glossary.md), discovery graph, or [projection](glossary.md) set.

Phase-runner executes its retained-ENS validation and the complete manifest
mutation transaction on the same PostgreSQL session that owns the startup
advisory lock; losing that session aborts the transaction.

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
- `resolver_implementations` — optional declared implementation artifacts and
  implementation-sensitive `read_features` for upgradeable resolver families
- `correlation_addresses` — optional named, validated EVM addresses used only
  to correlate decoded observations across declared emitters; these entries do
  not declare contracts, add discovery edges, or widen the watch plan
- `capability_flags`
- `roots`
- `contracts`
- `discovery_rules`

For one `(namespace, source_family, chain)` tuple in a selected deployment-profile root, at most one manifest version may declare `rollout_status = "active"`. Zero active versions remains valid for a family whose versions are only `draft`, `shadow`, or `deprecated`. Each `(namespace, source_family, chain, deployment_epoch, manifest_version)` tuple may come from only one file; the loader rejects duplicate tuples across repository layouts regardless of rollout status. Within one manifest version, every `[[contracts]].role` must be unique. The loader rejects these violations before repository sync.

Every active `ens_v2_migration_l1` manifest requires both
`correlation_addresses.ens_v1_name_wrapper` and
`correlation_addresses.ens_v1_base_registrar`, because Interpret consumes both
addresses on every ENSv1→ENSv2 migration batch. When the corresponding active
`ens_v1_wrapper_l1` or `ens_v1_registrar_l1` family is [admitted](glossary.md#admission) for the same
namespace and chain, the loader also requires the correlation value to equal
the family's `name_wrapper` or `registrar` contract address. A missing key,
missing contract role, or mismatch fails manifest loading before any declaration
can affect a watch plan.

The loader also rejects two active manifest versions on the same chain when both declare the same address as roots or both declare it as contracts, their open-ended `start_block` ranges overlap, and either family feeds manifest-declared event data into `PreimageObserved` rows produced directly from block logs. Those families are `ens_v1_registrar_l1`, `basenames_base_registrar`, `ens_v1_wrapper_l1`, `ens_v2_root_l1`, `ens_v2_registry_l1`, `ens_v2_registrar_l1`, `ens_v2_resolver_l1`, and `ens_v2_migration_l1`. This check does not compare a root declaration with a contract declaration or inspect `correlation_addresses`. Two roots or two contracts may still share an address when neither family is in that list; this is why the shared `l1_resolver` declaration in `basenames_l1_compat` and `basenames_execution` is accepted.

Each `[[roots]]` and `[[contracts]]` entry may declare an optional `start_block`.
When resolver classification sees repeated declarations for one address, it selects the matching
entry with the greatest `start_block` at or below the target; equal starts select the later manifest-array entry.
`start_block` is the inclusive first historical block for that target. Omitted
means deployment provenance is unknown, and manifest storage preserves it as
null. Runtime watch and Interpret selection use block zero as its conservative
lower bound within an admitted phase range. That authorizes intake from zero;
it does not claim that the contract was deployed at genesis.

For `[[contracts]]`, `proxy_kind` is required. `proxy_kind = "none"` omits
`implementation`. Any non-`none` `proxy_kind` includes `implementation` as the
expected implementation address for that manifest version. Manifest
synchronization stores that expectation as a
[proxy/implementation discovery edge](glossary.md#discovery-graph--discovery-edge).
When no current `Upgraded` observation exists, the manifest-declared edge is the
active baseline. An `Upgraded` observation naming a different implementation
closes that baseline and makes the observed edge current. An observation naming
the same implementation records event-derived evidence alongside the baseline.
Later manifest synchronization preserves either current observed edge instead
of resetting it to the declaration. The declared implementation remains
deployment metadata used to seed the baseline. There is no separate runtime
manifest-drift observation job. Baseline
materialization and handling of upgrade observations are the schema-v2
consumers that keep `proxy_kind` in the manifest schema.

`resolver_implementations` is a list of `{ role, address, read_features? }`
entries with unique addresses; several implementation generations may share
one role. `[[contracts]]` also accepts `read_features`. Each feature list is
deduplicated and uses the closed `ensip19_default_address` vocabulary in this
release. Unknown or duplicate values fail loading. Contract-level features are
valid only for resolver roles with `proxy_kind = "none"`; proxy-sensitive
features belong on the implementation declaration. A family with any
`resolver_implementations` entries rejects contract-level read features rather
than choosing between two authority forms. All direct contract declarations
for the same case-normalized address must also declare the same feature set;
role ordering never resolves conflicting getter authority. An empty list is
the default.

A [resolver read feature](glossary.md#resolver-read-feature) authorizes getter
behavior, not an event family or watch-plan expansion. Direct resolvers use the
features on their exact active contract declaration. A resolver proxy uses only
the features on the implementation selected by latest canonical `Upgraded`
history; features from older implementations are never unioned.
`ensip19_default_address` authorizes reading `addr:2147483648` when an eligible
requested EVM coin-type entry is empty. Eligibility follows
`chainFromCoinType(coinType) > 0`: coin type `60` and
`2147483649..=4294967295` are targets, while `2147483648` is the source key and
does not recurse. The read feature authorizes the fallback source; serving then
uses the requested getter's verified decode. A derived coin-type-60 zero address
is `not_found`, while an EVM-range multicoin request preserves the same non-empty
20 zero bytes. This target-specific normalization does not alter exact stored
records.
(upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L9-L38 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L68-L85 @ ens_v1@91c966f)
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L685-L697 @ ens_v2_sepolia_20260629@ccaeb58)

The current Mainnet ENS PublicResolver, the Sepolia PublicResolver at
`0xE99638b40E4Fff0129D56f03b55b6bbC4BBE49b5`, and the admitted archived-Sepolia
ENSv2 `PermissionedResolver` implementation declare this feature. Legacy ENS
resolver generations remain unflagged: bigname makes no derived-read claim for
them even if retained events exist. The current ENS contract composes
`AddrResolver`, and first-party app metadata identifies both admitted current
PublicResolver declarations as supporting the default coin type.
(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20-L31 @ ens_v1@91c966f)
(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L32-L40 @ ens_app_v3@7175858)
(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L151-L166 @ ens_app_v3@7175858)
The archived ENSv2 deployment identifies the admitted implementation, whose
embedded compiled-source metadata applies the same empty-address fallback.
(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L2 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L2398 @ ens_v2@a971bd64)

The admitted Basenames address is the legacy L2 resolver. It imports the
vendored exact-storage address resolver, so it deliberately carries no
`ensip19_default_address` feature. The fallback-bearing implementation is used
by the separate upgradeable resolver proxy, which is not admitted in this
manifest and is deferred to a follow-up admission decision.
(upstream: .refs/basenames/test/Fork/BaseMainnetConstants.sol:L9-L14 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/L2Resolver.sol:L4-L32 @ basenames@1809bbc)
(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/AddrResolver.sol:L35-L61 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/UpgradeableL2Resolver.sol:L11-L40 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L84-L99 @ basenames@1809bbc)

`resolver_implementations` does not admit an implementation as a watched
emitter. The project phase uses
the list only to classify a discovered ENSv2 resolver proxy after canonical
ERC-1967 `Upgraded` history identifies its current implementation. ENSv1 and
Basenames resolver classification instead requires the resolver address itself
to be an active `[[contracts]]` declaration. Neither path reads or infers a
runtime code hash.

For `[[discovery_rules]]`, the only authorable `admission` value is `reachable_from_root` — the discovered edge is authoritative while its `from_role` endpoint remains reachable from an active manifest root under an allowed rule. Internal labels are storage tags, not authored values: `manifest_declared` is an `admission_basis`, and `manifest_declared_proxy` is the `discovery_source` written alongside it for manifest-declared proxy edges.

`[abi]` is optional. When present, it declares the Solidity ABI fragments that this manifest version authorizes for adapter, execution, or watch-plan use. ABI entries are source-family metadata; they do not by themselves promote public capability support.

### `capability_flags`

Each flag carries a name, a status (`unsupported` | `shadow` | `supported`), and optional notes.

Capability flags are product-facing declarations. A source family that owns
intake or diagnostic history without owning a public consumer capability uses
an empty `[capability_flags]` table. Internal pipeline labels must not be added
as capability keys: the product namespace route intentionally maps a closed
set of declared capability names, while the diagnostics manifest route exposes
the complete source-family metadata.

### `chain`

`chain` names the authority chain for that manifest within the selected deployment profile. Mainnet manifests use chain IDs like `ethereum-mainnet` and `base-mainnet`. Sepolia support is additive as a separate manifest profile root and chain-ID set.

### `abi`

ABI entries use Alloy-parseable human-readable Solidity fragments, not handwritten selectors or topic hashes. The loader validates each fragment with Alloy and derives event topic0 values, function selectors, canonical signatures, indexed parameters, inputs, and outputs from the fragment. Authored selector/topic fields are intentionally absent.

`[[abi.events]]` entries contain:

- `name` — must match the parsed event name.
- `fragment` — a human-readable event fragment such as `event ResolverUpdated(uint256 indexed node, address resolver, address sender)`.
- `emitter_roles` — optional `[[contracts]].role` values that may emit the event. An empty list is
  valid only for a documented
  [emitter-role-independent event](glossary.md#emitter-role-independent-event). The only exception
  outside that finite list is `RegistryCreated` in `ens_v2_registry_l1` when the manifest has a
  `registry_announcement` discovery rule; that event may match without an address admission. Other
  events without `emitter_roles` fail manifest validation.
- `normalized_events` — optional normalized event kinds produced from the event.
- `status` — optional `unsupported` | `shadow` | `supported` marker for the ABI entry.
- `notes` — optional reviewer-facing context.

`[[abi.calls]]` entries contain:

- `name` — must match the parsed function name.
- `fragment` — a human-readable function fragment such as `function resolver(bytes32 node) view returns (address)`.
- `target_roles` — optional `[[contracts]].role` values that may be called.
- `status` — optional `unsupported` | `shadow` | `supported` marker for the ABI entry.
- `notes` — optional reviewer-facing context.

An [intake-only event](glossary.md#intake-only-event) may be declared for raw
intake without promising a manifest-owned normalized-event mapping by declaring
an empty `normalized_events` list. This is not an unrestricted
adapter-admission bypass. [Standard approval](glossary.md#standard-approval-derivation) declarations must retain the empty
list: their ABI decoding is declaration-backed, while any normalized output is
an adapter-owned mapping versioned by the
[interpreter content hash](glossary.md#interpreter-content-hash). The admitted
ENSv1 and Basenames registry `ApprovalForAll` declarations map to
`AccountPermissionChanged`; registrar, resolver, and NameWrapper approvals
remain decoded with no normalized output. Standard approval events are watched
only at explicitly declared, role-eligible
contract addresses and their declared historical intervals; they are not added
to generic resolver [all-emitter watches](glossary.md#watch-plan--watched-tuple).
The admitted Sepolia legacy registry artifact declares that exact
`ApprovalForAll(owner, operator, approved)` event.
(upstream: .refs/ens_v1/deployments/sepolia/LegacyENSRegistry.json:L2-L32 @ ens_v1@91c966f)
Raw capture does not by itself imply permission interpretation or complete
permission coverage.

ABI fragments should cite upstream in nearby manifest comments or in the public doc section that admits the source family. If an adapter still has an in-code selector or `sol!` definition for a manifest-declared fragment, that code is a compatibility bridge until the adapter consumes the manifest ABI directly.

`normalizer_version` is currently `ensip15@ens-normalize-0.1.1` for all admitted
ENS, ENSv2, and Basenames source families. Runtime code treats this as one
shared normalization boundary, not a per-source-family choice. Manifest
validation reads the same
`bigname_domain::normalization::ENS_NORMALIZER_VERSION` export that name
interpretation stamps on stored rows; it does not carry a second
normalizer-version literal.

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

`ens_execution` owns verified resolution at the ENS Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` with `verified_resolution = "shadow"`.[^ens-docs-univ][^v1-ur-deploy][^v1-ursol-l8] The pinned `.refs/` artifact is the implementation/ABI anchor; the lookup entry is the proxy address. The shadow flag records manifest ownership for the execution substrate; public ENS verified-resolution support is gated by the route-level support classes in `docs/api-v2-routes.md` and `docs/execution.md`, not by widening this manifest flag.

The ENS primary-name route does not introduce a second manifest capability. `ens_execution` supplies the manifest selection for the request-scoped, hash-pinned ENS/60 missing-tuple lookup under the same owner manifest, without turning `verified_resolution = "shadow"` into a route-level primary-name support flag. Indexed exact-tuple claim state lives in `bigname_phase.primary_names_current`; provider lookup responses are not persisted as execution outcomes or traces.

`ens_v1_reverse_l1` owns declared reverse-claim intake at the Mainnet `addr.reverse` Reverse Registrar `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`.[^v1-revreg-deploy][^v1-revreg-l15][^v1-revreg-l19] No dedicated `claimed_primary_name` flag is needed for that indexed claim-state contract.

`ens_v1_registry_l1` owns the current ENS registry at `0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E` with `start_block = 9380380`,[^subgraph-l15] plus `ENSRegistryOld` at `0x314159265dd8dbb310642f98f50c066173c1259b` with `start_block = 3327417` as old-registry [fallback-handoff](glossary.md#registry-fallback-handoff) input.[^subgraph-l39][^subgraph-l44] Old-registry logs do not union with current logs by latest block: a current-registry `NewOwner` or `Transfer` establishes the node's current-registry record; later old-registry `NewOwner`, `Transfer`, `NewTTL`, and non-root `NewResolver` updates for that node are suppressed.[^subgraph-ts-l134][^subgraph-ts-l230][^subgraph-ts-l238][^subgraph-ts-l246] Either ownership event creates the first current-registry record and ends a resolver pointer selected from the old registry because `ENSRegistryWithFallback.resolver` delegates only while that record does not exist. Resolver selections already made in the current registry survive later owner reassignments, including an old-registry `Transfer`. (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L24 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L68 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L82 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L54 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L150-L172 @ ens_v1@91c966f) Root-resolver updates from the old registry are the one frozen exception.[^v1-ensregfb-l40]

`ens_v1_registrar_l1` owns `.eth` BaseRegistrar at `start_block = 9380410`[^subgraph-l122] plus the legacy, wrapped, and current ETHRegistrarController contracts as label-bearing intake (LegacyEthRegistrarController `9380471`,[^subgraph-l145] WrappedETHRegistrarController `16925618`,[^v1-wrapethrc-l640] current ETHRegistrarController `22764821`).[^v1-ethrc-l706] Controllers do not split into a separate source-family owner. The wrapped controller's `NameRenewed` additionally derives a wrapper-resource expiry observation under this family because its renewal calls `NameWrapper.renew`, which stores registrar expiry plus grace without emitting `ExpiryExtended`. (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L318 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L333 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L337 @ ens_v1@91c966f)

`ens_v1_wrapper_l1` owns NameWrapper at `0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401` with `start_block = 16925608`,[^v1-namewrapper-deploy] for wrapper authority, direct fuse/expiry observations, wrapper-revealed names, and wrapper-driven registry changes.[^v1-iname-l27][^v1-iname-l35][^v1-iname-l37][^v1-iname-l38]

`ens_v1_resolver_l1` owns ENS Labs PublicResolver-generation profile admission. The seed entry is the latest PublicResolver at `0xF29100983E058B709F3D539b0c765937B804AC15` with `start_block = 22764828`.[^v1-publicresolver-deploy] [Resolver-profile](glossary.md) [admission](glossary.md) is exact-address classification from this declared list. It permits the project phase to publish the canonical normalized observations retained for that resolver; it does not prove exhaustive history or event-to-call parity. Unadmitted resolvers are explicitly unsupported.

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

Older rows do not inherit latest-only behavior. Unsupported interfaces and pending resolver profiles surface explicitly through `coverage`, `UnsupportedSummary`, `resolver_family_pending`, or `resolver_family_unsupported`. They are never reported as absent records.

Address-specific resolver `start_block`s come from ENSNode datasource pins where available: `0x1da0...` `3648359`, `0x5FfC...` `3733668`, `0x2261...` `8659893`, `0x4976...` `9412610`, `0x231b...` `16925619`.[^ensnode-mainnet] `0xDaaF...` has no pinned datasource; it uses the current ENSRegistry epoch `9380380` as a conservative bootstrap basis. The OffchainDNSResolver and ExtendedDNSResolver app-known maps remain deferred — they are not PublicResolver-generation profile admissions.

Registry `NewResolver(node, resolver)` changes only the node-to-resolver binding.[^v1-ens-l12][^v1-ensreg-l89][^v1-ensreg-l174] It does not discover or admit a resolver contract. Resolver-local history is selected across all emitting addresses by the ENS resolver signature set declared in `ens_v1_resolver_l1`; no discovered-address edge is required.[^v1-resolver-signature-abis] A matching event produces a record-history observation for a known ENS node even when that resolver is not the node's current resolver. Current record visibility still follows the node's resolver pointer. Resolver-profile admission remains the separate gate for complete-family coverage, parity, and supported resolver claims.

The no-value `TextChanged(bytes32,string,string)` event — the legacy signature that carries the record key but no new value — is decoded by topic count wherever this family’s signature-set selection applies, which is every emitting address, the same scope as the indexed layout. Three-topic logs decode with the indexed key. Two-topic logs, where the key is unindexed and both strings sit in the event data, are accepted only when the two strings are byte-equal and the key is nonempty. Non-whitespace UTF-8 keys without NUL bytes use the plain selector; every other nonempty key, including whitespace-only bytes, uses the opaque selector. The same nonempty/plain-versus-opaque key rule applies to value-bearing ENSv1, ENSv2, and Basenames `TextChanged` events. An accepted log produces the same normalized `RecordChanged` observation as the indexed-key layout, with no retained value. Two-topic logs whose strings differ are explicitly unsupported and skipped — third-party contracts reuse this signature with `(key, value)` semantics, which the byte-equality condition rejects. A two-topic log that fails decoding is ignored as a malformed lookalike only when its emitter is not declared in the active manifest; an undecodable log from a declared resolver remains a hard interpretation error even though this family is selected across every emitting address. The 2019 PublicResolver `0x226159d592E2b063810a10Ebf6dcbADA94Ed68b8` is the only admitted instance that emits the two-topic shape on mainnet, and reference indexers drop those logs because their ABIs declare the key indexed (upstream: .refs/graph_node/chain/ethereum/src/data_source.rs:L745-L774 @ graph_node@aefe173) (upstream: .refs/ponder/packages/core/src/utils/decodeEventLog.ts:L34-L47 @ ponder@c8f6935) (upstream: .refs/ponder/packages/core/src/runtime/events.ts:L556-L581 @ ponder@c8f6935). The pinned goerli `LegacyPublicResolver` deployment ABI records the unindexed-key layout (upstream: .refs/ens_v1/deployments/goerli/LegacyPublicResolver.json:L508-L532 @ ens_v1@91c966f), and the vendored legacy source emits the same key in both string positions (upstream: .refs/ens_v1/deployments/mainnet/solcInputs/08371ea78d6ca0259dbc9b2f768cf73e.json:L71 @ ens_v1@91c966f); the full upstream evidence — including the indexed-key reference ABIs and each reference indexer's drop path — and the mainnet census live in `docs/upstream.md` § Known divergences.

`PubkeyChanged` is ignored by the current admission model. `DataChanged` is
admitted and normalizes generically to `RecordChanged` wherever this family's
signature-set selection applies. No admitted Mainnet PublicResolver generation
profile declares `DataResolver` composition, so that event admission does not
confer `DataResolver`-family support; unknown resolver profiles remain
`pending`. The generic `resolver_record` fact is an observation bucket; it does
not act as a catch-all for unknown families.

### ENSv2 (`sepolia` deployment profile)

The `sepolia` deployment profile currently admits five ENSv2 families from the admitted post-audit 2026-06-29 Sepolia deployment — archived upstream at `contracts/deployments/sepolia-20260629-r1/` (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/.deployment.json:L4 @ ens_v2@a971bd64); upstream's 2026-07-30 redeploy is not admitted (upstream: .refs/ens_v2/contracts/deployments/sepolia/.deployment.json:L4 @ ens_v2@a971bd64) (see [`upstream.md` § Known divergences](upstream.md#known-divergences)) — under `manifests/sepolia/ethereum/ens/`, all in `deployment_epoch = "ens_v2_sepolia_post_audit"`:[^v2-deploy-root][^v2-deploy-ethreg][^v2-deploy-ethrc][^v2-deploy-pres]

- `ens_v2_root_l1` — `RootRegistry` at `0x11b5bfbe9078d826b1edbdd1cfc12f5828d9f50c`, `start_block = 11163319`. The admitted deployment artifact identifies that address and names the contract type `PermissionedRegistry`. (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/RootRegistry.json:L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/RootRegistry.json:L1670 @ ens_v2@a971bd64) It is a tokenized, [resource](glossary.md)-scoped permissioned registry seed for parent graph state.[^v2-pr-l22][^v2-pr-l28] Its `root_registry` role records `subregistry` edges as name topology only: they do not admit or watch the child registry without an independent `RegistryCreated` announcement. The same role separately admits `resolver` addresses through `reachable_from_root`, targeting `ens_v2_resolver_l1`. `PermissionedRegistry` explicitly emits `ResolverUpdated` when a resolver is set and also emits it when registration supplies a nonzero resolver. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L150-L154 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L477-L478 @ ens_v2@a971bd64) The RootRegistry manages TLDs, so its empty suffix anchor represents a registered root label as the existing single-label logical name; the root-discovery projection regression proves that its resolver pointer and records attach to that same resource rather than creating a separate serving model. (upstream: .refs/ens_v2/docs/indexing-ensv2-events.md:L505-L509 @ ens_v2@a971bd64)
- `ens_v2_registry_l1` — `ETHRegistry` at `0x67b728a792e789a8978b30cf1b3b641f19354b43`, `start_block = 11163391`, plus registry instances announced by `RegistryCreated()`. Direct `PermissionedRegistry` construction emits the announcement first; a `UserRegistry` proxy emits it during initialization. It admits the emitting address from that exact log position without requiring a parent link. `UserRegistryImpl` at `0x840fa461059862ea466a711e8c98c8de732061c0` is implementation metadata, not a separate owner. (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@a971bd64) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registry/UserRegistry.sol:L43 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registry/UserRegistry.sol:L47 @ ens_v2_sepolia_20260629@ccaeb58)[^v2-userreg-l15]
- `ens_v2_registrar_l1` — `ETHRegistrar` at `0xa4449a0dd2b83007553d9b1d28b583a46a805a30`, `start_block = 11163403`. Admitted registration and renewal lifecycle facts; registered-name resource identity links back to the registry resource.[^v2-ethrc-l49][^v2-ethrc-l173]
- `ens_v2_resolver_l1` — resolver contract instances discovered from either `ens_v2_registry_l1` or `ens_v2_root_l1` retain the manifest-configured normalized record and record-version observations; an applicable same-namespace exact contract declaration instead controls raw-log interpretation under the [declaration-precedence rule](architecture.md#discovery-graph). `PermissionedResolver` instances additionally provide alias, named-resource, and resolver-scoped EAC events. Resolver-local projection is supported only when the proxy's latest canonical ERC-1967 `Upgraded` event names an implementation in the active manifest's `resolver_implementations` list. The current declared `PermissionedResolverImpl` is `0x7e4b2d59938930168024201752ee5503df402303`; the contract inherits UUPS upgradeability and its deployment ABI exposes `Upgraded(address)`.[^v2-deploy-pres][^v2-pres-uups][^v2-pres-upgraded] A manifest admission change reclassifies the affected resolver inline during project-phase publication. No code-hash observation participates.
The fifth family, `ens_v2_migration_l1`, covers fixed ENSv1→ENSv2 migration
controllers, terminal-holder and renewal-bridge markers, factory history,
batch-reservation sender metadata, and scoped ENSv1 BaseRegistrar correlation.
Its admission and schema prerequisite are specified below.

The preceding `ens_v2_sepolia_dev` manifest versions remain checked in as `deprecated` historical records and citation evidence. Their addresses and ranges do not participate in the active post-audit watch or replay plan.

### ENSv1 (`sepolia` deployment profile)

The `sepolia` deployment profile also admits the ENSv1 deployment the ENSv1→ENSv2 migration family
bridges from, in `deployment_epoch = "ens_v1"`:

- `ens_v1_registry_l1` — `ENSRegistry` at `0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e`, `start_block = 3702728`, plus the superseded registry it falls back to as `registry_old` at `0x94f523b8261B815b87EFfCf4d18E6aBeF18d6e4b`, `start_block = 3702721`. The deployed current registry is a fallback registry whose constructor takes that older registry as its `old` delegate, matching the fallback-registry pairing in the mainnet deployment profile.[^v1-sepolia-ensregistry][^v1-sepolia-legacyregistry][^v1-sepolia-fallback]
- `ens_v1_registrar_l1` — `.eth` BaseRegistrar at `0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85`, `start_block = 3702731`. The address is pinned by the ENSv1 deployment artifact, but that artifact's receipt belongs to a superseded deployment; the start block therefore follows explicit reference-only ENS subgraph metadata and is recorded as a divergence in `docs/upstream.md`.[^v1-sepolia-baseregistrar][^v1-sepolia-baseregistrar-stale-receipt] The family is the sole owner of BaseRegistrar log attribution on this deployment profile. Its ordinary `Transfer` observations restore `.eth` registrar predecessor and fallback state, including after `NameUnwrapped`; its controller-change and numeric registration/renewal observations become candidate `ens_v2_migration_l1` rows only through the launch-bounded correlation rule below. No Sepolia registrar-controller address is admitted.
- `ens_v1_wrapper_l1` — `NameWrapper` at `0x0635513f179D50A207757E05759CbD106d7dFcE8`, `start_block = 3790153`. This is the contract the ENSv1→ENSv2 migration family names in `correlation_addresses.ens_v1_name_wrapper`; admitting it is what makes a migrated child's ENSv1 cleanup observable.[^v1-sepolia-namewrapper]
- `ens_v1_resolver_l1` — the complete four-address first-party app known-resolver list below. Direct `contracts` declarations provide the exact supported-address classification set. As on Mainnet, the resolver signatures use family-wide match-all intake; retained events from another emitter remain unsupported unless that exact address is declared. The family uses the same ordered event declarations as Mainnet.[^v1-sepolia-app-resolvers]

| Role | Address | Generation | Wrapper-aware | Start source | `start_block` |
| --- | --- | --- | --- | --- | ---: |
| `public_resolver` | `0xE99638b40E4Fff0129D56f03b55b6bbC4BBE49b5` | latest | yes | deployment receipt | `8580001` |
| `public_resolver_8948458` | `0x8948458626811dd0c23EB25Cc74291247077cC51` | outdated | yes | conservative zero lower bound | `0` |
| `public_resolver_8fade66` | `0x8FADE66B79cC9f707aB26799354482EB93a5B7dD` | outdated | yes | conservative zero lower bound | `0` |
| `public_resolver_0ceec52` | `0x0CeEC524b2807841739D3B5E161F5bf1430FFA48` | legacy | no | deployment receipt | `3790166` |

The two zero values are conservative watch lower bounds, not asserted deployment blocks; they use the existing [retired automatic-bootstrap divergence](upstream.md#known-divergences). Reverse resolvers, `ExtendedDNSResolver` / `OffchainDNSResolver`, and `UniversalResolver` are excluded. `OwnedResolver` is also outside the closed family: the pinned Mainnet deployment set records `EthOwnedResolver` and its `.eth`-level setup, but Mainnet's resolver manifest does not admit it; Sepolia's deployment artifact identifies `0x15222A1C2Bf3A4c24eAd1634B8Ee399fd95c3aaf` at block `3790128`, and that address is absent from the app's approved resolver list.[^v1-mainnet-owned-resolver][^v1-sepolia-owned-resolver]

The normalized ENSv1 resolver address set is disjoint from every address-bearing field in the active Sepolia `ens_v2_resolver_l1` manifest; its complete resolver-side set is the `permissioned_resolver` implementation metadata address `0x7e4b2d59938930168024201752ee5503df402303`. That implementation is v2 classification metadata, not an ENSv1 direct-address admission. The ENSv2 deployment's premigration registrar assigns the separate `ENSV1Resolver` mirror at `0x5339161a7896ca9841ecc034a49edca40f7b9491`; that mirror finds the selected resolver through the ENSv1 registry and forwards resolution there. The serving-side mirror stays in the ENSv2 deployment, while v1 record ingestion and exact resolver classification stay in `ens_v1_resolver_l1`.[^v2-sepolia-v1-mirror]

The latest resolver declares `read_features = ["ensip19_default_address"]`, matching its app metadata and inherited `AddrResolver` fallback behavior. Project therefore publishes the ENSIP-19 default-address read rule for `0xE99638b40E4Fff0129D56f03b55b6bbC4BBE49b5`; the other three generations remain unflagged.[^v1-sepolia-app-resolvers]

Admission is necessary but not sufficient for a migrated child's cleanup to be
observable. Both cleanup shapes — the wrapper token parked in the Graveyard, and
the node unwrapped into it — are derived against wrapper state that only exists
if that child's original `NameWrapped` was itself ingested. A source's ingest
floor is operator-configured per run, not derived from a manifest
`start_block`, so a Sepolia runtime started at the ENSv2 floor admits the
wrapper family but still sees no cleanup for children wrapped earlier. Deriving
child boundaries on this profile requires an ingest floor at or below the
wrapper's `start_block`, and below each child's own wrap block.

No ENSv1 registrar-controller contract is admitted on this deployment profile. The ordinary BaseRegistrar token lifecycle is present, but label-bearing registration and renewal observations emitted by registrar controllers are absent. Registrations visible only as numeric BaseRegistrar events establish no ordinary registrar identity after a full lapse, so re-registrations in that coverage gap do not independently restore an exact `.eth` name surface. The pinned `LegacyETHRegistrarController` at `0x7e02892cfc2Bfd53a75275451d73cF620e793fc0`, from block `3790197`, and `ETHRegistrarController` at `0xfb3cE5D01e0f33f41DbB39035dB9745962F1f968`, from block `8579988`, have receipt-backed deployment records. They remain outside this part because admitting either or both would only partially widen label-bearing intake and would not cover the wrapped-controller path that exposes the #515 gap.[^v1-sepolia-receipt-backed-controllers] This part therefore adopts #515 option (b); a separate controller capability slice owns any later admission.

The pins also carry a tracked Sepolia v1-reference address for `WrappedETHRegistrarController`, `0xFED6a969AaA60E4961FCD3EBF1A2e8913ac65B72`, and the ENSv2 `ETHRenewerV1` constructor data names the same controller. That reference artifact contains only the address and ABI, however: it has no deployment transaction, receipt, or historical start block. The ENS subgraph and ENSNode cross-check references both pair the address with block `3790244`, but those references do not supply authoritative deployment provenance. Unlike the explicit BaseRegistrar exception above, bigname does not elevate that cross-check metadata into a controller watch-plan floor, so the controller remains unadmitted and its admission remains deferred.[^v1-sepolia-wrapped-controller-gap]
Registrar-controller coverage remains a known asymmetry against the mainnet deployment profile; resolver-log coverage for the approved four-address set is no longer one.

An ordinary active name that carries both current ENSv1 and ENSv2 arms on this
deployment profile, without an admitted authority proof, qualifying release, or
deployment-wide ENSv2 release-threshold decision, is
[`independent_ens_deployments_overlap`](architecture.md) rather than a chosen
authority. The runtime admits evidence from both protocol eras, but only an
admitted ENSv1→ENSv2 migration boundary establishes per-name authority between
them.
The exact [shared ENS infrastructure](glossary.md#shared-ens-infrastructure)
names—root, `eth`, `reverse`, and `addr.reverse`—instead select ENSv2 when the
ENSv2 arm is current, ENSv1 evidence exists as either a current binding or
historical events, and none of those higher-precedence decisions applies.
Historical ENSv2 evidence alone does not qualify, and descendants do not
inherit the exception. A current ENSv2 arm without ENSv1 evidence remains the
ordinary single-arm ENSv2 case.
Admitting ENSv1 sources here
makes ordinary overlap reachable in production for the first time; it does not
establish an ENSv1→ENSv2 migration boundary.

`exact_name_profile` [capability promotion](glossary.md) is deployment-profile-scoped: only `exact_name_profile = "supported"` on the active `ens_v2_registrar_l1` version in the `sepolia` root promotes `.eth` exact-name declared reads to supported, backed by `ETHRegistry` resource/token state and `ETHRegistrar` lifecycle facts.[^v2-iperm-l22][^v2-events-l15][^v2-iethreg-l32] The admitted ENSv1 registrar remains `shadow` because registrar-controller label coverage is absent, so the product namespace route aggregates the two declarations as `name_profile.completeness = "partial"`; this does not demote the ENSv2 family-level support. The capability promotion does not apply to mainnet, another deployment profile, or any runtime that has not selected `manifests/sepolia`. Active rollout, raw preimage observations, resolver admission, or backfill completion promote no other capability.

Upstream events map to normalized adapter output: `TokenResource` →
`TokenResourceLinked`; `TokenRegenerated` → `TokenRegenerated`, plus the
existing terminal and registry-path reconciliation kinds `SurfaceUnbound`,
`RegistrationReleased`, `PreimageObserved`, `SurfaceBound`,
`RegistrationGranted`, `AuthorityTransferred`, `ExpiryChanged`,
`ResolverChanged`, and `SubregistryChanged` — the displaced-registration
closures only when an admitted noncanonical registry regenerates onto a token
key occupied by another registration, and the name-refresh reconciliation
kinds (reassertion `PreimageObserved`, expiry-crossing retirements) on any
regeneration, because the passive-expiry refresh runs at every
`TokenRegenerated`; each positive-value item in
`TransferSingle` or `TransferBatch` with nonzero `from` and `to` →
`TokenControlTransferred`; `SubregistryUpdated` → `SubregistryChanged`;
`ParentUpdated` → `ParentChanged`; `AliasChanged` → `AliasChanged`;
`EACRolesChanged` → resource- or resolver-scoped permission events.[^v2-iperm-l34][^v2-events-l49][^v2-events-l69][^v2-events-l75][^v2-iperm-resolver-l14][^v2-eac-l19]
The deployed `ETHRegistry` and `UserRegistryImpl` ABIs both contain the transfer
events, and upstream changes the stored owner only for a positive value; mint
and burn use a zero endpoint and therefore do not become token-control
transfers. (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistry.json:L652 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistry.json:L689 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/UserRegistryImpl.json:L723 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/UserRegistryImpl.json:L760 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L194 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L201 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L208 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L210 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L318 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L333 @ ens_v2@a971bd64)
These are adapter semantics, not manifest schema fields. Role changes remain
permission events and are not ownership evidence.

ENSv2 terminal lifecycle events also close interpreter-owned state. `LabelUnregistered` is emitted before upstream expires the entry and has no paired zero-target subregistry or resolver updates, so the ENSv2 interpreter closes the current surface binding and emits terminal discovery observations at that log position. It also emits null `SubregistryChanged` and `ResolverChanged` boundaries for any attached roles so full and incremental projections retire the old topology. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L199 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L206 @ ens_v2@a971bd64) A replacement registration or reservation can bump the token version and overwrite the stored subregistry and resolver, while upstream emits follow-up target updates only for nonzero replacements; the adapter therefore closes the prior discovery targets before accepting the successor lifecycle and emits the same null role boundaries. Replacement registration lets the following `TokenResource` close the old surface at the successor start; replacement reservation has no successor resource, so it closes immediately and emits `SurfaceUnbound` as position-specific reorg-repair evidence. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L455 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L462 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L474 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L478 @ ens_v2@a971bd64)

`RegistryCreated` is admitted as registry-instance history and discovery input. `URIUpdated`, the `PermissionedResolver` `DataChanged` / `NamedDataResource` pair, and ERC-1155 `ApprovalForAll` remain outside the active normalized behavior.[^v2-events-created][^v2-events-uri][^v2-pres-data] Operator approval is not treated as token ownership or an ENSv2 resource-role grant. (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L336 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L341 @ ens_v2@a971bd64) `PublicResolverV2` is not directly declared by a manifest and is not an admitted resolver profile.[^v2-deploy-public-resolver] Its configured normalized observations may remain stored, but its projection support status stays unsupported unless canonical upgrade history later matches an explicitly declared resolver implementation. Current record visibility remains limited to the current resolver emitter.[^v2-public-resolver-discovery][^v2-public-resolver-version]

#### ENSv2 migration-family admission plan

The ENSv1→ENSv2 migration family is a fixed-address, log-driven extension of the current
Sepolia ENSv2 deployment. Its first active manifest version uses an empty
capability table: the family does not own a product-facing namespace-metadata
capability, and future manifest presence must not by itself enable public
mixed-history current-state reads. Exact-name and direct-subname support follow
the separate consumer slices in
[`consumer-capabilities.md`](consumer-capabilities.md#ensv1ensv2-delivery-slices).
The empty capability table is not a serving barrier: current Project staging
and product event/history readers do not consult it. Slice 1 therefore marks
every correlation-dependent effect in the per-name
[migration correlation group](glossary.md#migration-correlation-group) with
`consumer_visibility=candidate`; an [independently admitted
event](glossary.md#independently-admitted-event) retains its
ordinary activated output and receives a separate candidate association. Every
consumer staging or direct-history read excludes correlation-dependent candidate
normalized events and candidate identity/discovery effects until the later
consumer-activation slice activates the group. Slice 2A added no capability,
runtime, or manifest flag and left production correlations candidate. The final
activation slice now activates [complete groups](glossary.md#complete-group) in production without changing
that capability table, runtime configuration, or manifest authority. A [physical Interpret batch](glossary.md#batch-grid) containing a registrar-token `unwrapped` group affected by
[issue #822](https://github.com/ensdomains/bigname/issues/822) rolls back every write from that attempt, including complete sibling groups. During redo, range preparation can already have deleted previously stored candidate rows in an earlier committed physical batch; failure in a later batch therefore leaves partial, fenced redo state rather than restoring the pre-redo rows. Failure in the initial physical redo batch rolls its preparation back with that batch.
`migration_event_associations` remains diagnostics-only before and after
activation.

The family admission includes a reviewed in-place schema-migration because the
schema-v2 normalized-event table has closed event-kind and derivation-kind
constraints. That upgrade admits the candidate `MigrationApplied` boundary,
factory `ContractDiscovered` observation, and `ens_v2_migration` derivation
identifier and changes the baseline and its apply checks; a normal
interpretation re-derivation does not alter an existing constraint. An
empty-schema replacement is excluded from this boundary because current event
identities include sequence-assigned manifest IDs and therefore cannot resume
outstanding cursors after a replacement. This is the schema-migration stop
condition for the implementation phase of this issue. The same reviewed
contract must admit the stable
`migration_correlation_ids` and `consumer_visibility` fields used by
correlation-dependent normalized events, the separate
`migration_event_associations` rows for independently admitted events, the
`migration_discovery_associations` rows attached to independently admitted
registry-announcement edges, and the candidate identity/discovery effect rows;
source family alone is not a valid visibility key.

The following fixed contracts become direct declarations under
`ens_v2_migration_l1`:

| Contract role | Sepolia address / start block | Admission purpose |
| --- | --- | --- |
| Unlocked migration controller | `0xd021a69db7f9e276a59cbbccf06e7f1e5434215c` / `11163401` | Authority marker for unwrapped and unlocked-wrapped `.eth` reservation claims. The controller emits no event of its own; the admitted registry and ENSv1 contracts emit the transaction's facts. (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/UnlockedMigrationController.json:L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/UnlockedMigrationController.json:L631 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L119 @ ens_v2@a971bd64) |
| Locked migration controller | `0x681802eff57b83edce99d688c023ab1284495176` / `11163413` | Authority marker for locked `.eth` reservation claims and [migration-registry](glossary.md#migration-registry-wrapperregistry) creation. (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/LockedMigrationController.json:L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/LockedMigrationController.json:L751 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L89 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L110 @ ens_v2@a971bd64) |
| Graveyard | `0x6f4bf58ac55e0018589b2d9734ed8bb82740124d` / `11163400` | Terminal-holder and registrar self-claim marker. `clear` can register a fully expired ENSv1 name to the Graveyard with a near-maximum expiry, which interpretation classifies as terminal cleanup rather than a user lease. (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/Graveyard.json:L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/Graveyard.json:L438 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L157 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L169 @ ens_v2@a971bd64) |
| ENSv1 renewal bridge (`ETHRenewerV1`) | `0x1be516ae1b72765ae55bd5e9ca628c9058a1c622` / `11163404` | Direct `NameRenewed` emitter and the marker for synchronized ENSv1/ENSv2 renewal. Its `syncWrapper` temporarily adds NameWrapper as an ENSv1 registrar controller and removes it in the same call, so controller membership is dynamic. (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRenewerV1.json:L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRenewerV1.json:L902 @ ens_v2@a971bd64) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L106 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L111 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L132 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L148 @ ens_v2_sepolia_20260629@ccaeb58) |
| Verifiable factory | `0x118bc31a50d559f7015a8da26d54b3b030cdb70f` / `11163324` | Direct `ProxyDeployed` history for ENSv1→ENSv2 migration-created registry proxies. This event is audit evidence, not registry admission. (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/VerifiableFactory.json:L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/VerifiableFactory.json:L48 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/VerifiableFactory.json:L194 @ ens_v2@a971bd64) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L146 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L161 @ ens_v2_sepolia_20260629@ccaeb58) |
| DAO batch-reservation registrar (`BatchRegistrar`) | `0xfe2aab6df1cbff84534ce65d9e4a755ba02d6795` / `11163411` | Sender marker for pre-migration reservations and reservation-expiry extensions. The authoritative `LabelReserved` / `ExpiryUpdated` logs still come from the existing `ETHRegistry` declaration. (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/BatchRegistrar.json:L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/BatchRegistrar.json:L279 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registrar/BatchRegistrar.sol:L43 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registrar/BatchRegistrar.sol:L69 @ ens_v2@a971bd64) |

The existing Sepolia ENSv1 BaseRegistrar at
`0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85` is named by `correlation_addresses.ens_v1_base_registrar`; it is not a migration-family contract declaration or watch-plan input.
`ens_v1_registrar_l1` admits that address from its historical start block, while the ENSv1→ENSv2 migration correlator accepts its observations only from the Graveyard deployment block `11163400` onward. This preserves the former launch-bounded scope: registrar rows that exist only because of migration correlation retain `ens_v2_migration_l1` normalized-row provenance and remain `consumer_visibility=candidate` unless every group they reference is complete; attribution of the raw log by `ens_v1_registrar_l1` alone does not make them ordinary consumer-visible registrar history.
The address-keyed contract instance remains the same identity across the two uses. (upstream: .refs/ens_v1/deployments/sepolia/BaseRegistrarImplementation.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/Graveyard.json:L438 @ ens_v2@a971bd64)
Correlation is per name, never per transaction alone. Interpretation hashes the decoded bridge label bytes exactly as emitted; it does not normalize or rewrite them. That labelhash, interpreted as `uint256`, must equal the BaseRegistrar token ID. Interpretation then derives the `.eth` namehash from `ETH_NODE` and that labelhash and requires it to equal the v2 logical name/namehash. (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L134 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2/contracts/src/utils/LibLabel.sol:L7 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/utils/LibLabel.sol:L8 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L108 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L113 @ ens_v2@a971bd64) A direct child under an already-migrated parent uses the same check without `ETH_NODE`: interpretation derives the child namehash from the parent [migration registry](glossary.md#migration-registry-wrapperregistry)'s own migration evidence — the CREATE2 salt of the factory log that created that registry, which is the parent's namehash — together with the registered labelhash, and requires the result to equal the ENSv2 logical name/namehash the registry topology resolves for that label. A mismatch means the evidence chain is incomplete and no boundary is derived. (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L151 @ ens_v2_sepolia_20260629@ccaeb58) The participating logs must have a valid path-specific order and come from the declared emitters and controller path; decoded expiry or duration values must agree for that path without reconstructing an expiry. A transaction-hash-only join is forbidden because one transaction can contain several labels through `syncWrapper` or a multi-item wrapper transfer. (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L106 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L132 @ ens_v2@a971bd64) Each name produces an independent correlation group, and unrelated co-located logs remain outside every group.

Each group carries `correlation_kind`. A synchronized bridge renewal uses
`synchronized_renewal` and never emits `MigrationApplied`; a later renewal or
authority transition receives its own stable group ID. Only
`correlation_kind=authority_transition` with the complete ENSv1→ENSv2 migration
shape may emit `MigrationApplied`. Its completion position is the successful v2
registry `LabelRegistered` log emitted during `_register`, in the same
transaction as the v1-side token release. The registry can also emit
`TokenResource`, `SubregistryUpdated`, and `ResolverUpdated`; the migration
controllers emit no separate completion event, and the wrapped finish function
is callable only by the receiver itself.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464-L479 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L164-L174 @ ens_v2@a971bd64)
The claimed token retains the unrevokable `ROLE_WAS_RESERVED` marker.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L450 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L47-L48 @ ens_v2@a971bd64)

The activated form is an exact-name operation. It carries the successful
registration's block number, transaction index, and log index, never timestamp
or transaction membership alone; an `ens_v1` predecessor selector; and the
concrete `ens_v2` binding and resource. A manifest supplies admission evidence
only and cannot activate that transition. One transaction may contain several
wrapped names or mix unwrapped, unlocked-wrapped, and locked-wrapped groups, so
correlation and transition identity remain per logical name.
(upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L132-L154 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/MigrationHelper.sol:L89-L135 @ ens_v2_sepolia_20260629@ccaeb58)
The three controller-mediated second-level shapes keep distinct predecessor
selectors. `unwrapped` and `unlocked_wrapped` record the exact BaseRegistrar
transfer to the Graveyard and select the registrar resource immediately before
that cleanup; `locked_wrapped` selects the NameWrapper resource immediately
before the ENSv2 registration boundary. The unlocked wrapped controller unwraps
before it injects the ENSv2 registration, while the locked receiver instead
parks the wrapper token before injecting.
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146-L148 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L144-L168 @ ens_v2_sepolia_20260629@ccaeb58)
When the deployment profile has no prior registrar identity, the ordered
`NameUnwrapped` and exact following BaseRegistrar transfer confirm the fallback
identity with its binding effective from the unwrap, preserving the
cleanup-relative selector.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
The registrant is not a predecessor selector: an approved operator can drive
the transfer, while the v2 owner is caller-supplied payload data and may differ
from the v1 registrant.
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/MigrationHelper.sol:L102-L114 @ ens_v2_sepolia_20260629@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L20-L31 @ ens_v2@a971bd64)
Slice 2A's controller-mediated shapes cover only `.eth` second-level names: the
unlocked path verifies a label token and derives its name under `ETH_NODE`, and
the locked controller returns `ETH_NODE` as its wrapped root. No transition
through a migration controller is admitted for a name that is not an `.eth`
second-level name.
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L108-L113 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L80-L83 @ ens_v2@a971bd64)
Slice 3A admits the one further shape, which reaches no controller at all: a
direct child registered by its already-migrated parent's own
[migration registry](glossary.md#migration-registry-wrapperregistry), under the
separately evidenced child rule below. That rule does not inherit the `.eth`
second-level rule and never uses `ETH_NODE`; its predecessor is the child's
ENSv1 NameWrapper position, selected immediately before the child's own ENSv1
cleanup rather than immediately before the ENSv2 registration, because the
emancipated shape's unwrap ends that position earlier in the same transaction.

A name-independent controller change outside a per-name synchronized-renewal
group uses `correlation_kind=controller_configuration`. Its stable derivation
group ID uses the BaseRegistrar emitter, controller account, event kind, anchor
position, and complete evidence set; it does not invent a logical name or use
the transaction hash as identity.

A BaseRegistrar `NameRegistered` whose owner is the declared Graveyard and
whose [emitted expiry](glossary.md#emitted-expiry) is exactly `uint64` maximum
minus the ENSv1 BaseRegistrar grace period is
[Graveyard cleanup](glossary.md#graveyard-cleanup): historical evidence, never
a registration, lease, current-authority, wrapped-state, resource, token-lineage,
or surface-binding fact. The Graveyard produces that shape when `clear`
self-claims a fully expired name.
(upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L157-L169 @ ens_v2@a971bd64)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L142-L154 @ ens_v1@91c966f)
Its group uses `correlation_kind=graveyard_cleanup`. A Graveyard-held event with
any other expiry does not satisfy this classification. `MigrationApplied`
requires the complete per-name controller path and successful v2 registration
in addition to the Graveyard evidence. Negative fixtures cover Graveyard cleanup
without an ENSv1→ENSv2 authority transition, unrelated Graveyard-owned registrar
events, bridge-less BaseRegistrar renewals, and unrelated events co-located in a
batch transaction.

A version-zero ownerless [premigration reservation](glossary.md#premigration-reservation)
materializes stable backing-resource and token-lineage identities for the
registry entry, but no token mint, registration, current authority, or surface
binding. `ResolverUpdated` and `ExpiryUpdated` before a claim remain normalized
against that same resource. The claim keeps the identities and uses the expiry
emitted by `LabelRegistered`; the adapter does not reconstruct a grace-period or
bridge-offset formula. The later `TokenResource` emission confirms the
registered ENSv2 EAC resource before any surface binding is created. A mismatch
between that emitted resource and the reservation's retained resource is an
interpretation error rather than permission to manufacture a second authority
object. A reservation whose emitted token has nonzero version bits remains
reservation evidence without a derived resource: upstream tracks token and EAC
resource versions independently, so the token alone cannot identify the
resource in that case. The upstream claim path copies the stored reservation
expiry when its expiry input is zero and then emits the copied value.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L25-L34 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L428-L471 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L474-L478 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L632-L650 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/utils/LibLabel.sol:L11-L17 @ ens_v2@a971bd64)
An `ExpiryUpdated` for that non-derived entry can still join its BaseRegistrar
and bridge facts as a resource-less `synchronized_renewal`; correlation uses the
logical name, registry token ID, emitted expiry, exact emitters, and log order
rather than inventing a resource anchor.
The admitted old-model registrar renews and emits the same registry token ID.
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/AbstractETHRegistrar.sol:L87-L93 @ ens_v2_sepolia_20260629@ccaeb58)

BaseRegistrar `NameRenewed` observations that participate in a bridge or
NameWrapper synchronization use `correlation_kind=synchronized_renewal`.
Another launch-bounded BaseRegistrar renewal is activated historical evidence
once its non-boundary correlation group is complete, under
`after_state.lifecycle_classification=historical_renewal`; it
does not materialize an ENSv1 resource, token lineage, authority transition, or
surface binding. This
retains post-boundary ENSv1 residue without allowing it to overwrite ENSv2
authority. Controller additions and removals remain name-independent permission
history. Slice 1 kept every BaseRegistrar row materialized through the
ENSv1→ENSv2 migration family's launch-bounded correlation candidate; the final
activation slice now activates completed non-boundary groups while refused or
incomplete groups remain candidate. `ens_v1_registrar_l1` remains the sole
source family that owns those raw logs. The ENSv1→ENSv2 migration family
declares no contract at that address, so the attribution guard and runtime
adapter selection remain unambiguous.
Without an `ens_v2_migration_l1` manifest, these four mappings extend raw-log ownership but produce no ordinary registrar rows.
The deployment profile admits the ENSv1 registry, registrar, NameWrapper, and resolver families (`ens_v1_registry_l1`, `ens_v1_registrar_l1`, `ens_v1_wrapper_l1`, and `ens_v1_resolver_l1`). The ENSv1→ENSv2 migration manifest's `correlation_addresses.ens_v1_name_wrapper` and `correlation_addresses.ens_v1_base_registrar` values are non-emitting correlation metadata: within the ENSv1→ENSv2 migration family they are not contract declarations, discovery edges, or watch-plan inputs. The contracts they name are separately declared and watched by their ENSv1 families. The ENSv1→ENSv2 migration family reads that cross-family evidence; it does not own or duplicate its raw log attribution. Resolver-event intake remains outside migration correlation and belongs to `ens_v1_resolver_l1`.
(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRenewerV1.json:L894 @ ens_v2@a971bd64)
A controller
event around a multi-label
`syncWrapper` call is stored once with the sorted `migration_correlation_ids` of
every participating name; it is not duplicated per name and remains candidate
until all referenced groups activate. A controller event outside such a batch
uses the name-independent `controller_configuration` derivation group above.
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L106-L111 @ ens_v2_sepolia_20260629@ccaeb58) Wrapper-expiry correlation requires the complete controller-addition, renewal, and matching controller-removal envelope in one transaction. An incomplete envelope remains historical evidence, and candidate correlation never advances the independently admitted NameWrapper state; an unmatched addition therefore cannot affect later ordinary output whether the blocks are interpreted in one batch or several.
The proven wrapper expiry is retained separately and may refine only the registrar expiry when a later ordered `NameUnwrapped` then BaseRegistrar `Transfer` first materializes a missing registrar identity; full replay, incremental replay, and cold restore make the same choice. When more than one completed correlation group exists for a name, bigname retains the monotone maximum correlated wrapper expiry. This remains correct across full lapse and re-registration: BaseRegistrar makes a lease available only after its stored expiry plus grace is earlier than `block.timestamp`, and a successful re-registration writes `block.timestamp + duration` with a strictly positive duration, so a legitimate successor lease expiry — and the corresponding wrapper expiry — is strictly greater than its predecessor. `syncWrapper` performs a zero-duration renewal, which reads that current registrar expiry without reducing it. A lower later-correlated value therefore cannot be the successor lease expiry that should govern this unwrap fallback. (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L103 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L168 @ ens_v1@91c966f) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L104-L111 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L318-L337 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L8 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L9 @ ens_v1@91c966f) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L106 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L107 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L111 @ ens_v2_sepolia_20260629@ccaeb58) The correlated bridge, ENSv1 registrar, and ENSv2 registry renewal observations remain separate normalized rows; no transaction-level synthetic renewal is created. A resource-bearing registry observation retains its resource, and the bridge observation uses that already-materialized ENSv2 `resource_id`. When the reserved registry resource cannot be derived, both observations remain resource-less rather than inventing an anchor. A launch-bounded BaseRegistrar row carries a deterministic candidate registrar-resource selector in `after_state.resource_anchor`; it materializes no ordinary ENSv1 resource or token lineage unless its complete correlation group activates, and incomplete or refused groups leave it candidate. This scoped declaration supplies ENSv1→ENSv2 correlation; it does not transfer ordinary ENSv1 registrar authority to the ENSv2 migration family.

The production migration driver revokes the superseded public ENSv1
registration controllers and enables the Graveyard and `ETHRenewerV1` handoff
controllers. On a network whose deployment set includes
`TestnetV1PremigrationRegistrar`, the same driver also enables that
premigration registrar — which registers names permissionlessly — as a v1
controller and leaves it enabled after the handoff, so a testnet launch does
not reduce the controller set to the two handoff controllers.
(upstream: .refs/ens_v2/contracts/script/migration.ts:L1594-L1607 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/script/migration.ts:L2391-L2405 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/script/migration.ts:L5008-L5025 @ ens_v2@a971bd64)
Only the fork/test-helper topology is deployer-only: fork-mode devnet
finalisation runs `activateV2`, which retires the v1 ETH controllers, grants
the controller role to the Graveyard and `ETHRenewerV1`, and adds the deployer
as a controller solely so the test mnemonic can drive v1 registration calls.
(upstream: .refs/ens_v2/contracts/script/runDevnet.ts:L145-L155 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/script/setup.ts:L911-L955 @ ens_v2@a971bd64)
Production fixtures therefore reject a controller sequence copied from that
fork/test-helper topology as evidence of a fresh ENSv1 registration stream.

`MigrationHelper` at `0xd54a53c1567b26f9653c8565dccc39bceb6ab327`,
starting at block `11163415`,
is declared as fixed deployment metadata. It emits no event and only orders
transfers through the two controllers and already-migrated parent registries.
Its order is unwrapped, unlocked-wrapped, locked-wrapped, then locked children.
The helper is therefore unobservable: using it is optional, and the transfers it
batches produce the same log sequence a caller would produce by sending the same
transfers itself, so correlation never keys on the helper's participation.
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/MigrationHelper.sol:L108-L113 @ ens_v2_sepolia_20260629@ccaeb58)
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/MigrationHelper.sol:L124 @ ens_v2_sepolia_20260629@ccaeb58)
The current family-wide watch planner assigns the
ENSv1→ENSv2 migration manifest's complete topic set to this declared address. (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/MigrationHelper.json:L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/MigrationHelper.json:L542 @ ens_v2@a971bd64) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/MigrationHelper.sol:L103 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/MigrationHelper.sol:L133 @ ens_v2_sepolia_20260629@ccaeb58)

ENSv1→ENSv2 migration-created `WrapperRegistry` proxies are not direct declarations and
do not use a new discovery rule. Each proxy's initializer emits
`RegistryCreated()` before `ParentUpdated` and role events. The existing
`ens_v2_registry_l1` match-all `registry_announcement` rule admits that
emitting address at the exact log position; subsequent logs in the same
transaction are then interpreted under the registry family. Rule ownership,
intake, and consumer visibility are separate axes. Rule ownership remains with
`registry_announcement`. Its independently admitted normalized
`RegistryCreated` event and indexability edge remain ordinary and unchanged; the
watch plan traverses the edge from the announcement position, and the
`migration_registry_creation` association attaches separately to the
event and edge. That association does not make the edge candidate. The edge
records only indexability: it creates no suffix, parent relation, name binding,
or current authority. Correlation-dependent identity, parent, role,
registration, renewal, topology, and normalized-event effects from the registry
remain candidate while their group is incomplete or refused and activate only
after every group they reference is complete, including effects in later
transactions or blocks. The association alone cannot
reclassify an effect: a `ParentUpdated`, role, registration, renewal, topology,
or normalized-event output that `ens_v2_registry_l1` derives from the ordinary
edge and raw event without ENSv1→ENSv2 migration correlation remains ordinary and
byte-for-byte unchanged. Only the additional meaning that depends on the
correlation follows candidate-to-activated complete-group visibility. Beyond
that rebuild-scope use, Project has two narrow semantic reads: after
an activated parent boundary, the exact-name authority selector may require the
readable ordinary edge and its canonical `migration_registry_creation`
association to classify a positive child-registration emitter, while child
reachability may require them to prove the current parent subregistry is the
migration-created `WrapperRegistry`. Neither
row is authority proof by itself, and no product route consumes either row
directly. A later
`SubregistryUpdated` remains the bidirectional parent-child topology edge and
does not itself admit the target. (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L131 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L133 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L134 @ ens_v2@a971bd64) The implementation at
`0xcf9f4863a1b44216cfc0be65f4e47b2b9a043924`, starting at block `11163410`,
is implementation metadata, not a root or a registry admission. It remains a declared
address in the family-wide watch plan. (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/WrapperRegistryImpl.json:L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/WrapperRegistryImpl.json:L3700 @ ens_v2@a971bd64)

Consumer slice 3A admits the same shape at any depth. The registry created for a
locked child is deployed by its parent's registry, not by the locked migration
controller, because `WrapperRegistry` inherits the same wrapper receiver, and the
CREATE2 salt is the child's namehash.
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L32 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L149 @ ens_v2_sepolia_20260629@ccaeb58)
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L151 @ ens_v2_sepolia_20260629@ccaeb58)
Such a registry is admitted exactly as a controller-deployed one is — from the
registry's own `RegistryCreated` announcement plus the factory log naming that
registry — so admitted depth is unbounded: second level, third level, fourth
level, and below. Rule ownership still stays with `registry_announcement`, and
the ordinary `RegistryCreated` event, indexability edge, and every
independently derivable existing-family output above remain byte-for-byte
unchanged.

A direct child never reaches a migration controller: the already-migrated
parent's registry is itself the receiver and registers the child into itself.
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/MigrationHelper.sol:L124 @ ens_v2_sepolia_20260629@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L32 @ ens_v2@a971bd64)
The observable discriminator is that the child's `LabelRegistered` is emitted by
the parent registry and its `sender` field equals that same emitting registry
address, because the receiver re-enters through an external self-call restricted
to itself; a second-level migration instead names a separate migration
controller as `sender`.
(upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L149 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L167 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L467 @ ens_v2@a971bd64)
A child boundary additionally requires the child's own ENSv1 predecessor cleanup
in the registration's transaction, which is what shows ENSv1 authority ended
rather than merely that an ENSv2 registration happened. `locked_child` parks the
child's wrapper token in the Graveyard without unwrapping it
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2_sepolia_20260629@ccaeb58);
`emancipated_child` unwraps the child's node into the Graveyard
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L180 @ ens_v2@a971bd64).
Both are emitted by the ENSv1 NameWrapper the ENSv1→ENSv2 migration manifest declares as a
correlation address, so the requirement is manifest-anchored. A self-claim
carrying neither derives no boundary.

Correlation is per child registration, never per transaction membership: one
transaction may carry a parent migration and several children, each with its own
correlation ID and evidence chain. The parent registry's creation must precede
the child's registration in full block, transaction-index, and log-index order,
including within one transaction. `correlation_kind` is unchanged — a child
group is an ordinary `authority_transition` group — and every child boundary and
dependent effect is candidate until the complete-group activation function admits it.

Five shapes derive no child boundary, each as explicit non-support rather than a
fallback, and a sixth never arises at all:

- An unmigrated [migratable child](glossary.md#migratable-child) under a
  migrated parent. It produces no ENSv2 registration event at all: the child
  stays ENSv1-authoritative and the parent registry answers `getResolver` with
  the ENSv1 fallback resolver, which is view-only state and emits no log.
  (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L186 @ ens_v2@a971bd64)
- A parent-controlled child that the parent's owner registers directly. The
  registry permits that registration because the label is not protected as
  migratable, so it is a real ordinary registry fact and, under consumer slice
  2C, an authority proof — but it is never a child `MigrationApplied`.
  (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L172 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L175 @ ens_v2@a971bd64)
- A `ProxyDeployed` factory log without the `RegistryCreated` announcement of
  the registry it names. That remains audit evidence and is not registry
  admission.
- A registration emitted by a registry that carries no
  `migration_registry_creation` correlation. Parent discovery is then
  incomplete and the emitter is an ordinary registry.
- A self-claim with no ENSv1 predecessor cleanup for the child in the same
  transaction. Nothing shows an ENSv1 authority ended, so nothing was migrated.

`MigrationHelper` participation is the shape that never arises rather than one
that is refused. The helper only forwards transfers and declares no event, so a
batch it sends and the same transfers sent directly produce the same logs
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/MigrationHelper.sol:L108-L113 @ ens_v2_sepolia_20260629@ccaeb58),
and there is nothing for correlation to key on.

Diagnostic correlation output does not mean zero effect on Project. Admitting a child
registry writes a `migration_registry_creation` discovery association, and
Project's rebuild scope and parent-reachability selector read that table without a `consumer_visibility` filter,
so names registered into a newly-admitted child registry enter delete-and-rebuild
candidacy. What those rebuilds publish is unchanged: the child-registration
authority proof also requires an activated parent boundary. Complete parent and
child groups now receive that visibility through the shared production
activation function after correlation is complete.

Source-family ownership does not break the visibility barrier. The ordinary
`RegistryCreated` event and `registry_announcement` indexability edge retain
their existing-family admission, while the attached diagnostic associations
carry the
[`migration_registry_creation`](glossary.md#migration-correlation-group)
correlation kind. Every correlation-dependent downstream effect keeps that
group's `migration_correlation_ids` and `consumer_visibility` even though it
interprets under `ens_v2_registry_l1`. Candidate effects do not update ordinary
identity, topology, or consumer state; the ordinary indexability edge is the
explicit slice-1 exception because the watch plan consumes it. The authority-selector
and parent-reachability reads above do not expose the edge or association as a
product row.

Independent admission takes precedence. An existing-family normalized event
that the active manifest and discovery rules already produce without the
correlation remains byte-for-byte activated and product-visible. Slice 1 records
its candidate relationship in a separate `migration_event_associations` row; it
does not duplicate, suppress, or reclassify the ordinary event. Project staging
and product event/history readers exclude refused or incomplete candidate rows
and never consume `migration_event_associations` or the diagnostic identity and
discovery effect tables. Unrelated existing-family facts in the same transaction
remain normally eligible. Slice 2A established arm-scoped ordinary binding
behavior and the explicit transition write. The final production path activates
complete correlation-dependent normalized rows through that same implementation;
candidate-effect tables retain their candidate-only diagnostic contract, and
event associations may become activated diagnostics but never become consumer input.

Slice 1 requires a restart boundary fixture, not only a same-transaction ordering
test. At block N, a migration-created proxy emits `RegistryCreated`; after an
Ingest and Interpret restart, a later transaction or block emits at least one
registry, role, registration, renewal, or topology event from that proxy. Both a
full historical replay lane and a live-follow lane must prove that the ordinary
announcement admission keeps the proxy watched, the later raw fact is retained,
its correlation-dependent augmentation has the completed group's visibility, and any output the
existing registry family derives independently remains ordinary and matches the
control test run. After restart, the generated watch plan must contain the
proxy through its persisted ordinary edge before either the retained-raw-log
announcement preload or the same-window announcement query adds it; otherwise
those intake paths could mask a broken edge path.
Every product row and DTO remains unchanged. A same-transaction initializer
fixture cannot satisfy this gate because it does not prove the proxy remains
watched after restart.

The current watch planner uses the ENSv1→ENSv2 migration manifest's complete
ABI topic set for every address declared by that manifest; its
`emitter_roles` constrain interpretation selection, not ingest planning. The
address-scoped [intake-only approval events](glossary.md#intake-only-event) in
other source families are the explicit exception: their declared roles also
constrain ingest planning. The active ENSv1→ENSv2 migration family
therefore widens the watch plan across all eight declared addresses,
including marker-only contracts, with each address bounded by its own pinned
start block. The
manifest content-hash rotation invalidates interpretation and projection
output. Deployment must inspect the actual generated watch plan, fetch complete
history for every widened address/topic range, then run Interpret and
[Project phase](glossary.md#projection) redo at the planned [re-derivation
boundary](glossary.md#re-derivation-boundary). Manifest presence and completed
backfill do not capability-promote mixed-history reads. In the test environment,
the slice-1 acceptance publication has no consumer-visible semantic delta
from that re-walk; the comparison in the consumer contract is a release gate,
not an optional fixture check.

The final production activation changes interpretation output without changing
source authority: fixed contracts, `manifests/mainnet/`, `manifests/sepolia/`,
and the generated watch plans remain byte-for-byte unchanged. The new
[interpreter content hash](glossary.md#interpreter-content-hash) therefore
requires one complete retained-range Interpret re-walk followed by Project,
with publication blocked until the completed generation is coherent. The
dual-current integrity assertions apply to activated proofs on the configured
Mainnet ENS deployment profile. Sepolia publishes a proof-selected result, and
ordinary unproven Sepolia ENSv1/ENSv2 overlap remains a per-name refusal rather
than a publication block. Extending the guardrail to Sepolia is deferred until
[PR #852](https://github.com/ensdomains/bigname/pull/852), the #503 e2e harness,
proves the connected Interpret→Project path;
[issue #851](https://github.com/ensdomains/bigname/issues/851) tracks re-applying
the guardrail. There is no production interval serving candidate-only data.
The ordinary announcement edge above remains a watch-plan input and this
activation creates no ingest gap.

Other artifacts of the admitted 2026-06-29 Sepolia deployment — including
universal/reverse resolution,
other wrapper surfaces, oracle, resolver-set administration, and mock-payment
surfaces — remain outside admission.

### Basenames mainnet

Basenames mainnet admits six families:[^bn-readme-l22][^bn-readme-l28][^bn-readme-l29][^bn-readme-l30][^bn-readme-l33][^bn-readme-l34][^bn-readme-l36][^bn-readme-l37][^bn-readme-l69][^bn-readme-l70]

- `basenames_base_registry` — `registry` at `0xb94704422c2a1e396835a571837aa5ae53285a95` (Base). Per-node owner/resolver/ttl state.[^bn-registry-l10][^bn-registry-l100][^bn-registry-l113][^bn-registry-l132]
- `basenames_base_registrar` — `registrar` at `0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a` (Base), plus `legacy_registrar_controller` at `0x4cCb0BB02FCABA27e82a56646E81d8c5bC4119a5` and `upgradeable_registrar_controller` proxy at `0xa7d2607c6BD39Ae9521e514026CBB078405Ab322`. Tokenized authority stays with BaseRegistrar; controller contracts are admitted in the same source family for label-bearing registration and renewal observations only.[^bn-baseregistrar-l15][^bn-baseregistrar-l17][^bn-baseregistrar-l237][^bn-baseregistrar-l327][^bn-registrar-controller-l180][^bn-registrar-controller-l187][^bn-upgradeable-registrar-controller-l191][^bn-upgradeable-registrar-controller-l198]
- `basenames_base_resolver` — `resolver` at `0xC6d566A56A1aFf6508b41f6c90ff131615583BCD` (Base). Default `L2Resolver` profile seed.[^bn-l2resolver-l22][^bn-l2resolver-l49][^bn-l2resolver-l52][^bn-l2resolver-l193]
- `basenames_base_primary` — ENSv1 `L2ReverseRegistrar` at `0x0000000000D8e504002cC26E3Ec46D81971C1664` (Base). Declared primary-name value intake only, keyed by `NameForAddrChanged(address,string)` and scoped to Base coin type `2147492101`; the adapter emits both the reverse claim anchor and the accompanying `RecordChanged(name)` claim-name observation from that raw fact. This source family does not admit the Basenames `ReverseRegistrar` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282` as the primary-name value authority; Basenames exact-name, address-name, and children truth still comes from the Base registry/registrar/resolver families.[^v1-l2rev-base-deploy][^v1-l2rev-base-args][^v1-l2rev-event][^v1-l2rev-nameforaddr][^bn-readme-l33][^bn-revreg-l12][^bn-revreg-l150]
- `basenames_l1_compat` — `l1_resolver` at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` (Ethereum). L1 compatibility transport for the `base.eth` domain.[^bn-l1resolver-l13]
- `basenames_execution` — `l1_resolver` at the same Ethereum address with `verified_resolution = "supported"` for the exact-surface transport-assisted direct-path class only. Execution entrypoint that initiates `OffchainLookup` and completes through `resolveWithProof`.[^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]

The L1 Resolver address appears in both `basenames_l1_compat` and `basenames_execution`. Transport ownership stays with `basenames_l1_compat`; execution entrypoint and verified-resolution routing stay with `basenames_execution`. Manifest declarations retain their authored checksummed address spelling; the projected topology serializes typed EVM addresses in lowercase.

`basenames_execution` v2 capability-promotes only the [path class](glossary.md) where `resolver_path[0].logical_name_id` equals the route surface, `wildcard.source = null`, `alias.final_target = null`, `subregistry_path = []`, `transport.source_chain_id = "base-mainnet"`, `transport.target_chain_id = "ethereum-mainnet"`, and `transport.contract_address = "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31"`. Alias-participating, wildcard-derived, linked-subregistry, transport-free, and offchain-gateway classes return selector-local `unsupported`.[^bn-readme-l71]

`basenames_execution` does not admit verified primary-name lookup. The current
verified primary-name product path is limited to ENS coin type `60`.

Basenames registry `NewResolver` updates a node binding but does not discover a contract. Base-side resolver-local events use the `basenames_base_resolver` signature set across all emitting addresses. Resolver-local supported behavior still requires `L2Resolver`-compatible profile admission for the emitted family. This match-all rule does not admit the L1 Resolver or offchain gateways.[^bn-registry-l19][^bn-registry-l223][^bn-l2resolver-l4][^bn-l2resolver-l16][^bn-l2resolver-l29][^bn-l2resolver-l182][^bn-l2resolver-l209][^bn-l2resolver-l225]

`basenames_offchain` is reserved for later gateway admission. It is not part of the current split.

## Contract instance admission and continuity

Manifest loading admits source-graph nodes as `contract_instance_id`s, not raw addresses. Each active `[[roots]]` and `[[contracts]]` entry resolves to one admitted instance.

[Address-admission floors](glossary.md#discovery-rule-widening-and-narrowing)
are monotone: manifest synchronization keeps the earliest declared
`start_block` for an address. A later declaration with a higher start has no
effect on the stored floor. Keeping the earlier floor can only over-fetch, which
is safe. Raising a stored floor could silently stop fetching previously
included history, so in-place floor narrowing is unsupported. An operator who
needs a narrower floor must retire the address, synchronize that close, and
then re-declare it at the later start: this explicit close-and-reopen path
creates the new bounded active range.

- `[[roots]]` seed canonical graph and watch-plan expansion; otherwise they follow the same identity rules as `[[contracts]]`.
- Reusing the same address on the same chain across manifest versions, even across an inactive gap, carries forward the existing `contract_instance_id` and appends a new non-overlapping active range.
- Changing a declared address closes the prior active range and admits a new instance, with a new `contract_instance_id` rather than ID reuse. No discovery edge records the succession: the loader writes only `proxy_implementation` edges, and the [`migration` edge kind](glossary.md#migration-edge-migration) the schema permits is [reserved surface](glossary.md#reserved-surface) with no writer. What ties the two instances together is the manifest declaration, not their addresses: successive manifest versions carry the same `(chain_id, declaration_kind, declaration_name, role)` tuple against different declared addresses. Do not try to recover succession from the instances' active ranges — a retired address is closed at the chain head observed when the manifest loaded, while its successor opens at its own declared `start_block`, so the two ranges frequently overlap instead of abutting.
- `proxy_kind = "none"` resolves the declared address directly; `implementation` is omitted.
- `proxy_kind != "none"` requires `implementation`. The proxy and implementation are separate instances linked by a time-ranged proxy/implementation edge.
- Changing only `implementation` keeps the proxy's identity. The implementation instance is reused if its address reappears, otherwise a new one is minted.

Contract addresses persist as time-ranged attributes for raw-fact matching and watch-plan expansion.

### Admission selection for addresses with multiple declared roles

One address may be declared under more than one role in the same manifest. The common case is an
address declared both as a `[[roots]]` entry and under its contract role. For each raw log,
interpretation considers the full set of active declarations at that address; database row order
does not choose the declaration.

When the event produces a discovery edge governed by a role-scoped `discovery_rules` entry, a
candidate carrying that rule's `from_role` outranks candidates with other roles. Selection
otherwise preserves each declaration's role. It clears the selected role only for a checked-in
`(source_family, event)` entry below whose adapter does not consume it. The checked-in pairs are:

- `ens_v1_resolver_l1`: `ABIChanged`, `AddrChanged`, `AddressChanged`, `ContentChanged`,
  `ContenthashChanged`, `DNSRecordChanged`, `DNSRecordDeleted`, `DNSZonehashChanged`, `DataChanged`,
  `InterfaceChanged`, `NameChanged`, `TextChanged`, and `VersionChanged`;
- `basenames_base_resolver`: `AddrChanged`, `AddressChanged`, `NameChanged`, `TextChanged`, and
  `VersionChanged`;
- `ens_v2_resolver_l1`: `AddressChanged`, `AliasChanged`, `ContenthashChanged`, `EACRolesChanged`,
  `NameChanged`, `NamedAddrResource`, `NamedResource`, `NamedTextResource`, `TextChanged`, `Upgraded`,
  and `VersionChanged`.

The canonical typed table is `bigname_manifests::ROLE_INSENSITIVE_EVENTS`; every entry carries a
justification and the adapter file it describes. A manifest that omits `emitter_roles` for any
other event is rejected unless the `ens_v2_registry_l1` `RegistryCreated` exception described
above applies.

Role-scoped events use `[[abi.events]].emitter_roles` to constrain eligible declarations before
selection, so the discovery-rule tie-break does not change their role. Candidates made equivalent
because the pair appears in the list above may collapse to one selection. If distinct-role
candidates remain and the applicable discovery rule cannot choose between them, interpretation
stops with the deterministic `ambiguous admitted adapters` error; it never picks an arbitrary row.

## Discovery admission

A contract is an indexable event source when one of these holds:

- it is declared directly in an active manifest
- it is reachable from an active manifest root through an allowed indexability-producing `discovery_rules` edge; an ENSv2 `subregistry` edge is topology-only and does not satisfy this rule
- it is explicitly allow-listed by a manifest version for a migration epoch
- its creation is announced by an admitted event selected across every emitting address

Interpret attempts ABI decoding for every event selected from this admission
catalog. A malformed event log from an address declared in an active
`[[contracts]]` or `[[roots]]` block is a fatal interpretation error regardless
of event selection scope. A malformed event log from an undeclared address is
skipped and recorded as an operator diagnostic whether that address was
admitted through discovery or the event was selected across every emitting
address. All other interpretation error classes keep their fatal posture. This
decision uses only the catalog's declaration record for the emitting address,
never an address or role claimed by the log itself. Missing or extra topics are
malformed ABI input under this rule. ENSv2 `setSubregistry` stores the registry pointer and
emits `SubregistryUpdated`, but that topology update does not admit the named
registry (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L142-L146 @ ens_v2@a971bd64).

Discovery is forward-only from the announcement event. ENSv1 and Basenames registries are manifest-declared singletons; registry owners are leaves and do not create contract instances. A registry `NewOwner` log still records the child-name assignment and its history, including removal through the zero address, but the assigned owner is not admitted as a registry contract. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L82 @ ens_v1@91c966f) ENSv1 and Basenames resolver record history is the exception to address admission: their manifest-declared ENS-specific signature sets are matched across all emitters because those resolver generations have no creation announcement. `NewResolver` changes a name's resolver pointer but creates no discovery edge. `VersionChanged` before-state is tracked independently for each `(emitting resolver, node)` pair because each resolver contract stores its own per-node record-version mapping. (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L7 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L22 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/resolver/ResolverBase.sol:L11 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/ResolverBase.sol:L38 @ basenames@1809bbc)

For ENSv2, `RegistryCreated()` admits the emitting registry with a [`registry_announcement` edge](glossary.md#registry-announcement-edge-registry_announcement) anchored by the active registry manifest. The edge records indexability only. It is not a parent-child edge and does not make the announced registry reachable through any name. Manifest-declared `RootRegistry` and `ETHRegistry` instances seed suffix anchors; an announced registry below them gains a suffix only when its current child-side parent claim and the parent's current unexpired `SubregistryUpdated` pointer agree. Either side breaking retracts that suffix and its name bindings. The additional `ETHRegistry` suffix anchor is recorded in [`upstream.md` § Known divergences](upstream.md#known-divergences). `SubregistryUpdated` remains the only source of registry parent-child relationship truth, and `ResolverUpdated` remains the source of resolver target truth. Defensive `TokenRegenerated` interpretation can reassert retained subregistry topology under the successor `observation_key`, but it never reopens a retained resolver edge. The survivor's existing resolver key remains active and is recorded on the successor token; the next explicit `ResolverUpdated` or terminal token event closes the current key and every recorded prior key not currently shared by another live token with resolver state. If a noncanonical token ID already retired the old key, regeneration therefore cannot invent an address-active interval whose logs were not loaded; if another live token has reused it, that token's edge and watch coverage remain active. Address-scoped interpretation begins at the exact `RegistryCreated()` transaction/log position: direct `PermissionedRegistry` construction emits it first, while a `UserRegistry` proxy emits it during initialization. (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@a971bd64) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registry/UserRegistry.sol:L43 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registry/UserRegistry.sol:L47 @ ens_v2_sepolia_20260629@ccaeb58)

Resolver discovery makes the target available to Interpret, subject to the
[exact-declaration precedence rule](architecture.md#discovery-graph); the edge
and its normalized resolver-pointer effects remain present when a declaration
controls the target's raw-log interpretation.

The active match-all sets widen retained live facts from this change forward. Historical Base resolver events, ENSv2 `RegistryCreated` events, and ERC-1967 `Upgraded` events that predate the widening require the mandatory one-time historical fetch before a derived-state rebuild. That fetch is an ingest operation, not discovery inference.

The initial schema-v2 cutover that introduced these match-all interpretations
used a fresh-schema rebuild. That historical cutover carried raw facts, chain
lineage, and label preimages, but did not carry normalized events, identity
rows, or projections from the transitional schema; it had no supported in-place
replay over those transitional derived rows. This rule describes that initial
cutover, not later versioned schema upgrades. The planned ENSv1→ENSv2 boundary
above instead requires a reviewed in-place schema-migration so outstanding
public cursors can continue across its full re-walk. Its historical fetch still
must complete the widened raw input before interpretation begins.

Each admitted edge stores `from_contract_instance_id`, `to_contract_instance_id`, source manifest version, edge kind, discovery source, active range, and provenance.

Discovery resolves `(chain, address, point in time)` to endpoint `contract_instance_id`s before storing the edge. Re-admitting an address that was previously admitted on the same chain reuses the prior `contract_instance_id` and appends a new range; replaying the exact same observation reuses its historical edge epoch instead of appending a duplicate. A new ID is minted only for addresses never admitted on that chain. Manifest-declared and discovered proxy/implementation links share the same edge and active-range rules.

Schema-v2 interpretation applies discovery opens and closes in block,
transaction, and log order. Every raw-log discovery edge requires non-negative
transaction and log positions and an observation key. Inserting an earlier
observation caps its predecessor at that position; repeating the same target
reuses or backdates the existing epoch instead of creating an overlapping
active epoch. An `interpret` redo first prepares the selected derived range and
then applies the same ordered writer. It does not use omitted observations as a
complete-source deletion signal or consult the deleted stored-lineage coverage
and adapter-checkpoint machinery. Closed intervals remain historical emitter
admission while their manifest authority is active, but do not expand the
current watch plan.

Schema-v2 manifest synchronization snapshots the directly preceding active
address declarations before replacing their stored children. It retires an
active address range that belonged to that snapshot when no desired active
manifest still declares the same contract instance and address. This membership
check repairs a row whose declaration provenance was overwritten by an older
interpreter before retiring it. Interpret may observe a currently declared
address through a discovery event and backdate its active range, but that
refresh preserves the declaration provenance and source-manifest ID; the
event-derived discovery edge records the raw-log observation separately.
Interpret redo preserves a finitely retired manifest-declared address row as
coordination state. Replaying an observation at or before that row's close block
may reproduce its discovery edge, but does not reopen the address range. A
genuinely later discovery observation either appends a bounded address range or
backdates an existing later active range to the greater of the observation
block and the greatest preceding address range's close plus one; it does not
change any retired range. Re-admission therefore remains possible. Deprecating
a manifest version or removing a declaration in place cannot be undone by
replay of the history that preceded retirement. An address admitted only
through discovery keeps its event provenance and remains outside this
retirement rule. Synchronization also
updates manifest-declared proxy edges. It does not run a full-source
reconciliation over event-driven edges. An authority change invalidates the
interpret and project phase content hashes. Complete the [mandatory historical
fetch and attested redo](#mandatory-historical-fetch-after-watch-plan-widening)
before re-deriving affected discovery rows under the new manifest authority.
The Base project phase also consumes Ethereum Mainnet `basenames_execution`
authority for Basenames support and provenance. Changing that family therefore
invalidates the `base-mainnet` project epoch as well, without invalidating its
interpret epoch.

ENSv1 and Basenames owner events do not create discovery contract instances.
Schema-v2 interpretation processes retained `RegistryCreated`,
`SubregistryUpdated`, and `ResolverUpdated` facts through the current
interpreter. The deleted indexer no longer synthesizes
`orphaned_discovery_edge` tombstones; branch replacement is handled through
lineage selection plus an explicit derived-range redo.

### Mandatory historical fetch after watch-plan widening

A manifest change widens the [watch
plan](glossary.md#watch-plan--watched-tuple) when it adds an address, event
signature, or active block range whose facts were not selected when older
blocks were loaded. Manifest synchronization compares the previous and desired
[compiled watch plans](glossary.md#compiled-watch-plan). Each stored manifest
payload carries the compiled
emitter/event/start entries produced when it was admitted, so a later binary
change to emitter-scope policy is compared with the policy that actually
preceded it rather than recompiling both sides with the new binary. This
internal snapshot is not an authorable TOML field. An all-emitter event covers
the same event for every namespace, family, and address. A family-wide event
covers emitters admitted through that family's discovery edges in the same
manifest namespace, but it does not cover a newly declared direct address;
declared addresses remain explicit watch targets.
Adding a less general target already covered by an all-emitter event does not
count as widening.

For each newly widened direct-address watch, synchronization checks the
continuous union of [persisted Ingest
coverage](glossary.md#persisted-ingest-coverage) for the same chain, source
family, address, and event topic. Each usable address epoch contributes the
inclusive intersection of its stored range with the declaration's start.
Synchronization discards an empty intersection and a deactivated epoch with no
finite end, sorts the remaining intervals, and merges overlap and adjacency.
The resulting union must continuously cover the promised start through an
open-ended final interval. A desired all-emitter entry for the same topic keeps
the existing shortcut and covers the direct-address watch without this check.
Whenever any required Ingest redo is pending on the chain, synchronization
deliberately and conservatively refuses to remove a previous all-emitter watch
if doing so would expose a gap in a desired direct-address watch. The redo need
not have been stamped by that watch. Let it complete and retry; if one manifest
change both widens registry-announcement coverage and removes the all-emitter
watch, split the change so its redo completes first.
The refusal preserves the previous compiled watch plan and redo state.

The transaction fails before recording a promise when the union has a leading
or internal gap. It also retains a finite-tail structural guard, although every
desired direct-address declaration contributes an open epoch today, so that
class cannot arise through manifest synchronization. The error names
the promised start and the first uncovered inclusive range; an uncovered tail
is rendered `<N>..=unbounded`. The stored [compiled watch
plan](glossary.md#compiled-watch-plan) keeps its existing
emitter/topic/start JSON shape and remains backward-decodable; the interval
union exists only while synchronization validates the desired plan.

For example, stored address epochs `[5,5]`, `[10,10]`, and `[11,∞)` normalize
to `[5,5]` and `[10,∞)`. A promise from block 5 is refused with uncovered
blocks `6..=9`, while a promise from block 10 is valid. The operator can raise
the declaration to the first continuously covered start, or rebuild from a
fresh database/from-zero Ingest when coverage from the earlier block is
required. On a retained production database, the operator must explicitly
fetch the missing range through a separately planned repair before making the
wider promise. This change does not add that retained-database repair path:
ordinary address-scoped redo follows the persisted address epochs and cannot
fill their gap. Manifest synchronization does not silently stamp an Ingest redo
and claim that the gap was repaired. Direct database edits remain unsupported.

Adding or broadening an indexability-producing `resolver` discovery rule over
an already-ingested range is a different ordering problem. Replacing a
declaration that emits an unchanged active resolver rule has the same problem:
the replacement contract's discovery events name resolver addresses only after
Interpret materializes their edges, so an Ingest redo cannot yet fetch those
resolvers' address-scoped history. `resolver` [discovery-rule widening and
narrowing](glossary.md#discovery-rule-widening-and-narrowing) comparison is
scoped within one chain by
`(namespace, source_family, edge_kind, from_role, admission)` and preserves the
normalized address and inclusive start block of each declaration for its
`from_role`. A producer is enabled only when its canonical ABI event is present,
Interpret can select it for that declaration role (or through the
registry-announcement role bypass), and the event declares the normalized
output required by Interpret. Enabling any part of that complete predicate is
also widening; changes to non-discovery-producing events remain ordinary
manifest authority changes. Manifest synchronization loudly rejects either transition
instead of mis-certifying a one-pass redo. The operator cannot perform that
ordering with the current in-place phase workflow, because Interpret reads
discovery rules only after admission. These transitions are therefore
unsupported over retained history and require a fresh rebuild or a future
dedicated discovery backfill mechanism. Adding the first emitting declaration
to a resolver rule that previously matched no root or contract declaration is
classified as [discovery-rule
widening](glossary.md#discovery-rule-widening-and-narrowing) and is intentionally rejected over
retained history as a conservative case of the same ordering constraint.
A `resolver` discovery rule with no matching root or contract declaration is itself historical
discovery input, so adding such a rule in a new namespace over retained history is rejected like any
other widening.

Runtime resolver admission also requires the registry and resolver families to
have the same [deployment epoch](glossary.md#deployment-epoch). Manifest
synchronization compares that relationship for each desired `resolver`
discovery rule whose source is `ens_v1_registry_l1`, `ens_v2_registry_l1`,
`ens_v2_root_l1`, or `basenames_base_registry`. When the desired manifests newly
match where the preceding active pair did not, or both sides rotate to a new
matching source epoch, synchronization classifies the transition as resolver
discovery-rule widening. A manifest-version rotation of the rule-bearing
registry within the same matching epoch is a [discovery source
replacement](glossary.md#discovery-rule-widening-and-narrowing), because each
materialized edge still names the preceding source manifest. Synchronization
rejects either change when the earliest desired emitter candidate intersects
retained history: manifest snapshots cannot prove that Interpret has
already materialized every resolver discovery edge, so one Ingest redo could
otherwise fetch an incomplete address set and clear the obligation before
Interpret adds the missing edges. The boundary is the earliest candidate among
the desired declarations that emit the rule, floored by the earliest persisted
address admission. Declaration history is scoped by namespace, family, role,
and address; contract-address active ranges are shared by chain and address. Synchronization
reconstructs the floor from current active declarations and the active manifest
states retained by `SourceManifestUpdated`, combined with current and finitely
retired contract-address active ranges named by those manifests. It is not recapped by
declaration text rewritten in an earlier synchronization or erased by later
Interpret provenance writes. An omitted declaration start is stored as `NULL`
on its first admission and read as an effective block-zero lower bound. Within
that initial address epoch, refreshing the active row materializes the bound as
zero; a later finite declaration therefore cannot replace the retained
effective-zero floor. Conversely, omitting a previously finite start backdates
that active epoch to zero, so the required redo can select the newly widened
interval. Re-admitting a retired address still begins after its prior epoch.
When a later declared start does not raise an already lower stored address
bound, synchronization keeps that bound. The [persisted address
floor](glossary.md#persisted-address-floor) notice is `declared start … did not
raise persisted address floor; keeping …`. It is informational: it reports
that the shared address interval did not move, while the declaration's own
start still participates in the effective Ingest range. It does not change
admission, watch selection, or redo behavior.
Retained omitted-start manifest history also contributes zero during widening
classification. Interpret's discovery refresh now leaves an initial epoch's
stored `NULL` untouched, fixing
[issue #547](https://github.com/ensdomains/bigname/issues/547); the laundering
sequence between unchanged synchronizations of an already-declared address is
legacy-only, while the repair still intentionally fires when a finite
discovery-created address row is later declared for the first time with an
omitted start. When a desired active declaration omits its start,
synchronization restores zero on the
earliest address epoch even if it has retired, while any re-admitted epoch
remains bounded after it, stamps the required Ingest redo from block zero
(clamped to the earliest configured source start), and invalidates the derived
phases for the restored interval. That repair
converges in one sync: the stored row is then zero and its positive-floor
predicate cannot fire again. A current finite declaration keeps the persisted
finite watch bound; retained omitted-start history still supplies zero only to
later widening classification. Thus
splitting a declaration-start raise and a discovery-enabling change across two
synchronizations cannot make a historically admitted emitter future-only. A
rule with no matching declaration contributes block zero as a conservative
historical input. An `ens_v2_registry_l1` manifest that also has a
`registry_announcement` rule
contributes a distinct block-zero candidate even when the emitterless candidate
or direct declarations already exist. Adding that role-free path is resolver
discovery-rule widening: a registry admitted by `RegistryCreated` has no
declaration role, but its `ResolverUpdated` event can still match the active
rule.
(upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L66 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L478 @ ens_v2@a971bd64)
A matching-epoch transition whose earliest effective candidate and persisted
admission floor both start after the latest published head is future-only and
remains admissible, as does
a source-manifest rotation before any Ingest range is retained. Changing from
matching to nonmatching removes resolver intervals and is admissible narrowing.
A family with no active `resolver` rule admits no discovered resolver address,
so an epoch match alone is also admissible.

Each ENSv2 discovery candidate records whether its canonical discovery-producing
event is effectively enabled: `RegistryCreated()` with normalized
`RegistryCreated` for registry announcements, or
`ResolverUpdated(uint256,address,address)` with normalized `ResolverChanged`
for resolver edges, subject to the Interpret role selection described above.
Adding either producer or enabling it through `emitter_roles` or
`normalized_events` changes discovery coverage even when `manifest_version` is
unchanged; ordinary ABI watch widening is insufficient because Interpret, not
Ingest, materializes the newly discovered address intervals. Removing a
resolver producer from a direct declaration is conservatively rejected over
retained history rather than assuming that Interpret has retracted every
retained edge. A `registry_announcement` normalized-output removal has
different behavior: synchronization accepts it with a stamped required Ingest
redo, but Interpret halts on the next selected `RegistryCreated` because the
required normalized event is undeclared. The [manifest-authority
marker](glossary.md#manifest-authority-marker) guarantees the invalidation at
synchronization, but it is not a permanent manifest-validity guard: a redo with
no matching event can clear, and a later `RegistryCreated` then halts normal
Interpret without an outstanding redo. Removing the resolver producer from an
announcement-only/emitterless path remains unclassified. ABI removal can
therefore leave retained coverage without a reproducible desired rule; dropping
only `ResolverChanged` from `normalized_events` instead causes a loud,
recoverable Interpret halt on the next selected `ResolverUpdated`, but an empty
rebuild can likewise clear the preceding invalidation. Other ABI events
continue through the ordinary watch-plan widening path.

The `ens_v2_root_l1` resolver rule is the concrete same-version case: its
`ResolverUpdated` producer and watch topic were already declared and selected,
but adding the missing rule still widens [discovery-rule
coverage](glossary.md#discovery-rule-widening-and-narrowing) because Interpret
can now materialize resolver address intervals. The active manifest remains
version 2; synchronization updates that manifest tuple in place and replaces
its child rule rows. With no completed retained Ingest range overlapping the
RootRegistry start, synchronization accepts the change, requests no Ingest
redo solely for the rule, and replaces non-null Interpret and Project content
hashes with the [manifest-authority
marker](glossary.md#manifest-authority-marker). With overlapping completed
retained Ingest history, synchronization rejects the transaction atomically:
the prior manifest payload, child rules, authority history, phase hashes, and
redo state remain unchanged. That retained database cannot install the rule in
place; replacing it with a rebuilt database does not make one pass sufficient
by itself. On a fresh replacement database, Interpret can discover resolver
address/topic intervals after the initial Ingest pass has completed. When those
intervals add coverage over already-ingested blocks, Interpret records required
Ingest work. The runner automatically re-fetches the affected retained range
with the discovery-aware filter and re-runs Interpret before Project and Verify
proceed. Interrupted discovery repairs remain durable and resume through the
normal runner recovery path. At startup, after Live, and after an
operator-requested Interpret or all-phase redo, the runner can repeat the
sequence of re-fetching newly admitted historical logs and re-running
Interpret once for each active admitted discovery rule, plus eight additional
times before downstream phases proceed. This fixed ceiling is a runaway
backstop, not a tuning control:
exhausting it stops the chain with an operator-visible error while Project and
Verify remain fenced. Keep serving disabled, inspect
`discovery_watch_admissions` and `chain_phase_state`, correct the
non-converging admission or redo lifecycle, and then restart the runner.
Operators do not need to schedule the former manual second pass for the
ordinary convergent case. Keep serving disabled until repair, projection, and
the configured verification gate have completed.

For a pre-[#652](https://github.com/ensdomains/bigname/issues/652) binary only,
the fallback remains a manual second pass. Use `phase-runner redo --phase
ingest` with the Sepolia manifest root explicitly selected (`--manifests-root
manifests/sepolia`) and the required database, chain, and source options while
normal and Live processing are held at a fixed target already completed by both
Interpret and Project, with Interpret's discovery edges materialized through
that target. Cover the manifest-declared RootRegistry start block through that
target, then explicitly rerun Interpret and Project through the same target. If
[stored-history
verification](glossary.md#stored-history-verification) halted on the missing
logs, restart the normal runner to complete Verify and keep serving traffic
disabled until Verify succeeds; otherwise complete Verify before resuming
normal and Live processing.

`registry_announcement` rules use the same namespace-scoped comparison. In the
`ens_v2_registry_l1` family they are backfillable in one Ingest redo: Ingest
first selects a declared `RegistryCreated` as an all-emitter event, then fetches
the announcing registry's remaining address-scoped events from that event's
position in the same window; later windows preload retained canonical
announcements. Historical rule widening or emitting-source replacement in that
family therefore stamps a required Ingest redo from the earlier of the desired
emitting declaration's start and the earliest retained canonical
`RegistryCreated` selected by the block-zero all-emitter watch. Starting at that
retained announcement lets its address-scoped events enter the same redo even
when the announcement predates the declaration used to anchor the rule
comparison. Historical announcement widening in any other family is loudly
rejected because it has no declared same-window intake path. Fresh transitions,
and future-only transitions with no earlier retained announcement, remain
admissible.
A chain's retained-history boundary for this check is its latest published
head. A finite Ingest position left ahead of that head after a rewind is not
readable coverage; when no published-head row exists, the check falls back to
the finite Ingest position.
A new chain with no ingested range, a tracked rule or emitting-source
replacement whose start is after retained history, and [discovery-rule
narrowing](glossary.md#discovery-rule-widening-and-narrowing) remain admissible.
Topology-only `subregistry` rules and the reserved
[`migration` edge kind](glossary.md#migration-edge-migration) are excluded from
this comparison because neither admits an address for historical intake.

This is a scoped completeness claim for the current address-admitting discovery
paths, not for every manifest field. The classifier covers resolver and registry
announcement rule identity, additions, removals, and declaration starts;
matching root/contract addresses and roles; source-manifest version and
deployment-epoch replacement; announcement-backed admission; canonical producer
ABI presence, `emitter_roles`, and required `normalized_events`; and persisted
address-admission floors, including finitely retired active ranges. The compiled-watch
comparison separately covers ordinary ABI topics, declared addresses, and
start ranges. Capability flags,
correlation metadata, resolver implementation metadata, and proxy metadata do
not feed these discovery intervals. The following are outside the supported
transition set and this completeness claim. Synchronization currently accepts
removal of the resolver producer from an announcement-only/emitterless path,
with the two consequence classes described above, and normalized-output removal
can be followed by an empty rebuild that clears before the next producer event.
Reuse of a retired address under a different namespace, family, or role whose
declared start precedes its bounded new contract-address active range can
conservatively inherit the older address floor. Interpret redo preserves a
finitely retired manifest-declared contract-address range, while a later
observation can append a bounded re-admission range. A binary change can also
add a new address-admitting discovery edge kind or change
Interpret selection or discovery behavior without a manifest-field transition.
The accepted fail-loud configurations are unsupported operator transitions,
not proof of safe narrowing; supporting any listed shape requires a new proof
and, where needed, a classifier arm.

If a newly watched tuple intersects an already-ingested range, synchronization
records the ordinary [manifest-authority
marker](glossary.md#manifest-authority-marker) on Interpret and Project and
stamps a required Ingest redo from the first newly watched block through the
latest published ingested head. It does not contact a provider or perform that
potentially expensive fetch. The phase runner fails closed before derivation
and prints the exact chain, phase, and range command prefix plus an instruction
to append the configured sources. Complete that command with the updated
manifests active and the chain's configured sources. Its shape is:

```sh
phase-runner redo \
  --chain <chain> \
  --phase ingest \
  --from-block <first-affected-block> \
  --to-block <last-affected-block> \
  --source <CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK[:ROLE]=URL_ENV>
```

Copy each [intake-capable](glossary.md#source-role) descriptor with its configured role preserved
as `CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK:ROLE=URL_ENV`; pass only `intake` or
`both` sources to Ingest redo, never a `verification-only` source. Repeat `--source` once for every intake-capable source key.

An ingest redo fetches and retains the newly selected facts, but intentionally
does not advance the finite `ingest_cursors` used by the initial spine. Finite
cursors prove that a source reached its target, and readable lineage proves
which blocks were loaded, but both cover only the facts selected by the watch
plan active at load time. Neither proves that a later widening's facts were
fetched. Successful completion of the stamped redo clears the Ingest
obligation; only then may ordinary derivation resume. The redo includes a
Live-loaded suffix through the latest published head even though Live does not
advance finite source cursors. A required redo clears only when its loaded
range-end hash is the readable hash at that height, so loading a coherent
sibling fork cannot certify facts for the fork Interpret will read. That
required-redo range-end check does not change ordinary operator repair redos,
which may reconcile a cursor to another retained fork before normal head
publication. Every Base Ingest redo, required or ordinary, whose range includes
the source seam also requires each redo batch to run one independent Coinbase
SQL `base.blocks` identity query and the RPC block lookup and to obtain the same
seam-block hash. Rechecking before every batch prevents a source fork change
from combining pre-seam coverage across batches. The Coinbase schema exposes
`block_number`, `block_hash`, and reorganization `action` on that table,
so this proof does not depend on a watched log being
present.[^coinbase-sql-blocks]

Removing watched tuples, repeating the same manifest set, adding a chain with
no Ingest coverage, or adding a watched window that starts after the retained
head does not stamp Ingest. A later widening extends an existing required or
interrupted Ingest redo rather than replacing it. Once stamped, the obligation
persists across a later narrow-back: synchronization has no clearing path, and
only successful completion of the recorded redo clears it. Completing that redo
after narrowing is safe because the extra work is fetch cost only; it cannot
reduce retained fact coverage. Do not edit cursors to clear the obligation.

The fence error prints the invalidation token from the current marker. After
the fetch, re-run the required full Interpret redo with
`--attest-watch-set-coverage <token>`. If review establishes that the authority
change widened no watch-plan range, the same token-valued flag attests that
conclusion without a fetch. For a multi-chain redo, repeat
`--attest-watch-set-coverage <chain>=<token>` for each affected chain; one token
cannot attest multiple chains. The runner compares each supplied token with
the current marker again while holding the phase-state lock. A later manifest
sync, including a return to the same desired authority, mints a different token
and makes an earlier attestation stale.

The redo-begin transaction appends one immutable audit row for the chain,
Interpret phase, invalidation token, authority fingerprint, redo range, runner
instance ID, and attestation time. That transaction also adopts the new
[interpreter content hash](glossary.md#interpreter-content-hash), so the marker
cannot be discharged without its audit row.
The error-level structured telemetry is emitted from the durable row after
commit. If the runner stops before that emission completes, the next redo
attempt re-emits the row only after the locked begin matches the same active
redo and commits. The same token-valued command is valid for that exact active,
audited redo; once the redo completes, passing the token again is a hard error.
If a binary upgrade changes the interpreter content hash while the redo is
interrupted, re-run that exact range with the same token. The new binary retains
the audit association but clears progress written under the prior hash and
walks the range again from its beginning.

Manifest synchronization now distinguishes manifest-authored watch-plan
widening from narrowing and unrelated authority changes and enforces the
historical fetch with the required Ingest redo. The attestation remains the
operator's durable acknowledgement of the whole authority transition and is
still required for every authority-marked Interpret redo, whether or not that
transition stamped Ingest. An interpreter content hash rotation with neither a
current manifest-authority marker nor an active audited redo remains flagless.

## Manifest change propagation

Manifest declaration changes produce the `SourceManifestUpdated` [normalized
event](glossary.md). Its state includes proxy declarations and the staged
authored capability fields, so manifest synchronization does not mint separate
proxy- or capability-change event kinds. Each `SourceManifestUpdated` event
written by current synchronization carries `raw_fact_ref.applied_change_count`,
the per-manifest transition counter incremented once for each applied manifest
transition in the same transaction, and that counter participates in
`event_identity`; older events retain their original `raw_fact_ref` and
identities. When persisted manifest authority already matches the desired
declaration but the newest stored event is missing or its `after_state`
disagrees with that persisted state, synchronization appends another
`SourceManifestUpdated` that re-derives the current-state transition without
changing manifest authority.
The synchronization transaction also invalidates completed interpret and
project phase content hashes for chains with changed authority or repaired
manifest event history. The deleted [admission
epoch](glossary.md#admission-epoch) and full-source
reconciliation writers no longer participate; phase redo applies the current
authority to discovery and projection state. Repairing Ethereum Mainnet
`basenames_execution` history also invalidates the Base project phase because
that phase consumes the repaired events. A history-triggered invalidation uses
the same manifest-authority marker and attested full Interpret redo as an
authority change.

The legacy resolver-profile authority journal, input queue, and reconciliation tables remain only in immutable migration history. Their Rust APIs and consumers have been deleted. Current manifests and schema-v2 discovery edges are the admission truth; legacy rows do not gate synchronization or interpretation.

ERC-1967 `Upgraded(address)` logs from manifest-declared and event-announced contracts produce `Upgraded` normalized history on the emitting contract. (upstream: .refs/basenames/lib/openzeppelin-contracts/contracts/interfaces/IERC1967.sol:L13 @ basenames@1809bbc) Manifest synchronization does not infer upgrades from code-hash drift and does not synthesize drift-alert normalized events.

The schema-v2 project phase has no code-hash reader. ENSv1 and Basenames use
only exact resolver addresses declared by the active manifest, while ENSv2
uses the latest canonical resolver-family `Upgraded` history and the active
manifest's `resolver_implementations` list. A manifest admission change
reclassifies the affected address inline; there is no journal, queue, code-hash
reader, or legacy resolver-profile view.

## Watch-plan expansion

Watch-plan expansion starts from active manifest roots by `contract_instance_id` and traverses active discovery edges by ID.

- The chain-intake watch target is the address range attached to each active contract instance at the requested time.
- If a manifest target carries `start_block`, the materialized watch range starts at that inclusive block unless a later active-range boundary narrows it.
- If `start_block` is omitted, runtime watch and Interpret selection use block
  zero as the effective lower bound; this is a conservative intake boundary,
  not a claim that the upstream contract was deployed at genesis. Refreshing an
  existing initial-epoch address row materializes that effective-zero bound so
  a later finite declaration cannot erase retained-history evidence; omitting a
  previously finite start likewise backdates that active epoch to zero. A
  readmission after retirement remains bounded after the preceding epoch.
- Legacy watch rows may denormalize address and code-hash state, but their durable explanation path is `manifest root → discovery edge(s) → contract_instance_id`; schema-v2 resolver classification does not read that denormalization.
- Address-only watch state is rebuildable from manifests, instance attributes, and active discovery edges.

The materialized watch plan is derived from active manifests and discovery
edges. No worker watch-plan inspection command remains.

## Capability policy

Capabilities gate behavior, not public-contract existence. An unsupported capability surfaces as `coverage.unsupported_reason` or a typed error. Shadow capabilities admit facts without enabling general reads. Adding a new capability is additive only when it does not change prior semantics.

## Ownership

- Manifest/discovery owners maintain the TOML files.
- Schema-v2 interpretation consumes manifest versions as inputs.
- Lookup uses manifest versions and admitted entrypoints for request-scoped
  provider execution.
- Schema changes require a doc-first update to this file.

---

## Bootstrap `start_block` provenance

Known historical starts cite a pinned upstream source. Targets without a pinned
source omit `start_block`; historical bootstrap skipped them rather than
inventing values. Basenames mainnet families and the ENS Universal Resolver
remain unknown. The current phase-loader fallback described
above does not change that provenance rule.

| Target | `start_block` | Source |
| --- | --- | --- |
| ENSv1 ENSRegistry | `9380380` | [^subgraph-l15] |
| ENSv1 ENSRegistryOld | `3327417` | [^subgraph-l39] |
| ENSv1 BaseRegistrar | `9380410` | [^subgraph-l122] |
| LegacyEthRegistrarController | `9380471` | [^subgraph-l145] |
| WrappedETHRegistrarController | `16925618` | [^v1-wrapethrc-l640] |
| ETHRegistrarController | `22764821` | [^v1-ethrc-l706] |
| ENSv1 NameWrapper | `16925608` | [^v1-namewrapper-deploy] |
| ENSv1 ENSRegistry (Sepolia) | `3702728` | [^v1-sepolia-ensregistry] |
| ENSv1 superseded registry (Sepolia) | `3702721` | [^v1-sepolia-legacyregistry] |
| ENSv1 NameWrapper (Sepolia) | `3790153` | [^v1-sepolia-namewrapper] |
| ENSv1 BaseRegistrar (Sepolia) | `3702731` | [^v1-sepolia-baseregistrar] |
| ENSv1 PublicResolver latest (Sepolia) | `8580001` | [^v1-sepolia-public-resolver-receipt] |
| ENSv1 PublicResolver `0x8948458…` (Sepolia) | `0` | [^v1-sepolia-app-resolvers] and the existing retired-bootstrap divergence |
| ENSv1 PublicResolver `0x8FADE66…` (Sepolia) | `0` | [^v1-sepolia-app-resolvers] and the existing retired-bootstrap divergence |
| ENSv1 LegacyPublicResolver (Sepolia) | `3790166` | [^v1-sepolia-legacy-resolver-receipt] |
| ENSv1 PublicResolver (latest) | `22764828` | [^v1-publicresolver-deploy] |
| ENSv1 ReverseRegistrar | `16925606` | [^v1-revreg-deploy-l379] |
| ENSv2 RootRegistry (post-audit Sepolia) | `11163319` | [^v2-deploy-root] |
| ENSv2 ETHRegistry (post-audit Sepolia) | `11163391` | [^v2-deploy-ethreg] |
| ENSv2 ETHRegistrar (post-audit Sepolia) | `11163403` | [^v2-deploy-ethrc] |

---

[^ens-docs-univ]: <https://docs.ens.domains/resolvers/universal/>
[^v1-app-resolvers]: (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L32 @ ens_app_v3@7175858)
[^ensnode-mainnet]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L343-L377 @ ensnode@2017ae6)

[^v1-ens-l12]: (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)
[^v1-ensreg-l89]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)
[^v1-ensreg-l174]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f)
[^v1-ensregfb-l40]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L40 @ ens_v1@91c966f)
[^v1-resolver-signature-abis]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IABIResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/Resolver.sol:L33 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IContentHashResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDNSRecordResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDNSRecordResolver.sol:L13 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDNSZoneResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDataResolver.sol:L7 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IInterfaceResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IVersionableResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_subgraph/subgraph.yaml:L109 @ ens_subgraph@723f1b6)

[^v1-iname-l27]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)
[^v1-iname-l35]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f)
[^v1-iname-l37]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f)
[^v1-iname-l38]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f)

[^v1-namewrapper-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L1498 @ ens_v1@91c966f)
[^v1-sepolia-ensregistry]: (upstream: .refs/ens_v1/deployments/sepolia/ENSRegistry.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/sepolia/ENSRegistry.json:L409 @ ens_v1@91c966f)
[^v1-sepolia-legacyregistry]: (upstream: .refs/ens_v1/deployments/sepolia/LegacyENSRegistry.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/sepolia/LegacyENSRegistry.json:L390 @ ens_v1@91c966f)
[^v1-sepolia-fallback]: (upstream: .refs/ens_v1/deployments/sepolia/ENSRegistry.json:L148 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/sepolia/ENSRegistry.json:L415 @ ens_v1@91c966f)
[^v1-sepolia-namewrapper]: (upstream: .refs/ens_v1/deployments/sepolia/NameWrapper.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/sepolia/NameWrapper.json:L1512 @ ens_v1@91c966f)
[^v1-sepolia-baseregistrar]: The address is authoritative ENSv1 deployment metadata, while the start block is reference-only ENS subgraph metadata: (upstream: .refs/ens_v1/deployments/sepolia/BaseRegistrarImplementation.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_subgraph/networks.json:L47-L49 @ ens_subgraph@723f1b6)
[^v1-sepolia-baseregistrar-stale-receipt]: The pinned artifact records a receipt for a different address and two deployments, so its receipt block is not evidence for the admitted address: (upstream: .refs/ens_v1/deployments/sepolia/BaseRegistrarImplementation.json:L733-L768 @ ens_v1@91c966f)
[^v1-sepolia-receipt-backed-controllers]: The Legacy controller artifact records its address and a successful deployment receipt at block `3790197`: (upstream: .refs/ens_v1/deployments/sepolia/LegacyETHRegistrarController.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/sepolia/LegacyETHRegistrarController.json:L562-L599 @ ens_v1@91c966f). The later controller artifact records its address and retained receipt at block `8579988`: (upstream: .refs/ens_v1/deployments/sepolia/ETHRegistrarController.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/sepolia/ETHRegistrarController.json:L680-L720 @ ens_v1@91c966f).
[^v1-sepolia-wrapped-controller-gap]: The v1-reference override is tracked for Sepolia chain `11155111`, and its complete controller artifact records the address and ABI but no deployment receipt or start metadata: (upstream: .refs/ens_v2/contracts/deployments/README.md:L60-L66 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/README.md:L141-L142 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/v1/sepolia/.chainId:L1 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/v1/sepolia/WrappedETHRegistrarController.json:L1-L21 @ ens_v2@a971bd64) `ETHRenewerV1` declares the constructor input and encodes the same address, while both reference indexers assign it block `3790244`: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRenewerV1.json:L37-L45 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRenewerV1.json:L894 @ ens_v2@a971bd64) (upstream: .refs/ens_subgraph/networks.json:L55-L57 @ ens_subgraph@723f1b6) (upstream: .refs/ensnode/packages/datasources/src/sepolia.ts:L76-L80 @ ensnode@2017ae6)
[^v1-sepolia-app-resolvers]: The four ordered entries record addresses, latest tagging, wrapper compatibility, supported interfaces, and the latest resolver's `supportsDefaultCoinType` flag. The table's older-generation classifications follow the maintainer-approved ordering rather than an upstream tag: (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L149-L221 @ ens_app_v3@7175858). The latest `PublicResolver` composes `AddrResolver`, whose multicoin getter applies the default coin-type fallback: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20-L31 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L68-L85 @ ens_v1@91c966f)
[^v1-sepolia-public-resolver-receipt]: The retained deployment receipt records block `8580001`: (upstream: .refs/ens_v1/deployments/sepolia/PublicResolver.json:L1091-L1107 @ ens_v1@91c966f)
[^v1-sepolia-legacy-resolver-receipt]: The retained deployment receipt records block `3790166`: (upstream: .refs/ens_v1/deployments/sepolia/LegacyPublicResolver.json:L868-L880 @ ens_v1@91c966f)
[^v1-mainnet-owned-resolver]: The pinned Mainnet deployment set records the `EthOwnedResolver` deployment marker, and its deploy script sets that resolver on `.eth`; the checked-in Mainnet `ens_v1_resolver_l1` manifest instead admits only its declared PublicResolver generations: (upstream: .refs/ens_v1/deployments/mainnet/.migrations.json:L15-L22 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deploy/resolvers/00_deploy_eth_owned_resolver.ts:L7-L38 @ ens_v1@91c966f)
[^v1-sepolia-owned-resolver]: The Sepolia artifact records its address and receipt block `3790128`, while the ruled app list contains the four PublicResolver generations above: (upstream: .refs/ens_v1/deployments/sepolia/OwnedResolver.json:L1-L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/sepolia/OwnedResolver.json:L878-L899 @ ens_v1@91c966f) (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L149-L221 @ ens_app_v3@7175858)
[^v2-sepolia-v1-mirror]: The premigration deploy passes `ENSV1Resolver` as the placeholder resolver and the registrar assigns it during reservation; the admitted deployment artifact records the mirror's address. The mirror looks up the ENSv1 registry's resolver and forwards resolution to it: (upstream: .refs/ens_v2/contracts/deploy/testnet/01_TestnetV1PremigrationRegistrar.ts:L42-L56 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L255-L266 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ENSV1Resolver.json:L1-L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/resolver/ENSV1Resolver.sol:L12-L40 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/resolver/AbstractMirrorResolver.sol:L66-L81 @ ens_v2@a971bd64)
[^v1-publicresolver-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L1104 @ ens_v1@91c966f)
[^v1-revreg-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-revreg-deploy-l379]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L379 @ ens_v1@91c966f)
[^v1-l2rev-base-deploy]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-l2rev-base-args]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f)
[^v1-l2rev-event]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
[^v1-l2rev-nameforaddr]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L154 @ ens_v1@91c966f)
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

[^v2-deploy-root]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/RootRegistry.json:L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/RootRegistry.json:L2792 @ ens_v2@a971bd64)
[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistry.json:L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistry.json:L2792 @ ens_v2@a971bd64)
[^v2-deploy-ethrc]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistrar.json:L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistrar.json:L1372 @ ens_v2@a971bd64)
[^v2-deploy-pres]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L2 @ ens_v2@a971bd64)
[^v2-pres-uups]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L22 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L89 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-upgraded]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L627 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L637 @ ens_v2@a971bd64)
[^v2-deploy-public-resolver]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PublicResolverV2.json:L2 @ ens_v2@a971bd64)
[^v2-public-resolver-discovery]: `PublicResolverV2` composes the standard resolver profiles and authorizes writes through registry ownership or approvals; locked-name migration can replace a recognized ENSv1 resolver with that public resolver before a nonzero registered resolver emits `ResolverUpdated`: (upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L4 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L23 @ ens_v2@a971bd64) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PublicResolverV2.sol:L179-L183 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L139 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L478 @ ens_v2@a971bd64)
[^v2-public-resolver-version]: The deployed resolver ABI includes `VersionChanged` and `clearRecords`: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PublicResolverV2.json:L429 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PublicResolverV2.json:L598 @ ens_v2@a971bd64)

[^v2-userreg-l15]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/UserRegistryImpl.json:L2 @ ens_v2@a971bd64) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registry/UserRegistry.sol:L15 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-ethrc-l49]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@a971bd64)
[^v2-ethrc-l173]: (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L151 @ ens_v2@a971bd64)

[^v2-pr-l22]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L23 @ ens_v2@a971bd64)
[^v2-pr-l28]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L29 @ ens_v2@a971bd64)

[^v2-pres-l38]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L33 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l70]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L19 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-data]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L46 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L161 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L437 @ ens_v2_sepolia_20260629@ccaeb58)

[^v2-iperm-l22]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L24 @ ens_v2@a971bd64)
[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L39 @ ens_v2@a971bd64)
[^v2-iperm-resolver-l14]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L19 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-iethreg-l32]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@a971bd64)

[^v2-events-created]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@a971bd64)
[^v2-events-l15]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L18 @ ens_v2@a971bd64)
[^v2-events-l49]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L56 @ ens_v2@a971bd64)
[^v2-events-l69]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L82 @ ens_v2@a971bd64)
[^v2-events-l75]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L88 @ ens_v2@a971bd64)
[^v2-events-uri]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L76 @ ens_v2@a971bd64)

[^v2-eac-l19]: (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L22 @ ens_v2@a971bd64)

[^bn-readme-l22]: (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc)
[^bn-readme-l28]: (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc)
[^bn-readme-l29]: (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc)
[^bn-readme-l30]: (upstream: .refs/basenames/README.md:L30 @ basenames@1809bbc)
[^bn-readme-l33]: (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc)
[^bn-readme-l34]: (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc)
[^bn-readme-l36]: (upstream: .refs/basenames/README.md:L36 @ basenames@1809bbc)
[^bn-readme-l37]: (upstream: .refs/basenames/README.md:L37 @ basenames@1809bbc)
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
[^bn-registrar-controller-l180]: (upstream: .refs/basenames/src/L2/RegistrarController.sol:L180 @ basenames@1809bbc)
[^bn-registrar-controller-l187]: (upstream: .refs/basenames/src/L2/RegistrarController.sol:L187 @ basenames@1809bbc)
[^bn-upgradeable-registrar-controller-l191]: (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L191 @ basenames@1809bbc)
[^bn-upgradeable-registrar-controller-l198]: (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L198 @ basenames@1809bbc)

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
[^bn-revreg-l58]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L58 @ basenames@1809bbc)
[^bn-revreg-l150]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
[^bn-revreg-l155]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L155 @ basenames@1809bbc)
[^bn-revreg-l156]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L156 @ basenames@1809bbc)
[^bn-revreg-l157]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L157 @ basenames@1809bbc)
[^bn-revreg-l193]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
[^bn-revreg-l209]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L209 @ basenames@1809bbc)
[^bn-constants-l12]: (upstream: .refs/basenames/src/util/Constants.sol:L12 @ basenames@1809bbc)
[^bn-constants-l13]: (upstream: .refs/basenames/src/util/Constants.sol:L13 @ basenames@1809bbc)
[^bn-sha3-l15]: (upstream: .refs/basenames/src/lib/Sha3.sol:L15 @ basenames@1809bbc)
[^bn-sha3-l20]: (upstream: .refs/basenames/src/lib/Sha3.sol:L20 @ basenames@1809bbc)
[^bn-sha3-l31]: (upstream: .refs/basenames/src/lib/Sha3.sol:L31 @ basenames@1809bbc)
[^coinbase-sql-blocks]: [Coinbase SQL API schema — `base.blocks`](https://docs.cdp.coinbase.com/data/sql-api/schema#base-blocks).
