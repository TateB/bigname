# Upstream references

bigname anchors every ENSv1, ENSv2, Basenames, admitted upstream app-metadata, reference-indexer comparison, and reference execution-client comparison claim to a specific upstream commit pinned under `.refs/`. This doc is the human-readable companion to `.refs/MANIFEST.toml` — the pin table, rotation policy, and the intentional-divergence list.

## Pinned refs

| Key | Repo | Commit | Purpose |
|-----|------|--------|---------|
| `ens_v1` | `ensdomains/ens-contracts` | `91c966fe` | Canonical ENSv1 Solidity |
| `ens_v2` | `ensdomains/contracts-v2` | `554c309b` | ENSv2 contracts |
| `basenames` | `base-org/basenames` | `1809bbc9` | Canonical Basenames Solidity |
| `ens_subgraph` | `ensdomains/ens-subgraph` | `723f1b6a` | Reference ENSv1 indexer |
| `ensnode` | `namehash/ensnode` | `2017ae62` | Alternative ENS indexer |
| `ens_app_v3` | `ensdomains/ens-app-v3` | `71758582` | ENS app known resolver metadata |
| `ponder` | `ponder-sh/ponder` | `c8f6935f` | Reference EVM indexer framework |
| `graph_node` | `graphprotocol/graph-node` | `aefe1737` | Reference Graph Node indexer |
| `reth` | `paradigmxyz/reth` | `194ece23` | Reference Ethereum execution client |

Full pin records (including per-ref `authoritative_for` lists) live in `.refs/MANIFEST.toml`. Sync with `scripts/sync-refs`.

## Citation format

```
(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)
```

Use this exact shape everywhere — docs, ADRs, manifests, code comments, task writeups, agent output. Consistent format lets `$upstream-evidence`, `evidence_reader`, and `verification_reviewer` verify citations mechanically.

## Rotation policy

- **Bump when**: a cited upstream file changes materially, or we need to adopt a new upstream behavior (new contract, new event, new invariant). Staying behind upstream is fine — being silently wrong is not.
- **Do not bump for**: drive-by upstream refactors, test-only changes, comment edits, rename-only commits.
- **How to bump**:
  1. Update the `commit` field in `.refs/MANIFEST.toml`.
  2. Update the row in the table above.
  3. Run `scripts/sync-refs`.
  4. Re-grep the repo for `@ <key>@<old-short-commit>` citations; update any that point at content that changed across the bump.
  5. Add or edit entries in § Known divergences if the bump surfaced or resolved an intentional deviation.
  6. Commit with a message naming what upstream change motivated the bump, e.g. `chore(refs): bump ens_v1 to <new-sha> — adopt new reverseClaimer event`.
- **Who decides**: whoever owns the surface affected. Ambiguous cases route through `$contract-impact`; cross-surface bumps route through `verification_reviewer` after the sync.

## Known divergences

Intentional differences between our docs/manifests and upstream. Every divergence lives here so that citations reading "differently than upstream" are legible instead of looking like bugs. If a divergence is not in this list, it should be treated as drift and closed — either by updating our doc or by adding the entry.

> **ENS Universal Resolver proxy entrypoint vs pinned implementation artifact** — bigname uses the official ENS Universal Resolver proxy address as the route-facing `ens_execution` entrypoint, even though the pinned ENSv1 deployment artifact under `.refs/ens_v1` records the implementation / ABI anchor.
> **Upstream**: official ENS docs list `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` as the Universal Resolver proxy on Mainnet and testnets (https://docs.ens.domains/resolvers/universal/; https://docs.ens.domains/learn/deployments/). The pinned ENSv1 deployment artifact records `0xED73a03F19e8D849E44a39252d222c6ad5217E1e` as the implementation artifact `(upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)` and the ABI/behavior anchor remains the pinned Universal Resolver Solidity `(upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f)`.
> **Our rule**: `docs/manifests.md` § Required Fields and Capability Policy, `docs/execution.md` § Resolution Execution Flow and § Initial Support Boundary, and `docs/architecture.md` § Source Families / Coverage And Exhaustiveness Rules / Deterministic Execution And Verification Plane.
> **Why**: callers and manifests should target the official proxy entrypoint, while `.refs/ens_v1` remains the pinned implementation/ABI source for behavior citations.
> **Since**: `2026-04-22`

> **Basenames verified/explain public support narrowing** — bigname narrows the upstream Basenames L1Resolver and CCIP entrypoint into one first public support class instead of publishing every upstream-reachable non-`base.eth` path immediately.
> **Upstream**: `(upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)` `(upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)` `(upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc)`
> **Our rule**: `docs/api-v1.md` § `GET /v1/profiles/names/{name}` and § `GET /v1/explain/resolutions/{namespace}/{name}/execution`; mirrored in `docs/execution.md` § Initial Support Boundary and `docs/manifests.md` § Basenames source-family ownership.
> **Why**: freeze the first Basenames consumer-replacement slice on the declared Base-authority plus L1-transport boundary before widening alias-participating, wildcard-derived, linked-subregistry, transport-free, or offchain-gateway path classes.
> **Since**: `2026-04-19`

> **ENSv1 wrapper/resolver admission narrowing** — bigname admits the mainnet NameWrapper and PublicResolver as source-family inputs for current declared-state normalization without claiming every upstream wrapper or resolver capability as supported public coverage.
> **Upstream**: `(upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L479 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L500 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L5 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L13 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f)`
> **Our rule**: `docs/manifests.md` § capability ownership and § capability policy; mirrored in `docs/architecture.md` § source families, `docs/storage.md` § table families, and `docs/projections.md`.
> **Why**: bind the wrapper/resolver adapter boundary to source-family ownership, identity continuity, and declared resolver record state. Wrapper-upgrade and migration history are admitted separately when those surfaces ship.
> **Since**: `2026-04-21`

> **ENSv1 generic resolver-event intake and known PublicResolver-generation profile narrowing** — bigname may retain generic ENSv1 resolver-local record events as observed selector/cache or version-boundary facts even when the emitter's resolver profile is still pending. ENS Labs PublicResolver-generation profiles are semantic admissions for complete family coverage, resolver overview support, latest-only behavior, resolver-local authorization semantics, and event-to-onchain-call parity claims; they are not the default address set for generic resolver-event intake. The checked-in mainnet manifest directly admits the latest PublicResolver plus first-party app known mainnet generations; Sepolia known resolver rows are documented for a future ENSv1 Sepolia profile root and are not loaded by the current mainnet runtime.
> **Upstream**: `(upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/IVersionableResolver.sol:L5 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L59 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L63 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L73 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L84 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L20 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L21 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L28 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L32 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L21 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L22 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L23 @ ens_v1@91c966f)`
> **ENS app known-resolver metadata**: `(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L32 @ ens_app_v3@7175858)` `(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L33 @ ens_app_v3@7175858)` `(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L35 @ ens_app_v3@7175858)` `(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L149 @ ens_app_v3@7175858)`
> **Our rule**: `docs/manifests.md` § ENSv1 NameWrapper and PublicResolver admission and § capability policy; mirrored in `docs/storage.md`, `docs/projections.md`, `docs/api-v1.md` (resolution and resolver routes), and `docs/consumer-capabilities.md`.
> **Why**: a registry-observed resolver is not the same as an admitted resolver-profile. Unknown dynamic resolvers and unsupported legacy interfaces stay `pending` or `unsupported` for profile-gated behavior; observed selector/cache facts still flow from generic resolver events.
> **Since**: `2026-04-21`

> **Basenames dynamic Base resolver discovery deferred until follow-on implementation** — bigname currently has the static Base `L2Resolver` seed, but the frozen consumer-replacement contract now requires resolver discovery from Basenames registry `NewResolver` observations before declared record reads can claim complete Base-side resolver coverage.
> **Upstream**: `(upstream: .refs/basenames/src/L2/Registry.sol:L19 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc)`
> **Our rule**: `docs/manifests.md` § Basenames source-family ownership and `docs/chain-intake.md` § resolver discovery boundary.
> **Why**: a Base-side resolver outside the statically admitted `L2Resolver` (or lacking supported profile admission) does not satisfy declared record reads. The L1 resolver and offchain gateways are separate surfaces.
> **Since**: `2026-04-21`

> **Basenames L2Resolver-compatible profile narrowing** — bigname admits Basenames registry-observed Base resolver addresses as watched contract instances, but it supports resolver-local fact consumption only for discovered instances explicitly admitted as `L2Resolver`-compatible.
> **Upstream**: `(upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)`
> **Our rule**: `docs/manifests.md` § Basenames source-family ownership and § capability policy; mirrored in `docs/architecture.md`, `docs/storage.md`, `docs/projections.md`, `docs/api-v1.md` (resolution and resolver routes), and `docs/consumer-capabilities.md`.
> **Why**: registry-observed Base resolvers are watched contract instances, but resolver-local fact consumption requires `L2Resolver`-compatible profile admission. The gate is separate from ENSv1 PublicResolver-generation admission and from Basenames L1 transport / execution.
> **Since**: `2026-04-22`

> **ENSv1 old-registry admission narrowing** — bigname may admit `ENSRegistryOld` as migration-aware `ens_v1_registry_l1` input, but it does not treat the current registry `startBlock: 9380380` as original ENS history and does not union old and current registry logs by latest block.
> **Upstream**: `(upstream: .refs/ens_subgraph/subgraph.yaml:L10 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L42 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L44 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L134 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L230 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L252 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L259 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L29 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L40 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L153 @ ens_v1@91c966f)`
> **Our rule**: `docs/manifests.md` § Required Fields and § Capability Policy; mirrored in `docs/architecture.md` § Source Families / Source Manifests And Capability Registry, `docs/chain-intake.md` § ENSv1 And Basenames Resolver Discovery Boundary, `docs/storage.md` § ID Strategy and § Table Families And Write Ownership, and `docs/consumer-capabilities.md` § Current Status.
> **Why**: preserve current-registry topology after a node migrates, keep the root resolver as the explicit old-registry exception, and prevent historical backfill or old-registry admission from graduating coverage or consumer replacement without route-level evidence.
> **Since**: `2026-04-24`

> **ENSv2 sepolia-dev source-family narrowing** — bigname admits only `ens_v2_root_l1`, `ens_v2_registry_l1`, `ens_v2_registrar_l1`, and `ens_v2_resolver_l1` for the `sepolia-dev` profile, not every upstream `sepolia-dev` deployment artifact.
> **Upstream**: `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/PermissionedResolverImpl.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/UniversalResolverV2.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ReverseRegistry.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/DNSAliasResolver.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/WrapperRegistryImpl.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/LockedMigrationController.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/HCAFactory.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/StandardRentPriceOracle.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/BatchRegistrar.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/MockUSDC.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/MockDAI.json:L2 @ ens_v2@554c309)`
> **Our rule**: `docs/manifests.md` § ENSv2 sepolia-dev source-family ownership; mirrored in `docs/architecture.md` § source families and `docs/chain-intake.md` § ENSv2 adapter intake.
> **Why**: scope the dev-profile to root, registry, registrar, and resolver resource/event semantics. Reverse, DNS, wrapper, migration, universal-resolver/execution, factory, oracle, batch, and mock-payment surfaces are admitted separately when needed.
> **Since**: `2026-04-20`

> **Automatic bootstrap start-block narrowing** — bigname treats manifest `start_block` as optional inclusive bootstrap metadata for `[[roots]]` and `[[contracts]]`, not as an inferred deployment truth. ENSv1 registry and `.eth` registrar values are reference candidates from `ens_subgraph` only; ENSv1 NameWrapper, PublicResolver, ReverseRegistrar, and ENSv2 `sepolia-dev` RootRegistry / ETHRegistry / ETHRegistrar values come from pinned deployment receipt metadata. Basenames mainnet source families and ENS UniversalResolver remain unknown, so automatic bootstrap skips those targets instead of defaulting to block zero or job-range start.
> **Upstream**: `(upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L122 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L1498 @ ens_v1@91c966f)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L200 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L1104 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L379 @ ens_v1@91c966f)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2617 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2617 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L1922 @ ens_v2@554c309)`
> **Our rule**: `docs/manifests.md` § required fields and § watch-plan expansion; mirrored in `docs/chain-intake.md` § automatic bootstrap and `docs/storage.md`.
> **Why**: keep automatic historical bootstrap from silently widening unknown source history, address-only target identity, or chain checkpoint state.
> **Since**: `2026-04-22`

Per-entry format:

> **Surface** — one-line description of what differs.
> **Upstream**: `(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)`
> **Our rule**: `docs/<file>.md` § section.
> **Why**: the constraint that drove the divergence (consumer capability, storage invariant, coverage narrowing, etc.).
> **Since**: commit or date the divergence was introduced.

## Evidence checks

Use `$upstream-evidence` or `evidence_reader` when a change adds or relies on ENSv1, ENSv2, Basenames, admitted app-metadata, reference-indexer, or execution-client behavior claims. The check produces a claim-to-citation ledger and flags any divergence that belongs in this file.

Pin drift checks are deliberate, not scheduled automation. Run them when manifests, ADRs, or load-bearing citations change. Stale pins are not urgent by default — material upstream behavior change is the trigger, not calendar time.
