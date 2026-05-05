# Upstream references

bigname's behaviour mirrors, narrows, or reshapes ENSv1, ENSv2, and Basenames. Anything we say about how those systems behave has to be checked against pinned source. This file is the index — pin table, citation format, rotation rules, and the list of places where we deliberately disagree with upstream.

`.refs/MANIFEST.toml` is the machine-readable companion. `scripts/sync-refs` materialises the checkouts; `scripts/sync-refs --check` verifies them.

## Pinned commits

| Key | Repo | Commit | Why we pin it |
|---|---|---|---|
| `ens_v1` | `ensdomains/ens-contracts` | `91c966fe` | Canonical ENSv1 Solidity (registry, registrar, resolver, NameWrapper, ReverseRegistrar, UniversalResolver). |
| `ens_v2` | `ensdomains/contracts-v2` | `554c309b` | ENSv2 contracts. Used for the `sepolia-dev` profile. |
| `basenames` | `base-org/basenames` | `1809bbc9` | Basenames Solidity on Base, plus the L1 compatibility resolver. |
| `ens_subgraph` | `ensdomains/ens-subgraph` | `723f1b6a` | Reference ENSv1 indexer. Cross-check only — never the source of truth on its own. |
| `ensnode` | `namehash/ensnode` | `2017ae62` | Alternative ENS indexer. Cross-check only. |
| `ens_app_v3` | `ensdomains/ens-app-v3` | `71758582` | Source of the first-party "known PublicResolver generation" address list. |

Per-ref `authoritative_for` notes live in `.refs/MANIFEST.toml`.

## Citation format

```
(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)
```

This format is enforced everywhere — docs, manifests, code comments, task writeups, agent output. The shape is mechanical so reviewers and the `upstream_auditor` can grep for it.

A claim about ENSv1, ENSv2, or Basenames behaviour without a `.refs/` citation should be rejected in review.

To keep prose readable, docs use Markdown footnotes (`[^v1-pres-l20]`) instead of inlining the full citation in the middle of a sentence. The footnote definition still uses the citation format above.

## Rotation policy

Bumping a pin is a deliberate change, not a routine bump.

- **Bump when** a cited file's behaviour changes, or we want to adopt new upstream behaviour (new contract, new event, new invariant). Falling behind upstream is fine; being silently wrong is not.
- **Don't bump for** drive-by refactors, comment edits, test-only changes, rename-only commits.
- **How:**
  1. Update `commit` in `.refs/MANIFEST.toml` and the table above.
  2. Run `scripts/sync-refs`.
  3. Re-grep for citations against the old short-commit; update any whose source content changed.
  4. Add or edit divergence entries below if the bump introduced or resolved one.
  5. Commit with a message naming the upstream change that motivated the bump.

Cross-surface bumps go through the verification reviewer after the sync.

## Known divergences

Every place where bigname intentionally differs from upstream lives here. If a divergence isn't listed, treat it as drift and close it — either update our docs to match upstream or add an entry.

Each entry uses the same shape: a one-line summary, the upstream anchors, where our rule lives, why we made the call, and the date.

### ENS Universal Resolver: route-facing proxy vs pinned implementation

The route-facing entrypoint for `ens_execution` is the official ENS Universal Resolver proxy at `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`. The pinned ENSv1 deployment artifact records the implementation address `0xED73a03F19e8D849E44a39252d222c6ad5217E1e`; that's the ABI/behaviour anchor, not the entrypoint.

- Upstream proxy: <https://docs.ens.domains/resolvers/universal/>, <https://docs.ens.domains/learn/deployments/>
- Implementation: `(upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)` and `(upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f)`
- Our rule: `manifests/ens/ens_execution/`, `docs/manifests.md`, `docs/execution.md`
- Why: callers and manifests should target the proxy. The pinned implementation stays the source of truth for behaviour citations.
- Since: 2026-04-22

### Basenames verified resolution: one support class, not the full upstream surface

Basenames upstream supports several routing paths through the L1 Resolver and CCIP-Read. We publish exactly one verified support class — exact-surface, transport-assisted, direct-path — and explicitly mark the rest unsupported.

- Upstream: `(upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)`, `(upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)`, `(upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc)`, `(upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)`, `(upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc)`
- Our rule: `docs/api-v1-routes.md` (`/v1/resolutions` and `/v1/explain/.../execution`), `docs/execution.md`, `docs/manifests.md`
- Why: freeze a small consumer-replacement slice before widening alias-participating, wildcard-derived, linked-subregistry, transport-free, or offchain-gateway classes.
- Since: 2026-04-19

### ENSv1 NameWrapper and PublicResolver: input only, no automatic capability claims

The Mainnet NameWrapper and PublicResolver are admitted as `ens_v1_wrapper_l1` and `ens_v1_resolver_l1` source families. Admission is for current-state normalization. Wrapper migration history, full upstream resolver capabilities, and other generations get explicit follow-on work — they are not implied.

- Upstream: NameWrapper deployment `(upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f)` and interface `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)`–L38; PublicResolver deployment `(upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f)` and interface mixins `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L5 @ ens_v1@91c966f)`–L114.
- Our rule: `docs/manifests.md` § ENS mainnet, mirrored in `docs/architecture.md`, `docs/storage.md`, `docs/projections.md`.
- Why: bind the adapter boundary to source-family ownership. Wrapper-upgrade and migration history come back as explicit, separate work.
- Since: 2026-04-21

### ENSv1 resolver-profile admission: per-generation, not "latest only"

Registry-observed resolver addresses are watched contract instances. They are not automatically supported PublicResolver-generation profiles. Generic resolver-local events still produce observed selector and cache facts; complete family coverage, latest-only behaviour, resolver-overview support, and event-to-call parity require explicit profile admission. The first dynamic admission is the ENS Labs PublicResolver-generation set; the mainnet manifest seeds the latest plus first-party app-known generations.

- Upstream resolver interfaces: `(upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)`, `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)`, profile interfaces under `.refs/ens_v1/contracts/resolvers/profiles/`, `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)`–L150, `(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f)`–L23.
- App known-resolver list: `(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L32 @ ens_app_v3@7175858)`.
- Our rule: `docs/manifests.md` § ENS mainnet, mirrored in `docs/storage.md`, `docs/projections.md`, `docs/api-v1.md` (resolution and resolver routes), `docs/consumer-capabilities.md`.
- Why: a registry-observed resolver is not the same as an admitted profile. Pubkey, DataResolver, and unknown profile state stay explicit (`pending`/`unsupported`).
- Since: 2026-04-21

### Basenames Base-side resolver discovery: deferred

The shipped Base-side `L2Resolver` is a static seed. Resolver discovery from Basenames registry `NewResolver` observations is a follow-on. Until it ships, declared record reads off any non-seed Base resolver stay topology-only.

- Upstream: `(upstream: .refs/basenames/src/L2/Registry.sol:L19 @ basenames@1809bbc)`, `(upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)`, `(upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc)`.
- Our rule: `docs/manifests.md` § Basenames, `docs/chain-intake.md` § resolver discovery.
- Since: 2026-04-21

### Basenames `L2Resolver`-compatible profile gate

Registry-observed Base resolvers are watched, but resolver-local fact consumption requires explicit admission as `L2Resolver`-compatible. The gate is independent of ENSv1 PublicResolver admission, the L1 transport path, and offchain gateways.

- Upstream: `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc)`–L225, plus the registry references above.
- Our rule: `docs/manifests.md` § Basenames, mirrored in `docs/architecture.md`, `docs/storage.md`, `docs/projections.md`, `docs/api-v1.md`, `docs/consumer-capabilities.md`.
- Since: 2026-04-22

### `ENSRegistryOld`: migration-aware input, not a parallel current registry

`ENSRegistryOld` is admitted under `ens_v1_registry_l1` as a migration-epoch input. A current-registry `NewOwner` marks the node migrated; later old-registry `NewOwner`/`Transfer`/`NewTTL`/non-root `NewResolver` observations for that node stay as raw facts but don't update topology. The single exception is the root resolver, where old-registry `NewResolver(ROOT_NODE, _)` still applies.

The current registry's pinned `start_block` of `9380380` is the current epoch's start, not original ENS history.

- Upstream: subgraph manifest `(upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)`, L39, L44; subgraph handlers `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L134 @ ens_subgraph@723f1b6)`, L230, L238, L246; registry-with-fallback `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L40 @ ens_v1@91c966f)`.
- Our rule: `docs/manifests.md` § ENS mainnet, mirrored in `docs/architecture.md`, `docs/chain-intake.md`, `docs/storage.md`, `docs/consumer-capabilities.md`.
- Since: 2026-04-24

### ENSv2 `sepolia-dev`: four families admitted, not the whole deployment set

The `sepolia-dev` profile admits exactly `ens_v2_root_l1`, `ens_v2_registry_l1`, `ens_v2_registrar_l1`, and `ens_v2_resolver_l1`. Other deployment artifacts (`UniversalResolverV2`, `ReverseRegistry`, `DNSAliasResolver`, `WrapperRegistryImpl`, `LockedMigrationController`, `HCAFactory`, `StandardRentPriceOracle`, `BatchRegistrar`, `MockUSDC`, `MockDAI`) stay outside admission until a doc-first update.

- Upstream: deployment artifacts under `.refs/ens_v2/contracts/deployments/sepolia-dev/`.
- Our rule: `docs/manifests.md` § ENSv2, mirrored in `docs/architecture.md`, `docs/chain-intake.md`.
- Since: 2026-04-20

### Bootstrap `start_block`: optional, not inferred

Manifest `start_block` is optional inclusive bootstrap metadata. ENSv1 registry/registrar/wrapper/resolver/reverse-registrar starts come from the pinned subgraph or deployment receipts; ENSv2 `sepolia-dev` starts come from pinned receipts. Basenames mainnet families and the ENS Universal Resolver have no pinned start, so automatic bootstrap skips them rather than falling back to block zero, the manifest activation height, or the current job range start.

- Upstream: subgraph `(upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)` and L122; deployment receipts under `.refs/ens_v1/deployments/mainnet/` and `.refs/ens_v2/contracts/deployments/sepolia-dev/`.
- Our rule: `docs/manifests.md` § Bootstrap `start_block`, mirrored in `docs/chain-intake.md` § automatic bootstrap and `docs/storage.md`.
- Since: 2026-04-22

## Audit loop

`upstream_auditor` (`.codex/agents/upstream-auditor.toml`) reads pinned commits against upstream `main`, identifies cited files that have changed, and reports the citations plausibly worth attention. It does not bump pins; bumping stays manual per the rotation policy above.

Run it opportunistically when manifests or this file change, or on a periodic schedule. Calendar age alone isn't a reason to bump — material upstream behaviour change is.
