# Upstream references

bigname anchors every ENSv1, ENSv2, and Basenames claim to a specific upstream commit pinned under `.refs/`. This doc is the human-readable companion to `.refs/MANIFEST.toml` — the pin table, rotation policy, and the intentional-divergence list.

## Pinned refs

| Key | Repo | Commit | Purpose |
|-----|------|--------|---------|
| `ens_v1` | `ensdomains/ens-contracts` | `91c966fe` | Canonical ENSv1 Solidity |
| `ens_v2` | `ensdomains/contracts-v2` | `554c309b` | ENSv2 contracts |
| `basenames` | `base-org/basenames` | `1809bbc9` | Canonical Basenames Solidity |
| `ens_subgraph` | `ensdomains/ens-subgraph` | `723f1b6a` | Reference ENSv1 indexer |
| `ensnode` | `namehash/ensnode` | `2017ae62` | Alternative ENS indexer |

Full pin records (including per-ref `authoritative_for` lists) live in `.refs/MANIFEST.toml`. Sync with `scripts/sync-refs`.

## Citation format

```
(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)
```

Use this exact shape everywhere — docs, ADRs, manifests, code comments, task writeups, agent output. Consistent format lets `verification_reviewer` and `upstream_auditor` verify citations mechanically.

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
- **Who decides**: whoever owns the surface affected. Ambiguous cases route through `$change-gate`; cross-surface bumps route through `verification_reviewer` after the sync.

## Known divergences

Intentional differences between our docs/manifests and upstream. Every divergence lives here so that citations reading "differently than upstream" are legible instead of looking like bugs. If a divergence is not in this list, it should be treated as drift and closed — either by updating our doc or by adding the entry.

> **Basenames verified/explain public support narrowing** — bigname narrows the upstream Basenames L1Resolver and CCIP entrypoint into one first public support class instead of publishing every upstream-reachable non-`base.eth` path immediately.
> **Upstream**: `(upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)` `(upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)` `(upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc)`
> **Our rule**: `docs/api-v1.md` § `GET /v1/resolutions/{namespace}/{name}` and § `GET /v1/explain/resolutions/{namespace}/{name}/execution`; mirrored in `docs/execution.md` § Initial Support Boundary and `docs/manifests.md` § Basenames source-family ownership.
> **Why**: freeze the first Basenames consumer-replacement slice on the declared Base-authority plus L1-transport boundary before widening alias-participating, wildcard-derived, linked-subregistry, transport-free, or offchain-gateway path classes.
> **Since**: `2026-04-19`

> **ENSv1 Phase 4 wrapper/resolver admission narrowing** — bigname admits the mainnet NameWrapper and PublicResolver as source-family inputs for current declared-state normalization without treating every upstream wrapper or resolver capability as shipped public support.
> **Upstream**: `(upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L479 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L500 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L5 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L13 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f)`
> **Our rule**: `docs/manifests.md` § Required Fields / capability ownership narrative and § Capability Policy; mirrored in `docs/architecture.md` § Source Families and § Coverage And Exhaustiveness Rules, `docs/storage.md` § ID Strategy and § Table Families And Write Ownership, and `docs/development-plan.md` § Phase 4: ENSv1 Adapter Slice.
> **Why**: freeze the first ENSv1 wrapper/resolver adapter boundary on source-family ownership, current identity continuity, and declared resolver record state before separately admitting wrapper upgrade / migration history, public route coverage graduation, or consumer-replacement semantics.
> **Since**: `2026-04-21`

> **ENSv2 sepolia-dev Phase 5 admission narrowing** — bigname admits only the first four ENSv2 source families for Phase 5 instead of treating every upstream `sepolia-dev` deployment artifact as an active source family.
> **Upstream**: `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/PermissionedResolverImpl.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/UniversalResolverV2.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ReverseRegistry.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/DNSAliasResolver.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/WrapperRegistryImpl.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/LockedMigrationController.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/HCAFactory.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/StandardRentPriceOracle.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/BatchRegistrar.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/MockUSDC.json:L2 @ ens_v2@554c309)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/MockDAI.json:L2 @ ens_v2@554c309)`
> **Our rule**: `docs/manifests.md` § ENSv2 Phase 5 source-family ownership; mirrored in `docs/architecture.md` § Source Families and `docs/chain-intake.md` § ENSv2 Phase 5 Adapter Intake Boundary.
> **Why**: freeze the first ENSv2 development-profile slice on root, registry, registrar, and resolver resource/event semantics before admitting reverse, DNS, wrapper, migration, universal-resolver/execution, factory, oracle, batch, or mock-payment surfaces.
> **Since**: `2026-04-20`

Per-entry format:

> **Surface** — one-line description of what differs.
> **Upstream**: `(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)`
> **Our rule**: `docs/<file>.md` § section.
> **Why**: the constraint that drove the divergence (consumer capability, storage invariant, coverage narrowing, etc.).
> **Since**: commit or date the divergence was introduced.

## Audit loop

`upstream_auditor` (read-only codex agent, `.codex/agents/upstream-auditor.toml`) diffs each pinned commit against its upstream `main`, identifies cited files that changed, and reports pins plausibly worth bumping. It does not bump; bumping stays manual per the rotation policy above.

Run opportunistically when manifests/ADRs change, or on a weekly `$schedule`. Stale pins are not urgent by default — material upstream behavior change is the trigger, not calendar time.
