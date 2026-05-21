# Architecture

bigname is a versioned, replayable indexing and read platform for ENS (v1 and v2) and Basenames. It exposes a native `v1` REST contract — not a legacy-subgraph parity layer — that answers point-in-time, provenance-tagged questions about names, addresses, resolvers, primary names, and verified resolution.

This document defines the model. Wire format lives in [`api-v1.md`](api-v1.md); persistence in [`storage.md`](storage.md); manifests, intake, projections, and execution in their own files. Implementation sequencing and parallel-work boundaries live under [`internal/`](internal/).

## Objectives

For any supported name or address, every answer must be:

- point-in-time
- replayable
- auditable
- explicit about provenance, coverage, finality, and consistency
- safe under chain reorgs and source-graph expansion

Every answer carries `declared_state`, `verified_state` (where applicable), `provenance`, `coverage`, `chain_positions`, `consistency`, and `last_updated`.

## Public namespaces

The public namespaces are exactly `ens` and `basenames`.

- `ens` is a single product that absorbs both ENSv1 and ENSv2 as internal authority epochs.
- `basenames` is separate, covering Basenames-issued `*.base.eth` names on Base.[^bn-readme-l70]
- `base.eth` itself stays under `ens` because upstream treats it as the L1 root domain handled by the Mainnet `L1Resolver`.[^bn-l1resolver-l13][^bn-l1resolver-l154]

Namespace assignment is driven by an internal `NamespaceRegistry` with versioned rules: highest-priority `exact_name`, then `suffix`, then `authority_root`. Initial policy:

- exact `base.eth` → `ens`
- suffix `*.base.eth` → `basenames`
- other supported ENS surfaces → `ens`

Conflicts reject canonical admission; namespace assignment happens before `logical_name_id` is minted. Deployment profile is separate from namespace: profiles select the admitted chain set (mainnet, sepolia-dev), not a different namespace product. One runtime answers under one profile at a time.

## Public read contract

`v1` resource families: `Namespace`, `Name`, `Address`, `Resolver`, `Resolution`, `PrimaryName`, `Permissions`, `History`, `Explain`, `SourceManifest`, `Coverage`. `Registration` is a sub-document of `Name`.

Routes accept some combination of: `namespace`, `name`, `address`, `coin_type`, `at`, `chain_positions`, `consistency=head|safe|finalized`, `mode=declared|verified|both`, `include`, and pagination. `at` selects a timestamp; `chain_positions` pins per-chain `(block_number, block_hash, timestamp)`. The two are mutually exclusive — supplying both rejects with `invalid_input`.

Coverage statuses: `full`, `partial`, `observed_only`, `unsupported`, `stale`. Exhaustiveness: `authoritative`, `best_effort`, `observed_only`, `non_enumerable`, `not_applicable`.

Per-result `ResultStatus` for mixed routes: `success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, `execution_failed`. `unsupported_reason` is required when status is `unsupported`; only `success` guarantees a concrete value.

Breaking semantic changes mean `v2`. The `v1` contract does not preserve ENSv1 subgraph entity names, ENSNode shapes, or GraphQL field-level parity.

## Identity model

Four identity layers, each with its own continuity rules:

### `logical_name_id`

Stable identity for a public name surface within a namespace, written as `<namespace>:<normalized_name>` (e.g. `ens:wallet.linked.parent.eth`, `basenames:alice.base.eth`). It survives backing-resource rotation, token regeneration, lapses, and re-registrations.

### `resource_id`

Stable identity for the backing authority object — the anchor for permission lineage, control lineage, token lineage, and resolver-scoped permissions. Opaque UUID.

- For ENSv2, `resource_id` maps to the upstream permissioned-registry EAC resource, not the current ERC-1155 token ID. The registry exposes `getResource(anyId)` and `getTokenId(anyId)`, emits `TokenResource(tokenId, resource)` when a label is linked, and emits `TokenRegenerated(oldTokenId, newTokenId)` when role changes burn and mint a replacement token while leaving the resource unchanged.[^v2-iperm-l34][^v2-iperm-l67][^v2-iperm-l72][^v2-events-l69][^v2-pr-l451]
- For ENSv1, `resource_id` is the stable identity for the authority object: registry-only control, registrar-backed registration, or wrapper-backed control. The same `resource_id` persists across holder, controller, resolver, expiry, grace, fuse, and status changes; it rotates when authority moves to a different anchor (registry-only ↔ registrar ↔ wrapper, or full lapse + re-registration). If the prior anchor becomes authoritative again (e.g. unwrap back to the same lease), reuse the prior `resource_id`.
- For Basenames, `resource_id` anchors the Base-side authority object even when L1 compatibility transport is involved.[^bn-readme-l69][^bn-readme-l70][^bn-l1resolver-l13]

### `token_lineage_id`

Stable identity for tokenized ownership history. Token IDs can change while the resource is unchanged; the lineage outlives the ID.

- ENSv1: registry-only control has none. A registrar lease or wrapper position mints one. Renewal, transfer, expiry, and grace within the same anchor preserve it. Authority moving to a different tokenized anchor rotates it; returning to the prior tokenized anchor reactivates the prior lineage.
- ENSv2: preserved across `TokenRegenerated`. Update the current token ID attribute and append the normalized event. Resource identity is anchored by upstream `eacVersionId`; tokens are versioned by `tokenVersionId`. Unregister/re-register increments both; regeneration increments only the token version.[^v2-pr-l28][^v2-pr-l203][^v2-pr-l237][^v2-pr-l451][^v2-pr-l461][^v2-pr-l536][^v2-pr-l545]

### `contract_instance_id`

Stable identity for registry, registrar, resolver, wrapper, or transport instances. Minted when a manifest-declared or discovery-admitted contract is first added to the canonical source graph. One admitted address on one chain maps to one `contract_instance_id` across all manifest and discovery epochs; re-admission after an inactive gap reuses it with a new active range. A proxy keeps its identity when implementation changes; only a different watched contract address rotates it.

## Name surface model

Two layers separate public names from backing authority:

`NameSurface` is the canonical row per `logical_name_id`. It stores admitted surface identity and one canonical normalization result: `input_name`, `canonical_display_name`, `normalized_name`, `dns_encoded_name`, `namehash`, `labelhashes`, `normalizer_version`, plus normalization warnings/errors. Per-observation spellings live in immutable preimage observation facts and normalized events, not in additional `NameSurface` rows.

`SurfaceBinding` records how a public surface binds to a backing resource through time:

- `surface_binding_id`, `logical_name_id`, `resource_id`, `binding_kind`, `active_from`, `active_to`, provenance, canonicality state.

Binding kinds: `declared_registry_path`, `linked_subregistry_path`, `resolver_alias_path`, `observed_wildcard_path`, `migration_rebind`, `observed_only`.

ENSv1 authority moves (wrap, unwrap, re-registration) carry the identity change in `resource_id` and `token_lineage_id`; ordinary lifecycle stays `declared_registry_path`. A new `SurfaceBinding` row appears only when the bound `resource_id` changes — transfer and expiry within the same anchor do not.

| Case | Anchor | `resource_id` | `token_lineage_id` |
| --- | --- | --- | --- |
| Registry-only sub.alice.eth | direct registry | one registry-anchored | none |
| Register alice.eth | registrar lease | one registrar-anchored | mint registrar lineage |
| Wrap alice.eth | wrapper-backed | close registrar binding, open wrapper-anchored | mint wrapper lineage |
| Unwrap before lease ends | same registrar lease | reactivate prior registrar | reactivate prior registrar lineage |
| Expiry / grace | unchanged anchor | unchanged | unchanged |
| Re-registration after lapse | new registrar lease | mint new | mint new |

This separation captures: one resource under multiple public names, alias-resolved names without direct registry entries, observed wildcard names, and surfaces that rebind across time.

## Normalization and preimage observation

Normalization is version-pinned via `normalizer_version`. The canonical `NameSurface` carries one representative result; alternate spellings persist as immutable preimage observation facts.

`PreimageObserved` facts may come from registrar/registry events with explicit labels, wrapper events with human-readable names, reverse/primary flows that reveal names, and metadata when a manifest allows. Invalid input is never silently coerced into a valid identity.

For ENSv1, resolver `NameChanged(node, name)` strings observed through admitted reverse/primary flows are preimage observations only.[^v1-namechanged-l10][^v1-namechanged-l18][^v1-revreg-l129][^v1-revreg-l130] They can attach already-observed forward-node facts to a human-readable name; they do not synthesize ownership, resolver, or record facts.

For ENSv2, admitted registry, registrar, and resolver name-bearing events produce preimage observations: registry `LabelRegistered`, `LabelReserved`, `ParentUpdated`; registrar `NameRegistered`, `NameRenewed`; resolver `AliasChanged`, `NamedResource`, `NamedTextResource`, `NamedAddrResource`.[^v2-events-l15][^v2-events-l30][^v2-events-l75][^v2-iethreg-l32][^v2-iethreg-l53][^v2-iperm-resolver-l14][^v2-pres-l132][^v2-pres-l142][^v2-pres-l153] These do not write projections or mutate manifest capability state.

## Canonicality, authority, and epochs

- For `ens`, authoritative registration and control come from Ethereum L1. `authority_epoch` is `ens_v1` or `ens_v2` per name and time; it is separate from `resolution_epoch`.
- For `basenames`, authoritative registration and control live on Base.[^bn-readme-l70] The Basenames L1 path is compatibility transport, not a competing authority source.[^bn-readme-l69][^bn-l1resolver-l13]
- Primary names are canonical only when verification succeeds for the requested `coin_type`. Reverse claims alone are insufficient; verification must resolve the claimed name back to the requested address.[^v1-aur-l217][^v1-aur-l226][^v1-aur-l263][^v1-aur-l269]

## Source families

ENS:

- `ens_v1_registry_l1`
- `ens_v1_registrar_l1`
- `ens_v1_wrapper_l1`
- `ens_v1_resolver_l1`
- `ens_v1_reverse_l1`
- `ens_dns_l1`
- `ens_offchain_metadata`
- `ens_v2_root_l1`
- `ens_v2_registry_l1`
- `ens_v2_registrar_l1`
- `ens_v2_resolver_l1`
- `ens_execution`

Basenames:

- `basenames_base_registry`
- `basenames_base_registrar`
- `basenames_base_resolver`
- `basenames_base_primary`
- `basenames_l1_compat`
- `basenames_execution`
- `basenames_offchain` (reserved; not currently admitted)

Shared: `shared_manifests`, `shared_normalization_rules`, `shared_capability_registry`.

Family ownership is fixed:

- `ens_v1_wrapper_l1` owns Mainnet NameWrapper authority, holder facts, fuse/expiry, wrapper-revealed names, and wrapper-originated resolver/TTL changes.[^v1-namewrapper-deploy][^v1-iname-l27][^v1-iname-l35][^v1-iname-l37][^v1-iname-l38][^v1-nw-l240][^v1-nw-l377][^v1-nw-l637][^v1-nw-l666][^v1-nw-l676]
- `ens_v1_resolver_l1` owns the Mainnet PublicResolver and admitted ENS Labs PublicResolver-generation profiles. Generation admission is the gate for complete family coverage, resolver-overview support, latest-only behavior, authorization semantics, and event-to-call parity. Unadmitted resolvers stay `pending` or `unsupported`.[^v1-publicresolver-deploy][^v1-pres-l5][^v1-pres-l13][^v1-pres-l20][^v1-pres-l66][^v1-pres-l114]
- ENS verified resolution belongs to `ens_execution` at the official Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`,[^ens-docs-univ] not to `ens_v1_registry_l1`. The pinned implementation artifact is recorded under `.refs/`.[^v1-ur-deploy][^v1-ursol-l8] (See [`upstream.md`](upstream.md) for the proxy-vs-implementation divergence.)
- ENS reverse-claim intake belongs to `ens_v1_reverse_l1` at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`.[^v1-revreg-deploy][^v1-revreg-l15][^v1-revreg-l19]
- ENSv1 `.eth` registrar label intake belongs to `ens_v1_registrar_l1`. BaseRegistrar is the tokenized authority; legacy, wrapped, and current registrar-controller contracts are admitted within the same family for label-bearing registration and renewal observations only.[^subgraph-l145][^subgraph-l170][^subgraph-l226][^v1-ethrc-l116][^v1-ethrc-l133]
- ENSv1 dynamic resolver discovery is required for declared record reads. Canonical nonzero `NewResolver(node, resolver)` from admitted registry emitters becomes a node-to-resolver binding update and a `ens_v1_resolver_l1` contract instance; zero-address closures release only the affected binding.[^v1-ens-l12][^v1-ensreg-l89][^v1-ensreg-l174] Generic resolver-local events (`AddrChanged`, `AddressChanged`, `TextChanged`, `VersionChanged`) feed observed selector/cache state while profile state stays pending.
- `ENSRegistryOld` is admitted as migration-aware input under `ens_v1_registry_l1`. Old- and current-registry logs are not unioned by latest block: a current-registry `NewOwner` marks a node migrated; later old-registry updates for that node are suppressed except for the root resolver.[^subgraph-l15][^subgraph-l39][^subgraph-l44][^subgraph-ts-l134][^subgraph-ts-l230][^subgraph-ts-l238][^subgraph-ts-l246]
- ENSv2 `sepolia-dev` admits four families: `ens_v2_root_l1` (`RootRegistry`), `ens_v2_registry_l1` (`ETHRegistry` plus discovered `UserRegistry`), `ens_v2_registrar_l1` (`ETHRegistrar`), `ens_v2_resolver_l1` (`PermissionedResolverImpl`).[^v2-deploy-root][^v2-deploy-ethreg][^v2-deploy-ethrc][^v2-deploy-pres][^v2-userreg-l15][^v2-ethrc-l30][^v2-ethrc-l173] Other `sepolia-dev` artifacts (`UniversalResolverV2`, `ReverseRegistry`, `DNSAliasResolver`, `WrapperRegistryImpl`, `LockedMigrationController`, `HCAFactory`, `StandardRentPriceOracle`, `MockUSDC`, `MockDAI`, `BatchRegistrar`) remain outside admission until a doc-first update.
- ENSv2 exact-name profile support is only promoted in the `sepolia-dev` profile when `ens_v2_registrar_l1` declares `exact_name_profile = "supported"`. Other profiles or capability states stay unsupported or shadow.
- Basenames mainnet authority splits across `basenames_base_registry` (`registry` at `0xb94704422c2a1e396835a571837aa5ae53285a95`), `basenames_base_registrar` (`registrar` at `0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a`), and `basenames_base_resolver` (`resolver` at `0xC6d566A56A1aFf6508b41f6c90ff131615583BCD`).[^bn-readme-l28][^bn-readme-l29][^bn-readme-l34][^bn-registry-l10][^bn-baseregistrar-l15][^bn-l2resolver-l22] `basenames_base_primary` (`reverse_registrar` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282`) is claim-intake only.[^bn-readme-l33][^bn-revreg-l12][^bn-revreg-l150] `basenames_l1_compat` and `basenames_execution` both reference the L1 Resolver at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` for transport and execution respectively.[^bn-readme-l22][^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]
- Basenames dynamic Base resolver discovery treats canonical nonzero `NewResolver` from admitted registry emitters as binding updates and `basenames_base_resolver` instances; resolver-local fact consumption requires `L2Resolver`-compatible profile admission.[^bn-registry-l19][^bn-registry-l132][^bn-registry-l223][^bn-l2resolver-l4][^bn-l2resolver-l16][^bn-l2resolver-l29][^bn-l2resolver-l182][^bn-l2resolver-l193][^bn-l2resolver-l209][^bn-l2resolver-l225]

## Source manifests

Manifests pin each source family by version and live under a selected profile root at `manifests/<profile>/<chain_combo>/<namespace>/<source_family>/<version>.toml`. The shipped runtime default is `manifests/mainnet/`; the Sepolia profile root is `manifests/sepolia/`. One runtime selects exactly one profile root.

Each manifest contains: `manifest_version`, `namespace`, `source_family`, `chain`, `deployment_epoch`, `rollout_status` (`draft` | `shadow` | `active` | `deprecated`), `normalizer_version`, `capability_flags` (`unsupported` | `shadow` | `supported`), `roots`, `contracts`, `discovery_rules`. `start_block` is optional inclusive bootstrap metadata; omitted means unknown — adapters preserve that state rather than inferring zero.

Manifest changes are first-class normalized events: `SourceManifestUpdated`, `ProxyImplementationChanged`, `CapabilityChanged`.

Rules:

- A discovered contract is authoritative only if it is reachable from a canonical root or explicitly admitted by a manifest.
- Re-declaring the same address mints no new instance — it appends a new active range.
- Declared proxy implementations resolve to separate `contract_instance_id` nodes; implementation changes update the proxy/implementation edge, not the proxy identity.
- Capability ownership attaches to the declaring `source_family` only.
- Draft features may sit behind manifest flags without changing the public contract.

Schema, capability ownership detail, and the discovery edge model are in [`manifests.md`](manifests.md).

## Discovery graph

Discovery expands the canonical graph through time-versioned reachability edges: resolver, subregistry, parent, alias, metadata, proxy/implementation, migration, and transport. Each edge stores `edge_id`, `from_contract_instance_id`, `to_contract_instance_id`, `discovered_by`, `edge_kind`, `active_from`, `active_to`, provenance, and canonicality.

ENSv2 mappings:

- `SubregistryUpdated(tokenId, subregistry, sender)` → normalized `SubregistryChanged`. Non-null endpoints resolve to `contract_instance_id` before storage.[^v2-events-l49][^v2-pr-l131][^v2-pr-l222]
- `ParentUpdated(parent, label, sender)` → `ParentChanged`, updates the parent edge for the emitting registry.[^v2-events-l75][^v2-pr-l151]
- `ResolverUpdated(tokenId, resolver, sender)` → updates the resolver edge for the current registry resource. Admitted resolver endpoints belong to `ens_v2_resolver_l1`.[^v2-events-l59][^v2-pr-l141][^v2-pr-l225]

Watch-plan expansion starts from active root `contract_instance_id`s and traverses active edges by ID. Address-only watch rows derive from instance + active range and remain explainable through the graph.

## Intake architecture

Three intake planes for one selected deployment profile:

- Ethereum L1 chain intake
- Base chain intake
- execution intake (verified reads, CCIP)

Per-profile provider availability: a Base RPC is not required for an Ethereum-only run, and a profile with no Base provider must mark Base intake idle/unavailable rather than failing startup.

Stages per chain:

1. block lineage intake
2. transactions, receipts, logs
3. raw fact persistence + payload-cache metadata
4. manifest/discovery updates
5. adapter routing
6. normalized event persistence
7. projection updates
8. execution-cache invalidation

Postgres is the hot indexed and replay-focused store. Lineage anchors, selected target logs, replay-required call snapshots, code-hash observations, and compact payload-cache metadata are durable. Large block payloads, non-indexed transaction or receipt bodies, and non-audit raw-log staging rows are evictable cache once their replay contract is satisfied. Empty historical blocks retain only lineage anchors and audit metadata.

Backfill enters as bounded persisted jobs with resumable range checkpoints and uses the same stages as live intake. Backfill checkpoint state is operational worker state — it does not promote canonical, safe, or finalized chain checkpoints.

Reconciliation, fetch, notification, and historical-backfill detail live in [`chain-intake.md`](chain-intake.md).

## Immutable facts and rebuildable state

Immutable: blocks, transactions, receipts, logs, contract code hashes, manifests, discovery edges, normalized events, normalization results, preimage observations, selected `eth_call` snapshots, CCIP request/response digests, verification outcomes, metadata responses, sync cursors. For large payloads the durable fact may be selected replay fields plus optional cache metadata or a digest, not the full body — compaction can evict non-critical bytes after replay facts are extracted.

Rebuildable: current name-surface snapshot, surface-binding snapshot, authority/registration snapshot, control snapshot, permissions snapshot, resolver topology, record inventory, record cache, primary-name snapshot, reverse and address indexes, resource-role indexes, resolver indexes, history materializations, coverage snapshots, execution cache, subscriptions/feeds.

Every projected row carries provenance pointers, manifest version, canonicality state, and chain-position context.

## Internal domain model

Core objects: `NameSurface`, `SurfaceBinding`, `BackingResource`, `NameClass`, `RegistrationSnapshot`, `AuthoritySnapshot`, `ControlVector`, `PermissionSnapshot`, `ResolutionTopology`, `RecordInventory`, `RecordCache`, `PrimaryNameSnapshot`, `SourceProvenance`, `CoverageSnapshot`, `TokenLineage`, `ExecutionResult`.

`ControlVector` is never a single owner field. It carries `token_holder`, `registrant`, `effective_controller`, `record_manager`, `delegates`, `reverse_manager`, `resolved_address_target`, `status`, `expiry`, `authority_epoch`, `resolution_epoch`.

`Registration.kind`: `lease`, `subname_assignment`, `reservation`, `dns_control`, `offchain_policy`, `observed_only`.

Permissions and control are anchored to `resource_id`, never to surface text. The chain `logical_name_id → SurfaceBinding → resource_id → token_lineage` must remain reconstructible through time.

## Normalized event taxonomy

Identity, preimage, discovery: `PreimageObserved`, `NameClassified`, `SurfaceBound`, `SurfaceUnbound`, `ContractDiscovered`, `MetadataChanged`, `SourceManifestUpdated`.

Registration and authority: `RegistrationReserved`, `RegistrationGranted`, `RegistrationRenewed`, `RegistrationReleased`, `ExpiryChanged`, `AuthorityTransferred`, `AuthorityEpochChanged`, `MigrationApplied`, `CommitmentMade`, `PricingPolicyChanged`.

Lineage and control: `TokenResourceLinked`, `TokenRegenerated`, `TokenControlTransferred`, `ResolutionEpochChanged`.

Topology and resolution: `ResolverChanged`, `SubregistryChanged`, `ParentChanged`, `AliasChanged`, `WildcardCoverageChanged`, `RecordChanged`, `RecordDeleted`, `RecordVersionChanged`, `RecordInventoryObserved`.

Permissions: `PermissionChanged`, `RootPermissionChanged`, `DelegateRetainedAfterTransfer`, `PermissionScopeChanged`.

Reverse and primary: `ReverseChanged`, `PrimaryNameClaimed`, `PrimaryNameVerified`, `PrimaryNameInvalidated`.

Execution and coverage: `VerifiedResolutionObserved`, `VerifiedResolutionInvalidated`, `CoverageChanged`.

ENSv2 mappings:

- `TokenResourceLinked` ← upstream `TokenResource(tokenId, resource)`. The only adapter event linking current token ID to upstream EAC resource.[^v2-iperm-l34][^v2-pr-l216]
- `TokenRegenerated` ← upstream `TokenRegenerated(oldTokenId, newTokenId)`. Preserves `resource_id`, `token_lineage_id`, and active surface binding.[^v2-events-l69][^v2-pr-l451]
- `SubregistryChanged` ← `SubregistryUpdated`; `ParentChanged` ← `ParentUpdated`.[^v2-events-l49][^v2-events-l75]
- `AliasChanged` ← `PermissionedResolver.AliasChanged`; the alias path stores source and destination DNS-encoded names.[^v2-iperm-resolver-l14][^v2-pres-l230]
- `PermissionChanged` and `RootPermissionChanged` ← upstream `EACRolesChanged(resource, account, oldRoleBitmap, newRoleBitmap)`. Root-resource permissions stay distinguishable because EAC root roles are checked separately and satisfy resource-level checks via root fallback.[^v2-eac-l19][^v2-eac-l176][^v2-eac-l181]

ENSv1 wrapper/resolver mappings: `PreimageObserved`, `SurfaceBound`, `SurfaceUnbound`, `AuthorityTransferred`, `ExpiryChanged`, `TokenControlTransferred`, `ResolverChanged`, `PermissionChanged`, `PermissionScopeChanged`, and `RecordChanged` come from admitted NameWrapper and PublicResolver events.[^v1-iname-l27][^v1-iname-l31][^v1-iname-l35][^v1-iname-l37][^v1-iname-l38][^v1-nw-l1022][^v1-nw-l1034][^v1-pres-l20][^v1-pres-l51][^v1-pres-l58] `PermissionScopeChanged` carries wrapper fuse changes that mask effective powers without inventing new subject grants.

Every normalized event carries: namespace, `logical_name_id` when applicable, `resource_id` when applicable, source family, manifest version, chain position, raw fact reference, derivation kind, canonicality flag, and before/after state where possible.

Normalized events are semantic adapter transitions. A row may be stateless when every payload field is derivable from one selected raw fact, stateful when fields such as `before_state`, resource continuity, authority metadata, resolver state, wrapper state, registrar expiry, and permission provenance depend on the adapter's prior canonical observations, or contextual when identity/resource/discovery fields depend on another adapter-owned output already being stable. Stateful replay is deterministic only from a full closure boundary for that adapter/source graph; contextual replay is deterministic only after dependency closure is stable or inside a topologically ordered closure replay. Source-family slices, target slices, block-hash selections, and IO chunks are not semantic substitutes for those closures.

## Resolution

`Resolution` is one mixed-route envelope with three declared sections and one verified section: `topology`, `record_inventory`, `record_cache`, `verified_queries`.

### `topology`

Fixed declared object:

- `registry_path` — ordered `NameRef` array from the requested surface toward declared registry authority. Never empty when `topology` is supported.
- `subregistry_path` — toward the nearest declared subregistry ancestor. Empty when none participates.
- `resolver_path` — ordered hops; each carries `logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `resource_id`, `chain_id`, `address`, `latest_event_kind`.
- `wildcard` — `{source, matched_labels}`. `null/[]` means wildcard didn't participate.
- `alias` — `{final_target, hops}`. `null/[]` means alias didn't participate.
- `version_boundaries` — `{topology_version_boundary, record_version_boundary}` with `logical_name_id`, `resource_id`, `normalized_event_id`, `event_kind`, `chain_position`.
- `transport` — `{source_chain_id, target_chain_id, contract_address, latest_event_kind}`. All `null` means no transport. For Basenames promotion-target paths, `source=base-mainnet, target=ethereum-mainnet` through the L1 Resolver.[^bn-readme-l22][^bn-readme-l28][^bn-readme-l29][^bn-readme-l34][^bn-readme-l69][^bn-readme-l70]

For ENSv2, `alias` is declared topology only when `PermissionedResolver` provides an `AliasChanged` mapping; the resolver resolves aliases by longest suffix and rewrites calldata before profile dispatch.[^v2-iperm-resolver-l14][^v2-pres-l56][^v2-pres-l412][^v2-pres-l650] Wildcard is observed topology — populated only when execution input identifies an ancestor/source resolver and matched labels.[^v2-pres-l38][^v2-pres-l412]

### `record_inventory`

What record space is known to exist. Carries `record_version_boundary`, `enumeration_basis` (`observed_selectors`, `capability_declared_families`, `globally_enumerable`), `selectors`, `explicit_gaps`, `unsupported_families`, `last_change`.

Selectors carry `record_key`, `record_family`, `selector_key`, `cacheable`. `record_key` is the round-trip string `record_family + ":" + selector_key`; `selector_key` is `null` for scalar families and a string otherwise. Numeric selector domains use string `selector_key` so `record_key` stays text.

Inventory is not global enumeration. It defines the stable selector space admitted by the route, including explicit gaps and unsupported families. Version changes invalidate inventory and cache for the prior boundary.

### `record_cache`

Last-known declared values for supported records. Each entry carries `record_key`, `record_family`, `selector_key`, `status`, `value`, `unsupported_reason`. Status uses `success`, `not_found`, `unsupported`. `value` appears only on `success` and uses the family-native JSON shape. `record_version_boundary` matches `record_inventory`'s and `topology.version_boundaries.record_version_boundary`.

Unsupported records remain requestable through verified execution where possible.

### `verified_queries`

Execution-derived answers per requested record selector, reusing `ResultStatus`. Verified queries do not backfill `record_inventory` or `record_cache` in the same response.

Public verified support is narrower than the topology model. ENS supports:

- exact-surface direct path: `resolver_path[0].logical_name_id == route surface`, `wildcard.source=null`, `alias.final_target=null`, all `transport=null`
- exact-surface alias-only non-direct: same but `alias.final_target` non-null with non-empty `hops`
- exact-surface wildcard-derived: `wildcard.source` non-null with non-empty `matched_labels`, `resolver_path[0].logical_name_id == wildcard.source.logical_name_id`, `alias.final_target=null`, `subregistry_path=[]`, `transport=null`

Other ENS classes (non-alias ancestor-selected, linked-subregistry ancestor-selected, transport-assisted, CCIP-participating) return selector-local `unsupported`.

Basenames supports the exact-surface transport-assisted direct path through active `basenames_execution` v2 at the L1 Resolver. Other Basenames verified path classes return selector-local `unsupported`.[^bn-readme-l69][^bn-readme-l70][^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]

Verified answers persist an `ExecutionTrace`. `ExplainResolution` shows resolver selection, wildcard traversal, alias rewriting, record version boundary, CCIP steps, and the source event or execution result that last changed the answer.

## Permissions

Permissions are first-class projections and explain views. Track grants by scope (root, registry, resource, resolver, record manager/operator, migration-derived, transport-derived). Each grant records source, revocation source, inheritance path, transfer behavior, scope, and effective powers.

Public reads expose `effective_powers` directly so callers don't reconstruct authority from raw role bitmaps. The first declared-state route is resource-centric: `GET /v1/resources/{resource_id}/permissions`. Name-, address-, and resolver-centric views summarize or filter the same resource-anchored truth.

For ENSv1 wrapper-backed resources, `effective_powers` is masked by the active NameWrapper fuse state before publication. `PermissionScopeChanged` carries fuse changes that remove powers without inventing new subject grants.[^v1-iname-l10][^v1-nw-l421][^v1-nw-l427][^v1-nw-l637][^v1-nw-l666][^v1-nw-l676][^v1-nw-l723][^v1-nw-l827][^v1-nw-l1023][^v1-nw-l132]

For ENSv2, `PermissionedRegistry.getResource(anyId)` keys permissions by upstream resource, so public permissions key by the bigname `resource_id` linked to that resource, not by token ID.[^v2-iperm-l57][^v2-pr-l261][^v2-pr-l351] Resolver-scoped permissions live in the same resource-anchored model with resolver scope metadata; `PermissionedResolver` uses name-, text-key-, and coin-type-specific EAC resources for setters.[^v2-pres-l70][^v2-pres-l159][^v2-pres-l239][^v2-pres-l257][^v2-pres-l282]

Required indexes: by resource, by account, by resolver; permission history by resource and by account.

## Primary and reverse names

`PrimaryName` is address- and `coin_type`-centric, not just a reverse-record projection. Persists `claimed_primary_name`, `verified_primary_name`, `reverse_namespace`, `coin_type`, `resolver`, provenance, coverage.

- Both objects use `ResultStatus`. `mismatch` and `execution_failed` apply to verified only.
- `claimed_primary_name` is candidate-only; `verified_primary_name` is authoritative only when `success`.
- A raw claim that cannot be normalized surfaces `invalid_name`, not silent drop.
- Reverse claims alone don't verify — verification must resolve back to the requested address.[^v1-aur-l217][^v1-aur-l226][^v1-aur-l263][^v1-aur-l269]

For ENS, declared claim precedence is reverse-only through `ens_v1_reverse_l1`.[^v1-revreg-deploy][^v1-revreg-l74][^v1-revreg-l83][^v1-revreg-l84] Persisted `claimed_primary_name.name` comes only from the exact requested `primary_names_current(address, coin_type, namespace)` row's declared normalized claim-identity source. The app default tuple (`namespace=ens`, `coin_type=60`) may use a route-local Ethereum Mainnet reverse RPC fallback when that persisted tuple is missing: build the `addr.reverse` node, read its ENS registry resolver, call resolver `name(bytes32)`, normalize the result, and publish provenance as `ens_reverse_rpc` without populating `primary_names_current`.[^v1-registry-deploy][^v1-revreg-l137][^v1-registry-l137][^v1-nameresolver-l7][^v1-nameresolverimpl-l25] In `mode=verified|both`, that route-local fallback verifies the claimed name with `addr:60` through the ENS Universal Resolver proxy at provider `latest`; it returns verification status without persisting an execution trace.[^v1-ur-deploy][^v1-iur-l44][^v1-iur-l52] Outside that fallback, `claimed_primary_name.name` is never synthesized from manifest presence, resolver identity, or verified execution.

For Basenames, declared primary-claim intake is `basenames_base_primary` (`reverse_registrar` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282`).[^bn-readme-l33][^bn-revreg-l12][^bn-revreg-l150] It does not replace the Base registry/registrar/resolver families for declared truth on exact-name, address-name, or children reads. Verified primary names enter through `basenames_execution` against the L1 Resolver.[^bn-readme-l22][^bn-l1resolver-l13][^bn-revreg-l193]

Verified-primary cache identity is `request_type=verified_primary_name` with key `{namespace}:{normalized_address}:{coin_type}`. The matching `primary_names_current` row is the only claim-side lookup/invalidation anchor.

Section-local provenance:

- `claimed_primary_name.provenance` is exact-tuple declared-only provenance from the requested row, or route-local `ens_reverse_rpc` resolver provenance for the ENS/60 on-demand fallback. No `execution_trace_id`.
- `verified_primary_name.provenance` (when present) is `{execution_trace_id, manifest_versions}` for persisted readback and must equal the top-level `execution_trace_id`. Route-local ENS/60 verification omits it.

## Collection semantics

### Exact-name lookup

Resolves a `NameSurface`. Returns normalized identity, current binding, declared summary sections (registration, authority, control, resolver, record inventory, history), provenance, coverage.

Each declared summary section is always present as an object; unprojected sections return an explicit unsupported object rather than disappearing. Exact-name `control` carries `registrant`, `registry_owner`, `latest_event_kind`. Exact-name `resolver` carries `chain_id`, `address`, `latest_event_kind`; `chain_id=null/address=null` means "no declared resolver", not "resolver reads unsupported". Exact-name `history` is two head pointers — `surface_head` and `resource_head` — into the canonical history contract, not embedded rows.

For Basenames, exact-name declared truth comes from the Base authority split (`basenames_base_registry`, `basenames_base_registrar`, `basenames_base_resolver`); claim and transport families don't widen it.[^bn-readme-l69][^bn-readme-l70]

### Address → names

Returns surfaces, not backing resources. Each item carries `logical_name_id`, surface identity, `resource_id`, relation facets (`registrant`, `token_holder`, `effective_controller`), binding kind, provenance, coverage.

`dedupe_by=resource` is grouping-only. Default sort is `display_name_asc`. Exhaustiveness is authoritative only for source classes with enumerable ownership/assignment surfaces; wildcard- and offchain-derived names are never silently treated as exhaustive.

### Address → names with `include=role_summary`

Additive expansion, not a separate route. Adds `role_summary` (one `subjects[*]` entry per distinct current permission subject for the same `resource_id`, with `scope` and `effective_powers`), `subname_count`, `record_count`, `status`, `expiry`. Identity, supported filters, grouping, default sort, cursor, and coverage stay unchanged.

`subname_count` reuses declared-direct-children semantics. `record_count` is the count of distinct stable declared record selectors at the current version boundary.

### Name → children

Default returns declared direct child surfaces. Optional buckets: linked-subregistry, alias-derived, observed wildcard. `subname_count` in the main name summary means declared direct children only.

### Resource → permissions

The resource-centric collection. One current row per `(resource_id, subject, scope)` key. Subject- or resolver-centric summaries derive from these rows. If a surface rebinds across ENSv1 anchors, reads stay partitioned by `resource_id` rather than stitching predecessors together.

### History

Queryable by `scope=surface|resource|both`. History reads are canonical normalized-event reads, not separate denormalized truth tables. `Address.history` composes address anchor resolution with the same contract.

### Resolver overview

Resolvers are first-class read targets. Sections: bindings, alias mappings, resolver-scoped permissions, role holders, events, counts. Each section is supported only when a projection owns the fan-in. Shared ENSv1 PublicResolver targets do not enumerate current-name fan-in for `bindings`, `aliases`, or event summaries — those return `UnsupportedSummary` with `resolver_binding_enumeration_not_projected`. Exact-name resolver state stays on exact-name routes.

### Explain by exact name

Three thin views over already-projected truth, each scoped to the same exact-name snapshot:

- `surface-binding` — current `SurfaceBinding` plus exact-name history head pointers
- `authority-control` — same `authority` and `control` summaries as the exact-name route
- `coverage` — the same `Coverage` object returned inline by `GET /v1/names/{namespace}/{name}`

None of these introduces a separate truth system or ledger.

## Coverage and exhaustiveness

Coverage is contractual.

- Exact-name lookup is authoritative for supported source classes. Route-level coverage may still be authoritative when individual declared summary subdocuments are unsupported.
- Address-to-name enumeration is exhaustive only for enumerable source classes.
- Wildcard and offchain name classes are not globally enumerable.
- Record inventory is `best_effort` unless a resolver family enumerates explicitly or there's a source-specific index.
- Child enumeration is authoritative only for declared direct children unless the caller opts into other surface classes.
- Primary-name route-level coverage is `partial` for the frozen ENS and Basenames exact-tuple persisted-readback class and for the ENS/60 on-demand fallback, including `ens_execution_rpc` when verified mode performs live forward verification, with `exhaustiveness=non_enumerable` and `enumeration_basis=primary_name_lookup`. Out-of-class tuples are explicit `unsupported`.

Every response carries `coverage.status`, `coverage.exhaustiveness`, `coverage.source_classes_considered`, `coverage.unsupported_reason`, `coverage.enumeration_basis`.

## Verified execution

Default verified entrypoints:

- ENS: `ens_execution` at the official Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`.[^ens-docs-univ][^v1-aur-l90][^v1-aur-l106]
- Basenames: active `basenames_execution` v2 at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` supports only the exact-surface transport-assisted direct path; other Basenames verified path classes stay `unsupported`.[^bn-readme-l22][^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]

The execution engine supports onchain calls, wildcard resolution, alias-aware execution, nested CCIP-Read, batch/multicall, proof and verification persistence.

`ExecutionTrace` per verified answer: entrypoint, resolver discovery path, contracts called, gateway URLs or digests, proof and callback checks, final result, errors, chain positions.

Cache identity: request, chain positions, manifest versions, relevant topology/version boundaries. Invalidate on reorg, manifest change, relevant topology change, relevant record change, relevant alias/wildcard change.

## Reorg, replay, backfill

The system stores block lineage per chain. On divergence: detect fork point, mark affected facts `orphaned`, invalidate dependent normalized events and execution cache, rebuild projections deterministically. Reorg repair preserves audit trail — orphaned rows persist for explanation and rebuild. Detail in [`chain-intake.md`](chain-intake.md).

Backfills use the same path as live ingestion (raw → manifest/discovery → normalized → projection). Source-scoped backfill is selected-target-only — it must not turn unselected block-wide bodies into hot rows merely because they were fetched. Operational catch-up to finalized head runs as bounded idempotent chunks; capacity failures pause the chunk explicitly rather than silently retaining less data.

Required backfills: ENSv1 historical state, ENSv1 wrapper/migration history, ENSv1 DNS and offchain discovery where supported, ENS reverse/primary history, ENSv2 historical registration, topology, permissions, alias history, Basenames historical registration, control, primary, resolution history.

Wildcard and offchain names cannot be assumed exhaustively enumerable; backfill for those classes is discovery- and observed-answer-based.

## Operations

Metrics: chain lag, safe/finalized lag, reorg depth, adapter failure rate, manifest drift, proxy upgrade detection, execution latency, CCIP error rate, verification failure rate, coverage partial rate, replay duration, backfill capacity checks (Postgres size, free disk).

Worker-owned tools (none expose public `v1` routes; none mutate truth):

- `bigname-worker inspect canonicality --chain-id <id> --block-hash <hash>` — single-block lineage, canonicality state, parent hash, raw fact counts, normalized-event counts.
- `bigname-worker inspect ...` — execution traces, manifest drift / proxy alerts, surface bindings, resolver topology, raw facts, manifest versions.
- replay from checkpoint, backfill source range, rerun projections from normalized events, invalidate execution cache, diff declared vs verified.
- finalized-head catch-up runs bounded idempotent chunks with capacity preflight (DB size, free disk, configured object-cache budget).

Live manifest drift / proxy upgrade alerting is a worker-owned operational loop. It does not write `normalized_events`, mutate manifests, rewrite discovery, or expose a public route.

## Constraints

- versioned native public contract from day one
- namespace is first-class and explicit
- public surface identity is distinct from backing resource, token, resolver instance, and reverse namespace identity
- provenance, coverage, and finality are first-class
- resolution is not modeled as event-only
- verified execution is a required subsystem
- permissions are first-class
- source manifests are first-class
- preimage observation is first-class
- projections are disposable and rebuildable
- protocol-specific logic lives in adapters and execution drivers, not in the public contract
- no silent cross-source fallback; every fallback appears in provenance/explain
- no requirement to preserve the ENSv1 indexer API surface

## Implementation shape

Rust modular monolith. PostgreSQL is the hot indexed/replay store. Hash-addressed object storage for execution artifacts and durable raw payloads. Workers handle ingestion, projection, replay, execution. The public `v1` API is read-only over projections and execution output. A small TypeScript conformance harness checks protocol and consumer-capability behavior.

Repository layout:

- `apps/api`, `apps/indexer`, `apps/worker`
- `crates/domain`, `crates/storage`, `crates/manifests`, `crates/adapters`, `crates/execution`, `crates/test-support`
- `tests/conformance`

## Test matrix

ENSv1 and wrapper: ENSv1-only name, wrapped name, wrapped expiry/grace edge, fuse changes that alter control, wrapped owner ≠ registrant, reverse claim vs verified primary mismatch.

ENSv2: root-scope role grant, delegate retained after transfer, token regeneration without ownership change, shared subregistry creating multiple surfaces for one resource, alias-derived surface with no direct registry entry, subregistry swap replacing a subtree, re-registration with same resource and new token ID.

DNS / wildcard / offchain: imported DNS name, gasless DNS or metadata-discovered name where supported, wildcard-derived subname, CCIP success, CCIP failure, offchain gateway mismatch.

Basenames: NFT-only transfer, management-only transfer, address-resolution change, full transfer, primary-name set/unset, L1 compatibility resolution, current single-address capability.

Operational: reorg across authority events, reorg across verified execution cache, replay determinism from raw facts, replay determinism from normalized events, proxy implementation change, manifest version change.

Validate at four layers: raw facts, normalized events, execution traces, public API output.

## Open decisions

- exact Postgres partitioning strategy
- exact cache invalidation granularity for verified queries
- which execution artifacts stay inline in Postgres vs object storage
- exact raw-payload cache retention windows and which payload classes are durable
- whether subscriptions ship in the first stable read milestone or after

---

[^ens-docs-univ]: <https://docs.ens.domains/resolvers/universal/> (official Universal Resolver proxy)

[^bn-readme-l8]: (upstream: .refs/basenames/README.md:L8 @ basenames@1809bbc)
[^bn-readme-l14]: (upstream: .refs/basenames/README.md:L14 @ basenames@1809bbc)
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
[^bn-registry-l132]: (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
[^bn-registry-l223]: (upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc)
[^bn-baseregistrar-l15]: (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L15 @ basenames@1809bbc)
[^bn-l2resolver-l4]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc)
[^bn-l2resolver-l16]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc)
[^bn-l2resolver-l22]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
[^bn-l2resolver-l29]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc)
[^bn-l2resolver-l182]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc)
[^bn-l2resolver-l193]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)
[^bn-l2resolver-l209]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc)
[^bn-l2resolver-l225]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)
[^bn-revreg-l12]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
[^bn-revreg-l150]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
[^bn-revreg-l193]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)

[^v1-ens-l12]: (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)
[^v1-ensreg-l89]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)
[^v1-ensreg-l174]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f)

[^v1-iname-l10]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10 @ ens_v1@91c966f)
[^v1-iname-l27]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)
[^v1-iname-l31]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L31 @ ens_v1@91c966f)
[^v1-iname-l35]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f)
[^v1-iname-l37]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f)
[^v1-iname-l38]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f)

[^v1-namewrapper-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f)
[^v1-publicresolver-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f)
[^v1-revreg-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-ur-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)
[^v1-iur-l44]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L44 @ ens_v1@91c966f)
[^v1-iur-l52]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L52 @ ens_v1@91c966f)

[^v1-nw-l132]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L132 @ ens_v1@91c966f)
[^v1-nw-l240]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L240 @ ens_v1@91c966f)
[^v1-nw-l377]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L377 @ ens_v1@91c966f)
[^v1-nw-l421]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L421 @ ens_v1@91c966f)
[^v1-nw-l427]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L427 @ ens_v1@91c966f)
[^v1-nw-l637]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L637 @ ens_v1@91c966f)
[^v1-nw-l666]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L666 @ ens_v1@91c966f)
[^v1-nw-l676]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L676 @ ens_v1@91c966f)
[^v1-nw-l723]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L723 @ ens_v1@91c966f)
[^v1-nw-l827]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L827 @ ens_v1@91c966f)
[^v1-nw-l1022]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022 @ ens_v1@91c966f)
[^v1-nw-l1023]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1023 @ ens_v1@91c966f)
[^v1-nw-l1034]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1034 @ ens_v1@91c966f)

[^v1-pres-l5]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L5 @ ens_v1@91c966f)
[^v1-pres-l13]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L13 @ ens_v1@91c966f)
[^v1-pres-l20]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^v1-pres-l51]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L51 @ ens_v1@91c966f)
[^v1-pres-l58]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L58 @ ens_v1@91c966f)
[^v1-pres-l66]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L66 @ ens_v1@91c966f)
[^v1-pres-l114]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f)

[^v1-namechanged-l10]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L10 @ ens_v1@91c966f)
[^v1-namechanged-l18]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L18 @ ens_v1@91c966f)

[^v1-revreg-l15]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f)
[^v1-revreg-l19]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L19 @ ens_v1@91c966f)
[^v1-revreg-l74]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f)
[^v1-revreg-l83]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f)
[^v1-revreg-l84]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
[^v1-revreg-l129]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f)
[^v1-revreg-l130]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
[^v1-registry-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ENSRegistry.json:L2 @ ens_v1@91c966f)
[^v1-revreg-l137]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L137 @ ens_v1@91c966f)
[^v1-registry-l137]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137 @ ens_v1@91c966f)
[^v1-nameresolver-l7]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L7 @ ens_v1@91c966f)
[^v1-nameresolverimpl-l25]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L25 @ ens_v1@91c966f)

[^v1-aur-l90]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L90 @ ens_v1@91c966f)
[^v1-aur-l106]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L106 @ ens_v1@91c966f)
[^v1-aur-l217]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L217 @ ens_v1@91c966f)
[^v1-aur-l226]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L226 @ ens_v1@91c966f)
[^v1-aur-l263]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L263 @ ens_v1@91c966f)
[^v1-aur-l269]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L269 @ ens_v1@91c966f)
[^v1-ursol-l8]: (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f)

[^v1-ethrc-l116]: (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L116 @ ens_v1@91c966f)
[^v1-ethrc-l133]: (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L133 @ ens_v1@91c966f)

[^subgraph-l15]: (upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)
[^subgraph-l39]: (upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)
[^subgraph-l44]: (upstream: .refs/ens_subgraph/subgraph.yaml:L44 @ ens_subgraph@723f1b6)
[^subgraph-l145]: (upstream: .refs/ens_subgraph/subgraph.yaml:L145 @ ens_subgraph@723f1b6)
[^subgraph-l170]: (upstream: .refs/ens_subgraph/subgraph.yaml:L170 @ ens_subgraph@723f1b6)
[^subgraph-l226]: (upstream: .refs/ens_subgraph/subgraph.yaml:L226 @ ens_subgraph@723f1b6)
[^subgraph-ts-l134]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L134 @ ens_subgraph@723f1b6)
[^subgraph-ts-l230]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L230 @ ens_subgraph@723f1b6)
[^subgraph-ts-l238]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6)
[^subgraph-ts-l246]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6)

[^v2-deploy-root]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2 @ ens_v2@554c309)
[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309)
[^v2-deploy-ethrc]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)
[^v2-deploy-pres]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/PermissionedResolverImpl.json:L2 @ ens_v2@554c309)

[^v2-userreg-l15]: (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L15 @ ens_v2@554c309)
[^v2-ethrc-l30]: (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L30 @ ens_v2@554c309)
[^v2-ethrc-l173]: (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L173 @ ens_v2@554c309)

[^v2-iperm-l22]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L22 @ ens_v2@554c309)
[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309)
[^v2-iperm-l57]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L57 @ ens_v2@554c309)
[^v2-iperm-l67]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L67 @ ens_v2@554c309)
[^v2-iperm-l72]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L72 @ ens_v2@554c309)
[^v2-events-l15]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309)
[^v2-events-l30]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L30 @ ens_v2@554c309)
[^v2-events-l49]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309)
[^v2-events-l59]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L59 @ ens_v2@554c309)
[^v2-events-l69]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309)
[^v2-events-l75]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309)

[^v2-pr-l28]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L28 @ ens_v2@554c309)
[^v2-pr-l131]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L131 @ ens_v2@554c309)
[^v2-pr-l141]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L141 @ ens_v2@554c309)
[^v2-pr-l151]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L151 @ ens_v2@554c309)
[^v2-pr-l203]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L203 @ ens_v2@554c309)
[^v2-pr-l216]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L216 @ ens_v2@554c309)
[^v2-pr-l222]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L222 @ ens_v2@554c309)
[^v2-pr-l225]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L225 @ ens_v2@554c309)
[^v2-pr-l237]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L237 @ ens_v2@554c309)
[^v2-pr-l261]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L261 @ ens_v2@554c309)
[^v2-pr-l351]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L351 @ ens_v2@554c309)
[^v2-pr-l451]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309)
[^v2-pr-l461]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461 @ ens_v2@554c309)
[^v2-pr-l536]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L536 @ ens_v2@554c309)
[^v2-pr-l545]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L545 @ ens_v2@554c309)

[^v2-iperm-resolver-l14]: (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309)
[^v2-iethreg-l32]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309)
[^v2-iethreg-l53]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309)

[^v2-pres-l38]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L38 @ ens_v2@554c309)
[^v2-pres-l56]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L56 @ ens_v2@554c309)
[^v2-pres-l70]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L70 @ ens_v2@554c309)
[^v2-pres-l132]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L132 @ ens_v2@554c309)
[^v2-pres-l142]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L142 @ ens_v2@554c309)
[^v2-pres-l153]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L153 @ ens_v2@554c309)
[^v2-pres-l159]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L159 @ ens_v2@554c309)
[^v2-pres-l230]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L230 @ ens_v2@554c309)
[^v2-pres-l239]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L239 @ ens_v2@554c309)
[^v2-pres-l257]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L257 @ ens_v2@554c309)
[^v2-pres-l282]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L282 @ ens_v2@554c309)
[^v2-pres-l412]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L412 @ ens_v2@554c309)
[^v2-pres-l650]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L650 @ ens_v2@554c309)

[^v2-eac-l19]: (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309)
[^v2-eac-l176]: (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L176 @ ens_v2@554c309)
[^v2-eac-l181]: (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L181 @ ens_v2@554c309)
