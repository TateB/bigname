# Architecture

bigname is a versioned, replayable indexing and read platform for ENS (v1 and v2) and Basenames. It serves the native `v2` REST contract, a narrow GraphQL compatibility surface, and operator health.

This document defines the model. Wire format lives in [`api-v2.md`](api-v2.md) and [`api-v2-routes.md`](api-v2-routes.md); persistence in [`storage.md`](storage.md); manifests, intake, projections, and execution in their own files. Implementation sequencing and parallel-work boundaries live under [`internal/`](internal/).

## Objectives

For any supported name or address, every answer must be:

- point-in-time
- replayable
- auditable
- explicit about provenance, coverage, finality, and consistency
- safe under chain reorgs and source-graph expansion

REST answers use the v2 `data`/`page`/`meta` envelope and report unsupported,
partial, stale, or failed results explicitly.

## Public namespaces

The public namespaces are exactly `ens` and `basenames`.

- `ens` is a single product that absorbs both ENSv1 and ENSv2 as internal authority epochs.
- `basenames` is separate, covering Basenames-issued `*.base.eth` names on Base.[^bn-readme-l70]
- `base.eth` itself stays under `ens` because upstream treats it as the L1 root domain handled by the Mainnet `L1Resolver`.[^bn-l1resolver-l13][^bn-l1resolver-l154]

Namespace assignment is driven by an internal `NamespaceRegistry` with versioned rules: highest-priority `exact_name`, then `suffix`, then `authority_root`. Initial policy:

- exact `base.eth` → `ens`
- suffix `*.base.eth` → `basenames`
- other supported ENS surfaces → `ens`

Conflicts reject canonical [admission](glossary.md); namespace assignment happens before `logical_name_id` is minted. [Deployment profile](glossary.md) is separate from namespace: deployment profiles select the admitted chain set (mainnet, post-audit Sepolia), not a different namespace product. One runtime answers under one deployment profile at a time.

## Read contract

The served REST families are lookup, status, name, address, permission, search,
event, resolver, namespace, and diagnostic routes under `/v2`. Their parameters,
result vocabulary, snapshot behavior, and pagination rules are defined in
[`api-v2-routes.md`](api-v2-routes.md). The deleted v1 REST shapes are not a
compatibility layer for this contract.

### Subgraph-compatible GraphQL surface

Alongside the REST contract, bigname serves a narrow, deliberately scoped subgraph-compatible read surface at `POST /graphql`. It is **not** general subgraph parity: it implements `account`, `accounts`, `domain`, `domains`, `resolver`, `resolvers`, `registrationConnection`, and `domainConnection` over `bigname_phase.name_current`, `bigname_phase.address_names_current`, and `bigname_phase.record_inventory_current` [projections](glossary.md), plus `_meta` for the served publication. Entity reads accept the subgraph-shaped `block` and `subgraphError` arguments, while the current execution boundary remains the [served head](glossary.md#served-head) rather than historical projection reads. All root fields in one HTTP GraphQL request share one served-head selection. Reads admit unchanged rows whose target is at or before that position, carry the same selection into nested record-inventory fields, and verify before returning that the matching completed `project` phase row did not change. Rows whose projection support status is `unsupported` are not exposed; an unsupported record inventory maps to the compatibility surface's existing empty record shapes. GraphQL `createdAt` uses a declared registration or history timestamp; when neither exists, it preserves the non-null response field with Unix epoch `0` because the current phase projection has no legacy surface-creation timestamp. `createdAt` and `expiryDate` are decimal-string `BigInt` values. The GraphQL surface is a compatibility adapter, not a consumer-replacement declaration.

The `domain(id: ...)` point root and legacy-connection `DomainFilter.name` have
ENS name semantics: they normalize a name, compute its namehash, and match that
hash, so `ALICE.eth` resolves the same ENS name as `alice.eth`. An `id` already
shaped as a namehash is matched only within the `ens` namespace. Legacy
`DomainFilter.name_contains` is ENSIP-15 normalized at the GraphQL boundary and
then compared byte-for-byte with the stored normalized name; invalid input
returns a GraphQL error. A single trailing dot remains preserved after
normalization as a label boundary. One leading dot is also accepted when it is
followed by a nonempty fragment that does not begin with another dot. The
following fragment is normalized as usual, and the leading dot is preserved for
matching. Thus `.eth`, `eth.`, `.eth.`, and `th.e` are accepted contains
fragments, while `.` and `..` return a GraphQL error. Leading-boundary support is
specific to the legacy contains filter; REST `match=prefix` fragment behavior is
unchanged and does not accept a leading dot. Generated `Domain_filter.name`
members instead compare the served display-name bytes directly, preserve SQL
wildcards for pattern operators, and use explicit `COLLATE "C"`.
`Domain_orderBy.name` orders the served display name with that collation. The
complete split is defined in [`consumer-capabilities.md`](consumer-capabilities.md#graphql-compatibility).
Resolver record fields select the sole projected inventory for the name's
current control resource or, for an ownerless V1 registry name, its retained
[serving resource](glossary.md#serving-resource), without coupling the
inventory's event boundary to the later name-publication target. If a resource has multiple
inventory rows and no declared boundary selects exactly one, the operation
errors instead of serving empty records or choosing arbitrarily.

GraphQL availability follows the exact current-head admission rule shared
with v2 lookup. While the `project` phase catches up to a newly stored chain head, an
operation that would return projection rows errors instead of serving the
previous completed publication as a stale view. Connection counts are also
more expensive than a plain `COUNT(*)`: they compute distinct matched rows and
rank their publication targets so the API can validate count admission before
returning the result.

## Identity model

Four identity layers, each with its own continuity rules:

### `logical_name_id`

Stable identity for an on-chain name within a namespace, written as `<namespace>:<namehash>` where `namehash` is the lowercase `0x`-prefixed 32-byte node. It survives backing-resource rotation, token regeneration, lapses, re-registrations, and normalizer-version changes. Raw label text and normalization results are attributes, never identity inputs, under the audit's [normalization-as-a-gate decision](../simplification-audit-20260730.md#normalization-as-a-gate-not-stored-identity-maintainer-2026-07-30).

### `resource_id`

Stable identity for the backing authority object — the [anchor](glossary.md) for permission lineage, control lineage, token lineage, and resolver-scoped permissions. Opaque UUID.

- For ENSv2, `resource_id` maps to the upstream permissioned-registry EAC resource, not the current ERC-1155 token ID. The registry exposes `getResource(anyId)` and `getTokenId(anyId)`, emits `TokenResource(tokenId, resource)` when a label is linked, and emits `TokenRegenerated(oldTokenId, newTokenId)` when role changes burn and mint a replacement token while leaving the resource unchanged.[^v2-iperm-l34][^v2-iperm-l67][^v2-iperm-l72][^v2-events-l69][^v2-pr-l451]
- For ENSv1, `resource_id` is the stable identity for the authority object: registry-only control, registrar-backed registration, or wrapper-backed control. Registry-only authority is scoped to the full node/namehash, not just the leftmost labelhash, so subnames with the same label under different parents never share a registry-only `resource_id`. The same `resource_id` persists across holder, resolver, expiry, grace, fuse, status, and non-divergent controller changes. It rotates when authority moves to a different anchor — the concrete authority object backing the name (direct registry control, a registrar lease, or a wrapper position). Rotation happens on a registry-only ↔ registrar ↔ wrapper move, a live registrar ↔ registry-owner divergence, or a full lapse + re-registration. Exact prior-anchor reuse applies only when that prior anchor becomes authoritative again, including unwrap back to the same registrar lease and registry-side convergence back to the same live unreleased registrar lease. If the deployment profile has no materialized prior registrar identity, the ordered `NameUnwrapped` then BaseRegistrar `Transfer` establishes it at that transfer and later replay reuses it. A completed `syncWrapper` [ENSv1→ENSv2 migration correlation group](glossary.md#migration-correlation-group) may refine the registrar expiry used only when that later transfer first materializes the missing registrar identity; multiple completed groups retain the monotone maximum correlated expiry, and that correlated state does not update ordinary NameWrapper normalized events or NameWrapper state. The maximum is safe across full lapse and re-registration because BaseRegistrar accepts re-registration only after the prior expiry plus grace and then writes a strictly later `block.timestamp + duration` expiry. After that fallback materializes the registrar identity, ordinary BaseRegistrar transfers continue to emit `fallback_from_wrapper: true` with `fallback_from` set to the current transfer sender so the latest transfer row can restore the identity by itself; a label-bearing registrar-controller registration or renewal replaces that fallback state. It does not imply that all registry owner / token holder convergence collapses history; post-release returns or different holders / controllers stay on distinct anchors. (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L103 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L168 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L104-L111 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L318-L337 @ ens_v1@91c966f)
- For Basenames, `resource_id` anchors the Base-side authority object even when L1 compatibility transport is involved.[^bn-readme-l69][^bn-readme-l70][^bn-l1resolver-l13]

A registry resource can remain the event-derived [serving resource](glossary.md#serving-resource)
after its control binding closes. Resource identity alone never establishes current authority:
control requires the selected binding, while resolver and record serving may use the separate typed
reference documented in [`projections.md`](projections.md#exact-name-projection).

### `token_lineage_id`

Stable identity for tokenized ownership history. Token IDs can change while the resource is unchanged; the lineage outlives the ID.

- ENSv1: registry-only control has none. A registrar lease or wrapper position mints one. Renewal, transfer, expiry, and grace within the same anchor preserve it. Authority moving to a different tokenized anchor rotates it; returning to the prior tokenized anchor reactivates the prior lineage.
- ENSv2: preserved across `TokenRegenerated`. Update the current token ID attribute and append the normalized event. Resource identity is anchored by upstream `eacVersionId`; tokens are versioned by `tokenVersionId`. Unregister/re-register of a registered entry increments both, while regeneration increments only the token version.[^v2-pr-l28][^v2-pr-l203][^v2-pr-l237][^v2-pr-l241][^v2-pr-l242][^v2-pr-l451][^v2-pr-l461][^v2-pr-l542][^v2-pr-l547] Releasing an owner-zero reservation and then registering it can instead preserve both versions, so the registration reuses the reservation resource. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L200-L206 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L428-L471 @ ens_v2@a971bd64)

### `contract_instance_id`

Stable identity for registry, registrar, resolver, wrapper, or transport instances. Minted when a manifest-declared or discovery-admitted contract is first added to the canonical source graph. One admitted address on one chain maps to one `contract_instance_id` across all manifest and discovery epochs; re-admission after an inactive gap reuses it with a new active range. A proxy keeps its identity when implementation changes; only a different watched contract address rotates it.

## Name surface model

Two layers separate public names from backing authority:

`NameSurface` is the canonical row per `logical_name_id`. It stores the raw name and labels observed for that namehash when they have a PostgreSQL-safe UTF-8 decoding, their available DNS wire encoding and hash path, the normalizer version used to evaluate them, and explicit visibility/error state. A label that does not byte-equal its normalized form, cannot decode, or cannot be represented as one DNS label keeps a deactivated shadow row. For undecodable labels, the chain-native namehash and byte-valued `label_preimages` are identity truth while the row's unavailable text display inputs remain empty. Normalized label or display text is derived at read time and is not stored as identity. Verbatim labels also live in `label_preimages` and [normalized events](glossary.md).

`SurfaceBinding` records how a public surface binds to a backing [resource](glossary.md) through time — each row is a [surface binding](glossary.md):

- `surface_binding_id`, `logical_name_id`, `resource_id`, `binding_kind`,
  `authority_arm`, `active_from`, `active_to`, chain, provenance, and
  [canonicality](glossary.md) state.

Binding kinds: `declared_registry_path`, `linked_subregistry_path`, `resolver_alias_path`, `observed_wildcard_path`, `observed_only`.

`authority_arm` is the storage discriminator for the name's
[authority epoch](glossary.md#authority-epoch): `ens_v1`, `ens_v2`, or
`basenames`. Adapters assign it explicitly when they create binding and closure
drafts; SQL never infers it from `binding_kind`, a
[source-family](glossary.md#source-family) string, or provenance JSON. An
ordinary binding open, unbind, or predecessor cap conflicts
only with rows in the same `(chain_id, logical_name_id, authority_arm)` domain.
Consequently ordinary ENSv1 and ENSv2 bindings for one logical name can coexist.
Only an activated [migration boundary](glossary.md#migration-boundary) may close
an ENSv1 row while retaining or opening the concrete ENSv2 successor.

Resolver-family normalized events attach `logical_name_id` and `resource_id` only when their node has a materialized active or deactivated-shadow `NameSurface`. Without that row, both identity fields remain null and only `raw_fact_ref.interpreter_state_key` relates successive state for the same record. Those interpretation-time null fields remain immutable. When Project builds an ENSv1 record inventory, it may additionally attribute an `ens_v1_resolver_l1` `RecordChanged` or `RecordVersionChanged` event whose `logical_name_id` is null to a materialized name when the selected pointer's source family is `ens_v1_registry_l1`, `ens_v1_registrar_l1`, or `ens_v1_wrapper_l1`. An `ens_v2_registry_l1` or `ens_v2_root_l1` pointer also qualifies when its target's final staged classification is supported `ens_v1_resolver_l1` from an applicable exact declaration. A `basenames_base_resolver` event whose `logical_name_id` is null qualifies only when the selected pointer is `basenames_base_registry`. Each exception matches the event's chain and node to the surface namehash and its emitting resolver to the resource's latest retained, linked `ResolverChanged` event for which Project staged a [readable canonical](glossary.md#readable--read-safe) name surface at the target; incremental staging applies the same family and exact-declaration guards. If a later linked resolver event's name lacks such a surface, an earlier linked event with one is the fallback. A selected zero-address resolver is a clear and suppresses inventory instead of falling back to an older nonzero event. Surface `visibility_state` does not participate in this pointer choice. The emitter is `after_state.resolver`, falling back to `raw_fact_ref.emitting_address`; `resolver_contract_instance_id` remains provenance rather than resolver identity. Events with a stored name remain attributable without restricting either the pointer or record event's source family, and an unknown node still creates no surface, binding, or serving row. This matches the ENSv1 read path: the registry returns the node's current resolver (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137 @ ens_v1@91c966f), while text storage and reads are keyed by record version, node, and key rather than resolver-selection time (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L28 @ ens_v1@91c966f). Basenames likewise stores the current resolver by node, authorizes resolver writes from its registrar controller and reverse registrar independently of the node owner, and stores text by record version, node, and key. (upstream: .refs/basenames/src/L2/Registry.sol:L173-L180 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193-L199 @ basenames@1809bbc) (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/ResolverBase.sol:L7-L24 @ basenames@1809bbc) (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/TextResolver.sol:L7-L36 @ basenames@1809bbc) The ordinary ENSv1 write is reachable before surface discovery because `setText` uses node authorization (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L19 @ ens_v1@91c966f).

A standalone ENSv1 or Basenames registry-owner observation for a node without a materialized `NameSurface` creates the node-scoped direct-registry resource, but it does not independently create a public surface or binding. A registry-owner observation attributed to a live registrar lease, including ownership setup reconciled within the registration transaction, instead remains retained interpreter state without creating a separate direct-registry resource, surface, or binding; that attribution keeps the direct-registry authority dormant. Once a registrar or wrapper observation materializes the surface, retained direct-registry authority may become its fallback. If release of the active registrar lease makes a nonzero retained registry owner authoritative, the release boundary must materialize the registry-anchored resource and open its replacement `SurfaceBinding` in the same interpret batch. The resource and binding use block-boundary provenance because upstream registrar availability is derived by comparing the stored expiry plus the 90-day grace period with `block.timestamp`, rather than by a lease-expiry log (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L103 @ ens_v1@91c966f). The retained registry owner survives that registrar release because ENS stores node ownership independently until another registry ownership write replaces it (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L7 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L13 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L170 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L171 @ ens_v1@91c966f). This rule does not widen ENSv2 registry-name topology or emit either side of a parent-child binding for an otherwise unknown registry-only surface.

ENSv1 authority moves (wrap, unwrap, re-registration) carry the identity change in `resource_id` and `token_lineage_id`; ordinary lifecycle stays `declared_registry_path`. A new `SurfaceBinding` row appears only when the bound `resource_id` changes — transfer and expiry within the same anchor do not. Same-transaction reconciliation considers only setup observations whose `source_event == NewOwner` when removing transient controller artifacts; its canonical admitted case is the retired 2019 controller stream and its register/reclaim-shaped ownership setup. Registrar reclaim writes the registry through `setSubnodeOwner`, which emits `NewOwner` (upstream: .refs/ens_subgraph/subgraph.yaml:L145 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L148 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L162 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L165 @ ens_subgraph@723f1b6) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L174 @ ens_v1@91c966f). The current controller's resolver path also registers first to `address(this)`, but then calls registry `setRecord`; that ownership write emits `Transfer`, not `NewOwner`, so it never enters this removal branch. This is benign: the general same-transaction reconciliation still attributes the incoming `Transfer` observations to the registration resource (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L294 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L301 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L33 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L39 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L68 @ ens_v1@91c966f). In the wrapped registration path, NameWrapper registers the registrar token and registry ownership to itself while the separate `wrappedOwner` remains the user; the incoming NameWrapper `resource_control` grant therefore belongs to the registrar-backed registration resource even though its subject differs from the registration event's registrant (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L291 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L297 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L300 @ ens_v1@91c966f). `NameWrapped` is emitted within that wrapper call before the controller emits its later `NameRegistered`, so the later registrar observation records the registrar resource without displacing the active wrapper resource (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f). Basenames writes the final owner directly before its registrar emits `NameRegistered`, so it has no equivalent wrapper-owner split or transient controller-owner epoch (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L423 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L425 @ basenames@1809bbc). When block-scoped replay has preloaded an older registry-only authority for that name, same-transaction registry and resolver observations that establish the incoming registration are attributed to the new registrar resource. Resolver records before the first registry ownership-setup observation retain the predecessor resource. From that first setup through `NameRegistered`, otherwise-unattached or registry-only resolver records belong to the registration resource; records already attached to another materialized resource retain their event-time authority. Pre-registration membership for permission events is decided by revocation semantics, not by comparing a subject with the registrant: revocations, plus matching earlier grants that those revocations close, stay on the preceding registry-only resource so latest-wins permission projection supersedes them. Other incoming grants move to the registration resource. The superseded registry-only resource row is always retained at its first derivation block, whether or not a surviving same-batch row references it. A proven transient registration-controller `NewOwner` observation and that controller's matching self-grant and self-revoke are setup artifacts rather than a separate authority transition. Renewals and wrapper observations retain their event-time authority. This registration-setup rule does not define a registry-only `RegistrationGranted` pre-state contract.

For born-wrapped registrations, registrar authority state tracks the final registry owner separately from the controller event's registrant. On unwrap, NameWrapper transfers the registrar token from itself to the requested registrant, so that registrar transfer closes NameWrapper's grant on the registration resource (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L391 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L394 @ ens_v1@91c966f).

| Case | Anchor | `resource_id` | `token_lineage_id` |
| --- | --- | --- | --- |
| Registry-only sub.alice.eth | direct registry | one registry-anchored | none |
| Register alice.eth | registrar lease | one registrar-anchored | mint registrar lineage |
| Wrap alice.eth | wrapper-backed | close registrar binding, open wrapper-anchored | mint wrapper lineage |
| Unwrap before lease ends | same registrar lease | reactivate prior registrar, or establish it at the BaseRegistrar transfer when no prior identity was materialized | reactivate prior registrar lineage, or establish it at that transfer |
| Expiry / grace | unchanged anchor | unchanged | unchanged |
| Registrar release with a retained nonzero registry owner | direct registry fallback | materialize the registry-anchored resource and replacement binding together | none |
| Re-registration after lapse | new registrar lease | mint new | mint new |

This separation captures: one resource under multiple public names, alias-resolved names without direct registry entries, observed wildcard names, and surfaces that rebind across time.

## Normalization and preimage observation

Normalization is version-pinned via `normalizer_version`. The active normalizer is `ensip15@ens-normalize-0.1.1`, backed by the Rust `ens-normalize` crate and its embedded ENSIP-15 data. API input normalization, adapter name-surface admission, reverse-claim claim-name normalization, resolver alias target normalization, DNS-encoded name handling, `namehash`, `labelhashes`, and DNS wire-name derivation all use that one boundary. IDNA/UTS-46 conversion, ASCII lowercasing, trimming, or route-local normalization are not fallback normalizers. Blank or whitespace-only reverse-claim source values are classified as no claim before name normalization; every nonblank reverse-claim source value must pass this ENSIP-15 boundary or surface as `invalid_name`.

The canonical `NameSurface` carries one representative result; alternate spellings persist as immutable preimage observation facts.

`PreimageObserved` facts may come from registrar/registry events with explicit labels, wrapper events with human-readable names, reverse/primary flows that reveal names, and metadata when a manifest allows. Invalid input is never silently coerced into a valid identity.

Across admitted ENSv1, ENSv2, and Basenames resolver-family intake,
every `NameChanged(node, name)` normalizes as `RecordChanged` in the `name`
family.[^v1-namechanged-l10][^v1-namechanged-l18][^v1-revreg-l129][^v1-revreg-l130][^v2-pres-namechanged][^bn-namechanged]
A nonempty `name` also produces preimage observations that can attach
already-observed forward-node facts to a human-readable name; an empty clear
produces no preimage observation.
Regardless of resolver or node type, these rows carry no `primary_claim_source`;
they do not synthesize ownership, resolver selection, or primary-name facts.

For ENSv2, admitted registry, registrar, and resolver name-bearing events produce preimage observations: registry `LabelRegistered`, `LabelReserved`, `ParentUpdated`; registrar `NameRegistered`, `NameRenewed`; resolver `AliasChanged`, `NamedResource`, `NamedTextResource`, `NamedAddrResource`.[^v2-events-l15][^v2-events-l30][^v2-events-l75][^v2-iethreg-l32][^v2-iethreg-l53][^v2-iperm-resolver-l14][^v2-pres-l132][^v2-pres-l142][^v2-pres-l153] These do not write projections or mutate manifest capability state.

## Canonicality, authority, and epochs

- For `ens`, authoritative registration and control come from Ethereum L1. `authority_epoch` is `ens_v1` or `ens_v2` per name and time; it is separate from `resolution_epoch`.
- For `basenames`, authoritative registration and control live on Base.[^bn-readme-l70] The Basenames L1 path is compatibility transport, not a competing authority source.[^bn-readme-l69][^bn-l1resolver-l13]
- Primary names are canonical only when verification succeeds for the requested `coin_type`. Reverse claims alone are insufficient; verification must resolve the claimed name back to the requested address.[^v1-aur-l217][^v1-aur-l226][^v1-aur-l263][^v1-aur-l269]

### ENSv1→ENSv2 current authority

Canonical ENS history may contain both ENSv1 and ENSv2 facts for one logical
name. That history is not itself a conflict. On a deployment profile that
admits the
[ENSv2 migration source family](glossary.md#source-family), a canonical
[ENSv1→ENSv2 migration boundary](glossary.md#migration-boundary) is first
recorded as a candidate. Slice 1 does not change the name's `authority_epoch`,
close its current ENSv1 `SurfaceBinding`, or make the ENSv2 binding eligible for
current selection. Every identity, discovery, and normalized effect that exists
only through the per-name
[migration correlation group](glossary.md#migration-correlation-group) carries
`consumer_visibility=candidate`, including effects interpreted under an
existing source family. Candidate identity and discovery effects live in
separate diagnostic effect rows rather than mutating consumer-authoritative
identity or active-range columns.

Correlation cannot revoke an
[independently admitted event](glossary.md#independently-admitted-event). When
an existing manifest and discovery path already produces one, slice 1 keeps it
byte-for-byte activated and product-visible and records only its
candidate correlation association in a diagnostic association table. Project staging and
product event/history reads exclude correlation-dependent candidate events and
all candidate association/effect tables, not the independently admitted ordinary event;
diagnostics may expose both.

Slice 2A makes this cross-era operation explicit and makes every ordinary
binding close/open arm-scoped. Its transition value carries the exact
`logical_name_id`; block number, transaction index, and log index of the
successful ENSv2 registration; a selector for the expected `ens_v1`
predecessor; and the concrete `ens_v2` successor binding and resource. The
writer resolves the predecessor from current bindings under lock and closes or
retains the two rows in the same transaction. Block timestamp and transaction
membership alone are never boundary ordering. The production interpreter now
waits until every batch correlation path has finished, then passes
[complete groups](glossary.md#complete-group) to the same deterministic activation function exercised by
the code-controlled test seam. There is no runtime or manifest activation flag
and no second evidence-reconstruction path. An authority-boundary group
activates only when exactly one self-sufficient `MigrationApplied` event, one
`surface_binding_transition` effect, one correlation ID, and one existing
ENSv2 successor agree. An incomplete or unresolvable registration refuses only
its own correlation; it does not make complete sibling groups incomplete. A
non-boundary group activates only its existing dependent rows and
never schedules a cross-arm transition. A row shared by several correlation
IDs activates only when every referenced group is complete.

Registrar-token `unwrapped` groups that carry the unlocked controller's ENSv1
registry cleanup currently derive activation but fail the exact-predecessor check while committing it, so no activation rows reach Project. This is the bounded production-path defect
tracked by [issue #822](https://github.com/ensdomains/bigname/issues/822); the
affected catalog rows are enumerated in
[`migration-activation-coverage.md`](migration-activation-coverage.md). The
other authority paths retain the activation behavior described here. A fatal #822 predecessor error rolls
back the whole [physical Interpret batch](glossary.md#batch-grid), including complete sibling groups in that batch.

For a complete group that passes predecessor resolution, production
re-derivation writes its normalized rows with
`consumer_visibility=activated`, activates its diagnostic associations,
and retains the candidate-effect records as the candidate-only diagnostic
source required by the storage contract. It does this
without rewriting their independently admitted events, changes the name's
[`authority_epoch`](glossary.md#authority-epoch) from `ens_v1` to `ens_v2`,
and retains or opens the concrete ENSv2 binding. Child and `unlocked_wrapped`
second-level predecessors close at the exact ENSv1 cleanup recorded by the
boundary; `locked_wrapped` second-level predecessors close at the boundary
position. Registrar-token `unwrapped` predecessors will use that exact-cleanup
rule after issue #822 restores their production path. The unlocked wrapped controller
unwraps before injecting the ENSv2 registration.
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146-L148 @ ens_v2@a971bd64)
If the deployment profile had not materialized the registrar identity before
that unwrap, the exact following BaseRegistrar transfer confirms the fallback
identity while its binding is effective from the preceding `NameUnwrapped`, so
the same cleanup-relative selector remains strict in time.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
Interpret first validates the
one-to-one correspondence between that activated `MigrationApplied` event and
its [migration authority transition](glossary.md#migration-authority-transition).
Project therefore consumes the activated
event's successor binding, resource, position, and correlation ID as an
[`authority proof`](glossary.md#authority-proof); it does not repeat migration
transition correlation or predecessor validation. The selected binding keeps the exact
`surface_bindings.authority_arm` vocabulary: `ens_v1`, `ens_v2`, or
`basenames`.

Only then do later ENSv1 facts for the same migrated name become history that
cannot reopen current authority. An ENSv2 release or unregister leaves
[`released v2 authority`](glossary.md#released-v2-authority) and does not fall
back to the [ENSv1 husk](glossary.md#ensv1-husk). The unlocked controller's
registrar-token path transfers the ENSv1 registry position and registrar token
to the Graveyard before claiming the reserved ENSv2 registration. Its
unlocked-wrapped path first unwraps into the Graveyard, which also transfers the
registrar token there, and then injects the ENSv2 registration. The locked path
instead parks the wrapper token in the Graveyard and registers the name in
ENSv2 while NameWrapper can remain the ENSv1 registry owner.
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L118 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146-L148 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2_sepolia_20260629@ccaeb58)
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L168 @ ens_v2_sepolia_20260629@ccaeb58)

The final activation rotates the
[interpreter content hash](glossary.md#interpreter-content-hash) and requires a
complete retained-range Interpret and Project re-walk before the matching API
is deployed. Project assertions are armed only against activated proofs in the
completed generation or redo range, never against candidate rows, a partial
walk, or mixed interpreter generations. Candidate-versus-activated state is a
replay and acceptance-test distinction, not a production soak interval.

For a second-level `.eth` name, both claim paths request an expiry of zero,
which tells the ENSv2 registry to retain the
[premigration reservation's](glossary.md#premigration-reservation)
expiry. (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L164 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L109 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L447 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L448 @ ens_v2@a971bd64) At the boundary, the selected current `expires_at` therefore changes
from the ENSv1 expiry to the stored ENSv2 reservation expiry only when the
complete boundary activates and Project selects its successor. Interpretation
must use the emitted value rather than reconstructing a fixed delta: the
pinned premigration tool converts a configurable whole-day value to seconds,
defaults it to 62 days, and adds that value to the ENSv1 expiry.
(upstream: .refs/ens_v2/contracts/script/preMigration.ts:L973 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/script/preMigration.ts:L1035 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/script/preMigration.ts:L1265 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/script/preMigration.ts:L1267 @ ens_v2@a971bd64) The separate renewal-bridge constructor receives the
62-day-and-1-second grace-period difference, so that constant is not evidence
for the stored reservation value. (upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L229 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L230 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L231 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L232 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deploy/03_ETHRenewerV1.ts:L38 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deploy/03_ETHRenewerV1.ts:L39 @ ens_v2@a971bd64) This remains an authority change, not an arithmetic reconciliation
rule: after activation, only expiry facts from the current ENSv2 resource can
replace current `expires_at`; later ENSv1 husk expiry or renewal facts remain
history. Candidate facts in slice 1 change neither value.

The replacement authority contract selects authority per logical name, but an
ENSv1 child relation must first remain reachable through its parent's
ENSv1→ENSv2 migration path. The parent rule is:

| Parent state | ENSv1 child-arm eligibility |
| --- | --- |
| No activated `MigrationApplied` | Eligible under the existing live-owner rule. |
| `migration_path = unwrapped` | Ineligible. |
| `migration_path = unlocked_wrapped` | Ineligible. |
| `migration_path = locked_wrapped` | Eligible only for a [migratable child](glossary.md#migratable-child): the label has never had a reserved, registered, or renewed entry in the parent's [migration `WrapperRegistry`](glossary.md#migration-registry-wrapperregistry), its current expiry-effective fuse word has `PARENT_CANNOT_CONTROL` set while `IS_DOT_ETH` is clear, and its current ENSv1 registry owner is nonzero. NameWrapper preserves fuse and expiry data when a child unwraps, so Project uses the latest wrapper resource evidence even when the active ENSv1 binding has rotated. (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L276-L277 @ ens_v1@91c966f) |
| `migration_path = locked_child` | Eligible under the same migratable-child predicate. A locked child receives its own proxy-backed `WrapperRegistry`, so that nested registry becomes the parent migration registry for its descendants. (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L146-L164 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L26-L32 @ ens_v2@a971bd64) |
| `migration_path = emancipated_child` | Ineligible. The child is unwrapped into the Graveyard and no registry is deployed for it; the caller-supplied subregistry is injected with the child instead. (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L178-L188 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L25-L27 @ ens_v2@a971bd64) |

Unlocked ENSv1→ENSv2 migration registers the parent in ENSv2 without deploying a child subregistry, while the Graveyard clears the unreachable ENSv1 descendants. (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L29-L31 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L153-L166 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L96-L102 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L170-L201 @ ens_v2@a971bd64) The two locked paths deploy a `WrapperRegistry` for the migrated name; it routes only migratable children to the ENSv1 resolver and blocks new ENSv2 registration while that condition holds. (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L158-L186 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L293-L307 @ ens_v2@a971bd64) The fuse predicate is exact: `PARENT_CANNOT_CONTROL` must be set and `IS_DOT_ETH` clear. (upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L84-L89 @ ens_v2@a971bd64) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L18-L19 @ ens_v1@91c966f)

Project accepts the current ENSv2 `SubregistryChanged` pointer only when its exact instance and address match a readable canonical `migration_registry_creation` association whose non-empty evidence references are contained in the activated boundary, and its active ordinary announcement. A replacement or empty-evidence association fails closed. `successor_registry_contract_instance_id` instead identifies the registry that received the parent; the locked controller registers the parent there with its new `WrapperRegistry` as subregistry. (upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L103-L109 @ ens_v2@a971bd64)
An entry in that migration registry is historical, not merely current: `RegistrationReserved`, `RegistrationGranted`, and `RegistrationRenewed` each show that `getExpiry(labelId)` has been positive, which makes the child non-migratable even after the entry lapses. Unregistering an entry records the current timestamp as its expiry rather than clearing it. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L195-L207 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L410-L480 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L212-L227 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L293-L307 @ ens_v2@a971bd64) An activated `MigrationApplied` path outside the five values above is a data integrity failure rather than a silently hidden child relation.

Parent reachability filters only the ENSv1 candidate arm. Project then unions it with the ENSv2 candidate, applies the child's selected [authority epoch](glossary.md#authority-epoch), and ranks only within that arm. A released ENSv2 child does not fall back; reachable arms that disagree without proof are omitted. Recency orders only within one arm by block, transaction, log, then stable `event_identity`; generated IDs and cross-era recency never choose the arm.

Both arms stating a relation for the same parent-child pair is not itself the
failure condition. Neither ENSv1→ENSv2 migration branch retracts the ENSv1
registry entry: the locked branch only moves the wrapper token to the Graveyard
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2_sepolia_20260629@ccaeb58),
which reassigns the token holder and writes nothing to the ENSv1 registry
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L301 @ ens_v1@91c966f),
and the emancipated branch unwraps the node into the Graveyard
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L180 @ ens_v2@a971bd64),
which sets a new registry owner rather than clearing the entry
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1029 @ ens_v1@91c966f).
A migrated or positively registered child retains its ENSv1 relation only when
parent reachability admits it. The slice 3B assertion, ordered after
reachability and exact-name integrity, fails a
[projection generation](glossary.md#projection-generation) on Mainnet when a
child with an activated
`migration_authority_transition` has a surviving ENSv1 relation asserted after
its authority epoch began. An unmigrated parent can expose this contradiction;
unwrapped, unlocked-wrapped, and emancipated-child paths cannot. Neither locked
path can expose the `positive_v2_child_registration` form because that
registration is permanent migration-registry entry history, so the
migratable-child predicate filters the ENSv1 relation first. The query retains
both proof kinds defensively, but the positive-proof form is unreachable under
this contract. A surviving contradiction aborts with
`dual_current_child_authority` through the post-rollback audit path below.
Sepolia instead publishes the proof-selected child relation; extending this
guardrail there is deferred until the connected Interpret→Project path is
proven.

The exact-name ownership rule consumes the activated proof. A
name with an activated transition authority proof, or a current ENSv2 child
registration in an admitted migration registry below a proven migrated parent,
is then not unsupported merely because its history contains both ENSv1 and
ENSv2 source families. The second case does not require a child
`MigrationApplied`: the registry permits a registration when the child is not
protected as migratable, and a prior positive ENSv2 expiry makes ENSv2 the
child authority. (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L172 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L175 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L293 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L306 @ ens_v2@a971bd64) The name is supported when the selected current authority's capability is
supported. Project identifies the registry from Interpret's readable canonical
`migration_registry_creation` association; that association only
classifies the independently admitted emitter and is not authority proof by
itself. The first positive registration under that registry establishes the
child's epoch. Later manifest rotation, parent topology changes, or release do
not erase that established epoch; a reorg of its parent proof, registry
association, topology-at-proof, or registration reconstructs the result from
the surviving lineage. The positive registration is an authority proof only: it neither
invents a `MigrationApplied` event nor creates ENSv1→ENSv2 migration history or a binding
transition. Direct-child correlation does derive a child `MigrationApplied`, but only
from a separate and separately evidenced shape — the parent's own migration
registry registering the child into itself, with the parent identified by that
registry's own migration evidence. It starts candidate and changes child state
only after its complete group activates. The two paths do not meet: a bare
positive child registration never becomes a child boundary, and the
positive-registration proof path still invents nothing.
That first ENSv2 registration supersedes the retained ENSv1 child
binding for subsequent current-state selection; releasing it leaves the child
with released v2 authority and does not reactivate the ENSv1 residue.

Project trusts the validated activated transition proof and does not
rank retained binding intervals against the proven arm. Its binding-order
regression fixture directly seeds Project's post-transition input with a closed
predecessor and current successor, proving selection without triggering the intentional
dual-current fatal. The exact-name dual-current
integrity assertion and durable failure audit run alongside the corresponding child
assertion. Those assertions run after transaction-level and then block-level
reconciliation, so a transient state while one ENSv1→ENSv2 migration transaction
cleans up the predecessor and establishes the successor does not fail a
generation. On Mainnet, a name whose
bindings remain current after the applicable proven activated boundary causes Project to abort
before `publish::swap`,
publishes no partial generation, and fails readiness for that target
generation. After the Project transaction rolls back, the phase runner writes a
separate append-only `project_generation_failures` diagnostic audit row with
both binding and resource identities, the boundary event, and the block,
transaction, and log position of each. The assertion examines the names that
generation derives, not the whole chain, so a clean run proves the invariant
only for its own affected scope. On the normal path that is contained: a failed
generation never advances the resume cursor, so the window holding the conflict
is re-derived until it is repaired. An operator redo over a range that excludes
the conflicted name still publishes. Reorgs retain the row and make its stored
block hashes explicitly orphaned through lineage; a later successful generation
does not erase the failure. Neither slice chooses by recency. A mixed Mainnet
corpus with no provable boundary is explicit `unsupported` with
`conflicting_current_ens_authority`; the equivalent Sepolia corpus remains
explicit `unsupported` with `independent_ens_deployments_overlap`. Configured
ingest start blocks that omit proof events are not authority evidence and do
not weaken either refusal. The ENS
root, `eth`, `reverse`, and `addr.reverse` are the four exact
[shared ENS infrastructure](glossary.md#shared-ens-infrastructure) names. When
the ENSv2 arm is current and ENSv1 evidence exists as a current binding or
historical events, they select ENSv2 without fabricating a proof or authority
epoch. Historical ENSv2 evidence without a current ENSv2 binding does not
qualify. The pinned ENSv2 deployment establishes
root, `eth`, and `reverse`. The pinned ENSv1 contract defines `addr.reverse` as
its reverse registrar node, and its deployment assigns that node directly on
testnets; bigname intentionally preserves this exact four-name classification
across configured ENS deployment profiles.
(upstream: .refs/ens_v2/contracts/deploy/00_RootRegistry.ts:L15-L29 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/deploy/01_ETHRegistry.ts:L23-L64 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/deploy/01_ReverseMirror.ts:L13-L34 @ ens_v2@a971bd64)
(upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15-L37 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/deploy/reverseregistrar/00_deploy_reverse_registrar.ts:L30-L48 @ ens_v1@91c966f)
Descendants, including `alice.addr.reverse`, are not exceptions. Existing ENSv1→ENSv2 migration proof, qualifying
release, and deployment-wide ENSv2 release-threshold branches retain precedence.
An ordinary no-proof overlap is refused rather than fatal. A dual-current
contradiction after a proven activated boundary aborts projection generation on
Mainnet; Sepolia publishes the proof-selected ENSv2 state, and extending the
guardrail there is deferred until the connected Interpret→Project path is
proven. Before the exact-name
slice, a corpus containing both families retained the historical
`mixed_exact_name_corpus` product reason. The current per-name rule and its two
reasons are the contracted replacement for that blanket refusal.

Resolver-bearing ENSv2 reservations, their expiry maintenance, and their
release are retained facts, but they do not establish ENSv2 authority.
Premigration can create an
owner-zero reservation or extend an existing reservation's expiry, and the
registry can return that reservation's resolver while it remains unexpired.
(upstream: .refs/ens_v2/contracts/src/registrar/BatchRegistrar.sol:L48-L71 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
The registry permits unregistering either a registered or reserved entry. An
owner-zero reservation release and later registration can reuse the same
resource because neither operation increments its version counters; therefore
only a release whose resource had a matching
[surface binding](glossary.md#surface-name-surface) at or before the release
remains ENSv2 era evidence.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L195-L206 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L428-L471 @ ens_v2@a971bd64)
A real ENSv2 registration or binding still establishes ENSv2 authority.
Therefore a live ENSv1 name plus only a premigration reservation selects ENSv1;
a live ENSv1 name plus an actual ENSv2 registration remains unsupported without
a proven boundary. This does not introduce a recency bridge between independent
deployments: genuine overlap remains unsupported regardless of event age.

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
- `ens_v2_migration_l1` (complete-group production activation)
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

- `ens_v1_wrapper_l1` owns NameWrapper authority, holder facts, direct fuse/expiry observations, wrapper-revealed names, and wrapper-originated resolver/TTL changes on both the Mainnet and Sepolia deployment profiles.[^v1-namewrapper-deploy][^v1-iname-l27][^v1-iname-l35][^v1-iname-l37][^v1-iname-l38][^v1-nw-l240][^v1-nw-l377][^v1-nw-l637][^v1-nw-l666][^v1-nw-l676]
- `ens_v1_resolver_l1` owns the declared PublicResolver address lists in the Mainnet and Sepolia deployment profiles. The schema-v2 project phase classifies an emitter as supported only when its exact address is in the active manifest; that classification permits projection of retained canonical normalized observations but does not prove complete history, authorization semantics, or event-to-call parity. Unlisted emitters are unsupported.[^v1-publicresolver-deploy][^v1-pres-l5][^v1-pres-l13][^v1-pres-l20][^v1-pres-l66][^v1-pres-l114]
- ENS verified resolution belongs to `ens_execution` at the official Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`,[^ens-docs-univ] not to `ens_v1_registry_l1`. The pinned implementation artifact is recorded under `.refs/`.[^v1-ur-deploy][^v1-ursol-l8] (See [`upstream.md`](upstream.md) for the proxy-vs-implementation divergence.)
- ENS reverse-claim intake belongs to `ens_v1_reverse_l1` at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`.[^v1-revreg-deploy][^v1-revreg-l15][^v1-revreg-l19]
- The `ens_v2_migration_l1` family owns fixed ENSv1→ENSv2 migration-controller, Graveyard,
  `ETHRenewerV1`, `VerifiableFactory`, `BatchRegistrar`, and helper admission.
  It also owns launch-bounded correlation of Sepolia BaseRegistrar Graveyard
  claims, bridge renewals, and controller-permission changes; ordinary ENSv1
  registrar authority remains outside this family. (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L8 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L9 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L157 @ ens_v2@a971bd64) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L106 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L134 @ ens_v2_sepolia_20260629@ccaeb58)
  Migration-created registries remain owned by `ens_v2_registry_l1` through
  its existing registry-announcement discovery rule; the ENSv1→ENSv2 migration family
  does not create a second registry authority.
- ENSv1 `.eth` registrar intake belongs to `ens_v1_registrar_l1`. BaseRegistrar is the tokenized authority and sole owner of its `Transfer`, controller-change, numeric registration, and numeric renewal log attribution. On Mainnet, legacy, wrapped, and current registrar-controller contracts are admitted within the same family for label-bearing registration and renewal observations.[^subgraph-l145][^subgraph-l170][^subgraph-l226][^v1-ethrc-l116][^v1-ethrc-l133] On Sepolia, only BaseRegistrar is admitted; ENSv1→ENSv2 migration interpretation consumes its observations cross-family through the launch-bounded rule in [manifest authority](manifests.md#ensv2-migration-family-admission-plan). A renewal from the admitted Mainnet `wrapped_registrar_controller` additionally derives a wrapper-resource expiry observation in this registrar family because that controller calls `NameWrapper.renew`, which stores registrar expiry plus grace without emitting `ExpiryExtended`. (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L318 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L333 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L337 @ ens_v1@91c966f) Label preimage intake is shared storage support rather than a new authority source family: proof-checked on-chain preimage observations, retained name surfaces, and optional rainbow-table imports may resolve labelhashes for projection readability, but they do not create exact-name authority, ownership, resolver, record, or primary-name truth.
- ENSv1 `NewResolver(node, resolver)` changes only the node-to-resolver binding; it creates no resolver contract instance or discovery edge.[^v1-ens-l12][^v1-ensreg-l89][^v1-ensreg-l174] Generic resolver-local logs come from the manifest-declared [all-emitter watch](glossary.md#watch-plan--watched-tuple). Standard approval [intake-only events](glossary.md#intake-only-event) instead use only the exact resolver roles and historical intervals declared for those events. Schema-v2 support classification uses only exact addresses in the active resolver manifest. Code-hash observations are not a classification input. Current record visibility still follows the node's resolver pointer.
- `ENSRegistryOld` is admitted as old-registry [fallback-handoff](glossary.md#registry-fallback-handoff) input under `ens_v1_registry_l1`. Old- and current-registry logs are not unioned by latest block: a current-registry `NewOwner` or `Transfer` establishes a current-registry record; later old-registry updates for that node are suppressed except for the root resolver. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L68 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L82 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L24 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L54 @ ens_v1@91c966f)[^subgraph-l15][^subgraph-l39][^subgraph-l44][^subgraph-ts-l134][^subgraph-ts-l230][^subgraph-ts-l238][^subgraph-ts-l246] The 2017 deployment behind `ENSRegistryOld` emitted 33 mainnet logs (30 `NewResolver`, 2 `NewOwner`, 1 `NewTTL`; blocks 3,800,374–7,460,548, censused from the retained [raw facts](glossary.md#raw-fact) and a chain-wide archive-node log sweep for issue #361) whose data holds a full 32-byte word in the declared address or uint64 slot instead of a zero-padded value — in the first of them the caller passed the node itself as the resolver argument. The current Solidity registry cannot produce such a log because it stores and emits typed `address` and `uint64` values.[^v1-ensreg-l93][^v1-ensreg-l94][^v1-ensreg-l104][^v1-ensreg-l105] The 2017 deployment's own source — the LLL registry pinned under `ens_v1_lll` — shows the setters storing the raw calldata word without masking it to the declared slot width: `setOwner` loads `new-owner` straight from calldata and stores it whole through an unmasked `sstore`,[^v1lll-enslll-l194][^v1lll-enslll-l200][^v1lll-enslll-l73][^v1lll-enslll-l74] and the subnode-owner, resolver, and TTL setters share the same load-then-`sstore` shape.[^v1lll-enslll-l82][^v1lll-enslll-l97][^v1lll-enslll-l112] For `ens_v1_registry_l1`'s `NewOwner`, `NewResolver`, and `Transfer` events with exactly-32-byte data, bigname decodes the address as the word's low 20 bytes, and for `NewTTL` with exactly-32-byte data it validates the TTL as the word's low 8 bytes (`NewTTL` decode is validation only and yields no normalized events): the values on-chain readers receive through the fallback registry's typed reads,[^v1-ensregfallback-l20][^v1-ensregfallback-l31][^v1-ensregfallback-l42] because the deployed fallback bytecode masks the delegated return word to the declared slot width rather than reverting (verified by executing it over an archive node), and the values reference indexers decode for such a log.[^graphnode-eventext-l17][^subgraph-ts-l168] On a strict failure, a retry is attempted only for exactly-32-byte data; the retry succeeds iff the same strict decoder accepts the same topics with the bytes above the declared slot width zeroed; all other inputs preserve the strict decoder's existing result. An owner word that needed the retry names no authenticatable owner: registry authorization checks the caller against the registry's own stored owner record,[^v1-ensreg-l17] and the 2017 source makes the comparison exact: the owner gate loads the stored word whole and jumps to an invalid location whenever the 20-byte caller differs from it,[^v1lll-enslll-l65][^v1lll-enslll-l66][^v1lll-enslll-l119][^v1lll-enslll-l120][^v1lll-enslll-l121][^v1lll-enslll-l30] so an unmasked stored word equals no caller address — corroborating archive-node execution of the deployed bytecode shows owner-gated calls from the low-20 value reverting on the 2017 deployment. A masked `NewOwner`/`Transfer` normalized event therefore records the low-20 value in `owner` with explicit `owner_word_unmasked` and `owner_word_raw` markers. The masked tail never appears in interpreter state, permission grants, effective-controller relations, or `name_current` control, and a prior registry-direct authority closes as it does on a transfer to the zero address. It remains visible in the child row's owner display field and in resolver addresses, exactly as fallback-registry delegated reads return it; the source normalized event always carries the corresponding marker fields. A masked `NewResolver` value keeps its low-20 serving semantics with the same marker pair (`resolver_word_unmasked`, `resolver_word_raw`). `basenames_base_registry` shares the adapter source, but the tolerance is scoped to `ens_v1_registry_l1` and Basenames keeps the strict decode. The 2017 registry's original LLL source is pinned as `ens_v1_lll` (`.refs/ens_v1_lll`) at upstream's `mainnet` tag, and the tag's committed `contracts/ENS.lll.bin` runtime is byte-for-byte identical to the code deployed at `0x314159265dd8dbb310642f98f50c066173c1259b` (verified against archive-node `eth_getCode`), so the cited source is the deployed contract.
- ENSv2 post-audit Sepolia admits five families: `ens_v2_root_l1` (`RootRegistry`), `ens_v2_registry_l1` (`ETHRegistry` plus discovered `UserRegistry`), `ens_v2_registrar_l1` (`ETHRegistrar`), `ens_v2_resolver_l1` (discovered or explicitly admitted `PermissionedResolver` instances), and migration-aware `ens_v2_migration_l1`, whose fixed sources, complete-group activation rule, and schema prerequisite are specified in [manifest authority](manifests.md#ensv2-migration-family-admission-plan). `PermissionedResolverImpl` is implementation metadata, not a watched root or contract.[^v2-deploy-root][^v2-deploy-ethreg][^v2-deploy-ethrc][^v2-deploy-pres][^v2-userreg-l15][^v2-ethrc-l30][^v2-ethrc-l151] The same deployment profile additionally admits four ENSv1 families — `ens_v1_registry_l1` (the current Sepolia registry plus the superseded registry it falls back to), `ens_v1_registrar_l1` (BaseRegistrar only), `ens_v1_wrapper_l1` (NameWrapper), and `ens_v1_resolver_l1` (the ruled four PublicResolver generations) — which are the ENSv1 deployment the ENSv1→ENSv2 migration family bridges from. Registry `NewResolver` events still create neither resolver instances nor discovery edges; the resolver family's declared contracts and signatures now supply the direct watch and classification surface for resolver-local logs. BaseRegistrar raw-log attribution belongs only to `ens_v1_registrar_l1`; observations used only for incomplete or refused ENSv1→ENSv2 migration correlation remain launch-bounded candidates, while an ordinary `Transfer` can materialize missing predecessor and fallback identity. See [manifest authority](manifests.md#ensv1-sepolia-deployment-profile). No other artifact of the admitted 2026-06-29 Sepolia deployment is admitted until a doc-first update, and upstream's 2026-07-30 Sepolia redeploy is not admitted at all (upstream: .refs/ens_v2/contracts/deployments/sepolia/.deployment.json:L4 @ ens_v2@a971bd64); see [`upstream.md` § Known divergences](upstream.md#known-divergences).
- ENSv2 `exact_name_profile` capability support is only promoted — a [capability promotion](glossary.md) — in the post-audit Sepolia deployment profile when `ens_v2_registrar_l1` declares `exact_name_profile = "supported"`. The same deployment profile's incomplete ENSv1 registrar-controller coverage remains `shadow`, so the namespace-level product summary is `partial` while the ENSv2 family stays supported. Other deployment profiles or capability states stay unsupported or shadow.
- Basenames mainnet authority splits across `basenames_base_registry` (`registry` at `0xb94704422c2a1e396835a571837aa5ae53285a95`), `basenames_base_registrar` (`registrar` at `0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a`, with `legacy_registrar_controller` at `0x4cCb0BB02FCABA27e82a56646E81d8c5bC4119a5` and `upgradeable_registrar_controller` proxy at `0xa7d2607c6BD39Ae9521e514026CBB078405Ab322` admitted for label-bearing registration and renewal observations), and `basenames_base_resolver` (`resolver` at `0xC6d566A56A1aFf6508b41f6c90ff131615583BCD`).[^bn-readme-l28][^bn-readme-l29][^bn-readme-l30][^bn-readme-l34][^bn-readme-l37][^bn-registry-l10][^bn-baseregistrar-l15][^bn-registrar-controller-l180][^bn-registrar-controller-l187][^bn-upgradeable-registrar-controller-l191][^bn-upgradeable-registrar-controller-l198][^bn-l2resolver-l22] `basenames_base_primary` uses the ENSv1 Base `L2ReverseRegistrar` at `0x0000000000D8e504002cC26E3Ec46D81971C1664` for declared primary-name value intake at Base coin type `2147492101`; the Basenames `ReverseRegistrar` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282` is not the primary-name value authority.[^v1-l2rev-base-deploy][^v1-l2rev-base-args][^v1-l2rev-event][^v1-l2rev-nameforaddr][^bn-readme-l33][^bn-revreg-l12][^bn-revreg-l150] `basenames_l1_compat` and `basenames_execution` both reference the L1 Resolver at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` for transport and execution respectively.[^bn-readme-l22][^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]
- Basenames `NewResolver` changes only the node-to-resolver binding; it creates no resolver contract instance or discovery edge.[^bn-registry-l19][^bn-registry-l132][^bn-registry-l223] The generic Base resolver signature set selects resolver-local logs across all emitters; standard approval [intake-only events](glossary.md#intake-only-event) are watched only at the exact resolver roles and historical intervals declared for them. Schema-v2 support classification requires the emitter's exact address in the active `basenames_base_resolver` manifest. Code-hash observations are not a classification input.[^bn-l2resolver-l4][^bn-l2resolver-l16][^bn-l2resolver-l29][^bn-l2resolver-l182][^bn-l2resolver-l193][^bn-l2resolver-l209][^bn-l2resolver-l225]

## Source manifests

Manifests pin each [source family](glossary.md) by version and live under a selected deployment-profile root at `manifests/<profile>/<chain_combo>/<namespace>/<source_family>/<version>.toml`. The shipped runtime default is `manifests/mainnet/`; the Sepolia profile root is `manifests/sepolia/`. One runtime selects exactly one profile root.

Each manifest contains: `manifest_version`, `namespace`, `source_family`, `chain`, `deployment_epoch`, `rollout_status` (`draft` | `shadow` | `active` | `deprecated`), `normalizer_version`, optional `resolver_implementations`, optional `correlation_addresses`, `capability_flags` (`unsupported` | `shadow` | `supported`), `roots`, `contracts`, `discovery_rules`. `resolver_implementations` declares the implementation addresses that canonical ERC-1967 upgrade history may classify for ENSv2; it does not create watch targets. `correlation_addresses` is a map of named, validated EVM addresses used only to correlate decoded observations across declared emitters; its entries do not declare contracts, add discovery edges, or widen the watch plan. `start_block` is optional inclusive bootstrap metadata; omission preserves unknown deployment provenance as null. Runtime watch and Interpret selection use block zero as the conservative lower bound for that admitted target without claiming a genesis deployment.

Manifest declaration changes are first-class `SourceManifestUpdated` normalized events. Proxy declarations and authored capability fields are part of that source-manifest state; the schema does not mint separate manifest-change event kinds for them.

Rules:

- A contract is indexable when an active manifest declares it, an admitted creation event announces it, or an allowed discovery edge makes it reachable from a canonical root. Announcement admission alone does not confer parent or name authority.
- Re-declaring the same address mints no new instance — it appends a new active range.
- When one address has multiple active manifest roles, interpretation follows the
  [selection behavior documented with contract instance admission](manifests.md#admission-selection-for-addresses-with-multiple-declared-roles).
- Declared proxy implementations resolve to separate `contract_instance_id` nodes; implementation changes update the proxy/implementation edge, not the proxy identity.
- Capability ownership attaches to the declaring `source_family` only.
- Draft features may sit behind manifest flags without changing the public contract.

Schema, capability ownership detail, and the discovery edge model are in [`manifests.md`](manifests.md).

## Discovery graph

Discovery expands the canonical graph through time-versioned indexability and relationship edges. The schema-v2 baseline constrains `edge_kind` to exactly five values: `resolver`, `subregistry`, `proxy_implementation`, `registry_announcement`, and `migration`. Four of the five have producers; nothing writes `migration`, which is [reserved surface](glossary.md#reserved-surface). (The legacy `public` schema built from `migrations/` never constrained the column, so historical rows there are not bounded by this list.) Each edge stores `edge_id`, `from_contract_instance_id`, `to_contract_instance_id`, `discovered_by`, `edge_kind`, `active_from`, `active_to`, provenance, and canonicality.

ENSv2 mappings:

- `RegistryCreated()` → normalized `RegistryCreated` and a registry-announcement instance admission at the emitting address. The admission does not require a parent link. For a registry created through the [ENSv1→ENSv2 migration family](manifests.md#ensv2-migration-family-admission-plan), rule ownership remains with `registry_announcement`: the normalized event and indexability edge remain ordinary, and the watch plan traverses the edge from that log position. A separate `migration_registry_creation` association attaches to each; it does not create a suffix, parent relation, name binding, or current authority. Correlation-dependent downstream identity, parent, role, registration, renewal, topology, and normalized rows activate only after all groups they reference are complete; incomplete or refused groups retain candidate output. Association with the migration group is not by itself enough to reclassify independently admitted output: anything that `ens_v2_registry_l1` derives from the ordinary edge and raw event without the association remains ordinary and byte-for-byte unchanged. When an address admitted by a registry announcement or resolver-discovery edge also has an applicable exact contract declaration in an active manifest for the same namespace, interpretation uses the declaring manifest; otherwise it uses the manifest inferred from the admission. This precedence changes raw-log adapter selection only; the announcement or discovery edge and its normalized topology or pointer effects remain intact. A direct `PermissionedRegistry` emits this event during construction. (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@a971bd64)
- `SubregistryUpdated(tokenId, subregistry, sender)` → normalized
  `SubregistryChanged` and the parent-child reachability edge. It is the source
  of the pointer and relationship truth; defensive `TokenRegenerated`
  interpretation reasserts the migrating token's retained edge under the
  successor token's `observation_key`. When the predecessor key is different
  and remains shared by another retained holder, interpretation also reasserts
  that key to the deterministically selected holder's target. The edge does not
  decide whether the child registry instance is
  indexable.[^v2-events-l49][^v2-pr-l131][^v2-pr-l222]
- `ParentUpdated(parent, label, sender)` → normalized `ParentChanged` contract history. Manifest-declared `RootRegistry` and `ETHRegistry` instances are suffix anchors; every registry below those anchors has a registry-name suffix only while both current sides agree: the child's latest claim names `(parent, label)`, and that parent's latest unexpired `SubregistryUpdated` pointer for `label` leads back to the child. Either side changing, clearing, or expiring retracts the binding. A suffix move closes and releases each old logical-name binding, then opens and grants a distinct binding epoch under the new reachable suffix; the underlying registry resource remains the same, and its current resolver and subregistry pointers are restated under the new logical name. `ParentUpdated` does not create parent-child reachability; `SubregistryUpdated` remains its only source. Replay retains both current sides even while an intermediate registry has no reachable name, and later descendant events recheck the complete bidirectional, unexpired ancestor path. The child's `setParent` call writes its parent and label atomically, independently of the parent's subregistry pointer. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L169 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L173 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L174 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L175 @ ens_v2@a971bd64) Canonical validation reads the child's current claim and rejects it unless the parent's current pointer leads back to the child. (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L91 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L95 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L96 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L97 @ ens_v2@a971bd64) Upstream stops that walk only at the supplied `RootRegistry`; treating the manifest-declared `ETHRegistry` as an additional suffix anchor is the documented ENSv2 cutover divergence. (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L87 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L88 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L89 @ ens_v2@a971bd64) See [`upstream.md` § Known divergences](upstream.md#known-divergences). An expired parent label makes `getSubregistry` return zero at the event timestamp. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L249 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L251 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L628 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L629 @ ens_v2@a971bd64)
Registry-name suffix labels are retained verbatim. Raw label text keys the live topology maps, so the current parent pointer and child claim must agree on the same raw label. A raw-distinct label path has a distinct namehash identity; if any label does not byte-equal its ENSIP-15 normalized result, that identity remains a shadow and cannot open a current binding. Thus labels such as `Foo` and `foo` retain distinct preimages and namehash identities, while only the normalization-gate-passing identity can bind.

- `ResolverUpdated(tokenId, resolver, sender)` → updates the resolver edge for
  the current registry resource and is the source of its resolver target;
  defensive `TokenRegenerated` interpretation leaves the survivor's resolver
  edge under its existing `observation_key` and records that prior key on the
  successor token. The next explicit resolver update or terminal token event
  closes both the current key and recorded prior keys, except for any key currently
  shared by another live token with resolver state. Regeneration never
  reopens a retained resolver edge, because another accepted noncanonical token
  ID could already have retired that key and its address-scoped logs would then
  be incomplete. Discovered-only resolver endpoints interpret under
  `ens_v2_resolver_l1`; an applicable exact declaration in the same namespace
  controls raw-log interpretation without changing the originating ENSv2
  `ResolverUpdated` event, resolver binding, or discovery edge.[^v2-events-l59][^v2-pr-l141][^v2-pr-l225]

Project applies the same declaration precedence when it classifies an active
resolver-discovery admission for serving. An applicable exact resolver
declaration in the same namespace has classification rank 0, ahead of the
original discovery admission at rank 1. This changes only the address's
Project family classification: it does not remove or rewrite the discovery
edge, the ENSv2-origin `ResolverChanged` event, its `logical_name_id`, or their
provenance. When one manifest has repeated applicable declarations for the
address, Project follows the [manifest field rule](manifests.md#required-fields):
the greatest `start_block` at or below the target wins, and equal starts select
the later manifest-array entry. Resolver classification is address-level, so
once the address is supported as `ens_v1_resolver_l1`, every current pointer to
that address uses that classification. A pointer may consume a
`manifest_declared_address` classification only when its namespace matches the
classifying manifest's namespace; otherwise its inventory is explicitly
unsupported with `resolver_classification_missing`, regardless of the pointer
family. Project attributes a node-keyed ENSv1 record through an ENSv2-origin
pointer only when the pointer target's final staged classification is supported
`ens_v1_resolver_l1` from an applicable exact declaration. Without that
declaration, the safe serving result remains explicit `unsupported`; this
precedence restores availability and completeness rather than correcting
previously served values.

[Watch-plan](glossary.md) expansion includes manifest-declared instances, registry-announcement admissions, resolver admissions, and proxy/implementation targets. Announcement admission is forward-only from the observed event; bootstrap can discover earlier announcements only back to the configured ingest coverage start and therefore assumes that coverage reaches the deployment. A `subregistry` edge changes parent-child topology only; it never admits the child emitter. Match-all signature scopes are manifest event subscriptions rather than address rows.

## Intake architecture

Three intake planes for one selected deployment profile:

- Ethereum L1 chain intake
- Base chain intake
- execution intake (verified reads, CCIP)

Per-profile provider availability: a Base RPC is not required for an Ethereum-only run, and a deployment profile with no Base provider must mark Base intake idle/unavailable rather than failing startup.

Phases per chain:

1. `ingest` — block lineage, selected transactions/receipts/logs, and raw-fact
   persistence
2. `interpret` — schema-v2 identity, discovery, and normalized-event writes,
   plus pre-delete resolver-reference coordination for Project redo
3. `project` — canonical identity and normalized-event input, staged current
   projections, one-transaction publication for the affected scope, and
   canonical-head [hydration](glossary.md#hydration) after publication
4. `verify` — read-only [stored-history verification](glossary.md#stored-history-verification)
   through a finalized boundary; Base
   can compare its Coinbase-loaded range with a distinct dRPC through the ingest
   seam, Ethereum Mainnet can compare with a distinct local reth, and Ethereum
   Sepolia can compare with a distinct [verification-only](glossary.md#source-role)
   dRPC or record its [provider-trusted](glossary.md#verification-level)
   ingested extent without an independent reference
5. `live` — continuous provider-head walk, bounded gap fill, chain-head
   publication, and downstream re-derivation after a reorg

Postgres is the hot indexed and replay-focused store. Lineage anchors plus
selected transactions, receipts, target logs, and same-transaction sibling
replay context are durable. The phase schema has no generic provider-payload
cache or retained call-snapshot family. Empty historical blocks retain only
lineage anchors and audit metadata.

The phase runner persists exact per-source and per-phase block-hash cursors.
Historical work is an explicit finite `ingest`, `interpret`, `project`, or
`verify` redo. The old persisted backfill scheduler, coverage frontier, adapter
startup pass, and normalized-event replay driver have been deleted. A newly
admitted source returns to `ingest` for the required range, and `interpret`
cannot advance past the ingested boundary.

### Stage B runtime boundary

The checked-in phase runner contains real `ingest`, `interpret`, `project`,
`verify`, and `live` implementations. Reference-comparison verification reads
canonical selected raw logs and the manifest-derived watch set through a
separately credentialed, SELECT-only database handle. Provider-trusted verification requires the
chain-policy-selected intake cursor to cover the finalized target. Startup requires the reader login to be directly
authenticated (the session user and active role must match) and rejects one that has
application-relation write privileges, schema/database creation authority,
elevated role attributes, or another role membership; the verifier never
receives the phase runner's writer pool. The reader and writer connections must
also report the same PostgreSQL system identifier, database OID, and database
name. With a distinct verification-only
Base dRPC, Verify compares the Coinbase-loaded range through the fixed block
`48,428,000` ingest seam and records `cross_checked`; the later dRPC-ingested
suffix does not inherit that level. Without that reference, Base records
`quick_synced` from its target-covering intake dRPC. A
Base `reth_db` reference is explicitly unsupported: the pinned reader uses
reth's Ethereum node type, whose signed transaction and receipt types are the
Ethereum primitives (upstream: .refs/reth/crates/ethereum/node/src/node.rs:L121 @ reth@88505c7f)
(upstream: .refs/reth/crates/ethereum/primitives/src/lib.rs:L27 @ reth@88505c7f)
(upstream: .refs/reth/crates/ethereum/primitives/src/lib.rs:L51 @ reth@88505c7f). Bigname does not
implement a separate OP Stack transaction and receipt reader.
Base-aware local database verification is tracked by
[issue #433](https://github.com/ensdomains/bigname/issues/433). Under that
enforcement, Ethereum Mainnet records `node_checked` with a distinct
verification-only local reth and `quick_synced` from its intake reth without
one. Ethereum Sepolia records `cross_checked` when a distinct verification-only
dRPC compares its durable ingested extent through the finalized marker. Without
that source it records `quick_synced` from the target-covering intake cursor and
never compares the intake provider with itself. The runner rejects a verification
level stronger than the chain-specific verification path can earn before persisting
the level or proceeding to Live. On a reference-comparison path, a
mismatch records its block, field, stored value, and reference value, then stops
only that chain. Normal verification starts at the durable ingest-cursor extent,
not a replacement command-line start, and a resumed scan retains the weaker of
its prior whole-extent level and the current reference's level.
The project phase is the single schema-v2 projection writer and has no claim
queue, dead-letter referee, heartbeat threading, or separate background
planner. When a hydration RPC is configured, the same project run refreshes
eligible Ethereum legacy reverse-name and text values at the exact published
canonical head after its event-derived projection work. Bounded reverse-name
polling keeps per-row attempted-head and attempt-order values so a failed page
cannot occupy every same-head retry. Those internal values are not serving
state; an affected projection rebuild clears them and selects the rebuilt tuple
from the event-derived delta. A redo whose event-derived publication target is
behind the canonical head defers polling until project catches up.

Current ingest, interpretation, projection, live follow, redo, and rewind
boundaries are described in [`chain-intake.md`](chain-intake.md).

## Immutable facts and rebuildable state

Immutable schema-v2 raw facts are lineage rows, selected transactions,
receipts, logs, and preimage observations. Provider calls used for hydration or
request-scoped lookup do not become raw facts.

Interpretation output is replay-derived: schema-v2 identity rows, discovery
edges, and normalized events can be replaced by an explicit bounded
`interpret` redo while raw facts remain unchanged. Current name, binding,
authority, control, permissions, resolver, record, primary-name, reverse,
address, history, and coverage projections remain rebuildable by the project
phase from canonical identity and normalized-event input. Canonical-head
hydration values are execution-derived current-state enrichment layered into
`record_inventory_current` and `primary_names_current` only after that
rebuildable event-derived publication; they are never raw facts, identity rows,
or normalized events.
Provider lookup responses are request-scoped and are not persisted as reusable
outcomes or durable execution traces. Guarded resolution disagreements may be
recorded in the [resolution divergence ledger](glossary.md#resolution-divergence-ledger).

Every projected row carries provenance pointers, manifest version, canonicality state, and chain-position context.

### ENSv2 expiry retirement

An ENSv2 registry token stops contributing to every current surface when an interpreted block timestamp reaches its retained expiry. At `now >= expiry`, Interpret closes the token's current name and registration or reservation, current ownership, resolver, subregistry, and every descendant connection that depends on that registry path. Expired-but-unreleased entries remain available only through normalized events and historical identity, resource, and token lineage surfaces. ENSv1's expired-but-still-owned presentation does not apply to ENSv2: the ENSv2 registry returns no subregistry, resolver, or owner for an expired entry. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L249-L258 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L343-L354 @ ens_v2@a971bd64)

For retained entries, retirement runs before the first raw log of the first interpreted block whose timestamp reaches the expiry. An ordered retained-expiry index selects only tokens that crossed the expiry boundary, and Interpret materializes the affected descendants as ordinary normalized lifecycle and pointer-clearing deltas. Wall-clock time, physical batch boundaries, read-time expiry filters, and per-read ancestor walks do not participate. Premigration reservations use the same rule: expiry removes the reservation and its mirror resolver from current surfaces without creating an ENSv2 authority binding.

An ownerless reservation may also be observed with a nonzero expiry already at or before its event block timestamp. Interpret retains the raw reservation and then emits its state-derived release in the same block with block-only provenance and no invented transaction or log position. Normalized output retains the release after the reservation, but public history does not infer an intra-block raw-log position. It never enters current state. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L452-L465 @ ens_v2@a971bd64)

Interpret and Project may land as separate deployment slices, but this combined [interpreter content hash](glossary.md#interpreter-content-hash) rollout is not serving-eligible until both are present and the retained range has been interpreted and projected coherently. Deployments must not serve the newly interpreted range between those slices.

Expiry retirement is computed and non-destructive. Interpret retains the token, registration or reservation payload, resource and token lineage, resolver, subregistry, and descendant topology. Any later observed renewal of the same token ID can therefore revive the same registration and reconnect its retained
subtree; bigname assumes no grace duration. Registry revival is role-gated but
has no time-bound check, while the grace period belongs to registrar policy.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L212-L227 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L600-L611 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L78 @ ens_v2@a971bd64)

A version-bumped token ID instead defines a new registration. ENSv2 keeps
independent permission-resource and token version bits, increments both when a
previously owned entry is re-registered, and writes the new resolver and
subregistry from that registration call. Interpret does not copy pointers from
the expired registration. A new registration may explicitly reuse the previous
child registry and thereby reconnect that registry's retained subtree.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L29-L32 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L455-L460 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L463 @ ens_v2@a971bd64)

## Internal domain model

Core objects: `NameSurface`, `SurfaceBinding`, `BackingResource`, `NameClass`, `RegistrationSnapshot`, `AuthoritySnapshot`, `ControlVector`, `PermissionSnapshot`, `ResolutionTopology`, `RecordInventory`, `RecordCache`, `PrimaryNameSnapshot`, `SourceProvenance`, `CoverageSnapshot`, `TokenLineage`, `ExecutionResult`.

`ControlVector` is never a single owner field. It carries `token_holder`, `registrant`, `effective_controller`, `record_manager`, `delegates`, `reverse_manager`, `resolved_address_target`, `status`, `expiry`, `authority_epoch`, `resolution_epoch`.

`Registration.kind`: `lease`, `subname_assignment`, `reservation`, `dns_control`, `offchain_policy`, `observed_only`.

Permissions and control are anchored to `resource_id`, never to surface text. The chain `logical_name_id → SurfaceBinding → resource_id → token_lineage` must remain reconstructible through time.

## Normalized event taxonomy

Identity, preimage, discovery, and contract history: `PreimageObserved`,
`SurfaceBound`, `SurfaceUnbound`, `ContractDiscovered`, `RegistryCreated`,
`Upgraded`, `SourceManifestUpdated`.

Registration and authority: `RegistrationReserved`, `RegistrationGranted`, `RegistrarNameRegistered`, `RegistrationRenewed`, `RegistrationReleased`, `ExpiryChanged`, `AuthorityTransferred`, `AuthorityEpochChanged`, `MigrationApplied`.

For a version-zero initial `RegistrationReserved`, the emitted token ID also
identifies the ENSv2 registry-entry resource, so interpretation materializes the
stable resource and token-lineage identities without a token mint or
`SurfaceBinding`. Resolver and [emitted-expiry](glossary.md#emitted-expiry)
observations may refer to that resource before a claim, but they do not become
registration or authority facts.
A successful claim retains those identities and copies the expiry emitted by
`LabelRegistered`; its later `TokenResource` confirms the retained resource and
can bind the name. A reservation with nonzero version bits remains reservation
evidence without an invented resource because token and EAC resource versions
can differ. Upstream stores independent token and EAC versions, writes them into
the lower 32 bits, emits `LabelReserved` for the ownerless state, copies the
stored expiry when a claim supplies zero, and emits `TokenResource` only after
the registered token mint.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L25-L34 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L428-L471 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L474-L478 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L632-L650 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/utils/LibLabel.sol:L11-L17 @ ens_v2@a971bd64)
Interpretation never derives an expiry from a grace period, renewal duration, or
cross-version offset.

An ENSv1 BaseRegistrar `NameRegistered` naming the declared Graveyard with the
exact terminal expiry emitted by `clear` normalizes only as
[`graveyard_cleanup`](glossary.md#graveyard-cleanup) historical evidence. It
creates no registration, lease, backing resource, token lineage, wrapped state,
authority transition, or surface binding.
(upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L157-L169 @ ens_v2@a971bd64)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L142-L154 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)

Lineage and control: `TokenResourceLinked`, `TokenRegenerated`, `TokenControlTransferred`.

Topology and resolution: `ResolverChanged`, `SubregistryChanged`, `ParentChanged`, `AliasChanged`, `RecordChanged`, `RecordVersionChanged`.

Permissions: `AccountPermissionChanged`, `PermissionChanged`,
`RootPermissionChanged`, `PermissionScopeChanged`.

Reverse and primary: `ReverseChanged`.

ENSv2 mappings:

- ENSv2 registry `RegistrationGranted`, `RegistrationRenewed`, and
  `RegistrationReleased` payloads always identify the emitting registry with
  `registry_contract_instance_id`. A direct `unregister` emits
  `LabelUnregistered` from that registry, so its normalized release carries the
  same identity as the corresponding grant or renewal. If an admitted
  noncanonical registry regenerates a token onto a token key already occupied
  by another registration, interpretation also emits
  `RegistrationReleased` for the displaced registration, with
  `source_event=TokenRegenerated` and
  `terminal_reason=registry_name_binding_changed`. Its `token_id` is the
  destination token ID now occupied by the surviving registration. Consumers
  select a release by `(logical_name_id, resource_id)`, not by `token_id`, and
  retained-state restoration applies the preceding `TokenRegenerated` key
  replacement while treating this terminal-reason release as a boundary rather
  than a second token-state mutation.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L195 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L199 @ ens_v2@a971bd64)
- The `MigrationApplied` kind records an [ENSv1→ENSv2
  migration boundary](glossary.md#migration-boundary). It is derived
  from the complete admitted per-name transaction shape at the successful ENSv2
  `LabelRegistered` position, not from source-family coexistence, Graveyard
  ownership, or transaction membership alone. For a `.eth` second-level name,
  the declared unlocked or locked ENSv1→ENSv2 migration controller claims an
  existing reservation through `register(..., expiry = 0)`.
  (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L152 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L164 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L89 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L110 @ ens_v2@a971bd64) For a child, the already-discovered migration registry receives the
  wrapper transfer and registers that child in itself. (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L168 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L188 @ ens_v2@a971bd64) A complete child group derives an activated `MigrationApplied` boundary and its correlation-dependent normalized rows only with the child's own ENSv1 predecessor cleanup earlier in the same transaction — its wrapper token parked in the Graveyard, or its node unwrapped into the Graveyard — so a self-claim without that cleanup derives nothing. (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L180 @ ens_v2@a971bd64) Production and the explicit test seam use the same activation function and recorded child selector. Reverted transactions produce no boundary.
- `MigrationApplied` is self-sufficient. Its payload identifies
  `logical_name_id` and namehash; `correlation_kind=authority_transition`;
  `migration_path` as `unwrapped`, `unlocked_wrapped`, or `locked_wrapped` for a
  controller-mediated `.eth` second-level name, or `locked_child` or
  `emancipated_child` for a direct child registered by its parent's own
  migration registry
  (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L149 @ ens_v2_sepolia_20260629@ccaeb58)
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L188 @ ens_v2@a971bd64);
  predecessor binding, resource, and
  `ens_v1` authority epoch; successor binding, resource, and `ens_v2` authority
  epoch; successor registry contract instance; the successful v2 registration
  block, transaction, and log position; the decoded stored expiry; the complete
  normalized and raw evidence set; its one-element
  `migration_correlation_ids` set; and
  `consumer_visibility` as `candidate` or `activated`. The diagnostic
  `candidate_authority_transition` marker is `true` only for the candidate
  form and becomes `false` when that same boundary activates. Interpretation emits
  exactly one event per authority transition. Duplicate companion logs do not
  create another boundary, and replay under one fixed manifest set and
  interpreter content hash reproduces the same event identity and payload. A
  candidate event performs no `SurfaceBinding` transition. Slice 2A defined and
  tested the explicit activated transition operation; the final activation
  slice now invokes that same operation in production for a complete group.
  An activated boundary remains `MigrationApplied`; it never
  reuses a `PreimageObserved` row carrying the `arm_wide_binding_close`,
  `closed_authority_arm`, and `surface_binding_id` marker tuple reserved for an
  ENSv2 same-arm survivor reassertion.
- [Synchronized renewal](glossary.md#synchronized-renewal) interpretation preserves separate bridge, ENSv1
  registrar, and ENSv2 registry normalized rows. A resource-bearing registry
  observation retains its derived resource and the bridge uses that resource;
  when the reserved registry resource cannot be derived, the registry and bridge
  observations remain resource-less. Interpretation never collapses a
  transaction into one synthetic renewal.
- `TokenResourceLinked` ← upstream `TokenResource(tokenId, resource)`. The only adapter event linking current token ID to upstream EAC resource.[^v2-iperm-l34][^v2-pr-l216]
- `TokenRegenerated` ← upstream `TokenRegenerated(oldTokenId, newTokenId)`. The
  ordinary canonical path preserves `resource_id`, `token_lineage_id`, and the
  active surface binding.[^v2-events-l69][^v2-pr-l451] If an admitted
  noncanonical registry regenerates onto an occupied destination token key,
  interpretation preserves the regenerated registration and closes the
  displaced registration's surface binding and unshared resolver and
  subregistry discovery edges, plus any child registrations that lose their
  registry path. When `LabelRegistered`, `LabelReserved`, `LabelUnregistered`,
  or `TokenRegenerated` would retire a shared subregistry `observation_key`,
  interpretation reasserts that key to the stored subregistry target of the
  greatest fixed-width lowercase token ID that still has a subregistry pointer
  in interpreter state; otherwise the key closes. This includes registration or
  reservation under an observation key formed by clearing the token ID's low 32
  version bits, even when no exact token ID was displaced. Expiry retracts name
  reachability but does not discard that stored pointer or its discovery
  coverage. When the selected target differs,
  materialization caps the prior edge under the shared key; a repeated edge
  assertion to the same target is deduplicated and stays continuously active.
  When an occupied `TokenRegenerated` destination has the same observation key
  as its predecessor, interpretation atomically closes and reasserts that key at
  the regeneration log position. When an occupied
  regeneration also moves a stored subregistry pointer onto that destination
  key, its single successor edge performs the cap instead of emitting a second
  survivor edge at the same log position. The
  surviving registration's subregistry observation moves to the destination
  token's key. When the survivor has resolver state, its
  resolver observation stays continuously active under the source token's key.
  That prior key is recorded on the successor so the next explicit resolver
  update or terminal token event retires it unless another live token still uses
  or retains the same resolver `observation_key`. A resolverless survivor retains
  no source-key observation, so an unshared displaced resolver edge closes at
  regeneration. The normalized event's `resolver_discovery_aliases` records the
  complete retained-key set so compacted restore preserves that protection.
  That restored-convergence guarantee covers the resolver aliases only: a
  noncanonical registry that repeats a regeneration for the same old token ID
  onto an occupied destination can compact away the intermediate transition,
  and restore then rebuilds the displaced registration identity as the
  survivor. That corner is explicitly unsupported and tracked in #596.

  The four lifecycle events above remove or replace a token without supplying
  the surviving subregistry value, so they may reassert a retained token's
  pointer. A zero-address `SubregistryUpdated` instead supplies the canonical
  entry's value. ENSv2 resolves every versioned ID to that entry, reconstructs
  the current token ID for the emitted update, and reads the subregistry from
  the same entry. Interpretation therefore closes the observation key and
  clears every retained per-token pointer under the emitting registry and token
  key after masking its low 32 version bits. A later lifecycle event cannot
  reassert those former pointers; only a later nonzero `SubregistryUpdated`
  reopens the child topology. Subregistry discovery edges are topology-only and
  do not alter the [watch plan](glossary.md#watch-plan--watched-tuple); while the
  pointer is zero, exact ENSv2 resolution has no child registry through which to
  continue. The normalized zero event's `subregistry_invalidated_token_ids`
  records the emitting token ID and the other token IDs whose pointers it
  cleared. [Cold restore](storage.md#interpret-process-memory) retains that zero
  separately from an ordinary later value for the same
  [interpreter state key](glossary.md#interpreter-state-key), preserving both
  the clear evidence and the logical before/after event stream.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L25-L33 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L63-L73 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L142-L146 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L249-L252 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L595-L598 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L614-L625 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L648-L650 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L18-L44 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L78-L82 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L475 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/utils/LibLabel.sol:L11-L16 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L21-L42 @ ens_v2@a971bd64)

  A resolver `observation_key` carries at most one live discovery edge: before
  asserting an edge to a different target, discovery closes the existing edge
  for the same contract instance, edge kind, and observation key; it deduplicates
  a repeated assertion to the same target. The retained-key protection governs
  retirement when the successor itself next updates its resolver or terminates,
  and is also consulted when another token sharing the key terminates; it does
  not make the shared key immune to an explicit resolver update or clear by a
  different token whose ID matches after the low 32-bit version is cleared. Such
  a collision requires a noncanonical emitter, because the canonical registry
  derives the version-cleared token ID from the label and regeneration changes
  only the low 32-bit version after burning the predecessor
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L426-L429 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L531-L541 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L648-L650 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/utils/LibLabel.sol:L15-L17 @ ens_v2@a971bd64).
  The collision therefore remains scoped to token state and discovery edges
  keyed to the emitting registry address.

  A resolver key shared by a resolver-holding survivor and the displaced
  registration is not closed at the collision. Regeneration never reopens a resolver edge from retained
  state, so interpretation cannot claim historical address coverage that
  Ingest did not load. The
  canonical registry burns the old token, increments the entry's version,
  constructs the successor token ID, and mints it
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L531-L541 @ ens_v2@a971bd64);
  minting an already-owned singleton ID fails the update's owner check
  (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L182-L203 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L305-L319 @ ens_v2@a971bd64).
- `TokenControlTransferred` ← each positive-value item in upstream ERC-1155 `TransferSingle` or `TransferBatch` when both `from` and `to` are nonzero. A batch item produces its own normalized event. The upstream update changes the current owner only for positive values and uses the zero address for mint and burn, so those lifecycle logs do not become token-control transfers. Both events are present in the deployed `ETHRegistry` and `UserRegistryImpl` ABIs. (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistry.json:L652 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistry.json:L689 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/UserRegistryImpl.json:L723 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/UserRegistryImpl.json:L760 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L194 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L201 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L208 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L210 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L318 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L333 @ ens_v2@a971bd64)
- `SubregistryChanged` ← `SubregistryUpdated`; `ParentChanged` ← `ParentUpdated`.[^v2-events-l49][^v2-events-l75]
- `AliasChanged` ← `PermissionedResolver.AliasChanged`; the alias path stores source and destination DNS-encoded names.[^v2-iperm-resolver-l14][^v2-pres-l230]
- `PermissionChanged` and `RootPermissionChanged` ← upstream `EACRolesChanged(resource, account, oldRoleBitmap, newRoleBitmap)`. Root-resource permissions stay distinguishable because EAC root roles are checked separately and satisfy resource-level checks via root fallback; this taxonomy admission covers normalized-event emission and manifest watch admission, while current-state projection consumption is a separate projection contract.[^v2-eac-l19][^v2-eac-l176][^v2-eac-l181] Registry/root sources decode role bitmaps with the post-audit `RegistryRolesLib` vocabulary (`registrar`, `register_reserved`, `set_parent`, `unregister`, `renew`, `set_subregistry`, `set_resolver`, `set_uri`, `can_name`, `upgrade`, `can_transfer_admin`, and the corresponding `admin_` powers). `ROLE_WAS_RESERVED` at bit 32 is an upstream non-power marker; bigname retains it in the bitmap and exposes it as `was_reserved` in `effective_powers` so a marker-only transition remains observable, but consumers must not treat it as authorization. Unknown bits are omitted rather than surfaced under invented names.[^v2-regroles-l6][^v2-regroles-l9][^v2-regroles-l14][^v2-regroles-l19][^v2-regroles-l24][^v2-regroles-l29][^v2-regroles-l34][^v2-regroles-l39][^v2-regroles-l45][^v2-regroles-l47][^v2-regroles-l50][^v2-regroles-l55][^v2-regroles-l60] Resolver sources decode the resolver vocabulary, including `set_data`, `can_name`, `upgrade`, and their admin powers.[^v2-resroles-l7][^v2-resroles-l51][^v2-resroles-l56][^v2-resroles-l61] `DataChanged` and `NamedDataResource` remain unadmitted even though `set_data` is a named permission power.[^v2-pres-l161][^v2-pres-l437]
- `RegistrarNameRegistered` ← upstream `ETHRegistrar.NameRegistered`; it is registrar-local registration intent and links back to the registry resource when that registry resource has already been observed.[^v2-iethreg-l32]
- `RegistrationRenewed` ← upstream `IETHRenewer.NameRenewed`; the post-audit terminal payment field is `amount`.[^v2-iethreg-l53] Post-audit normalized `after_state` publishes `amount` and retains `base` with the same value as a compatibility alias. When a two-topic renewal admitted by the deprecated pre-audit manifest is explicitly decoded, it retains its historical `base`-only payload shape.[^v2-sepolia-dev-iethreg-l53] Deprecated pre-audit emitter addresses remain outside the active post-audit watch and replay plan. This is an intentional payload-compatibility rule, not a claim that the post-audit upstream field is still named `base`.

Taxonomy reconciliation decisions:

- `RecordDeleted` is not a separate normalized kind for the currently admitted sources. Deletes are represented as `RecordChanged` payloads with deletion metadata, so consumers only need one record-change stream.
- `CommitmentMade` is not admitted in the normalized taxonomy yet. Upstream ENSv2 `ETHRegistrar` emits `CommitmentMade(bytes32 commitment)`, but current manifests and adapters do not consume it, and no current projection depends on commitment history. (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L19 @ ens_v2@a971bd64)
- `DelegateRetainedAfterTransfer` is not admitted until a concrete source event and consumer projection are specified. Role changes remain `PermissionChanged`, `RootPermissionChanged`, or `PermissionScopeChanged`; token ownership comes from `TokenControlTransferred` rather than inference from a role-event pattern.
- ERC-1155 `ApprovalForAll` remains unsupported. Operator approval is neither token ownership nor an ENSv2 resource-role grant, and no current projection consumes it. (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L336 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L341 @ ens_v2@a971bd64)

ENSv1 direct wrapper/resolver mappings from admitted NameWrapper and PublicResolver events are `PreimageObserved`, `SurfaceBound`, `SurfaceUnbound`, `AuthorityTransferred`, `ExpiryChanged`, `TokenControlTransferred`, `ResolverChanged`, `PermissionChanged`, `PermissionScopeChanged`, and `RecordChanged`.[^v1-iname-l27][^v1-iname-l31][^v1-iname-l35][^v1-iname-l37][^v1-iname-l38][^v1-nw-l1022][^v1-nw-l1034][^v1-pres-l20][^v1-pres-l51][^v1-pres-l58] The admitted wrapped registrar controller's `NameRenewed` also derives an `ExpiryChanged` for the wrapper resource under `ens_v1_registrar_l1`, as defined above; the source family follows the emitting log while the resource identifies the affected wrapper state. `PermissionScopeChanged` retains the effective fuse bitmap and its derived NameWrapper lifecycle state without inventing a subject grant: unwrapping retains fuse/expiry data, and an unexpired rewrap restores the parent-controlled fuses and larger expiry even though `NameWrapped` emits the supplied arguments. (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L235 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L239 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L242 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L246 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L269 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L276 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L901 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L902 @ ens_v1@91c966f) When a separate compatible holder grant exists, current projections apply the derived state, individual owner-controlled fuse bits, and wrapper expiry to that row. (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L16 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f)

Every normalized event carries: namespace, `logical_name_id` when applicable,
`resource_id` when applicable, source family, manifest version, chain position,
raw fact reference, derivation kind, canonicality flag, and before/after state
where possible. The slice-1 schema extends every row with
`consumer_visibility` and a sorted, duplicate-free
`migration_correlation_ids` set: ordinary events default to `activated` and an
empty set, while correlation-dependent events carry their candidate or
activated derivation-group IDs. Independently admitted ordinary events keep
those defaults; their ENSv1→ENSv2 relationships live only in the diagnostic
`migration_event_associations` table.
Raw-log name observations use the `raw_log_preimage_observation` derivation
kind. A `PreimageObserved` row produced by a block-boundary survivor
reassertion instead uses `raw_block_preimage_observation`, matching its
`raw_fact_ref.kind = raw_block` source.

### State-derived normalized events

Replayed interpreter state may supply identity linkage that was unavailable
when an earlier raw event was interpreted. A later raw event that makes the
identity surface active may therefore trigger an additive
[state-derived normalized event](glossary.md#state-derived-normalized-event).
The additive event takes its source family and manifest identifier from the
remembered state, while its block, transaction, log, timestamp, canonicality,
and raw-fact provenance come from the later triggering event. It does not
mutate or replace the earlier normalized fact and requires no provider read.

Fresh, restored, resumed, and redo interpretation over the same retained input
must emit identical state-derived output. Within one raw position, Interpret
orders the selected source's ordinary events first, then state-derived sourced
events, the block-boundary output, and finally identity and preimage output.
When a registrar registration becomes current at the first active ENSv1 name
surface, Interpret also copies that surface identity onto any retained
registry-only authority and registry resource. This does not make the
registry resource current while the registration is live; it preserves the
known direct-registry fallback so a later registrar release can bind that
existing registry resource in fresh, restored, resumed, and redo execution.
A `NameWrapped` observation can be the first active surface too. In that case,
Interpret links the retained registry read resource at the wrapper raw position
without binding the registry resource or displacing wrapper control; an
ownerless unwrap can later keep serving that registry-selected resolver.

The retained ENSv1 resolver pointer records whether its selecting `NewResolver`
came from the old or current registry. A current-registry `NewOwner` or
`Transfer` ends an old-registry pointer because either ownership write creates
a current-registry record and the fallback getter delegates only while that
record does not exist. Interpret persists that handoff even when a same-owner
`Transfer` has no other state delta, and emits linked zero-resolver events when
the handoff retracts an already surfaced old-registry pointer from every
registry, registrar, or wrapper resource to which it was linked. Later
current-registry owner assignments preserve a current-registry pointer, and an
old-registry `Transfer` cannot clear it. This provenance rule, rather than
replay insertion order, applies identically after normalized-event compaction.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L24 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L68 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L82 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L54 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L150-L172 @ ens_v1@91c966f)

For the explicitly ownerless first-surface case, a label-bearing registrar
renewal does not create registrar control when the getter-visible registry owner
is zero and no registrar state was previously known. A registrar authority
already current before the owner becomes zero remains current and is refreshed
by later renewals; only the synthetic first-surface control grant is suppressed.

Normalized events are schema-v2 interpreter transitions. Interpretation loads
canonical raw facts in chain order. Persisted normalized events are the working
store for per-key before/after state; the process carries protocol state and a
bounded [cache of recently used persisted values](storage.md#interpret-process-memory)
across physical batches. A redo is an explicit bounded operation: it prepares
the selected derived range, replays it through the same interpreter, and heals
identities the replay did not reproduce. Preparation restages only identities
whose stored anchor lies inside the redone range, so a redo that starts after
an identity's derivation block cannot move that anchor forward; an identity
the replay re-observes keeps its anchor at its first derivation block, and
only an identity still
orphaned after the replay is re-anchored to the earliest surviving reference
outside the redone range — for a name surface, the earliest surviving
body-carrying `PreimageObserved` observation of that name, and a surface with
no surviving body-carrying observation stays orphaned. The
deleted old-schema storage layer no longer provides general field repair,
payload arbitration, supersession, full-closure proof, or adapter-checkpoint
reuse.

Physical batching is an execution detail, not an input to interpretation for a completed target.
Identity rows, discovery edges, and normalized events must be a pure function
of the canonical raw facts and the declared manifests, discovery rules, and
admissions: after completion, a fresh full walk, an incremental follow, and a resumed session
over identical input must write identical rows no matter where the 500-block
batch boundaries fall. A finitely retired manifest-declared address range is
the narrow history-bearing exception: manifest synchronization supplies its
retirement boundary, Interpret redo preserves it, and a fresh database that
starts after the declaration was removed need not contain that historical
range and produces the same output only when it also lacks the corresponding
raw history. For in-place declaration removal while the discovery authority
remains active, preserving the closed range is what keeps redo normalized-event
output identical to the original interpretation pass. Deprecating the source
manifest instead makes that admission inactive, so equality with the original
pass is not claimed. The closed range cannot admit a new address or change a
projection. The batching guarantee is verified for the divergence classes
[#336](https://github.com/ensdomains/bigname/issues/336) identified on the
ENSv1 path and [#348](https://github.com/ensdomains/bigname/issues/348)
identified on the ENSv2 resolver path; the permutation lane's pinned
batch-artifact counts sit at zero. The structurally identified alias-only shape
is covered by [#529](https://github.com/ensdomains/bigname/issues/529): a name
link created only by a resolver `AliasChanged` preimage observation whose DNS
name passes normalization is rebuilt across fresh, incremental, and resumed
interpretation, subject to the known exception below, and the generated ENSv2
corpus includes that alias-only name followed by a resolver record. When a
resolver-emitted resource equals `namehash(N)`, named-resource and alias
preimages can share one retained [interpreter state
key](glossary.md#interpreter-state-key), so resumed interpretation can lose the
named-resource resolver hint and diverge from a fresh walk
([#560](https://github.com/ensdomains/bigname/issues/560); evidence is checked
in as an ignored collision probe). Five rules keep
the written rows batch-independent:

- `before_state` chains over the emitted event stream: a retained event's
  `before_state` is the `after_state` of the previous retained event under the
  same final `raw_fact_ref.interpreter_state_key` (`{}` at stream start),
  seeded from the pre-batch retained state. Events that same-transaction
  reconciliation later drops or re-keys leave no trace in surviving
  stream-chained state: each retained `before_state` is re-derived from the
  surviving stream alone. Interpreter-declared explicit befores (deliberate
  snapshots such as wrapper fuse state or permission grant bodies) are exempt
  from re-threading and are written as computed at emission, with one
  carve-out: same-transaction re-attribution to a registration (the second
  rule) resets the before to `{}`, so the registration's stream starts from
  an empty snapshot. A surviving explicit before may quote in-memory state a
  later-dropped same-transaction event wrote; that snapshot is computed
  identically in every [run shape](glossary.md#run-shape). The block-scoped
  predecessor-epoch
  permission closures keep their computed snapshot.
- Identity attribution is fixed at emission, with one exception:
  same-transaction reconciliation may attribute registry, resolver, and
  permission observations to a registration established later in the same
  transaction, and predecessor-epoch permission closures may be attributed
  when they share the registration's block. Reconciliation never reaches
  across a block boundary — batches never split a block, so the block is the
  atomic unit every [batch grid](glossary.md#batch-grid) loads — so
  predecessor-epoch observations that only
  a later block's registration could identify keep their event-time
  attribution (null `logical_name_id`/`resource_id` where no authority was
  known) in every run shape (fresh, incremental, or
  resumed).
- Resource rows anchor at their first derivation block. A superseded
  registry-only resource emission is retained even when no surviving
  same-batch row references it, so the first-committed identity upsert anchors
  the resource at the same block in every run shape. The single mover is the
  bounded redo's orphan healing above: an identity still orphaned after a
  replay re-anchors to the earliest surviving reference outside the redone
  range.
- An ENSv2 registry/root `PreimageObserved` event for a canonical [name
  surface](glossary.md#surface-name-surface), or a resolver `AliasChanged`
  preimage observation whose DNS name passes normalization, is lasting
  evidence that the surface exists. Registration release, expiry, or a
  topology change may close the current binding and remove its `resource_id`,
  but does not erase the surface. A cold restore rebuilds the canonical surface
  observation from the retained event. Restoring alias evidence records only
  the known surface and never creates or restores a resource binding;
  normalization-rejected name observations are not admitted to that state. The
  retained preimage-key collision in issue #560 is the known exception to this
  cross-run restore guarantee. A later resolver `RecordChanged` or
  `RecordVersionChanged`
  remains attributed to the logical name while remaining resource-less when no
  current resource exists. If an ended resource's latest retained
  `ResolverChanged` pointer still names that resolver, Project can rebuild a
  different resource-keyed record-inventory row from the newly attributed
  event. It does not restore the current binding or expose that inventory
  through a name whose `name_current.resource_id` is null. ENSv2 resolver
  records are stored by node and record version. `setName`
  passes part zero, selecting the node-specific, any-part permission resource;
  the cited authorization path reads EnhancedAccessControl role mappings and
  contains no current registry-registration lookup. (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L127-L133 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L77-L85 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L178-L186 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L467-L472 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L247-L254 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L66-L78 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L185-L192 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L374-L382 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L443-L455 @ ens_v2@a971bd64)
- ENSv1 time-derived lifecycle changes at the start of a block read ENSv1
  protocol state rebuilt from the preceding block's normalized events after
  same-transaction reconciliation. The interpreter applies that rule between
  blocks even when both blocks share one physical batch; a batch boundary
  therefore cannot create an extra state-rebuild observation point. For a
  wrapped `.eth` name, NameWrapper stores registrar expiry plus the 90-day
  grace period, and after that wrapper expiry its public data clears an
  emancipated owner and clears the fuse word. The first loaded raw block whose
  timestamp is later than that expiry anchors the resulting authority and
  permission changes; the physical batch grid never does. (upstream:
  .refs/ens_v1/contracts/wrapper/README.md:L77 @ ens_v1@91c966f) (upstream:
  .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L48 @ ens_v1@91c966f)
  (upstream:
  .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L297-L303 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L143-L153 @
  ens_v1@91c966f) (upstream:
  .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L852 @ ens_v1@91c966f)

## Resolution

`Resolution` is one mixed-route envelope with three declared sections and one verified section: `topology`, `record_inventory`, `record_cache`, `verified_queries`.

### `topology`

Fixed declared object. The authoritative serialized model is
`bigname_domain::resolution_topology::ResolutionTopology`: Project finishes
each set-based topology rebuild by deserializing and serializing through that
model before publication. Storage and live lookup deserialize the same model
and call its `ResolutionRoute` classifier. The classifier receives the
deployment's expected transport contract as a typed policy input; neither
consumer reparses the object. Storage supplies the documented support
contract, while live lookup supplies the address from the selected execution
manifest. Project publishes a Basenames topology only when that manifest
declares the same contract, so both typed policy inputs agree for every
published supported topology.

The model uses the domain-owned `Namespace`, `ChainId`, and strict 20-byte
`EvmAddress` vocabularies. The same domain module owns the closed
`SourceFamily` vocabulary used when lookup selects a supported execution
entrypoint, but source families are not fields in the serialized topology.
Manifest repository identities remain authored strings because generated
deployment profiles may use isolated test identities. The closed chain
vocabulary includes the fixed non-production labels used by the repository's
topology and reorg suites. Its complete wire set is `base-mainnet`,
`ethereum-mainnet`, `ethereum-sepolia`, `base-e2e-composed-reorg`,
`ethereum-e2e-rpc`, `ethereum-e2e-reorg`,
`ethereum-e2e-composed-reorg`, and `project-fixture`. Project rejects a
topology carrying any other chain ID before publication; generated deployment
profiles that publish topology must use one of those labels. EVM addresses in newly
derived topologies serialize as
lowercase `0x`-prefixed strings. Before this
model was introduced, the Basenames `transport.contract_address` retained the
checksummed manifest spelling; after the boundary re-derivation it is
lowercase. The field set and nesting otherwise remain unchanged.

- `registry_path` — ordered `NameRef` array from the requested surface toward declared registry authority. It is non-empty for authority-backed supported topology. A known-ownerless V1 name served only through an event-linked registry resolver keeps this array empty because its retained registry resource is not authority.
- `subregistry_path` — toward the nearest declared subregistry ancestor. Empty when none participates.
- `resolver_path` — ordered hops; each carries `logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `resource_id`, `chain_id`, `address`, `latest_event_kind`.
- `wildcard` — `{source, matched_labels}`. `null/[]` means wildcard didn't participate.
- `alias` — `{final_target, hops}`. `null/[]` means alias didn't participate.
- `version_boundaries` — `{topology_version_boundary, record_version_boundary}` with `logical_name_id`, `resource_id`, `normalized_event_id`, `event_kind`, `chain_position`.
- `transport` — `{source_chain_id, target_chain_id, contract_address, latest_event_kind}`. All `null` means no transport. For Basenames capability-promotion target paths, `source=base-mainnet, target=ethereum-mainnet` through the L1 Resolver.[^bn-readme-l22][^bn-readme-l28][^bn-readme-l29][^bn-readme-l34][^bn-readme-l69][^bn-readme-l70]

The authority-backed non-empty `registry_path` rule is a Project producer invariant, not a
[verified lookup](glossary.md#verified-lookup) route discriminator. The shared classifier preserves the
prior storage and lookup route matrix: it identifies direct, alias-only,
wildcard-derived, and Basenames transport-assisted paths from the resolver,
alias, wildcard, subregistry, and transport fields listed below.

For ENSv2, `alias` is declared topology only when `PermissionedResolver` provides an `AliasChanged` mapping; the resolver resolves aliases by longest suffix and rewrites calldata before [resolver-profile](glossary.md) dispatch.[^v2-iperm-resolver-l14][^v2-pres-l56][^v2-pres-l412][^v2-pres-l650] Wildcard is observed topology — populated only when execution input identifies an ancestor/source resolver and matched labels.[^v2-pres-l38][^v2-pres-l412]

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
- [Universal Resolver ancestor
  discovery](glossary.md#universal-resolver-ancestor-discovery): an Ethereum
  Mainnet exact surface whose projected exact resolver is null, DNS wire name
  is available, and alias, linked-subregistry, wildcard, and transport detail
  are empty. The manifest-admitted Universal Resolver performs the ancestor
  walk at the selected block; the API keeps the exact resolver null
  `(upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L25-L38 @ ens_v1@91c966f)`
  `(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L63-L88 @ ens_v1@91c966f)`.

Other ENS classes (projected non-alias ancestor-selected,
linked-subregistry ancestor-selected, transport-assisted, CCIP-participating)
return selector-local `unsupported`.

Basenames supports the exact-surface transport-assisted direct path through active `basenames_execution` v2 at the L1 Resolver. Other Basenames verified [path classes](glossary.md) return selector-local `unsupported`.[^bn-readme-l69][^bn-readme-l70][^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]

V2 verified name and record routes execute through the schema-v2 lookup engine
without a durable trace or reusable outcome. A
guarded direct live/indexed disagreement may
create or replace an active
[resolution divergence ledger](glossary.md#resolution-divergence-ledger) row;
restored agreement may clear the matching active row.

## Permissions

Permissions are first-class projections and explain views. Track grants by scope (root, registry, resource, resolver, record manager/operator). Each grant records source, revocation source, inheritance path, transfer behavior, scope, and effective powers.

Public reads expose effective powers directly so callers do not reconstruct
authority from raw role bitmaps. `GET /v2/permissions` is the current
resource-anchored permission collection; name- and address-centric views
summarize or filter the same truth.

The current projection interprets admitted ENSv1 and Basenames registry
`ApprovalForAll` events as [account permission
state](glossary.md#account-permission-state). Registrar token/operator,
resolver operator/delegate, NameWrapper, and ENSv2 registry approvals remain
unprojected. Non-wrapper permission summaries are therefore request-relative
partial rather than authoritative enumerations, including for empty results.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L118 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L148-L158 @ basenames@1809bbc) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L78-L103 @ ens_v1@91c966f)

For ENSv1 wrapper-backed resources, the current projection publishes no wrapper-holder subject grant derived from fuse state. Fuse changes remain available as `PermissionScopeChanged` history, and any separately observed compatible holder grant is masked by the effective lifecycle state and owner-controlled fuses. A locked name has no broad `resource_control`; individual fuses remove only their matching powers. Once an emancipated or locked position expires, it contributes no wrapper-holder powers because NameWrapper clears the owner and fuse values. (upstream: .refs/ens_v1/contracts/wrapper/README.md:L89 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/README.md:L93 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L848 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L849 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f) A `.eth` second-level name keeps its lifecycle state and token holder through the 90-day registrar grace period, while owner modification, transfer, and effective-controller membership stop at grace start. (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L48 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L218 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L221 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L820 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L825 @ ens_v1@91c966f) Internal projection inputs for a registrar name wrapped after registration can retain stale pre-wrap control facets; public exact-name reads do not publish those facets as effective control and instead return an explicit unsupported control summary for every current wrapper resource.[^v1-iname-l10][^v1-nw-l421][^v1-nw-l427][^v1-nw-l637][^v1-nw-l666][^v1-nw-l676][^v1-nw-l723][^v1-nw-l827][^v1-nw-l1023][^v1-nw-l132] An empty permission result therefore still does not prove complete wrapper-holder enumeration.

For ENSv2, `PermissionedRegistry.getResource(anyId)` keys permissions by upstream resource, so public permissions key by the bigname `resource_id` linked to that resource, not by token ID.[^v2-iperm-l57][^v2-pr-l261][^v2-pr-l351] Resolver-scoped permissions live in the same resource-anchored model with resolver scope metadata; `PermissionedResolver` uses name-, text-key-, and coin-type-specific EAC resources for setters.[^v2-pres-l70][^v2-pres-l159][^v2-pres-l239][^v2-pres-l257][^v2-pres-l282]

Required indexes: by resource, by account, by resolver; permission history by resource and by account.

## Primary and reverse names

The primary-name projection is address- and `coin_type`-centric, not just a
reverse-record projection. `bigname_phase.primary_names_current` stores the
declared claim, namespace, coin type, resolver evidence, provenance, support,
and publication position. It does not store verified output.

- Both objects use `ResultStatus`. `mismatch` applies to verified only; `execution_failed` also applies to a route-local claimed lookup when its provider fails.
- `claimed_primary_name` is candidate-only; `verified_primary_name` is authoritative only when `success`.
- A raw claim that cannot be normalized surfaces `invalid_name`, not silent drop.
- Verified success additionally requires the untrimmed on-chain claim to byte-equal its ENSIP-15 normalized form. A normalizable claim with a different raw spelling remains a successful claimed candidate, but `verified_primary_name` returns `status=invalid_name` with `failure_reason=claim_not_normalized` instead of resolving the normalized variant.
- Reverse claims alone don't verify — verification must resolve back to the requested address.[^v1-aur-l217][^v1-aur-l226][^v1-aur-l263][^v1-aur-l269]

For ENS, declared claim precedence is reverse-only through
`ens_v1_reverse_l1`.[^v1-revreg-deploy][^v1-revreg-l74][^v1-revreg-l83][^v1-revreg-l84]
Project may refresh an existing ENS/60 tuple through the configured
[event-silent](glossary.md) reverse resolver at the exact published head. It
retains the raw spelling and whether that spelling byte-equals its ENSIP-15
normalization; it never manufactures a claim from manifest presence, resolver
identity alone, or verified lookup.

For Basenames, declared primary-name value intake is `basenames_base_primary` at the ENSv1 Base `L2ReverseRegistrar` (`0x0000000000D8e504002cC26E3Ec46D81971C1664`), using the `NameForAddrChanged(address,string)` event and Base coin type `2147492101`.[^v1-l2rev-base-deploy][^v1-l2rev-base-args][^v1-l2rev-event][^v1-l2rev-nameforaddr] It does not replace the Base registry/registrar/resolver families for declared truth on exact-name, address-name, or children reads, and it does not use the Basenames `ReverseRegistrar` as the primary-name value source. Verified primary names enter through `basenames_execution` against the L1 Resolver.[^bn-readme-l22][^bn-l1resolver-l13]

V2 reads the indexed claim from the phase projection and obtains ENS/60
verification from a fresh [hash-pinned](glossary.md) schema-v2 lookup. A raw
claim that cannot be normalized returns `invalid_name`; a normalizable claim
whose original spelling differs from its normalized form is not verified.
Reverse claims alone do not verify: forward resolution must return the requested
address.[^v1-aur-l217][^v1-aur-l226][^v1-aur-l263][^v1-aur-l269] The request
writes no reusable outcome, trace, or divergence row. V2 Basenames primary-name
verification is unsupported; its indexed response remains Base-scoped.

## Collection semantics

Product collections share one authority rule,
[current-authority fanout](glossary.md#current-authority-fanout). A collection
may derive its membership and current fields from the current registration
already selected for the name by the
[ENSv1→ENSv2 current authority](#ensv1ensv2-current-authority) rule, but it may
not perform an independent cross-era ENSv1-versus-ENSv2 ranking of its own.
Authority-derived product-history fanout means authority-derived anchors and
current annotations only: it deletes or suppresses no independently admitted
historical fact, so exact-name history and explicit registration history retain
both eras.

One same-arm exception is deliberate, and it applies to any name whose selected
authority is registry-only, not to one arm. A registry/registrar split puts a
name's registry ownership and its registrar leasehold on two resources of a single
authority arm. The registrar's ERC721 transfer writes no registry state; after
registration only `reclaim` does (upstream:
.refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L174 @
ens_v1@91c966f). A transfer with no `reclaim` therefore leaves the registry owner
behind on the resource that the later registry-only binding superseded. Address
relations publish that divergent owner as the name's effective controller even
though its resource is not the selected registration, because otherwise the
divergence would be invisible in every product collection.

Both admitted registry/registrar arms reach this. ENSv1 is the obvious one.
Basenames reaches it too: its registrar and its registry are both admitted source
families, its registrar creates the non-registry predecessor binding, and its
registrar writes the registry owner before emitting the event the binding is
provenanced to, in the same order (upstream:
.refs/basenames/src/L2/BaseRegistrar.sol:L423-L425 @ basenames@1809bbc). The
`reclaim` half of the justification above is verified for ENSv1 only; what
Basenames shares is the registration-time write ordering the block bound below
depends on.

The exception admits only events on the immediate predecessor binding's resource,
from the block of that binding up to the selected binding's position. The bound is
what keeps the exception narrow. Registry-only authority is the ordinary state of
every plain subname and of every expired `.eth` name, so reaching back past the
immediate predecessor would republish owners from long-superseded eras — and for a
name that expired while wrapped it would publish the wrapper contract itself as
the controller of every such name, because registry-owner writes made during the
wrapped era are attributed to the wrapper resource. (The wrap itself is not such a
write: the registry hand-off to the wrapper is attributed to the pre-wrap
resource. Only the ENSv1 arm has an admitted wrapper source family, so wrapped-era
attribution arises only there.)

The lower bound compares blocks, not full log positions, and that is deliberate.
It is not the same window the exact-name authority uses for this divergence: that
one compares complete positions, because the events it admits are
registration-lifecycle events, emitted at or after the log the binding records as
its provenance. This exception admits `AuthorityTransferred`, which can precede
that log inside the very transaction that creates the binding — registration
writes the registry owner before emitting the event the binding is provenanced to
(upstream:
.refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L148-L152 @
ens_v1@91c966f). A position-precise bound would therefore drop exactly the
registration-time owner this exception exists to recover.

Widening to block granularity does not reach into an earlier era. A registrar or
wrapper resource identity is derived from the block hash and log index of the
event that created it, so a resource of either kind cannot carry an event that
predates its own creation, and the only admission earlier than the binding's
provenance is the same-transaction registry write above. Block granularity is also
what keeps the bound meaningful across both binding writers: an event-driven
binding is provenanced with the transaction and log index of the event that opened
it, but a binding opened by a lifecycle boundary such as a release records only
the block, so a position-precise lower bound would degenerate for the second kind.
A registry-only resource
identity is instead stable per node and can span eras, but when the predecessor is
one it is the same resource as the selection, and those events are already in the
selected set rather than readmitted through this exception.

The exact-name summary does not follow the exception: for this superseded-resource
case it keeps reporting no registry owner, so `name_current` and
`address_names_current` disagree here by design. That disagreement is specific to
this case — an ordinary registry-only name, which never had a superseded
predecessor, reports its registry owner in both collections.

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

Default returns declared direct child nodes. Basenames registry edges whose parent surface is active remain children. ENSv1 registry edges additionally must remain reachable through the parent's ENSv1→ENSv2 migration path under the rule above. A surviving row remains available even when bigname cannot state the child's name; it carries a [non-name form](glossary.md#non-name-form) — the bracketed labelhash placeholder when the label was never observed, or the escape encoding of the whole stored name when the label was observed as bytes that do not decode — rather than minting an exact-name surface. The ENSv2 arm additionally joins the child's own name surface, so a child without an active surface — label never observed, or rejected by the normalization gate — is absent there rather than named by a stand-in. Optional buckets: linked-subregistry, alias-derived, observed wildcard. `subname_count` in the main name summary means declared direct children only.

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
- `coverage` — the projection coverage facts exposed by the v2 name coverage diagnostic

None of these introduces a separate truth system or ledger.

## Coverage and exhaustiveness

Coverage is contractual.

- Exact-name lookup is authoritative for supported source classes. Route-level coverage may still be authoritative when individual declared summary subdocuments are unsupported.
- Address-to-name enumeration is exhaustive only for enumerable source classes.
- Wildcard and offchain name classes are not globally enumerable.
- Record inventory is `best_effort` unless a resolver family enumerates explicitly or there's a source-specific index.
- Child enumeration is authoritative only for declared direct children unless the caller opts into other surface classes.
- V2 primary-name route-level coverage is `partial`, with
  `exhaustiveness=non_enumerable` and
  `enumeration_basis=primary_name_lookup` for ENS/60 fresh verification. Other
  v2 verified tuples are explicit `unsupported`.

Every response carries `coverage.status`, `coverage.exhaustiveness`, `coverage.source_classes_considered`, `coverage.unsupported_reason`, `coverage.enumeration_basis`.

## Verified execution

Default verified entrypoints:

- ENS: `ens_execution` at the official Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`.[^ens-docs-univ][^v1-aur-l90][^v1-aur-l106]
- Basenames: active `basenames_execution` v2 at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` supports only the exact-surface transport-assisted direct path; other Basenames verified path classes stay `unsupported`.[^bn-readme-l22][^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]

The v2 lookup engine executes afresh at the schema-v2 current readable position.
It has no trace or cache identity. It may compare a direct record answer with
the projected exact entry or a manifest-authorized derived read from that same
record inventory and perform the guarded divergence-ledger write;
v2 primary-name verification performs no write.

## Reorg, redo, and historical ranges

The phase runner stores competing block hashes per chain, including observed and
orphaned branches. In the supported phase schema, the schema-v2 baseline's
partial unique index permits at most one `canonical`, `safe`, or `finalized`
block at a given chain height. Head publication marks a
displaced readable lineage branch `orphaned` before making the selected branch
readable; interpretation selects raw facts through that lineage rather than
rewriting immutable raw rows. An explicit `interpret` redo replaces derived
identity, discovery, and normalized-event output for its selected range, except
for four bounded kinds of coordination state carried across redo preparation.
It preserves the resolver references that Project needs to find projection rows
affected by disappearing events, the available logical-name and
permission-resource identifiers from state-derived ENSv2 path-expiry releases,
child identifiers from entry-creating events in ENSv1→ENSv2 migration registries,
and finitely retired manifest-declared address ranges that prevent replay of
older observations from reopening retired authority. Project seeds from the
resolver references, release identifiers, and child identifiers only during the
covering Redo-mode publication: logical names seed bounded descendant replay as
[expiry roots](glossary.md#expiry-root), permission resources force a resource
rebuild, and migration-registry entry history seeds the affected child. A later
Normal-mode catch-up consumes those rows without seeding from them; #828 tracks
whether that asymmetry should change.
Interpret uses the retired address boundary while rewriting discovery output.

The live phase uses the same head-publication transaction as ingest. That
transaction orphans the displaced suffix, clears affected active resolution
divergence observations, and stamps `interpret`, `project`, and overlapping
`verify` rows with a recorded cursor for bounded redo when the orphaned suffix
starts at or below their recorded cursors. The live loop consumes those stamps
in dependency order before advancing downstream work, so projections cannot
silently retain output from the losing fork. Successful `interpret` redo also
stamps `project` for the same replayed suffix; a direct data-repair redo
therefore cascades without a second operator command. Since interpret replaces
normalized events before project runs, project redo retains the incremental
keys cited by current rows when those event IDs disappear, including both
primary-name reverse and claim citations. The winning replay can therefore
retract an event-only losing-fork value even when its stable identity was first
observed before the redo range.
The persisted system marker distinguishes a pending required range from its
active replay. Dependency selection and live gap fill may pass a pending marker;
after replay acquires the phase writer slot, ordinary cross-phase writer
exclusion applies.
Live also checks that a suffix loaded after ancestry discovery still descends
from the selected common ancestor. A provider reorg between those reads is a
retryable snapshot change, not a terminal lineage failure.

The `phase-runner rewind` command is a thin head-publication operation. It takes
the ingest, interpret, project, and live advisory locks so it cannot race a head
publisher or downstream writer, selects an exact stored readable ancestor at or
above the safe head, and invokes the same atomic orphaning, divergence cleanup,
and redo-stamping path. The next supervised live cycle fills the winning path,
then runs the required downstream redo.

Historical work is a finite `ingest`, `interpret`, `project`, or `verify` run.
An explicit redo can select one phase or all four in dependency order for one,
several, or every active-manifest chain; it is not a persisted old-schema
backfill job. Live follow normally starts at the completed ingest handoff and
only walks the current head and a winning-fork gap; it never provides historical
coverage. The recovery-only exception starts at the published readable head
when a required Ingest redo end became unreadable and interrupted finite Ingest
recorded no handoff. That pass only republishes the winning suffix; it does not
execute or clear the operator-owned historical redo.
`--phase recompute-flags` supports bounded flag recomputation. Among otherwise
configured redo requests, historical `live` redo, unreadable range ends, and an
Interpret, Project, or recompute-flags redo requested while a required Ingest
redo is still stamped for that chain are rejected before redo state is written.
A deployment therefore still needs
complete admitted history for ENSv1, ENSv2, and Basenames source families.
Wildcard and offchain names remain
discovery/observed-answer based rather than exhaustively enumerable.

## Operations

API metrics remain available. The phase runner also exposes a read-only metrics
endpoint built from its phase state, heartbeat, chain-head, verification, and
redo records. These metrics include each phase's current and target block and
the live phase's lag behind the latest provider head it observed and stored as
that phase's target. An in-process heartbeat records when each supervised or
active one-shot repair chain last crossed a phase or batch boundary, including
periods when every supervised phase row is resting. The endpoint does not write
additional operating state. Dedicated reorg metrics remain deferred.

The phase runner owns the current schema-v2 operator tools. None expose public
API routes:

- `phase-runner redo` and `phase-runner rewind` own finite phase repair and thin head publication, respectively.
- `phase-runner inspect block-canonicality` reads bounded fork labels plus retained fact counts.
- `phase-runner inspect stored-lineage` reads bounded lineage and optional stored header audit fields.
- `phase-runner inspect raw-events` reads bounded raw logs with transaction, receipt, lineage, header-presence, and normalized-event context.

The inspection commands use read-only repeatable-read transactions and retain
orphaned forks in their output with explicit canonicality labels. Drift,
payload-cache, execution-trace, and watch-plan views were cut and have no
schema-v2 phase-runner replacements. Schema-v2 projection maintenance remains
an explicit project-phase normal run or bounded redo; there is no independent
projection replay command.

There is no live manifest-drift or proxy-upgrade alert loop. Manifest and
discovery authority remain explicit, and changes require the normal manifest
synchronization and phase redo path.

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
- projections are disposable and rebuildable, but their foreign keys require
  projection rows to be removed before identity rows and rebuilt only after the
  identity rows exist; serving resumes after coherent Project publication
- protocol-specific logic lives in adapters and execution drivers, not in the public contract
- no silent cross-source fallback; every fallback appears in provenance/explain
- no requirement to preserve the ENSv1 indexer API surface

## Implementation shape

Rust modular monolith. PostgreSQL is the hot indexed/replay store for durable
selected facts, projections, and guarded divergence observations. The phase
runner handles ingestion, interpretation, projection, verification, live
follow, and bounded redo. The API serves v2 projection and lookup reads,
GraphQL compatibility reads, health, and diagnostic readback.

Repository layout:

- `apps/api`, `apps/phase-runner`
- `crates/domain`, `crates/storage`, `crates/manifests`, `crates/adapters`,
  `crates/ingest`, `crates/interpret`, `crates/lookup`, `crates/project`, and
  `crates/test-support`

## Test matrix

This is a protocol-risk inventory, not a claim that the e2e suite covers every
row. API crate tests own the v2 and GraphQL route behavior. `tests/e2e` is
current contract-to-schema-v2 pipeline evidence: pinned contracts run on
Anvil, the production `phase-runner` ingests or consumes immutable raw facts,
and scenarios assert normalized events, phase state, and projections directly.
The suite does not start the API, so it is not public HTTP evidence.
Its exact runnable, retired, and deferred inventory is maintained in
[`tests/e2e/README.md`](../tests/e2e/README.md) and the expanded
[coverage ledger](internal/e2e-testing-plan.md).

ENSv1 and wrapper: ENSv1-only name, derived wrapped/emancipated/locked state, wrapped expiry/grace edge, expiry-gated fuse-scope history plus incomplete wrapper-holder permission enumeration and public suppression of stale internal control inputs, wrapped owner ≠ registrant, reverse claim vs verified primary mismatch. (upstream: .refs/ens_v1/contracts/wrapper/README.md:L32 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/README.md:L34 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f)

ENSv2: root-scope role grant, delegate retained after transfer, token regeneration without ownership change, shared subregistry creating multiple surfaces for one resource, alias-derived surface with no direct registry entry, subregistry swap replacing a subtree, and unregister/re-register rotating both resource and token lineage.[^v2-pr-l237][^v2-pr-l241][^v2-pr-l242][^v2-pr-l542][^v2-pr-l547]

DNS / wildcard / offchain: imported DNS name, gasless DNS or metadata-discovered name where supported, wildcard-derived subname, CCIP success, CCIP failure, offchain gateway mismatch.

Basenames: NFT-only transfer, management-only transfer, address-resolution change, full transfer, primary-name set/unset, L1 compatibility resolution, current single-address capability.

Operational: reorg across authority events, reorg repair of divergence observations, replay determinism from raw facts, replay determinism from normalized events, proxy implementation change, manifest version change.

End-to-end cases validate every schema-v2 layer material to their claim: raw
facts, normalized events, projections, and, once a contract-backed caller
exists, execution output or public API output. The current suite stops before
the API; route behavior remains owned by API crate tests.

## Open decisions

- exact Postgres partitioning strategy
- whether subscriptions ship in the first stable read milestone or after

---

[^ens-docs-univ]: <https://docs.ens.domains/resolvers/universal/> (official Universal Resolver proxy)

[^bn-readme-l8]: (upstream: .refs/basenames/README.md:L8 @ basenames@1809bbc)
[^bn-readme-l14]: (upstream: .refs/basenames/README.md:L14 @ basenames@1809bbc)
[^bn-readme-l22]: (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc)
[^bn-readme-l28]: (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc)
[^bn-readme-l29]: (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc)
[^bn-readme-l30]: (upstream: .refs/basenames/README.md:L30 @ basenames@1809bbc)
[^bn-readme-l33]: (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc)
[^bn-readme-l34]: (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc)
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
[^bn-registry-l132]: (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
[^bn-registry-l223]: (upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc)
[^bn-baseregistrar-l15]: (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L15 @ basenames@1809bbc)
[^bn-registrar-controller-l180]: (upstream: .refs/basenames/src/L2/RegistrarController.sol:L180 @ basenames@1809bbc)
[^bn-registrar-controller-l187]: (upstream: .refs/basenames/src/L2/RegistrarController.sol:L187 @ basenames@1809bbc)
[^bn-upgradeable-registrar-controller-l191]: (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L191 @ basenames@1809bbc)
[^bn-upgradeable-registrar-controller-l198]: (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L198 @ basenames@1809bbc)
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
[^bn-revreg-l155]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L155 @ basenames@1809bbc)
[^bn-revreg-l156]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L156 @ basenames@1809bbc)
[^bn-revreg-l157]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L157 @ basenames@1809bbc)
[^bn-revreg-l193]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
[^bn-constants-l12]: (upstream: .refs/basenames/src/util/Constants.sol:L12 @ basenames@1809bbc)
[^bn-constants-l13]: (upstream: .refs/basenames/src/util/Constants.sol:L13 @ basenames@1809bbc)

[^v1-ens-l12]: (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)
[^v1-ensreg-l17]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17 @ ens_v1@91c966f)
[^v1-ensreg-l89]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)
[^v1-ensreg-l93]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L93 @ ens_v1@91c966f)
[^v1-ensreg-l94]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L94 @ ens_v1@91c966f)
[^v1-ensreg-l104]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L104 @ ens_v1@91c966f)
[^v1-ensreg-l105]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L105 @ ens_v1@91c966f)
[^v1-ensreg-l174]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f)
[^v1-ensregfallback-l20]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L20 @ ens_v1@91c966f)
[^v1-ensregfallback-l31]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L31 @ ens_v1@91c966f)
[^v1-ensregfallback-l42]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L42 @ ens_v1@91c966f)

[^v1lll-enslll-l30]: (upstream: .refs/ens_v1_lll/contracts/ENS.lll:L30 @ ens_v1_lll@7e377df)
[^v1lll-enslll-l65]: (upstream: .refs/ens_v1_lll/contracts/ENS.lll:L65 @ ens_v1_lll@7e377df)
[^v1lll-enslll-l66]: (upstream: .refs/ens_v1_lll/contracts/ENS.lll:L66 @ ens_v1_lll@7e377df)
[^v1lll-enslll-l73]: (upstream: .refs/ens_v1_lll/contracts/ENS.lll:L73 @ ens_v1_lll@7e377df)
[^v1lll-enslll-l74]: (upstream: .refs/ens_v1_lll/contracts/ENS.lll:L74 @ ens_v1_lll@7e377df)
[^v1lll-enslll-l82]: (upstream: .refs/ens_v1_lll/contracts/ENS.lll:L82 @ ens_v1_lll@7e377df)
[^v1lll-enslll-l97]: (upstream: .refs/ens_v1_lll/contracts/ENS.lll:L97 @ ens_v1_lll@7e377df)
[^v1lll-enslll-l112]: (upstream: .refs/ens_v1_lll/contracts/ENS.lll:L112 @ ens_v1_lll@7e377df)
[^v1lll-enslll-l119]: (upstream: .refs/ens_v1_lll/contracts/ENS.lll:L119 @ ens_v1_lll@7e377df)
[^v1lll-enslll-l120]: (upstream: .refs/ens_v1_lll/contracts/ENS.lll:L120 @ ens_v1_lll@7e377df)
[^v1lll-enslll-l121]: (upstream: .refs/ens_v1_lll/contracts/ENS.lll:L121 @ ens_v1_lll@7e377df)
[^v1lll-enslll-l194]: (upstream: .refs/ens_v1_lll/contracts/ENS.lll:L194 @ ens_v1_lll@7e377df)
[^v1lll-enslll-l200]: (upstream: .refs/ens_v1_lll/contracts/ENS.lll:L200 @ ens_v1_lll@7e377df)

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
[^v1-iaddrres-l11]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L11 @ ens_v1@91c966f)

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
[^v1-nameresolver-l11]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L11 @ ens_v1@91c966f)
[^v1-nameresolverimpl-l25]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L25 @ ens_v1@91c966f)
[^v1-l2rev-base-deploy]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-l2rev-base-args]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f)
[^v1-l2rev-event]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
[^v1-l2rev-nameforaddr]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L154 @ ens_v1@91c966f)

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
[^subgraph-ts-l168]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L168 @ ens_subgraph@723f1b6)
[^subgraph-ts-l230]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L230 @ ens_subgraph@723f1b6)
[^subgraph-ts-l238]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6)
[^subgraph-ts-l246]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6)

[^graphnode-eventext-l17]: (upstream: .refs/graph_node/graph/src/abi/event_ext.rs:L17 @ graph_node@aefe173)

[^v2-deploy-root]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/RootRegistry.json:L2 @ ens_v2@a971bd64)
[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistry.json:L2 @ ens_v2@a971bd64)
[^v2-deploy-ethrc]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistrar.json:L2 @ ens_v2@a971bd64)
[^v2-deploy-pres]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L2 @ ens_v2@a971bd64)

[^v2-userreg-l15]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registry/UserRegistry.sol:L15 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-ethrc-l30]: (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L25 @ ens_v2@a971bd64)
[^v2-ethrc-l151]: (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L151 @ ens_v2@a971bd64)

[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L39 @ ens_v2@a971bd64)
[^v2-iperm-l57]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L73 @ ens_v2@a971bd64)
[^v2-iperm-l67]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L83 @ ens_v2@a971bd64)
[^v2-iperm-l72]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L88 @ ens_v2@a971bd64)
[^v2-events-l15]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L18 @ ens_v2@a971bd64)
[^v2-events-l30]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L33 @ ens_v2@a971bd64)
[^v2-events-l49]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L56 @ ens_v2@a971bd64)
[^v2-events-l59]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L66 @ ens_v2@a971bd64)
[^v2-events-l69]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L82 @ ens_v2@a971bd64)
[^v2-events-l75]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L88 @ ens_v2@a971bd64)

[^v2-pr-l28]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L29 @ ens_v2@a971bd64)
[^v2-pr-l131]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L142 @ ens_v2@a971bd64)
[^v2-pr-l141]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L150 @ ens_v2@a971bd64)
[^v2-pr-l151]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L169 @ ens_v2@a971bd64)
[^v2-pr-l203]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L455 @ ens_v2@a971bd64)
[^v2-pr-l216]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L467 @ ens_v2@a971bd64)
[^v2-pr-l222]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L474 @ ens_v2@a971bd64)
[^v2-pr-l225]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L477 @ ens_v2@a971bd64)
[^v2-pr-l237]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L199 @ ens_v2@a971bd64)
[^v2-pr-l241]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L203 @ ens_v2@a971bd64)
[^v2-pr-l242]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L204 @ ens_v2@a971bd64)
[^v2-pr-l261]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L231 @ ens_v2@a971bd64)
[^v2-pr-l351]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L363 @ ens_v2@a971bd64)
[^v2-pr-l451]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L531 @ ens_v2@a971bd64)
[^v2-pr-l461]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L540 @ ens_v2@a971bd64)
[^v2-pr-l542]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L640 @ ens_v2@a971bd64)
[^v2-pr-l547]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L650 @ ens_v2@a971bd64)

[^v2-regroles-l6]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L6 @ ens_v2@a971bd64)
[^v2-regroles-l9]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L9 @ ens_v2@a971bd64)
[^v2-regroles-l14]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L14 @ ens_v2@a971bd64)
[^v2-regroles-l19]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L19 @ ens_v2@a971bd64)
[^v2-regroles-l24]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L24 @ ens_v2@a971bd64)
[^v2-regroles-l29]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L29 @ ens_v2@a971bd64)
[^v2-regroles-l34]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L34 @ ens_v2@a971bd64)
[^v2-regroles-l39]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L39 @ ens_v2@a971bd64)
[^v2-regroles-l45]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L45 @ ens_v2@a971bd64)
[^v2-regroles-l47]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L47 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L48 @ ens_v2@a971bd64)
[^v2-regroles-l50]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L51 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L53 @ ens_v2@a971bd64)
[^v2-regroles-l55]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L56 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L58 @ ens_v2@a971bd64)
[^v2-regroles-l60]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L61 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L63 @ ens_v2@a971bd64)

[^v2-iperm-resolver-l14]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L19 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-iethreg-l32]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@a971bd64)
[^v2-iethreg-l53]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRenewerV1.json:L110-L158 @ ens_v2@a971bd64)
[^v2-sepolia-dev-iethreg-l53]: (upstream: .refs/ens_v2_sepolia_dev/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2_sepolia_dev@554c309) (upstream: .refs/ens_v2_sepolia_dev/contracts/src/registrar/interfaces/IETHRegistrar.sol:L54 @ ens_v2_sepolia_dev@554c309) (upstream: .refs/ens_v2_sepolia_dev/contracts/src/registrar/interfaces/IETHRegistrar.sol:L59 @ ens_v2_sepolia_dev@554c309) (upstream: .refs/ens_v2_sepolia_dev/contracts/src/registrar/interfaces/IETHRegistrar.sol:L60 @ ens_v2_sepolia_dev@554c309)

[^v2-resroles-l7]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L7 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-resroles-l51]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L52 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L54 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-resroles-l56]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L57 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L59 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-resroles-l61]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L62 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L64 @ ens_v2_sepolia_20260629@ccaeb58)

[^v2-pres-l38]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L33 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l56]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L53 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l70]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L65 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l132]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L142 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l142]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L149 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l153]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L172 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l159]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L178 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l161]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L161 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l230]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L258 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l239]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L273 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l257]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L303 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l282]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L369 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l412]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L508 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l437]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L437 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-l650]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L767 @ ens_v2_sepolia_20260629@ccaeb58)
[^v2-pres-namechanged]: (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L469-L472 @ ens_v2_sepolia_20260629@ccaeb58)

[^bn-namechanged]: (upstream: .refs/basenames/src/L2/resolver/NameResolver.sol:L25-L30 @ basenames@1809bbc)

[^v2-eac-l19]: (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L22 @ ens_v2@a971bd64)
[^v2-eac-l176]: (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L180 @ ens_v2@a971bd64)
[^v2-eac-l181]: (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L454 @ ens_v2@a971bd64)
