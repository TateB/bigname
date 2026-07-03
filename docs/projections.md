# Projections

Projections are read models. Normalized events are the source of truth; projection rows exist to serve stable reads at predictable cost. They carry no semantics that aren't already in the event stream — they replay deterministically from canonical events, and they're disposable.

This document defines the shipped projection set, replay semantics, invalidation, and worker ownership. Wire shapes live in [`api-v1.md`](api-v1.md); event taxonomy and identity rules live in [`architecture.md`](architecture.md); persistence in [`storage.md`](storage.md).

## Rules

- Projections rebuild from canonical facts and normalized events.
- Every row carries provenance, manifest version, and chain-position context.
- Only projection workers write projection tables. Adapters never do. The partner identity
  reverse count/display sidecars are a bounded storage-trigger exception documented in
  [`adrs/0005-identity-count-sidecar.md`](adrs/0005-identity-count-sidecar.md).
- Exact-name reads resolve `at`, `chain_positions`, `consistency` first; the selected positions then key one coherent join across `name_current`, `address_names_current`, `permissions_current`, `record_inventory_current`, `resolver_current`.
- A reader fails closed when the selected positions can't be served from current rows. It does not patch a missing snapshot from raw facts, adapter internals, or a newer projection row.
- A row with an older stored chain-position context may serve a later snapshot only when the reader can prove no newer canonical input exists for the row's keys through those positions. Stored rows may include auxiliary chain positions outside the selected snapshot; the selected chains must still be covered by matching or provably fresh stored positions. Otherwise the worker rebuilds it.
- Source-scoped raw-fact replay is an indexer rule. Projections still consume only canonical normalized events. Coverage, support, and gaps are never inferred from replay scope.
- Resource-keyed projections consume canonical normalized events only when the event's `resource_id` resolves to a canonical `resources` identity row at rebuild time. Events whose resource anchor is absent or noncanonical remain replay/audit input, but they do not publish current projection rows.
- Compact app-facing routes read the same projections as their full counterparts. Compact DTOs may omit provenance, coverage, and internal identifiers, but the underlying rows still carry them for `meta=full`, explain routes, and rebuilds.
- Verified-resolution output is execution-owned. Projections do not synthesize verified answers, do not fall back to declared cache when verified output is missing, and do not cache verified bodies.

## Families

| Projection | Primary key | Primary read | Source events |
| --- | --- | --- | --- |
| `name_current` | `logical_name_id` | exact-name lookup | identity, registration, control, resolver, history heads |
| `address_names_current` | `(address, logical_name_id, relation)` | address-to-names | authority, control, reverse, primary claim |
| `children_current` | `(parent_logical_name_id, child_logical_name_id, surface_class)` | name-to-children | registration, subregistry, alias, wildcard |
| `permissions_current` | `(resource_id, subject, scope)` | resource permissions | permission, scope-modifier, transfer |
| `resolver_current` | `(chain_id, resolver_address)` | resolver overview | resolver, alias, permission, inventory |
| `record_inventory_current` | `(resource_id, record_version_boundary_key)` | record inventory + cache | record, version-boundary |
| `primary_names_current` | `(address, coin_type, namespace)` | primary-name claim anchor | reverse, primary claim, verified primary |

`surface_bindings` is an immutable history table — exact-name reads pull the active row by `logical_name_id` and `tstzrange`, not from a `_current` projection. Coverage, surface bindings, and execution traces are not separate projection families. The all-current replay summary lists `coverage_current` and `surface_bindings_current` as zero-row placeholders for forward compatibility.

History reads consume canonical normalized events plus thin cursor support. There is no separate denormalized history table.

### App-facing route to projection

| Route | Owner |
| --- | --- |
| `GET /v1/names` | `name_current` for exact and search rows; `address_names_current` for relation membership; `children_current` and `record_inventory_current` only for compact counts |
| `GET /v1/names/{namespace}/{name}/records` | `name_current` resolver summary plus `record_inventory_current`; verified sections are execution-owned |
| `POST /v1/identity:lookup` | app-facing native identity read over `name_current`, `address_names_current`, `address_names_current_identity_counts`, `address_names_current_identity_feed`, `record_inventory_current`, `primary_names_current`, and projection checkpoint metadata; reverse pages, counts, and compact feed identity rows share the same readable `name_current` eligibility |
| `GET /v1/events`, history `view=compact` | canonical normalized events plus existing history anchor selection |
| `GET /v1/roles`, `GET /v1/names/{namespace}/{name}/roles` | `permissions_current`; `name_current` only for name-to-resource lookup |
| `GET /v1/resources/lookup` | `name_current` |
| `GET /v1/resolvers/{chain_id}/{resolver_address}/overview` | `resolver_current`; `permissions_current` and normalized events join in for sections that declare it |

## Exact-name lookup

`name_current` is keyed by `logical_name_id` and is the API join root for exact-name reads. Handlers may join other families by the selected exact-name identity and positions; they never rebuild exact-name state from raw facts.

Rows return the current binding plus fixed declared sections for registration, authority, control, resolver, record inventory, and history. Unsupported sections stay explicit. Authority falls back to binding identifiers when a richer summary isn't projected. `control` carries `registrant`, `registry_owner`, `latest_event_kind` — narrower than `ControlVector` and `permissions_current`. `resolver` carries `chain_id`, `address`, `latest_event_kind`; both addresses are `null` when the binding has no declared resolver. `history` is two head pointers (`surface_head`, `resource_head`) into the canonical history rows.

Full `name_current` replacement publishes with the reverse-identity sidecar triggers disabled, then rebuilds `address_names_current_identity_counts` and `address_names_current_identity_feed` once from the current public projections in the same transaction. Incremental `name_current` upserts keep row-level sidecar triggers enabled.

For ENSv1, reverse / primary `NameChanged` text supplies a forward-name preimage only.[^v1-namechanged-l10][^v1-namechanged-l18][^v1-revreg-l129][^v1-revreg-l130] Workers may use that preimage to release pending forward-node observations into `name_current`; the reverse claim itself never synthesizes authority, resolver topology, or primary-name truth.

For `namespace=ens` on the `sepolia-dev` profile, declared exact-name profile rows come from `ens_v2_registry_l1` and `ens_v2_registrar_l1`.[^v2-deploy-ethreg][^v2-deploy-ethrc][^v2-iperm-l34][^v2-events-l15][^v2-events-l30][^v2-events-l49][^v2-events-l69][^v2-events-l75][^v2-iethreg-l32][^v2-iethreg-l53] That profile produces no rows for mainnet, reverse or primary, wrapper authority, migration history, universal-resolver entrypoints, verified resolution, execution explain, or out-of-profile resolver-local facts.

For `namespace=basenames`, exact-name truth comes from `basenames_base_registry`, `basenames_base_registrar`, `basenames_base_resolver`. `basenames_base_primary` is ENSv1 Base `L2ReverseRegistrar` claim-intake only; `basenames_l1_compat` and `basenames_execution` do not become alternate exact-name truth.[^bn-readme-l70][^v1-l2rev-base-deploy][^v1-l2rev-event]

The exact-name `resolver` summary identifies the declared target only. Retained ENSv1 generic resolver-local record events feed observed selector and cache facts before profile admission, but full coverage and resolver-overview facts require supported profile admission. ENSv1 admission is per ENS Labs PublicResolver-generation profile, not latest-only.[^v1-ens-l12][^v1-iaddrres-l6][^v1-iaddressres-l6][^v1-itextres-l5] Basenames resolver-local facts are gated by the separate Base-side profile rule.[^bn-registry-l132]

The shipped explain routes `GET /v1/explain/names/{namespace}/{name}/surface-binding` and `GET /v1/explain/names/{namespace}/{name}/authority-control` read the same exact-name target plus `surface_bindings`, `name_current`, `permissions_current`. They add no explain-specific projection.

## Coverage

The shared `Coverage` object is read inline on `GET /v1/names/{namespace}/{name}` and as the body of `GET /v1/coverage/{namespace}/{name}`. Both reads use the same exact-name snapshot selector and return the same answer for the same `{namespace, name}` and selected positions.

For the ENSv2 `sepolia-dev` exact-name profile: `status=full`, `exhaustiveness=authoritative`, `source_classes_considered=["ens_v2_registry_l1","ens_v2_registrar_l1"]`, `enumeration_basis=exact_name_profile`, `unsupported_reason=null`.[^v2-deploy-ethreg][^v2-deploy-ethrc] That row is scoped to declared exact-name profile support only — it does not cover mainnet, reverse, primary, wrapper, migration, universal-resolver entrypoints, verified resolution, execution explain, or out-of-profile resolver-local sections.

`CoverageChanged` updates this state. Capability changes may invalidate or recompute it.

## Address to names

Default unit is the surface, not the resource. `GET /v1/names` without an address relation filter reads `name_current` as the row universe; with `owner`, `registrant`, or `account` filters it reads `address_names_current` membership first and joins back to `name_current` for compact display, sort, and counts.

`owner` is the token-holder filter; `account` plus `relation` is the generalized relation filter; `relation=any` is the deduped union of `registrant`, `token_holder`, `effective_controller` for the same `(namespace, normalized_name)`. Initial relation vocabulary: `registrant`, `token_holder`, `effective_controller`. Callers may request `dedupe_by=resource`. Default sort is `display_name_asc`.

For `namespace=basenames`, membership and relation facets derive from the same Base-side authority and control as exact-name lookup; primary-claim intake and L1 compatibility transport do not create membership rows.[^bn-readme-l70][^v1-l2rev-base-deploy][^v1-l2rev-event]

`include=role_summary` adds `role_summary`, `subname_count`, `record_count`, `status`, `expiry` per item. It does not change supported filters, default grouping, default sort, cursor semantics, or route-level coverage.

- `role_summary` groups current `permissions_current` rows for the row's `resource_id` by `subject`, keeping each subject's `scope` and `effective_powers`. Row-granular grant lineage stays on `permissions_current`.
- `subname_count` reuses `children_current` under the declared direct-child rule.
- `status` and `expiry` mirror the current `ControlVector` for the row's `resource_id`.
- `record_count` counts distinct stable declared record selectors at the current version boundary using the same semantics as `Resolution.record_inventory`. It is not a raw slot count or a verified-query count.

ENSv1 `TextChanged` events that carry a key and value produce selector-specific records (`text:avatar`, etc.) and retain the emitted value in `record_inventory_current.entries`. They are never collapsed into a generic `text` selector.[^v1-itextres-l5][^v1-textres-l21]

Sort keys `name`, `expiry_date`, `registration_date`, `created_at` are projection-backed and replay-stable; ties break by `(namespace, normalized_name, namehash)`. App-facing total counts count the filtered projection row universe before cursor slicing. Unsupported filter and count combinations are explicit; they never fall back to raw fact scans.

`resolved_address` filtering is deferred until a declared record-value equality projection exists for the namespace and selector family.

## Name to children

Default unit is declared direct child nodes. Compact rows for `GET /v1/names/{namespace}/{name}/children` come from `children_current`: child display name, normalized name or unknown-label placeholder, parent-relative label, labelhash where projected, namehash, owner, registrant where available, direct `subname_count` where projected. When the child has a current `name_current` summary, compact rows join it for owner/registrant and count expansion; unknown-label rows remain projection rows and do not become canonical exact-name surfaces.

For ENSv1 and Basenames registry-derived children, `SubregistryChanged` proves the parent node, child node, labelhash, and owner, but the registry event supplies only the label hash for a subnode.[^v1-registry-l45][^v1-registry-l82][^bn-registry-l81][^bn-registry-l120][^bn-registry-l122] Workers publish a declared child row for every current canonical registry edge whose parent surface is known. If the child label is known through a canonical child `name_surfaces` row or a retained label preimage, the row uses the readable child name. If the label is not known, the row uses the explicit placeholder form `[<labelhash-without-0x>].<parent-normalized-name>` for both `normalized_name` and `canonical_display_name`; square brackets are intentionally outside normalized ENS label syntax so clients can recognize unknown-label children. Label preimages may come from admitted on-chain name-bearing events, retained name surfaces, resolver/reverse preimage observations, or an operational rainbow-table import. They are proof-checked facts: bigname normalizes the candidate label and verifies that its keccak labelhash equals the retained `labelhash`. Once verified, source canonicality changes do not invalidate the preimage mapping, and the mapping still does not create exact-name authority, ownership, resolver, record, or primary-name truth. Adding a label preimage invalidates affected historical parent child collections so rebuilds replace matching unknown-label placeholders with readable labels over time. The ENS subgraph performs a similar labelhash-to-label lookup through `ens.nameByHash` before assembling `label.parent` names.[^ens-subgraph-namebyhash-l111][^ens-subgraph-namebyhash-l126] For ENSv2 `sepolia-dev`, declared direct child and linked-subregistry buckets come from `SubregistryChanged` and `ParentChanged` graph events, not token ID enumeration.[^v2-events-l49][^v2-events-l75][^v2-pr-l131][^v2-pr-l151] For Basenames, declared direct child rows still come from the admitted Base registry / registrar / resolver split, not primary-claim intake or L1 compatibility transport.[^bn-readme-l8][^bn-readme-l69][^bn-readme-l70][^v1-l2rev-base-deploy][^v1-l2rev-event]

Linked, alias-derived, and observed-wildcard children are separate `surface_class` buckets. Default sort is `display_name_asc`. `include=counts` uses the declared direct-child count only; other buckets stay deferred.

## History

Default sort is `chain_position_desc`. `scope=surface|resource|both` maps to normalized-event filters, not different truth systems. Name-history resource scope resolves across every resource ever bound to the requested surface; resource-history surface scope resolves across every surface ever bound to the requested resource. `Address.history` resolves address-derived surface and resource anchor sets first, then applies the same scope contract.

`view=compact` and `GET /v1/events` are presentation views over canonical normalized events. They may remap event kinds into compact `type` aliases and `data`, but they do not introduce a second history projection, include observed or orphaned rows by default, or read raw facts. `GET /v1/events` block filters apply to canonical normalized-event chain positions after the route has selected name, address, or opaque resource anchors. Selector-specific record history is deferred.

## Resource permissions

Keyed by `(resource_id, subject, scope)`. Default unit is the effective permission row for one subject and scope. Resolver-scoped permissions are rows in this family; resolver overview reads summarize them but do not replace them.

For ENSv1 registry-backed resources, registry-only authority uses the current ENS Registry owner as the permission subject and is keyed by the full node/namehash rather than the leftmost labelhash. The registry owner, or an operator approved by that owner, is the on-chain principal authorized to transfer node ownership, transfer or create subnodes, and set a node's resolver.[^v1-registry-l16][^v1-registry-l60][^v1-registry-l71][^v1-registry-l86] Therefore registry-only rows publish resource-scoped `resource_control` and, when a nonzero resolver is declared, resolver-scoped `resolver_control` for that registry owner. Registry-only authority becomes current when the retained registry owner diverges from registrar token control, whether the divergence is observed as a registry-owner change or as a later registrar-token transfer away from that retained registry owner. Registrar renewal updates registrar lease expiry and lineage, but it does not replace divergent registry-only authority unless the registrar and registry subjects realign.

`PermissionScopeChanged` is a modifier input for the same `resource_id`, not a subject grant and not a separate ledger. Workers apply the latest canonical scope modifier for the resource after selecting the current permission grant row (`PermissionChanged` or `RootPermissionChanged`), and they include the modifier in provenance and chain positions when it changes the published row.

For ENSv1 wrapper-backed resources, `PermissionScopeChanged` carries the active NameWrapper fuse value observed from wrapper events. Upstream defines `CANNOT_UNWRAP`, `CANNOT_BURN_FUSES`, `CANNOT_TRANSFER`, `CANNOT_SET_RESOLVER`, `CANNOT_SET_TTL`, `CANNOT_CREATE_SUBDOMAIN`, `CANNOT_APPROVE`, `PARENT_CANNOT_CONTROL`, and emits `NameWrapped` and `FusesSet` carrying fuse values.[^v1-iname-l10][^v1-iname-l11][^v1-iname-l12][^v1-iname-l13][^v1-iname-l14][^v1-iname-l15][^v1-iname-l16][^v1-iname-l18][^v1-iname-l31][^v1-iname-l37]

Fuse application masks `effective_powers`: any power that depends on a wrapper operation prohibited by the active fuse set is omitted. Resolver-target mutation depends on `CANNOT_SET_RESOLVER`; TTL on `CANNOT_SET_TTL`; subname creation on `CANNOT_CREATE_SUBDOMAIN` and child `PARENT_CANNOT_CONTROL`; transfer on `CANNOT_TRANSFER`; unwrap on `CANNOT_UNWRAP`; fuse burning on `CANNOT_BURN_FUSES`; wrapper token approval on `CANNOT_APPROVE`.[^v1-nw-l421][^v1-nw-l637][^v1-nw-l666][^v1-nw-l686][^v1-nw-l827][^v1-nw-l1022][^v1-nw-l132] If a coarse ENSv1 power would imply a prohibited operation, the projection drops the coarse power rather than publish an overbroad claim. A burned fuse never appears as an available public power. `coverage.status=full` for a wrapper-backed resource is valid only when the worker considered the current fuse modifier; without that proof, unmasked powers must not be published as fully authoritative.

For ENSv2, `permissions_current` consumes events derived from upstream `EACRolesChanged(resource, account, oldRoleBitmap, newRoleBitmap)` and retains whether each effective power is resource-specific or root-derived, because root roles satisfy resource-level `hasRoles` checks through root fallback.[^v2-eac-l19][^v2-eac-l176][^v2-eac-l181] Registry permissions key to the bigname `resource_id` linked to the upstream registry EAC resource; `TokenRegenerated` updates token attributes without moving permission rows to a successor resource.[^v2-iperm-l34][^v2-events-l69][^v2-pr-l451] Resolver-scoped permissions key by resolver contract instance plus the upstream resolver EAC resource for a whole name, text key, or coin type, as emitted by `NamedResource`, `NamedTextResource`, `NamedAddrResource`.[^v2-iperm-resolver-l14][^v2-pres-l132][^v2-pres-l142][^v2-pres-l153][^v2-pres-l239][^v2-pres-l257][^v2-pres-l282]

For `PermissionScope::Root`, the stored `scope` column is the root marker; the source scope object's `chain_id` and `registry_address` are retained in `inheritance_path` or `grant_source` rather than duplicated in the scope key.

`GET /v1/resources/lookup` reads `name_current` only to expose the current opaque `resource_id` for an exact `{namespace, name}`. `resource_hex` is nullable and deferred. `GET /v1/roles` and `GET /v1/names/{namespace}/{name}/roles` are compact reads over `permissions_current` and may expose `role_bitmap` only when the projection retained a stable bitmap. Name-qualified ENSv2 role reads compose the resolved resource rows with the owning registry's root-resource rows at read time when resource provenance carries the registry root anchor; projections do not fan root rows out onto every resource. `effective_powers` stays API-owned. Row-granular grant lineage stays on `GET /v1/resources/{resource_id}/permissions`.

## Resolver overview

Keyed by `(chain_id, resolver_address)`. Sections: bindings, aliases, permissions, role holders, event and count summaries.

`aliases` reuses the `{status, count, items}` envelope of `bindings`. Items come from current resolver-linked bindings whose `binding_kind=resolver_alias_path`. Resolver-overview alias support stays inside `resolver_current`.

For ENSv1 PublicResolver-generation targets, `bindings`, `aliases`, permissions, role-holder, and event fan-in summaries do not enumerate the current names or resolver-scoped permission rows pointing at a shared resolver address. Those sections return `UnsupportedSummary` with `resolver_binding_enumeration_not_projected` because shared PublicResolver fan-in is unbounded. Exact-name resolver state stays available through `name_current`, `permissions_current`, and resolution projections.

For full `resolver_current` rebuilds, binding, alias, permission, role-holder, and event fan-in may be treated as non-enumerable for bootstrap safety. The worker may publish explicit unsupported sections rather than materialize unbounded fan-in. Point rebuilds may still inspect bounded current binding and permission sets.

For ENSv2, alias mappings come from `AliasChanged` emitted by admitted `PermissionedResolver` instances. The resolver rewrites by longest matching suffix, so `aliases.items` preserves both source and final target DNS-encoded names.[^v2-iperm-resolver-l14][^v2-pres-l56][^v2-pres-l230][^v2-pres-l650]

For ENSv1 and Basenames, `resolver_current` summarizes a resolver only after that resolver address is manifest-admitted or resolver-discovery-admitted into the relevant resolver source family and admitted as a supported profile.[^v1-ens-l12][^bn-registry-l132] A topology edge observed from registry state alone does not create a supported resolver overview.

`GET /v1/resolvers/{chain_id}/{resolver_address}/overview` is the compact DTO over this family. `counts`, `nodes`, `aliases`, `roles`, `events` populate only from `resolver_current`, `permissions_current`, or canonical normalized-event joins explicitly owned by the route. Missing fan-in produces an unsupported section with `null` body — never zero as a substitute for unknown.

## Resolution

`GET /v1/profiles/names/{name}` uses the same exact-name snapshot selector for `data`, declared topology, route-level coverage, record-inventory and cache joins, verified support checks, and verified execution target selection after normalizing the input and inferring the namespace.

Persisted verified output joins the public response only when its stored requested chain positions exactly match the selected exact-name `ChainPositions`. In `mode=verified|both`, missing persisted output for supported ENS Universal Resolver selectors triggers API-driven execution at the selected snapshot; the trace and outcome persist before the response joins them. No `at` and no `chain_positions` means `consistency=head` at the latest stored checkpoint.[^v1-iuniv-l44][^v1-iuniv-l52]

Declared `topology` carries the fixed subdocument `{registry_path, subregistry_path, resolver_path, wildcard, alias, version_boundaries, transport}`.

For ENSv2, `subregistry_path` and `registry_path` consume `SubregistryChanged` and `ParentChanged`; `alias` consumes `AliasChanged`; `wildcard` populates only from observed extended-resolution evidence with a concrete source resolver and matched labels.[^v2-events-l49][^v2-events-l75][^v2-iperm-resolver-l14][^v2-pres-l412]

For Basenames, `topology` keeps Base-side authority on `registry_path` and `resolver_path` and publishes the compatibility hop in `transport`. The current `verified_resolution=supported` class is exact-surface transport-assisted direct path: `resolver_path[0].logical_name_id` equals the route surface, `wildcard.source=null`, `alias.final_target=null`, `subregistry_path=[]`, `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"`.[^bn-readme-l22][^bn-readme-l28][^bn-readme-l29][^bn-readme-l34][^bn-readme-l69][^bn-readme-l70]

`record_inventory_current` is keyed by `(resource_id, version_boundary)` and serves both declared `record_inventory` and declared `record_cache`. They are two declared subdocuments over the same selector space and version boundary; `record_version_boundary` is identical across `topology.version_boundaries`, `record_inventory`, and `record_cache`. `record_inventory.selectors[*]` and `record_cache.entries[*]` share the selector identity tuple `{record_key, record_family, selector_key}`. `selector_key` is `null` for scalar families and a string for parameterized families, so coin types stay textual. `record_cache.entries[*]` use `success|not_found|unsupported`.

For ENSv1 and Basenames, `record_inventory_current` and `record_cache` may consume retained resolver-local record events from the current resolver as event-evidenced selector and cache facts even while resolver-profile admission is pending. Only decoded normalized resolver events are projection inputs.[^v1-ens-l12][^v1-iaddrres-l6][^v1-iaddressres-l6][^v1-itextres-l5][^v1-itextres-l10][^bn-registry-l132][^bn-addrres-l61] Unobserved selectors in a pending family stay `resolver_family_pending`; selectors in an explicitly unsupported family stay `resolver_family_unsupported`. ENSv1 generic resolver-event observation is not a profile fallback: workers ignore pubkey evidence, keep `DataResolver` evidence unsupported for known PublicResolver-generation profiles and pending for unknown implementations, and never use a generic `resolver_record` observation to promote an unlisted family to supported.

For ENSv1 discovered resolver instances, the supported dynamic profile set is ENS Labs PublicResolver-generation-compatible and profile-exact.[^v1-pres-l20][^v1-pres-l31][^v1-pres-l131][^v1-pres-l150][^v1-resbase-l17][^v1-resbase-l23] Older admitted generations do not inherit latest-only NameWrapper awareness, default coin-type fallback, VersionableResolver boundaries, DNS records, text, contenthash, ABI, name, or interface support. For Basenames discovered Base-side resolver instances, the only complete supported dynamic profile is `L2Resolver`-compatible.[^bn-l2resolver-l4][^bn-l2resolver-l16][^bn-l2resolver-l22][^bn-l2resolver-l29][^bn-l2resolver-l182][^bn-l2resolver-l193][^bn-l2resolver-l209][^bn-l2resolver-l225]

After a `record_inventory_current` rebuild, and once after worker bootstrap handoff when worker RPC is configured, the worker may run a bounded text-value hydration pass for observed ENSv1 `text:<key>` selectors whose current resolver is admitted as a supported PublicResolver-compatible text profile. This repairs legacy PublicResolver-generation text events that identify the key but do not retain the emitted value.[^ensnode-legacy-text-l356] The pass runs after the normalized-event backfill/replay row has been rebuilt, batches `text(bytes32,string)` calls through Multicall3, and executes each batch at the current stored chain checkpoint for the row's chain using a hash-pinned block selector. It never uses provider `latest`, never queries the original event-emission block, writes only `success` and `not_found` into `record_inventory_current.entries`, leaves failed or unpinned calls as explicit `unsupported`, and creates no execution traces. The enrichment is projection-owned current-state repair only: normalized events remain replayable without these values, and historical snapshot materialization must use its own selected chain positions rather than reusing a later hydrated current row.

`GET /v1/names/{namespace}/{name}/records` is a compact read over the same resolver summary, `record_inventory_current`, `record_cache` join. Verified values stay execution-owned. `verified_queries` for the supported Basenames class can include persisted CCIP-participating traces only for the exact transport-assisted direct class.[^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191] Other Basenames path classes stay execution-local `unsupported`.

## Primary names

Keyed by `(address, coin_type, namespace)`. The row is the exact-tuple declared claim anchor plus invalidation context for current exact-tuple handling.

For ENS on Ethereum Mainnet, persisted declared claim precedence is reverse-only through `ens_v1_reverse_l1`.[^v1-revreg-deploy][^v1-revreg-l15][^v1-revreg-l74][^v1-revreg-l83][^v1-revreg-l84] The app route may use an ENS/60 on-demand reverse RPC fallback when the persisted tuple is missing; that fallback builds the `addr.reverse` node, reads its ENS registry resolver, calls resolver `name(bytes32)`, and stays route-local without populating `primary_names_current`.[^v1-registry-deploy][^v1-revreg-l137][^v1-registry-l137][^v1-nameresolver-l7][^v1-nameresolver-l11][^v1-nameresolverimpl-l25] In verified modes, the same route-local fallback can verify the claim by calling `addr:60` through the ENS Universal Resolver proxy at provider `latest`; that verified object is not persisted into the projection or execution cache.[^v1-ur-deploy][^v1-iur-l44][^v1-iur-l52] For Basenames, `basenames_base_primary` is declared primary-claim intake from ENSv1's Base `L2ReverseRegistrar` `NameForAddrChanged(address,string)` values at Base coin type `2147492101`. `primary_names_current` carries claim-local lookup and invalidation inputs; it does not become the declared truth family for exact-name, address-name, or children reads.[^v1-l2rev-base-deploy][^v1-l2rev-base-args][^v1-l2rev-event][^v1-l2rev-nameforaddr]

A configured set of legacy event-silent ENSv1 reverse resolver addresses is a narrow hydration exception inside that same reverse-only class. The built-in operational address set is limited to pinned reference-indexer evidence for an event-silent legacy reverse resolver; deployment-specific additions are operational configuration, not upstream deployment claims.[^ensnode-legacy-revresolver-l311][^ensnode-legacy-revresolver-l316] After a full normalized-event replay/backfill rebuild, and once after worker bootstrap handoff when worker RPC is configured, the worker may find current ENS/60 reverse tuples whose latest reverse resolver is one of those configured addresses and query that resolver's `name(bytes32)` value for the reverse node. It may also inspect current registry resolver edges whose node currently points at a configured event-silent reverse resolver even when no `ReverseChanged` tuple was admitted. Because a registry resolver event carries the node hash but not the address preimage, the worker may persist a resolver-edge-only row only when the hydrated name resolves forward for `addr:60` through the ENS Universal Resolver at the same hash-pinned checkpoint to an ETH address whose computed `addr.reverse` node equals that candidate node.[^v1-revreg-l137][^v1-registry-l137][^v1-nameresolver-l7][^v1-iaddrres-l11][^v1-iur-l44][^v1-iur-l52] That forward check is an address-recovery guard for the declared reverse row; it does not populate `verified_primary_name` or create an execution trace. This mirrors text-value hydration: resolver-name batches run through Multicall3 at the current stored Ethereum Mainnet checkpoint with a hash-pinned block selector, never provider `latest`, and it writes only `primary_names_current`. Large operational sweeps publish `primary_names_current` rows in bounded idempotent batches rather than one all-or-nothing transaction; a later pass recomputes the same current candidates and repairs any partially published hydration state. ENSv1 defines the reverse-name profile as `name(bytes32)`, while event-emitting implementations store and emit the same profile value through `NameChanged` when `setName` is used.[^v1-nameresolver-l5][^v1-nameresolver-l11][^v1-nameresolverimpl-l13][^v1-nameresolverimpl-l18][^v1-nameresolverimpl-l28]

Legacy reverse-resolver hydration does not create normalized events, exact-name truth, route-local fallback state, verified output, or execution traces. The row's declared source class remains `ens_v1_reverse_l1`; persisted provenance adds projection-owned hydration metadata with the resolver address, reverse node, hash-pinned checkpoint, tuple-source class (`reverse_claim` or `resolver_edge_forward_confirmed`), and hash-sensitive live-call invalidation boundary: latest successful direct-call block number, block hash, transaction hash, and transaction index. Blank or whitespace-only hydrated values are `not_found` when an admitted reverse tuple already identifies the address; resolver-edge-only rows require a nonblank forward-confirmed name before the address tuple exists. Nonblank values that cannot be normalized are `invalid_name` for admitted reverse tuples and skipped for resolver-edge-only candidates because no exact tuple can be recovered safely. Failed, CCIP/offchain-required, or unpinned calls leave the event-replayed current row unchanged. Resolver-edge-only forward lookup reverts from the Universal Resolver are non-confirmations rather than failed hydration triggers; offchain-required resolver-edge lookups without an existing hydrated row are also non-confirmations because no current row can be made stale, while offchain-required lookups for existing hydrated rows remain failed lookups. Transport, malformed, and unclassified lookup errors remain failed lookups. If the current reverse tuple no longer points at a configured legacy reverse resolver, the worker restores the event-replayed row and removes hydration metadata; if it points at a different configured legacy resolver, the worker rechecks that resolver even without a new retained direct-call observation. A resolver-edge-only hydrated row is removed if its current node no longer points at a configured legacy reverse resolver or no longer forward-confirms the stored address. Historical snapshots must use their own selected chain positions rather than reusing a later hydrated current row.

During live sync, retained selected raw transactions and receipts include successful direct calls to configured legacy event-silent reverse resolver addresses even when the transaction emits no selected logs. Intake copies those successful direct-call identities into durable `event_silent_resolver_call_observations` before raw-log staging compaction can discard the selected transaction and receipt rows. Those observation rows are projection-invalidation triggers only: because the selected transaction shape does not decode resolver calldata into a touched node, the worker conservatively rechecks current ENS/60 reverse tuples using that resolver when the canonical latest observation for that resolver appears, changes, or disappears after reorg. Normalized-event projection apply progress is the complementary trigger for resolver changes, reverse-claim changes, and resolver-edge cleanup that do not have a retained direct-call observation; once apply has no cursor lag, in-flight claims, or currently claimable invalidations, the worker runs one legacy reverse-resolver hydration pass and then returns to trigger-scoped polling. Retry-delayed `primary_names_current` invalidation failures remain hydration blockers because they affect the same projection family; retry-delayed failures for unrelated projection families do not block direct-call-triggered primary hydration. A hydration pass with failed lookups leaves its trigger cause pending so transient provider or offchain failures do not mark a stale hydrated row as current. The worker evaluates reverse-claim, resolver, claim-name, and retained observation inputs at or behind the same stored checkpoint used for the hash-pinned `name(bytes32)` call. Adapters still emit normalized primary-name facts only from admitted events.

Route-level `claimed_primary_name` and `verified_primary_name` share `ResultStatus` but stay distinct: declared claim state and verified execution state never collapse into one projection-owned field. `primary_names_current` does not persist or backfill `verified_primary_name`.[^bn-l1resolver-l13]

Projection-owned `claimed_primary_name` is limited to `success|not_found|unsupported|invalid_name`. Public claimed-local fields beyond bare status are exact-tuple declared `claimed_primary_name.name`, exact-tuple declared `claimed_primary_name.provenance`, and `raw_claim_name` for `invalid_name`.

- `claimed_primary_name.name` comes from the requested row's declared normalized claim-identity source, aligned with the current reverse-only claim precedence, from projection-owned legacy reverse-resolver hydration of that exact row, or from the route-local ENS/60 on-demand reverse RPC fallback when the persisted tuple is missing.[^v1-revreg-l100][^v1-revreg-l123][^v1-revreg-l129][^v1-revreg-l130][^v1-revreg-l137][^v1-registry-l137][^v1-nameresolver-l7][^v1-nameresolver-l11] It is not synthesized from manifest presence, resolver identity alone, verified execution identity, tuple presence alone, or a different tuple. It stays distinct from execution-derived `verified_primary_name.name`.
- `claimed_primary_name.provenance`, when published, is exact-tuple declared-only provenance from the requested row's claim-local inputs, optionally with projection-owned legacy reverse-resolver hydration metadata, or route-local `ens_reverse_rpc` resolver provenance for the ENS/60 on-demand fallback. The worker strips any `verified_primary_name_lookup` or `verified_primary_name_invalidation` hook material and omits `execution_trace_id`.
- `raw_claim_name` is copied verbatim from `primary_names_current.raw_claim_name` for the same exact tuple and only when `claim_status=invalid_name`. Blank or whitespace-only raw claim names are `not_found`; `invalid_name` is reserved for nonblank raw claim names that cannot be normalized. It is never copied into `verified_primary_name`.

The row owns claim-side inputs and invalidation context only — not route-local on-demand fallback selection, execution `request_type`, execution request key, `execution_trace_id`, verified status, verified name identity, verification-local failure payloads, or the route-level join between claim-side and verification-side provenance.

The exact-tuple persisted-readback class and ENS/60 on-demand fallback are the primary-name coverage support classes. Persisted ENS uses `source_classes_considered=["ens_v1_reverse_l1","ens_execution"]`; Basenames uses `["basenames_base_primary","basenames_execution"]`; ENS/60 declared-only fallback uses `["ens_reverse_rpc"]`; ENS/60 fallback with route-local verification uses `["ens_reverse_rpc","ens_execution_rpc"]`. Supported classes publish route-level `status=partial`, `exhaustiveness=non_enumerable`, `enumeration_basis=primary_name_lookup`, and `unsupported_reason=null`.[^v1-revreg-deploy][^v1-ur-deploy][^bn-readme-l22][^v1-l2rev-base-deploy][^v1-l2rev-event] Out-of-class tuples, non-ENS/60 fresh verified-primary execution, and broader address or namespace coverage stay explicit `unsupported` with `source_classes_considered=[]`.

The Basenames exact-tuple `verified_primary_name` support class stays execution-derived under `basenames_execution`. It uses the same route tuple, the request key `{namespace}:{normalized_address}:{coin_type}`, and execution identity `request_type=verified_primary_name`. The matching `primary_names_current` row is the only claim-side anchor.[^v1-l2rev-nameforaddr][^bn-l1resolver-l13]

The `verified_primary_name.provenance` invariant is additive to public publication. When admitted on the exact-tuple persisted-readback class, it reuses `Provenance` as a verification-local section refinement over execution output: `execution_trace_id` plus `manifest_versions` only. Route-local ENS/60 on-demand verification omits this field because it has no persisted execution trace. The primary-name route omits top-level route provenance by default, so clients must read persisted-verification provenance from `verified_primary_name.provenance`.

Tuple presence is a lookup and invalidation hook only. It does not widen claim precedence, admit fallback sources, change route-level coverage outside the exact-tuple class, or imply richer claimed payload support.

## Invalidation

Projection invalidation fires on:

- canonicality change
- manifest version change that affects a consumed capability
- normalized event insertion for a relevant key
- execution invalidation signal where the projection stores a declared cache summary

Invalidation is deterministic and key-scoped. `projection_invalidations` is the shared worker queue for affected projection families plus family-local keys (`logical_name_id`, `resource_id`, address, resolver tuple, or primary-name tuple). The queue has a `state` column; claimable work is `pending`. `projection_normalized_event_changes` is the append-only normalized-event input to that queue, populated by the normalized-event storage trigger for normalized-event inserts and canonicality-state updates. `projection_apply_cursors` records the worker's consumed `change_id` watermark for that input. Manifest, execution, and other non-normalized-event producers enqueue directly into `projection_invalidations` under the same generation rule. The storage-owned `surface_bindings` repair trigger is the bounded identity-row exception: it enqueues `name_current` and `address_names_current` keys when repair updates change `active_to` or `canonicality_state`; adapter code does not write the queue directly. Storage also enqueues `children_current` when a verified label preimage or read-safe parent `name_surfaces` row makes retained canonical registry child edges newly rebuildable. The storage-owned normalized-event repair path has bounded stale-key exceptions when a repaired value removes the old key from future derivation: Basenames primary-claim source repair enqueues both the stale and repaired `primary_names_current` tuple keys; ENSv1 registrar renewal and ENSv1 or Basenames Base registry/registrar event-time resource repairs enqueue old and repaired resource keys for affected resource-keyed projections; ENSv1 registry resolver before-state and authority-epoch resolver-boundary repairs enqueue affected `record_inventory_current` keys; and ENSv1 same-transaction registration setup repair enqueues the repaired `name_current` key plus affected `permissions_current` resource keys. A new invalidation for a key increments that row's generation, clears retry metadata, and returns the row to `pending`; an in-flight apply may delete only the generation it claimed, so a newer change cannot be lost by an older rebuild finishing late.

Projection apply treats repeated deterministic failures as terminal operator-visible work. After five failed apply attempts for the same claimed generation, the worker atomically moves the row out of `projection_invalidations` into `projection_invalidation_dead_letters` with the failed generation, attempt count, key payload, failure reason, failure timestamps, and `state='dead_letter'`. Dead-letter rows are not claimable, do not block primary-name hydration, and do not count as pending projection lag for indexing status because they are no longer live queue rows. They remain durable operator visibility and repair input: a later invalidation for the same `(projection, projection_key)` creates or updates a fresh pending queue row with a new generation instead of mutating the historical dead-letter record.

`record_inventory_current` is still keyed by `resource_id`, but a point rebuild may read retained resolver-local record facts and resolver-boundary events from earlier resources for the same logical name when those inputs are needed to bound the target resource's current resolver tenure. Normalized-event invalidation for record inventory therefore fans out changed resolver and record events to later canonical resources with the same logical name. Cross-resource `ResolverChanged` rows are rebuild inputs only as tenure boundaries; they do not replace the target resource's latest resolver.

Workers derive invalidations from normalized events and apply them in projection dependency order: `name_current`, `children_current`, `permissions_current`, `record_inventory_current`, `resolver_current`, `address_names_current`, `primary_names_current`. `resolver_current` follows `permissions_current` because resolver-scoped permission summaries are projection inputs. Durable claims are leases, not ownership transfers: if a worker exits mid-apply, another worker may reclaim a claimed invalidation after the retry delay and apply the same generation. Claims are heartbeated for every row in the claimed batch while a worker is applying the batch. Workers also take a per-key apply lock around rebuild plus queue completion/failure so two workers cannot publish the same projection key out of generation order. No projection refreshes from broad time-based polling; legacy reverse-resolver hydration is trigger-scoped by `event_silent_resolver_call_observations`, by projection-apply progress for normalized-event changes that may affect current reverse resolver edges, and by stored hydration provenance because the retained direct-call shape does not identify a single touched reverse node.

## Rebuild

Every projection supports point rebuild by key, range rebuild by chain position, and full rebuild from canonical events. Point rebuilds must use the family key to bound their canonical input set before recomputing the row; they must not scan unrelated current projection inputs on every invalidation. Worker modes: continuous apply, backfill apply, reorg repair, one-shot rebuild.

Fresh normalized replay may defer normalized-event indexes used only by projection or API readback while current projection tables are empty. Rebuild tooling treats those indexes as part of its readiness boundary: before full current-state rebuilds count as ready for API reads, the deferred indexes must exist again.

### Rewind and historical snapshots

Projection rewind is worker-owned deterministic rebuild to selected `ChainPositions`. It reads canonical normalized events and manifest inputs whose block identities are eligible at the requested snapshot, then rebuilds only the requested family/key set or range. `observed` and `orphaned` rows are excluded from normal rewind outputs.

When the selected snapshot is the latest eligible chain position for the projection key, the worker may publish into the current table. Older snapshots must be materialized with exact chain-position context, either in snapshot-scoped rows or an equivalent bounded cache. They must not overwrite newer current rows. If no eligible materialization exists, public routes return `stale`; they do not answer from raw facts, adapter internals, provider `latest`, or a newer projection row.

Reorg repair uses the same machinery after canonicality updates enqueue key-scoped invalidations. The apply path consumes normalized-event insert and canonicality-change records, rebuilds affected keys in dependency order, and only deletes the invalidation generation it claimed.

### Replay status tracking

`current_projection_replay_status` records durable worker-owned completion markers per projection family after a family publishes successfully. Columns: `projection`, `replay_version`, `completed_normalized_target_block`, `requested_key_count`, `upserted_row_count`, `deleted_row_count`, `completed_at`.

On worker restart before continuous apply has taken over, automatic bootstrap replay may skip a family when its marker's `replay_version` matches the current code's replay version and `completed_normalized_target_block` covers the requested normalized replay target. Once the normalized-event apply cursor exists and every current projection family has a current-version replay marker, the worker treats bootstrap as handed off and resumes continuous apply instead of forcing another full replay for newly advancing normalized blocks. Replay markers are bootstrap/full-rebuild resume aids only; they are not live-readiness signals and do not prove that projections have consumed normalized events after the recorded target. Continuous projection catch-up is owned by `projection_apply_cursors` and `projection_invalidations`.

When a bootstrap full replay actually rebuilds projection keys, the worker seeds the normalized-event apply cursor to the normalized-event change watermark captured before that replay began; an empty change log is watermark `0`. The bootstrap replay target must cover both the completed normalized replay cursor target and the greatest persisted chain-checkpoint block visible at that handoff, so seeding the cursor cannot skip live normalized events that arrived after historical replay completed but before projection apply took over. Events inserted or canonicality-updated after the captured watermark are still consumed through key-scoped invalidation. Schema migrations install the forward change log and trigger without bulk-copying historical `normalized_events`; historical baseline catch-up is owned by worker full/backfill replay. If a deployment already has old replay markers but no apply cursor, the worker starts from the beginning of `projection_normalized_event_changes` and derives key-scoped invalidations rather than trusting the replay markers as a live cursor; markers that do not cover the requested normalized replay target force replay first.

Explicit one-shot rebuild commands are force rebuilds. They clear any stale marker before rebuilding so a failed rebuild cannot leave a misleading completion marker behind.

### Replay summary (operational tooling)

`bigname-worker replay all-current-projections` is worker-owned operational tooling. Its `--json` output is operational, not a public `v1` API contract.

```json
{
  "command": "all-current-projections",
  "projections": [
    { "projection": "address_names_current", "requested": 0, "upserted": 0, "deleted": 0 }
  ],
  "totals": { "requested": 0, "upserted": 0, "deleted": 0 }
}
```

- `command` is always `all-current-projections`.
- `projections` lists one object per current family in stable identifier order: `address_names_current`, `children_current`, `coverage_current`, `name_current`, `permissions_current`, `primary_names_current`, `record_inventory_current`, `resolver_current`, `surface_bindings_current`. Families with no shipped rebuild orchestrator (`coverage_current`, `surface_bindings_current`) appear with zero counts.
- Each projection object carries exactly `projection`, `requested`, `upserted`, `deleted`, all non-negative integers.
- `totals` carries `requested`, `upserted`, `deleted` summed across the per-projection counts.
- The summary describes the completed worker command attempt only. It is not stored as projection truth and is not a replay checkpoint.

## Index baseline

Indexes that match the public contract:

- `name_current(logical_name_id)`
- `address_names_current(address, namespace, canonical_display_name, logical_name_id)`
- `address_names_current(logical_name_id, relation, address)` for identity forward relation hydration
- `address_names_current(address, relation, normalized_name, namespace, namehash, logical_name_id)` for identity reverse pagination
- `children_current(parent_logical_name_id, surface_class, canonical_display_name, child_logical_name_id)`
- `permissions_current(resource_id, subject, scope)`
- `resolver_current(chain_id, resolver_address)`
- `primary_names_current(address, coin_type, namespace)`

More indexes land only after measured query evidence. Compact routes may need additional measured indexes — they do not create new truth families. Candidates: name search over `(namespace, normalized_name)`; address relation filters over `(address, relation, namespace)`; sort support for expiry, registration, `created_at`; normalized-event filters for `GET /v1/events`; permission filters over `(subject, resource_id)` plus any projected `role_bitmap`.

## Ownership

- Adapters emit normalized events. They never write projection rows.
- Projection workers read normalized events and manifests. They own every projection write.
- API handlers read projections and execution output. They never write either.
- Execution workers may publish invalidation signals but do not mutate declared projections outside their owned cache summaries.

Workers own one family each: `name_current`, `address_names_current`, `children_current`, `permissions_current`, `record_inventory_current`, `resolver_current`, `primary_names_current`. Each lives under `apps/worker/src/<family>/` with its own projection, rebuild, and tests. Replay orchestration lives in `apps/worker/src/replay/` and runs the families in the order above so cross-family inputs are stable when later families read them.

---

[^bn-readme-l8]: (upstream: .refs/basenames/README.md:L8 @ basenames@1809bbc)
[^bn-readme-l22]: (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc)
[^bn-readme-l28]: (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc)
[^bn-readme-l29]: (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc)
[^bn-readme-l33]: (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc)
[^bn-readme-l34]: (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc)
[^bn-readme-l69]: (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)
[^bn-readme-l70]: (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)

[^bn-l1resolver-l13]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
[^bn-l1resolver-l154]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)
[^bn-l1resolver-l173]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc)
[^bn-l1resolver-l191]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)

[^bn-registry-l81]: (upstream: .refs/basenames/src/L2/Registry.sol:L81 @ basenames@1809bbc)
[^bn-registry-l120]: (upstream: .refs/basenames/src/L2/Registry.sol:L120 @ basenames@1809bbc)
[^bn-registry-l122]: (upstream: .refs/basenames/src/L2/Registry.sol:L122 @ basenames@1809bbc)
[^bn-registry-l132]: (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
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
[^bn-addrres-l61]: (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L61 @ basenames@1809bbc)

[^v1-ens-l12]: (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)
[^v1-iaddrres-l6]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f)
[^v1-iaddrres-l11]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L11 @ ens_v1@91c966f)
[^v1-iaddressres-l6]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f)
[^v1-itextres-l5]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f)
[^v1-itextres-l10]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L10 @ ens_v1@91c966f)
[^v1-textres-l21]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L21 @ ens_v1@91c966f)
[^v1-namechanged-l10]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L10 @ ens_v1@91c966f)
[^v1-namechanged-l18]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L18 @ ens_v1@91c966f)

[^v1-pres-l20]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^v1-pres-l31]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f)
[^v1-pres-l131]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f)
[^v1-pres-l150]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f)
[^v1-resbase-l17]: (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f)
[^v1-resbase-l23]: (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L23 @ ens_v1@91c966f)
[^ensnode-legacy-text-l356]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L356 @ ensnode@2017ae6) (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L364 @ ensnode@2017ae6)

[^v1-iname-l10]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10 @ ens_v1@91c966f)
[^v1-iname-l11]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L11 @ ens_v1@91c966f)
[^v1-iname-l12]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L12 @ ens_v1@91c966f)
[^v1-iname-l13]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L13 @ ens_v1@91c966f)
[^v1-iname-l14]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L14 @ ens_v1@91c966f)
[^v1-iname-l15]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L15 @ ens_v1@91c966f)
[^v1-iname-l16]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L16 @ ens_v1@91c966f)
[^v1-iname-l18]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L18 @ ens_v1@91c966f)
[^v1-iname-l31]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L31 @ ens_v1@91c966f)
[^v1-iname-l37]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f)

[^v1-nw-l132]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L132 @ ens_v1@91c966f)
[^v1-nw-l421]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L421 @ ens_v1@91c966f)
[^v1-nw-l637]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L637 @ ens_v1@91c966f)
[^v1-nw-l666]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L666 @ ens_v1@91c966f)
[^v1-nw-l686]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L686 @ ens_v1@91c966f)
[^v1-nw-l827]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L827 @ ens_v1@91c966f)
[^v1-nw-l1022]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022 @ ens_v1@91c966f)

[^v1-revreg-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-ur-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)
[^v1-iur-l44]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L44 @ ens_v1@91c966f)
[^v1-iur-l52]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L52 @ ens_v1@91c966f)
[^v1-revreg-l15]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f)
[^v1-revreg-l74]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f)
[^v1-revreg-l83]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f)
[^v1-revreg-l84]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
[^v1-revreg-l100]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f)
[^v1-revreg-l123]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f)
[^v1-revreg-l129]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f)
[^v1-revreg-l130]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
[^v1-registry-l16]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L16 @ ens_v1@91c966f)
[^v1-registry-l45]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L45 @ ens_v1@91c966f)
[^v1-registry-l60]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60 @ ens_v1@91c966f)
[^v1-registry-l71]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L71 @ ens_v1@91c966f)
[^v1-registry-l82]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L82 @ ens_v1@91c966f)
[^v1-registry-l86]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L86 @ ens_v1@91c966f)
[^ens-subgraph-namebyhash-l111]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L111 @ ens_subgraph@723f1b6)
[^ens-subgraph-namebyhash-l126]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L126 @ ens_subgraph@723f1b6)
[^v1-l2rev-base-deploy]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-l2rev-base-args]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f)
[^v1-l2rev-event]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
[^v1-l2rev-nameforaddr]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L154 @ ens_v1@91c966f)
[^v1-registry-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ENSRegistry.json:L2 @ ens_v1@91c966f)
[^v1-revreg-l137]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L137 @ ens_v1@91c966f)
[^v1-registry-l137]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137 @ ens_v1@91c966f)
[^v1-nameresolver-l5]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L5 @ ens_v1@91c966f)
[^v1-nameresolver-l7]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L7 @ ens_v1@91c966f)
[^v1-nameresolver-l11]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L11 @ ens_v1@91c966f)
[^v1-nameresolverimpl-l13]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L13 @ ens_v1@91c966f)
[^v1-nameresolverimpl-l18]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L18 @ ens_v1@91c966f)
[^v1-nameresolverimpl-l25]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L25 @ ens_v1@91c966f)
[^v1-nameresolverimpl-l28]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L28 @ ens_v1@91c966f)
[^ensnode-legacy-revresolver-l311]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L311 @ ensnode@2017ae6)
[^ensnode-legacy-revresolver-l316]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L316 @ ensnode@2017ae6)

[^v1-iuniv-l44]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L44 @ ens_v1@91c966f)
[^v1-iuniv-l52]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L52 @ ens_v1@91c966f)

[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309)
[^v2-deploy-ethrc]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)

[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309)
[^v2-events-l15]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309)
[^v2-events-l30]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L30 @ ens_v2@554c309)
[^v2-events-l49]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L49 @ ens_v2@554c309)
[^v2-events-l69]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309)
[^v2-events-l75]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L75 @ ens_v2@554c309)
[^v2-iethreg-l32]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309)
[^v2-iethreg-l53]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309)

[^v2-pr-l131]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L131 @ ens_v2@554c309)
[^v2-pr-l151]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L151 @ ens_v2@554c309)
[^v2-pr-l451]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309)

[^v2-iperm-resolver-l14]: (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14 @ ens_v2@554c309)
[^v2-pres-l56]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L56 @ ens_v2@554c309)
[^v2-pres-l132]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L132 @ ens_v2@554c309)
[^v2-pres-l142]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L142 @ ens_v2@554c309)
[^v2-pres-l153]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L153 @ ens_v2@554c309)
[^v2-pres-l230]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L230 @ ens_v2@554c309)
[^v2-pres-l239]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L239 @ ens_v2@554c309)
[^v2-pres-l257]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L257 @ ens_v2@554c309)
[^v2-pres-l282]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L282 @ ens_v2@554c309)
[^v2-pres-l412]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L412 @ ens_v2@554c309)
[^v2-pres-l650]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L650 @ ens_v2@554c309)

[^v2-eac-l19]: (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309)
[^v2-eac-l176]: (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L176 @ ens_v2@554c309)
[^v2-eac-l181]: (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L181 @ ens_v2@554c309)
