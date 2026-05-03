# Projections

Projections are read models. Normalized events are the source of truth; projection rows exist to serve stable reads at predictable cost. They carry no semantics that aren't already in the event stream — they replay deterministically from canonical events, and they're disposable.

This document defines the shipped projection set, replay semantics, invalidation, and worker ownership. Wire shapes live in [`api-v1.md`](api-v1.md); event taxonomy and identity rules live in [`architecture.md`](architecture.md); persistence in [`storage.md`](storage.md).

## Rules

- Projections rebuild from canonical facts and normalized events.
- Every row carries provenance, manifest version, and chain-position context.
- Only projection workers write projection tables. Adapters never do.
- Exact-name reads resolve `at`, `chain_positions`, `consistency` first; the selected positions then key one coherent join across `name_current`, `address_names_current`, `permissions_current`, `record_inventory_current`, `resolver_current`.
- A reader fails closed when the selected positions can't be served from current rows. It does not patch a missing snapshot from raw facts, adapter internals, or a newer projection row.
- A row with an older stored chain-position context may serve a later snapshot only when the reader can prove no newer canonical input exists for the row's keys through those positions. Otherwise the worker rebuilds it.
- Source-scoped raw-fact replay is an indexer rule. Projections still consume only canonical normalized events. Coverage, support, and gaps are never inferred from replay scope.
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
| `GET /v1/addresses/{address}/names/count` | `address_names_current` with the same name and search joins as `GET /v1/names` |
| `GET /v1/names/{namespace}/{name}/records` | `name_current` resolver summary plus `record_inventory_current`; verified sections are execution-owned |
| `GET /v1/events`, history `view=compact` | canonical normalized events plus existing history anchor selection |
| `GET /v1/roles`, `GET /v1/names/{namespace}/{name}/roles` | `permissions_current`; `name_current` only for name-to-resource lookup |
| `GET /v1/resources/lookup` | `name_current` |
| `GET /v1/resolvers/{chain_id}/{resolver_address}/overview` | `resolver_current`; `permissions_current` and normalized events join in for sections that declare it |

## Exact-name lookup

`name_current` is keyed by `logical_name_id` and is the API join root for exact-name reads. Handlers may join other families by the selected exact-name identity and positions; they never rebuild exact-name state from raw facts.

Rows return the current binding plus fixed declared sections for registration, authority, control, resolver, record inventory, and history. Unsupported sections stay explicit. Authority falls back to binding identifiers when a richer summary isn't projected. `control` carries `registrant`, `registry_owner`, `latest_event_kind` — narrower than `ControlVector` and `permissions_current`. `resolver` carries `chain_id`, `address`, `latest_event_kind`; both addresses are `null` when the binding has no declared resolver. `history` is two head pointers (`surface_head`, `resource_head`) into the canonical history rows.

For ENSv1, reverse / primary `NameChanged` text supplies a forward-name preimage only.[^v1-namechanged-l10][^v1-namechanged-l18][^v1-revreg-l129][^v1-revreg-l130] Workers may use that preimage to release pending forward-node observations into `name_current`; the reverse claim itself never synthesizes authority, resolver topology, or primary-name truth.

For `namespace=ens` on the `sepolia-dev` profile, declared exact-name profile rows come from `ens_v2_registry_l1` and `ens_v2_registrar_l1`.[^v2-deploy-ethreg][^v2-deploy-ethrc][^v2-iperm-l34][^v2-events-l15][^v2-events-l30][^v2-events-l49][^v2-events-l69][^v2-events-l75][^v2-iethreg-l32][^v2-iethreg-l53] That profile produces no rows for mainnet, reverse or primary, wrapper authority, migration history, universal-resolver entrypoints, verified resolution, execution explain, or out-of-profile resolver-local facts.

For `namespace=basenames`, exact-name truth comes from `basenames_base_registry`, `basenames_base_registrar`, `basenames_base_resolver`. `basenames_base_primary` is claim-intake only; `basenames_l1_compat` and `basenames_execution` do not become alternate exact-name truth.[^bn-readme-l70][^bn-revreg-l12]

The exact-name `resolver` summary identifies the declared target only. Retained ENSv1 generic resolver-local record events feed observed selector and cache facts before profile admission, but full coverage and resolver-overview facts require supported profile admission. ENSv1 admission is per ENS Labs PublicResolver-generation profile, not latest-only.[^v1-ens-l12][^v1-iaddrres-l6][^v1-iaddressres-l6][^v1-itextres-l5] Basenames resolver-local facts are gated by the separate Base-side profile rule.[^bn-registry-l132]

The shipped explain routes `GET /v1/explain/names/{namespace}/{name}/surface-binding` and `GET /v1/explain/names/{namespace}/{name}/authority-control` read the same exact-name target plus `surface_bindings`, `name_current`, `permissions_current`. They add no explain-specific projection.

## Coverage

The shared `Coverage` object is read inline on `GET /v1/names/{namespace}/{name}` and as the body of `GET /v1/coverage/{namespace}/{name}`. Both reads use the same exact-name snapshot selector and return the same answer for the same `{namespace, name}` and selected positions.

For the ENSv2 `sepolia-dev` exact-name profile: `status=full`, `exhaustiveness=authoritative`, `source_classes_considered=["ens_v2_registry_l1","ens_v2_registrar_l1"]`, `enumeration_basis=exact_name_profile`, `unsupported_reason=null`.[^v2-deploy-ethreg][^v2-deploy-ethrc] That row is scoped to declared exact-name profile support only — it does not cover mainnet, reverse, primary, wrapper, migration, universal-resolver entrypoints, verified resolution, execution explain, or out-of-profile resolver-local sections.

`CoverageChanged` updates this state. Capability changes may invalidate or recompute it.

## Address to names

Default unit is the surface, not the resource. `GET /v1/names` without an address relation filter reads `name_current` as the row universe; with `owner`, `registrant`, or `account` filters it reads `address_names_current` membership first and joins back to `name_current` for compact display, sort, and counts.

`owner` is the token-holder filter; `account` plus `relation` is the generalized relation filter; `relation=any` is the deduped union of `registrant`, `token_holder`, `effective_controller` for the same `(namespace, normalized_name)`. Initial relation vocabulary: `registrant`, `token_holder`, `effective_controller`. Callers may request `dedupe_by=resource`. Default sort is `display_name_asc`.

For `namespace=basenames`, membership and relation facets derive from the same Base-side authority and control as exact-name lookup; reverse-claim intake and L1 compatibility transport do not create membership rows.[^bn-readme-l70][^bn-revreg-l12]

`include=role_summary` adds `role_summary`, `subname_count`, `record_count`, `status`, `expiry` per item. It does not change supported filters, default grouping, default sort, cursor semantics, or route-level coverage.

- `role_summary` groups current `permissions_current` rows for the row's `resource_id` by `subject`, keeping each subject's `scope` and `effective_powers`. Row-granular grant lineage stays on `permissions_current`.
- `subname_count` reuses `children_current` under the declared direct-child rule.
- `status` and `expiry` mirror the current `ControlVector` for the row's `resource_id`.
- `record_count` counts distinct stable declared record selectors at the current version boundary using the same semantics as `Resolution.record_inventory`. It is not a raw slot count or a verified-query count.

ENSv1 `TextChanged` events that carry a key and value produce selector-specific records (`text:avatar`, etc.) and retain the emitted value in `record_inventory_current.entries`. They are never collapsed into a generic `text` selector.[^v1-itextres-l5][^v1-textres-l21]

Sort keys `name`, `expiry_date`, `registration_date`, `created_at` are projection-backed and replay-stable; ties break by `(namespace, normalized_name, namehash)`. App-facing total counts and `GET /v1/addresses/{address}/names/count` count the filtered projection row universe before cursor slicing. Unsupported filter and count combinations are explicit; they never fall back to raw fact scans.

`resolved_address` filtering is deferred until a declared record-value equality projection exists for the namespace and selector family.

## Name to children

Default unit is declared direct child surfaces. Compact rows for `GET /v1/names/{namespace}/{name}/children` come from `children_current` joined to the child's current `name_current` summary: child display name, normalized name, parent-relative label, labelhash where projected, namehash, owner, registrant where available, direct `subname_count` where projected.

For ENSv2 `sepolia-dev`, declared direct child and linked-subregistry buckets come from `SubregistryChanged` and `ParentChanged` graph events, not token ID enumeration.[^v2-events-l49][^v2-events-l75][^v2-pr-l131][^v2-pr-l151] For Basenames, declared direct child rows come from the admitted Base registry / registrar / resolver split, not primary-claim intake or L1 compatibility transport.[^bn-readme-l8][^bn-readme-l69][^bn-readme-l70][^bn-revreg-l12]

Linked, alias-derived, and observed-wildcard children are separate `surface_class` buckets. Default sort is `display_name_asc`. `include=counts` uses the declared direct-child count only; other buckets stay deferred.

## History

Default sort is `chain_position_desc`. `scope=surface|resource|both` maps to normalized-event filters, not different truth systems. Name-history resource scope resolves across every resource ever bound to the requested surface; resource-history surface scope resolves across every surface ever bound to the requested resource. `Address.history` resolves address-derived surface and resource anchor sets first, then applies the same scope contract.

`view=compact` and `GET /v1/events` are presentation views over canonical normalized events. They may remap event kinds into compact `type` aliases and `data`, but they do not introduce a second history projection, include observed or orphaned rows by default, or read raw facts. `GET /v1/events` block filters apply to canonical normalized-event chain positions after the route has selected name, address, or opaque resource anchors. Selector-specific record history is deferred.

## Resource permissions

Keyed by `(resource_id, subject, scope)`. Default unit is the effective permission row for one subject and scope. Resolver-scoped permissions are rows in this family; resolver overview reads summarize them but do not replace them.

`PermissionScopeChanged` is a modifier input for the same `resource_id`, not a subject grant and not a separate ledger. Workers apply the latest canonical scope modifier at or before the row's chain-position context after selecting the current `PermissionChanged` row, and they include the modifier in provenance and chain positions when it changes the published row.

For ENSv1 wrapper-backed resources, `PermissionScopeChanged` carries the active NameWrapper fuse value observed from wrapper events. Upstream defines `CANNOT_UNWRAP`, `CANNOT_BURN_FUSES`, `CANNOT_TRANSFER`, `CANNOT_SET_RESOLVER`, `CANNOT_SET_TTL`, `CANNOT_CREATE_SUBDOMAIN`, `CANNOT_APPROVE`, `PARENT_CANNOT_CONTROL`, and emits `NameWrapped` and `FusesSet` carrying fuse values.[^v1-iname-l10][^v1-iname-l11][^v1-iname-l12][^v1-iname-l13][^v1-iname-l14][^v1-iname-l15][^v1-iname-l16][^v1-iname-l18][^v1-iname-l31][^v1-iname-l37]

Fuse application masks `effective_powers`: any power that depends on a wrapper operation prohibited by the active fuse set is omitted. Resolver-target mutation depends on `CANNOT_SET_RESOLVER`; TTL on `CANNOT_SET_TTL`; subname creation on `CANNOT_CREATE_SUBDOMAIN` and child `PARENT_CANNOT_CONTROL`; transfer on `CANNOT_TRANSFER`; unwrap on `CANNOT_UNWRAP`; fuse burning on `CANNOT_BURN_FUSES`; wrapper token approval on `CANNOT_APPROVE`.[^v1-nw-l421][^v1-nw-l637][^v1-nw-l666][^v1-nw-l686][^v1-nw-l827][^v1-nw-l1022][^v1-nw-l132] If a coarse ENSv1 power would imply a prohibited operation, the projection drops the coarse power rather than publish an overbroad claim. A burned fuse never appears as an available public power. `coverage.status=full` for a wrapper-backed resource is valid only when the worker considered the current fuse modifier; without that proof, unmasked powers must not be published as fully authoritative.

For ENSv2, `permissions_current` consumes events derived from upstream `EACRolesChanged(resource, account, oldRoleBitmap, newRoleBitmap)` and retains whether each effective power is resource-specific or root-derived, because root roles satisfy resource-level `hasRoles` checks through root fallback.[^v2-eac-l19][^v2-eac-l176][^v2-eac-l181] Registry permissions key to the bigname `resource_id` linked to the upstream registry EAC resource; `TokenRegenerated` updates token attributes without moving permission rows to a successor resource.[^v2-iperm-l34][^v2-events-l69][^v2-pr-l451] Resolver-scoped permissions key by resolver contract instance plus the upstream resolver EAC resource for a whole name, text key, or coin type, as emitted by `NamedResource`, `NamedTextResource`, `NamedAddrResource`.[^v2-iperm-resolver-l14][^v2-pres-l132][^v2-pres-l142][^v2-pres-l153][^v2-pres-l239][^v2-pres-l257][^v2-pres-l282]

`GET /v1/resources/lookup` reads `name_current` only to expose the current opaque `resource_id` for an exact `{namespace, name}`. `resource_hex` is nullable and deferred. `GET /v1/roles` and `GET /v1/names/{namespace}/{name}/roles` are compact reads over `permissions_current` and may expose `role_bitmap` only when the projection retained a stable bitmap. `effective_powers` stays API-owned. Row-granular grant lineage stays on `GET /v1/resources/{resource_id}/permissions`.

## Resolver overview

Keyed by `(chain_id, resolver_address)`. Sections: bindings, aliases, permissions, role holders, event and count summaries.

`aliases` reuses the `{status, count, items}` envelope of `bindings`. Items come from current resolver-linked bindings whose `binding_kind=resolver_alias_path`. Resolver-overview alias support stays inside `resolver_current`.

For ENSv1 PublicResolver-generation targets, `bindings`, `aliases`, and event fan-in summaries do not enumerate the current names pointing at a shared resolver address. Those sections return `UnsupportedSummary` with `resolver_binding_enumeration_not_projected` because shared PublicResolver fan-in is unbounded. Exact-name resolver state stays available through `name_current` and resolution projections.

For full `resolver_current` rebuilds, binding, alias, permission, role-holder, and event fan-in may be treated as non-enumerable for bootstrap safety. The worker may publish explicit unsupported sections rather than materialize unbounded fan-in. Point rebuilds may still inspect the current binding and permission set.

For ENSv2, alias mappings come from `AliasChanged` emitted by admitted `PermissionedResolver` instances. The resolver rewrites by longest matching suffix, so `aliases.items` preserves both source and final target DNS-encoded names.[^v2-iperm-resolver-l14][^v2-pres-l56][^v2-pres-l230][^v2-pres-l650]

For ENSv1 and Basenames, `resolver_current` summarizes a resolver only after that resolver address is manifest-admitted or resolver-discovery-admitted into the relevant resolver source family and admitted as a supported profile.[^v1-ens-l12][^bn-registry-l132] A topology edge observed from registry state alone does not create a supported resolver overview.

`GET /v1/resolvers/{chain_id}/{resolver_address}/overview` is the compact DTO over this family. `counts`, `nodes`, `aliases`, `roles`, `events` populate only from `resolver_current`, `permissions_current`, or canonical normalized-event joins explicitly owned by the route. Missing fan-in produces an unsupported section with `null` body — never zero as a substitute for unknown.

## Resolution

`GET /v1/resolutions/{namespace}/{name}` uses the same exact-name snapshot selector for `data`, declared topology, route-level coverage, record-inventory and cache joins, verified support checks, and verified execution target selection.

Persisted verified output joins the public response only when its stored requested chain positions exactly match the selected exact-name `ChainPositions`. In `mode=verified|both`, missing persisted output for supported ENS Universal Resolver selectors triggers API-driven execution at the selected snapshot; the trace and outcome persist before the response joins them. No `at` and no `chain_positions` means `consistency=head` at the latest stored checkpoint.[^v1-iuniv-l44][^v1-iuniv-l52]

Declared `topology` carries the fixed subdocument `{registry_path, subregistry_path, resolver_path, wildcard, alias, version_boundaries, transport}`.

For ENSv2, `subregistry_path` and `registry_path` consume `SubregistryChanged` and `ParentChanged`; `alias` consumes `AliasChanged`; `wildcard` populates only from observed extended-resolution evidence with a concrete source resolver and matched labels.[^v2-events-l49][^v2-events-l75][^v2-iperm-resolver-l14][^v2-pres-l412]

For Basenames, `topology` keeps Base-side authority on `registry_path` and `resolver_path` and publishes the compatibility hop in `transport`. The current `verified_resolution=supported` class is exact-surface transport-assisted direct path: `resolver_path[0].logical_name_id` equals the route surface, `wildcard.source=null`, `alias.final_target=null`, `subregistry_path=[]`, `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"`.[^bn-readme-l22][^bn-readme-l28][^bn-readme-l29][^bn-readme-l34][^bn-readme-l69][^bn-readme-l70]

`record_inventory_current` is keyed by `(resource_id, version_boundary)` and serves both declared `record_inventory` and declared `record_cache`. They are two declared subdocuments over the same selector space and version boundary; `record_version_boundary` is identical across `topology.version_boundaries`, `record_inventory`, and `record_cache`. `record_inventory.selectors[*]` and `record_cache.entries[*]` share the selector identity tuple `{record_key, record_family, selector_key}`. `selector_key` is `null` for scalar families and a string for parameterized families, so coin types stay textual. `record_cache.entries[*]` use `success|not_found|unsupported`.

For ENSv1 and Basenames, `record_inventory_current` and `record_cache` may consume retained resolver-local record events from the current resolver as event-evidenced selector and cache facts even while resolver-profile admission is pending. Only decoded normalized resolver events are projection inputs.[^v1-ens-l12][^v1-iaddrres-l6][^v1-iaddressres-l6][^v1-itextres-l5][^v1-itextres-l10][^bn-registry-l132][^bn-addrres-l61] Unobserved selectors in a pending family stay `resolver_family_pending`; selectors in an explicitly unsupported family stay `resolver_family_unsupported`. ENSv1 generic resolver-event observation is not a profile fallback: workers ignore pubkey evidence, keep `DataResolver` evidence unsupported for known PublicResolver-generation profiles and pending for unknown implementations, and never use a generic `resolver_record` observation to promote an unlisted family to supported.

For ENSv1 discovered resolver instances, the supported dynamic profile set is ENS Labs PublicResolver-generation-compatible and profile-exact.[^v1-pres-l20][^v1-pres-l31][^v1-pres-l131][^v1-pres-l150][^v1-resbase-l17][^v1-resbase-l23] Older admitted generations do not inherit latest-only NameWrapper awareness, default coin-type fallback, VersionableResolver boundaries, DNS records, text, contenthash, ABI, name, or interface support. For Basenames discovered Base-side resolver instances, the only complete supported dynamic profile is `L2Resolver`-compatible.[^bn-l2resolver-l4][^bn-l2resolver-l16][^bn-l2resolver-l22][^bn-l2resolver-l29][^bn-l2resolver-l182][^bn-l2resolver-l193][^bn-l2resolver-l209][^bn-l2resolver-l225]

After a `record_inventory_current` rebuild, the worker may run a bounded text-value hydration pass for observed ENSv1 `text:<key>` selectors whose current resolver is admitted as a supported PublicResolver-compatible text profile. The pass batches `text(bytes32,string)` calls through Multicall3 at provider `latest`, writes only `success` and `not_found` into `record_inventory_current.entries`, leaves failed calls as explicit `unsupported`, and creates no execution traces.

`GET /v1/names/{namespace}/{name}/records` is a compact read over the same resolver summary, `record_inventory_current`, `record_cache` join. Verified values stay execution-owned. `verified_queries` for the supported Basenames class can include persisted CCIP-participating traces only for the exact transport-assisted direct class.[^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191] Other Basenames path classes stay execution-local `unsupported`.

## Primary names

Keyed by `(address, coin_type, namespace)`. The row is the exact-tuple declared claim anchor plus invalidation context for current exact-tuple handling.

For ENS on Ethereum Mainnet, declared claim precedence is reverse-only through `ens_v1_reverse_l1`.[^v1-revreg-deploy][^v1-revreg-l15][^v1-revreg-l74][^v1-revreg-l83][^v1-revreg-l84] Missing or unsupported reverse claims do not fall back to registry, resolver, or other claim-setting surfaces. For Basenames, `basenames_base_primary` is the declared primary-claim intake owner. `primary_names_current` carries claim-local lookup and invalidation inputs; it does not become the declared truth family for exact-name, address-name, or children reads.[^bn-readme-l33][^bn-revreg-l12][^bn-revreg-l150]

Route-level `claimed_primary_name` and `verified_primary_name` share `ResultStatus` but stay distinct: declared claim state and verified execution state never collapse into one projection-owned field. `primary_names_current` does not persist or backfill `verified_primary_name`.[^bn-l1resolver-l13]

Projection-owned `claimed_primary_name` is limited to `success|not_found|unsupported|invalid_name`. Public claimed-local fields beyond bare status are exact-tuple declared `claimed_primary_name.name`, exact-tuple declared `claimed_primary_name.provenance`, and `raw_claim_name` for `invalid_name`.

- `claimed_primary_name.name` comes only from the requested row's declared normalized claim-identity source, aligned with the current reverse-only claim precedence.[^v1-revreg-l100][^v1-revreg-l123][^v1-revreg-l129][^v1-revreg-l130] It is not synthesized from manifest presence, resolver identity, verified execution identity, tuple presence alone, a different tuple, or any fallback claim source. It stays distinct from execution-derived `verified_primary_name.name`.
- `claimed_primary_name.provenance`, when published, is exact-tuple declared-only provenance from the requested row's claim-local inputs. The worker strips any `verified_primary_name_lookup` or `verified_primary_name_invalidation` hook material and omits `execution_trace_id`.
- `raw_claim_name` is copied verbatim from `primary_names_current.raw_claim_name` for the same exact tuple and only when `claim_status=invalid_name`. Blank or whitespace-only raw claim names are `not_found`; `invalid_name` is reserved for nonblank raw claim names that cannot be normalized. It is never copied into `verified_primary_name`.

The row owns claim-side inputs and invalidation context only — not fallback-source selection beyond reverse-only, execution `request_type`, execution request key, `execution_trace_id`, verified status, verified name identity, verification-local failure payloads, or the route-level join between claim-side and verification-side provenance.

The exact-tuple persisted-readback class is the only primary-name coverage support class. ENS uses `source_classes_considered=["ens_v1_reverse_l1","ens_execution"]`; Basenames uses `["basenames_base_primary","basenames_execution"]`. Both publish route-level `status=partial`, `exhaustiveness=non_enumerable`, `enumeration_basis=primary_name_lookup`, `unsupported_reason=null` only for the requested tuple.[^v1-revreg-deploy][^v1-ur-deploy][^bn-readme-l22][^bn-readme-l33] Out-of-class tuples, fallback claim sources, fresh verified-primary execution, and broader address or namespace coverage stay explicit `unsupported` with `source_classes_considered=[]`.

The Basenames exact-tuple `verified_primary_name` support class stays execution-derived under `basenames_execution`. It uses the same route tuple, the request key `{namespace}:{normalized_address}:{coin_type}`, and execution identity `request_type=verified_primary_name`. The matching `primary_names_current` row is the only claim-side anchor.[^bn-revreg-l193][^bn-l1resolver-l13]

The `verified_primary_name.provenance` invariant is additive to public publication. When admitted on the exact-tuple persisted-readback class, it reuses `Provenance` as a verification-local section refinement over execution output: `execution_trace_id` plus `manifest_versions` only, with `verified_primary_name.provenance.execution_trace_id` equal to top-level `provenance.execution_trace_id`. Top-level `provenance` stays the only route-level join between declared claim inputs and the persisted verification trace.

Tuple presence is a lookup and invalidation hook only. It does not widen claim precedence, admit fallback sources, change route-level coverage outside the exact-tuple class, or imply richer claimed payload support.

## Invalidation

Projection invalidation fires on:

- canonicality change
- manifest version change that affects a consumed capability
- normalized event insertion for a relevant key
- execution invalidation signal where the projection stores a declared cache summary

Invalidation is deterministic and key-scoped. No projection refreshes from broad time-based polling.

## Rebuild

Every projection supports point rebuild by key, range rebuild by chain position, and full rebuild from canonical events. Worker modes: continuous apply, backfill apply, reorg repair, one-shot rebuild.

Fresh normalized replay may defer normalized-event indexes used only by projection or API readback while current projection tables are empty. Rebuild tooling treats those indexes as part of its readiness boundary: before full current-state rebuilds count as ready for API reads, the deferred indexes must exist again.

### Replay status tracking

`current_projection_replay_status` records durable worker-owned completion markers per projection family after a family publishes successfully. Columns: `projection`, `replay_version`, `completed_normalized_target_block`, `requested_key_count`, `upserted_row_count`, `deleted_row_count`, `completed_at`.

On worker restart, automatic replay skips a family when its marker's `replay_version` matches the current code's replay version. The recorded `completed_normalized_target_block` is operational metadata, not the skip condition — the chain head can advance while a bootstrap replay is in flight.

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
[^v1-revreg-l15]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f)
[^v1-revreg-l74]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f)
[^v1-revreg-l83]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f)
[^v1-revreg-l84]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
[^v1-revreg-l100]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L100 @ ens_v1@91c966f)
[^v1-revreg-l123]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f)
[^v1-revreg-l129]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f)
[^v1-revreg-l130]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)

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
