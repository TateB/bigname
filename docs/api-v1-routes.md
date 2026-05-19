# API v1 Routes

Per-route reference. Conventions, snapshot selection, the response envelope, shared objects, and the error model live in [`api-v1.md`](api-v1.md).

## `GET /v1/namespaces/{namespace}`

Manifest-backed metadata for one public namespace.

`declared_state`: `active_manifest_count`, `active_source_families`, `chains`, `normalizer_versions`.

- `200` with empty lists and `active_manifest_count=0` when the namespace is public but has no active manifests.
- `404 not_found` when the namespace isn't a supported public namespace.
- Per-manifest capability flags live on `GET /v1/manifests/{namespace}`.

## `GET /v1/manifests/{namespace}`

Active manifest versions and capability flags. Declared-only.

## `GET /v1/names/{namespace}/{name}`

Exact name lookup. Full envelope. Uses the exact-name snapshot selector.

Query: `at`, `chain_positions`, `consistency`.

`data`:

- Surface identity: `logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `namehash`.
- Binding: `resource_id`, `token_lineage_id`, `binding_kind`.

`declared_state`:

- `registration`
- `authority`
- `control`: `ExactNameControlSummary | UnsupportedSummary`
- `resolver`: `ExactNameResolverSummary | UnsupportedSummary`
- `record_inventory`: `ResolutionRecordInventory | UnsupportedSummary`
- `history`: `ExactNameHistorySummary | UnsupportedSummary`

Rules:

- Authoritative for supported source classes even when one or more declared sections are unsupported.
- Every declared section is always present as an object. Missing projections return `UnsupportedSummary`.
- `declared_state.authority` falls back to `{resource_id, token_lineage_id, binding_kind}` when no dedicated authority summary is projected but the binding is known.
- For `namespace=ens` on the ENSv2 `sepolia-dev` profile, the promoted exact-name profile is supported for declared exact-name lookup, backed by `ens_v2_registry_l1` registry/token/lifecycle/resolver-target events plus `ens_v2_registrar_l1` `.eth` registration and renewal events from the admitted `ETHRegistry` and `ETHRegistrar` deployments.[^v2-deploy-ethreg][^v2-deploy-ethrc][^v2-iperm-l34][^v2-events-l15][^v2-iethreg-l32][^v2-iethreg-l53] Coverage: `status=full`, `exhaustiveness=authoritative`, `source_classes_considered=["ens_v2_registry_l1","ens_v2_registrar_l1"]`, `enumeration_basis=exact_name_profile`. This doesn't widen mainnet, reverse/primary, wrapper authority, migration, Universal Resolver, verified resolution, or execution-explain.
- For `namespace=basenames`, declared truth comes from the Base authority split (`basenames_base_registry`, `basenames_base_registrar`, `basenames_base_resolver`). `basenames_base_primary`, `basenames_l1_compat`, and `basenames_execution` don't widen this route.[^bn-readme-l70][^bn-revreg-l12][^bn-revreg-l150]
- `declared_state.control` is the narrow current-resource summary. Full role/permission lineage stays on the dedicated permissions route.
- Supported `declared_state.resolver` uses `chain_id, address` as the same key as `GET /v1/resolvers/{chain_id}/{resolver_address}`. Both `null` means no declared resolver, not unsupported.
- Supported `declared_state.record_inventory` uses the same `ResolutionRecordInventory` shape as `GET /v1/resolutions/{namespace}/{name}` and exposes the same `record_version_boundary` for the same snapshot.
- `declared_state.history.surface_head` and `resource_head` point at the first canonical rows of `GET /v1/history/names/{namespace}/{name}` under `scope=surface` and `scope=resource`. No `both_head` field; use `scope=both` on the dedicated route.
- `coverage` matches `GET /v1/coverage/{namespace}/{name}` for the same `{namespace, name}` and snapshot.
- No `include` expansions. History, permissions, resolution, and primary-name reads stay on their dedicated routes.
- `verified_state` is `null`.

## `GET /v1/coverage/{namespace}/{name}`

Single-name coverage and explain. Uses the exact-name snapshot.

Query: `at`, `chain_positions`, `consistency`.

`data` is the same surface and binding as `GET /v1/names/{namespace}/{name}`. `declared_state` carries explain detail for the same coverage answer. `verified_state` is `null`.

The top-level `coverage` object equals the inline `coverage` from `GET /v1/names/{namespace}/{name}` for the same request. For ENSv2 `sepolia-dev`, coverage follows the same exact-name profile rule as that route.[^v2-deploy-ethreg][^v2-deploy-ethrc] No `include` expansions.

## `GET /v1/explain/names/{namespace}/{name}/surface-binding`

Surface-binding explain over the same exact-name target.

Query: `at`, `chain_positions`, `consistency`.

`declared_state.surface_binding`: `SurfaceBindingExplainSummary`. `declared_state.history`: `ExactNameHistorySummary | UnsupportedSummary`. `verified_state` is `null`.

`surface_binding_id` identifies the current `SurfaceBinding` row whose `binding_kind` matches the exact-name answer. No historical binding rows. Reuses `surface_bindings_current` plus the existing history truth families. No `include` expansions.

## `GET /v1/explain/names/{namespace}/{name}/authority-control`

Authority/control explain over the same exact-name target.

Query: `at`, `chain_positions`, `consistency`.

`declared_state.authority` (same shape and fallback as the exact-name route). `declared_state.control`: `ExactNameControlSummary | UnsupportedSummary`. `verified_state` is `null`.

Row-granular permission lineage stays on `GET /v1/resources/{resource_id}/permissions`. No `include` expansions.

## Compact route knobs

Compact routes advertise only the knobs they own:

| Route | `view` | `mode` | `meta` |
| --- | --- | --- | --- |
| `GET /v1/names` | `compact` only; `full` is compatibility-reserved and rejected | none | `none`, `summary`, `full` |
| `GET /v1/names/{namespace}/{name}/children` | `compact`, `full` | none | `none`, `summary`, `full` |
| `GET /v1/names/{namespace}/{name}/records` | `compact` only; `full` is compatibility-reserved and rejected | `auto`, `declared`, `verified`, `both` | `none`, `summary`, `full` |
| `GET /v1/resolve/{name}/records` | `compact` only; `full` is compatibility-reserved and rejected | `auto`, `declared`, `verified`, `both` | `none`, `summary`, `full` |
| `GET /v1/names/{namespace}/{name}/roles` | `compact` only; `full` is compatibility-reserved and rejected | none | `none`, `summary`, `full` |
| `GET /v1/roles` | `compact` only; `full` is compatibility-reserved and rejected | none | `none`, `summary`, `full` |
| `GET /v1/resources/lookup` | `compact` only; `full` is compatibility-reserved and rejected | none | `none`, `summary`, `full` |
| `GET /v1/resolvers/{chain_id}/{resolver_address}/overview` | `compact`, `full` | none | `none`, `summary`, `full` |
| `GET /v1/events` | `compact` only; `full` is compatibility-reserved and rejected | none | `none`, `summary`, `full` |
| History routes | `compact`, `full` | none | `none`, `summary`, `full` |

`GET /v1/names` keeps its compatibility bridge: omitting `namespace` spans supported public namespaces. First-party app replacement code should pass an explicit namespace when it knows one. `GET /v1/names?name=...` is a compact collection filter that returns zero or one `CompactDomainSummary`; the canonical exact-name profile remains `GET /v1/names/{namespace}/{name}`.

## `GET /v1/names`

Compact app-facing collection: exact lookup, address-owned lists, owner/registrant/effective-controller relations, name search, suggestions.

Query: `namespace`, `name`, `prefix`, `contains`, `contains_nocase`, `owner`, `account`, `registrant`, `resolver`, `resolved_address`, `relation=token_holder|registrant|effective_controller|any`, `sort=name|expiry_date|registration_date|created_at`, `order=asc|desc`, `include=record_summaries,total_count`, `view=compact`, `meta=none|summary|full`, `cursor`, `page_size`.

Defaults: `view=compact`, `meta=summary`, `relation=any` when `account` is supplied, `sort=name`, `order=asc`.

Each compact item is `CompactDomainSummary`.

Rules:

- `namespace` limits to one public namespace; without it, items span supported public namespaces and each carries its `namespace`.
- `name` is a compact exact lookup filter by normalized name. With `namespace`, it returns zero or one item. It doesn't own the full exact-name profile semantics.
- `prefix`, `contains`, `contains_nocase` are search filters compatible with `namespace`, address relation filters, and pagination. They aren't availability checks.
- `owner` is the token-holder filter and equals `account` with `relation=token_holder`. Supplying both `owner` and `account` returns `400 invalid_input`.
- `relation=any` returns the deduped union of token-holder, registrant, and effective-controller matches by `(namespace, normalized_name)`.
- `resolver` filters by current declared resolver address where the exact-name resolver summary is projected.
- `resolved_address` is supported only where a declared, replay-stable record-value equality projection exists for the namespace and selector family. Otherwise the filter returns a non-2xx `unsupported` error.
- Sort orders break ties on `(namespace, normalized_name, namehash)`. `null` sort values order after non-null on `asc`, before non-null on `desc`.
- `include=record_summaries` adds compact record counts, known text-key hints, avatar/content-hash presence, and known coin-type hints from declared inventory/cache. No verified execution.
- `include=total_count` populates `meta.total_count` for the filtered set before cursor slicing where supported. Unsupported combinations leave `total_count=null` and add `total_count` to `meta.unsupported_fields`.
- `view=full` is compatibility-reserved and still returns `400 invalid_input`; OpenAPI advertises only `view=compact`.

## `GET /v1/names/{namespace}/{name}/children`

Direct children. Compact by default.

Query: `surface_classes=declared`, `include=counts`, `view=compact|full`, `meta=none|summary|full`, `cursor`, `page_size`.

Each compact item: `name`, `normalized_name`, `label_name`, `labelhash`, `namehash`, `owner`, `registrant`, `subname_count`.

Rules:

- `view=compact` is the default. `view=full` returns the existing full-envelope declared child collection.
- `name` is the child display name; `label_name` is the single child label relative to the requested parent.
- `labelhash` is `null` when the projection doesn't carry a stable label hash.
- `owner` and `registrant` are `null` when not projected for that child; this doesn't imply route-level unsupported.
- `include=counts` adds `subname_count` per child where projected. When unprojected, the field is `null` and `meta.unsupported_fields` includes `subname_count` unless `meta=none`.
- `surface_classes=linked|alias|wildcard` is reserved and returns `unsupported`.
- For `namespace=basenames`, child surfaces come from the admitted Base authority split only.[^bn-readme-l69][^bn-readme-l70]
- `cursor` and `page_size` page over `display_name_asc`.

## `GET /v1/names/{namespace}/{name}/records`

Compact resolver records over declared inventory/cache and optional verified selectors. Selected-snapshot projection read; doesn't run normalized-event catch-up scans.

Query: `mode=auto|declared|verified|both`, `texts`, `known_text_keys=true|false`, `avatar=true|false`, `content_hash=true|false`, `coin_types`, `include=resolver_address,known_text_keys,avatar,content_hash,coins`, `view=compact`, `meta=none|summary|full`.

Defaults: `mode=declared`, `view=compact`, `meta=summary`, `include=resolver_address`.

`data` is `CompactRecordSummary` for `view=compact`.

Rules:

- `resolver_address` is the current declared resolver target. `null` means no declared resolver at the snapshot, not a verified failure.
- `texts` returns requested keys in `text_records` from the selected value source.
- `known_text_keys=true` returns projected inventory metadata, not verified enumeration.
- `avatar=true` is a compact alias for the `avatar` text key and may also populate the top-level `avatar` field from declared cache.
- `content_hash=true` requests the declared content-hash selector.
- `coin_types` is a comma-separated list of textual coin-type selectors.
- `mode=declared` uses `record_cache` and `record_inventory`. No live execution.
- `mode=verified|both` follows the same supported verified-resolution boundary as `GET /v1/resolutions/{namespace}/{name}`. Supported ENS cache misses execute through the configured Ethereum RPC provider at the selected stored snapshot and persist the trace/outcome before joining it.
- `mode=auto`: an authoritative declared profile uses local inventory/cache only when the declared cache can satisfy every requested value from replayable state, including worker-hydrated ENSv1 PublicResolver text values for observed selectors after rebuild. Requested selectors with explicit declared gaps, unretained declared values, or no declared selectors use verified output instead, including on-demand Universal Resolver execution at the selected stored snapshot when no exact-snapshot output exists.
- Without declared selectors, `mode=auto|verified|both` may probe the basic app profile set (`addr:60`, `avatar`, `contenthash`, text keys `description`, `url`, `email`).
- On-demand execution never targets provider `latest` independently of the selected stored snapshot. If the provider cannot serve that block, the route returns `409 stale`; declared cache is not a fallback for a verified miss.
- Selector-specific record history isn't on this route. Use `GET /v1/events` or history routes with event-type filters.
- `view=full` is compatibility-reserved and still returns `400 invalid_input`; OpenAPI advertises only `view=compact`.

## `GET /v1/names/{namespace}/{name}/roles`

Compact role rows for the name's current resource.

Query: `account`, `role_bitmap`, `view=compact`, `meta=none|summary|full`, `cursor`, `page_size`.

Resolves the current `resource_id` for `{namespace, name}` at the exact-name snapshot and returns `RoleRow` items for that resource. If role projection is unavailable for the resource, returns empty `data` only when the route can prove no current rows exist; otherwise non-2xx `unsupported` or `409 stale`. `resource_hex` follows the same nullable rule as `GET /v1/resources/lookup`. `view=full` is compatibility-reserved and still returns `400 invalid_input`; OpenAPI advertises only `view=compact`.

## `GET /v1/addresses/{address}/names`

Address-to-surface collection. Returns surfaces, not backing resources.

Query: `namespace`, `relation=registrant|token_holder|effective_controller`, `dedupe_by=surface|resource`, `include=role_summary`, `cursor`, `page_size`.

Each item: `logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `namehash`, `resource_id`, `binding_kind`, `relation_facets`. With `include=role_summary`, also `role_summary: RoleSummary`, `subname_count`, `record_count`, `status`, `expiry`.

Rules:

- `dedupe_by=surface` is the default. `dedupe_by=resource` is grouping-only; it doesn't change coverage or turn the route into a resource collection.
- Default sort is `display_name_asc`. `cursor` and `page_size` page over that frozen order.
- `include=role_summary` is additive. It groups current `GET /v1/resources/{resource_id}/permissions` rows by `subject` and keeps `(scope, effective_powers)` pairs. Row-granular grant lineage stays on the dedicated permissions route.
- `subname_count` reuses declared-direct-child semantics from `GET /v1/names/{namespace}/{name}/children`.
- `status` and `expiry` mirror the current `ControlVector.status` and `ControlVector.expiry` for the item's `resource_id`.
- `record_count` counts distinct stable declared selectors at the current version boundary (same answer shape as `Resolution.record_inventory`).
- For `namespace=basenames`, address-name membership and relations come from the Base authority split. Reverse-claim and transport state don't add rows or widen relations.[^bn-readme-l69][^bn-readme-l70][^bn-revreg-l12]

## `GET /v1/addresses/{address}/names/count`

Count companion to `GET /v1/names` address relation filters.

Query: `namespace`, `relation=token_holder|registrant|effective_controller|any`, `prefix`, `contains`, `contains_nocase`, `resolver`.

```json
{
  "data": {
    "address": "0x0000000000000000000000000000000000000000",
    "namespace": "ens",
    "relation": "token_holder",
    "count": 0
  },
  "meta": {
    "support_status": "partial",
    "unsupported_filters": []
  }
}
```

`relation=any` counts the deduped union. Filter support matches `GET /v1/names`. The count is over the filtered set before any cursor slice. No item rows, provenance, or coverage detail by default.

## `GET /v1/resources/lookup`

Compact lookup from `{namespace, name}` to current `resource_id`.

Query: `namespace`, `name`, `view=compact`, `meta=none|summary|full`. Both `namespace` and `name` are required.

```json
{
  "data": {
    "namespace": "ens",
    "name": "alice.eth",
    "normalized_name": "alice.eth",
    "resource_id": "00000000-0000-0000-0000-000000000000",
    "resource_hex": null
  }
}
```

`resource_id` is opaque and is the stable API key for resource-scoped roles and permissions. `resource_hex` is deferred unless a stable projected field is documented for the namespace; clients must not derive it from `resource_id`, `namehash`, token ID, or calldata. Reads the same exact-name projection as `GET /v1/names/{namespace}/{name}`. `view=full` is compatibility-reserved and still returns `400 invalid_input`; OpenAPI advertises only `view=compact`.

## `GET /v1/resources/{resource_id}/permissions`

Resource-centric current effective permission rows.

Query: `subject`, `scope`, `cursor`, `page_size`.

Each item: `resource_id`, `subject`, `scope`, `effective_powers`, `grant_source`, `revocation_source`, `inheritance_path`, `transfer_behavior`.

Rules:

- `resource_id` is the truth anchor. Surface names and resolver addresses appear only as explanatory context.
- `effective_powers` is server-computed post-scope-modifier. Clients don't apply NameWrapper fuse masks themselves.
- Resolver-scoped permissions are rows in this collection with resolver-scope detail, not a separate truth system.
- For ENSv1 wrapper-backed resources, current NameWrapper fuses are folded into `effective_powers`. A burned fuse removes any public power that depends on the prohibited operation, and a row whose powers are fully masked is omitted. Upstream emits fuses through `NameWrapped` and `FusesSet` and gates wrapper operations on those bits.[^v1-iname-l31][^v1-iname-l37][^v1-nw-l421][^v1-nw-l427][^v1-nw-l666][^v1-nw-l676][^v1-nw-l723][^v1-nw-l827][^v1-nw-l1023][^v1-nw-l132]
- A wrapper-backed answer is `full` only when the current fuse modifier for the selected resource snapshot was applied. If the projection can't prove current fuse state, the route fails closed rather than returning unmasked powers.
- `cursor` and `page_size` page over `subject_scope_asc`.
- Declared-state only; `verified_state` is `null`.

## `GET /v1/roles`

Compact role rows by account, resource, or name.

Query: `account`, `resource_id`, `namespace`, `name`, `role_bitmap`, `view=compact`, `meta=none|summary|full`, `cursor`, `page_size`.

Defaults: `view=compact`, `meta=summary`, sort `account_resource_scope_asc`.

Each item is `RoleRow`.

Rules:

- At least one of `account`, `resource_id`, or the pair `{namespace, name}` is required.
- `{namespace, name}` resolves through `GET /v1/resources/lookup` semantics, then reads current effective permission rows for that resource.
- `account` filters by effective permission subject. It doesn't search owner, registrant, or address-name relations unless those subjects also exist in `permissions_current`.
- `role_bitmap` filters only when the projection exposes it; otherwise non-2xx `unsupported` for that filter.
- `effective_powers` remains the API-owned post-scope result. Don't infer powers from `role_bitmap` alone.
- Compact role rows do not expose provenance, raw facts, normalized-event IDs, or execution traces. Row-granular grant lineage stays on `GET /v1/resources/{resource_id}/permissions`.
- `view=full` is compatibility-reserved and still returns `400 invalid_input`; OpenAPI advertises only `view=compact`.

## `GET /v1/resolvers/{chain_id}/{resolver_address}`

Resolver overview.

`data` identifies the resolver target. `declared_state`:

- `bindings`: `ResolverOverviewBindingSummary | UnsupportedSummary`
- `aliases`: `ResolverOverviewBindingSummary | UnsupportedSummary`
- resolver-scoped permissions
- role-holder summary
- resolver event summary

Declared-only; `verified_state` is `null`. No query parameters.

Rules:

- Supported enumerable `bindings` include every current resolver-linked binding whose target matches the route, regardless of `binding_kind`.
- `aliases` reuses the same `{status, count, items}` envelope but narrows `items` to the `binding_kind=resolver_alias_path` subset of current resolver-linked bindings. No historical alias enumeration.
- For an enumerable target with no current alias binding, `aliases` returns `{status:"supported", count:0, items:[]}`.
- For ENSv1 PublicResolver-generation targets admitted through the supported gate, `bindings`, `aliases`, and resolver event fan-in summaries return `UnsupportedSummary` with `unsupported_reason="resolver_binding_enumeration_not_projected"` rather than enumerating every name pointing at that shared resolver.[^v1-pres-l20][^v1-pres-l31][^v1-pres-l66][^v1-pres-l114] Exact-name resolver state stays on exact-name and resolution routes.
- A discovered ENSv1 target with `pending` or `unsupported` profile state, or an admitted legacy generation without the requested family, returns explicit `UnsupportedSummary` rather than zero-count latest-PublicResolver summaries.
- For Basenames, a discovered target requires `L2Resolver`-compatible `supported` profile state for the requested family; otherwise explicit `UnsupportedSummary`. The ENSv1 gate, L1 transport, and offchain gateways don't satisfy this.[^bn-l2resolver-l22][^bn-l2resolver-l182][^bn-l2resolver-l193][^bn-l2resolver-l209][^bn-l2resolver-l225]
- Counts for nodes, aliases, and role holders live inside the declared summaries.

## `GET /v1/resolvers/{chain_id}/{resolver_address}/overview`

Compact resolver overview using the same target and `resolver_current` boundary as the full-envelope route.

Query: `include=nodes,aliases,roles,events`, `view=compact|full`, `meta=none|summary|full`.

Defaults: `view=compact`, `meta=summary`, `include=nodes,aliases,roles,events`.

`data` is `ResolverOverviewCompact` for `view=compact`.

Rules:

- `counts.{nodes,aliases,role_holders,events}` are present only when the corresponding section is projected. Unsupported sections appear in `meta.unsupported_fields` unless `meta=none`.
- `nodes` and `aliases` are `null` when their fan-in is unprojected, and they appear in `meta.unsupported_fields` accordingly. Unsupported fan-in is never rendered as a supported zero count.
- `roles` is the compact role-holder list from resolver-scoped permission rows when projected; row-granular lineage stays on permissions routes.
- `events` is a compact event list from canonical normalized events for the target when projected. Selector-specific record history is deferred.
- `view=full` delegates to the full-envelope route when supported; otherwise reserved and returns `400 invalid_input`.

## `GET /v1/resolutions/{namespace}/{name}`

Mixed declared+verified resolution. Canonical route. `namespace` is part of the public resource identity and is the stable storage/execution/cache key.

Query: `at`, `chain_positions`, `consistency`, `mode=declared|verified|both`, `records`.

`data` matches `GET /v1/names/{namespace}/{name}` for the snapshot.

Populated `declared_state`:

- `topology`: `ResolutionTopology | UnsupportedSummary`
- `record_inventory`: `ResolutionRecordInventory | UnsupportedSummary`
- `record_cache`: `ResolutionRecordCache | UnsupportedSummary`

Populated `verified_state`:

- `verified_queries`

Example fully-supported declared shape:

```json
{
  "topology": {
    "registry_path": [
      {"logical_name_id": "ens:alice.eth", "namespace": "ens", "normalized_name": "alice.eth", "canonical_display_name": "alice.eth", "namehash": "0x...", "resource_id": "00000000-0000-0000-0000-000000000000", "binding_kind": "declared_registry_path"}
    ],
    "subregistry_path": [],
    "resolver_path": [
      {"logical_name_id": "ens:alice.eth", "namespace": "ens", "normalized_name": "alice.eth", "canonical_display_name": "alice.eth", "resource_id": "00000000-0000-0000-0000-000000000000", "chain_id": "ethereum-mainnet", "address": "0x...", "latest_event_kind": "ResolverChanged"}
    ],
    "wildcard": {"source": null, "matched_labels": []},
    "alias": {"final_target": null, "hops": []},
    "version_boundaries": {
      "topology_version_boundary": {"logical_name_id": "ens:alice.eth", "resource_id": "00000000-0000-0000-0000-000000000000", "normalized_event_id": null, "event_kind": null, "chain_position": {"chain_id": "ethereum-mainnet", "block_number": 0, "block_hash": "0x0", "timestamp": "2026-04-16T00:00:00Z"}},
      "record_version_boundary": {"logical_name_id": "ens:alice.eth", "resource_id": "00000000-0000-0000-0000-000000000000", "normalized_event_id": null, "event_kind": null, "chain_position": {"chain_id": "ethereum-mainnet", "block_number": 0, "block_hash": "0x0", "timestamp": "2026-04-16T00:00:00Z"}}
    },
    "transport": {"source_chain_id": null, "target_chain_id": null, "contract_address": null, "latest_event_kind": null}
  },
  "record_inventory": {
    "record_version_boundary": {"logical_name_id": "ens:alice.eth", "resource_id": "00000000-0000-0000-0000-000000000000", "normalized_event_id": null, "event_kind": null, "chain_position": {"chain_id": "ethereum-mainnet", "block_number": 0, "block_hash": "0x0", "timestamp": "2026-04-16T00:00:00Z"}},
    "enumeration_basis": {"observed_selectors": true, "capability_declared_families": true, "globally_enumerable": false},
    "selectors": [],
    "explicit_gaps": [],
    "unsupported_families": [],
    "last_change": null
  },
  "record_cache": {
    "record_version_boundary": {"logical_name_id": "ens:alice.eth", "resource_id": "00000000-0000-0000-0000-000000000000", "normalized_event_id": null, "event_kind": null, "chain_position": {"chain_id": "ethereum-mainnet", "block_number": 0, "block_hash": "0x0", "timestamp": "2026-04-16T00:00:00Z"}},
    "entries": []
  }
}
```

Rules:

- Uses the exact-name snapshot for data, declared sections, coverage, verified support, and execution target.
- `mode=verified|both`: persisted verified output is eligible only when its stored chain positions exactly match the selected snapshot. When matching output is missing for a supported ENS Universal Resolver selector, the route executes on demand against the selected snapshot, persists the trace and outcome, and returns it.[^v1-iur-l44][^v1-iur-l52]
- `topology`, `record_inventory`, and `record_cache` are always present as objects when `declared_state` is populated. Missing projections return `UnsupportedSummary`.
- Callers round-trip the surfaced `record_key` strings in `records`. `record_family` and `selector_key` are explanatory.
- `record_inventory` defines the known selector space and version boundary. It does not imply global enumeration.
- `record_cache` is the declared last-known-value view over that space. It never implies verified execution ran.
- For ENSv1 and Basenames, a current resolver target alone doesn't claim complete `record_inventory`, `record_cache`, or resolver-overview support. Retained resolver-local events may produce selector-level cache successes; complete family coverage requires resolver-profile admission for that family.[^v1-ens-l12][^v1-pres-l20][^v1-pres-l31][^bn-registry-l132][^bn-l2resolver-l4][^bn-l2resolver-l16][^bn-l2resolver-l22]
- `record_version_boundary` is identical across `topology.version_boundaries.record_version_boundary`, `record_inventory.record_version_boundary`, and `record_cache.record_version_boundary` when those sections are supported together.
- `record_cache.entries[*]` and `verified_queries[*]` always echo `record_key` even when status isn't `success`.
- `records` is comma-separated. In `mode=declared` it's optional; if supplied, `record_cache` narrows to those selectors. In `mode=verified|both`, `records` is required and duplicates return `400 invalid_input`. Malformed selectors return `400 invalid_input`.
- If the surface doesn't exist for the namespace and snapshot, return `404 not_found`.
- `verified_queries` returns one result per requested selector in request order. Statuses: `success`, `not_found`, `unsupported`, `execution_failed`. Unsupported selector families, unsupported verified path classes, and namespaces without a verified entrypoint return `200` with `verified_queries[*].status=unsupported`. They never silently downgrade to declared cache.
- Declared resolver-profile gaps don't suppress verified execution for an otherwise supported Universal Resolver path. In `mode=verified|both`, supported ENS selectors read persisted output or execute on demand.

### Verified support classes

ENS supports three exact-surface classes, evaluated against the same declared topology snapshot used for `mode=declared|both`:

- **Direct path**: `resolver_path[0].logical_name_id == data.logical_name_id`, `wildcard.source=null`, `alias.final_target=null`, all `transport=null`.
- **Alias-only**: same as direct, but `alias.final_target` non-null with non-empty `hops`.
- **Wildcard-derived**: `wildcard.source` non-null with non-empty `matched_labels`, `resolver_path[0].logical_name_id == wildcard.source.logical_name_id`, `alias.final_target=null`, `subregistry_path=[]`, all `transport=null`.

Other ENS classes — non-alias ancestor-selected, linked-subregistry ancestor-selected, transport-assisted, CCIP-participating — return `verified_queries[*].status=unsupported`.

Basenames supports one class: exact-surface transport-assisted direct-path through the L1 Resolver. `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"`. The class includes CCIP-Read participation because upstream `L1Resolver` emits `OffchainLookup` for non-`base.eth` requests and verifies the callback through `resolveWithProof`.[^bn-readme-l22][^bn-readme-l28][^bn-readme-l29][^bn-readme-l34][^bn-readme-l69][^bn-readme-l70][^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191] Other Basenames classes return `unsupported`.

Verified execution that runs but produces no trustworthy answer returns `status=execution_failed` with `failure_reason`. When a supported ENS selector needs on-demand execution, the Ethereum RPC provider must be configured and able to serve the selected block; otherwise `409 stale` with a configuration message — never declared cache fallback.

For `mode=verified|both`, top-level `provenance` summarizes the request-scoped trace; `verified_queries[*]` may carry narrower per-selector provenance.

Deeper execution explanation lives on `GET /v1/explain/resolutions/{namespace}/{name}/execution`. This route doesn't inline step lists or raw trace dumps. Per-selector verified misses don't change route-level `coverage`.

## `GET /v1/resolve/{name}`

Namespace-inferred convenience for `GET /v1/resolutions/{namespace}/{name}`.

Query: `mode=declared|verified|both`, `records`.

Returns the same envelope as the canonical route after inference.

Inference on the normalized `{name}`:

- exact `base.eth` → `namespace=ens`
- `*.base.eth` → `namespace=basenames`
- other supported ENS names → `namespace=ens`

Rules:

- The canonical namespaced route is preferred when callers know the namespace; persisted identity stays namespaced.
- Inferred namespace is echoed through `data.namespace` and `data.logical_name_id`.
- `at`, `chain_positions`, `consistency` are not on this route. The canonical default snapshot applies (head, latest stored checkpoint); supported ENS on-demand execution targets that.
- Selector identity is namespace-local after inference. `*.base.eth` interprets `records` against the Basenames selector space.
- Inference and verified support are independent. `*.base.eth` does not fall back to `namespace=ens` outside the Basenames support class.
- Inferred ENS in `mode=verified|both` shares the canonical cache-or-live-execute behavior and the same fail-closed RPC-provider requirement.

## `GET /v1/resolve/{name}/records`

Namespace-inferred convenience for `GET /v1/names/{namespace}/{name}/records`. Default `mode=auto`. Selected-snapshot projection read (no normalized-event catch-up).

Query: `mode=auto|declared|verified|both`, `texts`, `known_text_keys=true|false`, `avatar=true|false`, `content_hash=true|false`, `coin_types`, `include=resolver_address,known_text_keys,avatar,content_hash,coins`, `view=compact`, `meta=none|summary|full`.

Defaults: `mode=auto`, `view=compact`, `meta=summary`, `include=resolver_address,known_text_keys,avatar,content_hash,coins`.

Inference matches `GET /v1/resolve/{name}`. After inference, returns the same `CompactRecordSummary` and verified support boundary as the canonical compact records route. The default also turns on the common app-facing sections so one request returns resolver address, known text keys, avatar, content hash, and known coin addresses where available.

Without declared selectors, `mode=auto` probes the basic app profile set through verified resolution and returns successful fallback text rows plus the ETH coin row when available. It doesn't claim `known_text_keys` inventory support from those probes. No `at`, `chain_positions`, or `consistency`; the canonical default snapshot applies. Supported ENS verified fallback uses that selected stored snapshot and fails closed with `409 stale` when the provider cannot serve it. Identity, support state, and errors stay namespace-local — Basenames doesn't fall back to ENS when the inferred tuple is missing. `view=full` is compatibility-reserved and still returns `400 invalid_input`; OpenAPI advertises only `view=compact`.

## `GET /v1/explain/resolutions/{namespace}/{name}/execution`

Persisted verified execution explain.

Query: `records` (required).

`data` matches the current surface and binding from the resolution route. `declared_state` is `null`.

`verified_state`:

- `execution`: `ResolutionExecutionExplainSummary`
- `verified_queries`

Rules:

- Verified-only; doesn't duplicate declared topology, inventory, or cache.
- `at`, `chain_positions`, `consistency` are not on this route.
- Duplicate or malformed `records` selectors return `400 invalid_input`.
- Keyed by the same exact surface and selector set the resolution route would use. Explains the persisted answer.
- Public verified-resolution support boundary matches the resolution route. ENS direct, alias-only, and wildcard-derived classes are in scope. Basenames supports the exact-surface transport-assisted direct-path class through the L1 Resolver, including persisted CCIP-Read steps.[^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]
- `verified_queries` reuses selector-scoped result objects, request order, and `ResultStatus` from the resolution route.
- `verified_state.execution.execution_trace_id == provenance.execution_trace_id`. `verified_queries[*].provenance` stays under that same `execution_trace_id`.
- `verified_state.execution.steps` is the persisted ordered step summary. Not raw calldata, raw gateway payloads, or a replayable dump.
- The route doesn't trigger fresh execution and doesn't synthesize from declared topology. With no persisted answer for the requested surface and selector set, return `404 not_found`.
- For `{namespace, name, records}`, top-level `coverage` matches `GET /v1/resolutions/{namespace}/{name}`.
- No `include` expansions.

## `GET /v1/history/names/{namespace}/{name}`

Canonical normalized-event history for one logical name anchor.

Query: `scope=surface|resource|both` (default `both`), `view=compact|full`, `meta=none|summary|full`, `cursor`, `page_size`.

Rules:

- `scope=surface`: events anchored by the requested `logical_name_id`.
- `scope=resource`: events anchored by any `resource_id` ever bound to that surface.
- `scope=both`: union.
- Observed and orphaned events are excluded.
- `view=compact` returns `CompactHistoryEvent` rows with `meta=summary`. `view=full` returns the existing normalized-event history row shape.
- Pages over `chain_position_desc`.
- `declared_state` is `{}`. The rows themselves are the declared answer.

## `GET /v1/history/resources/{resource_id}`

Same contract anchored on `resource_id`.

Query: identical to the name history route.

Rules:

- `resource_id` must be a UUID; otherwise `400 invalid_input`.
- `scope=resource` is the requested resource. `scope=surface` is any `logical_name_id` ever bound to it. `scope=both` is the union.
- Observed and orphaned events are excluded.

## `GET /v1/history/addresses/{address}`

Canonical normalized-event history for the address-derived anchor set.

Query: `namespace`, `relation=registrant|token_holder|effective_controller`, `scope=surface|resource|both` (default `both`), `view=compact|full`, `meta=none|summary|full`, `cursor`, `page_size`.

Reuses the normalized-event history contract; no separate ledger. `namespace` and `relation` filter which surfaces and resources contribute anchors across current and historical matches; they don't change row shape, ordering, or coverage. Observed and orphaned events are excluded. Pages over `chain_position_desc`.

## `GET /v1/events`

Compact event search across name, address, resource, type, and block filters. Reuses the normalized-event history truth family.

Query: `namespace`, `name`, `address`, `resource`, `resource_id`, `type`, `relation=token_holder|registrant|effective_controller|any`, `from_block`, `to_block`, `view=compact`, `meta=none|summary|full`, `cursor`, `page_size`.

Defaults: `view=compact`, `meta=summary`, sort `chain_position_desc`.

Each row is `CompactHistoryEvent`.

Rules:

- `name` requires `namespace`. Otherwise `400 invalid_input`.
- `address` selects events whose projected anchor relates to the address under the requested `relation`. Same anchor selection as `GET /v1/history/addresses/{address}`.
- `resource` and `resource_id` both select events anchored on the resource. Supplying both returns `400 invalid_input`. `resource_hex` isn't accepted.
- `type` filters by normalized event type or route-owned compact type alias. Unsupported aliases return non-2xx `unsupported`.
- `from_block` and `to_block` apply to canonical chain position. They don't trigger raw fact scans.
- Observed and orphaned events are excluded.
- `view=full` is compatibility-reserved and still returns `400 invalid_input`; OpenAPI advertises only `view=compact`.

## `GET /v1/primary-names/{address}`

Claimed and verified primary name for one `(address, namespace, coin_type)` tuple.

Query: `mode=declared|verified|both`, `coin_type` (required), `namespace` (required).

`data`: `address`, `namespace`, `coin_type`.

Populated `declared_state`: `claimed_primary_name`. Populated `verified_state`: `verified_primary_name`.

Both objects use `ResultStatus`. `claimed_primary_name` uses `success`, `not_found`, `unsupported`, `invalid_name`. `verified_primary_name` uses those plus `mismatch` and `execution_failed`.

Rules:

- Head-only. No `at` or `consistency`.
- `claimed_primary_name` is the candidate only; it never implies verification.
- For ENS, the admitted claim source is `ens_v1_reverse_l1` reverse intake at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`.[^v1-revreg-deploy][^v1-revreg-l15][^v1-revreg-l19][^v1-revreg-l74][^v1-revreg-l83][^v1-revreg-l84] The route doesn't trigger fresh reverse lookups while serving the declared response. Missing or unsupported claims don't fall back to registry, resolver, or other claim-setting surfaces.
- `claimed_primary_name.name` comes only from the exact requested `primary_names_current(address, coin_type, namespace)` row's declared normalized claim-identity source. Never synthesized from manifest presence, resolver-backed name data, verified execution identity, tuple presence, or another tuple's stored identity.
- For Basenames, the admitted claim family is `basenames_base_primary` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282`.[^bn-readme-l33][^bn-revreg-l12][^bn-revreg-l150] Claim intake only — does not replace the Base registry/registrar/resolver families for declared truth on exact-name, address-name, or children reads.
- `claimed_primary_name.raw_claim_name` may appear only when `status=invalid_name` for the exact requested tuple, copied verbatim from `primary_names_current.raw_claim_name`. Blank or whitespace-only raw claims become `not_found`; `invalid_name` is for nonblank claims that fail normalization.
- `claimed_primary_name.provenance` is exact-tuple declared provenance from the requested `primary_names_current` row. It strips any verified-primary lookup/invalidation hook material and omits `execution_trace_id`.
- `verified_primary_name` field boundary: `{status, name?, unsupported_reason?, failure_reason?, provenance?}`. `name` uses `NameRef` and appears only for `success` or `mismatch`. `raw_claim_name` never appears here.
- `verified_primary_name.provenance` (when present) is the section-local `{execution_trace_id, manifest_versions}` for the same tuple. `execution_trace_id` equals top-level `provenance.execution_trace_id`.
- `verified_primary_name` is authoritative only on `status=success`. `status=mismatch` means the claim normalizes and the verified target resolves for the requested `coin_type` but doesn't equal the requested `{address}`.
- `failure_reason` on `verified_primary_name` is verification-local and may appear only for `mismatch`, `invalid_name`, or `execution_failed`.
- Verified persisted-readback uses execution identity `request_type=verified_primary_name` keyed on `{namespace}:{normalized_address}:{coin_type}` (lowercased address).
- `primary_names_current(address, coin_type, namespace)` is the only claim-side lookup/invalidation anchor.
- For Basenames in `mode=verified|both`, persisted `verified_primary_name` results are returned for the exact requested tuple via `basenames_execution`. Declared and verified stay separate because upstream keeps reverse-name writes on the Base ReverseRegistrar while verified resolution enters through the L1 Resolver.[^bn-readme-l22][^bn-readme-l33][^bn-revreg-l12][^bn-revreg-l193][^bn-l1resolver-l13]
- Invalid address syntax, missing `namespace` or `coin_type`, or a malformed tuple returns `400 invalid_input`.
- Unsupported public namespace returns `404 not_found`.
- No declared or verified answer for the tuple returns `200` with `status=not_found`.
- Unsupported claim surfaces or verified entrypoints return `200` with the corresponding object `status=unsupported`.

### Route-level coverage

Local to the requested tuple. Not the single-name `Coverage` from `GET /v1/coverage/{namespace}/{name}`.

- ENS mainnet supported tuple class: `coverage.status=partial`, `exhaustiveness=non_enumerable`, `source_classes_considered=["ens_v1_reverse_l1","ens_execution"]`, `enumeration_basis=primary_name_lookup`.[^v1-revreg-deploy][^v1-ur-deploy]
- Basenames mainnet supported tuple class: `coverage.status=partial`, `exhaustiveness=non_enumerable`, `source_classes_considered=["basenames_base_primary","basenames_execution"]`, `enumeration_basis=primary_name_lookup`.
- Out of class: `coverage.status=unsupported`, `exhaustiveness=not_applicable`, `source_classes_considered=[]`, `enumeration_basis=primary_name_lookup`, `unsupported_reason="primary-name exact-tuple persisted readback is not supported for the requested tuple"`. Out-of-class verified objects use `verified_primary_name.status=unsupported`.

Tuple presence, absence, mismatch, or resolver-backed verification detail doesn't change these states. Class membership chooses route-level coverage; result-object `status` describes the tuple answer inside that class.

## App-facing examples

Dashboard owned names:

```
GET /v1/names?namespace=ens&account=0x0000...&relation=token_holder&contains=ali&sort=expiry_date&order=asc&page_size=50
```

Name search suggestions:

```
GET /v1/names?namespace=ens&prefix=alic&sort=name&order=asc&page_size=10&meta=none
```

Exact compact lookup:

```
GET /v1/names?namespace=ens&name=alice.eth&include=record_summaries
```

Subnames:

```
GET /v1/names/ens/alice.eth/children?include=counts&page_size=50
```

Resolver records:

```
GET /v1/resolve/alice.eth/records
GET /v1/resolve/alice.eth/records?include=resolver_address,known_text_keys,avatar,content_hash,coins&texts=avatar,com.twitter&coin_types=60,0
```

Name history:

```
GET /v1/history/names/ens/alice.eth?view=compact&scope=both&page_size=25
```

Address activity:

```
GET /v1/events?address=0x0000...&relation=any&namespace=ens&page_size=25
```

Roles:

```
GET /v1/roles?account=0x0000...&page_size=50
GET /v1/roles?resource_id=00000000-0000-0000-0000-000000000000&page_size=50
GET /v1/names/ens/alice.eth/roles?page_size=50
```

Resolver overview:

```
GET /v1/resolvers/ethereum-mainnet/0x0000.../overview?include=nodes,aliases,roles,events
```

## Footnotes

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
[^bn-l2resolver-l182]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc)
[^bn-l2resolver-l193]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)
[^bn-l2resolver-l209]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc)
[^bn-l2resolver-l225]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)
[^bn-revreg-l12]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
[^bn-revreg-l150]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
[^bn-revreg-l193]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)

[^v1-ens-l12]: (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)

[^v1-iname-l31]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L31 @ ens_v1@91c966f)
[^v1-iname-l37]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f)

[^v1-nw-l132]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L132 @ ens_v1@91c966f)
[^v1-nw-l421]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L421 @ ens_v1@91c966f)
[^v1-nw-l427]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L427 @ ens_v1@91c966f)
[^v1-nw-l666]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L666 @ ens_v1@91c966f)
[^v1-nw-l676]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L676 @ ens_v1@91c966f)
[^v1-nw-l723]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L723 @ ens_v1@91c966f)
[^v1-nw-l827]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L827 @ ens_v1@91c966f)
[^v1-nw-l1023]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1023 @ ens_v1@91c966f)

[^v1-pres-l20]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^v1-pres-l31]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f)
[^v1-pres-l66]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L66 @ ens_v1@91c966f)
[^v1-pres-l114]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f)

[^v1-iur-l44]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L44 @ ens_v1@91c966f)
[^v1-iur-l52]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L52 @ ens_v1@91c966f)
[^v1-ur-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)

[^v1-revreg-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-revreg-l15]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f)
[^v1-revreg-l19]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L19 @ ens_v1@91c966f)
[^v1-revreg-l74]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f)
[^v1-revreg-l83]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f)
[^v1-revreg-l84]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)

[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309)
[^v2-deploy-ethrc]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)
[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309)
[^v2-events-l15]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309)
[^v2-iethreg-l32]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309)
[^v2-iethreg-l53]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309)
