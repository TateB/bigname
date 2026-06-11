# API v1 Routes

Per-route reference. Conventions, snapshot selection, the response envelope, shared objects, and the error model live in [`api-v1.md`](api-v1.md).

## Route Set Guide

Use the route groups as integration guidance, not just documentation order:

| Set | Routes | Use for |
| --- | --- | --- |
| Native slim identity | `POST /v1/identity:lookup`, `GET /v1/status` | Partner-1 feed/profile reads and shadow comparison. Use `profile=feed` for the under-10 ms p95 feed target. |
| Canonical product reads | `/v1/names*`, `/v1/profiles/names/*`, `/v1/addresses/{address}/names`, `/v1/primary-names*`, `/v1/resources/{resource_id}/permissions`, `/v1/events` | New app, explorer, and public API integrations that want bigname-native semantics. |
| Metadata/control plane | `/v1/namespaces/*`, `/v1/manifests/*`, `/healthz` | Namespace, manifest, and process/database liveness introspection. |
| Diagnostics/provenance | `/v1/coverage/*`, `/v1/explain/*` | Completeness, freshness, derivation, persisted execution, and audit detail. |
| Specialist adjuncts | `/v1/roles`, `/v1/names/*/roles`, `/v1/resources/lookup`, `/v1/history/*`, `/v1/resolvers/*/overview` | Supported surfaces for specialist workflows; prefer canonical product reads for new integrations when they fit. |

## `GET /v1/namespaces/{namespace}`

Manifest-backed metadata for one public namespace.

`declared_state`: `active_manifest_count`, `active_source_families`, `chains`, `normalizer_versions`.

- `200` with empty lists and `active_manifest_count=0` when the namespace is public but has no active manifests.
- `404 not_found` when the namespace isn't a supported public namespace.
- Per-manifest capability flags live on `GET /v1/manifests/{namespace}`.

## `GET /v1/manifests/{namespace}`

Active manifest versions and capability flags. Declared-only.

## `GET /v1/names/{namespace}/{name}`

Exact name lookup. Uses the exact-name snapshot selector and does not include route-level provenance by default.

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
- For `namespace=basenames`, declared truth comes from the Base authority split (`basenames_base_registry`, `basenames_base_registrar`, `basenames_base_resolver`). `basenames_base_primary`, `basenames_l1_compat`, and `basenames_execution` don't widen this route; `basenames_base_primary` is limited to declared primary-name value intake from ENSv1's Base `L2ReverseRegistrar`.[^bn-readme-l70][^v1-l2rev-base-deploy][^v1-l2rev-event]
- `declared_state.control` is the narrow current-resource summary. Full role/permission lineage stays on the dedicated permissions route.
- Supported `declared_state.resolver` uses `chain_id, address` as the same resolver identity key exposed by resolver overview. Both `null` means no declared resolver, not unsupported.
- Supported `declared_state.record_inventory` uses the same `ResolutionRecordInventory` shape as `GET /v1/profiles/names/{name}` and exposes the same `record_version_boundary` for the same snapshot.
- `declared_state.history.surface_head` and `resource_head` point at the first canonical rows of `GET /v1/history/names/{namespace}/{name}` under `scope=surface` and `scope=resource`. No `both_head` field; use `scope=both` on the dedicated route.
- `coverage` matches `GET /v1/coverage/{namespace}/{name}` for the same `{namespace, name}` and snapshot.
- No `include` expansions. History, permissions, resolution, and primary-name reads stay on their dedicated routes.
- `verified_state` is `null`.

## `GET /v1/profiles/names/{name}`

App-facing full profile path for callers that need name identity, declared topology/inventory/cache, and verified record results in one request without specifying a namespace.

Query: `at`, `chain_positions`, `consistency`, `mode=declared|verified|both` (default `both`), `meta=none|summary|full`.

Rules:

- The path `name` is normalized before lookup. Namespace inference matches native identity lookup: `base.eth` is ENS, any non-empty `*.base.eth` suffix is Basenames, and other names are ENS. The inferred namespace is returned on `data.namespace`.
- Default `data` is the compact app identity tuple: `name`, `namespace`, `namehash`, and `resource_id`. It does not include `logical_name_id`, `normalized_name`, `canonical_display_name`, `token_lineage_id`, or `binding_kind`. `meta=full` may return the diagnostic exact-name data shape used by `GET /v1/names/{namespace}/{name}` for the inferred namespace, normalized name, and selected snapshot.
- `declared_state` is present for `mode=declared|both` and contains compact `topology`, `record_inventory`, and `record_cache` by default.
- `verified_state` is present for `mode=verified|both` and contains compact `verified_queries` by default.
- Record selection is server-owned. The route does not accept caller-selected `records`, and `mode=verified|both` executes every selected profile record rather than a caller-supplied subset. The selector set is derived from the selected snapshot's declared state: every `record_inventory.selectors[*].record_key`, every `record_inventory.explicit_gaps[*].record_key`, and every `record_cache.entries[*].record_key`, deduped in that order. If that derived set is non-empty, it is the complete profile record set for this route.
- Default profile records are used only for a supported but empty declared inventory. In that case the route falls back to the bounded app profile set `addr:60`, `avatar`, `contenthash`, `text:description`, `text:url`, and `text:email`. Missing inventory, stale inventory, or explicitly unsupported inventory does not use fallback records.
- Supplying `records` on this route returns `400 invalid_input`; selector-specific reads belong on the compact records route.
- Use `GET /v1/names/{namespace}/{name}/records` for selector-specific app reads. Use `GET /v1/explain/resolutions/{namespace}/{name}/execution` for persisted execution diagnostics of an explicit selector set.
- Supported ENS cache misses execute through the configured Ethereum RPC provider at the selected stored snapshot and persist the trace/outcome before joining it. On-demand execution never targets provider `latest` independently of the selected stored snapshot.
- Default `meta=summary` omits top-level `coverage`, `chain_positions`, `consistency`, `last_updated`, and route-level `provenance`. It also strips diagnostic topology version boundaries and per-query execution provenance. `meta=full` restores the full route envelope and diagnostic topology/cache/execution metadata.
- Deeper execution explanation lives on `GET /v1/explain/resolutions/{namespace}/{name}/execution`; profile responses do not inline raw traces or step dumps.

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
| `GET /v1/names` | `compact` only; `full` is reserved and rejected | none | `none`, `summary`, `full` |
| `GET /v1/profiles/names/{name}` | full profile; selector set is server-derived from declared state, with bounded defaults only for supported empty inventory | `declared`, `verified`, `both` | `none`, `summary`, `full` |
| `GET /v1/names/{namespace}/{name}/children` | `compact`, `full` | none | `none`, `summary`, `full` |
| `GET /v1/names/{namespace}/{name}/records` | `compact` only; `full` is reserved and rejected | `auto`, `declared`, `verified`, `both` | `none`, `summary`, `full` |
| `GET /v1/names/{namespace}/{name}/roles` | `compact` only; `full` is reserved and rejected | none | `none`, `summary`, `full` |
| `GET /v1/roles` | `compact` only; `full` is reserved and rejected | none | `none`, `summary`, `full` |
| `GET /v1/resources/lookup` | `compact` only; `full` is reserved and rejected | none | `none`, `summary`, `full` |
| `GET /v1/resolvers/{chain_id}/{resolver_address}/overview` | `compact` only; `full` is reserved and rejected | none | `none`, `summary`, `full` |
| `GET /v1/events` | `compact` only; `full` is reserved and rejected | none | `none`, `summary`, `full` |
| History routes | `compact`, `full` | none | `none`, `summary`, `full` |

`GET /v1/names` keeps its namespace-omitted bridge: omitting `namespace` spans supported public namespaces. First-party app replacement code should pass an explicit namespace when it knows one. `GET /v1/names?name=...` is a compact collection filter that returns zero or one `CompactDomainSummary`; the canonical exact-name profile remains `GET /v1/names/{namespace}/{name}`.

## Identity Routes

`POST /v1/identity:lookup` is the native slim identity primitive for partner-1 feeds, profile aggregation, and shadow comparison.

Names supplied by callers are normalized before lookup.

In native bigname response DTOs, `name` is the canonical normalized name string returned by the API. Clients should render `name` by default and use `namespace + namehash` as the stable identity key.

The caller-supplied name is echoed only under `input.name` on lookup result objects.

A `normalization` object may appear on lookup results when caller input was transformed or rejected. Do not include `normalized_name` as a routine peer of `name` in native DTOs.

### `NormalizationInfo`

Only present on name lookup results when relevant.

```json
{
  "changed": true,
  "input_name": "Alice.eth",
  "reason": "case_normalized"
}
```

Rules:

- `record.name` is the normalized output name.
- `input.name` is the caller-supplied input.
- `normalization.changed=true` means the API accepted the input but canonicalized it.
- `status=unnormalizable_input` uses `record=null` and may include `normalization.reason`.

Native `IdentityRecord` detail shape:

```json
{
  "name": "alice.eth",
  "namespace": "ens",
  "namehash": "0x...",
  "owner_address": "0x...",
  "manager_address": "0x...",
  "primary_address": "0x...",
  "coin_type_addresses": { "60": "0x..." },
  "text_records": { "avatar": "ipfs://..." },
  "resolver_address": "0x...",
  "expiration": 1770000000,
  "token_id": "123",
  "network": "ethereum",
  "is_primary": true,
  "relation_facets": ["owned"],
  "status": "success",
  "unsupported_fields": []
}
```

Native feed profile subset:

```json
{
  "name": "alice.eth",
  "namespace": "ens",
  "namehash": "0x...",
  "network": "ethereum",
  "is_primary": true,
  "relation_facets": ["owned"],
  "status": "success"
}
```

## `POST /v1/identity:lookup`

Single/batch identity primitive.

Request:

```json
{
  "profile": "feed",
  "namespace": "public",
  "inputs": [
    {
      "id": "name-1",
      "kind": "name",
      "name": "Alice.eth"
    },
    {
      "id": "addr-1",
      "kind": "address",
      "address": "0x0000000000000000000000000000000000000000",
      "coin_type": 60,
      "roles": ["owned", "managed"],
      "page_size": 1,
      "cursor": null
    }
  ]
}
```

Response:

```json
{
  "results": [
    {
      "id": "name-1",
      "kind": "name",
      "status": "success",
      "input": { "name": "Alice.eth" },
      "normalization": {
        "changed": true,
        "input_name": "Alice.eth",
        "reason": "case_normalized"
      },
      "record": {
        "name": "alice.eth",
        "namespace": "ens",
        "namehash": "0x..."
      }
    },
    {
      "id": "addr-1",
      "kind": "address",
      "status": "success",
      "input": {
        "address": "0x0000000000000000000000000000000000000000",
        "coin_type": 60,
        "roles": ["owned", "managed"]
      },
      "records": [],
      "page": {
        "next_cursor": null,
        "total_count": 0,
        "has_more": false
      }
    }
  ]
}
```

Unnormalizable input example:

```json
{
  "id": "name-1",
  "kind": "name",
  "status": "unnormalizable_input",
  "input": { "name": "bad name" },
  "normalization": {
    "changed": false,
    "input_name": "bad name",
    "reason": "invalid_normalized_name"
  },
  "record": null
}
```

Rules:

- `profile=feed` is the partner-1 latency path. Address inputs return at most one compact identity row plus `total_count`, backed by indexed feed/count sidecars.
- `profile=detail` is the profile aggregation path. Address inputs return paged full native `IdentityRecord` rows and name inputs return full native `IdentityRecord`.
- `profile=shadow` currently follows `detail` response shape for deterministic migration comparison.
- Address inputs require `coin_type` per input. Address misses return `records=[]`, `status=success`, and `total_count=0` unless the input itself is invalid.
- Explicit `namespace=ens|basenames` is supported for name inputs only in this slice. Address inputs use `namespace=public`/`auto` semantics and reject explicit namespace filters; use canonical address routes for namespace-scoped address collections.
- Name misses return `record=null`, `status=not_found`.
- Input order and grouping are preserved. Backend planning may coalesce identical internal reads by normalized input key, selected namespace, `coin_type`, roles, and page request.
- Native identity record equality is `namespace + namehash`; deterministic name-order pagination may still sort by `(namespace, name, namehash)`.
- `profile=feed` is the only identity profile counted against partner-1 feed latency SLOs. `profile=detail`, `profile=shadow`, and full coverage/provenance diagnostics are outside that SLO.

## `GET /v1/status`

Projection/indexing readiness and chain lag. This is not `/healthz`; `/healthz` remains process and database liveness.

Response:

```json
{
  "data": {
    "status": "ready",
    "chains": {
      "ethereum-mainnet": {
        "canonical_block": 0,
        "safe_block": 0,
        "finalized_block": 0,
        "latest_projected_block": 0,
        "latest_projected_timestamp": null,
        "projection_lag_blocks": 0,
        "projection_lag_seconds": null
      }
    }
  }
}
```

Uses active/shadow `manifest_versions` to include chains expected by the loaded profile, plus `chain_checkpoints`, retained `chain_lineage`, `projection_normalized_event_changes`, `projection_apply_cursors`, and `projection_invalidations` where available. Fields stay `null` when the deployment has not yet retained the corresponding operational metadata. If no chain readiness data exists for an expected chain, or if pending direct invalidations cannot be tied to a normalized-event chain position, `status` is `degraded`.

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
- `view=full` is reserved and still returns `400 invalid_input`; OpenAPI advertises only `view=compact`.

## `GET /v1/names/{namespace}/{name}/children`

Direct children. Compact by default.

Query: `surface_classes=declared`, `include=counts`, `view=compact|full`, `meta=none|summary|full`, `cursor`, `page_size`.

Each compact item: `name`, `normalized_name`, `label_name`, `labelhash`, `namehash`, `owner`, `registrant`, `subname_count`.

Rules:

- `view=compact` is the default. `view=full` returns the existing full-envelope declared child collection.
- `name` is the child identity name when known, or the unknown-label placeholder when only the child node and labelhash are known. `label_name` is the single child label relative to the requested parent; for unknown ENSv1 and Basenames registry labels it is `[<labelhash-without-0x>]`.
- `labelhash` is `null` when the projection doesn't carry a stable label hash.
- `owner` and `registrant` are `null` when not projected for that child; this doesn't imply route-level unsupported.
- `include=counts` adds `subname_count` per child where projected. When unprojected, the field is `null` and `meta.unsupported_fields` includes `subname_count` unless `meta=none`.
- `surface_classes=linked|alias|wildcard` is reserved and returns `unsupported`.
- For ENSv1 registry-derived children, the registry `NewOwner` event proves `parent_node`, labelhash, owner, and child node, but not the plaintext child label.[^v1-registry-l45][^v1-registry-l82] Basenames Base registry subnode updates use the same parent-node plus labelhash shape.[^bn-registry-l81][^bn-registry-l120][^bn-registry-l122] The route still returns the declared child node. If no canonical child surface or retained, proof-checked label preimage identifies the label, `name`, `normalized_name`, and `label_name` use the explicit unknown-label bracket placeholder. Unknown-label rows count toward collection totals but do not make the placeholder a valid exact-name lookup target.
- For `namespace=basenames`, child rows come from the admitted Base authority split only; primary-claim and L1 compatibility transport do not add children.[^bn-readme-l69][^bn-readme-l70]
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
- `mode=verified|both` follows the same supported verified-resolution boundary as `GET /v1/profiles/names/{name}`. Supported ENS cache misses execute through the configured Ethereum RPC provider at the selected stored snapshot and persist the trace/outcome before joining it.
- `mode=auto`: an authoritative declared profile uses local inventory/cache only when the declared cache can satisfy every requested value from replayable state, including worker-hydrated ENSv1 PublicResolver text values for observed selectors after rebuild. Requested selectors with explicit declared gaps, unretained declared values, or no declared selectors use verified output instead, including on-demand Universal Resolver execution at the selected stored snapshot when no exact-snapshot output exists.
- Without declared selectors, `mode=auto|verified|both` may probe the basic app profile set (`addr:60`, `avatar`, `contenthash`, text keys `description`, `url`, `email`).
- On-demand execution never targets provider `latest` independently of the selected stored snapshot. If the provider cannot serve that block, the route returns `409 stale`; declared cache is not a fallback for a verified miss.
- Selector-specific record history isn't on this route. Use `GET /v1/events` or history routes with event-type filters.
- `view=full` is reserved and still returns `400 invalid_input`; OpenAPI advertises only `view=compact`.

## `GET /v1/names/{namespace}/{name}/roles`

Compact role rows for the name's current resource.

Query: `account`, `role_bitmap`, `view=compact`, `meta=none|summary|full`, `cursor`, `page_size`.

Resolves the current `resource_id` for `{namespace, name}` at the exact-name snapshot and returns `RoleRow` items for that resource. If role projection is unavailable for the resource, returns empty `data` only when the route can prove no current rows exist; otherwise non-2xx `unsupported` or `409 stale`. `resource_hex` follows the same nullable rule as `GET /v1/resources/lookup`. `view=full` is reserved and still returns `400 invalid_input`; OpenAPI advertises only `view=compact`.

## `GET /v1/addresses/{address}/names`

Address-to-surface collection. Returns surfaces, not backing resources.

Query: `namespace`, `relation=registrant|token_holder|effective_controller`, `dedupe_by=surface|resource`, `include=role_summary`, `cursor`, `page_size`.

Each item: `logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `namehash`, `resource_id`, `binding_kind`, `relation_facets`. With `include=role_summary`, also `role_summary: RoleSummary`, `subname_count`, `record_count`, `status`, `expiry`.

Rules:

- `dedupe_by=surface` is the default. `dedupe_by=resource` is grouping-only; it doesn't change coverage or turn the route into a resource collection.
- Default sort is `display_name_asc`. `cursor` and `page_size` page over that frozen order.
- `include=role_summary` is additive. It groups current `GET /v1/resources/{resource_id}/permissions` rows by `subject` and keeps `(scope, effective_powers)` pairs. The response provenance summarizes the base address-name collection plus expansion inputs. Row-granular grant lineage stays on the dedicated permissions route.
- `subname_count` reuses declared-direct-child semantics from `GET /v1/names/{namespace}/{name}/children`.
- `status` and `expiry` mirror the current `ControlVector.status` and `ControlVector.expiry` for the item's `resource_id`.
- `record_count` counts distinct stable declared selectors at the current version boundary (same answer shape as `Resolution.record_inventory`).
- For `namespace=basenames`, address-name membership and relations come from the Base authority split. Primary-claim intake and transport state don't add rows or widen relations.[^bn-readme-l69][^bn-readme-l70][^v1-l2rev-base-deploy][^v1-l2rev-event]

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

`resource_id` is opaque and is the stable API key for resource-scoped roles and permissions. `resource_hex` is deferred unless a stable projected field is documented for the namespace; clients must not derive it from `resource_id`, `namehash`, token ID, or calldata. Reads the same exact-name projection as `GET /v1/names/{namespace}/{name}`. `view=full` is reserved and still returns `400 invalid_input`; OpenAPI advertises only `view=compact`.

## `GET /v1/resources/{resource_id}/permissions`

Resource-centric current effective permission rows.

Query: `subject`, `scope`, `cursor`, `page_size`.

Each item: `resource_id`, `subject`, `scope`, `effective_powers`, `grant_source`, `revocation_source`, `inheritance_path`, `transfer_behavior`.

Rules:

- `resource_id` is the truth anchor. Surface names and resolver addresses appear only as explanatory context.
- `effective_powers` is server-computed post-scope-modifier. Clients don't apply NameWrapper fuse masks themselves.
- Resolver-scoped permissions are rows in this collection with resolver-scope detail, not a separate truth system.
- For ENSv1 registry-only authority, `subject` is the current ENS Registry owner. That owner, or an operator approved by that owner, is authorized by ENS Registry to transfer node ownership, transfer or create subnodes, and set resolvers, so registry-only permission rows expose `resource_control` and resolver-scoped `resolver_control` when a resolver target is declared.[^v1-registry-l16][^v1-registry-l60][^v1-registry-l71][^v1-registry-l86]
- ENSv1 registrar renewal events update registrar lease expiry and lineage, but they do not switch the current resource away from a divergent registry owner. Divergence can be introduced by an ENS Registry owner change or by a registrar token transfer away from the retained registry owner. The current resource only moves back to registrar authority when the registrar and registry subjects realign.
- For ENSv1 wrapper-backed resources, current NameWrapper fuses are folded into `effective_powers`. A burned fuse removes any public power that depends on the prohibited operation, and a row whose powers are fully masked is omitted. Upstream emits fuses through `NameWrapped` and `FusesSet` and gates wrapper operations on those bits.[^v1-iname-l31][^v1-iname-l37][^v1-nw-l421][^v1-nw-l427][^v1-nw-l666][^v1-nw-l676][^v1-nw-l723][^v1-nw-l827][^v1-nw-l1023][^v1-nw-l132]
- A wrapper-backed answer is `full` only when the current fuse modifier for the selected resource snapshot was applied. If the projection can't prove current fuse state, the route fails closed rather than returning unmasked powers.
- `cursor` and `page_size` page over `subject_scope_asc`.
- Declared-state only; `verified_state` is `null`.
- Response provenance summarizes the filtered `permissions_current` collection. Per-row grant, revocation, inheritance, and transfer details stay on each row.

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
- Compact item unit is one `(resource_id, subject, scope)` row. `effective_powers` is an array within that one scope; the same account appears in separate items when it has both resource-scoped and resolver-scoped powers. Summary metadata may group rows by subject, but compact `items` do not.
- Compact role rows do not expose provenance, raw facts, normalized-event IDs, or execution traces. Row-granular grant lineage stays on `GET /v1/resources/{resource_id}/permissions`.
- `view=full` is reserved and still returns `400 invalid_input`; OpenAPI advertises only `view=compact`.

## `GET /v1/resolvers/{chain_id}/{resolver_address}/overview`

Compact resolver overview using the resolver target and `resolver_current` boundary.

Query: `include=nodes,aliases,roles,events`, `view=compact`, `meta=none|summary|full`.

Defaults: `view=compact`, `meta=summary`, `include=nodes,aliases,roles,events`.

`data` is `ResolverOverviewCompact` for `view=compact`.

Rules:

- `counts.{nodes,aliases,role_holders,events}` are present only when the corresponding section is projected. Unsupported sections appear in `meta.unsupported_fields` unless `meta=none`.
- `nodes` and `aliases` are `null` when their fan-in is unprojected, and they appear in `meta.unsupported_fields` accordingly. Unsupported fan-in is never rendered as a supported zero count.
- `roles` is the compact role-holder list from resolver-scoped permission rows when projected; row-granular lineage stays on permissions routes.
- `events` is a compact event list from canonical normalized events for the target when projected. Selector-specific record history is deferred.
- `view=full` is reserved and returns `400 invalid_input`.

## `GET /v1/explain/resolutions/{namespace}/{name}/execution`

Persisted verified execution explain.

Query: `records` (required).

`data` matches the current surface and binding for the explicit `{namespace, name}` target. `declared_state` is `null`.

`verified_state`:

- `execution`: `ResolutionExecutionExplainSummary`
- `verified_queries`

Rules:

- Verified-only; doesn't duplicate declared topology, inventory, or cache.
- `at`, `chain_positions`, `consistency` are not on this route.
- Duplicate or malformed `records` selectors return `400 invalid_input`.
- Keyed by the explicit `{namespace, name}` exact surface and requested selector set. Explains the persisted answer.
- Public verified-resolution support boundary matches the full profile read for that exact surface. ENS direct, alias-only, and wildcard-derived classes are in scope. Basenames supports the exact-surface transport-assisted direct-path class through the L1 Resolver, including persisted CCIP-Read steps.[^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]
- `verified_queries` reuses selector-scoped result objects, request order, and `ResultStatus` from full profile reads.
- `verified_state.execution.execution_trace_id == provenance.execution_trace_id`. `verified_queries[*].provenance` stays under that same `execution_trace_id`.
- `verified_state.execution.steps` is the persisted ordered step summary. Not raw calldata, raw gateway payloads, or a replayable dump.
- The route doesn't trigger fresh execution and doesn't synthesize from declared topology. With no persisted answer for the requested surface and selector set, return `404 not_found`.
- For `{namespace, name, records}`, top-level `coverage` matches the explicit exact-name target's profile/coverage answer.
- No `include` expansions.

## `GET /v1/history/names/{namespace}/{name}`

Canonical normalized-event history for one logical name anchor.

Query: `scope=surface|resource|both` (default `both`), `view=compact|full` (default `full`), `meta=none|summary|full`, `cursor`, `page_size`.

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

Query: `namespace`, `relation=registrant|token_holder|effective_controller`, `scope=surface|resource|both` (default `both`), `view=compact|full` (default `full`), `meta=none|summary|full`, `cursor`, `page_size`.

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
- `view=full` is reserved and still returns `400 invalid_input`; OpenAPI advertises only `view=compact`.

## `GET /v1/primary-names/{address}`

Claimed and verified primary name for one address. The app fast path defaults to the ENS
coin type 60 tuple while still allowing explicit tuple selectors.

Query: `mode=declared|verified|both`, `coin_type` (default `60`), `namespace` (default `ens`).

`data`: `address`, `namespace`, `coin_type`.

Populated `declared_state`: `claimed_primary_name`. Populated `verified_state`: `verified_primary_name`.

Both objects use `ResultStatus`. `claimed_primary_name` uses `success`, `not_found`, `unsupported`, `invalid_name`. `verified_primary_name` uses those plus `mismatch` and `execution_failed`.

Rules:

- Head-only. No `at` or `consistency`.
- `claimed_primary_name` is the candidate only; it never implies verification.
- For ENS, the persisted admitted claim source is `ens_v1_reverse_l1` reverse intake at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`.[^v1-revreg-deploy][^v1-revreg-l15][^v1-revreg-l19][^v1-revreg-l74][^v1-revreg-l83][^v1-revreg-l84]
- `namespace` defaults to `ens` and `coin_type` defaults to `60`. Supplying both selectors keeps the exact tuple behavior for non-default use.
- Declared lookup first reads `primary_names_current(address, coin_type, namespace)`. If that tuple is missing and the request is the default ENS/60 tuple, the route may perform an on-demand Ethereum Mainnet RPC lookup against the current reverse node resolver and return the normalized `name(bytes32)` value as `claimed_primary_name.name`.[^v1-registry-deploy][^v1-revreg-l137][^v1-registry-l137][^v1-nameresolver-l7][^v1-nameresolverimpl-l25]
- On-demand ENS/60 fallback provenance is `{source_family: "ens_reverse_rpc", resolver_address}`. It is route-local read provenance, not projection lineage, and does not populate `primary_names_current`.
- A successful on-demand lookup with no resolver, empty name, wrong namespace, or unnormalizable reverse name returns `claimed_primary_name.status=not_found` with ENS reverse-RPC partial coverage. Missing or failing API RPC configuration suppresses the fallback and keeps the route in the persisted/no-fallback coverage class.
- For `mode=verified|both`, a successful ENS/60 on-demand reverse claim is verified immediately by calling the ENS Universal Resolver proxy for `addr:60` on the claimed name at provider `latest`.[^v1-ur-deploy][^v1-iur-l44][^v1-iur-l52] Matching address returns `verified_primary_name.status=success`; a concrete nonmatching address returns `mismatch`; an empty `addr` result returns `not_found`; an RPC or malformed-return failure returns `execution_failed`.
- Persisted `claimed_primary_name.name` comes only from the exact requested `primary_names_current(address, coin_type, namespace)` row's declared normalized claim-identity source, including projection-owned legacy reverse-resolver hydration for configured event-silent ENSv1 reverse resolvers. Resolver-edge-only legacy hydration may persist the exact row only after the hydrated reverse name resolves forward for `addr:60` through the ENS Universal Resolver at the same hash-pinned checkpoint to an ETH address whose computed `addr.reverse` node matches the candidate node.[^v1-revreg-l137][^v1-registry-l137][^v1-nameresolver-l7][^v1-iaddrres-l11][^v1-iur-l44][^v1-iur-l52] Outside exact-row hydration and the ENS/60 on-demand fallback, it is never synthesized from manifest presence, resolver-backed name data, verified execution identity, tuple presence, or another tuple's stored identity.
- For Basenames, the admitted claim family is `basenames_base_primary` at the ENSv1 Base `L2ReverseRegistrar` address `0x0000000000D8e504002cC26E3Ec46D81971C1664`, keyed by `NameForAddrChanged(address,string)` and Base coin type `2147492101`.[^v1-l2rev-base-deploy][^v1-l2rev-base-args][^v1-l2rev-event][^v1-l2rev-nameforaddr] Claim intake only — does not replace the Base registry/registrar/resolver families for declared truth on exact-name, address-name, or children reads, and the Basenames `ReverseRegistrar` is not the primary-name value authority.[^bn-readme-l33][^bn-revreg-l12][^bn-revreg-l150]
- `claimed_primary_name.raw_claim_name` may appear only when `status=invalid_name` for the exact requested tuple, copied verbatim from `primary_names_current.raw_claim_name`. Blank or whitespace-only raw claims become `not_found`; `invalid_name` is for nonblank claims that fail normalization.
- `claimed_primary_name.provenance` is exact-tuple declared provenance from the requested `primary_names_current` row, optionally with projection-owned legacy reverse-resolver hydration metadata, or route-local `ens_reverse_rpc` provenance for the ENS/60 on-demand fallback. Persisted declared provenance strips any verified-primary lookup/invalidation hook material and omits `execution_trace_id`.
- `verified_primary_name` field boundary: `{status, name?, unsupported_reason?, failure_reason?, provenance?}`. `name` uses `NameRef` and appears only for `success` or `mismatch`. `raw_claim_name` never appears here.
- `verified_primary_name.provenance` (when present) is the section-local `{execution_trace_id, manifest_versions}` for a persisted same-tuple answer. Route-local ENS/60 on-demand verification has no persisted trace and omits section provenance. The primary-name route does not emit top-level route provenance by default.
- `verified_primary_name` is authoritative only on `status=success`. `status=mismatch` means the claim normalizes and the verified target resolves for the requested `coin_type` but doesn't equal the requested `{address}`.
- `failure_reason` on `verified_primary_name` is verification-local and may appear only for `mismatch`, `invalid_name`, or `execution_failed`.
- Verified persisted-readback uses execution identity `request_type=verified_primary_name` keyed on `{namespace}:{normalized_address}:{coin_type}` (lowercased address).
- `primary_names_current(address, coin_type, namespace)` remains the only claim-side lookup/invalidation anchor for persisted claim and verified-primary readback.
- For Basenames in `mode=verified|both`, persisted `verified_primary_name` results are returned for the exact requested tuple via `basenames_execution`. Declared and verified stay separate because declared primary values enter through ENSv1's Base `L2ReverseRegistrar`, while verified resolution enters through the L1 Resolver.[^v1-l2rev-base-deploy][^v1-l2rev-event][^bn-readme-l22][^bn-l1resolver-l13]
- Invalid address syntax or malformed `namespace` / `coin_type` returns `400 invalid_input`.
- Unsupported public namespace returns `404 not_found`.
- No declared or verified answer for the tuple returns `200` with `status=not_found`.
- Unsupported claim surfaces or verified entrypoints return `200` with the corresponding object `status=unsupported`.

### Route-level coverage

Local to the requested tuple. Not the single-name `Coverage` from `GET /v1/coverage/{namespace}/{name}`.

- ENS mainnet supported tuple class: `coverage.status=partial`, `exhaustiveness=non_enumerable`, `source_classes_considered=["ens_v1_reverse_l1","ens_execution"]`, `enumeration_basis=primary_name_lookup`.[^v1-revreg-deploy][^v1-ur-deploy]
- ENS/60 on-demand fallback class: `coverage.status=partial`, `exhaustiveness=non_enumerable`, `source_classes_considered=["ens_reverse_rpc"]` for declared-only fallback and `["ens_reverse_rpc","ens_execution_rpc"]` when on-demand forward verification runs, `enumeration_basis=primary_name_lookup`.
- Basenames mainnet supported tuple class: `coverage.status=partial`, `exhaustiveness=non_enumerable`, `source_classes_considered=["basenames_base_primary","basenames_execution"]`, `enumeration_basis=primary_name_lookup`.
- Out of class: `coverage.status=unsupported`, `exhaustiveness=not_applicable`, `source_classes_considered=[]`, `enumeration_basis=primary_name_lookup`, `unsupported_reason="primary-name exact-tuple persisted readback is not supported for the requested tuple"`. Out-of-class verified objects use `verified_primary_name.status=unsupported`.

Persisted class membership, ENS/60 fallback availability, and result-object status are separate: `claimed_primary_name.status` describes the answer, while route-level coverage describes whether the answer came from the persisted exact-tuple class, the on-demand ENS/60 reverse RPC class, or neither.

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
GET /v1/names/ens/alice.eth/records
GET /v1/names/ens/alice.eth/records?include=resolver_address,known_text_keys,avatar,content_hash,coins&texts=avatar,com.twitter&coin_types=60,0
```

Full profile with verified records:

```
GET /v1/profiles/names/alice.eth
GET /v1/profiles/names/alice.eth?mode=both
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

[^bn-registry-l81]: (upstream: .refs/basenames/src/L2/Registry.sol:L81 @ basenames@1809bbc)
[^bn-registry-l120]: (upstream: .refs/basenames/src/L2/Registry.sol:L120 @ basenames@1809bbc)
[^bn-registry-l122]: (upstream: .refs/basenames/src/L2/Registry.sol:L122 @ basenames@1809bbc)
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
[^v1-iaddrres-l11]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L11 @ ens_v1@91c966f)

[^v1-registry-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ENSRegistry.json:L2 @ ens_v1@91c966f)
[^v1-revreg-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-l2rev-base-deploy]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-l2rev-base-args]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f)
[^v1-l2rev-event]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
[^v1-l2rev-nameforaddr]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L154 @ ens_v1@91c966f)
[^v1-revreg-l15]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f)
[^v1-revreg-l19]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L19 @ ens_v1@91c966f)
[^v1-revreg-l74]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f)
[^v1-revreg-l83]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f)
[^v1-revreg-l84]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
[^v1-revreg-l137]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L137 @ ens_v1@91c966f)
[^v1-registry-l16]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L16 @ ens_v1@91c966f)
[^v1-registry-l45]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L45 @ ens_v1@91c966f)
[^v1-registry-l60]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60 @ ens_v1@91c966f)
[^v1-registry-l71]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L71 @ ens_v1@91c966f)
[^v1-registry-l82]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L82 @ ens_v1@91c966f)
[^v1-registry-l86]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L86 @ ens_v1@91c966f)
[^v1-registry-l137]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137 @ ens_v1@91c966f)
[^v1-nameresolver-l7]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L7 @ ens_v1@91c966f)
[^v1-nameresolverimpl-l25]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L25 @ ens_v1@91c966f)

[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309)
[^v2-deploy-ethrc]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)
[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309)
[^v2-events-l15]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15 @ ens_v2@554c309)
[^v2-iethreg-l32]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309)
[^v2-iethreg-l53]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2@554c309)
