# Consumer Capabilities

This document lists the consumer-facing capabilities the bigname `v1` API serves and the routes that serve them. Identity, coverage, and resolution semantics live in [`architecture.md`](architecture.md); wire format in [`api-v1.md`](api-v1.md).

## Route sets

Use these sets when choosing a public route:

| Set | Routes | Intended consumers |
| --- | --- | --- |
| Native slim identity | `POST /v1/identity:lookup`, `GET /v1/status` | partner-1 style feeds, profile aggregation, and shadow comparison. Feed rendering should use `profile=feed`, which is backed by compact count/identity sidecars. |
| Canonical product reads | `/v1/names*`, `/v1/profiles/names/*`, `/v1/addresses/{address}/names`, `/v1/primary-names*`, `/v1/resources/{resource_id}/permissions`, `/v1/events` | first-party app, explorer, and public API integrations that want the bigname contract. |
| Metadata/control plane | `/v1/namespaces/*`, `/v1/manifests/*`, `/healthz` | manifest, namespace, and liveness introspection. |
| Diagnostics/provenance | `/v1/coverage/*`, `/v1/explain/*` | debugging completeness, support, derivation, persisted execution, and audit paths. |
| Specialist adjuncts | `/v1/roles`, `/v1/names/*/roles`, `/v1/resources/lookup`, `/v1/history/*`, `/v1/resolvers/*/overview` | supported routes for specialist views and narrow adjuncts. Prefer the canonical product reads above when they satisfy the use case. |

## Capability matrix

| Capability | Example consumer surface | Native `v1` responsibility |
| --- | --- | --- |
| exact name profile | profile pages, record editing, registration views | `Name.registration` + `Resolution` |
| names owned / controlled by address | dashboards and search flows | `Address.names` |
| names owned / controlled by address with role summary | dashboard lists | `Address.names` with `include=role_summary` |
| declared child subnames and counts | subname pages and creation flows | `Name.children` |
| record inventory for editing | profile and records screens | `Resolution.record_inventory` + `Resolution.record_cache` |
| verified record reads | profile, send, and address-resolution flows | `Resolution.verified_queries` |
| name history | profile history pages | `History(scope=both)` |
| address history across names | address activity views | `Address.history` |
| role holders for a resource | roles pages | `Permissions.by_resource` |
| role change history | roles history pages | `History(filter=permissions)` |
| resolver-centric overview | resolver pages | `Resolver` |
| claimed vs verified primary name | dashboard and profile | `PrimaryName.claimed_primary_name` + `PrimaryName.verified_primary_name` |
| compact name search and suggestions | dashboard search and explorer search | `GET /v1/names` |
| compact resolver records | profile records and record panels | `GET /v1/names/{namespace}/{name}/records` |
| compact events | activity tables | `GET /v1/events` and history routes with `view=compact` |
| roles by account/resource/name | resolver and role pages | `GET /v1/roles`, `GET /v1/names/{namespace}/{name}/roles`, `GET /v1/resources/lookup` |
| compact resolver overview | resolver overview pages | `GET /v1/resolvers/{chain_id}/{resolver_address}/overview` |
| native slim identity | feed identity, profile aggregation, shadow comparison | `POST /v1/identity:lookup` native DTOs over current projections, with result-level input/normalization and no routine `normalized_name` peer field |

## Route mapping by capability

| Capability | Route | Notes |
| --- | --- | --- |
| exact name profile | `GET /v1/names/{namespace}/{name}`, `GET /v1/profiles/names/{name}`, `GET /v1/names?namespace=...&name=...` | Exact-name lookup suppresses routine route-level provenance; profile adds declared topology/cache and verified record results. Coverage is reported at `GET /v1/coverage/{namespace}/{name}`. |
| names by address | `GET /v1/names?account=...&relation=token_holder|any` with optional `contains=`, `prefix=`, `sort=name|expiry_date|registration_date` | Compact `CompactDomainSummary` rows. Counts use collection metadata where supported. |
| names by address with role summary | `GET /v1/addresses/{address}/names?include=role_summary` | Additive expansion over the same address-to-surface collection — not a separate route. Adds `role_summary`, `subname_count`, `record_count`, `status`, `expiry`. |
| declared children | `GET /v1/names/{namespace}/{name}/children?include=counts` | Declared direct-child bucket only. Linked, alias, and wildcard buckets are not enumerated. |
| record inventory and cache | `GET /v1/profiles/names/{name}` (full profile), `GET /v1/names/{namespace}/{name}/records` (compact) | `record_inventory` defines the stable selector space; `record_cache` is the last-known declared value over that space. The profile route has no `records` query knob: it reads every declared selector/gap/cache record for the selected snapshot and, for verified/both modes, executes that whole server-derived set. It falls back to `addr:60`, `avatar`, `contenthash`, `text:description`, `text:url`, and `text:email` only when a supported declared inventory exists but has no declared records. Missing, stale, or explicitly unsupported inventory stays unsupported. The compact route defaults to `mode=declared`; callers can opt into `mode=auto`, `verified`, or `both` when they want selector-specific fallback or execution-backed values. |
| verified record reads | `GET /v1/profiles/names/{name}` `verified_queries`, selector-specific `GET /v1/names/{namespace}/{name}/records`, plus the execution explain route | Verified queries are execution-derived. They do not backfill `record_inventory` or `record_cache` in the same response. |
| name history | `GET /v1/history/names/{namespace}/{name}` | Canonical normalized-event reads with `scope=surface|resource|both`. |
| address history | `GET /v1/history/addresses/{address}` | Address-anchor composition over the same history contract. |
| role holders | `GET /v1/resources/{resource_id}/permissions` | One current row per `(resource_id, subject, scope)`. |
| role history | `GET /v1/history/resources/{resource_id}` | Permission events filtered out of the same history contract. |
| resolver overview | `GET /v1/resolvers/{chain_id}/{resolver_address}/overview` | Each compact section (`nodes`, `aliases`, `roles`, `events`) is supported only when a projection owns the fan-in; unsupported sections are `null` and listed in `meta.unsupported_fields`. |
| primary name | `GET /v1/primary-names/{address}` | Defaults to the ENS coin type 60 app lookup. `claimed_primary_name` is candidate-only; `verified_primary_name` is authoritative only when `success`. ENS/60 can use on-demand reverse RPC and forward `addr:60` verification when the persisted projection has not materialized the tuple; non-default explicit tuple verified readback remains persisted-only. |
| compact name search | `GET /v1/names?namespace=...&prefix=...` or `contains=...` | Search only — no availability, pricing, or registration workflow semantics. |
| compact events | `GET /v1/events` and history routes with `view=compact` | Canonical normalized events. Selector-specific record history beyond type filters is not enumerated. |
| compact roles | `GET /v1/roles`, `GET /v1/names/{namespace}/{name}/roles`, `GET /v1/resources/lookup` | `RoleRow` exposes opaque `resource_id`, nullable `resource_hex`, projected `role_bitmap`, and effective powers. |
| native slim identity | `POST /v1/identity:lookup`, `GET /v1/status` | App-facing native surface for partner-1 style indexed reads. `profile=feed` is the compact latency path for feeds and timelines; `profile=detail` preserves full native identity rows. Requirements reference: [`docs/partners/partner-1-indexing-requirements.md`](partners/partner-1-indexing-requirements.md). It does not create partner-specific identity composition and does not widen source-family admission. |

Compact defaults suppress full provenance, full coverage, internal projection identifiers, source bookkeeping, and raw normalized-event payloads. Routes may expose route-owned compact provenance or `meta=full` only where their contract says so; compact-only routes keep `view=full` reserved and return `400 invalid_input` for it. `GET /v1/profiles/names/{name}` follows this compact default even though it is the full-profile app path; use `meta=full` or explain/audit surfaces for full envelopes and trace detail.

`GET /v1/names` keeps the namespace-omitted bridge where an omitted `namespace` spans supported public namespaces. First-party replacement mappings should pass an explicit namespace whenever the app knows it; omitted namespace is not an ENS-only shortcut.

The slim surface removes namespace-inferred `/v1/resolve*` aliases. Canonical name collection, exact-name, records, children, and role routes keep explicit `{namespace}`. The app full-profile fast path is the deliberate namespace-inferred exception: `GET /v1/profiles/names/{name}` normalizes the input, infers the namespace, and returns the inferred namespace on `data.namespace`.

`POST /v1/identity:lookup` uses the same namespace inference rule and reads only current projections plus persisted projection metadata. It is the native slim read surface for partner-style feeds, profile aggregation, and shadow comparison, not a replacement core model. `profile=feed` intentionally narrows reverse responses to one compact identity row per input address and reads precomputed count/identity sidecars, so feed rendering does not pay for full `IdentityRecord` hydration, live first-row joins, or deep provenance. Production ENSv2 source-family manifests remain outside this slice; the existing ENSv2 rule stays limited to the `sepolia-dev` exact-name profile until production deployment metadata is admitted through the manifest process.

## Coverage notes

- `Address.names` with `include=role_summary` is an additive expansion of the same address-to-surface collection, not a separate route or replacement surface.
- `Address.history` is the declared-state address activity read over address-derived surface and resource anchors. It reuses the shared history contract rather than introducing a separate truth system.
- `GET /v1/profiles/names/{name}` is the app mixed profile route. `record_inventory` defines the known record-selector space; `record_cache` is the declared last-known-value view over that space; `verified_queries` is the server-selected execution answer set.
- ENSv2 exact-name profile support is promoted only for the `sepolia-dev` deployment profile when `ens_v2_registrar_l1` declares `exact_name_profile = "supported"`. That promotion covers exact-name profile reads from the admitted `ETHRegistry` and `ETHRegistrar`; it does not graduate resolver-profile support, verified resolution, primary names, or history coverage.[^v2-ethreg][^v2-ethrc][^v2-iperm-l34][^v2-iethreg-l32]
- ENSv1 profile `record_inventory`, `record_cache`, and resolver overview require ENS Labs PublicResolver-generation profile admission for complete family coverage, latest-only behavior, and event-to-call parity. Retained generic resolver-local events provide observed selector and cache facts while a profile is pending; malformed topic collisions stay raw without contributing to inventory or cache.[^v1-ens-l12][^v1-iaddr][^v1-iaddress][^v1-itext-l5][^v1-itext-l10] Shared PublicResolver targets do not enumerate current-name fan-in in resolver-overview `nodes`, `aliases`, or `events`; unsupported compact sections return `null` and appear in `meta.unsupported_fields` with `resolver_binding_enumeration_not_projected` in `meta.coverage.unsupported_reason`. Exact-name resolver state stays on exact-name routes.
- The declared resolver-profile gate is separate from profile `verified_queries`. For an already supported verified-resolution path, `resolver_family_pending` declared state stays visible in `record_inventory` and `record_cache` but does not suppress matching persisted Universal Resolver readback.[^v1-ur-l44][^v1-ur-l52]
- Basenames declared resolver-profile support is `L2Resolver`-compatible only. A discovered Base resolver that is watched but has `pending` or `unsupported` profile state remains topology-only — profile `record_inventory`, profile `record_cache`, and resolver overview stay unsupported. This gate is independent of Basenames L1 transport and execution: the Mainnet `L1Resolver`, `basenames_execution`, and any offchain gateway do not satisfy declared resolver-profile support.[^bn-l2resolver-l22][^bn-l2resolver-l182][^bn-l2resolver-l193][^bn-l2resolver-l209][^bn-l2resolver-l225]
- ENSv1 dynamic resolver-profile admission is profile-exact, not latest-PublicResolver-only. A resolver with `pending` or `unsupported` profile state may expose only observed selector and cache facts. An admitted legacy generation satisfies only the record/interface families listed for that profile; unsupported sections remain explicit.[^v1-pres-l20][^v1-pres-l31][^v1-resbase-l17]
- ENSv1 pubkey evidence is unadmitted. Known PublicResolver-generation profiles keep it explicit `unsupported`; unknown resolvers keep it `pending`. Generic resolver-record observation does not promote it.
- ENSv1 reverse and primary resolver `NameChanged` text is preimage intake only. It can attach already-observed forward-node facts to a human-readable name; it does not create primary-name truth, exact-name authority, or record support without those forward-node facts.[^v1-namech-l10][^v1-namech-l18][^v1-revreg-l129][^v1-revreg-l130]
- `ENSRegistryOld` is admitted as migration-aware input under `ens_v1_registry_l1`. Current-registry `NewOwner` migration, suppression of later old-registry topology for migrated nodes, and the root-resolver exception are honored before any old-registry fact contributes to declared reads. The current-registry subgraph start `9380380` stays current-registry scope only; the old-registry start is `3327417`.[^subgraph-l15][^subgraph-l39][^subgraph-l44][^subgraph-ts-l238][^subgraph-ts-l246]
- `PrimaryName` is one mixed route. `claimed_primary_name` is the declared claim candidate; `verified_primary_name` is the execution-derived verification result. Route-level coverage is `partial` for persisted exact-tuple classes and for the ENS/60 on-demand reverse RPC fallback, including `ens_execution_rpc` when verified mode performs live forward verification; unsupported tuples stay explicit.
- Mixed profile and primary-name results reuse the same `ResultStatus` vocabulary: `success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, `execution_failed`.

## Explicitly out of scope

The following are direct-chain or app-local services and are not bigname routes: favorites and local services, name availability, registration pricing, direct contract workflows, DNSSEC, app images, faucet, direct reverse checks not backed by a projection.

The following are deferred until projection-backed equality indexes, stable projected fields, or fan-in projections exist: resolved-address listing, `resource_hex` lookup, selector-specific record history beyond event filters, linked/alias/wildcard child buckets, and unprojected resolver fan-in. Unsupported filters or sections return explicit unsupported state, never silent empty results.

---

[^v1-ens-l12]: (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)
[^v1-iaddr]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f)
[^v1-iaddress]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f)
[^v1-itext-l5]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f)
[^v1-itext-l10]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L10 @ ens_v1@91c966f)
[^v1-pres-l20]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^v1-pres-l31]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f)
[^v1-resbase-l17]: (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f)
[^v1-namech-l10]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L10 @ ens_v1@91c966f)
[^v1-namech-l18]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L18 @ ens_v1@91c966f)
[^v1-revreg-l129]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f)
[^v1-revreg-l130]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
[^v1-ur-l44]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L44 @ ens_v1@91c966f)
[^v1-ur-l52]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L52 @ ens_v1@91c966f)

[^bn-l2resolver-l22]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
[^bn-l2resolver-l182]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc)
[^bn-l2resolver-l193]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)
[^bn-l2resolver-l209]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc)
[^bn-l2resolver-l225]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)

[^v2-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309)
[^v2-ethrc]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)
[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309)
[^v2-iethreg-l32]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309)

[^subgraph-l15]: (upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)
[^subgraph-l39]: (upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)
[^subgraph-l44]: (upstream: .refs/ens_subgraph/subgraph.yaml:L44 @ ens_subgraph@723f1b6)
[^subgraph-ts-l238]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6)
[^subgraph-ts-l246]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6)
