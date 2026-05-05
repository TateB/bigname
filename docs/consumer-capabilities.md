# Consumer capabilities

What the bigname `v1` API actually serves, and which routes serve it. Identity, coverage, and resolution semantics live in [`architecture.md`](architecture.md); wire format in [`api-v1.md`](api-v1.md); per-route detail in [`api-v1-routes.md`](api-v1-routes.md).

This file is the consumer-facing index. Anything not listed here is either out of scope or deferred.

## Capabilities

| Capability | Where it lands |
|---|---|
| Exact name profile | `GET /v1/names/{namespace}/{name}` (full envelope) and the compact lookup form on `GET /v1/names?namespace=…&name=…` |
| Names owned/controlled by an address | `GET /v1/addresses/{address}/names`, `GET /v1/names?account=…` |
| Address dashboard with role summary | `GET /v1/addresses/{address}/names?include=role_summary` |
| Direct subnames | `GET /v1/names/{namespace}/{name}/children?include=counts` |
| Record inventory (the selector space) | `GET /v1/resolutions/{namespace}/{name}` |
| Compact records (read for UI) | `GET /v1/resolve/{name}/records`, `GET /v1/names/{namespace}/{name}/records` |
| Verified record reads | `GET /v1/resolutions/{namespace}/{name}` `verified_queries`, `GET /v1/explain/resolutions/{namespace}/{name}/execution` |
| Name history | `GET /v1/history/names/{namespace}/{name}` (`scope=surface|resource|both`) |
| Address history | `GET /v1/history/addresses/{address}` |
| Resource permissions | `GET /v1/resources/{resource_id}/permissions` |
| Permission/role history | `GET /v1/history/resources/{resource_id}` |
| Resolver overview | `GET /v1/resolvers/{chain_id}/{resolver_address}` (full), `…/overview` (compact) |
| Primary name (claimed and verified) | `GET /v1/primary-names/{address}` |
| Compact name search | `GET /v1/names?namespace=…&prefix=…`, `…&contains=…` |
| Compact events | `GET /v1/events`, history routes with `view=compact` |
| Roles by account/resource/name | `GET /v1/roles`, `GET /v1/names/{namespace}/{name}/roles`, `GET /v1/resources/lookup` |

Compact routes default to `view=compact` and `meta=summary`. They suppress provenance, full coverage, internal projection ids, source bookkeeping, and raw normalised-event payloads unless the caller asks for `meta=full`, `view=full`, or uses an explain/audit surface.

`GET /v1/resolve/{name}` and `GET /v1/resolve/{name}/records` are convenience entries to `Resolution` and the compact records contract. Inference: exact `base.eth` → `namespace=ens`; `*.base.eth` → `namespace=basenames`; other supported ENS names → `namespace=ens`. Inferred Basenames requests use the Basenames-local selector and topology support — they don't fall back to ENS.

## Coverage notes

These are the gates that decide whether a route returns supported output, partial coverage, or explicit unsupported.

- **`Resolution`** is one mixed route. `record_inventory` defines the known selector space; `record_cache` is the declared last-known-value view; `verified_queries` is the request-bound execution answer set. Verified queries don't backfill inventory or cache in the same response.
- **ENSv2 exact-name profile** is supported only on the `sepolia-dev` deployment profile when `ens_v2_registrar_l1` declares `exact_name_profile = "supported"`. The promotion covers exact-name profile reads from the admitted `ETHRegistry` and `ETHRegistrar` only — it doesn't graduate resolver-profile support, verified resolution, primary names, or history coverage[^v2-iperm-l34][^v2-iethreg-l32].
- **ENSv1 record reads** require ENS Labs PublicResolver-generation profile admission for complete family coverage, latest-only behaviour, and event-to-call parity. Retained generic resolver-local events provide observed selector and cache facts while a profile is `pending`; topic collisions with malformed payloads stay raw without contributing to inventory or cache[^v1-pres-l20].
- **Shared PublicResolver fan-in** (`bindings`, `aliases`, event summaries on the resolver overview) returns `UnsupportedSummary` with `resolver_binding_enumeration_not_projected` for ENSv1 PublicResolver targets — the fan-in is unbounded. Exact-name resolver state stays on exact-name routes.
- **Verified resolution vs declared resolver-profile gaps**: `resolver_family_pending` declared state stays visible in `record_inventory` and `record_cache` but doesn't suppress matching persisted Universal Resolver readback for an otherwise supported path[^v1-iur-l44].
- **Basenames declared resolver-profile support** is `L2Resolver`-compatible only. A discovered Base resolver that is watched but has `pending` or `unsupported` profile state is topology-only — `Resolution.record_inventory`, `Resolution.record_cache`, and resolver-overview stay unsupported. The Mainnet `L1Resolver`, `basenames_execution`, and any offchain gateway don't satisfy declared resolver-profile support[^bn-l2resolver-l22].
- **ENSv1 dynamic resolver-profile admission** is profile-exact, not latest-PublicResolver-only. Older admitted generations satisfy only the families listed for that profile; unsupported sections remain explicit.
- **ENSv1 pubkey evidence** is unadmitted. Known PublicResolver-generation profiles keep it explicit `unsupported`; unknown resolvers keep it `pending`.
- **ENSv1 reverse/primary `NameChanged` text** is preimage intake only. It can attach already-observed forward-node facts to a human-readable name; it doesn't create primary-name truth, exact-name authority, or record support without those forward-node facts[^v1-namechanged-l10].
- **`ENSRegistryOld`** is admitted as migration-aware input under `ens_v1_registry_l1`. Current-registry `NewOwner` migration, suppression of later old-registry topology for migrated nodes, and the root-resolver exception are honoured before any old-registry fact contributes to declared reads. The current-registry subgraph start `9380380` stays current-registry scope only; the old-registry start is `3327417`.
- **`PrimaryName`** is one mixed route. `claimed_primary_name` is the declared claim candidate; `verified_primary_name` is the execution-derived verification result. Route-level coverage is `partial` for the ENS and Basenames exact-tuple persisted-readback classes and explicit `unsupported` outside them.
- **`ResultStatus`** is shared between `Resolution` and `PrimaryName`: `success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, `execution_failed`.

## Explicitly out of scope

Direct-chain or app-local services that are **not** bigname routes:

- favourites and local services
- name availability
- registration pricing
- direct contract workflows
- DNSSEC
- app images
- faucet
- direct reverse checks not backed by a projection

Deferred until the relevant projection or index exists:

- `resolved_address` filtering
- `resource_hex` lookup
- selector-specific record history beyond event-type filters
- linked, alias-derived, and observed-wildcard child buckets
- shared resolver fan-in enumeration

Unsupported filters or sections always return explicit unsupported state — never silent empty results.

---

## Footnotes

[^v1-pres-l20]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^v1-namechanged-l10]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L10 @ ens_v1@91c966f)
[^v1-iur-l44]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L44 @ ens_v1@91c966f)
[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309)
[^v2-iethreg-l32]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@554c309)
[^bn-l2resolver-l22]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
