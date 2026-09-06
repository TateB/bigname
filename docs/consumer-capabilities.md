# Consumer Capabilities

This document maps the consumer-facing capabilities served by the bigname API.
Wire format and route details live in [`api-v2.md`](api-v2.md) and
[`api-v2-routes.md`](api-v2-routes.md).

## Served route sets

| Set | Routes | Intended use |
| --- | --- | --- |
| Lookup | `POST /v2/lookup`, `GET /v2/status` | Batched name/address lookup and indexing readiness. |
| Product reads | `/v2/names/*`, `/v2/addresses/*`, `/v2/permissions`, `/v2/search`, `/v2/events`, `/v2/resolvers/*`, `/v2/namespaces/*` | Name, record, address, permission, event, resolver, and namespace reads. |
| Diagnostics | `/v2/diagnostics/*` | Coverage, binding, authority, record, manifest, and event inspection. |
| GraphQL compatibility | `POST /graphql` | The subgraph-shaped compatibility surface described below. |
| Operator health | `GET /healthz` | API process, opaque running-database-instance identity, and phase-runner heartbeat readiness. This is not a product route. |

The v1 REST surface has been removed. In particular,
`POST /v1/identity:lookup` no longer serves the native identity capability.
`POST /v2/lookup` owns batched forward and reverse lookup with the v2 envelope;
it does not preserve the deleted v1 DTOs.

## Capability mapping

| Capability | Route owner | Notes |
| --- | --- | --- |
| Batched forward and reverse lookup | `POST /v2/lookup` | `profile=feed` is the field-budgeted path; `profile=detail` returns the documented full record shape. |
| Indexing readiness | `GET /v2/status` | Per-chain projection progress, stored head, indexing-process liveness, network-head readiness, and required Sepolia completed-Ingest state plus [verification-level evidence](glossary.md#verification-level). |
| Exact name profile | `GET /v2/names/{name}` | Indexed or verified name and record fields, plus [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word) ENSv1 NameWrapper lifecycle and fuse data when backed, subject to the route's source rules. |
| Resolver records | `GET /v2/names/{name}/records` | Key-selected record reads plus inventory metadata. |
| Direct subnames | `GET /v2/names/{name}/subnames` | Latest-state direct-subname collection. |
| Name history | `GET /v2/names/{name}/history` | Name, registration, or combined history scope. |
| Names by address | `GET /v2/addresses/{address}/names` | Owner, manager, and registrant relations with optional expansions. |
| Primary name | `GET /v2/addresses/{address}/primary-name` | Indexed tuples and verified ENS coin-type 60 lookup as documented. |
| Address history | `GET /v2/addresses/{address}/history` | Latest-state address-anchored event history. |
| Permission holders | `GET /v2/permissions` | Known current permission rows that apply to each resource. Standard registry, registrar, and resolver approval/delegation paths are not yet authoritative enumerations, so coverage stays request-relative partial even for zero rows. An empty name-filter result reports `permission_support_unknown` when the name is missing or unrecognized, its current name is marked unsupported, or its current name is not bound to a registration resource. The exception is a resolved current name paired with an explicitly different `registration_id`: that supported filter combination selects no registration and returns an empty page without completeness metadata. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L118 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L78-L103 @ ens_v1@91c966f) ENSv1 NameWrapper holder enumeration remains a separate unsupported class. Returned current wrapper registrations still carry [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word) lifecycle and fuse data when backed. |
| Search | `GET /v2/search` | Name search only; no registration, pricing, or availability workflow. |
| Events | `GET /v2/events` | Product event collection. |
| Resolver overview | `GET /v2/resolvers/{chain_id}/{address}` | Resolver metadata, total section counts with deterministic samples capped at 100 items, and a separately paginated record-shaped bound-name collection, including [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word) ENSv1 NameWrapper metadata when backed. |
| Namespace metadata | `GET /v2/namespaces/{namespace}` | Product-facing namespace and capability metadata. |
| Pipeline diagnostics | `/v2/diagnostics/*` | Explicit diagnostic tier, separate from product reads. |

## Resolver address read modes

The records route, exact-name detail, and name results in batch lookup share
one indexed ENSIP-19 behavior. Projected exact entries remain event-derived.
When the selected resolver has the manifest-authorized
[resolver read feature](glossary.md#resolver-read-feature), an eligible EVM
coin-type request whose exact entry is empty or missing reads the projected
default entry instead. The records route identifies per-key derived results in
`records[key].meta`; values-only address maps contain the value without adding
provenance fields. Derived values use the requested getter's verified decode:
coin type `60` treats a 20-byte zero default as `not_found`, while EVM-range
multicoin selectors retain that non-empty byte value. Exact stored records are
not normalized by this rule. Completeness remains request-relative. ENSIP-19 defines the
coin-type-to-chain eligibility rule, and the admitted resolver getter performs
the default-entry fallback only when that rule returns a positive chain ID
`(upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L9-L38 @ ens_v1@91c966f)`
`(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)`
`(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L68-L85 @ ens_v1@91c966f)`.
The admitted archived-Sepolia implementation exposes the same two getter shapes
`(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L685-L697 @ ens_v2_sepolia_20260629@ccaeb58)`.

| Address read | Indexed | Auto | Verified |
| --- | --- | --- | --- |
| Exact entry | Exact value | Exact value | Chain value |
| Eligible EVM coin type, flagged resolver, default entry present | Derived value with per-key metadata | Derived value; no provider call | Chain value |
| Coin type 60, flagged resolver, default entry is 20 zero bytes | Derived `not_found` with per-key metadata | Derived `not_found`; no provider call | Chain `not_found` |
| Eligible EVM coin type, flagged resolver, default source authoritatively absent | Derived `not_found` with per-key metadata | Derived `not_found`; no provider call | Chain result |
| Default source unavailable or inventory non-authoritative | Explicit `unsupported` | Request-scoped verified fallback | Chain result |
| Ineligible coin type or unflagged resolver generation | Exact-key behavior; no derivation | Existing exact-key fallback policy | Chain result |

The auto column's exact-answer rule has one exception: for an Ethereum Mainnet
ENS name whose projected exact resolver is null and whose ordinary direct row
admits [Universal Resolver ancestor
discovery](glossary.md#universal-resolver-ancestor-discovery), all requested
keys execute through verified lookup. Retained exact inventory predates the
resolver-clear boundary and does not satisfy auto for that route.

The flagged deployments are the current ENS PublicResolver on mainnet, the
current Sepolia PublicResolver at `0xE99638b40E4Fff0129D56f03b55b6bbC4BBE49b5`,
and the admitted archived-Sepolia ENSv2 `PermissionedResolver` implementation. The
admitted Basenames address is the legacy resolver and remains unflagged; its
vendored coin-type getter reads exact storage, while the fallback-bearing
upgradeable resolver proxy is not admitted in this change.
`(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20-L31 @ ens_v1@91c966f)`
`(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L151-L166 @ ens_app_v3@7175858)`
`(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L2 @ ens_v2@a971bd64)`
`(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L2398 @ ens_v2@a971bd64)`
`(upstream: .refs/basenames/test/Fork/BaseMainnetConstants.sol:L9-L14 @ basenames@1809bbc)`
`(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/AddrResolver.sol:L35-L61 @ basenames@1809bbc)`

Verified answers never claim whether the on-chain resolver used exact storage,
default storage, or another execution path, so they omit derived metadata.

## ENSv1→ENSv2 mixed-history ownership

The replacement contract for exact-name and direct-subname current reads is
the per-name current-authority rule in
[`architecture.md`](architecture.md#ensv1ensv2-current-authority). Under that
rule, an [authority proof](glossary.md#authority-proof) first selects one
[authority epoch](glossary.md#authority-epoch), and every current field is then
selected inside that epoch. A migrated name keeps both eras in history while current
registration, control, resolver, expiry, address relations, and permissions
come only from its ENSv2 resource. Retained ENSv1 facts remain history and
provenance; they do not make the current read unsupported and cannot become
current again after an ENSv2 release. Release leaves
[released v2 authority](glossary.md#released-v2-authority), so later ENSv1
events cannot repopulate any current field. Slice 2C applies this rule to the
exact-name projection, the name-detail response, and per-result batch-lookup
records. Slice 2D makes the address-name, permission, search, primary-name, and
address-history collections consume that selected current registration, but
they acquire no row-local coverage status or unsupported-reason vocabulary;
callers inspect the exact-name or lookup result for coverage. An explicit
`registration_id` permission query may inspect a superseded ENSv1 registration
as historical/audit data. Every permission row
carries `authority_context`. The
[`current_for_name` context](glossary.md#current-for-name-authority-context)
means a `name` filter selected
the row's current registration for that requested name. A row admitted without
a `name` filter, including an explicit-`registration_id` or address-filtered
resource read, is `resource_audit` and makes no current-name claim; an optional
display name does not change that classification. Rows carrying the
[`resource_audit` context](glossary.md#resource-audit-context) remain queryable.
The marker changes only how that permission
response may be interpreted; the per-name ownership rule independently decides
which registration contributes current authority, address relations, and role
summaries. A superseded ENSv1 registration is therefore never selected, while a
current registration queried by resource can still contribute in a separate
name-scoped view.

The final activation re-derives a [complete
group](glossary.md#complete-group) through
the production interpreter and records `MigrationApplied` as an activated
authority boundary, except that registrar-token `unwrapped` groups carrying the
controller's ENSv1 registry cleanup currently stop at predecessor resolution
under [issue #822](https://github.com/ensdomains/bigname/issues/822) and do not
reach Project. Every normalized effect whose existence depends on the per-name
[migration correlation group](glossary.md#migration-correlation-group) carries
the completed group's visibility. The `migration_candidate_*_effects` tables
remain candidate-only diagnostic source records and are never Project input.
Refused and incomplete groups remain
candidate and are excluded from Project staging and product event/history reads.
An [independently admitted event](glossary.md#independently-admitted-event)
remains byte-for-byte activated
and only its diagnostic correlation association changes visibility. After its
[physical Interpret batch](glossary.md#batch-grid) commits in an ordinary walk,
the event is product-visible. An active or failed redo stays fenced from serving
until completion, even when an earlier redo batch committed. A batch-level failure
such as [issue #822](https://github.com/ensdomains/bigname/issues/822) rolls the
event back when it shares the failing batch, and later events remain unavailable
while Interpret cannot advance past the failing ENSv1→ENSv2 migration block. The shared production activation function performs the arm-scoped binding
transition already enforced by Interpret and enables the completed group's
dependent rows. Project consumes the validated
transition through its activated
`MigrationApplied` artifact without re-correlating raw ENSv1→ENSv2 migration
evidence. It
replaces that blanket refusal with the per-name exact-name rule. It
also activates non-boundary correlation groups; those groups never perform a
binding transition or change an authority epoch.

Slice 3B selects direct-subname ownership per child, replacing the previous
child recency tie-break rather than layering authority on top of it: recency now
orders only the current relation within the one selected arm. Parent reachability
first removes an ENSv1 relation below a parent on the `unwrapped`,
`unlocked_wrapped`, or `emancipated_child` path. A parent on the
`locked_wrapped` or `locked_child` path retains it only for a
[migratable child](glossary.md#migratable-child). Once that child migrates or
otherwise obtains a current ENSv2
registration, the published relation is the ENSv2 one and the retained ENSv1
relation is residue. A later release leaves the child unregistered on the ENSv2
side rather than restoring the retained ENSv1 relation. An ordinary Mainnet
pair whose two arms disagree with no authority proof to separate them is omitted
as unsupported
rather than resolved by event recency; it is neither an ambiguous product row nor
a publication failure. A proven Sepolia boundary, or a current
child registration in the admitted [migration registry](glossary.md#migration-registry-wrapperregistry) below a proven migrated
parent, follows the same per-name or per-child selection rule. Sepolia overlap
without either proof is expected because the runtime admits evidence from both
protocol eras without establishing per-name authority between them. It remains
unsupported under its retained contract reason. The
ENS root, `eth`, `reverse`, and `addr.reverse` are the four exact
[shared ENS infrastructure](glossary.md#shared-ens-infrastructure) names. They
select ENSv2 when the ENSv2 arm is current and ENSv1 evidence, current or
historical, exists, without fabricating a proof or extending the exception to
descendants. Historical ENSv2 evidence without a current binding does not
qualify. When that shared-infrastructure rule selects ENSv2, it overrides the
ordinary no-proof result, so those names carry neither Mainnet's
`conflicting_current_ens_authority` nor Sepolia's
`independent_ens_deployments_overlap`. Complete
direct-child groups now supply production input
to the activated-boundary branch; a refused or unmigrated child reaches ENSv2
authority only through a current positive ENSv2 registration.

The dual-current assertions run only for the Mainnet ENS
[deployment profile](glossary.md#deployment-profile),
after transaction- and block-level
reconciliation; a transient intra-transaction overlap is not a publication
failure. For the exact-name invariant, a dual-current result after the applicable
proven activated boundary
aborts before `publish::swap`, publishes no partial output, fails readiness for
that Project publication, and returns structured failure evidence. After the
Project transaction rolls back, the phase runner persists that evidence in the
append-only `project_generation_failures` diagnostic audit described in
[`storage.md`](storage.md#projection-publication). Slice 2E introduces that audit
path for the exact-name invariant; slice 3B reuses it for the child invariant,
whose condition is narrower. Because an ENSv1 relation can survive below an
unmigrated parent or a locked path, both arms stating a relation for one pair
can be expected residue rather than an anomaly. The child assertion runs after
parent reachability and fails a
[projection generation](glossary.md#projection-generation) with failure kind
`dual_current_child_authority` only when a surviving child on Mainnet with an
activated `migration_authority_transition` has an ENSv1 parent-child relation
asserted after that child's authority epoch started. A positive ENSv2 child
registration is permanent entry history in a locked parent's migration registry
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L293-L307 @ ens_v2@a971bd64), so parent reachability filters that ENSv1 relation before the
assertion even though the defensive integrity query recognizes that proof kind.
Relations filtered by an
unwrapped, unlocked-wrapped, or emancipated-child parent cannot trigger the
assertion. On Sepolia, a proven boundary selects and publishes ENSv2, while an
ordinary overlap without proof remains refused with
`independent_ens_deployments_overlap`; neither shape causes a dual-current
generation failure. Extending the publication guardrail to Sepolia is deferred
until [PR #852](https://github.com/ensdomains/bigname/pull/852), the #503 e2e
harness, drives an Interpret-activated boundary through Project;
[issue #851](https://github.com/ensdomains/bigname/issues/851) tracks re-applying
the guardrail.

## ENSv1→ENSv2 delivery slices

Registry-only ENSv1 and Basenames names with [getter-visible owner](glossary.md#getter-visible-owner) zero form one
cohesive read capability. Exact-name detail is supported and unregistered;
indexed records are supported when the retained [serving resource](glossary.md#serving-resource) has inventory;
verified and auto records follow the ordinary lookup capability; direct
subnames include a read-only row only while a current nonzero event-linked
resolver exists; and resolver `bound_names` remains subject to the resolver
family's existing binding-enumeration capability. Registration/control fields,
address-name relations, and owner-derived permissions stay absent. When the
latest nonzero registry resolver selection predates the [name surface](glossary.md#surface-name-surface), the event that first makes the surface active
links it to the retained serving resource without
requiring a repeated selection; a latest zero-address selection remains a
clear. The GraphQL compatibility surface uses the same serving resource for its
resolver record fields; it does not infer registration or control from that
read path.

For the fallback registry, a current-registry `NewOwner` or `Transfer` creates
the current record and ends any resolver pointer inherited from the old
registry. That [registry fallback handoff](glossary.md#registry-fallback-handoff) is retained across replay even for a same-owner
`Transfer`; linked zero-resolver events retract an inherited pointer that had
already become readable from every linked registry, registrar, or wrapper
resource. An old-registry `Transfer` does not affect a resolver
selected from the current registry.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L24 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L68 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L82 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L54 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L150-L172 @ ens_v1@91c966f)

Each slice includes its behavior tests and fixture provenance. Counts are
estimated hand-written production files; test fixtures, test-only harness
files, and docs are not included. Rows before “Final activation” describe the
contract at each historical delivery boundary; that final row supersedes their
statements that complete migration groups remain candidate-only.

| Slice | Coherent capability | Estimated production files |
| --- | --- | ---: |
| 1. Schema vocabulary, candidate ENSv1→ENSv2 intake, and replay with no product-visible change | Extend the closed schema-v2 event/derivation vocabulary through a reviewed in-place schema upgrade; admit fixed ENSv1→ENSv2 migration contracts; ratify [migration-registry](glossary.md#migration-registry-wrapperregistry) discovery; keep the independently admitted `registry_announcement` indexability edge ordinary and traversable by the watch plan while attaching candidate correlation provenance; interpret only controller-mediated second-level correlation-dependent identity, topology, role, registration, renewal, and normalized effects as candidate while leaving independently derivable existing-family output ordinary; exclude candidate groups and association/effect tables from Project staging and product event/history reads; defer every ENSv1→ENSv2 migration-driven `SurfaceBinding` transition; and add production provider-trusted Verify support plus declared-level guard fixtures for `ethereum-sepolia`. Child-migration derivation through a parent `WrapperRegistry` landed in slice 3A below as candidate-only output; publication of child authority landed in slice 3B below, while activating a child ENSv1→ENSv2 migration boundary remains deferred. Restart, full-replay, and live-follow fixtures prove later proxy facts remain retained without changing product behavior. | At least 22 (3 manifest TOML, up to 11 adapter/manifest Rust files, 2 schema contract/check files, at least 1 reviewed versioned schema-migration file, 1 phase-runner Verify module, and up to 4 Project/API/storage visibility modules) |
| 2A. Explicit migration authority transition and arm-scoped ordinary bindings | Add a required `authority_arm` to every binding and closure draft; scope ordinary close, predecessor, and successor behavior to chain, exact logical name, and arm; preserve coexisting ENSv1 and ENSv2 bindings; represent the exact-name cross-arm transition explicitly; and exercise its locked zero/one/multiple predecessor behavior through a code-only activated test seam. Production remains candidate-only and Project behavior does not change. | 3 production modules plus one reviewed schema-migration file |
| 2B. Graveyard, reservation, and renewal semantics | Classify Graveyard cleanup and production reservation seeding without reading cleanup registrations as user leases; establish the remaining renewal rules from deployment evidence. | To be scoped |
| 2C. Exact-name current authority | Consume a validated activated transition or positive ENSv2 child-registration proof to select one authority epoch, then publish every `name_current` field from only that epoch. Name detail exposes the selected exact-name result or the [deployment-profile](glossary.md#deployment-profile)-specific unsupported reason; candidate events remain inert. The resolver route's `bound_names` listing inherits this selection because it reads `name_current` directly — a name is listed only under its selected resolver, and rows classified `current_authority_not_projected` are omitted, per the resolver-route contract in [`api-v2-routes.md`](api-v2-routes.md). Batch lookup results carry the same selection in 2C: a name-keyed or reverse lookup result exposes the selected exact-name outcome or the minimal unsupported record shape, per the lookup contract in [`api-v2-routes.md`](api-v2-routes.md). | To be scoped |
| 2D. Authority fanout across product collections | Address-name membership and role summaries, name-filtered permission selection, search membership, primary-name forward verification, and address-derived product-history anchors all consume the exact-name authority slice 2C selects ([current-authority fanout](glossary.md#current-authority-fanout)); no collection performs an ENSv1-versus-ENSv2 ranking of its own. Explicit registration or resource reads remain audit views, and per-result exact-name classification in batch lookup stays 2C-owned. A collection that carries no row-local unsupported vocabulary omits a name whose exact-name authority is unsupported instead of inventing a row-local status. | 5 |
| 2E. Post-rollback generation-failure audit | Enforce the reconciled dual-current invariant on Mainnet and persist the rolled-back generation failure in a separate append-only diagnostic transaction. The Sepolia extension is deferred until the connected Interpret→Project path is proven. | To be scoped |
| 3A. Direct-child correlation | Derive the deferred child-migration shapes that reach no migration controller, where the already-migrated parent's own [migration registry](glossary.md#migration-registry-wrapperregistry) registers the child into itself through the self-call that definition cites; admit the registry a locked child receives from its parent registry so admitted depth is unbounded; derive the child's ENSv1 predecessor from the parent registry's own migration evidence and the registered labelhash rather than inheriting the `.eth` second-level rule, under the separate `wrapper_backed_child_control` anchor defined at [child migration boundary](glossary.md#child-migration-boundary), selected against the child's ENSv1 cleanup rather than the registration; admit both cleanup shapes that definition cites — the `locked_child` path, whose wrapper token is parked in the Graveyard, and the `emancipated_child` path, whose node is unwrapped into it — each only with that ENSv1 predecessor cleanup present, earlier in the registration's own transaction; and reject the clobbered registration, the unmigrated child, factory-only evidence, incomplete parent discovery, and any self-claim lacking ENSv1 predecessor cleanup as non-boundaries, `MigrationHelper` participation being unobservable for the reason cited there and so never a correlation key at all. Correlation reuses `authority_transition`; every child boundary and effect is candidate-only, so no child state, projection, or product row changes — though an admitted child registry does widen Project's delete-and-rebuild scope — and activating a child transition remains an explicit refusal until slice 3B. | 4 |
| 3B. Children publication invariant | Stage the parent-child relation each authority arm states, first filtering ENSv1 relations by the parent's activated ENSv1→ENSv2 migration path: unwrapped, unlocked-wrapped, and emancipated-child parents retain none, while locked-wrapped and locked-child parents retain only [migratable children](glossary.md#migratable-child). Then publish the arm the child's own staged authority selects, so recency orders only within that arm; a released ENSv2 child publishes nothing and does not fall back, and a pair whose surviving arms disagree with no authority proof is omitted as unsupported rather than ranked. On Mainnet, the ordered child assertion fails an ENS [projection generation](glossary.md#projection-generation) with `dual_current_child_authority` only when a post-epoch ENSv1 relation survives that parent filter; positive registration in a locked parent's migration registry is itself disqualifying entry history, so it is filtered before the assertion. Sepolia publishes the proof-selected relation; its guardrail extension is deferred until the connected Interpret→Project path is proven. | 4 (children projection builder, Project integrity assertion, child transition writer, redo reopen) plus one reviewed schema-migration file for the failure-kind vocabulary |
| Final activation. Production [complete groups](glossary.md#complete-group) | Run the already-proven activation function after all batch correlation paths finish; activate the authority paths that pass predecessor resolution and complete non-boundary normalized rows while retaining candidate-only diagnostic effect records; keep registrar-token `unwrapped` groups with the controller's ENSv1 registry cleanup blocked on issue #822 until their production writer path is repaired; preserve named refusals, ordinary events, exact predecessor selection, and Sepolia's refusal of ordinary no-proof overlap; rotate the [interpreter content hash](glossary.md#interpreter-content-hash) and require the full Interpret→Project walk before publication. Coverage is enumerated in [`migration-activation-coverage.md`](migration-activation-coverage.md). | 4 adapter production files, one of which deletes the superseded helper; no schema, manifest, API, or Project vocabulary change |

Issues [#348](https://github.com/ensdomains/bigname/issues/348) and
[#529](https://github.com/ensdomains/bigname/issues/529) ship together at one
[interpreter content hash](glossary.md#interpreter-content-hash)
[re-derivation boundary](glossary.md#re-derivation-boundary), before the
combined slice/PR-#391 boundary. Their allowed product delta arises from late
ENSv2 resolver `RecordChanged` and `RecordVersionChanged` rows for a retained
canonical [name surface](glossary.md#surface-name-surface): `event_identity`
stays fixed, `logical_name_id` becomes non-null, `resource_id` stays null,
`raw_fact_ref.interpreter_state_key`
changes with the attribution, and `before_state` may rethread onto the
logical-name/resource-null state stream. Issue #348 retains the surface from
registry/root evidence; issue #529 retains a surface observed only by resolver
`AliasChanged` before a batch boundary. Those rows may newly enter
name-filtered diagnostics and product history. This boundary does not claim
fresh/resumed parity for the known pre-existing exception: when a
resolver-emitted resource equals `namehash(N)`, named-resource and alias
preimages can share one retained [interpreter state
key](glossary.md#interpreter-state-key), so resumed interpretation can lose the
named-resource resolver hint and diverge from a fresh walk
([#560](https://github.com/ensdomains/bigname/issues/560); evidence is checked
in as an ignored collision probe). An ended resource whose latest
retained `ResolverChanged` pointer names the emitting resolver may also receive
a different rebuildable `record_inventory_current` row; the released or
expired name must still have no current binding or resource, and its name and
record reads must not expose that inventory. The boundary invalidates the
continuation contract for outstanding collection cursors; consumers restart
from the first page. Acceptance verifies the declared normalized-event and
inventory-row deltas, proves the ended name remains unbound with no served
record inventory, verifies both surface-retention triggers, and verifies fresh
complete pages, fields, membership, and
cursor continuation under the new publication. Any other product difference
blocks that publication. The later combined-boundary gate uses the
post-#348/#529 publication as its behavior-preserving baseline.

Slices 1, 2A, 2B, and 2C are separately reviewed and separately merged implementation
PRs, but deploy together at the same planned [re-derivation
boundary](glossary.md#re-derivation-boundary), which also carries
[PR #391](https://github.com/ensdomains/bigname/pull/391). The boundary
uses one [interpreter content hash](glossary.md#interpreter-content-hash), one
full source re-walk, and one Project
publication decision for `ethereum-sepolia`. Other chains
retain independent publication decisions. There is no production
interval serving candidate-only data on the `ethereum-sepolia` ENSv1→ENSv2
target.
Candidate-versus-activated state remains a replay/test-surface distinction, and
the acceptance comparison below runs in the test environment against the
boundary fixture corpus. The ordinary registry-announcement edge remains a
watch-plan input, so this one-boundary plan has no ingest hole.

Slice 1 has a mandatory full-re-walk acceptance comparison against the
pre-admission Project publication at a fixed readable chain head. This comparison
isolates slice 1: its control and candidate test runs hold every other
shared-boundary input constant, including PR #391's topology serializer. It is
not a comparison between the actual pre-boundary production publication and the
activated Project publication deployed after the shared boundary. It proves identical
product-visible row membership
and every DTO field for `name_current`, `children_current`,
`address_names_current`, both permission projections and `/v2/permissions`,
resolver and record reads, primary-name and search reads, `/v2/events`, and
name- and address-history reads, plus every GraphQL compatibility operation.
The comparison covers ordered pages, page membership, every REST and Manager
DTO field, summary/count fields, `has_more`, and point responses. Before the
test-only slice-1 re-walk, each normalized-event-backed cursor surface reads a
page and saves its `next_cursor`. After the full Interpret and Project re-walk
publishes, that pre-rewalk cursor is submitted to the post-rewalk test
publication.
For `/v2/events`, name history, address history, and every other product cursor
surface backed by normalized-event row identity, it must resume from the same
normalized-event keyset anchor with identical remaining product rows, pages,
fields, `has_more`, and summary behavior. The anchor may be an unmapped event
absent from the product response, so the corpus interleaves an unmapped event at
a product-page boundary and proves that no visible row is skipped or duplicated.
`/v2/diagnostics/events` must accept its old cursor and continue from the same
stable normalized-event anchor, but its remaining rows and fields may include
the expected new candidate diagnostics. A pre-existing diagnostic row's numeric
`normalized_event_id` may change, while its `event_identity` and pre-existing
semantic fields remain stable apart from those allowed candidate additions.
Fresh post-rewalk cursors are tested
separately on every covered route and must continue normally.
Implementations may preserve numeric `normalized_event_id` values or resolve an
old token through stable `event_identity` plus its stored sort tuple; freshly
issued cursor bytes may differ. Raw facts, candidate
normalized events,
diagnostic event associations and identity/discovery effects, manifest
metadata, internal provenance, cursor-embedded row identities, and content
hashes are expected to change; product behavior is not.
The comparison runs over the complete planned re-walk, so a unit fixture that
filters only `ens_v2_migration_l1` cannot satisfy this gate.

A separate shared-boundary integration gate exercises the final combined
slice-1, slice-2A, slice-2B, slice-2C, and PR-#391 artifact. It inspects the actual widened watch
plan, performs the boundary's mandatory historical fetch and full re-walk, and
compares the published DTOs, pages, summaries, and cursor continuation with the
pre-boundary Project publication. PR #391's exact allowed wire delta is that an
existing Basenames `transport.contract_address` becomes lowercase and remains
`0x`-prefixed; no other PR-#391 product DTO field may change. The other allowed
differences are the slice-2C authority and event/history
activation contracted here, and the planned diagnostic, manifest, provenance,
and content-hash changes. Product cursors issued before this combined boundary
must remain valid, although their non-snapshot remaining rows and fields may
reflect those explicit activated deltas. Any other difference or cursor
rejection blocks the `ethereum-sepolia` publication decision.

The production Verify phase validates `ethereum-sepolia`'s durable ingested extent through its finalized marker. A distinct [verification-only](glossary.md#source-role) dRPC earns `cross_checked`; without one, the target-covering intake cursor earns `quick_synced` and is never selected as its own reference. The persisted
intake cursor must match its key, kind, seed basis, and start block and cover that
finalized marker. Project publishes before Verify in the
pipeline sequence, but that publication remains unready and traffic-drained
until Verify succeeds for the target. Acceptance fixtures prove
that distinct intake and verification-only endpoints produce `cross_checked`,
that the verification-only source receives no Ingest/Live requests or cursor,
and that equal intake/reference endpoints fail before cursor initialization, provider construction or access, raw-fact writes, or [redo-marker](glossary.md#redo-marker-scope) publication.
Base fixtures must keep Coinbase and dRPC intake-capable, use only an optional
distinct verification-only dRPC for `cross_checked` through the ingest seam,
and otherwise fall back to the target-covering intake dRPC's `quick_synced`.
Ethereum Mainnet fixtures must likewise reserve `node_checked` for a distinct
verification-only reth and fall back to its intake reth's `quick_synced`; a
`both` source can never earn either independent level. Normal and
manifest-widening Ingest redo must receive every intake-capable source and no
verification-only source, while role-aware Verify and `all` redo must enforce
the complete intake-capable key set before publication or provider access.
Standalone Interpret and Project redo require the complete intake-capable source descriptor set so persisted cursor identities can prove the range, but they perform no ingest-provider I/O. In `all` redo, Interpret receives only intake-capable descriptors after Ingest enforces the complete cursor-key set.
An `--phase all` fixture must prove its Ingest and Interpret contexts receive
only the complete intake-capable set while its Verify context still receives
the optional verification-only reference; the reference must not leak into
intake or cause Interpret cursor rejection.
Endpoint-conflict and provider-construction errors must name source keys without exposing either resolved endpoint. A retained pre-#411 fixture must also prove that a role
change which alters the intake-capable cursor-key set is rejected while raw
facts remain, until the owner-approved reset and full re-walk occur.
Five-field descriptors must remain accepted for normal run and redo, with the
omitted role defaulting to `both`. Each known level—`quick_synced`,
`cross_checked`, and `node_checked`—must satisfy the API's `quick_synced`
readiness floor, while an unknown stored level must fail closed. Completed
pre-part-2 `cross_checked` or `node_checked` evidence must be revalidated and
downgraded to `quick_synced` when the current role configuration cannot earn the
stronger level; it must neither halt revalidation nor preserve the stale claim,
and it must not call a reference provider. Adding an independent source must
not automatically upgrade a completed `quick_synced` extent; only the required
from-zero walk or an explicit full-extent Verify redo may establish the stronger
level. For every accepted production chain, persistence and completed-state
validation must derive the maximum reportable level from the same role-aware
verification plan, without a second chain allowlist or fail-open fallback. The fixtures
will continue to reject a stale or mismatched
intake cursor and prevent Live before Verify succeeds.
Omitting, disabling, or replacing Verify with a no-op is not an acceptable
readiness gate.

The boundary fixture places a migration-created `RegistryCreated` at block N,
restarts Ingest and Interpret, and then emits a registry, role, registration,
renewal, or topology fact from that proxy in a later transaction or block. Full
historical replay and live-follow variants both prove the ordinary announcement
admission keeps the proxy watched, the later raw fact is retained, its
correlation-dependent augmentation is interpreted as candidate, and any output
independently derivable under `ens_v2_registry_l1` remains ordinary and matches
the control test run. The fixture also asserts that the persisted edge appears
in the generated watch plan after restart before either the retained-raw-log
announcement preload or the same-window announcement query adds the proxy, so
neither intake path can mask a missing edge. The product comparison above
remains unchanged. A
same-transaction ordering test alone cannot satisfy slice 1.

Catalog-derived slice-1 fixtures preserve each decoded expiry instead of
reconstructing a fixed premigration delta. They also require ENSv1 registry
resolver- or TTL-clear events only when the cleared value actually changed:
`setRecord` delegates to a helper that compares both stored values before it
emits either event. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L39 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L40 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L179 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L181 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L184 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L186 @ ens_v1@91c966f)

Slice 1 still fits the approximate production-file budget, but its inertness
contract requires narrow Project staging and API/storage history-selection
plumbing in addition to adapters, manifests, and fixtures. It also requires a
reviewed in-place schema-migration: `MigrationApplied`,
`ContractDiscovered`, `ens_v2_migration`, and the correlation-scoped visibility
provenance are outside the current closed schema-v2 contract. That requirement
is the stop condition for implementation in this change. An empty-schema
replacement is not an alternative for this boundary because it cannot preserve
outstanding cursors whose event identities currently include sequence-assigned
manifest IDs. Slice 1 must also add the reviewed `ethereum-sepolia` production
Verify path and readiness fixtures; the target cannot become ready or serve
traffic by omitting or bypassing that phase. Slices 1 and 2 remain
separately reviewed capabilities but share the deployment boundary above; slice
3 remains a later consumer capability.

### GraphQL compatibility

`POST /graphql` exposes a subgraph-shaped `Query` root over the schema-v2
current projections. The existing `domain`, `domains`,
`registrationConnection`, and `domainConnection` operations remain available;
`Domain.normalizedName`, `Domain.tokenId`, and both connection count operations
are bigname additions to the subgraph-shaped surface. The generated-style
Domain roots have these signatures:

```graphql
domain(
  id: ID!
  block: Block_height
  subgraphError: _SubgraphErrorPolicy_! = deny
): Domain

domains(
  skip: Int = 0
  first: Int = 100
  orderBy: Domain_orderBy
  orderDirection: OrderDirection
  where: Domain_filter
  block: Block_height
  subgraphError: _SubgraphErrorPolicy_! = deny
): [Domain!]!
```

The generated-style Account and Resolver roots have these signatures:

```graphql
account(
  id: ID!
  block: Block_height
  subgraphError: _SubgraphErrorPolicy_! = deny
): Account

accounts(
  skip: Int = 0
  first: Int = 100
  orderBy: Account_orderBy
  orderDirection: OrderDirection
  where: Account_filter
  block: Block_height
  subgraphError: _SubgraphErrorPolicy_! = deny
): [Account!]!

resolver(
  id: ID!
  block: Block_height
  subgraphError: _SubgraphErrorPolicy_! = deny
): Resolver

resolvers(
  skip: Int = 0
  first: Int = 100
  orderBy: Resolver_orderBy
  orderDirection: OrderDirection
  where: Resolver_filter
  block: Block_height
  subgraphError: _SubgraphErrorPolicy_! = deny
): [Resolver!]!
```

`Account.id: ID!` is the lowercase address. `Account_filter` accepts exactly
`id: ID` and `id_in: [ID!]`; `Account_orderBy` accepts only `id`. The entity
source is the distinct set of current registrant, token-holder, and effective-
controller addresses in `address_names_current`. An address with no current
membership relation is absent, including a current address-record target that
is not also a registrant, token holder, or effective controller. `#670/T4` owns
historical Account persistence and `#670/T5` owns serving resolved-address
Account membership. A former owner or registrant is therefore absent even
though the ENS subgraph retains the Account created by an earlier ownership
event; `#670/T4` owns that persistence
gap (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L89-L92 @ ens_subgraph@723f1b6)
(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L146-L151 @ ens_subgraph@723f1b6)
(upstream: .refs/ens_subgraph/src/ethRegistrar.ts:L43-L56 @ ens_subgraph@723f1b6)
(upstream: .refs/ens_subgraph/src/ethRegistrar.ts:L163-L174 @ ens_subgraph@723f1b6).
Account reverse fields remain deferred to `#670/T4`.
`Domain.owner.id` can also serve the zero address while `account(id:)` returns
null because the current address-name projection drops the zero address; this
is the same zero-owner residual owned by `#670/T5` as the Domain owner filter.

`Resolver.id: ID!` is
`<lowercase-resolver-address>-<lowercase-domain-namehash>`, and
`Resolver.address: Bytes!` continues to serialize as the same lowercase
hexadecimal JSON string. `Resolver_filter` accepts exactly `id: ID`,
`address: Bytes`, and `domain: String`; `Resolver_orderBy` accepts only `id`.
One entity comes from each current resolver-address/namehash binding in
`name_current`, so two names using one resolver address remain distinct. For
rows produced by the normal projection builder, nested `Domain.resolver` and
the Resolver roots form the same identity. The roots additionally reject zero-
address and off-snapshot-chain bindings if out-of-builder data contains them.
Changing a name's resolver removes the prior composite ID from local reads,
although the ENS subgraph retains the prior Resolver entity; current Resolver
materialization remains `#670/T6` work and event history remains `#670/T9`
(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L167-L201 @ ens_subgraph@723f1b6)
(upstream: .refs/ens_subgraph/src/resolver.ts:L233-L248 @ ens_subgraph@723f1b6).
`Resolver.addr`, `Resolver.coinTypes`, `Resolver.events`, and `Resolver.domain`
remain deferred to `#670/T6`. `Resolver.contentHash` remains the existing local
`String` divergence under its existing `#670/T2` disposition rather than
changing to upstream `Bytes` in this slice.

Account point IDs and generated Domain-filter IDs are compared exactly as
supplied. Uppercase `0X`, or uppercase hexadecimal digits after lowercase `0x`,
are valid GraphQL `ID` text but do not alias the lowercase served value. The
Domain point path differs: `domain(id:)` accepts uppercase hexadecimal digits
after lowercase `0x`, normalizes that namehash, and can match the lowercase
served ID. Uppercase `0X` does not alias a namehash ID, but, like every other
noncanonical point input, it can match a literal ENS name through the local
name extension. Graph Node applies point IDs as exact string equality (upstream:
.refs/graph_node/graphql/src/store/prefetch.rs:L726-L730 @ graph_node@aefe1737).
A Resolver point ID must use the exact lowercase `<address>-<namehash>` composite
form; a case-different form is a no-match. The pre-existing
`Resolver_filter.id` path remains hexadecimal-digit-canonicalizing when both
components retain lowercase `0x`, so that filter can match a composite ID whose
hexadecimal digits differ only by case. Uppercase `0X` remains a no-match.
`Resolver_filter.address` uses `Bytes`: lowercase `0x` followed by
uppercase or lowercase hexadecimal digits is accepted and byte-canonicalized,
while uppercase `0X` is a scalar-coercion error. Graph Node's Bytes parser strips
only lowercase `0x` (upstream:
.refs/graph_node/graph/src/data/store/scalar/bytes.rs:L47-L53 @ graph_node@aefe173).

All Account and Resolver filter members combine with logical AND, and an empty
`Account_filter.id_in` matches no rows. Filtering, distinctness, ordering, and
paging execute in PostgreSQL. Omitted ordering is `id` ascending. Account IDs
use lexical lowercase-address order. Resolver SQL orders by the exact indexed
lowercase-address expression and then `logical_name_id`. The interpreter mints
that key as `namespace:namehash`, so within the filtered namespace this is
identical to address/namehash composite-ID order. Admitted addresses and
namehashes are fixed-width lowercase hexadecimal text. The CI/test and default
Docker PostgreSQL image is Alpine/musl, whose libc collations are bytewise, so
it satisfies the hexadecimal-collation contract by construction rather than
proving glibc behavior. The integration test records the server provider and
compares text order with UTF-8 byte order; its ignored glibc probe has an
explicit run recipe. Issue `#833` tracks a glibc CI gate. An explicit-collation
index would be schema work outside this API-only change.
Pagination uses the Domain bounds: omitted `first`
is `100`, positive `first` is capped at `200`, non-positive `first` returns an
empty list after head validation, negative `skip` becomes zero, and positive
`skip` is capped at `1_000_000`.

The four roots inherit the Domain roots' current-snapshot-only block contract.
Absent blocks and constraints satisfied by the served head are accepted;
historical execution is not implemented. A singular refusal leaves its nullable
field null with a path-local error, while a plural refusal propagates the error
through its non-null root and makes `data` null.
Absent-request-head behavior remains unchanged and is owned by `#743`.

The composite Resolver ID is an intended wire change for every existing
`Domain.resolver { id }` selection. The [locally committed Manager fixture](../apps/api/src/tests/fixtures/manager-graphql/README.md),
copied from Manager commit `759860f5acc62ea287b0feefa23c0d17aeb862a9`, selects none of the four new
roots, but its Resolver fragment selects both `id` and `address`. The Manager
Domain response fixtures therefore receive the composite ID while the address
JSON remains unchanged.

`Domain.id` is `ID!` and remains a JSON string. `domain(id:)` also retains a
local runtime extension that accepts an ENS name string. A canonical `0x` plus
64-hex input attempts namehash lookup before the name fallback, preventing a
hash-shaped ENS name from shadowing an entity ID; every other input takes the
direct name lookup path. `Domain_filter.id` and `Domain_filter.id_in` match
namehashes only.

`Domain_filter` retains `owner` and `owner_in` and serves the complete generated
ID family: `id`, `id_not`, `id_gt`, `id_gte`, `id_lt`, `id_lte`, `id_in`, and
`id_not_in`. It also serves `name`, `name_not`, `name_gt`, `name_gte`, `name_lt`,
`name_lte`, `name_in`, `name_not_in`, and the `contains`, `starts_with`, and
`ends_with` positive and negative forms, each with a distinct `_nocase` sibling.
Graph Node generates these ID and String operator families (upstream:
.refs/graph_node/graph/src/schema/api.rs:L872-L912 @ graph_node@aefe173).
These members match the upstream `Domain.id: ID!` and nullable `Domain.name:
String` fields (upstream: .refs/ens_subgraph/schema.graphql:L1-L7 @
ens_subgraph@723f1b6). Every supplied member contributes an AND predicate.
Empty `in` and `not_in` lists both match no rows, as Graph Node emits SQL
`false` for either empty membership list (upstream:
.refs/graph_node/store/postgres/src/relational_queries.rs:L1716-L1725 @
graph_node@aefe173).

Generated ID operators compare the supplied text directly with the served
namehash text. Because namehashes are fixed-width lowercase hexadecimal text,
canonical predicates and their order/tie-break expressions deliberately omit
`COLLATE "C"` so PostgreSQL can use `name_current_lookup_idx`; this follows the
existing indexed Resolver-order precedent above and the hexadecimal-collation
[deployment contract](storage.md). A noncanonical ID range keeps
expression-local C comparison and requires a linear scan. Otherwise, `COLLATE "C"` is
used only where collation changes semantics: generated `raw_name` comparisons
and the name order key. Generated name equality, range, membership, and pattern operators
compare the supplied text directly with the nullable served `Domain.name`; they
do not perform ENSIP-15 normalization or convert equality to namehash lookup.
Case-sensitive pattern members use `LIKE`/`NOT LIKE`; nocase
members use `ILIKE`/`NOT ILIKE`. Contains surrounds a value with `%` unless the
value already starts or ends with `%`; starts-with appends `%`; ends-with
prepends `%`. `%`, `_`, and backslash remain SQL pattern characters and are not
escaped. Graph Node uses that contains construction and does not escape those
characters (upstream:
.refs/graph_node/store/postgres/src/relational_queries.rs:L1432-L1476 @
graph_node@aefe173); it builds starts/ends patterns separately and selects the
case-sensitive or nocase SQL operator per member (upstream:
.refs/graph_node/store/postgres/src/relational_queries.rs:L1376-L1410 @
graph_node@aefe173) (upstream:
.refs/graph_node/store/postgres/src/relational_queries.rs:L1533-L1555 @
graph_node@aefe173).
Explicit `null` on `id`, `id_not`, `name`, or `name_not` emits `IS NULL` or `IS
NOT NULL`; explicit `null` on a range, list, or pattern member is rejected
instead of becoming an omitted predicate. This follows Graph Node's distinct
null equality path (upstream: .refs/graph_node/graphql/src/store/query.rs:L270-L334 @
graph_node@aefe173) (upstream:
.refs/graph_node/graphql/src/store/query.rs:L416-L435 @ graph_node@aefe173)
(upstream:
.refs/graph_node/store/postgres/src/relational_queries.rs:L860-L890 @
graph_node@aefe173) (upstream:
.refs/graph_node/store/postgres/src/relational_queries.rs:L1603-L1617 @
graph_node@aefe173) (upstream:
.refs/graph_node/store/postgres/src/relational_queries.rs:L1696-L1710 @
graph_node@aefe173).

`owner` and `owner_in` match the projected effective controller.
The effective controller agrees with `Domain.owner` when the latest projected
registry-ownership event is an owner-bearing `AuthorityTransferred` to a non-zero
address on a non-wrapper-authority name and no later resource-scoped
`PermissionChanged` event exists on the selected resource. Task `#670/T5` owns
these remaining disagreement classes:

- Wrapper-authority names serve the wrapper token holder through the registrant
  fallback, while the effective controller is a registry-controller fold for
  eligible wrapped or emancipated names and is absent for locked or in-grace names.
- A zero registry owner is served as the zero address; a masked owner word is
  served as the registrant fallback, or the zero address when there is none. In
  both cases the address-name projection drops the row, so the filter selects
  nothing.
- Without a projected registry-ownership event, the effective controller can fall
  back to a token holder or be absent while the served owner falls back to a
  registrant or the zero address.
- Resource-scoped `PermissionChanged` events granting or revoking
  `resource_control`, or an owner-less `AuthorityEpochChanged` emitted on release.
  A resource-scoped `PermissionChanged` granting `resource_control` makes its
  grantee the effective controller, while one revoking the current controller's
  `resource_control` clears the current controller. The effective controller is
  then absent when the name has no registrar [token lineage](glossary.md#token-lineage),
  or falls back to the token holder or registrant otherwise; served control
  ownership is unchanged by either `PermissionChanged`. A release can also
  co-emit an owner-less `AuthorityEpochChanged`, which clears the served registry
  owner so `Domain.owner` falls back to the registrant or zero address, while the
  epoch event is excluded from the effective-controller fold and the release's
  `resource_control` grant keeps the registry owner there.

`DomainFilter` remains the separate input for the local `domainConnection`
operation. Its `name` continues the existing ENS name lookup, and its
`name_contains` continues ENSIP-15 normalization plus escaped-pattern behavior.
In particular, `isMigrated` remains on `DomainFilter` and is not accepted by
`Domain_filter`; task `#670/T10` remains outside this slice and is subject to the
Manager constraint below. Owner operators beyond `owner`/`owner_in`, date and
Resolver-ID filters, relation filters, `and`, `or`, and every other unclaimed
upstream member remain absent and produce the existing unknown-input-field
validation error.

When `orderBy` is omitted, `domains()` now orders by namehash ascending without
an explicit collation; this is a behavior change from the previous name
ordering. `Domain_orderBy.id` uses that same namehash ordering. The served
upstream order values are `id`, `name`, `createdAt`, `expiryDate`,
`owner`, `owner__id`, and `resolver`, corresponding to the upstream Domain
fields and Account/Resolver relations (upstream:
.refs/ens_subgraph/schema.graphql:L1-L40 @ ens_subgraph@723f1b6). Graph Node's
generator defines the upstream ordering vocabulary (upstream:
.refs/graph_node/graph/src/schema/api.rs:L506-L545 @ graph_node@aefe173).
`owner` and `owner__id` order by the exact non-null effective owner served by
`Domain.owner`; `resolver` orders by the nullable composite Resolver ID served
by `Domain.resolver`, not by address alone. Raw-name and other semantic text
keys use `COLLATE "C"`; the fixed-width namehash tie-breaker does not. That
same-direction namehash tie-break is a local deterministic refinement,
including for `owner__id`: Graph Node's child-ID path adds no parent-ID
tie-break. For a mutable entity such as `Domain`, its default configuration
instead appends the internal block-range column after the child ID (upstream:
.refs/ens_subgraph/schema.graphql:L1 @ ens_subgraph@723f1b6) (upstream:
.refs/graph_node/store/postgres/src/relational_queries.rs:L3838-L3842 @
graph_node@aefe1737) (upstream:
.refs/graph_node/store/postgres/src/relational_queries.rs:L4046-L4052 @
graph_node@aefe1737) (upstream:
.refs/graph_node/graph/src/env/store.rs:L299-L300 @ graph_node@aefe1737). The local
`registrationDate` extension remains; every other upstream order value is
assigned an exact upstream-only disposition and is absent from the local enum.
The default and explicit `id` order use `name_current_lookup_idx` without a
sort. ID equality and membership use a B-tree scan with every order. Equality
yields at most one candidate; membership is bounded only by the operand count,
which the client controls and the API does not cap. With a non-ID order, each
member may therefore cause one correlated probe, and a very large list can
cross PostgreSQL's JIT cost threshold. On the 5,004-row review fixture,
`id_in` lists of 10, 1,000, and 5,000 values produced 10, 1,000, and 5,000
probes at estimated costs 519, 46,682, and 227,149 respectively; the 5,000-value
case enabled JIT. With ID ordering, `LIMIT` bounded the same 5,000-value list to
200 probes at cost 18,456 without JIT. An explicitly empty `id_in` still selects
the correlated query shape, but its `AND FALSE` predicate becomes a cost-0.02
one-time filter and performs no probes. Canonical ranges use the B-tree scan
only with ID ordering, where `LIMIT` bounds the returned rows near the requested
page size plus skipped rows. A range with a non-ID order instead keeps the
ordinary joins even when the range is selective, because its operand alone
cannot bound the number of matching rows. ID negations, noncanonical ranges,
every name operator, and the other served order values therefore have cost
linear in the eligible names table, although filtering and sorting still occur
below `LIMIT`.
Graph Node creates an attribute index for every eligible entity column and uses
a B-tree for ordinary scalar attributes (upstream:
.refs/graph_node/store/postgres/src/relational/ddl.rs:L251-L275 @
graph_node@aefe173) (upstream:
.refs/graph_node/store/postgres/src/relational/ddl.rs:L277-L342 @
graph_node@aefe173). Bigname lacks equivalent indexes for raw-name
equality/range/prefix and name ordering and for the date, owner, and Resolver
sort expressions. Issue `#831` owns that names-projection indexing follow-up;
adding the indexes is a separate schema-migration slice. Substring,
suffix, nocase, and negated patterns still require scans under the upstream
operator/index combination as well.
`createdAt` still serves epoch zero when no projected timestamp exists; its SQL
`COALESCE(..., TO_TIMESTAMP(0))` makes those rows sort as numeric value `0`,
agreeing with that existing served fallback.
Omitted pagination starts at offset zero and returns the first 100 rows.
Non-positive `first` returns an empty page, positive `first` is capped at
`200`, negative `skip` becomes zero, and positive `skip` is capped at
`1_000_000`.

`BlockChangedFilter` and each `_change_block` input remain exact upstream-only.
Graph Node adds that input and member to generated filters (upstream:
.refs/graph_node/graph/src/schema/api.rs:L1202-L1209 @ graph_node@aefe173)
(upstream: .refs/graph_node/graph/src/schema/meta.graphql:L55-L57 @
graph_node@aefe173).
The current projection has no entity last-change block, so the API does not
substitute a block from `chain_positions`, the selected served head,
`manifest_version`, `last_recomputed_at`, or a resolver inventory boundary.
The existing `block: Block_height` argument continues to validate an eligible
current snapshot and does not execute a historical read.

Generated Domain filtering and ordering execute in PostgreSQL inside the
filtered CTE before `ORDER BY`, `LIMIT`, and `OFFSET`. Acceptance requires at
least 5,000 eligible rows spanning at least two effective owners and two
resolver addresses, with fixed reviewed EXPLAIN bounds for ID equality,
membership, canonical ranges, and the ID order. Operators and orders that scan
the eligible names table instead require their predicate below `LIMIT`, a match
near the end of the 5,004-row fixture, and bounded sort memory; fixture-sized
row, loop, and buffer ceilings are not treated as scalability proof.
PostgreSQL uses the correlated eligibility subquery for ID order, or for ID
equality or membership with any order. A range combined with equality or
membership stays bounded by that same candidate set. A range without equality
or membership uses ordinary joins under a non-ID order so each relation is
scanned once rather than probed once per matching name. The test prints that
selective EXPLAIN as `DOMAIN SELECTIVE ID_GT NAME PLAN`. A round-5
`--nocapture` run on the 5,004-row `postgres:16-alpine` review fixture printed
the accepted selective `id_gt` plus name-order flat plan at cost 537.01 and
13.319 ms, with one `name_surfaces` loop and no JIT, instead of five correlated
probes. The same rule prevents an unselective range from making 5,004 probes
and triggering JIT compilation.
This API-only slice adds no index or database locale startup gate.

The affected Manager operation set therefore requires two declaration edits:
`$id: String!` becomes `$id: ID!`, and the `Domains.graphql` declaration
`$where: DomainFilter!` becomes `$where: Domain_filter!`. A Manager runtime
`where` value containing `isMigrated` fails GraphQL input coercion with an
explicit unknown-member error; the compatibility layer does not silently
discard it. The `MigratedNamesCount.graphql` operation continues to call
`domainConnection` with `DomainFilter`.

The reviewed [GraphQL compatibility oracle](graphql-compatibility-oracle.md)
claims the generated-style Domain root signatures and the implemented partial
filter surface alongside its response cases.
Its captured SDL and semantic index form a [GraphQL upstream
census](glossary.md#graphql-upstream-census), not a claim of complete entity
coverage. Directive repeatability is excluded at the documented [schema-comparison
boundary](graphql-compatibility-oracle.md#schema-comparison).
The `skip` and `first` defaults follow Graph Node's generated collection
arguments (upstream: .refs/graph_node/graph/src/schema/api.rs:L667-L679 @
graph_node@aefe1737).
`Domain_orderBy.registrationDate` is an intentional bigname extension retained from bigname's earlier GraphQL schema:
upstream
has no direct `Domain.registrationDate` field and instead exposes the nested
`registration__registrationDate` order value through `Domain.registration`; the date itself belongs to
`Registration.registrationDate` (upstream: .refs/ens_subgraph/schema.graphql:L1-L46 @ ens_subgraph@723f1b6)
(upstream: .refs/ens_subgraph/schema.graphql:L184-L190 @ ens_subgraph@723f1b6), and Graph Node generates child order
values as `<parent>__<child>` (upstream: .refs/graph_node/graph/src/schema/api.rs:L531-L603 @ graph_node@aefe1737).

The schema includes graph-node-compatible `BigInt` and `Bytes` scalars,
`Block_height`, `_SubgraphErrorPolicy_`, and `_meta`/`_Meta_`/`_Block_` shapes.
`BigInt` is a decimal string of arbitrary width and `Bytes` is an even-length,
`0x`-prefixed hexadecimal string. The reference implementation serializes
these two values as a decimal string and prefixed hex bytes, respectively
(upstream: .refs/graph_node/graph/src/data/store/scalar/bigint.rs:L297-L318 @ graph_node@aefe1737)
(upstream: .refs/graph_node/graph/src/data/store/scalar/bytes.rs:L16-L18 @ graph_node@aefe1737)
(upstream: .refs/graph_node/graph/src/data/store/scalar/bytes.rs:L41-L51 @ graph_node@aefe1737).
`Domain.createdAt` and `Domain.expiryDate` use `BigInt`; the existing
[ENSv2 max-expiry projection narrowing](upstream.md#known-divergences) remains
in force, so an unrepresentable max expiry is still `null` rather than a
fabricated decimal value. The comparison shapes follow graph-node's generated
schema conventions
(upstream: .refs/graph_node/graph/src/schema/meta.graphql:L27-L28 @ graph_node@aefe1737)
(upstream: .refs/graph_node/graph/src/schema/meta.graphql:L41-L52 @ graph_node@aefe1737)
(upstream: .refs/graph_node/graph/src/schema/meta.graphql:L59-L81 @ graph_node@aefe1737).
`BigDecimal` is not part of this compatibility surface.

Entity operations accept `block: Block_height` and
`subgraphError: _SubgraphErrorPolicy_! = deny`. The endpoint resolves an omitted block,
an equal block number or hash, or a satisfied `number_gte` constraint against
the current [served head](glossary.md#served-head). It rejects historical or future block constraints;
future capability work may add database-backed historical execution, but no
serving path filters current rows in memory. The endpoint accepts the `subgraphError`
argument and emits the graph-node default without changing the existing
Manager response path. The served-head eligibility gate remains authoritative;
the [GraphQL claimed compatibility surface](glossary.md#graphql-claimed-compatibility-surface) includes both
`_SubgraphErrorPolicy_` values, and explicit `deny` behaves the same as omitting the argument. Per-entity `allow`/`deny`
behavior belongs to future entity capabilities that can
define it without inventing in-process filtering.

`_meta(block:)` reports the served head used by entity reads, including its
number, timestamp, and parent hash. Its hash is present for an unconstrained or
hash-constrained selection and is `null` for a number-constrained selection;
the initial oracle pin asserts only the block number in that case. All root fields within one HTTP
GraphQL request share one request-scoped served-head selection. `deployment` is the interpreter
[content hash](glossary.md#interpreter-content-hash) for the serving binary.
When a head is eligible to serve, `hasIndexingErrors` derives from durable
indexing state: a non-current or rebuilding [Project publication](glossary.md#projection),
a phase that settled while unconfigured, or an unmet required verification
floor set it to `true`.
A failed or otherwise ineligible publication is rejected by the served-head
gate before `_meta` can be returned. The value is not a constant or a
network-freshness guess.

The `domain(id:)` name extension and legacy `DomainFilter` name inputs are
ENS-normalized and matched by namehash within the `ens` namespace. Generated
`Domain_filter.name*` members instead compare the served display name directly,
as specified above. While the `project` phase has not completed at the newest stored
chain head, operations that would return projection rows fail rather than
serve the prior publication. Unsupported name rows are omitted, and
unsupported record inventories preserve the existing empty record shapes.

All top-level v2 collections use the standard `page` object. Latest-state
collections do not claim a frozen snapshot; point-in-time behavior is limited
to the routes and selectors documented in [`api-v2-routes.md`](api-v2-routes.md).

## Replacement boundary

The v2 route set is the current internal API contract. This document records
local route ownership only; it does not claim that an external application has
changed its call sites or that the production public edge exposes v2. The
checked-in Caddy configuration remains on the pre-C3 routing policy, so the v2
REST surface is not publicly reachable until the maintainer-gated C3 edge
flip.
