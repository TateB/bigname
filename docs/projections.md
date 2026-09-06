# Projections

[Projections](glossary.md#projection) are rebuildable read models over canonical
identity and [normalized events](glossary.md#normalized-event). Wire shapes live
in [`api-v2.md`](api-v2.md) and [`api-v2-routes.md`](api-v2-routes.md); identity
and event semantics live in [`architecture.md`](architecture.md); persistence
rules live in [`storage.md`](storage.md).

The schema-v2 Project phase is the only projection writer. Adapters and the API
never write projection rows.

## Live maintenance

For each chain, the phase runner publishes a provider head and advances or
redoes Interpret and Project through that exact head. A displaced readable
suffix stamps both derived phases and any overlapping Verify row with a
recorded cursor. Interpret redo also stamps Project for the actual derived
range, so a same-hash interpretation repair cannot leave an older projection
generation published.

Project includes existing current rows whose cited input is no longer readable,
allowing a winning fork to retract losing-fork output. It stages the affected
scope in connection-local tables and publishes the related projection rows and
phase state transactionally.

Normal incremental Project work starts from events and identity rows in the
`(previous, target]` block window. Name- or resource-local events initially
select only that name or resource. `RecordChanged`, `RecordVersionChanged`,
and `AliasChanged` also rebuild the emitting resolver's own `resolver_current`
row. `PermissionChanged` rebuilds every resolver identified by
`scope.resolver_address` in its before or after state. Raw emitting-address
metadata is not resolver evidence; resolver-family adapters put the emitting
resolver in that semantic scope.
None of these events rebuilds other names that use the resolver. Record and
record-version events do not contribute to the resolver overview's derived
sections, so an existing resolver touched only by those kinds is republished at
the new target without restaging unrelated resolver history. That republish
path is existing-row only: a record or record-version observation without a
linked name or resource does not create a resolver row.
`ResolverChanged` rebuilds its name and resource plus the old and new resolver
rows, again without expanding either resolver to its other names.
Only a resolver `Upgraded` event or stale resolver classification caused by the
active manifest set expands through resources whose current resolver pointer
matches that resolver. Permission history by itself does not disable the
record-only carry-forward path. When a resolver must be rebuilt, Project
restages the current delta and one stored event reference for each historical
[source family](glossary.md#source-family) and each relevant permission,
resolver-pointer, or alias input. A resource
referenced only by one of those stored events is builder input, not affected
serving state: its projection rows are neither deleted nor republished. The
stored events cover live and fully revoked resolver-scoped permission
families and unlinked resolver-pointer history, so candidate selection remains
equal to a full rebuild without loading every name that ever used a shared
resolver. A content-hash change first performs a complete rebuild, which writes
those stored event references before later incremental or redo work can use
them.

Before Interpret deletes a redo range, it records the resolver addresses,
source families, event kinds, and permission resources referenced by that
range's `PermissionChanged`, `ResolverChanged`, and `AliasChanged` rows. Project
compares that small pre-redo set with the re-derived events, rebuilds only
resolvers and permission resources whose evidence disappeared, stages a
replacement for an affected family when one still exists, and consumes the
record in the same transaction as projection publication. Interpret inserts
this record once and preserves it across a restarted redo until Project
publishes the repair. When the Project head clips the redo range, later normal
catch-up consumes the remaining records as it publishes those blocks. Resolver
provenance keeps the per-family event references
for explanation only; it is not the redo work queue. Before
ordinary event staging, Project expands child scope until no more connected
topology is found.
The expansion follows both current
`children_current` rows and activated canonical `SubregistryChanged` history
through the target. Normalized rows with `node` and `child_node` fields define
direct edges. Rows with a `subregistry` field join each logical parent through
its previous and current referenced contract instances to the normalized
registration histories for those instances. This transitive step can rebuild a
whole connected topology component: every child edge whose parent or child
enters deletion scope must have its complete per-name event history staged
before publication. Candidate events and events whose block is no longer on
readable canonical lineage never contribute builder input or ordinary topology
expansion. A Project-only redo may run before Interpret replaces the affected
range; in that narrow case, a retained orphaned, state-derived ENSv2 path-expiry
release directly seeds its available logical-name and permission-resource
identifiers. A logical name becomes an [expiry root](glossary.md#expiry-root)
after the earlier publication deleted its descendants. In the standard
pipeline, Interpret copies those same identifiers to
`project_redo_expiry_roots` before deleting the release and preserves the first
copy across retries. Project consumes it when a publication covers the recorded
release block. This handoff is necessary because the deleted
descendant projections and Project's transaction-local binding selection leave
no other durable citation from which to recover the ancestor. Project also
selects a still-live ENSv2 lifecycle whose expiry crossed the displaced branch's
timestamps or whose lifecycle changed in the affected range. From either seed,
it follows only activated canonical ENSv2 subregistry edges to descendants. The
deleted or orphaned release is not served, and unrelated topology components are
not admitted.
`project_events` remains the single filter for data that builders may serve.

Code that builds a replacement projection row may read normalized events staged
for the current Project batch and fields that an earlier build deliberately
stored for later reuse. A builder may obtain those stored reuse fields from
retained rows only when its query proves that every such row is outside the
batch's affected scope and merges staged replacements for affected rows. It
must not fill a replacement row by joining a live projection row that may also
be rebuilt in the batch: live projection values are one batch stale and related
rows may be mid-replacement. Explicit existing-row-only carry-forward may also
copy an unchanged row without using it to compute another rebuilt row.

Rows outside an incremental tick's affected scope keep the target block number,
hash, and timestamp from the last tick that rebuilt them. Readers require each
stored block hash to remain canonical; they do not require an unaffected row's
target to equal the latest head, so those rows remain readable. This can
preserve an older timestamp: when a name's `declared_summary` has neither
`registration.created_at` nor `history.created_at`, the API derives `created_at`
from the earliest timestamp in that row's `chain_positions`. Until that name is
rebuilt, the fallback therefore stays at the timestamp from its last rebuild
instead of advancing with the chain head.

Wrapper expiry and `.eth` grace transitions read the latest raw fuse word and
wrapper expiry stored in the affected resource's
`permissions_current_resource_summary` provenance.[^v1-wrapper-grace-expiry]
The permission builder
refreshes that internal boundary whenever the resource is rebuilt. This keeps
timestamp-only Project ticks on projected current state instead of re-reading
all `PermissionScopeChanged` and `ExpiryChanged` history.

After event-derived publication, configured Ethereum
[hydration](glossary.md#hydration) may refresh:

- an existing ENS/60 reverse tuple whose current resolver is an admitted
  event-silent resolver; and
- supported ENSv1 `text:<key>` entries whose normalized event retained the key
  but not the value.[^ensnode-legacy-revresolver-l311][^ensnode-legacy-revresolver-l316][^ensnode-legacy-text-l356]

Hydration uses the exact number and hash from `chain_heads`, revalidates that
head in the publication transaction, and never calls provider `latest`.
Failed calls restore the event-derived baseline and keep Project retryable. It
does not write raw facts, identity rows, normalized events, reusable execution
outcomes, or durable traces.

## Rules

- Every row carries stable identity, provenance, manifest version, support, and
  chain-position or target-publication context.
- Every projection-row value other than the closed set of
  [Project-owned maintenance fields](glossary.md#projection) is subject to the
  input source enumeration below. This includes every value a consumer can read
  through the API or history and every storage-only row key or retained
  evidence field. The
  maintenance fields are `last_recomputed_at`, `inserted_at`,
  `reverse_hydration_attempted_block_number`,
  `reverse_hydration_attempted_block_hash`, and
  `reverse_hydration_attempt_ordinal`; the glossary defines their exact table
  scope and value sources. Only the maintenance fields are outside the input
  source enumeration below.
- Review every projection-builder and hydration change for non-maintenance
  inputs. Each consumer-visible or storage-only builder field written to a
  replacement row or by hydration must be a contract-defined literal or take
  all of its inputs only from one or more of these exhaustive input classes:
  - the current batch's staged normalized events;
  - interpretation- and manifest-owned authority tables: identity rows,
    discovery edges, contract instance addresses, the specifically admitted
    [`migration_discovery_associations`](glossary.md#migration-correlation-group)
    evidence described below,
    [verified label preimages](glossary.md#preimage-observation--label-preimage),
    and staged manifest state — inputs to projection, not projection rows;
  - the Project request's target context: chain, target block number and hash,
    and the `chain_lineage` timestamp of that target block, written as
    publication context;
  - `chain_lineage` context resolved at any otherwise-admitted input's stored
    chain position, including a staged event's own position, for times such as
    registration, creation, and last change;
  - timestamp alignment that selects another chain's latest
    [read-safe](glossary.md#readable--read-safe) `chain_lineage` block at or
    before an input timestamp, for auxiliary-chain positions such as a declared
    registry path's execution-chain context;
  - a replacement row already staged in the same batch and derived only from
    these inputs;
  - a field deliberately stored for later reuse; or
  - the provider result and revalidated canonical-head context used by the
    documented Project [hydration](glossary.md#hydration) paths.
  A new non-maintenance input class requires this rule to change with the
  builder that introduces it. For replacement-row construction, a live
  projection-table read is allowed only to obtain a stored reuse field when the
  query proves the row is outside the affected scope and merges staged
  replacements for affected rows, or for explicit existing-row-only
  carry-forward. It must never use a row that may also be rebuilt in the batch.
- Exact-name reads resolve snapshot selection first, then join only rows
  admitted at those positions.
- A row published at an earlier target may serve a later selected head when the
  affected scope has not changed. Equal-height admission requires the selected
  hash to match.
- Readers fail closed when selected positions, canonical lineage, or the current
  Project generation cannot be proven. They do not patch a missing row from raw
  facts, adapter internals, provider data, or a newer projection.
- Resource-keyed projections require their selected resource to resolve to a
  readable identity row. Their input events normally carry that `resource_id`.
  `record_inventory_current` is the deliberate exception: it starts from the
  resource's latest retained linked `ResolverChanged` event whose name has a
  readable canonical surface staged at the target. If the latest linked event's
  name lacks such a surface, an earlier linked event with one is the fallback;
  a selected zero-address resolver suppresses inventory instead of reviving an
  older nonzero event, and surface visibility does not participate in this
  choice. It joins already-linked `RecordChanged` and `RecordVersionChanged`
  events by logical name and emitting resolver without restricting the source
  family of either the pointer or record event. For an `ens_v1_resolver_l1`
  event whose `logical_name_id` is null, attribution instead requires the
  selected pointer's source family to be `ens_v1_registry_l1`,
  `ens_v1_registrar_l1`, or `ens_v1_wrapper_l1`, then joins chain, surface
  namehash to event node, and current resolver to emitting address. A selected
  `ens_v2_registry_l1` or `ens_v2_root_l1` pointer may also attribute the event
  when its target resolver's final classification is supported
  `ens_v1_resolver_l1` from an applicable exact declaration and the classifying
  manifest's namespace matches the pointer's namespace. Incremental Project
  staging applies the same declaration and namespace guard when adding those
  null-name events. Serving still
  attaches that inventory to a name only through the name's current readable
  resource.
- Project stages only ordinary or `consumer_visibility=activated` interpreted
  input. It excludes candidate normalized events and never reads the planned
  `migration_event_associations` or candidate identity/discovery effect tables.
  Candidate effects therefore cannot change the materialized identity rows that
  builders join. An
  [independently admitted event](glossary.md#independently-admitted-event)
  remains activated and byte-for-byte unchanged when an ENSv1→ENSv2 correlation
  references it; only the ignored association row carries the candidate
  relationship.
- The independently admitted `registry_announcement` edge for an ENSv1→ENSv2
  migration-created registry remains ordinary because it drives the watch plan,
  not a product projection. Project ignores every candidate downstream effect.
  After an activated parent transition, authority selection may classify a positive
  child-registration [authority proof](glossary.md#authority-proof), and child reachability may prove the current subregistry is its migration-created `WrapperRegistry`.
  Both require the readable canonical association, active ordinary announcement, and matching topology; reachability additionally requires non-empty association evidence contained in the parent boundary. The association proves neither result by itself.
- Coverage and support are explicit. They are never inferred from row presence
  or a historical ingest range.
- Verified provider answers are request-scoped lookup output, not projection
  state.

## Families

| Projection | Primary key | Primary read |
| --- | --- | --- |
| `name_current` | `logical_name_id` | exact-name lookup and search |
| `address_names_current` | `(address, logical_name_id, relation)` | address-to-names and reverse lookup |
| `children_current` | parent/child identity plus class | direct and classified child collections |
| `permissions_current` | resource, subject, and scope | resource permissions and role summaries |
| `account_permission_state_current` | (`chain_id`, `authority_kind`, `authority_contract`, `owner`, `subject`, `relation_kind`) | no serving reader yet; a follow-up change adds storage and API readers |
| `permissions_current_resource_summary` | `resource_id` | permission support and authority summary |
| `resolver_current` | chain and resolver address | resolver overview |
| `record_inventory_current` | resource plus record boundary key | indexed record inventory and values |
| `primary_names_current` | address, coin type, and namespace | declared primary-name claims |

`surface_bindings` remains identity history rather than a `_current`
projection. Exact-name reads ordinarily first select the logical name's
[`authority epoch`](glossary.md#authority-epoch), then select fields only from
that epoch's binding and resources at the requested position. An activated
ENSv1→ENSv2 authority proof may select a closed ENSv2 binding after release;
that [released v2 authority](glossary.md#released-v2-authority) does not fall
back to an active retained ENSv1 binding. The exact
[shared ENS infrastructure](glossary.md#shared-ens-infrastructure) no-proof
exception selects a current ENSv2 arm when ENSv1 evidence is current or
historical, without establishing an authority epoch, so its epoch start and
proof fields remain null. Historical ENSv2 evidence without a current ENSv2
binding does not qualify.

## Exact-name projection

`name_current` assembles current registration, authority, control, resolver,
coverage, and display context for one logical name. Ordinary lifecycle changes
within the same authority anchor preserve `resource_id`; wrap, unwrap,
re-registration, or another authority-anchor change follows the identity rules
in [`architecture.md`](architecture.md#identity-model).
For ENSv2, a selected binding's non-terminal lifecycle remains the exact-name
registration until it becomes terminal, even if another lifecycle has a later
grant or reservation event.
After it becomes terminal, `name_current` prefers another surviving lifecycle;
if all lifecycles are terminal, it prefers the selected binding's terminal
event over a later terminal event from another lifecycle, then prefers the
greater block number and, within one block, the normalized event stored later.
`name_current.resource_id` identifies the current control or registration resource. The nullable
`name_current.serving_resource_id` identifies a separate, event-derived resolver and record-serving
[serving resource](glossary.md#serving-resource) when no control binding is open. It is not a binding, registration,
address relation, or permission authority. Resolver and record readers use
`COALESCE(serving_resource_id, resource_id)`; control, relation, and permission builders use only
`resource_id`.
Its projection provenance stores the [source family](glossary.md#source-family)
of the event that selected the current resolver pointer. Resolver binding
summaries use that stored event provenance rather than a prior resolver row's
classification.

When a retained direct-registry authority first becomes name-addressable, its
[`state-derived normalized event`](glossary.md#state-derived-normalized-event)
of kind `SurfaceBound` carries the observed registry owner. The exact-name
control summary exposes that owner, its registration authority context identifies
the registry-only anchor, and the effective-controller address relation includes
the owner; `control.status` remains null unless another selected authority event supplies it.

ENSv1 wrapper lifecycle and fuse effects are projected from canonical wrapper
facts. During registrar grace, the holder and lifecycle state remain visible,
while owner modification, transfer, and effective-controller membership stop at
grace start.[^v1-wrapper-grace-expiry][^v1-wrapper-grace-authority] Expired
wrapper fuses are projected as zero, matching NameWrapper `getData`; an expired
emancipated or locked position also contributes no lifecycle value or effective
holder powers because that read clears its owner.[^v1-wrapper-expired]

Incremental Project redo maps wrapper resources to affected children after it
has retained resources from projection rows whose cited events disappeared.
That second mapping reads only the resource IDs already selected for the batch;
it does not scan all wrapper resources. The ordering is required when a child
has no current child or exact-name row and its historical wrapper resource is
not the resource on its active binding: retracting the latest disqualifying
`PermissionScopeChanged` or `ExpiryChanged` event must still rebuild the child
from the surviving wrapper history.

A pre-existing owner-retraction gap remains: if an owner-zeroing ENSv1 or Basenames registry
`AuthorityTransferred` event hides a child that has no current child or exact-name row, later retracting that
event does not restore the child incrementally because no current child or
exact-name row cites it. A fresh Project rebuild or the next full source re-walk
at a [re-derivation boundary](glossary.md#re-derivation-boundary) restores the
child; [#835](https://github.com/ensdomains/bigname/issues/835) tracks the
missing bounded replay seed.

For the ENSv2 post-audit Sepolia deployment profile, declared exact-name rows
come from the admitted registry and registrar families. Out-of-profile resolver,
reverse, primary-name, mainnet, and execution behavior does not become exact-name
truth.

For Basenames, exact-name truth comes from the admitted Base registry,
registrar, and resolver families. Base primary-claim intake and L1 compatibility
transport do not create alternate exact-name rows.[^bn-readme-l70][^v1-l2rev-base-deploy][^v1-l2rev-event]

## Address and child collections

Address-to-name collections use `address_names_current` membership and join
`name_current` for display, sort, and compact record fields. Relation vocabulary
is `registrant`, `token_holder`, and `effective_controller`. Surface is the
default unit; resource deduplication is explicit.

`children_current` stores direct and classified child relations. For registry
events from ENSv1, Project first filters the relation by the parent's
ENSv1→ENSv2 migration path: `unwrapped`, `unlocked_wrapped`, and
`emancipated_child` parents retain no ENSv1 children, while `locked_wrapped` and
`locked_child` parents retain only a [migratable child](glossary.md#migratable-child)
through their [migration registry](glossary.md#migration-registry-wrapperregistry).
An unknown activated path is a Project data-integrity failure. Child authority
selection then chooses among the surviving arms; cross-era recency never chooses
the arm. A surviving locked-path row cites the matched association's stable
logical-edge and correlation identities plus its source manifest; its row-level
manifest version therefore accounts for the association that authorized the
migration registry. Its `normalized_event_ids`, `event_identities`,
`raw_fact_refs`, and `manifest_versions` arrays are independent evidence sets,
not positionally aligned tuples; an input contributes only the identifiers it
actually owns.
Reachability is per parent relation, not transitive: hiding a parent-to-child relation does not itself hide that child's children.
For registry
events that expose only a labelhash, Project composes the child name from a
verified label preimage when one exists and its normalization verdict is true,
and leaves the name columns null when none does — the labelhash and child node
are proven, the label is not. Reads name such a child by the [non-name
form](glossary.md#non-name-form)
`[<labelhash-without-0x>].<parent-name>`, built from the parent's stored
spelling, and returns those same stored bytes in both name fields. A preimage whose label
bytes are not valid UTF-8, or contain a NUL, is a third state: Project stores
the whole child name as raw bytes with no decoded form, and reads escape-encode
that whole string, parent portion included. A preimage whose bytes decode but
fail the verdict is a fourth state: the text is a valid string but not a name
for the proven node — serving it would attach a spelling that re-hashes to a
different node — and escaping it would serve the same misleading text, so
Project keeps the raw label bytes, withholds the decoded text and both name
columns, and the placeholder serves. None of these shapes is an addressable
name. A preimage improves readability but does not create ownership or
exact-name authority. ENSv2 direct and linked
children derive from admitted graph events rather than token enumeration, and
join the child's own active surface, so none of the name-less shapes arises
there.[^v1-registry-l45][^v1-registry-l82][^v2-events-l49][^v2-events-l75]

Chain-observed label preimages are shared across namespaces in one table set, as
is the child builder's labelhash join. Within one projection chain, a newly
observed mapping restages matching children in every namespace only when their
published label bytes would change; repeated observations of the same mapping
do not rebuild already-correct children. Label restaging is per projection
chain; cross-chain preimage propagation is tracked separately in issue
[#672](https://github.com/ensdomains/bigname/issues/672). Proof-checked rainbow
imports retain their separate explicit Project-redo path.

## History

History routes read normalized events, not a current projection cache. Product
surface, resource, and address scopes filter the same canonical,
consumer-visible event set: ordinary rows and
`consumer_visibility=activated` rows only. Candidate rows and
`migration_event_associations` remain available to diagnostics. An association
never removes or duplicates the independently admitted ordinary event it
references. One V1 registry resolver log can have a registry-resource row for
reads and a distinct control-resource row so both resource links survive
replay. Product history returns the control-resource row once and suppresses the
additional row carrying the registry resource link; raw diagnostics returns both normalized rows. Without
a distinct control resource, the sole registry-resource row remains
product-visible. Consumer visibility is applied before candidate evidence can
contribute an address anchor and again when rows are selected. Name and resource
anchors are constructed from readable bindings before row selection. Product
duplicate suppression then runs before cursor validation, summary calculation,
type filtering, keyset pagination, page-size limiting, or cursor construction,
so neither candidate admission nor the extra resource link can broaden, shorten,
or reorder a product page. Projection rows may supply readable names for result
decoration, but the API does not synthesize history from current state.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L94 @ ens_v1@91c966f)

For a slice-1 test re-walk that must not change product behavior at a fixed
readable chain head, an outstanding product cursor backed by normalized-event
identity must continue against the post-re-walk test publication. It resumes at
the same
normalized-event keyset anchor and preserves all remaining product rows, pages,
fields, `has_more`, and summary behavior. The anchor may be an unmapped event,
so an interleaved non-product event at a page boundary must not skip or duplicate
visible rows. A diagnostic-events cursor must remain valid and continue from the
same stable normalized-event anchor, but its subsequent diagnostic rows and
fields may reflect candidate admission. A pre-existing diagnostic row's numeric
`normalized_event_id` may change while its `event_identity` and pre-existing
semantic fields remain stable. Storage may preserve the numeric
normalized-event ID or resolve the old token through stable `event_identity` and
its stored sort tuple; these are alternative strategies. Fresh post-re-walk
cursor bytes may differ, and fresh cursors must also continue normally. The
control and candidate test runs hold every other shared-boundary input
constant, including PR #391's topology serializer.

Slices 1 and 2 deploy together with
[PR #391](https://github.com/ensdomains/bigname/pull/391) at one planned
[re-derivation boundary](glossary.md#re-derivation-boundary) under one
[interpreter content
hash](glossary.md#interpreter-content-hash), one full source
re-walk, and one Project publication decision for `ethereum-sepolia`. The
candidate filters above are exercised by replay and acceptance tests;
production makes only the activated Project publication. Other
chains retain independent publication decisions.

## Permissions

`permissions_current` is resource-anchored and preserves subject, scope,
effective powers, provenance, and chain positions. The companion resource
summary distinguishes authoritative empty enumeration from unsupported or
partial permission support. Current non-wrapper summaries are partial because
registrar token and account approvals, resolver operators and delegates, and
ENSv2 registry operators are not indexed.

`account_permission_state_current` separately folds `AccountPermissionChanged`
events from the [`standard_approval`
derivation](glossary.md#standard-approval-derivation) by chain, authority kind, authority contract,
owner, subject, and relation. It retains both active and revoked latest states;
`approved=true` carries `registry_control`, while `approved=false` carries no
effective powers. Project never fans this account mapping out into per-name
rows. After constructing `name_current`, Project carries the latest
[registry-owner binding](glossary.md#registry-owner-binding) onto the resource
selected for an ENSv1 or Basenames name. Registry-family owner observations are
first ranked by logical name or emitting resource to suppress detached history,
then mapped onto that selected resource and ranked again by output resource.
The separate resource that retains registry observations is bypassed by that
mapping. When `name_current` has no eligible selected resource, or the event has
no logical name, the observation stays on its emitting resource. This remapping
never crosses onto an ENSv2 resource. A latest zero owner or an admitted registry-
or registrar-family `SurfaceUnbound` transition clears the binding. A registrar-
family `SurfaceBound` carries the registry owner and emitter-derived registry
contract remembered at transition time, not the registrar token owner, so the new
current authority receives the binding without attribution to the registrar
emitter; wrapper-family authority transitions remain outside this rule.

(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L118 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L78-L103 @ ens_v1@91c966f)
(upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L70-L84 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L575-L592 @ ens_v2@a971bd64) Known
owner-derived rows remain available, but neither those rows nor a zero-row
summary is an authoritative permission enumeration. API contract tests inject
an independently proven full summary to verify that resource-bound public
requests are not globally forced to partial.

When a registrar `Transfer` changes ENSv1 or Basenames authority between a
registrar resource and a registry-only resource, `resource_control` and
`resolver_control` for any selected nonzero resolver are revoked on the retiring
registry-only resource or granted to its owner when it becomes active. An unchanged
authority emits no additional registry-only balancing rows; ordinary token-holder permission rows remain unchanged.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L86-L95 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/Registry.sol:L46-L52 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/Registry.sol:L132-L134 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L321-L329 @ basenames@1809bbc)

When a state-derived ENSv2 path-expiry release remains the resource's terminal
lifecycle event and retires effective permission rows, the resource summary
keeps the selected registration-authority event's provenance unchanged.
Separate `expiry_retirement_*` fields identify the release event, its source
manifest and source family, its manifest version, and its
block/transaction/log position. A later ENSv2 grant or reservation removes
these fields. A later `RegistrationRenewed` removes them when
`revived_from_expiry=true` and a preceding state-derived path-expiry release
belongs to the same `resource_id`. Whether the release named a surface does not
participate: a same-resource renewal revives retained grants even while another
token remains the current holder of that name. Unregistering an owned entry and
later registering it use a new versioned resource, so a renewal on that new
resource cannot match the old resource's release or grants. Registering a
non-expired owner-zero reservation does not enter the owner-burn branch, so
neither version counter advances. ENSv2 constructs the permission resource from
`eacVersionId`, so the registration reuses the reservation's resource ID.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L29-L34
@ ens_v2@a971bd64) (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L428-L471 @
ens_v2@a971bd64) (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L632-L645 @
ens_v2@a971bd64)
This is the
resource-lifecycle form of the [ENSv2 expired-role projection
narrowing](upstream.md#known-divergences). The retirement citation therefore
explains why the rows are absent without
rewriting which event established authority.

For ENSv1 wrapper-backed resources, fuse state alone does not manufacture a
holder grant. A separately observed compatible holder grant is masked by the
current lifecycle and [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word)
fuse rules. Returned permission rows
join the same wrapper lifecycle and fuse summary as exact-name reads; this does
not change the companion resource summary's unsupported wrapper-holder
enumeration status. For ENSv2, permissions remain keyed by the
upstream resource linked to bigname `resource_id`, not by token ID.[^v2-iperm-l57][^v2-pr-l261][^v2-pr-l351]

Unknown or inconsistent typed summary combinations are a storage error. A
persisted unsupported reason that a reader does not recognize maps to partial
unknown support rather than wrapper support or an internal server error.

## Resolver and records

`resolver_current` summarizes one resolver contract across readable bound names,
aliases, roles, record evidence, and normalized events. Embedded binding,
alias, permission, and role-holder summaries store `total_count`,
`sample_limit=100`, `sample_count`, `truncated`, and a deterministic `items`
sample no longer than that limit. Full bound-name and permission collections
remain on their name-side projections and routes instead of being duplicated
into one resolver row. The resolver summary is diagnostic and does not replace
exact-name topology.

`record_inventory_current` records the selectors observed under a resource's
latest retained linked resolver event whose name has a readable canonical
surface staged at the target, with fallback to an earlier linked event when a
later event's name lacks such a surface. A selected zero-address resolver
suppresses inventory rather than falling back to an older nonzero event. It
remains resource-keyed when a registry-only name loses control: an event-linked nonzero registry
resolver may keep that resource reachable through `name_current.serving_resource_id` while the
control resource and binding stay null. This evidence is derived entirely from normalized events;
Project and API serving perform no live registry or resolver read. It
also records the selected resolver's record boundary, explicit gaps,
unsupported families, and any retained indexed values. The record event need
not carry that resource: Project normally joins its `logical_name_id` and
emitting resolver to the pointer without restricting either event's source
family. An `ens_v1_resolver_l1` event whose `logical_name_id` is null may join
when the selected pointer's source family is `ens_v1_registry_l1`,
`ens_v1_registrar_l1`, or `ens_v1_wrapper_l1`, and only through the same chain,
the surface namehash equal to its retained node, and the pointer address equal
to its emitting resolver. A selected `ens_v2_registry_l1` or `ens_v2_root_l1`
pointer may also join when its target resolver's final classification is
supported `ens_v1_resolver_l1` from an applicable exact declaration and the
classifying manifest's namespace matches the pointer's namespace. Incremental
staging applies the same guarded exception. A `basenames_base_resolver` event
with no logical-name attribution may join only when the selected pointer is
`basenames_base_registry`, with the same chain, node-to-namehash, and resolver
emitter match. Basenames keeps the current resolver by node, permits its
registrar controller and reverse registrar to write independently of the node
owner, and stores text by record version, node, and key.
(upstream: .refs/basenames/src/L2/Registry.sol:L173-L180 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/L2Resolver.sol:L193-L199 @ basenames@1809bbc)
(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/ResolverBase.sol:L7-L24 @ basenames@1809bbc)
(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/TextResolver.sol:L7-L36 @ basenames@1809bbc)
Pointer position is
not a write-time lower bound: selecting a resolver exposes its retained
pre-pointer writes, switching away hides them,
and switching back restores them. The latest `RecordVersionChanged` from that
resolver remains the boundary, and records must be strictly later than it. This
follows the registry resolver lookup
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137 @ ens_v1@91c966f)
and the resolver's version-, node-, and key-scoped text storage
(upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L28 @ ens_v1@91c966f).
When the first active ENSv1 [name surface](glossary.md#surface-name-surface) is
materialized, Interpret links the latest replayed nonzero registry resolver to
the current registry-only authority resource. If getter-visible registry
ownership is explicitly zero, Interpret instead links that resolver to the
retained registry [serving resource](glossary.md#serving-resource) without
creating control. A latest zero-address
resolver selection suppresses this materialization pointer rather than reviving
an older nonzero selection. The original raw-derived normalized row remains
immutable; the linked pointer is an additive
[state-derived normalized event](glossary.md#state-derived-normalized-event) at
the raw event that first materializes the active surface. A wrapper-provided
surface links the retained registry read resource without binding that dormant
registry resource while wrapper control remains current. Same-transaction
registration reconciliation leaves that registry-read pointer on the dormant
registry resource rather than retargeting it to registrar control. Record
attribution remains node-keyed and provider-free. If a registrar registration
makes the registrar resource current before the retained registry-only
authority can be materialized, the same observation still marks that retained
authority's surface known. A later registrar release can therefore restore the
existing registry resource and its direct-registry owner instead of losing the
known name.
An old-registry resolver selection stops being eligible when either a
current-registry `NewOwner` or `Transfer` creates that node's current-registry
record. The ownership observation persists the
[registry fallback handoff](glossary.md#registry-fallback-handoff) across replay;
if the old pointer was already linked, later linked zero-resolver events
retract it from every registry, registrar, or wrapper resource to which it was
linked, including a resource from an authority epoch that ended before the
handoff. An old-registry `Transfer` cannot clear a resolver
selected from the current registry.
The root resolver is the frozen exception: current-registry ownership does not
retract its old-registry pointer or suppress later old-registry root updates.
(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L243-L248 @ ens_subgraph@723f1b6a)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L24 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L68 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L82 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L54 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L150-L172 @ ens_v1@91c966f)
A resource-less record event cannot create a binding, and name and record reads
expose the inventory only when the name's current readable control resource or
`serving_resource_id` selects it. Resolver-local events are accepted only under the manifest and
current-resolver rules documented in
[`manifests.md`](manifests.md).

The resolver classification also carries effective manifest-declared
[`read_features`](manifests.md#required-fields). A supported inventory copies
`ensip19_default_address` into `provenance.read_rules` with source key
`addr:2147483648`. `selectors` and `entries` remain exact `RecordChanged`
observations: Project does not fabricate target coin types or rewrite the
default entry. ENSv1 `ContenthashChanged` normalized state uses
`contenthash_hex` with `value_retained=false`. ENSv1 and Basenames
`AddressChanged` normalized state uses decimal `coin_type`,
`address_bytes_hex`, and `value_retained=false`, except that coin type 60 with
an exactly 20-byte payload preserves the scalar `value` envelope used by the
legacy `AddrChanged` event.
Project reconstructs a retained contenthash entry as
`value={"encoding":"hex","bytes":"0x..."}` and retains an address entry as
scalar `value="0x..."`. An empty `contenthash_hex` or `address_bytes_hex`
payload becomes an exact `not_found` entry with `value` omitted. The nested
`value.bytes` address compatibility shape receives the same empty-value
classification.

Rows produced under an earlier [interpreter content
hash](glossary.md#interpreter-content-hash) may retain the nested `value` object
until the [re-derivation boundary](glossary.md#re-derivation-boundary) completes.
They are not serving-eligible with the matching API during that interval;
shared readers nevertheless normalize both the nested bytes object and scalar
address forms. This hash rotation requires a complete retained-range Interpret
re-walk and Project rebuild before publication, with no manifest change.

For coin type 60, the multicoin `AddressChanged` payload takes precedence over
its immediately adjacent compatibility `AddrChanged` sibling in the same
transaction, so an empty multicoin clear remains empty instead of becoming a
retained zero-address value. The paired logs share one effective ordering
position; any later independent write in that transaction still wins.
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L65 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L68-L85 @ ens_v1@91c966f)
Removing the feature, changing to an unflagged resolver, or
rotating a proxy to an unflagged implementation removes the rule on the same
scoped rebuild. Full and incremental rebuilds select the feature from the same
current resolver classification.

For ENSv1, an admitted current resolver may contribute supported address, text,
and contenthash inventory. An unlisted or unsupported resolver family stays
explicitly unsupported. For ENSv2, current-emitter version evidence may define a
boundary while the unadmitted resolver profile still publishes no record
values. Basenames record facts remain gated by the admitted Base resolver
profile.

`GET /v2/names/{name}/records` reads this inventory for `indexed` behavior.
`verified` and `auto` may use fresh schema-v2 lookup as described in
[`execution.md`](execution.md); they never read a legacy execution cache.

## Primary names

`primary_names_current` stores declared claim state plus internal rolling
reverse-name polling selection state. Supported claim statuses are `success`,
`not_found`, `unsupported`, and `invalid_name`. A successful row keeps the raw
claim and whether its bytes already equal the normalized claim. The internal
selection columns are not claim fields and readers never select them. Project
does not persist a verified-primary result or trace identity.

Current-head hydration for an admitted event-silent ENSv1 reverse resolver may
refresh an existing ENS/60 claim tuple at the exact published Ethereum head. It
does not create a normalized event or verified result. Provider failure restores
the event-derived row and keeps Project retryable.

Each hydration tick refreshes every eligible reverse tuple rebuilt by the
Project delta at that head, then at most 250 additional eligible tuples. A tuple
is attempted at most once at a given head. The rolling selection orders groups
by their durable attempt order: never-attempted tuples first, then the group
attempted least recently. Within one group it orders tuples by oldest successful
hydration head, with missing hydration first, and then by stable tuple identity.
Every attempted group gets a new durable head and ordering value, including
after provider failure. Failure removes the
`canonical_head_multicall_hydration` provenance object that readers require
before accepting the provider-derived claim, but does not return the group to
the front; it keeps its place in the global round-robin. A same-head retry
therefore reaches tuples beyond a failed group, and a new head does not let that
group repeatedly overtake older waiting groups.
These values belong only to Project's rolling hydration selection: readers never
use them as claim data, and they cannot make a failed provider result readable.
They persist across transaction commit, process restart, same-head retry, and
head advancement. Rebuilding an affected primary-name tuple clears them with
the rest of that projection row; the rebuilt tuple is selected immediately from
the Project delta, so it does not depend on its prior rolling position. The tick
also restores every newly ineligible delta tuple and at most 250 older
ineligible hydrated tuples. Thus event-driven changes are visible immediately,
while event-silent provider values for the remaining corpus are refreshed in
bounded rolling batches instead of all being polled at every head. If a
hydration block becomes noncanonical, readers expose the stored event-derived
baseline until that tuple is refreshed at a readable head. A same-height fork
makes the prior attempt eligible at the replacement hash without changing its
round-robin position.

Verified ENS/60 primary-name status is computed per request by schema-v2 lookup.
It does not require a projected declared claim: the route performs a fresh
reverse lookup, requires that live claim to be byte-normalized, and accepts it
only when the forward address matches. A projected claim, when present, remains
an indexed candidate and an input to the pre-forward authority gate; tuple
presence alone does not prove primary status.

## Reorg and redo

Canonicality change, manifest change, or interpreted-content replacement stamps
the affected Project range. Project rebuilds the affected scope in dependency
order and publishes one coherent generation. There is no worker invalidation
queue, apply cursor, replay-version fence, general-purpose durable staging,
replay marker, dead-letter queue, or cache invalidation side effect. Three narrow
handoffs preserve input that would otherwise disappear before Project can
select its redo scope: `project_redo_resolver_evidence` retains resolver and
permission-resource references, while `project_redo_expiry_roots` retains
logical names and permission resources from state-derived ENSv2 path-expiry
releases. `project_redo_child_registration_history` retains affected child and
registry identifiers for removed migration-registry entry history. None is
serving data. Project consumes a row only when its
publication range covers the recorded block; an operator redo ending below an
already recorded Project head can therefore leave later rows for a covering
redo or full rebuild.

`phase-runner rewind` selects an exact stored readable ancestor, marks the
displaced suffix orphaned through normal head publication, and stamps downstream
redo. Historical API reads serve only when eligible projection materialization
exists for the selected positions; they never overwrite newer current rows or
fall forward to current state.

An interpreter content-hash rotation requires a full-history Interpret and
Project walk. Phase state and API admission refuse to mix output from different
compiled hashes.

## Index baseline

Indexes follow measured serving queries. Baseline access paths cover exact-name
identity, address relation membership and pagination, parent-child collections,
resource permissions, resolver identity, record-inventory boundaries, primary
claim tuples, normalized-event history, and phase lineage/head selection.
Adding a compact route may justify another measured index; it does not create a
new truth family.

## Ownership

- Interpret and adapters emit identity, discovery, and normalized events.
  Interpret also preserves pre-delete resolver references, ENSv2 path-expiry
  names or resources, and migration-registry child names that seed the covering
  Redo-mode Project publication. Normal-mode catch-up currently consumes these
  rows without seeding from them; #828 tracks that asymmetry. These rows are
  replay coordination, not projection writes.
- Project reads canonical interpreted input and owns every projection write.
- The API reads projections and request-scoped lookup output.
- Storage exposes typed reads and phase publication boundaries; it does not
  grant adapters or API handlers a projection write shortcut.

---

[^bn-readme-l70]: (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
[^v1-l2rev-base-deploy]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-l2rev-event]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
[^v1-registry-l45]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L45 @ ens_v1@91c966f)
[^v1-registry-l82]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L82 @ ens_v1@91c966f)
[^v1-wrapper-grace-expiry]: (upstream: .refs/ens_v1/contracts/wrapper/README.md:L69 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L806 @ ens_v1@91c966f)
[^v1-wrapper-grace-authority]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L218 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L221 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L820 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L828 @ ens_v1@91c966f)
[^v1-wrapper-expired]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f)
[^v2-events-l49]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L56 @ ens_v2@a971bd64)
[^v2-events-l75]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L88 @ ens_v2@a971bd64)
[^v2-iperm-l57]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L83 @ ens_v2@a971bd64)
[^v2-pr-l261]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L304 @ ens_v2@a971bd64)
[^v2-pr-l351]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L363 @ ens_v2@a971bd64)
[^ensnode-legacy-text-l356]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L356 @ ensnode@2017ae6) (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L364 @ ensnode@2017ae6)
[^ensnode-legacy-revresolver-l311]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L311 @ ensnode@2017ae6)
[^ensnode-legacy-revresolver-l316]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L316 @ ensnode@2017ae6)
