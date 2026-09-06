# Storage

PostgreSQL is the durable indexing and serving store. Current runtime objects
live in `bigname_phase`; the append-only SQLx history in `migrations/` records
the retired `public` schema, its schema-qualified deletion, and reviewed
in-place schema-migrations for initialized `bigname_phase` databases.
Deployments do not require the database itself to use C collation, but the
deployed collation must order fixed-width lowercase hexadecimal text
byte-lexically as C does; the API relies on that property to retain the existing
B-tree service for identity keys. Other comparisons that need C ordering apply
it locally. Numeric or otherwise hex-incompatible collations are unsupported
until a schema-migration index or startup locale gate explicitly admits them.
The repository's CI/test database and default Docker deployment use
`postgres:16-alpine`: musl-backed libc collations are bytewise, so those images
satisfy this contract by construction but cannot validate its glibc behavior.
An external glibc 2.39 deployment probe confirmed that 25,000 fixed-width lowercase
hexadecimal strings sort identically under `en_US.UTF-8` and C. This remains a
deployment property rather than a suite-enforced gate. The ignored integration
test runs the same probe on the available glibc PostgreSQL image; issue `#833`
tracks glibc 2.39 CI. Expression-local `COLLATE "C"` remains load-bearing on a
glibc server for noncanonical operands, where lowercase and uppercase
hexadecimal text can sort differently.

## Invariants

- [Raw facts](glossary.md#raw-fact) are immutable and block-hash anchored.
- A Verify row with a recorded cursor is stamped before intake replay can
  rewrite its readable raw-fact extent; stamping makes any retained level
  historical until Verify reruns.
- [Canonicality](glossary.md#canonicality) is explicit; block number alone is
  never sufficient identity.
- Interpretation output and [projections](glossary.md#projection) are
  rebuildable.
- Execution-provider responses are request-scoped. They are not persisted as
  reusable outcomes or durable traces.
- Unsupported behavior is stored and returned explicitly.
- API serving reads use `bigname_phase`; they have no fallback to legacy
  `public` tables.

## Schema and layers

`phase-runner init-schema` installs the fresh baseline into an empty
`bigname_phase` namespace and refuses a nonempty target. The phase runner and
API use that namespace in one database. Reviewed versioned schema-migrations
normally upgrade an initialized namespace in place when the change can preserve
its durable state; the reviewed replacement procedure is required otherwise.
One deployment's `bigname_phase` tables are one table set. Chains carrying the
`ens` namespace never share a table set: Ethereum Mainnet and Ethereum Sepolia
must not write to the same tables, and Sepolia always runs as its own deployment
with its own tables. Two chains may share one database only when their
chain-native name-system namespaces differ, as in the supported Ethereum-plus-Base
production deployment. The phase runner derives each configured chain's
namespace from the binary-approved [deployment
profiles](glossary.md#deployment-profile) and refuses this invalid topology
before starting any chain. A chain ID absent from those approved deployment
profiles is unsupported and refused explicitly. The check runs for supervised
startup and operator redo after its chain set is resolved, before manifest
synchronization or any indexing phase runs.
An additive baseline index may be an explicitly reviewed release exception when
its production build must use `CREATE INDEX CONCURRENTLY`: the release runbook
must carry the exact live DDL, validity checks, recovery procedure, and
release-record evidence instead of silently treating the baseline edit as an
initialized-namespace upgrade.

The physical layers are:

1. lineage and head state — `chain_lineage`, `chain_header_audit`,
   `chain_heads`, and per-phase progress;
2. selected immutable raw facts — admitted blocks, transactions, receipts,
   and logs;
3. manifests and discovery — synchronized declarations, admitted contract
   instances, capability state, and discovered edges;
4. interpreted identity and events — name surfaces, bindings, resources, token
   lineages, label preimages, and normalized events; and
5. current projections — name, relation, child, permission, resolver, record,
   and primary-name read models.

## Query ownership

`crates/storage` owns [canonicality](glossary.md#canonicality), snapshot selection,
reusable row reads, and database invariants. These rules are shared across callers and remain
below route code even when a route composes them into a larger query.

`apps/api` owns route-specific joins, pagination, wire shaping, and GraphQL compatibility.
GraphQL compatibility queries therefore live with the API surface, while their reusable
canonicality predicates come from storage. API helpers that confirm one request reads against an
unchanged selected chain position also remain in `apps/api`; they are not reusable database reads.

This documented boundary is authoritative. `scripts/check-query-ownership` is a tripwire for
known naming patterns, not a complete classification of SQL ownership. Review for every new
direct-SQL module in `apps/api` must state whether storage or the API owns its query behavior.

The first four layers are inputs to Project, but ENSv1→ENSv2
migration-correlated contributions marked `consumer_visibility=candidate` are
diagnostic input only until their contracted consumer activation. Current
projections can be rebuilt from canonical consumer-visible identity and
normalized events. Canonical-head
[hydration](glossary.md#hydration) is execution-derived enrichment applied by
Project to the documented record and primary-name surfaces after event-derived
publication.

## Identity

Stable identity follows [ADR 0002](adrs/0002-surface-resource-identity.md) and
the continuity rules in [`architecture.md`](architecture.md#identity-model).

- deterministic namehash-based IDs identify chain-native name surfaces;
- opaque UUIDs identify backing resources, bindings, and token lineages where
  upstream has no stable global identifier; and
- monotonic database IDs are limited to append-only observation order where
  they are not public identity.

Permissions and control attach to `resource_id`, not display text. Historical
surface-to-resource changes remain reconstructible through `surface_bindings`.
Canonical display text is derived from verified preimages and normalization
state; it is never identity.

### Binding intervals and authority arms

Every `surface_bindings` row stores a non-null `authority_arm` with one of the
closed values `ens_v1`, `ens_v2`, or `basenames`. The value is the persistence
form of the name's [authority epoch](glossary.md#authority-epoch), not a
replacement for `binding_kind`: both ENS eras can use
`declared_registry_path`. Adapters put the arm on each binding or closure draft,
and Interpret writes it directly. SQL must not infer it from strings or
provenance.

Ordinary binding interval operations use
`(chain_id, logical_name_id, authority_arm)` as their conflict domain. Their
predecessor and successor lookups, explicit closes, and implicit predecessor
caps cannot affect another chain or arm. The existing ordering and interval
rules are otherwise unchanged within that domain. This permits an ordinary
ENSv1 row and an ordinary ENSv2 row derived from an [independently admitted
event](glossary.md#independently-admitted-event) for the exact same logical name
to remain simultaneously open until an explicit activated
[migration boundary](glossary.md#migration-boundary) selects the successor.

When an ENSv2 registration release, a move away from a registry path, or a
block-boundary expiry closes this arm-wide conflict domain but the surface still
has a registered holder with a linked resource, Interpret reasserts the elected
holder at the same raw-log or block-boundary position.
The elected holder is the greatest lowercase `registry-address:token-id` key
among the retained registered holders with linked resources. The reassertion
writes the replacement binding and a closure that exempts it; it does not
synthesize registration, release, transfer, expiry, resolver, or subregistry
normalized events. The affected surfaces are tracked only while interpreting
the current batch, cleared at its boundary, and never persisted or restored.
The non-lifecycle `PreimageObserved` row written with a survivor reassertion
records the replacement binding and closed authority arm for redo. A raw-log
reassertion uses the existing `raw_log_preimage_observation`
[derivation kind](glossary.md#derivation-kind); a block-boundary reassertion
uses `raw_block_preimage_observation`. The latter derivation kind is admitted
by the fresh schema and by an in-place schema-migration for initialized
databases.

The append-numbered phase-schema upgrade adds
`surface_bindings.authority_arm text NOT NULL` with the closed-value check. It
ships before the planned production re-walk from block zero, so it does not
guess arms for historical rows or perform a historical backfill. Fresh replay
always supplies the value. The fresh phase baseline has the identical column,
constraint, and comment. At the offline boundary, operators empty only
`surface_bindings` and its `name_current` and `address_names_current`
dependents before applying the schema-migration; raw facts, manifest identities,
normalized-event identities, and unrelated phase rows remain in place for the
mandatory full Interpret and Project redos.

## Table ownership

| Family | Writer | Meaning |
| --- | --- | --- |
| `chain_lineage`, `chain_header_audit`, `chain_heads`, ingest cursors | Ingest and head publication | Block ancestry, readable heads, source progress, and explicit canonicality. |
| selected `raw_*` | Ingest | Immutable transaction, receipt, and log interpretation inputs. |
| `manifest_*` | manifest synchronization | Authored source declarations and admitted capability versions. |
| `discovery_*` | Interpret | Canonical discovered edges and admission evidence. |
| `name_surfaces`, `surface_bindings`, `resources`, `token_lineages` | Interpret | Stable identity anchors. |
| `label_preimages` | Interpret and `phase-runner label-preimages import-ens-rainbow` | Verified labelhash-to-label observations from chain events and the proof-checked rainbow import. |
| `ens_names` | operator rainbow load | Unverified rainbow-table candidates consumed by the import command. |
| `normalized_events` | Interpret; manifest synchronization for `SourceManifestUpdated` only | Protocol events normalized transactionally with identity output, plus retained manifest-authority history. Manifest synchronization's rows must not be deleted or rebuilt as Interpret output: [discovery-rule widening checks](glossary.md#discovery-rule-widening-and-narrowing) reconstruct historical declaration floors from them. |
| `discovery_watch_admissions` | Interpret | The last acknowledged [discovery-watch admission snapshot](glossary.md#discovery-watch-admission-snapshot) for each active manifest-authority fingerprint and lineage-orphaning epoch. This is replay coordination state, never fetched-fact evidence, redo authority, projection, or serving data. |
| `project_redo_resolver_evidence` | Interpret, then Project consumption | Pre-delete resolver and permission-resource references preserved across Interpret retries for one redo range; redo coordination only, never serving data. |
| `project_redo_expiry_roots` | Interpret, then Project consumption | Logical names or permission resources from state-derived ENSv2 path-expiry releases preserved before Interpret deletes a redo range; bounded projection-redo coordination only, never serving data. |
| `project_redo_child_registration_history` | Interpret, then Project consumption | Child and registry identifiers from entry-creating events in an ENSv1→ENSv2 [migration `WrapperRegistry`](glossary.md#migration-registry-wrapperregistry), preserved before Interpret deletes a redo range; bounded child-scope coordination only, never serving data. |
| `interpret_decode_skips` | Interpret | Append-only operator diagnostics for selected event logs from undeclared emitters skipped after malformed ABI decoding; never identity, normalized-event, projection, or serving data. |
| `migration_event_associations`, `migration_discovery_associations`, `migration_candidate_identity_effects`, `migration_candidate_discovery_effects` | Interpret | Correlation-versioned diagnostic associations and effects that slice 1 must not use to alter independently admitted normalized events, identity rows, or [discovery edges](glossary.md#discovery-graph--discovery-edge). The ordinary `registry_announcement` indexability edge remains a watch-plan input. |
| `*_current` projection families | Project | Current serving state, rebuildable from canonical interpreted input. |
| `chain_phase_state`, redo/invalidation state, `service_heartbeats` | phase runner; manifest synchronization may stamp or widen required Ingest redo work recorded by the [manifest-authority marker](glossary.md#manifest-authority-marker), and Interpret may stamp discovery-owned required Ingest work in the transaction that finalizes a completed pass | Phase progress, repair work, and runtime liveness. Both coordination writers use the shared required-Ingest installer under the existing synchronization and runner phase-exclusion rules. They preserve lifecycle backup fields, clear resumable evidence for genuinely new demand, and never execute the redo. The phase runner remains the sole executor and redo authority. |
| `project_generation_failures` | phase runner after Project rollback | Append-only audit evidence for a [projection generation failure](glossary.md#projection-generation-failure); never a product projection. |
| `resolution_divergences` | guarded lookup functions; Project publication may only clear outdated direct observations | Active live/indexed resolver disagreements and retained observations retired after the exact resolver becomes null; diagnostic only. |

Adapters provide interpretation behavior. They do not write projections. API
code reads projections and lookup output only, except for the guarded
[resolution divergence ledger](glossary.md#resolution-divergence-ledger) write.

Interpret finalizes the discovery-watch admission snapshot in the same database
transaction as the completed pass's discovery/address writes and any required
Ingest stamp. It compares the complete normalized union of concrete
address/topic intervals rather than a cursor-clipped view. An absent snapshot,
an active manifest-authority fingerprint change, or a lineage-orphaning epoch
change is a conservative empty baseline: existing discovery rows do not prove
that earlier intake fetched their address-scoped logs. The snapshot records
only that Interpret acknowledged the coverage demand; `chain_phase_state`
remains the sole work and redo authority.

Interpret redo may temporarily orphan and restage discovery rows without
creating repeated intake work because the acknowledged snapshot survives that
restaging. The row set is replaced only when a completed Interpret pass commits
under the same active authority and lineage epoch. Dropping and recreating a
chain starts a fresh comparison scope only when the wipe also clears that
chain's rows from `discovery_watch_admissions`; changing its active authority
fingerprint or advancing its lineage-orphaning epoch also starts a fresh scope.
Rollback leaves discovery writes, the snapshot, and the required Ingest stamp
unchanged together. Neither Project nor API code reads the snapshot.

Projection rows have foreign keys into `name_surfaces`, `surface_bindings`,
`resources`, and `token_lineages`. An offline rebuild must therefore remove
projection rows before removing any identity rows, rebuild or preserve the
identity rows first, and rebuild projections afterward. If an operator clears
projections before that rebuild completes, serving remains unavailable until
Project has published a coherent replacement; a failed partial rebuild is not
a serveable intermediate state.

Each `interpret_decode_skips` row records the chain, block and transaction
identity, log index, emitter, selected [source family](glossary.md#source-family)
and signature, selection scope, decoder context, and [interpreter content
hash](glossary.md#interpreter-content-hash). A malformed log from an emitter
declared in an active manifest is fatal regardless of selection scope, so this
table receives rows only for undeclared emitters. Its primary key combines
the raw-log position with that content hash, and Interpret inserts with conflict
ignore, so replaying or redoing the same log under one interpreter build does
not duplicate the diagnostic. The rows remain append-only across canonicality
changes and derived-state rebuilds; they are not replay input.

For a manifest-declared address, an omitted `start_block` is initially stored
as `contract_instance_addresses.active_from_block_number = NULL`; interval
readers treat it as an effective block-zero lower bound. Refreshing that same
initial-epoch active row materializes zero instead of replacing it with a later
finite declaration start. Omitting a previously finite start also backdates
that active epoch to zero so the widened watch range is reproducible. A
readmission after retirement remains bounded after the preceding epoch, and a
fresh admission still stores its declared finite start. For compatibility,
retained omitted-start manifest history supplies the zero widening floor even
when an older binary left a finite first-observed block. Interpret's discovery
refresh now leaves the stored `NULL` untouched, fixing
[issue #547](https://github.com/ensdomains/bigname/issues/547), so this repair is
legacy-only for the laundering sequence between unchanged synchronizations of
an already-declared address, while it still intentionally fires when a finite
discovery-created address row is later declared for the first time with an
omitted start. When a desired active declaration omits its start,
synchronization restores zero on the
earliest address epoch even if retired; later re-admitted epochs keep their
bounded starts. It stamps the required Ingest redo from block zero (clamped to
the earliest configured source start) and invalidates the derived phases for the
restored interval. The repair is
one-shot because the stored row is then zero and its positive-floor predicate
cannot fire again; a current finite declaration keeps its finite watch bound.

Manifest synchronization does not reduce these address epochs to a minimum
when it validates a newly widened direct-address watch. It constructs the
continuous union of [persisted Ingest
coverage](glossary.md#persisted-ingest-coverage) for each chain, source family,
address, and event topic after applying the declaration start to every usable
epoch. A gap or finite tail refuses the synchronization transaction, preserving
the preceding manifest rows, address epochs, derived-phase markers, and Ingest
redo state. The refusal never stamps the missing range as repaired. Operators
must instead choose the first continuously covered start or rebuild from zero.
A retained database that still requires the earlier start needs a separately
planned repair which explicitly fetches the gap before the wider promise;
ordinary address-scoped redo follows the persisted epochs and cannot fill it.

Before re-deriving a range, Interpret preserves a finitely retired
manifest-declared contract-address row as coordination state. An event-derived
observation at or before that row's close block may reproduce its discovery
edge but cannot reopen its address range. An observation after the close block
may append a range or backdate an existing later active range to the greater of
the observation block and the greatest preceding address range's close plus
one. Retired ranges remain unchanged. This preservation does not change raw
facts, normalized-event identity, or projection ownership.

A non-retryable validation failure on an already-completed Ingest or Verify
row changes its lifecycle status from `completed` to `failed` without clearing
the retained range markers, source provenance, verification level, or content
hash. Verify can also retain its final completion evidence without ever becoming
`completed` when an ordinary, non-validation failure is recorded after its final
checkpoint but before phase completion. A retained row may be restored without
replay only from structural evidence: Ingest requires matching current, target,
and live-handoff markers; Verify requires matching current and target markers
plus a verification level. The next accepted start repeats the checks for the
retained completion and moves that row through `failed` to `completed`. Error
text alone never authorizes that transition. This preserved evidence is
diagnostic state, not permission to publish: policy-based Sepolia readiness
requires both Ingest and Verify to remain completed.

At runner startup, a `running` or `paused` Interpret, Project, or Verify row with no
explicit redo is resolved only while its advisory lock remains held. A required Ingest
redo whose `last_error` begins with `required downstream redo active:` and outlived its
advisory-lock session is changed back to `required downstream redo:` while the next
runner holds that lock. Its `redo_from_block_number`, `redo_to_block_number`,
`redo_current_block_number`, `redo_current_block_hash`, `redo_target_block_number`,
`redo_target_block_hash`, `redo_source_boundary_markers`, and
`redo_manifest_authority_fingerprint` remain unchanged for the exact-range retry.
Pool-backed progress for a required Ingest redo also requires the active `last_error`
prefix, so a delayed write from the abandoned attempt cannot change those fields.
A saved Interpret or Project final checkpoint is recorded as `completed`; an
earlier checkpoint is recorded as `failed` so ordinary phase execution can
resume it. A saved Verify final checkpoint stays `failed` until current
configuration and retained verification evidence pass the completed-Verify
checks. A lock still held by another runner, or a lost lock connection during
the state update, stops the new runner. The update and lock use one database
connection. If the client cannot tell whether PostgreSQL committed the update
before that connection failed, the next start reads the durable phase state
again. An unlock or connection-close error after an acknowledged update is also
reported.

Startup settlement for a chain absent from runtime configuration records
`settled_while_unconfigured = true` on every active phase row that it changes to
`completed`. This nullable marker distinguishes a row deliberately settled
during chain removal from an ordinary completed row. Settlement requires the
row's `updated_at` revision to remain exactly the one observed by the startup
scan; if it changes before the locked update, startup reports a transient error
and its retry scans the durable state again. When that chain is configured
again, incomplete Ingest evidence resumes from its preserved source cursors,
and incomplete Verify evidence with the marker resumes normal verification.
The same incomplete Verify row with a NULL marker follows completed-evidence
validation and is recorded as failed with its diagnosis. Existing rows remain
NULL and therefore retain their ordinary phase-start behavior. Only
unconfigured-chain startup settlement writes the marker. It remains present
through a resumed attempt or retry. A failed completed-state validation leaves
it present with the diagnosis. Recovery clears it only after the same current
configuration and retained phase evidence that ordinary completed-state
revalidation requires have been accepted. Genuine normal completion or a
successful redo that leaves complete retained phase evidence also clears it, so
the recovered row is indistinguishable from an ordinary completion. These
clearing writes use the phase advisory-lock connection, so losing phase
ownership aborts the write. While any phase marker is present, that chain is not
eligible for `ready` on the status endpoint and reports `degraded` unless a
stronger `stale` condition applies, such as a genuinely failed phase or an
expired heartbeat. Interpret and Project do not
run a separate completed-state revalidation pass. For those phases, the
phase-start check is the revalidation: their retained current block must match
the canonical head's height and hash before `AlreadyCompleted` authorizes the
marker-clearing write. If no canonical head is stored, startup reports a data
integrity error and leaves both the retained position and marker unchanged.

### ENSv1→ENSv2 correlation visibility

The slice-1 ENSv1→ENSv2 intake persists the
[migration correlation group](glossary.md#migration-correlation-group) without
making it consumer-authoritative. A normalized event whose existence depends on
that correlation stores top-level `migration_correlation_ids` and
`consumer_visibility`. Ordinary events default to an empty ID set and
`activated`; a correlation-dependent event has a sorted, duplicate-free,
nonempty ID set and is `candidate` before consumer activation or `activated`
after it.
`MigrationApplied` has exactly one ID. A shared correlation-dependent event
keeps one event identity and lists every participating per-name ID; a
name-independent registrar controller event has one stable
`controller_configuration` derivation-group ID.

Independent admission takes precedence over correlation visibility. If an
existing manifest and discovery path already produces a normalized event without
the ENSv1→ENSv2 correlation, Interpret reproduces that ordinary event
byte-for-byte: its event identity, payload, provenance, and `activated`
visibility do not change. Interpret records the correlation relationship in a
separate `migration_event_associations` row keyed to the ordinary event identity,
with the sorted correlation ID set, `correlation_kind`, evidence references,
chain positions, canonicality, and `consumer_visibility`: candidate while its
group is incomplete or refused and activated when its [complete
group](glossary.md#complete-group) is admitted. Correlation
never duplicates, suppresses, or reclassifies the independently admitted event.
The event identity is a plain value rather than a foreign key. A redo deletes
normalized events in its range before replay, but retains association rows whose
lineage is already orphaned as fork evidence; such a row may therefore have no
normalized-event parent. Replay re-creates the canonical-path event under the
same identity. Project and product history readers ignore event-association rows;
diagnostic readers treat the normalized-event join as optional and can read a
retained association from its own position and `chain_lineage` anchor.

Slice 1 applies the same precedence to identity and discovery, with one explicit
intake carveout. A migration-created registry's independently admitted
`registry_announcement` edge remains an ordinary discovery row, active from the
announcement position, because it records indexability only and the watch plan
traverses it. Interpret attaches the `migration_registry_creation` relationship
in `migration_discovery_associations`, keyed to that ordinary edge;
the association does not change the edge's columns or active range. After an
activated parent transition, Project may use the readable canonical association
and active ordinary announcement to classify a positive child-registration
emitter or prove the current parent subregistry is the migration-created
`WrapperRegistry`. Candidate or activated, the association establishes neither
result by itself and activates no correlation-dependent effect. Parent
reachability additionally requires the association's evidence-reference array
to be non-empty, every reference to be a non-empty object, and the whole array
to be contained in the activated boundary; an empty array, non-object reference,
or empty-object reference cannot authorize an ENSv1 child relation under a locked parent.
Correlation-dependent parent, topology, identity,
role, registration, renewal, and normalized-event rows from the watched registry
activate only when every group they reference is complete. Refused and incomplete
rows remain candidate. Association with the migration group is not
sufficient to reclassify an effect that the ordinary edge and raw event produce
without that association; independently derivable existing-family output remains
ordinary.

Each `migration_discovery_associations` row keys identity by the tuple
([`logical_edge_identity`](glossary.md#logical-discovery-edge-identity),
`migration_correlation_id`), never by the sequence-
assigned `discovery_edge_id`. `logical_edge_identity` uses the exact canonical
tuple, length-prefix encoding, domain separator, and Keccak-256 representation
in [ADR 0002](adrs/0002-surface-resource-identity.md#discovery-edge-observation-identity). The row may
retain the current numeric edge ID as a foreign-key join accelerator, but a full
schema rebuild rebinds that value without changing association identity. The row stores
`correlation_kind=migration_registry_creation`, the announcement position,
complete evidence references, canonicality anchors, `consumer_visibility`, and
the interpreter content hash. A reorg retains the association as diagnostic
evidence under its original lineage but excludes it from current correlation
state when either the ordinary edge or cited evidence is unreadable. On an
Interpret restart, the input loader restores readable associations for active
ordinary announcement edges before folding later facts from those registries.
Full replay restages the association before its downstream effects; replaying
the same evidence produces the same key and payload, with candidate or activated
visibility derived under the current interpreter content hash.

Other candidate identity or discovery values do not merge into ordinary
materialized rows. Interpret writes correlation-versioned
`migration_candidate_identity_effects` and
`migration_candidate_discovery_effects` rows containing the proposed stable
identity or edge key, complete proposed value/range delta, sorted correlation ID
set, `correlation_kind`, evidence references, chain positions, canonicality, and
`consumer_visibility=candidate`. Those diagnostic rows are not Project input
and cannot update an ordinary row's columns, provenance, `active_from`, or
`active_to`. An independently activated ordinary identity or discovery row is
therefore byte-for-byte unchanged by candidate evidence. Candidate interpretation writes no
ENSv1→ENSv2 migration-driven predecessor close or successor open to
`surface_bindings`.

Consumer activation is a re-derivation semantic, not an in-place serving flag.
Slice 2A rotated the interpreter content hash for arm-scoped ordinary writes and
added an explicit transition value. The final activation slice now runs the
shared production/test activation function after all batch correlation paths finish;
there is no second test-only transition implementation. Its transition carries the exact
logical name, full chain position, expected `ens_v1` arm, predecessor selector,
expected `ens_v2` arm, and concrete successor binding/resource. The writer
selects current matching predecessors under `FOR UPDATE` and performs the
cross-arm close and successor retain/open in that same transaction. It never
ranks multiple predecessors and never applies the transition to descendants.
There is no runtime or manifest activation flag.

The `.eth` second-level selector is path-specific. The registrar-token
`unwrapped` and `unlocked_wrapped` paths record their exact BaseRegistrar
transfer to the Graveyard, select the registrar resource immediately before
that cleanup, and close it at the cleanup position. The `locked_wrapped` path
selects the live NameWrapper resource immediately before the ENSv2 registration
boundary and closes it there. The unlocked wrapped controller unwraps before
injecting the ENSv2 registration, so ordinary ENSv1 interpretation has already
closed its wrapper binding and reactivated its registrar position before that
recorded transfer. If no prior registrar identity was materialized, that exact
transfer confirms the fallback identity with its binding effective from the
preceding `NameUnwrapped`; the cleanup-relative time predicate remains strict.
For registrar-token `unwrapped`, [issue #822](https://github.com/ensdomains/bigname/issues/822) currently makes valid input present a false zero at that exact-predecessor check and rolls back the [physical Interpret batch](glossary.md#batch-grid). Zero remains an integrity error; the path above describes the required behavior after the writer resolves the actual predecessor.
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111-L119 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146-L148 @ ens_v2@a971bd64)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)

For the `.eth` second-level names covered by slice 2A, zero matching ENSv1
predecessors and multiple matching ENSv1 predecessors are both integrity
errors. The unlocked ERC-721 entry accepts transfers only from BaseRegistrar,
whose `ownerOf` rejects a token after its expiry, and both wrapper entry points
accept transfers only from NameWrapper. NameWrapper treats a `.eth` second-level
name as expired for transfer at the start of registrar grace.
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L92-L103 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L48-L55 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L101-L124 @ ens_v2@a971bd64)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L35-L50 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L815-L835 @ ens_v1@91c966f)
Therefore a completed supported second-level migration with no active ENSv1
predecessor means an ENSv1-from-genesis interpretation is corrupt; it is not a
valid chain state to tolerate. This rule is deliberately limited to `.eth`
second-level transitions. An emancipated child is gated by wrapper expiry
rather than registrar expiry and can migrate while its parent sits in registrar
grace, so slice 3A must state and prove its own predecessor rule instead of
inheriting this one.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L820-L823 @ ens_v1@91c966f)

The separately reviewed and separately merged slice-1, slice-2A, slice-2B, and
slice-2C implementation PRs deploy together at one planned [re-derivation
boundary](glossary.md#re-derivation-boundary), alongside
[PR #391](https://github.com/ensdomains/bigname/pull/391). The deployment adopts
one interpreter content hash, performs one full source re-walk, and makes one
Project publication decision for `ethereum-sepolia`.
Other chains retain independent publication decisions. Candidate
and activated forms remain distinct replay and acceptance-test inputs, but there
is no production interval that serves candidate-only data on the migration
target chain for ENSv1→ENSv2 migration. The ordinary announcement edge prevents
an intake gap in the restart, historical-replay, and live-follow boundary
fixtures.

For normalized-event-backed product collections, the slice-1 test re-walk must
also preserve outstanding cursor continuation at a fixed readable chain head.
A cursor issued before the re-walk must resume after publication at the same
normalized-event keyset anchor and preserve every remaining product row, page,
field, `has_more`, and summary result. The anchor may be an unmapped event, so an
interleaved non-product event at a page boundary must not skip or duplicate a
visible row. A diagnostic-events cursor must also remain valid and continue from
the same stable normalized-event anchor, although its remaining rows and fields
may reflect candidate admission. A pre-existing diagnostic row's numeric
`normalized_event_id` may change while its `event_identity` and pre-existing
semantic fields remain stable. Storage may preserve the numeric normalized-
event ID or resolve the old token through stable `event_identity` plus its stored
sort tuple; these are alternative strategies. Newly issued cursor bytes need
not match their earlier values.
The control and candidate test runs hold every other shared-boundary input
constant, including PR #391's topology serialization.

## Raw facts and payload retention

Ingest persists the minimum selected transaction, receipt, and log fields
needed to reproduce interpretation. These `bigname_phase` rows are the complete
current raw-fact family; there is no retained call-snapshot or generic payload-
cache table. Project hydration and request-scoped lookup call providers at an
explicit block position without turning those responses into raw facts.

`label_preimages` stores a verified labelhash-to-label observation: the proven
raw label bytes plus the normalization verdict for those bytes. A preimage
may improve readability but cannot create a surface, ownership, resolver,
record, permission, or primary-name fact by itself. The verdict gates how the
decoded text is consumed: Project reads it at build time and composes the text
into name-typed output only when the verdict is true. A later verdict change
does not reach projections by itself — a recompute-flags verdict flip stamps a
redo only for a surface visibility-class transition, so a flip on a label with
no name surface propagates through the full-range Project redo the
[recompute-flags runbook](deployment.md) requires after a normalizer-version
bump, not automatically. A false or error verdict keeps the proven bytes in the
store but withholds the text from names, so serving falls back to the
documented [non-name forms](glossary.md#non-name-form) — the escape-encoded raw
bytes when the label does not decode, the labelhash placeholder when it does.

## Rainbow-table preimage import

The ENS rainbow table maps a labelhash to a candidate human-readable label.
`ens_names` keeps the upstream table shape — one `(hash, name)` row per
candidate, where the generator records `hash =
keccak256(name)`.[^graph-ens-rainbow-table][^graph-ens-rainbow-hash]

Authority stays split between the two tables. `ens_names` is an unverified
staging store: every operator-loaded row is only a claim. `label_preimages`
remains the verified store: a candidate becomes a preimage row only when it is
exactly one DNS label and re-hashes — keccak256 of the raw label bytes — to the
row's recorded hash. A rejected candidate leaves no trace beyond the import's
logged counters. Collapsing the split would put unverified claims into the
verified store, so it stays.

`phase-runner label-preimages import-ens-rainbow` walks `ens_names` in
hash-keyset batches, proof-checks every row, and inserts the survivors with
`source_kind = 'ens_rainbow_import'` at priority 10 — below the interpreter's
chain-observed priority 100, so within one normalizer version a later chain
observation of the same label takes provenance precedence. A normalizer-version
bump suspends that precedence until repair: every `label_preimages` row written
under the old version — rainbow-imported rows included — must be refreshed by
`phase-runner redo --phase recompute-flags` before interpretation of the
label's next chain observation can proceed, and because rainbow rows carry no
chain coordinates the recompute selects them chain-independently, so one
chain's pass repairs every rainbow row. Conflicts on the `label_preimages`
primary key insert nothing: a re-run is a no-op and an existing verified row
is never rewritten. Rows carry the same normalization verdict the interpreter stores —
a proof-checked label whose bytes differ from their normalized form is kept
with `normalized_under_version = false` and the reason, not discarded. The
deleted worker importer instead hashed the normalized form, which admitted only
already-normalized labels; the current schema keys the labelhash on the raw
bytes and stores the verdict as a flag, so the port proofs the raw bytes.

The import writes `label_preimages` only — never projection rows. Projections
pick up the new preimages through Project:

- On a fresh deployment, load `ens_names` and run the import before the first
  Project walk; the first walk derives child names with the preimages present.
- On a populated database, run the import and then redo Project over each
  chain's full retained range, for example
  `phase-runner redo --chain ethereum-mainnet --source 'ethereum-mainnet:<key>:<kind>:<seed-basis>:<start>[:<role>]=<endpoint-env>' --phase project --from-block <first retained block> --to-block <head>`.
  Repeat `--source` with the complete intake-capable descriptor set recorded by
  that chain's Ingest cursors.
  A windowed or incremental Project run re-derives only its affected scope.
  Child-topology closure can add a connected component, but it does not cover
  older disconnected child edges; the full-range redo is the required
  sequence.

[^graph-ens-rainbow-table]: (upstream: .refs/ens_rainbow/src/main.rs:L36 @ ens_rainbow@bc44492)
[^graph-ens-rainbow-hash]: (upstream: .refs/ens_rainbow/src/main.rs:L50 @ ens_rainbow@bc44492)

## Canonicality and reorgs

Every fact-derived row that can be invalidated by a reorg carries chain,
number, hash, and canonicality evidence. `chain_lineage` is the authority for
parentage and readable block identity. Serving paths never join the deleted
`public.chain_lineage` table.

Head publication walks by block hash, marks the orphaned suffix explicitly,
publishes the replacement readable head, and records downstream redo in one
transaction. The same transaction clears active resolution-divergence
observations whose recorded positions include an orphaned block. There is no
legacy execution-cache invalidation call.

Project output is admitted only when its publication target is at or before the
selected readable head. Equal-height admission requires the selected block hash
to match. Name, relation, inventory, primary-name, and GraphQL reads all apply
this rule against phase lineage.

When a chain is removed from runtime configuration, recovery may change its
active phase row to the non-paging `completed` state without claiming that the
phase finished its work. If the chain is configured again, a completed Ingest
row resumes unless its current block and live handoff match its target. A
completed Verify row resumes unless it has a matching current/target block pair
and a [verification level](glossary.md#verification-level). Ingest already
persists its summary and every configured source cursor in one transaction.
That atomic set contains only
[intake-capable sources](glossary.md#source-role), so those completion markers
cannot survive without the matching source progress. Recovery also clears the
live handoff when it changes an active Ingest row to `completed`; this makes a
later re-add resume from the preserved source cursors even if an older runner
stopped between its formerly separate summary and cursor writes.

When a bounded Ingest redo loads a source boundary in its completing batch, the
source progress adopts the boundary marker returned by that load. The marker
must match the source target resolved before the load; a different hash at the
same boundary height fails the redo instead of substituting the pre-load target
for the loaded marker. The completing phase summary comes from the source whose
target is the redo range end after its boundary marker passes these checks. If
multiple sources meet at that height, all of their checked markers must agree.
In a multi-source redo, the final batch reloads an in-range source boundary below
the overall redo end when durable phase progress has passed it, so its completion
evidence also comes from the current
[watch plan](glossary.md#watch-plan--watched-tuple). An equal-height
durable phase marker is not intercepted by that reload. Each non-completing
batch also stores a map from source key to the boundary marker returned by an
actual source load during the active redo. At any source boundary where the
durable phase marker is exactly that height, including the overall redo range
end, a later batch resuming exactly there accepts only the stored load-derived
marker, which must
exist at that height and equal the fresh source target. A missing marker, such
as an active checkpoint written before this map existed, or a different hash
fails closed and requires a fresh redo of the full range. When this per-source
evidence proves that the boundary was inside the completed redo range, redo
completion updates the source cursor only when matching block lineage already
records that height and hash. The map is cleared with the other resumable redo
progress on completion or boundary divergence. The cursor update and phase
summary share one transaction. The previous live handoff remains in place until
the next normal Ingest pass confirms the reconciled cursor and publishes the
replacement handoff.

`chain_phase_state.redo_manifest_authority_fingerprint` binds the numeric
Ingest redo checkpoint and its per-source marker map to the chain's active
manifest payloads, excluding `normalizer_version`. Those payloads include the
roots, contracts, addresses, and watched block ranges contributed directly by
the manifests. Watch-relevant discovery-edge admissions are not fingerprinted.
An exact-range resume preserves evidence only when the stored fingerprint
matches the fingerprint of the current active payloads. Today no production
Interpret path can write a watch-relevant discovery edge while an interrupted
Ingest redo retains its checkpoint: phase-start compatibility checks gate those
writers. Manifest synchronization may run after the interrupted session's locks
are gone, but any widening stamp clears the redo cursor, fingerprint, and
per-source boundary markers before another attempt can resume. A missing or
different fingerprint likewise clears the resumable evidence and reports that
the active manifest/watch-plan inputs changed; rerunning the redo then loads the
full range under those inputs. Existing active redo rows receive no backfill,
so their first post-upgrade resume fails closed and requires that full-range
reload.

`chain_phase_state.redo_attempt_generation` has this contract: This nonnegative, row-local counter increments when an explicit redo begins, when the phase runner installs or extends a required redo stamp for a downstream phase (Interpret/Project), and when the shared required-Ingest installer records genuinely new manifest or discovery demand. New same-range demand advances the generation because an older attempt may already have passed those blocks under a narrower filter. Repeated observation of an unchanged discovery-watch admission never calls the installer and therefore does not advance the generation.
A batch carries that generation together with the persisted redo mode and the actual execution
range chosen at begin time. Its pool-backed progress update, including the
per-source boundary-marker map, succeeds only while all three values still
match the active row. No match means another attempt has superseded the batch;
the update records nothing and returns `redo attempt superseded; progress not
recorded`. Completion, failure recording, and downstream redo finalization use
the connection that owns the phase advisory lock, so losing that connection
also prevents their writes. This generation fence closes the redo-progress
instance of [#452](https://github.com/ensdomains/bigname/issues/452); that issue
continues to track whether every pool-backed phase write should move to its
lock-owning connection.

Retained lineage alone does not authorize that reconciliation when an
interrupted redo has already advanced past its last boundary. If the provider
then reports a different hash at the same boundary height and that older fork
also has retained lineage, the resumed redo fails instead of treating the fresh
hash as newly loaded. The failure keeps the redo marked in progress, clears
only its resumable progress, and leaves the source cursor unchanged. Re-running
the redo therefore starts at the requested range beginning and loads the
boundary under the current [watch plan](glossary.md#watch-plan--watched-tuple)
before cursor reconciliation can proceed. If that fresh hash has no retained
lineage, the equal-height evidence requirement still applies: the phase summary
cannot adopt the fresh resolution without a matching per-source marker returned
by a load during that redo.
Together with the per-chain manifest/watch-plan fingerprint, this closes the
last-boundary case when active inputs change between attempts. At manifest
synchronization, a semantic comparison of previous and desired watched event,
emitter scope, and start block now covers interior retained heights: any
widening that intersects stored Ingest coverage stamps a required Ingest redo
through the latest published ingested head. That redo reloads the whole
affected range under one current manifest/watch-plan fingerprint. Every redo
checkpoint uses the boundary returned by the load itself. Loaded headers must
form one parent-linked window, and each resumed window must descend from the
durable prior-batch checkpoint; a fork switch restarts the redo from its range
beginning instead of combining coverage from sibling forks. Completion of a
manifest-required redo also requires its loaded range-end hash to equal the
readable hash at that height. Ordinary repair redos retain their existing
ability to reconcile a source cursor to another retained fork before normal
head publication.

The widening comparison first proves that persisted address epochs continuously
honor the desired direct-address promise. It refuses an existing gap before the
ordinary widening path can stamp a required Ingest redo; redo state is created
only for a promise that the persisted interval union can represent. If a
required Ingest redo is pending anywhere on the chain, manifest synchronization
deliberately and conservatively refuses to remove a previous all-emitter watch
when the desired address-scoped replacement would expose a persisted epoch gap.
The redo need not belong to that watch; let it complete and retry, or split a combined
[registry-announcement widening](glossary.md#discovery-rule-widening-and-narrowing) and
all-emitter removal so its redo completes first. The transaction leaves the
previous watch plan and redo state unchanged.

## Interpretation replay

Interpretation is deterministic for a fixed manifest set, interpreter content
hash, canonical raw facts, and requested block range. A bounded Interpret redo
may replace only derived identity, discovery, and normalized-event output in
that range. Immediately before replacement, it may preserve the resolver
references Project needs to identify rows affected by disappearing events.
Those coordination rows are consumed by Project publication and are never
served. Raw facts are never edited by replay.

The ENSv1→ENSv2 `consumer_visibility` rule is included in the interpreter
content hash. Replaying one fixed hash reproduces the same correlation sets,
visibility, event identities, and payloads. Changing candidate groups to
activated groups therefore invalidates the full interpreted range and downstream
Project range; it is never a row-local patch or API-only configuration change.

Interpret redo proves raw-data presence without pretending that Live extended
each finite ingest source. Each `ingest_cursors` row proves that the source
reached from its configured start through its persisted target; Live does not
advance those source cursors. Before checking range coverage, the redo guard
already requires the complete configured source-key set and each source's
normalized kind, seed basis, and start block to match the persisted cursor
identities. That same check applies to
the configured intake-capable source set. A runtime
start above the redo range does not bypass that identity check. The guard also
requires one readable `chain_lineage` row at every height in the full execution
range. The schema-v2 baseline's partial unique index on
`(chain_id, block_number)` for `canonical`, `safe`, and `finalized` rows makes
two readable hashes at one height
structurally impossible in the supported schema; the redo check still fails if
the row is missing or if database integrity has been compromised. Cursors and
lineage both prove only the facts selected by the [watch
plan](glossary.md#watch-plan--watched-tuple) active when each block was loaded;
neither proves facts added by a later watch plan. Manifest synchronization
records a [manifest-authority marker](glossary.md#manifest-authority-marker)
when that authority changes, a persisted admission-floor repair invalidates
derived results, or stored manifest event history is repaired. Every Interpret
redo that would discharge the marker fails closed unless the operator passes
`--attest-watch-set-coverage <token>` with the invalidation token printed by the
fence error. Before passing it, the operator must run the
[mandatory historical fetch for any widened
range](manifests.md#mandatory-historical-fetch-after-watch-plan-widening), or
confirm that the change widened nothing. A multi-chain redo takes repeated
`--attest-watch-set-coverage <chain>=<token>` values. The locked redo begin
rejects a token that no longer matches the current marker.

Each attested discharge appends one immutable
`manifest_authority_attestations` row in the same transaction that begins the
redo and adopts the new [interpreter content
hash](glossary.md#interpreter-content-hash). It records the chain, Interpret
phase, redo range, authority fingerprint, invalidation token, runner instance
ID, and attestation time, with one row allowed per chain, phase, and generation.
The runner emits error-level structured telemetry from that row after commit;
if it stops before emission completes, a restart re-emits the row only after
the locked begin matches and commits the same interrupted redo. The same token
may resume that exact active, audited redo, but it is invalid after completion
or for any other redo. If the interpreter content hash changes while that redo
is interrupted, the same token and exact range preserve the audit
association while the redo cursor is cleared. Interpret walks the audited range
again from its beginning under the new hash; later interruptions under that
hash resume normally.

Manifest synchronization distinguishes manifest-authored watch-plan widening
from narrowing and unrelated authority changes. A widening over retained
coverage stamps the required Ingest redo; successful completion supplies the
current-watch-plan fetch before Interpret can run. The attestation remains the
operator's durable acknowledgement of every manifest-authority change,
persisted admission-floor repair, or stored manifest event-history repair,
including invalidations that stamp no Ingest work. An interpreter content hash
rotation with neither a current manifest-authority marker nor an active audited
redo remains flagless.
A missing lineage height, more than one readable row after loss of the schema
constraint, or an uncovered part of a source's finite target remains a fatal
presence failure.

The interpret engine loads the prior identity state required by the range,
folds physical batches without changing semantic order, and revalidates the
resume marker and current block anchors in the write transaction. A concurrent
reorg therefore cannot publish interpretation derived from an unreadable
branch.

### Interpret process memory

`normalized_events` is the working store for each [interpreter state
key](glossary.md#interpreter-state-key)'s `after_state`. The retained
[interpreter session](glossary.md#interpreter-session) may keep only a bounded
cache of those values. Cache capacity is an operator setting measured in entries;
changing it must not change normalized events, identity rows, discovery edges,
or the latest persisted state per key. A smaller capacity may cause more
database reads, but it has no interpretation meaning and is not part of the
[interpreter content hash](glossary.md#interpreter-content-hash).

Every cached value is the `after_state` of the latest readable normalized event
for the exact interpreter state key before the current batch. A cache miss uses
the existing interpreter-state history index: chain, presence of an opaque key,
SHA-256 of that key, and descending event position select the bounded index
range, then an exact comparison of the original key preserves correctness in
the event of a digest collision. The lookup applies the same canonical-lineage
and pre-batch boundary rules as a full restore. It does not scan an event range.
Every block-anchored normalized event produced by the schema-v2 adapter carries
an opaque state key; normalized bookkeeping rows without one are not adapter
state and are not eligible for cache reload. A key with no earlier readable row
has the empty object as its prior state, as it does during a fully resident
walk. Its [state facet](glossary.md#state-facet) groups event kinds that share
one value stream.

Values derived while a physical batch is being interpreted are a separate,
batch-bounded working set. They are not reloadable before the batch commits.
After each block's same-transaction reconciliation, ENSv1 protocol state
advances through only that block's surviving normalized events before the next
block's time-derived ENSv1 lifecycle checks run. Before-state chaining still
starts from the cached or reloaded pre-batch value and advances through the
exact surviving normalized-event sequence. Only those survivors update the
retained cache after the database transaction persists the batch. This keeps
dropped or retargeted provisional events out of later ENSv1 block-boundary
decisions, retained memory, and future restore input.

Ordinarily, a cold restore streams the latest readable event per [interpreter
state key](glossary.md#interpreter-state-key) in chain order. A zero-address
ENSv2 `SubregistryUpdated` carrying the
[`subregistry_invalidated_token_ids` marker](architecture.md#normalized-event-taxonomy)
is retained separately from the ordinary latest event for the same key. This
preserves one logical before/after stream and the clear needed to invalidate
older token-version pointers. Restore rebuilds the adapter's protocol state
while admitting at most
the configured number of `after_state` values to the cache; it does not first
materialize every retained JSON value in one process allocation. If the chain
[lineage orphaning epoch](glossary.md#lineage-orphaning-epoch) changes, the
process discards the whole interpreter session and rebuilds it from readable
rows. It retains only the block anchors added since the last validation while
the epoch is unchanged, rather than one dependency entry per historical state
key. Interpret also supplies the timestamp of the resume position's readable
predecessor block from `chain_lineage`; there is no predecessor at block zero
or before the first retained lineage block. After replaying retained events,
the adapter advances time-derived protocol state to that timestamp. Exact
cold-restore reconstruction therefore depends on the predecessor remaining
readable in the same input snapshot.

For ENSv2, a retained registry/root `PreimageObserved` event for a canonical
[name surface](glossary.md#surface-name-surface), or a retained resolver
`AliasChanged` preimage observation whose DNS name passes normalization,
permanently establishes that the surface is known in restored protocol state.
Alias restoration records only the known surface; it never creates or restores
a resource binding. A registration release or expiry can remove the current
binding and resource without removing that observation. Normalization-rejected
name observations are not admitted to this state. Later `RecordChanged` and
`RecordVersionChanged` resolver events
therefore retain the logical-name attribution but carry no `resource_id` when
no current resource exists, identically in a continuous walk and after a cold
restore, except for the known retained preimage-key collision: when a
resolver-emitted resource equals `namehash(N)`, named-resource and alias
preimages can share one retained [interpreter state
key](glossary.md#interpreter-state-key), so resumed interpretation can lose the
named-resource resolver hint and diverge from a fresh walk
([#560](https://github.com/ensdomains/bigname/issues/560); evidence is checked
in as an ignored collision probe). Project's record inventory attached to a resource
follows the resource's latest retained linked `ResolverChanged` event whose
name has a readable canonical surface staged at the target. If a later linked
event's name lacks such a surface, an earlier linked event with one is the
fallback. A selected zero-address resolver suppresses inventory rather than
reviving an older nonzero event; surface visibility does not participate in
this pointer choice. Record events that already carry a logical name are joined
without restricting either the pointer or record event's source family. An
`ens_v1_resolver_l1` event with no logical-name attribution may instead join
when the selected pointer's source family is `ens_v1_registry_l1`,
`ens_v1_registrar_l1`, or `ens_v1_wrapper_l1`. A selected
`ens_v2_registry_l1` or `ens_v2_root_l1` pointer may also join when its target
resolver has a final supported `ens_v1_resolver_l1` classification from an
applicable exact declaration, and that classifying manifest's namespace
matches the pointer's namespace. Incremental staging applies the same guarded
exception by requiring the pointer namespace and exact declared resolver
address to match. A `basenames_base_resolver` event without logical-name
attribution may join only through a `basenames_base_registry` pointer on the
same chain, node, and resolver emitter. Basenames keeps the current resolver by
node, authorizes its registrar controller and reverse registrar independently
of the node owner, and stores text by record version, node, and key.
(upstream: .refs/basenames/src/L2/Registry.sol:L173-L180 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/L2Resolver.sol:L193-L199 @ basenames@1809bbc)
(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/ResolverBase.sol:L7-L24 @ basenames@1809bbc)
(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/TextResolver.sol:L7-L36 @ basenames@1809bbc)
When an ended
resource still has a pointer to the emitting resolver, the newly attributed event can therefore
change that resource's rebuildable inventory row even though the event remains
resource-less. This does not restore a current binding. Registry-only ENSv1 and
Basenames names are different when a current nonzero resolver pointer remains
event-linked: `name_current.resource_id` stays null while the
[`serving_resource_id`](glossary.md#serving-resource) joins resolver and
inventory reads without creating control. An explicitly released ENSv2 name
instead keeps a row for a [released v2
authority](glossary.md#released-v2-authority) whose `resource_id` still
references the released resource, but its `serving_resource_id` is null; the
tombstone's summary nulls resolver state, so inventory attributed to that
resource stays out of current serving. A state-derived ENSv2 expiry release
removes the `name_current` row when ENSv2 is the selected authority, or when no
authority is selected and the row reports `current_authority_not_projected`;
for a resource-backed binding, the release's `resource_id` must also match the
binding's resource.
If a different ENSv2 reservation survives that expiry, the row's lifecycle
summary follows the reservation, but `surface_binding_id`, `resource_id`,
`serving_resource_id`, `token_lineage_id`, and `binding_kind` are all null: a
reservation does not write a surface binding, and the expired registration's
identity and record inventory are not current name data. This is an intentional serving narrowing:
ENSv2 stores a nonzero resolver supplied for an ownerless reservation and
returns it until expiry. (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @
ens_v2@a971bd64) (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @
ens_v2@a971bd64)
A surviving row whose ENSv1 and ENSv2 evidence cannot select one authority
instead remains explicitly unsupported. For a removed row, retained inventory
is reachable only through history. ENSv2 stores resolver records by node and
version.
`setName` passes
part zero, selecting the node-specific, any-part permission resource; the cited
authorization path reads EnhancedAccessControl role mappings and contains no
current registry-registration lookup. (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L127-L133 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L77-L85 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L178-L186 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L467-L472 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L247-L254 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L66-L78 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L185-L192 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L374-L382 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L443-L455 @ ens_v2@a971bd64)

Redo preparation restages only identities anchored inside the range, so an
identity derived before it keeps its anchor even when an in-range event
references it. An identity the replay re-observes is restored by the ordinary
upsert at its first derivation block; only one still orphaned afterwards is
re-anchored, and a name surface re-anchors from the earliest surviving
observation that carries the name itself, staying orphaned when none survives.
Outside that orphan replacement, a name surface's `deactivated_at` moves only
for a strictly lower incoming block, so the stored value does not depend on the
order emissions arrive in.

The interpreter content hash covers the current interpretation inputs: the
adapter, manifest-authority, and project sources, the manifest ABI event
declarations, and the named semantic dependencies those sources call to decide
a persisted row — ENS normalization, the typed projected-resolution topology
serializer and its closed wire vocabularies, plus the resolver-call
encode/decode, record-selector vocabulary, batched record and reverse-name read
helpers, and the JSON-RPC envelope interpretation deciding which provider
response those helpers accept as an answer. The lockfile fingerprints (version
and checksum) of the semantic dependencies are covered on the same rule:
alloy-sol-types, alloy-sol-macro and its expander and input crates,
alloy-sol-type-parser, alloy-dyn-abi, and alloy-primitives can change how a raw
log word decodes into a persisted event body; serde, serde-core, serde-derive,
and serde-json can change the final projected-topology serialization. The rest
of the lockfile stays
outside, so an unrelated dependency bump does not force a re-derivation.
Interpret's persistence stage is covered on the
same rule: which interpreted row wins a conflict, how a redo range reopens and
reanchors bindings, and which surfaces a normalizer-version recompute
activates all decide which identity, discovery, and label-preimage rows the
projections then read, so they are interpretation rather than plumbing.
Interpret's batch sizing stays outside, because completed walks that fold the same events into
differently sized physical batches produce the same rows. Request-scoped
serving is outside because it writes no interpreted, discovery, or projection
row — the guarded divergence ledger is diagnostic output, not interpretation
input. The rest of RPC transport — client construction, timeouts, and endpoint
configuration — is outside because it can only abort a request, never reshape
an answer. So a serving-only change does not force a re-derivation.

Several semantic surfaces are outside the hash today and are guarded by review
rather than by a rotation:

- interpret's input loader — which earlier interpreted state an adapter sees,
  which manifest versions, discovery rules, admitted address ranges, and
  canonical blocks it reads, and the order raw logs arrive in;
- the interpret engine's redo and completion gates, which decide whether a run
  clears a redo range or reanchors stable identities, and its prior-session
  reuse rule, which decides whether a batch folds onto retained adapter state
  or reloads it;
- the phase runner, which owns the redo marker, decides the replay range each
  run receives, and publishes the
  [lineage orphaning epoch](glossary.md#lineage-orphaning-epoch) interpret's
  prior cache revalidates against. It is outside the hash on purpose — no
  semantic interpretation may live there — but that is a rule it must be held
  to, not a property the hash enforces;
- checked-in SQL, meaning migration trigger bodies and the schema-v2 baseline
  constraints;
- chain intake's event-signature allowlist, which decides which resolver and
  registry-announcement logs become raw facts at all on the all-emitter path —
  the logs matched by topic with no address filter, and so the only way an
  unwatched emitter's logs are retained. A change there is a re-ingest decision
  as well as a re-derivation one.

Treat a change to any of them as a re-derivation decision and follow the
[planned migration and fingerprint boundary](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary).

An interpreter content hash rotation requires a planned full-history
interpretation and projection walk; the system refuses to mix generations from
different hashes. Interpret accepts the full finite-ingest range and extends
its execution through its recorded live-followed head. Its downstream Project
redo carries that effective range clipped to Project's own recorded head —
identical unless a crash between the two phases' live-cycle advances left
Project one block behind — and Project adopts the new hash only when the redo
covers its entire recorded head. An
interrupted redo retains that same effective range; recovery cannot narrow back
to the finite ingest handoff. If an interrupted attested Interpret redo spans
the hash rotation, its token remains valid only for that exact range. The new
binary clears the redo cursor written under the prior interpreter content hash
and walks the range from its beginning while retaining the durable audit
association. Moving a covered semantic source without updating the covered set
fails the build rather than silently narrowing the fingerprint.

## Projection publication

Project is the only projection writer. It derives the affected scope from
canonical interpreted input, stages rows in connection-local tables, and
publishes the affected projection set transactionally. It has no legacy claim
queue, general-purpose durable replay stage tables, apply cursors, dead-letter
queue, database session version stamp, or worker heartbeat. The three narrow replay
handoffs contain pre-delete input rather than staged projection rows:
`project_redo_resolver_evidence` retains resolver and permission-resource
references, and `project_redo_expiry_roots` retains the available logical-name
or permission-resource identifiers whose deleted path-expiry releases must seed
a bounded rebuild. `project_redo_child_registration_history` retains affected
child and registry identifiers for entry-creating events in a correlated
migration `WrapperRegistry`, so removing history can re-evaluate a hidden child.
Project consumes a row when a publication covers its recorded block. The normal
Interpret-to-Project pipeline does so immediately; if an operator runs an
Interpret redo whose requested Project endpoint is below the already recorded
Project head, rows above that endpoint remain until a covering Project redo or
full rebuild.

Consumer slice 2E adds one diagnostic exception to durable staging, not to
projection ownership. A post-reconciliation dual-current invariant makes the
Project transaction return a structured failure before `publish::swap`; that
transaction rolls back completely. The phase runner then appends one
[projection generation failure](glossary.md#projection-generation-failure) row
to `project_generation_failures` in a separate transaction. The row is
keyed by chain, target block number/hash, interpreter content hash, failure
kind, and a deterministic fingerprint of the conflict, and stores both
sides' identities (binding/resource for an exact name, parent/child relation
evidence for a child), the identity and block position of the event that proves
the surviving ENSv2 authority — an activated migration boundary or a positive
ENSv2 child registration — every relevant block/transaction/log position, and
the canonicality observed at failure for the proof, both sides, and the target
block. The fingerprint is part of the key so that a retry of the
same semantic failure records nothing, while a different conflict surfacing at
the same target still appends its own evidence rather than being swallowed. One
failed [projection generation](glossary.md#projection-generation) writes one
row, for the first failing invariant in the fixed assertion order — exact-name
authority before child authority — and within that invariant its own
deterministic witness ordering selects the recorded conflict. Any further
conflict surfaces on a later attempt. It marks the target
projection generation not ready. A later reorg or successful projection
generation never deletes the audit row: its recorded block hashes
remain resolvable through lineage as canonical or orphaned, and a later success
is a separate projection generation. Operator diagnostics may read this table;
product routes may not.

Projection rows carry:

- stable identity keys;
- manifest and source-family evidence;
- support status and an explicit unsupported reason when applicable;
- canonical chain-position or target-publication evidence; and
- the [Project-owned maintenance fields](glossary.md#projection) defined for
  that family. `primary_names_current` carries rolling hydration-selection
  fields rather than a last-recomputation time.

An unchanged row may retain an earlier publication target when a later Project
run does not affect its scope. Serving admission therefore accepts targets at
or before the selected head rather than requiring every row to equal the latest
Project block.

Published projection families include:

- `name_current` and identity companions;
- `address_names_current`;
- `children_current`;
- `permissions_current`, `account_permission_state_current`, and the
  per-resource permission summary;
- `resolver_current`;
- `record_inventory_current`; and
- `primary_names_current`.

`name_current.declared_summary` carries the current ENSv1 NameWrapper lifecycle
label and [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word)
fuse summary together. The underlying normalized
`PermissionScopeChanged` event keeps its expiry-unadjusted interpreted fuse word
unchanged; Project
clears only the rebuildable current summary when the served projection timestamp
passes wrapper expiry. Permission reads join this current summary by
`resource_id` rather than persisting a second copy in `permissions_current`.
Registry-wide approvals use the same rule: Project owns the replayable
[account permission state](glossary.md#account-permission-state) and the
[registry-owner binding](glossary.md#registry-owner-binding). Revoked account
rows remain in the current-state table so losing-fork grants and losing-fork
revocations both rebuild from surviving canonical history. Interpret re-walks
retained raw facts through the [`standard_approval`
derivation](glossary.md#standard-approval-derivation); Project then rebuilds both state legs without a provider
refetch. App-facing synthesis from those two state legs is deferred to the
follow-up serving change.

For ENSv2, a latest state-derived `RegistryPathExpired` release removes that resource's effective
permission rows without removing its partial-coverage summary. A later
`RegistrationRenewed` marked as a revival readmits retained grants when the same
resource has an earlier path-expiry release, regardless of whether that release
named a surface. A grant or reservation also readmits the resource. A new
versioned resource receives grants only from its own permission events. An
owner-zero reservation is different: registration keeps both version counters,
including `eacVersionId`, so it reuses the reservation's permission resource ID.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L29-L34
@ ens_v2@a971bd64) (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L428-L471 @
ens_v2@a971bd64) (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L632-L645 @
ens_v2@a971bd64)

Coverage wording is not an exhaustiveness claim. `support_status` and
`unsupported_reason` carry admission separately from projection completeness.
`operator_approval_surfaces_not_ingested` maps to partial, best-effort
permission coverage. This interpretation-and-projection change retains that
broad reason for every authority class; the follow-up serving change owns any
request-relative narrowing based on a proven registry-owner binding.
`ensv1_wrapper_holder_permissions_not_projected`
remains a separate unsupported class. Readers reject inconsistent typed combinations and
map an unrecognized persisted unsupported reason to unknown partial product
coverage rather than treating it as wrapper support or returning an internal
server error. The adapter-owned mapping requires a full-history Interpret
re-walk and Project rebuild under the rotated interpreter content hash; Ingest
does not rerun only when retained raw facts cover the registry
`ApprovalForAll` range required by the current [compiled watch
plan](glossary.md#compiled-watch-plan). That range was declared by commit
`b22bccee` on 2026-08-31 through
[`ens_v1_registry_l1` manifest version 3](../manifests/mainnet/ethereum/ens/ens_v1_registry_l1/v3.toml)
on Ethereum Mainnet,
[`ens_v1_registry_l1` manifest version 1](../manifests/sepolia/ethereum/ens/ens_v1_registry_l1/v1.toml)
on Sepolia, and
[`basenames_base_registry` manifest version 2](../manifests/mainnet/base/basenames/basenames_base_registry/v2.toml)
on Base Mainnet. A retained database whose Ingest predates that declaration must
have completed the retained-range Ingest redo; otherwise, the [re-derivation
boundary](glossary.md#re-derivation-boundary) must start with Ingest for that
range before Interpret and Project. A
from-zero Ingest under the current compiled watch plan satisfies the
precondition directly.

The ENSv2 expiry Project fold also rotates
the shared interpreter content hash without changing raw facts or
normalized-event semantics; the expiry interpretation slice must not be served
before its paired Project fold is deployed and that coherent replay and rebuild
has completed.

## Snapshot serving

Snapshot selection resolves `at`, explicit `chain_positions`, and consistency
to one concrete set of phase chain positions. Current head, safe, and finalized
positions come from `chain_heads`; timestamp and historical selection use
readable `chain_lineage` rows.

Projection `chain_positions` timestamps are decoded as RFC 3339 instants.
PostgreSQL JSON may spell UTC as `+00:00`; request selectors or retained rows
may carry other numeric UTC offsets and one to nine fractional-second digits.
Snapshot selection normalizes these values to UTC before comparing the
timestamp component of a chain-position identity. Storage reserializes the
normalized instant with `Z` and preserves non-zero fractional seconds. Invalid
timestamp syntax remains unusable projection state; a valid alternate offset
spelling is not stale state. This is a serving-boundary compatibility rule and
does not change which stored projection rows are authoritative or when they are
rebuilt.

Every selection also requires the current Project generation to be complete at
the newest stored head with the API's compiled interpreter content hash. The
API reads only projections eligible for the selected positions and revalidates
the Project generation before returning. A concurrent head or generation
change returns `409 stale`.

GraphQL carries the selected ENS head from the root operation into nested record
inventory reads, scopes list and count queries to the same selected chains, and
excludes unsupported name rows. An unsupported inventory maps to the existing
empty compatibility shape.

## Verified lookup storage

Schema-v2 verified lookup has no execution cache, durable trace, reusable
outcome, or persisted request-validation state. Each admitted provider lookup
runs for the current request at the selected block identity. See
[`execution.md`](execution.md).

For guarded direct resolver comparisons, fixed-`search_path`,
security-definer functions revalidate the selected state and then create,
refresh, or clear one active divergence observation. The API role receives
`EXECUTE` on those functions but no direct write access to
`resolution_divergences`. Ledger rows are durable operational observations;
they are not projection input or a response cache.
When projection publishes an ENS Mainnet exact resolver as null, a projection
lifecycle trigger retires active observations for the former direct resolver as
stale evidence. [Universal Resolver ancestor
discovery](glossary.md#universal-resolver-ancestor-discovery) only revalidates
the exact projected name, Ethereum head, Project publication, canonical
positions, and Universal Resolver manifest authority. It does not compare,
persist, or clear by agreement with the request-scoped ancestor-served result.
The guard derives the same exact-or-ENSIP-19 indexed comparison from the locked
inventory `entries` and `provenance.read_rules` before mutation. This baseline
comparison normalizes the legacy indexed status alias `failed` to
`execution_failed`, matching the Rust evaluator. The null-resolver retirement
trigger adds no table, column, or reusable provider state. Fresh databases
receive it from the baseline; schema-migration
`20260831120000_retire_direct_divergences_for_null_resolver.sql` installs it on
initialized databases, retains every ledger row, and retires already-stale
active rows without replacing the phase namespace.

## Inspection

`phase-runner inspect` provides bounded, read-only block-canonicality,
stored-lineage, and raw-event windows. These commands do not expose public API
routes and do not mutate raw facts, canonicality, manifests, or projections.
There is no worker inspection surface for backfill jobs, replay staging,
manifest drift, watch plans, or execution traces.

## Schema-migration rules

- SQLx schema-migrations are append-only and versioned. Applied files are never
  rewritten.
- Every destructive schema-migration names the target schema explicitly. This is
  mandatory where retired `public` table names collide with `bigname_phase`
  names.
- Schema-v2 baseline changes require either a reviewed in-place upgrade or the
  documented offline namespace replacement and full pipeline walk.
- Explicitly reviewed additive baseline indexes may use the manual concurrent
  build procedure documented for that release. The step must be named in
  deployment preflight and recorded outside `_sqlx_migrations`; it does not
  establish a general manual schema-change path.
- A schema-migration that changes identity, manifest authority, canonicality,
  projection meaning, or replay behavior updates the corresponding contract
  docs in the same change.
- The generic worker schema-migration entrypoint has been deleted. Deployment
  automation applies reviewed versioned schema-migrations at a planned boundary.

The legacy deletion schema-migration removes only schema-qualified `public`
objects. It preserves the SQLx schema-migration ledger and extension-owned
objects.

## Repository ownership

- `crates/ingest` and phase-runner intake own immutable chain facts and lineage.
- `crates/interpret` plus adapters own derived identity, discovery, and
  normalized events.
- `crates/project` and the phase runner own current projection publication.
- `crates/lookup` owns verified lookup behavior and guarded divergence writes;
  Project publication may only clear outdated direct observations when an ENS
  Mainnet exact resolver becomes null.
- `crates/storage` provides the typed persistence and read boundaries above.
- `apps/api` reads phase projections and lookup output; it does not write raw
  facts, interpretation output, projection rows, or legacy execution artifacts.
