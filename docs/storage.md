# Storage

Persistence boundaries for raw facts, identity, normalized events, projections, and execution. Architecture model in [`architecture.md`](architecture.md); intake detail in [`chain-intake.md`](chain-intake.md); manifest schema in [`manifests.md`](manifests.md); read model in [`projections.md`](projections.md); execution layout in [`execution.md`](execution.md).

## Invariants

- Durable raw facts are immutable. Evictable payload-cache entries and non-audit raw-log staging rows lose their system-of-record status once the replay contract that retained them is satisfied.
- Projections are disposable and rebuildable from canonical raw facts plus normalized events.
- Canonicality is explicit, never inferred from "latest row wins."
- Execution traces and steps are durable audit artifacts; cache outcomes are reusable only while their dependencies remain canonical.
- One write owner per storage family.

## Corrections

Raw-fact corrections are explicit, auditable events. They are not normal replay,
do not weaken the default immutability rule, and must name the corrupted field
set, cause, proof source, rewrite owner, acceptance checks, and ratification
record in this section before or with the tool that applies them. A correction
tool must be idempotent, resumable, fail closed on verification disagreement,
and update only the ratified fields. Any wider rewrite requirement is a new
doc-first storage task.

### 2026-07-03 raw code-hash padding correction

The maintainer ratified option (a), re-derive and rewrite, on 2026-07-03 for
the padded `raw_code_hashes` corpus written by the pre-#21 Reth DB code reader.
The audited bigname bug used the padded/analyzed bytecode view (`bytes_ref()`)
instead of the original deployed bytecode path, while the pinned Reth RPC
account-code path reads `.original_bytes()` for account bytecode
`(upstream: .refs/reth/crates/rpc/rpc-eth-api/src/helpers/state.rs:L237 @ reth@88505c7)`
`(upstream: .refs/reth/crates/rpc/rpc-eth-api/src/helpers/state.rs:L244 @ reth@88505c7)`.
The corrupted rows therefore carried deterministic `code_hash` and
`code_byte_length` values that were 1 to 19 bytes longer than the consensus
bytecode for affected observations.

The live audit measured 3,736,298 padded-corrupt rows, 94.7% of the audited
corpus, and 208,320 already-correct rows. The corrupt writes span
2026-05-01 through 2026-07-03 by `raw_code_hashes.observed_at`. The state-root
consensus anchor for the audit was a live `eth_getProof` sample on the immutable
registry contracts: the EIP-1186 `codeHash` agreed with the value derived from
the original-bytecode reader, not with the padded stored value. Pinned Reth
serves `eth_getProof` and populates the EIP-1186 response `code_hash` from the
account bytecode hash
`(upstream: .refs/reth/crates/rpc/rpc-eth-api/src/core.rs:L900 @ reth@88505c7)`
`(upstream: .refs/reth/crates/rpc/rpc-eth-api/src/core.rs:L908 @ reth@88505c7)`
`(upstream: .refs/reth/crates/trie/common/src/proofs.rs:L733 @ reth@88505c7)`
`(upstream: .refs/reth/crates/trie/common/src/proofs.rs:L736 @ reth@88505c7)`.

The ratified correction selection excludes 432 rows whose `block_hash` is
orphaned or absent from retained `chain_lineage` for the window. Those rows
retain their padded values as unverifiable historical observations of orphaned
blocks. No future canonical upsert touches those `(chain_id, block_hash,
contract_address)` keys, and deletion of those rows was not ratified.

The correction scope is limited to `raw_code_hashes.code_hash` and
`raw_code_hashes.code_byte_length`. It does not alter `raw_code_hash_id`,
`chain_id`, `block_hash`, `block_number`, `contract_address`,
`canonicality_state`, `observed_at`, any other raw-fact table, normalized
events, projections, manifests, discovery rows, execution artifacts, or service
configuration. The implementation owner is the indexer repair tooling invoking
storage-owned guarded update helpers for this raw-fact family.

The approved method is:

1. Select the ratified observed-at window, excluding rows whose block hash has
   no non-orphaned `chain_lineage` row. The tool reports the excluded rows in
   the `orphaned_skipped` bucket instead of attempting to prove them against a
   node state that no longer exists.
2. Re-derive each selected `(chain_id, block_hash, contract_address)` from the
   v2.2.0 Reth DB reader that uses `original_bytes()`.
3. Classify correction candidates by direct comparison between the stored value
   and the re-derived `(code_hash, code_byte_length)`, not by padding-length
   heuristics.
4. Refuse the run if a re-derived hash falls outside the stored variant family
   for an address that already has multiple stored variants.
5. Verify a substantive JSON-RPC sample before any write: at least 1% of
   selected rows, every distinct address at least once, and all mandatory
   out-of-family findings if any exist. The mandatory per-run sample uses
   `eth_getCode` by block hash and compares both bytecode hash and byte length
   to the Reth-derived value. The tool also attempts a small best-effort
   `eth_getProof` spot-check for state-root anchoring on the most recent
   correctable row per address; node timeout or provider-serving failure is
   logged as non-fatal after the mandatory `eth_getCode` sample, while a
   completed proof disagreement remains a verification disagreement.
6. Rewrite only `code_hash` and `code_byte_length` in guarded batched
   transactions. Each batch logs a correction-event line with row counts and
   block range, and enforces that corrected, already-correct, conflicting, and
   orphaned-skipped rows account for the requested batch. A rerun skips
   already-correct rows.

The post-run acceptance checks are node-dependent and are not CI gates. The
supervised operations run must finish with zero RPC verification disagreements,
zero unexpected variant rows, a dry-run census of zero remaining correctable
non-orphan rows for the ratified window, the audited 432 `orphaned_skipped`
rows reported, recorded `eth_getProof` spot-check status, and the env-widened
live verification test green table-wide, including
`reth_db_provider_latest_rows_match_consensus`.

## Storage layers

The system of record splits into six layers.

1. `chain_lineage` — block ancestry, fork points, hash-first reconciliation, head promotion, one durable header-anchor row per observed block hash.
2. `raw_facts` — hot indexed replay facts: selected/admitted target logs, the minimum transaction/receipt fields needed to decode them, code-hash observations, fetched call snapshots, optional header/log audit extensions, compact payload-cache metadata.
3. `manifests_and_discovery` — source manifests, discovered edges, rollout flags.
4. `identity_and_events` — `NameSurface`, `SurfaceBinding`, `resources`, `token_lineages`, and append-only `normalized_events`.
5. `projections` — current-state and collection read models.
6. `execution` — durable traces and steps, `execution_cache_outcomes`, invalidation records.

Layers 1–5 rebuild current declared state. Layer 6 replays verified answers and explains them.

Worker-owned manifest/proxy alert observations live alongside these layers as an operational audit family. They record drift findings; they are not manifest truth, discovery admission, projection state, or adapter-owned events.

## Storage substrates

Postgres is the hot indexed and replay-focused store. It retains:

- lineage and header anchors needed to reconcile forks, prove ancestry, promote checkpoints, audit canonicality
- selected/admitted target logs and the minimal transaction and receipt fields while they are needed to decode those logs, route them through adapters, and append normalized events
- block-scoped call snapshots and enrichments retained by an explicit replay contract for normalized events, projections, or execution artifacts
- durable event-silent resolver call observations used as projection-invalidation inputs after selected transaction and receipt staging rows are compacted
- code-hash observations and discovery/proxy evidence used by manifests, adapter routing, and audit tooling
- compact metadata and optional digests for full payloads fetched as cache

There is no deployed object-storage layer in the current schema or compose stack. When the system retains fetched payload metadata, Postgres stores the metadata and optional digests needed to validate later cache use; fetched bytes outside durable replay facts are cache-owned and may be absent.

## Raw-log retention modes

`raw_logs`, selected `raw_transactions`, and selected `raw_receipts` have two retention modes selected operationally:

- **minimal** — these rows are adapter-replay staging. They may be compacted after the normalized replay cursor advances past the retained block range and the corresponding `normalized_events`, identity rows, lineage rows, and projection rebuild inputs are durable.
- **log-audit** — the same rows remain durable audit facts and may keep heavier indexes for historical raw-fact replay.

Switching modes is operational policy. It does not change route coverage, projection truth, canonicality semantics, manifest rollout, or consumer-replacement meaning.

Live polling may retain selected `raw_transactions` and `raw_receipts` for successful direct transactions to configured event-silent resolver addresses even when those transactions do not emit selected logs. Intake copies the chain id, resolver address, block number/hash, transaction hash/index, and canonicality into `event_silent_resolver_call_observations` before those staging rows become compactable. The durable observation row is the projection-invalidation trigger for explicitly documented hydration repairs, such as legacy ENSv1 reverse-resolver primary-name hydration. It does not authorize adapters to synthesize normalized events from calldata or receipts, does not make raw facts an API fallback, and does not change minimal/log-audit compaction boundaries once downstream normalized replay and projection inputs are durable.

`bigname-worker raw-facts compact-log-staging` is the manual compaction boundary for minimal mode. It refuses to compact unless the `raw_fact_normalized_events` replay cursor is caught up and failure-free, and only operates on raw-log staging families. Log-audit deployments do not run it for retained ranges.

After compaction, `chain_lineage` and any retained compact block-anchor metadata remain the block-hash path for losing-branch repair, and `normalized_events` carry the block identity, source identity, event identity, and provenance needed by projection rebuilds and history reads. If raw-log staging rows are already gone, reorg repair marks normalized events and identity rows orphaned from lineage and updates zero raw-log rows for that range. Historical adapter replay from compacted ranges is an explicit backfill/refetch operation against the configured provider/cache substrate or requires log-audit retention; it is not an implicit API fallback.

Compaction and pruning must stay behind the rewind horizon they serve. Minimal mode may drop staging rows after replay is durable, but it must not drop lineage, normalized-event provenance, identity intervals, projection change records, or retained replay facts needed to orphan a losing branch and rebuild the canonical snapshot. If a compacted range later needs adapter-level byte replay and no retained digest-checked payload or provider/cache fill can satisfy it, that repair fails closed rather than inventing state from current projections.

## Evictable payload cache

Large/full block payloads, non-indexed transaction/receipt/block bodies, and non-audit raw-log staging rows are evictable cache by default once the selected replay contract has been satisfied. They may live inline during a hot window, in local/provider cache, or not be retained at all.

Retained cache metadata describes what was fetched: payload kind, chain id, block hash/number where block-scoped, optional digest, size, content type or encoding, source observation metadata, observed time, canonicality state. A retained digest authorizes later byte use; metadata without one cannot.

Provider re-fetch is an explicit, fallible cache-fill path. For block-scoped payloads it is block-hash-scoped, verifies the retained digest before any bytes are used, and fails closed when the digest is absent, the digest mismatches, or the provider cannot serve the exact historical payload. It is not a substitute for retained lineage, normalized events, execution artifacts, or orphaned-branch audit truth.

Local execution-client storage (e.g. a same-host Reth database) is provider/cache substrate, not a new storage family. Client table keys, row cursors, static-file offsets, and data-directory paths appear only in operational source metadata or evictable cache metadata — never as durable `raw_fact_ref` identities, normalized-event provenance, or projection inputs.

Historical backfill does not turn empty blocks into hot payload archives. A block with no selected target logs and no replay-required enrichment retains lineage/header anchors and any compact audit metadata required by the selected retention policy. Full block bodies, receipt bundles, transaction bundles, and payload-cache bytes for those empty historical blocks remain evictable or absent.

## Identity strategy

### Deterministic text IDs

`logical_name_id = "<namespace>:<normalized_name>"` — stable, human-auditable, derivable without database lookup.

`normalized_name` is the output of the single ENSIP-15 normalizer declared as `ensip15@ens-normalize-0.1.1`; storage validation and projection inputs must not substitute IDNA/UTS-46 conversion, ASCII lowercasing, or trimming. Name-surface DNS wire names, namehashes, and labelhash paths are derived from the same normalized labels. `primary_names_current` treats blank or whitespace-only reverse-claim source values as absent claims; nonblank claim-name sources either normalize through ENSIP-15 or remain verbatim as `raw_claim_name` for `invalid_name`.

ENSv1 name-surface materialization does not admit embedded NUL bytes in registrar labels, wrapper DNS labels, or resolver `name` record preimages. Those observations may still remain as raw facts and, where applicable, resolver-record normalized events, but they do not mint or update `NameSurface` rows or label preimage state. This keeps displayable name identity distinct from literal onchain strings that reference indexers also treat as invalid or unnormalizable for label/name interpretation.[^ens-subgraph-label-null][^ens-subgraph-name-null][^ensnode-null-label]

`label_preimages` is a hash-to-label fact table, not a name-surface table. Rows may be learned from retained `PreimageObserved` normalized events, retained `name_surfaces` that already carry normalized labels and labelhashes, or imported operationally from a rainbow-table source such as Graph Protocol's `ens-rainbow` dump. The pinned generator emits a prepared `ens_names(hash, name)` table and computes `hash` as `keccak256(name)`.[^graph-ens-rainbow-table][^graph-ens-rainbow-hash] Each imported or observed label must normalize as a single ENS label and must hash back to the stored labelhash before it can be retained. The worker-owned migration path runs a one-time retained-fact backfill after SQL migrations so upgraded databases learn already-retained `PreimageObserved` and `name_surfaces` preimages through the same Rust verification path as live upserts. Label preimages are allowed to improve projection readability, including historical ENSv1 and Basenames registry child rows, but they do not by themselves create exact-name `name_surfaces`, ownership, resolver topology, record facts, or primary-name truth.

`bigname-worker label-preimages import-ens-rainbow` is worker-owned operational tooling. Operators must first load the pinned `ens-rainbow`-shape source table `ens_names(hash, name)` into the bigname database. The command reads that table in hash order, validates and stores only labels that normalize as one ENS label and hash back to the supplied `hash`, and enqueues `children_current` invalidations for known parents that have matching canonical ENSv1 or Basenames registry child edges.

### Opaque UUIDs

- `resource_id`
- `token_lineage_id`
- `contract_instance_id`
- `surface_binding_id`
- `execution_trace_id`

UUID values are internal identities, not user-generated strings. `resource_id` and `token_lineage_id` survive projection rebuilds. Token IDs, node hashes, and resolver addresses are attributes, not identity anchors.

### Append-only event IDs

`bigint generated always as identity` for raw fact rows, normalized event rows, and projection job rows.

### Continuity rules

`logical_name_id`, `resource_id`, `token_lineage_id`, and `contract_instance_id` continuity is shared with [`architecture.md`](architecture.md) — see the identity model section there for the rules adapters must follow when minting and reusing IDs across ENSv1 wrap/unwrap/re-registration, ENSv2 token regeneration, and proxy implementation churn.

The storage-side guarantees those rules depend on:

- One admitted contract address on one chain maps to one stable `contract_instance_id` across all admission epochs. Re-admission after an inactive gap reuses the prior id and records a new non-overlapping active range.
- Proxy contracts and their implementations are separate `contract_instance_id`s. Implementation churn updates the proxy/implementation discovery edge, not the proxy id.
- Contract addresses are time-ranged attributes for raw-fact lookup, log routing, and watch-plan materialization. Addresses are never the primary key of the source graph.
- Stable adapter identity rows for `token_lineages`, `resources`, and `name_surfaces` are idempotent across retained replay anchors. Replaying a compatible row with the same stable identity and identity-defining fields from a later raw-log anchor may be accepted as an existing identity without rewriting the original anchor, anchor provenance, or `observed_at`; incompatible identity fields remain hard conflicts, and orphaned rows may be replaced through the normal reorg-replay path. For `name_surfaces`, the compatibility key is the stable logical id plus namespace, normalized name, DNS wire name, namehash, labelhash path, and normalization errors; input spelling, display spelling, normalizer version, and warnings are retained observation metadata and may differ across compatible replay observations. Retained ENSv1 unwrapped-authority name surfaces with empty normalization errors may repair a stale normalized surface path when the stable logical id, namespace, normalized name, and normalization errors still match the replayed normalized-label surface. This repair covers stale raw-cased hash paths and stale dot-containing registrar-label surfaces whose retained DNS/namehash/labelhash path collides with the normalized multi-label name; it updates only the stored DNS wire name, hash path, and ordinary canonicality/observation metadata allowed by the stable-row merge. For token/resource identities, provenance describes the retained observation anchor and is not itself a later-anchor compatibility key. ENSv1 registrar resources materialized only from a closed surface-binding segment after the lease has been released intentionally carry binding-derived provenance: `released_at` is the binding close time, `expiry` is that time minus the ENS grace period, and the prior registrant is not reconstructed into the resource row unless an unreleased current or superseded registrar lease survives finalization.[^v1-registrar-grace]
- Normalizer-version repair follows the same split. The indexer repair command may update retained `name_surfaces` observation metadata when the current normalizer produces the same logical id, normalized name, DNS wire name, namehash, labelhash path, and empty normalization errors; retained chain/block/provenance/`observed_at` anchors are preserved. Rows that reject or remap under the current normalizer are not silently rewritten; they are recorded in `name_surface_normalization_repair_findings` for semantic review before any future orphan/remap repair.
- For interval identity rows like `surface_bindings`, `active_from` and the stable identity anchors are immutable; `active_to` is replay-derived. Canonical historical replay may tighten an existing non-null `active_to` to an earlier close point when older or more complete facts reveal an earlier end. Normal replay and identity upsert paths do not extend or reopen a closed interval. Explicit adapter repairs are governed by the adapter-repair policy below: any future interval widening or reopen must be named there with its proof, overlap guard, and invalidation behavior. Replay batches that both close an existing interval and open a replacement at the same boundary apply the existing interval update before inserting the replacement, so the non-overlap invariant is enforced without relying on implicit snapshots.

For ENSv2, `resource_id` keys by `(chain_id, registry_contract_instance_id, upstream_eac_resource)` after observing the upstream EAC resource — not by the current ERC-1155 token id. Upstream exposes both `getResource(anyId)` and `getTokenId(anyId)`, emits `TokenResource(tokenId, resource)` when a token links to a resource, and emits `TokenRegenerated(oldTokenId, newTokenId)` when role changes burn and mint a replacement token while leaving the resource unchanged.[^v2-iperm-l34][^v2-iperm-l67][^v2-iperm-l72][^v2-events-l69][^v2-pr-l216][^v2-pr-l451] Unregister/re-register increments both `eacVersionId` and `tokenVersionId` and mints fresh `resource_id` and `token_lineage_id`.[^v2-pr-l28][^v2-pr-l203][^v2-pr-l237][^v2-pr-l536]

## Table families and write ownership

| Family | Write owner | Purpose |
| --- | --- | --- |
| `chain_*` | intake | lineage and canonical block graph |
| `raw_*` | intake | immutable hot replay facts and payload-cache metadata |
| `backfill_*` | worker/backfill substrate | persisted backfill jobs, bounded range leases, resumable range checkpoints |
| `normalized_replay_*` | indexer/replay orchestration | operational replay cursors and adapter-private replay checkpoints |
| `manifest_*` | manifests/discovery | source manifests, declared contract admission, capability versions |
| `discovery_*` | manifests/discovery | canonical reachable contract graph, watch-plan expansion keyed by `contract_instance_id` |
| `manifest_alert_*` | worker/audit | persisted manifest-drift and proxy-alert observations |
| `name_surfaces`, `surface_bindings`, `resources`, `token_lineages` | adapters | stable identity anchors |
| `name_surface_normalization_repair_findings` | indexer repair | audit table for retained name-surface rows that reject or remap under the active normalizer and therefore need explicit semantic follow-up rather than silent metadata rewrite |
| `label_preimages` | storage from verified retained name-bearing facts; worker/operator rainbow imports | retained labelhash-to-label facts used to resolve child labels and other display preimages without minting exact-name identity |
| `normalized_events` | adapters | append-only normalized protocol events |
| `event_silent_resolver_call_observations` | intake | durable block-scoped direct-call observations for documented projection hydration invalidation where the watched resolver emits no usable event |
| `projection_*` | projection workers | disposable read models |
| `address_names_current_identity_counts`, `address_names_current_identity_feed` | storage triggers on `address_names_current`, `primary_names_current`, and supporting identity-anchor and `name_current` readability changes | exact reverse identity total counts and compact feed display rows by address, role filter, and primary-name coin type for the partner-compatible identity façade, using the same canonical/read-safe and reachable-`name_current` row eligibility as reverse identity pages; this is the bounded exception in [`adrs/0005-identity-count-sidecar.md`](adrs/0005-identity-count-sidecar.md) |
| `current_projection_replay_status` | projection workers | durable operational completion markers for bootstrap/full all-current projection replay |
| `projection_normalized_event_changes` | normalized-event storage trigger; projection workers consume | append-only downstream change log for normalized-event inserts and canonicality-state updates |
| `projection_apply_cursors`, `projection_invalidations`, `projection_invalidation_dead_letters` | projection workers; storage trigger for projection-relevant `surface_bindings` repairs; bounded normalized-event adapter repair invalidations | durable projection apply watermarks, live key-scoped projection invalidation queue, and terminal operator-visible dead-letter records |
| `execution_*` | execution workers; API on-demand verified-resolution cache misses for documented product routes; synchronous indexer/reorg repair for orphan-block cache outcome deletes only | durable traces and steps, normal `execution_cache_outcomes` writes, invalidation records |

The API process is otherwise read-only against storage.

Within `execution_*`, the API may write traces, steps, and normal
`execution_cache_outcomes` only for documented on-demand verified-resolution
product routes when a selected-snapshot cache miss is live-executed and
returned in the same response. That path uses execution persistence and does
not write projections, API state, manifests, discovery rows, normalized events,
identity rows, or adapter-owned facts. Synchronous indexer/reorg repair is the
other non-execution-worker write owner during chain reconciliation. It may
delete or invalidate reusable `execution_cache_outcomes` rows whose dependency
set includes an orphaned block identity. It does not write traces, steps,
normal outcomes, projections, API state, or manifest state.

For identity-row repair, the storage-owned `surface_bindings` update trigger is the bounded non-projection-worker writer for `projection_invalidations`. It enqueues `name_current` and `address_names_current` keys when repair updates change `active_to` or `canonicality_state` for an identity row. The normalized-event upsert repair path has bounded stale-key invalidation exceptions: Basenames primary-claim source repair enqueues both old and repaired `primary_names_current` tuple keys when it rewrites an existing `RecordChanged(name)`/resolver claim observation from the old Basenames reverse-registrar tuple to the ENSv1 Base `L2ReverseRegistrar` tuple; ENSv1 registrar renewal resource repair enqueues old and repaired resource keys for affected resource-keyed projections when it repoints stale renewal/resource events; ENSv1 registry/registrar event-time resource repair enqueues stale and repaired resource keys, or only the non-null key when one side of the repair has no resource anchor; Basenames Base registry `AuthorityTransferred` event-time resource repair enqueues stale and repaired `permissions_current` resource keys under the same nullable-key rule; ENSv1 same-transaction registration setup repair enqueues affected `name_current` and `permissions_current` keys when it repairs a `RegistrationGranted` pre-state and orphans leaked registry-only setup control rows; ENSv1 authority-epoch registry-owner repair updates existing deterministic `AuthorityEpochChanged` after-state rows when replay adds the registry owner field; ENSv1 authority-epoch resolver-boundary repair enqueues affected `record_inventory_current` keys when it repairs deterministic `ResolverChanged` boundary rows; ENSv1 registry resolver before-state repair enqueues affected `record_inventory_current` keys when it repairs anchored `ResolverChanged` before-state rows; and ENSv1 wrapper-token before-state repair updates existing deterministic `TokenControlTransferred` before-state rows when replay replaces a stale pre-wrapper authority kind or stale previous wrapper owner with the current replay-derived value. ENSv1 reverse primary-claim resolver before-state repair has no projection key to invalidate because the repaired row is intentionally unanchored; it records only a normalized-event change. These authority repair paths record normalized-event changes so downstream projections can refresh. Label-preimage insertion is another bounded storage-owned invalidation path: new retained labelhashes enqueue `children_current` keys for known parent surfaces that have historical canonical ENSv1 or Basenames registry child edges using that labelhash, so later projection rebuilds can replace unknown-label placeholders. Read-safe parent `name_surfaces` insertion or refresh also enqueues `children_current` for retained canonical registry child edges under that parent, so child enumeration does not depend on whether the registry edge, label preimage, or parent surface arrived first. `label_preimages` rows are proof-checked by normalizing the candidate label and recomputing the keccak labelhash; once retained, the mapping is durable even if the source event or surface later becomes noncanonical. Canonicality still gates the registry child edge and exact-name surface rows that projections publish. Adapters still write identity rows and normalized events only; they do not write projection rows directly.[^v1-l2rev-base-deploy][^v1-l2rev-base-args][^v1-l2rev-event][^bn-revreg-l12][^bn-revreg-l150]

ENSv1 same-resource registration-release repair updates existing synthetic `RegistrationReleased` before-state rows when replay recovers a different prior registrant for the same registrar resource. It records normalized-event changes for downstream projection refresh without changing resource keys.

For interval identity and normalized authority/permission events, adapters mint and reuse `resource_id`, `token_lineage_id`, and `surface_binding_id` per the architecture identity rules. Projection workers consume those rows; they do not infer alternate continuity or synthesize cross-resource permission carry.

## Manifests and discovery persistence

At minimum:

- `contract_instances` — one row per stable `contract_instance_id` with chain, contract kind, and provenance. Roots use the same identity family as other contract instances.
- `contract_instance_addresses` — time-ranged address attributes keyed by `contract_instance_id`. One id may carry multiple non-overlapping active ranges. Manifest-declared address ranges may carry nullable inclusive `start_block` metadata where the manifest supplied it.
- `discovery_edges` — keyed by `edge_id` with `from_contract_instance_id`, `to_contract_instance_id`, `edge_kind`, active range, provenance, canonicality.
- Materialized watch-plan rows keyed by `contract_instance_id` plus chain and range; root start nodes keyed by the root `contract_instance_id`. Address is a derived watch target, not the durable identity. An omitted `start_block` is persisted as null rather than coerced to zero.

Resolver-profile admission state (PublicResolver-generation profiles for ENSv1, `L2Resolver` compatibility for Basenames) is gated separately from contract-instance admission. It may be derived from existing discovery provenance, normalized resolver-discovery events, manifest contract roles, code-hash facts, and proxy/implementation edges; a dedicated profile-fact table is not required. Profile admission gates complete-family, resolver-overview, latest-only, authorization, and onchain-call parity claims for the affected resolver instance — not baseline generic resolver-event observation.[^v1-pres-l20][^v1-pres-l66][^bn-l2resolver-l4][^bn-l2resolver-l16][^bn-l2resolver-l29]

`manifest_alert_*` carries an observation identity, observation kind (`manifest_drift` or `proxy_implementation_drift`), lifecycle status, manifest version, source family, chain, contract-instance references, nullable proxy/implementation edge references, expected and observed code-hash or implementation-edge material, derived watch-plan metadata, first/last observed timestamps, and nullable remediation metadata. Writing it does not write `normalized_events`, mutate manifest truth, mutate discovery admission, change capability flags, or expose API state. A proxy implementation observation preserves the proxy `contract_instance_id`; implementation churn is represented by an observed or admitted edge, not by minting a replacement proxy identity.

## Backfill persistence

At minimum:

- `backfill_jobs` — one row per bounded backfill job with selected profile, chain, selector kind, resolved source identity, scan mode, declared range start and end, idempotency key, lifecycle status, failure metadata, timestamps.
- `backfill_ranges` — child range records with declared range bounds, last-completed checkpoint, lease owner, lease token, lease expiry, attempt counters, lifecycle status, failure metadata, timestamps. A new range initializes its checkpoint to one block before the declared start so resume starts at `checkpoint_block_number + 1`.
- Monotonic helper-owned checkpoint fields that let a worker resume after crash without widening the original range or reclassifying already admitted facts.

Operational finalized catch-up uses these same families. It may create many finite chunks, but each chunk preserves one immutable job shape and idempotency key. Capacity preflight (current Postgres size, writable free disk, configured object-cache budget) records explicit failure or paused state in existing lifecycle/failure metadata when capacity is insufficient.

The selector identity fields on a job:

- `selector_kind` — `whole_active_watched_chain`, `source_family`, or `watched_target_set`
- `source_family` — the requested family for `selector_kind=source_family`, otherwise null
- `requested_watched_targets` — caller-supplied watched targets for `selector_kind=watched_target_set`, otherwise empty
- `selected_targets` — the resolved materialized target set sorted by `source_family`, `contract_instance_id`, normalized address, effective from-block, effective to-block
- `source_identity_hash` — digest of `selector_kind`, `source_family`, `requested_watched_targets`, and `selected_targets`

Very large source-family jobs may persist compact selector identity instead of a full `selected_targets` array. Compact identity sets `source_identity_payload_format=selected_targets_digest_v1` and carries `selected_target_count`, `selected_targets_digest_algorithm`, `selected_targets_digest`, a first/last `selected_targets_sample`, and `source_identity_hash`. The digest input remains the sorted canonical `selected_targets` tuple.

Backfill idempotency is derived from deployment profile, chain, finite range, scan family, and source identity. It must not include the local manifest root path: moving the same selected manifest corpus between filesystem locations does not create new raw backfill work. Bootstrap checkpoint reuse follows the same rule by matching persisted source identity and contiguous range coverage rather than the literal idempotency-key text.

`effective_to_block` is finite for every persisted selected target — backfill jobs are finite at creation time. Bootstrap ranges start at each eligible target's manifest/discovery admitted start and end at the finite provider head observed at job creation. A watched target whose manifest-declared `start_block` is unknown is skipped by bootstrap; it leaves no synthetic block-zero, provider-history, recent-window, or job-start range in `backfill_*`.

### Backfill range checkpoint vs chain checkpoint

Backfill range checkpoints are operational state. They record only that bounded fetch/resume work reached a position in a declared range. They do not change any `canonicality_state` value and do not advance `canonical_head`, `safe_head`, or `finalized_head`.

Backfill raw admission still writes canonicality for the facts it admits. When the admitted historical range is already proven canonical, safe, or finalized by retained lineage or provider checkpoint evidence, new lineage, raw-fact, and normalized-event rows use `canonical`, `safe`, or `finalized` as appropriate rather than staying `observed` solely because the source was backfill. If the evidence is absent, the storage layer preserves the weaker state.

## Partitioning status

The current migrations create ordinary PostgreSQL tables for lineage, raw facts, normalized events, execution, identity, and projections. There is no checked-in table partitioning baseline yet. Row-volume control currently comes from explicit indexes, bounded backfill ranges, normalized-replay catch-up chunks, and retention/compaction policy. Any future partitioning change is a migration-bearing storage change and must update this section with the concrete table list and keys.

## Canonicality model

`chain_lineage` persists the recent reconciled block window keyed by `(chain_id, block_hash)`:

- `parent_hash`
- `block_number`
- `timestamp`
- checkpoint-promotion state

Header audit fields are optional retention. The default lineage contract omits `logs_bloom`, `transactions_root`, `receipts_root`, and `state_root`; reorg repair walks backward through `(block_hash, parent_hash)` until it reaches a stored matching ancestor, then marks the losing stored branch and dependent facts noncanonical from that point forward.

An auditable-header retention mode stores those fields in `chain_header_audit` keyed by the same `(chain_id, block_hash)` so inspection tooling can explain or cross-check fetched payloads. Their absence cannot prevent canonicality repair, checkpoint promotion, replay over retained selected facts, or projection rebuilds. When both stored and incoming audit rows carry the same field, mismatches are hard storage conflicts.

`raw_blocks` is not a durable table. Intake, replay, workers, adapters, audit helpers, and tests read block timestamps and canonicality from `chain_lineage` and read optional audit roots/bloom from `chain_header_audit` when auditable retention is enabled. Normal replay batches block-anchor admission once through the `chain_lineage` write boundary.

Every fact-derived row that can be invalidated by reorg carries `chain_id`, `block_number`, `block_hash`, `canonicality_state`, `observed_at`. `canonicality_state` values:

- `observed`
- `canonical`
- `safe`
- `finalized`
- `orphaned`

Rules:

- block hash is the identity anchor; block number is position only
- fork detection marks affected rows `orphaned`; it does not delete them
- reorg repair preserves lineage and normalized-event/identity canonicality for losing branches as audit truth; log-audit mode also preserves selected raw-log/transaction/receipt facts. Minimal raw-log retention may already have compacted those staging rows
- evictable payload-cache bytes or compacted staging rows do not erase canonicality, normalized-event provenance, or replay-critical evidence retained by the selected policy
- optional header audit fields are verified when both stored and incoming audit rows carry them. A minimal replay does not conflict with an existing auditable row solely because it omitted those fields
- projection rebuilds read rows that are `canonical`, `safe`, or `finalized` by default; history and audit tools may opt into `observed` and `orphaned` rows
- safe and finalized promotion is monotonic per chain

## Reorg repair

Reorg repair preserves audit truth: orphaned rows persist for explanation and rebuild, not deletion. The losing branch's lineage, identity rows, and normalized events stay canonical-state `orphaned` so explain and history routes can still reconstruct what was observed.

Execution cache rows follow the same hash-first canonicality rule. When reorg repair marks a block identity `orphaned`, synchronous indexer/reorg repair invalidates or deletes any reusable `execution_cache_outcomes` row whose dependency set includes that `(chain_id, block_hash)` or a boundary resolved through it. The invalidation makes the cached outcome ineligible for reuse; it does not delete raw facts, traces, steps, attachments, or any execution-owned audit artifact.

Reusable `execution_cache_outcomes` rows carry dependencies tied to explicit block-hash-bearing chain positions or boundaries. Rows that lack those dependencies fail closed.

## Replay semantics

Raw-fact normalized-event replay is indexer-owned orchestration over the adapter-owned `normalized_events` boundary. It selects bounded canonical raw facts and asks adapters to perform an upsert-only resync; it advances only its own `normalized_replay_*` cursor.

Whole-range replay is the default. Automatic bootstrap and automatic catch-up share one all-source chain cursor over persisted canonical raw facts in block order — adapter-owned identity histories combine registry, registrar, wrapper, resolver, and reverse-claim signals into one storage write boundary, so independent per-source-family cursors would tear those histories.

Normalized events are adapter-owned semantic transition rows, not guaranteed-stateless decorations on individual raw logs. Some adapters can derive every emitted row from the selected raw fact alone; those stateless adapters may be replayed from a block-hash selection. Stateful adapters derive `before_state`, resource continuity, authority metadata, resolver state, wrapper state, registrar expiry, and permission provenance from the chronological adapter history. For those adapters, replay that emits or compares transition rows must start from a valid closure boundary and carry adapter state across every physical page in the replay.

The current valid closure boundary for a stateful adapter is the earliest retained canonical raw fact for that adapter/source graph. A later boundary is valid only after a documented, durable, versioned adapter-state snapshot exists for that boundary. Existing `normalized_events`, `surface_bindings`, `resources`, projection rows, or API-visible state are not semantic input for deterministic stateful replay and must not be used as implicit snapshots.

Full-closure replay may persist adapter-private checkpoints under `normalized_replay_adapter_checkpoints` and `normalized_replay_adapter_checkpoint_items`. These rows are replay orchestration state: they may contain staged adapter observations, scan watermarks, and versioned payloads needed to resume an in-progress closure pass, but they are not raw facts, manifest truth, identity rows, projection input, or API state. A checkpoint can make process restarts resumable only for the adapter and checkpoint payload version that wrote it. For `ens_v1_unwrapped_authority`, the durable checkpoint payload is the adapter's private closure snapshot: dirty name histories, reverse-claim histories, learned name metadata, pending namehash observations, migrated-registry markers, flushed normalized-event counters, and the block-boundary watermark. To keep closure replay bounded, the adapter may flush already-emitted normalized events through the adapter-owned `normalized_events` upsert boundary at checkpoint boundaries, then persist the checkpoint with those event buffers cleared. Those flushed rows are not projection readiness, public API readiness, identity-row finalization, or a cursor boundary; projection workers still wait for the global `raw_fact_normalized_events` cursor and identity finalization. If a process exits after a flush but before the matching checkpoint save, restart may replay and upsert the same event identities again, and any differing payload remains a hard storage mismatch. A completed snapshot may remain after cursor advancement so the next closure target can extend from that private adapter boundary. Transient adapter checkpoints may be cleared after a successful closure pass only when they are not declared durable snapshot boundaries. Full-closure replay chooses physical pages by canonical raw-log event candidate count while preserving whole-block boundaries; adapter routing may then filter that page down to the watched or generic source events that the closure pass consumes. Implementation scan guards may limit one database range probe, but they are throughput guards rather than semantic 512-block replay windows. If a single block exceeds the configured candidate-log cap, the full block is still replayed as one page; the cap is not allowed to split a block or create a replay cursor. When a scan guard is reached before the candidate-log cap, the page may advance through empty or low-density whole blocks because no semantic boundary is created until the closure target completes. The global `raw_fact_normalized_events` cursor advances only after the closure adapter finalizes its adapter-owned writes through the requested target block. Automatic full-closure catch-up latches that requested target when the cursor is created and does not widen the same closure pass just because newer live raw facts arrive while it is running or after it completes; a later closure target requires an explicit cursor rewind/reset or a documented adapter-state snapshot boundary. A completed automatic catch-up may be followed by a separate `post_replay_live_adapter_backlog` operational cursor that live-normalizes canonical raw-log blocks already persisted after the latched replay target before normal live polling resumes adapter sync. That cursor scopes adapter routing from the selected raw-log emitters, is not a closure replay cursor, does not change the full-closure target, and remains replay-safe because it uses the same deterministic adapter-owned upsert path as live polling. Backlog normalization never replaces provider-backed live intake; the following live reconciliation still admits raw payloads for canonical blocks that were not already persisted. Source-scoped live and backlog discovery writes are bounded manifest/discovery mutations: they reconcile touched observation keys and the affected descendant branch only, while full-source discovery carry-forward remains a closure/full-reconciliation operation. ENSv2 registry/resource replay runs before ENSv2 registrar and resolver replay so contextual rows see stable registry/resource outputs; ENSv2 permissions replay then runs over the retained resolver-family raw-log history. These ENSv2 closure passes do not currently publish durable adapter-private snapshots, so a restart reruns the topologically ordered closure pass from the retained closure boundary and relies on idempotent normalized-event and identity upserts.

Contextual adapters are not stateful because of `before_state`; they are contextual because their emitted identity, row set, or payload depends on another adapter-owned identity/discovery output being stable. Empty `before_state` is not proof of stateless replay. Replay for these adapters is deterministic only after dependency closure is complete and stable, or inside a documented topologically ordered closure replay.

Batching is only a physical IO and throughput detail. Chunk size, log-count caps, whole-block replay pages, block-hash paging, process restarts, and cursor checkpoints do not create semantic replay boundaries for stateful or contextual adapters. If automatic replay cannot resume with a durable adapter snapshot, it restarts from the closure boundary. Source-scoped, per-target, and block-hash replay for a stateful or contextual adapter is operational repair only and fails closed unless the requested selection is proven closure-complete.

Current raw-fact normalized replay allows restricted block-hash/source-scoped replay only for adapters classified `stateless_raw_fact`. `stateful_closure_required` and `contextual_dependency_required` adapters fail closed for restricted replay unless the requested range starts at the retained closure boundary and the adapter has an implemented closure/dependency replay session. The central code contract is mirrored here:

| Adapter / producer | Model | Raw-fact restricted replay | Reason and proof |
| --- | --- | --- | --- |
| `block_derived_normalized_events` | `stateless_raw_fact` | Allowed | Preimage rows are decoded from selected canonical raw logs, manifest/source metadata, and decoder constants. Covered by idempotent normalized replay and block-derived adapter tests. |
| `ens_v1_reverse_claim` | `stateless_raw_fact` | Allowed | `ReverseChanged` rows are derived from selected reverse raw logs and immutable manifest/source metadata. ENSv1 Mainnet uses `ReverseClaimed`; Basenames primary-name value intake uses the ENSv1 Base `L2ReverseRegistrar` `NameForAddrChanged` log and emits the companion `RecordChanged(name)` claim observation from the same raw fact.[^v1-l2rev-event] Covered by block-range, source-scoped, and block-hash replay tests. |
| `ens_v1_subregistry_discovery` | `contextual_dependency_required` | Restricted replay denied; full retained closure replay allowed | Normalized rows include discovery-edge contract-instance context; raw-log selection alone is not stable dependency closure. |
| `ens_v1_unwrapped_authority` | `stateful_closure_required` | Restricted replay denied; full retained closure replay allowed | Authority transitions, `before_state`, resource continuity, resolver state, wrapper state, registrar expiry, and permission provenance require one ordered in-memory history across registry, registrar, wrapper, resolver, and related Basenames families. |
| `ens_v2_registry_resource_surface` | `stateful_closure_required` | Restricted replay denied; full retained closure replay allowed | Token/resource identity, suffix state, bindings, discovery observations, and regeneration intervals depend on canonical ordered registry history through the replay target. |
| `ens_v2_registrar` | `contextual_dependency_required` | Restricted replay denied; full retained closure replay allowed | Registrar rows resolve `logical_name_id` and `resource_id` from stable ENSv2 registry/resource output replayed through the same target. |
| `ens_v2_resolver` | `contextual_dependency_required` | Restricted replay denied; full retained closure replay allowed | Resolver rows resolve name/resource links from stable `name_surfaces` and `surface_bindings` replayed through the same target. |
| `ens_v2_permissions` | `stateful_closure_required` | Restricted replay denied; full retained closure replay allowed | Permission resources and role events depend on prior resolver resource-hint observations in canonical order through the replay target. |
| `manifest_normalized_events` | `contextual_dependency_required` | Not a raw-fact replay participant | Manifest rows derive from manifest, capability, code-hash, and discovery-edge corpus state rather than selected raw logs. |

Source-scoped or per-target replay is an operational repair mode. It narrows the raw-log selection and adapter source scope; it does not narrow canonicality, change persisted backfill job identity, delete raw facts from other sources, mutate discovery or manifests, or graduate coverage. Storage helpers, projections, API code, and inspection tooling do not synthesize normalized events outside this boundary.

Replay reads canonical durable hot facts first. It may use a retained durable cold payload only when an explicitly retained replay contract requires that payload. For block-scoped payloads it may use provider re-fetch only through the digest-checked, fail-closed cache-fill path.

Adapter-private replay checkpoint payloads are resumability state, not canonical event payloads. They may use versioned, lossless encodings for strings that PostgreSQL `jsonb` cannot store directly, and adapters must decode those snapshots before continuing deterministic replay.

Replay does not delete stale `normalized_events` or replace existing payloads for an already persisted normalized-event identity. The upsert path inserts absent rows and refreshes canonicality for matching identities; conflicting payloads remain mismatches except for the explicitly documented adapter-repair fields below. Adapter-owned identity rows may be marked `orphaned` only when those rows have no backing normalized event, were produced by the same adapter boundary, and would otherwise overlap the incoming identity interval.

Replay does not mutate `chain_*`, `raw_*`, `backfill_*`, `projection_*`, `execution_*`, manifests, discovery rows, public API state, or checkpoint promotion state.

### Adapter repair

Explicit adapter repair is narrower than replay and exists for deterministic adapter bugs where existing normalized-event rows can be proven to be the same adapter output but a documented field or adapter-derived identity component was encoded lossily. The triggering conflicted row matches the retained source identity; related-row repair is constrained to the same adapter, chain, logical name, canonical state, and documented repair boundary. Repair updates only the fields, identity components, stale-row orphaning, and stale-key invalidations listed below. In minimal raw-log deployments, repair may fetch exact historical logs directly from the configured provider or same-host Reth substrate without re-materializing `raw_logs`.

New repair work lands in the Rust repair framework, or in shared SQL functions invoked from that framework when SQL is the natural expression of the proof. Migration-only repair rewrites are reserved for schema/index/trigger changes or tightly bounded one-time invocations of the same guarded repair logic; they must not depend on `_sqlx_migrations.installed_on` or wall-clock cutoffs. Repair code that rewrites `event_identity` must include the same collision handling as the Rust framework, and repair code that widens or reopens a `surface_bindings.active_to` interval must be explicitly listed in this section with the proof that no successor interval is invalidated, the non-overlap guard, and the downstream invalidations it records. Historical pre-framework SQL repair migrations that widened or reopened intervals are remediation artifacts, not precedent for future repair policy; the known artifacts are `20260508203000_ens_v1_registrar_live_renewal_resource_repair.sql`, `20260508204000_ens_v1_registrar_registry_boundary_repair.sql`, and `20260514110000_ens_v1_recent_renewal_resource_repair.sql`.

The currently admitted normalized-event field repairs are:

- ENSv1 PublicResolver-compatible `TextChanged` payload repair: legacy generic `RecordChanged` rows with `record_family=text`, `record_key=text`, `selector_key=null` are rewritten to selector-specific `text:<key>` rows; selector-specific text rows missing a retained value have that value filled when the source log verifies against the indexed key hash.[^v1-text-l5][^v1-text-l21]
- ENSv1 registrar renewal resource repair: `ExpiryChanged`/`RegistrationRenewed` rows whose event identity, name, and payload match may update `resource_id` when replay recovers the stable registrar/wrapper resource anchor that an earlier replay encoded incorrectly. The old and repaired `resources` rows must be canonical ENSv1 registrar anchors for the same mainnet logical name and labelhash; when the normalized event payload does not carry `labelhash`, as with expiry-only rows, the resource provenance provides that equality check. The same repair map may update `before_state.expiry` on the repaired renewal/expiry row when the stale row had copied the renewal after-expiry into the before-state and the repaired resource provenance, or an earlier canonical registrar grant/renewal/expiry event on the repaired resource, proves the replayed pre-renewal expiry. It may also repoint later authority events on the stale resource to the repaired resource, rewrite `PermissionChanged` grant/revocation authority keys, preserve each current replay-batch renewal/expiry row's own replayed `before_state`, refresh older related renewal/expiry `before_state.expiry` from the repaired replay proof, and rewrite a stale `RegistrationReleased` event identity from the old authority key to the repaired authority key. If that repaired release identity already exists, the stale release row is marked `orphaned` instead of rewritten. The repair also orphans stale synthetic registrar grant/surface events plus old `resources`/`surface_bindings` scaffolding when no earlier canonical backing event still uses the old resource, and enqueues old and repaired resource keys for resource-keyed projections whose stale key is no longer derivable after the row is repointed.
- ENSv1 renewal before-state repair: `ExpiryChanged`/`RegistrationRenewed` rows may update only `before_state.expiry` when the source identity, namespace, logical name, registrar resource, source family, derivation kind, and `after_state` are unchanged. The stale before-expiry may be the renewal after-expiry, a later/current expiry retained on the same registrar resource, or an earlier stale expiry in the same retained registrar-resource history when both stale and replayed before-expiries are numeric and strictly less than the unchanged renewal after-expiry. The canonical mainnet registrar resource must still anchor the same logical name and labelhash, but its provenance expiry is not the proof for the repaired before-expiry because the resource can carry the current post-renewal expiry for the same registrar authority. Outside the bounded numeric same-resource case, the replayed before-expiry must match an earlier canonical ENSv1 unwrapped-authority registrar grant, expiry, or renewal event on the same resource. The repair records a normalized-event projection change and does not rewrite resource keys.
- ENSv1 registration-release before-state repair: synthetic raw-block `RegistrationReleased` rows may update only `before_state.registrant` when the namespace, logical name, registrar resource, source identity, `before_state.expiry`, and full `after_state` are unchanged, both old and replayed registrants are non-empty, and the registrar resource is a canonical mainnet resource for that logical name. The repair records a normalized-event projection change and does not rewrite resource keys.
- ENSv1 registry/registrar event-time resource repair: resolver, record, and permission rows may update `resource_id` when replay recovers the registry-only resource that was authoritative at the event block but an earlier replay attached the row to a later registrar/wrapper resource, to a legacy registry-only resource keyed only by labelhash, or to no resource anchor; they may also drop a later registrar/wrapper resource when replay proves there is no resource anchor at the event block. Repaired registry-only resources must be canonical mainnet resources for the same logical name and leftmost labelhash; legacy registry-only sources are admitted only as stale labelhash-key collisions and are rewritten to the node/namehash-scoped registry-only resource. A nullable repaired resource is admitted only when the stale resource is a later mainnet registrar/wrapper anchor for the same logical name and the source identity plus before/after state still match. A null-to-resource repair is admitted only when the source identity, logical name, and event kind still match; before/after state must still match except registry `ResolverChanged` rows may combine the resource repair with a `before_state.resolver` update from JSON `null` to a lower-hex resolver address when the after-state namehash and resolver are lower-hex values. The referenced `resource_id` remains normal adapter-owned identity output, and downstream projection publication remains gated by the corresponding identity rows. Same-transaction registration setup is excluded from that registry-only rewrite: when a resolver/record/permission row precedes a later `RegistrationGranted` for the same registrar resource in the same transaction, the row remains attached to that registrar resource because replay intentionally defers those namehash observations until the registration anchor is known. Registrar-family permission rows may also be repointed from a stale registry-only resource to that registrar resource only when the registrar resource, authority key, and later `RegistrationGranted` row prove the same block/transaction ordering. Selector-specific text `RecordChanged` repairs may combine a `resource_id` update with a `value`-only after-state repair; the value-bearing after-state is preserved so event-time anchor repair does not erase a previously retained or newly replayed text value. If the renewal resource repair already repointed a related resolver/record row earlier in the same upsert transaction, the registry/registrar event-time repair treats the row as repaired when the current row now matches the replayed resource and state. `AuthorityTransferred` repairs may update only `before_state.owner` when the source identity, canonical mainnet resource, logical name, `resource_id`, and `after_state` are unchanged; a JSON `null` owner may be upgraded to a concrete owner, while an incoming JSON `null` owner is accepted as compatible but must not erase a retained concrete owner. The only admitted Basenames Base extension of this repair is `basenames_base_registry` `AuthorityTransferred` on `base-mainnet`, with the same resource-anchor and `before_state.owner` rules for Basenames resources. `RecordVersionChanged` repairs may update only `before_state.record_version`, with or without a `resource_id` change, between `null` and the immediately previous numeric version when `after_state.record_version` is unchanged, the numeric previous version is exactly `after_state.record_version - 1`, and the source identity is unchanged. Permission repairs may update only `grant_source` and `revocation_source` authority objects between the old and repaired resource provenance, and they enqueue stale and repaired resource keys, or only the non-null key for nullable repairs, for affected resource-keyed projections.
- ENSv1 registry resolver before-state repair: anchored mainnet `ResolverChanged` rows from `ens_v1_registry_l1`/`ens_v1_unwrapped_authority` may update only `before_state.resolver` between JSON `null` and a lower-hex resolver address, or between lower-hex resolver addresses when the replayed before resolver equals the unchanged lower-hex `after_state.resolver`, when the source identity, logical name, canonical mainnet resource, `after_state`, and all other `before_state` fields are unchanged. The unchanged `after_state` must carry a lower-hex namehash and resolver address, and the resource provenance must still anchor the same logical name with `registrar`, `wrapper`, or `registry_only` authority. The repair records a normalized-event projection change and enqueues the affected `record_inventory_current` resource key.
- ENSv1 reverse primary-claim resolver before-state repair: unanchored mainnet `ResolverChanged` rows from `ens_v1_registry_l1`/`ens_v1_unwrapped_authority` may update only `before_state.resolver` between JSON `null` and a lower-hex resolver address when the source identity, `after_state`, and lack of `logical_name_id`/`resource_id` are unchanged. The unchanged `after_state` must carry an ENS primary-claim source whose reverse node equals the row namehash and whose claim provenance is the ENSv1 reverse registrar. The repair records a normalized-event projection change and does not enqueue resource-key invalidations because no stale projection key exists on the row.
- ENSv1 authority-epoch resolver-boundary repair: deterministic raw-block `ResolverChanged` rows whose `after_state.source_event` is `AuthorityEpochChanged` may update only `after_state.resolver` when the source identity, canonical mainnet resource, logical name, `resource_id`, `before_state`, and the rest of `after_state` are unchanged. The repair records a normalized-event projection change and enqueues the affected `record_inventory_current` resource key.
- ENSv1 same-transaction registration setup repair: legacy rows may update a `RegistrationGranted.before_state` from an inferred registry-only authority to no prior authority when replay proves earlier registry owner observations in the same transaction were deferred setup for that registration. The repair may orphan leaked same-transaction `AuthorityTransferred` and `PermissionChanged` rows plus synthetic registry-only boundary rows that were minted from the setup observation against a registry-only resource for the same logical name. It enqueues the repaired name key and affected registry-only/registrar resource keys for projection rebuilds.
- ENSv1 wrapper-token before-state repair: deterministic `TokenControlTransferred` wrapper rows may update only `before_state.authority_kind` between stale `registrar`, `registry_only`, or JSON `null` values and current replay-derived `registrar`, `registry_only`, or JSON `null` values, or only `before_state.from` between lower-hex previous-owner addresses, when the source identity, metadata, `after_state`, and all other `before_state` fields match. The repair records a normalized-event projection change so downstream projections can refresh.
- Basenames primary-claim source repair: `RecordChanged(name)` claim-observation rows for `basenames_base_primary` may update only `after_state.primary_claim_source` when the stored tuple uses the old Basenames `ReverseRegistrar`/coin type `60`, while replay recovers the ENSv1 Base `L2ReverseRegistrar`/coin type `2147492101` tuple for the same address, namespace, reverse node, and reverse name.[^bn-readme-base-revreg][^v1-l2rev-base-deploy][^v1-l2rev-base-args][^v1-l2rev-event][^bn-revreg-l12][^bn-revreg-l150]

Repair does not write `raw_*`, `backfill_*`, projections, manifests, discovery rows, execution rows, or public API state directly. Field repairs append a normalized-event projection change. Repair paths also enqueue bounded stale-key invalidations when the stale projection key can no longer be derived after the normalized event is rewritten or when an anchored resource projection should be refreshed immediately: Basenames primary-claim source repair enqueues old and repaired `primary_names_current` keys; ENSv1 registrar renewal and ENSv1 or Basenames Base registry/registrar event-time resource repairs enqueue stale and repaired resource keys for affected resource-keyed projections, with nullable-resource repairs enqueueing only the non-null resource key; ENSv1 registry resolver before-state and authority-epoch resolver-boundary repairs enqueue affected `record_inventory_current` resource keys; and ENSv1 same-transaction registration setup repair enqueues the repaired `name_current` key plus affected `permissions_current` resource keys. Unanchored ENSv1 reverse primary-claim resolver before-state repair records only the normalized-event projection change. ENSv1 renewal repair updates to `surface_bindings` use the storage-owned surface-binding repair trigger to enqueue affected name/address keys.

### Bulk-load index deferral

During fresh normalized replay — current projection tables empty, normalized replay cursor not at target — the indexer may defer normalized-event indexes that exist only for projection/API readback while keeping replay-required indexes for event identity, reverse-claim lookup, and latest resolver/version preloads. Deferred indexes are recreated before projection rebuilds or API-ready declared reads complete.

`current_projection_replay_status` rows let worker restarts resume from the first unfinished projection family instead of restarting bootstrap/full replay from the start. They are worker-owned operational progress: not API truth, not projection data, not live-readiness state, and ignored unless the recorded replay version is still current and the recorded normalized target covers the requested replay target.

`projection_invalidations` rows are the durable key-scoped work queue for projection refreshes. `projection_normalized_event_changes` is the append-only downstream input for normalized-event inserts and canonicality-state updates; migrations install the forward log and trigger without bulk-copying historical `normalized_events`. `projection_apply_cursors` rows track consumed `change_id` watermarks for that input. Manifest, execution, and other non-normalized-event invalidation producers write the same queue directly. The primary key is `(projection, projection_key)`; repeated invalidations for the same key update the row generation, clear retry metadata, return the row to `pending`, and release any stale claim so an older apply cannot erase newer work. Projection workers claim and apply rows in projection dependency order, then delete only the claimed generation. Claims are leases with retry recovery, so rows claimed by a stopped worker become eligible again after the retry delay rather than requiring manual queue repair. Rows that fail the same claimed generation five times are removed from the live queue and copied to `projection_invalidation_dead_letters` with `state='dead_letter'`, the failure reason, timestamps, attempt count, and original queue identity for operator inspection. Dead-letter rows are durable operational evidence, not claimable work.

## Projection storage rules

Every current-state projection row carries provenance pointers, manifest version, relevant chain positions, canonicality summary, and last-recomputed timestamp.

Current projection timestamp fields are representable Unix-second values or `null`. ENSv2 `type(uint64).max` expiry observations project as `null` rather than a fabricated far-future timestamp; upstream uses that value for never-expiring reverse names, while registry renewal can carry any non-decreasing `uint64` expiry.[^v2-reverse-max-expiry][^v2-registry-renew-expiry] Numeric values that do not fit the projection timestamp representation are not converted into public projection timestamps.

Projection tables may be truncated and rebuilt from canonical facts plus normalized events.

Historical projection materializations are projection-owned caches, not truth. When a worker materializes an `at` or `chain_positions` snapshot, the rows are keyed by the normal projection key plus exact chain-position context or an equivalent snapshot key. They may be bounded and evicted by policy; absence returns `stale`. A historical materialization must never overwrite a newer current row in place, and the API must never fill a missing historical projection from raw facts or provider data.

Exact-name snapshot selection is a storage read boundary, not a new family. The API resolves `at`, explicit `chain_positions`, and `consistency` to one concrete `ChainPositions` object, then reads only projection rows and execution outputs eligible for that exact object. `name_current`, `coverage_current`, `surface_bindings_current`, `permissions_current`, and `record_inventory_current` retain enough chain-position context for the API to reject mismatched joins rather than combine rows from different snapshots.

If the selected positions are valid but no eligible projection or persisted execution output exists, the serving path returns the documented `stale`, `unsupported`, or `not_found` API state. It does not read raw facts, adapter-owned identity/event rows, or provider data directly to fill the public response.

## Execution storage

Inline in Postgres for small payloads:

- request metadata
- response digests
- decoded final values
- failure reasons

Large gateway bodies, metadata responses, and trace attachments are not persisted to a separate object store today. Execution may retain digests and trace metadata in Postgres, but adding durable external payload storage would be a migration-bearing storage change.

`execution_traces` and `execution_steps` preserve what was executed and why.
Normal `execution_cache_outcomes` writes record whether a verified outcome can
be reused under its request key, manifest versions, and block-hash-bearing
dependency boundaries. The API on-demand route exception and the
reorg-invalidation exception above are the only non-execution-worker write paths
for these execution-owned rows.

Exact block-anchored `raw_call_snapshots` used by verified resolution stay in
the intake-owned `raw_*` family. Execution persistence, including the API
on-demand route exception, may hand off candidate snapshots only through the
raw-fact boundary, only for the exact requested chain position, and only for
support classes that admit them. `execution_traces`, `execution_steps`, and
`execution_cache_outcomes` do not own those rows.

Before a verified-resolution selector persists as a supported reusable outcome, execution reloads from storage the exact manifest versions for the request, the same declared topology snapshot a mixed route would serve, and any resolver-profile admission state required by participating resolver-local fact families. The frozen support class derives from those stored inputs and matches the persisted trace and cache key. If those inputs are absent or do not re-establish one frozen class, the trace remains a durable audit artifact but the selector does not persist as a supported reusable outcome.

## Read-only inspection tooling

Worker-owned, read-only operational tooling reads storage audit helpers and renders stable JSON. It does not create public `v1` routes, mutate state, fetch fresh chain data, or bypass API read boundaries.

- `bigname-worker inspect canonicality --chain-id <id> --block-hash <hash>` — for a stored block: lineage, parent hash, block number, canonicality state, optional header-audit presence, raw fact counts, payload-cache metadata counts/digests where retained, normalized-event counts.
- `bigname-worker inspect stored-lineage-range --chain-id <id> --from <block> --to <block>` — lists only lineage rows already stored for the requested chain and finite block range, ordered by `(block_number, block_hash)`. Renders chain id, block number, block hash, parent hash, canonicality state, timestamp, and stored promotion markers per observed block. Nullable fields render as `null`. Does not infer missing heights, gaps, span-wide canonicality, or completeness.
- `bigname-worker inspect backfill-job --backfill-job-id <id>` — resolves one persisted job and its child ranges. Renders job lifecycle, declared range, selector kind, resolved source identity, idempotency key, timestamps, failure metadata, and a `ranges` array sorted by range bounds and id.
- `bigname-worker inspect execution-trace --execution-trace-id <id>` — reads `execution_traces`, `execution_steps`, and trace-attachment metadata for one stored trace.
- Manifest-drift and proxy-alert inspection — joins stored alert observations to manifest/discovery identifiers, code-hash facts, proxy/implementation edges, and derived watch-target metadata. Does not fetch fresh chain state, create alerts, mutate alert lifecycle, mutate manifest truth, or change capability flags.

## Migration rules

- Schema changes land through checked-in migrations only.
- Append-only tables prefer additive changes over destructive rewrites.
- Backfill job and range checkpoint storage lands as additive `backfill_*` tables or additive columns; it does not overload `chain_lineage`, projection job state, or public API tables.
- Projection tables may be recreated when the rebuild path already exists.
- Migrations that change a shared interface require the companion doc update first.
- If `CREATE INDEX CONCURRENTLY` leaves an `INVALID` index, the runbook is a later `-- no-transaction` migration that `DROP INDEX CONCURRENTLY IF EXISTS` for the invalid name before recreating or replacing it; do not rebuild by editing an already-applied migration.

## Repository ownership

- Storage owns migrations and query primitives.
- Storage owns backfill job/range helper primitives for idempotent create, reserve, advance, complete, and fail transitions.
- Worker/backfill code owns operational writes to `backfill_*` through those helpers, including finalized catch-up chunk creation and capacity pause/failure metadata.
- Adapters own inserts into identity and `normalized_events` tables.
- Projection workers own materialized read models.
- Execution workers own trace and step writes plus normal cache outcome writes,
  with the API on-demand verified-resolution product-route exception for
  selected-snapshot cache misses.
- Synchronous indexer/reorg repair owns only `execution_cache_outcomes` deletes/invalidations tied to orphaned block dependencies.
- Raw-fact normalized-event replay is indexer-owned orchestration over the adapter-owned `normalized_events` boundary; selected-target replay scopes are operational scan bounds and do not change adapter ownership.
- Normalized replay cursor and adapter-checkpoint storage is indexer-owned operational state for resuming bounded replay; it does not define canonicality, widen backfill jobs, or change adapter event ownership.
- Intake owns durable hot raw-fact writes, including admitted exact-block
  `raw_call_snapshots` handed off by execution persistence, plus optional
  payload-cache metadata. Replay and inspection tooling may dereference
  object-backed cache or re-fetch provider payloads only through the
  digest-checked, fail-closed boundary.
- API code does not query raw-fact tables directly except for explicit audit endpoints.
- Canonicality, raw-fact, stored-lineage-range, backfill-job, and execution-trace inspection tooling is worker-owned and read-only over storage audit helpers; none expose public `v1` routes.
- Manifest drift and proxy alerting tooling is worker-owned observation over `manifest_*`, `discovery_*`, code-hash facts, proxy/implementation edges, and derived watch targets. Its live audit path writes only `manifest_alert_*`; its read-only inspection path renders those observations as operational JSON without writing `normalized_events` or mutating manifest/discovery/projection/API state.

---

[^v1-pres-l20]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^v1-pres-l66]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L66 @ ens_v1@91c966f)
[^v1-text-l5]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f)
[^v1-text-l21]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L21 @ ens_v1@91c966f)
[^v1-registrar-grace]: (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L161 @ ens_v1@91c966f)
[^v1-l2rev-base-deploy]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-l2rev-base-args]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f)
[^v1-l2rev-event]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
[^bn-readme-base-revreg]: (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc)
[^bn-revreg-l12]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
[^bn-revreg-l150]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
[^ens-subgraph-label-null]: (upstream: .refs/ens_subgraph/src/utils.ts:L76 @ ens_subgraph@723f1b6)
[^ens-subgraph-name-null]: (upstream: .refs/ens_subgraph/src/resolver.ts:L85 @ ens_subgraph@723f1b6)
[^ensnode-null-label]: (upstream: .refs/ensnode/packages/enssdk/src/lib/types/ens.ts:L92 @ ensnode@2017ae6)
[^graph-ens-rainbow-table]: (upstream: .refs/ens_rainbow/src/main.rs:L36 @ ens_rainbow@bc44492)
[^graph-ens-rainbow-hash]: (upstream: .refs/ens_rainbow/src/main.rs:L50 @ ens_rainbow@bc44492)
[^v2-reverse-max-expiry]: (upstream: .refs/ens_v2/contracts/src/reverse-registrar/StandaloneReverseRegistrar.sol:L176 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/reverse-registrar/StandaloneReverseRegistrar.sol:L177 @ ens_v2@554c309)
[^v2-registry-renew-expiry]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L249 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L254 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L257 @ ens_v2@554c309)

[^bn-l2resolver-l4]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc)
[^bn-l2resolver-l16]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc)
[^bn-l2resolver-l29]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc)

[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309)
[^v2-iperm-l67]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L67 @ ens_v2@554c309)
[^v2-iperm-l72]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L72 @ ens_v2@554c309)
[^v2-events-l69]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69 @ ens_v2@554c309)
[^v2-pr-l28]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L28 @ ens_v2@554c309)
[^v2-pr-l203]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L203 @ ens_v2@554c309)
[^v2-pr-l216]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L216 @ ens_v2@554c309)
[^v2-pr-l237]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L237 @ ens_v2@554c309)
[^v2-pr-l451]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309)
[^v2-pr-l536]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L536 @ ens_v2@554c309)
