# Production Docker Operations

This runbook operates the current PostgreSQL, API, phase-runner, and optional
Caddy services. The deleted indexer and worker have no containers, commands,
heartbeats, replay jobs, or migration entrypoints.

## Validate configuration

Populate `.env.server` from `.env.server.example`, including the non-owner API
login and the phase-runner sources. Validate every overlay before changing a
running host:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml config

docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.public.yml config

docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml config
```

The reth overlay attaches the API and phase runner to the external
`eth-archive-node_default` network and bind-mounts the configured
`RETH_DATA_DIR` into the phase runner read-only. Create or start that network
and make the canonical reth database path readable before using the overlay.

## Before a from-zero or full-source walk

Do not start the walk until all of these checks pass:

1. The release commit has green cursor non-progress integration tests for all
   five phase names and valid repair-mode behavior.
2. `promtool test rules` proves the three-batch path, the two-batch/ten-minute
   path, and the legitimate exclusions.
3. The deployed metrics endpoint exposes
   `phase_runner_phase_batches_since_cursor_advance` and
   `phase_runner_phase_cursor_stall_age_seconds` for every configured chain and
   phase.
4. The host rule list contains `BignamePhaseRunnerPhaseNonProgress` and
   `BignamePhaseRunnerProgressMetricsMissing`.
5. The host retains the checked-in 15-second rule-group evaluation interval and
   configures `BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS` to at least 900
   seconds so the 13-minute two-batch bound remains valid.
6. The existing `severity=page` route passes the deployment's standard
   notification-path check.
7. Operators have the [manual halt procedure](pipeline-monitoring.md#phase-cursor-non-progress-response)
   open and know every active Compose overlay.
8. Record this acceptance statement in the walk log:

> **Phase livelock paging verified:** with the checked-in 15-second Prometheus
> rule interval, every executable phase/mode combination pages through the
> existing `severity=page` route no later than 13 minutes after its second
> committed [work-bearing batch](../glossary.md#work-bearing-batch) is confirmed
> at an unchanged [durable composite cursor](../glossary.md#durable-composite-cursor);
> a third pinned completion pages within 3 minutes. Intentional rescan
> and no-work shapes remain non-paging.

The equivalent operational acceptance is that livelock in any executable
phase/mode pages within 13 minutes after the second confirmed pinned completion,
or within 3 minutes after the third. Normal Ingest source movement, one Project
boundary replay, caught-up Live polling that reports no movement from the
starting durable cursor, no-head completion, capacity pause, and completed Verify revalidation do not page.

## Planned migration and fingerprint boundary

The image has no generic `migrate` command, so migrations are an operator step
run outside the containers. The migration runner is `sqlx-cli` against the
checked-in `migrations/` directory, from a checkout of the exact commit being
deployed and with the writer database URL:

```sh
cargo install sqlx-cli --no-default-features --features rustls,postgres  # once
git -C /path/to/bigname checkout <deployed-commit>
sqlx migrate info --source migrations --database-url "$BIGNAME_DATABASE_URL"
sqlx migrate run  --source migrations --database-url "$BIGNAME_DATABASE_URL"
```

Take and verify a backup first: `sqlx migrate run` applies every pending
version in order and has no down step. Raw facts dominate this database, so a
logical `pg_dump` is neither fast nor small; use the deployment's storage
snapshot or a filesystem-level base backup, sized against the current data
directory, and do not write it to the root filesystem. Run `sqlx migrate info`
again afterwards and confirm no version is still pending.

Do not hand-apply the SQL files with `psql`: the applied set is tracked in
`_sqlx_migrations`, and a file applied outside the runner leaves that ledger
out of sync. The runner still treats that version as pending, re-applying it
fails against the objects it already created, and the deploy stays down until
the ledger is reconciled by hand. A migration that drops legacy
`public`-schema tables is destructive and additionally requires an explicit
maintenance window.

Adding, editing, or deleting a covered interpreter input rotates the compiled
[interpreter content hash](../glossary.md#interpreter-content-hash);
`docs/storage.md` names what is covered. Covered files are hashed whole, so
editing a unit test that lives inside one rotates the hash as surely as
changing its production code. Do not mix new interpretation output with rows
published under the old hash. For such a release:

A planned [re-derivation boundary](../glossary.md#re-derivation-boundary) may
combine separately reviewed and separately merged PRs. Before merging the first
one, the release record must list the complete artifact set, intended per-chain
product and diagnostic deltas, generated watch-plan widening, historical-fetch
range, combined content hash, acceptance corpus, and rollback point. Do not
deploy any subset. In the test environment, run the slice-isolation gates and a
combined-artifact comparison that permits only the recorded deltas. Production
publication and readiness remain per chain rather than cross-chain atomic; keep
traffic drained for each affected chain until its own full re-walk, acceptance
checks, publication, and Verify phase succeed.

Before restoring traffic, run the separate
[production-scale benchmark gate](benchmark-gate.md) against a disposable
production-shaped copy and the drained new API generation. A small test database
run is not release evidence.

A phase-runner restart during a re-walk rebuilds its session cache with a full
ranked scan over all interpreted events. That scan is expensive at production
scale. Avoid restart loops; investigate the first interruption before restarting
the walk repeatedly.

The release containing Issue #400 adds baseline indexes and the versioned
schema-migrations
`20260813120000_reverse_hydration_attempt_state.sql` and
`20260813120100_reverse_hydration_attempt_state_validate.sql`, followed by
`20260814120000_project_redo_resolver_evidence.sql`. Fresh namespaces
receive the same objects from `schema-v2/baseline`. For an initialized
production namespace, keep the API and every phase-runner or one-shot Project
process stopped. Apply and validate the following concurrent indexes as step 3
below, then apply all three schema-migrations in order as step 4. The first adds the
three internal reverse-name polling selection columns, their sequence, and an
unvalidated all-null-or-complete constraint; the second validates that
constraint. The third adds the bounded Interpret-to-Project redo handoff for
resolver evidence. Before deploying the new binary, confirm the sequence, all
three columns, the handoff table, and its range index exist; also confirm that
`primary_names_current_reverse_hydration_attempt_check` has
`pg_constraint.convalidated = true`.

The release containing Issue #591 adds schema-migration
`20260827120000_normalized_events_ens_v1_record_node_resolver_idx.sql`. On an
initialized production namespace, build
`normalized_events_ens_v1_record_node_resolver_idx` concurrently in step 3
with the reviewed statement below and validate that it is ready and valid.
Then apply the schema-migration in step 4; its `IF NOT EXISTS` build is a no-op
when the concurrent index is already valid. Do not allow the versioned
schema-migration to perform the first build against a populated production
`normalized_events` table.

The release containing
`20260831150000_normalized_events_v2_expiry_scope_idx.sql` adds the bounded
ENSv2 expiry lookup used to select affected names during replay. On an
initialized production namespace, build both
`normalized_events_v2_expiry_scope_idx` and the widened
`normalized_events_subregistry_registration_history_idx` concurrently in step
3 with the reviewed statements below, and validate that both are ready and
valid. Then apply the schema-migration in step 4; its index builds are no-ops
when the concurrent indexes are already valid. Do not allow the versioned
schema-migration to perform either first build against a populated production
`normalized_events` table.

The release containing
`20260902140000_project_redo_expiry_roots.sql` adds the bounded
Interpret-to-Project handoff for logical names from deleted state-derived ENSv2
path-expiry releases. The follow-up
`20260902150000_project_redo_expiry_resources.sql` admits resource-only releases
and records the resource identifier when available. Apply both schema-migrations
in step 4 before deploying the new binary. Before starting any Project process,
confirm the handoff table, nullable identifier columns, and range index exist and
that the index is ready and valid with the query below.

The release containing
`20260904120000_project_redo_child_registration_history.sql` adds the bounded
Interpret-to-Project handoff for child and registry identifiers from deleted
ENSv1→ENSv2 [migration-registry](../glossary.md#migration-registry-wrapperregistry)
entry history. A fresh namespace receives the table and range index from
`schema-v2/baseline`; an initialized namespace needs the schema-migration
because its already-installed baseline is unchanged. Apply the schema-migration
in step 4 before deploying the new binary. Before starting any Project process,
confirm the handoff table and range index exist and that the index is ready and
valid with the query below.

The registry-operator projection release adds the ordered schema-migrations
`20260902160000_registry_operator_account_permissions.sql`,
`20260902160100_registry_operator_account_permissions_validate.sql`, and
`20260902160200_registry_operator_account_permissions_swap.sql`. Apply all
three in step 4 before deploying the new binary. Before starting Project,
confirm that the [account-permission state](../glossary.md#account-permission-state) table,
both account lookup indexes, the four [registry-owner binding](../glossary.md#registry-owner-binding)
summary columns, and the validated final binding constraint exist with the query below.

```sql
SELECT
    to_regclass('bigname_phase.project_redo_resolver_evidence') IS NOT NULL
        AS redo_handoff_exists,
    EXISTS (
        SELECT 1
        FROM pg_class index_relation
        JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
        WHERE index_relation.oid =
              to_regclass('bigname_phase.project_redo_resolver_evidence_range_idx')
          AND index_state.indisvalid
          AND index_state.indisready
    ) AS redo_handoff_range_index_ready;

SELECT
    to_regclass('bigname_phase.project_redo_expiry_roots') IS NOT NULL
        AS expiry_redo_handoff_exists,
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'bigname_phase'
          AND table_name = 'project_redo_expiry_roots'
          AND column_name = 'logical_name_id'
          AND is_nullable = 'YES'
    ) AS expiry_redo_logical_name_nullable,
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'bigname_phase'
          AND table_name = 'project_redo_expiry_roots'
          AND column_name = 'resource_id'
          AND is_nullable = 'YES'
    ) AS expiry_redo_resource_available,
    EXISTS (
        SELECT 1
        FROM pg_class index_relation
        JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
        WHERE index_relation.oid = to_regclass(
                  'bigname_phase.project_redo_expiry_roots_range_idx'
              )
          AND index_state.indisvalid
          AND index_state.indisready
    ) AS expiry_redo_handoff_range_index_ready;

SELECT
    to_regclass(
        'bigname_phase.project_redo_child_registration_history'
    ) IS NOT NULL AS child_registration_history_handoff_exists,
    EXISTS (
        SELECT 1
        FROM pg_class index_relation
        JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
        WHERE index_relation.oid = to_regclass(
                  'bigname_phase.project_redo_child_registration_history_range_idx'
              )
          AND index_state.indisvalid
          AND index_state.indisready
    ) AS child_registration_history_range_index_ready;

SELECT
    to_regclass('bigname_phase.account_permission_state_current') IS NOT NULL
        AS account_permission_state_exists,
    to_regclass('bigname_phase.account_permission_state_current_active_subject_idx') IS NOT NULL
        AS active_subject_index_exists,
    to_regclass('bigname_phase.account_permission_state_current_applicability_idx') IS NOT NULL
        AS applicability_index_exists,
    (
        SELECT count(*) = 4
        FROM information_schema.columns
        WHERE table_schema = 'bigname_phase'
          AND table_name = 'permissions_current_resource_summary'
          AND column_name IN (
              'registry_owner',
              'registry_contract',
              'registry_binding_provenance',
              'registry_binding_chain_positions'
          )
    ) AS registry_binding_columns_exist,
    EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid =
              'bigname_phase.permissions_current_resource_summary'::regclass
          AND conname =
              'permissions_current_resource_summary_registry_binding_check'
          AND convalidated
    ) AS registry_binding_constraint_validated;

SELECT EXISTS (
    SELECT 1
    FROM pg_class index_relation
    JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
    WHERE index_relation.oid =
          to_regclass('bigname_phase.normalized_events_v2_expiry_scope_idx')
      AND index_state.indisvalid
      AND index_state.indisready
) AS normalized_events_v2_expiry_scope_index_ready,
EXISTS (
    SELECT 1
    FROM pg_class index_relation
    JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
    WHERE index_relation.oid = to_regclass(
              'bigname_phase.normalized_events_subregistry_registration_history_idx'
          )
      AND index_state.indisvalid
      AND index_state.indisready
      AND pg_get_expr(index_state.indpred, index_state.indrelid, true)
          LIKE '%RegistrationReserved%'
) AS normalized_events_reserved_registration_history_index_ready;
```

Apply the following index statements one at a time with the writer role. Do not
wrap them in a transaction: PostgreSQL requires each `CREATE INDEX CONCURRENTLY`
to run as a top-level statement. The `normalized_events` builds are expected to
take hours at the production corpus size; monitor them through
`pg_stat_progress_create_index`, allow each build to finish, and confirm every
named index is valid in `pg_index` before continuing. A failed concurrent build
can leave an invalid index; drop only that exact invalid index and retry its
reviewed statement before proceeding.

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_chain_block_number_idx
    ON bigname_phase.normalized_events (chain_id, block_number);
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_resolver_alias_history_idx
    ON bigname_phase.normalized_events
       (chain_id,
        lower(COALESCE(after_state ->> 'resolver', before_state ->> 'resolver',
                       raw_fact_ref ->> 'emitting_address')),
        block_number DESC, normalized_event_id DESC)
    WHERE event_kind = 'AliasChanged'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_resolver_upgrade_history_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(after_state ->> 'proxy_address'),
        block_number DESC, normalized_event_id DESC)
    WHERE event_kind = 'Upgraded'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_pointer_after_resolver_history_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(after_state ->> 'resolver'), block_number, block_hash)
       INCLUDE (normalized_event_id)
    WHERE event_kind = 'ResolverChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_pointer_before_resolver_history_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(before_state ->> 'resolver'), block_number, block_hash)
       INCLUDE (normalized_event_id)
    WHERE event_kind = 'ResolverChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_permission_after_resolver_history_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(after_state #>> '{scope,resolver_address}'),
        block_number, block_hash) INCLUDE (resource_id)
    WHERE event_kind = 'PermissionChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state #>> '{scope,kind}' = 'resolver'
      AND resource_id IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_permission_before_resolver_history_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(before_state #>> '{scope,resolver_address}'),
        block_number, block_hash) INCLUDE (resource_id)
    WHERE event_kind = 'PermissionChanged'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND before_state #>> '{scope,kind}' = 'resolver'
      AND resource_id IS NOT NULL;
DROP INDEX CONCURRENTLY IF EXISTS bigname_phase.normalized_events_subregistry_registration_history_idx;
CREATE INDEX CONCURRENTLY normalized_events_subregistry_registration_history_idx
    ON bigname_phase.normalized_events
       (chain_id, (after_state ->> 'registry_contract_instance_id'),
        block_number DESC, normalized_event_id DESC, logical_name_id)
    WHERE event_kind IN (
              'RegistrationGranted', 'RegistrationReserved',
              'RegistrationRenewed', 'RegistrationReleased'
          )
      AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND logical_name_id IS NOT NULL
      AND after_state ->> 'registry_contract_instance_id' IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_ens_v1_record_node_resolver_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(after_state ->> 'node'),
        lower(COALESCE(NULLIF(after_state ->> 'resolver', ''),
                       NULLIF(raw_fact_ref ->> 'emitting_address', ''))),
        block_number, transaction_index, log_index, normalized_event_id)
    WHERE logical_name_id IS NULL
      AND source_family = 'ens_v1_resolver_l1'
      AND event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_v2_expiry_scope_idx
    ON bigname_phase.normalized_events
       (chain_id, ((after_state ->> 'expiry')::numeric),
        block_number, logical_name_id)
    WHERE logical_name_id IS NOT NULL
      AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND event_kind IN (
          'RegistrationGranted', 'RegistrationReserved',
          'RegistrationRenewed', 'RegistrationReleased', 'ExpiryChanged'
      )
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND jsonb_typeof(after_state -> 'expiry') = 'number';
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_basenames_record_node_resolver_idx
    ON bigname_phase.normalized_events
       (chain_id, lower(after_state ->> 'node'),
        lower(COALESCE(NULLIF(after_state ->> 'resolver', ''),
                       NULLIF(raw_fact_ref ->> 'emitting_address', ''))),
        block_number, transaction_index, log_index, normalized_event_id)
    WHERE logical_name_id IS NULL
      AND source_family = 'basenames_base_resolver'
      AND event_kind IN ('RecordChanged', 'RecordVersionChanged')
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
CREATE INDEX CONCURRENTLY IF NOT EXISTS name_surfaces_chain_block_number_idx
    ON bigname_phase.name_surfaces (chain_id, block_number);
CREATE INDEX CONCURRENTLY IF NOT EXISTS surface_bindings_chain_block_number_idx
    ON bigname_phase.surface_bindings (chain_id, block_number);
CREATE INDEX CONCURRENTLY IF NOT EXISTS resources_chain_block_number_idx
    ON bigname_phase.resources (chain_id, block_number);
CREATE INDEX CONCURRENTLY IF NOT EXISTS children_current_labelhash_idx
    ON bigname_phase.children_current
       (namespace, lower(labelhash), parent_logical_name_id, child_logical_name_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS name_current_resolver_idx
    ON bigname_phase.name_current
       ((declared_summary #>> '{resolver,chain_id}'),
        lower(declared_summary #>> '{resolver,address}'), logical_name_id)
    WHERE declared_summary #>> '{resolver,address}' IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS permissions_current_resolver_scope_idx
    ON bigname_phase.permissions_current
       ((scope_detail ->> 'chain_id'),
        lower(scope_detail ->> 'resolver_address'), resource_id)
    WHERE scope_kind = 'resolver'
      AND scope_detail ->> 'resolver_address' IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS record_inventory_current_resolver_idx
    ON bigname_phase.record_inventory_current
       ((provenance ->> 'chain_id'), lower(provenance ->> 'resolver_address'), resource_id)
    WHERE provenance ->> 'resolver_address' IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS primary_names_current_reverse_node_idx
    ON bigname_phase.primary_names_current
       ((claim_provenance ->> 'chain_id'),
        lower(claim_provenance ->> 'reverse_node'), address, coin_type, namespace)
    WHERE claim_provenance ->> 'reverse_node' IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS permissions_current_resource_wrapper_expiry_idx
    ON bigname_phase.permissions_current_resource_summary
       ((provenance ->> 'chain_id'),
        ((provenance -> 'wrapper_expiry_boundary' ->> 'expiry_seconds')::numeric),
        resource_id)
    WHERE provenance ? 'wrapper_expiry_boundary';
```

Record this manual index step, its start/end times, validity check, and the
pre/post published-head Project re-apply measurements in the release record for
the shared re-derivation boundary, alongside the complete artifact set. These
indexes are additive; rollback may leave them in place.

1. stop the API and phase runner;
2. take and verify a database backup;
   For the destructive Issue #411 Sepolia
   [source-role rollout](../glossary.md#source-role), also require the part-2
   release artifact, two distinct endpoint secrets, and an owner-approved
   rollback/restoration procedure before continuing. No narrower per-chain
   reset procedure is checked in. The
   [whole-schema replacement](../deployment.md#replacing-an-initialized-phase-schema)
   rebuilds every configured chain and is authorized only for a reviewed
   schema-migration that cannot preserve an initialized namespace; it does not
   authorize the Issue #411 source-role transition. Stop until part 3 supplies
   the reviewed per-chain reset and lossless preservation procedure. Once it is
   available, continue with steps 3–8 before that chain reset. Stop if any
   prerequisite is absent; never improvise a reset, data transfer, or rollback.
   Do not reset at this step. The optional one-shot redo instructions are not
   substitutes for the reset and full [source
   re-walk](../glossary.md#re-derivation-boundary). Execute the [owner-ratified
   rollout section](../deployment.md#owner-ratified-sepolia-source-role-rollout)
   at step 9;
3. for the release containing Issue #400, Issue #591, or
   `20260831150000_normalized_events_v2_expiry_scope_idx.sql`, or
   `20260902120000_normalized_events_basenames_record_node_resolver_idx.sql`,
   apply and validate the applicable concurrent baseline indexes above;
   otherwise skip this step;
   For the release containing
   `20260814130000_surface_binding_authority_arm.sql`, a populated phase schema
   cannot take the required `NOT NULL` column without the forbidden historical
   arm backfill. Before step 4, empty only the rebuildable binding rows and the
   two current projections that reference them:

   ```sql
   BEGIN;
   TRUNCATE TABLE
       bigname_phase.name_current,
       bigname_phase.address_names_current,
       bigname_phase.surface_bindings
       CONTINUE IDENTITY RESTRICT;
   COMMIT;
   ```

   Keep the API and phase runner stopped until steps 7 and 8 complete. This is
   a targeted derived-state reset, not a phase-schema replacement: it preserves
   raw facts, manifest rows and their sequence-assigned IDs, normalized-event
   identities, and the metadata needed to resume pre-boundary cursors. Do not
   clear or rename the phase schema at this boundary. Step 4 then applies the
   required column to the empty binding table, and the mandatory full-history
   Interpret and Project redos rebuild the cleared rows;
4. if the reviewed artifact set includes a versioned schema-migration, apply it;
   otherwise skip this step;
5. if an additive schema-migration created or changed a table, reapply and
   validate the verifier's `GRANT SELECT ON ALL TABLES IN SCHEMA
   bigname_phase` before starting any one-shot or long-running runner process;
   otherwise skip this step;
6. keep the long-running phase-runner supervisor stopped. If the generated
   watch plan widened an address/topic range, use the new artifact's one-shot
   Ingest redo over every widened range; otherwise skip this step. The command
   requires the full argument set or it is rejected before fetching anything:
   `phase-runner redo --chain <chain-id> --phase ingest --from-block <from>
   --to-block <to> --source <source> --metrics-bind-addr 0.0.0.0:9465`. Repeat
   `--source` for every configured intake-capable source key; the exact persisted
   cursor-key set is required.
   The CLI refuses an ingest redo without a source, and every redo requires the
   explicit block range. These recovery commands override redo's ephemeral
   metrics default with `0.0.0.0:9465` so the stopped supervisor's existing
   Prometheus target observes repair-mode progress. Use that fixed address only
   while the supervisor is stopped; another simultaneous redo needs its own
   stable target or the logged ephemeral default;
7. after any required Ingest redo succeeds, resume an already-audited Interpret
   redo with its existing token and exact active chain and range. Otherwise,
   invoke the exact required full-history Interpret redo without an attestation
   flag. If a current [manifest-authority
   marker](../glossary.md#manifest-authority-marker) makes the redo reject and
   print an invalidation token, run the required historical fetch for a widened
   watch plan, or complete the required review proving that the watch plan did
   not widen, then rerun the same chain and block range with
   `--attest-watch-set-coverage <token>`. If no marker exists, let the unflagged
   redo complete. Include `--metrics-bind-addr 0.0.0.0:9465` on this and the
   matching Project redo. Never invent a token, reuse one after completion, or
   use one for another redo. Do not use the unattended `run` path for an attestation;
8. complete the matching full-history Project redo while the supervisor remains
   stopped;
9. start the long-running phase runner only after those one-shot redos succeed.
   When the release also carries a versioned schema-migration or required
   replays, complete them before the Sepolia reset and full
   Ingest-through-Verify walk. Use only the reviewed part-3 per-chain reset after
   the preceding release work succeeds; stop if that procedure is unavailable.
   Before accepting any verification-only descriptor, review the deployment's
   endpoint-rotation record. If that endpoint served intake at any time during
   the retained walk—even under another key—stop and perform the reviewed
   affected-chain reset and full source re-walk under the intended
   endpoint-and-role configuration. Phase-runner does not persist endpoint
   history, so distinct current descriptors and the same-endpoint check cannot
   prove this temporal condition.
   Do not start the runner yet: deploy the part-2 binary and distinct secrets,
   validate the role-bearing configuration, perform the applicable reviewed
   reset, then run Sepolia through Verify before Live. Require
   `cross_checked`, confirm
   exactly one intake dRPC uses `ethereum_head` and start block zero, confirm
   only its cursor exists and the finalized Verify target is covered, and use
   provider/operator request accounting to confirm the verification-only key
   received zero Ingest/Live requests. For every affected chain, require the
   reviewed verification path rather than omitting or bypassing Verify. Under
   the source-role contract, other configurations use `cross_checked`
   with a distinct [verification-only](../glossary.md#source-role) dRPC,
   `node_checked` with a distinct verification-only Ethereum Mainnet reth, or
   `quick_synced` from the target-covering intake cursor without one;
10. confirm the phase state directly in the database while the API is still
   stopped — the `project` row in `chain_phase_state` current with no pending
   redo, and Verify success from the `verify` row for each affected chain plus
   the supervisor's Verify completion output (`/v2/status` cannot be used here
   because the API is stopped; after startup, the API accepts every known verification level at or above Sepolia's `quick_synced` floor and rejects unknown
   levels);
11. start the API built from the same commit and confirm `/v2/status` reports
   current phase state and no pending redo; and
12. run the release smoke and public-edge checks before undraining traffic.

If the phase schema itself must be replaced, follow
[`deployment.md`](../deployment.md#replacing-an-initialized-phase-schema). Do
not copy interpretation or projection rows from the old namespace into the new
one.

## Start or refresh services

Start the internal stack:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml up -d
```

Add the public edge when required:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.public.yml up -d
```

After changing only the Caddyfile, recreate the proxy explicitly:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.public.yml \
  up -d --no-deps --force-recreate public-proxy
```

## Verify health

Inspect container state and recent logs:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml ps

docker compose --env-file .env.server \
  -f docker-compose.server.yml logs --tail=200 api phase-runner postgres
```

Probe the host-private API listener:

```sh
curl -fsS http://127.0.0.1:3000/healthz
curl -fsS http://127.0.0.1:3000/v2/status
```

`api_status="ready"` proves the API can reach PostgreSQL. Aggregate readiness
also requires a current phase-runner heartbeat. Treat a stale phase heartbeat,
failed phase state, pending invalidation, or generation mismatch as an indexing
incident rather than masking it with an API restart.

## Pause and resume indexing

Pause the phase runner without stopping PostgreSQL or the API:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml stop phase-runner
```

Resume it with the same image and configuration:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml up -d phase-runner
```

The API remains reachable while indexing is paused, but health and status must
continue to report the stale or absent loop honestly.

## Recovery plays

Route from the first confirmed symptom:

- `interpret` crash-loops with an identity or derivation mismatch, or one
  chain's Interpret state is `failed` while the container stays up ->
  [stop and escalate before selecting a repair](#stop-and-escalate-an-interpreter-mismatch).
- a schema-migration deploy stops between the schema-migration and service
  start -> [recover an aborted schema-migration deploy](#recover-an-aborted-schema-migration-deploy).
- stored lineage, block canonicality, or verification disagrees ->
  [follow the reorg and verification incident play](#reorg-and-verification-incidents).
- rollback requires an older binary, deleted schema, or restored data ->
  [follow the rollback boundary](#rollback).
- `project` refuses a Mainnet or Sepolia projection with
  `dual_current_exact_name_authority` or `dual_current_child_authority` ->
  [follow the dual-current generation-failure runbook](dual-current-generation-failure.md),
  including its evidence-preserving child-failure escalation path.

Use the exact Compose file set deployed on the host for every recovery command,
retaining every active overlay. Replace `<compose-files>` below with that exact
set. The tracked baselines look like these; append any host-local overlays in
their deployed order:

- internal: `-f docker-compose.server.yml`;
- public: `-f docker-compose.server.yml -f docker-compose.public.yml`;
- reth: `-f docker-compose.server.yml -f docker-compose.reth-db.yml`; or
- public and reth: `-f docker-compose.server.yml -f docker-compose.public.yml
  -f docker-compose.reth-db.yml`.

Drain public traffic before running a recovery play. Use the deployment's
maintainer-approved edge procedure and confirm that no public request reaches
the API. The repository has no generic traffic-drain command; flag a missing
deployment-specific procedure and stop. Record this step as not applicable on
an internal deployment with no public edge.

### Stop and escalate an interpreter mismatch

Apply this play when the `interpret` phase crash-loops on an identity or
derivation mismatch, or when one chain's Interpret state is `failed` with that
error while another chain continues running.

1. Record the exact image ID of the existing phase-runner container as
   `<recovery-image>`. Do not use the mutable `latest` tag. If the command
   returns no container or more than one ID, stop and escalate:

   ```sh
   docker inspect --format '{{.Image}}' \
     "$(docker compose --env-file .env.server \
       <compose-files> ps -q phase-runner)"
   ```

2. Stop the phase runner:

   ```sh
   docker compose --env-file .env.server \
     <compose-files> stop phase-runner
   ```

3. Capture the full error and the affected chain from the logs. Record the
   affected block when the error reports one; do not infer a missing block.
   Choose `<incident-start>` early enough to include the first failure:

   ```sh
   docker compose --env-file .env.server \
     <compose-files> logs --since <incident-start> phase-runner
   ```

4. Capture the durable Interpret and Project status, recorded heads, pending
   redo state, and full `last_error`:

   ```sh
   docker compose --env-file .env.server \
     <compose-files> exec -T postgres \
     sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
   SELECT chain_id,
          phase_name,
          phase_status,
          current_block_number AS recorded_head,
          target_block_number,
          redo_in_progress,
          last_error,
          updated_at
   FROM bigname_phase.chain_phase_state
   WHERE phase_name IN ('interpret', 'project')
   ORDER BY chain_id, phase_name;
   SQL
   ```

5. Escalate the captured error before selecting a redo. If the Interpret row's
   `recorded_head` is `NULL`, keep the phase runner stopped: neither a scoped nor
   a full Interpret redo can start without a processed extent. Require a
   separately reviewed recovery that preserves non-rebuildable state; do not
   select phase-schema replacement from this symptom alone. If the error says
   to run `recompute-flags`, follow the
   [recompute-flags procedure](../deployment.md#phase-runner-configuration); do
   not run an ordinary Interpret redo. Otherwise, require the incident owner to
   identify the earliest affected stored block within the recorded Interpret
   extent. If that start cannot be established, skip the scoped redo and follow
   the full re-walk in step 7.
6. Run the approved Interpret redo from the affected start through the recorded
   Interpret head. Keep the long-running phase runner stopped. Interpret treats
   later rows as potentially dependent on earlier rows, so it replays from
   `<from>` through the recorded head and stamps the matching Project repair.
   Pin the one-shot container to `<recovery-image>`. Copy every intake-capable source descriptor for the affected chain exactly from the deployed configuration and repeat `--source` once for each descriptor. The descriptor uses the
   `CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK[:ROLE]=URL_ENV` form. The explicit
   arguments override the multi-chain `BIGNAME_PHASE_RUNNER_SOURCES` value for
   this one-off redo:

   ```sh
   BIGNAME_IMAGE=<recovery-image> \
     docker compose --env-file .env.server \
     <compose-files> run --rm --pull never --use-aliases --service-ports phase-runner \
     phase-runner redo --chain <chain-id> --phase interpret \
     --from-block <from> --to-block <recorded-interpret-head> \
     --metrics-bind-addr 0.0.0.0:9465 \
     --source <affected-chain-source> \
     [--source <additional-affected-chain-source> ...]
   ```

7. If the mismatch reproduces during the redo, keep the phase runner stopped
   and perform the full re-walk at the [planned re-derivation
   boundary](#planned-migration-and-fingerprint-boundary). Do not widen or
   repeat the scoped redo by guesswork. Stop this play; the full re-walk has its
   own image, restart, and verification steps.
8. If the redo succeeds, restart the phase runner with the same exact image.
   Let the supervisor resume Interpret and complete the stamped Project repair:

   ```sh
   BIGNAME_IMAGE=<recovery-image> \
     docker compose --env-file .env.server \
     <compose-files> up -d --pull never phase-runner
   ```

9. Repeat the status query from step 4 until Interpret advances beyond the
   pre-recovery recorded head without the same `last_error`. A
   mismatch in the next uncommitted batch can appear only after restart. If the
   same mismatch returns after the scoped redo, stop the phase runner and
   perform the full re-walk in step 7. Stop this play when escalating.
10. Require the Project row to report `phase_status = 'completed'`,
    `redo_in_progress = false`, and a `recorded_head` at or beyond the recovered
    Interpret head.
11. [Verify health](#verify-health) with the same Compose file set, then restore
    traffic through the same deployment-specific edge procedure used to drain
    it.

Never hand-edit identity or normalized-event rows. An in-place database update
is not a sanctioned recovery play on this stack.

### Recover an aborted schema-migration deploy

Apply this play when a deploy containing schema-migrations is interrupted
between the schema-migration step and service start, leaving the applied
schema-migration state and service versions desynchronized and the stack down.
The restore-or-re-roll decision was validated on 2026-07-29.

1. Keep the stack down. Never hand-apply pending SQL with `psql`, and never
   edit `_sqlx_migrations` to catch up.
2. If the schema-migration command stopped before it reported success, or its
   completion cannot be proven, treat it as half-applied. Keep the stack down
   and invoke the storage owner. Restore the verified pre-deploy backup required
   by the [existing backup steps](#planned-migration-and-fingerprint-boundary)
   with the deployment-specific restore procedure recorded for that backup.
   Do not substitute a generic repository command: storage snapshots and
   filesystem base backups use different restore mechanisms. Flag a missing
   deployment-specific restore procedure and stop. Do not complete or undo the
   partial change by hand, and do not continue until the restore is verified.
3. Do not resume at service start. Re-run the deploy from the top, from the
   exact target commit, so the applied schema-migration state and service
   versions move together. Repeat the applicable schema-migration checks and
   apply steps under the [schema-migration and fingerprint
   procedure](#planned-migration-and-fingerprint-boundary), but keep service
   start blocked.
4. Apply the release's reviewed re-derivation decision before starting
   services. Any deploy that changes the interpreter content hash requires the
   full re-walk under the [planned re-derivation
   boundary](#planned-migration-and-fingerprint-boundary). A semantic change
   outside that hash can also require re-derivation; review the surfaces listed
   under [interpretation replay](../storage.md#interpretation-replay). Keep the
   stack down until every required re-derivation step completes.
5. Treat saved Interpret and Project redo progress from the prior hash as
   invalid. Preserve pending Ingest or Verify redo markers and complete the
   exact persisted work named by the runner before starting `--phase all`; do
   not delete or skip those markers. When an all-phase redo fails, follow every
   phase-specific recovery command that it reports in dependency order.
6. If no re-derivation is required, or after the required re-derivation
   boundary completes, [start or refresh
   services](#start-or-refresh-services).
7. [Verify health](#verify-health) before restoring traffic.

### Reorg and verification incidents

Use the bounded `phase-runner inspect` commands for stored lineage, block
canonicality, and raw-event evidence. Use `phase-runner rewind` only after
identifying an exact stored readable ancestor. Verification mismatches require
the chain-scoped repair procedure in
[`deployment.md`](../deployment.md#verification-mismatch-repair); do not edit
immutable raw facts or mark a phase complete manually.

### Rollback

Run `scripts/rollback-smoke` from the exact rollback checkout before changing
binaries. A binary rollback does not recreate dropped legacy tables. If the
rollback needs deleted schema or data, restore the verified pre-migration
backup under a separately reviewed database rollback plan.
For the Issue #411 Sepolia rollout, a binary-only rollback also cannot parse
the role-bearing source configuration or preserve its readiness semantics; use
the owner-approved rollback and restoration path required by the
[rollout gate](../deployment.md#owner-ratified-sepolia-source-role-rollout).

Keep the public edge on its maintainer-approved policy throughout rollback and
re-run `scripts/public-edge-smoke` before restoring traffic.
