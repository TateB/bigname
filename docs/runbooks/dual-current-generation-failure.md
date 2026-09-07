# ENS dual-current projection-generation halt

This runbook is for the on-call operator responding when the
[Project phase](../glossary.md#projection-generation) refuses to publish on
Mainnet or Sepolia for either dual-current invariant. An exact-name halt means
both ENSv1 and ENSv2 [authority arms](../glossary.md#authority-epoch) retain a
current [surface binding](../glossary.md#surface-name-surface) after a proven,
activated [ENSv1→ENSv2 migration boundary](../glossary.md#migration-boundary).
A child halt means both arms retain a current parent-child relation after the
child's ENSv2 authority began; its admitted proof can instead be a positive
ENSv2 child registration
([`crates/project/src/integrity.rs:309-327`](../../crates/project/src/integrity.rs#L309-L327)).
This is a
[projection generation failure](../glossary.md#projection-generation-failure):
Project rolls back the whole attempted publication, and the phase runner then
attempts to write durable evidence. The assertion runs before publication
([`crates/project/src/engine.rs:62-82`](../../crates/project/src/engine.rs#L62-L82)),
and only the phase runner writes the post-rollback audit
([`apps/phase-runner/src/project_phase.rs:143-163`](../../apps/phase-runner/src/project_phase.rs#L143-L163)).

For `dual_current_child_authority`, use the stop-and-capture steps below, do not
run the exact-name repair queries, and escalate to the Project owner. Do not use
this play for an ordinary database error or a Verify failure. Do not start
recovery until the evidence-capture steps below are complete.

## Failure signature

The command and durable phase-row signature depend on whether Project was in
normal execution, a CLI-driven redo, or a required redo consumed by the
long-running supervisor. The audit signature is the same in all three cases.

### Normal Project execution

1. The phase-runner log has the terminal message
   `chain supervisor stopped after a terminal error`, with the affected
   `chain_id`, `error_kind=DataIntegrity`, and an `error` that begins with this
   exact text for the reported failure kind, with the bracketed values filled
   in. For an exact-name failure:

   ```text
   chain [chain-id] name [logical-name-id] holds current bindings on both authority arms after its activated ENSv1→ENSv2 migration boundary; projection generation for block [target-block] is not publishable
   ```

   For a child failure:

   ```text
   chain [chain-id] parent [parent-logical-name-id] and child [child-logical-name-id] state current relations on both authority arms after the child's ENSv2 authority began; projection generation for block [target-block] is not publishable
   ```

   The exact-name and child assertions construct those texts and attach their
   audit evidence at
   [`crates/project/src/integrity.rs:219-234`](../../crates/project/src/integrity.rs#L219-L234)
   and
   [`crates/project/src/integrity.rs:445-458`](../../crates/project/src/integrity.rs#L445-L458).
   A data-integrity error is non-retryable, so the chain supervisor stops rather
   than backing off and retrying
   ([`apps/phase-runner/src/runner.rs:193-232`](../../apps/phase-runner/src/runner.rs#L193-L232));
   the terminal structured log is emitted at
   [`apps/phase-runner/src/supervisor.rs:58-69`](../../apps/phase-runner/src/supervisor.rs#L58-L69).

2. `bigname_phase.chain_phase_state` has `phase_name = 'project'`,
   `phase_status = 'failed'`, and the same text in `last_error`. The failure
   transition sets those fields without replacing the last successfully
   published `current_block_number` or `current_block_hash`
   ([`apps/phase-runner/src/state.rs:370-407`](../../apps/phase-runner/src/state.rs#L370-L407)).

   Exception: after Project returns the invariant error, the runner separately
   records the failed phase. If that write fails, the terminal error appends
   `; additionally failed to record phase failure: [database error]`; the phase
   row can retain its preceding status and `/v2/status` need not yet show the
   direct `stale` mapping below. Capture the terminal error and any audit row,
   file the incident, and hold the chain. Do not edit the phase row or begin a
   redo while its durable state is unknown
   ([`apps/phase-runner/src/runner.rs:417-433`](../../apps/phase-runner/src/runner.rs#L417-L433),
   [`apps/phase-runner/src/error.rs:60-69`](../../apps/phase-runner/src/error.rs#L60-L69)).

3. `bigname_phase.project_generation_failures` has the matching
   `failure_kind`: `dual_current_exact_name_authority` or
   `dual_current_child_authority`, for the same chain, target block, and logical
   name (the child logical name for a child failure). The exact-name row includes
   the two binding/resource pairs and activated boundary position
   ([`crates/project/src/integrity.rs:159-194`](../../crates/project/src/integrity.rs#L159-L194));
   the child row includes the parent, authority proof, and both relation-event
   witnesses
   ([`crates/project/src/integrity.rs:345-419`](../../crates/project/src/integrity.rs#L345-L419)).
   It is append-only, and retrying the same conflict does not add a duplicate
   ([`apps/phase-runner/src/project_failure_audit.rs:6-24`](../../apps/phase-runner/src/project_failure_audit.rs#L6-L24)).

   Exception: if the audit insert itself fails, the terminal error and
   `last_error` keep the matching primary text above and append
   `; additionally failed to record projection generation failure: [database error]`.
   The Project phase still fails with `DataIntegrity`, but a new audit row is
   not guaranteed for either failure kind. Capture the complete secondary
   database error, query for a row without assuming one exists, file the
   incident, and hold the chain; do not infer evidence or begin a redo. The
   runner deliberately preserves the primary error when adding the audit-write
   failure
   ([`apps/phase-runner/src/project_phase.rs:143-163`](../../apps/phase-runner/src/project_phase.rs#L143-L163),
   [`apps/phase-runner/src/error.rs:60-69`](../../apps/phase-runner/src/error.rs#L60-L69)).

4. `GET /v2/status` reports the affected chain (`data.chains["1"]` for Mainnet
   or `data.chains["11155111"]` for Sepolia) as
   `status: "stale"` and keeps `indexed_block` at the most recent successful
   Project publication. A failed Project phase maps directly to `stale`
   ([`apps/api/src/v2/status.rs:175-226`](../../apps/api/src/v2/status.rs#L175-L226)).
   The route does **not** return `last_error`, the failure kind, or the audit
   payload; its response shape contains only aggregate and per-chain readiness
   fields ([`apps/api/src/v2/status.rs:18-42`](../../apps/api/src/v2/status.rs#L18-L42)).

### Project redo execution

When the assertion fires in Project `RunMode::Redo`, use the shared row, audit,
and API signatures in items 2–4. Determine item 1 from how the redo was invoked.

1. **CLI-driven redo:** an explicit Project redo and the automatic Project
   cascade inside `phase-runner redo` are one-shot work. The command exits
   nonzero with an aggregate error containing the affected chain,
   `(DataIntegrity)`, and the matching primary assertion text above. For one
   affected chain, the exact-name shape is:

   ```text
   Error: 1 chain supervisor(s) stopped on terminal errors: [chain-id] (DataIntegrity): chain [chain-id] name [logical-name-id] holds current bindings on both authority arms after its activated ENSv1→ENSv2 migration boundary; projection generation for block [target-block] is not publishable
   ```

   The child shape is:

   ```text
   Error: 1 chain supervisor(s) stopped on terminal errors: [chain-id] (DataIntegrity): chain [chain-id] parent [parent-logical-name-id] and child [child-logical-name-id] state current relations on both authority arms after the child's ENSv2 authority began; projection generation for block [target-block] is not publishable
   ```

   `redo_chains` returns each terminal error in `SupervisorReport`; it does not
   call the normal supervisor's structured-log helper
   ([`apps/phase-runner/src/runner_operator_redo.rs:384-425`](../../apps/phase-runner/src/runner_operator_redo.rs#L384-L425)).
   The CLI turns that report into the aggregate error after the redo returns
   ([`apps/phase-runner/src/main.rs:196-199`](../../apps/phase-runner/src/main.rs#L196-L199),
   [`apps/phase-runner/src/main.rs:332-346`](../../apps/phase-runner/src/main.rs#L332-L346)).
   Do not require a `chain supervisor stopped after a terminal error` log entry
   for this mode; that message belongs to normal supervised execution
   ([`apps/phase-runner/src/supervisor.rs:58-69`](../../apps/phase-runner/src/supervisor.rs#L58-L69)).

   **Supervisor-driven required redo:** after a restart or on the next
   live-follow iteration, the long-running runner's `run_spine_phase` detects a
   persisted required-redo marker and invokes Project with `RunMode::Redo`
   before normal Project execution
   ([`apps/phase-runner/src/runner_required_redo.rs:50-71`](../../apps/phase-runner/src/runner_required_redo.rs#L50-L71),
   [`apps/phase-runner/src/runner_chain.rs:72-87`](../../apps/phase-runner/src/runner_chain.rs#L72-L87),
   [`apps/phase-runner/src/runner_live_follow.rs:39-42`](../../apps/phase-runner/src/runner_live_follow.rs#L39-L42),
   [`apps/phase-runner/src/runner_live_follow.rs:180-183`](../../apps/phase-runner/src/runner_live_follow.rs#L180-L183)).
   A terminal assertion on that path emits the normal structured
   `chain supervisor stopped after a terminal error` entry with the primary
   assertion text. The Project row nevertheless has the resumable running-redo
   shape in item 2, not the failed normal-execution shape. The final aggregate
   CLI error is timing-dependent: the supervisor records the stopped chain
   immediately, but returns its `SupervisorReport` only after every configured
   chain task has returned; the CLI builds the aggregate only after that return.
   Another chain that remains live can therefore delay the aggregate
   indefinitely
   ([`apps/phase-runner/src/supervisor.rs:13-49`](../../apps/phase-runner/src/supervisor.rs#L13-L49),
   [`apps/phase-runner/src/supervisor.rs:58-69`](../../apps/phase-runner/src/supervisor.rs#L58-L69),
   [`apps/phase-runner/src/main.rs:105-117`](../../apps/phase-runner/src/main.rs#L105-L117),
   [`apps/phase-runner/src/main.rs:332-346`](../../apps/phase-runner/src/main.rs#L332-L346)).
2. The Project phase row remains `phase_status = 'running'` with
   `redo_in_progress = true`, `redo_mode = 'redo'`, and the active
   `redo_from_block_number`/`redo_to_block_number`. Redo start writes that shape,
   and failed redo completion records `last_error` without clearing the resumable
   marker or changing the lifecycle status
   ([`apps/phase-runner/src/redo_state.rs:155-198`](../../apps/phase-runner/src/redo_state.rs#L155-L198),
   [`apps/phase-runner/src/redo_state.rs:390-402`](../../apps/phase-runner/src/redo_state.rs#L390-L402),
   [`apps/phase-runner/src/redo_failure.rs:14-38`](../../apps/phase-runner/src/redo_failure.rs#L14-L38)).
   A directly started redo normally has the primary assertion text in
   `last_error`. An automatically stamped downstream redo can instead retain
   `required downstream redo: [reason]; last attempt failed: [primary assertion
   text]`: redo start changes the required-redo marker to its active form, then
   failed completion restores the required-redo prefix and appends the attempt
   failure
   ([`apps/phase-runner/src/redo_state.rs:191-215`](../../apps/phase-runner/src/redo_state.rs#L191-L215),
   [`apps/phase-runner/src/redo_failure.rs:21-49`](../../apps/phase-runner/src/redo_failure.rs#L21-L49)).
   Therefore, do not search only for `phase_status = 'failed'` or require
   `last_error` to begin with the primary text.

   Exception: if the runner cannot persist the failed redo state, the returned
   aggregate error keeps the primary assertion and appends
   `; additionally failed to record phase state after redo: [database error]`.
   For an automatically stamped downstream redo, the phase row can then retain
   `required downstream redo active: [reason]` without a
   `last attempt failed:` suffix. The aggregate error in item 1 still identifies
   the halt, and the complete audit key in item 3 still routes it unless the
   separately documented audit-insert exception also occurred. Capture those
   diagnostics and file-and-hold; do not treat the active prefix as proof of an
   unattempted handoff
   ([`apps/phase-runner/src/redo_state.rs:191-215`](../../apps/phase-runner/src/redo_state.rs#L191-L215),
   [`apps/phase-runner/src/runner.rs:371-389`](../../apps/phase-runner/src/runner.rs#L371-L389),
   [`apps/phase-runner/src/error.rs:60-69`](../../apps/phase-runner/src/error.rs#L60-L69)).
3. The audit row still has the matching exact-name or child failure kind and
   complete audit key described above. If its independent insert fails, use the
   same evidence-preserving file-and-hold exception; the phase row alone does
   not authorize recovery.
4. With a fresh phase-runner heartbeat and no other stale condition,
   `GET /v2/status` reports the chain as `degraded`, because an active Project
   redo is an explicit degraded condition. After the one-shot runner exits and
   its heartbeat exceeds the configured maximum age, the chain becomes `stale`;
   another stronger stale condition can also make it stale sooner. This API
   symptom is timing-dependent: heartbeat staleness is checked before the
   active-redo condition
   ([`apps/api/src/v2/status.rs:175-225`](../../apps/api/src/v2/status.rs#L175-L225)).
   The status route still does not expose the underlying error or audit key.

Distinguish this halt from other Project failures by the matching primary
message above, whether it is the whole `last_error` or the
`last attempt failed:` suffix, and an audit row with the corresponding
exact-name or child failure kind. Normal execution has a failed Project row;
redo execution has the active running redo row described above.
Either documented secondary write failure stops at
evidence-preserving escalation. Other Project errors carry no
projection-generation evidence, so the runner does not call the audit writer for them
([`apps/phase-runner/src/project_phase.rs:143-156`](../../apps/phase-runner/src/project_phase.rs#L143-L156)).
The same table also admits `dual_current_child_authority`. That kind enters this
runbook for evidence preservation only: skip the exact-name repair queries and
escalate to the Project owner
([`schema-v2/baseline/12_project_generation_failures.sql:19-22`](../../schema-v2/baseline/12_project_generation_failures.sql#L19-L22)).

## Stop and capture evidence

Keep the affected chain's long-running phase runner stopped. Before any redo,
restart, deploy, or code change, attach all of the following to the incident:

- the complete structured `chain supervisor stopped after a terminal error` log
  entry—from normal execution or a supervisor-driven required redo—or the redo
  CLI aggregate, including `chain_id`, `error_kind`, and exact error text;
- the phase-runner startup metadata log containing the build SHA and
  [interpreter content hash](../glossary.md#interpreter-content-hash), plus the
  immutable container image ID. The startup log emits those fields at
  [`apps/phase-runner/src/main.rs:266-272`](../../apps/phase-runner/src/main.rs#L266-L272);
- both the Interpret and Project phase rows, including their recorded heads,
  redo ranges, redo modes, `last_error`, and timestamps, plus any active
  [manifest-authority marker](../glossary.md#manifest-authority-marker)
  invalidation token and its reviewed coverage decision;
- every dual-current audit row for the affected chain and logical name (the
  child logical name for a child failure), including the failure kind, complete
  JSON evidence, and failure fingerprint;
- for an exact-name failure, the boundary event identity, ENSv1 and ENSv2
  binding IDs, resource IDs, block hashes, block/transaction/log positions,
  current lineage states, ENSv1→ENSv2 migration correlation IDs, and
  transaction hashes;
- for a child failure, the parent logical name, authority-proof identity and
  position, and both child-relation event identities, source families,
  normalized event IDs, positions, and failure-time canonicality recorded in
  the audit JSON; and
- for an exact-name failure, the raw transaction, receipt, and logs for every
  captured transaction hash. Preserve log order and emitting addresses.

The audit schema makes the chain/target/hash/interpreter/failure/fingerprint key
and the evidence payload durable
([`schema-v2/baseline/12_project_generation_failures.sql:1-25`](../../schema-v2/baseline/12_project_generation_failures.sql#L1-L25)).
Only one conflict is returned by one failed projection generation: the assertion
orders logical names and takes the first one
([`crates/project/src/integrity.rs:192-194`](../../crates/project/src/integrity.rs#L192-L194)).
After that name is repaired, another conflicting name can therefore be the next
failure at the same target. Route every Project failure during a redo or cascade
by the newly captured complete audit key: chain, target block number, target
block hash, interpreter content hash, failure kind, and failure fingerprint. Do
not declare the chain clean from one repaired row or identify a recurrence from
the fingerprint alone.

Run the following in a read-only `psql` session against `bigname_phase`. Replace
the sample values; never paste credentials into the incident.

```sql
BEGIN TRANSACTION READ ONLY;

\set chain_id 'ethereum-mainnet'

SELECT chain_id,
       phase_name,
       phase_status,
       current_block_number,
       current_block_hash,
       target_block_number,
       target_block_hash,
       input_content_hash,
       redo_in_progress,
       redo_attempt_generation,
       redo_mode,
       redo_previous_phase_status,
       redo_from_block_number,
       redo_to_block_number,
       redo_current_block_number,
       redo_current_block_hash,
       redo_target_block_number,
       redo_target_block_hash,
       last_error,
       COALESCE(
           last_error LIKE
               '%holds current bindings on both authority arms after its activated ENSv1→ENSv2 migration boundary; projection generation for block % is not publishable%'
           OR last_error LIKE
               '%state current relations on both authority arms after the child''s ENSv2 authority began; projection generation for block % is not publishable%',
           false
       ) AS has_dual_current_failure_signature,
       started_at,
       finished_at,
       updated_at
FROM bigname_phase.chain_phase_state
WHERE chain_id = :'chain_id'
  AND phase_name IN ('interpret', 'project')
ORDER BY phase_name;

SELECT audit.chain_id,
       audit.phase_name,
       audit.generation_token AS active_redo_invalidation_token,
       audit.authority_fingerprint,
       audit.redo_from_block_number,
       audit.redo_to_block_number,
       audit.attested_by,
       audit.attested_at
FROM bigname_phase.manifest_authority_attestations audit
JOIN bigname_phase.chain_phase_state phase
  ON phase.chain_id = audit.chain_id
 AND phase.phase_name = audit.phase_name
 AND phase.redo_in_progress
 AND phase.redo_from_block_number = audit.redo_from_block_number
 AND phase.redo_to_block_number = audit.redo_to_block_number
 AND phase.started_at = audit.attested_at
WHERE audit.chain_id = :'chain_id'
ORDER BY audit.attested_at DESC, audit.generation_token;

SELECT chain_id,
       target_block_number,
       target_block_hash,
       interpreter_content_hash,
       failure_kind,
       failure_fingerprint,
       logical_name_id,
       detected_at,
       jsonb_pretty(evidence) AS evidence
FROM bigname_phase.project_generation_failures
WHERE chain_id = :'chain_id'
  AND failure_kind IN (
      'dual_current_exact_name_authority',
      'dual_current_child_authority'
  )
ORDER BY detected_at DESC, logical_name_id;

ROLLBACK;
```

For `dual_current_child_authority`, preserve the matching row and its complete
audit JSON, then stop and escalate. The child candidate can combine a
registration event identity and transaction/log position with a later parent
subregistry block. Its durable audit payload therefore is not an authorized raw
transaction selector and this runbook does not guess the missing composite raw
evidence. The remaining evidence and repair queries are only for
`dual_current_exact_name_authority`.

Select an exact-name row by its complete audit key. The same semantic conflict
can have the same fingerprint at more than one target, so the fingerprint alone
is not a row selector. The next query
resolves the target and boundary lineage directly from the block coordinates in
the audit. The normalized-event join is only a secondary signal that the current
Interpret derivation still contains that exact boundary; a covering Interpret
redo may remove the old normalized event without removing lineage or raw facts
([`crates/interpret/src/write.rs:118-143`](../../crates/interpret/src/write.rs#L118-L143)).

```sql
BEGIN TRANSACTION READ ONLY;

\set chain_id 'ethereum-mainnet'
\set target_block 12345678
\set target_hash '0xreplace-with-target-block-hash'
\set interpreter_hash 'keccak256:replace-with-audit-row-hash'
\set failure_fingerprint 'replace-with-64-lowercase-hex-characters'

WITH failure AS (
    SELECT *
    FROM bigname_phase.project_generation_failures
    WHERE chain_id = :'chain_id'
      AND target_block_number = :target_block
      AND target_block_hash = :'target_hash'
      AND interpreter_content_hash = :'interpreter_hash'
      AND failure_kind = 'dual_current_exact_name_authority'
      AND failure_fingerprint = :'failure_fingerprint'
), boundary AS (
    SELECT event.*
    FROM failure
    JOIN bigname_phase.normalized_events event
      ON event.chain_id = failure.chain_id
     AND event.event_identity = failure.evidence #>> '{boundary,event_identity}'
     AND event.block_number =
         (failure.evidence #>> '{boundary,block_number}')::bigint
     AND event.block_hash = failure.evidence #>> '{boundary,block_hash}'
     AND COALESCE(event.transaction_index, -1) =
         (failure.evidence #>> '{boundary,transaction_index}')::bigint
     AND COALESCE(event.log_index, -1) =
         (failure.evidence #>> '{boundary,log_index}')::bigint
)
SELECT failure.chain_id,
       failure.logical_name_id,
       failure.target_block_number,
       failure.target_block_hash,
       failure.evidence #>> '{target,canonicality_state}'
           AS target_canonicality_at_failure,
       target_lineage.canonicality_state AS target_canonicality_now,
       failure.evidence #>> '{boundary,event_identity}'
           AS boundary_event_identity,
       (failure.evidence #>> '{boundary,block_number}')::bigint
           AS boundary_block_number,
       failure.evidence #>> '{boundary,block_hash}' AS boundary_block_hash,
       (failure.evidence #>> '{boundary,transaction_index}')::bigint
           AS boundary_transaction_index,
       (failure.evidence #>> '{boundary,log_index}')::bigint
           AS boundary_log_index,
       failure.evidence #>> '{boundary,canonicality_state}'
           AS boundary_canonicality_at_failure,
       boundary_lineage.canonicality_state AS boundary_lineage_now,
       boundary.event_identity IS NOT NULL
           AS boundary_present_in_current_derivation,
       boundary.event_kind AS current_boundary_event_kind,
       boundary.consumer_visibility AS current_boundary_visibility,
       boundary.migration_correlation_ids
           AS current_boundary_migration_correlation_ids,
       boundary.transaction_hash AS current_boundary_transaction_hash,
       boundary.canonicality_state AS current_boundary_row_canonicality,
       jsonb_pretty(boundary.after_state) AS current_boundary_after_state
FROM failure
LEFT JOIN bigname_phase.chain_lineage target_lineage
  ON target_lineage.chain_id = failure.chain_id
 AND target_lineage.block_number = failure.target_block_number
 AND target_lineage.block_hash = failure.target_block_hash
LEFT JOIN boundary ON TRUE
LEFT JOIN bigname_phase.chain_lineage boundary_lineage
  ON boundary_lineage.chain_id = failure.chain_id
 AND boundary_lineage.block_number =
     (failure.evidence #>> '{boundary,block_number}')::bigint
 AND boundary_lineage.block_hash =
     failure.evidence #>> '{boundary,block_hash}';

ROLLBACK;
```

Read the two stable binding IDs named by the audit and compare their current
rows with the captured audit fields. Capture this before any recovery action.
The audit stores the binding ID, resource ID, block number, position, and
canonicality observed at failure, but not the binding's block hash
([`crates/project/src/integrity.rs:159-184`](../../crates/project/src/integrity.rs#L159-L184)).
After a reorg and redo, Interpret can reanchor an orphaned binding under the
same stable ID by replacing its block hash and provenance
([`crates/interpret/src/write/identity.rs:264-310`](../../crates/interpret/src/write/identity.rs#L264-L310)).
The query therefore describes the current row and reports whether its non-fork
fields still match the audit; it does not reconstruct an old witness fork. The
`current_row_open_at_failed_target_time` expression mirrors the assertion's
end-of-target-block cutoff
([`crates/project/src/integrity.rs:46-50`](../../crates/project/src/integrity.rs#L46-L50),
[`crates/project/src/integrity.rs:87-126`](../../crates/project/src/integrity.rs#L87-L126)).

```sql
BEGIN TRANSACTION READ ONLY;

\set chain_id 'ethereum-mainnet'
\set target_block 12345678
\set target_hash '0xreplace-with-target-block-hash'
\set interpreter_hash 'keccak256:replace-with-audit-row-hash'
\set failure_fingerprint 'replace-with-64-lowercase-hex-characters'

WITH failure AS (
    SELECT *
    FROM bigname_phase.project_generation_failures
    WHERE chain_id = :'chain_id'
      AND target_block_number = :target_block
      AND target_block_hash = :'target_hash'
      AND interpreter_content_hash = :'interpreter_hash'
      AND failure_kind = 'dual_current_exact_name_authority'
      AND failure_fingerprint = :'failure_fingerprint'
), target_time AS (
    SELECT lineage.block_timestamp + interval '1 second' AS cutoff
    FROM failure
    JOIN bigname_phase.chain_lineage lineage
      ON lineage.chain_id = failure.chain_id
     AND lineage.block_number = failure.target_block_number
     AND lineage.block_hash = failure.target_block_hash
), witness AS (
    SELECT failure.chain_id, side, payload
    FROM failure
    CROSS JOIN LATERAL (
        VALUES
            ('predecessor'::text, failure.evidence -> 'predecessor'),
            ('successor'::text, failure.evidence -> 'successor')
    ) AS sides(side, payload)
)
SELECT witness.side,
       witness.payload ->> 'surface_binding_id' AS audit_binding_id,
       witness.payload ->> 'resource_id' AS audit_resource_id,
       (witness.payload ->> 'block_number')::bigint AS audit_block_number,
       (witness.payload ->> 'transaction_index')::bigint
           AS audit_transaction_index,
       (witness.payload ->> 'log_index')::bigint AS audit_log_index,
       witness.payload ->> 'canonicality_state'
           AS audit_canonicality_at_failure,
       binding.authority_arm,
       binding.surface_binding_id,
       binding.logical_name_id,
       binding.resource_id,
       binding.binding_kind,
       binding.active_from,
       binding.active_to,
       binding.block_number,
       binding.block_hash,
       binding.provenance,
       binding.canonicality_state AS binding_row_canonicality,
       binding_lineage.canonicality_state AS binding_lineage_now,
       resource.canonicality_state AS resource_row_canonicality,
       resource_lineage.canonicality_state AS resource_lineage_now,
       binding.resource_id = (witness.payload ->> 'resource_id')::uuid
           AND binding.block_number =
               (witness.payload ->> 'block_number')::bigint
           AND COALESCE((binding.provenance ->> 'transaction_index')::bigint, -1) =
               (witness.payload ->> 'transaction_index')::bigint
           AND COALESCE((binding.provenance ->> 'log_index')::bigint, -1) =
               (witness.payload ->> 'log_index')::bigint
           AS current_row_matches_audit_nonfork_fields,
       binding.active_from < target_time.cutoff
           AND (binding.active_to IS NULL OR binding.active_to >= target_time.cutoff)
           AS current_row_open_at_failed_target_time
FROM witness
LEFT JOIN bigname_phase.surface_bindings binding
  ON binding.chain_id = witness.chain_id
 AND binding.surface_binding_id =
     (witness.payload ->> 'surface_binding_id')::uuid
LEFT JOIN bigname_phase.resources resource
  ON resource.chain_id = binding.chain_id
 AND resource.resource_id = binding.resource_id
LEFT JOIN bigname_phase.chain_lineage binding_lineage
  ON binding_lineage.chain_id = binding.chain_id
 AND binding_lineage.block_number = binding.block_number
 AND binding_lineage.block_hash = binding.block_hash
LEFT JOIN bigname_phase.chain_lineage resource_lineage
  ON resource_lineage.chain_id = resource.chain_id
 AND resource_lineage.block_number = resource.block_number
 AND resource_lineage.block_hash = resource.block_hash
CROSS JOIN target_time
ORDER BY witness.side;

ROLLBACK;
```

Finally, capture the complete normalized-event, raw-transaction, receipt, and
raw-log evidence for every implicated transaction. This includes the whole
boundary transaction, not just the boundary position: predecessor cleanup can
occur earlier in that transaction. The boundary raw-fact lookup is pinned
directly to the block, transaction, and log coordinates in the audit rather
than to a normalized event that a redo may have replaced. Because each binding
witness lacks a recorded block hash, the query intentionally enumerates every
raw-log candidate at its recorded block, transaction, and log position across
all retained forks. From those candidates onward, every transaction, event,
receipt, and log join includes `block_hash`.
If one witness has candidates on multiple block hashes, or none, preserve that
result as ambiguity and file-and-hold; do not select a fork by current binding
state.
A [normalized event](../glossary.md#normalized-event) carries the transaction
hash, position, ENSv1→ENSv2 migration correlation IDs, and before/after state
([`schema-v2/baseline/05_normalized_events.sql:19-42`](../../schema-v2/baseline/05_normalized_events.sql#L19-L42));
the immutable raw tables are keyed by chain and block hash as well as transaction
identity
([`schema-v2/baseline/02_raw_facts.sql:1-29`](../../schema-v2/baseline/02_raw_facts.sql#L1-L29),
[`schema-v2/baseline/02_raw_facts.sql:77-107`](../../schema-v2/baseline/02_raw_facts.sql#L77-L107)).

```sql
BEGIN TRANSACTION READ ONLY;

\set chain_id 'ethereum-mainnet'
\set target_block 12345678
\set target_hash '0xreplace-with-target-block-hash'
\set interpreter_hash 'keccak256:replace-with-audit-row-hash'
\set failure_fingerprint 'replace-with-64-lowercase-hex-characters'

WITH failure AS (
    SELECT *
    FROM bigname_phase.project_generation_failures
    WHERE chain_id = :'chain_id'
      AND target_block_number = :target_block
      AND target_block_hash = :'target_hash'
      AND interpreter_content_hash = :'interpreter_hash'
      AND failure_kind = 'dual_current_exact_name_authority'
      AND failure_fingerprint = :'failure_fingerprint'
), witness AS (
    SELECT failure.chain_id,
           failure.logical_name_id,
           side,
           payload
    FROM failure
    CROSS JOIN LATERAL (
        VALUES
            ('predecessor'::text, failure.evidence -> 'predecessor'),
            ('successor'::text, failure.evidence -> 'successor')
    ) AS sides(side, payload)
), evidence_transactions AS (
    SELECT 'boundary'::text AS evidence_source,
           raw_log.chain_id,
           raw_log.block_number,
           raw_log.block_hash,
           raw_log.transaction_hash,
           raw_log.transaction_index
    FROM failure
    JOIN bigname_phase.raw_logs raw_log
      ON raw_log.chain_id = failure.chain_id
     AND raw_log.block_number =
         (failure.evidence #>> '{boundary,block_number}')::bigint
     AND raw_log.block_hash = failure.evidence #>> '{boundary,block_hash}'
     AND raw_log.transaction_index =
         (failure.evidence #>> '{boundary,transaction_index}')::bigint
     AND raw_log.log_index =
         (failure.evidence #>> '{boundary,log_index}')::bigint

    UNION ALL

    SELECT witness.side,
           raw_log.chain_id,
           raw_log.block_number,
           raw_log.block_hash,
           raw_log.transaction_hash,
           raw_log.transaction_index
    FROM witness
    JOIN bigname_phase.raw_logs raw_log
      ON raw_log.chain_id = witness.chain_id
     AND raw_log.block_number =
         (witness.payload ->> 'block_number')::bigint
     AND raw_log.transaction_index =
         (witness.payload ->> 'transaction_index')::bigint
     AND raw_log.log_index =
         (witness.payload ->> 'log_index')::bigint
), transactions AS (
    SELECT chain_id,
           block_number,
           block_hash,
           transaction_hash,
           transaction_index,
           array_agg(DISTINCT evidence_source ORDER BY evidence_source)
               AS evidence_sources
    FROM evidence_transactions
    GROUP BY chain_id, block_number, block_hash, transaction_hash,
             transaction_index
)
SELECT evidence_transaction.evidence_sources,
       evidence_transaction.chain_id,
       evidence_transaction.block_number,
       evidence_transaction.block_hash,
       evidence_transaction.transaction_hash,
       evidence_transaction.transaction_index,
       to_jsonb(raw_transaction) AS raw_transaction,
       to_jsonb(raw_receipt) AS raw_receipt,
       COALESCE((
           SELECT jsonb_agg(to_jsonb(event)
                            ORDER BY event.transaction_index,
                                     event.log_index,
                                     event.normalized_event_id)
           FROM bigname_phase.normalized_events event
           WHERE event.chain_id = evidence_transaction.chain_id
             AND event.block_number = evidence_transaction.block_number
             AND event.block_hash = evidence_transaction.block_hash
             AND event.transaction_hash = evidence_transaction.transaction_hash
             AND event.transaction_index = evidence_transaction.transaction_index
       ), '[]'::jsonb) AS normalized_events,
       COALESCE((
           SELECT jsonb_agg(to_jsonb(raw_log)
                            ORDER BY raw_log.log_index)
           FROM bigname_phase.raw_logs raw_log
           WHERE raw_log.chain_id = evidence_transaction.chain_id
             AND raw_log.block_number = evidence_transaction.block_number
             AND raw_log.block_hash = evidence_transaction.block_hash
             AND raw_log.transaction_hash = evidence_transaction.transaction_hash
             AND raw_log.transaction_index = evidence_transaction.transaction_index
       ), '[]'::jsonb) AS raw_logs
FROM transactions evidence_transaction
JOIN bigname_phase.raw_transactions raw_transaction
  ON raw_transaction.chain_id = evidence_transaction.chain_id
 AND raw_transaction.block_number = evidence_transaction.block_number
 AND raw_transaction.block_hash = evidence_transaction.block_hash
 AND raw_transaction.transaction_hash = evidence_transaction.transaction_hash
 AND raw_transaction.transaction_index = evidence_transaction.transaction_index
LEFT JOIN bigname_phase.raw_receipts raw_receipt
  ON raw_receipt.chain_id = evidence_transaction.chain_id
 AND raw_receipt.block_number = evidence_transaction.block_number
 AND raw_receipt.block_hash = evidence_transaction.block_hash
 AND raw_receipt.transaction_hash = evidence_transaction.transaction_hash
 AND raw_receipt.transaction_index = evidence_transaction.transaction_index
ORDER BY evidence_transaction.block_number,
         evidence_transaction.transaction_index;

ROLLBACK;
```

## Classify the cause

First rule out a historical audit row. If the captured target or boundary hash
is now `orphaned`, or Project has since completed beyond that target without the
same `last_error`, retain the row as incident history but do not recover from it.
Audit rows intentionally survive reorgs and later success
([`apps/phase-runner/src/project_failure_audit.rs:6-10`](../../apps/phase-runner/src/project_failure_audit.rs#L6-L10)).
Use `boundary_lineage_now`, which is resolved from the audit coordinates, for
that decision. Do not classify the audit as historical merely because
`boundary_present_in_current_derivation` is false: a covering Interpret redo
can remove the old normalized event while the recorded boundary block remains
readable. In that state, use the retained raw transaction and phase redo state
to distinguish an incomplete re-derivation from an indexing defect.
If a reorg or redo already occurred and either binding's current non-fork fields
do not match the audit, or its raw position has zero or multiple fork candidates,
the historical audit is insufficient to reconstruct that binding's old fork.
File the ambiguity and hold rather than classifying from the current row.

For a live failure, use this decision table. If the evidence does not establish
one row unambiguously, file the evidence and hold; do not choose the least costly
recovery by guesswork.

| Cause | Evidence | Classification |
| --- | --- | --- |
| Missed or mis-applied ENSv1→ENSv2 migration interpretation | The activated `MigrationApplied` row does not match the raw transaction, correlation group, exact predecessor cleanup, or successor binding; or it matches but the current interpreter did not close the one selected ENSv1 predecessor and no approved re-derivation is pending. | Indexing defect. Interpret validates a one-to-one activated boundary/transition pair ([`crates/interpret/src/write/identity/transition.rs:11-45`](../../crates/interpret/src/write/identity/transition.rs#L11-L45)) and updates exactly one selected predecessor binding ([`crates/interpret/src/write/identity/transition.rs:141-270`](../../crates/interpret/src/write/identity/transition.rs#L141-L270)). A mismatch means the stored interpretation or that logic is wrong. |
| Invariant is wrong for genuine on-chain behavior | The raw transaction, activated boundary, exact cleanup, successor, both bindings, and current lineage are all internally consistent, and the ENS protocol owner confirms that both bindings are legitimately current after that boundary. | On-chain assumption defect. Do not relabel it as stale data just because a redo would be convenient. The assertion deliberately treats two open arms after this proof as unpublishable ([`crates/project/src/integrity.rs:33-38`](../../crates/project/src/integrity.rs#L33-L38), [`crates/project/src/integrity.rs:127-135`](../../crates/project/src/integrity.rs#L127-L135)). |
| Stale derived state awaiting re-derivation | The current deployed interpreter is already known to produce the correct arm-scoped close, and the Interpret row shows the exact approved redo still pending or interrupted, or the deployment record already requires the affected full [re-derivation boundary](../glossary.md#re-derivation-boundary). | Operationally incomplete re-derivation, not a new protocol claim. A bounded Interpret redo deletes and re-derives its selected event range from [readable](../glossary.md#readable--read-safe) [raw facts](../glossary.md#raw-facts) ([`docs/glossary.md:1165-1171`](../glossary.md#glossary)). |

The ENSv1→ENSv2 migration transition writer is intentionally arm-scoped: it
selects the predecessor by `logical_name_id` and the recorded predecessor arm,
then closes that exact row
([`crates/interpret/src/write/identity/transition.rs:141-161`](../../crates/interpret/src/write/identity/transition.rs#L141-L161),
[`crates/interpret/src/write/identity/transition.rs:259-270`](../../crates/interpret/src/write/identity/transition.rs#L259-L270)).
This is why recovery re-derives evidence instead of editing the open interval.

## Recovery

### Stale derived state: complete the approved redo

1. Follow the production runner controls before selecting a redo: drain public
   traffic, record the exact Compose file set, capture the immutable running
   image as `<recovery-image>`, and stop the long-running phase runner. A missing
   deployment-specific drain procedure is a stop condition
   ([production recovery plays](production-docker.md#recovery-plays),
   [interpreter-mismatch escalation](production-docker.md#stop-and-escalate-an-interpreter-mismatch)).
   The commands below explicitly reuse the stopped supervisor's stable metrics
   target at `0.0.0.0:9465`. Outside this stopped-supervisor procedure, redo
   defaults to a logged ephemeral port; assign each simultaneously scraped redo
   its own stable address.
2. If an Interpret redo is already recorded, resume **that exact chain and
   persisted range**. Do not create a new range. If the evidence query returns
   an `active_redo_invalidation_token`, require the already-reviewed historical
   fetch or the decision that the
   [watch plan](../glossary.md#watch-plan--watched-tuple) did not widen, and pass
   that exact token as
   `--attest-watch-set-coverage <recorded-active-redo-invalidation-token>`.
   Omit the flag when no active attestation row exists. Never invent,
   substitute, or reuse a token; the runner binds it to the exact active redo
   ([`apps/phase-runner/src/redo_manifest_audit.rs:10-26`](../../apps/phase-runner/src/redo_manifest_audit.rs#L10-L26),
   [`apps/phase-runner/src/redo_manifest_audit.rs:116-145`](../../apps/phase-runner/src/redo_manifest_audit.rs#L116-L145)).

   Reserve the Project-only continuation below for a durable
   Interpret-to-Project handoff interruption in which no Project attempt failed.
   Prove that case from the Project row: Interpret has no marker; Project has
   `phase_status = 'running'`, `redo_in_progress = true`, `redo_mode = 'redo'`,
   the exact approved downstream range, and
   `last_error = 'required downstream redo: interpret redo completed'`, with
   neither `required downstream redo active:` nor `last attempt failed:`. The
   downstream stamp writes the first form and clears redo progress; starting
   Project changes it to the active form before phase execution
   ([`apps/phase-runner/src/redo_stamp.rs:166-203`](../../apps/phase-runner/src/redo_stamp.rs#L166-L203),
   [`apps/phase-runner/src/redo_state.rs:155-198`](../../apps/phase-runner/src/redo_state.rs#L155-L198)).
   Require the captured before/after rows and one-shot record to corroborate that
   origin. An assertion-bearing `last_error` or a `last attempt failed:` suffix
   proves a Project attempt failed and forbids this continuation. A new audit key
   first detected after the stamp is corroboration, but an absent audit row alone
   proves nothing because its insert has a documented failure exception. If the
   unconsumed handoff stamp is proved, resume only the exact persisted Project
   range, then continue at step 6:

   ```sh
   BIGNAME_IMAGE=<recovery-image> \
     docker compose --env-file .env.server \
     <compose-files> run --rm --pull never --use-aliases --service-ports phase-runner \
     phase-runner redo --chain ethereum-mainnet --phase project \
     --metrics-bind-addr 0.0.0.0:9465 \
     --source 'ethereum-mainnet:<key>:<kind>:<seed-basis>:<start>[:<role>]=<endpoint-env>' \
     --from-block <persisted-project-redo-from> \
     --to-block <persisted-project-redo-to>
   ```

   Repeat `--source` with the complete intake-capable descriptor set recorded
   by the chain's Ingest cursors.

   Project redo syntax is documented at
   [`docs/storage.md:431-439`](../storage.md#rainbow-table-preimage-import). Interpret completion
   stamps that Project range atomically, but the runner invokes Project in a
   separate call
   ([`apps/phase-runner/src/redo_state.rs:571-579`](../../apps/phase-runner/src/redo_state.rs#L571-L579),
   [`apps/phase-runner/src/runner_operator_redo.rs:350-363`](../../apps/phase-runner/src/runner_operator_redo.rs#L350-L363)).
   This exact-marker continuation is not a new Project-only repair. If the
   marker's origin, persisted range, or unconsumed state cannot be proved,
   file-and-hold.

   A retained **failed** Project cascade is a separate permitted entry for a
   newly diagnosed, different-fingerprint conflict. Use it only when Interpret
   has no redo marker, the Project row is a resumable redo whose `last_error`
   starts with the non-active `required downstream redo: ` prefix and contains
   `last attempt failed:`, and evidence capture and cause discrimination have
   approved a new bounded Interpret redo. Do not run the Project-only command.
   The runner permits Interpret to start behind that non-active downstream
   marker; `required downstream redo active: ` does not satisfy this exemption
   ([`apps/phase-runner/src/transitions.rs:175-201`](../../apps/phase-runner/src/transitions.rs#L175-L201),
   [`apps/phase-runner/src/transitions.rs:256-262`](../../apps/phase-runner/src/transitions.rs#L256-L262),
   [`apps/phase-runner/src/redo_stamp.rs:8-17`](../../apps/phase-runner/src/redo_stamp.rs#L8-L17)).
   Run the new approved Interpret range with the step-4 command. On successful
   Interpret completion, the downstream stamp extends the retained Project
   marker to the union of its old range and the new effective Interpret range:
   `from = min(old_from, new_from)` and
   `to = max(old_to, min(Project current, new_to))`. It clears prior Project
   redo progress
   before the automatic Project cascade retries that reconciled range, while
   retaining the marker's diagnostic history
   ([`apps/phase-runner/src/redo_state.rs:571-579`](../../apps/phase-runner/src/redo_state.rs#L571-L579),
   [`apps/phase-runner/src/redo_stamp.rs:95-113`](../../apps/phase-runner/src/redo_stamp.rs#L95-L113),
   [`apps/phase-runner/src/redo_stamp.rs:126-163`](../../apps/phase-runner/src/redo_stamp.rs#L126-L163)).
3. If neither Interpret nor Project has a redo marker, require the incident
   owner to identify the earliest
   affected stored block and approve the range through the recorded Interpret
   head. If that start cannot be established, skip the scoped play and escalate
   to the full [re-derivation boundary](../glossary.md#re-derivation-boundary)
   below. For the command below, `<redo-from>` and `<redo-to>` mean either these
   newly approved bounds or the exact persisted bounds from step 2.
4. Copy every source descriptor for the affected chain exactly from the deployed
   configuration, repeating `--source` once per descriptor, and run the
   documented bounded Interpret redo in the pinned recovery image:

   ```sh
   BIGNAME_IMAGE=<recovery-image> \
     docker compose --env-file .env.server \
     <compose-files> run --rm --pull never --use-aliases --service-ports phase-runner \
     phase-runner redo --chain ethereum-mainnet --phase interpret \
     --metrics-bind-addr 0.0.0.0:9465 \
     --from-block <redo-from> \
     --to-block <redo-to> \
     --source <affected-chain-source> \
     [--source <additional-affected-chain-source> ...] \
     [--attest-watch-set-coverage <recorded-active-redo-invalidation-token>]
   ```

   The source descriptors prove that the persisted raw-data range needed by the
   redo is present; they do not authorize new ingestion. Preserve the deployed
   `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS` value, or its exact
   `--hydration-rpc` equivalent, because the automatic Project cascade may need
   it. The canonical command and descriptor format are documented at
   [production interpreter-mismatch procedure](production-docker.md#stop-and-escalate-an-interpreter-mismatch).
   If a newly started scoped, unflagged redo is fenced by a
   [manifest-authority marker](../glossary.md#manifest-authority-marker), its
   first error is `ContentHashMismatch`: it prints the exact demanded full range
   `<full-redo-from>..=<redo-to>`, does not name the marker, and prints no token.
   `<full-redo-from>` is the deployment's earliest ingest cursor start, not
   necessarily block zero
   ([`apps/phase-runner/src/redo_state.rs:338-383`](../../apps/phase-runner/src/redo_state.rs#L338-L383)).
   That runner error-text gap is tracked in
   [issue #545](https://github.com/ensdomains/bigname/issues/545). The
   invalidation token is already visible in the captured Interpret phase row's
   `input_content_hash` marker value as
   `manifest-authority:<authority-fingerprint>:<invalidation-token>`. Do not use
   it to retry the scoped range. Instead, run the Interpret redo unflagged over
   the exact full range `<full-redo-from>..=<redo-to>` printed by that first
   error. That full-range attempt reaches the
   manifest-authority fence, stops, and prints the invalidation token
   ([`apps/phase-runner/src/redo_manifest_attestation.rs:70-85`](../../apps/phase-runner/src/redo_manifest_attestation.rs#L70-L85)).
   Complete the mandatory historical fetch for any widened watch plan, or the
   required review proving no widening, then rerun that same full range with
   `--attest-watch-set-coverage <token>`. The production rule and its
   no-invention/no-reuse constraints are at
   [planned re-derivation boundary](production-docker.md#planned-migration-and-fingerprint-boundary).
   There is no `--arm` option. The redo uses each closing event's recorded arm
   and refuses to guess one when evidence is missing
   ([`crates/adapters/src/schema_v2/seam.rs:71-99`](../../crates/adapters/src/schema_v2/seam.rs#L71-L99)).
5. Let the command's automatic Project cascade run. A successful Interpret redo
   executes the Project redo stamped for the required range
   ([`apps/phase-runner/src/runner_operator_redo.rs:343-363`](../../apps/phase-runner/src/runner_operator_redo.rs#L343-L363)).
   If Project fails, stop this play and use the recurrence route immediately
   below; do not continue to step 6.
6. Require the one-shot command to succeed and both redo markers to be clear,
   then restart the long-running runner under the production procedure with the
   same image and exact Compose file set. Do **not** require Project to be
   `completed` before restart: redo completion restores the lifecycle status
   that preceded the redo, which is `failed` for this incident
   ([`apps/phase-runner/src/redo_completion.rs:9-19`](../../apps/phase-runner/src/redo_completion.rs#L9-L19),
   [`apps/phase-runner/src/redo_state.rs:498-520`](../../apps/phase-runner/src/redo_state.rs#L498-L520)).
7. After restart, require Interpret and Project to report `completed`, confirm
   Project advances past the blocked target, and confirm `/v2/status` is no
   longer stale for this cause. Restore traffic only after the production health
   gates pass.

If any explicit Project redo or automatic Project cascade fails, stop the
one-shot command and capture the CLI error, active redo row, and corresponding
audit row before another action. The row can already exist when the same
complete key recurs. Select it by its complete key—chain, target
block number, target block hash, interpreter content hash, failure kind, and
failure fingerprint—not by `detected_at`, name, or fingerprint alone. Those six
fields are the audit primary key
([`schema-v2/baseline/12_project_generation_failures.sql:1-14`](../../schema-v2/baseline/12_project_generation_failures.sql#L1-L14)).
Then compare it with the complete key of the conflict the redo was meant to
repair:

- If the fingerprint is the same, stop. Do not widen or repeat the scoped redo;
  escalate to the boundary play, even if the target coordinates differ. This is
  the same semantic conflict recurring.
- If the fingerprint differs, treat the row as a fresh conflict: return to
  [Stop and capture evidence](#stop-and-capture-evidence), run the complete
  read-only queries for that new key, and perform cause discrimination again.
  If that fresh diagnosis approves an Interpret redo while the failed Project
  cascade marker is retained, enter through the retained-failed-marker case in
  recovery step 2; do not wait for the "neither marker" condition in step 3 and
  do not use the Project-only continuation.
  Do not send a different conflict directly to the same-fingerprint boundary
  escalation; Project deliberately reports only the first logical name in one
  failed generation
  ([`crates/project/src/integrity.rs:192-194`](../../crates/project/src/integrity.rs#L192-L194)).

If the audit insert failed or the complete new key cannot be established,
file-and-hold under the audit-write exception. Never use the Project-only
handoff continuation after an observed Project failure. The same-fingerprint
boundary route matches the existing runner instruction
([production interpreter-mismatch procedure](production-docker.md#stop-and-escalate-an-interpreter-mismatch)).

### Indexing defect: file, fix, then re-derive

File a bigname GitHub issue containing the complete evidence bundle and link it
from the active incident and issue
[#494](https://github.com/ensdomains/bigname/issues/494). Name the owning
[source family](../glossary.md#source-family), boundary event identity,
correlation ID, and first affected block. Keep
the chain held at the last successful Project publication until an Interpret or
manifest owner confirms one of these reviewed outcomes:

- the deployed code already contains the correction and the defect is confined
  to a known range: run the scoped Interpret redo above; or
- the correction changes interpretation semantics, source
  [admission](../glossary.md#admission), or the required historical
  fetch: use its approved re-derivation-boundary plan.

If the scoped redo reaches another Project failure, attach the new log and audit
result to the issue and follow the complete-key recurrence route above. Do not
keep retrying.

### Wrong invariant: file and hold for a code fix

Do not redo. Replaying an invalid assumption cannot make the publication safe.
File a bigname GitHub issue linked from the incident and #494, tag the Project
and ENS protocol owners, and include the raw transaction sequence plus the
specific upstream behavior that contradicts the assertion. Keep the chain held
until a reviewed code-and-contract change lands with its own re-derivation
decision. There is no operator override or skip path.

### Escalate to the full boundary play

Use the planned full re-derivation procedure when the earliest affected block
cannot be proved, an interpreter content hash changed, source admission or the
[watch plan](../glossary.md#watch-plan--watched-tuple) widened, or the approved scoped redo
reproduces the failure. This
runbook does not authorize an improvised reset. Hand the evidence to the release,
Interpret, manifest, and storage owners and follow
[planned production re-derivation boundary](production-docker.md#planned-migration-and-fingerprint-boundary),
including its fixed image, backup, historical fetch, full Interpret/Project walk,
Verify, and readiness gates.

## Prohibited actions

Never:

- edit `chain_phase_state` to mark Project completed, clear `last_error`, remove
  a redo marker, or advance a recorded head;
- update or delete `surface_bindings`, normalized events, resources, raw facts,
  or any Project projection row;
- delete or alter `project_generation_failures` rows;
- create a new Project-only redo and call the input repaired—the only sanctioned
  Project-only command here completes the exact downstream marker already
  stamped by a proven successful Interpret redo; or
- skip the name, suppress the assertion, or publish a partial projection
  generation.

The audit table is operator diagnostics rather than a product projection
([`schema-v2/baseline/12_project_generation_failures.sql:28-32`](../../schema-v2/baseline/12_project_generation_failures.sql#L28-L32)),
and Project is the sole projection writer
([`docs/glossary.md:1183-1189`](../glossary.md#glossary)). The existing
production recovery rules also prohibit hand-editing identity and normalized
event rows
([production interpreter-mismatch procedure](production-docker.md#stop-and-escalate-an-interpreter-mismatch)).

## Deployment-profile scope

Both exact-name and child assertions apply to the configured Mainnet and Sepolia
ENS [deployment profiles](../glossary.md#deployment-profile). They remain gated by
an admitted proof: exact-name failures require an activated
`migration_authority_transition`, and child failures require the selected
ENSv2 authority and its admitted proof
([`assert_exact_name_authority`](../../crates/project/src/integrity.rs#L41),
[`assert_child_authority`](../../crates/project/src/integrity.rs#L260)).
Ethereum Sepolia carries distinct ENSv1 and ENSv2 test deployments on the same
chain: the pinned ENSv1 registry is
`0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e`
(upstream: .refs/ens_v1/deployments/sepolia/ENSRegistry.json:L2 @ ens_v1@91c966f),
while the pinned ENSv2 RootRegistry and ETHRegistry are
`0x11b5bfbe9078d826b1edbdd1cfc12f5828d9f50c` and
`0x67b728a792e789a8978b30cf1b3b641f19354b43`
(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/RootRegistry.json:L2 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistry.json:L2 @ ens_v2@a971bd64).

Genuine Sepolia overlap therefore means that the same logical name has readable
ENSv1 and ENSv2 evidence **without** an
activated `MigrationApplied` boundary connecting that name. Project leaves that
shape unsupported with `independent_ens_deployments_overlap`; it does not raise
this halt. The exact
[shared ENS infrastructure](../glossary.md#shared-ens-infrastructure) names—root,
`eth`, `reverse`, and `addr.reverse`—instead select a current ENSv2 arm when
ENSv1 evidence is current or historical, without fabricating a proof, so they
also do not raise this halt
([`crates/project/src/builders/name_authority.rs:459-480`](../../crates/project/src/builders/name_authority.rs#L459-L480),
[`crates/project/src/builders/name_authority.rs:619-630`](../../crates/project/src/builders/name_authority.rs#L619-L630)).
Do not interpret mere cross-era Sepolia evidence as a missed ENSv1→ENSv2
migration.

An actual unlocked ENSv1→ENSv2 migration transaction is stronger evidence, but
its two entry paths must not be conflated. The registrar-token path reclaims the
token, replaces the ENSv1 registry record, transfers the token to the Graveyard,
and injects the ENSv2 registration
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111-L119 @ ens_v2@a971bd64).
The unlocked-wrapped path instead clears the wrapper resolver, unwraps the name
to the Graveyard, and then performs the same ENSv2 injection
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146-L160 @ ens_v2@a971bd64).
Only a proven per-name boundary derived from such admitted evidence connects the
two arms. If Sepolia has that proof and an open predecessor, treat the resulting
`dual_current_exact_name_authority` failure as this runbook's incident and follow
the same evidence-capture and repair procedure. A surviving post-epoch child
contradiction follows the `dual_current_child_authority` procedure. The command
examples above use Mainnet; substitute `ethereum-sepolia` and the affected
Sepolia source descriptors without changing proof, lineage or publication checks.
The coherent wrapped and locked publication paths are exercised by
[PR #852](https://github.com/ensdomains/bigname/pull/852).

## Closure and escalation record

Keep the incident and linked bigname issue updated with:

- the immutable audit row and whether its target/boundary hashes are still
  readable or are now orphaned;
- every boundary, binding, resource, correlation, and transaction identifier;
- the classification decision and the owners who approved it;
- the exact redo range or re-derivation-boundary record, image/build SHA, and
  interpreter content hash; and
- the post-recovery phase rows, new audit query result, and `/v2/status` result.

Close the incident only after the repaired target publishes, the affected chain
advances beyond it, no dual-current exact-name or child failure with new
evidence appears on the next attempt, all required redo markers are clear, and
the linked defect or on-chain-assumption issue records the durable resolution.
Leave all audit rows in place.
