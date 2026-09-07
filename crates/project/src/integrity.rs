use sqlx::{Postgres, Row, Transaction};

use crate::{Marker, ProjectError, Result};

pub const DUAL_CURRENT_EXACT_NAME_AUTHORITY: &str = "dual_current_exact_name_authority";
pub const DUAL_CURRENT_CHILD_AUTHORITY: &str = "dual_current_child_authority";

/// Structured evidence for a projection-blocking invariant failure.
///
/// The phase runner persists this after the generation transaction rolls back;
/// the assertion itself writes nothing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationFailureEvidence {
    pub failure_kind: String,
    pub failure_fingerprint: String,
    pub logical_name_id: String,
    pub target_block_number: i64,
    pub target_block_hash: String,
    pub payload: serde_json::Value,
}

/// Assert every projection-blocking invariant, in a fixed order so one failed
/// generation always records the same first conflict.
pub(crate) async fn assert_publishable(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    assert_exact_name_authority(transaction, chain_id, target).await?;
    assert_child_authority(transaction, chain_id, target).await
}

/// Fail the generation on the Mainnet and Sepolia ENS [deployment profiles] when a name still
/// holds current bindings on both arms after its proven activated ENSv1->ENSv2 migration boundary.
///
/// [deployment profiles]: ../../../docs/glossary.md#deployment-profile
///
/// Bindings are evaluated at end-of-target-block, which is the transaction- and
/// block-level reconciliation tolerance: transients inside one migration
/// transaction or block are invisible here by construction.
async fn assert_exact_name_authority(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    let conflict = sqlx::query(
        r#"
        WITH target_time AS (
            SELECT block_timestamp + interval '1 second' AS cutoff,
                   canonicality_state
            FROM chain_lineage
            WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
        ), conflict AS (
            SELECT authority.logical_name_id,
                   predecessor.surface_binding_id AS predecessor_binding_id,
                   predecessor.resource_id AS predecessor_resource_id,
                   predecessor.block_number AS predecessor_block_number,
                   COALESCE(
                       (predecessor.provenance ->> 'transaction_index')::bigint, -1
                   ) AS predecessor_transaction_index,
                   COALESCE(
                       (predecessor.provenance ->> 'log_index')::bigint, -1
                   ) AS predecessor_log_index,
                   predecessor.canonicality_state AS predecessor_canonicality_state,
                   successor.surface_binding_id AS successor_binding_id,
                   successor.resource_id AS successor_resource_id,
                   successor.block_number AS successor_block_number,
                   COALESCE(
                       (successor.provenance ->> 'transaction_index')::bigint, -1
                   ) AS successor_transaction_index,
                   COALESCE(
                       (successor.provenance ->> 'log_index')::bigint, -1
                   ) AS successor_log_index,
                   successor.canonicality_state AS successor_canonicality_state,
                   authority.authority_proof_event_identity AS boundary_event_identity,
                   boundary.block_number AS boundary_block_number,
                   boundary.block_hash AS boundary_block_hash,
                   COALESCE(boundary.transaction_index, -1) AS boundary_transaction_index,
                   COALESCE(boundary.log_index, -1) AS boundary_log_index,
                   boundary.canonicality_state AS boundary_canonicality_state,
                   target_time.canonicality_state AS target_canonicality_state
            FROM project_name_authority authority
            CROSS JOIN target_time
            JOIN project_events boundary
              ON boundary.normalized_event_id = authority.authority_proof_event_id
             -- The transition proof is selected per name, so pinning the name
             -- here is semantics-preserving and reaches the staged index.
             AND boundary.logical_name_id = authority.logical_name_id
            JOIN LATERAL (
                SELECT candidate.*
                FROM project_binding_candidates candidate
                WHERE candidate.logical_name_id = authority.logical_name_id
                  AND candidate.authority_arm = 'ens_v1'
                  AND candidate.active_from < target_time.cutoff
                  AND (
                      candidate.active_to IS NULL
                      OR candidate.active_to >= target_time.cutoff
                  )
                ORDER BY candidate.block_number DESC,
                         COALESCE(
                             (candidate.provenance ->> 'transaction_index')::bigint, -1
                         ) DESC,
                         COALESCE(
                             (candidate.provenance ->> 'log_index')::bigint, -1
                         ) DESC,
                         candidate.surface_binding_id DESC
                LIMIT 1
            ) predecessor ON TRUE
            JOIN LATERAL (
                SELECT candidate.*
                FROM project_binding_candidates candidate
                WHERE candidate.logical_name_id = authority.logical_name_id
                  AND candidate.authority_arm = 'ens_v2'
                  AND candidate.active_from < target_time.cutoff
                  AND (
                      candidate.active_to IS NULL
                      OR candidate.active_to >= target_time.cutoff
                  )
                ORDER BY candidate.block_number DESC,
                         COALESCE(
                             (candidate.provenance ->> 'transaction_index')::bigint, -1
                         ) DESC,
                         COALESCE(
                             (candidate.provenance ->> 'log_index')::bigint, -1
                         ) DESC,
                         candidate.surface_binding_id DESC
                LIMIT 1
            ) successor ON TRUE
            WHERE authority.deployment_profile IN ('mainnet', 'sepolia')
              -- Only the ENSv1->ENSv2 migration transition proof. A positive
              -- ENSv2 child registration
              -- supersedes the retained ENSv1 child binding without closing it,
              -- so an open predecessor there is expected rather than anomalous;
              -- the parent-child relation invariant below covers that case.
              AND authority.authority_proof_kind = 'migration_authority_transition'
              AND authority.authority_proof_event_identity IS NOT NULL
              AND boundary.block_number <= $2
        )
        SELECT logical_name_id,
               encode(
                   sha256(
                       convert_to(
                           concat_ws(
                               '|', logical_name_id,
                               predecessor_binding_id::text, predecessor_resource_id::text,
                               predecessor_block_number::text,
                               predecessor_transaction_index::text,
                               predecessor_log_index::text,
                               successor_binding_id::text, successor_resource_id::text,
                               successor_block_number::text,
                               successor_transaction_index::text,
                               successor_log_index::text,
                               boundary_event_identity, boundary_block_number::text,
                               boundary_transaction_index::text, boundary_log_index::text
                           ),
                           'UTF8'
                       )
                   ),
                   'hex'
               ) AS failure_fingerprint,
               jsonb_build_object(
                   'predecessor', jsonb_build_object(
                       'authority_arm', 'ens_v1',
                       'surface_binding_id', predecessor_binding_id,
                       'resource_id', predecessor_resource_id,
                       'block_number', predecessor_block_number,
                       'transaction_index', predecessor_transaction_index,
                       'log_index', predecessor_log_index,
                       'canonicality_state', predecessor_canonicality_state
                   ),
                   'successor', jsonb_build_object(
                       'authority_arm', 'ens_v2',
                       'surface_binding_id', successor_binding_id,
                       'resource_id', successor_resource_id,
                       'block_number', successor_block_number,
                       'transaction_index', successor_transaction_index,
                       'log_index', successor_log_index,
                       'canonicality_state', successor_canonicality_state
                   ),
                   'boundary', jsonb_build_object(
                       'event_identity', boundary_event_identity,
                       'block_number', boundary_block_number,
                       'block_hash', boundary_block_hash,
                       'transaction_index', boundary_transaction_index,
                       'log_index', boundary_log_index,
                       'canonicality_state', boundary_canonicality_state
                   ),
                   'target', jsonb_build_object(
                       'block_number', $2::bigint,
                       'block_hash', $3::text,
                       'canonicality_state', target_canonicality_state
                   )
               ) AS payload
        FROM conflict
        ORDER BY logical_name_id
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to assert exact-name authority integrity", error)
    })?;

    let Some(row) = conflict else {
        return Ok(());
    };
    let logical_name_id: String = row
        .try_get("logical_name_id")
        .map_err(|error| ProjectError::database("failed to read integrity failure name", error))?;
    let failure_fingerprint: String = row.try_get("failure_fingerprint").map_err(|error| {
        ProjectError::database("failed to read integrity failure fingerprint", error)
    })?;
    let payload: serde_json::Value = row.try_get("payload").map_err(|error| {
        ProjectError::database("failed to read integrity failure evidence", error)
    })?;

    Err(ProjectError::generation_failure(
        format!(
            "chain {chain_id} name {logical_name_id} holds current bindings on both \
             authority arms after its activated ENSv1\u{2192}ENSv2 migration boundary; \
             projection generation for block {} is not publishable",
            target.number
        ),
        GenerationFailureEvidence {
            failure_kind: DUAL_CURRENT_EXACT_NAME_AUTHORITY.into(),
            failure_fingerprint,
            logical_name_id,
            target_block_number: target.number,
            target_block_hash: target.hash.clone(),
            payload,
        },
    ))
}

/// Fail the generation on the Mainnet and Sepolia ENS [deployment profiles] when a parent-child
/// pair still states both an ENSv1 and an ENSv2 relation after the child's own proven ENSv2 authority began.
///
/// [deployment profiles]: ../../../docs/glossary.md#deployment-profile
///
/// Raw coexistence is not the anomaly: a migrated or positively registered ENSv2
/// child normally keeps its ENSv1 relation as residue, because neither migration
/// branch retracts the ENSv1 registry entry: the locked branch only parks the wrapper
/// token
/// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2_sepolia_20260629@ccaeb58),
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L301 @ ens_v1@91c966f), and the
/// emancipated branch unwraps to a reassignment rather than a deletion
/// (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L180 @ ens_v2@a971bd64),
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1029 @ ens_v1@91c966f).
/// What cannot be reconciled is an ENSv1
/// relation asserted *after* that authority epoch started — the selection would
/// silently drop it, and dropping a live contradiction is what this refuses.
///
/// A released ENSv2 child is deliberately outside that: release publishes no row and
/// never falls back to ENSv1, so a later ENSv1 relation is residue with nothing left to
/// contradict rather than a dual-current pair.
async fn assert_child_authority(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    let conflict = sqlx::query(
        r#"
        WITH target_lineage AS (
            SELECT canonicality_state
            FROM chain_lineage
            WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
        ), conflict AS (
            SELECT authority.logical_name_id AS child_logical_name_id,
                   successor.parent_logical_name_id,
                   authority.authority_proof_kind,
                   authority.authority_proof_event_identity,
                   proof.block_number AS proof_block_number,
                   proof.block_hash AS proof_block_hash,
                   proof.canonicality_state AS proof_canonicality_state,
                   target_lineage.canonicality_state AS target_canonicality_state,
                   COALESCE(
                       (authority.authority_epoch_start_position ->> 'block_number')::bigint, -1
                   ) AS epoch_block_number,
                   COALESCE(
                       (authority.authority_epoch_start_position ->> 'transaction_index')::bigint,
                       -1
                   ) AS epoch_transaction_index,
                   COALESCE(
                       (authority.authority_epoch_start_position ->> 'log_index')::bigint, -1
                   ) AS epoch_log_index,
                   predecessor.normalized_event_id AS predecessor_event_id,
                   predecessor.event_identity AS predecessor_event_identity,
                   predecessor.source_family AS predecessor_source_family,
                   predecessor.block_number AS predecessor_block_number,
                   COALESCE(predecessor.evidence_transaction_index, -1)
                       AS predecessor_transaction_index,
                   COALESCE(predecessor.evidence_log_index, -1) AS predecessor_log_index,
                   predecessor.canonicality_state AS predecessor_canonicality_state,
                   successor.normalized_event_id AS successor_event_id,
                   successor.event_identity AS successor_event_identity,
                   successor.source_family AS successor_source_family,
                   successor.block_number AS successor_block_number,
                   COALESCE(successor.evidence_transaction_index, -1)
                       AS successor_transaction_index,
                   COALESCE(successor.evidence_log_index, -1) AS successor_log_index,
                   successor.canonicality_state AS successor_canonicality_state
            FROM project_name_authority authority
            CROSS JOIN target_lineage
            -- The proof event's own block hash and canonicality are recorded so the audit row
            -- stays resolvable through lineage after a later reorganization.
            JOIN project_events proof
              ON proof.normalized_event_id = authority.authority_proof_event_id
             AND proof.logical_name_id = authority.logical_name_id
            JOIN project_child_candidates successor
              ON successor.child_logical_name_id = authority.logical_name_id
             AND successor.authority_arm = 'ens_v2'
            JOIN project_child_candidates predecessor
              ON predecessor.child_logical_name_id = successor.child_logical_name_id
             AND predecessor.parent_logical_name_id = successor.parent_logical_name_id
             AND predecessor.authority_arm = 'ens_v1'
            WHERE authority.deployment_profile IN ('mainnet', 'sepolia')
              AND authority.selected_authority_arm = 'ens_v2'
              -- Both ENSv2 child authority proofs: the activated migration boundary
              -- and the positive ENSv2 child registration.
              AND authority.authority_proof_kind IN (
                  'migration_authority_transition', 'positive_v2_child_registration'
              )
              AND authority.authority_proof_event_identity IS NOT NULL
              AND (
                  predecessor.block_number,
                  COALESCE(predecessor.evidence_transaction_index, -1),
                  COALESCE(predecessor.evidence_log_index, -1)
              ) > (
                  COALESCE(
                      (authority.authority_epoch_start_position ->> 'block_number')::bigint, -1
                  ),
                  COALESCE(
                      (authority.authority_epoch_start_position ->> 'transaction_index')::bigint,
                      -1
                  ),
                  COALESCE(
                      (authority.authority_epoch_start_position ->> 'log_index')::bigint, -1
                  )
              )
        )
        SELECT parent_logical_name_id,
               child_logical_name_id,
               -- Every input is stable across a replay. `normalized_event_id` is a generated
               -- identity that a redo's delete-and-reinsert changes, so the fingerprint and the
               -- durable evidence are keyed on `event_identity` instead; the same semantic
               -- conflict after a replay must dedup against the row already written.
               encode(
                   sha256(
                       convert_to(
                           concat_ws(
                               '|', parent_logical_name_id, child_logical_name_id,
                               authority_proof_kind, authority_proof_event_identity,
                               proof_block_number::text, proof_block_hash,
                               epoch_block_number::text, epoch_transaction_index::text,
                               epoch_log_index::text,
                               predecessor_event_identity, predecessor_block_number::text,
                               predecessor_transaction_index::text,
                               predecessor_log_index::text,
                               successor_event_identity, successor_block_number::text,
                               successor_transaction_index::text,
                               successor_log_index::text
                           ),
                           'UTF8'
                       )
                   ),
                   'hex'
               ) AS failure_fingerprint,
               jsonb_build_object(
                   'parent_logical_name_id', parent_logical_name_id,
                   'authority_proof', jsonb_build_object(
                       'proof_kind', authority_proof_kind,
                       'event_identity', authority_proof_event_identity,
                       'block_number', proof_block_number,
                       'block_hash', proof_block_hash,
                       'canonicality_state', proof_canonicality_state
                   ),
                   'authority_proof_kind', authority_proof_kind,
                   'authority_proof_event_identity', authority_proof_event_identity,
                   'authority_epoch_start_position', jsonb_build_object(
                       'block_number', epoch_block_number,
                       'transaction_index', epoch_transaction_index,
                       'log_index', epoch_log_index
                   ),
                   'predecessor', jsonb_build_object(
                       'authority_arm', 'ens_v1',
                       'source_family', predecessor_source_family,
                       'event_identity', predecessor_event_identity,
                       'normalized_event_id', predecessor_event_id,
                       'block_number', predecessor_block_number,
                       'transaction_index', predecessor_transaction_index,
                       'log_index', predecessor_log_index,
                       'canonicality_state', predecessor_canonicality_state
                   ),
                   'successor', jsonb_build_object(
                       'authority_arm', 'ens_v2',
                       'source_family', successor_source_family,
                       'event_identity', successor_event_identity,
                       'normalized_event_id', successor_event_id,
                       'block_number', successor_block_number,
                       'transaction_index', successor_transaction_index,
                       'log_index', successor_log_index,
                       'canonicality_state', successor_canonicality_state
                   ),
                   'target', jsonb_build_object(
                       'block_number', $2::bigint,
                       'block_hash', $3::text,
                       'canonicality_state', target_canonicality_state
                   )
               ) AS payload
        FROM conflict
        -- One pair can have several candidate rows per arm, so the witness is pinned by the
        -- candidate events themselves rather than by the pair alone.
        ORDER BY parent_logical_name_id, child_logical_name_id,
                 predecessor_event_identity, successor_event_identity
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to assert child authority integrity", error))?;

    let Some(row) = conflict else {
        return Ok(());
    };
    let parent: String = row
        .try_get("parent_logical_name_id")
        .map_err(|error| ProjectError::database("failed to read child failure parent", error))?;
    let logical_name_id: String = row
        .try_get("child_logical_name_id")
        .map_err(|error| ProjectError::database("failed to read child failure name", error))?;
    let failure_fingerprint: String = row.try_get("failure_fingerprint").map_err(|error| {
        ProjectError::database("failed to read child failure fingerprint", error)
    })?;
    let payload: serde_json::Value = row
        .try_get("payload")
        .map_err(|error| ProjectError::database("failed to read child failure evidence", error))?;

    Err(ProjectError::generation_failure(
        format!(
            "chain {chain_id} parent {parent} and child {logical_name_id} state current \
             relations on both authority arms after the child's ENSv2 authority began; \
             projection generation for block {} is not publishable",
            target.number
        ),
        GenerationFailureEvidence {
            failure_kind: DUAL_CURRENT_CHILD_AUTHORITY.into(),
            failure_fingerprint,
            logical_name_id,
            target_block_number: target.number,
            target_block_hash: target.hash.clone(),
            payload,
        },
    ))
}
