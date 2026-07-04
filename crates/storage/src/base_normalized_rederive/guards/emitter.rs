use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Row};

use super::super::{
    BaseNormalizedRederiveReplayTargetSnapshot, reverse_claim_derivation_kind,
    reverse_claim_source_families, subregistry_derivation_kinds, subregistry_source_families,
    unwrapped_authority_derivation_kind, unwrapped_authority_source_families,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct OrphanedScopeEmitter {
    derivation_kind: String,
    source_family: String,
    block_number: i64,
    block_hash: String,
    transaction_hash: String,
    log_index: i64,
    emitting_address: String,
}

pub(super) async fn ensure_delete_scope_emitters_replay_active(
    pool: &PgPool,
    replay_target_block: i64,
    active_replay_target_snapshot: &[BaseNormalizedRederiveReplayTargetSnapshot],
) -> Result<()> {
    let rows = fetch_orphaned_scope_emitters(
        sqlx::query(orphaned_delete_scope_emitters_sql()),
        replay_target_block,
        active_replay_target_snapshot,
    )
    .fetch_all(pool)
    .await
    .context(
        "failed to validate Base delete-scope emitters against active replay target addresses",
    )?;
    ensure_orphaned_scope_emitters_empty(orphaned_scope_emitters_from_rows(rows)?)
}

pub(super) async fn ensure_delete_scope_emitters_replay_active_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
    active_replay_target_snapshot: &[BaseNormalizedRederiveReplayTargetSnapshot],
) -> Result<()> {
    let rows = fetch_orphaned_scope_emitters(
        sqlx::query(orphaned_delete_scope_emitters_sql()),
        replay_target_block,
        active_replay_target_snapshot,
    )
    .fetch_all(&mut **transaction)
    .await
    .context(
        "failed to validate Base delete-scope emitters against active replay target addresses",
    )?;
    ensure_orphaned_scope_emitters_empty(orphaned_scope_emitters_from_rows(rows)?)
}

fn ensure_orphaned_scope_emitters_empty(rows: Vec<OrphanedScopeEmitter>) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let examples = rows
        .into_iter()
        .map(|row| {
            format!(
                "{}/{} block={} log={}/{}:{} emitter={}",
                row.derivation_kind,
                row.source_family,
                row.block_number,
                row.block_hash,
                row.transaction_hash,
                row.log_index,
                row.emitting_address
            )
        })
        .collect::<Vec<_>>();
    bail!(
        "Base normalized-event rederive delete scope contains log-derived rows emitted by addresses not in the current active replay target set: {}",
        examples.join(", ")
    );
}

fn fetch_orphaned_scope_emitters<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    replay_target_block: i64,
    active_replay_target_snapshot: &[BaseNormalizedRederiveReplayTargetSnapshot],
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    let source_families = active_replay_target_snapshot
        .iter()
        .map(|target| target.source_family.clone())
        .collect::<Vec<_>>();
    let addresses = active_replay_target_snapshot
        .iter()
        .map(|target| target.address.clone())
        .collect::<Vec<_>>();
    let from_blocks = active_replay_target_snapshot
        .iter()
        .map(|target| target.from_block)
        .collect::<Vec<_>>();
    let to_blocks = active_replay_target_snapshot
        .iter()
        .map(|target| target.to_block)
        .collect::<Vec<_>>();

    query
        .bind(replay_target_block)
        .bind(reverse_claim_derivation_kind())
        .bind(reverse_claim_source_families())
        .bind(subregistry_derivation_kinds())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_derivation_kind())
        .bind(unwrapped_authority_source_families())
        .bind(source_families)
        .bind(addresses)
        .bind(from_blocks)
        .bind(to_blocks)
}

fn orphaned_scope_emitters_from_rows(
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<OrphanedScopeEmitter>> {
    rows.into_iter()
        .map(|row| {
            Ok(OrphanedScopeEmitter {
                derivation_kind: row.try_get("derivation_kind")?,
                source_family: row.try_get("source_family")?,
                block_number: row.try_get("block_number")?,
                block_hash: row.try_get("block_hash")?,
                transaction_hash: row.try_get("transaction_hash")?,
                log_index: row.try_get("log_index")?,
                emitting_address: row.try_get("emitting_address")?,
            })
        })
        .collect()
}

pub(super) fn orphaned_delete_scope_emitters_sql() -> &'static str {
    r#"
    WITH active_targets AS (
        SELECT source_family, address, from_block, to_block
        FROM unnest(
            $8::TEXT[],
            $9::TEXT[],
            $10::BIGINT[],
            $11::BIGINT[]
        ) AS target(source_family, address, from_block, to_block)
    )
    SELECT
        event.derivation_kind,
        event.source_family,
        event.block_number,
        event.block_hash,
        event.transaction_hash,
        event.log_index,
        LOWER(raw_log.emitting_address) AS emitting_address
    FROM normalized_events event
    JOIN raw_logs raw_log
      ON raw_log.chain_id = event.chain_id
     AND raw_log.block_hash = event.block_hash
     AND raw_log.transaction_hash = event.transaction_hash
     AND raw_log.log_index = event.log_index
     AND raw_log.canonicality_state IN (
         'canonical'::canonicality_state,
         'safe'::canonicality_state,
         'finalized'::canonicality_state
     )
    WHERE event.chain_id = 'base-mainnet'
      AND event.block_number BETWEEN 17571485 AND $1
      AND event.block_hash IS NOT NULL
      AND event.transaction_hash IS NOT NULL
      AND event.log_index IS NOT NULL
      AND (
          (event.derivation_kind = $2 AND event.source_family = ANY($3::TEXT[]))
          OR (event.derivation_kind = ANY($4::TEXT[]) AND event.source_family = ANY($5::TEXT[]))
          OR (event.derivation_kind = $6 AND event.source_family = ANY($7::TEXT[]))
      )
      AND NOT EXISTS (
          SELECT 1
          FROM active_targets target
          WHERE target.source_family = event.source_family
            AND target.address = LOWER(raw_log.emitting_address)
            AND target.from_block <= event.block_number
            AND target.to_block >= event.block_number
      )
    LIMIT 10
    "#
}
