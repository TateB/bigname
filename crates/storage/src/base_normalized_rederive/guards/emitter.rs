use anyhow::{Context, Result, bail};
use sqlx::Row;

use super::super::{
    reverse_claim_derivation_kind, reverse_claim_source_families, subregistry_derivation_kinds,
    subregistry_source_families, unwrapped_authority_derivation_kind,
    unwrapped_authority_source_families,
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

pub(super) async fn ensure_delete_scope_emitters_replay_active_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
) -> Result<()> {
    let rows = fetch_orphaned_scope_emitters(
        sqlx::query(orphaned_delete_scope_emitters_sql()),
        replay_target_block,
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
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    query
        .bind(replay_target_block)
        .bind(reverse_claim_derivation_kind())
        .bind(reverse_claim_source_families())
        .bind(subregistry_derivation_kinds())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_derivation_kind())
        .bind(unwrapped_authority_source_families())
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
    WITH scope_rule_pairs AS (
        SELECT
            $2::TEXT AS derivation_kind,
            source_family
        FROM unnest($3::TEXT[]) AS source_families(source_family)

        UNION ALL

        SELECT derivation_kind, source_family
        FROM unnest($4::TEXT[]) AS derivation_kinds(derivation_kind)
        CROSS JOIN unnest($5::TEXT[]) AS source_families(source_family)

        UNION ALL

        SELECT
            $6::TEXT AS derivation_kind,
            source_family
        FROM unnest($7::TEXT[]) AS source_families(source_family)
    ),
    active_targets AS (
        SELECT source_family, address, from_block, to_block
        FROM base_rederive_active_replay_targets
        WHERE from_block <= $1
          AND to_block >= 17571485
    ),
    delete_scope_log_events AS (
        SELECT
            event.derivation_kind,
            event.source_family,
            event.chain_id,
            event.block_number,
            event.block_hash,
            event.transaction_hash,
            event.log_index
        FROM scope_rule_pairs pair
        JOIN normalized_events event
          ON event.derivation_kind = pair.derivation_kind
         AND event.source_family = pair.source_family
        WHERE event.chain_id = 'base-mainnet'
          AND event.block_number BETWEEN 17571485 AND $1
          AND event.block_hash IS NOT NULL
          AND event.transaction_hash IS NOT NULL
          AND event.log_index IS NOT NULL
    )
    SELECT
        event.derivation_kind,
        event.source_family,
        event.block_number,
        event.block_hash,
        event.transaction_hash,
        event.log_index,
        raw_log.emitting_address
    FROM delete_scope_log_events event
    JOIN LATERAL (
        SELECT LOWER(raw_log.emitting_address) AS emitting_address
        FROM raw_logs raw_log
        WHERE raw_log.chain_id = event.chain_id
          AND raw_log.block_hash = event.block_hash
          AND raw_log.transaction_hash = event.transaction_hash
          AND raw_log.log_index = event.log_index
          AND raw_log.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
    ) raw_log ON TRUE
    WHERE NOT EXISTS (
          SELECT 1
          FROM active_targets target
          WHERE target.source_family = event.source_family
            AND target.address = raw_log.emitting_address
            AND target.from_block <= event.block_number
            AND target.to_block >= event.block_number
      )
    LIMIT 10
    "#
}
