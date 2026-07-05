use anyhow::{Context, Result, bail};
use sqlx::Row;

use super::super::{
    BaseNormalizedRederiveRatifiedDroppedEmitterCensus, reverse_claim_derivation_kind,
    reverse_claim_source_families, subregistry_derivation_kinds, subregistry_source_families,
    unwrapped_authority_derivation_kind, unwrapped_authority_source_families,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RatifiedDroppedOrphanEmitter {
    derivation_kind: &'static str,
    source_family: &'static str,
    emitting_address: &'static str,
    event_kind: &'static str,
    source_event: &'static str,
    coin_type: &'static str,
    from_block: i64,
    to_block: i64,
    ratification: &'static str,
    reason: &'static str,
}

// Ratified 2026-07-05 option A: the legacy Basenames ReverseRegistrar
// ReverseChanged/BaseReverseClaimed coin-type 60 class is deprecated/superseded
// by the ENS Base L2ReverseRegistrar authority. Rows matching this exact class
// are deliberately deleted and not re-derived.
// Upstream anchors: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
// (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
// (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
// (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f)
const RATIFIED_DROPPED_ORPHAN_EMITTERS: &[RatifiedDroppedOrphanEmitter] = &[
    RatifiedDroppedOrphanEmitter {
        derivation_kind: "ens_v1_reverse_claim",
        source_family: "basenames_base_primary",
        emitting_address: "0x79ea96012eea67a83431f1701b3dff7e37f9e282",
        event_kind: "ReverseChanged",
        source_event: "BaseReverseClaimed",
        coin_type: "60",
        from_block: 17_575_714,
        to_block: 46_903_158,
        ratification: "2026-07-05 option A",
        reason: "deprecated legacy Basenames ReverseRegistrar superseded by ENS Base L2ReverseRegistrar; rows deliberately dropped, not re-derived",
    },
];

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
    let sql = orphaned_delete_scope_emitters_sql();
    let rows = fetch_orphaned_scope_emitters(sqlx::query(&sql), replay_target_block)
        .fetch_all(&mut **transaction)
        .await
        .context(
            "failed to validate Base delete-scope emitters against active replay target addresses",
        )?;
    ensure_orphaned_scope_emitters_empty(orphaned_scope_emitters_from_rows(rows)?)
}

pub(super) async fn load_ratified_dropped_orphan_emitter_census_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
) -> Result<Vec<BaseNormalizedRederiveRatifiedDroppedEmitterCensus>> {
    let sql = ratified_dropped_orphan_emitter_census_sql();
    let rows = sqlx::query(&sql)
        .bind(replay_target_block)
        .fetch_all(&mut **transaction)
        .await
        .context("failed to load Base ratified dropped orphan-emitter census")?;
    ratified_dropped_orphan_emitter_census_from_rows(rows)
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

fn ratified_dropped_orphan_emitter_census_from_rows(
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<BaseNormalizedRederiveRatifiedDroppedEmitterCensus>> {
    rows.into_iter()
        .map(|row| {
            Ok(BaseNormalizedRederiveRatifiedDroppedEmitterCensus {
                derivation_kind: row.try_get("derivation_kind")?,
                source_family: row.try_get("source_family")?,
                emitting_address: row.try_get("emitting_address")?,
                row_count: row.try_get("row_count")?,
                min_block_number: row.try_get("min_block_number")?,
                max_block_number: row.try_get("max_block_number")?,
                ratification: row.try_get("ratification")?,
                reason: row.try_get("reason")?,
            })
        })
        .collect()
}

pub(super) fn orphaned_delete_scope_emitters_sql() -> String {
    format!(
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
    {ratified_dropped_orphan_emitters_cte},
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
            event.event_kind,
            event.block_number,
            event.block_hash,
            event.transaction_hash,
            event.log_index,
            event.after_state
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
      AND NOT EXISTS (
          SELECT 1
          FROM ratified_dropped_orphan_emitters ratified
          WHERE ratified.derivation_kind = event.derivation_kind
            AND ratified.source_family = event.source_family
            AND ratified.emitting_address = raw_log.emitting_address
            AND event.block_number BETWEEN ratified.from_block AND ratified.to_block
            AND ratified.event_kind = event.event_kind
            AND ratified.source_event = event.after_state ->> 'source_event'
            AND ratified.coin_type = event.after_state ->> 'coin_type'
      )
    LIMIT 10
    "#,
        ratified_dropped_orphan_emitters_cte = ratified_dropped_orphan_emitters_cte_sql()
    )
}

pub(super) fn ratified_dropped_orphan_emitter_census_sql() -> String {
    format!(
        r#"
    WITH {ratified_dropped_orphan_emitters_cte},
    ratified_dropped_events AS (
        SELECT
            ratified.derivation_kind,
            ratified.source_family,
            ratified.emitting_address,
            ratified.ratification,
            ratified.reason,
            event.block_number
        FROM ratified_dropped_orphan_emitters ratified
        JOIN normalized_events event
          ON event.derivation_kind = ratified.derivation_kind
         AND event.source_family = ratified.source_family
        WHERE event.chain_id = 'base-mainnet'
          AND event.block_number BETWEEN ratified.from_block AND LEAST(ratified.to_block, $1)
          AND event.event_kind = ratified.event_kind
          AND event.after_state ->> 'source_event' = ratified.source_event
          AND event.after_state ->> 'coin_type' = ratified.coin_type
          AND event.block_hash IS NOT NULL
          AND event.transaction_hash IS NOT NULL
          AND event.log_index IS NOT NULL
          AND EXISTS (
              SELECT 1
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
                AND LOWER(raw_log.emitting_address) = ratified.emitting_address
          )
    )
    SELECT
        derivation_kind,
        source_family,
        emitting_address,
        COUNT(*)::BIGINT AS row_count,
        MIN(block_number)::BIGINT AS min_block_number,
        MAX(block_number)::BIGINT AS max_block_number,
        ratification,
        reason
    FROM ratified_dropped_events
    GROUP BY derivation_kind, source_family, emitting_address, ratification, reason
    ORDER BY derivation_kind, source_family, emitting_address
    "#,
        ratified_dropped_orphan_emitters_cte = ratified_dropped_orphan_emitters_cte_sql()
    )
}

fn ratified_dropped_orphan_emitters_cte_sql() -> String {
    let rows = RATIFIED_DROPPED_ORPHAN_EMITTERS
        .iter()
        .map(|emitter| {
            format!(
                "SELECT {}::TEXT AS derivation_kind, {}::TEXT AS source_family, {}::TEXT AS emitting_address, {}::TEXT AS event_kind, {}::TEXT AS source_event, {}::TEXT AS coin_type, {}::BIGINT AS from_block, {}::BIGINT AS to_block, {}::TEXT AS ratification, {}::TEXT AS reason",
                sql_text_literal(emitter.derivation_kind),
                sql_text_literal(emitter.source_family),
                sql_text_literal(emitter.emitting_address),
                sql_text_literal(emitter.event_kind),
                sql_text_literal(emitter.source_event),
                sql_text_literal(emitter.coin_type),
                emitter.from_block,
                emitter.to_block,
                sql_text_literal(emitter.ratification),
                sql_text_literal(emitter.reason),
            )
        })
        .collect::<Vec<_>>()
        .join("\n        UNION ALL\n        ");
    format!("ratified_dropped_orphan_emitters AS (\n        {rows}\n    )")
}

fn sql_text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
