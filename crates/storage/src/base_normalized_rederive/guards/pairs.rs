use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Row};

use super::super::{
    BaseNormalizedRederiveReplayTargetSnapshot, reverse_claim_derivation_kind,
    reverse_claim_source_families, subregistry_derivation_kinds, subregistry_source_families,
    unwrapped_authority_derivation_kind, unwrapped_authority_source_families,
};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DeleteScopePair {
    derivation_kind: String,
    source_family: String,
    replay_adapter: String,
}

pub(super) async fn ensure_delete_scope_pairs_replay_active(
    pool: &PgPool,
    replay_target_block: i64,
    active_replay_target_snapshot: &[BaseNormalizedRederiveReplayTargetSnapshot],
) -> Result<()> {
    let rows = bind_inactive_delete_scope_pairs(
        sqlx::query(inactive_delete_scope_pairs_sql()),
        replay_target_block,
        active_replay_target_snapshot,
    )
    .fetch_all(pool)
    .await
    .context(
        "failed to validate Base delete-scope source families against active replay manifests",
    )?;
    ensure_inactive_delete_scope_pairs_empty(delete_scope_pairs_from_rows(rows)?)
}

pub(super) async fn ensure_delete_scope_pairs_replay_active_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
    active_replay_target_snapshot: &[BaseNormalizedRederiveReplayTargetSnapshot],
) -> Result<()> {
    let rows = bind_inactive_delete_scope_pairs(
        sqlx::query(inactive_delete_scope_pairs_sql()),
        replay_target_block,
        active_replay_target_snapshot,
    )
    .fetch_all(&mut **transaction)
    .await
    .context(
        "failed to validate Base delete-scope source families against active replay manifests",
    )?;
    ensure_inactive_delete_scope_pairs_empty(delete_scope_pairs_from_rows(rows)?)
}

fn bind_inactive_delete_scope_pairs<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    replay_target_block: i64,
    active_replay_target_snapshot: &[BaseNormalizedRederiveReplayTargetSnapshot],
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    let replay_adapters = active_replay_target_snapshot
        .iter()
        .map(|target| target.replay_adapter.clone())
        .collect::<Vec<_>>();
    let source_families = active_replay_target_snapshot
        .iter()
        .map(|target| target.source_family.clone())
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
        .bind(replay_adapters)
        .bind(source_families)
        .bind(from_blocks)
        .bind(to_blocks)
}

fn ensure_inactive_delete_scope_pairs_empty(missing_pairs: Vec<DeleteScopePair>) -> Result<()> {
    if missing_pairs.is_empty() {
        return Ok(());
    }

    let missing = missing_pairs
        .into_iter()
        .map(|pair| {
            format!(
                "{derivation_kind}/{source_family} adapter={}",
                pair.replay_adapter,
                derivation_kind = pair.derivation_kind,
                source_family = pair.source_family,
            )
        })
        .collect::<Vec<_>>();
    bail!(
        "Base normalized-event rederive delete scope contains rows current full-closure replay will not re-emit: {}",
        missing.join(", ")
    );
}

fn delete_scope_pairs_from_rows(rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<DeleteScopePair>> {
    rows.into_iter()
        .map(|row| {
            Ok(DeleteScopePair {
                derivation_kind: row.try_get("derivation_kind")?,
                source_family: row.try_get("source_family")?,
                replay_adapter: row.try_get("replay_adapter")?,
            })
        })
        .collect()
}

pub(super) fn inactive_delete_scope_pairs_sql() -> &'static str {
    r#"
    WITH scope_rule_pairs AS (
        SELECT
            $2::TEXT AS derivation_kind,
            source_family,
            'ens_v1_reverse_claim'::TEXT AS replay_adapter
        FROM unnest($3::TEXT[]) AS source_families(source_family)

        UNION ALL

        SELECT
            derivation_kind,
            source_family,
            'ens_v1_subregistry_discovery'::TEXT AS replay_adapter
        FROM unnest($4::TEXT[]) AS derivation_kinds(derivation_kind)
        CROSS JOIN unnest($5::TEXT[]) AS source_families(source_family)

        UNION ALL

        SELECT
            $6::TEXT AS derivation_kind,
            source_family,
            'ens_v1_unwrapped_authority'::TEXT AS replay_adapter
        FROM unnest($7::TEXT[]) AS source_families(source_family)
    ),
    log_derived_delete_scope_pairs AS (
        SELECT pair.derivation_kind, pair.source_family, pair.replay_adapter
        FROM scope_rule_pairs pair
        WHERE EXISTS (
            SELECT 1
            FROM (
                SELECT 1
                FROM normalized_events event
                WHERE event.chain_id = 'base-mainnet'
                  AND event.block_number BETWEEN 17571485 AND $1
                  AND event.block_hash IS NOT NULL
                  AND event.derivation_kind = pair.derivation_kind
                  AND event.source_family = pair.source_family
                  AND NOT (
                      pair.replay_adapter = 'ens_v1_unwrapped_authority'
                      AND event.transaction_hash IS NULL
                      AND event.log_index IS NULL
                      AND event.raw_fact_ref ->> 'kind' IS NOT DISTINCT FROM 'raw_block'
                  )
                LIMIT 1
            ) non_boundary_event
        )
    ),
    active_targets AS (
        SELECT
            replay_adapter,
            source_family,
            GREATEST(from_block, 17571485) AS from_block,
            LEAST(to_block, $1) AS to_block
        FROM unnest(
            $8::TEXT[],
            $9::TEXT[],
            $10::BIGINT[],
            $11::BIGINT[]
        ) AS target(replay_adapter, source_family, from_block, to_block)
        WHERE from_block <= $1
          AND to_block >= 17571485
    ),
    ordered_active_targets AS (
        SELECT
            replay_adapter,
            source_family,
            from_block,
            to_block,
            MAX(to_block) OVER (
                PARTITION BY replay_adapter, source_family
                ORDER BY from_block, to_block
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS prior_max_to_block
        FROM active_targets
    ),
    covered_replay_pairs AS (
        SELECT replay_adapter, source_family
        FROM ordered_active_targets
        GROUP BY replay_adapter, source_family
        HAVING MIN(from_block) <= 17571485
           AND MAX(to_block) >= $1
           AND NOT COALESCE(
               BOOL_OR(
                   prior_max_to_block IS NOT NULL
                   AND from_block > prior_max_to_block + 1
               ),
               FALSE
           )
    ),
    uncovered_basenames_registry_boundary_pairs AS MATERIALIZED (
        SELECT pair.derivation_kind, pair.source_family, pair.replay_adapter
        FROM scope_rule_pairs pair
        WHERE pair.replay_adapter = 'ens_v1_unwrapped_authority'
          AND pair.source_family = 'ens_v1_registry_l1'
          AND NOT EXISTS (
            SELECT 1
            FROM covered_replay_pairs covered
            WHERE covered.replay_adapter = pair.replay_adapter
              AND covered.source_family = 'basenames_base_registry'
        )
    ),
    uncovered_stored_family_boundary_pairs AS MATERIALIZED (
        SELECT pair.derivation_kind, pair.source_family, pair.replay_adapter
        FROM scope_rule_pairs pair
        WHERE pair.replay_adapter = 'ens_v1_unwrapped_authority'
          AND NOT EXISTS (
            SELECT 1
            FROM covered_replay_pairs covered
            WHERE covered.replay_adapter = pair.replay_adapter
              AND covered.source_family = pair.source_family
        )
    ),
    closure_boundary_rederive_families AS (
        SELECT
            pair.derivation_kind,
            pair.source_family,
            pair.replay_adapter,
            'basenames_base_registry'::TEXT AS boundary_rederive_source_family
        FROM uncovered_basenames_registry_boundary_pairs pair
        WHERE EXISTS (
            SELECT 1
            FROM (
                SELECT 1
                FROM normalized_events event
                WHERE event.chain_id = 'base-mainnet'
                  AND event.block_number BETWEEN 17571485 AND $1
                  AND event.block_hash IS NOT NULL
                  AND event.derivation_kind = pair.derivation_kind
                  AND event.source_family = pair.source_family
                  AND event.namespace = 'basenames'
                  AND event.transaction_hash IS NULL
                  AND event.log_index IS NULL
                  AND event.raw_fact_ref ->> 'kind' IS NOT DISTINCT FROM 'raw_block'
                LIMIT 1
            ) basenames_boundary_event
        )

        UNION ALL

        SELECT
            pair.derivation_kind,
            pair.source_family,
            pair.replay_adapter,
            pair.source_family AS boundary_rederive_source_family
        FROM uncovered_stored_family_boundary_pairs pair
        WHERE EXISTS (
            SELECT 1
            FROM (
                SELECT 1
                FROM normalized_events event
                WHERE event.chain_id = 'base-mainnet'
                  AND event.block_number BETWEEN 17571485 AND $1
                  AND event.block_hash IS NOT NULL
                  AND event.derivation_kind = pair.derivation_kind
                  AND event.source_family = pair.source_family
                  AND event.transaction_hash IS NULL
                  AND event.log_index IS NULL
                  AND event.raw_fact_ref ->> 'kind' IS NOT DISTINCT FROM 'raw_block'
                  AND NOT (
                      pair.replay_adapter = 'ens_v1_unwrapped_authority'
                      AND pair.source_family = 'ens_v1_registry_l1'
                      AND event.namespace = 'basenames'
                  )
                LIMIT 1
            ) stored_family_boundary_event
        )
    ),
    inactive_log_pairs AS (
        SELECT pair.derivation_kind, pair.source_family, pair.replay_adapter
        FROM log_derived_delete_scope_pairs pair
        WHERE NOT EXISTS (
            SELECT 1
            FROM covered_replay_pairs covered
            WHERE covered.replay_adapter = pair.replay_adapter
              AND covered.source_family = pair.source_family
        )
    ),
    inactive_closure_boundary_pairs AS (
        SELECT pair.derivation_kind, pair.source_family, pair.replay_adapter
        FROM closure_boundary_rederive_families pair
        WHERE NOT EXISTS (
            SELECT 1
            FROM covered_replay_pairs covered
            WHERE covered.replay_adapter = pair.replay_adapter
              AND covered.source_family = pair.boundary_rederive_source_family
        )
    )
    SELECT pair.derivation_kind, pair.source_family, pair.replay_adapter
    FROM inactive_log_pairs pair
    UNION
    SELECT pair.derivation_kind, pair.source_family, pair.replay_adapter
    FROM inactive_closure_boundary_pairs pair
    ORDER BY derivation_kind, source_family
    "#
}
