use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bigname_manifests::{WatchedContract, load_watched_contracts};
use sqlx::PgPool;

use super::{
    CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS, RawLogBounds, ReplaySourceScope, ReplaySourceTarget,
};

pub(super) async fn load_replay_source_scopes(
    pool: &PgPool,
    chain: &str,
) -> Result<Vec<ReplaySourceScope>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .with_context(|| format!("failed to load watched contracts for replay chain {chain}"))?;
    let mut targets_by_source = BTreeMap::<String, Vec<ReplaySourceTarget>>::new();
    for contract in watched_contracts {
        if contract.chain != chain {
            continue;
        }
        let target = replay_source_target(&contract);
        if target.from_block > target.to_block {
            continue;
        }
        targets_by_source
            .entry(contract.source_family)
            .or_default()
            .push(target);
    }

    let mut scopes = Vec::with_capacity(targets_by_source.len());
    for (source_family, mut targets) in targets_by_source {
        targets.sort();
        targets.dedup();
        scopes.push(ReplaySourceScope {
            cursor_kind: source_cursor_kind(&source_family),
            source_family,
            targets,
        });
    }

    Ok(scopes)
}

fn replay_source_target(contract: &WatchedContract) -> ReplaySourceTarget {
    ReplaySourceTarget {
        address: contract.address.to_ascii_lowercase(),
        from_block: contract.active_from_block_number.unwrap_or(0),
        to_block: contract.active_to_block_number.unwrap_or(i64::MAX),
    }
}

fn source_cursor_kind(source_family: &str) -> String {
    format!("{CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS}:source_family={source_family}")
}

pub(super) async fn load_canonical_raw_log_bounds(
    pool: &PgPool,
    chain: &str,
    source_scope: &ReplaySourceScope,
) -> Result<Option<RawLogBounds>> {
    if source_scope.targets.is_empty() {
        return Ok(None);
    }
    let (addresses, from_blocks, to_blocks) = source_scope_bindings(source_scope);
    let start_block = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT block_number
        FROM raw_logs
        WHERE chain_id = $1
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND EXISTS (
              SELECT 1
              FROM unnest($2::TEXT[], $3::BIGINT[], $4::BIGINT[])
                AS source_scope(address, from_block, to_block)
              WHERE LOWER(raw_logs.emitting_address) = source_scope.address
                AND raw_logs.block_number >= source_scope.from_block
                AND raw_logs.block_number <= source_scope.to_block
          )
        ORDER BY block_number ASC
        LIMIT 1
        "#,
    )
    .bind(chain)
    .bind(&addresses)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load first canonical raw-log block for chain {chain}"))?;

    let Some(start_block) = start_block else {
        return Ok(None);
    };

    let target_block = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT block_number
        FROM raw_logs
        WHERE chain_id = $1
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND EXISTS (
              SELECT 1
              FROM unnest($2::TEXT[], $3::BIGINT[], $4::BIGINT[])
                AS source_scope(address, from_block, to_block)
              WHERE LOWER(raw_logs.emitting_address) = source_scope.address
                AND raw_logs.block_number >= source_scope.from_block
                AND raw_logs.block_number <= source_scope.to_block
          )
        ORDER BY block_number DESC
        LIMIT 1
        "#,
    )
    .bind(chain)
    .bind(&addresses)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load latest canonical raw-log block for chain {chain}"))?;

    Ok(Some(RawLogBounds {
        start_block,
        target_block,
    }))
}

pub(super) async fn select_log_bounded_replay_to_block(
    pool: &PgPool,
    chain: &str,
    source_scope: &ReplaySourceScope,
    from_block: i64,
    hard_to_block: i64,
    max_raw_logs_per_chunk: usize,
) -> Result<i64> {
    if from_block >= hard_to_block {
        return Ok(hard_to_block);
    }
    let max_raw_logs_per_chunk = i64::try_from(max_raw_logs_per_chunk)
        .context("normalized replay max logs per chunk does not fit in i64")?;
    let (addresses, from_blocks, to_blocks) = source_scope_bindings(source_scope);

    sqlx::query_scalar::<_, i64>(
        r#"
        WITH block_counts AS (
            SELECT block_number, COUNT(*)::BIGINT AS log_count
            FROM raw_logs
            WHERE chain_id = $1
              AND block_number >= $2
              AND block_number <= $3
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND EXISTS (
                  SELECT 1
                  FROM unnest($5::TEXT[], $6::BIGINT[], $7::BIGINT[])
                    AS source_scope(address, from_block, to_block)
                  WHERE LOWER(raw_logs.emitting_address) = source_scope.address
                    AND raw_logs.block_number >= source_scope.from_block
                    AND raw_logs.block_number <= source_scope.to_block
              )
            GROUP BY block_number
        ),
        running AS (
            SELECT
                block_number,
                SUM(log_count) OVER (ORDER BY block_number) AS running_log_count,
                ROW_NUMBER() OVER (ORDER BY block_number) AS ordinal
            FROM block_counts
        ),
        bounded AS (
            SELECT block_number
            FROM running
            WHERE running_log_count <= $4
            UNION ALL
            SELECT block_number
            FROM running
            WHERE ordinal = 1
        )
        SELECT COALESCE(MAX(block_number), $3)
        FROM bounded
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(hard_to_block)
    .bind(max_raw_logs_per_chunk)
    .bind(&addresses)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to select log-bounded normalized replay range for chain {chain} range {from_block}..={hard_to_block}"
        )
    })
}

pub(super) fn source_scope_bindings(
    source_scope: &ReplaySourceScope,
) -> (Vec<String>, Vec<i64>, Vec<i64>) {
    let mut addresses = Vec::with_capacity(source_scope.targets.len());
    let mut from_blocks = Vec::with_capacity(source_scope.targets.len());
    let mut to_blocks = Vec::with_capacity(source_scope.targets.len());

    for target in &source_scope.targets {
        addresses.push(target.address.clone());
        from_blocks.push(target.from_block);
        to_blocks.push(target.to_block);
    }

    (addresses, from_blocks, to_blocks)
}
