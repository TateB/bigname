use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use sqlx::Row;

use super::ReplayRawLogSelection;
use crate::{
    ens_v1_resolver::{
        GENERIC_SOURCE_SCOPE_ADDRESS, SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
        generic_resolver_record_topic0s,
    },
    reconciliation::types::RawFactNormalizedEventReplaySourceScope,
};

pub(super) async fn load_replay_raw_log_selection_for_scoped_range(
    pool: &sqlx::PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
    source_scope: &[RawFactNormalizedEventReplaySourceScope],
) -> Result<ReplayRawLogSelection> {
    let source_scope = normalized_source_scope_for_range(source_scope, from_block, to_block)?;
    if source_scope.is_empty() {
        return Ok(ReplayRawLogSelection {
            range: Some((from_block, to_block)),
            block_hashes: Vec::new(),
            address_targets: Vec::new(),
            canonical_raw_log_count: 0,
        });
    }
    let (source_families, addresses, from_blocks, to_blocks) =
        source_scope_filter_bindings(&source_scope);
    let ens_v1_resolver_event_topic0s = generic_resolver_record_topic0s();

    let canonical_raw_log_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM raw_logs AS logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = logs.chain_id
         AND lineage.block_hash = logs.block_hash
        WHERE logs.chain_id = $1
          AND logs.block_number >= $2
          AND logs.block_number <= $3
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND EXISTS (
              SELECT 1
              FROM unnest($4::TEXT[], $5::TEXT[], $6::BIGINT[], $7::BIGINT[])
                AS source_scope(source_family, address, from_block, to_block)
              WHERE (
                    LOWER(logs.emitting_address) = source_scope.address
                    OR (
                        source_scope.source_family = $8
                        AND source_scope.address = $9
                        AND LOWER(logs.topics[1]) = ANY($10::TEXT[])
                    )
                )
                AND logs.block_number >= source_scope.from_block
                AND logs.block_number <= source_scope.to_block
          )
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .bind(&source_families)
    .bind(&addresses)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .bind(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    .bind(GENERIC_SOURCE_SCOPE_ADDRESS)
    .bind(&ens_v1_resolver_event_topic0s)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to count scoped canonical raw log replay inputs for chain {chain} range {from_block}..={to_block}"
        )
    })?;
    let canonical_raw_log_count = usize::try_from(canonical_raw_log_count)
        .context("scoped canonical raw log count overflowed usize")?;

    let block_rows = sqlx::query(
        r#"
        SELECT logs.block_number, logs.block_hash
        FROM raw_logs AS logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = logs.chain_id
         AND lineage.block_hash = logs.block_hash
        WHERE logs.chain_id = $1
          AND logs.block_number >= $2
          AND logs.block_number <= $3
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND EXISTS (
              SELECT 1
              FROM unnest($4::TEXT[], $5::TEXT[], $6::BIGINT[], $7::BIGINT[])
                AS source_scope(source_family, address, from_block, to_block)
              WHERE (
                    LOWER(logs.emitting_address) = source_scope.address
                    OR (
                        source_scope.source_family = $8
                        AND source_scope.address = $9
                        AND LOWER(logs.topics[1]) = ANY($10::TEXT[])
                    )
                )
                AND logs.block_number >= source_scope.from_block
                AND logs.block_number <= source_scope.to_block
          )
        GROUP BY logs.block_number, logs.block_hash
        ORDER BY logs.block_number, logs.block_hash
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .bind(&source_families)
    .bind(&addresses)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .bind(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    .bind(GENERIC_SOURCE_SCOPE_ADDRESS)
    .bind(&ens_v1_resolver_event_topic0s)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to list scoped canonical raw log replay block hashes for chain {chain} range {from_block}..={to_block}"
        )
    })?;
    let block_hashes = block_rows
        .into_iter()
        .map(|row| row.get::<String, _>("block_hash"))
        .collect::<Vec<_>>();

    let address_rows = sqlx::query(
        r#"
        SELECT LOWER(logs.emitting_address) AS emitting_address
        FROM raw_logs AS logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = logs.chain_id
         AND lineage.block_hash = logs.block_hash
        WHERE logs.chain_id = $1
          AND logs.block_number >= $2
          AND logs.block_number <= $3
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND EXISTS (
              SELECT 1
              FROM unnest($4::TEXT[], $5::TEXT[], $6::BIGINT[], $7::BIGINT[])
                AS source_scope(source_family, address, from_block, to_block)
              WHERE (
                    LOWER(logs.emitting_address) = source_scope.address
                    OR (
                        source_scope.source_family = $8
                        AND source_scope.address = $9
                        AND LOWER(logs.topics[1]) = ANY($10::TEXT[])
                    )
                )
                AND logs.block_number >= source_scope.from_block
                AND logs.block_number <= source_scope.to_block
          )
        GROUP BY LOWER(logs.emitting_address)
        ORDER BY LOWER(logs.emitting_address)
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .bind(&source_families)
    .bind(&addresses)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .bind(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    .bind(GENERIC_SOURCE_SCOPE_ADDRESS)
    .bind(&ens_v1_resolver_event_topic0s)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to list scoped canonical raw log replay emitters for chain {chain} range {from_block}..={to_block}"
        )
    })?;
    let address_targets = address_rows
        .into_iter()
        .map(|row| (chain.to_owned(), row.get::<String, _>("emitting_address")))
        .collect::<Vec<_>>();

    Ok(ReplayRawLogSelection {
        range: Some((from_block, to_block)),
        block_hashes,
        address_targets,
        canonical_raw_log_count,
    })
}

pub(super) fn replay_source_scope_from_requested_scope(
    source_scope: &[RawFactNormalizedEventReplaySourceScope],
    from_block: i64,
    to_block: i64,
) -> Result<Vec<(String, String, i64, i64)>> {
    Ok(
        normalized_source_scope_for_range(source_scope, from_block, to_block)?
            .into_iter()
            .map(|target| {
                (
                    target.source_family,
                    target.address,
                    target.from_block,
                    target.to_block,
                )
            })
            .collect(),
    )
}

fn normalized_source_scope_for_range(
    source_scope: &[RawFactNormalizedEventReplaySourceScope],
    from_block: i64,
    to_block: i64,
) -> Result<Vec<RawFactNormalizedEventReplaySourceScope>> {
    let mut normalized = BTreeSet::new();
    for target in source_scope {
        if target.source_family.trim().is_empty() {
            bail!("scoped raw-fact replay source_family must not be empty");
        }
        if target.address.trim().is_empty() {
            bail!("scoped raw-fact replay address must not be empty");
        }
        if target.from_block > target.to_block {
            bail!(
                "scoped raw-fact replay source range {}..={} is invalid for {} {}",
                target.from_block,
                target.to_block,
                target.source_family,
                target.address
            );
        }
        let effective_from_block = target.from_block.max(from_block);
        let effective_to_block = target.to_block.min(to_block);
        if effective_from_block > effective_to_block {
            continue;
        }
        normalized.insert((
            target.source_family.clone(),
            target.address.to_ascii_lowercase(),
            effective_from_block,
            effective_to_block,
        ));
    }

    Ok(normalized
        .into_iter()
        .map(|(source_family, address, from_block, to_block)| {
            RawFactNormalizedEventReplaySourceScope {
                source_family,
                address,
                from_block,
                to_block,
            }
        })
        .collect())
}

fn source_scope_filter_bindings(
    source_scope: &[RawFactNormalizedEventReplaySourceScope],
) -> (Vec<String>, Vec<String>, Vec<i64>, Vec<i64>) {
    let mut source_families = Vec::with_capacity(source_scope.len());
    let mut addresses = Vec::with_capacity(source_scope.len());
    let mut from_blocks = Vec::with_capacity(source_scope.len());
    let mut to_blocks = Vec::with_capacity(source_scope.len());
    for target in source_scope {
        source_families.push(target.source_family.clone());
        addresses.push(target.address.clone());
        from_blocks.push(target.from_block);
        to_blocks.push(target.to_block);
    }

    (source_families, addresses, from_blocks, to_blocks)
}
