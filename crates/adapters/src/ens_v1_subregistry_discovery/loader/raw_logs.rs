use bigname_storage::sql_row;
use std::collections::HashMap;

use super::super::{
    CONTRACT_ROLE_REGISTRY_OLD,
    hex_topic::{
        new_owner_topic0, new_resolver_topic0, new_ttl_topic0, normalize_address,
        registry_transfer_topic0,
    },
    scope::{
        RegistryRawLogSourceScopeTarget, emitter_for_block_and_scope,
        scoped_ranges_for_active_emitters,
    },
};
use super::{ActiveEmitter, RegistryRawLogRow};
use anyhow::{Context, Result};
use futures_util::TryStreamExt;
use sqlx::PgPool;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ens_v1_subregistry_discovery) struct RegistryRawLogPosition {
    pub(in crate::ens_v1_subregistry_discovery) block_number: i64,
    pub(in crate::ens_v1_subregistry_discovery) transaction_index: i64,
    pub(in crate::ens_v1_subregistry_discovery) log_index: i64,
    pub(in crate::ens_v1_subregistry_discovery) emitting_address: String,
}

#[derive(Clone, Debug)]
pub(in crate::ens_v1_subregistry_discovery) struct RegistryRawLogPage {
    pub(in crate::ens_v1_subregistry_discovery) raw_logs: Vec<RegistryRawLogRow>,
    pub(in crate::ens_v1_subregistry_discovery) last_position: Option<RegistryRawLogPosition>,
}

pub(in crate::ens_v1_subregistry_discovery) async fn load_registry_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[RegistryRawLogSourceScopeTarget]>,
) -> Result<Vec<RegistryRawLogRow>> {
    let block_range = source_scope.and_then(registry_source_scope_block_range);
    load_registry_raw_logs_internal(
        pool,
        chain,
        emitters,
        restrict_to_block_hashes,
        block_hashes,
        source_scope,
        block_range,
    )
    .await
}

pub(in crate::ens_v1_subregistry_discovery) async fn stream_registry_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    mut handle_raw_log: impl FnMut(RegistryRawLogRow) -> Result<()>,
) -> Result<usize> {
    if emitters.is_empty() {
        return Ok(0);
    }

    let mut emitters_by_address = HashMap::<String, Vec<ActiveEmitter>>::new();
    for emitter in emitters.iter().cloned() {
        emitters_by_address
            .entry(emitter.address.clone())
            .or_default()
            .push(emitter);
    }
    let assignment_topics = [new_owner_topic0(), new_resolver_topic0()];
    let old_registry_migration_topics = [registry_transfer_topic0(), new_ttl_topic0()];
    let old_registry_addresses = emitters_by_address
        .values()
        .filter_map(|address_emitters| {
            address_emitters
                .iter()
                .find(|emitter| {
                    emitter.contract_role.as_deref() == Some(CONTRACT_ROLE_REGISTRY_OLD)
                })
                .map(|emitter| emitter.address.clone())
        })
        .collect::<Vec<_>>();

    let mut rows = sqlx::query(
        r#"
        SELECT *
        FROM (
            SELECT
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state::TEXT AS canonicality_state
            FROM raw_logs
            WHERE chain_id = $1
              AND lower(topics[1]) = ANY($2::TEXT[])
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            UNION ALL
            SELECT
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state::TEXT AS canonicality_state
            FROM raw_logs
            WHERE chain_id = $1
              AND lower(emitting_address) = ANY($4::TEXT[])
              AND lower(topics[1]) = ANY($3::TEXT[])
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        ) selected_raw_logs
        ORDER BY block_number, transaction_index, log_index, emitting_address
        "#,
    )
    .bind(chain)
    .bind(&assignment_topics)
    .bind(&old_registry_migration_topics)
    .bind(&old_registry_addresses)
    .fetch(pool);

    let mut scanned_log_count = 0usize;
    while let Some(row) = rows
        .try_next()
        .await
        .with_context(|| format!("failed to stream ENSv1 registry raw logs for chain {chain}"))?
    {
        let emitting_address =
            normalize_address(&sql_row::get::<String>(&row, "emitting_address")?);
        let block_number = sql_row::get(&row, "block_number")?;
        let Some(emitter) = emitters_by_address
            .get(&emitting_address)
            .and_then(|emitters| emitter_for_block_and_scope(emitters, block_number, None))
        else {
            continue;
        };
        let raw_log = registry_raw_log_from_row(row, emitting_address, block_number, emitter)?;
        handle_raw_log(raw_log)?;
        scanned_log_count += 1;
    }
    Ok(scanned_log_count)
}

pub(in crate::ens_v1_subregistry_discovery) async fn load_registry_raw_log_checkpoint_page(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    from_block: i64,
    to_block: i64,
    start_after: Option<&RegistryRawLogPosition>,
    limit: i64,
) -> Result<RegistryRawLogPage> {
    if emitters.is_empty() {
        return Ok(RegistryRawLogPage {
            raw_logs: Vec::new(),
            last_position: None,
        });
    }

    let mut emitters_by_address = HashMap::<String, Vec<ActiveEmitter>>::new();
    for emitter in emitters.iter().cloned() {
        emitters_by_address
            .entry(emitter.address.clone())
            .or_default()
            .push(emitter);
    }
    let assignment_topics = [new_owner_topic0(), new_resolver_topic0()];
    let old_registry_migration_topics = [registry_transfer_topic0(), new_ttl_topic0()];
    let old_registry_addresses = emitters_by_address
        .values()
        .filter_map(|address_emitters| {
            address_emitters
                .iter()
                .find(|emitter| {
                    emitter.contract_role.as_deref() == Some(CONTRACT_ROLE_REGISTRY_OLD)
                })
                .map(|emitter| emitter.address.clone())
        })
        .collect::<Vec<_>>();
    let last_block = start_after.map(|position| position.block_number);
    let last_transaction_index = start_after.map(|position| position.transaction_index);
    let last_log_index = start_after.map(|position| position.log_index);
    let last_emitting_address = start_after.map(|position| position.emitting_address.as_str());

    let rows = sqlx::query(
        r#"
        SELECT *
        FROM (
            SELECT
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state::TEXT AS canonicality_state
            FROM raw_logs
            WHERE chain_id = $1
              AND block_number BETWEEN $2 AND $3
              AND lower(topics[1]) = ANY($4::TEXT[])
              AND (
                  $7::BIGINT IS NULL
                  OR (block_number, transaction_index, log_index, lower(emitting_address))
                      > ($7::BIGINT, $8::BIGINT, $9::BIGINT, $10::TEXT)
              )
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            UNION ALL
            SELECT
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state::TEXT AS canonicality_state
            FROM raw_logs
            WHERE chain_id = $1
              AND block_number BETWEEN $2 AND $3
              AND lower(emitting_address) = ANY($6::TEXT[])
              AND lower(topics[1]) = ANY($5::TEXT[])
              AND (
                  $7::BIGINT IS NULL
                  OR (block_number, transaction_index, log_index, lower(emitting_address))
                      > ($7::BIGINT, $8::BIGINT, $9::BIGINT, $10::TEXT)
              )
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        ) selected_raw_logs
        ORDER BY block_number, transaction_index, log_index, lower(emitting_address)
        LIMIT $11
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .bind(&assignment_topics)
    .bind(&old_registry_migration_topics)
    .bind(&old_registry_addresses)
    .bind(last_block)
    .bind(last_transaction_index)
    .bind(last_log_index)
    .bind(last_emitting_address)
    .bind(limit)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load checkpointed ENSv1 registry raw-log page for chain {chain} range {from_block}..={to_block}"
        )
    })?;

    let mut raw_logs = Vec::new();
    let mut last_position = None;
    for row in rows {
        let emitting_address =
            normalize_address(&sql_row::get::<String>(&row, "emitting_address")?);
        let block_number = sql_row::get(&row, "block_number")?;
        let transaction_index = sql_row::get(&row, "transaction_index")?;
        let log_index = sql_row::get(&row, "log_index")?;
        last_position = Some(RegistryRawLogPosition {
            block_number,
            transaction_index,
            log_index,
            emitting_address: emitting_address.clone(),
        });
        let Some(emitter) = emitters_by_address
            .get(&emitting_address)
            .and_then(|emitters| emitter_for_block_and_scope(emitters, block_number, None))
        else {
            continue;
        };
        raw_logs.push(registry_raw_log_from_row(
            row,
            emitting_address,
            block_number,
            emitter,
        )?);
    }

    Ok(RegistryRawLogPage {
        raw_logs,
        last_position,
    })
}

async fn load_registry_raw_logs_internal(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[RegistryRawLogSourceScopeTarget]>,
    block_range: Option<(i64, i64)>,
) -> Result<Vec<RegistryRawLogRow>> {
    if emitters.is_empty() {
        return Ok(Vec::new());
    }

    let mut emitters_by_address = HashMap::<String, Vec<ActiveEmitter>>::new();
    for emitter in emitters.iter().cloned() {
        emitters_by_address
            .entry(emitter.address.clone())
            .or_default()
            .push(emitter);
    }
    let watched_addresses = emitters_by_address.keys().cloned().collect::<Vec<_>>();
    let watched_range_addresses = emitters
        .iter()
        .map(|emitter| emitter.address.clone())
        .collect::<Vec<_>>();
    let watched_effective_from_blocks = emitters
        .iter()
        .map(|emitter| emitter.active_from_block_number.unwrap_or(0))
        .collect::<Vec<_>>();
    let watched_effective_to_blocks = emitters
        .iter()
        .map(|emitter| emitter.active_to_block_number.unwrap_or(i64::MAX))
        .collect::<Vec<_>>();
    let scoped_ranges = source_scope
        .map(|source_scope| scoped_ranges_for_active_emitters(source_scope, emitters))
        .transpose()?;
    let (has_block_range, from_block, to_block) = block_range
        .map(|(from_block, to_block)| (true, from_block, to_block))
        .unwrap_or((false, 0, 0));
    let rows = if let Some(scoped_ranges) = scoped_ranges.as_ref() {
        if scoped_ranges.is_empty() {
            return Ok(Vec::new());
        }
        let scoped_addresses = scoped_ranges
            .iter()
            .map(|target| target.address.clone())
            .collect::<Vec<_>>();
        let scoped_from_blocks = scoped_ranges
            .iter()
            .map(|target| target.effective_from_block)
            .collect::<Vec<_>>();
        let scoped_to_blocks = scoped_ranges
            .iter()
            .map(|target| target.effective_to_block)
            .collect::<Vec<_>>();

        sqlx::query(
            r#"
            SELECT
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state::TEXT AS canonicality_state
            FROM raw_logs
            WHERE chain_id = $1
              AND lower(emitting_address) = ANY($2::TEXT[])
              AND ($3::BOOLEAN = FALSE OR block_hash = ANY($4::TEXT[]))
              AND ($8::BOOLEAN = FALSE OR block_number BETWEEN $9::BIGINT AND $10::BIGINT)
              AND EXISTS (
                  SELECT 1
                  FROM unnest($5::TEXT[], $6::BIGINT[], $7::BIGINT[]) AS watched(
                      address,
                      effective_from_block,
                      effective_to_block
                  )
                  WHERE watched.address = lower(emitting_address)
                    AND block_number BETWEEN watched.effective_from_block
                        AND watched.effective_to_block
              )
              AND EXISTS (
                  SELECT 1
                  FROM unnest($11::TEXT[], $12::BIGINT[], $13::BIGINT[]) AS scoped(
                      address,
                      effective_from_block,
                      effective_to_block
                  )
                  WHERE scoped.address = lower(emitting_address)
                    AND block_number BETWEEN scoped.effective_from_block
                        AND scoped.effective_to_block
              )
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            ORDER BY block_number, transaction_index, log_index, emitting_address
            "#,
        )
        .bind(chain)
        .bind(&watched_addresses)
        .bind(restrict_to_block_hashes)
        .bind(block_hashes)
        .bind(&watched_range_addresses)
        .bind(&watched_effective_from_blocks)
        .bind(&watched_effective_to_blocks)
        .bind(has_block_range)
        .bind(from_block)
        .bind(to_block)
        .bind(&scoped_addresses)
        .bind(&scoped_from_blocks)
        .bind(&scoped_to_blocks)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!("failed to load scoped ENSv1 registry raw logs for chain {chain}")
        })?
    } else {
        sqlx::query(
            r#"
            WITH watched_ranges AS MATERIALIZED (
                SELECT DISTINCT address, effective_from_block, effective_to_block
                FROM unnest($5::TEXT[], $6::BIGINT[], $7::BIGINT[]) AS watched(
                    address,
                    effective_from_block,
                    effective_to_block
                )
            )
            SELECT
                raw_log.chain_id,
                raw_log.block_hash,
                raw_log.block_number,
                raw_log.transaction_hash,
                raw_log.transaction_index,
                raw_log.log_index,
                raw_log.emitting_address,
                raw_log.topics,
                raw_log.data,
                raw_log.canonicality_state
            FROM watched_ranges watched
            CROSS JOIN LATERAL (
                SELECT
                    chain_id,
                    block_hash,
                    block_number,
                    transaction_hash,
                    transaction_index,
                    log_index,
                    emitting_address,
                    topics,
                    data,
                    canonicality_state::TEXT AS canonicality_state
                FROM raw_logs
                WHERE chain_id = $1
                  AND $2::TEXT[] IS NOT NULL
                  AND lower(emitting_address) = watched.address
                  AND ($3::BOOLEAN = FALSE OR block_hash = ANY($4::TEXT[]))
                  AND ($8::BOOLEAN = FALSE OR block_number BETWEEN $9::BIGINT AND $10::BIGINT)
                  AND block_number BETWEEN watched.effective_from_block
                      AND watched.effective_to_block
                  AND canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                OFFSET 0
            ) raw_log
            ORDER BY raw_log.block_number, raw_log.transaction_index, raw_log.log_index, raw_log.emitting_address
            "#,
        )
        .bind(chain)
        .bind(&watched_addresses)
        .bind(restrict_to_block_hashes)
        .bind(block_hashes)
        .bind(&watched_range_addresses)
        .bind(&watched_effective_from_blocks)
        .bind(&watched_effective_to_blocks)
        .bind(has_block_range)
        .bind(from_block)
        .bind(to_block)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load ENSv1 registry raw logs for chain {chain}"))?
    };

    rows.into_iter()
        .map(|row| {
            let emitting_address = normalize_address(
                &sql_row::get::<String>(&row, "emitting_address")?,
            );
            let block_number = sql_row::get(&row, "block_number")?;
            let emitter = emitters_by_address
                .get(&emitting_address)
                .and_then(|emitters| {
                    emitter_for_block_and_scope(emitters, block_number, source_scope)
                })
                .with_context(|| {
                    format!(
                        "missing active emitter attribution for chain {chain} address {emitting_address}"
                    )
                })?;
            registry_raw_log_from_row(row, emitting_address, block_number, emitter)
        })
        .collect()
}

fn registry_source_scope_block_range(
    source_scope: &[RegistryRawLogSourceScopeTarget],
) -> Option<(i64, i64)> {
    let from_block = source_scope
        .iter()
        .map(|target| target.effective_from_block)
        .min()?;
    let to_block = source_scope
        .iter()
        .map(|target| target.effective_to_block)
        .max()?;
    Some((from_block, to_block))
}

fn registry_raw_log_from_row(
    row: sqlx::postgres::PgRow,
    emitting_address: String,
    block_number: i64,
    emitter: &ActiveEmitter,
) -> Result<RegistryRawLogRow> {
    Ok(RegistryRawLogRow {
        chain_id: sql_row::get(&row, "chain_id")?,
        block_hash: sql_row::get(&row, "block_hash")?,
        block_number,
        transaction_hash: sql_row::get(&row, "transaction_hash")?,
        transaction_index: sql_row::get(&row, "transaction_index")?,
        log_index: sql_row::get(&row, "log_index")?,
        emitting_address,
        topics: sql_row::get(&row, "topics")?,
        data: sql_row::get(&row, "data")?,
        canonicality_state: sql_row::get(&row, "canonicality_state")?,
        emitting_contract_instance_id: emitter.contract_instance_id,
        source_manifest_id: emitter.source_manifest_id,
        namespace: emitter.namespace.clone(),
        source_family: emitter.source_family.clone(),
        manifest_version: emitter.manifest_version,
        contract_role: emitter.contract_role.clone(),
    })
}
