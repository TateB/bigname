use super::super::scope::{
    AuthorityRawLogSourceScopeTarget, emitter_for_block_and_scope,
    scoped_ranges_for_active_emitters,
};
use super::super::*;
use anyhow::{Context, Result};
use bigname_storage::sql_row;
use futures_util::TryStreamExt;
use sqlx::{PgConnection, PgPool};
use std::collections::HashMap;

mod generic_resolver;
mod row_helpers;
mod stream_page_bound;
mod stream_router;

use generic_resolver::load_generic_resolver_event_raw_logs;
use row_helpers::{authority_raw_log_from_row, authority_source_scope_block_range};
pub(in crate::ens_v1_unwrapped_authority) use stream_page_bound::select_authority_raw_log_stream_to_block;
pub(in crate::ens_v1_unwrapped_authority) use stream_router::AuthorityRawLogStreamSourceRouter;

pub(in crate::ens_v1_unwrapped_authority) async fn load_authority_raw_logs(
    pool: &PgPool,
    chain: &str,
    active_emitters: &[ActiveEmitter],
    generic_resolver_event_sources: &[GenericResolverEventSource],
    event_topics: &AuthorityEventTopics,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[AuthorityRawLogSourceScopeTarget]>,
) -> Result<Vec<AuthorityRawLogRow>> {
    let block_range = source_scope.and_then(authority_source_scope_block_range);
    load_authority_raw_logs_internal(
        pool,
        chain,
        active_emitters,
        generic_resolver_event_sources,
        event_topics,
        restrict_to_block_hashes,
        block_hashes,
        source_scope,
        block_range,
    )
    .await
}

pub(in crate::ens_v1_unwrapped_authority) async fn stream_authority_raw_logs(
    conn: &mut PgConnection,
    chain: &str,
    source_router: &AuthorityRawLogStreamSourceRouter<'_>,
    _event_topics: &AuthorityEventTopics,
    from_block: i64,
    to_block: i64,
    mut handle_raw_log: impl FnMut(AuthorityRawLogRow) -> Result<()>,
) -> Result<usize> {
    if from_block > to_block {
        return Ok(0);
    }

    let mut rows = sqlx::query(
        r#"
        SELECT
            rl.chain_id AS chain_id,
            rl.block_hash AS block_hash,
            rl.block_number AS block_number,
            rb.block_timestamp AS block_timestamp,
            rl.transaction_hash AS transaction_hash,
            rl.transaction_index AS transaction_index,
            rl.log_index AS log_index,
            LOWER(rl.emitting_address) AS emitting_address,
            LOWER(rl.topics[1]) AS topic0,
            rl.topics AS topics,
            rl.data AS data,
            rl.canonicality_state::TEXT AS canonicality_state
        FROM raw_logs rl
        JOIN chain_lineage rb
          ON rb.chain_id = rl.chain_id
         AND rb.block_hash = rl.block_hash
        WHERE rl.chain_id = $1
          AND rl.block_number BETWEEN $2::BIGINT AND $3::BIGINT
          AND rl.topics[1] IS NOT NULL
          AND rl.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY
            rl.chain_id,
            rl.block_number,
            rl.block_hash,
            rl.transaction_index,
            rl.log_index,
            rl.emitting_address
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .fetch(&mut *conn);

    let mut scanned_log_count = 0usize;
    while let Some(row) = rows.try_next().await.with_context(|| {
        format!(
            "failed to stream ENSv1 unwrapped authority raw logs for chain {chain} blocks {from_block}..={to_block}"
        )
    })? {
        let chain_id: String = sql_row::get(&row, "chain_id")?;
        let block_hash: String = sql_row::get(&row, "block_hash")?;
        let block_number: i64 = sql_row::get(&row, "block_number")?;
        let block_timestamp: OffsetDateTime = sql_row::get(&row, "block_timestamp")?;
        let transaction_hash: String = sql_row::get(&row, "transaction_hash")?;
        let transaction_index: i64 = sql_row::get(&row, "transaction_index")?;
        let log_index: i64 = sql_row::get(&row, "log_index")?;
        let emitting_address: String = sql_row::get(&row, "emitting_address")?;
        let topic0: String = sql_row::get(&row, "topic0")?;
        let topics: Vec<String> = sql_row::get(&row, "topics")?;
        let data: Vec<u8> = sql_row::get(&row, "data")?;
        let canonicality_state: CanonicalityState = sql_row::get(&row, "canonicality_state")?;
        for source in source_router.source_candidates(&emitting_address, &topic0, block_number) {
            let raw_log = AuthorityRawLogRow {
                chain_id: chain_id.clone(),
                block_hash: block_hash.clone(),
                block_number,
                block_timestamp,
                transaction_hash: transaction_hash.clone(),
                transaction_index,
                log_index,
                emitting_address: emitting_address.clone(),
                topics: topics.clone(),
                data: data.clone(),
                canonicality_state,
                source_manifest_id: source.source_manifest_id(),
                namespace: source.namespace().to_owned(),
                source_family: source.source_family().to_owned(),
                manifest_version: source.manifest_version(),
                normalizer_version: source.normalizer_version().to_owned(),
                contract_role: source.contract_role().map(str::to_owned),
            };
            handle_raw_log(raw_log)?;
            scanned_log_count += 1;
        }
    }
    Ok(scanned_log_count)
}

async fn load_authority_raw_logs_internal(
    pool: &PgPool,
    chain: &str,
    active_emitters: &[ActiveEmitter],
    generic_resolver_event_sources: &[GenericResolverEventSource],
    event_topics: &AuthorityEventTopics,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[AuthorityRawLogSourceScopeTarget]>,
    block_range: Option<(i64, i64)>,
) -> Result<Vec<AuthorityRawLogRow>> {
    let mut emitters_by_address = HashMap::<String, Vec<ActiveEmitter>>::new();
    for emitter in active_emitters.iter().cloned() {
        emitters_by_address
            .entry(emitter.address.clone())
            .or_default()
            .push(emitter);
    }
    let watched_addresses = emitters_by_address.keys().cloned().collect::<Vec<_>>();
    let watched_range_addresses = active_emitters
        .iter()
        .map(|emitter| emitter.address.clone())
        .collect::<Vec<_>>();
    let watched_effective_from_blocks = active_emitters
        .iter()
        .map(|emitter| emitter.active_from_block_number.unwrap_or(0))
        .collect::<Vec<_>>();
    let watched_effective_to_blocks = active_emitters
        .iter()
        .map(|emitter| emitter.active_to_block_number.unwrap_or(i64::MAX))
        .collect::<Vec<_>>();

    let scoped_ranges = source_scope
        .map(|source_scope| scoped_ranges_for_active_emitters(source_scope, active_emitters))
        .transpose()?;
    let (has_block_range, from_block, to_block) = block_range
        .map(|(from_block, to_block)| (true, from_block, to_block))
        .unwrap_or((false, 0, 0));
    let mut raw_logs = Vec::new();

    if !active_emitters.is_empty() {
        let rows = if let Some(scoped_ranges) = scoped_ranges.as_ref() {
            if scoped_ranges.is_empty() {
                Vec::new()
            } else {
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
                    rl.chain_id AS chain_id,
                    rl.block_hash AS block_hash,
                    rl.block_number AS block_number,
                    rb.block_timestamp AS block_timestamp,
                    rl.transaction_hash AS transaction_hash,
                    rl.transaction_index AS transaction_index,
                    rl.log_index AS log_index,
                    rl.emitting_address AS emitting_address,
                    rl.topics AS topics,
                    rl.data AS data,
                    rl.canonicality_state::TEXT AS canonicality_state
                FROM raw_logs rl
                JOIN chain_lineage rb
                  ON rb.chain_id = rl.chain_id
                 AND rb.block_hash = rl.block_hash
                WHERE rl.chain_id = $1
                  AND lower(rl.emitting_address) = ANY($2::TEXT[])
                  AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
                  AND ($8::BOOLEAN = FALSE OR rl.block_number BETWEEN $9::BIGINT AND $10::BIGINT)
                  AND EXISTS (
                      SELECT 1
                      FROM unnest($5::TEXT[], $6::BIGINT[], $7::BIGINT[]) AS watched(
                          address,
                          effective_from_block,
                          effective_to_block
                      )
                      WHERE watched.address = lower(rl.emitting_address)
                        AND rl.block_number BETWEEN watched.effective_from_block
                            AND watched.effective_to_block
                  )
                  AND EXISTS (
                      SELECT 1
                      FROM unnest($11::TEXT[], $12::BIGINT[], $13::BIGINT[]) AS scoped(
                          address,
                          effective_from_block,
                          effective_to_block
                      )
                      WHERE scoped.address = lower(rl.emitting_address)
                        AND rl.block_number BETWEEN scoped.effective_from_block
                            AND scoped.effective_to_block
                  )
                  AND rl.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                ORDER BY rl.block_number, rl.transaction_index, rl.log_index
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
                    format!(
                        "failed to load scoped ENSv1 unwrapped authority raw logs for chain {chain}"
                    )
                })?
            }
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
                    raw_log.chain_id AS chain_id,
                    raw_log.block_hash AS block_hash,
                    raw_log.block_number AS block_number,
                    raw_log.block_timestamp AS block_timestamp,
                    raw_log.transaction_hash AS transaction_hash,
                    raw_log.transaction_index AS transaction_index,
                    raw_log.log_index AS log_index,
                    raw_log.emitting_address AS emitting_address,
                    raw_log.topics AS topics,
                    raw_log.data AS data,
                    raw_log.canonicality_state AS canonicality_state
                FROM watched_ranges watched
                CROSS JOIN LATERAL (
                    SELECT
                        rl.chain_id AS chain_id,
                        rl.block_hash AS block_hash,
                        rl.block_number AS block_number,
                        rb.block_timestamp AS block_timestamp,
                        rl.transaction_hash AS transaction_hash,
                        rl.transaction_index AS transaction_index,
                        rl.log_index AS log_index,
                        rl.emitting_address AS emitting_address,
                        rl.topics AS topics,
                        rl.data AS data,
                        rl.canonicality_state::TEXT AS canonicality_state
                    FROM raw_logs rl
                    JOIN chain_lineage rb
                      ON rb.chain_id = rl.chain_id
                     AND rb.block_hash = rl.block_hash
                    WHERE rl.chain_id = $1
                      AND $2::TEXT[] IS NOT NULL
                      AND lower(rl.emitting_address) = watched.address
                      AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
                      AND ($8::BOOLEAN = FALSE OR rl.block_number BETWEEN $9::BIGINT AND $10::BIGINT)
                      AND rl.block_number BETWEEN watched.effective_from_block
                          AND watched.effective_to_block
                      AND rl.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                    OFFSET 0
                ) raw_log
                ORDER BY raw_log.block_number, raw_log.transaction_index, raw_log.log_index
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
            .with_context(|| {
                format!("failed to load ENSv1 unwrapped authority raw logs for chain {chain}")
            })?
        };

        raw_logs.extend(
            rows.into_iter()
                .map(|row| {
                    let address =
                        sql_row::get::<String>(&row, "emitting_address")?.to_ascii_lowercase();
                    let block_number = sql_row::get(&row, "block_number")?;
                    let emitter = emitters_by_address
                .get(&address)
                .and_then(|emitters| {
                    emitter_for_block_and_scope(emitters, block_number, source_scope)
                })
                .with_context(|| {
                    format!("missing active emitter metadata for chain {chain} address {address}")
                })?;
                    authority_raw_log_from_row(row, address, block_number, emitter)
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }

    raw_logs.extend(
        load_generic_resolver_event_raw_logs(
            pool,
            chain,
            generic_resolver_event_sources,
            event_topics,
            restrict_to_block_hashes,
            block_hashes,
            block_range,
        )
        .await?,
    );
    raw_logs.sort_by(|left, right| {
        left.block_number
            .cmp(&right.block_number)
            .then(left.transaction_index.cmp(&right.transaction_index))
            .then(left.log_index.cmp(&right.log_index))
            .then(left.emitting_address.cmp(&right.emitting_address))
            .then(left.source_family.cmp(&right.source_family))
    });
    raw_logs.dedup_by(|left, right| {
        left.chain_id == right.chain_id
            && left.block_hash == right.block_hash
            && left.transaction_hash == right.transaction_hash
            && left.log_index == right.log_index
            && left.source_family == right.source_family
    });
    Ok(raw_logs)
}
