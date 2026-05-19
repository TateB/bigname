use bigname_storage::sql_row;
use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::PgPool;

use super::event_topics::PreimageObservedEventTopics;
use super::source_selection::{load_active_emitters, normalized_source_scope_targets};
use super::types::WatchedRawLogRow;

#[derive(Clone, Debug)]
pub(super) struct WatchedRawLogLoad {
    pub(super) raw_logs: Vec<WatchedRawLogRow>,
    pub(super) event_topics: PreimageObservedEventTopics,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RawLogCanonicalityFilter {
    IncludeObserved,
    CanonicalOnly,
}

impl RawLogCanonicalityFilter {
    const fn canonical_only(self) -> bool {
        matches!(self, Self::CanonicalOnly)
    }
}

pub(super) async fn load_scanned_log_count(
    pool: &PgPool,
    chain: &str,
    block_hashes: &[String],
    canonicality_filter: RawLogCanonicalityFilter,
) -> Result<usize> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND (
              (
                  $3::BOOLEAN
                  AND canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
              )
              OR (
                  NOT $3::BOOLEAN
                  AND canonicality_state <> 'orphaned'::canonicality_state
              )
          )
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .bind(canonicality_filter.canonical_only())
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to count stored raw logs for chain {chain} across {} blocks",
            block_hashes.len()
        )
    })?;

    usize::try_from(count).context("raw log count does not fit in usize")
}

pub(super) async fn load_watched_raw_logs(
    pool: &PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    canonicality_filter: RawLogCanonicalityFilter,
) -> Result<WatchedRawLogLoad> {
    let source_scope = source_scope.map(normalized_source_scope_targets);
    if source_scope.as_ref().is_some_and(Vec::is_empty) {
        return Ok(WatchedRawLogLoad {
            raw_logs: Vec::new(),
            event_topics: PreimageObservedEventTopics::default(),
        });
    }
    let scoped_emitter_identities = source_scope.as_ref().map(|source_scope| {
        source_scope
            .iter()
            .map(|target| (target.source_family.clone(), target.address.clone()))
            .collect::<HashSet<_>>()
    });

    let active_emitters =
        load_active_emitters(pool, chain, scoped_emitter_identities.as_ref()).await?;
    if active_emitters.is_empty() {
        return Ok(WatchedRawLogLoad {
            raw_logs: Vec::new(),
            event_topics: PreimageObservedEventTopics::default(),
        });
    }
    let event_topics = PreimageObservedEventTopics::load(pool, &active_emitters).await?;
    let preimage_topic0s = event_topics.query_topic0s();

    let emitters_by_address = active_emitters
        .into_iter()
        .map(|emitter| (emitter.address.clone(), emitter))
        .collect::<HashMap<_, _>>();
    let watched_addresses = emitters_by_address.keys().cloned().collect::<Vec<_>>();

    let rows = if let Some(source_scope) = &source_scope {
        let scoped_addresses = source_scope
            .iter()
            .map(|target| target.address.clone())
            .collect::<Vec<_>>();
        let scoped_from_blocks = source_scope
            .iter()
            .map(|target| target.effective_from_block)
            .collect::<Vec<_>>();
        let scoped_to_blocks = source_scope
            .iter()
            .map(|target| target.effective_to_block)
            .collect::<Vec<_>>();

        sqlx::query(
            r#"
            SELECT
                rl.chain_id AS chain_id,
                rl.block_hash AS block_hash,
                rl.block_number AS block_number,
                rl.transaction_hash AS transaction_hash,
                rl.transaction_index AS transaction_index,
                rl.log_index AS log_index,
                rl.emitting_address AS emitting_address,
                rl.topics AS topics,
                rl.data AS data,
                rl.canonicality_state::TEXT AS canonicality_state
            FROM raw_logs rl
            WHERE rl.chain_id = $1
              AND rl.block_hash = ANY($2::TEXT[])
              AND rl.emitting_address = ANY($3::TEXT[])
              AND rl.topics[1] = ANY($7::TEXT[])
              AND EXISTS (
                  SELECT 1
                  FROM unnest($4::TEXT[], $5::BIGINT[], $6::BIGINT[]) AS scoped(
                      address,
                      effective_from_block,
                      effective_to_block
                  )
                  WHERE scoped.address = rl.emitting_address
                    AND rl.block_number BETWEEN scoped.effective_from_block
                        AND scoped.effective_to_block
              )
              AND (
                  (
                      $8::BOOLEAN
                      AND rl.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                  )
                  OR (
                      NOT $8::BOOLEAN
                      AND rl.canonicality_state <> 'orphaned'::canonicality_state
                  )
              )
            ORDER BY
                rl.block_number,
                rl.transaction_index,
                rl.log_index
            "#,
        )
        .bind(chain)
        .bind(block_hashes)
        .bind(&watched_addresses)
        .bind(&scoped_addresses)
        .bind(&scoped_from_blocks)
        .bind(&scoped_to_blocks)
        .bind(&preimage_topic0s)
        .bind(canonicality_filter.canonical_only())
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load scoped watched raw logs for chain {chain} across {} blocks",
                block_hashes.len()
            )
        })?
    } else {
        sqlx::query(
            r#"
            SELECT
                rl.chain_id AS chain_id,
                rl.block_hash AS block_hash,
                rl.block_number AS block_number,
                rl.transaction_hash AS transaction_hash,
                rl.transaction_index AS transaction_index,
                rl.log_index AS log_index,
                rl.emitting_address AS emitting_address,
                rl.topics AS topics,
                rl.data AS data,
                rl.canonicality_state::TEXT AS canonicality_state
            FROM raw_logs rl
            WHERE rl.chain_id = $1
              AND rl.block_hash = ANY($2::TEXT[])
              AND rl.emitting_address = ANY($3::TEXT[])
              AND rl.topics[1] = ANY($4::TEXT[])
              AND (
                  (
                      $5::BOOLEAN
                      AND rl.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                  )
                  OR (
                      NOT $5::BOOLEAN
                      AND rl.canonicality_state <> 'orphaned'::canonicality_state
                  )
              )
            ORDER BY
                rl.block_number,
                rl.transaction_index,
                rl.log_index
            "#,
        )
        .bind(chain)
        .bind(block_hashes)
        .bind(&watched_addresses)
        .bind(&preimage_topic0s)
        .bind(canonicality_filter.canonical_only())
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load watched raw logs for chain {chain} across {} blocks",
                block_hashes.len()
            )
        })?
    };

    let raw_logs = rows
        .into_iter()
        .map(|row| {
            let emitting_address = sql_row::get::<String>(&row, "emitting_address")?;
            let normalized_emitting_address = emitting_address.to_ascii_lowercase();
            let active_emitter = emitters_by_address
                .get(&normalized_emitting_address)
                .with_context(|| {
                    format!(
                        "missing active emitter attribution for chain {} address {}",
                        chain, emitting_address
                    )
                })?;

            Ok(WatchedRawLogRow {
                chain_id: sql_row::get(&row, "chain_id")?,
                block_hash: sql_row::get(&row, "block_hash")?,
                block_number: sql_row::get(&row, "block_number")?,
                transaction_hash: sql_row::get(&row, "transaction_hash")?,
                transaction_index: sql_row::get(&row, "transaction_index")?,
                log_index: sql_row::get(&row, "log_index")?,
                emitting_address,
                topics: sql_row::get(&row, "topics")?,
                data: sql_row::get(&row, "data")?,
                canonicality_state: sql_row::get(&row, "canonicality_state")?,
                source_manifest_id: active_emitter.source_manifest_id,
                namespace: active_emitter.namespace.clone(),
                source_family: active_emitter.source_family.clone(),
                manifest_version: active_emitter.manifest_version,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(WatchedRawLogLoad {
        raw_logs,
        event_topics,
    })
}
