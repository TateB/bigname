use super::super::*;
use anyhow::{Context, Result};
use bigname_storage::sql_row;
use sqlx::{PgPool, postgres::PgRow, types::time::OffsetDateTime};

pub(in crate::ens_v1_unwrapped_authority) async fn load_canonical_blocks(
    pool: &PgPool,
    chain: &str,
    target_block_number: Option<i64>,
) -> Result<Vec<RawBlockSnapshot>> {
    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state::TEXT AS canonicality_state
        FROM chain_lineage
        WHERE chain_id = $1
          AND ($2::BIGINT IS NULL OR block_number <= $2::BIGINT)
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY block_number
        "#,
    )
    .bind(chain)
    .bind(target_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load canonical raw blocks for chain {chain}"))?;

    rows.into_iter()
        .map(|row| {
            Ok(RawBlockSnapshot {
                chain_id: sql_row::get(&row, "chain_id")?,
                block_hash: sql_row::get(&row, "block_hash")?,
                block_number: sql_row::get(&row, "block_number")?,
                block_timestamp: sql_row::get(&row, "block_timestamp")?,
                canonicality_state: sql_row::get(&row, "canonicality_state")?,
            })
        })
        .collect()
}

pub(in crate::ens_v1_unwrapped_authority) async fn load_canonical_blocks_for_restricted_authority_sync(
    pool: &PgPool,
    chain: &str,
    raw_logs: &[AuthorityRawLogRow],
    event_topics: &AuthorityEventTopics,
) -> Result<Vec<RawBlockSnapshot>> {
    let Some(replay_head) = restricted_replay_head_block(raw_logs) else {
        return Ok(Vec::new());
    };
    let mut blocks = load_release_boundary_blocks_for_authority_logs(
        pool,
        chain,
        raw_logs,
        &replay_head,
        event_topics,
    )
    .await?;
    blocks.push(replay_head);

    blocks.sort_by(|left, right| {
        left.block_number
            .cmp(&right.block_number)
            .then(left.block_hash.cmp(&right.block_hash))
    });
    blocks.dedup_by(|left, right| {
        left.chain_id == right.chain_id
            && left.block_hash == right.block_hash
            && left.block_number == right.block_number
    });
    Ok(blocks)
}

fn restricted_replay_head_block(raw_logs: &[AuthorityRawLogRow]) -> Option<RawBlockSnapshot> {
    raw_logs
        .iter()
        .max_by(|left, right| {
            left.block_number
                .cmp(&right.block_number)
                .then(left.transaction_index.cmp(&right.transaction_index))
                .then(left.log_index.cmp(&right.log_index))
        })
        .map(|raw_log| RawBlockSnapshot {
            chain_id: raw_log.chain_id.clone(),
            block_hash: raw_log.block_hash.clone(),
            block_number: raw_log.block_number,
            block_timestamp: raw_log.block_timestamp,
            canonicality_state: raw_log.canonicality_state,
        })
}

async fn load_release_boundary_blocks_for_authority_logs(
    pool: &PgPool,
    chain: &str,
    raw_logs: &[AuthorityRawLogRow],
    replay_head: &RawBlockSnapshot,
    event_topics: &AuthorityEventTopics,
) -> Result<Vec<RawBlockSnapshot>> {
    let mut release_timestamps = Vec::new();
    let mut release_namespaces = Vec::new();
    for raw_log in raw_logs {
        let Some(release_timestamp) =
            release_boundary_timestamp_for_authority_log(raw_log, event_topics)?
        else {
            continue;
        };
        release_timestamps.push(release_timestamp);
        release_namespaces.push(raw_log.namespace.clone());
    }

    if release_timestamps.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (requested.release_timestamp, requested.namespace)
            rb.chain_id,
            rb.block_hash,
            rb.block_number,
            rb.block_timestamp,
            rb.canonicality_state::TEXT AS canonicality_state
        FROM unnest($2::TIMESTAMPTZ[], $3::TEXT[]) AS requested(
            release_timestamp,
            namespace
        )
        JOIN LATERAL (
            SELECT
                chain_id,
                block_hash,
                block_number,
                block_timestamp,
                canonicality_state
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_timestamp > requested.release_timestamp
              AND block_timestamp <= $4
              AND block_number <= $5
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            ORDER BY block_timestamp, block_number
            LIMIT 1
        ) rb ON TRUE
        ORDER BY requested.release_timestamp, requested.namespace, rb.block_timestamp, rb.block_number
        "#,
    )
    .bind(chain)
    .bind(&release_timestamps)
    .bind(&release_namespaces)
    .bind(replay_head.block_timestamp)
    .bind(replay_head.block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load ENSv1 unwrapped authority release boundary blocks for chain {chain}")
    })?;

    rows.into_iter().map(raw_block_snapshot_from_row).collect()
}

fn release_boundary_timestamp_for_authority_log(
    raw_log: &AuthorityRawLogRow,
    event_topics: &AuthorityEventTopics,
) -> Result<Option<OffsetDateTime>> {
    let Some(profile) = authority_profile_for_source_family(&raw_log.source_family) else {
        return Ok(None);
    };
    if raw_log.source_family != profile.registrar_source_family() {
        return Ok(None);
    }

    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    let expiry = if let Some(registration) =
        decode_registrar_name_registered_data(raw_log, topic0, event_topics)?
    {
        registration.expiry
    } else if let Some(renewal) = decode_registrar_name_renewed_data(raw_log, topic0, event_topics)?
    {
        renewal.expiry
    } else {
        return Ok(None);
    };

    let expiry = OffsetDateTime::from_unix_timestamp(expiry)
        .context("registrar authority log expiry is not a valid unix timestamp")?;
    release_after_grace(expiry).map(Some)
}

fn raw_block_snapshot_from_row(row: PgRow) -> Result<RawBlockSnapshot> {
    Ok(RawBlockSnapshot {
        chain_id: sql_row::get(&row, "chain_id")?,
        block_hash: sql_row::get(&row, "block_hash")?,
        block_number: sql_row::get(&row, "block_number")?,
        block_timestamp: sql_row::get(&row, "block_timestamp")?,
        canonicality_state: sql_row::get(&row, "canonicality_state")?,
    })
}
