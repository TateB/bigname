use super::super::*;
use anyhow::{Context, Result};
use sqlx::{PgPool, Row, postgres::PgRow, types::time::OffsetDateTime};

pub(in crate::ens_v1_unwrapped_authority) async fn load_canonical_blocks(
    pool: &PgPool,
    chain: &str,
) -> Result<Vec<RawBlockSnapshot>> {
    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_blocks
        WHERE chain_id = $1
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY block_number
        "#,
    )
    .bind(chain)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load canonical raw blocks for chain {chain}"))?;

    rows.into_iter()
        .map(|row| {
            Ok(RawBlockSnapshot {
                chain_id: row.try_get("chain_id").context("missing chain_id")?,
                block_hash: row.try_get("block_hash").context("missing block_hash")?,
                block_number: row
                    .try_get("block_number")
                    .context("missing block_number")?,
                block_timestamp: row
                    .try_get("block_timestamp")
                    .context("missing block_timestamp")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")
                        .context("missing canonicality_state")?,
                )?,
            })
        })
        .collect()
}

pub(in crate::ens_v1_unwrapped_authority) async fn load_canonical_blocks_for_restricted_authority_sync(
    pool: &PgPool,
    chain: &str,
    raw_logs: &[AuthorityRawLogRow],
) -> Result<Vec<RawBlockSnapshot>> {
    let mut blocks = load_release_boundary_blocks_for_authority_logs(pool, chain, raw_logs).await?;
    if let Some(head) = load_canonical_head_block(pool, chain).await? {
        blocks.push(head);
    }

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

async fn load_canonical_head_block(pool: &PgPool, chain: &str) -> Result<Option<RawBlockSnapshot>> {
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_blocks
        WHERE chain_id = $1
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY block_number DESC
        LIMIT 1
        "#,
    )
    .bind(chain)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load canonical raw-block head for chain {chain}"))?;

    row.map(raw_block_snapshot_from_row).transpose()
}

async fn load_release_boundary_blocks_for_authority_logs(
    pool: &PgPool,
    chain: &str,
    raw_logs: &[AuthorityRawLogRow],
) -> Result<Vec<RawBlockSnapshot>> {
    let mut release_timestamps = Vec::new();
    let mut release_namespaces = Vec::new();
    for raw_log in raw_logs {
        let Some(release_timestamp) = release_boundary_timestamp_for_authority_log(raw_log)? else {
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
            FROM raw_blocks
            WHERE chain_id = $1
              AND block_timestamp >= requested.release_timestamp
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
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load ENSv1 unwrapped authority release boundary blocks for chain {chain}")
    })?;

    rows.into_iter().map(raw_block_snapshot_from_row).collect()
}

fn release_boundary_timestamp_for_authority_log(
    raw_log: &AuthorityRawLogRow,
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
    let expiry_word_start = registrar_name_registered_expiry_word_start(topic0)
        .or_else(|| registrar_name_renewed_expiry_word_start(topic0));
    let Some(expiry_word_start) = expiry_word_start else {
        return Ok(None);
    };

    let expiry = abi_word_to_i64(
        raw_log
            .data
            .get(expiry_word_start..expiry_word_start + 32)
            .context("registrar authority log is missing expiry word")?,
    )?;
    let expiry = OffsetDateTime::from_unix_timestamp(expiry)
        .context("registrar authority log expiry is not a valid unix timestamp")?;
    release_after_grace(expiry).map(Some)
}

fn raw_block_snapshot_from_row(row: PgRow) -> Result<RawBlockSnapshot> {
    Ok(RawBlockSnapshot {
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp")?,
        canonicality_state: parse_canonicality_state(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}
