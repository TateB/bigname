use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres};

use super::decode::decode_raw_payload_cache_metadata;
use super::normalization::{ensure_metadata_identity_matches, normalize_metadata_upsert};
use super::read::load_raw_payload_cache_metadata_internal;
use super::types::{RawPayloadCacheMetadata, RawPayloadCacheMetadataUpsert};

/// Insert missing metadata rows or refresh canonicality for already observed
/// payload identities. Immutable metadata must match the stored row.
pub async fn upsert_raw_payload_cache_metadata(
    pool: &PgPool,
    entries: &[RawPayloadCacheMetadataUpsert],
) -> Result<Vec<RawPayloadCacheMetadata>> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw payload cache metadata upsert")?;

    let mut snapshots = Vec::with_capacity(entries.len());
    for entry in entries {
        let entry = normalize_metadata_upsert(entry)?;
        snapshots.push(upsert_raw_payload_cache_metadata_entry(&mut transaction, &entry).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw payload cache metadata upsert")?;

    Ok(snapshots)
}

async fn upsert_raw_payload_cache_metadata_entry(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    entry: &RawPayloadCacheMetadataUpsert,
) -> Result<RawPayloadCacheMetadata> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO raw_payload_cache_metadata (
            chain_id,
            block_hash,
            payload_kind,
            digest_algorithm,
            retained_digest,
            block_number,
            payload_size_bytes,
            content_type,
            content_encoding,
            cache_metadata,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::canonicality_state)
        ON CONFLICT DO NOTHING
        RETURNING
            raw_payload_cache_metadata_id,
            chain_id,
            block_hash,
            payload_kind,
            digest_algorithm,
            retained_digest,
            block_number,
            payload_size_bytes,
            content_type,
            content_encoding,
            cache_metadata,
            canonicality_state::TEXT AS canonicality_state,
            first_observed_at,
            last_observed_at
        "#,
    )
    .bind(&entry.chain_id)
    .bind(&entry.block_hash)
    .bind(&entry.payload_kind)
    .bind(&entry.digest_algorithm)
    .bind(&entry.retained_digest)
    .bind(entry.block_number)
    .bind(entry.payload_size_bytes)
    .bind(&entry.content_type)
    .bind(&entry.content_encoding)
    .bind(&entry.cache_metadata)
    .bind(entry.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw payload cache metadata for chain {} block {} payload kind {}",
            entry.chain_id, entry.block_hash, entry.payload_kind
        )
    })? {
        return decode_raw_payload_cache_metadata(snapshot);
    }

    let existing = load_raw_payload_cache_metadata_internal(
        &mut **executor,
        &entry.chain_id,
        &entry.block_hash,
        &entry.payload_kind,
        entry.digest_algorithm.as_deref(),
        entry.retained_digest.as_deref(),
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw payload cache metadata for chain {} block {} payload kind {} after insert conflict",
            entry.chain_id, entry.block_hash, entry.payload_kind
        )
    })?;

    ensure_metadata_identity_matches(&existing, entry)?;
    let next_state = existing
        .canonicality_state
        .merge_observation(entry.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE raw_payload_cache_metadata
        SET
            canonicality_state = $6::canonicality_state,
            last_observed_at = now()
        WHERE chain_id = $1
          AND block_hash = $2
          AND payload_kind = $3
          AND digest_algorithm IS NOT DISTINCT FROM $4::TEXT
          AND retained_digest IS NOT DISTINCT FROM $5::TEXT
        RETURNING
            raw_payload_cache_metadata_id,
            chain_id,
            block_hash,
            payload_kind,
            digest_algorithm,
            retained_digest,
            block_number,
            payload_size_bytes,
            content_type,
            content_encoding,
            cache_metadata,
            canonicality_state::TEXT AS canonicality_state,
            first_observed_at,
            last_observed_at
        "#,
    )
    .bind(&entry.chain_id)
    .bind(&entry.block_hash)
    .bind(&entry.payload_kind)
    .bind(&entry.digest_algorithm)
    .bind(&entry.retained_digest)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh raw payload cache metadata for chain {} block {} payload kind {}",
            entry.chain_id, entry.block_hash, entry.payload_kind
        )
    })?;

    decode_raw_payload_cache_metadata(snapshot)
}
