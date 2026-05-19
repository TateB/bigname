use anyhow::Result;
use sqlx::postgres::PgRow;

use super::types::RawPayloadCacheMetadata;

pub(super) fn decode_raw_payload_cache_metadata(row: PgRow) -> Result<RawPayloadCacheMetadata> {
    Ok(RawPayloadCacheMetadata {
        raw_payload_cache_metadata_id: crate::sql_row::get(&row, "raw_payload_cache_metadata_id")?,
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        payload_kind: crate::sql_row::get(&row, "payload_kind")?,
        digest_algorithm: crate::sql_row::get(&row, "digest_algorithm")?,
        retained_digest: crate::sql_row::get(&row, "retained_digest")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        payload_size_bytes: crate::sql_row::get(&row, "payload_size_bytes")?,
        content_type: crate::sql_row::get(&row, "content_type")?,
        content_encoding: crate::sql_row::get(&row, "content_encoding")?,
        cache_metadata: crate::sql_row::get(&row, "cache_metadata")?,
        canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
        first_observed_at: crate::sql_row::get(&row, "first_observed_at")?,
        last_observed_at: crate::sql_row::get(&row, "last_observed_at")?,
    })
}
