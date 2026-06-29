use std::collections::BTreeMap;

use sqlx::{PgPool, Row};
use tracing::error;

use crate::v2::{AsOf, V2Error, V2Result, format_timestamp, slug_to_numeric};

pub(super) async fn load_served_head_meta(pool: &PgPool) -> V2Result<BTreeMap<String, AsOf>> {
    let status = bigname_storage::load_indexing_status(pool)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                error = ?load_error,
                "failed to load v2 lookup indexing status"
            );
            V2Error::internal_error("failed to load lookup served head")
        })?;
    let hashes = load_canonical_hashes(pool).await?;
    let mut as_of = BTreeMap::new();

    for row in status.chains {
        let Some(block_number) = row.canonical_block else {
            continue;
        };
        let Some(block_hash) = hashes.get(&row.chain_id).cloned().flatten() else {
            return Err(V2Error::internal_error(format!(
                "indexing status row for {} is missing a head block hash",
                row.chain_id
            )));
        };
        let Some(timestamp) = row.canonical_timestamp else {
            continue;
        };
        let chain_id = slug_to_numeric(&row.chain_id).ok_or_else(|| {
            V2Error::internal_error(format!(
                "indexing status row uses unmapped chain_id {}",
                row.chain_id
            ))
        })?;
        let block_number = u64::try_from(block_number).map_err(|_| {
            V2Error::internal_error(format!(
                "indexing status row for {} has a negative head block",
                row.chain_id
            ))
        })?;

        as_of.insert(
            chain_id.to_string(),
            AsOf {
                block_number,
                block_hash,
                timestamp: format_timestamp(timestamp),
            },
        );
    }

    Ok(as_of)
}

pub(super) fn served_head_token(as_of: &BTreeMap<String, AsOf>) -> V2Result<String> {
    serde_json::to_string(as_of)
        .map_err(|_| V2Error::internal_error("failed to encode lookup served head"))
}

async fn load_canonical_hashes(pool: &PgPool) -> V2Result<BTreeMap<String, Option<String>>> {
    let rows = sqlx::query(
        r#"
        SELECT chain_id, canonical_block_hash
        FROM chain_checkpoints
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            error = ?load_error,
            "failed to load v2 lookup head hashes"
        );
        V2Error::internal_error("failed to load lookup served head")
    })?;

    let mut hashes = BTreeMap::new();
    for row in rows {
        hashes.insert(
            row.try_get::<String, _>("chain_id")
                .map_err(|_| V2Error::internal_error("failed to decode lookup served head"))?,
            row.try_get::<Option<String>, _>("canonical_block_hash")
                .map_err(|_| V2Error::internal_error("failed to decode lookup served head"))?,
        );
    }
    Ok(hashes)
}
