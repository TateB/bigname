use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    boundary_key::record_version_boundary_storage_key,
    snapshot_reads::DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER,
};

/// Count known public record selectors for exact current inventory keys.
pub async fn count_record_inventory_selectors_by_lookup_keys(
    pool: &PgPool,
    keys: &[(Uuid, Value)],
) -> Result<Vec<Option<u64>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut resource_ids = Vec::with_capacity(keys.len());
    let mut boundary_keys = Vec::with_capacity(keys.len());
    for (resource_id, boundary) in keys {
        let boundary_key =
            record_version_boundary_storage_key(boundary, *resource_id).with_context(|| {
                format!(
                    "failed to derive record_inventory_current count key for resource_id {resource_id}"
                )
            })?;
        resource_ids.push(*resource_id);
        boundary_keys.push(boundary_key);
    }

    let rows = sqlx::query_as::<_, (Uuid, String, i64)>(&format!(
        r#"
        SELECT
            ric.resource_id,
            ric.record_version_boundary_key,
            JSONB_ARRAY_LENGTH(
                CASE
                    WHEN JSONB_TYPEOF(ric.selectors) = 'array' THEN ric.selectors
                    ELSE '[]'::JSONB
                END
            )::BIGINT AS record_count
        FROM record_inventory_current ric
        JOIN UNNEST($1::UUID[], $2::TEXT[])
          AS requested(resource_id, record_version_boundary_key)
          ON requested.resource_id = ric.resource_id
         AND requested.record_version_boundary_key = ric.record_version_boundary_key
        JOIN resources resource
          ON resource.resource_id = ric.resource_id
        WHERE TRUE
        {DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER}
        "#
    ))
    .bind(&resource_ids)
    .bind(&boundary_keys)
    .fetch_all(pool)
    .await
    .context("failed to count record_inventory_current selectors by lookup keys")?;

    let mut counts = BTreeMap::new();
    for (resource_id, boundary_key, count) in rows {
        let count = u64::try_from(count).context("negative record inventory selector count")?;
        counts.insert((resource_id, boundary_key), count);
    }

    Ok(resource_ids
        .into_iter()
        .zip(boundary_keys)
        .map(|key| counts.get(&key).copied())
        .collect())
}
