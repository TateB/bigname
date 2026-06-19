use std::collections::HashMap;
use std::sync::Arc;

use async_graphql::dataloader::Loader;
use bigname_storage::{RecordInventoryCurrentRow, load_record_inventory_current_batch};
use serde_json::Value;
use sqlx::PgPool;
use sqlx::types::Uuid;

/// DataLoader key for a name's `record_inventory_current` row. The version boundary is a
/// `serde_json::Value` (not `Hash`/`Eq`), so it is carried as its canonical JSON serialization and
/// parsed back in [`RecordInventoryLoader::load`]; `serde_json` serializes a given `Value`
/// deterministically, so equal boundaries map to the same key (and the loader dedups them).
pub(super) type RecordInventoryKey = (Uuid, String);

/// Serialize a version boundary into the string half of a [`RecordInventoryKey`].
pub(super) fn record_inventory_key(resource_id: Uuid, boundary: &Value) -> RecordInventoryKey {
    (resource_id, boundary.to_string())
}

/// Batches the per-domain `record_inventory_current` reads behind `Domain.resolver` into one
/// storage round-trip, collapsing the list-page N+1. Caching is disabled at registration so the
/// loader only ever batches within a request window and never serves a stale row across requests.
pub(super) struct RecordInventoryLoader {
    pool: PgPool,
}

impl RecordInventoryLoader {
    pub(super) fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl Loader<RecordInventoryKey> for RecordInventoryLoader {
    type Value = RecordInventoryCurrentRow;
    // anyhow::Error is not Clone; async-graphql requires a Clone error, so share it behind an Arc.
    type Error = Arc<anyhow::Error>;

    async fn load(
        &self,
        keys: &[RecordInventoryKey],
    ) -> Result<HashMap<RecordInventoryKey, Self::Value>, Self::Error> {
        let pairs = keys
            .iter()
            .map(|(resource_id, boundary)| {
                let boundary: Value = serde_json::from_str(boundary).map_err(|error| {
                    Arc::new(anyhow::Error::new(error).context(
                        "failed to parse record inventory DataLoader boundary key as JSON",
                    ))
                })?;
                Ok((*resource_id, boundary))
            })
            .collect::<Result<Vec<_>, Self::Error>>()?;

        let rows = load_record_inventory_current_batch(&self.pool, &pairs)
            .await
            .map_err(Arc::new)?;

        Ok(keys
            .iter()
            .cloned()
            .zip(rows)
            .filter_map(|(key, row)| row.map(|row| (key, row)))
            .collect())
    }
}
