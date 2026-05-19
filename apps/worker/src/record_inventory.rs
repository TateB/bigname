mod chain_position;
mod constants;
mod hydration;
mod json;
mod loading;
mod profile;
mod projection;
mod types;

use anyhow::Result;
use bigname_execution::ChainRpcUrls;
use sqlx::PgPool;
use tracing::info;

pub use hydration::RecordInventoryTextHydrationConfig;
pub use types::{RecordInventoryCurrentRebuildSummary, RecordInventoryTextHydrationSummary};

pub async fn rebuild_record_inventory_current(
    pool: &PgPool,
    resource_id: Option<&str>,
) -> Result<RecordInventoryCurrentRebuildSummary> {
    projection::rebuild_record_inventory_current(pool, resource_id).await
}

pub async fn hydrate_record_inventory_text_values(
    pool: &PgPool,
    resource_id: Option<&str>,
    config: RecordInventoryTextHydrationConfig,
) -> Result<RecordInventoryTextHydrationSummary> {
    hydration::hydrate_record_inventory_text_values(pool, resource_id, config).await
}

impl RecordInventoryTextHydrationConfig {
    pub fn from_chain_rpc_url_entries(
        chain_rpc_url_entries: &[String],
        multicall3_address: String,
        batch_size: usize,
    ) -> Result<Option<Self>> {
        let chain_rpc_urls = ChainRpcUrls::from_entries(chain_rpc_url_entries)?;
        if chain_rpc_urls.is_empty() {
            return Ok(None);
        }

        let mut config = Self::new(chain_rpc_urls);
        config.multicall3_address = multicall3_address;
        config.batch_size = batch_size.max(1);
        Ok(Some(config))
    }
}

pub(crate) fn log_text_hydration_summary(
    resource_id: Option<&str>,
    summary: &RecordInventoryTextHydrationSummary,
) {
    info!(
        service = "worker",
        projection = "record_inventory_current",
        candidate_row_count = summary.candidate_row_count,
        candidate_entry_count = summary.candidate_entry_count,
        hydrated_entry_count = summary.hydrated_entry_count,
        not_found_entry_count = summary.not_found_entry_count,
        skipped_entry_count = summary.skipped_entry_count,
        failed_entry_count = summary.failed_entry_count,
        updated_row_count = summary.updated_row_count,
        resource_id = resource_id.unwrap_or("all"),
        "record_inventory_current text hydration completed"
    );
}

#[cfg(test)]
use anyhow::Context;
#[cfg(test)]
use bigname_storage::{
    CanonicalityState, RecordInventoryCurrentRow, upsert_record_inventory_current_rows,
};
#[cfg(test)]
use chain_position::format_timestamp;
#[cfg(test)]
use constants::*;
#[cfg(test)]
use serde_json::{Value, json};
#[cfg(test)]
use sqlx::{Row, types::time::OffsetDateTime};
#[cfg(test)]
use uuid::Uuid;

#[cfg(test)]
mod tests;
