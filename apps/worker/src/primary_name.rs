#[path = "primary_name/hydration.rs"]
mod hydration;
#[path = "primary_name/projection.rs"]
mod projection;
#[path = "primary_name/query.rs"]
mod query;
#[path = "primary_name/types.rs"]
mod types;

use anyhow::Result;
use bigname_execution::ChainRpcUrls;
use sqlx::PgPool;
use tracing::info;

pub use hydration::{
    PrimaryNameLegacyReverseHydrationConfig, PrimaryNameLegacyReverseHydrationTrigger,
};
pub use projection::rebuild_primary_names_current;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PrimaryNamesCurrentRebuildSummary {
    pub requested_tuple_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
    pub success_row_count: usize,
    pub not_found_row_count: usize,
    pub invalid_name_row_count: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PrimaryNameLegacyReverseHydrationSummary {
    pub candidate_tuple_count: usize,
    pub queried_tuple_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
    pub success_row_count: usize,
    pub not_found_row_count: usize,
    pub invalid_name_row_count: usize,
    pub failed_lookup_count: usize,
}

pub async fn hydrate_legacy_reverse_resolver_primary_names(
    pool: &PgPool,
    config: PrimaryNameLegacyReverseHydrationConfig,
) -> Result<PrimaryNameLegacyReverseHydrationSummary> {
    hydration::hydrate_legacy_reverse_resolver_primary_names(pool, config).await
}

pub async fn load_legacy_reverse_resolver_call_triggers(
    pool: &PgPool,
    config: &PrimaryNameLegacyReverseHydrationConfig,
) -> Result<Vec<PrimaryNameLegacyReverseHydrationTrigger>> {
    hydration::load_legacy_reverse_resolver_call_triggers(pool, config).await
}

impl PrimaryNameLegacyReverseHydrationConfig {
    pub fn from_chain_rpc_url_entries(
        chain_rpc_url_entries: &[String],
        multicall3_address: String,
        batch_size: usize,
        extra_resolver_addresses: &[String],
    ) -> Result<Option<Self>> {
        let chain_rpc_urls = ChainRpcUrls::from_entries(chain_rpc_url_entries)?;
        if chain_rpc_urls.is_empty() {
            return Ok(None);
        }

        let mut config = Self::new(chain_rpc_urls);
        config.multicall3_address = multicall3_address;
        config.batch_size = batch_size.max(1);
        config
            .resolver_addresses
            .extend(extra_resolver_addresses.iter().cloned());
        Ok(Some(config))
    }
}

pub(crate) fn log_legacy_reverse_hydration_summary(
    summary: &PrimaryNameLegacyReverseHydrationSummary,
) {
    info!(
        service = "worker",
        projection = "primary_names_current",
        candidate_tuple_count = summary.candidate_tuple_count,
        queried_tuple_count = summary.queried_tuple_count,
        upserted_row_count = summary.upserted_row_count,
        deleted_row_count = summary.deleted_row_count,
        success_row_count = summary.success_row_count,
        not_found_row_count = summary.not_found_row_count,
        invalid_name_row_count = summary.invalid_name_row_count,
        failed_lookup_count = summary.failed_lookup_count,
        "primary_names_current legacy reverse-resolver hydration completed"
    );
}

#[cfg(test)]
#[path = "primary_name/tests/mod.rs"]
mod tests;
