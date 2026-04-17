//! Bootstrap adapter surface for the repo skeleton.

#[cfg(test)]
use std::sync::{Arc, OnceLock};

mod block_derived_normalized_events;
mod manifest_normalized_events;

/// Current adapter bootstrap status.
pub const fn bootstrap_status() -> &'static str {
    "manifest-normalized-sync-ready"
}

pub use block_derived_normalized_events::{
    BlockDerivedNormalizedEventKindSyncSummary, BlockDerivedNormalizedEventSyncSummary,
    sync_block_derived_normalized_events,
};
pub use manifest_normalized_events::{
    ManifestNormalizedEventKindSyncSummary, ManifestNormalizedEventSyncSummary,
    sync_manifest_normalized_events,
};

#[cfg(test)]
static TEST_DB_SEMAPHORE: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

#[cfg(test)]
pub(crate) async fn acquire_test_db_permit() -> tokio::sync::OwnedSemaphorePermit {
    TEST_DB_SEMAPHORE
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
        .clone()
        .acquire_owned()
        .await
        .expect("test DB semaphore must stay open")
}
