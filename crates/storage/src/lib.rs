//! Shared PostgreSQL bootstrap utilities.

mod checkpoints;
mod lineage;
mod normalized_events;
mod raw;
mod raw_children;
mod raw_code;

use anyhow::{Context, Result};
use clap::Args;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tracing::info;

pub use checkpoints::{
    ChainCheckpoint, ChainCheckpointUpdate, CheckpointBlockRef, advance_chain_checkpoints,
    sync_chain_checkpoints,
};
pub use lineage::{
    CanonicalityState, ChainLineageBlock, load_chain_lineage_block,
    mark_chain_lineage_range_orphaned, upsert_chain_lineage_blocks,
};
pub use normalized_events::{
    NormalizedEvent, load_normalized_event_counts_by_kind, load_normalized_events_by_namespace,
    mark_block_derived_normalized_events_range_orphaned, upsert_normalized_events,
};
pub use raw::{
    RawBlock, load_raw_block, load_raw_blocks_by_hashes, mark_raw_block_range_orphaned,
    upsert_raw_blocks,
};
pub use raw_children::{
    RawFactOrphanCounts, RawLog, RawReceipt, RawTransaction, mark_raw_block_facts_range_orphaned,
    upsert_raw_logs, upsert_raw_receipts, upsert_raw_transactions,
};
pub use raw_code::{
    RawCodeHash, load_raw_code_hash_counts_by_block_hashes, upsert_raw_code_hashes,
};

/// Checked-in migrations for the bootstrap workspace.
pub const MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Common database settings shared by the bootstrap binaries.
#[derive(Args, Clone, Debug)]
pub struct DatabaseConfig {
    #[arg(long, env = "BIGNAME_DATABASE_URL")]
    pub database_url: Option<String>,
    #[arg(
        long,
        env = "BIGNAME_DATABASE_MAX_CONNECTIONS",
        default_value_t = 10_u32
    )]
    pub max_connections: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            database_url: Some(default_database_url().to_owned()),
            max_connections: 10,
        }
    }
}

/// Default bootstrap database URL for local development.
pub const fn default_database_url() -> &'static str {
    "postgres://bigname:bigname@127.0.0.1:5432/bigname"
}

/// Open a PostgreSQL connection pool using the shared bootstrap settings.
pub async fn connect(config: &DatabaseConfig) -> Result<PgPool> {
    let database_url = config
        .database_url
        .clone()
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .unwrap_or_else(|| default_database_url().to_owned());

    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&database_url)
        .await
        .context("failed to connect to PostgreSQL")
}

/// Apply all checked-in migrations.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    MIGRATOR
        .run(pool)
        .await
        .context("failed to apply checked-in migrations")?;
    info!("checked-in migrations applied");
    Ok(())
}
