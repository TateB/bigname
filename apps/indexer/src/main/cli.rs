use std::path::PathBuf;

use bigname_storage::DatabaseConfig;
use clap::{Args, Parser, Subcommand};

use crate::backfill::{
    BackfillSourceKind, CoinbaseSqlValidationMode, DEFAULT_COINBASE_SQL_API_KEY_ID_ENV,
    DEFAULT_COINBASE_SQL_API_KEY_SECRET_ENV, DEFAULT_COINBASE_SQL_INITIAL_WINDOW_BLOCKS,
    DEFAULT_COINBASE_SQL_MAX_WINDOW_BLOCKS, DEFAULT_COINBASE_SQL_PAGE_LIMIT,
    DEFAULT_COINBASE_SQL_QUERY_CHAR_LIMIT, DEFAULT_COINBASE_SQL_QUERY_TIMEOUT_SECS,
    DEFAULT_COINBASE_SQL_RATE_LIMIT_QPS, DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
};
use crate::bootstrap_backfill::{
    DEFAULT_BOOTSTRAP_BACKFILL_RANGE_BLOCKS, DEFAULT_BOOTSTRAP_BACKFILL_WORKERS,
};
use crate::normalized_replay_catchup::{
    DEFAULT_NORMALIZED_REPLAY_CATCHUP_CHUNK_BLOCKS,
    DEFAULT_NORMALIZED_REPLAY_CATCHUP_MAX_LOGS_PER_CHUNK,
    DEFAULT_NORMALIZED_REPLAY_CATCHUP_POLL_INTERVAL_SECS,
    DEFAULT_NORMALIZED_REPLAY_DEFER_PROJECTION_INDEXES,
};
use crate::ops_catchup::{
    DEFAULT_OPS_CATCHUP_CHUNK_BLOCKS, DEFAULT_OPS_CATCHUP_FOLLOW_POLL_INTERVAL_SECS,
    DEFAULT_OPS_CATCHUP_LEASE_DURATION_SECS,
};
use crate::repair::{
    DEFAULT_ENS_V1_TEXT_RECORD_REPAIR_CHUNK_BLOCKS, DEFAULT_ENS_V1_TEXT_RECORD_REPAIR_PAGE_SIZE,
    DEFAULT_NAME_SURFACE_NORMALIZATION_REPAIR_PAGE_SIZE,
};

#[derive(Parser, Debug)]
#[command(
    name = "bigname-indexer",
    about = "Bootstrap indexer process for bigname"
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    Run(RunArgs),
    Backfill(BackfillArgs),
    OpsCatchup(OpsCatchupArgs),
    Replay(ReplayArgs),
    Rewind(RewindArgs),
    Repair(RepairArgs),
}

#[derive(Args, Debug)]
pub(crate) struct RunArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_MANIFESTS_ROOT",
        default_value = "manifests/mainnet"
    )]
    pub(crate) manifests_root: PathBuf,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_POLL_INTERVAL_SECS",
        default_value_t = 5_u64
    )]
    pub(crate) poll_interval_secs: u64,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_INDEXER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[arg(
        long = "chain-reth-db-source",
        env = "BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES",
        value_delimiter = ','
    )]
    pub(crate) chain_reth_db_sources: Vec<String>,
    #[arg(
        long = "event-silent-reverse-resolver-address",
        env = "BIGNAME_INDEXER_EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES",
        value_delimiter = ','
    )]
    pub(crate) event_silent_reverse_resolver_addresses: Vec<String>,
    #[arg(
        long = "hash-pinned-chunk-blocks",
        env = "BIGNAME_INDEXER_HASH_PINNED_BACKFILL_CHUNK_BLOCKS",
        default_value_t = DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS
    )]
    pub(crate) hash_pinned_chunk_blocks: i64,
    #[arg(
        long = "hash-pinned-adapter-sync",
        env = "BIGNAME_INDEXER_HASH_PINNED_BACKFILL_ADAPTER_SYNC",
        default_value = "auto"
    )]
    pub(crate) hash_pinned_adapter_sync: String,
    #[arg(
        long = "bootstrap-backfill-workers",
        env = "BIGNAME_INDEXER_BOOTSTRAP_BACKFILL_WORKERS",
        default_value_t = DEFAULT_BOOTSTRAP_BACKFILL_WORKERS
    )]
    pub(crate) bootstrap_backfill_workers: usize,
    #[arg(
        long = "bootstrap-backfill-range-blocks",
        env = "BIGNAME_INDEXER_BOOTSTRAP_BACKFILL_RANGE_BLOCKS",
        default_value_t = DEFAULT_BOOTSTRAP_BACKFILL_RANGE_BLOCKS
    )]
    pub(crate) bootstrap_backfill_range_blocks: i64,
    #[arg(
        long = "normalized-replay-catchup-enabled",
        env = "BIGNAME_INDEXER_NORMALIZED_REPLAY_CATCHUP_ENABLED",
        default_value_t = true
    )]
    pub(crate) normalized_replay_catchup_enabled: bool,
    #[arg(
        long = "normalized-replay-catchup-chunk-blocks",
        env = "BIGNAME_INDEXER_NORMALIZED_REPLAY_CATCHUP_CHUNK_BLOCKS",
        default_value_t = DEFAULT_NORMALIZED_REPLAY_CATCHUP_CHUNK_BLOCKS
    )]
    pub(crate) normalized_replay_catchup_chunk_blocks: i64,
    #[arg(
        long = "normalized-replay-catchup-max-logs-per-chunk",
        env = "BIGNAME_INDEXER_NORMALIZED_REPLAY_CATCHUP_MAX_LOGS_PER_CHUNK",
        default_value_t = DEFAULT_NORMALIZED_REPLAY_CATCHUP_MAX_LOGS_PER_CHUNK
    )]
    pub(crate) normalized_replay_catchup_max_logs_per_chunk: usize,
    #[arg(
        long = "normalized-replay-catchup-poll-interval-secs",
        env = "BIGNAME_INDEXER_NORMALIZED_REPLAY_CATCHUP_POLL_INTERVAL_SECS",
        default_value_t = DEFAULT_NORMALIZED_REPLAY_CATCHUP_POLL_INTERVAL_SECS
    )]
    pub(crate) normalized_replay_catchup_poll_interval_secs: u64,
    #[arg(
        long = "normalized-replay-defer-projection-indexes",
        env = "BIGNAME_INDEXER_NORMALIZED_REPLAY_DEFER_PROJECTION_INDEXES",
        default_value_t = DEFAULT_NORMALIZED_REPLAY_DEFER_PROJECTION_INDEXES
    )]
    pub(crate) normalized_replay_defer_projection_indexes: bool,
    #[arg(
        long = "retain-header-audit-fields",
        env = "BIGNAME_INDEXER_RETAIN_HEADER_AUDIT_FIELDS",
        default_value_t = false
    )]
    pub(crate) retain_header_audit_fields: bool,
}

#[derive(Args, Debug)]
pub(crate) struct BackfillArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_MANIFESTS_ROOT",
        default_value = "manifests/mainnet"
    )]
    pub(crate) manifests_root: PathBuf,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_INDEXER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[arg(
        long = "chain-reth-db-source",
        env = "BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES",
        value_delimiter = ','
    )]
    pub(crate) chain_reth_db_sources: Vec<String>,
    #[arg(
        long = "backfill-source",
        env = "BIGNAME_INDEXER_BACKFILL_SOURCE",
        value_enum,
        default_value = "hash-pinned"
    )]
    pub(crate) backfill_source: BackfillSourceKind,
    #[arg(
        long = "coinbase-sql-url",
        env = "BIGNAME_INDEXER_COINBASE_SQL_URLS",
        value_delimiter = ','
    )]
    pub(crate) coinbase_sql_urls: Vec<String>,
    #[arg(
        long = "coinbase-sql-api-key-id-env",
        env = "BIGNAME_INDEXER_COINBASE_SQL_API_KEY_ID_ENV",
        default_value = DEFAULT_COINBASE_SQL_API_KEY_ID_ENV
    )]
    pub(crate) coinbase_sql_api_key_id_env: String,
    #[arg(
        long = "coinbase-sql-api-key-secret-env",
        env = "BIGNAME_INDEXER_COINBASE_SQL_API_KEY_SECRET_ENV",
        default_value = DEFAULT_COINBASE_SQL_API_KEY_SECRET_ENV
    )]
    pub(crate) coinbase_sql_api_key_secret_env: String,
    #[arg(
        long = "coinbase-sql-initial-window-blocks",
        env = "BIGNAME_INDEXER_COINBASE_SQL_INITIAL_WINDOW_BLOCKS",
        default_value_t = DEFAULT_COINBASE_SQL_INITIAL_WINDOW_BLOCKS
    )]
    pub(crate) coinbase_sql_initial_window_blocks: i64,
    #[arg(
        long = "coinbase-sql-max-window-blocks",
        env = "BIGNAME_INDEXER_COINBASE_SQL_MAX_WINDOW_BLOCKS",
        default_value_t = DEFAULT_COINBASE_SQL_MAX_WINDOW_BLOCKS
    )]
    pub(crate) coinbase_sql_max_window_blocks: i64,
    #[arg(
        long = "coinbase-sql-page-limit",
        env = "BIGNAME_INDEXER_COINBASE_SQL_PAGE_LIMIT",
        default_value_t = DEFAULT_COINBASE_SQL_PAGE_LIMIT
    )]
    pub(crate) coinbase_sql_page_limit: usize,
    #[arg(
        long = "coinbase-sql-query-char-limit",
        env = "BIGNAME_INDEXER_COINBASE_SQL_QUERY_CHAR_LIMIT",
        default_value_t = DEFAULT_COINBASE_SQL_QUERY_CHAR_LIMIT
    )]
    pub(crate) coinbase_sql_query_char_limit: usize,
    #[arg(
        long = "coinbase-sql-query-timeout-secs",
        env = "BIGNAME_INDEXER_COINBASE_SQL_QUERY_TIMEOUT_SECS",
        default_value_t = DEFAULT_COINBASE_SQL_QUERY_TIMEOUT_SECS
    )]
    pub(crate) coinbase_sql_query_timeout_secs: u64,
    #[arg(
        long = "coinbase-sql-rate-limit-qps",
        env = "BIGNAME_INDEXER_COINBASE_SQL_RATE_LIMIT_QPS",
        default_value_t = DEFAULT_COINBASE_SQL_RATE_LIMIT_QPS
    )]
    pub(crate) coinbase_sql_rate_limit_qps: u32,
    #[arg(
        long = "coinbase-sql-validation-mode",
        env = "BIGNAME_INDEXER_COINBASE_SQL_VALIDATION_MODE",
        value_enum,
        default_value = "full"
    )]
    pub(crate) coinbase_sql_validation_mode: CoinbaseSqlValidationMode,
    #[arg(
        long = "coinbase-sql-workers",
        env = "BIGNAME_INDEXER_COINBASE_SQL_WORKERS",
        default_value_t = 1_usize
    )]
    pub(crate) coinbase_sql_workers: usize,
    #[arg(
        long = "coinbase-sql-range-blocks",
        env = "BIGNAME_INDEXER_COINBASE_SQL_RANGE_BLOCKS",
        default_value_t = 0_i64
    )]
    pub(crate) coinbase_sql_range_blocks: i64,
    #[arg(long)]
    pub(crate) chain: String,
    #[arg(long)]
    pub(crate) from_block: i64,
    #[arg(long)]
    pub(crate) to_block: i64,
    #[arg(long)]
    pub(crate) idempotency_key: String,
    #[arg(long)]
    pub(crate) deployment_profile: Option<String>,
    #[arg(long, conflicts_with = "watch_targets")]
    pub(crate) source_family: Option<String>,
    #[arg(
        long = "watch-target",
        value_name = "CONTRACT_INSTANCE_ID",
        conflicts_with = "source_family"
    )]
    pub(crate) watch_targets: Vec<sqlx::types::Uuid>,
    #[arg(long)]
    pub(crate) lease_owner: Option<String>,
    #[arg(long)]
    pub(crate) lease_token: Option<String>,
    #[arg(long, default_value_t = 300_u64)]
    pub(crate) lease_duration_secs: u64,
    #[arg(
        long = "hash-pinned-chunk-blocks",
        env = "BIGNAME_INDEXER_HASH_PINNED_BACKFILL_CHUNK_BLOCKS",
        default_value_t = DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS
    )]
    pub(crate) hash_pinned_chunk_blocks: i64,
    #[arg(
        long = "hash-pinned-adapter-sync",
        env = "BIGNAME_INDEXER_HASH_PINNED_BACKFILL_ADAPTER_SYNC",
        default_value = "auto"
    )]
    pub(crate) hash_pinned_adapter_sync: String,
    #[arg(
        long = "retain-header-audit-fields",
        env = "BIGNAME_INDEXER_RETAIN_HEADER_AUDIT_FIELDS",
        default_value_t = false
    )]
    pub(crate) retain_header_audit_fields: bool,
}

#[derive(Args, Debug)]
pub(crate) struct OpsCatchupArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_MANIFESTS_ROOT",
        default_value = "manifests/mainnet"
    )]
    pub(crate) manifests_root: PathBuf,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_INDEXER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[arg(
        long = "chain-reth-db-source",
        env = "BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES",
        value_delimiter = ','
    )]
    pub(crate) chain_reth_db_sources: Vec<String>,
    #[arg(long, env = "BIGNAME_INDEXER_DEPLOYMENT_PROFILE")]
    pub(crate) deployment_profile: Option<String>,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_CHUNK_BLOCKS",
        default_value_t = DEFAULT_OPS_CATCHUP_CHUNK_BLOCKS
    )]
    pub(crate) chunk_blocks: i64,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_FOLLOW",
        default_value_t = false
    )]
    pub(crate) follow: bool,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_FOLLOW_ITERATIONS",
        requires = "follow"
    )]
    pub(crate) follow_iterations: Option<u64>,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_FOLLOW_POLL_INTERVAL_SECS",
        default_value_t = DEFAULT_OPS_CATCHUP_FOLLOW_POLL_INTERVAL_SECS
    )]
    pub(crate) follow_poll_interval_secs: u64,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_LEASE_DURATION_SECS",
        default_value_t = DEFAULT_OPS_CATCHUP_LEASE_DURATION_SECS
    )]
    pub(crate) lease_duration_secs: u64,
    #[arg(long, env = "BIGNAME_INDEXER_OPS_CATCHUP_POSTGRES_MAX_BYTES")]
    pub(crate) postgres_max_bytes: Option<u64>,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_MIN_FREE_DISK_BYTES",
        default_value_t = 0_u64
    )]
    pub(crate) min_writable_free_disk_bytes: u64,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_DISK_PATH",
        default_value = "."
    )]
    pub(crate) writable_free_disk_path: PathBuf,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_OPS_CATCHUP_ESTIMATED_BYTES_PER_BLOCK",
        default_value_t = 0_u64
    )]
    pub(crate) estimated_bytes_per_block: u64,
    #[arg(
        long = "retain-header-audit-fields",
        env = "BIGNAME_INDEXER_RETAIN_HEADER_AUDIT_FIELDS",
        default_value_t = false
    )]
    pub(crate) retain_header_audit_fields: bool,
}

#[derive(Args, Debug)]
pub(crate) struct ReplayArgs {
    #[command(subcommand)]
    pub(crate) command: ReplayCommand,
}

#[derive(Args, Debug)]
pub(crate) struct RepairArgs {
    #[command(subcommand)]
    pub(crate) command: RepairCommand,
}

#[derive(Args, Debug)]
pub(crate) struct RewindArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long, env = "BIGNAME_INDEXER_DEPLOYMENT_PROFILE")]
    pub(crate) deployment_profile: String,
    #[arg(long)]
    pub(crate) chain: String,
    #[arg(long)]
    pub(crate) ancestor_block_number: i64,
    #[arg(long)]
    pub(crate) ancestor_block_hash: String,
    #[arg(long)]
    pub(crate) from_block_hash: Option<String>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum ReplayCommand {
    NormalizedEvents(ReplayNormalizedEventsArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum RepairCommand {
    EnsV1TextRecords(RepairEnsV1TextRecordsArgs),
    NameSurfaceNormalization(RepairNameSurfaceNormalizationArgs),
}

#[derive(Args, Debug)]
pub(crate) struct ReplayNormalizedEventsArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long, env = "BIGNAME_INDEXER_DEPLOYMENT_PROFILE")]
    pub(crate) deployment_profile: String,
    #[arg(long)]
    pub(crate) chain: String,
    #[arg(long, requires = "to_block", conflicts_with = "block_hashes")]
    pub(crate) from_block: Option<i64>,
    #[arg(long, requires = "from_block", conflicts_with = "block_hashes")]
    pub(crate) to_block: Option<i64>,
    #[arg(long = "block-hash", value_name = "BLOCK_HASH")]
    pub(crate) block_hashes: Vec<String>,
}

#[derive(Args, Debug)]
pub(crate) struct RepairEnsV1TextRecordsArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_INDEXER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[arg(
        long = "chain-reth-db-source",
        env = "BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES",
        value_delimiter = ','
    )]
    pub(crate) chain_reth_db_sources: Vec<String>,
    #[arg(long)]
    pub(crate) chain: String,
    #[arg(long, requires = "to_block")]
    pub(crate) from_block: Option<i64>,
    #[arg(long, requires = "from_block")]
    pub(crate) to_block: Option<i64>,
    #[arg(
        long = "chunk-blocks",
        default_value_t = DEFAULT_ENS_V1_TEXT_RECORD_REPAIR_CHUNK_BLOCKS
    )]
    pub(crate) chunk_blocks: i64,
    #[arg(
        long = "candidate-page-size",
        default_value_t = DEFAULT_ENS_V1_TEXT_RECORD_REPAIR_PAGE_SIZE
    )]
    pub(crate) candidate_page_size: i64,
}

#[derive(Args, Debug)]
pub(crate) struct RepairNameSurfaceNormalizationArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(
        long,
        default_value = bigname_domain::normalization::ENS_NORMALIZER_VERSION
    )]
    pub(crate) expected_normalizer: String,
    #[arg(
        long = "page-size",
        default_value_t = DEFAULT_NAME_SURFACE_NORMALIZATION_REPAIR_PAGE_SIZE
    )]
    pub(crate) page_size: i64,
    #[arg(long)]
    pub(crate) limit: Option<i64>,
    #[arg(long, requires = "record_findings")]
    pub(crate) apply_compatible: bool,
    #[arg(long)]
    pub(crate) record_findings: bool,
}
