use bigname_storage::DatabaseConfig;
use clap::{Args, Parser, Subcommand};
use uuid::Uuid;

use crate::inspect;

#[derive(Parser, Debug)]
#[command(
    name = "bigname-worker",
    about = "Bootstrap worker process for bigname"
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    Run(RunArgs),
    Healthcheck(HealthcheckArgs),
    Migrate(MigrateArgs),
    AddressNamesCurrent(AddressNamesCurrentArgs),
    ChildrenCurrent(ChildrenCurrentArgs),
    Execution(ExecutionArgs),
    Inspect(inspect::InspectArgs),
    LabelPreimages(LabelPreimagesArgs),
    ManifestDrift(ManifestDriftArgs),
    NameCurrent(NameCurrentArgs),
    PermissionsCurrent(PermissionsCurrentArgs),
    PrimaryNamesCurrent(PrimaryNamesCurrentArgs),
    RawFacts(RawFactsArgs),
    Replay(ReplayArgs),
    RecordInventoryCurrent(RecordInventoryCurrentArgs),
    ResolverCurrent(ResolverCurrentArgs),
}

#[derive(Args, Debug)]
pub(crate) struct RunArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_WORKER_POLL_INTERVAL_SECS",
        default_value_t = 5_u64
    )]
    pub(crate) poll_interval_secs: u64,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_WORKER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[arg(
        long,
        env = "BIGNAME_WORKER_TEXT_HYDRATION_MULTICALL3_ADDRESS",
        default_value = "0xcA11bde05977b3631167028862bE2a173976CA11"
    )]
    pub(crate) text_hydration_multicall3_address: String,
    #[arg(
        long,
        env = "BIGNAME_WORKER_TEXT_HYDRATION_BATCH_SIZE",
        default_value_t = 250_usize
    )]
    pub(crate) text_hydration_batch_size: usize,
    #[arg(
        long = "legacy-reverse-resolver-address",
        env = "BIGNAME_WORKER_PRIMARY_NAME_LEGACY_REVERSE_RESOLVER_ADDRESSES",
        value_delimiter = ','
    )]
    pub(crate) legacy_reverse_resolver_addresses: Vec<String>,
    #[arg(
        long,
        env = "BIGNAME_WORKER_PRIMARY_NAME_LEGACY_REVERSE_HYDRATION_MULTICALL3_ADDRESS",
        default_value = "0xcA11bde05977b3631167028862bE2a173976CA11"
    )]
    pub(crate) legacy_reverse_hydration_multicall3_address: String,
    #[arg(
        long,
        env = "BIGNAME_WORKER_PRIMARY_NAME_LEGACY_REVERSE_HYDRATION_BATCH_SIZE",
        default_value_t = 250_usize
    )]
    pub(crate) legacy_reverse_hydration_batch_size: usize,
}

#[derive(Args, Debug)]
pub(crate) struct HealthcheckArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
}

#[derive(Args, Debug)]
pub(crate) struct MigrateArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
}

#[derive(Args, Debug)]
pub(crate) struct NameCurrentArgs {
    #[command(subcommand)]
    pub(crate) command: NameCurrentCommand,
}

#[derive(Args, Debug)]
pub(crate) struct AddressNamesCurrentArgs {
    #[command(subcommand)]
    pub(crate) command: AddressNamesCurrentCommand,
}

#[derive(Args, Debug)]
pub(crate) struct ChildrenCurrentArgs {
    #[command(subcommand)]
    pub(crate) command: ChildrenCurrentCommand,
}

#[derive(Args, Debug)]
pub(crate) struct ExecutionArgs {
    #[command(subcommand)]
    pub(crate) command: ExecutionCommand,
}

#[derive(Args, Debug)]
pub(crate) struct LabelPreimagesArgs {
    #[command(subcommand)]
    pub(crate) command: LabelPreimagesCommand,
}

#[derive(Args, Debug)]
pub(crate) struct ManifestDriftArgs {
    #[command(subcommand)]
    pub(crate) command: ManifestDriftCommand,
}

#[derive(Args, Debug)]
pub(crate) struct PermissionsCurrentArgs {
    #[command(subcommand)]
    pub(crate) command: PermissionsCurrentCommand,
}

#[derive(Args, Debug)]
pub(crate) struct PrimaryNamesCurrentArgs {
    #[command(subcommand)]
    pub(crate) command: PrimaryNamesCurrentCommand,
}

#[derive(Args, Debug)]
pub(crate) struct RawFactsArgs {
    #[command(subcommand)]
    pub(crate) command: RawFactsCommand,
}

#[derive(Args, Debug)]
pub(crate) struct ReplayArgs {
    #[command(subcommand)]
    pub(crate) command: ReplayCommand,
}

#[derive(Args, Debug)]
pub(crate) struct RecordInventoryCurrentArgs {
    #[command(subcommand)]
    pub(crate) command: RecordInventoryCurrentCommand,
}

#[derive(Args, Debug)]
pub(crate) struct ResolverCurrentArgs {
    #[command(subcommand)]
    pub(crate) command: ResolverCurrentCommand,
}

#[derive(Subcommand, Debug)]
pub(crate) enum NameCurrentCommand {
    Rebuild(NameCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum AddressNamesCurrentCommand {
    Rebuild(AddressNamesCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum ChildrenCurrentCommand {
    Rebuild(ChildrenCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum ExecutionCommand {
    InvalidateVerifiedResolutionManifest(InvalidateVerifiedResolutionManifestArgs),
    InvalidateVerifiedResolutionTopologyBoundary(InvalidateVerifiedResolutionBoundaryArgs),
    InvalidateVerifiedResolutionRecordBoundary(InvalidateVerifiedResolutionBoundaryArgs),
    InvalidateVerifiedPrimaryNameManifest(InvalidateVerifiedPrimaryNameManifestArgs),
    InvalidateVerifiedPrimaryNameTopologyBoundary(InvalidateVerifiedPrimaryNameBoundaryArgs),
    InvalidateVerifiedPrimaryNameRecordBoundary(InvalidateVerifiedPrimaryNameBoundaryArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum LabelPreimagesCommand {
    ImportEnsRainbow(LabelPreimagesImportEnsRainbowArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum ManifestDriftCommand {
    Audit(ManifestDriftAuditArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum PermissionsCurrentCommand {
    Rebuild(PermissionsCurrentRebuildArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum PrimaryNamesCurrentCommand {
    Rebuild(PrimaryNamesCurrentRebuildArgs),
    HydrateLegacyReverseResolver(PrimaryNamesCurrentHydrateLegacyReverseResolverArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum RawFactsCommand {
    CompactLogStaging(CompactLogStagingArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum ReplayCommand {
    AllCurrentProjections(AllCurrentProjectionsArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum RecordInventoryCurrentCommand {
    Rebuild(RecordInventoryCurrentRebuildArgs),
    HydrateTextValues(RecordInventoryCurrentHydrateTextValuesArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum ResolverCurrentCommand {
    Rebuild(ResolverCurrentRebuildArgs),
}

#[derive(Args, Debug)]
pub(crate) struct NameCurrentRebuildArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) logical_name_id: Option<String>,
}

#[derive(Args, Debug)]
pub(crate) struct AddressNamesCurrentRebuildArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) address: Option<String>,
}

#[derive(Args, Debug)]
pub(crate) struct ChildrenCurrentRebuildArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) logical_name_id: Option<String>,
}

#[derive(Args, Debug)]
pub(crate) struct LabelPreimagesImportEnsRainbowArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long, value_parser = parse_positive_i64)]
    pub(crate) batch_size: Option<i64>,
    #[arg(long, value_parser = parse_non_negative_i64)]
    pub(crate) limit: Option<i64>,
}

fn parse_positive_i64(value: &str) -> Result<i64, String> {
    let parsed = value
        .parse::<i64>()
        .map_err(|error| format!("must be a positive integer: {error}"))?;
    if parsed > 0 {
        Ok(parsed)
    } else {
        Err("must be a positive integer".to_owned())
    }
}

fn parse_non_negative_i64(value: &str) -> Result<i64, String> {
    let parsed = value
        .parse::<i64>()
        .map_err(|error| format!("must be a non-negative integer: {error}"))?;
    if parsed >= 0 {
        Ok(parsed)
    } else {
        Err("must be non-negative".to_owned())
    }
}

#[derive(Args, Debug)]
pub(crate) struct InvalidateVerifiedResolutionManifestArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) namespace: String,
    #[arg(long)]
    pub(crate) source_manifest_id: Option<i64>,
    #[arg(long)]
    pub(crate) source_family: Option<String>,
    #[arg(long)]
    pub(crate) manifest_version: i64,
}

#[derive(Args, Debug)]
pub(crate) struct InvalidateVerifiedResolutionBoundaryArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) namespace: String,
    #[arg(long)]
    pub(crate) logical_name_id: String,
    #[arg(long)]
    pub(crate) resource_id: Uuid,
    #[arg(long)]
    pub(crate) normalized_event_id: Option<i64>,
    #[arg(long)]
    pub(crate) event_kind: Option<String>,
    #[arg(long)]
    pub(crate) chain_id: String,
    #[arg(long)]
    pub(crate) block_number: i64,
    #[arg(long)]
    pub(crate) block_hash: String,
    #[arg(long)]
    pub(crate) timestamp: String,
}

#[derive(Args, Debug)]
pub(crate) struct InvalidateVerifiedPrimaryNameManifestArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) namespace: String,
    #[arg(long)]
    pub(crate) address: String,
    #[arg(long)]
    pub(crate) coin_type: String,
    #[arg(long)]
    pub(crate) source_manifest_id: Option<i64>,
    #[arg(long)]
    pub(crate) source_family: Option<String>,
    #[arg(long)]
    pub(crate) manifest_version: i64,
}

#[derive(Args, Debug)]
pub(crate) struct InvalidateVerifiedPrimaryNameBoundaryArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) namespace: String,
    #[arg(long)]
    pub(crate) address: String,
    #[arg(long)]
    pub(crate) coin_type: String,
    #[arg(long)]
    pub(crate) logical_name_id: String,
    #[arg(long)]
    pub(crate) resource_id: Uuid,
    #[arg(long)]
    pub(crate) normalized_event_id: Option<i64>,
    #[arg(long)]
    pub(crate) event_kind: Option<String>,
    #[arg(long)]
    pub(crate) chain_id: String,
    #[arg(long)]
    pub(crate) block_number: i64,
    #[arg(long)]
    pub(crate) block_hash: String,
    #[arg(long)]
    pub(crate) timestamp: String,
}

#[derive(Args, Debug)]
pub(crate) struct ManifestDriftAuditArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) json: bool,
    #[arg(long)]
    pub(crate) fail_on_alert: bool,
}

#[derive(Args, Debug)]
pub(crate) struct PermissionsCurrentRebuildArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) resource_id: Option<String>,
}

#[derive(Args, Debug)]
pub(crate) struct PrimaryNamesCurrentRebuildArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) address: Option<String>,
    #[arg(long)]
    pub(crate) namespace: Option<String>,
    #[arg(long)]
    pub(crate) coin_type: Option<String>,
}

#[derive(Args, Debug)]
pub(crate) struct PrimaryNamesCurrentHydrateLegacyReverseResolverArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_WORKER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[arg(long, default_value = "0xcA11bde05977b3631167028862bE2a173976CA11")]
    pub(crate) multicall3_address: String,
    #[arg(long, default_value_t = 250_usize)]
    pub(crate) batch_size: usize,
    #[arg(
        long = "legacy-reverse-resolver-address",
        env = "BIGNAME_WORKER_PRIMARY_NAME_LEGACY_REVERSE_RESOLVER_ADDRESSES",
        value_delimiter = ','
    )]
    pub(crate) legacy_reverse_resolver_addresses: Vec<String>,
}

#[derive(Args, Debug)]
pub(crate) struct CompactLogStagingArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) dry_run: bool,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct AllCurrentProjectionsArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) json: bool,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_WORKER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[arg(
        long,
        env = "BIGNAME_WORKER_TEXT_HYDRATION_MULTICALL3_ADDRESS",
        default_value = "0xcA11bde05977b3631167028862bE2a173976CA11"
    )]
    pub(crate) text_hydration_multicall3_address: String,
    #[arg(
        long,
        env = "BIGNAME_WORKER_TEXT_HYDRATION_BATCH_SIZE",
        default_value_t = 250_usize
    )]
    pub(crate) text_hydration_batch_size: usize,
    #[arg(
        long = "legacy-reverse-resolver-address",
        env = "BIGNAME_WORKER_PRIMARY_NAME_LEGACY_REVERSE_RESOLVER_ADDRESSES",
        value_delimiter = ','
    )]
    pub(crate) legacy_reverse_resolver_addresses: Vec<String>,
    #[arg(
        long,
        env = "BIGNAME_WORKER_PRIMARY_NAME_LEGACY_REVERSE_HYDRATION_MULTICALL3_ADDRESS",
        default_value = "0xcA11bde05977b3631167028862bE2a173976CA11"
    )]
    pub(crate) legacy_reverse_hydration_multicall3_address: String,
    #[arg(
        long,
        env = "BIGNAME_WORKER_PRIMARY_NAME_LEGACY_REVERSE_HYDRATION_BATCH_SIZE",
        default_value_t = 250_usize
    )]
    pub(crate) legacy_reverse_hydration_batch_size: usize,
}

#[derive(Args, Debug)]
pub(crate) struct RecordInventoryCurrentRebuildArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) resource_id: Option<String>,
    #[arg(long)]
    pub(crate) hydrate_text_values: bool,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_WORKER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[arg(long, default_value = "0xcA11bde05977b3631167028862bE2a173976CA11")]
    pub(crate) text_hydration_multicall3_address: String,
    #[arg(long, default_value_t = 250_usize)]
    pub(crate) text_hydration_batch_size: usize,
}

#[derive(Args, Debug)]
pub(crate) struct RecordInventoryCurrentHydrateTextValuesArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) resource_id: Option<String>,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_WORKER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[arg(long, default_value = "0xcA11bde05977b3631167028862bE2a173976CA11")]
    pub(crate) multicall3_address: String,
    #[arg(long, default_value_t = 250_usize)]
    pub(crate) batch_size: usize,
}

#[derive(Args, Debug)]
pub(crate) struct ResolverCurrentRebuildArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) chain_id: Option<String>,
    #[arg(long)]
    pub(crate) resolver_address: Option<String>,
}

impl Cli {
    pub(crate) fn writes_machine_json(&self) -> bool {
        matches!(
            &self.command,
            Command::Inspect(_)
                | Command::ManifestDrift(ManifestDriftArgs {
                    command: ManifestDriftCommand::Audit(ManifestDriftAuditArgs { json: true, .. })
                })
                | Command::RawFacts(RawFactsArgs {
                    command: RawFactsCommand::CompactLogStaging(CompactLogStagingArgs {
                        json: true,
                        ..
                    })
                })
                | Command::Replay(ReplayArgs {
                    command: ReplayCommand::AllCurrentProjections(AllCurrentProjectionsArgs {
                        json: true,
                        ..
                    })
                })
        )
    }
}
