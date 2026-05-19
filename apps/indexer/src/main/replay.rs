use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use bigname_manifests::{WatchedSourceSelector, WatchedTargetIdentity};
use sqlx::types::time::OffsetDateTime;

use crate::{
    cli::{BackfillArgs, ReplayNormalizedEventsArgs},
    reconciliation::RawFactNormalizedEventReplaySelection,
};

pub(crate) fn replay_normalized_events_selection(
    args: &ReplayNormalizedEventsArgs,
) -> Result<RawFactNormalizedEventReplaySelection> {
    if !args.block_hashes.is_empty() {
        return Ok(RawFactNormalizedEventReplaySelection::BlockHashes(
            args.block_hashes.clone(),
        ));
    }

    let from_block = args
        .from_block
        .context("--from-block is required when --block-hash is not supplied")?;
    let to_block = args
        .to_block
        .context("--to-block is required when --block-hash is not supplied")?;
    Ok(RawFactNormalizedEventReplaySelection::BlockRange {
        from_block,
        to_block,
    })
}

pub(crate) fn backfill_source_selector(args: &BackfillArgs) -> Result<WatchedSourceSelector> {
    if let Some(source_family) = &args.source_family {
        let source_family = source_family.trim();
        if source_family.is_empty() {
            bail!("--source-family must not be empty");
        }
        return Ok(WatchedSourceSelector::SourceFamily(
            source_family.to_owned(),
        ));
    }

    if !args.watch_targets.is_empty() {
        return Ok(WatchedSourceSelector::WatchedTargetSet(
            args.watch_targets
                .iter()
                .copied()
                .map(|contract_instance_id| WatchedTargetIdentity {
                    contract_instance_id,
                })
                .collect(),
        ));
    }

    Ok(WatchedSourceSelector::WholeActiveWatchedChain)
}

pub(crate) fn deployment_profile_from_manifest_root(manifests_root: &std::path::Path) -> String {
    let root_name = manifests_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("manifests");
    if root_name == "manifests" {
        "mainnet".to_owned()
    } else if let Some(profile) = root_name.strip_prefix("manifests-") {
        profile.to_owned()
    } else {
        root_name.to_owned()
    }
}

pub(crate) fn default_backfill_lease_owner() -> String {
    format!("bigname-indexer:{}", std::process::id())
}

pub(crate) fn generated_backfill_lease_token() -> Result<String> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_nanos();
    Ok(format!("bigname-indexer:{}:{nanos}", std::process::id()))
}

pub(crate) fn backfill_lease_expires_at(lease_duration_secs: u64) -> Result<OffsetDateTime> {
    if lease_duration_secs == 0 {
        bail!("backfill lease duration must be greater than zero");
    }
    let duration = i64::try_from(lease_duration_secs)
        .context("backfill lease duration does not fit in i64 seconds")?;
    let deadline = OffsetDateTime::now_utc()
        .unix_timestamp()
        .checked_add(duration)
        .context("backfill lease expiry timestamp overflowed")?;
    OffsetDateTime::from_unix_timestamp(deadline)
        .context("backfill lease expiry timestamp is out of range")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use bigname_storage::DatabaseConfig;
    use sqlx::types::Uuid;

    use super::*;

    fn replay_args(
        from_block: Option<i64>,
        to_block: Option<i64>,
        block_hashes: Vec<String>,
    ) -> ReplayNormalizedEventsArgs {
        ReplayNormalizedEventsArgs {
            database: DatabaseConfig::default(),
            deployment_profile: "mainnet".to_owned(),
            chain: "ethereum-mainnet".to_owned(),
            from_block,
            to_block,
            block_hashes,
        }
    }

    fn backfill_args() -> BackfillArgs {
        BackfillArgs {
            database: DatabaseConfig::default(),
            manifests_root: PathBuf::from("manifests/mainnet"),
            chain_rpc_urls: Vec::new(),
            chain_reth_db_sources: Vec::new(),
            chain: "ethereum-mainnet".to_owned(),
            from_block: 1,
            to_block: 2,
            idempotency_key: "test".to_owned(),
            deployment_profile: None,
            source_family: None,
            watch_targets: Vec::new(),
            lease_owner: None,
            lease_token: None,
            lease_duration_secs: 300,
            hash_pinned_chunk_blocks: crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
            hash_pinned_adapter_sync: "inline".to_owned(),
            retain_header_audit_fields: false,
        }
    }

    #[test]
    fn replay_normalized_events_selection_uses_block_hashes_or_range() -> Result<()> {
        let hashes = vec!["0xabc".to_owned(), "0xdef".to_owned()];
        let block_hash_selection =
            replay_normalized_events_selection(&replay_args(None, None, hashes.clone()))?;
        assert_eq!(
            block_hash_selection,
            RawFactNormalizedEventReplaySelection::BlockHashes(hashes)
        );

        let range_selection =
            replay_normalized_events_selection(&replay_args(Some(10), Some(12), Vec::new()))?;
        assert_eq!(
            range_selection,
            RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: 10,
                to_block: 12
            }
        );

        let missing_range_error =
            replay_normalized_events_selection(&replay_args(None, None, Vec::new()))
                .expect_err("range selector must require both bounds without block hashes");
        assert!(
            format!("{missing_range_error:?}")
                .contains("--from-block is required when --block-hash is not supplied")
        );

        Ok(())
    }

    #[test]
    fn backfill_source_selector_maps_and_validates_cli_selector() -> Result<()> {
        let mut source_family_args = backfill_args();
        source_family_args.source_family = Some(" ens_v1_registry_l1 ".to_owned());
        assert_eq!(
            backfill_source_selector(&source_family_args)?,
            WatchedSourceSelector::SourceFamily("ens_v1_registry_l1".to_owned())
        );

        let mut empty_source_family_args = backfill_args();
        empty_source_family_args.source_family = Some(" ".to_owned());
        let empty_error = backfill_source_selector(&empty_source_family_args)
            .expect_err("empty source family must be rejected");
        assert!(format!("{empty_error:?}").contains("--source-family must not be empty"));

        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);
        let mut target_args = backfill_args();
        target_args.watch_targets = vec![first, second];
        assert_eq!(
            backfill_source_selector(&target_args)?,
            WatchedSourceSelector::WatchedTargetSet(vec![
                WatchedTargetIdentity {
                    contract_instance_id: first
                },
                WatchedTargetIdentity {
                    contract_instance_id: second
                },
            ])
        );

        assert_eq!(
            backfill_source_selector(&backfill_args())?,
            WatchedSourceSelector::WholeActiveWatchedChain
        );

        Ok(())
    }

    #[test]
    fn deployment_profile_from_manifest_root_derives_legacy_profiles() {
        assert_eq!(
            deployment_profile_from_manifest_root(&PathBuf::from("manifests")),
            "mainnet"
        );
        assert_eq!(
            deployment_profile_from_manifest_root(&PathBuf::from("manifests/mainnet")),
            "mainnet"
        );
        assert_eq!(
            deployment_profile_from_manifest_root(&PathBuf::from("manifests/sepolia")),
            "sepolia"
        );
        assert_eq!(
            deployment_profile_from_manifest_root(&PathBuf::from("manifests-sepolia-dev")),
            "sepolia-dev"
        );
        assert_eq!(
            deployment_profile_from_manifest_root(&PathBuf::from("custom-profile")),
            "custom-profile"
        );
    }
}
