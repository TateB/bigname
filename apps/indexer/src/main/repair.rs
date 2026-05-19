use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use bigname_storage::serialize_jsonb_value;
use serde_json::Value;
use sqlx::Row;
use tracing::info;

use crate::{
    ens_v1_resolver::SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
    provider::{ChainProviderOps, ProviderLog},
    reconciliation::{keccak256_hex, parse_hex_bytes},
};

pub(crate) const DEFAULT_ENS_V1_TEXT_RECORD_REPAIR_CHUNK_BLOCKS: i64 = 5_000;
pub(crate) const DEFAULT_ENS_V1_TEXT_RECORD_REPAIR_PAGE_SIZE: i64 = 10_000;

const DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY: &str = "ens_v1_unwrapped_authority";
const EVENT_KIND_RECORD_CHANGED: &str = "RecordChanged";
const TEXT_CHANGED_WITHOUT_VALUE_SIGNATURE: &str = "TextChanged(bytes32,string,string)";
const TEXT_CHANGED_WITH_VALUE_SIGNATURE: &str = "TextChanged(bytes32,string,string,string)";
const TEXT_RECORD_FAMILY: &str = "text";
const LEGACY_TEXT_RECORD_KEY: &str = "text";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EnsV1TextRecordRepairConfig {
    pub(crate) chain: String,
    pub(crate) from_block: Option<i64>,
    pub(crate) to_block: Option<i64>,
    pub(crate) chunk_blocks: i64,
    pub(crate) candidate_page_size: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EnsV1TextRecordRepairOutcome {
    pub(crate) chain: String,
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
    pub(crate) candidate_count: usize,
    pub(crate) fetched_log_count: usize,
    pub(crate) matched_log_count: usize,
    pub(crate) repaired_event_count: usize,
    pub(crate) missing_log_count: usize,
    pub(crate) skipped_decode_count: usize,
}

include!("repair/text_records.rs");

pub(crate) async fn repair_ens_v1_text_records_from_provider(
    pool: &sqlx::PgPool,
    provider: &(impl ChainProviderOps + ?Sized),
    config: EnsV1TextRecordRepairConfig,
) -> Result<EnsV1TextRecordRepairOutcome> {
    validate_repair_config(&config)?;
    let Some((from_block, to_block)) = resolve_repair_block_range(pool, &config).await? else {
        return Ok(EnsV1TextRecordRepairOutcome {
            chain: config.chain,
            from_block: 0,
            to_block: 0,
            candidate_count: 0,
            fetched_log_count: 0,
            matched_log_count: 0,
            repaired_event_count: 0,
            missing_log_count: 0,
            skipped_decode_count: 0,
        });
    };

    let mut outcome = EnsV1TextRecordRepairOutcome {
        chain: config.chain.clone(),
        from_block,
        to_block,
        candidate_count: 0,
        fetched_log_count: 0,
        matched_log_count: 0,
        repaired_event_count: 0,
        missing_log_count: 0,
        skipped_decode_count: 0,
    };

    let mut chunk_from = from_block;
    while chunk_from <= to_block {
        let chunk_to = chunk_from
            .checked_add(config.chunk_blocks - 1)
            .map(|value| value.min(to_block))
            .context("ENSv1 text record repair chunk bound overflowed")?;
        let mut excluded_candidate_ids = Vec::new();

        loop {
            let candidates = load_text_record_repair_candidates(
                pool,
                &config.chain,
                chunk_from,
                chunk_to,
                config.candidate_page_size,
                &excluded_candidate_ids,
            )
            .await?;
            if candidates.is_empty() {
                break;
            }

            let (fetched_log_count, repaired_count) = repair_text_record_candidate_page(
                pool,
                provider,
                &candidates,
                &mut excluded_candidate_ids,
                &mut outcome,
            )
            .await?;
            info!(
                service = "indexer",
                command = "repair ens-v1-text-records",
                chain = %config.chain,
                from_block = chunk_from,
                to_block = chunk_to,
                candidate_count = candidates.len(),
                fetched_log_count,
                repaired_event_count = repaired_count,
                "ENSv1 text record repair chunk page completed"
            );

            if candidates.len()
                < usize::try_from(config.candidate_page_size)
                    .context("candidate_page_size overflowed usize")?
            {
                break;
            }
        }

        chunk_from = chunk_to
            .checked_add(1)
            .context("ENSv1 text record repair chunk advance overflowed")?;
    }

    Ok(outcome)
}

fn validate_repair_config(config: &EnsV1TextRecordRepairConfig) -> Result<()> {
    if config.chain.trim().is_empty() {
        bail!("ENSv1 text record repair chain must not be empty");
    }
    if config.chunk_blocks <= 0 {
        bail!(
            "ENSv1 text record repair chunk_blocks must be positive, got {}",
            config.chunk_blocks
        );
    }
    if config.candidate_page_size <= 0 {
        bail!(
            "ENSv1 text record repair candidate_page_size must be positive, got {}",
            config.candidate_page_size
        );
    }
    if let (Some(from_block), Some(to_block)) = (config.from_block, config.to_block) {
        if from_block < 0 || to_block < 0 {
            bail!("ENSv1 text record repair block range must be non-negative");
        }
        if from_block > to_block {
            bail!("ENSv1 text record repair from_block {from_block} is after to_block {to_block}");
        }
    }
    Ok(())
}
