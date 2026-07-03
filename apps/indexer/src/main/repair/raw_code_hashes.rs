use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use bigname_storage::{
    RawCodeHashAddressVariant, RawCodeHashCorrectionBatchOutcome, RawCodeHashCorrectionCandidate,
    RawCodeHashCorrectionUpdate, apply_raw_code_hash_corrections,
    count_raw_code_hash_correction_candidates, count_raw_code_hash_correction_orphaned_skips,
    load_raw_code_hash_address_variants, load_raw_code_hash_correction_page,
};
use sqlx::{PgPool, types::time::OffsetDateTime};
use tracing::info;

use crate::{
    cli::RepairRawCodeHashesArgs,
    provider::{ChainProvider, ChainProviderOps, JsonRpcProvider, RethDbProvider},
};

#[path = "raw_code_hashes/args.rs"]
mod args;
#[cfg(test)]
#[path = "raw_code_hashes/tests.rs"]
mod tests;
#[path = "raw_code_hashes/verification.rs"]
mod verification;

use args::{parse_single_chain_source, parse_timestamp_arg};
use verification::{
    PROOF_SPOT_CHECK_TIMEOUT_SECS, derive_code_hashes, verify_rpc_code_sample,
    verify_rpc_proof_spot_check,
};

pub(crate) const DEFAULT_RAW_CODE_HASH_CORRECTION_PAGE_SIZE: i64 = 10_000;
pub(crate) const DEFAULT_RAW_CODE_HASH_CORRECTION_WRITE_BATCH_SIZE: usize = 5_000;
pub(crate) const DEFAULT_RAW_CODE_HASH_CORRECTION_RPC_SAMPLE_PERCENT: f64 = 1.0;
pub(crate) const RAW_CODE_HASH_CORRECTION_DEFAULT_OBSERVED_FROM: &str = "2026-05-01T00:00:00Z";
pub(crate) const RAW_CODE_HASH_CORRECTION_DEFAULT_OBSERVED_BEFORE: &str = "2026-07-04T00:00:00Z";
const MAX_UNEXPECTED_VARIANT_EXAMPLES: usize = 10;

#[derive(Clone, Debug)]
pub(crate) struct RawCodeHashCorrectionConfig {
    pub(crate) chain: String,
    pub(crate) observed_from: OffsetDateTime,
    pub(crate) observed_before: OffsetDateTime,
    pub(crate) page_size: i64,
    pub(crate) write_batch_size: usize,
    pub(crate) rpc_sample_percent: f64,
    pub(crate) dry_run: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RawCodeHashCorrectionOutcome {
    pub(crate) scanned_count: i64,
    pub(crate) address_count: i64,
    pub(crate) already_correct_count: i64,
    pub(crate) to_correct_count: i64,
    pub(crate) orphaned_skipped_count: i64,
    pub(crate) rpc_sample_count: i64,
    pub(crate) proof_spot_check_attempted_count: i64,
    pub(crate) proof_spot_check_verified_count: i64,
    pub(crate) proof_spot_check_timed_out: bool,
    pub(crate) corrected_count: i64,
    pub(crate) already_correct_during_write_count: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct AddressCorrectionCensus {
    scanned_count: i64,
    already_correct_count: i64,
    to_correct_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DerivedCodeHash {
    pub(super) code_hash: String,
    pub(super) code_byte_length: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CorrectionSampleRow {
    pub(super) raw_code_hash_id: i64,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) contract_address: String,
    pub(super) rederived_code_hash: String,
    pub(super) rederived_code_byte_length: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VerifiedCorrectionUpdate {
    update: RawCodeHashCorrectionUpdate,
    block_hash: String,
    block_number: i64,
    contract_address: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UnexpectedVariantRow {
    raw_code_hash_id: i64,
    block_hash: String,
    contract_address: String,
    stored_code_hash: String,
    rederived_code_hash: String,
    stored_variants: Vec<String>,
}

#[derive(Default)]
struct ClassificationOutcome {
    scanned_count: i64,
    already_correct_count: i64,
    to_correct_count: i64,
    address_census: BTreeMap<String, AddressCorrectionCensus>,
    samples: Vec<CorrectionSampleRow>,
    updates: Vec<VerifiedCorrectionUpdate>,
    unexpected_variant_count: i64,
    unexpected_variant_examples: Vec<UnexpectedVariantRow>,
}

struct ClassificationAccumulator<'a> {
    variants_by_address: &'a BTreeMap<String, RawCodeHashAddressVariant>,
    sample_stride: i64,
    seen_sample_ids: BTreeSet<i64>,
    seen_addresses: BTreeSet<String>,
    outcome: ClassificationOutcome,
}

impl<'a> ClassificationAccumulator<'a> {
    fn new(
        variants_by_address: &'a BTreeMap<String, RawCodeHashAddressVariant>,
        rpc_sample_percent: f64,
    ) -> Result<Self> {
        Ok(Self {
            variants_by_address,
            sample_stride: sample_stride(rpc_sample_percent)?,
            seen_sample_ids: BTreeSet::new(),
            seen_addresses: BTreeSet::new(),
            outcome: ClassificationOutcome::default(),
        })
    }

    fn observe(
        &mut self,
        row: &RawCodeHashCorrectionCandidate,
        derived: &DerivedCodeHash,
    ) -> Result<()> {
        if derived.code_byte_length == 0 && row.code_byte_length > 0 {
            bail!(
                "raw code-hash correction re-derived empty code for non-empty stored row {} at {} contract {}; refusing without per-row proof",
                row.raw_code_hash_id,
                row.block_hash,
                row.contract_address
            );
        }

        let address_census = self
            .outcome
            .address_census
            .entry(row.contract_address.clone())
            .or_default();
        address_census.scanned_count += 1;

        let needs_correction =
            row.code_hash != derived.code_hash || row.code_byte_length != derived.code_byte_length;
        if needs_correction {
            self.outcome.to_correct_count += 1;
            address_census.to_correct_count += 1;
        } else {
            self.outcome.already_correct_count += 1;
            address_census.already_correct_count += 1;
        }

        let unexpected_variant = self.record_unexpected_variant(row, derived);
        self.record_sample(row, derived, unexpected_variant);
        self.outcome.scanned_count += 1;

        if needs_correction {
            self.outcome.updates.push(VerifiedCorrectionUpdate {
                update: RawCodeHashCorrectionUpdate {
                    raw_code_hash_id: row.raw_code_hash_id,
                    stored_code_hash: row.code_hash.clone(),
                    stored_code_byte_length: row.code_byte_length,
                    corrected_code_hash: derived.code_hash.clone(),
                    corrected_code_byte_length: derived.code_byte_length,
                },
                block_hash: row.block_hash.clone(),
                block_number: row.block_number,
                contract_address: row.contract_address.clone(),
            });
        }
        Ok(())
    }

    fn finish(self) -> ClassificationOutcome {
        self.outcome
    }

    fn record_sample(
        &mut self,
        row: &RawCodeHashCorrectionCandidate,
        derived: &DerivedCodeHash,
        force: bool,
    ) {
        let row_index = self.outcome.scanned_count;
        let stride_sample = row_index % self.sample_stride == 0;
        let address_sample = self.seen_addresses.insert(row.contract_address.clone());
        if (force || stride_sample || address_sample)
            && self.seen_sample_ids.insert(row.raw_code_hash_id)
        {
            self.outcome.samples.push(CorrectionSampleRow {
                raw_code_hash_id: row.raw_code_hash_id,
                block_hash: row.block_hash.clone(),
                block_number: row.block_number,
                contract_address: row.contract_address.clone(),
                rederived_code_hash: derived.code_hash.clone(),
                rederived_code_byte_length: derived.code_byte_length,
            });
        }
    }

    fn record_unexpected_variant(
        &mut self,
        row: &RawCodeHashCorrectionCandidate,
        derived: &DerivedCodeHash,
    ) -> bool {
        let Some(variant) = self.variants_by_address.get(&row.contract_address) else {
            return false;
        };
        if variant.code_hashes.len() < 2 || variant.code_hashes.contains(&derived.code_hash) {
            return false;
        }

        self.outcome.unexpected_variant_count += 1;
        if self.outcome.unexpected_variant_examples.len() < MAX_UNEXPECTED_VARIANT_EXAMPLES {
            self.outcome
                .unexpected_variant_examples
                .push(UnexpectedVariantRow {
                    raw_code_hash_id: row.raw_code_hash_id,
                    block_hash: row.block_hash.clone(),
                    contract_address: row.contract_address.clone(),
                    stored_code_hash: row.code_hash.clone(),
                    rederived_code_hash: derived.code_hash.clone(),
                    stored_variants: variant.code_hashes.clone(),
                });
        }
        true
    }
}

pub(crate) async fn repair_raw_code_hashes_command(args: RepairRawCodeHashesArgs) -> Result<()> {
    let observed_from = parse_timestamp_arg(
        &args.observed_from,
        "raw code-hash correction observed-from",
    )?;
    let observed_before = parse_timestamp_arg(
        &args.observed_before,
        "raw code-hash correction observed-before",
    )?;
    let (reth_chain, reth_datadir) =
        parse_single_chain_source(&args.chain_reth_db_source, "chain Reth DB source")?;
    let (rpc_chain, rpc_url) = parse_single_chain_source(&args.chain_rpc_url, "chain RPC URL")?;
    ensure!(
        reth_chain == args.chain,
        "raw code-hash correction Reth DB source chain {reth_chain} does not match requested chain {}",
        args.chain
    );
    ensure!(
        rpc_chain == args.chain,
        "raw code-hash correction RPC source chain {rpc_chain} does not match requested chain {}",
        args.chain
    );

    let pool = bigname_storage::connect(&args.database).await?;
    let reth = ChainProvider::RethDb(RethDbProvider::new(&args.chain, &reth_datadir)?);
    let rpc = JsonRpcProvider::new_with_request_timeout(
        &rpc_url,
        Duration::from_secs(PROOF_SPOT_CHECK_TIMEOUT_SECS),
    )?;
    let dry_run = args.dry_run;
    let outcome = repair_raw_code_hashes(
        &pool,
        &reth,
        &rpc,
        RawCodeHashCorrectionConfig {
            chain: args.chain,
            observed_from,
            observed_before,
            page_size: args.page_size,
            write_batch_size: args.write_batch_size,
            rpc_sample_percent: args.rpc_sample_percent,
            dry_run: args.dry_run,
        },
    )
    .await?;

    info!(
        service = "indexer",
        command = "repair raw-code-hashes",
        dry_run,
        scanned_count = outcome.scanned_count,
        address_count = outcome.address_count,
        already_correct_count = outcome.already_correct_count,
        to_correct_count = outcome.to_correct_count,
        rpc_sample_count = outcome.rpc_sample_count,
        proof_spot_check_attempted_count = outcome.proof_spot_check_attempted_count,
        proof_spot_check_verified_count = outcome.proof_spot_check_verified_count,
        proof_spot_check_timed_out = outcome.proof_spot_check_timed_out,
        orphaned_skipped_count = outcome.orphaned_skipped_count,
        corrected_count = outcome.corrected_count,
        already_correct_during_write_count = outcome.already_correct_during_write_count,
        "raw code-hash correction completed"
    );

    Ok(())
}

async fn repair_raw_code_hashes(
    pool: &PgPool,
    reth: &(impl ChainProviderOps + ?Sized),
    rpc: &JsonRpcProvider,
    config: RawCodeHashCorrectionConfig,
) -> Result<RawCodeHashCorrectionOutcome> {
    validate_config(&config)?;

    let selected_count = count_raw_code_hash_correction_candidates(
        pool,
        &config.chain,
        config.observed_from,
        config.observed_before,
    )
    .await?;
    let orphaned_skipped_count = count_raw_code_hash_correction_orphaned_skips(
        pool,
        &config.chain,
        config.observed_from,
        config.observed_before,
    )
    .await?;
    let variants_by_address = load_raw_code_hash_address_variants(
        pool,
        &config.chain,
        config.observed_from,
        config.observed_before,
    )
    .await?;
    let mut accumulator =
        ClassificationAccumulator::new(&variants_by_address, config.rpc_sample_percent)?;
    classify_rows(pool, reth, &config, &mut accumulator).await?;
    let classification = accumulator.finish();

    ensure!(
        classification.scanned_count == selected_count,
        "raw code-hash correction scanned {} rows but storage counted {selected_count}",
        classification.scanned_count
    );
    let update_count = i64::try_from(classification.updates.len())
        .context("raw code-hash correction update count overflowed i64")?;
    ensure!(
        classification.to_correct_count == update_count,
        "raw code-hash correction classified {} rows to correct but retained {update_count} verified updates",
        classification.to_correct_count
    );

    log_census(&config, &classification, orphaned_skipped_count);
    verify_rpc_code_sample(rpc, &classification.samples).await?;
    let proof_spot_check =
        verify_rpc_proof_spot_check(rpc, &proof_spot_check_samples(&classification.updates))
            .await?;
    if classification.unexpected_variant_count > 0 {
        bail!(
            "raw code-hash correction found {} rows whose re-derived hash falls outside the stored address variant family after RPC verification: {:?}",
            classification.unexpected_variant_count,
            classification.unexpected_variant_examples
        );
    }

    let mut outcome = RawCodeHashCorrectionOutcome {
        scanned_count: classification.scanned_count,
        address_count: i64::try_from(classification.address_census.len())
            .context("address census count overflowed i64")?,
        already_correct_count: classification.already_correct_count,
        to_correct_count: classification.to_correct_count,
        orphaned_skipped_count,
        rpc_sample_count: i64::try_from(classification.samples.len())
            .context("RPC sample count overflowed i64")?,
        proof_spot_check_attempted_count: i64::try_from(proof_spot_check.attempted_count)
            .context("proof spot-check attempted count overflowed i64")?,
        proof_spot_check_verified_count: i64::try_from(proof_spot_check.verified_count)
            .context("proof spot-check verified count overflowed i64")?,
        proof_spot_check_timed_out: proof_spot_check.timed_out,
        corrected_count: 0,
        already_correct_during_write_count: 0,
    };

    if config.dry_run {
        return Ok(outcome);
    }

    apply_corrections(pool, &config, &classification.updates, &mut outcome).await?;
    Ok(outcome)
}

async fn classify_rows(
    pool: &PgPool,
    reth: &(impl ChainProviderOps + ?Sized),
    config: &RawCodeHashCorrectionConfig,
    accumulator: &mut ClassificationAccumulator<'_>,
) -> Result<()> {
    let mut after_raw_code_hash_id = 0_i64;
    loop {
        let rows = load_raw_code_hash_correction_page(
            pool,
            &config.chain,
            config.observed_from,
            config.observed_before,
            after_raw_code_hash_id,
            config.page_size,
        )
        .await?;
        if rows.is_empty() {
            break;
        }
        after_raw_code_hash_id = rows
            .last()
            .map(|row| row.raw_code_hash_id)
            .unwrap_or(after_raw_code_hash_id);

        let derived = derive_code_hashes(reth, &rows).await?;
        for row in &rows {
            let key = (row.block_hash.clone(), row.contract_address.clone());
            let derived = derived.get(&key).with_context(|| {
                format!(
                    "Reth DB omitted re-derived code hash for {} at {}",
                    row.contract_address, row.block_hash
                )
            })?;
            accumulator.observe(row, derived)?;
        }
    }
    Ok(())
}

fn proof_spot_check_samples(updates: &[VerifiedCorrectionUpdate]) -> Vec<CorrectionSampleRow> {
    let mut latest_by_address = BTreeMap::<String, &VerifiedCorrectionUpdate>::new();
    for update in updates {
        latest_by_address
            .entry(update.contract_address.clone())
            .and_modify(|current| {
                if update.block_number > current.block_number {
                    *current = update;
                }
            })
            .or_insert(update);
    }

    latest_by_address
        .into_values()
        .map(|update| CorrectionSampleRow {
            raw_code_hash_id: update.update.raw_code_hash_id,
            block_hash: update.block_hash.clone(),
            block_number: update.block_number,
            contract_address: update.contract_address.clone(),
            rederived_code_hash: update.update.corrected_code_hash.clone(),
            rederived_code_byte_length: update.update.corrected_code_byte_length,
        })
        .collect()
}

async fn apply_corrections(
    pool: &PgPool,
    config: &RawCodeHashCorrectionConfig,
    verified_updates: &[VerifiedCorrectionUpdate],
    outcome: &mut RawCodeHashCorrectionOutcome,
) -> Result<()> {
    let mut batch_index = 0_i64;
    for chunk in verified_updates.chunks(config.write_batch_size) {
        let min_block = chunk
            .iter()
            .map(|verified| verified.block_number)
            .min()
            .unwrap_or(0);
        let max_block = chunk
            .iter()
            .map(|verified| verified.block_number)
            .max()
            .unwrap_or(0);
        let updates = chunk
            .iter()
            .map(|verified| verified.update.clone())
            .collect::<Vec<_>>();
        let batch = apply_raw_code_hash_corrections(pool, &updates).await?;
        let orphaned_skipped_count = 0_i64;
        ensure_batch_accounted(&batch, orphaned_skipped_count)?;
        outcome.corrected_count += batch.corrected_count;
        outcome.already_correct_during_write_count += batch.already_correct_count;
        batch_index += 1;
        info!(
            service = "indexer",
            command = "repair raw-code-hashes",
            correction_event = "2026-07-03 raw code-hash padding correction",
            ratification = "maintainer option (a) re-derive-and-rewrite",
            batch_index,
            requested_count = batch.requested_count,
            corrected_count = batch.corrected_count,
            already_correct_count = batch.already_correct_count,
            conflicting_count = batch.conflicting_count,
            orphaned_skipped_count,
            min_block,
            max_block,
            "raw code-hash correction batch completed"
        );
    }
    Ok(())
}

fn ensure_batch_accounted(
    batch: &RawCodeHashCorrectionBatchOutcome,
    orphaned_skipped_count: i64,
) -> Result<()> {
    let accounted_count = batch.corrected_count
        + batch.already_correct_count
        + batch.conflicting_count
        + orphaned_skipped_count;
    ensure!(
        accounted_count == batch.requested_count,
        "raw code-hash correction batch accounting drift: requested {}, corrected {}, already-correct {}, conflicting {}, orphaned-skipped {}",
        batch.requested_count,
        batch.corrected_count,
        batch.already_correct_count,
        batch.conflicting_count,
        orphaned_skipped_count
    );
    Ok(())
}

fn log_census(
    config: &RawCodeHashCorrectionConfig,
    classification: &ClassificationOutcome,
    orphaned_skipped_count: i64,
) {
    info!(
        service = "indexer",
        command = "repair raw-code-hashes",
        dry_run = config.dry_run,
        chain = %config.chain,
        scanned_count = classification.scanned_count,
        address_count = classification.address_census.len(),
        already_correct_count = classification.already_correct_count,
        to_correct_count = classification.to_correct_count,
        orphaned_skipped_count,
        rpc_sample_count = classification.samples.len(),
        "raw code-hash correction census completed"
    );

    for (address, census) in &classification.address_census {
        info!(
            service = "indexer",
            command = "repair raw-code-hashes",
            dry_run = config.dry_run,
            address = %address,
            scanned_count = census.scanned_count,
            already_correct_count = census.already_correct_count,
            to_correct_count = census.to_correct_count,
            "raw code-hash correction address census"
        );
    }
}

fn validate_config(config: &RawCodeHashCorrectionConfig) -> Result<()> {
    if config.chain.trim().is_empty() {
        bail!("raw code-hash correction chain must not be empty");
    }
    if config.observed_from >= config.observed_before {
        bail!("raw code-hash correction observed-from must be before observed-before");
    }
    if config.page_size <= 0 {
        bail!(
            "raw code-hash correction page size must be positive, got {}",
            config.page_size
        );
    }
    if config.write_batch_size == 0 {
        bail!("raw code-hash correction write batch size must be positive");
    }
    ensure!(
        (1.0..=100.0).contains(&config.rpc_sample_percent),
        "raw code-hash correction RPC sample percent must be between 1 and 100, got {}",
        config.rpc_sample_percent
    );
    Ok(())
}

fn sample_stride(rpc_sample_percent: f64) -> Result<i64> {
    ensure!(
        (1.0..=100.0).contains(&rpc_sample_percent),
        "raw code-hash correction RPC sample percent must be between 1 and 100, got {rpc_sample_percent}"
    );
    Ok((100.0 / rpc_sample_percent).floor().max(1.0) as i64)
}
