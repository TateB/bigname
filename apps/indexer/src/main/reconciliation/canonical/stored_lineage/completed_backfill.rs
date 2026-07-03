use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use bigname_storage::ChainLineageBlock;
use serde_json::Value;
use sqlx::Row;

mod generic_scan;
mod source_identity;

use generic_scan::{GenericScanWatchedTargets, load_generic_scan_watched_targets};
use source_identity::{CoverageSourceIdentity, coverage_targets_for_source_identity};

#[derive(Clone, Debug)]
pub(super) struct CompletedBackfillCoverageEvidence {
    ranges: Vec<CompletedBackfillCoverage>,
    generic_scan_watched_targets: GenericScanWatchedTargets,
}

impl CompletedBackfillCoverageEvidence {
    pub(super) fn empty() -> Self {
        Self {
            ranges: Vec::new(),
            generic_scan_watched_targets: GenericScanWatchedTargets::default(),
        }
    }

    pub(super) fn covers_block(&self, block_number: i64, selected_addresses: &[String]) -> bool {
        self.ranges.iter().any(|coverage| {
            coverage.covers_block(
                block_number,
                selected_addresses,
                &self.generic_scan_watched_targets,
            )
        })
    }
}

#[derive(Clone, Debug)]
struct CompletedBackfillCoverage {
    start_block: i64,
    end_block: i64,
    intervals_by_address: BTreeMap<String, Vec<(i64, i64)>>,
    generic_scan_source_families: BTreeSet<String>,
}

impl CompletedBackfillCoverage {
    fn new(start_block: i64, end_block: i64, source_identity: CoverageSourceIdentity) -> Self {
        let mut intervals_by_address = BTreeMap::<String, Vec<(i64, i64)>>::new();
        for target in source_identity.targets {
            intervals_by_address
                .entry(target.address)
                .or_default()
                .push((target.effective_from_block, target.effective_to_block));
        }
        for intervals in intervals_by_address.values_mut() {
            intervals.sort_unstable();
        }
        Self {
            start_block,
            end_block,
            intervals_by_address,
            generic_scan_source_families: source_identity.generic_scan_source_families,
        }
    }

    fn covers_block(
        &self,
        block_number: i64,
        selected_addresses: &[String],
        generic_scan_watched_targets: &GenericScanWatchedTargets,
    ) -> bool {
        if block_number < self.start_block || self.end_block < block_number {
            return false;
        }
        selected_addresses.iter().all(|address| {
            if self
                .intervals_by_address
                .get(address)
                .is_some_and(|intervals| {
                    intervals.iter().any(|(from_block, to_block)| {
                        *from_block <= block_number && block_number <= *to_block
                    })
                })
            {
                return true;
            }
            if self.intervals_by_address.contains_key(address) {
                return false;
            }
            self.generic_scan_source_families
                .iter()
                .any(|source_family| {
                    generic_scan_watched_targets.address_is_active_for_family(
                        source_family,
                        address,
                        block_number,
                    )
                })
        })
    }
}

pub(super) async fn completed_backfill_range_coverage(
    pool: &sqlx::PgPool,
    chain: &str,
    path: &[ChainLineageBlock],
    selected_addresses: &[String],
) -> Result<CompletedBackfillCoverageEvidence> {
    let Some(first) = path.first() else {
        return Ok(CompletedBackfillCoverageEvidence::empty());
    };
    let Some(last) = path.last() else {
        return Ok(CompletedBackfillCoverageEvidence::empty());
    };
    let rows = sqlx::query(
        r#"
        SELECT
            br.range_start_block_number,
            br.range_end_block_number,
            bj.range_start_block_number AS job_start_block_number,
            bj.range_end_block_number AS job_end_block_number,
            bj.source_identity
        FROM backfill_ranges br
        JOIN backfill_jobs bj
          ON bj.backfill_job_id = br.backfill_job_id
        WHERE bj.chain_id = $1
          AND bj.status = 'completed'::backfill_lifecycle_status
          AND br.status = 'completed'::backfill_lifecycle_status
          AND br.checkpoint_block_number = br.range_end_block_number
          AND br.range_start_block_number <= $3
          AND br.range_end_block_number >= $2
        ORDER BY br.range_start_block_number, br.range_end_block_number
        "#,
    )
    .bind(chain)
    .bind(first.block_number)
    .bind(last.block_number)
    .fetch_all(pool)
    .await?;

    let mut coverage = Vec::new();
    for row in rows {
        let range_start_block_number = row.try_get("range_start_block_number")?;
        let range_end_block_number = row.try_get("range_end_block_number")?;
        if selected_addresses.is_empty() {
            coverage.push(CompletedBackfillCoverage::new(
                range_start_block_number,
                range_end_block_number,
                CoverageSourceIdentity {
                    targets: Vec::new(),
                    generic_scan_source_families: BTreeSet::new(),
                },
            ));
            continue;
        }

        let source_identity: Value = row.try_get("source_identity")?;
        let source_identity = coverage_targets_for_source_identity(
            pool,
            chain,
            &source_identity,
            row.try_get("job_start_block_number")?,
            row.try_get("job_end_block_number")?,
        )
        .await?;
        let Some(source_identity) = source_identity else {
            continue;
        };
        coverage.push(CompletedBackfillCoverage::new(
            range_start_block_number,
            range_end_block_number,
            source_identity,
        ));
    }

    let generic_scan_watched_targets =
        load_generic_scan_watched_targets(pool, chain, selected_addresses, &coverage).await?;
    Ok(CompletedBackfillCoverageEvidence {
        ranges: coverage,
        generic_scan_watched_targets,
    })
}
