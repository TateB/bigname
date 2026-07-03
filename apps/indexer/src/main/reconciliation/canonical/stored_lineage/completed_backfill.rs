use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use bigname_storage::ChainLineageBlock;
use serde_json::Value;
use sqlx::Row;

mod basenames_scan_all;
mod generic_scan;
mod source_identity;

use generic_scan::{GenericScanWatchedTargets, load_generic_scan_watched_targets};
use source_identity::{CoverageSourceIdentity, coverage_targets_for_source_identity};

#[derive(Clone, Debug)]
pub(super) struct CompletedBackfillCoverageEvidence {
    ranges: Vec<CompletedBackfillCoverage>,
    generic_scan_watched_targets: GenericScanWatchedTargets,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct CoverageRequirement {
    pub(super) source_family: Option<String>,
    pub(super) address: String,
    pub(super) effective_from_block: i64,
    pub(super) effective_to_block: i64,
}

impl CoverageRequirement {
    fn is_active_at(&self, block_number: i64) -> bool {
        self.effective_from_block <= block_number && block_number <= self.effective_to_block
    }
}

impl CompletedBackfillCoverageEvidence {
    pub(super) fn empty() -> Self {
        Self {
            ranges: Vec::new(),
            generic_scan_watched_targets: GenericScanWatchedTargets::default(),
        }
    }

    pub(super) fn covers_block(
        &self,
        block_number: i64,
        requirements: &[CoverageRequirement],
    ) -> bool {
        self.ranges.iter().any(|coverage| {
            coverage.covers_block(
                block_number,
                requirements,
                &self.generic_scan_watched_targets,
            )
        })
    }
}

#[derive(Clone, Debug)]
struct CompletedBackfillCoverage {
    start_block: i64,
    end_block: i64,
    intervals_by_family_address: BTreeMap<(String, String), Vec<(i64, i64)>>,
    generic_scan_source_families: BTreeSet<String>,
}

impl CompletedBackfillCoverage {
    fn new(start_block: i64, end_block: i64, source_identity: CoverageSourceIdentity) -> Self {
        let mut intervals_by_family_address = BTreeMap::<(String, String), Vec<(i64, i64)>>::new();
        for target in source_identity.targets {
            intervals_by_family_address
                .entry((target.source_family, target.address))
                .or_default()
                .push((target.effective_from_block, target.effective_to_block));
        }
        for intervals in intervals_by_family_address.values_mut() {
            intervals.sort_unstable();
        }
        Self {
            start_block,
            end_block,
            intervals_by_family_address,
            generic_scan_source_families: source_identity.generic_scan_source_families,
        }
    }

    fn covers_block(
        &self,
        block_number: i64,
        requirements: &[CoverageRequirement],
        generic_scan_watched_targets: &GenericScanWatchedTargets,
    ) -> bool {
        if block_number < self.start_block || self.end_block < block_number {
            return false;
        }
        requirements
            .iter()
            .filter(|requirement| requirement.is_active_at(block_number))
            .all(|requirement| {
                let Some(source_family) = &requirement.source_family else {
                    return false;
                };
                let key = (source_family.clone(), requirement.address.clone());
                if self
                    .intervals_by_family_address
                    .get(&key)
                    .is_some_and(|intervals| {
                        intervals.iter().any(|(from_block, to_block)| {
                            *from_block <= block_number && block_number <= *to_block
                        })
                    })
                {
                    return true;
                }
                if self.intervals_by_family_address.contains_key(&key) {
                    return false;
                }
                self.generic_scan_source_families.contains(source_family)
                    && generic_scan_watched_targets.address_is_active_for_family(
                        source_family,
                        &requirement.address,
                        block_number,
                    )
            })
    }
}

pub(super) async fn completed_backfill_range_coverage(
    pool: &sqlx::PgPool,
    chain: &str,
    path: &[ChainLineageBlock],
    requirements: &[CoverageRequirement],
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
        if requirements.is_empty() {
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
        load_generic_scan_watched_targets(pool, chain, requirements, &coverage).await?;
    Ok(CompletedBackfillCoverageEvidence {
        ranges: coverage,
        generic_scan_watched_targets,
    })
}
