use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use bigname_manifests::{WatchedContract, load_watched_contracts_by_source_family_and_addresses};

use super::{CompletedBackfillCoverage, source_identity::CoverageTarget};

#[derive(Clone, Debug, Default)]
pub(super) struct GenericScanWatchedTargets {
    intervals_by_family_address: BTreeMap<(String, String), Vec<(i64, i64)>>,
}

impl GenericScanWatchedTargets {
    pub(super) fn address_is_active_for_family(
        &self,
        source_family: &str,
        address: &str,
        block_number: i64,
    ) -> bool {
        self.intervals_by_family_address
            .get(&(source_family.to_owned(), address.to_owned()))
            .is_some_and(|intervals| {
                intervals.iter().any(|(from_block, to_block)| {
                    *from_block <= block_number && block_number <= *to_block
                })
            })
    }
}

pub(super) async fn load_generic_scan_watched_targets(
    pool: &sqlx::PgPool,
    chain: &str,
    selected_addresses: &[String],
    coverages: &[CompletedBackfillCoverage],
) -> Result<GenericScanWatchedTargets> {
    let generic_scan_source_families = coverages
        .iter()
        .flat_map(|coverage| coverage.generic_scan_source_families.iter().cloned())
        .collect::<BTreeSet<_>>();
    if generic_scan_source_families.is_empty() {
        return Ok(GenericScanWatchedTargets::default());
    }

    let generic_candidate_addresses = selected_addresses
        .iter()
        .filter(|address| {
            coverages.iter().any(|coverage| {
                !coverage.generic_scan_source_families.is_empty()
                    && !coverage.intervals_by_address.contains_key(*address)
            })
        })
        .map(|address| (chain.to_owned(), address.clone()))
        .collect::<Vec<_>>();
    if generic_candidate_addresses.is_empty() {
        return Ok(GenericScanWatchedTargets::default());
    }

    let mut intervals_by_family_address = BTreeMap::<(String, String), Vec<(i64, i64)>>::new();
    for source_family in generic_scan_source_families {
        let watched_contracts = load_watched_contracts_by_source_family_and_addresses(
            pool,
            &source_family,
            &generic_candidate_addresses,
        )
        .await?;
        for target in watched_contracts {
            let coverage_target = coverage_target_from_watched_contract(&target);
            intervals_by_family_address
                .entry((coverage_target.source_family, coverage_target.address))
                .or_default()
                .push((
                    coverage_target.effective_from_block,
                    coverage_target.effective_to_block,
                ));
        }
    }
    for intervals in intervals_by_family_address.values_mut() {
        intervals.sort_unstable();
    }

    Ok(GenericScanWatchedTargets {
        intervals_by_family_address,
    })
}

fn coverage_target_from_watched_contract(target: &WatchedContract) -> CoverageTarget {
    CoverageTarget {
        source_family: target.source_family.clone(),
        address: target.address.to_ascii_lowercase(),
        effective_from_block: target.active_from_block_number.unwrap_or(0),
        effective_to_block: target.active_to_block_number.unwrap_or(i64::MAX),
    }
}
