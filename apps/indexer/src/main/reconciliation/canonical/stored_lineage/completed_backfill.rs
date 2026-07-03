use std::collections::BTreeMap;

use alloy_primitives::keccak256;
use anyhow::Result;
use bigname_manifests::{
    WatchedBackfillTarget, WatchedSourceSelector, WatchedTargetIdentity,
    load_watched_source_selector_plan,
};
use bigname_storage::ChainLineageBlock;
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub(super) struct CompletedBackfillCoverage {
    start_block: i64,
    end_block: i64,
    intervals_by_address: BTreeMap<String, Vec<(i64, i64)>>,
}

impl CompletedBackfillCoverage {
    fn new(start_block: i64, end_block: i64, targets: Vec<CoverageTarget>) -> Self {
        let mut intervals_by_address = BTreeMap::<String, Vec<(i64, i64)>>::new();
        for target in targets {
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
        }
    }

    pub(super) fn covers_block(&self, block_number: i64, selected_addresses: &[String]) -> bool {
        if block_number < self.start_block || self.end_block < block_number {
            return false;
        }
        selected_addresses.iter().all(|address| {
            self.intervals_by_address
                .get(address)
                .is_some_and(|intervals| {
                    intervals.iter().any(|(from_block, to_block)| {
                        *from_block <= block_number && block_number <= *to_block
                    })
                })
        })
    }
}

#[derive(Clone, Debug)]
struct CoverageTarget {
    address: String,
    effective_from_block: i64,
    effective_to_block: i64,
}

pub(super) async fn completed_backfill_range_coverage(
    pool: &sqlx::PgPool,
    chain: &str,
    path: &[ChainLineageBlock],
    selected_addresses: &[String],
) -> Result<Vec<CompletedBackfillCoverage>> {
    let Some(first) = path.first() else {
        return Ok(Vec::new());
    };
    let Some(last) = path.last() else {
        return Ok(Vec::new());
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
                Vec::new(),
            ));
            continue;
        }

        let source_identity: Value = row.try_get("source_identity")?;
        let targets = coverage_targets_for_source_identity(
            pool,
            chain,
            &source_identity,
            row.try_get("job_start_block_number")?,
            row.try_get("job_end_block_number")?,
        )
        .await?;
        let Some(targets) = targets else {
            continue;
        };
        coverage.push(CompletedBackfillCoverage::new(
            range_start_block_number,
            range_end_block_number,
            targets,
        ));
    }

    Ok(coverage)
}

async fn coverage_targets_for_source_identity(
    pool: &sqlx::PgPool,
    chain: &str,
    source_identity: &Value,
    job_start_block_number: i64,
    job_end_block_number: i64,
) -> Result<Option<Vec<CoverageTarget>>> {
    if let Some(targets) = full_source_identity_targets(source_identity) {
        return Ok(Some(targets));
    }
    if !source_identity_uses_compact_targets(source_identity) {
        return Ok(None);
    }

    let Some(selector) = selector_from_source_identity(source_identity) else {
        return Ok(None);
    };
    let Ok(source_plan) = load_watched_source_selector_plan(
        pool,
        chain,
        selector,
        job_start_block_number,
        job_end_block_number,
    )
    .await
    else {
        return Ok(None);
    };
    if !compact_source_identity_matches_plan(source_identity, &source_plan)? {
        return Ok(None);
    }

    Ok(Some(
        source_plan
            .selected_targets
            .iter()
            .map(coverage_target_from_watched)
            .collect(),
    ))
}

fn source_identity_uses_compact_targets(source_identity: &Value) -> bool {
    source_identity
        .get("source_identity_payload_format")
        .and_then(Value::as_str)
        == Some("selected_targets_digest_v1")
}

fn compact_source_identity_matches_plan(
    source_identity: &Value,
    source_plan: &bigname_manifests::WatchedSourceSelectorPlan,
) -> Result<bool> {
    if source_identity.get("selector_kind").and_then(Value::as_str)
        != Some(source_plan.selector_kind.as_str())
        || source_identity.get("source_family") != Some(&json!(&source_plan.source_family))
        || source_identity.get("requested_watched_targets")
            != Some(&serde_json::to_value(
                &source_plan.requested_watched_targets,
            )?)
        || source_identity
            .get("selected_target_count")
            .and_then(Value::as_u64)
            != Some(source_plan.selected_targets.len() as u64)
        || source_identity
            .get("selected_targets_digest_algorithm")
            .and_then(Value::as_str)
            != Some("keccak256")
    {
        return Ok(false);
    }

    let canonical_digest = canonical_selected_targets_digest(&source_plan.selected_targets)?;
    let direct_serde_digest = direct_serde_selected_targets_digest(&source_plan.selected_targets)?;
    let actual_digest = source_identity
        .get("selected_targets_digest")
        .and_then(Value::as_str);
    if actual_digest != Some(canonical_digest.as_str())
        && actual_digest != Some(direct_serde_digest.as_str())
    {
        return Ok(false);
    }
    if let Some(sample) = source_identity.get("selected_targets_sample") {
        let expected = json!({
            "first": source_plan.selected_targets.first(),
            "last": source_plan.selected_targets.last(),
        });
        if sample != &expected {
            return Ok(false);
        }
    }

    Ok(true)
}

fn canonical_selected_targets_digest(selected_targets: &[WatchedBackfillTarget]) -> Result<String> {
    let value = serde_json::to_value(selected_targets)?;
    let payload = serde_json::to_vec(&canonical_json_value(value))?;
    Ok(format!("keccak256:{}", keccak256(payload)))
}

fn direct_serde_selected_targets_digest(
    selected_targets: &[WatchedBackfillTarget],
) -> Result<String> {
    let payload = serde_json::to_vec(selected_targets)?;
    Ok(format!("keccak256:{}", keccak256(payload)))
}

fn canonical_json_value(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(canonical_json_value).collect()),
        Value::Object(fields) => {
            let mut fields = fields
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect::<Vec<_>>();
            fields.sort_by(|left, right| left.0.cmp(&right.0));

            let mut sorted = serde_json::Map::new();
            for (key, value) in fields {
                sorted.insert(key, value);
            }
            Value::Object(sorted)
        }
        value => value,
    }
}

fn selector_from_source_identity(source_identity: &Value) -> Option<WatchedSourceSelector> {
    match source_identity.get("selector_kind")?.as_str()? {
        "whole_active_watched_chain" => Some(WatchedSourceSelector::WholeActiveWatchedChain),
        "source_family" => Some(WatchedSourceSelector::SourceFamily(
            source_identity.get("source_family")?.as_str()?.to_owned(),
        )),
        "watched_target_set" => {
            let targets = source_identity
                .get("requested_watched_targets")?
                .as_array()?
                .iter()
                .map(|target| {
                    let id = target.get("contract_instance_id")?.as_str()?;
                    Some(WatchedTargetIdentity {
                        contract_instance_id: Uuid::parse_str(id).ok()?,
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(WatchedSourceSelector::WatchedTargetSet(targets))
        }
        _ => None,
    }
}

fn full_source_identity_targets(source_identity: &Value) -> Option<Vec<CoverageTarget>> {
    source_identity
        .get("selected_targets")?
        .as_array()?
        .iter()
        .map(coverage_target_from_value)
        .collect()
}

fn coverage_target_from_value(target: &Value) -> Option<CoverageTarget> {
    Some(CoverageTarget {
        address: target.get("address")?.as_str()?.to_ascii_lowercase(),
        effective_from_block: target.get("effective_from_block")?.as_i64()?,
        effective_to_block: target.get("effective_to_block")?.as_i64()?,
    })
}

fn coverage_target_from_watched(target: &WatchedBackfillTarget) -> CoverageTarget {
    CoverageTarget {
        address: target.address.to_ascii_lowercase(),
        effective_from_block: target.effective_from_block,
        effective_to_block: target.effective_to_block,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_manifests::{
        WatchedChainPlan, WatchedSourceSelectorKind, WatchedSourceSelectorPlan,
    };

    #[test]
    fn compact_source_identity_matches_source_family_producer_digest() -> Result<()> {
        let selected_targets = (0
            ..=crate::backfill::COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD)
            .map(|index| WatchedBackfillTarget {
                source_family: "test_source_family".to_owned(),
                contract_instance_id: Uuid::from_u128(index as u128 + 1),
                address: format!("0x{index:040x}"),
                effective_from_block: index as i64,
                effective_to_block: index as i64 + 10,
            })
            .collect::<Vec<_>>();
        let source_plan = WatchedSourceSelectorPlan {
            chain: "base-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::SourceFamily,
            source_family: Some("test_source_family".to_owned()),
            requested_watched_targets: Vec::new(),
            selected_targets,
            watched_chain_plan: WatchedChainPlan {
                chain: "base-mainnet".to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        };

        let payload = crate::backfill::backfill_job_source_identity_payload(&source_plan)?;
        assert_eq!(
            payload
                .get("source_identity_payload_format")
                .and_then(Value::as_str),
            Some("selected_targets_digest_v1")
        );
        assert!(compact_source_identity_matches_plan(
            &payload,
            &source_plan
        )?);
        Ok(())
    }
}
