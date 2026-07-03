use std::collections::BTreeSet;

use alloy_primitives::keccak256;
use anyhow::Result;
use bigname_manifests::{
    WatchedBackfillTarget, WatchedSourceSelector, WatchedTargetIdentity,
    load_watched_source_selector_plan,
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    backfill::backfill_job_source_identity_payload,
    ens_v1_resolver::SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
};

use super::basenames_scan_all::{
    FORMAT_BASENAMES_REGISTRY_SCAN_ALL_EVENT_SIGNATURES, SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
    basenames_registry_scan_all_identity_matches_plan,
};

const FORMAT_GENERIC_RESOLVER_EVENT_TOPICS: &str = "generic_resolver_event_topics_v1";
const FORMAT_SELECTED_TARGETS_DIGEST: &str = "selected_targets_digest_v1";
const FORMAT_SELECTED_TARGETS_WITH_GENERIC_TOPIC_SCANS: &str =
    "selected_targets_with_generic_topic_scans_v1";
const FORMAT_SELECTED_TARGETS_DIGEST_WITH_GENERIC_TOPIC_SCANS: &str =
    "selected_targets_digest_with_generic_topic_scans_v1";

#[derive(Clone, Debug)]
pub(super) struct CoverageSourceIdentity {
    pub(super) targets: Vec<CoverageTarget>,
    pub(super) generic_scan_source_families: BTreeSet<String>,
}

#[derive(Clone, Debug)]
pub(super) struct CoverageTarget {
    pub(super) source_family: String,
    pub(super) address: String,
    pub(super) effective_from_block: i64,
    pub(super) effective_to_block: i64,
}

pub(super) async fn coverage_targets_for_source_identity(
    pool: &sqlx::PgPool,
    chain: &str,
    source_identity: &Value,
    job_start_block_number: i64,
    job_end_block_number: i64,
) -> Result<Option<CoverageSourceIdentity>> {
    let generic_scan_source_families = generic_scan_source_families(source_identity);
    if !generic_scan_source_families.is_empty() {
        return generic_scan_coverage_targets_for_source_identity(
            pool,
            chain,
            source_identity,
            job_start_block_number,
            job_end_block_number,
            generic_scan_source_families,
        )
        .await;
    }

    if let Some(targets) = full_source_identity_targets(source_identity) {
        return Ok(Some(CoverageSourceIdentity {
            targets,
            generic_scan_source_families,
        }));
    }
    if !source_identity_uses_compact_targets(source_identity) {
        return Ok(None);
    }

    let Some(source_plan) = source_plan_for_source_identity(
        pool,
        chain,
        source_identity,
        job_start_block_number,
        job_end_block_number,
    )
    .await?
    else {
        return Ok(None);
    };
    if !compact_source_identity_matches_plan(source_identity, &source_plan, &BTreeSet::new(), None)?
    {
        return Ok(None);
    }

    Ok(Some(CoverageSourceIdentity {
        targets: compact_identity_selected_targets(&source_plan, &BTreeSet::new())
            .iter()
            .map(coverage_target_from_watched)
            .collect(),
        generic_scan_source_families,
    }))
}

async fn generic_scan_coverage_targets_for_source_identity(
    pool: &sqlx::PgPool,
    chain: &str,
    source_identity: &Value,
    job_start_block_number: i64,
    job_end_block_number: i64,
    generic_scan_source_families: BTreeSet<String>,
) -> Result<Option<CoverageSourceIdentity>> {
    let Some(source_plan) = source_plan_for_source_identity(
        pool,
        chain,
        source_identity,
        job_start_block_number,
        job_end_block_number,
    )
    .await?
    else {
        return Ok(None);
    };
    if source_identity
        .get("source_identity_payload_format")
        .and_then(Value::as_str)
        == Some(FORMAT_BASENAMES_REGISTRY_SCAN_ALL_EVENT_SIGNATURES)
    {
        if !basenames_registry_scan_all_identity_matches_plan(
            pool,
            chain,
            source_identity,
            &source_plan,
        )
        .await?
        {
            return Ok(None);
        }
        return Ok(Some(CoverageSourceIdentity {
            targets: Vec::new(),
            generic_scan_source_families,
        }));
    }

    let expected_source_identity = backfill_job_source_identity_payload(&source_plan)?;
    if !generic_source_identity_matches_expected(
        source_identity,
        &expected_source_identity,
        &source_plan,
        &generic_scan_source_families,
    )? {
        return Ok(None);
    }

    let targets = if source_identity_uses_compact_targets(source_identity) {
        compact_identity_selected_targets(&source_plan, &generic_scan_source_families)
            .iter()
            .map(coverage_target_from_watched)
            .collect()
    } else {
        full_source_identity_targets(source_identity).unwrap_or_default()
    };
    Ok(Some(CoverageSourceIdentity {
        targets,
        generic_scan_source_families,
    }))
}

async fn source_plan_for_source_identity(
    pool: &sqlx::PgPool,
    chain: &str,
    source_identity: &Value,
    job_start_block_number: i64,
    job_end_block_number: i64,
) -> Result<Option<bigname_manifests::WatchedSourceSelectorPlan>> {
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
    Ok(Some(source_plan))
}

fn generic_source_identity_matches_expected(
    source_identity: &Value,
    expected_source_identity: &Value,
    source_plan: &bigname_manifests::WatchedSourceSelectorPlan,
    actual_generic_scan_source_families: &BTreeSet<String>,
) -> Result<bool> {
    if actual_generic_scan_source_families
        != &generic_scan_source_families(expected_source_identity)
    {
        return Ok(false);
    }
    if source_identity.get("source_identity_hash")
        != expected_source_identity.get("source_identity_hash")
        || source_identity.get("selector_kind") != expected_source_identity.get("selector_kind")
        || source_identity.get("source_family") != expected_source_identity.get("source_family")
        || source_identity.get("requested_watched_targets")
            != expected_source_identity.get("requested_watched_targets")
        || source_identity.get("source_identity_payload_format")
            != expected_source_identity.get("source_identity_payload_format")
        || source_identity.get("generic_topic_scans")
            != expected_source_identity.get("generic_topic_scans")
    {
        return Ok(false);
    }

    match source_identity
        .get("source_identity_payload_format")
        .and_then(Value::as_str)
    {
        Some(FORMAT_BASENAMES_REGISTRY_SCAN_ALL_EVENT_SIGNATURES) => Ok(false),
        Some(FORMAT_GENERIC_RESOLVER_EVENT_TOPICS) => {
            Ok(source_identity.get("selected_targets").is_none())
        }
        Some(FORMAT_SELECTED_TARGETS_WITH_GENERIC_TOPIC_SCANS) => Ok(source_identity
            .get("selected_targets")
            == expected_source_identity.get("selected_targets")),
        Some(FORMAT_SELECTED_TARGETS_DIGEST_WITH_GENERIC_TOPIC_SCANS) => {
            let expected_hash = expected_source_identity
                .get("source_identity_hash")
                .and_then(Value::as_str);
            compact_source_identity_matches_plan(
                source_identity,
                source_plan,
                actual_generic_scan_source_families,
                expected_hash,
            )
        }
        _ => Ok(false),
    }
}

fn source_identity_uses_compact_targets(source_identity: &Value) -> bool {
    matches!(
        source_identity
            .get("source_identity_payload_format")
            .and_then(Value::as_str),
        Some(FORMAT_SELECTED_TARGETS_DIGEST)
            | Some(FORMAT_SELECTED_TARGETS_DIGEST_WITH_GENERIC_TOPIC_SCANS)
    )
}

fn compact_source_identity_matches_plan(
    source_identity: &Value,
    source_plan: &bigname_manifests::WatchedSourceSelectorPlan,
    generic_scan_source_families: &BTreeSet<String>,
    expected_source_identity_hash: Option<&str>,
) -> Result<bool> {
    let selected_targets =
        compact_identity_selected_targets(source_plan, generic_scan_source_families);
    if let Some(expected_hash) = expected_source_identity_hash
        && source_identity
            .get("source_identity_hash")
            .and_then(Value::as_str)
            != Some(expected_hash)
    {
        return Ok(false);
    }
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
            != Some(selected_targets.len() as u64)
        || source_identity
            .get("selected_targets_digest_algorithm")
            .and_then(Value::as_str)
            != Some("keccak256")
    {
        return Ok(false);
    }

    let canonical_digest = canonical_selected_targets_digest(&selected_targets)?;
    let direct_serde_digest = direct_serde_selected_targets_digest(&selected_targets)?;
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
            "first": selected_targets.first(),
            "last": selected_targets.last(),
        });
        if sample != &expected {
            return Ok(false);
        }
    }

    Ok(true)
}

fn compact_identity_selected_targets(
    source_plan: &bigname_manifests::WatchedSourceSelectorPlan,
    generic_scan_source_families: &BTreeSet<String>,
) -> Vec<WatchedBackfillTarget> {
    source_plan
        .selected_targets
        .iter()
        .filter(|target| !generic_scan_source_families.contains(&target.source_family))
        .cloned()
        .collect()
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

fn generic_scan_source_families(source_identity: &Value) -> BTreeSet<String> {
    let mut source_families = BTreeSet::new();
    if source_identity
        .get("source_identity_payload_format")
        .and_then(Value::as_str)
        == Some(FORMAT_BASENAMES_REGISTRY_SCAN_ALL_EVENT_SIGNATURES)
        && source_identity.get("source_family").and_then(Value::as_str)
            == Some(SOURCE_FAMILY_BASENAMES_BASE_REGISTRY)
    {
        source_families.insert(SOURCE_FAMILY_BASENAMES_BASE_REGISTRY.to_owned());
    }

    if source_identity
        .get("source_identity_payload_format")
        .and_then(Value::as_str)
        == Some(FORMAT_GENERIC_RESOLVER_EVENT_TOPICS)
        && source_identity.get("source_family").and_then(Value::as_str)
            == Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    {
        source_families.insert(SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned());
    }

    if let Some(scans) = source_identity
        .get("generic_topic_scans")
        .and_then(Value::as_array)
    {
        for scan in scans {
            if scan
                .get("source_identity_payload_format")
                .and_then(Value::as_str)
                == Some(FORMAT_GENERIC_RESOLVER_EVENT_TOPICS)
                && scan.get("source_family").and_then(Value::as_str)
                    == Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
            {
                source_families.insert(SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned());
            }
        }
    }

    source_families
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
        source_family: target.get("source_family")?.as_str()?.to_owned(),
        address: target.get("address")?.as_str()?.to_ascii_lowercase(),
        effective_from_block: target.get("effective_from_block")?.as_i64()?,
        effective_to_block: target.get("effective_to_block")?.as_i64()?,
    })
}

fn coverage_target_from_watched(target: &WatchedBackfillTarget) -> CoverageTarget {
    CoverageTarget {
        source_family: target.source_family.clone(),
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
            &source_plan,
            &BTreeSet::new(),
            None
        )?);
        Ok(())
    }

    #[test]
    fn compact_source_identity_matches_generic_topic_scan_producer_digest() -> Result<()> {
        let source_plan = source_plan_with_compact_generic_topic_scan();
        let payload = crate::backfill::backfill_job_source_identity_payload(&source_plan)?;
        assert_eq!(
            payload
                .get("source_identity_payload_format")
                .and_then(Value::as_str),
            Some(FORMAT_SELECTED_TARGETS_DIGEST_WITH_GENERIC_TOPIC_SCANS)
        );
        let generic_scan_source_families = generic_scan_source_families(&payload);
        assert_eq!(
            generic_scan_source_families,
            BTreeSet::from([SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned()])
        );
        assert!(generic_source_identity_matches_expected(
            &payload,
            &payload,
            &source_plan,
            &generic_scan_source_families
        )?);
        Ok(())
    }

    #[test]
    fn generic_source_identity_rejects_mismatched_generic_declaration() -> Result<()> {
        let source_plan = source_plan_with_compact_generic_topic_scan();
        let expected = crate::backfill::backfill_job_source_identity_payload(&source_plan)?;
        let mut drifted = expected.clone();
        drifted["generic_topic_scans"][0]["unexpected"] = json!(true);
        let generic_scan_source_families =
            BTreeSet::from([SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned()]);

        assert!(!generic_source_identity_matches_expected(
            &drifted,
            &expected,
            &source_plan,
            &generic_scan_source_families
        )?);
        Ok(())
    }

    #[test]
    fn generic_source_identity_rejects_mismatched_requested_targets() -> Result<()> {
        let mut source_plan = source_plan_with_compact_generic_topic_scan();
        source_plan.selector_kind = WatchedSourceSelectorKind::WatchedTargetSet;
        source_plan.requested_watched_targets = vec![WatchedTargetIdentity {
            contract_instance_id: Uuid::from_u128(99_999),
        }];
        let expected = crate::backfill::backfill_job_source_identity_payload(&source_plan)?;
        let mut drifted = expected.clone();
        drifted["requested_watched_targets"] = json!([{
            "contract_instance_id": Uuid::from_u128(88_888),
        }]);
        let generic_scan_source_families = generic_scan_source_families(&expected);

        assert!(!generic_source_identity_matches_expected(
            &drifted,
            &expected,
            &source_plan,
            &generic_scan_source_families
        )?);
        Ok(())
    }

    fn source_plan_with_compact_generic_topic_scan() -> WatchedSourceSelectorPlan {
        let mut selected_targets = (0
            ..=crate::backfill::COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD)
            .map(|index| WatchedBackfillTarget {
                source_family: "test_source_family".to_owned(),
                contract_instance_id: Uuid::from_u128(index as u128 + 1),
                address: format!("0x{index:040x}"),
                effective_from_block: index as i64,
                effective_to_block: index as i64 + 10,
            })
            .collect::<Vec<_>>();
        selected_targets.push(WatchedBackfillTarget {
            source_family: SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
            contract_instance_id: Uuid::from_u128(99_999),
            address: "0x00000000000000000000000000000000000000aa".to_owned(),
            effective_from_block: 1,
            effective_to_block: 20,
        });
        WatchedSourceSelectorPlan {
            chain: "ethereum-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::WholeActiveWatchedChain,
            source_family: None,
            requested_watched_targets: Vec::new(),
            selected_targets,
            watched_chain_plan: WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        }
    }
}
