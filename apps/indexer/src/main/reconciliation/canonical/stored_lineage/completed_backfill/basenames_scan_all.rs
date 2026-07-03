use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::keccak256;
use anyhow::{Context, Result};
use bigname_manifests::load_active_manifest_abi_events_by_chain_and_source_families;
use serde::Serialize;
use serde_json::Value;

use crate::backfill::BackfillTopicPlan;

pub(super) const FORMAT_BASENAMES_REGISTRY_SCAN_ALL_EVENT_SIGNATURES: &str =
    "basenames_registry_scan_all_event_signatures_v1";
pub(super) const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";

const COINBASE_SQL_HASH_PINNED_SCAN_MODE: &str = "coinbase_sql_hash_pinned_logs_v1";
const COINBASE_CDP_SQL_PROVIDER: &str = "coinbase_cdp_sql";

pub(super) async fn basenames_registry_scan_all_identity_matches_plan(
    pool: &sqlx::PgPool,
    chain: &str,
    source_identity: &Value,
    source_plan: &bigname_manifests::WatchedSourceSelectorPlan,
) -> Result<bool> {
    if source_plan.selector_kind != bigname_manifests::WatchedSourceSelectorKind::SourceFamily
        || source_plan.source_family.as_deref() != Some(SOURCE_FAMILY_BASENAMES_BASE_REGISTRY)
        || source_identity.get("selector_kind").and_then(Value::as_str) != Some("source_family")
        || source_identity.get("source_family").and_then(Value::as_str)
            != Some(SOURCE_FAMILY_BASENAMES_BASE_REGISTRY)
        || source_identity.get("requested_watched_targets")
            != Some(&serde_json::to_value(
                &source_plan.requested_watched_targets,
            )?)
        || source_identity
            .get("backfill_provider")
            .and_then(Value::as_str)
            != Some(COINBASE_CDP_SQL_PROVIDER)
        || source_identity.get("scan_mode").and_then(Value::as_str)
            != Some(COINBASE_SQL_HASH_PINNED_SCAN_MODE)
    {
        return Ok(false);
    }

    if !coinbase_sql_source_identity_hash_matches(source_identity)? {
        return Ok(false);
    }

    let Some(topic_plan) = source_identity.get("coinbase_sql_topic_plan") else {
        return Ok(false);
    };
    let expected_topic_plan = current_basenames_registry_topic_plan_payload(pool, chain).await?;
    Ok(topic_plan == &expected_topic_plan)
}

async fn current_basenames_registry_topic_plan_payload(
    pool: &sqlx::PgPool,
    chain: &str,
) -> Result<Value> {
    let source_family = SOURCE_FAMILY_BASENAMES_BASE_REGISTRY.to_owned();
    let events = load_active_manifest_abi_events_by_chain_and_source_families(
        pool,
        chain,
        std::slice::from_ref(&source_family),
    )
    .await
    .context("failed to load active Basenames registry ABI topics for scan-all coverage")?;
    let mut topic0s_by_source_family = BTreeMap::<String, BTreeSet<String>>::new();
    let mut event_signatures_by_source_family = BTreeMap::<String, BTreeSet<String>>::new();
    for event in events {
        let Some(topic0) = event.topic0 else {
            continue;
        };
        topic0s_by_source_family
            .entry(event.source_family.clone())
            .or_default()
            .insert(topic0.to_ascii_lowercase());
        event_signatures_by_source_family
            .entry(event.source_family)
            .or_default()
            .insert(event.canonical_signature);
    }
    let topic0s_by_source_family = topic0s_by_source_family
        .into_iter()
        .map(|(source_family, topics)| (source_family, topics.into_iter().collect()))
        .collect();
    let event_signatures_by_source_family = event_signatures_by_source_family
        .into_iter()
        .map(|(source_family, signatures)| (source_family, signatures.into_iter().collect()))
        .collect();
    BackfillTopicPlan::new(
        topic0s_by_source_family,
        event_signatures_by_source_family,
        BTreeSet::new(),
    )
    .source_identity_payload()
}

fn coinbase_sql_source_identity_hash_matches(source_identity: &Value) -> Result<bool> {
    let Some(actual_hash) = source_identity
        .get("source_identity_hash")
        .and_then(Value::as_str)
    else {
        return Ok(false);
    };
    let mut payload = source_identity.clone();
    let Some(object) = payload.as_object_mut() else {
        return Ok(false);
    };
    object.remove("source_identity_hash");
    let expected_hash = keccak256_json_digest(&payload)
        .context("failed to digest stored Coinbase SQL source identity")?;
    Ok(actual_hash == expected_hash)
}

fn keccak256_json_digest<T>(value: &T) -> Result<String>
where
    T: Serialize + ?Sized,
{
    let payload = serde_json::to_vec(value).context("failed to serialize JSON digest input")?;
    Ok(format!("keccak256:{}", keccak256(payload)))
}
