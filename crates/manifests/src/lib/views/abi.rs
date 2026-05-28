use anyhow::{Context, Result};
use serde::Deserialize;
use sqlx::{PgPool, Row};

use crate::ManifestAbi;

use super::types::ActiveManifestAbiEvent;

#[derive(Debug, Default, Deserialize)]
struct ManifestAbiPayload {
    #[serde(default)]
    abi: ManifestAbi,
}

pub async fn load_active_manifest_abi_events(
    pool: &PgPool,
    manifest_ids: &[i64],
) -> Result<Vec<ActiveManifestAbiEvent>> {
    if manifest_ids.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            manifest_id,
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            manifest_payload
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND manifest_id = ANY($1::BIGINT[])
        ORDER BY manifest_id
        "#,
    )
    .bind(manifest_ids)
    .fetch_all(pool)
    .await
    .context("failed to load active manifest ABI events")?;

    let mut events = Vec::new();
    for row in rows {
        let manifest_id = row
            .try_get("manifest_id")
            .context("failed to read ABI manifest_id")?;
        let payload = row
            .try_get("manifest_payload")
            .context("failed to read ABI manifest_payload")?;
        let manifest_version = row
            .try_get::<i64, _>("manifest_version")
            .with_context(|| format!("failed to read ABI manifest_version for {manifest_id}"))?;
        let manifest_version = u64::try_from(manifest_version)
            .with_context(|| format!("manifest_version for {manifest_id} must be non-negative"))?;
        let namespace = row
            .try_get::<String, _>("namespace")
            .with_context(|| format!("failed to read ABI namespace for {manifest_id}"))?;
        let source_family = row
            .try_get::<String, _>("source_family")
            .with_context(|| format!("failed to read ABI source_family for {manifest_id}"))?;
        let chain = row
            .try_get::<String, _>("chain")
            .with_context(|| format!("failed to read ABI chain for {manifest_id}"))?;
        let deployment_epoch = row
            .try_get::<String, _>("deployment_epoch")
            .with_context(|| format!("failed to read ABI deployment_epoch for {manifest_id}"))?;
        let payload: ManifestAbiPayload = serde_json::from_value(payload)
            .with_context(|| format!("failed to decode manifest ABI payload for {manifest_id}"))?;

        for event in &payload.abi.events {
            let parsed = event.parsed_event_view().with_context(|| {
                format!(
                    "failed to derive ABI event view for manifest_id {manifest_id} event {}",
                    event.name
                )
            })?;
            events.push(ActiveManifestAbiEvent {
                manifest_id,
                manifest_version,
                namespace: namespace.clone(),
                source_family: source_family.clone(),
                chain: chain.clone(),
                deployment_epoch: deployment_epoch.clone(),
                name: event.name.clone(),
                canonical_signature: parsed.canonical_signature(),
                topic0: parsed.topic0(),
                emitter_roles: event.emitter_roles.clone(),
                normalized_events: event.normalized_events.clone(),
            });
        }
    }

    Ok(events)
}

pub async fn load_active_manifest_abi_events_by_chain_and_source_families(
    pool: &PgPool,
    chain: &str,
    source_families: &[String],
) -> Result<Vec<ActiveManifestAbiEvent>> {
    if source_families.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            manifest_id,
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            manifest_payload
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND chain = $1
          AND source_family = ANY($2::TEXT[])
        ORDER BY source_family, manifest_id
        "#,
    )
    .bind(chain)
    .bind(source_families)
    .fetch_all(pool)
    .await
    .context("failed to load active manifest ABI events by chain and source families")?;

    active_manifest_abi_events_from_rows(rows).await
}

async fn active_manifest_abi_events_from_rows(
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<ActiveManifestAbiEvent>> {
    let mut events = Vec::new();
    for row in rows {
        let manifest_id = row
            .try_get("manifest_id")
            .context("failed to read ABI manifest_id")?;
        let payload = row
            .try_get("manifest_payload")
            .context("failed to read ABI manifest_payload")?;
        let manifest_version = row
            .try_get::<i64, _>("manifest_version")
            .with_context(|| format!("failed to read ABI manifest_version for {manifest_id}"))?;
        let manifest_version = u64::try_from(manifest_version)
            .with_context(|| format!("manifest_version for {manifest_id} must be non-negative"))?;
        let namespace = row
            .try_get::<String, _>("namespace")
            .with_context(|| format!("failed to read ABI namespace for {manifest_id}"))?;
        let source_family = row
            .try_get::<String, _>("source_family")
            .with_context(|| format!("failed to read ABI source_family for {manifest_id}"))?;
        let chain = row
            .try_get::<String, _>("chain")
            .with_context(|| format!("failed to read ABI chain for {manifest_id}"))?;
        let deployment_epoch = row
            .try_get::<String, _>("deployment_epoch")
            .with_context(|| format!("failed to read ABI deployment_epoch for {manifest_id}"))?;
        let payload: ManifestAbiPayload = serde_json::from_value(payload)
            .with_context(|| format!("failed to decode manifest ABI payload for {manifest_id}"))?;

        for event in &payload.abi.events {
            let parsed = event.parsed_event_view().with_context(|| {
                format!(
                    "failed to derive ABI event view for manifest_id {manifest_id} event {}",
                    event.name
                )
            })?;
            events.push(ActiveManifestAbiEvent {
                manifest_id,
                manifest_version,
                namespace: namespace.clone(),
                source_family: source_family.clone(),
                chain: chain.clone(),
                deployment_epoch: deployment_epoch.clone(),
                name: event.name.clone(),
                canonical_signature: parsed.canonical_signature(),
                topic0: parsed.topic0(),
                emitter_roles: event.emitter_roles.clone(),
                normalized_events: event.normalized_events.clone(),
            });
        }
    }

    Ok(events)
}
