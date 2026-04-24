use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres};

use crate::CanonicalityState;

use super::{
    decode::decode_normalized_event, reads::load_normalized_event_by_identity,
    types::NormalizedEvent, validation::validate_normalized_event,
};

/// Insert missing normalized events or refresh canonicality for existing rows.
pub async fn upsert_normalized_events(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<Vec<NormalizedEvent>> {
    if events.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for normalized-event upsert")?;

    let mut snapshots = Vec::with_capacity(events.len());
    for event in events {
        validate_normalized_event(event)?;
        snapshots.push(upsert_normalized_event(&mut transaction, event).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit normalized-event upsert")?;

    Ok(snapshots)
}

async fn upsert_normalized_event(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    event: &NormalizedEvent,
) -> Result<NormalizedEvent> {
    let raw_fact_ref = serde_json::to_string(&event.raw_fact_ref)
        .context("failed to serialize normalized-event raw_fact_ref")?;
    let before_state = serde_json::to_string(&event.before_state)
        .context("failed to serialize normalized-event before_state")?;
    let after_state = serde_json::to_string(&event.after_state)
        .context("failed to serialize normalized-event after_state")?;

    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state,
            before_state,
            after_state
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14::jsonb,
            $15,
            $16::canonicality_state,
            $17::jsonb,
            $18::jsonb
        )
        ON CONFLICT (event_identity) DO NOTHING
        RETURNING
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state::TEXT AS canonicality_state,
            before_state,
            after_state
        "#,
    )
    .bind(&event.event_identity)
    .bind(&event.namespace)
    .bind(&event.logical_name_id)
    .bind(event.resource_id)
    .bind(&event.event_kind)
    .bind(&event.source_family)
    .bind(event.manifest_version)
    .bind(event.source_manifest_id)
    .bind(&event.chain_id)
    .bind(event.block_number)
    .bind(&event.block_hash)
    .bind(&event.transaction_hash)
    .bind(event.log_index)
    .bind(raw_fact_ref)
    .bind(&event.derivation_kind)
    .bind(event.canonicality_state.as_str())
    .bind(before_state)
    .bind(after_state)
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert normalized event {} ({})",
            event.event_identity, event.event_kind
        )
    })? {
        return decode_normalized_event(snapshot);
    }

    let existing = load_normalized_event_by_identity(&mut **executor, &event.event_identity)
        .await?
        .with_context(|| {
            format!(
                "failed to reload existing normalized event {} after insert conflict",
                event.event_identity
            )
        })?;

    ensure_normalized_event_identity_matches(&existing, event)?;
    let next_state = merge_canonicality(existing.canonicality_state, event.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE normalized_events
        SET
            canonicality_state = $2::canonicality_state,
            observed_at = now()
        WHERE event_identity = $1
        RETURNING
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state::TEXT AS canonicality_state,
            before_state,
            after_state
        "#,
    )
    .bind(&event.event_identity)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh existing normalized event {} ({})",
            event.event_identity, event.event_kind
        )
    })?;

    decode_normalized_event(snapshot)
}

fn ensure_normalized_event_identity_matches(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
) -> Result<()> {
    if existing.namespace != incoming.namespace
        || existing.logical_name_id != incoming.logical_name_id
        || existing.resource_id != incoming.resource_id
        || existing.event_kind != incoming.event_kind
        || existing.source_family != incoming.source_family
        || existing.manifest_version != incoming.manifest_version
        || existing.source_manifest_id != incoming.source_manifest_id
        || existing.chain_id != incoming.chain_id
        || existing.block_number != incoming.block_number
        || existing.block_hash != incoming.block_hash
        || existing.transaction_hash != incoming.transaction_hash
        || existing.log_index != incoming.log_index
        || existing.raw_fact_ref != incoming.raw_fact_ref
        || existing.derivation_kind != incoming.derivation_kind
        || existing.before_state != incoming.before_state
        || existing.after_state != incoming.after_state
    {
        bail!(
            "normalized event identity mismatch for event {}",
            existing.event_identity
        );
    }

    Ok(())
}

fn merge_canonicality(
    current: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    match incoming {
        CanonicalityState::Orphaned => CanonicalityState::Orphaned,
        CanonicalityState::Observed => {
            if current == CanonicalityState::Orphaned {
                CanonicalityState::Observed
            } else {
                current
            }
        }
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized => {
            if current == CanonicalityState::Orphaned {
                incoming
            } else {
                current.promote_to(incoming)
            }
        }
    }
}
