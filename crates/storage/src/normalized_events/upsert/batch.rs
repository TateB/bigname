use std::collections::HashSet;

use anyhow::{Context, Result, ensure};
use sqlx::Postgres;

use crate::normalized_events::{decode::decode_normalized_event, types::NormalizedEvent};

use super::{NORMALIZED_EVENT_FAST_INSERT_BATCH_SIZE, sanitize::serialize_jsonb_value};

pub(super) async fn insert_normalized_events_do_nothing(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
) -> Result<HashSet<String>> {
    let mut inserted_identities = HashSet::new();
    for chunk in events.chunks(NORMALIZED_EVENT_FAST_INSERT_BATCH_SIZE) {
        let mut event_identities = Vec::with_capacity(chunk.len());
        let mut namespaces = Vec::with_capacity(chunk.len());
        let mut logical_name_ids = Vec::with_capacity(chunk.len());
        let mut resource_ids = Vec::with_capacity(chunk.len());
        let mut event_kinds = Vec::with_capacity(chunk.len());
        let mut source_families = Vec::with_capacity(chunk.len());
        let mut manifest_versions = Vec::with_capacity(chunk.len());
        let mut source_manifest_ids = Vec::with_capacity(chunk.len());
        let mut chain_ids = Vec::with_capacity(chunk.len());
        let mut block_numbers = Vec::with_capacity(chunk.len());
        let mut block_hashes = Vec::with_capacity(chunk.len());
        let mut transaction_hashes = Vec::with_capacity(chunk.len());
        let mut log_indexes = Vec::with_capacity(chunk.len());
        let mut raw_fact_refs = Vec::with_capacity(chunk.len());
        let mut derivation_kinds = Vec::with_capacity(chunk.len());
        let mut canonicality_states = Vec::with_capacity(chunk.len());
        let mut before_states = Vec::with_capacity(chunk.len());
        let mut after_states = Vec::with_capacity(chunk.len());

        for event in chunk {
            event_identities.push(event.event_identity.clone());
            namespaces.push(event.namespace.clone());
            logical_name_ids.push(event.logical_name_id.clone());
            resource_ids.push(event.resource_id);
            event_kinds.push(event.event_kind.clone());
            source_families.push(event.source_family.clone());
            manifest_versions.push(event.manifest_version);
            source_manifest_ids.push(event.source_manifest_id);
            chain_ids.push(event.chain_id.clone());
            block_numbers.push(event.block_number);
            block_hashes.push(event.block_hash.clone());
            transaction_hashes.push(event.transaction_hash.clone());
            log_indexes.push(event.log_index);
            raw_fact_refs.push(serialize_jsonb_value(
                &event.raw_fact_ref,
                "failed to serialize normalized-event raw_fact_ref",
            )?);
            derivation_kinds.push(event.derivation_kind.clone());
            canonicality_states.push(event.canonicality_state.as_str().to_owned());
            before_states.push(serialize_jsonb_value(
                &event.before_state,
                "failed to serialize normalized-event before_state",
            )?);
            after_states.push(serialize_jsonb_value(
                &event.after_state,
                "failed to serialize normalized-event after_state",
            )?);
        }

        let rows = sqlx::query_scalar::<_, String>(
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
            SELECT
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
                raw_fact_ref::jsonb,
                derivation_kind,
                canonicality_state::canonicality_state,
                before_state::jsonb,
                after_state::jsonb
            FROM unnest(
                $1::TEXT[],
                $2::TEXT[],
                $3::TEXT[],
                $4::UUID[],
                $5::TEXT[],
                $6::TEXT[],
                $7::BIGINT[],
                $8::BIGINT[],
                $9::TEXT[],
                $10::BIGINT[],
                $11::TEXT[],
                $12::TEXT[],
                $13::BIGINT[],
                $14::TEXT[],
                $15::TEXT[],
                $16::TEXT[],
                $17::TEXT[],
                $18::TEXT[]
            ) AS input(
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
            ON CONFLICT (event_identity) DO NOTHING
            RETURNING event_identity
            "#,
        )
        .bind(&event_identities)
        .bind(&namespaces)
        .bind(&logical_name_ids)
        .bind(&resource_ids)
        .bind(&event_kinds)
        .bind(&source_families)
        .bind(&manifest_versions)
        .bind(&source_manifest_ids)
        .bind(&chain_ids)
        .bind(&block_numbers)
        .bind(&block_hashes)
        .bind(&transaction_hashes)
        .bind(&log_indexes)
        .bind(&raw_fact_refs)
        .bind(&derivation_kinds)
        .bind(&canonicality_states)
        .bind(&before_states)
        .bind(&after_states)
        .fetch_all(&mut **executor)
        .await
        .context("failed to bulk insert normalized-event batch")?;

        inserted_identities.extend(rows);
    }

    Ok(inserted_identities)
}

pub(super) async fn load_normalized_events_by_identities(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    event_identities: &[String],
) -> Result<Vec<NormalizedEvent>> {
    if event_identities.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
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
        FROM normalized_events
        WHERE event_identity = ANY($1::TEXT[])
        "#,
    )
    .bind(event_identities)
    .fetch_all(&mut **executor)
    .await
    .context("failed to load existing normalized events for batch upsert")?;

    rows.into_iter().map(decode_normalized_event).collect()
}

pub(super) async fn upsert_normalized_event_batch(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
) -> Result<()> {
    let mut event_identities = Vec::with_capacity(events.len());
    let mut namespaces = Vec::with_capacity(events.len());
    let mut logical_name_ids = Vec::with_capacity(events.len());
    let mut resource_ids = Vec::with_capacity(events.len());
    let mut event_kinds = Vec::with_capacity(events.len());
    let mut source_families = Vec::with_capacity(events.len());
    let mut manifest_versions = Vec::with_capacity(events.len());
    let mut source_manifest_ids = Vec::with_capacity(events.len());
    let mut chain_ids = Vec::with_capacity(events.len());
    let mut block_numbers = Vec::with_capacity(events.len());
    let mut block_hashes = Vec::with_capacity(events.len());
    let mut transaction_hashes = Vec::with_capacity(events.len());
    let mut log_indexes = Vec::with_capacity(events.len());
    let mut raw_fact_refs = Vec::with_capacity(events.len());
    let mut derivation_kinds = Vec::with_capacity(events.len());
    let mut canonicality_states = Vec::with_capacity(events.len());
    let mut before_states = Vec::with_capacity(events.len());
    let mut after_states = Vec::with_capacity(events.len());

    for event in events {
        event_identities.push(event.event_identity.clone());
        namespaces.push(event.namespace.clone());
        logical_name_ids.push(event.logical_name_id.clone());
        resource_ids.push(event.resource_id);
        event_kinds.push(event.event_kind.clone());
        source_families.push(event.source_family.clone());
        manifest_versions.push(event.manifest_version);
        source_manifest_ids.push(event.source_manifest_id);
        chain_ids.push(event.chain_id.clone());
        block_numbers.push(event.block_number);
        block_hashes.push(event.block_hash.clone());
        transaction_hashes.push(event.transaction_hash.clone());
        log_indexes.push(event.log_index);
        raw_fact_refs.push(serialize_jsonb_value(
            &event.raw_fact_ref,
            "failed to serialize normalized-event raw_fact_ref",
        )?);
        derivation_kinds.push(event.derivation_kind.clone());
        canonicality_states.push(event.canonicality_state.as_str().to_owned());
        before_states.push(serialize_jsonb_value(
            &event.before_state,
            "failed to serialize normalized-event before_state",
        )?);
        after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize normalized-event after_state",
        )?);
    }

    let rows_affected = sqlx::query(
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
        SELECT
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
            raw_fact_ref::jsonb,
            derivation_kind,
            canonicality_state::canonicality_state,
            before_state::jsonb,
            after_state::jsonb
        FROM unnest(
            $1::TEXT[],
            $2::TEXT[],
            $3::TEXT[],
            $4::UUID[],
            $5::TEXT[],
            $6::TEXT[],
            $7::BIGINT[],
            $8::BIGINT[],
            $9::TEXT[],
            $10::BIGINT[],
            $11::TEXT[],
            $12::TEXT[],
            $13::BIGINT[],
            $14::TEXT[],
            $15::TEXT[],
            $16::TEXT[],
            $17::TEXT[],
            $18::TEXT[]
        ) AS input(
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
        ON CONFLICT (event_identity) DO UPDATE
        SET
            canonicality_state = CASE
                WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state
                    THEN 'orphaned'::canonicality_state
                WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                    THEN CASE
                        WHEN normalized_events.canonicality_state = 'orphaned'::canonicality_state
                            THEN 'observed'::canonicality_state
                        ELSE normalized_events.canonicality_state
                    END
                WHEN normalized_events.canonicality_state = 'orphaned'::canonicality_state
                    THEN EXCLUDED.canonicality_state
                WHEN normalized_events.canonicality_state = 'finalized'::canonicality_state
                    OR EXCLUDED.canonicality_state = 'finalized'::canonicality_state
                    THEN 'finalized'::canonicality_state
                WHEN normalized_events.canonicality_state = 'safe'::canonicality_state
                    OR EXCLUDED.canonicality_state = 'safe'::canonicality_state
                    THEN 'safe'::canonicality_state
                WHEN normalized_events.canonicality_state = 'canonical'::canonicality_state
                    OR EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                    THEN 'canonical'::canonicality_state
                ELSE normalized_events.canonicality_state
            END,
            observed_at = now()
        WHERE normalized_events.namespace IS NOT DISTINCT FROM EXCLUDED.namespace
          AND normalized_events.logical_name_id IS NOT DISTINCT FROM EXCLUDED.logical_name_id
          AND normalized_events.resource_id IS NOT DISTINCT FROM EXCLUDED.resource_id
          AND normalized_events.event_kind IS NOT DISTINCT FROM EXCLUDED.event_kind
          AND normalized_events.source_family IS NOT DISTINCT FROM EXCLUDED.source_family
          AND normalized_events.manifest_version IS NOT DISTINCT FROM EXCLUDED.manifest_version
          AND normalized_events.source_manifest_id IS NOT DISTINCT FROM EXCLUDED.source_manifest_id
          AND normalized_events.chain_id IS NOT DISTINCT FROM EXCLUDED.chain_id
          AND normalized_events.block_number IS NOT DISTINCT FROM EXCLUDED.block_number
          AND normalized_events.block_hash IS NOT DISTINCT FROM EXCLUDED.block_hash
          AND normalized_events.transaction_hash IS NOT DISTINCT FROM EXCLUDED.transaction_hash
          AND normalized_events.log_index IS NOT DISTINCT FROM EXCLUDED.log_index
          AND normalized_events.raw_fact_ref IS NOT DISTINCT FROM EXCLUDED.raw_fact_ref
          AND normalized_events.derivation_kind IS NOT DISTINCT FROM EXCLUDED.derivation_kind
          AND normalized_events.before_state IS NOT DISTINCT FROM EXCLUDED.before_state
          AND normalized_events.after_state IS NOT DISTINCT FROM EXCLUDED.after_state
        "#,
    )
    .bind(&event_identities)
    .bind(&namespaces)
    .bind(&logical_name_ids)
    .bind(&resource_ids)
    .bind(&event_kinds)
    .bind(&source_families)
    .bind(&manifest_versions)
    .bind(&source_manifest_ids)
    .bind(&chain_ids)
    .bind(&block_numbers)
    .bind(&block_hashes)
    .bind(&transaction_hashes)
    .bind(&log_indexes)
    .bind(&raw_fact_refs)
    .bind(&derivation_kinds)
    .bind(&canonicality_states)
    .bind(&before_states)
    .bind(&after_states)
    .execute(&mut **executor)
    .await
    .context("failed to upsert normalized-event batch")?
    .rows_affected();

    ensure!(
        usize::try_from(rows_affected).context("normalized-event upsert count overflowed")?
            == events.len(),
        "normalized event identity mismatch in batch upsert"
    );

    Ok(())
}
