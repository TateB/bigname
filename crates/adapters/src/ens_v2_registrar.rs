use anyhow::Result;
use sqlx::PgPool;

use crate::normalized_event_support::upsert_normalized_events_with_counts;

mod active_emitters;
mod decoding;
mod event_building;
mod persistence_summary;
mod raw_logs;
mod resource_links;

#[cfg(test)]
mod tests;

use active_emitters::load_active_emitters;
use decoding::build_registrar_observation;
use event_building::build_registrar_event;
use persistence_summary::empty_summary;
use raw_logs::load_registrar_raw_logs;

use crate::adapter_manifest::load_required_active_manifest_event_topic0s_by_signature;

pub use persistence_summary::{EnsV2RegistrarKindSyncSummary, EnsV2RegistrarSyncSummary};

pub(super) const SOURCE_FAMILY_ENS_V2_REGISTRAR_L1: &str = "ens_v2_registrar_l1";
pub(super) const DERIVATION_KIND_ENS_V2_REGISTRAR: &str = "ens_v2_registrar";
pub(super) const REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
pub(super) const EVENT_KIND_REGISTRAR_NAME_REGISTERED: &str = "RegistrarNameRegistered";
pub(super) const EVENT_KIND_REGISTRATION_RENEWED: &str = "RegistrationRenewed";
pub(super) const ABI_EVENT_NAME_REGISTERED_SIGNATURE: &str =
    "NameRegistered(uint256,string,address,address,address,uint64,address,bytes32,uint256,uint256)";
pub(super) const ABI_EVENT_NAME_RENEWED_SIGNATURE: &str =
    "NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)";

impl EnsV2RegistrarSyncSummary {
    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v2_registrar_with_scope(pool, chain, true, block_hashes, None, None).await
    }

    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v2_registrar_with_scope(pool, chain, true, block_hashes, Some(source_scope), None)
            .await
    }
}

pub async fn sync_ens_v2_registrar(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV2RegistrarSyncSummary> {
    sync_ens_v2_registrar_with_scope(pool, chain, false, &[], None, None).await
}

pub async fn sync_ens_v2_registrar_through_block(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
) -> Result<EnsV2RegistrarSyncSummary> {
    sync_ens_v2_registrar_with_scope(pool, chain, false, &[], None, Some(target_block_number)).await
}

async fn sync_ens_v2_registrar_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    max_block_number: Option<i64>,
) -> Result<EnsV2RegistrarSyncSummary> {
    let mut active_emitters = load_active_emitters(pool, chain).await?;
    if let Some(source_scope) = source_scope {
        active_emitters.retain(|emitter| registrar_scope_includes_emitter(source_scope, emitter));
    }
    if active_emitters.is_empty() {
        return Ok(empty_summary(0));
    }
    let manifest_ids = active_emitters
        .iter()
        .map(|emitter| emitter.source_manifest_id)
        .collect::<Vec<_>>();
    let event_topics = load_required_active_manifest_event_topic0s_by_signature(
        pool,
        &manifest_ids,
        &[
            ABI_EVENT_NAME_REGISTERED_SIGNATURE,
            ABI_EVENT_NAME_RENEWED_SIGNATURE,
        ],
        "ENSv2 registrar",
    )
    .await?;

    let raw_logs = load_registrar_raw_logs(
        pool,
        chain,
        &active_emitters,
        restrict_to_block_hashes,
        block_hashes,
        source_scope,
        max_block_number,
    )
    .await?;
    let scanned_log_count = raw_logs.len();
    if raw_logs.is_empty() {
        return Ok(empty_summary(scanned_log_count));
    }

    let mut matched_log_count = 0usize;
    let mut events = Vec::new();
    for raw_log in &raw_logs {
        let Some(observation) = build_registrar_observation(raw_log, &event_topics)? else {
            continue;
        };
        matched_log_count += 1;
        events.push(build_registrar_event(pool, raw_log, observation).await?);
    }

    let counts = upsert_normalized_events_with_counts(pool, &events, "ENSv2 registrar").await?;
    let (total_synced_count, total_inserted_count, by_kind) = counts.into_parts_by_kind(
        |synced_count, inserted_count| EnsV2RegistrarKindSyncSummary {
            synced_count,
            inserted_count,
        },
    );

    Ok(EnsV2RegistrarSyncSummary {
        scanned_log_count,
        matched_log_count,
        total_synced_count,
        total_inserted_count,
        by_kind,
    })
}

fn registrar_scope_includes_emitter(
    source_scope: &[(String, String, i64, i64)],
    emitter: &active_emitters::ActiveEmitter,
) -> bool {
    source_scope
        .iter()
        .any(|(source_family, address, from_block, to_block)| {
            source_family == &emitter.source_family
                && address.eq_ignore_ascii_case(&emitter.address)
                && from_block <= to_block
        })
}
