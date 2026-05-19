use std::collections::BTreeMap;

use anyhow::Result;
use sqlx::PgPool;

use crate::adapter_manifest::load_required_active_manifest_event_topic0s_by_signature;
use crate::ens_v2_common::ActiveEmitter;
use crate::normalized_event_support::upsert_normalized_events_with_counts;

mod constants;
mod decode;
mod events;
mod queries;
mod types;
mod util;

pub(crate) const DERIVATION_KIND_ENS_V2_RESOLVER: &str = constants::DERIVATION_KIND_ENS_V2_RESOLVER;
use decode::build_resolver_observation;
use events::build_resolver_events;
use queries::{load_active_emitters, load_resolver_raw_logs};

#[cfg(test)]
pub(crate) mod testsupport;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2ResolverSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, EnsV2ResolverKindSyncSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2ResolverKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

impl EnsV2ResolverSyncSummary {
    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v2_resolver_with_scope(pool, chain, true, block_hashes, None, None).await
    }

    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v2_resolver_with_scope(pool, chain, true, block_hashes, Some(source_scope), None)
            .await
    }
}

pub async fn sync_ens_v2_resolver(pool: &PgPool, chain: &str) -> Result<EnsV2ResolverSyncSummary> {
    sync_ens_v2_resolver_with_scope(pool, chain, false, &[], None, None).await
}

pub async fn sync_ens_v2_resolver_through_block(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
) -> Result<EnsV2ResolverSyncSummary> {
    sync_ens_v2_resolver_with_scope(pool, chain, false, &[], None, Some(target_block_number)).await
}

async fn sync_ens_v2_resolver_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    max_block_number: Option<i64>,
) -> Result<EnsV2ResolverSyncSummary> {
    let mut active_emitters = load_active_emitters(pool, chain).await?;
    if let Some(source_scope) = source_scope {
        active_emitters.retain(|emitter| resolver_scope_includes_emitter(source_scope, emitter));
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
        &constants::ABI_EVENT_SIGNATURES,
        "ENSv2 resolver",
    )
    .await?;

    let raw_logs = load_resolver_raw_logs(
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
        let Some(observation) = build_resolver_observation(raw_log, &event_topics)? else {
            continue;
        };
        matched_log_count += 1;
        events.extend(build_resolver_events(pool, raw_log, observation).await?);
    }

    let counts = upsert_normalized_events_with_counts(pool, &events, "ENSv2 resolver").await?;
    let (total_synced_count, total_inserted_count, by_kind) = counts.into_parts_by_kind(
        |synced_count, inserted_count| EnsV2ResolverKindSyncSummary {
            synced_count,
            inserted_count,
        },
    );

    Ok(EnsV2ResolverSyncSummary {
        scanned_log_count,
        matched_log_count,
        total_synced_count,
        total_inserted_count,
        by_kind,
    })
}

fn resolver_scope_includes_emitter(
    source_scope: &[(String, String, i64, i64)],
    emitter: &ActiveEmitter,
) -> bool {
    source_scope
        .iter()
        .any(|(source_family, address, from_block, to_block)| {
            source_family == &emitter.source_family
                && address.eq_ignore_ascii_case(&emitter.address)
                && from_block <= to_block
        })
}

fn empty_summary(scanned_log_count: usize) -> EnsV2ResolverSyncSummary {
    EnsV2ResolverSyncSummary {
        scanned_log_count,
        matched_log_count: 0,
        total_synced_count: 0,
        total_inserted_count: 0,
        by_kind: BTreeMap::new(),
    }
}
