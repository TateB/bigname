use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use anyhow::{Context, Result, ensure};
use sqlx::Row;
use tracing::info;

use crate::runtime::{
    log_ens_v1_reverse_claim_sync_summary, log_ens_v1_subregistry_discovery_sync_summary,
    log_ens_v1_unwrapped_authority_sync_summary, log_ens_v2_permissions_sync_summary,
    log_ens_v2_registrar_sync_summary, log_ens_v2_registry_resource_surface_sync_summary,
    log_ens_v2_resolver_sync_summary,
};

use super::sync_logging::log_adapter_call_timing;
use crate::reconciliation::{
    replay::NormalizedEventReplayAdapter, types::PersistedRawPayloadAdapterSyncSummary,
};

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}

pub(crate) async fn sync_full_closure_normalized_events_from_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    range_start_block_number: i64,
    target_block_number: i64,
    adapters: &[NormalizedEventReplayAdapter],
    max_raw_logs_per_page: usize,
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    let mut aggregate = PersistedRawPayloadAdapterSyncSummary::default();
    let checkpoint_context = bigname_adapters::ReplayAdapterCheckpointContext {
        deployment_profile: deployment_profile.to_owned(),
        cursor_kind: "raw_fact_normalized_events".to_owned(),
        range_start_block_number,
        target_block_number,
    };

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV1ReverseClaim) {
        let adapter_started = Instant::now();
        let summary = sync_ens_v1_reverse_claim_range_in_pages(
            pool,
            chain,
            range_start_block_number,
            target_block_number,
            max_raw_logs_per_page,
        )
        .await?;
        log_adapter_call_timing(
            chain,
            "ens_v1_reverse_claim",
            "sync_ens_v1_reverse_claim_range",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v1_reverse_claim_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
        );
    }

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery) {
        let adapter_started = Instant::now();
        let summary =
            bigname_adapters::sync_ens_v1_subregistry_discovery_with_replay_checkpoint_and_log_limit(
                pool,
                chain,
                &checkpoint_context,
                max_raw_logs_per_page,
            )
            .await?;
        log_adapter_call_timing(
            chain,
            "ens_v1_subregistry_discovery",
            "sync_ens_v1_subregistry_discovery",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_normalized_event_count,
            summary.total_normalized_event_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v1_subregistry_discovery_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_normalized_event_count,
            summary.total_normalized_event_inserted_count,
        );
        trim_allocator_after_full_closure_adapter("ens_v1_subregistry_discovery");
    }

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV1UnwrappedAuthority) {
        let adapter_started = Instant::now();
        let summary =
            bigname_adapters::sync_ens_v1_unwrapped_authority_with_replay_checkpoint_and_log_limit(
                pool,
                chain,
                &checkpoint_context,
                max_raw_logs_per_page,
            )
            .await?;
        log_adapter_call_timing(
            chain,
            "ens_v1_unwrapped_authority",
            "sync_ens_v1_unwrapped_authority",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_normalized_event_count,
            summary.total_normalized_event_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v1_unwrapped_authority_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_normalized_event_count,
            summary.total_normalized_event_inserted_count,
        );
        trim_allocator_after_full_closure_adapter("ens_v1_unwrapped_authority");
    }

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface) {
        let adapter_started = Instant::now();
        let summary = bigname_adapters::sync_ens_v2_registry_resource_surface_through_block(
            pool,
            chain,
            target_block_number,
        )
        .await?;
        log_adapter_call_timing(
            chain,
            "ens_v2_registry_resource_surface",
            "sync_ens_v2_registry_resource_surface_through_block",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_normalized_event_count,
            summary.total_normalized_event_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v2_registry_resource_surface_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_normalized_event_count,
            summary.total_normalized_event_inserted_count,
        );
        trim_allocator_after_full_closure_adapter("ens_v2_registry_resource_surface");
    }

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV2Registrar) {
        let adapter_started = Instant::now();
        let summary =
            bigname_adapters::sync_ens_v2_registrar_through_block(pool, chain, target_block_number)
                .await?;
        log_adapter_call_timing(
            chain,
            "ens_v2_registrar",
            "sync_ens_v2_registrar_through_block",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v2_registrar_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
        );
        trim_allocator_after_full_closure_adapter("ens_v2_registrar");
    }

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV2Resolver) {
        let adapter_started = Instant::now();
        let summary =
            bigname_adapters::sync_ens_v2_resolver_through_block(pool, chain, target_block_number)
                .await?;
        log_adapter_call_timing(
            chain,
            "ens_v2_resolver",
            "sync_ens_v2_resolver_through_block",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v2_resolver_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
        );
        trim_allocator_after_full_closure_adapter("ens_v2_resolver");
    }

    if adapters.contains(&NormalizedEventReplayAdapter::EnsV2Permissions) {
        let adapter_started = Instant::now();
        let summary = bigname_adapters::sync_ens_v2_permissions_through_block(
            pool,
            chain,
            target_block_number,
        )
        .await?;
        log_adapter_call_timing(
            chain,
            "ens_v2_permissions",
            "sync_ens_v2_permissions_through_block",
            0,
            0,
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
            adapter_started.elapsed().as_millis(),
        );
        log_ens_v2_permissions_sync_summary(chain, &summary);
        aggregate.add_counts(
            summary.scanned_log_count,
            summary.matched_log_count,
            summary.total_synced_count,
            summary.total_inserted_count,
        );
        trim_allocator_after_full_closure_adapter("ens_v2_permissions");
    }

    Ok(aggregate)
}

async fn sync_ens_v1_reverse_claim_range_in_pages(
    pool: &sqlx::PgPool,
    chain: &str,
    range_start_block_number: i64,
    target_block_number: i64,
    max_raw_logs_per_page: usize,
) -> Result<bigname_adapters::EnsV1ReverseClaimSyncSummary> {
    ensure!(
        max_raw_logs_per_page > 0,
        "ENSv1 reverse-claim replay max logs per page must be positive"
    );
    if range_start_block_number > target_block_number {
        return Ok(empty_reverse_claim_summary());
    }

    let reverse_scope = load_reverse_claim_replay_scope(pool, chain).await?;
    if reverse_scope.source_scope.is_empty() {
        return Ok(empty_reverse_claim_summary());
    }

    let mut aggregate = empty_reverse_claim_summary();
    let mut page_from_block = range_start_block_number;
    let mut page_count = 0usize;
    while page_from_block <= target_block_number {
        let page_to_block = select_reverse_claim_replay_page_to_block(
            pool,
            chain,
            page_from_block,
            target_block_number,
            &reverse_scope.addresses,
            max_raw_logs_per_page,
        )
        .await?;
        let page_block_hashes = load_reverse_claim_replay_page_block_hashes(
            pool,
            chain,
            page_from_block,
            page_to_block,
            &reverse_scope.addresses,
        )
        .await?;
        let page_summary =
            bigname_adapters::EnsV1ReverseClaimSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                &page_block_hashes,
                &reverse_scope.source_scope,
            )
            .await?;
        merge_reverse_claim_summary(&mut aggregate, page_summary);
        page_count += 1;
        info!(
            service = "indexer",
            adapter = "ens_v1_reverse_claim",
            chain,
            page_from_block,
            page_to_block,
            page_count,
            page_block_hash_count = page_block_hashes.len(),
            max_raw_logs_per_page,
            scanned_log_count = aggregate.scanned_log_count,
            matched_log_count = aggregate.matched_log_count,
            total_synced_count = aggregate.total_synced_count,
            total_inserted_count = aggregate.total_inserted_count,
            "ENSv1 reverse-claim full-closure replay page completed"
        );
        page_from_block = page_to_block
            .checked_add(1)
            .context("ENSv1 reverse-claim replay page boundary overflowed")?;
    }

    Ok(aggregate)
}

struct ReverseClaimReplayScope {
    source_scope: Vec<(String, String, i64, i64)>,
    addresses: Vec<String>,
}

async fn load_reverse_claim_replay_scope(
    pool: &sqlx::PgPool,
    chain: &str,
) -> Result<ReverseClaimReplayScope> {
    let mut source_scope = Vec::new();
    let mut addresses = BTreeSet::new();
    for contract in bigname_manifests::load_manifest_declared_watched_contracts(pool)
        .await?
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .filter(|contract| {
            matches!(
                contract.source_family.as_str(),
                "ens_v1_reverse_l1" | "basenames_base_primary"
            )
        })
    {
        let address = contract.address.to_ascii_lowercase();
        addresses.insert(address.clone());
        source_scope.push((contract.source_family, address, i64::MIN, i64::MAX));
    }
    Ok(ReverseClaimReplayScope {
        source_scope,
        addresses: addresses.into_iter().collect(),
    })
}

async fn select_reverse_claim_replay_page_to_block(
    pool: &sqlx::PgPool,
    chain: &str,
    from_block: i64,
    target_block: i64,
    reverse_addresses: &[String],
    max_raw_logs_per_page: usize,
) -> Result<i64> {
    if from_block >= target_block || reverse_addresses.is_empty() {
        return Ok(target_block);
    }
    let max_raw_logs_per_page = i64::try_from(max_raw_logs_per_page)
        .context("reverse-claim replay max logs per page does not fit in i64")?;

    sqlx::query_scalar::<_, i64>(
        r#"
        WITH ordered_logs AS (
            SELECT rl.block_number
            FROM raw_logs rl
            JOIN chain_lineage lineage
              ON lineage.chain_id = rl.chain_id
             AND lineage.block_hash = rl.block_hash
            WHERE rl.chain_id = $1
              AND rl.block_number BETWEEN $2::BIGINT AND $3::BIGINT
              AND LOWER(rl.emitting_address) = ANY($4::TEXT[])
              AND rl.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND lineage.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            ORDER BY
                rl.block_number,
                rl.block_hash,
                rl.transaction_index,
                rl.log_index,
                rl.raw_log_id
            LIMIT ($5::BIGINT + 1)
        ),
        numbered_logs AS (
            SELECT block_number, ROW_NUMBER() OVER () AS ordinal
            FROM ordered_logs
        ),
        overflow AS (
            SELECT block_number
            FROM numbered_logs
            WHERE ordinal = $5::BIGINT + 1
        ),
        bounded AS (
            SELECT block_number
            FROM numbered_logs
            WHERE EXISTS (SELECT 1 FROM overflow)
              AND block_number < (SELECT block_number FROM overflow)
            UNION ALL
            SELECT MIN(block_number)
            FROM numbered_logs
            WHERE EXISTS (SELECT 1 FROM overflow)
            UNION ALL
            SELECT $3::BIGINT
            WHERE NOT EXISTS (SELECT 1 FROM overflow)
        )
        SELECT COALESCE(MAX(block_number), $3::BIGINT)
        FROM bounded
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(target_block)
    .bind(reverse_addresses)
    .bind(max_raw_logs_per_page)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to select log-bounded ENSv1 reverse-claim replay page for chain {chain} range {from_block}..={target_block}"
        )
    })
}

async fn load_reverse_claim_replay_page_block_hashes(
    pool: &sqlx::PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
    reverse_addresses: &[String],
) -> Result<Vec<String>> {
    if reverse_addresses.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        r#"
        SELECT rl.block_number, rl.block_hash
        FROM raw_logs rl
        JOIN chain_lineage lineage
          ON lineage.chain_id = rl.chain_id
         AND lineage.block_hash = rl.block_hash
        WHERE rl.chain_id = $1
          AND rl.block_number BETWEEN $2::BIGINT AND $3::BIGINT
          AND LOWER(rl.emitting_address) = ANY($4::TEXT[])
          AND rl.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        GROUP BY rl.block_number, rl.block_hash
        ORDER BY rl.block_number, rl.block_hash
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .bind(reverse_addresses)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to list canonical ENSv1 reverse-claim replay block hashes for chain {chain} range {from_block}..={to_block}"
        )
    })?;

    Ok(rows
        .into_iter()
        .map(|row| row.get::<String, _>("block_hash"))
        .collect())
}

fn empty_reverse_claim_summary() -> bigname_adapters::EnsV1ReverseClaimSyncSummary {
    bigname_adapters::EnsV1ReverseClaimSyncSummary {
        scanned_log_count: 0,
        matched_log_count: 0,
        total_synced_count: 0,
        total_inserted_count: 0,
        by_kind: BTreeMap::new(),
    }
}

fn merge_reverse_claim_summary(
    aggregate: &mut bigname_adapters::EnsV1ReverseClaimSyncSummary,
    page: bigname_adapters::EnsV1ReverseClaimSyncSummary,
) {
    aggregate.scanned_log_count += page.scanned_log_count;
    aggregate.matched_log_count += page.matched_log_count;
    aggregate.total_synced_count += page.total_synced_count;
    aggregate.total_inserted_count += page.total_inserted_count;
    for (kind, count) in page.by_kind {
        let entry = aggregate.by_kind.entry(kind).or_insert_with(|| {
            bigname_adapters::EnsV1ReverseClaimKindSyncSummary {
                synced_count: 0,
                inserted_count: 0,
            }
        });
        entry.synced_count += count.synced_count;
        entry.inserted_count += count.inserted_count;
    }
}

fn trim_allocator_after_full_closure_adapter(adapter: &'static str) {
    #[cfg(target_os = "linux")]
    {
        let malloc_trim_result = unsafe { malloc_trim(0) };
        info!(
            service = "indexer",
            adapter, malloc_trim_result, "allocator trim requested after full closure adapter"
        );
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = adapter;
    }
}
