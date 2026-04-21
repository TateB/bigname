use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use bigname_manifests::{
    WatchedSourceSelector, load_watched_chain_plan, load_watched_source_selector_plan,
};
use bigname_storage::{
    CanonicalityState, ChainCheckpoint, ChainCheckpointUpdate, ChainLineageBlock,
    CheckpointBlockRef, RawBlock, RawCodeHash, RawLog, RawReceipt, RawTransaction,
    advance_chain_checkpoints, invalidate_execution_outcomes_for_orphaned_blocks,
    list_canonical_raw_log_replay_inputs, list_canonical_raw_log_replay_inputs_for_block_hashes,
    load_chain_lineage_block, load_raw_block, load_raw_blocks_by_hashes,
    load_raw_code_hash_counts_by_block_hashes, mark_block_derived_normalized_events_range_orphaned,
    mark_chain_lineage_range_orphaned, mark_identity_rows_range_orphaned,
    mark_raw_block_facts_range_orphaned, upsert_chain_lineage_blocks, upsert_raw_blocks,
    upsert_raw_code_hashes, upsert_raw_logs, upsert_raw_receipts, upsert_raw_transactions,
};
use sha3::{Digest, Keccak256};
use tracing::{info, warn};

use crate::{
    MAX_PARENT_FETCH_DEPTH,
    provider::{
        self, ProviderBlock, ProviderBlockBundle, ProviderBlockSelection, ProviderCodeObservation,
        ProviderHeadSnapshot, ProviderLog, ProviderReceipt, ProviderRegistry, ProviderTransaction,
    },
    runtime::{
        IntakeChainTask, checkpoint_mode, log_block_derived_normalized_event_summary,
        log_ens_v1_reverse_claim_sync_summary, log_ens_v1_unwrapped_authority_sync_summary,
        log_ens_v2_permissions_sync_summary, log_ens_v2_registrar_sync_summary,
        log_ens_v2_registry_resource_surface_sync_summary, log_ens_v2_resolver_sync_summary,
    },
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CanonicalReconciliationStatus {
    Initialized,
    Unchanged,
    Appended,
    GapBackfilled,
    ReorgReconciled,
    AwaitingAncestor,
}

impl CanonicalReconciliationStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Initialized => "initialized",
            Self::Unchanged => "unchanged",
            Self::Appended => "appended",
            Self::GapBackfilled => "gap_backfilled",
            Self::ReorgReconciled => "reorg_reconciled",
            Self::AwaitingAncestor => "awaiting_ancestor",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalReconciliation {
    pub(crate) status: CanonicalReconciliationStatus,
    pub(crate) canonical: Option<CheckpointBlockRef>,
    pub(crate) fetched_parent_count: usize,
    pub(crate) orphaned_block_count: usize,
    pub(crate) reconciled_blocks: Vec<ProviderBlock>,
    pub(crate) raw_orphan_stop_before_hash: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HeadChangeSet {
    pub(crate) canonical_head_changed: bool,
    pub(crate) safe_head_changed: bool,
    pub(crate) finalized_head_changed: bool,
}

impl HeadChangeSet {
    fn requires_raw_payload_refresh(self, canonical_status: CanonicalReconciliationStatus) -> bool {
        canonical_status != CanonicalReconciliationStatus::Unchanged
            || self.safe_head_changed
            || self.finalized_head_changed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChainReconciliationOutcome {
    pub(crate) chain: String,
    pub(crate) canonical_status: CanonicalReconciliationStatus,
    pub(crate) canonical_head_changed: bool,
    pub(crate) safe_head_changed: bool,
    pub(crate) finalized_head_changed: bool,
    pub(crate) fetched_parent_count: usize,
    pub(crate) orphaned_block_count: usize,
    pub(crate) canonical_block_number: Option<i64>,
    pub(crate) safe_block_number: Option<i64>,
    pub(crate) finalized_block_number: Option<i64>,
}

pub(crate) fn log_chain_reconciliation_outcome(outcome: &ChainReconciliationOutcome) {
    info!(
        service = "indexer",
        chain = %outcome.chain,
        canonical_reconciliation_status = outcome.canonical_status.as_str(),
        canonical_head_changed = outcome.canonical_head_changed,
        safe_head_changed = outcome.safe_head_changed,
        finalized_head_changed = outcome.finalized_head_changed,
        fetched_parent_count = outcome.fetched_parent_count,
        orphaned_block_count = outcome.orphaned_block_count,
        canonical_block_number = outcome.canonical_block_number,
        safe_block_number = outcome.safe_block_number,
        finalized_block_number = outcome.finalized_block_number,
        "provider heads reconciled for chain"
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RawFactNormalizedEventReplaySelection {
    BlockRange { from_block: i64, to_block: i64 },
    BlockHashes(Vec<String>),
}

impl RawFactNormalizedEventReplaySelection {
    fn as_str(&self) -> &'static str {
        match self {
            Self::BlockRange { .. } => "block_range",
            Self::BlockHashes(_) => "block_hashes",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RawFactNormalizedEventReplayRequest {
    pub(crate) deployment_profile: String,
    pub(crate) chain: String,
    pub(crate) selection: RawFactNormalizedEventReplaySelection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RawFactNormalizedEventReplayOutcome {
    pub(crate) deployment_profile: String,
    pub(crate) chain: String,
    pub(crate) selection_kind: &'static str,
    pub(crate) selected_block_count: usize,
    pub(crate) canonical_raw_log_count: usize,
    pub(crate) scanned_raw_log_count: usize,
    pub(crate) matched_raw_log_count: usize,
    pub(crate) normalized_event_synced_count: usize,
    pub(crate) normalized_event_inserted_count: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PersistedRawPayloadAdapterSyncSummary {
    pub(crate) scanned_log_count: usize,
    pub(crate) matched_log_count: usize,
    pub(crate) total_synced_count: usize,
    pub(crate) total_inserted_count: usize,
}

impl PersistedRawPayloadAdapterSyncSummary {
    fn add_counts(
        &mut self,
        scanned_log_count: usize,
        matched_log_count: usize,
        total_synced_count: usize,
        total_inserted_count: usize,
    ) {
        self.scanned_log_count += scanned_log_count;
        self.matched_log_count += matched_log_count;
        self.total_synced_count += total_synced_count;
        self.total_inserted_count += total_inserted_count;
    }
}

pub(crate) fn log_raw_fact_normalized_event_replay_outcome(
    outcome: &RawFactNormalizedEventReplayOutcome,
) {
    info!(
        service = "indexer",
        command = "replay normalized-events",
        deployment_profile = %outcome.deployment_profile,
        chain = %outcome.chain,
        selection_kind = outcome.selection_kind,
        selected_block_count = outcome.selected_block_count,
        canonical_raw_log_count = outcome.canonical_raw_log_count,
        scanned_raw_log_count = outcome.scanned_raw_log_count,
        matched_raw_log_count = outcome.matched_raw_log_count,
        normalized_event_sync_total_count = outcome.normalized_event_synced_count,
        normalized_event_inserted_total_count = outcome.normalized_event_inserted_count,
        "raw-fact normalized-event replay completed"
    );
}

pub(crate) async fn replay_raw_fact_normalized_events(
    pool: &sqlx::PgPool,
    request: RawFactNormalizedEventReplayRequest,
) -> Result<RawFactNormalizedEventReplayOutcome> {
    if request.deployment_profile.trim().is_empty() {
        bail!("deployment_profile must not be empty");
    }

    let selection_kind = request.selection.as_str();
    let raw_logs = match &request.selection {
        RawFactNormalizedEventReplaySelection::BlockRange {
            from_block,
            to_block,
        } => {
            list_canonical_raw_log_replay_inputs(pool, &request.chain, *from_block, *to_block)
                .await?
        }
        RawFactNormalizedEventReplaySelection::BlockHashes(block_hashes) => {
            list_canonical_raw_log_replay_inputs_for_block_hashes(
                pool,
                &request.chain,
                block_hashes,
            )
            .await?
        }
    };
    ensure_replay_matches_deployment_profile_scope(pool, &request, &raw_logs).await?;

    let block_hashes = raw_logs
        .iter()
        .map(|raw_log| raw_log.block_hash.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    ensure_replay_block_hashes_have_only_canonical_raw_logs(pool, &request.chain, &block_hashes)
        .await?;

    let normalized_event_summary = if block_hashes.is_empty() {
        PersistedRawPayloadAdapterSyncSummary::default()
    } else {
        sync_adapter_state_from_persisted_raw_payloads(pool, &request.chain, &block_hashes).await?
    };

    Ok(RawFactNormalizedEventReplayOutcome {
        deployment_profile: request.deployment_profile,
        chain: request.chain,
        selection_kind,
        selected_block_count: block_hashes.len(),
        canonical_raw_log_count: raw_logs.len(),
        scanned_raw_log_count: normalized_event_summary.scanned_log_count,
        matched_raw_log_count: normalized_event_summary.matched_log_count,
        normalized_event_synced_count: normalized_event_summary.total_synced_count,
        normalized_event_inserted_count: normalized_event_summary.total_inserted_count,
    })
}

async fn ensure_replay_matches_deployment_profile_scope(
    pool: &sqlx::PgPool,
    request: &RawFactNormalizedEventReplayRequest,
    raw_logs: &[bigname_storage::RawLogReplayInput],
) -> Result<()> {
    let active_profile = infer_active_manifest_deployment_profile(pool).await?;
    if request.deployment_profile != active_profile {
        bail!(
            "deployment_profile {} does not match active manifest/discovery corpus profile {active_profile}",
            request.deployment_profile
        );
    }

    if let Some((from_block, to_block)) = replay_manifest_scope_range(&request.selection, raw_logs)?
    {
        load_watched_source_selector_plan(
            pool,
            &request.chain,
            WatchedSourceSelector::WholeActiveWatchedChain,
            from_block,
            to_block,
        )
        .await
        .with_context(|| {
            format!(
                "deployment_profile {} has no active watched manifest/discovery route for chain {} over replay range {}..={}",
                request.deployment_profile, request.chain, from_block, to_block
            )
        })?;
    } else {
        ensure_active_watched_chain_for_replay_profile(
            pool,
            &request.deployment_profile,
            &request.chain,
        )
        .await?;
    }

    Ok(())
}

fn replay_manifest_scope_range(
    selection: &RawFactNormalizedEventReplaySelection,
    raw_logs: &[bigname_storage::RawLogReplayInput],
) -> Result<Option<(i64, i64)>> {
    match selection {
        RawFactNormalizedEventReplaySelection::BlockRange {
            from_block,
            to_block,
        } => Ok(Some((*from_block, *to_block))),
        RawFactNormalizedEventReplaySelection::BlockHashes(_) => {
            let from_block = raw_logs.iter().map(|raw_log| raw_log.block_number).min();
            let to_block = raw_logs.iter().map(|raw_log| raw_log.block_number).max();
            match (from_block, to_block) {
                (Some(from_block), Some(to_block)) => Ok(Some((from_block, to_block))),
                (None, None) => Ok(None),
                _ => bail!("raw log replay input block range is internally inconsistent"),
            }
        }
    }
}

async fn ensure_active_watched_chain_for_replay_profile(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<()> {
    let watched_plan = load_watched_chain_plan(pool).await.with_context(|| {
        format!(
            "failed to verify deployment_profile {deployment_profile} active watched chain route for chain {chain}"
        )
    })?;
    if !watched_plan.iter().any(|plan| plan.chain == chain) {
        bail!(
            "deployment_profile {deployment_profile} has no active watched manifest/discovery route for chain {chain}"
        );
    }

    Ok(())
}

async fn infer_active_manifest_deployment_profile(pool: &sqlx::PgPool) -> Result<String> {
    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT DISTINCT chain, deployment_epoch
        FROM manifest_versions
        WHERE rollout_status = 'active'
        ORDER BY chain, deployment_epoch
        "#,
    )
    .fetch_all(pool)
    .await
    .context(
        "failed to load active manifest/discovery corpus for replay deployment_profile enforcement",
    )?;

    if rows.is_empty() {
        bail!("deployment_profile cannot be enforced because no active manifests are loaded");
    }

    let all_mainnet = rows.iter().all(|(chain, _)| chain.ends_with("-mainnet"));
    if all_mainnet {
        return Ok("mainnet".to_owned());
    }

    let all_sepolia_dev = rows.iter().all(|(chain, deployment_epoch)| {
        chain.ends_with("-sepolia") && deployment_epoch.ends_with("_sepolia_dev")
    });
    if all_sepolia_dev {
        return Ok("sepolia-dev".to_owned());
    }

    bail!(
        "deployment_profile cannot be enforced because the active manifest/discovery corpus does not match a supported deployment profile"
    );
}

async fn ensure_replay_block_hashes_have_only_canonical_raw_logs(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<()> {
    if block_hashes.is_empty() {
        return Ok(());
    }

    let noncanonical_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state NOT IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to verify canonical raw log replay guard for chain {chain} across {} blocks",
            block_hashes.len()
        )
    })?;

    if noncanonical_count > 0 {
        bail!(
            "raw-fact normalized-event replay selected {noncanonical_count} noncanonical raw logs; refusing block-hash-scoped adapter replay"
        );
    }

    Ok(())
}

pub(crate) async fn poll_provider_heads(
    pool: &sqlx::PgPool,
    tasks: &mut Vec<IntakeChainTask>,
    provider_registry: &ProviderRegistry,
) -> Result<()> {
    let mut next_tasks = tasks.clone();
    let mut any_change = false;

    for (index, task) in tasks.iter().enumerate() {
        let Some(provider) = provider_registry.provider_for(&task.chain) else {
            continue;
        };

        match reconcile_intake_chain_task(pool, task, provider).await {
            Ok(Some((next_task, outcome))) => {
                log_chain_reconciliation_outcome(&outcome);
                next_tasks[index] = next_task;
                any_change = true;
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    service = "indexer",
                    chain = %task.chain,
                    error = ?error,
                    intake_checkpoint_mode = checkpoint_mode(&task.checkpoint),
                    "failed to fetch and reconcile provider heads for intake chain"
                );
            }
        }
    }

    if any_change {
        *tasks = next_tasks;
    }

    Ok(())
}

pub(crate) async fn reconcile_intake_chain_task(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &provider::JsonRpcProvider,
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    let heads = provider.fetch_chain_heads().await?;
    reconcile_fetched_heads(pool, task, provider, &heads).await
}

pub(crate) async fn reconcile_fetched_heads(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &provider::JsonRpcProvider,
    heads: &ProviderHeadSnapshot,
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    let canonical = reconcile_canonical_head(
        pool,
        provider,
        &task.chain,
        &task.checkpoint,
        &heads.canonical,
    )
    .await?;
    let head_change_set = head_change_set(task, heads, &canonical);

    if canonical.status == CanonicalReconciliationStatus::ReorgReconciled {
        if let Some(current_canonical_hash) = task.checkpoint.canonical_block_hash.as_deref()
            && load_raw_block(pool, &task.chain, current_canonical_hash)
                .await?
                .is_some()
        {
            mark_raw_block_facts_range_orphaned(
                pool,
                &task.chain,
                current_canonical_hash,
                canonical.raw_orphan_stop_before_hash.as_deref(),
            )
            .await?;
            let orphaned_normalized_event_count =
                mark_block_derived_normalized_events_range_orphaned(
                    pool,
                    &task.chain,
                    current_canonical_hash,
                    canonical.raw_orphan_stop_before_hash.as_deref(),
                )
                .await?;
            if orphaned_normalized_event_count > 0 {
                info!(
                    service = "indexer",
                    chain = %task.chain,
                    orphaned_normalized_event_count,
                    "block-derived normalized events orphaned for the losing branch"
                );
            }
            let orphaned_identity_counts = mark_identity_rows_range_orphaned(
                pool,
                &task.chain,
                current_canonical_hash,
                canonical.raw_orphan_stop_before_hash.as_deref(),
            )
            .await?;
            if orphaned_identity_counts.token_lineage_count > 0
                || orphaned_identity_counts.resource_count > 0
                || orphaned_identity_counts.name_surface_count > 0
                || orphaned_identity_counts.surface_binding_count > 0
            {
                info!(
                    service = "indexer",
                    chain = %task.chain,
                    orphaned_token_lineage_count = orphaned_identity_counts.token_lineage_count,
                    orphaned_resource_count = orphaned_identity_counts.resource_count,
                    orphaned_name_surface_count = orphaned_identity_counts.name_surface_count,
                    orphaned_surface_binding_count = orphaned_identity_counts.surface_binding_count,
                    "identity rows orphaned for the losing branch"
                );
            }
        }

        let execution_invalidation_summary =
            invalidate_execution_outcomes_for_orphaned_blocks(pool).await?;
        if execution_invalidation_summary.deleted_outcome_count > 0 {
            info!(
                service = "indexer",
                chain = %task.chain,
                invalidated_execution_outcome_count =
                    execution_invalidation_summary.deleted_outcome_count,
                "execution cache outcomes invalidated for orphaned block dependencies"
            );
        }
    }

    persist_reconciled_raw_blocks(pool, &task.chain, heads, &canonical).await?;
    if head_change_set.requires_raw_payload_refresh(canonical.status) {
        persist_reconciled_raw_payloads(
            pool,
            &task.chain,
            provider,
            heads,
            &canonical,
            head_change_set,
        )
        .await?;
    }
    persist_reconciled_raw_code_hashes(pool, task, provider, heads, &canonical, head_change_set)
        .await?;

    if let Some(safe_head) = &heads.safe {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                &task.chain,
                safe_head,
                CanonicalityState::Safe,
            )],
        )
        .await?;
    }
    if let Some(finalized_head) = &heads.finalized {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                &task.chain,
                finalized_head,
                CanonicalityState::Finalized,
            )],
        )
        .await?;
    }

    let next_checkpoint = advance_chain_checkpoints(
        pool,
        &ChainCheckpointUpdate {
            chain_id: task.chain.clone(),
            canonical: canonical.canonical.clone(),
            safe: heads.safe.as_ref().map(provider_block_to_checkpoint_ref),
            finalized: heads
                .finalized
                .as_ref()
                .map(provider_block_to_checkpoint_ref),
        },
    )
    .await?;

    if !head_change_set.canonical_head_changed
        && !head_change_set.safe_head_changed
        && !head_change_set.finalized_head_changed
        && canonical.status == CanonicalReconciliationStatus::Unchanged
    {
        return Ok(None);
    }

    let mut next_task = task.clone();
    next_task.checkpoint = next_checkpoint.clone();

    Ok(Some((
        next_task,
        ChainReconciliationOutcome {
            chain: task.chain.clone(),
            canonical_status: canonical.status,
            canonical_head_changed: head_change_set.canonical_head_changed,
            safe_head_changed: head_change_set.safe_head_changed,
            finalized_head_changed: head_change_set.finalized_head_changed,
            fetched_parent_count: canonical.fetched_parent_count,
            orphaned_block_count: canonical.orphaned_block_count,
            canonical_block_number: next_checkpoint.canonical_block_number,
            safe_block_number: next_checkpoint.safe_block_number,
            finalized_block_number: next_checkpoint.finalized_block_number,
        },
    )))
}

pub(crate) async fn reconcile_canonical_head(
    pool: &sqlx::PgPool,
    provider: &provider::JsonRpcProvider,
    chain: &str,
    checkpoint: &ChainCheckpoint,
    latest_head: &ProviderBlock,
) -> Result<CanonicalReconciliation> {
    let latest_hash = latest_head.block_hash.as_str();
    let current_canonical_hash = checkpoint.canonical_block_hash.as_deref();

    if current_canonical_hash.is_none() {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                chain,
                latest_head,
                CanonicalityState::Canonical,
            )],
        )
        .await?;
        return Ok(CanonicalReconciliation {
            status: CanonicalReconciliationStatus::Initialized,
            canonical: Some(provider_block_to_checkpoint_ref(latest_head)),
            fetched_parent_count: 0,
            orphaned_block_count: 0,
            reconciled_blocks: vec![latest_head.clone()],
            raw_orphan_stop_before_hash: None,
        });
    }

    if current_canonical_hash == Some(latest_hash) {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                chain,
                latest_head,
                CanonicalityState::Canonical,
            )],
        )
        .await?;
        return Ok(CanonicalReconciliation {
            status: CanonicalReconciliationStatus::Unchanged,
            canonical: Some(provider_block_to_checkpoint_ref(latest_head)),
            fetched_parent_count: 0,
            orphaned_block_count: 0,
            reconciled_blocks: vec![latest_head.clone()],
            raw_orphan_stop_before_hash: None,
        });
    }

    let mut path = vec![latest_head.clone()];
    let mut cursor = latest_head.clone();
    let mut fetched_parent_count = 0usize;
    let mut common_ancestor_hash = None::<String>;

    for _ in 0..MAX_PARENT_FETCH_DEPTH {
        if cursor.parent_hash.as_deref() == current_canonical_hash {
            common_ancestor_hash = current_canonical_hash.map(ToOwned::to_owned);
            break;
        }

        let Some(parent_hash) = cursor.parent_hash.clone() else {
            break;
        };

        if let Some(stored_parent) = load_chain_lineage_block(pool, chain, &parent_hash).await? {
            if stored_parent.canonicality_state != CanonicalityState::Orphaned {
                common_ancestor_hash = Some(stored_parent.block_hash.clone());
                break;
            }

            cursor = lineage_block_to_provider(&stored_parent);
            path.push(cursor.clone());
            continue;
        }

        let fetched_parent = provider.fetch_block_by_hash(&parent_hash).await?;
        fetched_parent_count += 1;
        if Some(fetched_parent.block_hash.as_str()) == current_canonical_hash {
            common_ancestor_hash = Some(fetched_parent.block_hash.clone());
            break;
        }

        cursor = fetched_parent.clone();
        path.push(fetched_parent);
    }

    if common_ancestor_hash.is_none() {
        for block in &path {
            upsert_chain_lineage_blocks(
                pool,
                &[provider_block_to_lineage(
                    chain,
                    block,
                    CanonicalityState::Observed,
                )],
            )
            .await?;
        }

        return Ok(CanonicalReconciliation {
            status: CanonicalReconciliationStatus::AwaitingAncestor,
            canonical: None,
            fetched_parent_count,
            orphaned_block_count: 0,
            reconciled_blocks: path,
            raw_orphan_stop_before_hash: None,
        });
    }

    let common_ancestor_hash = common_ancestor_hash.expect("checked above");
    let mut orphaned_block_count = 0usize;
    let status = if Some(common_ancestor_hash.as_str()) == current_canonical_hash {
        if path.len() == 1 {
            CanonicalReconciliationStatus::Appended
        } else {
            CanonicalReconciliationStatus::GapBackfilled
        }
    } else {
        orphaned_block_count = orphan_canonical_branch(
            pool,
            chain,
            current_canonical_hash.expect("current checkpoint must exist"),
            Some(common_ancestor_hash.as_str()),
        )
        .await?;
        CanonicalReconciliationStatus::ReorgReconciled
    };

    for block in path.iter().rev() {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                chain,
                block,
                CanonicalityState::Canonical,
            )],
        )
        .await?;
    }

    Ok(CanonicalReconciliation {
        status,
        canonical: Some(provider_block_to_checkpoint_ref(latest_head)),
        fetched_parent_count,
        orphaned_block_count,
        reconciled_blocks: path,
        raw_orphan_stop_before_hash: (status == CanonicalReconciliationStatus::ReorgReconciled)
            .then_some(common_ancestor_hash),
    })
}

pub(crate) async fn orphan_canonical_branch(
    pool: &sqlx::PgPool,
    chain: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<usize> {
    let mut orphaned_block_count = 0usize;
    let mut cursor_hash = Some(from_hash.to_owned());

    while let Some(block_hash) = cursor_hash {
        if Some(block_hash.as_str()) == stop_before_hash {
            break;
        }

        let snapshots =
            mark_chain_lineage_range_orphaned(pool, chain, &block_hash, stop_before_hash).await?;
        orphaned_block_count += snapshots.len();
        cursor_hash = None;
    }

    Ok(orphaned_block_count)
}

pub(crate) fn provider_block_to_lineage(
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: chain.to_owned(),
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp: sqlx::types::time::OffsetDateTime::from_unix_timestamp(
            block.block_timestamp_unix_secs,
        )
        .expect("provider block timestamp must fit in OffsetDateTime"),
        logs_bloom: block.logs_bloom.clone(),
        transactions_root: block.transactions_root.clone(),
        receipts_root: block.receipts_root.clone(),
        state_root: block.state_root.clone(),
        canonicality_state,
    }
}

pub(crate) fn lineage_block_to_provider(block: &ChainLineageBlock) -> ProviderBlock {
    ProviderBlock {
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp_unix_secs: block.block_timestamp.unix_timestamp(),
        logs_bloom: block.logs_bloom.clone(),
        transactions_root: block.transactions_root.clone(),
        receipts_root: block.receipts_root.clone(),
        state_root: block.state_root.clone(),
    }
}

pub(crate) fn provider_block_to_checkpoint_ref(block: &ProviderBlock) -> CheckpointBlockRef {
    CheckpointBlockRef {
        block_hash: block.block_hash.clone(),
        block_number: block.block_number,
    }
}

pub(crate) fn head_change_set(
    task: &IntakeChainTask,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
) -> HeadChangeSet {
    let next_safe = heads.safe.as_ref().map(provider_block_to_checkpoint_ref);
    let next_finalized = heads
        .finalized
        .as_ref()
        .map(provider_block_to_checkpoint_ref);

    HeadChangeSet {
        canonical_head_changed: checkpoint_ref_changed(
            task.checkpoint.canonical_block_hash.as_deref(),
            task.checkpoint.canonical_block_number,
            canonical.canonical.as_ref(),
        ),
        safe_head_changed: checkpoint_ref_changed(
            task.checkpoint.safe_block_hash.as_deref(),
            task.checkpoint.safe_block_number,
            next_safe.as_ref(),
        ),
        finalized_head_changed: checkpoint_ref_changed(
            task.checkpoint.finalized_block_hash.as_deref(),
            task.checkpoint.finalized_block_number,
            next_finalized.as_ref(),
        ),
    }
}

pub(crate) fn checkpoint_ref_changed(
    current_hash: Option<&str>,
    current_number: Option<i64>,
    next: Option<&CheckpointBlockRef>,
) -> bool {
    let Some(next) = next else {
        return false;
    };

    current_hash != Some(next.block_hash.as_str()) || current_number != Some(next.block_number)
}

pub(crate) async fn persist_reconciled_raw_blocks(
    pool: &sqlx::PgPool,
    chain: &str,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
) -> Result<()> {
    let mut blocks = std::collections::BTreeMap::<String, bigname_storage::RawBlock>::new();

    let canonical_state = canonical_raw_state(canonical.status);
    for block in &canonical.reconciled_blocks {
        insert_raw_block_candidate(&mut blocks, chain, block, canonical_state);
    }
    if let Some(safe) = &heads.safe {
        insert_raw_block_candidate(&mut blocks, chain, safe, CanonicalityState::Safe);
    }
    if let Some(finalized) = &heads.finalized {
        insert_raw_block_candidate(&mut blocks, chain, finalized, CanonicalityState::Finalized);
    }

    upsert_raw_blocks(pool, &blocks.into_values().collect::<Vec<_>>()).await?;
    Ok(())
}

pub(crate) async fn persist_reconciled_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    provider: &provider::JsonRpcProvider,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Result<()> {
    let block_hashes = raw_payload_candidate_hashes(heads, canonical, head_change_set);
    if block_hashes.is_empty() {
        return Ok(());
    }

    let raw_blocks = load_raw_blocks_by_hashes(pool, chain, &block_hashes).await?;
    if raw_blocks.len() != block_hashes.len() {
        bail!(
            "stored raw block count {} does not match the raw payload fetch plan size {} for chain {}",
            raw_blocks.len(),
            block_hashes.len(),
            chain
        );
    }

    let mut transactions = Vec::<RawTransaction>::new();
    let mut receipts = Vec::<RawReceipt>::new();
    let mut logs = Vec::<RawLog>::new();

    for raw_block in &raw_blocks {
        let bundle = provider
            .fetch_block_bundle_by_hash(&raw_block.block_hash)
            .await?;
        ensure_provider_bundle_matches_raw_block(raw_block, &bundle)?;

        transactions.extend(
            bundle
                .transactions
                .iter()
                .map(|transaction| {
                    provider_transaction_to_raw_transaction(chain, raw_block, transaction)
                })
                .collect::<Result<Vec<_>>>()?,
        );
        receipts.extend(
            bundle
                .receipts
                .iter()
                .map(|receipt| provider_receipt_to_raw_receipt(chain, raw_block, receipt))
                .collect::<Result<Vec<_>>>()?,
        );
        logs.extend(
            bundle
                .logs
                .iter()
                .map(|log| provider_log_to_raw_log(chain, raw_block, log))
                .collect::<Result<Vec<_>>>()?,
        );
    }

    upsert_raw_transactions(pool, &transactions).await?;
    upsert_raw_receipts(pool, &receipts).await?;
    upsert_raw_logs(pool, &logs).await?;
    sync_adapter_state_from_persisted_raw_payloads(pool, chain, &block_hashes).await?;

    Ok(())
}

pub(crate) async fn sync_adapter_state_from_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    let mut aggregate = PersistedRawPayloadAdapterSyncSummary::default();
    let initial_normalized_event_count = load_normalized_event_count(pool, chain).await?;

    let normalized_event_summary =
        bigname_adapters::sync_block_derived_normalized_events(pool, chain, block_hashes, None)
            .await?;
    log_block_derived_normalized_event_summary(chain, &normalized_event_summary);
    aggregate.add_counts(
        normalized_event_summary.scanned_log_count,
        normalized_event_summary.matched_log_count,
        normalized_event_summary.total_synced_count,
        0,
    );
    let reverse_claim_summary =
        bigname_adapters::EnsV1ReverseClaimSyncSummary::sync_for_block_hashes(
            pool,
            chain,
            block_hashes,
        )
        .await?;
    log_ens_v1_reverse_claim_sync_summary(chain, &reverse_claim_summary);
    aggregate.add_counts(
        reverse_claim_summary.scanned_log_count,
        reverse_claim_summary.matched_log_count,
        reverse_claim_summary.total_synced_count,
        0,
    );
    let unwrapped_authority_summary =
        bigname_adapters::EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
            pool,
            chain,
            block_hashes,
        )
        .await?;
    log_ens_v1_unwrapped_authority_sync_summary(chain, &unwrapped_authority_summary);
    aggregate.add_counts(
        unwrapped_authority_summary.scanned_log_count,
        unwrapped_authority_summary.matched_log_count,
        unwrapped_authority_summary.total_normalized_event_count,
        0,
    );
    let ens_v2_registry_summary =
        bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes(
            pool,
            chain,
            block_hashes,
        )
        .await?;
    log_ens_v2_registry_resource_surface_sync_summary(chain, &ens_v2_registry_summary);
    aggregate.add_counts(
        ens_v2_registry_summary.scanned_log_count,
        ens_v2_registry_summary.matched_log_count,
        ens_v2_registry_summary.total_normalized_event_count,
        0,
    );
    let ens_v2_registrar_summary =
        bigname_adapters::EnsV2RegistrarSyncSummary::sync_for_block_hashes(
            pool,
            chain,
            block_hashes,
        )
        .await?;
    log_ens_v2_registrar_sync_summary(chain, &ens_v2_registrar_summary);
    aggregate.add_counts(
        ens_v2_registrar_summary.scanned_log_count,
        ens_v2_registrar_summary.matched_log_count,
        ens_v2_registrar_summary.total_synced_count,
        0,
    );
    let ens_v2_resolver_summary =
        bigname_adapters::EnsV2ResolverSyncSummary::sync_for_block_hashes(
            pool,
            chain,
            block_hashes,
        )
        .await?;
    log_ens_v2_resolver_sync_summary(chain, &ens_v2_resolver_summary);
    aggregate.add_counts(
        ens_v2_resolver_summary.scanned_log_count,
        ens_v2_resolver_summary.matched_log_count,
        ens_v2_resolver_summary.total_synced_count,
        0,
    );
    let ens_v2_permissions_summary =
        bigname_adapters::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
            pool,
            chain,
            block_hashes,
        )
        .await?;
    log_ens_v2_permissions_sync_summary(chain, &ens_v2_permissions_summary);
    aggregate.add_counts(
        ens_v2_permissions_summary.scanned_log_count,
        ens_v2_permissions_summary.matched_log_count,
        ens_v2_permissions_summary.total_synced_count,
        0,
    );
    let final_normalized_event_count = load_normalized_event_count(pool, chain).await?;
    aggregate.total_inserted_count = normalized_event_insert_delta(
        initial_normalized_event_count,
        final_normalized_event_count,
    )?;

    Ok(aggregate)
}

async fn load_normalized_event_count(pool: &sqlx::PgPool, chain: &str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM normalized_events
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to count normalized_events for chain {chain}"))
}

fn normalized_event_insert_delta(before: i64, after: i64) -> Result<usize> {
    let inserted = after.saturating_sub(before);
    usize::try_from(inserted).context("normalized event insert count does not fit in usize")
}

pub(crate) async fn sync_adapter_state_from_scoped_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: &[(String, String, i64, i64)],
) -> Result<()> {
    let normalized_event_summary = bigname_adapters::sync_block_derived_normalized_events(
        pool,
        chain,
        block_hashes,
        Some(source_scope),
    )
    .await?;
    log_block_derived_normalized_event_summary(chain, &normalized_event_summary);

    Ok(())
}

pub(crate) async fn persist_reconciled_raw_code_hashes(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &provider::JsonRpcProvider,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Result<()> {
    if task.addresses.is_empty() {
        return Ok(());
    }

    let refreshed_block_hashes = raw_payload_candidate_hashes(heads, canonical, head_change_set)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let candidate_hashes = raw_code_hash_candidate_hashes(heads, canonical, head_change_set);
    if candidate_hashes.is_empty() {
        return Ok(());
    }

    let raw_blocks = load_raw_blocks_by_hashes(pool, &task.chain, &candidate_hashes).await?;
    if raw_blocks.len() != candidate_hashes.len() {
        bail!(
            "stored raw block count {} does not match the raw code-hash fetch plan size {} for chain {}",
            raw_blocks.len(),
            candidate_hashes.len(),
            task.chain
        );
    }

    let stored_counts =
        load_raw_code_hash_counts_by_block_hashes(pool, &task.chain, &candidate_hashes).await?;
    let raw_blocks = raw_blocks
        .into_iter()
        .filter(|raw_block| {
            refreshed_block_hashes.contains(&raw_block.block_hash)
                || stored_counts
                    .get(&raw_block.block_hash)
                    .copied()
                    .unwrap_or(0)
                    < task.addresses.len()
        })
        .collect::<Vec<_>>();
    if raw_blocks.is_empty() {
        return Ok(());
    }

    let mut code_hashes = Vec::<RawCodeHash>::new();
    for raw_block in &raw_blocks {
        let observations = provider
            .fetch_code_observations_at_block(
                &task.addresses,
                ProviderBlockSelection::Number(raw_block.block_number),
            )
            .await?;
        code_hashes.extend(
            observations
                .iter()
                .map(|observation| {
                    provider_code_observation_to_raw_code_hash(&task.chain, raw_block, observation)
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }

    upsert_raw_code_hashes(pool, &code_hashes).await?;
    Ok(())
}

pub(crate) fn raw_payload_candidate_hashes(
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Vec<String> {
    let mut hashes = BTreeSet::new();

    for block in &canonical.reconciled_blocks {
        hashes.insert(block.block_hash.clone());
    }

    if head_change_set.safe_head_changed
        || canonical.status == CanonicalReconciliationStatus::Initialized
    {
        if let Some(safe) = &heads.safe {
            hashes.insert(safe.block_hash.clone());
        }
    }

    if head_change_set.finalized_head_changed
        || canonical.status == CanonicalReconciliationStatus::Initialized
    {
        if let Some(finalized) = &heads.finalized {
            hashes.insert(finalized.block_hash.clone());
        }
    }

    hashes.into_iter().collect()
}

pub(crate) fn raw_code_hash_candidate_hashes(
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Vec<String> {
    let mut hashes = raw_payload_candidate_hashes(heads, canonical, head_change_set)
        .into_iter()
        .collect::<BTreeSet<_>>();

    if let Some(canonical) = canonical.canonical.as_ref() {
        hashes.insert(canonical.block_hash.clone());
    }
    if let Some(safe) = &heads.safe {
        hashes.insert(safe.block_hash.clone());
    }
    if let Some(finalized) = &heads.finalized {
        hashes.insert(finalized.block_hash.clone());
    }

    hashes.into_iter().collect()
}

pub(crate) fn ensure_provider_bundle_matches_raw_block(
    raw_block: &RawBlock,
    bundle: &ProviderBlockBundle,
) -> Result<()> {
    let candidate = provider_block_to_raw_block(
        raw_block.chain_id.as_str(),
        &bundle.block,
        raw_block.canonicality_state,
    );

    if candidate.block_hash != raw_block.block_hash
        || candidate.parent_hash != raw_block.parent_hash
        || candidate.block_number != raw_block.block_number
        || candidate.block_timestamp != raw_block.block_timestamp
        || candidate.logs_bloom != raw_block.logs_bloom
        || candidate.transactions_root != raw_block.transactions_root
        || candidate.receipts_root != raw_block.receipts_root
        || candidate.state_root != raw_block.state_root
    {
        bail!(
            "provider bundle block {} does not match stored raw block facts for chain {}",
            raw_block.block_hash,
            raw_block.chain_id
        );
    }

    Ok(())
}

pub(crate) fn canonical_raw_state(status: CanonicalReconciliationStatus) -> CanonicalityState {
    match status {
        CanonicalReconciliationStatus::AwaitingAncestor => CanonicalityState::Observed,
        CanonicalReconciliationStatus::Initialized
        | CanonicalReconciliationStatus::Unchanged
        | CanonicalReconciliationStatus::Appended
        | CanonicalReconciliationStatus::GapBackfilled
        | CanonicalReconciliationStatus::ReorgReconciled => CanonicalityState::Canonical,
    }
}

pub(crate) fn insert_raw_block_candidate(
    blocks: &mut std::collections::BTreeMap<String, bigname_storage::RawBlock>,
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
) {
    let candidate = provider_block_to_raw_block(chain, block, canonicality_state);
    blocks
        .entry(candidate.block_hash.clone())
        .and_modify(|existing| {
            existing.canonicality_state =
                preferred_canonicality(existing.canonicality_state, candidate.canonicality_state);
        })
        .or_insert(candidate);
}

pub(crate) fn preferred_canonicality(
    current: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    if canonicality_rank(incoming) > canonicality_rank(current) {
        incoming
    } else {
        current
    }
}

pub(crate) fn canonicality_rank(state: CanonicalityState) -> u8 {
    match state {
        CanonicalityState::Observed => 0,
        CanonicalityState::Canonical => 1,
        CanonicalityState::Safe => 2,
        CanonicalityState::Finalized => 3,
        CanonicalityState::Orphaned => 4,
    }
}

pub(crate) fn provider_transaction_to_raw_transaction(
    chain: &str,
    raw_block: &RawBlock,
    transaction: &ProviderTransaction,
) -> Result<RawTransaction> {
    ensure_block_scoped_identity(
        "transaction",
        chain,
        &raw_block.block_hash,
        raw_block.block_number,
        &transaction.block_hash,
        transaction.block_number,
    )?;

    Ok(RawTransaction {
        chain_id: chain.to_owned(),
        block_hash: transaction.block_hash.clone(),
        block_number: transaction.block_number,
        transaction_hash: transaction.transaction_hash.clone(),
        transaction_index: transaction.transaction_index,
        from_address: transaction.from.clone(),
        to_address: transaction.to.clone(),
        canonicality_state: raw_block.canonicality_state,
    })
}

pub(crate) fn provider_receipt_to_raw_receipt(
    chain: &str,
    raw_block: &RawBlock,
    receipt: &ProviderReceipt,
) -> Result<RawReceipt> {
    ensure_block_scoped_identity(
        "receipt",
        chain,
        &raw_block.block_hash,
        raw_block.block_number,
        &receipt.block_hash,
        receipt.block_number,
    )?;

    Ok(RawReceipt {
        chain_id: chain.to_owned(),
        block_hash: receipt.block_hash.clone(),
        block_number: receipt.block_number,
        transaction_hash: receipt.transaction_hash.clone(),
        transaction_index: receipt.transaction_index,
        contract_address: receipt.contract_address.clone(),
        status: parse_receipt_status(receipt.status)?,
        gas_used: receipt.gas_used,
        cumulative_gas_used: receipt.cumulative_gas_used,
        logs_bloom: receipt.logs_bloom.clone(),
        canonicality_state: raw_block.canonicality_state,
    })
}

pub(crate) fn provider_log_to_raw_log(
    chain: &str,
    raw_block: &RawBlock,
    log: &ProviderLog,
) -> Result<RawLog> {
    ensure_block_scoped_identity(
        "log",
        chain,
        &raw_block.block_hash,
        raw_block.block_number,
        &log.block_hash,
        log.block_number,
    )?;

    Ok(RawLog {
        chain_id: chain.to_owned(),
        block_hash: log.block_hash.clone(),
        block_number: log.block_number,
        transaction_hash: log.transaction_hash.clone(),
        transaction_index: log.transaction_index,
        log_index: log.log_index,
        emitting_address: log.address.clone(),
        topics: log.topics.clone(),
        data: parse_hex_bytes(&log.data)?,
        canonicality_state: raw_block.canonicality_state,
    })
}

pub(crate) fn provider_code_observation_to_raw_code_hash(
    chain: &str,
    raw_block: &RawBlock,
    observation: &ProviderCodeObservation,
) -> Result<RawCodeHash> {
    let code_byte_length = i64::try_from(observation.code.len()).with_context(|| {
        format!(
            "provider code observation byte length {} does not fit in i64 for chain {} block {} contract {}",
            observation.code.len(),
            chain,
            raw_block.block_hash,
            observation.address
        )
    })?;

    Ok(RawCodeHash {
        chain_id: chain.to_owned(),
        block_hash: raw_block.block_hash.clone(),
        block_number: raw_block.block_number,
        contract_address: observation.address.clone(),
        code_hash: keccak256_hex(&observation.code),
        code_byte_length,
        canonicality_state: raw_block.canonicality_state,
    })
}

pub(crate) fn ensure_block_scoped_identity(
    fact_kind: &str,
    chain: &str,
    expected_block_hash: &str,
    expected_block_number: i64,
    actual_block_hash: &str,
    actual_block_number: i64,
) -> Result<()> {
    if actual_block_hash != expected_block_hash || actual_block_number != expected_block_number {
        bail!(
            "provider {} block scope mismatch for chain {} expected {}@{} got {}@{}",
            fact_kind,
            chain,
            expected_block_hash,
            expected_block_number,
            actual_block_hash,
            actual_block_number
        );
    }

    Ok(())
}

pub(crate) fn parse_receipt_status(status: Option<i64>) -> Result<Option<bool>> {
    match status {
        Some(0) => Ok(Some(false)),
        Some(1) => Ok(Some(true)),
        Some(other) => bail!("unsupported receipt status value {other}"),
        None => Ok(None),
    }
}

pub(crate) fn keccak256_hex(bytes: &[u8]) -> String {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    hex_string(&hasher.finalize())
}

pub(crate) fn parse_hex_bytes(value: &str) -> Result<Vec<u8>> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() % 2 != 0 {
        bail!("invalid hex byte string with odd length");
    }

    let mut bytes = Vec::with_capacity(value.len() / 2);
    let chars = value.as_bytes();
    let mut index = 0;
    while index < chars.len() {
        let byte =
            std::str::from_utf8(&chars[index..index + 2]).context("invalid UTF-8 in hex string")?;
        bytes.push(
            u8::from_str_radix(byte, 16)
                .with_context(|| format!("failed to parse hex byte {byte}"))?,
        );
        index += 2;
    }

    Ok(bytes)
}

pub(crate) fn hex_string(bytes: &[u8]) -> String {
    let mut output = String::from("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }

    output
}

pub(crate) fn provider_block_to_raw_block(
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
) -> bigname_storage::RawBlock {
    bigname_storage::RawBlock {
        chain_id: chain.to_owned(),
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp: sqlx::types::time::OffsetDateTime::from_unix_timestamp(
            block.block_timestamp_unix_secs,
        )
        .expect("provider block timestamp must fit in OffsetDateTime"),
        logs_bloom: block.logs_bloom.clone(),
        transactions_root: block.transactions_root.clone(),
        receipts_root: block.receipts_root.clone(),
        state_root: block.state_root.clone(),
        canonicality_state,
    }
}
