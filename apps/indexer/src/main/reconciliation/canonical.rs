use anyhow::Result;
use bigname_storage::{
    CanonicalityState, ChainCheckpoint, ChainCheckpointUpdate, advance_chain_checkpoints,
    chain_lineage_contains_ancestor, load_chain_lineage_block, mark_chain_lineage_range_orphaned,
    upsert_chain_lineage_blocks,
};
use tracing::warn;

use crate::{
    provider::{ChainProviderOps, ProviderBlock, ProviderHeadSnapshot, ProviderRegistry},
    runtime::{IntakeChainTask, checkpoint_mode},
};

use super::{
    lineage::{
        head_change_set, lineage_block_to_provider, provider_block_to_checkpoint_ref,
        provider_block_to_lineage_with_header_audit_mode,
    },
    logging::log_chain_reconciliation_outcome,
    persistence::{
        persist_reconciled_raw_blocks, persist_reconciled_raw_code_hashes,
        persist_reconciled_raw_payloads,
    },
    types::{
        CanonicalReconciliation, CanonicalReconciliationStatus, ChainReconciliationOutcome,
        HeaderAuditMode,
    },
};

#[path = "canonical/orphaning.rs"]
mod orphaning;

use orphaning::orphan_reorg_losing_branch_payloads;

const MAX_PARENT_FETCH_DEPTH: usize = 131_072;

#[allow(dead_code)]
pub(crate) async fn poll_provider_heads(
    pool: &sqlx::PgPool,
    tasks: &mut Vec<IntakeChainTask>,
    provider_registry: &ProviderRegistry,
) -> Result<()> {
    poll_provider_heads_with_adapter_sync(
        pool,
        tasks,
        provider_registry,
        true,
        HeaderAuditMode::Minimal,
        &[],
    )
    .await
}

pub(crate) async fn poll_provider_heads_with_adapter_sync(
    pool: &sqlx::PgPool,
    tasks: &mut Vec<IntakeChainTask>,
    provider_registry: &ProviderRegistry,
    adapter_sync_enabled: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
) -> Result<()> {
    let mut next_tasks = tasks.clone();
    let mut any_change = false;

    for (index, task) in tasks.iter().enumerate() {
        let Some(provider) = provider_registry.provider_for(&task.chain) else {
            continue;
        };

        match reconcile_intake_chain_task_with_adapter_sync(
            pool,
            task,
            provider,
            adapter_sync_enabled,
            header_audit_mode,
            event_silent_reverse_resolver_addresses,
        )
        .await
        {
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

#[allow(dead_code)]
pub(crate) async fn reconcile_intake_chain_task(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    reconcile_intake_chain_task_with_adapter_sync(
        pool,
        task,
        provider,
        true,
        HeaderAuditMode::Minimal,
        &[],
    )
    .await
}

pub(crate) async fn reconcile_intake_chain_task_with_adapter_sync(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    adapter_sync_enabled: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    let heads = provider.fetch_chain_heads().await?;
    reconcile_fetched_heads_with_adapter_sync(
        pool,
        task,
        provider,
        &heads,
        adapter_sync_enabled,
        header_audit_mode,
        event_silent_reverse_resolver_addresses,
    )
    .await
}

#[allow(dead_code)]
pub(crate) async fn reconcile_fetched_heads(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    heads: &ProviderHeadSnapshot,
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    reconcile_fetched_heads_with_adapter_sync(
        pool,
        task,
        provider,
        heads,
        true,
        HeaderAuditMode::Minimal,
        &[],
    )
    .await
}

pub(crate) async fn reconcile_fetched_heads_with_adapter_sync(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    heads: &ProviderHeadSnapshot,
    adapter_sync_enabled: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: &[String],
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    let canonical = reconcile_canonical_head(
        pool,
        provider,
        &task.chain,
        &task.checkpoint,
        &heads.canonical,
        header_audit_mode,
    )
    .await?;
    let head_change_set = head_change_set(task, heads, &canonical);

    if canonical.status == CanonicalReconciliationStatus::ReorgReconciled {
        orphan_reorg_losing_branch_payloads(
            pool,
            &task.chain,
            task.checkpoint.canonical_block_hash.as_deref(),
            canonical.raw_orphan_stop_before_hash.as_deref(),
        )
        .await?;
    }

    persist_reconciled_raw_blocks(pool, &task.chain, heads, &canonical, header_audit_mode).await?;
    if head_change_set.requires_raw_payload_refresh(canonical.status) {
        persist_reconciled_raw_payloads(
            pool,
            &task.chain,
            &task.addresses,
            provider,
            heads,
            &canonical,
            head_change_set,
            adapter_sync_enabled,
            event_silent_reverse_resolver_addresses,
        )
        .await?;
    }
    persist_reconciled_raw_code_hashes(pool, task, provider, heads, &canonical, head_change_set)
        .await?;

    if let Some(safe_head) = &heads.safe {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage_with_header_audit_mode(
                &task.chain,
                safe_head,
                CanonicalityState::Safe,
                header_audit_mode,
            )],
        )
        .await?;
    }
    if let Some(finalized_head) = &heads.finalized {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage_with_header_audit_mode(
                &task.chain,
                finalized_head,
                CanonicalityState::Finalized,
                header_audit_mode,
            )],
        )
        .await?;
    }

    let canonical_update = canonical.canonical.clone();
    let (safe_update, finalized_update) = if canonical_update.is_some() {
        (
            heads.safe.as_ref().map(provider_block_to_checkpoint_ref),
            heads
                .finalized
                .as_ref()
                .map(provider_block_to_checkpoint_ref),
        )
    } else {
        (None, None)
    };

    let next_checkpoint = advance_chain_checkpoints(
        pool,
        &ChainCheckpointUpdate {
            chain_id: task.chain.clone(),
            canonical: canonical_update,
            safe: safe_update,
            finalized: finalized_update,
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
    provider: &(impl ChainProviderOps + ?Sized),
    chain: &str,
    checkpoint: &ChainCheckpoint,
    latest_head: &ProviderBlock,
    header_audit_mode: HeaderAuditMode,
) -> Result<CanonicalReconciliation> {
    let latest_hash = latest_head.block_hash.as_str();
    let current_canonical_hash = checkpoint.canonical_block_hash.as_deref();
    let current_canonical_number = checkpoint.canonical_block_number;

    if current_canonical_hash.is_none() {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage_with_header_audit_mode(
                chain,
                latest_head,
                CanonicalityState::Canonical,
                header_audit_mode,
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
            &[provider_block_to_lineage_with_header_audit_mode(
                chain,
                latest_head,
                CanonicalityState::Canonical,
                header_audit_mode,
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
            let can_be_current_branch_ancestor = stored_parent.canonicality_state
                != CanonicalityState::Orphaned
                && current_canonical_number
                    .is_some_and(|number| stored_parent.block_number <= number);
            let is_current_branch_ancestor = if can_be_current_branch_ancestor {
                if let Some(head_hash) = current_canonical_hash {
                    chain_lineage_contains_ancestor(
                        pool,
                        chain,
                        head_hash,
                        &stored_parent.block_hash,
                    )
                    .await?
                } else {
                    false
                }
            } else {
                false
            };
            if can_be_current_branch_ancestor && is_current_branch_ancestor {
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
                &[provider_block_to_lineage_with_header_audit_mode(
                    chain,
                    block,
                    CanonicalityState::Observed,
                    header_audit_mode,
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
            &[provider_block_to_lineage_with_header_audit_mode(
                chain,
                block,
                CanonicalityState::Canonical,
                header_audit_mode,
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
