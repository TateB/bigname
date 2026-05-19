use bigname_manifests::WatchedChainPlan;
use tracing::{info, warn};

use crate::provider::{ChainProviderKind, ProviderRegistry};

use super::super::intake::{
    IntakeChainTask, ProviderAvailabilityStatus, checkpoint_mode, intake_runtime_state,
    provider_availability_state, watched_chain_plan_state,
};

pub(crate) fn log_watched_chain_plan(stage: &'static str, plan: &[WatchedChainPlan]) {
    let state = watched_chain_plan_state(plan);

    if state.entry_count == 0 {
        warn!(
            service = "indexer",
            stage,
            watched_chain_count = state.chain_count,
            watched_address_count = state.address_count,
            watched_entry_count_total = state.entry_count,
            "no watched contract entries are active; indexer poll loop will stay idle until manifest state changes"
        );
        return;
    }

    info!(
        service = "indexer",
        stage,
        watched_chain_count = state.chain_count,
        watched_address_count = state.address_count,
        watched_entry_count_total = state.entry_count,
        "runtime watched chain plan rebuilt from stored manifest state"
    );

    for chain in plan {
        info!(
            service = "indexer",
            stage,
            chain = %chain.chain,
            watched_address_count = chain.addresses.len(),
            watched_entry_count_total = chain.manifest_root_entry_count
                + chain.manifest_contract_entry_count
                + chain.discovery_edge_entry_count,
            watched_manifest_root_entry_count = chain.manifest_root_entry_count,
            watched_manifest_contract_entry_count = chain.manifest_contract_entry_count,
            watched_discovery_edge_entry_count = chain.discovery_edge_entry_count,
            "runtime watched chain plan rebuilt for chain"
        );
    }
}

pub(crate) fn log_intake_chain_tasks(stage: &'static str, tasks: &[IntakeChainTask]) {
    let state = intake_runtime_state(tasks);

    if state.entry_count == 0 {
        warn!(
            service = "indexer",
            stage,
            intake_chain_count = state.chain_count,
            intake_address_count = state.address_count,
            intake_entry_count_total = state.entry_count,
            intake_cold_start_chain_count = state.cold_start_chain_count,
            intake_resumable_chain_count = state.resumable_chain_count,
            "no active intake chain tasks are available; persisted checkpoints will stay idle until manifest state changes"
        );
        return;
    }

    info!(
        service = "indexer",
        stage,
        intake_chain_count = state.chain_count,
        intake_address_count = state.address_count,
        intake_entry_count_total = state.entry_count,
        intake_cold_start_chain_count = state.cold_start_chain_count,
        intake_resumable_chain_count = state.resumable_chain_count,
        intake_safe_checkpoint_chain_count = state.safe_checkpoint_chain_count,
        intake_finalized_checkpoint_chain_count = state.finalized_checkpoint_chain_count,
        "runtime intake chain tasks rebuilt from stored watch state and persisted checkpoints"
    );

    for task in tasks {
        info!(
            service = "indexer",
            stage,
            chain = %task.chain,
            intake_checkpoint_mode = checkpoint_mode(&task.checkpoint),
            intake_address_count = task.addresses.len(),
            intake_entry_count_total = task.manifest_root_entry_count
                + task.manifest_contract_entry_count
                + task.discovery_edge_entry_count,
            intake_manifest_root_entry_count = task.manifest_root_entry_count,
            intake_manifest_contract_entry_count = task.manifest_contract_entry_count,
            intake_discovery_edge_entry_count = task.discovery_edge_entry_count,
            canonical_block_number = task.checkpoint.canonical_block_number,
            canonical_block_hash = task.checkpoint.canonical_block_hash.as_deref(),
            safe_block_number = task.checkpoint.safe_block_number,
            safe_block_hash = task.checkpoint.safe_block_hash.as_deref(),
            finalized_block_number = task.checkpoint.finalized_block_number,
            finalized_block_hash = task.checkpoint.finalized_block_hash.as_deref(),
            "runtime intake chain task rebuilt for chain"
        );
    }
}

pub(crate) fn log_provider_registry(
    stage: &'static str,
    tasks: &[IntakeChainTask],
    provider_registry: &ProviderRegistry,
) {
    let availability = provider_availability_state(tasks, provider_registry);

    info!(
        service = "indexer",
        stage,
        provider_configured_chain_count = availability.configured_chain_count,
        json_rpc_provider_configured_chain_count =
            provider_registry.configured_chain_count_by_kind(ChainProviderKind::JsonRpc),
        reth_db_provider_configured_chain_count =
            provider_registry.configured_chain_count_by_kind(ChainProviderKind::RethDb),
        intake_chain_count = availability.intake_chain_count,
        provider_available_chain_count = availability.available_chain_count,
        provider_unavailable_chain_count = availability.unavailable_chain_count,
        "provider registry loaded for intake chains"
    );

    for chain in &availability.chains {
        if chain.status == ProviderAvailabilityStatus::Unavailable {
            warn!(
                service = "indexer",
                stage,
                chain = %chain.chain,
                intake_address_count = chain.address_count,
                intake_entry_count_total = chain.entry_count,
                provider_availability_status = chain.status.as_str(),
                provider_unavailable_reason = chain
                    .unavailable_reason
                    .map(|reason| reason.as_str()),
                provider_backed_intake_status = "idle",
                "provider-backed intake is idle for active chain because no provider source is configured"
            );
        }
    }
}
