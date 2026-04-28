use anyhow::{Context, Result};
use bigname_manifests::WatchedChainPlan;
use bigname_storage::{ChainCheckpoint, sync_chain_checkpoints};

use crate::provider::ProviderRegistry;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WatchedChainPlanState {
    pub(crate) chain_count: usize,
    pub(crate) address_count: usize,
    pub(crate) entry_count: usize,
}

pub(crate) fn watched_chain_plan_state(plan: &[WatchedChainPlan]) -> WatchedChainPlanState {
    WatchedChainPlanState {
        chain_count: plan.len(),
        address_count: plan.iter().map(|chain| chain.addresses.len()).sum(),
        entry_count: plan
            .iter()
            .map(|chain| {
                chain.manifest_root_entry_count
                    + chain.manifest_contract_entry_count
                    + chain.discovery_edge_entry_count
            })
            .sum(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IntakeChainTask {
    pub(crate) chain: String,
    pub(crate) addresses: Vec<String>,
    pub(crate) manifest_root_entry_count: usize,
    pub(crate) manifest_contract_entry_count: usize,
    pub(crate) discovery_edge_entry_count: usize,
    pub(crate) checkpoint: ChainCheckpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IntakeRuntimeState {
    pub(crate) chain_count: usize,
    pub(crate) address_count: usize,
    pub(crate) entry_count: usize,
    pub(crate) cold_start_chain_count: usize,
    pub(crate) resumable_chain_count: usize,
    pub(crate) safe_checkpoint_chain_count: usize,
    pub(crate) finalized_checkpoint_chain_count: usize,
}

pub(crate) fn checkpoint_mode(checkpoint: &ChainCheckpoint) -> &'static str {
    if checkpoint.canonical_block_hash.is_some() && checkpoint.canonical_block_number.is_some() {
        "resume"
    } else {
        "cold_start"
    }
}

pub(crate) fn intake_runtime_state(tasks: &[IntakeChainTask]) -> IntakeRuntimeState {
    IntakeRuntimeState {
        chain_count: tasks.len(),
        address_count: tasks.iter().map(|task| task.addresses.len()).sum(),
        entry_count: tasks
            .iter()
            .map(|task| {
                task.manifest_root_entry_count
                    + task.manifest_contract_entry_count
                    + task.discovery_edge_entry_count
            })
            .sum(),
        cold_start_chain_count: tasks
            .iter()
            .filter(|task| checkpoint_mode(&task.checkpoint) == "cold_start")
            .count(),
        resumable_chain_count: tasks
            .iter()
            .filter(|task| checkpoint_mode(&task.checkpoint) == "resume")
            .count(),
        safe_checkpoint_chain_count: tasks
            .iter()
            .filter(|task| {
                task.checkpoint.safe_block_hash.is_some()
                    && task.checkpoint.safe_block_number.is_some()
            })
            .count(),
        finalized_checkpoint_chain_count: tasks
            .iter()
            .filter(|task| {
                task.checkpoint.finalized_block_hash.is_some()
                    && task.checkpoint.finalized_block_number.is_some()
            })
            .count(),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProviderAvailabilityStatus {
    Available,
    Unavailable,
}

impl ProviderAvailabilityStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProviderUnavailableReason {
    NoProvider,
}

impl ProviderUnavailableReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NoProvider => "no_provider",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IntakeProviderAvailability {
    pub(crate) chain: String,
    pub(crate) address_count: usize,
    pub(crate) entry_count: usize,
    pub(crate) status: ProviderAvailabilityStatus,
    pub(crate) unavailable_reason: Option<ProviderUnavailableReason>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderAvailabilityState {
    pub(crate) intake_chain_count: usize,
    pub(crate) configured_chain_count: usize,
    pub(crate) available_chain_count: usize,
    pub(crate) unavailable_chain_count: usize,
    pub(crate) chains: Vec<IntakeProviderAvailability>,
}

pub(crate) fn provider_availability_state(
    tasks: &[IntakeChainTask],
    provider_registry: &ProviderRegistry,
) -> ProviderAvailabilityState {
    let chains = tasks
        .iter()
        .map(|task| {
            let entry_count = task.manifest_root_entry_count
                + task.manifest_contract_entry_count
                + task.discovery_edge_entry_count;
            if provider_registry.provider_for(&task.chain).is_some() {
                IntakeProviderAvailability {
                    chain: task.chain.clone(),
                    address_count: task.addresses.len(),
                    entry_count,
                    status: ProviderAvailabilityStatus::Available,
                    unavailable_reason: None,
                }
            } else {
                IntakeProviderAvailability {
                    chain: task.chain.clone(),
                    address_count: task.addresses.len(),
                    entry_count,
                    status: ProviderAvailabilityStatus::Unavailable,
                    unavailable_reason: Some(ProviderUnavailableReason::NoProvider),
                }
            }
        })
        .collect::<Vec<_>>();

    ProviderAvailabilityState {
        intake_chain_count: tasks.len(),
        configured_chain_count: provider_registry.configured_chain_count(),
        available_chain_count: chains
            .iter()
            .filter(|chain| chain.status == ProviderAvailabilityStatus::Available)
            .count(),
        unavailable_chain_count: chains
            .iter()
            .filter(|chain| chain.status == ProviderAvailabilityStatus::Unavailable)
            .count(),
        chains,
    }
}

pub(crate) fn validate_provider_registry_for_intake_tasks(
    tasks: &[IntakeChainTask],
    provider_registry: &ProviderRegistry,
) -> Result<()> {
    provider_registry
        .ensure_configured_chains_admitted(tasks.iter().map(|task| task.chain.as_str()))
}

pub(crate) async fn sync_intake_chain_tasks(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<Vec<IntakeChainTask>> {
    let chain_ids = watched_chain_plan
        .iter()
        .map(|chain| chain.chain.clone())
        .collect::<Vec<_>>();
    let checkpoints = sync_chain_checkpoints(pool, &chain_ids).await?;
    let checkpoints = checkpoints
        .into_iter()
        .map(|checkpoint| (checkpoint.chain_id.clone(), checkpoint))
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut tasks = Vec::with_capacity(watched_chain_plan.len());
    for chain in watched_chain_plan {
        let checkpoint = checkpoints.get(&chain.chain).cloned().with_context(|| {
            format!(
                "checkpoint sync did not return a persisted chain row for {}",
                chain.chain
            )
        })?;
        tasks.push(IntakeChainTask {
            chain: chain.chain.clone(),
            addresses: chain.addresses.clone(),
            manifest_root_entry_count: chain.manifest_root_entry_count,
            manifest_contract_entry_count: chain.manifest_contract_entry_count,
            discovery_edge_entry_count: chain.discovery_edge_entry_count,
            checkpoint,
        });
    }

    Ok(tasks)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intake_task(chain: &str) -> IntakeChainTask {
        IntakeChainTask {
            chain: chain.to_owned(),
            addresses: vec!["0x0000000000000000000000000000000000000001".to_owned()],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 0,
            discovery_edge_entry_count: 0,
            checkpoint: ChainCheckpoint {
                chain_id: chain.to_owned(),
                canonical_block_hash: None,
                canonical_block_number: None,
                safe_block_hash: None,
                safe_block_number: None,
                finalized_block_hash: None,
                finalized_block_number: None,
            },
        }
    }

    #[test]
    fn provider_availability_marks_missing_base_provider_unavailable() -> Result<()> {
        let tasks = vec![intake_task("base-mainnet"), intake_task("ethereum-mainnet")];
        let provider_registry = ProviderRegistry::from_chain_rpc_urls(&[
            "ethereum-mainnet=http://127.0.0.1:8545".to_owned(),
        ])?;

        let state = provider_availability_state(&tasks, &provider_registry);

        assert_eq!(state.configured_chain_count, 1);
        assert_eq!(state.intake_chain_count, 2);
        assert_eq!(state.available_chain_count, 1);
        assert_eq!(state.unavailable_chain_count, 1);
        assert_eq!(
            state.chains,
            vec![
                IntakeProviderAvailability {
                    chain: "base-mainnet".to_owned(),
                    address_count: 1,
                    entry_count: 1,
                    status: ProviderAvailabilityStatus::Unavailable,
                    unavailable_reason: Some(ProviderUnavailableReason::NoProvider),
                },
                IntakeProviderAvailability {
                    chain: "ethereum-mainnet".to_owned(),
                    address_count: 1,
                    entry_count: 1,
                    status: ProviderAvailabilityStatus::Available,
                    unavailable_reason: None,
                },
            ]
        );
        assert_eq!(state.chains[0].status.as_str(), "unavailable");
        assert_eq!(
            state.chains[0]
                .unavailable_reason
                .expect("Base must be unavailable because no provider is configured")
                .as_str(),
            "no_provider"
        );

        Ok(())
    }

    #[test]
    fn provider_validation_rejects_configured_chains_outside_intake_tasks() -> Result<()> {
        let tasks = vec![intake_task("base-mainnet"), intake_task("ethereum-mainnet")];
        let provider_registry = ProviderRegistry::from_chain_rpc_urls(&[
            "ethereum-mainnet=http://127.0.0.1:8545".to_owned(),
            "optimism-mainnet=http://127.0.0.1:7545".to_owned(),
        ])?;

        let error = validate_provider_registry_for_intake_tasks(&tasks, &provider_registry)
            .expect_err("out-of-profile provider must be rejected");

        assert!(
            error.to_string().contains(
                "configured provider source chains outside selected/admitted runtime chain set: optimism-mainnet"
            ),
            "unexpected error: {error:#}"
        );
        assert!(
            error
                .to_string()
                .contains("admitted runtime chains: base-mainnet, ethereum-mainnet"),
            "unexpected error: {error:#}"
        );

        Ok(())
    }
}
