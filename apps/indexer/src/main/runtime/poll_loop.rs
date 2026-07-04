use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::normalized_replay_catchup::normalized_replay_cursors_complete;
use crate::provider::ProviderRegistry;
use crate::reconciliation::{
    HeaderAuditMode, poll_provider_heads_with_adapter_sync,
    sync_live_adapter_backlog_after_normalized_replay,
};
use crate::replay::deployment_profile_from_manifest_root;

use super::adapter_sync::sync_adapter_owned_raw_log_state;
use super::intake::{
    IntakeChainTask, intake_runtime_state, sync_intake_chain_tasks,
    validate_provider_registry_for_intake_tasks, watched_chain_plan_state,
};
use super::logging::{
    log_intake_chain_tasks, log_manifest_normalized_event_summary, log_manifest_runtime_state,
    log_manifest_summary, log_provider_registry, log_watched_chain_plan,
    log_watched_contract_summary,
};
use super::manifest::{
    ManifestRuntimeState, RuntimeWatchScope, build_manifest_runtime_state_with_watch_scope,
    ensure_manifest_root_ready, load_manifest_repository,
};
use super::refresh::{
    refresh_intake_chain_tasks, refresh_manifest_normalized_events_from_storage,
    refresh_runtime_state_from_storage_discovery,
};

pub(crate) async fn run_poll_loop(
    pool: &sqlx::PgPool,
    manifests_root: PathBuf,
    mut manifest_runtime_state: ManifestRuntimeState,
    mut intake_chain_tasks: Vec<IntakeChainTask>,
    provider_registry: &ProviderRegistry,
    poll_interval_secs: u64,
    runtime_watch_scope: RuntimeWatchScope,
    adapter_sync_on_manifest_refresh: bool,
    adapter_sync_on_live_poll: bool,
    adapter_sync_on_live_poll_after_normalized_replay_catchup: bool,
    manifest_observation_refresh_enabled: bool,
    discovery_refresh_enabled: bool,
    header_audit_mode: HeaderAuditMode,
    event_silent_reverse_resolver_addresses: Vec<String>,
) -> Result<()> {
    let deployment_profile = deployment_profile_from_manifest_root(&manifests_root);
    let mut live_poll_adapter_sync_restored_after_replay = false;
    let mut interval = tokio::time::interval(Duration::from_secs(poll_interval_secs));
    interval.tick().await;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!(service = "indexer", "shutdown signal received");
                return Ok(());
            }
            _ = interval.tick() => {
                match load_manifest_repository(&manifests_root) {
                    Ok(manifest_repository) => {
                        let manifest_summary = manifest_repository.summary().clone();
                        if manifest_summary != manifest_runtime_state.manifest_summary {
                            log_manifest_summary(&manifest_summary);
                        }

                        if !manifest_runtime_state.repository_refresh_needed(&manifest_repository) {
                        } else if let Err(error) = ensure_manifest_root_ready(&manifest_summary) {
                            let current_watch_state =
                                watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                            let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                            warn!(
                                service = "indexer",
                                refresh_reason = "timer",
                                plan_source = "repository_manifest_reload",
                                error = ?error,
                                manifests_root = %manifest_summary.root.display(),
                                manifests_status = manifest_summary.status.as_str(),
                                watched_chain_count = current_watch_state.chain_count,
                                watched_address_count = current_watch_state.address_count,
                                watched_entry_count_total = current_watch_state.entry_count,
                                intake_chain_count = current_intake_state.chain_count,
                                intake_address_count = current_intake_state.address_count,
                                intake_entry_count_total = current_intake_state.entry_count,
                                "failed to reload repository manifests; keeping last successful runtime state"
                            );
                        } else {
                            match build_manifest_runtime_state_with_watch_scope(
                                pool,
                                &manifest_repository,
                                runtime_watch_scope,
                            )
                            .await
                            {
                                Ok(next_manifest_runtime_state) => {
                                    let manifest_state_changed =
                                        next_manifest_runtime_state != manifest_runtime_state;
                                    let watched_plan_changed = next_manifest_runtime_state
                                        .watched_chain_plan
                                        != manifest_runtime_state.watched_chain_plan;

                                    if adapter_sync_on_manifest_refresh
                                        && (manifest_state_changed || watched_plan_changed)
                                        && let Err(error) = sync_adapter_owned_raw_log_state(
                                            pool,
                                            &next_manifest_runtime_state.watched_chain_plan,
                                        )
                                        .await
                                    {
                                        let current_watch_state = watched_chain_plan_state(
                                            &manifest_runtime_state.watched_chain_plan,
                                        );
                                        let current_intake_state =
                                            intake_runtime_state(&intake_chain_tasks);
                                        warn!(
                                            service = "indexer",
                                            refresh_reason = "timer",
                                            plan_source = "repository_manifest_reload",
                                            error = ?error,
                                            watched_chain_count = current_watch_state.chain_count,
                                            watched_address_count = current_watch_state.address_count,
                                            watched_entry_count_total = current_watch_state.entry_count,
                                            intake_chain_count = current_intake_state.chain_count,
                                            intake_address_count = current_intake_state.address_count,
                                            intake_entry_count_total = current_intake_state.entry_count,
                                            "failed to sync adapter-owned raw-log state after repository manifest refresh; keeping last successful runtime state"
                                        );
                                        continue;
                                    }

                                    if manifest_state_changed {
                                        let previous_watch_state = watched_chain_plan_state(
                                            &manifest_runtime_state.watched_chain_plan,
                                        );
                                        let next_watch_state = watched_chain_plan_state(
                                            &next_manifest_runtime_state.watched_chain_plan,
                                        );
                                        info!(
                                            service = "indexer",
                                            refresh_reason = "timer",
                                            plan_source = "repository_manifest_reload",
                                            manifest_state_changed = true,
                                            watched_plan_changed,
                                            previous_manifest_count = manifest_runtime_state.manifest_summary.manifest_count,
                                            manifest_count = next_manifest_runtime_state.manifest_summary.manifest_count,
                                            previous_active_manifest_count = manifest_runtime_state.discovery_admission.active_manifest_count,
                                            stored_active_manifest_count = next_manifest_runtime_state.discovery_admission.active_manifest_count,
                                            previous_watched_chain_count = previous_watch_state.chain_count,
                                            previous_watched_address_count = previous_watch_state.address_count,
                                            previous_watched_entry_count_total = previous_watch_state.entry_count,
                                            watched_chain_count = next_watch_state.chain_count,
                                            watched_address_count = next_watch_state.address_count,
                                            watched_entry_count_total = next_watch_state.entry_count,
                                            "repository manifest refresh changed stored runtime state"
                                        );
                                        log_manifest_runtime_state(&next_manifest_runtime_state);
                                    }

                                    if watched_plan_changed {
                                        match sync_intake_chain_tasks(
                                            pool,
                                            &next_manifest_runtime_state.watched_chain_plan,
                                        )
                                        .await
                                        {
                                            Ok(next_tasks) => {
                                                validate_provider_registry_for_intake_tasks(
                                                    &next_tasks,
                                                    provider_registry,
                                                )
                                                .context(
                                                    "refreshed repository manifest state no longer matches configured provider sources",
                                                )?;
                                                let previous_watch_state = watched_chain_plan_state(
                                                    &manifest_runtime_state.watched_chain_plan,
                                                );
                                                let next_watch_state = watched_chain_plan_state(
                                                    &next_manifest_runtime_state.watched_chain_plan,
                                                );
                                                let previous_intake_state =
                                                    intake_runtime_state(&intake_chain_tasks);
                                                let next_intake_state =
                                                    intake_runtime_state(&next_tasks);

                                                info!(
                                                    service = "indexer",
                                                    refresh_reason = "timer",
                                                    watched_plan_changed = true,
                                                    plan_source = "repository_manifest_reload",
                                                    previous_watched_chain_count = previous_watch_state.chain_count,
                                                    previous_watched_address_count = previous_watch_state.address_count,
                                                    previous_watched_entry_count_total = previous_watch_state.entry_count,
                                                    watched_chain_count = next_watch_state.chain_count,
                                                    watched_address_count = next_watch_state.address_count,
                                                    watched_entry_count_total = next_watch_state.entry_count,
                                                    previous_intake_chain_count = previous_intake_state.chain_count,
                                                    previous_intake_address_count = previous_intake_state.address_count,
                                                    previous_intake_entry_count_total = previous_intake_state.entry_count,
                                                    intake_chain_count = next_intake_state.chain_count,
                                                    intake_address_count = next_intake_state.address_count,
                                                    intake_entry_count_total = next_intake_state.entry_count,
                                                    intake_cold_start_chain_count = next_intake_state.cold_start_chain_count,
                                                    intake_resumable_chain_count = next_intake_state.resumable_chain_count,
                                                    "runtime watched chain plan changed after repository manifest refresh"
                                                );
                                                log_watched_chain_plan(
                                                    "refresh",
                                                    &next_manifest_runtime_state.watched_chain_plan,
                                                );
                                                log_intake_chain_tasks("refresh", &next_tasks);
                                                log_provider_registry(
                                                    "refresh",
                                                    &next_tasks,
                                                    provider_registry,
                                                );
                                                manifest_runtime_state = next_manifest_runtime_state;
                                                intake_chain_tasks = next_tasks;
                                            }
                                            Err(error) => {
                                                let current_watch_state = watched_chain_plan_state(
                                                    &manifest_runtime_state.watched_chain_plan,
                                                );
                                                let current_intake_state =
                                                    intake_runtime_state(&intake_chain_tasks);
                                                warn!(
                                                    service = "indexer",
                                                    refresh_reason = "timer",
                                                    plan_source = "repository_manifest_reload",
                                                    error = ?error,
                                                    watched_chain_count = current_watch_state.chain_count,
                                                    watched_address_count = current_watch_state.address_count,
                                                    watched_entry_count_total = current_watch_state.entry_count,
                                                    intake_chain_count = current_intake_state.chain_count,
                                                    intake_address_count = current_intake_state.address_count,
                                                    intake_entry_count_total = current_intake_state.entry_count,
                                                    "failed to sync intake chain tasks for a changed watch plan after repository manifest refresh; keeping last successful runtime state"
                                                );
                                            }
                                        }
                                    } else {
                                        manifest_runtime_state = next_manifest_runtime_state;
                                    }
                                }
                                Err(error) => {
                                    let current_watch_state = watched_chain_plan_state(
                                        &manifest_runtime_state.watched_chain_plan,
                                    );
                                    let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                                    warn!(
                                        service = "indexer",
                                        refresh_reason = "timer",
                                        plan_source = "repository_manifest_reload",
                                        error = ?error,
                                        watched_chain_count = current_watch_state.chain_count,
                                        watched_address_count = current_watch_state.address_count,
                                        watched_entry_count_total = current_watch_state.entry_count,
                                        intake_chain_count = current_intake_state.chain_count,
                                        intake_address_count = current_intake_state.address_count,
                                        intake_entry_count_total = current_intake_state.entry_count,
                                        "failed to sync repository manifests into storage during refresh; keeping last successful runtime state"
                                    );
                                }
                            }
                        }
                    }
                    Err(error) => {
                        let current_watch_state =
                            watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                        let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                        warn!(
                            service = "indexer",
                            refresh_reason = "timer",
                            plan_source = "repository_manifest_reload",
                            error = ?error,
                            manifests_root = %manifests_root.display(),
                            watched_chain_count = current_watch_state.chain_count,
                            watched_address_count = current_watch_state.address_count,
                            watched_entry_count_total = current_watch_state.entry_count,
                            intake_chain_count = current_intake_state.chain_count,
                            intake_address_count = current_intake_state.address_count,
                            intake_entry_count_total = current_intake_state.entry_count,
                            "failed to load repository manifests during refresh; keeping last successful runtime state"
                        );
                    }
                }

                match refresh_intake_chain_tasks(
                    pool,
                    &intake_chain_tasks,
                    &manifest_runtime_state.watched_chain_plan,
                )
                .await
                {
                    Ok(Some(next_tasks)) => {
                        let previous_state = intake_runtime_state(&intake_chain_tasks);
                        let next_state = intake_runtime_state(&next_tasks);
                        info!(
                            service = "indexer",
                            refresh_reason = "timer",
                            watched_plan_changed = false,
                            checkpoint_state_changed = true,
                            plan_source = "stored_manifest_state",
                            previous_intake_chain_count = previous_state.chain_count,
                            previous_intake_address_count = previous_state.address_count,
                            previous_intake_entry_count_total = previous_state.entry_count,
                            previous_intake_cold_start_chain_count = previous_state.cold_start_chain_count,
                            previous_intake_resumable_chain_count = previous_state.resumable_chain_count,
                            intake_chain_count = next_state.chain_count,
                            intake_address_count = next_state.address_count,
                            intake_entry_count_total = next_state.entry_count,
                            intake_cold_start_chain_count = next_state.cold_start_chain_count,
                            intake_resumable_chain_count = next_state.resumable_chain_count,
                            intake_safe_checkpoint_chain_count = next_state.safe_checkpoint_chain_count,
                            intake_finalized_checkpoint_chain_count = next_state.finalized_checkpoint_chain_count,
                            "persisted checkpoint state changed for active intake chains"
                        );
                        log_intake_chain_tasks("checkpoint-refresh", &next_tasks);
                        intake_chain_tasks = next_tasks;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        let current_watch_state =
                            watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                        let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                        warn!(
                            service = "indexer",
                            refresh_reason = "timer",
                            plan_source = "stored_manifest_state",
                            error = ?error,
                            watched_chain_count = current_watch_state.chain_count,
                            watched_address_count = current_watch_state.address_count,
                            watched_entry_count_total = current_watch_state.entry_count,
                            intake_chain_count = current_intake_state.chain_count,
                            intake_address_count = current_intake_state.address_count,
                            intake_entry_count_total = current_intake_state.entry_count,
                            "failed to refresh runtime intake chain tasks; keeping last successful state"
                        );
                    }
                }

                let provider_configured_chains =
                    if adapter_sync_on_live_poll_after_normalized_replay_catchup {
                        intake_chain_tasks
                            .iter()
                            .filter(|task| provider_registry.provider_for(&task.chain).is_some())
                            .map(|task| task.chain.clone())
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                let mut effective_adapter_sync_on_live_poll = if adapter_sync_on_live_poll {
                    true
                } else if adapter_sync_on_live_poll_after_normalized_replay_catchup {
                    !provider_configured_chains.is_empty()
                        && live_poll_adapter_sync_ready_after_replay(
                            normalized_replay_cursors_complete(
                                pool,
                                &deployment_profile,
                                &provider_configured_chains,
                            )
                            .await,
                        )
                } else {
                    false
                };
                if effective_adapter_sync_on_live_poll
                    && !adapter_sync_on_live_poll
                    && !live_poll_adapter_sync_restored_after_replay
                {
                    let backlog_sync_failed = match sync_live_adapter_backlog_after_normalized_replay(
                        pool,
                        &deployment_profile,
                        &provider_configured_chains,
                    )
                    .await
                    {
                        Ok(backlog_summary) => {
                            info!(
                                service = "indexer",
                                command = "poll",
                                deployment_profile,
                                post_replay_backlog_chain_count = backlog_summary.chain_count,
                                post_replay_backlog_selected_block_count =
                                    backlog_summary.selected_block_count,
                                post_replay_backlog_scanned_log_count =
                                    backlog_summary.scanned_log_count,
                                post_replay_backlog_matched_log_count =
                                    backlog_summary.matched_log_count,
                                post_replay_backlog_normalized_event_synced_count =
                                    backlog_summary.normalized_event_synced_count,
                                post_replay_backlog_normalized_event_inserted_count =
                                    backlog_summary.normalized_event_inserted_count,
                                "live raw payload adapter sync enabled after normalized replay catch-up completed"
                            );
                            live_poll_adapter_sync_restored_after_replay = true;
                            false
                        }
                        Err(error) => {
                            warn!(
                                service = "indexer",
                                command = "poll",
                                deployment_profile,
                                provider_configured_chain_count = provider_configured_chains.len(),
                                error = ?error,
                                "failed to sync live adapter backlog after normalized replay catch-up; live poll loop will retry"
                            );
                            true
                        }
                    };
                    effective_adapter_sync_on_live_poll =
                        live_poll_adapter_sync_after_backlog_attempt(
                            effective_adapter_sync_on_live_poll,
                            backlog_sync_failed,
                        );
                }

                poll_provider_heads_with_adapter_sync(
                    pool,
                    &mut intake_chain_tasks,
                    provider_registry,
                    effective_adapter_sync_on_live_poll,
                    header_audit_mode,
                    &event_silent_reverse_resolver_addresses,
                )
                .await?;

                if manifest_observation_refresh_enabled {
                    match refresh_manifest_normalized_events_from_storage(
                        pool,
                        &manifest_runtime_state,
                    )
                    .await
                    {
                        Ok(Some(next_manifest_runtime_state)) => {
                            info!(
                                service = "indexer",
                                refresh_reason = "timer",
                                plan_source = "stored_manifest_observations",
                                normalized_event_inserted_total_count = next_manifest_runtime_state
                                    .manifest_normalized_event_summary
                                    .total_inserted_count,
                                normalized_event_sync_total_count = next_manifest_runtime_state
                                    .manifest_normalized_event_summary
                                    .total_synced_count,
                                normalized_event_kind_count = next_manifest_runtime_state
                                    .manifest_normalized_event_summary
                                    .by_kind
                                    .len(),
                                "manifest observation alert events changed after provider polling"
                            );
                            log_manifest_normalized_event_summary(
                                &next_manifest_runtime_state.manifest_normalized_event_summary,
                            );
                            manifest_runtime_state = next_manifest_runtime_state;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            let current_watch_state =
                                watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                            let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                            warn!(
                                service = "indexer",
                                refresh_reason = "timer",
                                plan_source = "stored_manifest_observations",
                                error = ?error,
                                watched_chain_count = current_watch_state.chain_count,
                                watched_address_count = current_watch_state.address_count,
                                watched_entry_count_total = current_watch_state.entry_count,
                                intake_chain_count = current_intake_state.chain_count,
                                intake_address_count = current_intake_state.address_count,
                                intake_entry_count_total = current_intake_state.entry_count,
                                "failed to refresh manifest observation alert events after provider polling; keeping last successful state"
                            );
                        }
                    }
                }

                if discovery_refresh_enabled {
                    match refresh_runtime_state_from_storage_discovery(pool, &manifest_runtime_state)
                        .await
                    {
                        Ok(Some((next_manifest_runtime_state, next_tasks))) => {
                            validate_provider_registry_for_intake_tasks(
                                &next_tasks,
                                provider_registry,
                            )
                            .context(
                                "refreshed stored discovery state no longer matches configured provider sources",
                            )?;
                            let previous_watch_state =
                                watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                            let next_watch_state =
                                watched_chain_plan_state(&next_manifest_runtime_state.watched_chain_plan);
                            let previous_intake_state = intake_runtime_state(&intake_chain_tasks);
                            let next_intake_state = intake_runtime_state(&next_tasks);

                            info!(
                                service = "indexer",
                                refresh_reason = "timer",
                                watched_plan_changed = true,
                                checkpoint_state_changed = false,
                                plan_source = "stored_discovery_state",
                                previous_watched_chain_count = previous_watch_state.chain_count,
                                previous_watched_address_count = previous_watch_state.address_count,
                                previous_watched_entry_count_total = previous_watch_state.entry_count,
                                watched_chain_count = next_watch_state.chain_count,
                                watched_address_count = next_watch_state.address_count,
                                watched_entry_count_total = next_watch_state.entry_count,
                                previous_intake_chain_count = previous_intake_state.chain_count,
                                previous_intake_address_count = previous_intake_state.address_count,
                                previous_intake_entry_count_total = previous_intake_state.entry_count,
                                intake_chain_count = next_intake_state.chain_count,
                                intake_address_count = next_intake_state.address_count,
                                intake_entry_count_total = next_intake_state.entry_count,
                                intake_cold_start_chain_count = next_intake_state.cold_start_chain_count,
                                intake_resumable_chain_count = next_intake_state.resumable_chain_count,
                                intake_safe_checkpoint_chain_count = next_intake_state.safe_checkpoint_chain_count,
                                intake_finalized_checkpoint_chain_count = next_intake_state.finalized_checkpoint_chain_count,
                                "runtime watched chain plan changed after stored discovery sync"
                            );
                            log_watched_contract_summary(&next_manifest_runtime_state.watched_contract_summary);
                            log_watched_chain_plan(
                                "discovery-refresh",
                                &next_manifest_runtime_state.watched_chain_plan,
                            );
                            log_intake_chain_tasks("discovery-refresh", &next_tasks);
                            log_provider_registry("discovery-refresh", &next_tasks, provider_registry);
                            manifest_runtime_state = next_manifest_runtime_state;
                            intake_chain_tasks = next_tasks;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            let current_watch_state =
                                watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                            let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                            warn!(
                                service = "indexer",
                                refresh_reason = "timer",
                                plan_source = "stored_discovery_state",
                                error = ?error,
                                watched_chain_count = current_watch_state.chain_count,
                                watched_address_count = current_watch_state.address_count,
                                watched_entry_count_total = current_watch_state.entry_count,
                                intake_chain_count = current_intake_state.chain_count,
                                intake_address_count = current_intake_state.address_count,
                                intake_entry_count_total = current_intake_state.entry_count,
                                "failed to refresh runtime watch state from stored discovery edges; keeping last successful state"
                            );
                        }
                    }
                }
            }
        }
    }
}

fn live_poll_adapter_sync_ready_after_replay(replay_cursors_complete: Result<bool>) -> bool {
    match replay_cursors_complete {
        Ok(complete) => complete,
        Err(error) => {
            warn!(
                service = "indexer",
                command = "poll",
                error = ?error,
                "failed to check normalized replay cursor completion; live adapter sync remains disabled"
            );
            false
        }
    }
}

fn live_poll_adapter_sync_after_backlog_attempt(
    adapter_sync_enabled: bool,
    backlog_sync_failed: bool,
) -> bool {
    adapter_sync_enabled && !backlog_sync_failed
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::{
        live_poll_adapter_sync_after_backlog_attempt, live_poll_adapter_sync_ready_after_replay,
    };

    #[test]
    fn live_poll_adapter_sync_waits_on_replay_cursor_load_errors() {
        assert!(!live_poll_adapter_sync_ready_after_replay(Err(anyhow!(
            "transient database timeout"
        ))));
    }

    #[test]
    fn live_poll_adapter_sync_waits_when_backlog_sync_fails() {
        assert!(!live_poll_adapter_sync_after_backlog_attempt(true, true));
    }
}
