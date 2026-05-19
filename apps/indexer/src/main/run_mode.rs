use crate::{backfill::BackfillAdapterSyncMode, runtime::RuntimeWatchScope};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexerRunMode {
    pub(crate) adapter_sync_mode: BackfillAdapterSyncMode,
    pub(crate) runtime_watch_scope: RuntimeWatchScope,
    pub(crate) startup_backfill_adapter_sync_mode: BackfillAdapterSyncMode,
    pub(crate) sync_adapter_before_startup_backfill: bool,
    pub(crate) sync_adapter_after_startup_backfill: bool,
    pub(crate) normalized_replay_catchup_enabled: bool,
    pub(crate) live_poll_adapter_sync_enabled: bool,
    pub(crate) live_poll_adapter_sync_after_normalized_replay_catchup: bool,
    pub(crate) broad_runtime_refresh_enabled: bool,
}

impl IndexerRunMode {
    pub(crate) fn new(
        adapter_sync_mode: BackfillAdapterSyncMode,
        normalized_replay_catchup_requested: bool,
    ) -> Self {
        let normalized_replay_catchup_enabled = normalized_replay_catchup_requested
            && adapter_sync_mode == BackfillAdapterSyncMode::Auto;
        let runtime_watch_scope = match adapter_sync_mode {
            BackfillAdapterSyncMode::Inline => RuntimeWatchScope::ActiveWatchedChain,
            BackfillAdapterSyncMode::Auto | BackfillAdapterSyncMode::RawOnly => {
                RuntimeWatchScope::ManifestDeclaredOnly
            }
        };
        let live_poll_adapter_sync_enabled = adapter_sync_mode != BackfillAdapterSyncMode::RawOnly
            && !(adapter_sync_mode == BackfillAdapterSyncMode::Auto
                && normalized_replay_catchup_enabled);
        let live_poll_adapter_sync_after_normalized_replay_catchup =
            adapter_sync_mode == BackfillAdapterSyncMode::Auto && normalized_replay_catchup_enabled;

        Self {
            adapter_sync_mode,
            runtime_watch_scope,
            startup_backfill_adapter_sync_mode: adapter_sync_mode
                .startup_hash_pinned_backfill_mode(),
            sync_adapter_before_startup_backfill: adapter_sync_mode
                == BackfillAdapterSyncMode::Inline,
            sync_adapter_after_startup_backfill: false,
            normalized_replay_catchup_enabled,
            live_poll_adapter_sync_enabled,
            live_poll_adapter_sync_after_normalized_replay_catchup,
            broad_runtime_refresh_enabled: adapter_sync_mode == BackfillAdapterSyncMode::Inline,
        }
    }
}
