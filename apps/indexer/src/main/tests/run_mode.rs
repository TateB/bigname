#[test]
fn auto_normalized_replay_catchup_owns_live_adapter_sync() {
    let auto_with_catchup = IndexerRunMode::new(BackfillAdapterSyncMode::Auto, true);
    assert!(!auto_with_catchup.live_poll_adapter_sync_enabled);
    assert!(auto_with_catchup.live_poll_adapter_sync_after_normalized_replay_catchup);
    assert!(auto_with_catchup.normalized_replay_catchup_enabled);
    assert_eq!(
        auto_with_catchup.runtime_watch_scope,
        RuntimeWatchScope::ManifestDeclaredOnly
    );
    assert_eq!(
        auto_with_catchup.startup_backfill_adapter_sync_mode,
        BackfillAdapterSyncMode::RawOnly
    );

    let auto_without_catchup = IndexerRunMode::new(BackfillAdapterSyncMode::Auto, false);
    assert!(auto_without_catchup.live_poll_adapter_sync_enabled);
    assert!(!auto_without_catchup.live_poll_adapter_sync_after_normalized_replay_catchup);
    assert!(!auto_without_catchup.normalized_replay_catchup_enabled);

    let inline = IndexerRunMode::new(BackfillAdapterSyncMode::Inline, true);
    assert!(inline.live_poll_adapter_sync_enabled);
    assert!(!inline.live_poll_adapter_sync_after_normalized_replay_catchup);
    assert!(!inline.normalized_replay_catchup_enabled);
    assert_eq!(
        inline.runtime_watch_scope,
        RuntimeWatchScope::ActiveWatchedChain
    );
    assert_eq!(
        inline.startup_backfill_adapter_sync_mode,
        BackfillAdapterSyncMode::Inline
    );
    assert!(inline.broad_runtime_refresh_enabled);
    assert!(inline.sync_adapter_before_startup_backfill);

    let raw_only = IndexerRunMode::new(BackfillAdapterSyncMode::RawOnly, false);
    assert!(!raw_only.live_poll_adapter_sync_enabled);
    assert!(!raw_only.live_poll_adapter_sync_after_normalized_replay_catchup);
    assert_eq!(
        raw_only.runtime_watch_scope,
        RuntimeWatchScope::ManifestDeclaredOnly
    );
}
