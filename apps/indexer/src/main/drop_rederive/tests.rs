use super::*;
use bigname_storage::{BaseNormalizedRederiveRawFactRangeProof, DatabaseConfig};

fn args_with_expected(count: Option<i64>) -> DropAndRederiveBaseNormalizedEventsArgs {
    DropAndRederiveBaseNormalizedEventsArgs {
        database: DatabaseConfig::default(),
        deployment_profile: "mainnet".to_owned(),
        dry_run: false,
        execute: true,
        confirm_ratified_2026_07_03: true,
        run_id: "base-normalized-rederive-2026-07-03".to_owned(),
        batch_size: 100_000,
        replay_target_block: Some(1),
        expected_normalized_events: count,
        expected_resources: count,
        expected_token_lineages: count,
        expected_name_surfaces: count,
        expected_surface_bindings: count,
        expected_name_current: count,
        expected_address_names_current: count,
        expected_children_current: count,
        expected_permissions_current: count,
        expected_record_inventory_current: count,
        expected_projection_normalized_event_changes: count,
        expected_current_projection_replay_status: count,
        expected_replay_cursor_rows: count,
        expected_adapter_checkpoint_rows: count,
        expected_adapter_checkpoint_item_rows: count,
        expected_active_replay_target_snapshot_digest: count
            .map(|_| "keccak256:reviewed".to_owned()),
        expected_active_manifest_snapshot_digest: count
            .map(|_| "keccak256:manifest-reviewed".to_owned()),
    }
}

#[test]
fn expected_counts_require_complete_dry_run_census() {
    assert!(
        expected_from_args(&args_with_expected(None))
            .unwrap()
            .is_none()
    );
    let expected = expected_from_args(&args_with_expected(Some(7)))
        .unwrap()
        .expect("complete expected counts should build guard");
    assert_eq!(expected.counts.normalized_events, 7);
    assert_eq!(
        expected.active_replay_target_snapshot_digest.as_deref(),
        Some("keccak256:reviewed")
    );
    assert_eq!(
        expected.active_manifest_snapshot_digest.as_deref(),
        Some("keccak256:manifest-reviewed")
    );
    let mut incomplete = args_with_expected(Some(1));
    incomplete.expected_resources = None;
    assert!(
        format!("{:?}", expected_from_args(&incomplete).unwrap_err())
            .contains("requires every --expected-* count")
    );
    let mut missing_digest = args_with_expected(Some(1));
    missing_digest.expected_active_replay_target_snapshot_digest = None;
    assert!(
        format!("{:?}", expected_from_args(&missing_digest).unwrap_err())
            .contains("--expected-active-replay-target-snapshot-digest")
    );
    let mut missing_manifest_digest = args_with_expected(Some(1));
    missing_manifest_digest.expected_active_manifest_snapshot_digest = None;
    assert!(
        format!(
            "{:?}",
            expected_from_args(&missing_manifest_digest).unwrap_err()
        )
        .contains("--expected-active-manifest-snapshot-digest")
    );
}

#[tokio::test]
async fn execute_requires_reviewed_dry_run_census() {
    let error = drop_and_rederive_base_normalized_events_command(args_with_expected(None))
        .await
        .expect_err("execute must require reviewed dry-run counts before connecting");
    assert!(format!("{error:?}").contains("--execute requires every --expected-* count"));
}

#[tokio::test]
async fn execute_requires_reviewed_replay_target_block() {
    let mut args = args_with_expected(Some(1));
    args.replay_target_block = None;

    let error = drop_and_rederive_base_normalized_events_command(args)
        .await
        .expect_err("execute must require reviewed dry-run target before connecting");
    assert!(format!("{error:?}").contains("--execute requires --replay-target-block"));
}

#[test]
fn render_plan_reports_source_family_partition_and_both_cursors() {
    let target_block = BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 42;
    let plan = BaseNormalizedRederivePlan {
        deployment_profile: "mainnet".to_owned(),
        replay_target_block: target_block,
        max_affected_block: Some(target_block),
        replay_target_floor_block: Some(target_block),
        active_replay_target_snapshot: vec![],
        active_manifest_snapshot: vec![],
        raw_fact_range_proof: BaseNormalizedRederiveRawFactRangeProof::default(),
        raw_fact_safety_checks_deferred: false,
        derivation_kind_census: vec![
            bigname_storage::BaseNormalizedRederiveDerivationKindCensus {
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                row_count: 56_040_812,
                min_block_number: Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK),
                max_block_number: Some(target_block),
                rederivable: true,
            },
            bigname_storage::BaseNormalizedRederiveDerivationKindCensus {
                derivation_kind: "raw_log_preimage_observation".to_owned(),
                source_family: "basenames_l1_compat".to_owned(),
                row_count: 64,
                min_block_number: Some(46_923_016),
                max_block_number: Some(46_927_167),
                rederivable: false,
            },
        ],
        ratified_dropped_orphan_emitter_census: vec![
            bigname_storage::BaseNormalizedRederiveRatifiedDroppedEmitterCensus {
                derivation_kind: "ens_v1_reverse_claim".to_owned(),
                source_family: "basenames_base_primary".to_owned(),
                emitting_address: "0x79ea96012eea67a83431f1701b3dff7e37f9e282".to_owned(),
                row_count: 3_939_502,
                min_block_number: Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK),
                max_block_number: Some(46_903_158),
                ratification: "2026-07-05 option A".to_owned(),
                reason: "deprecated legacy Basenames ReverseRegistrar superseded by ENS Base L2ReverseRegistrar; rows deliberately dropped, not re-derived".to_owned(),
            },
        ],
        cursor_census: bigname_storage::BaseNormalizedRederiveCursorCensus {
            raw_fact_replay_cursor_rows: 1,
            post_replay_live_adapter_backlog_cursor_rows: 1,
        },
        counts: BaseNormalizedRederiveCounts {
            normalized_events: 56_040_812,
            replay_cursor_rows: 2,
            ..BaseNormalizedRederiveCounts::default()
        },
        raw_fact_completeness: bigname_storage::BaseNormalizedRederiveRawFactCompleteness {
            replay_target_block: target_block,
            log_derived_event_count: 44_000_000,
            missing_log_derived_raw_fact_count: 0,
            boundary_event_count: 12_000_000,
            missing_boundary_lineage_count: 0,
            canonical_raw_log_min_block: Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK),
            canonical_raw_log_max_block: Some(target_block),
            canonical_raw_log_head_block: Some(target_block),
        },
    };

    let output = render_plan(&plan, true, "reviewed-run", 100_000).unwrap();

    assert!(output.contains("run_state: run_id=reviewed-run batch_size=100000"));
    assert!(output.contains("source_families=[ens_v1_reverse_l1,basenames_base_primary]"));
    assert!(output.contains(
        "delete derivation_kind=ens_v1_unwrapped_authority source_family=basenames_base_registry"
    ));
    assert!(output.contains(
        "keep derivation_kind=raw_log_preimage_observation source_family=basenames_l1_compat"
    ));
    assert!(output.contains("ratified_dropped_orphan_emitters:"));
    assert!(output.contains("drop_not_rederived ratification=2026-07-05 option A derivation_kind=ens_v1_reverse_claim source_family=basenames_base_primary emitting_address=0x79ea96012eea67a83431f1701b3dff7e37f9e282 rows=3939502"));
    assert!(output.contains("max_block=Some(46903158)"));
    assert!(output.contains("clear_cursor=post_replay_live_adapter_backlog"));
    assert!(output.contains(&format!("target_block={target_block}")));
    assert!(output.contains(&format!("max_affected_block=Some({target_block})")));
    assert!(output.contains(&format!("replay_target_floor_block=Some({target_block})")));
    assert!(output.contains("active_replay_target_snapshot: rows=0"));
    assert!(output.contains("expected_active_replay_target_snapshot_digest=keccak256:"));
    assert!(output.contains("active_manifest_snapshot: rows=0"));
    assert!(output.contains("expected_active_manifest_snapshot_digest=keccak256:"));
    assert!(output.contains("batch_plan:"));
    assert!(output.contains("step=normalized_events rows=56040812 estimated_batches=561"));
}
