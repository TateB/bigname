use anyhow::{Result, bail};
use bigname_storage::{
    BASE_NORMALIZED_REDERIVE_ADAPTER, BASE_NORMALIZED_REDERIVE_BACKLOG_CURSOR_KIND,
    BASE_NORMALIZED_REDERIVE_CHAIN_ID, BASE_NORMALIZED_REDERIVE_CURSOR_KIND,
    BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER, BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
    BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_ADAPTER, BaseNormalizedRederiveBatchPlan,
    BaseNormalizedRederiveCounts, BaseNormalizedRederiveExpectedCounts, BaseNormalizedRederivePlan,
    DEFAULT_BASE_NORMALIZED_REDERIVE_BATCH_SIZE, DatabaseConfig,
    base_normalized_rederive_json_digest, base_normalized_rederive_scope_rules,
    execute_base_normalized_rederive_drop, load_base_normalized_rederive_plan,
};
use clap::Args;
use tracing::info;

#[derive(Args, Debug)]
pub(crate) struct DropAndRederiveBaseNormalizedEventsArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_DEPLOYMENT_PROFILE",
        default_value = "mainnet"
    )]
    pub(crate) deployment_profile: String,
    #[arg(long, conflicts_with = "execute")]
    pub(crate) dry_run: bool,
    #[arg(long)]
    pub(crate) execute: bool,
    #[arg(long = "confirm-ratified-2026-07-03", requires = "execute")]
    pub(crate) confirm_ratified_2026_07_03: bool,
    #[arg(long = "run-id", default_value = "base-normalized-rederive-2026-07-03")]
    pub(crate) run_id: String,
    #[arg(long = "batch-size", default_value_t = DEFAULT_BASE_NORMALIZED_REDERIVE_BATCH_SIZE)]
    pub(crate) batch_size: i64,
    #[arg(long = "replay-target-block")]
    pub(crate) replay_target_block: Option<i64>,
    #[arg(long = "expected-normalized-events")]
    pub(crate) expected_normalized_events: Option<i64>,
    #[arg(long = "expected-resources")]
    pub(crate) expected_resources: Option<i64>,
    #[arg(long = "expected-token-lineages")]
    pub(crate) expected_token_lineages: Option<i64>,
    #[arg(long = "expected-name-surfaces")]
    pub(crate) expected_name_surfaces: Option<i64>,
    #[arg(long = "expected-surface-bindings")]
    pub(crate) expected_surface_bindings: Option<i64>,
    #[arg(long = "expected-name-current")]
    pub(crate) expected_name_current: Option<i64>,
    #[arg(long = "expected-address-names-current")]
    pub(crate) expected_address_names_current: Option<i64>,
    #[arg(long = "expected-children-current")]
    pub(crate) expected_children_current: Option<i64>,
    #[arg(long = "expected-permissions-current")]
    pub(crate) expected_permissions_current: Option<i64>,
    #[arg(long = "expected-record-inventory-current")]
    pub(crate) expected_record_inventory_current: Option<i64>,
    #[arg(long = "expected-projection-normalized-event-changes")]
    pub(crate) expected_projection_normalized_event_changes: Option<i64>,
    #[arg(long = "expected-current-projection-replay-status")]
    pub(crate) expected_current_projection_replay_status: Option<i64>,
    #[arg(long = "expected-replay-cursor-rows")]
    pub(crate) expected_replay_cursor_rows: Option<i64>,
    #[arg(long = "expected-adapter-checkpoint-rows")]
    pub(crate) expected_adapter_checkpoint_rows: Option<i64>,
    #[arg(long = "expected-adapter-checkpoint-item-rows")]
    pub(crate) expected_adapter_checkpoint_item_rows: Option<i64>,
    #[arg(long = "expected-active-replay-target-snapshot-digest")]
    pub(crate) expected_active_replay_target_snapshot_digest: Option<String>,
    #[arg(long = "expected-active-manifest-snapshot-digest")]
    pub(crate) expected_active_manifest_snapshot_digest: Option<String>,
}

pub(crate) async fn drop_and_rederive_base_normalized_events_command(
    args: DropAndRederiveBaseNormalizedEventsArgs,
) -> Result<()> {
    if args.execute && !args.confirm_ratified_2026_07_03 {
        bail!("--execute requires --confirm-ratified-2026-07-03");
    }
    if args.execute && args.replay_target_block.is_none() {
        bail!("--execute requires --replay-target-block from reviewed dry-run output");
    }
    if args.execute && args.run_id.trim().is_empty() {
        bail!("--execute requires a non-empty --run-id");
    }
    if args.execute && args.batch_size <= 0 {
        bail!("--execute requires a positive --batch-size");
    }
    let expected = expected_from_args(&args)?;
    if args.execute && expected.is_none() {
        bail!(
            "--execute requires every --expected-* count plus --expected-active-replay-target-snapshot-digest and --expected-active-manifest-snapshot-digest emitted by dry-run"
        );
    }
    let pool = bigname_storage::connect(&args.database).await?;
    let dry_run = !args.execute;

    let plan = load_base_normalized_rederive_plan(
        &pool,
        &args.deployment_profile,
        args.replay_target_block,
    )
    .await?;
    print!(
        "{}",
        render_plan(&plan, dry_run, &args.run_id, args.batch_size)?
    );
    log_plan(&plan, dry_run, &args.run_id, args.batch_size)?;

    if dry_run {
        return Ok(());
    }

    let outcome = execute_base_normalized_rederive_drop(
        &pool,
        &args.deployment_profile,
        &args.run_id,
        args.batch_size,
        args.replay_target_block,
        expected.expect("execute path requires expected counts"),
    )
    .await?;
    info!(
        service = "indexer",
        command = "drop-and-rederive-base-normalized-events",
        correction_event = "2026-07-03 Base normalized-event corpus correction",
        cause = "multiple derivation/manifest changes over outage: 12bcea0 registry-only authority; resolver proxy 0x426f to implementation 0xC6d",
        method = "drop scoped normalized events and identity rows, reset full-closure replay from retained raw facts",
        ratified = "2026-07-03",
        deleted_normalized_events = outcome.deleted.normalized_events,
        deleted_resources = outcome.deleted.resources,
        deleted_token_lineages = outcome.deleted.token_lineages,
        deleted_name_surfaces = outcome.deleted.name_surfaces,
        deleted_surface_bindings = outcome.deleted.surface_bindings,
        deleted_projection_normalized_event_changes =
            outcome.deleted.projection_normalized_event_changes,
        reset_current_projection_replay_status_rows =
            outcome.deleted.current_projection_replay_status,
        reset_replay_start_block = BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
        reset_replay_target_block = outcome.plan.replay_target_block,
        run_id = %args.run_id,
        batch_size = args.batch_size,
        "Base normalized-event drop-and-rederive corpus correction completed"
    );
    Ok(())
}

fn expected_from_args(
    args: &DropAndRederiveBaseNormalizedEventsArgs,
) -> Result<Option<BaseNormalizedRederiveExpectedCounts>> {
    let values = [
        args.expected_normalized_events,
        args.expected_resources,
        args.expected_token_lineages,
        args.expected_name_surfaces,
        args.expected_surface_bindings,
        args.expected_name_current,
        args.expected_address_names_current,
        args.expected_children_current,
        args.expected_permissions_current,
        args.expected_record_inventory_current,
        args.expected_projection_normalized_event_changes,
        args.expected_current_projection_replay_status,
        args.expected_replay_cursor_rows,
        args.expected_adapter_checkpoint_rows,
        args.expected_adapter_checkpoint_item_rows,
    ];
    if values.iter().all(Option::is_none)
        && args.expected_active_replay_target_snapshot_digest.is_none()
        && args.expected_active_manifest_snapshot_digest.is_none()
    {
        return Ok(None);
    }
    if values.iter().any(Option::is_none)
        || args
            .expected_active_replay_target_snapshot_digest
            .as_deref()
            .is_none_or(str::is_empty)
        || args
            .expected_active_manifest_snapshot_digest
            .as_deref()
            .is_none_or(str::is_empty)
    {
        bail!(
            "expected execution guard requires every --expected-* count plus --expected-active-replay-target-snapshot-digest and --expected-active-manifest-snapshot-digest emitted by dry-run"
        );
    }
    Ok(Some(BaseNormalizedRederiveExpectedCounts {
        counts: BaseNormalizedRederiveCounts {
            normalized_events: args.expected_normalized_events.unwrap_or_default(),
            resources: args.expected_resources.unwrap_or_default(),
            token_lineages: args.expected_token_lineages.unwrap_or_default(),
            name_surfaces: args.expected_name_surfaces.unwrap_or_default(),
            surface_bindings: args.expected_surface_bindings.unwrap_or_default(),
            name_current: args.expected_name_current.unwrap_or_default(),
            address_names_current: args.expected_address_names_current.unwrap_or_default(),
            children_current: args.expected_children_current.unwrap_or_default(),
            permissions_current: args.expected_permissions_current.unwrap_or_default(),
            record_inventory_current: args.expected_record_inventory_current.unwrap_or_default(),
            projection_normalized_event_changes: args
                .expected_projection_normalized_event_changes
                .unwrap_or_default(),
            current_projection_replay_status: args
                .expected_current_projection_replay_status
                .unwrap_or_default(),
            replay_cursor_rows: args.expected_replay_cursor_rows.unwrap_or_default(),
            adapter_checkpoint_rows: args.expected_adapter_checkpoint_rows.unwrap_or_default(),
            adapter_checkpoint_item_rows: args
                .expected_adapter_checkpoint_item_rows
                .unwrap_or_default(),
        },
        active_replay_target_snapshot_digest: args
            .expected_active_replay_target_snapshot_digest
            .clone(),
        active_manifest_snapshot_digest: args.expected_active_manifest_snapshot_digest.clone(),
    }))
}

fn render_plan(
    plan: &BaseNormalizedRederivePlan,
    dry_run: bool,
    run_id: &str,
    batch_size: i64,
) -> Result<String> {
    let mut output = String::new();
    let batch_plan =
        BaseNormalizedRederiveBatchPlan::from_counts(run_id, batch_size, &plan.counts)?;
    output.push_str("Base normalized-event drop-and-rederive plan\n");
    output.push_str(&format!(
        "mode: {}\n",
        if dry_run { "dry-run" } else { "execute" }
    ));
    output.push_str(&format!(
        "run_state: run_id={} batch_size={} estimated_batches={}\n",
        batch_plan.run_id, batch_plan.batch_size, batch_plan.estimated_total_batches
    ));
    output.push_str(&format!(
        "scope: chain_id={} block_range={}..{} block_hash_not_null=true rederivable_rules={}\n",
        BASE_NORMALIZED_REDERIVE_CHAIN_ID,
        BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
        plan.replay_target_block,
        render_scope_rules()
    ));
    output.push_str(&format!(
        "identity_scope: chain_id={} provenance.adapter={}\n",
        BASE_NORMALIZED_REDERIVE_CHAIN_ID, BASE_NORMALIZED_REDERIVE_ADAPTER
    ));
    output.push_str(&format!(
        "replay_reset: deployment_profile={} reset_cursor={} clear_cursor={} checkpoint_adapters=[{},{},{}] next_block={} target_block={}\n",
        plan.deployment_profile,
        BASE_NORMALIZED_REDERIVE_CURSOR_KIND,
        BASE_NORMALIZED_REDERIVE_BACKLOG_CURSOR_KIND,
        BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_ADAPTER,
        BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER,
        BASE_NORMALIZED_REDERIVE_ADAPTER,
        BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
        plan.replay_target_block
    ));
    output.push_str(&format!(
        "target_validation: max_affected_block={:?} replay_target_floor_block={:?} canonical_raw_log_head={:?}\n",
        plan.max_affected_block,
        plan.replay_target_floor_block,
        plan.raw_fact_completeness.canonical_raw_log_head_block
    ));
    let active_replay_target_snapshot_digest =
        base_normalized_rederive_json_digest(&plan.active_replay_target_snapshot)?;
    output.push_str(&format!(
        "active_replay_target_snapshot: rows={} expected_active_replay_target_snapshot_digest={}\n",
        plan.active_replay_target_snapshot.len(),
        active_replay_target_snapshot_digest
    ));
    let active_manifest_snapshot_digest =
        base_normalized_rederive_json_digest(&plan.active_manifest_snapshot)?;
    output.push_str(&format!(
        "active_manifest_snapshot: rows={} expected_active_manifest_snapshot_digest={}\n",
        plan.active_manifest_snapshot.len(),
        active_manifest_snapshot_digest
    ));
    output.push_str("derivation_kind_partition:\n");
    for census in plan
        .derivation_kind_census
        .iter()
        .filter(|census| census.rederivable)
    {
        output.push_str(&format!(
            "  delete derivation_kind={} source_family={} rows={} min_block={:?} max_block={:?}\n",
            census.derivation_kind,
            census.source_family,
            census.row_count,
            census.min_block_number,
            census.max_block_number
        ));
    }
    let mut kept_any = false;
    for census in plan
        .derivation_kind_census
        .iter()
        .filter(|census| !census.rederivable)
    {
        kept_any = true;
        output.push_str(&format!(
            "  keep derivation_kind={} source_family={} rows={} min_block={:?} max_block={:?}\n",
            census.derivation_kind,
            census.source_family,
            census.row_count,
            census.min_block_number,
            census.max_block_number
        ));
    }
    if !kept_any {
        output.push_str("  keep none rows=0\n");
    }
    output.push_str(&format!(
        "cursor_census: {}={} {}={} expected_replay_cursor_rows={}\n",
        BASE_NORMALIZED_REDERIVE_CURSOR_KIND,
        plan.cursor_census.raw_fact_replay_cursor_rows,
        BASE_NORMALIZED_REDERIVE_BACKLOG_CURSOR_KIND,
        plan.cursor_census
            .post_replay_live_adapter_backlog_cursor_rows,
        plan.cursor_census.total_cursor_rows()
    ));
    output.push_str(&format!("delete_census: {:?}\n", plan.counts));
    output.push_str("batch_plan:\n");
    for step in batch_plan.steps {
        output.push_str(&format!(
            "  step={} rows={} estimated_batches={}\n",
            step.step, step.rows, step.estimated_batches
        ));
    }
    output.push_str(&format!(
        "raw_fact_completeness: {:?} complete_for_execute={}\n",
        plan.raw_fact_completeness,
        plan.raw_fact_completeness.is_complete_for_rerun()
    ));
    Ok(output)
}

fn render_scope_rules() -> String {
    base_normalized_rederive_scope_rules()
        .iter()
        .map(|rule| {
            format!(
                "{}:derivation_kinds=[{}]:source_families=[{}]",
                rule.adapter,
                rule.derivation_kinds.join(","),
                rule.source_families.join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn log_plan(
    plan: &BaseNormalizedRederivePlan,
    dry_run: bool,
    run_id: &str,
    batch_size: i64,
) -> Result<()> {
    let batch_plan =
        BaseNormalizedRederiveBatchPlan::from_counts(run_id, batch_size, &plan.counts)?;
    info!(
        service = "indexer",
        command = "drop-and-rederive-base-normalized-events",
        dry_run,
        run_id,
        batch_size,
        estimated_batches = batch_plan.estimated_total_batches,
        chain = BASE_NORMALIZED_REDERIVE_CHAIN_ID,
        deployment_profile = %plan.deployment_profile,
        normalized_events = plan.counts.normalized_events,
        resources = plan.counts.resources,
        token_lineages = plan.counts.token_lineages,
        name_surfaces = plan.counts.name_surfaces,
        surface_bindings = plan.counts.surface_bindings,
        projection_normalized_event_changes = plan.counts.projection_normalized_event_changes,
        current_projection_replay_status = plan.counts.current_projection_replay_status,
        replay_cursor_rows = plan.counts.replay_cursor_rows,
        replay_raw_cursor_rows = plan.cursor_census.raw_fact_replay_cursor_rows,
        replay_backlog_cursor_rows = plan
            .cursor_census
            .post_replay_live_adapter_backlog_cursor_rows,
        replay_start_block = BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
        replay_target_block = plan.replay_target_block,
        active_replay_target_digest = %base_normalized_rederive_json_digest(
            &plan.active_replay_target_snapshot
        )?,
        active_manifest_digest = %base_normalized_rederive_json_digest(
            &plan.active_manifest_snapshot
        )?,
        active_replay_target_rows = plan.active_replay_target_snapshot.len(),
        active_manifest_rows = plan.active_manifest_snapshot.len(),
        raw_fact_complete = plan.raw_fact_completeness.is_complete_for_rerun(),
        "Base normalized-event drop-and-rederive census"
    );
    for census in &plan.derivation_kind_census {
        info!(
            service = "indexer",
            command = "drop-and-rederive-base-normalized-events",
            dry_run,
            derivation_kind = %census.derivation_kind,
            source_family = %census.source_family,
            row_count = census.row_count,
            min_block = census.min_block_number,
            max_block = census.max_block_number,
            rederivable = census.rederivable,
            "Base normalized-event drop-and-rederive derivation-kind census"
        );
    }
    for step in batch_plan.steps {
        info!(
            service = "indexer",
            command = "drop-and-rederive-base-normalized-events",
            dry_run,
            run_id,
            step = step.step,
            rows = step.rows,
            estimated_batches = step.estimated_batches,
            "Base normalized-event drop-and-rederive batch plan"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_storage::DatabaseConfig;

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
            raw_fact_range_proof: bigname_storage::BaseNormalizedRederiveRawFactRangeProof {
                replay_target_block: target_block,
                canonical_raw_log_count: 2,
                canonical_raw_log_checksum: "0".to_owned(),
                canonical_lineage_count: 3,
                canonical_lineage_checksum: "0".to_owned(),
            },
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
        assert!(
            output.contains(
                "delete derivation_kind=ens_v1_unwrapped_authority source_family=basenames_base_registry"
            )
        );
        assert!(output.contains(
            "keep derivation_kind=raw_log_preimage_observation source_family=basenames_l1_compat"
        ));
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
}
