use anyhow::{Result, bail};
use bigname_storage::{
    BASE_NORMALIZED_REDERIVE_ADAPTER, BASE_NORMALIZED_REDERIVE_BACKLOG_CURSOR_KIND,
    BASE_NORMALIZED_REDERIVE_CHAIN_ID, BASE_NORMALIZED_REDERIVE_CURSOR_KIND,
    BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER, BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
    BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_ADAPTER, BaseNormalizedRederiveCounts,
    BaseNormalizedRederiveExpectedCounts, BaseNormalizedRederivePlan,
    base_normalized_rederive_scope_rules, execute_base_normalized_rederive_drop,
    load_base_normalized_rederive_plan,
};
use tracing::info;

use crate::cli::DropAndRederiveBaseNormalizedEventsArgs;

pub(crate) async fn drop_and_rederive_base_normalized_events_command(
    args: DropAndRederiveBaseNormalizedEventsArgs,
) -> Result<()> {
    if args.execute && !args.confirm_ratified_2026_07_03 {
        bail!("--execute requires --confirm-ratified-2026-07-03");
    }
    if args.execute && args.replay_target_block.is_none() {
        bail!("--execute requires --replay-target-block from reviewed dry-run output");
    }
    let expected_counts = expected_counts_from_args(&args)?;
    if args.execute && expected_counts.is_none() {
        bail!("--execute requires every --expected-* count emitted by dry-run");
    }
    let pool = bigname_storage::connect(&args.database).await?;
    let dry_run = !args.execute;

    let plan = load_base_normalized_rederive_plan(
        &pool,
        &args.deployment_profile,
        args.replay_target_block,
    )
    .await?;
    print!("{}", render_plan(&plan, dry_run));
    log_plan(&plan, dry_run);

    if dry_run {
        return Ok(());
    }

    let outcome = execute_base_normalized_rederive_drop(
        &pool,
        &args.deployment_profile,
        args.replay_target_block,
        BaseNormalizedRederiveExpectedCounts {
            counts: expected_counts.expect("execute path requires expected counts"),
        },
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
        "Base normalized-event drop-and-rederive corpus correction completed"
    );
    Ok(())
}

fn expected_counts_from_args(
    args: &DropAndRederiveBaseNormalizedEventsArgs,
) -> Result<Option<BaseNormalizedRederiveCounts>> {
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
    if values.iter().all(Option::is_none) {
        return Ok(None);
    }
    if values.iter().any(Option::is_none) {
        bail!(
            "expected-count execution guard requires every --expected-* count emitted by dry-run"
        );
    }
    Ok(Some(BaseNormalizedRederiveCounts {
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
    }))
}

fn render_plan(plan: &BaseNormalizedRederivePlan, dry_run: bool) -> String {
    let mut output = String::new();
    output.push_str("Base normalized-event drop-and-rederive plan\n");
    output.push_str(&format!(
        "mode: {}\n",
        if dry_run { "dry-run" } else { "execute" }
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
    output.push_str(&format!(
        "raw_fact_completeness: {:?} complete_for_execute={}\n",
        plan.raw_fact_completeness,
        plan.raw_fact_completeness.is_complete_for_rerun()
    ));
    output
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

fn log_plan(plan: &BaseNormalizedRederivePlan, dry_run: bool) {
    info!(
        service = "indexer",
        command = "drop-and-rederive-base-normalized-events",
        dry_run,
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
        }
    }

    #[test]
    fn expected_counts_require_complete_dry_run_census() {
        assert!(
            expected_counts_from_args(&args_with_expected(None))
                .unwrap()
                .is_none()
        );
        let counts = expected_counts_from_args(&args_with_expected(Some(7)))
            .unwrap()
            .expect("complete expected counts should build guard");
        assert_eq!(counts.normalized_events, 7);

        let mut incomplete = args_with_expected(Some(1));
        incomplete.expected_resources = None;
        assert!(
            format!("{:?}", expected_counts_from_args(&incomplete).unwrap_err())
                .contains("requires every --expected-* count")
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

        let output = render_plan(&plan, true);

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
    }
}
