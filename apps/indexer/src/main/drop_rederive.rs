use anyhow::{Result, bail};
use bigname_storage::{
    BASE_NORMALIZED_REDERIVE_ADAPTER, BASE_NORMALIZED_REDERIVE_CHAIN_ID,
    BASE_NORMALIZED_REDERIVE_CURSOR_KIND, BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER,
    BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK, BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK,
    BaseNormalizedRederiveCounts, BaseNormalizedRederiveExpectedCounts, BaseNormalizedRederivePlan,
    execute_base_normalized_rederive_drop, load_base_normalized_rederive_plan,
};
use tracing::info;

use crate::cli::DropAndRederiveBaseNormalizedEventsArgs;

pub(crate) async fn drop_and_rederive_base_normalized_events_command(
    args: DropAndRederiveBaseNormalizedEventsArgs,
) -> Result<()> {
    if args.execute && !args.confirm_ratified_2026_07_03 {
        bail!("--execute requires --confirm-ratified-2026-07-03");
    }
    let expected_counts = expected_counts_from_args(&args)?;
    if args.execute && expected_counts.is_none() {
        bail!("--execute requires every --expected-* count emitted by dry-run");
    }
    let pool = bigname_storage::connect(&args.database).await?;
    let dry_run = !args.execute;

    let plan = load_base_normalized_rederive_plan(&pool, &args.deployment_profile).await?;
    print!("{}", render_plan(&plan, dry_run));
    log_plan(&plan, dry_run);

    if dry_run {
        return Ok(());
    }

    let outcome = execute_base_normalized_rederive_drop(
        &pool,
        &args.deployment_profile,
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
        reset_replay_start_block = BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
        reset_replay_target_block = BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK,
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
        "scope: chain_id={} source_manifest_ids=[1,2,4,5] block_range={}..{} exclude_derivation_kind=[manifest_sync,manifest_alert]\n",
        BASE_NORMALIZED_REDERIVE_CHAIN_ID,
        BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
        BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK
    ));
    output.push_str(&format!(
        "replay_reset: deployment_profile={} cursor_kind={} checkpoint_adapters=[{},{}] next_block={} target_block={}\n",
        plan.deployment_profile,
        BASE_NORMALIZED_REDERIVE_CURSOR_KIND,
        BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER,
        BASE_NORMALIZED_REDERIVE_ADAPTER,
        BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
        BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK
    ));
    output.push_str("manifest_confirmation:\n");
    for manifest in &plan.manifests {
        output.push_str(&format!(
            "  {} {} namespace={} chain={} rollout={} file={}\n",
            manifest.manifest_id,
            manifest.source_family,
            manifest.namespace,
            manifest.chain,
            manifest.rollout_status,
            manifest.file_path
        ));
    }
    output.push_str("family_census:\n");
    for family in &plan.family_census {
        output.push_str(&format!(
            "  {} {} rows={} min_block={:?} max_block={:?}\n",
            family.source_manifest_id,
            family.source_family,
            family.row_count,
            family.min_block_number,
            family.max_block_number
        ));
    }
    output.push_str(&format!("delete_census: {:?}\n", plan.counts));
    output.push_str(&format!(
        "raw_fact_completeness: {:?} complete_for_execute={}\n",
        plan.raw_fact_completeness,
        plan.raw_fact_completeness.is_complete_for_rerun()
    ));
    output
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
        replay_start_block = BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
        replay_target_block = BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK,
        raw_fact_complete = plan.raw_fact_completeness.is_complete_for_rerun(),
        "Base normalized-event drop-and-rederive census"
    );
    for family in &plan.family_census {
        info!(
            service = "indexer",
            command = "drop-and-rederive-base-normalized-events",
            dry_run,
            source_manifest_id = family.source_manifest_id,
            source_family = %family.source_family,
            row_count = family.row_count,
            min_block = family.min_block_number,
            max_block = family.max_block_number,
            "Base normalized-event drop-and-rederive family census"
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
}
