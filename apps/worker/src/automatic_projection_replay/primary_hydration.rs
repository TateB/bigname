use anyhow::Result;
use sqlx::PgPool;

use crate::primary_name;

pub(super) type LegacyReverseHydrationTriggerState =
    Option<Vec<primary_name::PrimaryNameLegacyReverseHydrationTrigger>>;

pub(super) async fn hydrate_after_bootstrap(
    pool: &PgPool,
    primary_hydration_config: Option<&primary_name::PrimaryNameLegacyReverseHydrationConfig>,
    last_trigger: &mut LegacyReverseHydrationTriggerState,
) -> Result<primary_name::PrimaryNameLegacyReverseHydrationSummary> {
    let trigger_before = match primary_hydration_config {
        Some(config) => {
            primary_name::load_legacy_reverse_resolver_call_triggers(pool, config).await?
        }
        None => Vec::new(),
    };
    let summary = hydrate(pool, primary_hydration_config).await?;
    if primary_hydration_config.is_some() && hydration_cause_consumed(&summary) {
        *last_trigger = Some(trigger_before);
    }
    Ok(summary)
}

pub(super) async fn hydrate_if_projection_changed_or_triggered(
    pool: &PgPool,
    primary_hydration_config: Option<&primary_name::PrimaryNameLegacyReverseHydrationConfig>,
    last_trigger: &mut LegacyReverseHydrationTriggerState,
    projection_apply_changed: &mut bool,
) -> Result<primary_name::PrimaryNameLegacyReverseHydrationSummary> {
    let Some(config) = primary_hydration_config else {
        return Ok(primary_name::PrimaryNameLegacyReverseHydrationSummary::default());
    };
    let current_trigger =
        primary_name::load_legacy_reverse_resolver_call_triggers(pool, config).await?;
    if !needs_hydration(last_trigger, &current_trigger, *projection_apply_changed) {
        return Ok(primary_name::PrimaryNameLegacyReverseHydrationSummary::default());
    }

    let summary = hydrate(pool, Some(config)).await?;
    if hydration_cause_consumed(&summary) {
        *last_trigger = Some(current_trigger);
        *projection_apply_changed = false;
    }
    Ok(summary)
}

async fn hydrate(
    pool: &PgPool,
    primary_hydration_config: Option<&primary_name::PrimaryNameLegacyReverseHydrationConfig>,
) -> Result<primary_name::PrimaryNameLegacyReverseHydrationSummary> {
    let Some(config) = primary_hydration_config else {
        return Ok(primary_name::PrimaryNameLegacyReverseHydrationSummary::default());
    };
    let summary =
        primary_name::hydrate_legacy_reverse_resolver_primary_names(pool, config.clone()).await?;
    if summary.candidate_tuple_count > 0 || summary.failed_lookup_count > 0 {
        primary_name::log_legacy_reverse_hydration_summary(&summary);
    }
    Ok(summary)
}

fn trigger_changed(
    last_trigger: &LegacyReverseHydrationTriggerState,
    current_trigger: &[primary_name::PrimaryNameLegacyReverseHydrationTrigger],
) -> bool {
    last_trigger.as_deref() != Some(current_trigger)
}

fn needs_hydration(
    last_trigger: &LegacyReverseHydrationTriggerState,
    current_trigger: &[primary_name::PrimaryNameLegacyReverseHydrationTrigger],
    projection_apply_changed: bool,
) -> bool {
    projection_apply_changed || trigger_changed(last_trigger, current_trigger)
}

pub(super) fn bootstrap_hydration_made_progress(
    summary: &primary_name::PrimaryNameLegacyReverseHydrationSummary,
) -> bool {
    hydration_cause_consumed(summary)
        || summary.upserted_row_count > 0
        || summary.deleted_row_count > 0
}

fn hydration_cause_consumed(
    summary: &primary_name::PrimaryNameLegacyReverseHydrationSummary,
) -> bool {
    summary.failed_lookup_count == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hydration_trigger_only_runs_on_observation_change() {
        let trigger = primary_name::PrimaryNameLegacyReverseHydrationTrigger {
            resolver_address: "0x0000000000000000000000000000000000000001".to_owned(),
            block_number: 10,
            block_hash: "0x10".to_owned(),
            transaction_hash: "0xtx10".to_owned(),
            transaction_index: 1,
        };
        let later_trigger = primary_name::PrimaryNameLegacyReverseHydrationTrigger {
            resolver_address: "0x0000000000000000000000000000000000000001".to_owned(),
            block_number: 11,
            block_hash: "0x11".to_owned(),
            transaction_hash: "0xtx11".to_owned(),
            transaction_index: 0,
        };
        let other_resolver_trigger = primary_name::PrimaryNameLegacyReverseHydrationTrigger {
            resolver_address: "0x0000000000000000000000000000000000000002".to_owned(),
            block_number: 7,
            block_hash: "0x07".to_owned(),
            transaction_hash: "0xtx07".to_owned(),
            transaction_index: 0,
        };

        assert!(trigger_changed(&None, &[]));
        assert!(!trigger_changed(&Some(Vec::new()), &[]));
        assert!(trigger_changed(&Some(Vec::new()), &[trigger.clone()]));
        assert!(!trigger_changed(
            &Some(vec![trigger.clone()]),
            &[trigger.clone()]
        ));
        assert!(trigger_changed(
            &Some(vec![trigger.clone()]),
            &[later_trigger.clone()]
        ));
        assert!(trigger_changed(&Some(vec![trigger.clone()]), &[]));
        assert!(trigger_changed(
            &Some(vec![trigger.clone()]),
            &[trigger, other_resolver_trigger]
        ));
    }

    #[test]
    fn bootstrap_snapshot_keeps_checkpoint_catchup_visible() {
        let trigger = primary_name::PrimaryNameLegacyReverseHydrationTrigger {
            resolver_address: "0x0000000000000000000000000000000000000001".to_owned(),
            block_number: 10,
            block_hash: "0x10".to_owned(),
            transaction_hash: "0xtx10".to_owned(),
            transaction_index: 1,
        };
        let pre_hydration_state = Some(Vec::new());

        assert!(trigger_changed(&pre_hydration_state, &[trigger]));
    }

    #[test]
    fn projection_apply_progress_forces_hydration_without_direct_call_trigger_change() {
        let trigger = primary_name::PrimaryNameLegacyReverseHydrationTrigger {
            resolver_address: "0x0000000000000000000000000000000000000001".to_owned(),
            block_number: 10,
            block_hash: "0x10".to_owned(),
            transaction_hash: "0xtx10".to_owned(),
            transaction_index: 1,
        };
        let last_trigger = Some(vec![trigger.clone()]);

        assert!(!needs_hydration(&last_trigger, &[trigger.clone()], false));
        assert!(needs_hydration(&last_trigger, &[trigger], true));
    }

    #[test]
    fn failed_lookup_keeps_hydration_cause_pending() {
        assert!(hydration_cause_consumed(
            &primary_name::PrimaryNameLegacyReverseHydrationSummary::default()
        ));

        let failed = primary_name::PrimaryNameLegacyReverseHydrationSummary {
            failed_lookup_count: 1,
            ..primary_name::PrimaryNameLegacyReverseHydrationSummary::default()
        };
        assert!(!hydration_cause_consumed(&failed));
    }

    #[test]
    fn bootstrap_hydration_failed_lookup_without_row_changes_is_not_progress() {
        assert!(bootstrap_hydration_made_progress(
            &primary_name::PrimaryNameLegacyReverseHydrationSummary::default()
        ));

        let failed = primary_name::PrimaryNameLegacyReverseHydrationSummary {
            failed_lookup_count: 1,
            ..primary_name::PrimaryNameLegacyReverseHydrationSummary::default()
        };
        assert!(!bootstrap_hydration_made_progress(&failed));

        let failed_with_change = primary_name::PrimaryNameLegacyReverseHydrationSummary {
            failed_lookup_count: 1,
            upserted_row_count: 1,
            ..primary_name::PrimaryNameLegacyReverseHydrationSummary::default()
        };
        assert!(bootstrap_hydration_made_progress(&failed_with_change));
    }
}
