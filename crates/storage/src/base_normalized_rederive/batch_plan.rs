use anyhow::{Result, ensure};

use super::BaseNormalizedRederiveCounts;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseNormalizedRederiveBatchPlanStep {
    pub step: &'static str,
    pub rows: i64,
    pub estimated_batches: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseNormalizedRederiveBatchPlan {
    pub run_id: String,
    pub batch_size: i64,
    pub steps: Vec<BaseNormalizedRederiveBatchPlanStep>,
    pub estimated_total_batches: i64,
}

impl BaseNormalizedRederiveBatchPlan {
    pub fn from_counts(
        run_id: &str,
        batch_size: i64,
        counts: &BaseNormalizedRederiveCounts,
    ) -> Result<Self> {
        ensure!(
            !run_id.trim().is_empty(),
            "Base normalized-event rederive run id must not be empty"
        );
        ensure!(
            batch_size > 0,
            "Base normalized-event rederive batch size must be positive"
        );
        let mut steps = Vec::new();
        for (step, rows) in [
            ("address_names_current", counts.address_names_current),
            ("name_current", counts.name_current),
            ("children_current", counts.children_current),
            ("permissions_current", counts.permissions_current),
            ("record_inventory_current", counts.record_inventory_current),
            (
                "projection_normalized_event_changes",
                counts.projection_normalized_event_changes,
            ),
            ("normalized_events", counts.normalized_events),
            ("surface_bindings", counts.surface_bindings),
            ("resources", counts.resources),
            ("name_surfaces", counts.name_surfaces),
            ("token_lineages", counts.token_lineages),
        ] {
            steps.push(BaseNormalizedRederiveBatchPlanStep {
                step,
                rows,
                estimated_batches: estimated_batches(rows, batch_size),
            });
        }
        let final_reset_rows = counts.current_projection_replay_status
            + counts.replay_cursor_rows
            + counts.adapter_checkpoint_rows
            + counts.adapter_checkpoint_item_rows;
        steps.push(BaseNormalizedRederiveBatchPlanStep {
            step: "final_replay_reset",
            rows: final_reset_rows,
            estimated_batches: i64::from(final_reset_rows > 0),
        });
        let estimated_total_batches = steps.iter().map(|step| step.estimated_batches).sum();
        Ok(Self {
            run_id: run_id.to_owned(),
            batch_size,
            steps,
            estimated_total_batches,
        })
    }
}

fn estimated_batches(rows: i64, batch_size: i64) -> i64 {
    if rows <= 0 {
        0
    } else {
        (rows + batch_size - 1) / batch_size
    }
}
