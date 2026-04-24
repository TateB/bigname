use super::{ALL_CURRENT_PROJECTION_JSON_ORDER, AllCurrentProjectionsReplaySummary};

impl AllCurrentProjectionsReplaySummary {
    pub fn json_summary_value(&self) -> serde_json::Value {
        let projections = ALL_CURRENT_PROJECTION_JSON_ORDER
            .iter()
            .map(|projection| {
                let counts = self.projection_json_counts(projection);
                serde_json::json!({
                    "projection": projection,
                    "requested": counts.requested,
                    "upserted": counts.upserted,
                    "deleted": counts.deleted,
                })
            })
            .collect::<Vec<_>>();
        let totals = self.json_totals();

        serde_json::json!({
            "command": "all-current-projections",
            "projections": projections,
            "totals": {
                "requested": totals.requested,
                "upserted": totals.upserted,
                "deleted": totals.deleted,
            },
        })
    }

    pub fn json_summary_string(&self) -> serde_json::Result<String> {
        serde_json::to_string(&self.json_summary_value())
    }

    fn json_totals(&self) -> ProjectionJsonCounts {
        ALL_CURRENT_PROJECTION_JSON_ORDER.iter().fold(
            ProjectionJsonCounts::default(),
            |mut totals, projection| {
                let counts = self.projection_json_counts(projection);
                totals.requested += counts.requested;
                totals.upserted += counts.upserted;
                totals.deleted += counts.deleted;
                totals
            },
        )
    }

    fn projection_json_counts(&self, projection: &str) -> ProjectionJsonCounts {
        self.steps
            .iter()
            .find(|step| step.projection == projection)
            .map(|step| ProjectionJsonCounts {
                requested: step.requested_key_count as u64,
                upserted: step.upserted_row_count as u64,
                deleted: step.deleted_row_count,
            })
            .unwrap_or_default()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ProjectionJsonCounts {
    requested: u64,
    upserted: u64,
    deleted: u64,
}
