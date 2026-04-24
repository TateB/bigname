#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AllCurrentProjectionsReplaySummary {
    pub steps: Vec<CurrentProjectionReplayStepSummary>,
}

impl AllCurrentProjectionsReplaySummary {
    pub fn projection_order(&self) -> Vec<&'static str> {
        self.steps.iter().map(|step| step.projection).collect()
    }

    pub fn total_upserted_row_count(&self) -> usize {
        self.steps.iter().map(|step| step.upserted_row_count).sum()
    }

    pub fn total_deleted_row_count(&self) -> u64 {
        self.steps.iter().map(|step| step.deleted_row_count).sum()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentProjectionReplayStepSummary {
    pub projection: &'static str,
    pub requested_key_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}
