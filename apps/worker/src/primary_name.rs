#[path = "primary_name/projection.rs"]
mod projection;
#[path = "primary_name/query.rs"]
mod query;
#[path = "primary_name/types.rs"]
mod types;

pub use projection::rebuild_primary_names_current;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PrimaryNamesCurrentRebuildSummary {
    pub requested_tuple_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
    pub success_row_count: usize,
    pub not_found_row_count: usize,
    pub invalid_name_row_count: usize,
}

#[cfg(test)]
#[path = "primary_name/tests/mod.rs"]
mod tests;
