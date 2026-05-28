mod constants;
mod load;
mod model;
mod positions;
mod projection;
mod rebuild;
mod relations;
mod source_policy;
mod util;

pub use rebuild::rebuild_address_names_current;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AddressNamesCurrentRebuildSummary {
    pub requested_address_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

#[cfg(test)]
mod tests;
