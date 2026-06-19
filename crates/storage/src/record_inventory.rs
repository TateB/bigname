mod batch_upsert;
mod boundary_key;
mod row_decode;
mod snapshot_reads;
mod validation;

pub(crate) use boundary_key::record_version_boundary_storage_key;

pub use batch_upsert::upsert_record_inventory_current_rows;
pub use row_decode::RecordInventoryCurrentRow;
pub use snapshot_reads::{
    clear_record_inventory_current, delete_record_inventory_current, load_record_inventory_current,
    load_record_inventory_current_batch, load_record_inventory_current_for_snapshot,
    load_record_inventory_current_with_anchor_fallback,
};

#[cfg(test)]
mod tests;
