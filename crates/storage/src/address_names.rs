mod address_replacement;
mod collapse;
mod count;
mod decode;
mod full_rebuild;
mod page;
mod query;
mod read;
mod types;
mod write;

pub use address_replacement::{
    AddressNamesCurrentAddressReplacement, begin_address_names_current_address_replacement,
    drop_address_names_current_address_replacement,
    insert_address_names_current_address_replacement_rows,
    publish_address_names_current_address_replacement, replace_address_names_current_logical_names,
};
pub use collapse::collapse_address_name_current_rows;
pub use count::{AddressNamesCurrentCountFilter, count_address_names_current_for_app_filter};
pub use full_rebuild::{
    AddressNamesCurrentFullRebuild, begin_address_names_current_full_rebuild,
    drop_address_names_current_full_rebuild, insert_address_names_current_full_rebuild_rows,
    publish_address_names_current_full_rebuild, rebuild_address_names_current_identity_sidecars,
};
pub use page::load_address_names_current_page;
pub use read::{load_address_names_current, load_address_names_current_including_noncanonical};
pub use types::{
    AddressNameCurrentEntry, AddressNameCurrentRow, AddressNameRelation, AddressNamesCurrentCursor,
    AddressNamesCurrentDedupe, AddressNamesCurrentPage, AddressNamesCurrentProvenanceSummary,
    AddressNamesCurrentSummary,
};
pub use write::{
    clear_address_names_current, delete_address_names_current, upsert_address_names_current_rows,
};

#[cfg(test)]
use page::address_names_current_cursor_from_entry;

#[cfg(test)]
mod tests;
