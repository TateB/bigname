mod collapse;
mod decode;
mod page;
mod query;
mod read;
mod types;
mod write;

pub use collapse::collapse_address_name_current_rows;
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
