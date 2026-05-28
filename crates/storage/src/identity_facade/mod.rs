mod counts;
mod forward;
mod reverse;
mod reverse_feed;
mod reverse_page;
mod reverse_rows;
mod status;
mod types;

use std::collections::BTreeSet;

pub use forward::{load_identity_name_feed_records_by_names, load_identity_records_by_names};
pub use reverse::load_reverse_identity_records;
pub use reverse_feed::load_reverse_identity_feed_records;
pub use status::load_indexing_status;
pub use types::{
    IdentityAddressRelationRow, IdentityNameCurrentRow, IdentityNameRecordRow,
    IdentityPrimaryNameSnapshot, IdentityRecordInventoryRow, IndexingStatusChainRow,
    IndexingStatusRead, ReverseIdentityCursor, ReverseIdentityFeedGroup, ReverseIdentityFeedInput,
    ReverseIdentityFeedRecordRow, ReverseIdentityGroup, ReverseIdentityRecordRow,
    ReverseIdentityRoles, ReverseIdentityStorageInput,
};

const DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER: &str = r#"
  AND surface.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND resource.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND binding.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND (
      anc.token_lineage_id IS NULL
      OR token_lineage.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
  )
"#;

const DEFAULT_IDENTITY_NAME_CURRENT_READ_FILTER: &str = r#"
  AND identity_nc_surface.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND (
      identity_nc.surface_binding_id IS NULL
      OR (
          identity_nc_resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND identity_nc_binding.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND (
              identity_nc.token_lineage_id IS NULL
              OR identity_nc_token_lineage.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
          )
      )
  )
"#;

const DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER: &str = r#"
  AND resource.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
"#;

fn dedupe_in_order(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}
