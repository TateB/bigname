mod decode;
mod paging;
mod reads;
mod types;
mod validation;
mod writes;

pub use paging::{
    load_permissions_current_account_resource_page,
    load_permissions_current_account_resource_page_count_summary, load_permissions_current_page,
};
pub use reads::{
    load_permissions_current, load_permissions_current_by_resource_ids,
    load_permissions_current_for_resolver_scope, load_permissions_current_resolver_targets,
};
pub use types::{
    PermissionScope, PermissionsCurrentAccountResourceCursor,
    PermissionsCurrentAccountResourcePage, PermissionsCurrentFullFilterSummary,
    PermissionsCurrentKeysetCursor, PermissionsCurrentPage, PermissionsCurrentRow,
};
pub use writes::{
    clear_permissions_current, delete_permissions_current, upsert_permissions_current_rows,
};

#[cfg(test)]
use anyhow::{Context, Result};
#[cfg(test)]
use serde_json::json;
#[cfg(test)]
use sqlx::{PgPool, types::time::OffsetDateTime};
#[cfg(test)]
use uuid::Uuid;

#[cfg(test)]
mod tests;
