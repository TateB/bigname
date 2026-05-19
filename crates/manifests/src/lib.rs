//! Repository manifest loading, persistence, and discovery admission.

#[path = "lib/discovery.rs"]
mod discovery;
#[path = "lib/managed_edges.rs"]
mod managed_edges;
#[path = "lib/model.rs"]
mod model;
#[path = "lib/repository.rs"]
mod repository;
#[path = "lib/support.rs"]
mod support;
#[path = "lib/sync.rs"]
mod sync;
#[cfg(test)]
#[path = "lib/tests.rs"]
mod tests;
#[path = "lib/views.rs"]
mod views;

pub const fn bootstrap_status() -> &'static str {
    "manifest-loader-ready"
}

const DECLARATION_KIND_ROOT: &str = "root";
const DECLARATION_KIND_CONTRACT: &str = "contract";
const CONTRACT_KIND_ROOT: &str = "root";
const CONTRACT_KIND_CONTRACT: &str = "contract";
const MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND: &str = "proxy_implementation";
const MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE: &str = "manifest_declared_proxy";
const MANIFEST_PROXY_IMPLEMENTATION_ADMISSION: &str = "manifest_declared";
const MANIFEST_SUCCESSOR_EDGE_KIND: &str = "migration";
const MANIFEST_SUCCESSOR_DISCOVERY_SOURCE: &str = "manifest_successor";
const MANIFEST_SUCCESSOR_ADMISSION: &str = "manifest_successor";
const REACHABLE_FROM_ROOT_ADMISSION: &str = "reachable_from_root";
const PROPAGATED_ROLE_PROVENANCE_FIELD: &str = "propagated_role";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

pub use discovery::*;
pub use model::*;
pub use repository::load_repository;
pub use sync::sync_repository;
pub use views::*;

pub(crate) use managed_edges::{
    reconcile_active_contract_instance_addresses,
    reconcile_active_contract_instance_addresses_for_ids,
};
pub(crate) use repository::normalize_address;
pub(crate) use sync::{
    ensure_contract_instance_address_seed, resolve_contract_instance_by_address,
};
