#[path = "managed_edges/active_addresses.rs"]
mod active_addresses;
#[path = "managed_edges/children.rs"]
mod children;
#[path = "managed_edges/source_graph.rs"]
mod source_graph;

pub(crate) use active_addresses::{
    reconcile_active_contract_instance_addresses,
    reconcile_active_contract_instance_addresses_for_ids,
};
pub(crate) use children::replace_manifest_children;
pub(crate) use source_graph::reconcile_manifest_source_graph;
