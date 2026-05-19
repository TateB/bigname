mod merge;
mod orphan;
mod read;
mod types;
mod validate;
mod write;
mod write_fast;
mod write_rows;

pub use orphan::{mark_identity_rows_range_orphaned, mark_surface_binding_range_orphaned};
pub use read::{
    load_name_surface, load_name_surface_including_noncanonical, load_resource,
    load_resource_including_noncanonical, load_surface_binding,
    load_surface_binding_including_noncanonical, load_surface_bindings_by_logical_name_id,
    load_surface_bindings_by_logical_name_id_including_noncanonical,
    load_surface_bindings_by_resource_id,
    load_surface_bindings_by_resource_id_including_noncanonical, load_token_lineage,
    load_token_lineage_including_noncanonical,
};
pub use types::{
    IdentityOrphanCounts, NameSurface, Resource, SurfaceBinding, SurfaceBindingKind, TokenLineage,
};
pub use write::{
    upsert_name_surfaces, upsert_name_surfaces_without_snapshots, upsert_resources,
    upsert_resources_without_snapshots, upsert_surface_bindings,
    upsert_surface_bindings_without_snapshots, upsert_token_lineages,
    upsert_token_lineages_without_snapshots,
};

#[cfg(test)]
mod tests;
