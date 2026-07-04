use super::*;

pub(in crate::ens_v1_unwrapped_authority) async fn build_surface_binding(
    _pool: &PgPool,
    logical_name_id: &str,
    segment: &BindingSegment,
    chain: &str,
) -> Result<SurfaceBinding> {
    Ok(SurfaceBinding {
        surface_binding_id: segment.surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id: segment.authority.resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: segment.active_from,
        active_to: segment.active_to,
        chain_id: chain.to_owned(),
        block_hash: segment.anchor_ref.block_hash.clone(),
        block_number: segment.anchor_ref.block_number,
        provenance: surface_binding_provenance(segment),
        canonicality_state: segment.anchor_ref.canonicality_state,
    })
}

pub(in crate::ens_v1_unwrapped_authority) fn surface_binding_provenance(
    segment: &BindingSegment,
) -> Value {
    json!({
        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        "authority_kind": segment.authority.kind.as_str(),
        "authority_key": segment.authority.authority_key,
        "binding_source_family": segment.authority.binding_source_family,
        "binding_manifest_version": segment.authority.binding_manifest_version,
        "binding_manifest_id": segment.authority.binding_manifest_id,
    })
}
