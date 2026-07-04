use anyhow::{Result, bail};

use super::types::{NameSurface, Resource, SurfaceBinding, TokenLineage};

pub(super) fn validate_token_lineage(token_lineage: &TokenLineage) -> Result<()> {
    validate_anchor_fields(
        "token lineage",
        &token_lineage.chain_id,
        &token_lineage.block_hash,
        token_lineage.block_number,
    )?;
    if !token_lineage.provenance.is_object() {
        bail!(
            "token lineage {} must store provenance as a JSON object",
            token_lineage.token_lineage_id
        );
    }

    Ok(())
}

pub(super) fn validate_resource(resource: &Resource) -> Result<()> {
    validate_anchor_fields(
        "resource",
        &resource.chain_id,
        &resource.block_hash,
        resource.block_number,
    )?;
    if !resource.provenance.is_object() {
        bail!(
            "resource {} must store provenance as a JSON object",
            resource.resource_id
        );
    }

    Ok(())
}

pub(super) fn validate_name_surface(name_surface: &NameSurface) -> Result<()> {
    if name_surface.logical_name_id.is_empty() {
        bail!("name surface has empty logical_name_id");
    }
    if name_surface.namespace.is_empty() {
        bail!(
            "name surface {} has empty namespace",
            name_surface.logical_name_id
        );
    }
    if name_surface.input_name.is_empty() {
        bail!(
            "name surface {} has empty input_name",
            name_surface.logical_name_id
        );
    }
    if name_surface.canonical_display_name.is_empty() {
        bail!(
            "name surface {} has empty canonical_display_name",
            name_surface.logical_name_id
        );
    }
    if name_surface.normalized_name.is_empty() {
        bail!(
            "name surface {} has empty normalized_name",
            name_surface.logical_name_id
        );
    }
    if name_surface.logical_name_id
        != format!(
            "{}:{}",
            name_surface.namespace, name_surface.normalized_name
        )
    {
        bail!(
            "name surface {} does not match namespace {} and normalized_name {}",
            name_surface.logical_name_id,
            name_surface.namespace,
            name_surface.normalized_name
        );
    }
    if name_surface.dns_encoded_name.is_empty() {
        bail!(
            "name surface {} has empty dns_encoded_name",
            name_surface.logical_name_id
        );
    }
    if name_surface.namehash.is_empty() {
        bail!(
            "name surface {} has empty namehash",
            name_surface.logical_name_id
        );
    }
    if name_surface.labelhashes.is_empty() {
        bail!(
            "name surface {} has empty labelhashes",
            name_surface.logical_name_id
        );
    }
    if name_surface.normalizer_version.is_empty() {
        bail!(
            "name surface {} has empty normalizer_version",
            name_surface.logical_name_id
        );
    }
    if !name_surface.normalization_warnings.is_array() {
        bail!(
            "name surface {} must store normalization_warnings as a JSON array",
            name_surface.logical_name_id
        );
    }
    if !name_surface.normalization_errors.is_array() {
        bail!(
            "name surface {} must store normalization_errors as a JSON array",
            name_surface.logical_name_id
        );
    }
    validate_anchor_fields(
        "name surface",
        &name_surface.chain_id,
        &name_surface.block_hash,
        name_surface.block_number,
    )?;
    if !name_surface.provenance.is_object() {
        bail!(
            "name surface {} must store provenance as a JSON object",
            name_surface.logical_name_id
        );
    }

    Ok(())
}

pub(super) fn validate_surface_binding(binding: &SurfaceBinding) -> Result<()> {
    if binding.logical_name_id.is_empty() {
        bail!(
            "surface binding {} has empty logical_name_id",
            binding.surface_binding_id
        );
    }
    if let Some(active_to) = binding.active_to
        && active_to <= binding.active_from
    {
        bail!(
            "surface binding {} must have active_to after active_from",
            binding.surface_binding_id
        );
    }
    validate_anchor_fields(
        "surface binding",
        &binding.chain_id,
        &binding.block_hash,
        binding.block_number,
    )?;
    if !binding.provenance.is_object() {
        bail!(
            "surface binding {} must store provenance as a JSON object",
            binding.surface_binding_id
        );
    }

    Ok(())
}

fn validate_anchor_fields(
    row_kind: &str,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
) -> Result<()> {
    if chain_id.trim().is_empty() || chain_id == "unknown" {
        bail!("{row_kind} must provide a real chain_id anchor");
    }
    if block_hash.trim().is_empty() || block_hash == "unknown" {
        bail!("{row_kind} must provide a real block_hash anchor");
    }
    if block_number < 0 {
        bail!("{row_kind} has negative block_number {block_number}");
    }

    Ok(())
}

pub(super) fn ensure_token_lineage_identity_matches(
    existing: &TokenLineage,
    incoming: &TokenLineage,
) -> Result<()> {
    let _ = (existing, incoming);
    Ok(())
}

pub(super) fn ensure_resource_identity_matches(
    existing: &Resource,
    incoming: &Resource,
) -> Result<()> {
    let _ = (existing, incoming);
    Ok(())
}

pub(super) fn ensure_name_surface_identity_matches(
    existing: &NameSurface,
    incoming: &NameSurface,
) -> Result<()> {
    if existing.namespace != incoming.namespace
        || existing.normalized_name != incoming.normalized_name
        || existing.dns_encoded_name != incoming.dns_encoded_name
        || existing.namehash != incoming.namehash
        || existing.labelhashes != incoming.labelhashes
        || existing.normalization_errors != incoming.normalization_errors
    {
        if name_surface_normalized_path_repair_allowed(existing, incoming) {
            return Ok(());
        }
        bail!(
            "name surface identity mismatch for {}",
            existing.logical_name_id
        );
    }

    Ok(())
}

pub(super) fn name_surface_normalized_path_repair_allowed(
    existing: &NameSurface,
    incoming: &NameSurface,
) -> bool {
    existing.namespace == incoming.namespace
        && existing.normalized_name == incoming.normalized_name
        && existing.normalization_errors == incoming.normalization_errors
        && existing
            .normalization_errors
            .as_array()
            .is_some_and(Vec::is_empty)
        && (existing.dns_encoded_name != incoming.dns_encoded_name
            || existing.namehash != incoming.namehash
            || existing.labelhashes != incoming.labelhashes)
        && ens_v1_unwrapped_authority_surface_provenance(&existing.provenance)
        && ens_v1_unwrapped_authority_surface_provenance(&incoming.provenance)
}

fn ens_v1_unwrapped_authority_surface_provenance(provenance: &serde_json::Value) -> bool {
    provenance
        .get("adapter")
        .and_then(serde_json::Value::as_str)
        == Some("ens_v1_unwrapped_authority")
}

pub(super) fn ensure_surface_binding_identity_matches(
    existing: &SurfaceBinding,
    incoming: &SurfaceBinding,
) -> Result<()> {
    if existing.logical_name_id != incoming.logical_name_id
        || existing.resource_id != incoming.resource_id
        || existing.binding_kind != incoming.binding_kind
        || existing.active_from != incoming.active_from
        || existing.chain_id != incoming.chain_id
        || existing.block_hash != incoming.block_hash
        || existing.block_number != incoming.block_number
        || existing.provenance != incoming.provenance
    {
        bail!(
            "surface binding identity mismatch for {}",
            existing.surface_binding_id
        );
    }

    Ok(())
}
