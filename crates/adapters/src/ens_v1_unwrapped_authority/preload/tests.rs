use super::*;

#[test]
fn selected_binding_provenance_drives_reused_registry_preload_manifest() -> Result<()> {
    let name = observe_registrar_name_with_version(
        "based1",
        AuthorityProfile::Basenames,
        ENS_NORMALIZER_VERSION,
    )?;
    let labelhash = name.labelhashes[0].clone();
    let authority_key = format!("registry-only:base-mainnet:{}", name.namehash);
    let resource_id = deterministic_uuid(&format!("resource:{authority_key}"));
    let earlier_ref = basenames_boundary_ref(30, 1_700_000_030, "33")?;
    let next_ref = basenames_boundary_ref(40, 1_700_000_040, "44")?;
    let provenance = binding_provenance_over_resource_provenance(
        json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "authority_kind": "registry_only",
            "authority_key": authority_key.clone(),
            "logical_name_id": name.logical_name_id.clone(),
            "namehash": name.namehash.clone(),
            "labelhash": labelhash.clone(),
            "current_registry_owner": "0x0000000000000000000000000000000000000202",
            "binding_source_family": SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            "binding_manifest_version": 2,
            "binding_manifest_id": 202,
        }),
        json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "authority_kind": "registry_only",
            "authority_key": authority_key.clone(),
            "binding_source_family": SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            "binding_manifest_version": 1,
            "binding_manifest_id": 101,
        }),
    );

    assert_eq!(
        provenance
            .get("binding_manifest_version")
            .and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(
        provenance
            .get("binding_manifest_id")
            .and_then(Value::as_i64),
        Some(101)
    );

    let mut history = empty_preloaded_history(labelhash, Some(name));
    preload_registry_history(
        &mut history,
        &provenance,
        &earlier_ref,
        Uuid::from_u128(0x3213),
        resource_id,
        None,
    );
    let before_anchor = history
        .open_binding
        .as_ref()
        .map(|binding| binding.authority.clone());
    transition_authority(
        &mut history,
        before_anchor,
        None,
        &next_ref,
        next_ref.block_timestamp,
    )?;

    let surface_unbound = history
        .events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_SURFACE_UNBOUND)
        .context("preloaded registry binding should emit SurfaceUnbound")?;
    assert_eq!(surface_unbound.manifest_version, 1);
    assert_eq!(surface_unbound.source_manifest_id, Some(101));
    assert_eq!(
        surface_unbound.source_family,
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
    );

    Ok(())
}

#[test]
fn selected_binding_manifest_survives_active_anchor_recompute() -> Result<()> {
    let name = observe_registrar_name_with_version(
        "based1",
        AuthorityProfile::Basenames,
        ENS_NORMALIZER_VERSION,
    )?;
    let labelhash = name.labelhashes[0].clone();
    let authority_key = format!("registry-only:base-mainnet:{}", name.namehash);
    let resource_id = deterministic_uuid(&format!("resource:{authority_key}"));
    let registry_ref = basenames_boundary_ref(30, 1_700_000_030, "33")?;
    let grant_ref = ObservationRef {
        chain_id: "base-mainnet".to_owned(),
        block_hash: "0x4444444444444444444444444444444444444444444444444444444444444444".to_owned(),
        block_number: 40,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_040)?,
        transaction_hash: Some(
            "0x5555555555555555555555555555555555555555555555555555555555555555".to_owned(),
        ),
        transaction_index: Some(0),
        log_index: Some(7),
        canonicality_state: CanonicalityState::Finalized,
        namespace: AuthorityProfile::Basenames.namespace().to_owned(),
        source_manifest_id: 303,
        source_family: SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR.to_owned(),
        manifest_version: 3,
    };
    let provenance = binding_provenance_over_resource_provenance(
        json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "authority_kind": "registry_only",
            "authority_key": authority_key.clone(),
            "logical_name_id": name.logical_name_id.clone(),
            "namehash": name.namehash.clone(),
            "labelhash": labelhash.clone(),
            "current_registry_owner": "0x0000000000000000000000000000000000000202",
            "binding_source_family": SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            "binding_manifest_version": 2,
            "binding_manifest_id": 202,
        }),
        json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "authority_kind": "registry_only",
            "authority_key": authority_key,
            "binding_source_family": SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            "binding_manifest_version": 2,
            "binding_manifest_id": 202,
        }),
    );
    let mut history = empty_preloaded_history(labelhash.clone(), Some(name.clone()));

    preload_registry_history(
        &mut history,
        &provenance,
        &registry_ref,
        Uuid::from_u128(0x3215),
        resource_id,
        None,
    );
    apply_registration_granted(
        &mut history,
        NameRegistrationObservation {
            label: "based1".to_owned(),
            labelhash,
            registrant: "0x0000000000000000000000000000000000000303".to_owned(),
            expiry: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
            reference: grant_ref,
        },
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;

    let surface_unbound = history
        .events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_SURFACE_UNBOUND)
        .context("registration grant should unbind the preloaded registry authority")?;
    assert_eq!(
        surface_unbound.source_family,
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
    );
    assert_eq!(surface_unbound.manifest_version, 2);
    assert_eq!(surface_unbound.source_manifest_id, Some(202));

    Ok(())
}

#[test]
fn legacy_binding_provenance_does_not_inherit_current_resource_manifest() -> Result<()> {
    let name = observe_registrar_name_with_version(
        "based1",
        AuthorityProfile::Basenames,
        ENS_NORMALIZER_VERSION,
    )?;
    let labelhash = name.labelhashes[0].clone();
    let authority_key = format!("registry-only:base-mainnet:{}", name.namehash);
    let resource_id = deterministic_uuid(&format!("resource:{authority_key}"));
    let earlier_ref = basenames_boundary_ref(30, 1_700_000_030, "33")?;
    let next_ref = basenames_boundary_ref(40, 1_700_000_040, "44")?;
    let provenance = binding_provenance_over_resource_provenance(
        json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "authority_kind": "registry_only",
            "authority_key": authority_key.clone(),
            "logical_name_id": name.logical_name_id.clone(),
            "namehash": name.namehash.clone(),
            "labelhash": labelhash.clone(),
            "current_registry_owner": "0x0000000000000000000000000000000000000202",
            "binding_source_family": SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            "binding_manifest_version": 2,
            "binding_manifest_id": 202,
        }),
        json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "authority_kind": "registry_only",
            "authority_key": authority_key.clone(),
        }),
    );

    assert_eq!(
        provenance
            .get("binding_source_family")
            .and_then(Value::as_str),
        None
    );
    assert_eq!(
        provenance
            .get("binding_manifest_version")
            .and_then(Value::as_i64),
        None
    );
    assert_eq!(
        provenance
            .get("binding_manifest_id")
            .and_then(Value::as_i64),
        None
    );

    let mut history = empty_preloaded_history(labelhash, Some(name));
    preload_registry_history(
        &mut history,
        &provenance,
        &earlier_ref,
        Uuid::from_u128(0x3214),
        resource_id,
        None,
    );
    let before_anchor = history
        .open_binding
        .as_ref()
        .map(|binding| binding.authority.clone());
    transition_authority(
        &mut history,
        before_anchor,
        None,
        &next_ref,
        next_ref.block_timestamp,
    )?;

    let surface_unbound = history
        .events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_SURFACE_UNBOUND)
        .context("preloaded legacy registry binding should emit SurfaceUnbound")?;
    assert_eq!(surface_unbound.manifest_version, 1);
    assert_eq!(surface_unbound.source_manifest_id, None);
    assert_eq!(
        surface_unbound.source_family,
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
    );

    Ok(())
}

fn basenames_boundary_ref(
    block_number: i64,
    block_timestamp: i64,
    block_hash_seed: &str,
) -> Result<BoundaryRef> {
    Ok(BoundaryRef {
        chain_id: "base-mainnet".to_owned(),
        block_hash: format!("0x{}", block_hash_seed.repeat(32)),
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(block_timestamp)?,
        canonicality_state: CanonicalityState::Finalized,
        namespace: AuthorityProfile::Basenames.namespace().to_owned(),
    })
}
