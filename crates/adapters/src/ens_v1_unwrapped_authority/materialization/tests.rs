use super::*;

#[test]
fn coalesce_name_surfaces_for_upsert_keeps_first_identity() {
    let first = test_surface(
        "ens:missioncontrol.2718.eth",
        "MissionControl.2718.eth",
        "0x1111111111111111111111111111111111111111111111111111111111111111",
    );
    let other = test_surface(
        "ens:other.eth",
        "other.eth",
        "0x2222222222222222222222222222222222222222222222222222222222222222",
    );
    let duplicate = test_surface(
        "ens:missioncontrol.2718.eth",
        "missioncontrol.2718.eth",
        "0x3333333333333333333333333333333333333333333333333333333333333333",
    );

    let mut surfaces = vec![first.clone(), other.clone(), duplicate];
    coalesce_name_surfaces_for_upsert(&mut surfaces);

    assert_eq!(surfaces, vec![first, other]);
}

#[test]
fn normalize_surface_bindings_closes_same_batch_open_intervals() -> Result<()> {
    let first = test_binding(
        Uuid::from_u128(0x100),
        "ens:missioncontrol.2718.eth",
        1_695_230_399,
        None,
    );
    let second = test_binding(
        Uuid::from_u128(0x200),
        "ens:missioncontrol.2718.eth",
        1_695_284_247,
        None,
    );

    let mut bindings = vec![second.clone(), first.clone()];
    normalize_surface_bindings_for_upsert(&mut bindings)?;

    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0].surface_binding_id, first.surface_binding_id);
    assert_eq!(bindings[0].active_to, Some(second.active_from));
    assert_eq!(bindings[1].surface_binding_id, second.surface_binding_id);
    assert_eq!(bindings[1].active_to, None);

    Ok(())
}

#[test]
fn trim_incoming_bindings_at_existing_starts_closes_historical_open_interval() {
    let mut incoming = vec![test_binding(
        Uuid::from_u128(0x100),
        "ens:future.eth",
        1_695_230_399,
        None,
    )];
    let existing = vec![test_binding(
        Uuid::from_u128(0x200),
        "ens:future.eth",
        1_695_284_247,
        None,
    )];

    trim_incoming_bindings_at_existing_starts(&mut incoming, &existing);

    assert_eq!(incoming[0].active_to, Some(existing[0].active_from));
}

fn test_surface(logical_name_id: &str, input_name: &str, namehash: &str) -> NameSurface {
    let normalized_name = logical_name_id
        .strip_prefix("ens:")
        .expect("test logical name id uses ens namespace")
        .to_owned();

    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        input_name: input_name.to_owned(),
        canonical_display_name: normalized_name.clone(),
        normalized_name,
        dns_encoded_name: vec![3, b'e', b't', b'h', 0],
        namehash: namehash.to_owned(),
        labelhashes: vec![namehash.to_owned()],
        normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: namehash.to_owned(),
        block_number: 1,
        provenance: json!({}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn test_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    active_from: i64,
    active_to: Option<OffsetDateTime>,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id: surface_binding_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: OffsetDateTime::from_unix_timestamp(active_from)
            .expect("test timestamp is valid"),
        active_to,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0x{surface_binding_id}"),
        block_number: active_from,
        provenance: json!({}),
        canonicality_state: CanonicalityState::Canonical,
    }
}
