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

#[test]
fn existing_binding_closure_uses_next_later_incoming_segment() {
    let existing = test_binding(
        Uuid::from_u128(0x200),
        "ens:bancambios.eth",
        1_582_156_785,
        None,
    );
    let earlier_incoming = test_binding(
        Uuid::from_u128(0x100),
        "ens:bancambios.eth",
        1_581_795_431,
        Some(existing.active_from),
    );
    let later_incoming = test_binding(
        Uuid::from_u128(0x300),
        "ens:bancambios.eth",
        1_582_530_648,
        None,
    );

    let closures = existing_binding_closures_for_incoming(
        std::slice::from_ref(&existing),
        &[earlier_incoming, later_incoming.clone()],
    );

    assert_eq!(closures.len(), 1);
    assert_eq!(closures[0].surface_binding_id, existing.surface_binding_id);
    assert_eq!(closures[0].active_to, Some(later_incoming.active_from));
}

#[test]
fn stale_overlap_candidates_only_include_adapter_rows_without_matching_binding_id() {
    let incoming = vec![test_binding(
        Uuid::from_u128(0x100),
        "ens:retry.eth",
        1_695_230_399,
        None,
    )];
    let mut stale = test_binding(Uuid::from_u128(0x200), "ens:retry.eth", 1_695_230_399, None);
    stale.provenance = json!({
        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        "authority_key": "registrar:ethereum-mainnet:0xabc:1"
    });
    let mut same_id = stale.clone();
    same_id.surface_binding_id = incoming[0].surface_binding_id;
    let unrelated_adapter =
        test_binding(Uuid::from_u128(0x300), "ens:retry.eth", 1_695_230_399, None);

    let candidates = stale_overlapping_surface_binding_candidates(
        &incoming,
        &[stale.clone(), same_id, unrelated_adapter],
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates
            .get(&stale.surface_binding_id)
            .map(|candidate| candidate.resource_id),
        Some(stale.resource_id)
    );
    assert_eq!(
        candidates
            .get(&stale.surface_binding_id)
            .and_then(|candidate| candidate.authority_key.as_deref()),
        Some("registrar:ethereum-mainnet:0xabc:1")
    );
    assert_eq!(
        candidates
            .get(&stale.surface_binding_id)
            .map(|candidate| candidate.active_from_epoch),
        Some(stale.active_from.unix_timestamp())
    );
}

#[test]
fn normalize_surface_bindings_tightens_duplicate_active_to() -> Result<()> {
    let earlier_close =
        OffsetDateTime::from_unix_timestamp(1_695_284_247).expect("test timestamp is valid");
    let later_close =
        OffsetDateTime::from_unix_timestamp(1_695_370_647).expect("test timestamp is valid");
    let first = test_binding(
        Uuid::from_u128(0x100),
        "ens:missioncontrol.2718.eth",
        1_695_230_399,
        Some(later_close),
    );
    let second = test_binding(
        Uuid::from_u128(0x100),
        "ens:missioncontrol.2718.eth",
        1_695_230_399,
        Some(earlier_close),
    );

    let mut bindings = vec![first, second];
    normalize_surface_bindings_for_upsert(&mut bindings)?;

    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].active_to, Some(earlier_close));

    Ok(())
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
