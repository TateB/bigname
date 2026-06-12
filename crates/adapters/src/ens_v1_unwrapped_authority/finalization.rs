use super::*;

#[derive(Clone, Debug)]
pub(super) struct FinalizedHistory {
    pub(super) labelhash: String,
    pub(super) first_name_ref: Option<ObservationRef>,
    pub(super) bindings: Vec<BindingSegment>,
    pub(super) events: Vec<NormalizedEvent>,
    pub(super) registrar_leases: Vec<RegistrationLease>,
    pub(super) wrapper_authorities: Vec<WrapperAuthority>,
    pub(super) registry_resource_anchor: Option<BoundaryRef>,
    pub(super) current_registry_owner: Option<String>,
}

pub(super) fn finalize_history(
    mut history: NameHistory,
    head_ref: &BoundaryRef,
) -> Result<FinalizedHistory> {
    settle_due_registration_release(&mut history, head_ref)?;

    if history.current_wrapper_key.is_none()
        && history.open_binding.is_none()
        && let Some(lease) = history.current_registration.as_ref()
    {
        let registrar_anchor = build_registrar_anchor(lease);
        history.open_binding = Some(OpenBinding {
            surface_binding_id: deterministic_uuid(&format!(
                "binding:{}:{}",
                registrar_anchor.authority_key,
                lease.start_ref.block_timestamp.unix_timestamp()
            )),
            authority: registrar_anchor,
            active_from: lease.start_ref.block_timestamp,
            anchor_ref: lease.start_ref.as_boundary_ref(),
        });
    }

    if history.open_binding.is_none()
        && let Some(wrapper_key) = history.current_wrapper_key.as_ref()
        && let Some(wrapper) = history.wrapper_authorities.get(wrapper_key)
    {
        let wrapper_anchor = build_wrapper_anchor(wrapper);
        history.open_binding = Some(OpenBinding {
            surface_binding_id: deterministic_uuid(&format!(
                "binding:{}:{}",
                wrapper_anchor.authority_key,
                wrapper.start_ref.block_timestamp.unix_timestamp()
            )),
            authority: wrapper_anchor,
            active_from: wrapper.start_ref.block_timestamp,
            anchor_ref: wrapper.start_ref.as_boundary_ref(),
        });
    }

    if history.open_binding.is_none()
        && history.current_registration.is_none()
        && history.current_wrapper_key.is_none()
        && history
            .current_registry_owner
            .as_deref()
            .is_some_and(|owner| owner != ZERO_ADDRESS)
        && let Some(anchor) =
            registry_anchor_for_history(&history, &head_ref.chain_id, &history.labelhash)
    {
        history.open_binding = Some(OpenBinding {
            surface_binding_id: deterministic_uuid(&format!(
                "binding:{}:{}",
                anchor.authority_key,
                anchor
                    .binding_manifest_id
                    .checked_mul(0)
                    .unwrap_or_default()
                    + head_ref.block_timestamp.unix_timestamp()
            )),
            authority: anchor,
            active_from: head_ref.block_timestamp,
            anchor_ref: head_ref.clone(),
        });
    }

    if let Some(open_binding) = history.open_binding.take() {
        history.bindings.push(BindingSegment {
            surface_binding_id: open_binding.surface_binding_id,
            authority: open_binding.authority,
            active_from: open_binding.active_from,
            active_to: None,
            anchor_ref: open_binding.anchor_ref,
        });
    }

    let registrar_leases = history
        .current_registration
        .into_iter()
        .chain(history.superseded_registration)
        .collect::<Vec<_>>();
    let wrapper_authorities = history
        .wrapper_authorities
        .into_values()
        .collect::<Vec<_>>();
    let registry_resource_anchor = history.registry_resource_anchor.clone().or_else(|| {
        history
            .latest_registry_owner_ref
            .as_ref()
            .or(history.latest_registry_owner_before_registration.as_ref())
            .map(ObservationRef::as_boundary_ref)
    });

    Ok(FinalizedHistory {
        labelhash: history.labelhash,
        first_name_ref: history.first_name_ref,
        bindings: history.bindings,
        events: history.events,
        registrar_leases,
        wrapper_authorities,
        registry_resource_anchor,
        current_registry_owner: history.current_registry_owner,
    })
}

pub(super) fn build_registrar_anchor(lease: &RegistrationLease) -> AuthorityAnchor {
    AuthorityAnchor {
        kind: AuthorityKind::Registrar,
        authority_key: lease.authority_key.clone(),
        resource_id: deterministic_uuid(&format!("resource:{}", lease.authority_key)),
        token_lineage_id: Some(deterministic_uuid(&format!(
            "token-lineage:{}",
            lease.authority_key
        ))),
        binding_source_family: lease.start_ref.source_family.clone(),
        binding_manifest_version: lease.start_ref.manifest_version,
        binding_manifest_id: lease.start_ref.source_manifest_id,
    }
}

pub(super) fn build_wrapper_anchor(authority: &WrapperAuthority) -> AuthorityAnchor {
    AuthorityAnchor {
        kind: AuthorityKind::Wrapper,
        authority_key: authority.authority_key.clone(),
        resource_id: deterministic_uuid(&format!("resource:{}", authority.authority_key)),
        token_lineage_id: Some(deterministic_uuid(&format!(
            "token-lineage:{}",
            authority.authority_key
        ))),
        binding_source_family: authority.start_ref.source_family.clone(),
        binding_manifest_version: authority.start_ref.manifest_version,
        binding_manifest_id: authority.start_ref.source_manifest_id,
    }
}

pub(super) fn registry_anchor_for_history(
    history: &NameHistory,
    chain: &str,
    labelhash: &str,
) -> Option<AuthorityAnchor> {
    if history
        .current_registry_owner
        .as_deref()
        .is_none_or(|owner| owner == ZERO_ADDRESS)
    {
        return None;
    }

    let reference = history
        .latest_registry_owner_ref
        .as_ref()
        .or(history.latest_registry_owner_before_registration.as_ref())?;
    let node = registry_authority_node(history).unwrap_or(labelhash);
    Some(AuthorityAnchor {
        kind: AuthorityKind::RegistryOnly,
        authority_key: format!("registry-only:{chain}:{node}"),
        resource_id: deterministic_uuid(&format!("resource:registry-only:{chain}:{node}")),
        token_lineage_id: None,
        binding_source_family: reference.source_family.clone(),
        binding_manifest_version: reference.manifest_version,
        binding_manifest_id: reference.source_manifest_id,
    })
}

fn registry_authority_node(history: &NameHistory) -> Option<&str> {
    if !history.namehash.is_empty() {
        return Some(history.namehash.as_str());
    }
    history.name.as_ref().map(|name| name.namehash.as_str())
}
