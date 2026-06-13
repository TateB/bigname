use super::*;

pub(super) fn transition_authority(
    history: &mut NameHistory,
    before: Option<AuthorityAnchor>,
    after: Option<AuthorityAnchor>,
    reference: &BoundaryRef,
    effective_time: OffsetDateTime,
) -> Result<()> {
    if authority_eq(before.as_ref(), after.as_ref()) {
        return Ok(());
    }

    history.current_record_version = None;

    if let Some(open_binding) = history.open_binding.take() {
        if open_binding.active_from < effective_time {
            history.bindings.push(BindingSegment {
                surface_binding_id: open_binding.surface_binding_id,
                authority: open_binding.authority.clone(),
                active_from: open_binding.active_from,
                active_to: Some(effective_time),
                anchor_ref: open_binding.anchor_ref.clone(),
            });
        }
        if open_binding.active_from <= effective_time
            && let Some(name) = history.name.as_ref()
        {
            history.events.push(build_boundary_event(
                reference,
                BoundaryEventPayload {
                    logical_name_id: Some(name.logical_name_id.clone()),
                    resource_id: Some(open_binding.authority.resource_id),
                    event_kind: EVENT_KIND_SURFACE_UNBOUND,
                    before_state: json!({
                        "authority_kind": open_binding.authority.kind.as_str(),
                        "authority_key": open_binding.authority.authority_key,
                    }),
                    after_state: json!({
                        "authority_kind": open_binding.authority.kind.as_str(),
                        "authority_key": open_binding.authority.authority_key,
                        "active_to": effective_time.unix_timestamp(),
                    }),
                    identity_suffix: format!(
                        "surface-unbound:{}:{}:{}",
                        reference.block_hash, name.logical_name_id, open_binding.surface_binding_id
                    ),
                },
                BoundaryEventSource {
                    source_family: open_binding.authority.binding_source_family.clone(),
                    manifest_version: open_binding.authority.binding_manifest_version,
                    source_manifest_id: source_manifest_id_if_known(
                        open_binding.authority.binding_manifest_id,
                    ),
                    canonicality_state: reference.canonicality_state,
                },
            ));
        }
    }

    if let Some(after_anchor) = after.clone() {
        let surface_binding_id = deterministic_uuid(&format!(
            "binding:{}:{}",
            after_anchor.authority_key,
            effective_time.unix_timestamp()
        ));
        history.open_binding = Some(OpenBinding {
            surface_binding_id,
            authority: after_anchor.clone(),
            active_from: effective_time,
            anchor_ref: reference.clone(),
        });
        if let Some(name) = history.name.as_ref() {
            history.events.push(build_boundary_event(
                reference,
                BoundaryEventPayload {
                    logical_name_id: Some(name.logical_name_id.clone()),
                    resource_id: Some(after_anchor.resource_id),
                    event_kind: EVENT_KIND_SURFACE_BOUND,
                    before_state: json!({}),
                    after_state: json!({
                        "authority_kind": after_anchor.kind.as_str(),
                        "authority_key": after_anchor.authority_key,
                        "active_from": effective_time.unix_timestamp(),
                        "binding_kind": SurfaceBindingKind::DeclaredRegistryPath.as_str(),
                    }),
                    identity_suffix: format!(
                        "surface-bound:{}:{}:{}",
                        reference.block_hash, name.logical_name_id, surface_binding_id
                    ),
                },
                BoundaryEventSource {
                    source_family: after_anchor.binding_source_family.clone(),
                    manifest_version: after_anchor.binding_manifest_version,
                    source_manifest_id: source_manifest_id_if_known(
                        after_anchor.binding_manifest_id,
                    ),
                    canonicality_state: reference.canonicality_state,
                },
            ));
        }
    }

    if let Some(name) = history.name.as_ref() {
        let source_family = after
            .as_ref()
            .map(|value| value.binding_source_family.clone())
            .or_else(|| {
                before
                    .as_ref()
                    .map(|value| value.binding_source_family.clone())
            })
            .unwrap_or_else(|| default_registrar_source_family(&name.namespace).to_owned());
        let manifest_version = after
            .as_ref()
            .map(|value| value.binding_manifest_version)
            .or_else(|| before.as_ref().map(|value| value.binding_manifest_version))
            .unwrap_or(1);
        let manifest_id = after
            .as_ref()
            .map(|value| value.binding_manifest_id)
            .or_else(|| before.as_ref().map(|value| value.binding_manifest_id))
            .unwrap_or(0);
        let mut after_state = json!({
            "authority_kind": after.as_ref().map(|value| value.kind.as_str()),
            "authority_key": after.as_ref().map(|value| value.authority_key.clone()),
        });
        if after
            .as_ref()
            .is_some_and(|value| value.kind == AuthorityKind::RegistryOnly)
            && let Some(owner) = nonzero_address(history.current_registry_owner.as_deref())
            && let Some(object) = after_state.as_object_mut()
        {
            object.insert("registry_owner".to_owned(), json!(owner));
        }

        history.events.push(build_boundary_event(
            reference,
            BoundaryEventPayload {
                logical_name_id: Some(name.logical_name_id.clone()),
                resource_id: after
                    .as_ref()
                    .map(|value| value.resource_id)
                    .or(before.as_ref().map(|value| value.resource_id)),
                event_kind: EVENT_KIND_AUTHORITY_EPOCH_CHANGED,
                before_state: json!({
                    "authority_kind": before.as_ref().map(|value| value.kind.as_str()),
                    "authority_key": before.as_ref().map(|value| value.authority_key.clone()),
                }),
                after_state,
                identity_suffix: format!(
                    "authority-epoch:{}:{}:{}:{}:{}",
                    reference.block_hash,
                    name.logical_name_id,
                    effective_time.unix_timestamp(),
                    before
                        .as_ref()
                        .map(|value| value.authority_key.as_str())
                        .unwrap_or("none"),
                    after
                        .as_ref()
                        .map(|value| value.authority_key.as_str())
                        .unwrap_or("none")
                ),
            },
            BoundaryEventSource {
                source_family,
                manifest_version,
                source_manifest_id: source_manifest_id_if_known(manifest_id),
                canonicality_state: reference.canonicality_state,
            },
        ));
    }

    if let (Some(name), Some(after_anchor), Some(current_resolver)) = (
        history.name.as_ref(),
        after.as_ref(),
        nonzero_address(history.current_resolver.as_deref()),
    ) {
        history.events.push(build_boundary_event(
            reference,
            BoundaryEventPayload {
                logical_name_id: Some(name.logical_name_id.clone()),
                resource_id: Some(after_anchor.resource_id),
                event_kind: EVENT_KIND_RESOLVER_CHANGED,
                before_state: json!({
                    "resolver": serde_json::Value::Null,
                }),
                after_state: json!({
                    "resolver": current_resolver,
                    "namehash": name.namehash.clone(),
                    "source_event": "AuthorityEpochChanged",
                }),
                identity_suffix: format!(
                    "resolver-boundary:{}:{}:{}:{}",
                    reference.block_hash,
                    name.logical_name_id,
                    effective_time.unix_timestamp(),
                    after_anchor.authority_key
                ),
            },
            BoundaryEventSource {
                source_family: after_anchor.binding_source_family.clone(),
                manifest_version: after_anchor.binding_manifest_version,
                source_manifest_id: source_manifest_id_if_known(after_anchor.binding_manifest_id),
                canonicality_state: reference.canonicality_state,
            },
        ));
    }

    Ok(())
}

fn authority_eq(left: Option<&AuthorityAnchor>, right: Option<&AuthorityAnchor>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.authority_key == right.authority_key,
        _ => false,
    }
}

pub(super) fn active_anchor_for_history(
    history: &NameHistory,
    chain: &str,
) -> Option<AuthorityAnchor> {
    if let Some(wrapper_key) = history.current_wrapper_key.as_ref()
        && let Some(wrapper) = history.wrapper_authorities.get(wrapper_key)
    {
        return Some(build_wrapper_anchor(wrapper));
    }
    if let Some(registration) = history.current_registration.as_ref() {
        return Some(build_registrar_anchor(registration));
    }
    registry_anchor_for_history(history, chain, &history.labelhash)
}

pub(super) fn active_anchor_for_observation(
    history: &NameHistory,
    reference: &ObservationRef,
) -> Option<AuthorityAnchor> {
    if let Some(wrapper_key) = history.current_wrapper_key.as_ref()
        && let Some(wrapper) = history.wrapper_authorities.get(wrapper_key)
    {
        return Some(build_wrapper_anchor(wrapper));
    }
    if let Some(registration) = history.current_registration.as_ref() {
        if registration
            .release_ref
            .as_ref()
            .is_some_and(|release_ref| release_ref.block_timestamp <= reference.block_timestamp)
        {
            return registry_anchor_for_history(history, &reference.chain_id, &history.labelhash);
        }
        return Some(build_registrar_anchor(registration));
    }
    registry_anchor_for_history(history, &reference.chain_id, &history.labelhash)
}

pub(super) fn clear_stale_wrapper_authority_for_registration_grant(
    history: &mut NameHistory,
    reference: &ObservationRef,
) -> Result<()> {
    if history.current_wrapper_key.is_none() {
        return Ok(());
    }
    let before_anchor = active_anchor_for_observation(history, reference);
    history.current_wrapper_key = None;
    let after_anchor = active_anchor_for_observation(history, reference);
    transition_authority(
        history,
        before_anchor,
        after_anchor,
        &reference.as_boundary_ref(),
        reference.block_timestamp,
    )
}

pub(super) fn current_resolver_matches(history: &NameHistory, resolver: &str) -> bool {
    nonzero_address(history.current_resolver.as_deref())
        .is_some_and(|current| current.eq_ignore_ascii_case(resolver))
}

pub(super) fn nonzero_address(value: Option<&str>) -> Option<String> {
    value
        .filter(|address| !address.eq_ignore_ascii_case(ZERO_ADDRESS))
        .map(ToOwned::to_owned)
}

impl RegistrationLease {
    pub(super) fn reference_chain(&self) -> String {
        self.start_ref.chain_id.clone()
    }
}

impl ObservationRef {
    pub(super) fn as_boundary_ref(&self) -> BoundaryRef {
        BoundaryRef {
            chain_id: self.chain_id.clone(),
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            block_timestamp: self.block_timestamp,
            canonicality_state: self.canonicality_state,
            namespace: self.namespace.clone(),
        }
    }
}

pub(super) fn release_after_grace(expiry: OffsetDateTime) -> Result<OffsetDateTime> {
    let release_unix = expiry
        .unix_timestamp()
        .checked_add(ENS_GRACE_PERIOD_SECS)
        .context("ENSv1 release timestamp overflowed i64")?;
    OffsetDateTime::from_unix_timestamp(release_unix)
        .context("ENSv1 release timestamp is not a valid unix timestamp")
}
