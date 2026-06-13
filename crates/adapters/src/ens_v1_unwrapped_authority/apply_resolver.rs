use super::*;

pub(super) fn apply_resolver_changed(
    history: &mut NameHistory,
    event: ResolverObservation,
) -> Result<()> {
    let before_resolver = history.current_resolver.clone();
    let before_normalized_resolver = nonzero_address(before_resolver.as_deref());
    let after_normalized_resolver = nonzero_address(Some(event.resolver.as_str()));
    if before_normalized_resolver != after_normalized_resolver {
        history.current_record_version = None;
    }
    history.current_resolver = Some(event.resolver.clone());

    let Some(name) = history.name.clone() else {
        return Ok(());
    };
    let authority = active_anchor_for_observation(history, &event.reference);
    history.events.push(build_normalized_event(
        &event.reference,
        Some(name.logical_name_id.clone()),
        authority.as_ref().map(|value| value.resource_id),
        EVENT_KIND_RESOLVER_CHANGED,
        json!({
            "resolver": before_resolver,
        }),
        resolver_changed_after_state(&event, None),
        format!(
            "resolver:{}:{}:{}",
            event.reference.block_hash,
            event
                .reference
                .transaction_hash
                .as_deref()
                .unwrap_or_default(),
            event.reference.log_index.unwrap_or_default()
        ),
    ));
    let authority_subject = match authority.as_ref().map(|value| value.kind) {
        Some(AuthorityKind::Wrapper) => history
            .current_wrapper_key
            .as_ref()
            .and_then(|key| history.wrapper_authorities.get(key))
            .map(|wrapper| wrapper.owner.as_str()),
        Some(AuthorityKind::Registrar) => history
            .current_registration
            .as_ref()
            .map(|registration| registration.registrant.as_str()),
        Some(AuthorityKind::RegistryOnly) => history.current_registry_owner.as_deref(),
        None => None,
    };
    if let (Some(anchor), Some(subject)) = (authority.as_ref(), nonzero_address(authority_subject))
    {
        let before_resolver = before_normalized_resolver;
        let after_resolver = after_normalized_resolver;
        if before_resolver != after_resolver {
            if let Some(previous_resolver) = before_resolver.as_deref() {
                history
                    .events
                    .push(build_observation_permission_change_event(
                        &event.reference,
                        &name.logical_name_id,
                        anchor,
                        PermissionChange {
                            subject: &subject,
                            scope: resolver_permission_scope(
                                &event.reference.chain_id,
                                previous_resolver,
                            ),
                            scope_identity: format!("resolver:{previous_resolver}"),
                            power: PERMISSION_POWER_RESOLVER_CONTROL,
                            action: PermissionAction::Revoke,
                            source_event_kind: EVENT_KIND_RESOLVER_CHANGED,
                        },
                    ));
            }
            if let Some(current_resolver) = after_resolver.as_deref() {
                history
                    .events
                    .push(build_observation_permission_change_event(
                        &event.reference,
                        &name.logical_name_id,
                        anchor,
                        PermissionChange {
                            subject: &subject,
                            scope: resolver_permission_scope(
                                &event.reference.chain_id,
                                current_resolver,
                            ),
                            scope_identity: format!("resolver:{current_resolver}"),
                            power: PERMISSION_POWER_RESOLVER_CONTROL,
                            action: PermissionAction::Grant,
                            source_event_kind: EVENT_KIND_RESOLVER_CHANGED,
                        },
                    ));
            }
        }
    }
    Ok(())
}

pub(super) fn apply_record_changed(
    history: &mut NameHistory,
    event: RecordChangeObservation,
) -> Result<()> {
    let Some(name) = history.name.clone() else {
        return Ok(());
    };
    let Some(authority) = active_anchor_for_observation(history, &event.reference) else {
        return Ok(());
    };
    history.events.push(build_normalized_event(
        &event.reference,
        Some(name.logical_name_id.clone()),
        Some(authority.resource_id),
        EVENT_KIND_RECORD_CHANGED,
        json!({}),
        record_changed_after_state(&event, None),
        format!(
            "record-change:{}:{}:{}",
            event.reference.block_hash,
            event
                .reference
                .transaction_hash
                .as_deref()
                .unwrap_or_default(),
            event.reference.log_index.unwrap_or_default()
        ),
    ));
    Ok(())
}

pub(super) fn apply_record_version_changed(
    history: &mut NameHistory,
    event: RecordVersionObservation,
) -> Result<()> {
    let Some(name) = history.name.clone() else {
        return Ok(());
    };
    if !current_resolver_matches(history, &event.resolver) {
        return Ok(());
    }
    let Some(authority) = active_anchor_for_observation(history, &event.reference) else {
        return Ok(());
    };
    let before_version = history.current_record_version;
    history.current_record_version = Some(event.record_version);
    history.events.push(build_normalized_event(
        &event.reference,
        Some(name.logical_name_id.clone()),
        Some(authority.resource_id),
        EVENT_KIND_RECORD_VERSION_CHANGED,
        json!({
            "record_version": before_version,
        }),
        record_version_changed_after_state(&event, None),
        format!(
            "record-version:{}:{}:{}",
            event.reference.block_hash,
            event
                .reference
                .transaction_hash
                .as_deref()
                .unwrap_or_default(),
            event.reference.log_index.unwrap_or_default()
        ),
    ));
    Ok(())
}
