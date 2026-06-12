use super::*;

pub(super) fn apply_wrapper_name_wrapped(
    history: &mut NameHistory,
    event: WrapperNameWrappedObservation,
) -> Result<()> {
    history
        .first_name_ref
        .get_or_insert(event.reference.clone());
    history.name = Some(event.name.clone());
    let before_anchor = active_anchor_for_observation(history, &event.reference);
    let before_owner = history
        .current_wrapper_key
        .as_ref()
        .and_then(|key| history.wrapper_authorities.get(key))
        .map(|wrapper| wrapper.owner.clone());
    let before_fuses = history
        .current_wrapper_key
        .as_ref()
        .and_then(|key| history.wrapper_authorities.get(key))
        .map(|wrapper| wrapper.fuses);
    let before_expiry = history
        .current_wrapper_key
        .as_ref()
        .and_then(|key| history.wrapper_authorities.get(key))
        .map(|wrapper| wrapper.expiry);
    let authority_key = format!(
        "wrapper:{}:{}:{}:{}:{}",
        event.reference.chain_id,
        event.reference.source_manifest_id,
        event.name.namehash,
        event.reference.block_hash,
        event.reference.log_index.unwrap_or_default()
    );
    let wrapper = WrapperAuthority {
        authority_key: authority_key.clone(),
        node: event.name.namehash.clone(),
        owner: event.owner.clone(),
        fuses: event.fuses,
        expiry: event.expiry,
        start_ref: event.reference.clone(),
        end_ref: None,
    };
    history
        .wrapper_authorities
        .insert(authority_key.clone(), wrapper.clone());
    history.current_wrapper_key = Some(authority_key);
    let after_anchor = Some(build_wrapper_anchor(&wrapper));

    history.events.push(build_normalized_event(
        &event.reference,
        Some(event.name.logical_name_id.clone()),
        after_anchor.as_ref().map(|value| value.resource_id),
        EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
        json!({
            "from": before_owner,
            "authority_kind": before_anchor.as_ref().map(|value| value.kind.as_str()),
        }),
        json!({
            "to": event.owner,
            "authority_kind": "wrapper",
            "authority_key": wrapper.authority_key,
            "namehash": event.name.namehash,
        }),
        format!(
            "wrapper-token:{}:{}:{}",
            event.reference.block_hash,
            event
                .reference
                .transaction_hash
                .as_deref()
                .unwrap_or_default(),
            event.reference.log_index.unwrap_or_default()
        ),
    ));
    history.events.push(build_normalized_event(
        &event.reference,
        Some(event.name.logical_name_id.clone()),
        after_anchor.as_ref().map(|value| value.resource_id),
        EVENT_KIND_EXPIRY_CHANGED,
        json!({
            "expiry": before_expiry.map(|value| value.unix_timestamp()),
        }),
        json!({
            "expiry": event.expiry.unix_timestamp(),
            "namehash": event.name.namehash,
            "authority_kind": "wrapper",
            "authority_key": wrapper.authority_key,
        }),
        format!(
            "wrapper-expiry:{}:{}:{}",
            event.reference.block_hash,
            event
                .reference
                .transaction_hash
                .as_deref()
                .unwrap_or_default(),
            event.reference.log_index.unwrap_or_default()
        ),
    ));
    emit_wrapper_fuse_event(
        &mut history.events,
        &event.reference,
        &event.name.logical_name_id,
        after_anchor.as_ref().context("wrapper anchor must exist")?,
        WrapperFuseChange {
            namehash: &event.name.namehash,
            before_fuses,
            after_fuses: event.fuses,
            identity_prefix: "wrapper-fuses",
            event_kind: EVENT_KIND_PERMISSION_SCOPE_CHANGED,
        },
    );
    transition_authority(
        history,
        before_anchor,
        after_anchor,
        &event.reference.as_boundary_ref(),
        event.reference.block_timestamp,
    )?;
    Ok(())
}

pub(super) fn apply_wrapper_name_unwrapped(
    history: &mut NameHistory,
    event: WrapperNameUnwrappedObservation,
) -> Result<()> {
    if history.name.is_none() {
        return Ok(());
    };
    let before_anchor = active_anchor_for_observation(history, &event.reference);
    if let Some(wrapper_key) = history.current_wrapper_key.take()
        && let Some(wrapper) = history.wrapper_authorities.get_mut(&wrapper_key)
    {
        wrapper.end_ref = Some(event.reference.clone());
    }
    let after_anchor = active_anchor_for_observation(history, &event.reference);
    transition_authority(
        history,
        before_anchor,
        after_anchor,
        &event.reference.as_boundary_ref(),
        event.reference.block_timestamp,
    )?;
    Ok(())
}

pub(super) fn apply_wrapper_fuses_set(
    history: &mut NameHistory,
    event: WrapperFusesObservation,
) -> Result<()> {
    let Some(name) = history.name.clone() else {
        return Ok(());
    };
    let Some(wrapper_key) = history.current_wrapper_key.clone() else {
        return Ok(());
    };
    let Some(wrapper) = history.wrapper_authorities.get_mut(&wrapper_key) else {
        return Ok(());
    };
    let before_fuses = wrapper.fuses;
    wrapper.fuses = event.fuses;
    let anchor = build_wrapper_anchor(wrapper);
    emit_wrapper_fuse_event(
        &mut history.events,
        &event.reference,
        &name.logical_name_id,
        &anchor,
        WrapperFuseChange {
            namehash: &event.namehash,
            before_fuses: Some(before_fuses),
            after_fuses: event.fuses,
            identity_prefix: "wrapper-fuses",
            event_kind: EVENT_KIND_PERMISSION_SCOPE_CHANGED,
        },
    );
    Ok(())
}

pub(super) fn apply_wrapper_expiry_extended(
    history: &mut NameHistory,
    event: WrapperExpiryObservation,
) -> Result<()> {
    let Some(name) = history.name.clone() else {
        return Ok(());
    };
    let Some(wrapper_key) = history.current_wrapper_key.clone() else {
        return Ok(());
    };
    let Some(wrapper) = history.wrapper_authorities.get_mut(&wrapper_key) else {
        return Ok(());
    };
    let before_expiry = wrapper.expiry;
    wrapper.expiry = event.expiry;
    let anchor = build_wrapper_anchor(wrapper);
    history.events.push(build_normalized_event(
        &event.reference,
        Some(name.logical_name_id.clone()),
        Some(anchor.resource_id),
        EVENT_KIND_EXPIRY_CHANGED,
        json!({
            "expiry": before_expiry.unix_timestamp(),
        }),
        json!({
            "expiry": event.expiry.unix_timestamp(),
            "namehash": event.namehash,
            "authority_kind": "wrapper",
            "authority_key": wrapper.authority_key,
        }),
        format!(
            "wrapper-expiry:{}:{}:{}",
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

pub(super) fn apply_wrapper_token_transferred(
    history: &mut NameHistory,
    event: WrapperTokenTransferObservation,
) -> Result<()> {
    if event.value != 1 {
        return Ok(());
    }
    if event.from_address == ZERO_ADDRESS || event.to_address == ZERO_ADDRESS {
        return Ok(());
    }
    let Some(name) = history.name.clone() else {
        return Ok(());
    };
    let Some(wrapper_key) = history.current_wrapper_key.clone() else {
        return Ok(());
    };
    let Some(wrapper) = history.wrapper_authorities.get_mut(&wrapper_key) else {
        return Ok(());
    };
    let before_owner = wrapper.owner.clone();
    wrapper.owner = event.to_address.clone();
    let anchor = build_wrapper_anchor(wrapper);
    let transfer_identity_suffix = event
        .transfer_index
        .map(|transfer_index| format!("batch:{transfer_index}"));
    let identity_suffix = if let Some(transfer_index) = event.transfer_index {
        format!(
            "wrapper-token:{}:{}:{}:{}",
            event.reference.block_hash,
            event
                .reference
                .transaction_hash
                .as_deref()
                .unwrap_or_default(),
            event.reference.log_index.unwrap_or_default(),
            transfer_index
        )
    } else {
        format!(
            "wrapper-token:{}:{}:{}",
            event.reference.block_hash,
            event
                .reference
                .transaction_hash
                .as_deref()
                .unwrap_or_default(),
            event.reference.log_index.unwrap_or_default()
        )
    };
    history.events.push(build_normalized_event(
        &event.reference,
        Some(name.logical_name_id.clone()),
        Some(anchor.resource_id),
        EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
        json!({
            "from": before_owner,
        }),
        json!({
            "to": event.to_address,
            "namehash": event.namehash,
            "authority_kind": "wrapper",
            "authority_key": wrapper.authority_key,
        }),
        identity_suffix,
    ));
    emit_observation_permission_subject_change(
        &mut history.events,
        &event.reference,
        &name.logical_name_id,
        &anchor,
        PermissionSubjectChange {
            before_subject: Some(before_owner.as_str()),
            after_subject: Some(event.to_address.as_str()),
            resolver: history.current_resolver.as_deref(),
            source_event_kind: EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
            identity_suffix: transfer_identity_suffix.as_deref(),
        },
    );
    Ok(())
}
