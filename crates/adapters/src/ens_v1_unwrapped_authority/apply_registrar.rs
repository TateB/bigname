use super::*;

pub(super) fn apply_registration_granted(
    history: &mut NameHistory,
    event: NameRegistrationObservation,
    block_index: &CanonicalBlockIndex,
) -> Result<()> {
    let name = observe_registrar_name_with_reference(
        &event.label,
        &event.reference,
        ENS_NORMALIZER_VERSION,
    )?;
    history
        .first_name_ref
        .get_or_insert(event.reference.clone());
    history.name = Some(name.clone());
    history.latest_registry_owner_before_registration = history.latest_registry_owner_ref.clone();

    let before_anchor = active_anchor_for_observation(history, &event.reference);
    let authority_key = format!(
        "registrar:{}:{}:{}:{}:{}",
        event.reference.chain_id,
        event.reference.source_manifest_id,
        event.labelhash,
        event.reference.block_hash,
        event.reference.log_index.unwrap_or_default()
    );
    let lease = RegistrationLease {
        authority_key,
        labelhash: event.labelhash.clone(),
        registrant: event.registrant.clone(),
        expiry: event.expiry,
        release_ref: block_index.first_block_at_or_after(
            release_after_grace(event.expiry)?,
            &event.reference.namespace,
        ),
        start_ref: event.reference.clone(),
    };
    let after_anchor = Some(build_registrar_anchor(&lease));
    let before_expiry = history
        .current_registration
        .as_ref()
        .map(|value| value.expiry);
    history.current_registration = Some(lease.clone());
    history.superseded_registration = None;

    history.events.push(build_normalized_event(
        &event.reference,
        Some(name.logical_name_id.clone()),
        after_anchor.as_ref().map(|value| value.resource_id),
        EVENT_KIND_REGISTRATION_GRANTED,
        json!({
            "authority_kind": before_anchor.as_ref().map(|value| value.kind.as_str()),
            "registrant": before_anchor.as_ref().and_then(|value| value.token_lineage_id).map(|_| serde_json::Value::Null),
        }),
        json!({
            "authority_kind": "registrar",
            "authority_key": lease.authority_key,
            "registrant": event.registrant,
            "expiry": event.expiry.unix_timestamp(),
            "labelhash": event.labelhash,
        }),
        format!(
            "grant:{}:{}:{}",
            event.reference.block_hash,
            event.reference.transaction_hash.as_deref().unwrap_or_default(),
            event.reference.log_index.unwrap_or_default()
        ),
    ));
    history.events.push(build_normalized_event(
        &event.reference,
        Some(name.logical_name_id.clone()),
        after_anchor.as_ref().map(|value| value.resource_id),
        EVENT_KIND_EXPIRY_CHANGED,
        json!({
            "expiry": before_expiry.map(|value| value.unix_timestamp()),
        }),
        json!({
            "expiry": event.expiry.unix_timestamp(),
        }),
        format!(
            "expiry:{}:{}:{}",
            event.reference.block_hash,
            event
                .reference
                .transaction_hash
                .as_deref()
                .unwrap_or_default(),
            event.reference.log_index.unwrap_or_default()
        ),
    ));
    if let (Some(anchor), Some(subject)) = (
        after_anchor.as_ref(),
        nonzero_address(Some(event.registrant.as_str())),
    ) {
        emit_observation_permission_grants(
            &mut history.events,
            &event.reference,
            &name.logical_name_id,
            anchor,
            &subject,
            history.current_resolver.as_deref(),
            EVENT_KIND_REGISTRATION_GRANTED,
        );
    }

    let surface_after_anchor = active_anchor_for_observation(history, &event.reference);
    transition_authority(
        history,
        before_anchor,
        surface_after_anchor,
        &event.reference.as_boundary_ref(),
        event.reference.block_timestamp,
    )?;
    Ok(())
}

pub(super) fn apply_registration_renewed(
    history: &mut NameHistory,
    event: NameRenewalObservation,
    block_index: &CanonicalBlockIndex,
) -> Result<()> {
    let observed_name = observe_registrar_name_with_reference(
        &event.label,
        &event.reference,
        ENS_NORMALIZER_VERSION,
    )?;
    history.name = Some(observed_name);
    history
        .first_name_ref
        .get_or_insert(event.reference.clone());

    restore_superseded_registration_for_renewal(history, &event)?;

    if history.current_registration.is_none() {
        let name = history
            .name
            .clone()
            .context("failed to build registrar name metadata")?;
        let lease = RegistrationLease {
            authority_key: format!(
                "registrar:{}:{}:{}:{}:{}",
                event.reference.chain_id,
                event.reference.source_manifest_id,
                event.labelhash,
                event.reference.block_hash,
                event.reference.log_index.unwrap_or_default()
            ),
            labelhash: event.labelhash.clone(),
            registrant: history
                .current_registration
                .as_ref()
                .map(|value| value.registrant.clone())
                .unwrap_or_else(|| ZERO_ADDRESS.to_owned()),
            expiry: event.expiry,
            release_ref: block_index.first_block_at_or_after(
                release_after_grace(event.expiry)?,
                &event.reference.namespace,
            ),
            start_ref: event.reference.clone(),
        };
        let registrar_anchor = Some(build_registrar_anchor(&lease));
        let before_anchor = active_anchor_for_observation(history, &event.reference);
        history.current_registration = Some(lease.clone());
        history.superseded_registration = None;
        let surface_after_anchor = active_anchor_for_observation(history, &event.reference);
        transition_authority(
            history,
            before_anchor,
            surface_after_anchor,
            &event.reference.as_boundary_ref(),
            event.reference.block_timestamp,
        )?;
        history.events.push(build_normalized_event(
            &event.reference,
            Some(name.logical_name_id.clone()),
            registrar_anchor.as_ref().map(|value| value.resource_id),
            EVENT_KIND_REGISTRATION_GRANTED,
            json!({}),
            json!({
                "authority_kind": "registrar",
                "authority_key": lease.authority_key,
                "registrant": lease.registrant,
                "expiry": event.expiry.unix_timestamp(),
                "labelhash": event.labelhash,
            }),
            format!(
                "grant:{}:{}:{}",
                event.reference.block_hash,
                event
                    .reference
                    .transaction_hash
                    .as_deref()
                    .unwrap_or_default(),
                event.reference.log_index.unwrap_or_default()
            ),
        ));
        if let (Some(anchor), Some(subject)) = (
            registrar_anchor.as_ref(),
            nonzero_address(Some(lease.registrant.as_str())),
        ) {
            emit_observation_permission_grants(
                &mut history.events,
                &event.reference,
                &name.logical_name_id,
                anchor,
                &subject,
                history.current_resolver.as_deref(),
                EVENT_KIND_REGISTRATION_GRANTED,
            );
        }
    }
    let name = history
        .name
        .clone()
        .context("failed to build registrar name metadata")?;

    if let Some(current_registration) = history.current_registration.as_mut() {
        let before_expiry = current_registration.expiry;
        current_registration.expiry = event.expiry;
        current_registration.release_ref = block_index.first_block_at_or_after(
            release_after_grace(event.expiry)?,
            &event.reference.namespace,
        );

        history.events.push(build_normalized_event(
            &event.reference,
            Some(name.logical_name_id.clone()),
            Some(deterministic_uuid(&format!(
                "resource:{}",
                current_registration.authority_key
            ))),
            EVENT_KIND_REGISTRATION_RENEWED,
            json!({
                "expiry": before_expiry.unix_timestamp(),
            }),
            json!({
                "expiry": event.expiry.unix_timestamp(),
                "labelhash": event.labelhash,
            }),
            format!(
                "renewal:{}:{}:{}",
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
            Some(name.logical_name_id.clone()),
            Some(deterministic_uuid(&format!(
                "resource:{}",
                current_registration.authority_key
            ))),
            EVENT_KIND_EXPIRY_CHANGED,
            json!({
                "expiry": before_expiry.unix_timestamp(),
            }),
            json!({
                "expiry": event.expiry.unix_timestamp(),
            }),
            format!(
                "expiry:{}:{}:{}",
                event.reference.block_hash,
                event
                    .reference
                    .transaction_hash
                    .as_deref()
                    .unwrap_or_default(),
                event.reference.log_index.unwrap_or_default()
            ),
        ));
    }
    Ok(())
}

pub(super) fn settle_due_registration_release(
    history: &mut NameHistory,
    boundary: &BoundaryRef,
) -> Result<()> {
    if history
        .superseded_registration
        .as_ref()
        .is_some_and(|lease| registration_released_at_or_before(lease, boundary.block_timestamp))
    {
        history.superseded_registration = None;
    }
    let Some(lease) = history.current_registration.take() else {
        return Ok(());
    };
    let Some(release_ref) = lease.release_ref.clone() else {
        history.current_registration = Some(lease);
        return Ok(());
    };
    if release_ref.block_timestamp > boundary.block_timestamp {
        history.current_registration = Some(lease);
        return Ok(());
    }

    emit_registration_released_event(history, &lease, &release_ref)?;
    if history.current_wrapper_key.is_some() {
        return Ok(());
    }
    let registry_after =
        registry_anchor_for_history(history, &lease.reference_chain(), &lease.labelhash);
    transition_authority(
        history,
        Some(build_registrar_anchor(&lease)),
        registry_after.clone(),
        &release_ref,
        release_ref.block_timestamp,
    )?;
    if let (Some(name), Some(anchor), Some(subject)) = (
        history.name.as_ref(),
        registry_after.as_ref(),
        nonzero_address(history.current_registry_owner.as_deref()),
    ) {
        emit_boundary_permission_grants(
            &mut history.events,
            &release_ref,
            &name.logical_name_id,
            anchor,
            &subject,
            history.current_resolver.as_deref(),
            EVENT_KIND_REGISTRATION_RELEASED,
        );
    }
    Ok(())
}

pub(super) fn apply_token_transferred(
    history: &mut NameHistory,
    event: TokenTransferObservation,
) -> Result<()> {
    let Some(name) = history.name.clone() else {
        return Ok(());
    };
    let current_resolver = history.current_resolver.clone();
    let mut transfer_applied_to_superseded_registration = false;
    let anchor = {
        let current_registration =
            if let Some(current_registration) = history.current_registration.as_mut() {
                current_registration
            } else if let Some(superseded_registration) = history.superseded_registration.as_mut() {
                if registration_released_at_or_before(
                    superseded_registration,
                    event.reference.block_timestamp,
                ) {
                    return Ok(());
                }
                transfer_applied_to_superseded_registration = true;
                superseded_registration
            } else {
                return Ok(());
            };
        if event.from_address == ZERO_ADDRESS || event.to_address == ZERO_ADDRESS {
            return Ok(());
        }
        current_registration.registrant = event.to_address.clone();
        build_registrar_anchor(current_registration)
    };
    let previous_registrant = event.from_address.clone();
    history.events.push(build_normalized_event(
        &event.reference,
        Some(name.logical_name_id.clone()),
        Some(anchor.resource_id),
        EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
        json!({
            "from": previous_registrant,
        }),
        json!({
            "to": event.to_address,
            "labelhash": event.labelhash,
        }),
        format!(
            "token-transfer:{}:{}:{}",
            event.reference.block_hash,
            event
                .reference
                .transaction_hash
                .as_deref()
                .unwrap_or_default(),
            event.reference.log_index.unwrap_or_default()
        ),
    ));
    emit_observation_permission_subject_change(
        &mut history.events,
        &event.reference,
        &name.logical_name_id,
        &anchor,
        PermissionSubjectChange {
            before_subject: Some(previous_registrant.as_str()),
            after_subject: Some(event.to_address.as_str()),
            resolver: current_resolver.as_deref(),
            source_event_kind: EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
        },
    );
    if transfer_applied_to_superseded_registration
        && history.current_wrapper_key.is_none()
        && nonzero_address(history.current_registry_owner.as_deref())
            .is_some_and(|owner| owner.eq_ignore_ascii_case(&event.to_address))
    {
        let before_anchor = active_anchor_for_observation(history, &event.reference);
        if let Some(lease) = history.superseded_registration.take() {
            history.current_registration = Some(lease);
            let after_anchor = active_anchor_for_observation(history, &event.reference);
            transition_authority(
                history,
                before_anchor,
                after_anchor,
                &event.reference.as_boundary_ref(),
                event.reference.block_timestamp,
            )?;
        }
    }
    Ok(())
}

fn restore_superseded_registration_for_renewal(
    history: &mut NameHistory,
    event: &NameRenewalObservation,
) -> Result<()> {
    if history.current_registration.is_some() {
        return Ok(());
    }
    let Some(lease) = history.superseded_registration.take() else {
        return Ok(());
    };
    if !lease.labelhash.eq_ignore_ascii_case(&event.labelhash)
        || registration_released_at_or_before(&lease, event.reference.block_timestamp)
    {
        history.superseded_registration = Some(lease);
        return Ok(());
    }

    let before_anchor = active_anchor_for_observation(history, &event.reference);
    history.current_registration = Some(lease);
    let after_anchor = active_anchor_for_observation(history, &event.reference);
    transition_authority(
        history,
        before_anchor,
        after_anchor,
        &event.reference.as_boundary_ref(),
        event.reference.block_timestamp,
    )
}

fn registration_released_at_or_before(
    lease: &RegistrationLease,
    timestamp: OffsetDateTime,
) -> bool {
    lease
        .release_ref
        .as_ref()
        .is_some_and(|release_ref| release_ref.block_timestamp <= timestamp)
}
