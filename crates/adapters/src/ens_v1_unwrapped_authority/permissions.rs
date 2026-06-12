use super::*;

fn resource_permission_scope() -> serde_json::Value {
    json!({
        "kind": "resource",
    })
}

pub(super) fn resolver_permission_scope(chain_id: &str, resolver: &str) -> serde_json::Value {
    json!({
        "kind": "resolver",
        "chain_id": chain_id,
        "resolver_address": resolver,
    })
}

fn permission_source(anchor: &AuthorityAnchor, source_event_kind: &str) -> serde_json::Value {
    json!({
        "kind": "ens_v1_authority",
        "authority_kind": anchor.kind.as_str(),
        "authority_key": anchor.authority_key,
        "source_event_kind": source_event_kind,
    })
}

fn permission_state(
    subject: &str,
    scope: serde_json::Value,
    effective_powers: &[&str],
    grant_source: Option<serde_json::Value>,
    revocation_source: Option<serde_json::Value>,
) -> serde_json::Value {
    json!({
        "subject": subject,
        "scope": scope,
        "effective_powers": effective_powers,
        "grant_source": grant_source,
        "revocation_source": revocation_source,
        "inheritance_path": [],
        "transfer_behavior": PERMISSION_TRANSFER_BEHAVIOR,
    })
}

pub(super) struct PermissionChange<'a> {
    pub(super) subject: &'a str,
    pub(super) scope: serde_json::Value,
    pub(super) scope_identity: String,
    pub(super) power: &'a str,
    pub(super) action: PermissionAction,
    pub(super) source_event_kind: &'a str,
}

pub(super) struct PermissionSubjectChange<'a> {
    pub(super) before_subject: Option<&'a str>,
    pub(super) after_subject: Option<&'a str>,
    pub(super) resolver: Option<&'a str>,
    pub(super) source_event_kind: &'a str,
    pub(super) identity_suffix: Option<&'a str>,
}

pub(super) struct WrapperFuseChange<'a> {
    pub(super) namehash: &'a str,
    pub(super) before_fuses: Option<i64>,
    pub(super) after_fuses: i64,
    pub(super) identity_prefix: &'a str,
    pub(super) event_kind: &'a str,
}

pub(super) fn build_observation_permission_change_event(
    reference: &ObservationRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    change: PermissionChange<'_>,
) -> NormalizedEvent {
    build_observation_permission_change_event_with_identity_suffix(
        reference,
        logical_name_id,
        anchor,
        change,
        None,
    )
}

fn build_observation_permission_change_event_with_identity_suffix(
    reference: &ObservationRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    change: PermissionChange<'_>,
    identity_suffix: Option<&str>,
) -> NormalizedEvent {
    let event_identity = observation_permission_identity(&change, reference, identity_suffix);
    let source = permission_source(anchor, change.source_event_kind);
    let before_state = match change.action {
        PermissionAction::Grant => {
            permission_state(change.subject, change.scope.clone(), &[], None, None)
        }
        PermissionAction::Revoke => permission_state(
            change.subject,
            change.scope.clone(),
            &[change.power],
            Some(source.clone()),
            None,
        ),
    };
    let after_state = match change.action {
        PermissionAction::Grant => permission_state(
            change.subject,
            change.scope,
            &[change.power],
            Some(source),
            None,
        ),
        PermissionAction::Revoke => {
            permission_state(change.subject, change.scope, &[], None, Some(source))
        }
    };

    build_normalized_event(
        reference,
        Some(logical_name_id.to_owned()),
        Some(anchor.resource_id),
        EVENT_KIND_PERMISSION_CHANGED,
        before_state,
        after_state,
        event_identity,
    )
}

fn observation_permission_identity(
    change: &PermissionChange<'_>,
    reference: &ObservationRef,
    identity_suffix: Option<&str>,
) -> String {
    let mut identity = format!(
        "permission:{}:{}:{}:{}:{}:{}",
        change.action.as_str(),
        change.scope_identity,
        change.subject,
        reference.block_hash,
        reference.transaction_hash.as_deref().unwrap_or_default(),
        reference.log_index.unwrap_or_default()
    );
    if let Some(suffix) = identity_suffix {
        identity.push(':');
        identity.push_str(suffix);
    }
    identity
}

fn build_boundary_permission_change_event(
    reference: &BoundaryRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    change: PermissionChange<'_>,
) -> NormalizedEvent {
    let source = permission_source(anchor, change.source_event_kind);
    let before_state = match change.action {
        PermissionAction::Grant => {
            permission_state(change.subject, change.scope.clone(), &[], None, None)
        }
        PermissionAction::Revoke => permission_state(
            change.subject,
            change.scope.clone(),
            &[change.power],
            Some(source.clone()),
            None,
        ),
    };
    let after_state = match change.action {
        PermissionAction::Grant => permission_state(
            change.subject,
            change.scope,
            &[change.power],
            Some(source),
            None,
        ),
        PermissionAction::Revoke => {
            permission_state(change.subject, change.scope, &[], None, Some(source))
        }
    };

    build_boundary_event(
        reference,
        BoundaryEventPayload {
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(anchor.resource_id),
            event_kind: EVENT_KIND_PERMISSION_CHANGED,
            before_state,
            after_state,
            identity_suffix: format!(
                "permission:{}:{}:{}:{}:{}",
                change.action.as_str(),
                change.scope_identity,
                change.subject,
                reference.block_hash,
                anchor.authority_key
            ),
        },
        BoundaryEventSource {
            source_family: anchor.binding_source_family.clone(),
            manifest_version: anchor.binding_manifest_version,
            source_manifest_id: source_manifest_id_if_known(anchor.binding_manifest_id),
            canonicality_state: reference.canonicality_state,
        },
    )
}

pub(super) fn emit_observation_permission_grants(
    events: &mut Vec<NormalizedEvent>,
    reference: &ObservationRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    subject: &str,
    resolver: Option<&str>,
    source_event_kind: &str,
) {
    emit_observation_permission_grants_with_identity_suffix(
        events,
        reference,
        logical_name_id,
        anchor,
        subject,
        resolver,
        source_event_kind,
        None,
    );
}

fn emit_observation_permission_grants_with_identity_suffix(
    events: &mut Vec<NormalizedEvent>,
    reference: &ObservationRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    subject: &str,
    resolver: Option<&str>,
    source_event_kind: &str,
    identity_suffix: Option<&str>,
) {
    events.push(
        build_observation_permission_change_event_with_identity_suffix(
            reference,
            logical_name_id,
            anchor,
            PermissionChange {
                subject,
                scope: resource_permission_scope(),
                scope_identity: "resource".to_owned(),
                power: PERMISSION_POWER_RESOURCE_CONTROL,
                action: PermissionAction::Grant,
                source_event_kind,
            },
            identity_suffix,
        ),
    );

    if let Some(resolver) = nonzero_address(resolver) {
        events.push(
            build_observation_permission_change_event_with_identity_suffix(
                reference,
                logical_name_id,
                anchor,
                PermissionChange {
                    subject,
                    scope: resolver_permission_scope(&reference.chain_id, &resolver),
                    scope_identity: format!("resolver:{resolver}"),
                    power: PERMISSION_POWER_RESOLVER_CONTROL,
                    action: PermissionAction::Grant,
                    source_event_kind,
                },
                identity_suffix,
            ),
        );
    }
}

pub(super) fn emit_boundary_permission_grants(
    events: &mut Vec<NormalizedEvent>,
    reference: &BoundaryRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    subject: &str,
    resolver: Option<&str>,
    source_event_kind: &str,
) {
    events.push(build_boundary_permission_change_event(
        reference,
        logical_name_id,
        anchor,
        PermissionChange {
            subject,
            scope: resource_permission_scope(),
            scope_identity: "resource".to_owned(),
            power: PERMISSION_POWER_RESOURCE_CONTROL,
            action: PermissionAction::Grant,
            source_event_kind,
        },
    ));

    if let Some(resolver) = nonzero_address(resolver) {
        events.push(build_boundary_permission_change_event(
            reference,
            logical_name_id,
            anchor,
            PermissionChange {
                subject,
                scope: resolver_permission_scope(&reference.chain_id, &resolver),
                scope_identity: format!("resolver:{resolver}"),
                power: PERMISSION_POWER_RESOLVER_CONTROL,
                action: PermissionAction::Grant,
                source_event_kind,
            },
        ));
    }
}

pub(super) fn emit_observation_permission_subject_change(
    events: &mut Vec<NormalizedEvent>,
    reference: &ObservationRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    change: PermissionSubjectChange<'_>,
) {
    let before_subject = nonzero_address(change.before_subject);
    let after_subject = nonzero_address(change.after_subject);
    if before_subject == after_subject {
        return;
    }

    if let Some(subject) = before_subject.as_deref() {
        events.push(
            build_observation_permission_change_event_with_identity_suffix(
                reference,
                logical_name_id,
                anchor,
                PermissionChange {
                    subject,
                    scope: resource_permission_scope(),
                    scope_identity: "resource".to_owned(),
                    power: PERMISSION_POWER_RESOURCE_CONTROL,
                    action: PermissionAction::Revoke,
                    source_event_kind: change.source_event_kind,
                },
                change.identity_suffix,
            ),
        );
        if let Some(resolver) = nonzero_address(change.resolver) {
            events.push(
                build_observation_permission_change_event_with_identity_suffix(
                    reference,
                    logical_name_id,
                    anchor,
                    PermissionChange {
                        subject,
                        scope: resolver_permission_scope(&reference.chain_id, &resolver),
                        scope_identity: format!("resolver:{resolver}"),
                        power: PERMISSION_POWER_RESOLVER_CONTROL,
                        action: PermissionAction::Revoke,
                        source_event_kind: change.source_event_kind,
                    },
                    change.identity_suffix,
                ),
            );
        }
    }

    if let Some(subject) = after_subject.as_deref() {
        emit_observation_permission_grants_with_identity_suffix(
            events,
            reference,
            logical_name_id,
            anchor,
            subject,
            change.resolver,
            change.source_event_kind,
            change.identity_suffix,
        );
    }
}

pub(super) fn emit_wrapper_fuse_event(
    events: &mut Vec<NormalizedEvent>,
    reference: &ObservationRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    change: WrapperFuseChange<'_>,
) {
    if change.before_fuses == Some(change.after_fuses) {
        return;
    }

    events.push(build_normalized_event(
        reference,
        Some(logical_name_id.to_owned()),
        Some(anchor.resource_id),
        change.event_kind,
        json!({
            "fuses": change.before_fuses,
        }),
        json!({
            "fuses": change.after_fuses,
            "namehash": change.namehash,
            "authority_kind": anchor.kind.as_str(),
            "authority_key": anchor.authority_key,
        }),
        format!(
            "{}:{}:{}:{}",
            change.identity_prefix,
            reference.block_hash,
            reference.transaction_hash.as_deref().unwrap_or_default(),
            reference.log_index.unwrap_or_default()
        ),
    ));
}
