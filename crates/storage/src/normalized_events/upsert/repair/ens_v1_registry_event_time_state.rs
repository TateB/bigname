use serde_json::Value;

pub(super) fn authority_transfer_state_repair_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
    existing_after_state: &Value,
    incoming_after_state: &Value,
) -> bool {
    if existing_after_state != incoming_after_state {
        return false;
    }
    if !authority_transfer_owner_transition_allowed(existing_before_state, incoming_before_state) {
        return false;
    }
    let mut existing_without_owner = existing_before_state.clone();
    if let Some(object) = existing_without_owner.as_object_mut() {
        object.remove("owner");
    }
    let mut incoming_without_owner = incoming_before_state.clone();
    if let Some(object) = incoming_without_owner.as_object_mut() {
        object.remove("owner");
    }
    existing_without_owner == incoming_without_owner
}

fn authority_transfer_owner_transition_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
) -> bool {
    matches!(
        (
            owner_state(existing_before_state),
            owner_state(incoming_before_state)
        ),
        (Some(OwnerState::Known), Some(OwnerState::Known))
            | (Some(OwnerState::Known), Some(OwnerState::Null))
            | (Some(OwnerState::Null), Some(OwnerState::Known))
    )
}

enum OwnerState {
    Known,
    Null,
}

fn owner_state(value: &Value) -> Option<OwnerState> {
    match value.get("owner")? {
        Value::Null => Some(OwnerState::Null),
        Value::String(owner) if !owner.trim().is_empty() => Some(OwnerState::Known),
        _ => None,
    }
}

pub(super) fn record_version_state_repair_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
    existing_after_state: &Value,
    incoming_after_state: &Value,
) -> bool {
    if existing_after_state != incoming_after_state {
        return false;
    }
    let Some(after_version) = incoming_after_state
        .get("record_version")
        .and_then(Value::as_i64)
    else {
        return false;
    };
    let mut existing_without_version = existing_before_state.clone();
    if let Some(object) = existing_without_version.as_object_mut() {
        object.remove("record_version");
    }
    let mut incoming_without_version = incoming_before_state.clone();
    if let Some(object) = incoming_without_version.as_object_mut() {
        object.remove("record_version");
    }
    if existing_without_version != incoming_without_version {
        return false;
    }
    let existing_version = existing_before_state.get("record_version");
    let incoming_version = incoming_before_state.get("record_version");
    let previous_version = match (existing_version, incoming_version) {
        (Some(Value::Null), Some(value)) => value.as_i64(),
        (Some(value), Some(Value::Null)) => value.as_i64(),
        _ => None,
    };
    previous_version.and_then(|version| version.checked_add(1)) == Some(after_version)
}

pub(super) fn record_changed_text_value_state_repair_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
    existing_after_state: &Value,
    incoming_after_state: &Value,
) -> bool {
    if existing_before_state != incoming_before_state
        || !selector_text_record_state(existing_after_state)
        || !selector_text_record_state(incoming_after_state)
    {
        return false;
    }
    let existing_value = existing_after_state.get("value").and_then(Value::as_str);
    let incoming_value = incoming_after_state.get("value").and_then(Value::as_str);
    if existing_value.is_some() == incoming_value.is_some() {
        return false;
    }
    let mut existing_without_value = existing_after_state.clone();
    if let Some(object) = existing_without_value.as_object_mut() {
        object.remove("value");
    }
    let mut incoming_without_value = incoming_after_state.clone();
    if let Some(object) = incoming_without_value.as_object_mut() {
        object.remove("value");
    }
    existing_without_value == incoming_without_value
}

fn selector_text_record_state(state: &Value) -> bool {
    let Some(record_family) = state.get("record_family").and_then(Value::as_str) else {
        return false;
    };
    let Some(record_key) = state.get("record_key").and_then(Value::as_str) else {
        return false;
    };
    let Some(selector_key) = state.get("selector_key").and_then(Value::as_str) else {
        return false;
    };
    record_family == "text" && record_key.starts_with("text:") && !selector_key.is_empty()
}

pub(super) fn registry_only_authority_state_resource_repair_allowed(
    event_kind: &str,
    existing_before_state: &Value,
    incoming_before_state: &Value,
    existing_after_state: &Value,
    incoming_after_state: &Value,
) -> bool {
    match event_kind {
        "AuthorityEpochChanged" => {
            authority_state_unchanged_or_key_repaired(existing_before_state, incoming_before_state)
                && authority_epoch_after_state_unchanged_or_key_repaired(
                    existing_after_state,
                    incoming_after_state,
                )
                && (registry_only_authority_state(existing_before_state)
                    || registry_only_authority_state(existing_after_state))
                && (registry_only_authority_state(incoming_before_state)
                    || registry_only_authority_state(incoming_after_state))
        }
        "SurfaceBound" => {
            existing_before_state == incoming_before_state
                && authority_state_unchanged_or_key_repaired(
                    existing_after_state,
                    incoming_after_state,
                )
                && registry_only_authority_state(existing_after_state)
                && registry_only_authority_state(incoming_after_state)
        }
        "SurfaceUnbound" => {
            authority_state_unchanged_or_key_repaired(existing_before_state, incoming_before_state)
                && authority_state_unchanged_or_key_repaired(
                    existing_after_state,
                    incoming_after_state,
                )
                && registry_only_authority_state(existing_before_state)
                && registry_only_authority_state(incoming_before_state)
                && registry_only_authority_state(existing_after_state)
                && registry_only_authority_state(incoming_after_state)
        }
        _ => false,
    }
}

fn authority_state_unchanged_or_key_repaired(
    existing_state: &Value,
    incoming_state: &Value,
) -> bool {
    existing_state == incoming_state
        || (registry_only_authority_state(existing_state)
            && registry_only_authority_state(incoming_state)
            && authority_state_without_authority_key(existing_state)
                == authority_state_without_authority_key(incoming_state)
            && authority_key_state(existing_state).is_some()
            && authority_key_state(incoming_state).is_some())
}

fn authority_epoch_after_state_unchanged_or_key_repaired(
    existing_state: &Value,
    incoming_state: &Value,
) -> bool {
    if authority_state_unchanged_or_key_repaired(existing_state, incoming_state) {
        return true;
    }
    if !registry_only_authority_state(existing_state)
        || !registry_only_authority_state(incoming_state)
    {
        return false;
    }
    if authority_state_without_derivation_fields(existing_state)
        != authority_state_without_derivation_fields(incoming_state)
    {
        return false;
    }
    match (
        existing_state.get("registry_owner"),
        incoming_state.get("registry_owner"),
    ) {
        (None, Some(Value::String(owner))) => is_lower_hex_address(owner),
        (Some(existing_owner), Some(incoming_owner)) => existing_owner == incoming_owner,
        (None, None) => true,
        _ => false,
    }
}

fn authority_state_without_authority_key(value: &Value) -> Option<Value> {
    let mut object = value.as_object()?.clone();
    object.remove("authority_key");
    Some(Value::Object(object))
}

fn authority_state_without_derivation_fields(value: &Value) -> Option<Value> {
    let mut object = value.as_object()?.clone();
    object.remove("authority_key");
    object.remove("registry_owner");
    Some(Value::Object(object))
}

fn registry_only_authority_state(value: &Value) -> bool {
    value.get("authority_kind").and_then(Value::as_str) == Some("registry_only")
}

fn authority_key_state(value: &Value) -> Option<&str> {
    value
        .get("authority_key")
        .and_then(Value::as_str)
        .filter(|authority_key| !authority_key.trim().is_empty())
}

fn is_lower_hex_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value
            .as_bytes()
            .iter()
            .skip(2)
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

pub(super) fn permission_state_authority_repair_allowed(
    existing_state: &Value,
    incoming_state: &Value,
) -> bool {
    if existing_state == incoming_state {
        return true;
    }
    if permission_state_without_authority_sources(existing_state)
        != permission_state_without_authority_sources(incoming_state)
    {
        return false;
    }
    let grant_source_repair_allowed = authority_source_unchanged_or_repaired(
        existing_state.get("grant_source"),
        incoming_state.get("grant_source"),
    );
    let revocation_source_repair_allowed = authority_source_unchanged_or_repaired(
        existing_state.get("revocation_source"),
        incoming_state.get("revocation_source"),
    );
    grant_source_repair_allowed && revocation_source_repair_allowed
}

fn permission_state_without_authority_sources(state: &Value) -> Value {
    let mut value = state.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("grant_source");
        object.remove("revocation_source");
    }
    value
}

fn authority_source_unchanged_or_repaired(
    existing_source: Option<&Value>,
    incoming_source: Option<&Value>,
) -> bool {
    existing_source == incoming_source
        || authority_source_transition_allowed(existing_source, incoming_source)
}

fn authority_source_transition_allowed(
    existing_source: Option<&Value>,
    incoming_source: Option<&Value>,
) -> bool {
    let (Some(existing_source), Some(incoming_source)) = (existing_source, incoming_source) else {
        return false;
    };
    existing_source.get("kind").and_then(Value::as_str) == Some("ens_v1_authority")
        && incoming_source.get("kind").and_then(Value::as_str) == Some("ens_v1_authority")
        && existing_source
            .get("authority_kind")
            .and_then(Value::as_str)
            .is_some_and(|authority_kind| {
                matches!(authority_kind, "registrar" | "wrapper" | "registry_only")
            })
        && incoming_source
            .get("authority_kind")
            .and_then(Value::as_str)
            .is_some_and(|incoming_authority_kind| {
                let existing_authority_kind = existing_source
                    .get("authority_kind")
                    .and_then(Value::as_str);
                incoming_authority_kind == "registry_only"
                    || (existing_authority_kind == Some("registry_only")
                        && incoming_authority_kind == "registrar")
            })
        && existing_source
            .get("authority_key")
            .and_then(Value::as_str)
            .is_some_and(|authority_key| !authority_key.is_empty())
        && incoming_source
            .get("authority_key")
            .and_then(Value::as_str)
            .is_some_and(|authority_key| !authority_key.is_empty())
        && existing_source
            .get("source_event_kind")
            .and_then(Value::as_str)
            .is_some_and(|source_event_kind| !source_event_kind.is_empty())
        && existing_source
            .get("source_event_kind")
            .and_then(Value::as_str)
            == incoming_source
                .get("source_event_kind")
                .and_then(Value::as_str)
}
