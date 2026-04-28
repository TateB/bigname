use super::*;

pub(super) fn resolver_changed_after_state(
    event: &ResolverObservation,
    claim_source: Option<&ReverseClaimSource>,
) -> Value {
    let mut state = Map::from_iter([
        ("resolver".to_owned(), Value::String(event.resolver.clone())),
        ("namehash".to_owned(), Value::String(event.namehash.clone())),
    ]);
    if let Some(claim_source) = claim_source {
        state.insert("primary_claim_source".to_owned(), claim_source.as_value());
    }
    Value::Object(state)
}

pub(super) fn record_changed_after_state(
    event: &RecordChangeObservation,
    claim_source: Option<&ReverseClaimSource>,
) -> Value {
    let mut state = Map::from_iter([
        (
            "record_key".to_owned(),
            Value::String(event.selector.record_key.clone()),
        ),
        (
            "record_family".to_owned(),
            Value::String(event.selector.record_family.clone()),
        ),
        (
            "selector_key".to_owned(),
            event
                .selector
                .selector_key
                .as_ref()
                .map(|value| Value::String(value.clone()))
                .unwrap_or(Value::Null),
        ),
    ]);
    if let Some(raw_name) = event.raw_name.as_ref() {
        state.insert("raw_name".to_owned(), Value::String(raw_name.clone()));
    }
    if let Some(value) = event.value.as_ref() {
        state.insert("value".to_owned(), value.clone());
    }
    if let Some(claim_source) = claim_source {
        state.insert("primary_claim_source".to_owned(), claim_source.as_value());
    }
    Value::Object(state)
}

pub(super) fn record_version_changed_after_state(
    event: &RecordVersionObservation,
    claim_source: Option<&ReverseClaimSource>,
) -> Value {
    let mut state = Map::from_iter([(
        "record_version".to_owned(),
        Value::Number(event.record_version.into()),
    )]);
    if let Some(claim_source) = claim_source {
        state.insert("primary_claim_source".to_owned(), claim_source.as_value());
    }
    Value::Object(state)
}
