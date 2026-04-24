pub(super) fn supported_summary_field(section: Option<&JsonValue>, key: &str) -> JsonValue {
    if summary_is_unsupported(section) {
        return JsonValue::Null;
    }

    section
        .and_then(|value| provenance_field(value, key))
        .cloned()
        .unwrap_or(JsonValue::Null)
}

pub(super) fn summary_is_unsupported(section: Option<&JsonValue>) -> bool {
    matches!(
        string_field(section.and_then(|value| provenance_field(value, "status"))).as_deref(),
        Some("unsupported")
    ) && string_field(section.and_then(|value| provenance_field(value, "unsupported_reason")))
        .is_some()
}
