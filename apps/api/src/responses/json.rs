fn dedupe_json_values(values: impl Iterator<Item = JsonValue>) -> JsonValue {
    let mut deduped = Vec::new();
    for value in values {
        if !deduped.contains(&value) {
            deduped.push(value);
        }
    }

    JsonValue::Array(deduped)
}

fn provenance_field<'a>(value: &'a JsonValue, key: &str) -> Option<&'a JsonValue> {
    value.as_object().and_then(|object| object.get(key))
}

fn string_field(value: Option<&JsonValue>) -> Option<String> {
    match value {
        Some(JsonValue::String(value)) => Some(value.clone()),
        Some(JsonValue::Number(value)) => Some(value.to_string()),
        Some(JsonValue::Bool(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn array_or_empty(value: Option<&JsonValue>) -> JsonValue {
    match value {
        Some(JsonValue::Array(values)) => JsonValue::Array(values.clone()),
        _ => JsonValue::Array(Vec::new()),
    }
}

fn array_value_strings(value: Option<&JsonValue>) -> JsonValue {
    match value {
        Some(JsonValue::Array(values)) => JsonValue::Array(
            values
                .iter()
                .filter_map(|value| value_to_string(value).map(JsonValue::String))
                .collect(),
        ),
        _ => JsonValue::Array(Vec::new()),
    }
}

fn value_to_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        JsonValue::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn ensure_allowed_json_fields(
    object: &JsonMap<String, JsonValue>,
    allowed_fields: &[&str],
    context: &str,
) -> Result<()> {
    for key in object.keys() {
        if !allowed_fields
            .iter()
            .any(|allowed| allowed == &key.as_str())
        {
            bail!("{context} must not set field {key}");
        }
    }

    Ok(())
}

fn required_json_string_field<'a>(
    object: &'a JsonMap<String, JsonValue>,
    field_name: &str,
    context: &str,
) -> Result<&'a str> {
    object
        .get(field_name)
        .and_then(JsonValue::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("{context} must include non-empty string field {field_name}"))
}

fn optional_nonempty_json_string_field(
    object: &JsonMap<String, JsonValue>,
    field_name: &str,
    context: &str,
) -> Result<Option<String>> {
    match object.get(field_name) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => bail!("{context} field {field_name} must be null or a non-empty string"),
    }
}

fn ensure_json_field_absent(
    object: &JsonMap<String, JsonValue>,
    field_name: &str,
    context: &str,
) -> Result<()> {
    if object.contains_key(field_name) {
        bail!("{context} must not set field {field_name}");
    }

    Ok(())
}

fn ensure_object(value: &JsonValue) -> JsonValue {
    value
        .as_object()
        .map(|_| value.clone())
        .unwrap_or_else(empty_object)
}

fn unsupported_section(unsupported_reason: &str) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "status", "unsupported".to_owned());
    insert_string_field(
        &mut value,
        "unsupported_reason",
        unsupported_reason.to_owned(),
    );
    value
}

fn compact_meta_object(
    support_status: &str,
    total_count: Option<u64>,
    unsupported_fields: impl IntoIterator<Item = String>,
    unsupported_filters: impl IntoIterator<Item = String>,
) -> JsonValue {
    let mut meta = empty_object();
    insert_string_field(&mut meta, "support_status", support_status.to_owned());
    insert_value_field(
        &mut meta,
        "unsupported_filters",
        JsonValue::Array(unsupported_filters.into_iter().map(JsonValue::String).collect()),
    );
    insert_value_field(
        &mut meta,
        "unsupported_fields",
        JsonValue::Array(unsupported_fields.into_iter().map(JsonValue::String).collect()),
    );
    insert_value_field(
        &mut meta,
        "total_count",
        total_count
            .map(serde_json::Number::from)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
    );
    meta
}

fn empty_object() -> JsonValue {
    JsonValue::Object(Default::default())
}

fn insert_string_field(object: &mut JsonValue, key: &str, value: String) {
    object
        .as_object_mut()
        .expect("object helper must receive object")
        .insert(key.to_owned(), JsonValue::String(value));
}

fn insert_optional_string_field(object: &mut JsonValue, key: &str, value: Option<String>) {
    object
        .as_object_mut()
        .expect("object helper must receive object")
        .insert(
            key.to_owned(),
            value.map(JsonValue::String).unwrap_or(JsonValue::Null),
        );
}

fn insert_nullable_string_field(object: &mut JsonValue, key: &str, value: Option<String>) {
    insert_optional_string_field(object, key, value);
}

fn insert_value_field(object: &mut JsonValue, key: &str, value: JsonValue) {
    object
        .as_object_mut()
        .expect("object helper must receive object")
        .insert(key.to_owned(), value);
}
