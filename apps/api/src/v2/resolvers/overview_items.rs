use serde_json::{Map, Value, json};

use crate::v2::{V2Error, V2Result, history_event_type, permission_powers_value, slug_to_numeric};

pub(super) fn projected_section_items(summary: &Value, field_key: &str) -> V2Result<Option<Value>> {
    if !summary_is_supported(summary) {
        return Ok(None);
    }

    if field_key == "events" {
        return compact_resolver_event_summary(summary).map(Some);
    }

    let Some(items) = summary.get("items").and_then(Value::as_array) else {
        return Ok(None);
    };

    let items = match field_key {
        "nodes" | "aliases" => Value::Array(
            items
                .iter()
                .map(compact_resolver_binding_item)
                .collect::<V2Result<Vec<_>>>()?,
        ),
        "roles" => Value::Array(
            items
                .iter()
                .map(compact_resolver_role_item)
                .collect::<V2Result<Vec<_>>>()?,
        ),
        _ => Value::Array(items.clone()),
    };
    Ok(Some(items))
}

fn compact_resolver_event_summary(summary: &Value) -> V2Result<Value> {
    let Some(count) = summary.get("count") else {
        return Err(resolver_event_summary_mapping_error());
    };
    if count.as_u64().is_none() {
        return Err(resolver_event_summary_mapping_error());
    }

    let Some(by_kind) = summary.get("by_kind").and_then(Value::as_object) else {
        return Err(resolver_event_summary_mapping_error());
    };
    let mut by_type = Map::new();
    for (event_kind, count) in by_kind {
        let Some(event_type) = history_event_type(event_kind) else {
            continue;
        };
        let count = count
            .as_u64()
            .ok_or_else(resolver_event_summary_mapping_error)?;
        let key = event_type.as_str();
        let previous = by_type.get(key).and_then(Value::as_u64).unwrap_or(0);
        let mapped_count = previous
            .checked_add(count)
            .ok_or_else(resolver_event_summary_mapping_error)?;
        by_type.insert(key.to_owned(), json!(mapped_count));
    }

    let mut compact = Map::new();
    compact.insert("count".to_owned(), count.clone());
    compact.insert("by_type".to_owned(), Value::Object(by_type));
    Ok(Value::Object(compact))
}

fn resolver_event_summary_mapping_error() -> V2Error {
    V2Error::internal_error("failed to map resolver event summary")
}

fn compact_resolver_binding_item(item: &Value) -> V2Result<Value> {
    if resolver_alias_item_has_writer_shape(item) {
        return compact_resolver_alias_item(item);
    }

    let mut compact = Map::new();
    if let Some(logical_name_id) = item.get("logical_name_id").and_then(Value::as_str)
        && let Some((namespace, _)) = logical_name_id.split_once(':')
    {
        insert_optional_string(&mut compact, "namespace", Some(namespace.to_owned()));
    }
    insert_optional_string(&mut compact, "name", item_string(item, "normalized_name"));
    insert_optional_string(
        &mut compact,
        "display_name",
        item_string(item, "canonical_display_name"),
    );
    insert_optional_string(&mut compact, "namehash", item_string(item, "namehash"));
    Ok(Value::Object(compact))
}

fn resolver_alias_item_has_writer_shape(item: &Value) -> bool {
    item.as_object().is_some_and(|object| {
        [
            "alias_state",
            "from_name",
            "to_name",
            "to_logical_name_id",
            "to_resource_id",
        ]
        .iter()
        .any(|key| object.contains_key(*key))
    })
}

fn compact_resolver_alias_item(item: &Value) -> V2Result<Value> {
    let Some(object) = item.as_object() else {
        return Err(resolver_alias_mapping_error());
    };
    reject_unknown_resolver_alias_keys(object)?;

    let logical_name = resolver_alias_logical_name_parts(object, "logical_name_id")?;
    let to_logical_name = resolver_alias_logical_name_parts(object, "to_logical_name_id")?;
    let mut compact = Map::new();

    if let Some((namespace, _)) = to_logical_name.as_ref().or(logical_name.as_ref()) {
        compact.insert("namespace".to_owned(), Value::String(namespace.clone()));
    }
    insert_resolver_alias_name(&mut compact, object, "from_name", "from_name", logical_name)?;
    insert_resolver_alias_name(&mut compact, object, "to_name", "to_name", to_logical_name)?;
    insert_resolver_alias_string_from_keys(
        &mut compact,
        object,
        "from_display_name",
        &["from_display_name", "from_canonical_display_name"],
    )?;
    insert_resolver_alias_string_from_keys(
        &mut compact,
        object,
        "to_display_name",
        &["to_display_name", "to_canonical_display_name"],
    )?;
    insert_resolver_alias_string_from_keys(&mut compact, object, "state", &["alias_state"])?;
    if let Some(to_registration_id) = resolver_alias_string(object, "to_resource_id")? {
        compact.insert(
            "to_registration_id".to_owned(),
            Value::String(to_registration_id),
        );
    }
    if object.contains_key("resolver_address") || object.contains_key("chain_id") {
        compact.insert("resolver".to_owned(), resolver_alias_resolver(object)?);
    }

    Ok(Value::Object(compact))
}

fn resolver_alias_logical_name_parts(
    object: &Map<String, Value>,
    key: &str,
) -> V2Result<Option<(String, String)>> {
    let Some(value) = resolver_alias_string(object, key)? else {
        return Ok(None);
    };
    let Some((namespace, name)) = value.split_once(':') else {
        return Err(resolver_alias_mapping_error());
    };
    if namespace.is_empty() || name.is_empty() {
        return Err(resolver_alias_mapping_error());
    }
    Ok(Some((namespace.to_owned(), name.to_owned())))
}

fn insert_resolver_alias_string_from_keys(
    compact: &mut Map<String, Value>,
    object: &Map<String, Value>,
    output_key: &str,
    input_keys: &[&str],
) -> V2Result<()> {
    for input_key in input_keys {
        if let Some(value) = resolver_alias_string(object, input_key)? {
            compact.insert(output_key.to_owned(), Value::String(value));
            break;
        }
    }
    Ok(())
}

fn insert_resolver_alias_name(
    compact: &mut Map<String, Value>,
    object: &Map<String, Value>,
    output_key: &str,
    input_key: &str,
    fallback_logical_name: Option<(String, String)>,
) -> V2Result<()> {
    let value = resolver_alias_string(object, input_key)?
        .or_else(|| fallback_logical_name.map(|(_, name)| name));
    compact.insert(
        output_key.to_owned(),
        value.map(Value::String).unwrap_or(Value::Null),
    );
    Ok(())
}

fn resolver_alias_resolver(object: &Map<String, Value>) -> V2Result<Value> {
    let Some(chain_id) = resolver_alias_string(object, "chain_id")? else {
        return Err(resolver_alias_mapping_error());
    };
    let Some(chain_id) = slug_to_numeric(&chain_id) else {
        return Err(resolver_alias_mapping_error());
    };
    let Some(address) = resolver_alias_string(object, "resolver_address")? else {
        return Err(resolver_alias_mapping_error());
    };

    Ok(json!({
        "chain_id": chain_id,
        "address": address.to_ascii_lowercase(),
    }))
}

fn resolver_alias_string(object: &Map<String, Value>, key: &str) -> V2Result<Option<String>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.to_owned())),
        _ => Err(resolver_alias_mapping_error()),
    }
}

fn reject_unknown_resolver_alias_keys(object: &Map<String, Value>) -> V2Result<()> {
    for key in object.keys() {
        if !RESOLVER_ALIAS_ITEM_KEYS.contains(&key.as_str())
            && resolver_alias_unknown_key_has_banned_vocabulary(key)
        {
            return Err(resolver_alias_mapping_error());
        }
    }
    Ok(())
}

const RESOLVER_ALIAS_ITEM_KEYS: &[&str] = &[
    "logical_name_id",
    "resource_id",
    "binding_kind",
    "alias_state",
    "active",
    "chain_id",
    "resolver_address",
    "from_dns_encoded_name",
    "to_dns_encoded_name",
    "from_name",
    "to_name",
    "from_display_name",
    "to_display_name",
    "from_canonical_display_name",
    "to_canonical_display_name",
    "to_logical_name_id",
    "to_resource_id",
    "latest_event_kind",
];

fn resolver_alias_unknown_key_has_banned_vocabulary(key: &str) -> bool {
    const BANNED_TERMS: &[&str] = &[
        "logical_name",
        "normalized",
        "canonical_display_name",
        "resource",
        "resolver_address",
        "surface_binding",
        "permission_row",
        "effective_power",
        "manifest",
        "raw_fact",
        "raw_log",
        "coverage",
        "provenance",
    ];

    BANNED_TERMS.iter().any(|term| key.contains(term))
}

fn resolver_alias_mapping_error() -> V2Error {
    V2Error::internal_error("failed to map resolver alias item")
}

fn compact_resolver_role_item(item: &Value) -> V2Result<Value> {
    let Some(object) = item.as_object() else {
        return Ok(item.clone());
    };

    let mut compact = object.clone();
    if let Some(value) = compact.remove("subject") {
        compact.insert("address".to_owned(), value);
    }
    if let Some(value) = compact.remove("resource_count") {
        compact.insert("registration_count".to_owned(), value);
    }
    if let Some(value) = compact.remove("permission_row_count") {
        compact.insert("permission_count".to_owned(), value);
    }
    if let Some(value) = compact.remove("effective_powers") {
        compact.insert("powers".to_owned(), permission_powers_value(&value)?);
    }
    if let Some(value) = compact.remove("resource_ids") {
        compact.insert("registration_ids".to_owned(), value);
    }
    Ok(Value::Object(compact))
}

fn item_string(item: &Value, key: &str) -> Option<String> {
    item.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn insert_optional_string(object: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        object.insert(key.to_owned(), Value::String(value));
    }
}

pub(super) fn summary_is_supported(summary: &Value) -> bool {
    summary.get("status").and_then(Value::as_str) == Some("supported")
}
