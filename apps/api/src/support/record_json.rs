use super::*;

pub(crate) type JsonFieldAccessor = for<'a> fn(&'a JsonValue, &str) -> Option<&'a JsonValue>;

#[derive(Clone, Copy)]
pub(crate) struct RecordUnsupportedFieldNames {
    pub(crate) address_map: &'static str,
    pub(crate) primary_address: &'static str,
    pub(crate) text_records: &'static str,
    pub(crate) content_hash: Option<&'static str>,
}

pub(crate) const V2_RECORD_UNSUPPORTED_FIELD_NAMES: RecordUnsupportedFieldNames =
    RecordUnsupportedFieldNames {
        address_map: "addresses",
        primary_address: "primary_address",
        text_records: "text_records",
        content_hash: Some("content_hash"),
    };

pub(crate) const V1_IDENTITY_RECORD_UNSUPPORTED_FIELD_NAMES: RecordUnsupportedFieldNames =
    RecordUnsupportedFieldNames {
        address_map: "coin_type_addresses",
        primary_address: "primary_address",
        text_records: "text_records",
        content_hash: None,
    };

pub(crate) fn direct_json_field<'a>(value: &'a JsonValue, key: &str) -> Option<&'a JsonValue> {
    value.get(key)
}

pub(crate) fn record_addresses_from_entries(
    entries: Option<&JsonValue>,
    field: JsonFieldAccessor,
) -> BTreeMap<String, String> {
    successful_record_entries(entries, "addr", field)
        .filter_map(|entry| {
            let coin_type = string_field(field(entry, "selector_key")).or_else(|| {
                field(entry, "value")
                    .and_then(|value| field(value, "coin_type"))
                    .and_then(value_to_string)
            })?;
            let coin_type = bigname_storage::canonical_addr_coin_type(&coin_type)?;
            let value = record_value_string_from_entry(entry, field)?;
            Some((coin_type, value))
        })
        .collect()
}

pub(crate) fn record_text_records_from_entries(
    entries: Option<&JsonValue>,
    field: JsonFieldAccessor,
) -> BTreeMap<String, String> {
    let mut records = BTreeMap::new();
    for entry in successful_record_entries(entries, "text", field) {
        let Some(key) = string_field(field(entry, "selector_key")).or_else(|| {
            field(entry, "value")
                .and_then(|value| field(value, "key"))
                .and_then(value_to_string)
        }) else {
            continue;
        };
        if let Some(value) = record_value_string_from_entry(entry, field) {
            records.insert(key, value);
        }
    }
    for entry in successful_record_entries(entries, "avatar", field) {
        if let Some(value) = record_value_string_from_entry(entry, field) {
            records.insert("avatar".to_owned(), value);
        }
    }
    records
}

pub(crate) fn record_content_hash_from_entries(
    entries: Option<&JsonValue>,
    field: JsonFieldAccessor,
) -> Option<String> {
    successful_record_entries(entries, "contenthash", field)
        .find_map(|entry| record_value_string_from_entry(entry, field))
}

pub(crate) fn record_value_string_from_entry(
    entry: &JsonValue,
    field: JsonFieldAccessor,
) -> Option<String> {
    let value = field(entry, "value")?;
    field(value, "value")
        .and_then(value_to_string)
        .or_else(|| value_to_string(value))
}

pub(crate) fn record_unsupported_fields(
    inventory_present: bool,
    unsupported_families: Option<&JsonValue>,
    field: JsonFieldAccessor,
    names: RecordUnsupportedFieldNames,
) -> BTreeSet<String> {
    let mut fields = BTreeSet::new();
    if !inventory_present {
        fields.insert(names.address_map.to_owned());
        fields.insert(names.primary_address.to_owned());
        fields.insert(names.text_records.to_owned());
        if let Some(content_hash) = names.content_hash {
            fields.insert(content_hash.to_owned());
        }
        return fields;
    }

    for family in unsupported_families
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
        .filter_map(|family| string_field(field(family, "record_family")))
    {
        match family.as_str() {
            "addr" => {
                fields.insert(names.address_map.to_owned());
                fields.insert(names.primary_address.to_owned());
            }
            "text" | "avatar" => {
                fields.insert(names.text_records.to_owned());
            }
            "contenthash" => {
                if let Some(content_hash) = names.content_hash {
                    fields.insert(content_hash.to_owned());
                }
            }
            _ => {}
        }
    }
    fields
}

pub(crate) fn record_network_from_chain_positions(
    namespace: &str,
    chain_positions: &JsonValue,
    field: JsonFieldAccessor,
) -> String {
    match namespace {
        "basenames" if chain_positions_have_chain(chain_positions, "base-sepolia", field) => {
            "base-sepolia".to_owned()
        }
        "basenames" => "base".to_owned(),
        "ens" if chain_positions_have_chain(chain_positions, "ethereum-sepolia", field) => {
            "ethereum-sepolia".to_owned()
        }
        "ens" => "ethereum".to_owned(),
        namespace => namespace.to_owned(),
    }
}

pub(crate) fn chain_positions_have_chain(
    chain_positions: &JsonValue,
    chain_id: &str,
    field: JsonFieldAccessor,
) -> bool {
    chain_positions
        .as_object()
        .into_iter()
        .flatten()
        .any(|(slot, value)| {
            slot == chain_id || string_field(field(value, "chain_id")).as_deref() == Some(chain_id)
        })
}

pub(crate) fn record_json_string_at_paths(
    value: &JsonValue,
    paths: &[&[&str]],
    field: JsonFieldAccessor,
) -> Option<String> {
    paths
        .iter()
        .find_map(|path| record_json_path(value, path, field).and_then(value_to_string))
        .filter(|value| !value.trim().is_empty())
}

pub(crate) fn record_json_path<'a>(
    mut value: &'a JsonValue,
    path: &[&str],
    field: JsonFieldAccessor,
) -> Option<&'a JsonValue> {
    for key in path {
        value = field(value, key)?;
    }
    Some(value)
}

fn successful_record_entries<'a>(
    entries: Option<&'a JsonValue>,
    record_family: &'static str,
    field: JsonFieldAccessor,
) -> impl Iterator<Item = &'a JsonValue> {
    entries
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
        .filter(move |entry| {
            string_field(field(entry, "record_family")).as_deref() == Some(record_family)
                && string_field(field(entry, "status")).as_deref() == Some("success")
        })
}
