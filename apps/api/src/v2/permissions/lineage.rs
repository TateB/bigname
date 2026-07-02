use bigname_storage::PermissionsCurrentRow;
use serde_json::{Map, Value, json};

use super::super::{permission_powers_value, slug_to_numeric};
use super::PermissionLineage;
use crate::v2::{V2Error, V2Result};

pub(super) fn permission_lineage(row: &PermissionsCurrentRow) -> V2Result<PermissionLineage> {
    Ok(PermissionLineage {
        grant: map_permission_lineage_value(&row.grant_source)?,
        revocation: row
            .revocation_source
            .as_ref()
            .map(map_permission_lineage_value)
            .transpose()?,
        inheritance_path: non_empty_array(&row.inheritance_path)
            .map(map_permission_lineage_value)
            .transpose()?,
        transfer_behavior: map_optional_permission_lineage_value(&row.transfer_behavior)?,
    })
}

fn map_permission_lineage_value(value: &Value) -> V2Result<Value> {
    match value {
        Value::Object(object) => map_permission_lineage_object(object),
        Value::Array(items) => Ok(Value::Array(
            items
                .iter()
                .map(map_permission_lineage_value)
                .collect::<V2Result<Vec<_>>>()?,
        )),
        _ => Ok(value.clone()),
    }
}

fn map_optional_permission_lineage_value(value: &Value) -> V2Result<Option<Value>> {
    if value.is_null() {
        return Ok(None);
    }

    let mapped = map_permission_lineage_value(value)?;
    if mapped_permission_lineage_value_is_empty(&mapped) {
        Ok(None)
    } else {
        Ok(Some(mapped))
    }
}

fn mapped_permission_lineage_value_is_empty(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.is_empty(),
        Value::Array(items) => {
            items.is_empty() || items.iter().all(mapped_permission_lineage_value_is_empty)
        }
        _ => false,
    }
}

fn map_permission_lineage_object(object: &Map<String, Value>) -> V2Result<Value> {
    let mut mapped = Map::new();
    if let Some(kind) = object.get("kind") {
        mapped.insert(
            "kind".to_owned(),
            Value::String(product_lineage_kind(kind)?),
        );
    }
    if let Some(registration_id) = object.get("resource_id") {
        mapped.insert(
            "registration_id".to_owned(),
            product_lineage_string_value(registration_id, "resource_id")?,
        );
    }
    if object.contains_key("resolver_address") {
        mapped.insert("resolver".to_owned(), product_lineage_resolver(object)?);
    }
    if let Some(powers) = object.get("powers") {
        mapped.insert("powers".to_owned(), permission_powers_value(powers)?);
    }
    Ok(Value::Object(mapped))
}

fn product_lineage_kind(value: &Value) -> V2Result<String> {
    let Some(kind) = value.as_str() else {
        return Err(lineage_mapping_error());
    };
    let mapped = match kind {
        "raw_log" | "normalized_event" => "event",
        "permission_row" => "permission",
        "resource_authority" => "registration_authority",
        "resource_rebound" => "registration_rebound",
        "ens_v1_authority" => "ens_v1_authority",
        "registry_root_fallback" => "registry_root_fallback",
        "resolver_root_fallback" => "resolver_root_fallback",
        _ => return Err(lineage_mapping_error()),
    };
    Ok(mapped.to_owned())
}

fn product_lineage_resolver(object: &Map<String, Value>) -> V2Result<Value> {
    let chain_id = object
        .get("chain_id")
        .and_then(Value::as_str)
        .and_then(slug_to_numeric)
        .ok_or_else(lineage_mapping_error)?;
    let address = object
        .get("resolver_address")
        .and_then(Value::as_str)
        .ok_or_else(lineage_mapping_error)?
        .to_ascii_lowercase();

    Ok(json!({
        "chain_id": chain_id,
        "address": address,
    }))
}

fn product_lineage_string_value(value: &Value, _field: &str) -> V2Result<Value> {
    value
        .as_str()
        .map(|value| Value::String(value.to_owned()))
        .ok_or_else(lineage_mapping_error)
}

fn lineage_mapping_error() -> V2Error {
    V2Error::internal_error("failed to map permission lineage")
}

fn non_empty_array(value: &Value) -> Option<&Value> {
    value
        .as_array()
        .filter(|values| !values.is_empty())
        .map(|_| value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::ErrorCode;

    #[test]
    fn lineage_mapping_rejects_unknown_storage_kind() {
        let error = map_permission_lineage_value(&json!({
            "kind": "contract_internal",
            "source_event": "EACRolesChanged"
        }))
        .expect_err("unknown lineage kinds must fail loudly");

        assert_eq!(error.code(), ErrorCode::InternalError);
    }
}
