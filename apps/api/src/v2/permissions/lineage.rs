use bigname_storage::PermissionsCurrentRow;
use serde_json::{Map, Value};

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
        transfer_behavior: non_null_value(&row.transfer_behavior)
            .map(map_permission_lineage_value)
            .transpose()?,
    })
}

fn map_permission_lineage_value(value: &Value) -> V2Result<Value> {
    match value {
        Value::Object(object) => {
            let mut mapped = Map::new();
            for (key, child) in object {
                if key == "manifest_version" {
                    continue;
                }

                let mapped_child = if key == "kind" {
                    map_permission_lineage_kind(child)?
                } else {
                    map_permission_lineage_value(child)?
                };
                mapped.insert(permission_lineage_key(key), mapped_child);
            }
            Ok(Value::Object(mapped))
        }
        Value::Array(items) => Ok(Value::Array(
            items
                .iter()
                .map(map_permission_lineage_value)
                .collect::<V2Result<Vec<_>>>()?,
        )),
        _ => Ok(value.clone()),
    }
}

fn map_permission_lineage_kind(value: &Value) -> V2Result<Value> {
    let Some(kind) = value.as_str() else {
        return Err(V2Error::internal_error("failed to map permission lineage"));
    };

    let mapped = match kind {
        "normalized_event" => "event".to_owned(),
        "permission_row" => "permission".to_owned(),
        "resource_authority" => "registration_authority".to_owned(),
        "resource_rebound" => "registration_rebound".to_owned(),
        _ if kind.starts_with("resource_") => kind.replacen("resource_", "registration_", 1),
        _ if permission_lineage_kind_contains_banned_storage_term(kind) => {
            return Err(V2Error::internal_error("failed to map permission lineage"));
        }
        _ => kind.to_owned(),
    };

    Ok(Value::String(mapped))
}

fn permission_lineage_kind_contains_banned_storage_term(kind: &str) -> bool {
    kind.contains("normalized_event")
        || kind.contains("permission_row")
        || kind.contains("manifest")
        || kind.contains("raw_fact")
}

fn permission_lineage_key(key: &str) -> String {
    if let Some(mapped) = replace_underscore_boundary_term(key, "resource_id", "registration_id") {
        return mapped;
    }
    if let Some(mapped) = replace_underscore_boundary_term(key, "normalized_event_id", "event_id") {
        return mapped;
    }
    if let Some(mapped) = replace_underscore_boundary_term(key, "permission_row", "permission") {
        return mapped;
    }
    if let Some(mapped) = replace_underscore_boundary_term(key, "normalized_event", "event") {
        return mapped;
    }
    if key == "subject" {
        return "address".to_owned();
    }
    if let Some(suffix) = key.strip_prefix("resource_") {
        return format!("registration_{suffix}");
    }
    key.to_owned()
}

fn replace_underscore_boundary_term(key: &str, term: &str, replacement: &str) -> Option<String> {
    key.match_indices(term).find_map(|(start, _)| {
        term_match_has_underscore_boundaries(key, term, start)
            .then(|| replace_key_term_at_boundary(key, term, replacement, start))
    })
}

fn replace_key_term_at_boundary(key: &str, term: &str, replacement: &str, start: usize) -> String {
    let end = start + term.len();
    let plural = key.as_bytes().get(end) == Some(&b's');
    let replacement_end = end + usize::from(plural);

    let mut mapped = String::new();
    mapped.push_str(&key[..start]);
    mapped.push_str(replacement);
    if plural {
        mapped.push('s');
    }
    mapped.push_str(&key[replacement_end..]);
    mapped
}

fn term_match_has_underscore_boundaries(key: &str, term: &str, start: usize) -> bool {
    let before_is_boundary = start == 0 || key.as_bytes()[start - 1] == b'_';
    if !before_is_boundary {
        return false;
    }

    let end = start + term.len();
    if end == key.len() || key.as_bytes()[end] == b'_' {
        return true;
    }

    key.as_bytes()[end] == b's' && (end + 1 == key.len() || key.as_bytes()[end + 1] == b'_')
}

fn non_empty_array(value: &Value) -> Option<&Value> {
    value
        .as_array()
        .filter(|values| !values.is_empty())
        .map(|_| value)
}

fn non_null_value(value: &Value) -> Option<&Value> {
    (!value.is_null()).then_some(value)
}
