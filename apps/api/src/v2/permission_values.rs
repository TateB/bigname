use bigname_storage::PermissionScope;
use serde_json::{Value, json};

use super::{V2Error, V2Result, slug_to_numeric};

pub(crate) fn permission_scope_value(scope: &PermissionScope) -> V2Result<Value> {
    let detail = match scope {
        PermissionScope::Root | PermissionScope::Registry | PermissionScope::Resource => json!({}),
        PermissionScope::Resolver {
            chain_id,
            resolver_address,
        } => json!({
            "resolver": {
                "chain_id": permission_scope_chain_id(chain_id)?,
                "address": resolver_address.to_ascii_lowercase(),
            }
        }),
        PermissionScope::RecordManager {
            chain_id,
            manager_address,
        } => json!({
            "chain_id": permission_scope_chain_id(chain_id)?,
            "manager": manager_address.to_ascii_lowercase(),
        }),
        PermissionScope::MigrationDerived {
            predecessor_resource_id,
        } => json!({
            "predecessor_registration_id": predecessor_resource_id.to_string(),
        }),
        PermissionScope::TransportDerived { transport } => json!({
            "transport": transport,
        }),
    };
    let kind = match scope {
        PermissionScope::Resource => "registration",
        _ => scope.kind(),
    };

    Ok(json!({
        "kind": kind,
        "detail": detail,
    }))
}

pub(crate) fn permission_powers_value(powers: &Value) -> V2Result<Value> {
    let Some(items) = powers.as_array() else {
        return Err(V2Error::internal_error(
            "permission powers must be a JSON array",
        ));
    };

    items
        .iter()
        .map(|power| {
            let Some(power) = power.as_str() else {
                return Err(V2Error::internal_error(
                    "permission powers must be string values",
                ));
            };
            product_permission_power(power).map(Value::String)
        })
        .collect::<V2Result<Vec<_>>>()
        .map(Value::Array)
}

fn product_permission_power(power: &str) -> V2Result<String> {
    match power {
        "resource_control" => Ok("registration_control".to_owned()),
        _ if power == "resource" || power.contains("resource_") || power.contains("_resource") => {
            Err(V2Error::internal_error(
                "permission power uses unmapped storage vocabulary",
            ))
        }
        _ => Ok(power.to_owned()),
    }
}

fn permission_scope_chain_id(storage_chain_id: &str) -> V2Result<u64> {
    slug_to_numeric(storage_chain_id).ok_or_else(|| {
        V2Error::internal_error(format!(
            "permission scope uses unmapped chain_id {storage_chain_id}"
        ))
    })
}
