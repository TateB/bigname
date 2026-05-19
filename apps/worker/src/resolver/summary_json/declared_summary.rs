use std::collections::{BTreeMap, BTreeSet};

use crate::projection_json::unsupported_summary;

use super::*;

pub(super) fn build_declared_summary(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Value {
    json!({
        "bindings": build_binding_summary(bindings.iter()),
        "aliases": build_alias_summary(bindings, aliases),
        "permissions": build_permissions_summary(permissions),
        "role_holders": build_role_holders_summary(permissions),
        "event_summary": build_event_summary(bindings, aliases, permissions),
    })
}

pub(super) fn build_binding_enumeration_not_projected_declared_summary(
    permissions: &[PermissionsCurrentRow],
    skip_permission_enumeration: bool,
) -> Value {
    let permissions_summary = if skip_permission_enumeration {
        unsupported_summary(RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON)
    } else {
        build_permissions_summary(permissions)
    };
    let role_holders_summary = if skip_permission_enumeration {
        unsupported_summary(RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON)
    } else {
        build_role_holders_summary(permissions)
    };

    json!({
        "bindings": unsupported_summary(RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON),
        "aliases": unsupported_summary(RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON),
        "permissions": permissions_summary,
        "role_holders": role_holders_summary,
        "event_summary": unsupported_summary(RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON),
    })
}

fn build_permissions_summary(permissions: &[PermissionsCurrentRow]) -> Value {
    json!({
        "status": "supported",
        "count": permissions.len(),
        "items": permissions
            .iter()
            .map(|permission| {
                json!({
                    "resource_id": permission.resource_id,
                    "subject": permission.subject,
                    "effective_powers": permission.effective_powers,
                    "grant_source": permission.grant_source,
                    "revocation_source": permission.revocation_source,
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn build_binding_summary<'a>(bindings: impl Iterator<Item = &'a CurrentBindingSeed>) -> Value {
    let items = bindings.map(build_binding_item).collect::<Vec<_>>();
    json!({
        "status": "supported",
        "count": items.len(),
        "items": items,
    })
}

fn build_binding_item(binding: &CurrentBindingSeed) -> Value {
    json!({
        "logical_name_id": binding.logical_name_id,
        "canonical_display_name": binding.canonical_display_name,
        "normalized_name": binding.normalized_name,
        "namehash": binding.namehash,
        "resource_id": binding.resource_id,
        "surface_binding_id": binding.surface_binding_id,
        "binding_kind": binding.binding_kind.as_str(),
    })
}

fn build_alias_summary(bindings: &[CurrentBindingSeed], aliases: &[AliasSeed]) -> Value {
    let mut items = bindings
        .iter()
        .filter(|binding| binding.binding_kind == SurfaceBindingKind::ResolverAliasPath)
        .map(build_binding_item)
        .collect::<Vec<_>>();
    items.extend(aliases.iter().map(build_alias_item));
    items.sort_by(|left, right| {
        left.get("logical_name_id")
            .and_then(Value::as_str)
            .cmp(&right.get("logical_name_id").and_then(Value::as_str))
            .then(
                left.get("from_dns_encoded_name")
                    .and_then(Value::as_str)
                    .cmp(&right.get("from_dns_encoded_name").and_then(Value::as_str)),
            )
    });
    json!({
        "status": "supported",
        "count": items.len(),
        "items": items,
    })
}

fn build_alias_item(alias: &AliasSeed) -> Value {
    json!({
        "logical_name_id": alias.logical_name_id,
        "resource_id": alias.resource_id,
        "binding_kind": "resolver_alias_path",
        "alias_state": alias.after_state.get("alias_state").cloned().unwrap_or_else(|| json!("active")),
        "active": alias.after_state.get("active").cloned().unwrap_or(Value::Bool(true)),
        "chain_id": alias.chain_id,
        "resolver_address": alias.resolver_address,
        "from_dns_encoded_name": alias.after_state.get("from_dns_encoded_name").cloned().unwrap_or(Value::Null),
        "to_dns_encoded_name": alias.after_state.get("to_dns_encoded_name").cloned().unwrap_or(Value::Null),
        "from_name": alias.after_state.get("from_name").cloned().unwrap_or(Value::Null),
        "to_name": alias.after_state.get("to_name").cloned().unwrap_or(Value::Null),
        "to_logical_name_id": alias.after_state.get("to_logical_name_id").cloned().unwrap_or(Value::Null),
        "to_resource_id": alias.after_state.get("to_resource_id").cloned().unwrap_or(Value::Null),
        "latest_event_kind": EVENT_KIND_ALIAS_CHANGED,
    })
}

fn build_role_holders_summary(permissions: &[PermissionsCurrentRow]) -> Value {
    let mut holders = BTreeMap::<String, (BTreeSet<String>, BTreeSet<String>)>::new();

    for permission in permissions {
        let entry = holders
            .entry(permission.subject.clone())
            .or_insert_with(|| (BTreeSet::new(), BTreeSet::new()));
        entry.0.insert(permission.resource_id.to_string());
        for power in json_string_array(&permission.effective_powers) {
            entry.1.insert(power);
        }
    }

    json!({
        "status": "supported",
        "count": holders.len(),
        "items": holders
            .into_iter()
            .map(|(subject, (resource_ids, powers))| {
                json!({
                    "subject": subject,
                    "resource_count": resource_ids.len(),
                    "permission_row_count": resource_ids.len(),
                    "effective_powers": powers.into_iter().collect::<Vec<_>>(),
                    "resource_ids": resource_ids.into_iter().collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn build_event_summary(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Value {
    let resolver_changed_count = bindings.len();
    let alias_changed_count = aliases.len();
    let permission_changed_count = permissions
        .iter()
        .map(|permission| {
            permission
                .provenance
                .get("normalized_event_ids")
                .and_then(Value::as_array)
                .map(|ids| ids.len())
                .unwrap_or(0)
        })
        .sum::<usize>();
    let total_count = resolver_changed_count + alias_changed_count + permission_changed_count;
    let mut by_kind = serde_json::Map::new();
    if alias_changed_count > 0 {
        by_kind.insert(
            EVENT_KIND_ALIAS_CHANGED.to_owned(),
            Value::Number(alias_changed_count.into()),
        );
    }
    if permission_changed_count > 0 {
        by_kind.insert(
            EVENT_KIND_PERMISSION_CHANGED.to_owned(),
            Value::Number(permission_changed_count.into()),
        );
    }
    if resolver_changed_count > 0 {
        by_kind.insert(
            EVENT_KIND_RESOLVER_CHANGED.to_owned(),
            Value::Number(resolver_changed_count.into()),
        );
    }

    json!({
        "status": "supported",
        "count": total_count,
        "by_kind": by_kind,
    })
}

pub(super) fn build_unsupported_declared_summary(unsupported_reason: &str) -> Value {
    json!({
        "bindings": unsupported_summary(unsupported_reason),
        "aliases": unsupported_summary(unsupported_reason),
        "permissions": unsupported_summary(unsupported_reason),
        "role_holders": unsupported_summary(unsupported_reason),
        "event_summary": unsupported_summary(unsupported_reason),
    })
}

fn json_string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}
