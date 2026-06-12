use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::Result;
use bigname_storage::{NormalizedEvent, Resource, load_resource_including_noncanonical};
use serde_json::{Value, json};
use sqlx::{PgPool, types::Uuid};

use super::constants::{
    DERIVATION_KIND_ENS_V2_PERMISSIONS, EVENT_KIND_PERMISSION_CHANGED,
    EVENT_KIND_ROOT_PERMISSION_CHANGED, SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V2_ROOT_L1,
};
use super::types::{PermissionsRawLogRow, ResolverResourceHint};
use super::util::{decode_hex_32, deterministic_uuid, hex_string, resource_is_root};

pub(super) async fn remember_hint_and_resource(
    pool: &PgPool,
    raw_log: &PermissionsRawLogRow,
    hint: ResolverResourceHint,
    hints: &mut HashMap<(String, String), ResolverResourceHint>,
    resources: &mut BTreeMap<Uuid, (Resource, ResolverResourceHint)>,
) -> Result<()> {
    let key = (
        raw_log.emitting_address.clone(),
        hint.upstream_resource.clone(),
    );
    let resource = build_resource(pool, raw_log, &hint).await?;
    resources
        .entry(resource.resource_id)
        .or_insert((resource, hint.clone()));
    hints.insert(key, hint);
    Ok(())
}

pub(super) async fn build_resource(
    pool: &PgPool,
    raw_log: &PermissionsRawLogRow,
    hint: &ResolverResourceHint,
) -> Result<Resource> {
    let resource_id = permission_resource_id(
        &raw_log.chain_id,
        raw_log.emitting_contract_instance_id,
        &hint.upstream_resource,
        is_registry_permission_source(&raw_log.source_family),
    );
    if let Some(existing) = load_resource_including_noncanonical(pool, resource_id).await? {
        return Ok(Resource {
            resource_id: existing.resource_id,
            token_lineage_id: existing.token_lineage_id,
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: resource_provenance(raw_log, hint),
            canonicality_state: raw_log.canonicality_state,
        });
    }

    Ok(Resource {
        resource_id,
        token_lineage_id: None,
        chain_id: hint.first_ref.chain_id.clone(),
        block_hash: hint.first_ref.block_hash.clone(),
        block_number: hint.first_ref.block_number,
        provenance: resource_provenance(raw_log, hint),
        canonicality_state: raw_log.canonicality_state,
    })
}

pub(super) fn permission_changed_event(
    raw_log: &PermissionsRawLogRow,
    hint: &ResolverResourceHint,
    resource_id: Uuid,
    account: String,
    old_role_bitmap: String,
    new_role_bitmap: String,
) -> Result<NormalizedEvent> {
    let effective_powers = role_bitmap_powers(&new_role_bitmap)?;
    let old_powers = role_bitmap_powers(&old_role_bitmap)?;
    let has_effective_powers = !effective_powers.is_empty();
    let fully_revoked = !has_effective_powers && !old_powers.is_empty();
    let root_resource = resource_is_root(&hint.upstream_resource);
    let registry_permission_source = is_registry_permission_source(&raw_log.source_family);
    let event_kind = if registry_permission_source && root_resource {
        EVENT_KIND_ROOT_PERMISSION_CHANGED
    } else {
        EVENT_KIND_PERMISSION_CHANGED
    };
    let changed_powers = changed_role_powers(&old_role_bitmap, &new_role_bitmap)?;
    let source_contract_instance_key = if registry_permission_source {
        "registry_contract_instance_id"
    } else {
        "resolver_contract_instance_id"
    };
    let scope = permission_scope(raw_log, registry_permission_source, root_resource);
    let inheritance_path =
        permission_inheritance_path(raw_log, registry_permission_source, &hint.upstream_resource);
    let grant_source = if has_effective_powers {
        permission_source(
            raw_log,
            source_contract_instance_key,
            &hint.upstream_resource,
            root_resource,
            changed_powers.clone(),
        )
    } else {
        json!({})
    };
    let revocation_source = fully_revoked.then(|| {
        permission_source(
            raw_log,
            source_contract_instance_key,
            &hint.upstream_resource,
            root_resource,
            changed_powers.clone(),
        )
    });
    let mut after_state = json!({
        "subject": account.clone(),
        "scope": scope,
        "effective_powers": effective_powers,
        "grant_source": grant_source,
        "revocation_source": revocation_source,
        "inheritance_path": inheritance_path,
        "transfer_behavior": {},
        "source_event": "EACRolesChanged",
        "upstream_resource": hint.upstream_resource,
        "role_bitmap": new_role_bitmap.clone(),
        "old_role_bitmap": old_role_bitmap.clone(),
        "root_resource": root_resource,
        "selector": {
            "kind": hint.selector_kind,
            "key": hint.selector_key,
            "hash": hint.selector_hash,
            "normalized_name": hint.normalized_name,
            "dns_encoded_name": hint.dns_encoded_name.as_ref().map(|bytes| format!("0x{}", hex_string(bytes))),
        },
    });
    if let Some(object) = after_state.as_object_mut() {
        object.insert(
            source_contract_instance_key.to_owned(),
            Value::String(raw_log.emitting_contract_instance_id.to_string()),
        );
    }

    Ok(NormalizedEvent {
        event_identity: format!(
            "ens_v2_permissions:{}:{}:{}:{}:{}:{}",
            raw_log.source_manifest_id,
            raw_log.block_hash,
            raw_log.transaction_hash,
            raw_log.log_index,
            event_kind,
            hint.upstream_resource
        ),
        namespace: raw_log.namespace.clone(),
        logical_name_id: hint.logical_name_id.clone(),
        resource_id: Some(resource_id),
        event_kind: event_kind.to_owned(),
        source_family: raw_log.source_family.clone(),
        manifest_version: raw_log.manifest_version,
        source_manifest_id: Some(raw_log.source_manifest_id),
        chain_id: Some(raw_log.chain_id.clone()),
        block_number: Some(raw_log.block_number),
        block_hash: Some(raw_log.block_hash.clone()),
        transaction_hash: Some(raw_log.transaction_hash.clone()),
        log_index: Some(raw_log.log_index),
        raw_fact_ref: raw_fact_ref(raw_log),
        derivation_kind: DERIVATION_KIND_ENS_V2_PERMISSIONS.to_owned(),
        canonicality_state: raw_log.canonicality_state,
        before_state: json!({
            "subject": account,
            "role_bitmap": old_role_bitmap,
            "effective_powers": old_powers,
        }),
        after_state,
    })
}

fn resource_provenance(raw_log: &PermissionsRawLogRow, hint: &ResolverResourceHint) -> Value {
    let registry_permission_source = is_registry_permission_source(&raw_log.source_family);
    let source_contract_instance_key = if registry_permission_source {
        "registry_contract_instance_id"
    } else {
        "resolver_contract_instance_id"
    };
    let mut provenance = json!({
        "adapter": DERIVATION_KIND_ENS_V2_PERMISSIONS,
        "chain_id": raw_log.chain_id,
        "upstream_resource": hint.upstream_resource,
        "selector_kind": hint.selector_kind,
        "selector_key": hint.selector_key,
        "selector_hash": hint.selector_hash,
        "logical_name_id": hint.logical_name_id,
        "normalized_name": hint.normalized_name,
        "source_family": raw_log.source_family,
        "source_manifest_id": raw_log.source_manifest_id,
        "manifest_version": raw_log.manifest_version,
    });
    if let Some(object) = provenance.as_object_mut() {
        object.insert(
            source_contract_instance_key.to_owned(),
            Value::String(raw_log.emitting_contract_instance_id.to_string()),
        );
        object.insert(
            if registry_permission_source {
                "registry_address"
            } else {
                "resolver_address"
            }
            .to_owned(),
            Value::String(raw_log.emitting_address.clone()),
        );
    }
    provenance
}

fn raw_fact_ref(raw_log: &PermissionsRawLogRow) -> Value {
    json!({
        "kind": "raw_log",
        "chain_id": raw_log.chain_id,
        "block_hash": raw_log.block_hash,
        "block_number": raw_log.block_number,
        "transaction_hash": raw_log.transaction_hash,
        "transaction_index": raw_log.transaction_index,
        "log_index": raw_log.log_index,
        "emitting_address": raw_log.emitting_address,
    })
}

fn role_bitmap_powers(bitmap: &str) -> Result<Vec<String>> {
    let bytes = decode_hex_32(bitmap)?;
    let role_bits = [
        (0usize, "set_addr"),
        (4, "set_text"),
        (8, "set_contenthash"),
        (12, "set_pubkey"),
        (16, "set_abi"),
        (20, "set_interface"),
        (24, "set_name"),
        (28, "set_alias"),
        (32, "clear_records"),
        (124, "upgrade"),
        (128, "admin_set_addr"),
        (132, "admin_set_text"),
        (136, "admin_set_contenthash"),
        (140, "admin_set_pubkey"),
        (144, "admin_set_abi"),
        (148, "admin_set_interface"),
        (152, "admin_set_name"),
        (156, "admin_set_alias"),
        (160, "admin_clear_records"),
        (252, "admin_upgrade"),
    ];
    Ok(role_bits
        .into_iter()
        .filter(|(bit, _)| bit_is_set(&bytes, *bit))
        .map(|(_, power)| power.to_owned())
        .collect())
}

fn changed_role_powers(old_bitmap: &str, new_bitmap: &str) -> Result<Vec<String>> {
    let old = role_bitmap_powers(old_bitmap)?
        .into_iter()
        .collect::<HashSet<_>>();
    let new = role_bitmap_powers(new_bitmap)?
        .into_iter()
        .collect::<HashSet<_>>();
    let mut changed = old.symmetric_difference(&new).cloned().collect::<Vec<_>>();
    changed.sort();
    Ok(changed)
}

fn bit_is_set(bytes: &[u8; 32], bit: usize) -> bool {
    let byte_index = 31usize.saturating_sub(bit / 8);
    let bit_mask = 1u8 << (bit % 8);
    bytes[byte_index] & bit_mask != 0
}

fn permission_resource_id(
    chain_id: &str,
    contract_instance_id: Uuid,
    upstream_resource: &str,
    registry_permission_source: bool,
) -> Uuid {
    let prefix = if registry_permission_source {
        "ens-v2-resource"
    } else {
        "ens-v2-resolver-resource"
    };
    deterministic_uuid(&format!(
        "{prefix}:{chain_id}:{contract_instance_id}:{upstream_resource}"
    ))
}

fn is_registry_permission_source(source_family: &str) -> bool {
    matches!(
        source_family,
        SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1
    )
}

fn permission_scope(
    raw_log: &PermissionsRawLogRow,
    registry_permission_source: bool,
    root_resource: bool,
) -> Value {
    if registry_permission_source {
        json!({
            "kind": if root_resource { "registry_root" } else { "registry" },
            "chain_id": raw_log.chain_id,
            "registry_address": raw_log.emitting_address,
        })
    } else {
        json!({
            "kind": "resolver",
            "chain_id": raw_log.chain_id,
            "resolver_address": raw_log.emitting_address,
        })
    }
}

fn permission_inheritance_path(
    raw_log: &PermissionsRawLogRow,
    registry_permission_source: bool,
    upstream_resource: &str,
) -> Value {
    if !resource_is_root(upstream_resource) {
        return json!([]);
    }
    if registry_permission_source {
        json!([{
            "kind": "registry_root_fallback",
            "chain_id": raw_log.chain_id,
            "registry_address": raw_log.emitting_address,
            "upstream_resource": upstream_resource,
        }])
    } else {
        json!([{
            "kind": "resolver_root_fallback",
            "chain_id": raw_log.chain_id,
            "resolver_address": raw_log.emitting_address,
            "upstream_resource": upstream_resource,
        }])
    }
}

fn permission_source(
    raw_log: &PermissionsRawLogRow,
    source_contract_instance_key: &str,
    upstream_resource: &str,
    root_resource: bool,
    changed_powers: Vec<String>,
) -> Value {
    let mut source = json!({
        "kind": "raw_log",
        "source_event": "EACRolesChanged",
        "upstream_resource": upstream_resource,
        "root_resource": root_resource,
        "changed_powers": changed_powers,
    });
    if let Some(object) = source.as_object_mut() {
        object.insert(
            source_contract_instance_key.to_owned(),
            Value::String(raw_log.emitting_contract_instance_id.to_string()),
        );
    }
    source
}
