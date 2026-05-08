use std::collections::BTreeSet;

use anyhow::Result;
use bigname_storage::{PermissionsCurrentRow, ResolverCurrentRow, SurfaceBindingKind};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::types::time::OffsetDateTime;

use crate::projection_json::{
    dedupe_json_values, json_array_field, json_string_array_field, projection_coverage,
};

use super::{
    EVENT_KIND_ALIAS_CHANGED, EVENT_KIND_PERMISSION_CHANGED, EVENT_KIND_RESOLVER_CHANGED,
    RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON, RESOLVER_CURRENT_DERIVATION_KIND,
    RESOLVER_CURRENT_ENUMERATION_BASIS, RESOLVER_FAMILY_PENDING_REASON,
    RESOLVER_PROFILE_STATUS_SUPPORTED, SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
    profile::ResolverProfileGate,
    state_helpers::{build_canonicality_summary, build_chain_positions},
    target_loading::{
        AliasSeed, CurrentBindingSeed, ResolverTarget, load_alias_events, load_current_bindings,
        load_resolver_permissions,
    },
};

mod declared_summary;

use declared_summary::{
    build_binding_enumeration_not_projected_declared_summary, build_declared_summary,
    build_unsupported_declared_summary,
};

pub(super) async fn build_resolver_current_row(
    pool: &PgPool,
    profile_gate: &ResolverProfileGate,
    target: &ResolverTarget,
) -> Result<Option<ResolverCurrentRow>> {
    let skip_known_binding_enumeration = profile_gate.skips_binding_enumeration(target);
    let hinted_target_status = target
        .profile_source_family
        .as_deref()
        .map(|source_family| {
            profile_gate
                .target_status_for_source_family(target, source_family)
                .to_owned()
        });
    let skip_pending_binding_enumeration = hinted_target_status
        .as_deref()
        .is_some_and(|status| status != RESOLVER_PROFILE_STATUS_SUPPORTED);
    let skip_full_rebuild_binding_enumeration = !target.enumerate_bindings;
    let skip_binding_enumeration = skip_known_binding_enumeration
        || skip_pending_binding_enumeration
        || skip_full_rebuild_binding_enumeration;
    let skip_permission_enumeration = skip_binding_enumeration;
    let bindings = if skip_binding_enumeration {
        Vec::new()
    } else {
        load_current_bindings(pool, target).await?
    };
    let aliases = if skip_binding_enumeration {
        Vec::new()
    } else {
        load_alias_events(pool, target).await?
    };
    let permissions = if skip_permission_enumeration {
        Vec::new()
    } else {
        load_resolver_permissions(pool, target).await?
    };
    if bindings.is_empty()
        && aliases.is_empty()
        && permissions.is_empty()
        && !skip_binding_enumeration
    {
        return Ok(None);
    }

    let provenance = build_provenance(&bindings, &aliases, &permissions)?;
    let chain_positions = build_chain_positions(&bindings, &aliases, &permissions);
    let canonicality_summary = build_canonicality_summary(&bindings, &aliases, &permissions)?;
    let manifest_version = manifest_version(&bindings, &aliases, &permissions);
    let last_recomputed_at = last_recomputed_at(&bindings, &aliases, &permissions);
    let target_status = if skip_known_binding_enumeration {
        profile_gate.target_status_for_source_family(target, SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    } else if let Some(status) = hinted_target_status.as_deref() {
        status
    } else {
        profile_gate.target_status_for_bindings(target, &bindings)
    };
    let (declared_summary, coverage) = if target_status != RESOLVER_PROFILE_STATUS_SUPPORTED {
        (
            build_unsupported_declared_summary(RESOLVER_FAMILY_PENDING_REASON),
            build_unsupported_coverage(&bindings, &aliases, &permissions),
        )
    } else if skip_binding_enumeration {
        (
            build_binding_enumeration_not_projected_declared_summary(
                &permissions,
                skip_permission_enumeration,
            ),
            build_binding_enumeration_not_projected_coverage(
                &permissions,
                binding_enumeration_source_family(target, skip_known_binding_enumeration),
            ),
        )
    } else {
        (
            build_declared_summary(&bindings, &aliases, &permissions),
            build_coverage(&bindings, &aliases, &permissions),
        )
    };

    Ok(Some(ResolverCurrentRow {
        chain_id: target.chain_id.clone(),
        resolver_address: target.resolver_address.clone(),
        declared_summary,
        provenance,
        coverage,
        chain_positions,
        canonicality_summary,
        manifest_version,
        last_recomputed_at,
    }))
}

fn binding_enumeration_source_family(
    target: &ResolverTarget,
    skip_known_binding_enumeration: bool,
) -> Option<&str> {
    if skip_known_binding_enumeration {
        Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    } else {
        target.profile_source_family.as_deref()
    }
}

fn manifest_version(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> i64 {
    bindings
        .iter()
        .map(|binding| binding.manifest_version)
        .chain(aliases.iter().map(|alias| alias.manifest_version))
        .chain(
            permissions
                .iter()
                .map(|permission| permission.manifest_version),
        )
        .max()
        .unwrap_or(1)
}

fn last_recomputed_at(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> OffsetDateTime {
    bindings
        .iter()
        .filter_map(|binding| binding.block_timestamp)
        .chain(aliases.iter().filter_map(|alias| alias.block_timestamp))
        .chain(
            permissions
                .iter()
                .map(|permission| permission.last_recomputed_at),
        )
        .max()
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn build_provenance(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Result<Value> {
    let normalized_event_ids = bindings
        .iter()
        .map(|binding| Value::Number(binding.normalized_event_id.into()))
        .chain(
            aliases
                .iter()
                .map(|alias| Value::Number(alias.normalized_event_id.into())),
        )
        .chain(permissions.iter().flat_map(|permission| {
            json_array_field(&permission.provenance, "normalized_event_ids")
        }))
        .collect::<Vec<_>>();
    let raw_fact_refs = bindings
        .iter()
        .map(|binding| binding.raw_fact_ref.clone())
        .chain(aliases.iter().map(|alias| alias.raw_fact_ref.clone()))
        .chain(
            permissions
                .iter()
                .flat_map(|permission| json_array_field(&permission.provenance, "raw_fact_refs")),
        )
        .collect::<Vec<_>>();
    let manifest_versions =
        bindings
            .iter()
            .map(|binding| {
                json!({
                    "source_manifest_id": binding.source_manifest_id,
                    "source_family": binding.source_family,
                    "manifest_version": binding.manifest_version,
                })
            })
            .chain(aliases.iter().map(|alias| {
                json!({
                    "source_manifest_id": alias.source_manifest_id,
                    "source_family": alias.source_family,
                    "manifest_version": alias.manifest_version,
                })
            }))
            .chain(permissions.iter().flat_map(|permission| {
                json_array_field(&permission.provenance, "manifest_versions")
            }))
            .collect::<Vec<_>>();

    Ok(json!({
        "normalized_event_ids": dedupe_json_values(normalized_event_ids)?,
        "raw_fact_refs": dedupe_json_values(raw_fact_refs)?,
        "manifest_versions": dedupe_json_values(manifest_versions)?,
        "execution_trace_id": Value::Null,
        "derivation_kind": RESOLVER_CURRENT_DERIVATION_KIND,
    }))
}

fn build_coverage(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Value {
    let mut source_classes = bindings
        .iter()
        .map(|binding| binding.source_family.clone())
        .collect::<BTreeSet<_>>();

    source_classes.extend(aliases.iter().map(|alias| alias.source_family.clone()));

    for permission in permissions {
        for value in json_string_array_field(&permission.coverage, "source_classes_considered") {
            source_classes.insert(value);
        }
    }

    projection_coverage(
        "full",
        "authoritative",
        source_classes,
        None,
        RESOLVER_CURRENT_ENUMERATION_BASIS,
    )
}

fn build_unsupported_coverage(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Value {
    let mut coverage = build_coverage(bindings, aliases, permissions);
    coverage["status"] = json!("partial");
    coverage["exhaustiveness"] = json!("best_effort");
    coverage["unsupported_reason"] = json!(RESOLVER_FAMILY_PENDING_REASON);
    coverage
}

fn build_binding_enumeration_not_projected_coverage(
    permissions: &[PermissionsCurrentRow],
    source_family: Option<&str>,
) -> Value {
    let mut coverage = build_coverage(&[], &[], permissions);
    let mut source_classes = json_string_array_field(&coverage, "source_classes_considered")
        .into_iter()
        .collect::<BTreeSet<_>>();
    if let Some(source_family) = source_family {
        source_classes.insert(source_family.to_owned());
    }
    coverage["status"] = json!("partial");
    coverage["exhaustiveness"] = json!("non_enumerable");
    coverage["source_classes_considered"] = json!(source_classes.into_iter().collect::<Vec<_>>());
    coverage["unsupported_reason"] = json!(RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON);
    coverage
}
