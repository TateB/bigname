use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bigname_storage::PermissionsCurrentRow;
use serde_json::{Value, json};
use sqlx::{PgPool, types::time::OffsetDateTime};
use uuid::Uuid;

use super::canonicality::{build_canonicality_summary, build_chain_positions};
use super::json::{
    build_coverage, build_provenance, json_object_or_default, json_optional_object,
    json_string_array, json_text, parse_scope,
};
use super::load::load_permission_events;
use super::types::{PermissionKey, RelevantEvent};
use super::{EVENT_KIND_PERMISSION_CHANGED, EVENT_KIND_PERMISSION_SCOPE_CHANGED};

const CANNOT_UNWRAP: i64 = 1;
const CANNOT_BURN_FUSES: i64 = 2;
const CANNOT_TRANSFER: i64 = 4;
const CANNOT_SET_RESOLVER: i64 = 8;
const CANNOT_SET_TTL: i64 = 16;
const CANNOT_CREATE_SUBDOMAIN: i64 = 32;
const CANNOT_APPROVE: i64 = 64;
const PARENT_CANNOT_CONTROL: i64 = 1 << 16;
const RESOURCE_CONTROL_FUSE_MASK: i64 = CANNOT_UNWRAP
    | CANNOT_BURN_FUSES
    | CANNOT_TRANSFER
    | CANNOT_SET_RESOLVER
    | CANNOT_SET_TTL
    | CANNOT_CREATE_SUBDOMAIN
    | CANNOT_APPROVE;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FuseMaskState {
    NoModifier,
    Proven(i64),
    Unproven,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FuseMaskedPowers {
    pub(crate) effective_powers: Vec<String>,
    pub(crate) changed: bool,
}

pub(super) async fn build_rows(
    pool: &PgPool,
    resource_ids: &[Uuid],
) -> Result<Vec<PermissionsCurrentRow>> {
    let mut rows = Vec::new();

    for resource_id in resource_ids {
        let events = load_permission_events(pool, *resource_id).await?;
        rows.extend(project_rows(*resource_id, &events)?);
    }

    Ok(rows)
}

fn project_rows(resource_id: Uuid, events: &[RelevantEvent]) -> Result<Vec<PermissionsCurrentRow>> {
    let mut latest_by_key = BTreeMap::<PermissionKey, usize>::new();
    let mut history_by_key = BTreeMap::<PermissionKey, Vec<&RelevantEvent>>::new();
    let mut scope_modifiers = Vec::<&RelevantEvent>::new();

    for (index, event) in events.iter().enumerate() {
        if event.event_kind == EVENT_KIND_PERMISSION_SCOPE_CHANGED {
            scope_modifiers.push(event);
            continue;
        }
        if event.event_kind != EVENT_KIND_PERMISSION_CHANGED {
            continue;
        }
        let subject = json_text(&event.after_state, &["subject"])?;
        let scope = parse_scope(&event.after_state)?;
        let key = PermissionKey {
            subject,
            scope: scope.storage_key(),
        };
        latest_by_key.insert(key.clone(), index);
        history_by_key.entry(key).or_default().push(event);
    }

    let mut rows = Vec::new();
    for (key, latest_index) in latest_by_key {
        let latest = &events[latest_index];
        let base_effective_powers = json_string_array(&latest.after_state, &["effective_powers"])?;
        let modifier = latest_scope_modifier(&scope_modifiers);
        let (effective_powers, modifier_changed_row) =
            mask_effective_powers(base_effective_powers, modifier)?;
        if effective_powers.is_empty() {
            continue;
        }

        let base_history = history_by_key
            .get(&key)
            .context("missing permissions_current history for projected key")?;
        let mut history = base_history.clone();
        if modifier_changed_row && let Some(modifier) = modifier {
            history.push(modifier);
        }
        let scope = parse_scope(&latest.after_state)?;

        rows.push(PermissionsCurrentRow {
            resource_id,
            subject: key.subject,
            scope,
            effective_powers: Value::Array(
                effective_powers
                    .into_iter()
                    .map(Value::String)
                    .collect::<Vec<_>>(),
            ),
            grant_source: json_object_or_default(&latest.after_state, "grant_source"),
            revocation_source: json_optional_object(&latest.after_state, "revocation_source"),
            inheritance_path: latest
                .after_state
                .get("inheritance_path")
                .cloned()
                .unwrap_or_else(|| json!([])),
            transfer_behavior: json_object_or_default(&latest.after_state, "transfer_behavior"),
            provenance: build_provenance(&history)?,
            coverage: build_coverage(&history),
            chain_positions: build_chain_positions(&history),
            canonicality_summary: build_canonicality_summary(&history),
            manifest_version: history
                .iter()
                .map(|event| event.manifest_version)
                .max()
                .unwrap_or(1),
            last_recomputed_at: history
                .iter()
                .filter_map(|event| event.block_timestamp)
                .max()
                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
        });
    }

    Ok(rows)
}

fn latest_scope_modifier<'a>(scope_modifiers: &[&'a RelevantEvent]) -> Option<&'a RelevantEvent> {
    scope_modifiers
        .iter()
        .copied()
        .max_by_key(|modifier| event_sort_key(modifier))
}

fn event_sort_key(event: &RelevantEvent) -> (i64, i64, i64) {
    (
        event.block_number,
        event.log_index.unwrap_or(i64::MIN),
        event.normalized_event_id,
    )
}

fn mask_effective_powers(
    powers: Vec<String>,
    modifier: Option<&RelevantEvent>,
) -> Result<(Vec<String>, bool)> {
    let masked = mask_effective_powers_for_fuse_state(
        powers,
        scope_fuse_state_from_after_state(modifier.map(|modifier| &modifier.after_state)),
    );
    Ok((masked.effective_powers, masked.changed))
}

pub(crate) fn scope_fuse_state_from_after_state(after_state: Option<&Value>) -> FuseMaskState {
    let Some(after_state) = after_state else {
        return FuseMaskState::NoModifier;
    };
    after_state
        .get("fuses")
        .and_then(Value::as_i64)
        .filter(|fuses| *fuses >= 0)
        .map(FuseMaskState::Proven)
        .unwrap_or(FuseMaskState::Unproven)
}

pub(crate) fn mask_effective_powers_for_fuse_state(
    powers: Vec<String>,
    fuse_state: FuseMaskState,
) -> FuseMaskedPowers {
    let fuses = match fuse_state {
        FuseMaskState::NoModifier => {
            return FuseMaskedPowers {
                effective_powers: powers,
                changed: false,
            };
        }
        FuseMaskState::Proven(fuses) => fuses,
        FuseMaskState::Unproven => {
            return FuseMaskedPowers {
                effective_powers: Vec::new(),
                changed: true,
            };
        }
    };
    let mut changed = false;
    let effective_powers = powers
        .into_iter()
        .filter(|power| {
            let keep = !power_masked_by_fuses(power, fuses);
            changed |= !keep;
            keep
        })
        .collect::<Vec<_>>();
    FuseMaskedPowers {
        effective_powers,
        changed,
    }
}

fn power_masked_by_fuses(power: &str, fuses: i64) -> bool {
    match power {
        // `_wrapETH2LD` marks every wrapped .eth 2LD with
        // PARENT_CANNOT_CONTROL | IS_DOT_ETH, but PCC restricts the parent,
        // not the owner row's resource control.
        // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1013 @ ens_v1@91c966f)
        "resource_control" => fuses & RESOURCE_CONTROL_FUSE_MASK != 0,
        "resolver_control" => fuses & CANNOT_SET_RESOLVER != 0,
        // The adapter emits resource_control/resolver_control today. These
        // operation-specific names are forward vocabulary for future
        // normalized powers and remain fail-closed under the same fuse bits.
        "set_resolver" => fuses & CANNOT_SET_RESOLVER != 0,
        "set_ttl" => fuses & CANNOT_SET_TTL != 0,
        "create_subnames" | "create_subdomain" => {
            fuses & (CANNOT_CREATE_SUBDOMAIN | PARENT_CANNOT_CONTROL) != 0
        }
        "transfer" | "transfer_name" => fuses & CANNOT_TRANSFER != 0,
        "unwrap" => fuses & CANNOT_UNWRAP != 0,
        "burn_fuses" => fuses & CANNOT_BURN_FUSES != 0,
        "approve" | "approve_wrapper" => fuses & CANNOT_APPROVE != 0,
        _ => false,
    }
}
