use super::{
    model::{CurrentBindingSeed, ProjectedRelations, RelevantEvent},
    util::{json_str, normalize_address},
};
use crate::permissions::{mask_effective_powers_for_fuse_state, scope_fuse_state_from_after_state};

pub(super) fn project_relations(
    binding: &CurrentBindingSeed,
    events: &[RelevantEvent],
) -> ProjectedRelations {
    let mut registrant = None;
    let mut token_holder = None;
    let mut registry_owner = None;
    let scope_modifier = latest_scope_modifier(events, binding.resource_id);

    for event in events {
        match event.event_kind.as_str() {
            "RegistrationGranted" => {
                registrant = json_str(&event.after_state, &["registrant"]).map(normalize_address);
            }
            "TokenControlTransferred" => {
                let transferred_to = json_str(&event.after_state, &["to"]).map(normalize_address);
                registrant = transferred_to.clone();
                token_holder = transferred_to;
            }
            "AuthorityTransferred" => {
                registry_owner = json_str(&event.after_state, &["owner"]).map(normalize_address);
            }
            "PermissionChanged" if event.resource_id == Some(binding.resource_id) => {
                if let Some(subject) = resource_control_subject(event, scope_modifier) {
                    registry_owner = Some(subject);
                } else if let Some(subject) =
                    resource_control_revocation_subject(event, scope_modifier)
                    && registry_owner.as_deref() == Some(subject.as_str())
                {
                    registry_owner = None;
                }
            }
            "AuthorityEpochChanged" | "TokenRegenerated" => {}
            _ => {}
        }
    }

    if binding.token_lineage_id.is_some() {
        let token_holder = token_holder.or_else(|| registrant.clone());
        let effective_controller = registry_owner
            .or_else(|| token_holder.clone())
            .or_else(|| registrant.clone());
        ProjectedRelations {
            registrant,
            token_holder,
            effective_controller,
        }
    } else {
        ProjectedRelations {
            registrant: None,
            token_holder: None,
            effective_controller: registry_owner,
        }
    }
}

fn latest_scope_modifier(
    events: &[RelevantEvent],
    resource_id: uuid::Uuid,
) -> Option<&RelevantEvent> {
    events.iter().rev().find(|event| {
        event.event_kind == "PermissionScopeChanged" && event.resource_id == Some(resource_id)
    })
}

fn resource_control_subject(
    event: &RelevantEvent,
    scope_modifier: Option<&RelevantEvent>,
) -> Option<String> {
    if json_str(&event.after_state, &["scope", "kind"]).as_deref() != Some("resource") {
        return None;
    }
    let powers = masked_effective_powers(event, scope_modifier)?;
    if !powers
        .iter()
        .any(|candidate| candidate == "resource_control")
    {
        return None;
    }
    json_str(&event.after_state, &["subject"]).map(normalize_address)
}

fn resource_control_revocation_subject(
    event: &RelevantEvent,
    scope_modifier: Option<&RelevantEvent>,
) -> Option<String> {
    if json_str(&event.after_state, &["scope", "kind"]).as_deref() != Some("resource") {
        return None;
    }
    let powers = masked_effective_powers(event, scope_modifier)?;
    if powers
        .iter()
        .any(|candidate| candidate == "resource_control")
    {
        return None;
    }
    json_str(&event.after_state, &["subject"]).map(normalize_address)
}

fn masked_effective_powers(
    event: &RelevantEvent,
    scope_modifier: Option<&RelevantEvent>,
) -> Option<Vec<String>> {
    let powers = event
        .after_state
        .get("effective_powers")
        .and_then(|value| value.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        })?;
    let masked = mask_effective_powers_for_fuse_state(
        powers,
        scope_fuse_state_from_after_state(scope_modifier.map(|modifier| &modifier.after_state)),
    );
    Some(masked.effective_powers)
}
