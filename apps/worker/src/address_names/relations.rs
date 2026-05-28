use super::{
    model::{CurrentBindingSeed, ProjectedRelations, RelevantEvent},
    util::{json_str, normalize_address},
};

pub(super) fn project_relations(
    binding: &CurrentBindingSeed,
    events: &[RelevantEvent],
) -> ProjectedRelations {
    let mut registrant = None;
    let mut token_holder = None;
    let mut registry_owner = None;

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
                if let Some(subject) = resource_control_subject(event) {
                    registry_owner = Some(subject);
                } else if let Some(subject) = resource_control_revocation_subject(event)
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

fn resource_control_subject(event: &RelevantEvent) -> Option<String> {
    if json_str(&event.after_state, &["scope", "kind"]).as_deref() != Some("resource") {
        return None;
    }
    if !has_effective_power(&event.after_state, "resource_control") {
        return None;
    }
    json_str(&event.after_state, &["subject"]).map(normalize_address)
}

fn resource_control_revocation_subject(event: &RelevantEvent) -> Option<String> {
    if json_str(&event.after_state, &["scope", "kind"]).as_deref() != Some("resource") {
        return None;
    }
    let powers = event
        .after_state
        .get("effective_powers")
        .and_then(|value| value.as_array())?;
    if powers
        .iter()
        .any(|value| value.as_str() == Some("resource_control"))
    {
        return None;
    }
    json_str(&event.after_state, &["subject"]).map(normalize_address)
}

fn has_effective_power(state: &serde_json::Value, power: &str) -> bool {
    state
        .get("effective_powers")
        .and_then(|value| value.as_array())
        .is_some_and(|values| values.iter().any(|value| value.as_str() == Some(power)))
}
