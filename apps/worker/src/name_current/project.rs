use std::collections::BTreeMap;

use anyhow::{Result, bail};
use bigname_storage::CanonicalityState;
use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;

use super::EVENT_KIND_RESOLVER_CHANGED;
use super::json::{
    format_timestamp, history_pointer_from_event, json_i64, json_str, normalize_resolver_address,
};
use super::types::{
    ChainPositionCandidate, CurrentBindingContext, HistoryHeads, NameSurfaceSeed, ProjectedFacts,
    RelevantEvent, SupplementalChainObservation,
};

pub(super) fn latest_chain_position_for_chain(
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    chain_id: &str,
) -> Option<ChainPositionCandidate> {
    let mut latest_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    if name.chain_id == chain_id
        && let Some(timestamp) = name.block_timestamp
    {
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(&name.chain_id),
                chain_id: name.chain_id.clone(),
                block_number: name.block_number,
                block_hash: name.block_hash.clone(),
                timestamp,
            },
        );
    }

    if let Some(binding) = current_binding
        && binding.chain_id == chain_id
        && let Some(timestamp) = binding.block_timestamp
    {
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(&binding.chain_id),
                chain_id: binding.chain_id.clone(),
                block_number: binding.block_number,
                block_hash: binding.block_hash.clone(),
                timestamp,
            },
        );
    }

    for event in events {
        let (Some(event_chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
            event.chain_id.as_ref(),
            event.block_number,
            event.block_hash.as_ref(),
            event.block_timestamp,
        ) else {
            continue;
        };
        if event_chain_id != chain_id {
            continue;
        }
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(event_chain_id),
                chain_id: event_chain_id.clone(),
                block_number,
                block_hash: block_hash.clone(),
                timestamp,
            },
        );
    }

    for event in history_heads.iter() {
        let (Some(event_chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
            event.chain_id.as_ref(),
            event.block_number,
            event.block_hash.as_ref(),
            event.block_timestamp,
        ) else {
            continue;
        };
        if event_chain_id != chain_id {
            continue;
        }
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(event_chain_id),
                chain_id: event_chain_id.clone(),
                block_number,
                block_hash: block_hash.clone(),
                timestamp,
            },
        );
    }

    latest_positions.into_values().next()
}

pub(super) fn build_chain_positions(
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    supplemental_chain_observations: &[SupplementalChainObservation],
) -> Value {
    let mut latest_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    if let Some(timestamp) = name.block_timestamp {
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(&name.chain_id),
                chain_id: name.chain_id.clone(),
                block_number: name.block_number,
                block_hash: name.block_hash.clone(),
                timestamp,
            },
        );
    }

    if let Some(binding) = current_binding
        && let Some(timestamp) = binding.block_timestamp
    {
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(&binding.chain_id),
                chain_id: binding.chain_id.clone(),
                block_number: binding.block_number,
                block_hash: binding.block_hash.clone(),
                timestamp,
            },
        );
    }

    for event in events {
        let (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
            event.chain_id.as_ref(),
            event.block_number,
            event.block_hash.as_ref(),
            event.block_timestamp,
        ) else {
            continue;
        };
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(chain_id),
                chain_id: chain_id.clone(),
                block_number,
                block_hash: block_hash.clone(),
                timestamp,
            },
        );
    }

    for event in history_heads.iter() {
        let (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
            event.chain_id.as_ref(),
            event.block_number,
            event.block_hash.as_ref(),
            event.block_timestamp,
        ) else {
            continue;
        };
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(chain_id),
                chain_id: chain_id.clone(),
                block_number,
                block_hash: block_hash.clone(),
                timestamp,
            },
        );
    }

    for observation in supplemental_chain_observations {
        push_chain_position(&mut latest_positions, observation.candidate.clone());
    }

    Value::Object(
        latest_positions
            .into_iter()
            .map(|(slot, position)| {
                (
                    slot,
                    json!({
                        "chain_id": position.chain_id,
                        "block_number": position.block_number,
                        "block_hash": position.block_hash,
                        "timestamp": format_timestamp(position.timestamp),
                    }),
                )
            })
            .collect(),
    )
}

fn push_chain_position(
    latest_positions: &mut BTreeMap<String, ChainPositionCandidate>,
    candidate: ChainPositionCandidate,
) {
    let replace = latest_positions
        .get(&candidate.slot)
        .map(|current| {
            candidate.block_number > current.block_number
                || (candidate.block_number == current.block_number
                    && candidate.block_hash > current.block_hash)
        })
        .unwrap_or(true);
    if replace {
        latest_positions.insert(candidate.slot.clone(), candidate);
    }
}

pub(super) fn build_canonicality_summary(
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    supplemental_chain_observations: &[SupplementalChainObservation],
) -> Value {
    let mut states = vec![name.canonicality_state];
    let mut chain_states = BTreeMap::<String, CanonicalityState>::new();
    merge_chain_state(&mut chain_states, &name.chain_id, name.canonicality_state);

    if let Some(binding) = current_binding {
        states.push(binding.surface_binding_state);
        states.push(binding.resource_state);
        merge_chain_state(
            &mut chain_states,
            &binding.chain_id,
            binding.surface_binding_state,
        );
        merge_chain_state(&mut chain_states, &binding.chain_id, binding.resource_state);
        if let Some(token_lineage_state) = binding.token_lineage_state {
            states.push(token_lineage_state);
            merge_chain_state(&mut chain_states, &binding.chain_id, token_lineage_state);
        }
    }

    for event in events {
        states.push(event.canonicality_state);
        if let Some(chain_id) = event.chain_id.as_ref() {
            merge_chain_state(&mut chain_states, chain_id, event.canonicality_state);
        }
    }

    for event in history_heads.iter() {
        states.push(event.canonicality_state);
        if let Some(chain_id) = event.chain_id.as_ref() {
            merge_chain_state(&mut chain_states, chain_id, event.canonicality_state);
        }
    }

    for observation in supplemental_chain_observations {
        states.push(observation.canonicality_state);
        merge_chain_state(
            &mut chain_states,
            &observation.candidate.chain_id,
            observation.canonicality_state,
        );
    }

    let status =
        CanonicalityState::weakest(states.iter().copied()).unwrap_or(CanonicalityState::Canonical);
    json!({
        "status": status.as_str(),
        "chains": chain_states
            .into_iter()
            .map(|(chain_id, state)| (chain_id, Value::String(state.as_str().to_owned())))
            .collect::<serde_json::Map<String, Value>>(),
    })
}

fn merge_chain_state(
    chain_states: &mut BTreeMap<String, CanonicalityState>,
    chain_id: &str,
    state: CanonicalityState,
) {
    let replacement = chain_states
        .get(chain_id)
        .map(|current| state.rank() < current.rank())
        .unwrap_or(true);
    if replacement {
        chain_states.insert(chain_id.to_owned(), state);
    }
}

pub(super) fn project_facts(
    events: &[RelevantEvent],
    current_binding: Option<&CurrentBindingContext>,
    history_heads: &HistoryHeads,
) -> Result<ProjectedFacts> {
    let mut facts = ProjectedFacts::default();
    let mut latest_explicit_resolver_observation_block: Option<i64> = None;

    for event in events {
        if let Some(status) = json_str(&event.after_state, &["status"]) {
            facts.control_status_substrate = Some(status);
        }
        if let Some(expiry) = json_i64(&event.after_state, &["expiry"]) {
            facts.control_expiry_substrate = Some(expiry);
        }

        match event.event_kind.as_str() {
            "RegistrationGranted" => {
                facts.registration_status = Some("active".to_owned());
                facts.authority_kind = json_str(&event.after_state, &["authority_kind"]);
                facts.authority_key = json_str(&event.after_state, &["authority_key"]);
                facts.registrant = json_str(&event.after_state, &["registrant"]);
                facts.expiry = json_i64(&event.after_state, &["expiry"]);
                facts.latest_registration_event_kind = Some(event.event_kind.clone());
            }
            "RegistrationRenewed" => {
                if facts.registration_status.as_deref() != Some("released") {
                    facts.registration_status = Some("active".to_owned());
                }
                facts.expiry = json_i64(&event.after_state, &["expiry"]).or(facts.expiry);
                facts.latest_registration_event_kind = Some(event.event_kind.clone());
            }
            "ExpiryChanged" => {
                facts.expiry = json_i64(&event.after_state, &["expiry"]).or(facts.expiry);
                facts.latest_registration_event_kind = Some(event.event_kind.clone());
            }
            "RegistrationReleased" => {
                facts.registration_status = Some("released".to_owned());
                facts.released_at = json_i64(&event.after_state, &["released_at"]);
                facts.latest_registration_event_kind = Some(event.event_kind.clone());
            }
            "TokenControlTransferred" => {
                facts.registrant = json_str(&event.after_state, &["to"]);
                facts.latest_control_event_kind = Some(event.event_kind.clone());
            }
            "AuthorityTransferred" => {
                facts.registry_owner = json_str(&event.after_state, &["owner"]);
                facts.latest_control_event_kind = Some(event.event_kind.clone());
            }
            "AuthorityEpochChanged" => {
                facts.authority_kind = json_str(&event.after_state, &["authority_kind"]);
                facts.authority_key = json_str(&event.after_state, &["authority_key"]);
                facts.latest_control_event_kind = Some(event.event_kind.clone());
            }
            EVENT_KIND_RESOLVER_CHANGED
                if current_binding.map(|binding| binding.resource_id) == event.resource_id =>
            {
                let is_boundary = is_authority_epoch_resolver_boundary(event);
                if is_boundary
                    && event.block_number.is_some()
                    && event.block_number == latest_explicit_resolver_observation_block
                {
                    continue;
                }
                let resolver_address = normalize_resolver_address(
                    json_str(&event.after_state, &["resolver"]).as_deref(),
                );
                if resolver_address.is_some() && event.chain_id.is_none() {
                    bail!(
                        "ResolverChanged event {} for logical_name_id {} is missing chain_id",
                        event.normalized_event_id,
                        current_binding
                            .map(|binding| binding.surface_binding_id.to_string())
                            .unwrap_or_default()
                    );
                }
                facts.resolver_chain_id = resolver_address
                    .as_ref()
                    .and_then(|_| event.chain_id.clone());
                facts.resolver_address = resolver_address;
                facts.latest_resolver_event_kind = Some(event.event_kind.clone());
                if !is_boundary {
                    latest_explicit_resolver_observation_block = event.block_number;
                }
            }
            _ => {}
        }
    }

    if current_binding.is_some() && facts.registration_status.is_none() {
        facts.registration_status = Some("active".to_owned());
    }

    facts.surface_head = history_heads
        .surface_head
        .as_ref()
        .map(history_pointer_from_event);
    facts.resource_head = history_heads
        .resource_head
        .as_ref()
        .map(history_pointer_from_event);

    Ok(facts)
}

fn is_authority_epoch_resolver_boundary(event: &RelevantEvent) -> bool {
    json_str(&event.after_state, &["source_event"]).as_deref() == Some("AuthorityEpochChanged")
}

pub(super) fn max_timestamp(
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    supplemental_chain_observations: &[SupplementalChainObservation],
) -> Option<OffsetDateTime> {
    let mut timestamps = Vec::new();
    if let Some(timestamp) = name.block_timestamp {
        timestamps.push(timestamp);
    }
    if let Some(binding) = current_binding
        && let Some(timestamp) = binding.block_timestamp
    {
        timestamps.push(timestamp);
    }
    timestamps.extend(events.iter().filter_map(|event| event.block_timestamp));
    timestamps.extend(
        history_heads
            .iter()
            .filter_map(|event| event.block_timestamp),
    );
    timestamps.extend(
        supplemental_chain_observations
            .iter()
            .map(|observation| observation.candidate.timestamp),
    );
    timestamps.into_iter().max()
}

pub(super) fn chain_slot(chain_id: &str) -> String {
    match chain_id {
        "ethereum-mainnet" => "ethereum".to_owned(),
        "base-mainnet" => "base".to_owned(),
        _ => chain_id.to_owned(),
    }
}
