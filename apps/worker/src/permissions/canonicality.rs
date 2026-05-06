use std::collections::BTreeMap;

use bigname_storage::CanonicalityState;
use serde_json::{Value, json};

pub(super) use crate::projection_json::format_timestamp;

use super::types::{ChainPositionCandidate, RelevantEvent};

pub(super) fn build_chain_positions(events: &[&RelevantEvent]) -> Value {
    let mut chain_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    for event in events {
        let Some(timestamp) = event.block_timestamp else {
            continue;
        };
        let candidate = ChainPositionCandidate {
            chain_id: event.chain_id.clone(),
            block_number: event.block_number,
            block_hash: event.block_hash.clone(),
            timestamp,
        };

        match chain_positions.get(&candidate.chain_id) {
            Some(existing)
                if existing.block_number > candidate.block_number
                    || (existing.block_number == candidate.block_number
                        && existing.block_hash >= candidate.block_hash) => {}
            _ => {
                chain_positions.insert(candidate.chain_id.clone(), candidate);
            }
        }
    }

    json!(
        chain_positions
            .into_iter()
            .map(|(chain_id, candidate)| {
                (
                    chain_id,
                    json!({
                        "chain_id": candidate.chain_id,
                        "block_number": candidate.block_number,
                        "block_hash": candidate.block_hash,
                        "timestamp": format_timestamp(candidate.timestamp),
                    }),
                )
            })
            .collect::<serde_json::Map<String, Value>>()
    )
}

pub(super) fn build_canonicality_summary(events: &[&RelevantEvent]) -> Value {
    let status = weakest_canonicality(events.iter().map(|event| event.canonicality_state))
        .unwrap_or(CanonicalityState::Canonical);

    let mut chain_states = BTreeMap::<String, CanonicalityState>::new();
    for event in events {
        let replacement = chain_states
            .get(&event.chain_id)
            .map(|current| {
                canonicality_rank(event.canonicality_state) < canonicality_rank(*current)
            })
            .unwrap_or(true);
        if replacement {
            chain_states.insert(event.chain_id.clone(), event.canonicality_state);
        }
    }

    json!({
        "status": status.as_str(),
        "chains": chain_states
            .into_iter()
            .map(|(chain_id, state)| (chain_id, Value::String(state.as_str().to_owned())))
            .collect::<serde_json::Map<String, Value>>(),
    })
}

fn weakest_canonicality(
    states: impl IntoIterator<Item = CanonicalityState>,
) -> Option<CanonicalityState> {
    states
        .into_iter()
        .min_by_key(|state| canonicality_rank(*state))
}

fn canonicality_rank(state: CanonicalityState) -> u8 {
    match state {
        CanonicalityState::Canonical => 0,
        CanonicalityState::Safe => 1,
        CanonicalityState::Finalized => 2,
        CanonicalityState::Observed => 3,
        CanonicalityState::Orphaned => 4,
    }
}
