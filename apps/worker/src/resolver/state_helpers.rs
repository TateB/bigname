use std::collections::BTreeMap;

use anyhow::Result;
use bigname_storage::{CanonicalityState, PermissionsCurrentRow};
use serde_json::{Value, json};

use crate::projection_json::format_timestamp;

use super::target_loading::{AliasSeed, CurrentBindingSeed};

#[derive(Clone, Debug)]
struct ChainPositionCandidate {
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: String,
}

pub(super) fn build_chain_positions(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Value {
    let mut chain_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    for binding in bindings {
        let Some(timestamp) = binding.block_timestamp else {
            continue;
        };
        let candidate = ChainPositionCandidate {
            chain_id: binding.chain_id.clone(),
            block_number: binding.block_number,
            block_hash: binding.block_hash.clone(),
            timestamp: format_timestamp(timestamp),
        };
        merge_chain_position(&mut chain_positions, candidate);
    }

    for alias in aliases {
        let Some(timestamp) = alias.block_timestamp else {
            continue;
        };
        let candidate = ChainPositionCandidate {
            chain_id: alias.chain_id.clone(),
            block_number: alias.block_number,
            block_hash: alias.block_hash.clone(),
            timestamp: format_timestamp(timestamp),
        };
        merge_chain_position(&mut chain_positions, candidate);
    }

    for permission in permissions {
        let Some(entries) = permission.chain_positions.as_object() else {
            continue;
        };
        for entry in entries.values() {
            let Some(candidate) = decode_chain_position(entry) else {
                continue;
            };
            merge_chain_position(&mut chain_positions, candidate);
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
                        "timestamp": candidate.timestamp,
                    }),
                )
            })
            .collect::<serde_json::Map<String, Value>>()
    )
}

pub(super) fn build_canonicality_summary(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Result<Value> {
    let mut statuses = bindings
        .iter()
        .map(|binding| binding.canonicality_state)
        .collect::<Vec<_>>();
    let mut chain_states = BTreeMap::<String, CanonicalityState>::new();

    for binding in bindings {
        merge_chain_state(
            &mut chain_states,
            binding.chain_id.clone(),
            binding.canonicality_state,
        );
    }

    for alias in aliases {
        statuses.push(alias.canonicality_state);
        merge_chain_state(
            &mut chain_states,
            alias.chain_id.clone(),
            alias.canonicality_state,
        );
    }

    for permission in permissions {
        if let Some(status) = permission
            .canonicality_summary
            .get("status")
            .and_then(Value::as_str)
        {
            statuses.push(parse_canonicality_state(status)?);
        }
        if let Some(chains) = permission
            .canonicality_summary
            .get("chains")
            .and_then(Value::as_object)
        {
            for (chain_id, value) in chains {
                let Some(state) = value.as_str() else {
                    continue;
                };
                merge_chain_state(
                    &mut chain_states,
                    chain_id.clone(),
                    parse_canonicality_state(state)?,
                );
            }
        }
    }

    let status = weakest_canonicality(statuses).unwrap_or(CanonicalityState::Canonical);
    Ok(json!({
        "status": status.as_str(),
        "chains": chain_states
            .into_iter()
            .map(|(chain_id, state)| (chain_id, Value::String(state.as_str().to_owned())))
            .collect::<serde_json::Map<String, Value>>(),
    }))
}

pub(super) fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    CanonicalityState::parse(value)
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

fn merge_chain_state(
    chain_states: &mut BTreeMap<String, CanonicalityState>,
    chain_id: String,
    state: CanonicalityState,
) {
    let replace = chain_states
        .get(&chain_id)
        .map(|current| canonicality_rank(state) < canonicality_rank(*current))
        .unwrap_or(true);
    if replace {
        chain_states.insert(chain_id, state);
    }
}

fn merge_chain_position(
    chain_positions: &mut BTreeMap<String, ChainPositionCandidate>,
    candidate: ChainPositionCandidate,
) {
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

fn decode_chain_position(value: &Value) -> Option<ChainPositionCandidate> {
    let chain_id = value.get("chain_id")?.as_str()?.to_owned();
    let block_number = value.get("block_number")?.as_i64()?;
    let block_hash = value.get("block_hash")?.as_str()?.to_owned();
    let timestamp = value.get("timestamp")?.as_str()?.to_owned();

    Some(ChainPositionCandidate {
        chain_id,
        block_number,
        block_hash,
        timestamp,
    })
}
