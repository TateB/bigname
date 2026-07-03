use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;

use super::error::{SnapshotSelectionError, SnapshotSelectionResult};
use super::parsing::{decode_chain_positions_value, parse_explicit_chain_positions_json};
use crate::time::format_timestamp;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotPositionRequirement {
    pub slot: String,
    pub chain_id: String,
}

impl SnapshotPositionRequirement {
    pub fn new(slot: impl Into<String>, chain_id: impl Into<String>) -> Self {
        Self {
            slot: slot.into(),
            chain_id: chain_id.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotSelectionScope {
    required_positions: Vec<SnapshotPositionRequirement>,
    authoritative_slot: Option<String>,
}

impl SnapshotSelectionScope {
    pub fn new(
        required_positions: Vec<SnapshotPositionRequirement>,
        authoritative_slot: Option<String>,
    ) -> SnapshotSelectionResult<Self> {
        if required_positions.is_empty() {
            return Err(SnapshotSelectionError::invalid_input(
                "snapshot selection scope must require at least one position slot",
            ));
        }

        let mut seen_slots = BTreeSet::new();
        let mut seen_chain_ids = BTreeSet::new();
        for requirement in &required_positions {
            if requirement.slot.trim().is_empty() {
                return Err(SnapshotSelectionError::invalid_input(
                    "snapshot selection scope contains an empty position slot",
                ));
            }
            if requirement.chain_id.trim().is_empty() {
                return Err(SnapshotSelectionError::invalid_input(format!(
                    "snapshot selection slot {} has an empty chain_id",
                    requirement.slot
                )));
            }
            if !seen_slots.insert(requirement.slot.clone()) {
                return Err(SnapshotSelectionError::invalid_input(format!(
                    "snapshot selection scope repeats position slot {}",
                    requirement.slot
                )));
            }
            if !seen_chain_ids.insert(requirement.chain_id.clone()) {
                return Err(SnapshotSelectionError::invalid_input(format!(
                    "snapshot selection scope repeats chain_id {}",
                    requirement.chain_id
                )));
            }
        }

        if let Some(authoritative_slot) = authoritative_slot.as_ref()
            && !seen_slots.contains(authoritative_slot)
        {
            return Err(SnapshotSelectionError::invalid_input(format!(
                "authoritative snapshot slot {authoritative_slot} is not required by the scope"
            )));
        }

        Ok(Self {
            required_positions,
            authoritative_slot,
        })
    }

    pub fn required_positions(&self) -> &[SnapshotPositionRequirement] {
        &self.required_positions
    }

    pub fn authoritative_slot(&self) -> Option<&str> {
        self.authoritative_slot.as_deref()
    }

    pub(super) fn requirement_for_slot(&self, slot: &str) -> Option<&SnapshotPositionRequirement> {
        self.required_positions
            .iter()
            .find(|requirement| requirement.slot == slot)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainPosition {
    pub slot: String,
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub timestamp: OffsetDateTime,
}

impl ChainPosition {
    pub fn to_value(&self) -> Value {
        json!({
            "chain_id": self.chain_id,
            "block_number": self.block_number,
            "block_hash": self.block_hash,
            "timestamp": format_timestamp(self.timestamp),
        })
    }

    fn identity(&self) -> ChainPositionIdentity {
        ChainPositionIdentity {
            block_number: self.block_number,
            block_hash: self.block_hash.clone(),
            timestamp: self.timestamp,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainPositions {
    positions: BTreeMap<String, ChainPosition>,
}

impl ChainPositions {
    pub fn new(positions: BTreeMap<String, ChainPosition>) -> Self {
        Self { positions }
    }

    pub fn from_value(value: &Value) -> SnapshotSelectionResult<Self> {
        decode_chain_positions_value(value, "chain_positions")
    }

    pub fn parse_explicit_json(
        raw: &str,
        scope: &SnapshotSelectionScope,
    ) -> SnapshotSelectionResult<Self> {
        parse_explicit_chain_positions_json(raw, scope)
    }

    pub fn as_map(&self) -> &BTreeMap<String, ChainPosition> {
        &self.positions
    }

    pub fn get(&self, slot: &str) -> Option<&ChainPosition> {
        self.positions.get(slot)
    }

    pub fn to_value(&self) -> Value {
        Value::Object(
            self.positions
                .iter()
                .map(|(slot, position)| (slot.clone(), position.to_value()))
                .collect(),
        )
    }

    pub fn equivalent_by_chain_id(&self, other: &Self) -> bool {
        let Some(left) = self.by_chain_id() else {
            return false;
        };
        let Some(right) = other.by_chain_id() else {
            return false;
        };
        left == right
    }

    pub(super) fn validate_scope(
        &self,
        scope: &SnapshotSelectionScope,
    ) -> SnapshotSelectionResult<()> {
        for slot in self.positions.keys() {
            if scope.requirement_for_slot(slot).is_none() {
                return Err(SnapshotSelectionError::invalid_input(format!(
                    "unsupported snapshot position slot {slot}"
                )));
            }
        }

        for requirement in scope.required_positions() {
            let Some(position) = self.positions.get(&requirement.slot) else {
                return Err(SnapshotSelectionError::invalid_input(format!(
                    "missing required snapshot position slot {}",
                    requirement.slot
                )));
            };
            if position.chain_id != requirement.chain_id {
                return Err(SnapshotSelectionError::invalid_input(format!(
                    "snapshot position slot {} names chain_id {}, expected {}",
                    requirement.slot, position.chain_id, requirement.chain_id
                )));
            }
        }

        Ok(())
    }

    fn by_chain_id(&self) -> Option<BTreeMap<String, ChainPositionIdentity>> {
        let mut by_chain_id = BTreeMap::new();
        for position in self.positions.values() {
            if by_chain_id
                .insert(position.chain_id.clone(), position.identity())
                .is_some()
            {
                return None;
            }
        }
        Some(by_chain_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChainPositionIdentity {
    block_number: i64,
    block_hash: String,
    timestamp: OffsetDateTime,
}
