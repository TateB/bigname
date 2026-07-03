use std::collections::BTreeMap;

use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Row, postgres::PgRow};

use super::chain_position::{
    ChainPosition, ChainPositions, SnapshotPositionRequirement, SnapshotSelectionScope,
};
use super::consistency::SnapshotConsistency;
use super::error::{SnapshotSelectionError, SnapshotSelectionResult};
use crate::checkpoints::{ChainCheckpoint, CheckpointBlockRef, load_chain_checkpoint};
use crate::lineage::{CanonicalityState, load_chain_lineage_block};
use crate::time::format_timestamp;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotAt {
    Timestamp(OffsetDateTime),
    ResolvedPositions(ChainPositions),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SnapshotSelectorInput {
    pub at: Option<SnapshotAt>,
    pub chain_positions: Option<ChainPositions>,
    pub consistency: SnapshotConsistency,
}

impl SnapshotSelectorInput {
    pub fn new(
        at: Option<SnapshotAt>,
        chain_positions: Option<ChainPositions>,
        consistency: SnapshotConsistency,
    ) -> SnapshotSelectionResult<Self> {
        if at.is_some() && chain_positions.is_some() {
            return Err(SnapshotSelectionError::invalid_input(
                "at and chain_positions are mutually exclusive snapshot selectors",
            ));
        }
        Ok(Self {
            at,
            chain_positions,
            consistency,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectedSnapshot {
    pub chain_positions: ChainPositions,
    pub consistency: SnapshotConsistency,
}

impl SelectedSnapshot {
    pub fn chain_positions_value(&self) -> Value {
        self.chain_positions.to_value()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotProjectionRead<T> {
    Found(T),
    NotFound,
}

pub async fn resolve_exact_name_snapshot_selection(
    pool: &PgPool,
    scope: &SnapshotSelectionScope,
    input: &SnapshotSelectorInput,
) -> SnapshotSelectionResult<SelectedSnapshot> {
    if input.at.is_some() && input.chain_positions.is_some() {
        return Err(SnapshotSelectionError::invalid_input(
            "at and chain_positions are mutually exclusive snapshot selectors",
        ));
    }

    let chain_positions = match (&input.at, &input.chain_positions) {
        (_, Some(chain_positions)) => {
            chain_positions.validate_scope(scope)?;
            validate_supplied_positions(pool, chain_positions, input.consistency).await?;
            chain_positions.clone()
        }
        (Some(SnapshotAt::ResolvedPositions(chain_positions)), None) => {
            chain_positions.validate_scope(scope)?;
            validate_supplied_positions(pool, chain_positions, input.consistency).await?;
            chain_positions.clone()
        }
        (Some(SnapshotAt::Timestamp(timestamp)), None) => {
            resolve_positions_at_timestamp(pool, scope, *timestamp, input.consistency).await?
        }
        (None, None) => resolve_latest_positions(pool, scope, input.consistency).await?,
    };

    validate_cross_chain_positions(scope, &chain_positions)?;
    Ok(SelectedSnapshot {
        chain_positions,
        consistency: input.consistency,
    })
}

pub fn ensure_projection_chain_positions_match(
    projection_family: &str,
    projection_chain_positions: &Value,
    selected_chain_positions: &ChainPositions,
) -> SnapshotSelectionResult<()> {
    let projected = ChainPositions::from_value(projection_chain_positions).map_err(|error| {
        SnapshotSelectionError::stale(format!(
            "{projection_family} projection has unusable chain_positions: {}",
            error.message()
        ))
    })?;

    if selected_chain_positions.equivalent_by_chain_id(&projected) {
        Ok(())
    } else {
        Err(SnapshotSelectionError::stale(format!(
            "{projection_family} projection does not match the selected snapshot"
        )))
    }
}

async fn validate_supplied_positions(
    pool: &PgPool,
    chain_positions: &ChainPositions,
    consistency: SnapshotConsistency,
) -> SnapshotSelectionResult<()> {
    for position in chain_positions.as_map().values() {
        let block = load_chain_lineage_block(pool, &position.chain_id, &position.block_hash)
            .await
            .map_err(|error| {
                SnapshotSelectionError::internal(format!(
                    "failed to load lineage for supplied snapshot position {} {}: {error}",
                    position.chain_id, position.block_hash
                ))
            })?
            .ok_or_else(|| {
                SnapshotSelectionError::conflict(format!(
                    "snapshot position {} {} is not present in stored lineage",
                    position.chain_id, position.block_hash
                ))
            })?;

        if block.block_number != position.block_number {
            return Err(SnapshotSelectionError::conflict(format!(
                "snapshot position {} {} has block_number {}, stored lineage has {}",
                position.chain_id, position.block_hash, position.block_number, block.block_number
            )));
        }
        if block.block_timestamp != position.timestamp {
            return Err(SnapshotSelectionError::conflict(format!(
                "snapshot position {} {} has timestamp {}, stored lineage has {}",
                position.chain_id,
                position.block_hash,
                format_timestamp(position.timestamp),
                format_timestamp(block.block_timestamp)
            )));
        }
        if !consistency.allows(block.canonicality_state) {
            return Err(SnapshotSelectionError::conflict(format!(
                "snapshot position {} {} does not satisfy consistency {}",
                position.chain_id,
                position.block_hash,
                consistency.as_str()
            )));
        }
    }

    Ok(())
}

async fn resolve_latest_positions(
    pool: &PgPool,
    scope: &SnapshotSelectionScope,
    consistency: SnapshotConsistency,
) -> SnapshotSelectionResult<ChainPositions> {
    let mut positions = BTreeMap::new();

    if let Some(authoritative_slot) = scope.authoritative_slot() {
        let authoritative_requirement = scope.requirement_for_slot(authoritative_slot).ok_or_else(
            || {
                SnapshotSelectionError::invalid_input(format!(
                    "authoritative snapshot slot {authoritative_slot} is not required by the scope"
                ))
            },
        )?;
        let authoritative_position =
            load_checkpoint_position(pool, authoritative_requirement, consistency).await?;
        let upper_bound = authoritative_position.timestamp;
        positions.insert(authoritative_position.slot.clone(), authoritative_position);

        for requirement in scope.required_positions() {
            if requirement.slot == authoritative_slot {
                continue;
            }
            let position =
                load_lineage_position_at_or_before(pool, requirement, upper_bound, consistency)
                    .await?;
            positions.insert(position.slot.clone(), position);
        }

        return Ok(ChainPositions::new(positions));
    }

    for requirement in scope.required_positions() {
        let position = load_checkpoint_position(pool, requirement, consistency).await?;
        positions.insert(position.slot.clone(), position);
    }

    Ok(ChainPositions::new(positions))
}

async fn resolve_positions_at_timestamp(
    pool: &PgPool,
    scope: &SnapshotSelectionScope,
    timestamp: OffsetDateTime,
    consistency: SnapshotConsistency,
) -> SnapshotSelectionResult<ChainPositions> {
    let mut positions = BTreeMap::new();

    if let Some(authoritative_slot) = scope.authoritative_slot() {
        let authoritative_requirement = scope.requirement_for_slot(authoritative_slot).ok_or_else(
            || {
                SnapshotSelectionError::invalid_input(format!(
                    "authoritative snapshot slot {authoritative_slot} is not required by the scope"
                ))
            },
        )?;
        let authoritative_position = load_lineage_position_at_or_before(
            pool,
            authoritative_requirement,
            timestamp,
            consistency,
        )
        .await?;
        let upper_bound = authoritative_position.timestamp;
        positions.insert(authoritative_position.slot.clone(), authoritative_position);

        for requirement in scope.required_positions() {
            if requirement.slot == authoritative_slot {
                continue;
            }
            let position =
                load_lineage_position_at_or_before(pool, requirement, upper_bound, consistency)
                    .await?;
            positions.insert(position.slot.clone(), position);
        }

        return Ok(ChainPositions::new(positions));
    }

    for requirement in scope.required_positions() {
        let position =
            load_lineage_position_at_or_before(pool, requirement, timestamp, consistency).await?;
        positions.insert(position.slot.clone(), position);
    }

    Ok(ChainPositions::new(positions))
}

async fn load_checkpoint_position(
    pool: &PgPool,
    requirement: &SnapshotPositionRequirement,
    consistency: SnapshotConsistency,
) -> SnapshotSelectionResult<ChainPosition> {
    let checkpoint = load_chain_checkpoint(pool, &requirement.chain_id)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load checkpoint for chain {}: {error}",
                requirement.chain_id
            ))
        })?
        .ok_or_else(|| {
            SnapshotSelectionError::conflict(format!(
                "chain {} has no stored checkpoint row",
                requirement.chain_id
            ))
        })?;
    let checkpoint_ref = checkpoint_block_ref(&checkpoint, consistency).ok_or_else(|| {
        SnapshotSelectionError::conflict(format!(
            "chain {} has no {} checkpoint",
            requirement.chain_id,
            consistency.as_str()
        ))
    })?;

    let block = load_chain_lineage_block(pool, &requirement.chain_id, &checkpoint_ref.block_hash)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load lineage for checkpoint {} {}: {error}",
                requirement.chain_id, checkpoint_ref.block_hash
            ))
        })?
        .ok_or_else(|| {
            SnapshotSelectionError::conflict(format!(
                "checkpoint for chain {} references missing lineage block {}",
                requirement.chain_id, checkpoint_ref.block_hash
            ))
        })?;

    if block.block_number != checkpoint_ref.block_number {
        return Err(SnapshotSelectionError::conflict(format!(
            "checkpoint for chain {} block {} stores number {}, lineage stores {}",
            requirement.chain_id,
            checkpoint_ref.block_hash,
            checkpoint_ref.block_number,
            block.block_number
        )));
    }
    if !consistency.allows(block.canonicality_state) {
        return Err(SnapshotSelectionError::conflict(format!(
            "checkpoint for chain {} block {} does not satisfy consistency {}",
            requirement.chain_id,
            checkpoint_ref.block_hash,
            consistency.as_str()
        )));
    }

    Ok(ChainPosition {
        slot: requirement.slot.clone(),
        chain_id: block.chain_id,
        block_number: block.block_number,
        block_hash: block.block_hash,
        timestamp: block.block_timestamp,
    })
}

async fn load_lineage_position_at_or_before(
    pool: &PgPool,
    requirement: &SnapshotPositionRequirement,
    upper_bound: OffsetDateTime,
    consistency: SnapshotConsistency,
) -> SnapshotSelectionResult<ChainPosition> {
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state::TEXT AS canonicality_state
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_timestamp <= $2
          AND (
              ($3 = 'head' AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              ))
              OR ($3 = 'safe' AND canonicality_state IN (
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              ))
              OR ($3 = 'finalized' AND canonicality_state = 'finalized'::canonicality_state)
          )
        ORDER BY block_timestamp DESC, block_number DESC, block_hash DESC
        LIMIT 1
        "#,
    )
    .bind(&requirement.chain_id)
    .bind(upper_bound)
    .bind(consistency.as_str())
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to load lineage position for chain {} at consistency {}: {error}",
            requirement.chain_id,
            consistency.as_str()
        ))
    })?;

    let row = row.ok_or_else(|| {
        SnapshotSelectionError::conflict(format!(
            "chain {} has no stored {} lineage position at or before {}",
            requirement.chain_id,
            consistency.as_str(),
            format_timestamp(upper_bound)
        ))
    })?;
    decode_lineage_position(requirement, row, consistency)
}

fn decode_lineage_position(
    requirement: &SnapshotPositionRequirement,
    row: PgRow,
    consistency: SnapshotConsistency,
) -> SnapshotSelectionResult<ChainPosition> {
    let canonicality_state = row
        .try_get::<String, _>("canonicality_state")
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode lineage canonicality state for chain {}: {error}",
                requirement.chain_id
            ))
        })
        .and_then(|value| {
            CanonicalityState::parse(&value).map_err(|error| {
                SnapshotSelectionError::internal(format!(
                    "failed to parse lineage canonicality state for chain {}: {error}",
                    requirement.chain_id
                ))
            })
        })?;
    if !consistency.allows(canonicality_state) {
        return Err(SnapshotSelectionError::conflict(format!(
            "lineage position for chain {} does not satisfy consistency {}",
            requirement.chain_id,
            consistency.as_str()
        )));
    }

    Ok(ChainPosition {
        slot: requirement.slot.clone(),
        chain_id: row.try_get("chain_id").map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode lineage chain_id for chain {}: {error}",
                requirement.chain_id
            ))
        })?,
        block_hash: row.try_get("block_hash").map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode lineage block_hash for chain {}: {error}",
                requirement.chain_id
            ))
        })?,
        block_number: row.try_get("block_number").map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode lineage block_number for chain {}: {error}",
                requirement.chain_id
            ))
        })?,
        timestamp: row.try_get("block_timestamp").map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to decode lineage block_timestamp for chain {}: {error}",
                requirement.chain_id
            ))
        })?,
    })
}

fn validate_cross_chain_positions(
    scope: &SnapshotSelectionScope,
    chain_positions: &ChainPositions,
) -> SnapshotSelectionResult<()> {
    let Some(authoritative_slot) = scope.authoritative_slot() else {
        return Ok(());
    };
    let authoritative = chain_positions.get(authoritative_slot).ok_or_else(|| {
        SnapshotSelectionError::invalid_input(format!(
            "missing authoritative snapshot position slot {authoritative_slot}"
        ))
    })?;

    for (slot, position) in chain_positions.as_map() {
        if slot == authoritative_slot {
            continue;
        }
        if position.timestamp > authoritative.timestamp {
            return Err(SnapshotSelectionError::conflict(format!(
                "snapshot position slot {slot} is newer than authoritative slot {authoritative_slot}"
            )));
        }
    }

    Ok(())
}

fn checkpoint_block_ref(
    checkpoint: &ChainCheckpoint,
    consistency: SnapshotConsistency,
) -> Option<CheckpointBlockRef> {
    match consistency {
        SnapshotConsistency::Head => Some(CheckpointBlockRef {
            block_hash: checkpoint.canonical_block_hash.clone()?,
            block_number: checkpoint.canonical_block_number?,
        }),
        SnapshotConsistency::Safe => Some(CheckpointBlockRef {
            block_hash: checkpoint.safe_block_hash.clone()?,
            block_number: checkpoint.safe_block_number?,
        }),
        SnapshotConsistency::Finalized => Some(CheckpointBlockRef {
            block_hash: checkpoint.finalized_block_hash.clone()?,
            block_number: checkpoint.finalized_block_number?,
        }),
    }
}
