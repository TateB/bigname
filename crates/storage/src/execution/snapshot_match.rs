use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::lineage::chain_lineage_contains_canonical_ancestor_position;
use crate::snapshot_selection::ChainPositions;

use super::{decode::decode_requested_chain_positions, types::ExecutionOutcome};

pub(super) async fn resolution_execution_outcome_is_at_or_before_snapshot(
    pool: &PgPool,
    outcome: &ExecutionOutcome,
    snapshot_positions: &ChainPositions,
) -> Result<bool> {
    let requested_positions = decode_requested_chain_positions(
        &outcome.cache_key.requested_chain_positions,
        &outcome.cache_key.request_key,
    )
    .context("failed to decode resolution execution requested_chain_positions")?;

    for requested_position in &requested_positions {
        let Some(selected_position) = snapshot_positions
            .as_map()
            .values()
            .find(|selected_position| selected_position.chain_id == requested_position.chain_id)
        else {
            return Ok(false);
        };

        if requested_position.block_number > selected_position.block_number {
            return Ok(false);
        }
        if requested_position.block_number == selected_position.block_number {
            if requested_position.block_hash != selected_position.block_hash {
                return Ok(false);
            }
            continue;
        }
        if !chain_lineage_contains_canonical_ancestor_position(
            pool,
            &requested_position.chain_id,
            &selected_position.block_hash,
            selected_position.block_number,
            requested_position.block_number,
            &requested_position.block_hash,
        )
        .await?
        {
            return Ok(false);
        }
    }

    let requested_chain_ids = requested_positions
        .iter()
        .map(|position| position.chain_id.as_str())
        .collect::<BTreeSet<_>>();
    for selected_chain_id in snapshot_positions
        .as_map()
        .values()
        .map(|position| position.chain_id.as_str())
    {
        if !requested_chain_ids.contains(selected_chain_id) {
            return Ok(false);
        }
    }

    Ok(true)
}
