use std::collections::BTreeMap;

use bigname_storage::{ChainPosition, ChainPositions, SelectedSnapshot, SnapshotConsistency};
use sqlx::PgPool;
use tracing::error;

use crate::v2::{
    AsOf, Meta, V2Error, V2Result, encode_at_token, format_timestamp, slug_to_numeric,
    snapshot_slot_for_slug,
};

pub(crate) async fn load_served_head_meta(pool: &PgPool) -> V2Result<Meta> {
    let status = bigname_storage::load_indexing_status(pool)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                error = ?load_error,
                "failed to load v2 lookup indexing status"
            );
            V2Error::internal_error("failed to load lookup served head")
        })?;
    let checkpoint_chain_ids = status
        .chains
        .iter()
        .filter(|row| row.canonical_block.is_some() && row.canonical_timestamp.is_some())
        .map(|row| row.chain_id.clone())
        .collect::<Vec<_>>();
    let hashes = bigname_storage::load_chain_checkpoint_snapshots(pool, &checkpoint_chain_ids)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                chain_count = checkpoint_chain_ids.len(),
                error = ?load_error,
                "failed to load v2 lookup head checkpoint snapshots"
            );
            V2Error::internal_error("failed to load lookup served head")
        })?
        .into_iter()
        .map(|checkpoint| (checkpoint.chain_id, checkpoint.canonical_block_hash))
        .collect::<BTreeMap<_, _>>();
    let mut as_of = BTreeMap::new();
    let mut positions = BTreeMap::new();

    for row in status.chains {
        let Some(block_number) = row.canonical_block else {
            continue;
        };
        let Some(timestamp) = row.canonical_timestamp else {
            continue;
        };
        let Some(block_hash) = hashes.get(&row.chain_id).cloned().flatten() else {
            continue;
        };
        let numeric_chain_id = slug_to_numeric(&row.chain_id).ok_or_else(|| {
            V2Error::internal_error(format!(
                "indexing status row uses unmapped chain_id {}",
                row.chain_id
            ))
        })?;
        let slot = snapshot_slot_for_slug(&row.chain_id).ok_or_else(|| {
            V2Error::internal_error(format!(
                "indexing status row uses unmapped snapshot slot {}",
                row.chain_id
            ))
        })?;
        let block_number = u64::try_from(block_number).map_err(|_| {
            V2Error::internal_error(format!(
                "indexing status row for {} has a negative head block",
                row.chain_id
            ))
        })?;

        as_of.insert(
            numeric_chain_id.to_string(),
            AsOf {
                block_number,
                block_hash: block_hash.clone(),
                timestamp: format_timestamp(timestamp),
            },
        );
        if positions
            .insert(
                slot.to_owned(),
                ChainPosition {
                    slot: slot.to_owned(),
                    chain_id: row.chain_id,
                    block_number: block_number as i64,
                    block_hash,
                    timestamp,
                },
            )
            .is_some()
        {
            return Err(V2Error::internal_error(format!(
                "indexing status rows repeat snapshot slot {slot}"
            )));
        }
    }

    let selected = SelectedSnapshot {
        chain_positions: ChainPositions::new(positions),
        consistency: SnapshotConsistency::Head,
    };

    Ok(Meta {
        as_of: Some(as_of),
        as_of_token: Some(encode_at_token(&selected)),
        ..Meta::default()
    })
}
