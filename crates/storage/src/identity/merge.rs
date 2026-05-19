use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::CanonicalityState;

pub(super) struct StableObservationRefresh {
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) provenance: String,
}

pub(super) struct StableObservationInput<'a> {
    pub(super) chain_id: &'a str,
    pub(super) block_hash: &'a str,
    pub(super) block_number: i64,
    pub(super) provenance: &'a Value,
}

pub(super) fn merge_token_lineage_anchor(
    current: Option<Uuid>,
    incoming: Option<Uuid>,
) -> Result<Option<Uuid>> {
    match (current, incoming) {
        (Some(current), Some(incoming)) if current != incoming => {
            bail!("resource token_lineage_id mismatch: stored {current}, incoming {incoming}")
        }
        (Some(current), _) => Ok(Some(current)),
        (None, incoming) => Ok(incoming),
    }
}

pub(super) fn merge_stable_row_observation(
    current_state: CanonicalityState,
    current: StableObservationInput<'_>,
    incoming: StableObservationInput<'_>,
) -> Result<StableObservationRefresh> {
    let same_anchor = current.chain_id == incoming.chain_id
        && current.block_hash == incoming.block_hash
        && current.block_number == incoming.block_number;

    if !same_anchor && current_state != CanonicalityState::Orphaned {
        bail!(
            "stable identity row cannot change observation anchor before orphaning: stored {}/{}/{}, incoming {}/{}/{}",
            current.chain_id,
            current.block_hash,
            current.block_number,
            incoming.chain_id,
            incoming.block_hash,
            incoming.block_number
        );
    }

    let provenance = if same_anchor && current.provenance == incoming.provenance {
        serde_json::to_string(current.provenance)
            .context("failed to serialize stable-row provenance")?
    } else {
        serde_json::to_string(incoming.provenance)
            .context("failed to serialize stable-row provenance")?
    };

    Ok(StableObservationRefresh {
        chain_id: incoming.chain_id.to_owned(),
        block_hash: incoming.block_hash.to_owned(),
        block_number: incoming.block_number,
        provenance,
    })
}

pub(super) fn merge_binding_active_to(
    current: Option<OffsetDateTime>,
    incoming: Option<OffsetDateTime>,
) -> Result<Option<OffsetDateTime>> {
    match (current, incoming) {
        (Some(current), Some(incoming)) => Ok(Some(current.min(incoming))),
        (Some(current), _) => Ok(Some(current)),
        (None, incoming) => Ok(incoming),
    }
}
