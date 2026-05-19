use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{Executor, PgPool, Postgres, postgres::PgRow};

use crate::{CanonicalityState, evm_primitives::normalize_evm_b256};

/// Persisted exact block-anchored call snapshot stored as an immutable raw fact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawCallSnapshot {
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub request_hash: String,
    pub request_payload: Value,
    pub response_hash: String,
    pub response_payload: Value,
    pub canonicality_state: CanonicalityState,
}

/// Insert missing raw call snapshots or refresh canonicality for already
/// observed block-scoped call snapshots.
///
/// `raw_call_snapshots` remain intake-owned raw facts even when another
/// workstream supplies an admitted exact-block handoff candidate.
pub async fn upsert_raw_call_snapshots(
    pool: &PgPool,
    snapshots: &[RawCallSnapshot],
) -> Result<Vec<RawCallSnapshot>> {
    if snapshots.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw call snapshot upsert")?;

    let persisted = upsert_raw_call_snapshots_in_transaction(&mut transaction, snapshots).await?;

    transaction
        .commit()
        .await
        .context("failed to commit raw call snapshot upsert")?;

    Ok(persisted)
}

/// Insert missing raw call snapshots or refresh canonicality inside an
/// existing transaction so intake can persist them in the same block admission
/// unit as other raw facts.
///
/// Callers outside intake should treat this as the narrow storage boundary for
/// an already admitted exact-block handoff rather than as an execution-owned
/// persistence surface.
pub async fn upsert_raw_call_snapshots_in_transaction(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    snapshots: &[RawCallSnapshot],
) -> Result<Vec<RawCallSnapshot>> {
    if snapshots.is_empty() {
        return Ok(Vec::new());
    }

    let snapshots = snapshots
        .iter()
        .map(normalize_raw_call_snapshot)
        .collect::<Vec<_>>();
    let mut persisted = Vec::with_capacity(snapshots.len());
    for snapshot in &snapshots {
        validate_raw_call_snapshot(snapshot)?;
        persisted.push(upsert_raw_call_snapshot(transaction, snapshot).await?);
    }

    Ok(persisted)
}

/// Load stored raw call snapshots for one exact block identity.
pub async fn load_raw_call_snapshots_by_block_hash(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<Vec<RawCallSnapshot>> {
    let block_hash = normalize_evm_b256(block_hash);
    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            request_hash,
            request_payload,
            response_hash,
            response_payload,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_call_snapshots
        WHERE chain_id = $1
          AND block_hash = $2
        ORDER BY request_hash
        "#,
    )
    .bind(chain_id)
    .bind(&block_hash)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load raw call snapshots for chain {chain_id} block {block_hash}")
    })?;

    rows.into_iter().map(decode_raw_call_snapshot).collect()
}

fn normalize_raw_call_snapshot(snapshot: &RawCallSnapshot) -> RawCallSnapshot {
    RawCallSnapshot {
        chain_id: snapshot.chain_id.clone(),
        block_hash: normalize_evm_b256(&snapshot.block_hash),
        block_number: snapshot.block_number,
        request_hash: normalize_evm_b256(&snapshot.request_hash),
        request_payload: snapshot.request_payload.clone(),
        response_hash: normalize_evm_b256(&snapshot.response_hash),
        response_payload: snapshot.response_payload.clone(),
        canonicality_state: snapshot.canonicality_state,
    }
}

async fn upsert_raw_call_snapshot(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    snapshot: &RawCallSnapshot,
) -> Result<RawCallSnapshot> {
    if let Some(persisted) = sqlx::query(
        r#"
        INSERT INTO raw_call_snapshots (
            chain_id,
            block_hash,
            block_number,
            request_hash,
            request_payload,
            response_hash,
            response_payload,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::canonicality_state)
        ON CONFLICT (chain_id, block_hash, request_hash) DO NOTHING
        RETURNING
            chain_id,
            block_hash,
            block_number,
            request_hash,
            request_payload,
            response_hash,
            response_payload,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&snapshot.chain_id)
    .bind(&snapshot.block_hash)
    .bind(snapshot.block_number)
    .bind(&snapshot.request_hash)
    .bind(&snapshot.request_payload)
    .bind(&snapshot.response_hash)
    .bind(&snapshot.response_payload)
    .bind(snapshot.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw call snapshot for chain {} block {} request {}",
            snapshot.chain_id, snapshot.block_hash, snapshot.request_hash
        )
    })? {
        return decode_raw_call_snapshot(persisted);
    }

    let existing = load_raw_call_snapshot_internal(
        &mut **executor,
        &snapshot.chain_id,
        &snapshot.block_hash,
        &snapshot.request_hash,
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw call snapshot for chain {} block {} request {} after insert conflict",
            snapshot.chain_id, snapshot.block_hash, snapshot.request_hash
        )
    })?;

    ensure_raw_call_snapshot_identity_matches(&existing, snapshot)?;
    let next_state = existing
        .canonicality_state
        .merge_observation(snapshot.canonicality_state);

    let persisted = sqlx::query(
        r#"
        UPDATE raw_call_snapshots
        SET
            canonicality_state = $4::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = $2
          AND request_hash = $3
        RETURNING
            chain_id,
            block_hash,
            block_number,
            request_hash,
            request_payload,
            response_hash,
            response_payload,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&snapshot.chain_id)
    .bind(&snapshot.block_hash)
    .bind(&snapshot.request_hash)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh raw call snapshot for chain {} block {} request {}",
            snapshot.chain_id, snapshot.block_hash, snapshot.request_hash
        )
    })?;

    decode_raw_call_snapshot(persisted)
}

async fn load_raw_call_snapshot_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
    request_hash: &str,
) -> Result<Option<RawCallSnapshot>>
where
    E: Executor<'e, Database = Postgres>,
{
    let block_hash = normalize_evm_b256(block_hash);
    let request_hash = normalize_evm_b256(request_hash);
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            request_hash,
            request_payload,
            response_hash,
            response_payload,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_call_snapshots
        WHERE chain_id = $1
          AND block_hash = $2
          AND request_hash = $3
        "#,
    )
    .bind(chain_id)
    .bind(&block_hash)
    .bind(&request_hash)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load raw call snapshot for chain {chain_id} block {block_hash} request {request_hash}"
        )
    })?;

    row.map(decode_raw_call_snapshot).transpose()
}

fn validate_raw_call_snapshot(snapshot: &RawCallSnapshot) -> Result<()> {
    if snapshot.block_number < 0 {
        bail!(
            "raw call snapshot for chain {} block {} request {} has negative block number {}",
            snapshot.chain_id,
            snapshot.block_hash,
            snapshot.request_hash,
            snapshot.block_number
        );
    }
    if snapshot.request_hash.is_empty() {
        bail!(
            "raw call snapshot for chain {} block {} has empty request hash",
            snapshot.chain_id,
            snapshot.block_hash
        );
    }
    if snapshot.response_hash.is_empty() {
        bail!(
            "raw call snapshot for chain {} block {} request {} has empty response hash",
            snapshot.chain_id,
            snapshot.block_hash,
            snapshot.request_hash
        );
    }
    if !snapshot.request_payload.is_object() {
        bail!(
            "raw call snapshot for chain {} block {} request {} must have object request payload",
            snapshot.chain_id,
            snapshot.block_hash,
            snapshot.request_hash
        );
    }

    Ok(())
}

fn ensure_raw_call_snapshot_identity_matches(
    existing: &RawCallSnapshot,
    incoming: &RawCallSnapshot,
) -> Result<()> {
    if existing.block_number != incoming.block_number
        || existing.request_payload != incoming.request_payload
        || existing.response_hash != incoming.response_hash
        || existing.response_payload != incoming.response_payload
    {
        bail!(
            "raw call snapshot identity mismatch for chain {} block {} request {}",
            existing.chain_id,
            existing.block_hash,
            existing.request_hash
        );
    }

    Ok(())
}

fn decode_raw_call_snapshot(row: PgRow) -> Result<RawCallSnapshot> {
    Ok(RawCallSnapshot {
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        request_hash: crate::sql_row::get(&row, "request_hash")?,
        request_payload: crate::sql_row::get(&row, "request_payload")?,
        response_hash: crate::sql_row::get(&row, "response_hash")?,
        response_payload: crate::sql_row::get(&row, "response_payload")?,
        canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
    })
}

#[cfg(test)]
mod tests;
