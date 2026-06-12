use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::reads::load_chain_lineage_block_internal;
use super::types::ChainLineageBlock;
use super::validation::validate_lineage_block;

/// Insert missing lineage rows or refresh existing rows when the same block hash
/// is observed again. Immutable block metadata must match the stored row.
pub async fn upsert_chain_lineage_blocks(
    pool: &PgPool,
    blocks: &[ChainLineageBlock],
) -> Result<Vec<ChainLineageBlock>> {
    upsert_chain_lineage_blocks_with_orphaned_conflict(
        pool,
        blocks,
        OrphanedLineageConflict::Preserve,
    )
    .await
}

/// Insert missing lineage rows or refresh existing rows, allowing explicitly
/// fresh reconciliation evidence to re-canonicalize a stored orphaned row.
pub async fn upsert_chain_lineage_blocks_recanonicalizing_orphaned(
    pool: &PgPool,
    blocks: &[ChainLineageBlock],
) -> Result<Vec<ChainLineageBlock>> {
    upsert_chain_lineage_blocks_with_orphaned_conflict(
        pool,
        blocks,
        OrphanedLineageConflict::Recanonicalize,
    )
    .await
}

async fn upsert_chain_lineage_blocks_with_orphaned_conflict(
    pool: &PgPool,
    blocks: &[ChainLineageBlock],
    orphaned_conflict: OrphanedLineageConflict,
) -> Result<Vec<ChainLineageBlock>> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }

    upsert_chain_lineage_blocks_without_snapshots_with_orphaned_conflict(
        pool,
        blocks,
        orphaned_conflict,
    )
    .await?;

    let mut snapshots = Vec::with_capacity(blocks.len());
    for block in blocks {
        let snapshot = load_chain_lineage_block_internal(pool, &block.chain_id, &block.block_hash)
            .await?
            .with_context(|| {
                format!(
                    "failed to reload lineage snapshot for chain {} block {} after upsert",
                    block.chain_id, block.block_hash
                )
            })?;
        snapshots.push(snapshot);
    }
    Ok(snapshots)
}

/// Insert or refresh chain lineage blocks without returning row snapshots.
///
/// Minimal header anchors are written once to `chain_lineage`. Optional
/// auditable header roots/bloom are written only when present, into
/// `chain_header_audit`.
pub async fn upsert_chain_lineage_blocks_without_snapshots(
    pool: &PgPool,
    blocks: &[ChainLineageBlock],
) -> Result<()> {
    upsert_chain_lineage_blocks_without_snapshots_with_orphaned_conflict(
        pool,
        blocks,
        OrphanedLineageConflict::Preserve,
    )
    .await
}

/// Insert or refresh chain lineage blocks without returning row snapshots,
/// allowing explicitly fresh reconciliation evidence to re-canonicalize a
/// stored orphaned row.
pub async fn upsert_chain_lineage_blocks_without_snapshots_recanonicalizing_orphaned(
    pool: &PgPool,
    blocks: &[ChainLineageBlock],
) -> Result<()> {
    upsert_chain_lineage_blocks_without_snapshots_with_orphaned_conflict(
        pool,
        blocks,
        OrphanedLineageConflict::Recanonicalize,
    )
    .await
}

async fn upsert_chain_lineage_blocks_without_snapshots_with_orphaned_conflict(
    pool: &PgPool,
    blocks: &[ChainLineageBlock],
    orphaned_conflict: OrphanedLineageConflict,
) -> Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }

    for block in blocks {
        validate_lineage_block(block)?;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for chain lineage bulk upsert")?;

    for chunk in blocks.chunks(BULK_LINEAGE_UPSERT_CHUNK_ROWS) {
        upsert_lineage_anchor_chunk_without_snapshots(&mut transaction, chunk, orphaned_conflict)
            .await?;
        upsert_header_audit_chunk_without_snapshots(&mut transaction, chunk).await?;
    }

    transaction
        .commit()
        .await
        .context("failed to commit chain lineage bulk upsert")?;

    Ok(())
}

#[derive(Clone, Copy)]
enum OrphanedLineageConflict {
    Preserve,
    Recanonicalize,
}

async fn upsert_lineage_anchor_chunk_without_snapshots(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    chunk: &[ChainLineageBlock],
    orphaned_conflict: OrphanedLineageConflict,
) -> Result<()> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        WITH input (
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            canonicality_state
        ) AS (
        "#,
    );
    push_lineage_anchor_values(&mut builder, chunk);
    builder.push(
        r#"
        ),
        mismatch_count AS (
            SELECT COUNT(*)::BIGINT AS value
            FROM input
            JOIN chain_lineage
              ON chain_lineage.chain_id = input.chain_id
             AND chain_lineage.block_hash = input.block_hash
            WHERE chain_lineage.parent_hash IS DISTINCT FROM input.parent_hash
               OR chain_lineage.block_number <> input.block_number
               OR chain_lineage.block_timestamp <> input.block_timestamp
        ),
        identity_guard AS (
            SELECT CASE
                WHEN value > 0 THEN 1 / (value - value)
                ELSE 1
            END AS ok
            FROM mismatch_count
        )
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        SELECT
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            canonicality_state::canonicality_state
        FROM input
        CROSS JOIN identity_guard
        WHERE identity_guard.ok = 1
        ON CONFLICT (chain_id, block_hash) DO UPDATE
        SET
            canonicality_state = CASE
                WHEN chain_lineage.canonicality_state = 'orphaned'::canonicality_state THEN
        "#,
    );
    push_orphaned_lineage_conflict_target(&mut builder, orphaned_conflict);
    builder.push(
        r#"
                WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state THEN 'orphaned'::canonicality_state
                WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                    AND chain_lineage.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                    THEN chain_lineage.canonicality_state
                WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                    AND chain_lineage.canonicality_state = 'finalized'::canonicality_state
                    THEN chain_lineage.canonicality_state
                WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                    THEN chain_lineage.canonicality_state
                ELSE EXCLUDED.canonicality_state
            END,
            observed_at = now()
        WHERE chain_lineage.parent_hash IS NOT DISTINCT FROM EXCLUDED.parent_hash
          AND chain_lineage.block_number = EXCLUDED.block_number
          AND chain_lineage.block_timestamp = EXCLUDED.block_timestamp
          AND chain_lineage.canonicality_state IS DISTINCT FROM CASE
                WHEN chain_lineage.canonicality_state = 'orphaned'::canonicality_state THEN
        "#,
    );
    push_orphaned_lineage_conflict_target(&mut builder, orphaned_conflict);
    builder.push(
        r#"
                WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state THEN 'orphaned'::canonicality_state
                WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                    AND chain_lineage.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                    THEN chain_lineage.canonicality_state
                WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                    AND chain_lineage.canonicality_state = 'finalized'::canonicality_state
                    THEN chain_lineage.canonicality_state
                WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                    THEN chain_lineage.canonicality_state
                ELSE EXCLUDED.canonicality_state
            END
        "#,
    );
    builder
        .build()
        .execute(&mut **transaction)
        .await
        .context("failed to upsert chain lineage anchors without snapshots; chain lineage identity mismatch or storage write error")?;

    Ok(())
}

fn push_orphaned_lineage_conflict_target(
    builder: &mut QueryBuilder<'_, Postgres>,
    orphaned_conflict: OrphanedLineageConflict,
) {
    match orphaned_conflict {
        OrphanedLineageConflict::Preserve => {
            builder.push("'orphaned'::canonicality_state");
        }
        OrphanedLineageConflict::Recanonicalize => {
            builder.push("EXCLUDED.canonicality_state");
        }
    }
}

async fn upsert_header_audit_chunk_without_snapshots(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    chunk: &[ChainLineageBlock],
) -> Result<()> {
    let audited_blocks = chunk
        .iter()
        .filter(|block| has_header_audit_fields(block))
        .collect::<Vec<_>>();
    if audited_blocks.is_empty() {
        return Ok(());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        WITH input (
            chain_id,
            block_hash,
            logs_bloom,
            transactions_root,
            receipts_root,
            state_root
        ) AS (
        "#,
    );
    push_header_audit_values(&mut builder, &audited_blocks);
    builder.push(
        r#"
        ),
        mismatch_count AS (
            SELECT COUNT(*)::BIGINT AS value
            FROM input
            JOIN chain_header_audit AS audit
              ON audit.chain_id = input.chain_id
             AND audit.block_hash = input.block_hash
            WHERE (audit.logs_bloom IS NOT NULL AND input.logs_bloom IS NOT NULL AND audit.logs_bloom <> input.logs_bloom)
               OR (audit.transactions_root IS NOT NULL AND input.transactions_root IS NOT NULL AND audit.transactions_root <> input.transactions_root)
               OR (audit.receipts_root IS NOT NULL AND input.receipts_root IS NOT NULL AND audit.receipts_root <> input.receipts_root)
               OR (audit.state_root IS NOT NULL AND input.state_root IS NOT NULL AND audit.state_root <> input.state_root)
        ),
        identity_guard AS (
            SELECT CASE
                WHEN value > 0 THEN 1 / (value - value)
                ELSE 1
            END AS ok
            FROM mismatch_count
        )
        INSERT INTO chain_header_audit (
            chain_id,
            block_hash,
            logs_bloom,
            transactions_root,
            receipts_root,
            state_root
        )
        SELECT
            chain_id,
            block_hash,
            logs_bloom,
            transactions_root,
            receipts_root,
            state_root
        FROM input
        CROSS JOIN identity_guard
        WHERE identity_guard.ok = 1
        ON CONFLICT (chain_id, block_hash) DO UPDATE
        SET
            logs_bloom = COALESCE(chain_header_audit.logs_bloom, EXCLUDED.logs_bloom),
            transactions_root = COALESCE(chain_header_audit.transactions_root, EXCLUDED.transactions_root),
            receipts_root = COALESCE(chain_header_audit.receipts_root, EXCLUDED.receipts_root),
            state_root = COALESCE(chain_header_audit.state_root, EXCLUDED.state_root),
            observed_at = now()
        WHERE (chain_header_audit.logs_bloom IS NULL OR EXCLUDED.logs_bloom IS NULL OR chain_header_audit.logs_bloom = EXCLUDED.logs_bloom)
          AND (chain_header_audit.transactions_root IS NULL OR EXCLUDED.transactions_root IS NULL OR chain_header_audit.transactions_root = EXCLUDED.transactions_root)
          AND (chain_header_audit.receipts_root IS NULL OR EXCLUDED.receipts_root IS NULL OR chain_header_audit.receipts_root = EXCLUDED.receipts_root)
          AND (chain_header_audit.state_root IS NULL OR EXCLUDED.state_root IS NULL OR chain_header_audit.state_root = EXCLUDED.state_root)
          AND (
            (chain_header_audit.logs_bloom IS NULL AND EXCLUDED.logs_bloom IS NOT NULL)
            OR (chain_header_audit.transactions_root IS NULL AND EXCLUDED.transactions_root IS NOT NULL)
            OR (chain_header_audit.receipts_root IS NULL AND EXCLUDED.receipts_root IS NOT NULL)
            OR (chain_header_audit.state_root IS NULL AND EXCLUDED.state_root IS NOT NULL)
          )
        "#,
    );
    builder
        .build()
        .execute(&mut **transaction)
        .await
        .context("failed to upsert chain header audit fields without snapshots; header audit identity mismatch or storage write error")?;

    Ok(())
}

fn push_lineage_anchor_values<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    chunk: &'a [ChainLineageBlock],
) {
    builder.push_values(chunk, |mut row, block| {
        row.push_bind(&block.chain_id)
            .push_bind(&block.block_hash)
            .push_bind(&block.parent_hash)
            .push_bind(block.block_number)
            .push_bind(block.block_timestamp)
            .push_bind(block.canonicality_state.as_str());
    });
}

fn push_header_audit_values<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    chunk: &'a [&'a ChainLineageBlock],
) {
    builder.push_values(chunk, |mut row, block| {
        row.push_bind(&block.chain_id)
            .push_bind(&block.block_hash)
            .push_bind(&block.logs_bloom)
            .push_bind(&block.transactions_root)
            .push_bind(&block.receipts_root)
            .push_bind(&block.state_root);
    });
}

fn has_header_audit_fields(block: &ChainLineageBlock) -> bool {
    block.logs_bloom.is_some()
        || block.transactions_root.is_some()
        || block.receipts_root.is_some()
        || block.state_root.is_some()
}

const BULK_LINEAGE_UPSERT_CHUNK_ROWS: usize = 10_000;
