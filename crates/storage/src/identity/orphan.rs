use anyhow::{Context, Result, bail};
use sqlx::{Executor, PgPool, Postgres, Row};

use super::types::IdentityOrphanCounts;

/// Walk one stored lineage branch from `from_hash` and mark matching surface
/// bindings `orphaned` until `stop_before_hash` is reached.
pub async fn mark_surface_binding_range_orphaned(
    pool: &PgPool,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<u64> {
    if stop_before_hash == Some(from_hash) {
        return Ok(0);
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for surface-binding orphaning")?;

    let block_hashes =
        load_chain_lineage_hash_path(&mut *transaction, chain_id, from_hash, stop_before_hash)
            .await
            .with_context(|| {
                format!(
                    "failed to load chain lineage path for surface-binding orphaning on chain {chain_id} from block {from_hash}"
                )
            })?;
    if block_hashes.is_empty() {
        bail!("missing stored lineage row for chain {chain_id} block {from_hash}");
    }

    let surface_binding_count = mark_identity_table_orphaned(
        &mut transaction,
        "surface_bindings",
        chain_id,
        &block_hashes,
    )
    .await?;

    transaction
        .commit()
        .await
        .context("failed to commit surface-binding orphaning")?;

    Ok(surface_binding_count)
}

/// Walk one stored lineage branch from `from_hash` and mark matching identity
/// rows `orphaned` until `stop_before_hash` is reached.
pub async fn mark_identity_rows_range_orphaned(
    pool: &PgPool,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<IdentityOrphanCounts> {
    if stop_before_hash == Some(from_hash) {
        return Ok(IdentityOrphanCounts::default());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for identity orphaning")?;

    let block_hashes =
        load_chain_lineage_hash_path(&mut *transaction, chain_id, from_hash, stop_before_hash)
            .await
            .with_context(|| {
                format!(
                    "failed to load chain lineage path for identity orphaning on chain {chain_id} from block {from_hash}"
                )
            })?;
    if block_hashes.is_empty() {
        bail!("missing stored lineage row for chain {chain_id} block {from_hash}");
    }

    let token_lineage_count =
        mark_identity_table_orphaned(&mut transaction, "token_lineages", chain_id, &block_hashes)
            .await?;
    let resource_count =
        mark_identity_table_orphaned(&mut transaction, "resources", chain_id, &block_hashes)
            .await?;
    let name_surface_count =
        mark_identity_table_orphaned(&mut transaction, "name_surfaces", chain_id, &block_hashes)
            .await?;
    let surface_binding_count = mark_identity_table_orphaned(
        &mut transaction,
        "surface_bindings",
        chain_id,
        &block_hashes,
    )
    .await?;

    transaction
        .commit()
        .await
        .context("failed to commit identity orphaning")?;

    Ok(IdentityOrphanCounts {
        token_lineage_count,
        resource_count,
        name_surface_count,
        surface_binding_count,
    })
}

async fn load_chain_lineage_hash_path<'e, E>(
    executor: E,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<Vec<String>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        WITH RECURSIVE lineage_path AS (
            SELECT chain_id, block_hash, parent_hash, 0 AS depth
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_hash = $2

            UNION ALL

            SELECT parent.chain_id, parent.block_hash, parent.parent_hash, lineage_path.depth + 1
            FROM chain_lineage AS parent
            JOIN lineage_path
              ON parent.chain_id = lineage_path.chain_id
             AND parent.block_hash = lineage_path.parent_hash
            WHERE $3::TEXT IS NULL
               OR parent.block_hash <> $3::TEXT
        )
        SELECT block_hash
        FROM lineage_path
        ORDER BY depth
        "#,
    )
    .bind(chain_id)
    .bind(from_hash)
    .bind(stop_before_hash)
    .fetch_all(executor)
    .await?;

    rows.into_iter()
        .map(|row| {
            row.try_get::<String, _>("block_hash")
                .context("failed to decode identity orphaning block_hash")
        })
        .collect()
}

async fn mark_identity_table_orphaned(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    table_name: &str,
    chain_id: &str,
    block_hashes: &[String],
) -> Result<u64> {
    let statement = format!(
        r#"
        UPDATE {table_name}
        SET
            canonicality_state = 'orphaned'::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state <> 'orphaned'::canonicality_state
        "#,
    );

    sqlx::query(&statement)
        .bind(chain_id)
        .bind(block_hashes)
        .execute(&mut **executor)
        .await
        .with_context(|| {
            format!("failed to mark orphaned identity rows in {table_name} for chain {chain_id}")
        })
        .map(|result| result.rows_affected())
}
