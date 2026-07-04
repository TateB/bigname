use anyhow::{Context, Result};

use super::super::guards::ensure_canonical_raw_log_floor_from;
use super::super::{
    BASE_NORMALIZED_REDERIVE_CHAIN_ID, BASE_NORMALIZED_REDERIVE_CURSOR_KIND,
    BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK, BaseNormalizedRederiveCounts, checkpoint_adapters,
    current_projection_replay_status_projections, cursor_kinds,
};
use super::state::RunState;

pub(super) async fn reset_replay_state(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &RunState,
) -> Result<BaseNormalizedRederiveCounts> {
    ensure_canonical_raw_log_floor_from(transaction).await?;
    let current_projection_replay_status =
        delete_current_projection_replay_status(transaction).await?;
    let adapter_checkpoint_item_rows =
        delete_replay_checkpoint_items(transaction, &state.deployment_profile).await?;
    let adapter_checkpoint_rows =
        delete_replay_checkpoints(transaction, &state.deployment_profile).await?;
    let replay_cursor_rows = reset_replay_cursors(
        transaction,
        &state.deployment_profile,
        state.replay_target_block,
    )
    .await?;
    Ok(BaseNormalizedRederiveCounts {
        current_projection_replay_status,
        replay_cursor_rows,
        adapter_checkpoint_rows,
        adapter_checkpoint_item_rows,
        ..BaseNormalizedRederiveCounts::default()
    })
}

async fn delete_current_projection_replay_status(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<i64> {
    let result = sqlx::query(
        r#"
        DELETE FROM current_projection_replay_status
        WHERE projection = ANY($1::TEXT[])
        "#,
    )
    .bind(current_projection_replay_status_projections())
    .execute(&mut **transaction)
    .await
    .context("failed to delete affected current projection replay markers")?;
    rows_affected(result)
}

async fn delete_replay_checkpoint_items(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<i64> {
    delete_replay_rows(
        transaction,
        r#"
        DELETE FROM normalized_replay_adapter_checkpoint_items
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = ANY($3::TEXT[])
          AND adapter = ANY($4::TEXT[])
        "#,
        deployment_profile,
    )
    .await
}

async fn delete_replay_checkpoints(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<i64> {
    delete_replay_rows(
        transaction,
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = ANY($3::TEXT[])
          AND adapter = ANY($4::TEXT[])
        "#,
        deployment_profile,
    )
    .await
}

async fn reset_replay_cursors(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
    replay_target_block: i64,
) -> Result<i64> {
    let result = sqlx::query(
        r#"
        DELETE FROM normalized_replay_cursors
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = ANY($3::TEXT[])
        "#,
    )
    .bind(deployment_profile)
    .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
    .bind(cursor_kinds())
    .execute(&mut **transaction)
    .await
    .context("failed to delete Base normalized-event replay cursors")?;
    let deleted = rows_affected(result)?;
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number
        )
        VALUES ($1, $2, $3, $4, $4, $5)
        "#,
    )
    .bind(deployment_profile)
    .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
    .bind(BASE_NORMALIZED_REDERIVE_CURSOR_KIND)
    .bind(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK)
    .bind(replay_target_block)
    .execute(&mut **transaction)
    .await
    .context("failed to reset Base normalized-event replay cursor")?;
    Ok(deleted)
}

async fn delete_replay_rows(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: &str,
    deployment_profile: &str,
) -> Result<i64> {
    let result = sqlx::query(sql)
        .bind(deployment_profile)
        .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
        .bind(cursor_kinds())
        .bind(checkpoint_adapters())
        .execute(&mut **transaction)
        .await
        .with_context(|| {
            format!("failed to execute Base normalized-event rederive reset: {sql}")
        })?;
    rows_affected(result)
}

fn rows_affected(result: sqlx::postgres::PgQueryResult) -> Result<i64> {
    i64::try_from(result.rows_affected()).context("row count overflowed i64")
}
