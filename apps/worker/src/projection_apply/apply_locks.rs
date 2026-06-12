use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::{Connection, PgConnection, PgPool};

use super::apply::ClaimedInvalidation;

pub(super) struct InvalidationApplyLocks {
    conn: PgConnection,
    keys: Vec<String>,
}

pub(super) async fn acquire_invalidation_apply_locks(
    pool: &PgPool,
    invalidations: &[ClaimedInvalidation],
) -> Result<InvalidationApplyLocks> {
    let mut keys = invalidations
        .iter()
        .map(invalidation_apply_lock_key)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let connect_options = pool.connect_options();
    let mut conn = PgConnection::connect_with(&connect_options)
        .await
        .context("failed to open projection invalidation apply lock connection")?;

    for key in &keys {
        sqlx::query("SELECT pg_advisory_lock(hashtextextended($1::text, 0::bigint))")
            .bind(key)
            .execute(&mut conn)
            .await
            .with_context(|| {
                format!("failed to acquire projection invalidation apply lock {key}")
            })?;
    }
    keys.reverse();

    Ok(InvalidationApplyLocks { conn, keys })
}

pub(super) async fn release_invalidation_apply_locks(
    locks: &mut InvalidationApplyLocks,
) -> Result<()> {
    for key in &locks.keys {
        sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint))")
            .bind(key)
            .execute(&mut locks.conn)
            .await
            .with_context(|| {
                format!("failed to release projection invalidation apply lock {key}")
            })?;
    }

    Ok(())
}

pub(super) fn invalidation_apply_lock_key(invalidation: &ClaimedInvalidation) -> String {
    format!(
        "{}:{};{}:{}",
        invalidation.projection.len(),
        invalidation.projection,
        invalidation.projection_key.len(),
        invalidation.projection_key
    )
}
