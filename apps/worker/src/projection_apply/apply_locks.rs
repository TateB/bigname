use std::{collections::BTreeSet, time::Duration};

use anyhow::{Context, Result, bail};
use sqlx::{Connection, PgConnection, PgPool};
use tokio::time::timeout;

use super::apply::ClaimedInvalidation;

const APPLY_LOCK_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const APPLY_LOCK_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct InvalidationApplyLocks {
    conn: PgConnection,
    keys: Vec<String>,
}

pub(super) async fn acquire_invalidation_apply_locks(
    pool: &PgPool,
    invalidations: &[ClaimedInvalidation],
) -> Result<InvalidationApplyLocks> {
    acquire_invalidation_apply_locks_with_timeout(pool, invalidations, APPLY_LOCK_ACQUIRE_TIMEOUT)
        .await
}

pub(super) async fn acquire_invalidation_apply_locks_with_timeout(
    pool: &PgPool,
    invalidations: &[ClaimedInvalidation],
    acquire_timeout: Duration,
) -> Result<InvalidationApplyLocks> {
    let mut keys = invalidations
        .iter()
        .map(invalidation_apply_lock_key)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let connect_options = pool.connect_options();
    let mut conn = timeout(
        APPLY_LOCK_CONNECT_TIMEOUT,
        PgConnection::connect_with(&connect_options),
    )
    .await
    .context("timed out opening projection invalidation apply lock connection")?
    .context("failed to open projection invalidation apply lock connection")?;

    if acquire_timeout.is_zero() {
        bail!("projection invalidation apply lock acquire timeout must be positive");
    }

    ensure_invalidation_apply_locks_connection_alive(&mut conn)
        .await
        .context("projection invalidation apply lock connection failed initial liveness check")?;

    for key in &keys {
        timeout(
            acquire_timeout,
            sqlx::query("SELECT pg_advisory_lock(hashtextextended($1::text, 0::bigint))")
                .bind(key)
                .execute(&mut conn),
        )
        .await
        .with_context(|| format!("timed out acquiring projection invalidation apply lock {key}"))?
        .with_context(|| format!("failed to acquire projection invalidation apply lock {key}"))?;
        ensure_invalidation_apply_locks_connection_alive(&mut conn)
            .await
            .with_context(|| {
                format!("projection invalidation apply lock connection failed liveness check after acquiring {key}")
            })?;
    }
    keys.reverse();

    Ok(InvalidationApplyLocks { conn, keys })
}

pub(super) async fn ensure_invalidation_apply_locks_alive(
    locks: &mut InvalidationApplyLocks,
) -> Result<()> {
    ensure_invalidation_apply_locks_connection_alive(&mut locks.conn)
        .await
        .context("projection invalidation apply lock connection is not alive")
}

async fn ensure_invalidation_apply_locks_connection_alive(conn: &mut PgConnection) -> Result<()> {
    let probe: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(conn)
        .await
        .context("failed to run projection invalidation apply lock liveness probe")?;
    if probe != 1 {
        bail!("projection invalidation apply lock liveness probe returned {probe}");
    }

    Ok(())
}

#[cfg(test)]
pub(super) async fn invalidation_apply_locks_backend_pid(
    locks: &mut InvalidationApplyLocks,
) -> Result<i32> {
    sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut locks.conn)
        .await
        .context("failed to load projection invalidation apply lock backend pid")
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
