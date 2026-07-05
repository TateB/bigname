use anyhow::{Context, Result, ensure};

use super::{BASE_NORMALIZED_REDERIVE_CHAIN_ID, BASE_NORMALIZED_REDERIVE_CURSOR_KIND};

pub(super) fn validate_deployment_profile(deployment_profile: &str) -> Result<()> {
    ensure!(
        !deployment_profile.trim().is_empty(),
        "Base normalized-event rederive deployment profile must not be empty"
    );
    Ok(())
}

pub(super) async fn validate_base_deployment_profile_owns_chain_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<()> {
    let raw_fact_cursor_rows = sqlx::query_scalar::<_, i64>(profile_ownership_sql())
        .bind(deployment_profile)
        .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
        .bind(BASE_NORMALIZED_REDERIVE_CURSOR_KIND)
        .fetch_one(&mut **transaction)
        .await
        .context("failed to validate Base normalized-event rederive deployment profile")?;
    ensure_base_deployment_profile_owns_chain(deployment_profile, raw_fact_cursor_rows)
}

fn profile_ownership_sql() -> &'static str {
    r#"
    SELECT COUNT(*)::BIGINT
    FROM normalized_replay_cursors
    WHERE deployment_profile = $1
      AND chain_id = $2
      AND cursor_kind = $3
    "#
}

fn ensure_base_deployment_profile_owns_chain(
    deployment_profile: &str,
    raw_fact_cursor_rows: i64,
) -> Result<()> {
    ensure!(
        raw_fact_cursor_rows > 0,
        "Base normalized-event rederive deployment profile {deployment_profile:?} is not verified for the global Base delete: no {}/{} replay cursor exists",
        BASE_NORMALIZED_REDERIVE_CHAIN_ID,
        BASE_NORMALIZED_REDERIVE_CURSOR_KIND
    );
    Ok(())
}
