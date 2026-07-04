use std::collections::BTreeMap;

use anyhow::{Context, Result, bail, ensure};
use sqlx::{PgPool, Postgres, Row, pool::PoolConnection};
use tracing::info;

mod counts;
mod execution;

use counts::{
    load_counts, load_counts_from, load_family_census, load_family_census_from,
    load_raw_fact_completeness, load_raw_fact_completeness_from,
};
use execution::{
    create_scope_tables, delete_scoped_rows_and_reset_replay, refuse_if_bigname_runtime_sessions,
    refuse_if_out_of_scope_identity_dependencies,
};

pub const BASE_NORMALIZED_REDERIVE_CHAIN_ID: &str = "base-mainnet";
pub const BASE_NORMALIZED_REDERIVE_ADAPTER: &str = "ens_v1_unwrapped_authority";
pub const BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER: &str = "ens_v1_subregistry_discovery";
pub const BASE_NORMALIZED_REDERIVE_CURSOR_KIND: &str = "raw_fact_normalized_events";
pub const BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK: i64 = 17_571_485;
pub const BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK: i64 = 46_954_147;

const BASE_NORMALIZED_REDERIVE_ADVISORY_LOCK_KEY: &str =
    "bigname:indexer:drop-and-rederive-base-normalized-events:2026-07-03";
const EXPECTED_MANIFESTS: [(i64, &str); 4] = [
    (1, "basenames_base_registry"),
    (2, "basenames_base_registrar"),
    (4, "basenames_base_resolver"),
    (5, "basenames_base_primary"),
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseNormalizedRederiveManifest {
    pub manifest_id: i64,
    pub source_family: String,
    pub namespace: String,
    pub chain: String,
    pub rollout_status: String,
    pub file_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseNormalizedRederiveFamilyCensus {
    pub source_manifest_id: i64,
    pub source_family: String,
    pub row_count: i64,
    pub min_block_number: Option<i64>,
    pub max_block_number: Option<i64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BaseNormalizedRederiveCounts {
    pub normalized_events: i64,
    pub resources: i64,
    pub token_lineages: i64,
    pub name_surfaces: i64,
    pub surface_bindings: i64,
    pub name_current: i64,
    pub address_names_current: i64,
    pub children_current: i64,
    pub permissions_current: i64,
    pub record_inventory_current: i64,
    pub projection_normalized_event_changes: i64,
    pub replay_cursor_rows: i64,
    pub adapter_checkpoint_rows: i64,
    pub adapter_checkpoint_item_rows: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseNormalizedRederiveRawFactCompleteness {
    pub log_derived_event_count: i64,
    pub missing_log_derived_raw_fact_count: i64,
    pub boundary_event_count: i64,
    pub missing_boundary_lineage_count: i64,
    pub canonical_raw_log_min_block: Option<i64>,
    pub canonical_raw_log_max_block: Option<i64>,
}

impl BaseNormalizedRederiveRawFactCompleteness {
    pub fn is_complete_for_rerun(&self) -> bool {
        self.missing_log_derived_raw_fact_count == 0
            && self.missing_boundary_lineage_count == 0
            && self.canonical_raw_log_min_block == Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK)
            && self.canonical_raw_log_max_block
                == Some(BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseNormalizedRederivePlan {
    pub deployment_profile: String,
    pub manifests: Vec<BaseNormalizedRederiveManifest>,
    pub family_census: Vec<BaseNormalizedRederiveFamilyCensus>,
    pub counts: BaseNormalizedRederiveCounts,
    pub raw_fact_completeness: BaseNormalizedRederiveRawFactCompleteness,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseNormalizedRederiveExpectedCounts {
    pub counts: BaseNormalizedRederiveCounts,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseNormalizedRederiveExecutionOutcome {
    pub plan: BaseNormalizedRederivePlan,
    pub deleted: BaseNormalizedRederiveCounts,
}

pub async fn hold_base_normalized_rederive_runtime_shared_lock(
    pool: &PgPool,
    service: &str,
) -> Result<PoolConnection<Postgres>> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire runtime guard connection")?;
    sqlx::query("SELECT pg_advisory_lock_shared(hashtextextended($1::text, 0::bigint))")
        .bind(BASE_NORMALIZED_REDERIVE_ADVISORY_LOCK_KEY)
        .execute(&mut *connection)
        .await
        .context("failed to acquire Base normalized-event rederive runtime shared lock")?;
    info!(
        service,
        lock = BASE_NORMALIZED_REDERIVE_ADVISORY_LOCK_KEY,
        "runtime holds Base normalized-event rederive shared advisory lock"
    );
    Ok(connection)
}

pub async fn load_base_normalized_rederive_plan(
    pool: &PgPool,
    deployment_profile: &str,
) -> Result<BaseNormalizedRederivePlan> {
    validate_deployment_profile(deployment_profile)?;
    let manifests = load_manifest_confirmation(pool).await?;
    validate_manifest_confirmation(&manifests)?;
    let family_census = load_family_census(pool).await?;
    let counts = load_counts(pool, deployment_profile).await?;
    let raw_fact_completeness = load_raw_fact_completeness(pool).await?;
    Ok(BaseNormalizedRederivePlan {
        deployment_profile: deployment_profile.to_owned(),
        manifests,
        family_census,
        counts,
        raw_fact_completeness,
    })
}

pub async fn execute_base_normalized_rederive_drop(
    pool: &PgPool,
    deployment_profile: &str,
    expected_counts: BaseNormalizedRederiveExpectedCounts,
) -> Result<BaseNormalizedRederiveExecutionOutcome> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open Base normalized-event rederive transaction")?;
    let lock_acquired = sqlx::query_scalar::<_, bool>(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1::text, 0::bigint))",
    )
    .bind(BASE_NORMALIZED_REDERIVE_ADVISORY_LOCK_KEY)
    .fetch_one(&mut *transaction)
    .await
    .context("failed to acquire Base normalized-event rederive advisory lock")?;
    ensure!(
        lock_acquired,
        "Base normalized-event rederive advisory lock is already held"
    );
    refuse_if_bigname_runtime_sessions(pool).await?;

    create_scope_tables(&mut transaction).await?;
    let plan = load_plan_in_transaction(&mut transaction, deployment_profile).await?;
    ensure!(
        plan.raw_fact_completeness.is_complete_for_rerun(),
        "Base normalized-event rederive raw-fact completeness check failed: {:?}",
        plan.raw_fact_completeness
    );
    ensure!(
        expected_counts.counts == plan.counts,
        "Base normalized-event rederive count divergence: expected {:?}, found {:?}",
        expected_counts.counts,
        plan.counts
    );
    refuse_if_out_of_scope_identity_dependencies(&mut transaction).await?;

    let deleted = delete_scoped_rows_and_reset_replay(&mut transaction, deployment_profile).await?;
    transaction
        .commit()
        .await
        .context("failed to commit Base normalized-event rederive drop")?;
    Ok(BaseNormalizedRederiveExecutionOutcome { plan, deleted })
}

async fn load_plan_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<BaseNormalizedRederivePlan> {
    validate_deployment_profile(deployment_profile)?;
    let manifests = load_manifest_confirmation_from(transaction).await?;
    validate_manifest_confirmation(&manifests)?;
    let family_census = load_family_census_from(transaction).await?;
    let counts = load_counts_from(transaction, deployment_profile).await?;
    let raw_fact_completeness = load_raw_fact_completeness_from(transaction).await?;
    Ok(BaseNormalizedRederivePlan {
        deployment_profile: deployment_profile.to_owned(),
        manifests,
        family_census,
        counts,
        raw_fact_completeness,
    })
}

async fn load_manifest_confirmation(pool: &PgPool) -> Result<Vec<BaseNormalizedRederiveManifest>> {
    let rows = sqlx::query(
        r#"
        SELECT manifest_id, source_family, namespace, chain, rollout_status::TEXT, file_path
        FROM manifest_versions
        WHERE manifest_id = ANY($1::BIGINT[])
        ORDER BY manifest_id
        "#,
    )
    .bind(expected_manifest_ids())
    .fetch_all(pool)
    .await
    .context("failed to load Base normalized-event rederive manifest confirmation")?;
    manifest_rows(rows)
}

async fn load_manifest_confirmation_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<BaseNormalizedRederiveManifest>> {
    let rows = sqlx::query(
        r#"
        SELECT manifest_id, source_family, namespace, chain, rollout_status::TEXT, file_path
        FROM manifest_versions
        WHERE manifest_id = ANY($1::BIGINT[])
        ORDER BY manifest_id
        "#,
    )
    .bind(expected_manifest_ids())
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load Base normalized-event rederive manifest confirmation")?;
    manifest_rows(rows)
}

fn manifest_rows(rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<BaseNormalizedRederiveManifest>> {
    rows.into_iter()
        .map(|row| {
            Ok(BaseNormalizedRederiveManifest {
                manifest_id: row.try_get("manifest_id")?,
                source_family: row.try_get("source_family")?,
                namespace: row.try_get("namespace")?,
                chain: row.try_get("chain")?,
                rollout_status: row.try_get("rollout_status")?,
                file_path: row.try_get("file_path")?,
            })
        })
        .collect()
}

fn validate_manifest_confirmation(manifests: &[BaseNormalizedRederiveManifest]) -> Result<()> {
    let expected = EXPECTED_MANIFESTS
        .into_iter()
        .collect::<BTreeMap<i64, &'static str>>();
    ensure!(
        manifests.len() == expected.len(),
        "Base normalized-event rederive expected manifest IDs {:?}, found {:?}",
        expected.keys().collect::<Vec<_>>(),
        manifests.iter().map(|m| m.manifest_id).collect::<Vec<_>>()
    );
    for manifest in manifests {
        let expected_family = expected
            .get(&manifest.manifest_id)
            .with_context(|| format!("unexpected source_manifest_id {}", manifest.manifest_id))?;
        ensure!(
            manifest.source_family == *expected_family
                && manifest.namespace == "basenames"
                && manifest.chain == BASE_NORMALIZED_REDERIVE_CHAIN_ID,
            "Base normalized-event rederive manifest {} is {}/{}/{}; expected basenames/{}/{}",
            manifest.manifest_id,
            manifest.namespace,
            manifest.source_family,
            manifest.chain,
            expected_family,
            BASE_NORMALIZED_REDERIVE_CHAIN_ID
        );
    }
    Ok(())
}

pub(super) fn expected_manifest_ids() -> Vec<i64> {
    EXPECTED_MANIFESTS.iter().map(|(id, _)| *id).collect()
}

pub(super) fn checkpoint_adapters() -> Vec<String> {
    [
        BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER,
        BASE_NORMALIZED_REDERIVE_ADAPTER,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn validate_deployment_profile(deployment_profile: &str) -> Result<()> {
    if deployment_profile.trim().is_empty() {
        bail!("Base normalized-event rederive deployment profile must not be empty");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
