use anyhow::{Context, Result, bail, ensure};
use sqlx::{PgPool, Postgres, pool::PoolConnection};
use tracing::info;

mod counts;
mod execution;
mod guards;
mod profile;

use counts::{
    load_counts, load_counts_from, load_cursor_census, load_cursor_census_from,
    load_derivation_kind_census, load_derivation_kind_census_from, load_max_affected_block,
    load_max_affected_block_from, load_raw_fact_completeness, load_raw_fact_completeness_from,
    load_reset_replay_cursor_target_block, load_reset_replay_cursor_target_block_from,
};
use execution::{
    create_scope_tables, delete_scoped_rows_and_reset_replay, refuse_if_bigname_runtime_sessions,
    refuse_if_out_of_scope_identity_dependencies,
};
use guards::{
    ensure_delete_scope_replay_active, ensure_delete_scope_replay_active_from,
    ensure_no_affected_rows_above_raw_log_head, ensure_no_affected_rows_above_raw_log_head_from,
};
use profile::{
    validate_base_deployment_profile_owns_chain, validate_base_deployment_profile_owns_chain_from,
    validate_deployment_profile,
};

pub const BASE_NORMALIZED_REDERIVE_CHAIN_ID: &str = "base-mainnet";
pub const BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_ADAPTER: &str = "ens_v1_reverse_claim";
pub const BASE_NORMALIZED_REDERIVE_ADAPTER: &str = "ens_v1_unwrapped_authority";
pub const BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER: &str = "ens_v1_subregistry_discovery";
pub const BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_DERIVATION_KIND: &str = "ens_v1_reverse_claim";
pub const BASE_NORMALIZED_REDERIVE_SUBREGISTRY_CHANGED_DERIVATION_KIND: &str =
    "ens_v1_subregistry_changed";
pub const BASE_NORMALIZED_REDERIVE_REGISTRY_RESOLVER_CHANGED_DERIVATION_KIND: &str =
    "ens_v1_registry_resolver_changed";
pub const BASE_NORMALIZED_REDERIVE_UNWRAPPED_AUTHORITY_DERIVATION_KIND: &str =
    "ens_v1_unwrapped_authority";
pub const BASE_NORMALIZED_REDERIVE_CURSOR_KIND: &str = "raw_fact_normalized_events";
pub const BASE_NORMALIZED_REDERIVE_BACKLOG_CURSOR_KIND: &str = "post_replay_live_adapter_backlog";
pub const BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK: i64 = 17_571_485;

const BASE_NORMALIZED_REDERIVE_ADVISORY_LOCK_KEY: &str =
    "bigname:indexer:drop-and-rederive-base-normalized-events:2026-07-03";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseNormalizedRederiveScopeRule {
    pub adapter: &'static str,
    pub derivation_kinds: &'static [&'static str],
    pub source_families: &'static [&'static str],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseNormalizedRederiveDerivationKindCensus {
    pub derivation_kind: String,
    pub source_family: String,
    pub row_count: i64,
    pub min_block_number: Option<i64>,
    pub max_block_number: Option<i64>,
    pub rederivable: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BaseNormalizedRederiveCursorCensus {
    pub raw_fact_replay_cursor_rows: i64,
    pub post_replay_live_adapter_backlog_cursor_rows: i64,
}

impl BaseNormalizedRederiveCursorCensus {
    pub fn total_cursor_rows(&self) -> i64 {
        self.raw_fact_replay_cursor_rows + self.post_replay_live_adapter_backlog_cursor_rows
    }
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
    pub current_projection_replay_status: i64,
    pub replay_cursor_rows: i64,
    pub adapter_checkpoint_rows: i64,
    pub adapter_checkpoint_item_rows: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseNormalizedRederiveRawFactCompleteness {
    pub replay_target_block: i64,
    pub log_derived_event_count: i64,
    pub missing_log_derived_raw_fact_count: i64,
    pub boundary_event_count: i64,
    pub missing_boundary_lineage_count: i64,
    pub canonical_raw_log_min_block: Option<i64>,
    pub canonical_raw_log_max_block: Option<i64>,
    pub canonical_raw_log_head_block: Option<i64>,
}

impl BaseNormalizedRederiveRawFactCompleteness {
    pub fn is_complete_for_rerun(&self) -> bool {
        self.missing_log_derived_raw_fact_count == 0
            && self.missing_boundary_lineage_count == 0
            && self.canonical_raw_log_min_block == Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK)
            && self.canonical_raw_log_max_block == Some(self.replay_target_block)
            && self
                .canonical_raw_log_head_block
                .is_some_and(|head| head >= self.replay_target_block)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseNormalizedRederivePlan {
    pub deployment_profile: String,
    pub replay_target_block: i64,
    pub max_affected_block: Option<i64>,
    pub replay_target_floor_block: Option<i64>,
    pub derivation_kind_census: Vec<BaseNormalizedRederiveDerivationKindCensus>,
    pub cursor_census: BaseNormalizedRederiveCursorCensus,
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
    requested_replay_target_block: Option<i64>,
) -> Result<BaseNormalizedRederivePlan> {
    validate_deployment_profile(deployment_profile)?;
    validate_base_deployment_profile_owns_chain(pool, deployment_profile).await?;
    let (replay_target_block, max_affected_block, replay_target_floor_block) =
        resolve_replay_target_block(pool, deployment_profile, requested_replay_target_block)
            .await
            .context("failed to resolve Base normalized-event rederive replay target")?;
    ensure_delete_scope_replay_active(pool, replay_target_block).await?;
    let derivation_kind_census = load_derivation_kind_census(pool, replay_target_block).await?;
    let cursor_census = load_cursor_census(pool, deployment_profile).await?;
    let counts = load_counts(pool, deployment_profile, replay_target_block).await?;
    let raw_fact_completeness = load_raw_fact_completeness(pool, replay_target_block).await?;
    Ok(BaseNormalizedRederivePlan {
        deployment_profile: deployment_profile.to_owned(),
        replay_target_block,
        max_affected_block,
        replay_target_floor_block,
        derivation_kind_census,
        cursor_census,
        counts,
        raw_fact_completeness,
    })
}

pub async fn execute_base_normalized_rederive_drop(
    pool: &PgPool,
    deployment_profile: &str,
    requested_replay_target_block: Option<i64>,
    expected_counts: BaseNormalizedRederiveExpectedCounts,
) -> Result<BaseNormalizedRederiveExecutionOutcome> {
    ensure!(
        requested_replay_target_block.is_some(),
        "Base normalized-event rederive execute requires reviewed replay target block"
    );
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
    refuse_if_bigname_runtime_sessions(&mut transaction).await?;
    validate_base_deployment_profile_owns_chain_from(&mut transaction, deployment_profile).await?;

    let (replay_target_block, max_affected_block, replay_target_floor_block) =
        resolve_replay_target_block_from(
            &mut transaction,
            deployment_profile,
            requested_replay_target_block,
        )
        .await
        .context("failed to resolve Base normalized-event rederive replay target")?;
    ensure_delete_scope_replay_active_from(&mut transaction, replay_target_block).await?;
    create_scope_tables(&mut transaction, replay_target_block).await?;
    let plan = load_plan_in_transaction(
        &mut transaction,
        deployment_profile,
        replay_target_block,
        max_affected_block,
        replay_target_floor_block,
    )
    .await?;
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

    let deleted = delete_scoped_rows_and_reset_replay(
        &mut transaction,
        deployment_profile,
        replay_target_block,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("failed to commit Base normalized-event rederive drop")?;
    Ok(BaseNormalizedRederiveExecutionOutcome { plan, deleted })
}

async fn load_plan_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
    replay_target_block: i64,
    max_affected_block: Option<i64>,
    replay_target_floor_block: Option<i64>,
) -> Result<BaseNormalizedRederivePlan> {
    validate_deployment_profile(deployment_profile)?;
    validate_base_deployment_profile_owns_chain_from(transaction, deployment_profile).await?;
    let derivation_kind_census =
        load_derivation_kind_census_from(transaction, replay_target_block).await?;
    let cursor_census = load_cursor_census_from(transaction, deployment_profile).await?;
    let counts = load_counts_from(transaction, deployment_profile, replay_target_block).await?;
    let raw_fact_completeness =
        load_raw_fact_completeness_from(transaction, replay_target_block).await?;
    Ok(BaseNormalizedRederivePlan {
        deployment_profile: deployment_profile.to_owned(),
        replay_target_block,
        max_affected_block,
        replay_target_floor_block,
        derivation_kind_census,
        cursor_census,
        counts,
        raw_fact_completeness,
    })
}

async fn resolve_replay_target_block(
    pool: &PgPool,
    deployment_profile: &str,
    requested_replay_target_block: Option<i64>,
) -> Result<(i64, Option<i64>, Option<i64>)> {
    let head = validate_canonical_raw_log_head(load_canonical_raw_log_head(pool).await?)?;
    ensure_no_affected_rows_above_raw_log_head(pool, head).await?;
    let max_affected_block = load_max_affected_block(pool, head).await?;
    let reset_replay_cursor_target_block =
        load_pending_reset_replay_cursor_target_block(pool, deployment_profile).await?;
    let target = validate_replay_target_block(
        head,
        max_affected_block,
        reset_replay_cursor_target_block,
        requested_replay_target_block,
    )?;
    Ok((
        target,
        max_affected_block,
        target_floor_block(max_affected_block, reset_replay_cursor_target_block),
    ))
}

async fn resolve_replay_target_block_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
    requested_replay_target_block: Option<i64>,
) -> Result<(i64, Option<i64>, Option<i64>)> {
    let head =
        validate_canonical_raw_log_head(load_canonical_raw_log_head_from(transaction).await?)?;
    ensure_no_affected_rows_above_raw_log_head_from(transaction, head).await?;
    let max_affected_block = load_max_affected_block_from(transaction, head).await?;
    let reset_replay_cursor_target_block =
        load_pending_reset_replay_cursor_target_block_from(transaction, deployment_profile).await?;
    let target = validate_replay_target_block(
        head,
        max_affected_block,
        reset_replay_cursor_target_block,
        requested_replay_target_block,
    )?;
    Ok((
        target,
        max_affected_block,
        target_floor_block(max_affected_block, reset_replay_cursor_target_block),
    ))
}

async fn load_canonical_raw_log_head(pool: &PgPool) -> Result<Option<i64>> {
    sqlx::query_scalar(canonical_raw_log_head_sql())
        .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
        .fetch_one(pool)
        .await
        .context("failed to load Base canonical raw-log head")
}

async fn load_canonical_raw_log_head_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<i64>> {
    sqlx::query_scalar(canonical_raw_log_head_sql())
        .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
        .fetch_one(&mut **transaction)
        .await
        .context("failed to load Base canonical raw-log head")
}

fn canonical_raw_log_head_sql() -> &'static str {
    r#"
    SELECT MAX(raw_logs.block_number)::BIGINT
    FROM raw_logs
    JOIN chain_lineage lineage
      ON lineage.chain_id = raw_logs.chain_id
     AND lineage.block_hash = raw_logs.block_hash
    WHERE raw_logs.chain_id = $1
      AND raw_logs.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
      AND lineage.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
    "#
}

fn validate_canonical_raw_log_head(canonical_raw_log_head: Option<i64>) -> Result<i64> {
    let Some(head) = canonical_raw_log_head else {
        bail!(
            "Base normalized-event rederive cannot resolve replay target: no canonical raw logs for {}",
            BASE_NORMALIZED_REDERIVE_CHAIN_ID
        );
    };
    ensure!(
        head >= BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
        "Base normalized-event rederive canonical raw-log head {head} is before closure boundary {}",
        BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK
    );
    Ok(head)
}

fn validate_replay_target_block(
    canonical_raw_log_head: i64,
    max_affected_block: Option<i64>,
    reset_replay_cursor_target_block: Option<i64>,
    requested_replay_target_block: Option<i64>,
) -> Result<i64> {
    let target = requested_replay_target_block.unwrap_or(canonical_raw_log_head);
    ensure!(
        target >= BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
        "Base normalized-event rederive requested replay target block {target} is before closure boundary {}",
        BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK
    );
    ensure!(
        target <= canonical_raw_log_head,
        "Base normalized-event rederive requested replay target block {target} must not exceed canonical raw-log head {canonical_raw_log_head}"
    );
    if let Some(max_affected) = max_affected_block {
        ensure!(
            target >= max_affected,
            "Base normalized-event rederive requested replay target block {target} is before max affected normalized-event block {max_affected}"
        );
    }
    if let Some(reset_target) = reset_replay_cursor_target_block {
        ensure!(
            target >= reset_target,
            "Base normalized-event rederive requested replay target block {target} is before max required replay target block {reset_target}"
        );
    }
    Ok(target)
}

async fn load_pending_reset_replay_cursor_target_block(
    pool: &PgPool,
    deployment_profile: &str,
) -> Result<Option<i64>> {
    load_reset_replay_cursor_target_block(pool, deployment_profile).await
}

async fn load_pending_reset_replay_cursor_target_block_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<Option<i64>> {
    load_reset_replay_cursor_target_block_from(transaction, deployment_profile).await
}

fn target_floor_block(
    max_affected_block: Option<i64>,
    reset_replay_cursor_target_block: Option<i64>,
) -> Option<i64> {
    match (max_affected_block, reset_replay_cursor_target_block) {
        (Some(max_affected), Some(reset_target)) => Some(max_affected.max(reset_target)),
        (Some(max_affected), None) => Some(max_affected),
        (None, Some(reset_target)) => Some(reset_target),
        (None, None) => None,
    }
}

pub fn base_normalized_rederive_scope_rules() -> &'static [BaseNormalizedRederiveScopeRule] {
    &[
        BaseNormalizedRederiveScopeRule {
            adapter: BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_ADAPTER,
            derivation_kinds: &[BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_DERIVATION_KIND],
            source_families: &["ens_v1_reverse_l1", "basenames_base_primary"],
        },
        BaseNormalizedRederiveScopeRule {
            adapter: BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER,
            derivation_kinds: &[
                BASE_NORMALIZED_REDERIVE_REGISTRY_RESOLVER_CHANGED_DERIVATION_KIND,
                BASE_NORMALIZED_REDERIVE_SUBREGISTRY_CHANGED_DERIVATION_KIND,
            ],
            source_families: &["ens_v1_registry_l1", "basenames_base_registry"],
        },
        BaseNormalizedRederiveScopeRule {
            adapter: BASE_NORMALIZED_REDERIVE_ADAPTER,
            derivation_kinds: &[BASE_NORMALIZED_REDERIVE_UNWRAPPED_AUTHORITY_DERIVATION_KIND],
            source_families: &[
                "ens_v1_registrar_l1",
                "ens_v1_registry_l1",
                "ens_v1_resolver_l1",
                "ens_v1_wrapper_l1",
                "basenames_base_registrar",
                "basenames_base_registry",
                "basenames_base_resolver",
            ],
        },
    ]
}

pub(super) fn reverse_claim_derivation_kind() -> String {
    BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_DERIVATION_KIND.to_owned()
}

pub(super) fn reverse_claim_source_families() -> Vec<String> {
    vec![
        "ens_v1_reverse_l1".to_owned(),
        "basenames_base_primary".to_owned(),
    ]
}

pub(super) fn subregistry_derivation_kinds() -> Vec<String> {
    vec![
        BASE_NORMALIZED_REDERIVE_REGISTRY_RESOLVER_CHANGED_DERIVATION_KIND.to_owned(),
        BASE_NORMALIZED_REDERIVE_SUBREGISTRY_CHANGED_DERIVATION_KIND.to_owned(),
    ]
}

pub(super) fn subregistry_source_families() -> Vec<String> {
    vec![
        "ens_v1_registry_l1".to_owned(),
        "basenames_base_registry".to_owned(),
    ]
}

pub(super) fn unwrapped_authority_derivation_kind() -> String {
    BASE_NORMALIZED_REDERIVE_UNWRAPPED_AUTHORITY_DERIVATION_KIND.to_owned()
}

pub(super) fn unwrapped_authority_source_families() -> Vec<String> {
    vec![
        "ens_v1_registrar_l1".to_owned(),
        "ens_v1_registry_l1".to_owned(),
        "ens_v1_resolver_l1".to_owned(),
        "ens_v1_wrapper_l1".to_owned(),
        "basenames_base_registrar".to_owned(),
        "basenames_base_registry".to_owned(),
        "basenames_base_resolver".to_owned(),
    ]
}

pub(super) fn cursor_kinds() -> Vec<String> {
    [
        BASE_NORMALIZED_REDERIVE_CURSOR_KIND,
        BASE_NORMALIZED_REDERIVE_BACKLOG_CURSOR_KIND,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

pub(super) fn checkpoint_adapters() -> Vec<String> {
    [
        BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_ADAPTER,
        BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER,
        BASE_NORMALIZED_REDERIVE_ADAPTER,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

pub(super) fn current_projection_replay_status_projections() -> Vec<String> {
    [
        "address_names_current",
        "children_current",
        "name_current",
        "permissions_current",
        "primary_names_current",
        "record_inventory_current",
        "resolver_current",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[cfg(test)]
mod tests;
