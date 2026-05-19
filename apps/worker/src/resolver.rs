use anyhow::{Result, bail};
use bigname_storage::{
    ResolverCurrentRow, clear_resolver_current, delete_resolver_current,
    upsert_resolver_current_rows,
};
use sqlx::PgPool;
use tokio::task::JoinSet;

mod profile;
mod state_helpers;
mod summary_json;
mod target_loading;

use profile::ResolverProfileGate;
use summary_json::build_resolver_current_row;
use target_loading::{ResolverTarget, load_target_resolvers, normalize_resolver_address};

#[cfg(test)]
use bigname_storage::{CanonicalityState, SurfaceBindingKind};
#[cfg(test)]
use serde_json::{Value, json};
#[cfg(test)]
use sqlx::{Row, types::time::OffsetDateTime};
#[cfg(test)]
use uuid::Uuid;

const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
const EVENT_KIND_ALIAS_CHANGED: &str = "AliasChanged";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
#[cfg(test)]
const BASENAMES_NAMESPACE: &str = "basenames";
const SOURCE_FAMILY_ENS_V1_REGISTRY_L1: &str = "ens_v1_registry_l1";
const SOURCE_FAMILY_ENS_V1_RESOLVER_L1: &str = "ens_v1_resolver_l1";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";
const ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE: &str = "public_resolver_compatible";
const BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE: &str = "l2_resolver_compatible";
const RESOLVER_CURRENT_DERIVATION_KIND: &str = "resolver_current_rebuild";
const RESOLVER_CURRENT_ENUMERATION_BASIS: &str = "resolver_overview";
const RESOLVER_CURRENT_REBUILD_BATCH_SIZE: usize = 1_000;
const RESOLVER_CURRENT_REBUILD_CONCURRENCY: usize = 1;
const RESOLVER_CURRENT_REBUILD_LOG_INTERVAL: usize = 100;
const RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON: &str =
    "resolver_binding_enumeration_not_projected";
const RESOLVER_PROFILE_STATUS_PENDING: &str = "pending";
const RESOLVER_PROFILE_STATUS_SUPPORTED: &str = "supported";
const RESOLVER_PROFILE_FACT_FAMILY_AUTHORIZATION: &str = "resolver_authorization";
const RESOLVER_PROFILE_FACT_FAMILY_RECORD: &str = "resolver_record";
const RESOLVER_PROFILE_FACT_FAMILY_RECORD_VERSION: &str = "resolver_record_version";
const RESOLVER_FAMILY_PENDING_REASON: &str = "resolver_family_pending";
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolverCurrentRebuildSummary {
    pub requested_resolver_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

pub async fn rebuild_resolver_current(
    pool: &PgPool,
    chain_id: Option<&str>,
    resolver_address: Option<&str>,
) -> Result<ResolverCurrentRebuildSummary> {
    match (chain_id, resolver_address) {
        (Some(chain_id), Some(resolver_address)) => {
            rebuild_one_resolver(pool, chain_id, resolver_address).await
        }
        (None, None) => rebuild_all_resolvers(pool).await,
        _ => bail!(
            "resolver_current rebuild requires both chain_id and resolver_address when targeting one resolver"
        ),
    }
}

async fn rebuild_all_resolvers(pool: &PgPool) -> Result<ResolverCurrentRebuildSummary> {
    let profile_gate = ResolverProfileGate::load(pool).await?;
    let targets = load_target_resolvers(pool).await?;
    let requested_resolver_count = targets.len();
    tracing::info!(
        projection = "resolver_current",
        requested_resolver_count,
        rebuild_concurrency = RESOLVER_CURRENT_REBUILD_CONCURRENCY,
        "resolver_current rebuild targets loaded"
    );
    let deleted_row_count = clear_resolver_current(pool).await?;

    let mut rows = Vec::with_capacity(RESOLVER_CURRENT_REBUILD_BATCH_SIZE);
    let mut completed_resolver_count = 0usize;
    let mut upserted_row_count = 0usize;
    let mut targets = targets.into_iter();
    let mut tasks = JoinSet::new();

    for _ in 0..RESOLVER_CURRENT_REBUILD_CONCURRENCY {
        let Some(target) = targets.next() else {
            break;
        };
        spawn_resolver_rebuild_task(&mut tasks, pool, profile_gate.clone(), target);
    }

    while let Some(result) = tasks.join_next().await {
        completed_resolver_count += 1;
        if let Some(row) = result?? {
            rows.push(row);
        }

        if rows.len() >= RESOLVER_CURRENT_REBUILD_BATCH_SIZE {
            upserted_row_count += upsert_resolver_current_rows(pool, &rows).await?.len();
            rows.clear();
        }

        if completed_resolver_count % RESOLVER_CURRENT_REBUILD_LOG_INTERVAL == 0 {
            tracing::info!(
                projection = "resolver_current",
                requested_resolver_count,
                completed_resolver_count,
                upserted_row_count,
                "resolver_current rebuild resolvers processed"
            );
        }

        if let Some(target) = targets.next() {
            spawn_resolver_rebuild_task(&mut tasks, pool, profile_gate.clone(), target);
        }
    }

    if !rows.is_empty() {
        upserted_row_count += upsert_resolver_current_rows(pool, &rows).await?.len();
    }

    Ok(ResolverCurrentRebuildSummary {
        requested_resolver_count,
        upserted_row_count,
        deleted_row_count,
    })
}

fn spawn_resolver_rebuild_task(
    tasks: &mut JoinSet<Result<Option<ResolverCurrentRow>>>,
    pool: &PgPool,
    profile_gate: ResolverProfileGate,
    target: ResolverTarget,
) {
    let pool = pool.clone();
    tasks.spawn(async move { build_resolver_current_row(&pool, &profile_gate, &target).await });
}

async fn rebuild_one_resolver(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<ResolverCurrentRebuildSummary> {
    let target = ResolverTarget {
        chain_id: chain_id.to_owned(),
        resolver_address: normalize_resolver_address(resolver_address),
        profile_source_family: None,
        enumerate_bindings: true,
    };
    let profile_gate = ResolverProfileGate::load_for_target(pool, &target).await?;
    let Some(row) = build_resolver_current_row(pool, &profile_gate, &target).await? else {
        let deleted_row_count =
            delete_resolver_current(pool, &target.chain_id, &target.resolver_address).await?;
        return Ok(ResolverCurrentRebuildSummary {
            requested_resolver_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    };

    let upserted_row_count = upsert_resolver_current_rows(pool, &[row]).await?.len();
    Ok(ResolverCurrentRebuildSummary {
        requested_resolver_count: 1,
        upserted_row_count,
        deleted_row_count: 0,
    })
}

#[cfg(test)]
mod tests;
