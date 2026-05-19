use anyhow::{Context, Result, bail};
use bigname_manifests::{
    WatchedSourceSelector, load_manifest_declared_watched_source_selector_plan,
    load_watched_chain_plan, load_watched_contracts_by_addresses,
};

use super::scoped::replay_source_scope_from_requested_scope;
use crate::{
    ens_v1_resolver::{SOURCE_FAMILY_ENS_V1_RESOLVER_L1, generic_resolver_record_topic0s},
    reconciliation::types::{
        RawFactNormalizedEventReplayRequest, RawFactNormalizedEventReplaySelection,
        RawFactNormalizedEventReplaySourceScope,
    },
    source_scope::SourceScope,
};

pub(super) async fn ensure_replay_matches_deployment_profile_scope(
    pool: &sqlx::PgPool,
    request: &RawFactNormalizedEventReplayRequest,
    range: Option<(i64, i64)>,
) -> Result<()> {
    let active_profile = infer_active_manifest_deployment_profile(pool).await?;
    if request.deployment_profile != active_profile {
        bail!(
            "deployment_profile {} does not match active manifest/discovery corpus profile {active_profile}",
            request.deployment_profile
        );
    }

    if let Some((from_block, to_block)) = range {
        load_manifest_declared_watched_source_selector_plan(
            pool,
            &request.chain,
            WatchedSourceSelector::WholeActiveWatchedChain,
            from_block,
            to_block,
        )
        .await
        .with_context(|| {
            format!(
                "deployment_profile {} has no active watched manifest/discovery route for chain {} over replay range {}..={}",
                request.deployment_profile, request.chain, from_block, to_block
            )
        })?;
    } else {
        ensure_active_watched_chain_for_replay_profile(
            pool,
            &request.deployment_profile,
            &request.chain,
        )
        .await?;
    }

    Ok(())
}

pub(super) async fn load_replay_adapter_source_scope(
    pool: &sqlx::PgPool,
    request: &RawFactNormalizedEventReplayRequest,
    range: Option<(i64, i64)>,
    address_targets: &[(String, String)],
) -> Result<Vec<(String, String, i64, i64)>> {
    let Some((from_block, to_block)) = range else {
        return Ok(Vec::new());
    };
    if let Some(source_scope) = replay_selection_source_scope(&request.selection) {
        return replay_source_scope_from_requested_scope(source_scope, from_block, to_block);
    }
    if address_targets.is_empty() {
        return Ok(Vec::new());
    }

    let watched_contracts = load_watched_contracts_by_addresses(pool, &address_targets)
        .await
        .with_context(|| {
            format!(
                "failed to load replay source scope targets for chain {} range {}..={}",
                request.chain, from_block, to_block
            )
        })?;
    let include_generic_resolver_scope =
        active_ens_v1_resolver_manifest_exists(pool, &request.chain).await?
            && selected_replay_includes_generic_resolver_logs(
                pool,
                &request.chain,
                &request.selection,
            )
            .await?;
    let source_scope = SourceScope::from_watched_contracts(
        &watched_contracts,
        &request.chain,
        from_block,
        to_block,
        include_generic_resolver_scope,
    );

    Ok(source_scope.adapter_sync_scope())
}

async fn selected_replay_includes_generic_resolver_logs(
    pool: &sqlx::PgPool,
    chain: &str,
    selection: &RawFactNormalizedEventReplaySelection,
) -> Result<bool> {
    let topic0s = generic_resolver_record_topic0s()
        .into_iter()
        .map(|topic0| topic0.to_ascii_lowercase())
        .collect::<Vec<_>>();
    match selection {
        RawFactNormalizedEventReplaySelection::BlockRange {
            from_block,
            to_block,
        } => {
            selected_range_includes_generic_resolver_logs(
                pool,
                chain,
                *from_block,
                *to_block,
                &topic0s,
            )
            .await
        }
        RawFactNormalizedEventReplaySelection::BlockHashes(block_hashes) => {
            selected_block_hashes_include_generic_resolver_logs(pool, chain, block_hashes, &topic0s)
                .await
        }
        RawFactNormalizedEventReplaySelection::ScopedBlockRange { .. } => Ok(false),
    }
}

async fn selected_range_includes_generic_resolver_logs(
    pool: &sqlx::PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
    topic0s: &[String],
) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM raw_logs AS logs
            JOIN chain_lineage AS lineage
              ON lineage.chain_id = logs.chain_id
             AND lineage.block_hash = logs.block_hash
            WHERE logs.chain_id = $1
              AND logs.block_number >= $2
              AND logs.block_number <= $3
              AND LOWER(logs.topics[1]) = ANY($4::TEXT[])
              AND lineage.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND logs.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        )
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .bind(topic0s)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to check generic ENSv1 resolver replay logs for chain {chain} range {from_block}..={to_block}"
        )
    })
}

async fn selected_block_hashes_include_generic_resolver_logs(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    topic0s: &[String],
) -> Result<bool> {
    if block_hashes.is_empty() {
        return Ok(false);
    }

    sqlx::query_scalar::<_, bool>(
        r#"
        WITH selected_blocks AS (
            SELECT DISTINCT block_hash
            FROM UNNEST($2::TEXT[]) AS selected(block_hash)
        )
        SELECT EXISTS (
            SELECT 1
            FROM selected_blocks selected
            JOIN raw_logs AS logs
              ON logs.chain_id = $1
             AND logs.block_hash = selected.block_hash
            JOIN chain_lineage AS lineage
              ON lineage.chain_id = logs.chain_id
             AND lineage.block_hash = logs.block_hash
            WHERE LOWER(logs.topics[1]) = ANY($3::TEXT[])
              AND lineage.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND logs.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        )
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .bind(topic0s)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to check generic ENSv1 resolver replay logs for chain {chain} across {} blocks",
            block_hashes.len()
        )
    })
}

async fn active_ens_v1_resolver_manifest_exists(pool: &sqlx::PgPool, chain: &str) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM manifest_versions
            WHERE chain = $1
              AND source_family = $2
              AND rollout_status = 'active'::manifest_rollout_status
        )
        "#,
    )
    .bind(chain)
    .bind(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to check active ENSv1 resolver manifest for replay on chain {chain}")
    })
}

fn replay_selection_source_scope(
    selection: &RawFactNormalizedEventReplaySelection,
) -> Option<&[RawFactNormalizedEventReplaySourceScope]> {
    match selection {
        RawFactNormalizedEventReplaySelection::ScopedBlockRange { source_scope, .. } => {
            Some(source_scope)
        }
        RawFactNormalizedEventReplaySelection::BlockRange { .. }
        | RawFactNormalizedEventReplaySelection::BlockHashes(_) => None,
    }
}

async fn ensure_active_watched_chain_for_replay_profile(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<()> {
    let watched_plan = load_watched_chain_plan(pool).await.with_context(|| {
        format!(
            "failed to verify deployment_profile {deployment_profile} active watched chain route for chain {chain}"
        )
    })?;
    if !watched_plan.iter().any(|plan| plan.chain == chain) {
        bail!(
            "deployment_profile {deployment_profile} has no active watched manifest/discovery route for chain {chain}"
        );
    }

    Ok(())
}

async fn infer_active_manifest_deployment_profile(pool: &sqlx::PgPool) -> Result<String> {
    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT DISTINCT chain, deployment_epoch
        FROM manifest_versions
        WHERE rollout_status = 'active'
        ORDER BY chain, deployment_epoch
        "#,
    )
    .fetch_all(pool)
    .await
    .context(
        "failed to load active manifest/discovery corpus for replay deployment_profile enforcement",
    )?;

    if rows.is_empty() {
        bail!("deployment_profile cannot be enforced because no active manifests are loaded");
    }

    let all_mainnet = rows.iter().all(|(chain, _)| chain.ends_with("-mainnet"));
    if all_mainnet {
        return Ok("mainnet".to_owned());
    }

    let all_sepolia_dev = rows.iter().all(|(chain, deployment_epoch)| {
        chain.ends_with("-sepolia") && deployment_epoch.ends_with("_sepolia_dev")
    });
    if all_sepolia_dev {
        return Ok("sepolia-dev".to_owned());
    }

    bail!(
        "deployment_profile cannot be enforced because the active manifest/discovery corpus does not match a supported deployment profile"
    );
}
