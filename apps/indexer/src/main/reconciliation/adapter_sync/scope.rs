use anyhow::{Context, Result, bail};
use bigname_manifests::{
    WatchedSourceSelector, WatchedSourceSelectorPlan, load_watched_source_selector_plan,
};

const SOURCE_FAMILY_ENS_V2_ROOT_L1: &str = "ens_v2_root_l1";
const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
const SOURCE_FAMILY_ENS_V2_REGISTRAR_L1: &str = "ens_v2_registrar_l1";
const SOURCE_FAMILY_ENS_V2_RESOLVER_L1: &str = "ens_v2_resolver_l1";
const SOURCE_FAMILY_ENS_V1_REVERSE_L1: &str = "ens_v1_reverse_l1";
const SOURCE_FAMILY_ENS_V1_REGISTRAR_L1: &str = "ens_v1_registrar_l1";
const SOURCE_FAMILY_ENS_V1_REGISTRY_L1: &str = "ens_v1_registry_l1";
const SOURCE_FAMILY_ENS_V1_RESOLVER_L1: &str = "ens_v1_resolver_l1";
const SOURCE_FAMILY_ENS_V1_WRAPPER_L1: &str = "ens_v1_wrapper_l1";
const SOURCE_FAMILY_BASENAMES_BASE_PRIMARY: &str = "basenames_base_primary";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR: &str = "basenames_base_registrar";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";

pub(super) async fn load_live_adapter_source_scope(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<Vec<(String, String, i64, i64)>> {
    if block_hashes.is_empty() {
        return Ok(Vec::new());
    }

    let mut unique_block_hashes = block_hashes.to_vec();
    unique_block_hashes.sort();
    unique_block_hashes.dedup();

    let (from_block, to_block, stored_count): (Option<i64>, Option<i64>, i64) = sqlx::query_as(
        r#"
        SELECT MIN(block_number), MAX(block_number), COUNT(*)::BIGINT
        FROM raw_blocks
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
        "#,
    )
    .bind(chain)
    .bind(&unique_block_hashes)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load raw-block range for chain {chain} adapter sync"))?;

    let stored_count = usize::try_from(stored_count)
        .context("raw-block adapter sync range count does not fit in usize")?;
    if stored_count != unique_block_hashes.len() {
        bail!(
            "stored raw block count {stored_count} does not match adapter sync block-hash count {} for chain {chain}",
            unique_block_hashes.len()
        );
    }
    let (Some(from_block), Some(to_block)) = (from_block, to_block) else {
        bail!("adapter sync block range is empty for non-empty block-hash input on chain {chain}");
    };

    let source_plan = load_watched_source_selector_plan(
        pool,
        chain,
        WatchedSourceSelector::WholeActiveWatchedChain,
        from_block,
        to_block,
    )
    .await
    .with_context(|| {
        format!(
            "failed to load source-scoped adapter sync plan for chain {chain} range {from_block}..={to_block}"
        )
    })?;
    Ok(selected_target_sync_scope(&source_plan))
}

pub(crate) fn selected_target_sync_scope(
    source_plan: &WatchedSourceSelectorPlan,
) -> Vec<(String, String, i64, i64)> {
    source_plan
        .selected_targets
        .iter()
        .map(|target| {
            (
                target.source_family.clone(),
                target.address.to_ascii_lowercase(),
                target.effective_from_block,
                target.effective_to_block,
            )
        })
        .collect()
}

pub(super) fn source_scope_includes_reverse_claim(
    source_scope: &[(String, String, i64, i64)],
) -> bool {
    source_scope.iter().any(|(source_family, _, _, _)| {
        source_family == SOURCE_FAMILY_ENS_V1_REVERSE_L1
            || source_family == SOURCE_FAMILY_BASENAMES_BASE_PRIMARY
    })
}

pub(super) fn source_scope_includes_ens_v1_unwrapped_authority(
    source_scope: &[(String, String, i64, i64)],
) -> bool {
    source_scope.iter().any(|(source_family, _, _, _)| {
        source_family == SOURCE_FAMILY_ENS_V1_REGISTRAR_L1
            || source_family == SOURCE_FAMILY_ENS_V1_REGISTRY_L1
            || source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1
            || source_family == SOURCE_FAMILY_ENS_V1_WRAPPER_L1
            || source_family == SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
            || source_family == SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
            || source_family == SOURCE_FAMILY_BASENAMES_BASE_RESOLVER
    })
}

pub(super) fn source_scope_includes_ens_v2_registry(
    source_scope: &[(String, String, i64, i64)],
) -> bool {
    source_scope.iter().any(|(source_family, _, _, _)| {
        source_family == SOURCE_FAMILY_ENS_V2_ROOT_L1
            || source_family == SOURCE_FAMILY_ENS_V2_REGISTRY_L1
    })
}

pub(super) fn source_scope_includes_ens_v2_registrar(
    source_scope: &[(String, String, i64, i64)],
) -> bool {
    source_scope
        .iter()
        .any(|(source_family, _, _, _)| source_family == SOURCE_FAMILY_ENS_V2_REGISTRAR_L1)
}

pub(super) fn source_scope_includes_ens_v2_resolver(
    source_scope: &[(String, String, i64, i64)],
) -> bool {
    source_scope
        .iter()
        .any(|(source_family, _, _, _)| source_family == SOURCE_FAMILY_ENS_V2_RESOLVER_L1)
}
