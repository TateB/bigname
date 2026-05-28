mod coverage;
mod json;
mod load;
mod project;
mod resolution;
mod supplemental;
mod types;
mod wildcard;

use anyhow::Result;
use bigname_storage::{
    NameCurrentReplacement, NameCurrentRow, delete_name_current, upsert_name_current_rows,
};
use coverage::build_exact_name_coverage;
use json::{build_declared_summary, build_provenance};
use load::{
    load_canonical_name_surface, load_canonical_name_surfaces, load_current_binding_context,
    load_history_heads, load_relevant_events,
};
use project::{build_canonicality_summary, build_chain_positions, max_timestamp, project_facts};
use resolution::build_supported_resolution_projection;
use sqlx::{PgPool, types::time::OffsetDateTime};
use supplemental::{
    load_active_basenames_execution_manifest, load_supplemental_chain_observations,
};
use tokio::task::JoinSet;
use types::{NameSurfaceSeed, WildcardSourceContext};
use wildcard::load_wildcard_source_context;

#[cfg(test)]
use bigname_storage::{
    CanonicalityState, HistoryScope, SurfaceBindingKind, load_name_history_head,
};
#[cfg(test)]
use json::{format_timestamp, history_pointer_from_event, history_pointer_json};
#[cfg(test)]
use load::load_name_resource_ids;
#[cfg(test)]
use resolution::{empty_alias_detail, empty_transport_detail, empty_wildcard_detail};
#[cfg(test)]
use serde_json::{Value, json};
#[cfg(test)]
use sqlx::Row;
#[cfg(test)]
use types::RelevantEvent;
#[cfg(test)]
use uuid::Uuid;

const ENS_NAMESPACE: &str = "ens";
const BASENAMES_NAMESPACE: &str = "basenames";
const ENS_V1_AUTHORITY_DERIVATION_KIND: &str = "ens_v1_unwrapped_authority";
const ENS_V2_REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
const ENS_V2_REGISTRAR_DERIVATION_KIND: &str = "ens_v2_registrar";
const ENS_V2_RESOLVER_DERIVATION_KIND: &str = "ens_v2_resolver";
const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
const SOURCE_FAMILY_ENS_V2_REGISTRAR_L1: &str = "ens_v2_registrar_l1";
const SELECTED_ENS_V2_EXACT_NAME_DEPLOYMENT_EPOCH: &str = "ens_v2_sepolia_dev";
const CAPABILITY_STATUS_SUPPORTED: &str = "supported";
const MANIFEST_ROLLOUT_STATUS_ACTIVE: &str = "active";
const ETHEREUM_SEPOLIA_CHAIN_ID: &str = "ethereum-sepolia";
const ETHEREUM_MAINNET_CHAIN_ID: &str = "ethereum-mainnet";
const BASE_MAINNET_CHAIN_ID: &str = "base-mainnet";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR: &str = "basenames_base_registrar";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";
const SOURCE_FAMILY_BASENAMES_EXECUTION: &str = "basenames_execution";
const VERIFIED_RESOLUTION_CAPABILITY: &str = "verified_resolution";
const BASENAMES_V1_DEPLOYMENT_EPOCH: &str = "basenames_v1";
const BASENAMES_L1_RESOLVER_ADDRESS: &str = "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31";
const NAME_CURRENT_DERIVATION_KIND: &str = "name_current_rebuild";
const NAME_CURRENT_REBUILD_CONCURRENCY: usize = 32;
const EVENT_KIND_ALIAS_CHANGED: &str = "AliasChanged";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
const EVENT_KIND_RECORD_VERSION_CHANGED: &str = "RecordVersionChanged";
const RECORD_INVENTORY_UNSUPPORTED_REASON: &str =
    "record_inventory remains unsupported in the ENSv1 name_current rebuild";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const NAME_CURRENT_REBUILD_STAGE_BATCH_SIZE: usize = 2_000;
const RELEVANT_EVENT_KINDS: &[&str] = &[
    "AuthorityEpochChanged",
    "AuthorityTransferred",
    EVENT_KIND_ALIAS_CHANGED,
    "ExpiryChanged",
    "RegistrationGranted",
    "RegistrationReleased",
    "RegistrationRenewed",
    EVENT_KIND_RECORD_VERSION_CHANGED,
    EVENT_KIND_RESOLVER_CHANGED,
    "SurfaceBound",
    "SurfaceUnbound",
    "TokenResourceLinked",
    "TokenRegenerated",
    "TokenControlTransferred",
];
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NameCurrentRebuildSummary {
    pub requested_name_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

pub async fn rebuild_name_current(
    pool: &PgPool,
    logical_name_id: Option<&str>,
) -> Result<NameCurrentRebuildSummary> {
    match logical_name_id {
        Some(logical_name_id) => rebuild_one_name_current(pool, logical_name_id).await,
        None => rebuild_all_name_current(pool).await,
    }
}

async fn rebuild_all_name_current(pool: &PgPool) -> Result<NameCurrentRebuildSummary> {
    let names = load_canonical_name_surfaces(pool).await?;
    let requested_name_count = names.len();
    let mut replacement = NameCurrentReplacement::begin(pool).await?;
    let mut rows = Vec::with_capacity(NAME_CURRENT_REBUILD_STAGE_BATCH_SIZE);
    let mut completed_name_count = 0usize;
    let mut names = names.into_iter();
    let mut tasks = JoinSet::new();

    for _ in 0..NAME_CURRENT_REBUILD_CONCURRENCY {
        let Some(name) = names.next() else {
            break;
        };
        spawn_name_current_rebuild_task(&mut tasks, pool, name);
    }

    while let Some(result) = tasks.join_next().await {
        rows.push(result??);
        completed_name_count += 1;
        if rows.len() >= NAME_CURRENT_REBUILD_STAGE_BATCH_SIZE {
            replacement.stage_rows(&rows).await?;
            rows.clear();
        }
        if completed_name_count % 5_000 == 0 {
            tracing::info!(
                projection = "name_current",
                requested_name_count,
                completed_name_count,
                staged_row_count = replacement.staged_row_count(),
                "name_current rebuild rows built"
            );
        }
        if let Some(name) = names.next() {
            spawn_name_current_rebuild_task(&mut tasks, pool, name);
        }
    }

    if !rows.is_empty() {
        replacement.stage_rows(&rows).await?;
        rows.clear();
    }
    let upserted_row_count = replacement.staged_row_count();
    let (published_row_count, deleted_row_count) = replacement.publish().await?;
    tracing::info!(
        projection = "name_current",
        requested_name_count,
        completed_name_count,
        upserted_row_count,
        published_row_count,
        deleted_row_count,
        "name_current rebuild replacement published"
    );
    Ok(NameCurrentRebuildSummary {
        requested_name_count,
        upserted_row_count,
        deleted_row_count,
    })
}

fn spawn_name_current_rebuild_task(
    tasks: &mut JoinSet<Result<bigname_storage::NameCurrentRow>>,
    pool: &PgPool,
    name: NameSurfaceSeed,
) {
    let pool = pool.clone();
    tasks.spawn(async move { build_name_current_row(&pool, &name).await });
}

async fn rebuild_one_name_current(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<NameCurrentRebuildSummary> {
    let Some(name) = load_canonical_name_surface(pool, logical_name_id).await? else {
        let deleted_row_count = delete_name_current(pool, logical_name_id).await?;
        return Ok(NameCurrentRebuildSummary {
            requested_name_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    };

    let row = build_name_current_row(pool, &name).await?;
    let upserted_row_count = upsert_name_current_rows(pool, &[row]).await?.len();
    Ok(NameCurrentRebuildSummary {
        requested_name_count: 1,
        upserted_row_count,
        deleted_row_count: 0,
    })
}

async fn build_name_current_row(pool: &PgPool, name: &NameSurfaceSeed) -> Result<NameCurrentRow> {
    let current_binding = load_current_binding_context(pool, &name.logical_name_id).await?;
    let events = load_relevant_events(pool, name, current_binding.as_ref()).await?;
    let history_heads = load_history_heads(pool, &name.logical_name_id).await?;
    let basenames_execution_manifest =
        load_active_basenames_execution_manifest(pool, &name.namespace).await?;
    let wildcard_source_context =
        load_wildcard_source_context(pool, name, current_binding.as_ref()).await?;
    let supplemental_chain_observations = load_supplemental_chain_observations(
        pool,
        name,
        current_binding.as_ref(),
        &events,
        &history_heads,
        wildcard_source_context.as_ref(),
        basenames_execution_manifest.as_ref(),
    )
    .await?;
    let facts = project_facts(&events, current_binding.as_ref(), &history_heads)?;
    let chain_positions = build_chain_positions(
        name,
        current_binding.as_ref(),
        &events,
        &history_heads,
        &supplemental_chain_observations,
    );
    let supported_resolution_projection = build_supported_resolution_projection(
        name,
        current_binding.as_ref(),
        &facts,
        &events,
        &chain_positions,
        wildcard_source_context.as_ref(),
        basenames_execution_manifest.as_ref(),
    )?;
    let canonicality_summary = build_canonicality_summary(
        name,
        current_binding.as_ref(),
        &events,
        &history_heads,
        &supplemental_chain_observations,
    );
    let provenance = build_provenance(
        &events,
        &history_heads,
        wildcard_source_context.as_ref(),
        supported_resolution_projection
            .as_ref()
            .map(|projection| projection.manifest_versions.as_slice())
            .unwrap_or(&[]),
    )?;
    let manifest_version = events
        .iter()
        .map(|event| event.manifest_version)
        .chain(
            wildcard_source_context
                .as_ref()
                .into_iter()
                .flat_map(WildcardSourceContext::events)
                .map(|event| event.manifest_version),
        )
        .chain(history_heads.iter().map(|event| event.manifest_version))
        .max()
        .unwrap_or(1);
    let last_recomputed_at = max_timestamp(
        name,
        current_binding.as_ref(),
        &events,
        &history_heads,
        &supplemental_chain_observations,
    )
    .unwrap_or(OffsetDateTime::UNIX_EPOCH);

    Ok(NameCurrentRow {
        logical_name_id: name.logical_name_id.clone(),
        namespace: name.namespace.clone(),
        canonical_display_name: name.canonical_display_name.clone(),
        normalized_name: name.normalized_name.clone(),
        namehash: name.namehash.clone(),
        surface_binding_id: current_binding
            .as_ref()
            .map(|binding| binding.surface_binding_id),
        resource_id: current_binding.as_ref().map(|binding| binding.resource_id),
        token_lineage_id: current_binding
            .as_ref()
            .and_then(|binding| binding.token_lineage_id),
        binding_kind: current_binding.as_ref().map(|binding| binding.binding_kind),
        declared_summary: build_declared_summary(
            facts,
            supported_resolution_projection.map(|projection| projection.topology),
        ),
        provenance,
        coverage: build_exact_name_coverage(&name.namespace, &events),
        chain_positions,
        canonicality_summary,
        manifest_version,
        last_recomputed_at,
    })
}

#[cfg(test)]
mod tests;
