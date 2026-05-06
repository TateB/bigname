use anyhow::{Context, Result};
use bigname_storage::{CanonicalityState, SurfaceBindingKind};
use sqlx::{PgPool, Row};

use super::project::{chain_slot, latest_chain_position_for_chain};
use super::types::{
    BasenamesExecutionManifestVersion, ChainPositionCandidate, CurrentBindingContext, HistoryHeads,
    NameSurfaceSeed, RelevantEvent, SupplementalChainObservation, WildcardSourceContext,
};
use super::{
    BASE_MAINNET_CHAIN_ID, BASENAMES_L1_RESOLVER_ADDRESS, BASENAMES_NAMESPACE,
    BASENAMES_V1_DEPLOYMENT_EPOCH, CANONICAL_STATE_FILTER, ETHEREUM_MAINNET_CHAIN_ID,
    SOURCE_FAMILY_BASENAMES_EXECUTION, VERIFIED_RESOLUTION_CAPABILITY,
};

pub(super) async fn load_active_basenames_execution_manifest(
    pool: &PgPool,
    namespace: &str,
) -> Result<Option<BasenamesExecutionManifestVersion>> {
    if namespace != BASENAMES_NAMESPACE {
        return Ok(None);
    }

    let row = sqlx::query(
        r#"
        SELECT
            mv.manifest_version,
            mv.chain,
            mv.deployment_epoch,
            mci.declared_address AS contract_address
        FROM manifest_versions mv
        JOIN manifest_capability_flags mcf
          ON mcf.manifest_id = mv.manifest_id
         AND mcf.capability_name = $1
         AND mcf.status = 'supported'::capability_support_status
        JOIN manifest_contract_instances mci
          ON mci.manifest_id = mv.manifest_id
         AND mci.declaration_kind = 'contract'
         AND mci.role = 'l1_resolver'
         AND lower(mci.declared_address) = lower($6)
        WHERE mv.namespace = $2
          AND mv.source_family = $3
          AND mv.chain = $4
          AND mv.deployment_epoch = $5
          AND mv.rollout_status = 'active'::manifest_rollout_status
        ORDER BY mv.manifest_version DESC, mv.manifest_id DESC
        LIMIT 1
        "#,
    )
    .bind(VERIFIED_RESOLUTION_CAPABILITY)
    .bind(BASENAMES_NAMESPACE)
    .bind(SOURCE_FAMILY_BASENAMES_EXECUTION)
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(BASENAMES_V1_DEPLOYMENT_EPOCH)
    .bind(BASENAMES_L1_RESOLVER_ADDRESS)
    .fetch_optional(pool)
    .await
    .context("failed to load active basenames_execution manifest metadata for name_current")?;

    row.map(|row| {
        Ok(BasenamesExecutionManifestVersion {
            manifest_version: row
                .try_get("manifest_version")
                .context("missing basenames_execution manifest_version")?,
            chain: row
                .try_get("chain")
                .context("missing basenames_execution chain")?,
            deployment_epoch: row
                .try_get("deployment_epoch")
                .context("missing basenames_execution deployment_epoch")?,
            contract_address: row
                .try_get("contract_address")
                .context("missing basenames_execution contract_address")?,
        })
    })
    .transpose()
}

pub(super) async fn load_supplemental_chain_observations(
    pool: &PgPool,
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    wildcard_source_context: Option<&WildcardSourceContext>,
    basenames_execution_manifest: Option<&BasenamesExecutionManifestVersion>,
) -> Result<Vec<SupplementalChainObservation>> {
    let mut observations = Vec::new();

    if let Some(context) = wildcard_source_context {
        for event in context.events() {
            if let Some(observation) = supplemental_chain_observation_from_event(event)? {
                observations.push(observation);
            }
        }
    }

    if let Some(observation) = load_basenames_execution_target_lineage_observation(
        pool,
        name,
        current_binding,
        events,
        history_heads,
        basenames_execution_manifest,
    )
    .await?
    {
        observations.push(observation);
    }

    Ok(observations)
}

fn supplemental_chain_observation_from_event(
    event: &RelevantEvent,
) -> Result<Option<SupplementalChainObservation>> {
    let (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
        event.chain_id.as_ref(),
        event.block_number,
        event.block_hash.as_ref(),
        event.block_timestamp,
    ) else {
        return Ok(None);
    };

    Ok(Some(SupplementalChainObservation {
        candidate: ChainPositionCandidate {
            slot: chain_slot(chain_id),
            chain_id: chain_id.clone(),
            block_number,
            block_hash: block_hash.clone(),
            timestamp,
        },
        canonicality_state: event.canonicality_state,
    }))
}

async fn load_basenames_execution_target_lineage_observation(
    pool: &PgPool,
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    basenames_execution_manifest: Option<&BasenamesExecutionManifestVersion>,
) -> Result<Option<SupplementalChainObservation>> {
    if name.namespace != BASENAMES_NAMESPACE || basenames_execution_manifest.is_none() {
        return Ok(None);
    }
    if current_binding
        .is_none_or(|binding| binding.binding_kind != SurfaceBindingKind::DeclaredRegistryPath)
    {
        return Ok(None);
    }

    let Some(base_boundary) = latest_chain_position_for_chain(
        name,
        current_binding,
        events,
        history_heads,
        BASE_MAINNET_CHAIN_ID,
    ) else {
        return Ok(None);
    };

    let row = sqlx::query(&format!(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state::TEXT AS canonicality_state
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_timestamp <= $2
          AND canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY block_timestamp DESC, block_number DESC, block_hash DESC
        LIMIT 1
        "#
    ))
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(base_boundary.timestamp)
    .fetch_optional(pool)
    .await
    .context("failed to load Basenames execution target lineage position for name_current")?;

    row.map(|row| {
        let chain_id = row
            .try_get::<String, _>("chain_id")
            .context("missing Basenames transport chain_id")?;
        Ok(SupplementalChainObservation {
            candidate: ChainPositionCandidate {
                slot: chain_slot(&chain_id),
                chain_id,
                block_number: row
                    .try_get("block_number")
                    .context("missing Basenames transport block_number")?,
                block_hash: row
                    .try_get("block_hash")
                    .context("missing Basenames transport block_hash")?,
                timestamp: row
                    .try_get("block_timestamp")
                    .context("missing Basenames transport block_timestamp")?,
            },
            canonicality_state: CanonicalityState::parse(
                &row.try_get::<String, _>("canonicality_state")
                    .context("missing Basenames transport canonicality_state")?,
            )?,
        })
    })
    .transpose()
}
