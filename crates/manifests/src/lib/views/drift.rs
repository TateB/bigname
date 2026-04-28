#[path = "drift/code_hashes.rs"]
mod code_hashes;

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use crate::{
    MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE, MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND,
    normalize_address,
};

use super::types::{
    ManifestDeclaredContractDriftInput, ManifestDriftActiveManifest, ManifestDriftInputs,
    ManifestNormalizedEventInput, ManifestProxyImplementationDriftEdge,
};

pub use code_hashes::{
    load_manifest_code_hash_observations,
    load_manifest_code_hash_observations_for_watched_contracts,
};

pub async fn load_manifest_drift_inputs(pool: &PgPool) -> Result<ManifestDriftInputs> {
    Ok(ManifestDriftInputs {
        active_manifests: load_manifest_drift_active_manifests(pool).await?,
        declared_contracts: load_manifest_declared_contract_drift_inputs(pool).await?,
        proxy_implementation_edges: load_manifest_proxy_implementation_drift_edges(pool).await?,
        code_hash_observations: load_manifest_code_hash_observations(pool).await?,
        normalized_manifest_events: load_manifest_normalized_event_inputs(pool).await?,
    })
}

pub async fn load_manifest_drift_active_manifests(
    pool: &PgPool,
) -> Result<Vec<ManifestDriftActiveManifest>> {
    let rows = sqlx::query(
        r#"
        SELECT
            manifest_id,
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            normalizer_version,
            file_path,
            manifest_payload
        FROM manifest_versions
        WHERE rollout_status = 'active'
        ORDER BY namespace, source_family, chain, deployment_epoch, manifest_version
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active manifest drift inputs")?;

    rows.into_iter()
        .map(|row| {
            let manifest_version = row
                .try_get::<i64, _>("manifest_version")
                .context("failed to read manifest drift manifest_version")?;
            Ok(ManifestDriftActiveManifest {
                manifest_id: row
                    .try_get("manifest_id")
                    .context("failed to read manifest drift manifest_id")?,
                manifest_version: u64::try_from(manifest_version)
                    .context("manifest_version must be non-negative")?,
                namespace: row
                    .try_get("namespace")
                    .context("failed to read manifest drift namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read manifest drift source_family")?,
                chain: row
                    .try_get("chain")
                    .context("failed to read manifest drift chain")?,
                deployment_epoch: row
                    .try_get("deployment_epoch")
                    .context("failed to read manifest drift deployment_epoch")?,
                normalizer_version: row
                    .try_get("normalizer_version")
                    .context("failed to read manifest drift normalizer_version")?,
                file_path: row
                    .try_get("file_path")
                    .context("failed to read manifest drift file_path")?,
                manifest_payload: row
                    .try_get("manifest_payload")
                    .context("failed to read manifest drift manifest_payload")?,
            })
        })
        .collect()
}

pub async fn load_manifest_declared_contract_drift_inputs(
    pool: &PgPool,
) -> Result<Vec<ManifestDeclaredContractDriftInput>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.manifest_version,
            mv.namespace,
            mv.source_family,
            mv.chain,
            mv.deployment_epoch,
            mci.declaration_kind,
            mci.declaration_name,
            mci.contract_instance_id,
            mci.declared_address,
            mci.code_hash,
            mci.abi_ref,
            mci.role,
            mci.proxy_kind,
            mci.implementation_contract_instance_id,
            mci.declared_implementation_address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        ORDER BY
            mv.namespace,
            mv.source_family,
            mv.chain,
            mv.deployment_epoch,
            mv.manifest_version,
            mci.declaration_kind,
            mci.declaration_name
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load manifest declared contract drift inputs")?;

    rows.into_iter()
        .map(|row| {
            let manifest_version = row
                .try_get::<i64, _>("manifest_version")
                .context("failed to read declared contract manifest_version")?;
            let declared_address = row
                .try_get::<String, _>("declared_address")
                .context("failed to read declared contract address")?;
            let declared_implementation_address = row
                .try_get::<Option<String>, _>("declared_implementation_address")
                .context("failed to read declared implementation address")?
                .map(|address| normalize_address(&address));
            Ok(ManifestDeclaredContractDriftInput {
                manifest_id: row
                    .try_get("manifest_id")
                    .context("failed to read declared contract manifest_id")?,
                manifest_version: u64::try_from(manifest_version)
                    .context("manifest_version must be non-negative")?,
                namespace: row
                    .try_get("namespace")
                    .context("failed to read declared contract namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read declared contract source_family")?,
                chain: row
                    .try_get("chain")
                    .context("failed to read declared contract chain")?,
                deployment_epoch: row
                    .try_get("deployment_epoch")
                    .context("failed to read declared contract deployment_epoch")?,
                declaration_kind: row
                    .try_get("declaration_kind")
                    .context("failed to read declaration_kind")?,
                declaration_name: row
                    .try_get("declaration_name")
                    .context("failed to read declaration_name")?,
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read declared contract_instance_id")?,
                declared_address: normalize_address(&declared_address),
                code_hash: row
                    .try_get("code_hash")
                    .context("failed to read code_hash")?,
                abi_ref: row.try_get("abi_ref").context("failed to read abi_ref")?,
                role: row.try_get("role").context("failed to read role")?,
                proxy_kind: row
                    .try_get("proxy_kind")
                    .context("failed to read proxy_kind")?,
                implementation_contract_instance_id: row
                    .try_get("implementation_contract_instance_id")
                    .context("failed to read implementation_contract_instance_id")?,
                declared_implementation_address,
            })
        })
        .collect()
}

pub async fn load_manifest_proxy_implementation_drift_edges(
    pool: &PgPool,
) -> Result<Vec<ManifestProxyImplementationDriftEdge>> {
    let rows = sqlx::query(
        r#"
        SELECT
            de.discovery_edge_id,
            de.source_manifest_id,
            mv.manifest_version,
            mv.namespace,
            mv.source_family,
            de.chain_id,
            de.from_contract_instance_id AS proxy_contract_instance_id,
            proxy_address.address AS proxy_address,
            de.to_contract_instance_id AS implementation_contract_instance_id,
            implementation_address.address AS implementation_address,
            mci.declaration_name,
            mci.role,
            mci.proxy_kind,
            de.admission,
            de.active_from_block_number,
            de.active_to_block_number,
            de.provenance
        FROM discovery_edges de
        JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
        LEFT JOIN contract_instance_addresses proxy_address
          ON proxy_address.contract_instance_id = de.from_contract_instance_id
         AND proxy_address.deactivated_at IS NULL
        LEFT JOIN contract_instance_addresses implementation_address
          ON implementation_address.contract_instance_id = de.to_contract_instance_id
         AND implementation_address.deactivated_at IS NULL
        LEFT JOIN manifest_contract_instances mci
          ON mci.manifest_id = mv.manifest_id
         AND mci.contract_instance_id = de.from_contract_instance_id
         AND mci.implementation_contract_instance_id = de.to_contract_instance_id
        WHERE mv.rollout_status = 'active'
          AND de.deactivated_at IS NULL
          AND de.edge_kind = $1
          AND de.discovery_source = $2
        ORDER BY mv.namespace, mv.source_family, de.chain_id, proxy_address.address, implementation_address.address
        "#,
    )
    .bind(MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND)
    .bind(MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE)
    .fetch_all(pool)
    .await
    .context("failed to load manifest proxy implementation drift edges")?;

    rows.into_iter()
        .map(|row| {
            let manifest_version = row
                .try_get::<i64, _>("manifest_version")
                .context("failed to read proxy edge manifest_version")?;
            let proxy_address = row
                .try_get::<Option<String>, _>("proxy_address")
                .context("failed to read proxy edge proxy_address")?
                .map(|address| normalize_address(&address));
            let implementation_address = row
                .try_get::<Option<String>, _>("implementation_address")
                .context("failed to read proxy edge implementation_address")?
                .map(|address| normalize_address(&address));
            Ok(ManifestProxyImplementationDriftEdge {
                discovery_edge_id: row
                    .try_get("discovery_edge_id")
                    .context("failed to read proxy edge discovery_edge_id")?,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("failed to read proxy edge source_manifest_id")?,
                manifest_version: u64::try_from(manifest_version)
                    .context("manifest_version must be non-negative")?,
                namespace: row
                    .try_get("namespace")
                    .context("failed to read proxy edge namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read proxy edge source_family")?,
                chain: row
                    .try_get("chain_id")
                    .context("failed to read proxy edge chain_id")?,
                proxy_contract_instance_id: row
                    .try_get("proxy_contract_instance_id")
                    .context("failed to read proxy_contract_instance_id")?,
                proxy_address,
                implementation_contract_instance_id: row
                    .try_get("implementation_contract_instance_id")
                    .context("failed to read implementation_contract_instance_id")?,
                implementation_address,
                declaration_name: row
                    .try_get("declaration_name")
                    .context("failed to read proxy edge declaration_name")?,
                role: row
                    .try_get("role")
                    .context("failed to read proxy edge role")?,
                proxy_kind: row
                    .try_get("proxy_kind")
                    .context("failed to read proxy edge proxy_kind")?,
                admission: row
                    .try_get("admission")
                    .context("failed to read proxy edge admission")?,
                active_from_block_number: row
                    .try_get("active_from_block_number")
                    .context("failed to read proxy edge active_from_block_number")?,
                active_to_block_number: row
                    .try_get("active_to_block_number")
                    .context("failed to read proxy edge active_to_block_number")?,
                provenance: row
                    .try_get("provenance")
                    .context("failed to read proxy edge provenance")?,
            })
        })
        .collect()
}

pub async fn load_manifest_normalized_event_inputs(
    pool: &PgPool,
) -> Result<Vec<ManifestNormalizedEventInput>> {
    let rows = sqlx::query(
        r#"
        SELECT
            normalized_event_id,
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state::TEXT AS canonicality_state,
            before_state,
            after_state
        FROM normalized_events
        WHERE event_kind IN (
            'SourceManifestUpdated',
            'ProxyImplementationChanged',
            'CapabilityChanged'
        )
          AND canonicality_state <> 'orphaned'
        ORDER BY namespace, source_family, manifest_version, event_kind, normalized_event_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load manifest normalized-event inputs")?;

    rows.into_iter()
        .map(|row| {
            let manifest_version = row
                .try_get::<i64, _>("manifest_version")
                .context("failed to read manifest event manifest_version")?;
            Ok(ManifestNormalizedEventInput {
                normalized_event_id: row
                    .try_get("normalized_event_id")
                    .context("failed to read normalized_event_id")?,
                event_identity: row
                    .try_get("event_identity")
                    .context("failed to read event_identity")?,
                namespace: row
                    .try_get("namespace")
                    .context("failed to read manifest event namespace")?,
                logical_name_id: row
                    .try_get("logical_name_id")
                    .context("failed to read logical_name_id")?,
                resource_id: row
                    .try_get("resource_id")
                    .context("failed to read resource_id")?,
                event_kind: row
                    .try_get("event_kind")
                    .context("failed to read event_kind")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read manifest event source_family")?,
                manifest_version: u64::try_from(manifest_version)
                    .context("manifest_version must be non-negative")?,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("failed to read source_manifest_id")?,
                chain_id: row.try_get("chain_id").context("failed to read chain_id")?,
                block_number: row
                    .try_get("block_number")
                    .context("failed to read block_number")?,
                block_hash: row
                    .try_get("block_hash")
                    .context("failed to read block_hash")?,
                transaction_hash: row
                    .try_get("transaction_hash")
                    .context("failed to read transaction_hash")?,
                log_index: row
                    .try_get("log_index")
                    .context("failed to read log_index")?,
                raw_fact_ref: row
                    .try_get("raw_fact_ref")
                    .context("failed to read raw_fact_ref")?,
                derivation_kind: row
                    .try_get("derivation_kind")
                    .context("failed to read derivation_kind")?,
                canonicality_state: row
                    .try_get("canonicality_state")
                    .context("failed to read manifest event canonicality_state")?,
                before_state: row
                    .try_get("before_state")
                    .context("failed to read before_state")?,
                after_state: row
                    .try_get("after_state")
                    .context("failed to read after_state")?,
            })
        })
        .collect()
}
