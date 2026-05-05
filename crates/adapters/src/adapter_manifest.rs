use bigname_storage::sql_row;
use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::{WatchedContract, WatchedContractSource};
use sqlx::PgPool;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActiveManifestMetadata {
    pub(crate) manifest_id: i64,
    pub(crate) chain: String,
    pub(crate) namespace: String,
    pub(crate) source_family: String,
    pub(crate) manifest_version: i64,
}

pub(crate) fn source_rank(source: WatchedContractSource) -> i32 {
    match source {
        WatchedContractSource::ManifestRoot => 0,
        WatchedContractSource::ManifestContract => 1,
        WatchedContractSource::DiscoveryEdge => 2,
    }
}

pub(crate) fn watched_contract_manifest_ids(
    watched_contracts: &[WatchedContract],
) -> Result<Vec<i64>> {
    Ok(watched_contracts
        .iter()
        .map(required_source_manifest_id)
        .collect::<Result<HashSet<_>>>()?
        .into_iter()
        .collect::<Vec<_>>())
}

pub(crate) fn required_source_manifest_id(contract: &WatchedContract) -> Result<i64> {
    contract.source_manifest_id.with_context(|| {
        format!(
            "watched contract {} on {} is missing source_manifest_id",
            contract.address, contract.chain
        )
    })
}

pub(crate) fn active_manifest_for_watched_contract<'a>(
    active_manifests: &'a HashMap<i64, ActiveManifestMetadata>,
    watched_contract: &WatchedContract,
) -> Result<(i64, &'a ActiveManifestMetadata)> {
    let source_manifest_id = required_source_manifest_id(watched_contract)?;
    let manifest = active_manifests.get(&source_manifest_id).with_context(|| {
        format!("missing active manifest metadata for manifest_id {source_manifest_id}")
    })?;
    Ok((source_manifest_id, manifest))
}

pub(crate) fn ensure_watched_contract_manifest_chain(
    watched_contract: &WatchedContract,
    manifest: &ActiveManifestMetadata,
    source_manifest_id: i64,
) -> Result<()> {
    if manifest.chain != watched_contract.chain {
        bail!(
            "watched contract chain {} does not match active manifest chain {} for manifest_id {}",
            watched_contract.chain,
            manifest.chain,
            source_manifest_id
        );
    }
    Ok(())
}

pub(crate) async fn load_active_manifest_metadata(
    pool: &PgPool,
    manifest_ids: &[i64],
    context_label: &str,
) -> Result<HashMap<i64, ActiveManifestMetadata>> {
    if manifest_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT manifest_id, chain, namespace, source_family, manifest_version
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND manifest_id = ANY($1::BIGINT[])
        "#,
    )
    .bind(manifest_ids)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load active manifest metadata for {context_label}"))?;

    rows.into_iter()
        .map(|row| {
            let manifest = decode_active_manifest_metadata(row)?;
            Ok((manifest.manifest_id, manifest))
        })
        .collect()
}

pub(crate) async fn load_latest_active_manifest_metadata_for_source_family(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
    context_label: &str,
) -> Result<Option<ActiveManifestMetadata>> {
    let row = sqlx::query(
        r#"
        SELECT manifest_id, chain, namespace, source_family, manifest_version
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND chain = $1
          AND source_family = $2
        ORDER BY manifest_version DESC, manifest_id DESC
        LIMIT 1
        "#,
    )
    .bind(chain)
    .bind(source_family)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load {context_label} for {chain}"))?;

    row.map(decode_active_manifest_metadata).transpose()
}

fn decode_active_manifest_metadata(row: sqlx::postgres::PgRow) -> Result<ActiveManifestMetadata> {
    Ok(ActiveManifestMetadata {
        manifest_id: sql_row::get(&row, "manifest_id")?,
        chain: sql_row::get(&row, "chain")?,
        namespace: sql_row::get(&row, "namespace")?,
        source_family: sql_row::get(&row, "source_family")?,
        manifest_version: sql_row::get(&row, "manifest_version")?,
    })
}
