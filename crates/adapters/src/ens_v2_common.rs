use bigname_storage::sql_row;
use std::collections::HashMap;

use anyhow::{Context, Result};
use bigname_manifests::load_watched_contracts;
use sqlx::{PgPool, Row, types::Uuid};

use crate::adapter_manifest::{
    ActiveManifestMetadata, active_manifest_for_watched_contract,
    ensure_watched_contract_manifest_chain, load_active_manifest_metadata,
    load_latest_active_manifest_metadata_for_source_family, watched_contract_manifest_ids,
};
pub(crate) use crate::evm_abi::{keccak256_bytes, keccak256_hex};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActiveEmitter {
    pub(crate) address: String,
    pub(crate) contract_instance_id: Uuid,
    pub(crate) source_manifest_id: i64,
    pub(crate) namespace: String,
    pub(crate) source_family: String,
    pub(crate) manifest_version: i64,
    pub(crate) active_from_block_number: Option<i64>,
    pub(crate) active_to_block_number: Option<i64>,
}

pub(crate) fn source_scope_bindings(
    source_scope: Option<&[(String, String, i64, i64)]>,
    source_family_filter: &str,
) -> (Vec<String>, Vec<i64>, Vec<i64>) {
    let mut addresses = Vec::new();
    let mut from_blocks = Vec::new();
    let mut to_blocks = Vec::new();
    for (source_family, address, from_block, to_block) in source_scope.unwrap_or(&[]) {
        if source_family != source_family_filter {
            continue;
        }
        addresses.push(address.to_ascii_lowercase());
        from_blocks.push(*from_block);
        to_blocks.push(*to_block);
    }
    (addresses, from_blocks, to_blocks)
}

pub(crate) fn emitters_by_address(
    emitters: &[ActiveEmitter],
) -> HashMap<String, Vec<ActiveEmitter>> {
    let mut output = HashMap::<String, Vec<ActiveEmitter>>::new();
    for emitter in emitters.iter().cloned() {
        output
            .entry(emitter.address.clone())
            .or_default()
            .push(emitter);
    }
    output
}

pub(crate) fn active_emitter_for_block(
    emitters: &[ActiveEmitter],
    block_number: i64,
) -> Option<&ActiveEmitter> {
    emitters.iter().find(|emitter| {
        emitter
            .active_from_block_number
            .is_none_or(|active_from| block_number >= active_from)
            && emitter
                .active_to_block_number
                .is_none_or(|active_to| block_number < active_to)
    })
}

pub(crate) async fn load_active_emitters(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
    resolver_edge_kind: &str,
    adapter_label: &str,
) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .with_context(|| format!("failed to load watched contracts for {adapter_label} adapter"))?;
    let watched_contracts = watched_contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .collect::<Vec<_>>();
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let manifest_ids = watched_contract_manifest_ids(&watched_contracts)?;
    let context_label = format!("{adapter_label} emitters");
    let active_manifests =
        load_active_manifest_metadata(pool, &manifest_ids, &context_label).await?;

    let mut emitters_by_address = HashMap::<String, ActiveEmitter>::new();
    for watched_contract in watched_contracts {
        let (source_manifest_id, manifest) =
            active_manifest_for_watched_contract(&active_manifests, &watched_contract)?;
        if manifest.source_family != source_family {
            continue;
        }
        ensure_watched_contract_manifest_chain(&watched_contract, manifest, source_manifest_id)?;

        emitters_by_address.insert(
            watched_contract.address.clone(),
            ActiveEmitter {
                address: watched_contract.address,
                contract_instance_id: watched_contract.contract_instance_id,
                source_manifest_id,
                namespace: manifest.namespace.clone(),
                source_family: manifest.source_family.clone(),
                manifest_version: manifest.manifest_version,
                active_from_block_number: watched_contract.active_from_block_number,
                active_to_block_number: watched_contract.active_to_block_number,
            },
        );
    }
    if let Some(manifest) =
        load_active_source_family_manifest_metadata(pool, chain, source_family).await?
    {
        for emitter in
            load_discovered_resolver_emitters(pool, chain, resolver_edge_kind, &manifest).await?
        {
            emitters_by_address
                .entry(emitter.address.clone())
                .or_insert(emitter);
        }
    }

    let mut emitters = emitters_by_address.into_values().collect::<Vec<_>>();
    emitters.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
            .then(left.contract_instance_id.cmp(&right.contract_instance_id))
    });
    Ok(emitters)
}

async fn load_discovered_resolver_emitters(
    pool: &PgPool,
    chain: &str,
    resolver_edge_kind: &str,
    manifest: &ActiveManifestMetadata,
) -> Result<Vec<ActiveEmitter>> {
    let rows = sqlx::query(
        r#"
        SELECT
            cia.address,
            de.to_contract_instance_id,
            de.active_from_block_number,
            de.active_to_block_number
        FROM discovery_edges de
        JOIN manifest_versions source_mv
          ON source_mv.manifest_id = de.source_manifest_id
         AND source_mv.rollout_status = 'active'
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
        WHERE de.chain_id = $1
          AND de.edge_kind = $2
        ORDER BY lower(cia.address), de.active_from_block_number NULLS FIRST, de.discovery_edge_id
        "#,
    )
    .bind(chain)
    .bind(resolver_edge_kind)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 discovered resolver emitters for {chain}"))?;

    rows.into_iter()
        .map(|row| {
            let address = normalize_address(
                &row.try_get::<String, _>("address")
                    .context("missing discovered resolver address")?,
            );
            Ok(ActiveEmitter {
                address,
                contract_instance_id: row
                    .try_get("to_contract_instance_id")
                    .context("missing discovered resolver contract_instance_id")?,
                source_manifest_id: manifest.manifest_id,
                namespace: manifest.namespace.clone(),
                source_family: manifest.source_family.clone(),
                manifest_version: manifest.manifest_version,
                active_from_block_number: sql_row::get(&row, "active_from_block_number")?,
                active_to_block_number: sql_row::get(&row, "active_to_block_number")?,
            })
        })
        .collect()
}

async fn load_active_source_family_manifest_metadata(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
) -> Result<Option<ActiveManifestMetadata>> {
    load_latest_active_manifest_metadata_for_source_family(
        pool,
        chain,
        source_family,
        "active ENSv2 resolver manifest",
    )
    .await
}

pub(crate) fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

pub(crate) fn dns_decode_optional(bytes: &[u8]) -> Result<Option<String>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        dns_decode(bytes).map(Some)
    }
}

pub(crate) fn dns_decode(bytes: &[u8]) -> Result<String> {
    bigname_domain::normalization::normalize_dns_encoded_name(bytes)
        .map(|name| name.normalized_name)
        .map_err(anyhow::Error::from)
}

pub(crate) fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    crate::evm_abi::hex_string_without_prefix(bytes)
}
