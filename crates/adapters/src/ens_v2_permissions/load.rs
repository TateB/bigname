use anyhow::{Context, Result};
use bigname_manifests::load_watched_contracts;
use bigname_storage::sql_row;
use sqlx::PgPool;

use crate::adapter_manifest::{
    active_manifest_for_watched_contract, ensure_watched_contract_manifest_chain,
    load_active_manifest_metadata, watched_contract_manifest_ids,
};
use crate::ens_v2_common::{
    ActiveEmitter, active_emitter_for_block, emitters_by_address, normalize_address,
};

use super::constants::{
    RESOLVER_EDGE_KIND, SOURCE_FAMILY_ENS_V2_REGISTRY_L1, SOURCE_FAMILY_ENS_V2_RESOLVER_L1,
    SOURCE_FAMILY_ENS_V2_ROOT_L1,
};
use super::types::PermissionsRawLogRow;

pub(super) async fn load_permissions_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    max_block_number: Option<i64>,
) -> Result<Vec<PermissionsRawLogRow>> {
    if emitters.is_empty() {
        return Ok(Vec::new());
    }

    let active_emitters_by_address = emitters_by_address(emitters);
    let watched_addresses = active_emitters_by_address
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let (scope_addresses, scope_from_blocks, scope_to_blocks) = source_scope_bindings(source_scope);
    if source_scope.is_some() && scope_addresses.is_empty() {
        return Ok(Vec::new());
    }
    let has_max_block_number = max_block_number.is_some();
    let max_block_number = max_block_number.unwrap_or(i64::MAX);
    let rows = sqlx::query(
        r#"
        SELECT
            rl.chain_id,
            rl.block_hash,
            rl.block_number,
            rl.transaction_hash,
            rl.transaction_index,
            rl.log_index,
            rl.emitting_address,
            rl.topics,
            rl.data,
            rl.canonicality_state::TEXT AS canonicality_state
        FROM raw_logs rl
        WHERE rl.chain_id = $1
          AND LOWER(rl.emitting_address) = ANY($2::TEXT[])
          AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
          AND ($9::BOOLEAN = FALSE OR rl.block_number <= $10::BIGINT)
          AND (
              $5::BOOLEAN = FALSE
              OR EXISTS (
                  SELECT 1
                  FROM unnest($6::TEXT[], $7::BIGINT[], $8::BIGINT[])
                    AS source_scope(address, from_block, to_block)
                  WHERE LOWER(rl.emitting_address) = source_scope.address
                    AND rl.block_number >= source_scope.from_block
                    AND rl.block_number <= source_scope.to_block
              )
          )
          AND rl.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY rl.block_number, rl.transaction_index, rl.log_index, LOWER(rl.emitting_address)
        "#,
    )
    .bind(chain)
    .bind(&watched_addresses)
    .bind(restrict_to_block_hashes)
    .bind(block_hashes)
    .bind(source_scope.is_some())
    .bind(&scope_addresses)
    .bind(&scope_from_blocks)
    .bind(&scope_to_blocks)
    .bind(has_max_block_number)
    .bind(max_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 permission raw logs for chain {chain}"))?;

    let mut output = Vec::new();
    for row in rows {
        let emitting_address =
            normalize_address(&sql_row::get::<String>(&row, "emitting_address")?);
        let block_number = sql_row::get(&row, "block_number")?;
        let Some(emitter) = active_emitters_by_address
            .get(&emitting_address)
            .and_then(|emitters| active_emitter_for_block(emitters, block_number))
        else {
            continue;
        };
        output.push(PermissionsRawLogRow {
            chain_id: sql_row::get(&row, "chain_id")?,
            block_hash: sql_row::get(&row, "block_hash")?,
            block_number,
            transaction_hash: sql_row::get(&row, "transaction_hash")?,
            transaction_index: sql_row::get(&row, "transaction_index")?,
            log_index: sql_row::get(&row, "log_index")?,
            emitting_address,
            emitting_contract_instance_id: emitter.contract_instance_id,
            topics: sql_row::get(&row, "topics")?,
            data: sql_row::get(&row, "data")?,
            canonicality_state: sql_row::get(&row, "canonicality_state")?,
            source_manifest_id: emitter.source_manifest_id,
            namespace: emitter.namespace.clone(),
            source_family: emitter.source_family.clone(),
            manifest_version: emitter.manifest_version,
        });
    }
    Ok(output)
}

pub(super) async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
    let mut emitters = crate::ens_v2_common::load_active_emitters(
        pool,
        chain,
        SOURCE_FAMILY_ENS_V2_RESOLVER_L1,
        RESOLVER_EDGE_KIND,
        "ENSv2 permissions",
    )
    .await?;
    emitters.extend(load_registry_active_emitters(pool, chain).await?);
    // Order intervals by activation within each (address, source_family) group, then by identity.
    // Including active_from/active_to keeps this sort TOTAL: one address can now carry several
    // distinct intervals that share a (source_manifest_id, contract_instance_id) — without the
    // interval bounds in the key, their order would fall back to HashMap-iteration order and make
    // `active_emitter_for_block`'s first-match selection nondeterministic for overlapping windows.
    // This matches the resolver path's earliest-activation-first ordering (ens_v2_common).
    emitters.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then(left.source_family.cmp(&right.source_family))
            .then(
                left.active_from_block_number
                    .cmp(&right.active_from_block_number),
            )
            .then(
                left.active_to_block_number
                    .cmp(&right.active_to_block_number),
            )
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
            .then(left.contract_instance_id.cmp(&right.contract_instance_id))
    });
    Ok(emitters)
}

async fn load_registry_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .context("failed to load watched contracts for ENSv2 registry permissions")?;
    let watched_contracts = watched_contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .collect::<Vec<_>>();
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let manifest_ids = watched_contract_manifest_ids(&watched_contracts)?;
    let active_manifests =
        load_active_manifest_metadata(pool, &manifest_ids, "ENSv2 registry permissions").await?;
    let mut emitters = Vec::new();
    for watched_contract in watched_contracts {
        let (source_manifest_id, manifest) =
            active_manifest_for_watched_contract(&active_manifests, &watched_contract)?;
        if manifest.source_family != SOURCE_FAMILY_ENS_V2_ROOT_L1
            && manifest.source_family != SOURCE_FAMILY_ENS_V2_REGISTRY_L1
        {
            continue;
        }
        ensure_watched_contract_manifest_chain(&watched_contract, manifest, source_manifest_id)?;
        emitters.push(ActiveEmitter {
            address: watched_contract.address,
            contract_instance_id: watched_contract.contract_instance_id,
            source_manifest_id,
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            manifest_version: manifest.manifest_version,
            active_from_block_number: watched_contract.active_from_block_number,
            active_to_block_number: watched_contract.active_to_block_number,
        });
    }
    Ok(emitters)
}

fn source_scope_bindings(
    source_scope: Option<&[(String, String, i64, i64)]>,
) -> (Vec<String>, Vec<i64>, Vec<i64>) {
    let mut addresses = Vec::new();
    let mut from_blocks = Vec::new();
    let mut to_blocks = Vec::new();
    for (source_family, address, from_block, to_block) in source_scope.unwrap_or(&[]) {
        if source_family != SOURCE_FAMILY_ENS_V2_ROOT_L1
            && source_family != SOURCE_FAMILY_ENS_V2_REGISTRY_L1
            && source_family != SOURCE_FAMILY_ENS_V2_RESOLVER_L1
        {
            continue;
        }
        addresses.push(address.to_ascii_lowercase());
        from_blocks.push(*from_block);
        to_blocks.push(*to_block);
    }
    (addresses, from_blocks, to_blocks)
}
