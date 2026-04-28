use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::load_watched_contracts;
use bigname_storage::NormalizedEvent;
use sqlx::{PgPool, Row};

use super::constants::{RESOLVER_EDGE_KIND, SOURCE_FAMILY_ENS_V2_RESOLVER_L1};
use super::types::{ActiveEmitter, ActiveManifestMetadata, PermissionsRawLogRow};
use super::util::{normalize_address, parse_canonicality_state};

pub(super) async fn load_permissions_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
) -> Result<Vec<PermissionsRawLogRow>> {
    if emitters.is_empty() {
        return Ok(Vec::new());
    }

    let mut emitters_by_address = HashMap::<String, Vec<ActiveEmitter>>::new();
    for emitter in emitters.iter().cloned() {
        emitters_by_address
            .entry(emitter.address.clone())
            .or_default()
            .push(emitter);
    }
    let watched_addresses = emitters_by_address.keys().cloned().collect::<Vec<_>>();
    let (scope_addresses, scope_from_blocks, scope_to_blocks) =
        permissions_source_scope_bindings(source_scope);
    if source_scope.is_some() && scope_addresses.is_empty() {
        return Ok(Vec::new());
    }
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
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 permission raw logs for chain {chain}"))?;

    let mut output = Vec::new();
    for row in rows {
        let emitting_address = normalize_address(
            &row.try_get::<String, _>("emitting_address")
                .context("missing emitting_address")?,
        );
        let block_number = row
            .try_get("block_number")
            .context("missing block_number")?;
        let Some(emitter) = emitters_by_address
            .get(&emitting_address)
            .and_then(|emitters| emitter_for_block(emitters, block_number))
        else {
            continue;
        };
        output.push(PermissionsRawLogRow {
            chain_id: row.try_get("chain_id").context("missing chain_id")?,
            block_hash: row.try_get("block_hash").context("missing block_hash")?,
            block_number,
            transaction_hash: row
                .try_get("transaction_hash")
                .context("missing transaction_hash")?,
            transaction_index: row
                .try_get("transaction_index")
                .context("missing transaction_index")?,
            log_index: row.try_get("log_index").context("missing log_index")?,
            emitting_address,
            emitting_contract_instance_id: emitter.contract_instance_id,
            topics: row.try_get("topics").context("missing topics")?,
            data: row.try_get("data").context("missing data")?,
            canonicality_state: parse_canonicality_state(
                &row.try_get::<String, _>("canonicality_state")
                    .context("missing canonicality_state")?,
            )?,
            source_manifest_id: emitter.source_manifest_id,
            namespace: emitter.namespace.clone(),
            source_family: emitter.source_family.clone(),
            manifest_version: emitter.manifest_version,
        });
    }
    Ok(output)
}

fn permissions_source_scope_bindings(
    source_scope: Option<&[(String, String, i64, i64)]>,
) -> (Vec<String>, Vec<i64>, Vec<i64>) {
    let mut addresses = Vec::new();
    let mut from_blocks = Vec::new();
    let mut to_blocks = Vec::new();
    for (source_family, address, from_block, to_block) in source_scope.unwrap_or(&[]) {
        if source_family != SOURCE_FAMILY_ENS_V2_RESOLVER_L1 {
            continue;
        }
        addresses.push(address.to_ascii_lowercase());
        from_blocks.push(*from_block);
        to_blocks.push(*to_block);
    }
    (addresses, from_blocks, to_blocks)
}

pub(super) async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .context("failed to load watched contracts for ENSv2 permissions adapter")?;
    let watched_contracts = watched_contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .collect::<Vec<_>>();
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let manifest_ids = watched_contracts
        .iter()
        .map(|contract| {
            contract.source_manifest_id.with_context(|| {
                format!(
                    "watched contract {} on {} is missing source_manifest_id",
                    contract.address, contract.chain
                )
            })
        })
        .collect::<Result<HashSet<_>>>()?
        .into_iter()
        .collect::<Vec<_>>();
    let active_manifests = load_active_manifest_metadata(pool, &manifest_ids).await?;

    let mut emitters_by_address = HashMap::<String, ActiveEmitter>::new();
    for watched_contract in watched_contracts {
        let source_manifest_id = watched_contract
            .source_manifest_id
            .context("watched contract missing source_manifest_id after validation")?;
        let manifest = active_manifests.get(&source_manifest_id).with_context(|| {
            format!("missing active manifest metadata for manifest_id {source_manifest_id}")
        })?;
        if manifest.source_family != SOURCE_FAMILY_ENS_V2_RESOLVER_L1 {
            continue;
        }
        if manifest.chain != watched_contract.chain {
            bail!(
                "watched contract chain {} does not match active manifest chain {} for manifest_id {}",
                watched_contract.chain,
                manifest.chain,
                source_manifest_id
            );
        }

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
    if let Some(manifest) = load_active_resolver_manifest_metadata(pool, chain).await? {
        for emitter in load_discovered_resolver_emitters(pool, chain, &manifest).await? {
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
    .bind(RESOLVER_EDGE_KIND)
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
                active_from_block_number: row
                    .try_get("active_from_block_number")
                    .context("missing active_from_block_number")?,
                active_to_block_number: row
                    .try_get("active_to_block_number")
                    .context("missing active_to_block_number")?,
            })
        })
        .collect()
}

fn emitter_for_block(emitters: &[ActiveEmitter], block_number: i64) -> Option<&ActiveEmitter> {
    emitters.iter().find(|emitter| {
        emitter
            .active_from_block_number
            .is_none_or(|active_from| block_number >= active_from)
            && emitter
                .active_to_block_number
                .is_none_or(|active_to| block_number < active_to)
    })
}

async fn load_active_resolver_manifest_metadata(
    pool: &PgPool,
    chain: &str,
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
    .bind(SOURCE_FAMILY_ENS_V2_RESOLVER_L1)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load active ENSv2 resolver manifest for {chain}"))?;

    row.map(decode_active_manifest_metadata).transpose()
}

async fn load_active_manifest_metadata(
    pool: &PgPool,
    manifest_ids: &[i64],
) -> Result<HashMap<i64, ActiveManifestMetadata>> {
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
    .context("failed to load active manifest metadata for ENSv2 permissions emitters")?;

    rows.into_iter()
        .map(|row| {
            let manifest = decode_active_manifest_metadata(row)?;
            Ok((manifest.manifest_id, manifest))
        })
        .collect()
}

fn decode_active_manifest_metadata(row: sqlx::postgres::PgRow) -> Result<ActiveManifestMetadata> {
    Ok(ActiveManifestMetadata {
        manifest_id: row.try_get("manifest_id").context("missing manifest_id")?,
        chain: row.try_get("chain").context("missing chain")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        source_family: row
            .try_get("source_family")
            .context("missing source_family")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
    })
}

pub(super) async fn load_existing_event_identities(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<HashSet<String>> {
    if events.is_empty() {
        return Ok(HashSet::new());
    }

    let identities = events
        .iter()
        .map(|event| event.event_identity.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT event_identity
        FROM normalized_events
        WHERE event_identity = ANY($1::TEXT[])
        "#,
    )
    .bind(&identities)
    .fetch_all(pool)
    .await
    .context("failed to load existing ENSv2 permissions event identities")?;

    rows.into_iter()
        .map(|row| {
            row.try_get("event_identity")
                .context("missing event_identity")
        })
        .collect()
}
