use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::load_watched_contracts;
use bigname_storage::NormalizedEvent;
use sqlx::{PgPool, Row};

use super::{
    constants::{RESOLVER_EDGE_KIND, SOURCE_FAMILY_ENS_V2_RESOLVER_L1},
    types::{ActiveEmitter, ActiveManifestMetadata, NameLink, ResolverRawLogRow},
    util::{
        display_name, event_position_timestamp, logical_name_id, normalize_address,
        parse_canonicality_state,
    },
};

pub(super) async fn load_name_link_by_namehash(
    pool: &PgPool,
    raw_log: &ResolverRawLogRow,
    namehash: &str,
) -> Result<NameLink> {
    let position = event_position_timestamp(raw_log);
    let row = sqlx::query(
        r#"
        SELECT
            ns.logical_name_id,
            ns.normalized_name,
            ns.canonical_display_name,
            ns.namehash,
            sb.resource_id
        FROM name_surfaces ns
        LEFT JOIN surface_bindings sb
          ON sb.logical_name_id = ns.logical_name_id
         AND sb.active_from <= $3
         AND (sb.active_to IS NULL OR sb.active_to > $3)
         AND sb.canonicality_state IN (
            'canonical'::canonicality_state,
            'safe'::canonicality_state,
            'finalized'::canonicality_state
         )
        WHERE ns.namespace = $1
          AND lower(ns.namehash) = lower($2)
          AND ns.canonicality_state IN (
            'canonical'::canonicality_state,
            'safe'::canonicality_state,
            'finalized'::canonicality_state
         )
        ORDER BY sb.active_from DESC NULLS LAST, sb.surface_binding_id DESC NULLS LAST
        LIMIT 1
        "#,
    )
    .bind(&raw_log.namespace)
    .bind(namehash)
    .bind(position)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load name link for namespace {} node {namehash} at chain position",
            raw_log.namespace
        )
    })?;

    row.map(decode_name_link)
        .transpose()
        .map(|link| link.unwrap_or_else(NameLink::unknown))
}

pub(super) async fn load_name_link_by_name(
    pool: &PgPool,
    raw_log: &ResolverRawLogRow,
    name: &str,
) -> Result<NameLink> {
    let normalized_name = name.to_ascii_lowercase();
    if normalized_name.is_empty() {
        return Ok(NameLink::unknown());
    }
    let position = event_position_timestamp(raw_log);
    let row = sqlx::query(
        r#"
        SELECT
            ns.logical_name_id,
            ns.normalized_name,
            ns.canonical_display_name,
            ns.namehash,
            sb.resource_id
        FROM name_surfaces ns
        LEFT JOIN surface_bindings sb
          ON sb.logical_name_id = ns.logical_name_id
         AND sb.active_from <= $3
         AND (sb.active_to IS NULL OR sb.active_to > $3)
         AND sb.canonicality_state IN (
            'canonical'::canonicality_state,
            'safe'::canonicality_state,
            'finalized'::canonicality_state
         )
        WHERE ns.namespace = $1
          AND ns.normalized_name = $2
          AND ns.canonicality_state IN (
            'canonical'::canonicality_state,
            'safe'::canonicality_state,
            'finalized'::canonicality_state
         )
        ORDER BY sb.active_from DESC NULLS LAST, sb.surface_binding_id DESC NULLS LAST
        LIMIT 1
        "#,
    )
    .bind(&raw_log.namespace)
    .bind(&normalized_name)
    .bind(position)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load name link for {}:{normalized_name} at chain position",
            raw_log.namespace
        )
    })?;

    Ok(row.map(decode_name_link).transpose()?.unwrap_or(NameLink {
        logical_name_id: Some(logical_name_id(&raw_log.namespace, &normalized_name)),
        normalized_name: Some(normalized_name.clone()),
        canonical_display_name: Some(display_name(&normalized_name)),
        namehash: None,
        resource_id: None,
    }))
}

fn decode_name_link(row: sqlx::postgres::PgRow) -> Result<NameLink> {
    Ok(NameLink {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
    })
}

pub(super) async fn load_resolver_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
) -> Result<Vec<ResolverRawLogRow>> {
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
    let rows = sqlx::query(
        r#"
        SELECT
            rl.chain_id,
            rl.block_hash,
            rl.block_number,
            rb.block_timestamp
              + (((rl.transaction_index * 1000) + GREATEST(rl.log_index, 0)) * INTERVAL '1 microsecond')
              AS event_position_timestamp,
            rl.transaction_hash,
            rl.transaction_index,
            rl.log_index,
            rl.emitting_address,
            rl.topics,
            rl.data,
            rl.canonicality_state::TEXT AS canonicality_state
        FROM raw_logs rl
        JOIN raw_blocks rb
          ON rb.chain_id = rl.chain_id
         AND rb.block_hash = rl.block_hash
        WHERE rl.chain_id = $1
          AND lower(rl.emitting_address) = ANY($2::TEXT[])
          AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
          AND rl.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY rl.block_number, rl.transaction_index, rl.log_index, lower(rl.emitting_address)
        "#,
    )
    .bind(chain)
    .bind(&watched_addresses)
    .bind(restrict_to_block_hashes)
    .bind(block_hashes)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 resolver raw logs for chain {chain}"))?;

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
        output.push(ResolverRawLogRow {
            chain_id: row.try_get("chain_id").context("missing chain_id")?,
            block_hash: row.try_get("block_hash").context("missing block_hash")?,
            block_number,
            event_position_timestamp: row
                .try_get("event_position_timestamp")
                .context("missing event_position_timestamp")?,
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

pub(super) async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .context("failed to load watched contracts for ENSv2 resolver adapter")?;
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
                active_from_block_number: None,
                active_to_block_number: None,
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
    .context("failed to load active manifest metadata for ENSv2 resolver emitters")?;

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
    .context("failed to load existing ENSv2 resolver event identities")?;

    rows.into_iter()
        .map(|row| {
            row.try_get("event_identity")
                .context("missing event_identity")
        })
        .collect()
}
