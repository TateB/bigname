use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::load_watched_contracts;
use bigname_storage::{
    CanonicalityState, NormalizedEvent, Resource, load_resource_including_noncanonical,
    upsert_normalized_events, upsert_resources,
};
use serde_json::{Value, json};
use sha3::{Digest, Keccak256};
use sqlx::{PgPool, Row, types::Uuid};

const SOURCE_FAMILY_ENS_V2_RESOLVER_L1: &str = "ens_v2_resolver_l1";
const DERIVATION_KIND_ENS_V2_PERMISSIONS: &str = "ens_v2_permissions";
const RESOLVER_EDGE_KIND: &str = "resolver";
const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";

const NAMED_RESOURCE_SIGNATURE: &str = "NamedResource(uint256,bytes)";
const NAMED_TEXT_RESOURCE_SIGNATURE: &str = "NamedTextResource(uint256,bytes,bytes32,string)";
const NAMED_ADDR_RESOURCE_SIGNATURE: &str = "NamedAddrResource(uint256,bytes,uint256)";
const EAC_ROLES_CHANGED_SIGNATURE: &str = "EACRolesChanged(uint256,address,uint256,uint256)";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2PermissionsSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_resource_count: usize,
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, EnsV2PermissionsKindSyncSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2PermissionsKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

impl EnsV2PermissionsSyncSummary {
    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v2_permissions_with_scope(pool, chain, true, block_hashes).await
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveEmitter {
    address: String,
    contract_instance_id: Uuid,
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    active_from_block_number: Option<i64>,
    active_to_block_number: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveManifestMetadata {
    manifest_id: i64,
    chain: String,
    namespace: String,
    source_family: String,
    manifest_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PermissionsRawLogRow {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    emitting_address: String,
    emitting_contract_instance_id: Uuid,
    topics: Vec<String>,
    data: Vec<u8>,
    canonicality_state: CanonicalityState,
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolverResourceHint {
    upstream_resource: String,
    logical_name_id: Option<String>,
    normalized_name: Option<String>,
    dns_encoded_name: Option<Vec<u8>>,
    selector_kind: String,
    selector_key: Option<String>,
    selector_hash: Option<String>,
    first_ref: PermissionRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PermissionRef {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    emitting_address: String,
    emitting_contract_instance_id: Uuid,
    canonicality_state: CanonicalityState,
    source_manifest_id: i64,
    source_family: String,
    manifest_version: i64,
}

enum PermissionsObservation {
    NamedResource {
        resource: String,
        name: Vec<u8>,
    },
    NamedTextResource {
        resource: String,
        name: Vec<u8>,
        key_hash: String,
        key: String,
    },
    NamedAddrResource {
        resource: String,
        name: Vec<u8>,
        coin_type: String,
    },
    EacRolesChanged {
        resource: String,
        account: String,
        old_role_bitmap: String,
        new_role_bitmap: String,
    },
}

pub async fn sync_ens_v2_permissions(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV2PermissionsSyncSummary> {
    sync_ens_v2_permissions_with_scope(pool, chain, false, &[]).await
}

async fn sync_ens_v2_permissions_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
) -> Result<EnsV2PermissionsSyncSummary> {
    let active_emitters = load_active_emitters(pool, chain).await?;
    if active_emitters.is_empty() {
        return Ok(empty_summary(0));
    }

    let raw_logs = load_permissions_raw_logs(
        pool,
        chain,
        &active_emitters,
        restrict_to_block_hashes,
        block_hashes,
    )
    .await?;
    let scanned_log_count = raw_logs.len();
    if raw_logs.is_empty() {
        return Ok(empty_summary(scanned_log_count));
    }

    let mut matched_log_count = 0usize;
    let mut hints = HashMap::<(String, String), ResolverResourceHint>::new();
    let mut resources = BTreeMap::<Uuid, (Resource, ResolverResourceHint)>::new();
    let mut events = Vec::new();

    for raw_log in &raw_logs {
        let Some(observation) = build_permissions_observation(raw_log)? else {
            continue;
        };
        matched_log_count += 1;
        match observation {
            PermissionsObservation::NamedResource { resource, name } => {
                let hint = resolver_resource_hint(raw_log, resource, name, "name", None, None)?;
                remember_hint_and_resource(pool, raw_log, hint, &mut hints, &mut resources).await?;
            }
            PermissionsObservation::NamedTextResource {
                resource,
                name,
                key_hash,
                key,
            } => {
                let hint = resolver_resource_hint(
                    raw_log,
                    resource,
                    name,
                    "text",
                    Some(key),
                    Some(key_hash),
                )?;
                remember_hint_and_resource(pool, raw_log, hint, &mut hints, &mut resources).await?;
            }
            PermissionsObservation::NamedAddrResource {
                resource,
                name,
                coin_type,
            } => {
                let hint =
                    resolver_resource_hint(raw_log, resource, name, "addr", Some(coin_type), None)?;
                remember_hint_and_resource(pool, raw_log, hint, &mut hints, &mut resources).await?;
            }
            PermissionsObservation::EacRolesChanged {
                resource,
                account,
                old_role_bitmap,
                new_role_bitmap,
            } => {
                let key = (raw_log.emitting_address.clone(), resource.clone());
                let hint = hints.get(&key).cloned().unwrap_or_else(|| {
                    fallback_resource_hint(raw_log, resource.clone(), resource_is_root(&resource))
                });
                let resource_row =
                    build_resource(pool, raw_log, &hint)
                        .await
                        .with_context(|| {
                            format!(
                                "failed to build ENSv2 resolver permission resource {}",
                                hint.upstream_resource
                            )
                        })?;
                let resource_id = resource_row.resource_id;
                resources
                    .entry(resource_id)
                    .or_insert((resource_row, hint.clone()));
                events.push(permission_changed_event(
                    raw_log,
                    &hint,
                    resource_id,
                    account,
                    old_role_bitmap,
                    new_role_bitmap,
                )?);
            }
        }
    }

    let resources = resources
        .into_values()
        .map(|(resource, _)| resource)
        .collect::<Vec<_>>();
    let existing = load_existing_event_identities(pool, &events).await?;
    let inserted_by_kind = count_inserted_events_by_kind(&events, &existing);
    let synced_by_kind = count_events_by_kind(&events);
    upsert_resources(pool, &resources).await?;
    upsert_normalized_events(pool, &events).await?;

    let by_kind = synced_by_kind
        .into_iter()
        .map(|(event_kind, synced_count)| {
            let inserted_count = inserted_by_kind.get(&event_kind).copied().unwrap_or(0);
            (
                event_kind,
                EnsV2PermissionsKindSyncSummary {
                    synced_count,
                    inserted_count,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(EnsV2PermissionsSyncSummary {
        scanned_log_count,
        matched_log_count,
        total_resource_count: resources.len(),
        total_synced_count: events.len(),
        total_inserted_count: inserted_by_kind.values().sum(),
        by_kind,
    })
}

async fn remember_hint_and_resource(
    pool: &PgPool,
    raw_log: &PermissionsRawLogRow,
    hint: ResolverResourceHint,
    hints: &mut HashMap<(String, String), ResolverResourceHint>,
    resources: &mut BTreeMap<Uuid, (Resource, ResolverResourceHint)>,
) -> Result<()> {
    let key = (
        raw_log.emitting_address.clone(),
        hint.upstream_resource.clone(),
    );
    let resource = build_resource(pool, raw_log, &hint).await?;
    resources
        .entry(resource.resource_id)
        .or_insert((resource, hint.clone()));
    hints.insert(key, hint);
    Ok(())
}

fn resolver_resource_hint(
    raw_log: &PermissionsRawLogRow,
    upstream_resource: String,
    dns_encoded_name: Vec<u8>,
    selector_kind: &str,
    selector_key: Option<String>,
    selector_hash: Option<String>,
) -> Result<ResolverResourceHint> {
    let normalized_name = dns_decode(&dns_encoded_name)?;
    Ok(ResolverResourceHint {
        upstream_resource,
        logical_name_id: (!normalized_name.is_empty())
            .then(|| format!("{}:{normalized_name}", raw_log.namespace)),
        normalized_name: (!normalized_name.is_empty()).then_some(normalized_name),
        dns_encoded_name: Some(dns_encoded_name),
        selector_kind: selector_kind.to_owned(),
        selector_key,
        selector_hash,
        first_ref: raw_log.reference(),
    })
}

fn fallback_resource_hint(
    raw_log: &PermissionsRawLogRow,
    upstream_resource: String,
    is_root: bool,
) -> ResolverResourceHint {
    ResolverResourceHint {
        upstream_resource,
        logical_name_id: None,
        normalized_name: None,
        dns_encoded_name: None,
        selector_kind: if is_root { "root" } else { "unknown" }.to_owned(),
        selector_key: None,
        selector_hash: None,
        first_ref: raw_log.reference(),
    }
}

async fn build_resource(
    pool: &PgPool,
    raw_log: &PermissionsRawLogRow,
    hint: &ResolverResourceHint,
) -> Result<Resource> {
    let resource_id = resolver_permission_resource_id(
        &raw_log.chain_id,
        raw_log.emitting_contract_instance_id,
        &hint.upstream_resource,
    );
    if let Some(existing) = load_resource_including_noncanonical(pool, resource_id).await? {
        return Ok(Resource {
            resource_id: existing.resource_id,
            token_lineage_id: existing.token_lineage_id,
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: resource_provenance(raw_log, hint),
            canonicality_state: raw_log.canonicality_state,
        });
    }

    Ok(Resource {
        resource_id,
        token_lineage_id: None,
        chain_id: hint.first_ref.chain_id.clone(),
        block_hash: hint.first_ref.block_hash.clone(),
        block_number: hint.first_ref.block_number,
        provenance: resource_provenance(raw_log, hint),
        canonicality_state: raw_log.canonicality_state,
    })
}

fn permission_changed_event(
    raw_log: &PermissionsRawLogRow,
    hint: &ResolverResourceHint,
    resource_id: Uuid,
    account: String,
    old_role_bitmap: String,
    new_role_bitmap: String,
) -> Result<NormalizedEvent> {
    let effective_powers = role_bitmap_powers(&new_role_bitmap)?;
    let old_powers = role_bitmap_powers(&old_role_bitmap)?;
    let has_effective_powers = !effective_powers.is_empty();
    let fully_revoked = !has_effective_powers && !old_powers.is_empty();
    let root_resource = resource_is_root(&hint.upstream_resource);
    let changed_powers = changed_role_powers(&old_role_bitmap, &new_role_bitmap)?;

    Ok(NormalizedEvent {
        event_identity: format!(
            "ens_v2_permissions:{}:{}:{}:{}:{}:{}",
            raw_log.source_manifest_id,
            raw_log.block_hash,
            raw_log.transaction_hash,
            raw_log.log_index,
            EVENT_KIND_PERMISSION_CHANGED,
            hint.upstream_resource
        ),
        namespace: raw_log.namespace.clone(),
        logical_name_id: hint.logical_name_id.clone(),
        resource_id: Some(resource_id),
        event_kind: EVENT_KIND_PERMISSION_CHANGED.to_owned(),
        source_family: raw_log.source_family.clone(),
        manifest_version: raw_log.manifest_version,
        source_manifest_id: Some(raw_log.source_manifest_id),
        chain_id: Some(raw_log.chain_id.clone()),
        block_number: Some(raw_log.block_number),
        block_hash: Some(raw_log.block_hash.clone()),
        transaction_hash: Some(raw_log.transaction_hash.clone()),
        log_index: Some(raw_log.log_index),
        raw_fact_ref: raw_fact_ref(raw_log),
        derivation_kind: DERIVATION_KIND_ENS_V2_PERMISSIONS.to_owned(),
        canonicality_state: raw_log.canonicality_state,
        before_state: json!({
            "subject": account,
            "role_bitmap": old_role_bitmap,
            "effective_powers": old_powers,
        }),
        after_state: json!({
            "subject": account,
            "scope": {
                "kind": "resolver",
                "chain_id": raw_log.chain_id,
                "resolver_address": raw_log.emitting_address,
            },
            "effective_powers": effective_powers,
            "grant_source": has_effective_powers.then(|| json!({
                "kind": "raw_log",
                "source_event": "EACRolesChanged",
                "upstream_resource": hint.upstream_resource,
                "resolver_contract_instance_id": raw_log.emitting_contract_instance_id.to_string(),
                "root_resource": root_resource,
                "changed_powers": changed_powers.clone(),
            })).unwrap_or_else(|| json!({})),
            "revocation_source": fully_revoked.then(|| json!({
                "kind": "raw_log",
                "source_event": "EACRolesChanged",
                "upstream_resource": hint.upstream_resource,
                "resolver_contract_instance_id": raw_log.emitting_contract_instance_id.to_string(),
                "root_resource": root_resource,
                "changed_powers": changed_powers,
            })),
            "inheritance_path": if root_resource {
                json!([{
                    "kind": "resolver_root_fallback",
                    "chain_id": raw_log.chain_id,
                    "resolver_address": raw_log.emitting_address,
                    "upstream_resource": hint.upstream_resource,
                }])
            } else {
                json!([])
            },
            "transfer_behavior": {},
            "source_event": "EACRolesChanged",
            "upstream_resource": hint.upstream_resource,
            "resolver_contract_instance_id": raw_log.emitting_contract_instance_id.to_string(),
            "role_bitmap": new_role_bitmap,
            "old_role_bitmap": old_role_bitmap,
            "root_resource": root_resource,
            "selector": {
                "kind": hint.selector_kind,
                "key": hint.selector_key,
                "hash": hint.selector_hash,
                "normalized_name": hint.normalized_name,
                "dns_encoded_name": hint.dns_encoded_name.as_ref().map(|bytes| format!("0x{}", hex_string(bytes))),
            },
        }),
    })
}

fn resource_provenance(raw_log: &PermissionsRawLogRow, hint: &ResolverResourceHint) -> Value {
    json!({
        "adapter": DERIVATION_KIND_ENS_V2_PERMISSIONS,
        "chain_id": raw_log.chain_id,
        "resolver_contract_instance_id": raw_log.emitting_contract_instance_id.to_string(),
        "resolver_address": raw_log.emitting_address,
        "upstream_resource": hint.upstream_resource,
        "selector_kind": hint.selector_kind,
        "selector_key": hint.selector_key,
        "selector_hash": hint.selector_hash,
        "logical_name_id": hint.logical_name_id,
        "normalized_name": hint.normalized_name,
        "source_family": raw_log.source_family,
        "source_manifest_id": raw_log.source_manifest_id,
        "manifest_version": raw_log.manifest_version,
    })
}

fn build_permissions_observation(
    raw_log: &PermissionsRawLogRow,
) -> Result<Option<PermissionsObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_RESOURCE_SIGNATURE)) {
        let resource = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NamedResource missing resource topic")?,
        )?;
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(PermissionsObservation::NamedResource {
            resource,
            name,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_TEXT_RESOURCE_SIGNATURE)) {
        let resource = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NamedTextResource missing resource topic")?,
        )?;
        let key_hash = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("NamedTextResource missing key hash topic")?,
        )?;
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        let key = decode_dynamic_string(&raw_log.data, 1)?;
        return Ok(Some(PermissionsObservation::NamedTextResource {
            resource,
            name,
            key_hash,
            key,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_ADDR_RESOURCE_SIGNATURE)) {
        let resource = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NamedAddrResource missing resource topic")?,
        )?;
        let coin_type = decode_u256_topic_decimal(
            raw_log
                .topics
                .get(2)
                .context("NamedAddrResource missing coin type topic")?,
        )?;
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(PermissionsObservation::NamedAddrResource {
            resource,
            name,
            coin_type,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(EAC_ROLES_CHANGED_SIGNATURE)) {
        let resource = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("EACRolesChanged missing resource topic")?,
        )?;
        let account = normalize_topic_address(
            raw_log
                .topics
                .get(2)
                .context("EACRolesChanged missing account topic")?,
        )?;
        let old_role_bitmap = normalize_hex_32_word(word_at(&raw_log.data, 0)?)?;
        let new_role_bitmap = normalize_hex_32_word(word_at(&raw_log.data, 1)?)?;
        return Ok(Some(PermissionsObservation::EacRolesChanged {
            resource,
            account,
            old_role_bitmap,
            new_role_bitmap,
        }));
    }

    Ok(None)
}

async fn load_permissions_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
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

async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
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

async fn load_existing_event_identities(
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

impl PermissionsRawLogRow {
    fn reference(&self) -> PermissionRef {
        PermissionRef {
            chain_id: self.chain_id.clone(),
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            transaction_hash: self.transaction_hash.clone(),
            transaction_index: self.transaction_index,
            log_index: self.log_index,
            emitting_address: self.emitting_address.clone(),
            emitting_contract_instance_id: self.emitting_contract_instance_id,
            canonicality_state: self.canonicality_state,
            source_manifest_id: self.source_manifest_id,
            source_family: self.source_family.clone(),
            manifest_version: self.manifest_version,
        }
    }
}

fn raw_fact_ref(raw_log: &PermissionsRawLogRow) -> Value {
    json!({
        "kind": "raw_log",
        "chain_id": raw_log.chain_id,
        "block_hash": raw_log.block_hash,
        "block_number": raw_log.block_number,
        "transaction_hash": raw_log.transaction_hash,
        "transaction_index": raw_log.transaction_index,
        "log_index": raw_log.log_index,
        "emitting_address": raw_log.emitting_address,
    })
}

fn count_events_by_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

fn count_inserted_events_by_kind(
    events: &[NormalizedEvent],
    existing: &HashSet<String>,
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        if !existing.contains(&event.event_identity) {
            *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
        }
    }
    counts
}

fn empty_summary(scanned_log_count: usize) -> EnsV2PermissionsSyncSummary {
    EnsV2PermissionsSyncSummary {
        scanned_log_count,
        matched_log_count: 0,
        total_resource_count: 0,
        total_synced_count: 0,
        total_inserted_count: 0,
        by_kind: BTreeMap::new(),
    }
}

fn role_bitmap_powers(bitmap: &str) -> Result<Vec<String>> {
    let bytes = decode_hex_32(bitmap)?;
    let role_bits = [
        (0usize, "set_addr"),
        (4, "set_text"),
        (8, "set_contenthash"),
        (12, "set_pubkey"),
        (16, "set_abi"),
        (20, "set_interface"),
        (24, "set_name"),
        (28, "set_alias"),
        (32, "clear_records"),
        (124, "upgrade"),
        (128, "admin_set_addr"),
        (132, "admin_set_text"),
        (136, "admin_set_contenthash"),
        (140, "admin_set_pubkey"),
        (144, "admin_set_abi"),
        (148, "admin_set_interface"),
        (152, "admin_set_name"),
        (156, "admin_set_alias"),
        (160, "admin_clear_records"),
        (252, "admin_upgrade"),
    ];
    Ok(role_bits
        .into_iter()
        .filter_map(|(bit, power)| bit_is_set(&bytes, bit).then(|| power.to_owned()))
        .collect())
}

fn changed_role_powers(old_bitmap: &str, new_bitmap: &str) -> Result<Vec<String>> {
    let old = role_bitmap_powers(old_bitmap)?
        .into_iter()
        .collect::<HashSet<_>>();
    let new = role_bitmap_powers(new_bitmap)?
        .into_iter()
        .collect::<HashSet<_>>();
    let mut changed = old.symmetric_difference(&new).cloned().collect::<Vec<_>>();
    changed.sort();
    Ok(changed)
}

fn bit_is_set(bytes: &[u8; 32], bit: usize) -> bool {
    let byte_index = 31usize.saturating_sub(bit / 8);
    let bit_mask = 1u8 << (bit % 8);
    bytes[byte_index] & bit_mask != 0
}

fn resolver_permission_resource_id(
    chain_id: &str,
    resolver_contract_instance_id: Uuid,
    upstream_resource: &str,
) -> Uuid {
    deterministic_uuid(&format!(
        "ens-v2-resolver-resource:{chain_id}:{resolver_contract_instance_id}:{upstream_resource}"
    ))
}

fn resource_is_root(resource: &str) -> bool {
    resource == "0x0000000000000000000000000000000000000000000000000000000000000000"
}

fn decode_dynamic_string(data: &[u8], offset_word_index: usize) -> Result<String> {
    String::from_utf8(decode_dynamic_bytes(data, offset_word_index)?)
        .context("dynamic string is not valid UTF-8")
}

fn decode_dynamic_bytes(data: &[u8], offset_word_index: usize) -> Result<Vec<u8>> {
    let offset = decode_usize_word(data, offset_word_index)?;
    if data.len() < offset + 32 {
        bail!("dynamic bytes payload is missing length word");
    }
    let length = decode_usize_at(data, offset)?;
    let start = offset + 32;
    let end = start + length;
    if data.len() < end {
        bail!("dynamic bytes payload is shorter than declared length");
    }
    Ok(data[start..end].to_vec())
}

fn decode_usize_word(data: &[u8], word_index: usize) -> Result<usize> {
    let word = word_at(data, word_index)?;
    decode_usize(word)
}

fn decode_usize_at(data: &[u8], offset: usize) -> Result<usize> {
    if data.len() < offset + 32 {
        bail!("ABI word offset is outside payload");
    }
    decode_usize(&data[offset..offset + 32])
}

fn decode_usize(word: &[u8]) -> Result<usize> {
    if word.len() != 32 {
        bail!("ABI word must be exactly 32 bytes");
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word exceeds supported usize width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    usize::try_from(u64::from_be_bytes(bytes)).context("ABI word does not fit in usize")
}

fn word_at(data: &[u8], word_index: usize) -> Result<&[u8]> {
    let start = word_index
        .checked_mul(32)
        .context("ABI word index overflow")?;
    let end = start + 32;
    data.get(start..end)
        .with_context(|| format!("ABI data missing word {word_index}"))
}

fn normalize_hex_32(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    let normalized = if normalized.starts_with("0x") {
        normalized
    } else {
        format!("0x{normalized}")
    };
    if normalized.len() != 66 {
        bail!("expected 32-byte hex value, got {normalized}");
    }
    Ok(normalized)
}

fn normalize_hex_32_word(word: &[u8]) -> Result<String> {
    if word.len() != 32 {
        bail!("ABI word must be exactly 32 bytes");
    }
    Ok(format!("0x{}", hex_string(word)))
}

fn decode_hex_32(value: &str) -> Result<[u8; 32]> {
    let normalized = normalize_hex_32(value)?;
    let mut output = [0u8; 32];
    for (index, chunk) in normalized[2..].as_bytes().chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk).context("hex chunk must be UTF-8")?;
        output[index] =
            u8::from_str_radix(hex, 16).with_context(|| format!("invalid hex byte {hex}"))?;
    }
    Ok(output)
}

fn normalize_topic_address(value: &str) -> Result<String> {
    let normalized = normalize_hex_32(value)?;
    Ok(format!("0x{}", &normalized[26..]))
}

fn decode_u256_topic_decimal(value: &str) -> Result<String> {
    let bytes = decode_hex_32(value)?;
    Ok(decimal_string_from_be_bytes(&bytes))
}

fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown canonicality_state value {value}"),
    }
}

fn dns_decode(bytes: &[u8]) -> Result<String> {
    let mut labels = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        let length = bytes[index] as usize;
        index += 1;
        if length == 0 {
            if index != bytes.len() {
                bail!("DNS-encoded name has trailing bytes");
            }
            return Ok(labels.join(".").to_ascii_lowercase());
        }
        let end = index + length;
        if end > bytes.len() {
            bail!("DNS-encoded name label exceeds payload length");
        }
        labels.push(
            String::from_utf8(bytes[index..end].to_vec())
                .context("DNS-encoded label is not valid UTF-8")?,
        );
        index = end;
    }
    bail!("DNS-encoded name is missing root label")
}

fn decimal_string_from_be_bytes(bytes: &[u8]) -> String {
    let mut digits = vec![0u8];
    for byte in bytes {
        let mut carry = *byte as u32;
        for digit in digits.iter_mut().rev() {
            let value = (*digit as u32) * 256 + carry;
            *digit = (value % 10) as u8;
            carry = value / 10;
        }
        while carry > 0 {
            digits.insert(0, (carry % 10) as u8);
            carry /= 10;
        }
    }
    let value = digits
        .into_iter()
        .skip_while(|digit| *digit == 0)
        .map(|digit| char::from(b'0' + digit))
        .collect::<String>();
    if value.is_empty() {
        "0".to_owned()
    } else {
        value
    }
}

fn keccak_signature_hex(signature: &str) -> String {
    format!("0x{}", hex_string(keccak256_bytes(signature.as_bytes())))
}

fn keccak256_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

fn deterministic_uuid(seed: &str) -> Uuid {
    let mut digest = Keccak256::new();
    digest.update(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
