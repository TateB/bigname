use super::registrar_history::empty_preloaded_history;
use super::*;

pub(super) fn restricted_replay_labelhashes(
    raw_logs: &[AuthorityRawLogRow],
    known_names_by_namehash: &HashMap<String, NameMetadata>,
    namehash_to_labelhash: &HashMap<String, String>,
    event_topics: &AuthorityEventTopics,
) -> Result<Vec<String>> {
    let mut labelhashes = BTreeSet::<String>::new();
    for raw_log in raw_logs {
        let Some(observation) = build_authority_observation(raw_log, event_topics)? else {
            continue;
        };
        if let Some(namehash) = observation_namehash(&observation) {
            if let Some(labelhash) = namehash_to_labelhash.get(namehash) {
                labelhashes.insert(labelhash.to_ascii_lowercase());
            } else if let Some(name) = known_names_by_namehash.get(namehash)
                && let Some(labelhash) = name.labelhashes.first()
            {
                labelhashes.insert(labelhash.to_ascii_lowercase());
            }
        } else {
            labelhashes.insert(observation_labelhash(&observation).to_ascii_lowercase());
        }
    }
    Ok(labelhashes.into_iter().collect())
}

pub(super) fn resolver_state_scopes_for_selected_names(
    raw_logs: &[AuthorityRawLogRow],
    known_names_by_namehash: &mut HashMap<String, NameMetadata>,
    labelhashes: &[String],
    event_topics: &AuthorityEventTopics,
) -> Result<Vec<ResolverStateScope>> {
    if labelhashes.is_empty() {
        return Ok(Vec::new());
    }
    for raw_log in raw_logs {
        let Some(observation) = build_authority_observation(raw_log, event_topics)? else {
            continue;
        };
        let name = match observation {
            AuthorityObservation::RegistrationGranted(value) => {
                Some(observe_registrar_name_with_reference(
                    &value.label,
                    &value.reference,
                    ENS_NORMALIZER_VERSION,
                )?)
            }
            AuthorityObservation::RegistrationRenewed(value) => {
                Some(observe_registrar_name_with_reference(
                    &value.label,
                    &value.reference,
                    ENS_NORMALIZER_VERSION,
                )?)
            }
            AuthorityObservation::WrapperNameWrapped(value) => Some(value.name),
            _ => None,
        };
        if let Some(name) = name {
            known_names_by_namehash
                .entry(name.namehash.clone())
                .or_insert(name);
        }
    }
    let selected_labelhashes = labelhashes.iter().cloned().collect::<BTreeSet<_>>();
    let mut scopes = BTreeMap::<String, ResolverStateScope>::new();
    for name in known_names_by_namehash.values() {
        let Some(labelhash) = name.labelhashes.first() else {
            continue;
        };
        if !selected_labelhashes.contains(&labelhash.to_ascii_lowercase()) {
            continue;
        }
        let registry_source_family = match name.namespace.as_str() {
            "basenames" => SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            _ => SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        };
        scopes
            .entry(name.logical_name_id.clone())
            .or_insert_with(|| ResolverStateScope {
                logical_name_id: name.logical_name_id.clone(),
                namehash: name.namehash.to_ascii_lowercase(),
                registry_source_family: registry_source_family.to_owned(),
            });
    }
    Ok(scopes.into_values().collect())
}

pub(super) fn preload_latent_resolver_histories(
    scopes: &[ResolverStateScope],
    resolver_state: &HashMap<String, String>,
    histories: &mut BTreeMap<String, NameHistory>,
    known_names_by_namehash: &HashMap<String, NameMetadata>,
    namehash_to_labelhash: &mut HashMap<String, String>,
) {
    for scope in scopes {
        let Some(resolver) = resolver_state.get(&scope.logical_name_id) else {
            continue;
        };
        let Some(name) = known_names_by_namehash.get(&scope.namehash) else {
            continue;
        };
        let Some(labelhash) = name.labelhashes.first().cloned() else {
            continue;
        };
        namehash_to_labelhash
            .entry(name.namehash.clone())
            .or_insert_with(|| labelhash.clone());
        let history = histories
            .entry(name.namehash.clone())
            .or_insert_with(|| empty_preloaded_history(labelhash, Some(name.clone())));
        if history.name.is_none() {
            history.name = Some(name.clone());
        }
        if history.current_resolver.is_none() {
            history.current_resolver = Some(resolver.clone());
        }
    }
}

pub(super) async fn load_latest_resolver_state_before_block(
    pool: &PgPool,
    logical_name_ids: &[String],
    boundary_block: i64,
) -> Result<HashMap<String, String>> {
    if logical_name_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT ON (logical_name_id)
            logical_name_id,
            after_state->>'resolver' AS resolver
        FROM normalized_events
        WHERE logical_name_id = ANY($1::TEXT[])
          AND block_number < $2
          AND event_kind = $3
          AND canonicality_state {CANONICALITY_STATE_FILTER}
        ORDER BY logical_name_id, block_number DESC, log_index DESC NULLS LAST, normalized_event_id DESC
        "#
    ))
    .bind(logical_name_ids)
    .bind(boundary_block)
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .fetch_all(pool)
    .await
    .context("failed to preload latest resolver state before restricted replay")?;

    let mut state = HashMap::new();
    for row in rows {
        let Some(resolver) = row.try_get::<Option<String>, _>("resolver")? else {
            continue;
        };
        state.insert(row.try_get("logical_name_id")?, resolver);
    }
    Ok(state)
}

pub(super) async fn load_latest_registry_resolver_raw_state_before_block(
    pool: &PgPool,
    chain: &str,
    scopes: &[ResolverStateScope],
    boundary_block: i64,
    event_topics: &AuthorityEventTopics,
) -> Result<HashMap<String, String>> {
    if scopes.is_empty() {
        return Ok(HashMap::new());
    }
    let Some(new_resolver_topic0) = event_topics.optional_topic0(NEW_RESOLVER_SIGNATURE) else {
        return Ok(HashMap::new());
    };

    let logical_name_ids = scopes
        .iter()
        .map(|scope| scope.logical_name_id.clone())
        .collect::<Vec<_>>();
    let namehashes = scopes
        .iter()
        .map(|scope| scope.namehash.clone())
        .collect::<Vec<_>>();
    let registry_source_families = scopes
        .iter()
        .map(|scope| scope.registry_source_family.clone())
        .collect::<Vec<_>>();

    let rows = sqlx::query(&format!(
        r#"
        WITH scope AS (
            SELECT *
            FROM UNNEST($2::TEXT[], $3::TEXT[], $4::TEXT[]) AS scope(
                logical_name_id,
                namehash,
                registry_source_family
            )
        ),
        registry_emitters AS (
            SELECT DISTINCT
                lower(address.address) AS address,
                COALESCE(address.active_from_block_number, 0) AS active_from_block_number,
                COALESCE(address.active_to_block_number, 9223372036854775807) AS active_to_block_number,
                manifest.source_family
            FROM contract_instance_addresses address
            JOIN manifest_contract_instances manifest_contract
              ON manifest_contract.contract_instance_id = address.contract_instance_id
             AND manifest_contract.manifest_id = address.source_manifest_id
            JOIN manifest_versions manifest
              ON manifest.manifest_id = address.source_manifest_id
            WHERE address.chain_id = $1
              AND address.deactivated_at IS NULL
        )
        SELECT DISTINCT ON (scope.logical_name_id)
            scope.logical_name_id,
            resolver_log.data
        FROM scope
        JOIN LATERAL (
            SELECT
                log.data,
                log.block_number,
                log.transaction_index,
                log.log_index,
                log.raw_log_id
            FROM raw_logs log
            WHERE log.chain_id = $1
              AND log.topics[1] = $6
              AND lower(log.topics[2]) = scope.namehash
              AND log.block_number < $5
              AND log.canonicality_state {CANONICALITY_STATE_FILTER}
              AND EXISTS (
                  SELECT 1
                  FROM registry_emitters emitter
                  WHERE emitter.source_family = scope.registry_source_family
                    AND emitter.address = log.emitting_address
                    AND log.block_number BETWEEN emitter.active_from_block_number
                        AND LEAST(emitter.active_to_block_number, $5 - 1)
              )
            ORDER BY log.block_number DESC, log.transaction_index DESC, log.log_index DESC, log.raw_log_id DESC
            LIMIT 1
        ) resolver_log ON TRUE
        ORDER BY scope.logical_name_id, resolver_log.block_number DESC, resolver_log.transaction_index DESC, resolver_log.log_index DESC, resolver_log.raw_log_id DESC
        "#
    ))
    .bind(chain)
    .bind(logical_name_ids)
    .bind(namehashes)
    .bind(registry_source_families)
    .bind(boundary_block)
    .bind(new_resolver_topic0)
    .fetch_all(pool)
    .await
    .context("failed to preload latest registry resolver raw state before restricted replay")?;

    let mut state = HashMap::new();
    for row in rows {
        let data: Vec<u8> = row.try_get("data")?;
        state.insert(
            row.try_get("logical_name_id")?,
            decode_owner_address(&data)?,
        );
    }
    Ok(state)
}

pub(super) async fn load_selected_registry_resolver_state_before_replay(
    pool: &PgPool,
    logical_name_ids: &[String],
    raw_logs: &[AuthorityRawLogRow],
) -> Result<HashMap<String, String>> {
    if logical_name_ids.is_empty() || raw_logs.is_empty() {
        return Ok(HashMap::new());
    }

    let block_hashes = raw_logs
        .iter()
        .map(|raw_log| raw_log.block_hash.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT ON (event.logical_name_id)
            event.logical_name_id,
            event.after_state->>'resolver' AS resolver
        FROM normalized_events event
        JOIN resources resource
          ON resource.resource_id = event.resource_id
        WHERE event.logical_name_id = ANY($1::TEXT[])
          AND event.block_hash = ANY($2::TEXT[])
          AND event.event_kind = $3
          AND event.transaction_hash IS NULL
          AND event.log_index IS NULL
          AND event.after_state->>'source_event' = $4
          AND resource.provenance->>'authority_kind' = 'registry_only'
          AND event.canonicality_state {CANONICALITY_STATE_FILTER}
          AND resource.canonicality_state {CANONICALITY_STATE_FILTER}
        ORDER BY event.logical_name_id, event.block_number DESC, event.normalized_event_id DESC
        "#
    ))
    .bind(logical_name_ids)
    .bind(&block_hashes)
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(EVENT_KIND_AUTHORITY_EPOCH_CHANGED)
    .fetch_all(pool)
    .await
    .context("failed to preload selected registry resolver state before restricted replay")?;

    let mut state = HashMap::new();
    for row in rows {
        let Some(resolver) = row.try_get::<Option<String>, _>("resolver")? else {
            continue;
        };
        state.insert(row.try_get("logical_name_id")?, resolver);
    }
    Ok(state)
}

pub(super) async fn load_latest_record_versions_before_block(
    pool: &PgPool,
    logical_name_ids: &[String],
    boundary_block: i64,
) -> Result<HashMap<String, i64>> {
    if logical_name_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT ON (logical_name_id)
            logical_name_id,
            after_state->>'record_version' AS record_version
        FROM normalized_events
        WHERE logical_name_id = ANY($1::TEXT[])
          AND block_number < $2
          AND event_kind = $3
          AND canonicality_state {CANONICALITY_STATE_FILTER}
        ORDER BY logical_name_id, block_number DESC, log_index DESC NULLS LAST, normalized_event_id DESC
        "#
    ))
    .bind(logical_name_ids)
    .bind(boundary_block)
    .bind(EVENT_KIND_RECORD_VERSION_CHANGED)
    .fetch_all(pool)
    .await
    .context("failed to preload latest record versions before restricted replay")?;

    let mut state = HashMap::new();
    for row in rows {
        let Some(record_version) = row.try_get::<Option<String>, _>("record_version")? else {
            continue;
        };
        if let Ok(record_version) = record_version.parse::<i64>() {
            state.insert(row.try_get("logical_name_id")?, record_version);
        }
    }
    Ok(state)
}
