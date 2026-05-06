use super::*;
use bigname_storage::sql_row;

#[derive(Clone, Debug, Default)]
pub(super) struct PreloadedRegistrarState {
    pub(super) expiry: Option<OffsetDateTime>,
    pub(super) registrant: Option<String>,
    pub(super) authority_key: Option<String>,
    pub(super) labelhash: Option<String>,
    pub(super) start_ref: Option<ObservationRef>,
}

#[derive(Clone, Debug, Default)]
struct PreloadedWrapperState {
    owner: Option<String>,
    fuses: Option<i64>,
    expiry: Option<OffsetDateTime>,
}

#[derive(Clone, Debug)]
struct RegistrarStateScope {
    logical_name_id: String,
    lower_block_number: i64,
}

#[derive(Clone, Debug)]
struct ResolverStateScope {
    logical_name_id: String,
    namehash: String,
    registry_source_family: String,
}

pub(super) async fn preload_restricted_name_histories(
    pool: &PgPool,
    chain: &str,
    raw_logs: &[AuthorityRawLogRow],
    histories: &mut BTreeMap<String, NameHistory>,
    known_names_by_namehash: &mut HashMap<String, NameMetadata>,
    known_name_refs_by_namehash: &mut HashMap<String, ObservationRef>,
    namehash_to_labelhash: &mut HashMap<String, String>,
    block_index: &CanonicalBlockIndex,
    event_topics: &AuthorityEventTopics,
) -> Result<()> {
    let Some(first_log) = raw_logs.first() else {
        return Ok(());
    };
    let boundary_block = first_log.block_number;
    let boundary_timestamp = first_log.block_timestamp;
    let labelhashes = restricted_replay_labelhashes(
        raw_logs,
        known_names_by_namehash,
        namehash_to_labelhash,
        event_topics,
    )?;
    if labelhashes.is_empty() {
        return Ok(());
    }

    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (surface.logical_name_id)
            surface.namespace,
            surface.logical_name_id,
            surface.input_name,
            surface.canonical_display_name,
            surface.normalized_name,
            surface.dns_encoded_name,
            surface.namehash,
            surface.labelhashes,
            surface.normalizer_version,
            binding.surface_binding_id,
            binding.resource_id,
            binding.binding_kind,
            binding.active_from,
            binding.active_to,
            binding.chain_id AS binding_chain_id,
            binding.block_hash AS binding_block_hash,
            binding.block_number AS binding_block_number,
            binding.canonicality_state::TEXT AS binding_canonicality_state,
            resource.provenance AS resource_provenance
        FROM name_surfaces surface
        JOIN surface_bindings binding
          ON binding.logical_name_id = surface.logical_name_id
        JOIN resources resource
          ON resource.resource_id = binding.resource_id
        WHERE binding.chain_id = $1
          AND lower(surface.labelhashes[1]) = ANY($2::TEXT[])
          AND binding.active_from <= $3
          AND binding.block_number < $4
          AND (
              binding.active_to IS NULL
              OR binding.active_to >= $3
          )
          AND surface.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND binding.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY
            surface.logical_name_id,
            binding.active_from DESC,
            binding.block_number DESC,
            binding.surface_binding_id
        "#,
    )
    .bind(chain)
    .bind(&labelhashes)
    .bind(boundary_timestamp)
    .bind(boundary_block)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to preload ENSv1 restricted replay histories for chain {chain} before block {boundary_block}"
        )
    })?;

    let registrar_scopes = rows
        .iter()
        .map(|row| RegistrarStateScope {
            logical_name_id: row.get("logical_name_id"),
            lower_block_number: row.get("binding_block_number"),
        })
        .collect::<Vec<_>>();
    let logical_name_ids = registrar_scopes
        .iter()
        .map(|scope| scope.logical_name_id.clone())
        .collect::<Vec<_>>();
    let mut registrar_state =
        load_latest_registrar_state_before_block(pool, &registrar_scopes, boundary_block).await?;
    let selected_registrar_state = load_selected_registrar_state_before_replay(
        pool,
        &logical_name_ids,
        raw_logs,
        event_topics,
    )
    .await?;
    for (logical_name_id, selected_state) in selected_registrar_state {
        let state = registrar_state.entry(logical_name_id).or_default();
        if selected_state.expiry.is_some() {
            state.expiry = selected_state.expiry;
        }
        if selected_state.registrant.is_some() {
            state.registrant = selected_state.registrant;
        }
        if selected_state.authority_key.is_some() {
            state.authority_key = selected_state.authority_key;
            state.labelhash = selected_state.labelhash;
            state.start_ref = selected_state.start_ref;
        }
    }
    let selected_wrapper_state =
        load_selected_wrapper_state_before_replay(pool, &logical_name_ids, raw_logs).await?;
    let resolver_scopes = resolver_state_scopes_for_selected_names(
        raw_logs,
        known_names_by_namehash,
        &labelhashes,
        event_topics,
    )?;
    let mut resolver_state =
        load_latest_resolver_state_before_block(pool, &logical_name_ids, boundary_block).await?;
    let raw_resolver_state = load_latest_registry_resolver_raw_state_before_block(
        pool,
        chain,
        &resolver_scopes,
        boundary_block,
        event_topics,
    )
    .await?;
    for (logical_name_id, resolver) in raw_resolver_state {
        resolver_state.entry(logical_name_id).or_insert(resolver);
    }
    let selected_resolver_state =
        load_selected_registry_resolver_state_before_replay(pool, &logical_name_ids, raw_logs)
            .await?;
    for (logical_name_id, resolver) in selected_resolver_state {
        resolver_state.entry(logical_name_id).or_insert(resolver);
    }
    preload_latent_resolver_histories(
        &resolver_scopes,
        &resolver_state,
        histories,
        known_names_by_namehash,
        namehash_to_labelhash,
    );
    let record_versions =
        load_latest_record_versions_before_block(pool, &logical_name_ids, boundary_block).await?;
    let preload_block_index = block_index_with_preloaded_registrar_release_boundaries(
        pool,
        chain,
        &rows,
        &registrar_state,
        block_index,
    )
    .await?;

    for row in rows {
        let name = name_metadata_from_preload_row(&row)?;
        let Some(labelhash) = name.labelhashes.first().cloned() else {
            continue;
        };
        known_names_by_namehash
            .entry(name.namehash.clone())
            .or_insert_with(|| name.clone());
        namehash_to_labelhash
            .entry(name.namehash.clone())
            .or_insert_with(|| labelhash.clone());

        let logical_name_id = name.logical_name_id.clone();
        let resource_provenance: Value = sql_row::get(&row, "resource_provenance")?;
        let active_from = row
            .try_get("active_from")
            .context("missing binding active_from")?;
        let active_to = row
            .try_get("active_to")
            .context("missing binding active_to")?;
        let binding_ref = BoundaryRef {
            chain_id: sql_row::get(&row, "binding_chain_id")?,
            block_hash: sql_row::get(&row, "binding_block_hash")?,
            block_number: sql_row::get(&row, "binding_block_number")?,
            block_timestamp: active_from,
            canonicality_state: sql_row::get(&row, "binding_canonicality_state")?,
            namespace: name.namespace.clone(),
        };
        let surface_binding_id = sql_row::get(&row, "surface_binding_id")?;
        let resource_id = sql_row::get(&row, "resource_id")?;

        let history = histories
            .entry(name.namehash.clone())
            .or_insert_with(|| empty_preloaded_history(labelhash.clone(), Some(name.clone())));
        if history.name.is_none() {
            history.name = Some(name.clone());
        }
        known_name_refs_by_namehash
            .entry(name.namehash.clone())
            .or_insert_with(|| observation_ref_from_boundary(&binding_ref, None, None, None));

        if let Some(resolver) = resolver_state.get(&logical_name_id) {
            history.current_resolver = Some(resolver.clone());
        }
        if let Some(record_version) = record_versions.get(&logical_name_id) {
            history.current_record_version = Some(*record_version);
        }

        let authority_kind = resource_provenance
            .get("authority_kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match authority_kind {
            "registrar" => preload_registrar_history(
                history,
                &resource_provenance,
                &binding_ref,
                surface_binding_id,
                active_to,
                registrar_state.get(&logical_name_id),
                &preload_block_index,
            )?,
            "wrapper" => preload_wrapper_history(
                history,
                &resource_provenance,
                &binding_ref,
                surface_binding_id,
                &selected_wrapper_state,
            )?,
            "registry_only" => {
                preload_registry_history(
                    history,
                    &resource_provenance,
                    &binding_ref,
                    surface_binding_id,
                    resource_id,
                );
                preload_selected_registrar_lease(
                    history,
                    registrar_state.get(&logical_name_id),
                    &preload_block_index,
                )?;
            }
            _ => {}
        }
    }

    let selected_registrar_logical_name_ids = registrar_state
        .iter()
        .filter_map(|(logical_name_id, state)| {
            state
                .authority_key
                .is_some()
                .then(|| logical_name_id.clone())
        })
        .collect::<Vec<_>>();
    let selected_registrar_names =
        load_name_metadata_by_logical_name_ids(pool, &selected_registrar_logical_name_ids).await?;
    for (logical_name_id, state) in &registrar_state {
        if state.authority_key.is_none() {
            continue;
        }
        let Some(name) = selected_registrar_names.get(logical_name_id) else {
            continue;
        };
        let Some(labelhash) = name.labelhashes.first().cloned() else {
            continue;
        };
        known_names_by_namehash
            .entry(name.namehash.clone())
            .or_insert_with(|| name.clone());
        if let Some(start_ref) = state.start_ref.clone() {
            known_name_refs_by_namehash
                .entry(name.namehash.clone())
                .or_insert(start_ref);
        }
        namehash_to_labelhash
            .entry(name.namehash.clone())
            .or_insert_with(|| labelhash.clone());
        let history = histories
            .entry(name.namehash.clone())
            .or_insert_with(|| empty_preloaded_history(labelhash, Some(name.clone())));
        if history.name.is_none() {
            history.name = Some(name.clone());
        }
        preload_selected_registrar_lease(history, Some(state), &preload_block_index)?;
    }

    Ok(())
}

async fn block_index_with_preloaded_registrar_release_boundaries(
    pool: &PgPool,
    chain: &str,
    rows: &[sqlx::postgres::PgRow],
    registrar_state: &HashMap<String, PreloadedRegistrarState>,
    block_index: &CanonicalBlockIndex,
) -> Result<CanonicalBlockIndex> {
    let Some(replay_head) = block_index.blocks.last() else {
        return Ok(block_index.clone());
    };
    let mut release_timestamps = Vec::new();
    let mut release_namespaces = Vec::new();

    for row in rows {
        let resource_provenance: Value = sql_row::get(&row, "resource_provenance")?;
        if resource_provenance
            .get("authority_kind")
            .and_then(Value::as_str)
            != Some("registrar")
        {
            continue;
        }

        let logical_name_id: String = sql_row::get(&row, "logical_name_id")?;
        let expiry = if let Some(expiry) = registrar_state
            .get(&logical_name_id)
            .and_then(|state| state.expiry)
        {
            expiry
        } else {
            let active_to = row
                .try_get("active_to")
                .context("missing binding active_to")?;
            let expiry =
                registrar_expiry_from_provenance_or_binding_end(&resource_provenance, active_to)?;
            OffsetDateTime::from_unix_timestamp(expiry)
                .context("preloaded registrar expiry is not a valid unix timestamp")?
        };
        let release_timestamp = release_after_grace(expiry)?;
        if release_timestamp <= replay_head.block_timestamp {
            release_timestamps.push(release_timestamp);
            release_namespaces.push(sql_row::get(&row, "namespace")?);
        }
    }

    if release_timestamps.is_empty() {
        return Ok(block_index.clone());
    }

    let release_blocks = load_release_boundary_blocks_for_timestamps(
        pool,
        chain,
        &release_timestamps,
        &release_namespaces,
        replay_head,
    )
    .await?;
    if release_blocks.is_empty() {
        return Ok(block_index.clone());
    }

    let mut blocks = block_index.blocks.clone();
    blocks.extend(release_blocks);
    sort_and_dedup_blocks(&mut blocks);
    Ok(CanonicalBlockIndex { blocks })
}

async fn load_release_boundary_blocks_for_timestamps(
    pool: &PgPool,
    chain: &str,
    release_timestamps: &[OffsetDateTime],
    release_namespaces: &[String],
    replay_head: &RawBlockSnapshot,
) -> Result<Vec<RawBlockSnapshot>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (requested.release_timestamp, requested.namespace)
            rb.chain_id,
            rb.block_hash,
            rb.block_number,
            rb.block_timestamp,
            rb.canonicality_state::TEXT AS canonicality_state
        FROM unnest($2::TIMESTAMPTZ[], $3::TEXT[]) AS requested(
            release_timestamp,
            namespace
        )
        JOIN LATERAL (
            SELECT
                chain_id,
                block_hash,
                block_number,
                block_timestamp,
                canonicality_state
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_timestamp >= requested.release_timestamp
              AND block_timestamp <= $4
              AND block_number <= $5
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            ORDER BY block_timestamp, block_number
            LIMIT 1
        ) rb ON TRUE
        ORDER BY requested.release_timestamp, requested.namespace, rb.block_timestamp, rb.block_number
        "#,
    )
    .bind(chain)
    .bind(release_timestamps)
    .bind(release_namespaces)
    .bind(replay_head.block_timestamp)
    .bind(replay_head.block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load ENSv1 preloaded registrar release boundary blocks for chain {chain}"
        )
    })?;

    rows.into_iter().map(raw_block_snapshot_from_row).collect()
}

fn sort_and_dedup_blocks(blocks: &mut Vec<RawBlockSnapshot>) {
    blocks.sort_by(|left, right| {
        left.block_number
            .cmp(&right.block_number)
            .then(left.block_hash.cmp(&right.block_hash))
    });
    blocks.dedup_by(|left, right| {
        left.chain_id == right.chain_id
            && left.block_hash == right.block_hash
            && left.block_number == right.block_number
    });
}

fn raw_block_snapshot_from_row(row: sqlx::postgres::PgRow) -> Result<RawBlockSnapshot> {
    Ok(RawBlockSnapshot {
        chain_id: sql_row::get(&row, "chain_id")?,
        block_hash: sql_row::get(&row, "block_hash")?,
        block_number: sql_row::get(&row, "block_number")?,
        block_timestamp: sql_row::get(&row, "block_timestamp")?,
        canonicality_state: sql_row::get(&row, "canonicality_state")?,
    })
}

async fn load_latest_registrar_state_before_block(
    pool: &PgPool,
    scopes: &[RegistrarStateScope],
    boundary_block: i64,
) -> Result<HashMap<String, PreloadedRegistrarState>> {
    if scopes.is_empty() {
        return Ok(HashMap::new());
    }
    let logical_name_ids = scopes
        .iter()
        .map(|scope| scope.logical_name_id.clone())
        .collect::<Vec<_>>();
    let lower_block_numbers = scopes
        .iter()
        .map(|scope| scope.lower_block_number)
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        WITH scope AS (
            SELECT *
            FROM UNNEST($1::TEXT[], $2::BIGINT[]) AS scope(logical_name_id, lower_block_number)
        ),
        candidates AS (
            SELECT
                event.logical_name_id,
                CASE
                    WHEN event.event_kind IN ($4, $5, $6)
                    THEN (event.after_state->>'expiry')::BIGINT
                    ELSE NULL
                END AS expiry,
                CASE
                    WHEN event.event_kind = $4 THEN event.after_state->>'registrant'
                    WHEN event.event_kind = $7 THEN event.after_state->>'to'
                    ELSE NULL
                END AS registrant,
                event.block_number,
                COALESCE(event.log_index, -1) AS log_index,
                event.normalized_event_id
            FROM normalized_events event
            JOIN scope
              ON scope.logical_name_id = event.logical_name_id
            WHERE event.block_number >= scope.lower_block_number
              AND event.block_number < $3
              AND event.event_kind IN ($4, $5, $6, $7)
              AND event.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        )
        SELECT
            logical_name_id,
            (
                ARRAY_AGG(
                    expiry
                    ORDER BY block_number DESC, log_index DESC, normalized_event_id DESC
                ) FILTER (WHERE expiry IS NOT NULL)
            )[1] AS expiry,
            (
                ARRAY_AGG(
                    registrant
                    ORDER BY block_number DESC, log_index DESC, normalized_event_id DESC
                ) FILTER (WHERE registrant IS NOT NULL)
            )[1] AS registrant
        FROM candidates
        GROUP BY logical_name_id
        "#,
    )
    .bind(logical_name_ids)
    .bind(lower_block_numbers)
    .bind(boundary_block)
    .bind(EVENT_KIND_REGISTRATION_GRANTED)
    .bind(EVENT_KIND_REGISTRATION_RENEWED)
    .bind(EVENT_KIND_EXPIRY_CHANGED)
    .bind(EVENT_KIND_TOKEN_CONTROL_TRANSFERRED)
    .fetch_all(pool)
    .await
    .context("failed to preload latest registrar state before restricted replay")?;

    let mut state = HashMap::new();
    for row in rows {
        let expiry = row
            .try_get::<Option<i64>, _>("expiry")?
            .map(|value| {
                OffsetDateTime::from_unix_timestamp(value)
                    .context("preloaded registrar expiry is not a valid unix timestamp")
            })
            .transpose()?;
        let registrant = row.try_get("registrant")?;
        state.insert(
            row.try_get("logical_name_id")?,
            PreloadedRegistrarState {
                expiry,
                registrant,
                authority_key: None,
                labelhash: None,
                start_ref: None,
            },
        );
    }
    Ok(state)
}

async fn load_selected_registrar_state_before_replay(
    pool: &PgPool,
    _logical_name_ids: &[String],
    raw_logs: &[AuthorityRawLogRow],
    event_topics: &AuthorityEventTopics,
) -> Result<HashMap<String, PreloadedRegistrarState>> {
    if raw_logs.is_empty() {
        return Ok(HashMap::new());
    }

    let event_identities = selected_registrar_event_identities(raw_logs, event_topics)?;
    if event_identities.is_empty() {
        return Ok(HashMap::new());
    }
    let block_timestamps = raw_logs
        .iter()
        .map(|raw_log| (raw_log.block_hash.clone(), raw_log.block_timestamp))
        .collect::<HashMap<_, _>>();
    let rows = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT
                event.logical_name_id,
                CASE
                    WHEN event.event_kind IN ($2, $3)
                    THEN (event.before_state->>'expiry')::BIGINT
                    ELSE NULL
                END AS expiry,
                CASE
                    WHEN event.event_kind = $4 THEN event.before_state->>'from'
                    ELSE NULL
                END AS registrant,
                CASE
                    WHEN resource.provenance->>'authority_kind' = 'registrar'
                    THEN resource.provenance->>'authority_key'
                    ELSE NULL
                END AS authority_key,
                CASE
                    WHEN resource.provenance->>'authority_kind' = 'registrar'
                    THEN COALESCE(resource.provenance->>'labelhash', event.after_state->>'labelhash')
                    ELSE NULL
                END AS labelhash,
                event.chain_id,
                event.block_hash,
                event.block_number,
                event.transaction_hash,
                event.log_index,
                event.namespace,
                event.source_manifest_id,
                event.source_family,
                event.manifest_version,
                event.canonicality_state::TEXT AS canonicality_state,
                event.normalized_event_id
            FROM normalized_events event
            LEFT JOIN resources resource
              ON resource.resource_id = event.resource_id
            WHERE event.event_identity = ANY($1::TEXT[])
              AND event.event_kind IN ($2, $3, $4)
              AND event.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        )
        SELECT
            logical_name_id,
            (
                ARRAY_AGG(
                    expiry
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE expiry IS NOT NULL)
            )[1] AS expiry,
            (
                ARRAY_AGG(
                    registrant
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE registrant IS NOT NULL)
            )[1] AS registrant
            ,
            (
                ARRAY_AGG(
                    authority_key
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS authority_key,
            (
                ARRAY_AGG(
                    labelhash
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS labelhash,
            (
                ARRAY_AGG(
                    chain_id
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS reference_chain_id,
            (
                ARRAY_AGG(
                    block_hash
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS reference_block_hash,
            (
                ARRAY_AGG(
                    block_number
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS reference_block_number,
            (
                ARRAY_AGG(
                    transaction_hash
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS reference_transaction_hash,
            (
                ARRAY_AGG(
                    log_index
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS reference_log_index,
            (
                ARRAY_AGG(
                    namespace
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS reference_namespace,
            (
                ARRAY_AGG(
                    source_manifest_id
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS reference_source_manifest_id,
            (
                ARRAY_AGG(
                    source_family
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS reference_source_family,
            (
                ARRAY_AGG(
                    manifest_version
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS reference_manifest_version,
            (
                ARRAY_AGG(
                    canonicality_state
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS reference_canonicality_state
        FROM candidates
        GROUP BY logical_name_id
        "#,
    )
    .bind(&event_identities)
    .bind(EVENT_KIND_REGISTRATION_RENEWED)
    .bind(EVENT_KIND_EXPIRY_CHANGED)
    .bind(EVENT_KIND_TOKEN_CONTROL_TRANSFERRED)
    .fetch_all(pool)
    .await
    .context("failed to preload selected registrar state before restricted replay")?;

    let mut state = HashMap::new();
    for row in rows {
        let expiry = row
            .try_get::<Option<i64>, _>("expiry")?
            .map(|value| {
                OffsetDateTime::from_unix_timestamp(value)
                    .context("preloaded selected registrar expiry is not a valid unix timestamp")
            })
            .transpose()?;
        let registrant = row.try_get("registrant")?;
        let authority_key: Option<String> = row.try_get("authority_key")?;
        let labelhash: Option<String> = row.try_get("labelhash")?;
        let reference_block_hash: Option<String> = row.try_get("reference_block_hash")?;
        let start_ref = if authority_key.is_some() {
            let block_hash = reference_block_hash
                .clone()
                .context("selected registrar replay state is missing reference block hash")?;
            Some(ObservationRef {
                chain_id: row
                    .try_get::<Option<String>, _>("reference_chain_id")?
                    .context("selected registrar replay state is missing reference chain")?,
                block_timestamp: *block_timestamps.get(&block_hash).context(
                    "selected registrar replay state is missing raw log block timestamp",
                )?,
                block_hash,
                block_number: row
                    .try_get::<Option<i64>, _>("reference_block_number")?
                    .context("selected registrar replay state is missing reference block number")?,
                transaction_hash: row.try_get("reference_transaction_hash")?,
                transaction_index: None,
                log_index: row.try_get("reference_log_index")?,
                canonicality_state: CanonicalityState::parse(
                    &row.try_get::<Option<String>, _>("reference_canonicality_state")?
                        .context("selected registrar replay state is missing canonicality")?,
                )?,
                namespace: row
                    .try_get::<Option<String>, _>("reference_namespace")?
                    .context("selected registrar replay state is missing namespace")?,
                source_manifest_id: row
                    .try_get::<Option<i64>, _>("reference_source_manifest_id")?
                    .unwrap_or(0),
                source_family: row
                    .try_get::<Option<String>, _>("reference_source_family")?
                    .unwrap_or_else(|| SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned()),
                manifest_version: row
                    .try_get::<Option<i64>, _>("reference_manifest_version")?
                    .unwrap_or(1),
            })
        } else {
            None
        };
        state.insert(
            row.try_get("logical_name_id")?,
            PreloadedRegistrarState {
                expiry,
                registrant,
                authority_key,
                labelhash,
                start_ref,
            },
        );
    }
    Ok(state)
}

fn selected_registrar_event_identities(
    raw_logs: &[AuthorityRawLogRow],
    event_topics: &AuthorityEventTopics,
) -> Result<Vec<String>> {
    let mut identities = BTreeSet::<String>::new();
    for raw_log in raw_logs {
        let Some(observation) = build_authority_observation(raw_log, event_topics)? else {
            continue;
        };
        match observation {
            AuthorityObservation::RegistrationRenewed(_) => {
                identities.insert(raw_log_event_identity(
                    raw_log,
                    EVENT_KIND_REGISTRATION_RENEWED,
                    "renewal",
                ));
                identities.insert(raw_log_event_identity(
                    raw_log,
                    EVENT_KIND_EXPIRY_CHANGED,
                    "expiry",
                ));
            }
            AuthorityObservation::TokenTransferred(_) => {
                identities.insert(raw_log_event_identity(
                    raw_log,
                    EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
                    "token-transfer",
                ));
            }
            _ => {}
        }
    }
    Ok(identities.into_iter().collect())
}

fn raw_log_event_identity(
    raw_log: &AuthorityRawLogRow,
    event_kind: &str,
    identity_prefix: &str,
) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}",
        DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        event_kind,
        identity_prefix,
        raw_log.block_hash,
        raw_log.transaction_hash,
        raw_log.log_index
    )
}

async fn load_selected_wrapper_state_before_replay(
    pool: &PgPool,
    logical_name_ids: &[String],
    raw_logs: &[AuthorityRawLogRow],
) -> Result<HashMap<String, PreloadedWrapperState>> {
    if logical_name_ids.is_empty() || raw_logs.is_empty() {
        return Ok(HashMap::new());
    }

    let block_hashes = raw_logs
        .iter()
        .map(|raw_log| raw_log.block_hash.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT
                after_state->>'authority_key' AS authority_key,
                CASE
                    WHEN event_kind = $3 THEN before_state->>'from'
                    ELSE NULL
                END AS owner,
                CASE
                    WHEN event_kind = $4 THEN (before_state->>'fuses')::BIGINT
                    ELSE NULL
                END AS fuses,
                CASE
                    WHEN event_kind = $5 THEN (before_state->>'expiry')::BIGINT
                    ELSE NULL
                END AS expiry,
                block_number,
                COALESCE(log_index, -1) AS log_index,
                normalized_event_id
            FROM normalized_events
            WHERE logical_name_id = ANY($1::TEXT[])
              AND block_hash = ANY($2::TEXT[])
              AND event_kind IN ($3, $4, $5)
              AND after_state->>'authority_kind' = 'wrapper'
              AND after_state ? 'authority_key'
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        )
        SELECT
            authority_key,
            (
                ARRAY_AGG(
                    owner
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE owner IS NOT NULL)
            )[1] AS owner,
            (
                ARRAY_AGG(
                    fuses
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE fuses IS NOT NULL)
            )[1] AS fuses,
            (
                ARRAY_AGG(
                    expiry
                    ORDER BY block_number ASC, log_index ASC, normalized_event_id ASC
                ) FILTER (WHERE expiry IS NOT NULL)
            )[1] AS expiry
        FROM candidates
        GROUP BY authority_key
        "#,
    )
    .bind(logical_name_ids)
    .bind(&block_hashes)
    .bind(EVENT_KIND_TOKEN_CONTROL_TRANSFERRED)
    .bind(EVENT_KIND_PERMISSION_SCOPE_CHANGED)
    .bind(EVENT_KIND_EXPIRY_CHANGED)
    .fetch_all(pool)
    .await
    .context("failed to preload selected wrapper state before restricted replay")?;

    let mut state = HashMap::new();
    for row in rows {
        let expiry = row
            .try_get::<Option<i64>, _>("expiry")?
            .map(|value| {
                OffsetDateTime::from_unix_timestamp(value)
                    .context("preloaded selected wrapper expiry is not a valid unix timestamp")
            })
            .transpose()?;
        state.insert(
            row.try_get("authority_key")?,
            PreloadedWrapperState {
                owner: row.try_get("owner")?,
                fuses: row.try_get("fuses")?,
                expiry,
            },
        );
    }
    Ok(state)
}

fn restricted_replay_labelhashes(
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

fn resolver_state_scopes_for_selected_names(
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

fn preload_latent_resolver_histories(
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

async fn load_latest_resolver_state_before_block(
    pool: &PgPool,
    logical_name_ids: &[String],
    boundary_block: i64,
) -> Result<HashMap<String, String>> {
    if logical_name_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (logical_name_id)
            logical_name_id,
            after_state->>'resolver' AS resolver
        FROM normalized_events
        WHERE logical_name_id = ANY($1::TEXT[])
          AND block_number < $2
          AND event_kind = $3
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY logical_name_id, block_number DESC, log_index DESC, normalized_event_id DESC
        "#,
    )
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

async fn load_latest_registry_resolver_raw_state_before_block(
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

    let rows = sqlx::query(
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
            FROM registry_emitters emitter
            JOIN raw_logs log
              ON log.chain_id = $1
             AND log.emitting_address = emitter.address
             AND log.topics[1] = $6
             AND log.block_number BETWEEN emitter.active_from_block_number
                 AND LEAST(emitter.active_to_block_number, $5 - 1)
            WHERE emitter.source_family = scope.registry_source_family
              AND lower(log.topics[2]) = scope.namehash
              AND log.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            ORDER BY log.block_number DESC, log.transaction_index DESC, log.log_index DESC, log.raw_log_id DESC
            LIMIT 1
        ) resolver_log ON TRUE
        ORDER BY scope.logical_name_id, resolver_log.block_number DESC, resolver_log.transaction_index DESC, resolver_log.log_index DESC, resolver_log.raw_log_id DESC
        "#,
    )
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

async fn load_selected_registry_resolver_state_before_replay(
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
    let rows = sqlx::query(
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
          AND event.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY event.logical_name_id, event.block_number DESC, event.normalized_event_id DESC
        "#,
    )
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

async fn load_latest_record_versions_before_block(
    pool: &PgPool,
    logical_name_ids: &[String],
    boundary_block: i64,
) -> Result<HashMap<String, i64>> {
    if logical_name_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (logical_name_id)
            logical_name_id,
            after_state->>'record_version' AS record_version
        FROM normalized_events
        WHERE logical_name_id = ANY($1::TEXT[])
          AND block_number < $2
          AND event_kind = $3
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY logical_name_id, block_number DESC, log_index DESC, normalized_event_id DESC
        "#,
    )
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

fn name_metadata_from_preload_row(row: &sqlx::postgres::PgRow) -> Result<NameMetadata> {
    Ok(NameMetadata {
        namespace: sql_row::get(&row, "namespace")?,
        logical_name_id: sql_row::get(&row, "logical_name_id")?,
        input_name: sql_row::get(&row, "input_name")?,
        canonical_display_name: sql_row::get(&row, "canonical_display_name")?,
        normalized_name: sql_row::get(&row, "normalized_name")?,
        dns_encoded_name: sql_row::get(&row, "dns_encoded_name")?,
        namehash: sql_row::get::<String>(&row, "namehash")?.to_ascii_lowercase(),
        labelhashes: sql_row::get(&row, "labelhashes")?,
        normalizer_version: sql_row::get(&row, "normalizer_version")?,
    })
}

async fn load_name_metadata_by_logical_name_ids(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<HashMap<String, NameMetadata>> {
    if logical_name_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            namespace,
            logical_name_id,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version
        FROM name_surfaces
        WHERE logical_name_id = ANY($1::TEXT[])
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(logical_name_ids)
    .fetch_all(pool)
    .await
    .context("failed to preload selected registrar replay name metadata")?;

    let mut names = HashMap::new();
    for row in rows {
        let name = name_metadata_from_preload_row(&row)?;
        names.insert(name.logical_name_id.clone(), name);
    }
    Ok(names)
}

pub(super) fn empty_preloaded_history(
    labelhash: String,
    name: Option<NameMetadata>,
) -> NameHistory {
    NameHistory {
        name,
        labelhash,
        first_name_ref: None,
        current_registration: None,
        superseded_registration: None,
        current_wrapper_key: None,
        wrapper_authorities: BTreeMap::new(),
        current_registry_owner: None,
        current_resolver: None,
        current_record_version: None,
        open_binding: None,
        bindings: Vec::new(),
        events: Vec::new(),
        registry_resource_anchor: None,
        latest_registry_owner_ref: None,
        latest_registry_owner_before_registration: None,
    }
}

pub(super) fn preload_registrar_history(
    history: &mut NameHistory,
    provenance: &Value,
    binding_ref: &BoundaryRef,
    surface_binding_id: Uuid,
    binding_active_to: Option<OffsetDateTime>,
    registrar_state: Option<&PreloadedRegistrarState>,
    block_index: &CanonicalBlockIndex,
) -> Result<()> {
    let provenance_authority_key = provenance_string(provenance, "authority_key")?;
    let authority_key = registrar_state
        .and_then(|state| state.authority_key.as_deref())
        .unwrap_or(&provenance_authority_key)
        .to_owned();
    let labelhash = registrar_labelhash_from_provenance_or_authority_key(
        provenance,
        &authority_key,
        &history.labelhash,
    );
    let expiry = if let Some(expiry) = registrar_state.and_then(|state| state.expiry) {
        expiry
    } else {
        let expiry =
            registrar_expiry_from_provenance_or_binding_end(provenance, binding_active_to)?;
        OffsetDateTime::from_unix_timestamp(expiry)
            .context("preloaded registrar expiry is not a valid unix timestamp")?
    };
    let registrant = registrar_state
        .and_then(|state| state.registrant.as_deref())
        .or_else(|| provenance.get("registrant").and_then(Value::as_str))
        .unwrap_or(ZERO_ADDRESS)
        .to_owned();
    let source_manifest_id = manifest_id_from_authority_key(&authority_key).unwrap_or(0);
    let source_family = default_registrar_source_family(&binding_ref.namespace).to_owned();
    let start_ref = registrar_state
        .and_then(|state| state.start_ref.clone())
        .unwrap_or_else(|| {
            observation_ref_from_boundary(
                binding_ref,
                Some(source_family),
                Some(source_manifest_id),
                log_index_from_authority_key(&authority_key),
            )
        });
    let lease = RegistrationLease {
        authority_key,
        labelhash,
        registrant,
        expiry,
        release_ref: block_index
            .first_block_at_or_after(release_after_grace(expiry)?, &binding_ref.namespace),
        start_ref,
    };
    let anchor = build_registrar_anchor(&lease);
    history.current_registration = Some(lease);
    history.superseded_registration = None;
    history.open_binding = Some(OpenBinding {
        surface_binding_id,
        authority: anchor,
        active_from: binding_ref.block_timestamp,
        anchor_ref: binding_ref.clone(),
    });
    Ok(())
}

pub(super) fn preload_selected_registrar_lease(
    history: &mut NameHistory,
    registrar_state: Option<&PreloadedRegistrarState>,
    block_index: &CanonicalBlockIndex,
) -> Result<()> {
    if history.current_registration.is_some() {
        return Ok(());
    }
    let Some(state) = registrar_state else {
        return Ok(());
    };
    let (Some(authority_key), Some(expiry), Some(start_ref)) = (
        state.authority_key.as_ref(),
        state.expiry,
        state.start_ref.as_ref(),
    ) else {
        return Ok(());
    };

    let labelhash = state
        .labelhash
        .clone()
        .or_else(|| registrar_labelhash_from_authority_key(authority_key))
        .unwrap_or_else(|| history.labelhash.clone());
    let registrant = state
        .registrant
        .clone()
        .unwrap_or_else(|| ZERO_ADDRESS.to_owned());
    history.current_registration = Some(RegistrationLease {
        authority_key: authority_key.clone(),
        labelhash,
        registrant,
        expiry,
        release_ref: block_index
            .first_block_at_or_after(release_after_grace(expiry)?, &start_ref.namespace),
        start_ref: start_ref.clone(),
    });
    history.superseded_registration = None;

    Ok(())
}

fn registrar_labelhash_from_provenance_or_authority_key(
    provenance: &Value,
    authority_key: &str,
    history_labelhash: &str,
) -> String {
    provenance
        .get("labelhash")
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_lowercase())
        .or_else(|| registrar_labelhash_from_authority_key(authority_key))
        .unwrap_or_else(|| history_labelhash.to_owned())
}

fn registrar_expiry_from_provenance_or_binding_end(
    provenance: &Value,
    binding_active_to: Option<OffsetDateTime>,
) -> Result<i64> {
    if let Some(expiry) = provenance.get("expiry").and_then(Value::as_i64) {
        return Ok(expiry);
    }
    if let Some(released_at) = provenance.get("released_at").and_then(Value::as_i64) {
        return released_at
            .checked_sub(ENS_GRACE_PERIOD_SECS)
            .context("preloaded registrar released_at cannot be converted to expiry");
    }
    if let Some(active_to) = binding_active_to {
        return active_to
            .unix_timestamp()
            .checked_sub(ENS_GRACE_PERIOD_SECS)
            .context("preloaded registrar binding end cannot be converted to expiry");
    }

    bail!("preloaded authority provenance is missing integer expiry");
}

pub(super) fn registrar_labelhash_from_authority_key(authority_key: &str) -> Option<String> {
    let mut parts = authority_key.split(':');
    if parts.next()? != "registrar" {
        return None;
    }
    let _chain = parts.next()?;
    let _manifest_id = parts.next()?;
    let labelhash = parts.next()?;
    if !labelhash.starts_with("0x") {
        return None;
    }
    Some(labelhash.to_ascii_lowercase())
}

fn preload_wrapper_history(
    history: &mut NameHistory,
    provenance: &Value,
    binding_ref: &BoundaryRef,
    surface_binding_id: Uuid,
    selected_wrapper_state: &HashMap<String, PreloadedWrapperState>,
) -> Result<()> {
    let authority_key = provenance_string(provenance, "authority_key")?;
    let selected_state = selected_wrapper_state.get(&authority_key);
    let node = provenance_string(provenance, "namehash")
        .or_else(|_| provenance_string(provenance, "node"))
        .unwrap_or_else(|_| {
            history
                .name
                .as_ref()
                .map(|name| name.namehash.clone())
                .unwrap_or_default()
        });
    let owner = selected_state
        .and_then(|state| state.owner.clone())
        .or_else(|| {
            provenance
                .get("owner")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| ZERO_ADDRESS.to_owned());
    let fuses = selected_state
        .and_then(|state| state.fuses)
        .or_else(|| provenance.get("fuses").and_then(Value::as_i64))
        .unwrap_or_default();
    let expiry = if let Some(expiry) = selected_state.and_then(|state| state.expiry) {
        expiry
    } else {
        let expiry = provenance_i64(provenance, "expiry")?;
        OffsetDateTime::from_unix_timestamp(expiry)
            .context("preloaded wrapper expiry is not a valid unix timestamp")?
    };
    let source_manifest_id = manifest_id_from_authority_key(&authority_key).unwrap_or(0);
    let start_ref = observation_ref_from_boundary(
        binding_ref,
        Some(SOURCE_FAMILY_ENS_V1_WRAPPER_L1.to_owned()),
        Some(source_manifest_id),
        log_index_from_authority_key(&authority_key),
    );
    let wrapper = WrapperAuthority {
        authority_key: authority_key.clone(),
        node,
        owner,
        fuses,
        expiry,
        start_ref,
        end_ref: None,
    };
    let anchor = build_wrapper_anchor(&wrapper);
    history
        .wrapper_authorities
        .insert(authority_key.clone(), wrapper);
    history.current_wrapper_key = Some(authority_key);
    history.open_binding = Some(OpenBinding {
        surface_binding_id,
        authority: anchor,
        active_from: binding_ref.block_timestamp,
        anchor_ref: binding_ref.clone(),
    });
    Ok(())
}

fn preload_registry_history(
    history: &mut NameHistory,
    provenance: &Value,
    binding_ref: &BoundaryRef,
    surface_binding_id: Uuid,
    resource_id: Uuid,
) {
    let labelhash = history.labelhash.clone();
    let authority_key = provenance
        .get("authority_key")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("registry-only:{}:{}", binding_ref.chain_id, labelhash));
    let source_family = authority_profile_for_source_family(
        provenance
            .get("source_family")
            .and_then(Value::as_str)
            .unwrap_or(SOURCE_FAMILY_ENS_V1_REGISTRY_L1),
    )
    .map(|profile| profile.registry_source_family().to_owned())
    .unwrap_or_else(|| SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned());
    let source_manifest_id = manifest_id_from_authority_key(&authority_key).unwrap_or(0);
    let authority = AuthorityAnchor {
        kind: AuthorityKind::RegistryOnly,
        authority_key,
        resource_id,
        token_lineage_id: None,
        binding_source_family: source_family,
        binding_manifest_version: 1,
        binding_manifest_id: source_manifest_id,
    };
    history.current_registry_owner = provenance
        .get("current_registry_owner")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    history.registry_resource_anchor = Some(binding_ref.clone());
    history.latest_registry_owner_ref = Some(observation_ref_from_boundary(
        binding_ref,
        Some(authority.binding_source_family.clone()),
        Some(authority.binding_manifest_id),
        None,
    ));
    history.open_binding = Some(OpenBinding {
        surface_binding_id,
        authority,
        active_from: binding_ref.block_timestamp,
        anchor_ref: binding_ref.clone(),
    });
}

fn observation_ref_from_boundary(
    boundary: &BoundaryRef,
    source_family: Option<String>,
    source_manifest_id: Option<i64>,
    log_index: Option<i64>,
) -> ObservationRef {
    ObservationRef {
        chain_id: boundary.chain_id.clone(),
        block_hash: boundary.block_hash.clone(),
        block_number: boundary.block_number,
        block_timestamp: boundary.block_timestamp,
        transaction_hash: None,
        transaction_index: None,
        log_index,
        canonicality_state: boundary.canonicality_state,
        namespace: boundary.namespace.clone(),
        source_manifest_id: source_manifest_id.unwrap_or(0),
        source_family: source_family
            .unwrap_or_else(|| default_registrar_source_family(&boundary.namespace).to_owned()),
        manifest_version: 1,
    }
}

fn provenance_string(provenance: &Value, key: &str) -> Result<String> {
    provenance
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .with_context(|| format!("preloaded authority provenance is missing {key}"))
}

fn provenance_i64(provenance: &Value, key: &str) -> Result<i64> {
    provenance
        .get(key)
        .and_then(Value::as_i64)
        .with_context(|| format!("preloaded authority provenance is missing integer {key}"))
}

fn manifest_id_from_authority_key(authority_key: &str) -> Option<i64> {
    authority_key.split(':').nth(2)?.parse().ok()
}

fn log_index_from_authority_key(authority_key: &str) -> Option<i64> {
    authority_key.rsplit(':').next()?.parse().ok()
}
