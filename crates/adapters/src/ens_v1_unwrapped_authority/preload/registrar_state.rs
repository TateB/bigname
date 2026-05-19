use super::registrar_history::registrar_expiry_from_provenance_or_binding_end;
use super::support::*;
use super::*;

mod selected;

use selected::selected_registrar_event_identities;

pub(super) async fn block_index_with_preloaded_registrar_release_boundaries(
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
    for state in registrar_state.values() {
        let (Some(expiry), Some(start_ref)) = (state.expiry, state.start_ref.as_ref()) else {
            continue;
        };
        let release_timestamp = release_after_grace(expiry)?;
        if release_timestamp <= replay_head.block_timestamp {
            release_timestamps.push(release_timestamp);
            release_namespaces.push(start_ref.namespace.clone());
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
    let rows = sqlx::query(&format!(
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
              AND canonicality_state {CANONICALITY_STATE_FILTER}
            ORDER BY block_timestamp, block_number
            LIMIT 1
        ) rb ON TRUE
        ORDER BY requested.release_timestamp, requested.namespace, rb.block_timestamp, rb.block_number
        "#
    ))
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

pub(super) async fn load_latest_registrar_state_before_block(
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

    let rows = sqlx::query(&format!(
        r#"
        WITH scope AS (
            SELECT *
            FROM UNNEST($1::TEXT[], $2::BIGINT[]) AS scope(logical_name_id, lower_block_number)
        ),
        candidates AS (
            SELECT
                event.logical_name_id,
                event.chain_id,
                event.block_hash,
                CASE
                    WHEN event.event_kind IN ($4, $5)
                    THEN (event.after_state->>'expiry')::BIGINT
                    WHEN event.event_kind = $6
                     AND resource.provenance->>'authority_kind' = 'registrar'
                     AND resource.canonicality_state {CANONICALITY_STATE_FILTER}
                    THEN (event.after_state->>'expiry')::BIGINT
                    ELSE NULL
                END AS expiry,
                CASE
                    WHEN event.event_kind = $4 THEN event.after_state->>'registrant'
                    WHEN event.event_kind = $7 THEN event.after_state->>'to'
                    ELSE NULL
                END AS registrant,
                CASE
                    WHEN resource.provenance->>'authority_kind' = 'registrar'
                     AND resource.canonicality_state {CANONICALITY_STATE_FILTER}
                    THEN resource.provenance->>'authority_key'
                    ELSE NULL
                END AS authority_key,
                CASE
                    WHEN resource.provenance->>'authority_kind' = 'registrar'
                     AND resource.canonicality_state {CANONICALITY_STATE_FILTER}
                    THEN COALESCE(resource.provenance->>'labelhash', event.after_state->>'labelhash')
                    ELSE NULL
                END AS labelhash,
                resource.chain_id AS start_chain_id,
                resource.block_hash AS start_block_hash,
                resource.block_number AS start_block_number,
                start_block.block_timestamp AS start_block_timestamp,
                resource.canonicality_state::TEXT AS start_canonicality_state,
                event.namespace,
                event.source_manifest_id,
                event.source_family,
                event.manifest_version,
                event.block_number,
                COALESCE(event.log_index, -1) AS log_index,
                event.normalized_event_id
            FROM normalized_events event
            LEFT JOIN resources resource
              ON resource.resource_id = event.resource_id
            LEFT JOIN chain_lineage start_block
              ON start_block.chain_id = resource.chain_id
             AND start_block.block_hash = resource.block_hash
            JOIN scope
              ON scope.logical_name_id = event.logical_name_id
            WHERE event.block_number >= scope.lower_block_number
              AND event.block_number < $3
              AND event.event_kind IN ($4, $5, $6, $7)
              AND event.canonicality_state {CANONICALITY_STATE_FILTER}
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
            ,
            (
                ARRAY_AGG(
                    authority_key
                    ORDER BY block_number DESC, log_index DESC, normalized_event_id DESC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS authority_key,
            (
                ARRAY_AGG(
                    labelhash
                    ORDER BY block_number DESC, log_index DESC, normalized_event_id DESC
                ) FILTER (WHERE authority_key IS NOT NULL)
            )[1] AS labelhash,
            (
                ARRAY_AGG(
                    jsonb_build_object(
                        'chain_id', start_chain_id,
                        'block_hash', start_block_hash,
                        'block_number', start_block_number,
                        'block_timestamp', EXTRACT(EPOCH FROM start_block_timestamp)::BIGINT,
                        'canonicality_state', start_canonicality_state,
                        'namespace', namespace,
                        'source_manifest_id', source_manifest_id,
                        'source_family', source_family,
                        'manifest_version', manifest_version
                    )
                    ORDER BY block_number DESC, log_index DESC, normalized_event_id DESC
                ) FILTER (
                    WHERE authority_key IS NOT NULL
                      AND start_chain_id IS NOT NULL
                      AND start_block_hash IS NOT NULL
                      AND start_block_number IS NOT NULL
                      AND start_block_timestamp IS NOT NULL
                )
            )[1] AS reference
        FROM candidates
        GROUP BY logical_name_id
        "#
    ))
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
        let authority_key: Option<String> = row.try_get("authority_key")?;
        let labelhash: Option<String> = row.try_get("labelhash")?;
        let reference: Option<Value> = row.try_get("reference")?;
        let start_ref = match (authority_key.as_ref(), reference) {
            (Some(authority_key), Some(reference)) => {
                Some(latest_registrar_start_ref(&reference, authority_key)?)
            }
            (Some(_), None) => {
                bail!("latest registrar replay state is missing reference")
            }
            _ => None,
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

fn latest_registrar_start_ref(reference: &Value, authority_key: &str) -> Result<ObservationRef> {
    Ok(ObservationRef {
        chain_id: json_string(reference, "chain_id")?,
        block_timestamp: OffsetDateTime::from_unix_timestamp(json_i64(
            reference,
            "block_timestamp",
        )?)
        .context("latest registrar replay state timestamp is not a valid unix timestamp")?,
        block_hash: json_string(reference, "block_hash")?,
        block_number: json_i64(reference, "block_number")?,
        transaction_hash: None,
        transaction_index: None,
        log_index: log_index_from_authority_key(authority_key),
        canonicality_state: CanonicalityState::parse(&json_string(
            reference,
            "canonicality_state",
        )?)?,
        namespace: json_string(reference, "namespace")?,
        source_manifest_id: manifest_id_from_authority_key(authority_key)
            .or_else(|| json_optional_i64(reference, "source_manifest_id"))
            .unwrap_or(0),
        source_family: json_optional_string(reference, "source_family")
            .unwrap_or_else(|| SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned()),
        manifest_version: json_optional_i64(reference, "manifest_version").unwrap_or(1),
    })
}

pub(super) async fn load_selected_registrar_state_before_replay(
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
    let rows = sqlx::query(&format!(
        r#"
        WITH selected_events AS (
            SELECT
                event.logical_name_id,
                event.chain_id,
                event.block_hash,
                CASE
                    WHEN event.event_kind = $2
                    THEN (event.before_state->>'expiry')::BIGINT
                    WHEN event.event_kind = $3
                     AND resource.provenance->>'authority_kind' = 'registrar'
                     AND resource.canonicality_state {CANONICALITY_STATE_FILTER}
                    THEN (event.before_state->>'expiry')::BIGINT
                    ELSE NULL
                END AS expiry,
                CASE
                    WHEN event.event_kind = $4 THEN event.before_state->>'from'
                    ELSE NULL
                END AS registrant,
                event.block_number,
                COALESCE(event.log_index, -1) AS log_index,
                event.normalized_event_id
            FROM normalized_events event
            LEFT JOIN resources resource
              ON resource.resource_id = event.resource_id
            WHERE event.event_identity = ANY($1::TEXT[])
              AND event.event_kind IN ($2, $3, $4)
              AND event.canonicality_state {CANONICALITY_STATE_FILTER}
        ),
        first_selected AS (
            SELECT DISTINCT ON (logical_name_id)
                logical_name_id,
                chain_id,
                block_hash,
                block_number,
                log_index,
                normalized_event_id
            FROM selected_events
            ORDER BY logical_name_id, block_number ASC, log_index ASC, normalized_event_id ASC
        ),
        scalar_state AS (
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
            FROM selected_events
            GROUP BY logical_name_id
        )
        SELECT
            scalar_state.logical_name_id,
            scalar_state.expiry,
            scalar_state.registrant,
            prior_authority.authority_key,
            prior_authority.labelhash,
            prior_authority.reference
        FROM scalar_state
        JOIN first_selected
          ON first_selected.logical_name_id = scalar_state.logical_name_id
        LEFT JOIN LATERAL (
            SELECT EXTRACT(EPOCH FROM block_timestamp)::BIGINT AS block_timestamp_unix
            FROM chain_lineage
            WHERE chain_id = first_selected.chain_id
              AND block_hash = first_selected.block_hash
            LIMIT 1
        ) selected_block ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                prior.authority_key,
                prior.labelhash,
                prior.reference
            FROM (
                SELECT
                    resource.provenance->>'authority_key' AS authority_key,
                    COALESCE(resource.provenance->>'labelhash', event.after_state->>'labelhash') AS labelhash,
                    COALESCE(
                        (event.after_state->>'expiry')::BIGINT,
                        (resource.provenance->>'expiry')::BIGINT
                    ) AS expiry,
                    jsonb_build_object(
                        'chain_id', resource.chain_id,
                        'block_hash', resource.block_hash,
                        'block_number', resource.block_number,
                        'block_timestamp', EXTRACT(EPOCH FROM start_block.block_timestamp)::BIGINT,
                        'canonicality_state', resource.canonicality_state::TEXT,
                        'namespace', event.namespace,
                        'source_manifest_id', event.source_manifest_id,
                        'source_family', event.source_family,
                        'manifest_version', event.manifest_version
                    ) AS reference,
                    event.block_number,
                    COALESCE(event.log_index, -1) AS log_index,
                    event.normalized_event_id
                FROM normalized_events event
                JOIN resources resource
                  ON resource.resource_id = event.resource_id
                 AND resource.provenance->>'authority_kind' = 'registrar'
                 AND resource.canonicality_state {CANONICALITY_STATE_FILTER}
                LEFT JOIN chain_lineage start_block
                  ON start_block.chain_id = resource.chain_id
                 AND start_block.block_hash = resource.block_hash
                WHERE event.logical_name_id = first_selected.logical_name_id
                  AND event.event_kind IN ($5, $2, $3, $4)
                  AND event.canonicality_state {CANONICALITY_STATE_FILTER}
                  AND (
                      event.block_number < first_selected.block_number
                      OR (
                          event.block_number = first_selected.block_number
                          AND COALESCE(event.log_index, -1) < first_selected.log_index
                      )
                  )
            ) prior
            WHERE prior.authority_key IS NOT NULL
              AND prior.reference IS NOT NULL
              AND (
                  prior.expiry IS NULL
                  OR prior.expiry + $6 > selected_block.block_timestamp_unix
              )
            ORDER BY prior.block_number DESC, prior.log_index DESC, prior.normalized_event_id DESC
            LIMIT 1
        ) prior_authority ON TRUE
        "#
    ))
    .bind(&event_identities)
    .bind(EVENT_KIND_REGISTRATION_RENEWED)
    .bind(EVENT_KIND_EXPIRY_CHANGED)
    .bind(EVENT_KIND_TOKEN_CONTROL_TRANSFERRED)
    .bind(EVENT_KIND_REGISTRATION_GRANTED)
    .bind(ENS_GRACE_PERIOD_SECS)
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
        let reference: Option<Value> = row.try_get("reference")?;
        let start_ref = match (authority_key.as_ref(), reference) {
            (Some(authority_key), Some(reference)) => {
                Some(latest_registrar_start_ref(&reference, authority_key)?)
            }
            (Some(_), None) => {
                bail!("selected registrar replay state is missing reference")
            }
            _ => None,
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
