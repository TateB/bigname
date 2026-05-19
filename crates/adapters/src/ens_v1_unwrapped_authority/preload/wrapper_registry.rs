use super::support::*;
use super::*;

pub(super) async fn load_selected_wrapper_state_before_replay(
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
    let rows = sqlx::query(&format!(
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
              AND canonicality_state {CANONICALITY_STATE_FILTER}
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
        "#
    ))
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

pub(super) async fn load_latest_wrapper_state_before_block(
    pool: &PgPool,
    logical_name_ids: &[String],
    boundary_block: i64,
) -> Result<HashMap<String, PreloadedWrapperState>> {
    if logical_name_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(&format!(
        r#"
        WITH candidates AS (
            SELECT
                resource.provenance->>'authority_key' AS authority_key,
                CASE
                    WHEN event.event_kind = $3 THEN event.after_state->>'to'
                    ELSE NULL
                END AS owner,
                CASE
                    WHEN event.event_kind = $4 THEN (event.after_state->>'fuses')::BIGINT
                    ELSE NULL
                END AS fuses,
                CASE
                    WHEN event.event_kind = $5 THEN (event.after_state->>'expiry')::BIGINT
                    ELSE NULL
                END AS expiry,
                event.block_number,
                event.log_index,
                event.normalized_event_id
            FROM normalized_events event
            JOIN resources resource
              ON resource.resource_id = event.resource_id
            WHERE event.logical_name_id = ANY($1::TEXT[])
              AND event.block_number < $2
              AND event.event_kind IN ($3, $4, $5)
              AND resource.provenance->>'authority_kind' = 'wrapper'
              AND resource.provenance ? 'authority_key'
              AND event.canonicality_state {CANONICALITY_STATE_FILTER}
              AND resource.canonicality_state {CANONICALITY_STATE_FILTER}
        )
        SELECT
            authority_key,
            (
                ARRAY_AGG(
                    owner
                    ORDER BY block_number DESC, log_index DESC NULLS LAST, normalized_event_id DESC
                ) FILTER (WHERE owner IS NOT NULL)
            )[1] AS owner,
            (
                ARRAY_AGG(
                    fuses
                    ORDER BY block_number DESC, log_index DESC NULLS LAST, normalized_event_id DESC
                ) FILTER (WHERE fuses IS NOT NULL)
            )[1] AS fuses,
            (
                ARRAY_AGG(
                    expiry
                    ORDER BY block_number DESC, log_index DESC NULLS LAST, normalized_event_id DESC
                ) FILTER (WHERE expiry IS NOT NULL)
            )[1] AS expiry
        FROM candidates
        GROUP BY authority_key
        "#
    ))
    .bind(logical_name_ids)
    .bind(boundary_block)
    .bind(EVENT_KIND_TOKEN_CONTROL_TRANSFERRED)
    .bind(EVENT_KIND_PERMISSION_SCOPE_CHANGED)
    .bind(EVENT_KIND_EXPIRY_CHANGED)
    .fetch_all(pool)
    .await
    .context("failed to preload latest wrapper state before restricted replay")?;

    let mut state = HashMap::new();
    for row in rows {
        let expiry = row
            .try_get::<Option<i64>, _>("expiry")?
            .map(|value| {
                OffsetDateTime::from_unix_timestamp(value)
                    .context("preloaded latest wrapper expiry is not a valid unix timestamp")
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

pub(super) async fn load_latest_registry_owner_before_block(
    pool: &PgPool,
    logical_name_ids: &[String],
    boundary_block: i64,
) -> Result<HashMap<String, PreloadedRegistryOwnerState>> {
    if logical_name_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(&format!(
        r#"
        WITH candidates AS (
            SELECT
                event.logical_name_id,
                CASE
                    WHEN event.event_kind = $3 THEN event.after_state->>'owner'
                    WHEN event.event_kind = $4 THEN event.after_state->>'subject'
                    ELSE NULL
                END AS owner,
                event.chain_id,
                event.block_hash,
                event.block_number,
                block.block_timestamp,
                event.transaction_hash,
                NULLIF(event.raw_fact_ref->>'transaction_index', '')::BIGINT AS transaction_index,
                event.log_index,
                event.canonicality_state::TEXT AS canonicality_state,
                event.namespace,
                event.source_manifest_id,
                event.source_family,
                event.manifest_version,
                event.normalized_event_id
            FROM normalized_events event
            JOIN chain_lineage block
              ON block.chain_id = event.chain_id
             AND block.block_hash = event.block_hash
            WHERE event.logical_name_id = ANY($1::TEXT[])
              AND event.block_number < $2
              AND event.event_kind IN ($3, $4)
              AND (
                  event.event_kind = $3
                  OR (
                      event.after_state->'scope'->>'kind' = 'resource'
                      AND event.after_state->'grant_source'->>'authority_kind' = 'registry_only'
                      AND event.after_state->'grant_source'->>'source_event_kind' = $3
                      AND event.after_state->'effective_powers' ? 'resource_control'
                  )
              )
              AND event.canonicality_state {CANONICALITY_STATE_FILTER}
        )
        SELECT DISTINCT ON (logical_name_id)
            logical_name_id,
            owner,
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            canonicality_state,
            namespace,
            source_manifest_id,
            source_family,
            manifest_version
        FROM candidates
        WHERE owner IS NOT NULL
        ORDER BY
            logical_name_id,
            block_number DESC,
            log_index DESC NULLS LAST,
            normalized_event_id DESC
        "#
    ))
    .bind(logical_name_ids)
    .bind(boundary_block)
    .bind(EVENT_KIND_AUTHORITY_TRANSFERRED)
    .bind(EVENT_KIND_PERMISSION_CHANGED)
    .fetch_all(pool)
    .await
    .context("failed to preload latest registry owner before restricted replay")?;

    let mut state = HashMap::new();
    for row in rows {
        let owner = row.try_get("owner")?;
        let reference = ObservationRef {
            chain_id: row.try_get("chain_id")?,
            block_hash: row.try_get("block_hash")?,
            block_number: row.try_get("block_number")?,
            block_timestamp: row.try_get("block_timestamp")?,
            transaction_hash: row.try_get("transaction_hash")?,
            transaction_index: row.try_get("transaction_index")?,
            log_index: row.try_get("log_index")?,
            canonicality_state: row.try_get("canonicality_state")?,
            namespace: row.try_get("namespace")?,
            source_manifest_id: row.try_get("source_manifest_id")?,
            source_family: row.try_get("source_family")?,
            manifest_version: row.try_get("manifest_version")?,
        };
        state.insert(
            row.try_get("logical_name_id")?,
            PreloadedRegistryOwnerState {
                owner,
                reference: Some(reference),
            },
        );
    }
    Ok(state)
}

pub(super) fn preload_wrapper_history(
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

pub(super) fn preload_registry_history(
    history: &mut NameHistory,
    provenance: &Value,
    binding_ref: &BoundaryRef,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    registry_owner_state: Option<&PreloadedRegistryOwnerState>,
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
    history.current_registry_owner = registry_owner_state
        .and_then(|state| state.owner.clone())
        .or_else(|| {
            provenance
                .get("current_registry_owner")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        });
    history.registry_resource_anchor = Some(binding_ref.clone());
    history.latest_registry_owner_ref = registry_owner_state
        .and_then(|state| state.reference.clone())
        .or_else(|| {
            Some(observation_ref_from_boundary(
                binding_ref,
                Some(authority.binding_source_family.clone()),
                Some(authority.binding_manifest_id),
                None,
            ))
        });
    history.open_binding = Some(OpenBinding {
        surface_binding_id,
        authority,
        active_from: binding_ref.block_timestamp,
        anchor_ref: binding_ref.clone(),
    });
}
