use super::*;

pub(super) async fn orphan_stale_overlapping_surface_bindings(
    pool: &PgPool,
    incoming: &[SurfaceBinding],
    existing: &mut Vec<SurfaceBinding>,
) -> Result<usize> {
    let candidates = stale_overlapping_surface_binding_candidates(incoming, existing);
    if candidates.is_empty() {
        return Ok(0);
    }

    let mut surface_binding_ids = Vec::with_capacity(candidates.len());
    let mut resource_ids = Vec::with_capacity(candidates.len());
    let mut logical_name_ids = Vec::with_capacity(candidates.len());
    let mut authority_keys = Vec::with_capacity(candidates.len());
    let mut active_from_epochs = Vec::with_capacity(candidates.len());
    for candidate in candidates.values() {
        surface_binding_ids.push(candidate.surface_binding_id);
        resource_ids.push(candidate.resource_id);
        logical_name_ids.push(candidate.logical_name_id.clone());
        authority_keys.push(candidate.authority_key.clone());
        active_from_epochs.push(candidate.active_from_epoch);
    }

    let rows = sqlx::query_scalar::<_, Uuid>(
        r#"
        WITH candidate(
            surface_binding_id,
            resource_id,
            logical_name_id,
            authority_key,
            active_from_epoch
        ) AS (
            SELECT *
            FROM unnest(
                $1::UUID[],
                $2::UUID[],
                $3::TEXT[],
                $4::TEXT[],
                $5::BIGINT[]
            )
        ),
        event_backed AS (
            SELECT DISTINCT candidate.surface_binding_id
            FROM normalized_events
            JOIN candidate
              ON normalized_events.logical_name_id = candidate.logical_name_id
             AND normalized_events.resource_id = candidate.resource_id
             AND normalized_events.event_kind = $6
             AND normalized_events.after_state->>'authority_key'
                 IS NOT DISTINCT FROM candidate.authority_key
             AND normalized_events.after_state->>'active_from' = candidate.active_from_epoch::TEXT
            WHERE normalized_events.canonicality_state IN ('canonical', 'safe', 'finalized')
        )
        UPDATE surface_bindings
        SET
            canonicality_state = 'orphaned'::canonicality_state,
            observed_at = now()
        FROM candidate
        WHERE surface_bindings.surface_binding_id = candidate.surface_binding_id
          AND surface_bindings.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND NOT EXISTS (
              SELECT 1
              FROM event_backed
              WHERE event_backed.surface_binding_id = candidate.surface_binding_id
          )
        RETURNING surface_bindings.surface_binding_id
        "#,
    )
    .bind(&surface_binding_ids)
    .bind(&resource_ids)
    .bind(&logical_name_ids)
    .bind(&authority_keys)
    .bind(&active_from_epochs)
    .bind(EVENT_KIND_SURFACE_BOUND)
    .fetch_all(pool)
    .await
    .context(
        "failed to orphan stale overlapping surface bindings before restricted authority replay",
    )?;

    if rows.is_empty() {
        return Ok(0);
    }

    let orphaned_ids = rows.into_iter().collect::<HashSet<_>>();
    existing.retain(|binding| !orphaned_ids.contains(&binding.surface_binding_id));

    tracing::warn!(
        adapter = DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        orphaned_surface_binding_count = orphaned_ids.len(),
        "orphaned stale overlapping surface bindings before restricted authority replay"
    );

    Ok(orphaned_ids.len())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StaleSurfaceBindingCandidate {
    pub(super) surface_binding_id: Uuid,
    pub(super) resource_id: Uuid,
    pub(super) logical_name_id: String,
    pub(super) authority_key: Option<String>,
    pub(super) active_from_epoch: i64,
}

pub(super) fn stale_overlapping_surface_binding_candidates(
    incoming: &[SurfaceBinding],
    existing: &[SurfaceBinding],
) -> BTreeMap<Uuid, StaleSurfaceBindingCandidate> {
    let mut incoming_by_name = BTreeMap::<&str, Vec<&SurfaceBinding>>::new();
    for binding in incoming
        .iter()
        .filter(|binding| surface_binding_exclusion_applies(binding.canonicality_state))
    {
        incoming_by_name
            .entry(binding.logical_name_id.as_str())
            .or_default()
            .push(binding);
    }

    let mut candidates = BTreeMap::new();
    for existing in existing.iter().filter(|binding| {
        binding.provenance.get("adapter").and_then(Value::as_str)
            == Some(DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY)
            && surface_binding_exclusion_applies(binding.canonicality_state)
    }) {
        let Some(incoming_for_name) = incoming_by_name.get(existing.logical_name_id.as_str())
        else {
            continue;
        };
        if incoming_for_name.iter().any(|incoming| {
            incoming.surface_binding_id != existing.surface_binding_id
                && surface_binding_ranges_overlap(existing, incoming)
        }) {
            candidates.insert(
                existing.surface_binding_id,
                StaleSurfaceBindingCandidate {
                    surface_binding_id: existing.surface_binding_id,
                    resource_id: existing.resource_id,
                    logical_name_id: existing.logical_name_id.clone(),
                    authority_key: existing
                        .provenance
                        .get("authority_key")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    active_from_epoch: existing.active_from.unix_timestamp(),
                },
            );
        }
    }

    candidates
}

fn surface_binding_ranges_overlap(left: &SurfaceBinding, right: &SurfaceBinding) -> bool {
    right
        .active_to
        .is_none_or(|right_active_to| left.active_from < right_active_to)
        && left
            .active_to
            .is_none_or(|left_active_to| right.active_from < left_active_to)
}
