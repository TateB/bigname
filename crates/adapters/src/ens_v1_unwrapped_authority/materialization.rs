use super::*;
use bigname_storage::sql_row;

mod lineage;

pub(super) use lineage::{build_resource, build_token_lineage, build_token_lineage_from_boundary};

pub(super) fn coalesce_name_surfaces_for_upsert(surfaces: &mut Vec<NameSurface>) {
    let mut seen = HashSet::<String>::new();
    surfaces.retain(|surface| seen.insert(surface.logical_name_id.clone()));
}

pub(super) fn normalize_surface_bindings_for_upsert(
    bindings: &mut Vec<SurfaceBinding>,
) -> Result<()> {
    if bindings.len() < 2 {
        return Ok(());
    }

    coalesce_surface_bindings_for_upsert(bindings)?;
    bindings.sort_by(|left, right| {
        left.logical_name_id
            .cmp(&right.logical_name_id)
            .then_with(|| left.active_from.cmp(&right.active_from))
            .then_with(|| left.block_number.cmp(&right.block_number))
            .then_with(|| left.surface_binding_id.cmp(&right.surface_binding_id))
    });

    let mut group_start = 0usize;
    while group_start < bindings.len() {
        let logical_name_id = bindings[group_start].logical_name_id.clone();
        let mut group_end = group_start + 1;
        while group_end < bindings.len() && bindings[group_end].logical_name_id == logical_name_id {
            group_end += 1;
        }

        close_incoming_binding_group(&mut bindings[group_start..group_end]);
        group_start = group_end;
    }

    Ok(())
}

fn coalesce_surface_bindings_for_upsert(bindings: &mut Vec<SurfaceBinding>) -> Result<()> {
    let mut by_id = BTreeMap::<Uuid, SurfaceBinding>::new();
    for binding in bindings.drain(..) {
        if let Some(existing) = by_id.get_mut(&binding.surface_binding_id) {
            ensure_same_surface_binding_identity(existing, &binding)?;
            existing.active_to =
                merge_replayed_binding_active_to(existing.active_to, binding.active_to)?;
            existing.canonicality_state = merge_replayed_canonicality(
                existing.canonicality_state,
                binding.canonicality_state,
            );
        } else {
            by_id.insert(binding.surface_binding_id, binding);
        }
    }

    bindings.extend(by_id.into_values());
    Ok(())
}

fn close_incoming_binding_group(bindings: &mut [SurfaceBinding]) {
    let mut previous_excluding_binding = None::<usize>;
    for index in 0..bindings.len() {
        if !surface_binding_exclusion_applies(bindings[index].canonicality_state) {
            continue;
        }

        if let Some(previous_index) = previous_excluding_binding {
            let next_active_from = bindings[index].active_from;
            let previous = &mut bindings[previous_index];
            if previous.active_from < next_active_from
                && previous
                    .active_to
                    .is_none_or(|active_to| active_to > next_active_from)
            {
                previous.active_to = Some(next_active_from);
            }
        }

        previous_excluding_binding = Some(index);
    }
}

fn trim_incoming_bindings_at_existing_starts(
    incoming: &mut [SurfaceBinding],
    existing: &[SurfaceBinding],
) {
    for binding in incoming
        .iter_mut()
        .filter(|binding| surface_binding_exclusion_applies(binding.canonicality_state))
    {
        let Some(close_at) = existing
            .iter()
            .filter(|existing| {
                existing.logical_name_id == binding.logical_name_id
                    && existing.surface_binding_id != binding.surface_binding_id
                    && surface_binding_exclusion_applies(existing.canonicality_state)
                    && binding.active_from < existing.active_from
                    && binding
                        .active_to
                        .is_none_or(|active_to| active_to > existing.active_from)
            })
            .map(|existing| existing.active_from)
            .min()
        else {
            continue;
        };

        binding.active_to = Some(close_at);
    }
}

async fn orphan_stale_overlapping_surface_bindings(
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
    for (surface_binding_id, resource_id) in candidates {
        surface_binding_ids.push(surface_binding_id);
        resource_ids.push(resource_id);
    }

    let rows = sqlx::query_scalar::<_, Uuid>(
        r#"
        WITH candidate(surface_binding_id, resource_id) AS (
            SELECT *
            FROM unnest($1::UUID[], $2::UUID[])
        ),
        event_backed AS (
            SELECT DISTINCT normalized_events.resource_id
            FROM normalized_events
            JOIN candidate
              ON candidate.resource_id = normalized_events.resource_id
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
              WHERE event_backed.resource_id = candidate.resource_id
          )
        RETURNING surface_bindings.surface_binding_id
        "#,
    )
    .bind(&surface_binding_ids)
    .bind(&resource_ids)
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

fn stale_overlapping_surface_binding_candidates(
    incoming: &[SurfaceBinding],
    existing: &[SurfaceBinding],
) -> BTreeMap<Uuid, Uuid> {
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
            candidates.insert(existing.surface_binding_id, existing.resource_id);
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

fn ensure_same_surface_binding_identity(
    existing: &SurfaceBinding,
    incoming: &SurfaceBinding,
) -> Result<()> {
    if existing.logical_name_id != incoming.logical_name_id
        || existing.resource_id != incoming.resource_id
        || existing.binding_kind != incoming.binding_kind
        || existing.active_from != incoming.active_from
        || existing.chain_id != incoming.chain_id
        || existing.block_hash != incoming.block_hash
        || existing.block_number != incoming.block_number
        || existing.provenance != incoming.provenance
    {
        bail!(
            "surface binding identity mismatch for {}",
            existing.surface_binding_id
        );
    }

    Ok(())
}

fn merge_replayed_binding_active_to(
    current: Option<OffsetDateTime>,
    incoming: Option<OffsetDateTime>,
) -> Result<Option<OffsetDateTime>> {
    match (current, incoming) {
        (Some(current), Some(incoming)) => Ok(Some(current.min(incoming))),
        (Some(current), _) => Ok(Some(current)),
        (None, incoming) => Ok(incoming),
    }
}

fn merge_replayed_canonicality(
    current: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    match incoming {
        CanonicalityState::Orphaned => CanonicalityState::Orphaned,
        CanonicalityState::Observed => {
            if current == CanonicalityState::Orphaned {
                CanonicalityState::Observed
            } else {
                current
            }
        }
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized => {
            if current == CanonicalityState::Orphaned || incoming.rank() > current.rank() {
                incoming
            } else {
                current
            }
        }
    }
}

pub(super) async fn build_name_surface(
    pool: &PgPool,
    name: &NameMetadata,
    reference: Option<&ObservationRef>,
) -> Result<Option<NameSurface>> {
    let Some(reference) = reference else {
        return Ok(None);
    };

    if let Some(existing) =
        load_name_surface_including_noncanonical(pool, &name.logical_name_id).await?
    {
        return Ok(Some(NameSurface {
            logical_name_id: existing.logical_name_id,
            namespace: existing.namespace,
            input_name: existing.input_name,
            canonical_display_name: existing.canonical_display_name,
            normalized_name: existing.normalized_name,
            dns_encoded_name: existing.dns_encoded_name,
            namehash: existing.namehash,
            labelhashes: existing.labelhashes,
            normalizer_version: existing.normalizer_version,
            normalization_warnings: existing.normalization_warnings,
            normalization_errors: existing.normalization_errors,
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: json!({
                "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                "logical_name_id": name.logical_name_id,
            }),
            canonicality_state: reference.canonicality_state,
        }));
    }

    Ok(Some(NameSurface {
        logical_name_id: name.logical_name_id.clone(),
        namespace: name.namespace.clone(),
        input_name: name.input_name.clone(),
        canonical_display_name: name.canonical_display_name.clone(),
        normalized_name: name.normalized_name.clone(),
        dns_encoded_name: name.dns_encoded_name.clone(),
        namehash: name.namehash.clone(),
        labelhashes: name.labelhashes.clone(),
        normalizer_version: name.normalizer_version.clone(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: reference.chain_id.clone(),
        block_hash: reference.block_hash.clone(),
        block_number: reference.block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "logical_name_id": name.logical_name_id,
            "source_event": "registrar_name_observation",
        }),
        canonicality_state: reference.canonicality_state,
    }))
}

pub(super) async fn build_surface_binding(
    pool: &PgPool,
    logical_name_id: &str,
    segment: &BindingSegment,
    chain: &str,
) -> Result<SurfaceBinding> {
    if let Some(existing) =
        load_surface_binding_including_noncanonical(pool, segment.surface_binding_id).await?
    {
        return Ok(SurfaceBinding {
            surface_binding_id: existing.surface_binding_id,
            logical_name_id: existing.logical_name_id,
            resource_id: existing.resource_id,
            binding_kind: existing.binding_kind,
            active_from: existing.active_from,
            active_to: segment.active_to.or(existing.active_to),
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: existing.provenance,
            canonicality_state: segment.anchor_ref.canonicality_state,
        });
    }

    Ok(SurfaceBinding {
        surface_binding_id: segment.surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id: segment.authority.resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: segment.active_from,
        active_to: segment.active_to,
        chain_id: chain.to_owned(),
        block_hash: segment.anchor_ref.block_hash.clone(),
        block_number: segment.anchor_ref.block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "authority_kind": segment.authority.kind.as_str(),
            "authority_key": segment.authority.authority_key,
        }),
        canonicality_state: segment.anchor_ref.canonicality_state,
    })
}

pub(super) async fn prepend_existing_open_binding_closures(
    pool: &PgPool,
    bindings: &mut Vec<SurfaceBinding>,
) -> Result<usize> {
    if bindings.is_empty() {
        return Ok(0);
    }

    let mut closure_points = BTreeMap::<String, (OffsetDateTime, CanonicalityState)>::new();
    let incoming_binding_ids = bindings
        .iter()
        .map(|binding| binding.surface_binding_id)
        .collect::<HashSet<_>>();
    for binding in bindings
        .iter()
        .filter(|binding| surface_binding_exclusion_applies(binding.canonicality_state))
    {
        closure_points
            .entry(binding.logical_name_id.clone())
            .and_modify(|(active_from, canonicality_state)| {
                if binding.active_from < *active_from {
                    *active_from = binding.active_from;
                    *canonicality_state = binding.canonicality_state;
                }
            })
            .or_insert((binding.active_from, binding.canonicality_state));
    }

    if closure_points.is_empty() {
        return Ok(0);
    }

    let logical_name_ids = closure_points.keys().cloned().collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            active_to,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        FROM surface_bindings
        WHERE logical_name_id = ANY($1)
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY logical_name_id, active_from, surface_binding_id
        "#,
    )
    .bind(&logical_name_ids)
    .fetch_all(pool)
    .await
    .context("failed to load existing surface bindings for restricted authority replay")?;

    let mut existing_bindings = rows
        .into_iter()
        .map(decode_adapter_surface_binding)
        .collect::<Result<Vec<_>>>()?;

    orphan_stale_overlapping_surface_bindings(pool, bindings, &mut existing_bindings).await?;

    trim_incoming_bindings_at_existing_starts(bindings, &existing_bindings);

    let mut closures = Vec::new();
    for existing in existing_bindings {
        let Some((close_at, canonicality_state)) = closure_points.get(&existing.logical_name_id)
        else {
            continue;
        };
        if incoming_binding_ids.contains(&existing.surface_binding_id)
            || existing.active_from >= *close_at
            || existing
                .active_to
                .is_some_and(|active_to| active_to <= *close_at)
        {
            continue;
        }

        closures.push(SurfaceBinding {
            active_to: Some(*close_at),
            canonicality_state: *canonicality_state,
            ..existing
        });
    }

    if closures.is_empty() {
        return Ok(0);
    }

    let closure_count = closures.len();
    closures.append(bindings);
    *bindings = closures;

    Ok(closure_count)
}

fn surface_binding_exclusion_applies(canonicality_state: CanonicalityState) -> bool {
    matches!(
        canonicality_state,
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized
    )
}

fn decode_adapter_surface_binding(row: sqlx::postgres::PgRow) -> Result<SurfaceBinding> {
    Ok(SurfaceBinding {
        surface_binding_id: sql_row::get(&row, "surface_binding_id")?,
        logical_name_id: sql_row::get(&row, "logical_name_id")?,
        resource_id: sql_row::get(&row, "resource_id")?,
        binding_kind: sql_row::get(&row, "binding_kind")?,
        active_from: sql_row::get(&row, "active_from")?,
        active_to: sql_row::get(&row, "active_to")?,
        chain_id: sql_row::get(&row, "chain_id")?,
        block_hash: sql_row::get(&row, "block_hash")?,
        block_number: sql_row::get(&row, "block_number")?,
        provenance: sql_row::get(&row, "provenance")?,
        canonicality_state: sql_row::get(&row, "canonicality_state")?,
    })
}

#[cfg(test)]
mod tests;
