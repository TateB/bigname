use super::*;
use bigname_storage::sql_row;

mod lineage;
mod orphaning;

pub(super) use lineage::{build_resource, build_token_lineage, build_token_lineage_from_boundary};
use orphaning::orphan_stale_overlapping_surface_bindings;
#[cfg(test)]
use orphaning::stale_overlapping_surface_binding_candidates;

const EXISTING_SURFACE_BINDING_LOOKUP_NAME_CHUNK_SIZE: usize = 5_000;

#[cfg(test)]
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
    _pool: &PgPool,
    name: &NameMetadata,
    reference: Option<&ObservationRef>,
) -> Result<Option<NameSurface>> {
    let Some(reference) = reference else {
        return Ok(None);
    };

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
    _pool: &PgPool,
    logical_name_id: &str,
    segment: &BindingSegment,
    chain: &str,
) -> Result<SurfaceBinding> {
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

    let mut closures = Vec::new();
    let mut group_start = 0usize;
    while group_start < bindings.len() {
        let group_end = next_surface_binding_name_chunk_end(bindings, group_start);
        let incoming = &mut bindings[group_start..group_end];
        let logical_name_ids = incoming
            .iter()
            .filter(|binding| surface_binding_exclusion_applies(binding.canonicality_state))
            .map(|binding| binding.logical_name_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if logical_name_ids.is_empty() {
            group_start = group_end;
            continue;
        }

        let mut existing_bindings =
            load_existing_surface_bindings_for_logical_names(pool, &logical_name_ids).await?;

        orphan_stale_overlapping_surface_bindings(pool, incoming, &mut existing_bindings).await?;
        trim_incoming_bindings_at_existing_starts(incoming, &existing_bindings);

        let mut chunk_closures =
            existing_binding_closures_for_incoming(&existing_bindings, incoming);
        log_unresolved_surface_binding_overlaps(&existing_bindings, incoming, &chunk_closures);
        closures.append(&mut chunk_closures);

        group_start = group_end;
    }

    if closures.is_empty() {
        return Ok(0);
    }

    let closure_count = closures.len();
    let mut next_bindings = closures;
    next_bindings.append(bindings);
    *bindings = next_bindings;

    Ok(closure_count)
}

fn next_surface_binding_name_chunk_end(bindings: &[SurfaceBinding], start: usize) -> usize {
    let mut end = start;
    let mut distinct_names = 0usize;
    while end < bindings.len() && distinct_names < EXISTING_SURFACE_BINDING_LOOKUP_NAME_CHUNK_SIZE {
        let logical_name_id = bindings[end].logical_name_id.as_str();
        distinct_names += 1;
        end += 1;
        while end < bindings.len() && bindings[end].logical_name_id == logical_name_id {
            end += 1;
        }
    }
    end
}

async fn load_existing_surface_bindings_for_logical_names(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<Vec<SurfaceBinding>> {
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
    .bind(logical_name_ids)
    .fetch_all(pool)
    .await
    .context("failed to load existing surface bindings for restricted authority replay")?;

    rows.into_iter()
        .map(decode_adapter_surface_binding)
        .collect()
}

fn log_unresolved_surface_binding_overlaps(
    existing_bindings: &[SurfaceBinding],
    incoming: &[SurfaceBinding],
    closures: &[SurfaceBinding],
) {
    let closure_active_tos = closures
        .iter()
        .filter_map(|closure| {
            closure
                .active_to
                .map(|active_to| (closure.surface_binding_id, active_to))
        })
        .collect::<HashMap<_, _>>();
    let incoming_same_id_active_tos = incoming
        .iter()
        .filter_map(|binding| {
            binding
                .active_to
                .map(|active_to| (binding.surface_binding_id, active_to))
        })
        .fold(
            HashMap::new(),
            |mut map, (surface_binding_id, active_to)| {
                map.entry(surface_binding_id)
                    .and_modify(|current: &mut OffsetDateTime| *current = (*current).min(active_to))
                    .or_insert(active_to);
                map
            },
        );
    let incoming_by_name = incoming
        .iter()
        .filter(|binding| surface_binding_exclusion_applies(binding.canonicality_state))
        .fold(
            BTreeMap::<&str, Vec<&SurfaceBinding>>::new(),
            |mut map, binding| {
                map.entry(binding.logical_name_id.as_str())
                    .or_default()
                    .push(binding);
                map
            },
        );

    let mut unresolved_count = 0usize;
    let mut samples = Vec::new();
    for existing in existing_bindings
        .iter()
        .filter(|binding| surface_binding_exclusion_applies(binding.canonicality_state))
    {
        let Some(incoming_for_name) = incoming_by_name.get(existing.logical_name_id.as_str())
        else {
            continue;
        };
        let planned_active_to = closure_active_tos
            .get(&existing.surface_binding_id)
            .copied()
            .or_else(|| {
                incoming_same_id_active_tos
                    .get(&existing.surface_binding_id)
                    .copied()
            });
        let existing_active_to = planned_active_to
            .map(|active_to| {
                existing
                    .active_to
                    .map_or(active_to, |current| current.min(active_to))
            })
            .or(existing.active_to);
        for incoming in incoming_for_name {
            if incoming.surface_binding_id == existing.surface_binding_id {
                continue;
            }
            if !surface_binding_ranges_overlap_parts(
                existing.active_from,
                existing_active_to,
                incoming.active_from,
                incoming.active_to,
            ) {
                continue;
            }
            unresolved_count += 1;
            if samples.len() < 5 {
                samples.push(json!({
                    "logical_name_id": existing.logical_name_id.clone(),
                    "existing_surface_binding_id": existing.surface_binding_id,
                    "existing_resource_id": existing.resource_id,
                    "existing_active_from": existing.active_from.unix_timestamp(),
                    "existing_active_to": existing_active_to.map(|value| value.unix_timestamp()),
                    "incoming_surface_binding_id": incoming.surface_binding_id,
                    "incoming_resource_id": incoming.resource_id,
                    "incoming_active_from": incoming.active_from.unix_timestamp(),
                    "incoming_active_to": incoming.active_to.map(|value| value.unix_timestamp()),
                }));
            }
        }
    }

    if unresolved_count > 0 {
        tracing::warn!(
            adapter = DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            unresolved_surface_binding_overlap_count = unresolved_count,
            unresolved_surface_binding_overlap_samples = ?samples,
            "restricted authority replay left overlapping surface bindings after closure planning"
        );
    }
}

fn surface_binding_ranges_overlap_parts(
    left_active_from: OffsetDateTime,
    left_active_to: Option<OffsetDateTime>,
    right_active_from: OffsetDateTime,
    right_active_to: Option<OffsetDateTime>,
) -> bool {
    right_active_to.is_none_or(|right_active_to| left_active_from < right_active_to)
        && left_active_to.is_none_or(|left_active_to| right_active_from < left_active_to)
}

fn existing_binding_closures_for_incoming(
    existing_bindings: &[SurfaceBinding],
    incoming: &[SurfaceBinding],
) -> Vec<SurfaceBinding> {
    let incoming_binding_ids = incoming
        .iter()
        .map(|binding| binding.surface_binding_id)
        .collect::<HashSet<_>>();
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

    let mut closures = Vec::new();
    for existing in existing_bindings {
        if incoming_binding_ids.contains(&existing.surface_binding_id) {
            continue;
        }

        let Some((close_at, canonicality_state)) =
            next_incoming_binding_start(existing, &incoming_by_name)
        else {
            continue;
        };

        closures.push(SurfaceBinding {
            active_to: Some(close_at),
            canonicality_state,
            ..existing.clone()
        });
    }

    closures
}

fn next_incoming_binding_start(
    existing: &SurfaceBinding,
    incoming_by_name: &BTreeMap<&str, Vec<&SurfaceBinding>>,
) -> Option<(OffsetDateTime, CanonicalityState)> {
    incoming_by_name
        .get(existing.logical_name_id.as_str())?
        .iter()
        .filter(|incoming| incoming.surface_binding_id != existing.surface_binding_id)
        .filter(|incoming| existing.active_from < incoming.active_from)
        .filter(|incoming| {
            existing
                .active_to
                .is_none_or(|active_to| active_to > incoming.active_from)
        })
        .min_by(|left, right| {
            left.active_from
                .cmp(&right.active_from)
                .then_with(|| left.surface_binding_id.cmp(&right.surface_binding_id))
        })
        .map(|incoming| (incoming.active_from, incoming.canonicality_state))
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
