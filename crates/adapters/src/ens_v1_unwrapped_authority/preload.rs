use super::*;
use bigname_storage::sql_row;

mod registrar_history;
mod registrar_state;
mod resolver;
mod support;
mod wrapper_registry;

pub(super) use registrar_history::{
    empty_preloaded_history, preload_registrar_history, preload_selected_registrar_lease,
    preload_superseded_registrar_lease, registrar_labelhash_from_authority_key,
};
use registrar_state::*;
use resolver::*;
use support::*;
pub(in crate::ens_v1_unwrapped_authority) use wrapper_registry::preload_registry_history;
use wrapper_registry::{
    load_latest_registry_owner_before_block, load_latest_wrapper_state_before_block,
    load_selected_wrapper_state_before_replay, preload_wrapper_history,
};

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

#[derive(Clone, Debug, Default)]
pub(in crate::ens_v1_unwrapped_authority) struct PreloadedRegistryOwnerState {
    owner: Option<String>,
    reference: Option<ObservationRef>,
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

const CANONICALITY_STATE_FILTER: &str = r#"
IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
)
"#;

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

    let rows = sqlx::query(&format!(
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
          AND surface.canonicality_state {CANONICALITY_STATE_FILTER}
          AND binding.canonicality_state {CANONICALITY_STATE_FILTER}
          AND resource.canonicality_state {CANONICALITY_STATE_FILTER}
        ORDER BY
            surface.logical_name_id,
            binding.active_from DESC,
            binding.block_number DESC,
            binding.surface_binding_id
        "#
    ))
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

    let mut registrar_scopes = Vec::with_capacity(rows.len());
    for row in &rows {
        let resource_provenance: Value = sql_row::get(row, "resource_provenance")?;
        let lower_block_number = if resource_provenance
            .get("authority_kind")
            .and_then(Value::as_str)
            == Some("registry_only")
        {
            0
        } else {
            row.get("binding_block_number")
        };
        registrar_scopes.push(RegistrarStateScope {
            logical_name_id: row.get("logical_name_id"),
            lower_block_number,
        });
    }
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
    let mut wrapper_state =
        load_latest_wrapper_state_before_block(pool, &logical_name_ids, boundary_block).await?;
    for (authority_key, selected_state) in
        load_selected_wrapper_state_before_replay(pool, &logical_name_ids, raw_logs).await?
    {
        let state = wrapper_state.entry(authority_key).or_default();
        if selected_state.owner.is_some() {
            state.owner = selected_state.owner;
        }
        if selected_state.fuses.is_some() {
            state.fuses = selected_state.fuses;
        }
        if selected_state.expiry.is_some() {
            state.expiry = selected_state.expiry;
        }
    }
    let registry_owner_state =
        load_latest_registry_owner_before_block(pool, &logical_name_ids, boundary_block).await?;
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
                &wrapper_state,
            )?,
            "registry_only" => {
                preload_registry_history(
                    history,
                    &resource_provenance,
                    &binding_ref,
                    surface_binding_id,
                    resource_id,
                    registry_owner_state.get(&logical_name_id),
                );
                preload_superseded_registrar_lease(
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
