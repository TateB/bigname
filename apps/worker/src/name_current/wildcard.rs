use anyhow::{Context, Result};
use bigname_storage::SurfaceBindingKind;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::json::{json_str, normalize_resolver_address};
use super::resolution::relevant_event_chain_position;
use super::types::{CurrentBindingContext, NameSurfaceSeed, RelevantEvent, WildcardSourceContext};
use super::{
    CANONICAL_STATE_FILTER, ENS_NAMESPACE, EVENT_KIND_RECORD_VERSION_CHANGED,
    EVENT_KIND_RESOLVER_CHANGED,
};

pub(super) async fn load_wildcard_source_context(
    pool: &PgPool,
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
) -> Result<Option<WildcardSourceContext>> {
    if name.namespace != ENS_NAMESPACE
        || current_binding
            .is_none_or(|binding| binding.binding_kind != SurfaceBindingKind::ObservedWildcardPath)
    {
        return Ok(None);
    }

    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ns.logical_name_id,
            ns.namespace,
            ns.canonical_display_name,
            ns.normalized_name,
            ns.namehash,
            sb.resource_id
        FROM name_surfaces ns
        JOIN surface_bindings sb
          ON sb.logical_name_id = ns.logical_name_id
         AND sb.active_to IS NULL
         AND sb.canonicality_state {CANONICAL_STATE_FILTER}
        JOIN resources r
          ON r.resource_id = sb.resource_id
         AND r.canonicality_state {CANONICAL_STATE_FILTER}
        WHERE ns.namespace = $1
          AND ns.logical_name_id <> $2
          AND ns.canonicality_state {CANONICAL_STATE_FILTER}
          AND $3 LIKE ('%.' || ns.normalized_name)
        ORDER BY char_length(ns.normalized_name) DESC, sb.active_from DESC, sb.surface_binding_id DESC
        LIMIT 8
        "#
    ))
    .bind(&name.namespace)
    .bind(&name.logical_name_id)
    .bind(&name.normalized_name)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load wildcard source candidates for {}",
            name.logical_name_id
        )
    })?;

    for row in rows {
        let source_normalized_name = row
            .try_get::<String, _>("normalized_name")
            .context("missing wildcard source normalized_name")?;
        let Some(matched_labels) =
            wildcard_matched_labels(&name.normalized_name, &source_normalized_name)
        else {
            continue;
        };
        let logical_name_id = row
            .try_get::<String, _>("logical_name_id")
            .context("missing wildcard source logical_name_id")?;
        let resource_id = row
            .try_get::<Uuid, _>("resource_id")
            .context("missing wildcard source resource_id")?;
        let Some((resolver_event, boundary_event)) =
            load_wildcard_source_events(pool, &logical_name_id, resource_id).await?
        else {
            continue;
        };

        return Ok(Some(WildcardSourceContext {
            logical_name_id,
            namespace: row
                .try_get("namespace")
                .context("missing wildcard source namespace")?,
            normalized_name: source_normalized_name,
            canonical_display_name: row
                .try_get("canonical_display_name")
                .context("missing wildcard source canonical_display_name")?,
            namehash: row
                .try_get("namehash")
                .context("missing wildcard source namehash")?,
            resource_id,
            resolver_event,
            boundary_event,
            matched_labels,
        }));
    }

    Ok(None)
}

async fn load_wildcard_source_events(
    pool: &PgPool,
    logical_name_id: &str,
    resource_id: Uuid,
) -> Result<Option<(RelevantEvent, RelevantEvent)>> {
    let event_kinds = vec![
        EVENT_KIND_RESOLVER_CHANGED.to_owned(),
        EVENT_KIND_RECORD_VERSION_CHANGED.to_owned(),
    ];
    let events = sqlx::query_as::<_, RelevantEvent>(&format!(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.resource_id,
            ne.event_kind,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            mv.manifest_version AS source_manifest_version,
            mv.namespace AS source_manifest_namespace,
            mv.source_family AS source_manifest_source_family,
            mv.chain AS source_manifest_chain,
            mv.deployment_epoch AS source_manifest_deployment_epoch,
            mv.rollout_status::TEXT AS source_manifest_rollout_status,
            mcf.status::TEXT AS exact_name_profile_status,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp,
            ne.raw_fact_ref,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.after_state
        FROM normalized_events ne
        LEFT JOIN chain_lineage rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        LEFT JOIN manifest_versions mv
          ON mv.manifest_id = ne.source_manifest_id
        LEFT JOIN manifest_capability_flags mcf
          ON mcf.manifest_id = ne.source_manifest_id
         AND mcf.capability_name = 'exact_name_profile'
        WHERE ne.namespace = $1
          AND ne.logical_name_id = $2
          AND ne.resource_id = $3
          AND ne.event_kind = ANY($4::TEXT[])
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY
            ne.block_number DESC NULLS LAST,
            COALESCE(ne.log_index, -1) DESC,
            ne.normalized_event_id DESC
        LIMIT 16
        "#
    ))
    .bind(ENS_NAMESPACE)
    .bind(logical_name_id)
    .bind(resource_id)
    .bind(&event_kinds)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load wildcard source events for logical_name_id {logical_name_id}")
    })?;
    let resolver_event = events
        .iter()
        .find(|event| {
            event.event_kind == EVENT_KIND_RESOLVER_CHANGED
                && normalize_resolver_address(
                    json_str(&event.after_state, &["resolver"]).as_deref(),
                )
                .is_some()
                && event.chain_id.is_some()
        })
        .cloned();
    let boundary_event = events.iter().find(|event| {
        matches!(
            event.event_kind.as_str(),
            EVENT_KIND_RECORD_VERSION_CHANGED | EVENT_KIND_RESOLVER_CHANGED
        ) && relevant_event_chain_position(event).is_some()
    });

    Ok(resolver_event.zip(boundary_event.cloned()))
}

fn wildcard_matched_labels(requested_name: &str, source_name: &str) -> Option<Vec<String>> {
    let suffix = format!(".{source_name}");
    let prefix = requested_name.strip_suffix(&suffix)?;
    let labels = prefix.split('.').map(str::to_owned).collect::<Vec<_>>();
    (!labels.is_empty() && labels.iter().all(|label| !label.is_empty())).then_some(labels)
}
