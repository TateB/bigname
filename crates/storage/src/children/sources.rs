use anyhow::{Context, Result};
use futures_util::{Stream, StreamExt};
use sqlx::{PgPool, Postgres, postgres::PgArguments, query::Query};

use super::{
    BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY, ENSV1_SUBREGISTRY_SOURCE_FAMILY,
    ENSV2_REGISTRY_DERIVATION_KIND, ENSV2_REGISTRY_SOURCE_FAMILY, ENSV2_ROOT_SOURCE_FAMILY,
    PARENT_EVENT_KIND, REGISTRATION_GRANTED_EVENT_KIND, REGISTRATION_RELEASED_EVENT_KIND,
    REGISTRATION_RENEWED_EVENT_KIND, SUBREGISTRY_DERIVATION_KIND, SUBREGISTRY_EVENT_KIND,
    source_decode::decode_declared_child_event_source, types::DeclaredChildEventSource,
};

/// Load the latest canonical declared-child subregistry event per child surface.
pub async fn load_canonical_declared_child_sources(
    pool: &PgPool,
    parent_logical_name_id: Option<&str>,
) -> Result<Vec<DeclaredChildEventSource>> {
    let rows = canonical_declared_child_sources_query(parent_logical_name_id)
        .fetch_all(pool)
        .await
        .with_context(|| declared_child_sources_context(parent_logical_name_id))?;

    rows.into_iter()
        .map(decode_declared_child_event_source)
        .collect()
}

/// Stream the latest canonical declared-child subregistry event per child surface.
pub fn stream_canonical_declared_child_sources<'a>(
    pool: &'a PgPool,
    parent_logical_name_id: Option<&'a str>,
) -> impl Stream<Item = Result<DeclaredChildEventSource>> + 'a {
    let context = declared_child_sources_context(parent_logical_name_id);
    canonical_declared_child_sources_query(parent_logical_name_id)
        .fetch(pool)
        .map(move |row| {
            row.with_context(|| context.clone())
                .and_then(decode_declared_child_event_source)
        })
}

fn canonical_declared_child_sources_query<'a>(
    parent_logical_name_id: Option<&'a str>,
) -> Query<'a, Postgres, PgArguments> {
    sqlx::query(
        r#"
        WITH target_v1_child_nodes AS (
            SELECT DISTINCT candidate_ne.after_state ->> 'child_node' AS child_node
            FROM normalized_events candidate_ne
            JOIN name_surfaces candidate_parent
              ON candidate_parent.namehash = candidate_ne.after_state ->> 'parent_node'
            WHERE $5::TEXT IS NOT NULL
              AND candidate_ne.event_kind = $1
              AND candidate_ne.derivation_kind = $2
              AND candidate_ne.source_family IN ($3, $4)
              AND candidate_ne.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
              AND candidate_ne.after_state ->> 'child_node' IS NOT NULL
              AND candidate_parent.logical_name_id = $5
              AND candidate_parent.namespace = candidate_ne.namespace
              AND candidate_parent.chain_id = candidate_ne.chain_id
              AND candidate_parent.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
        ),
        ranked_v1_sources AS (
            SELECT
                parent.logical_name_id AS parent_logical_name_id,
                child.logical_name_id AS child_logical_name_id,
                child.namespace,
                child.canonical_display_name,
                child.normalized_name,
                child.namehash,
                ne.normalized_event_id,
                ne.event_identity,
                ne.source_family,
                ne.manifest_version,
                ne.source_manifest_id,
                ne.chain_id,
                ne.block_number,
                ne.block_hash,
                ne.transaction_hash,
                ne.log_index,
                ne.raw_fact_ref,
                ARRAY[ne.normalized_event_id]::BIGINT[] AS normalized_event_ids,
                jsonb_build_array(ne.raw_fact_ref) AS raw_fact_refs,
                jsonb_build_array(jsonb_build_object(
                    'source_manifest_id', ne.source_manifest_id,
                    'source_family', ne.source_family,
                    'manifest_version', ne.manifest_version
                )) AS manifest_versions,
                COALESCE((ne.after_state ->> 'tombstone')::BOOLEAN, FALSE) AS tombstone,
                COALESCE((ne.after_state ->> 'active_edge')::BOOLEAN, FALSE) AS active_edge,
                ROW_NUMBER() OVER (
                    PARTITION BY child.logical_name_id
                    ORDER BY
                        ne.block_number DESC,
                        ne.log_index DESC,
                        ne.normalized_event_id DESC
                ) AS current_child_rank
            FROM normalized_events ne
            JOIN name_surfaces parent
              ON parent.namehash = ne.after_state ->> 'parent_node'
            JOIN name_surfaces child
              ON child.namehash = ne.after_state ->> 'child_node'
            WHERE ne.event_kind = $1
              AND ne.derivation_kind = $2
              AND ne.source_family IN ($3, $4)
              AND parent.namespace = child.namespace
              AND parent.namespace = ne.namespace
              AND child.namespace = ne.namespace
              AND parent.chain_id = child.chain_id
              AND parent.chain_id = ne.chain_id
              AND child.chain_id = ne.chain_id
              AND ne.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
              AND (
                    $5::TEXT IS NULL
                    OR ne.after_state ->> 'child_node' IN (
                        SELECT child_node
                        FROM target_v1_child_nodes
                    )
              )
              AND parent.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
              AND child.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
        ),
        current_v1_sources AS (
            SELECT
                parent_logical_name_id,
                child_logical_name_id,
                namespace,
                canonical_display_name,
                normalized_name,
                namehash,
                normalized_event_id,
                event_identity,
                source_family,
                manifest_version,
                source_manifest_id,
                chain_id,
                block_number,
                block_hash,
                transaction_hash,
                log_index,
                raw_fact_ref,
                normalized_event_ids,
                raw_fact_refs,
                manifest_versions
            FROM ranked_v1_sources
            WHERE current_child_rank = 1
              AND tombstone = FALSE
              AND active_edge = TRUE
        ),
        ensv2_ranked_subregistries AS (
            SELECT
                ne.normalized_event_id,
                ne.event_identity,
                ne.source_family,
                ne.manifest_version,
                ne.source_manifest_id,
                ne.chain_id,
                ne.block_number,
                ne.block_hash,
                ne.transaction_hash,
                ne.log_index,
                ne.raw_fact_ref,
                ne.logical_name_id AS parent_logical_name_id,
                ne.after_state ->> 'from_contract_instance_id' AS from_contract_instance_id,
                ne.after_state ->> 'to_contract_instance_id' AS to_contract_instance_id,
                ROW_NUMBER() OVER (
                    PARTITION BY ne.logical_name_id
                    ORDER BY
                        ne.block_number DESC,
                        ne.log_index DESC,
                        ne.normalized_event_id DESC
                ) AS current_rank
            FROM normalized_events ne
            WHERE ne.event_kind = $6
              AND ne.derivation_kind = $7
              AND ne.source_family IN ($8, $9)
              AND ne.logical_name_id IS NOT NULL
              AND ne.after_state ->> 'from_contract_instance_id' IS NOT NULL
              AND ($5::TEXT IS NULL OR ne.logical_name_id = $5)
              AND ne.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
        ),
        ensv2_current_subregistries AS (
            SELECT *
            FROM ensv2_ranked_subregistries
            WHERE current_rank = 1
              AND to_contract_instance_id IS NOT NULL
        ),
        ensv2_ranked_parent_events AS (
            SELECT
                ne.normalized_event_id,
                ne.event_identity,
                ne.source_family,
                ne.manifest_version,
                ne.source_manifest_id,
                ne.chain_id,
                ne.block_number,
                ne.block_hash,
                ne.transaction_hash,
                ne.log_index,
                ne.raw_fact_ref,
                ne.after_state ->> 'registry_contract_instance_id' AS registry_contract_instance_id,
                ne.after_state ->> 'parent_contract_instance_id' AS parent_contract_instance_id,
                ne.after_state ->> 'registry_name' AS registry_name,
                ROW_NUMBER() OVER (
                    PARTITION BY ne.after_state ->> 'registry_contract_instance_id'
                    ORDER BY
                        ne.block_number DESC,
                        ne.log_index DESC,
                        ne.normalized_event_id DESC
                ) AS current_rank
            FROM normalized_events ne
            WHERE ne.event_kind = $10
              AND ne.derivation_kind = $7
              AND ne.source_family IN ($8, $9)
              AND ne.after_state ->> 'registry_contract_instance_id' IS NOT NULL
              AND ne.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
        ),
        ensv2_current_parent_events AS (
            SELECT *
            FROM ensv2_ranked_parent_events
            WHERE current_rank = 1
              AND parent_contract_instance_id IS NOT NULL
              AND registry_name IS NOT NULL
        ),
        ensv2_ranked_child_events AS (
            SELECT
                ne.normalized_event_id,
                ne.event_identity,
                ne.source_family,
                ne.manifest_version,
                ne.source_manifest_id,
                ne.chain_id,
                ne.block_number,
                ne.block_hash,
                ne.transaction_hash,
                ne.log_index,
                ne.raw_fact_ref,
                ne.logical_name_id AS child_logical_name_id,
                ne.event_kind,
                ne.after_state ->> 'registry_contract_instance_id' AS registry_contract_instance_id,
                ROW_NUMBER() OVER (
                    PARTITION BY ne.logical_name_id
                    ORDER BY
                        ne.block_number DESC,
                        ne.log_index DESC,
                        ne.normalized_event_id DESC
                ) AS current_rank
            FROM normalized_events ne
            WHERE ne.event_kind IN ($11, $12, $13)
              AND ne.derivation_kind = $7
              AND ne.source_family IN ($8, $9)
              AND ne.logical_name_id IS NOT NULL
              AND ne.after_state ->> 'registry_contract_instance_id' IS NOT NULL
              AND ne.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
        ),
        ensv2_current_child_events AS (
            SELECT *
            FROM ensv2_ranked_child_events
            WHERE current_rank = 1
              AND event_kind <> $13
        ),
        ensv2_sources AS (
            SELECT
                parent.logical_name_id AS parent_logical_name_id,
                child.logical_name_id AS child_logical_name_id,
                child.namespace,
                child.canonical_display_name,
                child.normalized_name,
                child.namehash,
                latest.normalized_event_id,
                latest.event_identity,
                latest.source_family,
                composite_manifest.manifest_version,
                latest.source_manifest_id,
                latest.chain_id,
                latest.block_number,
                latest.block_hash,
                latest.transaction_hash,
                latest.log_index,
                latest.raw_fact_ref,
                ARRAY[
                    subregistry.normalized_event_id,
                    parent_event.normalized_event_id,
                    child_event.normalized_event_id
                ]::BIGINT[] AS normalized_event_ids,
                jsonb_build_array(
                    subregistry.raw_fact_ref,
                    parent_event.raw_fact_ref,
                    child_event.raw_fact_ref
                ) AS raw_fact_refs,
                composite_manifest.manifest_versions
            FROM ensv2_current_subregistries subregistry
            JOIN name_surfaces parent
              ON parent.logical_name_id = subregistry.parent_logical_name_id
            JOIN ensv2_current_parent_events parent_event
              ON parent_event.registry_contract_instance_id = subregistry.to_contract_instance_id
             AND parent_event.parent_contract_instance_id = subregistry.from_contract_instance_id
             AND parent_event.registry_name = parent.normalized_name
            JOIN ensv2_current_child_events child_event
              ON child_event.registry_contract_instance_id = subregistry.to_contract_instance_id
             AND child_event.registry_contract_instance_id = parent_event.registry_contract_instance_id
            JOIN name_surfaces child
              ON child.logical_name_id = child_event.child_logical_name_id
            CROSS JOIN LATERAL (
                SELECT *
                FROM (
                    VALUES
                        (
                            subregistry.normalized_event_id,
                            subregistry.event_identity,
                            subregistry.source_family,
                            subregistry.manifest_version,
                            subregistry.source_manifest_id,
                            subregistry.chain_id,
                            subregistry.block_number,
                            subregistry.block_hash,
                            subregistry.transaction_hash,
                            subregistry.log_index,
                            subregistry.raw_fact_ref
                        ),
                        (
                            parent_event.normalized_event_id,
                            parent_event.event_identity,
                            parent_event.source_family,
                            parent_event.manifest_version,
                            parent_event.source_manifest_id,
                            parent_event.chain_id,
                            parent_event.block_number,
                            parent_event.block_hash,
                            parent_event.transaction_hash,
                            parent_event.log_index,
                            parent_event.raw_fact_ref
                        ),
                        (
                            child_event.normalized_event_id,
                            child_event.event_identity,
                            child_event.source_family,
                            child_event.manifest_version,
                            child_event.source_manifest_id,
                            child_event.chain_id,
                            child_event.block_number,
                            child_event.block_hash,
                            child_event.transaction_hash,
                            child_event.log_index,
                            child_event.raw_fact_ref
                        )
                ) AS candidates(
                    normalized_event_id,
                    event_identity,
                    source_family,
                    manifest_version,
                    source_manifest_id,
                    chain_id,
                    block_number,
                    block_hash,
                    transaction_hash,
                    log_index,
                    raw_fact_ref
                )
                ORDER BY
                    block_number DESC,
                    log_index DESC,
                    normalized_event_id DESC
                LIMIT 1
            ) latest
            CROSS JOIN LATERAL (
                SELECT
                    MAX(manifest_version) AS manifest_version,
                    jsonb_agg(
                        jsonb_build_object(
                            'source_manifest_id', source_manifest_id,
                            'source_family', source_family,
                            'manifest_version', manifest_version
                        )
                        ORDER BY source_family ASC, source_manifest_id ASC NULLS FIRST, manifest_version ASC
                    ) AS manifest_versions
                FROM (
                    SELECT DISTINCT source_manifest_id, source_family, manifest_version
                    FROM (
                        VALUES
                            (
                                subregistry.source_manifest_id,
                                subregistry.source_family,
                                subregistry.manifest_version
                            ),
                            (
                                parent_event.source_manifest_id,
                                parent_event.source_family,
                                parent_event.manifest_version
                            ),
                            (
                                child_event.source_manifest_id,
                                child_event.source_family,
                                child_event.manifest_version
                            )
                    ) AS candidates(source_manifest_id, source_family, manifest_version)
                ) manifest_candidates
            ) composite_manifest
            WHERE parent.namespace = child.namespace
              AND parent.namespace = 'ens'
              AND child.namespace = 'ens'
              AND parent.chain_id = child.chain_id
              AND parent.chain_id = subregistry.chain_id
              AND parent.chain_id = parent_event.chain_id
              AND child.chain_id = child_event.chain_id
              AND parent.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
              AND child.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
              AND child.normalized_name <> parent.normalized_name
              AND right(child.normalized_name, length(parent.normalized_name) + 1) = concat('.', parent.normalized_name)
              AND array_length(string_to_array(child.normalized_name, '.'), 1)
                    = array_length(string_to_array(parent.normalized_name, '.'), 1) + 1
        ),
        current_sources AS (
            SELECT *
            FROM current_v1_sources
            UNION ALL
            SELECT *
            FROM ensv2_sources
        )
        SELECT
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            normalized_event_id,
            event_identity,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            normalized_event_ids,
            raw_fact_refs,
            manifest_versions
        FROM current_sources
        WHERE ($5::TEXT IS NULL OR parent_logical_name_id = $5)
        ORDER BY
            parent_logical_name_id ASC,
            canonical_display_name ASC,
            child_logical_name_id ASC
        "#,
    )
    .bind(SUBREGISTRY_EVENT_KIND)
    .bind(SUBREGISTRY_DERIVATION_KIND)
    .bind(ENSV1_SUBREGISTRY_SOURCE_FAMILY)
    .bind(BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY)
    .bind(parent_logical_name_id)
    .bind(SUBREGISTRY_EVENT_KIND)
    .bind(ENSV2_REGISTRY_DERIVATION_KIND)
    .bind(ENSV2_ROOT_SOURCE_FAMILY)
    .bind(ENSV2_REGISTRY_SOURCE_FAMILY)
    .bind(PARENT_EVENT_KIND)
    .bind(REGISTRATION_GRANTED_EVENT_KIND)
    .bind(REGISTRATION_RENEWED_EVENT_KIND)
    .bind(REGISTRATION_RELEASED_EVENT_KIND)
}

fn declared_child_sources_context(parent_logical_name_id: Option<&str>) -> String {
    match parent_logical_name_id {
        Some(parent_logical_name_id) => format!(
            "failed to load canonical declared child sources for parent_logical_name_id {parent_logical_name_id}"
        ),
        None => "failed to load canonical declared child sources".to_owned(),
    }
}

/// Back-compat alias for the generalized declared-child source loader.
pub async fn load_canonical_ens_v1_declared_child_sources(
    pool: &PgPool,
    parent_logical_name_id: Option<&str>,
) -> Result<Vec<DeclaredChildEventSource>> {
    load_canonical_declared_child_sources(pool, parent_logical_name_id).await
}
