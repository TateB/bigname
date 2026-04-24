use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};

use crate::projection_helpers::{
    checked_page_limit_i64, checked_page_size_usize, require_json_object, serialize_jsonb_field,
    take_json_array,
};

const DECLARED_SURFACE_CLASS: &str = "declared";
const SUBREGISTRY_EVENT_KIND: &str = "SubregistryChanged";
const PARENT_EVENT_KIND: &str = "ParentChanged";
const REGISTRATION_GRANTED_EVENT_KIND: &str = "RegistrationGranted";
const REGISTRATION_RENEWED_EVENT_KIND: &str = "RegistrationRenewed";
const REGISTRATION_RELEASED_EVENT_KIND: &str = "RegistrationReleased";
const SUBREGISTRY_DERIVATION_KIND: &str = "ens_v1_subregistry_changed";
const ENSV2_REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
const ENSV1_SUBREGISTRY_SOURCE_FAMILY: &str = "ens_v1_registry_l1";
const BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY: &str = "basenames_base_registry";
const ENSV2_ROOT_SOURCE_FAMILY: &str = "ens_v2_root_l1";
const ENSV2_REGISTRY_SOURCE_FAMILY: &str = "ens_v2_registry_l1";
const DEFAULT_CHILDREN_CURRENT_READ_FILTER: &str = r#"
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
"#;

/// Persisted current child-collection row for declared direct children only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentRow {
    pub parent_logical_name_id: String,
    pub child_logical_name_id: String,
    pub surface_class: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub provenance: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Storage-local keyset cursor for declared direct child collection reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentKeysetCursor {
    pub canonical_display_name: String,
    pub child_logical_name_id: String,
}

impl From<&ChildrenCurrentRow> for ChildrenCurrentKeysetCursor {
    fn from(row: &ChildrenCurrentRow) -> Self {
        Self {
            canonical_display_name: row.canonical_display_name.clone(),
            child_logical_name_id: row.child_logical_name_id.clone(),
        }
    }
}

/// Compact metadata for the full declared direct child filter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentSummary {
    pub parent_logical_name_id: String,
    pub child_count: i64,
    pub provenance_inputs: Vec<Value>,
    pub chain_positions: Vec<Value>,
    pub canonicality_summaries: Vec<Value>,
    pub last_recomputed_at: Option<OffsetDateTime>,
}

/// Bounded declared direct child page plus full-filter summary metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentPage {
    pub rows: Vec<ChildrenCurrentRow>,
    pub next_cursor: Option<ChildrenCurrentKeysetCursor>,
    pub summary: ChildrenCurrentSummary,
}

/// Canonical declared-child subregistry event seed for rebuilding declared child rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeclaredChildEventSource {
    pub parent_logical_name_id: String,
    pub child_logical_name_id: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub normalized_event_id: i64,
    pub event_identity: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub log_index: i64,
    pub raw_fact_ref: Value,
    pub normalized_event_ids: Vec<i64>,
    pub raw_fact_refs: Value,
    pub manifest_versions: Value,
}

/// Load declared direct child rows for one parent from the default canonical read set.
pub async fn load_children_current(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<Vec<ChildrenCurrentRow>> {
    load_children_current_internal(pool, parent_logical_name_id, false).await
}

/// Load declared direct child rows for one parent, including noncanonical parent or child surfaces.
pub async fn load_children_current_including_noncanonical(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<Vec<ChildrenCurrentRow>> {
    load_children_current_internal(pool, parent_logical_name_id, true).await
}

/// Load one bounded declared direct-child page from the default canonical read set.
pub async fn load_children_current_page(
    pool: &PgPool,
    parent_logical_name_id: &str,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<ChildrenCurrentPage> {
    let limit = checked_page_limit_i64(
        page_size,
        "children_current page_size must be positive",
        "children_current page_size is too large",
    )?;
    let page_size = checked_page_size_usize(
        page_size,
        "children_current page_size must be positive",
        "children_current page_size does not fit in usize",
    )?;

    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            cc.parent_logical_name_id,
            cc.child_logical_name_id,
            cc.surface_class,
            cc.namespace,
            cc.canonical_display_name,
            cc.normalized_name,
            cc.namehash,
            cc.provenance,
            cc.chain_positions,
            cc.canonicality_summary,
            cc.manifest_version,
            cc.last_recomputed_at
        FROM children_current cc
        JOIN name_surfaces parent
          ON parent.logical_name_id = cc.parent_logical_name_id
        JOIN name_surfaces child
          ON child.logical_name_id = cc.child_logical_name_id
        WHERE cc.parent_logical_name_id =
        "#,
    );
    builder.push_bind(parent_logical_name_id);
    builder.push(" AND cc.surface_class = ");
    builder.push_bind(DECLARED_SURFACE_CLASS);
    builder.push(DEFAULT_CHILDREN_CURRENT_READ_FILTER);

    if let Some(cursor) = cursor {
        builder.push(
            r#"
            AND (
                cc.canonical_display_name,
                cc.child_logical_name_id
            ) > (
            "#,
        );
        builder.push_bind(&cursor.canonical_display_name);
        builder.push(", ");
        builder.push_bind(&cursor.child_logical_name_id);
        builder.push(")");
    }

    builder.push(
        r#"
        ORDER BY
            cc.canonical_display_name ASC,
            cc.child_logical_name_id ASC
        LIMIT
        "#,
    );
    builder.push_bind(limit);

    let mut rows = builder
        .build()
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load children_current page for parent_logical_name_id {parent_logical_name_id}"
            )
        })?
        .into_iter()
        .map(decode_children_current_row)
        .collect::<Result<Vec<_>>>()?;

    let has_next_page = rows.len() > page_size;
    if has_next_page {
        rows.truncate(page_size);
    }
    let next_cursor = has_next_page
        .then(|| rows.last().map(ChildrenCurrentKeysetCursor::from))
        .flatten();

    let summary = load_children_current_summary(pool, parent_logical_name_id).await?;

    Ok(ChildrenCurrentPage {
        rows,
        next_cursor,
        summary,
    })
}

/// Load compact declared direct-child summaries for parent collection keys in input order.
pub async fn load_children_current_summaries(
    pool: &PgPool,
    parent_logical_name_ids: &[String],
) -> Result<Vec<ChildrenCurrentSummary>> {
    if parent_logical_name_ids.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        WITH requested AS (
            SELECT
                input.parent_logical_name_id,
                input.ordinal
            FROM UNNEST($1::TEXT[]) WITH ORDINALITY AS input(parent_logical_name_id, ordinal)
        )
        SELECT
            requested.parent_logical_name_id,
            COUNT(child.logical_name_id)::BIGINT AS child_count,
            COALESCE(
                jsonb_agg(
                    cc.provenance
                    ORDER BY cc.canonical_display_name ASC, cc.child_logical_name_id ASC
                ) FILTER (WHERE child.logical_name_id IS NOT NULL),
                '[]'::jsonb
            ) AS provenance_inputs,
            COALESCE(
                jsonb_agg(
                    cc.chain_positions
                    ORDER BY cc.canonical_display_name ASC, cc.child_logical_name_id ASC
                ) FILTER (WHERE child.logical_name_id IS NOT NULL),
                '[]'::jsonb
            ) AS chain_positions,
            COALESCE(
                jsonb_agg(
                    cc.canonicality_summary
                    ORDER BY cc.canonical_display_name ASC, cc.child_logical_name_id ASC
                ) FILTER (WHERE child.logical_name_id IS NOT NULL),
                '[]'::jsonb
            ) AS canonicality_summaries,
            MAX(cc.last_recomputed_at) FILTER (WHERE child.logical_name_id IS NOT NULL)
                AS last_recomputed_at
        FROM requested
        LEFT JOIN name_surfaces parent
          ON parent.logical_name_id = requested.parent_logical_name_id
         AND parent.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
         )
        LEFT JOIN children_current cc
          ON cc.parent_logical_name_id = requested.parent_logical_name_id
         AND cc.surface_class = $2
         AND parent.logical_name_id IS NOT NULL
        LEFT JOIN name_surfaces child
          ON child.logical_name_id = cc.child_logical_name_id
         AND child.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
         )
        GROUP BY
            requested.ordinal,
            requested.parent_logical_name_id
        ORDER BY requested.ordinal ASC
        "#,
    )
    .bind(parent_logical_name_ids)
    .bind(DECLARED_SURFACE_CLASS)
    .fetch_all(pool)
    .await
    .context("failed to load children_current summaries")?;

    rows.into_iter()
        .map(decode_children_current_summary)
        .collect()
}

/// Insert or replace current declared child rows for one or more parents.
pub async fn upsert_children_current_rows(
    pool: &PgPool,
    rows: &[ChildrenCurrentRow],
) -> Result<Vec<ChildrenCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for children_current upsert")?;

    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        validate_children_current_row(row)?;
        snapshots.push(upsert_children_current_row(&mut transaction, row).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit children_current upsert")?;

    Ok(snapshots)
}

/// Delete all declared child rows for one parent so a worker can rebuild that collection key.
pub async fn delete_children_current(pool: &PgPool, parent_logical_name_id: &str) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM children_current
        WHERE parent_logical_name_id = $1
          AND surface_class = $2
        "#,
    )
    .bind(parent_logical_name_id)
    .bind(DECLARED_SURFACE_CLASS)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to delete children_current rows for parent_logical_name_id {parent_logical_name_id}"
        )
    })
    .map(|result| result.rows_affected())
}

/// Clear the declared direct-child projection so a worker can perform a one-shot rebuild.
pub async fn clear_children_current(pool: &PgPool) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM children_current
        WHERE surface_class = $1
        "#,
    )
    .bind(DECLARED_SURFACE_CLASS)
    .execute(pool)
    .await
    .context("failed to clear children_current rows")
    .map(|result| result.rows_affected())
}

/// Load the latest canonical declared-child subregistry event per child surface.
pub async fn load_canonical_declared_child_sources(
    pool: &PgPool,
    parent_logical_name_id: Option<&str>,
) -> Result<Vec<DeclaredChildEventSource>> {
    let rows = sqlx::query(
        r#"
        WITH ranked_v1_sources AS (
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
    .fetch_all(pool)
    .await
    .with_context(|| match parent_logical_name_id {
        Some(parent_logical_name_id) => format!(
            "failed to load canonical declared child sources for parent_logical_name_id {parent_logical_name_id}"
        ),
        None => "failed to load canonical declared child sources".to_owned(),
    })?;

    rows.into_iter()
        .map(decode_declared_child_event_source)
        .collect()
}

/// Back-compat alias for the generalized declared-child source loader.
pub async fn load_canonical_ens_v1_declared_child_sources(
    pool: &PgPool,
    parent_logical_name_id: Option<&str>,
) -> Result<Vec<DeclaredChildEventSource>> {
    load_canonical_declared_child_sources(pool, parent_logical_name_id).await
}

async fn load_children_current_internal(
    pool: &PgPool,
    parent_logical_name_id: &str,
    include_noncanonical: bool,
) -> Result<Vec<ChildrenCurrentRow>> {
    let read_filter = if include_noncanonical {
        ""
    } else {
        DEFAULT_CHILDREN_CURRENT_READ_FILTER
    };

    let query = format!(
        r#"
        SELECT
            cc.parent_logical_name_id,
            cc.child_logical_name_id,
            cc.surface_class,
            cc.namespace,
            cc.canonical_display_name,
            cc.normalized_name,
            cc.namehash,
            cc.provenance,
            cc.chain_positions,
            cc.canonicality_summary,
            cc.manifest_version,
            cc.last_recomputed_at
        FROM children_current cc
        JOIN name_surfaces parent
          ON parent.logical_name_id = cc.parent_logical_name_id
        JOIN name_surfaces child
          ON child.logical_name_id = cc.child_logical_name_id
        WHERE cc.parent_logical_name_id = $1
          AND cc.surface_class = $2
        {read_filter}
        ORDER BY
            cc.canonical_display_name ASC,
            cc.child_logical_name_id ASC
        "#
    );

    let rows = sqlx::query(&query)
        .bind(parent_logical_name_id)
        .bind(DECLARED_SURFACE_CLASS)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load children_current rows for parent_logical_name_id {parent_logical_name_id}"
            )
        })?;

    rows.into_iter().map(decode_children_current_row).collect()
}

async fn load_children_current_summary(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<ChildrenCurrentSummary> {
    let parent_logical_name_ids = [parent_logical_name_id.to_owned()];
    let summaries = load_children_current_summaries(pool, &parent_logical_name_ids).await?;
    summaries
        .into_iter()
        .next()
        .with_context(|| {
            format!(
                "failed to load children_current summary for parent_logical_name_id {parent_logical_name_id}"
            )
        })
}

async fn upsert_children_current_row(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &ChildrenCurrentRow,
) -> Result<ChildrenCurrentRow> {
    let provenance = serialize_jsonb_field(
        &row.provenance,
        "failed to serialize children_current provenance",
    )?;
    let chain_positions = serialize_jsonb_field(
        &row.chain_positions,
        "failed to serialize children_current chain_positions",
    )?;
    let canonicality_summary = serialize_jsonb_field(
        &row.canonicality_summary,
        "failed to serialize children_current canonicality_summary",
    )?;

    let snapshot = sqlx::query(
        r#"
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            surface_class,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            provenance,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8::jsonb,
            $9::jsonb,
            $10::jsonb,
            $11,
            $12
        )
        ON CONFLICT (parent_logical_name_id, child_logical_name_id, surface_class) DO UPDATE
        SET
            namespace = EXCLUDED.namespace,
            canonical_display_name = EXCLUDED.canonical_display_name,
            normalized_name = EXCLUDED.normalized_name,
            namehash = EXCLUDED.namehash,
            provenance = EXCLUDED.provenance,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING
            parent_logical_name_id,
            child_logical_name_id,
            surface_class,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            provenance,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        "#,
    )
    .bind(&row.parent_logical_name_id)
    .bind(&row.child_logical_name_id)
    .bind(&row.surface_class)
    .bind(&row.namespace)
    .bind(&row.canonical_display_name)
    .bind(&row.normalized_name)
    .bind(&row.namehash)
    .bind(provenance)
    .bind(chain_positions)
    .bind(canonicality_summary)
    .bind(row.manifest_version)
    .bind(row.last_recomputed_at)
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert children_current row for parent_logical_name_id {} child_logical_name_id {}",
            row.parent_logical_name_id,
            row.child_logical_name_id
        )
    })?;

    decode_children_current_row(snapshot)
}

fn validate_children_current_row(row: &ChildrenCurrentRow) -> Result<()> {
    if row.parent_logical_name_id.trim().is_empty() {
        bail!("children_current row must include parent_logical_name_id");
    }
    if row.child_logical_name_id.trim().is_empty() {
        bail!("children_current row must include child_logical_name_id");
    }
    if row.parent_logical_name_id == row.child_logical_name_id {
        bail!(
            "children_current row {} cannot target itself as a child",
            row.parent_logical_name_id
        );
    }
    if row.surface_class != DECLARED_SURFACE_CLASS {
        bail!(
            "children_current row {} -> {} must use declared surface_class",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.namespace.trim().is_empty() {
        bail!(
            "children_current row {} -> {} must include namespace",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.normalized_name.trim().is_empty() {
        bail!(
            "children_current row {} -> {} must include normalized_name",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.canonical_display_name.trim().is_empty() {
        bail!(
            "children_current row {} -> {} must include canonical_display_name",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.namehash.trim().is_empty() {
        bail!(
            "children_current row {} -> {} must include namehash",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.child_logical_name_id != format!("{}:{}", row.namespace, row.normalized_name) {
        bail!(
            "children_current row {} -> {} does not match namespace {} and normalized_name {}",
            row.parent_logical_name_id,
            row.child_logical_name_id,
            row.namespace,
            row.normalized_name
        );
    }
    if row.manifest_version <= 0 {
        bail!(
            "children_current row {} -> {} has non-positive manifest_version {}",
            row.parent_logical_name_id,
            row.child_logical_name_id,
            row.manifest_version
        );
    }

    require_json_object(&row.provenance, || {
        format!(
            "children_current row {} -> {} field provenance must be a JSON object",
            row.parent_logical_name_id, row.child_logical_name_id
        )
    })?;
    require_json_object(&row.chain_positions, || {
        format!(
            "children_current row {} -> {} field chain_positions must be a JSON object",
            row.parent_logical_name_id, row.child_logical_name_id
        )
    })?;
    require_json_object(&row.canonicality_summary, || {
        format!(
            "children_current row {} -> {} field canonicality_summary must be a JSON object",
            row.parent_logical_name_id, row.child_logical_name_id
        )
    })?;

    Ok(())
}

fn decode_children_current_row(row: PgRow) -> Result<ChildrenCurrentRow> {
    let surface_class = row
        .try_get::<String, _>("surface_class")
        .context("missing surface_class")?;
    if surface_class != DECLARED_SURFACE_CLASS {
        bail!("unknown children_current surface_class {surface_class}");
    }

    Ok(ChildrenCurrentRow {
        parent_logical_name_id: row
            .try_get("parent_logical_name_id")
            .context("missing parent_logical_name_id")?,
        child_logical_name_id: row
            .try_get("child_logical_name_id")
            .context("missing child_logical_name_id")?,
        surface_class,
        namespace: row.try_get("namespace").context("missing namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

fn decode_children_current_summary(row: PgRow) -> Result<ChildrenCurrentSummary> {
    Ok(ChildrenCurrentSummary {
        parent_logical_name_id: row
            .try_get("parent_logical_name_id")
            .context("missing parent_logical_name_id")?,
        child_count: row.try_get("child_count").context("missing child_count")?,
        provenance_inputs: json_array_field(&row, "provenance_inputs")?,
        chain_positions: json_array_field(&row, "chain_positions")?,
        canonicality_summaries: json_array_field(&row, "canonicality_summaries")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

fn json_array_field(row: &PgRow, field_name: &str) -> Result<Vec<Value>> {
    let value: Value = row
        .try_get(field_name)
        .with_context(|| format!("children_current summary row missing {field_name}"))?;
    take_json_array(value, || {
        format!("children_current summary field {field_name} must be a JSON array")
    })
}

fn decode_declared_child_event_source(row: PgRow) -> Result<DeclaredChildEventSource> {
    Ok(DeclaredChildEventSource {
        parent_logical_name_id: row
            .try_get("parent_logical_name_id")
            .context("missing parent_logical_name_id")?,
        child_logical_name_id: row
            .try_get("child_logical_name_id")
            .context("missing child_logical_name_id")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        normalized_event_id: row
            .try_get("normalized_event_id")
            .context("missing normalized_event_id")?,
        event_identity: row
            .try_get("event_identity")
            .context("missing event_identity")?,
        source_family: row
            .try_get("source_family")
            .context("missing source_family")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        source_manifest_id: row
            .try_get("source_manifest_id")
            .context("missing source_manifest_id")?,
        chain_id: row
            .try_get::<Option<String>, _>("chain_id")
            .context("missing chain_id")?
            .context("declared child source is missing chain_id")?,
        block_number: row
            .try_get::<Option<i64>, _>("block_number")
            .context("missing block_number")?
            .context("declared child source is missing block_number")?,
        block_hash: row
            .try_get::<Option<String>, _>("block_hash")
            .context("missing block_hash")?
            .context("declared child source is missing block_hash")?,
        transaction_hash: row
            .try_get::<Option<String>, _>("transaction_hash")
            .context("missing transaction_hash")?
            .context("declared child source is missing transaction_hash")?,
        log_index: row
            .try_get::<Option<i64>, _>("log_index")
            .context("missing log_index")?
            .context("declared child source is missing log_index")?,
        raw_fact_ref: row
            .try_get("raw_fact_ref")
            .context("missing raw_fact_ref")?,
        normalized_event_ids: row
            .try_get("normalized_event_ids")
            .context("missing normalized_event_ids")?,
        raw_fact_refs: row
            .try_get("raw_fact_refs")
            .context("missing raw_fact_refs")?,
        manifest_versions: row
            .try_get("manifest_versions")
            .context("missing manifest_versions")?,
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use anyhow::Result;
    use serde_json::json;
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
        types::Uuid,
    };

    use super::*;
    use crate::{
        CanonicalityState, NameSurface, NormalizedEvent, default_database_url,
        upsert_name_surfaces, upsert_normalized_events,
    };

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for children_current tests")?;
            let database_name = format!(
                "bigname_storage_children_current_test_{}_{}",
                std::process::id(),
                Uuid::new_v4().simple()
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for children_current tests")?;

            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                database_name
            ))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to drop stale test database {database_name}"))?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect children_current test pool")?;

            crate::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for children_current tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    fn timestamp(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
    }

    fn name_surface(
        logical_name_id: &str,
        display_name: &str,
        namehash: &str,
        block_number: i64,
        canonicality_state: CanonicalityState,
    ) -> NameSurface {
        name_surface_on_chain(
            logical_name_id,
            display_name,
            namehash,
            "ethereum-mainnet",
            block_number,
            canonicality_state,
        )
    }

    fn name_surface_on_chain(
        logical_name_id: &str,
        display_name: &str,
        namehash: &str,
        chain_id: &str,
        block_number: i64,
        canonicality_state: CanonicalityState,
    ) -> NameSurface {
        let namespace = logical_name_id
            .split_once(':')
            .map(|(namespace, _)| namespace)
            .expect("logical_name_id must include namespace")
            .to_owned();

        NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace,
            input_name: display_name.to_owned(),
            canonical_display_name: display_name.to_owned(),
            normalized_name: display_name.to_owned(),
            dns_encoded_name: display_name.as_bytes().to_vec(),
            namehash: namehash.to_owned(),
            labelhashes: vec![format!("labelhash:{display_name}")],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: chain_id.to_owned(),
            block_hash: format!("0xsurface{block_number:02x}"),
            block_number,
            provenance: json!({"source": "children_current_test", "kind": "surface"}),
            canonicality_state,
        }
    }

    fn children_current_row(
        parent_logical_name_id: &str,
        child_logical_name_id: &str,
        display_name: &str,
        namehash: &str,
        block_number: i64,
    ) -> ChildrenCurrentRow {
        ChildrenCurrentRow {
            parent_logical_name_id: parent_logical_name_id.to_owned(),
            child_logical_name_id: child_logical_name_id.to_owned(),
            surface_class: DECLARED_SURFACE_CLASS.to_owned(),
            namespace: "ens".to_owned(),
            canonical_display_name: display_name.to_owned(),
            normalized_name: display_name.to_owned(),
            namehash: namehash.to_owned(),
            provenance: json!({
                "normalized_event_ids": [block_number],
                "derivation_kind": "children_current_rebuild"
            }),
            chain_positions: json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": block_number,
                    "block_hash": format!("0xblock{block_number:02x}"),
                    "timestamp": "2026-04-17T00:00:00Z"
                }
            }),
            canonicality_summary: json!({
                "status": "finalized",
                "chains": {
                    "ethereum-mainnet": "finalized"
                }
            }),
            manifest_version: 1,
            last_recomputed_at: timestamp(1_717_172_000 + block_number),
        }
    }

    struct SubregistryEventSeed<'a> {
        event_identity: &'a str,
        namespace: &'a str,
        source_family: &'a str,
        chain_id: &'a str,
        parent_namehash: &'a str,
        child_namehash: &'a str,
        block_number: i64,
        log_index: i64,
        canonicality_state: CanonicalityState,
        tombstone: bool,
        active_edge: bool,
    }

    fn subregistry_event(seed: SubregistryEventSeed<'_>) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: seed.event_identity.to_owned(),
            namespace: seed.namespace.to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: SUBREGISTRY_EVENT_KIND.to_owned(),
            source_family: seed.source_family.to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some(seed.chain_id.to_owned()),
            block_number: Some(seed.block_number),
            block_hash: Some(format!("0xeventblock{:02x}", seed.block_number)),
            transaction_hash: Some(format!("0xtx{:02x}", seed.block_number)),
            log_index: Some(seed.log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": seed.chain_id,
                "block_number": seed.block_number,
                "log_index": seed.log_index
            }),
            derivation_kind: SUBREGISTRY_DERIVATION_KIND.to_owned(),
            canonicality_state: seed.canonicality_state,
            before_state: json!({}),
            after_state: json!({
                "source_event": "NewOwner",
                "edge_kind": "subregistry",
                "parent_node": seed.parent_namehash,
                "child_node": seed.child_namehash,
                "labelhash": format!("labelhash:{}", seed.child_namehash),
                "owner": "0x0000000000000000000000000000000000000001",
                "tombstone": seed.tombstone,
                "active_edge": seed.active_edge
            }),
        }
    }

    fn ensv2_subregistry_event(
        event_identity: &str,
        parent_logical_name_id: &str,
        from_contract_instance_id: &str,
        to_contract_instance_id: Option<&str>,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some(parent_logical_name_id.to_owned()),
            resource_id: None,
            event_kind: SUBREGISTRY_EVENT_KIND.to_owned(),
            source_family: ENSV2_ROOT_SOURCE_FAMILY.to_owned(),
            manifest_version: 2,
            source_manifest_id: None,
            chain_id: Some("ethereum-sepolia".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xensv2eventblock{block_number:02x}")),
            transaction_hash: Some(format!("0xensv2tx{block_number:02x}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-sepolia",
                "block_number": block_number,
                "log_index": log_index,
                "emitting_address": "0x00000000000000000000000000000000000000aa"
            }),
            derivation_kind: ENSV2_REGISTRY_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "source_event": "SubregistryUpdated",
                "token_id": format!("0xtoken{block_number:02x}"),
                "subregistry": to_contract_instance_id.map(|_| "0x00000000000000000000000000000000000000bb"),
                "from_contract_instance_id": from_contract_instance_id,
                "to_contract_instance_id": to_contract_instance_id,
            }),
        }
    }

    fn ensv2_parent_event(
        event_identity: &str,
        parent_name: &str,
        parent_contract_instance_id: &str,
        registry_contract_instance_id: &str,
        emitting_address: &str,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: PARENT_EVENT_KIND.to_owned(),
            source_family: ENSV2_REGISTRY_SOURCE_FAMILY.to_owned(),
            manifest_version: 3,
            source_manifest_id: None,
            chain_id: Some("ethereum-sepolia".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xensv2eventblock{block_number:02x}")),
            transaction_hash: Some(format!("0xensv2tx{block_number:02x}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-sepolia",
                "block_number": block_number,
                "log_index": log_index,
                "emitting_address": emitting_address
            }),
            derivation_kind: ENSV2_REGISTRY_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "source_event": "ParentUpdated",
                "parent": "0x00000000000000000000000000000000000000aa",
                "label": parent_name.split('.').next().unwrap_or(parent_name),
                "registry_name": parent_name,
                "registry_contract_instance_id": registry_contract_instance_id,
                "parent_contract_instance_id": parent_contract_instance_id,
            }),
        }
    }

    fn ensv2_registration_event(
        event_identity: &str,
        child_logical_name_id: &str,
        event_kind: &str,
        registry_contract_instance_id: &str,
        emitting_address: &str,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some(child_logical_name_id.to_owned()),
            resource_id: None,
            event_kind: event_kind.to_owned(),
            source_family: ENSV2_REGISTRY_SOURCE_FAMILY.to_owned(),
            manifest_version: 3,
            source_manifest_id: None,
            chain_id: Some("ethereum-sepolia".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xensv2eventblock{block_number:02x}")),
            transaction_hash: Some(format!("0xensv2tx{block_number:02x}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-sepolia",
                "block_number": block_number,
                "log_index": log_index,
                "emitting_address": emitting_address
            }),
            derivation_kind: ENSV2_REGISTRY_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "source_event": event_kind,
                "registry_contract_instance_id": registry_contract_instance_id,
                "status": if event_kind == REGISTRATION_RELEASED_EVENT_KIND {
                    "released"
                } else {
                    "registered"
                },
            }),
        }
    }

    #[tokio::test]
    async fn children_current_upserts_and_loads_declared_rows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let parent_logical_name_id = "ens:parent.eth";
        let child_logical_name_id = "ens:alice.parent.eth";

        upsert_name_surfaces(
            database.pool(),
            &[
                name_surface(
                    parent_logical_name_id,
                    "parent.eth",
                    "node:parent.eth",
                    10,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    child_logical_name_id,
                    "alice.parent.eth",
                    "node:alice.parent.eth",
                    11,
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;

        let expected = children_current_row(
            parent_logical_name_id,
            child_logical_name_id,
            "alice.parent.eth",
            "node:alice.parent.eth",
            11,
        );

        let inserted =
            upsert_children_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;
        assert_eq!(inserted, vec![expected.clone()]);
        assert_eq!(
            load_children_current(database.pool(), parent_logical_name_id).await?,
            vec![expected.clone()]
        );

        assert_eq!(
            delete_children_current(database.pool(), parent_logical_name_id).await?,
            1
        );
        assert!(
            load_children_current(database.pool(), parent_logical_name_id)
                .await?
                .is_empty()
        );

        upsert_children_current_rows(database.pool(), &[expected]).await?;
        assert_eq!(clear_children_current(database.pool()).await?, 1);

        database.cleanup().await
    }

    #[tokio::test]
    async fn children_current_load_orders_by_display_name() -> Result<()> {
        let database = TestDatabase::new().await?;
        let parent_logical_name_id = "ens:parent.eth";

        upsert_name_surfaces(
            database.pool(),
            &[
                name_surface(
                    parent_logical_name_id,
                    "parent.eth",
                    "node:parent.eth",
                    20,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    "ens:bob.parent.eth",
                    "bob.parent.eth",
                    "node:bob.parent.eth",
                    21,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    "ens:alice.parent.eth",
                    "alice.parent.eth",
                    "node:alice.parent.eth",
                    22,
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;

        let bob = children_current_row(
            parent_logical_name_id,
            "ens:bob.parent.eth",
            "bob.parent.eth",
            "node:bob.parent.eth",
            21,
        );
        let alice = children_current_row(
            parent_logical_name_id,
            "ens:alice.parent.eth",
            "alice.parent.eth",
            "node:alice.parent.eth",
            22,
        );
        upsert_children_current_rows(database.pool(), &[bob.clone(), alice.clone()]).await?;

        assert_eq!(
            load_children_current(database.pool(), parent_logical_name_id).await?,
            vec![alice, bob]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn children_current_page_uses_keyset_cursor_and_full_filter_summary() -> Result<()> {
        let database = TestDatabase::new().await?;
        let parent_logical_name_id = "ens:parent.eth";

        upsert_name_surfaces(
            database.pool(),
            &[
                name_surface(
                    parent_logical_name_id,
                    "parent.eth",
                    "node:parent.eth",
                    30,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    "ens:alice.parent.eth",
                    "alice.parent.eth",
                    "node:alice.parent.eth",
                    31,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    "ens:bob.parent.eth",
                    "bob.parent.eth",
                    "node:bob.parent.eth",
                    32,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    "ens:carla.parent.eth",
                    "carla.parent.eth",
                    "node:carla.parent.eth",
                    33,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    "ens:zara.parent.eth",
                    "zara.parent.eth",
                    "node:zara.parent.eth",
                    34,
                    CanonicalityState::Observed,
                ),
            ],
        )
        .await?;

        let alice = children_current_row(
            parent_logical_name_id,
            "ens:alice.parent.eth",
            "alice.parent.eth",
            "node:alice.parent.eth",
            31,
        );
        let bob = children_current_row(
            parent_logical_name_id,
            "ens:bob.parent.eth",
            "bob.parent.eth",
            "node:bob.parent.eth",
            32,
        );
        let carla = children_current_row(
            parent_logical_name_id,
            "ens:carla.parent.eth",
            "carla.parent.eth",
            "node:carla.parent.eth",
            33,
        );
        let zara_observed = children_current_row(
            parent_logical_name_id,
            "ens:zara.parent.eth",
            "zara.parent.eth",
            "node:zara.parent.eth",
            34,
        );
        upsert_children_current_rows(
            database.pool(),
            &[carla.clone(), zara_observed, bob.clone(), alice.clone()],
        )
        .await?;

        let first_page =
            load_children_current_page(database.pool(), parent_logical_name_id, None, 2).await?;
        assert_eq!(first_page.rows, vec![alice.clone(), bob.clone()]);
        assert_eq!(
            first_page.next_cursor,
            Some(ChildrenCurrentKeysetCursor::from(&bob))
        );
        assert_eq!(
            first_page.summary.parent_logical_name_id,
            parent_logical_name_id
        );
        assert_eq!(first_page.summary.child_count, 3);
        assert_eq!(
            first_page.summary.provenance_inputs,
            vec![
                alice.provenance.clone(),
                bob.provenance.clone(),
                carla.provenance.clone()
            ]
        );
        assert_eq!(
            first_page.summary.chain_positions,
            vec![
                alice.chain_positions.clone(),
                bob.chain_positions.clone(),
                carla.chain_positions.clone()
            ]
        );
        assert_eq!(
            first_page.summary.canonicality_summaries,
            vec![
                alice.canonicality_summary.clone(),
                bob.canonicality_summary.clone(),
                carla.canonicality_summary.clone()
            ]
        );
        assert_eq!(
            first_page.summary.last_recomputed_at,
            Some(carla.last_recomputed_at)
        );

        let cursor = ChildrenCurrentKeysetCursor {
            canonical_display_name: bob.canonical_display_name.clone(),
            child_logical_name_id: bob.child_logical_name_id.clone(),
        };
        let second_page =
            load_children_current_page(database.pool(), parent_logical_name_id, Some(&cursor), 2)
                .await?;
        assert_eq!(second_page.rows, vec![carla.clone()]);
        assert_eq!(second_page.next_cursor, None);
        assert_eq!(second_page.summary, first_page.summary);
        assert_eq!(
            load_children_current(database.pool(), parent_logical_name_id).await?,
            vec![alice, bob, carla]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn children_current_batch_summaries_preserve_order_and_zero_counts() -> Result<()> {
        let database = TestDatabase::new().await?;
        let parent_a = "ens:alpha.eth";
        let parent_b = "ens:beta.eth";
        let missing_parent = "ens:missing.eth";

        upsert_name_surfaces(
            database.pool(),
            &[
                name_surface(
                    parent_a,
                    "alpha.eth",
                    "node:alpha.eth",
                    40,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    parent_b,
                    "beta.eth",
                    "node:beta.eth",
                    41,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    "ens:one.alpha.eth",
                    "one.alpha.eth",
                    "node:one.alpha.eth",
                    42,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    "ens:two.alpha.eth",
                    "two.alpha.eth",
                    "node:two.alpha.eth",
                    43,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    "ens:draft.beta.eth",
                    "draft.beta.eth",
                    "node:draft.beta.eth",
                    44,
                    CanonicalityState::Observed,
                ),
            ],
        )
        .await?;

        let alpha_one = children_current_row(
            parent_a,
            "ens:one.alpha.eth",
            "one.alpha.eth",
            "node:one.alpha.eth",
            42,
        );
        let alpha_two = children_current_row(
            parent_a,
            "ens:two.alpha.eth",
            "two.alpha.eth",
            "node:two.alpha.eth",
            43,
        );
        let beta_observed = children_current_row(
            parent_b,
            "ens:draft.beta.eth",
            "draft.beta.eth",
            "node:draft.beta.eth",
            44,
        );
        upsert_children_current_rows(
            database.pool(),
            &[alpha_two.clone(), beta_observed, alpha_one.clone()],
        )
        .await?;

        let summaries = load_children_current_summaries(
            database.pool(),
            &[
                parent_b.to_owned(),
                parent_a.to_owned(),
                missing_parent.to_owned(),
            ],
        )
        .await?;

        assert_eq!(summaries.len(), 3);
        assert_eq!(summaries[0].parent_logical_name_id, parent_b);
        assert_eq!(summaries[0].child_count, 0);
        assert!(summaries[0].provenance_inputs.is_empty());
        assert!(summaries[0].chain_positions.is_empty());
        assert!(summaries[0].canonicality_summaries.is_empty());
        assert_eq!(summaries[0].last_recomputed_at, None);

        assert_eq!(summaries[1].parent_logical_name_id, parent_a);
        assert_eq!(summaries[1].child_count, 2);
        assert_eq!(
            summaries[1].provenance_inputs,
            vec![alpha_one.provenance.clone(), alpha_two.provenance.clone()]
        );
        assert_eq!(
            summaries[1].chain_positions,
            vec![
                alpha_one.chain_positions.clone(),
                alpha_two.chain_positions.clone()
            ]
        );
        assert_eq!(
            summaries[1].canonicality_summaries,
            vec![
                alpha_one.canonicality_summary.clone(),
                alpha_two.canonicality_summary.clone()
            ]
        );
        assert_eq!(
            summaries[1].last_recomputed_at,
            Some(alpha_two.last_recomputed_at)
        );

        assert_eq!(summaries[2].parent_logical_name_id, missing_parent);
        assert_eq!(summaries[2].child_count, 0);
        assert!(summaries[2].provenance_inputs.is_empty());
        assert!(summaries[2].chain_positions.is_empty());
        assert!(summaries[2].canonicality_summaries.is_empty());
        assert_eq!(summaries[2].last_recomputed_at, None);

        database.cleanup().await
    }

    #[tokio::test]
    async fn children_current_declared_child_sources_filter_noncanonical_events_and_reassignments()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let parent_a = "ens:parent.eth";
        let parent_b = "ens:other.eth";
        let child_alice = "ens:alice.parent.eth";
        let child_bob = "ens:bob.parent.eth";
        let child_carla = "ens:carla.parent.eth";

        upsert_name_surfaces(
            database.pool(),
            &[
                name_surface(
                    parent_a,
                    "parent.eth",
                    "node:parent.eth",
                    30,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    parent_b,
                    "other.eth",
                    "node:other.eth",
                    31,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    child_alice,
                    "alice.parent.eth",
                    "node:alice.parent.eth",
                    32,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    child_bob,
                    "bob.parent.eth",
                    "node:bob.parent.eth",
                    33,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    child_carla,
                    "carla.parent.eth",
                    "node:carla.parent.eth",
                    34,
                    CanonicalityState::Observed,
                ),
            ],
        )
        .await?;

        upsert_normalized_events(
            database.pool(),
            &[
                subregistry_event(SubregistryEventSeed {
                    event_identity: "alice-parent-a",
                    namespace: "ens",
                    source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                    chain_id: "ethereum-mainnet",
                    parent_namehash: "node:parent.eth",
                    child_namehash: "node:alice.parent.eth",
                    block_number: 100,
                    log_index: 0,
                    canonicality_state: CanonicalityState::Finalized,
                    tombstone: false,
                    active_edge: true,
                }),
                subregistry_event(SubregistryEventSeed {
                    event_identity: "alice-parent-b",
                    namespace: "ens",
                    source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                    chain_id: "ethereum-mainnet",
                    parent_namehash: "node:other.eth",
                    child_namehash: "node:alice.parent.eth",
                    block_number: 101,
                    log_index: 0,
                    canonicality_state: CanonicalityState::Finalized,
                    tombstone: false,
                    active_edge: true,
                }),
                subregistry_event(SubregistryEventSeed {
                    event_identity: "bob-observed",
                    namespace: "ens",
                    source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                    chain_id: "ethereum-mainnet",
                    parent_namehash: "node:other.eth",
                    child_namehash: "node:bob.parent.eth",
                    block_number: 102,
                    log_index: 0,
                    canonicality_state: CanonicalityState::Observed,
                    tombstone: false,
                    active_edge: true,
                }),
                subregistry_event(SubregistryEventSeed {
                    event_identity: "carla-finalized",
                    namespace: "ens",
                    source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                    chain_id: "ethereum-mainnet",
                    parent_namehash: "node:other.eth",
                    child_namehash: "node:carla.parent.eth",
                    block_number: 103,
                    log_index: 0,
                    canonicality_state: CanonicalityState::Finalized,
                    tombstone: false,
                    active_edge: true,
                }),
                subregistry_event(SubregistryEventSeed {
                    event_identity: "alice-orphaned",
                    namespace: "ens",
                    source_family: ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                    chain_id: "ethereum-mainnet",
                    parent_namehash: "node:parent.eth",
                    child_namehash: "node:alice.parent.eth",
                    block_number: 104,
                    log_index: 0,
                    canonicality_state: CanonicalityState::Orphaned,
                    tombstone: false,
                    active_edge: true,
                }),
            ],
        )
        .await?;

        assert!(
            load_canonical_ens_v1_declared_child_sources(database.pool(), Some(parent_a))
                .await?
                .is_empty()
        );

        let current =
            load_canonical_ens_v1_declared_child_sources(database.pool(), Some(parent_b)).await?;
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].parent_logical_name_id, parent_b);
        assert_eq!(current[0].child_logical_name_id, child_alice);
        assert_eq!(current[0].event_identity, "alice-parent-b");

        database.cleanup().await
    }

    #[tokio::test]
    async fn children_current_declared_child_sources_include_basenames_base_registry() -> Result<()>
    {
        let database = TestDatabase::new().await?;
        let parent = "basenames:base.eth";
        let child = "basenames:alice.base.eth";
        let colliding_ens_parent = "ens:base.eth";
        let colliding_ens_child = "ens:alice.base.eth";

        upsert_name_surfaces(
            database.pool(),
            &[
                name_surface_on_chain(
                    colliding_ens_parent,
                    "base.eth",
                    "node:base.eth",
                    "ethereum-mainnet",
                    39,
                    CanonicalityState::Finalized,
                ),
                name_surface_on_chain(
                    parent,
                    "base.eth",
                    "node:base.eth",
                    "base-mainnet",
                    40,
                    CanonicalityState::Finalized,
                ),
                name_surface_on_chain(
                    colliding_ens_child,
                    "alice.base.eth",
                    "node:alice.base.eth",
                    "ethereum-mainnet",
                    40,
                    CanonicalityState::Finalized,
                ),
                name_surface_on_chain(
                    child,
                    "alice.base.eth",
                    "node:alice.base.eth",
                    "base-mainnet",
                    41,
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;

        upsert_normalized_events(
            database.pool(),
            &[
                subregistry_event(SubregistryEventSeed {
                    event_identity: "alice-base-registry",
                    namespace: "basenames",
                    source_family: BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY,
                    chain_id: "base-mainnet",
                    parent_namehash: "node:base.eth",
                    child_namehash: "node:alice.base.eth",
                    block_number: 200,
                    log_index: 0,
                    canonicality_state: CanonicalityState::Finalized,
                    tombstone: false,
                    active_edge: true,
                }),
                subregistry_event(SubregistryEventSeed {
                    event_identity: "alice-base-primary",
                    namespace: "basenames",
                    source_family: "basenames_base_primary",
                    chain_id: "base-mainnet",
                    parent_namehash: "node:base.eth",
                    child_namehash: "node:alice.base.eth",
                    block_number: 201,
                    log_index: 0,
                    canonicality_state: CanonicalityState::Finalized,
                    tombstone: false,
                    active_edge: true,
                }),
            ],
        )
        .await?;

        assert!(
            load_canonical_declared_child_sources(database.pool(), Some(colliding_ens_parent))
                .await?
                .is_empty()
        );

        let current = load_canonical_declared_child_sources(database.pool(), Some(parent)).await?;
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].parent_logical_name_id, parent);
        assert_eq!(current[0].child_logical_name_id, child);
        assert_eq!(
            current[0].source_family,
            BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY
        );
        assert_eq!(current[0].namespace, "basenames");
        assert_eq!(current[0].chain_id, "base-mainnet");
        assert_eq!(current[0].event_identity, "alice-base-registry");

        database.cleanup().await
    }

    #[tokio::test]
    async fn children_current_declared_child_sources_include_ensv2_linked_subregistry_graph_and_reject_registry_mismatch()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let parent = "ens:alice.eth";
        let child = "ens:bob.alice.eth";
        let wrong_registry_child = "ens:eve.alice.eth";
        let released_child = "ens:carol.alice.eth";
        let parent_registry = "00000000-0000-0000-0000-0000000000aa";
        let child_registry = "00000000-0000-0000-0000-0000000000bb";
        let child_registry_address = "0x00000000000000000000000000000000000000bb";

        upsert_name_surfaces(
            database.pool(),
            &[
                name_surface_on_chain(
                    parent,
                    "alice.eth",
                    "node:alice.eth",
                    "ethereum-sepolia",
                    50,
                    CanonicalityState::Finalized,
                ),
                name_surface_on_chain(
                    child,
                    "bob.alice.eth",
                    "node:bob.alice.eth",
                    "ethereum-sepolia",
                    51,
                    CanonicalityState::Finalized,
                ),
                name_surface_on_chain(
                    wrong_registry_child,
                    "eve.alice.eth",
                    "node:eve.alice.eth",
                    "ethereum-sepolia",
                    52,
                    CanonicalityState::Finalized,
                ),
                name_surface_on_chain(
                    released_child,
                    "carol.alice.eth",
                    "node:carol.alice.eth",
                    "ethereum-sepolia",
                    53,
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;

        upsert_normalized_events(
            database.pool(),
            &[
                ensv2_subregistry_event(
                    "ensv2-subregistry-active",
                    parent,
                    parent_registry,
                    Some(child_registry),
                    300,
                    0,
                ),
                ensv2_parent_event(
                    "ensv2-parent-active",
                    "alice.eth",
                    parent_registry,
                    child_registry,
                    child_registry_address,
                    301,
                    0,
                ),
                ensv2_registration_event(
                    "ensv2-bob-registered",
                    child,
                    REGISTRATION_GRANTED_EVENT_KIND,
                    child_registry,
                    child_registry_address,
                    302,
                    0,
                ),
                ensv2_registration_event(
                    "ensv2-eve-wrong-registry",
                    wrong_registry_child,
                    REGISTRATION_GRANTED_EVENT_KIND,
                    "00000000-0000-0000-0000-0000000000cc",
                    child_registry_address,
                    303,
                    0,
                ),
                ensv2_registration_event(
                    "ensv2-carol-registered",
                    released_child,
                    REGISTRATION_GRANTED_EVENT_KIND,
                    child_registry,
                    child_registry_address,
                    304,
                    0,
                ),
                ensv2_registration_event(
                    "ensv2-carol-released",
                    released_child,
                    REGISTRATION_RELEASED_EVENT_KIND,
                    child_registry,
                    child_registry_address,
                    305,
                    0,
                ),
            ],
        )
        .await?;

        let current = load_canonical_declared_child_sources(database.pool(), Some(parent)).await?;
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].parent_logical_name_id, parent);
        assert_eq!(current[0].child_logical_name_id, child);
        assert!(
            current
                .iter()
                .all(|source| source.child_logical_name_id != wrong_registry_child),
            "registration with matching raw emitting_address but mismatched registry_contract_instance_id must be rejected"
        );
        assert_eq!(current[0].event_identity, "ensv2-bob-registered");
        assert_eq!(current[0].source_family, ENSV2_REGISTRY_SOURCE_FAMILY);
        assert_eq!(current[0].manifest_version, 3);
        assert_eq!(
            current[0].manifest_versions,
            json!([
                {
                    "source_manifest_id": null,
                    "source_family": ENSV2_REGISTRY_SOURCE_FAMILY,
                    "manifest_version": 3
                },
                {
                    "source_manifest_id": null,
                    "source_family": ENSV2_ROOT_SOURCE_FAMILY,
                    "manifest_version": 2
                }
            ])
        );
        assert_eq!(current[0].chain_id, "ethereum-sepolia");
        assert_eq!(current[0].normalized_event_ids.len(), 3);
        assert_eq!(current[0].raw_fact_refs.as_array().map(Vec::len), Some(3));

        database.cleanup().await
    }
}
