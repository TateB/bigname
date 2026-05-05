use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};

use crate::projection_helpers::{
    checked_page_limit_i64, checked_page_size_usize, split_keyset_page, take_json_array,
};

use super::{
    DECLARED_SURFACE_CLASS, DEFAULT_CHILDREN_CURRENT_READ_FILTER,
    types::{
        ChildrenCurrentKeysetCursor, ChildrenCurrentPage, ChildrenCurrentRow,
        ChildrenCurrentSummary,
    },
};

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

    let rows = builder
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

    let (rows, next_cursor) = split_keyset_page(rows, page_size, |row| {
        ChildrenCurrentKeysetCursor::from(row)
    });

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
    summaries.into_iter().next().with_context(|| {
        format!(
            "failed to load children_current summary for parent_logical_name_id {parent_logical_name_id}"
        )
    })
}

pub(super) fn decode_children_current_row(row: PgRow) -> Result<ChildrenCurrentRow> {
    let surface_class = crate::sql_row::get::<String>(&row, "surface_class")?;
    if surface_class != DECLARED_SURFACE_CLASS {
        bail!("unknown children_current surface_class {surface_class}");
    }

    Ok(ChildrenCurrentRow {
        parent_logical_name_id: crate::sql_row::get(&row, "parent_logical_name_id")?,
        child_logical_name_id: crate::sql_row::get(&row, "child_logical_name_id")?,
        surface_class,
        namespace: crate::sql_row::get(&row, "namespace")?,
        canonical_display_name: crate::sql_row::get(&row, "canonical_display_name")?,
        normalized_name: crate::sql_row::get(&row, "normalized_name")?,
        namehash: crate::sql_row::get(&row, "namehash")?,
        provenance: crate::sql_row::get(&row, "provenance")?,
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        canonicality_summary: crate::sql_row::get(&row, "canonicality_summary")?,
        manifest_version: crate::sql_row::get(&row, "manifest_version")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
    })
}

fn decode_children_current_summary(row: PgRow) -> Result<ChildrenCurrentSummary> {
    Ok(ChildrenCurrentSummary {
        parent_logical_name_id: crate::sql_row::get(&row, "parent_logical_name_id")?,
        child_count: crate::sql_row::get(&row, "child_count")?,
        provenance_inputs: json_array_field(&row, "provenance_inputs")?,
        chain_positions: json_array_field(&row, "chain_positions")?,
        canonicality_summaries: json_array_field(&row, "canonicality_summaries")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
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
