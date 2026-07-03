use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, QueryBuilder, postgres::PgRow, types::time::OffsetDateTime};

use super::{
    decode::{decode_address_name_current_entry, decode_address_names_current_summary},
    query::{
        push_address_names_current_cursor_after, push_address_names_current_cursor_identity_match,
        push_address_names_current_cursor_sort_value_match,
        push_address_names_current_grouped_entries_cte, push_address_names_current_order,
        push_address_names_current_sortable_entries_cte,
    },
    types::{
        AddressNameCurrentEntry, AddressNameRelation, AddressNamesCurrentCursor,
        AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentPage,
        AddressNamesCurrentSort, AddressNamesCurrentSortedCursor,
        AddressNamesCurrentSortedCursorValue, AddressNamesCurrentSortedPage,
        AddressNamesCurrentSummary,
    },
};
use crate::projection_helpers::{
    checked_page_limit_i64_from_usize, checked_page_size_usize, split_keyset_page,
};

/// Load a bounded page of grouped current address-name entries from the default canonical read set.
pub async fn load_address_names_current_page(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
    dedupe_by: AddressNamesCurrentDedupe,
    cursor: Option<&AddressNamesCurrentCursor>,
    page_size: u64,
) -> Result<AddressNamesCurrentPage> {
    let sorted_cursor = cursor.map(address_names_current_sorted_cursor_from_legacy);
    let relations = relation.into_iter().collect::<Vec<_>>();
    let relations = (!relations.is_empty()).then_some(relations.as_slice());
    let page = load_address_names_current_page_sorted_for_relations(
        pool,
        address,
        namespace,
        relations,
        dedupe_by,
        None,
        AddressNamesCurrentSort::Name,
        AddressNamesCurrentOrder::Asc,
        sorted_cursor.as_ref(),
        page_size,
    )
    .await?;

    let next_cursor = page
        .next_cursor
        .map(address_names_current_legacy_cursor_from_sorted)
        .transpose()?;

    Ok(AddressNamesCurrentPage {
        entries: page.entries,
        next_cursor,
        summary: page.summary,
    })
}

/// Load a bounded page of grouped current address-name entries with v2 set-valued relation controls.
#[allow(clippy::too_many_arguments)]
pub async fn load_address_names_current_page_sorted_for_relations(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<&str>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressNamesCurrentSortedPage> {
    let page_size = checked_page_size_usize(
        page_size,
        "address_names_current page_size must be positive",
        "address_names_current page_size does not fit in usize",
    )?;
    let page_limit = checked_page_limit_i64_from_usize(
        page_size,
        "address_names_current page_size is too large",
        "address_names_current page_size exceeds SQL limit",
    )?;

    let summary =
        load_address_names_current_summary(pool, address, namespace, relations, dedupe_by, q)
            .await?;

    if let Some(cursor) = cursor {
        ensure_address_names_current_cursor_matches_sort(sort, cursor)?;
        ensure_address_names_current_cursor_exists(
            pool, address, namespace, relations, dedupe_by, q, sort, cursor,
        )
        .await?;
    }

    let mut builder = QueryBuilder::<Postgres>::new("");
    push_address_names_current_grouped_entries_cte(
        &mut builder,
        address,
        namespace,
        relations,
        dedupe_by,
        q,
    );
    push_address_names_current_sortable_entries_cte(&mut builder, sort);
    builder.push(
        r#"
        SELECT
            address,
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            relations,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at,
        "#,
    );
    if sort.is_timestamp() {
        builder.push("sort_timestamp");
    } else {
        builder.push("NULL::TIMESTAMPTZ AS sort_timestamp");
    }
    builder.push(" FROM ");
    builder.push(if sort.is_timestamp() {
        "sortable_entries"
    } else {
        "entries"
    });
    builder.push(" WHERE TRUE ");
    if let Some(cursor) = cursor {
        push_address_names_current_cursor_after(&mut builder, sort, order, cursor);
    }
    push_address_names_current_order(&mut builder, sort, order);
    builder.push(" LIMIT ");
    builder.push_bind(page_limit);

    let rows = builder.build().fetch_all(pool).await.with_context(|| {
        let mut parts = load_context_parts(address, namespace, relations, dedupe_by, q);
        parts.push(format!("sort {}", sort.as_str()));
        parts.push(format!("order {}", order.as_str()));
        format!(
            "failed to load address_names_current grouped page for {}",
            parts.join(" ")
        )
    })?;

    let rows = rows
        .into_iter()
        .map(|row| decode_address_name_current_sorted_entry(row, sort))
        .collect::<Result<Vec<_>>>()?;
    let (rows, next_cursor) = split_keyset_page(rows, page_size, |row| {
        address_names_current_sorted_cursor_from_entry(row, sort)
    });
    let entries = rows.into_iter().map(|row| row.entry).collect();

    Ok(AddressNamesCurrentSortedPage {
        entries,
        next_cursor,
        summary,
    })
}

fn load_context_parts(
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<&str>,
) -> Vec<String> {
    let mut parts = vec![format!("address {address}")];
    if let Some(namespace) = namespace {
        parts.push(format!("namespace {namespace}"));
    }
    if let Some(relations) = relations.filter(|relations| !relations.is_empty()) {
        parts.push(format!(
            "relations {}",
            relations
                .iter()
                .map(|relation| relation.as_str())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(q) = q {
        parts.push(format!("q {q}"));
    }
    parts.push(format!("dedupe_by {}", dedupe_by.as_str()));
    parts
}

async fn load_address_names_current_summary(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<&str>,
) -> Result<AddressNamesCurrentSummary> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_address_names_current_grouped_entries_cte(
        &mut builder,
        address,
        namespace,
        relations,
        dedupe_by,
        q,
    );
    builder.push(
        r#",
        ordered_entries AS (
            SELECT
                entries.*,
                ROW_NUMBER() OVER (
                    ORDER BY
                        canonical_display_name ASC,
                        logical_name_id ASC,
                        resource_id::TEXT ASC
                ) AS entry_position
            FROM entries
        ),
        normalized_event_id_values AS (
            SELECT DISTINCT ON (value)
                value,
                entry_position,
                value_position
            FROM ordered_entries
            CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
                CASE
                    WHEN JSONB_TYPEOF(provenance -> 'normalized_event_ids') = 'array'
                    THEN provenance -> 'normalized_event_ids'
                    ELSE '[]'::JSONB
                END
            ) WITH ORDINALITY AS provenance_values(value, value_position)
            ORDER BY value, entry_position ASC, value_position ASC
        ),
        raw_fact_ref_values AS (
            SELECT DISTINCT ON (value)
                value,
                entry_position,
                value_position
            FROM ordered_entries
            CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
                CASE
                    WHEN JSONB_TYPEOF(provenance -> 'raw_fact_refs') = 'array'
                    THEN provenance -> 'raw_fact_refs'
                    ELSE '[]'::JSONB
                END
            ) WITH ORDINALITY AS provenance_values(value, value_position)
            ORDER BY value, entry_position ASC, value_position ASC
        ),
        manifest_version_values AS (
            SELECT DISTINCT ON (value)
                value,
                entry_position,
                value_position
            FROM ordered_entries
            CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
                CASE
                    WHEN JSONB_TYPEOF(provenance -> 'manifest_versions') = 'array'
                    THEN provenance -> 'manifest_versions'
                    ELSE '[]'::JSONB
                END
            ) WITH ORDINALITY AS provenance_values(value, value_position)
            ORDER BY value, entry_position ASC, value_position ASC
        ),
        chain_position_values AS (
            SELECT
                slot,
                position_value,
                (position_value ->> 'block_number')::BIGINT AS block_number,
                position_value ->> 'block_hash' AS block_hash
            FROM ordered_entries
            CROSS JOIN LATERAL JSONB_EACH(chain_positions) AS positions(slot, position_value)
            WHERE position_value ? 'chain_id'
              AND position_value ? 'block_number'
              AND position_value ? 'block_hash'
              AND position_value ? 'timestamp'
              AND JSONB_TYPEOF(position_value -> 'block_number') = 'number'
        ),
        chain_position_heads AS (
            SELECT DISTINCT ON (slot)
                slot,
                position_value
            FROM chain_position_values
            ORDER BY slot, block_number DESC, block_hash DESC
        )
        SELECT
            (SELECT COUNT(*)::BIGINT FROM ordered_entries) AS grouped_entry_count,
            COALESCE(
                (
                    SELECT JSONB_AGG(value ORDER BY entry_position ASC, value_position ASC)
                    FROM normalized_event_id_values
                ),
                '[]'::JSONB
            ) AS provenance_normalized_event_ids,
            COALESCE(
                (
                    SELECT JSONB_AGG(value ORDER BY entry_position ASC, value_position ASC)
                    FROM raw_fact_ref_values
                ),
                '[]'::JSONB
            ) AS provenance_raw_fact_refs,
            COALESCE(
                (
                    SELECT JSONB_AGG(value ORDER BY entry_position ASC, value_position ASC)
                    FROM manifest_version_values
                ),
                '[]'::JSONB
            ) AS provenance_manifest_versions,
            (
                SELECT provenance ->> 'derivation_kind'
                FROM ordered_entries
                WHERE JSONB_TYPEOF(provenance -> 'derivation_kind') = 'string'
                ORDER BY entry_position ASC
                LIMIT 1
            ) AS provenance_derivation_kind,
            COALESCE(
                (
                    SELECT JSONB_OBJECT_AGG(slot, position_value ORDER BY slot)
                    FROM chain_position_heads
                ),
                '{}'::JSONB
            ) AS chain_positions,
            CASE
                WHEN (SELECT COUNT(*) FROM ordered_entries) = 0 THEN 'head'
                WHEN EXISTS (
                    SELECT 1
                    FROM ordered_entries
                    WHERE COALESCE(canonicality_summary ->> 'status', '') NOT IN ('safe', 'finalized')
                ) THEN 'head'
                WHEN EXISTS (
                    SELECT 1
                    FROM ordered_entries
                    WHERE canonicality_summary ->> 'status' = 'safe'
                ) THEN 'safe'
                ELSE 'finalized'
            END AS consistency,
            (SELECT MAX(last_recomputed_at) FROM ordered_entries) AS last_recomputed_at
        "#,
    );

    let row = builder.build().fetch_one(pool).await.with_context(|| {
        let parts = load_context_parts(address, namespace, relations, dedupe_by, q);
        format!(
            "failed to load address_names_current grouped summary for {}",
            parts.join(" ")
        )
    })?;

    decode_address_names_current_summary(row)
}

#[allow(clippy::too_many_arguments)]
async fn ensure_address_names_current_cursor_exists(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<&str>,
    sort: AddressNamesCurrentSort,
    cursor: &AddressNamesCurrentSortedCursor,
) -> Result<()> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_address_names_current_grouped_entries_cte(
        &mut builder,
        address,
        namespace,
        relations,
        dedupe_by,
        q,
    );
    push_address_names_current_sortable_entries_cte(&mut builder, sort);
    builder.push(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM
        "#,
    );
    builder.push(if sort.is_timestamp() {
        "sortable_entries"
    } else {
        "entries"
    });
    builder.push(" WHERE ");
    push_address_names_current_cursor_identity_match(&mut builder, cursor);
    push_address_names_current_cursor_sort_value_match(&mut builder, sort, cursor);
    builder.push(
        r#"
        ) AS cursor_exists
        "#,
    );

    let row = builder.build().fetch_one(pool).await.with_context(|| {
        let mut parts = load_context_parts(address, namespace, relations, dedupe_by, q);
        parts.push(format!("sort {}", sort.as_str()));
        format!(
            "failed to validate address_names_current grouped page cursor for {}",
            parts.join(" ")
        )
    })?;

    if crate::sql_row::get::<bool>(&row, "cursor_exists")? {
        Ok(())
    } else {
        bail!("address_names_current page cursor does not match a grouped entry")
    }
}

#[cfg(test)]
pub(super) fn address_names_current_cursor_from_entry(
    entry: &AddressNameCurrentEntry,
) -> AddressNamesCurrentCursor {
    AddressNamesCurrentCursor {
        canonical_display_name: entry.canonical_display_name.clone(),
        logical_name_id: entry.logical_name_id.clone(),
        resource_id: entry.resource_id,
    }
}

struct AddressNameCurrentSortedEntry {
    entry: AddressNameCurrentEntry,
    sort_timestamp: Option<OffsetDateTime>,
}

fn decode_address_name_current_sorted_entry(
    row: PgRow,
    sort: AddressNamesCurrentSort,
) -> Result<AddressNameCurrentSortedEntry> {
    let sort_timestamp = sort
        .is_timestamp()
        .then(|| crate::sql_row::get::<Option<OffsetDateTime>>(&row, "sort_timestamp"))
        .transpose()?
        .flatten();
    let entry = decode_address_name_current_entry(row)?;

    Ok(AddressNameCurrentSortedEntry {
        entry,
        sort_timestamp,
    })
}

fn address_names_current_sorted_cursor_from_entry(
    row: &AddressNameCurrentSortedEntry,
    sort: AddressNamesCurrentSort,
) -> AddressNamesCurrentSortedCursor {
    AddressNamesCurrentSortedCursor {
        sort_value: match sort {
            AddressNamesCurrentSort::Name => {
                AddressNamesCurrentSortedCursorValue::Name(row.entry.canonical_display_name.clone())
            }
            AddressNamesCurrentSort::ExpiresAt | AddressNamesCurrentSort::RegisteredAt => {
                AddressNamesCurrentSortedCursorValue::Timestamp(row.sort_timestamp)
            }
        },
        logical_name_id: row.entry.logical_name_id.clone(),
        resource_id: row.entry.resource_id,
    }
}

fn address_names_current_sorted_cursor_from_legacy(
    cursor: &AddressNamesCurrentCursor,
) -> AddressNamesCurrentSortedCursor {
    AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Name(
            cursor.canonical_display_name.clone(),
        ),
        logical_name_id: cursor.logical_name_id.clone(),
        resource_id: cursor.resource_id,
    }
}

fn address_names_current_legacy_cursor_from_sorted(
    cursor: AddressNamesCurrentSortedCursor,
) -> Result<AddressNamesCurrentCursor> {
    let AddressNamesCurrentSortedCursorValue::Name(canonical_display_name) = cursor.sort_value
    else {
        bail!("address_names_current sorted cursor cannot be converted to legacy name cursor");
    };

    Ok(AddressNamesCurrentCursor {
        canonical_display_name,
        logical_name_id: cursor.logical_name_id,
        resource_id: cursor.resource_id,
    })
}

fn ensure_address_names_current_cursor_matches_sort(
    sort: AddressNamesCurrentSort,
    cursor: &AddressNamesCurrentSortedCursor,
) -> Result<()> {
    match (sort, &cursor.sort_value) {
        (AddressNamesCurrentSort::Name, AddressNamesCurrentSortedCursorValue::Name(_))
        | (
            AddressNamesCurrentSort::ExpiresAt | AddressNamesCurrentSort::RegisteredAt,
            AddressNamesCurrentSortedCursorValue::Timestamp(_),
        ) => Ok(()),
        _ => bail!(
            "address_names_current page cursor sort value does not match sort {}",
            sort.as_str()
        ),
    }
}
