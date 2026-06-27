use sqlx::types::time::OffsetDateTime;
use sqlx::{Postgres, QueryBuilder};

use super::types::{
    AddressNameRelation, AddressNamesCurrentDedupe, AddressNamesCurrentOrder,
    AddressNamesCurrentSort, AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue,
};

pub(super) const DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER: &str = r#"
  AND surface.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND resource.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND binding.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND binding.active_to IS NULL
  AND (
      anc.token_lineage_id IS NULL
      OR token_lineage.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
  )
"#;

pub(super) fn push_address_names_current_grouped_entries_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
    namespace: Option<&'a str>,
    relation: Option<AddressNameRelation>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<&'a str>,
) {
    builder.push(
        r#"
        WITH filtered AS (
            SELECT
                anc.address,
                anc.logical_name_id,
                anc.relation,
                anc.namespace,
                anc.canonical_display_name,
                anc.normalized_name,
                anc.namehash,
                anc.surface_binding_id,
                anc.resource_id,
                anc.token_lineage_id,
                anc.binding_kind,
                anc.provenance,
                anc.coverage,
                anc.chain_positions,
                anc.canonicality_summary,
                anc.manifest_version,
                anc.last_recomputed_at,
                CASE anc.relation
                    WHEN 'registrant' THEN 0
                    WHEN 'token_holder' THEN 1
                    WHEN 'effective_controller' THEN 2
                    ELSE 99
                END AS relation_rank
            FROM address_names_current anc
            JOIN name_surfaces surface
              ON surface.logical_name_id = anc.logical_name_id
            JOIN resources resource
              ON resource.resource_id = anc.resource_id
            JOIN surface_bindings binding
              ON binding.surface_binding_id = anc.surface_binding_id
            LEFT JOIN token_lineages token_lineage
              ON token_lineage.token_lineage_id = anc.token_lineage_id
            WHERE anc.address =
        "#,
    );
    builder.push(" ");
    builder.push_bind(address);

    if let Some(namespace) = namespace {
        builder.push(" AND anc.namespace = ");
        builder.push_bind(namespace);
    }
    if let Some(relation) = relation {
        builder.push(" AND anc.relation = ");
        builder.push_bind(relation.as_str());
    }
    if let Some(prefix) = q {
        builder.push(" AND anc.normalized_name LIKE ");
        builder.push_bind(format!("{}%", escape_like_pattern(prefix)));
        builder.push(" ESCAPE '\\'");
    }
    builder.push(DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER);

    match dedupe_by {
        AddressNamesCurrentDedupe::Surface => builder.push(
            r#"
        ),
        representatives AS (
            SELECT DISTINCT ON (address, logical_name_id)
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
                provenance,
                coverage,
                chain_positions,
                canonicality_summary,
                manifest_version,
                last_recomputed_at
            FROM filtered
            ORDER BY
                address ASC,
                logical_name_id ASC,
                canonical_display_name ASC,
                relation_rank ASC
        ),
        relation_values AS (
            SELECT
                address,
                logical_name_id,
                relation,
                MIN(relation_rank) AS relation_rank
            FROM filtered
            GROUP BY address, logical_name_id, relation
        ),
        relation_facets AS (
            SELECT
                address,
                logical_name_id,
                ARRAY_AGG(relation ORDER BY relation_rank ASC) AS relations
            FROM relation_values
            GROUP BY address, logical_name_id
        ),
        entries AS (
            SELECT
                representatives.address,
                representatives.logical_name_id,
                representatives.namespace,
                representatives.canonical_display_name,
                representatives.normalized_name,
                representatives.namehash,
                representatives.surface_binding_id,
                representatives.resource_id,
                representatives.token_lineage_id,
                representatives.binding_kind,
                relation_facets.relations,
                representatives.provenance,
                representatives.coverage,
                representatives.chain_positions,
                representatives.canonicality_summary,
                representatives.manifest_version,
                representatives.last_recomputed_at
            FROM representatives
            JOIN relation_facets
              ON relation_facets.address = representatives.address
             AND relation_facets.logical_name_id = representatives.logical_name_id
        )
            "#,
        ),
        AddressNamesCurrentDedupe::Resource => builder.push(
            r#"
        ),
        representatives AS (
            SELECT DISTINCT ON (address, resource_id)
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
                provenance,
                coverage,
                chain_positions,
                canonicality_summary,
                manifest_version,
                last_recomputed_at
            FROM filtered
            ORDER BY
                address ASC,
                resource_id ASC,
                canonical_display_name ASC,
                logical_name_id ASC,
                relation_rank ASC
        ),
        relation_values AS (
            SELECT
                address,
                resource_id,
                relation,
                MIN(relation_rank) AS relation_rank
            FROM filtered
            GROUP BY address, resource_id, relation
        ),
        relation_facets AS (
            SELECT
                address,
                resource_id,
                ARRAY_AGG(relation ORDER BY relation_rank ASC) AS relations
            FROM relation_values
            GROUP BY address, resource_id
        ),
        entries AS (
            SELECT
                representatives.address,
                representatives.logical_name_id,
                representatives.namespace,
                representatives.canonical_display_name,
                representatives.normalized_name,
                representatives.namehash,
                representatives.surface_binding_id,
                representatives.resource_id,
                representatives.token_lineage_id,
                representatives.binding_kind,
                relation_facets.relations,
                representatives.provenance,
                representatives.coverage,
                representatives.chain_positions,
                representatives.canonicality_summary,
                representatives.manifest_version,
                representatives.last_recomputed_at
            FROM representatives
            JOIN relation_facets
              ON relation_facets.address = representatives.address
             AND relation_facets.resource_id = representatives.resource_id
        )
            "#,
        ),
    };
}

pub(super) fn push_address_names_current_sortable_entries_cte(
    builder: &mut QueryBuilder<'_, Postgres>,
    sort: AddressNamesCurrentSort,
) {
    if !sort.is_timestamp() {
        return;
    }

    builder.push(
        r#",
        sortable_entries AS (
            SELECT
                entries.*,
        "#,
    );
    push_address_names_current_sort_timestamp_expr(builder, sort);
    builder.push(
        r#"
                AS sort_timestamp
            FROM entries
            LEFT JOIN name_current nc
              ON nc.logical_name_id = entries.logical_name_id
        )
        "#,
    );
}

pub(super) fn push_address_names_current_cursor_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: &'a AddressNamesCurrentSortedCursor,
) {
    match sort {
        AddressNamesCurrentSort::Name => {
            let AddressNamesCurrentSortedCursorValue::Name(sort_value) = &cursor.sort_value else {
                return;
            };
            builder.push(" AND (canonical_display_name ");
            builder.push(match order {
                AddressNamesCurrentOrder::Asc => "> ",
                AddressNamesCurrentOrder::Desc => "< ",
            });
            builder.push_bind(sort_value);
            push_address_names_current_name_tie_after(builder, sort_value, cursor);
            builder.push(")");
        }
        AddressNamesCurrentSort::ExpiresAt | AddressNamesCurrentSort::RegisteredAt => {
            let sort_value = match &cursor.sort_value {
                AddressNamesCurrentSortedCursorValue::Timestamp(sort_value) => *sort_value,
                AddressNamesCurrentSortedCursorValue::Name(_) => return,
            };
            let cursor_rank = timestamp_null_rank(sort_value, order);
            builder.push(" AND (");
            builder.push(timestamp_rank_expr("sort_timestamp", order));
            builder.push(" > ");
            builder.push_bind(cursor_rank);
            builder.push(" OR (");
            builder.push(timestamp_rank_expr("sort_timestamp", order));
            builder.push(" = ");
            builder.push_bind(cursor_rank);
            builder.push(" AND ");
            match sort_value {
                None => {
                    push_address_names_current_timestamp_tie_after(builder, None, cursor);
                }
                Some(value) => match order {
                    AddressNamesCurrentOrder::Asc => {
                        builder.push("(sort_timestamp > ");
                        builder.push_bind(value);
                        builder.push(" OR (sort_timestamp = ");
                        builder.push_bind(value);
                        builder.push(" AND ");
                        push_address_names_current_timestamp_tie_after(
                            builder,
                            Some(value),
                            cursor,
                        );
                        builder.push("))");
                    }
                    AddressNamesCurrentOrder::Desc => {
                        builder.push("(sort_timestamp < ");
                        builder.push_bind(value);
                        builder.push(" OR (sort_timestamp = ");
                        builder.push_bind(value);
                        builder.push(" AND ");
                        push_address_names_current_timestamp_tie_after(
                            builder,
                            Some(value),
                            cursor,
                        );
                        builder.push("))");
                    }
                },
            }
            builder.push("))");
        }
    }
}

pub(super) fn push_address_names_current_order(
    builder: &mut QueryBuilder<'_, Postgres>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
) {
    match sort {
        AddressNamesCurrentSort::Name => {
            builder.push(" ORDER BY canonical_display_name ");
            builder.push(match order {
                AddressNamesCurrentOrder::Asc => "ASC",
                AddressNamesCurrentOrder::Desc => "DESC",
            });
            builder.push(", logical_name_id ASC, resource_id::TEXT ASC");
        }
        AddressNamesCurrentSort::ExpiresAt | AddressNamesCurrentSort::RegisteredAt => {
            builder.push(" ORDER BY ");
            builder.push(timestamp_rank_expr("sort_timestamp", order));
            builder.push(" ASC, sort_timestamp ");
            builder.push(match order {
                AddressNamesCurrentOrder::Asc => "ASC",
                AddressNamesCurrentOrder::Desc => "DESC",
            });
            builder.push(", logical_name_id ASC, resource_id::TEXT ASC");
        }
    }
}

pub(super) fn push_address_names_current_cursor_identity_match<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    cursor: &'a AddressNamesCurrentSortedCursor,
) {
    builder.push("logical_name_id = ");
    builder.push_bind(&cursor.logical_name_id);
    builder.push(" AND resource_id::TEXT = ");
    builder.push_bind(cursor.resource_id.to_string());
}

pub(super) fn push_address_names_current_cursor_sort_value_match<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    sort: AddressNamesCurrentSort,
    cursor: &'a AddressNamesCurrentSortedCursor,
) {
    match sort {
        AddressNamesCurrentSort::Name => {
            let AddressNamesCurrentSortedCursorValue::Name(sort_value) = &cursor.sort_value else {
                return;
            };
            builder.push(" AND canonical_display_name = ");
            builder.push_bind(sort_value);
        }
        AddressNamesCurrentSort::ExpiresAt | AddressNamesCurrentSort::RegisteredAt => {
            let AddressNamesCurrentSortedCursorValue::Timestamp(sort_value) = &cursor.sort_value
            else {
                return;
            };
            match *sort_value {
                None => {
                    builder.push(" AND sort_timestamp IS NULL");
                }
                Some(value) => {
                    builder.push(" AND sort_timestamp = ");
                    builder.push_bind(value);
                }
            };
        }
    }
}

fn push_address_names_current_name_tie_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    sort_value: &'a str,
    cursor: &'a AddressNamesCurrentSortedCursor,
) {
    builder.push(" OR (canonical_display_name = ");
    builder.push_bind(sort_value);
    builder.push(" AND ");
    push_address_names_current_tie_after(builder, cursor);
    builder.push(")");
}

fn push_address_names_current_timestamp_tie_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    value: Option<OffsetDateTime>,
    cursor: &'a AddressNamesCurrentSortedCursor,
) {
    match value {
        None => {
            builder.push("sort_timestamp IS NULL AND ");
        }
        Some(value) => {
            builder.push("sort_timestamp = ");
            builder.push_bind(value);
            builder.push(" AND ");
        }
    }
    push_address_names_current_tie_after(builder, cursor);
}

fn push_address_names_current_tie_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    cursor: &'a AddressNamesCurrentSortedCursor,
) {
    builder.push("(logical_name_id, resource_id::TEXT) > (");
    builder.push_bind(&cursor.logical_name_id);
    builder.push(", ");
    builder.push_bind(cursor.resource_id.to_string());
    builder.push(")");
}

fn push_address_names_current_sort_timestamp_expr(
    builder: &mut QueryBuilder<'_, Postgres>,
    sort: AddressNamesCurrentSort,
) {
    match sort {
        AddressNamesCurrentSort::Name => {
            builder.push("NULL::TIMESTAMPTZ");
        }
        AddressNamesCurrentSort::ExpiresAt => {
            push_json_timestamp_expr(builder, &["registration", "expiry"]);
        }
        AddressNamesCurrentSort::RegisteredAt => {
            push_json_timestamp_expr(builder, &["registration", "registered_at"]);
        }
    };
}

fn push_json_timestamp_expr(builder: &mut QueryBuilder<'_, Postgres>, path: &[&str]) {
    let path_literal = format!("'{{{}}}'", path.join(","));
    builder.push("CASE WHEN JSONB_TYPEOF(nc.declared_summary #> ");
    builder.push(path_literal.as_str());
    builder.push(") = 'number' THEN TO_TIMESTAMP((nc.declared_summary #>> ");
    builder.push(path_literal.as_str());
    builder.push(")::DOUBLE PRECISION) WHEN JSONB_TYPEOF(nc.declared_summary #> ");
    builder.push(path_literal.as_str());
    builder.push(") = 'string' AND nc.declared_summary #>> ");
    builder.push(path_literal.as_str());
    builder.push(" ~ '^[0-9]+(\\.[0-9]+)?$' THEN TO_TIMESTAMP((nc.declared_summary #>> ");
    builder.push(path_literal.as_str());
    builder.push(")::DOUBLE PRECISION) WHEN JSONB_TYPEOF(nc.declared_summary #> ");
    builder.push(path_literal.as_str());
    builder.push(") = 'string' AND nc.declared_summary #>> ");
    builder.push(path_literal.as_str());
    builder.push(" ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$' THEN (nc.declared_summary #>> ");
    builder.push(path_literal.as_str());
    builder.push(")::TIMESTAMPTZ ELSE NULL END");
}

fn timestamp_rank_expr(column: &str, order: AddressNamesCurrentOrder) -> String {
    match order {
        AddressNamesCurrentOrder::Asc => {
            format!("CASE WHEN {column} IS NULL THEN 1 ELSE 0 END")
        }
        AddressNamesCurrentOrder::Desc => {
            format!("CASE WHEN {column} IS NULL THEN 0 ELSE 1 END")
        }
    }
}

fn timestamp_null_rank(value: Option<OffsetDateTime>, order: AddressNamesCurrentOrder) -> i32 {
    match (value.is_none(), order) {
        (true, AddressNamesCurrentOrder::Asc) => 1,
        (false, AddressNamesCurrentOrder::Asc) => 0,
        (true, AddressNamesCurrentOrder::Desc) => 0,
        (false, AddressNamesCurrentOrder::Desc) => 1,
    }
}

fn escape_like_pattern(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('%', r"\%")
        .replace('_', r"\_")
}
