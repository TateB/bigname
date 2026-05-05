fn push_name_current_list_cursor_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    sort: NameCurrentListSort,
    order: NameCurrentListOrder,
    cursor: &'a NameCurrentListCursor,
) {
    match sort {
        NameCurrentListSort::Name => {
            let NameCurrentListCursorValue::Name(sort_value) = &cursor.sort_value else {
                return;
            };
            match order {
                NameCurrentListOrder::Asc => {
                    builder.push(
                        r#"
                        AND (
                            canonical_display_name > "#,
                    );
                    builder.push_bind(sort_value);
                    push_name_tie_after(builder, "canonical_display_name", sort_value, cursor);
                    builder.push(")");
                }
                NameCurrentListOrder::Desc => {
                    builder.push(
                        r#"
                        AND (
                            canonical_display_name < "#,
                    );
                    builder.push_bind(sort_value);
                    push_name_tie_after(builder, "canonical_display_name", sort_value, cursor);
                    builder.push(")");
                }
            }
        }
        NameCurrentListSort::ExpiryDate
        | NameCurrentListSort::RegistrationDate
        | NameCurrentListSort::CreatedAt => {
            let sort_value = match &cursor.sort_value {
                NameCurrentListCursorValue::Timestamp(sort_value) => *sort_value,
                NameCurrentListCursorValue::Name(_) => return,
            };
            let column = timestamp_sort_column(sort);
            let cursor_rank = timestamp_null_rank(sort_value, order);
            builder.push(" AND (");
            builder.push(timestamp_rank_expr(column, order));
            builder.push(" > ");
            builder.push_bind(cursor_rank);
            builder.push(" OR (");
            builder.push(timestamp_rank_expr(column, order));
            builder.push(" = ");
            builder.push_bind(cursor_rank);
            builder.push(" AND ");
            match sort_value {
                None => {
                    push_timestamp_tie_after(builder, column, None, cursor);
                }
                Some(value) => match order {
                    NameCurrentListOrder::Asc => {
                        builder.push("(");
                        builder.push(column);
                        builder.push(" > ");
                        builder.push_bind(value);
                        builder.push(" OR (");
                        builder.push(column);
                        builder.push(" = ");
                        builder.push_bind(value);
                        builder.push(" AND ");
                        push_timestamp_tie_after(builder, column, Some(value), cursor);
                        builder.push("))");
                    }
                    NameCurrentListOrder::Desc => {
                        builder.push("(");
                        builder.push(column);
                        builder.push(" < ");
                        builder.push_bind(value);
                        builder.push(" OR (");
                        builder.push(column);
                        builder.push(" = ");
                        builder.push_bind(value);
                        builder.push(" AND ");
                        push_timestamp_tie_after(builder, column, Some(value), cursor);
                        builder.push("))");
                    }
                },
            }
            builder.push("))");
        }
    }
}

fn push_name_tie_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    sort_column: &str,
    sort_value: &'a str,
    cursor: &'a NameCurrentListCursor,
) {
    builder.push(" OR (");
    builder.push(sort_column);
    builder.push(" = ");
    builder.push_bind(sort_value);
    builder.push(" AND (namespace, normalized_name, namehash) > (");
    builder.push_bind(&cursor.namespace);
    builder.push(", ");
    builder.push_bind(&cursor.normalized_name);
    builder.push(", ");
    builder.push_bind(&cursor.namehash);
    builder.push("))");
}

fn push_timestamp_tie_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    value: Option<OffsetDateTime>,
    cursor: &'a NameCurrentListCursor,
) {
    match value {
        None => {
            builder.push(column);
            builder.push(" IS NULL AND ");
        }
        Some(value) => {
            builder.push(column);
            builder.push(" = ");
            builder.push_bind(value);
            builder.push(" AND ");
        }
    }
    builder.push("(namespace, normalized_name, namehash) > (");
    builder.push_bind(&cursor.namespace);
    builder.push(", ");
    builder.push_bind(&cursor.normalized_name);
    builder.push(", ");
    builder.push_bind(&cursor.namehash);
    builder.push(")");
}

fn push_name_current_list_order(
    builder: &mut QueryBuilder<'_, Postgres>,
    sort: NameCurrentListSort,
    order: NameCurrentListOrder,
) {
    match sort {
        NameCurrentListSort::Name => {
            builder.push(" ORDER BY canonical_display_name ");
            builder.push(match order {
                NameCurrentListOrder::Asc => "ASC",
                NameCurrentListOrder::Desc => "DESC",
            });
            builder.push(", namespace ASC, normalized_name ASC, namehash ASC");
        }
        NameCurrentListSort::ExpiryDate
        | NameCurrentListSort::RegistrationDate
        | NameCurrentListSort::CreatedAt => {
            let column = timestamp_sort_column(sort);
            builder.push(" ORDER BY ");
            builder.push(timestamp_rank_expr(column, order));
            builder.push(" ASC, ");
            builder.push(column);
            builder.push(" ");
            builder.push(match order {
                NameCurrentListOrder::Asc => "ASC",
                NameCurrentListOrder::Desc => "DESC",
            });
            builder.push(", namespace ASC, normalized_name ASC, namehash ASC");
        }
    }
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

fn timestamp_sort_column(sort: NameCurrentListSort) -> &'static str {
    match sort {
        NameCurrentListSort::Name => "canonical_display_name",
        NameCurrentListSort::ExpiryDate => "expiry_date",
        NameCurrentListSort::RegistrationDate => "registration_date",
        NameCurrentListSort::CreatedAt => "created_at",
    }
}

fn timestamp_rank_expr(column: &str, order: NameCurrentListOrder) -> String {
    match order {
        NameCurrentListOrder::Asc => {
            format!("CASE WHEN {column} IS NULL THEN 1 ELSE 0 END")
        }
        NameCurrentListOrder::Desc => {
            format!("CASE WHEN {column} IS NULL THEN 0 ELSE 1 END")
        }
    }
}

fn timestamp_null_rank(value: Option<OffsetDateTime>, order: NameCurrentListOrder) -> i32 {
    match (value.is_none(), order) {
        (true, NameCurrentListOrder::Asc) => 1,
        (false, NameCurrentListOrder::Asc) => 0,
        (true, NameCurrentListOrder::Desc) => 0,
        (false, NameCurrentListOrder::Desc) => 1,
    }
}

fn decode_name_current_list_row(row: PgRow) -> Result<NameCurrentListRow> {
    let labelhash = crate::sql_row::get(&row, "labelhash")?;
    let token_id = crate::sql_row::get(&row, "token_id")?;
    let owner = crate::sql_row::get(&row, "owner")?;
    let registrant = crate::sql_row::get(&row, "registrant")?;
    let created_at = crate::sql_row::get(&row, "created_at")?;
    let registration_date = crate::sql_row::get(&row, "registration_date")?;
    let expiry_date = crate::sql_row::get(&row, "expiry_date")?;
    let resolver_address = crate::sql_row::get(&row, "resolver_address")?;
    let row = decode_name_current_row(row)?;

    Ok(NameCurrentListRow {
        row,
        labelhash,
        token_id,
        owner,
        registrant,
        created_at,
        registration_date,
        expiry_date,
        resolver_address,
    })
}

pub fn name_current_list_cursor_from_row(
    row: &NameCurrentListRow,
    sort: NameCurrentListSort,
) -> NameCurrentListCursor {
    NameCurrentListCursor {
        sort_value: match sort {
            NameCurrentListSort::Name => {
                NameCurrentListCursorValue::Name(row.row.canonical_display_name.clone())
            }
            NameCurrentListSort::ExpiryDate => {
                NameCurrentListCursorValue::Timestamp(row.expiry_date)
            }
            NameCurrentListSort::RegistrationDate => {
                NameCurrentListCursorValue::Timestamp(row.registration_date)
            }
            NameCurrentListSort::CreatedAt => NameCurrentListCursorValue::Timestamp(row.created_at),
        },
        namespace: row.row.namespace.clone(),
        normalized_name: row.row.normalized_name.clone(),
        namehash: row.row.namehash.clone(),
    }
}

fn escape_like_pattern(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('%', r"\%")
        .replace('_', r"\_")
}

