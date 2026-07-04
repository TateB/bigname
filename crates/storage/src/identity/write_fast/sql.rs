pub(super) fn canonicality_merge_sql_from(existing_table: &str, incoming_state: &str) -> String {
    format!(
        r#"
        CASE
            WHEN {incoming_state} = 'orphaned'::canonicality_state THEN
                'orphaned'::canonicality_state
            WHEN {incoming_state} = 'observed'::canonicality_state THEN
                CASE
                    WHEN {existing_table}.canonicality_state = 'orphaned'::canonicality_state THEN
                        'observed'::canonicality_state
                    ELSE {existing_table}.canonicality_state
                END
            WHEN {existing_table}.canonicality_state = 'orphaned'::canonicality_state THEN
                {incoming_state}
            WHEN (
                CASE {existing_table}.canonicality_state
                    WHEN 'observed'::canonicality_state THEN 0
                    WHEN 'canonical'::canonicality_state THEN 1
                    WHEN 'safe'::canonicality_state THEN 2
                    WHEN 'finalized'::canonicality_state THEN 3
                    WHEN 'orphaned'::canonicality_state THEN 4
                END
            ) >= (
                CASE {incoming_state}
                    WHEN 'observed'::canonicality_state THEN 0
                    WHEN 'canonical'::canonicality_state THEN 1
                    WHEN 'safe'::canonicality_state THEN 2
                    WHEN 'finalized'::canonicality_state THEN 3
                    WHEN 'orphaned'::canonicality_state THEN 4
                END
            ) THEN {existing_table}.canonicality_state
            ELSE {incoming_state}
        END
        "#,
    )
}

pub(super) fn canonicality_merge_sql(table_name: &str) -> String {
    canonicality_merge_sql_from(table_name, "EXCLUDED.canonicality_state")
}

pub(super) fn surface_binding_active_to_merge_sql(
    existing_table: &str,
    incoming_table: &str,
) -> String {
    format!(
        r#"
        CASE
            WHEN {existing_table}.active_to IS NOT NULL
             AND {incoming_table}.active_to IS NOT NULL THEN
                LEAST({existing_table}.active_to, {incoming_table}.active_to)
            WHEN {existing_table}.active_to IS NOT NULL THEN {existing_table}.active_to
            ELSE {incoming_table}.active_to
        END
        "#
    )
}

fn stable_anchor_matches_sql(table_name: &str) -> String {
    format!(
        r#"
        (
            {table_name}.chain_id = EXCLUDED.chain_id
            AND {table_name}.block_hash = EXCLUDED.block_hash
            AND {table_name}.block_number = EXCLUDED.block_number
        )
        "#
    )
}

pub(super) fn stable_provenance_merge_sql(table_name: &str) -> String {
    format!(
        r#"
        CASE
            WHEN {same_anchor}
             AND {table_name}.provenance = EXCLUDED.provenance THEN {table_name}.provenance
            ELSE EXCLUDED.provenance
        END
        "#,
        same_anchor = stable_anchor_matches_sql(table_name),
    )
}

pub(super) fn stable_anchor_refresh_required_sql(table_name: &str) -> String {
    format!(
        r#"
        (
            (
                {table_name}.canonicality_state = 'orphaned'::canonicality_state
                OR {same_anchor}
            )
            AND (
                {table_name}.chain_id IS DISTINCT FROM EXCLUDED.chain_id
                OR {table_name}.block_hash IS DISTINCT FROM EXCLUDED.block_hash
                OR {table_name}.block_number IS DISTINCT FROM EXCLUDED.block_number
                OR {table_name}.provenance IS DISTINCT FROM {provenance_merge}
                OR {table_name}.canonicality_state IS DISTINCT FROM {canonicality_merge}
            )
        )
        "#,
        same_anchor = stable_anchor_matches_sql(table_name),
        provenance_merge = stable_provenance_merge_sql(table_name),
        canonicality_merge = canonicality_merge_sql(table_name),
    )
}

pub(super) fn stable_later_anchor_canonicality_refresh_allowed_sql(table_name: &str) -> String {
    format!(
        r#"
        (
            EXCLUDED.canonicality_state <> 'orphaned'::canonicality_state
            AND {table_name}.canonicality_state IS DISTINCT FROM {canonicality_merge}
        )
        "#,
        canonicality_merge = canonicality_merge_sql(table_name),
    )
}
