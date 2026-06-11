// This helper is included by standalone projection modules; each caller uses one family subset.
#![allow(dead_code)]

use anyhow::{Context, Result, bail};
use bigname_storage::{
    ChildrenCurrentRow, PermissionsCurrentRow, PrimaryNameCurrentSnapshot,
    RecordInventoryCurrentRow, ResolverCurrentRow,
};
use serde_json::Value;
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder, types::Uuid};

pub(crate) const CHILDREN_CURRENT_COLUMNS: &[&str] = &[
    "parent_logical_name_id",
    "child_logical_name_id",
    "surface_class",
    "namespace",
    "canonical_display_name",
    "normalized_name",
    "namehash",
    "labelhash",
    "owner",
    "registrant",
    "provenance",
    "chain_positions",
    "canonicality_summary",
    "manifest_version",
    "last_recomputed_at",
];
pub(crate) const PERMISSIONS_CURRENT_COLUMNS: &[&str] = &[
    "resource_id",
    "subject",
    "scope",
    "scope_kind",
    "scope_detail",
    "effective_powers",
    "grant_source",
    "revocation_source",
    "inheritance_path",
    "transfer_behavior",
    "provenance",
    "coverage",
    "chain_positions",
    "canonicality_summary",
    "manifest_version",
    "last_recomputed_at",
];
pub(crate) const PRIMARY_NAMES_CURRENT_COLUMNS: &[&str] = &[
    "address",
    "coin_type",
    "namespace",
    "claim_status",
    "raw_claim_name",
    "normalized_claim_name",
    "claim_provenance",
];
pub(crate) const RECORD_INVENTORY_CURRENT_COLUMNS: &[&str] = &[
    "resource_id",
    "record_version_boundary_key",
    "record_version_boundary",
    "enumeration_basis",
    "selectors",
    "explicit_gaps",
    "unsupported_families",
    "last_change",
    "entries",
    "provenance",
    "coverage",
    "chain_positions",
    "canonicality_summary",
    "manifest_version",
    "last_recomputed_at",
];
pub(crate) const RESOLVER_CURRENT_COLUMNS: &[&str] = &[
    "chain_id",
    "resolver_address",
    "declared_summary",
    "provenance",
    "coverage",
    "chain_positions",
    "canonicality_summary",
    "manifest_version",
    "last_recomputed_at",
];

pub(crate) async fn create_stage_table(
    conn: &mut PgConnection,
    target_table: &str,
) -> Result<String> {
    let stage_table = format!("{target_table}_stage");
    sqlx::query(&format!("DROP TABLE IF EXISTS {stage_table}"))
        .execute(&mut *conn)
        .await
        .with_context(|| format!("failed to reset {target_table} staging table"))?;
    sqlx::query(&format!(
        "CREATE TEMP TABLE {stage_table} (LIKE {target_table} INCLUDING DEFAULTS)"
    ))
    .execute(&mut *conn)
    .await
    .with_context(|| format!("failed to create {target_table} staging table"))?;
    Ok(stage_table)
}

pub(crate) async fn drop_stage_table(conn: &mut PgConnection, stage_table: &str) -> Result<()> {
    sqlx::query(&format!("DROP TABLE IF EXISTS {stage_table}"))
        .execute(conn)
        .await
        .with_context(|| format!("failed to drop staging table {stage_table}"))?;
    Ok(())
}

pub(crate) async fn count_rows(
    conn: &mut PgConnection,
    table: &str,
    where_clause: Option<&str>,
) -> Result<u64> {
    let sql = match where_clause {
        Some(where_clause) => format!("SELECT COUNT(*)::BIGINT FROM {table} {where_clause}"),
        None => format!("SELECT COUNT(*)::BIGINT FROM {table}"),
    };
    let count = sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(conn)
        .await
        .with_context(|| format!("failed to count rows in {table}"))?;
    u64::try_from(count).context("row count must fit u64")
}

pub(crate) async fn publish_stage_table(
    conn: &mut PgConnection,
    target_table: &str,
    stage_table: &str,
    columns: &[&str],
    delete_where: Option<&str>,
) -> Result<(u64, u64)> {
    let column_list = columns.join(", ");
    let delete_sql = match delete_where {
        Some(delete_where) => format!("DELETE FROM {target_table} {delete_where}"),
        None => format!("DELETE FROM {target_table}"),
    };
    let insert_sql = format!(
        "INSERT INTO {target_table} ({column_list}) SELECT {column_list} FROM {stage_table}"
    );
    let mut tx = conn
        .begin()
        .await
        .with_context(|| format!("failed to open {target_table} replacement transaction"))?;
    let deleted = sqlx::query(&delete_sql)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("failed to delete old {target_table} rows"))?
        .rows_affected();
    let inserted = sqlx::query(&insert_sql)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("failed to publish staged {target_table} rows"))?
        .rows_affected();
    tx.commit()
        .await
        .with_context(|| format!("failed to commit {target_table} replacement"))?;
    Ok((deleted, inserted))
}

pub(crate) async fn stage_children_current_rows(
    conn: &mut PgConnection,
    stage_table: &str,
    rows: &[ChildrenCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut builder = QueryBuilder::<Postgres>::new(format!(
        "INSERT INTO {stage_table} ({}) ",
        CHILDREN_CURRENT_COLUMNS.join(", ")
    ));
    builder.push_values(rows, |mut values, row| {
        values.push_bind(&row.parent_logical_name_id);
        values.push_bind(&row.child_logical_name_id);
        values.push_bind(&row.surface_class);
        values.push_bind(&row.namespace);
        values.push_bind(&row.canonical_display_name);
        values.push_bind(&row.normalized_name);
        values.push_bind(&row.namehash);
        values.push_bind(&row.labelhash);
        values.push_bind(&row.owner);
        values.push_bind(&row.registrant);
        values.push_bind(row.provenance.clone());
        values.push_bind(row.chain_positions.clone());
        values.push_bind(row.canonicality_summary.clone());
        values.push_bind(row.manifest_version);
        values.push_bind(row.last_recomputed_at);
    });
    execute_stage_insert(conn, builder, "children_current").await
}

pub(crate) async fn stage_permissions_current_rows(
    conn: &mut PgConnection,
    stage_table: &str,
    rows: &[PermissionsCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut builder = QueryBuilder::<Postgres>::new(format!(
        "INSERT INTO {stage_table} ({}) ",
        PERMISSIONS_CURRENT_COLUMNS.join(", ")
    ));
    builder.push_values(rows, |mut values, row| {
        values.push_bind(row.resource_id);
        values.push_bind(&row.subject);
        values.push_bind(row.scope.storage_key());
        values.push_bind(row.scope.kind());
        values.push_bind(row.scope.detail());
        values.push_bind(row.effective_powers.clone());
        values.push_bind(row.grant_source.clone());
        values.push_bind(row.revocation_source.clone());
        values.push_bind(row.inheritance_path.clone());
        values.push_bind(row.transfer_behavior.clone());
        values.push_bind(row.provenance.clone());
        values.push_bind(row.coverage.clone());
        values.push_bind(row.chain_positions.clone());
        values.push_bind(row.canonicality_summary.clone());
        values.push_bind(row.manifest_version);
        values.push_bind(row.last_recomputed_at);
    });
    execute_stage_insert(conn, builder, "permissions_current").await
}

pub(crate) async fn stage_primary_names_current_snapshots(
    conn: &mut PgConnection,
    stage_table: &str,
    snapshots: &[PrimaryNameCurrentSnapshot],
) -> Result<u64> {
    if snapshots.is_empty() {
        return Ok(0);
    }
    let mut builder = QueryBuilder::<Postgres>::new(format!(
        "INSERT INTO {stage_table} ({}) ",
        PRIMARY_NAMES_CURRENT_COLUMNS.join(", ")
    ));
    builder.push_values(snapshots, |mut values, snapshot| {
        values.push_bind(snapshot.row.address.to_ascii_lowercase());
        values.push_bind(&snapshot.row.coin_type);
        values.push_bind(&snapshot.row.namespace);
        values.push_bind(snapshot.row.claim_status.as_str());
        values.push_bind(&snapshot.row.raw_claim_name);
        values.push_bind(&snapshot.normalized_claim_name);
        values.push_bind(snapshot.row.claim_provenance.clone());
    });
    execute_stage_insert(conn, builder, "primary_names_current").await
}

pub(crate) async fn stage_record_inventory_current_rows(
    conn: &mut PgConnection,
    stage_table: &str,
    rows: &[RecordInventoryCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let prepared_rows = rows
        .iter()
        .map(|row| {
            Ok((
                row,
                record_version_boundary_storage_key(&row.record_version_boundary, row.resource_id)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut builder = QueryBuilder::<Postgres>::new(format!(
        "INSERT INTO {stage_table} ({}) ",
        RECORD_INVENTORY_CURRENT_COLUMNS.join(", ")
    ));
    builder.push_values(&prepared_rows, |mut values, (row, boundary_key)| {
        values.push_bind(row.resource_id);
        values.push_bind(boundary_key);
        values.push_bind(row.record_version_boundary.clone());
        values.push_bind(row.enumeration_basis.clone());
        values.push_bind(row.selectors.clone());
        values.push_bind(row.explicit_gaps.clone());
        values.push_bind(row.unsupported_families.clone());
        values.push_bind(row.last_change.clone());
        values.push_bind(row.entries.clone());
        values.push_bind(row.provenance.clone());
        values.push_bind(row.coverage.clone());
        values.push_bind(row.chain_positions.clone());
        values.push_bind(row.canonicality_summary.clone());
        values.push_bind(row.manifest_version);
        values.push_bind(row.last_recomputed_at);
    });
    execute_stage_insert(conn, builder, "record_inventory_current").await
}

pub(crate) async fn stage_resolver_current_rows(
    conn: &mut PgConnection,
    stage_table: &str,
    rows: &[ResolverCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut builder = QueryBuilder::<Postgres>::new(format!(
        "INSERT INTO {stage_table} ({}) ",
        RESOLVER_CURRENT_COLUMNS.join(", ")
    ));
    builder.push_values(rows, |mut values, row| {
        values.push_bind(&row.chain_id);
        values.push_bind(row.resolver_address.to_ascii_lowercase());
        values.push_bind(row.declared_summary.clone());
        values.push_bind(row.provenance.clone());
        values.push_bind(row.coverage.clone());
        values.push_bind(row.chain_positions.clone());
        values.push_bind(row.canonicality_summary.clone());
        values.push_bind(row.manifest_version);
        values.push_bind(row.last_recomputed_at);
    });
    execute_stage_insert(conn, builder, "resolver_current").await
}

async fn execute_stage_insert(
    conn: &mut PgConnection,
    mut builder: QueryBuilder<'_, Postgres>,
    projection: &str,
) -> Result<u64> {
    builder
        .build()
        .execute(conn)
        .await
        .with_context(|| format!("failed to stage {projection} rows"))
        .map(|result| result.rows_affected())
}

pub(super) fn record_version_boundary_storage_key(
    record_version_boundary: &Value,
    expected_resource_id: Uuid,
) -> Result<String> {
    let object = record_version_boundary
        .as_object()
        .context("record_version_boundary must be a JSON object")?;
    let logical_name_id = required_string(object, "logical_name_id")?;
    let resource_id = Uuid::parse_str(required_string(object, "resource_id")?)
        .context("record_version_boundary resource_id must be a UUID")?;
    if resource_id != expected_resource_id {
        bail!(
            "record_version_boundary resource_id {resource_id} does not match storage key resource_id {expected_resource_id}"
        );
    }
    let normalized_event_id = match object.get("normalized_event_id") {
        Some(Value::Null) => None,
        Some(value) => Some(value.as_i64().filter(|value| *value > 0).with_context(
            || "record_version_boundary normalized_event_id must be null or positive integer",
        )?),
        None => bail!("record_version_boundary must include normalized_event_id"),
    };
    let event_kind = match object.get("event_kind") {
        Some(Value::Null) => None,
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.as_str()),
        Some(_) => bail!("record_version_boundary event_kind must be null or non-empty string"),
        None => bail!("record_version_boundary must include event_kind"),
    };
    if normalized_event_id.is_some() != event_kind.is_some() {
        bail!(
            "record_version_boundary normalized_event_id and event_kind must both be present or both be null"
        );
    }
    let chain_position = object
        .get("chain_position")
        .and_then(Value::as_object)
        .context("record_version_boundary must include chain_position object")?;
    let mut key = String::new();
    append_key_part(&mut key, logical_name_id);
    append_key_part(&mut key, &resource_id.to_string());
    append_key_part(
        &mut key,
        &normalized_event_id
            .map(|value| value.to_string())
            .unwrap_or_default(),
    );
    append_key_part(&mut key, event_kind.unwrap_or_default());
    append_key_part(&mut key, required_string(chain_position, "chain_id")?);
    append_key_part(
        &mut key,
        &chain_position
            .get("block_number")
            .and_then(Value::as_i64)
            .filter(|value| *value >= 0)
            .context("record_version_boundary chain_position must include block_number")?
            .to_string(),
    );
    append_key_part(&mut key, required_string(chain_position, "block_hash")?);
    append_key_part(&mut key, required_string(chain_position, "timestamp")?);
    Ok(key)
}

fn append_key_part(buffer: &mut String, value: &str) {
    buffer.push_str(&value.len().to_string());
    buffer.push(':');
    buffer.push_str(value);
    buffer.push(';');
}

fn required_string<'a>(object: &'a serde_json::Map<String, Value>, field: &str) -> Result<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("record_version_boundary must include non-empty {field}"))
}
