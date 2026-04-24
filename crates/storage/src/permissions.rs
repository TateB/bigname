use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};
use uuid::Uuid;

use crate::projection_helpers::{
    checked_page_limit_i64, checked_page_size_usize, serialize_jsonb_field,
    serialize_optional_jsonb_field, take_json_array,
};

const DEFAULT_PERMISSIONS_CURRENT_READ_FILTER: &str = r#"
  AND resource.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
"#;

/// Persisted current effective permission row for one resource-anchored subject and scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionsCurrentRow {
    pub resource_id: Uuid,
    pub subject: String,
    pub scope: PermissionScope,
    pub effective_powers: Value,
    pub grant_source: Value,
    pub revocation_source: Option<Value>,
    pub inheritance_path: Value,
    pub transfer_behavior: Value,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Keyset cursor fields for the frozen subject/scope permissions order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionsCurrentKeysetCursor {
    pub subject: String,
    pub scope: String,
}

/// Compact summary over the full filtered permissions collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionsCurrentFullFilterSummary {
    pub row_count: i64,
    pub provenance: Vec<Value>,
    pub coverage: Option<Value>,
    pub chain_positions: Vec<Value>,
    pub canonicality_summaries: Vec<Value>,
    pub last_recomputed_at: Option<OffsetDateTime>,
}

/// Bounded keyset page plus full-filter summary data for permissions reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionsCurrentPage {
    pub rows: Vec<PermissionsCurrentRow>,
    pub next_cursor: Option<PermissionsCurrentKeysetCursor>,
    pub summary: PermissionsCurrentFullFilterSummary,
}

/// Stable storage representation for permission scope keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PermissionScope {
    Root,
    Registry,
    Resource,
    Resolver {
        chain_id: String,
        resolver_address: String,
    },
    RecordManager {
        chain_id: String,
        manager_address: String,
    },
    MigrationDerived {
        predecessor_resource_id: Uuid,
    },
    TransportDerived {
        transport: String,
    },
}

impl PermissionScope {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Registry => "registry",
            Self::Resource => "resource",
            Self::Resolver { .. } => "resolver",
            Self::RecordManager { .. } => "record_manager",
            Self::MigrationDerived { .. } => "migration_derived",
            Self::TransportDerived { .. } => "transport_derived",
        }
    }

    pub fn storage_key(&self) -> String {
        match self {
            Self::Root => "root".to_owned(),
            Self::Registry => "registry".to_owned(),
            Self::Resource => "resource".to_owned(),
            Self::Resolver {
                chain_id,
                resolver_address,
            } => format!(
                "resolver:{chain_id}:{}",
                resolver_address.to_ascii_lowercase()
            ),
            Self::RecordManager {
                chain_id,
                manager_address,
            } => format!(
                "record_manager:{chain_id}:{}",
                manager_address.to_ascii_lowercase()
            ),
            Self::MigrationDerived {
                predecessor_resource_id,
            } => format!("migration_derived:{predecessor_resource_id}"),
            Self::TransportDerived { transport } => format!("transport_derived:{transport}"),
        }
    }

    pub fn detail(&self) -> Value {
        match self {
            Self::Root | Self::Registry | Self::Resource => json!({}),
            Self::Resolver {
                chain_id,
                resolver_address,
            } => json!({
                "chain_id": chain_id,
                "resolver_address": resolver_address.to_ascii_lowercase(),
            }),
            Self::RecordManager {
                chain_id,
                manager_address,
            } => json!({
                "chain_id": chain_id,
                "manager_address": manager_address.to_ascii_lowercase(),
            }),
            Self::MigrationDerived {
                predecessor_resource_id,
            } => json!({
                "predecessor_resource_id": predecessor_resource_id,
            }),
            Self::TransportDerived { transport } => json!({
                "transport": transport,
            }),
        }
    }

    fn parse(scope_kind: &str, scope_detail: &Value) -> Result<Self> {
        match scope_kind {
            "root" => Ok(Self::Root),
            "registry" => Ok(Self::Registry),
            "resource" => Ok(Self::Resource),
            "resolver" => {
                let chain_id = json_text_field(scope_detail, "chain_id")?;
                let resolver_address = json_text_field(scope_detail, "resolver_address")?;
                Ok(Self::Resolver {
                    chain_id,
                    resolver_address: resolver_address.to_ascii_lowercase(),
                })
            }
            "record_manager" => {
                let chain_id = json_text_field(scope_detail, "chain_id")?;
                let manager_address = json_text_field(scope_detail, "manager_address")?;
                Ok(Self::RecordManager {
                    chain_id,
                    manager_address: manager_address.to_ascii_lowercase(),
                })
            }
            "migration_derived" => {
                let predecessor_resource_id = Uuid::parse_str(&json_text_field(
                    scope_detail,
                    "predecessor_resource_id",
                )?)
                .context(
                    "permissions_current scope_detail.predecessor_resource_id must be a UUID",
                )?;
                Ok(Self::MigrationDerived {
                    predecessor_resource_id,
                })
            }
            "transport_derived" => Ok(Self::TransportDerived {
                transport: json_text_field(scope_detail, "transport")?,
            }),
            _ => bail!("unknown permissions_current scope_kind {scope_kind}"),
        }
    }
}

impl From<&PermissionsCurrentRow> for PermissionsCurrentKeysetCursor {
    fn from(row: &PermissionsCurrentRow) -> Self {
        Self {
            subject: row.subject.clone(),
            scope: row.scope.storage_key(),
        }
    }
}

/// Load resource-centric permission rows with optional exact subject and scope filters.
pub async fn load_permissions_current(
    pool: &PgPool,
    resource_id: Uuid,
    subject: Option<&str>,
    scope: Option<&PermissionScope>,
) -> Result<Vec<PermissionsCurrentRow>> {
    let scope_storage_key = scope.map(PermissionScope::storage_key);
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            pc.resource_id,
            pc.subject,
            pc.scope,
            pc.scope_kind,
            pc.scope_detail,
            pc.effective_powers,
            pc.grant_source,
            pc.revocation_source,
            pc.inheritance_path,
            pc.transfer_behavior,
            pc.provenance,
            pc.coverage,
            pc.chain_positions,
            pc.canonicality_summary,
            pc.manifest_version,
            pc.last_recomputed_at
        FROM permissions_current pc
        JOIN resources resource
          ON resource.resource_id = pc.resource_id
        WHERE "#,
    );
    push_permissions_current_filters(
        &mut builder,
        resource_id,
        subject,
        scope_storage_key.as_deref(),
    );

    builder.push(" ORDER BY pc.subject ASC, pc.scope ASC");

    let rows = builder.build().fetch_all(pool).await.with_context(|| {
        format!("failed to load permissions_current rows for resource_id {resource_id}")
    })?;

    rows.into_iter()
        .map(decode_permissions_current_row)
        .collect()
}

async fn load_permissions_current_full_filter_summary(
    pool: &PgPool,
    resource_id: Uuid,
    subject: Option<&str>,
    scope_storage_key: Option<&str>,
) -> Result<PermissionsCurrentFullFilterSummary> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            COUNT(*)::BIGINT AS row_count,
            COALESCE(jsonb_agg(pc.provenance ORDER BY pc.subject ASC, pc.scope ASC), '[]'::jsonb) AS provenance,
            (jsonb_agg(pc.coverage ORDER BY pc.subject ASC, pc.scope ASC)->0) AS coverage,
            COALESCE(jsonb_agg(pc.chain_positions ORDER BY pc.subject ASC, pc.scope ASC), '[]'::jsonb) AS chain_positions,
            COALESCE(jsonb_agg(pc.canonicality_summary ORDER BY pc.subject ASC, pc.scope ASC), '[]'::jsonb) AS canonicality_summaries,
            MAX(pc.last_recomputed_at) AS last_recomputed_at
        FROM permissions_current pc
        JOIN resources resource
          ON resource.resource_id = pc.resource_id
        WHERE "#,
    );
    push_permissions_current_filters(&mut builder, resource_id, subject, scope_storage_key);

    let row = builder.build().fetch_one(pool).await.with_context(|| {
        format!("failed to summarize permissions_current rows for resource_id {resource_id}")
    })?;

    decode_permissions_current_full_filter_summary(row)
}

/// Load one bounded keyset page for a resource's current permission rows.
pub async fn load_permissions_current_page(
    pool: &PgPool,
    resource_id: Uuid,
    subject: Option<&str>,
    scope: Option<&PermissionScope>,
    cursor: Option<&PermissionsCurrentKeysetCursor>,
    page_size: u64,
) -> Result<PermissionsCurrentPage> {
    let limit = checked_page_limit_i64(
        page_size,
        "permissions_current page_size must be positive",
        "permissions_current page_size is too large",
    )?;
    let page_size_usize = checked_page_size_usize(
        page_size,
        "permissions_current page_size must be positive",
        "permissions_current page_size must fit in usize",
    )?;
    let scope_storage_key = scope.map(PermissionScope::storage_key);

    let page_rows = {
        let mut page_builder = QueryBuilder::<Postgres>::new(
            r#"
            SELECT
                pc.resource_id,
                pc.subject,
                pc.scope,
                pc.scope_kind,
                pc.scope_detail,
                pc.effective_powers,
                pc.grant_source,
                pc.revocation_source,
                pc.inheritance_path,
                pc.transfer_behavior,
                pc.provenance,
                pc.coverage,
                pc.chain_positions,
                pc.canonicality_summary,
                pc.manifest_version,
                pc.last_recomputed_at
            FROM permissions_current pc
            JOIN resources resource
              ON resource.resource_id = pc.resource_id
            WHERE "#,
        );
        push_permissions_current_filters(
            &mut page_builder,
            resource_id,
            subject,
            scope_storage_key.as_deref(),
        );
        push_permissions_current_keyset_cursor(&mut page_builder, cursor);
        page_builder.push(" ORDER BY pc.subject ASC, pc.scope ASC LIMIT ");
        page_builder.push_bind(limit);

        page_builder
            .build()
            .fetch_all(pool)
            .await
            .with_context(|| {
                format!("failed to load permissions_current page for resource_id {resource_id}")
            })?
    };

    let mut rows = page_rows
        .into_iter()
        .map(decode_permissions_current_row)
        .collect::<Result<Vec<_>>>()?;
    let has_next_page = rows.len() > page_size_usize;
    if has_next_page {
        rows.truncate(page_size_usize);
    }
    let next_cursor = has_next_page
        .then(|| rows.last().map(PermissionsCurrentKeysetCursor::from))
        .flatten();

    let summary = load_permissions_current_full_filter_summary(
        pool,
        resource_id,
        subject,
        scope_storage_key.as_deref(),
    )
    .await?;

    Ok(PermissionsCurrentPage {
        rows,
        next_cursor,
        summary,
    })
}

/// Load current permission rows for many resources, grouped by resource_id.
pub async fn load_permissions_current_by_resource_ids(
    pool: &PgPool,
    resource_ids: &[Uuid],
) -> Result<BTreeMap<Uuid, Vec<PermissionsCurrentRow>>> {
    let mut grouped = resource_ids
        .iter()
        .copied()
        .map(|resource_id| (resource_id, Vec::new()))
        .collect::<BTreeMap<_, _>>();

    if grouped.is_empty() {
        return Ok(grouped);
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            pc.resource_id,
            pc.subject,
            pc.scope,
            pc.scope_kind,
            pc.scope_detail,
            pc.effective_powers,
            pc.grant_source,
            pc.revocation_source,
            pc.inheritance_path,
            pc.transfer_behavior,
            pc.provenance,
            pc.coverage,
            pc.chain_positions,
            pc.canonicality_summary,
            pc.manifest_version,
            pc.last_recomputed_at
        FROM permissions_current pc
        JOIN resources resource
          ON resource.resource_id = pc.resource_id
        WHERE pc.resource_id IN ("#,
    );
    {
        let mut separated = builder.separated(", ");
        for resource_id in grouped.keys() {
            separated.push_bind(*resource_id);
        }
        separated.push_unseparated(")");
    }
    builder.push(DEFAULT_PERMISSIONS_CURRENT_READ_FILTER);
    builder.push(" ORDER BY pc.resource_id ASC, pc.subject ASC, pc.scope ASC");

    let rows = builder.build().fetch_all(pool).await.with_context(|| {
        format!(
            "failed to load permissions_current rows for {} resource_ids",
            grouped.len()
        )
    })?;

    for row in rows {
        let row = decode_permissions_current_row(row)?;
        grouped.entry(row.resource_id).or_default().push(row);
    }

    Ok(grouped)
}

/// Load persisted resolver-scoped permission rows across all resources.
pub async fn load_permissions_current_for_resolver_scope(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<Vec<PermissionsCurrentRow>> {
    let scope = PermissionScope::Resolver {
        chain_id: chain_id.to_owned(),
        resolver_address: resolver_address.to_ascii_lowercase(),
    }
    .storage_key();
    let rows = sqlx::query(
        r#"
        SELECT
            pc.resource_id,
            pc.subject,
            pc.scope,
            pc.scope_kind,
            pc.scope_detail,
            pc.effective_powers,
            pc.grant_source,
            pc.revocation_source,
            pc.inheritance_path,
            pc.transfer_behavior,
            pc.provenance,
            pc.coverage,
            pc.chain_positions,
            pc.canonicality_summary,
            pc.manifest_version,
            pc.last_recomputed_at
        FROM permissions_current pc
        JOIN resources resource
          ON resource.resource_id = pc.resource_id
        WHERE pc.scope = $1
          AND pc.scope_kind = 'resolver'
          AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY pc.subject ASC, pc.resource_id ASC, pc.manifest_version ASC
        "#,
    )
    .bind(scope)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load resolver-scoped permissions_current rows for chain {chain_id} resolver {resolver_address}"
        )
    })?;

    rows.into_iter()
        .map(decode_permissions_current_row)
        .collect()
}

/// Discover resolver targets represented by persisted resolver-scoped permission rows.
pub async fn load_permissions_current_resolver_targets(
    pool: &PgPool,
) -> Result<Vec<(String, String)>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT
            pc.scope_detail->>'chain_id' AS chain_id,
            LOWER(pc.scope_detail->>'resolver_address') AS resolver_address
        FROM permissions_current pc
        JOIN resources resource
          ON resource.resource_id = pc.resource_id
        WHERE pc.scope_kind = 'resolver'
          AND pc.scope_detail->>'chain_id' IS NOT NULL
          AND pc.scope_detail->>'chain_id' <> ''
          AND pc.scope_detail->>'resolver_address' IS NOT NULL
          AND pc.scope_detail->>'resolver_address' <> ''
          AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY chain_id, resolver_address
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load resolver targets from permissions_current")?;

    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("chain_id")?,
                row.try_get::<String, _>("resolver_address")?
                    .to_ascii_lowercase(),
            ))
        })
        .collect()
}

/// Insert or replace resource-centric permission rows.
pub async fn upsert_permissions_current_rows(
    pool: &PgPool,
    rows: &[PermissionsCurrentRow],
) -> Result<Vec<PermissionsCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for permissions_current upsert")?;

    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        validate_permissions_current_row(row)?;
        snapshots.push(upsert_permissions_current_row(&mut transaction, row).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit permissions_current upsert")?;

    Ok(snapshots)
}

/// Delete all permission rows for one resource so a worker can rebuild that collection key.
pub async fn delete_permissions_current(pool: &PgPool, resource_id: Uuid) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM permissions_current
        WHERE resource_id = $1
        "#,
    )
    .bind(resource_id)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete permissions_current rows for resource_id {resource_id}")
    })
    .map(|result| result.rows_affected())
}

/// Clear the resource-centric permissions projection so a worker can perform a one-shot rebuild.
pub async fn clear_permissions_current(pool: &PgPool) -> Result<u64> {
    sqlx::query("DELETE FROM permissions_current")
        .execute(pool)
        .await
        .context("failed to clear permissions_current rows")
        .map(|result| result.rows_affected())
}

async fn upsert_permissions_current_row(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &PermissionsCurrentRow,
) -> Result<PermissionsCurrentRow> {
    let scope = row.scope.storage_key();
    let scope_kind = row.scope.kind();
    let scope_detail =
        serialize_jsonb_field(&row.scope.detail(), "failed to serialize scope_detail")?;
    let effective_powers = serialize_jsonb_field(
        &row.effective_powers,
        "failed to serialize permissions_current effective_powers",
    )?;
    let grant_source = serialize_jsonb_field(
        &row.grant_source,
        "failed to serialize permissions_current grant_source",
    )?;
    let revocation_source = serialize_optional_jsonb_field(
        row.revocation_source.as_ref(),
        "failed to serialize permissions_current revocation_source",
    )?;
    let inheritance_path = serialize_jsonb_field(
        &row.inheritance_path,
        "failed to serialize permissions_current inheritance_path",
    )?;
    let transfer_behavior = serialize_jsonb_field(
        &row.transfer_behavior,
        "failed to serialize permissions_current transfer_behavior",
    )?;
    let provenance = serialize_jsonb_field(
        &row.provenance,
        "failed to serialize permissions_current provenance",
    )?;
    let coverage = serialize_jsonb_field(
        &row.coverage,
        "failed to serialize permissions_current coverage",
    )?;
    let chain_positions = serialize_jsonb_field(
        &row.chain_positions,
        "failed to serialize permissions_current chain_positions",
    )?;
    let canonicality_summary = serialize_jsonb_field(
        &row.canonicality_summary,
        "failed to serialize permissions_current canonicality_summary",
    )?;

    let snapshot = sqlx::query(
        r#"
        INSERT INTO permissions_current (
            resource_id,
            subject,
            scope,
            scope_kind,
            scope_detail,
            effective_powers,
            grant_source,
            revocation_source,
            inheritance_path,
            transfer_behavior,
            provenance,
            coverage,
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
            $5::jsonb,
            $6::jsonb,
            $7::jsonb,
            $8::jsonb,
            $9::jsonb,
            $10::jsonb,
            $11::jsonb,
            $12::jsonb,
            $13::jsonb,
            $14::jsonb,
            $15,
            $16
        )
        ON CONFLICT (resource_id, subject, scope) DO UPDATE
        SET
            scope_kind = EXCLUDED.scope_kind,
            scope_detail = EXCLUDED.scope_detail,
            effective_powers = EXCLUDED.effective_powers,
            grant_source = EXCLUDED.grant_source,
            revocation_source = EXCLUDED.revocation_source,
            inheritance_path = EXCLUDED.inheritance_path,
            transfer_behavior = EXCLUDED.transfer_behavior,
            provenance = EXCLUDED.provenance,
            coverage = EXCLUDED.coverage,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING
            resource_id,
            subject,
            scope,
            scope_kind,
            scope_detail,
            effective_powers,
            grant_source,
            revocation_source,
            inheritance_path,
            transfer_behavior,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        "#,
    )
    .bind(row.resource_id)
    .bind(&row.subject)
    .bind(scope)
    .bind(scope_kind)
    .bind(scope_detail)
    .bind(effective_powers)
    .bind(grant_source)
    .bind(revocation_source)
    .bind(inheritance_path)
    .bind(transfer_behavior)
    .bind(provenance)
    .bind(coverage)
    .bind(chain_positions)
    .bind(canonicality_summary)
    .bind(row.manifest_version)
    .bind(row.last_recomputed_at)
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert permissions_current row for resource_id {} subject {} scope {}",
            row.resource_id,
            row.subject,
            row.scope.storage_key()
        )
    })?;

    decode_permissions_current_row(snapshot)
}

fn validate_permissions_current_row(row: &PermissionsCurrentRow) -> Result<()> {
    if row.subject.trim().is_empty() {
        bail!(
            "permissions_current row for resource_id {} must include subject",
            row.resource_id
        );
    }
    if row.manifest_version <= 0 {
        bail!(
            "permissions_current row for resource_id {} subject {} must include positive manifest_version",
            row.resource_id,
            row.subject
        );
    }

    match &row.scope {
        PermissionScope::Resolver {
            chain_id,
            resolver_address,
        } => {
            if chain_id.trim().is_empty() {
                bail!("resolver permission scope must include chain_id");
            }
            if resolver_address.trim().is_empty() {
                bail!("resolver permission scope must include resolver_address");
            }
        }
        PermissionScope::RecordManager {
            chain_id,
            manager_address,
        } => {
            if chain_id.trim().is_empty() {
                bail!("record_manager permission scope must include chain_id");
            }
            if manager_address.trim().is_empty() {
                bail!("record_manager permission scope must include manager_address");
            }
        }
        PermissionScope::TransportDerived { transport } => {
            if transport.trim().is_empty() {
                bail!("transport_derived permission scope must include transport");
            }
        }
        PermissionScope::Root
        | PermissionScope::Registry
        | PermissionScope::Resource
        | PermissionScope::MigrationDerived { .. } => {}
    }

    Ok(())
}

fn decode_permissions_current_row(row: PgRow) -> Result<PermissionsCurrentRow> {
    let scope_kind: String = row.try_get("scope_kind")?;
    let scope_detail: Value = row.try_get("scope_detail")?;
    let scope = PermissionScope::parse(&scope_kind, &scope_detail)?;
    let stored_scope: String = row.try_get("scope")?;
    let expected_scope = scope.storage_key();
    if stored_scope != expected_scope {
        bail!(
            "permissions_current scope mismatch for resource_id {} subject {}: stored {stored_scope}, decoded {expected_scope}",
            row.try_get::<Uuid, _>("resource_id")?,
            row.try_get::<String, _>("subject")?
        );
    }

    Ok(PermissionsCurrentRow {
        resource_id: row.try_get("resource_id")?,
        subject: row.try_get("subject")?,
        scope,
        effective_powers: row.try_get("effective_powers")?,
        grant_source: row.try_get("grant_source")?,
        revocation_source: row.try_get("revocation_source")?,
        inheritance_path: row.try_get("inheritance_path")?,
        transfer_behavior: row.try_get("transfer_behavior")?,
        provenance: row.try_get("provenance")?,
        coverage: row.try_get("coverage")?,
        chain_positions: row.try_get("chain_positions")?,
        canonicality_summary: row.try_get("canonicality_summary")?,
        manifest_version: row.try_get("manifest_version")?,
        last_recomputed_at: row.try_get("last_recomputed_at")?,
    })
}

fn decode_permissions_current_full_filter_summary(
    row: PgRow,
) -> Result<PermissionsCurrentFullFilterSummary> {
    Ok(PermissionsCurrentFullFilterSummary {
        row_count: row.try_get("row_count")?,
        provenance: json_array(row.try_get("provenance")?, "provenance")?,
        coverage: row.try_get("coverage")?,
        chain_positions: json_array(row.try_get("chain_positions")?, "chain_positions")?,
        canonicality_summaries: json_array(
            row.try_get("canonicality_summaries")?,
            "canonicality_summaries",
        )?,
        last_recomputed_at: row.try_get("last_recomputed_at")?,
    })
}

fn push_permissions_current_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    resource_id: Uuid,
    subject: Option<&'a str>,
    scope_storage_key: Option<&'a str>,
) {
    builder.push("pc.resource_id = ");
    builder.push_bind(resource_id);

    if let Some(subject) = subject {
        builder.push(" AND pc.subject = ");
        builder.push_bind(subject);
    }

    if let Some(scope_storage_key) = scope_storage_key {
        builder.push(" AND pc.scope = ");
        builder.push_bind(scope_storage_key);
    }

    builder.push(DEFAULT_PERMISSIONS_CURRENT_READ_FILTER);
}

fn push_permissions_current_keyset_cursor<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    cursor: Option<&'a PermissionsCurrentKeysetCursor>,
) {
    if let Some(cursor) = cursor {
        builder.push(" AND (pc.subject, pc.scope) > (");
        builder.push_bind(&cursor.subject);
        builder.push(", ");
        builder.push_bind(&cursor.scope);
        builder.push(")");
    }
}

fn json_array(value: Value, field: &str) -> Result<Vec<Value>> {
    take_json_array(value, || {
        format!("permissions_current summary field {field} must be a JSON array")
    })
}

fn json_text_field(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("permissions_current scope_detail must include {field}"))
}

#[cfg(test)]
mod tests;
