use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};
use uuid::Uuid;

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

/// Load resource-centric permission rows with optional exact subject and scope filters.
pub async fn load_permissions_current(
    pool: &PgPool,
    resource_id: Uuid,
    subject: Option<&str>,
    scope: Option<&PermissionScope>,
) -> Result<Vec<PermissionsCurrentRow>> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
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
        FROM permissions_current
        WHERE resource_id = "#,
    );
    builder.push_bind(resource_id);

    if let Some(subject) = subject {
        builder.push(" AND subject = ");
        builder.push_bind(subject);
    }

    if let Some(scope) = scope {
        builder.push(" AND scope = ");
        builder.push_bind(scope.storage_key());
    }

    builder.push(" ORDER BY subject ASC, scope ASC");

    let rows = builder.build().fetch_all(pool).await.with_context(|| {
        format!("failed to load permissions_current rows for resource_id {resource_id}")
    })?;

    rows.into_iter()
        .map(decode_permissions_current_row)
        .collect()
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
        FROM permissions_current
        WHERE scope = $1
          AND scope_kind = 'resolver'
        ORDER BY subject ASC, resource_id ASC, manifest_version ASC
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
            scope_detail->>'chain_id' AS chain_id,
            LOWER(scope_detail->>'resolver_address') AS resolver_address
        FROM permissions_current
        WHERE scope_kind = 'resolver'
          AND scope_detail->>'chain_id' IS NOT NULL
          AND scope_detail->>'chain_id' <> ''
          AND scope_detail->>'resolver_address' IS NOT NULL
          AND scope_detail->>'resolver_address' <> ''
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
        serde_json::to_string(&row.scope.detail()).context("failed to serialize scope_detail")?;
    let effective_powers = serde_json::to_string(&row.effective_powers)
        .context("failed to serialize permissions_current effective_powers")?;
    let grant_source = serde_json::to_string(&row.grant_source)
        .context("failed to serialize permissions_current grant_source")?;
    let revocation_source = row
        .revocation_source
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("failed to serialize permissions_current revocation_source")?;
    let inheritance_path = serde_json::to_string(&row.inheritance_path)
        .context("failed to serialize permissions_current inheritance_path")?;
    let transfer_behavior = serde_json::to_string(&row.transfer_behavior)
        .context("failed to serialize permissions_current transfer_behavior")?;
    let provenance = serde_json::to_string(&row.provenance)
        .context("failed to serialize permissions_current provenance")?;
    let coverage = serde_json::to_string(&row.coverage)
        .context("failed to serialize permissions_current coverage")?;
    let chain_positions = serde_json::to_string(&row.chain_positions)
        .context("failed to serialize permissions_current chain_positions")?;
    let canonicality_summary = serde_json::to_string(&row.canonicality_summary)
        .context("failed to serialize permissions_current canonicality_summary")?;

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

fn json_text_field(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("permissions_current scope_detail must include {field}"))
}

#[cfg(test)]
mod tests {
    use std::{
        cmp::Ordering,
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    use crate::{CanonicalityState, Resource, default_database_url, upsert_resources};
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

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
                .context("failed to parse database URL for permissions_current tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, AtomicOrdering::Relaxed);
            let database_name = format!("bg_perm_{}_{unique:x}_{sequence:x}", std::process::id());

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for permissions_current tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect permissions_current test pool")?;

            crate::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for permissions_current tests")?;

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

    fn resource(resource_id: Uuid, block_hash: &str, block_number: i64) -> Resource {
        Resource {
            resource_id,
            token_lineage_id: None,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "permissions_current_test", "anchor": "resource"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    async fn seed_resources(database: &TestDatabase, resource_ids: &[Uuid]) -> Result<()> {
        let resources = resource_ids
            .iter()
            .enumerate()
            .map(|(index, resource_id)| {
                resource(
                    *resource_id,
                    &format!("0xresource{:02x}", index),
                    21_000_100 + index as i64,
                )
            })
            .collect::<Vec<_>>();
        upsert_resources(database.pool(), &resources).await?;
        Ok(())
    }

    fn permissions_current_row(
        resource_id: Uuid,
        subject: &str,
        scope: PermissionScope,
        manifest_version: i64,
    ) -> PermissionsCurrentRow {
        PermissionsCurrentRow {
            resource_id,
            subject: subject.to_owned(),
            scope,
            effective_powers: json!(["set_records", "set_resolver"]),
            grant_source: json!({
                "kind": "normalized_event",
                "normalized_event_id": 701
            }),
            revocation_source: None,
            inheritance_path: json!([
                {
                    "kind": "resource_authority",
                    "resource_id": resource_id
                }
            ]),
            transfer_behavior: json!({
                "kind": "resource_rebound"
            }),
            provenance: json!({
                "normalized_event_ids": [701, 702],
                "derivation_kind": "permissions_current_rebuild"
            }),
            coverage: json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "enumeration_basis": "resource_permissions"
            }),
            chain_positions: json!({
                "ethereum-mainnet": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_111,
                    "block_hash": "0xpermissions",
                    "timestamp": "2026-04-17T00:01:51Z"
                }
            }),
            canonicality_summary: json!({
                "status": "finalized",
                "chains": {
                    "ethereum-mainnet": "finalized"
                }
            }),
            manifest_version,
            last_recomputed_at: timestamp(1_776_000_111),
        }
    }

    #[tokio::test]
    async fn permissions_current_upserts_and_loads_resource_and_resolver_rows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x6100);
        seed_resources(&database, &[resource_id]).await?;

        let resource_scope = permissions_current_row(
            resource_id,
            "0x0000000000000000000000000000000000000abc",
            PermissionScope::Resource,
            3,
        );
        let resolver_scope = permissions_current_row(
            resource_id,
            "0x0000000000000000000000000000000000000abc",
            PermissionScope::Resolver {
                chain_id: "ethereum-mainnet".to_owned(),
                resolver_address: "0x0000000000000000000000000000000000000def".to_owned(),
            },
            3,
        );

        let inserted = upsert_permissions_current_rows(
            database.pool(),
            &[resource_scope.clone(), resolver_scope.clone()],
        )
        .await?;
        let expected = vec![resource_scope.clone(), resolver_scope.clone()];
        assert_eq!(inserted, expected);

        let loaded = load_permissions_current(database.pool(), resource_id, None, None).await?;
        let mut expected_sorted = expected;
        expected_sorted.sort_by(compare_permissions_sort_key);
        assert_eq!(loaded, expected_sorted);

        database.cleanup().await
    }

    #[tokio::test]
    async fn permissions_current_upsert_replaces_existing_keyed_row() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x6200);
        seed_resources(&database, &[resource_id]).await?;

        let first = permissions_current_row(
            resource_id,
            "0x0000000000000000000000000000000000000abc",
            PermissionScope::Resolver {
                chain_id: "ethereum-mainnet".to_owned(),
                resolver_address: "0x0000000000000000000000000000000000000def".to_owned(),
            },
            3,
        );
        upsert_permissions_current_rows(database.pool(), std::slice::from_ref(&first)).await?;

        let mut replacement = first.clone();
        replacement.effective_powers = json!(["set_resolver"]);
        replacement.revocation_source = Some(json!({
            "kind": "normalized_event",
            "normalized_event_id": 799
        }));
        replacement.manifest_version = 4;

        let updated =
            upsert_permissions_current_rows(database.pool(), std::slice::from_ref(&replacement))
                .await?;
        assert_eq!(updated, vec![replacement.clone()]);
        assert_eq!(
            load_permissions_current(database.pool(), resource_id, None, None).await?,
            vec![replacement]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn permissions_current_filters_subject_scope_and_resource_boundaries() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x6300);
        let other_resource_id = Uuid::from_u128(0x6301);
        seed_resources(&database, &[resource_id, other_resource_id]).await?;

        let shared_subject = "0x0000000000000000000000000000000000000abc";
        let resource_row =
            permissions_current_row(resource_id, shared_subject, PermissionScope::Resource, 3);
        let resolver_row = permissions_current_row(
            resource_id,
            shared_subject,
            PermissionScope::Resolver {
                chain_id: "ethereum-mainnet".to_owned(),
                resolver_address: "0x0000000000000000000000000000000000000def".to_owned(),
            },
            3,
        );
        let other_subject_row = permissions_current_row(
            resource_id,
            "0x0000000000000000000000000000000000000fed",
            PermissionScope::Resource,
            3,
        );
        let other_resource_row = permissions_current_row(
            other_resource_id,
            shared_subject,
            PermissionScope::Resolver {
                chain_id: "ethereum-mainnet".to_owned(),
                resolver_address: "0x0000000000000000000000000000000000000def".to_owned(),
            },
            3,
        );

        upsert_permissions_current_rows(
            database.pool(),
            &[
                resource_row.clone(),
                resolver_row.clone(),
                other_subject_row.clone(),
                other_resource_row.clone(),
            ],
        )
        .await?;

        assert_eq!(
            load_permissions_current(database.pool(), resource_id, Some(shared_subject), None)
                .await?,
            vec![resolver_row.clone(), resource_row.clone()]
        );
        assert_eq!(
            load_permissions_current(
                database.pool(),
                resource_id,
                None,
                Some(&PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000def".to_owned(),
                })
            )
            .await?,
            vec![resolver_row.clone()]
        );
        assert_eq!(
            load_permissions_current(database.pool(), resource_id, None, None).await?,
            vec![resolver_row.clone(), resource_row, other_subject_row]
        );
        assert_eq!(
            load_permissions_current_for_resolver_scope(
                database.pool(),
                "ethereum-mainnet",
                "0x0000000000000000000000000000000000000DEF",
            )
            .await?,
            vec![resolver_row.clone(), other_resource_row]
        );
        assert_eq!(
            load_permissions_current_resolver_targets(database.pool()).await?,
            vec![(
                "ethereum-mainnet".to_owned(),
                "0x0000000000000000000000000000000000000def".to_owned()
            )]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn permissions_current_delete_and_clear_support_rebuild_workflows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let first_resource_id = Uuid::from_u128(0x6400);
        let second_resource_id = Uuid::from_u128(0x6401);
        seed_resources(&database, &[first_resource_id, second_resource_id]).await?;

        let first = permissions_current_row(
            first_resource_id,
            "0x0000000000000000000000000000000000000abc",
            PermissionScope::Resource,
            3,
        );
        let second = permissions_current_row(
            second_resource_id,
            "0x0000000000000000000000000000000000000abc",
            PermissionScope::Resolver {
                chain_id: "ethereum-mainnet".to_owned(),
                resolver_address: "0x0000000000000000000000000000000000000def".to_owned(),
            },
            3,
        );

        upsert_permissions_current_rows(database.pool(), &[first.clone(), second.clone()]).await?;

        assert_eq!(
            delete_permissions_current(database.pool(), first_resource_id).await?,
            1
        );
        assert!(
            load_permissions_current(database.pool(), first_resource_id, None, None)
                .await?
                .is_empty()
        );
        assert_eq!(
            load_permissions_current(database.pool(), second_resource_id, None, None).await?,
            vec![second]
        );

        assert_eq!(clear_permissions_current(database.pool()).await?, 1);
        assert!(
            load_permissions_current(database.pool(), second_resource_id, None, None)
                .await?
                .is_empty()
        );

        database.cleanup().await
    }

    fn compare_permissions_sort_key(
        left: &PermissionsCurrentRow,
        right: &PermissionsCurrentRow,
    ) -> Ordering {
        left.subject
            .cmp(&right.subject)
            .then_with(|| left.scope.storage_key().cmp(&right.scope.storage_key()))
    }
}
