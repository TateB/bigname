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

async fn orphan_resource(database: &TestDatabase, resource_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE resources
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE resource_id = $1
        "#,
    )
    .bind(resource_id)
    .execute(database.pool())
    .await?;
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
        load_permissions_current(database.pool(), resource_id, Some(shared_subject), None).await?,
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

#[tokio::test]
async fn permissions_current_excludes_orphaned_resources_across_readers() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x6480);
    seed_resources(&database, &[resource_id]).await?;

    let resolver_row = permissions_current_row(
        resource_id,
        "0x0000000000000000000000000000000000000abc",
        PermissionScope::Resolver {
            chain_id: "ethereum-mainnet".to_owned(),
            resolver_address: "0x0000000000000000000000000000000000000def".to_owned(),
        },
        3,
    );
    upsert_permissions_current_rows(database.pool(), std::slice::from_ref(&resolver_row)).await?;

    orphan_resource(&database, resource_id).await?;

    assert!(
        load_permissions_current(database.pool(), resource_id, None, None)
            .await?
            .is_empty()
    );

    let page =
        load_permissions_current_page(database.pool(), resource_id, None, None, None, 10).await?;
    assert!(page.rows.is_empty());
    assert_eq!(page.summary.row_count, 0);
    assert!(page.summary.provenance.is_empty());
    assert_eq!(page.summary.coverage, None);
    assert!(page.summary.chain_positions.is_empty());
    assert!(page.summary.canonicality_summaries.is_empty());
    assert_eq!(page.summary.last_recomputed_at, None);

    let grouped = load_permissions_current_by_resource_ids(database.pool(), &[resource_id]).await?;
    assert!(
        grouped
            .get(&resource_id)
            .expect("requested resource id must be present in grouped output")
            .is_empty()
    );

    assert!(
        load_permissions_current_for_resolver_scope(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000def",
        )
        .await?
        .is_empty()
    );
    assert!(
        load_permissions_current_resolver_targets(database.pool())
            .await?
            .is_empty()
    );

    database.cleanup().await
}

#[tokio::test]
async fn permissions_current_keyset_page_uses_subject_scope_cursor_and_full_filter_summary()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x6500);
    let other_resource_id = Uuid::from_u128(0x6501);
    seed_resources(&database, &[resource_id, other_resource_id]).await?;

    let subject = "0x0000000000000000000000000000000000000aaa";
    let resolver_row = permissions_current_row(
        resource_id,
        subject,
        PermissionScope::Resolver {
            chain_id: "ethereum-mainnet".to_owned(),
            resolver_address: "0x0000000000000000000000000000000000000def".to_owned(),
        },
        3,
    );
    let resource_row = permissions_current_row(resource_id, subject, PermissionScope::Resource, 4);
    let mut later_subject_row = permissions_current_row(
        resource_id,
        "0x0000000000000000000000000000000000000bbb",
        PermissionScope::Resource,
        5,
    );
    later_subject_row.last_recomputed_at = timestamp(1_776_000_222);
    let other_resource_row =
        permissions_current_row(other_resource_id, subject, PermissionScope::Resource, 6);

    upsert_permissions_current_rows(
        database.pool(),
        &[
            resource_row.clone(),
            other_resource_row,
            later_subject_row.clone(),
            resolver_row.clone(),
        ],
    )
    .await?;

    let first_page =
        load_permissions_current_page(database.pool(), resource_id, None, None, None, 1).await?;
    assert_eq!(first_page.rows, vec![resolver_row.clone()]);
    assert_eq!(
        first_page.next_cursor,
        Some(PermissionsCurrentKeysetCursor::from(&resolver_row))
    );
    assert_eq!(first_page.summary.row_count, 3);
    assert_eq!(first_page.summary.provenance.len(), 3);
    assert_eq!(
        first_page.summary.coverage,
        Some(resolver_row.coverage.clone())
    );
    assert_eq!(first_page.summary.chain_positions.len(), 3);
    assert_eq!(first_page.summary.canonicality_summaries.len(), 3);
    assert_eq!(
        first_page.summary.last_recomputed_at,
        Some(later_subject_row.last_recomputed_at)
    );

    let second_page = load_permissions_current_page(
        database.pool(),
        resource_id,
        None,
        None,
        first_page.next_cursor.as_ref(),
        2,
    )
    .await?;
    assert_eq!(
        second_page.rows,
        vec![resource_row.clone(), later_subject_row]
    );
    assert_eq!(second_page.next_cursor, None);
    assert_eq!(second_page.summary.row_count, 3);

    let filtered_page = load_permissions_current_page(
        database.pool(),
        resource_id,
        Some(subject),
        Some(&PermissionScope::Resource),
        None,
        10,
    )
    .await?;
    assert_eq!(filtered_page.rows, vec![resource_row]);
    assert_eq!(filtered_page.next_cursor, None);
    assert_eq!(filtered_page.summary.row_count, 1);

    database.cleanup().await
}

#[tokio::test]
async fn permissions_current_batch_loader_groups_resource_rows_in_subject_scope_order() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let first_resource_id = Uuid::from_u128(0x6600);
    let second_resource_id = Uuid::from_u128(0x6601);
    let empty_resource_id = Uuid::from_u128(0x6602);
    seed_resources(
        &database,
        &[first_resource_id, second_resource_id, empty_resource_id],
    )
    .await?;

    let first_later = permissions_current_row(
        first_resource_id,
        "0x0000000000000000000000000000000000000bbb",
        PermissionScope::Resource,
        3,
    );
    let first_earlier = permissions_current_row(
        first_resource_id,
        "0x0000000000000000000000000000000000000aaa",
        PermissionScope::Resource,
        3,
    );
    let second = permissions_current_row(
        second_resource_id,
        "0x0000000000000000000000000000000000000ccc",
        PermissionScope::Resolver {
            chain_id: "ethereum-mainnet".to_owned(),
            resolver_address: "0x0000000000000000000000000000000000000def".to_owned(),
        },
        3,
    );

    upsert_permissions_current_rows(
        database.pool(),
        &[first_later.clone(), second.clone(), first_earlier.clone()],
    )
    .await?;

    let grouped = load_permissions_current_by_resource_ids(
        database.pool(),
        &[
            second_resource_id,
            first_resource_id,
            empty_resource_id,
            first_resource_id,
        ],
    )
    .await?;

    assert_eq!(grouped.len(), 3);
    assert_eq!(
        grouped.get(&first_resource_id),
        Some(&vec![first_earlier, first_later])
    );
    assert_eq!(grouped.get(&second_resource_id), Some(&vec![second]));
    assert_eq!(grouped.get(&empty_resource_id), Some(&Vec::new()));

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
