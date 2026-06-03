use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};
use uuid::Uuid;

use super::*;
use crate::{
    CanonicalityState, ChainPositions, Resource, SnapshotProjectionRead,
    SnapshotSelectionErrorKind, default_database_url, upsert_resources,
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

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
            .context("failed to parse database URL for record_inventory_current tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("bg_rec_inv_{}_{unique:x}_{sequence:x}", std::process::id());

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for record_inventory_current tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect record_inventory_current test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for record_inventory_current tests")?;

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
        provenance: json!({
            "source": "record_inventory_current_test",
            "anchor": "resource"
        }),
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
                &format!("0xrecordinventory{:02x}", index),
                21_000_300 + index as i64,
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

async fn seed_chain_lineage(
    database: &TestDatabase,
    block_hash: &str,
    block_number: i64,
    timestamp: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES (
            'ethereum-mainnet',
            $1,
            $2,
            $3::TIMESTAMPTZ,
            'finalized'::canonicality_state
        )
        "#,
    )
    .bind(block_hash)
    .bind(block_number)
    .bind(timestamp)
    .execute(database.pool())
    .await?;
    Ok(())
}

async fn seed_record_inventory_input(
    database: &TestDatabase,
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    block_number: i64,
    block_hash: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            block_number,
            block_hash,
            derivation_kind,
            canonicality_state,
            after_state
        )
        VALUES (
            $1,
            'ens',
            $2,
            $3,
            'RecordChanged',
            'ens_v1_resolver_l1',
            1,
            'ethereum-mainnet',
            $4,
            $5,
            'record_inventory_current_test',
            'finalized'::canonicality_state,
            '{"record_key":"addr:60","record_family":"addr","selector_key":"60"}'::JSONB
        )
        "#,
    )
    .bind(event_identity)
    .bind(logical_name_id)
    .bind(resource_id)
    .bind(block_number)
    .bind(block_hash)
    .execute(database.pool())
    .await?;
    Ok(())
}

fn record_version_boundary(
    resource_id: Uuid,
    logical_name_id: &str,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
    block_number: i64,
    block_hash: &str,
) -> Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": "2026-04-18T00:15:00Z"
        }
    })
}

fn last_change(normalized_event_id: i64, event_kind: &str, block_number: i64) -> Value {
    json!({
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "block_hash": format!("0xlastchange{block_number:x}"),
            "timestamp": "2026-04-18T00:20:00Z"
        }
    })
}

fn record_inventory_current_row(
    resource_id: Uuid,
    logical_name_id: &str,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
    block_number: i64,
    block_hash: &str,
    manifest_version: i64,
) -> RecordInventoryCurrentRow {
    RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: record_version_boundary(
            resource_id,
            logical_name_id,
            normalized_event_id,
            event_kind,
            block_number,
            block_hash,
        ),
        enumeration_basis: json!({
            "observed_selectors": true,
            "capability_declared_families": true,
            "globally_enumerable": false
        }),
        selectors: json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true
            },
            {
                "record_key": "avatar",
                "record_family": "avatar",
                "selector_key": null,
                "cacheable": true
            },
            {
                "record_key": "text:com.twitter",
                "record_family": "text",
                "selector_key": "com.twitter",
                "cacheable": false
            }
        ]),
        explicit_gaps: json!([
            {
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "gap_reason": "not_observed_on_current_resolver"
            }
        ]),
        unsupported_families: json!([
            {
                "record_family": "abi",
                "unsupported_reason": "resolver_family_pending"
            },
            {
                "record_family": "pubkey",
                "unsupported_reason": "resolver_family_pending"
            }
        ]),
        last_change: Some(last_change(
            normalized_event_id.unwrap_or(1_200),
            event_kind.unwrap_or("RecordsChanged"),
            block_number,
        )),
        entries: json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x0000000000000000000000000000000000000abc"
                }
            },
            {
                "record_key": "avatar",
                "record_family": "avatar",
                "selector_key": null,
                "status": "unsupported",
                "unsupported_reason": "resolver_family_pending"
            }
        ]),
        provenance: json!({
            "normalized_event_ids": [normalized_event_id.unwrap_or(1200)],
            "derivation_kind": "record_inventory_current_rebuild"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "enumeration_basis": "declared_record_inventory"
        }),
        chain_positions: json!({
            "ethereum-mainnet": {
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "block_hash": block_hash,
                "timestamp": "2026-04-18T00:15:00Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version,
        last_recomputed_at: timestamp(1_776_100_500),
    }
}

#[tokio::test]
async fn record_inventory_current_migration_creates_projection_table() -> Result<()> {
    let database = TestDatabase::new().await?;

    let table_name: Option<String> = sqlx::query_scalar(
        r#"
        SELECT to_regclass('public.record_inventory_current')::TEXT
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(table_name.as_deref(), Some("record_inventory_current"));

    let columns = sqlx::query_scalar::<_, String>(
        r#"
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'record_inventory_current'
        ORDER BY ordinal_position
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert!(columns.contains(&"record_version_boundary_key".to_owned()));
    assert!(columns.contains(&"record_version_boundary".to_owned()));
    assert!(columns.contains(&"entries".to_owned()));

    database.cleanup().await
}

#[tokio::test]
async fn record_inventory_current_upserts_and_loads_by_exact_key() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7100);
    seed_resources(&database, &[resource_id]).await?;

    let expected = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        Some(901),
        Some("RecordsChanged"),
        21_500_001,
        "0xrecordinventorya",
        4,
    );

    let inserted =
        upsert_record_inventory_current_rows(database.pool(), std::slice::from_ref(&expected))
            .await?;
    assert_eq!(inserted, vec![expected.clone()]);

    let loaded = load_record_inventory_current(
        database.pool(),
        resource_id,
        &expected.record_version_boundary,
    )
    .await?;
    assert_eq!(loaded, Some(expected));

    database.cleanup().await
}

#[tokio::test]
async fn record_inventory_snapshot_read_fails_stale_on_position_mismatch() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7110);
    seed_resources(&database, &[resource_id]).await?;

    let expected = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        Some(921),
        Some("RecordsChanged"),
        21_500_021,
        "0xrecordinventorysnapshot",
        4,
    );

    upsert_record_inventory_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;

    let selected = ChainPositions::from_value(&expected.chain_positions)?;
    assert_eq!(
        load_record_inventory_current_for_snapshot(
            database.pool(),
            resource_id,
            &expected.record_version_boundary,
            &selected,
        )
        .await?,
        SnapshotProjectionRead::Found(expected.clone())
    );

    let stale_selected = ChainPositions::from_value(&json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_500_022,
            "block_hash": "0xrecordinventorynewer",
            "timestamp": "2026-04-18T00:15:01Z"
        }
    }))?;
    let error = load_record_inventory_current_for_snapshot(
        database.pool(),
        resource_id,
        &expected.record_version_boundary,
        &stale_selected,
    )
    .await
    .expect_err("mismatched selected snapshot must be stale");
    assert_eq!(error.kind(), SnapshotSelectionErrorKind::Stale);

    database.cleanup().await
}

#[tokio::test]
async fn record_inventory_snapshot_read_allows_projected_auxiliary_chain_positions() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7115);
    seed_resources(&database, &[resource_id]).await?;

    let mut expected = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        Some(923),
        Some("RecordsChanged"),
        21_500_021,
        "0xrecordinventorysnapshot",
        4,
    );
    expected
        .chain_positions
        .as_object_mut()
        .expect("test chain_positions must be an object")
        .insert(
            "base".to_owned(),
            json!({
                "chain_id": "base-mainnet",
                "block_number": 31_500_021,
                "block_hash": "0xbaseinventorysnapshot",
                "timestamp": "2026-04-18T00:15:00Z"
            }),
        );

    upsert_record_inventory_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;

    let selected = ChainPositions::from_value(&json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_500_021,
            "block_hash": "0xrecordinventorysnapshot",
            "timestamp": "2026-04-18T00:15:00Z"
        }
    }))?;
    assert_eq!(
        load_record_inventory_current_for_snapshot(
            database.pool(),
            resource_id,
            &expected.record_version_boundary,
            &selected,
        )
        .await?,
        SnapshotProjectionRead::Found(expected)
    );

    database.cleanup().await
}

#[tokio::test]
async fn record_inventory_snapshot_read_covers_later_snapshot_until_new_input() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7111);
    let logical_name_id = "ens:alice.eth";
    seed_resources(&database, &[resource_id]).await?;

    let expected = record_inventory_current_row(
        resource_id,
        logical_name_id,
        Some(922),
        Some("RecordsChanged"),
        21_500_021,
        "0xrecordinventorysnapshot",
        4,
    );

    upsert_record_inventory_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;
    seed_chain_lineage(
        &database,
        "0xrecordinventorysnapshot",
        21_500_021,
        "2026-04-18T00:15:00Z",
    )
    .await?;
    seed_chain_lineage(
        &database,
        "0xrecordinventorynewer",
        21_500_022,
        "2026-04-18T00:15:01Z",
    )
    .await?;

    let later_selected = ChainPositions::from_value(&json!({
        "ethereum-mainnet": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_500_022,
            "block_hash": "0xrecordinventorynewer",
            "timestamp": "2026-04-18T00:15:01Z"
        }
    }))?;
    assert_eq!(
        load_record_inventory_current_for_snapshot(
            database.pool(),
            resource_id,
            &expected.record_version_boundary,
            &later_selected,
        )
        .await?,
        SnapshotProjectionRead::Found(expected.clone())
    );

    seed_record_inventory_input(
        &database,
        "record-inventory-newer-input",
        logical_name_id,
        resource_id,
        21_500_022,
        "0xrecordinventorynewer",
    )
    .await?;
    let error = load_record_inventory_current_for_snapshot(
        database.pool(),
        resource_id,
        &expected.record_version_boundary,
        &later_selected,
    )
    .await
    .expect_err("newer record inventory input must make the projection stale");
    assert_eq!(error.kind(), SnapshotSelectionErrorKind::Stale);

    database.cleanup().await
}

#[tokio::test]
async fn record_inventory_current_upsert_replaces_existing_projection_row() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7101);
    seed_resources(&database, &[resource_id]).await?;

    let first = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        Some(902),
        Some("RecordsChanged"),
        21_500_002,
        "0xrecordinventoryb",
        4,
    );
    upsert_record_inventory_current_rows(database.pool(), std::slice::from_ref(&first)).await?;

    let mut replacement = first.clone();
    replacement.enumeration_basis = json!({
        "observed_selectors": true,
        "capability_declared_families": false,
        "globally_enumerable": true
    });
    replacement.entries = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000def"
            }
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "status": "unsupported",
            "unsupported_reason": "resolver_family_pending"
        }
    ]);
    replacement.coverage = json!({
        "status": "partial",
        "unsupported_reason": "inventory_rebuild_in_progress"
    });
    replacement.manifest_version = 5;

    let updated =
        upsert_record_inventory_current_rows(database.pool(), std::slice::from_ref(&replacement))
            .await?;
    assert_eq!(updated, vec![replacement.clone()]);
    assert_eq!(
        load_record_inventory_current(
            database.pool(),
            resource_id,
            &replacement.record_version_boundary,
        )
        .await?,
        Some(replacement)
    );

    database.cleanup().await
}

#[tokio::test]
async fn record_inventory_current_excludes_orphaned_resources() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7108);
    seed_resources(&database, &[resource_id]).await?;

    let expected = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        Some(911),
        Some("RecordsChanged"),
        21_500_011,
        "0xrecordinventoryk",
        4,
    );
    upsert_record_inventory_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;

    orphan_resource(&database, resource_id).await?;

    assert_eq!(
        load_record_inventory_current(
            database.pool(),
            resource_id,
            &expected.record_version_boundary,
        )
        .await?,
        None
    );

    database.cleanup().await
}

#[tokio::test]
async fn record_inventory_current_delete_and_clear_support_rebuild_workflows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7102);
    seed_resources(&database, &[resource_id]).await?;

    let first = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        Some(903),
        Some("RecordsChanged"),
        21_500_003,
        "0xrecordinventoryc",
        4,
    );
    let second = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        Some(904),
        Some("RecordsChanged"),
        21_500_004,
        "0xrecordinventoryd",
        4,
    );

    let inserted =
        upsert_record_inventory_current_rows(database.pool(), &[first.clone(), second.clone()])
            .await?;
    assert_eq!(inserted, vec![first.clone(), second.clone()]);

    assert_eq!(
        delete_record_inventory_current(
            database.pool(),
            resource_id,
            &first.record_version_boundary
        )
        .await?,
        1
    );
    assert_eq!(
        load_record_inventory_current(database.pool(), resource_id, &first.record_version_boundary)
            .await?,
        None
    );
    assert_eq!(
        load_record_inventory_current(
            database.pool(),
            resource_id,
            &second.record_version_boundary
        )
        .await?,
        Some(second.clone())
    );

    assert_eq!(clear_record_inventory_current(database.pool()).await?, 1);
    assert_eq!(
        load_record_inventory_current(
            database.pool(),
            resource_id,
            &second.record_version_boundary
        )
        .await?,
        None
    );

    database.cleanup().await
}

#[tokio::test]
async fn record_inventory_current_bulk_upsert_preserves_duplicate_input_order() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7109);
    seed_resources(&database, &[resource_id]).await?;

    let first = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        Some(910),
        Some("RecordsChanged"),
        21_500_012,
        "0xrecordinventoryl",
        4,
    );
    let mut replacement = first.clone();
    replacement.coverage = json!({
        "status": "partial",
        "unsupported_reason": "inventory_rebuild_in_progress"
    });
    replacement.manifest_version = 5;
    replacement.last_recomputed_at = timestamp(1_776_100_600);

    let inserted = upsert_record_inventory_current_rows(
        database.pool(),
        &[first.clone(), replacement.clone()],
    )
    .await?;
    assert_eq!(inserted, vec![first, replacement.clone()]);
    assert_eq!(
        load_record_inventory_current(
            database.pool(),
            resource_id,
            &replacement.record_version_boundary,
        )
        .await?,
        Some(replacement)
    );

    database.cleanup().await
}

#[tokio::test]
async fn record_inventory_current_rejects_invalid_json_shapes() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7103);
    seed_resources(&database, &[resource_id]).await?;

    let mut invalid_boundary = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        Some(905),
        Some("RecordsChanged"),
        21_500_005,
        "0xrecordinventorye",
        4,
    );
    invalid_boundary.record_version_boundary = json!({
        "logical_name_id": "ens:alice.eth",
        "resource_id": Uuid::from_u128(0x9999).to_string(),
        "normalized_event_id": 905,
        "event_kind": "RecordsChanged",
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_500_005,
            "block_hash": "0xrecordinventorye",
            "timestamp": "2026-04-18T00:15:00Z"
        }
    });
    let error = upsert_record_inventory_current_rows(
        database.pool(),
        std::slice::from_ref(&invalid_boundary),
    )
    .await
    .expect_err("boundary resource mismatch must fail");
    let rendered = format!("{error:#}");
    assert!(rendered.contains("does not match storage key resource_id"));

    let mut invalid_entry = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        Some(906),
        Some("RecordsChanged"),
        21_500_006,
        "0xrecordinventoryf",
        4,
    );
    invalid_entry.entries = json!([
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "status": "unsupported"
        }
    ]);
    let error =
        upsert_record_inventory_current_rows(database.pool(), std::slice::from_ref(&invalid_entry))
            .await
            .expect_err("unsupported cache entry without reason must fail");
    let rendered = format!("{error:#}");
    assert!(rendered.contains("record_cache entry unsupported_reason"));

    database.cleanup().await
}

#[tokio::test]
async fn record_inventory_current_preserves_selector_and_cache_ordering() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7104);
    seed_resources(&database, &[resource_id]).await?;

    let expected = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        None,
        None,
        21_500_007,
        "0xrecordinventoryg",
        4,
    );
    upsert_record_inventory_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;

    let loaded = load_record_inventory_current(
        database.pool(),
        resource_id,
        &expected.record_version_boundary,
    )
    .await?
    .expect("row must exist");

    assert_eq!(loaded.selectors, expected.selectors);
    assert_eq!(loaded.explicit_gaps, expected.explicit_gaps);
    assert_eq!(loaded.entries, expected.entries);

    database.cleanup().await
}

#[tokio::test]
async fn record_inventory_current_rejects_missing_cacheable_entry_drift() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7105);
    seed_resources(&database, &[resource_id]).await?;

    let mut invalid = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        Some(907),
        Some("RecordsChanged"),
        21_500_008,
        "0xrecordinventoryh",
        4,
    );
    invalid.entries = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000abc"
            }
        }
    ]);

    let error =
        upsert_record_inventory_current_rows(database.pool(), std::slice::from_ref(&invalid))
            .await
            .expect_err("missing cacheable selector entry must fail");
    assert!(
        error
            .to_string()
            .contains("missing cacheable selectors [avatar]")
    );

    database.cleanup().await
}

#[tokio::test]
async fn record_inventory_current_rejects_extra_entry_drift() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7106);
    seed_resources(&database, &[resource_id]).await?;

    let mut invalid = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        Some(908),
        Some("RecordsChanged"),
        21_500_009,
        "0xrecordinventoryi",
        4,
    );
    invalid.entries = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000abc"
            }
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "status": "unsupported",
            "unsupported_reason": "resolver_family_pending"
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "status": "unsupported",
            "unsupported_reason": "not_observed_on_current_resolver"
        }
    ]);

    let error =
        upsert_record_inventory_current_rows(database.pool(), std::slice::from_ref(&invalid))
            .await
            .expect_err("extra selector outside cacheable selector space must fail");
    assert!(
        error
            .to_string()
            .contains("extra selectors outside cacheable selector space [contenthash]")
    );

    database.cleanup().await
}

#[tokio::test]
async fn record_inventory_current_rejects_unsorted_selector_arrays() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7107);
    seed_resources(&database, &[resource_id]).await?;

    let mut invalid = record_inventory_current_row(
        resource_id,
        "ens:alice.eth",
        Some(909),
        Some("RecordsChanged"),
        21_500_010,
        "0xrecordinventoryj",
        4,
    );
    invalid.selectors = json!([
        {
            "record_key": "text:com.twitter",
            "record_family": "text",
            "selector_key": "com.twitter",
            "cacheable": false
        },
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "cacheable": true
        }
    ]);

    let error =
        upsert_record_inventory_current_rows(database.pool(), std::slice::from_ref(&invalid))
            .await
            .expect_err("unsorted selectors must fail");
    assert!(
        error
            .to_string()
            .contains("selectors must be sorted by record_key ascending")
    );

    database.cleanup().await
}
