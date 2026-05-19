use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::default_database_url;

use super::*;

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
            .context("failed to parse database URL for resolver_current tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bn_st_resolver_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for resolver_current tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect resolver_current test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for resolver_current tests")?;

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

fn resolver_current_row(
    chain_id: &str,
    resolver_address: &str,
    manifest_version: i64,
) -> ResolverCurrentRow {
    ResolverCurrentRow {
        chain_id: chain_id.to_owned(),
        resolver_address: resolver_address.to_owned(),
        declared_summary: json!({
            "bindings": {
                "count": 2,
                "status": "supported"
            },
            "aliases": {
                "count": 1,
                "status": "supported"
            },
            "permissions": {
                "count": 3,
                "status": "supported"
            },
            "role_holders": {
                "count": 1,
                "status": "supported"
            },
            "event_summary": {
                "count": 5,
                "status": "supported"
            }
        }),
        provenance: json!({
            "normalized_event_ids": [801, 802, 803],
            "derivation_kind": "resolver_current_rebuild"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "enumeration_basis": "resolver_overview"
        }),
        chain_positions: json!({
            chain_id: {
                "chain_id": chain_id,
                "block_number": 21_100_001,
                "block_hash": "0xresolver",
                "timestamp": "2026-04-17T00:15:00Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized"
            }
        }),
        manifest_version,
        last_recomputed_at: timestamp(1_776_000_900),
    }
}

#[tokio::test]
async fn resolver_current_upserts_and_loads_by_key() -> Result<()> {
    let database = TestDatabase::new().await?;
    let expected = resolver_current_row(
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000ABC",
        5,
    );

    let inserted =
        upsert_resolver_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;
    let mut normalized_expected = expected.clone();
    normalized_expected.resolver_address = expected.resolver_address.to_ascii_lowercase();
    assert_eq!(inserted, vec![normalized_expected.clone()]);

    let loaded = load_resolver_current(
        database.pool(),
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000abc",
    )
    .await?;
    assert_eq!(loaded, Some(normalized_expected));

    database.cleanup().await
}

#[tokio::test]
async fn resolver_current_upsert_replaces_existing_projection_row() -> Result<()> {
    let database = TestDatabase::new().await?;
    let first = resolver_current_row(
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000def",
        5,
    );
    upsert_resolver_current_rows(database.pool(), std::slice::from_ref(&first)).await?;

    let mut replacement = first.clone();
    replacement.declared_summary = json!({
        "bindings": {
            "count": 4,
            "status": "supported"
        },
        "aliases": {
            "count": 2,
            "status": "supported"
        }
    });
    replacement.coverage = json!({
        "status": "partial",
        "unsupported_reason": "role_holders_pending"
    });
    replacement.manifest_version = 6;

    let updated =
        upsert_resolver_current_rows(database.pool(), std::slice::from_ref(&replacement)).await?;
    let mut normalized_replacement = replacement.clone();
    normalized_replacement.resolver_address = replacement.resolver_address.to_ascii_lowercase();
    assert_eq!(updated, vec![normalized_replacement.clone()]);
    assert_eq!(
        load_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000DEF",
        )
        .await?,
        Some(normalized_replacement)
    );

    database.cleanup().await
}

#[tokio::test]
async fn resolver_current_bulk_upsert_preserves_order_with_duplicate_keys() -> Result<()> {
    let database = TestDatabase::new().await?;
    let first = resolver_current_row(
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000ABC",
        5,
    );
    let other = resolver_current_row(
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000def",
        6,
    );
    let mut replacement = first.clone();
    replacement.resolver_address = replacement.resolver_address.to_ascii_lowercase();
    replacement.declared_summary = json!({
        "bindings": {
            "count": 8,
            "status": "supported"
        },
        "aliases": {
            "count": 3,
            "status": "supported"
        }
    });
    replacement.manifest_version = 7;
    replacement.last_recomputed_at = timestamp(1_776_001_200);

    let snapshots = upsert_resolver_current_rows(
        database.pool(),
        &[first.clone(), other.clone(), replacement.clone()],
    )
    .await?;

    let mut normalized_first = first.clone();
    normalized_first.resolver_address = first.resolver_address.to_ascii_lowercase();
    let mut normalized_other = other.clone();
    normalized_other.resolver_address = other.resolver_address.to_ascii_lowercase();
    assert_eq!(
        snapshots,
        vec![
            normalized_first,
            normalized_other.clone(),
            replacement.clone()
        ]
    );
    assert_eq!(
        load_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000ABC",
        )
        .await?,
        Some(replacement)
    );
    assert_eq!(
        load_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000def",
        )
        .await?,
        Some(normalized_other)
    );

    database.cleanup().await
}

#[tokio::test]
async fn resolver_current_bulk_upsert_rejects_invalid_slice_without_partial_write() -> Result<()> {
    let database = TestDatabase::new().await?;
    let valid = resolver_current_row(
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000aaa",
        5,
    );
    let mut invalid = resolver_current_row(
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000bbb",
        0,
    );
    invalid.manifest_version = 0;

    let error = upsert_resolver_current_rows(database.pool(), &[valid.clone(), invalid])
        .await
        .expect_err("invalid resolver_current input should fail");
    assert!(
        format!("{error:#}").contains("non-positive manifest_version 0"),
        "unexpected error: {error:#}"
    );
    assert_eq!(
        load_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000aaa",
        )
        .await?,
        None
    );

    database.cleanup().await
}

#[tokio::test]
async fn resolver_current_delete_and_clear_support_rebuild_workflows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let first = resolver_current_row(
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000101",
        5,
    );
    let second = resolver_current_row(
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000102",
        5,
    );

    upsert_resolver_current_rows(database.pool(), &[first.clone(), second.clone()]).await?;

    assert_eq!(
        delete_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000101",
        )
        .await?,
        1
    );
    assert_eq!(
        load_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000101",
        )
        .await?,
        None
    );

    let mut normalized_second = second.clone();
    normalized_second.resolver_address = second.resolver_address.to_ascii_lowercase();
    assert_eq!(
        load_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000102",
        )
        .await?,
        Some(normalized_second)
    );

    assert_eq!(clear_resolver_current(database.pool()).await?, 1);
    assert_eq!(
        load_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000102",
        )
        .await?,
        None
    );

    database.cleanup().await
}
