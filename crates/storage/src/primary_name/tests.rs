use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use super::*;
use crate::default_database_url;

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
            .context("failed to parse database URL for primary_names_current tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("bn_spn_{}_{}_{}", std::process::id(), unique, sequence);

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for primary_names_current tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect primary_names_current test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for primary_names_current tests")?;

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

#[tokio::test]
async fn upsert_and_load_round_trip_exact_tuple() -> Result<()> {
    let database = TestDatabase::new().await?;

    let row = PrimaryNameCurrentRow {
        address: "0x0000000000000000000000000000000000000ABC".to_owned(),
        namespace: "ens".to_owned(),
        coin_type: "60".to_owned(),
        claim_status: PrimaryNameClaimStatus::NotFound,
        raw_claim_name: None,
        claim_provenance: serde_json::json!({
            "source_family": "ens_v1_reverse_l1",
            "contract_role": "reverse_registrar",
        }),
    };

    let inserted = upsert_primary_name_current_rows(database.pool(), &[row]).await?;
    assert_eq!(
        inserted,
        vec![PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::NotFound,
            raw_claim_name: None,
            claim_provenance: serde_json::json!({
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
            }),
        }]
    );

    let loaded = load_primary_name_current(
        database.pool(),
        "0x0000000000000000000000000000000000000abc",
        "ens",
        "60",
    )
    .await?;
    assert_eq!(loaded, inserted.into_iter().next());

    database.cleanup().await
}

#[tokio::test]
async fn delete_and_clear_remove_rows() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_primary_name_current_rows(
        database.pool(),
        &[
            PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000abc".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: serde_json::json!({
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                }),
            },
            PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000def".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Unsupported,
                raw_claim_name: None,
                claim_provenance: serde_json::json!({}),
            },
        ],
    )
    .await?;

    let deleted = delete_primary_name_current(
        database.pool(),
        "0x0000000000000000000000000000000000000ABC",
        "ens",
        "60",
    )
    .await?;
    assert_eq!(deleted, 1);
    assert!(
        load_primary_name_current(
            database.pool(),
            "0x0000000000000000000000000000000000000abc",
            "ens",
            "60",
        )
        .await?
        .is_none()
    );

    let cleared = clear_primary_names_current(database.pool()).await?;
    assert_eq!(cleared, 1);
    assert!(
        load_primary_name_current(
            database.pool(),
            "0x0000000000000000000000000000000000000def",
            "ens",
            "60",
        )
        .await?
        .is_none()
    );

    database.cleanup().await
}

#[tokio::test]
async fn round_trips_invalid_name_rows_with_raw_claim_input() -> Result<()> {
    let database = TestDatabase::new().await?;

    let row = PrimaryNameCurrentRow {
        address: "0x0000000000000000000000000000000000000abc".to_owned(),
        namespace: "ens".to_owned(),
        coin_type: "60".to_owned(),
        claim_status: PrimaryNameClaimStatus::InvalidName,
        raw_claim_name: Some("alice..eth".to_owned()),
        claim_provenance: serde_json::json!({
            "source_family": "ens_v1_resolver_l1",
            "contract_role": "resolver",
            "contract_instance_id": "00000000-0000-0000-0000-000000000123",
            "emitting_address": "0x0000000000000000000000000000000000000fed",
        }),
    };

    let inserted =
        upsert_primary_name_current_rows(database.pool(), std::slice::from_ref(&row)).await?;
    assert_eq!(inserted, vec![row.clone()]);

    let loaded = load_primary_name_current(
        database.pool(),
        "0x0000000000000000000000000000000000000ABC",
        "ens",
        "60",
    )
    .await?;
    assert_eq!(loaded, Some(row));

    database.cleanup().await
}

#[tokio::test]
async fn round_trips_primary_name_snapshots_with_normalized_claim_name() -> Result<()> {
    let database = TestDatabase::new().await?;

    let snapshot = PrimaryNameCurrentSnapshot {
        row: PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: serde_json::json!({
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
            }),
        },
        normalized_claim_name: Some("alice.eth".to_owned()),
    };

    let inserted =
        upsert_primary_name_current_snapshots(database.pool(), std::slice::from_ref(&snapshot))
            .await?;
    assert_eq!(inserted, vec![snapshot.clone()]);

    assert_eq!(
        load_primary_name_current_snapshot(
            database.pool(),
            "0x0000000000000000000000000000000000000abc",
            "ens",
            "60",
        )
        .await?,
        Some(snapshot)
    );

    database.cleanup().await
}

#[tokio::test]
async fn batch_upsert_preserves_input_order_after_sorted_locking() -> Result<()> {
    let database = TestDatabase::new().await?;

    let second_address_snapshot = PrimaryNameCurrentSnapshot {
        row: PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000def".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: serde_json::json!({"source": "primary_name_order_test"}),
        },
        normalized_claim_name: Some("zeta.eth".to_owned()),
    };
    let first_address_snapshot = PrimaryNameCurrentSnapshot {
        row: PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: serde_json::json!({"source": "primary_name_order_test"}),
        },
        normalized_claim_name: Some("alpha.eth".to_owned()),
    };

    let input = vec![
        second_address_snapshot.clone(),
        first_address_snapshot.clone(),
    ];
    let inserted = upsert_primary_name_current_snapshots(database.pool(), &input).await?;
    assert_eq!(inserted, input);

    database.cleanup().await
}

#[tokio::test]
async fn rejects_non_normalized_claim_name_sources() -> Result<()> {
    let database = TestDatabase::new().await?;

    let error = upsert_primary_name_current_snapshots(
        database.pool(),
        &[PrimaryNameCurrentSnapshot {
            row: PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000abc".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: serde_json::json!({}),
            },
            normalized_claim_name: Some("Alice.eth".to_owned()),
        }],
    )
    .await
    .expect_err("non-normalized claimed names must be rejected");

    assert!(
        error
            .to_string()
            .contains("must already be ENSIP-15-normalized")
    );

    database.cleanup().await
}

#[test]
fn verified_primary_name_claim_hooks_reads_persisted_hook_material() -> Result<()> {
    let row = PrimaryNameCurrentRow {
        address: "0x0000000000000000000000000000000000000abc".to_owned(),
        namespace: "ens".to_owned(),
        coin_type: "60".to_owned(),
        claim_status: PrimaryNameClaimStatus::InvalidName,
        raw_claim_name: Some("alice..eth".to_owned()),
        claim_provenance: serde_json::json!({
            "source_family": "ens_v1_reverse_l1",
            "contract_role": "reverse_registrar",
            VERIFIED_PRIMARY_NAME_LOOKUP_KEY: {
                "address": "0x0000000000000000000000000000000000000abc",
                "namespace": "ens",
                "coin_type": "60",
            },
            VERIFIED_PRIMARY_NAME_INVALIDATION_KEY: {
                "claim_status": "invalid_name",
                "primary_claim_source": {
                    "address": "0x0000000000000000000000000000000000000abc",
                    "namespace": "ens",
                    "coin_type": "60",
                    "reverse_name": "0000000000000000000000000000000000000abc.addr.reverse",
                    "reverse_node": "0x000000000000000000000000000000000000000000000000000000000000012e",
                    "claim_provenance": {
                        "source_family": "ens_v1_reverse_l1",
                        "contract_role": "reverse_registrar",
                    },
                },
            },
        }),
    };

    let hooks = verified_primary_name_claim_hooks(&row)?;
    assert_eq!(
        hooks.lookup,
        VerifiedPrimaryNameLookupHook {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
        }
    );
    assert_eq!(
        hooks.lookup.request_key(),
        "ens:0x0000000000000000000000000000000000000abc:60"
    );
    assert_eq!(
        hooks.invalidation,
        VerifiedPrimaryNameInvalidationHook {
            claim_status: PrimaryNameClaimStatus::InvalidName,
            reverse_claim_provenance: serde_json::json!({
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
            }),
            primary_claim_source: Some(serde_json::json!({
                "address": "0x0000000000000000000000000000000000000abc",
                "namespace": "ens",
                "coin_type": "60",
                "reverse_name": "0000000000000000000000000000000000000abc.addr.reverse",
                "reverse_node": "0x000000000000000000000000000000000000000000000000000000000000012e",
                "claim_provenance": {
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                },
            })),
        }
    );

    Ok(())
}

#[test]
fn verified_primary_name_claim_hooks_fall_back_to_row_tuple_without_nested_hooks() -> Result<()> {
    let row = PrimaryNameCurrentRow {
        address: "0x0000000000000000000000000000000000000abc".to_owned(),
        namespace: "ens".to_owned(),
        coin_type: "60".to_owned(),
        claim_status: PrimaryNameClaimStatus::NotFound,
        raw_claim_name: None,
        claim_provenance: serde_json::json!({
            "source_family": "ens_v1_reverse_l1",
            "contract_role": "reverse_registrar",
        }),
    };

    let hooks = verified_primary_name_claim_hooks(&row)?;
    assert_eq!(
        hooks.lookup.request_key(),
        "ens:0x0000000000000000000000000000000000000abc:60"
    );
    assert_eq!(
        hooks.invalidation,
        VerifiedPrimaryNameInvalidationHook {
            claim_status: PrimaryNameClaimStatus::NotFound,
            reverse_claim_provenance: serde_json::json!({
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
            }),
            primary_claim_source: None,
        }
    );

    Ok(())
}

#[tokio::test]
async fn rejects_raw_claim_name_outside_invalid_name_status() -> Result<()> {
    let database = TestDatabase::new().await?;

    let error = upsert_primary_name_current_rows(
        database.pool(),
        &[PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: Some("alice.eth".to_owned()),
            claim_provenance: serde_json::json!({}),
        }],
    )
    .await
    .expect_err("success rows must reject raw_claim_name");

    assert!(
        error
            .to_string()
            .contains("raw_claim_name only for claim_status invalid_name")
    );

    database.cleanup().await
}
