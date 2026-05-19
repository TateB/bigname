use std::str::FromStr;

use anyhow::{Context, Result};
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use super::*;
use crate::{CanonicalityState, default_database_url};

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
            .context("failed to parse database URL for raw payload cache tests")?;
        let database_name = format!(
            "bn_st_payload_{}_{}",
            std::process::id(),
            Uuid::new_v4().simple()
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for raw payload cache tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect raw payload cache test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for raw payload cache tests")?;

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

fn metadata(state: CanonicalityState) -> RawPayloadCacheMetadataUpsert {
    RawPayloadCacheMetadataUpsert {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: "0xaaa".to_owned(),
        payload_kind: "full_block".to_owned(),
        digest_algorithm: Some("SHA256".to_owned()),
        retained_digest: Some("ABCDEF".to_owned()),
        block_number: Some(101),
        payload_size_bytes: 42,
        content_type: Some("application/json".to_owned()),
        content_encoding: Some("identity".to_owned()),
        cache_metadata: json!({
            "source": "json-rpc",
            "fetch_mode": "block_hash"
        }),
        canonicality_state: state,
    }
}

fn verification(retained_digest: &str, size: i64) -> RawPayloadCacheDigestVerification {
    RawPayloadCacheDigestVerification {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: "0xaaa".to_owned(),
        payload_kind: "full_block".to_owned(),
        digest_algorithm: "SHA256".to_owned(),
        candidate_digest: retained_digest.to_owned(),
        payload_size_bytes: size,
    }
}

#[tokio::test]
async fn upserts_loads_and_verifies_payload_cache_metadata() -> Result<()> {
    let database = TestDatabase::new().await?;

    let inserted = upsert_raw_payload_cache_metadata(
        database.pool(),
        &[metadata(CanonicalityState::Canonical)],
    )
    .await?;
    assert_eq!(inserted.len(), 1);
    assert_eq!(inserted[0].digest_algorithm.as_deref(), Some("sha256"));
    assert_eq!(inserted[0].retained_digest.as_deref(), Some("abcdef"));
    assert_eq!(inserted[0].block_number, Some(101));
    assert_eq!(inserted[0].payload_size_bytes, 42);

    let promoted = upsert_raw_payload_cache_metadata(
        database.pool(),
        &[metadata(CanonicalityState::Finalized)],
    )
    .await?;
    assert_eq!(promoted[0].canonicality_state, CanonicalityState::Finalized);
    assert_eq!(
        promoted[0].raw_payload_cache_metadata_id,
        inserted[0].raw_payload_cache_metadata_id
    );
    assert!(promoted[0].last_observed_at >= inserted[0].last_observed_at);

    let loaded = load_raw_payload_cache_metadata(
        database.pool(),
        "eth-mainnet",
        "0xaaa",
        "full_block",
        Some("sha256"),
        Some("abcdef"),
    )
    .await?;
    assert_eq!(loaded, Some(promoted[0].clone()));

    let listed =
        list_raw_payload_cache_metadata_by_block_hash(database.pool(), "eth-mainnet", "0xaaa")
            .await?;
    assert_eq!(listed, vec![promoted[0].clone()]);

    let verified =
        verify_raw_payload_cache_digest(database.pool(), &verification("ABCDEF", 42)).await?;
    assert_eq!(verified, promoted[0]);

    database.cleanup().await
}

#[tokio::test]
async fn rejects_mismatched_payload_cache_metadata_identity() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_payload_cache_metadata(database.pool(), &[metadata(CanonicalityState::Canonical)])
        .await?;

    let mut conflicting = metadata(CanonicalityState::Observed);
    conflicting.payload_size_bytes = 43;
    let error = upsert_raw_payload_cache_metadata(database.pool(), &[conflicting])
        .await
        .expect_err("payload cache metadata identity mismatch must fail");
    assert!(
        error.to_string().contains(
            "raw payload cache metadata identity mismatch for chain eth-mainnet block 0xaaa payload kind full_block"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn digest_verification_fails_closed() -> Result<()> {
    let database = TestDatabase::new().await?;

    let mut without_digest = metadata(CanonicalityState::Canonical);
    without_digest.block_hash = "0xnodigest".to_owned();
    without_digest.digest_algorithm = None;
    without_digest.retained_digest = None;
    upsert_raw_payload_cache_metadata(database.pool(), &[without_digest]).await?;

    let mut missing_digest_check = verification("abcdef", 42);
    missing_digest_check.block_hash = "0xnodigest".to_owned();
    let error = verify_raw_payload_cache_digest(database.pool(), &missing_digest_check)
        .await
        .expect_err("metadata without retained digest must fail closed");
    assert!(
        error.to_string().contains("has no retained digest"),
        "unexpected error: {error:#}"
    );

    upsert_raw_payload_cache_metadata(database.pool(), &[metadata(CanonicalityState::Canonical)])
        .await?;

    let error = verify_raw_payload_cache_digest(database.pool(), &verification("bbbbbb", 42))
        .await
        .expect_err("mismatched digest must fail closed");
    assert!(
        error
            .to_string()
            .contains("raw payload cache digest mismatch"),
        "unexpected error: {error:#}"
    );

    let error = verify_raw_payload_cache_digest(database.pool(), &verification("abcdef", 41))
        .await
        .expect_err("mismatched payload size must fail closed");
    assert!(
        error
            .to_string()
            .contains("raw payload cache payload size mismatch"),
        "unexpected error: {error:#}"
    );

    let mut wrong_identity = verification("abcdef", 42);
    wrong_identity.block_hash = "0xmissing".to_owned();
    let error = verify_raw_payload_cache_digest(database.pool(), &wrong_identity)
        .await
        .expect_err("mismatched payload identity must fail closed");
    assert!(
        error
            .to_string()
            .contains("raw payload cache identity mismatch"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}
