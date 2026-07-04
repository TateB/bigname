use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde_json::json;
use sqlx::types::time::OffsetDateTime;
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
            .context("failed to parse database URL for backfill job tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_storage_backfill_job_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context(
                "failed to connect admin pool for backfill job tests. Run DB-backed tests through ./scripts/test-db -- <cargo test command>, or set BIGNAME_TEST_DATABASE_URL for an already-running PostgreSQL server.",
            )?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect backfill job test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for backfill job tests")?;

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

fn backfill_job_create(idempotency_key: &str) -> BackfillJobCreate {
    BackfillJobCreate {
        deployment_profile: "mainnet".to_owned(),
        chain_id: "eth-mainnet".to_owned(),
        source_identity: json!({
            "source_family": "ens_v1_registry_l1",
            "watch_targets": ["0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e"]
        }),
        scan_mode: "logs".to_owned(),
        range_start_block_number: 100,
        range_end_block_number: 120,
        idempotency_key: idempotency_key.to_owned(),
        ranges: vec![
            BackfillRangeSpec {
                range_start_block_number: 100,
                range_end_block_number: 109,
            },
            BackfillRangeSpec {
                range_start_block_number: 110,
                range_end_block_number: 120,
            },
        ],
    }
}

fn lease_deadline() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + 300)
        .expect("lease deadline must be valid")
}

#[tokio::test]
async fn backfill_job_create_is_idempotent_and_rejects_range_widening() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = backfill_job_create("job-create-idempotent");

    let created = create_backfill_job(database.pool(), &request).await?;
    assert_eq!(created.job.status, BackfillLifecycleStatus::Pending);
    assert_eq!(created.job.range_start_block_number, 100);
    assert_eq!(created.job.range_end_block_number, 120);
    assert_eq!(created.ranges.len(), 2);
    assert_eq!(created.ranges[0].checkpoint_block_number, 99);
    assert_eq!(created.ranges[1].checkpoint_block_number, 109);

    let repeated = create_backfill_job(database.pool(), &request).await?;
    assert_eq!(repeated.job.backfill_job_id, created.job.backfill_job_id);
    assert_eq!(
        repeated
            .ranges
            .iter()
            .map(|range| range.backfill_range_id)
            .collect::<Vec<_>>(),
        created
            .ranges
            .iter()
            .map(|range| range.backfill_range_id)
            .collect::<Vec<_>>()
    );

    let mut widened = request.clone();
    widened.range_end_block_number = 121;
    widened.ranges[1].range_end_block_number = 121;
    let error = create_backfill_job(database.pool(), &widened)
        .await
        .expect_err("idempotent create must reject range widening");
    assert!(
        error
            .to_string()
            .contains("does not match requested immutable job identity"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn backfill_job_accepts_legacy_full_whole_active_with_compact_hash() -> Result<()> {
    let database = TestDatabase::new().await?;
    let selected_targets = vec![
        json!({
            "source_family": "basenames_base_registry",
            "contract_instance_id": "00000000-0000-0000-0000-000000000001",
            "address": "0x0000000000000000000000000000000000000001",
            "effective_from_block": 100,
            "effective_to_block": 120
        }),
        json!({
            "source_family": "basenames_base_registry",
            "contract_instance_id": "00000000-0000-0000-0000-000000000002",
            "address": "0x0000000000000000000000000000000000000002",
            "effective_from_block": 100,
            "effective_to_block": 120
        }),
    ];
    let legacy_full_source_identity_hash =
        "keccak256:0x1111111111111111111111111111111111111111111111111111111111111111";
    let mut request = backfill_job_create("job-create-compact-source-identity");
    request.source_identity = json!({
        "selector_kind": "whole_active_watched_chain",
        "source_family": null,
        "requested_watched_targets": [],
        "selected_targets": selected_targets.clone(),
        "backfill_provider": "coinbase_cdp_sql",
        "scan_mode": "coinbase_sql_hash_pinned_logs_v1",
        "coinbase_sql_plan_version": "base_logs_v2",
        "validation_provider_required": true,
        "coinbase_sql_validation_mode": "sample",
        "topic_filtering": "manifest_abi_topic0_union_v1",
        "coinbase_sql_topic_plan": {
            "topic0s_by_source_family": {
                "basenames_base_registry": ["0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"]
            },
            "event_signatures_by_source_family": {
                "basenames_base_registry": ["NewOwner(bytes32,bytes32,address)"]
            },
            "source_families_without_topics": []
        },
        "source_identity_hash": legacy_full_source_identity_hash,
    });

    let created = create_backfill_job(database.pool(), &request).await?;
    let selected_targets = request
        .source_identity
        .get("selected_targets")
        .and_then(serde_json::Value::as_array)
        .expect("test source identity has selected_targets");
    let compact_source_identity = |selected_targets: &[serde_json::Value], hash: &str| {
        json!({
            "selector_kind": "whole_active_watched_chain",
            "source_family": null,
            "requested_watched_targets": [],
            "selected_target_count": selected_targets.len(),
            "selected_targets_digest_algorithm": "keccak256",
            "selected_targets_digest": validate::selected_targets_digest(selected_targets),
            "selected_targets_sample": {
                "first": selected_targets.first(),
                "last": selected_targets.last(),
            },
            "source_identity_payload_format": "selected_targets_digest_v1",
            "backfill_provider": "coinbase_cdp_sql",
            "scan_mode": "coinbase_sql_hash_pinned_logs_v1",
            "coinbase_sql_plan_version": "base_logs_v2",
            "validation_provider_required": true,
            "coinbase_sql_validation_mode": "sample",
            "topic_filtering": "manifest_abi_topic0_union_v1",
            "coinbase_sql_topic_plan": {
                "topic0s_by_source_family": {
                    "basenames_base_registry": ["0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"]
                },
                "event_signatures_by_source_family": {
                    "basenames_base_registry": ["NewOwner(bytes32,bytes32,address)"]
                },
                "source_families_without_topics": []
            },
            "source_identity_hash": hash,
        })
    };
    let mut compact = request.clone();
    compact.source_identity = compact_source_identity(
        selected_targets,
        "keccak256:0x2222222222222222222222222222222222222222222222222222222222222222",
    );

    let repeated = create_backfill_job(database.pool(), &compact).await?;

    assert_eq!(repeated.job.backfill_job_id, created.job.backfill_job_id);
    assert_eq!(
        repeated.job.source_identity, request.source_identity,
        "existing full source identity must be reused without rewriting"
    );

    let mut different_targets = selected_targets.to_vec();
    *different_targets[1]
        .get_mut("effective_to_block")
        .expect("test target has effective_to_block") = json!(121);
    let mut different_compact = compact.clone();
    different_compact.source_identity = compact_source_identity(
        &different_targets,
        "keccak256:0x3333333333333333333333333333333333333333333333333333333333333333",
    );
    let error = create_backfill_job(database.pool(), &different_compact)
        .await
        .expect_err("different compact target set must not reuse legacy full job");
    assert!(
        error
            .to_string()
            .contains("does not match requested immutable job identity"),
        "unexpected error: {error:#}"
    );

    let mut provider_drift = compact.clone();
    provider_drift
        .source_identity
        .as_object_mut()
        .expect("compact source identity is an object")
        .insert("coinbase_sql_validation_mode".to_owned(), json!("full"));
    let error = create_backfill_job(database.pool(), &provider_drift)
        .await
        .expect_err(
            "same target set with changed Coinbase SQL fields must not reuse legacy full job",
        );
    assert!(
        error
            .to_string()
            .contains("does not match requested immutable job identity"),
        "unexpected error: {error:#}"
    );

    let mut missing_sample = compact;
    missing_sample
        .source_identity
        .as_object_mut()
        .expect("compact source identity is an object")
        .remove("selected_targets_sample");
    let error = create_backfill_job(database.pool(), &missing_sample)
        .await
        .expect_err(
            "compact identity without selected_targets_sample must not reuse legacy full job",
        );
    assert!(
        error
            .to_string()
            .contains("does not match requested immutable job identity"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn backfill_job_reservation_is_idempotent_and_reclaims_expired_leases() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-reservation-idempotent"),
    )
    .await?;

    let reserved = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-a",
        lease_deadline(),
    )
    .await?
    .expect("range must be reservable");
    assert_eq!(reserved.status, BackfillLifecycleStatus::Reserved);
    assert_eq!(reserved.lease_token.as_deref(), Some("lease-a"));
    assert_eq!(reserved.attempt_count, 1);

    let duplicate = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-a",
        lease_deadline(),
    )
    .await?
    .expect("duplicate lease must return the same reservation");
    assert_eq!(duplicate.backfill_range_id, reserved.backfill_range_id);
    assert_eq!(duplicate.attempt_count, 1);

    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET lease_expires_at = now() - interval '1 second'
        WHERE backfill_range_id = $1
        "#,
    )
    .bind(reserved.backfill_range_id)
    .execute(database.pool())
    .await?;

    let reclaimed = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-b",
        "lease-b",
        lease_deadline(),
    )
    .await?
    .expect("expired lease must be reclaimable");
    assert_eq!(reclaimed.backfill_range_id, reserved.backfill_range_id);
    assert_eq!(reclaimed.lease_token.as_deref(), Some("lease-b"));
    assert_eq!(reclaimed.lease_owner.as_deref(), Some("worker-b"));
    assert_eq!(reclaimed.attempt_count, 2);
    let reclaimed_deadline = reclaimed
        .lease_expires_at
        .expect("reclaimed range must hold worker-b's lease deadline");

    let stale_advance =
        advance_backfill_range(database.pool(), reserved.backfill_range_id, "lease-a", 105)
            .await
            .expect_err("stale worker-a token must not advance or heartbeat after worker-b steals");
    assert!(
        stale_advance
            .to_string()
            .contains("not held by lease token"),
        "unexpected error: {stale_advance:#}"
    );
    let after_stale_advance = load_backfill_ranges(database.pool(), created.job.backfill_job_id)
        .await?
        .into_iter()
        .find(|range| range.backfill_range_id == reclaimed.backfill_range_id)
        .expect("reclaimed range must still exist after stale advance");
    assert_eq!(after_stale_advance.lease_token.as_deref(), Some("lease-b"));
    assert_eq!(after_stale_advance.lease_owner.as_deref(), Some("worker-b"));
    assert_eq!(
        after_stale_advance.lease_expires_at,
        Some(reclaimed_deadline)
    );

    database.cleanup().await
}

#[tokio::test]
async fn backfill_range_advance_refreshes_active_lease_deadline() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-advance-refreshes-lease"),
    )
    .await?;

    let reserved = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-refresh",
        lease_deadline(),
    )
    .await?
    .expect("range must be reservable");

    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET
            updated_at = now() - interval '295 seconds',
            lease_expires_at = now() + interval '5 seconds'
        WHERE backfill_range_id = $1
        "#,
    )
    .bind(reserved.backfill_range_id)
    .execute(database.pool())
    .await?;

    let advanced = advance_backfill_range(
        database.pool(),
        reserved.backfill_range_id,
        "lease-refresh",
        105,
    )
    .await?;
    let refreshed_lease = advanced
        .lease_expires_at
        .expect("running range must retain an active lease deadline");
    let minimum_refresh_deadline = OffsetDateTime::now_utc()
        .unix_timestamp()
        .checked_add(240)
        .context("minimum lease refresh timestamp overflowed")?;
    assert!(
        refreshed_lease.unix_timestamp() >= minimum_refresh_deadline,
        "advance must extend the active lease; got {refreshed_lease}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn reservation_finalizes_running_job_when_all_ranges_already_completed() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-reservation-finalizes-drained-running-job"),
    )
    .await?;

    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET
            status = 'completed'::backfill_lifecycle_status,
            checkpoint_block_number = range_end_block_number,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = now(),
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(created.job.backfill_job_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET
            status = 'running'::backfill_lifecycle_status,
            completed_at = NULL,
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(created.job.backfill_job_id)
    .execute(database.pool())
    .await?;

    let reserved = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-finalizer",
        "lease-finalizer",
        lease_deadline(),
    )
    .await?;
    assert!(reserved.is_none());
    let job = load_backfill_job(database.pool(), created.job.backfill_job_id)
        .await?
        .expect("job must still exist");
    assert_eq!(job.status, BackfillLifecycleStatus::Completed);
    assert!(
        job.completed_at.is_some(),
        "reservation should complete the already-drained running job"
    );

    database.cleanup().await
}

#[tokio::test]
async fn backfill_range_advance_rejects_expired_lease_token() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-advance-rejects-expired-lease"),
    )
    .await?;

    let reserved = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-expired",
        lease_deadline(),
    )
    .await?
    .expect("range must be reservable");

    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET lease_expires_at = now() - interval '1 second'
        WHERE backfill_range_id = $1
        "#,
    )
    .bind(reserved.backfill_range_id)
    .execute(database.pool())
    .await?;

    let error = advance_backfill_range(
        database.pool(),
        reserved.backfill_range_id,
        "lease-expired",
        105,
    )
    .await
    .expect_err("expired lease token must not advance or refresh a range");
    assert!(
        error.to_string().contains("lease expired"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn backfill_job_range_advance_and_completion_are_monotonic() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-advance-complete"),
    )
    .await?;

    let first = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-first",
        lease_deadline(),
    )
    .await?
    .expect("first range must be reservable");

    let advanced =
        advance_backfill_range(database.pool(), first.backfill_range_id, "lease-first", 105)
            .await?;
    assert_eq!(advanced.status, BackfillLifecycleStatus::Running);
    assert_eq!(advanced.checkpoint_block_number, 105);

    let stale =
        advance_backfill_range(database.pool(), first.backfill_range_id, "lease-first", 104)
            .await?;
    assert_eq!(stale.checkpoint_block_number, 105);

    let error = complete_backfill_range(database.pool(), first.backfill_range_id, "lease-first")
        .await
        .expect_err("range completion must require checkpoint at declared end");
    assert!(
        error
            .to_string()
            .contains("has not reached declared range end"),
        "unexpected error: {error:#}"
    );

    advance_backfill_range(database.pool(), first.backfill_range_id, "lease-first", 109).await?;
    let completed_first =
        complete_backfill_range(database.pool(), first.backfill_range_id, "lease-first").await?;
    assert_eq!(completed_first.status, BackfillLifecycleStatus::Completed);
    assert_eq!(completed_first.lease_token, None);

    let second = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-second",
        lease_deadline(),
    )
    .await?
    .expect("second range must be reservable");
    assert_ne!(second.backfill_range_id, first.backfill_range_id);
    advance_backfill_range(
        database.pool(),
        second.backfill_range_id,
        "lease-second",
        120,
    )
    .await?;
    complete_backfill_range(database.pool(), second.backfill_range_id, "lease-second").await?;

    let job = load_backfill_job(database.pool(), created.job.backfill_job_id)
        .await?
        .expect("job must still exist");
    assert_eq!(job.status, BackfillLifecycleStatus::Completed);
    assert!(job.completed_at.is_some());

    database.cleanup().await
}

#[tokio::test]
async fn backfill_job_failure_records_metadata_without_rewinding_checkpoint() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = backfill_job_create("job-failure");
    request.ranges = Vec::new();
    let created = create_backfill_job(database.pool(), &request).await?;

    let reserved = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-fail",
        lease_deadline(),
    )
    .await?
    .expect("range must be reservable");
    advance_backfill_range(
        database.pool(),
        reserved.backfill_range_id,
        "lease-fail",
        111,
    )
    .await?;

    let failed = fail_backfill_range(
        database.pool(),
        reserved.backfill_range_id,
        "lease-fail",
        "rpc timeout",
        json!({ "block": 112 }),
    )
    .await?;
    assert_eq!(failed.status, BackfillLifecycleStatus::Failed);
    assert_eq!(failed.checkpoint_block_number, 111);
    assert_eq!(failed.failure_reason.as_deref(), Some("rpc timeout"));
    assert_eq!(failed.failure_metadata, json!({ "block": 112 }));

    let failed_job = load_backfill_job(database.pool(), created.job.backfill_job_id)
        .await?
        .expect("job must still exist");
    assert_eq!(failed_job.status, BackfillLifecycleStatus::Failed);

    let retried = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-b",
        "lease-retry",
        lease_deadline(),
    )
    .await?
    .expect("failed range must be explicitly reservable");
    assert_eq!(retried.backfill_range_id, reserved.backfill_range_id);
    assert_eq!(retried.checkpoint_block_number, 111);
    assert_eq!(retried.status, BackfillLifecycleStatus::Reserved);
    assert_eq!(retried.failure_reason, None);
    assert_eq!(retried.failure_metadata, json!({}));

    database.cleanup().await
}

#[tokio::test]
async fn complete_backfill_job_preserves_failed_range_lifecycle_at_range_end() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = backfill_job_create("job-failed-complete-guard");
    request.ranges = Vec::new();
    let created = create_backfill_job(database.pool(), &request).await?;

    let reserved = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-end-fail",
        lease_deadline(),
    )
    .await?
    .expect("range must be reservable");
    advance_backfill_range(
        database.pool(),
        reserved.backfill_range_id,
        "lease-end-fail",
        request.range_end_block_number,
    )
    .await?;

    let failure_metadata = json!({ "block": request.range_end_block_number, "attempt": 1 });
    let failed = fail_backfill_range(
        database.pool(),
        reserved.backfill_range_id,
        "lease-end-fail",
        "rpc timeout",
        failure_metadata.clone(),
    )
    .await?;
    assert_eq!(failed.status, BackfillLifecycleStatus::Failed);
    assert_eq!(
        failed.checkpoint_block_number,
        request.range_end_block_number
    );

    let error = complete_backfill_job(database.pool(), created.job.backfill_job_id)
        .await
        .expect_err("job completion must not overwrite failed ranges at declared end");
    assert!(
        error.to_string().contains("failed ranges"),
        "unexpected error: {error:#}"
    );

    let job = load_backfill_job(database.pool(), created.job.backfill_job_id)
        .await?
        .expect("job must still exist");
    assert_eq!(job.status, BackfillLifecycleStatus::Failed);
    assert_eq!(job.failure_reason.as_deref(), Some("rpc timeout"));
    assert_eq!(job.failure_metadata, failure_metadata);
    assert!(job.completed_at.is_none());

    let ranges = load_backfill_ranges(database.pool(), created.job.backfill_job_id).await?;
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].status, BackfillLifecycleStatus::Failed);
    assert_eq!(
        ranges[0].checkpoint_block_number,
        request.range_end_block_number
    );
    assert_eq!(ranges[0].failure_reason.as_deref(), Some("rpc timeout"));
    assert_eq!(
        ranges[0].failure_metadata,
        json!({ "block": request.range_end_block_number, "attempt": 1 })
    );
    assert!(ranges[0].completed_at.is_none());

    database.cleanup().await
}
