use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigname_storage::{
    ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, ExecutionTraceStep, default_database_url,
    load_execution_outcome, upsert_execution_outcome, upsert_execution_trace,
};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};

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
            .context("failed to parse database URL for worker execution tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_worker_execution_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for worker execution tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect worker execution test pool")?;

        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for worker execution tests")?;

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

fn execution_trace(
    execution_trace_id: Uuid,
    request_type: &str,
    namespace: &str,
    request_key: &str,
    finished_at: i64,
) -> ExecutionTrace {
    ExecutionTrace {
        execution_trace_id,
        request_type: request_type.to_owned(),
        request_key: request_key.to_owned(),
        namespace: namespace.to_owned(),
        chain_context: json!({
            "requested_positions": [{
                "chain_id": "ethereum-mainnet",
                "block_number": 22_000_000,
                "block_hash": "0xabc123"
            }]
        }),
        manifest_context: json!({
            "manifest_versions": [{
                "source_family": "ens_execution",
                "manifest_version": 5
            }]
        }),
        contracts_called: json!([]),
        gateway_digests: json!([]),
        final_payload: Some(json!({
            "verified_queries": [{
                "record_key": "addr:60",
                "status": "success"
            }]
        })),
        failure_payload: None,
        request_metadata: json!({
            "surface": request_key,
            "normalizer_version": "ensip15@ens-normalize-0.1.1"
        }),
        finished_at: Some(timestamp(finished_at)),
        steps: vec![ExecutionTraceStep {
            step_index: 0,
            step_kind: "load_declared_topology".to_owned(),
            input_digest: Some("sha256:input".to_owned()),
            output_digest: Some("sha256:output".to_owned()),
            latency_ms: Some(3),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xabc123",
                    "block_number": 22_000_000,
                    "state": "canonical"
                }
            }),
            step_payload: json!({
                "entrypoint": "universal_resolver"
            }),
        }],
    }
}

fn version_boundary(
    logical_name_id: &str,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id,
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": timestamp,
        }
    })
}

fn execution_outcome(
    trace: &ExecutionTrace,
    manifest_versions: Value,
    topology_version_boundary: Value,
    record_version_boundary: Value,
) -> ExecutionOutcome {
    ExecutionOutcome {
        cache_key: ExecutionCacheKey {
            request_key: trace.request_key.clone(),
            requested_chain_positions: json!([{
                "chain_id": "ethereum-mainnet",
                "block_number": 22_000_000,
                "block_hash": "0xabc123"
            }]),
            manifest_versions,
            topology_version_boundary,
            record_version_boundary,
        },
        execution_trace_id: trace.execution_trace_id,
        request_type: trace.request_type.clone(),
        namespace: trace.namespace.clone(),
        outcome_payload: Some(json!({
            "verified_queries": [{
                "record_key": "addr:60",
                "status": "success"
            }]
        })),
        failure_payload: None,
        finished_at: trace
            .finished_at
            .expect("worker execution trace fixture must finish"),
    }
}

async fn insert_trace_and_outcome(
    database: &TestDatabase,
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<()> {
    upsert_execution_trace(database.pool(), trace).await?;
    upsert_execution_outcome(database.pool(), outcome).await?;
    Ok(())
}

#[tokio::test]
async fn manifest_invalidation_wrapper_deletes_only_verified_resolution_targets() -> Result<()> {
    let database = TestDatabase::new().await?;

    let target_trace = execution_trace(
        Uuid::from_u128(0x5e710000000000000000000000000001),
        VERIFIED_RESOLUTION_REQUEST_TYPE,
        "ens",
        "ens:alice.eth:addr:60",
        1_717_173_000,
    );
    let target_outcome = execution_outcome(
        &target_trace,
        json!([{
            "source_manifest_id": 19,
            "manifest_version": 3
        }]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x5e71000000000000000000000000aaa1),
            Some(2_100),
            Some("ResolverChanged"),
            22_100_010,
            "0xaaa010",
            "2024-06-04T00:00:27Z",
        ),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x5e71000000000000000000000000bbb1),
            Some(2_110),
            Some("RecordsChanged"),
            22_100_011,
            "0xaaa011",
            "2024-06-04T00:00:28Z",
        ),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let keep_trace = execution_trace(
        Uuid::from_u128(0x5e710000000000000000000000000002),
        "verified_primary_name",
        "ens",
        "ens:alice.eth:primary",
        1_717_173_001,
    );
    let keep_outcome = execution_outcome(
        &keep_trace,
        json!([{
            "source_manifest_id": 19,
            "manifest_version": 3
        }]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x5e71000000000000000000000000aaa2),
            Some(2_120),
            Some("ResolverChanged"),
            22_100_020,
            "0xbbb020",
            "2024-06-04T00:00:37Z",
        ),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x5e71000000000000000000000000bbb2),
            Some(2_130),
            Some("RecordsChanged"),
            22_100_021,
            "0xbbb021",
            "2024-06-04T00:00:38Z",
        ),
    );
    insert_trace_and_outcome(&database, &keep_trace, &keep_outcome).await?;

    let summary = invalidate_verified_resolution_manifest_version(
        database.pool(),
        &VerifiedResolutionManifestInvalidation {
            namespace: "ens".to_owned(),
            source_manifest_id: Some(19),
            source_family: None,
            manifest_version: 3,
        },
    )
    .await?;
    assert_eq!(summary.deleted_outcome_count, 1);
    assert_eq!(
        load_execution_outcome(database.pool(), &target_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &keep_outcome.cache_key).await?,
        Some(keep_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn topology_boundary_wrapper_deletes_exact_boundary_matches() -> Result<()> {
    let database = TestDatabase::new().await?;

    let invalidation = VerifiedResolutionBoundaryInvalidation {
        namespace: "ens".to_owned(),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x5e71000000000000000000000000ccc1),
        normalized_event_id: Some(2_200),
        event_kind: Some("ResolverChanged".to_owned()),
        chain_id: "ethereum-mainnet".to_owned(),
        block_number: 22_200_010,
        block_hash: "0xccc010".to_owned(),
        timestamp: "2024-06-05T00:00:27Z".to_owned(),
    };

    let target_trace = execution_trace(
        Uuid::from_u128(0x5e710000000000000000000000000003),
        VERIFIED_RESOLUTION_REQUEST_TYPE,
        "ens",
        "ens:alice.eth:text",
        1_717_173_100,
    );
    let target_outcome = execution_outcome(
        &target_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 7
        }]),
        invalidation.boundary(),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x5e71000000000000000000000000ddd1),
            Some(2_210),
            Some("RecordsChanged"),
            22_200_011,
            "0xccc011",
            "2024-06-05T00:00:28Z",
        ),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let keep_trace = execution_trace(
        Uuid::from_u128(0x5e710000000000000000000000000004),
        VERIFIED_RESOLUTION_REQUEST_TYPE,
        "ens",
        "ens:bob.eth:text",
        1_717_173_101,
    );
    let keep_outcome = execution_outcome(
        &keep_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 7
        }]),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x5e71000000000000000000000000ccc2),
            Some(2_220),
            Some("ResolverChanged"),
            22_200_020,
            "0xddd020",
            "2024-06-05T00:00:37Z",
        ),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x5e71000000000000000000000000ddd2),
            Some(2_230),
            Some("RecordsChanged"),
            22_200_021,
            "0xddd021",
            "2024-06-05T00:00:38Z",
        ),
    );
    insert_trace_and_outcome(&database, &keep_trace, &keep_outcome).await?;

    let summary =
        invalidate_verified_resolution_topology_boundary(database.pool(), &invalidation).await?;
    assert_eq!(summary.deleted_outcome_count, 1);
    assert_eq!(
        load_execution_outcome(database.pool(), &target_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &keep_outcome.cache_key).await?,
        Some(keep_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn record_boundary_wrapper_deletes_exact_boundary_matches() -> Result<()> {
    let database = TestDatabase::new().await?;

    let invalidation = VerifiedResolutionBoundaryInvalidation {
        namespace: "ens".to_owned(),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x5e71000000000000000000000000eee1),
        normalized_event_id: Some(2_300),
        event_kind: Some("RecordsChanged".to_owned()),
        chain_id: "ethereum-mainnet".to_owned(),
        block_number: 22_300_010,
        block_hash: "0xeee010".to_owned(),
        timestamp: "2024-06-06T00:00:27Z".to_owned(),
    };

    let target_trace = execution_trace(
        Uuid::from_u128(0x5e710000000000000000000000000005),
        VERIFIED_RESOLUTION_REQUEST_TYPE,
        "ens",
        "ens:alice.eth:addr:60",
        1_717_173_200,
    );
    let target_outcome = execution_outcome(
        &target_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 8
        }]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x5e71000000000000000000000000fff1),
            Some(2_310),
            Some("ResolverChanged"),
            22_300_011,
            "0xeee011",
            "2024-06-06T00:00:28Z",
        ),
        invalidation.boundary(),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let keep_trace = execution_trace(
        Uuid::from_u128(0x5e710000000000000000000000000006),
        VERIFIED_RESOLUTION_REQUEST_TYPE,
        "ens",
        "ens:alice.eth:text",
        1_717_173_201,
    );
    let keep_outcome = execution_outcome(
        &keep_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 8
        }]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x5e71000000000000000000000000fff2),
            Some(2_320),
            Some("ResolverChanged"),
            22_300_020,
            "0xfff020",
            "2024-06-06T00:00:37Z",
        ),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x5e71000000000000000000000000eee2),
            Some(2_330),
            Some("RecordsChanged"),
            22_300_021,
            "0xfff021",
            "2024-06-06T00:00:38Z",
        ),
    );
    insert_trace_and_outcome(&database, &keep_trace, &keep_outcome).await?;

    let summary =
        invalidate_verified_resolution_record_boundary(database.pool(), &invalidation).await?;
    assert_eq!(summary.deleted_outcome_count, 1);
    assert_eq!(
        load_execution_outcome(database.pool(), &target_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &keep_outcome.cache_key).await?,
        Some(keep_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn verified_primary_manifest_wrapper_scopes_to_exact_tuple_request_key() -> Result<()> {
    let database = TestDatabase::new().await?;

    let invalidation = VerifiedPrimaryNameManifestInvalidation {
        namespace: "ens".to_owned(),
        address: "0xAbCd".to_owned(),
        coin_type: "60".to_owned(),
        source_manifest_id: Some(29),
        source_family: None,
        manifest_version: 4,
    };

    let target_trace = execution_trace(
        Uuid::from_u128(0x5e710000000000000000000000000007),
        VERIFIED_PRIMARY_NAME_REQUEST_TYPE,
        "ens",
        &invalidation.request_key(),
        1_717_173_300,
    );
    let target_outcome = execution_outcome(
        &target_trace,
        json!([{
            "source_manifest_id": 29,
            "manifest_version": 4
        }]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x5e71000000000000000000000000aaa3),
            Some(2_400),
            Some("ResolverChanged"),
            22_400_010,
            "0x111010",
            "2024-06-07T00:00:27Z",
        ),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x5e71000000000000000000000000bbb3),
            Some(2_410),
            Some("RecordsChanged"),
            22_400_011,
            "0x111011",
            "2024-06-07T00:00:28Z",
        ),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let other_tuple_trace = execution_trace(
        Uuid::from_u128(0x5e710000000000000000000000000008),
        VERIFIED_PRIMARY_NAME_REQUEST_TYPE,
        "ens",
        &verified_primary_name_request_key("ens", "0xEf01", "60"),
        1_717_173_301,
    );
    let other_tuple_outcome = execution_outcome(
        &other_tuple_trace,
        json!([{
            "source_manifest_id": 29,
            "manifest_version": 4
        }]),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x5e71000000000000000000000000aaa4),
            Some(2_420),
            Some("ResolverChanged"),
            22_400_020,
            "0x222020",
            "2024-06-07T00:00:37Z",
        ),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x5e71000000000000000000000000bbb4),
            Some(2_430),
            Some("RecordsChanged"),
            22_400_021,
            "0x222021",
            "2024-06-07T00:00:38Z",
        ),
    );
    insert_trace_and_outcome(&database, &other_tuple_trace, &other_tuple_outcome).await?;

    let verified_resolution_trace = execution_trace(
        Uuid::from_u128(0x5e710000000000000000000000000009),
        VERIFIED_RESOLUTION_REQUEST_TYPE,
        "ens",
        &invalidation.request_key(),
        1_717_173_302,
    );
    let verified_resolution_outcome = execution_outcome(
        &verified_resolution_trace,
        json!([{
            "source_manifest_id": 29,
            "manifest_version": 4
        }]),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x5e71000000000000000000000000aaa5),
            Some(2_440),
            Some("ResolverChanged"),
            22_400_030,
            "0x333030",
            "2024-06-07T00:00:47Z",
        ),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x5e71000000000000000000000000bbb5),
            Some(2_450),
            Some("RecordsChanged"),
            22_400_031,
            "0x333031",
            "2024-06-07T00:00:48Z",
        ),
    );
    insert_trace_and_outcome(
        &database,
        &verified_resolution_trace,
        &verified_resolution_outcome,
    )
    .await?;

    let summary =
        invalidate_verified_primary_name_manifest_version(database.pool(), &invalidation).await?;
    assert_eq!(summary.deleted_outcome_count, 1);
    assert_eq!(
        load_execution_outcome(database.pool(), &target_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &other_tuple_outcome.cache_key).await?,
        Some(other_tuple_outcome)
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &verified_resolution_outcome.cache_key).await?,
        Some(verified_resolution_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn verified_primary_topology_wrapper_scopes_to_exact_tuple_request_key() -> Result<()> {
    let database = TestDatabase::new().await?;

    let invalidation = VerifiedPrimaryNameBoundaryInvalidation {
        namespace: "ens".to_owned(),
        address: "0xAbCd".to_owned(),
        coin_type: "60".to_owned(),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x5e71000000000000000000000000ccc3),
        normalized_event_id: Some(2_500),
        event_kind: Some("ResolverChanged".to_owned()),
        chain_id: "ethereum-mainnet".to_owned(),
        block_number: 22_500_010,
        block_hash: "0x444010".to_owned(),
        timestamp: "2024-06-08T00:00:27Z".to_owned(),
    };

    let target_trace = execution_trace(
        Uuid::from_u128(0x5e71000000000000000000000000000a),
        VERIFIED_PRIMARY_NAME_REQUEST_TYPE,
        "ens",
        &invalidation.request_key(),
        1_717_173_400,
    );
    let target_outcome = execution_outcome(
        &target_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 9
        }]),
        invalidation.boundary(),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x5e71000000000000000000000000ddd3),
            Some(2_510),
            Some("RecordsChanged"),
            22_500_011,
            "0x444011",
            "2024-06-08T00:00:28Z",
        ),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let other_tuple_trace = execution_trace(
        Uuid::from_u128(0x5e71000000000000000000000000000b),
        VERIFIED_PRIMARY_NAME_REQUEST_TYPE,
        "ens",
        &verified_primary_name_request_key("ens", "0xEf01", "60"),
        1_717_173_401,
    );
    let other_tuple_outcome = execution_outcome(
        &other_tuple_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 9
        }]),
        invalidation.boundary(),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x5e71000000000000000000000000ddd4),
            Some(2_520),
            Some("RecordsChanged"),
            22_500_021,
            "0x555021",
            "2024-06-08T00:00:38Z",
        ),
    );
    insert_trace_and_outcome(&database, &other_tuple_trace, &other_tuple_outcome).await?;

    let verified_resolution_trace = execution_trace(
        Uuid::from_u128(0x5e71000000000000000000000000000c),
        VERIFIED_RESOLUTION_REQUEST_TYPE,
        "ens",
        &invalidation.request_key(),
        1_717_173_402,
    );
    let verified_resolution_outcome = execution_outcome(
        &verified_resolution_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 9
        }]),
        invalidation.boundary(),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x5e71000000000000000000000000ddd5),
            Some(2_530),
            Some("RecordsChanged"),
            22_500_031,
            "0x666031",
            "2024-06-08T00:00:48Z",
        ),
    );
    insert_trace_and_outcome(
        &database,
        &verified_resolution_trace,
        &verified_resolution_outcome,
    )
    .await?;

    let summary =
        invalidate_verified_primary_name_topology_boundary(database.pool(), &invalidation).await?;
    assert_eq!(summary.deleted_outcome_count, 1);
    assert_eq!(
        load_execution_outcome(database.pool(), &target_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &other_tuple_outcome.cache_key).await?,
        Some(other_tuple_outcome)
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &verified_resolution_outcome.cache_key).await?,
        Some(verified_resolution_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn verified_primary_record_wrapper_scopes_to_exact_tuple_request_key() -> Result<()> {
    let database = TestDatabase::new().await?;

    let invalidation = VerifiedPrimaryNameBoundaryInvalidation {
        namespace: "ens".to_owned(),
        address: "0xAbCd".to_owned(),
        coin_type: "60".to_owned(),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x5e71000000000000000000000000eee3),
        normalized_event_id: Some(2_600),
        event_kind: Some("RecordsChanged".to_owned()),
        chain_id: "ethereum-mainnet".to_owned(),
        block_number: 22_600_010,
        block_hash: "0x777010".to_owned(),
        timestamp: "2024-06-09T00:00:27Z".to_owned(),
    };

    let target_trace = execution_trace(
        Uuid::from_u128(0x5e71000000000000000000000000000d),
        VERIFIED_PRIMARY_NAME_REQUEST_TYPE,
        "ens",
        &invalidation.request_key(),
        1_717_173_500,
    );
    let target_outcome = execution_outcome(
        &target_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 10
        }]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x5e71000000000000000000000000fff3),
            Some(2_610),
            Some("ResolverChanged"),
            22_600_011,
            "0x777011",
            "2024-06-09T00:00:28Z",
        ),
        invalidation.boundary(),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let other_tuple_trace = execution_trace(
        Uuid::from_u128(0x5e71000000000000000000000000000e),
        VERIFIED_PRIMARY_NAME_REQUEST_TYPE,
        "ens",
        &verified_primary_name_request_key("ens", "0xEf01", "60"),
        1_717_173_501,
    );
    let other_tuple_outcome = execution_outcome(
        &other_tuple_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 10
        }]),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x5e71000000000000000000000000fff4),
            Some(2_620),
            Some("ResolverChanged"),
            22_600_021,
            "0x888021",
            "2024-06-09T00:00:38Z",
        ),
        invalidation.boundary(),
    );
    insert_trace_and_outcome(&database, &other_tuple_trace, &other_tuple_outcome).await?;

    let verified_resolution_trace = execution_trace(
        Uuid::from_u128(0x5e71000000000000000000000000000f),
        VERIFIED_RESOLUTION_REQUEST_TYPE,
        "ens",
        &invalidation.request_key(),
        1_717_173_502,
    );
    let verified_resolution_outcome = execution_outcome(
        &verified_resolution_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 10
        }]),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x5e71000000000000000000000000fff5),
            Some(2_630),
            Some("ResolverChanged"),
            22_600_031,
            "0x999031",
            "2024-06-09T00:00:48Z",
        ),
        invalidation.boundary(),
    );
    insert_trace_and_outcome(
        &database,
        &verified_resolution_trace,
        &verified_resolution_outcome,
    )
    .await?;

    let summary =
        invalidate_verified_primary_name_record_boundary(database.pool(), &invalidation).await?;
    assert_eq!(summary.deleted_outcome_count, 1);
    assert_eq!(
        load_execution_outcome(database.pool(), &target_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &other_tuple_outcome.cache_key).await?,
        Some(other_tuple_outcome)
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &verified_resolution_outcome.cache_key).await?,
        Some(verified_resolution_outcome)
    );

    database.cleanup().await
}
