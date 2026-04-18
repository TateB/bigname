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
            .context("failed to parse database URL for execution-trace tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_storage_execution_trace_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for execution-trace tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect execution-trace test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for execution-trace tests")?;

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

fn execution_trace() -> ExecutionTrace {
    ExecutionTrace {
        execution_trace_id: Uuid::from_u128(0x0e7ec7ace00000000000000000000001),
        request_type: "verified_resolution".to_owned(),
        request_key: "ens:alice.eth:addr:60".to_owned(),
        namespace: "ens".to_owned(),
        chain_context: json!({
            "requested_positions": [
                {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_000,
                    "block_hash": "0xabc123"
                }
            ],
            "topology_version_boundary": {
                "ethereum-mainnet": 21_000_000
            }
        }),
        manifest_context: json!({
            "manifest_versions": [
                {
                    "source_manifest_id": 7,
                    "manifest_version": 3
                }
            ],
            "rollout_boundary": 3
        }),
        contracts_called: json!([
            {
                "chain_id": "ethereum-mainnet",
                "contract_address": "0x0000000000000000000000000000000000000abc",
                "selector": "0x9061b923"
            }
        ]),
        gateway_digests: json!([
            {
                "digest": "sha256:feedface",
                "content_type": "application/json"
            }
        ]),
        final_payload: Some(json!({
            "record_kind": "addr",
            "coin_type": 60,
            "value": "0x00000000000000000000000000000000000000aa"
        })),
        failure_payload: None,
        request_metadata: json!({
            "surface": "alice.eth",
            "normalizer_version": "nfkc-v1"
        }),
        finished_at: Some(timestamp(1_717_171_717)),
        steps: vec![
            ExecutionTraceStep {
                step_index: 0,
                step_kind: "load_declared_topology".to_owned(),
                input_digest: Some("sha256:topology-input".to_owned()),
                output_digest: Some("sha256:topology-output".to_owned()),
                latency_ms: Some(4),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "entrypoint": "universal_resolver",
                    "resolver": "0x0000000000000000000000000000000000000abc"
                }),
            },
            ExecutionTraceStep {
                step_index: 1,
                step_kind: "call_universal_resolver".to_owned(),
                input_digest: Some("sha256:resolver-input".to_owned()),
                output_digest: Some("sha256:resolver-output".to_owned()),
                latency_ms: Some(28),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "coin_type": 60,
                    "name": "alice.eth",
                    "resolved_address": "0x00000000000000000000000000000000000000aa"
                }),
            },
        ],
    }
}

async fn expect_trace_validation_error(
    database: &TestDatabase,
    trace: &ExecutionTrace,
    expected_message: &str,
) -> Result<()> {
    let error = upsert_execution_trace(database.pool(), trace)
        .await
        .expect_err("execution trace validation must fail");

    assert!(
        error.to_string().contains(expected_message),
        "unexpected error: {error:#}"
    );
    assert!(
        load_execution_trace(database.pool(), trace.execution_trace_id)
            .await?
            .is_none(),
        "invalid execution trace {} must not be written",
        trace.execution_trace_id
    );

    Ok(())
}

fn version_boundary(resource_id: Uuid) -> serde_json::Value {
    json!({
        "logical_name_id": "ens:alice.eth",
        "resource_id": resource_id.to_string(),
        "normalized_event_id": 1_200,
        "event_kind": "RecordsChanged",
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_000,
            "block_hash": "0xabc123",
            "timestamp": "2024-06-01T00:00:17Z",
        }
    })
}

fn execution_outcome(trace: &ExecutionTrace) -> ExecutionOutcome {
    ExecutionOutcome {
        cache_key: ExecutionCacheKey {
            request_key: trace.request_key.clone(),
            requested_chain_positions: json!([
                {
                    "chain_id": "base-mainnet",
                    "block_number": 17_500_000,
                    "block_hash": "0xbase999"
                },
                {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_000,
                    "block_hash": "0xabc123"
                }
            ]),
            manifest_versions: json!([
                {
                    "source_family": "ens_v1_registry_l1",
                    "manifest_version": 3
                },
                {
                    "source_manifest_id": 7,
                    "manifest_version": 3
                }
            ]),
            topology_version_boundary: version_boundary(Uuid::from_u128(
                0x0e7ec7ace0000000000000000000aaa1,
            )),
            record_version_boundary: version_boundary(Uuid::from_u128(
                0x0e7ec7ace0000000000000000000aaa2,
            )),
        },
        execution_trace_id: trace.execution_trace_id,
        request_type: trace.request_type.clone(),
        namespace: trace.namespace.clone(),
        outcome_payload: Some(json!({
            "verified_queries": [
                {
                    "record_key": "addr:60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x00000000000000000000000000000000000000aa"
                    }
                }
            ]
        })),
        failure_payload: None,
        finished_at: trace
            .finished_at
            .expect("execution trace test fixture must finish"),
    }
}

async fn expect_outcome_validation_error(
    database: &TestDatabase,
    outcome: &ExecutionOutcome,
    expected_message: &str,
) -> Result<()> {
    let error = upsert_execution_outcome(database.pool(), outcome)
        .await
        .expect_err("execution outcome validation must fail");
    let rendered = format!("{error:#}");

    assert!(
        rendered.contains(expected_message),
        "unexpected error: {error:#}"
    );

    let persisted_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution_cache_outcomes")
        .fetch_one(database.pool())
        .await
        .context("failed to count execution_cache_outcomes rows after validation error")?;
    assert!(
        persisted_count == 0,
        "invalid execution outcomes must not be written"
    );

    Ok(())
}

#[tokio::test]
async fn upserts_and_loads_execution_trace_with_ordered_steps() -> Result<()> {
    let database = TestDatabase::new().await?;
    let trace = execution_trace();

    let inserted = upsert_execution_trace(database.pool(), &trace).await?;
    assert_eq!(inserted, trace);

    let upserted_again = upsert_execution_trace(database.pool(), &trace).await?;
    assert_eq!(upserted_again, trace);

    let loaded = load_execution_trace(database.pool(), trace.execution_trace_id)
        .await?
        .expect("execution trace must exist after upsert");
    assert_eq!(loaded, trace);
    assert_eq!(
        loaded
            .steps
            .iter()
            .map(|step| step.step_index)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(loaded.steps[0].step_kind, "load_declared_topology");
    assert_eq!(loaded.steps[1].step_kind, "call_universal_resolver");

    database.cleanup().await
}

#[tokio::test]
async fn rejects_execution_trace_without_steps() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut trace = execution_trace();
    trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000002);
    trace.steps.clear();

    expect_trace_validation_error(&database, &trace, "must include at least one step").await?;

    database.cleanup().await
}

#[tokio::test]
async fn rejects_execution_trace_without_nonempty_contexts_or_terminal_state() -> Result<()> {
    let database = TestDatabase::new().await?;

    let mut empty_chain_context = execution_trace();
    empty_chain_context.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000003);
    empty_chain_context.chain_context = json!({});
    expect_trace_validation_error(
        &database,
        &empty_chain_context,
        "field chain_context must not be empty",
    )
    .await?;

    let mut empty_manifest_context = execution_trace();
    empty_manifest_context.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000004);
    empty_manifest_context.manifest_context = json!({});
    expect_trace_validation_error(
        &database,
        &empty_manifest_context,
        "field manifest_context must not be empty",
    )
    .await?;

    let mut missing_finished_at = execution_trace();
    missing_finished_at.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000005);
    missing_finished_at.finished_at = None;
    expect_trace_validation_error(&database, &missing_finished_at, "must set finished_at").await?;

    let mut missing_terminal_payload = execution_trace();
    missing_terminal_payload.execution_trace_id =
        Uuid::from_u128(0x0e7ec7ace00000000000000000000006);
    missing_terminal_payload.final_payload = None;
    missing_terminal_payload.failure_payload = None;
    expect_trace_validation_error(
        &database,
        &missing_terminal_payload,
        "must set final_payload or failure_payload",
    )
    .await?;

    database.cleanup().await
}

#[tokio::test]
async fn rejects_execution_trace_with_empty_step_canonicality_dependency() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut trace = execution_trace();
    trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000007);
    trace.steps[0].canonicality_dependency = json!({});

    expect_trace_validation_error(
        &database,
        &trace,
        "field canonicality_dependency must not be empty",
    )
    .await?;

    database.cleanup().await
}

#[tokio::test]
async fn partial_trace_cannot_get_stuck_before_complete_insert() -> Result<()> {
    let database = TestDatabase::new().await?;
    let complete_trace = execution_trace();
    let mut partial_trace = complete_trace.clone();
    partial_trace.final_payload = None;
    partial_trace.failure_payload = None;

    expect_trace_validation_error(
        &database,
        &partial_trace,
        "must set final_payload or failure_payload",
    )
    .await?;

    let inserted = upsert_execution_trace(database.pool(), &complete_trace).await?;
    assert_eq!(inserted, complete_trace);
    assert_eq!(
        load_execution_trace(database.pool(), complete_trace.execution_trace_id).await?,
        Some(complete_trace)
    );

    database.cleanup().await
}

#[tokio::test]
async fn upserts_and_loads_execution_outcome_by_cache_key() -> Result<()> {
    let database = TestDatabase::new().await?;
    let trace = execution_trace();
    upsert_execution_trace(database.pool(), &trace).await?;

    let outcome = execution_outcome(&trace);
    let inserted = upsert_execution_outcome(database.pool(), &outcome).await?;
    assert_eq!(inserted, outcome);

    let loaded = load_execution_outcome(database.pool(), &outcome.cache_key)
        .await?
        .expect("execution outcome must exist after upsert");
    assert_eq!(loaded, outcome);

    let mut replacement_trace = trace.clone();
    replacement_trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000008);
    replacement_trace.final_payload = Some(json!({
        "record_kind": "addr",
        "coin_type": 60,
        "value": "0x00000000000000000000000000000000000000bb"
    }));
    replacement_trace.finished_at = Some(timestamp(1_717_171_800));
    upsert_execution_trace(database.pool(), &replacement_trace).await?;

    let mut replacement_outcome = outcome.clone();
    replacement_outcome.execution_trace_id = replacement_trace.execution_trace_id;
    replacement_outcome.outcome_payload = Some(json!({
        "verified_queries": [
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000bb"
                }
            }
        ]
    }));
    replacement_outcome.finished_at = replacement_trace
        .finished_at
        .expect("replacement trace must finish");

    let updated = upsert_execution_outcome(database.pool(), &replacement_outcome).await?;
    assert_eq!(updated, replacement_outcome);
    assert_eq!(
        load_execution_outcome(database.pool(), &replacement_outcome.cache_key).await?,
        Some(replacement_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn execution_outcome_cache_key_is_order_insensitive_for_positions_and_manifests() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let trace = execution_trace();
    upsert_execution_trace(database.pool(), &trace).await?;

    let outcome = execution_outcome(&trace);
    upsert_execution_outcome(database.pool(), &outcome).await?;

    let mut reordered_key = outcome.cache_key.clone();
    reordered_key.requested_chain_positions = json!([
        {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_000,
            "block_hash": "0xabc123"
        },
        {
            "chain_id": "base-mainnet",
            "block_number": 17_500_000,
            "block_hash": "0xbase999"
        }
    ]);
    reordered_key.manifest_versions = json!([
        {
            "source_manifest_id": 7,
            "manifest_version": 3
        },
        {
            "source_family": "ens_v1_registry_l1",
            "manifest_version": 3
        }
    ]);

    assert_eq!(
        load_execution_outcome(database.pool(), &reordered_key).await?,
        Some(outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_execution_outcome_with_duplicate_chain_position_or_manifest_identity() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let trace = execution_trace();
    upsert_execution_trace(database.pool(), &trace).await?;

    let mut duplicate_chain = execution_outcome(&trace);
    duplicate_chain.cache_key.requested_chain_positions = json!([
        {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_000,
            "block_hash": "0xabc123"
        },
        {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_001,
            "block_hash": "0xabc124"
        }
    ]);
    expect_outcome_validation_error(
        &database,
        &duplicate_chain,
        "requested_chain_positions must not repeat chain_id ethereum-mainnet",
    )
    .await?;

    let mut duplicate_manifest = execution_outcome(&trace);
    duplicate_manifest.cache_key.manifest_versions = json!([
        {
            "source_manifest_id": 7,
            "manifest_version": 3
        },
        {
            "source_manifest_id": 7,
            "manifest_version": 3
        }
    ]);
    expect_outcome_validation_error(
        &database,
        &duplicate_manifest,
        "manifest_versions must not repeat the same manifest identity",
    )
    .await?;

    database.cleanup().await
}

#[tokio::test]
async fn rejects_execution_outcome_when_same_cache_key_changes_route_identity() -> Result<()> {
    let database = TestDatabase::new().await?;
    let trace = execution_trace();
    upsert_execution_trace(database.pool(), &trace).await?;

    let outcome = execution_outcome(&trace);
    upsert_execution_outcome(database.pool(), &outcome).await?;

    let mut conflicting_trace = trace.clone();
    conflicting_trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000009);
    conflicting_trace.request_type = "verified_primary_name".to_owned();
    conflicting_trace.final_payload = Some(json!({
        "verified_primary_name": {
            "status": "success",
            "name": "alice.eth"
        }
    }));
    upsert_execution_trace(database.pool(), &conflicting_trace).await?;

    let mut conflicting_outcome = execution_outcome(&conflicting_trace);
    conflicting_outcome.cache_key = outcome.cache_key.clone();
    conflicting_outcome.namespace = "basenames".to_owned();

    let error = upsert_execution_outcome(database.pool(), &conflicting_outcome)
        .await
        .expect_err("route identity drift on the same cache key must fail");
    assert!(
        error
            .to_string()
            .contains("execution outcome cache identity mismatch"),
        "unexpected error: {error:#}"
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &outcome.cache_key).await?,
        Some(outcome)
    );

    database.cleanup().await
}
