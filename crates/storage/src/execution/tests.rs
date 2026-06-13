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
use crate::{
    CanonicalityState, ChainLineageBlock, default_database_url, upsert_chain_lineage_blocks,
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn migration_prunes_noncanonical_addr_selector_execution_artifacts() {
    let migration = include_str!(
        "../../../../migrations/20260612133200_noncanonical_addr_selector_cache_cleanup.sql"
    );

    assert!(
        !migration.contains("DELETE FROM public.execution_traces"),
        "migration must retain durable execution trace and step audit artifacts"
    );
    assert!(
        migration.contains("DELETE FROM public.execution_cache_outcomes"),
        "migration may delete noncanonical reusable cache outcomes"
    );
    assert!(
        !migration.contains("DELETE FROM public.record_inventory_current"),
        "migration must not delete declared record-inventory projection rows"
    );
    assert!(
        migration.contains("jsonb_path_query"),
        "migration must match structured record selector fields"
    );
    assert!(
        migration.contains("substring(record_key FROM 6) <>"),
        "migration must match leading-zero addr selectors without deleting canonical addr:0"
    );
    assert!(
        migration.contains("18446744073709551615"),
        "migration must compare digit selectors against u64::MAX"
    );
    assert!(
        migration.contains("string_to_table(selector_part[1], ',')"),
        "migration must parse comma-separated request selectors instead of matching addr text anywhere in the key"
    );
}

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
            "bn_st_execution_{}_{}_{}",
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
            .max_connections(2)
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

fn lineage_block(
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    canonicality_state: CanonicalityState,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: None,
        block_number,
        block_timestamp: timestamp(1_717_180_000 + block_number.rem_euclid(1_000)),
        logs_bloom: None,
        transactions_root: Some(format!("0xtx-{block_hash}")),
        receipts_root: Some(format!("0xrc-{block_hash}")),
        state_root: Some(format!("0xst-{block_hash}")),
        canonicality_state,
    }
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
            "normalizer_version": "ensip15@ens-normalize-0.1.1"
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

fn version_boundary(
    logical_name_id: &str,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> serde_json::Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
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

fn execution_trace_variant(
    execution_trace_id: Uuid,
    request_key: &str,
    finished_at: i64,
) -> ExecutionTrace {
    let mut trace = execution_trace();
    trace.execution_trace_id = execution_trace_id;
    trace.request_key = request_key.to_owned();
    trace.request_metadata = json!({
        "surface": request_key,
        "normalizer_version": "ensip15@ens-normalize-0.1.1"
    });
    trace.finished_at = Some(timestamp(finished_at));
    trace
}

fn execution_outcome_variant(
    trace: &ExecutionTrace,
    manifest_versions: serde_json::Value,
    topology_version_boundary: serde_json::Value,
    record_version_boundary: serde_json::Value,
) -> ExecutionOutcome {
    let mut outcome = execution_outcome(trace);
    outcome.cache_key.request_key = trace.request_key.clone();
    outcome.cache_key.manifest_versions = manifest_versions;
    outcome.cache_key.topology_version_boundary = topology_version_boundary;
    outcome.cache_key.record_version_boundary = record_version_boundary;
    outcome.finished_at = trace
        .finished_at
        .expect("execution trace variant fixture must finish");
    outcome
}

fn verified_primary_request_key(address: &str, coin_type: &str) -> String {
    format!("ens:{}:{coin_type}", address.to_ascii_lowercase())
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

async fn insert_legacy_resolution_cache_row(
    database: &TestDatabase,
    execution_trace_id: Uuid,
    execution_cache_key: &str,
    request_key: &str,
    record_key: &str,
) -> Result<()> {
    let finished_at = timestamp(1_717_173_000);
    let final_payload = json!({
        "verified_queries": [
            {
                "record_key": record_key,
                "status": "not_found",
                "failure_reason": "legacy_test"
            }
        ]
    });
    let requested_chain_positions = json!([
        {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_000,
            "block_hash": "0xabc123"
        }
    ]);
    let manifest_versions = json!([
        {
            "source_manifest_id": 7,
            "manifest_version": 3
        }
    ]);
    let topology_boundary = version_boundary(
        "ens:legacy.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000b001),
        Some(2_000),
        Some("ResolverChanged"),
        21_000_000,
        "0xabc123",
        "2024-06-09T00:00:00Z",
    );
    let record_boundary = version_boundary(
        "ens:legacy.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000b002),
        Some(2_001),
        Some("RecordsChanged"),
        21_000_001,
        "0xabc124",
        "2024-06-09T00:00:01Z",
    );

    sqlx::query(
        r#"
        INSERT INTO execution_traces (
            execution_trace_id,
            request_type,
            request_key,
            namespace,
            chain_context,
            manifest_context,
            contracts_called,
            gateway_digests,
            final_payload,
            request_metadata,
            finished_at
        )
        VALUES (
            $1,
            'verified_resolution',
            $2,
            'ens',
            $3::jsonb,
            $4::jsonb,
            '[]'::jsonb,
            '[]'::jsonb,
            $5::jsonb,
            $6::jsonb,
            $7
        )
        "#,
    )
    .bind(execution_trace_id)
    .bind(request_key)
    .bind(
        json!({
            "requested_positions": requested_chain_positions,
            "topology_version_boundary": topology_boundary,
            "record_version_boundary": record_boundary,
        })
        .to_string(),
    )
    .bind(
        json!({
            "manifest_versions": manifest_versions,
        })
        .to_string(),
    )
    .bind(final_payload.to_string())
    .bind(
        json!({
            "surface": "legacy.eth",
            "record_keys": [record_key],
        })
        .to_string(),
    )
    .bind(finished_at)
    .execute(database.pool())
    .await
    .context("failed to insert legacy execution trace fixture")?;

    sqlx::query(
        r#"
        INSERT INTO execution_cache_outcomes (
            execution_cache_key,
            request_key,
            requested_chain_positions,
            manifest_versions,
            topology_version_boundary,
            record_version_boundary,
            execution_trace_id,
            request_type,
            namespace,
            outcome_payload,
            finished_at
        )
        VALUES (
            $1,
            $2,
            $3::jsonb,
            $4::jsonb,
            $5::jsonb,
            $6::jsonb,
            $7,
            'verified_resolution',
            'ens',
            $8::jsonb,
            $9
        )
        "#,
    )
    .bind(execution_cache_key)
    .bind(request_key)
    .bind(requested_chain_positions.to_string())
    .bind(manifest_versions.to_string())
    .bind(topology_boundary.to_string())
    .bind(record_boundary.to_string())
    .bind(execution_trace_id)
    .bind(final_payload.to_string())
    .bind(finished_at)
    .execute(database.pool())
    .await
    .context("failed to insert legacy execution cache outcome fixture")?;

    Ok(())
}

async fn execution_cache_outcome_count(
    database: &TestDatabase,
    execution_cache_key: &str,
) -> Result<i64> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_cache_outcomes WHERE execution_cache_key = $1",
    )
    .bind(execution_cache_key)
    .fetch_one(database.pool())
    .await
    .with_context(|| format!("failed to count execution cache outcome {execution_cache_key}"))
}

#[tokio::test]
async fn noncanonical_addr_selector_cleanup_prunes_only_reusable_resolution_outcomes() -> Result<()>
{
    let database = TestDatabase::new().await?;
    insert_legacy_resolution_cache_row(
        &database,
        Uuid::from_u128(0x0e7ec7ace00000000000000000001001),
        "legacy-leading-zero-addr-selector",
        "ens:legacy.eth:addr:060",
        "addr:060",
    )
    .await?;
    insert_legacy_resolution_cache_row(
        &database,
        Uuid::from_u128(0x0e7ec7ace00000000000000000001002),
        "legacy-overflow-addr-selector",
        "ens:legacy.eth:addr:18446744073709551616",
        "addr:18446744073709551616",
    )
    .await?;
    insert_legacy_resolution_cache_row(
        &database,
        Uuid::from_u128(0x0e7ec7ace00000000000000000001003),
        "legacy-text-selector-containing-addr-text",
        "ens:legacy.eth:text:xaddr:060",
        "text:xaddr:060",
    )
    .await?;
    insert_legacy_resolution_cache_row(
        &database,
        Uuid::from_u128(0x0e7ec7ace00000000000000000001004),
        "legacy-canonical-addr-selector",
        "ens:legacy.eth:addr:60",
        "addr:60",
    )
    .await?;

    sqlx::raw_sql(include_str!(
        "../../../../migrations/20260612133200_noncanonical_addr_selector_cache_cleanup.sql"
    ))
    .execute(database.pool())
    .await
    .context("failed to rerun noncanonical addr selector cleanup migration")?;

    assert_eq!(
        execution_cache_outcome_count(&database, "legacy-leading-zero-addr-selector").await?,
        0,
        "leading-zero addr selector outcomes are not byte-comparable with canonical request keys"
    );
    assert_eq!(
        execution_cache_outcome_count(&database, "legacy-overflow-addr-selector").await?,
        0,
        "overflowing addr selector outcomes are outside bigname's public selector grammar"
    );
    assert_eq!(
        execution_cache_outcome_count(&database, "legacy-text-selector-containing-addr-text")
            .await?,
        1,
        "text selectors that merely contain addr-like text must not be pruned"
    );
    assert_eq!(
        execution_cache_outcome_count(&database, "legacy-canonical-addr-selector").await?,
        1,
        "canonical addr selector outcomes remain reusable"
    );

    let trace_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_traces WHERE execution_trace_id IN ($1, $2, $3, $4)",
    )
    .bind(Uuid::from_u128(0x0e7ec7ace00000000000000000001001))
    .bind(Uuid::from_u128(0x0e7ec7ace00000000000000000001002))
    .bind(Uuid::from_u128(0x0e7ec7ace00000000000000000001003))
    .bind(Uuid::from_u128(0x0e7ec7ace00000000000000000001004))
    .fetch_one(database.pool())
    .await
    .context("failed to count durable legacy execution trace fixtures")?;
    assert_eq!(
        trace_count, 4,
        "cleanup must retain durable execution trace audit artifacts"
    );

    database.cleanup().await
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
            topology_version_boundary: version_boundary(
                "ens:alice.eth",
                Uuid::from_u128(0x0e7ec7ace0000000000000000000aaa1),
                Some(1_200),
                Some("RecordsChanged"),
                21_000_000,
                "0xabc123",
                "2024-06-01T00:00:17Z",
            ),
            record_version_boundary: version_boundary(
                "ens:alice.eth",
                Uuid::from_u128(0x0e7ec7ace0000000000000000000aaa2),
                Some(1_200),
                Some("RecordsChanged"),
                21_000_000,
                "0xabc123",
                "2024-06-01T00:00:17Z",
            ),
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
async fn loads_execution_trace_inspection_without_mutating_execution_storage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let trace = execution_trace();
    let outcome = execution_outcome(&trace);
    insert_trace_and_outcome(&database, &trace, &outcome).await?;

    let before_counts = execution_storage_counts(database.pool()).await?;
    let inspection = load_execution_trace_inspection(database.pool(), trace.execution_trace_id)
        .await?
        .expect("execution trace inspection must exist");
    let after_counts = execution_storage_counts(database.pool()).await?;

    assert_eq!(inspection.trace, trace);
    assert_eq!(after_counts, before_counts);
    assert_eq!(
        load_execution_outcome(database.pool(), &outcome.cache_key).await?,
        Some(outcome),
        "execution trace inspection must not mutate cache outcomes"
    );
    assert_eq!(
        inspection
            .trace
            .steps
            .iter()
            .map(|step| step.step_index)
            .collect::<Vec<_>>(),
        vec![0, 1],
        "execution trace inspection must preserve persisted step order"
    );

    database.cleanup().await
}

#[tokio::test]
async fn load_execution_trace_inspection_returns_none_for_missing_trace() -> Result<()> {
    let database = TestDatabase::new().await?;
    assert!(
        load_execution_trace_inspection(
            database.pool(),
            Uuid::from_u128(0x0e7ec7ace00000000000000000999999),
        )
        .await?
        .is_none()
    );

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

async fn execution_storage_counts(pool: &PgPool) -> Result<(i64, i64, i64)> {
    let trace_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution_traces")
        .fetch_one(pool)
        .await
        .context("failed to count execution_traces")?;
    let step_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution_steps")
        .fetch_one(pool)
        .await
        .context("failed to count execution_steps")?;
    let outcome_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution_cache_outcomes")
        .fetch_one(pool)
        .await
        .context("failed to count execution_cache_outcomes")?;

    Ok((trace_count, step_count, outcome_count))
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
async fn rejects_execution_trace_with_missing_step_latency() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut trace = execution_trace();
    trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000008);
    trace.steps[0].latency_ms = None;

    expect_trace_validation_error(&database, &trace, "must set latency_ms").await?;

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
async fn transaction_scoped_execution_upserts_stay_pending_until_commit() -> Result<()> {
    let database = TestDatabase::new().await?;
    let trace = execution_trace();
    let outcome = execution_outcome(&trace);

    let mut transaction = database.pool().begin().await?;
    let inserted_trace = upsert_execution_trace_in_transaction(&mut transaction, &trace).await?;
    let inserted_outcome =
        upsert_execution_outcome_in_transaction(&mut transaction, &outcome).await?;
    assert_eq!(inserted_trace, trace);
    assert_eq!(inserted_outcome, outcome);
    assert!(
        load_execution_trace(database.pool(), trace.execution_trace_id)
            .await?
            .is_none(),
        "transaction-scoped trace upsert must remain invisible before commit"
    );
    assert!(
        load_execution_outcome(database.pool(), &outcome.cache_key)
            .await?
            .is_none(),
        "transaction-scoped outcome upsert must remain invisible before commit"
    );

    transaction.commit().await?;

    assert_eq!(
        load_execution_trace(database.pool(), trace.execution_trace_id).await?,
        Some(trace)
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &outcome.cache_key).await?,
        Some(outcome)
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

#[tokio::test]
async fn invalidates_execution_outcomes_for_exact_manifest_version_only() -> Result<()> {
    let database = TestDatabase::new().await?;

    let stale_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000010),
        "ens:alice.eth:addr:60",
        1_717_171_900,
    );
    let stale_outcome = execution_outcome_variant(
        &stale_trace,
        json!([
            {
                "source_manifest_id": 7,
                "manifest_version": 3
            },
            {
                "source_family": "ens_execution",
                "manifest_version": 5
            }
        ]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000bbb1),
            Some(1_210),
            Some("ResolverChanged"),
            21_000_010,
            "0xaaa010",
            "2024-06-01T00:00:27Z",
        ),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000ccc1),
            Some(1_220),
            Some("RecordsChanged"),
            21_000_011,
            "0xaaa011",
            "2024-06-01T00:00:28Z",
        ),
    );
    insert_trace_and_outcome(&database, &stale_trace, &stale_outcome).await?;

    let current_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000011),
        "ens:bob.eth:addr:60",
        1_717_171_901,
    );
    let current_outcome = execution_outcome_variant(
        &current_trace,
        json!([
            {
                "source_manifest_id": 7,
                "manifest_version": 4
            },
            {
                "source_family": "ens_execution",
                "manifest_version": 5
            }
        ]),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000bbb2),
            Some(1_230),
            Some("ResolverChanged"),
            21_000_020,
            "0xbbb020",
            "2024-06-01T00:00:37Z",
        ),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000ccc2),
            Some(1_240),
            Some("RecordsChanged"),
            21_000_021,
            "0xbbb021",
            "2024-06-01T00:00:38Z",
        ),
    );
    insert_trace_and_outcome(&database, &current_trace, &current_outcome).await?;

    let mut other_route_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000012),
        "ens:charlie.eth:addr:60",
        1_717_171_902,
    );
    other_route_trace.request_type = "verified_primary_name".to_owned();
    let other_route_outcome = execution_outcome_variant(
        &other_route_trace,
        json!([{
            "source_manifest_id": 7,
            "manifest_version": 3
        }]),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000bbb3),
            Some(1_250),
            Some("ResolverChanged"),
            21_000_030,
            "0xccc030",
            "2024-06-01T00:00:47Z",
        ),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000ccc3),
            Some(1_260),
            Some("RecordsChanged"),
            21_000_031,
            "0xccc031",
            "2024-06-01T00:00:48Z",
        ),
    );
    insert_trace_and_outcome(&database, &other_route_trace, &other_route_outcome).await?;

    let summary = invalidate_execution_outcomes_for_manifest_version(
        database.pool(),
        &ExecutionManifestInvalidation {
            request_type: "verified_resolution".to_owned(),
            namespace: "ens".to_owned(),
            source_manifest_id: Some(7),
            source_family: None,
            manifest_version: 3,
        },
    )
    .await?;
    assert_eq!(summary.deleted_outcome_count, 1);

    assert_eq!(
        load_execution_outcome(database.pool(), &stale_outcome.cache_key).await?,
        None
    );
    assert!(
        load_execution_trace(database.pool(), stale_trace.execution_trace_id)
            .await?
            .is_some(),
        "execution traces stay durable after cache invalidation"
    );
    assert!(
        load_execution_outcome(database.pool(), &current_outcome.cache_key)
            .await?
            .is_some(),
        "non-matching manifest version must remain cached"
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &other_route_outcome.cache_key).await?,
        Some(other_route_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn invalidates_execution_outcomes_for_exact_topology_boundary_only() -> Result<()> {
    let database = TestDatabase::new().await?;

    let target_boundary = version_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000ddd1),
        Some(1_310),
        Some("ResolverChanged"),
        21_100_010,
        "0xddd010",
        "2024-06-02T00:00:27Z",
    );
    let record_boundary = version_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000eee1),
        Some(1_320),
        Some("RecordsChanged"),
        21_100_011,
        "0xddd011",
        "2024-06-02T00:00:28Z",
    );

    let target_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000013),
        "ens:alice.eth:text",
        1_717_172_000,
    );
    let target_outcome = execution_outcome_variant(
        &target_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 5
        }]),
        target_boundary.clone(),
        record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let keep_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000014),
        "ens:bob.eth:text",
        1_717_172_001,
    );
    let keep_outcome = execution_outcome_variant(
        &keep_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 5
        }]),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000ddd2),
            Some(1_330),
            Some("ResolverChanged"),
            21_100_020,
            "0xeee020",
            "2024-06-02T00:00:37Z",
        ),
        record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &keep_trace, &keep_outcome).await?;

    let mut other_namespace_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000015),
        "base:alice.base.eth:text",
        1_717_172_002,
    );
    other_namespace_trace.namespace = "basenames".to_owned();
    let other_namespace_outcome = execution_outcome_variant(
        &other_namespace_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 5
        }]),
        target_boundary.clone(),
        record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &other_namespace_trace, &other_namespace_outcome).await?;

    let summary = invalidate_execution_outcomes_for_topology_boundary(
        database.pool(),
        &ExecutionBoundaryInvalidation {
            request_type: "verified_resolution".to_owned(),
            namespace: "ens".to_owned(),
            boundary: target_boundary,
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
    assert_eq!(
        load_execution_outcome(database.pool(), &other_namespace_outcome.cache_key).await?,
        Some(other_namespace_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn invalidates_execution_outcomes_for_exact_record_boundary_only() -> Result<()> {
    let database = TestDatabase::new().await?;

    let topology_boundary = version_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000fff1),
        Some(1_410),
        Some("ResolverChanged"),
        21_200_010,
        "0xfff010",
        "2024-06-03T00:00:27Z",
    );
    let target_record_boundary = version_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000aaa3),
        Some(1_420),
        Some("RecordsChanged"),
        21_200_011,
        "0xfff011",
        "2024-06-03T00:00:28Z",
    );

    let target_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000016),
        "ens:alice.eth:addr:60",
        1_717_172_100,
    );
    let target_outcome = execution_outcome_variant(
        &target_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 6
        }]),
        topology_boundary.clone(),
        target_record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let keep_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000017),
        "ens:alice.eth:text",
        1_717_172_101,
    );
    let keep_outcome = execution_outcome_variant(
        &keep_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 6
        }]),
        topology_boundary,
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000aaa4),
            Some(1_430),
            Some("RecordsChanged"),
            21_200_020,
            "0xaaa020",
            "2024-06-03T00:00:37Z",
        ),
    );
    insert_trace_and_outcome(&database, &keep_trace, &keep_outcome).await?;

    let mut other_route_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000018),
        "ens:alice.eth:contenthash",
        1_717_172_102,
    );
    other_route_trace.request_type = "verified_primary_name".to_owned();
    let other_route_outcome = execution_outcome_variant(
        &other_route_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 6
        }]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000fff2),
            Some(1_440),
            Some("ResolverChanged"),
            21_200_030,
            "0xaaa030",
            "2024-06-03T00:00:47Z",
        ),
        target_record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &other_route_trace, &other_route_outcome).await?;

    let summary = invalidate_execution_outcomes_for_record_boundary(
        database.pool(),
        &ExecutionBoundaryInvalidation {
            request_type: "verified_resolution".to_owned(),
            namespace: "ens".to_owned(),
            boundary: target_record_boundary,
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
    assert_eq!(
        load_execution_outcome(database.pool(), &other_route_outcome.cache_key).await?,
        Some(other_route_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn invalidates_verified_primary_execution_outcomes_for_exact_manifest_and_request_key_only()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let target_request_key = verified_primary_request_key("0xAbCd", "60");

    let mut target_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000019),
        &target_request_key,
        1_717_172_200,
    );
    target_trace.request_type = "verified_primary_name".to_owned();
    let target_outcome = execution_outcome_variant(
        &target_trace,
        json!([{
            "source_manifest_id": 31,
            "manifest_version": 4
        }]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc1),
            Some(1_510),
            Some("ResolverChanged"),
            21_300_010,
            "0xabd010",
            "2024-06-04T00:00:27Z",
        ),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc2),
            Some(1_520),
            Some("RecordsChanged"),
            21_300_011,
            "0xabd011",
            "2024-06-04T00:00:28Z",
        ),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let mut other_tuple_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace0000000000000000000001a),
        &verified_primary_request_key("0xEf01", "60"),
        1_717_172_201,
    );
    other_tuple_trace.request_type = "verified_primary_name".to_owned();
    let other_tuple_outcome = execution_outcome_variant(
        &other_tuple_trace,
        json!([{
            "source_manifest_id": 31,
            "manifest_version": 4
        }]),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc3),
            Some(1_530),
            Some("ResolverChanged"),
            21_300_020,
            "0xabd020",
            "2024-06-04T00:00:37Z",
        ),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc4),
            Some(1_540),
            Some("RecordsChanged"),
            21_300_021,
            "0xabd021",
            "2024-06-04T00:00:38Z",
        ),
    );
    insert_trace_and_outcome(&database, &other_tuple_trace, &other_tuple_outcome).await?;

    let resolution_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace0000000000000000000001b),
        &target_request_key,
        1_717_172_202,
    );
    let resolution_outcome = execution_outcome_variant(
        &resolution_trace,
        json!([{
            "source_manifest_id": 31,
            "manifest_version": 4
        }]),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc5),
            Some(1_550),
            Some("ResolverChanged"),
            21_300_030,
            "0xabd030",
            "2024-06-04T00:00:47Z",
        ),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc6),
            Some(1_560),
            Some("RecordsChanged"),
            21_300_031,
            "0xabd031",
            "2024-06-04T00:00:48Z",
        ),
    );
    insert_trace_and_outcome(&database, &resolution_trace, &resolution_outcome).await?;

    let summary = invalidate_execution_outcomes_for_manifest_version_and_request_key(
        database.pool(),
        &ExecutionManifestInvalidation {
            request_type: "verified_primary_name".to_owned(),
            namespace: "ens".to_owned(),
            source_manifest_id: Some(31),
            source_family: None,
            manifest_version: 4,
        },
        &target_request_key,
    )
    .await?;
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
        load_execution_outcome(database.pool(), &resolution_outcome.cache_key).await?,
        Some(resolution_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn invalidates_verified_primary_execution_outcomes_for_exact_topology_and_request_key_only()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let target_request_key = verified_primary_request_key("0xAbCd", "60");
    let target_boundary = version_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000abc7),
        Some(1_610),
        Some("ResolverChanged"),
        21_400_010,
        "0xabe010",
        "2024-06-05T00:00:27Z",
    );

    let mut target_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace0000000000000000000001c),
        &target_request_key,
        1_717_172_300,
    );
    target_trace.request_type = "verified_primary_name".to_owned();
    let target_outcome = execution_outcome_variant(
        &target_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 7
        }]),
        target_boundary.clone(),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc8),
            Some(1_620),
            Some("RecordsChanged"),
            21_400_011,
            "0xabe011",
            "2024-06-05T00:00:28Z",
        ),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let mut other_tuple_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace0000000000000000000001d),
        &verified_primary_request_key("0xEf01", "60"),
        1_717_172_301,
    );
    other_tuple_trace.request_type = "verified_primary_name".to_owned();
    let other_tuple_outcome = execution_outcome_variant(
        &other_tuple_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 7
        }]),
        target_boundary.clone(),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc9),
            Some(1_630),
            Some("RecordsChanged"),
            21_400_021,
            "0xabe021",
            "2024-06-05T00:00:38Z",
        ),
    );
    insert_trace_and_outcome(&database, &other_tuple_trace, &other_tuple_outcome).await?;

    let resolution_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace0000000000000000000001e),
        &target_request_key,
        1_717_172_302,
    );
    let resolution_outcome = execution_outcome_variant(
        &resolution_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 7
        }]),
        target_boundary.clone(),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abca),
            Some(1_640),
            Some("RecordsChanged"),
            21_400_031,
            "0xabe031",
            "2024-06-05T00:00:48Z",
        ),
    );
    insert_trace_and_outcome(&database, &resolution_trace, &resolution_outcome).await?;

    let summary = invalidate_execution_outcomes_for_topology_boundary_and_request_key(
        database.pool(),
        &ExecutionBoundaryInvalidation {
            request_type: "verified_primary_name".to_owned(),
            namespace: "ens".to_owned(),
            boundary: target_boundary,
        },
        &target_request_key,
    )
    .await?;
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
        load_execution_outcome(database.pool(), &resolution_outcome.cache_key).await?,
        Some(resolution_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn invalidates_verified_primary_execution_outcomes_for_exact_record_and_request_key_only()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let target_request_key = verified_primary_request_key("0xAbCd", "60");
    let target_record_boundary = version_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000abcb),
        Some(1_710),
        Some("RecordsChanged"),
        21_500_010,
        "0xabf010",
        "2024-06-06T00:00:27Z",
    );

    let mut target_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace0000000000000000000001f),
        &target_request_key,
        1_717_172_400,
    );
    target_trace.request_type = "verified_primary_name".to_owned();
    let target_outcome = execution_outcome_variant(
        &target_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 8
        }]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abcc),
            Some(1_720),
            Some("ResolverChanged"),
            21_500_011,
            "0xabf011",
            "2024-06-06T00:00:28Z",
        ),
        target_record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let mut other_tuple_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000020),
        &verified_primary_request_key("0xEf01", "60"),
        1_717_172_401,
    );
    other_tuple_trace.request_type = "verified_primary_name".to_owned();
    let other_tuple_outcome = execution_outcome_variant(
        &other_tuple_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 8
        }]),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abcd),
            Some(1_730),
            Some("ResolverChanged"),
            21_500_021,
            "0xabf021",
            "2024-06-06T00:00:38Z",
        ),
        target_record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &other_tuple_trace, &other_tuple_outcome).await?;

    let resolution_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000021),
        &target_request_key,
        1_717_172_402,
    );
    let resolution_outcome = execution_outcome_variant(
        &resolution_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 8
        }]),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abce),
            Some(1_740),
            Some("ResolverChanged"),
            21_500_031,
            "0xabf031",
            "2024-06-06T00:00:48Z",
        ),
        target_record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &resolution_trace, &resolution_outcome).await?;

    let summary = invalidate_execution_outcomes_for_record_boundary_and_request_key(
        database.pool(),
        &ExecutionBoundaryInvalidation {
            request_type: "verified_primary_name".to_owned(),
            namespace: "ens".to_owned(),
            boundary: target_record_boundary,
        },
        &target_request_key,
    )
    .await?;
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
        load_execution_outcome(database.pool(), &resolution_outcome.cache_key).await?,
        Some(resolution_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn invalidates_verified_execution_outcomes_for_orphaned_block_dependencies() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block(
                "ethereum-mainnet",
                "0xrequested-orphan",
                21_600_001,
                CanonicalityState::Orphaned,
            ),
            lineage_block(
                "ethereum-mainnet",
                "0xtopology-orphan",
                21_600_002,
                CanonicalityState::Orphaned,
            ),
            lineage_block(
                "ethereum-mainnet",
                "0xrecord-orphan",
                21_600_003,
                CanonicalityState::Orphaned,
            ),
            lineage_block(
                "ethereum-mainnet",
                "0xcanonical-keep",
                21_600_004,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let requested_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000022),
        "ens:requested.eth:addr:60",
        1_717_172_500,
    );
    let mut requested_outcome = execution_outcome_variant(
        &requested_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 9
        }]),
        version_boundary(
            "ens:requested.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000acd1),
            Some(1_810),
            Some("ResolverChanged"),
            21_600_010,
            "0xrequested-topology-keep",
            "2024-06-07T00:00:27Z",
        ),
        version_boundary(
            "ens:requested.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000acd2),
            Some(1_820),
            Some("RecordsChanged"),
            21_600_011,
            "0xrequested-record-keep",
            "2024-06-07T00:00:28Z",
        ),
    );
    requested_outcome.cache_key.requested_chain_positions = json!([{
        "chain_id": "ethereum-mainnet",
        "block_number": 21_600_001,
        "block_hash": "0xrequested-orphan"
    }]);
    insert_trace_and_outcome(&database, &requested_trace, &requested_outcome).await?;

    let topology_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000023),
        "ens:topology.eth:text",
        1_717_172_501,
    );
    let topology_outcome = execution_outcome_variant(
        &topology_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 9
        }]),
        version_boundary(
            "ens:topology.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000acd3),
            Some(1_830),
            Some("ResolverChanged"),
            21_600_002,
            "0xtopology-orphan",
            "2024-06-07T00:00:29Z",
        ),
        version_boundary(
            "ens:topology.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000acd4),
            Some(1_840),
            Some("RecordsChanged"),
            21_600_021,
            "0xtopology-record-keep",
            "2024-06-07T00:00:30Z",
        ),
    );
    insert_trace_and_outcome(&database, &topology_trace, &topology_outcome).await?;

    let record_request_key = verified_primary_request_key("0xAbCd1122", "60");
    let mut record_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000024),
        &record_request_key,
        1_717_172_502,
    );
    record_trace.request_type = "verified_primary_name".to_owned();
    let record_outcome = execution_outcome_variant(
        &record_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 9
        }]),
        version_boundary(
            "ens:record.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000acd5),
            Some(1_850),
            Some("ResolverChanged"),
            21_600_031,
            "0xrecord-topology-keep",
            "2024-06-07T00:00:31Z",
        ),
        version_boundary(
            "ens:record.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000acd6),
            Some(1_860),
            Some("RecordsChanged"),
            21_600_003,
            "0xrecord-orphan",
            "2024-06-07T00:00:32Z",
        ),
    );
    insert_trace_and_outcome(&database, &record_trace, &record_outcome).await?;

    let keep_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000025),
        "ens:keep.eth:addr:60",
        1_717_172_503,
    );
    let keep_outcome = execution_outcome_variant(
        &keep_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 9
        }]),
        version_boundary(
            "ens:keep.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000acd7),
            Some(1_870),
            Some("ResolverChanged"),
            21_600_004,
            "0xcanonical-keep",
            "2024-06-07T00:00:33Z",
        ),
        version_boundary(
            "ens:keep.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000acd8),
            Some(1_880),
            Some("RecordsChanged"),
            21_600_041,
            "0xkeep-record",
            "2024-06-07T00:00:34Z",
        ),
    );
    insert_trace_and_outcome(&database, &keep_trace, &keep_outcome).await?;

    let mut out_of_scope_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000026),
        "ens:declared.eth:addr:60",
        1_717_172_504,
    );
    out_of_scope_trace.request_type = "declared_resolution".to_owned();
    let mut out_of_scope_outcome = execution_outcome_variant(
        &out_of_scope_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 9
        }]),
        version_boundary(
            "ens:declared.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000acd9),
            Some(1_890),
            Some("ResolverChanged"),
            21_600_051,
            "0xdeclared-topology",
            "2024-06-07T00:00:35Z",
        ),
        version_boundary(
            "ens:declared.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000acda),
            Some(1_900),
            Some("RecordsChanged"),
            21_600_052,
            "0xdeclared-record",
            "2024-06-07T00:00:36Z",
        ),
    );
    out_of_scope_outcome.cache_key.requested_chain_positions = json!([{
        "chain_id": "ethereum-mainnet",
        "block_number": 21_600_001,
        "block_hash": "0xrequested-orphan"
    }]);
    insert_trace_and_outcome(&database, &out_of_scope_trace, &out_of_scope_outcome).await?;

    let summary = invalidate_execution_outcomes_for_orphaned_blocks(database.pool()).await?;
    assert_eq!(summary.deleted_outcome_count, 3);

    assert_eq!(
        load_execution_outcome(database.pool(), &requested_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &topology_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &record_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &keep_outcome.cache_key).await?,
        Some(keep_outcome)
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &out_of_scope_outcome.cache_key).await?,
        Some(out_of_scope_outcome)
    );
    assert!(
        load_execution_trace(database.pool(), requested_trace.execution_trace_id)
            .await?
            .is_some(),
        "execution traces stay durable after reorg cache invalidation"
    );

    let trace_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution_traces")
        .fetch_one(database.pool())
        .await
        .context("failed to count traces after reorg cache invalidation")?;
    assert_eq!(trace_count, 5);
    let step_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution_steps")
        .fetch_one(database.pool())
        .await
        .context("failed to count steps after reorg cache invalidation")?;
    assert_eq!(step_count, 10);

    database.cleanup().await
}

#[tokio::test]
async fn reorg_invalidation_fails_closed_for_verified_outcome_without_block_hash_dependency()
-> Result<()> {
    let database = TestDatabase::new().await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[lineage_block(
            "ethereum-mainnet",
            "0xmalformed-orphan",
            21_700_001,
            CanonicalityState::Orphaned,
        )],
    )
    .await?;

    let trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000027),
        "ens:malformed.eth:addr:60",
        1_717_172_600,
    );
    upsert_execution_trace(database.pool(), &trace).await?;

    let topology_boundary = version_boundary(
        "ens:malformed.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000acdb),
        Some(1_910),
        Some("ResolverChanged"),
        21_700_010,
        "0xmalformed-topology",
        "2024-06-08T00:00:27Z",
    );
    let record_boundary = version_boundary(
        "ens:malformed.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000acdc),
        Some(1_920),
        Some("RecordsChanged"),
        21_700_011,
        "0xmalformed-record",
        "2024-06-08T00:00:28Z",
    );
    sqlx::query(
        r#"
        INSERT INTO execution_cache_outcomes (
            execution_cache_key,
            request_key,
            requested_chain_positions,
            manifest_versions,
            topology_version_boundary,
            record_version_boundary,
            execution_trace_id,
            request_type,
            namespace,
            outcome_payload,
            finished_at
        )
        VALUES (
            $1,
            $2,
            $3::jsonb,
            $4::jsonb,
            $5::jsonb,
            $6::jsonb,
            $7,
            $8,
            $9,
            $10::jsonb,
            $11
        )
        "#,
    )
    .bind("malformed-cache-key")
    .bind(&trace.request_key)
    .bind(
        json!([{
            "chain_id": "ethereum-mainnet",
            "block_number": 21_700_001
        }])
        .to_string(),
    )
    .bind(
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 10
        }])
        .to_string(),
    )
    .bind(topology_boundary.to_string())
    .bind(record_boundary.to_string())
    .bind(trace.execution_trace_id)
    .bind("verified_resolution")
    .bind("ens")
    .bind(json!({"status": "success"}).to_string())
    .bind(
        trace
            .finished_at
            .expect("malformed dependency trace must finish"),
    )
    .execute(database.pool())
    .await
    .context("failed to insert malformed execution cache outcome")?;

    let mut primary_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000028),
        &verified_primary_request_key("0xMalformed", "60"),
        1_717_172_601,
    );
    primary_trace.request_type = "verified_primary_name".to_owned();
    upsert_execution_trace(database.pool(), &primary_trace).await?;

    let primary_topology_boundary = json!({
        "logical_name_id": "ens:malformed-primary.eth",
        "resource_id": Uuid::from_u128(0x0e7ec7ace0000000000000000000acdd).to_string(),
        "normalized_event_id": 1_930,
        "event_kind": "ResolverChanged",
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_700_012,
            "timestamp": "2024-06-08T00:00:29Z",
        }
    });
    let primary_record_boundary = json!({
        "logical_name_id": "ens:malformed-primary.eth",
        "resource_id": Uuid::from_u128(0x0e7ec7ace0000000000000000000acde).to_string(),
        "normalized_event_id": 1_940,
        "event_kind": "RecordsChanged",
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_700_013,
            "timestamp": "2024-06-08T00:00:30Z",
        }
    });
    sqlx::query(
        r#"
        INSERT INTO execution_cache_outcomes (
            execution_cache_key,
            request_key,
            requested_chain_positions,
            manifest_versions,
            topology_version_boundary,
            record_version_boundary,
            execution_trace_id,
            request_type,
            namespace,
            outcome_payload,
            finished_at
        )
        VALUES (
            $1,
            $2,
            $3::jsonb,
            $4::jsonb,
            $5::jsonb,
            $6::jsonb,
            $7,
            $8,
            $9,
            $10::jsonb,
            $11
        )
        "#,
    )
    .bind("malformed-primary-cache-key")
    .bind(&primary_trace.request_key)
    .bind(
        json!([{
            "chain_id": "ethereum-mainnet",
            "block_number": 21_700_012
        }])
        .to_string(),
    )
    .bind(
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 10
        }])
        .to_string(),
    )
    .bind(primary_topology_boundary.to_string())
    .bind(primary_record_boundary.to_string())
    .bind(primary_trace.execution_trace_id)
    .bind("verified_primary_name")
    .bind("ens")
    .bind(json!({"status": "success"}).to_string())
    .bind(
        primary_trace
            .finished_at
            .expect("malformed primary dependency trace must finish"),
    )
    .execute(database.pool())
    .await
    .context("failed to insert malformed verified primary-name cache outcome")?;

    let summary = invalidate_execution_outcomes_for_orphaned_blocks(database.pool()).await?;
    assert_eq!(summary.deleted_outcome_count, 2);

    let malformed_outcome_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_cache_outcomes WHERE execution_cache_key IN ($1, $2)",
    )
    .bind("malformed-cache-key")
    .bind("malformed-primary-cache-key")
    .fetch_one(database.pool())
    .await
    .context("failed to count malformed execution cache outcomes after reorg invalidation")?;
    assert_eq!(malformed_outcome_count, 0);

    assert_eq!(
        load_execution_trace(database.pool(), trace.execution_trace_id)
            .await?
            .expect("malformed resolution trace must remain durable")
            .steps
            .len(),
        2,
        "resolution trace steps stay durable after fail-closed cache invalidation"
    );
    assert_eq!(
        load_execution_trace(database.pool(), primary_trace.execution_trace_id)
            .await?
            .expect("malformed verified primary-name trace must remain durable")
            .steps
            .len(),
        2,
        "verified primary-name trace steps stay durable after fail-closed cache invalidation"
    );

    database.cleanup().await
}
