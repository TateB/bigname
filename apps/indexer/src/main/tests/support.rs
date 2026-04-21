use std::{
    fs,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use bigname_manifests::load_discovery_admission_state;
use bigname_storage::{
    ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, ExecutionTraceStep, NameSurface, Resource,
    SurfaceBinding, SurfaceBindingKind, TokenLineage, default_database_url, load_execution_outcome,
    load_execution_trace, upsert_execution_outcome, upsert_execution_trace, upsert_name_surfaces,
    upsert_resources, upsert_surface_bindings, upsert_token_lineages,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::{Uuid, time::OffsetDateTime},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

use super::*;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestManifestDir {
    path: PathBuf,
}

impl TestManifestDir {
    fn new() -> Result<Self> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "bigname-indexer-manifests-tests-{}-{unique}-{sequence}",
            std::process::id(),
        ));
        fs::create_dir_all(&path)
            .with_context(|| format!("failed to create test directory {}", path.display()))?;
        Ok(Self { path })
    }

    fn write_manifest(&self, contents: &str) -> Result<PathBuf> {
        self.write_manifest_for_source_family("ens_v2_registry_l1", contents)
    }

    fn write_manifest_for_source_family(
        &self,
        source_family: &str,
        contents: &str,
    ) -> Result<PathBuf> {
        self.write_manifest_for_namespace_source_family("ens", source_family, contents)
    }

    fn write_manifest_for_namespace_source_family(
        &self,
        namespace: &str,
        source_family: &str,
        contents: &str,
    ) -> Result<PathBuf> {
        let directory = self.path.join(namespace).join(source_family);
        fs::create_dir_all(&directory)
            .with_context(|| format!("failed to create {}", directory.display()))?;
        let path = directory.join("v1.toml");
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }
}

impl Drop for TestManifestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
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
            .context("failed to parse database URL for indexer tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_indexer_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for indexer tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect indexer test pool")?;

        sqlx::query(
            r#"
                CREATE TYPE canonicality_state AS ENUM (
                    'observed',
                    'canonical',
                    'safe',
                    'finalized',
                    'orphaned'
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create canonicality_state type for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TYPE manifest_rollout_status AS ENUM (
                    'draft',
                    'shadow',
                    'active',
                    'deprecated'
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create manifest_rollout_status type for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TYPE capability_support_status AS ENUM (
                    'unsupported',
                    'shadow',
                    'supported'
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create capability_support_status type for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE manifest_versions (
                    manifest_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    manifest_version BIGINT NOT NULL DEFAULT 1,
                    namespace TEXT NOT NULL DEFAULT 'ens',
                    source_family TEXT NOT NULL DEFAULT 'ens_test',
                    chain TEXT NOT NULL,
                    deployment_epoch TEXT NOT NULL DEFAULT 'bootstrap',
                    rollout_status manifest_rollout_status NOT NULL,
                    normalizer_version TEXT NOT NULL DEFAULT 'uts46-v1',
                    file_path TEXT NOT NULL DEFAULT 'tests/v1.toml',
                    manifest_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (namespace, source_family, chain, deployment_epoch, manifest_version)
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create manifest_versions table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE contract_instances (
                    contract_instance_id UUID PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    contract_kind TEXT NOT NULL,
                    provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create contract_instances table for indexer tests")?;
        sqlx::query(
                r#"
                CREATE TABLE contract_instance_addresses (
                    contract_instance_address_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    contract_instance_id UUID NOT NULL REFERENCES contract_instances (contract_instance_id) ON DELETE CASCADE,
                    chain_id TEXT NOT NULL,
                    address TEXT NOT NULL,
                    admitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    deactivated_at TIMESTAMPTZ,
                    active_from_block_number BIGINT,
                    active_from_block_hash TEXT,
                    active_to_block_number BIGINT,
                    active_to_block_hash TEXT,
                    source_manifest_id BIGINT REFERENCES manifest_versions (manifest_id) ON DELETE SET NULL,
                    provenance JSONB NOT NULL DEFAULT '{}'::jsonb
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create contract_instance_addresses table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE UNIQUE INDEX contract_instance_addresses_active_instance_idx
                ON contract_instance_addresses (contract_instance_id)
                WHERE deactivated_at IS NULL
                "#,
        )
        .execute(&pool)
        .await
        .context(
            "failed to create contract_instance_addresses_active_instance_idx for indexer tests",
        )?;
        sqlx::query(
            r#"
                CREATE INDEX contract_instance_addresses_active_address_idx
                ON contract_instance_addresses (chain_id, address)
                WHERE deactivated_at IS NULL
                "#,
        )
        .execute(&pool)
        .await
        .context(
            "failed to create contract_instance_addresses_active_address_idx for indexer tests",
        )?;
        sqlx::query(
            r#"
                CREATE TABLE manifest_roots (
                    manifest_id BIGINT NOT NULL,
                    name TEXT NOT NULL DEFAULT 'RootRegistry',
                    address TEXT NOT NULL,
                    code_hash TEXT,
                    abi_ref TEXT
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create manifest_roots table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE manifest_contracts (
                    manifest_id BIGINT NOT NULL,
                    role TEXT NOT NULL,
                    address TEXT NOT NULL,
                    proxy_kind TEXT NOT NULL DEFAULT 'none',
                    implementation TEXT
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create manifest_contracts table for indexer tests")?;
        sqlx::query(
                r#"
                CREATE TABLE manifest_contract_instances (
                    manifest_contract_instance_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    manifest_id BIGINT NOT NULL REFERENCES manifest_versions (manifest_id) ON DELETE CASCADE,
                    declaration_kind TEXT NOT NULL,
                    declaration_name TEXT NOT NULL,
                    contract_instance_id UUID NOT NULL REFERENCES contract_instances (contract_instance_id),
                    declared_address TEXT NOT NULL,
                    code_hash TEXT,
                    abi_ref TEXT,
                    role TEXT,
                    proxy_kind TEXT,
                    implementation_contract_instance_id UUID REFERENCES contract_instances (contract_instance_id),
                    declared_implementation_address TEXT,
                    UNIQUE (manifest_id, declaration_kind, declaration_name)
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create manifest_contract_instances table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE manifest_capability_flags (
                    manifest_id BIGINT NOT NULL,
                    capability_name TEXT NOT NULL,
                    status capability_support_status NOT NULL,
                    notes TEXT
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create manifest_capability_flags table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE manifest_discovery_rules (
                    manifest_id BIGINT NOT NULL,
                    edge_kind TEXT NOT NULL,
                    from_role TEXT NOT NULL,
                    admission TEXT NOT NULL
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create manifest_discovery_rules table for indexer tests")?;
        sqlx::query(
                r#"
                CREATE TABLE discovery_edges (
                    discovery_edge_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    edge_kind TEXT NOT NULL,
                    from_contract_instance_id UUID NOT NULL REFERENCES contract_instances (contract_instance_id),
                    to_contract_instance_id UUID NOT NULL REFERENCES contract_instances (contract_instance_id),
                    discovery_source TEXT NOT NULL,
                    source_manifest_id BIGINT REFERENCES manifest_versions (manifest_id) ON DELETE SET NULL,
                    admission TEXT NOT NULL,
                    admitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    deactivated_at TIMESTAMPTZ,
                    active_from_block_number BIGINT,
                    active_from_block_hash TEXT,
                    active_to_block_number BIGINT,
                    active_to_block_hash TEXT,
                    provenance JSONB NOT NULL DEFAULT '{}'::jsonb
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create discovery_edges table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE chain_lineage (
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    parent_hash TEXT,
                    block_number BIGINT NOT NULL,
                    block_timestamp TIMESTAMPTZ NOT NULL,
                    logs_bloom BYTEA,
                    transactions_root TEXT,
                    receipts_root TEXT,
                    state_root TEXT,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (chain_id, block_hash)
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create chain_lineage table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE chain_checkpoints (
                    chain_id TEXT PRIMARY KEY,
                    canonical_block_hash TEXT,
                    canonical_block_number BIGINT,
                    safe_block_hash TEXT,
                    safe_block_number BIGINT,
                    finalized_block_hash TEXT,
                    finalized_block_number BIGINT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create chain_checkpoints table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE raw_blocks (
                    raw_block_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    parent_hash TEXT,
                    block_number BIGINT NOT NULL,
                    block_timestamp TIMESTAMPTZ NOT NULL,
                    logs_bloom BYTEA,
                    transactions_root TEXT,
                    receipts_root TEXT,
                    state_root TEXT,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (chain_id, block_hash)
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create raw_blocks table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE raw_transactions (
                    raw_transaction_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_number BIGINT NOT NULL,
                    transaction_hash TEXT NOT NULL,
                    transaction_index BIGINT NOT NULL,
                    from_address TEXT NOT NULL,
                    to_address TEXT,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (chain_id, block_hash, transaction_index)
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create raw_transactions table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE raw_code_hashes (
                    raw_code_hash_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_number BIGINT NOT NULL,
                    contract_address TEXT NOT NULL,
                    code_hash TEXT NOT NULL,
                    code_byte_length BIGINT NOT NULL,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (chain_id, block_hash, contract_address)
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create raw_code_hashes table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE raw_receipts (
                    raw_receipt_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_number BIGINT NOT NULL,
                    transaction_hash TEXT NOT NULL,
                    transaction_index BIGINT NOT NULL,
                    contract_address TEXT,
                    status BOOLEAN,
                    gas_used BIGINT,
                    cumulative_gas_used BIGINT,
                    logs_bloom BYTEA,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (chain_id, block_hash, transaction_index)
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create raw_receipts table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE raw_logs (
                    raw_log_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_number BIGINT NOT NULL,
                    transaction_hash TEXT NOT NULL,
                    transaction_index BIGINT NOT NULL,
                    log_index BIGINT NOT NULL,
                    emitting_address TEXT NOT NULL,
                    topics TEXT[] NOT NULL DEFAULT '{}',
                    data BYTEA NOT NULL DEFAULT '\x',
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (chain_id, block_hash, log_index)
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create raw_logs table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE raw_call_snapshots (
                    raw_call_snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_number BIGINT NOT NULL,
                    request_hash TEXT NOT NULL,
                    request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    response_hash TEXT NOT NULL,
                    response_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (chain_id, block_hash, request_hash)
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create raw_call_snapshots table for indexer tests")?;
        sqlx::query("CREATE EXTENSION IF NOT EXISTS btree_gist")
            .execute(&pool)
            .await
            .context("failed to create btree_gist extension for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE token_lineages (
                    token_lineage_id UUID PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_number BIGINT NOT NULL CHECK (block_number >= 0),
                    provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create token_lineages table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE resources (
                    resource_id UUID PRIMARY KEY,
                    token_lineage_id UUID REFERENCES token_lineages (token_lineage_id),
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_number BIGINT NOT NULL CHECK (block_number >= 0),
                    provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (token_lineage_id)
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create resources table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE name_surfaces (
                    logical_name_id TEXT PRIMARY KEY,
                    namespace TEXT NOT NULL,
                    input_name TEXT NOT NULL,
                    canonical_display_name TEXT NOT NULL,
                    normalized_name TEXT NOT NULL,
                    dns_encoded_name BYTEA NOT NULL,
                    namehash TEXT NOT NULL,
                    labelhashes TEXT[] NOT NULL,
                    normalizer_version TEXT NOT NULL,
                    normalization_warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
                    normalization_errors JSONB NOT NULL DEFAULT '[]'::jsonb,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_number BIGINT NOT NULL CHECK (block_number >= 0),
                    provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CHECK (logical_name_id = namespace || ':' || normalized_name)
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create name_surfaces table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE surface_bindings (
                    surface_binding_id UUID PRIMARY KEY,
                    logical_name_id TEXT NOT NULL REFERENCES name_surfaces (logical_name_id),
                    resource_id UUID NOT NULL REFERENCES resources (resource_id),
                    binding_kind TEXT NOT NULL,
                    active_from TIMESTAMPTZ NOT NULL,
                    active_to TIMESTAMPTZ,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_number BIGINT NOT NULL CHECK (block_number >= 0),
                    provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CHECK (
                        binding_kind IN (
                            'declared_registry_path',
                            'linked_subregistry_path',
                            'resolver_alias_path',
                            'observed_wildcard_path',
                            'migration_rebind',
                            'observed_only'
                        )
                    ),
                    CHECK (active_to IS NULL OR active_to > active_from),
                    CONSTRAINT surface_bindings_no_overlap
                        EXCLUDE USING gist (
                            logical_name_id WITH =,
                            tstzrange(active_from, COALESCE(active_to, 'infinity'::timestamptz), '[)') WITH &&
                        )
                        WHERE (canonicality_state IN ('canonical', 'safe', 'finalized'))
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create surface_bindings table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE normalized_events (
                    normalized_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    event_identity TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    logical_name_id TEXT,
                    resource_id UUID,
                    event_kind TEXT NOT NULL,
                    source_family TEXT NOT NULL,
                    manifest_version BIGINT NOT NULL,
                    source_manifest_id BIGINT,
                    chain_id TEXT,
                    block_number BIGINT,
                    block_hash TEXT,
                    transaction_hash TEXT,
                    log_index BIGINT,
                    raw_fact_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
                    derivation_kind TEXT NOT NULL,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    before_state JSONB NOT NULL DEFAULT '{}'::jsonb,
                    after_state JSONB NOT NULL DEFAULT '{}'::jsonb,
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (event_identity)
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create normalized_events table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE execution_traces (
                    execution_trace_id UUID PRIMARY KEY,
                    request_type TEXT NOT NULL,
                    request_key TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    chain_context JSONB NOT NULL DEFAULT '{}'::jsonb,
                    manifest_context JSONB NOT NULL DEFAULT '{}'::jsonb,
                    contracts_called JSONB NOT NULL DEFAULT '[]'::jsonb,
                    gateway_digests JSONB NOT NULL DEFAULT '[]'::jsonb,
                    final_payload JSONB,
                    failure_payload JSONB,
                    request_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    finished_at TIMESTAMPTZ NOT NULL,
                    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CHECK (jsonb_typeof(chain_context) = 'object' AND chain_context <> '{}'::jsonb),
                    CHECK (
                        jsonb_typeof(manifest_context) = 'object'
                        AND manifest_context <> '{}'::jsonb
                    ),
                    CHECK (jsonb_typeof(contracts_called) = 'array'),
                    CHECK (jsonb_typeof(gateway_digests) = 'array'),
                    CHECK (jsonb_typeof(request_metadata) = 'object'),
                    CHECK (final_payload IS NOT NULL OR failure_payload IS NOT NULL)
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create execution_traces table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE execution_steps (
                    execution_trace_id UUID NOT NULL REFERENCES execution_traces (execution_trace_id) ON DELETE CASCADE,
                    step_index BIGINT NOT NULL CHECK (step_index >= 0),
                    step_kind TEXT NOT NULL,
                    input_digest TEXT,
                    output_digest TEXT,
                    latency_ms BIGINT CHECK (latency_ms IS NULL OR latency_ms >= 0),
                    canonicality_dependency JSONB NOT NULL DEFAULT '{}'::jsonb,
                    step_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (execution_trace_id, step_index),
                    CHECK (
                        jsonb_typeof(canonicality_dependency) = 'object'
                        AND canonicality_dependency <> '{}'::jsonb
                    ),
                    CHECK (jsonb_typeof(step_payload) = 'object')
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create execution_steps table for indexer tests")?;
        sqlx::query(
            r#"
                CREATE TABLE execution_cache_outcomes (
                    execution_cache_key TEXT PRIMARY KEY,
                    request_key TEXT NOT NULL,
                    requested_chain_positions JSONB NOT NULL DEFAULT '[]'::jsonb,
                    manifest_versions JSONB NOT NULL DEFAULT '[]'::jsonb,
                    topology_version_boundary JSONB NOT NULL DEFAULT '{}'::jsonb,
                    record_version_boundary JSONB NOT NULL DEFAULT '{}'::jsonb,
                    execution_trace_id UUID NOT NULL REFERENCES execution_traces (execution_trace_id) ON DELETE CASCADE,
                    request_type TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    outcome_payload JSONB,
                    failure_payload JSONB,
                    finished_at TIMESTAMPTZ NOT NULL,
                    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CHECK (request_key <> ''),
                    CHECK (
                        jsonb_typeof(requested_chain_positions) = 'array'
                        AND requested_chain_positions <> '[]'::jsonb
                    ),
                    CHECK (
                        jsonb_typeof(manifest_versions) = 'array'
                        AND manifest_versions <> '[]'::jsonb
                    ),
                    CHECK (
                        jsonb_typeof(topology_version_boundary) = 'object'
                        AND topology_version_boundary <> '{}'::jsonb
                    ),
                    CHECK (
                        jsonb_typeof(record_version_boundary) = 'object'
                        AND record_version_boundary <> '{}'::jsonb
                    ),
                    CHECK (outcome_payload IS NOT NULL OR failure_payload IS NOT NULL)
                )
                "#,
        )
        .execute(&pool)
        .await
        .context("failed to create execution_cache_outcomes table for indexer tests")?;

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

fn manifest_load_summary(status: ManifestLoadStatus) -> ManifestLoadSummary {
    ManifestLoadSummary {
        root: PathBuf::from("/tmp/manifests"),
        status,
        namespace_count: usize::from(matches!(status, ManifestLoadStatus::Loaded)),
        source_family_count: usize::from(matches!(status, ManifestLoadStatus::Loaded)),
        manifest_count: usize::from(matches!(status, ManifestLoadStatus::Loaded)),
    }
}

fn synced_manifest_summary(active_manifest_count: usize) -> ManifestSyncSummary {
    ManifestSyncSummary {
        status: ManifestSyncStatus::Synced,
        synced_manifest_count: active_manifest_count,
        active_manifest_count,
        root_count: 0,
        contract_count: 0,
        capability_count: 0,
        discovery_rule_count: 0,
        removed_manifest_count: 0,
        cleared_discovery_edge_count: 0,
    }
}

fn manifest_contents(root_address: &str, capability_status: &str) -> String {
    manifest_contents_with_contract(
        root_address,
        capability_status,
        "0x00000000000000000000000000000000000000aa",
        "none",
        None,
    )
}

fn manifest_contents_with_contract(
    root_address: &str,
    capability_status: &str,
    contract_address: &str,
    proxy_kind: &str,
    implementation_address: Option<&str>,
) -> String {
    let implementation = implementation_address
        .map(|address| format!("implementation = \"{address}\""))
        .unwrap_or_default();

    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "uts46-v1"

[capability_flags]
exact_lookup = "{capability_status}"

[[roots]]
name = "RootRegistry"
address = "{root_address}"

[[contracts]]
role = "registry"
address = "{contract_address}"
proxy_kind = "{proxy_kind}"
{implementation}

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
    )
}

fn ens_v1_manifest_contents() -> String {
    r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v1_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v1"
rollout_status = "active"
normalizer_version = "uts46-v1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "ENSRegistry"
address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E"

[[contracts]]
role = "registry"
address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
    .to_owned()
}

fn basenames_base_registry_manifest_contents() -> String {
    r#"
manifest_version = 1
namespace = "basenames"
source_family = "basenames_base_registry"
chain = "base-mainnet"
deployment_epoch = "basenames_v1"
rollout_status = "active"
normalizer_version = "uts46-v1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "BasenamesRegistry"
address = "0xb94704422c2a1e396835a571837aa5ae53285a95"

[[contracts]]
role = "registry"
address = "0xb94704422c2a1e396835a571837aa5ae53285a95"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
    .to_owned()
}

fn ens_v1_new_owner_topic0() -> String {
    keccak256_hex(b"NewOwner(bytes32,bytes32,address)")
}

fn labelhash_hex(label: &str) -> String {
    keccak256_hex(label.as_bytes())
}

fn encode_new_owner_log_data(owner: &str) -> Vec<u8> {
    abi_word_address(owner).to_vec()
}

async fn insert_raw_new_owner_log(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    emitting_address: &str,
    owner: &str,
    canonicality_state: CanonicalityState,
) -> Result<()> {
    insert_raw_new_owner_log_for_parent(
        pool,
        chain,
        block,
        emitting_address,
        owner,
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "eth",
        canonicality_state,
    )
    .await
}

async fn insert_raw_new_owner_log_for_parent(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    emitting_address: &str,
    owner: &str,
    parent_node: &str,
    label: &str,
    canonicality_state: CanonicalityState,
) -> Result<()> {
    upsert_raw_blocks(
        pool,
        &[provider_block_to_raw_block(
            chain,
            block,
            canonicality_state,
        )],
    )
    .await?;
    upsert_raw_logs(
        pool,
        &[RawLog {
            chain_id: chain.to_owned(),
            block_hash: block.block_hash.clone(),
            block_number: block.block_number,
            transaction_hash: transaction_hash_for_block(block),
            transaction_index: 0,
            log_index: 1,
            emitting_address: emitting_address.to_ascii_lowercase(),
            topics: vec![
                ens_v1_new_owner_topic0(),
                parent_node.to_owned(),
                labelhash_hex(label),
            ],
            data: encode_new_owner_log_data(owner),
            canonicality_state,
        }],
    )
    .await?;

    Ok(())
}

async fn insert_raw_reverse_claimed_log(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    emitting_address: &str,
    claimed_address: &str,
    canonicality_state: CanonicalityState,
) -> Result<()> {
    upsert_raw_blocks(
        pool,
        &[provider_block_to_raw_block(
            chain,
            block,
            canonicality_state,
        )],
    )
    .await?;
    upsert_raw_logs(
        pool,
        &[RawLog {
            chain_id: chain.to_owned(),
            block_hash: block.block_hash.clone(),
            block_number: block.block_number,
            transaction_hash: transaction_hash_for_block(block),
            transaction_index: 0,
            log_index: 0,
            emitting_address: emitting_address.to_ascii_lowercase(),
            topics: vec![
                reverse_claimed_topic0(),
                hex_string(&abi_word_address(claimed_address)),
                reverse_node_for_address(claimed_address),
            ],
            data: Vec::new(),
            canonicality_state,
        }],
    )
    .await?;

    Ok(())
}

async fn insert_chain_lineage_for_block(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
) -> Result<()> {
    upsert_chain_lineage_blocks(
        pool,
        &[provider_block_to_lineage(chain, block, canonicality_state)],
    )
    .await?;

    Ok(())
}

async fn insert_raw_name_wrapped_log(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    emitting_address: &str,
    log_index: i64,
    canonicality_state: CanonicalityState,
) -> Result<()> {
    upsert_raw_blocks(
        pool,
        &[provider_block_to_raw_block(
            chain,
            block,
            canonicality_state,
        )],
    )
    .await?;
    let dns_name = dns_encoded_test_name();
    upsert_raw_logs(
        pool,
        &[RawLog {
            chain_id: chain.to_owned(),
            block_hash: block.block_hash.clone(),
            block_number: block.block_number,
            transaction_hash: transaction_hash_for_block(block),
            transaction_index: 0,
            log_index,
            emitting_address: emitting_address.to_ascii_lowercase(),
            topics: vec![name_wrapped_topic0(), namehash_for_dns_name(&dns_name)],
            data: decode_hex_string(&encode_name_wrapped_log_data(&dns_name)),
            canonicality_state,
        }],
    )
    .await?;

    Ok(())
}

async fn insert_contract_instance(
    pool: &PgPool,
    contract_instance_id: Uuid,
    chain: &str,
    contract_kind: &str,
) -> Result<()> {
    sqlx::query(
            r#"
            INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(contract_instance_id)
        .bind(chain)
        .bind(contract_kind)
        .execute(pool)
        .await
        .with_context(|| {
            format!(
                "failed to insert contract_instance_id {contract_instance_id} for {chain}:{contract_kind}"
            )
        })?;

    Ok(())
}

async fn insert_active_contract_instance_address(
    pool: &PgPool,
    contract_instance_id: Uuid,
    chain: &str,
    address: &str,
    source_manifest_id: Option<i64>,
) -> Result<()> {
    sqlx::query(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                source_manifest_id
            )
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(contract_instance_id)
        .bind(chain)
        .bind(address)
        .bind(source_manifest_id)
        .execute(pool)
        .await
        .with_context(|| {
            format!(
                "failed to insert active address {address} for contract_instance_id {contract_instance_id}"
            )
        })?;

    Ok(())
}

async fn insert_manifest_root_contract_instance(
    pool: &PgPool,
    manifest_id: i64,
    contract_instance_id: Uuid,
    address: &str,
) -> Result<()> {
    sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
                declaration_kind,
                declaration_name,
                contract_instance_id,
                declared_address
            )
            VALUES ($1, 'root', 'RootRegistry', $2, $3)
            "#,
        )
        .bind(manifest_id)
        .bind(contract_instance_id)
        .bind(address)
        .execute(pool)
        .await
        .with_context(|| {
            format!(
                "failed to insert manifest root contract_instance_id {contract_instance_id} for manifest_id {manifest_id}"
            )
        })?;
    sqlx::query(
        r#"
            INSERT INTO manifest_roots (manifest_id, address)
            VALUES ($1, $2)
            "#,
    )
    .bind(manifest_id)
    .bind(address)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to mirror manifest_roots row for manifest_id {manifest_id}")
    })?;

    Ok(())
}

async fn insert_manifest_contract_instance(
    pool: &PgPool,
    manifest_id: i64,
    role: &str,
    contract_instance_id: Uuid,
    address: &str,
    proxy_kind: &str,
    implementation_contract_instance_id: Option<Uuid>,
    declared_implementation_address: Option<&str>,
) -> Result<()> {
    sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
                declaration_kind,
                declaration_name,
                contract_instance_id,
                declared_address,
                role,
                proxy_kind,
                implementation_contract_instance_id,
                declared_implementation_address
            )
            VALUES ($1, 'contract', $2, $3, $4, $2, $5, $6, $7)
            "#,
        )
        .bind(manifest_id)
        .bind(role)
        .bind(contract_instance_id)
        .bind(address)
        .bind(proxy_kind)
        .bind(implementation_contract_instance_id)
        .bind(declared_implementation_address)
        .execute(pool)
        .await
        .with_context(|| {
            format!(
                "failed to insert manifest contract contract_instance_id {contract_instance_id} for manifest_id {manifest_id}"
            )
        })?;
    sqlx::query(
        r#"
            INSERT INTO manifest_contracts (manifest_id, role, address, proxy_kind, implementation)
            VALUES ($1, $2, $3, $4, $5)
            "#,
    )
    .bind(manifest_id)
    .bind(role)
    .bind(address)
    .bind(proxy_kind)
    .bind(declared_implementation_address)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to mirror manifest_contracts row for manifest_id {manifest_id}")
    })?;

    Ok(())
}

async fn insert_manifest_discovery_rule(
    pool: &PgPool,
    manifest_id: i64,
    edge_kind: &str,
    from_role: &str,
    admission: &str,
) -> Result<()> {
    sqlx::query(
        r#"
            INSERT INTO manifest_discovery_rules (manifest_id, edge_kind, from_role, admission)
            VALUES ($1, $2, $3, $4)
            "#,
    )
    .bind(manifest_id)
    .bind(edge_kind)
    .bind(from_role)
    .bind(admission)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to insert {edge_kind} discovery rule for manifest_id {manifest_id}")
    })?;

    Ok(())
}

async fn insert_active_discovery_edge(
    pool: &PgPool,
    chain: &str,
    edge_kind: &str,
    from_contract_instance_id: Uuid,
    to_contract_instance_id: Uuid,
    source_manifest_id: Option<i64>,
) -> Result<()> {
    sqlx::query(
            r#"
            INSERT INTO discovery_edges (
                chain_id,
                edge_kind,
                from_contract_instance_id,
                to_contract_instance_id,
                discovery_source,
                source_manifest_id,
                admission
            )
            VALUES ($1, $2, $3, $4, 'test', $5, 'test')
            "#,
        )
        .bind(chain)
        .bind(edge_kind)
        .bind(from_contract_instance_id)
        .bind(to_contract_instance_id)
        .bind(source_manifest_id)
        .execute(pool)
        .await
        .with_context(|| {
            format!(
                "failed to insert {edge_kind} discovery edge from {from_contract_instance_id} to {to_contract_instance_id}"
            )
        })?;

    Ok(())
}

async fn load_single_contract_instance_for_address(
    pool: &PgPool,
    chain: &str,
    address: &str,
) -> Result<Uuid> {
    sqlx::query_scalar::<_, Uuid>(
        r#"
            SELECT contract_instance_id
            FROM contract_instance_addresses
            WHERE chain_id = $1
              AND address = $2
            ORDER BY admitted_at DESC
            LIMIT 1
            "#,
    )
    .bind(chain)
    .bind(address.to_ascii_lowercase())
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load contract_instance_id for {chain}:{address}"))
}

fn provider_block(block_hash: &str, parent_hash: Option<&str>, block_number: i64) -> ProviderBlock {
    ProviderBlock {
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(ToOwned::to_owned),
        block_number,
        block_timestamp_unix_secs: 1_700_000_000 + block_number,
        logs_bloom: None,
        transactions_root: Some(format!("0xtransactions{block_number:02x}")),
        receipts_root: Some(format!("0xreceipts{block_number:02x}")),
        state_root: Some(format!("0xstate{block_number:02x}")),
    }
}

#[derive(Clone, Debug)]
struct ProviderBlockFixture {
    block: ProviderBlock,
    logs: Vec<Value>,
}

async fn bundle_provider(
    blocks: Vec<ProviderBlock>,
) -> Result<(provider::JsonRpcProvider, JoinHandle<()>)> {
    bundle_provider_with_fixtures(
        blocks
            .into_iter()
            .map(|block| ProviderBlockFixture {
                logs: vec![rpc_log_payload(&block)],
                block,
            })
            .collect(),
    )
    .await
}

async fn bundle_provider_with_fixtures(
    fixtures: Vec<ProviderBlockFixture>,
) -> Result<(provider::JsonRpcProvider, JoinHandle<()>)> {
    let blocks = Arc::new(
        fixtures
            .into_iter()
            .map(|fixture| (fixture.block.block_hash.clone(), fixture))
            .collect::<std::collections::BTreeMap<_, _>>(),
    );

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        let result = match method {
            "eth_getBlockByHash" => {
                let block_hash = params
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let fixture = blocks
                    .get(&block_hash)
                    .unwrap_or_else(|| panic!("unexpected block bundle request: {body}"));
                rpc_block_bundle_payload(&fixture.block)
            }
            "eth_getLogs" => {
                let block_hash = params
                    .first()
                    .and_then(Value::as_object)
                    .and_then(|filter| filter.get("blockHash"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let fixture = blocks
                    .get(&block_hash)
                    .unwrap_or_else(|| panic!("unexpected log request: {body}"));
                Value::Array(fixture.logs.clone())
            }
            "eth_getBlockReceipts" => {
                let block_hash = params
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let fixture = blocks
                    .get(&block_hash)
                    .unwrap_or_else(|| panic!("unexpected receipt request: {body}"));
                Value::Array(vec![rpc_receipt_payload(&fixture.block)])
            }
            "eth_getCode" => {
                let address = params
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let code = if address == "0x0000000000000000000000000000000000000002" {
                    "0x"
                } else {
                    "0x6001600155"
                };
                Value::String(code.to_owned())
            }
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        })
    }))
    .await?;

    Ok((provider::JsonRpcProvider::new(&url)?, server))
}

fn transaction_hash_for_block(block: &ProviderBlock) -> String {
    let seed = format!(
        "{}{:x}",
        block.block_hash.trim_start_matches("0x"),
        block.block_number
    );
    let suffix = if seed.len() > 64 {
        &seed[seed.len() - 64..]
    } else {
        seed.as_str()
    };

    format!("0x{suffix:0>64}")
}

fn rpc_block_bundle_payload(block: &ProviderBlock) -> Value {
    let transaction_hash = transaction_hash_for_block(block);
    json!({
        "hash": block.block_hash.clone(),
        "parentHash": block.parent_hash.clone().unwrap_or_else(|| {
            "0x0000000000000000000000000000000000000000000000000000000000000000".to_owned()
        }),
        "number": format!("0x{:x}", block.block_number),
        "timestamp": format!("0x{:x}", block.block_timestamp_unix_secs),
        "logsBloom": block.logs_bloom.as_ref().map(|bytes| hex_string(bytes)),
        "transactionsRoot": block.transactions_root.clone(),
        "receiptsRoot": block.receipts_root.clone(),
        "stateRoot": block.state_root.clone(),
        "transactions": [
            {
                "hash": transaction_hash,
                "blockHash": block.block_hash.clone(),
                "blockNumber": format!("0x{:x}", block.block_number),
                "transactionIndex": "0x0",
                "from": "0x0000000000000000000000000000000000000001",
                "to": "0x0000000000000000000000000000000000000002"
            }
        ]
    })
}

fn rpc_receipt_payload(block: &ProviderBlock) -> Value {
    json!({
        "transactionHash": transaction_hash_for_block(block),
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionIndex": "0x0",
        "contractAddress": null,
        "status": "0x1",
        "gasUsed": "0x5208",
        "cumulativeGasUsed": "0x5208",
        "logsBloom": block.logs_bloom.as_ref().map(|bytes| hex_string(bytes)),
    })
}

fn dns_encoded_test_name() -> Vec<u8> {
    vec![
        7, b'w', b'r', b'a', b'p', b'p', b'e', b'd', 3, b'e', b't', b'h', 0,
    ]
}

fn name_wrapped_topic0() -> String {
    keccak256_hex(b"NameWrapped(bytes,bytes32,address,uint32,uint64)")
}

fn registrar_name_registered_topic0() -> String {
    keccak256_hex(b"NameRegistered(string,bytes32,address,uint256,uint256)")
}

fn registry_new_resolver_topic0() -> String {
    keccak256_hex(b"NewResolver(bytes32,address)")
}

fn ens_v2_label_registered_topic0() -> String {
    keccak256_hex(b"LabelRegistered(uint256,bytes32,string,address,uint64,address)")
}

fn ens_v2_resolver_updated_topic0() -> String {
    keccak256_hex(b"ResolverUpdated(uint256,address,address)")
}

fn ens_v2_token_resource_topic0() -> String {
    keccak256_hex(b"TokenResource(uint256,uint256)")
}

fn resolver_addr_changed_topic0() -> String {
    keccak256_hex(b"AddrChanged(bytes32,address)")
}

fn ens_v2_resolver_address_changed_topic0() -> String {
    keccak256_hex(b"AddressChanged(bytes32,uint256,bytes)")
}

fn ens_v2_alias_changed_topic0() -> String {
    keccak256_hex(b"AliasChanged(bytes,bytes,bytes,bytes)")
}

fn resolver_text_changed_topic0() -> String {
    keccak256_hex(b"TextChanged(bytes32,string,string)")
}

fn resolver_name_changed_topic0() -> String {
    keccak256_hex(b"NameChanged(bytes32,string)")
}

fn resolver_version_changed_topic0() -> String {
    keccak256_hex(b"VersionChanged(bytes32,uint64)")
}

fn ens_v2_named_resource_topic0() -> String {
    keccak256_hex(b"NamedResource(uint256,bytes)")
}

fn ens_v2_eac_roles_changed_topic0() -> String {
    keccak256_hex(b"EACRolesChanged(uint256,address,uint256,uint256)")
}

fn reverse_claimed_topic0() -> String {
    keccak256_hex(b"ReverseClaimed(address,bytes32)")
}

const REVERSE_REGISTRAR_ROLE: &str = "reverse_registrar";

fn reverse_label_for_address(address: &str) -> String {
    let normalized = address
        .strip_prefix("0x")
        .unwrap_or(address)
        .to_ascii_lowercase();
    assert_eq!(
        normalized.len(),
        40,
        "reverse claim address must be 20 bytes"
    );
    normalized
}

fn reverse_name_for_address(address: &str) -> String {
    format!("{}.addr.reverse", reverse_label_for_address(address))
}

fn reverse_node_for_address(address: &str) -> String {
    let reverse_label = reverse_label_for_address(address);
    let mut dns_name = Vec::new();
    dns_name.push(u8::try_from(reverse_label.len()).expect("reverse label length must fit in u8"));
    dns_name.extend_from_slice(reverse_label.as_bytes());
    dns_name.push(4);
    dns_name.extend_from_slice(b"addr");
    dns_name.push(7);
    dns_name.extend_from_slice(b"reverse");
    dns_name.push(0);
    namehash_for_dns_name(&dns_name)
}

fn namehash_for_dns_name(dns_name: &[u8]) -> String {
    let mut labels = Vec::<Vec<u8>>::new();
    let mut cursor = 0usize;
    while cursor < dns_name.len() {
        let length = usize::from(dns_name[cursor]);
        cursor += 1;
        if length == 0 {
            break;
        }
        labels.push(dns_name[cursor..cursor + length].to_vec());
        cursor += length;
    }

    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let label_hash = {
            let mut hasher = Keccak256::new();
            hasher.update(label);
            let digest = hasher.finalize();
            let mut output = [0u8; 32];
            output.copy_from_slice(&digest);
            output
        };
        let mut hasher = Keccak256::new();
        hasher.update(node);
        hasher.update(label_hash);
        let digest = hasher.finalize();
        node.copy_from_slice(&digest);
    }

    hex_string(&node)
}

fn encode_name_wrapped_log_data(dns_name: &[u8]) -> String {
    let mut data = Vec::new();

    let mut push_word = |value: [u8; 32]| data.extend_from_slice(&value);
    push_word(abi_word_u64(128));
    push_word(abi_word_address(
        "0x0000000000000000000000000000000000000001",
    ));
    push_word(abi_word_u64(0));
    push_word(abi_word_u64(0));
    push_word(abi_word_u64(
        u64::try_from(dns_name.len()).expect("dns name test payload length must fit in u64"),
    ));
    data.extend_from_slice(dns_name);
    let padded_length = ((dns_name.len() + 31) / 32) * 32;
    data.resize(32 * 5 + padded_length, 0);

    hex_string(&data)
}

fn dns_encoded_eth_name(label: &str) -> Vec<u8> {
    dns_encoded_name(&[label, "eth"])
}

fn dns_encoded_base_eth_name(label: &str) -> Vec<u8> {
    dns_encoded_name(&[label, "base", "eth"])
}

fn dns_encoded_name(labels: &[&str]) -> Vec<u8> {
    let mut output = Vec::new();
    for label in labels {
        output.push(u8::try_from(label.len()).expect("resolver label length must fit in u8"));
        output.extend_from_slice(label.as_bytes());
    }
    output.push(0);
    output
}

fn base_eth_node() -> String {
    namehash_for_dns_name(&dns_encoded_name(&["base", "eth"]))
}

fn abi_word_u64(value: u64) -> [u8; 32] {
    let mut word = [0u8; 32];
    word[24..].copy_from_slice(&value.to_be_bytes());
    word
}

fn abi_word_bytes32(value: &str) -> [u8; 32] {
    let value = value.strip_prefix("0x").unwrap_or(value);
    let mut word = [0u8; 32];
    for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk).expect("test bytes32 must be utf-8 hex");
        word[index] = u8::from_str_radix(hex, 16).expect("test bytes32 chunk must be valid hex");
    }
    word
}

fn abi_word_address(address: &str) -> [u8; 32] {
    let address = address.strip_prefix("0x").unwrap_or(address);
    let mut word = [0u8; 32];
    for (index, chunk) in address.as_bytes().chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk).expect("test address must be utf-8 hex");
        word[12 + index] =
            u8::from_str_radix(hex, 16).expect("test address chunk must be valid hex");
    }
    word
}

fn rpc_log_payload(block: &ProviderBlock) -> Value {
    let dns_name = dns_encoded_test_name();
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "address": "0x0000000000000000000000000000000000000001",
        "topics": [
            name_wrapped_topic0(),
            namehash_for_dns_name(&dns_name)
        ],
        "data": encode_name_wrapped_log_data(&dns_name)
    })
}

fn encode_registrar_name_registered_log_data(label: &str, expiry_unix: i64) -> String {
    let label_bytes = label.as_bytes();
    let mut data = Vec::new();

    data.extend_from_slice(&abi_word_u64(96));
    data.extend_from_slice(&abi_word_u64(1));
    data.extend_from_slice(&abi_word_u64(expiry_unix as u64));
    data.extend_from_slice(&abi_word_u64(
        u64::try_from(label_bytes.len()).expect("registrar label test payload must fit in u64"),
    ));
    data.extend_from_slice(label_bytes);

    let padded_length = ((label_bytes.len() + 31) / 32) * 32;
    data.resize(32 * 4 + padded_length, 0);

    hex_string(&data)
}

fn encode_ens_v2_label_registered_log_data(label: &str, owner: &str, expiry_unix: i64) -> String {
    let label_bytes = label.as_bytes();
    let mut data = Vec::new();

    data.extend_from_slice(&abi_word_u64(96));
    data.extend_from_slice(&abi_word_address(owner));
    data.extend_from_slice(&abi_word_u64(expiry_unix as u64));
    data.extend_from_slice(&abi_word_u64(
        u64::try_from(label_bytes.len()).expect("ENSv2 label test payload must fit in u64"),
    ));
    data.extend_from_slice(label_bytes);

    let padded_length = ((label_bytes.len() + 31) / 32) * 32;
    data.resize(32 * 4 + padded_length, 0);

    hex_string(&data)
}

fn rpc_registrar_name_registered_log_payload(
    block: &ProviderBlock,
    address: &str,
    label: &str,
    expiry_unix: i64,
) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "address": address,
        "topics": [
            registrar_name_registered_topic0(),
            labelhash_hex(label),
            hex_string(&abi_word_address("0x0000000000000000000000000000000000000001"))
        ],
        "data": encode_registrar_name_registered_log_data(label, expiry_unix)
    })
}

fn rpc_reverse_claimed_log_payload(
    block: &ProviderBlock,
    address: &str,
    claimed_address: &str,
    log_index: u64,
) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            reverse_claimed_topic0(),
            hex_string(&abi_word_address(claimed_address)),
            reverse_node_for_address(claimed_address)
        ],
        "data": "0x"
    })
}

fn encode_registry_new_resolver_log_data(resolver: &str) -> String {
    hex_string(&abi_word_address(resolver))
}

fn encode_dynamic_bytes_log_data(value: &[u8]) -> String {
    let mut output = Vec::new();
    output.extend_from_slice(&abi_word_u64(32));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(value.len()).expect("test bytes length must fit in u64"),
    ));
    output.extend_from_slice(value);
    let padded_length = ((value.len() + 31) / 32) * 32;
    output.resize(64 + padded_length, 0);
    hex_string(&output)
}

fn encode_two_dynamic_bytes_log_data(left: &[u8], right: &[u8]) -> String {
    let left_padded_length = ((left.len() + 31) / 32) * 32;
    let right_offset = 64 + 32 + left_padded_length;
    let mut output = Vec::new();
    output.extend_from_slice(&abi_word_u64(64));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(right_offset).expect("test ABI offset must fit in u64"),
    ));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(left.len()).expect("left bytes length must fit in u64"),
    ));
    output.extend_from_slice(left);
    output.resize(64 + 32 + left_padded_length, 0);
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(right.len()).expect("right bytes length must fit in u64"),
    ));
    output.extend_from_slice(right);
    let right_padded_length = ((right.len() + 31) / 32) * 32;
    output.resize(right_offset + 32 + right_padded_length, 0);
    hex_string(&output)
}

fn encode_dynamic_string_log_data(value: &str) -> String {
    let value_bytes = value.as_bytes();
    let mut output = Vec::new();
    output.extend_from_slice(&abi_word_u64(32));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(value_bytes.len()).expect("test string length must fit in u64"),
    ));
    output.extend_from_slice(value_bytes);
    let padded_length = ((value_bytes.len() + 31) / 32) * 32;
    output.resize(64 + padded_length, 0);
    hex_string(&output)
}

fn encode_resolver_addr_changed_log_data(address: &str) -> String {
    hex_string(&abi_word_address(address))
}

fn encode_ens_v2_resolver_address_changed_log_data(coin_type: u64, address_bytes: &[u8]) -> String {
    let mut data = Vec::new();
    data.extend_from_slice(&abi_word_u64(coin_type));
    data.extend_from_slice(&abi_word_u64(64));
    data.extend_from_slice(&abi_word_u64(
        u64::try_from(address_bytes.len()).expect("address bytes test payload must fit in u64"),
    ));
    data.extend_from_slice(address_bytes);
    let padded_length = ((address_bytes.len() + 31) / 32) * 32;
    data.resize(96 + padded_length, 0);
    hex_string(&data)
}

fn encode_resolver_version_changed_log_data(version: u64) -> String {
    hex_string(&abi_word_u64(version))
}

fn encode_eac_roles_changed_log_data(old_role_bitmap: &str, new_role_bitmap: &str) -> String {
    let mut data = Vec::new();
    data.extend_from_slice(&abi_word_bytes32(old_role_bitmap));
    data.extend_from_slice(&abi_word_bytes32(new_role_bitmap));
    hex_string(&data)
}

fn rpc_registry_new_resolver_log_payload(
    block: &ProviderBlock,
    address: &str,
    label: &str,
    resolver: &str,
    log_index: u64,
) -> Value {
    let dns_name = dns_encoded_eth_name(label);

    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            registry_new_resolver_topic0(),
            namehash_for_dns_name(&dns_name)
        ],
        "data": encode_registry_new_resolver_log_data(resolver)
    })
}

fn rpc_registry_new_resolver_log_payload_for_namehash(
    block: &ProviderBlock,
    address: &str,
    namehash: &str,
    resolver: &str,
    log_index: u64,
) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            registry_new_resolver_topic0(),
            namehash
        ],
        "data": encode_registry_new_resolver_log_data(resolver)
    })
}

fn rpc_resolver_text_changed_log_payload(
    block: &ProviderBlock,
    address: &str,
    label: &str,
    key: &str,
    log_index: u64,
) -> Value {
    let dns_name = dns_encoded_eth_name(label);

    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            resolver_text_changed_topic0(),
            namehash_for_dns_name(&dns_name),
            keccak256_hex(key.as_bytes())
        ],
        "data": encode_dynamic_string_log_data(key)
    })
}

fn rpc_resolver_text_changed_log_payload_for_namehash(
    block: &ProviderBlock,
    address: &str,
    namehash: &str,
    key: &str,
    log_index: u64,
) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            resolver_text_changed_topic0(),
            namehash,
            keccak256_hex(key.as_bytes())
        ],
        "data": encode_dynamic_string_log_data(key)
    })
}

fn rpc_resolver_name_changed_log_payload_for_namehash(
    block: &ProviderBlock,
    address: &str,
    namehash: &str,
    value: &str,
    log_index: u64,
) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            resolver_name_changed_topic0(),
            namehash
        ],
        "data": encode_dynamic_string_log_data(value)
    })
}

fn rpc_resolver_addr_changed_log_payload(
    block: &ProviderBlock,
    address: &str,
    label: &str,
    resolved_address: &str,
    log_index: u64,
) -> Value {
    let dns_name = dns_encoded_eth_name(label);

    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            resolver_addr_changed_topic0(),
            namehash_for_dns_name(&dns_name)
        ],
        "data": encode_resolver_addr_changed_log_data(resolved_address)
    })
}

fn rpc_resolver_version_changed_log_payload(
    block: &ProviderBlock,
    address: &str,
    label: &str,
    version: u64,
    log_index: u64,
) -> Value {
    let dns_name = dns_encoded_eth_name(label);

    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            resolver_version_changed_topic0(),
            namehash_for_dns_name(&dns_name)
        ],
        "data": encode_resolver_version_changed_log_data(version)
    })
}

fn rpc_resolver_version_changed_log_payload_for_namehash(
    block: &ProviderBlock,
    address: &str,
    namehash: &str,
    version: u64,
    log_index: u64,
) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            resolver_version_changed_topic0(),
            namehash
        ],
        "data": encode_resolver_version_changed_log_data(version)
    })
}

fn hex_string(bytes: &[u8]) -> String {
    let mut output = String::from("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn decode_hex_string(payload: &str) -> Vec<u8> {
    let payload = payload.strip_prefix("0x").unwrap_or(payload);
    payload
        .as_bytes()
        .chunks(2)
        .map(|chunk| {
            let hex = std::str::from_utf8(chunk).expect("hex payload must be utf-8");
            u8::from_str_radix(hex, 16).expect("hex payload must contain valid bytes")
        })
        .collect()
}

async fn spawn_json_rpc_server(
    handler: Arc<dyn Fn(Value) -> Value + Send + Sync>,
) -> Result<(String, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind JSON-RPC test server")?;
    let address = listener
        .local_addr()
        .context("failed to read JSON-RPC test server address")?;
    let url = format!("http://{address}");

    let server = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0_u8; 4096];
                loop {
                    let Ok(bytes_read) = stream.read(&mut chunk).await else {
                        return;
                    };
                    if bytes_read == 0 {
                        return;
                    }
                    buffer.extend_from_slice(&chunk[..bytes_read]);
                    if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }

                let header_end = buffer
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|index| index + 4)
                    .expect("HTTP request must contain header terminator");
                let header = &buffer[..header_end];
                let header_text = String::from_utf8_lossy(header).to_ascii_lowercase();
                let content_length = header_text
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length:")
                            .and_then(|value| value.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                let mut body = buffer[header_end..].to_vec();
                while body.len() < content_length {
                    let Ok(bytes_read) = stream.read(&mut chunk).await else {
                        return;
                    };
                    if bytes_read == 0 {
                        return;
                    }
                    body.extend_from_slice(&chunk[..bytes_read]);
                }
                body.truncate(content_length);

                let request_body = serde_json::from_slice::<Value>(&body)
                    .expect("JSON-RPC test request body must decode");
                let response_body = handler(request_body).to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );

                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });

    Ok((url, server))
}
