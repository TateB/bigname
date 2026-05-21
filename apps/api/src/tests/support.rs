use std::{
    fs,
    str::FromStr,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::Context;
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use bigname_storage::{
    CanonicalityState, ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, ExecutionTraceStep,
    NameSurface, NormalizedEvent, PermissionScope, PermissionsCurrentRow, PrimaryNameClaimStatus,
    PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot, RawBlock, ResolverCurrentRow, Resource,
    SurfaceBinding, SurfaceBindingKind, TokenLineage, default_database_url,
    load_primary_name_current, upsert_execution_outcome, upsert_execution_trace,
    upsert_normalized_events, upsert_primary_name_current_rows,
    upsert_primary_name_current_snapshots,
};
use bigname_test_support::TestDatabaseConfig;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sqlx::{
    ConnectOptions, PgPool, Row,
    postgres::PgConnectOptions,
    types::{Uuid, time::OffsetDateTime},
};
use tower::ServiceExt;

use super::*;

#[path = "../../../worker/src/primary_name.rs"]
mod worker_primary_name;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);
static WORKER_CARGO_LOCK: Mutex<()> = Mutex::new(());

struct TestDatabase {
    database: bigname_test_support::TestDatabase,
    pool: PgPool,
    database_name: String,
}

struct AuthorityHistorySeed<'a> {
    event_identity: &'a str,
    namespace: &'a str,
    logical_name_id: &'a str,
    resource_id: Uuid,
    event_kind: &'a str,
    block_number: i64,
    block_hash: &'a str,
    after_state: Value,
}

#[derive(Clone, Copy)]
enum BasenamesControlVectorScenario {
    NftOnly,
    ManagementOnly,
    FullTransfer,
}

impl BasenamesControlVectorScenario {
    fn current_token_subject(self) -> &'static str {
        match self {
            Self::NftOnly => "0x00000000000000000000000000000000000000c1",
            Self::ManagementOnly => "0x00000000000000000000000000000000000000a2",
            Self::FullTransfer => "0x00000000000000000000000000000000000000c3",
        }
    }

    fn current_effective_controller(self) -> &'static str {
        match self {
            Self::NftOnly => "0x00000000000000000000000000000000000000b1",
            Self::ManagementOnly => "0x00000000000000000000000000000000000000b2",
            Self::FullTransfer => "0x00000000000000000000000000000000000000c3",
        }
    }

    fn previous_effective_controller(self) -> Option<&'static str> {
        match self {
            Self::FullTransfer => Some("0x00000000000000000000000000000000000000b3"),
            _ => None,
        }
    }
}

impl TestDatabase {
    async fn new(initialize_manifest_schema: bool) -> Result<Self> {
        Self::new_with_schemas(initialize_manifest_schema, false).await
    }

    async fn new_with_schemas(
        initialize_manifest_schema: bool,
        initialize_name_current_schema: bool,
    ) -> Result<Self> {
        let database = bigname_test_support::TestDatabase::create(
            TestDatabaseConfig::new("bigname_api_test")
                .admin_database_from_url()
                .pool_max_connections(1)
                .parse_context("failed to parse database URL for API tests")
                .admin_connect_context("failed to connect admin pool for API tests")
                .pool_connect_context("failed to connect API test pool"),
        )
        .await?;
        let pool = database.pool().clone();
        let database_name = database.database_name().to_owned();

        if initialize_manifest_schema {
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
            .context("failed to create manifest_rollout_status for API tests")?;
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
            .context("failed to create capability_support_status for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE manifest_versions (
                        manifest_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
                        namespace TEXT NOT NULL,
                        source_family TEXT NOT NULL,
                        chain TEXT NOT NULL,
                        deployment_epoch TEXT NOT NULL,
                        rollout_status manifest_rollout_status NOT NULL,
                        normalizer_version TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        manifest_payload JSONB NOT NULL,
                        loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create manifest_versions for API tests")?;
            sqlx::query(
                    r#"
                    CREATE TABLE manifest_capability_flags (
                        manifest_id BIGINT NOT NULL REFERENCES manifest_versions (manifest_id) ON DELETE CASCADE,
                        capability_name TEXT NOT NULL,
                        status capability_support_status NOT NULL,
                        notes TEXT,
                        PRIMARY KEY (manifest_id, capability_name)
                    )
                    "#,
                )
                .execute(&pool)
                .await
                .context("failed to create manifest_capability_flags for API tests")?;
        }

        if initialize_name_current_schema {
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
            .context("failed to create canonicality_state for API tests")?;
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
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        CHECK ((canonical_block_hash IS NULL) = (canonical_block_number IS NULL)),
                        CHECK ((safe_block_hash IS NULL) = (safe_block_number IS NULL)),
                        CHECK ((finalized_block_hash IS NULL) = (finalized_block_number IS NULL))
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create chain_checkpoints for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE chain_lineage (
                        chain_id TEXT NOT NULL,
                        block_hash TEXT NOT NULL,
                        parent_hash TEXT,
                        block_number BIGINT NOT NULL CHECK (block_number >= 0),
                        block_timestamp TIMESTAMPTZ NOT NULL,
                        canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                        observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (chain_id, block_hash)
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create chain_lineage for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE chain_header_audit (
                        chain_id TEXT NOT NULL,
                        block_hash TEXT NOT NULL,
                        logs_bloom BYTEA,
                        transactions_root TEXT,
                        receipts_root TEXT,
                        state_root TEXT,
                        observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (chain_id, block_hash),
                        FOREIGN KEY (chain_id, block_hash)
                            REFERENCES chain_lineage (chain_id, block_hash)
                            ON DELETE CASCADE,
                        CHECK (
                            logs_bloom IS NOT NULL
                            OR transactions_root IS NOT NULL
                            OR receipts_root IS NOT NULL
                            OR state_root IS NOT NULL
                        )
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create chain_header_audit for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE name_surfaces (
                        logical_name_id TEXT PRIMARY KEY,
                        namespace TEXT NOT NULL,
                        canonical_display_name TEXT NOT NULL,
                        normalized_name TEXT NOT NULL,
                        namehash TEXT NOT NULL,
                        canonicality_state canonicality_state NOT NULL DEFAULT 'finalized',
                        CHECK (logical_name_id = namespace || ':' || normalized_name)
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create name_surfaces for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE resources (
                        resource_id UUID PRIMARY KEY,
                        canonicality_state canonicality_state NOT NULL DEFAULT 'finalized'
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create resources for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE token_lineages (
                        token_lineage_id UUID PRIMARY KEY,
                        canonicality_state canonicality_state NOT NULL DEFAULT 'finalized'
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create token_lineages for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE surface_bindings (
                        surface_binding_id UUID PRIMARY KEY,
                        logical_name_id TEXT NOT NULL REFERENCES name_surfaces (logical_name_id),
                        resource_id UUID NOT NULL REFERENCES resources (resource_id),
                        binding_kind TEXT NOT NULL,
                        canonicality_state canonicality_state NOT NULL DEFAULT 'finalized',
                        CHECK (
                            binding_kind IN (
                                'declared_registry_path',
                                'linked_subregistry_path',
                                'resolver_alias_path',
                                'observed_wildcard_path',
                                'migration_rebind',
                                'observed_only'
                            )
                        )
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create surface_bindings for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE name_current (
                        logical_name_id TEXT PRIMARY KEY REFERENCES name_surfaces (logical_name_id),
                        namespace TEXT NOT NULL,
                        canonical_display_name TEXT NOT NULL,
                        normalized_name TEXT NOT NULL,
                        namehash TEXT NOT NULL,
                        surface_binding_id UUID REFERENCES surface_bindings (surface_binding_id),
                        resource_id UUID REFERENCES resources (resource_id),
                        token_lineage_id UUID REFERENCES token_lineages (token_lineage_id),
                        binding_kind TEXT,
                        declared_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                        provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                        coverage JSONB NOT NULL DEFAULT '{}'::jsonb,
                        chain_positions JSONB NOT NULL DEFAULT '{}'::jsonb,
                        canonicality_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                        manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
                        last_recomputed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        CHECK (logical_name_id = namespace || ':' || normalized_name),
                        CHECK (
                            (surface_binding_id IS NULL AND resource_id IS NULL AND binding_kind IS NULL)
                            OR
                            (surface_binding_id IS NOT NULL AND resource_id IS NOT NULL AND binding_kind IS NOT NULL)
                        ),
                        CHECK (
                            token_lineage_id IS NULL
                            OR resource_id IS NOT NULL
                        ),
                        CHECK (
                            binding_kind IS NULL
                            OR binding_kind IN (
                                'declared_registry_path',
                                'linked_subregistry_path',
                                'resolver_alias_path',
                                'observed_wildcard_path',
                                'migration_rebind',
                                'observed_only'
                            )
                        )
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create name_current for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE record_inventory_current (
                        resource_id UUID NOT NULL REFERENCES resources (resource_id),
                        record_version_boundary_key TEXT NOT NULL,
                        record_version_boundary JSONB NOT NULL DEFAULT '{}'::jsonb,
                        enumeration_basis JSONB NOT NULL DEFAULT '{}'::jsonb,
                        selectors JSONB NOT NULL DEFAULT '[]'::jsonb,
                        explicit_gaps JSONB NOT NULL DEFAULT '[]'::jsonb,
                        unsupported_families JSONB NOT NULL DEFAULT '[]'::jsonb,
                        last_change JSONB,
                        entries JSONB NOT NULL DEFAULT '[]'::jsonb,
                        provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                        coverage JSONB NOT NULL DEFAULT '{}'::jsonb,
                        chain_positions JSONB NOT NULL DEFAULT '{}'::jsonb,
                        canonicality_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                        manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
                        last_recomputed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (resource_id, record_version_boundary_key),
                        CHECK (record_version_boundary_key <> '')
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create record_inventory_current for API tests")?;
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
            .context("failed to create execution_traces for API tests")?;
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
            .context("failed to create execution_steps for API tests")?;
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
            .context("failed to create execution_cache_outcomes for API tests")?;
        }

        Ok(Self {
            database,
            pool,
            database_name,
        })
    }

    async fn new_migrated() -> Result<Self> {
        let database = Self::new(false).await?;
        database
            .database
            .apply_migrations(
                &bigname_storage::MIGRATOR,
                "failed to apply checked-in migrations for API tests",
            )
            .await?;
        Ok(database)
    }

    fn app_state(&self) -> AppState {
        self.app_state_with_chain_rpc_urls(bigname_execution::ChainRpcUrls::default())
    }

    fn app_state_with_chain_rpc_urls(&self, chain_rpc_urls: bigname_execution::ChainRpcUrls) -> AppState {
        AppState {
            phase: "test",
            pool: self.pool.clone(),
            chain_rpc_urls,
        }
    }

    async fn seed_history_binding(
        &self,
        logical_name_id: &str,
        resource_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        bigname_storage::upsert_name_surfaces(&self.pool, &[name_surface(logical_name_id)])
            .await
            .context("failed to upsert name surface for history API test")?;
        bigname_storage::upsert_resources(&self.pool, &[resource(resource_id)])
            .await
            .context("failed to upsert resource for history API test")?;
        bigname_storage::upsert_surface_bindings(
            &self.pool,
            &[surface_binding(
                surface_binding_id,
                logical_name_id,
                resource_id,
                timestamp(1_700_000_000),
            )],
        )
        .await
        .context("failed to upsert surface binding for history API test")?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_manifest(
        &self,
        namespace: &str,
        source_family: &str,
        chain: &str,
        deployment_epoch: &str,
        manifest_version: u64,
        rollout_status: &str,
        normalizer_version: &str,
    ) -> Result<i64> {
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let file_path =
            format!("tests/{namespace}/{source_family}/{manifest_version}-{sequence}.toml");

        sqlx::query(
            r#"
                INSERT INTO manifest_versions (
                    manifest_version,
                    namespace,
                    source_family,
                    chain,
                    deployment_epoch,
                    rollout_status,
                    normalizer_version,
                    file_path,
                    manifest_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6::manifest_rollout_status, $7, $8, $9::jsonb)
                RETURNING manifest_id
                "#,
        )
        .bind(i64::try_from(manifest_version).context("manifest_version exceeds BIGINT")?)
        .bind(namespace)
        .bind(source_family)
        .bind(chain)
        .bind(deployment_epoch)
        .bind(rollout_status)
        .bind(normalizer_version)
        .bind(file_path)
        .bind("{}")
        .fetch_one(&self.pool)
        .await
        .context("failed to insert manifest_version for API test")?
        .try_get("manifest_id")
        .context("failed to read manifest_id for API test")
    }

    async fn insert_capability_flag(
        &self,
        manifest_id: i64,
        capability_name: &str,
        status: &str,
        notes: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
                INSERT INTO manifest_capability_flags (
                    manifest_id,
                    capability_name,
                    status,
                    notes
                )
                VALUES ($1, $2, $3::capability_support_status, $4)
                "#,
        )
        .bind(manifest_id)
        .bind(capability_name)
        .bind(status)
        .bind(notes)
        .execute(&self.pool)
        .await
        .context("failed to insert manifest capability flag for API test")?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_name_current_binding(
        &self,
        logical_name_id: &str,
        namespace: &str,
        normalized_name: &str,
        canonical_display_name: &str,
        namehash: &str,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        sqlx::query(
            r#"
                INSERT INTO name_surfaces (
                    logical_name_id,
                    namespace,
                    canonical_display_name,
                    normalized_name,
                    namehash
                )
                VALUES ($1, $2, $3, $4, $5)
                "#,
        )
        .bind(logical_name_id)
        .bind(namespace)
        .bind(canonical_display_name)
        .bind(normalized_name)
        .bind(namehash)
        .execute(&self.pool)
        .await
        .context("failed to insert name_surface for API test")?;

        sqlx::query("INSERT INTO resources (resource_id) VALUES ($1)")
            .bind(resource_id)
            .execute(&self.pool)
            .await
            .context("failed to insert resource for API test")?;

        sqlx::query("INSERT INTO token_lineages (token_lineage_id) VALUES ($1)")
            .bind(token_lineage_id)
            .execute(&self.pool)
            .await
            .context("failed to insert token_lineage for API test")?;

        sqlx::query(
            r#"
                INSERT INTO surface_bindings (
                    surface_binding_id,
                    logical_name_id,
                    resource_id,
                    binding_kind
                )
                VALUES ($1, $2, $3, $4)
                "#,
        )
        .bind(surface_binding_id)
        .bind(logical_name_id)
        .bind(resource_id)
        .bind("declared_registry_path")
        .execute(&self.pool)
        .await
        .context("failed to insert surface_binding for API test")?;

        Ok(())
    }

    async fn seed_name_current_binding_migrated(
        &self,
        logical_name_id: &str,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        bigname_storage::upsert_raw_blocks(
            &self.pool,
            &[
                raw_block("ethereum-mainnet", "0xsurface", None, 98, 1_717_171_698),
                raw_block("ethereum-mainnet", "0xresource", None, 99, 1_717_171_699),
                raw_block("ethereum-mainnet", "0xbinding", None, 100, 1_717_171_700),
            ],
        )
        .await?;
        bigname_storage::upsert_name_surfaces(&self.pool, &[name_surface(logical_name_id)]).await?;
        bigname_storage::upsert_token_lineages(
            &self.pool,
            &[address_name_token_lineage(
                token_lineage_id,
                "0xresource",
                99,
            )],
        )
        .await?;
        bigname_storage::upsert_resources(
            &self.pool,
            &[address_name_resource(
                resource_id,
                Some(token_lineage_id),
                "0xresource",
                99,
            )],
        )
        .await?;
        bigname_storage::upsert_surface_bindings(
            &self.pool,
            &[surface_binding(
                surface_binding_id,
                logical_name_id,
                resource_id,
                timestamp(1_717_171_700),
            )],
        )
        .await?;

        Ok(())
    }

    async fn insert_name_current_row(&self, row: bigname_storage::NameCurrentRow) -> Result<()> {
        self.seed_snapshot_selector_chain_positions(&row.chain_positions)
            .await?;
        bigname_storage::upsert_name_current_rows(&self.pool, &[row])
            .await
            .context("failed to upsert name_current row for API test")?;
        Ok(())
    }

    async fn insert_record_inventory_current_row(
        &self,
        row: bigname_storage::RecordInventoryCurrentRow,
    ) -> Result<()> {
        bigname_storage::upsert_record_inventory_current_rows(&self.pool, &[row])
            .await
            .context("failed to upsert record_inventory_current row for API test")?;
        Ok(())
    }

    async fn seed_snapshot_selector_chain_positions(&self, chain_positions: &Value) -> Result<()> {
        let Some(positions) = chain_positions.as_object() else {
            return Ok(());
        };

        for position in positions.values() {
            let chain_id = position
                .get("chain_id")
                .and_then(Value::as_str)
                .context("chain_position.chain_id must be present for API selector test seed")?;
            let block_hash = position
                .get("block_hash")
                .and_then(Value::as_str)
                .context("chain_position.block_hash must be present for API selector test seed")?;
            let block_number = position
                .get("block_number")
                .and_then(Value::as_i64)
                .context(
                    "chain_position.block_number must be present for API selector test seed",
                )?;
            let timestamp_value = position
                .get("timestamp")
                .and_then(Value::as_str)
                .context("chain_position.timestamp must be present for API selector test seed")?;
            let timestamp = parse_rfc3339_utc_timestamp(timestamp_value)
                .map_err(|error| anyhow::anyhow!("{error}"))?;

            sqlx::query(
                r#"
                INSERT INTO chain_lineage (
                    chain_id,
                    block_hash,
                    block_number,
                    block_timestamp,
                    canonicality_state
                )
                VALUES ($1, $2, $3, $4, 'finalized'::canonicality_state)
                ON CONFLICT (chain_id, block_hash) DO UPDATE SET
                    block_number = EXCLUDED.block_number,
                    block_timestamp = EXCLUDED.block_timestamp,
                    canonicality_state = EXCLUDED.canonicality_state
                "#,
            )
            .bind(chain_id)
            .bind(block_hash)
            .bind(block_number)
            .bind(timestamp)
            .execute(&self.pool)
            .await
            .with_context(|| {
                format!("failed to seed chain_lineage for {chain_id} block {block_hash}")
            })?;

            sqlx::query(
                r#"
                INSERT INTO chain_checkpoints (
                    chain_id,
                    canonical_block_hash,
                    canonical_block_number,
                    safe_block_hash,
                    safe_block_number,
                    finalized_block_hash,
                    finalized_block_number
                )
                VALUES ($1, $2, $3, $2, $3, $2, $3)
                ON CONFLICT (chain_id) DO UPDATE SET
                    canonical_block_hash = EXCLUDED.canonical_block_hash,
                    canonical_block_number = EXCLUDED.canonical_block_number,
                    safe_block_hash = EXCLUDED.safe_block_hash,
                    safe_block_number = EXCLUDED.safe_block_number,
                    finalized_block_hash = EXCLUDED.finalized_block_hash,
                    finalized_block_number = EXCLUDED.finalized_block_number,
                    updated_at = now()
                "#,
            )
            .bind(chain_id)
            .bind(block_hash)
            .bind(block_number)
            .execute(&self.pool)
            .await
            .with_context(|| format!("failed to seed chain checkpoint for {chain_id}"))?;
        }

        Ok(())
    }

    async fn seed_default_ens_snapshot_selector_position(&self) -> Result<()> {
        self.seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }))
        .await
    }

    async fn rebuild_name_current(&self, logical_name_id: &str) -> Result<()> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for API worker rebuild")?;
        let rebuild_database_url = base_options
            .database(&self.database_name)
            .to_url_lossy()
            .to_string();
        let logical_name_id = logical_name_id.to_owned();
        let logical_name_id_for_seed = logical_name_id.clone();
        let worker_manifest_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../apps/worker/Cargo.toml");

        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = WORKER_CARGO_LOCK
                .lock()
                .expect("worker cargo lock must not be poisoned");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let output = std::process::Command::new(cargo)
                .arg("run")
                .arg("--quiet")
                .arg("--manifest-path")
                .arg(worker_manifest_path)
                .arg("--")
                .arg("name-current")
                .arg("rebuild")
                .arg("--database-url")
                .arg(&rebuild_database_url)
                .arg("--logical-name-id")
                .arg(&logical_name_id)
                .output()
                .with_context(|| {
                    format!(
                        "failed to invoke worker name_current rebuild for {logical_name_id}"
                    )
                })?;

            if !output.status.success() {
                return Err(anyhow::anyhow!(
                    "worker name_current rebuild failed for {logical_name_id}\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                ));
            }

            Ok(())
        })
        .await
        .context("worker name_current rebuild task panicked")??;

        if let Some(row) = bigname_storage::load_name_current(&self.pool, &logical_name_id_for_seed)
            .await
            .with_context(|| {
                format!(
                    "failed to load rebuilt name_current row {logical_name_id_for_seed} for selector seed"
                )
            })?
        {
            self.seed_snapshot_selector_chain_positions(&row.chain_positions)
                .await?;
        }

        Ok(())
    }

    async fn rebuild_address_names_current(&self, address: Option<&str>) -> Result<()> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for API worker address_names rebuild")?;
        let rebuild_database_url = base_options
            .database(&self.database_name)
            .to_url_lossy()
            .to_string();
        let address = address.map(str::to_owned);
        let worker_manifest_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../apps/worker/Cargo.toml");

        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = WORKER_CARGO_LOCK
                .lock()
                .expect("worker cargo lock must not be poisoned");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let mut command = std::process::Command::new(cargo);
            command
                .arg("run")
                .arg("--quiet")
                .arg("--manifest-path")
                .arg(worker_manifest_path)
                .arg("--")
                .arg("address-names-current")
                .arg("rebuild")
                .arg("--database-url")
                .arg(&rebuild_database_url);
            if let Some(address) = address.as_deref() {
                command.arg("--address").arg(address);
            }

            let output = command.output().with_context(|| {
                format!(
                    "failed to invoke worker address_names_current rebuild for {}",
                    address.as_deref().unwrap_or("all")
                )
            })?;

            if !output.status.success() {
                return Err(anyhow::anyhow!(
                    "worker address_names_current rebuild failed for {}\nstdout:\n{}\nstderr:\n{}",
                    address.as_deref().unwrap_or("all"),
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                ));
            }

            Ok(())
        })
        .await
        .context("worker address_names_current rebuild task panicked")??;

        Ok(())
    }

    async fn rebuild_record_inventory_current(&self, resource_id: Uuid) -> Result<()> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for API worker record_inventory rebuild")?;
        let rebuild_database_url = base_options
            .database(&self.database_name)
            .to_url_lossy()
            .to_string();
        let resource_id_value = resource_id;
        let resource_id = resource_id.to_string();
        let worker_manifest_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../apps/worker/Cargo.toml");

        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = WORKER_CARGO_LOCK
                .lock()
                .expect("worker cargo lock must not be poisoned");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let output = std::process::Command::new(cargo)
                .arg("run")
                .arg("--quiet")
                .arg("--manifest-path")
                .arg(worker_manifest_path)
                .arg("--")
                .arg("record-inventory-current")
                .arg("rebuild")
                .arg("--database-url")
                .arg(&rebuild_database_url)
                .arg("--resource-id")
                .arg(&resource_id)
                .output()
                .with_context(|| {
                    format!(
                        "failed to invoke worker record_inventory_current rebuild for {resource_id}"
                    )
                })?;

            if !output.status.success() {
                return Err(anyhow::anyhow!(
                    "worker record_inventory_current rebuild failed for {resource_id}\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                ));
            }

            Ok(())
        })
        .await
        .context("worker record_inventory_current rebuild task panicked")??;

        let rows = sqlx::query(
            r#"
            SELECT chain_positions
            FROM record_inventory_current
            WHERE resource_id = $1
            "#,
        )
        .bind(resource_id_value)
        .fetch_all(&self.pool)
        .await
        .with_context(|| {
            format!(
                "failed to load rebuilt record_inventory_current rows for resource_id {resource_id_value}"
            )
        })?;
        for row in rows {
            let chain_positions = row
                .try_get::<Value, _>("chain_positions")
                .context("record_inventory_current row missing chain_positions")?;
            self.seed_snapshot_selector_chain_positions(&chain_positions)
                .await?;
        }

        Ok(())
    }

    async fn rebuild_permissions_current(&self, resource_id: Option<Uuid>) -> Result<()> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for API worker permissions rebuild")?;
        let rebuild_database_url = base_options
            .database(&self.database_name)
            .to_url_lossy()
            .to_string();
        let resource_id = resource_id.map(|value| value.to_string());
        let worker_manifest_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../apps/worker/Cargo.toml");

        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = WORKER_CARGO_LOCK
                .lock()
                .expect("worker cargo lock must not be poisoned");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let mut command = std::process::Command::new(cargo);
            command
                .arg("run")
                .arg("--quiet")
                .arg("--manifest-path")
                .arg(worker_manifest_path)
                .arg("--")
                .arg("permissions-current")
                .arg("rebuild")
                .arg("--database-url")
                .arg(&rebuild_database_url);
            if let Some(resource_id) = resource_id.as_deref() {
                command.arg("--resource-id").arg(resource_id);
            }

            let output = command.output().with_context(|| {
                format!(
                    "failed to invoke worker permissions_current rebuild for {}",
                    resource_id.as_deref().unwrap_or("all")
                )
            })?;

            if !output.status.success() {
                return Err(anyhow::anyhow!(
                    "worker permissions_current rebuild failed for {}\nstdout:\n{}\nstderr:\n{}",
                    resource_id.as_deref().unwrap_or("all"),
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                ));
            }

            Ok(())
        })
        .await
        .context("worker permissions_current rebuild task panicked")??;

        Ok(())
    }

    async fn rebuild_resolver_current(
        &self,
        chain_id: Option<&str>,
        resolver_address: Option<&str>,
    ) -> Result<()> {
        match (chain_id, resolver_address) {
            (Some(_), Some(_)) | (None, None) => {}
            _ => {
                return Err(anyhow::anyhow!(
                    "resolver_current rebuild requires both chain_id and resolver_address when targeting one resolver"
                ));
            }
        }

        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for API worker resolver rebuild")?;
        let rebuild_database_url = base_options
            .database(&self.database_name)
            .to_url_lossy()
            .to_string();
        let chain_id = chain_id.map(str::to_owned);
        let resolver_address = resolver_address.map(str::to_owned);
        let worker_manifest_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../apps/worker/Cargo.toml");

        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = WORKER_CARGO_LOCK
                .lock()
                .expect("worker cargo lock must not be poisoned");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let mut command = std::process::Command::new(cargo);
            command
                .arg("run")
                .arg("--quiet")
                .arg("--manifest-path")
                .arg(worker_manifest_path)
                .arg("--")
                .arg("resolver-current")
                .arg("rebuild")
                .arg("--database-url")
                .arg(&rebuild_database_url);
            if let (Some(chain_id), Some(resolver_address)) =
                (chain_id.as_deref(), resolver_address.as_deref())
            {
                command.arg("--chain-id").arg(chain_id);
                command.arg("--resolver-address").arg(resolver_address);
            }

            let output = command.output().with_context(|| {
                format!(
                    "failed to invoke worker resolver_current rebuild for {}",
                    resolver_address.as_deref().unwrap_or("all")
                )
            })?;

            if !output.status.success() {
                return Err(anyhow::anyhow!(
                    "worker resolver_current rebuild failed for {}\nstdout:\n{}\nstderr:\n{}",
                    resolver_address.as_deref().unwrap_or("all"),
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                ));
            }

            Ok(())
        })
        .await
        .context("worker resolver_current rebuild task panicked")??;

        Ok(())
    }

    async fn seed_basenames_exact_name_rebuild_inputs(
        &self,
        logical_name_id: &str,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        bigname_storage::upsert_raw_blocks(
            &self.pool,
            &[
                raw_block("base-mainnet", "0xbase-surface", None, 98, 1_717_171_698),
                raw_block("base-mainnet", "0xbase-resource", None, 99, 1_717_171_699),
                raw_block("base-mainnet", "0xbase-binding", None, 100, 1_717_171_700),
                raw_block("base-mainnet", "0xbase-grant", None, 101, 1_717_171_701),
                raw_block("base-mainnet", "0xbase-authority", None, 102, 1_717_171_702),
                raw_block("base-mainnet", "0xbase-resolver", None, 103, 1_717_171_703),
            ],
        )
        .await
        .context("failed to upsert raw blocks for basenames exact-name API test")?;
        bigname_storage::upsert_name_surfaces(
            &self.pool,
            &[NameSurface {
                logical_name_id: logical_name_id.to_owned(),
                namespace: "basenames".to_owned(),
                input_name: "alice.base.eth".to_owned(),
                canonical_display_name: "Alice.base.eth".to_owned(),
                normalized_name: "alice.base.eth".to_owned(),
                dns_encoded_name: b"alice.base.eth".to_vec(),
                namehash: "namehash:alice.base.eth".to_owned(),
                labelhashes: vec!["labelhash:alice.base.eth".to_owned()],
                normalizer_version: "ensip15@2026-04-16".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xbase-surface".to_owned(),
                block_number: 98,
                provenance: json!({"seed": "basenames_exact_name_surface"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await
        .context("failed to upsert basenames name surface for API test")?;
        bigname_storage::upsert_token_lineages(
            &self.pool,
            &[TokenLineage {
                token_lineage_id,
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xbase-resource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "basenames_exact_name_token_lineage"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await
        .context("failed to upsert basenames token lineage for API test")?;
        bigname_storage::upsert_resources(
            &self.pool,
            &[Resource {
                resource_id,
                token_lineage_id: Some(token_lineage_id),
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xbase-resource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "basenames_exact_name_resource"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await
        .context("failed to upsert basenames resource for API test")?;
        bigname_storage::upsert_surface_bindings(
            &self.pool,
            &[SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.to_owned(),
                resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from: timestamp(1_717_171_700),
                active_to: None,
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xbase-binding".to_owned(),
                block_number: 100,
                provenance: json!({"seed": "basenames_exact_name_binding"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await
        .context("failed to upsert basenames surface binding for API test")?;
        bigname_storage::upsert_normalized_events(
            &self.pool,
            &[
                NormalizedEvent {
                    event_identity: "api-test:basenames:grant".to_owned(),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "RegistrationGranted".to_owned(),
                    source_family: "basenames_base_registrar".to_owned(),
                    manifest_version: 3,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(101),
                    block_hash: Some("0xbase-grant".to_owned()),
                    transaction_hash: Some("0xtxbasegrant".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:basenames:grant"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:base-mainnet:alice",
                        "registrant": "0x00000000000000000000000000000000000000aa",
                        "expiry": 1_900_000_000_i64,
                    }),
                },
                NormalizedEvent {
                    event_identity: "api-test:basenames:authority".to_owned(),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "AuthorityTransferred".to_owned(),
                    source_family: "basenames_base_registry".to_owned(),
                    manifest_version: 3,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(102),
                    block_hash: Some("0xbase-authority".to_owned()),
                    transaction_hash: Some("0xtxbaseauthority".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:basenames:authority"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "owner": "0x00000000000000000000000000000000000000bb",
                    }),
                },
                NormalizedEvent {
                    event_identity: "api-test:basenames:resolver".to_owned(),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "ResolverChanged".to_owned(),
                    source_family: "basenames_base_resolver".to_owned(),
                    manifest_version: 4,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(103),
                    block_hash: Some("0xbase-resolver".to_owned()),
                    transaction_hash: Some("0xtxbaseresolver".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:basenames:resolver"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "resolver": "0x0000000000000000000000000000000000000abc",
                        "namehash": "namehash:alice.base.eth",
                    }),
                },
            ],
        )
        .await
        .context("failed to upsert basenames normalized events for API test")?;

        Ok(())
    }

    async fn seed_basenames_control_vector_rebuild_inputs(
        &self,
        logical_name_id: &str,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
        scenario: BasenamesControlVectorScenario,
    ) -> Result<()> {
        let normalized_name = logical_name_id
            .split_once(':')
            .map(|(_, normalized_name)| normalized_name)
            .expect("logical_name_id must include namespace");

        bigname_storage::upsert_raw_blocks(
            &self.pool,
            &[
                raw_block("base-mainnet", "0xbase-surface", None, 98, 1_717_181_698),
                raw_block("base-mainnet", "0xbase-resource", None, 99, 1_717_181_699),
                raw_block("base-mainnet", "0xbase-binding", None, 100, 1_717_181_700),
                raw_block("base-mainnet", "0xbase-grant", None, 101, 1_717_181_701),
                raw_block("base-mainnet", "0xbase-authority", None, 102, 1_717_181_702),
                raw_block("base-mainnet", "0xbase-token", None, 103, 1_717_181_703),
                raw_block(
                    "base-mainnet",
                    "0xbase-authority-final",
                    None,
                    104,
                    1_717_181_704,
                ),
                raw_block("base-mainnet", "0xbase-resolver", None, 105, 1_717_181_705),
            ],
        )
        .await
        .context("failed to upsert raw blocks for Basenames control-vector API test")?;
        bigname_storage::upsert_name_surfaces(
            &self.pool,
            &[NameSurface {
                logical_name_id: logical_name_id.to_owned(),
                namespace: "basenames".to_owned(),
                input_name: normalized_name.to_owned(),
                canonical_display_name: normalized_name.to_owned(),
                normalized_name: normalized_name.to_owned(),
                dns_encoded_name: normalized_name.as_bytes().to_vec(),
                namehash: format!("namehash:{normalized_name}"),
                labelhashes: vec![format!("labelhash:{normalized_name}")],
                normalizer_version: "ensip15@2026-04-16".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xbase-surface".to_owned(),
                block_number: 98,
                provenance: json!({"seed": "basenames_control_vector_surface"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await
        .context("failed to upsert Basenames control-vector surface for API test")?;
        bigname_storage::upsert_token_lineages(
            &self.pool,
            &[TokenLineage {
                token_lineage_id,
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xbase-resource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "basenames_control_vector_token_lineage"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await
        .context("failed to upsert Basenames control-vector token lineage for API test")?;
        bigname_storage::upsert_resources(
            &self.pool,
            &[Resource {
                resource_id,
                token_lineage_id: Some(token_lineage_id),
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xbase-resource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "basenames_control_vector_resource"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await
        .context("failed to upsert Basenames control-vector resource for API test")?;
        bigname_storage::upsert_surface_bindings(
            &self.pool,
            &[SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.to_owned(),
                resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from: timestamp(1_717_181_700),
                active_to: None,
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xbase-binding".to_owned(),
                block_number: 100,
                provenance: json!({"seed": "basenames_control_vector_binding"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await
        .context("failed to upsert Basenames control-vector surface binding for API test")?;

        let mut events = vec![NormalizedEvent {
            event_identity: format!("api-test:{logical_name_id}:grant"),
            namespace: "basenames".to_owned(),
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(resource_id),
            event_kind: "RegistrationGranted".to_owned(),
            source_family: "basenames_base_registrar".to_owned(),
            manifest_version: 3,
            source_manifest_id: None,
            chain_id: Some("base-mainnet".to_owned()),
            block_number: Some(101),
            block_hash: Some("0xbase-grant".to_owned()),
            transaction_hash: Some(format!("0xtx:{logical_name_id}:grant")),
            log_index: Some(0),
            raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("api-test:{logical_name_id}:grant")}),
            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            before_state: json!({}),
            after_state: json!({
                "authority_kind": "registrar",
                "authority_key": format!("registrar:base-mainnet:{normalized_name}"),
                "registrant": match scenario {
                    BasenamesControlVectorScenario::NftOnly => "0x00000000000000000000000000000000000000a1",
                    BasenamesControlVectorScenario::ManagementOnly => "0x00000000000000000000000000000000000000a2",
                    BasenamesControlVectorScenario::FullTransfer => "0x00000000000000000000000000000000000000a3",
                },
                "expiry": 1_900_000_000_i64,
            }),
        }];

        match scenario {
            BasenamesControlVectorScenario::NftOnly => {
                events.push(NormalizedEvent {
                    event_identity: format!("api-test:{logical_name_id}:authority"),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "AuthorityTransferred".to_owned(),
                    source_family: "basenames_base_registry".to_owned(),
                    manifest_version: 3,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(102),
                    block_hash: Some("0xbase-authority".to_owned()),
                    transaction_hash: Some(format!("0xtx:{logical_name_id}:authority")),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("api-test:{logical_name_id}:authority")}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({
                        "owner": "0x00000000000000000000000000000000000000a1",
                    }),
                    after_state: json!({
                        "owner": "0x00000000000000000000000000000000000000b1",
                    }),
                });
                events.push(NormalizedEvent {
                    event_identity: format!("api-test:{logical_name_id}:token"),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "TokenControlTransferred".to_owned(),
                    source_family: "basenames_base_registrar".to_owned(),
                    manifest_version: 3,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(103),
                    block_hash: Some("0xbase-token".to_owned()),
                    transaction_hash: Some(format!("0xtx:{logical_name_id}:token")),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("api-test:{logical_name_id}:token")}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({
                        "from": "0x00000000000000000000000000000000000000a1",
                    }),
                    after_state: json!({
                        "to": "0x00000000000000000000000000000000000000c1",
                    }),
                });
            }
            BasenamesControlVectorScenario::ManagementOnly => {
                events.push(NormalizedEvent {
                    event_identity: format!("api-test:{logical_name_id}:authority"),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "AuthorityTransferred".to_owned(),
                    source_family: "basenames_base_registry".to_owned(),
                    manifest_version: 3,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(102),
                    block_hash: Some("0xbase-authority".to_owned()),
                    transaction_hash: Some(format!("0xtx:{logical_name_id}:authority")),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("api-test:{logical_name_id}:authority")}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({
                        "owner": "0x00000000000000000000000000000000000000a2",
                    }),
                    after_state: json!({
                        "owner": "0x00000000000000000000000000000000000000b2",
                    }),
                });
            }
            BasenamesControlVectorScenario::FullTransfer => {
                events.push(NormalizedEvent {
                    event_identity: format!("api-test:{logical_name_id}:authority"),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "AuthorityTransferred".to_owned(),
                    source_family: "basenames_base_registry".to_owned(),
                    manifest_version: 3,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(102),
                    block_hash: Some("0xbase-authority".to_owned()),
                    transaction_hash: Some(format!("0xtx:{logical_name_id}:authority")),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("api-test:{logical_name_id}:authority")}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({
                        "owner": "0x00000000000000000000000000000000000000a3",
                    }),
                    after_state: json!({
                        "owner": "0x00000000000000000000000000000000000000b3",
                    }),
                });
                events.push(NormalizedEvent {
                    event_identity: format!("api-test:{logical_name_id}:token"),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "TokenControlTransferred".to_owned(),
                    source_family: "basenames_base_registrar".to_owned(),
                    manifest_version: 3,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(103),
                    block_hash: Some("0xbase-token".to_owned()),
                    transaction_hash: Some(format!("0xtx:{logical_name_id}:token")),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("api-test:{logical_name_id}:token")}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({
                        "from": "0x00000000000000000000000000000000000000a3",
                    }),
                    after_state: json!({
                        "to": "0x00000000000000000000000000000000000000c3",
                    }),
                });
                events.push(NormalizedEvent {
                    event_identity: format!("api-test:{logical_name_id}:authority-final"),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "AuthorityTransferred".to_owned(),
                    source_family: "basenames_base_registry".to_owned(),
                    manifest_version: 3,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(104),
                    block_hash: Some("0xbase-authority-final".to_owned()),
                    transaction_hash: Some(format!("0xtx:{logical_name_id}:authority-final")),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("api-test:{logical_name_id}:authority-final")}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({
                        "owner": "0x00000000000000000000000000000000000000b3",
                    }),
                    after_state: json!({
                        "owner": "0x00000000000000000000000000000000000000c3",
                    }),
                });
            }
        }

        events.push(NormalizedEvent {
            event_identity: format!("api-test:{logical_name_id}:resolver"),
            namespace: "basenames".to_owned(),
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(resource_id),
            event_kind: "ResolverChanged".to_owned(),
            source_family: "basenames_base_resolver".to_owned(),
            manifest_version: 4,
            source_manifest_id: None,
            chain_id: Some("base-mainnet".to_owned()),
            block_number: Some(105),
            block_hash: Some("0xbase-resolver".to_owned()),
            transaction_hash: Some(format!("0xtx:{logical_name_id}:resolver")),
            log_index: Some(0),
            raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("api-test:{logical_name_id}:resolver")}),
            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            before_state: json!({}),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000abc",
                "namehash": format!("namehash:{normalized_name}"),
            }),
        });

        bigname_storage::upsert_normalized_events(&self.pool, &events)
            .await
            .context("failed to upsert Basenames control-vector normalized events for API test")?;

        Ok(())
    }

    async fn seed_ensv2_address_names_rebuild_inputs(
        &self,
        logical_name_id: &str,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
        registrant: &str,
        controller: &str,
    ) -> Result<()> {
        let normalized_name = logical_name_id
            .split_once(':')
            .map(|(_, normalized_name)| normalized_name)
            .expect("logical_name_id must include namespace");

        bigname_storage::upsert_raw_blocks(
            &self.pool,
            &[
                raw_block(
                    "ethereum-sepolia",
                    "0xensv2-surface",
                    None,
                    201,
                    1_717_182_201,
                ),
                raw_block(
                    "ethereum-sepolia",
                    "0xensv2-resource",
                    None,
                    202,
                    1_717_182_202,
                ),
                raw_block(
                    "ethereum-sepolia",
                    "0xensv2-binding",
                    None,
                    203,
                    1_717_182_203,
                ),
                raw_block(
                    "ethereum-sepolia",
                    "0xensv2-grant",
                    None,
                    204,
                    1_717_182_204,
                ),
                raw_block(
                    "ethereum-sepolia",
                    "0xensv2-authority",
                    None,
                    205,
                    1_717_182_205,
                ),
                raw_block(
                    "ethereum-sepolia",
                    "0xensv2-regen",
                    None,
                    206,
                    1_717_182_206,
                ),
            ],
        )
        .await
        .context("failed to upsert raw blocks for ENSv2 address-name API test")?;
        bigname_storage::upsert_name_surfaces(
            &self.pool,
            &[NameSurface {
                logical_name_id: logical_name_id.to_owned(),
                namespace: "ens".to_owned(),
                input_name: normalized_name.to_owned(),
                canonical_display_name: normalized_name.to_owned(),
                normalized_name: normalized_name.to_owned(),
                dns_encoded_name: normalized_name.as_bytes().to_vec(),
                namehash: format!("namehash:{normalized_name}"),
                labelhashes: vec![format!("labelhash:{normalized_name}")],
                normalizer_version: "ensip15@2026-04-16".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xensv2-surface".to_owned(),
                block_number: 201,
                provenance: json!({"seed": "ensv2_address_names_surface"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await
        .context("failed to upsert ENSv2 address-name surface for API test")?;
        bigname_storage::upsert_token_lineages(
            &self.pool,
            &[TokenLineage {
                token_lineage_id,
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xensv2-resource".to_owned(),
                block_number: 202,
                provenance: json!({"seed": "ensv2_address_names_token_lineage"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await
        .context("failed to upsert ENSv2 address-name token lineage for API test")?;
        bigname_storage::upsert_resources(
            &self.pool,
            &[Resource {
                resource_id,
                token_lineage_id: Some(token_lineage_id),
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xensv2-resource".to_owned(),
                block_number: 202,
                provenance: json!({
                    "seed": "ensv2_address_names_resource",
                    "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                }),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await
        .context("failed to upsert ENSv2 address-name resource for API test")?;
        bigname_storage::upsert_surface_bindings(
            &self.pool,
            &[SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.to_owned(),
                resource_id,
                binding_kind: SurfaceBindingKind::LinkedSubregistryPath,
                active_from: timestamp(1_717_182_203),
                active_to: None,
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xensv2-binding".to_owned(),
                block_number: 203,
                provenance: json!({
                    "seed": "ensv2_address_names_binding",
                    "binding_kind": "linked_subregistry_path",
                }),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await
        .context("failed to upsert ENSv2 address-name surface binding for API test")?;
        bigname_storage::upsert_normalized_events(
            &self.pool,
            &[
                NormalizedEvent {
                    event_identity: format!("api-test:{logical_name_id}:ensv2-grant"),
                    namespace: "ens".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "RegistrationGranted".to_owned(),
                    source_family: "ens_v2_registry_l1".to_owned(),
                    manifest_version: 11,
                    source_manifest_id: None,
                    chain_id: Some("ethereum-sepolia".to_owned()),
                    block_number: Some(204),
                    block_hash: Some("0xensv2-grant".to_owned()),
                    transaction_hash: Some(format!("0xtx:{logical_name_id}:ensv2-grant")),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("api-test:{logical_name_id}:ensv2-grant")}),
                    derivation_kind: "ens_v2_registry_resource_surface".to_owned(),
                    canonicality_state: CanonicalityState::Finalized,
                    before_state: json!({}),
                    after_state: json!({
                        "authority_kind": "ens_v2_registry",
                        "authority_key": format!("ens-v2-registry:ethereum-sepolia:{normalized_name}:0xeac"),
                        "registrant": registrant,
                        "expiry": 1_900_000_000_i64,
                        "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                        "status": "registered",
                    }),
                },
                NormalizedEvent {
                    event_identity: format!("api-test:{logical_name_id}:ensv2-authority"),
                    namespace: "ens".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "AuthorityTransferred".to_owned(),
                    source_family: "ens_v2_registry_l1".to_owned(),
                    manifest_version: 11,
                    source_manifest_id: None,
                    chain_id: Some("ethereum-sepolia".to_owned()),
                    block_number: Some(205),
                    block_hash: Some("0xensv2-authority".to_owned()),
                    transaction_hash: Some(format!("0xtx:{logical_name_id}:ensv2-authority")),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("api-test:{logical_name_id}:ensv2-authority")}),
                    derivation_kind: "ens_v2_registry_resource_surface".to_owned(),
                    canonicality_state: CanonicalityState::Finalized,
                    before_state: json!({
                        "owner": registrant,
                    }),
                    after_state: json!({
                        "owner": controller,
                        "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                    }),
                },
                NormalizedEvent {
                    event_identity: format!("api-test:{logical_name_id}:ensv2-regen"),
                    namespace: "ens".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "TokenRegenerated".to_owned(),
                    source_family: "ens_v2_registry_l1".to_owned(),
                    manifest_version: 11,
                    source_manifest_id: None,
                    chain_id: Some("ethereum-sepolia".to_owned()),
                    block_number: Some(206),
                    block_hash: Some("0xensv2-regen".to_owned()),
                    transaction_hash: Some(format!("0xtx:{logical_name_id}:ensv2-regen")),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("api-test:{logical_name_id}:ensv2-regen")}),
                    derivation_kind: "ens_v2_registry_resource_surface".to_owned(),
                    canonicality_state: CanonicalityState::Finalized,
                    before_state: json!({
                        "token_id": "0x01",
                    }),
                    after_state: json!({
                        "old_token_id": "0x01",
                        "new_token_id": "0x02",
                        "resource_id": resource_id.to_string(),
                    }),
                },
            ],
        )
        .await
        .context("failed to upsert ENSv2 address-name normalized events for API test")?;

        Ok(())
    }

    async fn seed_basenames_resolution_rebuild_inputs(
        &self,
        logical_name_id: &str,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        self.seed_basenames_exact_name_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

        bigname_storage::upsert_normalized_events(
            &self.pool,
            &[
                NormalizedEvent {
                    event_identity: "api-test:basenames:record-version".to_owned(),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "RecordVersionChanged".to_owned(),
                    source_family: "basenames_base_resolver".to_owned(),
                    manifest_version: 4,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(103),
                    block_hash: Some("0xbase-resolver".to_owned()),
                    transaction_hash: Some("0xtxbaseresolver".to_owned()),
                    log_index: Some(1),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:basenames:record-version"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({
                        "record_version": 6,
                    }),
                    after_state: json!({
                        "record_version": 7,
                    }),
                },
                NormalizedEvent {
                    event_identity: "api-test:basenames:addr".to_owned(),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "RecordChanged".to_owned(),
                    source_family: "basenames_base_resolver".to_owned(),
                    manifest_version: 4,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(103),
                    block_hash: Some("0xbase-resolver".to_owned()),
                    transaction_hash: Some("0xtxbaseresolver".to_owned()),
                    log_index: Some(2),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:basenames:addr"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                    }),
                },
                NormalizedEvent {
                    event_identity: "api-test:basenames:text".to_owned(),
                    namespace: "basenames".to_owned(),
                    logical_name_id: Some(logical_name_id.to_owned()),
                    resource_id: Some(resource_id),
                    event_kind: "RecordChanged".to_owned(),
                    source_family: "basenames_base_resolver".to_owned(),
                    manifest_version: 4,
                    source_manifest_id: None,
                    chain_id: Some("base-mainnet".to_owned()),
                    block_number: Some(103),
                    block_hash: Some("0xbase-resolver".to_owned()),
                    transaction_hash: Some("0xtxbaseresolver".to_owned()),
                    log_index: Some(3),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:basenames:text"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "record_key": "text",
                        "record_family": "text",
                        "selector_key": null,
                    }),
                },
            ],
        )
        .await
        .context("failed to upsert basenames resolution events for API test")?;

        Ok(())
    }

    async fn create_primary_names_current_table(&self) -> Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE primary_names_current (
                    address TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    coin_type TEXT NOT NULL,
                    claim_status TEXT NOT NULL,
                    raw_claim_name TEXT,
                    normalized_claim_name TEXT,
                    claim_provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                    PRIMARY KEY (address, namespace, coin_type)
                )
                "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to create primary_names_current for API tests")?;
        Ok(())
    }

    async fn insert_primary_name_current_row(
        &self,
        address: &str,
        namespace: &str,
        coin_type: &str,
    ) -> Result<()> {
        self.insert_primary_name_current_claim_row(
            address,
            namespace,
            coin_type,
            PrimaryNameClaimStatus::Unsupported,
            None,
        )
        .await
    }

    async fn insert_primary_name_current_claim_row(
        &self,
        address: &str,
        namespace: &str,
        coin_type: &str,
        claim_status: PrimaryNameClaimStatus,
        raw_claim_name: Option<&str>,
    ) -> Result<()> {
        self.insert_primary_name_current_claim_row_with_provenance(
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            json!({}),
        )
        .await
    }

    async fn insert_primary_name_current_claim_row_with_provenance(
        &self,
        address: &str,
        namespace: &str,
        coin_type: &str,
        claim_status: PrimaryNameClaimStatus,
        raw_claim_name: Option<&str>,
        claim_provenance: Value,
    ) -> Result<()> {
        upsert_primary_name_current_rows(
            &self.pool,
            &[PrimaryNameCurrentRow {
                address: address.to_ascii_lowercase(),
                namespace: namespace.to_owned(),
                coin_type: coin_type.to_owned(),
                claim_status,
                raw_claim_name: raw_claim_name.map(str::to_owned),
                claim_provenance,
            }],
        )
        .await
        .context("failed to upsert primary_names_current row for API tests")?;
        Ok(())
    }

    async fn insert_primary_name_current_normalized_claim_name(
        &self,
        address: &str,
        namespace: &str,
        coin_type: &str,
        normalized_claim_name: Option<&str>,
    ) -> Result<()> {
        let row = load_primary_name_current(&self.pool, address, namespace, coin_type)
            .await
            .context("failed to load primary_names_current row for API test")?
            .with_context(|| {
                format!(
                    "missing primary_names_current row for API test address {} namespace {} coin_type {}",
                    address, namespace, coin_type
                )
            })?;

        upsert_primary_name_current_snapshots(
            &self.pool,
            &[PrimaryNameCurrentSnapshot {
                row,
                normalized_claim_name: normalized_claim_name.map(str::to_owned),
            }],
        )
        .await
        .context("failed to upsert primary_names_current snapshot for API test")?;
        Ok(())
    }

    async fn cleanup(self) -> Result<()> {
        let Self {
            database,
            pool,
            database_name: _,
        } = self;
        drop(pool);
        database.cleanup().await
    }
}

async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T> {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .context("failed to read API response body")?;
    serde_json::from_slice(&bytes).context("failed to decode API response JSON")
}

fn encode_query_value(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte))
            }
            _ => {
                std::fmt::Write::write_fmt(&mut encoded, format_args!("%{byte:02X}"))
                    .expect("writing to a String cannot fail");
            }
        }
    }
    encoded
}

async fn assert_public_invalid_input_response(
    response: Response,
    expected_message_fragment: &str,
) -> Result<()> {
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert!(
        payload.error.message.contains(expected_message_fragment),
        "expected invalid_input message to contain {expected_message_fragment:?}, got {:?}",
        payload.error.message
    );
    assert!(payload.error.details.is_empty());
    Ok(())
}

fn rewrite_cursor(cursor: &str, rewrite: impl FnOnce(&mut CursorEnvelope)) -> String {
    let decoded = decode_hex(cursor).expect("pagination cursor must be valid hex");
    let mut envelope: CursorEnvelope =
        serde_json::from_slice(&decoded).expect("pagination cursor must decode as JSON");
    rewrite(&mut envelope);
    encode_cursor(&envelope)
}

async fn assert_invalid_cursor_request(state: AppState, uri: impl Into<String>) -> Result<()> {
    let uri = uri.into();
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri(uri.as_str())
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .with_context(|| format!("invalid cursor request failed for {uri}"))?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "cursor must be a valid pagination cursor"
    );
    assert!(payload.error.details.is_empty());
    Ok(())
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

fn primary_name_reverse_changed_event(
    event_identity: &str,
    address: &str,
    coin_type: &str,
    block_number: i64,
    log_index: i64,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    let normalized_address = address.to_ascii_lowercase();
    let reverse_label = normalized_address.trim_start_matches("0x").to_owned();

    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "ReverseChanged".to_owned(),
        source_family: "ens_v1_reverse_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xblock{block_number:064x}")),
        transaction_hash: Some(format!("0xtx{block_number:064x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v1_reverse_claim".to_owned(),
        canonicality_state,
        before_state: json!({}),
        after_state: json!({
            "source_event": "ReverseClaimed",
            "address": normalized_address,
            "coin_type": coin_type,
            "namespace": "ens",
            "reverse_namespace": "ens",
            "reverse_label": reverse_label,
            "reverse_name": format!("{reverse_label}.addr.reverse"),
            "reverse_node": format!("0x{block_number:064x}"),
            "claim_provenance": {
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
                "contract_instance_id": format!("00000000-0000-0000-0000-{block_number:012x}"),
                "emitting_address": "0x00000000000000000000000000000000000000ad",
            },
        }),
    }
}

fn primary_name_reverse_linked_name_event(
    event_identity: &str,
    address: &str,
    coin_type: &str,
    raw_name: Option<&str>,
    block_number: i64,
    log_index: i64,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    let normalized_address = address.to_ascii_lowercase();
    let reverse_label = normalized_address.trim_start_matches("0x").to_owned();
    let mut after_state = serde_json::Map::from_iter([
        ("record_key".to_owned(), json!("name")),
        ("record_family".to_owned(), json!("name")),
        ("selector_key".to_owned(), Value::Null),
        (
            "primary_claim_source".to_owned(),
            json!({
                "address": normalized_address,
                "namespace": "ens",
                "coin_type": coin_type,
                "reverse_name": format!("{reverse_label}.addr.reverse"),
                "reverse_node": format!("0x{block_number:064x}"),
                "claim_provenance": {
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": format!("00000000-0000-0000-0000-{block_number:012x}"),
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }),
        ),
    ]);
    if let Some(raw_name) = raw_name {
        after_state.insert("raw_name".to_owned(), json!(raw_name));
    }

    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "RecordChanged".to_owned(),
        source_family: "ens_v1_unwrapped_authority".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xclaimblock{block_number:064x}")),
        transaction_hash: Some(format!("0xclaimtx{block_number:064x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state,
        before_state: json!({}),
        after_state: Value::Object(after_state),
    }
}

fn basenames_primary_name_reverse_changed_event(
    event_identity: &str,
    address: &str,
    coin_type: &str,
    block_number: i64,
    log_index: i64,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    let normalized_address = address.to_ascii_lowercase();
    let reverse_label = normalized_address.trim_start_matches("0x").to_owned();

    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "basenames".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "ReverseChanged".to_owned(),
        source_family: "basenames_base_primary".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("base-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xbaseblock{block_number:064x}")),
        transaction_hash: Some(format!("0xbasetx{block_number:064x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "base-mainnet",
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v1_reverse_claim".to_owned(),
        canonicality_state,
        before_state: json!({}),
        after_state: json!({
            "source_event": "ReverseClaimed",
            "address": normalized_address,
            "coin_type": coin_type,
            "namespace": "basenames",
            "reverse_namespace": "basenames",
            "reverse_label": reverse_label,
            "reverse_name": format!("{reverse_label}.addr.reverse"),
            "reverse_node": format!("0x{block_number:064x}"),
            "claim_provenance": {
                "source_family": "basenames_base_primary",
                "contract_role": "reverse_registrar",
                "contract_instance_id": format!("00000000-0000-0000-0000-{block_number:012x}"),
                "emitting_address": "0x00000000000000000000000000000000000000ad",
            },
        }),
    }
}

fn basenames_primary_name_reverse_linked_name_event(
    event_identity: &str,
    address: &str,
    coin_type: &str,
    raw_name: Option<&str>,
    block_number: i64,
    log_index: i64,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    let normalized_address = address.to_ascii_lowercase();
    let reverse_label = normalized_address.trim_start_matches("0x").to_owned();
    let mut after_state = serde_json::Map::from_iter([
        ("record_key".to_owned(), json!("name")),
        ("record_family".to_owned(), json!("name")),
        ("selector_key".to_owned(), Value::Null),
        (
            "primary_claim_source".to_owned(),
            json!({
                "address": normalized_address,
                "namespace": "basenames",
                "coin_type": coin_type,
                "reverse_name": format!("{reverse_label}.addr.reverse"),
                "reverse_node": format!("0x{block_number:064x}"),
                "claim_provenance": {
                    "source_family": "basenames_base_primary",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": format!("00000000-0000-0000-0000-{block_number:012x}"),
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }),
        ),
    ]);
    if let Some(raw_name) = raw_name {
        after_state.insert("raw_name".to_owned(), json!(raw_name));
    }

    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "basenames".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "RecordChanged".to_owned(),
        source_family: "basenames_base_resolver".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("base-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xbaseclaimblock{block_number:064x}")),
        transaction_hash: Some(format!("0xbaseclaimtx{block_number:064x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "base-mainnet",
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state,
        before_state: json!({}),
        after_state: Value::Object(after_state),
    }
}

fn raw_block(
    chain_id: &str,
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
    block_timestamp: i64,
) -> RawBlock {
    RawBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(str::to_owned),
        block_number,
        block_timestamp: timestamp(block_timestamp),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn resource(resource_id: Uuid) -> Resource {
    Resource {
        resource_id,
        token_lineage_id: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xresource".to_owned(),
        block_number: 99,
        provenance: json!({"seed": "resource"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn name_surface(logical_name_id: &str) -> NameSurface {
    let (namespace, normalized_name) = logical_name_id
        .split_once(':')
        .expect("logical_name_id must include namespace");
    let chain_id = chain_id_for_namespace(namespace);

    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: namespace.to_owned(),
        input_name: normalized_name.to_owned(),
        canonical_display_name: "Alice.eth".to_owned(),
        normalized_name: normalized_name.to_owned(),
        dns_encoded_name: vec![5, b'a', b'l', b'i', b'c', b'e'],
        namehash: format!("namehash:{normalized_name}"),
        labelhashes: vec!["labelhash:alice".to_owned()],
        normalizer_version: "uts46-v1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: chain_id.to_owned(),
        block_hash: "0xsurface".to_owned(),
        block_number: 98,
        provenance: json!({"seed": "surface"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn surface_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
    active_from: OffsetDateTime,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from,
        active_to: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xbinding".to_owned(),
        block_number: 100,
        provenance: json!({"seed": "binding"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

#[allow(clippy::too_many_arguments)]
fn history_event(
    event_identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    chain_id: Option<&str>,
    block_number: Option<i64>,
    block_hash: Option<&str>,
    transaction_hash: Option<&str>,
    log_index: Option<i64>,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: logical_name_id.map(str::to_owned),
        resource_id,
        event_kind: "HistoryEvent".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 7,
        source_manifest_id: None,
        chain_id: chain_id.map(str::to_owned),
        block_number,
        block_hash: block_hash.map(str::to_owned),
        transaction_hash: transaction_hash.map(str::to_owned),
        log_index,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "event_identity": event_identity,
        }),
        derivation_kind: "history_test".to_owned(),
        canonicality_state,
        before_state: json!({
            "provenance": {
                "before": event_identity,
            }
        }),
        after_state: json!({
            "provenance": {
                "after": event_identity,
            },
            "coverage": {
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["normalized_events"],
                "enumeration_basis": event_identity,
                "unsupported_reason": null,
            }
        }),
    }
}

fn authority_history_event(seed: AuthorityHistorySeed<'_>) -> NormalizedEvent {
    NormalizedEvent {
        namespace: seed.namespace.to_owned(),
        event_kind: seed.event_kind.to_owned(),
        source_family: "ens_v1_registrar_l1".to_owned(),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        after_state: seed.after_state,
        before_state: json!({}),
        ..history_event(
            seed.event_identity,
            Some(seed.logical_name_id),
            Some(seed.resource_id),
            Some("ethereum-mainnet"),
            Some(seed.block_number),
            Some(seed.block_hash),
            Some(&format!("0xtx{}", seed.block_number)),
            Some(0),
            CanonicalityState::Canonical,
        )
    }
}

fn history_event_identities(payload: &HistoryResponse) -> Vec<&str> {
    payload
        .data
        .iter()
        .map(|row| {
            row.get("event_identity")
                .and_then(Value::as_str)
                .expect("history row must include event_identity")
        })
        .collect()
}

fn permission_current_row(
    resource_id: Uuid,
    subject: &str,
    scope: PermissionScope,
    manifest_version: i64,
    block_number: i64,
) -> PermissionsCurrentRow {
    PermissionsCurrentRow {
        resource_id,
        subject: subject.to_owned(),
        scope,
        effective_powers: json!([
            "set_resolver",
            if manifest_version % 2 == 0 {
                "create_subnames"
            } else {
                "set_records"
            }
        ]),
        grant_source: json!({
            "kind": "normalized_event",
            "manifest_version": manifest_version,
        }),
        revocation_source: None,
        inheritance_path: json!([
            {
                "kind": "resource_authority",
                "resource_id": resource_id,
            }
        ]),
        transfer_behavior: json!({
            "kind": "resource_rebound",
        }),
        provenance: json!({
            "normalized_event_ids": [block_number, block_number + 1],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": manifest_version,
                "source_family": "ens_v2_registry_l1",
                "chain": "ethereum-mainnet",
                "deployment_epoch": "ens_v2",
            }],
            "derivation_kind": "permissions_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["permissions_current"],
            "enumeration_basis": "resource_permissions",
            "unsupported_reason": null,
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "block_hash": format!("0xperm{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized",
            }
        }),
        manifest_version,
        last_recomputed_at: timestamp(1_717_174_000 + block_number),
    }
}

fn permission_subjects(payload: &ResourcePermissionsResponse) -> Vec<&str> {
    payload
        .data
        .iter()
        .map(|row| {
            row.get("subject")
                .and_then(Value::as_str)
                .expect("permission row must include subject")
        })
        .collect()
}

fn stable_row_strings(rows: &[Value]) -> Vec<String> {
    rows.iter()
        .map(|row| serde_json::to_string(row).expect("response rows must serialize"))
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn assert_replay_stable_pagination(
    base_rows: &[Value],
    base_page: &HistoryPageResponse,
    first_rows: &[Value],
    first_page: &HistoryPageResponse,
    second_rows: &[Value],
    second_page: &HistoryPageResponse,
    replay_rows: &[Value],
    replay_page: &HistoryPageResponse,
    expected_sort: &str,
    expected_unpaged_page_size: u64,
    expected_paged_page_size: u64,
) {
    let base_rows = stable_row_strings(base_rows);
    let first_rows = stable_row_strings(first_rows);
    let second_rows = stable_row_strings(second_rows);
    let replay_rows = stable_row_strings(replay_rows);

    assert_eq!(base_page.cursor, None);
    assert_eq!(base_page.next_cursor, None);
    assert_eq!(base_page.page_size, expected_unpaged_page_size);
    assert_eq!(base_page.sort, expected_sort);

    assert_eq!(first_page.cursor, None);
    assert_eq!(first_page.page_size, expected_paged_page_size);
    assert_eq!(first_page.sort, expected_sort);

    let applied_cursor = first_page
        .next_cursor
        .clone()
        .expect("first page must return a cursor for replay assertions");

    assert_eq!(
        first_rows,
        base_rows
            .iter()
            .take(first_rows.len())
            .cloned()
            .collect::<Vec<_>>()
    );

    assert_eq!(second_page.cursor.as_deref(), Some(applied_cursor.as_str()));
    assert_eq!(second_page.page_size, expected_paged_page_size);
    assert_eq!(second_page.sort, expected_sort);
    assert_eq!(
        second_rows,
        base_rows
            .iter()
            .skip(first_rows.len())
            .take(second_rows.len())
            .cloned()
            .collect::<Vec<_>>()
    );

    assert_eq!(replay_page.cursor.as_deref(), Some(applied_cursor.as_str()));
    assert_eq!(replay_page, second_page);
    assert_eq!(replay_rows, second_rows);
}

fn assert_children_collection_metadata_eq(base: &ChildrenResponse, candidate: &ChildrenResponse) {
    assert_eq!(candidate.declared_state, base.declared_state);
    assert_eq!(candidate.verified_state, base.verified_state);
    assert_eq!(candidate.provenance, base.provenance);
    assert_eq!(candidate.coverage, base.coverage);
    assert_eq!(candidate.chain_positions, base.chain_positions);
    assert_eq!(candidate.consistency, base.consistency);
    assert_eq!(candidate.last_updated, base.last_updated);
}

fn assert_address_names_collection_metadata_eq(
    base: &AddressNamesResponse,
    candidate: &AddressNamesResponse,
) {
    assert_eq!(candidate.declared_state, base.declared_state);
    assert_eq!(candidate.verified_state, base.verified_state);
    assert_eq!(candidate.provenance, base.provenance);
    assert_eq!(candidate.coverage, base.coverage);
    assert_eq!(candidate.chain_positions, base.chain_positions);
    assert_eq!(candidate.consistency, base.consistency);
    assert_eq!(candidate.last_updated, base.last_updated);
}

fn assert_resource_permissions_collection_metadata_eq(
    base: &ResourcePermissionsResponse,
    candidate: &ResourcePermissionsResponse,
) {
    assert_eq!(candidate.declared_state, base.declared_state);
    assert_eq!(candidate.verified_state, base.verified_state);
    assert_eq!(candidate.provenance, base.provenance);
    assert_eq!(candidate.coverage, base.coverage);
    assert_eq!(candidate.chain_positions, base.chain_positions);
    assert_eq!(candidate.consistency, base.consistency);
    assert_eq!(candidate.last_updated, base.last_updated);
}

fn resolver_current_row(chain_id: &str, resolver_address: &str) -> ResolverCurrentRow {
    ResolverCurrentRow {
        chain_id: chain_id.to_owned(),
        resolver_address: resolver_address.to_owned(),
        declared_summary: json!({
            "bindings": {
                "status": "supported",
                "count": 2,
                "items": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "normalized_name": "alice.eth",
                        "namehash": "namehash:alice.eth",
                        "resource_id": "00000000-0000-0000-0000-00000000b100",
                        "surface_binding_id": "00000000-0000-0000-0000-00000000b101",
                        "binding_kind": "declared_registry_path",
                    },
                    {
                        "logical_name_id": "ens:beta.eth",
                        "canonical_display_name": "Beta.eth",
                        "normalized_name": "beta.eth",
                        "namehash": "namehash:beta.eth",
                        "resource_id": "00000000-0000-0000-0000-00000000b102",
                        "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                        "binding_kind": "resolver_alias_path",
                    }
                ],
            },
            "aliases": {
                "status": "supported",
                "count": 1,
                "items": [{
                    "logical_name_id": "ens:beta.eth",
                    "canonical_display_name": "Beta.eth",
                    "normalized_name": "beta.eth",
                    "namehash": "namehash:beta.eth",
                    "resource_id": "00000000-0000-0000-0000-00000000b102",
                    "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                    "binding_kind": "resolver_alias_path",
                }],
            },
            "permissions": {
                "status": "supported",
                "count": 1,
                "items": [{
                    "resource_id": "00000000-0000-0000-0000-00000000b100",
                    "subject": "0x0000000000000000000000000000000000000abc",
                    "effective_powers": ["set_resolver", "set_records"],
                    "grant_source": {
                        "kind": "normalized_event",
                        "event_identity": "resolver-permission-1",
                    },
                    "revocation_source": null,
                }],
            },
            "role_holders": {
                "status": "supported",
                "count": 1,
                "items": [{
                    "subject": "0x0000000000000000000000000000000000000abc",
                    "resource_count": 1,
                    "permission_row_count": 1,
                    "effective_powers": ["set_records", "set_resolver"],
                    "resource_ids": ["00000000-0000-0000-0000-00000000b100"],
                }],
            },
            "event_summary": {
                "status": "supported",
                "count": 2,
                "by_kind": {
                    "PermissionChanged": 1,
                    "ResolverChanged": 1,
                },
            },
        }),
        provenance: json!({
            "normalized_event_ids": [101, 202],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "chain_id": chain_id,
                "block_number": 202,
            }],
            "manifest_versions": [{
                "manifest_version": 7,
                "source_family": "ens_v2_registry_l1",
                "chain": chain_id,
                "deployment_epoch": "ens_v2",
            }],
            "execution_trace_id": null,
            "derivation_kind": "resolver_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ens_v2_registry_l1", "permissions_current"],
            "unsupported_reason": null,
            "enumeration_basis": "resolver_target",
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": chain_id,
                "block_number": 202,
                "block_hash": "0xresolverc8",
                "timestamp": "2026-04-17T00:00:22Z",
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized",
            }
        }),
        manifest_version: 7,
        last_recomputed_at: timestamp(1_748_800_202),
    }
}

fn exact_name_row(
    logical_name_id: &str,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Uuid,
) -> bigname_storage::NameCurrentRow {
    bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "Alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "namehash:alice.eth".to_owned(),
        surface_binding_id: Some(surface_binding_id),
        resource_id: Some(resource_id),
        token_lineage_id: Some(token_lineage_id),
        binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "resolver": {
                "chain_id": "ethereum-mainnet",
                "address": "0x0000000000000000000000000000000000000abc",
                "latest_event_kind": "ResolverChanged"
            }
        }),
        provenance: json!({
            "normalized_event_ids": [101, 102],
            "raw_fact_refs": [
                {
                    "kind": "log",
                    "chain_id": "ethereum-mainnet",
                    "block_hash": "0xabc"
                }
            ],
            "manifest_versions": [
                {
                    "manifest_version": 3,
                    "source_family": "ens_v1_registry",
                    "chain": "ethereum-mainnet",
                    "deployment_epoch": "ens_v1"
                }
            ],
            "execution_trace_id": null,
            "derivation_kind": "projection_apply"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "unsupported_reason": null,
            "enumeration_basis": "exact_name"
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_171_717),
    }
}

fn name_current_row_with_current_resolver(
    mut row: bigname_storage::NameCurrentRow,
    chain_id: &str,
    resolver_address: &str,
) -> bigname_storage::NameCurrentRow {
    let resolver = row
        .declared_summary
        .get_mut("resolver")
        .and_then(Value::as_object_mut)
        .expect("name_current fixture must include a resolver summary");
    resolver.insert("chain_id".to_owned(), json!(chain_id));
    resolver.insert("address".to_owned(), json!(resolver_address));
    row
}

fn basenames_exact_name_control_summary() -> Value {
    json!({
        "registrant": "0x00000000000000000000000000000000000000aa",
        "registry_owner": "0x00000000000000000000000000000000000000bb",
        "latest_event_kind": "AuthorityTransferred",
    })
}

fn basenames_control_vector_control_summary(scenario: BasenamesControlVectorScenario) -> Value {
    match scenario {
        BasenamesControlVectorScenario::NftOnly => json!({
            "registrant": "0x00000000000000000000000000000000000000c1",
            "registry_owner": "0x00000000000000000000000000000000000000b1",
            "latest_event_kind": "TokenControlTransferred",
        }),
        BasenamesControlVectorScenario::ManagementOnly => json!({
            "registrant": "0x00000000000000000000000000000000000000a2",
            "registry_owner": "0x00000000000000000000000000000000000000b2",
            "latest_event_kind": "AuthorityTransferred",
        }),
        BasenamesControlVectorScenario::FullTransfer => json!({
            "registrant": "0x00000000000000000000000000000000000000c3",
            "registry_owner": "0x00000000000000000000000000000000000000c3",
            "latest_event_kind": "AuthorityTransferred",
        }),
    }
}

fn basenames_exact_name_resolver_summary() -> Value {
    json!({
        "chain_id": "base-mainnet",
        "address": "0x0000000000000000000000000000000000000abc",
        "latest_event_kind": "ResolverChanged",
    })
}

fn record_inventory_boundary_with_pointer(
    logical_name_id: &str,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
) -> Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z"
        }
    })
}

fn record_inventory_boundary(logical_name_id: &str, resource_id: Uuid) -> Value {
    record_inventory_boundary_with_pointer(logical_name_id, resource_id, None, None)
}

fn record_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    bigname_storage::RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: record_inventory_boundary(logical_name_id, resource_id),
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
        last_change: Some(json!({
            "normalized_event_id": 1200,
            "event_kind": "RecordsChanged",
            "chain_position": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xlastchange",
                "timestamp": "2026-04-17T00:00:04Z"
            }
        })),
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
            "normalized_event_ids": [1200],
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
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_171_718),
    }
}

fn dynamic_resolver_unsupported_profile_record_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    bigname_storage::RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: record_inventory_boundary(logical_name_id, resource_id),
        enumeration_basis: json!({
            "observed_selectors": false,
            "capability_declared_families": true,
            "globally_enumerable": false
        }),
        selectors: json!([]),
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
                "record_family": "addr",
                "unsupported_reason": "resolver_family_pending"
            },
            {
                "record_family": "text",
                "unsupported_reason": "resolver_family_pending"
            }
        ]),
        last_change: Some(json!({
            "normalized_event_id": 1201,
            "event_kind": "ResolverChanged",
            "chain_position": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xdynamicresolver",
                "timestamp": "2026-04-17T00:00:04Z"
            }
        })),
        entries: json!([]),
        provenance: json!({
            "normalized_event_ids": [1201],
            "derivation_kind": "record_inventory_current_rebuild"
        }),
        coverage: json!({
            "status": "partial",
            "exhaustiveness": "best_effort",
            "enumeration_basis": "declared_record_inventory",
            "unsupported_reason": "resolver_family_pending"
        }),
        chain_positions: json!({
            "ethereum-mainnet": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 7,
        last_recomputed_at: timestamp(1_717_171_719),
    }
}

fn worker_record_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    bigname_storage::RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: record_inventory_boundary_with_pointer(
            logical_name_id,
            resource_id,
            Some(1201),
            Some("RecordVersionChanged"),
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
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "cacheable": true
            }
        ]),
        explicit_gaps: json!([]),
        unsupported_families: json!([]),
        last_change: Some(json!({
            "normalized_event_id": 1202,
            "event_kind": "RecordChanged",
            "chain_position": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_004,
                "block_hash": "0xlastchange",
                "timestamp": "2026-04-17T00:00:04Z"
            }
        })),
        entries: json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "unsupported",
                "unsupported_reason": "value_not_retained_in_normalized_events"
            },
            {
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "status": "unsupported",
                "unsupported_reason": "value_not_retained_in_normalized_events"
            }
        ]),
        provenance: json!({
            "normalized_event_ids": [1201, 1202],
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
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_171_719),
    }
}

fn resolution_execution_requested_chain_positions() -> Value {
    json!([{
        "chain_id": "ethereum-mainnet",
        "block_number": 21_000_003,
        "block_hash": "0xbinding"
    }])
}

fn resolution_execution_request_key(records: &[&str]) -> String {
    let mut records = records
        .iter()
        .map(|record| (*record).to_owned())
        .collect::<Vec<_>>();
    records.sort_unstable();
    format!("ens:alice.eth:{}", records.join(","))
}

fn resolution_execution_trace(
    execution_trace_id: Uuid,
    request_key: &str,
    request_record_keys: &[&str],
    verified_queries: Value,
) -> ExecutionTrace {
    ExecutionTrace {
        execution_trace_id,
        request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
        request_key: request_key.to_owned(),
        namespace: "ens".to_owned(),
        chain_context: json!({
            "requested_positions": resolution_execution_requested_chain_positions(),
        }),
        manifest_context: json!({
            "manifest_versions": [{
                "source_family": "ens_execution",
                "manifest_version": 5
            }]
        }),
        contracts_called: json!([
            {
                "chain_id": "ethereum-mainnet",
                "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                "selector": "0x9061b923"
            }
        ]),
        gateway_digests: json!([]),
        final_payload: Some(json!({
            "verified_queries": verified_queries.clone()
        })),
        failure_payload: None,
        request_metadata: json!({
            "surface": "alice.eth",
            "record_keys": request_record_keys,
            "entrypoint": "universal_resolver",
            "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
        }),
        finished_at: Some(timestamp(1_717_171_900)),
        steps: vec![
            ExecutionTraceStep {
                step_index: 0,
                step_kind: "load_declared_topology".to_owned(),
                input_digest: Some("sha256:topology-input".to_owned()),
                output_digest: Some("sha256:topology-output".to_owned()),
                latency_ms: Some(4),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xbinding",
                        "block_number": 21_000_003,
                        "state": "finalized"
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
                        "block_hash": "0xbinding",
                        "block_number": 21_000_003,
                        "state": "finalized"
                    }
                }),
                step_payload: json!({
                    "name": "alice.eth",
                    "record_count": 2
                }),
            },
        ],
    }
}

fn resolution_execution_outcome(
    execution_trace_id: Uuid,
    request_key: &str,
    verified_queries: Value,
    logical_name_id: &str,
    resource_id: Uuid,
) -> ExecutionOutcome {
    resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        request_key,
        verified_queries,
        record_inventory_boundary(logical_name_id, resource_id),
        record_inventory_boundary(logical_name_id, resource_id),
    )
}

fn resolution_execution_outcome_with_boundaries(
    execution_trace_id: Uuid,
    request_key: &str,
    verified_queries: Value,
    topology_version_boundary: Value,
    record_version_boundary: Value,
) -> ExecutionOutcome {
    ExecutionOutcome {
        cache_key: ExecutionCacheKey {
            request_key: request_key.to_owned(),
            requested_chain_positions: resolution_execution_requested_chain_positions(),
            manifest_versions: json!([
                {
                    "manifest_version": 3,
                    "source_family": "ens_v1_registry",
                    "chain": "ethereum-mainnet",
                    "deployment_epoch": "ens_v1"
                }
            ]),
            topology_version_boundary,
            record_version_boundary,
        },
        execution_trace_id,
        request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
        namespace: "ens".to_owned(),
        outcome_payload: Some(json!({
            "verified_queries": verified_queries
        })),
        failure_payload: None,
        finished_at: timestamp(1_717_171_900),
    }
}

fn primary_name_execution_requested_chain_positions() -> Value {
    json!([{
        "chain_id": "ethereum-mainnet",
        "block_number": 21_000_010,
        "block_hash": "0xprimary"
    }])
}

fn primary_name_execution_manifest_versions_for_namespace(namespace: &str) -> Value {
    match namespace {
        "ens" => json!([{
            "manifest_version": 3,
            "source_family": "ens_execution"
        }]),
        "basenames" => json!([{
            "manifest_version": 4,
            "source_family": "basenames_execution"
        }]),
        other => panic!("unsupported primary-name test namespace {other}"),
    }
}

fn primary_name_execution_manifest_versions() -> Value {
    primary_name_execution_manifest_versions_for_namespace("ens")
}

fn primary_name_execution_request_key(namespace: &str, address: &str, coin_type: &str) -> String {
    format!("{namespace}:{}:{coin_type}", address.to_ascii_lowercase())
}

fn primary_name_execution_trace(
    execution_trace_id: Uuid,
    namespace: &str,
    address: &str,
    coin_type: &str,
    verified_primary_name: Value,
    finished_at: OffsetDateTime,
) -> ExecutionTrace {
    let normalized_address = address.to_ascii_lowercase();
    let manifest_versions = primary_name_execution_manifest_versions_for_namespace(namespace);
    let status = verified_primary_name
        .get("status")
        .and_then(Value::as_str)
        .expect("verified_primary_name payload must include string status");
    let (contracts_called, gateway_digests, steps) = match (namespace, status) {
        ("ens", "success" | "mismatch" | "execution_failed") => (
            json!([{
                "chain_id": "ethereum-mainnet",
                "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                "selector": "0x9061b923"
            }]),
            json!([]),
            vec![ExecutionTraceStep {
                step_index: 0,
                step_kind: "call_universal_resolver".to_owned(),
                input_digest: Some("sha256:primary-input".to_owned()),
                output_digest: Some("sha256:primary-output".to_owned()),
                latency_ms: Some(14),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xprimary",
                        "block_number": 21_000_010,
                        "state": "finalized"
                    }
                }),
                step_payload: json!({
                    "address": normalized_address,
                    "coin_type": coin_type
                }),
            }],
        ),
        ("basenames", "success" | "mismatch" | "execution_failed") => (
            json!([{
                "chain_id": "ethereum-mainnet",
                "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
                "selector": "0x9061b923"
            }]),
            json!(["sha256:basenames-primary-name"]),
            vec![
                ExecutionTraceStep {
                    step_index: 0,
                    step_kind: "call_l1_resolver".to_owned(),
                    input_digest: Some("sha256:primary-input".to_owned()),
                    output_digest: Some("sha256:primary-output".to_owned()),
                    latency_ms: Some(14),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xprimary",
                            "block_number": 21_000_010,
                            "state": "finalized"
                        }
                    }),
                    step_payload: json!({
                        "address": normalized_address,
                        "coin_type": coin_type
                    }),
                },
                ExecutionTraceStep {
                    step_index: 1,
                    step_kind: "complete_offchain_lookup".to_owned(),
                    input_digest: Some("sha256:gateway-input".to_owned()),
                    output_digest: Some("sha256:gateway-output".to_owned()),
                    latency_ms: Some(19),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xprimary",
                            "block_number": 21_000_010,
                            "state": "finalized"
                        }
                    }),
                    step_payload: json!({
                        "gateway": "https://basenames.example.test"
                    }),
                },
            ],
        ),
        ("ens" | "basenames", "not_found") => (
            json!([]),
            json!([]),
            vec![ExecutionTraceStep {
                step_index: 0,
                step_kind: "load_primary_name_claim".to_owned(),
                input_digest: Some("sha256:claim-input".to_owned()),
                output_digest: Some("sha256:claim-output".to_owned()),
                latency_ms: Some(2),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xprimary",
                        "block_number": 21_000_010,
                        "state": "finalized"
                    }
                }),
                step_payload: json!({
                    "address": normalized_address,
                    "coin_type": coin_type
                }),
            }],
        ),
        ("ens" | "basenames", "invalid_name") => (
            json!([]),
            json!([]),
            vec![
                ExecutionTraceStep {
                    step_index: 0,
                    step_kind: "load_primary_name_claim".to_owned(),
                    input_digest: Some("sha256:claim-input".to_owned()),
                    output_digest: Some("sha256:claim-output".to_owned()),
                    latency_ms: Some(2),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xprimary",
                            "block_number": 21_000_010,
                            "state": "finalized"
                        }
                    }),
                    step_payload: json!({
                        "address": normalized_address,
                        "coin_type": coin_type
                    }),
                },
                ExecutionTraceStep {
                    step_index: 1,
                    step_kind: "normalize_claimed_name".to_owned(),
                    input_digest: Some("sha256:normalize-input".to_owned()),
                    output_digest: Some("sha256:normalize-output".to_owned()),
                    latency_ms: Some(1),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xprimary",
                            "block_number": 21_000_010,
                            "state": "finalized"
                        }
                    }),
                    step_payload: json!({
                        "normalizer_version": "uts46-v1",
                        "error": "claim_name_not_normalizable"
                    }),
                },
            ],
        ),
        (other, _) if other != "ens" && other != "basenames" => {
            panic!("unsupported primary-name test namespace {other}")
        }
        (_, other) => panic!("unsupported primary-name test status {other}"),
    };
    ExecutionTrace {
        execution_trace_id,
        request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
        request_key: primary_name_execution_request_key(namespace, &normalized_address, coin_type),
        namespace: namespace.to_owned(),
        chain_context: json!({
            "requested_positions": primary_name_execution_requested_chain_positions(),
        }),
        manifest_context: json!({
            "manifest_versions": manifest_versions,
        }),
        contracts_called,
        gateway_digests,
        final_payload: Some(json!({
            "verified_primary_name": verified_primary_name.clone()
        })),
        failure_payload: None,
        request_metadata: json!({
            "normalized_address": normalized_address,
            "coin_type": coin_type,
            "namespace": namespace
        }),
        finished_at: Some(finished_at),
        steps,
    }
}

fn primary_name_execution_outcome(
    execution_trace_id: Uuid,
    namespace: &str,
    address: &str,
    coin_type: &str,
    verified_primary_name: Value,
    finished_at: OffsetDateTime,
) -> ExecutionOutcome {
    let normalized_address = address.to_ascii_lowercase();
    ExecutionOutcome {
        cache_key: ExecutionCacheKey {
            request_key: primary_name_execution_request_key(
                namespace,
                &normalized_address,
                coin_type,
            ),
            requested_chain_positions: primary_name_execution_requested_chain_positions(),
            manifest_versions: primary_name_execution_manifest_versions_for_namespace(namespace),
            topology_version_boundary: record_inventory_boundary(
                "ens:alice.eth",
                Uuid::from_u128(0x0e7ec7ace0000000000000000000bbb1),
            ),
            record_version_boundary: record_inventory_boundary(
                "ens:alice.eth",
                Uuid::from_u128(0x0e7ec7ace0000000000000000000bbb2),
            ),
        },
        execution_trace_id,
        request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
        namespace: namespace.to_owned(),
        outcome_payload: Some(json!({
            "verified_primary_name": verified_primary_name
        })),
        failure_payload: None,
        finished_at,
    }
}

#[allow(clippy::too_many_arguments)]
fn address_name_name_current_row(
    logical_name_id: &str,
    canonical_display_name: &str,
    normalized_name: &str,
    namehash: &str,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    block_number: i64,
    declared_summary: Value,
) -> bigname_storage::NameCurrentRow {
    bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: logical_name_id
            .split_once(':')
            .map(|(namespace, _)| namespace)
            .expect("logical_name_id must include namespace")
            .to_owned(),
        canonical_display_name: canonical_display_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: namehash.to_owned(),
        surface_binding_id: Some(surface_binding_id),
        resource_id: Some(resource_id),
        token_lineage_id,
        binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary,
        provenance: json!({
            "normalized_event_ids": [block_number, block_number + 1],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 3,
                "source_family": "ens_v1_registry",
                "chain": "ethereum-mainnet",
                "deployment_epoch": "ens_v1",
            }],
            "execution_trace_id": null,
            "derivation_kind": "projection_apply",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "unsupported_reason": null,
            "enumeration_basis": "exact_name",
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "block_hash": format!("0xname{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_175_000 + block_number),
    }
}

fn collection_name_surface(
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
) -> NameSurface {
    let namespace = logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .expect("logical_name_id must include namespace")
        .to_owned();
    let chain_id = chain_id_for_namespace(&namespace).to_owned();

    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace,
        input_name: display_name.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        dns_encoded_name: display_name.as_bytes().to_vec(),
        namehash: namehash.to_owned(),
        labelhashes: vec![format!("labelhash:{display_name}")],
        normalizer_version: "ensip15@2026-04-16".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id,
        block_hash: format!("0xsurface{block_number:02x}"),
        block_number,
        provenance: json!({"seed": "children_surface"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn declared_child_row(
    parent_logical_name_id: &str,
    child_logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    normalized_event_id: i64,
    block_number: i64,
) -> bigname_storage::ChildrenCurrentRow {
    let namespace = parent_logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .expect("parent_logical_name_id must include namespace");
    let chain_id = chain_id_for_namespace(namespace);
    let chain_slot = chain_slot_for_namespace(namespace);

    bigname_storage::ChildrenCurrentRow {
        parent_logical_name_id: parent_logical_name_id.to_owned(),
        child_logical_name_id: child_logical_name_id.to_owned(),
        surface_class: "declared".to_owned(),
        namespace: namespace.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        namehash: namehash.to_owned(),
        provenance: json!({
            "normalized_event_ids": [normalized_event_id],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 1,
                "source_family": source_family_for_namespace(namespace),
                "source_manifest_id": null,
            }],
            "execution_trace_id": null,
            "derivation_kind": "children_current_rebuild",
        }),
        chain_positions: json!({
            chain_slot: {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": format!("0xblock{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized"
            }
        }),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_717_172_000 + block_number),
    }
}

fn chain_id_for_namespace(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => "base-mainnet",
        _ => "ethereum-mainnet",
    }
}

fn chain_slot_for_namespace(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => "base",
        _ => "ethereum",
    }
}

fn source_family_for_namespace(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => "basenames_base_registry",
        _ => "ens_v1_registry_l1",
    }
}

fn address_name_token_lineage(
    token_lineage_id: Uuid,
    block_hash: &str,
    block_number: i64,
) -> TokenLineage {
    TokenLineage {
        token_lineage_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"seed": "address_name_token_lineage"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn address_name_resource(
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    block_hash: &str,
    block_number: i64,
) -> Resource {
    Resource {
        resource_id,
        token_lineage_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"seed": "address_name_resource"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn address_name_surface_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
    block_hash: &str,
    block_number: i64,
    active_from: i64,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: timestamp(active_from),
        active_to: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"seed": "address_name_binding"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

#[allow(clippy::too_many_arguments)]
fn address_name_current_row(
    address: &str,
    logical_name_id: &str,
    relation: bigname_storage::AddressNameRelation,
    display_name: &str,
    normalized_name: &str,
    namehash: &str,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    block_number: i64,
) -> bigname_storage::AddressNameCurrentRow {
    bigname_storage::AddressNameCurrentRow {
        address: address.to_owned(),
        logical_name_id: logical_name_id.to_owned(),
        relation,
        namespace: logical_name_id
            .split_once(':')
            .map(|(namespace, _)| namespace)
            .expect("logical_name_id must include namespace")
            .to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: namehash.to_owned(),
        surface_binding_id,
        resource_id,
        token_lineage_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        provenance: json!({
            "normalized_event_ids": [block_number],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 3,
                "source_family": "ens_v1_registrar_l1",
                "source_manifest_id": null,
            }],
            "execution_trace_id": null,
            "derivation_kind": "address_names_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "unsupported_reason": null,
            "enumeration_basis": "surface_current_relations",
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "block_hash": format!("0xaddr{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_173_000 + block_number),
    }
}
