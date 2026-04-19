use std::{
    fs,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use bigname_storage::{
    CanonicalityState, ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, ExecutionTraceStep,
    NameSurface, NormalizedEvent, PermissionScope, PermissionsCurrentRow, PrimaryNameClaimStatus,
    PrimaryNameCurrentRow, RawBlock, ResolverCurrentRow, Resource, SurfaceBinding,
    SurfaceBindingKind, TokenLineage, default_database_url, upsert_execution_outcome,
    upsert_execution_trace, upsert_primary_name_current_rows,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::{Uuid, time::OffsetDateTime},
};
use tower::ServiceExt;

use super::*;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new(initialize_manifest_schema: bool) -> Result<Self> {
        Self::new_with_schemas(initialize_manifest_schema, false).await
    }

    async fn new_with_schemas(
        initialize_manifest_schema: bool,
        initialize_name_current_schema: bool,
    ) -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for API tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_api_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone())
            .await
            .context("failed to connect admin pool for API tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect API test pool")?;

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
                    CREATE TABLE name_surfaces (
                        logical_name_id TEXT PRIMARY KEY,
                        namespace TEXT NOT NULL,
                        canonical_display_name TEXT NOT NULL,
                        normalized_name TEXT NOT NULL,
                        namehash TEXT NOT NULL,
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
                        resource_id UUID PRIMARY KEY
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create resources for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE token_lineages (
                        token_lineage_id UUID PRIMARY KEY
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
        }

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    async fn new_migrated() -> Result<Self> {
        let database = Self::new(false).await?;
        bigname_storage::MIGRATOR
            .run(&database.pool)
            .await
            .context("failed to apply checked-in migrations for API tests")?;
        Ok(database)
    }

    fn app_state(&self) -> AppState {
        AppState {
            phase: "test",
            pool: self.pool.clone(),
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

    async fn create_primary_names_current_table(&self) -> Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE primary_names_current (
                    address TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    coin_type TEXT NOT NULL,
                    claim_status TEXT NOT NULL,
                    raw_claim_name TEXT,
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

async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T> {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .context("failed to read API response body")?;
    serde_json::from_slice(&bytes).context("failed to decode API response JSON")
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
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
    let normalized_name = logical_name_id
        .split_once(':')
        .map(|(_, normalized_name)| normalized_name)
        .expect("logical_name_id must include namespace");

    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        input_name: normalized_name.to_owned(),
        canonical_display_name: "Alice.eth".to_owned(),
        normalized_name: normalized_name.to_owned(),
        dns_encoded_name: vec![5, b'a', b'l', b'i', b'c', b'e'],
        namehash: format!("namehash:{normalized_name}"),
        labelhashes: vec!["labelhash:alice".to_owned()],
        normalizer_version: "uts46-v1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: "ethereum-mainnet".to_owned(),
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

fn authority_history_event(
    event_identity: &str,
    namespace: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    event_kind: &str,
    block_number: i64,
    block_hash: &str,
    after_state: Value,
) -> NormalizedEvent {
    NormalizedEvent {
        namespace: namespace.to_owned(),
        event_kind: event_kind.to_owned(),
        source_family: "ens_v1_registrar_l1".to_owned(),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        after_state,
        before_state: json!({}),
        ..history_event(
            event_identity,
            Some(logical_name_id),
            Some(resource_id),
            Some("ethereum-mainnet"),
            Some(block_number),
            Some(block_hash),
            Some(&format!("0xtx{block_number}")),
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
                "block_number": 21_000_004,
                "block_hash": "0xlastchange",
                "timestamp": "2026-04-17T00:00:04Z"
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
            topology_version_boundary: record_inventory_boundary(logical_name_id, resource_id),
            record_version_boundary: record_inventory_boundary(logical_name_id, resource_id),
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

fn primary_name_execution_manifest_versions() -> Value {
    json!([{
        "manifest_version": 3,
        "source_family": "ens_v1_registry"
    }])
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
    ExecutionTrace {
        execution_trace_id,
        request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
        request_key: primary_name_execution_request_key(namespace, &normalized_address, coin_type),
        namespace: namespace.to_owned(),
        chain_context: json!({
            "requested_positions": primary_name_execution_requested_chain_positions(),
        }),
        manifest_context: json!({
            "manifest_versions": primary_name_execution_manifest_versions(),
        }),
        contracts_called: json!([{
            "chain_id": "ethereum-mainnet",
            "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
            "selector": "0x9061b923"
        }]),
        gateway_digests: json!([]),
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
        steps: vec![ExecutionTraceStep {
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
            manifest_versions: primary_name_execution_manifest_versions(),
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
        chain_id: "ethereum-mainnet".to_owned(),
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
    bigname_storage::ChildrenCurrentRow {
        parent_logical_name_id: parent_logical_name_id.to_owned(),
        child_logical_name_id: child_logical_name_id.to_owned(),
        surface_class: "declared".to_owned(),
        namespace: "ens".to_owned(),
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
                "source_family": "ens_v1_registry_l1",
                "source_manifest_id": null,
            }],
            "execution_trace_id": null,
            "derivation_kind": "children_current_rebuild",
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "block_hash": format!("0xblock{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_717_172_000 + block_number),
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

#[tokio::test]
async fn get_name_returns_current_projection_envelope() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NameResponse = read_json(response).await?;
    assert_eq!(payload.verified_state, None);
    assert_eq!(payload.consistency, "finalized");
    assert_eq!(payload.last_updated, "2024-05-31T16:08:37Z");

    let data = payload.data.as_object().expect("data must be an object");
    assert_eq!(
        data.get("logical_name_id"),
        Some(&Value::String("ens:alice.eth".to_owned()))
    );
    assert_eq!(
        data.get("namespace"),
        Some(&Value::String("ens".to_owned()))
    );
    assert_eq!(
        data.get("normalized_name"),
        Some(&Value::String("alice.eth".to_owned()))
    );
    assert_eq!(
        data.get("canonical_display_name"),
        Some(&Value::String("Alice.eth".to_owned()))
    );
    assert_eq!(
        data.get("namehash"),
        Some(&Value::String("namehash:alice.eth".to_owned()))
    );
    assert_eq!(
        data.get("resource_id"),
        Some(&Value::String(resource_id.to_string()))
    );
    assert_eq!(
        data.get("token_lineage_id"),
        Some(&Value::String(token_lineage_id.to_string()))
    );
    assert_eq!(
        data.get("binding_kind"),
        Some(&Value::String("declared_registry_path".to_owned()))
    );

    let declared_state = payload
        .declared_state
        .as_object()
        .expect("declared_state must be an object");
    assert_eq!(
        declared_state
            .get("registration")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("active")
    );
    assert_eq!(
        declared_state
            .get("resolver")
            .and_then(Value::as_object)
            .and_then(|value| value.get("address"))
            .and_then(Value::as_str),
        Some("0x0000000000000000000000000000000000000abc")
    );
    assert_eq!(
        declared_state
            .get("authority")
            .and_then(Value::as_object)
            .and_then(|value| value.get("resource_id")),
        Some(&Value::String(resource_id.to_string()))
    );
    assert_eq!(
        declared_state
            .get("control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("unsupported")
    );
    assert_eq!(
        declared_state.get("record_inventory").cloned(),
        Some(json!({
            "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
            "enumeration_basis": {
                "observed_selectors": true,
                "capability_declared_families": true,
                "globally_enumerable": false
            },
            "selectors": [
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
            ],
            "explicit_gaps": [
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "gap_reason": "not_observed_on_current_resolver"
                }
            ],
            "unsupported_families": [
                {
                    "record_family": "abi",
                    "unsupported_reason": "resolver_family_pending"
                },
                {
                    "record_family": "pubkey",
                    "unsupported_reason": "resolver_family_pending"
                }
            ],
            "last_change": {
                "normalized_event_id": 1200,
                "event_kind": "RecordsChanged",
                "chain_position": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": "0xlastchange",
                    "timestamp": "2026-04-17T00:00:04Z"
                }
            }
        }))
    );
    assert_eq!(
        declared_state
            .get("history")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("unsupported")
    );

    let provenance = payload
        .provenance
        .as_object()
        .expect("provenance must be an object");
    assert_eq!(
        provenance.get("normalized_event_ids"),
        Some(&json!(["101", "102"]))
    );
    assert_eq!(
        provenance.get("derivation_kind").and_then(Value::as_str),
        Some("projection_apply")
    );
    assert_eq!(provenance.get("execution_trace_id"), Some(&Value::Null));
    assert_eq!(
        provenance.get("manifest_versions"),
        Some(&json!([
            {
                "manifest_version": 3,
                "source_family": "ens_v1_registry",
                "chain": "ethereum-mainnet",
                "deployment_epoch": "ens_v1"
            }
        ]))
    );

    let coverage = payload
        .coverage
        .as_object()
        .expect("coverage must be an object");
    assert_eq!(coverage.get("status").and_then(Value::as_str), Some("full"));
    assert_eq!(
        coverage.get("exhaustiveness").and_then(Value::as_str),
        Some("authoritative")
    );
    assert_eq!(
        coverage.get("source_classes_considered"),
        Some(&json!(["ensv1_registry_path"]))
    );
    assert_eq!(
        coverage.get("enumeration_basis").and_then(Value::as_str),
        Some("exact_name")
    );
    assert_eq!(coverage.get("unsupported_reason"), Some(&Value::Null));

    assert_eq!(
        payload.chain_positions,
        json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_preserves_worker_record_inventory_boundary_pointer() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let worker_boundary = record_inventory_boundary_with_pointer(
        logical_name_id,
        resource_id,
        Some(1201),
        Some("RecordVersionChanged"),
    );

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(worker_record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request with worker-shaped record inventory projection failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NameResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .get("record_inventory")
            .and_then(|value| value.get("record_version_boundary")),
        Some(&worker_boundary)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_returns_unsupported_record_inventory_when_projection_row_is_missing() -> Result<()>
{
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request without record inventory projection failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NameResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .get("record_inventory")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("unsupported")
    );
    assert_eq!(
        payload
            .declared_state
            .get("record_inventory")
            .and_then(Value::as_object)
            .and_then(|value| value.get("unsupported_reason"))
            .and_then(Value::as_str),
        Some("declared record inventory summary is not yet projected")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_coverage_returns_declared_state_explain_with_shared_top_level_coverage() -> Result<()>
{
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;

    let coverage_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/coverage/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("coverage request failed")?;
    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(coverage_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let coverage_payload: NameResponse = read_json(coverage_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;

    assert_eq!(coverage_payload.data, name_payload.data);
    assert_eq!(coverage_payload.coverage, name_payload.coverage);
    assert_eq!(coverage_payload.provenance, name_payload.provenance);
    assert_eq!(
        coverage_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(coverage_payload.consistency, name_payload.consistency);
    assert_eq!(coverage_payload.last_updated, name_payload.last_updated);
    assert_eq!(coverage_payload.verified_state, None);
    assert_eq!(
        coverage_payload.declared_state,
        json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "enumeration_basis": "exact_name",
            "unsupported_reason": null
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_surface_binding_explain_reuses_exact_name_envelope_fields() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

    let mut row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    row.declared_summary = json!({
        "registration": {
            "status": "active",
            "authority_kind": "registrar"
        },
        "resolver": {
            "chain_id": "ethereum-mainnet",
            "address": "0x0000000000000000000000000000000000000abc",
            "latest_event_kind": "ResolverChanged"
        },
        "history": {
            "surface_head": null,
            "resource_head": null
        }
    });
    database.insert_name_current_row(row).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/names/ens/alice.eth/surface-binding")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("surface-binding explain request failed")?;
    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let explain_payload: NameResponse = read_json(explain_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;

    assert_eq!(explain_payload.data, name_payload.data);
    assert_eq!(explain_payload.coverage, name_payload.coverage);
    assert_eq!(explain_payload.provenance, name_payload.provenance);
    assert_eq!(
        explain_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, name_payload.consistency);
    assert_eq!(explain_payload.last_updated, name_payload.last_updated);
    assert_eq!(explain_payload.verified_state, None);
    assert_eq!(
        explain_payload.declared_state.get("history"),
        name_payload.declared_state.get("history")
    );
    assert_eq!(
        explain_payload.declared_state,
        json!({
            "surface_binding": {
                "surface_binding_id": surface_binding_id.to_string(),
                "binding_kind": "declared_registry_path"
            },
            "history": {
                "surface_head": null,
                "resource_head": null
            }
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_authority_control_explain_reuses_exact_name_envelope_fields() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let registrant = "0x0000000000000000000000000000000000000abc";
    let registry_owner = "0x0000000000000000000000000000000000000def";

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

    let mut row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    row.declared_summary = json!({
        "registration": {
            "status": "active",
            "authority_kind": "registrar"
        },
        "control": {
            "registrant": registrant,
            "registry_owner": registry_owner,
            "latest_event_kind": "NameWrapped"
        },
        "resolver": {
            "chain_id": "ethereum-mainnet",
            "address": "0x0000000000000000000000000000000000000abc",
            "latest_event_kind": "ResolverChanged"
        }
    });
    database.insert_name_current_row(row).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/names/ens/alice.eth/authority-control")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("authority-control explain request failed")?;
    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let explain_payload: NameResponse = read_json(explain_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;

    assert_eq!(explain_payload.data, name_payload.data);
    assert_eq!(explain_payload.coverage, name_payload.coverage);
    assert_eq!(explain_payload.provenance, name_payload.provenance);
    assert_eq!(
        explain_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, name_payload.consistency);
    assert_eq!(explain_payload.last_updated, name_payload.last_updated);
    assert_eq!(explain_payload.verified_state, None);
    assert_eq!(
        explain_payload.declared_state.get("authority"),
        name_payload.declared_state.get("authority")
    );
    assert_eq!(
        explain_payload.declared_state.get("control"),
        name_payload.declared_state.get("control")
    );
    assert_eq!(
        explain_payload.declared_state,
        json!({
            "authority": {
                "resource_id": resource_id.to_string(),
                "token_lineage_id": token_lineage_id.to_string(),
                "binding_kind": "declared_registry_path"
            },
            "control": {
                "registrant": registrant,
                "registry_owner": registry_owner,
                "latest_event_kind": "NameWrapped"
            }
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_returns_persisted_verified_state_and_reuses_resolution_envelope_fields()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000021);
    let request_key = resolution_execution_request_key(&["text:com.twitter", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter", "addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution execution explain request failed")?;
    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(resolution_response.status(), StatusCode::OK);

    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
    let expected_resolution_verified_state = json!({
        "verified_queries": [
            {
                "record_key": "text:com.twitter",
                "status": "success",
                "value": {
                    "value": "@alice"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]
    });

    assert_eq!(explain_payload.data, resolution_payload.data);
    assert_eq!(explain_payload.coverage, resolution_payload.coverage);
    assert_eq!(explain_payload.provenance, resolution_payload.provenance);
    assert_eq!(
        explain_payload.chain_positions,
        resolution_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, resolution_payload.consistency);
    assert_eq!(
        explain_payload.last_updated,
        resolution_payload.last_updated
    );
    assert_eq!(explain_payload.declared_state, None);
    assert_eq!(
        resolution_payload.verified_state,
        Some(expected_resolution_verified_state)
    );
    assert_eq!(
        explain_payload.verified_state,
        Some(json!({
            "execution": {
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "ens_execution",
                    "role": "universal_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
                },
                "resolver_discovery_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged"
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": []
                },
                "alias": {
                    "final_target": null,
                    "hops": []
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_universal_resolver",
                        "input_digest": "sha256:resolver-input",
                        "output_digest": "sha256:resolver-output",
                        "latency": 28,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900))
            },
            "verified_queries": [
                {
                    "record_key": "text:com.twitter",
                    "status": "success",
                    "value": {
                        "value": "@alice"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "addr:60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x00000000000000000000000000000000000000aa"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_reads_persisted_alias_only_avatar_answers_for_ens_alias_binding()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000025);
    let request_key = resolution_execution_request_key(&["text:com.twitter"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice-via-alias.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice-via-alias"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);
    let alias_target = json!({
        "logical_name_id": "ens:profile.alice.eth",
        "namespace": "ens",
        "normalized_name": "profile.alice.eth",
        "canonical_display_name": "Profile.alice.eth",
        "namehash": "namehash:profile.alice.eth",
        "resource_id": resource_id.to_string(),
        "binding_kind": "resolver_alias_path"
    });

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

    let mut row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    row.binding_kind = Some(bigname_storage::SurfaceBindingKind::ResolverAliasPath);
    database.insert_name_current_row(row).await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let mut trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar", "text:com.twitter"],
        persisted_verified_queries.clone(),
    );
    trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_keys": ["avatar", "text:com.twitter"],
        "entrypoint": "universal_resolver",
        "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
        "alias": {
            "final_target": alias_target.clone(),
            "hops": [alias_target.clone()]
        }
    });
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=avatar,text:com.twitter")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution execution explain alias request failed")?;
    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=avatar,text:com.twitter")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution alias request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(resolution_response.status(), StatusCode::OK);

    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
    let expected_resolution_verified_state = json!({
        "verified_queries": [
            {
                "record_key": "avatar",
                "status": "success",
                "value": {
                    "value": "https://cdn.example.test/alice-via-alias.png"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "text:com.twitter",
                "status": "success",
                "value": {
                    "value": "@alice-via-alias"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]
    });

    assert_eq!(explain_payload.data, resolution_payload.data);
    assert_eq!(explain_payload.coverage, resolution_payload.coverage);
    assert_eq!(explain_payload.provenance, resolution_payload.provenance);
    assert_eq!(
        explain_payload.chain_positions,
        resolution_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, resolution_payload.consistency);
    assert_eq!(
        explain_payload.last_updated,
        resolution_payload.last_updated
    );
    assert_eq!(explain_payload.declared_state, None);
    assert_eq!(
        resolution_payload.verified_state,
        Some(expected_resolution_verified_state)
    );
    assert_eq!(
        explain_payload.verified_state,
        Some(json!({
            "execution": {
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "ens_execution",
                    "role": "universal_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
                },
                "resolver_discovery_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged"
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": []
                },
                "alias": {
                    "final_target": alias_target.clone(),
                    "hops": [alias_target.clone()]
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_universal_resolver",
                        "input_digest": "sha256:resolver-input",
                        "output_digest": "sha256:resolver-output",
                        "latency": 28,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900))
            },
            "verified_queries": [
                {
                    "record_key": "avatar",
                    "status": "success",
                    "value": {
                        "value": "https://cdn.example.test/alice-via-alias.png"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "text:com.twitter",
                    "status": "success",
                    "value": {
                        "value": "@alice-via-alias"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_verified_state_uses_supported_persisted_answers_and_preserves_request_order()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000022);
    let request_key = resolution_execution_request_key(&["text:com.twitter", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter", "addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=avatar,text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified resolution request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both&records=avatar,text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed resolution request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: ResolutionResponse = read_json(verified_response).await?;
    let both_payload: ResolutionResponse = read_json(both_response).await?;
    let expected_verified_state = json!({
        "verified_queries": [
            {
                "record_key": "avatar",
                "status": "unsupported",
                "unsupported_reason": "verified resolution entrypoint is not yet supported"
            },
            {
                "record_key": "text:com.twitter",
                "status": "success",
                "value": {
                    "value": "@alice"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]
    });

    assert_eq!(
        verified_payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert_eq!(
        verified_payload.verified_state,
        Some(expected_verified_state.clone())
    );
    assert_eq!(
        both_payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert!(both_payload.declared_state.is_some());
    assert_eq!(both_payload.verified_state, Some(expected_verified_state));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_both_mode_reads_persisted_alias_only_avatar_answers_for_ens_alias_binding()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000026);
    let request_key = resolution_execution_request_key(&["text:com.twitter"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice-via-alias.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice-via-alias"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

    let mut row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    row.binding_kind = Some(bigname_storage::SurfaceBindingKind::ResolverAliasPath);
    database.insert_name_current_row(row).await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar", "text:com.twitter"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both&records=avatar,text:com.twitter")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed resolution alias request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");

    assert_eq!(
        payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert_eq!(
        declared_state.get("topology"),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared resolution topology is not yet projected",
        }))
    );
    assert!(
        declared_state
            .get("record_inventory")
            .and_then(|value| value.get("record_version_boundary"))
            .is_some(),
        "record inventory should still load through the persisted readback lane"
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_queries": [
                {
                    "record_key": "avatar",
                    "status": "success",
                    "value": {
                        "value": "https://cdn.example.test/alice-via-alias.png"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "text:com.twitter",
                    "status": "success",
                    "value": {
                        "value": "@alice-via-alias"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_verified_state_surfaces_persisted_avatar_answers_and_preserves_request_order()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000023);
    let contenthash = "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u";
    let request_key =
        resolution_execution_request_key(&["text:com.twitter", "contenthash", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "not_found",
            "failure_reason": "no_text_record"
        },
        {
            "record_key": "contenthash",
            "status": "success",
            "value": {
                "value": contenthash
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar", "text:com.twitter", "contenthash", "addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=avatar,text:com.twitter,contenthash,addr:60")
                .body(Body::empty())
                .expect("verified request must build"),
        )
        .await
        .context("verified resolution request with contenthash failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both&records=avatar,text:com.twitter,contenthash,addr:60")
                .body(Body::empty())
                .expect("mixed request must build"),
        )
        .await
        .context("mixed resolution request with contenthash failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: ResolutionResponse = read_json(verified_response).await?;
    let both_payload: ResolutionResponse = read_json(both_response).await?;
    let expected_verified_state = json!({
        "verified_queries": [
            {
                "record_key": "avatar",
                "status": "success",
                "value": {
                    "value": "https://cdn.example.test/alice.png"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "text:com.twitter",
                "status": "not_found",
                "failure_reason": "no_text_record"
            },
            {
                "record_key": "contenthash",
                "status": "success",
                "value": {
                    "value": contenthash
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]
    });

    assert_eq!(
        verified_payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert_eq!(
        verified_payload.verified_state,
        Some(expected_verified_state.clone())
    );
    assert_eq!(
        both_payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert!(both_payload.declared_state.is_some());
    assert_eq!(both_payload.verified_state, Some(expected_verified_state));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_returns_not_found_when_persisted_answer_is_missing()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution execution explain request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "persisted resolution execution explain was not found for name alice.eth in namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_surfaces_persisted_avatar_answers_and_reuses_resolution_envelope_fields()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000024);
    let contenthash = "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u";
    let request_key =
        resolution_execution_request_key(&["text:com.twitter", "contenthash", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "not_found",
            "failure_reason": "no_text_record"
        },
        {
            "record_key": "contenthash",
            "status": "success",
            "value": {
                "value": contenthash
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar", "text:com.twitter", "contenthash", "addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=avatar,text:com.twitter,contenthash,addr:60")
                .body(Body::empty())
                .expect("explain request must build"),
        )
        .await
        .context("resolution execution explain request with contenthash failed")?;
    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=avatar,text:com.twitter,contenthash,addr:60")
                .body(Body::empty())
                .expect("resolution request must build"),
        )
        .await
        .context("resolution request with contenthash failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(resolution_response.status(), StatusCode::OK);

    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
    let expected_resolution_verified_state = json!({
        "verified_queries": [
            {
                "record_key": "avatar",
                "status": "success",
                "value": {
                    "value": "https://cdn.example.test/alice.png"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "text:com.twitter",
                "status": "not_found",
                "failure_reason": "no_text_record"
            },
            {
                "record_key": "contenthash",
                "status": "success",
                "value": {
                    "value": contenthash
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]
    });

    assert_eq!(explain_payload.data, resolution_payload.data);
    assert_eq!(explain_payload.coverage, resolution_payload.coverage);
    assert_eq!(explain_payload.provenance, resolution_payload.provenance);
    assert_eq!(
        explain_payload.chain_positions,
        resolution_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, resolution_payload.consistency);
    assert_eq!(
        explain_payload.last_updated,
        resolution_payload.last_updated
    );
    assert_eq!(explain_payload.declared_state, None);
    assert_eq!(
        resolution_payload.verified_state,
        Some(expected_resolution_verified_state)
    );
    assert_eq!(
        explain_payload.verified_state,
        Some(json!({
            "execution": {
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "ens_execution",
                    "role": "universal_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
                },
                "resolver_discovery_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged"
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": []
                },
                "alias": {
                    "final_target": null,
                    "hops": []
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_universal_resolver",
                        "input_digest": "sha256:resolver-input",
                        "output_digest": "sha256:resolver-output",
                        "latency": 28,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900))
            },
            "verified_queries": [
                {
                    "record_key": "avatar",
                    "status": "success",
                    "value": {
                        "value": "https://cdn.example.test/alice.png"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "text:com.twitter",
                    "status": "not_found",
                    "failure_reason": "no_text_record"
                },
                {
                    "record_key": "contenthash",
                    "status": "success",
                    "value": {
                        "value": contenthash
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "addr:60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x00000000000000000000000000000000000000aa"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_rejects_duplicate_records() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=text,text")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("duplicate resolution execution explain request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "records must not contain duplicate selectors"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_rejects_malformed_records() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=:avatar")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed resolution execution explain request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "records must contain only valid record selectors"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_mode_parsing_populates_expected_sections() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;

    let default_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("default resolution request failed")?;
    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=declared&records=text")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared resolution request failed")?;
    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=text,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified resolution request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both&records=text")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed resolution request failed")?;

    assert_eq!(default_response.status(), StatusCode::OK);
    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let default_payload: ResolutionResponse = read_json(default_response).await?;
    let declared_payload: ResolutionResponse = read_json(declared_response).await?;
    let verified_payload: ResolutionResponse = read_json(verified_response).await?;
    let both_payload: ResolutionResponse = read_json(both_response).await?;

    assert!(default_payload.declared_state.is_some());
    assert_eq!(default_payload.verified_state, None);
    assert!(declared_payload.declared_state.is_some());
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_queries": [
                {
                    "record_key": "text",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported",
                },
                {
                    "record_key": "addr:60",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported",
                }
            ]
        }))
    );
    assert!(both_payload.declared_state.is_some());
    assert_eq!(
        both_payload.verified_state,
        Some(json!({
            "verified_queries": [
                {
                    "record_key": "text",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported",
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_requires_records_for_verified_modes() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified resolution request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed resolution request failed")?;

    assert_eq!(verified_response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(both_response.status(), StatusCode::BAD_REQUEST);

    let verified_payload: ErrorResponse = read_json(verified_response).await?;
    let both_payload: ErrorResponse = read_json(both_response).await?;
    assert_eq!(verified_payload.error.code, "invalid_input");
    assert_eq!(both_payload.error.code, "invalid_input");
    assert_eq!(
        verified_payload.error.message,
        "records is required when mode is verified or both"
    );
    assert_eq!(both_payload.error.message, verified_payload.error.message);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_rejects_duplicate_records_for_verified_modes() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=text,text")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("duplicate resolution request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "records must not contain duplicate selectors"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_rejects_malformed_records() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=declared&records=:avatar")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed resolution request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "records must contain only valid record selectors"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_returns_supported_topology_for_direct_ens_binding() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(payload.verified_state, None);
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "topology": {
                "registry_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "namehash": "namehash:alice.eth",
                        "resource_id": resource_id.to_string(),
                        "binding_kind": "declared_registry_path",
                    }
                ],
                "subregistry_path": [],
                "resolver_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged",
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": [],
                },
                "alias": {
                    "final_target": null,
                    "hops": [],
                },
                "version_boundaries": {
                    "topology_version_boundary": {
                        "logical_name_id": "ens:alice.eth",
                        "resource_id": resource_id.to_string(),
                        "normalized_event_id": null,
                        "event_kind": null,
                        "chain_position": {
                            "chain_id": "ethereum-mainnet",
                            "block_number": 21_000_003,
                            "block_hash": "0xbinding",
                            "timestamp": "2026-04-17T00:00:03Z",
                        },
                    },
                    "record_version_boundary": {
                        "logical_name_id": "ens:alice.eth",
                        "resource_id": resource_id.to_string(),
                        "normalized_event_id": null,
                        "event_kind": null,
                        "chain_position": {
                            "chain_id": "ethereum-mainnet",
                            "block_number": 21_000_003,
                            "block_hash": "0xbinding",
                            "timestamp": "2026-04-17T00:00:03Z",
                        },
                    },
                },
                "transport": {
                    "source_chain_id": null,
                    "target_chain_id": null,
                    "contract_address": null,
                    "latest_event_kind": null,
                },
            },
            "record_inventory": {
                "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
                "enumeration_basis": {
                    "observed_selectors": true,
                    "capability_declared_families": true,
                    "globally_enumerable": false,
                },
                "selectors": [
                    {
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                        "cacheable": true,
                    },
                    {
                        "record_key": "avatar",
                        "record_family": "avatar",
                        "selector_key": null,
                        "cacheable": true,
                    },
                    {
                        "record_key": "text:com.twitter",
                        "record_family": "text",
                        "selector_key": "com.twitter",
                        "cacheable": false,
                    }
                ],
                "explicit_gaps": [
                    {
                        "record_key": "contenthash",
                        "record_family": "contenthash",
                        "selector_key": null,
                        "gap_reason": "not_observed_on_current_resolver",
                    }
                ],
                "unsupported_families": [
                    {
                        "record_family": "abi",
                        "unsupported_reason": "resolver_family_pending",
                    },
                    {
                        "record_family": "pubkey",
                        "unsupported_reason": "resolver_family_pending",
                    }
                ],
                "last_change": {
                    "normalized_event_id": 1200,
                    "event_kind": "RecordsChanged",
                    "chain_position": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 21_000_003,
                        "block_hash": "0xlastchange",
                        "timestamp": "2026-04-17T00:00:04Z",
                    }
                }
            },
            "record_cache": {
                "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
                "entries": [
                    {
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                        "status": "success",
                        "value": {
                            "coin_type": "60",
                            "value": "0x0000000000000000000000000000000000000abc",
                        }
                    },
                    {
                        "record_key": "avatar",
                        "record_family": "avatar",
                        "selector_key": null,
                        "status": "unsupported",
                        "unsupported_reason": "resolver_family_pending",
                    }
                ]
            }
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_preserves_worker_record_inventory_boundary_pointer() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let worker_boundary = record_inventory_boundary_with_pointer(
        logical_name_id,
        resource_id,
        Some(1201),
        Some("RecordVersionChanged"),
    );

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(worker_record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request with worker-shaped record inventory projection failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    let topology = declared_state
        .get("topology")
        .and_then(Value::as_object)
        .expect("topology must be supported");
    let version_boundaries = topology
        .get("version_boundaries")
        .and_then(Value::as_object)
        .expect("version_boundaries must be present");

    assert_eq!(
        version_boundaries.get("topology_version_boundary"),
        Some(&worker_boundary)
    );
    assert_eq!(
        version_boundaries.get("record_version_boundary"),
        Some(&worker_boundary)
    );
    assert_eq!(
        declared_state
            .get("record_inventory")
            .and_then(|value| value.get("record_version_boundary")),
        Some(&worker_boundary)
    );
    assert_eq!(
        declared_state
            .get("record_cache")
            .and_then(|value| value.get("record_version_boundary")),
        Some(&worker_boundary)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_returns_unsupported_record_inventory_sections_when_projection_row_is_missing()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request without record inventory projection failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(
        declared_state.get("record_inventory"),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared resolution record inventory is not yet projected",
        }))
    );
    assert_eq!(
        declared_state.get("record_cache"),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared resolution record cache is not yet projected",
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_declared_records_narrow_record_cache_in_request_order() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=declared&records=text:com.twitter,addr:60,avatar")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared resolution request with narrowed records failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("record_cache")),
        Some(&json!({
            "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
            "entries": [
                {
                    "record_key": "text:com.twitter",
                    "record_family": "text",
                    "selector_key": "com.twitter",
                    "status": "not_found",
                },
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x0000000000000000000000000000000000000abc",
                    }
                },
                {
                    "record_key": "avatar",
                    "record_family": "avatar",
                    "selector_key": null,
                    "status": "unsupported",
                    "unsupported_reason": "resolver_family_pending",
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_declared_records_return_not_found_cache_entry_for_explicit_gap()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=declared&records=contenthash")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared resolution request with explicit-gap selector failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(
        declared_state
            .get("record_inventory")
            .and_then(|value| value.get("explicit_gaps")),
        Some(&json!([
            {
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "gap_reason": "not_observed_on_current_resolver",
            }
        ]))
    );
    assert_eq!(
        declared_state.get("record_cache"),
        Some(&json!({
            "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
            "entries": [
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "status": "not_found",
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_declared_records_synthesize_unsupported_family_entries() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let worker_boundary = record_inventory_boundary_with_pointer(
        logical_name_id,
        resource_id,
        Some(1201),
        Some("RecordVersionChanged"),
    );

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(worker_record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=declared&records=abi:json,addr:60,pubkey,text")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared resolution request with unsupported-family selectors failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("record_inventory"))
            .and_then(|inventory| inventory.get("unsupported_families")),
        Some(&json!([]))
    );
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("record_cache")),
        Some(&json!({
            "record_version_boundary": worker_boundary,
            "entries": [
                {
                    "record_key": "abi:json",
                    "record_family": "abi",
                    "selector_key": "json",
                    "status": "unsupported",
                    "unsupported_reason": "record_family_not_supported_in_phase6_projection",
                },
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "status": "unsupported",
                    "unsupported_reason": "value_not_retained_in_normalized_events",
                },
                {
                    "record_key": "pubkey",
                    "record_family": "pubkey",
                    "selector_key": null,
                    "status": "unsupported",
                    "unsupported_reason": "record_family_not_supported_in_phase6_projection",
                },
                {
                    "record_key": "text",
                    "record_family": "text",
                    "selector_key": null,
                    "status": "unsupported",
                    "unsupported_reason": "value_not_retained_in_normalized_events",
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_returns_unsupported_topology_for_non_direct_bindings() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

    let mut row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    row.binding_kind = Some(bigname_storage::SurfaceBindingKind::ResolverAliasPath);
    database.insert_name_current_row(row).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(payload.verified_state, None);
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("topology")),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared resolution topology is not yet projected",
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_supported_topology_uses_terminal_null_hop_when_no_resolver_is_declared()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

    let mut row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    row.declared_summary = json!({
        "registration": {
            "status": "active",
            "authority_kind": "registrar"
        },
        "resolver": {
            "chain_id": null,
            "address": null,
            "latest_event_kind": null
        }
    });
    database.insert_name_current_row(row).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let topology = payload
        .declared_state
        .as_ref()
        .and_then(|state| state.get("topology"))
        .and_then(Value::as_object)
        .expect("topology must be supported");
    let resolver_path = topology
        .get("resolver_path")
        .and_then(Value::as_array)
        .expect("resolver_path must be an array");
    assert_eq!(resolver_path.len(), 1);
    assert_eq!(
        resolver_path.first(),
        Some(&json!({
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "resource_id": resource_id.to_string(),
            "chain_id": null,
            "address": null,
            "latest_event_kind": null,
        }))
    );
    assert_eq!(
        topology
            .get("version_boundaries")
            .and_then(Value::as_object)
            .and_then(|value| value.get("topology_version_boundary")),
        topology
            .get("version_boundaries")
            .and_then(Value::as_object)
            .and_then(|value| value.get("record_version_boundary"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_reuses_exact_name_envelope_fields() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;

    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both&records=text,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;
    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(resolution_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;

    assert_eq!(resolution_payload.data, name_payload.data);
    assert_eq!(resolution_payload.provenance, name_payload.provenance);
    assert_eq!(resolution_payload.coverage, name_payload.coverage);
    assert_eq!(
        resolution_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(resolution_payload.consistency, name_payload.consistency);
    assert_eq!(resolution_payload.last_updated, name_payload.last_updated);
    assert_eq!(
        resolution_payload.verified_state,
        Some(json!({
            "verified_queries": [
                {
                    "record_key": "text",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported",
                },
                {
                    "record_key": "addr:60",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported",
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolver_overview_returns_declared_state_with_shared_projection_envelope() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let chain_id = "ethereum-mainnet";
    let resolver_address = "0x0000000000000000000000000000000000000aaa";

    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[resolver_current_row(chain_id, resolver_address)],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolvers/ethereum-mainnet/0x0000000000000000000000000000000000000AAA")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolver overview request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolverResponse = read_json(response).await?;
    assert_eq!(
        payload.data,
        json!({
            "chain_id": chain_id,
            "resolver_address": resolver_address,
        })
    );
    assert_eq!(
        payload.declared_state,
        json!({
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
        })
    );
    assert_eq!(payload.verified_state, None);
    assert_eq!(
        payload.provenance,
        json!({
            "normalized_event_ids": ["101", "202"],
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
        })
    );
    assert_eq!(
        payload.coverage,
        json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ens_v2_registry_l1", "permissions_current"],
            "enumeration_basis": "resolver_target",
            "unsupported_reason": null,
        })
    );
    assert_eq!(
        payload.chain_positions,
        json!({
            "ethereum": {
                "chain_id": chain_id,
                "block_number": 202,
                "block_hash": "0xresolverc8",
                "timestamp": "2026-04-17T00:00:22Z",
            }
        })
    );
    assert_eq!(payload.consistency, "finalized");
    assert_eq!(payload.last_updated, "2025-06-01T17:50:02Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolver_overview_returns_not_found_when_projection_is_missing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolvers/ethereum-mainnet/0x0000000000000000000000000000000000000aaa")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("missing resolver overview request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "resolver 0x0000000000000000000000000000000000000aaa was not found on chain ethereum-mainnet"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_returns_declared_rows_sorted_with_declared_only_coverage() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "ens:parent.eth";

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(parent_logical_name_id, "parent.eth", "node:parent.eth", 10),
            collection_name_surface(
                "ens:bob.parent.eth",
                "bob.parent.eth",
                "node:bob.parent.eth",
                11,
            ),
            collection_name_surface(
                "ens:alice.parent.eth",
                "alice.parent.eth",
                "node:alice.parent.eth",
                12,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            declared_child_row(
                parent_logical_name_id,
                "ens:bob.parent.eth",
                "bob.parent.eth",
                "node:bob.parent.eth",
                201,
                11,
            ),
            declared_child_row(
                parent_logical_name_id,
                "ens:alice.parent.eth",
                "alice.parent.eth",
                "node:alice.parent.eth",
                202,
                12,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("children request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ChildrenResponse = read_json(response).await?;
    assert!(
        payload
            .declared_state
            .as_object()
            .map(|value| value.is_empty())
            .unwrap_or(false)
    );
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["declared".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "declared_direct_children"
    );
    assert_eq!(payload.coverage.unsupported_reason, None);
    assert_eq!(payload.page.sort, "display_name_asc");
    assert_eq!(payload.page.page_size, 2);
    assert_eq!(payload.consistency, "finalized");
    assert_eq!(
        payload.last_updated,
        format_timestamp(timestamp(1_717_172_012))
    );
    assert_eq!(
        payload.provenance,
        json!({
            "normalized_event_ids": ["202", "201"],
            "raw_fact_refs": [
                {"kind": "raw_log", "block_number": 12},
                {"kind": "raw_log", "block_number": 11}
            ],
            "manifest_versions": [{
                "manifest_version": 1,
                "source_family": "ens_v1_registry_l1",
                "source_manifest_id": null
            }],
            "execution_trace_id": null,
            "derivation_kind": "children_current_rebuild"
        })
    );
    assert_eq!(
        payload.chain_positions,
        json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 12,
                "block_hash": "0xblock0c",
                "timestamp": "2026-04-17T00:00:12Z"
            }
        })
    );

    let child_ids = payload
        .data
        .iter()
        .map(|row| {
            row.get("logical_name_id")
                .and_then(Value::as_str)
                .expect("child row must include logical_name_id")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        child_ids,
        vec!["ens:alice.parent.eth", "ens:bob.parent.eth"]
    );
    assert_eq!(
        payload.data[0].get("surface_class").and_then(Value::as_str),
        Some("declared")
    );

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children?page_size=1")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("children first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: ChildrenResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("children first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names/ens/parent.eth/children?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("children second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: ChildrenResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names/ens/parent.eth/children?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("children replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: ChildrenResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &payload.data,
        &payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "display_name_asc",
        2,
        1,
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_include_counts_returns_declared_subname_count() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "ens:parent.eth";

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(parent_logical_name_id, "parent.eth", "node:parent.eth", 20),
            collection_name_surface(
                "ens:alice.parent.eth",
                "alice.parent.eth",
                "node:alice.parent.eth",
                21,
            ),
            collection_name_surface(
                "ens:bob.parent.eth",
                "bob.parent.eth",
                "node:bob.parent.eth",
                22,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            declared_child_row(
                parent_logical_name_id,
                "ens:alice.parent.eth",
                "alice.parent.eth",
                "node:alice.parent.eth",
                301,
                21,
            ),
            declared_child_row(
                parent_logical_name_id,
                "ens:bob.parent.eth",
                "bob.parent.eth",
                "node:bob.parent.eth",
                302,
                22,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children?include=counts")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("children counts request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ChildrenResponse = read_json(response).await?;
    assert_eq!(payload.declared_state.get("subname_count"), Some(&json!(2)));
    assert_eq!(payload.data.len(), 2);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_rejects_non_declared_surface_classes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[collection_name_surface(
            "ens:parent.eth",
            "parent.eth",
            "node:parent.eth",
            30,
        )],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children?surface_classes=declared,linked")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("children unsupported surface_classes request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "unsupported");
    assert_eq!(
        payload.error.message,
        "surface_classes other than declared are not yet supported"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_returns_not_found_when_projection_row_is_missing() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/missing.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "name missing.eth was not found in namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_coverage_returns_not_found_when_projection_row_is_missing() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/coverage/ens/missing.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("coverage request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "name missing.eth was not found in namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_surface_binding_explain_returns_not_found_when_projection_row_is_missing() -> Result<()>
{
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/names/ens/missing.eth/surface-binding")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("surface-binding explain request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "name missing.eth was not found in namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_authority_control_explain_returns_not_found_when_projection_row_is_missing()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/names/ens/missing.eth/authority-control")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("authority-control explain request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "name missing.eth was not found in namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_returns_surface_first_rows_sorted_with_stable_relation_facets()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bbb";
    let alpha_resource_id = Uuid::from_u128(0x8100);
    let alpha_token_lineage_id = Uuid::from_u128(0x8101);
    let alpha_surface_binding_id = Uuid::from_u128(0x8102);
    let beta_resource_id = Uuid::from_u128(0x8200);
    let beta_token_lineage_id = Uuid::from_u128(0x8201);
    let beta_surface_binding_id = Uuid::from_u128(0x8202);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xalpha", None, 11, 1_717_173_011),
            raw_block("ethereum-mainnet", "0xbeta", None, 12, 1_717_173_012),
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[
            address_name_token_lineage(alpha_token_lineage_id, "0xalpha", 11),
            address_name_token_lineage(beta_token_lineage_id, "0xbeta", 12),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(
                alpha_resource_id,
                Some(alpha_token_lineage_id),
                "0xalpha",
                11,
            ),
            address_name_resource(beta_resource_id, Some(beta_token_lineage_id), "0xbeta", 12),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:beta.eth", "beta.eth", "node:beta.eth", 12),
            collection_name_surface("ens:alpha.eth", "alpha.eth", "node:alpha.eth", 11),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(
                beta_surface_binding_id,
                "ens:beta.eth",
                beta_resource_id,
                "0xbeta",
                12,
                1_717_173_012,
            ),
            address_name_surface_binding(
                alpha_surface_binding_id,
                "ens:alpha.eth",
                alpha_resource_id,
                "0xalpha",
                11,
                1_717_173_011,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                address,
                "ens:beta.eth",
                bigname_storage::AddressNameRelation::EffectiveController,
                "beta.eth",
                "beta.eth",
                "node:beta.eth",
                beta_surface_binding_id,
                beta_resource_id,
                Some(beta_token_lineage_id),
                12,
            ),
            address_name_current_row(
                address,
                "ens:alpha.eth",
                bigname_storage::AddressNameRelation::TokenHolder,
                "alpha.eth",
                "alpha.eth",
                "node:alpha.eth",
                alpha_surface_binding_id,
                alpha_resource_id,
                Some(alpha_token_lineage_id),
                11,
            ),
            address_name_current_row(
                address,
                "ens:alpha.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "alpha.eth",
                "alpha.eth",
                "node:alpha.eth",
                alpha_surface_binding_id,
                alpha_resource_id,
                Some(alpha_token_lineage_id),
                11,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/addresses/{address}/names"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address names request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: AddressNamesResponse = read_json(response).await?;
    assert!(
        payload
            .declared_state
            .as_object()
            .map(|value| value.is_empty())
            .unwrap_or(false)
    );
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["ensv1_registry_path".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "surface_current_relations"
    );
    assert_eq!(payload.page.sort, "display_name_asc");
    assert_eq!(payload.page.page_size, 2);
    assert_eq!(payload.consistency, "finalized");

    let logical_name_ids = payload
        .data
        .iter()
        .map(|row| {
            row.get("logical_name_id")
                .and_then(Value::as_str)
                .expect("address-name row must include logical_name_id")
        })
        .collect::<Vec<_>>();
    assert_eq!(logical_name_ids, vec!["ens:alpha.eth", "ens:beta.eth"]);
    assert_eq!(
        payload.data[0].get("relation_facets"),
        Some(&json!(["registrant", "token_holder"]))
    );
    assert_eq!(
        payload.data[1].get("relation_facets"),
        Some(&json!(["effective_controller"]))
    );

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/addresses/{address}/names?page_size=1"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address names first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: AddressNamesResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("address names first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{address}/names?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address names second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: AddressNamesResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{address}/names?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address names replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: AddressNamesResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &payload.data,
        &payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "display_name_asc",
        2,
        1,
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_honors_namespace_and_relation_filters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let ens_resource_id = Uuid::from_u128(0x8300);
    let ens_token_lineage_id = Uuid::from_u128(0x8301);
    let ens_surface_binding_id = Uuid::from_u128(0x8302);
    let base_resource_id = Uuid::from_u128(0x8400);
    let base_surface_binding_id = Uuid::from_u128(0x8402);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xens", None, 21, 1_717_173_021),
            raw_block("ethereum-mainnet", "0xbase", None, 22, 1_717_173_022),
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            ens_token_lineage_id,
            "0xens",
            21,
        )],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(ens_resource_id, Some(ens_token_lineage_id), "0xens", 21),
            address_name_resource(base_resource_id, None, "0xbase", 22),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:alice.eth", "alice.eth", "node:alice.eth", 21),
            collection_name_surface(
                "basenames:alice.base.eth",
                "alice.base.eth",
                "node:alice.base.eth",
                22,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(
                ens_surface_binding_id,
                "ens:alice.eth",
                ens_resource_id,
                "0xens",
                21,
                1_717_173_021,
            ),
            address_name_surface_binding(
                base_surface_binding_id,
                "basenames:alice.base.eth",
                base_resource_id,
                "0xbase",
                22,
                1_717_173_022,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                address,
                "ens:alice.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "alice.eth",
                "alice.eth",
                "node:alice.eth",
                ens_surface_binding_id,
                ens_resource_id,
                Some(ens_token_lineage_id),
                21,
            ),
            address_name_current_row(
                address,
                "basenames:alice.base.eth",
                bigname_storage::AddressNameRelation::EffectiveController,
                "alice.base.eth",
                "alice.base.eth",
                "node:alice.base.eth",
                base_surface_binding_id,
                base_resource_id,
                None,
                22,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{address}/names?namespace=ens&relation=registrant"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("filtered address names request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: AddressNamesResponse = read_json(response).await?;
    assert_eq!(payload.data.len(), 1);
    assert_eq!(
        payload.data[0].get("logical_name_id"),
        Some(&Value::String("ens:alice.eth".to_owned()))
    );
    assert_eq!(
        payload.data[0].get("relation_facets"),
        Some(&json!(["registrant"]))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_dedupe_by_resource_changes_grouping_only() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000def";
    let shared_resource_id = Uuid::from_u128(0x8500);
    let shared_token_lineage_id = Uuid::from_u128(0x8501);
    let alpha_surface_binding_id = Uuid::from_u128(0x8502);
    let beta_surface_binding_id = Uuid::from_u128(0x8503);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[raw_block(
            "ethereum-mainnet",
            "0xshared",
            None,
            31,
            1_717_173_031,
        )],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            shared_token_lineage_id,
            "0xshared",
            31,
        )],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[address_name_resource(
            shared_resource_id,
            Some(shared_token_lineage_id),
            "0xshared",
            31,
        )],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:beta.eth", "beta.eth", "node:beta.eth", 31),
            collection_name_surface("ens:alpha.eth", "alpha.eth", "node:alpha.eth", 31),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(
                beta_surface_binding_id,
                "ens:beta.eth",
                shared_resource_id,
                "0xshared",
                31,
                1_717_173_031,
            ),
            address_name_surface_binding(
                alpha_surface_binding_id,
                "ens:alpha.eth",
                shared_resource_id,
                "0xshared",
                31,
                1_717_173_031,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                address,
                "ens:beta.eth",
                bigname_storage::AddressNameRelation::EffectiveController,
                "beta.eth",
                "beta.eth",
                "node:beta.eth",
                beta_surface_binding_id,
                shared_resource_id,
                Some(shared_token_lineage_id),
                31,
            ),
            address_name_current_row(
                address,
                "ens:alpha.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "alpha.eth",
                "alpha.eth",
                "node:alpha.eth",
                alpha_surface_binding_id,
                shared_resource_id,
                Some(shared_token_lineage_id),
                31,
            ),
            address_name_current_row(
                address,
                "ens:alpha.eth",
                bigname_storage::AddressNameRelation::TokenHolder,
                "alpha.eth",
                "alpha.eth",
                "node:alpha.eth",
                alpha_surface_binding_id,
                shared_resource_id,
                Some(shared_token_lineage_id),
                31,
            ),
        ],
    )
    .await?;

    let surface_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/addresses/{address}/names?dedupe_by=surface"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("surface-dedupe address names request failed")?;
    let surface_payload: AddressNamesResponse = read_json(surface_response).await?;
    assert_eq!(surface_payload.data.len(), 2);

    let resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/addresses/{address}/names?dedupe_by=resource"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource-dedupe address names request failed")?;

    assert_eq!(resource_response.status(), StatusCode::OK);

    let resource_payload: AddressNamesResponse = read_json(resource_response).await?;
    assert_eq!(resource_payload.data.len(), 1);
    assert_eq!(
        resource_payload.data[0].get("logical_name_id"),
        Some(&Value::String("ens:alpha.eth".to_owned()))
    );
    assert_eq!(
        resource_payload.data[0].get("resource_id"),
        Some(&Value::String(shared_resource_id.to_string()))
    );
    assert_eq!(
        resource_payload.data[0].get("relation_facets"),
        Some(&json!([
            "registrant",
            "token_holder",
            "effective_controller"
        ]))
    );
    assert_eq!(resource_payload.coverage, surface_payload.coverage);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_include_role_summary_adds_projection_backed_expansion_fields()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000fed";
    let resource_id = Uuid::from_u128(0x8600);
    let token_lineage_id = Uuid::from_u128(0x8601);
    let surface_binding_id = Uuid::from_u128(0x8602);
    let subject = "0x0000000000000000000000000000000000000abc";
    let other_subject = "0x0000000000000000000000000000000000000def";

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xalpha", None, 61, 1_717_173_061),
            raw_block("ethereum-mainnet", "0xperm", None, 62, 1_717_173_062),
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[address_name_token_lineage(token_lineage_id, "0xalpha", 61)],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[address_name_resource(
            resource_id,
            Some(token_lineage_id),
            "0xalpha",
            61,
        )],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:alpha.eth", "alpha.eth", "node:alpha.eth", 61),
            collection_name_surface(
                "ens:child-one.alpha.eth",
                "child-one.alpha.eth",
                "node:child-one.alpha.eth",
                62,
            ),
            collection_name_surface(
                "ens:child-two.alpha.eth",
                "child-two.alpha.eth",
                "node:child-two.alpha.eth",
                63,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[address_name_surface_binding(
            surface_binding_id,
            "ens:alpha.eth",
            resource_id,
            "0xalpha",
            61,
            1_717_173_061,
        )],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[address_name_current_row(
            address,
            "ens:alpha.eth",
            bigname_storage::AddressNameRelation::Registrant,
            "alpha.eth",
            "alpha.eth",
            "node:alpha.eth",
            surface_binding_id,
            resource_id,
            Some(token_lineage_id),
            61,
        )],
    )
    .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:alpha.eth",
            "alpha.eth",
            "alpha.eth",
            "node:alpha.eth",
            surface_binding_id,
            resource_id,
            Some(token_lineage_id),
            64,
            json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar",
                },
                "control": {
                    "status": "wrapped",
                    "expiry": "2026-09-01T00:00:00Z",
                    "registrant": address,
                    "registry_owner": subject,
                    "latest_event_kind": "NameWrapped",
                },
                "resolver": {
                    "chain_id": "ethereum-mainnet",
                    "address": "0x0000000000000000000000000000000000000aaa",
                    "latest_event_kind": "ResolverChanged",
                },
                "record_inventory": {
                    "status": "supported",
                    "count": 2,
                },
                "history": {
                    "surface_head": null,
                    "resource_head": null,
                },
            }),
        ))
        .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            declared_child_row(
                "ens:alpha.eth",
                "ens:child-one.alpha.eth",
                "child-one.alpha.eth",
                "node:child-one.alpha.eth",
                701,
                62,
            ),
            declared_child_row(
                "ens:alpha.eth",
                "ens:child-two.alpha.eth",
                "child-two.alpha.eth",
                "node:child-two.alpha.eth",
                702,
                63,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            permission_current_row(resource_id, subject, PermissionScope::Resource, 7, 71),
            permission_current_row(
                resource_id,
                subject,
                PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000aaa".to_owned(),
                },
                8,
                72,
            ),
            permission_current_row(resource_id, other_subject, PermissionScope::Registry, 9, 73),
        ],
    )
    .await?;

    let base_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/addresses/{address}/names"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("base address names request failed")?;
    let include_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{address}/names?include=role_summary"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("role summary request failed")?;
    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alpha.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("exact-name request failed")?;

    assert_eq!(base_response.status(), StatusCode::OK);
    assert_eq!(include_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let base_payload: AddressNamesResponse = read_json(base_response).await?;
    let payload: AddressNamesResponse = read_json(include_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;

    assert_eq!(payload.coverage, base_payload.coverage);
    assert_eq!(payload.page, base_payload.page);
    assert_eq!(payload.declared_state, base_payload.declared_state);
    assert_eq!(payload.data.len(), 1);
    assert_eq!(
        payload.data[0].get("logical_name_id"),
        base_payload.data[0].get("logical_name_id")
    );
    assert_eq!(
        payload.data[0].get("resource_id"),
        base_payload.data[0].get("resource_id")
    );
    assert_eq!(
        payload.data[0].get("relation_facets"),
        base_payload.data[0].get("relation_facets")
    );
    assert_eq!(payload.data[0].get("status"), Some(&json!("wrapped")));
    assert_eq!(
        payload.data[0].get("expiry"),
        Some(&json!("2026-09-01T00:00:00Z"))
    );
    assert_eq!(
        name_payload.coverage.get("status").and_then(Value::as_str),
        Some("full")
    );
    assert_eq!(
        name_payload
            .declared_state
            .get("control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("registrant")),
        Some(&json!(address))
    );
    assert_eq!(
        name_payload
            .declared_state
            .get("control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("registry_owner")),
        Some(&json!(subject))
    );
    assert_eq!(
        name_payload
            .declared_state
            .get("control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("latest_event_kind")),
        Some(&json!("NameWrapped"))
    );
    assert!(
        name_payload
            .declared_state
            .get("control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .is_none()
    );
    assert!(
        name_payload
            .declared_state
            .get("control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("expiry"))
            .is_none()
    );
    assert_eq!(payload.data[0].get("record_count"), Some(&json!(2)));
    assert_eq!(payload.data[0].get("subname_count"), Some(&json!(2)));
    assert_eq!(
        payload.data[0].get("role_summary"),
        Some(&json!({
            "subjects": [
                {
                    "subject": subject,
                    "scopes": [
                        {
                            "scope": {
                                "kind": "resolver",
                                "detail": {
                                    "chain_id": "ethereum-mainnet",
                                    "resolver_address": "0x0000000000000000000000000000000000000aaa",
                                },
                            },
                            "effective_powers": ["set_resolver", "create_subnames"],
                        },
                        {
                            "scope": {
                                "kind": "resource",
                                "detail": {},
                            },
                            "effective_powers": ["set_resolver", "set_records"],
                        },
                    ],
                },
                {
                    "subject": other_subject,
                    "scopes": [
                        {
                            "scope": {
                                "kind": "registry",
                                "detail": {},
                            },
                            "effective_powers": ["set_resolver", "set_records"],
                        },
                    ],
                },
            ],
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_rejects_unknown_include_values() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/addresses/0x0000000000000000000000000000000000000abc/names?include=role_summary,unknown")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("invalid include request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "include must contain only role_summary"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_returns_not_found_for_unsupported_namespace_without_storage_read() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/unknown/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_coverage_returns_not_found_for_unsupported_namespace_without_storage_read()
-> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/coverage/unknown/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("coverage request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_returns_internal_error_envelope_on_storage_failure() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        "failed to load current projection for name ens/alice.eth"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_coverage_returns_internal_error_envelope_on_storage_failure() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/coverage/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("coverage request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        "failed to load current projection for name ens/alice.eth"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_history_returns_canonical_only_rows_with_provenance_and_coverage() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0xa001);
    let surface_binding_id = Uuid::from_u128(0xb001);
    let manifest_id_v7 = database
        .insert_manifest(
            "ens",
            "ens_v1_registry_l1",
            "ethereum-mainnet",
            "bootstrap",
            7,
            "active",
            "history-test-v1",
        )
        .await?;
    let manifest_id_v8 = database
        .insert_manifest(
            "ens",
            "ens_v1_registry_l1",
            "ethereum-mainnet",
            "bootstrap-next",
            8,
            "active",
            "history-test-v2",
        )
        .await?;

    database
        .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
        .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x100", None, 100, 1_700_000_100),
            raw_block(
                "ethereum-mainnet",
                "0x101",
                Some("0x100"),
                101,
                1_700_000_101,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x102",
                Some("0x101"),
                102,
                1_700_000_102,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x103",
                Some("0x102"),
                103,
                1_700_000_103,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            NormalizedEvent {
                manifest_version: 7,
                source_manifest_id: Some(manifest_id_v7),
                ..history_event(
                    "history:canonical",
                    Some(logical_name_id),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(100),
                    Some("0x100"),
                    Some("0xtx100"),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                manifest_version: 8,
                source_manifest_id: Some(manifest_id_v8),
                ..history_event(
                    "history:safe",
                    Some(logical_name_id),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(101),
                    Some("0x101"),
                    Some("0xtx101"),
                    Some(0),
                    CanonicalityState::Safe,
                )
            },
            NormalizedEvent {
                manifest_version: 7,
                source_manifest_id: Some(manifest_id_v7),
                ..history_event(
                    "history:finalized",
                    Some(logical_name_id),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(102),
                    Some("0x102"),
                    Some("0xtx102"),
                    Some(0),
                    CanonicalityState::Finalized,
                )
            },
            history_event(
                "history:observed",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(103),
                Some("0x103"),
                Some("0xtx103"),
                Some(0),
                CanonicalityState::Observed,
            ),
            history_event(
                "history:orphaned",
                Some(logical_name_id),
                Some(resource_id),
                None,
                None,
                None,
                None,
                None,
                CanonicalityState::Orphaned,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name history request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: HistoryResponse = read_json(response).await?;
    assert_eq!(
        history_event_identities(&payload),
        vec!["history:finalized", "history:safe", "history:canonical"]
    );
    assert_eq!(payload.page.sort, "chain_position_desc");
    assert_eq!(payload.page.page_size, 50);
    assert_eq!(payload.consistency, "head");
    assert_eq!(payload.last_updated, "2023-11-14T22:15:02Z");
    assert_eq!(payload.verified_state, None);
    assert_eq!(payload.declared_state, json!({}));
    assert_eq!(
        payload.coverage,
        CoverageResponse {
            status: "full".to_owned(),
            exhaustiveness: "authoritative".to_owned(),
            source_classes_considered: vec!["normalized_events".to_owned()],
            enumeration_basis: "canonical normalized-event history for the requested both scope"
                .to_owned(),
            unsupported_reason: None,
        }
    );
    assert_eq!(
        payload
            .provenance
            .get("derivation_kind")
            .and_then(Value::as_str),
        Some("normalized_event_history")
    );
    assert_eq!(
        payload.provenance.get("execution_trace_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        payload.provenance.get("manifest_versions"),
        Some(&json!([
            {
                "manifest_version": 7,
                "source_family": "ens_v1_registry_l1",
                "source_manifest_id": manifest_id_v7
            },
            {
                "manifest_version": 8,
                "source_family": "ens_v1_registry_l1",
                "source_manifest_id": manifest_id_v8
            }
        ]))
    );
    assert_eq!(
        payload
            .provenance
            .get("raw_fact_refs")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(3)
    );
    assert_eq!(
        payload.chain_positions,
        json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 102,
                "block_hash": "0x102",
                "timestamp": "2023-11-14T22:15:02Z"
            }
        })
    );

    let first_row = payload
        .data
        .first()
        .and_then(Value::as_object)
        .expect("first history row must be an object");
    assert_eq!(
        first_row.get("canonicality_state").and_then(Value::as_str),
        Some("finalized")
    );
    assert_eq!(
        first_row.get("chain_position"),
        Some(&json!({
            "chain_id": "ethereum-mainnet",
            "block_number": 102,
            "block_hash": "0x102",
            "timestamp": "2023-11-14T22:15:02Z"
        }))
    );
    assert_eq!(
        first_row.get("provenance"),
        Some(&json!({
            "after": "history:finalized"
        }))
    );
    assert_eq!(
        first_row.get("coverage"),
        Some(&json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["normalized_events"],
            "enumeration_basis": "history:finalized",
            "unsupported_reason": null
        }))
    );

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?page_size=1")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name history first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: HistoryResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("name history first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/names/ens/alice.eth?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name history second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: HistoryResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/names/ens/alice.eth?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name history replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: HistoryResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &payload.data,
        &payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "chain_position_desc",
        50,
        1,
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_history_honors_scope_query_parameter() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0xa100);
    let other_resource_id = Uuid::from_u128(0xa101);
    let surface_binding_id = Uuid::from_u128(0xb100);

    database
        .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
        .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x200", None, 200, 1_700_000_200),
            raw_block(
                "ethereum-mainnet",
                "0x201",
                Some("0x200"),
                201,
                1_700_000_201,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x202",
                Some("0x201"),
                202,
                1_700_000_202,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x203",
                Some("0x202"),
                203,
                1_700_000_203,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x204",
                Some("0x203"),
                204,
                1_700_000_204,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "surface-only",
                Some(logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(200),
                Some("0x200"),
                Some("0xtx200"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "resource-only",
                None,
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(201),
                Some("0x201"),
                Some("0xtx201"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "both-anchors",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(202),
                Some("0x202"),
                Some("0xtx202"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "same-resource-other-name",
                Some("ens:other.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(203),
                Some("0x203"),
                Some("0xtx203"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "same-name-other-resource",
                Some(logical_name_id),
                Some(other_resource_id),
                Some("ethereum-mainnet"),
                Some(204),
                Some("0x204"),
                Some("0xtx204"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let surface_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?scope=surface")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("surface history request failed")?;
    let surface_payload: HistoryResponse = read_json(surface_response).await?;
    assert_eq!(
        history_event_identities(&surface_payload),
        vec!["same-name-other-resource", "both-anchors", "surface-only"]
    );

    let resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?scope=resource")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource history request failed")?;
    let resource_payload: HistoryResponse = read_json(resource_response).await?;
    assert_eq!(
        history_event_identities(&resource_payload),
        vec!["same-resource-other-name", "both-anchors", "resource-only"]
    );

    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?scope=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("combined history request failed")?;
    let both_payload: HistoryResponse = read_json(both_response).await?;
    assert_eq!(
        history_event_identities(&both_payload),
        vec![
            "same-name-other-resource",
            "same-resource-other-name",
            "both-anchors",
            "resource-only",
            "surface-only",
        ]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_history_resource_scope_preserves_rebound_resources() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let old_resource_id = Uuid::from_u128(0xa120);
    let current_resource_id = Uuid::from_u128(0xa121);

    bigname_storage::upsert_name_surfaces(&database.pool, &[name_surface(logical_name_id)]).await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[resource(old_resource_id), resource(current_resource_id)],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            SurfaceBinding {
                active_to: Some(timestamp(1_700_000_250)),
                ..surface_binding(
                    Uuid::from_u128(0xb120),
                    logical_name_id,
                    old_resource_id,
                    timestamp(1_700_000_200),
                )
            },
            surface_binding(
                Uuid::from_u128(0xb121),
                logical_name_id,
                current_resource_id,
                timestamp(1_700_000_251),
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x220", None, 220, 1_700_000_220),
            raw_block(
                "ethereum-mainnet",
                "0x221",
                Some("0x220"),
                221,
                1_700_000_221,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x222",
                Some("0x221"),
                222,
                1_700_000_222,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "resource-old",
                None,
                Some(old_resource_id),
                Some("ethereum-mainnet"),
                Some(220),
                Some("0x220"),
                Some("0xtx220"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "resource-current",
                None,
                Some(current_resource_id),
                Some("ethereum-mainnet"),
                Some(221),
                Some("0x221"),
                Some("0xtx221"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "surface-anchor",
                Some(logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(222),
                Some("0x222"),
                Some("0xtx222"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?scope=resource")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name resource-scope history request failed")?;
    let resource_payload: HistoryResponse = read_json(resource_response).await?;
    assert_eq!(
        history_event_identities(&resource_payload),
        vec!["resource-current", "resource-old"]
    );

    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?scope=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name combined history request failed")?;
    let both_payload: HistoryResponse = read_json(both_response).await?;
    assert_eq!(
        history_event_identities(&both_payload),
        vec!["surface-anchor", "resource-current", "resource-old"]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_history_returns_not_found_when_anchor_is_missing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/missing.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name history request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "name missing.eth was not found in namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_history_returns_not_found_for_unsupported_namespace() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/unknown/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name history request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_history_returns_chain_position_desc_ordering() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0xa300);
    let surface_binding_id = Uuid::from_u128(0xb300);

    database
        .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
        .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("base-mainnet", "0xb101", None, 101, 1_700_000_401),
            raw_block("ethereum-mainnet", "0xe100", None, 100, 1_700_000_400),
            raw_block("base-mainnet", "0xb100", Some("0xb101"), 100, 1_700_000_399),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "no-chain-position",
                Some(logical_name_id),
                Some(resource_id),
                None,
                None,
                None,
                None,
                None,
                CanonicalityState::Canonical,
            ),
            history_event(
                "ethereum-lower-log",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(100),
                Some("0xe100"),
                Some("0xtx100"),
                Some(1),
                CanonicalityState::Canonical,
            ),
            history_event(
                "ethereum-higher-log",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(100),
                Some("0xe100"),
                Some("0xtx100"),
                Some(7),
                CanonicalityState::Canonical,
            ),
            history_event(
                "base-same-height",
                Some(logical_name_id),
                Some(resource_id),
                Some("base-mainnet"),
                Some(100),
                Some("0xb100"),
                Some("0xtx090"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "base-higher-height",
                Some(logical_name_id),
                Some(resource_id),
                Some("base-mainnet"),
                Some(101),
                Some("0xb101"),
                Some("0xtx101"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!("/v1/history/resources/{resource_id}"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource history request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: HistoryResponse = read_json(response).await?;
    assert_eq!(
        history_event_identities(&payload),
        vec![
            "base-higher-height",
            "base-same-height",
            "ethereum-higher-log",
            "ethereum-lower-log",
            "no-chain-position",
        ]
    );
    assert_eq!(payload.page.sort, "chain_position_desc");
    assert_eq!(
        payload.chain_positions,
        json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 101,
                "block_hash": "0xb101",
                "timestamp": "2023-11-14T22:20:01Z"
            },
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 100,
                "block_hash": "0xe100",
                "timestamp": "2023-11-14T22:20:00Z"
            }
        })
    );

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!("/v1/history/resources/{resource_id}?page_size=1"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource history first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: HistoryResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("resource history first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!(
                    "/v1/history/resources/{resource_id}?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource history second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: HistoryResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!(
                    "/v1/history/resources/{resource_id}?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource history replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: HistoryResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &payload.data,
        &payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "chain_position_desc",
        50,
        1,
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_history_honors_scope_query_parameter() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0xa200);
    let other_resource_id = Uuid::from_u128(0xa201);
    let surface_binding_id = Uuid::from_u128(0xb200);

    database
        .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
        .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x300", None, 300, 1_700_000_300),
            raw_block(
                "ethereum-mainnet",
                "0x301",
                Some("0x300"),
                301,
                1_700_000_301,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x302",
                Some("0x301"),
                302,
                1_700_000_302,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x303",
                Some("0x302"),
                303,
                1_700_000_303,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x304",
                Some("0x303"),
                304,
                1_700_000_304,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "surface-only",
                Some(logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(300),
                Some("0x300"),
                Some("0xtx300"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "resource-only",
                None,
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(301),
                Some("0x301"),
                Some("0xtx301"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "both-anchors",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(302),
                Some("0x302"),
                Some("0xtx302"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "same-resource-other-name",
                Some("ens:other.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(303),
                Some("0x303"),
                Some("0xtx303"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "same-name-other-resource",
                Some(logical_name_id),
                Some(other_resource_id),
                Some("ethereum-mainnet"),
                Some(304),
                Some("0x304"),
                Some("0xtx304"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let surface_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!(
                    "/v1/history/resources/{resource_id}?scope=surface"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("surface resource-history request failed")?;
    let surface_payload: HistoryResponse = read_json(surface_response).await?;
    assert_eq!(
        history_event_identities(&surface_payload),
        vec!["same-name-other-resource", "both-anchors", "surface-only"]
    );

    let resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!(
                    "/v1/history/resources/{resource_id}?scope=resource"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource resource-history request failed")?;
    let resource_payload: HistoryResponse = read_json(resource_response).await?;
    assert_eq!(
        history_event_identities(&resource_payload),
        vec!["same-resource-other-name", "both-anchors", "resource-only"]
    );

    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!("/v1/history/resources/{resource_id}?scope=both"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("combined resource-history request failed")?;
    let both_payload: HistoryResponse = read_json(both_response).await?;
    assert_eq!(
        history_event_identities(&both_payload),
        vec![
            "same-name-other-resource",
            "same-resource-other-name",
            "both-anchors",
            "resource-only",
            "surface-only",
        ]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_history_surface_scope_preserves_multiple_bound_surfaces() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource_id = Uuid::from_u128(0xa230);
    let primary_logical_name_id = "ens:alice.eth";
    let alias_logical_name_id = "ens:alice-base.eth";

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            name_surface(primary_logical_name_id),
            name_surface(alias_logical_name_id),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(&database.pool, &[resource(resource_id)]).await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            surface_binding(
                Uuid::from_u128(0xb230),
                primary_logical_name_id,
                resource_id,
                timestamp(1_700_000_300),
            ),
            surface_binding(
                Uuid::from_u128(0xb231),
                alias_logical_name_id,
                resource_id,
                timestamp(1_700_000_301),
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x330", None, 330, 1_700_000_330),
            raw_block(
                "ethereum-mainnet",
                "0x331",
                Some("0x330"),
                331,
                1_700_000_331,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x332",
                Some("0x331"),
                332,
                1_700_000_332,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "surface-primary",
                Some(primary_logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(330),
                Some("0x330"),
                Some("0xtx330"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "surface-alias",
                Some(alias_logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(331),
                Some("0x331"),
                Some("0xtx331"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "resource-anchor",
                None,
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(332),
                Some("0x332"),
                Some("0xtx332"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let surface_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!(
                    "/v1/history/resources/{resource_id}?scope=surface"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource surface-scope history request failed")?;
    let surface_payload: HistoryResponse = read_json(surface_response).await?;
    assert_eq!(
        history_event_identities(&surface_payload),
        vec!["surface-alias", "surface-primary"]
    );

    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!("/v1/history/resources/{resource_id}?scope=both"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource combined history request failed")?;
    let both_payload: HistoryResponse = read_json(both_response).await?;
    assert_eq!(
        history_event_identities(&both_payload),
        vec!["resource-anchor", "surface-alias", "surface-primary"]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_history_composes_current_and_historical_matches() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let current_resource_id = Uuid::from_u128(0xa240);
    let current_token_lineage_id = Uuid::from_u128(0xa241);
    let current_surface_binding_id = Uuid::from_u128(0xb240);
    let basenames_resource_id = Uuid::from_u128(0xa242);
    let basenames_surface_binding_id = Uuid::from_u128(0xb242);
    let historical_resource_id = Uuid::from_u128(0xa243);
    let historical_token_lineage_id = Uuid::from_u128(0xa244);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x540", None, 540, 1_700_000_540),
            raw_block(
                "ethereum-mainnet",
                "0x541",
                Some("0x540"),
                541,
                1_700_000_541,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x542",
                Some("0x541"),
                542,
                1_700_000_542,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x543",
                Some("0x542"),
                543,
                1_700_000_543,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x544",
                Some("0x543"),
                544,
                1_700_000_544,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x545",
                Some("0x544"),
                545,
                1_700_000_545,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x546",
                Some("0x545"),
                546,
                1_700_000_546,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[
            address_name_token_lineage(current_token_lineage_id, "0x540", 540),
            address_name_token_lineage(historical_token_lineage_id, "0x541", 541),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(
                current_resource_id,
                Some(current_token_lineage_id),
                "0x540",
                540,
            ),
            address_name_resource(basenames_resource_id, None, "0x546", 546),
            address_name_resource(
                historical_resource_id,
                Some(historical_token_lineage_id),
                "0x541",
                541,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:current.eth", "current.eth", "node:current.eth", 540),
            collection_name_surface(
                "basenames:filtered.base.eth",
                "filtered.base.eth",
                "node:filtered.base.eth",
                546,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(
                current_surface_binding_id,
                "ens:current.eth",
                current_resource_id,
                "0x540",
                540,
                1_717_173_540,
            ),
            address_name_surface_binding(
                basenames_surface_binding_id,
                "basenames:filtered.base.eth",
                basenames_resource_id,
                "0x546",
                546,
                1_717_173_546,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                address,
                "ens:current.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "current.eth",
                "current.eth",
                "node:current.eth",
                current_surface_binding_id,
                current_resource_id,
                Some(current_token_lineage_id),
                540,
            ),
            address_name_current_row(
                address,
                "basenames:filtered.base.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "filtered.base.eth",
                "filtered.base.eth",
                "node:filtered.base.eth",
                basenames_surface_binding_id,
                basenames_resource_id,
                None,
                546,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "current-surface",
                Some("ens:current.eth"),
                None,
                Some("ethereum-mainnet"),
                Some(544),
                Some("0x544"),
                Some("0xtx544"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "current-resource",
                None,
                Some(current_resource_id),
                Some("ethereum-mainnet"),
                Some(545),
                Some("0x545"),
                Some("0xtx545"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "historical-surface",
                Some("ens:historical.eth"),
                None,
                Some("ethereum-mainnet"),
                Some(543),
                Some("0x543"),
                Some("0xtx543"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "historical-resource",
                None,
                Some(historical_resource_id),
                Some("ethereum-mainnet"),
                Some(542),
                Some("0x542"),
                Some("0xtx542"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            authority_history_event(
                "historical-match",
                "ens",
                "ens:historical.eth",
                historical_resource_id,
                "RegistrationGranted",
                541,
                "0x541",
                json!({
                    "registrant": "0x0000000000000000000000000000000000000ABC",
                }),
            ),
            history_event(
                "filtered-basenames",
                Some("basenames:filtered.base.eth"),
                Some(basenames_resource_id),
                Some("ethereum-mainnet"),
                Some(546),
                Some("0x546"),
                Some("0xtx546"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?namespace=ens&relation=registrant"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: HistoryResponse = read_json(response).await?;
    assert_eq!(
        history_event_identities(&payload),
        vec![
            "current-resource",
            "current-surface",
            "historical-surface",
            "historical-resource",
            "historical-match",
        ]
    );
    assert_eq!(payload.page.sort, "chain_position_desc");
    assert_eq!(payload.page.page_size, 50);
    assert_eq!(
        payload.coverage.enumeration_basis,
        "canonical normalized-event history for the requested both scope"
    );

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?namespace=ens&relation=registrant&page_size=1"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: HistoryResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("address history first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?namespace=ens&relation=registrant&page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: HistoryResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?namespace=ens&relation=registrant&page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: HistoryResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &payload.data,
        &payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "chain_position_desc",
        50,
        1,
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_history_honors_scope_and_relation_filters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000def";
    let current_resource_id = Uuid::from_u128(0xa250);
    let current_token_lineage_id = Uuid::from_u128(0xa251);
    let current_surface_binding_id = Uuid::from_u128(0xb250);
    let controller_resource_id = Uuid::from_u128(0xa252);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x550", None, 550, 1_700_000_550),
            raw_block(
                "ethereum-mainnet",
                "0x551",
                Some("0x550"),
                551,
                1_700_000_551,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x552",
                Some("0x551"),
                552,
                1_700_000_552,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x553",
                Some("0x552"),
                553,
                1_700_000_553,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x554",
                Some("0x553"),
                554,
                1_700_000_554,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x555",
                Some("0x554"),
                555,
                1_700_000_555,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            current_token_lineage_id,
            "0x550",
            550,
        )],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(
                current_resource_id,
                Some(current_token_lineage_id),
                "0x550",
                550,
            ),
            address_name_resource(controller_resource_id, None, "0x551", 551),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[collection_name_surface(
            "ens:current-controller.eth",
            "current-controller.eth",
            "node:current-controller.eth",
            550,
        )],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[address_name_surface_binding(
            current_surface_binding_id,
            "ens:current-controller.eth",
            current_resource_id,
            "0x550",
            550,
            1_717_173_550,
        )],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[address_name_current_row(
            address,
            "ens:current-controller.eth",
            bigname_storage::AddressNameRelation::EffectiveController,
            "current-controller.eth",
            "current-controller.eth",
            "node:current-controller.eth",
            current_surface_binding_id,
            current_resource_id,
            Some(current_token_lineage_id),
            550,
        )],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "current-controller-surface",
                Some("ens:current-controller.eth"),
                None,
                Some("ethereum-mainnet"),
                Some(554),
                Some("0x554"),
                Some("0xtx554"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "current-controller-resource",
                None,
                Some(current_resource_id),
                Some("ethereum-mainnet"),
                Some(555),
                Some("0x555"),
                Some("0xtx555"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "historical-controller-surface",
                Some("ens:historical-controller.eth"),
                None,
                Some("ethereum-mainnet"),
                Some(553),
                Some("0x553"),
                Some("0xtx553"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "historical-controller-resource",
                None,
                Some(controller_resource_id),
                Some("ethereum-mainnet"),
                Some(552),
                Some("0x552"),
                Some("0xtx552"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            authority_history_event(
                "historical-controller-match",
                "ens",
                "ens:historical-controller.eth",
                controller_resource_id,
                "AuthorityTransferred",
                551,
                "0x551",
                json!({
                    "owner": "0x0000000000000000000000000000000000000DEF",
                }),
            ),
        ],
    )
    .await?;

    let surface_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?relation=effective_controller&scope=surface"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history surface-scope request failed")?;
    let surface_payload: HistoryResponse = read_json(surface_response).await?;
    assert_eq!(
        history_event_identities(&surface_payload),
        vec![
            "current-controller-surface",
            "historical-controller-surface",
            "historical-controller-match",
        ]
    );

    let resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?relation=effective_controller&scope=resource"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history resource-scope request failed")?;
    let resource_payload: HistoryResponse = read_json(resource_response).await?;
    assert_eq!(
        history_event_identities(&resource_payload),
        vec![
            "current-controller-resource",
            "historical-controller-resource",
            "historical-controller-match",
        ]
    );

    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?relation=effective_controller&scope=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history combined request failed")?;
    let both_payload: HistoryResponse = read_json(both_response).await?;
    assert_eq!(
        history_event_identities(&both_payload),
        vec![
            "current-controller-resource",
            "current-controller-surface",
            "historical-controller-surface",
            "historical-controller-resource",
            "historical-controller-match",
        ]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_history_returns_not_found_when_anchor_is_missing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource_id = Uuid::from_u128(0xa999);

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!("/v1/history/resources/{resource_id}"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource history request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        format!("resource {resource_id} was not found")
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_permissions_returns_declared_state_collection() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource_id = Uuid::from_u128(0xa300);
    let filtered_subject = "0x0000000000000000000000000000000000000abc";
    let other_subject = "0x0000000000000000000000000000000000000def";

    bigname_storage::upsert_resources(&database.pool, &[resource(resource_id)]).await?;
    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            permission_current_row(
                resource_id,
                filtered_subject,
                PermissionScope::Resource,
                7,
                41,
            ),
            permission_current_row(
                resource_id,
                filtered_subject,
                PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000aaa".to_owned(),
                },
                8,
                42,
            ),
            permission_current_row(resource_id, other_subject, PermissionScope::Registry, 9, 43),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!("/v1/resources/{resource_id}/permissions"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResourcePermissionsResponse = read_json(response).await?;
    assert_eq!(
        permission_subjects(&payload),
        vec![filtered_subject, filtered_subject, other_subject]
    );
    assert!(payload.verified_state.is_none());
    assert_eq!(payload.declared_state, json!({}));
    assert_eq!(payload.page.page_size, 3);
    assert_eq!(payload.page.sort, "subject_scope_asc");
    assert_eq!(payload.consistency, "finalized");
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["permissions_current".to_owned()]
    );
    assert_eq!(payload.coverage.enumeration_basis, "resource_permissions");
    assert_eq!(payload.coverage.unsupported_reason, None);
    assert_eq!(
        payload
            .provenance
            .get("derivation_kind")
            .and_then(Value::as_str),
        Some("permissions_current_rebuild")
    );

    let resource_row = payload
        .data
        .iter()
        .find(|row| {
            row.get("scope")
                .and_then(|value| value.get("kind"))
                .and_then(Value::as_str)
                == Some("resource")
        })
        .expect("resource row");
    assert_eq!(
        resource_row.get("resource_id"),
        Some(&Value::String(resource_id.to_string()))
    );
    assert_eq!(
        resource_row.get("scope"),
        Some(&json!({
            "kind": "resource",
            "detail": {},
        }))
    );
    assert_eq!(
        resource_row.get("effective_powers"),
        Some(&json!(["set_resolver", "set_records"]))
    );
    assert_eq!(resource_row.get("revocation_source"), Some(&Value::Null));

    let resolver_row = payload
        .data
        .iter()
        .find(|row| {
            row.get("scope")
                .and_then(|value| value.get("kind"))
                .and_then(Value::as_str)
                == Some("resolver")
        })
        .expect("resolver row");
    assert_eq!(
        resolver_row.get("scope"),
        Some(&json!({
            "kind": "resolver",
            "detail": {
                "chain_id": "ethereum-mainnet",
                "resolver_address": "0x0000000000000000000000000000000000000aaa",
            },
        }))
    );

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!(
                    "/v1/resources/{resource_id}/permissions?page_size=1"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: ResourcePermissionsResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("resource permissions first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!(
                    "/v1/resources/{resource_id}/permissions?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: ResourcePermissionsResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!(
                    "/v1/resources/{resource_id}/permissions?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: ResourcePermissionsResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &payload.data,
        &payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "subject_scope_asc",
        3,
        1,
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_permissions_honors_subject_and_scope_filters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource_id = Uuid::from_u128(0xa301);
    let shared_subject = "0x0000000000000000000000000000000000000abc";

    bigname_storage::upsert_resources(&database.pool, &[resource(resource_id)]).await?;
    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            permission_current_row(
                resource_id,
                shared_subject,
                PermissionScope::Resource,
                7,
                51,
            ),
            permission_current_row(
                resource_id,
                shared_subject,
                PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000bbb".to_owned(),
                },
                8,
                52,
            ),
            permission_current_row(
                resource_id,
                "0x0000000000000000000000000000000000000def",
                PermissionScope::Resource,
                9,
                53,
            ),
        ],
    )
    .await?;

    let subject_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!(
                    "/v1/resources/{resource_id}/permissions?subject={shared_subject}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions subject filter request failed")?;
    let subject_payload: ResourcePermissionsResponse = read_json(subject_response).await?;
    assert_eq!(
        permission_subjects(&subject_payload),
        vec![shared_subject, shared_subject]
    );

    let scope_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(&format!(
                    "/v1/resources/{resource_id}/permissions?scope=resolver:ethereum-mainnet:0x0000000000000000000000000000000000000bbb"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions scope filter request failed")?;
    let scope_payload: ResourcePermissionsResponse = read_json(scope_response).await?;
    assert_eq!(scope_payload.data.len(), 1);
    assert_eq!(
        scope_payload.data[0].get("scope"),
        Some(&json!({
            "kind": "resolver",
            "detail": {
                "chain_id": "ethereum-mainnet",
                "resolver_address": "0x0000000000000000000000000000000000000bbb",
            },
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_permissions_rejects_invalid_resource_id() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resources/not-a-uuid/permissions")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("invalid resource permissions request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(payload.error.message, "resource_id must be a UUID");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_manifests_returns_active_entries() -> Result<()> {
    let database = TestDatabase::new(true).await?;

    let ens_l1 = database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l1",
            "ethereum-mainnet",
            "ens_v2",
            1,
            "active",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(ens_l1, "declared_children", "supported", None)
        .await?;
    database
        .insert_capability_flag(
            ens_l1,
            "verified_resolution",
            "shadow",
            Some("tracked but not yet served"),
        )
        .await?;

    let ens_l2 = database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l2",
            "base-mainnet",
            "ens_v2_base",
            2,
            "active",
            "uts46-v2",
        )
        .await?;
    database
        .insert_capability_flag(ens_l2, "declared_children", "unsupported", Some("pending"))
        .await?;

    let ens_shadow = database
        .insert_manifest(
            "ens",
            "ens_shadow_registry",
            "ethereum-mainnet",
            "ens_shadow",
            3,
            "shadow",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(ens_shadow, "declared_children", "supported", None)
        .await?;

    let basenames = database
        .insert_manifest(
            "basenames",
            "base_registry",
            "base-mainnet",
            "basenames_v1",
            1,
            "active",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(basenames, "declared_children", "supported", None)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/manifests/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NamespaceManifestsResponse = read_json(response).await?;
    assert_eq!(payload.data.namespace, "ens");
    assert_eq!(payload.consistency, "head");
    assert!(payload.last_updated.ends_with('Z'));
    assert!(payload.verified_state.is_none());
    assert!(payload.chain_positions.is_empty());
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["source_manifests".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "active manifests for the requested namespace"
    );
    assert_eq!(payload.coverage.unsupported_reason, None);
    assert!(payload.provenance.normalized_event_ids.is_empty());
    assert!(payload.provenance.raw_fact_refs.is_empty());
    assert_eq!(payload.provenance.derivation_kind, "declared");
    assert_eq!(payload.provenance.execution_trace_id, None);
    assert_eq!(payload.provenance.manifest_versions.len(), 2);
    assert_eq!(payload.declared_state.manifests.len(), 2);

    assert_eq!(payload.declared_state.manifests[0].manifest_version, 1);
    assert_eq!(
        payload.declared_state.manifests[0].source_family,
        "ens_v2_registry_l1"
    );
    assert_eq!(
        payload.declared_state.manifests[0].chain,
        "ethereum-mainnet"
    );
    assert_eq!(
        payload.declared_state.manifests[0].deployment_epoch,
        "ens_v2"
    );
    assert_eq!(
        payload.declared_state.manifests[0].normalizer_version,
        "uts46-v1"
    );
    assert_eq!(
        payload.declared_state.manifests[0]
            .capability_flags
            .get("declared_children")
            .expect("declared_children capability")
            .status,
        bigname_manifests::CapabilitySupportStatus::Supported
    );
    assert_eq!(
        payload.declared_state.manifests[0]
            .capability_flags
            .get("verified_resolution")
            .expect("verified_resolution capability")
            .notes
            .as_deref(),
        Some("tracked but not yet served")
    );
    assert_eq!(
        payload.provenance.manifest_versions[0],
        ManifestVersionRef {
            manifest_version: 1,
            source_family: "ens_v2_registry_l1".to_owned(),
            chain: "ethereum-mainnet".to_owned(),
            deployment_epoch: "ens_v2".to_owned(),
        }
    );

    assert_eq!(payload.declared_state.manifests[1].manifest_version, 2);
    assert_eq!(
        payload.declared_state.manifests[1].source_family,
        "ens_v2_registry_l2"
    );
    assert_eq!(payload.declared_state.manifests[1].chain, "base-mainnet");
    assert_eq!(
        payload.declared_state.manifests[1].deployment_epoch,
        "ens_v2_base"
    );
    assert_eq!(
        payload.declared_state.manifests[1].normalizer_version,
        "uts46-v2"
    );
    assert_eq!(
        payload.declared_state.manifests[1]
            .capability_flags
            .get("declared_children")
            .expect("declared_children capability")
            .status,
        bigname_manifests::CapabilitySupportStatus::Unsupported
    );
    assert_eq!(
        payload.provenance.manifest_versions[1],
        ManifestVersionRef {
            manifest_version: 2,
            source_family: "ens_v2_registry_l2".to_owned(),
            chain: "base-mainnet".to_owned(),
            deployment_epoch: "ens_v2_base".to_owned(),
        }
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_metadata_returns_active_summary() -> Result<()> {
    let database = TestDatabase::new(true).await?;

    let ens_l1 = database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l1",
            "ethereum-mainnet",
            "ens_v2",
            1,
            "active",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(ens_l1, "declared_children", "supported", None)
        .await?;

    let ens_l2 = database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l2",
            "base-mainnet",
            "ens_v2_base",
            2,
            "active",
            "uts46-v2",
        )
        .await?;
    database
        .insert_capability_flag(ens_l2, "declared_children", "shadow", Some("shadowed"))
        .await?;

    let ens_shadow = database
        .insert_manifest(
            "ens",
            "ens_shadow_registry",
            "ethereum-mainnet",
            "ens_shadow",
            3,
            "shadow",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(ens_shadow, "verified_resolution", "shadow", None)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("namespace metadata request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NamespaceMetadataResponse = read_json(response).await?;
    assert_eq!(payload.data.namespace, "ens");
    assert_eq!(payload.declared_state.active_manifest_count, 2);
    assert_eq!(
        payload.declared_state.active_source_families,
        vec![
            "ens_v2_registry_l1".to_owned(),
            "ens_v2_registry_l2".to_owned()
        ]
    );
    assert_eq!(
        payload.declared_state.chains,
        vec!["base-mainnet".to_owned(), "ethereum-mainnet".to_owned()]
    );
    assert_eq!(
        payload.declared_state.normalizer_versions,
        vec!["uts46-v1".to_owned(), "uts46-v2".to_owned()]
    );
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["source_manifests".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "active manifests for the requested namespace"
    );
    assert_eq!(payload.coverage.unsupported_reason, None);
    assert_eq!(payload.provenance.derivation_kind, "declared");
    assert_eq!(payload.provenance.execution_trace_id, None);
    assert!(payload.provenance.normalized_event_ids.is_empty());
    assert!(payload.provenance.raw_fact_refs.is_empty());
    assert_eq!(payload.provenance.manifest_versions.len(), 2);
    assert_eq!(
        payload.provenance.manifest_versions[0],
        ManifestVersionRef {
            manifest_version: 1,
            source_family: "ens_v2_registry_l1".to_owned(),
            chain: "ethereum-mainnet".to_owned(),
            deployment_epoch: "ens_v2".to_owned(),
        }
    );
    assert_eq!(
        payload.provenance.manifest_versions[1],
        ManifestVersionRef {
            manifest_version: 2,
            source_family: "ens_v2_registry_l2".to_owned(),
            chain: "base-mainnet".to_owned(),
            deployment_epoch: "ens_v2_base".to_owned(),
        }
    );
    assert_eq!(payload.consistency, "head");
    assert!(payload.last_updated.ends_with('Z'));
    assert!(payload.verified_state.is_none());
    assert!(payload.chain_positions.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_metadata_returns_empty_summary_when_namespace_has_no_active_manifests()
-> Result<()> {
    let database = TestDatabase::new(true).await?;

    let ens_shadow = database
        .insert_manifest(
            "ens",
            "ens_shadow_registry",
            "ethereum-mainnet",
            "ens_shadow",
            1,
            "shadow",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(ens_shadow, "declared_children", "supported", None)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("namespace metadata request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NamespaceMetadataResponse = read_json(response).await?;
    assert_eq!(payload.data.namespace, "ens");
    assert_eq!(payload.declared_state.active_manifest_count, 0);
    assert!(payload.declared_state.active_source_families.is_empty());
    assert!(payload.declared_state.chains.is_empty());
    assert!(payload.declared_state.normalizer_versions.is_empty());
    assert!(payload.provenance.manifest_versions.is_empty());
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["source_manifests".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "active manifests for the requested namespace"
    );
    assert_eq!(payload.coverage.unsupported_reason, None);
    assert_eq!(payload.provenance.derivation_kind, "declared");
    assert_eq!(payload.consistency, "head");
    assert!(payload.last_updated.ends_with('Z'));
    assert!(payload.verified_state.is_none());
    assert!(payload.chain_positions.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_metadata_returns_internal_error_envelope_on_load_failure() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("namespace metadata request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        "failed to load namespace metadata for namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_metadata_returns_not_found_for_unknown_namespace() -> Result<()> {
    let database = TestDatabase::new(true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/unknown")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("namespace metadata request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_manifests_returns_empty_list_when_namespace_has_no_active_entries()
-> Result<()> {
    let database = TestDatabase::new(true).await?;

    let ens_shadow = database
        .insert_manifest(
            "ens",
            "ens_shadow_registry",
            "ethereum-mainnet",
            "ens_shadow",
            1,
            "shadow",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(ens_shadow, "declared_children", "supported", None)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/manifests/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NamespaceManifestsResponse = read_json(response).await?;
    assert_eq!(payload.data.namespace, "ens");
    assert!(payload.declared_state.manifests.is_empty());
    assert!(payload.provenance.manifest_versions.is_empty());
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["source_manifests".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "active manifests for the requested namespace"
    );
    assert_eq!(payload.provenance.derivation_kind, "declared");
    assert_eq!(payload.consistency, "head");
    assert!(payload.last_updated.ends_with('Z'));
    assert!(payload.verified_state.is_none());
    assert!(payload.chain_positions.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_manifests_returns_internal_error_envelope_on_load_failure() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/manifests/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        "failed to load manifest snapshot for namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_manifests_returns_not_found_for_unknown_namespace() -> Result<()> {
    let database = TestDatabase::new(true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/manifests/unknown")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_freezes_bootstrap_mode_envelopes() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let default_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000ABC?namespace=ens&coin_type=60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("default primary-name request failed")?;
    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=declared")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared primary-name request failed")?;
    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified primary-name request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("both primary-name request failed")?;

    assert_eq!(default_response.status(), StatusCode::OK);
    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let default_payload: PrimaryNameResponse = read_json(default_response).await?;
    let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;

    assert_eq!(
        default_payload.data,
        json!({
            "address": "0x0000000000000000000000000000000000000abc",
            "namespace": "ens",
            "coin_type": "60",
        })
    );
    assert_eq!(default_payload.data, declared_payload.data);
    assert_eq!(default_payload.data, verified_payload.data);
    assert_eq!(default_payload.data, both_payload.data);

    assert_eq!(
        default_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "unsupported_reason": "declared primary-name claim surface is not yet supported",
            }
        }))
    );
    assert_eq!(
        declared_payload.declared_state,
        default_payload.declared_state
    );
    assert_eq!(default_payload.verified_state, None);
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "unsupported",
                "unsupported_reason": "verified primary-name entrypoint is not yet supported",
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "unsupported_reason": "declared primary-name claim surface is not yet supported",
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    assert_eq!(
        default_payload.coverage,
        json!({
            "status": "unsupported",
            "exhaustiveness": "not_applicable",
            "source_classes_considered": [],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": "primary-name coverage is not yet supported",
        })
    );
    assert_eq!(
        default_payload.provenance.get("derivation_kind"),
        Some(&json!("primary_name_route_bootstrap"))
    );
    assert_eq!(default_payload.chain_positions, json!({}));
    assert_eq!(default_payload.consistency, "head");
    assert!(default_payload.last_updated.ends_with('Z'));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_returns_not_found_for_tuple_miss_when_projection_exists() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .insert_primary_name_current_row("0x0000000000000000000000000000000000000abc", "ens", "61")
        .await?;

    let other_verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:other.eth",
            "namespace": "ens",
            "normalized_name": "other.eth",
            "canonical_display_name": "other.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000456",
            "resource_id": "00000000-0000-0000-0000-000000000999",
            "binding_kind": "declared_registry_path"
        }
    });
    let other_trace = primary_name_execution_trace(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000031),
        "ens",
        "0x0000000000000000000000000000000000000abc",
        "61",
        other_verified_primary_name.clone(),
        timestamp(1_717_172_301),
    );
    let other_outcome = primary_name_execution_outcome(
        other_trace.execution_trace_id,
        "ens",
        "0x0000000000000000000000000000000000000abc",
        "61",
        other_verified_primary_name,
        timestamp(1_717_172_301),
    );
    upsert_execution_trace(&database.pool, &other_trace).await?;
    upsert_execution_outcome(&database.pool, &other_outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name tuple miss request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(
        payload.provenance.get("execution_trace_id"),
        Some(&Value::Null)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_declared_claim_status_for_exact_tuple() -> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    database
        .insert_primary_name_current_claim_row(
            "0x0000000000000000000000000000000000000abc",
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
        )
        .await?;

    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=declared")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared primary-name status request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name status request failed")?;

    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;

    assert_eq!(
        declared_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "provenance": {},
            }
        }))
    );
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(both_payload.declared_state, declared_payload.declared_state);
    assert_eq!(
        both_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "unsupported",
                "unsupported_reason": "verified primary-name entrypoint is not yet supported",
            }
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_declared_claim_provenance_for_exact_tuple() -> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    database
        .insert_primary_name_current_claim_row_with_provenance(
            address,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
            json!({
                "source_family": "target_reverse",
                "contract_role": "reverse_registrar",
                "contract_instance_id": "00000000-0000-0000-0000-000000000123",
                "emitting_address": "0x00000000000000000000000000000000000000ad",
                "execution_trace_id": "must-be-omitted",
                "verified_primary_name_lookup": {
                    "address": address,
                    "namespace": "ens",
                    "coin_type": "60",
                },
                "verified_primary_name_invalidation": {
                    "claim_status": "success",
                    "primary_claim_source": {
                        "seed": "ignored",
                    },
                },
            }),
        )
        .await?;
    database
        .insert_primary_name_current_claim_row_with_provenance(
            address,
            "ens",
            "61",
            PrimaryNameClaimStatus::Success,
            None,
            json!({
                "source_family": "sibling_reverse",
            }),
        )
        .await?;

    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared primary-name provenance request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name provenance request failed")?;

    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let expected_claimed_primary_name = json!({
        "status": "success",
        "provenance": {
            "source_family": "target_reverse",
            "contract_role": "reverse_registrar",
            "contract_instance_id": "00000000-0000-0000-0000-000000000123",
            "emitting_address": "0x00000000000000000000000000000000000000ad",
        },
    });

    assert_eq!(
        declared_payload.declared_state,
        Some(json!({
            "claimed_primary_name": expected_claimed_primary_name.clone(),
        }))
    );
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(both_payload.declared_state, declared_payload.declared_state);
    assert_eq!(
        both_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "unsupported",
                "unsupported_reason": "verified primary-name entrypoint is not yet supported",
            }
        }))
    );

    let claimed_primary_name = declared_payload
        .declared_state
        .as_ref()
        .and_then(|declared_state| declared_state.get("claimed_primary_name"))
        .and_then(Value::as_object)
        .expect("declared claimed_primary_name must be present");
    let provenance = claimed_primary_name
        .get("provenance")
        .and_then(Value::as_object)
        .expect("declared claimed_primary_name provenance must be present");
    assert!(!provenance.contains_key("execution_trace_id"));
    assert!(!provenance.contains_key("verified_primary_name_lookup"));
    assert!(!provenance.contains_key("verified_primary_name_invalidation"));
    assert_eq!(
        provenance.get("source_family"),
        Some(&json!("target_reverse"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_raw_claim_name_for_invalid_name_exact_tuple() -> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    database
        .insert_primary_name_current_claim_row(
            "0x0000000000000000000000000000000000000abc",
            "ens",
            "60",
            PrimaryNameClaimStatus::InvalidName,
            Some("alice..eth"),
        )
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed invalid-name primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    let claimed_primary_name = payload
        .declared_state
        .as_ref()
        .and_then(|declared_state| declared_state.get("claimed_primary_name"))
        .and_then(Value::as_object)
        .expect("declared claimed_primary_name must be present");

    assert_eq!(
        claimed_primary_name.get("status"),
        Some(&json!("invalid_name"))
    );
    assert_eq!(
        claimed_primary_name.get("raw_claim_name"),
        Some(&json!("alice..eth"))
    );
    assert_eq!(claimed_primary_name.get("provenance"), Some(&json!({})));
    assert!(
        !claimed_primary_name.contains_key("name"),
        "declared invalid-name readback must not backfill claimed_primary_name.name"
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "unsupported",
                "unsupported_reason": "verified primary-name entrypoint is not yet supported",
            }
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_persisted_verified_primary_name_for_exact_tuple() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000041);
    let finished_at = timestamp(1_717_172_401);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;
    database
        .insert_primary_name_current_row(address, "ens", "61")
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let other_trace = primary_name_execution_trace(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000042),
        "ens",
        address,
        "61",
        json!({
            "status": "mismatch",
            "name": {
                "logical_name_id": "ens:other.eth",
                "namespace": "ens",
                "normalized_name": "other.eth",
                "canonical_display_name": "other.eth",
                "namehash": "0x0000000000000000000000000000000000000000000000000000000000000456",
                "resource_id": "00000000-0000-0000-0000-000000000999",
                "binding_kind": "declared_registry_path"
            },
            "failure_reason": "resolved_address_mismatch"
        }),
        timestamp(1_717_172_499),
    );
    let other_outcome = primary_name_execution_outcome(
        other_trace.execution_trace_id,
        "ens",
        address,
        "61",
        json!({
            "status": "mismatch",
            "name": {
                "logical_name_id": "ens:other.eth",
                "namespace": "ens",
                "normalized_name": "other.eth",
                "canonical_display_name": "other.eth",
                "namehash": "0x0000000000000000000000000000000000000000000000000000000000000456",
                "resource_id": "00000000-0000-0000-0000-000000000999",
                "binding_kind": "declared_registry_path"
            },
            "failure_reason": "resolved_address_mismatch"
        }),
        timestamp(1_717_172_499),
    );
    upsert_execution_trace(&database.pool, &other_trace).await?;
    upsert_execution_outcome(&database.pool, &other_outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified primary-name persisted readback request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name persisted readback request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions(),
        "execution_trace_id": execution_trace_id.to_string(),
    });

    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "ens:alice.eth",
                    "namespace": "ens",
                    "normalized_name": "alice.eth",
                    "canonical_display_name": "Alice.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
                    "resource_id": "00000000-0000-0000-0000-000000000456",
                    "binding_kind": "declared_registry_path"
                },
                "provenance": verified_section_provenance.clone(),
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "provenance": {},
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    assert_eq!(
        verified_payload.provenance,
        json!({
            "normalized_event_ids": [],
            "raw_fact_refs": [],
            "manifest_versions": primary_name_execution_manifest_versions(),
            "execution_trace_id": execution_trace_id.to_string(),
            "derivation_kind": "primary_name_route_bootstrap",
        })
    );
    assert_eq!(both_payload.provenance, verified_payload.provenance);
    let verified_primary_name = verified_payload
        .verified_state
        .as_ref()
        .and_then(|verified_state| verified_state.get("verified_primary_name"))
        .and_then(Value::as_object)
        .expect("verified_primary_name must be present");
    assert_eq!(
        verified_primary_name.get("provenance"),
        Some(&verified_section_provenance)
    );
    assert_eq!(
        verified_primary_name
            .get("provenance")
            .and_then(|provenance| provenance.get("execution_trace_id")),
        verified_payload.provenance.get("execution_trace_id"),
    );
    assert_eq!(
        verified_primary_name
            .get("provenance")
            .and_then(|provenance| provenance.get("manifest_versions")),
        verified_payload.provenance.get("manifest_versions"),
    );
    assert_eq!(
        verified_payload.coverage,
        json!({
            "status": "unsupported",
            "exhaustiveness": "not_applicable",
            "source_classes_considered": [],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": "primary-name coverage is not yet supported",
        })
    );
    assert_eq!(both_payload.coverage, verified_payload.coverage);
    assert_eq!(verified_payload.last_updated, "2024-05-31T16:20:01Z");
    assert_eq!(both_payload.last_updated, "2024-05-31T16:20:01Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_persisted_verified_primary_name_mismatch_for_exact_tuple()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000043);
    let finished_at = timestamp(1_717_172_403);
    let verified_primary_name = json!({
        "status": "mismatch",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        },
        "failure_reason": "resolved_target_mismatch"
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified primary-name persisted mismatch request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name persisted mismatch request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions(),
        "execution_trace_id": execution_trace_id.to_string(),
    });

    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "mismatch",
                "name": {
                    "logical_name_id": "ens:alice.eth",
                    "namespace": "ens",
                    "normalized_name": "alice.eth",
                    "canonical_display_name": "Alice.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
                    "resource_id": "00000000-0000-0000-0000-000000000456",
                    "binding_kind": "declared_registry_path"
                },
                "failure_reason": "resolved_target_mismatch",
                "provenance": verified_section_provenance.clone(),
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "provenance": {},
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    let verified_primary_name = verified_payload
        .verified_state
        .as_ref()
        .and_then(|verified_state| verified_state.get("verified_primary_name"))
        .and_then(Value::as_object)
        .expect("verified_primary_name must be present");
    assert_eq!(
        verified_primary_name.get("provenance"),
        Some(&verified_section_provenance)
    );
    assert_eq!(
        verified_primary_name
            .get("provenance")
            .and_then(|provenance| provenance.get("execution_trace_id")),
        verified_payload.provenance.get("execution_trace_id"),
    );
    assert_eq!(
        verified_primary_name
            .get("provenance")
            .and_then(|provenance| provenance.get("manifest_versions")),
        verified_payload.provenance.get("manifest_versions"),
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_rejects_malformed_persisted_verified_primary_name_section() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000044);
    let finished_at = timestamp(1_717_172_404);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    let mut outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );
    outcome
        .outcome_payload
        .as_mut()
        .and_then(Value::as_object_mut)
        .and_then(|payload| payload.get_mut("verified_primary_name"))
        .and_then(Value::as_object_mut)
        .expect("verified_primary_name section must be present")
        .insert("legacy_field".to_owned(), json!("unexpected"));

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed persisted verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        format!("persisted verified primary-name payload mismatch for address {address}")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_rejects_persisted_verified_primary_name_manifest_version_drift()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000045);
    let finished_at = timestamp(1_717_172_405);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let mut trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    trace.manifest_context = json!({
        "manifest_versions": [{
            "manifest_version": 99,
            "source_family": "ens_v1_registry"
        }],
    });
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest-drift verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        format!("persisted verified primary-name provenance mismatch for address {address}")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_omits_verified_section_provenance_for_unsupported_boundaries()
-> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    database
        .insert_primary_name_current_row("0x0000000000000000000000000000000000000abc", "ens", "60")
        .await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("unsupported verified primary-name request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("unsupported mixed primary-name request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_primary_name = verified_payload
        .verified_state
        .as_ref()
        .and_then(|verified_state| verified_state.get("verified_primary_name"))
        .and_then(Value::as_object)
        .expect("verified_primary_name must be present");
    let both_verified_primary_name = both_payload
        .verified_state
        .as_ref()
        .and_then(|verified_state| verified_state.get("verified_primary_name"))
        .and_then(Value::as_object)
        .expect("verified_primary_name must be present");

    assert_eq!(
        verified_primary_name.get("status"),
        Some(&json!("unsupported"))
    );
    assert_eq!(
        verified_primary_name.get("unsupported_reason"),
        Some(&json!(
            "verified primary-name entrypoint is not yet supported"
        ))
    );
    assert!(!verified_primary_name.contains_key("provenance"));
    assert_eq!(both_verified_primary_name, verified_primary_name);
    assert_eq!(
        verified_payload.provenance.get("execution_trace_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        verified_payload.provenance.get("manifest_versions"),
        Some(&json!([]))
    );
    assert_eq!(both_payload.provenance, verified_payload.provenance);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_freezes_bootstrap_behavior_for_tuple_present() -> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    database
        .insert_primary_name_current_row("0x0000000000000000000000000000000000000abc", "ens", "60")
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name tuple present request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "provenance": {},
            }
        }))
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "unsupported",
                "unsupported_reason": "verified primary-name entrypoint is not yet supported",
            }
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_requires_namespace_and_coin_type() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let missing_namespace = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?coin_type=60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("missing-namespace request failed")?;
    let missing_coin_type = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("missing-coin-type request failed")?;

    assert_eq!(missing_namespace.status(), StatusCode::BAD_REQUEST);
    assert_eq!(missing_coin_type.status(), StatusCode::BAD_REQUEST);

    let missing_namespace_payload: ErrorResponse = read_json(missing_namespace).await?;
    let missing_coin_type_payload: ErrorResponse = read_json(missing_coin_type).await?;
    assert_eq!(missing_namespace_payload.error.code, "invalid_input");
    assert_eq!(
        missing_namespace_payload.error.message,
        "namespace is required"
    );
    assert_eq!(missing_coin_type_payload.error.code, "invalid_input");
    assert_eq!(
        missing_coin_type_payload.error.message,
        "coin_type is required"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_rejects_malformed_input() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let malformed_address = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/not-an-address?namespace=ens&coin_type=60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed-address request failed")?;
    let malformed_coin_type = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60,61")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed-coin-type request failed")?;

    assert_eq!(malformed_address.status(), StatusCode::BAD_REQUEST);
    assert_eq!(malformed_coin_type.status(), StatusCode::BAD_REQUEST);

    let malformed_address_payload: ErrorResponse = read_json(malformed_address).await?;
    let malformed_coin_type_payload: ErrorResponse = read_json(malformed_coin_type).await?;
    assert_eq!(malformed_address_payload.error.code, "invalid_input");
    assert_eq!(
        malformed_address_payload.error.message,
        "address must be a 0x-prefixed 20-byte hex string"
    );
    assert_eq!(malformed_coin_type_payload.error.code, "invalid_input");
    assert_eq!(
        malformed_coin_type_payload.error.message,
        "coin_type must contain only decimal digits"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_returns_not_found_for_unsupported_namespace() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=unknown&coin_type=60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("unsupported-namespace primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");

    database.cleanup().await?;
    Ok(())
}

fn openapi_paths(document: &Value) -> &serde_json::Map<String, Value> {
    document
        .get("paths")
        .and_then(Value::as_object)
        .expect("OpenAPI document must expose paths")
}

fn openapi_operation<'a>(document: &'a Value, path: &str) -> &'a Value {
    openapi_paths(document)
        .get(path)
        .and_then(|path_item| path_item.get("get"))
        .expect("OpenAPI path must expose a GET operation")
}

fn openapi_parameter<'a>(operation: &'a Value, name: &str) -> &'a Value {
    operation
        .get("parameters")
        .and_then(Value::as_array)
        .expect("OpenAPI operation must expose parameters")
        .iter()
        .find(|parameter| parameter.get("name") == Some(&json!(name)))
        .expect("expected parameter to exist")
}

fn openapi_schema<'a>(document: &'a Value, name: &str) -> &'a Value {
    document
        .get("components")
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_object)
        .and_then(|schemas| schemas.get(name))
        .expect("expected OpenAPI schema to exist")
}

fn required_fields(schema: &Value) -> Vec<&str> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .expect("schema must define required fields")
        .iter()
        .map(|value| {
            value
                .as_str()
                .expect("required field names must be strings")
        })
        .collect()
}

#[test]
fn openapi_document_publishes_only_shipped_routes() {
    let document = openapi_document();
    let actual = openapi_paths(&document).keys().cloned().collect::<Vec<_>>();

    assert_eq!(
        actual,
        vec![
            "/v1/addresses/{address}/names".to_owned(),
            "/v1/coverage/{namespace}/{name}".to_owned(),
            "/v1/explain/names/{namespace}/{name}/authority-control".to_owned(),
            "/v1/explain/names/{namespace}/{name}/surface-binding".to_owned(),
            "/v1/explain/resolutions/{namespace}/{name}/execution".to_owned(),
            "/v1/history/addresses/{address}".to_owned(),
            "/v1/history/names/{namespace}/{name}".to_owned(),
            "/v1/history/resources/{resource_id}".to_owned(),
            "/v1/manifests/{namespace}".to_owned(),
            "/v1/names/{namespace}/{name}".to_owned(),
            "/v1/names/{namespace}/{name}/children".to_owned(),
            "/v1/namespaces/{namespace}".to_owned(),
            "/v1/primary-names/{address}".to_owned(),
            "/v1/resolutions/{namespace}/{name}".to_owned(),
            "/v1/resolvers/{chain_id}/{resolver_address}".to_owned(),
            "/v1/resources/{resource_id}/permissions".to_owned(),
        ]
    );
    assert!(!openapi_paths(&document).contains_key("/healthz"));
}

#[test]
fn openapi_document_freezes_query_params_and_shared_envelopes() {
    let document = openapi_document();

    let address_names = openapi_operation(&document, "/v1/addresses/{address}/names");
    let dedupe_by = openapi_parameter(address_names, "dedupe_by");
    assert_eq!(
        dedupe_by.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["surface", "resource"],
            "default": "surface",
        }))
    );
    let page_size = openapi_parameter(address_names, "page_size");
    assert_eq!(
        page_size.get("schema"),
        Some(&json!({
            "type": "integer",
            "minimum": 1,
            "maximum": MAX_PAGE_SIZE,
        }))
    );

    let address_history = openapi_operation(&document, "/v1/history/addresses/{address}");
    let history_scope = openapi_parameter(address_history, "scope");
    assert_eq!(
        history_scope.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["surface", "resource", "both"],
            "default": "both",
        }))
    );

    let children = openapi_operation(&document, "/v1/names/{namespace}/{name}/children");
    let surface_classes = openapi_parameter(children, "surface_classes");
    assert_eq!(
        surface_classes.get("schema"),
        Some(&json!({
            "type": "string",
            "default": "declared",
        }))
    );
    assert_eq!(surface_classes.get("style"), Some(&json!("form")));
    assert_eq!(surface_classes.get("explode"), Some(&json!(false)));

    let resolutions = openapi_operation(&document, "/v1/resolutions/{namespace}/{name}");
    let mode = openapi_parameter(resolutions, "mode");
    assert_eq!(
        mode.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["declared", "verified", "both"],
            "default": "declared",
        }))
    );
    let records = openapi_parameter(resolutions, "records");
    assert_eq!(records.get("style"), Some(&json!("form")));
    assert_eq!(records.get("explode"), Some(&json!(false)));

    let resolution_execution = openapi_operation(
        &document,
        "/v1/explain/resolutions/{namespace}/{name}/execution",
    );
    let resolution_execution_parameters = resolution_execution
        .get("parameters")
        .and_then(Value::as_array)
        .expect("resolution execution explain must expose parameters");
    let resolution_execution_parameter_names = resolution_execution_parameters
        .iter()
        .filter_map(|parameter| parameter.get("name").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert_eq!(
        resolution_execution_parameter_names,
        vec!["namespace", "name", "records"]
    );
    let resolution_execution_records = openapi_parameter(resolution_execution, "records");
    assert_eq!(
        resolution_execution_records.get("schema"),
        Some(&json!({ "type": "string" }))
    );
    assert_eq!(
        resolution_execution_records.get("required"),
        Some(&json!(true))
    );
    assert_eq!(
        resolution_execution_records.get("style"),
        Some(&json!("form"))
    );
    assert_eq!(
        resolution_execution_records.get("explode"),
        Some(&json!(false))
    );

    let primary_names = openapi_operation(&document, "/v1/primary-names/{address}");
    let primary_namespace = openapi_parameter(primary_names, "namespace");
    assert_eq!(primary_namespace.get("required"), Some(&json!(true)));
    assert_eq!(
        primary_namespace.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["ens", "basenames"],
        }))
    );
    let primary_coin_type = openapi_parameter(primary_names, "coin_type");
    assert_eq!(primary_coin_type.get("required"), Some(&json!(true)));
    assert_eq!(
        primary_coin_type.get("schema"),
        Some(&json!({
            "type": "string",
            "pattern": "^[0-9]+$",
        }))
    );
    let primary_mode = openapi_parameter(primary_names, "mode");
    assert_eq!(
        primary_mode.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["declared", "verified", "both"],
            "default": "declared",
        }))
    );

    let exact_name = openapi_schema(&document, "ExactNameResponse");
    assert_eq!(
        required_fields(exact_name),
        vec![
            "data",
            "declared_state",
            "verified_state",
            "provenance",
            "coverage",
            "chain_positions",
            "consistency",
            "last_updated",
        ]
    );
    assert_eq!(
        exact_name
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("coverage")),
        Some(&json!({ "$ref": "#/components/schemas/CoverageResponse" }))
    );

    let collection = openapi_schema(&document, "CollectionResponse");
    assert_eq!(
        required_fields(collection),
        vec![
            "data",
            "declared_state",
            "verified_state",
            "provenance",
            "coverage",
            "chain_positions",
            "consistency",
            "last_updated",
            "page",
        ]
    );
    assert_eq!(
        collection
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("page")),
        Some(&json!({ "$ref": "#/components/schemas/HistoryPageResponse" }))
    );

    let resolution = openapi_schema(&document, "ResolutionResponse");
    assert_eq!(
        resolution
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("declared_state")),
        Some(&json!({
            "type": ["object", "null"],
            "additionalProperties": true,
        }))
    );
    assert_eq!(
        resolution
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("verified_state")),
        Some(&json!({
            "type": ["object", "null"],
            "additionalProperties": true,
        }))
    );

    let primary_name = openapi_schema(&document, "PrimaryNameResponse");
    assert_eq!(
        primary_name
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("data")),
        Some(&json!({ "$ref": "#/components/schemas/PrimaryNameData" }))
    );
    assert_eq!(
        primary_name
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("declared_state")),
        Some(&json!({
            "anyOf": [
                { "$ref": "#/components/schemas/PrimaryNameDeclaredState" },
                { "$ref": "#/components/schemas/NullValue" },
            ],
        }))
    );
    assert_eq!(
        primary_name
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("verified_state")),
        Some(&json!({
            "anyOf": [
                { "$ref": "#/components/schemas/PrimaryNameVerifiedState" },
                { "$ref": "#/components/schemas/NullValue" },
            ],
        }))
    );
    let primary_name_declared_state = openapi_schema(&document, "PrimaryNameDeclaredState");
    assert_eq!(
        required_fields(primary_name_declared_state),
        vec!["claimed_primary_name"]
    );
    assert_eq!(
        primary_name_declared_state
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("claimed_primary_name")),
        Some(&json!({ "$ref": "#/components/schemas/PrimaryNameClaimedResult" }))
    );
    assert_eq!(
        primary_name_declared_state.get("additionalProperties"),
        Some(&json!(false))
    );

    let primary_name_verified_state = openapi_schema(&document, "PrimaryNameVerifiedState");
    assert_eq!(
        required_fields(primary_name_verified_state),
        vec!["verified_primary_name"]
    );
    assert_eq!(
        primary_name_verified_state
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("verified_primary_name")),
        Some(&json!({ "$ref": "#/components/schemas/PrimaryNameVerifiedResult" }))
    );
    assert_eq!(
        primary_name_verified_state.get("additionalProperties"),
        Some(&json!(false))
    );

    let primary_name_verified_result = openapi_schema(&document, "PrimaryNameVerifiedResult");
    assert_eq!(
        primary_name_verified_result.get("type"),
        Some(&json!("object"))
    );
    assert_eq!(
        required_fields(primary_name_verified_result),
        vec!["status"]
    );
    assert_eq!(
        primary_name_verified_result
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("status")),
        Some(&json!({
            "type": "string",
        }))
    );
    assert_eq!(
        primary_name_verified_result
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("provenance")),
        Some(&json!({
            "$ref": "#/components/schemas/PrimaryNameVerifiedResultProvenance",
        }))
    );
    assert_eq!(
        primary_name_verified_result.get("additionalProperties"),
        Some(&json!(true))
    );

    let primary_name_verified_result_provenance =
        openapi_schema(&document, "PrimaryNameVerifiedResultProvenance");
    assert_eq!(
        required_fields(primary_name_verified_result_provenance),
        vec!["manifest_versions", "execution_trace_id"]
    );
    assert_eq!(
        primary_name_verified_result_provenance
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("manifest_versions")),
        Some(&json!({
            "type": "array",
            "items": {},
        }))
    );
    assert_eq!(
        primary_name_verified_result_provenance
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("execution_trace_id")),
        Some(&json!({
            "type": "string",
        }))
    );
    assert_eq!(
        primary_name_verified_result_provenance.get("additionalProperties"),
        Some(&json!(false))
    );

    let primary_name_claimed_result = openapi_schema(&document, "PrimaryNameClaimedResult");
    let primary_name_claimed_variants = primary_name_claimed_result
        .get("oneOf")
        .and_then(Value::as_array)
        .expect("PrimaryNameClaimedResult must define oneOf variants");
    assert!(primary_name_claimed_variants.iter().any(|variant| {
        variant
            == &json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "success",
                    },
                    "provenance": {
                        "$ref": "#/components/schemas/JsonObject",
                    },
                },
                "additionalProperties": false,
            })
    }));
    assert!(primary_name_claimed_variants.iter().any(|variant| {
        variant
            == &json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "not_found",
                    },
                    "provenance": {
                        "$ref": "#/components/schemas/JsonObject",
                    },
                },
                "additionalProperties": false,
            })
    }));
    assert!(primary_name_claimed_variants.iter().any(|variant| {
        variant
            == &json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "unsupported",
                    },
                    "provenance": {
                        "$ref": "#/components/schemas/JsonObject",
                    },
                },
                "additionalProperties": false,
            })
    }));
    assert!(primary_name_claimed_variants.iter().any(|variant| {
        variant
            == &json!({
                "type": "object",
                "required": ["status", "raw_claim_name", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "invalid_name",
                    },
                    "raw_claim_name": {
                        "type": "string",
                    },
                    "provenance": {
                        "$ref": "#/components/schemas/JsonObject",
                    },
                },
                "additionalProperties": false,
            })
    }));
    assert!(primary_name_claimed_variants.iter().all(|variant| {
        !variant
            .get("properties")
            .and_then(Value::as_object)
            .is_some_and(|properties| properties.contains_key("name"))
    }));

    let coverage = openapi_schema(&document, "CoverageResponse");
    assert_eq!(
        required_fields(coverage),
        vec![
            "status",
            "exhaustiveness",
            "source_classes_considered",
            "enumeration_basis",
            "unsupported_reason",
        ]
    );
}

#[test]
fn openapi_document_matches_checked_in_artifact() {
    let artifact_path = format!(
        "{}/../../docs/api-v1.openapi.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let checked_in: Value = serde_json::from_str(
        &fs::read_to_string(&artifact_path).expect("checked-in OpenAPI artifact must exist"),
    )
    .expect("checked-in OpenAPI artifact must be valid JSON");
    let rendered: Value = serde_json::from_str(&render_openapi_document())
        .expect("rendered OpenAPI document must be valid JSON");
    assert_eq!(checked_in, rendered);
}
