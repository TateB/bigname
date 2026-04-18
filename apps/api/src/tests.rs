use std::{
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
    CanonicalityState, NameSurface, NormalizedEvent, RawBlock, Resource, SurfaceBinding,
    SurfaceBindingKind, TokenLineage, default_database_url,
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

    async fn insert_name_current_row(&self, row: bigname_storage::NameCurrentRow) -> Result<()> {
        bigname_storage::upsert_name_current_rows(&self.pool, &[row])
            .await
            .context("failed to upsert name_current row for API test")?;
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
                "address": "0x0000000000000000000000000000000000000abc"
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
        declared_state
            .get("record_inventory")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("unsupported")
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
async fn get_address_names_include_role_summary_is_explicitly_unsupported() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/addresses/0x0000000000000000000000000000000000000abc/names?include=role_summary")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("role summary request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "unsupported");
    assert_eq!(
        payload.error.message,
        "include=role_summary is not yet supported"
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
