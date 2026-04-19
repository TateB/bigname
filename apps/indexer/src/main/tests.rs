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
    NameSurface, Resource, SurfaceBinding, SurfaceBindingKind, TokenLineage, default_database_url,
    upsert_name_surfaces, upsert_resources, upsert_surface_bindings, upsert_token_lineages,
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

fn resolver_addr_changed_topic0() -> String {
    keccak256_hex(b"AddrChanged(bytes32,address)")
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

fn encode_resolver_version_changed_log_data(version: u64) -> String {
    hex_string(&abi_word_u64(version))
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

#[test]
fn ensure_manifest_root_ready_accepts_loaded_root() -> Result<()> {
    ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::Loaded))
}

#[test]
fn ensure_manifest_root_ready_accepts_empty_root() -> Result<()> {
    ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::Empty))
}

#[test]
fn ensure_manifest_root_ready_rejects_missing_root() {
    let error = ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::MissingRoot))
        .expect_err("missing root must fail");

    assert!(
        error
            .to_string()
            .contains("refusing to boot on stale stored manifest state")
    );
    assert!(
        error.to_string().contains("/tmp/manifests does not exist"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn ensure_manifest_root_ready_rejects_invalid_root() {
    let error = ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::InvalidRoot))
        .expect_err("invalid root must fail");

    assert!(
        error
            .to_string()
            .contains("refusing to boot on stale stored manifest state")
    );
    assert!(
        error
            .to_string()
            .contains("/tmp/manifests is not a directory"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn verify_stored_manifest_state_accepts_matching_active_manifest_count() -> Result<()> {
    let database = TestDatabase::new().await?;
    let admission_state = load_discovery_admission_state(database.pool()).await?;

    verify_stored_manifest_state(&synced_manifest_summary(0), &admission_state)?;

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn verify_stored_manifest_state_rejects_mismatched_active_manifest_count() -> Result<()> {
    let database = TestDatabase::new().await?;
    let admission_state = load_discovery_admission_state(database.pool()).await?;

    let error = verify_stored_manifest_state(&synced_manifest_summary(1), &admission_state)
        .expect_err("mismatched counts must fail");

    assert!(
        error.to_string().contains(
            "stored active manifest count 0 does not match the synced active manifest count 1"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn load_watched_contract_summary_rebuilds_counts_from_storage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(1);
    let contract_contract_instance_id = Uuid::from_u128(2);
    let discovered_contract_instance_id = Uuid::from_u128(3);
    let implementation_contract_instance_id = Uuid::from_u128(4);
    let shadow_contract_instance_id = Uuid::from_u128(5);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES
                (1, 'ethereum-mainnet', 'active'),
                (2, 'base-mainnet', 'shadow')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for watched summary test")?;
    insert_contract_instance(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000001",
        Some(1),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        1,
        root_contract_instance_id,
        "0x0000000000000000000000000000000000000001",
    )
    .await?;

    insert_contract_instance(
        database.pool(),
        contract_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        contract_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
        Some(1),
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        discovered_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        discovered_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000cc",
        Some(1),
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000dd",
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registry",
        contract_contract_instance_id,
        "0x00000000000000000000000000000000000000aa",
        "erc1967",
        Some(implementation_contract_instance_id),
        Some("0x00000000000000000000000000000000000000dd"),
    )
    .await?;
    insert_active_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "subregistry",
        contract_contract_instance_id,
        discovered_contract_instance_id,
        Some(1),
    )
    .await?;
    insert_active_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "proxy_implementation",
        contract_contract_instance_id,
        implementation_contract_instance_id,
        Some(1),
    )
    .await?;

    insert_contract_instance(
        database.pool(),
        shadow_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        shadow_contract_instance_id,
        "base-mainnet",
        "0x00000000000000000000000000000000000000bb",
        Some(2),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        2,
        "registry",
        shadow_contract_instance_id,
        "0x00000000000000000000000000000000000000bb",
        "none",
        None,
        None,
    )
    .await?;

    let summary = load_watched_contract_summary(database.pool()).await?;
    assert_eq!(summary.unique_contract_count, 4);
    assert_eq!(summary.source_entry_count, 4);
    assert_eq!(summary.manifest_root_count, 1);
    assert_eq!(summary.manifest_contract_count, 1);
    assert_eq!(summary.discovery_edge_count, 2);
    assert_eq!(summary.chains.len(), 1);
    assert_eq!(summary.chains[0].chain, "ethereum-mainnet");
    assert_eq!(summary.chains[0].unique_contract_count, 4);
    assert_eq!(summary.chains[0].manifest_root_count, 1);
    assert_eq!(summary.chains[0].manifest_contract_count, 1);
    assert_eq!(summary.chains[0].discovery_edge_count, 2);

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(watched_plan.len(), 1);
    assert_eq!(
        watched_plan[0].addresses,
        vec![
            "0x0000000000000000000000000000000000000001".to_owned(),
            "0x00000000000000000000000000000000000000aa".to_owned(),
            "0x00000000000000000000000000000000000000cc".to_owned(),
            "0x00000000000000000000000000000000000000dd".to_owned(),
        ]
    );
    assert_eq!(watched_plan[0].manifest_root_entry_count, 1);
    assert_eq!(watched_plan[0].manifest_contract_entry_count, 1);
    assert_eq!(watched_plan[0].discovery_edge_entry_count, 2);

    database.cleanup().await?;
    Ok(())
}

#[test]
fn watched_chain_plan_state_counts_chains_addresses_and_entries() {
    let state = watched_chain_plan_state(&[
        WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
        },
        WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec!["0x00000000000000000000000000000000000000bb".to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        },
    ]);

    assert_eq!(
        state,
        WatchedChainPlanState {
            chain_count: 2,
            address_count: 3,
            entry_count: 4,
        }
    );
}

#[test]
fn intake_runtime_state_counts_checkpoint_modes() {
    let state = intake_runtime_state(&[
        IntakeChainTask {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
            checkpoint: ChainCheckpoint {
                chain_id: "ethereum-mainnet".to_owned(),
                canonical_block_hash: Some(
                    "0x00000000000000000000000000000000000000000000000000000000000000aa".to_owned(),
                ),
                canonical_block_number: Some(42),
                safe_block_hash: Some(
                    "0x0000000000000000000000000000000000000000000000000000000000000099".to_owned(),
                ),
                safe_block_number: Some(41),
                finalized_block_hash: None,
                finalized_block_number: None,
            },
        },
        IntakeChainTask {
            chain: "base-mainnet".to_owned(),
            addresses: vec!["0x00000000000000000000000000000000000000bb".to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
            checkpoint: ChainCheckpoint {
                chain_id: "base-mainnet".to_owned(),
                canonical_block_hash: None,
                canonical_block_number: None,
                safe_block_hash: None,
                safe_block_number: None,
                finalized_block_hash: None,
                finalized_block_number: None,
            },
        },
    ]);

    assert_eq!(
        state,
        IntakeRuntimeState {
            chain_count: 2,
            address_count: 3,
            entry_count: 4,
            cold_start_chain_count: 1,
            resumable_chain_count: 1,
            safe_checkpoint_chain_count: 1,
            finalized_checkpoint_chain_count: 0,
        }
    );
}

#[tokio::test]
async fn sync_intake_chain_tasks_creates_missing_checkpoint_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(11);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for intake task sync test")?;
    insert_contract_instance(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000001",
        Some(1),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        1,
        root_contract_instance_id,
        "0x0000000000000000000000000000000000000001",
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].chain, "ethereum-mainnet");
    assert_eq!(
        tasks[0].addresses,
        vec!["0x0000000000000000000000000000000000000001".to_owned()]
    );
    assert_eq!(checkpoint_mode(&tasks[0].checkpoint), "cold_start");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_checkpoints")
            .fetch_one(database.pool())
            .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn sync_intake_chain_tasks_preserves_manifest_contract_implementation_addresses() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let contract_address = "0x00000000000000000000000000000000000000aa";
    let implementation_address = "0x00000000000000000000000000000000000000bb";
    let contract_contract_instance_id = Uuid::from_u128(21);
    let implementation_contract_instance_id = Uuid::from_u128(22);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for implementation watch-plan test")?;
    insert_contract_instance(
        database.pool(),
        contract_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        contract_contract_instance_id,
        "ethereum-mainnet",
        contract_address,
        Some(1),
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        implementation_address,
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registry",
        contract_contract_instance_id,
        contract_address,
        "erc1967",
        Some(implementation_contract_instance_id),
        Some(implementation_address),
    )
    .await?;
    insert_active_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "proxy_implementation",
        contract_contract_instance_id,
        implementation_contract_instance_id,
        Some(1),
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;

    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].chain, "ethereum-mainnet");
    assert_eq!(
        tasks[0].addresses,
        vec![
            contract_address.to_owned(),
            implementation_address.to_owned()
        ]
    );
    assert_eq!(tasks[0].manifest_root_entry_count, 0);
    assert_eq!(tasks[0].manifest_contract_entry_count, 1);
    assert_eq!(tasks[0].discovery_edge_entry_count, 1);
    assert_eq!(checkpoint_mode(&tasks[0].checkpoint), "cold_start");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_checkpoints")
            .fetch_one(database.pool())
            .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_initializes_chain_from_provider_heads() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(31);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for cold start reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000001",
        Some(1),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        1,
        root_contract_instance_id,
        "0x0000000000000000000000000000000000000001",
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        42,
    );
    let safe_head = provider_block(
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        Some("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
        41,
    );
    let finalized_head = provider_block(
        "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        Some("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        40,
    );
    let (provider, server) = bundle_provider(vec![
        canonical_head.clone(),
        safe_head.clone(),
        finalized_head.clone(),
    ])
    .await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head,
            safe: Some(safe_head),
            finalized: Some(finalized_head),
        },
    )
    .await?
    .expect("cold start reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert!(outcome.canonical_head_changed);
    assert!(outcome.safe_head_changed);
    assert!(outcome.finalized_head_changed);
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(42));
    assert_eq!(next_task.checkpoint.safe_block_number, Some(41));
    assert_eq!(next_task.checkpoint.finalized_block_number, Some(40));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_blocks")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_transactions")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_receipts")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 41"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_number = 41"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 41"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_receipts WHERE block_number = 41"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_logs WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_number = 41"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'decoded_name' FROM normalized_events WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "wrapped.eth".to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_registrar_name_observation_events() -> Result<()> {
    let database = TestDatabase::new().await?;
    let registrar_contract_instance_id = Uuid::from_u128(32);
    let registrar_address = "0x00000000000000000000000000000000000000aa";

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
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
            VALUES (
                1,
                1,
                'ens',
                'ens_v1_registrar_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'uts46-v1',
                'manifests/ens/ens_v1_registrar_l1/v1.toml',
                '{}'::jsonb
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for registrar observation reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        registrar_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registrar_contract_instance_id,
        "ethereum-mainnet",
        registrar_address,
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registrar",
        registrar_contract_instance_id,
        registrar_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        42,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![rpc_registrar_name_registered_log_payload(
            &canonical_head,
            registrar_address,
            "registrar",
            canonical_head.block_timestamp_unix_secs + 31_536_000,
        )],
        block: canonical_head.clone(),
    }])
    .await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("registrar observation reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(42));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        6
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v1_registrar_l1".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'decoded_name' FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        "registrar.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'source_event' FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        "NameRegistered".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT raw_fact_ref->>'emitting_address' FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        registrar_address.to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_ensv1_reverse_claim_normalized_events() -> Result<()> {
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x341);
    let reverse_address = "0x00000000000000000000000000000000000000ad";
    let claimed_address = "0x1234567890abcdef1234567890abcdef12345678";

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
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
            VALUES (
                1,
                1,
                'ens',
                'ens_v1_reverse_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'uts46-v1',
                'manifests/ens/ens_v1_reverse_l1/v1.toml',
                '{}'::jsonb
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for ENSv1 reverse reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        reverse_address,
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xabababababababababababababababababababababababababababababababab",
        Some("0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"),
        63,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![rpc_reverse_claimed_log_payload(
            &canonical_head,
            reverse_address,
            claimed_address,
            0,
        )],
        block: canonical_head.clone(),
    }])
    .await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("ENSv1 reverse reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(63));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT event_kind FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ReverseChanged".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v1_reverse_l1".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT derivation_kind FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v1_reverse_claim".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'address' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        claimed_address.to_ascii_lowercase()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'namespace' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'reverse_name' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_name_for_address(claimed_address)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'reverse_node' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_node_for_address(claimed_address)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'source_family' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v1_reverse_l1".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'contract_role' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        REVERSE_REGISTRAR_ROLE.to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'contract_instance_id' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_contract_instance_id.to_string()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'emitting_address' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_address.to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_basenames_reverse_claim_normalized_events() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x346);
    let reverse_address = "0x79ea96012eea67a83431f1701b3dff7e37f9e282";
    let claimed_address = "0x1234567890abcdef1234567890abcdef12345678";

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
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
            VALUES (
                1,
                1,
                'basenames',
                'basenames_base_primary',
                'base-mainnet',
                'basenames_v1',
                'active',
                'uts46-v1',
                'manifests/basenames/basenames_base_primary/v1.toml',
                '{}'::jsonb
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for Basenames reverse reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        "base-mainnet",
        reverse_address,
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xbabababababababababababababababababababababababababababababababa",
        Some("0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"),
        63,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![rpc_reverse_claimed_log_payload(
            &canonical_head,
            reverse_address,
            claimed_address,
            0,
        )],
        block: canonical_head.clone(),
    }])
    .await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("Basenames reverse reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(63));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames_base_primary".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT namespace FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'namespace' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'reverse_namespace' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'source_family' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames_base_primary".to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_ensv1_primary_claim_source_observations() -> Result<()> {
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x343);
    let registry_contract_instance_id = Uuid::from_u128(0x344);
    let resolver_contract_instance_id = Uuid::from_u128(0x345);
    let reverse_address = "0x00000000000000000000000000000000000000ad";
    let registry_address = "0x00000000000000000000000000000000000000ae";
    let resolver_address = "0x00000000000000000000000000000000000000af";
    let claimed_address = "0x1234567890abcdef1234567890abcdef12345678";
    let reverse_node = reverse_node_for_address(claimed_address);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
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
            VALUES
                (
                    1,
                    1,
                    'ens',
                    'ens_v1_reverse_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'uts46-v1',
                    'manifests/ens/ens_v1_reverse_l1/v1.toml',
                    '{}'::jsonb
                ),
                (
                    2,
                    1,
                    'ens',
                    'ens_v1_registry_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'uts46-v1',
                    'manifests/ens/ens_v1_registry_l1/v1.toml',
                    '{}'::jsonb
                ),
                (
                    3,
                    1,
                    'ens',
                    'ens_v1_resolver_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'uts46-v1',
                    'manifests/ens/ens_v1_resolver_l1/v1.toml',
                    '{}'::jsonb
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for primary-claim source reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        reverse_address,
        Some(1),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        registry_address,
        Some(2),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        resolver_contract_instance_id,
        "ethereum-mainnet",
        resolver_address,
        Some(3),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        2,
        "registry",
        registry_contract_instance_id,
        registry_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        3,
        "resolver",
        resolver_contract_instance_id,
        resolver_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacac",
        Some("0xbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbd"),
        65,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![
            rpc_reverse_claimed_log_payload(&canonical_head, reverse_address, claimed_address, 0),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &reverse_node,
                resolver_address,
                1,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                resolver_address,
                &reverse_node,
                "alice.eth",
                2,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                resolver_address,
                &reverse_node,
                7,
                3,
            ),
        ],
        block: canonical_head.clone(),
    }])
    .await?;

    reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("primary-claim source reconciliation must update task state");

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE logical_name_id IS NULL AND resource_id IS NULL AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'raw_name' FROM normalized_events WHERE logical_name_id IS NULL AND event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "alice.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->>'address' FROM normalized_events WHERE logical_name_id IS NULL AND event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        claimed_address.to_ascii_lowercase()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->>'reverse_node' FROM normalized_events WHERE logical_name_id IS NULL AND event_kind = 'RecordVersionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_node
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->'claim_provenance'->>'contract_role' FROM normalized_events WHERE logical_name_id IS NULL AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        REVERSE_REGISTRAR_ROLE.to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_basenames_primary_claim_source_observations()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x347);
    let registry_contract_instance_id = Uuid::from_u128(0x348);
    let resolver_contract_instance_id = Uuid::from_u128(0x349);
    let reverse_address = "0x79ea96012eea67a83431f1701b3dff7e37f9e282";
    let registry_address = "0xb94704422c2a1e396835a571837aa5ae53285a95";
    let resolver_address = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd";
    let claimed_address = "0x1234567890abcdef1234567890abcdef12345678";
    let reverse_node = reverse_node_for_address(claimed_address);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
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
            VALUES
                (
                    1,
                    1,
                    'basenames',
                    'basenames_base_primary',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_primary/v1.toml',
                    '{}'::jsonb
                ),
                (
                    2,
                    1,
                    'basenames',
                    'basenames_base_registry',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_registry/v1.toml',
                    '{}'::jsonb
                ),
                (
                    3,
                    1,
                    'basenames',
                    'basenames_base_resolver',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_resolver/v1.toml',
                    '{}'::jsonb
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context(
        "failed to insert manifest_versions for Basenames primary-claim source reconciliation test",
    )?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        "base-mainnet",
        reverse_address,
        Some(1),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "base-mainnet",
        registry_address,
        Some(2),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        resolver_contract_instance_id,
        "base-mainnet",
        resolver_address,
        Some(3),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        2,
        "registry",
        registry_contract_instance_id,
        registry_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        3,
        "resolver",
        resolver_contract_instance_id,
        resolver_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadad",
        Some("0xbebebebebebebebebebebebebebebebebebebebebebebebebebebebebebebebe"),
        65,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![
            rpc_reverse_claimed_log_payload(&canonical_head, reverse_address, claimed_address, 0),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &reverse_node,
                resolver_address,
                1,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                resolver_address,
                &reverse_node,
                "alice.base.eth",
                2,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                resolver_address,
                &reverse_node,
                7,
                3,
            ),
        ],
        block: canonical_head.clone(),
    }])
    .await?;

    reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("Basenames primary-claim source reconciliation must update task state");

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND resource_id IS NULL AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'raw_name' FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "alice.base.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->>'address' FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        claimed_address.to_ascii_lowercase()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->>'reverse_node' FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND event_kind = 'RecordVersionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_node
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->'claim_provenance'->>'source_family' FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames_base_primary".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'primary_claim_source'->'claim_provenance'->>'contract_role' FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        REVERSE_REGISTRAR_ROLE.to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_unwrapped_ensv1_authority_identity_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let registrar_contract_instance_id = Uuid::from_u128(33);
    let registry_contract_instance_id = Uuid::from_u128(34);
    let resolver_contract_instance_id = Uuid::from_u128(35);
    let registrar_address = "0x00000000000000000000000000000000000000ab";
    let registry_address = "0x00000000000000000000000000000000000000ac";
    let resolver_address = "0x00000000000000000000000000000000000000cc";

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
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
            VALUES (
                1,
                1,
                'ens',
                'ens_v1_registrar_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'uts46-v1',
                'manifests/ens/ens_v1_registrar_l1/v1.toml',
                '{}'::jsonb
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for unwrapped authority reconciliation test")?;
    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
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
            VALUES (
                2,
                1,
                'ens',
                'ens_v1_registry_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'uts46-v1',
                'manifests/ens/ens_v1_registry_l1/v1.toml',
                '{}'::jsonb
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context(
        "failed to insert registry manifest_versions for unwrapped authority reconciliation test",
    )?;
    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
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
            VALUES (
                3,
                1,
                'ens',
                'ens_v1_resolver_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'uts46-v1',
                'manifests/ens/ens_v1_resolver_l1/v1.toml',
                '{}'::jsonb
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context(
        "failed to insert resolver manifest_versions for unwrapped authority reconciliation test",
    )?;
    insert_contract_instance(
        database.pool(),
        registrar_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registrar_contract_instance_id,
        "ethereum-mainnet",
        registrar_address,
        Some(1),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        registry_address,
        Some(2),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        resolver_contract_instance_id,
        "ethereum-mainnet",
        resolver_address,
        Some(3),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registrar",
        registrar_contract_instance_id,
        registrar_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        2,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        3,
        "resolver",
        resolver_contract_instance_id,
        resolver_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        Some("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
        52,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![
            rpc_registrar_name_registered_log_payload(
                &canonical_head,
                registrar_address,
                "alice",
                canonical_head.block_timestamp_unix_secs + 31_536_000,
            ),
            rpc_registry_new_resolver_log_payload(
                &canonical_head,
                registry_address,
                "alice",
                resolver_address,
                1,
            ),
            rpc_resolver_text_changed_log_payload(
                &canonical_head,
                resolver_address,
                "alice",
                "com.twitter",
                2,
            ),
            rpc_resolver_addr_changed_log_payload(
                &canonical_head,
                resolver_address,
                "alice",
                "0x00000000000000000000000000000000000000aa",
                3,
            ),
            rpc_resolver_version_changed_log_payload(
                &canonical_head,
                resolver_address,
                "alice",
                7,
                4,
            ),
        ],
        block: canonical_head.clone(),
    }])
    .await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("unwrapped authority reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(52));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM token_lineages")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resources")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM name_surfaces")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM surface_bindings")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT logical_name_id FROM name_surfaces LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "ens:alice.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT binding_kind FROM surface_bindings LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "declared_registry_path".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RegistrationGranted'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'AuthorityEpochChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'SurfaceBound'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordChanged' AND canonicality_state = 'canonical'::canonicality_state"
        )
        .bind(&canonical_head.block_hash)
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordVersionChanged' AND canonicality_state = 'canonical'::canonicality_state"
        )
        .bind(&canonical_head.block_hash)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(after_state->>'record_key' ORDER BY after_state->>'record_key') FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordChanged'"
        )
        .bind(&canonical_head.block_hash)
        .fetch_one(database.pool())
        .await?,
        vec!["addr:60".to_owned(), "text".to_owned()]
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'record_version' FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordVersionChanged'"
        )
        .bind(&canonical_head.block_hash)
        .fetch_one(database.pool())
        .await?,
        "7".to_owned()
    );
    let resolver_event_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM normalized_events WHERE event_kind = 'ResolverChanged'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        resolver_event_resource_id,
        sqlx::query_scalar::<_, Uuid>("SELECT resource_id FROM resources LIMIT 1")
            .fetch_one(database.pool())
            .await?
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1"
        )
        .bind(resolver_event_resource_id)
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'scope'->>'kind' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND after_state->'scope'->>'kind' = 'resource' LIMIT 1"
        )
        .fetch_one(database.pool())
        .await?,
        "resource".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'scope'->>'kind' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND after_state->'scope'->>'kind' = 'resolver' LIMIT 1"
        )
        .fetch_one(database.pool())
        .await?,
        "resolver".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT logical_name_id FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens:alice.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v1_registry_l1".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'resolver' FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "0x00000000000000000000000000000000000000cc".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_basenames_unwrapped_authority_identity_rows()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let registrar_contract_instance_id = Uuid::from_u128(0x351);
    let registry_contract_instance_id = Uuid::from_u128(0x352);
    let resolver_contract_instance_id = Uuid::from_u128(0x353);
    let registrar_address = "0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a";
    let registry_address = "0xb94704422c2a1e396835a571837aa5ae53285a95";
    let resolver_address = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd";
    let alice_namehash = namehash_for_dns_name(&dns_encoded_base_eth_name("alice"));

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
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
            VALUES
                (
                    1,
                    1,
                    'basenames',
                    'basenames_base_registrar',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_registrar/v1.toml',
                    '{}'::jsonb
                ),
                (
                    2,
                    1,
                    'basenames',
                    'basenames_base_registry',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_registry/v1.toml',
                    '{}'::jsonb
                ),
                (
                    3,
                    1,
                    'basenames',
                    'basenames_base_resolver',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_resolver/v1.toml',
                    '{}'::jsonb
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for Basenames authority reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        registrar_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "base-mainnet",
        "root",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registrar_contract_instance_id,
        "base-mainnet",
        registrar_address,
        Some(1),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "base-mainnet",
        registry_address,
        Some(2),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        resolver_contract_instance_id,
        "base-mainnet",
        resolver_address,
        Some(3),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registrar",
        registrar_contract_instance_id,
        registrar_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        2,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        3,
        "resolver",
        resolver_contract_instance_id,
        resolver_address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xedededededededededededededededededededededededededededededededed",
        Some("0xfefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefe"),
        52,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![
            rpc_registrar_name_registered_log_payload(
                &canonical_head,
                registrar_address,
                "alice",
                canonical_head.block_timestamp_unix_secs + 31_536_000,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &alice_namehash,
                resolver_address,
                1,
            ),
            rpc_resolver_text_changed_log_payload_for_namehash(
                &canonical_head,
                resolver_address,
                &alice_namehash,
                "com.twitter",
                2,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                resolver_address,
                &alice_namehash,
                7,
                3,
            ),
        ],
        block: canonical_head.clone(),
    }])
    .await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("Basenames authority reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(52));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM token_lineages")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resources")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM name_surfaces")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM surface_bindings")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT logical_name_id FROM name_surfaces LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "basenames:alice.base.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT canonical_display_name FROM name_surfaces LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "alice.base.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT namespace FROM name_surfaces LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "basenames".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT binding_kind FROM surface_bindings LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "declared_registry_path".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames_base_registry".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT namespace FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames".to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn build_manifest_runtime_state_loads_checked_in_repository_seed() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../manifests");
    let manifest_repository = load_manifest_repository(&manifests_root)?;

    let runtime_state = build_manifest_runtime_state(database.pool(), &manifest_repository).await?;

    assert_eq!(
        runtime_state.manifest_summary.status,
        ManifestLoadStatus::Loaded
    );
    assert_eq!(runtime_state.manifest_summary.namespace_count, 2);
    assert_eq!(runtime_state.manifest_summary.source_family_count, 10);
    assert_eq!(runtime_state.manifest_summary.manifest_count, 10);
    assert_eq!(
        runtime_state.sync_summary.status,
        ManifestSyncStatus::Synced
    );
    assert_eq!(runtime_state.sync_summary.synced_manifest_count, 10);
    assert_eq!(runtime_state.sync_summary.active_manifest_count, 8);
    assert_eq!(runtime_state.sync_summary.root_count, 3);
    assert_eq!(runtime_state.sync_summary.contract_count, 10);
    assert_eq!(runtime_state.sync_summary.capability_count, 6);
    assert_eq!(runtime_state.sync_summary.discovery_rule_count, 2);
    assert_eq!(runtime_state.discovery_admission.active_manifest_count, 8);
    assert_eq!(runtime_state.discovery_admission.active_root_count, 3);
    assert_eq!(runtime_state.discovery_admission.active_contract_count, 8);
    assert_eq!(runtime_state.discovery_admission.active_rule_count, 2);
    assert_eq!(
        runtime_state
            .manifest_normalized_event_summary
            .total_synced_count,
        12
    );
    assert_eq!(
        runtime_state.watched_contract_summary.unique_contract_count,
        8
    );
    assert_eq!(
        runtime_state.watched_contract_summary.source_entry_count,
        11
    );
    assert_eq!(
        runtime_state.watched_contract_summary.manifest_root_count,
        3
    );
    assert_eq!(
        runtime_state
            .watched_contract_summary
            .manifest_contract_count,
        8
    );
    assert_eq!(
        runtime_state.watched_contract_summary.discovery_edge_count,
        0
    );
    assert_eq!(
        runtime_state.watched_chain_plan,
        vec![
            WatchedChainPlan {
                chain: "base-mainnet".to_owned(),
                addresses: vec![
                    "0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a".to_owned(),
                    "0x79ea96012eea67a83431f1701b3dff7e37f9e282".to_owned(),
                    "0xb94704422c2a1e396835a571837aa5ae53285a95".to_owned(),
                    "0xc6d566a56a1aff6508b41f6c90ff131615583bcd".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 4,
                discovery_edge_entry_count: 0,
            },
            WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec![
                    "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
                    "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85".to_owned(),
                    "0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb".to_owned(),
                    "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31".to_owned(),
                ],
                manifest_root_entry_count: 2,
                manifest_contract_entry_count: 4,
                discovery_edge_entry_count: 0,
            }
        ]
    );

    let stored_admission = load_discovery_admission_state(database.pool()).await?;
    assert_eq!(stored_admission.active_manifest_count, 8);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn refresh_watched_chain_plan_detects_storage_changes() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(41);
    let discovered_contract_instance_id = Uuid::from_u128(42);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for watched plan refresh test")?;
    insert_contract_instance(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000001",
        Some(1),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        1,
        root_contract_instance_id,
        "0x0000000000000000000000000000000000000001",
    )
    .await?;

    let initial_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        refresh_watched_chain_plan(database.pool(), &initial_plan).await?,
        None
    );

    insert_contract_instance(
        database.pool(),
        discovered_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        discovered_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000cc",
        Some(1),
    )
    .await?;
    insert_active_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "subregistry",
        root_contract_instance_id,
        discovered_contract_instance_id,
        Some(1),
    )
    .await?;

    let refreshed_plan = refresh_watched_chain_plan(database.pool(), &initial_plan)
        .await?
        .expect("watch plan change must be detected");
    assert_eq!(refreshed_plan.len(), 1);
    assert_eq!(refreshed_plan[0].chain, "ethereum-mainnet");
    assert_eq!(
        refreshed_plan[0].addresses,
        vec![
            "0x0000000000000000000000000000000000000001".to_owned(),
            "0x00000000000000000000000000000000000000cc".to_owned(),
        ]
    );
    assert_eq!(refreshed_plan[0].manifest_root_entry_count, 1);
    assert_eq!(refreshed_plan[0].manifest_contract_entry_count, 0);
    assert_eq!(refreshed_plan[0].discovery_edge_entry_count, 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn refresh_intake_chain_tasks_detects_checkpoint_updates() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(51);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for checkpoint refresh test")?;
    insert_contract_instance(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000001",
        Some(1),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        1,
        root_contract_instance_id,
        "0x0000000000000000000000000000000000000001",
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let initial_tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    assert_eq!(
        refresh_intake_chain_tasks(database.pool(), &initial_tasks, &watched_plan).await?,
        None
    );

    sqlx::query(
            r#"
            UPDATE chain_checkpoints
            SET
                canonical_block_hash = '0x00000000000000000000000000000000000000000000000000000000000000aa',
                canonical_block_number = 42,
                safe_block_hash = '0x0000000000000000000000000000000000000000000000000000000000000099',
                safe_block_number = 41,
                finalized_block_hash = '0x0000000000000000000000000000000000000000000000000000000000000088',
                finalized_block_number = 40
            WHERE chain_id = 'ethereum-mainnet'
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to update chain_checkpoints for checkpoint refresh test")?;

    let refreshed_tasks =
        refresh_intake_chain_tasks(database.pool(), &initial_tasks, &watched_plan)
            .await?
            .expect("checkpoint change must be detected");
    assert_eq!(refreshed_tasks.len(), 1);
    assert_eq!(
        refreshed_tasks[0].checkpoint.canonical_block_number,
        Some(42)
    );
    assert_eq!(checkpoint_mode(&refreshed_tasks[0].checkpoint), "resume");
    assert_eq!(
        intake_runtime_state(&refreshed_tasks),
        IntakeRuntimeState {
            chain_count: 1,
            address_count: 1,
            entry_count: 1,
            cold_start_chain_count: 0,
            resumable_chain_count: 1,
            safe_checkpoint_chain_count: 1,
            finalized_checkpoint_chain_count: 1,
        }
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn build_manifest_runtime_state_reloads_repository_changes_without_restart() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests.write_manifest(&manifest_contents(
        "0x0000000000000000000000000000000000000001",
        "shadow",
    ))?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    assert_eq!(
        initial_repository.summary().status,
        ManifestLoadStatus::Loaded
    );
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;
    assert_eq!(initial_state.watched_chain_plan.len(), 1);
    assert_eq!(
        initial_state.watched_chain_plan[0].addresses,
        vec![
            "0x0000000000000000000000000000000000000001".to_owned(),
            "0x00000000000000000000000000000000000000aa".to_owned(),
        ]
    );
    assert_eq!(
        initial_state
            .manifest_normalized_event_summary
            .total_inserted_count,
        2
    );

    fs::write(
        &manifest_path,
        manifest_contents("0x0000000000000000000000000000000000000002", "supported"),
    )
    .with_context(|| format!("failed to rewrite {}", manifest_path.display()))?;

    let refreshed_repository = load_manifest_repository(&manifests.path)?;
    let refreshed_state =
        build_manifest_runtime_state(database.pool(), &refreshed_repository).await?;
    assert_eq!(
        refreshed_state.watched_chain_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000002".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
        }]
    );
    assert_eq!(
        refreshed_state
            .manifest_normalized_event_summary
            .total_inserted_count,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE event_kind = 'CapabilityChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn refresh_watched_chain_plan_reuses_contract_instance_ids_across_inactive_gaps() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests.write_manifest(&manifest_contents(
        "0x0000000000000000000000000000000000000001",
        "shadow",
    ))?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;
    let initial_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;

    fs::remove_file(&manifest_path)
        .with_context(|| format!("failed to remove {}", manifest_path.display()))?;

    let empty_repository = load_manifest_repository(&manifests.path)?;
    let empty_state = build_manifest_runtime_state(database.pool(), &empty_repository).await?;
    assert!(empty_state.watched_chain_plan.is_empty());
    assert_eq!(
        refresh_watched_chain_plan(database.pool(), &initial_state.watched_chain_plan).await?,
        Some(Vec::new())
    );

    fs::write(
        &manifest_path,
        manifest_contents("0x0000000000000000000000000000000000000001", "shadow"),
    )
    .with_context(|| format!("failed to rewrite {}", manifest_path.display()))?;

    let restored_repository = load_manifest_repository(&manifests.path)?;
    let restored_state =
        build_manifest_runtime_state(database.pool(), &restored_repository).await?;
    let restored_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;

    assert_eq!(initial_contract_instance_id, restored_contract_instance_id);
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1"
            )
            .bind(initial_contract_instance_id)
            .fetch_one(database.pool())
            .await?,
            2
        );
    assert_eq!(
        refresh_watched_chain_plan(database.pool(), &empty_state.watched_chain_plan).await?,
        Some(restored_state.watched_chain_plan.clone())
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn refresh_watched_chain_plan_tracks_proxy_implementation_churn() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests.write_manifest(&manifest_contents_with_contract(
        "0x0000000000000000000000000000000000000001",
        "supported",
        "0x00000000000000000000000000000000000000aa",
        "erc1967",
        Some("0x00000000000000000000000000000000000000dd"),
    ))?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;
    let proxy_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;
    let first_implementation_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000dd",
    )
    .await?;

    assert_eq!(
        initial_state.watched_chain_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
                "0x00000000000000000000000000000000000000dd".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );

    fs::write(
        &manifest_path,
        manifest_contents_with_contract(
            "0x0000000000000000000000000000000000000001",
            "supported",
            "0x00000000000000000000000000000000000000aa",
            "erc1967",
            Some("0x00000000000000000000000000000000000000ee"),
        ),
    )
    .with_context(|| format!("failed to rewrite {}", manifest_path.display()))?;

    let refreshed_repository = load_manifest_repository(&manifests.path)?;
    let refreshed_state =
        build_manifest_runtime_state(database.pool(), &refreshed_repository).await?;
    let refreshed_plan =
        refresh_watched_chain_plan(database.pool(), &initial_state.watched_chain_plan)
            .await?
            .expect("proxy implementation churn must refresh the watched plan");
    let second_implementation_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000ee",
    )
    .await?;
    let intake_tasks =
        sync_intake_chain_tasks(database.pool(), &refreshed_state.watched_chain_plan).await?;

    assert_eq!(
        proxy_contract_instance_id,
        load_single_contract_instance_for_address(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
        )
        .await?
    );
    assert_ne!(
        first_implementation_contract_instance_id,
        second_implementation_contract_instance_id
    );
    assert_eq!(refreshed_plan, refreshed_state.watched_chain_plan);
    assert_eq!(
        refreshed_state.watched_chain_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
                "0x00000000000000000000000000000000000000ee".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );
    assert_eq!(
        intake_tasks[0].addresses,
        refreshed_state.watched_chain_plan[0].addresses
    );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'proxy_implementation' AND deactivated_at IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
            )
            .bind(first_implementation_contract_instance_id)
            .fetch_one(database.pool())
            .await?,
            0
        );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn watched_plan_excludes_successor_migration_edges_from_address_expansion() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests.write_manifest(&manifest_contents_with_contract(
        "0x0000000000000000000000000000000000000001",
        "supported",
        "0x00000000000000000000000000000000000000aa",
        "erc1967",
        Some("0x00000000000000000000000000000000000000dd"),
    ))?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;

    fs::write(
        &manifest_path,
        manifest_contents_with_contract(
            "0x0000000000000000000000000000000000000001",
            "supported",
            "0x00000000000000000000000000000000000000bb",
            "erc1967",
            Some("0x00000000000000000000000000000000000000dd"),
        ),
    )
    .with_context(|| format!("failed to rewrite {}", manifest_path.display()))?;

    let refreshed_repository = load_manifest_repository(&manifests.path)?;
    let refreshed_state =
        build_manifest_runtime_state(database.pool(), &refreshed_repository).await?;
    let refreshed_plan =
        refresh_watched_chain_plan(database.pool(), &initial_state.watched_chain_plan)
            .await?
            .expect("successor address rotation must refresh the watched plan");
    let intake_tasks =
        sync_intake_chain_tasks(database.pool(), &refreshed_state.watched_chain_plan).await?;

    assert_eq!(refreshed_plan, refreshed_state.watched_chain_plan);
    assert_eq!(
        refreshed_state.watched_chain_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000bb".to_owned(),
                "0x00000000000000000000000000000000000000dd".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );
    assert!(
        !refreshed_state.watched_chain_plan[0]
            .addresses
            .contains(&"0x00000000000000000000000000000000000000aa".to_owned())
    );
    assert_eq!(
        intake_tasks[0].addresses,
        refreshed_state.watched_chain_plan[0].addresses
    );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'migration' AND deactivated_at IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn build_manifest_runtime_state_accepts_empty_root_and_clears_watch_plan() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests.write_manifest(&manifest_contents(
        "0x0000000000000000000000000000000000000001",
        "shadow",
    ))?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;
    assert_eq!(
        initial_state.manifest_summary.status,
        ManifestLoadStatus::Loaded
    );
    assert_eq!(initial_state.watched_chain_plan.len(), 1);

    fs::remove_file(&manifest_path)
        .with_context(|| format!("failed to remove {}", manifest_path.display()))?;

    let empty_repository = load_manifest_repository(&manifests.path)?;
    assert_eq!(empty_repository.summary().status, ManifestLoadStatus::Empty);
    let empty_state = build_manifest_runtime_state(database.pool(), &empty_repository).await?;
    assert_eq!(
        empty_state.manifest_summary.status,
        ManifestLoadStatus::Empty
    );
    assert!(empty_state.watched_chain_plan.is_empty());
    assert_eq!(empty_state.discovery_admission.active_manifest_count, 0);
    assert_eq!(empty_state.watched_contract_summary.source_entry_count, 0);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_code_hashes_for_new_watched_addresses() -> Result<()> {
    let database = TestDatabase::new().await?;
    let first_root_contract_instance_id = Uuid::from_u128(61);
    let second_contract_instance_id = Uuid::from_u128(62);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for code-hash backfill test")?;
    insert_contract_instance(
        database.pool(),
        first_root_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        first_root_contract_instance_id,
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000001",
        Some(1),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        1,
        first_root_contract_instance_id,
        "0x0000000000000000000000000000000000000001",
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let mut tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        42,
    );
    let (provider, server) = bundle_provider(vec![canonical_head.clone()]).await?;

    let (next_task, initial_outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("initial code-hash reconciliation must update task state");
    assert_eq!(
        initial_outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    tasks[0] = next_task;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        1
    );

    insert_contract_instance(
        database.pool(),
        second_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        second_contract_instance_id,
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000002",
        Some(1),
    )
    .await?;
    insert_active_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "subregistry",
        first_root_contract_instance_id,
        second_contract_instance_id,
        Some(1),
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let unchanged = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head,
            safe: None,
            finalized: None,
        },
    )
    .await?;
    assert!(
        unchanged.is_none(),
        "unchanged heads should not report a task transition"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        2
    );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT code_byte_length FROM raw_code_hashes WHERE contract_address = '0x0000000000000000000000000000000000000002'"
            )
            .fetch_one(database.pool())
            .await?,
            0
        );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn storage_discovery_refresh_adds_ensv1_address_without_manifest_reload_and_next_poll_backfills_code_hash()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests
        .write_manifest_for_source_family("ens_v1_registry_l1", &ens_v1_manifest_contents())?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;
    let initial_manifest_summary = initial_state.manifest_summary.clone();
    let initial_sync_summary = initial_state.sync_summary.clone();
    let initial_discovery_admission = initial_state.discovery_admission.clone();
    let initial_manifest_event_summary = initial_state.manifest_normalized_event_summary.clone();
    let initial_tasks =
        sync_intake_chain_tasks(database.pool(), &initial_state.watched_chain_plan).await?;

    assert_eq!(initial_state.watched_chain_plan.len(), 1);
    assert_eq!(initial_tasks.len(), 1);
    assert_eq!(initial_tasks[0].addresses.len(), 1);

    let canonical_head = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        42,
    );
    let (provider, server) = bundle_provider(vec![canonical_head.clone()]).await?;

    let (_next_task, initial_outcome) = reconcile_fetched_heads(
        database.pool(),
        &initial_tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("initial ENSv1 registry poll must update task state");
    assert_eq!(
        initial_outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        1
    );

    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        &canonical_head,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x0000000000000000000000000000000000000002",
        CanonicalityState::Canonical,
    )
    .await?;

    let (refreshed_state, refreshed_tasks) =
        refresh_runtime_state_from_storage_discovery(database.pool(), &initial_state)
            .await?
            .expect("stored ENSv1 discovery must refresh the watched plan without manifest reload");

    assert_eq!(refreshed_state.manifest_summary, initial_manifest_summary);
    assert_eq!(refreshed_state.sync_summary, initial_sync_summary);
    assert_eq!(
        refreshed_state.discovery_admission,
        initial_discovery_admission
    );
    assert_eq!(
        refreshed_state.manifest_normalized_event_summary,
        initial_manifest_event_summary
    );
    assert_eq!(
        fs::read_to_string(&manifest_path)
            .with_context(|| format!("failed to read {}", manifest_path.display()))?,
        ens_v1_manifest_contents()
    );
    assert_eq!(refreshed_state.watched_chain_plan.len(), 1);
    assert_eq!(refreshed_tasks.len(), 1);
    assert_eq!(
        refreshed_state.watched_chain_plan[0].chain,
        "ethereum-mainnet"
    );
    assert_eq!(refreshed_state.watched_chain_plan[0].addresses.len(), 2);
    assert!(
        refreshed_state.watched_chain_plan[0]
            .addresses
            .contains(&"0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned())
    );
    assert!(
        refreshed_state.watched_chain_plan[0]
            .addresses
            .contains(&"0x0000000000000000000000000000000000000002".to_owned())
    );
    assert_eq!(
        refreshed_state.watched_chain_plan[0].manifest_root_entry_count,
        1
    );
    assert_eq!(
        refreshed_state.watched_chain_plan[0].manifest_contract_entry_count,
        1
    );
    assert_eq!(
        refreshed_state.watched_chain_plan[0].discovery_edge_entry_count,
        1
    );
    assert_eq!(
        refreshed_tasks[0].addresses,
        refreshed_state.watched_chain_plan[0].addresses
    );
    assert_eq!(
        refreshed_tasks[0].checkpoint.canonical_block_number,
        Some(42)
    );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = 'ens_v1_registry_new_owner:ethereum-mainnet' AND deactivated_at IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE event_kind = 'SubregistryChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'owner' FROM normalized_events WHERE event_kind = 'SubregistryChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            "0x0000000000000000000000000000000000000002".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM normalized_events WHERE event_kind = 'SubregistryChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );

    let unchanged = reconcile_fetched_heads(
        database.pool(),
        &refreshed_tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head,
            safe: None,
            finalized: None,
        },
    )
    .await?;
    assert!(
        unchanged.is_none(),
        "unchanged heads should still backfill code hashes for newly watched ENSv1 addresses"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        2
    );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT code_byte_length FROM raw_code_hashes WHERE contract_address = '0x0000000000000000000000000000000000000002'"
            )
            .fetch_one(database.pool())
            .await?,
            0
        );
    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn storage_discovery_refresh_adds_basenames_address_without_manifest_reload_and_next_poll_backfills_code_hash()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests.write_manifest_for_namespace_source_family(
        "basenames",
        "basenames_base_registry",
        &basenames_base_registry_manifest_contents(),
    )?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;
    let initial_manifest_summary = initial_state.manifest_summary.clone();
    let initial_sync_summary = initial_state.sync_summary.clone();
    let initial_discovery_admission = initial_state.discovery_admission.clone();
    let initial_manifest_event_summary = initial_state.manifest_normalized_event_summary.clone();
    let initial_tasks =
        sync_intake_chain_tasks(database.pool(), &initial_state.watched_chain_plan).await?;

    assert_eq!(initial_state.watched_chain_plan.len(), 1);
    assert_eq!(initial_tasks.len(), 1);
    assert_eq!(initial_tasks[0].addresses.len(), 1);

    let canonical_head = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbc"),
        42,
    );
    let (provider, server) = bundle_provider(vec![canonical_head.clone()]).await?;

    let (_next_task, initial_outcome) = reconcile_fetched_heads(
        database.pool(),
        &initial_tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("initial Basenames registry poll must update task state");
    assert_eq!(
        initial_outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        1
    );

    insert_raw_new_owner_log_for_parent(
        database.pool(),
        "base-mainnet",
        &canonical_head,
        "0xb94704422c2a1e396835a571837aa5ae53285a95",
        "0x0000000000000000000000000000000000000002",
        &base_eth_node(),
        "alice",
        CanonicalityState::Canonical,
    )
    .await?;

    let (refreshed_state, refreshed_tasks) =
        refresh_runtime_state_from_storage_discovery(database.pool(), &initial_state)
            .await?
            .expect(
                "stored Basenames discovery must refresh the watched plan without manifest reload",
            );

    assert_eq!(refreshed_state.manifest_summary, initial_manifest_summary);
    assert_eq!(refreshed_state.sync_summary, initial_sync_summary);
    assert_eq!(
        refreshed_state.discovery_admission,
        initial_discovery_admission
    );
    assert_eq!(
        refreshed_state.manifest_normalized_event_summary,
        initial_manifest_event_summary
    );
    assert_eq!(
        fs::read_to_string(&manifest_path)
            .with_context(|| format!("failed to read {}", manifest_path.display()))?,
        basenames_base_registry_manifest_contents()
    );
    assert_eq!(refreshed_state.watched_chain_plan.len(), 1);
    assert_eq!(refreshed_tasks.len(), 1);
    assert_eq!(refreshed_state.watched_chain_plan[0].chain, "base-mainnet");
    assert_eq!(refreshed_state.watched_chain_plan[0].addresses.len(), 2);
    assert!(
        refreshed_state.watched_chain_plan[0]
            .addresses
            .contains(&"0xb94704422c2a1e396835a571837aa5ae53285a95".to_owned())
    );
    assert!(
        refreshed_state.watched_chain_plan[0]
            .addresses
            .contains(&"0x0000000000000000000000000000000000000002".to_owned())
    );
    assert_eq!(
        refreshed_state.watched_chain_plan[0].manifest_root_entry_count,
        1
    );
    assert_eq!(
        refreshed_state.watched_chain_plan[0].manifest_contract_entry_count,
        1
    );
    assert_eq!(
        refreshed_state.watched_chain_plan[0].discovery_edge_entry_count,
        1
    );
    assert_eq!(
        refreshed_tasks[0].addresses,
        refreshed_state.watched_chain_plan[0].addresses
    );
    assert_eq!(
        refreshed_tasks[0].checkpoint.canonical_block_number,
        Some(42)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = 'ens_v1_registry_new_owner:base-mainnet' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE event_kind = 'SubregistryChanged' AND namespace = 'basenames'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'owner' FROM normalized_events WHERE event_kind = 'SubregistryChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "0x0000000000000000000000000000000000000002".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE event_kind = 'SubregistryChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );

    let unchanged = reconcile_fetched_heads(
        database.pool(),
        &refreshed_tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head,
            safe: None,
            finalized: None,
        },
    )
    .await?;
    assert!(
        unchanged.is_none(),
        "unchanged heads should still backfill code hashes for newly watched Basenames addresses"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT code_byte_length FROM raw_code_hashes WHERE contract_address = '0x0000000000000000000000000000000000000002'"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn sync_adapter_owned_raw_log_state_backfills_reverse_claims_from_stored_raw_logs()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x342);
    let reverse_address = "0x00000000000000000000000000000000000000ae";
    let claimed_address = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";
    let stored_block = provider_block(
        "0xdededededededededededededededededededededededededededededededede",
        Some("0xefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef"),
        64,
    );

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
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
            VALUES (
                1,
                1,
                'ens',
                'ens_v1_reverse_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'uts46-v1',
                'manifests/ens/ens_v1_reverse_l1/v1.toml',
                '{}'::jsonb
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for reverse runtime bootstrap test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        reverse_address,
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        "ethereum-mainnet",
        &stored_block,
        reverse_address,
        claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    sync_adapter_owned_raw_log_state(database.pool(), &watched_plan).await?;
    sync_adapter_owned_raw_log_state(database.pool(), &watched_plan).await?;

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'address' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        claimed_address.to_ascii_lowercase()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'namespace' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'reverse_name' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_name_for_address(claimed_address)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'source_family' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v1_reverse_l1".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'contract_role' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        REVERSE_REGISTRAR_ROLE.to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'contract_instance_id' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_contract_instance_id.to_string()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'emitting_address' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_address.to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT raw_fact_ref->>'block_hash' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        stored_block.block_hash
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn sync_adapter_owned_raw_log_state_backfills_basenames_reverse_claims_and_authority_from_stored_raw_logs()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x361);
    let registrar_contract_instance_id = Uuid::from_u128(0x362);
    let registry_contract_instance_id = Uuid::from_u128(0x363);
    let resolver_contract_instance_id = Uuid::from_u128(0x364);
    let reverse_address = "0x79ea96012eea67a83431f1701b3dff7e37f9e282";
    let registrar_address = "0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a";
    let registry_address = "0xb94704422c2a1e396835a571837aa5ae53285a95";
    let resolver_address = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd";
    let claimed_address = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";
    let stored_block = provider_block(
        "0xdededededededededededededededededededededededededededededededede",
        Some("0xefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef"),
        64,
    );
    let alice_namehash = namehash_for_dns_name(&dns_encoded_base_eth_name("alice"));
    let transaction_hash = transaction_hash_for_block(&stored_block);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
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
            VALUES
                (
                    1,
                    1,
                    'basenames',
                    'basenames_base_primary',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_primary/v1.toml',
                    '{}'::jsonb
                ),
                (
                    2,
                    1,
                    'basenames',
                    'basenames_base_registrar',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_registrar/v1.toml',
                    '{}'::jsonb
                ),
                (
                    3,
                    1,
                    'basenames',
                    'basenames_base_registry',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_registry/v1.toml',
                    '{}'::jsonb
                ),
                (
                    4,
                    1,
                    'basenames',
                    'basenames_base_resolver',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'uts46-v1',
                    'manifests/basenames/basenames_base_resolver/v1.toml',
                    '{}'::jsonb
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for Basenames runtime bootstrap test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registrar_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "base-mainnet",
        "root",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        "base-mainnet",
        reverse_address,
        Some(1),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registrar_contract_instance_id,
        "base-mainnet",
        registrar_address,
        Some(2),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "base-mainnet",
        registry_address,
        Some(3),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        resolver_contract_instance_id,
        "base-mainnet",
        resolver_address,
        Some(4),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        2,
        "registrar",
        registrar_contract_instance_id,
        registrar_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        3,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        4,
        "resolver",
        resolver_contract_instance_id,
        resolver_address,
        "none",
        None,
        None,
    )
    .await?;

    upsert_raw_blocks(
        database.pool(),
        &[provider_block_to_raw_block(
            "base-mainnet",
            &stored_block,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash: transaction_hash.clone(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: reverse_address.to_owned(),
                topics: vec![
                    reverse_claimed_topic0(),
                    hex_string(&abi_word_address(claimed_address)),
                    reverse_node_for_address(claimed_address),
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash: transaction_hash.clone(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    registrar_name_registered_topic0(),
                    labelhash_hex("alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: decode_hex_string(&encode_registrar_name_registered_log_data(
                    "alice",
                    1_700_010_000,
                )),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash: transaction_hash.clone(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: registry_address.to_owned(),
                topics: vec![registry_new_resolver_topic0(), alice_namehash.clone()],
                data: decode_hex_string(&encode_registry_new_resolver_log_data(resolver_address)),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash: transaction_hash.clone(),
                transaction_index: 0,
                log_index: 3,
                emitting_address: resolver_address.to_owned(),
                topics: vec![
                    resolver_text_changed_topic0(),
                    alice_namehash.clone(),
                    keccak256_hex(b"com.twitter"),
                ],
                data: decode_hex_string(&encode_dynamic_string_log_data("com.twitter")),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash: transaction_hash,
                transaction_index: 0,
                log_index: 4,
                emitting_address: resolver_address.to_owned(),
                topics: vec![resolver_version_changed_topic0(), alice_namehash],
                data: decode_hex_string(&encode_resolver_version_changed_log_data(7)),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    sync_adapter_owned_raw_log_state(database.pool(), &watched_plan).await?;
    sync_adapter_owned_raw_log_state(database.pool(), &watched_plan).await?;

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged' AND namespace = 'basenames'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames_base_primary".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM name_surfaces")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT logical_name_id FROM name_surfaces LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "basenames:alice.base.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ResolverChanged' AND namespace = 'basenames'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordChanged' AND namespace = 'basenames'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordVersionChanged' AND namespace = 'basenames'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_marks_losing_branch_orphaned_on_reorg() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(71);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for reorg reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000001",
        Some(1),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        1,
        root_contract_instance_id,
        "0x0000000000000000000000000000000000000001",
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let mut tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let ancestor_block = provider_block(
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        Some("0x0000000000000000000000000000000000000000000000000000000000000000"),
        41,
    );
    let losing_block = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        42,
    );
    let new_parent_block = provider_block(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        42,
    );
    let new_head_block = provider_block(
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        43,
    );
    upsert_chain_lineage_blocks(
        database.pool(),
        &[provider_block_to_lineage(
            "ethereum-mainnet",
            &ancestor_block,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[provider_block_to_lineage(
            "ethereum-mainnet",
            &losing_block,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[provider_block_to_lineage(
            "ethereum-mainnet",
            &new_parent_block,
            CanonicalityState::Orphaned,
        )],
    )
    .await?;
    upsert_raw_blocks(
        database.pool(),
        &[
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &ancestor_block,
                CanonicalityState::Canonical,
            ),
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &losing_block,
                CanonicalityState::Canonical,
            ),
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &new_parent_block,
                CanonicalityState::Orphaned,
            ),
        ],
    )
    .await?;
    upsert_raw_transactions(
        database.pool(),
        &[RawTransaction {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            transaction_hash: transaction_hash_for_block(&losing_block),
            transaction_index: 0,
            from_address: "0x0000000000000000000000000000000000000001".to_owned(),
            to_address: Some("0x0000000000000000000000000000000000000002".to_owned()),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_code_hashes(
        database.pool(),
        &[RawCodeHash {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            contract_address: "0x0000000000000000000000000000000000000001".to_owned(),
            code_hash: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
                .to_owned(),
            code_byte_length: 32,
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_receipts(
        database.pool(),
        &[RawReceipt {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            transaction_hash: transaction_hash_for_block(&losing_block),
            transaction_index: 0,
            contract_address: None,
            status: Some(true),
            gas_used: Some(21_000),
            cumulative_gas_used: Some(21_000),
            logs_bloom: losing_block.logs_bloom.clone(),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[RawLog {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            transaction_hash: transaction_hash_for_block(&losing_block),
            transaction_index: 0,
            log_index: 0,
            emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
            topics: vec![
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ],
            data: vec![0xde, 0xad, 0xbe, 0xef],
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    sqlx::query(
            r#"
            INSERT INTO normalized_events (
                event_identity,
                namespace,
                event_kind,
                source_family,
                manifest_version,
                source_manifest_id,
                chain_id,
                block_number,
                block_hash,
                transaction_hash,
                log_index,
                raw_fact_ref,
                derivation_kind,
                canonicality_state,
                before_state,
                after_state
            )
            VALUES (
                'raw_log_preimage_observed:1:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:0:0x00000000000000000000000000000000000000aa',
                'ens',
                'PreimageObserved',
                'ens_test',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $1,
                0,
                '{"kind":"raw_log"}'::jsonb,
                'raw_log_preimage_observation',
                'canonical'::canonicality_state,
                '{}'::jsonb,
                '{"decoded_name":"wrapped.eth"}'::jsonb
            )
            "#,
        )
        .bind(transaction_hash_for_block(&losing_block))
        .execute(database.pool())
        .await
        .context("failed to insert normalized event for reorg reconciliation test")?;
    let losing_timestamp =
        OffsetDateTime::from_unix_timestamp(losing_block.block_timestamp_unix_secs)
            .expect("losing block timestamp must be valid");
    let token_lineage_id = Uuid::from_u128(0x7100);
    let resource_id = Uuid::from_u128(0x7200);
    let surface_binding_id = Uuid::from_u128(0x7300);
    upsert_token_lineages(
        database.pool(),
        &[TokenLineage {
            token_lineage_id,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[NameSurface {
            logical_name_id: "ens:reorg.eth".to_owned(),
            namespace: "ens".to_owned(),
            input_name: "reorg.eth".to_owned(),
            canonical_display_name: "reorg.eth".to_owned(),
            normalized_name: "reorg.eth".to_owned(),
            dns_encoded_name: vec![5, b'r', b'e', b'o', b'r', b'g', 3, b'e', b't', b'h', 0],
            namehash: "0xnamehashreorg".to_owned(),
            labelhashes: vec!["0xlabelhashreorg".to_owned()],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[SurfaceBinding {
            surface_binding_id,
            logical_name_id: "ens:reorg.eth".to_owned(),
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: losing_timestamp,
            active_to: None,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    sqlx::query(
        r#"
            INSERT INTO normalized_events (
                event_identity,
                namespace,
                logical_name_id,
                resource_id,
                event_kind,
                source_family,
                manifest_version,
                source_manifest_id,
                chain_id,
                block_number,
                block_hash,
                transaction_hash,
                log_index,
                raw_fact_ref,
                derivation_kind,
                canonicality_state,
                before_state,
                after_state
            )
            VALUES (
                'ens_v1_unwrapped_authority:ResolverChanged:resolver:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:1',
                'ens',
                'ens:reorg.eth',
                $1,
                'ResolverChanged',
                'ens_v1_registry_l1',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $2,
                1,
                '{"kind":"raw_log"}'::jsonb,
                'ens_v1_unwrapped_authority',
                'canonical'::canonicality_state,
                '{"resolver":"0x00000000000000000000000000000000000000aa"}'::jsonb,
                '{"resolver":"0x00000000000000000000000000000000000000bb"}'::jsonb
            )
            "#,
    )
    .bind(resource_id)
    .bind(transaction_hash_for_block(&losing_block))
    .execute(database.pool())
    .await
    .context("failed to insert ResolverChanged event for reorg reconciliation test")?;
    sqlx::query(
        r#"
            INSERT INTO normalized_events (
                event_identity,
                namespace,
                logical_name_id,
                resource_id,
                event_kind,
                source_family,
                manifest_version,
                source_manifest_id,
                chain_id,
                block_number,
                block_hash,
                transaction_hash,
                log_index,
                raw_fact_ref,
                derivation_kind,
                canonicality_state,
                before_state,
                after_state
            )
            VALUES
            (
                'ens_v1_unwrapped_authority:RecordChanged:record-change:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:2',
                'ens',
                'ens:reorg.eth',
                $1,
                'RecordChanged',
                'ens_v1_resolver_l1',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $2,
                2,
                '{"kind":"raw_log"}'::jsonb,
                'ens_v1_unwrapped_authority',
                'canonical'::canonicality_state,
                '{}'::jsonb,
                '{"record_key":"text","record_family":"text","selector_key":null}'::jsonb
            ),
            (
                'ens_v1_unwrapped_authority:RecordChanged:record-change:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:3',
                'ens',
                'ens:reorg.eth',
                $1,
                'RecordChanged',
                'ens_v1_resolver_l1',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $2,
                3,
                '{"kind":"raw_log"}'::jsonb,
                'ens_v1_unwrapped_authority',
                'canonical'::canonicality_state,
                '{}'::jsonb,
                '{"record_key":"addr:60","record_family":"addr","selector_key":"60"}'::jsonb
            ),
            (
                'ens_v1_unwrapped_authority:RecordVersionChanged:record-version:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:4',
                'ens',
                'ens:reorg.eth',
                $1,
                'RecordVersionChanged',
                'ens_v1_resolver_l1',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $2,
                4,
                '{"kind":"raw_log"}'::jsonb,
                'ens_v1_unwrapped_authority',
                'canonical'::canonicality_state,
                '{"record_version":null}'::jsonb,
                '{"record_version":7}'::jsonb
            )
            "#,
    )
    .bind(resource_id)
    .bind(transaction_hash_for_block(&losing_block))
    .execute(database.pool())
    .await
    .context("failed to insert record change events for reorg reconciliation test")?;
    tasks[0].checkpoint = advance_chain_checkpoints(
        database.pool(),
        &ChainCheckpointUpdate {
            chain_id: "ethereum-mainnet".to_owned(),
            canonical: Some(CheckpointBlockRef {
                block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                block_number: 42,
            }),
            safe: None,
            finalized: None,
        },
    )
    .await?;
    let (provider, server) =
        bundle_provider(vec![new_parent_block.clone(), new_head_block.clone()]).await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: new_head_block,
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("reorg reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::ReorgReconciled
    );
    assert_eq!(outcome.orphaned_block_count, 1);
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(43));
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0x1111111111111111111111111111111111111111111111111111111111111111'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_receipts WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_logs WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordChanged' AND canonicality_state = 'orphaned'::canonicality_state"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordVersionChanged' AND canonicality_state = 'orphaned'::canonicality_state"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND event_kind IN ('RecordChanged', 'RecordVersionChanged') AND canonicality_state = 'canonical'::canonicality_state"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM token_lineages WHERE token_lineage_id = $1"
        )
        .bind(token_lineage_id)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM resources WHERE resource_id = $1"
        )
        .bind(resource_id)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM name_surfaces WHERE logical_name_id = 'ens:reorg.eth'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM surface_bindings WHERE surface_binding_id = $1"
        )
        .bind(surface_binding_id)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );

    server.abort();
    database.cleanup().await?;
    Ok(())
}
