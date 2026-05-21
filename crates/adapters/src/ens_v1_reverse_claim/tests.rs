use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use super::*;
use bigname_storage::{
    MIGRATOR, RawBlock, RawLog, default_database_url, load_normalized_event_counts_by_kind,
    load_normalized_events_by_namespace, upsert_raw_blocks, upsert_raw_logs,
};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::{Uuid, time::OffsetDateTime},
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

struct TestReverseClaimConfig<'a> {
    namespace: &'a str,
    source_family: &'a str,
    chain: &'a str,
    deployment_epoch: &'a str,
    file_path: &'a str,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for ENSv1 reverse tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bn_ad_ensv1_rev_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for ENSv1 reverse tests")?;
        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect test pool for ENSv1 reverse tests")?;
        MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for ENSv1 reverse tests")?;

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

struct ManifestVersionSeed<'a> {
    manifest_version: i64,
    namespace: &'a str,
    source_family: &'a str,
    chain: &'a str,
    deployment_epoch: &'a str,
    rollout_status: &'a str,
    file_path: &'a str,
}

async fn insert_manifest_version(pool: &PgPool, seed: ManifestVersionSeed<'_>) -> Result<i64> {
    sqlx::query_scalar(
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
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6::manifest_rollout_status,
            'ensip15@ens-normalize-0.1.1',
            $7,
            '{}'::jsonb
        )
        RETURNING manifest_id
        "#,
    )
    .bind(seed.manifest_version)
    .bind(seed.namespace)
    .bind(seed.source_family)
    .bind(seed.chain)
    .bind(seed.deployment_epoch)
    .bind(seed.rollout_status)
    .bind(seed.file_path)
    .fetch_one(pool)
    .await
    .context("failed to insert manifest version")
}

async fn insert_contract_instance(
    pool: &PgPool,
    chain: &str,
    contract_instance_id: Uuid,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind,
            provenance
        )
        VALUES ($1, $2, 'contract', '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(chain)
    .execute(pool)
    .await
    .context("failed to insert contract instance")?;
    Ok(())
}

async fn insert_manifest_contract_instance(
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
            declared_address,
            role,
            proxy_kind
        )
        VALUES ($1, 'contract', 'reverse_registrar', $2, $3, 'reverse_registrar', 'none')
        "#,
    )
    .bind(manifest_id)
    .bind(contract_instance_id)
    .bind(address)
    .execute(pool)
    .await
    .context("failed to insert manifest reverse contract instance")?;
    Ok(())
}

async fn insert_contract_instance_address(
    pool: &PgPool,
    chain: &str,
    contract_instance_id: Uuid,
    address: &str,
    source_manifest_id: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            source_manifest_id,
            provenance
        )
        VALUES ($1, $2, $3, $4, '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(chain)
    .bind(address)
    .bind(source_manifest_id)
    .execute(pool)
    .await
    .context("failed to insert contract-instance address")?;
    Ok(())
}

async fn insert_raw_reverse_claim_log(
    pool: &PgPool,
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    claimed_address: &str,
    canonicality_state: CanonicalityState,
) -> Result<()> {
    upsert_raw_blocks(
        pool,
        &[RawBlock {
            chain_id: chain.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: None,
            block_number,
            block_timestamp: OffsetDateTime::UNIX_EPOCH,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state,
        }],
    )
    .await?;
    upsert_raw_logs(
        pool,
        &[RawLog {
            chain_id: chain.to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            transaction_hash: format!("0xtx{block_number:02x}"),
            transaction_index: 0,
            log_index: 0,
            emitting_address: emitting_address.to_owned(),
            topics: vec![
                reverse_claimed_topic0(),
                hex_string(&abi_word_address(claimed_address)),
                reverse_node_for_address(claimed_address)?,
            ],
            data: Vec::new(),
            canonicality_state,
        }],
    )
    .await?;
    Ok(())
}

async fn run_idempotence_case(config: TestReverseClaimConfig<'_>) -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let active_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: config.namespace,
            source_family: config.source_family,
            chain: config.chain,
            deployment_epoch: config.deployment_epoch,
            rollout_status: "active",
            file_path: config.file_path,
        },
    )
    .await?;
    let draft_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 2,
            namespace: config.namespace,
            source_family: config.source_family,
            chain: config.chain,
            deployment_epoch: config.deployment_epoch,
            rollout_status: "draft",
            file_path: "manifests/test/draft.toml",
        },
    )
    .await?;
    let active_contract_instance_id = Uuid::new_v4();
    let draft_contract_instance_id = Uuid::new_v4();
    let active_emitter = "0x00000000000000000000000000000000000000aa";
    let draft_emitter = "0x00000000000000000000000000000000000000bb";
    let claimed_address = "0x1111111111111111111111111111111111111111";

    insert_contract_instance(database.pool(), config.chain, active_contract_instance_id).await?;
    insert_contract_instance(database.pool(), config.chain, draft_contract_instance_id).await?;
    insert_manifest_contract_instance(
        database.pool(),
        active_manifest_id,
        active_contract_instance_id,
        active_emitter,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        draft_manifest_id,
        draft_contract_instance_id,
        draft_emitter,
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        config.chain,
        active_contract_instance_id,
        active_emitter,
        active_manifest_id,
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        config.chain,
        draft_contract_instance_id,
        draft_emitter,
        draft_manifest_id,
    )
    .await?;

    insert_raw_reverse_claim_log(
        database.pool(),
        config.chain,
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        42,
        active_emitter,
        claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claim_log(
        database.pool(),
        config.chain,
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        43,
        draft_emitter,
        "0x2222222222222222222222222222222222222222",
        CanonicalityState::Canonical,
    )
    .await?;

    let first = sync_ens_v1_reverse_claim(database.pool(), config.chain).await?;
    assert_eq!(first.scanned_log_count, 1);
    assert_eq!(first.matched_log_count, 1);
    assert_eq!(first.total_synced_count, 1);
    assert_eq!(first.total_inserted_count, 1);
    assert_eq!(
        first.by_kind,
        BTreeMap::from([(
            EVENT_KIND_REVERSE_CHANGED.to_owned(),
            EnsV1ReverseClaimKindSyncSummary {
                synced_count: 1,
                inserted_count: 1,
            }
        )])
    );

    let events = load_normalized_events_by_namespace(database.pool(), config.namespace).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_kind, EVENT_KIND_REVERSE_CHANGED);
    assert_eq!(
        events[0].derivation_kind,
        DERIVATION_KIND_ENS_V1_REVERSE_CLAIM
    );
    assert_eq!(events[0].source_family, config.source_family);
    assert_eq!(events[0].source_manifest_id, Some(active_manifest_id));
    assert_eq!(events[0].chain_id.as_deref(), Some(config.chain));
    assert_eq!(
        events[0].after_state["address"],
        claimed_address.to_ascii_lowercase()
    );
    assert_eq!(events[0].after_state["coin_type"], ENS_NATIVE_COIN_TYPE);
    assert_eq!(events[0].after_state["namespace"], config.namespace);
    assert_eq!(events[0].after_state["reverse_namespace"], config.namespace);
    assert_eq!(
        events[0].after_state["reverse_node"],
        reverse_node_for_address(claimed_address)?
    );
    assert_eq!(
        events[0].after_state["reverse_name"],
        format!(
            "{}.addr.reverse",
            claimed_address
                .trim_start_matches("0x")
                .to_ascii_lowercase()
        )
    );
    assert_eq!(
        events[0].after_state["claim_provenance"]["source_family"],
        config.source_family
    );
    assert_eq!(
        events[0].after_state["claim_provenance"]["contract_role"],
        CONTRACT_ROLE_REVERSE_REGISTRAR
    );
    assert_eq!(
        events[0].after_state["claim_provenance"]["contract_instance_id"],
        active_contract_instance_id.to_string()
    );
    assert_eq!(
        events[0].after_state["claim_provenance"]["emitting_address"],
        active_emitter
    );

    let second = sync_ens_v1_reverse_claim(database.pool(), config.chain).await?;
    assert_eq!(second.scanned_log_count, 1);
    assert_eq!(second.matched_log_count, 1);
    assert_eq!(second.total_synced_count, 1);
    assert_eq!(second.total_inserted_count, 0);

    let counts = load_normalized_event_counts_by_kind(database.pool(), config.namespace).await?;
    assert_eq!(
        counts,
        BTreeMap::from([(EVENT_KIND_REVERSE_CHANGED.to_owned(), 1_usize)])
    );

    database.cleanup().await
}

async fn run_canonicality_case(config: TestReverseClaimConfig<'_>) -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: config.namespace,
            source_family: config.source_family,
            chain: config.chain,
            deployment_epoch: config.deployment_epoch,
            rollout_status: "active",
            file_path: config.file_path,
        },
    )
    .await?;
    let contract_instance_id = Uuid::new_v4();
    let emitter = "0x00000000000000000000000000000000000000aa";
    let claimed_address = "0x3333333333333333333333333333333333333333";

    insert_contract_instance(database.pool(), config.chain, contract_instance_id).await?;
    insert_manifest_contract_instance(database.pool(), manifest_id, contract_instance_id, emitter)
        .await?;
    insert_contract_instance_address(
        database.pool(),
        config.chain,
        contract_instance_id,
        emitter,
        manifest_id,
    )
    .await?;

    insert_raw_reverse_claim_log(
        database.pool(),
        config.chain,
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        44,
        emitter,
        claimed_address,
        CanonicalityState::Safe,
    )
    .await?;

    let first = sync_ens_v1_reverse_claim(database.pool(), config.chain).await?;
    assert_eq!(first.total_inserted_count, 1);
    let mut events = load_normalized_events_by_namespace(database.pool(), config.namespace).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].canonicality_state, CanonicalityState::Safe);

    insert_raw_reverse_claim_log(
        database.pool(),
        config.chain,
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        44,
        emitter,
        claimed_address,
        CanonicalityState::Finalized,
    )
    .await?;

    let second = sync_ens_v1_reverse_claim(database.pool(), config.chain).await?;
    assert_eq!(second.total_inserted_count, 0);
    events = load_normalized_events_by_namespace(database.pool(), config.namespace).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].canonicality_state, CanonicalityState::Finalized);
    assert_eq!(events[0].source_family, config.source_family);
    assert_eq!(events[0].chain_id.as_deref(), Some(config.chain));
    assert_eq!(events[0].after_state["namespace"], config.namespace);
    assert_eq!(events[0].after_state["reverse_namespace"], config.namespace);
    assert_eq!(
        events[0].after_state["claim_provenance"]["contract_role"],
        CONTRACT_ROLE_REVERSE_REGISTRAR
    );
    assert_eq!(
        events[0].after_state["claim_provenance"]["contract_instance_id"],
        contract_instance_id.to_string()
    );
    assert_eq!(
        events[0].after_state["claim_provenance"]["emitting_address"],
        emitter
    );

    database.cleanup().await
}

fn abi_word_address(value: &str) -> [u8; 32] {
    let value = value.strip_prefix("0x").unwrap_or(value);
    assert_eq!(value.len(), 40, "test address must be 20 bytes");
    let mut word = [0u8; 32];
    for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk).expect("test address chunk must be utf-8");
        word[12 + index] =
            u8::from_str_radix(hex, 16).expect("test address chunk must be valid hex");
    }
    word
}

#[tokio::test]
async fn sync_ens_v1_reverse_claim_is_idempotent() -> Result<()> {
    run_idempotence_case(TestReverseClaimConfig {
        namespace: "ens",
        source_family: SOURCE_FAMILY_ENS_V1_REVERSE_L1,
        chain: "ethereum-mainnet",
        deployment_epoch: "ens_v1",
        file_path: "manifests/ens/ens_v1_reverse_l1/v1.toml",
    })
    .await
}

#[tokio::test]
async fn sync_ens_v1_reverse_claim_is_idempotent_for_basenames_base_primary() -> Result<()> {
    run_idempotence_case(TestReverseClaimConfig {
        namespace: "basenames",
        source_family: SOURCE_FAMILY_BASENAMES_BASE_PRIMARY,
        chain: "base-mainnet",
        deployment_epoch: "basenames_v1",
        file_path: "manifests/basenames/basenames_base_primary/v1.toml",
    })
    .await
}

#[tokio::test]
async fn sync_ens_v1_reverse_claim_updates_event_canonicality() -> Result<()> {
    run_canonicality_case(TestReverseClaimConfig {
        namespace: "ens",
        source_family: SOURCE_FAMILY_ENS_V1_REVERSE_L1,
        chain: "ethereum-mainnet",
        deployment_epoch: "ens_v1",
        file_path: "manifests/ens/ens_v1_reverse_l1/v1.toml",
    })
    .await
}

#[tokio::test]
async fn sync_ens_v1_reverse_claim_updates_basenames_event_canonicality() -> Result<()> {
    run_canonicality_case(TestReverseClaimConfig {
        namespace: "basenames",
        source_family: SOURCE_FAMILY_BASENAMES_BASE_PRIMARY,
        chain: "base-mainnet",
        deployment_epoch: "basenames_v1",
        file_path: "manifests/basenames/basenames_base_primary/v1.toml",
    })
    .await
}
