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
    source_family: &str,
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    claimed_address: &str,
    canonicality_state: CanonicalityState,
) -> Result<()> {
    if source_family == SOURCE_FAMILY_BASENAMES_BASE_PRIMARY {
        return insert_raw_l2_reverse_name_log(
            pool,
            chain,
            block_hash,
            block_number,
            emitting_address,
            claimed_address,
            "alice.base.eth",
            canonicality_state,
        )
        .await;
    }

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
                reverse_claimed_topic0_for_source_family(source_family)
                    .context("test source family must have a reverse-claim topic")?,
                hex_string(&abi_word_address(claimed_address)),
                reverse_node_for_source_family(source_family, claimed_address)?,
            ],
            data: Vec::new(),
            canonicality_state,
        }],
    )
    .await?;
    Ok(())
}

#[test]
fn reverse_claimed_node_mismatch_drops_log_without_error() -> Result<()> {
    let claimed_address = "0x1111111111111111111111111111111111111111";
    let raw_log = raw_logs::ReverseRawLogRow {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 42,
        transaction_hash: "0xtx".to_owned(),
        transaction_index: 0,
        log_index: 7,
        emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
        emitting_contract_instance_id: Uuid::new_v4(),
        topics: vec![
            reverse_claimed_topic0_for_source_family(SOURCE_FAMILY_ENS_V1_REVERSE_L1)
                .context("ENSv1 reverse source should have a ReverseClaimed topic")?,
            hex_string(&abi_word_address(claimed_address)),
            hex_string(&[0x44; 32]),
        ],
        data: Vec::new(),
        canonicality_state: CanonicalityState::Canonical,
        source_manifest_id: 42,
        namespace: "ens".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REVERSE_L1.to_owned(),
        manifest_version: 1,
    };

    assert!(events::build_reverse_changed_events(&raw_log)?.is_empty());

    Ok(())
}

#[test]
fn malformed_reverse_claimed_topics_drop_log_without_error() -> Result<()> {
    let base_raw_log = raw_logs::ReverseRawLogRow {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 42,
        transaction_hash: "0xtx".to_owned(),
        transaction_index: 0,
        log_index: 7,
        emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
        emitting_contract_instance_id: Uuid::new_v4(),
        topics: vec![
            reverse_claimed_topic0_for_source_family(SOURCE_FAMILY_ENS_V1_REVERSE_L1)
                .context("ENSv1 reverse source should have a ReverseClaimed topic")?,
            "0x1234".to_owned(),
            hex_string(&[0x44; 32]),
        ],
        data: Vec::new(),
        canonicality_state: CanonicalityState::Canonical,
        source_manifest_id: 42,
        namespace: "ens".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REVERSE_L1.to_owned(),
        manifest_version: 1,
    };
    assert!(events::build_reverse_changed_events(&base_raw_log)?.is_empty());

    let mut missing_node_log = base_raw_log;
    missing_node_log.topics.truncate(2);
    assert!(events::build_reverse_changed_events(&missing_node_log)?.is_empty());

    Ok(())
}

fn expected_events_per_reverse_log(source_family: &str) -> usize {
    if source_family == SOURCE_FAMILY_BASENAMES_BASE_PRIMARY {
        2
    } else {
        1
    }
}

fn expected_reverse_claim_kind_counts(
    source_family: &str,
    inserted_count: usize,
) -> BTreeMap<String, EnsV1ReverseClaimKindSyncSummary> {
    let mut counts = BTreeMap::from([(
        EVENT_KIND_REVERSE_CHANGED.to_owned(),
        EnsV1ReverseClaimKindSyncSummary {
            synced_count: 1,
            inserted_count,
        },
    )]);
    if source_family == SOURCE_FAMILY_BASENAMES_BASE_PRIMARY {
        counts.insert(
            EVENT_KIND_RECORD_CHANGED.to_owned(),
            EnsV1ReverseClaimKindSyncSummary {
                synced_count: 1,
                inserted_count,
            },
        );
    }
    counts
}

async fn insert_raw_l2_reverse_name_log(
    pool: &PgPool,
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    claimed_address: &str,
    name: &str,
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
                name_for_addr_changed_topic0(),
                hex_string(&abi_word_address(claimed_address)),
            ],
            data: abi_encode_string(name),
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
        config.source_family,
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
        config.source_family,
        config.chain,
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        43,
        draft_emitter,
        "0x2222222222222222222222222222222222222222",
        CanonicalityState::Canonical,
    )
    .await?;

    let first = sync_ens_v1_reverse_claim(database.pool(), config.chain).await?;
    let expected_event_count = expected_events_per_reverse_log(config.source_family);
    assert_eq!(first.scanned_log_count, 1);
    assert_eq!(first.matched_log_count, 1);
    assert_eq!(first.total_synced_count, expected_event_count);
    assert_eq!(first.total_inserted_count, expected_event_count);
    assert_eq!(
        first.by_kind,
        expected_reverse_claim_kind_counts(config.source_family, 1)
    );

    let events = load_normalized_events_by_namespace(database.pool(), config.namespace).await?;
    assert_eq!(events.len(), expected_event_count);
    let reverse_event = events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_REVERSE_CHANGED)
        .context("missing reverse claim event")?;
    assert_eq!(reverse_event.event_kind, EVENT_KIND_REVERSE_CHANGED);
    assert_eq!(
        reverse_event.derivation_kind,
        DERIVATION_KIND_ENS_V1_REVERSE_CLAIM
    );
    assert_eq!(reverse_event.source_family, config.source_family);
    assert_eq!(reverse_event.source_manifest_id, Some(active_manifest_id));
    assert_eq!(reverse_event.chain_id.as_deref(), Some(config.chain));
    assert_eq!(
        reverse_event.after_state["address"],
        claimed_address.to_ascii_lowercase()
    );
    assert_eq!(
        reverse_event.after_state["coin_type"],
        if config.source_family == SOURCE_FAMILY_BASENAMES_BASE_PRIMARY {
            BASE_NATIVE_COIN_TYPE
        } else {
            ENS_NATIVE_COIN_TYPE
        }
    );
    assert_eq!(reverse_event.after_state["namespace"], config.namespace);
    assert_eq!(
        reverse_event.after_state["source_event"],
        if config.source_family == SOURCE_FAMILY_BASENAMES_BASE_PRIMARY {
            SOURCE_EVENT_NAME_FOR_ADDR_CHANGED
        } else {
            SOURCE_EVENT_REVERSE_CLAIMED
        }
    );
    assert_eq!(
        reverse_event.after_state["reverse_namespace"],
        config.namespace
    );
    assert_eq!(
        reverse_event.after_state["reverse_node"],
        reverse_node_for_source_family(config.source_family, claimed_address)?
    );
    assert_eq!(
        reverse_event.after_state["reverse_name"],
        reverse_name_for_source_family(config.source_family, claimed_address)?
    );
    assert_eq!(
        reverse_event.after_state["claim_provenance"]["source_family"],
        config.source_family
    );
    assert_eq!(
        reverse_event.after_state["claim_provenance"]["contract_role"],
        CONTRACT_ROLE_REVERSE_REGISTRAR
    );
    assert_eq!(
        reverse_event.after_state["claim_provenance"]["contract_instance_id"],
        active_contract_instance_id.to_string()
    );
    assert_eq!(
        reverse_event.after_state["claim_provenance"]["emitting_address"],
        active_emitter
    );
    if config.source_family == SOURCE_FAMILY_BASENAMES_BASE_PRIMARY {
        let record_event = events
            .iter()
            .find(|event| event.event_kind == EVENT_KIND_RECORD_CHANGED)
            .context("missing Basenames primary-name value event")?;
        assert_eq!(
            record_event.after_state["source_event"],
            SOURCE_EVENT_NAME_FOR_ADDR_CHANGED
        );
        assert_eq!(record_event.after_state["record_key"], "name");
        assert_eq!(record_event.after_state["raw_name"], "alice.base.eth");
        assert_eq!(
            record_event.after_state["primary_claim_source"]["coin_type"],
            BASE_NATIVE_COIN_TYPE
        );
    }

    let second = sync_ens_v1_reverse_claim(database.pool(), config.chain).await?;
    assert_eq!(second.scanned_log_count, 1);
    assert_eq!(second.matched_log_count, 1);
    assert_eq!(second.total_synced_count, expected_event_count);
    assert_eq!(second.total_inserted_count, 0);

    let counts = load_normalized_event_counts_by_kind(database.pool(), config.namespace).await?;
    let mut expected_counts = BTreeMap::from([(EVENT_KIND_REVERSE_CHANGED.to_owned(), 1_usize)]);
    if config.source_family == SOURCE_FAMILY_BASENAMES_BASE_PRIMARY {
        expected_counts.insert(EVENT_KIND_RECORD_CHANGED.to_owned(), 1);
    }
    assert_eq!(counts, expected_counts);

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
        config.source_family,
        config.chain,
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        44,
        emitter,
        claimed_address,
        CanonicalityState::Safe,
    )
    .await?;

    let first = sync_ens_v1_reverse_claim(database.pool(), config.chain).await?;
    let expected_event_count = expected_events_per_reverse_log(config.source_family);
    assert_eq!(first.total_inserted_count, expected_event_count);
    let mut events = load_normalized_events_by_namespace(database.pool(), config.namespace).await?;
    assert_eq!(events.len(), expected_event_count);
    assert!(
        events
            .iter()
            .all(|event| event.canonicality_state == CanonicalityState::Safe)
    );

    insert_raw_reverse_claim_log(
        database.pool(),
        config.source_family,
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
    assert_eq!(events.len(), expected_event_count);
    assert!(
        events
            .iter()
            .all(|event| event.canonicality_state == CanonicalityState::Finalized)
    );
    let reverse_event = events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_REVERSE_CHANGED)
        .context("missing canonical reverse claim event")?;
    assert_eq!(reverse_event.source_family, config.source_family);
    assert_eq!(reverse_event.chain_id.as_deref(), Some(config.chain));
    assert_eq!(reverse_event.after_state["namespace"], config.namespace);
    assert_eq!(
        reverse_event.after_state["reverse_namespace"],
        config.namespace
    );
    assert_eq!(
        reverse_event.after_state["claim_provenance"]["contract_role"],
        CONTRACT_ROLE_REVERSE_REGISTRAR
    );
    assert_eq!(
        reverse_event.after_state["claim_provenance"]["contract_instance_id"],
        contract_instance_id.to_string()
    );
    assert_eq!(
        reverse_event.after_state["claim_provenance"]["emitting_address"],
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

fn abi_word_u64(value: u64) -> [u8; 32] {
    let mut word = [0u8; 32];
    word[24..].copy_from_slice(&value.to_be_bytes());
    word
}

fn abi_encode_string(value: &str) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&abi_word_u64(32));
    data.extend_from_slice(&abi_word_u64(value.len() as u64));
    data.extend_from_slice(value.as_bytes());
    let padding = (32 - value.len() % 32) % 32;
    data.extend(std::iter::repeat_n(0, padding));
    data
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
async fn sync_basenames_base_primary_from_l2_reverse_registrar_name_value() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let active_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "basenames",
            source_family: SOURCE_FAMILY_BASENAMES_BASE_PRIMARY,
            chain: "base-mainnet",
            deployment_epoch: "basenames_v1",
            rollout_status: "active",
            file_path: "manifests/basenames/basenames_base_primary/v1.toml",
        },
    )
    .await?;
    let contract_instance_id = Uuid::new_v4();
    let l2_reverse_registrar = "0x0000000000d8e504002cc26e3ec46d81971c1664";
    let claimed_address = "0x1111111111111111111111111111111111111111";

    insert_contract_instance(database.pool(), "base-mainnet", contract_instance_id).await?;
    insert_manifest_contract_instance(
        database.pool(),
        active_manifest_id,
        contract_instance_id,
        l2_reverse_registrar,
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        "base-mainnet",
        contract_instance_id,
        l2_reverse_registrar,
        active_manifest_id,
    )
    .await?;
    insert_raw_l2_reverse_name_log(
        database.pool(),
        "base-mainnet",
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        42,
        l2_reverse_registrar,
        claimed_address,
        "alice.base.eth",
        CanonicalityState::Canonical,
    )
    .await?;

    let summary = sync_ens_v1_reverse_claim(database.pool(), "base-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 1);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.total_synced_count, 2);
    assert_eq!(
        summary.by_kind,
        BTreeMap::from([
            (
                EVENT_KIND_REVERSE_CHANGED.to_owned(),
                EnsV1ReverseClaimKindSyncSummary {
                    synced_count: 1,
                    inserted_count: 1,
                }
            ),
            (
                "RecordChanged".to_owned(),
                EnsV1ReverseClaimKindSyncSummary {
                    synced_count: 1,
                    inserted_count: 1,
                }
            )
        ])
    );

    let events = load_normalized_events_by_namespace(database.pool(), "basenames").await?;
    let reverse_event = events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_REVERSE_CHANGED)
        .context("missing Basenames L2 ReverseChanged event")?;
    assert_eq!(
        reverse_event.source_family,
        SOURCE_FAMILY_BASENAMES_BASE_PRIMARY
    );
    assert_eq!(reverse_event.chain_id.as_deref(), Some("base-mainnet"));
    assert_eq!(
        reverse_event.after_state["source_event"],
        "NameForAddrChanged"
    );
    assert_eq!(reverse_event.after_state["address"], claimed_address);
    assert_eq!(reverse_event.after_state["coin_type"], "2147492101");
    assert_eq!(reverse_event.after_state["namespace"], "basenames");
    assert_eq!(
        reverse_event.after_state["claim_provenance"]["source_family"],
        SOURCE_FAMILY_BASENAMES_BASE_PRIMARY
    );
    assert_eq!(
        reverse_event.after_state["claim_provenance"]["contract_instance_id"],
        contract_instance_id.to_string()
    );

    let record_event = events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .context("missing Basenames L2 primary-name value event")?;
    assert_eq!(record_event.after_state["record_key"], "name");
    assert_eq!(record_event.after_state["raw_name"], "alice.base.eth");
    assert_eq!(
        record_event.after_state["primary_claim_source"]["coin_type"],
        "2147492101"
    );
    assert_eq!(
        record_event.after_state["primary_claim_source"]["claim_provenance"]["emitting_address"],
        l2_reverse_registrar
    );

    database.cleanup().await
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
