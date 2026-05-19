use std::{
    collections::BTreeMap,
    sync::{Mutex, atomic::AtomicUsize},
};

use crate::ops_catchup::{CapacityGuardConfig, OpsCatchupConfig, run_ops_finalized_catchup};
use bigname_storage::{BackfillLifecycleStatus, load_backfill_job, load_backfill_ranges};

include!("support.rs");

#[derive(Clone, Debug, Eq, PartialEq)]
struct OpsRecordedRpcRequest {
    method: String,
    params: Vec<Value>,
}

#[tokio::test]
async fn ops_catchup_bounds_chunks_to_observed_finalized_head_and_preserves_checkpoints()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(41_001);
    let address = "0x0000000000000000000000000000000000000001";
    insert_ops_watched_manifest_contract(
        database.pool(),
        410,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
        Some(1),
    )
    .await?;
    insert_checkpoint_guard(database.pool(), "ethereum-mainnet").await?;

    let blocks = provider_blocks(1, 3);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (provider_url, server) =
        ops_catchup_provider(blocks, vec![3], Arc::clone(&requests)).await?;
    let registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={provider_url}")])?;
    let outcome = run_ops_finalized_catchup(
        database.pool(),
        &[catchup_task("ethereum-mainnet", address)],
        &registry,
        ops_config(10),
    )
    .await?;

    assert_eq!(outcome.planned_chunk_count, 1);
    assert_eq!(outcome.capacity_check_count, 1);
    assert_eq!(outcome.drained_job_count, 1);
    let ranges = all_backfill_ranges(database.pool()).await?;
    assert_eq!(ranges, vec![(1, 3, "completed".to_owned())]);
    assert_checkpoint_guard_unchanged(database.pool(), "ethereum-mainnet").await?;

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn ops_catchup_follow_rereads_finalized_head_for_repeated_finite_jobs() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(42_001);
    let address = "0x0000000000000000000000000000000000000001";
    insert_ops_watched_manifest_contract(
        database.pool(),
        420,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
        Some(1),
    )
    .await?;

    let requests = Arc::new(Mutex::new(Vec::new()));
    let (provider_url, server) =
        ops_catchup_provider(provider_blocks(1, 4), vec![2, 4], Arc::clone(&requests)).await?;
    let registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={provider_url}")])?;
    let mut config = ops_config(2);
    config.follow = true;
    config.follow_iterations = Some(2);

    let outcome = run_ops_finalized_catchup(
        database.pool(),
        &[catchup_task("ethereum-mainnet", address)],
        &registry,
        config,
    )
    .await?;

    assert_eq!(outcome.follow_iteration_count, 2);
    assert_eq!(outcome.drained_job_count, 2);
    assert_eq!(outcome.reused_completed_chunk_count, 1);
    assert_eq!(
        all_backfill_ranges(database.pool()).await?,
        vec![
            (1, 2, "completed".to_owned()),
            (3, 4, "completed".to_owned()),
        ]
    );
    assert!(
        requests
            .lock()
            .expect("request log must not be poisoned")
            .iter()
            .filter(|request| {
                request.method == "eth_getBlockByNumber"
                    && request.params.first().and_then(Value::as_str) == Some("finalized")
            })
            .count()
            >= 2,
        "follow mode must re-read finalized head between iterations"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn ops_catchup_capacity_guard_fails_chunk_with_persisted_metadata_before_range_work()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(43_001);
    let address = "0x0000000000000000000000000000000000000001";
    insert_ops_watched_manifest_contract(
        database.pool(),
        430,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
        Some(1),
    )
    .await?;

    let requests = Arc::new(Mutex::new(Vec::new()));
    let (provider_url, server) =
        ops_catchup_provider(provider_blocks(1, 2), vec![2], Arc::clone(&requests)).await?;
    let registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={provider_url}")])?;
    let mut config = ops_config(2);
    config.capacity.postgres_max_bytes = Some(1);

    let error = run_ops_finalized_catchup(
        database.pool(),
        &[catchup_task("ethereum-mainnet", address)],
        &registry,
        config,
    )
    .await
    .expect_err("capacity breach must fail the chunk explicitly");
    assert!(
        error.to_string().contains("capacity guard breached"),
        "unexpected error: {error:#}"
    );

    let job_id = sqlx::query_scalar::<_, i64>("SELECT backfill_job_id FROM backfill_jobs")
        .fetch_one(database.pool())
        .await?;
    let job = load_backfill_job(database.pool(), job_id)
        .await?
        .expect("capacity-failed job must exist");
    assert_eq!(job.status, BackfillLifecycleStatus::Failed);
    assert_eq!(
        job.failure_reason.as_deref(),
        Some("ops catch-up capacity guard breached")
    );
    assert_eq!(
        job.failure_metadata.get("phase").and_then(Value::as_str),
        Some("capacity_guard")
    );
    assert_eq!(
        job.failure_metadata
            .get("capacity_status")
            .and_then(Value::as_str),
        Some("breached")
    );
    assert_eq!(
        job.failure_metadata
            .get("object_cache_budget_checked")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert!(
        job.failure_metadata
            .get("postgres_database_size_bytes")
            .and_then(Value::as_u64)
            .is_some()
    );
    let ranges = load_backfill_ranges(database.pool(), job_id).await?;
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].status, BackfillLifecycleStatus::Failed);
    assert_eq!(ops_table_count(database.pool(), "chain_lineage").await?, 0);
    assert_eq!(ops_table_count(database.pool(), "raw_logs").await?, 0);

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn ops_catchup_missing_base_provider_is_idle_while_ethereum_runs_and_unknown_start_skips()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let eth_address = "0x0000000000000000000000000000000000000001";
    let base_address = "0x0000000000000000000000000000000000000002";
    insert_ops_watched_manifest_contract(
        database.pool(),
        440,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        Uuid::from_u128(44_001),
        eth_address,
        Some(1),
    )
    .await?;
    insert_ops_watched_manifest_contract(
        database.pool(),
        441,
        "ens",
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        Uuid::from_u128(44_002),
        "0x0000000000000000000000000000000000000003",
        None,
    )
    .await?;
    insert_ops_watched_manifest_contract(
        database.pool(),
        442,
        "basenames",
        "base-mainnet",
        "basenames_base_registry",
        Uuid::from_u128(44_003),
        base_address,
        Some(1),
    )
    .await?;

    let requests = Arc::new(Mutex::new(Vec::new()));
    let (provider_url, server) =
        ops_catchup_provider(provider_blocks(1, 2), vec![2], Arc::clone(&requests)).await?;
    let registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={provider_url}")])?;
    let outcome = run_ops_finalized_catchup(
        database.pool(),
        &[
            catchup_task("base-mainnet", base_address),
            catchup_task("ethereum-mainnet", eth_address),
        ],
        &registry,
        ops_config(2),
    )
    .await?;

    assert_eq!(outcome.provider_configured_chain_count, 1);
    assert_eq!(outcome.missing_provider_chain_count, 1);
    assert_eq!(outcome.skipped_unknown_start_target_count, 1);
    assert_eq!(outcome.drained_job_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM backfill_jobs WHERE chain_id = 'base-mainnet'"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        all_backfill_ranges(database.pool()).await?,
        vec![(1, 2, "completed".to_owned())]
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn ops_catchup_rejects_configured_provider_outside_intake_tasks() -> Result<()> {
    let database = TestDatabase::new().await?;
    let registry = ProviderRegistry::from_chain_rpc_urls(&[
        "ethereum-mainnet=http://127.0.0.1:8545".to_owned(),
        "optimism-mainnet=http://127.0.0.1:7545".to_owned(),
    ])?;

    let error = run_ops_finalized_catchup(
        database.pool(),
        &[catchup_task(
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000001",
        )],
        &registry,
        ops_config(2),
    )
    .await
    .expect_err("ops catch-up must reject providers outside selected runtime chains");

    assert!(
        error.to_string().contains(
            "configured provider source chains outside selected/admitted runtime chain set: optimism-mainnet"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

async fn ops_catchup_provider(
    blocks: Vec<ProviderBlock>,
    finalized_sequence: Vec<i64>,
    requests: Arc<Mutex<Vec<OpsRecordedRpcRequest>>>,
) -> Result<(String, JoinHandle<()>)> {
    let fixtures_by_hash = Arc::new(
        blocks
            .into_iter()
            .map(|block| {
                (
                    block.block_hash.clone(),
                    ProviderBlockFixture {
                        logs: vec![rpc_log_payload(&block)],
                        block,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>(),
    );
    let hashes_by_number = Arc::new(
        fixtures_by_hash
            .values()
            .map(|fixture| (fixture.block.block_number, fixture.block.block_hash.clone()))
            .collect::<BTreeMap<_, _>>(),
    );
    let latest_number = *hashes_by_number
        .keys()
        .next_back()
        .context("ops catch-up provider needs at least one block")?;
    let finalized_reads = Arc::new(AtomicUsize::new(0));
    let finalized_sequence = Arc::new(finalized_sequence);

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
        requests
            .lock()
            .expect("request log must not be poisoned")
            .push(OpsRecordedRpcRequest {
                method: method.to_owned(),
                params: params.clone(),
            });

        let result = match method {
            "eth_getBlockByNumber" => {
                let selection = params
                    .first()
                    .and_then(Value::as_str)
                    .expect("block number or tag parameter must be present");
                let block_number = match selection {
                    "latest" => latest_number,
                    "safe" => finalized_sequence.first().copied().unwrap_or(latest_number),
                    "finalized" => {
                        let index = finalized_reads.fetch_add(1, Ordering::Relaxed);
                        finalized_sequence
                            .get(index)
                            .copied()
                            .or_else(|| finalized_sequence.last().copied())
                            .unwrap_or(latest_number)
                    }
                    number => support_parse_rpc_block_number(number),
                };
                let hash = hashes_by_number
                    .get(&block_number)
                    .unwrap_or_else(|| panic!("unexpected block number request: {body}"));
                rpc_block_bundle_payload(&fixtures_by_hash.get(hash).unwrap().block)
            }
            "eth_getBlockByHash" => {
                let block_hash = params
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                rpc_block_bundle_payload(&fixtures_by_hash.get(&block_hash).unwrap().block)
            }
            "eth_getLogs" => support_logs_for_filter(
                params.first().and_then(Value::as_object).unwrap(),
                &fixtures_by_hash,
                &hashes_by_number,
            ),
            "eth_getBlockReceipts" => {
                let block_hash = params
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                Value::Array(vec![rpc_receipt_payload(
                    &fixtures_by_hash.get(&block_hash).unwrap().block,
                )])
            }
            "eth_getCode" => Value::String("0x6001600155".to_owned()),
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        })
    }))
    .await?;
    Ok((url, server))
}

fn provider_blocks(from: i64, to: i64) -> Vec<ProviderBlock> {
    (from..=to)
        .map(|block_number| {
            let byte = u8::try_from(block_number).expect("test block number must fit in u8");
            let hash = hex_string(&[byte; 32]);
            let parent_hash = if block_number == from {
                None
            } else {
                let parent_byte =
                    u8::try_from(block_number - 1).expect("test parent must fit in u8");
                Some(hex_string(&[parent_byte; 32]))
            };
            provider_block(&hash, parent_hash.as_deref(), block_number)
        })
        .collect()
}

fn ops_config(chunk_blocks: i64) -> OpsCatchupConfig {
    OpsCatchupConfig {
        deployment_profile: "mainnet".to_owned(),
        manifests_root: PathBuf::from("manifests/mainnet"),
        chunk_blocks,
        follow: false,
        follow_iterations: None,
        follow_poll_interval_secs: 1,
        lease_duration_secs: 300,
        header_audit_mode: HeaderAuditMode::Minimal,
        capacity: CapacityGuardConfig {
            postgres_max_bytes: None,
            min_writable_free_disk_bytes: 0,
            writable_free_disk_path: std::env::current_dir().expect("current dir must exist"),
            estimated_bytes_per_block: 0,
        },
    }
}

fn catchup_task(chain: &str, address: &str) -> IntakeChainTask {
    IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: None,
            canonical_block_number: None,
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_ops_watched_manifest_contract(
    pool: &PgPool,
    manifest_id: i64,
    namespace: &str,
    chain: &str,
    source_family: &str,
    contract_instance_id: Uuid,
    address: &str,
    start_block: Option<i64>,
) -> Result<()> {
    let contracts = match start_block {
        Some(start_block) => json!([{"role": "WatchedContract", "start_block": start_block}]),
        None => json!([{"role": "WatchedContract"}]),
    };
    let manifest_payload = json!({
        "contracts": contracts,
        "roots": [],
        "abi": {"events": test_manifest_abi_events()},
    });
    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                namespace,
                source_family,
                chain,
                rollout_status,
                manifest_payload
            )
            VALUES ($1, $2, $3, $4, 'active', $5::jsonb)
            "#,
    )
    .bind(manifest_id)
    .bind(namespace)
    .bind(source_family)
    .bind(chain)
    .bind(serde_json::to_string(&manifest_payload)?)
    .execute(pool)
    .await?;
    insert_contract_instance(pool, contract_instance_id, chain, "contract").await?;
    insert_active_contract_instance_address(
        pool,
        contract_instance_id,
        chain,
        address,
        Some(manifest_id),
    )
    .await?;
    insert_manifest_contract_instance(
        pool,
        manifest_id,
        "WatchedContract",
        contract_instance_id,
        address,
        "none",
        None,
        None,
    )
    .await
}

async fn insert_checkpoint_guard(pool: &PgPool, chain: &str) -> Result<()> {
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
        VALUES (
            $1,
            '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
            70,
            '0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
            60,
            '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
            50
        )
        "#,
    )
    .bind(chain)
    .execute(pool)
    .await?;
    Ok(())
}

async fn assert_checkpoint_guard_unchanged(pool: &PgPool, chain: &str) -> Result<()> {
    assert_eq!(
        sqlx::query_as::<_, (String, i64, String, i64, String, i64)>(
            r#"
            SELECT
                canonical_block_hash,
                canonical_block_number,
                safe_block_hash,
                safe_block_number,
                finalized_block_hash,
                finalized_block_number
            FROM chain_checkpoints
            WHERE chain_id = $1
            "#
        )
        .bind(chain)
        .fetch_one(pool)
        .await?,
        (
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            70,
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
            60,
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned(),
            50,
        )
    );
    Ok(())
}

async fn all_backfill_ranges(pool: &PgPool) -> Result<Vec<(i64, i64, String)>> {
    sqlx::query_as::<_, (i64, i64, String)>(
        r#"
        SELECT
            range_start_block_number,
            range_end_block_number,
            status::TEXT
        FROM backfill_ranges
        ORDER BY range_start_block_number, range_end_block_number
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load backfill ranges")
}

async fn ops_table_count(pool: &PgPool, table_name: &str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table_name}"))
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to count rows in {table_name}"))
}
