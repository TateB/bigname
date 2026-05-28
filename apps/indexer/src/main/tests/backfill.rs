use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Mutex, atomic::AtomicBool},
    time::Duration as StdDuration,
};

use bigname_manifests::{
    WatchedBackfillTarget, WatchedChainPlan, WatchedSourceSelector, WatchedSourceSelectorKind,
    WatchedSourceSelectorPlan, WatchedTargetIdentity, load_watched_source_selector_plan,
};
use bigname_storage::{BackfillLifecycleStatus, load_backfill_job, load_backfill_ranges};

include!("support.rs");

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordedRpcRequest {
    method: String,
    params: Vec<Value>,
    http_request_id: u64,
    batch_size: usize,
}

#[derive(Clone, Copy, Debug)]
struct FocusedSourceFamilyFixture {
    namespace: &'static str,
    chain: &'static str,
    source_family: &'static str,
    contract_instance_id: Uuid,
    address: &'static str,
    block_number: i64,
    block_hash: &'static str,
}

#[derive(Clone, Copy, Debug)]
struct DynamicResolverBackfillFixture {
    namespace: &'static str,
    chain: &'static str,
    deployment_epoch: &'static str,
    registry_source_family: &'static str,
    resolver_source_family: &'static str,
    manifest_id_base: i64,
    uuid_base: u128,
    idempotency_key: &'static str,
}

#[test]
fn large_source_family_backfill_source_identity_uses_compact_digest() -> Result<()> {
    let selected_targets = (0..=backfill::COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD)
        .map(|index| WatchedBackfillTarget {
            source_family: "ens_v1_wrapper_l1".to_owned(),
            contract_instance_id: Uuid::from_u128(index as u128 + 1),
            address: format!("0x{index:040x}"),
            effective_from_block: index as i64,
            effective_to_block: index as i64 + 10,
        })
        .collect::<Vec<_>>();
    let source_plan = WatchedSourceSelectorPlan {
        chain: "ethereum-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::SourceFamily,
        source_family: Some("ens_v1_wrapper_l1".to_owned()),
        requested_watched_targets: Vec::new(),
        selected_targets,
        watched_chain_plan: WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: Vec::new(),
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 0,
            discovery_edge_entry_count: 0,
        },
    };

    let payload = backfill::backfill_job_source_identity_payload(&source_plan)?;
    assert_eq!(
        payload
            .get("source_identity_payload_format")
            .and_then(Value::as_str),
        Some("selected_targets_digest_v1")
    );
    assert!(payload.get("selected_targets").is_none());
    assert_eq!(
        payload.get("selected_target_count").and_then(Value::as_u64),
        Some(source_plan.selected_targets.len() as u64)
    );
    assert!(
        payload
            .get("selected_targets_digest")
            .and_then(Value::as_str)
            .map(|digest| digest.starts_with("keccak256:0x"))
            .unwrap_or(false)
    );
    assert!(
        payload
            .get("source_identity_hash")
            .and_then(Value::as_str)
            .map(|digest| digest.starts_with("keccak256:0x"))
            .unwrap_or(false)
    );
    assert_eq!(
        backfill::backfill_job_source_identity_payload(&source_plan)?,
        payload
    );

    let mut drifted_source_plan = source_plan.clone();
    drifted_source_plan
        .selected_targets
        .last_mut()
        .expect("test source plan has targets")
        .effective_to_block += 1;
    let drifted_payload = backfill::backfill_job_source_identity_payload(&drifted_source_plan)?;
    assert_ne!(
        drifted_payload
            .get("selected_targets_digest")
            .and_then(Value::as_str),
        payload
            .get("selected_targets_digest")
            .and_then(Value::as_str)
    );
    assert_ne!(
        drifted_payload
            .get("source_identity_hash")
            .and_then(Value::as_str),
        payload.get("source_identity_hash").and_then(Value::as_str)
    );

    Ok(())
}

#[test]
fn ensv1_resolver_backfill_source_identity_uses_generic_event_topics() -> Result<()> {
    let selected_targets = vec![WatchedBackfillTarget {
        source_family: "ens_v1_resolver_l1".to_owned(),
        contract_instance_id: Uuid::from_u128(1),
        address: "0x0000000000000000000000000000000000000001".to_owned(),
        effective_from_block: 1,
        effective_to_block: 10,
    }];
    let source_plan = WatchedSourceSelectorPlan {
        chain: "ethereum-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::SourceFamily,
        source_family: Some("ens_v1_resolver_l1".to_owned()),
        requested_watched_targets: Vec::new(),
        selected_targets,
        watched_chain_plan: WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: Vec::new(),
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 0,
            discovery_edge_entry_count: 0,
        },
    };

    let payload = backfill::backfill_job_source_identity_payload(&source_plan)?;
    assert_eq!(
        payload
            .get("source_identity_payload_format")
            .and_then(Value::as_str),
        Some("generic_resolver_event_topics_v1")
    );
    assert!(payload.get("selected_targets").is_none());

    let mut drifted_source_plan = source_plan.clone();
    drifted_source_plan.selected_targets[0].effective_to_block += 1;
    assert_eq!(
        backfill::backfill_job_source_identity_payload(&drifted_source_plan)?,
        payload
    );

    Ok(())
}

#[tokio::test]
async fn hash_pinned_backfill_persists_range_and_is_idempotent_without_advancing_checkpoints()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let root_contract_instance_id = Uuid::from_u128(901);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for backfill test")?;
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
                'ethereum-mainnet',
                '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
                7,
                '0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
                6,
                '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
                5
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert checkpoint guard row for backfill test")?;

    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::WholeActiveWatchedChain,
        42,
        43,
    )
    .await?;
    let block_42 = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
        42,
    );
    let block_43 = provider_block(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        43,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider(
        vec![block_42.clone(), block_43.clone()],
        Arc::clone(&requests),
    )
    .await?;

    let range = BackfillBlockRange::new(42, 43)?;
    let config = backfill_job_config(range, "indexer-backfill-hash-pinned", "lease-first")?;
    let outcome = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        config.clone(),
    )
    .await?;
    assert_eq!(
        outcome,
        backfill::BackfillJobRunOutcome {
            backfill_job_id: outcome.backfill_job_id,
            chain: "ethereum-mainnet".to_owned(),
            from_block: 42,
            to_block: 43,
            idempotency_key: "indexer-backfill-hash-pinned".to_owned(),
            reserved_range_count: 1,
            completed_range_count: 1,
            resolved_block_count: 2,
            raw_block_count: 2,
            raw_transaction_count: 2,
            raw_receipt_count: 2,
            raw_log_count: 2,
            raw_code_hash_count: 2,
        }
    );

    let job = load_backfill_job(database.pool(), outcome.backfill_job_id)
        .await?
        .expect("backfill job must exist");
    assert_eq!(job.status, BackfillLifecycleStatus::Completed);
    assert_eq!(job.deployment_profile, "mainnet");
    assert_eq!(job.chain_id, "ethereum-mainnet");
    assert_eq!(job.range_start_block_number, 42);
    assert_eq!(job.range_end_block_number, 43);
    assert_eq!(job.idempotency_key, "indexer-backfill-hash-pinned");
    assert_eq!(job.scan_mode, "hash_pinned_block");
    assert_eq!(
        job.source_identity
            .get("selector_kind")
            .and_then(Value::as_str),
        Some("whole_active_watched_chain")
    );
    assert_eq!(
        job.source_identity
            .get("source_identity_hash")
            .and_then(Value::as_str)
            .map(|value| value.starts_with("fnv1a64:")),
        Some(true)
    );
    assert_eq!(
        job.source_identity
            .get("selected_targets")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1)
    );

    let ranges = load_backfill_ranges(database.pool(), outcome.backfill_job_id).await?;
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].status, BackfillLifecycleStatus::Completed);
    assert_eq!(ranges[0].range_start_block_number, 42);
    assert_eq!(ranges[0].range_end_block_number, 43);
    assert_eq!(ranges[0].checkpoint_block_number, 43);
    assert_eq!(ranges[0].attempt_count, 1);

    let rerun = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(range, "indexer-backfill-hash-pinned", "lease-repeat")?,
    )
    .await?;
    assert_eq!(rerun.backfill_job_id, outcome.backfill_job_id);
    assert_eq!(rerun.reserved_range_count, 0);
    assert_eq!(rerun.completed_range_count, 0);
    assert_eq!(rerun.resolved_block_count, 0);

    let widened_error = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(
            BackfillBlockRange::new(42, 44)?,
            "indexer-backfill-hash-pinned",
            "lease-widened",
        )?,
    )
    .await
    .expect_err("same idempotency key must not widen work");
    assert!(
        widened_error
            .to_string()
            .contains("does not match requested immutable job identity"),
        "unexpected error: {widened_error:#}"
    );

    let ranges_after_conflict =
        load_backfill_ranges(database.pool(), outcome.backfill_job_id).await?;
    assert_eq!(ranges_after_conflict.len(), 1);
    assert_eq!(ranges_after_conflict[0].range_start_block_number, 42);
    assert_eq!(ranges_after_conflict[0].range_end_block_number, 43);
    assert_eq!(ranges_after_conflict[0].checkpoint_block_number, 43);
    assert_eq!(ranges_after_conflict[0].attempt_count, 1);

    assert_eq!(table_count(database.pool(), "chain_lineage").await?, 2);
    assert_eq!(table_count(database.pool(), "raw_transactions").await?, 2);
    assert_eq!(table_count(database.pool(), "raw_receipts").await?, 2);
    assert_eq!(table_count(database.pool(), "raw_logs").await?, 2);
    assert_eq!(table_count(database.pool(), "raw_code_hashes").await?, 2);
    assert_eq!(
        table_count(database.pool(), "raw_payload_cache_metadata").await?,
        0
    );
    let payload_cache_summary =
        sqlx::query_as::<_, (String, i64, i64, i64, Vec<String>, Vec<String>)>(
            r#"
            SELECT
                payload_kind,
                COUNT(*)::BIGINT,
                COUNT(retained_digest)::BIGINT,
                COUNT(DISTINCT retained_digest)::BIGINT,
                ARRAY_AGG(DISTINCT cache_metadata->>'method' ORDER BY cache_metadata->>'method')::TEXT[],
                ARRAY_AGG(DISTINCT cache_metadata->>'fetch_mode' ORDER BY cache_metadata->>'fetch_mode')::TEXT[]
            FROM raw_payload_cache_metadata
            GROUP BY payload_kind
            ORDER BY payload_kind
            "#,
        )
        .fetch_all(database.pool())
        .await?;
    assert_eq!(
        payload_cache_summary,
        Vec::<(String, i64, i64, i64, Vec<String>, Vec<String>)>::new(),
        "multi-block batched fetches must not fake hash-scoped retained payload metadata"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(table_count(database.pool(), "chain_lineage").await?, 2);
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
            WHERE chain_id = 'ethereum-mainnet'
            "#
        )
        .fetch_one(database.pool())
        .await?,
        (
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            7,
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
            6,
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned(),
            5,
        )
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
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 43"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(requests.len(), 21);
    let tagged_head_requests = requests
        .iter()
        .filter(|request| {
            request.method == "eth_getBlockByNumber"
                && request
                    .params
                    .first()
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.starts_with("0x"))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        tagged_head_requests
            .iter()
            .map(|request| request.params.first().and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec![
            Some("latest"),
            Some("safe"),
            Some("finalized"),
            Some("latest"),
            Some("safe"),
            Some("finalized")
        ]
    );
    for batch in tagged_head_requests.chunks(3) {
        assert_eq!(
            batch
                .iter()
                .map(|request| request.params.first().and_then(Value::as_str))
                .collect::<Vec<_>>(),
            vec![Some("latest"), Some("safe"), Some("finalized")]
        );
    }
    let head_hash_requests = requests
        .iter()
        .filter(|request| {
            request.method == "eth_getBlockByHash"
                && request.params.get(1) == Some(&Value::Bool(false))
        })
        .collect::<Vec<_>>();
    assert_eq!(head_hash_requests.len(), 2);
    let block_number_requests = requests
        .iter()
        .filter(|request| {
            request.method == "eth_getBlockByNumber"
                && request
                    .params
                    .first()
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.starts_with("0x"))
        })
        .collect::<Vec<_>>();
    assert_eq!(block_number_requests.len(), 6);
    assert_eq!(
        block_number_requests
            .iter()
            .map(|request| request.params.first().and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec![
            Some("0x2a"),
            Some("0x2b"),
            Some("0x2a"),
            Some("0x2b"),
            Some("0x2a"),
            Some("0x2b")
        ]
    );
    for batch in block_number_requests.chunks(2) {
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].batch_size, 2);
        assert!(
            batch.iter().all(
                |request| request.http_request_id == batch[0].http_request_id
                    && request.batch_size == 2
            ),
            "42..43 block-number resolution and post-log validation must use JSON-RPC batch HTTP requests"
        );
    }
    assert_ne!(
        block_number_requests[0].http_request_id, block_number_requests[2].http_request_id,
        "post-log hash validation must re-fetch block numbers after the range log request"
    );
    assert_ne!(
        block_number_requests[2].http_request_id, block_number_requests[4].http_request_id,
        "canonicality assignment must revalidate block hashes after the hash-pinned bundle fetch"
    );
    assert_eq!(requests[4].method, "eth_getBlockByNumber");
    assert_eq!(
        requests[4].params.first().and_then(Value::as_str),
        Some("0x2a")
    );
    assert_eq!(requests[5].method, "eth_getBlockByNumber");
    assert_eq!(
        requests[5].params.first().and_then(Value::as_str),
        Some("0x2b")
    );
    let log_requests = requests
        .iter()
        .filter(|request| request.method == "eth_getLogs")
        .collect::<Vec<_>>();
    assert_eq!(log_requests.len(), 1);
    assert_eq!(log_requests[0].batch_size, 1);
    let log_filter = log_requests[0]
        .params
        .first()
        .and_then(Value::as_object)
        .expect("log request must include a filter object");
    assert_eq!(
        log_filter.get("fromBlock").and_then(Value::as_str),
        Some("0x2a")
    );
    assert_eq!(
        log_filter.get("toBlock").and_then(Value::as_str),
        Some("0x2b")
    );
    assert!(
        !log_filter.contains_key("blockHash"),
        "multi-block backfill logs must use block ranges instead of per-block blockHash filters"
    );
    assert_eq!(
        log_filter.get("address").and_then(Value::as_array),
        Some(&vec![Value::String(
            "0x0000000000000000000000000000000000000001".to_owned()
        )]),
        "backfill log range must be scoped to the selected address set"
    );
    let full_block_requests = requests
        .iter()
        .filter(|request| {
            request.method == "eth_getBlockByHash"
                && request.params.get(1) == Some(&Value::Bool(true))
        })
        .collect::<Vec<_>>();
    assert_eq!(full_block_requests.len(), 2);
    assert_eq!(full_block_requests[0].method, "eth_getBlockByHash");
    assert_eq!(
        full_block_requests[0]
            .params
            .first()
            .and_then(Value::as_str),
        Some(block_42.block_hash.as_str())
    );
    assert_eq!(full_block_requests[1].method, "eth_getBlockByHash");
    assert_eq!(
        full_block_requests[1]
            .params
            .first()
            .and_then(Value::as_str),
        Some(block_43.block_hash.as_str())
    );
    let receipt_requests = requests
        .iter()
        .filter(|request| request.method == "eth_getBlockReceipts")
        .collect::<Vec<_>>();
    assert_eq!(receipt_requests.len(), 2);
    assert_eq!(receipt_requests[0].batch_size, 2);
    assert!(
        receipt_requests
            .iter()
            .all(
                |request| request.http_request_id == receipt_requests[0].http_request_id
                    && request.batch_size == 2
            ),
        "hash-pinned receipt hydration must share one JSON-RPC batch HTTP request"
    );
    assert_eq!(
        receipt_requests[0].params.first().and_then(Value::as_str),
        Some(block_42.block_hash.as_str())
    );
    assert_eq!(
        receipt_requests[1].params.first().and_then(Value::as_str),
        Some(block_43.block_hash.as_str())
    );
    let code_requests = requests
        .iter()
        .filter(|request| request.method == "eth_getCode")
        .collect::<Vec<_>>();
    assert_eq!(code_requests.len(), 2);
    assert_eq!(code_requests[0].batch_size, 2);
    assert!(
        code_requests.iter().all(|request| request.http_request_id
            == code_requests[0].http_request_id
            && request.batch_size == 2),
        "hash-pinned code observations must share one JSON-RPC batch HTTP request"
    );
    assert_eq!(code_requests[0].method, "eth_getCode");
    assert_eq!(
        code_requests[0]
            .params
            .get(1)
            .and_then(Value::as_object)
            .and_then(|selection| selection.get("blockHash"))
            .and_then(Value::as_str),
        Some(block_42.block_hash.as_str())
    );
    assert_eq!(code_requests[1].method, "eth_getCode");
    assert_eq!(
        code_requests[1]
            .params
            .get(1)
            .and_then(Value::as_object)
            .and_then(|selection| selection.get("blockHash"))
            .and_then(Value::as_str),
        Some(block_43.block_hash.as_str())
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn hash_pinned_backfill_refreshes_lease_before_completed_reservation_noop() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let root_contract_instance_id = Uuid::from_u128(902);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for backfill lease refresh test")?;
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

    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::WholeActiveWatchedChain,
        42,
        42,
    )
    .await?;
    let block = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
        42,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures_and_heads_and_delay(
        vec![ProviderBlockFixture {
            logs: vec![rpc_log_payload(&block)],
            block,
        }],
        Arc::clone(&requests),
        None,
        None,
        Some(StdDuration::from_millis(2_500)),
    )
    .await?;

    let range = BackfillBlockRange::new(42, 42)?;
    let mut config = backfill_job_config(
        range,
        "indexer-backfill-refreshes-expired-lease",
        "lease-refresh",
    )?;
    config.lease_expires_at =
        OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + 2)
            .context("short lease deadline must be valid")?;
    config.hash_pinned_chunk_blocks = 1;

    let outcome =
        run_resumable_hash_pinned_backfill_job(database.pool(), &source_plan, &provider, config)
            .await?;
    assert_eq!(outcome.reserved_range_count, 1);
    assert_eq!(outcome.completed_range_count, 1);
    assert_eq!(outcome.resolved_block_count, 1);

    let job = load_backfill_job(database.pool(), outcome.backfill_job_id)
        .await?
        .expect("backfill job must exist");
    assert_eq!(job.status, BackfillLifecycleStatus::Completed);
    let ranges = load_backfill_ranges(database.pool(), outcome.backfill_job_id).await?;
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].status, BackfillLifecycleStatus::Completed);
    assert_eq!(ranges[0].checkpoint_block_number, 42);
    assert!(ranges[0].lease_expires_at.is_none());

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn manual_finite_backfill_runs_full_requested_range_without_startup_cap() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(9_100);
    let address = "0x0000000000000000000000000000000000000001";

    insert_watched_manifest_contract(
        database.pool(),
        9_100,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
    )
    .await?;

    let range = BackfillBlockRange::new(1, 4)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::WholeActiveWatchedChain,
        range.from_block,
        range.to_block,
    )
    .await?;
    let block_1 = provider_block(
        "0x1000000000000000000000000000000000000000000000000000000000000001",
        Some("0x0000000000000000000000000000000000000000000000000000000000000000"),
        1,
    );
    let block_2 = provider_block(
        "0x2000000000000000000000000000000000000000000000000000000000000002",
        Some(&block_1.block_hash),
        2,
    );
    let block_3 = provider_block(
        "0x3000000000000000000000000000000000000000000000000000000000000003",
        Some(&block_2.block_hash),
        3,
    );
    let block_4 = provider_block(
        "0x4000000000000000000000000000000000000000000000000000000000000004",
        Some(&block_3.block_hash),
        4,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider(
        vec![
            block_1.clone(),
            block_2.clone(),
            block_3.clone(),
            block_4.clone(),
        ],
        Arc::clone(&requests),
    )
    .await?;

    let outcome = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(range, "manual-full-finite-range", "lease-manual-full")?,
    )
    .await?;

    assert_eq!((outcome.from_block, outcome.to_block), (1, 4));
    assert_eq!(outcome.resolved_block_count, 4);
    assert_eq!(outcome.raw_block_count, 4);
    assert_eq!(outcome.raw_log_count, 4);
    assert_eq!(outcome.raw_code_hash_count, 4);

    let job = load_backfill_job(database.pool(), outcome.backfill_job_id)
        .await?
        .expect("manual finite backfill job must exist");
    assert_eq!(job.range_start_block_number, 1);
    assert_eq!(job.range_end_block_number, 4);
    let ranges = load_backfill_ranges(database.pool(), outcome.backfill_job_id).await?;
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].range_start_block_number, 1);
    assert_eq!(ranges[0].range_end_block_number, 4);
    assert_eq!(table_count(database.pool(), "chain_lineage").await?, 4);
    assert_eq!(table_count(database.pool(), "chain_lineage").await?, 4);

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn source_scoped_backfill_empty_historical_blocks_skip_payload_cache_metadata() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(9_200);
    let address = "0x0000000000000000000000000000000000000001";

    insert_watched_manifest_contract(
        database.pool(),
        9_200,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
    )
    .await?;

    let range = BackfillBlockRange::new(42, 42)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_wrapper_l1".to_owned()),
        range.from_block,
        range.to_block,
    )
    .await?;
    let block = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
        42,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures(
        vec![ProviderBlockFixture {
            block: block.clone(),
            logs: Vec::new(),
        }],
        Arc::clone(&requests),
    )
    .await?;

    let outcome = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(range, "empty-selected-target-block", "lease-empty")?,
    )
    .await?;

    assert_eq!(outcome.raw_block_count, 1);
    assert_eq!(outcome.raw_log_count, 0);
    assert_eq!(outcome.raw_transaction_count, 0);
    assert_eq!(outcome.raw_receipt_count, 0);
    assert_eq!(outcome.raw_code_hash_count, 1);
    assert_eq!(table_count(database.pool(), "chain_lineage").await?, 1);
    assert_eq!(table_count(database.pool(), "chain_lineage").await?, 1);
    assert_eq!(table_count(database.pool(), "raw_code_hashes").await?, 1);
    assert_eq!(table_count(database.pool(), "raw_logs").await?, 0);
    assert_eq!(table_count(database.pool(), "raw_transactions").await?, 0);
    assert_eq!(table_count(database.pool(), "raw_receipts").await?, 0);
    assert_eq!(
        table_count(database.pool(), "raw_payload_cache_metadata").await?,
        0
    );
    assert_eq!(table_count(database.pool(), "normalized_events").await?, 0);

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn single_block_ensv1_resolver_backfill_uses_topic_filtered_logs() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let resolver_contract_instance_id = Uuid::from_u128(9_210);
    let resolver_address = "0x0000000000000000000000000000000000000a01";
    let unrelated_address = "0x0000000000000000000000000000000000000b01";

    insert_watched_manifest_contract(
        database.pool(),
        9_210,
        "ens",
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;

    let range = BackfillBlockRange::new(42, 42)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_resolver_l1".to_owned()),
        range.from_block,
        range.to_block,
    )
    .await?;
    let block = provider_block(
        "0x4242424242424242424242424242424242424242424242424242424242424242",
        Some("0x4141414141414141414141414141414141414141414141414141414141414141"),
        42,
    );
    let resolver_node = namehash_for_dns_name(&dns_encoded_eth_name("alice"));
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures(
        vec![ProviderBlockFixture {
            block: block.clone(),
            logs: vec![
                rpc_resolver_name_changed_log_payload_for_namehash(
                    &block,
                    resolver_address,
                    &resolver_node,
                    "alice.example",
                    0,
                ),
                rpc_log_payload_at_address(&block, unrelated_address, 1),
            ],
        }],
        Arc::clone(&requests),
    )
    .await?;

    let outcome = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(
            range,
            "single-block-ensv1-resolver-topic-filter",
            "lease-single-block-resolver",
        )?,
    )
    .await?;

    assert_eq!(outcome.raw_log_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            r#"
            SELECT COALESCE(
                ARRAY_AGG(emitting_address ORDER BY log_index),
                ARRAY[]::TEXT[]
            )
            FROM raw_logs
            "#
        )
        .fetch_one(database.pool())
        .await?,
        vec![resolver_address.to_owned()]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs WHERE emitting_address = $1")
            .bind(unrelated_address)
            .fetch_one(database.pool())
            .await?,
        0,
        "single-block resolver-family backfill must not retain unrelated same-block logs"
    );

    let recorded_requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    let log_requests = recorded_requests
        .iter()
        .filter(|request| request.method == "eth_getLogs")
        .collect::<Vec<_>>();
    assert_eq!(log_requests.len(), 1);
    let log_filter = log_requests[0]
        .params
        .first()
        .and_then(Value::as_object)
        .expect("single-block resolver lookup must include a log filter");
    assert_eq!(
        log_filter.get("fromBlock").and_then(Value::as_str),
        Some("0x2a")
    );
    assert_eq!(
        log_filter.get("toBlock").and_then(Value::as_str),
        Some("0x2a")
    );
    assert!(
        !log_filter.contains_key("address"),
        "generic ENSv1 resolver lookup must scan all emitters"
    );
    assert!(
        support_log_filter_topic0s(log_filter)
            .expect("generic ENSv1 resolver lookup must constrain topic0")
            .contains(&resolver_name_changed_topic0()),
        "generic ENSv1 resolver lookup must retain resolver-event topic filtering"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn raw_only_hash_pinned_backfill_skips_adapter_replay_after_raw_persistence() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(9_250);
    let address = "0x0000000000000000000000000000000000000001";

    insert_watched_manifest_contract(
        database.pool(),
        9_250,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
    )
    .await?;

    let range = BackfillBlockRange::new(42, 43)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_wrapper_l1".to_owned()),
        range.from_block,
        range.to_block,
    )
    .await?;
    let block = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
        42,
    );
    let next_block = provider_block(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some(&block.block_hash),
        43,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures(
        vec![
            ProviderBlockFixture {
                block: block.clone(),
                logs: vec![rpc_log_payload(&block)],
            },
            ProviderBlockFixture {
                block: next_block.clone(),
                logs: vec![rpc_log_payload(&next_block)],
            },
        ],
        Arc::clone(&requests),
    )
    .await?;
    let mut config = backfill_job_config(range, "raw-only-adapter-sync", "lease-raw-only")?;
    config.adapter_sync_mode = backfill::BackfillAdapterSyncMode::RawOnly;

    let outcome =
        run_resumable_hash_pinned_backfill_job(database.pool(), &source_plan, &provider, config)
            .await?;
    assert_eq!(outcome.raw_log_count, 2);
    assert_eq!(outcome.raw_transaction_count, 2);
    assert_eq!(outcome.raw_receipt_count, 2);
    assert_eq!(table_count(database.pool(), "raw_logs").await?, 2);
    assert_eq!(table_count(database.pool(), "normalized_events").await?, 0);
    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert!(
        requests
            .iter()
            .filter(|request| request.method == "eth_getBlockByHash")
            .all(|request| request.params.get(1) == Some(&Value::Bool(false))),
        "raw-only multi-block backfill must fetch block headers without full transaction payloads"
    );
    assert!(
        requests
            .iter()
            .any(|request| request.method == "eth_getTransactionByHash")
    );
    assert!(
        requests
            .iter()
            .any(|request| request.method == "eth_getTransactionReceipt")
    );
    assert!(
        requests
            .iter()
            .all(|request| request.method != "eth_getBlockReceipts"),
        "raw-only multi-block backfill must not fetch whole-block receipts"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn auto_hash_pinned_backfill_normalizes_selected_raw_facts() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(9_260);
    let address = "0x0000000000000000000000000000000000000001";

    insert_watched_manifest_contract(
        database.pool(),
        9_260,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
    )
    .await?;

    let range = BackfillBlockRange::new(42, 42)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_wrapper_l1".to_owned()),
        range.from_block,
        range.to_block,
    )
    .await?;
    let block = provider_block(
        "0xabababababababababababababababababababababababababababababababab",
        Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
        42,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures(
        vec![ProviderBlockFixture {
            block: block.clone(),
            logs: vec![rpc_log_payload(&block)],
        }],
        Arc::clone(&requests),
    )
    .await?;
    let mut config = backfill_job_config(range, "auto-adapter-sync", "lease-auto")?;
    config.adapter_sync_mode = backfill::BackfillAdapterSyncMode::Auto;

    let outcome =
        run_resumable_hash_pinned_backfill_job(database.pool(), &source_plan, &provider, config)
            .await?;

    assert_eq!(outcome.raw_log_count, 1);
    assert_eq!(table_count(database.pool(), "raw_logs").await?, 1);
    assert!(
        table_count(database.pool(), "normalized_events").await? > 0,
        "manual auto hash-pinned backfill must normalize selected raw facts"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn auto_source_family_backfill_normalizes_reverse_claims_after_raw_persistence() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(9_265);
    let reverse_address = "0x00000000000000000000000000000000000000af";
    let claimed_address = "0x2222222222222222222222222222222222222222";

    insert_watched_manifest_contract(
        database.pool(),
        9_265,
        "ens",
        "ethereum-mainnet",
        "ens_v1_reverse_l1",
        contract_instance_id,
        reverse_address,
    )
    .await?;

    let range = BackfillBlockRange::new(42, 42)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_reverse_l1".to_owned()),
        range.from_block,
        range.to_block,
    )
    .await?;
    let block = provider_block(
        "0xacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacac",
        Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
        42,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures(
        vec![ProviderBlockFixture {
            block: block.clone(),
            logs: vec![rpc_reverse_claimed_log_payload(
                &block,
                reverse_address,
                claimed_address,
                0,
            )],
        }],
        Arc::clone(&requests),
    )
    .await?;
    let mut config = backfill_job_config(range, "auto-reverse-scoped-sync", "lease-auto-reverse")?;
    config.adapter_sync_mode = backfill::BackfillAdapterSyncMode::Auto;

    let outcome =
        run_resumable_hash_pinned_backfill_job(database.pool(), &source_plan, &provider, config)
            .await?;

    assert_eq!(outcome.raw_log_count, 1);
    assert_eq!(table_count(database.pool(), "raw_logs").await?, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "manual auto source-family backfill must run the reverse-claim adapter after raw persistence"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v1_reverse_l1".to_owned()
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn auto_watched_target_backfill_scopes_reverse_claim_replay_to_selected_target() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let selected_contract_instance_id = Uuid::from_u128(9_266);
    let sibling_contract_instance_id = Uuid::from_u128(9_267);
    let selected_reverse_address = "0x00000000000000000000000000000000000000af";
    let sibling_reverse_address = "0x00000000000000000000000000000000000000bf";
    let selected_claimed_address = "0x2222222222222222222222222222222222222222";
    let sibling_claimed_address = "0x3333333333333333333333333333333333333333";

    insert_watched_manifest_contract(
        database.pool(),
        9_266,
        "ens",
        "ethereum-mainnet",
        "ens_v1_reverse_l1",
        selected_contract_instance_id,
        selected_reverse_address,
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        sibling_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        sibling_contract_instance_id,
        "ethereum-mainnet",
        sibling_reverse_address,
        Some(9_266),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        9_266,
        "reverse_sibling",
        sibling_contract_instance_id,
        sibling_reverse_address,
        "none",
        None,
        None,
    )
    .await?;

    let range = BackfillBlockRange::new(42, 42)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::WatchedTargetSet(vec![WatchedTargetIdentity {
            contract_instance_id: selected_contract_instance_id,
        }]),
        range.from_block,
        range.to_block,
    )
    .await?;
    let block = provider_block(
        "0xadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadad",
        Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
        42,
    );
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        "ethereum-mainnet",
        &block,
        sibling_reverse_address,
        sibling_claimed_address,
        CanonicalityState::Canonical,
        1,
    )
    .await?;
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures(
        vec![ProviderBlockFixture {
            block: block.clone(),
            logs: vec![rpc_reverse_claimed_log_payload(
                &block,
                selected_reverse_address,
                selected_claimed_address,
                0,
            )],
        }],
        Arc::clone(&requests),
    )
    .await?;
    let mut config = backfill_job_config(
        range,
        "auto-reverse-target-scoped-sync",
        "lease-auto-reverse-target",
    )?;
    config.adapter_sync_mode = backfill::BackfillAdapterSyncMode::Auto;

    let outcome =
        run_resumable_hash_pinned_backfill_job(database.pool(), &source_plan, &provider, config)
            .await?;

    assert_eq!(outcome.raw_log_count, 1);
    assert_eq!(
        table_count(database.pool(), "raw_logs").await?,
        2,
        "the sibling raw log is already persisted in the selected block"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "scoped replay must normalize only the selected reverse target"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events
             WHERE event_kind = 'ReverseChanged'
               AND LOWER(after_state->'claim_provenance'->>'emitting_address') = LOWER($1)"
        )
        .bind(selected_reverse_address)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events
             WHERE event_kind = 'ReverseChanged'
               AND LOWER(after_state->'claim_provenance'->>'emitting_address') = LOWER($1)"
        )
        .bind(sibling_reverse_address)
        .fetch_one(database.pool())
        .await?,
        0,
        "same-block sibling raw facts outside the explicit watched-target scope must stay untouched"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn raw_only_sparse_backfill_retains_empty_block_lineage_and_raw_anchors() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(9_270);
    let address = "0x0000000000000000000000000000000000000001";

    insert_watched_manifest_contract(
        database.pool(),
        9_270,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
    )
    .await?;

    let range = BackfillBlockRange::new(40, 42)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_wrapper_l1".to_owned()),
        range.from_block,
        range.to_block,
    )
    .await?;
    let block_40 = provider_block(
        "0x4040404040404040404040404040404040404040404040404040404040404040",
        Some("0x3939393939393939393939393939393939393939393939393939393939393939"),
        40,
    );
    let block_41 = provider_block(
        "0x4141414141414141414141414141414141414141414141414141414141414141",
        Some(&block_40.block_hash),
        41,
    );
    let block_42 = provider_block(
        "0x4242424242424242424242424242424242424242424242424242424242424242",
        Some(&block_41.block_hash),
        42,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures(
        vec![
            ProviderBlockFixture {
                block: block_40.clone(),
                logs: vec![rpc_log_payload(&block_40)],
            },
            ProviderBlockFixture {
                block: block_41.clone(),
                logs: Vec::new(),
            },
            ProviderBlockFixture {
                block: block_42.clone(),
                logs: vec![rpc_log_payload(&block_42)],
            },
        ],
        Arc::clone(&requests),
    )
    .await?;
    let mut config = backfill_job_config(range, "raw-only-sparse-empty", "lease-sparse-empty")?;
    config.adapter_sync_mode = backfill::BackfillAdapterSyncMode::RawOnly;

    let outcome =
        run_resumable_hash_pinned_backfill_job(database.pool(), &source_plan, &provider, config)
            .await?;

    assert_eq!(outcome.resolved_block_count, 3);
    assert_eq!(outcome.raw_block_count, 3);
    assert_eq!(outcome.raw_log_count, 2);
    assert_eq!(table_count(database.pool(), "chain_lineage").await?, 3);
    assert_eq!(table_count(database.pool(), "chain_lineage").await?, 3);
    assert_eq!(table_count(database.pool(), "raw_logs").await?, 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage WHERE block_number = 41")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage WHERE block_number = 41")
            .fetch_one(database.pool())
            .await?,
        1
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn backfill_uses_finalized_safe_and_canonical_evidence_for_admitted_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(9_300);
    let address = "0x0000000000000000000000000000000000000001";

    insert_watched_manifest_contract(
        database.pool(),
        9_300,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
    )
    .await?;

    let block_40 = provider_block(
        "0x4000000000000000000000000000000000000000000000000000000000000040",
        Some("0x3999999999999999999999999999999999999999999999999999999999999939"),
        40,
    );
    let block_41 = provider_block(
        "0x4100000000000000000000000000000000000000000000000000000000000041",
        Some(&block_40.block_hash),
        41,
    );
    let block_42 = provider_block(
        "0x4200000000000000000000000000000000000000000000000000000000000042",
        Some(&block_41.block_hash),
        42,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures_and_heads(
        vec![
            ProviderBlockFixture {
                block: block_40.clone(),
                logs: vec![rpc_log_payload(&block_40)],
            },
            ProviderBlockFixture {
                block: block_41.clone(),
                logs: vec![rpc_log_payload(&block_41)],
            },
            ProviderBlockFixture {
                block: block_42.clone(),
                logs: vec![rpc_log_payload(&block_42)],
            },
        ],
        Arc::clone(&requests),
        Some(41),
        Some(40),
    )
    .await?;

    let range = BackfillBlockRange::new(40, 42)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_wrapper_l1".to_owned()),
        range.from_block,
        range.to_block,
    )
    .await?;
    let outcome = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(range, "canonicality-evidence", "lease-canonicality")?,
    )
    .await?;
    assert_eq!(outcome.raw_log_count, 3);
    assert_eq!(outcome.raw_code_hash_count, 3);

    let expected_states = vec![
        (40, "finalized".to_owned()),
        (41, "safe".to_owned()),
        (42, "canonical".to_owned()),
    ];
    for table in [
        "chain_lineage",
        "chain_lineage",
        "raw_logs",
        "raw_code_hashes",
    ] {
        let states = sqlx::query_as::<_, (i64, String)>(&format!(
            "SELECT block_number, canonicality_state::TEXT FROM {table} ORDER BY block_number"
        ))
        .fetch_all(database.pool())
        .await?;
        assert_eq!(states, expected_states, "{table} canonicality mismatch");
    }

    let payload_states = sqlx::query_as::<_, (i64, Vec<String>)>(
        r#"
        SELECT
            block_number,
            ARRAY_AGG(DISTINCT canonicality_state::TEXT ORDER BY canonicality_state::TEXT)::TEXT[]
        FROM raw_payload_cache_metadata
        GROUP BY block_number
        ORDER BY block_number
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(payload_states, Vec::<(i64, Vec<String>)>::new());

    let normalized_event_states = sqlx::query_as::<_, (i64, String)>(
        r#"
        SELECT block_number, canonicality_state::TEXT
        FROM normalized_events
        WHERE event_kind = 'PreimageObserved'
        ORDER BY block_number
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(normalized_event_states, expected_states);

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn source_family_backfill_persists_selector_identity_and_only_selected_target_facts()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let registry_contract_instance_id = Uuid::from_u128(1_001);
    let registrar_contract_instance_id = Uuid::from_u128(1_002);
    let registry_address = "0x0000000000000000000000000000000000000001";
    let registrar_address = "0x0000000000000000000000000000000000000002";

    insert_watched_manifest_contract(
        database.pool(),
        11,
        "ens",
        "ethereum-mainnet",
        "ens_v2_registry_l1",
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_watched_manifest_contract(
        database.pool(),
        12,
        "ens",
        "ethereum-mainnet",
        "ens_v2_registrar_l1",
        registrar_contract_instance_id,
        registrar_address,
    )
    .await?;

    let range = BackfillBlockRange::new(42, 42)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v2_registry_l1".to_owned()),
        range.from_block,
        range.to_block,
    )
    .await?;
    assert_eq!(
        source_plan.watched_chain_plan.addresses,
        vec![registry_address.to_owned()]
    );

    let block_42 = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
        42,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures(
        vec![ProviderBlockFixture {
            block: block_42.clone(),
            logs: vec![
                rpc_ens_v2_label_registered_log_payload(&block_42, registry_address, "alice", 1, 0),
                rpc_ens_v2_token_resource_log_payload(&block_42, registry_address, 1, 1_001, 1),
                rpc_log_payload_at_address(&block_42, registrar_address, 1),
            ],
        }],
        Arc::clone(&requests),
    )
    .await?;

    let outcome = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(range, "source-family-idempotent", "lease-source-family")?,
    )
    .await?;
    assert_eq!(outcome.raw_log_count, 2);
    assert_eq!(outcome.raw_code_hash_count, 1);
    assert_eq!(table_count(database.pool(), "raw_transactions").await?, 1);
    assert_eq!(table_count(database.pool(), "raw_receipts").await?, 1);
    assert_eq!(
        table_count(database.pool(), "raw_payload_cache_metadata").await?,
        3
    );
    let exact_payload_cache_summary =
        sqlx::query_as::<_, (String, i64, i64, i64, Vec<String>, Vec<String>)>(
            r#"
            SELECT
                payload_kind,
                COUNT(*)::BIGINT,
                COUNT(retained_digest)::BIGINT,
                COUNT(DISTINCT retained_digest)::BIGINT,
                ARRAY_AGG(DISTINCT cache_metadata->>'method' ORDER BY cache_metadata->>'method')::TEXT[],
                ARRAY_AGG(DISTINCT cache_metadata->>'fetch_mode' ORDER BY cache_metadata->>'fetch_mode')::TEXT[]
            FROM raw_payload_cache_metadata
            GROUP BY payload_kind
            ORDER BY payload_kind
            "#,
        )
        .fetch_all(database.pool())
        .await?;
    assert_eq!(
        exact_payload_cache_summary,
        vec![
            (
                provider::RAW_PAYLOAD_KIND_BLOCK_LOGS.to_owned(),
                1,
                1,
                1,
                vec!["eth_getLogs".to_owned()],
                vec!["block_hash".to_owned()],
            ),
            (
                provider::RAW_PAYLOAD_KIND_BLOCK_RECEIPTS.to_owned(),
                1,
                1,
                1,
                vec!["eth_getBlockReceipts".to_owned()],
                vec!["block_hash".to_owned()],
            ),
            (
                provider::RAW_PAYLOAD_KIND_FULL_BLOCK.to_owned(),
                1,
                1,
                1,
                vec!["eth_getBlockByHash".to_owned()],
                vec!["block_hash".to_owned()],
            ),
        ],
        "single-block retained log fetches remain hash-scoped while block and receipt metadata stay hash-scoped"
    );

    let job = load_backfill_job(database.pool(), outcome.backfill_job_id)
        .await?
        .expect("source-family backfill job must exist");
    assert_eq!(
        job.source_identity
            .get("selector_kind")
            .and_then(Value::as_str),
        Some("source_family")
    );
    assert_eq!(
        job.source_identity
            .get("source_family")
            .and_then(Value::as_str),
        Some("ens_v2_registry_l1")
    );
    assert_eq!(
        job.source_identity
            .get("selected_targets")
            .and_then(Value::as_array)
            .and_then(|targets| targets.first())
            .and_then(|target| target.get("address"))
            .and_then(Value::as_str),
        Some(registry_address)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs WHERE emitting_address = $1")
            .bind(registrar_address)
            .fetch_one(database.pool())
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs WHERE emitting_address = $1")
            .bind(registry_address)
            .fetch_one(database.pool())
            .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT emitting_address FROM raw_logs ORDER BY log_index LIMIT 1"
        )
        .fetch_one(database.pool())
        .await?,
        registry_address.to_owned()
    );
    assert_eq!(table_count(database.pool(), "resources").await?, 1);
    assert_eq!(table_count(database.pool(), "name_surfaces").await?, 1);
    assert_eq!(table_count(database.pool(), "surface_bindings").await?, 1);

    let registry_event_counts = sqlx::query_as::<_, (String, i64)>(
        r#"
        SELECT event_kind, COUNT(*)::BIGINT
        FROM normalized_events
        WHERE derivation_kind = 'ens_v2_registry_resource_surface'
        GROUP BY event_kind
        "#,
    )
    .fetch_all(database.pool())
    .await?
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    assert_eq!(
        registry_event_counts.get("TokenResourceLinked"),
        Some(&1),
        "real scoped backfill must run the ENSv2 registry resource/surface adapter"
    );
    assert_eq!(registry_event_counts.get("SurfaceBound"), Some(&1));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_events
            WHERE derivation_kind = 'raw_log_preimage_observation'
              AND event_kind = 'PreimageObserved'
            "#
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    let code_requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .iter()
        .filter(|request| request.method == "eth_getCode")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(code_requests.len(), 1);
    assert_eq!(
        code_requests[0].params.first().and_then(Value::as_str),
        Some(registry_address)
    );
    assert_eq!(
        code_requests[0]
            .params
            .get(1)
            .and_then(Value::as_object)
            .and_then(|selection| selection.get("blockHash"))
            .and_then(Value::as_str),
        Some(block_42.block_hash.as_str())
    );

    let rerun = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(
            range,
            "source-family-idempotent",
            "lease-source-family-rerun",
        )?,
    )
    .await?;
    assert_eq!(rerun.backfill_job_id, outcome.backfill_job_id);
    assert_eq!(rerun.reserved_range_count, 0);

    let registrar_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v2_registrar_l1".to_owned()),
        range.from_block,
        range.to_block,
    )
    .await?;
    let conflict = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &registrar_plan,
        &provider,
        backfill_job_config(
            range,
            "source-family-idempotent",
            "lease-source-family-conflict",
        )?,
    )
    .await
    .expect_err("same idempotency key with different source selector must conflict");
    assert!(
        conflict
            .to_string()
            .contains("does not match requested immutable job identity"),
        "unexpected selector conflict: {conflict:#}"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn frozen_source_family_backfills_lock_wrapper_resolver_and_basenames_l1_identity()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let fixtures = focused_source_family_fixtures();

    for (index, fixture) in fixtures.iter().enumerate() {
        insert_watched_manifest_contract(
            database.pool(),
            30 + i64::try_from(index).context("fixture index must fit i64")?,
            fixture.namespace,
            fixture.chain,
            fixture.source_family,
            fixture.contract_instance_id,
            fixture.address,
        )
        .await?;
    }

    let provider_fixtures = fixtures
        .iter()
        .enumerate()
        .map(|(index, fixture)| {
            let block = provider_block(
                fixture.block_hash,
                Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
                fixture.block_number,
            );
            let logs = if fixture.source_family == "ens_v1_resolver_l1" {
                vec![rpc_resolver_name_changed_log_payload_for_namehash(
                    &block,
                    fixture.address,
                    &namehash_for_dns_name(&dns_encoded_eth_name("focused")),
                    "focused.eth",
                    index as u64,
                )]
            } else {
                vec![rpc_log_payload_at_address(
                    &block,
                    fixture.address,
                    index as i64,
                )]
            };
            ProviderBlockFixture {
                block: block.clone(),
                logs,
            }
        })
        .collect::<Vec<_>>();
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) =
        number_resolving_provider_with_fixtures(provider_fixtures, Arc::clone(&requests)).await?;

    for fixture in fixtures {
        let range = BackfillBlockRange::new(fixture.block_number, fixture.block_number)?;
        let source_plan = load_watched_source_selector_plan(
            database.pool(),
            fixture.chain,
            WatchedSourceSelector::SourceFamily(fixture.source_family.to_owned()),
            range.from_block,
            range.to_block,
        )
        .await?;
        assert_eq!(source_plan.selected_targets.len(), 1);
        let selected_target = &source_plan.selected_targets[0];
        assert_eq!(selected_target.source_family, fixture.source_family);
        assert_eq!(
            selected_target.contract_instance_id,
            fixture.contract_instance_id
        );
        assert_eq!(selected_target.address, fixture.address);
        assert_eq!(selected_target.effective_from_block, fixture.block_number);
        assert_eq!(selected_target.effective_to_block, fixture.block_number);

        let idempotency_key = format!("focused-source-family-{}", fixture.source_family);
        let first_lease = format!("lease-{}-first", fixture.source_family);
        let outcome = run_resumable_hash_pinned_backfill_job(
            database.pool(),
            &source_plan,
            &provider,
            backfill_job_config(range, &idempotency_key, &first_lease)?,
        )
        .await?;
        assert_eq!(outcome.raw_log_count, 1);
        assert_eq!(outcome.raw_code_hash_count, 1);

        let job = load_backfill_job(database.pool(), outcome.backfill_job_id)
            .await?
            .expect("focused source-family backfill job must exist");
        let expected_source_identity =
            backfill::backfill_job_source_identity_payload(&source_plan)?;
        let expected_source_identity_hash = expected_source_identity
            .get("source_identity_hash")
            .and_then(Value::as_str)
            .expect("expected source identity must include hash")
            .to_owned();
        assert_eq!(job.source_identity, expected_source_identity);
        assert_eq!(
            job.source_identity
                .get("source_identity_hash")
                .and_then(Value::as_str),
            Some(expected_source_identity_hash.as_str())
        );
        if fixture.source_family == "ens_v1_resolver_l1" {
            assert_eq!(
                job.source_identity
                    .get("source_identity_payload_format")
                    .and_then(Value::as_str),
                Some("generic_resolver_event_topics_v1")
            );
            assert!(job.source_identity.get("selected_targets").is_none());
        } else {
            assert_eq!(
                job.source_identity
                    .get("selected_targets")
                    .and_then(Value::as_array)
                    .and_then(|targets| targets.first())
                    .and_then(|target| target.get("source_family"))
                    .and_then(Value::as_str),
                Some(fixture.source_family)
            );
        }

        let repeat_lease = format!("lease-{}-repeat", fixture.source_family);
        let rerun = run_resumable_hash_pinned_backfill_job(
            database.pool(),
            &source_plan,
            &provider,
            backfill_job_config(range, &idempotency_key, &repeat_lease)?,
        )
        .await?;
        assert_eq!(rerun.backfill_job_id, outcome.backfill_job_id);
        assert_eq!(rerun.reserved_range_count, 0);
        assert_eq!(rerun.completed_range_count, 0);
        assert_eq!(rerun.resolved_block_count, 0);
    }

    let compat = focused_source_family_fixture("basenames_l1_compat");
    let execution = focused_source_family_fixture("basenames_execution");
    assert_eq!(compat.address, execution.address);
    assert_ne!(compat.contract_instance_id, execution.contract_instance_id);

    let conflict_range = BackfillBlockRange::new(compat.block_number, compat.block_number)?;
    let compat_plan = load_watched_source_selector_plan(
        database.pool(),
        compat.chain,
        WatchedSourceSelector::SourceFamily(compat.source_family.to_owned()),
        conflict_range.from_block,
        conflict_range.to_block,
    )
    .await?;
    let execution_plan = load_watched_source_selector_plan(
        database.pool(),
        execution.chain,
        WatchedSourceSelector::SourceFamily(execution.source_family.to_owned()),
        conflict_range.from_block,
        conflict_range.to_block,
    )
    .await?;
    let l1_lock = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &compat_plan,
        &provider,
        backfill_job_config(
            conflict_range,
            "basenames-l1-same-address-lock",
            "lease-l1-lock",
        )?,
    )
    .await?;
    let conflict = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &execution_plan,
        &provider,
        backfill_job_config(
            conflict_range,
            "basenames-l1-same-address-lock",
            "lease-l1-lock-conflict",
        )?,
    )
    .await
    .expect_err("same idempotency key must not collapse same-address source families");
    assert!(
        conflict
            .to_string()
            .contains("does not match requested immutable job identity"),
        "unexpected same-address source-family conflict: {conflict:#}"
    );
    let l1_lock_job = load_backfill_job(database.pool(), l1_lock.backfill_job_id)
        .await?
        .expect("same-address source-family lock job must exist");
    assert_eq!(
        l1_lock_job
            .source_identity
            .get("source_family")
            .and_then(Value::as_str),
        Some("basenames_l1_compat")
    );

    assert_eq!(table_count(database.pool(), "raw_logs").await?, 4);
    assert_eq!(table_count(database.pool(), "raw_code_hashes").await?, 4);

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn source_scoped_backfill_dynamic_resolver_ensv1_scans_generic_resolver_events() -> Result<()>
{
    assert_dynamic_resolver_backfill_scope_behavior(DynamicResolverBackfillFixture {
        namespace: "ens",
        chain: "ethereum-mainnet",
        deployment_epoch: "ens_v1",
        registry_source_family: "ens_v1_registry_l1",
        resolver_source_family: "ens_v1_resolver_l1",
        manifest_id_base: 401,
        uuid_base: 4_100,
        idempotency_key: "dynamic-resolver-ensv1-selected-target-lock",
    })
    .await
}

#[tokio::test]
async fn source_scoped_backfill_dynamic_resolver_basenames_selected_targets_are_range_locked()
-> Result<()> {
    assert_dynamic_resolver_backfill_scope_behavior(DynamicResolverBackfillFixture {
        namespace: "basenames",
        chain: "base-mainnet",
        deployment_epoch: "basenames_v1",
        registry_source_family: "basenames_base_registry",
        resolver_source_family: "basenames_base_resolver",
        manifest_id_base: 501,
        uuid_base: 5_100,
        idempotency_key: "dynamic-resolver-basenames-selected-target-lock",
    })
    .await
}

#[tokio::test]
async fn source_scoped_backfill_enforces_selected_target_effective_ranges_during_intake()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let first_contract_instance_id = Uuid::from_u128(1_101);
    let second_contract_instance_id = Uuid::from_u128(1_102);
    let first_address = "0x0000000000000000000000000000000000000011";
    let second_address = "0x0000000000000000000000000000000000000012";

    insert_watched_manifest_contract(
        database.pool(),
        101,
        "ens",
        "ethereum-mainnet",
        "ens_v2_registry_l1",
        first_contract_instance_id,
        first_address,
    )
    .await?;
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
        second_address,
        Some(101),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        101,
        "registry",
        second_contract_instance_id,
        second_address,
        "none",
        None,
        None,
    )
    .await?;
    set_contract_instance_address_range(
        database.pool(),
        first_contract_instance_id,
        Some(42),
        Some(42),
    )
    .await?;
    set_contract_instance_address_range(
        database.pool(),
        second_contract_instance_id,
        Some(43),
        Some(43),
    )
    .await?;

    let range = BackfillBlockRange::new(42, 43)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v2_registry_l1".to_owned()),
        range.from_block,
        range.to_block,
    )
    .await?;
    assert_eq!(source_plan.selected_targets.len(), 2);
    assert_eq!(
        source_plan
            .selected_targets
            .iter()
            .map(|target| (
                target.address.as_str(),
                target.effective_from_block,
                target.effective_to_block
            ))
            .collect::<BTreeSet<_>>(),
        [(first_address, 42, 42), (second_address, 43, 43)]
            .into_iter()
            .collect::<BTreeSet<_>>()
    );

    let block_42 = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
        42,
    );
    let block_43 = provider_block(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        43,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures(
        vec![
            ProviderBlockFixture {
                block: block_42.clone(),
                logs: vec![
                    rpc_log_payload_at_address(&block_42, first_address, 0),
                    rpc_log_payload_at_address(&block_42, second_address, 1),
                ],
            },
            ProviderBlockFixture {
                block: block_43.clone(),
                logs: vec![
                    rpc_log_payload_at_address(&block_43, first_address, 0),
                    rpc_log_payload_at_address(&block_43, second_address, 1),
                ],
            },
        ],
        Arc::clone(&requests),
    )
    .await?;

    let outcome = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(range, "source-effective-ranges", "lease-effective")?,
    )
    .await?;
    assert_eq!(outcome.raw_log_count, 2);
    assert_eq!(outcome.raw_code_hash_count, 2);
    assert_eq!(
        sqlx::query_as::<_, (Vec<i64>, Vec<String>)>(
            "SELECT ARRAY_AGG(block_number ORDER BY block_number), ARRAY_AGG(emitting_address ORDER BY block_number) FROM raw_logs"
        )
        .fetch_one(database.pool())
        .await?,
        (
            vec![42, 43],
            vec![first_address.to_owned(), second_address.to_owned()]
        )
    );
    assert_eq!(
        sqlx::query_as::<_, (Vec<i64>, Vec<String>)>(
            "SELECT ARRAY_AGG(block_number ORDER BY block_number), ARRAY_AGG(contract_address ORDER BY block_number) FROM raw_code_hashes"
        )
        .fetch_one(database.pool())
        .await?,
        (
            vec![42, 43],
            vec![first_address.to_owned(), second_address.to_owned()]
        )
    );

    let recorded_requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    let log_requests = recorded_requests
        .iter()
        .filter(|request| request.method == "eth_getLogs")
        .collect::<Vec<_>>();
    assert_eq!(
        log_requests.len(),
        2,
        "selected address changes must split log ranges rather than widening the address set"
    );
    let log_filters = log_requests
        .iter()
        .map(|request| {
            assert_eq!(request.batch_size, 1);
            request
                .params
                .first()
                .and_then(Value::as_object)
                .expect("log request must include a filter object")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        log_filters
            .iter()
            .map(|filter| (
                filter.get("fromBlock").and_then(Value::as_str),
                filter.get("toBlock").and_then(Value::as_str),
                filter.get("address").and_then(Value::as_array),
                filter.get("blockHash").and_then(Value::as_str),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                Some("0x2a"),
                Some("0x2a"),
                Some(&vec![Value::String(first_address.to_owned())]),
                None,
            ),
            (
                Some("0x2b"),
                Some("0x2b"),
                Some(&vec![Value::String(second_address.to_owned())]),
                None,
            ),
        ]
    );

    let code_requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .iter()
        .filter(|request| request.method == "eth_getCode")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(code_requests.len(), 2);
    assert_eq!(
        code_requests
            .iter()
            .map(|request| request.params.first().and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec![Some(first_address), Some(second_address)]
    );
    assert_eq!(
        code_requests
            .iter()
            .map(|request| {
                request
                    .params
                    .get(1)
                    .and_then(Value::as_object)
                    .and_then(|selection| selection.get("blockHash"))
                    .and_then(Value::as_str)
            })
            .collect::<Vec<_>>(),
        vec![
            Some(block_42.block_hash.as_str()),
            Some(block_43.block_hash.as_str())
        ]
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn source_scoped_backfill_ensv1_registry_syncs_current_and_old_targets_safely() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let registry_contract_instance_id = Uuid::from_u128(1_301);
    let registry_old_contract_instance_id = Uuid::from_u128(1_302);
    let registry_address = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
    let registry_old_address = "0x314159265dd8dbb310642f98f50c066173c1259b";

    insert_manifest_version_with_source_family(
        database.pool(),
        131,
        "ens",
        "ethereum-mainnet",
        "ens_v1_registry_l1",
    )
    .await?;
    for (contract_instance_id, role, address, active_from) in [
        (
            registry_contract_instance_id,
            "registry",
            registry_address,
            43,
        ),
        (
            registry_old_contract_instance_id,
            "registry_old",
            registry_old_address,
            41,
        ),
    ] {
        insert_contract_instance(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_active_contract_instance_address(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            address,
            Some(131),
        )
        .await?;
        set_contract_instance_address_range(
            database.pool(),
            contract_instance_id,
            Some(active_from),
            None,
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            131,
            role,
            contract_instance_id,
            address,
            "none",
            None,
            None,
        )
        .await?;
    }
    insert_manifest_root_contract_instance(
        database.pool(),
        131,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_discovery_rule(
        database.pool(),
        131,
        "subregistry",
        "registry",
        "reachable_from_root",
    )
    .await?;

    let range = BackfillBlockRange::new(41, 43)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_registry_l1".to_owned()),
        range.from_block,
        range.to_block,
    )
    .await?;
    assert_eq!(
        source_plan
            .selected_targets
            .iter()
            .map(|target| (
                target.address.as_str(),
                target.effective_from_block,
                target.effective_to_block
            ))
            .collect::<Vec<_>>(),
        vec![(registry_address, 43, 43), (registry_old_address, 41, 43),]
    );

    let old_block = provider_block(
        "0x1313131313131313131313131313131313131313131313131313131313131313",
        Some("0x1212121212121212121212121212121212121212121212121212121212121212"),
        41,
    );
    let gap_block = provider_block(
        "0x1515151515151515151515151515151515151515151515151515151515151515",
        Some(&old_block.block_hash),
        42,
    );
    let current_block = provider_block(
        "0x1414141414141414141414141414141414141414141414141414141414141414",
        Some(&gap_block.block_hash),
        43,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures(
        vec![
            ProviderBlockFixture {
                block: old_block.clone(),
                logs: vec![rpc_registry_new_owner_log_payload(
                    &old_block,
                    registry_old_address,
                    "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "eth",
                    "0x0000000000000000000000000000000000000001",
                    0,
                )],
            },
            ProviderBlockFixture {
                block: gap_block,
                logs: Vec::new(),
            },
            ProviderBlockFixture {
                block: current_block.clone(),
                logs: vec![
                    rpc_registry_new_owner_log_payload(
                        &current_block,
                        registry_address,
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "eth",
                        "0x0000000000000000000000000000000000000002",
                        0,
                    ),
                    rpc_registry_new_owner_log_payload(
                        &current_block,
                        registry_old_address,
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "eth",
                        "0x0000000000000000000000000000000000000003",
                        1,
                    ),
                ],
            },
        ],
        Arc::clone(&requests),
    )
    .await?;

    let outcome = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(
            range,
            "ensv1-registry-old-adapter-guard",
            "lease-ensv1-registry",
        )?,
    )
    .await?;
    assert_eq!(outcome.raw_log_count, 3);
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            r#"
            SELECT COALESCE(
                ARRAY_AGG(emitting_address ORDER BY block_number, log_index),
                ARRAY[]::TEXT[]
            )
            FROM raw_logs
            "#
        )
        .fetch_one(database.pool())
        .await?,
        vec![
            registry_old_address.to_owned(),
            registry_address.to_owned(),
            registry_old_address.to_owned(),
        ]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT raw_fact_ref->>'emitting_address'
            FROM normalized_events
            WHERE event_kind = 'SubregistryChanged'
              AND derivation_kind = 'ens_v1_subregistry_changed'
            "#
        )
        .fetch_one(database.pool())
        .await?,
        registry_address.to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT after_state->>'owner'
            FROM normalized_events
            WHERE event_kind = 'SubregistryChanged'
              AND derivation_kind = 'ens_v1_subregistry_changed'
            "#
        )
        .fetch_one(database.pool())
        .await?,
        "0x0000000000000000000000000000000000000002".to_owned()
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn source_scoped_backfill_does_not_normalize_preexisting_unselected_raw_logs() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let selected_contract_instance_id = Uuid::from_u128(1_201);
    let unselected_contract_instance_id = Uuid::from_u128(1_202);
    let selected_address = "0x0000000000000000000000000000000000000021";
    let unselected_address = "0x0000000000000000000000000000000000000022";

    insert_watched_manifest_contract(
        database.pool(),
        121,
        "ens",
        "ethereum-mainnet",
        "ens_v2_registry_l1",
        selected_contract_instance_id,
        selected_address,
    )
    .await?;
    insert_watched_manifest_contract(
        database.pool(),
        122,
        "ens",
        "ethereum-mainnet",
        "ens_v2_registrar_l1",
        unselected_contract_instance_id,
        unselected_address,
    )
    .await?;

    let block_42 = provider_block(
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
        42,
    );
    insert_raw_name_wrapped_log_at_address(
        database.pool(),
        "ethereum-mainnet",
        &block_42,
        unselected_address,
        7,
    )
    .await?;

    let range = BackfillBlockRange::new(42, 42)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v2_registry_l1".to_owned()),
        range.from_block,
        range.to_block,
    )
    .await?;
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures(
        vec![ProviderBlockFixture {
            block: block_42.clone(),
            logs: vec![rpc_ens_v2_label_registered_log_payload(
                &block_42,
                selected_address,
                "selected",
                1,
                0,
            )],
        }],
        Arc::clone(&requests),
    )
    .await?;

    let outcome = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(range, "source-scoped-sync", "lease-scoped-sync")?,
    )
    .await?;
    assert_eq!(outcome.raw_log_count, 1);
    assert_eq!(table_count(database.pool(), "raw_logs").await?, 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE raw_fact_ref->>'emitting_address' = $1"
        )
        .bind(selected_address)
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE raw_fact_ref->>'emitting_address' = $1"
        )
        .bind(unselected_address)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        2
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn explicit_watched_targets_are_sorted_idempotent_and_validated() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let registry_contract_instance_id = Uuid::from_u128(2_001);
    let registrar_contract_instance_id = Uuid::from_u128(2_002);
    let registry_address = "0x0000000000000000000000000000000000000001";
    let registrar_address = "0x0000000000000000000000000000000000000002";

    insert_watched_manifest_contract(
        database.pool(),
        21,
        "ens",
        "ethereum-mainnet",
        "ens_v2_registry_l1",
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_watched_manifest_contract(
        database.pool(),
        22,
        "ens",
        "ethereum-mainnet",
        "ens_v2_registrar_l1",
        registrar_contract_instance_id,
        registrar_address,
    )
    .await?;

    let range = BackfillBlockRange::new(42, 42)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::WatchedTargetSet(vec![
            WatchedTargetIdentity {
                contract_instance_id: registrar_contract_instance_id,
            },
            WatchedTargetIdentity {
                contract_instance_id: registry_contract_instance_id,
            },
            WatchedTargetIdentity {
                contract_instance_id: registrar_contract_instance_id,
            },
        ]),
        range.from_block,
        range.to_block,
    )
    .await?;
    assert_eq!(
        source_plan.requested_watched_targets,
        vec![
            WatchedTargetIdentity {
                contract_instance_id: registry_contract_instance_id,
            },
            WatchedTargetIdentity {
                contract_instance_id: registrar_contract_instance_id,
            },
        ]
    );

    let block_42 = provider_block(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
        42,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) =
        number_resolving_provider(vec![block_42.clone()], Arc::clone(&requests)).await?;

    let outcome = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(range, "explicit-target-idempotent", "lease-explicit")?,
    )
    .await?;
    let job = load_backfill_job(database.pool(), outcome.backfill_job_id)
        .await?
        .expect("explicit watched-target backfill job must exist");
    assert_eq!(
        job.source_identity
            .get("selector_kind")
            .and_then(Value::as_str),
        Some("watched_target_set")
    );
    assert_eq!(
        job.source_identity
            .get("requested_watched_targets")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(2)
    );
    assert_eq!(
        job.source_identity
            .get("selected_targets")
            .and_then(Value::as_array)
            .and_then(|targets| targets.first())
            .and_then(|target| target.get("source_family"))
            .and_then(Value::as_str),
        Some("ens_v2_registrar_l1")
    );
    assert_eq!(outcome.raw_code_hash_count, 2);

    let reordered_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::WatchedTargetSet(vec![
            WatchedTargetIdentity {
                contract_instance_id: registry_contract_instance_id,
            },
            WatchedTargetIdentity {
                contract_instance_id: registrar_contract_instance_id,
            },
        ]),
        range.from_block,
        range.to_block,
    )
    .await?;
    let rerun = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &reordered_plan,
        &provider,
        backfill_job_config(range, "explicit-target-idempotent", "lease-explicit-rerun")?,
    )
    .await?;
    assert_eq!(rerun.backfill_job_id, outcome.backfill_job_id);
    assert_eq!(rerun.reserved_range_count, 0);

    let narrowed_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::WatchedTargetSet(vec![WatchedTargetIdentity {
            contract_instance_id: registry_contract_instance_id,
        }]),
        range.from_block,
        range.to_block,
    )
    .await?;
    let conflict = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &narrowed_plan,
        &provider,
        backfill_job_config(
            range,
            "explicit-target-idempotent",
            "lease-explicit-conflict",
        )?,
    )
    .await
    .expect_err("same idempotency key with changed explicit target set must conflict");
    assert!(
        conflict
            .to_string()
            .contains("does not match requested immutable job identity"),
        "unexpected explicit target conflict: {conflict:#}"
    );

    let invalid_family = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("missing_family".to_owned()),
        range.from_block,
        range.to_block,
    )
    .await
    .expect_err("unknown source family must fail before job creation");
    assert!(
        invalid_family
            .to_string()
            .contains("source_family missing_family found no active watched targets"),
        "unexpected invalid source-family error: {invalid_family:#}"
    );

    let invalid_target = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::WatchedTargetSet(vec![WatchedTargetIdentity {
            contract_instance_id: Uuid::from_u128(9_999),
        }]),
        range.from_block,
        range.to_block,
    )
    .await
    .expect_err("unknown watched target must fail before job creation");
    assert!(
        invalid_target
            .to_string()
            .contains("is not active for chain ethereum-mainnet"),
        "unexpected invalid watched-target error: {invalid_target:#}"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn hash_pinned_backfill_fails_missing_hash_payload_without_number_fallback() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let request_log = Arc::clone(&requests);

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
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(RecordedRpcRequest {
                method: method.to_owned(),
                params: params.clone(),
                http_request_id: json_rpc_test_http_request_id(&body),
                batch_size: json_rpc_test_batch_size(&body),
            });

        let result = match method {
            "eth_getBlockByNumber" => {
                let selection = params
                    .first()
                    .and_then(Value::as_str)
                    .expect("block number or tag parameter must be present");
                match selection {
                    "latest" | "0x2a" => {
                        rpc_block_bundle_payload(&provider_block(block_hash, None, 42))
                    }
                    "safe" | "finalized" => Value::Null,
                    _ => panic!("unexpected block selection: {body}"),
                }
            }
            "eth_getBlockByHash" => {
                if params.get(1) == Some(&Value::Bool(false)) {
                    rpc_block_bundle_payload(&provider_block(block_hash, None, 42))
                } else {
                    Value::Null
                }
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
    let provider = provider::JsonRpcProvider::new(&url)?;
    let source_plan = WatchedSourceSelectorPlan {
        chain: "ethereum-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::WholeActiveWatchedChain,
        source_family: None,
        requested_watched_targets: Vec::new(),
        selected_targets: Vec::new(),
        watched_chain_plan: WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: Vec::new(),
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 0,
            discovery_edge_entry_count: 0,
        },
    };

    let error = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(
            BackfillBlockRange::new(42, 42)?,
            "indexer-backfill-missing-hash",
            "lease-fail",
        )?,
    )
    .await
    .expect_err("missing hash-scoped block payload must fail");
    assert!(
        format!("{error:#}").contains(
            "provider did not return block 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ),
        "unexpected error: {error:#}"
    );

    let backfill_job_id = sqlx::query_scalar::<_, i64>(
        "SELECT backfill_job_id FROM backfill_jobs WHERE idempotency_key = $1",
    )
    .bind("indexer-backfill-missing-hash")
    .fetch_one(database.pool())
    .await?;
    let job = load_backfill_job(database.pool(), backfill_job_id)
        .await?
        .expect("failed backfill job must exist");
    assert_eq!(job.status, BackfillLifecycleStatus::Failed);
    assert_eq!(
        job.failure_reason.as_deref(),
        Some("hash-pinned backfill failed")
    );
    assert_eq!(
        job.failure_metadata.get("phase").and_then(Value::as_str),
        Some("hash_pinned_intake")
    );

    let ranges = load_backfill_ranges(database.pool(), backfill_job_id).await?;
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].status, BackfillLifecycleStatus::Failed);
    assert_eq!(
        ranges[0].failure_reason.as_deref(),
        Some("hash-pinned backfill failed")
    );
    assert_eq!(ranges[0].range_start_block_number, 42);
    assert_eq!(ranges[0].range_end_block_number, 42);
    assert_eq!(ranges[0].checkpoint_block_number, 41);
    assert_eq!(ranges[0].attempt_count, 1);
    assert_eq!(
        ranges[0]
            .failure_metadata
            .get("block_number")
            .and_then(Value::as_i64),
        Some(42)
    );
    assert!(
        ranges[0]
            .failure_metadata
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("provider did not return block"),
        "unexpected failure metadata: {}",
        ranges[0].failure_metadata
    );

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        vec![
            "eth_getBlockByNumber",
            "eth_getBlockByNumber",
            "eth_getBlockByNumber",
            "eth_getBlockByHash",
            "eth_getBlockByNumber",
            "eth_getBlockByHash"
        ]
    );

    server.abort();
    database.cleanup().await
}

async fn number_resolving_provider(
    blocks: Vec<ProviderBlock>,
    requests: Arc<Mutex<Vec<RecordedRpcRequest>>>,
) -> Result<(provider::JsonRpcProvider, JoinHandle<()>)> {
    number_resolving_provider_with_fixtures(
        blocks
            .into_iter()
            .map(|block| ProviderBlockFixture {
                logs: vec![rpc_log_payload(&block)],
                block,
            })
            .collect(),
        requests,
    )
    .await
}

async fn number_resolving_provider_with_fixtures(
    fixtures: Vec<ProviderBlockFixture>,
    requests: Arc<Mutex<Vec<RecordedRpcRequest>>>,
) -> Result<(provider::JsonRpcProvider, JoinHandle<()>)> {
    number_resolving_provider_with_fixtures_and_heads(fixtures, requests, None, None).await
}

async fn number_resolving_provider_with_fixtures_and_heads(
    fixtures: Vec<ProviderBlockFixture>,
    requests: Arc<Mutex<Vec<RecordedRpcRequest>>>,
    safe_block_number: Option<i64>,
    finalized_block_number: Option<i64>,
) -> Result<(provider::JsonRpcProvider, JoinHandle<()>)> {
    number_resolving_provider_with_fixtures_and_heads_and_delay(
        fixtures,
        requests,
        safe_block_number,
        finalized_block_number,
        None,
    )
    .await
}

async fn number_resolving_provider_with_fixtures_and_heads_and_delay(
    fixtures: Vec<ProviderBlockFixture>,
    requests: Arc<Mutex<Vec<RecordedRpcRequest>>>,
    safe_block_number: Option<i64>,
    finalized_block_number: Option<i64>,
    response_delay_once: Option<StdDuration>,
) -> Result<(provider::JsonRpcProvider, JoinHandle<()>)> {
    let fixtures_by_hash = Arc::new(
        fixtures
            .into_iter()
            .map(|fixture| (fixture.block.block_hash.clone(), fixture))
            .collect::<BTreeMap<_, _>>(),
    );
    let hashes_by_number = Arc::new(
        fixtures_by_hash
            .values()
            .map(|fixture| (fixture.block.block_number, fixture.block.block_hash.clone()))
            .collect::<BTreeMap<_, _>>(),
    );
    let latest_hash = hashes_by_number
        .iter()
        .next_back()
        .map(|(_, hash)| hash.clone())
        .context("backfill provider fixture must include a latest block")?;
    let safe_hash = safe_block_number
        .map(|block_number| {
            hashes_by_number
                .get(&block_number)
                .cloned()
                .with_context(|| format!("safe block fixture {block_number} is missing"))
        })
        .transpose()?;
    let finalized_hash = finalized_block_number
        .map(|block_number| {
            hashes_by_number
                .get(&block_number)
                .cloned()
                .with_context(|| format!("finalized block fixture {block_number} is missing"))
        })
        .transpose()?;
    let response_delay_once =
        response_delay_once.map(|delay| Arc::new((AtomicBool::new(true), delay)));

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        if let Some(delay_once) = &response_delay_once {
            if delay_once
                .0
                .swap(false, std::sync::atomic::Ordering::Relaxed)
            {
                std::thread::sleep(delay_once.1);
            }
        }

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
            .push(RecordedRpcRequest {
                method: method.to_owned(),
                params: params.clone(),
                http_request_id: json_rpc_test_http_request_id(&body),
                batch_size: json_rpc_test_batch_size(&body),
            });

        let result = match method {
            "eth_getBlockByNumber" => {
                assert_eq!(params.get(1), Some(&Value::Bool(false)));
                let selection = params
                    .first()
                    .and_then(Value::as_str)
                    .expect("block number or tag parameter must be present");
                match selection {
                    "latest" => {
                        let fixture = fixtures_by_hash
                            .get(&latest_hash)
                            .expect("latest hash must point at a fixture block");
                        rpc_block_bundle_payload(&fixture.block)
                    }
                    "safe" => match &safe_hash {
                        Some(block_hash) => {
                            let fixture = fixtures_by_hash
                                .get(block_hash)
                                .expect("safe hash must point at a fixture block");
                            rpc_block_bundle_payload(&fixture.block)
                        }
                        None => Value::Null,
                    },
                    "finalized" => match &finalized_hash {
                        Some(block_hash) => {
                            let fixture = fixtures_by_hash
                                .get(block_hash)
                                .expect("finalized hash must point at a fixture block");
                            rpc_block_bundle_payload(&fixture.block)
                        }
                        None => Value::Null,
                    },
                    block_number => {
                        let block_number = parse_rpc_block_number(block_number);
                        let block_hash = hashes_by_number
                            .get(&block_number)
                            .unwrap_or_else(|| panic!("unexpected block number request: {body}"));
                        let fixture = fixtures_by_hash
                            .get(block_hash)
                            .expect("number index must point at a fixture block");
                        rpc_block_bundle_payload(&fixture.block)
                    }
                }
            }
            "eth_getBlockByHash" => {
                let block_hash = params
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let fixture = fixtures_by_hash
                    .get(&block_hash)
                    .unwrap_or_else(|| panic!("unexpected block hash request: {body}"));
                rpc_block_bundle_payload(&fixture.block)
            }
            "eth_getLogs" => {
                let filter = params
                    .first()
                    .and_then(Value::as_object)
                    .expect("log request must include a filter object");
                logs_for_backfill_filter(filter, &fixtures_by_hash, &hashes_by_number)
            }
            "eth_getBlockReceipts" => {
                let block_hash = params
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let fixture = fixtures_by_hash
                    .get(&block_hash)
                    .unwrap_or_else(|| panic!("unexpected receipt request: {body}"));
                Value::Array(vec![rpc_receipt_payload(&fixture.block)])
            }
            "eth_getTransactionByHash" => {
                let transaction_hash = params
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let fixture = fixtures_by_hash
                    .values()
                    .find(|fixture| transaction_hash_for_block(&fixture.block) == transaction_hash)
                    .unwrap_or_else(|| panic!("unexpected transaction request: {body}"));
                rpc_transaction_payload(&fixture.block)
            }
            "eth_getTransactionReceipt" => {
                let transaction_hash = params
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let fixture = fixtures_by_hash
                    .values()
                    .find(|fixture| transaction_hash_for_block(&fixture.block) == transaction_hash)
                    .unwrap_or_else(|| panic!("unexpected transaction receipt request: {body}"));
                rpc_receipt_payload(&fixture.block)
            }
            "eth_getCode" => {
                let block_hash = params
                    .get(1)
                    .and_then(Value::as_object)
                    .and_then(|selection| selection.get("blockHash"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                assert!(
                    fixtures_by_hash.contains_key(&block_hash),
                    "unexpected code block selection: {body}"
                );
                Value::String("0x6001600155".to_owned())
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

fn logs_for_backfill_filter(
    filter: &serde_json::Map<String, Value>,
    fixtures_by_hash: &BTreeMap<String, ProviderBlockFixture>,
    hashes_by_number: &BTreeMap<i64, String>,
) -> Value {
    let address_filter = log_filter_addresses(filter);
    let topic0_filter = log_filter_topic0s(filter);
    let mut logs = Vec::new();

    if let Some(block_hash) = filter.get("blockHash").and_then(Value::as_str) {
        let fixture = fixtures_by_hash
            .get(&block_hash.to_ascii_lowercase())
            .unwrap_or_else(|| panic!("unexpected log blockHash filter: {filter:?}"));
        logs.extend(filtered_fixture_logs(
            fixture,
            address_filter.as_ref(),
            topic0_filter.as_ref(),
        ));
    } else {
        let from_block = filter
            .get("fromBlock")
            .and_then(Value::as_str)
            .map(parse_rpc_block_number)
            .expect("range log filter must include fromBlock");
        let to_block = filter
            .get("toBlock")
            .and_then(Value::as_str)
            .map(parse_rpc_block_number)
            .expect("range log filter must include toBlock");
        assert!(
            from_block <= to_block,
            "range log filter start must not exceed end: {filter:?}"
        );

        for block_number in from_block..=to_block {
            let block_hash = hashes_by_number
                .get(&block_number)
                .unwrap_or_else(|| panic!("unexpected log range block: {filter:?}"));
            let fixture = fixtures_by_hash
                .get(block_hash)
                .expect("number index must point at a fixture block");
            logs.extend(filtered_fixture_logs(
                fixture,
                address_filter.as_ref(),
                topic0_filter.as_ref(),
            ));
        }
    }

    Value::Array(logs)
}

fn log_filter_addresses(filter: &serde_json::Map<String, Value>) -> Option<BTreeSet<String>> {
    let addresses = filter.get("address")?;
    let addresses = match addresses {
        Value::String(address) => vec![address.to_ascii_lowercase()],
        Value::Array(addresses) => addresses
            .iter()
            .map(|address| {
                address
                    .as_str()
                    .expect("log address filter values must be strings")
                    .to_ascii_lowercase()
            })
            .collect(),
        value => panic!("unexpected log address filter: {value:?}"),
    };

    Some(addresses.into_iter().collect())
}

fn log_filter_topic0s(filter: &serde_json::Map<String, Value>) -> Option<BTreeSet<String>> {
    let topics = filter.get("topics")?.as_array()?;
    let topic0 = topics.first()?;
    let values = match topic0 {
        Value::String(topic) => vec![topic.to_ascii_lowercase()],
        Value::Array(topics) => topics
            .iter()
            .map(|topic| {
                topic
                    .as_str()
                    .expect("log topic filter values must be strings")
                    .to_ascii_lowercase()
            })
            .collect(),
        Value::Null => return None,
        value => panic!("unexpected log topic0 filter: {value:?}"),
    };

    Some(values.into_iter().collect())
}

fn filtered_fixture_logs(
    fixture: &ProviderBlockFixture,
    address_filter: Option<&BTreeSet<String>>,
    topic0_filter: Option<&BTreeSet<String>>,
) -> Vec<Value> {
    fixture
        .logs
        .iter()
        .filter(|log| {
            let Some(address_filter) = address_filter else {
                return true;
            };
            log.get("address")
                .and_then(Value::as_str)
                .map(|address| address_filter.contains(&address.to_ascii_lowercase()))
                .unwrap_or(false)
        })
        .filter(|log| {
            let Some(topic0_filter) = topic0_filter else {
                return true;
            };
            log.get("topics")
                .and_then(Value::as_array)
                .and_then(|topics| topics.first())
                .and_then(Value::as_str)
                .map(|topic0| topic0_filter.contains(&topic0.to_ascii_lowercase()))
                .unwrap_or(false)
        })
        .cloned()
        .collect()
}

fn rpc_log_payload_at_address(block: &ProviderBlock, address: &str, log_index: i64) -> Value {
    let mut payload = rpc_log_payload(block);
    let fields = payload
        .as_object_mut()
        .expect("test log payload must be a JSON object");
    fields.insert("address".to_owned(), Value::String(address.to_owned()));
    fields.insert(
        "logIndex".to_owned(),
        Value::String(format!("0x{log_index:x}")),
    );
    payload
}

fn rpc_registry_new_owner_log_payload(
    block: &ProviderBlock,
    address: &str,
    parent_node: &str,
    label: &str,
    owner: &str,
    log_index: i64,
) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            ens_v1_new_owner_topic0(),
            parent_node,
            labelhash_hex(label),
        ],
        "data": hex_string(&abi_word_address(owner)),
    })
}

fn rpc_ens_v2_label_registered_log_payload(
    block: &ProviderBlock,
    address: &str,
    label: &str,
    token_id: u64,
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
            ens_v2_label_registered_topic0(),
            hex_string(&abi_word_u64(token_id)),
            labelhash_hex(label),
            hex_string(&abi_word_address("0x0000000000000000000000000000000000000dad"))
        ],
        "data": encode_ens_v2_label_registered_log_data(
            label,
            "0x0000000000000000000000000000000000000a11",
            1_900_000_000,
        )
    })
}

fn rpc_ens_v2_token_resource_log_payload(
    block: &ProviderBlock,
    address: &str,
    token_id: u64,
    resource: u64,
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
            ens_v2_token_resource_topic0(),
            hex_string(&abi_word_u64(token_id)),
            hex_string(&abi_word_u64(resource))
        ],
        "data": "0x"
    })
}

async fn insert_raw_name_wrapped_log_at_address(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    emitting_address: &str,
    log_index: i64,
) -> Result<()> {
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
            canonicality_state: CanonicalityState::Observed,
        }],
    )
    .await?;

    Ok(())
}

async fn insert_manifest_version_with_source_family(
    pool: &PgPool,
    manifest_id: i64,
    namespace: &str,
    chain: &str,
    source_family: &str,
) -> Result<()> {
    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                namespace,
                source_family,
                chain,
                rollout_status
            )
            VALUES ($1, $2, $3, $4, 'active')
            "#,
    )
    .bind(manifest_id)
    .bind(namespace)
    .bind(source_family)
    .bind(chain)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to insert active manifest {manifest_id} for {chain}:{source_family}")
    })?;

    Ok(())
}

async fn insert_watched_manifest_contract(
    pool: &PgPool,
    manifest_id: i64,
    namespace: &str,
    chain: &str,
    source_family: &str,
    contract_instance_id: Uuid,
    address: &str,
) -> Result<()> {
    insert_manifest_version_with_source_family(pool, manifest_id, namespace, chain, source_family)
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

fn focused_source_family_fixtures() -> [FocusedSourceFamilyFixture; 4] {
    [
        FocusedSourceFamilyFixture {
            namespace: "ens",
            chain: "ethereum-mainnet",
            source_family: "ens_v1_wrapper_l1",
            contract_instance_id: Uuid::from_u128(3_001),
            address: "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401",
            block_number: 42,
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
        FocusedSourceFamilyFixture {
            namespace: "ens",
            chain: "ethereum-mainnet",
            source_family: "ens_v1_resolver_l1",
            contract_instance_id: Uuid::from_u128(3_002),
            address: "0xf29100983e058b709f3d539b0c765937b804ac15",
            block_number: 43,
            block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        },
        FocusedSourceFamilyFixture {
            namespace: "basenames",
            chain: "ethereum-mainnet",
            source_family: "basenames_l1_compat",
            contract_instance_id: Uuid::from_u128(3_003),
            address: "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31",
            block_number: 44,
            block_hash: "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        },
        FocusedSourceFamilyFixture {
            namespace: "basenames",
            chain: "ethereum-mainnet",
            source_family: "basenames_execution",
            contract_instance_id: Uuid::from_u128(3_004),
            address: "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31",
            block_number: 45,
            block_hash: "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        },
    ]
}

fn focused_source_family_fixture(source_family: &str) -> FocusedSourceFamilyFixture {
    focused_source_family_fixtures()
        .into_iter()
        .find(|fixture| fixture.source_family == source_family)
        .expect("focused source-family fixture must exist")
}

async fn assert_dynamic_resolver_backfill_scope_behavior(
    fixture: DynamicResolverBackfillFixture,
) -> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;

    let registry_manifest_id = fixture.manifest_id_base;
    let resolver_manifest_id = fixture.manifest_id_base + 1;
    let registry_contract_instance_id = Uuid::from_u128(fixture.uuid_base);
    let seed_resolver_contract_instance_id = Uuid::from_u128(fixture.uuid_base + 10);
    let selected_resolver_contract_instance_id = Uuid::from_u128(fixture.uuid_base + 1);
    let pending_resolver_contract_instance_id = Uuid::from_u128(fixture.uuid_base + 5);
    let unsupported_resolver_contract_instance_id = Uuid::from_u128(fixture.uuid_base + 6);
    let closed_resolver_contract_instance_id = Uuid::from_u128(fixture.uuid_base + 2);
    let deactivated_resolver_contract_instance_id = Uuid::from_u128(fixture.uuid_base + 3);
    let orphan_equivalent_resolver_contract_instance_id = Uuid::from_u128(fixture.uuid_base + 4);
    let registry_address = "0x0000000000000000000000000000000000000a00";
    let seed_resolver_address = "0x0000000000000000000000000000000000000a10";
    let selected_resolver_address = "0x0000000000000000000000000000000000000a01";
    let pending_resolver_address = "0x0000000000000000000000000000000000000a05";
    let unsupported_resolver_address = "0x0000000000000000000000000000000000000a06";
    let closed_resolver_address = "0x0000000000000000000000000000000000000a02";
    let deactivated_resolver_address = "0x0000000000000000000000000000000000000a03";
    let orphan_equivalent_resolver_address = "0x0000000000000000000000000000000000000a04";
    let resolver_seed_role = if fixture.resolver_source_family == "ens_v1_resolver_l1" {
        "public_resolver"
    } else {
        "resolver"
    };

    insert_active_backfill_manifest_version(
        database.pool(),
        registry_manifest_id,
        fixture.namespace,
        fixture.chain,
        fixture.registry_source_family,
        fixture.deployment_epoch,
    )
    .await?;
    insert_active_backfill_manifest_version(
        database.pool(),
        resolver_manifest_id,
        fixture.namespace,
        fixture.chain,
        fixture.resolver_source_family,
        fixture.deployment_epoch,
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        fixture.chain,
        "registry",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        fixture.chain,
        registry_address,
        Some(registry_manifest_id),
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        seed_resolver_contract_instance_id,
        fixture.chain,
        "resolver",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        seed_resolver_contract_instance_id,
        fixture.chain,
        seed_resolver_address,
        Some(resolver_manifest_id),
    )
    .await?;
    set_contract_instance_address_range(
        database.pool(),
        seed_resolver_contract_instance_id,
        Some(1),
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        resolver_seed_role,
        seed_resolver_contract_instance_id,
        seed_resolver_address,
        "none",
        None,
        None,
    )
    .await?;
    for (contract_instance_id, address) in [
        (
            selected_resolver_contract_instance_id,
            selected_resolver_address,
        ),
        (
            pending_resolver_contract_instance_id,
            pending_resolver_address,
        ),
        (
            unsupported_resolver_contract_instance_id,
            unsupported_resolver_address,
        ),
        (
            closed_resolver_contract_instance_id,
            closed_resolver_address,
        ),
        (
            deactivated_resolver_contract_instance_id,
            deactivated_resolver_address,
        ),
        (
            orphan_equivalent_resolver_contract_instance_id,
            orphan_equivalent_resolver_address,
        ),
    ] {
        insert_contract_instance(
            database.pool(),
            contract_instance_id,
            fixture.chain,
            "resolver",
        )
        .await?;
        insert_active_contract_instance_address(
            database.pool(),
            contract_instance_id,
            fixture.chain,
            address,
            None,
        )
        .await?;
    }
    set_contract_instance_address_range(
        database.pool(),
        selected_resolver_contract_instance_id,
        Some(40),
        Some(44),
    )
    .await?;
    set_contract_instance_address_range(
        database.pool(),
        pending_resolver_contract_instance_id,
        Some(42),
        Some(43),
    )
    .await?;
    set_contract_instance_address_range(
        database.pool(),
        unsupported_resolver_contract_instance_id,
        Some(42),
        Some(43),
    )
    .await?;
    set_contract_instance_address_range(
        database.pool(),
        closed_resolver_contract_instance_id,
        Some(30),
        Some(39),
    )
    .await?;
    set_contract_instance_address_range(
        database.pool(),
        deactivated_resolver_contract_instance_id,
        Some(42),
        Some(43),
    )
    .await?;
    set_contract_instance_address_range(
        database.pool(),
        orphan_equivalent_resolver_contract_instance_id,
        Some(44),
        Some(45),
    )
    .await?;

    insert_active_discovery_edge_with_range(
        database.pool(),
        fixture.chain,
        "resolver",
        registry_contract_instance_id,
        selected_resolver_contract_instance_id,
        Some(registry_manifest_id),
        Some(42),
        Some(43),
    )
    .await?;
    insert_active_discovery_edge_with_range(
        database.pool(),
        fixture.chain,
        "resolver",
        registry_contract_instance_id,
        pending_resolver_contract_instance_id,
        Some(registry_manifest_id),
        Some(42),
        Some(43),
    )
    .await?;
    insert_active_discovery_edge_with_range(
        database.pool(),
        fixture.chain,
        "resolver",
        registry_contract_instance_id,
        unsupported_resolver_contract_instance_id,
        Some(registry_manifest_id),
        Some(42),
        Some(43),
    )
    .await?;
    insert_active_discovery_edge_with_range(
        database.pool(),
        fixture.chain,
        "resolver",
        registry_contract_instance_id,
        closed_resolver_contract_instance_id,
        Some(registry_manifest_id),
        Some(30),
        Some(39),
    )
    .await?;
    insert_active_discovery_edge_with_range(
        database.pool(),
        fixture.chain,
        "resolver",
        registry_contract_instance_id,
        deactivated_resolver_contract_instance_id,
        Some(registry_manifest_id),
        Some(42),
        Some(43),
    )
    .await?;
    deactivate_discovery_edge(database.pool(), deactivated_resolver_contract_instance_id).await?;
    insert_active_discovery_edge_with_range(
        database.pool(),
        fixture.chain,
        "resolver",
        registry_contract_instance_id,
        orphan_equivalent_resolver_contract_instance_id,
        Some(registry_manifest_id),
        Some(42),
        Some(43),
    )
    .await?;

    let range = BackfillBlockRange::new(40, 44)?;
    let source_plan = load_watched_source_selector_plan(
        database.pool(),
        fixture.chain,
        WatchedSourceSelector::SourceFamily(fixture.resolver_source_family.to_owned()),
        range.from_block,
        range.to_block,
    )
    .await?;
    assert_eq!(source_plan.selected_targets.len(), 3);
    assert_eq!(source_plan.watched_chain_plan.discovery_edge_entry_count, 3);
    assert_eq!(
        source_plan.watched_chain_plan.addresses,
        vec![
            selected_resolver_address.to_owned(),
            pending_resolver_address.to_owned(),
            unsupported_resolver_address.to_owned()
        ]
    );
    let selected_target_summary = source_plan
        .selected_targets
        .iter()
        .map(|target| {
            (
                target.contract_instance_id,
                target.address.as_str(),
                target.effective_from_block,
                target.effective_to_block,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        selected_target_summary,
        vec![
            (
                selected_resolver_contract_instance_id,
                selected_resolver_address,
                42,
                43
            ),
            (
                pending_resolver_contract_instance_id,
                pending_resolver_address,
                42,
                43
            ),
            (
                unsupported_resolver_contract_instance_id,
                unsupported_resolver_address,
                42,
                43
            )
        ]
    );

    let block_40 = provider_block(
        &repeated_byte_hash("40"),
        Some(&repeated_byte_hash("3f")),
        40,
    );
    let block_41 = provider_block(&repeated_byte_hash("41"), Some(&block_40.block_hash), 41);
    let block_42 = provider_block(&repeated_byte_hash("42"), Some(&block_41.block_hash), 42);
    let block_43 = provider_block(&repeated_byte_hash("43"), Some(&block_42.block_hash), 43);
    let block_44 = provider_block(&repeated_byte_hash("44"), Some(&block_43.block_hash), 44);
    let resolver_node = if fixture.namespace == "basenames" {
        namehash_for_dns_name(&dns_encoded_base_eth_name("alice"))
    } else {
        namehash_for_dns_name(&dns_encoded_eth_name("alice"))
    };
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider_with_fixtures(
        vec![
            ProviderBlockFixture {
                block: block_40.clone(),
                logs: vec![rpc_log_payload_at_address(
                    &block_40,
                    selected_resolver_address,
                    0,
                )],
            },
            ProviderBlockFixture {
                block: block_41.clone(),
                logs: vec![rpc_log_payload_at_address(
                    &block_41,
                    selected_resolver_address,
                    0,
                )],
            },
            ProviderBlockFixture {
                block: block_42.clone(),
                logs: vec![
                    rpc_resolver_name_changed_log_payload_for_namehash(
                        &block_42,
                        selected_resolver_address,
                        &resolver_node,
                        "supported.example",
                        0,
                    ),
                    rpc_resolver_version_changed_log_payload_for_namehash(
                        &block_42,
                        selected_resolver_address,
                        &resolver_node,
                        7,
                        1,
                    ),
                    rpc_resolver_name_changed_log_payload_for_namehash(
                        &block_42,
                        pending_resolver_address,
                        &resolver_node,
                        "pending.example",
                        2,
                    ),
                    rpc_resolver_version_changed_log_payload_for_namehash(
                        &block_42,
                        pending_resolver_address,
                        &resolver_node,
                        8,
                        3,
                    ),
                    rpc_resolver_name_changed_log_payload_for_namehash(
                        &block_42,
                        unsupported_resolver_address,
                        &resolver_node,
                        "unsupported.example",
                        4,
                    ),
                    rpc_resolver_version_changed_log_payload_for_namehash(
                        &block_42,
                        unsupported_resolver_address,
                        &resolver_node,
                        9,
                        5,
                    ),
                    rpc_resolver_name_changed_log_payload_for_namehash(
                        &block_42,
                        closed_resolver_address,
                        &resolver_node,
                        "closed.example",
                        6,
                    ),
                    rpc_resolver_name_changed_log_payload_for_namehash(
                        &block_42,
                        deactivated_resolver_address,
                        &resolver_node,
                        "deactivated.example",
                        7,
                    ),
                    rpc_resolver_name_changed_log_payload_for_namehash(
                        &block_42,
                        orphan_equivalent_resolver_address,
                        &resolver_node,
                        "orphan-equivalent.example",
                        8,
                    ),
                ],
            },
            ProviderBlockFixture {
                block: block_43.clone(),
                logs: vec![rpc_resolver_name_changed_log_payload_for_namehash(
                    &block_43,
                    selected_resolver_address,
                    &resolver_node,
                    "supported-next.example",
                    0,
                )],
            },
            ProviderBlockFixture {
                block: block_44.clone(),
                logs: vec![rpc_log_payload_at_address(
                    &block_44,
                    selected_resolver_address,
                    0,
                )],
            },
        ],
        Arc::clone(&requests),
    )
    .await?;

    let outcome = run_resumable_hash_pinned_backfill_job(
        database.pool(),
        &source_plan,
        &provider,
        backfill_job_config(range, fixture.idempotency_key, "lease-dynamic-resolver")?,
    )
    .await?;
    let generic_ensv1_resolver = fixture.resolver_source_family == "ens_v1_resolver_l1";
    assert_eq!(outcome.resolved_block_count, 5);
    assert_eq!(
        outcome.raw_log_count,
        if generic_ensv1_resolver { 10 } else { 7 }
    );
    assert_eq!(
        outcome.raw_code_hash_count,
        if generic_ensv1_resolver { 7 } else { 6 }
    );

    let job = load_backfill_job(database.pool(), outcome.backfill_job_id)
        .await?
        .expect("dynamic resolver backfill job must exist");
    assert_eq!(
        job.source_identity,
        backfill::backfill_job_source_identity_payload(&source_plan)?
    );
    let source_identity = serde_json::to_string(&job.source_identity)
        .context("dynamic resolver source identity must serialize")?;
    let forbidden_targets = vec![
        seed_resolver_address.to_owned(),
        closed_resolver_address.to_owned(),
        deactivated_resolver_address.to_owned(),
        orphan_equivalent_resolver_address.to_owned(),
        seed_resolver_contract_instance_id.to_string(),
        closed_resolver_contract_instance_id.to_string(),
        deactivated_resolver_contract_instance_id.to_string(),
        orphan_equivalent_resolver_contract_instance_id.to_string(),
    ];
    for forbidden in forbidden_targets {
        assert!(
            !source_identity.contains(&forbidden),
            "excluded resolver target leaked into source_identity: {source_identity}"
        );
    }

    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            r#"
            SELECT COALESCE(
                ARRAY_AGG(emitting_address ORDER BY block_number, log_index),
                ARRAY[]::TEXT[]
            )
            FROM raw_logs
            "#
        )
        .fetch_one(database.pool())
        .await?,
        if generic_ensv1_resolver {
            vec![
                selected_resolver_address.to_owned(),
                selected_resolver_address.to_owned(),
                pending_resolver_address.to_owned(),
                pending_resolver_address.to_owned(),
                unsupported_resolver_address.to_owned(),
                unsupported_resolver_address.to_owned(),
                closed_resolver_address.to_owned(),
                deactivated_resolver_address.to_owned(),
                orphan_equivalent_resolver_address.to_owned(),
                selected_resolver_address.to_owned(),
            ]
        } else {
            vec![
                selected_resolver_address.to_owned(),
                selected_resolver_address.to_owned(),
                pending_resolver_address.to_owned(),
                pending_resolver_address.to_owned(),
                unsupported_resolver_address.to_owned(),
                unsupported_resolver_address.to_owned(),
                selected_resolver_address.to_owned(),
            ]
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<i64>>(
            r#"
            SELECT COALESCE(
                ARRAY_AGG(block_number ORDER BY block_number, log_index),
                ARRAY[]::BIGINT[]
            )
            FROM raw_logs
            "#
        )
        .fetch_one(database.pool())
        .await?,
        if generic_ensv1_resolver {
            vec![42, 42, 42, 42, 42, 42, 42, 42, 42, 43]
        } else {
            vec![42, 42, 42, 42, 42, 42, 43]
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            r#"
            SELECT COALESCE(
                ARRAY_AGG(block_hash ORDER BY block_number, log_index),
                ARRAY[]::TEXT[]
            )
            FROM raw_logs
            "#
        )
        .fetch_one(database.pool())
        .await?,
        if generic_ensv1_resolver {
            vec![
                block_42.block_hash.clone(),
                block_42.block_hash.clone(),
                block_42.block_hash.clone(),
                block_42.block_hash.clone(),
                block_42.block_hash.clone(),
                block_42.block_hash.clone(),
                block_42.block_hash.clone(),
                block_42.block_hash.clone(),
                block_42.block_hash.clone(),
                block_43.block_hash.clone(),
            ]
        } else {
            vec![
                block_42.block_hash.clone(),
                block_42.block_hash.clone(),
                block_42.block_hash.clone(),
                block_42.block_hash.clone(),
                block_42.block_hash.clone(),
                block_42.block_hash.clone(),
                block_43.block_hash.clone(),
            ]
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            r#"
            SELECT COALESCE(
                ARRAY_AGG(contract_address ORDER BY block_number, contract_address),
                ARRAY[]::TEXT[]
            )
            FROM raw_code_hashes
            "#
        )
        .fetch_one(database.pool())
        .await?,
        if generic_ensv1_resolver {
            vec![
                selected_resolver_address.to_owned(),
                closed_resolver_address.to_owned(),
                deactivated_resolver_address.to_owned(),
                orphan_equivalent_resolver_address.to_owned(),
                pending_resolver_address.to_owned(),
                unsupported_resolver_address.to_owned(),
                selected_resolver_address.to_owned(),
            ]
        } else {
            vec![
                selected_resolver_address.to_owned(),
                pending_resolver_address.to_owned(),
                unsupported_resolver_address.to_owned(),
                selected_resolver_address.to_owned(),
                pending_resolver_address.to_owned(),
                unsupported_resolver_address.to_owned(),
            ]
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<i64>>(
            r#"
            SELECT COALESCE(
                ARRAY_AGG(block_number ORDER BY block_number, contract_address),
                ARRAY[]::BIGINT[]
            )
            FROM raw_code_hashes
            "#
        )
        .fetch_one(database.pool())
        .await?,
        if generic_ensv1_resolver {
            vec![42, 42, 42, 42, 42, 42, 43]
        } else {
            vec![42, 42, 42, 43, 43, 43]
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind IN ('RecordChanged', 'RecordVersionChanged')"
        )
        .fetch_one(database.pool())
        .await?,
        0,
        "source-scoped resolver-family backfill must not bypass resolver-profile replay gates"
    );
    let excluded_addresses = if generic_ensv1_resolver {
        vec![seed_resolver_address]
    } else {
        vec![
            seed_resolver_address,
            closed_resolver_address,
            deactivated_resolver_address,
            orphan_equivalent_resolver_address,
        ]
    };
    for excluded_address in excluded_addresses {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM raw_logs WHERE emitting_address = $1"
            )
            .bind(excluded_address)
            .fetch_one(database.pool())
            .await?,
            0,
            "{excluded_address} must not be admitted as raw logs"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM raw_code_hashes WHERE contract_address = $1"
            )
            .bind(excluded_address)
            .fetch_one(database.pool())
            .await?,
            0,
            "{excluded_address} must not be admitted as raw code hashes"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE raw_fact_ref->>'emitting_address' = $1"
            )
            .bind(excluded_address)
            .fetch_one(database.pool())
            .await?,
            0,
            "{excluded_address} must not produce normalized events"
        );
    }
    for gated_address in [
        selected_resolver_address,
        pending_resolver_address,
        unsupported_resolver_address,
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE raw_fact_ref->>'emitting_address' = $1 AND event_kind IN ('RecordChanged', 'RecordVersionChanged')"
            )
            .bind(gated_address)
            .fetch_one(database.pool())
            .await?,
            0,
            "{gated_address} must wait for full raw replay before resolver-local normalization"
        );
    }

    let recorded_requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    let log_requests = recorded_requests
        .iter()
        .filter(|request| request.method == "eth_getLogs")
        .collect::<Vec<_>>();
    assert_eq!(
        log_requests.len(),
        1,
        "only one resolver-family range request should fetch logs"
    );
    assert_eq!(log_requests[0].batch_size, 1);
    let log_filter = log_requests[0]
        .params
        .first()
        .and_then(Value::as_object)
        .expect("log request must include a filter object");
    assert_eq!(
        log_filter.get("fromBlock").and_then(Value::as_str),
        if generic_ensv1_resolver {
            Some("0x28")
        } else {
            Some("0x2a")
        }
    );
    assert_eq!(
        log_filter.get("toBlock").and_then(Value::as_str),
        if generic_ensv1_resolver {
            Some("0x2c")
        } else {
            Some("0x2b")
        }
    );
    assert!(
        !log_filter.contains_key("blockHash"),
        "selected resolver log lookup should use one safe range instead of per-block blockHash filters"
    );
    if generic_ensv1_resolver {
        assert!(
            !log_filter.contains_key("address"),
            "ENSv1 generic resolver event scan must not be narrowed to selected targets"
        );
        let topic0s = support_log_filter_topic0s(log_filter)
            .expect("generic resolver log lookup must constrain topic0");
        assert!(
            topic0s.contains(&resolver_text_changed_topic0()),
            "generic resolver log lookup must include legacy TextChanged"
        );
        assert!(
            topic0s.contains(&resolver_text_changed_with_value_topic0()),
            "generic resolver log lookup must include TextChanged with value"
        );
        assert!(
            !topic0s.contains(&keccak256_hex(b"ApprovalForAll(address,address,bool)")),
            "generic resolver log lookup must not include common permission topics globally"
        );
    } else {
        assert_eq!(
            log_filter.get("address").and_then(Value::as_array),
            Some(&vec![
                Value::String(selected_resolver_address.to_owned()),
                Value::String(pending_resolver_address.to_owned()),
                Value::String(unsupported_resolver_address.to_owned()),
            ]),
            "log range must include only resolver targets effective for every block in the range"
        );
    }

    let code_requests = recorded_requests
        .iter()
        .filter(|request| request.method == "eth_getCode")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        code_requests.len(),
        if generic_ensv1_resolver { 7 } else { 6 }
    );
    assert_eq!(
        code_requests
            .iter()
            .map(|request| request.params.first().and_then(Value::as_str))
            .collect::<Vec<_>>(),
        if generic_ensv1_resolver {
            vec![
                Some(selected_resolver_address),
                Some(closed_resolver_address),
                Some(deactivated_resolver_address),
                Some(orphan_equivalent_resolver_address),
                Some(pending_resolver_address),
                Some(unsupported_resolver_address),
                Some(selected_resolver_address),
            ]
        } else {
            vec![
                Some(selected_resolver_address),
                Some(pending_resolver_address),
                Some(unsupported_resolver_address),
                Some(selected_resolver_address),
                Some(pending_resolver_address),
                Some(unsupported_resolver_address),
            ]
        }
    );
    assert_eq!(
        code_requests
            .iter()
            .map(|request| {
                request
                    .params
                    .get(1)
                    .and_then(Value::as_object)
                    .and_then(|selection| selection.get("blockHash"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect::<Vec<_>>(),
        if generic_ensv1_resolver {
            vec![
                Some(block_42.block_hash.clone()),
                Some(block_42.block_hash.clone()),
                Some(block_42.block_hash.clone()),
                Some(block_42.block_hash.clone()),
                Some(block_42.block_hash.clone()),
                Some(block_42.block_hash.clone()),
                Some(block_43.block_hash.clone()),
            ]
        } else {
            vec![
                Some(block_42.block_hash.clone()),
                Some(block_42.block_hash.clone()),
                Some(block_42.block_hash.clone()),
                Some(block_43.block_hash.clone()),
                Some(block_43.block_hash.clone()),
                Some(block_43.block_hash.clone()),
            ]
        }
    );

    server.abort();
    database.cleanup().await
}

async fn set_contract_instance_address_range(
    pool: &PgPool,
    contract_instance_id: Uuid,
    active_from_block_number: Option<i64>,
    active_to_block_number: Option<i64>,
) -> Result<()> {
    sqlx::query(
        r#"
            UPDATE contract_instance_addresses
            SET active_from_block_number = $2,
                active_to_block_number = $3
            WHERE contract_instance_id = $1
            "#,
    )
    .bind(contract_instance_id)
    .bind(active_from_block_number)
    .bind(active_to_block_number)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to set active range for contract_instance_id {contract_instance_id}")
    })?;

    Ok(())
}

async fn insert_active_backfill_manifest_version(
    pool: &PgPool,
    manifest_id: i64,
    namespace: &str,
    chain: &str,
    source_family: &str,
    deployment_epoch: &str,
) -> Result<()> {
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
                $1,
                1,
                $2,
                $3,
                $4,
                $5,
                'active',
                'ensip15@ens-normalize-0.1.1',
                ('manifests/' || $2 || '/' || $3 || '/v1.toml'),
                DEFAULT
            )
            "#,
    )
    .bind(manifest_id)
    .bind(namespace)
    .bind(source_family)
    .bind(chain)
    .bind(deployment_epoch)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to insert active manifest {manifest_id} for {chain}:{source_family}")
    })?;

    Ok(())
}

async fn deactivate_discovery_edge(pool: &PgPool, to_contract_instance_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"
            UPDATE discovery_edges
            SET deactivated_at = now()
            WHERE to_contract_instance_id = $1
            "#,
    )
    .bind(to_contract_instance_id)
    .execute(pool)
    .await
    .with_context(|| format!("failed to deactivate discovery edge to {to_contract_instance_id}"))?;

    Ok(())
}

fn repeated_byte_hash(byte_hex: &str) -> String {
    let mut hash = String::from("0x");
    for _ in 0..32 {
        hash.push_str(byte_hex);
    }
    hash
}

fn backfill_job_config(
    range: BackfillBlockRange,
    idempotency_key: &str,
    lease_token: &str,
) -> Result<BackfillJobRunConfig> {
    Ok(BackfillJobRunConfig {
        deployment_profile: "mainnet".to_owned(),
        idempotency_key: idempotency_key.to_owned(),
        range,
        lease_owner: "indexer-backfill-test".to_owned(),
        lease_token: lease_token.to_owned(),
        lease_expires_at: backfill_lease_deadline()?,
        hash_pinned_chunk_blocks: backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
        adapter_sync_mode: backfill::BackfillAdapterSyncMode::Inline,
        header_audit_mode: HeaderAuditMode::Minimal,
    })
}

fn backfill_lease_deadline() -> Result<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + 300)
        .context("backfill lease deadline must be valid")
}

async fn create_backfill_job_tables(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TYPE backfill_lifecycle_status AS ENUM (
            'pending',
            'reserved',
            'running',
            'completed',
            'failed'
        )
        "#,
    )
    .execute(pool)
    .await
    .context("failed to create backfill_lifecycle_status type for indexer tests")?;

    sqlx::query(
        r#"
        CREATE TABLE backfill_jobs (
            backfill_job_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            deployment_profile TEXT NOT NULL,
            chain_id TEXT NOT NULL,
            source_identity JSONB NOT NULL,
            scan_mode TEXT NOT NULL,
            range_start_block_number BIGINT NOT NULL CHECK (range_start_block_number >= 0),
            range_end_block_number BIGINT NOT NULL CHECK (range_end_block_number >= range_start_block_number),
            idempotency_key TEXT NOT NULL,
            status backfill_lifecycle_status NOT NULL DEFAULT 'pending',
            failure_reason TEXT,
            failure_metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            completed_at TIMESTAMPTZ,
            UNIQUE (idempotency_key),
            CHECK (jsonb_typeof(source_identity) IN ('object', 'array')),
            CHECK (jsonb_typeof(failure_metadata) = 'object'),
            CHECK ((status = 'failed'::backfill_lifecycle_status) = (failure_reason IS NOT NULL) OR status <> 'failed'::backfill_lifecycle_status),
            CHECK ((status = 'completed'::backfill_lifecycle_status) = (completed_at IS NOT NULL) OR status <> 'completed'::backfill_lifecycle_status)
        )
        "#,
    )
    .execute(pool)
    .await
    .context("failed to create backfill_jobs table for indexer tests")?;

    sqlx::query(
        r#"
        CREATE INDEX backfill_jobs_lookup_idx
            ON backfill_jobs (deployment_profile, chain_id, scan_mode, status)
        "#,
    )
    .execute(pool)
    .await
    .context("failed to create backfill_jobs_lookup_idx for indexer tests")?;

    sqlx::query(
        r#"
        CREATE INDEX backfill_jobs_range_idx
            ON backfill_jobs (chain_id, range_start_block_number, range_end_block_number)
        "#,
    )
    .execute(pool)
    .await
    .context("failed to create backfill_jobs_range_idx for indexer tests")?;

    sqlx::query(
        r#"
        CREATE TABLE backfill_ranges (
            backfill_range_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            backfill_job_id BIGINT NOT NULL REFERENCES backfill_jobs (backfill_job_id) ON DELETE CASCADE,
            range_start_block_number BIGINT NOT NULL CHECK (range_start_block_number >= 0),
            range_end_block_number BIGINT NOT NULL CHECK (range_end_block_number >= range_start_block_number),
            checkpoint_block_number BIGINT NOT NULL CHECK (checkpoint_block_number >= range_start_block_number - 1 AND checkpoint_block_number <= range_end_block_number),
            status backfill_lifecycle_status NOT NULL DEFAULT 'pending',
            lease_token TEXT,
            lease_owner TEXT,
            lease_expires_at TIMESTAMPTZ,
            attempt_count BIGINT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
            failure_reason TEXT,
            failure_metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            completed_at TIMESTAMPTZ,
            UNIQUE (backfill_job_id, range_start_block_number, range_end_block_number),
            CHECK (jsonb_typeof(failure_metadata) = 'object'),
            CHECK ((lease_token IS NULL) = (lease_owner IS NULL)),
            CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
            CHECK ((status IN ('reserved'::backfill_lifecycle_status, 'running'::backfill_lifecycle_status)) = (lease_token IS NOT NULL)),
            CHECK ((status = 'failed'::backfill_lifecycle_status) = (failure_reason IS NOT NULL) OR status <> 'failed'::backfill_lifecycle_status),
            CHECK ((status = 'completed'::backfill_lifecycle_status) = (completed_at IS NOT NULL) OR status <> 'completed'::backfill_lifecycle_status)
        )
        "#,
    )
    .execute(pool)
    .await
    .context("failed to create backfill_ranges table for indexer tests")?;

    sqlx::query(
        r#"
        CREATE INDEX backfill_ranges_reservation_idx
            ON backfill_ranges (backfill_job_id, status, range_start_block_number, range_end_block_number)
        "#,
    )
    .execute(pool)
    .await
    .context("failed to create backfill_ranges_reservation_idx for indexer tests")?;

    sqlx::query(
        r#"
        CREATE INDEX backfill_ranges_lease_expiry_idx
            ON backfill_ranges (lease_expires_at)
            WHERE lease_expires_at IS NOT NULL
        "#,
    )
    .execute(pool)
    .await
    .context("failed to create backfill_ranges_lease_expiry_idx for indexer tests")?;

    sqlx::query(
        r#"
        CREATE UNIQUE INDEX backfill_ranges_active_lease_token_idx
            ON backfill_ranges (lease_token)
            WHERE lease_token IS NOT NULL
              AND status IN ('reserved'::backfill_lifecycle_status, 'running'::backfill_lifecycle_status)
        "#,
    )
    .execute(pool)
    .await
    .context("failed to create backfill_ranges_active_lease_token_idx for indexer tests")?;

    Ok(())
}

fn parse_rpc_block_number(value: &str) -> i64 {
    i64::from_str_radix(value.strip_prefix("0x").unwrap_or(value), 16)
        .expect("test RPC block number must be valid hex")
}

async fn table_count(pool: &PgPool, table_name: &str) -> Result<i64> {
    let query = format!("SELECT COUNT(*) FROM {table_name}");
    sqlx::query_scalar::<_, i64>(&query)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to count {table_name} rows"))
}
