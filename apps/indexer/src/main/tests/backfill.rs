use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

use bigname_manifests::{
    WatchedSourceSelector, WatchedSourceSelectorKind, WatchedSourceSelectorPlan,
    WatchedTargetIdentity, load_watched_source_selector_plan,
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

    assert_eq!(table_count(database.pool(), "raw_blocks").await?, 2);
    assert_eq!(table_count(database.pool(), "raw_transactions").await?, 2);
    assert_eq!(table_count(database.pool(), "raw_receipts").await?, 2);
    assert_eq!(table_count(database.pool(), "raw_logs").await?, 2);
    assert_eq!(table_count(database.pool(), "raw_code_hashes").await?, 2);
    assert_eq!(
        table_count(database.pool(), "raw_payload_cache_metadata").await?,
        4
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
        vec![
            (
                provider::RAW_PAYLOAD_KIND_BLOCK_RECEIPTS.to_owned(),
                2,
                2,
                2,
                vec!["eth_getBlockReceipts".to_owned()],
                vec!["block_hash".to_owned()],
            ),
            (
                provider::RAW_PAYLOAD_KIND_FULL_BLOCK.to_owned(),
                2,
                2,
                2,
                vec!["eth_getBlockByHash".to_owned()],
                vec!["block_hash".to_owned()],
            ),
        ],
        "range-fetched logs must not fake hash-scoped block_logs retained payload metadata"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(table_count(database.pool(), "chain_lineage").await?, 0);
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
            "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "observed".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 43"
        )
        .fetch_one(database.pool())
        .await?,
        "observed".to_owned()
    );

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(requests.len(), 11);
    let block_number_requests = requests
        .iter()
        .filter(|request| request.method == "eth_getBlockByNumber")
        .collect::<Vec<_>>();
    assert_eq!(block_number_requests.len(), 4);
    assert_eq!(
        block_number_requests
            .iter()
            .map(|request| request.params.first().and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec![Some("0x2a"), Some("0x2b"), Some("0x2a"), Some("0x2b")]
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
    assert_eq!(requests[0].method, "eth_getBlockByNumber");
    assert_eq!(
        requests[0].params.first().and_then(Value::as_str),
        Some("0x2a")
    );
    assert_eq!(requests[1].method, "eth_getBlockByNumber");
    assert_eq!(
        requests[1].params.first().and_then(Value::as_str),
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
    assert_eq!(requests[5].method, "eth_getBlockByHash");
    assert_eq!(
        requests[5].params.first().and_then(Value::as_str),
        Some(block_42.block_hash.as_str())
    );
    assert_eq!(requests[7].method, "eth_getBlockByHash");
    assert_eq!(
        requests[7].params.first().and_then(Value::as_str),
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
    assert_eq!(requests[9].method, "eth_getCode");
    assert_eq!(
        requests[9]
            .params
            .get(1)
            .and_then(Value::as_object)
            .and_then(|selection| selection.get("blockHash"))
            .and_then(Value::as_str),
        Some(block_42.block_hash.as_str())
    );
    assert_eq!(requests[10].method, "eth_getCode");
    assert_eq!(
        requests[10]
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
                rpc_log_payload_at_address(&block_42, registry_address, 0),
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
    assert_eq!(outcome.raw_log_count, 1);
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
        sqlx::query_scalar::<_, String>("SELECT emitting_address FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        registry_address.to_owned()
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
            ProviderBlockFixture {
                block: block.clone(),
                logs: vec![rpc_log_payload_at_address(
                    &block,
                    fixture.address,
                    index as i64,
                )],
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
        let expected_source_identity_hash = source_plan.source_identity_hash();
        assert_eq!(job.source_identity, source_plan.source_identity_payload());
        assert_eq!(
            job.source_identity
                .get("source_identity_hash")
                .and_then(Value::as_str),
            Some(expected_source_identity_hash.as_str())
        );
        assert_eq!(
            job.source_identity
                .get("selected_targets")
                .and_then(Value::as_array)
                .and_then(|targets| targets.first())
                .and_then(|target| target.get("source_family"))
                .and_then(Value::as_str),
            Some(fixture.source_family)
        );

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
async fn source_scoped_backfill_dynamic_resolver_ensv1_selected_targets_are_range_locked()
-> Result<()> {
    assert_dynamic_resolver_backfill_is_selected_target_only(DynamicResolverBackfillFixture {
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
    assert_dynamic_resolver_backfill_is_selected_target_only(DynamicResolverBackfillFixture {
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
            logs: vec![rpc_log_payload_at_address(&block_42, selected_address, 0)],
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
        1
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
        1
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
                assert_eq!(params.first().and_then(Value::as_str), Some("0x2a"));
                rpc_block_bundle_payload(&provider_block(block_hash, None, 42))
            }
            "eth_getBlockByHash" => Value::Null,
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
    assert_eq!(ranges[0].checkpoint_block_number, 42);
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
        vec!["eth_getBlockByNumber", "eth_getBlockByHash"]
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
            .push(RecordedRpcRequest {
                method: method.to_owned(),
                params: params.clone(),
                http_request_id: json_rpc_test_http_request_id(&body),
                batch_size: json_rpc_test_batch_size(&body),
            });

        let result = match method {
            "eth_getBlockByNumber" => {
                assert_eq!(params.get(1), Some(&Value::Bool(false)));
                let block_number = params
                    .first()
                    .and_then(Value::as_str)
                    .map(parse_rpc_block_number)
                    .expect("block number parameter must be present");
                let block_hash = hashes_by_number
                    .get(&block_number)
                    .unwrap_or_else(|| panic!("unexpected block number request: {body}"));
                let fixture = fixtures_by_hash
                    .get(block_hash)
                    .expect("number index must point at a fixture block");
                rpc_block_bundle_payload(&fixture.block)
            }
            "eth_getBlockByHash" => {
                assert_eq!(params.get(1), Some(&Value::Bool(true)));
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
    let mut logs = Vec::new();

    if let Some(block_hash) = filter.get("blockHash").and_then(Value::as_str) {
        let fixture = fixtures_by_hash
            .get(&block_hash.to_ascii_lowercase())
            .unwrap_or_else(|| panic!("unexpected log blockHash filter: {filter:?}"));
        logs.extend(filtered_fixture_logs(fixture, address_filter.as_ref()));
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
            logs.extend(filtered_fixture_logs(fixture, address_filter.as_ref()));
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

fn filtered_fixture_logs(
    fixture: &ProviderBlockFixture,
    address_filter: Option<&BTreeSet<String>>,
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

async fn assert_dynamic_resolver_backfill_is_selected_target_only(
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
    assert_eq!(outcome.resolved_block_count, 5);
    assert_eq!(outcome.raw_log_count, 7);
    assert_eq!(outcome.raw_code_hash_count, 6);

    let job = load_backfill_job(database.pool(), outcome.backfill_job_id)
        .await?
        .expect("dynamic resolver backfill job must exist");
    assert_eq!(job.source_identity, source_plan.source_identity_payload());
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
        vec![
            selected_resolver_address.to_owned(),
            selected_resolver_address.to_owned(),
            pending_resolver_address.to_owned(),
            pending_resolver_address.to_owned(),
            unsupported_resolver_address.to_owned(),
            unsupported_resolver_address.to_owned(),
            selected_resolver_address.to_owned()
        ]
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
        vec![42, 42, 42, 42, 42, 42, 43]
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
        vec![
            block_42.block_hash.clone(),
            block_42.block_hash.clone(),
            block_42.block_hash.clone(),
            block_42.block_hash.clone(),
            block_42.block_hash.clone(),
            block_42.block_hash.clone(),
            block_43.block_hash.clone()
        ]
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
        vec![
            selected_resolver_address.to_owned(),
            pending_resolver_address.to_owned(),
            unsupported_resolver_address.to_owned(),
            selected_resolver_address.to_owned(),
            pending_resolver_address.to_owned(),
            unsupported_resolver_address.to_owned()
        ]
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
        vec![42, 42, 42, 43, 43, 43]
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
    for excluded_address in [
        seed_resolver_address,
        closed_resolver_address,
        deactivated_resolver_address,
        orphan_equivalent_resolver_address,
    ] {
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
        "only the contiguous selected resolver target range should fetch logs"
    );
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
        "selected resolver log lookup should use one safe range instead of per-block blockHash filters"
    );
    assert_eq!(
        log_filter.get("address").and_then(Value::as_array),
        Some(&vec![
            Value::String(selected_resolver_address.to_owned()),
            Value::String(pending_resolver_address.to_owned()),
            Value::String(unsupported_resolver_address.to_owned()),
        ]),
        "log range must include only resolver targets effective for every block in the range"
    );

    let code_requests = recorded_requests
        .iter()
        .filter(|request| request.method == "eth_getCode")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(code_requests.len(), 6);
    assert_eq!(
        code_requests
            .iter()
            .map(|request| request.params.first().and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec![
            Some(selected_resolver_address),
            Some(pending_resolver_address),
            Some(unsupported_resolver_address),
            Some(selected_resolver_address),
            Some(pending_resolver_address),
            Some(unsupported_resolver_address)
        ]
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
        vec![
            Some(block_42.block_hash.clone()),
            Some(block_42.block_hash.clone()),
            Some(block_42.block_hash.clone()),
            Some(block_43.block_hash.clone()),
            Some(block_43.block_hash.clone()),
            Some(block_43.block_hash.clone())
        ]
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
                'uts46-v1',
                ('manifests/' || $2 || '/' || $3 || '/v1.toml'),
                '{}'::jsonb
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
            checkpoint_block_number BIGINT NOT NULL CHECK (checkpoint_block_number >= range_start_block_number AND checkpoint_block_number <= range_end_block_number),
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
