use std::{collections::BTreeMap, sync::Mutex};

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
    assert_eq!(requests.len(), 10);
    assert_eq!(requests[0].method, "eth_getBlockByNumber");
    assert_eq!(
        requests[0].params.first().and_then(Value::as_str),
        Some("0x2a")
    );
    assert_eq!(requests[1].method, "eth_getBlockByHash");
    assert_eq!(
        requests[1].params.first().and_then(Value::as_str),
        Some(block_42.block_hash.as_str())
    );
    assert_eq!(requests[4].method, "eth_getCode");
    assert_eq!(
        requests[4]
            .params
            .get(1)
            .and_then(Value::as_object)
            .and_then(|selection| selection.get("blockHash"))
            .and_then(Value::as_str),
        Some(block_42.block_hash.as_str())
    );
    assert_eq!(requests[5].method, "eth_getBlockByNumber");
    assert_eq!(
        requests[5].params.first().and_then(Value::as_str),
        Some("0x2b")
    );
    assert_eq!(requests[6].method, "eth_getBlockByHash");
    assert_eq!(
        requests[6].params.first().and_then(Value::as_str),
        Some(block_43.block_hash.as_str())
    );
    assert_eq!(requests[9].method, "eth_getCode");
    assert_eq!(
        requests[9]
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
async fn source_scoped_backfill_enforces_selected_target_effective_ranges_during_intake()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(1_101);
    let watched_address = "0x0000000000000000000000000000000000000011";

    insert_watched_manifest_contract(
        database.pool(),
        101,
        "ens",
        "ethereum-mainnet",
        "ens_v2_registry_l1",
        contract_instance_id,
        watched_address,
    )
    .await?;
    set_contract_instance_address_range(database.pool(), contract_instance_id, Some(43), Some(43))
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
    assert_eq!(source_plan.selected_targets.len(), 1);
    assert_eq!(source_plan.selected_targets[0].effective_from_block, 43);
    assert_eq!(source_plan.selected_targets[0].effective_to_block, 43);

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
                logs: vec![rpc_log_payload_at_address(&block_42, watched_address, 0)],
            },
            ProviderBlockFixture {
                block: block_43.clone(),
                logs: vec![rpc_log_payload_at_address(&block_43, watched_address, 0)],
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
    assert_eq!(outcome.raw_log_count, 1);
    assert_eq!(outcome.raw_code_hash_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, Vec<i64>>(
            "SELECT ARRAY_AGG(block_number ORDER BY block_number) FROM raw_logs"
        )
        .fetch_one(database.pool())
        .await?,
        vec![43]
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<i64>>(
            "SELECT ARRAY_AGG(block_number ORDER BY block_number) FROM raw_code_hashes"
        )
        .fetch_one(database.pool())
        .await?,
        vec![43]
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
        Some(watched_address)
    );
    assert_eq!(
        code_requests[0]
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
                let block_hash = params
                    .first()
                    .and_then(Value::as_object)
                    .and_then(|filter| filter.get("blockHash"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let fixture = fixtures_by_hash
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
