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
    let canonical_parent = provider_block(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
        41,
    );
    let safe_head = provider_block(
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        Some("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
        40,
    );
    let safe_parent = provider_block(
        "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        Some("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
        39,
    );
    let finalized_head = provider_block(
        "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        Some("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        38,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![
        ProviderBlockFixture {
            logs: vec![rpc_current_name_wrapped_log_payload(&canonical_head)],
            block: canonical_head.clone(),
        },
        ProviderBlockFixture {
            logs: vec![],
            block: canonical_parent,
        },
        ProviderBlockFixture {
            logs: vec![rpc_current_name_wrapped_log_payload(&safe_head)],
            block: safe_head.clone(),
        },
        ProviderBlockFixture {
            logs: vec![],
            block: safe_parent,
        },
        ProviderBlockFixture {
            logs: vec![rpc_current_name_wrapped_log_payload(&finalized_head)],
            block: finalized_head.clone(),
        },
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
    assert_eq!(next_task.checkpoint.safe_block_number, Some(40));
    assert_eq!(next_task.checkpoint.finalized_block_number, Some(38));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage")
            .fetch_one(database.pool())
            .await?,
        5
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage")
            .fetch_one(database.pool())
            .await?,
        5
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
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_payload_cache_metadata")
            .fetch_one(database.pool())
            .await?,
        9
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
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 38"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
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
        "canonical".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 38"
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
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 38"
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
            "SELECT canonicality_state::TEXT FROM raw_receipts WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_logs WHERE block_number = 38"
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
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_number = 40"
        )
        .fetch_one(database.pool())
        .await?,
        "safe".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_number = 38"
        )
        .fetch_one(database.pool())
        .await?,
        "finalized".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'decoded_name' FROM normalized_events WHERE event_kind = 'PreimageObserved' AND block_number = 42"
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
async fn reconcile_fetched_heads_skips_stale_finalized_checkpoint_tag() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "base-mainnet";
    let block_102 = provider_block(
        "0x1020000000000000000000000000000000000000000000000000000000000000",
        Some("0x1010000000000000000000000000000000000000000000000000000000000000"),
        102,
    );
    let block_103 = provider_block(
        "0x1030000000000000000000000000000000000000000000000000000000000000",
        Some(&block_102.block_hash),
        103,
    );
    let block_104 = provider_block(
        "0x1040000000000000000000000000000000000000000000000000000000000000",
        Some(&block_103.block_hash),
        104,
    );
    let block_105 = provider_block(
        "0x1050000000000000000000000000000000000000000000000000000000000000",
        Some(&block_104.block_hash),
        105,
    );
    let block_106 = provider_block(
        "0x1060000000000000000000000000000000000000000000000000000000000000",
        Some(&block_105.block_hash),
        106,
    );
    let block_107 = provider_block(
        "0x1070000000000000000000000000000000000000000000000000000000000000",
        Some(&block_106.block_hash),
        107,
    );
    let (provider, server) = bundle_provider(vec![
        block_102.clone(),
        block_103.clone(),
        block_104.clone(),
        block_105.clone(),
        block_106.clone(),
        block_107.clone(),
    ])
    .await?;

    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: Vec::new(),
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 0,
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
    };
    let (task, _) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: block_105.clone(),
            safe: Some(block_104.clone()),
            finalized: Some(block_103.clone()),
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
    )
    .await?
    .expect("initial reconciliation must seed checkpoints");

    let (task, _) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: block_107,
            safe: Some(block_106),
            finalized: Some(block_102),
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
    )
    .await?
    .expect("stale finalized provider tag must not fail canonical advancement");

    assert_eq!(task.checkpoint.canonical_block_number, Some(107));
    assert_eq!(task.checkpoint.safe_block_number, Some(106));
    assert_eq!(task.checkpoint.finalized_block_number, Some(103));
    assert_eq!(
        task.checkpoint.finalized_block_hash.as_deref(),
        Some(block_103.block_hash.as_str())
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_does_not_revive_off_branch_safe_head_lineage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "base-mainnet";
    let block_100 = provider_block(
        "0x1000000000000000000000000000000000000000000000000000000000000000",
        Some("0x0990000000000000000000000000000000000000000000000000000000000000"),
        100,
    );
    let block_101 = provider_block(
        "0x1010000000000000000000000000000000000000000000000000000000000000",
        Some(&block_100.block_hash),
        101,
    );
    let block_102 = provider_block(
        "0x1020000000000000000000000000000000000000000000000000000000000000",
        Some(&block_101.block_hash),
        102,
    );
    let off_branch_safe = provider_block(
        "0x2afe000000000000000000000000000000000000000000000000000000000000",
        Some("0x2afa000000000000000000000000000000000000000000000000000000000000"),
        100,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &block_100,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &block_101,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &off_branch_safe,
        CanonicalityState::Canonical,
    )
    .await?;
    bigname_storage::mark_chain_lineage_range_orphaned(
        database.pool(),
        chain,
        &off_branch_safe.block_hash,
        None,
    )
    .await?;
    let (provider, server) = bundle_provider(vec![
        block_100.clone(),
        block_101.clone(),
        block_102.clone(),
        off_branch_safe.clone(),
    ])
    .await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: Vec::new(),
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 0,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(block_101.block_hash.clone()),
            canonical_block_number: Some(block_101.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };

    let (task, _outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: block_102,
            safe: Some(off_branch_safe.clone()),
            finalized: None,
        },
        false,
        HeaderAuditMode::Minimal,
        &[],
    )
    .await?
    .expect("canonical append must still advance");

    assert_eq!(task.checkpoint.canonical_block_number, Some(102));
    assert_eq!(task.checkpoint.safe_block_number, None);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = $1"
        )
        .bind(&off_branch_safe.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_canonical_head_rejects_gap_larger_than_bounded_backfill_chunk() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "base-mainnet";
    let gap_end_block = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 2;
    let mut blocks = Vec::new();
    let mut parent_hash = None::<String>;
    for block_number in 1..=gap_end_block {
        let block_hash = format!("0x{block_number:064x}");
        let block = provider_block(&block_hash, parent_hash.as_deref(), block_number);
        parent_hash = Some(block_hash);
        blocks.push(block);
    }
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &current,
        CanonicalityState::Canonical,
    )
    .await?;
    let (provider, server) = bundle_provider(blocks).await?;
    let checkpoint = ChainCheckpoint {
        chain_id: chain.to_owned(),
        canonical_block_hash: Some(current.block_hash.clone()),
        canonical_block_number: Some(current.block_number),
        safe_block_hash: None,
        safe_block_number: None,
        finalized_block_hash: None,
        finalized_block_number: None,
    };

    let error = reconcile_canonical_head(
        database.pool(),
        &provider,
        chain,
        &checkpoint,
        &latest,
        HeaderAuditMode::Minimal,
    )
    .await
    .expect_err("live reconciliation must reject unbounded contiguous gaps");

    assert!(
        error
            .to_string()
            .contains("exceeds live gap fill limit"),
        "unexpected unbounded gap error: {error:#}"
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_canonical_head_recovers_large_gap_from_stored_lineage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "base-mainnet";
    let gap_end_block = crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS + 2;
    let mut blocks = Vec::new();
    let mut parent_hash = None::<String>;
    for block_number in 1..=gap_end_block {
        let block_hash = format!("0x{block_number:064x}");
        let block = provider_block(&block_hash, parent_hash.as_deref(), block_number);
        parent_hash = Some(block_hash);
        blocks.push(block);
    }
    let current = blocks
        .first()
        .expect("test chain must include a current block")
        .clone();
    let latest = blocks
        .last()
        .expect("test chain must include a latest block")
        .clone();
    for block in &blocks {
        insert_chain_lineage_for_block(
            database.pool(),
            chain,
            block,
            CanonicalityState::Canonical,
        )
        .await?;
    }
    let (provider, server) = bundle_provider(blocks).await?;
    let checkpoint = ChainCheckpoint {
        chain_id: chain.to_owned(),
        canonical_block_hash: Some(current.block_hash.clone()),
        canonical_block_number: Some(current.block_number),
        safe_block_hash: None,
        safe_block_number: None,
        finalized_block_hash: None,
        finalized_block_number: None,
    };

    let reconciliation = reconcile_canonical_head(
        database.pool(),
        &provider,
        chain,
        &checkpoint,
        &latest,
        HeaderAuditMode::Minimal,
    )
    .await
    .expect("stored lineage must allow large-gap checkpoint recovery");

    assert_eq!(
        reconciliation.status,
        CanonicalReconciliationStatus::GapBackfilled
    );
    assert_eq!(
        reconciliation.canonical,
        Some(bigname_storage::CheckpointBlockRef {
            block_hash: latest.block_hash.clone(),
            block_number: latest.block_number,
        })
    );
    assert_eq!(reconciliation.fetched_parent_count, 0);

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn cache_fill_authorizes_full_block_metadata_from_provider_fetch() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let block = provider_block(
        "0xa0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0",
        Some("0xb0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0"),
        100,
    );
    let (provider, server) = bundle_provider(vec![block.clone()]).await?;

    let bundle = provider
        .fetch_block_bundle_by_hash(&block.block_hash)
        .await?;
    let full_block_payload = bundle
        .raw_payloads
        .iter()
        .find(|payload| payload.payload_kind == provider::RAW_PAYLOAD_KIND_FULL_BLOCK)
        .expect("provider bundle fetch must retain full-block payload metadata");
    let expected_response_body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": rpc_block_bundle_payload(&block),
    })
    .to_string();
    let expected_payload_size = i64::try_from(expected_response_body.len())
        .context("expected JSON-RPC response body size must fit in i64")?;
    assert_eq!(full_block_payload.digest_algorithm, "keccak256");
    assert_eq!(
        full_block_payload.retained_digest,
        keccak256_hex(expected_response_body.as_bytes())
    );
    assert_eq!(full_block_payload.payload_size_bytes, expected_payload_size);
    assert_eq!(
        full_block_payload.cache_metadata,
        json!({
            "source": "json-rpc",
            "method": "eth_getBlockByHash",
            "fetch_mode": "block_hash",
            "digest_scope": "json_rpc_response_body",
        })
    );

    let raw_block = provider_block_to_raw_block(chain, &block, CanonicalityState::Canonical);
    let upserts = provider_raw_payload_cache_metadata_to_upserts(
        chain,
        &raw_block,
        std::slice::from_ref(full_block_payload),
    );
    bigname_storage::upsert_raw_payload_cache_metadata(database.pool(), &upserts).await?;

    let persisted = bigname_storage::load_raw_payload_cache_metadata(
        database.pool(),
        chain,
        &block.block_hash,
        provider::RAW_PAYLOAD_KIND_FULL_BLOCK,
        Some(&full_block_payload.digest_algorithm),
        Some(&full_block_payload.retained_digest),
    )
    .await?
    .expect("provider fetch metadata must be persisted for later cache fill");
    assert_eq!(
        persisted.retained_digest.as_deref(),
        Some(full_block_payload.retained_digest.as_str())
    );
    assert_eq!(persisted.payload_size_bytes, expected_payload_size);

    let filled_block = provider
        .cache_fill_full_block_by_hash(
            database.pool(),
            chain,
            &block.block_hash,
            block.block_number,
        )
        .await?;
    assert_eq!(filled_block, block);

    let number_error = provider
        .cache_fill_full_block_by_hash(
            database.pool(),
            chain,
            &block.block_hash,
            block.block_number + 1,
        )
        .await
        .expect_err(
            "cache-fill must validate the returned block number after digest authorization",
        );
    assert!(
        number_error
            .to_string()
            .contains("with block number 100; expected 101"),
        "unexpected error: {number_error:#}"
    );

    let requested_block = provider_block(
        "0xc0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0",
        Some("0xd0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0"),
        200,
    );
    let returned_block = provider_block(
        "0xe0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0",
        requested_block.parent_hash.as_deref(),
        requested_block.block_number,
    );
    let mismatched_response_body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": rpc_block_bundle_payload(&returned_block),
    })
    .to_string();
    let mismatched_payload_size = i64::try_from(mismatched_response_body.len())
        .context("mismatched JSON-RPC response body size must fit in i64")?;
    bigname_storage::upsert_raw_payload_cache_metadata(
        database.pool(),
        &[bigname_storage::RawPayloadCacheMetadataUpsert {
            chain_id: chain.to_owned(),
            block_hash: requested_block.block_hash.clone(),
            payload_kind: provider::RAW_PAYLOAD_KIND_FULL_BLOCK.to_owned(),
            digest_algorithm: Some("keccak256".to_owned()),
            retained_digest: Some(keccak256_hex(mismatched_response_body.as_bytes())),
            block_number: Some(requested_block.block_number),
            payload_size_bytes: mismatched_payload_size,
            content_type: Some(provider::JSON_RPC_PAYLOAD_CONTENT_TYPE.to_owned()),
            content_encoding: Some(provider::JSON_RPC_PAYLOAD_CONTENT_ENCODING.to_owned()),
            cache_metadata: json!({
                "source": "json-rpc",
                "method": "eth_getBlockByHash",
                "fetch_mode": "block_hash",
                "digest_scope": "json_rpc_response_body"
            }),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    let requested_hash = requested_block.block_hash.clone();
    let returned_hash = returned_block.block_hash.clone();
    let (mismatched_url, mismatched_server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let first_param = body
            .get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.first())
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();

        match (method, first_param.as_str()) {
            ("eth_getBlockByHash", hash) if hash == requested_hash => json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": rpc_block_bundle_payload(&returned_block),
            }),
            _ => panic!("unexpected RPC request: {body}"),
        }
    }))
    .await?;
    let mismatched_provider = provider::JsonRpcProvider::new(&mismatched_url)?;

    let hash_error = mismatched_provider
        .cache_fill_full_block_by_hash(
            database.pool(),
            chain,
            &requested_block.block_hash,
            requested_block.block_number,
        )
        .await
        .expect_err("cache-fill must validate the returned block hash after digest authorization");
    assert!(
        hash_error.to_string().contains(&format!(
            "provider cache-fill returned block {returned_hash} for requested hash {}",
            requested_block.block_hash
        )),
        "unexpected error: {hash_error:#}"
    );

    mismatched_server.abort();
    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn cache_fill_requires_retained_digest() -> Result<()> {
    let database = TestDatabase::new().await?;
    let block = provider_block(
        "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
        Some("0xb1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1"),
        101,
    );
    bigname_storage::upsert_raw_payload_cache_metadata(
        database.pool(),
        &[bigname_storage::RawPayloadCacheMetadataUpsert {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block.block_hash.clone(),
            payload_kind: provider::RAW_PAYLOAD_KIND_FULL_BLOCK.to_owned(),
            digest_algorithm: None,
            retained_digest: None,
            block_number: Some(block.block_number),
            payload_size_bytes: 0,
            content_type: Some(provider::JSON_RPC_PAYLOAD_CONTENT_TYPE.to_owned()),
            content_encoding: Some(provider::JSON_RPC_PAYLOAD_CONTENT_ENCODING.to_owned()),
            cache_metadata: json!({
                "source": "json-rpc",
                "method": "eth_getBlockByHash",
                "fetch_mode": "block_hash",
                "digest_scope": "json_rpc_response_body"
            }),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    let (provider, server) = bundle_provider(vec![block.clone()]).await?;

    let error = provider
        .cache_fill_full_block_by_hash(
            database.pool(),
            "ethereum-mainnet",
            &block.block_hash,
            block.block_number,
        )
        .await
        .expect_err("cache-fill must reject metadata without a retained digest");
    assert!(
        error.to_string().contains("has no retained digest"),
        "unexpected error: {error:#}"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn cache_fill_rejects_digest_mismatch() -> Result<()> {
    let database = TestDatabase::new().await?;
    let block = provider_block(
        "0xa2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2",
        Some("0xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"),
        102,
    );
    bigname_storage::upsert_raw_payload_cache_metadata(
        database.pool(),
        &[bigname_storage::RawPayloadCacheMetadataUpsert {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block.block_hash.clone(),
            payload_kind: provider::RAW_PAYLOAD_KIND_FULL_BLOCK.to_owned(),
            digest_algorithm: Some("keccak256".to_owned()),
            retained_digest: Some(
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned(),
            ),
            block_number: Some(block.block_number),
            payload_size_bytes: 1,
            content_type: Some(provider::JSON_RPC_PAYLOAD_CONTENT_TYPE.to_owned()),
            content_encoding: Some(provider::JSON_RPC_PAYLOAD_CONTENT_ENCODING.to_owned()),
            cache_metadata: json!({
                "source": "json-rpc",
                "method": "eth_getBlockByHash",
                "fetch_mode": "block_hash",
                "digest_scope": "json_rpc_response_body"
            }),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    let (provider, server) = bundle_provider(vec![block.clone()]).await?;

    let error = provider
        .cache_fill_full_block_by_hash(
            database.pool(),
            "ethereum-mainnet",
            &block.block_hash,
            block.block_number,
        )
        .await
        .expect_err("cache-fill must reject mismatched retained digests");
    assert!(
        error
            .to_string()
            .contains("raw payload cache digest mismatch"),
        "unexpected error: {error:#}"
    );

    server.abort();
    database.cleanup().await
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
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_registrar_l1/v1.toml',
                DEFAULT
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
        None,
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
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_reverse_l1/v1.toml',
                DEFAULT
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
    let reverse_address = "0x0000000000d8e504002cc26e3ec46d81971c1664";
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
                'ensip15@ens-normalize-0.1.1',
                'manifests/basenames/basenames_base_primary/v1.toml',
                DEFAULT
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
        logs: vec![rpc_l2_reverse_name_log_payload(
            &canonical_head,
            reverse_address,
            claimed_address,
            "alice.base.eth",
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
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'raw_name' FROM normalized_events WHERE event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "alice.base.eth".to_owned()
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
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_reverse_l1/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'ens',
                    'ens_v1_registry_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_registry_l1/v1.toml',
                    DEFAULT
                ),
                (
                    3,
                    1,
                    'ens',
                    'ens_v1_resolver_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_resolver_l1/v1.toml',
                    DEFAULT
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
        "public_resolver",
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
            rpc_reverse_claimed_log_payload(
                &canonical_head,
                reverse_address,
                claimed_address,
                0,
            ),
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
    let reverse_address = "0x0000000000d8e504002cc26e3ec46d81971c1664";
    let registry_address = "0xb94704422c2a1e396835a571837aa5ae53285a95";
    let resolver_address = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd";
    let claimed_address = "0x1234567890abcdef1234567890abcdef12345678";
    let reverse_node = base_reverse_node_for_address(claimed_address);

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
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_primary/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'basenames',
                    'basenames_base_registry',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_registry/v1.toml',
                    DEFAULT
                ),
                (
                    3,
                    1,
                    'basenames',
                    'basenames_base_resolver',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_resolver/v1.toml',
                    DEFAULT
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
            rpc_l2_reverse_name_log_payload(
                &canonical_head,
                reverse_address,
                claimed_address,
                "alice.base.eth",
                0,
            ),
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
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_registrar_l1/v1.toml',
                DEFAULT
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
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_registry_l1/v1.toml',
                DEFAULT
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
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_resolver_l1/v1.toml',
                DEFAULT
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
        "public_resolver",
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
        vec!["addr:60".to_owned(), "text:com.twitter".to_owned()]
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
async fn reconcile_fetched_heads_gates_discovered_ensv1_resolver_local_facts_by_profile()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let registrar_contract_instance_id = Uuid::from_u128(0x381);
    let registry_contract_instance_id = Uuid::from_u128(0x382);
    let public_resolver_seed_contract_instance_id = Uuid::from_u128(0x383);
    let supported_resolver_contract_instance_id = Uuid::from_u128(0x384);
    let pending_resolver_contract_instance_id = Uuid::from_u128(0x385);
    let unsupported_resolver_contract_instance_id = Uuid::from_u128(0x386);
    let registrar_address = "0x0000000000000000000000000000000000000381";
    let registry_address = "0x0000000000000000000000000000000000000382";
    let public_resolver_seed_address = "0x0000000000000000000000000000000000000383";
    let supported_resolver_address = "0x0000000000000000000000000000000000000384";
    let pending_resolver_address = "0x0000000000000000000000000000000000000385";
    let unsupported_resolver_address = "0x0000000000000000000000000000000000000386";
    let public_resolver_code_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";

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
                    'ens_v1_registrar_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_registrar_l1/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'ens',
                    'ens_v1_registry_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_registry_l1/v1.toml',
                    DEFAULT
                ),
                (
                    3,
                    1,
                    'ens',
                    'ens_v1_resolver_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_resolver_l1/v1.toml',
                    DEFAULT
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for ENSv1 resolver profile gate test")?;

    for (contract_instance_id, address, manifest_id, role) in [
        (
            registrar_contract_instance_id,
            registrar_address,
            1,
            "registrar",
        ),
        (
            registry_contract_instance_id,
            registry_address,
            2,
            "registry",
        ),
        (
            public_resolver_seed_contract_instance_id,
            public_resolver_seed_address,
            3,
            "public_resolver",
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
            Some(manifest_id),
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            manifest_id,
            role,
            contract_instance_id,
            address,
            "none",
            None,
            None,
        )
        .await?;
    }

    for (contract_instance_id, address) in [
        (
            supported_resolver_contract_instance_id,
            supported_resolver_address,
        ),
        (
            pending_resolver_contract_instance_id,
            pending_resolver_address,
        ),
        (
            unsupported_resolver_contract_instance_id,
            unsupported_resolver_address,
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
            Some(3),
        )
        .await?;
        insert_active_discovery_edge(
            database.pool(),
            "ethereum-mainnet",
            "resolver",
            registry_contract_instance_id,
            contract_instance_id,
            Some(2),
        )
        .await?;
    }

    upsert_raw_code_hashes(
        database.pool(),
        &[
            RawCodeHash {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999"
                    .to_owned(),
                block_number: 41,
                contract_address: public_resolver_seed_address.to_owned(),
                code_hash: public_resolver_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawCodeHash {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999"
                    .to_owned(),
                block_number: 41,
                contract_address: supported_resolver_address.to_owned(),
                code_hash: public_resolver_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawCodeHash {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999"
                    .to_owned(),
                block_number: 41,
                contract_address: unsupported_resolver_address.to_owned(),
                code_hash: "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0x3838383838383838383838383838383838383838383838383838383838383838",
        Some("0x3737373737373737373737373737373737373737373737373737373737373737"),
        52,
    );
    let alice_namehash = namehash_for_dns_name(&dns_encoded_eth_name("alice"));
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
                supported_resolver_address,
                1,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                supported_resolver_address,
                &alice_namehash,
                "supported.eth",
                2,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                supported_resolver_address,
                &alice_namehash,
                7,
                3,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &alice_namehash,
                pending_resolver_address,
                4,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                pending_resolver_address,
                &alice_namehash,
                "pending.eth",
                5,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                pending_resolver_address,
                &alice_namehash,
                8,
                6,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &alice_namehash,
                unsupported_resolver_address,
                7,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                unsupported_resolver_address,
                &alice_namehash,
                "unsupported.eth",
                8,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                unsupported_resolver_address,
                &alice_namehash,
                9,
                9,
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
    .expect("ENSv1 resolver profile gate reconciliation must update task state");

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        10
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'raw_name' FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "supported.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'record_version' FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind = 'RecordVersionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "7".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind IN ('RecordChanged', 'RecordVersionChanged') AND log_index = ANY($1::BIGINT[])"
        )
        .bind(vec![5_i64, 6, 8, 9])
        .fetch_one(database.pool())
        .await?,
        0
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_gates_basenames_dynamic_resolver_local_facts_by_l2_profile()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let registrar_contract_instance_id = Uuid::from_u128(0x391);
    let registry_contract_instance_id = Uuid::from_u128(0x392);
    let seed_resolver_contract_instance_id = Uuid::from_u128(0x393);
    let supported_resolver_contract_instance_id = Uuid::from_u128(0x394);
    let pending_resolver_contract_instance_id = Uuid::from_u128(0x395);
    let unsupported_resolver_contract_instance_id = Uuid::from_u128(0x396);
    let registrar_address = "0x0000000000000000000000000000000000000391";
    let registry_address = "0x0000000000000000000000000000000000000392";
    let seed_resolver_address = "0x0000000000000000000000000000000000000393";
    let supported_resolver_address = "0x0000000000000000000000000000000000000394";
    let pending_resolver_address = "0x0000000000000000000000000000000000000395";
    let unsupported_resolver_address = "0x0000000000000000000000000000000000000396";
    let l2_resolver_code_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";

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
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_registrar/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'basenames',
                    'basenames_base_registry',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_registry/v1.toml',
                    DEFAULT
                ),
                (
                    3,
                    1,
                    'basenames',
                    'basenames_base_resolver',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_resolver/v1.toml',
                    DEFAULT
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for Basenames resolver profile gate test")?;

    for (contract_instance_id, chain, contract_kind) in [
        (registrar_contract_instance_id, "base-mainnet", "contract"),
        (registry_contract_instance_id, "base-mainnet", "root"),
        (
            seed_resolver_contract_instance_id,
            "base-mainnet",
            "contract",
        ),
        (
            supported_resolver_contract_instance_id,
            "base-mainnet",
            "contract",
        ),
        (
            pending_resolver_contract_instance_id,
            "base-mainnet",
            "contract",
        ),
        (
            unsupported_resolver_contract_instance_id,
            "base-mainnet",
            "contract",
        ),
    ] {
        insert_contract_instance(database.pool(), contract_instance_id, chain, contract_kind)
            .await?;
    }

    for (contract_instance_id, address, manifest_id) in [
        (registrar_contract_instance_id, registrar_address, 1),
        (registry_contract_instance_id, registry_address, 2),
        (seed_resolver_contract_instance_id, seed_resolver_address, 3),
        (
            supported_resolver_contract_instance_id,
            supported_resolver_address,
            3,
        ),
        (
            pending_resolver_contract_instance_id,
            pending_resolver_address,
            3,
        ),
        (
            unsupported_resolver_contract_instance_id,
            unsupported_resolver_address,
            3,
        ),
    ] {
        insert_active_contract_instance_address(
            database.pool(),
            contract_instance_id,
            "base-mainnet",
            address,
            Some(manifest_id),
        )
        .await?;
    }

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
        seed_resolver_contract_instance_id,
        seed_resolver_address,
        "none",
        None,
        None,
    )
    .await?;
    for contract_instance_id in [
        supported_resolver_contract_instance_id,
        pending_resolver_contract_instance_id,
        unsupported_resolver_contract_instance_id,
    ] {
        insert_active_discovery_edge(
            database.pool(),
            "base-mainnet",
            "resolver",
            registry_contract_instance_id,
            contract_instance_id,
            Some(2),
        )
        .await?;
    }

    upsert_raw_code_hashes(
        database.pool(),
        &[
            RawCodeHash {
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999"
                    .to_owned(),
                block_number: 41,
                contract_address: seed_resolver_address.to_owned(),
                code_hash: l2_resolver_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawCodeHash {
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999"
                    .to_owned(),
                block_number: 41,
                contract_address: supported_resolver_address.to_owned(),
                code_hash: l2_resolver_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawCodeHash {
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999"
                    .to_owned(),
                block_number: 41,
                contract_address: unsupported_resolver_address.to_owned(),
                code_hash: "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0x3939393939393939393939393939393939393939393939393939393939393939",
        Some("0x3838383838383838383838383838383838383838383838383838383838383838"),
        52,
    );
    let alice_namehash = namehash_for_dns_name(&dns_encoded_base_eth_name("alice"));
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![
            rpc_basenames_name_registered_log_payload(
                &canonical_head,
                registrar_address,
                "alice",
                canonical_head.block_timestamp_unix_secs + 31_536_000,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &alice_namehash,
                supported_resolver_address,
                1,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                supported_resolver_address,
                &alice_namehash,
                "supported.base.eth",
                2,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                supported_resolver_address,
                &alice_namehash,
                7,
                3,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &alice_namehash,
                pending_resolver_address,
                4,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                pending_resolver_address,
                &alice_namehash,
                "pending.base.eth",
                5,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                pending_resolver_address,
                &alice_namehash,
                8,
                6,
            ),
            rpc_registry_new_resolver_log_payload_for_namehash(
                &canonical_head,
                registry_address,
                &alice_namehash,
                unsupported_resolver_address,
                7,
            ),
            rpc_resolver_name_changed_log_payload_for_namehash(
                &canonical_head,
                unsupported_resolver_address,
                &alice_namehash,
                "unsupported.base.eth",
                8,
            ),
            rpc_resolver_version_changed_log_payload_for_namehash(
                &canonical_head,
                unsupported_resolver_address,
                &alice_namehash,
                9,
                9,
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
    .expect("Basenames resolver profile gate reconciliation must update task state");

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        10
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'raw_name' FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "supported.base.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'record_version' FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind = 'RecordVersionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "7".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE derivation_kind = 'ens_v1_unwrapped_authority' AND event_kind IN ('RecordChanged', 'RecordVersionChanged') AND log_index = ANY($1::BIGINT[])"
        )
        .bind(vec![5_i64, 6, 8, 9])
        .fetch_one(database.pool())
        .await?,
        0
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_ensv2_resolver_and_permission_events() -> Result<()> {
    let database = TestDatabase::new().await?;
    let registry_contract_instance_id = Uuid::from_u128(0x371);
    let registry_address = "0x0000000000000000000000000000000000000371";
    let resolver_address = "0x0000000000000000000000000000000000000372";
    let owner_address = "0x00000000000000000000000000000000000000aa";
    let sender_address = "0x00000000000000000000000000000000000000bb";
    let operator_address = "0x00000000000000000000000000000000000000cc";
    let record_address = "0x00000000000000000000000000000000000000dd";
    let token_id = hex_string(&abi_word_u64(1));
    let upstream_resource = hex_string(&abi_word_u64(42));
    let alice_dns_name = dns_encoded_eth_name("alice");
    let alice_namehash = namehash_for_dns_name(&alice_dns_name);
    let new_role_bitmap = hex_string(&abi_word_u64(1));
    let zero_role_bitmap = hex_string(&abi_word_u64(0));

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
                    'ens_v2_registry_l1',
                    'ethereum-mainnet',
                    'ens_v2',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v2_registry_l1/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'ens',
                    'ens_v2_resolver_l1',
                    'ethereum-mainnet',
                    'ens_v2',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v2_resolver_l1/v1.toml',
                    DEFAULT
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for ENSv2 resolver reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        registry_address,
        Some(1),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        1,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registry",
        registry_contract_instance_id,
        registry_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_discovery_rule(
        database.pool(),
        1,
        "resolver",
        "registry",
        "reachable_from_root",
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xf1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1",
        Some("0xe1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1"),
        61,
    );
    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        logs: vec![
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x0",
                "address": registry_address,
                "topics": [
                    ens_v2_label_registered_topic0(),
                    token_id.clone(),
                    labelhash_hex("alice"),
                    hex_string(&abi_word_address(sender_address))
                ],
                "data": encode_ens_v2_label_registered_log_data(
                    "alice",
                    owner_address,
                    canonical_head.block_timestamp_unix_secs + 31_536_000,
                )
            }),
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x1",
                "address": registry_address,
                "topics": [
                    ens_v2_token_resource_topic0(),
                    token_id.clone(),
                    upstream_resource.clone()
                ],
                "data": "0x"
            }),
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x2",
                "address": registry_address,
                "topics": [
                    ens_v2_resolver_updated_topic0(),
                    token_id.clone(),
                    hex_string(&abi_word_address(resolver_address)),
                    hex_string(&abi_word_address(sender_address))
                ],
                "data": "0x"
            }),
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x3",
                "address": resolver_address,
                "topics": [
                    ens_v2_resolver_address_changed_topic0(),
                    alice_namehash.clone()
                ],
                "data": encode_ens_v2_resolver_address_changed_log_data(
                    60,
                    &decode_hex_string(record_address),
                )
            }),
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x4",
                "address": resolver_address,
                "topics": [
                    ens_v2_named_resource_topic0(),
                    upstream_resource.clone()
                ],
                "data": encode_dynamic_bytes_log_data(&alice_dns_name)
            }),
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x5",
                "address": resolver_address,
                "topics": [
                    ens_v2_eac_roles_changed_topic0(),
                    upstream_resource.clone(),
                    hex_string(&abi_word_address(operator_address))
                ],
                "data": encode_eac_roles_changed_log_data(&zero_role_bitmap, &new_role_bitmap)
            }),
            json!({
                "blockHash": canonical_head.block_hash.clone(),
                "blockNumber": format!("0x{:x}", canonical_head.block_number),
                "transactionHash": transaction_hash_for_block(&canonical_head),
                "transactionIndex": "0x0",
                "logIndex": "0x6",
                "address": resolver_address,
                "topics": [
                    ens_v2_alias_changed_topic0(),
                    keccak256_hex(&alice_dns_name),
                    keccak256_hex(&[])
                ],
                "data": encode_two_dynamic_bytes_log_data(&alice_dns_name, &[])
            }),
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
    .expect("ENSv2 resolver reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(61));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM discovery_edges WHERE edge_kind = 'resolver' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT cia.address FROM discovery_edges de JOIN contract_instance_addresses cia ON cia.contract_instance_id = de.to_contract_instance_id WHERE de.edge_kind = 'resolver'"
        )
        .fetch_one(database.pool())
        .await?,
        resolver_address.to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordChanged' AND derivation_kind = 'ens_v2_resolver'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT logical_name_id FROM normalized_events WHERE event_kind = 'RecordChanged' AND derivation_kind = 'ens_v2_resolver'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens:alice.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'record_key' FROM normalized_events WHERE event_kind = 'RecordChanged' AND derivation_kind = 'ens_v2_resolver'"
        )
        .fetch_one(database.pool())
        .await?,
        "addr:60".to_owned()
    );
    let record_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM normalized_events WHERE event_kind = 'RecordChanged' AND derivation_kind = 'ens_v2_resolver'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        record_resource_id,
        sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id FROM surface_bindings WHERE logical_name_id = 'ens:alice.eth'"
        )
        .fetch_one(database.pool())
        .await?
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND derivation_kind = 'ens_v2_permissions'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT logical_name_id FROM normalized_events WHERE event_kind = 'PermissionChanged' AND derivation_kind = 'ens_v2_permissions'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens:alice.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'scope'->>'kind' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND derivation_kind = 'ens_v2_permissions'"
        )
        .fetch_one(database.pool())
        .await?,
        "resolver".to_owned()
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT after_state->'effective_powers' ? 'set_addr' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND derivation_kind = 'ens_v2_permissions'"
        )
        .fetch_one(database.pool())
        .await?
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'PermissionChanged' AND derivation_kind = 'ens_v2_permissions'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v2_resolver_l1".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'alias_state' FROM normalized_events WHERE event_kind = 'AliasChanged' AND derivation_kind = 'ens_v2_resolver'"
        )
        .fetch_one(database.pool())
        .await?,
        "removed".to_owned()
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT (after_state->>'active')::BOOLEAN FROM normalized_events WHERE event_kind = 'AliasChanged' AND derivation_kind = 'ens_v2_resolver'"
        )
        .fetch_one(database.pool())
        .await?
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PreimageObserved' AND logical_name_id IS NULL AND resource_id IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(after_state->>'source_event' ORDER BY after_state->>'source_event') FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        vec![
            "AliasChanged".to_owned(),
            "LabelRegistered".to_owned(),
            "NamedResource".to_owned(),
        ]
    );
    let resolver_preimage_fact_refs = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT after_state->>'source_event', raw_fact_ref->>'data_hex'
        FROM normalized_events
        WHERE event_kind = 'PreimageObserved'
          AND after_state->>'source_event' IN ('AliasChanged', 'NamedResource')
        ORDER BY after_state->>'source_event'
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        resolver_preimage_fact_refs,
        vec![
            (
                "AliasChanged".to_owned(),
                encode_two_dynamic_bytes_log_data(&alice_dns_name, &[])
                    .trim_start_matches("0x")
                    .to_owned(),
            ),
            (
                "NamedResource".to_owned(),
                encode_dynamic_bytes_log_data(&alice_dns_name)
                    .trim_start_matches("0x")
                    .to_owned(),
            ),
        ]
    );

    let pre_admission_hash = "0xf0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0";
    let pre_admission_tx = "0x0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f";
    sqlx::query(
        r#"
            INSERT INTO chain_lineage (
                chain_id,
                block_hash,
                parent_hash,
                block_number,
                block_timestamp,
                canonicality_state
            )
            VALUES (
                'ethereum-mainnet',
                $1,
                '0xe0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0',
                60,
                to_timestamp($2),
                'canonical'
            )
            "#,
    )
    .bind(pre_admission_hash)
    .bind(canonical_head.block_timestamp_unix_secs - 12)
    .execute(database.pool())
    .await
    .context("failed to insert pre-admission raw block")?;
    sqlx::query(
        r#"
            INSERT INTO raw_logs (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state
            )
            VALUES (
                'ethereum-mainnet',
                $1,
                60,
                $2,
                0,
                0,
                $3,
                $4,
                $5,
                'canonical'
            )
            "#,
    )
    .bind(pre_admission_hash)
    .bind(pre_admission_tx)
    .bind(resolver_address)
    .bind(vec![
        ens_v2_resolver_address_changed_topic0(),
        alice_namehash.clone(),
    ])
    .bind(decode_hex_string(
        &encode_ens_v2_resolver_address_changed_log_data(60, &decode_hex_string(record_address)),
    ))
    .execute(database.pool())
    .await
    .context("failed to insert pre-admission resolver raw log")?;
    sqlx::query(
        r#"
            INSERT INTO raw_logs (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state
            )
            VALUES (
                'ethereum-mainnet',
                $1,
                60,
                $2,
                0,
                1,
                $3,
                $4,
                $5,
                'canonical'
            )
            "#,
    )
    .bind(pre_admission_hash)
    .bind(pre_admission_tx)
    .bind(resolver_address)
    .bind(vec![
        ens_v2_eac_roles_changed_topic0(),
        upstream_resource.clone(),
        hex_string(&abi_word_address(operator_address)),
    ])
    .bind(decode_hex_string(&encode_eac_roles_changed_log_data(
        &zero_role_bitmap,
        &new_role_bitmap,
    )))
    .execute(database.pool())
    .await
    .context("failed to insert pre-admission permissions raw log")?;

    bigname_adapters::sync_ens_v2_resolver(database.pool(), "ethereum-mainnet").await?;
    bigname_adapters::sync_ens_v2_permissions(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordChanged' AND derivation_kind = 'ens_v2_resolver'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND derivation_kind = 'ens_v2_permissions'"
        )
        .fetch_one(database.pool())
        .await?,
        1
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
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_registrar/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'basenames',
                    'basenames_base_registry',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_registry/v1.toml',
                    DEFAULT
                ),
                (
                    3,
                    1,
                    'basenames',
                    'basenames_base_resolver',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_resolver/v1.toml',
                    DEFAULT
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
            rpc_basenames_name_registered_log_payload(
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

fn rpc_current_name_wrapped_log_payload(block: &ProviderBlock) -> Value {
    let dns_name = dns_encoded_test_name();
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "address": "0x0000000000000000000000000000000000000001",
        "topics": [
            keccak256_hex(b"NameWrapped(bytes32,bytes,address,uint32,uint64)"),
            namehash_for_dns_name(&dns_name)
        ],
        "data": encode_name_wrapped_log_data(&dns_name)
    })
}
