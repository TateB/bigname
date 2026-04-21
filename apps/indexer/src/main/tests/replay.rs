#[tokio::test]
async fn replay_normalized_events_runs_full_persisted_raw_adapter_boundary() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let reverse_contract_instance_id = Uuid::from_u128(0x900);
    let reverse_address = "0x00000000000000000000000000000000000000af";
    let claimed_address = "0x1234567890abcdef1234567890abcdef12345678";
    let unrelated_claimed_address = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";
    let block = provider_block(
        "0x9090909090909090909090909090909090909090909090909090909090909090",
        Some("0x8080808080808080808080808080808080808080808080808080808080808080"),
        90,
    );
    let unrelated_block = provider_block(
        "0x9292929292929292929292929292929292929292929292929292929292929292",
        Some(&block.block_hash),
        92,
    );

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        10,
        chain,
        "ens_v1_reverse_l1",
        reverse_contract_instance_id,
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &block,
        reverse_address,
        claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &unrelated_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &unrelated_block,
        reverse_address,
        unrelated_claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;

    let outcome = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: block.block_number,
                to_block: block.block_number,
            },
        },
    )
    .await?;

    assert_eq!(outcome.selected_block_count, 1);
    assert_eq!(outcome.canonical_raw_log_count, 1);
    assert_eq!(outcome.scanned_raw_log_count, 2);
    assert_eq!(outcome.matched_raw_log_count, 1);
    assert_eq!(outcome.normalized_event_synced_count, 1);
    assert_eq!(outcome.normalized_event_inserted_count, 1);
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
            "SELECT after_state->>'reverse_name' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_name_for_address(claimed_address)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT raw_fact_ref->>'block_hash' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        block.block_hash
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE raw_fact_ref->>'block_hash' = $1"
        )
        .bind(&unrelated_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_is_upsert_only_for_stale_selected_payloads() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let contract_instance_id = Uuid::from_u128(0x905);
    let watched_address = "0x0000000000000000000000000000000000000001";
    let block = provider_block(
        "0xf5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5",
        Some("0x8585858585858585858585858585858585858585858585858585858585858585"),
        106,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        5,
        chain,
        contract_instance_id,
        watched_address,
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &block,
        watched_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_stale_name_wrapped_preimage_event(database.pool(), chain, 5, &block, watched_address)
        .await?;

    let error = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: block.block_number,
                to_block: block.block_number,
            },
        },
    )
    .await
    .expect_err("stale selected normalized-event payload must not be replaced");

    assert!(
        format!("{error:?}").contains("normalized event identity mismatch"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'decoded_name' FROM normalized_events"
        )
        .fetch_one(database.pool())
        .await?,
        "stale.eth"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_is_idempotent_without_checkpoint_mutation() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let contract_instance_id = Uuid::from_u128(0x901);
    let watched_address = "0x0000000000000000000000000000000000000001";
    let block = provider_block(
        "0x9191919191919191919191919191919191919191919191919191919191919191",
        Some("0x8181818181818181818181818181818181818181818181818181818181818181"),
        91,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        1,
        chain,
        contract_instance_id,
        watched_address,
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &block,
        watched_address,
        0,
        CanonicalityState::Canonical,
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
        VALUES ($1, $2, $3, $2, $3, $2, $3)
        "#,
    )
    .bind(chain)
    .bind(&block.block_hash)
    .bind(block.block_number)
    .execute(database.pool())
    .await
    .context("failed to insert checkpoint guard row for replay test")?;

    let request = RawFactNormalizedEventReplayRequest {
        deployment_profile: "mainnet".to_owned(),
        chain: chain.to_owned(),
        selection: RawFactNormalizedEventReplaySelection::BlockRange {
            from_block: block.block_number,
            to_block: block.block_number,
        },
    };

    let first = replay_raw_fact_normalized_events(database.pool(), request.clone()).await?;

    assert_eq!(first.selected_block_count, 1);
    assert_eq!(first.canonical_raw_log_count, 1);
    assert_eq!(first.scanned_raw_log_count, 1);
    assert_eq!(first.matched_raw_log_count, 1);
    assert_eq!(first.normalized_event_synced_count, 1);
    assert_eq!(first.normalized_event_inserted_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'decoded_name' FROM normalized_events"
        )
        .fetch_one(database.pool())
        .await?,
        "wrapped.eth".to_owned()
    );

    let second = replay_raw_fact_normalized_events(database.pool(), request).await?;

    assert_eq!(second.selected_block_count, 1);
    assert_eq!(second.canonical_raw_log_count, 1);
    assert_eq!(second.normalized_event_synced_count, 1);
    assert_eq!(second.normalized_event_inserted_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonical_block_hash FROM chain_checkpoints WHERE chain_id = $1"
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        block.block_hash
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_blocks")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_uses_only_persisted_canonical_raw_log_inputs() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let watched_address = "0x0000000000000000000000000000000000000001";
    let canonical_block = provider_block(
        "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        101,
    );
    let observed_block = provider_block(
        "0xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        102,
    );
    let orphaned_block = provider_block(
        "0xc3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        103,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        2,
        chain,
        Uuid::from_u128(0x902),
        watched_address,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &canonical_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &observed_block,
        CanonicalityState::Observed,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &orphaned_block,
        CanonicalityState::Orphaned,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &canonical_block,
        watched_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &observed_block,
        watched_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &orphaned_block,
        watched_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    let outcome = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: 101,
                to_block: 103,
            },
        },
    )
    .await?;

    assert_eq!(outcome.selected_block_count, 1);
    assert_eq!(outcome.canonical_raw_log_count, 1);
    assert_eq!(outcome.normalized_event_inserted_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(block_hash ORDER BY block_hash) FROM normalized_events"
        )
        .fetch_one(database.pool())
        .await?,
        vec![canonical_block.block_hash]
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_rejects_deployment_profile_outside_active_manifest_scope()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let contract_instance_id = Uuid::from_u128(0x904);
    let watched_address = "0x0000000000000000000000000000000000000001";
    let block = provider_block(
        "0xe5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        105,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        4,
        chain,
        contract_instance_id,
        watched_address,
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &block,
        watched_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    let error = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "sepolia-dev".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: block.block_number,
                to_block: block.block_number,
            },
        },
    )
    .await
    .expect_err("mismatched deployment profile must be rejected");

    assert!(
        format!("{error:?}")
            .contains("does not match active manifest/discovery corpus profile mainnet"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_rejects_mixed_canonicality_raw_logs() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let watched_address = "0x0000000000000000000000000000000000000001";
    let block = provider_block(
        "0xd4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        104,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        3,
        chain,
        Uuid::from_u128(0x903),
        watched_address,
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &block,
        watched_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &block,
        watched_address,
        1,
        CanonicalityState::Observed,
    )
    .await?;

    let error = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockHashes(vec![
                block.block_hash.clone(),
            ]),
        },
    )
    .await
    .expect_err("mixed canonicality raw logs must be rejected");

    assert!(
        format!("{error:?}").contains("refusing block-hash-scoped adapter replay"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        0
    );

    database.cleanup().await
}

async fn insert_active_replay_watched_contract(
    pool: &PgPool,
    manifest_id: i64,
    chain: &str,
    contract_instance_id: Uuid,
    address: &str,
) -> Result<()> {
    insert_active_replay_watched_contract_with_source_family(
        pool,
        manifest_id,
        chain,
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
        "name_wrapper",
    )
    .await
}

async fn insert_stale_name_wrapped_preimage_event(
    pool: &PgPool,
    chain: &str,
    source_manifest_id: i64,
    block: &ProviderBlock,
    emitting_address: &str,
) -> Result<()> {
    let dns_name = dns_encoded_test_name();
    let transaction_hash = transaction_hash_for_block(block);
    let event_identity = format!(
        "raw_log_preimage_observed:{}:{}:{}:{}:{}",
        source_manifest_id,
        block.block_hash,
        transaction_hash,
        0,
        emitting_address.to_ascii_lowercase()
    );
    let data_hex = encode_name_wrapped_log_data(&dns_name)
        .trim_start_matches("0x")
        .to_owned();
    let raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": chain,
        "block_hash": block.block_hash,
        "block_number": block.block_number,
        "transaction_hash": transaction_hash,
        "transaction_index": 0,
        "log_index": 0,
        "emitting_address": emitting_address.to_ascii_lowercase(),
        "topic0": name_wrapped_topic0(),
        "topic1": namehash_for_dns_name(&dns_name),
        "topic2": null,
        "data_hex": data_hex,
    });

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
            $1,
            'ens',
            'PreimageObserved',
            'ens_v1_wrapper_l1',
            1,
            $2,
            $3,
            $4,
            $5,
            $6,
            0,
            $7::jsonb,
            'raw_log_preimage_observation',
            'canonical',
            '{}'::jsonb,
            '{"source_event":"NameWrapped","decoded_name":"stale.eth"}'::jsonb
        )
        "#,
    )
    .bind(event_identity)
    .bind(source_manifest_id)
    .bind(chain)
    .bind(block.block_number)
    .bind(&block.block_hash)
    .bind(transaction_hash)
    .bind(raw_fact_ref.to_string())
    .execute(pool)
    .await
    .context("failed to insert stale normalized event for replay test")?;

    Ok(())
}

async fn insert_active_replay_watched_contract_with_source_family(
    pool: &PgPool,
    manifest_id: i64,
    chain: &str,
    source_family: &str,
    contract_instance_id: Uuid,
    address: &str,
    role: &str,
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
            'ens',
            $3,
            $2,
            'ens_v1',
            'active',
            'uts46-v1',
            ('manifests/ens/' || $3 || '/v1.toml'),
            '{}'::jsonb
        )
        "#,
    )
    .bind(manifest_id)
    .bind(chain)
    .bind(source_family)
    .execute(pool)
    .await
    .context("failed to insert manifest_versions for replay test")?;
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
        role,
        contract_instance_id,
        address,
        "none",
        None,
        None,
    )
    .await
}
