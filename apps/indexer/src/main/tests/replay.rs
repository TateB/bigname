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
    assert_eq!(first.scanned_raw_log_count, 2);
    assert_eq!(first.matched_raw_log_count, 2);
    assert_eq!(first.normalized_event_synced_count, 6);
    assert_eq!(first.normalized_event_inserted_count, 6);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        6
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'decoded_name' FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        "wrapped.eth".to_owned()
    );
    assert_eq!(count_wrapper_replay_events(database.pool()).await?, 5);

    let second = replay_raw_fact_normalized_events(database.pool(), request).await?;

    assert_eq!(second.selected_block_count, 1);
    assert_eq!(second.canonical_raw_log_count, 1);
    assert_eq!(second.scanned_raw_log_count, 2);
    assert_eq!(second.matched_raw_log_count, 2);
    assert_eq!(second.normalized_event_synced_count, 6);
    assert_eq!(second.normalized_event_inserted_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        6
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
async fn replay_normalized_events_replays_canonical_name_wrapper_raw_logs_idempotently()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let wrapper_contract_instance_id = Uuid::from_u128(0x906);
    let wrapper_address = "0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401";
    let block = provider_block(
        "0x9696969696969696969696969696969696969696969696969696969696969696",
        Some("0x8686868686868686868686868686868686868686868686868686868686868686"),
        96,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        6,
        chain,
        wrapper_contract_instance_id,
        wrapper_address,
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &block,
        wrapper_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;

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
    assert_eq!(first.scanned_raw_log_count, 2);
    assert_eq!(first.matched_raw_log_count, 2);
    assert_eq!(first.normalized_event_inserted_count, 6);

    let wrapper_event_count = count_wrapper_replay_events(database.pool()).await?;
    assert_eq!(wrapper_event_count, 5);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_events
            WHERE derivation_kind = 'ens_v1_unwrapped_authority'
              AND source_family = 'ens_v1_wrapper_l1'
              AND logical_name_id = 'ens:wrapped.eth'
              AND raw_fact_ref->>'block_hash' = $1
            "#
        )
        .bind(&block.block_hash)
        .fetch_one(database.pool())
        .await?,
        wrapper_event_count
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            r#"
            SELECT ARRAY_AGG(event_kind ORDER BY event_kind)
            FROM normalized_events
            WHERE derivation_kind = 'ens_v1_unwrapped_authority'
              AND source_family = 'ens_v1_wrapper_l1'
            "#
        )
        .fetch_one(database.pool())
        .await?,
        vec![
            "AuthorityEpochChanged".to_owned(),
            "ExpiryChanged".to_owned(),
            "PermissionScopeChanged".to_owned(),
            "SurfaceBound".to_owned(),
            "TokenControlTransferred".to_owned(),
        ]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_events
            WHERE derivation_kind = 'raw_log_preimage_observation'
              AND event_kind = 'PreimageObserved'
              AND after_state->>'source_event' = 'NameWrapped'
            "#
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_no_duplicate_normalized_event_identities(database.pool()).await?;

    let second = replay_raw_fact_normalized_events(database.pool(), request).await?;

    assert_eq!(second.selected_block_count, 1);
    assert_eq!(second.canonical_raw_log_count, 1);
    assert_eq!(second.scanned_raw_log_count, 2);
    assert_eq!(second.matched_raw_log_count, 2);
    assert_eq!(second.normalized_event_inserted_count, 0);
    assert_eq!(
        count_wrapper_replay_events(database.pool()).await?,
        wrapper_event_count
    );
    assert_no_duplicate_normalized_event_identities(database.pool()).await?;

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
    assert_eq!(outcome.normalized_event_inserted_count, 6);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1"
        )
        .bind(&canonical_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        6
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash <> $1"
        )
        .bind(&canonical_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
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

#[tokio::test]
async fn replay_normalized_events_respects_ensv1_dynamic_resolver_watch_target_range()
-> Result<()> {
    assert_dynamic_resolver_replay_respects_watch_target_range(DynamicResolverReplayConfig {
        namespace: "ens",
        chain: "ethereum-mainnet",
        deployment_epoch: "ens_v1",
        reverse_source_family: "ens_v1_reverse_l1",
        registry_source_family: "ens_v1_registry_l1",
        resolver_source_family: "ens_v1_resolver_l1",
        in_range_raw_name: "alice.eth",
        closed_raw_name: "closed.eth",
        manifest_id_base: 300,
        uuid_base: 0x3000,
    })
    .await
}

#[tokio::test]
async fn replay_normalized_events_respects_basenames_dynamic_resolver_watch_target_range()
-> Result<()> {
    assert_dynamic_resolver_replay_respects_watch_target_range(DynamicResolverReplayConfig {
        namespace: "basenames",
        chain: "base-mainnet",
        deployment_epoch: "basenames_v1",
        reverse_source_family: "basenames_base_primary",
        registry_source_family: "basenames_base_registry",
        resolver_source_family: "basenames_base_resolver",
        in_range_raw_name: "alice.base.eth",
        closed_raw_name: "closed.base.eth",
        manifest_id_base: 400,
        uuid_base: 0x4000,
    })
    .await
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

struct DynamicResolverReplayConfig {
    namespace: &'static str,
    chain: &'static str,
    deployment_epoch: &'static str,
    reverse_source_family: &'static str,
    registry_source_family: &'static str,
    resolver_source_family: &'static str,
    in_range_raw_name: &'static str,
    closed_raw_name: &'static str,
    manifest_id_base: i64,
    uuid_base: u128,
}

async fn assert_dynamic_resolver_replay_respects_watch_target_range(
    config: DynamicResolverReplayConfig,
) -> Result<()> {
    let database = TestDatabase::new().await?;
    let reverse_manifest_id = config.manifest_id_base + 1;
    let registry_manifest_id = config.manifest_id_base + 2;
    let resolver_manifest_id = config.manifest_id_base + 3;
    let reverse_contract_instance_id = Uuid::from_u128(config.uuid_base + 1);
    let registry_contract_instance_id = Uuid::from_u128(config.uuid_base + 2);
    let seed_resolver_contract_instance_id = Uuid::from_u128(config.uuid_base + 3);
    let supported_resolver_contract_instance_id = Uuid::from_u128(config.uuid_base + 4);
    let pending_resolver_contract_instance_id = Uuid::from_u128(config.uuid_base + 5);
    let unsupported_resolver_contract_instance_id = Uuid::from_u128(config.uuid_base + 6);
    let reverse_address = "0x00000000000000000000000000000000000000ad";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let seed_resolver_address = "0x00000000000000000000000000000000000000c0";
    let supported_resolver_address = "0x00000000000000000000000000000000000000c1";
    let pending_resolver_address = "0x00000000000000000000000000000000000000c2";
    let unsupported_resolver_address = "0x00000000000000000000000000000000000000c3";
    let unadmitted_resolver_address = "0x00000000000000000000000000000000000000dd";
    let claimed_address = "0x0000000000000000000000000000000000001234";
    let resolver_seed_role = if config.resolver_source_family == "ens_v1_resolver_l1" {
        "public_resolver"
    } else {
        "resolver"
    };
    let supported_profile_code_hash = if config.resolver_source_family == "ens_v1_resolver_l1" {
        "0x1111111111111111111111111111111111111111111111111111111111111111"
    } else {
        "0x2222222222222222222222222222222222222222222222222222222222222222"
    };
    let unsupported_profile_code_hash =
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    let reverse_node = reverse_node_for_address(claimed_address);
    let block_50 = provider_block(
        "0x5050505050505050505050505050505050505050505050505050505050505050",
        Some("0x4040404040404040404040404040404040404040404040404040404040404040"),
        50,
    );
    let block_51 = provider_block(
        "0x5151515151515151515151515151515151515151515151515151515151515151",
        Some(&block_50.block_hash),
        51,
    );
    let orphaned_block_51 = provider_block(
        "0x51515151515151515151515151515151515151515151515151515151515151ff",
        Some(&block_50.block_hash),
        51,
    );
    let block_52 = provider_block(
        "0x5252525252525252525252525252525252525252525252525252525252525252",
        Some(&block_51.block_hash),
        52,
    );

    insert_active_replay_manifest_contract(
        database.pool(),
        reverse_manifest_id,
        config.namespace,
        config.reverse_source_family,
        config.chain,
        config.deployment_epoch,
        reverse_contract_instance_id,
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_active_replay_manifest_contract(
        database.pool(),
        registry_manifest_id,
        config.namespace,
        config.registry_source_family,
        config.chain,
        config.deployment_epoch,
        registry_contract_instance_id,
        registry_address,
        "registry",
    )
    .await?;
    insert_active_replay_manifest(
        database.pool(),
        resolver_manifest_id,
        config.namespace,
        config.resolver_source_family,
        config.chain,
        config.deployment_epoch,
    )
    .await?;
    for (contract_instance_id, address, source_manifest_id) in [
        (
            seed_resolver_contract_instance_id,
            seed_resolver_address,
            Some(resolver_manifest_id),
        ),
        (
            supported_resolver_contract_instance_id,
            supported_resolver_address,
            None,
        ),
        (
            pending_resolver_contract_instance_id,
            pending_resolver_address,
            None,
        ),
        (
            unsupported_resolver_contract_instance_id,
            unsupported_resolver_address,
            None,
        ),
    ] {
        insert_contract_instance(database.pool(), contract_instance_id, config.chain, "contract")
            .await?;
        insert_active_contract_instance_address(
            database.pool(),
            contract_instance_id,
            config.chain,
            address,
            source_manifest_id,
        )
        .await?;
    }
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
    for contract_instance_id in [
        supported_resolver_contract_instance_id,
        pending_resolver_contract_instance_id,
        unsupported_resolver_contract_instance_id,
    ] {
        insert_active_discovery_edge_with_range(
            database.pool(),
            config.chain,
            "resolver",
            registry_contract_instance_id,
            contract_instance_id,
            Some(registry_manifest_id),
            Some(50),
            Some(51),
        )
        .await?;
    }

    for (block, canonicality_state) in [
        (&block_50, CanonicalityState::Canonical),
        (&block_51, CanonicalityState::Canonical),
        (&orphaned_block_51, CanonicalityState::Orphaned),
        (&block_52, CanonicalityState::Canonical),
    ] {
        insert_chain_lineage_for_block(database.pool(), config.chain, block, canonicality_state)
            .await?;
    }
    upsert_raw_code_hashes(
        database.pool(),
        &[
            RawCodeHash {
                chain_id: config.chain.to_owned(),
                block_hash: block_50.block_hash.clone(),
                block_number: block_50.block_number,
                contract_address: seed_resolver_address.to_owned(),
                code_hash: supported_profile_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawCodeHash {
                chain_id: config.chain.to_owned(),
                block_hash: block_50.block_hash.clone(),
                block_number: block_50.block_number,
                contract_address: supported_resolver_address.to_owned(),
                code_hash: supported_profile_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawCodeHash {
                chain_id: config.chain.to_owned(),
                block_hash: block_50.block_hash.clone(),
                block_number: block_50.block_number,
                contract_address: unsupported_resolver_address.to_owned(),
                code_hash: unsupported_profile_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        config.chain,
        &block_50,
        reverse_address,
        claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_new_resolver_log_for_node_at_index(
        database.pool(),
        config.chain,
        &block_50,
        registry_address,
        supported_resolver_address,
        &reverse_node,
        1,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_new_resolver_log_for_node_at_index(
        database.pool(),
        config.chain,
        &block_50,
        registry_address,
        pending_resolver_address,
        &reverse_node,
        5,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_new_resolver_log_for_node_at_index(
        database.pool(),
        config.chain,
        &block_50,
        registry_address,
        unsupported_resolver_address,
        &reverse_node,
        8,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_name_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_50,
        supported_resolver_address,
        &reverse_node,
        config.in_range_raw_name,
        2,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_name_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_50,
        unadmitted_resolver_address,
        &reverse_node,
        "unadmitted.example",
        4,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_name_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_50,
        pending_resolver_address,
        &reverse_node,
        "pending.example",
        6,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_name_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_50,
        unsupported_resolver_address,
        &reverse_node,
        "unsupported.example",
        9,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_version_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_50,
        supported_resolver_address,
        &reverse_node,
        7,
        3,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_version_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_50,
        pending_resolver_address,
        &reverse_node,
        8,
        7,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_version_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_51,
        unsupported_resolver_address,
        &reverse_node,
        9,
        2,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_name_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_52,
        supported_resolver_address,
        &reverse_node,
        config.closed_raw_name,
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_version_changed_log_for_node(
        database.pool(),
        config.chain,
        &orphaned_block_51,
        supported_resolver_address,
        &reverse_node,
        99,
        0,
        CanonicalityState::Orphaned,
    )
    .await?;

    let outcome = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: config.chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: 50,
                to_block: 52,
            },
        },
    )
    .await?;

    assert_eq!(outcome.selected_block_count, 3);
    assert_eq!(outcome.canonical_raw_log_count, 12);
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            r#"
            SELECT COALESCE(
                ARRAY_AGG(after_state->>'raw_name' ORDER BY block_number, log_index),
                ARRAY[]::TEXT[]
            )
            FROM normalized_events
            WHERE source_family = $1
              AND event_kind = 'RecordChanged'
            "#
        )
        .bind(config.resolver_source_family)
        .fetch_one(database.pool())
        .await?,
        vec![config.in_range_raw_name.to_owned()]
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<i64>>(
            r#"
            SELECT COALESCE(
                ARRAY_AGG((after_state->>'record_version')::BIGINT ORDER BY block_number, log_index),
                ARRAY[]::BIGINT[]
            )
            FROM normalized_events
            WHERE source_family = $1
              AND event_kind = 'RecordVersionChanged'
            "#
        )
        .bind(config.resolver_source_family)
        .fetch_one(database.pool())
        .await?,
        vec![7]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE source_family = $1 AND event_kind IN ('RecordChanged', 'RecordVersionChanged')"
        )
        .bind(config.resolver_source_family)
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE raw_fact_ref->>'emitting_address' = $1"
        )
        .bind(unadmitted_resolver_address)
        .fetch_one(database.pool())
        .await?,
        0
    );
    for gated_address in [pending_resolver_address, unsupported_resolver_address] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE raw_fact_ref->>'emitting_address' = $1 AND event_kind IN ('RecordChanged', 'RecordVersionChanged')"
            )
            .bind(gated_address)
            .fetch_one(database.pool())
            .await?,
            0,
            "{gated_address} resolver-local facts must stay gated from replay output"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            r#"
            SELECT COALESCE(
                ARRAY_AGG(after_state->>'raw_name' ORDER BY after_state->>'raw_name'),
                ARRAY[]::TEXT[]
            )
            FROM normalized_events
            WHERE event_kind = 'RecordChanged'
              AND after_state->>'raw_name' IN ('pending.example', 'unsupported.example', $1)
            "#
        )
        .bind(config.closed_raw_name)
        .fetch_one(database.pool())
        .await?,
        Vec::<String>::new()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE raw_fact_ref->>'block_hash' = $1"
        )
        .bind(&block_52.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE raw_fact_ref->>'block_hash' = $1"
        )
        .bind(&orphaned_block_51.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        3
    );
    let admissions = if config.resolver_source_family == "ens_v1_resolver_l1" {
        bigname_manifests::load_ens_v1_public_resolver_profile_admissions(database.pool()).await?
    } else {
        bigname_manifests::load_basenames_l2_resolver_profile_admissions(database.pool()).await?
    };
    let profile_statuses = admissions
        .iter()
        .filter(|admission| admission.fact_family == "resolver_record")
        .filter(|admission| {
            [
                supported_resolver_address,
                pending_resolver_address,
                unsupported_resolver_address,
            ]
            .contains(&admission.address.as_str())
        })
        .map(|admission| {
            (
                admission.address.as_str(),
                admission.status.as_str(),
                admission.admission_basis.as_str(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        profile_statuses,
        vec![
            (
                supported_resolver_address,
                "supported",
                "code_hash_match"
            ),
            (
                pending_resolver_address,
                "pending",
                "code_hash_pending"
            ),
            (
                unsupported_resolver_address,
                "unsupported",
                "code_hash_mismatch"
            )
        ]
    );
    assert_no_duplicate_normalized_event_identities(database.pool()).await?;

    database.cleanup().await
}

async fn insert_raw_new_resolver_log_for_node_at_index(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    emitting_address: &str,
    resolver: &str,
    node: &str,
    log_index: i64,
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
            log_index,
            emitting_address: emitting_address.to_ascii_lowercase(),
            topics: vec![registry_new_resolver_topic0(), node.to_owned()],
            data: decode_hex_string(&encode_registry_new_resolver_log_data(resolver)),
            canonicality_state,
        }],
    )
    .await?;

    Ok(())
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

async fn insert_active_replay_manifest_contract(
    pool: &PgPool,
    manifest_id: i64,
    namespace: &str,
    source_family: &str,
    chain: &str,
    deployment_epoch: &str,
    contract_instance_id: Uuid,
    address: &str,
    role: &str,
) -> Result<()> {
    insert_active_replay_manifest(
        pool,
        manifest_id,
        namespace,
        source_family,
        chain,
        deployment_epoch,
    )
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
        role,
        contract_instance_id,
        address,
        "none",
        None,
        None,
    )
    .await
}

async fn insert_active_replay_manifest(
    pool: &PgPool,
    manifest_id: i64,
    namespace: &str,
    source_family: &str,
    chain: &str,
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
    .context("failed to insert manifest_versions for dynamic resolver replay test")?;

    Ok(())
}

async fn count_wrapper_replay_events(pool: &PgPool) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM normalized_events
        WHERE derivation_kind = 'ens_v1_unwrapped_authority'
          AND source_family = 'ens_v1_wrapper_l1'
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to count wrapper replay normalized events")
}

async fn assert_no_duplicate_normalized_event_identities(pool: &PgPool) -> Result<()> {
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM (
                SELECT event_identity
                FROM normalized_events
                GROUP BY event_identity
                HAVING COUNT(*) > 1
            ) duplicates
            "#,
        )
        .fetch_one(pool)
        .await
        .context("failed to count duplicate normalized event identities")?,
        0
    );
    Ok(())
}
