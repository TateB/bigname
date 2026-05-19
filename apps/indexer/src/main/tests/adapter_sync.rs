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
                DEFAULT
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
async fn live_adapter_sync_continues_after_block_derived_events() -> Result<()> {
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x343);
    let reverse_address = "0x00000000000000000000000000000000000000af";
    let claimed_address = "0x1111111111111111111111111111111111111111";
    let stored_block = provider_block(
        "0xdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdf",
        Some("0xefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef"),
        65,
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
                11,
                1,
                'ens',
                'ens_v1_reverse_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'uts46-v1',
                'manifests/ens/ens_v1_reverse_l1/v1.toml',
                DEFAULT
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for live adapter sync test")?;
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
        Some(11),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        11,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        "ethereum-mainnet",
        &stored_block,
        CanonicalityState::Canonical,
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

    let summary = sync_live_adapter_state_from_persisted_raw_payloads(
        database.pool(),
        "ethereum-mainnet",
        std::slice::from_ref(&stored_block.block_hash),
    )
    .await?;

    assert_eq!(summary.total_synced_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn post_replay_live_adapter_backlog_latches_tail_before_live_sync_resumes() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let reverse_contract_instance_id = Uuid::from_u128(0x344);
    let reverse_address = "0x00000000000000000000000000000000000000b1";
    let replay_target_claimed_address = "0x2222222222222222222222222222222222222222";
    let backlog_claimed_address = "0x3333333333333333333333333333333333333333";
    let future_claimed_address = "0x4444444444444444444444444444444444444444";
    let replay_target_block = provider_block(
        "0x1010101010101010101010101010101010101010101010101010101010101010",
        Some("0x0909090909090909090909090909090909090909090909090909090909090909"),
        10,
    );
    let backlog_block = provider_block(
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        Some(&replay_target_block.block_hash),
        11,
    );
    let future_block = provider_block(
        "0x1212121212121212121212121212121212121212121212121212121212121212",
        Some(&backlog_block.block_hash),
        12,
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
                12,
                1,
                'ens',
                'ens_v1_reverse_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'uts46-v1',
                'manifests/ens/ens_v1_reverse_l1/v1.toml',
                DEFAULT
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for post-replay backlog test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        chain,
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        chain,
        reverse_address,
        Some(12),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        12,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_completed_block_number
        )
        VALUES ('mainnet', $1, 'raw_fact_normalized_events', 1, 11, 10, 10)
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await
    .context("failed to seed completed normalized replay cursor")?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &replay_target_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &backlog_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &replay_target_block,
        reverse_address,
        replay_target_claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &backlog_block,
        reverse_address,
        backlog_claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;

    let summary = sync_live_adapter_backlog_after_normalized_replay(
        database.pool(),
        "mainnet",
        &[chain.to_owned()],
    )
    .await?;
    assert_eq!(summary.selected_block_count, 1);
    assert_eq!(summary.normalized_event_synced_count, 1);
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
            "SELECT raw_fact_ref->>'block_hash' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        backlog_block.block_hash
    );

    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &future_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &future_block,
        reverse_address,
        future_claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;

    let second_summary = sync_live_adapter_backlog_after_normalized_replay(
        database.pool(),
        "mainnet",
        &[chain.to_owned()],
    )
    .await?;
    assert_eq!(second_summary.selected_block_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            r#"
            SELECT next_block_number, target_block_number
            FROM normalized_replay_cursors
            WHERE deployment_profile = 'mainnet'
              AND chain_id = $1
              AND cursor_kind = 'post_replay_live_adapter_backlog'
            "#,
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (12, 11)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn sync_adapter_owned_raw_log_state_backfills_wrapper_authority_from_stored_raw_logs()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let wrapper_contract_instance_id = Uuid::from_u128(0x352);
    let registry_contract_instance_id = Uuid::from_u128(0x353);
    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let orphan_block = provider_block(
        "0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
        Some("0xefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef"),
        63,
    );
    let stored_block = provider_block(
        "0xdededededededededededededededededededededededededededededededede",
        Some(&orphan_block.block_hash),
        64,
    );
    let dns_name = dns_encoded_eth_name("wrapped");
    let wrapped_namehash = namehash_for_dns_name(&dns_name);
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
                    'ens',
                    'ens_v1_wrapper_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'uts46-v1',
                    'manifests/ens/ens_v1_wrapper_l1/v1.toml',
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
                    'uts46-v1',
                    'manifests/ens/ens_v1_registry_l1/v1.toml',
                    DEFAULT
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for wrapper runtime bootstrap test")?;
    insert_contract_instance(
        database.pool(),
        wrapper_contract_instance_id,
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
    insert_active_contract_instance_address(
        database.pool(),
        wrapper_contract_instance_id,
        "ethereum-mainnet",
        wrapper_address,
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
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "name_wrapper",
        wrapper_contract_instance_id,
        wrapper_address,
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
    upsert_raw_blocks(
        database.pool(),
        &[
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &orphan_block,
                CanonicalityState::Orphaned,
            ),
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &stored_block,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: orphan_block.block_hash.clone(),
                block_number: orphan_block.block_number,
                transaction_hash: transaction_hash_for_block(&orphan_block),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![
                    keccak256_hex(b"NameWrapped(bytes32,bytes,address,uint32,uint64)"),
                    wrapped_namehash.clone(),
                ],
                data: decode_hex_string(&encode_name_wrapped_log_data(&dns_name)),
                canonicality_state: CanonicalityState::Orphaned,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash: transaction_hash.clone(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![
                    keccak256_hex(b"NameWrapped(bytes32,bytes,address,uint32,uint64)"),
                    wrapped_namehash.clone(),
                ],
                data: decode_hex_string(&encode_name_wrapped_log_data(&dns_name)),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash,
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![registry_new_resolver_topic0(), wrapped_namehash],
                data: decode_hex_string(&encode_registry_new_resolver_log_data(
                    "0x00000000000000000000000000000000000000cc",
                )),
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
            "SELECT COUNT(*) FROM resources WHERE provenance->>'authority_kind' = 'wrapper'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT logical_name_id FROM name_surfaces LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "ens:wrapped.eth".to_owned()
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
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'resolver' FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "0x00000000000000000000000000000000000000cc".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE raw_fact_ref->>'block_hash' = $1"
        )
        .bind(orphan_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        7
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
                    DEFAULT
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
                    DEFAULT
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
                    DEFAULT
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
                    DEFAULT
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
        3,
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
        3,
        "resolver",
        "registry",
        "reachable_from_root",
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
                    basenames_name_registered_topic0(),
                    labelhash_hex("alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: decode_hex_string(&encode_basenames_name_registered_log_data(
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
                    resolver_text_changed_with_value_topic0(),
                    alice_namehash.clone(),
                    keccak256_hex(b"com.twitter"),
                ],
                data: decode_hex_string(&encode_two_dynamic_string_log_data(
                    "com.twitter",
                    "alice",
                )),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash,
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
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_events
            WHERE event_kind = 'ResolverChanged'
              AND namespace = 'basenames'
              AND derivation_kind = 'ens_v1_unwrapped_authority'
            "#
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_events
            WHERE event_kind = 'ResolverChanged'
              AND source_family = 'basenames_base_registry'
              AND derivation_kind = 'ens_v1_registry_resolver_changed'
            "#
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
