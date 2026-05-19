#[tokio::test]
async fn normalized_replay_catchup_rewinds_for_later_older_raw_backfill() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let reverse_contract_instance_id = Uuid::from_u128(0x390);
    let reverse_address = "0x00000000000000000000000000000000000000af";
    let newer_claimed_address = "0x2222222222222222222222222222222222222222";
    let older_claimed_address = "0x3333333333333333333333333333333333333333";
    let newer_block = provider_block(
        "0xa0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0",
        Some("0x9090909090909090909090909090909090909090909090909090909090909090"),
        100,
    );
    let older_block = provider_block(
        "0x5050505050505050505050505050505050505050505050505050505050505050",
        Some("0x4040404040404040404040404040404040404040404040404040404040404040"),
        50,
    );

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        390,
        chain,
        "ens_v1_reverse_l1",
        reverse_contract_instance_id,
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &newer_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &newer_block,
        reverse_address,
        newer_claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1_000,
        1,
    )?;
    assert_eq!(
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(
            database.pool(),
            &config,
            chain,
        )
        .await?,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    let cursor_kind = "raw_fact_normalized_events";
    let last_replayed_at = sqlx::query_scalar::<_, sqlx::types::time::OffsetDateTime>(
        r#"
        SELECT last_replayed_at
        FROM normalized_replay_cursors
        WHERE deployment_profile = 'mainnet'
          AND chain_id = 'ethereum-mainnet'
          AND cursor_kind = $1
        "#,
    )
    .bind(cursor_kind)
    .fetch_one(database.pool())
    .await?;

    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &older_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &older_block,
        reverse_address,
        older_claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;
    sqlx::query(
        "UPDATE raw_logs SET observed_at = $1 + INTERVAL '1 second' WHERE chain_id = $2 AND block_hash = $3",
    )
        .bind(last_replayed_at)
        .bind(chain)
        .bind(&older_block.block_hash)
        .execute(database.pool())
        .await?;

    assert_eq!(
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(
            database.pool(),
            &config,
            chain,
        )
        .await?,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged' AND block_number = 50"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_replay_cursors WHERE cursor_kind LIKE 'raw_fact_normalized_events:%'"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn normalized_replay_catchup_log_bound_preserves_whole_blocks() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let reverse_contract_instance_id = Uuid::from_u128(0x391);
    let reverse_address = "0x00000000000000000000000000000000000000bf";
    let block_10 = provider_block(
        "0x1010101010101010101010101010101010101010101010101010101010101010",
        Some("0x0909090909090909090909090909090909090909090909090909090909090909"),
        10,
    );
    let block_11 = provider_block(
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        Some(&block_10.block_hash),
        11,
    );
    let block_12 = provider_block(
        "0x1212121212121212121212121212121212121212121212121212121212121212",
        Some(&block_11.block_hash),
        12,
    );

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        391,
        chain,
        "ens_v1_reverse_l1",
        reverse_contract_instance_id,
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &block_10,
        reverse_address,
        "0x0000000000000000000000000000000000000010",
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        chain,
        &block_11,
        reverse_address,
        "0x0000000000000000000000000000000000000011",
        CanonicalityState::Canonical,
        0,
    )
    .await?;
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        chain,
        &block_11,
        reverse_address,
        "0x0000000000000000000000000000000000000012",
        CanonicalityState::Canonical,
        1,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &block_12,
        reverse_address,
        "0x0000000000000000000000000000000000000013",
        CanonicalityState::Canonical,
    )
    .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec![chain.to_owned()],
        1_000,
        2,
        1,
    )?;
    assert_eq!(
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(
            database.pool(),
            &config,
            chain,
        )
        .await?,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );

    let cursor_kind = "raw_fact_normalized_events";
    let (last_completed, next_block) = sqlx::query_as::<_, (Option<i64>, i64)>(
        r#"
        SELECT last_completed_block_number, next_block_number
        FROM normalized_replay_cursors
        WHERE deployment_profile = 'mainnet'
          AND chain_id = 'ethereum-mainnet'
          AND cursor_kind = $1
        "#,
    )
    .bind(cursor_kind)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(last_completed, Some(10));
    assert_eq!(next_block, 11);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn normalized_replay_catchup_log_bound_allows_oversized_first_block() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let reverse_contract_instance_id = Uuid::from_u128(0x392);
    let reverse_address = "0x00000000000000000000000000000000000000cf";
    let block_20 = provider_block(
        "0x2020202020202020202020202020202020202020202020202020202020202020",
        Some("0x1919191919191919191919191919191919191919191919191919191919191919"),
        20,
    );
    let block_21 = provider_block(
        "0x2121212121212121212121212121212121212121212121212121212121212121",
        Some(&block_20.block_hash),
        21,
    );

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        392,
        chain,
        "ens_v1_reverse_l1",
        reverse_contract_instance_id,
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        chain,
        &block_20,
        reverse_address,
        "0x0000000000000000000000000000000000000020",
        CanonicalityState::Canonical,
        0,
    )
    .await?;
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        chain,
        &block_20,
        reverse_address,
        "0x0000000000000000000000000000000000000021",
        CanonicalityState::Canonical,
        1,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &block_21,
        reverse_address,
        "0x0000000000000000000000000000000000000022",
        CanonicalityState::Canonical,
    )
    .await?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1,
        1,
    )?;
    assert_eq!(
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(
            database.pool(),
            &config,
            chain,
        )
        .await?,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );

    let cursor_kind = "raw_fact_normalized_events";
    let (last_completed, next_block) = sqlx::query_as::<_, (Option<i64>, i64)>(
        r#"
        SELECT last_completed_block_number, next_block_number
        FROM normalized_replay_cursors
        WHERE deployment_profile = 'mainnet'
          AND chain_id = 'ethereum-mainnet'
          AND cursor_kind = $1
        "#,
    )
    .bind(cursor_kind)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(last_completed, Some(20));
    assert_eq!(next_block, 21);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn normalized_replay_catchup_does_not_use_log_bound_as_stateful_boundary() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let wrapper_address = "0x0000000000000000000000000000000000000130";
    let reverse_address = "0x0000000000000000000000000000000000000131";
    let wrapper_block = provider_block(
        "0x3030303030303030303030303030303030303030303030303030303030303030",
        Some("0x2929292929292929292929292929292929292929292929292929292929292929"),
        30,
    );
    let reverse_block = provider_block(
        "0x3131313131313131313131313131313131313131313131313131313131313131",
        Some(&wrapper_block.block_hash),
        31,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        393,
        chain,
        Uuid::from_u128(0x393),
        wrapper_address,
    )
    .await?;
    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        394,
        chain,
        "ens_v1_reverse_l1",
        Uuid::from_u128(0x394),
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &wrapper_block,
        wrapper_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &reverse_block,
        reverse_address,
        "0x0000000000000000000000000000000000000031",
        CanonicalityState::Canonical,
    )
    .await?;
    sqlx::query("DROP INDEX IF EXISTS normalized_events_namespace_idx")
        .execute(database.pool())
        .await
        .context("failed to drop deferred normalized event index for stateful catch-up test")?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec![chain.to_owned()],
        1_000,
        1,
        1,
    )?;
    let status = normalized_replay_catchup::run_normalized_replay_catchup_iteration(
        database.pool(),
        &config,
        chain,
    )
    .await?;
    assert_eq!(
        status,
        normalized_replay_catchup::CatchupIterationStatus::Progressed
    );

    let cursor_kind = "raw_fact_normalized_events";
    let (last_completed, next_block) = sqlx::query_as::<_, (Option<i64>, i64)>(
        r#"
        SELECT last_completed_block_number, next_block_number
        FROM normalized_replay_cursors
        WHERE deployment_profile = 'mainnet'
          AND chain_id = 'ethereum-mainnet'
          AND cursor_kind = $1
        "#,
    )
    .bind(cursor_kind)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(last_completed, Some(31));
    assert_eq!(next_block, 32);
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events"
        )
        .fetch_one(database.pool())
        .await?
            > 0
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT to_regclass('normalized_events_namespace_idx') IS NOT NULL"
        )
        .fetch_one(database.pool())
        .await?,
        "closure replay must restore deferred projection indexes before running"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn normalized_replay_catchup_preserves_latched_closure_target() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";

    assert_eq!(
        normalized_replay_catchup::ensure_cursor_for_test(
            database.pool(),
            "mainnet",
            chain,
            10,
            20,
            false,
        )
        .await?,
        (10, 10, 20)
    );
    assert_eq!(
        normalized_replay_catchup::ensure_cursor_for_test(
            database.pool(),
            "mainnet",
            chain,
            10,
            25,
            false,
        )
        .await?,
        (10, 10, 20)
    );
    assert_eq!(
        normalized_replay_catchup::ensure_cursor_for_test(
            database.pool(),
            "mainnet",
            chain,
            10,
            25,
            true,
        )
        .await?,
        (10, 10, 25)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn normalized_replay_catchup_rebuilds_deferred_indexes_when_configured_chain_has_no_logs()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    sqlx::query("DROP INDEX IF EXISTS normalized_events_namespace_idx")
        .execute(database.pool())
        .await
        .context("failed to drop deferred normalized event index for test")?;
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
        VALUES ('mainnet', 'ethereum-mainnet', 'raw_fact_normalized_events', 1, 2, 1, 1)
        "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert complete normalized replay cursor for test")?;

    let config = normalized_replay_catchup::NormalizedReplayCatchupConfig::new(
        "mainnet".to_owned(),
        vec!["ethereum-mainnet".to_owned(), "base-mainnet".to_owned()],
        1_000,
        1_000,
        1,
    )?;
    assert_eq!(
        normalized_replay_catchup::run_normalized_replay_catchup_iteration(
            database.pool(),
            &config,
            "base-mainnet",
        )
        .await?,
        normalized_replay_catchup::CatchupIterationStatus::Idle
    );

    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT to_regclass('normalized_events_namespace_idx') IS NOT NULL"
        )
        .fetch_one(database.pool())
        .await?
    );

    database.cleanup().await?;
    Ok(())
}

async fn create_normalized_replay_cursor_table(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE normalized_replay_cursors (
            deployment_profile TEXT NOT NULL,
            chain_id TEXT NOT NULL,
            cursor_kind TEXT NOT NULL,
            range_start_block_number BIGINT NOT NULL CHECK (range_start_block_number >= 0),
            next_block_number BIGINT NOT NULL CHECK (next_block_number >= range_start_block_number),
            target_block_number BIGINT NOT NULL CHECK (target_block_number >= range_start_block_number),
            last_completed_block_number BIGINT CHECK (last_completed_block_number IS NULL OR last_completed_block_number >= range_start_block_number),
            last_selected_block_count BIGINT NOT NULL DEFAULT 0 CHECK (last_selected_block_count >= 0),
            last_canonical_raw_log_count BIGINT NOT NULL DEFAULT 0 CHECK (last_canonical_raw_log_count >= 0),
            last_scanned_raw_log_count BIGINT NOT NULL DEFAULT 0 CHECK (last_scanned_raw_log_count >= 0),
            last_matched_raw_log_count BIGINT NOT NULL DEFAULT 0 CHECK (last_matched_raw_log_count >= 0),
            last_normalized_event_synced_count BIGINT NOT NULL DEFAULT 0 CHECK (last_normalized_event_synced_count >= 0),
            last_normalized_event_inserted_count BIGINT NOT NULL DEFAULT 0 CHECK (last_normalized_event_inserted_count >= 0),
            last_replayed_at TIMESTAMPTZ,
            last_failure_reason TEXT,
            last_failure_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (deployment_profile, chain_id, cursor_kind),
            CHECK (next_block_number <= target_block_number + 1)
        )
        "#,
    )
    .execute(pool)
    .await
    .context("failed to create normalized_replay_cursors table for indexer tests")?;
    sqlx::raw_sql(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/20260509120000_normalized_replay_adapter_checkpoints.sql"
    )))
    .execute(pool)
    .await
    .context("failed to create normalized replay adapter checkpoint tables for indexer tests")?;

    Ok(())
}
