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

    let cursor_kind = "raw_fact_normalized_events:source_family=ens_v1_reverse_l1";
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

    Ok(())
}
