use std::sync::Mutex;

#[test]
fn ensure_manifest_root_ready_accepts_loaded_root() -> Result<()> {
    ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::Loaded))
}

#[test]
fn ensure_manifest_root_ready_accepts_empty_root() -> Result<()> {
    ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::Empty))
}

#[test]
fn ensure_manifest_root_ready_rejects_missing_root() {
    let error = ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::MissingRoot))
        .expect_err("missing root must fail");

    assert!(
        error
            .to_string()
            .contains("refusing to boot on stale stored manifest state")
    );
    assert!(
        error.to_string().contains("/tmp/manifests does not exist"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn ensure_manifest_root_ready_rejects_invalid_root() {
    let error = ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::InvalidRoot))
        .expect_err("invalid root must fail");

    assert!(
        error
            .to_string()
            .contains("refusing to boot on stale stored manifest state")
    );
    assert!(
        error
            .to_string()
            .contains("/tmp/manifests is not a directory"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn verify_stored_manifest_state_accepts_matching_active_manifest_count() -> Result<()> {
    let database = TestDatabase::new().await?;
    let admission_state = load_discovery_admission_state(database.pool()).await?;

    verify_stored_manifest_state(&synced_manifest_summary(0), &admission_state)?;

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn verify_stored_manifest_state_rejects_mismatched_active_manifest_count() -> Result<()> {
    let database = TestDatabase::new().await?;
    let admission_state = load_discovery_admission_state(database.pool()).await?;

    let error = verify_stored_manifest_state(&synced_manifest_summary(1), &admission_state)
        .expect_err("mismatched counts must fail");

    assert!(
        error.to_string().contains(
            "stored active manifest count 0 does not match the synced active manifest count 1"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn load_watched_contract_summary_rebuilds_counts_from_storage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(1);
    let contract_contract_instance_id = Uuid::from_u128(2);
    let discovered_contract_instance_id = Uuid::from_u128(3);
    let implementation_contract_instance_id = Uuid::from_u128(4);
    let shadow_contract_instance_id = Uuid::from_u128(5);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES
                (1, 'ethereum-mainnet', 'active'),
                (2, 'base-mainnet', 'shadow')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for watched summary test")?;
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

    insert_contract_instance(
        database.pool(),
        contract_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        contract_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
        Some(1),
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        discovered_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        discovered_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000cc",
        Some(1),
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000dd",
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registry",
        contract_contract_instance_id,
        "0x00000000000000000000000000000000000000aa",
        "erc1967",
        Some(implementation_contract_instance_id),
        Some("0x00000000000000000000000000000000000000dd"),
    )
    .await?;
    insert_active_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "subregistry",
        contract_contract_instance_id,
        discovered_contract_instance_id,
        Some(1),
    )
    .await?;
    insert_active_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "proxy_implementation",
        contract_contract_instance_id,
        implementation_contract_instance_id,
        Some(1),
    )
    .await?;

    insert_contract_instance(
        database.pool(),
        shadow_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        shadow_contract_instance_id,
        "base-mainnet",
        "0x00000000000000000000000000000000000000bb",
        Some(2),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        2,
        "registry",
        shadow_contract_instance_id,
        "0x00000000000000000000000000000000000000bb",
        "none",
        None,
        None,
    )
    .await?;

    let summary = load_watched_contract_summary(database.pool()).await?;
    assert_eq!(summary.unique_contract_count, 4);
    assert_eq!(summary.source_entry_count, 4);
    assert_eq!(summary.manifest_root_count, 1);
    assert_eq!(summary.manifest_contract_count, 1);
    assert_eq!(summary.discovery_edge_count, 2);
    assert_eq!(summary.chains.len(), 1);
    assert_eq!(summary.chains[0].chain, "ethereum-mainnet");
    assert_eq!(summary.chains[0].unique_contract_count, 4);
    assert_eq!(summary.chains[0].manifest_root_count, 1);
    assert_eq!(summary.chains[0].manifest_contract_count, 1);
    assert_eq!(summary.chains[0].discovery_edge_count, 2);

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(watched_plan.len(), 1);
    assert_eq!(
        watched_plan[0].addresses,
        vec![
            "0x0000000000000000000000000000000000000001".to_owned(),
            "0x00000000000000000000000000000000000000aa".to_owned(),
            "0x00000000000000000000000000000000000000cc".to_owned(),
            "0x00000000000000000000000000000000000000dd".to_owned(),
        ]
    );
    assert_eq!(watched_plan[0].manifest_root_entry_count, 1);
    assert_eq!(watched_plan[0].manifest_contract_entry_count, 1);
    assert_eq!(watched_plan[0].discovery_edge_entry_count, 2);

    database.cleanup().await?;
    Ok(())
}

#[test]
fn watched_chain_plan_state_counts_chains_addresses_and_entries() {
    let state = watched_chain_plan_state(&[
        WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
        },
        WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec!["0x00000000000000000000000000000000000000bb".to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        },
    ]);

    assert_eq!(
        state,
        WatchedChainPlanState {
            chain_count: 2,
            address_count: 3,
            entry_count: 4,
        }
    );
}

#[test]
fn intake_runtime_state_counts_checkpoint_modes() {
    let state = intake_runtime_state(&[
        IntakeChainTask {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
            checkpoint: ChainCheckpoint {
                chain_id: "ethereum-mainnet".to_owned(),
                canonical_block_hash: Some(
                    "0x00000000000000000000000000000000000000000000000000000000000000aa".to_owned(),
                ),
                canonical_block_number: Some(42),
                safe_block_hash: Some(
                    "0x0000000000000000000000000000000000000000000000000000000000000099".to_owned(),
                ),
                safe_block_number: Some(41),
                finalized_block_hash: None,
                finalized_block_number: None,
            },
        },
        IntakeChainTask {
            chain: "base-mainnet".to_owned(),
            addresses: vec!["0x00000000000000000000000000000000000000bb".to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
            checkpoint: ChainCheckpoint {
                chain_id: "base-mainnet".to_owned(),
                canonical_block_hash: None,
                canonical_block_number: None,
                safe_block_hash: None,
                safe_block_number: None,
                finalized_block_hash: None,
                finalized_block_number: None,
            },
        },
    ]);

    assert_eq!(
        state,
        IntakeRuntimeState {
            chain_count: 2,
            address_count: 3,
            entry_count: 4,
            cold_start_chain_count: 1,
            resumable_chain_count: 1,
            safe_checkpoint_chain_count: 1,
            finalized_checkpoint_chain_count: 0,
        }
    );
}

#[test]
fn provider_registry_validation_accepts_missing_base_and_rejects_out_of_profile_entries()
-> Result<()> {
    let tasks = vec![
        IntakeChainTask {
            chain: "base-mainnet".to_owned(),
            addresses: vec!["0x00000000000000000000000000000000000000bb".to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
            checkpoint: ChainCheckpoint {
                chain_id: "base-mainnet".to_owned(),
                canonical_block_hash: None,
                canonical_block_number: None,
                safe_block_hash: None,
                safe_block_number: None,
                finalized_block_hash: None,
                finalized_block_number: None,
            },
        },
        IntakeChainTask {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec!["0x0000000000000000000000000000000000000001".to_owned()],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
            checkpoint: ChainCheckpoint {
                chain_id: "ethereum-mainnet".to_owned(),
                canonical_block_hash: None,
                canonical_block_number: None,
                safe_block_hash: None,
                safe_block_number: None,
                finalized_block_hash: None,
                finalized_block_number: None,
            },
        },
    ];
    let ethereum_only =
        ProviderRegistry::from_chain_rpc_urls(&["ethereum-mainnet=http://127.0.0.1:8545".into()])?;
    validate_provider_registry_for_intake_tasks(&tasks, &ethereum_only)?;

    let out_of_profile = ProviderRegistry::from_chain_rpc_urls(&[
        "ethereum-mainnet=http://127.0.0.1:8545".into(),
        "optimism-mainnet=http://127.0.0.1:7545".into(),
    ])?;
    let error = validate_provider_registry_for_intake_tasks(&tasks, &out_of_profile)
        .expect_err("configured provider outside selected profile must fail");
    assert!(
        error.to_string().contains(
            "configured provider source chains outside selected/admitted runtime chain set: optimism-mainnet"
        ),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[tokio::test]
async fn sync_intake_chain_tasks_creates_missing_checkpoint_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(11);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for intake task sync test")?;
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

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].chain, "ethereum-mainnet");
    assert_eq!(
        tasks[0].addresses,
        vec!["0x0000000000000000000000000000000000000001".to_owned()]
    );
    assert_eq!(checkpoint_mode(&tasks[0].checkpoint), "cold_start");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_checkpoints")
            .fetch_one(database.pool())
            .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn sync_intake_chain_tasks_preserves_manifest_contract_implementation_addresses() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let contract_address = "0x00000000000000000000000000000000000000aa";
    let implementation_address = "0x00000000000000000000000000000000000000bb";
    let contract_contract_instance_id = Uuid::from_u128(21);
    let implementation_contract_instance_id = Uuid::from_u128(22);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for implementation watch-plan test")?;
    insert_contract_instance(
        database.pool(),
        contract_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        contract_contract_instance_id,
        "ethereum-mainnet",
        contract_address,
        Some(1),
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        implementation_address,
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registry",
        contract_contract_instance_id,
        contract_address,
        "erc1967",
        Some(implementation_contract_instance_id),
        Some(implementation_address),
    )
    .await?;
    insert_active_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "proxy_implementation",
        contract_contract_instance_id,
        implementation_contract_instance_id,
        Some(1),
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;

    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].chain, "ethereum-mainnet");
    assert_eq!(
        tasks[0].addresses,
        vec![
            contract_address.to_owned(),
            implementation_address.to_owned()
        ]
    );
    assert_eq!(tasks[0].manifest_root_entry_count, 0);
    assert_eq!(tasks[0].manifest_contract_entry_count, 1);
    assert_eq!(tasks[0].discovery_edge_entry_count, 1);
    assert_eq!(checkpoint_mode(&tasks[0].checkpoint), "cold_start");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_checkpoints")
            .fetch_one(database.pool())
            .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn bootstrap_auto_backfill_drains_manifest_started_targets_and_preserves_checkpoints()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_bootstrap_backfill_job_tables(database.pool()).await?;
    let manifest_root = PathBuf::from("manifests/sepolia");
    let eligible_contract_instance_id = Uuid::from_u128(9_001);
    let unknown_start_contract_instance_id = Uuid::from_u128(9_002);
    let grouped_contract_instance_id = Uuid::from_u128(9_003);
    let future_contract_instance_id = Uuid::from_u128(9_004);
    let eligible_address = "0x0000000000000000000000000000000000000901";
    let unknown_start_address = "0x0000000000000000000000000000000000000902";
    let grouped_address = "0x0000000000000000000000000000000000000903";
    let future_address = "0x0000000000000000000000000000000000000904";

    insert_bootstrap_manifest_version(
        database.pool(),
        901,
        "ens",
        "ethereum-mainnet",
        "ens_bootstrap_registry",
        json!({
            "contracts": [
                {
                    "role": "registry",
                    "address": eligible_address,
                    "start_block": 42
                },
                {
                    "role": "registrar",
                    "address": grouped_address,
                    "start_block": 42
                },
                {
                    "role": "future",
                    "address": future_address,
                    "start_block": 44
                }
            ],
            "roots": []
        }),
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        eligible_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        eligible_contract_instance_id,
        "ethereum-mainnet",
        eligible_address,
        Some(901),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        901,
        "registry",
        eligible_contract_instance_id,
        eligible_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        grouped_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        grouped_contract_instance_id,
        "ethereum-mainnet",
        grouped_address,
        Some(901),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        901,
        "registrar",
        grouped_contract_instance_id,
        grouped_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        future_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        future_contract_instance_id,
        "ethereum-mainnet",
        future_address,
        Some(901),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        901,
        "future",
        future_contract_instance_id,
        future_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_bootstrap_manifest_version(
        database.pool(),
        902,
        "basenames",
        "base-mainnet",
        "basenames_base_resolver",
        json!({
            "contracts": [
                {
                    "role": "resolver",
                    "address": unknown_start_address
                }
            ],
            "roots": []
        }),
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        unknown_start_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        unknown_start_contract_instance_id,
        "base-mainnet",
        unknown_start_address,
        None,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        902,
        "resolver",
        unknown_start_contract_instance_id,
        unknown_start_address,
        "none",
        None,
        None,
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
    .context("failed to insert checkpoint guard row for bootstrap backfill test")?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let intake_tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    assert_eq!(intake_tasks.len(), 2);

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
    let requests = Arc::new(Mutex::new(Vec::<BootstrapRpcRequest>::new()));
    let (provider, server) = bootstrap_auto_backfill_provider(
        vec![
            ProviderBlockFixture {
                block: block_42.clone(),
                logs: vec![
                    bootstrap_rpc_log_payload_at_address(&block_42, eligible_address, 0),
                    bootstrap_rpc_log_payload_at_address(&block_42, grouped_address, 1),
                    bootstrap_rpc_log_payload_at_address(&block_42, future_address, 2),
                ],
            },
            ProviderBlockFixture {
                block: block_43.clone(),
                logs: vec![
                    bootstrap_rpc_log_payload_at_address(&block_43, eligible_address, 0),
                    bootstrap_rpc_log_payload_at_address(&block_43, grouped_address, 1),
                    bootstrap_rpc_log_payload_at_address(&block_43, future_address, 2),
                ],
            },
        ],
        Arc::clone(&requests),
    )
    .await?;
    let provider_registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={provider}")])?;

    let outcome = run_startup_bootstrap_backfills(
        database.pool(),
        &manifest_root,
        &intake_tasks,
        &provider_registry,
        crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
        crate::backfill::BackfillAdapterSyncMode::Inline,
        false,
        HeaderAuditMode::Minimal,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_WORKERS,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_RANGE_BLOCKS,
    )
    .await?;
    assert_eq!(outcome.active_chain_count, 2);
    assert_eq!(outcome.provider_configured_chain_count, 1);
    assert_eq!(outcome.missing_provider_chain_count, 1);
    assert_eq!(outcome.eligible_target_count, 3);
    assert_eq!(outcome.skipped_unknown_start_target_count, 1);
    assert_eq!(outcome.skipped_unknown_start_targets.len(), 1);
    assert_eq!(
        outcome.skipped_unknown_start_targets[0].source_family,
        "basenames_base_resolver"
    );
    assert_eq!(
        outcome.skipped_unknown_start_targets[0].contract_instance_id,
        unknown_start_contract_instance_id
    );
    assert_eq!(
        outcome.skipped_unknown_start_targets[0].address,
        unknown_start_address
    );
    assert_eq!(
        outcome.skipped_unknown_start_targets[0].skip_reason,
        "unknown_start"
    );
    assert_eq!(outcome.drained_job_count, 1);
    assert_eq!(outcome.skipped_future_target_count, 1);
    assert_eq!(outcome.reserved_range_count, 1);
    assert_eq!(outcome.completed_range_count, 1);
    assert_eq!(outcome.resolved_block_count, 2);
    assert_eq!(outcome.raw_log_count, 4);
    assert_eq!(outcome.raw_code_hash_count, 4);

    let jobs = sqlx::query_as::<_, (i64, String, String, i64, i64, String, Value)>(
        r#"
        SELECT
            backfill_job_id,
            deployment_profile,
            chain_id,
            range_start_block_number,
            range_end_block_number,
            idempotency_key,
            source_identity
        FROM backfill_jobs
        ORDER BY backfill_job_id
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(jobs.len(), 1);
    let (
        backfill_job_id,
        deployment_profile,
        chain_id,
        from_block,
        to_block,
        idempotency_key,
        source_identity,
    ) = &jobs[0];
    assert_eq!(deployment_profile, "sepolia");
    assert_eq!(chain_id, "ethereum-mainnet");
    assert_eq!((*from_block, *to_block), (42, 43));
    assert!(idempotency_key.starts_with("indexer-bootstrap-backfill:v3:"));
    assert!(idempotency_key.contains("deployment_profile=sepolia"));
    assert!(!idempotency_key.contains("manifest_root="));
    assert!(idempotency_key.contains("chain=ethereum-mainnet"));
    assert!(idempotency_key.contains("from=42:to=43"));
    assert_eq!(
        source_identity.get("selector_kind").and_then(Value::as_str),
        Some("watched_target_set")
    );
    assert_eq!(
        source_identity
            .get("requested_watched_targets")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(2)
    );
    let selected_target_ids = source_identity
        .get("selected_targets")
        .and_then(Value::as_array)
        .expect("selected targets must be persisted")
        .iter()
        .map(|target| {
            target
                .get("contract_instance_id")
                .and_then(Value::as_str)
                .expect("selected target must include contract_instance_id")
                .to_owned()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        selected_target_ids,
        [
            eligible_contract_instance_id.to_string(),
            grouped_contract_instance_id.to_string()
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
    );
    let source_identity_hash = source_identity
        .get("source_identity_hash")
        .and_then(Value::as_str)
        .expect("source identity hash must be persisted");
    assert!(idempotency_key.contains(source_identity_hash));

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs WHERE emitting_address = $1")
            .bind(eligible_address)
            .fetch_one(database.pool())
            .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs WHERE emitting_address = $1")
            .bind(grouped_address)
            .fetch_one(database.pool())
            .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs WHERE emitting_address = $1")
            .bind(future_address)
            .fetch_one(database.pool())
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs WHERE emitting_address = $1")
            .bind(unknown_start_address)
            .fetch_one(database.pool())
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM raw_code_hashes WHERE contract_address = $1"
        )
        .bind(unknown_start_address)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM raw_code_hashes WHERE contract_address = $1"
        )
        .bind(future_address)
        .fetch_one(database.pool())
        .await?,
        0
    );
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
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage")
            .fetch_one(database.pool())
            .await?,
        2
    );
    let initial_requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();

    let rerun = run_startup_bootstrap_backfills(
        database.pool(),
        &manifest_root,
        &intake_tasks,
        &provider_registry,
        crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
        crate::backfill::BackfillAdapterSyncMode::Inline,
        false,
        HeaderAuditMode::Minimal,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_WORKERS,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_RANGE_BLOCKS,
    )
    .await?;
    assert_eq!(rerun.drained_job_count, 0);
    assert_eq!(rerun.reserved_range_count, 0);
    assert_eq!(rerun.completed_range_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM backfill_jobs")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT backfill_job_id FROM backfill_jobs WHERE idempotency_key = $1"
        )
        .bind(idempotency_key)
        .fetch_one(database.pool())
        .await?,
        *backfill_job_id
    );

    let requests = initial_requests;
    assert!(
        requests
            .iter()
            .any(|request| request.method == "eth_getBlockByNumber"
                && request.params.first().and_then(Value::as_str) == Some("latest")),
        "bootstrap backfill must fetch a finite provider head before scheduling"
    );
    assert!(
        requests
            .iter()
            .filter(|request| request.method == "eth_getCode")
            .all(|request| request.params.first().and_then(Value::as_str) == Some(eligible_address)
                || request.params.first().and_then(Value::as_str) == Some(grouped_address)),
        "unknown-start and future targets must not be code-fetched by automatic bootstrap"
    );
    let code_requests = requests
        .iter()
        .filter(|request| request.method == "eth_getCode")
        .collect::<Vec<_>>();
    assert_eq!(code_requests.len(), 4);
    assert_eq!(code_requests[0].batch_size, 4);
    assert!(
        code_requests
            .iter()
            .all(|request| request.http_request_id == code_requests[0].http_request_id
                && request.batch_size == 4),
        "grouped bootstrap code lookups should use one JSON-RPC batch HTTP request"
    );
    let block_number_requests = requests
        .iter()
        .enumerate()
        .filter(|(_, request)| request.method == "eth_getBlockByNumber"
            && request
                .params
                .first()
                .and_then(Value::as_str)
                .is_some_and(|value| value.starts_with("0x")))
        .collect::<Vec<_>>();
    assert_eq!(
        block_number_requests.len(),
        6,
        "grouped bootstrap should resolve, revalidate, and code-pin the two selected target blocks"
    );
    let resolved_block_params = block_number_requests[..2]
        .iter()
        .map(|(_, request)| request.params.first().and_then(Value::as_str))
        .collect::<Vec<_>>();
    let revalidated_block_params = block_number_requests[2..4]
        .iter()
        .map(|(_, request)| request.params.first().and_then(Value::as_str))
        .collect::<Vec<_>>();
    let code_observation_block_params = block_number_requests[4..]
        .iter()
        .map(|(_, request)| request.params.first().and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert_eq!(
        resolved_block_params, revalidated_block_params,
        "grouped bootstrap should revalidate the same block hashes after fetching logs"
    );
    assert_eq!(
        resolved_block_params, code_observation_block_params,
        "grouped bootstrap should pin code observations to the selected block hashes"
    );
    let log_requests = requests
        .iter()
        .enumerate()
        .filter(|(_, request)| request.method == "eth_getLogs")
        .collect::<Vec<_>>();
    assert_eq!(
        log_requests.len(),
        1,
        "grouped bootstrap should fetch one safe log range for a stable target set"
    );
    assert!(
        block_number_requests[1].0 < log_requests[0].0
            && log_requests[0].0 < block_number_requests[2].0,
        "grouped bootstrap should fetch logs between range resolution and revalidation"
    );
    for batch in [
        &block_number_requests[..2],
        &block_number_requests[2..4],
        &block_number_requests[4..],
    ] {
        assert_eq!(batch[0].1.batch_size, 2);
        assert!(
            batch.iter().all(|(_, request)| {
                request.http_request_id == batch[0].1.http_request_id && request.batch_size == 2
            }),
            "grouped bootstrap block lookups should use two-call JSON-RPC batch HTTP requests"
        );
    }
    assert_eq!(log_requests[0].1.batch_size, 1);
    let filter = log_requests[0]
        .1
        .params
        .first()
        .and_then(Value::as_object)
        .expect("log request must include a filter object");
    assert_eq!(filter.get("fromBlock").and_then(Value::as_str), Some("0x2a"));
    assert_eq!(filter.get("toBlock").and_then(Value::as_str), Some("0x2b"));
    assert!(
        !filter.contains_key("blockHash"),
        "bootstrap backfill logs must use the selected target range instead of per-block blockHash filters"
    );
    assert_eq!(
        filter.get("address").and_then(Value::as_array),
        Some(&vec![
            Value::String(eligible_address.to_owned()),
            Value::String(grouped_address.to_owned()),
        ]),
        "bootstrap log range must include grouped eligible addresses only"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn bootstrap_auto_backfill_scans_ensv1_resolver_events_by_source_family() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_bootstrap_backfill_job_tables(database.pool()).await?;
    let manifest_root = PathBuf::from("manifests/mainnet");
    let resolver_a_contract_instance_id = Uuid::from_u128(10_001);
    let resolver_b_contract_instance_id = Uuid::from_u128(10_002);
    let registry_contract_instance_id = Uuid::from_u128(10_003);
    let resolver_a_address = "0x0000000000000000000000000000000000000a01";
    let resolver_b_address = "0x0000000000000000000000000000000000000a02";
    let unlisted_resolver_address = "0x0000000000000000000000000000000000000a03";
    let registry_address = "0x0000000000000000000000000000000000000b01";

    insert_bootstrap_manifest_version(
        database.pool(),
        10_001,
        "ens",
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        json!({
            "contracts": [
                {
                    "role": "public_resolver_a",
                    "address": resolver_a_address,
                    "start_block": 10
                },
                {
                    "role": "public_resolver_b",
                    "address": resolver_b_address,
                    "start_block": 12
                }
            ],
            "roots": []
        }),
    )
    .await?;
    insert_bootstrap_manifest_version(
        database.pool(),
        10_003,
        "ens",
        "ethereum-mainnet",
        "ens_v1_registry_l1",
        json!({
            "contracts": [
                {
                    "role": "registry",
                    "address": registry_address,
                    "start_block": 10
                }
            ],
            "roots": []
        }),
    )
    .await?;
    for (contract_instance_id, address, role) in [
        (
            resolver_a_contract_instance_id,
            resolver_a_address,
            "public_resolver_a",
        ),
        (
            resolver_b_contract_instance_id,
            resolver_b_address,
            "public_resolver_b",
        ),
        (registry_contract_instance_id, registry_address, "registry"),
    ] {
        insert_contract_instance(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            if role == "registry" {
                "contract"
            } else {
                "resolver"
            },
        )
        .await?;
        insert_active_contract_instance_address(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            address,
            Some(if role == "registry" { 10_003 } else { 10_001 }),
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            if role == "registry" { 10_003 } else { 10_001 },
            role,
            contract_instance_id,
            address,
            "none",
            None,
            None,
        )
        .await?;
    }

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let intake_tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    assert_eq!(intake_tasks.len(), 1);

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
    let block_13 = provider_block(
        "0x1313131313131313131313131313131313131313131313131313131313131313",
        Some(&block_12.block_hash),
        13,
    );
    let resolver_node = namehash_for_dns_name(&dns_encoded_eth_name("alice"));
    let requests = Arc::new(Mutex::new(Vec::<BootstrapRpcRequest>::new()));
    let (provider, server) = bootstrap_auto_backfill_provider(
        vec![
            ProviderBlockFixture {
                block: block_10.clone(),
                logs: vec![rpc_resolver_name_changed_log_payload_for_namehash(
                    &block_10,
                    unlisted_resolver_address,
                    &resolver_node,
                    "unlisted.example",
                    0,
                )],
            },
            ProviderBlockFixture {
                block: block_11.clone(),
                logs: vec![rpc_resolver_name_changed_log_payload_for_namehash(
                    &block_11,
                    resolver_a_address,
                    &resolver_node,
                    "resolver-a.example",
                    0,
                )],
            },
            ProviderBlockFixture {
                block: block_12.clone(),
                logs: vec![rpc_resolver_name_changed_log_payload_for_namehash(
                    &block_12,
                    resolver_b_address,
                    &resolver_node,
                    "resolver-b.example",
                    0,
                )],
            },
            ProviderBlockFixture {
                block: block_13.clone(),
                logs: vec![
                    bootstrap_rpc_log_payload_at_address(&block_13, resolver_a_address, 0),
                    bootstrap_rpc_log_payload_at_address(&block_13, registry_address, 1),
                ],
            },
        ],
        Arc::clone(&requests),
    )
    .await?;
    let provider_registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={provider}")])?;

    let outcome = run_startup_bootstrap_backfills(
        database.pool(),
        &manifest_root,
        &intake_tasks,
        &provider_registry,
        crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
        crate::backfill::BackfillAdapterSyncMode::RawOnly,
        false,
        HeaderAuditMode::Minimal,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_WORKERS,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_RANGE_BLOCKS,
    )
    .await?;
    assert_eq!(outcome.eligible_target_count, 3);
    assert_eq!(outcome.drained_job_count, 1);
    assert_eq!(outcome.resolved_block_count, 4);
    assert_eq!(outcome.raw_log_count, 4);

    let source_identity =
        sqlx::query_scalar::<_, Value>("SELECT source_identity FROM backfill_jobs")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(
        source_identity.get("selector_kind").and_then(Value::as_str),
        Some("watched_target_set")
    );
    assert_eq!(
        source_identity
            .get("source_identity_payload_format")
            .and_then(Value::as_str),
        Some("selected_targets_with_generic_topic_scans_v1")
    );
    assert_eq!(
        source_identity
            .get("generic_topic_scans")
            .and_then(Value::as_array)
            .and_then(|scans| scans.first())
            .and_then(|scan| scan.get("source_family"))
            .and_then(Value::as_str),
        Some("ens_v1_resolver_l1")
    );
    assert_eq!(
        source_identity
            .get("requested_watched_targets")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(3)
    );
    assert_eq!(
        source_identity
            .get("selected_targets")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1)
    );
    let source_identity_text = serde_json::to_string(&source_identity)?;
    assert!(!source_identity_text.contains(resolver_a_address));
    assert!(!source_identity_text.contains(resolver_b_address));

    for (address, expected_count) in [
        (unlisted_resolver_address, 1_i64),
        (resolver_a_address, 1_i64),
        (resolver_b_address, 1_i64),
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs WHERE emitting_address = $1")
                .bind(address)
                .fetch_one(database.pool())
                .await?,
            expected_count
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs WHERE emitting_address = $1")
            .bind(registry_address)
            .fetch_one(database.pool())
            .await?,
        1
    );

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    let log_requests = requests
        .iter()
        .filter(|request| request.method == "eth_getLogs")
        .collect::<Vec<_>>();
    assert_eq!(log_requests.len(), 2);
    let log_filter = log_requests[0]
        .params
        .first()
        .and_then(Value::as_object)
        .expect("log request must include a filter object");
    assert_eq!(
        log_filter.get("fromBlock").and_then(Value::as_str),
        Some("0xa")
    );
    assert_eq!(
        log_filter.get("toBlock").and_then(Value::as_str),
        Some("0xd")
    );
    assert!(
        !log_filter.contains_key("address"),
        "generic ENSv1 resolver bootstrap scan must not carry a resolver address filter"
    );
    assert!(
        log_filter.get("topics").is_some(),
        "generic ENSv1 resolver bootstrap scan must still constrain to resolver event topics"
    );
    let address_filter = log_requests[1]
        .params
        .first()
        .and_then(Value::as_object)
        .expect("address-scoped log request must include a filter object");
    assert_eq!(
        address_filter.get("fromBlock").and_then(Value::as_str),
        Some("0xa")
    );
    assert_eq!(
        address_filter.get("toBlock").and_then(Value::as_str),
        Some("0xd")
    );
    assert_eq!(
        address_filter.get("address").and_then(Value::as_array),
        Some(&vec![Value::String(registry_address.to_owned())]),
        "mixed bootstrap scan should query non-resolver targets in the same job"
    );

    let rerun = run_startup_bootstrap_backfills(
        database.pool(),
        &manifest_root,
        &intake_tasks,
        &provider_registry,
        crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
        crate::backfill::BackfillAdapterSyncMode::RawOnly,
        false,
        HeaderAuditMode::Minimal,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_WORKERS,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_RANGE_BLOCKS,
    )
    .await?;
    assert_eq!(rerun.drained_job_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM backfill_jobs")
            .fetch_one(database.pool())
            .await?,
        1
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn bootstrap_auto_backfill_covers_declared_start_to_provider_head() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_bootstrap_backfill_job_tables(database.pool()).await?;
    let manifest_root = PathBuf::from("manifests/mainnet");
    let contract_instance_id = Uuid::from_u128(9_500);
    let address = "0x0000000000000000000000000000000000000950";

    insert_bootstrap_manifest_version(
        database.pool(),
        950,
        "ens",
        "ethereum-mainnet",
        "ens_bootstrap_registry",
        json!({
            "contracts": [
                {
                    "role": "registry",
                    "address": address,
                    "start_block": 1
                }
            ],
            "roots": []
        }),
    )
    .await?;
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
        Some(950),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        950,
        "registry",
        contract_instance_id,
        address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let intake_tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
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
    let requests = Arc::new(Mutex::new(Vec::<BootstrapRpcRequest>::new()));
    let (provider, server) = bootstrap_auto_backfill_provider(
        vec![
            ProviderBlockFixture {
                block: block_1.clone(),
                logs: vec![bootstrap_rpc_log_payload_at_address(&block_1, address, 0)],
            },
            ProviderBlockFixture {
                block: block_2.clone(),
                logs: vec![bootstrap_rpc_log_payload_at_address(&block_2, address, 0)],
            },
            ProviderBlockFixture {
                block: block_3.clone(),
                logs: vec![bootstrap_rpc_log_payload_at_address(&block_3, address, 0)],
            },
            ProviderBlockFixture {
                block: block_4.clone(),
                logs: vec![bootstrap_rpc_log_payload_at_address(&block_4, address, 0)],
            },
        ],
        Arc::clone(&requests),
    )
    .await?;
    let provider_registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={provider}")])?;

    let outcome = run_startup_bootstrap_backfills(
        database.pool(),
        &manifest_root,
        &intake_tasks,
        &provider_registry,
        crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
        crate::backfill::BackfillAdapterSyncMode::Inline,
        false,
        HeaderAuditMode::Minimal,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_WORKERS,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_RANGE_BLOCKS,
    )
    .await?;
    assert_eq!(outcome.drained_job_count, 1);
    assert_eq!(outcome.resolved_block_count, 4);
    assert_eq!(outcome.raw_log_count, 4);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage")
            .fetch_one(database.pool())
            .await?,
        4
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage")
            .fetch_one(database.pool())
            .await?,
        4
    );

    let job = sqlx::query_as::<_, (i64, i64, String, Value)>(
        r#"
        SELECT
            range_start_block_number,
            range_end_block_number,
            idempotency_key,
            source_identity
        FROM backfill_jobs
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!((job.0, job.1), (1, 4));
    assert!(job.2.contains("from=1:to=4"));
    assert_eq!(
        job.3
            .get("selected_targets")
            .and_then(Value::as_array)
            .and_then(|targets| targets.first())
            .and_then(|target| target.get("effective_from_block"))
            .and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(
        job.3
            .get("selected_targets")
            .and_then(Value::as_array)
            .and_then(|targets| targets.first())
            .and_then(|target| target.get("effective_to_block"))
            .and_then(Value::as_i64),
        Some(4)
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT range_start_block_number, range_end_block_number FROM backfill_ranges"
        )
        .fetch_one(database.pool())
        .await?,
        (1, 4)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM chain_lineage WHERE block_number IN (1, 2)"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );

    sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET idempotency_key = replace(
            idempotency_key,
            'indexer-bootstrap-backfill:v3:deployment_profile=mainnet:',
            'indexer-bootstrap-backfill:v1:deployment_profile=mainnet:manifest_root=manifests:'
        )
        "#,
    )
    .execute(database.pool())
    .await
    .context("failed to rewrite bootstrap job to legacy manifest-root idempotency key")?;
    let legacy_idempotency_key =
        sqlx::query_scalar::<_, String>("SELECT idempotency_key FROM backfill_jobs")
            .fetch_one(database.pool())
            .await?;
    assert!(legacy_idempotency_key.contains("manifest_root=manifests"));

    let root_alias_rerun = run_startup_bootstrap_backfills(
        database.pool(),
        &manifest_root,
        &intake_tasks,
        &provider_registry,
        crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
        crate::backfill::BackfillAdapterSyncMode::Inline,
        false,
        HeaderAuditMode::Minimal,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_WORKERS,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_RANGE_BLOCKS,
    )
    .await?;
    assert_eq!(root_alias_rerun.drained_job_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM backfill_jobs")
            .fetch_one(database.pool())
            .await?,
        1
    );

    let block_5 = provider_block(
        "0x5000000000000000000000000000000000000000000000000000000000000005",
        Some(&block_4.block_hash),
        5,
    );
    let block_6 = provider_block(
        "0x6000000000000000000000000000000000000000000000000000000000000006",
        Some(&block_5.block_hash),
        6,
    );
    let catchup_requests = Arc::new(Mutex::new(Vec::<BootstrapRpcRequest>::new()));
    let (catchup_provider, catchup_server) = bootstrap_auto_backfill_provider(
        vec![
            ProviderBlockFixture {
                block: block_1.clone(),
                logs: vec![bootstrap_rpc_log_payload_at_address(&block_1, address, 0)],
            },
            ProviderBlockFixture {
                block: block_2.clone(),
                logs: vec![bootstrap_rpc_log_payload_at_address(&block_2, address, 0)],
            },
            ProviderBlockFixture {
                block: block_3.clone(),
                logs: vec![bootstrap_rpc_log_payload_at_address(&block_3, address, 0)],
            },
            ProviderBlockFixture {
                block: block_4.clone(),
                logs: vec![bootstrap_rpc_log_payload_at_address(&block_4, address, 0)],
            },
            ProviderBlockFixture {
                block: block_5.clone(),
                logs: vec![bootstrap_rpc_log_payload_at_address(&block_5, address, 0)],
            },
            ProviderBlockFixture {
                block: block_6.clone(),
                logs: vec![bootstrap_rpc_log_payload_at_address(&block_6, address, 0)],
            },
        ],
        Arc::clone(&catchup_requests),
    )
    .await?;
    let catchup_provider_registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={catchup_provider}")])?;

    let catchup = run_startup_bootstrap_backfills(
        database.pool(),
        &manifest_root,
        &intake_tasks,
        &catchup_provider_registry,
        crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
        crate::backfill::BackfillAdapterSyncMode::Inline,
        false,
        HeaderAuditMode::Minimal,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_WORKERS,
        crate::bootstrap_backfill::DEFAULT_BOOTSTRAP_BACKFILL_RANGE_BLOCKS,
    )
    .await?;
    assert_eq!(catchup.drained_job_count, 1);
    assert_eq!(catchup.resolved_block_count, 2);
    assert_eq!(catchup.raw_log_count, 2);
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT range_start_block_number, range_end_block_number FROM backfill_jobs ORDER BY backfill_job_id"
        )
        .fetch_all(database.pool())
        .await?,
        vec![(1, 4), (5, 6)]
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT range_start_block_number, range_end_block_number FROM backfill_ranges ORDER BY backfill_range_id"
        )
        .fetch_all(database.pool())
        .await?,
        vec![(1, 4), (5, 6)]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage")
            .fetch_one(database.pool())
            .await?,
        6
    );
    let catchup_job = sqlx::query_as::<_, (String, Value)>(
        r#"
        SELECT idempotency_key, source_identity
        FROM backfill_jobs
        ORDER BY backfill_job_id DESC
        LIMIT 1
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert!(catchup_job.0.contains("from=5:to=6"));
    assert_eq!(
        catchup_job
            .1
            .get("selected_targets")
            .and_then(Value::as_array)
            .and_then(|targets| targets.first())
            .and_then(|target| target.get("effective_from_block"))
            .and_then(Value::as_i64),
        Some(5)
    );
    assert_eq!(
        catchup_job
            .1
            .get("selected_targets")
            .and_then(Value::as_array)
            .and_then(|targets| targets.first())
            .and_then(|target| target.get("effective_to_block"))
            .and_then(Value::as_i64),
        Some(6)
    );

    catchup_server.abort();
    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn bootstrap_auto_backfill_partitions_ranges_for_internal_workers() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_bootstrap_backfill_job_tables(database.pool()).await?;
    let manifest_root = PathBuf::from("manifests/mainnet");
    let contract_instance_id = Uuid::from_u128(9_600);
    let address = "0x0000000000000000000000000000000000000960";

    insert_bootstrap_manifest_version(
        database.pool(),
        960,
        "ens",
        "ethereum-mainnet",
        "ens_bootstrap_registry",
        json!({
            "contracts": [
                {
                    "role": "registry",
                    "address": address,
                    "start_block": 1
                }
            ],
            "roots": []
        }),
    )
    .await?;
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
        Some(960),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        960,
        "registry",
        contract_instance_id,
        address,
        "none",
        None,
        None,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let intake_tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let blocks = (1..=4)
        .map(|block_number| {
            provider_block(
                &format!("0x{block_number:064x}"),
                Some(&format!("0x{:064x}", block_number - 1)),
                block_number,
            )
        })
        .collect::<Vec<_>>();
    let requests = Arc::new(Mutex::new(Vec::<BootstrapRpcRequest>::new()));
    let (provider, server) = bootstrap_auto_backfill_provider(
        blocks
            .iter()
            .map(|block| ProviderBlockFixture {
                block: block.clone(),
                logs: vec![bootstrap_rpc_log_payload_at_address(block, address, 0)],
            })
            .collect(),
        Arc::clone(&requests),
    )
    .await?;
    let provider_registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={provider}")])?;

    let outcome = run_startup_bootstrap_backfills(
        database.pool(),
        &manifest_root,
        &intake_tasks,
        &provider_registry,
        crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
        crate::backfill::BackfillAdapterSyncMode::RawOnly,
        false,
        HeaderAuditMode::Minimal,
        2,
        2,
    )
    .await?;
    assert_eq!(outcome.drained_job_count, 1);
    assert_eq!(outcome.requested_worker_count, 2);
    assert_eq!(outcome.effective_worker_count, 2);
    assert_eq!(outcome.reserved_range_count, 2);
    assert_eq!(outcome.completed_range_count, 2);
    assert_eq!(outcome.resolved_block_count, 4);
    assert_eq!(outcome.raw_log_count, 4);

    assert_eq!(
        sqlx::query_as::<_, (i64, i64, String)>(
            r#"
            SELECT range_start_block_number, range_end_block_number, status::TEXT
            FROM backfill_ranges
            ORDER BY range_start_block_number
            "#
        )
        .fetch_all(database.pool())
        .await?,
        vec![
            (1, 2, "completed".to_owned()),
            (3, 4, "completed".to_owned())
        ]
    );
    let idempotency_key = sqlx::query_scalar::<_, String>(
        "SELECT idempotency_key FROM backfill_jobs ORDER BY backfill_job_id",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(idempotency_key.starts_with("indexer-bootstrap-backfill:v3:"));
    assert!(!idempotency_key.contains("manifest_root="));
    assert!(idempotency_key.contains("range_blocks=2"));

    server.abort();
    database.cleanup().await
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BootstrapRpcRequest {
    method: String,
    params: Vec<Value>,
    http_request_id: u64,
    batch_size: usize,
}

async fn insert_bootstrap_manifest_version(
    pool: &PgPool,
    manifest_id: i64,
    namespace: &str,
    chain: &str,
    source_family: &str,
    manifest_payload: Value,
) -> Result<()> {
    let manifest_payload = test_manifest_payload_with_abi(manifest_payload);
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
            VALUES ($1, $2, $3, $4, 'active', $5)
            "#,
    )
    .bind(manifest_id)
    .bind(namespace)
    .bind(source_family)
    .bind(chain)
    .bind(manifest_payload)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to insert bootstrap manifest {manifest_id} for {chain}:{source_family}")
    })?;

    Ok(())
}

async fn bootstrap_auto_backfill_provider(
    fixtures: Vec<ProviderBlockFixture>,
    requests: Arc<Mutex<Vec<BootstrapRpcRequest>>>,
) -> Result<(String, JoinHandle<()>)> {
    let fixtures_by_hash = Arc::new(
        fixtures
            .into_iter()
            .map(|fixture| (fixture.block.block_hash.clone(), fixture))
            .collect::<std::collections::BTreeMap<_, _>>(),
    );
    let hashes_by_number = Arc::new(
        fixtures_by_hash
            .values()
            .map(|fixture| (fixture.block.block_number, fixture.block.block_hash.clone()))
            .collect::<std::collections::BTreeMap<_, _>>(),
    );
    let latest_hash = hashes_by_number
        .iter()
        .next_back()
        .map(|(_, hash)| hash.clone())
        .context("bootstrap provider fixture must include a latest block")?;

    spawn_json_rpc_server(Arc::new(move |body| {
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
            .push(BootstrapRpcRequest {
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
                    "latest" => json!({ "hash": latest_hash }),
                    "safe" | "finalized" => Value::Null,
                    block_number => {
                        let block_number = parse_bootstrap_rpc_block_number(block_number);
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
                bootstrap_logs_for_filter(filter, &fixtures_by_hash, &hashes_by_number)
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
            "eth_getCode" => Value::String("0x6001600155".to_owned()),
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        })
    }))
    .await
}

fn bootstrap_logs_for_filter(
    filter: &serde_json::Map<String, Value>,
    fixtures_by_hash: &std::collections::BTreeMap<String, ProviderBlockFixture>,
    hashes_by_number: &std::collections::BTreeMap<i64, String>,
) -> Value {
    let address_filter = bootstrap_log_filter_addresses(filter);
    let topic0_filter = bootstrap_log_filter_topic0s(filter);
    let mut logs = Vec::new();

    if let Some(block_hash) = filter.get("blockHash").and_then(Value::as_str) {
        let fixture = fixtures_by_hash
            .get(&block_hash.to_ascii_lowercase())
            .unwrap_or_else(|| panic!("unexpected bootstrap log blockHash filter: {filter:?}"));
        logs.extend(bootstrap_filtered_fixture_logs(
            fixture,
            address_filter.as_ref(),
            topic0_filter.as_ref(),
        ));
    } else {
        let from_block = filter
            .get("fromBlock")
            .and_then(Value::as_str)
            .map(parse_bootstrap_rpc_block_number)
            .expect("bootstrap range log filter must include fromBlock");
        let to_block = filter
            .get("toBlock")
            .and_then(Value::as_str)
            .map(parse_bootstrap_rpc_block_number)
            .expect("bootstrap range log filter must include toBlock");
        assert!(
            from_block <= to_block,
            "bootstrap range log filter start must not exceed end: {filter:?}"
        );

        for block_number in from_block..=to_block {
            let block_hash = hashes_by_number
                .get(&block_number)
                .unwrap_or_else(|| panic!("unexpected bootstrap log range block: {filter:?}"));
            let fixture = fixtures_by_hash
                .get(block_hash)
                .expect("number index must point at a fixture block");
            logs.extend(bootstrap_filtered_fixture_logs(
                fixture,
                address_filter.as_ref(),
                topic0_filter.as_ref(),
            ));
        }
    }

    Value::Array(logs)
}

fn bootstrap_log_filter_addresses(
    filter: &serde_json::Map<String, Value>,
) -> Option<std::collections::BTreeSet<String>> {
    let addresses = filter.get("address")?;
    let addresses = match addresses {
        Value::String(address) => vec![address.to_ascii_lowercase()],
        Value::Array(addresses) => addresses
            .iter()
            .map(|address| {
                address
                    .as_str()
                    .expect("bootstrap log address filter values must be strings")
                    .to_ascii_lowercase()
            })
            .collect(),
        value => panic!("unexpected bootstrap log address filter: {value:?}"),
    };

    Some(addresses.into_iter().collect())
}

fn bootstrap_log_filter_topic0s(
    filter: &serde_json::Map<String, Value>,
) -> Option<std::collections::BTreeSet<String>> {
    let topics = filter.get("topics")?.as_array()?;
    let topic0 = topics.first()?;
    let values = match topic0 {
        Value::String(topic) => vec![topic.to_ascii_lowercase()],
        Value::Array(topics) => topics
            .iter()
            .map(|topic| {
                topic
                    .as_str()
                    .expect("bootstrap topic filter values must be strings")
                    .to_ascii_lowercase()
            })
            .collect(),
        Value::Null => return None,
        value => panic!("unexpected bootstrap topic0 filter: {value:?}"),
    };

    Some(values.into_iter().collect())
}

fn bootstrap_filtered_fixture_logs(
    fixture: &ProviderBlockFixture,
    address_filter: Option<&std::collections::BTreeSet<String>>,
    topic0_filter: Option<&std::collections::BTreeSet<String>>,
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

fn bootstrap_rpc_log_payload_at_address(
    block: &ProviderBlock,
    address: &str,
    log_index: i64,
) -> Value {
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

fn parse_bootstrap_rpc_block_number(value: &str) -> i64 {
    i64::from_str_radix(value.trim_start_matches("0x"), 16)
        .expect("test RPC block number must be hex")
}

async fn create_bootstrap_backfill_job_tables(pool: &PgPool) -> Result<()> {
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
    .context("failed to create backfill_lifecycle_status type for bootstrap tests")?;

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
            CHECK (jsonb_typeof(failure_metadata) = 'object')
        )
        "#,
    )
    .execute(pool)
    .await
    .context("failed to create backfill_jobs table for bootstrap tests")?;

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
            CHECK (jsonb_typeof(failure_metadata) = 'object')
        )
        "#,
    )
    .execute(pool)
    .await
    .context("failed to create backfill_ranges table for bootstrap tests")?;

    Ok(())
}
