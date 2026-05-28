use bigname_manifests::{
    WatchedContractSource, WatchedSourceSelector, load_watched_contracts,
    load_watched_source_selector_plan,
};

#[tokio::test]
async fn build_manifest_runtime_state_loads_checked_in_repository_seed() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet");
    let manifest_repository = load_manifest_repository(&manifests_root)?;

    let runtime_state = build_manifest_runtime_state(database.pool(), &manifest_repository).await?;

    assert_eq!(
        runtime_state.manifest_summary.status,
        ManifestLoadStatus::Loaded
    );
    assert_eq!(runtime_state.manifest_summary.namespace_count, 2);
    assert_eq!(runtime_state.manifest_summary.source_family_count, 12);
    assert_eq!(runtime_state.manifest_summary.manifest_count, 16);
    assert_eq!(
        runtime_state.sync_summary.status,
        ManifestSyncStatus::Synced
    );
    assert_eq!(runtime_state.sync_summary.synced_manifest_count, 16);
    assert_eq!(runtime_state.sync_summary.active_manifest_count, 11);
    assert_eq!(runtime_state.sync_summary.root_count, 6);
    assert_eq!(runtime_state.sync_summary.contract_count, 26);
    assert_eq!(runtime_state.sync_summary.capability_count, 10);
    assert_eq!(runtime_state.sync_summary.discovery_rule_count, 8);
    assert_eq!(runtime_state.discovery_admission.active_manifest_count, 11);
    assert_eq!(runtime_state.discovery_admission.active_root_count, 3);
    assert_eq!(runtime_state.discovery_admission.active_contract_count, 21);
    assert_eq!(runtime_state.discovery_admission.active_rule_count, 4);
    assert_eq!(
        runtime_state
            .manifest_normalized_event_summary
            .total_synced_count,
        16
    );
    assert_eq!(
        runtime_state.watched_contract_summary.unique_contract_count,
        20
    );
    assert_eq!(
        runtime_state.watched_contract_summary.source_entry_count,
        24
    );
    assert_eq!(
        runtime_state.watched_contract_summary.manifest_root_count,
        3
    );
    assert_eq!(
        runtime_state
            .watched_contract_summary
            .manifest_contract_count,
        21
    );
    assert_eq!(
        runtime_state.watched_contract_summary.discovery_edge_count,
        0
    );
    assert_eq!(
        runtime_state.watched_chain_plan,
        vec![
            WatchedChainPlan {
                chain: "base-mainnet".to_owned(),
                addresses: vec![
                    "0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a".to_owned(),
                    "0x79ea96012eea67a83431f1701b3dff7e37f9e282".to_owned(),
                    "0xb94704422c2a1e396835a571837aa5ae53285a95".to_owned(),
                    "0xc6d566a56a1aff6508b41f6c90ff131615583bcd".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 4,
                discovery_edge_entry_count: 0,
            },
            WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec![
                    "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
                    "0x1da022710df5002339274aadee8d58218e9d6ab5".to_owned(),
                    "0x226159d592e2b063810a10ebf6dcbada94ed68b8".to_owned(),
                    "0x231b0ee14048e9dccd1d247744d114a4eb5e8e63".to_owned(),
                    "0x253553366da8546fc250f225fe3d25d0c782303b".to_owned(),
                    "0x283af0b28c62c092c9727f1ee09c02ca627eb7f5".to_owned(),
                    "0x314159265dd8dbb310642f98f50c066173c1259b".to_owned(),
                    "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41".to_owned(),
                    "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85".to_owned(),
                    "0x59e16fccd424cc24e280be16e11bcd56fb0ce547".to_owned(),
                    "0x5ffc014343cd971b7eb70732021e26c35b744cc4".to_owned(),
                    "0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb".to_owned(),
                    "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401".to_owned(),
                    "0xdaaf96c344f63131acadd0ea35170e7892d3dfba".to_owned(),
                    "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31".to_owned(),
                    "0xf29100983e058b709f3d539b0c765937b804ac15".to_owned(),
                ],
                manifest_root_entry_count: 2,
                manifest_contract_entry_count: 17,
                discovery_edge_entry_count: 0,
            }
        ]
    );

    let stored_admission = load_discovery_admission_state(database.pool()).await?;
    assert_eq!(stored_admission.active_manifest_count, 11);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn ethereum_only_provider_leaves_active_base_watch_state_idle() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet");
    let manifest_repository = load_manifest_repository(&manifests_root)?;
    let runtime_state = build_manifest_runtime_state(database.pool(), &manifest_repository).await?;
    let mut intake_tasks =
        sync_intake_chain_tasks(database.pool(), &runtime_state.watched_chain_plan).await?;

    let base_task = intake_tasks
        .iter()
        .find(|task| task.chain == "base-mainnet")
        .expect("checked-in manifests must leave Base actively watched")
        .clone();
    assert!(
        intake_tasks
            .iter()
            .any(|task| task.chain == "ethereum-mainnet"),
        "checked-in manifests must leave Ethereum actively watched"
    );

    let canonical_head = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        42,
    );
    let canonical_hash = canonical_head.block_hash.clone();
    let rpc_head = canonical_head.clone();
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let request_log = std::sync::Arc::clone(&requests);
    let (ethereum_rpc_url, server) = spawn_json_rpc_server(std::sync::Arc::new(move |body| {
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(body.clone());
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let first_param = params
            .first()
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();

        let result = match method {
            "eth_getBlockByNumber" if first_param == "latest" => {
                json!({ "hash": canonical_hash.clone() })
            }
            "eth_getBlockByNumber" if first_param == "safe" || first_param == "finalized" => {
                Value::Null
            }
            "eth_getBlockByHash" if first_param == rpc_head.block_hash.as_str() => {
                rpc_block_bundle_payload(&rpc_head)
            }
            "eth_getLogs" => Value::Array(Vec::<Value>::new()),
            "eth_getBlockReceipts" if first_param == rpc_head.block_hash.as_str() => {
                Value::Array(vec![rpc_receipt_payload(&rpc_head)])
            }
            "eth_getCode" => Value::String("0x6001600155".to_owned()),
            _ => panic!("unexpected Ethereum-only RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        })
    }))
    .await?;
    let chain_rpc_urls = vec![format!("ethereum-mainnet={ethereum_rpc_url}")];
    let provider_registry = ProviderRegistry::from_chain_rpc_urls(&chain_rpc_urls)?;
    assert!(provider_registry.provider_for("ethereum-mainnet").is_some());
    assert!(provider_registry.provider_for("base-mainnet").is_none());
    validate_provider_registry_for_intake_tasks(&intake_tasks, &provider_registry)?;

    log_provider_registry("test", &intake_tasks, &provider_registry);
    poll_provider_heads(database.pool(), &mut intake_tasks, &provider_registry).await?;

    let base_task_after_poll = intake_tasks
        .iter()
        .find(|task| task.chain == "base-mainnet")
        .expect("Base task must remain present after provider polling");
    assert_eq!(base_task_after_poll.checkpoint.canonical_block_number, None);
    assert_eq!(base_task_after_poll.checkpoint.canonical_block_hash, None);
    let ethereum_task_after_poll = intake_tasks
        .iter()
        .find(|task| task.chain == "ethereum-mainnet")
        .expect("Ethereum task must remain present after provider polling");
    assert_eq!(
        ethereum_task_after_poll.checkpoint.canonical_block_number,
        Some(42)
    );

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM chain_lineage WHERE chain_id = 'base-mainnet'"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM chain_lineage WHERE chain_id = 'ethereum-mainnet'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    let request_bodies = requests
        .lock()
        .expect("request log must not be poisoned")
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>();
    assert!(
        !request_bodies.is_empty(),
        "Ethereum provider should be used for the configured chain"
    );
    for base_address in &base_task.addresses {
        assert!(
            !request_bodies
                .iter()
                .any(|request| request.contains(base_address)),
            "Base address {base_address} must not be requested from the Ethereum-only RPC"
        );
    }

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn refresh_watched_chain_plan_detects_storage_changes() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(41);
    let discovered_contract_instance_id = Uuid::from_u128(42);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for watched plan refresh test")?;
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

    let initial_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        refresh_watched_chain_plan(database.pool(), &initial_plan).await?,
        None
    );

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
    insert_active_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "subregistry",
        root_contract_instance_id,
        discovered_contract_instance_id,
        Some(1),
    )
    .await?;

    let refreshed_plan = refresh_watched_chain_plan(database.pool(), &initial_plan)
        .await?
        .expect("watch plan change must be detected");
    assert_eq!(refreshed_plan.len(), 1);
    assert_eq!(refreshed_plan[0].chain, "ethereum-mainnet");
    assert_eq!(
        refreshed_plan[0].addresses,
        vec![
            "0x0000000000000000000000000000000000000001".to_owned(),
            "0x00000000000000000000000000000000000000cc".to_owned(),
        ]
    );
    assert_eq!(refreshed_plan[0].manifest_root_entry_count, 1);
    assert_eq!(refreshed_plan[0].manifest_contract_entry_count, 0);
    assert_eq!(refreshed_plan[0].discovery_edge_entry_count, 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn refresh_intake_chain_tasks_detects_checkpoint_updates() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(51);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for checkpoint refresh test")?;
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
    let initial_tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    assert_eq!(
        refresh_intake_chain_tasks(database.pool(), &initial_tasks, &watched_plan).await?,
        None
    );

    sqlx::query(
            r#"
            UPDATE chain_checkpoints
            SET
                canonical_block_hash = '0x00000000000000000000000000000000000000000000000000000000000000aa',
                canonical_block_number = 42,
                safe_block_hash = '0x0000000000000000000000000000000000000000000000000000000000000099',
                safe_block_number = 41,
                finalized_block_hash = '0x0000000000000000000000000000000000000000000000000000000000000088',
                finalized_block_number = 40
            WHERE chain_id = 'ethereum-mainnet'
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to update chain_checkpoints for checkpoint refresh test")?;

    let refreshed_tasks =
        refresh_intake_chain_tasks(database.pool(), &initial_tasks, &watched_plan)
            .await?
            .expect("checkpoint change must be detected");
    assert_eq!(refreshed_tasks.len(), 1);
    assert_eq!(
        refreshed_tasks[0].checkpoint.canonical_block_number,
        Some(42)
    );
    assert_eq!(checkpoint_mode(&refreshed_tasks[0].checkpoint), "resume");
    assert_eq!(
        intake_runtime_state(&refreshed_tasks),
        IntakeRuntimeState {
            chain_count: 1,
            address_count: 1,
            entry_count: 1,
            cold_start_chain_count: 0,
            resumable_chain_count: 1,
            safe_checkpoint_chain_count: 1,
            finalized_checkpoint_chain_count: 1,
        }
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn build_manifest_runtime_state_reloads_repository_changes_without_restart() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests.write_manifest(&manifest_contents(
        "0x0000000000000000000000000000000000000001",
        "shadow",
    ))?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    assert_eq!(
        initial_repository.summary().status,
        ManifestLoadStatus::Loaded
    );
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;
    assert_eq!(initial_state.watched_chain_plan.len(), 1);
    assert_eq!(
        initial_state.watched_chain_plan[0].addresses,
        vec![
            "0x0000000000000000000000000000000000000001".to_owned(),
            "0x00000000000000000000000000000000000000aa".to_owned(),
        ]
    );
    assert_eq!(
        initial_state
            .manifest_normalized_event_summary
            .total_inserted_count,
        2
    );

    fs::write(
        &manifest_path,
        manifest_contents("0x0000000000000000000000000000000000000002", "supported"),
    )
    .with_context(|| format!("failed to rewrite {}", manifest_path.display()))?;

    let refreshed_repository = load_manifest_repository(&manifests.path)?;
    let refreshed_state =
        build_manifest_runtime_state(database.pool(), &refreshed_repository).await?;
    assert_eq!(
        refreshed_state.watched_chain_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000002".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
        }]
    );
    assert_eq!(
        refreshed_state
            .manifest_normalized_event_summary
            .total_inserted_count,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE event_kind = 'CapabilityChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn refresh_watched_chain_plan_reuses_contract_instance_ids_across_inactive_gaps() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests.write_manifest(&manifest_contents(
        "0x0000000000000000000000000000000000000001",
        "shadow",
    ))?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;
    let initial_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;

    fs::remove_file(&manifest_path)
        .with_context(|| format!("failed to remove {}", manifest_path.display()))?;

    let empty_repository = load_manifest_repository(&manifests.path)?;
    let empty_state = build_manifest_runtime_state(database.pool(), &empty_repository).await?;
    assert!(empty_state.watched_chain_plan.is_empty());
    assert_eq!(
        refresh_watched_chain_plan(database.pool(), &initial_state.watched_chain_plan).await?,
        Some(Vec::new())
    );

    fs::write(
        &manifest_path,
        manifest_contents("0x0000000000000000000000000000000000000001", "shadow"),
    )
    .with_context(|| format!("failed to rewrite {}", manifest_path.display()))?;

    let restored_repository = load_manifest_repository(&manifests.path)?;
    let restored_state =
        build_manifest_runtime_state(database.pool(), &restored_repository).await?;
    let restored_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;

    assert_eq!(initial_contract_instance_id, restored_contract_instance_id);
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1"
            )
            .bind(initial_contract_instance_id)
            .fetch_one(database.pool())
            .await?,
            2
        );
    assert_eq!(
        refresh_watched_chain_plan(database.pool(), &empty_state.watched_chain_plan).await?,
        Some(restored_state.watched_chain_plan.clone())
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn runtime_refresh_tracks_proxy_implementation_alert_churn() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests.write_manifest(&manifest_contents_with_contract(
        "0x0000000000000000000000000000000000000001",
        "supported",
        "0x00000000000000000000000000000000000000aa",
        "erc1967",
        Some("0x00000000000000000000000000000000000000dd"),
    ))?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;
    let proxy_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;
    let first_implementation_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000dd",
    )
    .await?;

    assert_eq!(
        initial_state.watched_chain_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
                "0x00000000000000000000000000000000000000dd".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );
    assert_eq!(
        manifest_normalized_event_kind_count(
            &initial_state.manifest_normalized_event_summary,
            "ManifestProxyImplementationAlert",
        ),
        1
    );

    fs::write(
        &manifest_path,
        manifest_contents_with_contract(
            "0x0000000000000000000000000000000000000001",
            "supported",
            "0x00000000000000000000000000000000000000aa",
            "erc1967",
            Some("0x00000000000000000000000000000000000000ee"),
        ),
    )
    .with_context(|| format!("failed to rewrite {}", manifest_path.display()))?;

    let refreshed_repository = load_manifest_repository(&manifests.path)?;
    let refreshed_state =
        build_manifest_runtime_state(database.pool(), &refreshed_repository).await?;
    let refreshed_plan =
        refresh_watched_chain_plan(database.pool(), &initial_state.watched_chain_plan)
            .await?
            .expect("proxy implementation churn must refresh the watched plan");
    let second_implementation_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000ee",
    )
    .await?;
    let intake_tasks =
        sync_intake_chain_tasks(database.pool(), &refreshed_state.watched_chain_plan).await?;

    assert_eq!(
        proxy_contract_instance_id,
        load_single_contract_instance_for_address(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
        )
        .await?
    );
    assert_ne!(
        first_implementation_contract_instance_id,
        second_implementation_contract_instance_id
    );
    assert_eq!(
        manifest_normalized_event_kind_count(
            &refreshed_state.manifest_normalized_event_summary,
            "ManifestProxyImplementationAlert",
        ),
        1
    );
    assert_eq!(
        refreshed_state
            .manifest_normalized_event_summary
            .by_kind
            .get("ManifestProxyImplementationAlert")
            .expect("proxy implementation alert summary must be present")
            .inserted_count,
        1
    );
    assert_eq!(refreshed_plan, refreshed_state.watched_chain_plan);
    assert_eq!(
        refreshed_state.watched_chain_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
                "0x00000000000000000000000000000000000000ee".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );
    assert_eq!(
        intake_tasks[0].addresses,
        refreshed_state.watched_chain_plan[0].addresses
    );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'proxy_implementation' AND deactivated_at IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
            )
            .bind(first_implementation_contract_instance_id)
            .fetch_one(database.pool())
            .await?,
            0
        );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'implementation_address' FROM normalized_events WHERE event_kind = 'ManifestProxyImplementationAlert' ORDER BY normalized_event_id DESC LIMIT 1"
        )
        .fetch_one(database.pool())
        .await?,
        "0x00000000000000000000000000000000000000ee".to_owned()
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn runtime_refresh_emits_code_hash_drift_alert_without_watch_plan_change() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let root_address = "0x0000000000000000000000000000000000000001";
    manifests.write_manifest(&manifest_contents_with_root_code_hash(
        root_address,
        "0xexpected",
    ))?;

    let manifest_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &manifest_repository).await?;
    assert_eq!(initial_state.watched_chain_plan.len(), 1);
    assert_eq!(
        manifest_normalized_event_kind_count(
            &initial_state.manifest_normalized_event_summary,
            "ManifestCodeHashDriftAlert",
        ),
        0
    );

    upsert_raw_code_hashes(
        database.pool(),
        &[RawCodeHash {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            block_number: 42,
            contract_address: root_address.to_owned(),
            code_hash: "0xobserved".to_owned(),
            code_byte_length: 32,
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;

    assert_eq!(
        refresh_watched_chain_plan(database.pool(), &initial_state.watched_chain_plan).await?,
        None
    );
    let refreshed_state =
        refresh_manifest_normalized_events_from_storage(database.pool(), &initial_state)
            .await?
            .expect("code-hash drift alert must refresh manifest normalized-event summary");
    assert_eq!(
        refreshed_state.watched_chain_plan,
        initial_state.watched_chain_plan
    );
    assert_eq!(
        manifest_normalized_event_kind_count(
            &refreshed_state.manifest_normalized_event_summary,
            "ManifestCodeHashDriftAlert",
        ),
        1
    );
    assert_eq!(
        refreshed_state
            .manifest_normalized_event_summary
            .by_kind
            .get("ManifestCodeHashDriftAlert")
            .expect("code-hash drift alert summary must be present")
            .inserted_count,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'expected_code_hash' FROM normalized_events WHERE event_kind = 'ManifestCodeHashDriftAlert'"
        )
        .fetch_one(database.pool())
        .await?,
        "0xexpected".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'observed_code_hash' FROM normalized_events WHERE event_kind = 'ManifestCodeHashDriftAlert'"
        )
        .fetch_one(database.pool())
        .await?,
        "0xobserved".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE event_kind = 'ManifestCodeHashDriftAlert'"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn watched_plan_excludes_successor_migration_edges_from_address_expansion() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests.write_manifest(&manifest_contents_with_contract(
        "0x0000000000000000000000000000000000000001",
        "supported",
        "0x00000000000000000000000000000000000000aa",
        "erc1967",
        Some("0x00000000000000000000000000000000000000dd"),
    ))?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;

    fs::write(
        &manifest_path,
        manifest_contents_with_contract(
            "0x0000000000000000000000000000000000000001",
            "supported",
            "0x00000000000000000000000000000000000000bb",
            "erc1967",
            Some("0x00000000000000000000000000000000000000dd"),
        ),
    )
    .with_context(|| format!("failed to rewrite {}", manifest_path.display()))?;

    let refreshed_repository = load_manifest_repository(&manifests.path)?;
    let refreshed_state =
        build_manifest_runtime_state(database.pool(), &refreshed_repository).await?;
    let refreshed_plan =
        refresh_watched_chain_plan(database.pool(), &initial_state.watched_chain_plan)
            .await?
            .expect("successor address rotation must refresh the watched plan");
    let intake_tasks =
        sync_intake_chain_tasks(database.pool(), &refreshed_state.watched_chain_plan).await?;

    assert_eq!(refreshed_plan, refreshed_state.watched_chain_plan);
    assert_eq!(
        refreshed_state.watched_chain_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000bb".to_owned(),
                "0x00000000000000000000000000000000000000dd".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );
    assert!(
        !refreshed_state.watched_chain_plan[0]
            .addresses
            .contains(&"0x00000000000000000000000000000000000000aa".to_owned())
    );
    assert_eq!(
        intake_tasks[0].addresses,
        refreshed_state.watched_chain_plan[0].addresses
    );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'migration' AND deactivated_at IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn build_manifest_runtime_state_accepts_empty_root_and_clears_watch_plan() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests.write_manifest(&manifest_contents(
        "0x0000000000000000000000000000000000000001",
        "shadow",
    ))?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;
    assert_eq!(
        initial_state.manifest_summary.status,
        ManifestLoadStatus::Loaded
    );
    assert_eq!(initial_state.watched_chain_plan.len(), 1);

    fs::remove_file(&manifest_path)
        .with_context(|| format!("failed to remove {}", manifest_path.display()))?;

    let empty_repository = load_manifest_repository(&manifests.path)?;
    assert_eq!(empty_repository.summary().status, ManifestLoadStatus::Empty);
    let empty_state = build_manifest_runtime_state(database.pool(), &empty_repository).await?;
    assert_eq!(
        empty_state.manifest_summary.status,
        ManifestLoadStatus::Empty
    );
    assert!(empty_state.watched_chain_plan.is_empty());
    assert_eq!(empty_state.discovery_admission.active_manifest_count, 0);
    assert_eq!(empty_state.watched_contract_summary.source_entry_count, 0);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reconcile_fetched_heads_backfills_code_hashes_for_new_watched_addresses() -> Result<()> {
    let database = TestDatabase::new().await?;
    let first_root_contract_instance_id = Uuid::from_u128(61);
    let second_contract_instance_id = Uuid::from_u128(62);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for code-hash backfill test")?;
    insert_contract_instance(
        database.pool(),
        first_root_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        first_root_contract_instance_id,
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000001",
        Some(1),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        1,
        first_root_contract_instance_id,
        "0x0000000000000000000000000000000000000001",
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let mut tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let canonical_head = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        42,
    );
    let (provider, server) = bundle_provider(vec![canonical_head.clone()]).await?;

    let (next_task, initial_outcome) = reconcile_fetched_heads(
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
    .expect("initial code-hash reconciliation must update task state");
    assert_eq!(
        initial_outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    tasks[0] = next_task;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        1
    );

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
        "0x0000000000000000000000000000000000000002",
        Some(1),
    )
    .await?;
    insert_active_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "subregistry",
        first_root_contract_instance_id,
        second_contract_instance_id,
        Some(1),
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let unchanged = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head,
            safe: None,
            finalized: None,
        },
    )
    .await?;
    assert!(
        unchanged.is_none(),
        "unchanged heads should not report a task transition"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        2
    );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT code_byte_length FROM raw_code_hashes WHERE contract_address = '0x0000000000000000000000000000000000000002'"
            )
            .fetch_one(database.pool())
            .await?,
            0
        );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn storage_discovery_refresh_adds_ensv1_address_without_manifest_reload_and_next_poll_backfills_code_hash()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests
        .write_manifest_for_source_family("ens_v1_registry_l1", &ens_v1_manifest_contents())?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;
    let initial_manifest_summary = initial_state.manifest_summary.clone();
    let initial_sync_summary = initial_state.sync_summary.clone();
    let initial_discovery_admission = initial_state.discovery_admission.clone();
    let initial_manifest_event_summary = initial_state.manifest_normalized_event_summary.clone();
    let initial_tasks =
        sync_intake_chain_tasks(database.pool(), &initial_state.watched_chain_plan).await?;

    assert_eq!(initial_state.watched_chain_plan.len(), 1);
    assert_eq!(initial_tasks.len(), 1);
    assert_eq!(initial_tasks[0].addresses.len(), 1);

    let canonical_head = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        42,
    );
    let (provider, server) = bundle_provider(vec![canonical_head.clone()]).await?;

    let (_next_task, initial_outcome) = reconcile_fetched_heads(
        database.pool(),
        &initial_tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("initial ENSv1 registry poll must update task state");
    assert_eq!(
        initial_outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        1
    );

    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        &canonical_head,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x0000000000000000000000000000000000000002",
        CanonicalityState::Canonical,
    )
    .await?;

    let (refreshed_state, refreshed_tasks) =
        refresh_runtime_state_from_storage_discovery(database.pool(), &initial_state)
            .await?
            .expect("stored ENSv1 discovery must refresh the watched plan without manifest reload");

    assert_eq!(refreshed_state.manifest_summary, initial_manifest_summary);
    assert_eq!(refreshed_state.sync_summary, initial_sync_summary);
    assert_eq!(
        refreshed_state.discovery_admission,
        initial_discovery_admission
    );
    assert_eq!(
        refreshed_state.manifest_normalized_event_summary,
        initial_manifest_event_summary
    );
    assert_eq!(
        fs::read_to_string(&manifest_path)
            .with_context(|| format!("failed to read {}", manifest_path.display()))?,
        ens_v1_manifest_contents()
    );
    assert_eq!(refreshed_state.watched_chain_plan.len(), 1);
    assert_eq!(refreshed_tasks.len(), 1);
    assert_eq!(
        refreshed_state.watched_chain_plan[0].chain,
        "ethereum-mainnet"
    );
    assert_eq!(refreshed_state.watched_chain_plan[0].addresses.len(), 2);
    assert!(
        refreshed_state.watched_chain_plan[0]
            .addresses
            .contains(&"0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned())
    );
    assert!(
        refreshed_state.watched_chain_plan[0]
            .addresses
            .contains(&"0x0000000000000000000000000000000000000002".to_owned())
    );
    assert_eq!(
        refreshed_state.watched_chain_plan[0].manifest_root_entry_count,
        1
    );
    assert_eq!(
        refreshed_state.watched_chain_plan[0].manifest_contract_entry_count,
        1
    );
    assert_eq!(
        refreshed_state.watched_chain_plan[0].discovery_edge_entry_count,
        1
    );
    assert_eq!(
        refreshed_tasks[0].addresses,
        refreshed_state.watched_chain_plan[0].addresses
    );
    assert_eq!(
        refreshed_tasks[0].checkpoint.canonical_block_number,
        Some(42)
    );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = 'ens_v1_registry_new_owner:ethereum-mainnet' AND deactivated_at IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE event_kind = 'SubregistryChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'owner' FROM normalized_events WHERE event_kind = 'SubregistryChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            "0x0000000000000000000000000000000000000002".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM normalized_events WHERE event_kind = 'SubregistryChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );

    let unchanged = reconcile_fetched_heads(
        database.pool(),
        &refreshed_tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head,
            safe: None,
            finalized: None,
        },
    )
    .await?;
    assert!(
        unchanged.is_none(),
        "unchanged heads should still backfill code hashes for newly watched ENSv1 addresses"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        2
    );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT code_byte_length FROM raw_code_hashes WHERE contract_address = '0x0000000000000000000000000000000000000002'"
            )
            .fetch_one(database.pool())
            .await?,
            0
        );
    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn runtime_refresh_adds_ensv1_resolver_watch_target_without_manifest_reload() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_contents = ens_v1_registry_resolver_discovery_manifest_contents();
    let manifest_path =
        manifests.write_manifest_for_source_family("ens_v1_registry_l1", &manifest_contents)?;
    manifests.write_manifest_for_source_family(
        "ens_v1_resolver_l1",
        &ens_v1_resolver_manifest_contents(),
    )?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;
    let initial_manifest_summary = initial_state.manifest_summary.clone();
    let initial_sync_summary = initial_state.sync_summary.clone();
    let initial_discovery_admission = initial_state.discovery_admission.clone();
    let initial_manifest_event_summary = initial_state.manifest_normalized_event_summary.clone();
    let initial_tasks =
        sync_intake_chain_tasks(database.pool(), &initial_state.watched_chain_plan).await?;

    assert_eq!(initial_state.watched_chain_plan.len(), 1);
    assert_eq!(initial_tasks.len(), 1);
    assert_eq!(initial_tasks[0].addresses.len(), 1);

    let canonical_head = provider_block(
        "0xabababababababababababababababababababababababababababababababab",
        Some("0xbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc"),
        43,
    );
    let (provider, server) = bundle_provider(vec![canonical_head.clone()]).await?;

    reconcile_fetched_heads(
        database.pool(),
        &initial_tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("initial ENSv1 registry poll must update task state");

    insert_raw_new_resolver_log_for_node(
        database.pool(),
        "ethereum-mainnet",
        &canonical_head,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x0000000000000000000000000000000000000003",
        &namehash_for_dns_name(&dns_encoded_eth_name("alice")),
        CanonicalityState::Canonical,
    )
    .await?;

    let (refreshed_state, refreshed_tasks) = refresh_runtime_state_from_storage_discovery(
        database.pool(),
        &initial_state,
    )
    .await?
    .expect(
        "stored ENSv1 resolver discovery must refresh the watched plan without manifest reload",
    );

    assert_eq!(refreshed_state.manifest_summary, initial_manifest_summary);
    assert_eq!(refreshed_state.sync_summary, initial_sync_summary);
    assert_eq!(
        refreshed_state.discovery_admission,
        initial_discovery_admission
    );
    assert_eq!(
        refreshed_state.manifest_normalized_event_summary,
        initial_manifest_event_summary
    );
    assert_eq!(
        fs::read_to_string(&manifest_path)
            .with_context(|| format!("failed to read {}", manifest_path.display()))?,
        manifest_contents
    );
    assert_eq!(refreshed_state.watched_chain_plan.len(), 1);
    assert_eq!(refreshed_tasks.len(), 1);
    assert_eq!(
        refreshed_state.watched_chain_plan[0].addresses,
        vec![
            "0x0000000000000000000000000000000000000003".to_owned(),
            "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
        ]
    );
    assert_eq!(
        refreshed_state.watched_chain_plan[0].discovery_edge_entry_count,
        1
    );
    assert_eq!(
        refreshed_tasks[0].addresses,
        refreshed_state.watched_chain_plan[0].addresses
    );
    let resolver_source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_resolver_l1".to_owned()),
        43,
        43,
    )
    .await?;
    assert_eq!(resolver_source_plan.selected_targets.len(), 1);
    assert_eq!(
        resolver_source_plan.selected_targets[0].source_family,
        "ens_v1_resolver_l1"
    );
    assert_eq!(
        resolver_source_plan.selected_targets[0].address,
        "0x0000000000000000000000000000000000000003"
    );
    let resolver_manifest_id = sqlx::query_scalar::<_, i64>(
        "SELECT manifest_id FROM manifest_versions WHERE source_family = 'ens_v1_resolver_l1' AND rollout_status = 'active'",
    )
    .fetch_one(database.pool())
    .await?;
    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert!(watched_contracts.iter().any(|contract| {
        contract.chain == "ethereum-mainnet"
            && contract.address == "0x0000000000000000000000000000000000000003"
            && contract.source == WatchedContractSource::DiscoveryEdge
            && contract.source_family == "ens_v1_resolver_l1"
            && contract.source_manifest_id == Some(resolver_manifest_id)
    }));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = 'ens_v1_registry_resolver:ethereum-mainnet' AND edge_kind = 'resolver' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT (after_state->>'resolver_profile_supported')::BOOLEAN FROM normalized_events WHERE event_kind = 'ResolverChanged' AND derivation_kind = 'ens_v1_registry_resolver_changed'"
        )
        .fetch_one(database.pool())
        .await?
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn storage_discovery_refresh_adds_basenames_address_without_manifest_reload_and_next_poll_backfills_code_hash()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let manifest_path = manifests.write_manifest_for_namespace_source_family(
        "basenames",
        "basenames_base_registry",
        &basenames_base_registry_manifest_contents(),
    )?;

    let initial_repository = load_manifest_repository(&manifests.path)?;
    let initial_state = build_manifest_runtime_state(database.pool(), &initial_repository).await?;
    let initial_manifest_summary = initial_state.manifest_summary.clone();
    let initial_sync_summary = initial_state.sync_summary.clone();
    let initial_discovery_admission = initial_state.discovery_admission.clone();
    let initial_manifest_event_summary = initial_state.manifest_normalized_event_summary.clone();
    let initial_tasks =
        sync_intake_chain_tasks(database.pool(), &initial_state.watched_chain_plan).await?;

    assert_eq!(initial_state.watched_chain_plan.len(), 1);
    assert_eq!(initial_tasks.len(), 1);
    assert_eq!(initial_tasks[0].addresses.len(), 1);

    let canonical_head = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbc"),
        42,
    );
    let (provider, server) = bundle_provider(vec![canonical_head.clone()]).await?;

    let (_next_task, initial_outcome) = reconcile_fetched_heads(
        database.pool(),
        &initial_tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("initial Basenames registry poll must update task state");
    assert_eq!(
        initial_outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        1
    );

    insert_raw_new_owner_log_for_parent(
        database.pool(),
        "base-mainnet",
        &canonical_head,
        "0xb94704422c2a1e396835a571837aa5ae53285a95",
        "0x0000000000000000000000000000000000000002",
        &base_eth_node(),
        "alice",
        CanonicalityState::Canonical,
    )
    .await?;

    let (refreshed_state, refreshed_tasks) =
        refresh_runtime_state_from_storage_discovery(database.pool(), &initial_state)
            .await?
            .expect(
                "stored Basenames discovery must refresh the watched plan without manifest reload",
            );

    assert_eq!(refreshed_state.manifest_summary, initial_manifest_summary);
    assert_eq!(refreshed_state.sync_summary, initial_sync_summary);
    assert_eq!(
        refreshed_state.discovery_admission,
        initial_discovery_admission
    );
    assert_eq!(
        refreshed_state.manifest_normalized_event_summary,
        initial_manifest_event_summary
    );
    assert_eq!(
        fs::read_to_string(&manifest_path)
            .with_context(|| format!("failed to read {}", manifest_path.display()))?,
        basenames_base_registry_manifest_contents()
    );
    assert_eq!(refreshed_state.watched_chain_plan.len(), 1);
    assert_eq!(refreshed_tasks.len(), 1);
    assert_eq!(refreshed_state.watched_chain_plan[0].chain, "base-mainnet");
    assert_eq!(refreshed_state.watched_chain_plan[0].addresses.len(), 2);
    assert!(
        refreshed_state.watched_chain_plan[0]
            .addresses
            .contains(&"0xb94704422c2a1e396835a571837aa5ae53285a95".to_owned())
    );
    assert!(
        refreshed_state.watched_chain_plan[0]
            .addresses
            .contains(&"0x0000000000000000000000000000000000000002".to_owned())
    );
    assert_eq!(
        refreshed_state.watched_chain_plan[0].manifest_root_entry_count,
        1
    );
    assert_eq!(
        refreshed_state.watched_chain_plan[0].manifest_contract_entry_count,
        1
    );
    assert_eq!(
        refreshed_state.watched_chain_plan[0].discovery_edge_entry_count,
        1
    );
    assert_eq!(
        refreshed_tasks[0].addresses,
        refreshed_state.watched_chain_plan[0].addresses
    );
    assert_eq!(
        refreshed_tasks[0].checkpoint.canonical_block_number,
        Some(42)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = 'ens_v1_registry_new_owner:base-mainnet' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE event_kind = 'SubregistryChanged' AND namespace = 'basenames'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'owner' FROM normalized_events WHERE event_kind = 'SubregistryChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "0x0000000000000000000000000000000000000002".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE event_kind = 'SubregistryChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );

    let unchanged = reconcile_fetched_heads(
        database.pool(),
        &refreshed_tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_head,
            safe: None,
            finalized: None,
        },
    )
    .await?;
    assert!(
        unchanged.is_none(),
        "unchanged heads should still backfill code hashes for newly watched Basenames addresses"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
            .fetch_one(database.pool())
            .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT code_byte_length FROM raw_code_hashes WHERE contract_address = '0x0000000000000000000000000000000000000002'"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

fn manifest_contents_with_root_code_hash(root_address: &str, code_hash: &str) -> String {
    let abi = test_manifest_abi_toml();
    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
exact_lookup = "shadow"

[[roots]]
name = "RootRegistry"
address = "{root_address}"
code_hash = "{code_hash}"

[[contracts]]
role = "registry"
address = "0x00000000000000000000000000000000000000aa"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
{abi}
"#
    )
}

fn ens_v1_registry_resolver_discovery_manifest_contents() -> String {
    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v1_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v1"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "ENSRegistry"
address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E"

[[contracts]]
role = "registry"
address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

[[discovery_rules]]
edge_kind = "resolver"
from_role = "registry"
admission = "reachable_from_root"
{abi}
"#
    ,
        abi = test_manifest_abi_toml()
    )
}

fn ens_v1_resolver_manifest_contents() -> String {
    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v1_resolver_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v1"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"
roots = []
contracts = []
discovery_rules = []

[capability_flags]
{abi}
"#
    ,
        abi = test_manifest_abi_toml()
    )
}

async fn insert_raw_new_resolver_log_for_node(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    emitting_address: &str,
    resolver: &str,
    node: &str,
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
            log_index: 1,
            emitting_address: emitting_address.to_ascii_lowercase(),
            topics: vec![registry_new_resolver_topic0(), node.to_owned()],
            data: decode_hex_string(&encode_registry_new_resolver_log_data(resolver)),
            canonicality_state,
        }],
    )
    .await?;

    Ok(())
}
