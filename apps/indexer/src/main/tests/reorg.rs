#[tokio::test]
async fn reconcile_fetched_heads_marks_losing_branch_orphaned_on_reorg() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(71);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for reorg reconciliation test")?;
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
    let mut tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let ancestor_block = provider_block(
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        Some("0x0000000000000000000000000000000000000000000000000000000000000000"),
        41,
    );
    let losing_block = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        42,
    );
    let new_parent_block = provider_block(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        42,
    );
    let new_head_block = provider_block(
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        43,
    );
    upsert_chain_lineage_blocks(
        database.pool(),
        &[provider_block_to_lineage(
            "ethereum-mainnet",
            &ancestor_block,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[provider_block_to_lineage(
            "ethereum-mainnet",
            &losing_block,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[provider_block_to_lineage(
            "ethereum-mainnet",
            &new_parent_block,
            CanonicalityState::Orphaned,
        )],
    )
    .await?;
    upsert_raw_blocks(
        database.pool(),
        &[
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &ancestor_block,
                CanonicalityState::Canonical,
            ),
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &losing_block,
                CanonicalityState::Canonical,
            ),
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &new_parent_block,
                CanonicalityState::Orphaned,
            ),
        ],
    )
    .await?;
    upsert_raw_transactions(
        database.pool(),
        &[RawTransaction {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            transaction_hash: transaction_hash_for_block(&losing_block),
            transaction_index: 0,
            from_address: "0x0000000000000000000000000000000000000001".to_owned(),
            to_address: Some("0x0000000000000000000000000000000000000002".to_owned()),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_code_hashes(
        database.pool(),
        &[RawCodeHash {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            contract_address: "0x0000000000000000000000000000000000000001".to_owned(),
            code_hash: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
                .to_owned(),
            code_byte_length: 32,
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_receipts(
        database.pool(),
        &[RawReceipt {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            transaction_hash: transaction_hash_for_block(&losing_block),
            transaction_index: 0,
            contract_address: None,
            status: Some(true),
            gas_used: Some(21_000),
            cumulative_gas_used: Some(21_000),
            logs_bloom: losing_block.logs_bloom.clone(),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[RawLog {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            transaction_hash: transaction_hash_for_block(&losing_block),
            transaction_index: 0,
            log_index: 0,
            emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
            topics: vec![
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ],
            data: vec![0xde, 0xad, 0xbe, 0xef],
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
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
                'raw_log_preimage_observed:1:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:0:0x00000000000000000000000000000000000000aa',
                'ens',
                'PreimageObserved',
                'ens_test',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $1,
                0,
                '{"kind":"raw_log"}'::jsonb,
                'raw_log_preimage_observation',
                'canonical'::canonicality_state,
                '{}'::jsonb,
                '{"decoded_name":"wrapped.eth"}'::jsonb
            )
            "#,
        )
        .bind(transaction_hash_for_block(&losing_block))
        .execute(database.pool())
        .await
        .context("failed to insert normalized event for reorg reconciliation test")?;
    let losing_timestamp =
        OffsetDateTime::from_unix_timestamp(losing_block.block_timestamp_unix_secs)
            .expect("losing block timestamp must be valid");
    let token_lineage_id = Uuid::from_u128(0x7100);
    let resource_id = Uuid::from_u128(0x7200);
    let surface_binding_id = Uuid::from_u128(0x7300);
    upsert_token_lineages(
        database.pool(),
        &[TokenLineage {
            token_lineage_id,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[NameSurface {
            logical_name_id: "ens:reorg.eth".to_owned(),
            namespace: "ens".to_owned(),
            input_name: "reorg.eth".to_owned(),
            canonical_display_name: "reorg.eth".to_owned(),
            normalized_name: "reorg.eth".to_owned(),
            dns_encoded_name: vec![5, b'r', b'e', b'o', b'r', b'g', 3, b'e', b't', b'h', 0],
            namehash: "0xnamehashreorg".to_owned(),
            labelhashes: vec!["0xlabelhashreorg".to_owned()],
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[SurfaceBinding {
            surface_binding_id,
            logical_name_id: "ens:reorg.eth".to_owned(),
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: losing_timestamp,
            active_to: None,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    sqlx::query(
        r#"
            INSERT INTO normalized_events (
                event_identity,
                namespace,
                logical_name_id,
                resource_id,
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
                'ens_v1_unwrapped_authority:ResolverChanged:resolver:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:1',
                'ens',
                'ens:reorg.eth',
                $1,
                'ResolverChanged',
                'ens_v1_registry_l1',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $2,
                1,
                '{"kind":"raw_log"}'::jsonb,
                'ens_v1_unwrapped_authority',
                'canonical'::canonicality_state,
                '{"resolver":"0x00000000000000000000000000000000000000aa"}'::jsonb,
                '{"resolver":"0x00000000000000000000000000000000000000bb"}'::jsonb
            )
            "#,
    )
    .bind(resource_id)
    .bind(transaction_hash_for_block(&losing_block))
    .execute(database.pool())
    .await
    .context("failed to insert ResolverChanged event for reorg reconciliation test")?;
    sqlx::query(
        r#"
            INSERT INTO normalized_events (
                event_identity,
                namespace,
                logical_name_id,
                resource_id,
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
            VALUES
            (
                'ens_v1_unwrapped_authority:RecordChanged:record-change:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:2',
                'ens',
                'ens:reorg.eth',
                $1,
                'RecordChanged',
                'ens_v1_resolver_l1',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $2,
                2,
                '{"kind":"raw_log"}'::jsonb,
                'ens_v1_unwrapped_authority',
                'canonical'::canonicality_state,
                '{}'::jsonb,
                '{"record_key":"text","record_family":"text","selector_key":null}'::jsonb
            ),
            (
                'ens_v1_unwrapped_authority:RecordChanged:record-change:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:3',
                'ens',
                'ens:reorg.eth',
                $1,
                'RecordChanged',
                'ens_v1_resolver_l1',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $2,
                3,
                '{"kind":"raw_log"}'::jsonb,
                'ens_v1_unwrapped_authority',
                'canonical'::canonicality_state,
                '{}'::jsonb,
                '{"record_key":"addr:60","record_family":"addr","selector_key":"60"}'::jsonb
            ),
            (
                'ens_v1_unwrapped_authority:RecordVersionChanged:record-version:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:4',
                'ens',
                'ens:reorg.eth',
                $1,
                'RecordVersionChanged',
                'ens_v1_resolver_l1',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $2,
                4,
                '{"kind":"raw_log"}'::jsonb,
                'ens_v1_unwrapped_authority',
                'canonical'::canonicality_state,
                '{"record_version":null}'::jsonb,
                '{"record_version":7}'::jsonb
            )
            "#,
    )
    .bind(resource_id)
    .bind(transaction_hash_for_block(&losing_block))
    .execute(database.pool())
    .await
    .context("failed to insert record change events for reorg reconciliation test")?;

    let losing_resolution_trace = execution_trace_fixture(
        Uuid::from_u128(0x9001),
        "verified_resolution",
        "ens:reorg.eth:addr:60",
        &losing_block,
    );
    let losing_resolution_outcome = execution_outcome_fixture(
        &losing_resolution_trace,
        &losing_block,
        &ancestor_block,
        &ancestor_block,
        Uuid::from_u128(0x9101),
    );
    insert_execution_fixture(
        database.pool(),
        &losing_resolution_trace,
        &losing_resolution_outcome,
    )
    .await?;

    let losing_primary_trace = execution_trace_fixture(
        Uuid::from_u128(0x9002),
        "verified_primary_name",
        "ens:0x0000000000000000000000000000000000000001:60",
        &losing_block,
    );
    let losing_primary_outcome = execution_outcome_fixture(
        &losing_primary_trace,
        &ancestor_block,
        &ancestor_block,
        &losing_block,
        Uuid::from_u128(0x9103),
    );
    insert_execution_fixture(
        database.pool(),
        &losing_primary_trace,
        &losing_primary_outcome,
    )
    .await?;

    let unrelated_resolution_trace = execution_trace_fixture(
        Uuid::from_u128(0x9003),
        "verified_resolution",
        "ens:keep.eth:addr:60",
        &ancestor_block,
    );
    let unrelated_resolution_outcome = execution_outcome_fixture(
        &unrelated_resolution_trace,
        &ancestor_block,
        &ancestor_block,
        &ancestor_block,
        Uuid::from_u128(0x9105),
    );
    insert_execution_fixture(
        database.pool(),
        &unrelated_resolution_trace,
        &unrelated_resolution_outcome,
    )
    .await?;

    let out_of_scope_trace = execution_trace_fixture(
        Uuid::from_u128(0x9004),
        "declared_resolution",
        "ens:declared.eth:addr:60",
        &losing_block,
    );
    let out_of_scope_outcome = execution_outcome_fixture(
        &out_of_scope_trace,
        &losing_block,
        &losing_block,
        &losing_block,
        Uuid::from_u128(0x9107),
    );
    insert_execution_fixture(database.pool(), &out_of_scope_trace, &out_of_scope_outcome).await?;

    let execution_trace_count_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_traces")
            .fetch_one(database.pool())
            .await
            .context("failed to count execution traces before reorg")?;
    let execution_step_count_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_steps")
            .fetch_one(database.pool())
            .await
            .context("failed to count execution steps before reorg")?;
    let execution_outcome_count_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_cache_outcomes")
            .fetch_one(database.pool())
            .await
            .context("failed to count execution cache outcomes before reorg")?;
    assert_eq!(execution_trace_count_before, 4);
    assert_eq!(execution_step_count_before, 4);
    assert_eq!(execution_outcome_count_before, 4);

    tasks[0].checkpoint = advance_chain_checkpoints(
        database.pool(),
        &ChainCheckpointUpdate {
            chain_id: "ethereum-mainnet".to_owned(),
            canonical: Some(CheckpointBlockRef {
                block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                block_number: 42,
            }),
            safe: None,
            finalized: None,
        },
    )
    .await?;
    let (provider, server) =
        bundle_provider(vec![new_parent_block.clone(), new_head_block.clone()]).await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: new_head_block,
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("reorg reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::ReorgReconciled
    );
    assert_eq!(outcome.orphaned_block_count, 1);
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(43));
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0x1111111111111111111111111111111111111111111111111111111111111111'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_receipts WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_logs WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordChanged' AND canonicality_state = 'orphaned'::canonicality_state"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordVersionChanged' AND canonicality_state = 'orphaned'::canonicality_state"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND event_kind IN ('RecordChanged', 'RecordVersionChanged') AND canonicality_state = 'canonical'::canonicality_state"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM token_lineages WHERE token_lineage_id = $1"
        )
        .bind(token_lineage_id)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM resources WHERE resource_id = $1"
        )
        .bind(resource_id)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM name_surfaces WHERE logical_name_id = 'ens:reorg.eth'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM surface_bindings WHERE surface_binding_id = $1"
        )
        .bind(surface_binding_id)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
        load_execution_outcome(database.pool(), &losing_resolution_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &losing_primary_outcome.cache_key).await?,
        None
    );
    assert!(
        load_execution_outcome(database.pool(), &unrelated_resolution_outcome.cache_key)
            .await?
            .is_some(),
        "unrelated verified resolution outcome must remain reusable"
    );
    assert!(
        load_execution_outcome(database.pool(), &out_of_scope_outcome.cache_key)
            .await?
            .is_some(),
        "out-of-scope execution outcome must remain reusable"
    );
    for trace_id in [
        losing_resolution_trace.execution_trace_id,
        losing_primary_trace.execution_trace_id,
        unrelated_resolution_trace.execution_trace_id,
        out_of_scope_trace.execution_trace_id,
    ] {
        assert!(
            load_execution_trace(database.pool(), trace_id)
                .await?
                .is_some(),
            "execution trace {trace_id} must remain durable after reorg invalidation"
        );
    }
    let execution_trace_count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_traces")
            .fetch_one(database.pool())
            .await
            .context("failed to count execution traces after reorg")?;
    let execution_step_count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_steps")
            .fetch_one(database.pool())
            .await
            .context("failed to count execution steps after reorg")?;
    let execution_outcome_count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_cache_outcomes")
            .fetch_one(database.pool())
            .await
            .context("failed to count execution cache outcomes after reorg")?;
    assert_eq!(execution_trace_count_after, execution_trace_count_before);
    assert_eq!(execution_step_count_after, execution_step_count_before);
    assert_eq!(
        execution_outcome_count_after,
        execution_outcome_count_before - 2
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reorg_common_ancestor_must_be_on_current_canonical_branch() -> Result<()> {
    let database = TestDatabase::new().await?;
    let ancestor = provider_block(
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        None,
        1,
    );
    let old_two = provider_block(
        "0x2222222222222222222222222222222222222222222222222222222222222222",
        Some(&ancestor.block_hash),
        2,
    );
    let old_head = provider_block(
        "0x3333333333333333333333333333333333333333333333333333333333333333",
        Some(&old_two.block_hash),
        3,
    );
    let stray_two = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some(&ancestor.block_hash),
        2,
    );
    let new_head = provider_block(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some(&stray_two.block_hash),
        3,
    );

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            provider_block_to_lineage("ethereum-mainnet", &ancestor, CanonicalityState::Canonical),
            provider_block_to_lineage("ethereum-mainnet", &old_two, CanonicalityState::Canonical),
            provider_block_to_lineage("ethereum-mainnet", &old_head, CanonicalityState::Canonical),
            provider_block_to_lineage("ethereum-mainnet", &stray_two, CanonicalityState::Observed),
        ],
    )
    .await?;

    let (provider, server) = bundle_provider(vec![stray_two.clone(), new_head.clone()]).await?;
    let reconciliation = reconcile_canonical_head(
        database.pool(),
        &provider,
        "ethereum-mainnet",
        &ChainCheckpoint {
            chain_id: "ethereum-mainnet".to_owned(),
            canonical_block_hash: Some(old_head.block_hash.clone()),
            canonical_block_number: Some(old_head.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
        &new_head,
        HeaderAuditMode::Minimal,
    )
    .await?;

    assert_eq!(
        reconciliation.status,
        CanonicalReconciliationStatus::ReorgReconciled
    );
    assert_eq!(reconciliation.orphaned_block_count, 2);
    assert_eq!(
        reconciliation.raw_orphan_stop_before_hash.as_deref(),
        Some(ancestor.block_hash.as_str())
    );

    let states = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT block_hash, canonicality_state::TEXT
        FROM chain_lineage
        WHERE chain_id = 'ethereum-mainnet'
        ORDER BY block_number, block_hash
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        states,
        vec![
            (ancestor.block_hash, "canonical".to_owned()),
            (old_two.block_hash, "orphaned".to_owned()),
            (stray_two.block_hash, "canonical".to_owned()),
            (old_head.block_hash, "orphaned".to_owned()),
            (new_head.block_hash, "canonical".to_owned()),
        ]
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn reorg_reconcile_fetched_heads_orphans_losing_branch_rows_when_raw_block_is_missing()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(72);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for missing-raw-block reorg test")?;
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
    let mut tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let ancestor_block = provider_block(
        "0x2111111111111111111111111111111111111111111111111111111111111111",
        Some("0x0000000000000000000000000000000000000000000000000000000000000000"),
        41,
    );
    let losing_block = provider_block(
        "0x2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0x2111111111111111111111111111111111111111111111111111111111111111"),
        42,
    );
    let new_parent_block = provider_block(
        "0x2bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some("0x2111111111111111111111111111111111111111111111111111111111111111"),
        42,
    );
    let new_head_block = provider_block(
        "0x2ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        Some("0x2bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        43,
    );
    upsert_chain_lineage_blocks(
        database.pool(),
        &[provider_block_to_lineage(
            "ethereum-mainnet",
            &ancestor_block,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[provider_block_to_lineage(
            "ethereum-mainnet",
            &losing_block,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[provider_block_to_lineage(
            "ethereum-mainnet",
            &new_parent_block,
            CanonicalityState::Orphaned,
        )],
    )
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage WHERE block_hash = $1")
            .bind(&losing_block.block_hash)
            .fetch_one(database.pool())
            .await?,
        1
    );
    upsert_raw_transactions(
        database.pool(),
        &[RawTransaction {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            transaction_hash: transaction_hash_for_block(&losing_block),
            transaction_index: 0,
            from_address: "0x0000000000000000000000000000000000000001".to_owned(),
            to_address: Some("0x0000000000000000000000000000000000000002".to_owned()),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_code_hashes(
        database.pool(),
        &[RawCodeHash {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            contract_address: "0x0000000000000000000000000000000000000001".to_owned(),
            code_hash: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
                .to_owned(),
            code_byte_length: 32,
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_receipts(
        database.pool(),
        &[RawReceipt {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            transaction_hash: transaction_hash_for_block(&losing_block),
            transaction_index: 0,
            contract_address: None,
            status: Some(true),
            gas_used: Some(21_000),
            cumulative_gas_used: Some(21_000),
            logs_bloom: losing_block.logs_bloom.clone(),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[RawLog {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            transaction_hash: transaction_hash_for_block(&losing_block),
            transaction_index: 0,
            log_index: 0,
            emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
            topics: vec![
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ],
            data: vec![0xde, 0xad, 0xbe, 0xef],
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
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
                'raw_log_preimage_observed:1:0x2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:0:0x00000000000000000000000000000000000000aa',
                'ens',
                'PreimageObserved',
                'ens_test',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0x2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $1,
                0,
                '{"kind":"raw_log"}'::jsonb,
                'raw_log_preimage_observation',
                'canonical'::canonicality_state,
                '{}'::jsonb,
                '{"decoded_name":"missing-raw-block.eth"}'::jsonb
            )
            "#,
    )
    .bind(transaction_hash_for_block(&losing_block))
    .execute(database.pool())
    .await
    .context("failed to insert normalized event for missing-raw-block reorg test")?;
    let losing_timestamp =
        OffsetDateTime::from_unix_timestamp(losing_block.block_timestamp_unix_secs)
            .expect("losing block timestamp must be valid");
    let token_lineage_id = Uuid::from_u128(0x7200);
    let resource_id = Uuid::from_u128(0x7300);
    let surface_binding_id = Uuid::from_u128(0x7400);
    upsert_token_lineages(
        database.pool(),
        &[TokenLineage {
            token_lineage_id,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "missing_raw_block_reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "missing_raw_block_reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[NameSurface {
            logical_name_id: "ens:missing-raw-block.eth".to_owned(),
            namespace: "ens".to_owned(),
            input_name: "missing-raw-block.eth".to_owned(),
            canonical_display_name: "missing-raw-block.eth".to_owned(),
            normalized_name: "missing-raw-block.eth".to_owned(),
            dns_encoded_name: vec![
                17, b'm', b'i', b's', b's', b'i', b'n', b'g', b'-', b'r', b'a', b'w', b'-', b'b',
                b'l', b'o', b'c', b'k', 3, b'e', b't', b'h', 0,
            ],
            namehash: "0xmissingrawblocknamehash".to_owned(),
            labelhashes: vec!["0xmissingrawblocklabelhash".to_owned()],
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "missing_raw_block_reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[SurfaceBinding {
            surface_binding_id,
            logical_name_id: "ens:missing-raw-block.eth".to_owned(),
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: losing_timestamp,
            active_to: None,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "missing_raw_block_reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;

    tasks[0].checkpoint = advance_chain_checkpoints(
        database.pool(),
        &ChainCheckpointUpdate {
            chain_id: "ethereum-mainnet".to_owned(),
            canonical: Some(CheckpointBlockRef {
                block_hash: losing_block.block_hash.clone(),
                block_number: losing_block.block_number,
            }),
            safe: None,
            finalized: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider_with_fixtures(vec![
        ProviderBlockFixture {
            block: new_parent_block.clone(),
            logs: Vec::new(),
        },
        ProviderBlockFixture {
            block: new_head_block.clone(),
            logs: Vec::new(),
        },
    ])
    .await?;

    let (_, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: new_head_block,
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("reorg reconciliation must update task state when chain_lineage rows are missing");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::ReorgReconciled
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_receipts WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_logs WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM token_lineages WHERE token_lineage_id = $1"
        )
        .bind(token_lineage_id)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM resources WHERE resource_id = $1"
        )
        .bind(resource_id)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM name_surfaces WHERE logical_name_id = 'ens:missing-raw-block.eth'"
        )
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM surface_bindings WHERE surface_binding_id = $1"
        )
        .bind(surface_binding_id)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

async fn insert_execution_fixture(
    pool: &PgPool,
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<()> {
    upsert_execution_trace(pool, trace).await?;
    upsert_execution_outcome(pool, outcome).await?;
    Ok(())
}

fn execution_trace_fixture(
    execution_trace_id: Uuid,
    request_type: &str,
    request_key: &str,
    block: &ProviderBlock,
) -> ExecutionTrace {
    ExecutionTrace {
        execution_trace_id,
        request_type: request_type.to_owned(),
        request_key: request_key.to_owned(),
        namespace: "ens".to_owned(),
        chain_context: json!({
            "requested_positions": [requested_chain_position(block)]
        }),
        manifest_context: json!({
            "manifest_versions": [{
                "source_family": "ens_execution",
                "manifest_version": 1
            }]
        }),
        contracts_called: json!([{
            "chain_id": "ethereum-mainnet",
            "contract_address": "0x0000000000000000000000000000000000000001",
            "selector": "0x3b3b57de"
        }]),
        gateway_digests: json!([]),
        final_payload: Some(json!({
            "status": "success",
            "request_key": request_key
        })),
        failure_payload: None,
        request_metadata: json!({
            "test": "reorg_execution_cache_invalidation"
        }),
        finished_at: Some(test_execution_timestamp(block)),
        steps: vec![ExecutionTraceStep {
            step_index: 0,
            step_kind: "execute_verified_read".to_owned(),
            input_digest: Some(format!("sha256:input-{}", execution_trace_id.simple())),
            output_digest: Some(format!("sha256:output-{}", execution_trace_id.simple())),
            latency_ms: Some(1),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_number": block.block_number,
                    "block_hash": block.block_hash
                }
            }),
            step_payload: json!({
                "request_type": request_type,
                "request_key": request_key
            }),
        }],
    }
}

fn execution_outcome_fixture(
    trace: &ExecutionTrace,
    requested_block: &ProviderBlock,
    topology_block: &ProviderBlock,
    record_block: &ProviderBlock,
    boundary_seed: Uuid,
) -> ExecutionOutcome {
    ExecutionOutcome {
        cache_key: ExecutionCacheKey {
            request_key: trace.request_key.clone(),
            requested_chain_positions: json!([requested_chain_position(requested_block)]),
            manifest_versions: json!([{
                "source_family": "ens_execution",
                "manifest_version": 1
            }]),
            topology_version_boundary: version_boundary_fixture(
                "ens:reorg.eth",
                boundary_seed,
                Some(9_101),
                Some("ResolverChanged"),
                topology_block,
            ),
            record_version_boundary: version_boundary_fixture(
                "ens:reorg.eth",
                Uuid::from_u128(boundary_seed.as_u128() + 1),
                Some(9_102),
                Some("RecordChanged"),
                record_block,
            ),
        },
        execution_trace_id: trace.execution_trace_id,
        request_type: trace.request_type.clone(),
        namespace: trace.namespace.clone(),
        outcome_payload: Some(json!({
            "status": "success",
            "request_key": trace.request_key
        })),
        failure_payload: None,
        finished_at: trace
            .finished_at
            .expect("execution trace fixture must be finished"),
    }
}

fn requested_chain_position(block: &ProviderBlock) -> serde_json::Value {
    json!({
        "chain_id": "ethereum-mainnet",
        "block_number": block.block_number,
        "block_hash": block.block_hash
    })
}

fn version_boundary_fixture(
    logical_name_id: &str,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
    block: &ProviderBlock,
) -> serde_json::Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": block.block_number,
            "block_hash": block.block_hash,
            "timestamp": "2024-06-07T00:00:00Z"
        }
    })
}

fn test_execution_timestamp(block: &ProviderBlock) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(block.block_timestamp_unix_secs)
        .expect("test block timestamp must be valid")
}
