#[tokio::test]
async fn repair_ens_v1_text_records_fetches_provider_logs_without_raw_log_staging() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let resolver_address = "0x0000000000000000000000000000000000004976";
    let block = provider_block(
        "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
        Some("0x8181818181818181818181818181818181818181818181818181818181818181"),
        777,
    );
    let node = namehash_for_dns_name(&dns_encoded_eth_name("alice"));

    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_generic_text_record_event(
        database.pool(),
        chain,
        &block,
        resolver_address,
        &node,
        "ens:alice.eth",
    )
    .await?;

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: block.clone(),
        logs: vec![rpc_resolver_text_changed_with_value_log_payload_for_namehash(
            &block,
            resolver_address,
            &node,
            "avatar",
            "https://euc.li/alice.eth",
            3,
        )],
    }])
    .await?;

    let outcome = repair_ens_v1_text_records_from_provider(
        database.pool(),
        &provider,
        EnsV1TextRecordRepairConfig {
            chain: chain.to_owned(),
            from_block: Some(block.block_number),
            to_block: Some(block.block_number),
            chunk_blocks: 1,
            candidate_page_size: 10,
        },
    )
    .await?;

    assert_eq!(outcome.candidate_count, 1);
    assert_eq!(outcome.fetched_log_count, 1);
    assert_eq!(outcome.matched_log_count, 1);
    assert_eq!(outcome.repaired_event_count, 1);
    assert_eq!(outcome.missing_log_count, 0);
    assert_eq!(outcome.skipped_decode_count, 0);

    let after_state = sqlx::query_scalar::<_, Value>(
        "SELECT after_state FROM normalized_events WHERE event_identity = $1",
    )
    .bind(format!(
        "ens_v1_unwrapped_authority:RecordChanged:record-change:{}:{}:3",
        block.block_hash,
        transaction_hash_for_block(&block)
    ))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(after_state["record_key"], "text:avatar");
    assert_eq!(after_state["record_family"], "text");
    assert_eq!(after_state["selector_key"], "avatar");
    assert_eq!(after_state["value"], "https://euc.li/alice.eth");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        0
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn repair_ens_v1_text_records_escapes_nul_text_values_for_jsonb() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let resolver_address = "0x0000000000000000000000000000000000004976";
    let block = provider_block(
        "0xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2",
        Some("0x9292929292929292929292929292929292929292929292929292929292929292"),
        778,
    );
    let node = namehash_for_dns_name(&dns_encoded_eth_name("nul"));

    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_generic_text_record_event(
        database.pool(),
        chain,
        &block,
        resolver_address,
        &node,
        "ens:nul.eth",
    )
    .await?;

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: block.clone(),
        logs: vec![rpc_resolver_text_changed_with_value_log_payload_for_namehash(
            &block,
            resolver_address,
            &node,
            "avatar",
            "ipfs://avatar\0tail",
            3,
        )],
    }])
    .await?;

    let outcome = repair_ens_v1_text_records_from_provider(
        database.pool(),
        &provider,
        EnsV1TextRecordRepairConfig {
            chain: chain.to_owned(),
            from_block: Some(block.block_number),
            to_block: Some(block.block_number),
            chunk_blocks: 1,
            candidate_page_size: 10,
        },
    )
    .await?;

    assert_eq!(outcome.candidate_count, 1);
    assert_eq!(outcome.repaired_event_count, 1);

    let after_state = sqlx::query_scalar::<_, Value>(
        "SELECT after_state FROM normalized_events WHERE event_identity = $1",
    )
    .bind(format!(
        "ens_v1_unwrapped_authority:RecordChanged:record-change:{}:{}:3",
        block.block_hash,
        transaction_hash_for_block(&block)
    ))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(after_state["record_key"], "text:avatar");
    assert_eq!(after_state["selector_key"], "avatar");
    assert_eq!(after_state["value"], "ipfs://avatar\\u0000tail");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        0
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn repair_ens_v1_text_records_fills_selectorized_rows_missing_value() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let resolver_address = "0x0000000000000000000000000000000000004976";
    let block = provider_block(
        "0xc3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3",
        Some("0xa3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3"),
        779,
    );
    let node = namehash_for_dns_name(&dns_encoded_eth_name("selector"));

    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_selectorized_text_record_event_without_value(
        database.pool(),
        chain,
        &block,
        resolver_address,
        &node,
        "ens:selector.eth",
        "avatar",
    )
    .await?;

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: block.clone(),
        logs: vec![rpc_resolver_text_changed_with_value_log_payload_for_namehash(
            &block,
            resolver_address,
            &node,
            "avatar",
            "https://euc.li/selector.eth",
            3,
        )],
    }])
    .await?;

    let outcome = repair_ens_v1_text_records_from_provider(
        database.pool(),
        &provider,
        EnsV1TextRecordRepairConfig {
            chain: chain.to_owned(),
            from_block: Some(block.block_number),
            to_block: Some(block.block_number),
            chunk_blocks: 1,
            candidate_page_size: 10,
        },
    )
    .await?;

    assert_eq!(outcome.candidate_count, 1);
    assert_eq!(outcome.repaired_event_count, 1);
    assert_eq!(outcome.missing_log_count, 0);
    assert_eq!(outcome.skipped_decode_count, 0);

    let after_state = sqlx::query_scalar::<_, Value>(
        "SELECT after_state FROM normalized_events WHERE event_identity = $1",
    )
    .bind(format!(
        "ens_v1_unwrapped_authority:RecordChanged:record-change:{}:{}:3",
        block.block_hash,
        transaction_hash_for_block(&block)
    ))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(after_state["record_key"], "text:avatar");
    assert_eq!(after_state["selector_key"], "avatar");
    assert_eq!(after_state["value"], "https://euc.li/selector.eth");

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn repair_name_surface_normalization_updates_compatible_and_records_findings() -> Result<()> {
    let database = TestDatabase::new().await?;
    let expected = bigname_domain::normalization::ENS_NORMALIZER_VERSION;
    let old_version = "ensip15@2026-04-16";

    upsert_name_surfaces(
        database.pool(),
        &[
            normalization_repair_surface("Alice.eth", "alice.eth", old_version),
            normalization_repair_surface("bad name.eth", "bad name.eth", old_version),
            normalization_repair_surface("🅰️🅱.eth", "🅰️🅱.eth", old_version),
        ],
    )
    .await?;
    insert_raw_normalization_repair_surface(database.pool(), "", "empty-input.eth", old_version)
        .await?;
    let retained_observed_at =
        OffsetDateTime::from_unix_timestamp(1_717_000_000).context("valid timestamp")?;
    sqlx::query("UPDATE name_surfaces SET observed_at = $1 WHERE logical_name_id = $2")
        .bind(retained_observed_at)
        .bind("ens:alice.eth")
        .execute(database.pool())
        .await
        .context("failed to anchor compatible name-surface observed_at")?;

    let outcome = repair_name_surface_normalization(
        database.pool(),
        NameSurfaceNormalizationRepairConfig {
            expected_normalizer_version: expected.to_owned(),
            page_size: 2,
            limit: None,
            apply_compatible: true,
            record_findings: true,
        },
    )
    .await?;

    assert_eq!(outcome.scanned_count, 4);
    assert_eq!(outcome.compatible_count, 1);
    assert_eq!(outcome.updated_compatible_count, 1);
    assert_eq!(outcome.rejected_count, 2);
    assert_eq!(outcome.incompatible_count, 1);
    assert_eq!(outcome.recorded_finding_count, 3);
    assert_eq!(outcome.remaining_old_normalizer_count, 3);

    let (normalizer_version, canonical_display_name, observed_at) =
        sqlx::query_as::<_, (String, String, OffsetDateTime)>(
        r#"
        SELECT normalizer_version, canonical_display_name, observed_at
        FROM name_surfaces
        WHERE logical_name_id = 'ens:alice.eth'
        "#,
        )
        .fetch_one(database.pool())
        .await?;
    assert_eq!(normalizer_version, expected);
    assert_eq!(canonical_display_name, "alice.eth");
    assert_eq!(observed_at, retained_observed_at);

    let findings = sqlx::query_as::<_, (String, String, Option<String>)>(
        r#"
        SELECT logical_name_id, finding_kind, candidate_logical_name_id
        FROM name_surface_normalization_repair_findings
        ORDER BY logical_name_id
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        findings,
        vec![
            ("ens:bad name.eth".to_owned(), "rejected".to_owned(), None),
            (
                "ens:empty-input.eth".to_owned(),
                "rejected".to_owned(),
                None
            ),
            (
                "ens:🅰️🅱.eth".to_owned(),
                "incompatible".to_owned(),
                Some("ens:🅰🅱.eth".to_owned())
            ),
        ]
    );

    database.cleanup().await
}

async fn insert_generic_text_record_event(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    resolver_address: &str,
    node: &str,
    logical_name_id: &str,
) -> Result<()> {
    insert_text_record_event_without_value(
        pool,
        chain,
        block,
        resolver_address,
        node,
        logical_name_id,
        "text",
        None,
    )
    .await
}

async fn insert_raw_normalization_repair_surface(
    pool: &PgPool,
    input_name: &str,
    normalized_name: &str,
    normalizer_version: &str,
) -> Result<()> {
    let labels = normalized_name.split('.').collect::<Vec<_>>();
    let dns_encoded_name = dns_encoded_name(&labels);
    sqlx::query(
        r#"
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            normalization_warnings,
            normalization_errors,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES (
            $1,
            'ens',
            $2,
            $3,
            $3,
            $4,
            $5,
            $6,
            $7,
            '[]'::jsonb,
            '[]'::jsonb,
            'ethereum-mainnet',
            '0x2222222222222222222222222222222222222222222222222222222222222222',
            2,
            '{"source":"name_surface_normalization_repair_raw_test"}'::jsonb,
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(format!("ens:{normalized_name}"))
    .bind(input_name)
    .bind(normalized_name)
    .bind(&dns_encoded_name)
    .bind(namehash_for_dns_name(&dns_encoded_name))
    .bind(
        labels
            .iter()
            .map(|label| keccak256_hex(label.as_bytes()))
            .collect::<Vec<_>>(),
    )
    .bind(normalizer_version)
    .execute(pool)
    .await
    .context("failed to insert raw name-surface normalization repair row")?;
    Ok(())
}

fn normalization_repair_surface(
    input_name: &str,
    normalized_name: &str,
    normalizer_version: &str,
) -> NameSurface {
    let labels = normalized_name.split('.').collect::<Vec<_>>();
    let dns_encoded_name = dns_encoded_name(&labels);
    NameSurface {
        logical_name_id: format!("ens:{normalized_name}"),
        namespace: "ens".to_owned(),
        input_name: input_name.to_owned(),
        canonical_display_name: normalized_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        dns_encoded_name: dns_encoded_name.clone(),
        namehash: namehash_for_dns_name(&dns_encoded_name),
        labelhashes: labels
            .iter()
            .map(|label| keccak256_hex(label.as_bytes()))
            .collect(),
        normalizer_version: normalizer_version.to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0x1111111111111111111111111111111111111111111111111111111111111111"
            .to_owned(),
        block_number: 1,
        provenance: json!({"source": "name_surface_normalization_repair_test"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

async fn insert_selectorized_text_record_event_without_value(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    resolver_address: &str,
    node: &str,
    logical_name_id: &str,
    selector_key: &str,
) -> Result<()> {
    insert_text_record_event_without_value(
        pool,
        chain,
        block,
        resolver_address,
        node,
        logical_name_id,
        &format!("text:{selector_key}"),
        Some(selector_key),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_text_record_event_without_value(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    resolver_address: &str,
    node: &str,
    logical_name_id: &str,
    record_key: &str,
    selector_key: Option<&str>,
) -> Result<()> {
    let transaction_hash = transaction_hash_for_block(block);
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
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
            $2,
            'RecordChanged',
            'ens_v1_resolver_l1',
            1,
            1,
            $3,
            $4,
            $5,
            $6,
            3,
            $7::jsonb,
            'ens_v1_unwrapped_authority',
            'canonical',
            '{}'::jsonb,
            $8::jsonb
        )
        "#,
    )
    .bind(format!(
        "ens_v1_unwrapped_authority:RecordChanged:record-change:{}:{}:3",
        block.block_hash, transaction_hash
    ))
    .bind(logical_name_id)
    .bind(chain)
    .bind(block.block_number)
    .bind(&block.block_hash)
    .bind(&transaction_hash)
    .bind(
        json!({
            "kind": "raw_log",
            "chain_id": chain,
            "block_hash": block.block_hash,
            "block_number": block.block_number,
            "transaction_hash": transaction_hash,
            "transaction_index": 0,
            "log_index": 3,
        })
        .to_string(),
    )
    .bind(
        json!({
            "record_key": record_key,
            "record_family": "text",
            "selector_key": selector_key,
            "resolver": resolver_address,
            "node": node,
        })
        .to_string(),
    )
    .execute(pool)
    .await
    .context("failed to insert generic text record event for repair test")?;

    Ok(())
}

fn rpc_resolver_text_changed_with_value_log_payload_for_namehash(
    block: &ProviderBlock,
    address: &str,
    namehash: &str,
    key: &str,
    value: &str,
    log_index: u64,
) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            resolver_text_changed_with_value_topic0(),
            namehash,
            keccak256_hex(key.as_bytes())
        ],
        "data": encode_two_dynamic_string_log_data(key, value)
    })
}
