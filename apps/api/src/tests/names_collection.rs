#[tokio::test]
async fn names_collection_returns_compact_projection_rows_with_counts_and_stable_cursor()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let resolver = "0x0000000000000000000000000000000000000def";
    let alice_resource_id = Uuid::from_u128(0xa110);
    let alice_token_lineage_id = Uuid::from_u128(0xa111);
    let alice_surface_binding_id = Uuid::from_u128(0xa112);
    let alicia_resource_id = Uuid::from_u128(0xa120);
    let alicia_token_lineage_id = Uuid::from_u128(0xa121);
    let alicia_surface_binding_id = Uuid::from_u128(0xa122);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block(
                "ethereum-mainnet",
                "0xnames-alice",
                None,
                411,
                1_717_174_011,
            ),
            raw_block(
                "ethereum-mainnet",
                "0xnames-alicia",
                None,
                412,
                1_717_174_012,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[
            address_name_token_lineage(alice_token_lineage_id, "0xnames-alice", 411),
            address_name_token_lineage(alicia_token_lineage_id, "0xnames-alicia", 412),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(
                alice_resource_id,
                Some(alice_token_lineage_id),
                "0xnames-alice",
                411,
            ),
            address_name_resource(
                alicia_resource_id,
                Some(alicia_token_lineage_id),
                "0xnames-alicia",
                412,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:alice.eth", "alice.eth", "namehash:alice.eth", 411),
            collection_name_surface("ens:alicia.eth", "alicia.eth", "namehash:alicia.eth", 412),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(
                alice_surface_binding_id,
                "ens:alice.eth",
                alice_resource_id,
                "0xnames-alice",
                411,
                1_717_174_011,
            ),
            address_name_surface_binding(
                alicia_surface_binding_id,
                "ens:alicia.eth",
                alicia_resource_id,
                "0xnames-alicia",
                412,
                1_717_174_012,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                address,
                "ens:alice.eth",
                bigname_storage::AddressNameRelation::TokenHolder,
                "Alice.eth",
                "alice.eth",
                "namehash:alice.eth",
                alice_surface_binding_id,
                alice_resource_id,
                Some(alice_token_lineage_id),
                411,
            ),
            address_name_current_row(
                address,
                "ens:alicia.eth",
                bigname_storage::AddressNameRelation::EffectiveController,
                "Alicia.eth",
                "alicia.eth",
                "namehash:alicia.eth",
                alicia_surface_binding_id,
                alicia_resource_id,
                Some(alicia_token_lineage_id),
                412,
            ),
        ],
    )
    .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:alice.eth",
            "Alice.eth",
            "alice.eth",
            "namehash:alice.eth",
            alice_surface_binding_id,
            alice_resource_id,
            Some(alice_token_lineage_id),
            411,
            compact_name_declared_summary(
                address,
                address,
                resolver,
                1_900_000_000,
                "2026-04-17T00:00:21Z",
                "2026-04-17T00:00:11Z",
            ),
        ))
        .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:alicia.eth",
            "Alicia.eth",
            "alicia.eth",
            "namehash:alicia.eth",
            alicia_surface_binding_id,
            alicia_resource_id,
            Some(alicia_token_lineage_id),
            412,
            compact_name_declared_summary(
                address,
                address,
                resolver,
                1_800_000_000,
                "2026-04-17T00:00:22Z",
                "2026-04-17T00:00:12Z",
            ),
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names?namespace=ens&account={address}&relation=any&contains=lic&sort=expiry_date&order=asc&include=total_count&page_size=1"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact names request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["page"]["sort"], json!("expiry_date_asc"));
    assert_eq!(payload["page"]["page_size"], json!(1));
    assert_eq!(payload["meta"]["total_count"], json!(2));
    assert_eq!(payload["meta"]["unsupported_fields"], json!([]));
    assert_eq!(
        payload["data"]
            .as_array()
            .expect("data must be array")
            .len(),
        1
    );
    assert_eq!(payload["data"][0]["name"], json!("Alicia.eth"));
    assert_eq!(payload["data"][0]["normalized_name"], json!("alicia.eth"));
    assert_eq!(payload["data"][0]["owner"], json!(address));
    assert_eq!(payload["data"][0]["registrant"], json!(address));
    assert_eq!(payload["data"][0]["resolver_address"], json!(resolver));
    assert!(payload["data"][0].get("logical_name_id").is_none());
    assert!(payload["data"][0].get("provenance").is_none());
    let cursor = payload["page"]["next_cursor"]
        .as_str()
        .expect("first compact names page must include cursor");

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names?namespace=ens&account={address}&relation=any&contains=lic&sort=expiry_date&order=asc&include=total_count&page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact names second page request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let second_payload: Value = read_json(response).await?;
    assert_eq!(second_payload["data"][0]["name"], json!("Alice.eth"));
    assert_eq!(second_payload["meta"]["total_count"], json!(2));
    assert_eq!(second_payload["page"]["next_cursor"], Value::Null);

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names?resolved_address=0x0000000000000000000000000000000000000abc")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact names unsupported request failed")?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "unsupported");
    assert!(payload.error.message.contains("resolved_address"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_names_returns_explicit_unsupported_for_resolved_address_filter() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names?resolved_address=0x0000000000000000000000000000000000000abc")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact names unsupported request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "unsupported");
    assert!(payload.error.message.contains("resolved_address"));

    database.cleanup().await?;
    Ok(())
}

fn compact_name_declared_summary(
    owner: &str,
    registrant: &str,
    resolver: &str,
    expiry: i64,
    registration_date: &str,
    created_at: &str,
) -> Value {
    json!({
        "registration": {
            "status": "active",
            "registrant": registrant,
            "expiry": expiry,
            "registration_date": registration_date,
            "created_at": created_at,
        },
        "control": {
            "registry_owner": owner,
            "registrant": registrant,
            "expiry": expiry,
        },
        "resolver": {
            "chain_id": "ethereum-mainnet",
            "address": resolver,
            "latest_event_kind": "ResolverChanged",
        }
    })
}
