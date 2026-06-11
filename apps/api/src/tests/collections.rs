#[tokio::test]
async fn get_name_children_compact_default_returns_rows_with_summary_meta() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "ens:parent.eth";
    let owner = "0x0000000000000000000000000000000000000ABC";
    let registrant = "0x0000000000000000000000000000000000000DEF";
    let alice_labelhash = labelhash_for_display_name("alice.parent.eth")
        .expect("alice child labelhash must be available");
    let bob_labelhash =
        labelhash_for_display_name("bob.parent.eth").expect("bob child labelhash must be available");

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(parent_logical_name_id, "parent.eth", "node:parent.eth", 10),
            collection_name_surface(
                "ens:bob.parent.eth",
                "bob.parent.eth",
                "node:bob.parent.eth",
                11,
            ),
            collection_name_surface(
                "ens:alice.parent.eth",
                "alice.parent.eth",
                "node:alice.parent.eth",
                12,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            declared_child_row(
                parent_logical_name_id,
                "ens:bob.parent.eth",
                "bob.parent.eth",
                "node:bob.parent.eth",
                201,
                11,
            ),
            declared_child_row(
                parent_logical_name_id,
                "ens:alice.parent.eth",
                "alice.parent.eth",
                "node:alice.parent.eth",
                202,
                12,
            ),
        ],
    )
    .await?;
    database
        .insert_name_current_row(compact_child_name_current_row(
            "ens:alice.parent.eth",
            "alice.parent.eth",
            "node:alice.parent.eth",
            12,
            json!({
                "control": {
                    "registry_owner": owner,
                    "registrant": registrant,
                }
            }),
        ))
        .await?;
    database
        .insert_name_current_row(compact_child_name_current_row(
            "ens:bob.parent.eth",
            "bob.parent.eth",
            "node:bob.parent.eth",
            11,
            json!({
                "control": {
                    "owner": "0x0000000000000000000000000000000000000b0b",
                },
                "registration": {
                    "registrant": "0x0000000000000000000000000000000000000b0c",
                }
            }),
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact children request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload["data"],
        json!([
            {
                "name": "alice.parent.eth",
                "normalized_name": "alice.parent.eth",
                "label_name": "alice",
                "labelhash": alice_labelhash,
                "namehash": "node:alice.parent.eth",
                "owner": owner.to_ascii_lowercase(),
                "registrant": registrant.to_ascii_lowercase(),
            },
            {
                "name": "bob.parent.eth",
                "normalized_name": "bob.parent.eth",
                "label_name": "bob",
                "labelhash": bob_labelhash,
                "namehash": "node:bob.parent.eth",
                "owner": "0x0000000000000000000000000000000000000b0b",
                "registrant": "0x0000000000000000000000000000000000000b0c",
            },
        ])
    );
    assert_eq!(payload["meta"]["support_status"], json!("supported"));
    assert_eq!(payload["meta"]["unsupported_fields"], json!([]));
    assert_eq!(payload["meta"]["unsupported_filters"], json!([]));
    assert_eq!(payload["meta"]["total_count"], json!(2));
    assert!(payload["meta"].get("provenance").is_none());
    assert_eq!(payload["page"]["sort"], json!("display_name_asc"));
    assert_eq!(payload["page"]["page_size"], json!(50));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_compact_returns_unknown_label_rows_without_child_surface() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "ens:parent.eth";
    let labelhash = labelhash_for_display_name("mystery.parent.eth")
        .expect("mystery child labelhash must be available");
    let placeholder = format!(
        "[{}].parent.eth",
        labelhash.trim_start_matches("0x")
    );
    let owner = "0x0000000000000000000000000000000000000abc";
    let mut child_row = declared_child_row(
        parent_logical_name_id,
        "ens:mystery.parent.eth",
        "mystery.parent.eth",
        "node:mystery.parent.eth",
        211,
        11,
    );
    child_row.child_logical_name_id = format!("ens:{placeholder}");
    child_row.canonical_display_name = placeholder.clone();
    child_row.normalized_name = placeholder.clone();
    child_row.provenance["label"] = json!({
        "labelhash": labelhash,
        "source": "unknown",
        "status": "unknown",
    });
    child_row.owner = Some(owner.to_owned());

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[collection_name_surface(
            parent_logical_name_id,
            "parent.eth",
            "node:parent.eth",
            10,
        )],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(&database.pool, &[child_row]).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact unknown-label children request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload["data"],
        json!([
            {
                "name": placeholder,
                "normalized_name": placeholder,
                "label_name": format!("[{}]", labelhash.trim_start_matches("0x")),
                "labelhash": labelhash,
                "namehash": "node:mystery.parent.eth",
                "owner": owner,
                "registrant": null,
            }
        ])
    );
    assert_eq!(payload["meta"]["total_count"], json!(1));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_compact_paginates_unknown_label_rows_without_child_surface() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "ens:parent.eth";
    let mystery_labelhash = labelhash_for_display_name("mystery.parent.eth")
        .expect("mystery child labelhash must be available");
    let mystery_placeholder = format!(
        "[{}].parent.eth",
        mystery_labelhash.trim_start_matches("0x")
    );
    let mut mystery_row = declared_child_row(
        parent_logical_name_id,
        "ens:mystery.parent.eth",
        "mystery.parent.eth",
        "node:mystery.parent.eth",
        212,
        11,
    );
    mystery_row.child_logical_name_id = format!("ens:{mystery_placeholder}");
    mystery_row.canonical_display_name = mystery_placeholder;
    mystery_row.normalized_name = mystery_row.canonical_display_name.clone();
    mystery_row.provenance["label"] = json!({
        "labelhash": mystery_labelhash,
        "source": "unknown",
        "status": "unknown",
    });

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(parent_logical_name_id, "parent.eth", "node:parent.eth", 10),
            collection_name_surface(
                "ens:zeta.parent.eth",
                "zeta.parent.eth",
                "node:zeta.parent.eth",
                12,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            mystery_row,
            declared_child_row(
                parent_logical_name_id,
                "ens:zeta.parent.eth",
                "zeta.parent.eth",
                "node:zeta.parent.eth",
                213,
                12,
            ),
        ],
    )
    .await?;

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children?page_size=1")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact unknown-label children first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: Value = read_json(first_page_response).await?;
    let cursor = first_page_payload["page"]["next_cursor"]
        .as_str()
        .context("first unknown-label page must include next_cursor")?;

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names/ens/parent.eth/children?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact unknown-label children second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: Value = read_json(second_page_response).await?;
    assert_eq!(
        second_page_payload["data"][0]["name"],
        json!("zeta.parent.eth")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_compact_counts_marks_unknown_label_child_count_unsupported()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "ens:parent.eth";
    let labelhash = labelhash_for_display_name("mystery.parent.eth")
        .expect("mystery child labelhash must be available");
    let placeholder = format!(
        "[{}].parent.eth",
        labelhash.trim_start_matches("0x")
    );
    let mut child_row = declared_child_row(
        parent_logical_name_id,
        "ens:mystery.parent.eth",
        "mystery.parent.eth",
        "node:mystery.parent.eth",
        214,
        11,
    );
    child_row.child_logical_name_id = format!("ens:{placeholder}");
    child_row.canonical_display_name = placeholder;
    child_row.normalized_name = child_row.canonical_display_name.clone();
    child_row.provenance["label"] = json!({
        "labelhash": labelhash,
        "source": "unknown",
        "status": "unknown",
    });

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[collection_name_surface(
            parent_logical_name_id,
            "parent.eth",
            "node:parent.eth",
            10,
        )],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(&database.pool, &[child_row]).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children?include=counts")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact unknown-label children counts request failed")?;
    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"][0]["subname_count"], Value::Null);
    assert_eq!(payload["meta"]["unsupported_fields"], json!(["subname_count"]));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_compact_falls_back_to_surface_labelhash_for_legacy_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "ens:parent.eth";
    let child_logical_name_id = "ens:alice.parent.eth";
    let expected_labelhash = labelhash_for_display_name("alice.parent.eth")
        .expect("alice child labelhash must be available");
    let mut child_row = declared_child_row(
        parent_logical_name_id,
        child_logical_name_id,
        "alice.parent.eth",
        "node:alice.parent.eth",
        221,
        11,
    );
    child_row.labelhash = None;

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(parent_logical_name_id, "parent.eth", "node:parent.eth", 10),
            collection_name_surface(
                child_logical_name_id,
                "alice.parent.eth",
                "node:alice.parent.eth",
                11,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(&database.pool, &[child_row]).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact legacy-labelhash children request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"][0]["labelhash"], json!(expected_labelhash));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_compact_include_counts_returns_row_subname_counts() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "ens:parent.eth";
    let child_logical_name_id = "ens:alice.parent.eth";

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(parent_logical_name_id, "parent.eth", "node:parent.eth", 20),
            collection_name_surface(
                child_logical_name_id,
                "alice.parent.eth",
                "node:alice.parent.eth",
                21,
            ),
            collection_name_surface(
                "ens:one.alice.parent.eth",
                "one.alice.parent.eth",
                "node:one.alice.parent.eth",
                22,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            declared_child_row(
                parent_logical_name_id,
                child_logical_name_id,
                "alice.parent.eth",
                "node:alice.parent.eth",
                301,
                21,
            ),
            declared_child_row(
                child_logical_name_id,
                "ens:one.alice.parent.eth",
                "one.alice.parent.eth",
                "node:one.alice.parent.eth",
                302,
                22,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children?include=counts")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact children counts request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"][0]["subname_count"], json!(1));
    assert_eq!(payload["meta"]["total_count"], json!(1));
    assert_eq!(payload["meta"]["unsupported_fields"], json!([]));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_compact_meta_none_omits_meta() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "ens:parent.eth";

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(parent_logical_name_id, "parent.eth", "node:parent.eth", 30),
            collection_name_surface(
                "ens:alice.parent.eth",
                "alice.parent.eth",
                "node:alice.parent.eth",
                31,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[declared_child_row(
            parent_logical_name_id,
            "ens:alice.parent.eth",
            "alice.parent.eth",
            "node:alice.parent.eth",
            401,
            31,
        )],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children?meta=none")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact children meta=none request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert!(payload.get("meta").is_none());
    assert!(payload.get("data").is_some());
    assert!(payload.get("page").is_some());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_returns_declared_rows_sorted_with_declared_only_coverage() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "ens:parent.eth";

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(parent_logical_name_id, "parent.eth", "node:parent.eth", 10),
            collection_name_surface(
                "ens:bob.parent.eth",
                "bob.parent.eth",
                "node:bob.parent.eth",
                11,
            ),
            collection_name_surface(
                "ens:alice.parent.eth",
                "alice.parent.eth",
                "node:alice.parent.eth",
                12,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            declared_child_row(
                parent_logical_name_id,
                "ens:bob.parent.eth",
                "bob.parent.eth",
                "node:bob.parent.eth",
                201,
                11,
            ),
            declared_child_row(
                parent_logical_name_id,
                "ens:alice.parent.eth",
                "alice.parent.eth",
                "node:alice.parent.eth",
                202,
                12,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children?view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("children request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ChildrenResponse = read_json(response).await?;
    assert!(
        payload
            .declared_state
            .as_object()
            .map(|value| value.is_empty())
            .unwrap_or(false)
    );
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["declared".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "declared_direct_children"
    );
    assert_eq!(payload.coverage.unsupported_reason, None);
    assert_eq!(payload.page.sort, "display_name_asc");
    assert_eq!(payload.page.page_size, 50);
    assert_eq!(payload.consistency, "finalized");
    assert_eq!(
        payload.last_updated,
        format_timestamp(timestamp(1_717_172_012))
    );
    assert_eq!(
        payload.provenance,
        json!({
            "normalized_event_ids": ["202", "201"],
            "raw_fact_refs": [
                {"kind": "raw_log", "block_number": 12},
                {"kind": "raw_log", "block_number": 11}
            ],
            "manifest_versions": [{
                "manifest_version": 1,
                "source_family": "ens_v1_registry_l1",
                "source_manifest_id": null
            }],
            "derivation_kind": "children_current_rebuild"
        })
    );
    assert_eq!(
        payload.chain_positions,
        json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 12,
                "block_hash": "0xblock0c",
                "timestamp": "2026-04-17T00:00:12Z"
            }
        })
    );

    let child_ids = payload
        .data
        .iter()
        .map(|row| {
            row.get("logical_name_id")
                .and_then(Value::as_str)
                .expect("child row must include logical_name_id")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        child_ids,
        vec!["ens:alice.parent.eth", "ens:bob.parent.eth"]
    );
    assert_eq!(
        payload.data[0].get("surface_class").and_then(Value::as_str),
        Some("declared")
    );

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children?view=full&page_size=1")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("children first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: ChildrenResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("children first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names/ens/parent.eth/children?view=full&page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("children second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: ChildrenResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names/ens/parent.eth/children?view=full&page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("children replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: ChildrenResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &payload.data,
        &payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "display_name_asc",
        50,
        1,
    );
    assert_children_collection_metadata_eq(&payload, &first_page_payload);
    assert_children_collection_metadata_eq(&payload, &second_page_payload);
    assert_children_collection_metadata_eq(&payload, &replay_page_payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_defaults_to_first_page_of_fifty_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "ens:default-limit.eth";
    let mut surfaces = vec![collection_name_surface(
        parent_logical_name_id,
        "default-limit.eth",
        "node:default-limit.eth",
        10,
    )];
    let mut child_rows = Vec::new();

    for index in 0..55 {
        let display_name = format!("child{index:02}.default-limit.eth");
        let logical_name_id = format!("ens:{display_name}");
        let namehash = format!("node:{display_name}");
        let block_number = 11 + i64::from(index);

        surfaces.push(collection_name_surface(
            &logical_name_id,
            &display_name,
            &namehash,
            block_number,
        ));
        child_rows.push(declared_child_row(
            parent_logical_name_id,
            &logical_name_id,
            &display_name,
            &namehash,
            400 + i64::from(index),
            block_number,
        ));
    }

    bigname_storage::upsert_name_surfaces(&database.pool, &surfaces).await?;
    bigname_storage::upsert_children_current_rows(&database.pool, &child_rows).await?;

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/default-limit.eth/children?view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("default children pagination request failed")?;

    assert_eq!(first_page_response.status(), StatusCode::OK);

    let first_page_payload: ChildrenResponse = read_json(first_page_response).await?;
    assert_eq!(first_page_payload.page.page_size, 50);
    assert_eq!(first_page_payload.data.len(), 50);
    assert_eq!(first_page_payload.page.cursor, None);
    assert!(
        first_page_payload.page.next_cursor.is_some(),
        "default children pagination must include a next cursor when more than 50 rows exist"
    );
    assert_eq!(
        first_page_payload
            .data
            .first()
            .and_then(|row| row.get("logical_name_id"))
            .and_then(Value::as_str),
        Some("ens:child00.default-limit.eth")
    );
    assert_eq!(
        first_page_payload
            .data
            .last()
            .and_then(|row| row.get("logical_name_id"))
            .and_then(Value::as_str),
        Some("ens:child49.default-limit.eth")
    );

    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("default children pagination must return next_cursor");
    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names/ens/default-limit.eth/children?view=full&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("second children pagination request failed")?;

    assert_eq!(second_page_response.status(), StatusCode::OK);

    let second_page_payload: ChildrenResponse = read_json(second_page_response).await?;
    assert_eq!(second_page_payload.page.page_size, 50);
    assert_eq!(second_page_payload.data.len(), 5);
    assert_eq!(second_page_payload.page.cursor.as_deref(), Some(cursor.as_str()));
    assert_eq!(second_page_payload.page.next_cursor, None);
    assert_eq!(
        second_page_payload
            .data
            .first()
            .and_then(|row| row.get("logical_name_id"))
            .and_then(Value::as_str),
        Some("ens:child50.default-limit.eth")
    );
    assert_eq!(
        second_page_payload
            .data
            .last()
            .and_then(|row| row.get("logical_name_id"))
            .and_then(Value::as_str),
        Some("ens:child54.default-limit.eth")
    );
    assert_children_collection_metadata_eq(&first_page_payload, &second_page_payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_rejects_malformed_wrong_route_filter_and_stale_cursors() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(
                "ens:cursor-parent.eth",
                "cursor-parent.eth",
                "node:cursor-parent.eth",
                121,
            ),
            collection_name_surface(
                "ens:alpha.cursor-parent.eth",
                "alpha.cursor-parent.eth",
                "node:alpha.cursor-parent.eth",
                122,
            ),
            collection_name_surface(
                "ens:beta.cursor-parent.eth",
                "beta.cursor-parent.eth",
                "node:beta.cursor-parent.eth",
                123,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            declared_child_row(
                "ens:cursor-parent.eth",
                "ens:alpha.cursor-parent.eth",
                "alpha.cursor-parent.eth",
                "node:alpha.cursor-parent.eth",
                1201,
                122,
            ),
            declared_child_row(
                "ens:cursor-parent.eth",
                "ens:beta.cursor-parent.eth",
                "beta.cursor-parent.eth",
                "node:beta.cursor-parent.eth",
                1202,
                123,
            ),
        ],
    )
    .await?;

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/cursor-parent.eth/children?view=full&page_size=1")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("children cursor seed request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: ChildrenResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("children first page must include next_cursor");

    assert_invalid_cursor_request(
        database.app_state(),
        "/v1/names/ens/cursor-parent.eth/children?page_size=1&cursor=not-a-cursor",
    )
    .await?;

    let wrong_route_cursor = rewrite_cursor(&cursor, |envelope| {
        envelope.route = "/v1/addresses/{address}/names".to_owned();
    });
    assert_invalid_cursor_request(
        database.app_state(),
        format!("/v1/names/ens/cursor-parent.eth/children?page_size=1&cursor={wrong_route_cursor}"),
    )
    .await?;

    let filter_cursor = rewrite_cursor(&cursor, |envelope| {
        envelope
            .filters
            .insert("namespace".to_owned(), "ens".to_owned());
    });
    assert_invalid_cursor_request(
        database.app_state(),
        format!("/v1/names/ens/cursor-parent.eth/children?page_size=1&cursor={filter_cursor}"),
    )
    .await?;

    let stale_cursor = rewrite_cursor(&cursor, |envelope| {
        envelope.item.insert(
            "canonical_display_name".to_owned(),
            "missing.cursor-parent.eth".to_owned(),
        );
        envelope.item.insert(
            "child_logical_name_id".to_owned(),
            "ens:missing.cursor-parent.eth".to_owned(),
        );
    });
    assert_invalid_cursor_request(
        database.app_state(),
        format!("/v1/names/ens/cursor-parent.eth/children?page_size=1&cursor={stale_cursor}"),
    )
    .await?;

    sqlx::query(
        r#"
        UPDATE name_surfaces
        SET canonicality_state = 'observed'::canonicality_state
        WHERE logical_name_id = 'ens:alpha.cursor-parent.eth'
        "#,
    )
    .execute(&database.pool)
    .await?;
    assert_invalid_cursor_request(
        database.app_state(),
        format!("/v1/names/ens/cursor-parent.eth/children?page_size=1&cursor={cursor}"),
    )
    .await?;

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_returns_ensv2_declared_children_without_widening_route_shape()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "ens:subregistry.eth";

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(
                parent_logical_name_id,
                "subregistry.eth",
                "node:subregistry.eth",
                50,
            ),
            collection_name_surface(
                "ens:bob.subregistry.eth",
                "bob.subregistry.eth",
                "node:bob.subregistry.eth",
                51,
            ),
            collection_name_surface(
                "ens:alice.subregistry.eth",
                "alice.subregistry.eth",
                "node:alice.subregistry.eth",
                52,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            ensv2_declared_child_row(
                parent_logical_name_id,
                "ens:bob.subregistry.eth",
                "bob.subregistry.eth",
                "node:bob.subregistry.eth",
                501,
                51,
            ),
            ensv2_declared_child_row(
                parent_logical_name_id,
                "ens:alice.subregistry.eth",
                "alice.subregistry.eth",
                "node:alice.subregistry.eth",
                502,
                52,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/subregistry.eth/children?surface_classes=declared&include=counts&view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 children request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ChildrenResponse = read_json(response).await?;
    assert_eq!(payload.declared_state, json!({"subname_count": 2}));
    assert_eq!(payload.verified_state, None);
    assert_eq!(
        payload.coverage,
        CoverageResponse {
            status: "full".to_owned(),
            exhaustiveness: "authoritative".to_owned(),
            source_classes_considered: vec!["declared".to_owned()],
            enumeration_basis: "declared_direct_children".to_owned(),
            unsupported_reason: None,
        }
    );
    assert_eq!(
        payload.provenance,
        json!({
            "normalized_event_ids": ["502", "501"],
            "raw_fact_refs": [
                {"kind": "raw_log", "chain_id": "ethereum-mainnet", "block_number": 52},
                {"kind": "raw_log", "chain_id": "ethereum-mainnet", "block_number": 51}
            ],
            "manifest_versions": [{
                "manifest_version": 7,
                "source_family": "ens_v2_registry_l1",
                "source_manifest_id": null
            }],
            "derivation_kind": "children_current_rebuild"
        })
    );
    assert_eq!(
        payload.data,
        vec![
            json!({
                "logical_name_id": "ens:alice.subregistry.eth",
                "namespace": "ens",
                "normalized_name": "alice.subregistry.eth",
                "canonical_display_name": "alice.subregistry.eth",
                "namehash": "node:alice.subregistry.eth",
                "surface_class": "declared",
            }),
            json!({
                "logical_name_id": "ens:bob.subregistry.eth",
                "namespace": "ens",
                "normalized_name": "bob.subregistry.eth",
                "canonical_display_name": "bob.subregistry.eth",
                "namehash": "node:bob.subregistry.eth",
                "surface_class": "declared",
            }),
        ]
    );
    assert_eq!(payload.page.sort, "display_name_asc");
    assert_eq!(payload.page.page_size, 50);
    assert_eq!(payload.consistency, "finalized");
    assert_eq!(
        payload.last_updated,
        format_timestamp(timestamp(1_717_172_052))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_include_counts_returns_declared_subname_count() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "ens:parent.eth";

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(parent_logical_name_id, "parent.eth", "node:parent.eth", 20),
            collection_name_surface(
                "ens:alice.parent.eth",
                "alice.parent.eth",
                "node:alice.parent.eth",
                21,
            ),
            collection_name_surface(
                "ens:bob.parent.eth",
                "bob.parent.eth",
                "node:bob.parent.eth",
                22,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            declared_child_row(
                parent_logical_name_id,
                "ens:alice.parent.eth",
                "alice.parent.eth",
                "node:alice.parent.eth",
                301,
                21,
            ),
            declared_child_row(
                parent_logical_name_id,
                "ens:bob.parent.eth",
                "bob.parent.eth",
                "node:bob.parent.eth",
                302,
                22,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/parent.eth/children?include=counts&view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("children counts request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ChildrenResponse = read_json(response).await?;
    assert_eq!(payload.declared_state.get("subname_count"), Some(&json!(2)));
    assert_eq!(payload.data.len(), 2);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_returns_basenames_rows_from_base_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let parent_logical_name_id = "basenames:base.eth";

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(parent_logical_name_id, "base.eth", "node:base.eth", 40),
            collection_name_surface(
                "basenames:bob.base.eth",
                "bob.base.eth",
                "node:bob.base.eth",
                41,
            ),
            collection_name_surface(
                "basenames:alice.base.eth",
                "alice.base.eth",
                "node:alice.base.eth",
                42,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            declared_child_row(
                parent_logical_name_id,
                "basenames:bob.base.eth",
                "bob.base.eth",
                "node:bob.base.eth",
                401,
                41,
            ),
            declared_child_row(
                parent_logical_name_id,
                "basenames:alice.base.eth",
                "alice.base.eth",
                "node:alice.base.eth",
                402,
                42,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/basenames/base.eth/children?view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames children request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ChildrenResponse = read_json(response).await?;
    assert!(
        payload
            .declared_state
            .as_object()
            .map(|value| value.is_empty())
            .unwrap_or(false)
    );
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["declared".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "declared_direct_children"
    );
    assert_eq!(payload.page.sort, "display_name_asc");
    assert_eq!(payload.consistency, "finalized");
    assert_eq!(
        payload.last_updated,
        format_timestamp(timestamp(1_717_172_042))
    );
    assert_eq!(
        payload.provenance,
        json!({
            "normalized_event_ids": ["402", "401"],
            "raw_fact_refs": [
                {"kind": "raw_log", "block_number": 42},
                {"kind": "raw_log", "block_number": 41}
            ],
            "manifest_versions": [{
                "manifest_version": 1,
                "source_family": "basenames_base_registry",
                "source_manifest_id": null
            }],
            "derivation_kind": "children_current_rebuild"
        })
    );
    assert_eq!(
        payload.chain_positions,
        json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 42,
                "block_hash": "0xblock2a",
                "timestamp": "2026-04-17T00:00:42Z"
            }
        })
    );

    let child_ids = payload
        .data
        .iter()
        .map(|row| {
            row.get("logical_name_id")
                .and_then(Value::as_str)
                .expect("child row must include logical_name_id")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        child_ids,
        vec!["basenames:alice.base.eth", "basenames:bob.base.eth"]
    );
    assert_eq!(
        payload.data[0].get("surface_class").and_then(Value::as_str),
        Some("declared")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_children_rejects_non_declared_surface_classes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[collection_name_surface(
            "ens:parent.eth",
            "parent.eth",
            "node:parent.eth",
            30,
        )],
    )
    .await?;

    for surface_classes in ["linked", "alias", "wildcard", "declared,linked"] {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/names/ens/parent.eth/children?surface_classes={surface_classes}"
                    ))
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .with_context(|| {
                format!("children unsupported surface_classes={surface_classes} request failed")
            })?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let payload: ErrorResponse = read_json(response).await?;
        assert_eq!(payload.error.code, "unsupported");
        assert_eq!(
            payload.error.message,
            "surface_classes other than declared are not yet supported"
        );
        assert!(payload.error.details.is_empty());
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_returns_not_found_when_projection_row_is_missing() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/missing.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "name missing.eth was not found in namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_coverage_returns_not_found_when_projection_row_is_missing() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/coverage/ens/missing.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("coverage request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "name missing.eth was not found in namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_surface_binding_explain_returns_not_found_when_projection_row_is_missing() -> Result<()>
{
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/names/ens/missing.eth/surface-binding")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("surface-binding explain request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "name missing.eth was not found in namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_authority_control_explain_returns_not_found_when_projection_row_is_missing()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database
        .seed_default_ens_snapshot_selector_position()
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/names/ens/missing.eth/authority-control")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("authority-control explain request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "name missing.eth was not found in namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_returns_surface_first_rows_sorted_with_stable_relation_facets()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bbb";
    let alpha_resource_id = Uuid::from_u128(0x8100);
    let alpha_token_lineage_id = Uuid::from_u128(0x8101);
    let alpha_surface_binding_id = Uuid::from_u128(0x8102);
    let beta_resource_id = Uuid::from_u128(0x8200);
    let beta_token_lineage_id = Uuid::from_u128(0x8201);
    let beta_surface_binding_id = Uuid::from_u128(0x8202);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xalpha", None, 11, 1_717_173_011),
            raw_block("ethereum-mainnet", "0xbeta", None, 12, 1_717_173_012),
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[
            address_name_token_lineage(alpha_token_lineage_id, "0xalpha", 11),
            address_name_token_lineage(beta_token_lineage_id, "0xbeta", 12),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(
                alpha_resource_id,
                Some(alpha_token_lineage_id),
                "0xalpha",
                11,
            ),
            address_name_resource(beta_resource_id, Some(beta_token_lineage_id), "0xbeta", 12),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:beta.eth", "beta.eth", "node:beta.eth", 12),
            collection_name_surface("ens:alpha.eth", "alpha.eth", "node:alpha.eth", 11),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(
                beta_surface_binding_id,
                "ens:beta.eth",
                beta_resource_id,
                "0xbeta",
                12,
                1_717_173_012,
            ),
            address_name_surface_binding(
                alpha_surface_binding_id,
                "ens:alpha.eth",
                alpha_resource_id,
                "0xalpha",
                11,
                1_717_173_011,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                address,
                "ens:beta.eth",
                bigname_storage::AddressNameRelation::EffectiveController,
                "beta.eth",
                "beta.eth",
                "node:beta.eth",
                beta_surface_binding_id,
                beta_resource_id,
                Some(beta_token_lineage_id),
                12,
            ),
            address_name_current_row(
                address,
                "ens:alpha.eth",
                bigname_storage::AddressNameRelation::TokenHolder,
                "alpha.eth",
                "alpha.eth",
                "node:alpha.eth",
                alpha_surface_binding_id,
                alpha_resource_id,
                Some(alpha_token_lineage_id),
                11,
            ),
            address_name_current_row(
                address,
                "ens:alpha.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "alpha.eth",
                "alpha.eth",
                "node:alpha.eth",
                alpha_surface_binding_id,
                alpha_resource_id,
                Some(alpha_token_lineage_id),
                11,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/addresses/{address}/names"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address names request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: AddressNamesResponse = read_json(response).await?;
    assert!(
        payload
            .declared_state
            .as_object()
            .map(|value| value.is_empty())
            .unwrap_or(false)
    );
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["ensv1_registry_path".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "surface_current_relations"
    );
    assert_eq!(payload.page.sort, "display_name_asc");
    assert_eq!(payload.page.page_size, 50);
    assert_eq!(payload.consistency, "finalized");

    let logical_name_ids = payload
        .data
        .iter()
        .map(|row| {
            row.get("logical_name_id")
                .and_then(Value::as_str)
                .expect("address-name row must include logical_name_id")
        })
        .collect::<Vec<_>>();
    assert_eq!(logical_name_ids, vec!["ens:alpha.eth", "ens:beta.eth"]);
    assert_eq!(
        payload.data[0].get("relation_facets"),
        Some(&json!(["registrant", "token_holder"]))
    );
    assert_eq!(
        payload.data[1].get("relation_facets"),
        Some(&json!(["effective_controller"]))
    );

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/addresses/{address}/names?page_size=1"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address names first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: AddressNamesResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("address names first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{address}/names?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address names second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: AddressNamesResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{address}/names?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address names replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: AddressNamesResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &payload.data,
        &payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "display_name_asc",
        50,
        1,
    );
    assert_address_names_collection_metadata_eq(&payload, &first_page_payload);
    assert_address_names_collection_metadata_eq(&payload, &second_page_payload);
    assert_address_names_collection_metadata_eq(&payload, &replay_page_payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_rejects_malformed_wrong_route_filter_and_stale_cursors() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000c01";
    let alpha_resource_id = Uuid::from_u128(0xb101);
    let beta_resource_id = Uuid::from_u128(0xb102);
    let alpha_surface_binding_id = Uuid::from_u128(0xb111);
    let beta_surface_binding_id = Uuid::from_u128(0xb112);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block(
                "ethereum-mainnet",
                "0xaddress-cursor-alpha",
                None,
                131,
                1_717_173_131,
            ),
            raw_block(
                "ethereum-mainnet",
                "0xaddress-cursor-beta",
                None,
                132,
                1_717_173_132,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(alpha_resource_id, None, "0xaddress-cursor-alpha", 131),
            address_name_resource(beta_resource_id, None, "0xaddress-cursor-beta", 132),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:cursor-alpha.eth", "cursor-alpha.eth", "node:ca", 131),
            collection_name_surface("ens:cursor-beta.eth", "cursor-beta.eth", "node:cb", 132),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(
                alpha_surface_binding_id,
                "ens:cursor-alpha.eth",
                alpha_resource_id,
                "0xaddress-cursor-alpha",
                131,
                1_717_173_131,
            ),
            address_name_surface_binding(
                beta_surface_binding_id,
                "ens:cursor-beta.eth",
                beta_resource_id,
                "0xaddress-cursor-beta",
                132,
                1_717_173_132,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                address,
                "ens:cursor-alpha.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "cursor-alpha.eth",
                "cursor-alpha.eth",
                "node:ca",
                alpha_surface_binding_id,
                alpha_resource_id,
                None,
                131,
            ),
            address_name_current_row(
                address,
                "ens:cursor-beta.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "cursor-beta.eth",
                "cursor-beta.eth",
                "node:cb",
                beta_surface_binding_id,
                beta_resource_id,
                None,
                132,
            ),
        ],
    )
    .await?;

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/addresses/{address}/names?page_size=1"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address names cursor seed request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: AddressNamesResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("address names first page must include next_cursor");

    assert_invalid_cursor_request(
        database.app_state(),
        format!("/v1/addresses/{address}/names?page_size=1&cursor=not-a-cursor"),
    )
    .await?;

    let wrong_route_cursor = rewrite_cursor(&cursor, |envelope| {
        envelope.route = "/v1/names/{namespace}/{name}/children".to_owned();
    });
    assert_invalid_cursor_request(
        database.app_state(),
        format!("/v1/addresses/{address}/names?page_size=1&cursor={wrong_route_cursor}"),
    )
    .await?;

    assert_invalid_cursor_request(
        database.app_state(),
        format!("/v1/addresses/{address}/names?namespace=ens&page_size=1&cursor={cursor}"),
    )
    .await?;

    let stale_cursor = rewrite_cursor(&cursor, |envelope| {
        envelope.item.insert(
            "canonical_display_name".to_owned(),
            "missing-cursor.eth".to_owned(),
        );
        envelope.item.insert(
            "logical_name_id".to_owned(),
            "ens:missing-cursor.eth".to_owned(),
        );
        envelope.item.insert(
            "resource_id".to_owned(),
            Uuid::from_u128(0xb1ff).to_string(),
        );
    });
    assert_invalid_cursor_request(
        database.app_state(),
        format!("/v1/addresses/{address}/names?page_size=1&cursor={stale_cursor}"),
    )
    .await?;

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_honors_namespace_and_relation_filters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let ens_resource_id = Uuid::from_u128(0x8300);
    let ens_token_lineage_id = Uuid::from_u128(0x8301);
    let ens_surface_binding_id = Uuid::from_u128(0x8302);
    let base_resource_id = Uuid::from_u128(0x8400);
    let base_surface_binding_id = Uuid::from_u128(0x8402);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xens", None, 21, 1_717_173_021),
            raw_block("ethereum-mainnet", "0xbase", None, 22, 1_717_173_022),
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            ens_token_lineage_id,
            "0xens",
            21,
        )],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(ens_resource_id, Some(ens_token_lineage_id), "0xens", 21),
            address_name_resource(base_resource_id, None, "0xbase", 22),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:alice.eth", "alice.eth", "node:alice.eth", 21),
            collection_name_surface(
                "basenames:alice.base.eth",
                "alice.base.eth",
                "node:alice.base.eth",
                22,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(
                ens_surface_binding_id,
                "ens:alice.eth",
                ens_resource_id,
                "0xens",
                21,
                1_717_173_021,
            ),
            address_name_surface_binding(
                base_surface_binding_id,
                "basenames:alice.base.eth",
                base_resource_id,
                "0xbase",
                22,
                1_717_173_022,
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
                bigname_storage::AddressNameRelation::Registrant,
                "alice.eth",
                "alice.eth",
                "node:alice.eth",
                ens_surface_binding_id,
                ens_resource_id,
                Some(ens_token_lineage_id),
                21,
            ),
            address_name_current_row(
                address,
                "basenames:alice.base.eth",
                bigname_storage::AddressNameRelation::EffectiveController,
                "alice.base.eth",
                "alice.base.eth",
                "node:alice.base.eth",
                base_surface_binding_id,
                base_resource_id,
                None,
                22,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{address}/names?namespace=ens&relation=registrant"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("filtered address names request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: AddressNamesResponse = read_json(response).await?;
    assert_eq!(payload.data.len(), 1);
    assert_eq!(
        payload.data[0].get("logical_name_id"),
        Some(&Value::String("ens:alice.eth".to_owned()))
    );
    assert_eq!(
        payload.data[0].get("relation_facets"),
        Some(&json!(["registrant"]))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_dedupe_by_resource_changes_grouping_only() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000def";
    let shared_resource_id = Uuid::from_u128(0x8500);
    let shared_token_lineage_id = Uuid::from_u128(0x8501);
    let alpha_surface_binding_id = Uuid::from_u128(0x8502);
    let beta_surface_binding_id = Uuid::from_u128(0x8503);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[raw_block(
            "ethereum-mainnet",
            "0xshared",
            None,
            31,
            1_717_173_031,
        )],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            shared_token_lineage_id,
            "0xshared",
            31,
        )],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[address_name_resource(
            shared_resource_id,
            Some(shared_token_lineage_id),
            "0xshared",
            31,
        )],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:beta.eth", "beta.eth", "node:beta.eth", 31),
            collection_name_surface("ens:alpha.eth", "alpha.eth", "node:alpha.eth", 31),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(
                beta_surface_binding_id,
                "ens:beta.eth",
                shared_resource_id,
                "0xshared",
                31,
                1_717_173_031,
            ),
            address_name_surface_binding(
                alpha_surface_binding_id,
                "ens:alpha.eth",
                shared_resource_id,
                "0xshared",
                31,
                1_717_173_031,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                address,
                "ens:beta.eth",
                bigname_storage::AddressNameRelation::EffectiveController,
                "beta.eth",
                "beta.eth",
                "node:beta.eth",
                beta_surface_binding_id,
                shared_resource_id,
                Some(shared_token_lineage_id),
                31,
            ),
            address_name_current_row(
                address,
                "ens:alpha.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "alpha.eth",
                "alpha.eth",
                "node:alpha.eth",
                alpha_surface_binding_id,
                shared_resource_id,
                Some(shared_token_lineage_id),
                31,
            ),
            address_name_current_row(
                address,
                "ens:alpha.eth",
                bigname_storage::AddressNameRelation::TokenHolder,
                "alpha.eth",
                "alpha.eth",
                "node:alpha.eth",
                alpha_surface_binding_id,
                shared_resource_id,
                Some(shared_token_lineage_id),
                31,
            ),
        ],
    )
    .await?;

    let surface_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/addresses/{address}/names?dedupe_by=surface"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("surface-dedupe address names request failed")?;
    let surface_payload: AddressNamesResponse = read_json(surface_response).await?;
    assert_eq!(surface_payload.data.len(), 2);

    let resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/addresses/{address}/names?dedupe_by=resource"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource-dedupe address names request failed")?;

    assert_eq!(resource_response.status(), StatusCode::OK);

    let resource_payload: AddressNamesResponse = read_json(resource_response).await?;
    assert_eq!(resource_payload.data.len(), 1);
    assert_eq!(
        resource_payload.data[0].get("logical_name_id"),
        Some(&Value::String("ens:alpha.eth".to_owned()))
    );
    assert_eq!(
        resource_payload.data[0].get("resource_id"),
        Some(&Value::String(shared_resource_id.to_string()))
    );
    assert_eq!(
        resource_payload.data[0].get("relation_facets"),
        Some(&json!([
            "registrant",
            "token_holder",
            "effective_controller"
        ]))
    );
    assert_eq!(resource_payload.coverage, surface_payload.coverage);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_returns_basenames_base_authority_relation_facets() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bcd";
    let resource_id = Uuid::from_u128(0x85a0);
    let token_lineage_id = Uuid::from_u128(0x85a1);
    let surface_binding_id = Uuid::from_u128(0x85a2);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[raw_block(
            "base-mainnet",
            "0xbase-alpha",
            None,
            41,
            1_717_173_041,
        )],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            token_lineage_id,
            "0xbase-alpha",
            41,
        )],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[address_name_resource(
            resource_id,
            Some(token_lineage_id),
            "0xbase-alpha",
            41,
        )],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[collection_name_surface(
            "basenames:alice.base.eth",
            "alice.base.eth",
            "node:alice.base.eth",
            41,
        )],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[address_name_surface_binding(
            surface_binding_id,
            "basenames:alice.base.eth",
            resource_id,
            "0xbase-alpha",
            41,
            1_717_173_041,
        )],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                address,
                "basenames:alice.base.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "alice.base.eth",
                "alice.base.eth",
                "node:alice.base.eth",
                surface_binding_id,
                resource_id,
                Some(token_lineage_id),
                41,
            ),
            address_name_current_row(
                address,
                "basenames:alice.base.eth",
                bigname_storage::AddressNameRelation::TokenHolder,
                "alice.base.eth",
                "alice.base.eth",
                "node:alice.base.eth",
                surface_binding_id,
                resource_id,
                Some(token_lineage_id),
                41,
            ),
            address_name_current_row(
                address,
                "basenames:alice.base.eth",
                bigname_storage::AddressNameRelation::EffectiveController,
                "alice.base.eth",
                "alice.base.eth",
                "node:alice.base.eth",
                surface_binding_id,
                resource_id,
                Some(token_lineage_id),
                41,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/addresses/{address}/names?namespace=basenames"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames address names request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: AddressNamesResponse = read_json(response).await?;
    assert_eq!(payload.data.len(), 1);
    assert_eq!(
        payload.data[0].get("logical_name_id"),
        Some(&Value::String("basenames:alice.base.eth".to_owned()))
    );
    assert_eq!(
        payload.data[0].get("relation_facets"),
        Some(&json!([
            "registrant",
            "token_holder",
            "effective_controller"
        ]))
    );
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["ensv1_registry_path".to_owned()]
    );
    assert!(payload.data[0].get("role_summary").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_returns_basenames_base_authority_relation_facets_across_control_vectors()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let cases = [
        (
            "nft-only.base.eth",
            BasenamesControlVectorScenario::NftOnly,
            0x86a0_u128,
        ),
        (
            "management-only.base.eth",
            BasenamesControlVectorScenario::ManagementOnly,
            0x86b0_u128,
        ),
        (
            "full-transfer.base.eth",
            BasenamesControlVectorScenario::FullTransfer,
            0x86c0_u128,
        ),
    ];

    for (name, scenario, base_id) in cases {
        let logical_name_id = format!("basenames:{name}");
        database
            .seed_basenames_control_vector_rebuild_inputs(
                &logical_name_id,
                Uuid::from_u128(base_id),
                Uuid::from_u128(base_id + 1),
                Uuid::from_u128(base_id + 2),
                scenario,
            )
            .await?;
    }
    database.rebuild_address_names_current(None).await?;

    for (name, scenario, _) in cases {
        let logical_name_id = format!("basenames:{name}");
        let holder_response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/addresses/{}/names?namespace=basenames",
                        scenario.current_token_subject()
                    ))
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .with_context(|| format!("Basenames address names request failed for {name}"))?;

        assert_eq!(holder_response.status(), StatusCode::OK);
        let holder_payload: AddressNamesResponse = read_json(holder_response).await?;
        assert_eq!(holder_payload.data.len(), 1);
        assert_eq!(
            holder_payload.data[0].get("logical_name_id"),
            Some(&json!(logical_name_id))
        );
        assert_eq!(
            holder_payload.data[0].get("relation_facets"),
            Some(&match scenario {
                BasenamesControlVectorScenario::FullTransfer =>
                    json!(["registrant", "token_holder", "effective_controller"]),
                _ => json!(["registrant", "token_holder"]),
            })
        );
        assert_eq!(
            holder_payload.coverage.source_classes_considered,
            vec!["ensv1_registry_path".to_owned()]
        );

        if scenario.current_effective_controller() != scenario.current_token_subject() {
            let controller_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{}/names?namespace=basenames",
                            scenario.current_effective_controller()
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!("Basenames controller address names request failed for {name}")
                })?;

            assert_eq!(controller_response.status(), StatusCode::OK);
            let controller_payload: AddressNamesResponse = read_json(controller_response).await?;
            assert_eq!(controller_payload.data.len(), 1);
            assert_eq!(
                controller_payload.data[0].get("logical_name_id"),
                Some(&json!(logical_name_id))
            );
            assert_eq!(
                controller_payload.data[0].get("relation_facets"),
                Some(&json!(["effective_controller"]))
            );
        }

        if let Some(previous_controller) = scenario.previous_effective_controller() {
            let previous_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{previous_controller}/names?namespace=basenames"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!("Basenames previous controller request failed for {name}")
                })?;

            assert_eq!(previous_response.status(), StatusCode::OK);
            let previous_payload: AddressNamesResponse = read_json(previous_response).await?;
            assert!(previous_payload.data.is_empty());
        }
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_include_role_summary_adds_projection_backed_expansion_fields()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000fed";
    let resource_id = Uuid::from_u128(0x8600);
    let token_lineage_id = Uuid::from_u128(0x8601);
    let surface_binding_id = Uuid::from_u128(0x8602);
    let subject = "0x0000000000000000000000000000000000000abc";
    let other_subject = "0x0000000000000000000000000000000000000def";

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xalpha", None, 61, 1_717_173_061),
            raw_block("ethereum-mainnet", "0xperm", None, 62, 1_717_173_062),
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[address_name_token_lineage(token_lineage_id, "0xalpha", 61)],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[address_name_resource(
            resource_id,
            Some(token_lineage_id),
            "0xalpha",
            61,
        )],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:alpha.eth", "alpha.eth", "node:alpha.eth", 61),
            collection_name_surface(
                "ens:child-one.alpha.eth",
                "child-one.alpha.eth",
                "node:child-one.alpha.eth",
                62,
            ),
            collection_name_surface(
                "ens:child-two.alpha.eth",
                "child-two.alpha.eth",
                "node:child-two.alpha.eth",
                63,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[address_name_surface_binding(
            surface_binding_id,
            "ens:alpha.eth",
            resource_id,
            "0xalpha",
            61,
            1_717_173_061,
        )],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[address_name_current_row(
            address,
            "ens:alpha.eth",
            bigname_storage::AddressNameRelation::Registrant,
            "alpha.eth",
            "alpha.eth",
            "node:alpha.eth",
            surface_binding_id,
            resource_id,
            Some(token_lineage_id),
            61,
        )],
    )
    .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:alpha.eth",
            "alpha.eth",
            "alpha.eth",
            "node:alpha.eth",
            surface_binding_id,
            resource_id,
            Some(token_lineage_id),
            64,
            json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar",
                },
                "control": {
                    "status": "wrapped",
                    "expiry": "2026-09-01T00:00:00Z",
                    "registrant": address,
                    "registry_owner": subject,
                    "latest_event_kind": "NameWrapped",
                },
                "resolver": {
                    "chain_id": "ethereum-mainnet",
                    "address": "0x0000000000000000000000000000000000000aaa",
                    "latest_event_kind": "ResolverChanged",
                },
                "record_inventory": {
                    "status": "supported",
                    "count": 2,
                },
                "history": {
                    "surface_head": null,
                    "resource_head": null,
                },
            }),
        ))
        .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            declared_child_row(
                "ens:alpha.eth",
                "ens:child-one.alpha.eth",
                "child-one.alpha.eth",
                "node:child-one.alpha.eth",
                701,
                62,
            ),
            declared_child_row(
                "ens:alpha.eth",
                "ens:child-two.alpha.eth",
                "child-two.alpha.eth",
                "node:child-two.alpha.eth",
                702,
                63,
            ),
        ],
    )
    .await?;
    let mut resolver_permission = permission_current_row(
        resource_id,
        subject,
        PermissionScope::Resolver {
            chain_id: "ethereum-mainnet".to_owned(),
            resolver_address: "0x0000000000000000000000000000000000000aaa".to_owned(),
        },
        8,
        72,
    );
    resolver_permission.canonicality_summary = json!({
        "status": "head",
        "chains": {
            "ethereum-mainnet": "head",
        }
    });

    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            permission_current_row(resource_id, subject, PermissionScope::Resource, 7, 71),
            resolver_permission,
            permission_current_row(resource_id, other_subject, PermissionScope::Registry, 9, 73),
        ],
    )
    .await?;

    let base_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/addresses/{address}/names"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("base address names request failed")?;
    let include_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{address}/names?include=role_summary"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("role summary request failed")?;
    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alpha.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("exact-name request failed")?;

    assert_eq!(base_response.status(), StatusCode::OK);
    assert_eq!(include_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let base_payload: AddressNamesResponse = read_json(base_response).await?;
    let payload: AddressNamesResponse = read_json(include_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;

    assert_eq!(payload.coverage, base_payload.coverage);
    assert_eq!(payload.page, base_payload.page);
    assert_eq!(payload.declared_state, base_payload.declared_state);
    assert_eq!(payload.consistency, base_payload.consistency);
    assert_eq!(payload.data.len(), 1);
    assert_eq!(
        payload.data[0].get("logical_name_id"),
        base_payload.data[0].get("logical_name_id")
    );
    assert_eq!(
        payload.data[0].get("resource_id"),
        base_payload.data[0].get("resource_id")
    );
    assert_eq!(
        payload.data[0].get("relation_facets"),
        base_payload.data[0].get("relation_facets")
    );
    assert_eq!(payload.data[0].get("status"), Some(&json!("wrapped")));
    assert_eq!(
        payload.data[0].get("expiry"),
        Some(&json!("2026-09-01T00:00:00Z"))
    );
    assert_eq!(
        name_payload.coverage.get("status").and_then(Value::as_str),
        Some("full")
    );
    assert_eq!(
        name_payload
            .declared_state
            .get("control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("registrant")),
        Some(&json!(address))
    );
    assert_eq!(
        name_payload
            .declared_state
            .get("control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("registry_owner")),
        Some(&json!(subject))
    );
    assert_eq!(
        name_payload
            .declared_state
            .get("control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("latest_event_kind")),
        Some(&json!("NameWrapped"))
    );
    assert!(
        name_payload
            .declared_state
            .get("control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .is_none()
    );
    assert!(
        name_payload
            .declared_state
            .get("control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("expiry"))
            .is_none()
    );
    assert_eq!(payload.data[0].get("record_count"), Some(&json!(2)));
    assert_eq!(payload.data[0].get("subname_count"), Some(&json!(2)));
    assert_eq!(
        payload.data[0].get("role_summary"),
        Some(&json!({
            "subjects": [
                {
                    "subject": subject,
                    "scopes": [
                        {
                            "scope": {
                                "kind": "resolver",
                                "detail": {
                                    "chain_id": "ethereum-mainnet",
                                    "resolver_address": "0x0000000000000000000000000000000000000aaa",
                                },
                            },
                            "effective_powers": ["set_resolver", "create_subnames"],
                        },
                        {
                            "scope": {
                                "kind": "resource",
                                "detail": {},
                            },
                            "effective_powers": ["set_resolver", "set_records"],
                        },
                    ],
                },
                {
                    "subject": other_subject,
                    "scopes": [
                        {
                            "scope": {
                                "kind": "registry",
                                "detail": {},
                            },
                            "effective_powers": ["set_resolver", "set_records"],
                        },
                    ],
                },
            ],
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_include_role_summary_paginates_with_batched_expansions() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bee";
    let alpha_resource_id = Uuid::from_u128(0x8a00);
    let alpha_surface_binding_id = Uuid::from_u128(0x8a01);
    let beta_resource_id = Uuid::from_u128(0x8b00);
    let beta_surface_binding_id = Uuid::from_u128(0x8b01);
    let alpha_subject = "0x0000000000000000000000000000000000000a11";
    let beta_subject = "0x0000000000000000000000000000000000000b22";

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xrole-alpha", None, 91, 1_717_173_091),
            raw_block("ethereum-mainnet", "0xrole-beta", None, 92, 1_717_173_092),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(alpha_resource_id, None, "0xrole-alpha", 91),
            address_name_resource(beta_resource_id, None, "0xrole-beta", 92),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:alpha.eth", "alpha.eth", "node:alpha.eth", 91),
            collection_name_surface("ens:beta.eth", "beta.eth", "node:beta.eth", 92),
            collection_name_surface(
                "ens:one.alpha.eth",
                "one.alpha.eth",
                "node:one.alpha.eth",
                93,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(
                alpha_surface_binding_id,
                "ens:alpha.eth",
                alpha_resource_id,
                "0xrole-alpha",
                91,
                1_717_173_091,
            ),
            address_name_surface_binding(
                beta_surface_binding_id,
                "ens:beta.eth",
                beta_resource_id,
                "0xrole-beta",
                92,
                1_717_173_092,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                address,
                "ens:beta.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "beta.eth",
                "beta.eth",
                "node:beta.eth",
                beta_surface_binding_id,
                beta_resource_id,
                None,
                92,
            ),
            address_name_current_row(
                address,
                "ens:alpha.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "alpha.eth",
                "alpha.eth",
                "node:alpha.eth",
                alpha_surface_binding_id,
                alpha_resource_id,
                None,
                91,
            ),
        ],
    )
    .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:alpha.eth",
            "alpha.eth",
            "alpha.eth",
            "node:alpha.eth",
            alpha_surface_binding_id,
            alpha_resource_id,
            None,
            94,
            json!({
                "control": {
                    "status": "active",
                    "expiry": "2026-10-01T00:00:00Z",
                },
                "record_inventory": {
                    "status": "supported",
                    "count": 1,
                },
            }),
        ))
        .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:beta.eth",
            "beta.eth",
            "beta.eth",
            "node:beta.eth",
            beta_surface_binding_id,
            beta_resource_id,
            None,
            95,
            json!({
                "control": {
                    "status": "wrapped",
                    "expiry": "2026-11-01T00:00:00Z",
                },
                "record_inventory": {
                    "status": "supported",
                    "count": 2,
                },
            }),
        ))
        .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[declared_child_row(
            "ens:alpha.eth",
            "ens:one.alpha.eth",
            "one.alpha.eth",
            "node:one.alpha.eth",
            901,
            93,
        )],
    )
    .await?;
    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            permission_current_row(
                alpha_resource_id,
                alpha_subject,
                PermissionScope::Resource,
                15,
                96,
            ),
            permission_current_row(
                beta_resource_id,
                beta_subject,
                PermissionScope::Registry,
                16,
                97,
            ),
        ],
    )
    .await?;

    let base_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{address}/names?include=role_summary"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("role summary base request failed")?;
    assert_eq!(base_response.status(), StatusCode::OK);
    let base_payload: AddressNamesResponse = read_json(base_response).await?;

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{address}/names?include=role_summary&page_size=1"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("role summary first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: AddressNamesResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("role summary first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{address}/names?include=role_summary&page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("role summary second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: AddressNamesResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{address}/names?include=role_summary&page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("role summary replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: AddressNamesResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &base_payload.data,
        &base_payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "display_name_asc",
        50,
        1,
    );
    assert_eq!(
        first_page_payload.data[0].get("subname_count"),
        Some(&json!(1))
    );
    assert_eq!(
        first_page_payload.data[0].get("record_count"),
        Some(&json!(1))
    );
    assert_eq!(
        first_page_payload.data[0].get("status"),
        Some(&json!("active"))
    );
    assert_eq!(
        first_page_payload.data[0].get("role_summary"),
        Some(&json!({
            "subjects": [{
                "subject": alpha_subject,
                "scopes": [{
                    "scope": {
                        "kind": "resource",
                        "detail": {},
                    },
                    "effective_powers": ["set_resolver", "set_records"],
                }],
            }]
        }))
    );
    assert_eq!(
        second_page_payload.data[0].get("subname_count"),
        Some(&json!(0))
    );
    assert_eq!(
        second_page_payload.data[0].get("record_count"),
        Some(&json!(2))
    );
    assert_eq!(
        second_page_payload.data[0].get("status"),
        Some(&json!("wrapped"))
    );
    assert_eq!(
        second_page_payload.data[0].get("role_summary"),
        Some(&json!({
            "subjects": [{
                "subject": beta_subject,
                "scopes": [{
                    "scope": {
                        "kind": "registry",
                        "detail": {},
                    },
                    "effective_powers": ["set_resolver", "create_subnames"],
                }],
            }]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_include_role_summary_reads_ensv2_projection_outputs_with_exact_name_support()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:bob.alice.eth";
    let normalized_name = "bob.alice.eth";
    let resource_id = Uuid::from_u128(0x8c10);
    let token_lineage_id = Uuid::from_u128(0x8c11);
    let surface_binding_id = Uuid::from_u128(0x8c12);
    let registrant = "0x0000000000000000000000000000000000000b0b";
    let controller = "0x0000000000000000000000000000000000000c0c";
    let resolver_address = "0x0000000000000000000000000000000000000abc";

    database
        .seed_ensv2_address_names_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
            registrant,
            controller,
        )
        .await?;
    database
        .rebuild_address_names_current(Some(controller))
        .await?;

    let inventory_row = record_inventory_current_row(logical_name_id, resource_id);
    let selector_count = inventory_row
        .selectors
        .as_array()
        .expect("record_inventory_current selectors must be an array")
        .len();
    database
        .insert_record_inventory_current_row(inventory_row)
        .await?;

    let mut name_row = address_name_name_current_row(
        logical_name_id,
        normalized_name,
        normalized_name,
        "namehash:bob.alice.eth",
        surface_binding_id,
        resource_id,
        Some(token_lineage_id),
        206,
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "ens_v2_registry",
            },
            "control": {
                "status": "active",
                "expiry": "2030-03-17T17:46:40Z",
                "registrant": registrant,
                "registry_owner": controller,
                "latest_event_kind": "AuthorityTransferred",
            },
            "resolver": {
                "chain_id": "ethereum-sepolia",
                "address": resolver_address,
                "latest_event_kind": "ResolverChanged",
            },
            "record_inventory": {
                "status": "supported",
                "count": selector_count,
            },
            "history": {
                "surface_head": null,
                "resource_head": null,
            },
        }),
    );
    name_row.binding_kind = Some(bigname_storage::SurfaceBindingKind::LinkedSubregistryPath);
    name_row.coverage = json!({
        "status": "full",
        "exhaustiveness": "authoritative",
        "source_classes_considered": ["ens_v2_registry_l1", "ens_v2_registrar_l1"],
        "unsupported_reason": null,
        "enumeration_basis": "exact_name_profile",
    });
    name_row.provenance = json!({
        "normalized_event_ids": [204, 205, 206],
        "raw_fact_refs": [{
            "kind": "raw_log",
            "chain_id": "ethereum-sepolia",
            "block_number": 206,
        }],
        "manifest_versions": [{
            "manifest_version": 11,
            "source_family": "ens_v2_registry_l1",
            "chain": "ethereum-sepolia",
            "deployment_epoch": "ens_v2_sepolia_dev",
        }],
        "execution_trace_id": null,
        "derivation_kind": "name_current_rebuild",
    });
    name_row.chain_positions = json!({
        "ethereum-sepolia": {
            "chain_id": "ethereum-sepolia",
            "block_number": 206,
            "block_hash": "0xensv2-regen",
            "timestamp": "2026-04-17T00:00:26Z",
        }
    });
    name_row.canonicality_summary = json!({
        "status": "finalized",
        "chains": {
            "ethereum-sepolia": "finalized",
        }
    });
    name_row.manifest_version = 11;
    database.insert_name_current_row(name_row).await?;

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(
                "ens:carol.bob.alice.eth",
                "carol.bob.alice.eth",
                "node:carol.bob.alice.eth",
                207,
            ),
            collection_name_surface(
                "ens:dave.bob.alice.eth",
                "dave.bob.alice.eth",
                "node:dave.bob.alice.eth",
                208,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            ensv2_declared_child_row(
                logical_name_id,
                "ens:carol.bob.alice.eth",
                "carol.bob.alice.eth",
                "node:carol.bob.alice.eth",
                801,
                207,
            ),
            ensv2_declared_child_row(
                logical_name_id,
                "ens:dave.bob.alice.eth",
                "dave.bob.alice.eth",
                "node:dave.bob.alice.eth",
                802,
                208,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            permission_current_row(resource_id, controller, PermissionScope::Resource, 11, 209),
            permission_current_row(
                resource_id,
                controller,
                PermissionScope::Resolver {
                    chain_id: "ethereum-sepolia".to_owned(),
                    resolver_address: resolver_address.to_owned(),
                },
                12,
                210,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[bigname_storage::ResolverCurrentRow {
            chain_id: "ethereum-sepolia".to_owned(),
            resolver_address: resolver_address.to_owned(),
            declared_summary: json!({
                "bindings": {
                    "status": "supported",
                    "count": 1,
                    "items": [{
                        "logical_name_id": logical_name_id,
                        "canonical_display_name": normalized_name,
                        "normalized_name": normalized_name,
                        "namehash": "namehash:bob.alice.eth",
                        "resource_id": resource_id.to_string(),
                        "surface_binding_id": surface_binding_id.to_string(),
                        "binding_kind": "linked_subregistry_path",
                    }],
                },
                "aliases": {
                    "status": "supported",
                    "count": 0,
                    "items": [],
                },
                "permissions": {
                    "status": "supported",
                    "count": 1,
                    "items": [{
                        "resource_id": resource_id.to_string(),
                        "subject": controller,
                        "effective_powers": ["set_resolver", "create_subnames"],
                        "grant_source": {
                            "kind": "normalized_event",
                            "event_identity": "api-test:ensv2:resolver-permission",
                        },
                        "revocation_source": null,
                    }],
                },
                "role_holders": {
                    "status": "supported",
                    "count": 1,
                    "items": [{
                        "subject": controller,
                        "resource_count": 1,
                        "permission_row_count": 1,
                        "effective_powers": ["create_subnames", "set_resolver"],
                        "resource_ids": [resource_id.to_string()],
                    }],
                },
                "event_summary": {
                    "status": "supported",
                    "count": 2,
                    "by_kind": {
                        "PermissionChanged": 1,
                        "ResolverChanged": 1,
                    },
                },
            }),
            provenance: json!({
                "normalized_event_ids": [209, 210],
                "raw_fact_refs": [{
                    "kind": "raw_log",
                    "chain_id": "ethereum-sepolia",
                    "block_number": 210,
                }],
                "manifest_versions": [{
                    "manifest_version": 11,
                    "source_family": "ens_v2_registry_l1",
                    "chain": "ethereum-sepolia",
                    "deployment_epoch": "ens_v2_sepolia_dev",
                }],
                "execution_trace_id": null,
                "derivation_kind": "resolver_current_rebuild",
            }),
            coverage: json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["ens_v2_registry_l1", "permissions_current"],
                "unsupported_reason": null,
                "enumeration_basis": "resolver_target",
            }),
            chain_positions: json!({
                "ethereum-sepolia": {
                    "chain_id": "ethereum-sepolia",
                    "block_number": 210,
                    "block_hash": "0xensv2resolver",
                    "timestamp": "2026-04-17T00:00:30Z",
                }
            }),
            canonicality_summary: json!({
                "status": "finalized",
                "chains": {
                    "ethereum-sepolia": "finalized",
                }
            }),
            manifest_version: 11,
            last_recomputed_at: timestamp(1_717_182_210),
        }],
    )
    .await?;

    let base_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/addresses/{controller}/names?namespace=ens"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 base address names request failed")?;
    let include_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{controller}/names?namespace=ens&include=role_summary"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 role_summary request failed")?;
    assert_eq!(base_response.status(), StatusCode::OK);
    assert_eq!(include_response.status(), StatusCode::OK);

    let base_payload: AddressNamesResponse = read_json(base_response).await?;
    let payload: AddressNamesResponse = read_json(include_response).await?;

    assert_eq!(payload.coverage, base_payload.coverage);
    assert_eq!(payload.page, base_payload.page);
    assert_eq!(payload.declared_state, base_payload.declared_state);
    assert_eq!(payload.consistency, base_payload.consistency);
    assert_eq!(payload.data.len(), 1);
    assert_eq!(
        payload.data[0].get("logical_name_id"),
        Some(&json!(logical_name_id))
    );
    assert_eq!(
        payload.data[0].get("binding_kind"),
        Some(&json!("linked_subregistry_path"))
    );
    assert_eq!(
        payload.data[0].get("relation_facets"),
        Some(&json!(["effective_controller"]))
    );
    assert_eq!(payload.data[0].get("status"), Some(&json!("active")));
    assert_eq!(
        payload.data[0].get("expiry"),
        Some(&json!("2030-03-17T17:46:40Z"))
    );
    assert_eq!(payload.data[0].get("subname_count"), Some(&json!(2)));
    assert_eq!(
        payload.data[0].get("record_count"),
        Some(&json!(selector_count))
    );
    assert_eq!(
        payload.data[0].get("role_summary"),
        Some(&json!({
            "subjects": [{
                "subject": controller,
                "scopes": [
                    {
                        "scope": {
                            "kind": "resolver",
                            "detail": {
                                "chain_id": "ethereum-sepolia",
                                "resolver_address": resolver_address,
                            },
                        },
                        "effective_powers": ["set_resolver", "create_subnames"],
                    },
                    {
                        "scope": {
                            "kind": "resource",
                            "detail": {},
                        },
                        "effective_powers": ["set_resolver", "set_records"],
                    },
                ],
            }]
        }))
    );
    assert!(base_payload.provenance.is_null());
    assert!(
        payload.provenance.get("execution_trace_id").is_none(),
        "declared-only role-summary provenance must omit execution_trace_id"
    );
    let manifest_versions = payload.provenance["manifest_versions"]
        .as_array()
        .expect("role-summary provenance manifest_versions must be an array");
    assert!(manifest_versions.iter().any(|manifest| {
        manifest.get("source_family") == Some(&json!("ens_v2_registry_l1"))
    }));
    let normalized_event_ids = payload.provenance["normalized_event_ids"]
        .as_array()
        .expect("role-summary provenance normalized_event_ids must be an array");
    assert!(normalized_event_ids.contains(&json!("210")));
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_include_role_summary_reads_basenames_permissions_from_permission_changed_rows()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:management-only.base.eth";
    let resource_id = Uuid::from_u128(0x8b10);
    let token_lineage_id = Uuid::from_u128(0x8b11);
    let surface_binding_id = Uuid::from_u128(0x8b12);
    let resolver_address = "0x0000000000000000000000000000000000000abc";
    let subject = BasenamesControlVectorScenario::ManagementOnly.current_effective_controller();

    database
        .seed_basenames_control_vector_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
            BasenamesControlVectorScenario::ManagementOnly,
        )
        .await?;
    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block(
                "base-mainnet",
                "0xbase-permission-3",
                None,
                106,
                1_717_181_706,
            ),
            raw_block(
                "base-mainnet",
                "0xbase-permission-4",
                None,
                107,
                1_717_181_707,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            bigname_storage::NormalizedEvent {
                event_identity: "api-test:basenames:resource-permission".to_owned(),
                namespace: "basenames".to_owned(),
                logical_name_id: Some(logical_name_id.to_owned()),
                resource_id: Some(resource_id),
                event_kind: "PermissionChanged".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                manifest_version: 5,
                source_manifest_id: None,
                chain_id: Some("base-mainnet".to_owned()),
                block_number: Some(106),
                block_hash: Some("0xbase-permission-3".to_owned()),
                transaction_hash: Some("0xtxbasepermission3".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:basenames:resource-permission"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "subject": subject,
                    "scope": {
                        "kind": "resource",
                    },
                    "effective_powers": ["resource_control"],
                    "grant_source": {
                        "kind": "normalized_event",
                        "event_identity": "api-test:basenames:resource-permission",
                    },
                    "revocation_source": null,
                    "inheritance_path": [],
                    "transfer_behavior": {},
                }),
            },
            bigname_storage::NormalizedEvent {
                event_identity: "api-test:basenames:resolver-permission-role-summary".to_owned(),
                namespace: "basenames".to_owned(),
                logical_name_id: Some(logical_name_id.to_owned()),
                resource_id: Some(resource_id),
                event_kind: "PermissionChanged".to_owned(),
                source_family: "basenames_base_resolver".to_owned(),
                manifest_version: 6,
                source_manifest_id: None,
                chain_id: Some("base-mainnet".to_owned()),
                block_number: Some(107),
                block_hash: Some("0xbase-permission-4".to_owned()),
                transaction_hash: Some("0xtxbasepermission4".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:basenames:resolver-permission-role-summary"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "subject": subject,
                    "scope": {
                        "kind": "resolver",
                        "chain_id": "base-mainnet",
                        "resolver_address": resolver_address,
                    },
                    "effective_powers": ["resolver_control"],
                    "grant_source": {
                        "kind": "normalized_event",
                        "event_identity": "api-test:basenames:resolver-permission-role-summary",
                    },
                    "revocation_source": null,
                    "inheritance_path": [],
                    "transfer_behavior": {},
                }),
            },
        ],
    )
    .await?;
    database
        .rebuild_permissions_current(Some(resource_id))
        .await?;
    database
        .rebuild_address_names_current(Some(subject))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/addresses/{subject}/names?namespace=basenames&include=role_summary"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("Basenames role_summary request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: AddressNamesResponse = read_json(response).await?;
    assert_eq!(payload.data.len(), 1);
    assert_eq!(
        payload.data[0].get("logical_name_id"),
        Some(&json!(logical_name_id))
    );
    assert_eq!(
        payload.data[0].get("relation_facets"),
        Some(&json!(["effective_controller"]))
    );
    assert_eq!(
        payload.data[0].get("role_summary"),
        Some(&json!({
            "subjects": [{
                "subject": subject,
                "scopes": [
                    {
                        "scope": {
                            "kind": "resolver",
                            "detail": {
                                "chain_id": "base-mainnet",
                                "resolver_address": resolver_address,
                            },
                        },
                        "effective_powers": ["resolver_control"],
                    },
                    {
                        "scope": {
                            "kind": "resource",
                            "detail": {},
                        },
                        "effective_powers": ["resource_control"],
                    }
                ],
            }]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_names_rejects_unknown_include_values() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/addresses/0x0000000000000000000000000000000000000abc/names?include=role_summary,unknown")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("invalid include request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "include must contain only role_summary"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_permissions_keyset_pagination_preserves_full_filter_summary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource_id = Uuid::from_u128(0xa390);
    let first_subject = "0x0000000000000000000000000000000000000a01";
    let second_subject = "0x0000000000000000000000000000000000000b02";

    bigname_storage::upsert_resources(&database.pool, &[resource(resource_id)]).await?;
    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            permission_current_row(
                resource_id,
                first_subject,
                PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000aaa".to_owned(),
                },
                12,
                81,
            ),
            permission_current_row(
                resource_id,
                first_subject,
                PermissionScope::Resource,
                13,
                82,
            ),
            permission_current_row(
                resource_id,
                second_subject,
                PermissionScope::Registry,
                14,
                83,
            ),
        ],
    )
    .await?;

    let base_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/resources/{resource_id}/permissions"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions request failed")?;
    assert_eq!(base_response.status(), StatusCode::OK);
    let base_payload: ResourcePermissionsResponse = read_json(base_response).await?;

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/resources/{resource_id}/permissions?page_size=2"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: ResourcePermissionsResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("resource permissions first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/resources/{resource_id}/permissions?page_size=2&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: ResourcePermissionsResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/resources/{resource_id}/permissions?page_size=2&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: ResourcePermissionsResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &base_payload.data,
        &base_payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "subject_scope_asc",
        50,
        2,
    );
    assert_resource_permissions_collection_metadata_eq(&base_payload, &first_page_payload);
    assert_resource_permissions_collection_metadata_eq(&base_payload, &second_page_payload);
    assert_resource_permissions_collection_metadata_eq(&base_payload, &replay_page_payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_permissions_rejects_malformed_wrong_route_filter_and_stale_cursors()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource_id = Uuid::from_u128(0xb290);
    let first_subject = "0x0000000000000000000000000000000000000c11";
    let second_subject = "0x0000000000000000000000000000000000000c22";

    bigname_storage::upsert_resources(&database.pool, &[resource(resource_id)]).await?;
    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            permission_current_row(
                resource_id,
                first_subject,
                PermissionScope::Resource,
                17,
                111,
            ),
            permission_current_row(
                resource_id,
                second_subject,
                PermissionScope::Registry,
                18,
                112,
            ),
        ],
    )
    .await?;

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/resources/{resource_id}/permissions?page_size=1"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions cursor seed request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: ResourcePermissionsResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("resource permissions first page must include next_cursor");

    assert_invalid_cursor_request(
        database.app_state(),
        format!("/v1/resources/{resource_id}/permissions?page_size=1&cursor=not-a-cursor"),
    )
    .await?;

    let wrong_route_cursor = rewrite_cursor(&cursor, |envelope| {
        envelope.route = "/v1/addresses/{address}/names".to_owned();
    });
    assert_invalid_cursor_request(
        database.app_state(),
        format!("/v1/resources/{resource_id}/permissions?page_size=1&cursor={wrong_route_cursor}"),
    )
    .await?;

    assert_invalid_cursor_request(
        database.app_state(),
        format!(
            "/v1/resources/{resource_id}/permissions?subject={first_subject}&page_size=1&cursor={cursor}"
        ),
    )
    .await?;

    let stale_cursor = rewrite_cursor(&cursor, |envelope| {
        envelope.item.insert(
            "subject".to_owned(),
            "0x0000000000000000000000000000000000000bad".to_owned(),
        );
        envelope
            .item
            .insert("scope".to_owned(), "resource".to_owned());
    });
    assert_invalid_cursor_request(
        database.app_state(),
        format!("/v1/resources/{resource_id}/permissions?page_size=1&cursor={stale_cursor}"),
    )
    .await?;

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_returns_not_found_for_unsupported_namespace_without_storage_read() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/unknown/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_coverage_returns_not_found_for_unsupported_namespace_without_storage_read()
-> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/coverage/unknown/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("coverage request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_returns_internal_error_envelope_on_storage_failure() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        "failed to load current projection for name ens/alice.eth"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_coverage_returns_internal_error_envelope_on_storage_failure() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/coverage/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("coverage request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        "failed to load current projection for name ens/alice.eth"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

fn ensv2_declared_child_row(
    parent_logical_name_id: &str,
    child_logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    normalized_event_id: i64,
    block_number: i64,
) -> bigname_storage::ChildrenCurrentRow {
    let mut row = declared_child_row(
        parent_logical_name_id,
        child_logical_name_id,
        display_name,
        namehash,
        normalized_event_id,
        block_number,
    );
    row.provenance = json!({
        "normalized_event_ids": [normalized_event_id],
        "raw_fact_refs": [{
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
        }],
        "manifest_versions": [{
            "manifest_version": 7,
            "source_family": "ens_v2_registry_l1",
            "source_manifest_id": null,
        }],
        "execution_trace_id": null,
        "derivation_kind": "children_current_rebuild",
    });
    row.manifest_version = 7;
    row
}

fn compact_child_name_current_row(
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
    declared_summary: Value,
) -> bigname_storage::NameCurrentRow {
    let namespace = logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .expect("logical_name_id must include namespace");
    let chain_id = chain_id_for_namespace(namespace);
    let chain_slot = chain_slot_for_namespace(namespace);

    bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: namespace.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        namehash: namehash.to_owned(),
        surface_binding_id: None,
        resource_id: None,
        token_lineage_id: None,
        binding_kind: None,
        declared_summary,
        provenance: json!({
            "normalized_event_ids": [block_number],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 1,
                "source_family": source_family_for_namespace(namespace),
                "source_manifest_id": null,
            }],
            "execution_trace_id": null,
            "derivation_kind": "name_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": [source_family_for_namespace(namespace)],
            "enumeration_basis": "exact_name",
            "unsupported_reason": null,
        }),
        chain_positions: json!({
            chain_slot: {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": format!("0xname{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized"
            }
        }),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_717_173_000 + block_number),
    }
}
