// Integration coverage for the native subgraph-compatible GraphQL surface. The SDL-snapshot test
// guards the codegen contract the Manager depends on; the end-to-end test drives the four dashboard
// ops through `app_router` + `oneshot` against seeded Sepolia-v2-shaped rows.

const GRAPHQL_OWNER: &str = "0x000000000000000000000000000000000000000a";
const GRAPHQL_REGISTRANT: &str = "0x000000000000000000000000000000000000000b";
const GRAPHQL_RESOLVER: &str = "0x000000000000000000000000000000000000def0";
const GRAPHQL_ALICE_NAMEHASH: &str = "0xa11ce";
const GRAPHQL_BOB_NAMEHASH: &str = "0xb0b";
/// TokenHolder for the owner-fallback fixture names (carol, dave) — kept distinct from
/// `GRAPHQL_OWNER` so the dashboard-op tests' `owner_in` windows stay two-name stable.
const GRAPHQL_FALLBACK_HOLDER: &str = "0x000000000000000000000000000000000000000c";
/// Declared registrant for carol — exercises the `owner → registrant` non-null fallback and the
/// plural `registrant_in` filter.
const GRAPHQL_REGISTRANT_C: &str = "0x000000000000000000000000000000000000000d";
const GRAPHQL_CAROL_NAMEHASH: &str = "0xca401";
const GRAPHQL_DAVE_NAMEHASH: &str = "0xda4e";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

fn graphql_declared_summary(
    owner: &str,
    resolver: &str,
    authority_kind: &str,
    expiry: i64,
    created_at: i64,
) -> Value {
    json!({
        "registration": {
            "status": "active",
            "authority_kind": authority_kind,
            "registrant": owner,
            "expiry": expiry,
            "created_at": created_at,
        },
        "control": {
            "registry_owner": owner,
            "registrant": owner,
            "expiry": expiry,
        },
        "resolver": {
            "chain_id": "ethereum-mainnet",
            "address": resolver,
        }
    })
}

async fn seed_graphql_dashboard_fixture(database: &TestDatabase) -> Result<()> {
    let alice_tl = Uuid::from_u128(0x6_a001);
    let alice_res = Uuid::from_u128(0x6_a002);
    let alice_sb = Uuid::from_u128(0x6_a003);
    let bob_tl = Uuid::from_u128(0x6_b001);
    let bob_res = Uuid::from_u128(0x6_b002);
    let bob_sb = Uuid::from_u128(0x6_b003);

    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[
            address_name_token_lineage(alice_tl, "0xtl-alice", 411),
            address_name_token_lineage(bob_tl, "0xtl-bob", 412),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(alice_res, Some(alice_tl), "0xres-alice", 411),
            address_name_resource(bob_res, Some(bob_tl), "0xres-bob", 412),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            // `collection_name_surface` derives the surface `normalized_name` from this arg, and
            // `upsert_name_current_rows` validates it equals the row's normalized name — so pass the
            // normalized (lowercase) form here. The display-cased name lives on the name_current row.
            collection_name_surface("ens:alice.eth", "alice.eth", GRAPHQL_ALICE_NAMEHASH, 411),
            collection_name_surface("ens:bob.eth", "bob.eth", GRAPHQL_BOB_NAMEHASH, 412),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(alice_sb, "ens:alice.eth", alice_res, "0xsb-alice", 411, 1_717_174_011),
            address_name_surface_binding(bob_sb, "ens:bob.eth", bob_res, "0xsb-bob", 412, 1_717_174_012),
        ],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                GRAPHQL_OWNER,
                "ens:alice.eth",
                bigname_storage::AddressNameRelation::TokenHolder,
                "Alice.eth",
                "alice.eth",
                GRAPHQL_ALICE_NAMEHASH,
                alice_sb,
                alice_res,
                Some(alice_tl),
                411,
            ),
            address_name_current_row(
                GRAPHQL_OWNER,
                "ens:bob.eth",
                bigname_storage::AddressNameRelation::TokenHolder,
                "Bob.eth",
                "bob.eth",
                GRAPHQL_BOB_NAMEHASH,
                bob_sb,
                bob_res,
                Some(bob_tl),
                412,
            ),
            address_name_current_row(
                GRAPHQL_REGISTRANT,
                "ens:alice.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "Alice.eth",
                "alice.eth",
                GRAPHQL_ALICE_NAMEHASH,
                alice_sb,
                alice_res,
                Some(alice_tl),
                411,
            ),
        ],
    )
    .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:alice.eth",
            "Alice.eth",
            "alice.eth",
            GRAPHQL_ALICE_NAMEHASH,
            alice_sb,
            alice_res,
            Some(alice_tl),
            411,
            graphql_declared_summary(GRAPHQL_OWNER, GRAPHQL_RESOLVER, "ens_v2_registry", 1_900_000_000, 1_700_000_000),
        ))
        .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:bob.eth",
            "Bob.eth",
            "bob.eth",
            GRAPHQL_BOB_NAMEHASH,
            bob_sb,
            bob_res,
            Some(bob_tl),
            412,
            graphql_declared_summary(GRAPHQL_OWNER, GRAPHQL_RESOLVER, "registrar", 1_800_000_000, 1_650_000_000),
        ))
        .await?;

    seed_graphql_fallback_fixture(database).await?;
    Ok(())
}

/// Two extra names under `GRAPHQL_FALLBACK_HOLDER` exercising the degenerate summary shapes the
/// dashboard fixture never hits: carol has no declared owner (only a registrant — the middle leg
/// of the non-null `owner` fallback) and a real expiry; dave has no owner, registrant, expiry, or
/// created_at at all (zero-address fallback, epoch `createdAt`, NULL-ranked expiry sort).
async fn seed_graphql_fallback_fixture(database: &TestDatabase) -> Result<()> {
    let carol_tl = Uuid::from_u128(0x6_c001);
    let carol_res = Uuid::from_u128(0x6_c002);
    let carol_sb = Uuid::from_u128(0x6_c003);
    let dave_tl = Uuid::from_u128(0x6_d001);
    let dave_res = Uuid::from_u128(0x6_d002);
    let dave_sb = Uuid::from_u128(0x6_d003);

    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[
            address_name_token_lineage(carol_tl, "0xtl-carol", 413),
            address_name_token_lineage(dave_tl, "0xtl-dave", 414),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(carol_res, Some(carol_tl), "0xres-carol", 413),
            address_name_resource(dave_res, Some(dave_tl), "0xres-dave", 414),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:carol.eth", "carol.eth", GRAPHQL_CAROL_NAMEHASH, 413),
            collection_name_surface("ens:dave.eth", "dave.eth", GRAPHQL_DAVE_NAMEHASH, 414),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(carol_sb, "ens:carol.eth", carol_res, "0xsb-carol", 413, 1_717_174_013),
            address_name_surface_binding(dave_sb, "ens:dave.eth", dave_res, "0xsb-dave", 414, 1_717_174_014),
        ],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                GRAPHQL_FALLBACK_HOLDER,
                "ens:carol.eth",
                bigname_storage::AddressNameRelation::TokenHolder,
                "Carol.eth",
                "carol.eth",
                GRAPHQL_CAROL_NAMEHASH,
                carol_sb,
                carol_res,
                Some(carol_tl),
                413,
            ),
            address_name_current_row(
                GRAPHQL_FALLBACK_HOLDER,
                "ens:dave.eth",
                bigname_storage::AddressNameRelation::TokenHolder,
                "Dave.eth",
                "dave.eth",
                GRAPHQL_DAVE_NAMEHASH,
                dave_sb,
                dave_res,
                Some(dave_tl),
                414,
            ),
            address_name_current_row(
                GRAPHQL_REGISTRANT_C,
                "ens:carol.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "Carol.eth",
                "carol.eth",
                GRAPHQL_CAROL_NAMEHASH,
                carol_sb,
                carol_res,
                Some(carol_tl),
                413,
            ),
        ],
    )
    .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:carol.eth",
            "Carol.eth",
            "carol.eth",
            GRAPHQL_CAROL_NAMEHASH,
            carol_sb,
            carol_res,
            Some(carol_tl),
            413,
            json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "ens_v2_registry",
                    "expiry": 1_950_000_000,
                    "created_at": 1_710_000_000,
                },
                "control": {
                    "registrant": GRAPHQL_REGISTRANT_C,
                },
            }),
        ))
        .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:dave.eth",
            "Dave.eth",
            "dave.eth",
            GRAPHQL_DAVE_NAMEHASH,
            dave_sb,
            dave_res,
            Some(dave_tl),
            414,
            json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar",
                },
            }),
        ))
        .await?;
    Ok(())
}

/// Project a record inventory for alice: two text selector keys (avatar with a retained value,
/// url without one), a retained addr:60 entry, and no contenthash. The lookup key is derived with
/// the same pure function the GraphQL resolver uses (`resolution_record_inventory_lookup_key`)
/// applied to an identical rebuild of the fixture's `name_current` row, so the read is guaranteed
/// to hit this row.
async fn seed_alice_record_inventory(database: &TestDatabase) -> Result<()> {
    let alice_row = address_name_name_current_row(
        "ens:alice.eth",
        "Alice.eth",
        "alice.eth",
        GRAPHQL_ALICE_NAMEHASH,
        Uuid::from_u128(0x6_a003),
        Uuid::from_u128(0x6_a002),
        Some(Uuid::from_u128(0x6_a001)),
        411,
        graphql_declared_summary(GRAPHQL_OWNER, GRAPHQL_RESOLVER, "ens_v2_registry", 1_900_000_000, 1_700_000_000),
    );
    let (resource_id, record_version_boundary) =
        bigname_storage::resolution_record_inventory_lookup_key_any_chain(&alice_row)
            .expect("alice fixture row must yield a record-inventory lookup key");

    bigname_storage::upsert_record_inventory_current_rows(
        &database.pool,
        &[bigname_storage::RecordInventoryCurrentRow {
            resource_id,
            record_version_boundary,
            enumeration_basis: json!({
                "observed_selectors": true,
                "capability_declared_families": false,
                "globally_enumerable": false,
            }),
            selectors: json!([
                {"record_key": "addr:2147483658", "record_family": "addr", "selector_key": "2147483658", "cacheable": true},
                {"record_key": "addr:60", "record_family": "addr", "selector_key": "60", "cacheable": true},
                {"record_key": "contenthash", "record_family": "contenthash", "selector_key": null, "cacheable": true},
                {"record_key": "text:avatar", "record_family": "text", "selector_key": "avatar", "cacheable": true},
                {"record_key": "text:url", "record_family": "text", "selector_key": "url", "cacheable": true},
            ]),
            explicit_gaps: json!([]),
            unsupported_families: json!([]),
            last_change: None,
            entries: json!([
                {
                    "record_key": "addr:2147483658",
                    "record_family": "addr",
                    "selector_key": "2147483658",
                    "status": "success",
                    "value": "0x00000000000000000000000000000000000000bb",
                },
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "status": "success",
                    "value": "0x00000000000000000000000000000000000000aa",
                },
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "status": "success",
                    "value": "0xe30101701220aabbccdd",
                },
                {
                    "record_key": "text:avatar",
                    "record_family": "text",
                    "selector_key": "avatar",
                    "status": "success",
                    "value": "https://example.com/avatar.png",
                },
                {
                    "record_key": "text:url",
                    "record_family": "text",
                    "selector_key": "url",
                    "status": "unsupported",
                    "unsupported_reason": "value_not_retained_in_normalized_events",
                },
            ]),
            provenance: json!({"seed": "graphql_record_inventory_test"}),
            coverage: json!({"status": "full"}),
            chain_positions: json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 411,
                    "block_hash": "0xsb-alice",
                    "timestamp": "2026-04-17T00:00:03Z",
                }
            }),
            canonicality_summary: json!({"status": "finalized"}),
            manifest_version: 3,
            last_recomputed_at: sqlx::types::time::OffsetDateTime::from_unix_timestamp(
                1_717_171_717,
            )
            .expect("valid timestamp"),
        }],
    )
    .await?;
    Ok(())
}

/// Seed a record inventory for bob with records DISTINCT from alice's, so a multi-domain list
/// query can assert the DataLoader batch attributes each domain its own records (no cross-talk).
async fn seed_bob_record_inventory(database: &TestDatabase) -> Result<()> {
    let bob_row = address_name_name_current_row(
        "ens:bob.eth",
        "Bob.eth",
        "bob.eth",
        GRAPHQL_BOB_NAMEHASH,
        Uuid::from_u128(0x6_b003),
        Uuid::from_u128(0x6_b002),
        Some(Uuid::from_u128(0x6_b001)),
        412,
        graphql_declared_summary(GRAPHQL_OWNER, GRAPHQL_RESOLVER, "registrar", 1_800_000_000, 1_650_000_000),
    );
    let (resource_id, record_version_boundary) =
        bigname_storage::resolution_record_inventory_lookup_key_any_chain(&bob_row)
            .expect("bob fixture row must yield a record-inventory lookup key");

    bigname_storage::upsert_record_inventory_current_rows(
        &database.pool,
        &[bigname_storage::RecordInventoryCurrentRow {
            resource_id,
            record_version_boundary,
            enumeration_basis: json!({
                "observed_selectors": true,
                "capability_declared_families": false,
                "globally_enumerable": false,
            }),
            selectors: json!([
                {"record_key": "addr:60", "record_family": "addr", "selector_key": "60", "cacheable": true},
                {"record_key": "contenthash", "record_family": "contenthash", "selector_key": null, "cacheable": true},
            ]),
            explicit_gaps: json!([]),
            unsupported_families: json!([]),
            last_change: None,
            entries: json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "status": "success",
                    "value": "0x00000000000000000000000000000000000000cc",
                },
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "status": "success",
                    "value": "0xe301017012209988",
                },
            ]),
            provenance: json!({"seed": "graphql_record_inventory_test_bob"}),
            coverage: json!({"status": "full"}),
            chain_positions: json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 412,
                    "block_hash": "0xsb-bob",
                    "timestamp": "2026-04-17T00:00:04Z",
                }
            }),
            canonicality_summary: json!({"status": "finalized"}),
            manifest_version: 3,
            last_recomputed_at: sqlx::types::time::OffsetDateTime::from_unix_timestamp(
                1_717_171_718,
            )
            .expect("valid timestamp"),
        }],
    )
    .await?;
    Ok(())
}

/// The N+1 fix end-to-end: a `domains` list that selects `resolver` for every row resolves its
/// per-domain record reads through the DataLoader (one batched `record_inventory_current` query),
/// and each domain must carry ITS OWN records — the batch must not cross-attribute alice's records
/// to bob or vice versa.
#[tokio::test]
async fn graphql_domains_list_batches_resolver_records_per_domain() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_dashboard_fixture(&database).await?;
    seed_alice_record_inventory(&database).await?;
    seed_bob_record_inventory(&database).await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query Domains($where: DomainFilter!) {
            domains(where: $where, orderBy: name, orderDirection: asc) {
                name
                resolver { contentHash addresses { coinType address } }
            }
        }"#,
        json!({ "where": { "owner": GRAPHQL_OWNER } }),
    )
    .await?;

    let domains = payload["data"]["domains"]
        .as_array()
        .expect("domains array");
    let alice = domains
        .iter()
        .find(|domain| domain["name"] == json!("Alice.eth"))
        .expect("alice present");
    let bob = domains
        .iter()
        .find(|domain| domain["name"] == json!("Bob.eth"))
        .expect("bob present");

    assert_eq!(alice["resolver"]["contentHash"], json!("0xe30101701220aabbccdd"));
    assert_eq!(
        alice["resolver"]["addresses"],
        json!([
            { "coinType": 2_147_483_658u32, "address": "0x00000000000000000000000000000000000000bb" },
            { "coinType": 60, "address": "0x00000000000000000000000000000000000000aa" },
        ])
    );
    assert_eq!(bob["resolver"]["contentHash"], json!("0xe301017012209988"));
    assert_eq!(
        bob["resolver"]["addresses"],
        json!([{ "coinType": 60, "address": "0x00000000000000000000000000000000000000cc" }])
    );

    database.cleanup().await
}

/// A heterogeneous batched page: alice has a seeded inventory (hit) while bob is keyed but unseeded
/// (clean miss). In one request/one batch, the hit must keep its records and the miss must serve the
/// empty resolver shapes — the batch must not cross-attribute alice's records to bob or shift slots.
#[tokio::test]
async fn graphql_domains_list_batch_mixes_hit_and_clean_miss() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_dashboard_fixture(&database).await?;
    seed_alice_record_inventory(&database).await?;
    // Deliberately do NOT seed bob's inventory: bob is keyed (has a resolver) but has no row.

    let payload = post_graphql(
        database.app_state(),
        r#"query Domains($where: DomainFilter!) {
            domains(where: $where, orderBy: name, orderDirection: asc) {
                name
                resolver { address contentHash texts addresses { coinType address } }
            }
        }"#,
        json!({ "where": { "owner": GRAPHQL_OWNER } }),
    )
    .await?;

    let domains = payload["data"]["domains"]
        .as_array()
        .expect("domains array");
    let alice = domains
        .iter()
        .find(|domain| domain["name"] == json!("Alice.eth"))
        .expect("alice present");
    let bob = domains
        .iter()
        .find(|domain| domain["name"] == json!("Bob.eth"))
        .expect("bob present");

    // alice (hit) keeps her records.
    assert_eq!(alice["resolver"]["contentHash"], json!("0xe30101701220aabbccdd"));
    assert_eq!(
        alice["resolver"]["addresses"].as_array().map(Vec::len),
        Some(2)
    );
    // bob (clean miss in the same batch) serves the empty shapes, resolver address still present.
    assert_eq!(bob["resolver"]["address"], json!(GRAPHQL_RESOLVER));
    assert_eq!(bob["resolver"]["contentHash"], Value::Null);
    assert_eq!(bob["resolver"]["texts"], json!([]));
    assert_eq!(bob["resolver"]["addresses"], json!([]));

    database.cleanup().await
}

/// Reproduce the live Sepolia shape end-to-end: erin's `name_current` row is positioned on
/// `ethereum-sepolia` (which the mainnet-gated verified-resolution lookup rejects — the any-chain
/// key must serve it), and her inventory row is keyed by a *pointered* boundary (the worker fills
/// the anchoring event pointer; the caller-derived boundary is pointer-less), so the read only
/// succeeds through the anchor fallback. This is exactly the drift class found on live data.
async fn seed_erin_sepolia_record_fixture(database: &TestDatabase) -> Result<()> {
    let erin_tl = Uuid::from_u128(0x6_e001);
    let erin_res = Uuid::from_u128(0x6_e002);
    let erin_sb = Uuid::from_u128(0x6_e003);

    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[address_name_token_lineage(erin_tl, "0xtl-erin", 415)],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[address_name_resource(erin_res, Some(erin_tl), "0xres-erin", 415)],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[collection_name_surface("ens:erin.eth", "erin.eth", "0xe417", 415)],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[address_name_surface_binding(erin_sb, "ens:erin.eth", erin_res, "0xsb-erin", 415, 1_717_174_015)],
    )
    .await?;

    let mut erin_row = address_name_name_current_row(
        "ens:erin.eth",
        "Erin.eth",
        "erin.eth",
        "0xe417",
        erin_sb,
        erin_res,
        Some(erin_tl),
        415,
        graphql_declared_summary(GRAPHQL_OWNER, GRAPHQL_RESOLVER, "ens_v2_registry", 1_960_000_000, 1_720_000_000),
    );
    erin_row.chain_positions = json!({
        "ethereum-sepolia": {
            "chain_id": "ethereum-sepolia",
            "block_number": 10_940_282,
            "block_hash": "0xsepolia-erin",
            "timestamp": "2026-05-28T13:15:36Z",
        }
    });
    database.insert_name_current_row(erin_row.clone()).await?;

    let (resource_id, declared_boundary) =
        bigname_storage::resolution_record_inventory_lookup_key_any_chain(&erin_row)
            .expect("erin's sepolia row must yield an any-chain lookup key");
    // The worker keys its row with the anchoring event pointer filled in — same anchor, different
    // exact key. The GraphQL read must land on it via the anchor fallback.
    let mut pointered_boundary = declared_boundary.clone();
    pointered_boundary["normalized_event_id"] = json!(12_345);
    pointered_boundary["event_kind"] = json!("RecordChanged");

    bigname_storage::upsert_record_inventory_current_rows(
        &database.pool,
        &[bigname_storage::RecordInventoryCurrentRow {
            resource_id,
            record_version_boundary: pointered_boundary,
            enumeration_basis: json!({
                "observed_selectors": true,
                "capability_declared_families": false,
                "globally_enumerable": false,
            }),
            selectors: json!([
                {"record_key": "text:avatar", "record_family": "text", "selector_key": "avatar", "cacheable": true},
                {"record_key": "text:com.github", "record_family": "text", "selector_key": "com.github", "cacheable": true},
            ]),
            explicit_gaps: json!([]),
            unsupported_families: json!([]),
            last_change: None,
            entries: json!([
                {
                    "record_key": "text:avatar",
                    "record_family": "text",
                    "selector_key": "avatar",
                    "status": "success",
                    "value": "https://example.com/erin.png",
                },
                {
                    "record_key": "text:com.github",
                    "record_family": "text",
                    "selector_key": "com.github",
                    "status": "unsupported",
                    "unsupported_reason": "value_not_retained_in_normalized_events",
                },
            ]),
            provenance: json!({"seed": "graphql_sepolia_record_fixture"}),
            coverage: json!({"status": "full"}),
            chain_positions: json!({
                "ethereum-sepolia": {
                    "chain_id": "ethereum-sepolia",
                    "block_number": 10_940_282,
                    "block_hash": "0xsepolia-erin",
                    "timestamp": "2026-05-28T13:15:36Z",
                }
            }),
            canonicality_summary: json!({"status": "finalized"}),
            manifest_version: 3,
            last_recomputed_at: sqlx::types::time::OffsetDateTime::from_unix_timestamp(
                1_717_171_717,
            )
            .expect("valid timestamp"),
        }],
    )
    .await?;
    Ok(())
}

async fn post_graphql(state: AppState, query: &str, variables: Value) -> Result<Value> {
    let body = json!({ "query": query, "variables": variables }).to_string();
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .expect("graphql request must build"),
        )
        .await
        .context("graphql request failed")?;
    assert_eq!(response.status(), StatusCode::OK, "graphql HTTP status");
    let payload: Value = read_json(response).await?;
    assert!(
        payload.get("errors").is_none(),
        "unexpected graphql errors: {payload}"
    );
    Ok(payload)
}

#[tokio::test]
async fn graphql_endpoint_answers_cors_preflight_with_wildcard() -> Result<()> {
    // The Manager dev build calls the endpoint cross-origin, so the browser sends a preflight for
    // the application/json POST. The permissive CORS layer must answer it with a wildcard origin
    // (no credentials) or the browser blocks the real request.
    let database = TestDatabase::new_migrated().await?;
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/graphql")
                .header("origin", "https://manager.example")
                .header("access-control-request-method", "POST")
                .header("access-control-request-headers", "content-type")
                .body(Body::empty())
                .expect("preflight request must build"),
        )
        .await
        .context("cors preflight request failed")?;
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .and_then(|value| value.to_str().ok()),
        Some("*"),
        "GraphQL endpoint must answer the cross-origin preflight with a wildcard ACAO"
    );
    database.cleanup().await?;
    Ok(())
}

#[test]
fn graphql_sdl_matches_subgraph_codegen_contract() {
    let sdl = crate::graphql::subgraph_sdl();

    // Golden-file lock: ANY schema change (type/nullability/arg/enum drift) must show up as a
    // reviewable diff of the committed fixture. Re-bless intentional changes with:
    //   cargo test -p bigname-api print_subgraph_sdl_for_blessing -- --ignored --nocapture
    let golden = include_str!("fixtures/subgraph_schema.graphql");
    assert_eq!(
        sdl.trim(),
        golden.trim(),
        "SDL drifted from tests/fixtures/subgraph_schema.graphql — if intentional, re-bless via print_subgraph_sdl_for_blessing"
    );

    // Documentation-level pins for the load-bearing Manager-codegen contract points (redundant
    // with the golden file; kept so a failure names the broken contract directly).
    assert!(sdl.contains("owner: Account!"), "Domain.owner must be non-null");
    assert!(sdl.contains("createdAt: Int!"), "Domain.createdAt is codegen-pinned non-null");
    assert!(sdl.contains("address: String!"), "Resolver.address is codegen-pinned non-null");
}

/// Bless helper for the golden SDL fixture — prints the live SDL so it can be copied into
/// `tests/fixtures/subgraph_schema.graphql` when a schema change is intentional.
#[test]
#[ignore = "bless helper: prints the SDL for updating the golden fixture"]
fn print_subgraph_sdl_for_blessing() {
    println!("{}", crate::graphql::subgraph_sdl());
}

#[tokio::test]
async fn graphql_domain_op_returns_subgraph_domain_shape() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_dashboard_fixture(&database).await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query Domain($id: String!) {
            domain(id: $id) {
                id name normalizedName tokenId createdAt expiryDate
                owner { id }
                resolver { id address texts contentHash addresses { coinType address } }
            }
        }"#,
        json!({ "id": GRAPHQL_ALICE_NAMEHASH }),
    )
    .await?;

    let domain = &payload["data"]["domain"];
    assert_eq!(domain["id"], json!(GRAPHQL_ALICE_NAMEHASH));
    assert_eq!(domain["name"], json!("Alice.eth"));
    assert_eq!(domain["normalizedName"], json!("alice.eth"));
    assert_eq!(domain["owner"]["id"], json!(GRAPHQL_OWNER));
    // Codegen pins createdAt/expiryDate to Int — they must serialize as JSON numbers, not strings.
    assert_eq!(domain["createdAt"], json!(1_700_000_000));
    assert_eq!(domain["expiryDate"], json!(1_900_000_000));
    assert!(domain["createdAt"].is_number());
    // alice.eth has no record_inventory_current row seeded, so the resolver serves its empty
    // shapes: address present, texts/addresses empty, contentHash null. (Populated record fields
    // are covered by graphql_domain_resolver_serves_record_inventory_fields.)
    assert_eq!(domain["resolver"]["address"], json!(GRAPHQL_RESOLVER));
    assert_eq!(domain["resolver"]["texts"], json!([]));
    assert_eq!(domain["resolver"]["addresses"], json!([]));
    assert_eq!(domain["resolver"]["contentHash"], Value::Null);

    // The Manager passes the ENS name string as `id`; the name path resolves the same row that the
    // namehash query above reached via the fallback.
    let by_name = post_graphql(
        database.app_state(),
        r#"query Domain($id: String!) {
            domain(id: $id) { id name normalizedName owner { id } }
        }"#,
        json!({ "id": "alice.eth" }),
    )
    .await?;
    let by_name = &by_name["data"]["domain"];
    assert_eq!(by_name["id"], json!(GRAPHQL_ALICE_NAMEHASH));
    assert_eq!(by_name["name"], json!("Alice.eth"));
    assert_eq!(by_name["normalizedName"], json!("alice.eth"));
    assert_eq!(by_name["owner"]["id"], json!(GRAPHQL_OWNER));

    // Unknown id (neither a known name nor a known namehash) resolves to null without an error.
    let missing = post_graphql(
        database.app_state(),
        r#"query Domain($id: String!) { domain(id: $id) { id } }"#,
        json!({ "id": "0xdeadbeef" }),
    )
    .await?;
    assert_eq!(missing["data"]["domain"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn graphql_domains_op_offset_paginates_owner_in() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_dashboard_fixture(&database).await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query Domains($where: DomainFilter!, $first: Int, $skip: Int, $orderBy: Domain_orderBy, $orderDirection: OrderDirection) {
            domains(where: $where, first: $first, skip: $skip, orderBy: $orderBy, orderDirection: $orderDirection) {
                id name owner { id }
            }
        }"#,
        json!({
            "where": { "owner_in": [GRAPHQL_OWNER] },
            "first": 200,
            "skip": 0,
            "orderBy": "name",
            "orderDirection": "asc",
        }),
    )
    .await?;

    let domains = payload["data"]["domains"]
        .as_array()
        .expect("domains must be an array");
    assert_eq!(domains.len(), 2);
    assert_eq!(domains[0]["name"], json!("Alice.eth"));
    assert_eq!(domains[1]["name"], json!("Bob.eth"));
    assert_eq!(domains[0]["owner"]["id"], json!(GRAPHQL_OWNER));

    // Offset window is disjoint and stable.
    let second = post_graphql(
        database.app_state(),
        r#"query Domains($where: DomainFilter!, $first: Int, $skip: Int, $orderBy: Domain_orderBy, $orderDirection: OrderDirection) {
            domains(where: $where, first: $first, skip: $skip, orderBy: $orderBy, orderDirection: $orderDirection) { name }
        }"#,
        json!({
            "where": { "owner_in": [GRAPHQL_OWNER] },
            "first": 1,
            "skip": 1,
            "orderBy": "name",
            "orderDirection": "asc",
        }),
    )
    .await?;
    let page = second["data"]["domains"].as_array().expect("array");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0]["name"], json!("Bob.eth"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn graphql_connection_ops_return_total_counts() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_dashboard_fixture(&database).await?;

    // OwnedNamesCount: registrant B holds alice only.
    let owned = post_graphql(
        database.app_state(),
        r#"query OwnedNamesCount($where: RegistrationFilter!) {
            registrationConnection(first: 0, where: $where) { totalCount }
        }"#,
        json!({ "where": { "registrant": GRAPHQL_REGISTRANT } }),
    )
    .await?;
    assert_eq!(owned["data"]["registrationConnection"]["totalCount"], json!(1));

    // MigratedNamesCount: owner A holds alice + bob, but only alice is ens_v2_registry.
    let migrated = post_graphql(
        database.app_state(),
        r#"query MigratedNamesCount($where: DomainFilter!) {
            domainConnection(first: 0, where: $where) { totalCount }
        }"#,
        json!({ "where": { "owner": GRAPHQL_OWNER, "isMigrated": true } }),
    )
    .await?;
    assert_eq!(migrated["data"]["domainConnection"]["totalCount"], json!(1));

    // Without isMigrated, owner A holds both names.
    let all = post_graphql(
        database.app_state(),
        r#"query MigratedNamesCount($where: DomainFilter!) {
            domainConnection(first: 0, where: $where) { totalCount }
        }"#,
        json!({ "where": { "owner": GRAPHQL_OWNER } }),
    )
    .await?;
    assert_eq!(all["data"]["domainConnection"]["totalCount"], json!(2));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn graphql_domain_owner_falls_back_to_registrant_then_zero_address() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_dashboard_fixture(&database).await?;

    // Carol has no declared owner — `owner` resolves through the registrant leg of the fallback.
    let carol = post_graphql(
        database.app_state(),
        r#"query Domain($id: String!) { domain(id: $id) { owner { id } expiryDate } }"#,
        json!({ "id": "carol.eth" }),
    )
    .await?;
    assert_eq!(
        carol["data"]["domain"]["owner"]["id"],
        json!(GRAPHQL_REGISTRANT_C)
    );
    assert_eq!(carol["data"]["domain"]["expiryDate"], json!(1_950_000_000));

    // Dave has neither owner nor registrant — `owner` stays non-null via the zero-address
    // sentinel, the missing expiry serializes as null, and the missing created_at degenerates to
    // epoch rather than breaking the codegen-pinned `createdAt: Int!`.
    let dave = post_graphql(
        database.app_state(),
        r#"query Domain($id: String!) { domain(id: $id) { owner { id } expiryDate createdAt } }"#,
        json!({ "id": "dave.eth" }),
    )
    .await?;
    assert_eq!(dave["data"]["domain"]["owner"]["id"], json!(ZERO_ADDRESS));
    assert_eq!(dave["data"]["domain"]["expiryDate"], Value::Null);
    assert_eq!(dave["data"]["domain"]["createdAt"], json!(0));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn graphql_domain_resolver_serves_record_inventory_fields() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_dashboard_fixture(&database).await?;
    seed_alice_record_inventory(&database).await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query Domain($id: String!) {
            domain(id: $id) {
                resolver { id address texts contentHash addresses { coinType address } }
            }
        }"#,
        json!({ "id": "alice.eth" }),
    )
    .await?;

    let resolver = &payload["data"]["domain"]["resolver"];
    assert_eq!(resolver["address"], json!(GRAPHQL_RESOLVER));
    // texts are the text-family selector KEYS (subgraph semantics) — url is listed even though its
    // value was not retained.
    assert_eq!(resolver["texts"], json!(["avatar", "url"]));
    // addresses carry only retained (status=success) addr entries, parsed into {coinType, address}.
    // The first is an ENSIP-11 EVM coinType (0x80000000 | chainId) — beyond i32 by design.
    assert_eq!(
        resolver["addresses"],
        json!([
            { "coinType": 2_147_483_658u32, "address": "0x00000000000000000000000000000000000000bb" },
            { "coinType": 60, "address": "0x00000000000000000000000000000000000000aa" },
        ])
    );
    // The retained contenthash entry serves its raw multicodec payload hex.
    assert_eq!(resolver["contentHash"], json!("0xe30101701220aabbccdd"));

    // Bob has no inventory row — the resolver still serves the empty record shapes.
    let bob = post_graphql(
        database.app_state(),
        r#"query Domain($id: String!) {
            domain(id: $id) { resolver { texts contentHash addresses { coinType address } } }
        }"#,
        json!({ "id": "bob.eth" }),
    )
    .await?;
    assert_eq!(bob["data"]["domain"]["resolver"]["texts"], json!([]));
    assert_eq!(bob["data"]["domain"]["resolver"]["addresses"], json!([]));
    assert_eq!(bob["data"]["domain"]["resolver"]["contentHash"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn graphql_domain_resolver_serves_sepolia_records_via_anchor_fallback() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_erin_sepolia_record_fixture(&database).await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query Domain($id: String!) {
            domain(id: $id) { resolver { texts addresses { coinType address } contentHash } }
        }"#,
        json!({ "id": "erin.eth" }),
    )
    .await?;

    // The sepolia chain position must not gate the read (the live failure), and the pointered
    // projection boundary must be reached through the anchor fallback (the live drift).
    let resolver = &payload["data"]["domain"]["resolver"];
    assert_eq!(resolver["texts"], json!(["avatar", "com.github"]));
    assert_eq!(resolver["addresses"], json!([]));
    assert_eq!(resolver["contentHash"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn graphql_domains_op_orders_desc_and_ranks_null_expiry() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_dashboard_fixture(&database).await?;

    let names = |payload: &Value| -> Vec<String> {
        payload["data"]["domains"]
            .as_array()
            .expect("domains must be an array")
            .iter()
            .map(|domain| domain["name"].as_str().expect("name").to_owned())
            .collect()
    };

    // Descending name order inverts the ascending window the dashboard test pins.
    let desc = post_graphql(
        database.app_state(),
        r#"query Domains($where: DomainFilter!, $orderBy: Domain_orderBy, $orderDirection: OrderDirection) {
            domains(where: $where, orderBy: $orderBy, orderDirection: $orderDirection) { name }
        }"#,
        json!({
            "where": { "owner_in": [GRAPHQL_FALLBACK_HOLDER] },
            "orderBy": "name",
            "orderDirection": "desc",
        }),
    )
    .await?;
    assert_eq!(names(&desc), vec!["Dave.eth", "Carol.eth"]);

    // expiryDate asc ranks NULL expiries last (carol has 1.95e9, dave has none)…
    let expiry_asc = post_graphql(
        database.app_state(),
        r#"query Domains($where: DomainFilter!, $orderBy: Domain_orderBy, $orderDirection: OrderDirection) {
            domains(where: $where, orderBy: $orderBy, orderDirection: $orderDirection) { name }
        }"#,
        json!({
            "where": { "owner_in": [GRAPHQL_FALLBACK_HOLDER] },
            "orderBy": "expiryDate",
            "orderDirection": "asc",
        }),
    )
    .await?;
    assert_eq!(names(&expiry_asc), vec!["Carol.eth", "Dave.eth"]);

    // …and desc ranks them first ("no expiry" sorts as furthest-future).
    let expiry_desc = post_graphql(
        database.app_state(),
        r#"query Domains($where: DomainFilter!, $orderBy: Domain_orderBy, $orderDirection: OrderDirection) {
            domains(where: $where, orderBy: $orderBy, orderDirection: $orderDirection) { name }
        }"#,
        json!({
            "where": { "owner_in": [GRAPHQL_FALLBACK_HOLDER] },
            "orderBy": "expiryDate",
            "orderDirection": "desc",
        }),
    )
    .await?;
    assert_eq!(names(&expiry_desc), vec!["Dave.eth", "Carol.eth"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn graphql_filters_registrant_in_and_name_contains() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_dashboard_fixture(&database).await?;

    // Plural registrant_in unions the Registrant relation across both fixture registrants.
    let owned = post_graphql(
        database.app_state(),
        r#"query OwnedNamesCount($where: RegistrationFilter!) {
            registrationConnection(first: 0, where: $where) { totalCount }
        }"#,
        json!({ "where": { "registrant_in": [GRAPHQL_REGISTRANT, GRAPHQL_REGISTRANT_C] } }),
    )
    .await?;
    assert_eq!(owned["data"]["registrationConnection"]["totalCount"], json!(2));

    // name_contains narrows the holder's two names down to the substring match.
    let contains = post_graphql(
        database.app_state(),
        r#"query Domains($where: DomainFilter!) {
            domains(where: $where) { name }
        }"#,
        json!({ "where": { "owner_in": [GRAPHQL_FALLBACK_HOLDER], "name_contains": "aro" } }),
    )
    .await?;
    let matched = contains["data"]["domains"].as_array().expect("array");
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0]["name"], json!("Carol.eth"));

    database.cleanup().await?;
    Ok(())
}
