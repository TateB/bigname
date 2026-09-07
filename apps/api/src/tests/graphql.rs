// Integration coverage for the native subgraph-compatible GraphQL surface. The SDL-snapshot test
// guards the compatibility contract; the end-to-end tests drive the supported operations through
// `app_router` + `oneshot` against seeded Sepolia-v2-shaped rows.

const GRAPHQL_OWNER: &str = "0x000000000000000000000000000000000000000a";
const GRAPHQL_REGISTRANT: &str = "0x000000000000000000000000000000000000000b";
const GRAPHQL_RESOLVER: &str = "0x000000000000000000000000000000000000def0";
const GRAPHQL_ALICE_NAMEHASH: &str =
    "0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec";
const GRAPHQL_BOB_NAMEHASH: &str =
    "0xbe11069ec59144113f438b6ef59dd30497769fc2dce8e2b52e3ae71ac18e47c9";
/// TokenHolder for the owner-fallback fixture names (carol, dave) — kept distinct from
/// `GRAPHQL_OWNER` so the compatibility tests' `owner_in` windows stay two-name stable.
const GRAPHQL_FALLBACK_HOLDER: &str = "0x000000000000000000000000000000000000000c";
const GRAPHQL_OTHER_CHAIN_HOLDER: &str = "0x000000000000000000000000000000000000000e";
/// Declared registrant for carol — exercises the `owner → registrant` non-null fallback and the
/// plural `registrant_in` filter.
const GRAPHQL_REGISTRANT_C: &str = "0x000000000000000000000000000000000000000d";
const GRAPHQL_CAROL_NAMEHASH: &str =
    "0xe3a6b53d6803112ab111b8dd6a02bc89a802451dec3eaec120740e5ed87bd5cb";
const GRAPHQL_DAVE_NAMEHASH: &str =
    "0x2ca4a3098bf61a1886dac6774bfe4dccdd1477d99a6fdbac5b409549f281cbe9";
const GRAPHQL_ERIN_NAMEHASH: &str =
    "0x93b576b9c8b56a6b4c3041e60f742e3678cfec194a3d9e4f5c069c8a2d0d194a";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[tokio::test]
async fn graphql_preserves_stored_ensip15_normalized_name_bytes() -> Result<()> {
    const NORMALIZED_NAME: &str = "ᏣᎳᎩ.eth";

    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    seed_identity_name(
        &database,
        "ens:ᏣᎳᎩ.eth",
        NORMALIZED_NAME,
        NORMALIZED_NAME,
        &bigname_lookup::ens_namehash_hex(NORMALIZED_NAME)?,
        Uuid::from_u128(0x349_5001),
        Uuid::from_u128(0x349_5002),
        Uuid::from_u128(0x349_5003),
        GRAPHQL_OWNER,
        bigname_storage::AddressNameRelation::TokenHolder,
        410,
    )
    .await?;
    let filtered = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) {
            domains(where: $where) { name }
        }"#,
        json!({ "where": { "name_contains": "Ꮳ" } }),
    )
    .await?;
    assert_eq!(filtered["data"]["domains"].as_array().map(Vec::len), Some(1));
    assert_eq!(filtered["data"]["domains"][0]["name"], json!(NORMALIZED_NAME));

    let lowercase_cherokee = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) {
            domains(where: $where) { name }
        }"#,
        json!({ "where": { "name_contains": "ꮳꮃꭹ" } }),
    )
    .await?;
    assert_eq!(
        lowercase_cherokee["data"]["domains"]
            .as_array()
            .map(Vec::len),
        Some(0)
    );

    let original_namehash = bigname_lookup::ens_namehash_hex(NORMALIZED_NAME)?;
    const STORED_NAME: &str = "ᏣᎳᎩ bad.eth";
    sqlx::query("UPDATE bigname_phase.name_current SET raw_name = $1 WHERE raw_name = $2")
        .bind(STORED_NAME)
        .bind(NORMALIZED_NAME)
        .execute(&database.pool)
        .await?;
    let stored_raw_name: String = sqlx::query_scalar(
        "SELECT raw_name FROM bigname_phase.name_current WHERE raw_name = $1",
    )
    .bind(STORED_NAME)
    .fetch_one(&database.pool)
    .await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query Domain($id: ID!) { domain(id: $id) { name normalizedName } }"#,
        json!({"id": original_namehash}),
    )
    .await?;
    assert_eq!(payload["data"]["domain"]["name"], json!(stored_raw_name));
    assert_eq!(
        payload["data"]["domain"]["normalizedName"],
        json!(stored_raw_name)
    );

    database.cleanup().await
}

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

async fn seed_graphql_compat_fixture(database: &TestDatabase) -> Result<()> {
    let alice_tl = Uuid::from_u128(0x6_a001);
    let alice_res = Uuid::from_u128(0x6_a002);
    let alice_sb = Uuid::from_u128(0x6_a003);
    let bob_tl = Uuid::from_u128(0x6_b001);
    let bob_res = Uuid::from_u128(0x6_b002);
    let bob_sb = Uuid::from_u128(0x6_b003);

    upsert_test_token_lineages(
        &database.pool,
        &[
            address_name_token_lineage(alice_tl, "0xname19b", 411),
            address_name_token_lineage(bob_tl, "0xname19c", 412),
        ],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[
            address_name_resource(alice_res, Some(alice_tl), "0xres-alice", 411),
            address_name_resource(bob_res, Some(bob_tl), "0xres-bob", 412),
        ],
    )
    .await?;
    upsert_test_name_surfaces(
        &database.pool,
        &[
            // Phase serving projections admit and retain the normalized bytes from these surfaces.
            collection_name_surface("ens:alice.eth", "alice.eth", GRAPHQL_ALICE_NAMEHASH, 411),
            collection_name_surface("ens:bob.eth", "bob.eth", GRAPHQL_BOB_NAMEHASH, 412),
        ],
    )
    .await?;
    upsert_test_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(alice_sb, "ens:alice.eth", alice_res, "0xsb-alice", 411, 1_717_174_011),
            address_name_surface_binding(bob_sb, "ens:bob.eth", bob_res, "0xsb-bob", 412, 1_717_174_012),
        ],
    )
    .await?;
    upsert_phase_address_names_current_rows(
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
                GRAPHQL_OWNER,
                "ens:alice.eth",
                bigname_storage::AddressNameRelation::EffectiveController,
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
                bigname_storage::AddressNameRelation::EffectiveController,
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
/// compatibility fixture never hits: carol has no declared owner (only a registrant — the middle leg
/// of the non-null `owner` fallback) and a real expiry; dave has no owner, registrant, expiry, or
/// declared created_at at all (zero-address fallback, epoch `createdAt`, NULL-ranked expiry sort).
async fn seed_graphql_fallback_fixture(database: &TestDatabase) -> Result<()> {
    let carol_tl = Uuid::from_u128(0x6_c001);
    let carol_res = Uuid::from_u128(0x6_c002);
    let carol_sb = Uuid::from_u128(0x6_c003);
    let dave_tl = Uuid::from_u128(0x6_d001);
    let dave_res = Uuid::from_u128(0x6_d002);
    let dave_sb = Uuid::from_u128(0x6_d003);

    upsert_test_token_lineages(
        &database.pool,
        &[
            address_name_token_lineage(carol_tl, "0xname19d", 413),
            address_name_token_lineage(dave_tl, "0xname19e", 414),
        ],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[
            address_name_resource(carol_res, Some(carol_tl), "0xres-carol", 413),
            address_name_resource(dave_res, Some(dave_tl), "0xres-dave", 414),
        ],
    )
    .await?;
    upsert_test_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:carol.eth", "carol.eth", GRAPHQL_CAROL_NAMEHASH, 413),
            collection_name_surface("ens:dave.eth", "dave.eth", GRAPHQL_DAVE_NAMEHASH, 414),
        ],
    )
    .await?;
    upsert_test_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(carol_sb, "ens:carol.eth", carol_res, "0xsb-carol", 413, 1_717_174_013),
            address_name_surface_binding(dave_sb, "ens:dave.eth", dave_res, "0xsb-dave", 414, 1_717_174_014),
        ],
    )
    .await?;
    upsert_phase_address_names_current_rows(
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
    sqlx::query(
        r#"
        UPDATE bigname_phase.chain_lineage lineage
        SET block_timestamp = '1970-01-01T00:00:00Z'::timestamptz
        FROM name_surfaces surface
        WHERE surface.logical_name_id = 'ens:dave.eth'
          AND lineage.chain_id = surface.chain_id
          AND lineage.block_hash = surface.block_hash
        "#,
    )
    .execute(&database.pool)
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
    let (resource_id, mut record_version_boundary) =
        bigname_storage::resolution_record_inventory_lookup_key_any_chain(&alice_row)
            .expect("alice fixture row must yield a record-inventory lookup key");
    record_version_boundary["logical_name_id"] =
        json!(format!("ens:{GRAPHQL_ALICE_NAMEHASH}"));

    upsert_phase_record_inventory_current_rows(
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
    let (resource_id, mut record_version_boundary) =
        bigname_storage::resolution_record_inventory_lookup_key_any_chain(&bob_row)
            .expect("bob fixture row must yield a record-inventory lookup key");
    record_version_boundary["logical_name_id"] = json!(format!("ens:{GRAPHQL_BOB_NAMEHASH}"));

    upsert_phase_record_inventory_current_rows(
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
    seed_graphql_compat_fixture(&database).await?;
    seed_alice_record_inventory(&database).await?;
    seed_bob_record_inventory(&database).await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) {
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
        .find(|domain| domain["name"] == json!("alice.eth"))
        .expect("alice present");
    let bob = domains
        .iter()
        .find(|domain| domain["name"] == json!("bob.eth"))
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
    seed_graphql_compat_fixture(&database).await?;
    seed_alice_record_inventory(&database).await?;
    // Deliberately do NOT seed bob's inventory: bob is keyed (has a resolver) but has no row.

    let payload = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) {
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
        .find(|domain| domain["name"] == json!("alice.eth"))
        .expect("alice present");
    let bob = domains
        .iter()
        .find(|domain| domain["name"] == json!("bob.eth"))
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
/// key must serve it), and her inventory row is keyed by a *pointered* boundary (projection fills
/// the anchoring event pointer; the caller-derived boundary is pointer-less), so the read only
/// succeeds through the anchor fallback. This is exactly the drift class found on live data.
async fn seed_erin_sepolia_record_fixture(database: &TestDatabase) -> Result<()> {
    let erin_tl = Uuid::from_u128(0x6_e001);
    let erin_res = Uuid::from_u128(0x6_e002);
    let erin_sb = Uuid::from_u128(0x6_e003);

    upsert_test_token_lineages(
        &database.pool,
        &[address_name_token_lineage(erin_tl, "0xname19f", 415)],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(erin_res, Some(erin_tl), "0xres-erin", 415)],
    )
    .await?;
    upsert_test_name_surfaces(
        &database.pool,
        &[collection_name_surface(
            "ens:erin.eth",
            "erin.eth",
            GRAPHQL_ERIN_NAMEHASH,
            415,
        )],
    )
    .await?;
    upsert_test_surface_bindings(
        &database.pool,
        &[address_name_surface_binding(erin_sb, "ens:erin.eth", erin_res, "0xsb-erin", 415, 1_717_174_015)],
    )
    .await?;

    let mut erin_row = address_name_name_current_row(
        "ens:erin.eth",
        "Erin.eth",
        "erin.eth",
        GRAPHQL_ERIN_NAMEHASH,
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
    // Projection keys its row with the anchoring event pointer filled in — same anchor, different
    // exact key. The GraphQL read must land on it via the anchor fallback.
    let mut pointered_boundary = declared_boundary.clone();
    pointered_boundary["logical_name_id"] = json!(format!("ens:{GRAPHQL_ERIN_NAMEHASH}"));
    pointered_boundary["normalized_event_id"] = json!(12_345);
    pointered_boundary["event_kind"] = json!("RecordChanged");

    upsert_phase_record_inventory_current_rows(
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
    let payload = post_graphql_allow_errors(state, query, variables).await?;
    assert!(
        payload.get("errors").is_none(),
        "unexpected graphql errors: {payload}"
    );
    Ok(payload)
}

async fn post_graphql_allow_errors(
    state: AppState,
    query: &str,
    variables: Value,
) -> Result<Value> {
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
    read_json(response).await
}

#[tokio::test]
async fn graphql_endpoint_answers_cors_preflight_with_wildcard() -> Result<()> {
    // A browser client can call the endpoint cross-origin, so the browser sends a preflight for
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
fn graphql_sdl_matches_subgraph_compatibility_contract() {
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

    // Documentation-level pins for load-bearing compatibility contract points (redundant with the
    // golden file; kept so a failure names the broken contract directly).
    assert!(sdl.contains("owner: Account!"), "Domain.owner must be non-null");
    assert!(sdl.contains("createdAt: BigInt!"), "Domain.createdAt must be non-null");
    assert!(
        sdl.contains("type Resolver {\n\tid: ID!\n\taddress: Bytes!"),
        "Resolver.address must be non-null Bytes"
    );
    assert!(sdl.contains("type Query {"), "the query root must be Query");
    assert!(sdl.contains("domain(id: ID!"), "domain.id must be ID!");
    assert!(!sdl.contains("BigDecimal"), "unused BigDecimal must stay absent");
}

/// Bless helper for the golden SDL fixture — prints the live SDL so it can be copied into
/// `tests/fixtures/subgraph_schema.graphql` when a schema change is intentional.
#[test]
#[ignore = "bless helper: prints the SDL for updating the golden fixture"]
fn print_subgraph_sdl_for_blessing() {
    println!("{}", crate::graphql::subgraph_sdl());
}

#[tokio::test]
async fn graphql_meta_reports_the_served_head_and_real_readiness() -> Result<()> {
    const PARENT_HASH: &str =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    const HEAD_HASH: &str =
        "0x2222222222222222222222222222222222222222222222222222222222222222";
    const HEAD_NUMBER: i64 = 500;

    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
        ) VALUES
        (
            'ethereum-mainnet', $1, NULL, $2, '2027-01-15T07:59:59Z', 'finalized'
        ),
        (
            'ethereum-mainnet', $3, $1, $4, '2027-01-15T08:00:00Z', 'finalized'
        )
        "#,
    )
    .bind(PARENT_HASH)
    .bind(HEAD_NUMBER - 1)
    .bind(HEAD_HASH)
    .bind(HEAD_NUMBER)
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE chain_heads SET
            latest_block_hash = $1,
            latest_block_number = $2,
            safe_block_hash = $1,
            safe_block_number = $2,
            finalized_block_hash = $1,
            finalized_block_number = $2
        WHERE chain_id = 'ethereum-mainnet'
        "#,
    )
    .bind(HEAD_HASH)
    .bind(HEAD_NUMBER)
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE chain_phase_state SET
            phase_status = 'completed',
            current_block_number = $1,
            current_block_hash = $2,
            target_block_number = $1,
            target_block_hash = $2,
            input_content_hash = $3,
            settled_while_unconfigured = NULL
        WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'
        "#,
    )
    .bind(HEAD_NUMBER)
    .bind(HEAD_HASH)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(&database.lookup_pool)
    .await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query { _meta { block { number hash timestamp parentHash } deployment hasIndexingErrors } }"#,
        json!({}),
    )
    .await?;
    let meta = &payload["data"]["_meta"];
    assert_eq!(meta["block"]["number"], json!(HEAD_NUMBER));
    assert_eq!(meta["block"]["hash"], json!(HEAD_HASH));
    assert_eq!(meta["block"]["parentHash"], json!(PARENT_HASH));
    assert!(meta["block"]["timestamp"].is_number());
    assert_eq!(
        meta["deployment"],
        json!(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    );
    assert_eq!(meta["hasIndexingErrors"], json!(false));

    let number_constrained = post_graphql(
        database.app_state(),
        r#"query {
            _meta(block: { number: 500 }) { block { number hash } }
            domain(id: "alice.eth", block: { number_gte: 500 }) { id }
        }"#,
        json!({}),
    )
    .await?;
    assert_eq!(
        number_constrained["data"]["_meta"]["block"],
        json!({"number": HEAD_NUMBER, "hash": null})
    );
    assert_eq!(
        number_constrained["data"]["domain"]["id"],
        json!(GRAPHQL_ALICE_NAMEHASH)
    );
    let selector_precedence = post_graphql(
        database.app_state(),
        r#"query {
            _meta(block: {
                hash: "0x2222222222222222222222222222222222222222222222222222222222222222"
                number: 499
                number_gte: 501
            }) { block { number hash } }
        }"#,
        json!({}),
    )
    .await?;
    assert_eq!(
        selector_precedence["data"]["_meta"]["block"],
        json!({"number": HEAD_NUMBER, "hash": HEAD_HASH})
    );
    let number_precedence = post_graphql(
        database.app_state(),
        r#"query {
            _meta(block: { number: 500, number_gte: 501 }) { block { number hash } }
        }"#,
        json!({}),
    )
    .await?;
    assert_eq!(
        number_precedence["data"]["_meta"]["block"],
        json!({"number": HEAD_NUMBER, "hash": null})
    );
    let empty_block = post_graphql_allow_errors(
        database.app_state(),
        r#"query { _meta(block: {}) { block { number } } }"#,
        json!({}),
    )
    .await?;
    assert_eq!(empty_block["data"]["_meta"], Value::Null);
    assert!(empty_block["errors"].is_array());
    let null_precedence = post_graphql_allow_errors(
        database.app_state(),
        r#"query { _meta(block: { hash: null, number: 500 }) { block { number } } }"#,
        json!({}),
    )
    .await?;
    assert_eq!(null_precedence["data"]["_meta"], Value::Null);
    assert!(null_precedence["errors"].is_array());
    let null_number_precedence = post_graphql_allow_errors(
        database.app_state(),
        r#"query { _meta(block: { number: null, number_gte: 500 }) { block { number } } }"#,
        json!({}),
    )
    .await?;
    assert_eq!(null_number_precedence["data"]["_meta"], Value::Null);
    assert!(null_number_precedence["errors"].is_array());
    let historical = post_graphql_allow_errors(
        database.app_state(),
        r#"query { domain(id: "alice.eth", block: { number: 499 }) { id } }"#,
        json!({}),
    )
    .await?;
    assert_eq!(historical["data"]["domain"], Value::Null);
    assert!(
        historical["errors"]
            .as_array()
            .is_some_and(|errors| !errors.is_empty())
    );

    // Advance intake and Project after this request selects block 500 but before it reads indexing
    // status. The final served-head revalidation must reject the now-old publication, so the
    // transient equality mismatch never reaches the wire as an indexing error.
    const NEXT_HASH: &str =
        "0x3333333333333333333333333333333333333333333333333333333333333333";
    let (_guard, control) =
        crate::graphql::graphql_indexing_status_test_hooks::install(&database.lookup_pool).await?;
    let state = database.app_state();
    let lag_request = tokio::spawn(async move {
        post_graphql_allow_errors(
            state,
            r#"query { _meta { block { number } hasIndexingErrors } }"#,
            json!({}),
        )
        .await
    });
    control.wait_until_reached().await;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
        ) VALUES (
            'ethereum-mainnet', $1, $2, $3, '2027-01-15T08:00:01Z', 'canonical'
        )
        "#,
    )
    .bind(NEXT_HASH)
    .bind(HEAD_HASH)
    .bind(HEAD_NUMBER + 1)
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.chain_heads SET latest_block_hash = $1, \
         latest_block_number = $2 WHERE chain_id = 'ethereum-mainnet'",
    )
    .bind(NEXT_HASH)
    .bind(HEAD_NUMBER + 1)
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state SET current_block_hash = $1, \
         current_block_number = $2, target_block_hash = $1, target_block_number = $2 \
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .bind(NEXT_HASH)
    .bind(HEAD_NUMBER + 1)
    .execute(&database.lookup_pool)
    .await?;
    control.resume().await;
    let lag_payload = lag_request.await.context("GraphQL lag request panicked")??;
    assert_eq!(lag_payload["data"]["_meta"], Value::Null);
    assert_eq!(
        lag_payload["errors"][0]["extensions"]["code"],
        json!("internal_error")
    );

    sqlx::query(
        "UPDATE bigname_phase.chain_heads SET latest_block_hash = $1, \
         latest_block_number = $2 WHERE chain_id = 'ethereum-mainnet'",
    )
    .bind(HEAD_HASH)
    .bind(HEAD_NUMBER)
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state SET current_block_hash = $1, \
         current_block_number = $2, target_block_hash = $1, target_block_number = $2 \
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .bind(HEAD_HASH)
    .bind(HEAD_NUMBER)
    .execute(&database.lookup_pool)
    .await?;

    sqlx::query(
        "UPDATE chain_phase_state SET settled_while_unconfigured = true \
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .execute(&database.lookup_pool)
    .await?;
    let degraded = post_graphql(
        database.app_state(),
        r#"query { _meta { hasIndexingErrors } domain(
            id: "alice.eth",
            block: { hash: "0x2222222222222222222222222222222222222222222222222222222222222222" },
            subgraphError: allow
        ) { id } }"#,
        json!({}),
    )
    .await?;
    assert_eq!(degraded["data"]["_meta"]["hasIndexingErrors"], json!(true));
    assert_eq!(
        degraded["data"]["domain"]["id"],
        json!(GRAPHQL_ALICE_NAMEHASH)
    );
    // The policy is scaffold plumbing in T1: its default must not break this existing Manager
    // selection while later entity slices define per-entity error behavior.
    let default_policy = post_graphql(
        database.app_state(),
        r#"query { domain(id: "alice.eth") { id } }"#,
        json!({}),
    )
    .await?;
    assert_eq!(
        default_policy["data"]["domain"]["id"],
        json!(GRAPHQL_ALICE_NAMEHASH)
    );

    database.cleanup().await
}

#[tokio::test]
async fn graphql_compatibility_reads_phase_projections() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    seed_alice_record_inventory(&database).await?;
    let payload = post_graphql(
        database.app_state(),
        r#"query PhaseOnly($id: ID!, $domainWhere: Domain_filter!, $connectionWhere: DomainFilter!) {
            domain(id: $id) {
                name
                resolver { contentHash addresses { coinType address } }
            }
            domains(where: $domainWhere) { name }
            domainConnection(first: 0, where: $connectionWhere) { totalCount }
        }"#,
        json!({
            "id": "alice.eth",
            "domainWhere": {"owner": GRAPHQL_OWNER},
            "connectionWhere": {"owner": GRAPHQL_OWNER}
        }),
    )
    .await?;
    assert_eq!(payload["data"]["domain"]["name"], json!("alice.eth"));
    assert_eq!(
        payload["data"]["domain"]["resolver"]["contentHash"],
        json!("0xe30101701220aabbccdd")
    );
    assert_eq!(payload["data"]["domains"].as_array().unwrap().len(), 2);
    assert_eq!(
        payload["data"]["domainConnection"]["totalCount"],
        json!(2)
    );

    database.cleanup().await
}

#[tokio::test]
async fn graphql_phase_reads_accept_rfc3339_summary_timestamps() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.name_current
        SET declared_summary = jsonb_set(
            jsonb_set(
                declared_summary,
                '{registration,created_at}',
                '"2023-11-14T22:13:20+00:00"'::jsonb
            ),
            '{registration,expiry}',
            '"2030-03-17T17:46:40+00:00"'::jsonb
        )
        WHERE logical_name_id = 'ens:' || $1
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;
    let payload = post_graphql(
        database.app_state(),
        r#"query Domain($id: ID!) {
            domain(id: $id) { createdAt expiryDate }
        }"#,
        json!({ "id": "alice.eth" }),
    )
    .await?;
    assert_eq!(payload["data"]["domain"]["createdAt"], json!("1700000000"));
    assert_eq!(payload["data"]["domain"]["expiryDate"], json!("1900000000"));

    database.cleanup().await
}

#[tokio::test]
async fn graphql_bigint_timestamps_do_not_saturate_at_i32_max() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.name_current
        SET declared_summary = jsonb_set(
            jsonb_set(declared_summary, '{registration,created_at}', '2200000000'),
            '{registration,expiry}', '2500000000'
        )
        WHERE logical_name_id = 'ens:' || $1
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;
    let payload = post_graphql(
        database.app_state(),
        r#"query Domain($id: ID!) { domain(id: $id) { createdAt expiryDate } }"#,
        json!({ "id": "alice.eth" }),
    )
    .await?;
    assert_eq!(payload["data"]["domain"]["createdAt"], json!("2200000000"));
    assert_eq!(payload["data"]["domain"]["expiryDate"], json!("2500000000"));

    database.cleanup().await
}

#[tokio::test]
async fn graphql_exact_name_predicate_uses_phase_namehash_index() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let mut transaction = database.lookup_pool.begin().await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await?;
    let plan = sqlx::query_scalar::<_, String>(
        r#"
        EXPLAIN (COSTS OFF)
        WITH filtered_names AS (
            SELECT logical_name_id, namespace, raw_name, namehash
            FROM name_current
            WHERE support_status IN ('supported', 'unsupported')
              AND namespace = $1
              AND namehash = $2
        )
        SELECT * FROM filtered_names LIMIT 1
        "#,
    )
    .bind("ens")
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .fetch_all(&mut *transaction)
    .await?
    .join("\n");
    assert!(
        plan.contains("name_current_lookup_idx"),
        "exact-name lookup must use the bounded phase namehash index:\n{plan}"
    );
    transaction.rollback().await?;

    database.cleanup().await
}

#[tokio::test]
async fn graphql_connection_counts_reject_targets_ahead_of_head() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.name_current
        SET chain_positions = jsonb_set(chain_positions, '{ethereum,block_number}', '415')
        WHERE logical_name_id = 'ens:' || $1
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;

    let name_target = post_graphql_allow_errors(
        database.app_state(),
        r#"query Count($where: DomainFilter!) {
            domainConnection(first: 0, where: $where) { totalCount }
        }"#,
        json!({ "where": { "owner": GRAPHQL_OWNER } }),
    )
    .await?;
    assert_eq!(name_target["data"]["domainConnection"], Value::Null);
    assert!(name_target["errors"].as_array().is_some_and(|errors| !errors.is_empty()));

    sqlx::query(
        r#"
        UPDATE bigname_phase.name_current
        SET chain_positions = jsonb_set(chain_positions, '{ethereum,block_number}', '411')
        WHERE logical_name_id = 'ens:' || $1
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.address_names_current
        SET chain_positions = jsonb_set(chain_positions, '{target_block_number}', '415')
        WHERE address = $1 AND logical_name_id = 'ens:' || $2
        "#,
    )
    .bind(GRAPHQL_OWNER)
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;

    let membership_target = post_graphql_allow_errors(
        database.app_state(),
        r#"query Count($where: DomainFilter!) {
            domainConnection(first: 0, where: $where) { totalCount }
        }"#,
        json!({ "where": { "owner": GRAPHQL_OWNER } }),
    )
    .await?;
    assert_eq!(membership_target["data"]["domainConnection"], Value::Null);
    assert!(
        membership_target["errors"]
            .as_array()
            .is_some_and(|errors| !errors.is_empty())
    );

    database.cleanup().await
}

#[tokio::test]
async fn graphql_connection_count_snapshot_metadata_is_bounded() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.name_current
        SET chain_positions = jsonb_set(
            jsonb_set(chain_positions, '{ethereum,block_number}',
                to_jsonb(CASE raw_name
                    WHEN 'alice.eth' THEN 401
                    WHEN 'bob.eth' THEN 402
                    WHEN 'carol.eth' THEN 403
                    ELSE 404
                END)),
            '{ethereum,block_hash}',
            to_jsonb('0x' || lower(split_part(raw_name, '.', 1)))
        )
        WHERE namespace = 'ens'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.address_names_current
        SET chain_positions = jsonb_set(
            jsonb_set(
                chain_positions,
                '{ethereum,block_number}',
                to_jsonb(CASE raw_name
                    WHEN 'alice.eth' THEN 401
                    WHEN 'bob.eth' THEN 402
                    WHEN 'carol.eth' THEN 403
                    ELSE 404
                END)
            ),
            '{ethereum,block_hash}',
            to_jsonb('0x' || lower(split_part(raw_name, '.', 1)))
        )
        WHERE namespace = 'ens'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;

    let count = crate::graphql::count_phase_graphql_name_list(
        &database.lookup_pool,
        &bigname_storage::NameCurrentListFilter {
            namespace: Some("ens".to_owned()),
            address: Some(bigname_storage::NameCurrentAddressFilter {
                address: String::new(),
                relation: bigname_storage::NameCurrentAddressRelationFilter::Any,
                addresses: Some(vec![
                    GRAPHQL_OWNER.to_owned(),
                    GRAPHQL_FALLBACK_HOLDER.to_owned(),
                ]),
            }),
            ..Default::default()
        },
        &["ethereum-mainnet".to_owned()],
    )
    .await?;
    assert_eq!(count.total_count, 4);
    assert!(count.name_targets.len() <= 2, "{count:?}");
    assert!(count.membership_targets.len() <= 2, "{count:?}");

    database.cleanup().await
}

#[tokio::test]
async fn graphql_point_list_and_inventory_reject_targets_ahead_of_head() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.name_current
        SET chain_positions = jsonb_set(chain_positions, '{ethereum,block_number}', '415')
        WHERE logical_name_id = 'ens:' || $1
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;

    for (query, variables, path) in [
        (
            r#"query Domain($id: ID!) { domain(id: $id) { name } }"#,
            json!({ "id": "alice.eth" }),
            "domain",
        ),
        (
            r#"query Domains($where: Domain_filter!) { domains(where: $where) { name } }"#,
            json!({ "where": { "owner": GRAPHQL_OWNER } }),
            "domains",
        ),
    ] {
        let payload = post_graphql_allow_errors(database.app_state(), query, variables).await?;
        assert_eq!(payload["data"][path], Value::Null);
        assert!(payload["errors"].as_array().is_some_and(|errors| !errors.is_empty()));
    }

    sqlx::query(
        r#"
        UPDATE bigname_phase.name_current
        SET chain_positions = jsonb_set(chain_positions, '{ethereum,block_number}', '411')
        WHERE logical_name_id = 'ens:' || $1
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;
    let membership_rows = sqlx::query(
        r#"
        UPDATE bigname_phase.address_names_current
        SET chain_positions = jsonb_set(chain_positions, '{target_block_number}', '415')
        WHERE logical_name_id = 'ens:' || $1
          AND relation = 'effective_controller'
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(membership_rows.rows_affected(), 1);
    for owner_filter in [
        json!({ "owner": GRAPHQL_OWNER }),
        json!({ "owner_contains": "000a" }),
    ] {
        let membership_target = post_graphql_allow_errors(
            database.app_state(),
            r#"query Domains($where: Domain_filter!) { domains(where: $where) { name } }"#,
            json!({ "where": owner_filter }),
        )
        .await?;
        assert_eq!(membership_target["data"]["domains"], Value::Null);
        assert!(membership_target["errors"].as_array().is_some_and(|errors| !errors.is_empty()));
    }
    sqlx::query(
        r#"
        UPDATE bigname_phase.address_names_current
        SET chain_positions = jsonb_set(chain_positions, '{target_block_number}', '411')
        WHERE logical_name_id = 'ens:' || $1
          AND relation = 'effective_controller'
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;
    seed_alice_record_inventory(&database).await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.record_inventory_current
        SET chain_positions = jsonb_set(chain_positions, '{target_block_number}', '415')
        WHERE resource_id = $1
        "#,
    )
    .bind(Uuid::from_u128(0x6_a002))
    .execute(&database.lookup_pool)
    .await?;
    let inventory = post_graphql_allow_errors(
        database.app_state(),
        r#"query Domain($id: ID!) {
            domain(id: $id) { resolver { contentHash } }
        }"#,
        json!({ "id": "alice.eth" }),
    )
    .await?;
    assert_eq!(inventory["data"]["domain"]["resolver"], Value::Null);
    assert!(inventory["errors"].as_array().is_some_and(|errors| !errors.is_empty()));

    database.cleanup().await
}

#[tokio::test]
async fn graphql_lists_and_counts_scope_rows_to_the_selected_snapshot_chains() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        ) VALUES (
            'ethereum-sepolia', '0xother-chain', 10940282,
            '2026-05-28T13:15:36Z', 'finalized'
        )
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.name_surfaces (
            logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
            namehash, labelhashes, normalizer_version, visibility_state,
            normalization_errors, chain_id, block_hash, block_number,
            provenance, canonicality_state
        )
        SELECT 'ens:0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
               namespace, 'OtherChain.eth', raw_labels, dns_encoded_name,
               '0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
               labelhashes, normalizer_version, visibility_state,
               normalization_errors, 'ethereum-sepolia', '0xother-chain',
               10940282, provenance, canonicality_state
        FROM bigname_phase.name_surfaces
        WHERE logical_name_id = 'ens:' || $1
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.name_current (
            logical_name_id, namespace, raw_name, namehash, surface_binding_id,
            resource_id, token_lineage_id, binding_kind, declared_summary,
            support_status, unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT 'ens:0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
               namespace, 'OtherChain.eth',
               '0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
               NULL, NULL, NULL, NULL, '{}'::jsonb, support_status,
               unsupported_reason, provenance,
               chain_positions || jsonb_build_object(
                   'ethereum-sepolia', jsonb_build_object(
                   'chain_id', 'ethereum-sepolia',
                   'block_number', 10940282,
                   'block_hash', '0xother-chain',
                   'timestamp', '2026-05-28T13:15:36Z'
               )), canonicality_summary, manifest_version
        FROM bigname_phase.name_current
        WHERE logical_name_id = 'ens:' || $1
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;
    let other_chain_relations = sqlx::query(
        r#"
        INSERT INTO bigname_phase.address_names_current (
            address, logical_name_id, relation, namespace, raw_name, namehash,
            surface_binding_id, resource_id, token_lineage_id, binding_kind,
            support_status, unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT $2, logical_name_id, relation, namespace, raw_name, namehash,
               surface_binding_id, resource_id, token_lineage_id, binding_kind,
               support_status, unsupported_reason,
               jsonb_set(provenance, '{chain_id}', '"ethereum-sepolia"'),
               jsonb_build_object(
                   'target_block_number', 10940282,
                   'target_block_hash', '0xother-chain'
               ), canonicality_summary, manifest_version
        FROM bigname_phase.address_names_current
        WHERE logical_name_id = 'ens:' || $1
          AND relation IN ('token_holder', 'effective_controller')
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .bind(GRAPHQL_OTHER_CHAIN_HOLDER)
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(other_chain_relations.rows_affected(), 2);
    let mut eligible = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "SELECT COUNT(*) FROM bigname_phase.address_names_current anc ",
    );
    eligible
        .push(bigname_storage::DEFAULT_ADDRESS_NAMES_MEMBERSHIP_JOINS)
        .push(" WHERE anc.address = ")
        .push_bind(GRAPHQL_OTHER_CHAIN_HOLDER)
        .push(" AND anc.relation = 'effective_controller'")
        .push(bigname_storage::DEFAULT_ADDRESS_NAMES_MEMBERSHIP_READ_FILTER);
    let eligible: i64 = eligible
        .build_query_scalar()
        .fetch_one(&database.lookup_pool)
        .await?;
    assert_eq!(eligible, 1, "cross-chain relation must pass baseline membership eligibility");

    let payload = post_graphql(
        database.app_state(),
        r#"query {
            domains(first: 10, orderBy: name) { name }
            domainConnection(first: 0) { totalCount }
        }"#,
        json!({}),
    )
    .await?;
    assert_eq!(payload["data"]["domains"].as_array().map(Vec::len), Some(4));
    assert_eq!(payload["data"]["domainConnection"]["totalCount"], json!(4));

    let other_chain = post_graphql(
        database.app_state(),
        r#"query OtherChain($domainWhere: Domain_filter!, $connectionWhere: DomainFilter!) {
            domains(where: $domainWhere) { name }
            domainConnection(first: 0, where: $connectionWhere) { totalCount }
        }"#,
        json!({
            "domainWhere": {"owner": GRAPHQL_OTHER_CHAIN_HOLDER},
            "connectionWhere": {"owner": GRAPHQL_OTHER_CHAIN_HOLDER}
        }),
    )
    .await?;
    assert_eq!(other_chain["data"]["domains"], json!([]));
    assert_eq!(
        other_chain["data"]["domainConnection"]["totalCount"],
        json!(0)
    );

    database.cleanup().await
}

#[tokio::test]
async fn graphql_rejects_project_republication_during_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let (_guard, control) =
        crate::v2::lookup_served_head_revalidation_test_hooks::install(&database.lookup_pool)
            .await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/graphql")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "query": "query { domain(id: \"alice.eth\") { name } }"
                        })
                        .to_string(),
                    ))
                    .expect("graphql request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    sqlx::query(
        r#"
        UPDATE bigname_phase.chain_phase_state
        SET updated_at = clock_timestamp()
        WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("GraphQL republish request task panicked")?
        .context("GraphQL republish request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["domain"], Value::Null);
    assert_eq!(payload["errors"][0]["extensions"]["code"], json!("internal_error"));

    database.cleanup().await
}

#[tokio::test]
async fn graphql_root_fields_share_one_served_head_selection() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let (_guard, control) =
        crate::v2::lookup_served_head_initial_validation_test_hooks::install(
            &database.lookup_pool,
        )
        .await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        post_graphql(
            state,
            r#"query {
                registrationConnection(first: 0) { totalCount }
                domain(id: "alice.eth") { name }
            }"#,
            json!({}),
        )
        .await
    });

    control.wait_until_reached().await;
    control.resume().await;
    let payload = tokio::time::timeout(std::time::Duration::from_secs(1), request_task)
        .await
        .context("sibling GraphQL root fields selected the served head more than once")???;
    assert_eq!(payload["data"]["domain"]["name"], json!("alice.eth"));
    assert_eq!(
        payload["data"]["registrationConnection"]["totalCount"],
        json!(4)
    );

    database.cleanup().await
}

#[tokio::test]
async fn graphql_nested_resolver_rejects_publication_after_parent_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    seed_alice_record_inventory(&database).await?;
    let (_guard, control) =
        crate::graphql::nested_inventory_test_hooks::install(&database.lookup_pool).await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/graphql")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "query": "query { domain(id: \"alice.eth\") { name resolver { contentHash } } }"
                        })
                        .to_string(),
                    ))
                    .expect("graphql request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    sqlx::query(
        r#"
        UPDATE bigname_phase.chain_phase_state
        SET updated_at = clock_timestamp()
        WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("GraphQL nested resolver request task panicked")?
        .context("GraphQL nested resolver request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["domain"]["resolver"], Value::Null);
    assert_eq!(payload["errors"][0]["extensions"]["code"], json!("internal_error"));

    database.cleanup().await
}

#[tokio::test]
async fn graphql_does_not_publish_unsupported_phase_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.name_current
        SET support_status = 'unsupported',
            unsupported_reason = 'unsupported GraphQL fixture'
        WHERE logical_name_id = 'ens:' || $1
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;

    let point = post_graphql(
        database.app_state(),
        "query { domain(id: \"alice.eth\") { name } }",
        json!({}),
    )
    .await?;
    assert_eq!(point["data"]["domain"], Value::Null);
    let list = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) { domains(where: $where) { name } }"#,
        json!({ "where": { "owner": GRAPHQL_OWNER } }),
    )
    .await?;
    assert_eq!(list["data"]["domains"], json!([{ "name": "bob.eth" }]));

    sqlx::query(
        r#"
        UPDATE bigname_phase.name_current
        SET support_status = 'supported', unsupported_reason = NULL
        WHERE logical_name_id = 'ens:' || $1
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;
    seed_alice_record_inventory(&database).await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.name_current name
        SET declared_summary = jsonb_set(
            name.declared_summary,
            '{topology}',
            jsonb_build_object(
                'version_boundaries', jsonb_build_object(
                    'record_version_boundary', inventory.record_version_boundary
                )
            )
        )
        FROM bigname_phase.record_inventory_current inventory
        WHERE name.logical_name_id = 'ens:' || $1
          AND inventory.resource_id = $2
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .bind(Uuid::from_u128(0x6_a002))
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.record_inventory_current
        SET support_status = 'unsupported',
            unsupported_reason = 'unsupported GraphQL inventory fixture'
        WHERE resource_id = $1
        "#,
    )
    .bind(Uuid::from_u128(0x6_a002))
    .execute(&database.lookup_pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.record_inventory_current (
            resource_id, record_version_boundary_key, record_version_boundary,
            selectors, unsupported_families, entries, support_status,
            unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT resource_id, record_version_boundary_key || ':other',
               jsonb_set(record_version_boundary, '{event_kind}', '"other"'),
               selectors, unsupported_families, entries, 'supported', NULL,
               provenance, chain_positions, canonicality_summary, manifest_version
        FROM bigname_phase.record_inventory_current
        WHERE resource_id = $1
        "#,
    )
    .bind(Uuid::from_u128(0x6_a002))
    .execute(&database.lookup_pool)
    .await?;
    let inventory = post_graphql(
        database.app_state(),
        r#"query { domain(id: "alice.eth") {
            resolver { texts contentHash addresses { coinType address } }
        } }"#,
        json!({}),
    )
    .await?;
    let resolver = &inventory["data"]["domain"]["resolver"];
    assert_eq!(resolver["texts"], json!([]));
    assert_eq!(resolver["contentHash"], Value::Null);
    assert_eq!(resolver["addresses"], json!([]));

    database.cleanup().await
}

#[tokio::test]
async fn graphql_domain_op_returns_subgraph_domain_shape() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query Domain($id: ID!) {
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
    assert_eq!(domain["name"], json!("alice.eth"));
    assert_eq!(domain["normalizedName"], json!("alice.eth"));
    assert_eq!(domain["owner"]["id"], json!(GRAPHQL_OWNER));
    // BigInt values serialize as arbitrary-width decimal strings.
    assert_eq!(domain["createdAt"], json!("1700000000"));
    assert_eq!(domain["expiryDate"], json!("1900000000"));
    assert!(domain["createdAt"].is_string());
    // alice.eth has no record_inventory_current row seeded, so the resolver serves its empty
    // shapes: address present, texts/addresses empty, contentHash null. (Populated record fields
    // are covered by graphql_domain_resolver_serves_record_inventory_fields.)
    assert_eq!(domain["resolver"]["address"], json!(GRAPHQL_RESOLVER));
    assert_eq!(domain["resolver"]["texts"], json!([]));
    assert_eq!(domain["resolver"]["addresses"], json!([]));
    assert_eq!(domain["resolver"]["contentHash"], Value::Null);

    // The name-string `id` path resolves the same row that the namehash query above reached via the
    // fallback.
    let by_name = post_graphql(
        database.app_state(),
        r#"query Domain($id: ID!) {
            domain(id: $id) { id name normalizedName owner { id } }
        }"#,
        json!({ "id": "alice.eth" }),
    )
    .await?;
    let by_name = &by_name["data"]["domain"];
    assert_eq!(by_name["id"], json!(GRAPHQL_ALICE_NAMEHASH));
    assert_eq!(by_name["name"], json!("alice.eth"));
    assert_eq!(by_name["normalizedName"], json!("alice.eth"));
    assert_eq!(by_name["owner"]["id"], json!(GRAPHQL_OWNER));

    // Unknown id (neither a known name nor a known namehash) resolves to null without an error.
    let missing = post_graphql(
        database.app_state(),
        r#"query Domain($id: ID!) { domain(id: $id) { id } }"#,
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
    seed_graphql_compat_fixture(&database).await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!, $first: Int, $skip: Int, $orderBy: Domain_orderBy, $orderDirection: OrderDirection) {
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
    assert_eq!(domains[0]["name"], json!("alice.eth"));
    assert_eq!(domains[1]["name"], json!("bob.eth"));
    assert_eq!(domains[0]["owner"]["id"], json!(GRAPHQL_OWNER));

    // Offset window is disjoint and stable.
    let second = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!, $first: Int, $skip: Int, $orderBy: Domain_orderBy, $orderDirection: OrderDirection) {
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
    assert_eq!(page[0]["name"], json!("bob.eth"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn graphql_empty_owner_in_matches_nothing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;

    // A non-empty owner_in returns the owner's names...
    let populated = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) { domains(where: $where) { id } }"#,
        json!({ "where": { "owner_in": [GRAPHQL_OWNER] } }),
    )
    .await?;
    assert!(
        !populated["data"]["domains"]
            .as_array()
            .expect("domains array")
            .is_empty(),
        "a non-empty owner_in must return the owner's names"
    );

    // ...but an EMPTY owner_in matches NOTHING (the compatibility contract) rather than
    // silently widening to the whole namespace. Both the list and the connection count must be empty.
    let empty_list = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) { domains(where: $where) { id } }"#,
        json!({ "where": { "owner_in": [] } }),
    )
    .await?;
    assert_eq!(
        empty_list["data"]["domains"],
        json!([]),
        "empty owner_in must match nothing"
    );

    let empty_count = post_graphql(
        database.app_state(),
        r#"query DomainConnection($where: DomainFilter!) {
            domainConnection(first: 0, where: $where) { totalCount }
        }"#,
        json!({ "where": { "owner_in": [] } }),
    )
    .await?;
    assert_eq!(
        empty_count["data"]["domainConnection"]["totalCount"],
        json!(0),
        "empty owner_in must count nothing"
    );

    database.cleanup().await
}

#[tokio::test]
async fn graphql_connection_ops_return_total_counts() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;

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
    seed_graphql_compat_fixture(&database).await?;

    // Carol has no declared owner — `owner` resolves through the registrant leg of the fallback.
    let carol = post_graphql(
        database.app_state(),
        r#"query Domain($id: ID!) { domain(id: $id) { owner { id } expiryDate } }"#,
        json!({ "id": "carol.eth" }),
    )
    .await?;
    assert_eq!(
        carol["data"]["domain"]["owner"]["id"],
        json!(GRAPHQL_REGISTRANT_C)
    );
    assert_eq!(carol["data"]["domain"]["expiryDate"], json!("1950000000"));

    // Dave has neither owner nor registrant — `owner` stays non-null via the zero-address
    // sentinel, the missing expiry serializes as null, and the missing created_at degenerates to
    // epoch rather than breaking the non-null `createdAt: BigInt!`.
    let dave = post_graphql(
        database.app_state(),
        r#"query Domain($id: ID!) { domain(id: $id) { owner { id } expiryDate createdAt } }"#,
        json!({ "id": "dave.eth" }),
    )
    .await?;
    assert_eq!(dave["data"]["domain"]["owner"]["id"], json!(ZERO_ADDRESS));
    assert_eq!(dave["data"]["domain"]["expiryDate"], Value::Null);
    assert_eq!(dave["data"]["domain"]["createdAt"], json!("0"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn graphql_domain_resolver_serves_record_inventory_fields() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    seed_alice_record_inventory(&database).await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query Domain($id: ID!) {
            domain(id: $id) {
                resolver { id address texts contentHash addresses { coinType address } }
            }
        }"#,
        json!({ "id": "alice.eth" }),
    )
    .await?;

    let resolver = &payload["data"]["domain"]["resolver"];
    assert_eq!(resolver["address"], json!(GRAPHQL_RESOLVER));
    // texts are the text-family selector keys, so url is listed even though its value was not
    // retained.
    assert_eq!(resolver["texts"], json!(["avatar", "url"]));
    // addresses carry only retained (status=success) addr entries, parsed into {coinType, address}.
    // The first coin type is beyond i32 by design, so it exercises the u32 GraphQL scalar path.
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
        r#"query Domain($id: ID!) {
            domain(id: $id) { resolver { id address texts contentHash addresses { coinType address } } }
        }"#,
        json!({ "id": "bob.eth" }),
    )
    .await?;
    assert_eq!(bob["data"]["domain"]["resolver"]["texts"], json!([]));
    assert_eq!(bob["data"]["domain"]["resolver"]["addresses"], json!([]));
    assert_eq!(bob["data"]["domain"]["resolver"]["contentHash"], Value::Null);

    let roots = post_graphql(
        database.app_state(),
        r#"query { resolvers { id address texts contentHash addresses { coinType address } } }"#,
        json!({}),
    )
    .await?;
    let roots = roots["data"]["resolvers"]
        .as_array()
        .context("resolvers must be a list")?;
    assert_eq!(roots.len(), 2);
    assert!(roots.contains(&resolver.clone()));
    assert!(roots.contains(&bob["data"]["domain"]["resolver"]));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn graphql_ownerless_domain_resolver_uses_serving_resource_inventory() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    seed_alice_record_inventory(&database).await?;

    let update = sqlx::query(
        r#"
        UPDATE bigname_phase.name_current
        SET serving_resource_id = resource_id,
            resource_id = NULL,
            surface_binding_id = NULL,
            token_lineage_id = NULL,
            binding_kind = NULL,
            declared_summary = jsonb_set(
                jsonb_set(
                    declared_summary,
                    '{registration}',
                    '{"status":"unregistered"}'::jsonb
                ),
                '{control}',
                '{}'::jsonb
            )
        WHERE logical_name_id = 'ens:' || $1
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.pool)
    .await?;
    assert_eq!(update.rows_affected(), 1, "ownerless fixture must update Alice");
    let (current_resource_id, serving_resource_id): (Option<Uuid>, Option<Uuid>) =
        sqlx::query_as(
            "SELECT resource_id, serving_resource_id
             FROM bigname_phase.name_current
             WHERE logical_name_id = 'ens:' || $1",
        )
        .bind(GRAPHQL_ALICE_NAMEHASH)
        .fetch_one(&database.pool)
        .await?;
    assert!(current_resource_id.is_none());
    assert!(serving_resource_id.is_some());
    assert_ne!(current_resource_id, serving_resource_id);

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
    assert_eq!(resolver["texts"], json!(["avatar", "url"]));
    assert_eq!(resolver["contentHash"], json!("0xe30101701220aabbccdd"));
    assert_eq!(
        resolver["addresses"],
        json!([
            { "coinType": 2_147_483_658u32, "address": "0x00000000000000000000000000000000000000bb" },
            { "coinType": 60, "address": "0x00000000000000000000000000000000000000aa" },
        ])
    );

    let root_payload = post_graphql(
        database.app_state(),
        r#"query Resolver($id: ID!) {
            resolver(id: $id) { id address texts contentHash addresses { coinType address } }
        }"#,
        json!({ "id": resolver["id"] }),
    )
    .await?;
    assert_eq!(
        root_payload["data"]["resolver"], *resolver,
        "the generated Resolver root must hydrate the same serving-resource inventory as Domain.resolver"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn graphql_domain_resolver_serves_sepolia_records_via_anchor_fallback() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_erin_sepolia_record_fixture(&database).await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query Domain($id: ID!) {
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
async fn graphql_domain_resolver_serves_inventory_after_name_republication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    seed_alice_record_inventory(&database).await?;
    seed_schema_v2_lookup_head(
        &database.lookup_pool,
        "ethereum-mainnet",
        415,
        "0xgraphql-republication",
        "2026-04-17T00:00:05Z",
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.name_current
        SET chain_positions = jsonb_set(
            jsonb_set(chain_positions, '{ethereum,block_number}', '415'),
            '{ethereum,block_hash}', '"0xgraphql-republication"'
        )
        WHERE logical_name_id = 'ens:' || $1
        "#,
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query { domain(id: "alice.eth") {
            resolver { texts contentHash addresses { coinType address } }
        } }"#,
        json!({}),
    )
    .await?;
    let resolver = &payload["data"]["domain"]["resolver"];
    assert_eq!(resolver["texts"], json!(["avatar", "url"]));
    assert_eq!(resolver["contentHash"], json!("0xe30101701220aabbccdd"));
    assert_eq!(resolver["addresses"].as_array().map(Vec::len), Some(2));

    database.cleanup().await
}

#[tokio::test]
async fn graphql_domain_resolver_rejects_ambiguous_resource_inventories() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    seed_alice_record_inventory(&database).await?;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.record_inventory_current (
            resource_id, record_version_boundary_key, record_version_boundary,
            selectors, unsupported_families, entries, support_status,
            unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT resource_id, record_version_boundary_key || ':ambiguous',
               jsonb_set(record_version_boundary, '{event_kind}', '"ambiguous"'),
               selectors, unsupported_families, entries, support_status,
               unsupported_reason, provenance, chain_positions,
               canonicality_summary, manifest_version
        FROM bigname_phase.record_inventory_current
        WHERE resource_id = $1
        "#,
    )
    .bind(Uuid::from_u128(0x6_a002))
    .execute(&database.lookup_pool)
    .await?;

    let payload = post_graphql_allow_errors(
        database.app_state(),
        r#"query { domain(id: "alice.eth") { resolver { texts } } }"#,
        json!({}),
    )
    .await?;
    assert_eq!(payload["data"]["domain"]["resolver"], Value::Null);
    assert!(payload["errors"].as_array().is_some_and(|errors| !errors.is_empty()));

    database.cleanup().await
}

#[tokio::test]
async fn graphql_domain_serves_a_stored_name_that_no_longer_normalizes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current SET raw_name = 'a..eth' \
         WHERE logical_name_id = 'ens:' || $1",
    )
    .bind(GRAPHQL_ALICE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query Domain($id: ID!) { domain(id: $id) { name normalizedName } }"#,
        json!({ "id": GRAPHQL_ALICE_NAMEHASH }),
    )
    .await?;
    assert_eq!(payload["data"]["domain"]["name"], json!("a..eth"));
    assert_eq!(payload["data"]["domain"]["normalizedName"], json!("a..eth"));

    database.cleanup().await
}

#[tokio::test]
async fn graphql_name_order_uses_stored_normalized_name_bytes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET raw_name = CASE logical_name_id
             WHEN 'ens:' || $1 THEN 'ᏣᎳᎩ.eth'
             WHEN 'ens:' || $2 THEN 'z.eth'
         END
         WHERE logical_name_id IN ('ens:' || $1, 'ens:' || $2)",
    )
    .bind(GRAPHQL_CAROL_NAMEHASH)
    .bind(GRAPHQL_DAVE_NAMEHASH)
    .execute(&database.lookup_pool)
    .await?;

    let payload = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) {
            domains(where: $where, orderBy: name, orderDirection: asc) { name }
        }"#,
        json!({ "where": { "id_in": [GRAPHQL_CAROL_NAMEHASH, GRAPHQL_DAVE_NAMEHASH] } }),
    )
    .await?;
    assert_eq!(
        payload["data"]["domains"],
        json!([{ "name": "z.eth" }, { "name": "ᏣᎳᎩ.eth" }])
    );

    database.cleanup().await
}

#[tokio::test]
async fn graphql_name_order_sql_pins_semantic_collation_in_both_directions() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;

    let config = database.database_config(1)?;
    let options = PgConnectOptions::from_str(
        config
            .database_url
            .as_deref()
            .context("GraphQL SQL-capture database URL is missing")?,
    )?
    .options([("search_path", "bigname_phase".to_owned())]);
    let capture_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let state = AppState::new_with_rpc_urls(
        capture_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    )
    .with_public_namespaces_for_test(["ens", "basenames"]);

    for direction in ["asc", "desc"] {
        post_graphql(
            state.clone(),
            &format!(
                r#"query Domains($where: Domain_filter!) {{
                    domains(where: $where, orderBy: name, orderDirection: {direction}) {{ name }}
                }}"#
            ),
            json!({ "where": { "id_in": [GRAPHQL_CAROL_NAMEHASH, GRAPHQL_DAVE_NAMEHASH] } }),
        )
        .await?;
    }

    let prepared_statements: Vec<String> = sqlx::query_scalar(
        "SELECT statement FROM pg_prepared_statements \
         WHERE statement LIKE '%canonical_display_name%'",
    )
    .fetch_all(&capture_pool)
    .await?;
    for direction in ["ASC", "DESC"] {
        let expected = format!(
            "ORDER BY canonical_display_name COLLATE \"C\" {direction}, \
             namehash {direction}, namespace ASC"
        );
        assert!(
            prepared_statements
                .iter()
                .any(|statement| statement.contains(&expected)),
            "GraphQL name {direction} SQL must include `{expected}`"
        );
    }

    capture_pool.close().await;
    database.cleanup().await
}

#[tokio::test]
async fn graphql_domains_op_orders_desc_and_ranks_null_expiry() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;

    let names = |payload: &Value| -> Vec<String> {
        payload["data"]["domains"]
            .as_array()
            .expect("domains must be an array")
            .iter()
            .map(|domain| domain["name"].as_str().expect("name").to_owned())
            .collect()
    };

    // Descending name order inverts the ascending window the compatibility test pins.
    let desc = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!, $orderBy: Domain_orderBy, $orderDirection: OrderDirection) {
            domains(where: $where, orderBy: $orderBy, orderDirection: $orderDirection) { name }
        }"#,
        json!({
            "where": { "id_in": [GRAPHQL_CAROL_NAMEHASH, GRAPHQL_DAVE_NAMEHASH] },
            "orderBy": "name",
            "orderDirection": "desc",
        }),
    )
    .await?;
    assert_eq!(names(&desc), vec!["dave.eth", "carol.eth"]);

    // expiryDate asc ranks NULL expiries last (carol has 1.95e9, dave has none)…
    let expiry_asc = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!, $orderBy: Domain_orderBy, $orderDirection: OrderDirection) {
            domains(where: $where, orderBy: $orderBy, orderDirection: $orderDirection) { name }
        }"#,
        json!({
            "where": { "id_in": [GRAPHQL_CAROL_NAMEHASH, GRAPHQL_DAVE_NAMEHASH] },
            "orderBy": "expiryDate",
            "orderDirection": "asc",
        }),
    )
    .await?;
    assert_eq!(names(&expiry_asc), vec!["carol.eth", "dave.eth"]);

    // …and desc ranks them first ("no expiry" sorts as furthest-future).
    let expiry_desc = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!, $orderBy: Domain_orderBy, $orderDirection: OrderDirection) {
            domains(where: $where, orderBy: $orderBy, orderDirection: $orderDirection) { name }
        }"#,
        json!({
            "where": { "id_in": [GRAPHQL_CAROL_NAMEHASH, GRAPHQL_DAVE_NAMEHASH] },
            "orderBy": "expiryDate",
            "orderDirection": "desc",
        }),
    )
    .await?;
    assert_eq!(names(&expiry_desc), vec!["dave.eth", "carol.eth"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn graphql_filters_registrant_in_and_name_contains() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;

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

    // name_contains narrows the fixture names down to the substring match.
    let contains = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) {
            domains(where: $where) { name }
        }"#,
        json!({ "where": { "name_contains": "aro" } }),
    )
    .await?;
    let matched = contains["data"]["domains"].as_array().expect("array");
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0]["name"], json!("carol.eth"));

    let wrong_case = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) {
            domains(where: $where) { name }
        }"#,
        json!({ "where": { "name_contains": "ARO" } }),
    )
    .await?;
    assert_eq!(wrong_case["data"]["domains"].as_array().map(Vec::len), Some(0));

    let wildcard = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) {
            domains(where: $where) { name }
        }"#,
        json!({ "where": { "name_contains": "%" } }),
    )
    .await?;
    assert_eq!(wildcard["data"]["domains"].as_array().map(Vec::len), Some(4));

    let empty = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) {
            domains(where: $where) { name }
        }"#,
        json!({ "where": { "name_contains": "" } }),
    )
    .await?;
    assert_eq!(empty["data"]["domains"].as_array().map(Vec::len), Some(4));

    let empty_page = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) {
            domains(first: 0, where: $where) { name }
        }"#,
        json!({ "where": { "name_contains": "%" } }),
    )
    .await?;
    assert_eq!(empty_page["data"]["domains"].as_array().map(Vec::len), Some(0));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn graphql_name_contains_accepts_label_boundary_fragments() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    seed_identity_name(
        &database,
        "ens:alice.eth.example",
        "alice.eth.example",
        "alice.eth.example",
        "namehash:alice.eth.example",
        Uuid::from_u128(0x584_3001),
        Uuid::from_u128(0x584_3002),
        Uuid::from_u128(0x584_3003),
        "0x0000000000000000000000000000000000058430",
        bigname_storage::AddressNameRelation::TokenHolder,
        430,
    )
    .await?;
    seed_identity_name(
        &database,
        "ens:ethereal.xyz",
        "ethereal.xyz",
        "ethereal.xyz",
        "namehash:ethereal.xyz",
        Uuid::from_u128(0x584_4001),
        Uuid::from_u128(0x584_4002),
        Uuid::from_u128(0x584_4003),
        "0x0000000000000000000000000000000000058440",
        bigname_storage::AddressNameRelation::TokenHolder,
        431,
    )
    .await?;

    let query = r#"query Domains($where: Domain_filter!) {
        domains(where: $where) { name }
    }"#;
    for (fragment, expected_names) in [
        (".eth", None),
        ("eth.", Some(vec!["alice.eth.example"])),
        (".eth.", Some(vec!["alice.eth.example"])),
        ("th.e", Some(vec!["alice.eth.example"])),
    ] {
        let payload = post_graphql_allow_errors(
            database.app_state(),
            query,
            json!({ "where": { "name_contains": fragment } }),
        )
        .await?;
        assert!(payload.get("errors").is_none(), "{fragment}: {payload}");
        let matched = payload["data"]["domains"]
            .as_array()
            .expect("domains must be an array")
            .iter()
            .map(|domain| domain["name"].as_str().expect("name"))
            .collect::<Vec<_>>();
        if let Some(expected_names) = expected_names {
            assert_eq!(matched, expected_names, "{fragment}");
        } else {
            assert!(matched.contains(&"alice.eth"), "{fragment}: {matched:?}");
            assert!(matched.contains(&"bob.eth"), "{fragment}: {matched:?}");
            assert!(
                !matched.contains(&"ethereal.xyz"),
                "{fragment}: {matched:?}"
            );
        }
    }

    for (fragment, expected_len) in [(".", 6), ("..", 0)] {
        let payload = post_graphql(
            database.app_state(),
            query,
            json!({ "where": { "name_contains": fragment } }),
        )
        .await?;
        assert_eq!(
            payload["data"]["domains"].as_array().map(Vec::len),
            Some(expected_len),
            "{fragment}: {payload}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

async fn graphql_make_owner_target_future(database: &TestDatabase, namehash: &str) -> Result<()> {
    let stamp_query = "SELECT jsonb_build_object('head', to_jsonb(head), 'phase', to_jsonb(phase), 'generation', phase.xmin::text)::text FROM bigname_phase.chain_heads head JOIN bigname_phase.chain_phase_state phase USING(chain_id) WHERE head.chain_id = 'ethereum-mainnet' AND phase.phase_name = 'project'";
    let before: String = sqlx::query_scalar(stamp_query).fetch_one(&database.lookup_pool).await?;
    // Stable inconsistent projection state, not an ordinary concurrent Project race:
    // the request head and phase generation remain fixed while canonical future lineage exists.
    sqlx::query("INSERT INTO bigname_phase.chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state) SELECT chain_id, '0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff', latest_block_hash, 415, '2027-01-15T08:00:00Z', 'finalized' FROM bigname_phase.chain_heads WHERE chain_id = 'ethereum-mainnet' ON CONFLICT DO NOTHING")
        .execute(&database.lookup_pool).await?;
    let changed = sqlx::query("UPDATE bigname_phase.address_names_current SET chain_positions = jsonb_build_object('target_block_number', 415, 'target_block_hash', '0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff') WHERE logical_name_id = 'ens:' || $1 AND relation = 'effective_controller' AND address = $2")
        .bind(namehash).bind(GRAPHQL_OWNER).execute(&database.lookup_pool).await?;
    assert_eq!(changed.rows_affected(), 1);
    let after: String = sqlx::query_scalar(stamp_query).fetch_one(&database.lookup_pool).await?;
    assert_eq!(before, after, "head and Project generation must stay fixed");

    Ok(())
}

#[tokio::test]
async fn graphql_owner_hidden_negative_witness_refuses() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query("INSERT INTO bigname_phase.address_names_current SELECT (jsonb_populate_record(NULL::bigname_phase.address_names_current, to_jsonb(anc) || jsonb_build_object('address', $1::text))).* FROM bigname_phase.address_names_current anc WHERE logical_name_id = 'ens:' || $2 AND relation = 'effective_controller'")
        .bind(GRAPHQL_FALLBACK_HOLDER).bind(GRAPHQL_ALICE_NAMEHASH).execute(&database.lookup_pool).await?;
    let query = "query($where: Domain_filter!) { domains(where: $where) { id } }";
    let filters = [
        json!({"id": GRAPHQL_ALICE_NAMEHASH, "owner_not": GRAPHQL_OWNER}),
        json!({"id": GRAPHQL_ALICE_NAMEHASH, "owner_not_in": [GRAPHQL_OWNER]}),
        json!({"id": GRAPHQL_ALICE_NAMEHASH, "owner_not_contains": "000a"}),
        json!({"id": GRAPHQL_ALICE_NAMEHASH, "owner": GRAPHQL_FALLBACK_HOLDER, "owner_not": GRAPHQL_OWNER}),
    ];
    for filter in &filters {
        let current = post_graphql(database.app_state(), query, json!({"where": filter})).await?;
        assert_eq!(current["data"]["domains"], json!([]));
    }
    graphql_make_owner_target_future(&database, GRAPHQL_ALICE_NAMEHASH).await?;
    let control = post_graphql_allow_errors(database.app_state(), query, json!({"where": {"id": GRAPHQL_ALICE_NAMEHASH, "owner": GRAPHQL_OWNER}})).await?;
    assert!(control["errors"].as_array().is_some_and(|errors| !errors.is_empty()), "{control}");
    let unrelated = post_graphql(database.app_state(), query, json!({"where": {
        "id": GRAPHQL_ALICE_NAMEHASH, "owner": GRAPHQL_FALLBACK_HOLDER,
        "owner_not": GRAPHQL_OTHER_CHAIN_HOLDER
    }})).await?;
    assert_eq!(unrelated["data"]["domains"], json!([{"id": GRAPHQL_ALICE_NAMEHASH}]),
        "a nonmatching invalid sibling is not rejecting evidence");
    let mut missing = Vec::new();
    for filter in filters {
        let response = post_graphql_allow_errors(database.app_state(), query, json!({"where": filter})).await?;
        if response["errors"].as_array().is_none_or(|errors| errors.is_empty()) {
            missing.push((filter, response));
        }
    }
    assert!(missing.is_empty(), "negative witnesses escaped validation: {missing:?}");
    database.cleanup().await
}

#[tokio::test]
async fn graphql_owner_hidden_offset_witness_refuses() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let query = "query($where: Domain_filter!, $skip: Int!, $first: Int!) { domains(where: $where, orderBy: id, orderDirection: asc, skip: $skip, first: $first) { id } }";
    let filters = [
        json!({"owner_contains": "0x"}),
        json!({"owner": GRAPHQL_OWNER}),
        json!({"owner_in": [GRAPHQL_OWNER]}),
    ];
    // Bound the unanchored pattern to the same two-name candidate set.
    let filters = filters.map(|mut filter| {
        filter["id_in"] = json!([GRAPHQL_ALICE_NAMEHASH, GRAPHQL_BOB_NAMEHASH]);
        filter
    });
    for filter in &filters {
        let current = post_graphql(database.app_state(), query, json!({"where": filter, "skip": 0, "first": 2})).await?;
        assert_eq!(current["data"]["domains"], json!([{"id": GRAPHQL_ALICE_NAMEHASH}, {"id": GRAPHQL_BOB_NAMEHASH}]));
        let page = post_graphql(database.app_state(), query, json!({"where": filter, "skip": 1, "first": 1})).await?;
        assert_eq!(page["data"]["domains"], json!([{"id": GRAPHQL_BOB_NAMEHASH}]));
    }
    graphql_make_owner_target_future(&database, GRAPHQL_ALICE_NAMEHASH).await?;
    let mut missing = Vec::new();
    for filter in filters {
        let control = post_graphql_allow_errors(database.app_state(), query, json!({"where": filter, "skip": 0, "first": 1})).await?;
        assert!(control["errors"].as_array().is_some_and(|errors| !errors.is_empty()), "{control}");
        let page = post_graphql_allow_errors(database.app_state(), query, json!({"where": filter, "skip": 1, "first": 1})).await?;
        if page["errors"].as_array().is_none_or(|errors| errors.is_empty()) {
            missing.push((filter, page));
        }
    }
    assert!(missing.is_empty(), "skipped witnesses escaped validation: {missing:?}");
    database.cleanup().await
}

#[tokio::test]
async fn graphql_owner_hidden_offset_name_target_refuses() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let query = "query($where: Domain_filter!, $skip: Int!) { domains(where: $where, orderBy: id, orderDirection: asc, skip: $skip, first: 1) { id } }";
    let filters = [json!({"owner": GRAPHQL_OWNER}), json!({"owner_contains": "0x"})]
        .map(|mut filter| {
            filter["id_in"] = json!([GRAPHQL_ALICE_NAMEHASH, GRAPHQL_BOB_NAMEHASH]);
            filter
        });
    let stamp_query = "SELECT jsonb_build_object('head', to_jsonb(head), 'phase', to_jsonb(phase), 'generation', phase.xmin::text)::text FROM bigname_phase.chain_heads head JOIN bigname_phase.chain_phase_state phase USING(chain_id) WHERE head.chain_id = 'ethereum-mainnet' AND phase.phase_name = 'project'";
    let stamp: String = sqlx::query_scalar(stamp_query).fetch_one(&database.lookup_pool).await?;
    let original: Value = sqlx::query_scalar("SELECT chain_positions FROM bigname_phase.name_current WHERE namehash = $1")
        .bind(GRAPHQL_ALICE_NAMEHASH).fetch_one(&database.lookup_pool).await?;
    let witness_query = "SELECT chain_positions FROM bigname_phase.address_names_current WHERE namehash = $1 AND relation = 'effective_controller' AND address = $2";
    let witness: Value = sqlx::query_scalar(witness_query).bind(GRAPHQL_ALICE_NAMEHASH).bind(GRAPHQL_OWNER).fetch_one(&database.lookup_pool).await?;
    for filter in &filters {
        for (skip, expected) in [(0, GRAPHQL_ALICE_NAMEHASH), (1, GRAPHQL_BOB_NAMEHASH)] {
            let response = post_graphql(database.app_state(), query, json!({"where": filter, "skip": skip})).await?;
            assert_eq!(response["data"]["domains"], json!([{"id": expected}]));
        }
    }
    // Refusal of stable inconsistent projection state; ordinary Project publication
    // rebuilds the name and controller together. Keep its head/generation fixed here.
    sqlx::query("INSERT INTO bigname_phase.chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state) SELECT chain_id, '0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff', latest_block_hash, latest_block_number + 1, '2027-01-15T08:00:00Z', 'finalized' FROM bigname_phase.chain_heads WHERE chain_id = 'ethereum-mainnet' ON CONFLICT DO NOTHING")
        .execute(&database.lookup_pool).await?;
    let mut missing = Vec::new();
    for future in [true, false] {
        let changed = sqlx::query("UPDATE bigname_phase.name_current SET chain_positions = (SELECT jsonb_object_agg(position.key, position.value || jsonb_build_object('block_number', head.latest_block_number + $2, 'block_hash', '0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff')) FROM jsonb_each($3::jsonb) position CROSS JOIN bigname_phase.chain_heads head WHERE head.chain_id = 'ethereum-mainnet') WHERE namehash = $1")
            .bind(GRAPHQL_ALICE_NAMEHASH).bind(i64::from(future)).bind(&original).execute(&database.lookup_pool).await?;
        assert_eq!(changed.rows_affected(), 1);
        for filter in &filters {
            for skip in [0, 1] {
                let response = post_graphql_allow_errors(database.app_state(), query, json!({"where": filter, "skip": skip})).await?;
                if response["errors"].as_array().is_none_or(|errors| errors.is_empty()) {
                    missing.push((future, filter.clone(), skip, response));
                }
            }
        }
        assert_eq!(stamp, sqlx::query_scalar::<_, String>(stamp_query).fetch_one(&database.lookup_pool).await?);
        assert_eq!(witness, sqlx::query_scalar::<_, Value>(witness_query).bind(GRAPHQL_ALICE_NAMEHASH).bind(GRAPHQL_OWNER).fetch_one(&database.lookup_pool).await?);
    }
    database.cleanup().await?;
    assert!(missing.is_empty(), "name targets escaped validation: {missing:?}");
    Ok(())
}
