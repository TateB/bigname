// Integration coverage for the native subgraph-compatible GraphQL surface. The SDL-snapshot test
// guards the codegen contract the Manager depends on; the end-to-end test drives the four dashboard
// ops through `app_router` + `oneshot` against seeded Sepolia-v2-shaped rows.

const GRAPHQL_OWNER: &str = "0x000000000000000000000000000000000000000a";
const GRAPHQL_REGISTRANT: &str = "0x000000000000000000000000000000000000000b";
const GRAPHQL_RESOLVER: &str = "0x000000000000000000000000000000000000def0";
const GRAPHQL_ALICE_NAMEHASH: &str = "0xa11ce";
const GRAPHQL_BOB_NAMEHASH: &str = "0xb0b";

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

#[test]
fn graphql_sdl_matches_subgraph_codegen_contract() {
    let sdl = crate::graphql::subgraph_sdl();

    assert!(sdl.contains("enum Domain_orderBy"), "SDL:\n{sdl}");
    for value in ["createdAt", "expiryDate", "id", "name", "registrationDate"] {
        assert!(
            sdl.contains(value),
            "SDL missing Domain_orderBy value {value}:\n{sdl}"
        );
    }
    assert!(sdl.contains("enum OrderDirection"));
    assert!(sdl.contains("asc"));
    assert!(sdl.contains("desc"));

    assert!(sdl.contains("normalizedName"));
    assert!(sdl.contains("tokenId"));
    assert!(sdl.contains("owner: Account!"), "owner must be non-null:\n{sdl}");
    assert!(sdl.contains("type Account"));
    assert!(sdl.contains("type Resolver"));
    assert!(sdl.contains("contentHash"));
    assert!(sdl.contains("coinType"));

    assert!(sdl.contains("owner_in"));
    assert!(sdl.contains("name_contains"));
    assert!(sdl.contains("isMigrated"));
    assert!(sdl.contains("registrant_in"));
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
    // Dashboard-scope resolver stubs: address present, texts/addresses empty, contentHash null.
    assert_eq!(domain["resolver"]["address"], json!(GRAPHQL_RESOLVER));
    assert_eq!(domain["resolver"]["texts"], json!([]));
    assert_eq!(domain["resolver"]["addresses"], json!([]));
    assert_eq!(domain["resolver"]["contentHash"], Value::Null);

    // Unknown namehash resolves to null without an error.
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
