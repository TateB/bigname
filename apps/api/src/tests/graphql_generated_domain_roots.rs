fn generated_type_ref(value: &Value) -> String {
    match value["kind"].as_str().expect("introspection type kind") {
        "NON_NULL" => format!("{}!", generated_type_ref(&value["ofType"])),
        "LIST" => format!("[{}]", generated_type_ref(&value["ofType"])),
        _ => value["name"]
            .as_str()
            .expect("introspection named type")
            .to_owned(),
    }
}

fn generated_field<'a>(type_value: &'a Value, name: &str) -> &'a Value {
    type_value["fields"]
        .as_array()
        .expect("introspection fields")
        .iter()
        .find(|field| field["name"] == name)
        .unwrap_or_else(|| panic!("missing GraphQL field {name}"))
}

#[tokio::test]
async fn graphql_generated_domain_root_signature_matches_pinned_schema() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let payload = post_graphql(
        database.app_state(),
        r#"query {
          queryType: __type(name: "Query") { fields { name args { name defaultValue type { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } type { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } }
          domainType: __type(name: "Domain") { fields { name type { kind name ofType { kind name } } } }
        }"#,
        json!({}),
    )
    .await?;
    let query = &payload["data"]["queryType"];
    for (name, expected_args, expected_return) in [
        (
            "domain",
            vec![
                ("id", "ID!", None),
                ("block", "Block_height", None),
                ("subgraphError", "_SubgraphErrorPolicy_!", Some("deny")),
            ],
            "Domain",
        ),
        (
            "domains",
            vec![
                ("skip", "Int", Some("0")),
                ("first", "Int", Some("100")),
                ("orderBy", "Domain_orderBy", None),
                ("orderDirection", "OrderDirection", None),
                ("where", "Domain_filter", None),
                ("block", "Block_height", None),
                ("subgraphError", "_SubgraphErrorPolicy_!", Some("deny")),
            ],
            "[Domain!]!",
        ),
    ] {
        let field = generated_field(query, name);
        let actual_args = field["args"]
            .as_array()
            .expect("field args")
            .iter()
            .map(|arg| {
                (
                    arg["name"].as_str().unwrap(),
                    generated_type_ref(&arg["type"]),
                    arg["defaultValue"].as_str(),
                )
            })
            .collect::<Vec<_>>();
        let expected_args = expected_args
            .into_iter()
            .map(|(arg, ty, default)| (arg, ty.to_owned(), default))
            .collect::<Vec<_>>();
        assert_eq!(actual_args, expected_args, "{name} argument signature");
        assert_eq!(generated_type_ref(&field["type"]), expected_return);
    }
    let id = generated_field(&payload["data"]["domainType"], "id");
    assert_eq!(generated_type_ref(&id["type"]), "ID!");
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_filter_has_slice_1b_owner_members() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let payload = post_graphql(
        database.app_state(),
        r#"query { __type(name: "Domain_filter") { inputFields { name } } }"#,
        json!({}),
    )
    .await?;
    let mut actual = payload["data"]["__type"]["inputFields"]
        .as_array()
        .context("Domain_filter input fields")?
        .iter()
        .filter_map(|field| field["name"].as_str())
        .collect::<Vec<_>>();
    actual.sort_unstable();
    assert_eq!(
        actual,
        [
            "id", "id_gt", "id_gte", "id_in", "id_lt", "id_lte", "id_not",
            "id_not_in", "name", "name_contains", "name_contains_nocase",
            "name_ends_with", "name_ends_with_nocase", "name_gt", "name_gte",
            "name_in", "name_lt", "name_lte", "name_not", "name_not_contains",
            "name_not_contains_nocase", "name_not_ends_with",
            "name_not_ends_with_nocase", "name_not_in", "name_not_starts_with",
            "name_not_starts_with_nocase", "name_starts_with",
            "name_starts_with_nocase", "owner", "owner_contains",
            "owner_contains_nocase", "owner_ends_with", "owner_ends_with_nocase",
            "owner_gt", "owner_gte", "owner_in", "owner_lt", "owner_lte", "owner_not",
            "owner_not_contains", "owner_not_contains_nocase", "owner_not_ends_with",
            "owner_not_ends_with_nocase", "owner_not_in", "owner_not_starts_with",
            "owner_not_starts_with_nocase", "owner_starts_with",
            "owner_starts_with_nocase"
        ]
    );
    database.cleanup().await
}

async fn generated_domain_names(database: &TestDatabase, where_value: Value) -> Result<Vec<String>> {
    let payload = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) { domains(where: $where, orderBy: name) { name } }"#,
        json!({"where": where_value}),
    )
    .await?;
    Ok(payload["data"]["domains"]
        .as_array()
        .context("domains array")?
        .iter()
        .filter_map(|row| row["name"].as_str().map(str::to_owned))
        .collect())
}

async fn generated_domain_owner_rows(
    database: &TestDatabase,
    where_value: Value,
) -> Result<Vec<(String, String)>> {
    let payload = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) {
            domains(where: $where, orderBy: name) { name owner { id } }
        }"#,
        json!({"where": where_value}),
    )
    .await?;
    payload["data"]["domains"]
        .as_array()
        .context("domains array")?
        .iter()
        .map(|row| {
            Ok((
                row["name"].as_str().context("domain name")?.to_owned(),
                row["owner"]["id"]
                    .as_str()
                    .context("domain owner id")?
                    .to_owned(),
            ))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn seed_generated_owner_shape(
    database: &TestDatabase,
    name: &str,
    registry_owner: &str,
    token_holder: &str,
    tokenized: bool,
    wrapped: bool,
    id_base: u128,
    block_number: i64,
) -> Result<()> {
    let namehash = bigname_lookup::ens_namehash_hex(name)?;
    let resource_id = Uuid::from_u128(id_base);
    let token_lineage_id = Uuid::from_u128(id_base + 1);
    let surface_binding_id = Uuid::from_u128(id_base + 2);
    seed_identity_name(
        database,
        &format!("ens:{name}"),
        name,
        name,
        &namehash,
        resource_id,
        token_lineage_id,
        surface_binding_id,
        token_holder,
        bigname_storage::AddressNameRelation::TokenHolder,
        block_number,
    )
    .await?;

    let mut summary = json!({
        "registration": {
            "status": "active",
            "authority_kind": "registrar",
            "registrant": token_holder,
            "expiry": 1_900_000_000_i64,
            "created_at": 1_700_000_000_i64,
        },
        "control": {
            "registry_owner": registry_owner,
            "registrant": token_holder,
            "expiry": 1_900_000_000_i64,
        }
    });
    if wrapped {
        summary["wrapper_state"] = json!("wrapped");
    }
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = $1,
             token_lineage_id = CASE WHEN $2 THEN token_lineage_id ELSE NULL END
         WHERE raw_name = $3",
    )
    .bind(summary)
    .bind(tokenized)
    .bind(name)
    .execute(&database.lookup_pool)
    .await?;
    if !tokenized {
        sqlx::query("DELETE FROM bigname_phase.address_names_current WHERE raw_name = $1")
            .bind(name)
            .execute(&database.lookup_pool)
            .await?;
    }
    upsert_phase_address_names_current_rows(
        &database.lookup_pool,
        &[address_name_current_row(
            registry_owner,
            &format!("ens:{name}"),
            bigname_storage::AddressNameRelation::EffectiveController,
            name,
            name,
            &namehash,
            surface_binding_id,
            resource_id,
            tokenized.then_some(token_lineage_id),
            block_number,
        )],
    )
    .await?;
    Ok(())
}

const OWNER_SCALAR_A: &str = "0x0000000000000000000000000000000000000671";
const OWNER_SCALAR_B: &str = "0x0000000000000000000000000000000000000672";
const OWNER_SCALAR_C: &str = "0x0000000000000000000000000000000000000abc";

async fn seed_generated_owner_scalar_corpus(database: &TestDatabase) -> Result<()> {
    for (name, owner, id) in [
        ("owner-scalar-a.eth", OWNER_SCALAR_A, 0x670_3101),
        ("owner-scalar-b.eth", OWNER_SCALAR_B, 0x670_3111),
        ("owner-scalar-c.eth", OWNER_SCALAR_C, 0x670_3121),
    ] {
        seed_generated_owner_shape(database, name, owner, owner, true, false, id, 720).await?;
    }
    Ok(())
}

fn owner_scalar_matches(owner: &str, member: &str, operand: &Value) -> bool {
    let scalar = operand.as_str().unwrap_or_default();
    match member {
        "owner" => owner == scalar,
        "owner_not" => owner != scalar,
        "owner_gt" => owner > scalar, "owner_gte" => owner >= scalar,
        "owner_lt" => owner < scalar, "owner_lte" => owner <= scalar,
        "owner_in" => operand.as_array().is_some_and(|values| values.iter().any(|value| value.as_str().is_some_and(|value| owner == value))),
        "owner_not_in" => operand.as_array().is_some_and(|values| !values.is_empty() && values.iter().all(|value| value.as_str().is_some_and(|value| owner != value))),
        _ => {
            let pattern = if member.contains("contains") {
                if scalar.starts_with('%') || scalar.ends_with('%') { scalar.to_owned() } else { format!("%{scalar}%") }
            } else if member.contains("starts_with") { format!("{scalar}%") } else { format!("%{scalar}") };
            sql_like(owner, &pattern, member.ends_with("_nocase")) != member.contains("_not_")
        }
    }
}

fn owner_scalar_rows(rows: Vec<(String, String)>) -> Vec<(String, String)> {
    rows.into_iter().filter(|(name, _)| name.starts_with("owner-scalar-")).collect()
}

#[tokio::test]
async fn graphql_generated_domains_default_to_first_100_ids_and_cap_first_at_200() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let mut expected = vec![
        GRAPHQL_ALICE_NAMEHASH.to_owned(),
        GRAPHQL_BOB_NAMEHASH.to_owned(),
        GRAPHQL_CAROL_NAMEHASH.to_owned(),
        GRAPHQL_DAVE_NAMEHASH.to_owned(),
    ];
    for index in 0..198_u128 {
        let name = format!("generated-{index:03}.eth");
        let namehash = bigname_lookup::ens_namehash_hex(&name)?;
        expected.push(namehash.clone());
        seed_identity_name(
            &database,
            &format!("ens:{name}"),
            &name,
            &name,
            &namehash,
            Uuid::from_u128(0x670_1000 + index * 3),
            Uuid::from_u128(0x670_1001 + index * 3),
            Uuid::from_u128(0x670_1002 + index * 3),
            GRAPHQL_OWNER,
            bigname_storage::AddressNameRelation::TokenHolder,
            500 + index as i64,
        )
        .await?;
    }
    expected.sort();
    let expected_capped = expected.iter().take(200).cloned().collect::<Vec<_>>();
    let expected_default = expected.iter().take(100).cloned().collect::<Vec<_>>();
    let payload = post_graphql(database.app_state(), "query { domains { id } }", json!({})).await?;
    let actual = payload["data"]["domains"]
        .as_array()
        .context("domains array")?
        .iter()
        .filter_map(|row| row["id"].as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected_default);

    let payload = post_graphql(
        database.app_state(),
        "query { domains(first: 500) { id } }",
        json!({}),
    )
    .await?;
    let capped = payload["data"]["domains"]
        .as_array()
        .context("capped domains array")?
        .iter()
        .filter_map(|row| row["id"].as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    assert_eq!(capped, expected_capped);
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domains_apply_id_and_current_filters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    for (filter, expected) in [
        (json!({"id": GRAPHQL_ALICE_NAMEHASH}), vec!["alice.eth"]),
        (json!({"id_in": [GRAPHQL_BOB_NAMEHASH, GRAPHQL_ALICE_NAMEHASH, GRAPHQL_BOB_NAMEHASH]}), vec!["alice.eth", "bob.eth"]),
        (json!({"id": GRAPHQL_ALICE_NAMEHASH, "id_in": [GRAPHQL_ALICE_NAMEHASH, GRAPHQL_BOB_NAMEHASH]}), vec!["alice.eth"]),
        (json!({"id": GRAPHQL_ALICE_NAMEHASH, "id_in": [GRAPHQL_BOB_NAMEHASH]}), vec![]),
        (json!({"id_in": []}), vec![]),
        (json!({"name": "alice.eth"}), vec!["alice.eth"]),
        (json!({"name_contains": "lic"}), vec!["alice.eth"]),
        (json!({"owner": GRAPHQL_OWNER}), vec!["alice.eth", "bob.eth"]),
        (json!({"owner_in": [GRAPHQL_OWNER]}), vec!["alice.eth", "bob.eth"]),
        (json!({"owner_in": []}), vec![]),
        (json!({"owner": GRAPHQL_OWNER, "owner_in": [GRAPHQL_FALLBACK_HOLDER]}), vec![]),
        (json!({"owner": GRAPHQL_OWNER, "name": "alice.eth"}), vec!["alice.eth"]),
    ] {
        assert_eq!(generated_domain_names(&database, filter).await?, expected);
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_owner_filters_match_the_served_owner() -> Result<()> {
    const SUBNAME_OWNER: &str = "0x0000000000000000000000000000000000000671";
    const SECOND_LEVEL_OWNER: &str = "0x0000000000000000000000000000000000000672";
    const SECOND_LEVEL_HOLDER: &str = "0x0000000000000000000000000000000000000673";
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    for (name, owner, holder, tokenized, wrapped, id_base, block_number) in [
        (
            "owner-sub.parent.eth",
            SUBNAME_OWNER,
            SUBNAME_OWNER,
            false,
            false,
            0x670_3001,
            710,
        ),
        (
            "owner-second-level.eth",
            SECOND_LEVEL_OWNER,
            SECOND_LEVEL_HOLDER,
            true,
            false,
            0x670_3011,
            711,
        ),
    ] {
        seed_generated_owner_shape(
            &database,
            name,
            owner,
            holder,
            tokenized,
            wrapped,
            id_base,
            block_number,
        )
        .await?;
    }

    let served_owner_matches = [
        (SUBNAME_OWNER, "owner-sub.parent.eth"),
        (SECOND_LEVEL_OWNER, "owner-second-level.eth"),
    ];
    let mut actual = Vec::new();
    for (owner, _) in served_owner_matches {
        actual.push((
            owner,
            generated_domain_owner_rows(&database, json!({"owner": owner})).await?,
        ));
    }
    let mut actual_holders = Vec::new();
    for holder in [SECOND_LEVEL_HOLDER] {
        actual_holders.push((
            holder,
            generated_domain_owner_rows(&database, json!({"owner": holder})).await?,
        ));
    }
    let owner_in = generated_domain_owner_rows(
        &database,
        json!({"owner_in": [SUBNAME_OWNER, SECOND_LEVEL_OWNER]}),
    )
    .await?;
    let mut legacy_counts = Vec::new();
    for address in [SECOND_LEVEL_HOLDER, SECOND_LEVEL_OWNER] {
        let payload = post_graphql(
            database.app_state(),
            r#"query LegacyDomainCount($where: DomainFilter!) {
                domainConnection(first: 0, where: $where) { totalCount }
            }"#,
            json!({"where": {"owner": address}}),
        )
        .await?;
        legacy_counts.push(payload["data"]["domainConnection"]["totalCount"].clone());
    }

    assert_eq!(
        (actual, actual_holders, owner_in, legacy_counts),
        (
            vec![
                (SUBNAME_OWNER, vec![("owner-sub.parent.eth".into(), SUBNAME_OWNER.into())]),
                (SECOND_LEVEL_OWNER, vec![("owner-second-level.eth".into(), SECOND_LEVEL_OWNER.into())]),
            ],
            vec![(SECOND_LEVEL_HOLDER, vec![])],
            vec![
                ("owner-second-level.eth".into(), SECOND_LEVEL_OWNER.into()),
                ("owner-sub.parent.eth".into(), SUBNAME_OWNER.into()),
            ],
            vec![json!(1), json!(0)],
        )
    );
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_owner_scalar_operators_match_served_owner() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    seed_generated_owner_scalar_corpus(&database).await?;
    let corpus = owner_scalar_rows(generated_domain_owner_rows(&database, json!({})).await?);
    assert_eq!(corpus.len(), 3, "owner scalar corpus");
    for (member, operand) in [
        ("owner", json!(OWNER_SCALAR_A)), ("owner_not", json!(OWNER_SCALAR_A)),
        ("owner_gt", json!(OWNER_SCALAR_A)), ("owner_gte", json!(OWNER_SCALAR_B)),
        ("owner_lt", json!(OWNER_SCALAR_B)), ("owner_lte", json!(OWNER_SCALAR_A)),
        ("owner_in", json!([OWNER_SCALAR_A, OWNER_SCALAR_A, OWNER_SCALAR_C])),
        ("owner_not_in", json!([OWNER_SCALAR_A, OWNER_SCALAR_A])),
        ("owner_contains", json!("067")), ("owner_contains_nocase", json!("ABC")),
        ("owner_not_contains", json!("ABC")), ("owner_not_contains_nocase", json!("ABC")),
        ("owner_starts_with", json!("0x000")), ("owner_starts_with_nocase", json!("0X000")),
        ("owner_not_starts_with", json!("0X000")), ("owner_not_starts_with_nocase", json!("0X000")),
        ("owner_ends_with", json!("671")), ("owner_ends_with_nocase", json!("ABC")),
        ("owner_not_ends_with", json!("ABC")), ("owner_not_ends_with_nocase", json!("ABC")),
    ] {
        let expected = corpus.iter().filter(|(_, owner)| owner_scalar_matches(owner, member, &operand)).cloned().collect::<Vec<_>>();
        let actual = owner_scalar_rows(generated_domain_owner_rows(&database, json!({(member): operand})).await?);
        assert_eq!(actual, expected, "{member}");
    }
    for (filter, expected) in [
        (json!({"owner": OWNER_SCALAR_A.to_uppercase()}), vec![]),
        (json!({"owner_in": [OWNER_SCALAR_A.to_uppercase()]}), vec![]),
        (json!({"owner_not": OWNER_SCALAR_A.to_uppercase()}), vec!["owner-scalar-a.eth", "owner-scalar-b.eth", "owner-scalar-c.eth"]),
        (json!({"owner_not_in": [OWNER_SCALAR_A.to_uppercase()]}), vec!["owner-scalar-a.eth", "owner-scalar-b.eth", "owner-scalar-c.eth"]),
        (json!({"owner": OWNER_SCALAR_A, "owner_in": [OWNER_SCALAR_B]}), vec![]),
        (json!({"owner_gte": OWNER_SCALAR_A, "owner_lt": OWNER_SCALAR_B, "name": "owner-scalar-a.eth"}), vec!["owner-scalar-a.eth"]),
    ] {
        assert_eq!(owner_scalar_rows(generated_domain_owner_rows(&database, filter).await?).into_iter().map(|row| row.0).collect::<Vec<_>>(), expected);
    }
    for direction in ["asc", "desc"] {
        let payload = post_graphql(database.app_state(), "query($direction: OrderDirection!) { domains(first: 1, skip: 1, orderBy: id, orderDirection: $direction, where: { owner_starts_with: \"0x\" }) { id name owner { id } } }", json!({"direction": direction})).await?;
        assert_eq!(payload["data"]["domains"].as_array().map(Vec::len), Some(1));
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_owner_operator_null_and_empty_rules() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    seed_generated_owner_scalar_corpus(&database).await?;
    let omitted = generated_domain_owner_rows(&database, json!({})).await?;
    for filter in [json!({"owner": null}), json!({"owner_in": null})] {
        assert_eq!(generated_domain_owner_rows(&database, filter).await?, omitted);
    }
    for member in ["owner_not", "owner_gt", "owner_gte", "owner_lt", "owner_lte", "owner_not_in", "owner_contains", "owner_contains_nocase", "owner_not_contains", "owner_not_contains_nocase", "owner_starts_with", "owner_starts_with_nocase", "owner_not_starts_with", "owner_not_starts_with_nocase", "owner_ends_with", "owner_ends_with_nocase", "owner_not_ends_with", "owner_not_ends_with_nocase"] {
        let payload = post_graphql_allow_errors(database.app_state(), "query($where: Domain_filter!) { domains(where: $where) { id } }", json!({"where": {(member): Value::Null}})).await?;
        assert_eq!(payload["errors"][0]["message"], json!(format!("Domain_filter.{member} must not be null")));
    }
    for member in ["owner_in", "owner_not_in"] {
        assert!(generated_domain_owner_rows(&database, json!({(member): []})).await?.is_empty(), "{member}");
    }
    assert_eq!(generated_domain_owner_rows(&database, json!({"owner_in": [OWNER_SCALAR_A, OWNER_SCALAR_A]})).await?, generated_domain_owner_rows(&database, json!({"owner": OWNER_SCALAR_A})).await?);
    assert_eq!(generated_domain_owner_rows(&database, json!({"owner_not_in": [OWNER_SCALAR_A, OWNER_SCALAR_A]})).await?, generated_domain_owner_rows(&database, json!({"owner_not": OWNER_SCALAR_A})).await?);
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_owner_patterns_match_graph_node_rules() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    seed_generated_owner_scalar_corpus(&database).await?;
    let corpus = owner_scalar_rows(generated_domain_owner_rows(&database, json!({})).await?);
    for (member, operand) in [("owner_contains", "067"), ("owner_contains", "%0671"), ("owner_contains", "0672%"), ("owner_contains", "06_1"), ("owner_contains", r"\%"), ("owner_contains", "ABC"), ("owner_contains_nocase", "ABC"), ("owner_not_contains_nocase", "ABC"), ("owner_starts_with", "0x000"), ("owner_not_starts_with", "0X000"), ("owner_ends_with", "abc"), ("owner_not_ends_with_nocase", "ABC")] {
        let operand = json!(operand);
        let expected = corpus.iter().filter(|(_, owner)| owner_scalar_matches(owner, member, &operand)).cloned().collect::<Vec<_>>();
        assert_eq!(owner_scalar_rows(generated_domain_owner_rows(&database, json!({(member): operand})).await?), expected, "{member}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_owner_operators_preserve_residual_classes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    for (index, (name, owner, wrapped)) in [("owner-residual-wrapper.eth", OWNER_SCALAR_A, true), ("owner-residual-zero.eth", ZERO_ADDRESS, false), ("owner-residual-no-event.eth", OWNER_SCALAR_B, false), ("owner-residual-state-derived.eth", OWNER_SCALAR_C, false)].into_iter().enumerate() {
        seed_generated_owner_shape(&database, name, owner, owner, true, wrapped, 0x670_3201 + index as u128 * 0x10, 730 + index as i64).await?;
        sqlx::query("DELETE FROM bigname_phase.address_names_current WHERE raw_name = $1 AND relation = 'effective_controller'").bind(name).execute(&database.lookup_pool).await?;
        if name.contains("no-event") { sqlx::query("UPDATE bigname_phase.name_current SET declared_summary = declared_summary #- '{control,registry_owner}' WHERE raw_name = $1").bind(name).execute(&database.lookup_pool).await?; }
    }
    let unfiltered = generated_domain_owner_rows(&database, json!({})).await?;
    let residual = unfiltered.into_iter().filter(|(name, _)| name.starts_with("owner-residual-")).collect::<Vec<_>>();
    assert_eq!(residual.len(), 4, "absent-relation residual corpus");
    for (name, served) in residual {
        for (member, operand) in [("owner", json!(served)), ("owner_not", json!("never")), ("owner_gt", json!("")), ("owner_gte", json!(served)), ("owner_lt", json!("z")), ("owner_lte", json!(served)), ("owner_in", json!([served])), ("owner_not_in", json!(["never"])), ("owner_contains", json!(served)), ("owner_contains_nocase", json!(served.to_uppercase())), ("owner_not_contains", json!("never")), ("owner_not_contains_nocase", json!("never")), ("owner_starts_with", json!(served)), ("owner_starts_with_nocase", json!(served.to_uppercase())), ("owner_not_starts_with", json!("never")), ("owner_not_starts_with_nocase", json!("never")), ("owner_ends_with", json!(served)), ("owner_ends_with_nocase", json!(served.to_uppercase())), ("owner_not_ends_with", json!("never")), ("owner_not_ends_with_nocase", json!("never"))] {
            assert!(!generated_domain_owner_rows(&database, json!({(member): operand})).await?.iter().any(|row| row.0 == name), "{name}: {member}");
        }
    }

    const DISAGREEING_NAME: &str = "owner-t5-witness-disagreement.eth";
    seed_generated_owner_shape(&database, DISAGREEING_NAME, OWNER_SCALAR_A, OWNER_SCALAR_A, true, false, 0x670_3251, 735).await?;
    sqlx::query("UPDATE bigname_phase.address_names_current SET address = $1 WHERE raw_name = $2 AND relation = 'effective_controller'").bind(OWNER_SCALAR_C).bind(DISAGREEING_NAME).execute(&database.lookup_pool).await?;
    for filter in [json!({"owner": OWNER_SCALAR_C}), json!({"owner_not": OWNER_SCALAR_A}), json!({"owner_not_in": [OWNER_SCALAR_A]}), json!({"owner_not_ends_with": "671"})] {
        assert!(generated_domain_owner_rows(&database, filter).await?.iter().any(|row| row.0 == DISAGREEING_NAME), "T5 witness-present disagreement follows relation");
    }

    const MULTI_NAME: &str = "owner-multi-controller.eth";
    const MULTI_BASE: u128 = 0x670_3261;
    seed_generated_owner_shape(&database, MULTI_NAME, OWNER_SCALAR_A, OWNER_SCALAR_A, true, false, MULTI_BASE, 736).await?;
    let multi_namehash = bigname_lookup::ens_namehash_hex(MULTI_NAME)?;
    upsert_phase_address_names_current_rows(&database.lookup_pool, &[address_name_current_row(OWNER_SCALAR_C, &format!("ens:{MULTI_NAME}"), bigname_storage::AddressNameRelation::EffectiveController, MULTI_NAME, MULTI_NAME, &multi_namehash, Uuid::from_u128(MULTI_BASE + 2), Uuid::from_u128(MULTI_BASE), Some(Uuid::from_u128(MULTI_BASE + 1)), 736)]).await?;
    assert!(generated_domain_owner_rows(&database, json!({"owner": OWNER_SCALAR_A})).await?.iter().any(|row| row.0 == MULTI_NAME), "multi-controller fixture");
    for filter in [json!({"owner": OWNER_SCALAR_A, "owner_contains": "abc"}), json!({"owner": OWNER_SCALAR_A, "owner_not_contains": "abc"})] {
        assert!(!generated_domain_owner_rows(&database, filter).await?.iter().any(|row| row.0 == MULTI_NAME), "owner predicates share relation-row semantics");
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domains_apply_skip_first_and_direction() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let mut ids = [GRAPHQL_ALICE_NAMEHASH, GRAPHQL_BOB_NAMEHASH];
    ids.sort_unstable();
    for (direction, skip, expected) in [
        ("asc", 1, ids[1]),
        ("desc", 0, ids[1]),
        ("desc", 1, ids[0]),
    ] {
        let payload = post_graphql(
            database.app_state(),
            "query Page($skip: Int!, $direction: OrderDirection!, $where: Domain_filter!) { domains(first: 1, skip: $skip, orderBy: id, orderDirection: $direction, where: $where) { id } }",
            json!({"skip": skip, "direction": direction, "where": {"owner": GRAPHQL_OWNER}}),
        )
        .await?;
        assert_eq!(payload["data"]["domains"][0]["id"], json!(expected));
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_name_fallback_cannot_shadow_namehash() -> Result<()> {
    const TARGET_NAME: &str = "namehash-target.eth";
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let target_namehash = bigname_lookup::ens_namehash_hex(TARGET_NAME)?;
    let normalized = bigname_domain::normalization::normalize_name(&target_namehash)?;
    assert_eq!(normalized.normalized_name, target_namehash);
    let hash_shaped_namehash = bigname_lookup::ens_namehash_hex(&target_namehash)?;
    assert_ne!(hash_shaped_namehash, target_namehash);
    seed_identity_name(
        &database,
        &format!("ens:{target_namehash}"),
        &target_namehash,
        &target_namehash,
        &hash_shaped_namehash,
        Uuid::from_u128(0x670_2001),
        Uuid::from_u128(0x670_2002),
        Uuid::from_u128(0x670_2003),
        GRAPHQL_OWNER,
        bigname_storage::AddressNameRelation::TokenHolder,
        700,
    )
    .await?;
    seed_identity_name(
        &database,
        "ens:namehash-target.eth",
        TARGET_NAME,
        TARGET_NAME,
        &target_namehash,
        Uuid::from_u128(0x670_2011),
        Uuid::from_u128(0x670_2012),
        Uuid::from_u128(0x670_2013),
        GRAPHQL_OWNER,
        bigname_storage::AddressNameRelation::TokenHolder,
        701,
    )
    .await?;
    let payload = post_graphql(
        database.app_state(),
        "query Domain($id: ID!) { domain(id: $id) { name } }",
        json!({"id": target_namehash}),
    )
    .await?;
    assert_eq!(payload["data"]["domain"]["name"], json!(TARGET_NAME));
    let uppercase_prefix = target_namehash.replacen("0x", "0X", 1);
    let payload = post_graphql(
        database.app_state(),
        "query Domain($id: ID!) { domain(id: $id) { name } }",
        json!({"id": uppercase_prefix}),
    )
    .await?;
    assert_eq!(payload["data"]["domain"]["name"], json!(target_namehash));
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_ordinary_name_uses_one_projection_query() -> Result<()> {
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
    sqlx::query("SET enable_seqscan = off")
        .execute(&capture_pool)
        .await?;
    sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(&capture_pool)
        .await?;
    let baseline_scans: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(idx_scan), 0)::BIGINT FROM pg_stat_user_indexes \
         WHERE relid = 'bigname_phase.name_current'::regclass",
    )
    .fetch_one(&capture_pool)
    .await?;
    let state = AppState::new_with_rpc_urls(
        capture_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    )
    .with_public_namespaces_for_test(["ens", "basenames"]);

    let payload = post_graphql(
        state,
        "query { domain(id: \"alice.eth\") { name } }",
        json!({}),
    )
    .await?;
    assert_eq!(payload["data"]["domain"]["name"], json!("alice.eth"));

    sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(&capture_pool)
        .await?;
    let final_scans: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(idx_scan), 0)::BIGINT FROM pg_stat_user_indexes \
         WHERE relid = 'bigname_phase.name_current'::regclass",
    )
    .fetch_one(&capture_pool)
    .await?;
    assert_eq!(
        final_scans - baseline_scans,
        1,
        "ordinary names need one projection lookup"
    );

    capture_pool.close().await;
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domains_reject_t3_filter_members() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    for (query, variables, member) in [(
        "query Domains($where: Domain_filter!) { domains(where: $where) { id } }",
        json!({"where": {"owner_": {"id": "0x"}}}),
        "owner_",
    )] {
        let payload = post_graphql_allow_errors(database.app_state(), query, variables).await?;
        let error = payload["errors"][0]["message"].as_str().context("validation error")?;
        assert!(error.contains("Domain_filter") && error.contains(member), "{error}");
        assert!(payload.get("data").is_none() || payload["data"].is_null());
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_roots_enforce_current_snapshot_blocks() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let (old_number, old_hash): (i64, String) = sqlx::query_as(
        "SELECT current_block_number, current_block_hash FROM bigname_phase.chain_phase_state WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .fetch_one(&database.lookup_pool)
    .await?;
    let number = old_number + 1;
    let hash = format!("0x{}", "22".repeat(32));
    sqlx::query("INSERT INTO bigname_phase.chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state) VALUES ('ethereum-mainnet', $1, $2, $3, now(), 'finalized')")
        .bind(&hash)
        .bind(&old_hash)
        .bind(number)
        .execute(&database.lookup_pool)
        .await?;
    sqlx::query("UPDATE bigname_phase.chain_heads SET latest_block_hash = $1, latest_block_number = $2, safe_block_hash = $1, safe_block_number = $2, finalized_block_hash = $1, finalized_block_number = $2 WHERE chain_id = 'ethereum-mainnet'")
        .bind(&hash)
        .bind(number)
        .execute(&database.lookup_pool)
        .await?;
    sqlx::query("UPDATE bigname_phase.chain_phase_state SET current_block_hash = $1, current_block_number = $2, target_block_hash = $1, target_block_number = $2 WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'")
        .bind(&hash)
        .bind(number)
        .execute(&database.lookup_pool)
        .await?;
    let wrong_hash = format!("0x{}", "ff".repeat(32));
    for block in [
        format!("{{ number: {number} }}"),
        format!("{{ hash: \"{hash}\" }}"),
        format!("{{ number_gte: {} }}", number - 1),
    ] {
        for root in ["domain(id: \"alice.eth\"", "domains("] {
            let payload = post_graphql_allow_errors(
                database.app_state(),
                &format!("query {{ {root} block: {block}) {{ id }} }}"),
                json!({}),
            )
            .await?;
            assert!(payload.get("errors").is_none(), "{payload}");
        }
    }
    for (block, message) in [
        (format!("{{ number: {} }}", number - 1), "the requested block number is not the served head"),
        (format!("{{ hash: \"{wrong_hash}\" }}"), "the requested block hash is not the served head"),
        (format!("{{ number_gte: {} }}", number + 1), "the served head has not reached block.number_gte"),
        ("{}".into(), "block must contain hash, number, or number_gte"),
        ("{ hash: null }".into(), "block.hash must not be null"),
        ("{ number: null }".into(), "block.number must not be null"),
        ("{ number_gte: null }".into(), "block.number_gte must not be null"),
        ("{ number: -1 }".into(), "block number constraints must be non-negative"),
        ("{ number_gte: -1 }".into(), "block number constraints must be non-negative"),
    ] {
        for (field, root) in [("domain", "domain(id: \"alice.eth\""), ("domains", "domains(")] {
            let payload = post_graphql_allow_errors(
                database.app_state(),
                &format!("query {{ {root} block: {block}) {{ id }} }}"),
                json!({}),
            )
            .await?;
            assert_eq!(payload["errors"][0]["message"], json!(message));
            assert_eq!(payload["errors"][0]["path"], json!([field]));
            if message == "the requested block number is not the served head" {
                println!("{field} refusal: {payload}");
            }
            if field == "domain" {
                assert_eq!(payload["data"], json!({"domain": null}));
            } else {
                assert_eq!(payload["data"], Value::Null);
            }
        }
    }
    database.cleanup().await
}

async fn generated_domain_values(database: &TestDatabase, where_value: Value) -> Result<Vec<Value>> {
    let payload = post_graphql(
        database.app_state(),
        r#"query Domains($where: Domain_filter!) {
            domains(first: 200, orderBy: id, where: $where) {
                id name createdAt expiryDate owner { id } resolver { id }
            }
        }"#,
        json!({"where": where_value}),
    )
    .await?;
    payload["data"]["domains"]
        .as_array()
        .cloned()
        .context("generated Domain rows")
}

fn sql_like(value: &str, pattern: &str, nocase: bool) -> bool {
    let (value, pattern) = if nocase {
        (value.to_lowercase(), pattern.to_lowercase())
    } else {
        (value.to_owned(), pattern.to_owned())
    };
    fn walk(value: &[char], pattern: &[char]) -> bool {
        match pattern {
            [] => value.is_empty(),
            ['%', rest @ ..] => (0..=value.len()).any(|skip| walk(&value[skip..], rest)),
            ['_', rest @ ..] => !value.is_empty() && walk(&value[1..], rest),
            ['\\', escaped, rest @ ..] => {
                value.first() == Some(escaped) && walk(&value[1..], rest)
            }
            [literal, rest @ ..] => {
                value.first() == Some(literal) && walk(&value[1..], rest)
            }
        }
    }
    walk(&value.chars().collect::<Vec<_>>(), &pattern.chars().collect::<Vec<_>>())
}

fn operator_matches(row: &Value, member: &str, operand: &Value) -> bool {
    let field = if member.starts_with("id") { "id" } else { "name" };
    let Some(value) = row[field].as_str() else { return false };
    let scalar = operand.as_str().unwrap_or_default();
    match member {
        "id" | "name" => value == scalar,
        "id_not" | "name_not" => value != scalar,
        "id_gt" | "name_gt" => value > scalar,
        "id_gte" | "name_gte" => value >= scalar,
        "id_lt" | "name_lt" => value < scalar,
        "id_lte" | "name_lte" => value <= scalar,
        "id_in" | "name_in" => operand.as_array().is_some_and(|items| items.iter().any(|item| item == value)),
        "id_not_in" | "name_not_in" => operand.as_array().is_some_and(|items| !items.is_empty() && items.iter().all(|item| item != value)),
        _ => {
            let negative = member.contains("_not_");
            let nocase = member.ends_with("_nocase");
            let pattern = if member.contains("contains") {
                if scalar.starts_with('%') || scalar.ends_with('%') { scalar.to_owned() } else { format!("%{scalar}%") }
            } else if member.contains("starts_with") {
                format!("{scalar}%")
            } else {
                format!("%{scalar}")
            };
            sql_like(value, &pattern, nocase) != negative
        }
    }
}

#[tokio::test]
async fn graphql_generated_domain_all_id_and_name_operators_agree_with_served_fields() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let corpus = generated_domain_values(&database, json!({})).await?;
    let ids = corpus.iter().map(|row| row["id"].as_str().unwrap()).collect::<Vec<_>>();
    let names = corpus.iter().map(|row| row["name"].as_str().unwrap()).collect::<Vec<_>>();
    let mixed_id = ids[0].to_uppercase().replacen("0X", "0x", 1);
    for member in ["id_gt", "id_gte", "id_lt", "id_lte"] {
        let mut filter = crate::graphql::GeneratedDomainFilter::default();
        match member {
            "id_gt" => filter.id.gt = Some(mixed_id.clone()),
            "id_gte" => filter.id.gte = Some(mixed_id.clone()),
            "id_lt" => filter.id.lt = Some(mixed_id.clone()),
            "id_lte" => filter.id.lte = Some(mixed_id.clone()),
            _ => unreachable!(),
        }
        let mut statement = sqlx::QueryBuilder::<sqlx::Postgres>::new("");
        crate::graphql::push_generated_domain_filters(&mut statement, &filter);
        assert!(statement.sql().contains("nc.namehash COLLATE \"C\""), "mixed-case {member} must use the noncanonical C fallback: {}", statement.sql());
    }
    let cases = [
        ("id", json!(ids[0])), ("id_not", json!(ids[0])), ("id_gt", json!(ids[0])),
        ("id_gte", json!(ids[1])), ("id_lt", json!(ids[1])), ("id_lte", json!(ids[0])),
        ("id_gt", json!(mixed_id.clone())), ("id_gte", json!(mixed_id.clone())),
        ("id_lt", json!(mixed_id.clone())), ("id_lte", json!(mixed_id)),
        ("id_in", json!([ids[0], ids[0]])), ("id_not_in", json!([ids[0]])),
        ("name", json!(names[0])), ("name_not", json!(names[0])),
        ("name_gt", json!(names[0])), ("name_gte", json!(names[1])),
        ("name_lt", json!(names[1])), ("name_lte", json!(names[0])),
        ("name_in", json!([names[0], names[0]])), ("name_not_in", json!([names[0]])),
        ("name_contains", json!("ali")), ("name_contains_nocase", json!("ALI")),
        ("name_not_contains", json!("ALI")), ("name_not_contains_nocase", json!("ALI")),
        ("name_starts_with", json!("ali")), ("name_starts_with_nocase", json!("ALI")),
        ("name_not_starts_with", json!("ALI")), ("name_not_starts_with_nocase", json!("ALI")),
        ("name_ends_with", json!(".eth")), ("name_ends_with_nocase", json!(".ETH")),
        ("name_not_ends_with", json!(".ETH")), ("name_not_ends_with_nocase", json!(".ETH")),
    ];
    for (member, operand) in cases {
        let expected = corpus.iter().filter(|row| operator_matches(row, member, &operand))
            .map(|row| row["id"].clone()).collect::<Vec<_>>();
        let actual = generated_domain_values(&database, json!({(member): operand})).await?
            .into_iter().map(|row| row["id"].clone()).collect::<Vec<_>>();
        assert_eq!(actual, expected, "{member} must agree with served fields");
    }
    for member in ["id_in", "id_not_in", "name_in", "name_not_in"] {
        assert!(generated_domain_values(&database, json!({(member): []})).await?.is_empty(), "{member}");
    }
    for (member, operand) in [("name_contains", "%"), ("name_contains", "_"), ("name_contains", r"\%"), ("name_contains", "")] {
        let expected = corpus.iter().filter(|row| operator_matches(row, member, &json!(operand)))
            .map(|row| row["id"].clone()).collect::<Vec<_>>();
        let actual = generated_domain_values(&database, json!({(member): operand})).await?
            .into_iter().map(|row| row["id"].clone()).collect::<Vec<_>>();
        assert_eq!(actual, expected, "wildcard case {operand:?}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_members_conjoin_before_pagination() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let corpus = generated_domain_values(&database, json!({})).await?;
    let first = corpus[0]["id"].clone();
    for (left, left_value, right, right_value) in [
        ("id_gte", first.clone(), "name_ends_with", json!(".eth")),
        ("id", first.clone(), "id_in", json!([first.clone()])),
        ("name", json!("alice.eth"), "name_contains", json!("lic")),
        ("name_not", json!("bob.eth"), "name_starts_with", json!("ali")),
        ("id_gt", json!("0x00"), "id_lte", first.clone()),
        ("name_contains", json!("li"), "name_ends_with", json!("eth")),
    ] {
        let expected = corpus.iter().filter(|row| operator_matches(row, left, &left_value) && operator_matches(row, right, &right_value))
            .map(|row| row["id"].clone()).collect::<Vec<_>>();
        let actual = generated_domain_values(&database, json!({(left): left_value, (right): right_value})).await?
            .into_iter().map(|row| row["id"].clone()).collect::<Vec<_>>();
        assert_eq!(actual, expected, "{left} AND {right}");
    }
    let expected = corpus.iter().filter(|row| operator_matches(row, "name_ends_with", &json!(".eth")))
        .map(|row| row["id"].clone()).collect::<Vec<_>>();
    let payload = post_graphql(database.app_state(), "query($where: Domain_filter!) { domains(first: 1, skip: 1, orderBy: id, where: $where) { id } }", json!({"where":{"name_ends_with":".eth"}})).await?;
    assert_eq!(payload["data"]["domains"][0]["id"], expected[1]);
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_0x_ids_are_exact_no_matches() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    for id in [GRAPHQL_ALICE_NAMEHASH.replacen("0x", "0X", 1), GRAPHQL_ALICE_NAMEHASH.to_uppercase().replacen("0X", "0x", 1)] {
        for where_value in [json!({"id": id}), json!({"id_in": [id]})] {
            assert!(generated_domain_values(&database, where_value).await?.is_empty());
        }
    }
    assert_eq!(generated_domain_values(&database, json!({"id": GRAPHQL_ALICE_NAMEHASH})).await?.len(), 1);
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_explicit_nulls_are_not_omitted() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let corpus = generated_domain_values(&database, json!({})).await?;
    let all_ids = corpus.iter().map(|row| row["id"].clone()).collect::<Vec<_>>();
    for (member, expected) in [
        ("id", Vec::new()),
        ("id_not", all_ids.clone()),
        ("name", Vec::new()),
        ("name_not", all_ids),
    ] {
        let actual = generated_domain_values(&database, json!({(member): Value::Null})).await?
            .into_iter().map(|row| row["id"].clone()).collect::<Vec<_>>();
        assert_eq!(actual, expected, "explicit null: {member}");
    }
    for member in [
        "id_gt", "id_gte", "id_lt", "id_lte", "id_in", "id_not_in",
        "name_gt", "name_gte", "name_lt", "name_lte", "name_in", "name_not_in",
        "name_contains", "name_contains_nocase", "name_not_contains", "name_not_contains_nocase",
        "name_starts_with", "name_starts_with_nocase", "name_not_starts_with", "name_not_starts_with_nocase",
        "name_ends_with", "name_ends_with_nocase", "name_not_ends_with", "name_not_ends_with_nocase",
    ] {
        let payload = post_graphql_allow_errors(
            database.app_state(),
            "query($where: Domain_filter!) { domains(where: $where) { id } }",
            json!({"where": {(member): Value::Null}}),
        ).await?;
        assert!(payload["errors"][0]["message"].as_str().is_some_and(|error| error.contains(&format!("Domain_filter.{member} must not be null"))), "{payload}");
        assert_eq!(payload["data"]["domains"], Value::Null);
    }
    database.cleanup().await
}

#[tokio::test]
async fn graphql_change_block_remains_exact_upstream_only() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let introspection = post_graphql(database.app_state(), "query { __type(name: \"BlockChangedFilter\") { name } }", json!({})).await?;
    assert!(introspection["data"]["__type"].is_null());
    let payload = post_graphql_allow_errors(database.app_state(), "query { domains(where: { _change_block: { number_gte: 1 } }) { id } }", json!({})).await?;
    let error = payload["errors"][0]["message"].as_str().context("validation error")?;
    assert!(error.contains("Domain_filter") && error.contains("_change_block"), "{payload}");
    assert!(payload.get("data").is_none() || payload["data"].is_null());
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_order_values_match_served_fields() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    let introspection = post_graphql(database.app_state(), "query { __type(name: \"Domain_orderBy\") { enumValues { name } } }", json!({})).await?;
    let mut actual_values = introspection["data"]["__type"]["enumValues"].as_array().context("order values")?
        .iter().map(|value| value["name"].as_str().unwrap()).collect::<Vec<_>>();
    actual_values.sort_unstable();
    assert_eq!(actual_values, ["createdAt", "expiryDate", "id", "name", "owner", "owner__id", "registrationDate", "resolver"]);

    let corpus = generated_domain_values(&database, json!({})).await?;
    assert!(corpus.iter().any(|row| row["createdAt"] == "0"));
    assert!(corpus.iter().any(|row| row["expiryDate"].is_null()));
    assert!(corpus.iter().any(|row| row["resolver"].is_null()));
    assert!(corpus.iter().enumerate().any(|(index, row)| corpus[index + 1..].iter().any(|other| row["owner"] == other["owner"])), "owner tie fixture");
    for (order_by, pointer) in [
        ("id", "/id"), ("name", "/name"), ("createdAt", "/createdAt"),
        ("expiryDate", "/expiryDate"), ("owner", "/owner/id"),
        ("owner__id", "/owner/id"), ("resolver", "/resolver/id"),
    ] {
        for direction in ["asc", "desc"] {
            let mut expected = corpus.clone();
            expected.sort_by(|left, right| {
                let left_key = left.pointer(pointer).and_then(Value::as_str);
                let right_key = right.pointer(pointer).and_then(Value::as_str);
                let primary = match (left_key, right_key) {
                    (None, None) => std::cmp::Ordering::Equal,
                    (None, Some(_)) => if direction == "asc" { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less },
                    (Some(_), None) => if direction == "asc" { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater },
                    (Some(left), Some(right)) => {
                        let order = if matches!(order_by, "createdAt" | "expiryDate") {
                            left.parse::<i128>().unwrap().cmp(&right.parse::<i128>().unwrap())
                        } else {
                            left.cmp(right)
                        };
                        if direction == "asc" { order } else { order.reverse() }
                    },
                };
                primary
                    .then_with(|| {
                        let order = left["id"].as_str().cmp(&right["id"].as_str());
                        if direction == "asc" { order } else { order.reverse() }
                    })
            });
            let payload = post_graphql(database.app_state(), &format!("query {{ domains(first: 200, orderBy: {order_by}, orderDirection: {direction}) {{ id }} }}"), json!({})).await?;
            let actual = payload["data"]["domains"].as_array().context("ordered domains")?
                .iter().map(|row| row["id"].clone()).collect::<Vec<_>>();
            let expected = expected.into_iter().map(|row| row["id"].clone()).collect::<Vec<_>>();
            assert_eq!(actual, expected, "{order_by} {direction}");
        }
    }
    let mut local_expected = corpus.clone();
    local_expected.sort_by(|left, right| left["id"].as_str().cmp(&right["id"].as_str()));
    let payload = post_graphql(database.app_state(), "query { domains(first: 200, orderBy: registrationDate, orderDirection: asc) { id } }", json!({})).await?;
    assert_eq!(payload["data"]["domains"].as_array().context("registration order")?.iter().map(|row| row["id"].clone()).collect::<Vec<_>>(), local_expected.into_iter().map(|row| row["id"].clone()).collect::<Vec<_>>());
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_and_legacy_name_filters_remain_separate() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    sqlx::query("UPDATE bigname_phase.name_current SET raw_name = 'MiXeD.eth' WHERE namehash = $1")
        .bind(GRAPHQL_ALICE_NAMEHASH)
        .execute(&database.lookup_pool)
        .await?;
    let generated_raw = generated_domain_values(&database, json!({"name":"MiXeD.eth"})).await?;
    let generated_normalized = generated_domain_values(&database, json!({"name":"mixed.eth"})).await?;
    assert_eq!(generated_raw.len(), 1);
    assert!(generated_normalized.is_empty());
    for where_value in [json!({"name":"alice.eth"}), json!({"name_contains":"BO"})] {
        let payload = post_graphql(database.app_state(), "query($where: DomainFilter!) { domainConnection(first: 0, where: $where) { totalCount } }", json!({"where":where_value})).await?;
        assert_eq!(payload["data"]["domainConnection"]["totalCount"], json!(1));
    }
    database.cleanup().await
}
