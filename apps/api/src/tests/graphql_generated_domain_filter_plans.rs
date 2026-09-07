fn plan_domain_filter(member: &str, value: &str) -> crate::graphql::GeneratedDomainFilter {
    let mut filter = crate::graphql::GeneratedDomainFilter::default();
    let list = || Some(vec![value.to_owned()]);
    match member {
        "id" => filter.id.eq = Some(Some(value.into())),
        "id_not" => filter.id.not = Some(Some(value.into())),
        "id_gt" => filter.id.gt = Some(value.into()),
        "id_gte" => filter.id.gte = Some(value.into()),
        "id_lt" => filter.id.lt = Some(value.into()),
        "id_lte" => filter.id.lte = Some(value.into()),
        "id_in" => filter.id.in_values = list(),
        "id_not_in" => filter.id.not_in_values = list(),
        "name" => filter.name.eq = Some(Some(value.into())),
        "name_not" => filter.name.not = Some(Some(value.into())),
        "name_gt" => filter.name.gt = Some(value.into()),
        "name_gte" => filter.name.gte = Some(value.into()),
        "name_lt" => filter.name.lt = Some(value.into()),
        "name_lte" => filter.name.lte = Some(value.into()),
        "name_in" => filter.name.in_values = list(),
        "name_not_in" => filter.name.not_in_values = list(),
        "name_contains" => filter.name.contains = Some(value.into()),
        "name_contains_nocase" => filter.name.contains_nocase = Some(value.into()),
        "name_not_contains" => filter.name.not_contains = Some(value.into()),
        "name_not_contains_nocase" => filter.name.not_contains_nocase = Some(value.into()),
        "name_starts_with" => filter.name.starts_with = Some(value.into()),
        "name_starts_with_nocase" => filter.name.starts_with_nocase = Some(value.into()),
        "name_not_starts_with" => filter.name.not_starts_with = Some(value.into()),
        "name_not_starts_with_nocase" => filter.name.not_starts_with_nocase = Some(value.into()),
        "name_ends_with" => filter.name.ends_with = Some(value.into()),
        "name_ends_with_nocase" => filter.name.ends_with_nocase = Some(value.into()),
        "name_not_ends_with" => filter.name.not_ends_with = Some(value.into()),
        "name_not_ends_with_nocase" => filter.name.not_ends_with_nocase = Some(value.into()),
        "owner" => filter.owner.eq = Some(Some(value.to_lowercase())),
        "owner_not" => filter.owner.not = Some(Some(value.to_lowercase())),
        "owner_gt" => filter.owner.gt = Some(value.into()),
        "owner_gte" => filter.owner.gte = Some(value.into()),
        "owner_lt" => filter.owner.lt = Some(value.into()),
        "owner_lte" => filter.owner.lte = Some(value.into()),
        "owner_in" => filter.owner.in_values = list(),
        "owner_not_in" => filter.owner.not_in_values = list(),
        "owner_contains" => filter.owner.contains = Some(value.into()),
        "owner_contains_nocase" => filter.owner.contains_nocase = Some(value.into()),
        "owner_not_contains" => filter.owner.not_contains = Some(value.into()),
        "owner_not_contains_nocase" => filter.owner.not_contains_nocase = Some(value.into()),
        "owner_starts_with" => filter.owner.starts_with = Some(value.into()),
        "owner_starts_with_nocase" => filter.owner.starts_with_nocase = Some(value.into()),
        "owner_not_starts_with" => filter.owner.not_starts_with = Some(value.into()),
        "owner_not_starts_with_nocase" => filter.owner.not_starts_with_nocase = Some(value.into()),
        "owner_ends_with" => filter.owner.ends_with = Some(value.into()),
        "owner_ends_with_nocase" => filter.owner.ends_with_nocase = Some(value.into()),
        "owner_not_ends_with" => filter.owner.not_ends_with = Some(value.into()),
        "owner_not_ends_with_nocase" => filter.owner.not_ends_with_nocase = Some(value.into()),
        _ => panic!("unknown generated Domain member {member}"),
    }
    filter
}

#[test]
fn noncanonical_id_ranges_pin_c_collation() {
    let mut sql = sqlx::QueryBuilder::<sqlx::Postgres>::new("");
    let filter = plan_domain_filter("id_gt", "0xA");
    crate::graphql::push_generated_domain_filters(&mut sql, &filter);
    assert!(sql.sql().contains("(nc.namehash COLLATE \"C\") >"), "{}", sql.sql());
}

#[tokio::test]
async fn graphql_generated_domain_owner_positive_plans_are_relation_bounded_or_linear() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_generated_owner_plans(&database).await?;
    let chains = ["ethereum-mainnet".to_owned()];
    let mut budget_failures = Vec::new();
    for (member, value) in [
        ("owner", "0x0000000000000000000000000000000000000672"),
        ("owner_in", "0x0000000000000000000000000000000000000672"),
        ("owner_gt", "0x0000000000000000000000000000000000000671"),
        ("owner_gte", "0x0000000000000000000000000000000000000671"),
        ("owner_lt", "z"), ("owner_lte", "0x0000000000000000000000000000000000000672"),
        ("owner_contains", "067"), ("owner_contains_nocase", "067"),
        ("owner_starts_with", "0x0000"), ("owner_starts_with_nocase", "0X0000"),
        ("owner_ends_with", "672"), ("owner_ends_with_nocase", "672"),
    ] {
        let filter = plan_domain_filter(member, value);
        let explain = crate::graphql::explain_phase_graphql_name_list_page(
            &database.lookup_pool, &chains, &filter, crate::graphql::GeneratedDomainSort::Id,
            bigname_storage::NameCurrentListOrder::Asc, 200, 0, false,
        ).await?;
        println!("OWNER POSITIVE {member} PLAN {}", serde_json::to_string_pretty(&explain)?);
        if let Err(error) = assert_owner_plan_limits(&explain, member) {
            budget_failures.push(format!("{member}: {error}"));
        }
        if matches!(member, "owner" | "owner_in" | "owner_contains") {
            let prefix = crate::graphql::explain_phase_graphql_name_list_page(
                &database.lookup_pool, &chains, &filter, crate::graphql::GeneratedDomainSort::Id,
                bigname_storage::NameCurrentListOrder::Asc, 200, 200, true,
            ).await?;
            println!("OWNER PREFIX {member} PLAN {}", serde_json::to_string_pretty(&prefix)?);
            let page = crate::graphql::explain_phase_graphql_name_list_page(
                &database.lookup_pool, &chains, &filter, crate::graphql::GeneratedDomainSort::Id,
                bigname_storage::NameCurrentListOrder::Asc, 200, 200, false,
            ).await?;
            println!("OWNER OFFSET PAGE {member} PLAN {}", serde_json::to_string_pretty(&page)?);
            let page_plan = &page[0]["Plan"];
            assert_eq!(page_plan["Node Type"], "Limit");
            assert_eq!(page_plan["Actual Rows"], if member == "owner_contains" { 200 } else { 1 });
            assert!(page_plan["Total Cost"].as_f64().unwrap_or(f64::MAX) < 100_000.0);
            assert!(page[0].get("JIT").is_none());
            assert_eq!(page_plan["Temp Read Blocks"], 0);
            assert_eq!(page_plan["Temp Written Blocks"], 0);
            assert!(plan_nodes(&page).iter().all(|node| node["Actual Loops"].as_u64().unwrap_or(0) <= 404));
            let plan = &prefix[0]["Plan"];
            assert_eq!(plan["Temp Written Blocks"], 0, "{member}: {plan}");
            assert_eq!(plan["Temp Read Blocks"], 0, "{member}: {plan}");
            assert!(prefix[0].get("JIT").is_none());
            assert!(plan["Total Cost"].as_f64().unwrap_or(f64::MAX) < 100_000.0);
            assert!(plan["Total Cost"].as_f64().unwrap_or(f64::MAX) + page_plan["Total Cost"].as_f64().unwrap_or(f64::MAX) < 200_000.0);
            let page_blocks = page_plan["Shared Hit Blocks"].as_u64().unwrap_or(0) + page_plan["Shared Read Blocks"].as_u64().unwrap_or(0);
            assert!(page_blocks <= 24_576, "offset page {member}: {page_blocks}");
            for (statement, max_names) in [(&prefix, 204), (&page, 404)] {
                let nodes = plan_nodes(statement);
                let outer = nodes.iter().find(|node| node["Relation Name"] == "name_current" && node["Alias"] == "nc").context("offset ordered name probe")?;
                assert!(outer["Actual Rows"].as_u64().unwrap_or(u64::MAX).saturating_mul(outer["Actual Loops"].as_u64().unwrap_or(u64::MAX)) <= max_names);
            }
            let blocks = plan["Shared Hit Blocks"].as_u64().unwrap_or(0) + plan["Shared Read Blocks"].as_u64().unwrap_or(0);
            assert!(blocks + page_blocks <= 34_816, "whole OFFSET request {member}: prefix {blocks} + page {page_blocks}");
            if blocks > 10_240 {
                budget_failures.push(format!("prefix {member}: {blocks} blocks exceeds 10240"));
            }
            assert!(plan_nodes(&prefix).iter().all(|node| node["Actual Loops"].as_u64().unwrap_or(0) <= 404), "{member}: {prefix}");
        }
        let nodes = plan_nodes(&explain);
        if matches!(member, "owner" | "owner_in") {
            let scan = nodes.iter().find(|node| node["Index Name"].as_str()
                .is_some_and(|index| index == "address_names_current_pkey" || index == "address_names_current_address_idx"))
                .with_context(|| format!("address-indexed relation scan: {member}"))?;
            assert_eq!(scan["Actual Loops"], 1, "{member}: {scan}");
            assert!(scan["Index Cond"].as_str().is_some_and(|condition| condition.contains("address")), "{member}: {scan}");
            let returned = crate::graphql::load_phase_graphql_name_list_page_offset(
                &database.lookup_pool,
                &bigname_storage::NameCurrentListFilter { namespace: Some("ens".into()), ..Default::default() },
                &chains, &filter, crate::graphql::GeneratedDomainSort::Id,
                bigname_storage::NameCurrentListOrder::Asc, 200, 0,
            ).await?;
            assert!(returned.iter().any(|row| row.row.row.namehash == format!("0x{:064x}", 1_199)), "late second-owner target: {member}");
        } else {
            assert!(nodes.iter().any(|node| node["Index Name"] == "address_names_current_name_idx"
                && node["Actual Loops"].as_u64().unwrap_or(0) <= 204), "page-driven indexed relation probe: {member}");
        }
    }
    database.cleanup().await?;
    assert!(budget_failures.is_empty(), "owner plan budgets: {budget_failures:?}");
    Ok(())
}

#[tokio::test]
async fn graphql_generated_domain_owner_negative_plans_are_page_bounded_anti_joins() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_generated_owner_plans(&database).await?;
    let chains = ["ethereum-mainnet".to_owned()];
    let mut budget_failures = Vec::new();
    for (member, value) in [
        ("owner_not", GRAPHQL_OWNER), ("owner_not_in", GRAPHQL_OWNER),
        ("owner_not_contains", "000a"), ("owner_not_contains_nocase", "000A"),
        ("owner_not_starts_with", GRAPHQL_OWNER),
        ("owner_not_starts_with_nocase", "0X000000000000000000000000000000000000000A"),
        ("owner_not_ends_with", "00a"), ("owner_not_ends_with_nocase", "00A"),
    ] {
        let filter = plan_domain_filter(member, value);
        let explain = crate::graphql::explain_phase_graphql_name_list_page(
            &database.lookup_pool, &chains, &filter, crate::graphql::GeneratedDomainSort::Id,
            bigname_storage::NameCurrentListOrder::Asc, 200, 0, false,
        ).await?;
        println!("OWNER NEGATIVE {member} PLAN {}", serde_json::to_string_pretty(&explain)?);
        if let Err(error) = assert_owner_plan_limits(&explain, member) {
            budget_failures.push(format!("{member}: {error}"));
        }
        let nodes = plan_nodes(&explain);
        let anti = nodes.iter().find(|node| node["Join Type"] == "Anti")
            .with_context(|| format!("anti-semijoin: {member}"))?;
        assert!(anti["Actual Loops"].as_u64().unwrap_or(u64::MAX) <= 204, "{member}: {anti}");
        let anti_blocks = anti["Shared Hit Blocks"].as_u64().unwrap_or(0) + anti["Shared Read Blocks"].as_u64().unwrap_or(0);
        if anti_blocks > 6_144 {
            budget_failures.push(format!("{member}: anti blocks {anti_blocks} exceeds 6144"));
        }
        let outer = nodes.iter().find(|node| node["Relation Name"] == "name_current" && node["Alias"] == "nc")
            .with_context(|| format!("ordered outer name_current scan: {member}"))?;
        assert!(outer["Actual Rows"].as_u64().unwrap_or(u64::MAX) <= 204, "{member}: {outer}");
        let probe = nodes.iter().find(|node| node["Index Name"] == "address_names_current_name_idx")
            .with_context(|| format!("name-keyed anti probe: {member}"))?;
        assert!(probe["Actual Loops"].as_u64().unwrap_or(u64::MAX) <= 204, "{member}: {probe}");
        let returned = crate::graphql::load_phase_graphql_name_list_page_offset(&database.lookup_pool, &bigname_storage::NameCurrentListFilter { namespace: Some("ens".into()), ..Default::default() }, &chains, &filter, crate::graphql::GeneratedDomainSort::Id, bigname_storage::NameCurrentListOrder::Asc, 200, 0).await?;
        assert!(returned.iter().any(|row| row.row.row.namehash == format!("0x{:064x}", 1_199)), "late second-owner target: {member}");
    }
    database.cleanup().await?;
    assert!(budget_failures.is_empty(), "owner plan budgets: {budget_failures:?}");
    Ok(())
}

#[tokio::test]
async fn graphql_generated_domain_owner_rare_pattern_plan_records_linear_direction_case() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_generated_owner_plans(&database).await?;
    let ordered_names: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bigname_phase.name_current WHERE namespace = 'ens' AND support_status = 'supported' AND chain_positions <> '{}'::JSONB").fetch_one(&database.lookup_pool).await?;
    let filter = plan_domain_filter("owner_ends_with", "00a");
    let explain = crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &["ethereum-mainnet".to_owned()], &filter,
        crate::graphql::GeneratedDomainSort::Id, bigname_storage::NameCurrentListOrder::Desc, 200, 0, false,
    ).await?;
    println!("OWNER RARE DESC PLAN {}", serde_json::to_string_pretty(&explain)?);
    let plan = &explain[0]["Plan"];
    assert_eq!(plan["Node Type"], "Limit");
    assert_eq!(plan["Actual Rows"], 6, "rare owner population: {plan}");
    let nodes = plan_nodes(&explain);
    let outer = nodes.iter().find(|node| node["Relation Name"] == "name_current" && node["Alias"] == "nc").context("ordered outer name_current scan")?;
    let probe = nodes.iter().find(|node| node["Index Name"] == "address_names_current_name_idx").context("name-keyed owner probe")?;
    assert_eq!(outer["Actual Rows"].as_i64(), Some(ordered_names), "full ordered walk: {outer}");
    assert_eq!(probe["Actual Loops"].as_i64(), Some(ordered_names), "linear owner probes: {probe}");
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_owner_negative_desc_plan_exercises_removal_bound() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_generated_owner_plans(&database).await?;
    let filter = plan_domain_filter("owner_not", GRAPHQL_OWNER);
    let explain = crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &["ethereum-mainnet".to_owned()], &filter,
        crate::graphql::GeneratedDomainSort::Id, bigname_storage::NameCurrentListOrder::Desc, 200, 0, false,
    ).await?;
    println!("OWNER NEGATIVE DESC PLAN {}", serde_json::to_string_pretty(&explain)?);
    assert_owner_plan_limits(&explain, "owner_not_desc")?;
    let nodes = plan_nodes(&explain);
    let outer = nodes.iter().find(|node| node["Relation Name"] == "name_current" && node["Alias"] == "nc").context("ordered outer name_current scan")?;
    assert_eq!(outer["Actual Rows"], 208, "descending removal corpus: {outer}");
    let rejected = nodes.iter().find(|node| node["Index Name"] == "address_names_current_name_idx" && node["Filter"].as_str().is_some_and(|filter| filter.contains(GRAPHQL_OWNER))).context("negative address rejection probe")?;
    let removed = rejected["Rows Removed by Filter"].as_u64().unwrap_or(0);
    assert!((1..=4).contains(&removed), "descending anti-probe removals: {rejected}");
    database.cleanup().await
}

fn assert_owner_plan_limits(explain: &Value, member: &str) -> Result<()> {
    let plan = &explain[0]["Plan"];
    assert_eq!(plan["Node Type"], "Limit", "{member}");
    assert_eq!(plan["Actual Rows"], 200, "{member}: requested page");
    anyhow::ensure!(plan["Total Cost"].as_f64().unwrap_or(f64::MAX) < 100_000.0, "{member}: page cost exceeds 100000: {plan}");
    assert!(explain[0].get("JIT").is_none(), "{member}: {explain}");
    assert_eq!(plan["Temp Read Blocks"].as_u64().unwrap_or(0), 0, "{member}");
    assert_eq!(plan["Temp Written Blocks"].as_u64().unwrap_or(0), 0, "{member}");
    anyhow::ensure!(plan["Shared Hit Blocks"].as_u64().unwrap_or(0) + plan["Shared Read Blocks"].as_u64().unwrap_or(0) <= 12_288, "{member}: page blocks exceeds 12288: {plan}");
    assert!(plan_nodes(explain).iter().all(|node| node["Actual Loops"].as_u64().unwrap_or(0) < 5_000), "{member}: {explain}");
    let text = serde_json::to_string(plan)?;
    assert!(text.contains("effective_controller") && text.contains("canonical_lineage"), "eligible effective owner below Limit: {member}");
    Ok(())
}

async fn pad_generated_domain_plans(database: &TestDatabase) -> Result<()> {
    pad_resolver_planner_statistics(database).await?;
    let primary_owner = "0x0000000000000000000000000000000000000671";
    let alternate_owner = "0x0000000000000000000000000000000000000672";
    let alternate_resolver = "0x0000000000000000000000000000000000000673";
    let changed = sqlx::query(
        r#"UPDATE bigname_phase.name_current
              SET declared_summary = JSONB_SET(
                    JSONB_SET(declared_summary, '{control,registry_owner}', TO_JSONB(
                        CASE WHEN namehash >= '0x000000000000000000000000000000000000000000000000000000000000176c'
                             THEN $1::TEXT
                             WHEN namehash < '0x00000000000000000000000000000000000000000000000000000000000004b0'
                             THEN $3::TEXT ELSE $2::TEXT END)),
                    '{resolver,address}', TO_JSONB($4::TEXT))
            WHERE namehash BETWEEN '0x00000000000000000000000000000000000000000000000000000000000003e8'
                               AND '0x000000000000000000000000000000000000000000000000000000000000176f'"#,
    )
    .bind(GRAPHQL_OWNER)
    .bind(primary_owner)
    .bind(alternate_owner)
    .bind(alternate_resolver)
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(changed.rows_affected(), 5_000);
    sqlx::query("ANALYZE bigname_phase.name_current")
        .execute(&database.lookup_pool)
        .await?;
    Ok(())
}

async fn pad_generated_owner_plans(database: &TestDatabase) -> Result<()> {
    pad_generated_domain_plans(database).await?;
    for (index, owner) in ["0x0000000000000000000000000000000000000671", "0x0000000000000000000000000000000000000672"].into_iter().enumerate() {
        let name = format!("owner-plan-extra-{index}.eth");
        seed_identity_name(database, &format!("ens:{name}"), &name, &name,
            &bigname_lookup::ens_namehash_hex(&name)?, Uuid::from_u128(0x670_4001 + index as u128 * 3),
            Uuid::from_u128(0x670_4002 + index as u128 * 3), Uuid::from_u128(0x670_4003 + index as u128 * 3),
            owner, bigname_storage::AddressNameRelation::EffectiveController, 740 + index as i64).await?;
    }
    let resources = sqlx::query(
        r#"WITH generated AS (
              SELECT * FROM bigname_phase.name_surfaces
              WHERE namehash BETWEEN '0x00000000000000000000000000000000000000000000000000000000000003e8'
                                 AND '0x000000000000000000000000000000000000000000000000000000000000176f'
            ), lineages AS (
              INSERT INTO bigname_phase.token_lineages (
                token_lineage_id, chain_id, block_hash, block_number, canonicality_state
              ) SELECT MD5('owner-token-' || logical_name_id)::UUID,
                  chain_id, block_hash, block_number, canonicality_state FROM generated
              RETURNING *
            ) INSERT INTO bigname_phase.resources (
              resource_id, token_lineage_id, chain_id, block_hash, block_number, canonicality_state
            ) SELECT MD5('owner-resource-' || generated.logical_name_id)::UUID,
                lineages.token_lineage_id, lineages.chain_id, lineages.block_hash,
                lineages.block_number, lineages.canonicality_state
              FROM generated JOIN lineages
                ON lineages.token_lineage_id = MD5('owner-token-' || generated.logical_name_id)::UUID"#,
    ).execute(&database.lookup_pool).await?;
    assert_eq!(resources.rows_affected(), 5_000);
    let bindings = sqlx::query(
        r#"WITH source AS (
              SELECT binding.* FROM bigname_phase.surface_bindings binding
              JOIN bigname_phase.name_current nc
                ON nc.surface_binding_id = binding.surface_binding_id
              WHERE nc.raw_name = 'alice.eth'
            ), generated AS (
              SELECT * FROM bigname_phase.name_surfaces
              WHERE namehash BETWEEN '0x00000000000000000000000000000000000000000000000000000000000003e8'
                                 AND '0x000000000000000000000000000000000000000000000000000000000000176f'
            ) INSERT INTO bigname_phase.surface_bindings (
              surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm,
              active_from, active_to, chain_id, block_hash, block_number, provenance, canonicality_state
            ) SELECT MD5('owner-plan-' || generated.logical_name_id)::UUID,
                generated.logical_name_id, MD5('owner-resource-' || generated.logical_name_id)::UUID, source.binding_kind, source.authority_arm,
                source.active_from, source.active_to, generated.chain_id, generated.block_hash,
                generated.block_number, source.provenance, source.canonicality_state
              FROM generated CROSS JOIN source"#,
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(bindings.rows_affected(), 5_000);
    sqlx::query(
        r#"UPDATE bigname_phase.name_current nc SET
              surface_binding_id = binding.surface_binding_id, resource_id = binding.resource_id,
              token_lineage_id = resource.token_lineage_id, binding_kind = binding.binding_kind
            FROM bigname_phase.surface_bindings binding
            JOIN bigname_phase.resources resource ON resource.resource_id = binding.resource_id
            WHERE binding.logical_name_id = nc.logical_name_id
              AND binding.surface_binding_id = MD5('owner-plan-' || nc.logical_name_id)::UUID"#,
    )
    .execute(&database.lookup_pool)
    .await?;
    let relations = sqlx::query(
        r#"WITH source AS (
              SELECT * FROM bigname_phase.address_names_current
              WHERE raw_name = 'alice.eth' AND relation = 'effective_controller'
            ) INSERT INTO bigname_phase.address_names_current (
              address, logical_name_id, relation, namespace, raw_name, namehash,
              surface_binding_id, resource_id, token_lineage_id, binding_kind,
              support_status, unsupported_reason, provenance, chain_positions,
              canonicality_summary, manifest_version
            ) SELECT nc.declared_summary #>> '{control,registry_owner}', nc.logical_name_id,
                'effective_controller', nc.namespace, nc.raw_name, nc.namehash,
                nc.surface_binding_id, nc.resource_id, nc.token_lineage_id, nc.binding_kind,
                source.support_status, source.unsupported_reason, source.provenance,
                source.chain_positions, source.canonicality_summary, source.manifest_version
              FROM bigname_phase.name_current nc CROSS JOIN source
              WHERE nc.namehash BETWEEN '0x00000000000000000000000000000000000000000000000000000000000003e8'
                                    AND '0x000000000000000000000000000000000000000000000000000000000000176f'"#,
    )
    .execute(&database.lookup_pool)
    .await?;
    assert_eq!(relations.rows_affected(), 5_000);
    let eligible_relations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bigname_phase.address_names_current WHERE relation = 'effective_controller'").fetch_one(&database.lookup_pool).await?;
    assert_eq!(eligible_relations, 5_004);
    sqlx::query("ANALYZE bigname_phase.name_current, bigname_phase.address_names_current, bigname_phase.name_surfaces, bigname_phase.resources, bigname_phase.surface_bindings, bigname_phase.chain_lineage, bigname_phase.token_lineages")
        .execute(&database.lookup_pool)
        .await?;
    let mut eligible = sqlx::QueryBuilder::<sqlx::Postgres>::new("");
    let storage_filter = bigname_storage::NameCurrentListFilter { namespace: Some("ens".into()), ..Default::default() };
    let owner_filter = plan_domain_filter("owner_contains", "0x");
    let chains = ["ethereum-mainnet".to_owned()];
    crate::graphql::push_filtered_names(
        &mut eligible, &storage_filter, None, Some(&owner_filter), Some(&chains), false,
    );
    eligible.push(" SELECT COUNT(*), COUNT(*) FILTER (WHERE surface_binding_id IS NOT NULL AND resource_id IS NOT NULL AND token_lineage_id IS NOT NULL AND binding_kind IS NOT NULL), COUNT(DISTINCT surface_binding_id), COUNT(DISTINCT resource_id), COUNT(DISTINCT token_lineage_id) FROM filtered_names");
    let counts: (i64, i64, i64, i64, i64) = eligible.build_query_as().fetch_one(&database.lookup_pool).await?;
    assert_eq!(counts, (5_004, 5_004, 5_004, 5_004, 5_004), "every eligible owner name must retain its own binding/resource/lineage");
    Ok(())
}

fn assert_domain_plan_shape(explain: &Value, label: &str) -> Result<()> {
    let plan = &explain[0]["Plan"];
    assert_eq!(plan["Node Type"], "Limit", "{label}");
    let text = serde_json::to_string(plan)?;
    assert!(text.contains("name_current"), "predicate must execute below Limit: {label}");
    assert!(text.contains("chain_positions") && text.contains("supported"), "snapshot eligibility: {label}");
    assert!(text.contains("chain_lineage_readable_height_idx"), "lineage index: {label}");
    for node in plan_nodes(explain) {
        if node["Node Type"] == "Sort" {
            assert!(matches!(node["Sort Method"].as_str(), Some("top-N heapsort" | "quicksort")), "sort method: {label}: {node}");
            assert!(node["Sort Space Used"].as_u64().unwrap_or(0) <= 2_048, "sort memory: {label}");
        }
    }
    Ok(())
}

fn assert_id_index_bounded(explain: &Value, label: &str) -> Result<()> {
    assert_domain_plan_shape(explain, label)?;
    let nodes = plan_nodes(explain);
    let scan = nodes.iter().find(|node| node["Node Type"] == "Index Scan" && node["Index Name"] == "name_current_lookup_idx")
        .with_context(|| format!("name_current_lookup_idx Index Scan: {label}"))?;
    assert!(scan["Actual Rows"].as_u64().unwrap_or(0) <= 204, "index rows: {label}: {scan}");
    assert_eq!(scan["Actual Loops"], 1, "index loops: {label}: {scan}");
    assert!(scan["Rows Removed by Filter"].as_u64().unwrap_or(0) <= 4, "index removals: {label}: {scan}");
    Ok(())
}

fn assert_flat_eligibility(explain: &Value, label: &str) -> Result<()> {
    let surface = plan_nodes(explain)
        .into_iter()
        .find(|node| node["Relation Name"] == "name_surfaces" && node["Alias"] == "surface")
        .with_context(|| format!("name_surfaces plan: {label}"))?;
    assert_eq!(surface["Actual Loops"], 1, "flat eligibility join: {label}: {surface}");
    assert!(explain[0].get("JIT").is_none(), "linear page must not trigger JIT: {label}: {explain}");
    assert!(explain[0]["Plan"]["Total Cost"].as_f64().unwrap_or(f64::MAX) < 100_000.0, "linear page cost must stay below jit_above_cost: {label}: {explain}");
    Ok(())
}

#[tokio::test]
async fn graphql_generated_domain_operator_plans_are_index_bounded_or_linear() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_generated_domain_plans(&database).await?;
    let chains = vec!["ethereum-mainnet".to_owned()];
    let target_id = format!("0x{:064x}", 5_999);
    let locale = sqlx::query("SELECT VERSION() AS version, datlocprovider::TEXT AS provider, datcollate AS locale FROM pg_database WHERE datname = CURRENT_DATABASE()")
        .fetch_one(&database.lookup_pool).await?;
    let version: String = locale.try_get("version")?;
    let provider: String = locale.try_get("provider")?;
    let locale: String = locale.try_get("locale")?;
    if !version.starts_with("PostgreSQL 16.") {
        eprintln!("skipping PostgreSQL 16 image identity check on {version}; byte-order assertions remain applicable");
    }
    assert!(!provider.is_empty() && !locale.is_empty(), "collation authority: {provider}/{locale}");
    let database_range = sqlx::query_scalar::<_, String>("SELECT namehash FROM bigname_phase.name_current WHERE namespace = 'ens' AND namehash >= $1 ORDER BY namehash").bind("0x").fetch_all(&database.lookup_pool).await?;
    let byte_range = sqlx::query_scalar::<_, String>("SELECT namehash FROM bigname_phase.name_current WHERE namespace = 'ens' AND convert_to(namehash, 'UTF8') >= convert_to($1, 'UTF8') ORDER BY convert_to(namehash, 'UTF8')").bind("0x").fetch_all(&database.lookup_pool).await?;
    assert_eq!(database_range, byte_range, "fixed-width hexadecimal order must match UTF-8 byte order under {provider}/{locale} on {version}");
    let pair = vec![format!("0x2a{}", "0".repeat(62)), format!("0x10a{}", "0".repeat(61))];
    let database_pair = sqlx::query_scalar::<_, String>("SELECT value FROM UNNEST($1::text[]) sample(value) ORDER BY value").bind(&pair).fetch_all(&database.lookup_pool).await?;
    let byte_pair = sqlx::query_scalar::<_, String>("SELECT value FROM UNNEST($1::text[]) sample(value) ORDER BY convert_to(value, 'UTF8')").bind(&pair).fetch_all(&database.lookup_pool).await?;
    assert_eq!(database_pair, byte_pair, "deployed collation must order hexadecimal text bytewise under {provider}/{locale} on {version}");
    let adversarial = vec!["B", "a", "0xA", "0xa"];
    let byte_adversarial = sqlx::query_scalar::<_, String>("SELECT value FROM UNNEST($1::text[]) sample(value) ORDER BY convert_to(value, 'UTF8')").bind(&adversarial).fetch_all(&database.lookup_pool).await?;
    let has_icu = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_collation WHERE collname = 'en-US-x-icu')",
    )
    .fetch_one(&database.lookup_pool)
    .await?;
    if has_icu {
        let icu_adversarial = sqlx::query_scalar::<_, String>("SELECT value FROM UNNEST($1::text[]) sample(value) ORDER BY value COLLATE \"en-US-x-icu\"").bind(&adversarial).fetch_all(&database.lookup_pool).await?;
        assert_ne!(byte_adversarial, icu_adversarial, "negative control must distinguish locale ordering from byte ordering");
    } else {
        eprintln!("skipping ICU negative control: server does not provide en-US-x-icu");
    }
    crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &chains, &Default::default(),
        crate::graphql::GeneratedDomainSort::Id, bigname_storage::NameCurrentListOrder::Desc,
        200, 0, false,
    ).await?;
    let members = [
        "id", "id_not", "id_gt", "id_gte", "id_lt", "id_lte", "id_in", "id_not_in",
        "name", "name_not", "name_gt", "name_gte", "name_lt", "name_lte", "name_in", "name_not_in",
        "name_contains", "name_contains_nocase", "name_not_contains", "name_not_contains_nocase",
        "name_starts_with", "name_starts_with_nocase", "name_not_starts_with", "name_not_starts_with_nocase",
        "name_ends_with", "name_ends_with_nocase", "name_not_ends_with", "name_not_ends_with_nocase",
    ];
    for member in members {
        let value = match member {
            "id_not" | "id_not_in" => "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe",
            "id_gt" | "id_gte" => "0x000000000000000000000000000000000000000000000000000000000000176e",
            "id_lt" | "id_lte" => "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            value if value.starts_with("id") => target_id.as_str(),
            value if value.starts_with("name_not") => "never-present",
            "name_gt" | "name_gte" => "alice.eth5998",
            "name_lt" | "name_lte" => "zzzz",
            value if value.contains("starts") => "alice",
            value if value.contains("ends") || value.contains("contains") => "5999",
            _ => "alice.eth5999",
        };
        let filter = plan_domain_filter(member, value);
        let explain = crate::graphql::explain_phase_graphql_name_list_page(
            &database.lookup_pool, &chains, &filter,
            crate::graphql::GeneratedDomainSort::Id, bigname_storage::NameCurrentListOrder::Desc,
            200, 0, false,
        ).await?;
        println!("DOMAIN OPERATOR {member} PLAN {}", serde_json::to_string_pretty(&explain)?);
        if matches!(member, "id" | "id_gt" | "id_gte" | "id_lt" | "id_lte" | "id_in") {
            assert_id_index_bounded(&explain, member)?;
        } else {
            assert_domain_plan_shape(&explain, member)?;
        }
        assert!(
            serde_json::to_string(&explain[0]["Plan"])?.contains(value),
            "bound predicate value must occur below Limit: {member}"
        );
        let returned = crate::graphql::load_phase_graphql_name_list_page_offset(
            &database.lookup_pool,
            &bigname_storage::NameCurrentListFilter { namespace: Some("ens".into()), ..Default::default() },
            &chains, &filter, crate::graphql::GeneratedDomainSort::Id,
            bigname_storage::NameCurrentListOrder::Desc, 200, 0,
        ).await?;
        assert!(returned.iter().any(|row| row.row.row.namehash == target_id), "target: {member}");
    }

    let late_name = plan_domain_filter("name", "alice.eth5999");
    let late_explain = crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &chains, &late_name,
        crate::graphql::GeneratedDomainSort::Id, bigname_storage::NameCurrentListOrder::Asc,
        200, 0, false,
    ).await?;
    let late_scan = plan_nodes(&late_explain)
        .into_iter()
        .find(|node| node["Relation Name"] == "name_current" && node["Alias"] == "nc")
        .context("name_current plan for late name match")?;
    assert!(late_scan["Rows Removed by Filter"].as_u64().unwrap_or(0) >= 5_000, "late name predicate must be exercised after scanning the fixture: {late_scan}");
    let returned = crate::graphql::load_phase_graphql_name_list_page_offset(
        &database.lookup_pool,
        &bigname_storage::NameCurrentListFilter { namespace: Some("ens".into()), ..Default::default() },
        &chains, &late_name, crate::graphql::GeneratedDomainSort::Id,
        bigname_storage::NameCurrentListOrder::Asc, 200, 0,
    ).await?;
    assert!(returned.iter().any(|row| row.row.row.namehash == target_id), "late name target");
    database.cleanup().await
}

#[tokio::test]
async fn graphql_generated_domain_order_plans_are_index_bounded_or_linear() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_generated_domain_plans(&database).await?;
    let chains = vec!["ethereum-mainnet".to_owned()];
    crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &chains, &Default::default(),
        crate::graphql::GeneratedDomainSort::Id, bigname_storage::NameCurrentListOrder::Asc,
        200, 0, false,
    ).await?;
    let sorts = [
        crate::graphql::GeneratedDomainSort::Id,
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::Name),
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::CreatedAt),
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::ExpiryDate),
        crate::graphql::GeneratedDomainSort::Owner,
        crate::graphql::GeneratedDomainSort::OwnerId,
        crate::graphql::GeneratedDomainSort::Resolver,
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::RegistrationDate),
    ];
    for sort in sorts {
        let explain = crate::graphql::explain_phase_graphql_name_list_page(
            &database.lookup_pool, &chains, &Default::default(), sort,
            bigname_storage::NameCurrentListOrder::Asc, 200, 0, false,
        ).await?;
        println!("DOMAIN ORDER {sort:?} PLAN {}", serde_json::to_string_pretty(&explain)?);
        if sort == crate::graphql::GeneratedDomainSort::Id {
            assert_id_index_bounded(&explain, "id order")?;
            assert!(!plan_nodes(&explain).iter().any(|node| node["Node Type"] == "Sort"), "id order must not sort");
        } else {
            assert_domain_plan_shape(&explain, &format!("{sort:?}"))?;
            assert_flat_eligibility(&explain, &format!("{sort:?}"))?;
            assert!(plan_nodes(&explain).iter().any(|node| node["Node Type"] == "Sort"), "sort must occur below Limit: {sort:?}");
        }
    }
    let id_lt = plan_domain_filter("id_lt", "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    let unselective_range = crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &chains, &id_lt,
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::Name),
        bigname_storage::NameCurrentListOrder::Asc, 200, 0, false,
    ).await?;
    assert_flat_eligibility(&unselective_range, "unselective id_lt with name order")?;

    let id_gt = plan_domain_filter("id_gt", "0x000000000000000000000000000000000000000000000000000000000000176e");
    let selective_range = crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &chains, &id_gt,
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::Name),
        bigname_storage::NameCurrentListOrder::Asc, 200, 0, false,
    ).await?;
    println!("DOMAIN SELECTIVE ID_GT NAME PLAN {}",
        serde_json::to_string_pretty(&selective_range)?);
    assert_flat_eligibility(&selective_range, "selective id_gt with name order")?;

    let bounded_id = "0x000000000000000000000000000000000000000000000000000000000000176f";
    let id = plan_domain_filter("id", bounded_id);
    let bounded_equality = crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &chains, &id,
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::Name),
        bigname_storage::NameCurrentListOrder::Asc, 200, 0, false,
    ).await?;
    assert_id_index_bounded(&bounded_equality, "id equality with name order")?;

    let id_in = plan_domain_filter("id_in", bounded_id);
    let bounded_membership = crate::graphql::explain_phase_graphql_name_list_page(
        &database.lookup_pool, &chains, &id_in,
        crate::graphql::GeneratedDomainSort::Storage(bigname_storage::NameCurrentListSort::Name),
        bigname_storage::NameCurrentListOrder::Asc, 200, 0, false,
    ).await?;
    assert_id_index_bounded(&bounded_membership, "id membership with name order")?;
    database.cleanup().await
}

#[tokio::test]
#[ignore = "run on glibc: docker rm -f bigname-test-postgres-glibc; BIGNAME_TEST_POSTGRES_IMAGE=postgres:16-bookworm BIGNAME_TEST_POSTGRES_CONTAINER=bigname-test-postgres-glibc BIGNAME_TEST_POSTGRES_PORT=59555 ./scripts/test-db -- cargo test -p bigname-api tests::graphql_glibc_en_us_hexadecimal_order_matches_bytes -- --ignored --exact"]
async fn graphql_glibc_en_us_hexadecimal_order_matches_bytes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let locale = sqlx::query("SELECT VERSION() AS version, datlocprovider::TEXT AS provider, datcollate AS locale FROM pg_database WHERE datname = CURRENT_DATABASE()")
        .fetch_one(&database.lookup_pool).await?;
    let version: String = locale.try_get("version")?;
    let provider: String = locale.try_get("provider")?;
    let locale: String = locale.try_get("locale")?;
    assert!(
        !version.contains("musl"),
        "glibc probe cannot run on {version}; rerun with: BIGNAME_TEST_POSTGRES_IMAGE=postgres:16-bookworm BIGNAME_TEST_POSTGRES_CONTAINER=bigname-test-postgres-glibc BIGNAME_TEST_POSTGRES_PORT=59555 ./scripts/test-db -- cargo test -p bigname-api tests::graphql_glibc_en_us_hexadecimal_order_matches_bytes -- --ignored --exact"
    );
    assert_eq!(provider, "c", "glibc probe requires libc provider: {provider}");
    assert!(locale.to_ascii_lowercase().starts_with("en_us"), "glibc probe requires en_US locale: {locale}");
    let database_order = sqlx::query_scalar::<_, String>("SELECT '0x' || LPAD(TO_HEX(value), 64, '0') AS namehash FROM GENERATE_SERIES(0, 24999) value ORDER BY namehash")
        .fetch_all(&database.lookup_pool).await?;
    let byte_order = sqlx::query_scalar::<_, String>("SELECT '0x' || LPAD(TO_HEX(value), 64, '0') AS namehash FROM GENERATE_SERIES(0, 24999) value ORDER BY convert_to('0x' || LPAD(TO_HEX(value), 64, '0'), 'UTF8')")
        .fetch_all(&database.lookup_pool).await?;
    assert_eq!(database_order, byte_order, "25,000 lowercase hexadecimal strings under {locale}");
    database.cleanup().await
}

#[tokio::test]
async fn graphql_legacy_count_plan_keeps_flat_eligibility_join() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_graphql_compat_fixture(&database).await?;
    pad_generated_domain_plans(&database).await?;
    let filter = bigname_storage::NameCurrentListFilter { namespace: Some("ens".into()), ..Default::default() };
    let chains = vec!["ethereum-mainnet".into()];
    let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ");
    crate::graphql::push_filtered_names(&mut builder, &filter, None, None, Some(&chains), false);
    builder.push(" SELECT COUNT(*) FROM filtered_names");
    let explain: Value = builder.build().fetch_one(&database.lookup_pool).await?.try_get(0)?;
    let surface = plan_nodes(&explain).into_iter().find(|node| node["Relation Name"] == "name_surfaces").context("name_surfaces plan")?;
    assert_eq!(surface["Actual Loops"], 1, "count eligibility must remain flat: {surface}");
    assert!(explain[0].get("JIT").is_none(), "count plan must not trigger JIT: {explain}");
    database.cleanup().await
}
