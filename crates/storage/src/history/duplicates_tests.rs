use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::super::{
    EventHistoryReadFilter, duplicates::push_product_history_duplicate_filter,
    paging::push_history_filters, selectors::HistorySelector, source::push_history_source,
};

#[tokio::test]
async fn handoff_representatives_use_indexed_origin_bounds() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("handoff_plan").pool_max_connections(1))
            .await?;
    let result = {
        let mut connection = database.pool().acquire().await?;
        exercise_plan(&mut connection).await
    };
    database.cleanup().await?;
    result
}

async fn exercise_plan(connection: &mut PgConnection) -> Result<()> {
    sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
    ] {
        sqlx::raw_sql(baseline).execute(&mut *connection).await?;
    }
    sqlx::raw_sql(
        r#"
        INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
        SELECT 'ethereum-mainnet', 'block-' || n, n, to_timestamp(n), 'canonical'
        FROM generate_series(1, 100050) n;
        INSERT INTO normalized_events
            (event_identity, namespace, event_kind, source_family, manifest_version,
             chain_id, block_hash, block_number, transaction_hash, transaction_index,
             log_index, derivation_kind, canonicality_state, after_state)
        SELECT 'ens_v1_unwrapped_authority:1:ethereum-mainnet:block-' || n ||
                   ':tx-' || n || ':0:' ||
                   CASE WHEN n <= 100000 THEN 'ResolverChanged:0'
                        ELSE 'ResolverChanged:registry-fallback-handoff:node-' ||
                             n || ':resource-' || copy || ':resolver:0' END,
               'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1,
               'ethereum-mainnet', 'block-' || n, n, 'tx-' || n, 0, 0,
               'ens_v1_unwrapped_authority', 'canonical',
               jsonb_build_object('node', 'node-' || n)
        FROM generate_series(1, 100050) n
        CROSS JOIN generate_series(1, 2) copy WHERE n > 100000 OR copy = 1;
        ANALYZE normalized_events;
        ANALYZE chain_lineage;
    "#,
    )
    .execute(&mut *connection)
    .await?;
    let filter = EventHistoryReadFilter::default();
    let mut query = QueryBuilder::<Postgres>::new("SELECT count(*)::bigint");
    push_history_source(&mut query, false);
    push_history_filters(&mut query, &filter, true);
    push_product_history_duplicate_filter(&mut query, &filter, true);
    let sql = query.sql().to_owned();
    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) {sql}"))
        .fetch_one(&mut *connection)
        .await?;
    eprintln!("planned global count: {plan}");
    assert_no_unbounded_nested_loop_scan(&plan[0]["Plan"], false);
    let mut candidates = Vec::new();
    candidate_scans(&plan[0]["Plan"], false, &mut candidates);
    assert!(
        !candidates.is_empty(),
        "the production query must exercise a handoff lookup"
    );
    for scan in candidates {
        let bound = format!("{} {}", scan["Index Cond"], scan["Recheck Cond"]);
        assert!(
            bound.contains("chain_id")
                && (bound.contains("block_number") || bound.contains("block_hash")),
            "handoff candidate scan lacks indexed origin bounds: {scan}"
        );
    }
    // Execute only after the structural assertion: a bad plan fails deterministically,
    // without making a wall-clock timeout the regression contract.
    let actual: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) {sql}"
    ))
    .fetch_one(&mut *connection)
    .await?;
    let mut scans = Vec::new();
    candidate_scans(&actual[0]["Plan"], false, &mut scans);
    for scan in scans {
        let visited = scan["Actual Rows"].as_f64().unwrap_or(0.0)
            + scan["Rows Removed by Filter"].as_f64().unwrap_or(0.0);
        assert!(
            visited <= 2.0,
            "unrelated history reached the handoff search: {scan}"
        );
        eprintln!("handoff scan: {scan}");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&sql)
            .fetch_one(&mut *connection)
            .await?,
        100050
    );
    eprintln!("global handoff count plan: {actual}");
    Ok(())
}

fn candidate_scans<'a>(node: &'a Value, in_subplan: bool, output: &mut Vec<&'a Value>) {
    let in_subplan = in_subplan || node["Parent Relationship"] == "SubPlan";
    if in_subplan && node["Relation Name"] == "normalized_events" {
        output.push(node);
    }
    if let Some(children) = node["Plans"].as_array() {
        for child in children {
            candidate_scans(child, in_subplan, output);
        }
    }
}

fn assert_no_unbounded_nested_loop_scan(node: &Value, repeated: bool) {
    assert!(
        !(repeated && node["Node Type"] == "Seq Scan"),
        "global history plan repeats an unbounded table scan: {node}"
    );
    if let Some(children) = node["Plans"].as_array() {
        for child in children {
            let repeated = repeated
                || (node["Node Type"] == "Nested Loop" && child["Parent Relationship"] == "Inner");
            assert_no_unbounded_nested_loop_scan(child, repeated);
        }
    }
}

#[tokio::test]
async fn expanded_history_selectors_fit_postgres_bind_limit() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("history_binds").pool_max_connections(1))
            .await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
            .execute(&mut *connection)
            .await?;
        for baseline in [
            include_str!("../../../../schema-v2/baseline/01_chain.sql"),
            include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
            include_str!("../../../../schema-v2/baseline/03_identity.sql"),
            include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
            include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
        ] {
            sqlx::raw_sql(baseline).execute(&mut *connection).await?;
        }
        // Address expansion has no anchor count limit. Execute the production
        // outer filter and representative subquery with more than 32,768 anchors.
        // Empty history is sufficient: PostgreSQL rejects oversized bind lists
        // before row execution, independently of whether any anchor matches.
        let names: Vec<String> = (0..34000).map(|n| format!("ens:0x{n:064x}")).collect();
        let resources: Vec<uuid::Uuid> = (0..34000).map(uuid::Uuid::from_u128).collect();
        for selector in [
            HistorySelector::logical_names(names.clone()),
            HistorySelector::resources(resources.clone()),
            HistorySelector::logical_names_or_resources(
                names[..17000].to_vec(),
                resources[..17000].to_vec(),
            ),
        ] {
            let filter = EventHistoryReadFilter {
                selectors: vec![selector],
                ..Default::default()
            };
            let mut query = QueryBuilder::<Postgres>::new("SELECT count(*)::bigint");
            push_history_source(&mut query, false);
            push_history_filters(&mut query, &filter, true);
            push_product_history_duplicate_filter(&mut query, &filter, true);
            assert_eq!(
                query
                    .build_query_scalar::<i64>()
                    .fetch_one(&mut *connection)
                    .await?,
                0
            );
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
