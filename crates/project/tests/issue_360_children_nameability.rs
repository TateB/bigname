use anyhow::Result;
use bigname_project::{BatchRequest, Engine, RunMode};
use bigname_storage::load_children_current;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const PARENT: &str = "ens:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const V2_CHILD: &str = "ens:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const V1_CHILD: &str = "ens:0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const V1_LABELHASH: &str = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const V2_LABELHASH: &str = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const V2_RESOURCE: &str = "00000000-0000-0000-0000-000000000360";
const V2_BINDING: &str = "00000000-0000-0000-0000-000000000361";
const V2_REGISTRY: &str = "00000000-0000-0000-0000-000000000362";
const V2_REGISTRY_ADDRESS: &str = "0x0000000000000000000000000000000000000360";
const OWNER: &str = "0x0000000000000000000000000000000000000001";

fn block_hash(block: i64) -> String {
    format!("0x{block:064x}")
}

async fn database(prefix: &str) -> Result<(TestDatabase, PgPool)> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let pool = database.pool().clone();
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut transaction = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *transaction)
        .await?;
    raw_sql(&format!(
        "ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public",
        name.replace('"', r#""""#)
    ))
    .execute(&mut *transaction)
    .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await?;
    for script in [
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connection = pool.acquire().await?;
    sqlx::query("SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    drop(connection);
    for block in [10, 11, 12] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, block_number, block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, to_timestamp(1800000000 + $3), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(block))
        .bind(block)
        .execute(&pool)
        .await?;
    }
    Ok((database, pool))
}

#[allow(clippy::too_many_arguments)]
async fn insert_normalized_event(
    pool: &PgPool,
    identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<&str>,
    source_family: &str,
    event_kind: &str,
    log_index: i64,
    after_state: Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state, after_state, raw_fact_ref
         ) VALUES (
             $1, 'ens', $2, $3::uuid, $4, $5, 1, $6, 10, $7, $8, 0, $9,
             CASE WHEN $5 LIKE 'ens_v1_%' THEN 'ens_v1_unwrapped_authority'
                  ELSE 'ens_v2_registry_resource_surface' END,
             'canonical', $10, jsonb_build_object('event_identity', $1::text)
         )",
    )
    .bind(identity)
    .bind(logical_name_id)
    .bind(resource_id)
    .bind(event_kind)
    .bind(source_family)
    .bind(CHAIN)
    .bind(block_hash(10))
    .bind(format!("0x{:064x}", log_index + 100))
    .bind(log_index)
    .bind(after_state)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_parent_surface(pool: &PgPool) -> Result<()> {
    sqlx::query("INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state, chain_id, block_hash, block_number, canonicality_state) VALUES ($1, 'ens', 'parent.eth', ARRAY['parent', 'eth'], '\\x00', $2, ARRAY['0x1111111111111111111111111111111111111111111111111111111111111111', '0x2222222222222222222222222222222222222222222222222222222222222222'], 'ensip15', 'active', $3, $4, 10, 'canonical')")
    .bind(PARENT)
    .bind(PARENT.trim_start_matches("ens:"))
    .bind(CHAIN)
    .bind(block_hash(10))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_v2_shadow_child(pool: &PgPool) -> Result<()> {
    sqlx::query("INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state, chain_id, block_hash, block_number, canonicality_state) VALUES ($1, 'ens', 'child.parent.eth', ARRAY['child', 'parent', 'eth'], '\\x00', $2, ARRAY[$3, '0x1111111111111111111111111111111111111111111111111111111111111111', '0x2222222222222222222222222222222222222222222222222222222222222222'], 'ensip15', 'active', $4, $5, 10, 'canonical')")
    .bind(V2_CHILD)
    .bind(V2_CHILD.trim_start_matches("ens:"))
    .bind(V2_LABELHASH)
    .bind(CHAIN)
    .bind(block_hash(10))
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE name_surfaces
         SET visibility_state = 'shadow',
             deactivation_reason = 'issue 360 unnameable child fixture',
             deactivated_at = now()
         WHERE logical_name_id = $1",
    )
    .bind(V2_CHILD)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_v2_registry_path(pool: &PgPool) -> Result<()> {
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
    .bind(V2_RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(10))
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v2', to_timestamp(1700000000), $4, $5, 10, '{\"transaction_index\":0,\"log_index\":0}', 'canonical')")
    .bind(V2_BINDING)
    .bind(V2_CHILD)
    .bind(V2_RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(10))
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ($1::uuid, $2, 'contract')")
    .bind(V2_REGISTRY)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number, active_from_block_hash) VALUES ($1::uuid, $2, $3, 10, $4)")
    .bind(V2_REGISTRY)
    .bind(CHAIN)
    .bind(V2_REGISTRY_ADDRESS)
    .bind(block_hash(10))
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id, discovery_source, admission_basis, active_from_block_number, active_from_block_hash, canonicality_state, provenance) VALUES ($1, 'registry_announcement', $2::uuid, $2::uuid, 'fixture', 'fixture', 10, $3, 'canonical', '{\"transaction_index\":0,\"log_index\":0}')")
    .bind(CHAIN)
    .bind(V2_REGISTRY)
    .bind(block_hash(10))
    .execute(pool)
    .await?;
    insert_normalized_event(
        pool,
        "issue-360-v2-parent-registry",
        Some(PARENT),
        None,
        "ens_v2_registry_l1",
        "SubregistryChanged",
        1,
        json!({"subregistry": V2_REGISTRY_ADDRESS}),
    )
    .await?;
    insert_normalized_event(
        pool,
        "issue-360-v2-child-registration",
        Some(V2_CHILD),
        Some(V2_RESOURCE),
        "ens_v2_registry_l1",
        "RegistrationGranted",
        2,
        json!({
            "registry_contract_instance_id": V2_REGISTRY,
            "status": "registered",
            "registrant": OWNER
        }),
    )
    .await
}

async fn seed_v1_topology_only_child(pool: &PgPool) -> Result<()> {
    insert_normalized_event(
        pool,
        "issue-360-v1-topology-only-child",
        None,
        None,
        "ens_v1_registry_l1",
        "SubregistryChanged",
        1,
        json!({
            "node": PARENT.trim_start_matches("ens:"),
            "child_node": V1_CHILD.trim_start_matches("ens:"),
            "labelhash": V1_LABELHASH,
            "owner": OWNER
        }),
    )
    .await
}

async fn run_project(pool: &PgPool) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: 10,
            affected_from_block: 10,
            affected_to_block: 10,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    Ok(())
}

#[derive(Debug, sqlx::FromRow)]
struct ProjectedChild {
    parent_logical_name_id: String,
    child_logical_name_id: String,
    surface_class: String,
    namespace: String,
    raw_name: Option<Vec<u8>>,
    decoded_name: Option<String>,
    namehash: String,
    labelhash: String,
    owner: Option<String>,
    registrant: Option<String>,
    provenance: Value,
    chain_positions: Value,
    canonicality_summary: Value,
    manifest_version: i64,
}

async fn projected_child(pool: &PgPool, child: &str) -> Result<Option<ProjectedChild>> {
    Ok(sqlx::query_as(
        "SELECT parent_logical_name_id, child_logical_name_id, surface_class,
                namespace, raw_name, decoded_name, namehash, labelhash, owner,
                registrant, provenance, chain_positions, canonicality_summary,
                manifest_version
         FROM children_current
         WHERE parent_logical_name_id = $1 AND child_logical_name_id = $2",
    )
    .bind(PARENT)
    .bind(child)
    .fetch_optional(pool)
    .await?)
}

// Contracts: docs/architecture.md "Name → children" and docs/projections.md
// "Address and child collections" require the [ENSv2 authority
// arm](../../../docs/glossary.md#authority-epoch) to join the child's active
// [name surface](../../../docs/glossary.md#surface-name-surface).
#[tokio::test]
async fn ens_v2_child_without_active_surface_is_absent() -> Result<()> {
    let (database, pool) = database("issue360_v2_shadow").await?;
    seed_parent_surface(&pool).await?;
    seed_v2_shadow_child(&pool).await?;
    seed_v2_registry_path(&pool).await?;

    run_project(&pool).await?;

    assert!(
        projected_child(&pool, V2_CHILD).await?.is_none(),
        "an ENSv2 registration must not publish a child whose staged surface is shadow"
    );
    assert!(
        load_children_current(&pool, PARENT).await?.is_empty(),
        "storage must not serve an ENSv2 child absent from the projection"
    );
    database.cleanup().await
}

// Contracts: docs/architecture.md "Name → children" and docs/projections.md
// "Address and child collections" retain the ENSv1 topology row while leaving unknown names null.
#[tokio::test]
async fn ens_v1_topology_only_child_keeps_non_name_form() -> Result<()> {
    let (database, pool) = database("issue360_v1_hash_only").await?;
    seed_parent_surface(&pool).await?;
    seed_v1_topology_only_child(&pool).await?;

    run_project(&pool).await?;

    let child = projected_child(&pool, V1_CHILD)
        .await?
        .expect("an ENSv1 topology-only child must remain projected");
    assert_eq!(child.parent_logical_name_id, PARENT);
    assert_eq!(child.child_logical_name_id, V1_CHILD);
    assert_eq!(child.surface_class, "declared");
    assert_eq!(child.namespace, "ens");
    assert_eq!(child.namehash, V1_CHILD.trim_start_matches("ens:"));
    assert_eq!(child.labelhash, V1_LABELHASH);
    assert_eq!(child.owner.as_deref(), Some(OWNER));
    assert_eq!(child.registrant, None);
    assert_eq!(
        child.raw_name, None,
        "Project must not invent raw name bytes"
    );
    assert_eq!(
        child.decoded_name, None,
        "Project must not invent a decoded child name"
    );
    assert_eq!(child.provenance["chain_id"], CHAIN);
    assert_eq!(child.chain_positions["target_block_number"], 10);
    assert_eq!(child.chain_positions["target_block_hash"], block_hash(10));
    assert_eq!(child.canonicality_summary["state"], "canonical");
    assert_eq!(child.canonicality_summary["target_block_number"], 10);
    assert_eq!(
        child.canonicality_summary["target_block_hash"],
        block_hash(10)
    );
    assert_eq!(child.manifest_version, 1);

    // Contract: docs/api-v2-routes.md "GET /v2/names/{name}/subnames" requires
    // the prefix-free lowercase labelhash placeholder in both served name fields.
    let rows = load_children_current(&pool, PARENT).await?;
    assert_eq!(rows.len(), 1, "storage must serve the one projected child");
    let served = &rows[0];
    let placeholder =
        "[dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd].parent.eth";
    assert_eq!(served.parent_logical_name_id, PARENT);
    assert_eq!(served.child_logical_name_id, V1_CHILD);
    assert_eq!(served.surface_class, "declared");
    assert_eq!(served.namespace, "ens");
    assert_eq!(served.canonical_display_name, placeholder);
    assert_eq!(served.normalized_name, placeholder);
    assert_eq!(served.namehash, V1_CHILD.trim_start_matches("ens:"));
    assert_eq!(served.labelhash.as_deref(), Some(V1_LABELHASH));
    assert_eq!(served.owner.as_deref(), Some(OWNER));
    assert_eq!(served.registrant, None);
    assert_eq!(served.provenance["chain_id"], CHAIN);
    assert_eq!(served.chain_positions["target_block_number"], 10);
    assert_eq!(served.chain_positions["target_block_hash"], block_hash(10));
    assert_eq!(served.canonicality_summary["state"], "canonical");
    assert_eq!(served.canonicality_summary["target_block_number"], 10);
    assert_eq!(
        served.canonicality_summary["target_block_hash"],
        block_hash(10)
    );
    database.cleanup().await
}
