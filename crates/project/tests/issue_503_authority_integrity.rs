use alloy_primitives::keccak256;
use anyhow::{Context, Result};
use bigname_project::{
    BatchRequest, DUAL_CURRENT_CHILD_AUTHORITY, DUAL_CURRENT_EXACT_NAME_AUTHORITY, Engine, RunMode,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000503";

async fn database(prefix: &str) -> Result<(TestDatabase, PgPool)> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let pool = database.pool().clone();
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut tx = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *tx)
        .await?;
    raw_sql(&format!(
        "ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public",
        name.replace('"', r#""""#)
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *tx)
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
        raw_sql(script).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        connections.push(pool.acquire().await?);
    }
    for connection in &mut connections {
        sqlx::query("SET search_path TO bigname_phase, public")
            .execute(&mut **connection)
            .await?;
    }
    drop(connections);
    sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES ($1, $2, 10, '2026-08-26T00:00:00Z', 'canonical')")
        .bind(CHAIN).bind(HASH).execute(&pool).await?;
    Ok((database, pool))
}

fn uuid(kind: u8, index: u16) -> String {
    format!("{kind:08x}-0000-0000-0000-{index:012x}")
}

fn labelhash(label: &str) -> String {
    format!("{:#x}", keccak256(label.as_bytes()))
}

async fn surface(pool: &PgPool, index: u16, raw_name: &str, arms: &[&str]) -> Result<String> {
    let labels: Vec<_> = if raw_name.is_empty() {
        vec![]
    } else {
        raw_name.split('.').collect()
    };
    let hash = format!(
        "{:#x}",
        bigname_storage::ens_namehash_label_bytes(
            &labels
                .iter()
                .map(|label| label.as_bytes())
                .collect::<Vec<_>>()
        )
    );
    let logical = format!("ens:{hash}");
    let labelhashes: Vec<_> = labels.iter().map(|label| labelhash(label)).collect();
    sqlx::query("INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state, chain_id, block_hash, block_number, canonicality_state) VALUES ($1, 'ens', $2, $3, '\\x00', $4, $5, 'ensip15', 'active', $6, $7, 10, 'canonical')")
        .bind(&logical).bind(raw_name).bind(labels).bind(&hash).bind(labelhashes).bind(CHAIN).bind(HASH).execute(pool).await?;
    for (offset, arm) in arms.iter().enumerate() {
        let resource = uuid(1 + offset as u8, index);
        let binding = uuid(3 + offset as u8, index);
        sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
            .bind(&resource).bind(CHAIN).bind(HASH).execute(pool).await?;
        sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', $4, '2026-08-25T00:00:00Z', $5, $6, 10, '{\"transaction_index\":0,\"log_index\":0}', 'canonical')")
            .bind(binding).bind(&logical).bind(resource).bind(arm).bind(CHAIN).bind(HASH).execute(pool).await?;
    }
    Ok(logical)
}

struct Event<'a> {
    family: &'a str,
    kind: &'a str,
    log: i64,
    after: Value,
}

#[derive(sqlx::FromRow)]
struct CapturedAuthority {
    selected_authority_arm: Option<String>,
    authority_epoch_start_position: Option<Value>,
    authority_proof_kind: Option<String>,
    authority_proof_event_id: Option<i64>,
    authority_proof_event_identity: Option<String>,
    authority_transition_id: Option<String>,
}

async fn event(
    pool: &PgPool,
    identity: &str,
    logical: &str,
    resource: Option<&str>,
    event: Event<'_>,
) -> Result<i64> {
    Ok(sqlx::query_scalar("INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id, event_kind, source_family, manifest_version, chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state, after_state, migration_correlation_ids) VALUES ($1, 'ens', $2, $3::uuid, $4, $5, 1, $6, 10, $7, '0x503', 0, $8, CASE WHEN $4 = 'MigrationApplied' THEN 'ens_v2_migration' ELSE 'ens_v2_registry_resource_surface' END, 'canonical', $9, CASE WHEN $4 = 'MigrationApplied' THEN ARRAY['issue-503'] ELSE ARRAY[]::text[] END) RETURNING normalized_event_id")
        .bind(identity).bind(logical).bind(resource).bind(event.kind).bind(event.family).bind(CHAIN).bind(HASH).bind(event.log).bind(event.after).fetch_one(pool).await?)
}

async fn run(pool: &PgPool) -> bigname_project::Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: 10,
            affected_from_block: 10,
            affected_to_block: 10,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await
        .map(|_| ())
}

async fn authority(
    pool: &PgPool,
    logical: &str,
) -> Result<(
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
)> {
    Ok(sqlx::query_as("SELECT provenance #>> '{authority_selection,authority_arm}', provenance #>> '{authority_selection,unsupported_reason}', provenance #>> '{authority_selection,proof_kind}', provenance #>> '{authority_selection,transition_id}' FROM name_current WHERE logical_name_id = $1")
        .bind(logical).fetch_one(pool).await?)
}

async fn optional_authority(
    pool: &PgPool,
    logical: &str,
) -> Result<
    Option<(
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    )>,
> {
    Ok(sqlx::query_as("SELECT provenance #>> '{authority_selection,authority_arm}', provenance #>> '{authority_selection,unsupported_reason}', provenance #>> '{authority_selection,proof_kind}', provenance #>> '{authority_selection,transition_id}' FROM name_current WHERE logical_name_id = $1")
        .bind(logical).fetch_optional(pool).await?)
}

async fn authority_evidence(
    pool: &PgPool,
    logical: &str,
) -> Result<(
    Option<String>,
    Option<String>,
    Option<String>,
    Option<serde_json::Value>,
)> {
    Ok(sqlx::query_as("SELECT provenance #>> '{authority_selection,proof_kind}', provenance #>> '{authority_selection,proof_event_id}', provenance #>> '{authority_selection,proof_event_identity}', provenance #> '{authority_selection,epoch_start_position}' FROM name_current WHERE logical_name_id = $1")
        .bind(logical).fetch_one(pool).await?)
}

async fn capture_staged_authority(pool: &PgPool) -> Result<()> {
    sqlx::query("CREATE TABLE issue503_authority_capture (logical_name_id text, selected_authority_arm text, authority_epoch_start_position jsonb, authority_proof_kind text, authority_proof_event_id bigint, authority_proof_event_identity text, authority_transition_id text)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE FUNCTION capture_issue503_authority() RETURNS trigger LANGUAGE plpgsql AS $capture$ BEGIN INSERT INTO issue503_authority_capture SELECT logical_name_id, selected_authority_arm, authority_epoch_start_position, authority_proof_kind, authority_proof_event_id, authority_proof_event_identity, authority_transition_id FROM project_name_authority; RETURN NULL; END $capture$")
        .execute(pool)
        .await?;
    sqlx::query("CREATE TRIGGER capture_issue503_authority AFTER INSERT ON name_current FOR EACH STATEMENT EXECUTE FUNCTION capture_issue503_authority()")
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn sepolia_no_proof_overlap_remains_refused_not_fatal() -> Result<()> {
    let (db, pool) = database("issue503_no_proof").await?;
    let logical = surface(&pool, 1, "ordinary.eth", &["ens_v1", "ens_v2"]).await?;
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?,
        (
            None,
            Some("independent_ens_deployments_overlap".into()),
            None,
            None
        )
    );
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn shared_ens_infrastructure_selects_v2_without_fabricating_proof() -> Result<()> {
    let (db, pool) = database("issue503_shared").await?;
    let mut logicals = Vec::new();
    for (index, name) in ["", "eth", "reverse", "addr.reverse"]
        .into_iter()
        .enumerate()
    {
        logicals.push(surface(&pool, index as u16 + 10, name, &["ens_v1", "ens_v2"]).await?);
    }
    capture_staged_authority(&pool).await?;
    run(&pool).await?;
    let root_authority: CapturedAuthority = sqlx::query_as("SELECT selected_authority_arm, authority_epoch_start_position, authority_proof_kind, authority_proof_event_id, authority_proof_event_identity, authority_transition_id FROM issue503_authority_capture WHERE logical_name_id = $1")
        .bind(&logicals[0])
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        root_authority.selected_authority_arm.as_deref(),
        Some("ens_v2")
    );
    assert_eq!(root_authority.authority_epoch_start_position, None);
    assert_eq!(root_authority.authority_proof_kind, None);
    assert_eq!(root_authority.authority_proof_event_id, None);
    assert_eq!(root_authority.authority_proof_event_identity, None);
    assert_eq!(root_authority.authority_transition_id, None);
    // The exact root participates in authority selection, while the current
    // name projection intentionally omits its empty surface.
    assert_eq!(optional_authority(&pool, &logicals[0]).await?, None);
    for logical in &logicals[1..] {
        assert_eq!(
            authority(&pool, logical).await?,
            (Some("ens_v2".into()), None, None, None)
        );
        assert_eq!(
            authority_evidence(&pool, logical).await?,
            (None, None, None, None)
        );
    }
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn shared_infrastructure_refuses_historical_only_v2_evidence() -> Result<()> {
    let (db, pool) = database("issue503_shared_historical_v2").await?;
    let logical = surface(&pool, 15, "eth", &["ens_v1"]).await?;
    let v2_resource = uuid(2, 15);
    let v2_binding = uuid(4, 15);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(&v2_resource).bind(CHAIN).bind(HASH).execute(&pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, active_to, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v2', '2026-08-25T00:00:00Z', '2026-08-25T12:00:00Z', $4, $5, 10, '{\"transaction_index\":0,\"log_index\":0}', 'canonical')")
        .bind(v2_binding).bind(&logical).bind(&v2_resource).bind(CHAIN).bind(HASH).execute(&pool).await?;
    event(
        &pool,
        "issue503-shared-v2-release",
        &logical,
        Some(&v2_resource),
        Event {
            family: "ens_v2_registrar_l1",
            kind: "RegistrationReleased",
            log: 2,
            after: json!({"status":"unregistered"}),
        },
    )
    .await?;

    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?,
        (
            None,
            Some("independent_ens_deployments_overlap".into()),
            None,
            None
        )
    );
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn shared_infrastructure_current_v2_accepts_historical_or_absent_v1_evidence() -> Result<()> {
    let (db, pool) = database("issue503_shared_current_v2").await?;
    let historical_v1 = surface(&pool, 16, "eth", &["ens_v2"]).await?;
    let v2_only = surface(&pool, 17, "reverse", &["ens_v2"]).await?;
    event(
        &pool,
        "issue503-shared-v1-history",
        &historical_v1,
        None,
        Event {
            family: "ens_v1_registry_l1",
            kind: "AuthorityTransferred",
            log: 1,
            after: json!({"owner":"0x0000000000000000000000000000000000000001"}),
        },
    )
    .await?;

    run(&pool).await?;
    assert_eq!(
        authority(&pool, &historical_v1).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    assert_eq!(
        authority_evidence(&pool, &historical_v1).await?,
        (None, None, None, None)
    );
    assert_eq!(
        authority(&pool, &v2_only).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    assert_eq!(
        authority_evidence(&pool, &v2_only).await?,
        (
            None,
            None,
            None,
            Some(json!({
                "block_number": 10,
                "transaction_index": 0,
                "log_index": 0
            }))
        )
    );
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reverse_descendants_are_not_shared_infrastructure() -> Result<()> {
    let (db, pool) = database("issue503_reverse_descendants").await?;
    let a = surface(&pool, 20, "alice.addr.reverse", &["ens_v1", "ens_v2"]).await?;
    let b = surface(&pool, 21, "default.reverse", &["ens_v1", "ens_v2"]).await?;
    run(&pool).await?;
    for logical in [a, b] {
        assert_eq!(
            authority(&pool, &logical).await?.1.as_deref(),
            Some("independent_ens_deployments_overlap")
        );
    }
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn proven_sepolia_dual_current_exact_name_is_fatal() -> Result<()> {
    let (db, pool) = database("issue503_exact_fatal").await?;
    let logical = surface(&pool, 30, "proven.eth", &["ens_v1", "ens_v2"]).await?;
    let successor_binding = uuid(4, 30);
    let successor_resource = uuid(2, 30);
    event(
        &pool,
        "issue503-exact-proof",
        &logical,
        None,
        Event {
            family: "ens_v2_migration_l1",
            kind: "MigrationApplied",
            log: 1,
            after: json!({"migration_path":"unlocked_wrapped","successor_binding":{"binding_id":successor_binding,"resource_id":successor_resource}}),
        },
    )
    .await?;
    let error = run(&pool)
        .await
        .expect_err("proven Sepolia conflict must fail");
    let evidence = error
        .generation_failure_evidence()
        .context("failure evidence")?;
    assert_eq!(evidence.failure_kind, DUAL_CURRENT_EXACT_NAME_AUTHORITY);
    assert_eq!(evidence.logical_name_id, logical);
    assert_eq!(
        evidence.payload["boundary"]["event_identity"],
        "issue503-exact-proof"
    );
    assert_eq!(evidence.payload["target"]["block_number"], 10);
    assert_eq!(evidence.failure_fingerprint.len(), 64);
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn proven_sepolia_dual_current_child_is_fatal() -> Result<()> {
    let (db, pool) = database("issue503_child_fatal").await?;
    let parent = surface(&pool, 40, "parent.eth", &["ens_v2"]).await?;
    let child = surface(&pool, 41, "child.parent.eth", &["ens_v2"]).await?;
    let registry = uuid(8, 40);
    sqlx::query("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ($1::uuid, $2, 'contract')")
        .bind(&registry).bind(CHAIN).execute(&pool).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number) VALUES ($1::uuid, $2, '0x0000000000000000000000000000000000000503', 10)")
        .bind(&registry).bind(CHAIN).execute(&pool).await?;
    event(
        &pool,
        "issue503-parent-registry",
        &parent,
        None,
        Event {
            family: "ens_v2_registry_l1",
            kind: "SubregistryChanged",
            log: 1,
            after: json!({"subregistry":"0x0000000000000000000000000000000000000503"}),
        },
    )
    .await?;
    event(
        &pool,
        "issue503-v2-child",
        &child,
        Some(&uuid(1, 41)),
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationGranted",
            log: 2,
            after: json!({"registry_contract_instance_id":registry,"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        },
    )
    .await?;
    event(
        &pool,
        "issue503-child-proof",
        &child,
        None,
        Event {
            family: "ens_v2_migration_l1",
            kind: "MigrationApplied",
            log: 3,
            after: json!({"migration_path":"locked_wrapped","successor_binding":{"binding_id":uuid(3, 41),"resource_id":uuid(1, 41)}}),
        },
    )
    .await?;
    event(
        &pool,
        "issue503-v1-child",
        &child,
        None,
        Event {
            family: "ens_v1_registry_l1",
            kind: "SubregistryChanged",
            log: 4,
            after: json!({"node":parent.trim_start_matches("ens:"),"child_node":child.trim_start_matches("ens:"),"labelhash":labelhash("child"),"owner":"0x0000000000000000000000000000000000000002"}),
        },
    )
    .await?;
    let error = run(&pool)
        .await
        .expect_err("proven Sepolia child conflict must fail");
    let evidence = error
        .generation_failure_evidence()
        .context("failure evidence")?;
    assert_eq!(evidence.failure_kind, DUAL_CURRENT_CHILD_AUTHORITY);
    assert_eq!(evidence.payload["parent_logical_name_id"], parent);
    assert_eq!(evidence.logical_name_id, child);
    assert_eq!(
        evidence.payload["authority_proof_event_identity"],
        "issue503-child-proof"
    );
    assert_eq!(
        evidence.payload["predecessor"]["event_identity"],
        "issue503-v1-child"
    );
    assert_eq!(evidence.target_block_number, 10);
    assert_eq!(evidence.failure_fingerprint.len(), 64);
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn shared_infrastructure_without_proof_is_not_integrity_fatal() -> Result<()> {
    let (db, pool) = database("issue503_shared_nonfatal").await?;
    let logical = surface(&pool, 50, "eth", &["ens_v1", "ens_v2"]).await?;
    run(&pool).await?;
    let selected = authority(&pool, &logical).await?;
    assert_eq!(selected, (Some("ens_v2".into()), None, None, None));
    db.cleanup().await?;
    Ok(())
}
