#[allow(dead_code)]
mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::{Context, Result};
use bigname_ingest::load_watch_filter;
use bigname_interpret::{
    BatchRequest, Engine, ErrorKind as InterpretErrorKind, Marker, RunMode as InterpretRunMode,
};
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use bigname_project::{
    BatchRequest as ProjectBatchRequest, Engine as ProjectEngine, RunMode as ProjectRunMode,
};
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    error::{ErrorKind, RunnerError},
    heads::{BlockMarker, HeadMarkers, publish_heads},
    interpret_phase::InterpretPhase,
    phase::{BlockRange, LoopbackPhase, Phase, PhaseContext, PhaseFuture, PhaseName, PhaseSet},
    phase_lock::PhaseLock,
    project_phase::ProjectPhase,
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool,
    postgres::PgPoolOptions,
    types::{Uuid, time},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use support::ScratchDatabase;

const CONTRACT: &str = "0x0000000000000000000000000000000000000042";
const RIVAL_REGISTRY: &str = "0x0000000000000000000000000000000000000069";
const REGISTRY: &str = "0x0000000000000000000000000000000000000046";
const DISCOVERED_RESOLVER: &str = "0x0000000000000000000000000000000000000044";
const ANNOUNCED_REGISTRY: &str = "0x0000000000000000000000000000000000000045";
const SENDER: &str = "0x0000000000000000000000000000000000000043";
const PRIOR_REGISTRY_OWNER: &str = "0x0000000000000000000000000000000000000048";
const REGISTRANT: &str = "0x0000000000000000000000000000000000000049";
const REMINTED_REGISTRY_OWNER: &str = "0x000000000000000000000000000000000000004a";
const WRAPPED_REGISTRY_OWNER: &str = "0x000000000000000000000000000000000000004b";
const NORMALIZER: &str = "ensip15@ens-normalize-0.1.1";
type DiscoveryEpochRow = (i64, Option<i64>, bool, Option<String>, Option<String>);

sol! {
    event NameRegistered(
        string name,
        bytes32 indexed label,
        address indexed owner,
        uint256 expires
    );
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event Transfer(bytes32 indexed node, address owner);
    event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);
    event ResolverUpdated(
        uint256 indexed tokenId,
        address indexed resolver,
        address indexed sender
    );
    event TextChanged(
        bytes32 indexed node,
        string indexed indexedKey,
        string key,
        string value
    );
    event Upgraded(address indexed implementation);
    event AddrChanged(bytes32 indexed node, address a);
    event NameChanged(bytes32 indexed node, string name);
    event ReverseClaimed(address indexed addr, bytes32 indexed node);
    event RegistryCreated();
    event NameWrapped(
        bytes32 indexed node,
        bytes name,
        address owner,
        uint32 fuses,
        uint64 expiry
    );
}

mod raw_string_events {
    use alloy_sol_types::sol;

    sol! {
        event RawTextChanged(bytes32 indexed node, bytes32 indexed indexedKey, bytes key, bytes value);
        event RawNameChanged(bytes32 indexed node, bytes name);
    }
}

mod legacy_registrar_controller_events {
    use alloy_sol_types::sol;

    sol! {
        event NameRegistered(
            string name,
            bytes32 indexed label,
            address indexed owner,
            uint256 cost,
            uint256 expires
        );
    }
}

mod base_registrar_events {
    use alloy_sol_types::sol;

    sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    }
}

mod v2_registry_events {
    use alloy_sol_types::sol;

    sol! {
        event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
        event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
        event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
        // (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L56-L60 @ ens_v2@a971bd64)
        event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
        // (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L88 @ ens_v2@a971bd64)
        event ParentUpdated(address indexed parent, string label, address indexed sender);
        event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
        event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    }
}

#[tokio::test]
async fn production_interpret_writes_plain_events_identity_preimages_and_flags() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret").await?;
    seed_fixture(scratch.pool(), "interpret-basic", &[(1, "alice")]).await?;

    run_engine(
        scratch.pool(),
        "interpret-basic",
        0,
        1,
        InterpretRunMode::Normal,
    )
    .await?;

    let event_kinds: Vec<String> =
        sqlx::query_scalar("SELECT event_kind FROM normalized_events ORDER BY normalized_event_id")
            .fetch_all(scratch.pool())
            .await?;
    assert_eq!(
        event_kinds,
        [
            "RegistrationGranted",
            "ExpiryChanged",
            "PermissionChanged",
            "SurfaceBound",
            "AuthorityEpochChanged",
            "PreimageObserved"
        ]
    );
    let surface: (String, String, Vec<String>, String) = sqlx::query_as(
        "
        SELECT raw_name, visibility_state, raw_labels, logical_name_id
        FROM name_surfaces
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(surface.0, "alice.eth");
    assert_eq!(surface.1, "active");
    assert_eq!(surface.2, ["alice", "eth"]);
    assert!(surface.3.starts_with("ens:0x"));
    assert!(!surface.3.contains("alice"));
    let preimages: Vec<(String, bool, Option<String>)> = sqlx::query_as(
        "
        SELECT decoded_label, normalized_under_version, normalization_error
        FROM label_preimages
        ORDER BY decoded_label
        ",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        preimages,
        vec![("alice".into(), true, None), ("eth".into(), true, None)]
    );
    let binding_count: i64 = sqlx::query_scalar("SELECT count(*) FROM surface_bindings")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(binding_count, 1);
    scratch.cleanup().await
}

#[tokio::test]
async fn same_transaction_registry_permission_writes_with_registration_resource() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_same_transaction_resource").await?;
    let chain = "interpret-same-transaction-resource";
    seed_same_transaction_registration_fixture(scratch.pool(), chain).await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let (registration_resource, registration_authority_key): (Uuid, String) = sqlx::query_as(
        "SELECT resource_id, after_state ->> 'authority_key' FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RegistrationGranted'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await
    .context("registration resource was not written")?;
    let registry_permissions: Vec<(Uuid, serde_json::Value, String)> = sqlx::query_as(
        "SELECT resource_id, after_state, raw_fact_ref ->> 'interpreter_state_key'
         FROM normalized_events
         WHERE chain_id = $1
           AND source_family = 'ens_v1_registry_l1'
           AND event_kind = 'PermissionChanged'",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert!(!registry_permissions.is_empty());
    for (resource_id, after_state, interpreter_state_key) in registry_permissions {
        assert_eq!(resource_id, registration_resource);
        assert!(interpreter_state_key.contains(&registration_resource.to_string()));
        let mut saw_authority_source = false;
        for field in ["grant_source", "revocation_source"] {
            let Some(source) = after_state
                .get(field)
                .and_then(serde_json::Value::as_object)
                .filter(|source| {
                    source.get("kind").and_then(serde_json::Value::as_str)
                        == Some("ens_v1_authority")
                })
            else {
                continue;
            };
            saw_authority_source = true;
            assert_eq!(
                source
                    .get("authority_kind")
                    .and_then(serde_json::Value::as_str),
                Some("registrar")
            );
            assert_eq!(
                source
                    .get("authority_key")
                    .and_then(serde_json::Value::as_str),
                Some(registration_authority_key.as_str())
            );
        }
        assert!(saw_authority_source);
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn lease_release_materializes_dormant_registry_resource_before_binding() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_lease_release_resource").await?;
    let chain = "interpret-lease-release-resource";
    seed_same_transaction_registration_fixture(scratch.pool(), chain).await?;
    let release_timestamp = 1_000_001 + 90 * 24 * 60 * 60 + 1;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp,
             canonicality_state
         )
         VALUES ($1, $2, $3, 2, to_timestamp($4), 'canonical')",
    )
    .bind(chain)
    .bind(block_hash(chain, 2))
    .bind(block_hash(chain, 1))
    .bind(release_timestamp)
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Normal).await?;

    let (resource_id, source_event): (Uuid, String) = sqlx::query_as(
        "SELECT resource_id, after_state ->> 'source_event'
         FROM normalized_events
         WHERE chain_id = $1
           AND block_number = 2
           AND event_kind = 'SurfaceBound'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await
    .context("lease release did not emit its registry fallback binding")?;
    assert_eq!(source_event, "RegistrationReleased");
    let resource_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM resources
             WHERE chain_id = $1 AND resource_id = $2
         )",
    )
    .bind(chain)
    .bind(resource_id)
    .fetch_one(scratch.pool())
    .await?;
    assert!(resource_exists);
    scratch.cleanup().await
}

#[tokio::test]
async fn legacy_register_with_config_midflow_record_writes_with_registration_identity() -> Result<()>
{
    let scratch =
        ScratchDatabase::create("production_interpret_legacy_register_with_config").await?;
    let chain = "interpret-legacy-register-with-config";
    let node = seed_legacy_register_with_config_fixture(scratch.pool(), chain).await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let registration: (String, Uuid) = sqlx::query_as(
        "SELECT logical_name_id, resource_id
         FROM normalized_events
         WHERE chain_id = $1
           AND source_family = 'ens_v1_registrar_l1'
           AND event_kind = 'RegistrationGranted'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await
    .context("registration identity was not written")?;
    let record: (Option<String>, Option<Uuid>) = sqlx::query_as(
        "SELECT logical_name_id, resource_id
         FROM normalized_events
         WHERE chain_id = $1
           AND source_family = 'ens_v1_resolver_l1'
           AND event_kind = 'RecordChanged'
           AND log_index = 1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await
    .context("mid-flow address record was not written")?;

    assert_eq!(registration.0, format!("ens:{node:#x}"));
    assert_eq!(record.0.as_deref(), Some(registration.0.as_str()));
    assert_eq!(record.1, Some(registration.1));
    scratch.cleanup().await
}

#[tokio::test]
async fn reverse_node_resolver_event_writes_without_unmaterialized_surface_reference() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_interpret_reverse_node_resolver").await?;
    let chain = "interpret-reverse-node-resolver";
    let reverse_node = seed_reverse_node_resolver_burst_fixture(scratch.pool(), chain).await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let reverse_records: Vec<(String, Option<String>, Option<Uuid>, serde_json::Value)> =
        sqlx::query_as(
            "SELECT event_kind, logical_name_id, resource_id, after_state
             FROM normalized_events
             WHERE chain_id = $1
               AND source_family = 'ens_v1_resolver_l1'
               AND after_state ->> 'node' = $2
             ORDER BY log_index, normalized_event_id",
        )
        .bind(chain)
        .bind(format!("{reverse_node:#x}"))
        .fetch_all(scratch.pool())
        .await?;
    assert_eq!(reverse_records.len(), 1);
    assert_eq!(reverse_records[0].0, "RecordChanged");
    assert_eq!(reverse_records[0].3["source_event"], "NameChanged");
    assert_eq!(reverse_records[0].3["raw_name"], "alice.eth");
    assert_eq!(reverse_records[0].1, None);
    assert_eq!(reverse_records[0].2, None);

    let reverse_surface_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_surfaces WHERE logical_name_id = $1")
            .bind(format!("ens:{reverse_node:#x}"))
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(reverse_surface_count, 0);
    let registration_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id
         FROM normalized_events
         WHERE chain_id = $1
           AND source_family = 'ens_v1_registrar_l1'
           AND event_kind = 'RegistrationGranted'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let forward_logical_name_id = format!("ens:{:#x}", raw_namehash(&[b"alice", b"eth"]));
    let forward_records: Vec<(String, Option<Uuid>)> = sqlx::query_as(
        "SELECT logical_name_id, resource_id
         FROM normalized_events
         WHERE chain_id = $1
           AND source_family = 'ens_v1_resolver_l1'
           AND event_kind = 'RecordChanged'
           AND after_state ->> 'node' = $2
         ORDER BY log_index",
    )
    .bind(chain)
    .bind(format!("{:#x}", raw_namehash(&[b"alice", b"eth"])))
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(forward_records.len(), 2);
    assert!(
        forward_records
            .iter()
            .all(|(logical_name_id, resource_id)| {
                logical_name_id == &forward_logical_name_id
                    && *resource_id == Some(registration_resource)
            })
    );
    let reverse_claims: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1
           AND source_family = 'ens_v1_reverse_l1'
           AND event_kind = 'ReverseChanged'
           AND logical_name_id IS NULL
           AND resource_id IS NULL",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(reverse_claims, 1);
    scratch.cleanup().await
}

#[derive(Clone, Copy)]
enum PriorOwnerRegistrationFlow {
    LegacyTwoOwnerChanges,
    ModernSingleOwnerChange,
}

#[tokio::test]
async fn legacy_prior_owner_revocation_writes_and_projects_on_its_registry_epoch() -> Result<()> {
    assert_prior_owner_revocation_writes_and_projects(
        "production_interpret_legacy_prior_owner",
        "interpret-legacy-prior-owner",
        PriorOwnerRegistrationFlow::LegacyTwoOwnerChanges,
        REGISTRANT,
    )
    .await
}

#[tokio::test]
async fn modern_prior_owner_revocation_writes_and_projects_on_its_registry_epoch() -> Result<()> {
    assert_prior_owner_revocation_writes_and_projects(
        "production_interpret_modern_prior_owner",
        "interpret-modern-prior-owner",
        PriorOwnerRegistrationFlow::ModernSingleOwnerChange,
        REGISTRANT,
    )
    .await
}

#[tokio::test]
async fn legacy_wrapped_owner_grant_writes_and_projects_on_the_registration_epoch() -> Result<()> {
    assert_prior_owner_revocation_writes_and_projects(
        "production_interpret_legacy_wrapped_owner",
        "interpret-legacy-wrapped-owner",
        PriorOwnerRegistrationFlow::LegacyTwoOwnerChanges,
        WRAPPED_REGISTRY_OWNER,
    )
    .await
}

#[tokio::test]
async fn modern_wrapped_owner_grant_writes_and_projects_on_the_registration_epoch() -> Result<()> {
    assert_prior_owner_revocation_writes_and_projects(
        "production_interpret_modern_wrapped_owner",
        "interpret-modern-wrapped-owner",
        PriorOwnerRegistrationFlow::ModernSingleOwnerChange,
        WRAPPED_REGISTRY_OWNER,
    )
    .await
}

async fn assert_prior_owner_revocation_writes_and_projects(
    database_name: &str,
    chain: &str,
    flow: PriorOwnerRegistrationFlow,
    final_registry_owner: &str,
) -> Result<()> {
    let scratch = ScratchDatabase::create(database_name).await?;
    seed_prior_owner_registration_fixture(scratch.pool(), chain, flow, final_registry_owner)
        .await?;

    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    run_project(scratch.pool(), chain, 2, 0, 2).await?;

    let registrar_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RegistrationGranted'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let registry_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events
         WHERE chain_id = $1
           AND source_family = 'ens_v1_registry_l1'
           AND event_kind = 'PermissionChanged'
           AND after_state ->> 'subject' = $2
           AND block_number = 1",
    )
    .bind(chain)
    .bind(PRIOR_REGISTRY_OWNER)
    .fetch_one(scratch.pool())
    .await
    .context("prior owner's registry permission was not written")?;
    assert_ne!(registry_resource, registrar_resource);
    let retained_resource: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM resources
             WHERE chain_id = $1 AND resource_id = $2
         )",
    )
    .bind(chain)
    .bind(registry_resource)
    .fetch_one(scratch.pool())
    .await
    .context("retained registry resource row was not readable")?;
    assert!(retained_resource);

    assert_prior_owner_is_revoked(scratch.pool(), chain, registry_resource).await?;
    assert_registrar_epoch_permission(scratch.pool(), registrar_resource).await?;
    if final_registry_owner == WRAPPED_REGISTRY_OWNER {
        assert_wrapped_owner_is_on_registration_epoch(
            scratch.pool(),
            chain,
            registrar_resource,
            registry_resource,
        )
        .await?;
    }

    run_engine(scratch.pool(), chain, 3, 3, InterpretRunMode::Normal).await?;
    run_project(scratch.pool(), chain, 3, 3, 3).await?;
    assert_prior_owner_is_revoked(scratch.pool(), chain, registry_resource).await?;
    if final_registry_owner == WRAPPED_REGISTRY_OWNER {
        assert_wrapped_owner_is_on_registration_epoch(
            scratch.pool(),
            chain,
            registrar_resource,
            registry_resource,
        )
        .await?;
    } else {
        let reminted_owner_powers: serde_json::Value = sqlx::query_scalar(
            "SELECT effective_powers FROM permissions_current
             WHERE resource_id = $1 AND subject = lower($2) AND scope = 'resource'",
        )
        .bind(registry_resource)
        .bind(REMINTED_REGISTRY_OWNER)
        .fetch_one(scratch.pool())
        .await
        .context("reminted registry owner permission was not projected")?;
        assert_eq!(reminted_owner_powers, json!(["resource_control"]));
    }

    scratch.cleanup().await
}

async fn assert_wrapped_owner_is_on_registration_epoch(
    pool: &PgPool,
    chain_id: &str,
    registrar_resource: Uuid,
    registry_resource: Uuid,
) -> Result<()> {
    let permission: (serde_json::Value, serde_json::Value) = sqlx::query_as(
        "SELECT effective_powers, grant_source
         FROM permissions_current
         WHERE resource_id = $1 AND subject = lower($2) AND scope = 'resource'",
    )
    .bind(registrar_resource)
    .bind(WRAPPED_REGISTRY_OWNER)
    .fetch_one(pool)
    .await
    .context("wrapped registry owner permission was not projected on the registrar resource")?;
    assert_eq!(permission.0, json!(["resource_control"]));
    assert_eq!(permission.1["authority_kind"], "registrar");
    let stale_active_row: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM permissions_current
             WHERE resource_id = $1 AND subject = lower($2) AND scope = 'resource'
         )",
    )
    .bind(registry_resource)
    .bind(WRAPPED_REGISTRY_OWNER)
    .fetch_one(pool)
    .await?;
    assert!(!stale_active_row);
    let wrapper_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events
         WHERE chain_id = $1
           AND source_family = 'ens_v1_wrapper_l1'
           AND event_kind = 'SurfaceBound'",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .context("wrapped registration did not write a wrapper surface event")?;
    assert_ne!(wrapper_resource, registrar_resource);
    let active_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM surface_bindings
         WHERE chain_id = $1 AND active_to IS NULL AND canonicality_state = 'canonical'",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .context("wrapped registration has no active surface binding")?;
    assert_eq!(active_resource, wrapper_resource);
    Ok(())
}

async fn assert_registrar_epoch_permission(pool: &PgPool, registrar_resource: Uuid) -> Result<()> {
    let permission: (serde_json::Value, serde_json::Value) = sqlx::query_as(
        "SELECT effective_powers, grant_source
         FROM permissions_current
         WHERE resource_id = $1 AND subject = lower($2) AND scope = 'resource'",
    )
    .bind(registrar_resource)
    .bind(REGISTRANT)
    .fetch_one(pool)
    .await
    .context("registrar permission was not projected")?;
    assert_eq!(permission.0, json!(["resource_control"]));
    assert_eq!(permission.1["authority_kind"], "registrar");
    Ok(())
}

async fn assert_prior_owner_is_revoked(
    pool: &PgPool,
    chain_id: &str,
    registry_resource: Uuid,
) -> Result<()> {
    let latest_history: (serde_json::Value, serde_json::Value) = sqlx::query_as(
        "SELECT after_state -> 'effective_powers', after_state -> 'revocation_source'
         FROM normalized_events
         WHERE chain_id = $1
           AND resource_id = $2
           AND event_kind = 'PermissionChanged'
           AND after_state ->> 'subject' = $3
           AND after_state -> 'scope' ->> 'kind' = 'resource'
         ORDER BY block_number DESC, transaction_index DESC, log_index DESC,
                  normalized_event_id DESC
         LIMIT 1",
    )
    .bind(chain_id)
    .bind(registry_resource)
    .bind(PRIOR_REGISTRY_OWNER)
    .fetch_one(pool)
    .await
    .context("prior owner permission history was not written")?;
    assert_eq!(latest_history.0, json!([]));
    assert_eq!(latest_history.1["authority_kind"], "registry_only");
    let stale_active_row: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM permissions_current
             WHERE resource_id = $1 AND subject = lower($2) AND scope = 'resource'
         )",
    )
    .bind(registry_resource)
    .bind(PRIOR_REGISTRY_OWNER)
    .fetch_one(pool)
    .await?;
    assert!(!stale_active_row);
    Ok(())
}

#[tokio::test]
async fn divergent_normalized_event_identity_errors_loudly() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_event_conflict").await?;
    let chain = "interpret-event-conflict";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    sqlx::query(
        "UPDATE normalized_events SET after_state = '{\"tampered\":true}'::jsonb WHERE chain_id = $1 AND event_kind = 'RegistrationGranted'",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    let error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("a stable event identity must not overwrite divergent data");
    assert_eq!(error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(error.to_string().contains("different event data"));
    scratch.cleanup().await
}

#[tokio::test]
async fn normalized_event_replay_accepts_only_canonicality_lifecycle_changes() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_event_canonicality").await?;
    let chain = "interpret-event-canonicality";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'safe' WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    let advanced: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT canonicality_state::text FROM normalized_events WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(advanced, ["safe"]);

    let recanonicalized_chain = "interpret-event-recanonicalized";
    seed_fixture(scratch.pool(), recanonicalized_chain, &[(1, "bob")]).await?;
    run_engine(
        scratch.pool(),
        recanonicalized_chain,
        0,
        1,
        InterpretRunMode::Normal,
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE chain_id = $1")
        .bind(recanonicalized_chain)
        .execute(scratch.pool())
        .await?;
    run_engine(
        scratch.pool(),
        recanonicalized_chain,
        0,
        1,
        InterpretRunMode::Normal,
    )
    .await?;
    let recanonicalized: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT canonicality_state::text FROM normalized_events WHERE chain_id = $1",
    )
    .bind(recanonicalized_chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(recanonicalized, ["canonical"]);
    scratch.cleanup().await
}

#[tokio::test]
async fn divergent_token_lineage_chain_errors_loudly() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_lineage_conflict").await?;
    let chain = "interpret-lineage-conflict";
    let other_chain = "interpret-lineage-conflict-other";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    sqlx::query("DELETE FROM normalized_events WHERE chain_id = $1")
        .bind(chain)
        .execute(scratch.pool())
        .await?;
    sqlx::query("DELETE FROM surface_bindings WHERE chain_id = $1")
        .bind(chain)
        .execute(scratch.pool())
        .await?;
    sqlx::query("DELETE FROM resources WHERE chain_id = $1")
        .bind(chain)
        .execute(scratch.pool())
        .await?;
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        )
        VALUES ($1, $2, 1, to_timestamp(1), 'canonical')
        ",
    )
    .bind(other_chain)
    .bind(block_hash(other_chain, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query("UPDATE token_lineages SET chain_id = $2, block_hash = $3 WHERE chain_id = $1")
        .bind(chain)
        .bind(other_chain)
        .bind(block_hash(other_chain, 1))
        .execute(scratch.pool())
        .await?;

    let error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 2,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("a token lineage ID must not move across chains");
    assert_eq!(error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(error.to_string().contains("different chain"));
    scratch.cleanup().await
}

#[tokio::test]
async fn divergent_token_lineage_provenance_errors_loudly() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_lineage_data_conflict").await?;
    let chain = "interpret-lineage-data-conflict";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        UPDATE token_lineages
        SET provenance = jsonb_set(provenance, '{log_index}', '-1'::jsonb)
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    let error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 2,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("a token lineage ID must not overwrite divergent same-chain data");
    assert_eq!(error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(error.to_string().contains("different lineage data"));
    scratch.cleanup().await
}

#[tokio::test]
async fn nonzero_full_replay_rejects_a_forged_prior_token_lineage_anchor() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_lineage_coverage_conflict").await?;
    let chain = "interpret-lineage-coverage-conflict";
    seed_fixture(scratch.pool(), chain, &[(100, "alice")]).await?;
    run_engine(scratch.pool(), chain, 100, 100, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        UPDATE token_lineages
        SET block_hash = $2,
            block_number = 99,
            provenance = $3
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 99))
    .bind(json!({
        "source": "raw_log",
        "chain_id": chain,
        "block_hash": block_hash(chain, 99),
        "block_number": 99,
        "transaction_index": 0,
        "log_index": 0,
    }))
    .execute(scratch.pool())
    .await?;

    let error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 100,
            to_block: 100,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("full replay must reject a forged pre-coverage lineage anchor");
    assert_eq!(error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(error.to_string().contains("different lineage data"));
    scratch.cleanup().await
}

#[tokio::test]
async fn later_batch_token_lineage_observation_preserves_the_first_anchor() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_lineage_later_batch").await?;
    let chain = "interpret-lineage-later-batch";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    let before: (i64, serde_json::Value) =
        sqlx::query_as("SELECT block_number, provenance FROM token_lineages WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;

    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Normal).await?;
    let after: (i64, serde_json::Value) =
        sqlx::query_as("SELECT block_number, provenance FROM token_lineages WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(after, before);
    scratch.cleanup().await
}

#[tokio::test]
async fn cross_batch_prior_state_uses_hash_covered_compaction_keys() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_prior_state_keys").await?;
    let chain = "interpret-prior-state-keys";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "alice")]).await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Normal).await?;

    let counts: (i64, i64) = sqlx::query_as(
        "
        SELECT count(*),
               count(*) FILTER (
                   WHERE raw_fact_ref ? 'interpreter_state_key'
               )
        FROM normalized_events
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(counts.0 > 0);
    assert_eq!(
        counts.1, counts.0,
        "every adapter event must carry its hash-covered prior-state compaction key"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn prior_state_is_loaded_once_and_folded_forward_across_500_block_batches() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_prior_state_session").await?;
    let chain = "interpret-prior-state-session";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (501, "alice")]).await?;
    let engine = Engine::new(scratch.pool().clone());
    let first = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(!first.complete);
    assert_eq!(first.current.number, 499);
    sqlx::query("DELETE FROM normalized_events WHERE chain_id = $1")
        .bind(chain)
        .execute(scratch.pool())
        .await?;

    let second = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: Some(first.current),
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(second.complete);
    let prior_registrant: Option<String> = sqlx::query_scalar(
        "
        SELECT before_state ->> 'registrant'
        FROM normalized_events
        WHERE chain_id = $1
          AND block_number = 501
          AND event_kind = 'RegistrationGranted'
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(prior_registrant.as_deref(), Some(CONTRACT));
    scratch.cleanup().await
}

#[tokio::test]
async fn restarted_interpret_redo_preserves_retracted_resolver_evidence_for_project() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_interpret_redo_resolver_handoff").await?;
    let chain = "interpret-redo-resolver-handoff";
    seed_discovery_fixture(scratch.pool(), chain).await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         )
         SELECT $1, $1 || '-block-' || height::text,
                $1 || '-block-' || (height - 1)::text,
                height, to_timestamp(height), 'canonical'::canonicality_state
         FROM generate_series(2, 501) AS height",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, raw_fact_ref,
             derivation_kind, canonicality_state, before_state, after_state
         )
         SELECT $1 || ':manifest:' || manifest.manifest_id::text,
                manifest.namespace, 'SourceManifestUpdated', manifest.source_family,
                manifest.manifest_version, manifest.manifest_id, manifest.chain_id,
                jsonb_build_object(
                    'manifest_id', manifest.manifest_id,
                    'namespace', manifest.namespace,
                    'source_family', manifest.source_family,
                    'chain', manifest.chain_id,
                    'deployment_epoch', manifest.deployment_label
                ), 'manifest_sync', 'finalized', '{}'::jsonb,
                jsonb_build_object(
                    'manifest_version', manifest.manifest_version,
                    'normalizer_version', manifest.normalizer_version,
                    'rollout_status', manifest.rollout_status,
                    'manifest_payload', manifest.manifest_payload
                )
         FROM manifest_versions manifest
         WHERE manifest.chain_id = $1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let registry_manifest_id: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions
         WHERE chain_id = $1 AND source_family = 'ens_v2_registry_l1'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let registry_instance: Uuid = sqlx::query_scalar(
        "SELECT contract_instance_id FROM manifest_contract_instances WHERE manifest_id = $1",
    )
    .bind(registry_manifest_id)
    .fetch_one(scratch.pool())
    .await?;
    sqlx::query("INSERT INTO migration_discovery_associations (logical_edge_identity, migration_correlation_id, correlation_kind, registry_contract_instance_id, registry_address, source_manifest_id, evidence_refs, chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index, canonicality_state, consumer_visibility, interpreter_content_hash) VALUES ('redo-child-edge', 'redo-child-correlation', 'migration_registry_creation', $1, lower($2), $3, '[{\"event_identity\":\"redo-child-association\"}]', $4, 501, $5, 'redo-child-association-tx', 0, 0, 'canonical', 'candidate', $6)")
        .bind(registry_instance).bind(CONTRACT).bind(registry_manifest_id).bind(chain)
        .bind(block_hash(chain, 501)).bind(INTERPRETER_CONTENT_HASH).execute(scratch.pool()).await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id,
             block_number, block_hash, raw_fact_ref, derivation_kind,
             canonicality_state, after_state
         ) VALUES (
             'retracted-resolver-suffix', 'ens', 'ResolverChanged',
             'ens_v2_registry_l1', 1, $1, $2, 501, $3,
             '{}'::jsonb, 'ens_v2_registry_resource_surface', 'canonical',
             jsonb_build_object('resolver', lower($4::text))
         )",
    )
    .bind(registry_manifest_id)
    .bind(chain)
    .bind(block_hash(chain, 501))
    .bind(DISCOVERED_RESOLVER)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'ens:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
             'ens', 'expired.eth', ARRAY['expired','eth'], decode('00','hex'),
             '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
             ARRAY['0xexpired','0xeth'], $1, 'active', $2, $3, 501, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(chain)
    .bind(block_hash(chain, 501))
    .execute(scratch.pool())
    .await?;
    sqlx::query("INSERT INTO normalized_events (event_identity, namespace, logical_name_id, event_kind, source_family, manifest_version, source_manifest_id, chain_id, block_number, block_hash, raw_fact_ref, derivation_kind, canonicality_state, after_state) VALUES ('retracted-child-history-suffix', 'ens', 'ens:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'RegistrationReserved', 'ens_v2_registry_l1', 1, $1, $2, 501, $3, '{}'::jsonb, 'ens_v2_registry_resource_surface', 'canonical', jsonb_build_object('registry_contract_instance_id', $4))")
        .bind(registry_manifest_id).bind(chain).bind(block_hash(chain, 501))
        .bind(registry_instance.to_string()).execute(scratch.pool()).await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id,
             block_number, block_hash, raw_fact_ref, derivation_kind,
             canonicality_state, after_state
         ) VALUES (
             'retracted-expiry-root-suffix', 'ens',
             'ens:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
             'RegistrationReleased', 'ens_v2_registry_l1', 1, $1, $2, 501, $3,
             '{}'::jsonb, 'ens_v2_registry_resource_surface', 'canonical',
             jsonb_build_object(
                 'source_event', 'RegistryPathExpired',
                 'derived_from', 'interpreter_state',
                 'terminal_reason', 'registry_name_binding_expired',
                 'status', 'released'
             )
         )",
    )
    .bind(registry_manifest_id)
    .bind(chain)
    .bind(block_hash(chain, 501))
    .execute(scratch.pool())
    .await?;
    let resource_only_expiry = Uuid::parse_str("00000000-0000-0000-0000-000000000501")?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 501, 'canonical')",
    )
    .bind(resource_only_expiry)
    .bind(chain)
    .bind(block_hash(chain, 501))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, resource_id, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id,
             block_number, block_hash, raw_fact_ref, derivation_kind,
             canonicality_state, after_state
         ) VALUES (
             'retracted-resource-only-expiry', 'ens', $1,
             'RegistrationReleased', 'ens_v2_registry_l1', 1, $2, $3, 501, $4,
             '{}'::jsonb, 'ens_v2_registry_resource_surface', 'canonical',
             jsonb_build_object(
                 'source_event', 'RegistryPathExpired',
                 'derived_from', 'interpreter_state',
                 'terminal_reason', 'registry_name_binding_expired',
                 'status', 'released'
             )
         )",
    )
    .bind(resource_only_expiry)
    .bind(registry_manifest_id)
    .bind(chain)
    .bind(block_hash(chain, 501))
    .execute(scratch.pool())
    .await?;
    run_project(scratch.pool(), chain, 501, 0, 501).await?;
    let projected_before: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)
         )",
    )
    .bind(chain)
    .bind(DISCOVERED_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        projected_before,
        "fixture must publish the suffix-only resolver"
    );
    sqlx::query(
        "UPDATE resolver_current
         SET declared_summary = declared_summary || '{\"redo_guard\":true}'::jsonb
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(chain)
    .bind(DISCOVERED_RESOLVER)
    .execute(scratch.pool())
    .await?;

    let first = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 501,
            resume_current: None,
            mode: InterpretRunMode::Redo,
        })
        .await?;
    assert!(!first.complete);
    assert_eq!(first.current.number, 499);
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'ens:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
             'ens', 'retry.eth', ARRAY['retry','eth'], decode('00','hex'),
             '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
             ARRAY['0xretry','0xeth'], $1, 'active', $2, $3, 501, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(chain)
    .bind(block_hash(chain, 501))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id,
             block_number, block_hash, raw_fact_ref, derivation_kind,
             canonicality_state, after_state
         ) VALUES (
             'retracted-expiry-root-suffix', 'ens',
             'ens:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
             'RegistrationReleased', 'ens_v2_registry_l1', 1, $1, $2, 501, $3,
             '{}'::jsonb, 'ens_v2_registry_resource_surface', 'canonical',
             jsonb_build_object(
                 'source_event', 'RegistryPathExpired',
                 'derived_from', 'interpreter_state',
                 'terminal_reason', 'registry_name_binding_expired',
                 'status', 'released'
             )
         )",
    )
    .bind(registry_manifest_id)
    .bind(chain)
    .bind(block_hash(chain, 501))
    .execute(scratch.pool())
    .await?;
    let restarted_engine = Engine::new(scratch.pool().clone());
    let restarted = restarted_engine
        .run_batch(BatchRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 501,
            resume_current: None,
            mode: InterpretRunMode::Redo,
        })
        .await?;
    assert!(!restarted.complete);
    let suffix_evidence_survived: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM project_redo_resolver_evidence
             WHERE chain_id = $1 AND event_identity = 'retracted-resolver-suffix'
         )",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        suffix_evidence_survived,
        "redo restart replaced the original handoff with the re-derived prefix"
    );
    let expiry_name_survived: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM project_redo_expiry_roots
         WHERE chain_id = $1 AND event_identity = 'retracted-expiry-root-suffix'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        expiry_name_survived,
        "ens:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "redo restart replaced the first captured path-expiry name"
    );
    let resource_only_survived: Option<Uuid> = sqlx::query_scalar(
        "SELECT resource_id FROM project_redo_expiry_roots
         WHERE chain_id = $1 AND event_identity = 'retracted-resource-only-expiry'",
    )
    .bind(chain)
    .fetch_optional(scratch.pool())
    .await?;
    assert_eq!(
        resource_only_survived,
        Some(resource_only_expiry),
        "redo omitted a resource-only path-expiry release from the Project handoff"
    );
    let finished = restarted_engine
        .run_batch(BatchRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 501,
            resume_current: Some(restarted.current),
            mode: InterpretRunMode::Redo,
        })
        .await?;
    assert!(finished.complete);
    let child_handoff: Option<String> = sqlx::query_scalar(
        "SELECT logical_name_id FROM project_redo_child_registration_history WHERE chain_id = $1 AND event_identity = 'retracted-child-history-suffix'",
    )
    .bind(chain).fetch_optional(scratch.pool()).await?;
    assert_eq!(
        child_handoff.as_deref(),
        Some("ens:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
    let projected = ProjectEngine::new(scratch.pool().clone())
        .run_batch(ProjectBatchRequest {
            chain_id: chain.into(),
            target_block: 501,
            affected_from_block: 0,
            affected_to_block: 501,
            resume_current: None,
            mode: ProjectRunMode::Redo,
        })
        .await?;
    assert!(projected.complete);
    let projected_after: (bool, bool) = sqlx::query_as(
        "SELECT EXISTS (
             SELECT 1 FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)
         ), COALESCE((
             SELECT declared_summary @> '{\"redo_guard\":true}'::jsonb
             FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)
         ), false)",
    )
    .bind(chain)
    .bind(DISCOVERED_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        projected_after,
        (true, false),
        "Project did not rebuild the discovered resolver after suffix evidence retracted"
    );
    let handoff_rows: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM project_redo_resolver_evidence WHERE chain_id = $1)
              + (SELECT count(*) FROM project_redo_expiry_roots WHERE chain_id = $1)
              + (SELECT count(*) FROM project_redo_child_registration_history WHERE chain_id = $1)",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        handoff_rows, 0,
        "Project did not consume both redo handoffs"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn zero_capacity_reloads_the_exact_prior_state_from_normalized_events() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_zero_state_cache").await?;
    let chain = "interpret-zero-state-cache";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (501, "alice")]).await?;
    let first = Engine::with_state_cache_capacity(scratch.pool().clone(), 0)
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(first.complete);

    // A new engine has no retained in-process state. It must stream prior rows into an empty cache,
    // then use the request-shaped reload for the evicted value needed at block 501.
    let second = Engine::with_state_cache_capacity(scratch.pool().clone(), 0)
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 2,
            to_block: 501,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(second.complete);
    let prior_registrant: Option<String> = sqlx::query_scalar(
        "SELECT before_state ->> 'registrant' FROM normalized_events \
         WHERE chain_id = $1 AND block_number = 501 AND event_kind = 'RegistrationGranted'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(prior_registrant.as_deref(), Some(CONTRACT));
    scratch.cleanup().await
}

#[tokio::test]
async fn cache_reload_snapshot_aba_is_rejected_by_the_orphaning_epoch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_state_reload_aba").await?;
    let chain = "interpret-state-reload-aba";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (501, "alice")]).await?;
    insert_orphaned_registration(scratch.pool(), chain, 1, "bob").await?;
    publish_heads(
        scratch.pool(),
        chain,
        &HeadMarkers {
            latest: BlockMarker::new(501, block_hash(chain, 501))?,
            safe: None,
            finalized: None,
        },
    )
    .await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    // The fourth connection acquisition is the bounded-cache miss snapshot: target and marker
    // selection use the first two, and the input snapshot uses the third.
    let (engine_pool, reload_acquire_reached, release_reload_acquire) =
        pool_with_acquire_gate(scratch.pool(), 4).await?;
    let pool = engine_pool.clone();
    let chain_for_task = chain.to_owned();
    let running = tokio::spawn(async move {
        Engine::with_state_cache_capacity(pool, 0)
            .run_batch(BatchRequest {
                chain_id: chain_for_task,
                from_block: 2,
                to_block: 501,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await
    });

    reload_acquire_reached.notified().await;
    switch_live_lineage(scratch.pool(), chain, &format!("{chain}-orphan-1")).await?;
    switch_live_lineage(scratch.pool(), chain, &block_hash(chain, 1)).await?;
    sqlx::query(
        "UPDATE chain_heads SET lineage_orphaning_epoch = lineage_orphaning_epoch + 1 \
         WHERE chain_id = $1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    release_reload_acquire.notify_one();

    let error = running
        .await?
        .expect_err("an ABA change across the state reload snapshot must retry");
    assert_eq!(error.kind(), InterpretErrorKind::Transient);
    assert!(error.to_string().contains("between input reads and write"));
    let block_501_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE chain_id = $1 AND block_number = 501",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(block_501_events, 0);
    engine_pool.close().await;
    scratch.cleanup().await
}

#[tokio::test]
async fn cached_prior_state_reloads_when_an_earlier_dependency_is_repaired() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_prior_dependency_repair").await?;
    let chain = "interpret-prior-dependency-repair";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (501, "alice")]).await?;
    insert_orphaned_registration(scratch.pool(), chain, 1, "alice").await?;
    let engine = Engine::new(scratch.pool().clone());
    let first = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert_eq!(first.current.number, 499);

    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = CASE
            WHEN block_hash = $2 THEN 'canonical'::canonicality_state
            ELSE 'orphaned'::canonicality_state
        END
        WHERE chain_id = $1 AND block_number = 1
        ",
    )
    .bind(chain)
    .bind(format!("{chain}-orphan-1"))
    .execute(scratch.pool())
    .await?;

    let second = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: Some(first.current),
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(second.complete);
    let prior_registrant: Option<String> = sqlx::query_scalar(
        "
        SELECT before_state ->> 'registrant'
        FROM normalized_events
        WHERE chain_id = $1
          AND block_number = 501
          AND event_kind = 'RegistrationGranted'
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(prior_registrant, None);
    scratch.cleanup().await
}

#[tokio::test]
async fn completed_redo_evicts_cached_normal_fold_before_resume() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_redo_cache_evict").await?;
    let chain = "interpret-redo-cache-evict";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (501, "alice")]).await?;
    let engine = Engine::new(scratch.pool().clone());
    let first = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(!first.complete);
    assert_eq!(first.current.number, 499);

    replace_registered_label(scratch.pool(), chain, 1, "bob").await?;
    let redo_first = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: None,
            mode: InterpretRunMode::Redo,
        })
        .await?;
    assert!(!redo_first.complete);
    let redo_second = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: Some(redo_first.current),
            mode: InterpretRunMode::Redo,
        })
        .await?;
    assert!(redo_second.complete);

    let resumed = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 501,
            resume_current: Some(first.current),
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(resumed.complete);
    let before_registrant: Option<String> = sqlx::query_scalar(
        "SELECT before_state ->> 'registrant' FROM normalized_events \
         WHERE chain_id = $1 AND block_number = 501 AND event_kind = 'RegistrationGranted'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(before_registrant, None, "resume must reload the redo fold");
    scratch.cleanup().await
}

#[tokio::test]
async fn lapsed_reregistration_rotates_the_resource_lineage_and_binding() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_reregistration").await?;
    let chain = "interpret-reregistration";
    seed_fixture_with_timestamps(
        scratch.pool(),
        chain,
        &[(1, "alice"), (2, "alice")],
        &[(2, 10_000_000)],
    )
    .await?;

    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let registration_resources: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT resource_id) FROM normalized_events \
         WHERE chain_id = $1 AND event_kind = 'RegistrationGranted'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let registration_lineages: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT token_lineage_id) FROM resources WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let bindings: Vec<(bool,)> = sqlx::query_as(
        "SELECT active_to IS NULL FROM surface_bindings \
         WHERE chain_id = $1 ORDER BY active_from",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(registration_resources, 2);
    assert_eq!(registration_lineages, 2);
    assert_eq!(bindings, vec![(false,), (true,)]);
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_redo_replays_the_dependent_suffix_through_the_recorded_head() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_redo").await?;
    let chain = "interpret-redo";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "bob")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        UPDATE normalized_events
        SET before_state = '{\"tampered\":true}'::jsonb
        WHERE block_number IN (1, 2)
          AND event_kind = 'RegistrationGranted'
        ",
    )
    .execute(scratch.pool())
    .await?;

    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_phase_extent(scratch.pool(), chain, INTERPRETER_CONTENT_HASH).await?;
    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.runner().pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-redo-runner",
        test_timing(),
    )?;
    runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;

    let states: Vec<(i64, bool)> = sqlx::query_as(
        "
        SELECT block_number, before_state ? 'tampered'
        FROM normalized_events
        WHERE event_kind = 'RegistrationGranted'
        ORDER BY block_number
        ",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(states, [(1, false), (2, false)]);
    let identity_states: Vec<String> =
        sqlx::query_scalar("SELECT canonicality_state::text FROM name_surfaces ORDER BY raw_name")
            .fetch_all(scratch.pool())
            .await?;
    assert_eq!(identity_states, ["canonical", "canonical"]);
    let state: (String, bool, Option<String>) = sqlx::query_as(
        "
        SELECT phase_status, redo_in_progress, input_content_hash
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        (
            "completed".into(),
            false,
            Some(INTERPRETER_CONTENT_HASH.into())
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn pre_surface_resolver_materialization_matches_fresh_resume_and_redo() -> Result<()> {
    let (fresh, fresh_snapshot) = live_materialization("fresh", false).await?;
    let (resumed, resumed_snapshot) = live_materialization("resume", true).await?;

    let redone = ScratchDatabase::create("pre_surface_pointer_redo").await?;
    seed_pre_surface_pointer(redone.pool()).await?;
    run_engine(
        redone.pool(),
        "ethereum-mainnet",
        0,
        2,
        InterpretRunMode::Normal,
    )
    .await?;
    complete_interpret_state(&redone, 2).await?;
    let raw_before = raw_log_digest(redone.pool()).await?;
    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(redone.pool().clone())),
    )?;
    PhaseRunner::new(
        redone.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "pre-surface-pointer-redo",
        test_timing(),
    )?
    .redo(
        &chain_config("ethereum-mainnet")?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(2, 2)?,
        CancellationToken::new(),
    )
    .await?;
    let redo_snapshot = materialization_snapshot(redone.pool()).await?;
    assert_eq!(fresh_snapshot, resumed_snapshot);
    assert_eq!(fresh_snapshot, redo_snapshot);
    assert_eq!(raw_before, raw_log_digest(redone.pool()).await?);
    assert_eq!(fresh_snapshot["pointer_count"], 1);
    assert_eq!(fresh_snapshot["binding_count"], 1);
    run_project(redone.pool(), "ethereum-mainnet", 2, 0, 2).await?;
    let projected_control: (Option<String>, Option<String>, Option<String>, bool, bool) =
        sqlx::query_as(
            "SELECT declared_summary #>> '{control,registry_owner}',
                declared_summary #>> '{control,status}',
                declared_summary #>> '{registration,authority_kind}',
                declared_summary #>> '{registration,authority_key}' IS NOT NULL,
                EXISTS (SELECT 1 FROM address_names_current address
                        WHERE address.logical_name_id = name_current.logical_name_id
                          AND address.relation = 'effective_controller'
                          AND address.address = $1)
         FROM name_current WHERE raw_name = 'pointer.eth'",
        )
        .bind(REGISTRANT)
        .fetch_one(redone.pool())
        .await?;
    assert_eq!(
        projected_control,
        (
            Some(REGISTRANT.to_owned()),
            None,
            Some("registry_only".to_owned()),
            true,
            true
        ),
        "owned surface materialization must match release-rebound control fields"
    );
    let projected_binding: (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT registry_owner, registry_contract FROM permissions_current_resource_summary
         JOIN name_current USING (resource_id) WHERE raw_name = 'pointer.eth'",
    )
    .fetch_one(redone.pool())
    .await?;
    assert_eq!(
        projected_binding,
        (
            Some(REGISTRANT.to_owned()),
            Some("0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned())
        ),
        "owned surface materialization must retain the registry-owner binding"
    );
    fresh.cleanup().await?;
    resumed.cleanup().await?;
    redone.cleanup().await
}

#[tokio::test]
async fn pre_surface_registry_fallback_after_registrar_expiry_matches_fresh_resume_and_redo()
-> Result<()> {
    let (fresh, fresh_snapshot) = live_pre_surface_registry_fallback("fresh", false).await?;
    let (resumed, resumed_snapshot) = live_pre_surface_registry_fallback("resume", true).await?;

    let redone = ScratchDatabase::create("pre_surface_registry_fallback_redo").await?;
    seed_pre_surface_registry_fallback(redone.pool()).await?;
    run_engine(
        redone.pool(),
        "ethereum-mainnet",
        0,
        2,
        InterpretRunMode::Normal,
    )
    .await?;
    complete_interpret_state(&redone, 2).await?;
    let raw_before = raw_log_digest(redone.pool()).await?;
    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(redone.pool().clone())),
    )?;
    PhaseRunner::new(
        redone.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "pre-surface-registry-fallback-redo",
        test_timing(),
    )?
    .redo(
        &chain_config("ethereum-mainnet")?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(1, 2)?,
        CancellationToken::new(),
    )
    .await?;
    let redo_snapshot = materialization_snapshot(redone.pool()).await?;
    assert_eq!(fresh_snapshot, resumed_snapshot);
    assert_eq!(fresh_snapshot, redo_snapshot);
    assert_eq!(raw_before, raw_log_digest(redone.pool()).await?);

    run_project(redone.pool(), "ethereum-mainnet", 2, 0, 2).await?;
    let projected: (Option<String>, Option<Uuid>) = sqlx::query_as(
        "SELECT declared_summary -> 'control' ->> 'registry_owner', resource_id
         FROM name_current
         WHERE raw_name = 'pointer.eth'",
    )
    .fetch_one(redone.pool())
    .await
    .context("load projected pointer.eth registry fallback")?;
    assert_eq!(projected.0.as_deref(), Some(PRIOR_REGISTRY_OWNER));
    let rebound_resource: Option<Uuid> = sqlx::query_scalar(
        "SELECT resource_id
         FROM normalized_events
         WHERE chain_id = 'ethereum-mainnet'
           AND block_number = 2
           AND event_kind = 'SurfaceBound'
           AND after_state ->> 'source_event' = 'RegistrationReleased'",
    )
    .fetch_one(redone.pool())
    .await
    .context("load release SurfaceBound resource")?;
    assert_eq!(projected.1, rebound_resource);
    fresh.cleanup().await?;
    resumed.cleanup().await?;
    redone.cleanup().await
}

#[tokio::test]
async fn current_registry_handoff_retracts_surfaced_old_resolver_after_redo_and_project()
-> Result<()> {
    let scratch = ScratchDatabase::create("old_registry_resolver_handoff").await?;
    seed_old_registry_resolver_handoff(scratch.pool()).await?;
    run_engine(
        scratch.pool(),
        "ethereum-mainnet",
        0,
        6,
        InterpretRunMode::Normal,
    )
    .await?;
    complete_interpret_state(&scratch, 6).await?;
    let raw_before = raw_log_digest(scratch.pool()).await?;
    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.pool().clone())),
    )?;
    PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "old-registry-resolver-handoff-redo",
        test_timing(),
    )?
    .redo(
        &chain_config("ethereum-mainnet")?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(4, 6)?,
        CancellationToken::new(),
    )
    .await?;
    assert_eq!(raw_before, raw_log_digest(scratch.pool()).await?);
    let clears: Vec<(String, String, bool, Uuid)> = sqlx::query_as(
        "SELECT after_state ->> 'resolver', after_state ->> 'previous_resolver',
                logical_name_id IS NOT NULL, resource_id
         FROM normalized_events
         WHERE chain_id = 'ethereum-mainnet'
           AND block_number = 4
           AND event_kind = 'ResolverChanged'
           AND after_state ->> 'registry_fallback_handoff' = 'true'
         ORDER BY resource_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(clears.len(), 2, "both linked resources must be retracted");
    assert!(clears.iter().all(|clear| {
        clear.0 == "0x0000000000000000000000000000000000000000"
            && clear.1 == "0xF29100983E058B709F3D539b0c765937B804AC15".to_ascii_lowercase()
            && clear.2
    }));
    assert_ne!(clears[0].3, clears[1].3);
    run_project(scratch.pool(), "ethereum-mainnet", 6, 0, 6).await?;
    let projected: (Option<String>, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT declared_summary #>> '{resolver,address}',
                declared_summary #>> '{registration,status}',
                declared_summary #>> '{control,status}'
         FROM name_current WHERE raw_name = 'pointer.eth'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        projected.0, None,
        "Project must not serve the retired fallback"
    );
    assert_eq!(projected.1.as_deref(), Some("unregistered"));
    assert_eq!(projected.2.as_deref(), Some("unregistered"));
    assert_handoff_product_history(scratch.pool(), &clears).await?;
    scratch.cleanup().await
}

async fn assert_handoff_product_history(
    pool: &PgPool,
    clears: &[(String, String, bool, Uuid)],
) -> Result<()> {
    use bigname_storage::{
        EventHistoryAddressFilter, EventHistoryFilter, HistoryScope, HistorySummaryMode,
        load_event_history_page, load_name_history, load_resource_history,
    };
    let logical_name: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM name_current WHERE raw_name = 'pointer.eth'",
    )
    .fetch_one(pool)
    .await?;
    let filter = EventHistoryFilter {
        logical_name_id: Some(logical_name.clone()),
        event_kinds: vec!["ResolverChanged".into()],
        from_block: Some(4),
        to_block: Some(4),
        ..Default::default()
    };
    let page = load_event_history_page(
        pool,
        filter.clone(),
        true,
        None,
        10,
        HistorySummaryMode::Full,
        false,
    )
    .await?;
    assert_eq!(
        page.summary.as_ref().unwrap().total_count,
        2,
        "resource copies of one handoff must appear once in product history"
    );
    assert_eq!(page.rows.len(), 2);
    assert!(page.next_cursor.is_none());
    for address in [
        None,
        Some(EventHistoryAddressFilter {
            address: PRIOR_REGISTRY_OWNER.into(),
            relation: None,
        }),
    ] {
        let scoped = EventHistoryFilter {
            logical_name_id: None,
            address,
            ..filter.clone()
        };
        let scope_page = load_event_history_page(
            pool,
            scoped,
            true,
            None,
            10,
            HistorySummaryMode::Full,
            false,
        )
        .await?;
        assert_eq!(
            scope_page.rows.len(),
            2,
            "global/address scope preserves one handoff plus the resolver selection"
        );
        assert_eq!(scope_page.summary.unwrap().total_count, 2);
    }
    let diagnostic = load_event_history_page(
        pool,
        filter.clone(),
        true,
        None,
        10,
        HistorySummaryMode::None,
        true,
    )
    .await?;
    assert_eq!(
        diagnostic.rows.len(),
        clears.len() + 1,
        "retain every diagnostic resource copy"
    );
    let name_rows =
        load_name_history(pool, &logical_name, &[], HistoryScope::Surface, true).await?;
    assert_eq!(
        name_rows
            .iter()
            .filter(|event| event.after_state["registry_fallback_handoff"] == true)
            .count(),
        1
    );
    for clear in clears {
        let rows = load_resource_history(pool, clear.3, &[], HistoryScope::Resource, true).await?;
        assert_eq!(
            rows.iter()
                .filter(|event| event.after_state["registry_fallback_handoff"] == true)
                .count(),
            1,
            "each resource keeps its sole matching clear"
        );
    }
    let all = EventHistoryFilter {
        from_block: None,
        to_block: None,
        ..filter
    };
    let first = load_event_history_page(
        pool,
        all.clone(),
        true,
        None,
        1,
        HistorySummaryMode::Count,
        false,
    )
    .await?;
    let cursor = first
        .next_cursor
        .as_ref()
        .expect("an earlier resolver event remains");
    let rest = load_event_history_page(
        pool,
        all,
        true,
        Some(cursor),
        100,
        HistorySummaryMode::Full,
        false,
    )
    .await?;
    assert_eq!(
        first.rows.len() + rest.rows.len(),
        rest.summary.as_ref().unwrap().total_count as usize
    );
    assert!(
        rest.rows
            .iter()
            .all(|event| event.event_identity != first.rows[0].event_identity)
    );
    assert_eq!(
        first
            .rows
            .iter()
            .chain(&rest.rows)
            .filter(|event| event.after_state["registry_fallback_handoff"] == true)
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn interpret_redo_rejects_a_range_without_ingest_cursor_coverage() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_redo_raw_presence").await?;
    let chain = "interpret-redo-raw-presence";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "bob")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_phase_extent(scratch.pool(), chain, INTERPRETER_CONTENT_HASH).await?;
    sqlx::query(
        "
        UPDATE ingest_cursors
        SET next_block_number = 1,
            last_processed_block_number = NULL,
            last_processed_block_hash = NULL
        WHERE chain_id = $1
          AND source_key = 'source'
        ",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.runner().pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-redo-raw-presence-runner",
        test_timing(),
    )?;

    let error = runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("interpret redo must prove the selected raw range was ingested");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("raw-data presence"));

    let registrations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE event_kind = 'RegistrationGranted'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(registrations, 2);
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_loader_rejects_two_live_hashes_at_one_height() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_duplicate_live_loader").await?;
    let chain = "interpret-duplicate-live-loader";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    permit_duplicate_live_heights_for_corruption_test(scratch.pool()).await?;
    insert_competing_live_block(scratch.pool(), chain, 1).await?;

    let error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("the loader must reject ambiguous live lineage");
    assert_eq!(error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(error.to_string().contains("multiple live-lineage hashes"));
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_redo_presence_rejects_two_live_hashes_at_one_height() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_duplicate_live_redo").await?;
    let chain = "interpret-duplicate-live-redo";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_phase_extent_at(scratch.pool(), chain, 1, INTERPRETER_CONTENT_HASH).await?;
    permit_duplicate_live_heights_for_corruption_test(scratch.pool()).await?;
    insert_competing_live_block(scratch.pool(), chain, 1).await?;
    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.runner().pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-duplicate-live-redo-runner",
        test_timing(),
    )?;

    let error = runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("redo presence must reject ambiguous live lineage");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("multiple hashes at one height"));
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_redo_requires_every_applicable_source_cursor_to_cover_the_suffix() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_interpret_redo_source_presence").await?;
    let chain = "interpret-redo-source-presence";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "bob")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_phase_extent(scratch.pool(), chain, INTERPRETER_CONTENT_HASH).await?;
    let configured = ChainConfig::new(
        chain,
        vec![
            SourceConfig::new(
                chain,
                "source",
                "test",
                SeedBasis::EthereumHead,
                0,
                "http://source.invalid",
            )?,
            SourceConfig::new(
                chain,
                "source-b",
                "test",
                SeedBasis::EthereumHead,
                0,
                "http://source-b.invalid",
            )?,
        ],
        false,
    )?;
    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.runner().pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-redo-source-presence-runner",
        test_timing(),
    )?;

    let missing = runner
        .redo(
            &configured,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("each applicable configured source needs its own cursor");
    assert!(missing.to_string().contains("source-b has no cursor"));

    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number
         ) VALUES ($1, 'source-b', 'test', 'ethereum_head', 0, 1, 2)",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let behind = runner
        .redo(
            &configured,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("one advanced source must not mask a behind source");
    assert!(behind.to_string().contains("source-b covers through 0"));
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_redo_is_blocked_by_a_partial_old_content_hash_epoch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_hash_gate").await?;
    let chain_id = "interpret-hash-gate";
    seed_fixture(scratch.pool(), chain_id, &[(1, "alice"), (2, "bob")]).await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_completed_phase_extent(scratch.pool(), chain_id, "keccak256:older-binary").await?;
    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.runner().pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-hash-gate-runner",
        test_timing(),
    )?;

    let error = runner
        .redo(
            &chain_config(chain_id)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a partial redo must not adopt a changed interpreter hash");
    assert_eq!(error.kind(), ErrorKind::ContentHashMismatch);
    assert!(error.to_string().contains("full range 0..=2"));
    scratch.cleanup().await
}

#[tokio::test]
async fn unnormalizable_raw_label_creates_a_deactivated_shadow_identity() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_shadow").await?;
    let chain = "interpret-shadow";
    seed_fixture(scratch.pool(), chain, &[(1, "bad\u{1}")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let surface: (String, String, Option<String>, bool) = sqlx::query_as(
        "
        SELECT raw_name,
               visibility_state,
               deactivation_reason,
               deactivated_at IS NOT NULL
        FROM name_surfaces
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        surface,
        (
            "bad\u{1}.eth".into(),
            "shadow".into(),
            Some("normalization_gate".into()),
            true
        )
    );
    let flag: (bool, bool) = sqlx::query_as(
        "
        SELECT normalized_under_version, normalization_error IS NOT NULL
        FROM label_preimages
        WHERE decoded_label = $1
        ",
    )
    .bind("bad\u{1}")
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(flag, (false, true));
    let binding_count: i64 = sqlx::query_scalar("SELECT count(*) FROM surface_bindings")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(binding_count, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn non_utf8_label_persists_raw_bytes_and_completes_as_shadow() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_non_utf8_shadow").await?;
    let chain = "interpret-non-utf8-shadow";
    let raw_label = vec![0xff];
    seed_hostile_wrapper_fixture(scratch.pool(), chain, &raw_label).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    assert_hostile_label(scratch.pool(), &raw_label, None).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn embedded_dot_label_persists_bytes_and_completes_as_shadow() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_dot_shadow").await?;
    let chain = "interpret-dot-shadow";
    seed_fixture(scratch.pool(), chain, &[(1, "a.b")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    assert_hostile_label(scratch.pool(), b"a.b", Some("a.b")).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn embedded_nul_label_persists_bytes_and_completes_as_shadow() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_nul_shadow").await?;
    let chain = "interpret-nul-shadow";
    let label = "a\0b";
    seed_fixture(scratch.pool(), chain, &[(1, label), (2, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let stored: (Vec<u8>, Option<String>, bool) = sqlx::query_as(
        "
        SELECT raw_label, decoded_label, normalized_under_version
        FROM label_preimages
        WHERE raw_label = $1
        ",
    )
    .bind(label.as_bytes())
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(stored, (label.as_bytes().to_vec(), None, false));

    let identities: Vec<(String, String)> = sqlx::query_as(
        "
        SELECT raw_name, visibility_state
        FROM name_surfaces
        ORDER BY raw_name
        ",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        identities,
        [
            (String::new(), "shadow".into()),
            ("alice.eth".into(), "active".into())
        ]
    );
    let binding_count: i64 = sqlx::query_scalar("SELECT count(*) FROM surface_bindings")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(binding_count, 1);
    scratch.cleanup().await
}

#[tokio::test]
async fn hostile_resolver_strings_persist_losslessly_and_batch_completes() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_hostile_resolver_strings").await?;
    let chain = "interpret-hostile-resolver-strings";
    seed_hostile_resolver_strings(scratch.pool(), chain).await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let states: Vec<(i64, serde_json::Value)> = sqlx::query_as(
        "
        SELECT log_index, after_state
        FROM normalized_events
        WHERE chain_id = $1 AND event_kind = 'RecordChanged'
        ORDER BY log_index
        ",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(states.len(), 4);
    assert_eq!(
        states[0].1["value"],
        json!({"encoding":"hex","bytes":"0x610062"})
    );
    assert_eq!(
        states[1].1["raw_selector_key"],
        json!({"encoding":"hex","bytes":"0x780079"})
    );
    assert_eq!(states[1].1["record_family"], "text_opaque");
    assert_eq!(states[1].1["selector_key"], "0x780079");
    assert_eq!(states[1].1["record_key"], "text_opaque:0x780079");
    assert_eq!(
        states[2].1["raw_name"],
        json!({"encoding":"hex","bytes":"0x626164006e616d65"})
    );
    assert_eq!(
        states[3].1["raw_name"],
        json!({"encoding":"hex","bytes":"0xfffe"})
    );
    let hostile_preimages: Vec<(Vec<u8>, Option<String>, bool)> = sqlx::query_as(
        "
        SELECT raw_label, decoded_label, normalized_under_version
        FROM label_preimages
        WHERE raw_label IN ($1, $2)
        ORDER BY raw_label
        ",
    )
    .bind(b"bad\0name".as_slice())
    .bind([0xff_u8, 0xfe].as_slice())
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(hostile_preimages.len(), 2);
    assert!(
        hostile_preimages
            .iter()
            .all(|(_, decoded, normalized)| decoded.is_none() && !normalized)
    );
    let shadows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM name_surfaces WHERE chain_id = $1 AND visibility_state = 'shadow'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(shadows, 2);
    scratch.cleanup().await
}

#[tokio::test]
async fn recompute_scopes_resolver_preimages_through_retained_event_evidence() -> Result<()> {
    let scratch =
        ScratchDatabase::create("production_interpret_resolver_preimage_recompute").await?;
    let chain = "interpret-resolver-preimage-recompute";
    seed_hostile_resolver_strings(scratch.pool(), chain).await?;
    let transaction_hash = format!("{chain}-transaction-1");
    let name = NameChanged {
        node: B256::repeat_byte(0x32),
        name: "alice.eth".to_owned(),
    }
    .encode_log_data();
    insert_log(
        scratch.pool(),
        chain,
        &transaction_hash,
        4,
        CONTRACT,
        name.topics(),
        name.data.as_ref(),
    )
    .await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let alice_hash: String =
        sqlx::query_scalar("SELECT labelhash FROM label_preimages WHERE decoded_label = 'alice'")
            .fetch_one(scratch.pool())
            .await?;
    let retained_event: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM normalized_events
             WHERE chain_id = $1
               AND block_number = 1
               AND event_kind = 'PreimageObserved'
               AND after_state ->> 'labelhash' = $2
         )",
    )
    .bind(chain)
    .bind(&alice_hash)
    .fetch_one(scratch.pool())
    .await?;
    assert!(retained_event);
    let represented_by_surface: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM name_surfaces surface,
                           unnest(surface.labelhashes) AS labels(labelhash)
             WHERE surface.chain_id = $1 AND labels.labelhash = $2
         )",
    )
    .bind(chain)
    .bind(&alice_hash)
    .fetch_one(scratch.pool())
    .await?;
    assert!(!represented_by_surface);

    sqlx::query(
        "UPDATE label_preimages
         SET normalizer_version = 'stale-version',
             normalized_under_version = false,
             normalization_error = 'stale flag',
             provenance = jsonb_set(provenance, '{block_number}', '0'::jsonb)
         WHERE labelhash = $1",
    )
    .bind(&alice_hash)
    .execute(scratch.pool())
    .await?;

    run_engine(
        scratch.pool(),
        chain,
        1,
        1,
        InterpretRunMode::RecomputeFlags,
    )
    .await?;
    let repaired: (String, bool, Option<String>) = sqlx::query_as(
        "SELECT normalizer_version, normalized_under_version, normalization_error
         FROM label_preimages WHERE labelhash = $1",
    )
    .bind(&alice_hash)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(repaired, (NORMALIZER.into(), true, None));

    run_engine(scratch.pool(), chain, 1, 1, InterpretRunMode::Normal).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn two_hundred_fifty_six_byte_label_completes_as_shadow() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_256_byte_shadow").await?;
    let chain = "interpret-256-byte-shadow";
    let label = "a".repeat(256);
    seed_fixture(scratch.pool(), chain, &[(1, &label)]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    assert_hostile_label(scratch.pool(), label.as_bytes(), Some(&label)).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn three_kib_label_persists_by_labelhash_and_completes_as_shadow() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_3k_label_shadow").await?;
    let chain = "interpret-3k-label-shadow";
    let label = (0_u64..48)
        .map(|chunk| format!("{:x}", keccak256(chunk.to_be_bytes())))
        .collect::<String>();
    assert_eq!(label.len(), 3 * 1024);
    seed_fixture(scratch.pool(), chain, &[(1, &label)]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    assert_hostile_label(scratch.pool(), label.as_bytes(), Some(&label)).await?;
    scratch.cleanup().await
}

async fn assert_hostile_label(
    pool: &PgPool,
    raw_label: &[u8],
    decoded_label: Option<&str>,
) -> Result<()> {
    let labelhash = format!("{:#x}", keccak256(raw_label));
    let stored: (Vec<u8>, Option<String>, bool, bool) = sqlx::query_as(
        "
        SELECT raw_label, decoded_label, normalized_under_version,
               normalization_error IS NOT NULL
        FROM label_preimages
        WHERE labelhash = $1
        ",
    )
    .bind(labelhash)
    .fetch_one(pool)
    .await?;
    assert_eq!(stored.0, raw_label);
    assert_eq!(stored.1.as_deref(), decoded_label);
    assert_eq!((stored.2, stored.3), (false, true));
    let (surfaces, shadow_surfaces, bindings): (i64, i64, i64) = sqlx::query_as(
        "
        SELECT (SELECT count(*) FROM name_surfaces),
               (SELECT count(*) FROM name_surfaces
                WHERE visibility_state = 'shadow'
                  AND deactivation_reason = 'normalization_gate'
                  AND deactivated_at IS NOT NULL),
               (SELECT count(*) FROM surface_bindings)
        ",
    )
    .fetch_one(pool)
    .await?;
    assert_eq!((surfaces, shadow_surfaces, bindings), (1, 1, 0));
    let shadow_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE after_state ->> 'visibility_state' = 'shadow'",
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(shadow_events, 1);
    Ok(())
}

#[tokio::test]
async fn normalization_collision_keeps_raw_preimages_distinct_and_binds_only_visible_identity()
-> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_collision").await?;
    let chain = "interpret-collision";
    seed_fixture(scratch.pool(), chain, &[(1, "Alice"), (2, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let raw_labels: Vec<(String, String, bool)> = sqlx::query_as(
        "
        SELECT decoded_label, labelhash, normalized_under_version
        FROM label_preimages
        WHERE decoded_label IN ('Alice', 'alice')
        ORDER BY decoded_label
        ",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(raw_labels.len(), 2);
    assert_ne!(raw_labels[0].1, raw_labels[1].1);
    assert_eq!(
        raw_labels.iter().map(|row| row.2).collect::<Vec<_>>(),
        [false, true]
    );
    let visibility: Vec<(String, String)> =
        sqlx::query_as("SELECT raw_name, visibility_state FROM name_surfaces ORDER BY raw_name")
            .fetch_all(scratch.pool())
            .await?;
    assert_eq!(
        visibility,
        [
            ("Alice.eth".into(), "shadow".into()),
            ("alice.eth".into(), "active".into())
        ]
    );
    let binding_count: i64 = sqlx::query_scalar("SELECT count(*) FROM surface_bindings")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(binding_count, 1);
    scratch.cleanup().await
}

#[tokio::test]
async fn same_class_recompute_updates_flags_without_events_bindings_or_identity_anchors()
-> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_flags").await?;
    let chain = "interpret-flags";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        UPDATE label_preimages
        SET normalized_under_version = false,
            normalization_error = 'stale flag',
            normalizer_version = 'stale-version'
        WHERE decoded_label = 'alice'
        ",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE name_surfaces
         SET normalizer_version = 'stale-version'
         WHERE chain_id = $1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let events_before: Vec<(i64, serde_json::Value, serde_json::Value, String)> = sqlx::query_as(
        "SELECT normalized_event_id, before_state, after_state, canonicality_state::text
         FROM normalized_events
         WHERE chain_id = $1
         ORDER BY normalized_event_id",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    let surface_anchor_before: (String, i64, serde_json::Value, time::OffsetDateTime) =
        sqlx::query_as(
            "SELECT block_hash, block_number, provenance, observed_at
             FROM name_surfaces WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(scratch.pool())
        .await?;
    let bindings_before: Vec<(Uuid, String, i64, Option<time::OffsetDateTime>, String)> =
        sqlx::query_as(
            "SELECT surface_binding_id, block_hash, block_number, active_to,
                    canonicality_state::text
             FROM surface_bindings WHERE chain_id = $1 ORDER BY surface_binding_id",
        )
        .bind(chain)
        .fetch_all(scratch.pool())
        .await?;

    run_engine(
        scratch.pool(),
        chain,
        0,
        1,
        InterpretRunMode::RecomputeFlags,
    )
    .await?;

    let flag: (String, bool, Option<String>) = sqlx::query_as(
        "
        SELECT normalizer_version, normalized_under_version, normalization_error
        FROM label_preimages
        WHERE decoded_label = 'alice'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(flag, (NORMALIZER.into(), true, None));
    let surface_flags: (String, String, serde_json::Value) = sqlx::query_as(
        "SELECT normalizer_version, visibility_state, normalization_errors
         FROM name_surfaces WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        surface_flags,
        (NORMALIZER.into(), "active".into(), json!([]))
    );
    let events_after: Vec<(i64, serde_json::Value, serde_json::Value, String)> = sqlx::query_as(
        "SELECT normalized_event_id, before_state, after_state, canonicality_state::text
         FROM normalized_events
         WHERE chain_id = $1
         ORDER BY normalized_event_id",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    let surface_anchor_after: (String, i64, serde_json::Value, time::OffsetDateTime) =
        sqlx::query_as(
            "SELECT block_hash, block_number, provenance, observed_at
             FROM name_surfaces WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(scratch.pool())
        .await?;
    let bindings_after: Vec<(Uuid, String, i64, Option<time::OffsetDateTime>, String)> =
        sqlx::query_as(
            "SELECT surface_binding_id, block_hash, block_number, active_to,
                    canonicality_state::text
             FROM surface_bindings WHERE chain_id = $1 ORDER BY surface_binding_id",
        )
        .bind(chain)
        .fetch_all(scratch.pool())
        .await?;
    assert_eq!(events_after, events_before);
    assert_eq!(surface_anchor_after, surface_anchor_before);
    assert_eq!(bindings_after, bindings_before);

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn shadow_to_active_recompute_stamps_redo_then_replay_derives_the_binding() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_flags_shadow_active").await?;
    let chain = "interpret-flags-shadow-active";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    sqlx::query(
        "UPDATE label_preimages
         SET normalizer_version = 'stale-version',
             normalized_under_version = false,
             normalization_error = 'stale flag'
         WHERE decoded_label = 'alice'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE name_surfaces
         SET normalizer_version = 'stale-version',
             visibility_state = 'shadow',
             normalization_errors = '[{\"error\":\"stale flag\"}]'::jsonb,
             deactivation_reason = 'normalization_gate',
             deactivated_at = now()
         WHERE chain_id = $1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query("DELETE FROM surface_bindings WHERE chain_id = $1")
        .bind(chain)
        .execute(scratch.pool())
        .await?;
    initialize_completed_recompute_extent(scratch.pool(), chain, 1).await?;
    let runner = recompute_runner(&scratch, chain, "shadow-active-runner")?;

    runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await?;

    let flags: (String, bool, String) = sqlx::query_as(
        "SELECT preimage.normalizer_version, preimage.normalized_under_version,
                surface.visibility_state
         FROM label_preimages preimage
         JOIN name_surfaces surface ON surface.chain_id = $1
         WHERE preimage.decoded_label = 'alice'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(flags, (NORMALIZER.into(), true, "active".into()));
    let open_bindings: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings
         WHERE chain_id = $1 AND active_to IS NULL
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(open_bindings, 0, "recompute must not fabricate the binding");
    assert_transition_redo(scratch.pool(), chain, 1).await?;

    runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;
    let supported: (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(DISTINCT resource_id)
         FROM surface_bindings
         WHERE chain_id = $1 AND active_to IS NULL
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(supported, (1, 1));
    assert_no_interpret_project_redo(scratch.pool(), chain).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn active_to_shadow_recompute_stamps_redo_then_replay_retracts_the_binding() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_flags_active_shadow").await?;
    let chain = "interpret-flags-active-shadow";
    seed_fixture(scratch.pool(), chain, &[(1, "Alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    let expected_surface_flags: (String, String, serde_json::Value, Option<String>) =
        sqlx::query_as(
            "SELECT normalizer_version, visibility_state, normalization_errors,
                    deactivation_reason
             FROM name_surfaces WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(scratch.pool())
        .await?;
    sqlx::query(
        "UPDATE label_preimages
         SET normalizer_version = 'stale-version',
             normalized_under_version = true,
             normalization_error = NULL
         WHERE decoded_label = 'Alice'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE name_surfaces
         SET normalizer_version = 'stale-version',
             visibility_state = 'active',
             normalization_errors = '[]'::jsonb,
             deactivation_reason = NULL,
             deactivated_at = NULL
         WHERE chain_id = $1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let (logical_name_id, resource_id): (String, Uuid) = sqlx::query_as(
        "SELECT surface.logical_name_id, resource.resource_id
         FROM name_surfaces surface
         JOIN resources resource ON resource.chain_id = surface.chain_id
         WHERE surface.chain_id = $1
         LIMIT 1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let stale_binding_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 'declared_registry_path', 'ens_v1', to_timestamp(1),
                   $4, $5, 1, 'canonical')",
    )
    .bind(stale_binding_id)
    .bind(logical_name_id)
    .bind(resource_id)
    .bind(chain)
    .bind(block_hash(chain, 1))
    .execute(scratch.pool())
    .await?;
    initialize_completed_recompute_extent(scratch.pool(), chain, 1).await?;
    let runner = recompute_runner(&scratch, chain, "active-shadow-runner")?;

    runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await?;

    let flags: (String, bool, String) = sqlx::query_as(
        "SELECT preimage.normalizer_version, preimage.normalized_under_version,
                surface.visibility_state
         FROM label_preimages preimage
         JOIN name_surfaces surface ON surface.chain_id = $1
         WHERE preimage.decoded_label = 'Alice'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(flags, (NORMALIZER.into(), false, "shadow".into()));
    let recomputed_surface_flags: (String, String, serde_json::Value, Option<String>) =
        sqlx::query_as(
            "SELECT normalizer_version, visibility_state, normalization_errors,
                    deactivation_reason
             FROM name_surfaces WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(recomputed_surface_flags, expected_surface_flags);
    let binding_before_replay: (bool, String) = sqlx::query_as(
        "SELECT active_to IS NULL, canonicality_state::text
         FROM surface_bindings WHERE surface_binding_id = $1",
    )
    .bind(stale_binding_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(binding_before_replay, (true, "canonical".into()));
    assert_transition_redo(scratch.pool(), chain, 1).await?;

    runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await?;
    let binding_after_replay: String = sqlx::query_scalar(
        "SELECT canonicality_state::text
         FROM surface_bindings WHERE surface_binding_id = $1",
    )
    .bind(stale_binding_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(binding_after_replay, "orphaned");
    let readable_open_bindings: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings
         WHERE chain_id = $1 AND active_to IS NULL
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(readable_open_bindings, 0);
    assert_no_interpret_project_redo(scratch.pool(), chain).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn normal_mode_rejects_stored_normalizer_state_drift() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_normalizer_drift").await?;
    let chain = "interpret-normalizer-drift";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    sqlx::query(
        "
        UPDATE label_preimages
        SET normalizer_version = 'stale-version',
            normalized_under_version = false,
            normalization_error = 'stale flag'
        WHERE decoded_label = 'alice'
        ",
    )
    .execute(scratch.pool())
    .await?;
    let preimage_error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("normal mode must not publish over stale preimage normalization state");
    assert_eq!(preimage_error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(preimage_error.to_string().contains("recompute-flags"));
    let preimage: (String, bool, Option<String>) = sqlx::query_as(
        "
        SELECT normalizer_version, normalized_under_version, normalization_error
        FROM label_preimages
        WHERE decoded_label = 'alice'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        preimage,
        ("stale-version".into(), false, Some("stale flag".into()))
    );

    sqlx::query(
        "
        UPDATE label_preimages
        SET normalizer_version = $1,
            normalized_under_version = true,
            normalization_error = NULL
        WHERE decoded_label = 'alice'
        ",
    )
    .bind(NORMALIZER)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE name_surfaces
        SET normalizer_version = 'stale-version',
            visibility_state = 'shadow',
            normalization_errors = '[{"error":"stale flag"}]'::jsonb,
            deactivation_reason = 'normalization_gate',
            deactivated_at = now()
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let surface_error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("normal mode must not publish over stale surface normalization state");
    assert_eq!(surface_error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(surface_error.to_string().contains("recompute-flags"));
    let surface: (String, String, serde_json::Value, Option<String>, bool) = sqlx::query_as(
        "
        SELECT normalizer_version, visibility_state, normalization_errors,
               deactivation_reason, deactivated_at IS NOT NULL
        FROM name_surfaces
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(surface.0, "stale-version");
    assert_eq!(surface.1, "shadow");
    assert_eq!(surface.2, json!([{"error":"stale flag"}]));
    assert_eq!(surface.3.as_deref(), Some("normalization_gate"));
    assert!(surface.4);
    let active_bindings: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings WHERE chain_id = $1 AND active_to IS NULL",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(active_bindings, 1);
    scratch.cleanup().await
}

#[tokio::test]
async fn interrupted_recompute_resumes_without_stranding_a_marker() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_flags_redo_state").await?;
    let chain = "interpret-flags-redo-state";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "bob")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    initialize_completed_recompute_extent(scratch.pool(), chain, 2).await?;
    sqlx::query(
        "UPDATE label_preimages
         SET normalizer_version = 'stale-version',
             normalized_under_version = false,
             normalization_error = 'stale flag'
         WHERE decoded_label = 'alice'",
    )
    .execute(scratch.pool())
    .await?;
    let phases = PhaseSet::with_ingest_interpret_and_project(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterruptAfterRecomputePhase {
            engine: Engine::new(scratch.pool().clone()),
        }),
        Arc::new(ProjectPhase::new(scratch.pool().clone())),
    )?;
    let interrupted_runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-flags-redo-state-runner",
        test_timing(),
    )?;

    let error = interrupted_runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 2)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the test interpret phase must interrupt recompute after its write");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("injected recompute interruption")
    );

    let interrupted: (String, bool, String, i64, i64, bool) = sqlx::query_as(
        "SELECT phase_status, redo_in_progress, redo_mode,
                redo_from_block_number, redo_to_block_number,
                last_error LIKE 'required downstream redo%'
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        interrupted,
        (
            "running".into(),
            true,
            "recompute_flags".into(),
            0,
            2,
            false,
        )
    );
    let body_committed: (String, bool) = sqlx::query_as(
        "SELECT normalizer_version, normalized_under_version
         FROM label_preimages WHERE decoded_label = 'alice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(body_committed, (NORMALIZER.into(), true));

    recompute_runner(&scratch, chain, "interpret-flags-resume-runner")?
        .redo(
            &chain_config(chain)?,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 2)?,
            CancellationToken::new(),
        )
        .await?;
    assert_no_interpret_project_redo(scratch.pool(), chain).await?;
    let repaired: (String, bool, Option<String>) = sqlx::query_as(
        "SELECT normalizer_version, normalized_under_version, normalization_error
         FROM label_preimages WHERE decoded_label = 'alice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(repaired, (NORMALIZER.into(), true, None));
    scratch.cleanup().await
}

#[tokio::test]
async fn recompute_holds_the_project_lock_through_interpret_completion() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_flags_project_lock").await?;
    let chain = "interpret-flags-project-lock";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    initialize_completed_recompute_extent(scratch.pool(), chain, 1).await?;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let phases = PhaseSet::with_ingest_interpret_and_project(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(BlockingInterpretPhase {
            inner: InterpretPhase::new(scratch.pool().clone()),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        }),
        Arc::new(ProjectPhase::new(scratch.pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-flags-project-lock-runner",
        test_timing(),
    )?;
    let configured_chain = chain_config(chain)?;
    let task = tokio::spawn(async move {
        runner
            .redo(
                &configured_chain,
                RedoPhase::RecomputeFlags,
                BlockRange::new(0, 1).expect("fixed range"),
                CancellationToken::new(),
            )
            .await
    });
    entered.notified().await;

    let lock_attempt =
        PhaseLock::acquire(scratch.writer_connect_options(), chain, PhaseName::Project).await;
    let Err(error) = lock_attempt else {
        panic!("project lock must remain held until interpret recompute finishes");
    };
    assert_eq!(error.kind(), ErrorKind::LockHeld);

    release.notify_one();
    task.await??;
    assert_no_interpret_project_redo(scratch.pool(), chain).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn recompute_widens_but_does_not_absorb_a_pending_operator_project_redo() -> Result<()> {
    assert_pending_project_redo_survives(
        "production_interpret_flags_pending_project",
        "interpret-flags-pending-project",
        Some("operator project redo interrupted"),
    )
    .await
}

#[tokio::test]
async fn recompute_preserves_a_pending_operator_project_redo_with_no_error() -> Result<()> {
    assert_pending_project_redo_survives(
        "production_interpret_flags_pending_project_null",
        "interpret-flags-pending-project-null",
        None,
    )
    .await
}

#[tokio::test]
async fn recompute_preserves_a_pending_system_project_redo() -> Result<()> {
    assert_pending_project_redo_survives(
        "production_interpret_flags_pending_system_project",
        "interpret-flags-pending-system-project",
        Some("required downstream redo: pre-existing projection repair"),
    )
    .await
}

#[tokio::test]
async fn recompute_transition_reinstall_advances_pending_project_redo_generation() -> Result<()> {
    let scratch =
        ScratchDatabase::create("production_interpret_flags_transition_generation").await?;
    let chain = "interpret-flags-transition-generation";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    sqlx::query(
        "UPDATE label_preimages
         SET normalizer_version = 'stale-version',
             normalized_under_version = false,
             normalization_error = 'stale flag'
         WHERE decoded_label = 'alice'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE name_surfaces
         SET normalizer_version = 'stale-version',
             visibility_state = 'shadow',
             normalization_errors = '[{\"error\":\"stale flag\"}]'::jsonb,
             deactivation_reason = 'normalization_gate',
             deactivated_at = now()
         WHERE chain_id = $1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    initialize_completed_recompute_extent(scratch.pool(), chain, 1).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running',
             redo_in_progress = true,
             redo_mode = 'redo',
             redo_previous_phase_status = 'completed',
             redo_previous_last_error = NULL,
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 1,
             redo_to_block_number = 1,
             last_error = 'operator project redo interrupted',
             started_at = now(),
             finished_at = NULL,
             updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let generation_before: i64 = sqlx::query_scalar(
        "SELECT redo_attempt_generation
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let interpret_generation_before: i64 = sqlx::query_scalar(
        "SELECT redo_attempt_generation
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;

    recompute_runner(
        &scratch,
        chain,
        "interpret-flags-transition-generation-runner",
    )?
    .redo(
        &chain_config(chain)?,
        RedoPhase::RecomputeFlags,
        BlockRange::new(0, 1)?,
        CancellationToken::new(),
    )
    .await?;

    let project: (i64, i64, i64, Option<i64>) = sqlx::query_as(
        "SELECT redo_attempt_generation, redo_from_block_number,
                redo_to_block_number, redo_current_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project' AND redo_in_progress",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        project,
        (generation_before + 2, 0, 1, None),
        "recompute preparation and its visibility-transition stamp must each supersede older progress"
    );
    let interpret: (i64, i64, i64, Option<i64>) = sqlx::query_as(
        "SELECT redo_attempt_generation, redo_from_block_number,
                redo_to_block_number, redo_current_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret' AND redo_in_progress",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        interpret,
        (interpret_generation_before + 2, 1, 1, None),
        "the recompute attempt and its fresh visibility-transition stamp must each advance the generation"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn recompute_resumes_its_own_queued_project_refresh() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_flags_queued_refresh").await?;
    let chain = "interpret-flags-queued-refresh";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "bob")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    initialize_completed_recompute_extent(scratch.pool(), chain, 2).await?;
    sqlx::query(
        "UPDATE label_preimages
         SET normalizer_version = 'stale-version',
             normalized_under_version = false,
             normalization_error = 'stale flag'
         WHERE decoded_label = 'alice'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running',
             redo_in_progress = true,
             redo_mode = 'redo',
             redo_previous_phase_status = 'completed',
             redo_previous_last_error = NULL,
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 0,
             redo_to_block_number = 2,
             last_error = 'required downstream redo: recompute-flags scoped projection refresh',
             started_at = now(),
             finished_at = NULL,
             updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    recompute_runner(&scratch, chain, "interpret-flags-queued-refresh-runner")?
        .redo(
            &chain_config(chain)?,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 2)?,
            CancellationToken::new(),
        )
        .await?;

    assert_no_interpret_project_redo(scratch.pool(), chain).await?;
    let repaired: (String, bool) = sqlx::query_as(
        "SELECT normalizer_version, normalized_under_version
         FROM label_preimages WHERE decoded_label = 'alice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(repaired, (NORMALIZER.into(), true));
    scratch.cleanup().await
}

#[tokio::test]
async fn source_free_recompute_flags_runs_its_internal_project_refresh() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_source_free_recompute").await?;
    let chain = "interpret-source-free-recompute";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    initialize_completed_recompute_extent(scratch.pool(), chain, 1).await?;
    sqlx::query(
        "UPDATE label_preimages
         SET normalizer_version = 'stale-version',
             normalized_under_version = false,
             normalization_error = 'stale flag'
         WHERE decoded_label = 'alice'",
    )
    .execute(scratch.pool())
    .await?;
    let source_free_chain = ChainConfig::new(chain, Vec::new(), false)?;

    recompute_runner(&scratch, chain, "interpret-source-free-recompute-runner")?
        .redo(
            &source_free_chain,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await?;

    assert_no_interpret_project_redo(scratch.pool(), chain).await?;
    let repaired: (String, bool) = sqlx::query_as(
        "SELECT normalizer_version, normalized_under_version
         FROM label_preimages WHERE decoded_label = 'alice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(repaired, (NORMALIZER.into(), true));
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_recompute_project_refresh_retains_ownership_and_resumes() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_flags_failed_refresh").await?;
    let chain = "interpret-flags-failed-refresh";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    initialize_completed_recompute_extent(scratch.pool(), chain, 1).await?;
    let phases = PhaseSet::with_ingest_interpret_and_project(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.pool().clone())),
        Arc::new(FailProjectOncePhase {
            inner: ProjectPhase::new(scratch.pool().clone()),
            attempts: AtomicUsize::new(0),
        }),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-flags-failed-refresh-runner",
        test_timing(),
    )?;

    let error = runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the first scoped project refresh must fail");
    assert!(
        error
            .to_string()
            .contains("injected project refresh failure")
    );
    let failed_owner: Option<String> = sqlx::query_scalar(
        "SELECT last_error FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project' AND redo_in_progress",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        failed_owner
            .as_deref()
            .is_some_and(|message| message.contains("recompute-flags scoped projection refresh"))
    );

    runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await?;
    assert_no_interpret_project_redo(scratch.pool(), chain).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn project_refresh_handoff_keeps_a_durable_recompute_marker() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_flags_project_handoff").await?;
    let chain = "interpret-flags-project-handoff";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    initialize_completed_recompute_extent(scratch.pool(), chain, 1).await?;
    let cancellation = CancellationToken::new();
    let phases = PhaseSet::with_ingest_interpret_and_project(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.pool().clone())),
        Arc::new(CancelAfterProjectPhase {
            inner: ProjectPhase::new(scratch.pool().clone()),
            cancellation: cancellation.clone(),
        }),
    )?;
    let interrupted_runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-flags-project-handoff-runner",
        test_timing(),
    )?;

    interrupted_runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 1)?,
            cancellation,
        )
        .await
        .expect_err("cancellation after Project must stop before Interpret");
    let durable_marker: Option<(String, String)> = sqlx::query_as(
        "SELECT redo_mode, last_error FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project' AND redo_in_progress",
    )
    .bind(chain)
    .fetch_optional(scratch.pool())
    .await?;
    assert_eq!(
        durable_marker,
        Some((
            "redo".into(),
            "recompute-flags project refresh complete; interpret flags pending".into(),
        ))
    );

    let ordinary_project_error = recompute_runner(
        &scratch,
        chain,
        "interpret-flags-project-handoff-ordinary-project",
    )?
    .redo(
        &chain_config(chain)?,
        RedoPhase::Phase(PhaseName::Project),
        BlockRange::new(0, 1)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("ordinary Project redo must not consume the recompute handoff marker");
    assert!(
        ordinary_project_error
            .to_string()
            .contains("--phase recompute-flags --from-block 0 --to-block 1")
    );
    let marker_after_ordinary_project: Option<(String, String)> = sqlx::query_as(
        "SELECT redo_mode, last_error FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project' AND redo_in_progress",
    )
    .bind(chain)
    .fetch_optional(scratch.pool())
    .await?;
    assert_eq!(marker_after_ordinary_project, durable_marker);

    recompute_runner(&scratch, chain, "interpret-flags-project-handoff-resume")?
        .redo(
            &chain_config(chain)?,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await?;
    assert_no_interpret_project_redo(scratch.pool(), chain).await?;
    scratch.cleanup().await
}

async fn assert_pending_project_redo_survives(
    database_name: &str,
    chain: &str,
    operator_error: Option<&str>,
) -> Result<()> {
    let scratch = ScratchDatabase::create(database_name).await?;
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "bob")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    initialize_completed_recompute_extent(scratch.pool(), chain, 2).await?;
    sqlx::query(
        "UPDATE label_preimages
         SET normalizer_version = 'stale-version',
             normalized_under_version = false,
             normalization_error = 'stale flag'
         WHERE decoded_label = 'alice'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running',
             redo_in_progress = true,
             redo_mode = 'redo',
             redo_previous_phase_status = 'completed',
             redo_previous_last_error = NULL,
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 1,
             redo_to_block_number = 1,
             last_error = $2,
             started_at = now(),
             finished_at = NULL,
             updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .bind(operator_error)
    .execute(scratch.pool())
    .await?;

    recompute_runner(&scratch, chain, "interpret-flags-pending-project-runner")?
        .redo(
            &chain_config(chain)?,
            RedoPhase::RecomputeFlags,
            BlockRange::new(0, 2)?,
            CancellationToken::new(),
        )
        .await?;

    let project: (String, bool, String, i64, i64, Option<String>) = sqlx::query_as(
        "SELECT phase_status, redo_in_progress, redo_mode,
                redo_from_block_number, redo_to_block_number, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        project,
        (
            "running".into(),
            true,
            "redo".into(),
            0,
            2,
            operator_error.map(str::to_owned),
        )
    );
    let interpret_pending: bool = sqlx::query_scalar(
        "SELECT redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(!interpret_pending);
    let repaired: (String, bool) = sqlx::query_as(
        "SELECT normalizer_version, normalized_under_version
         FROM label_preimages WHERE decoded_label = 'alice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(repaired, (NORMALIZER.into(), true));
    scratch.cleanup().await
}

#[tokio::test]
async fn registry_announcement_round_trips_to_a_forward_only_watch_range() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_announcement").await?;
    let chain = "interpret-announcement";
    seed_announcement_fixture(scratch.pool(), chain).await?;

    let topic0 = format!("{:#x}", RegistryCreated::SIGNATURE_HASH);
    let intake_watch = load_watch_filter(scratch.pool(), chain, 0, 1).await?;
    assert!(
        intake_watch.includes(ANNOUNCED_REGISTRY, &topic0, 1),
        "RegistryCreated must be collected from an address that is not admitted yet"
    );
    assert!(intake_watch.queries().iter().any(|query| {
        query.addresses.is_empty()
            && query.from_block == 0
            && query.to_block == 1
            && query.topic0s.iter().any(|topic| topic == &topic0)
    }));
    let watched_topic = format!("{:#x}", v2_registry_events::LabelRegistered::SIGNATURE_HASH);
    let provisional_watch = load_watch_filter(scratch.pool(), chain, 0, 5).await?;
    assert!(!provisional_watch.includes(ANNOUNCED_REGISTRY, &watched_topic, 0));
    assert!(provisional_watch.includes(ANNOUNCED_REGISTRY, &watched_topic, 1));
    assert!(provisional_watch.includes(ANNOUNCED_REGISTRY, &watched_topic, 5));

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let edge: (String, Uuid, Uuid, i64, String) = sqlx::query_as(
        "
        SELECT edge.edge_kind,
               edge.from_contract_instance_id,
               edge.to_contract_instance_id,
               edge.active_from_block_number,
               address.address
        FROM discovery_edges edge
        JOIN contract_instance_addresses address
          ON address.contract_instance_id = edge.to_contract_instance_id
         AND address.chain_id = edge.chain_id
        WHERE edge.chain_id = $1
          AND edge.edge_kind = 'registry_announcement'
          AND edge.deactivated_at IS NULL
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(edge.0, "registry_announcement");
    assert_eq!(edge.1, edge.2);
    assert_eq!(edge.3, 1);
    assert_eq!(edge.4, ANNOUNCED_REGISTRY);

    let watch = load_watch_filter(scratch.pool(), chain, 0, 5).await?;
    assert!(!watch.includes(ANNOUNCED_REGISTRY, &watched_topic, 0));
    assert!(watch.includes(ANNOUNCED_REGISTRY, &watched_topic, 1));
    assert!(watch.includes(ANNOUNCED_REGISTRY, &watched_topic, 5));
    assert!(watch.queries().iter().any(|query| {
        query.from_block == 1
            && query.to_block == 5
            && query
                .addresses
                .iter()
                .any(|address| address == ANNOUNCED_REGISTRY)
    }));
    scratch.cleanup().await
}

#[tokio::test]
async fn subregistry_topology_edge_does_not_expand_the_watch_plan() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_subregistry_topology").await?;
    let chain = "interpret-subregistry-topology";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    let (manifest_id, registry_id): (i64, Uuid) = sqlx::query_as(
        "
        SELECT manifest.manifest_id, declaration.contract_instance_id
        FROM manifest_versions manifest
        JOIN manifest_contract_instances declaration
          ON declaration.manifest_id = manifest.manifest_id
        WHERE manifest.chain_id = $1
          AND declaration.role = 'registry'
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let child_id = Uuid::new_v4();
    let child_address = "0x0000000000000000000000000000000000000077";
    sqlx::query(
        "
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, $2, 'contract', '{}'::jsonb)
        ",
    )
    .bind(child_id)
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address,
            active_from_block_number, source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 1, $4, '{}'::jsonb)
        ",
    )
    .bind(child_id)
    .bind(chain)
    .bind(child_address)
    .bind(manifest_id)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        INSERT INTO discovery_edges (
            chain_id, edge_kind, from_contract_instance_id,
            to_contract_instance_id, discovery_source, admission_basis,
            source_manifest_id, active_from_block_number,
            active_from_block_hash, canonicality_state, provenance
        )
        VALUES (
            $1, 'subregistry', $2, $3, 'SubregistryUpdated',
            'linked_subregistry_event', $4, 1, $5, 'canonical', '{}'::jsonb
        )
        ",
    )
    .bind(chain)
    .bind(registry_id)
    .bind(child_id)
    .bind(manifest_id)
    .bind(block_hash(chain, 1))
    .execute(scratch.pool())
    .await?;

    let topic0 = format!("{:#x}", v2_registry_events::LabelRegistered::SIGNATURE_HASH);
    let watch = load_watch_filter(scratch.pool(), chain, 0, 3).await?;
    assert!(!watch.includes(child_address, &topic0, 1));
    scratch.cleanup().await
}

#[tokio::test]
async fn root_resolver_discovery_projects_single_label_records_in_normal_and_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_root_resolver_watch").await?;
    let chain = "interpret-root-resolver-watch";
    seed_root_resolver_watch_fixture(scratch.pool(), chain).await?;

    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let pointer: (String, String, String, Uuid, String) = sqlx::query_as(
        "SELECT event.source_family, event.after_state ->> 'resolver',
                event.logical_name_id, event.resource_id, surface.raw_name
         FROM normalized_events event
         JOIN name_surfaces surface USING (logical_name_id)
         WHERE event.chain_id = $1 AND event.event_kind = 'ResolverChanged'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(pointer.0, "ens_v2_root_l1");
    assert_eq!(pointer.1, DISCOVERED_RESOLVER);
    assert_eq!(pointer.4, "box");

    let edge: (i64, String, String, String) = sqlx::query_as(
        "SELECT count(*), min(source.source_family), min(address.address),
                min(edge.admission_basis)
         FROM discovery_edges edge
         JOIN manifest_versions source ON source.manifest_id = edge.source_manifest_id
         JOIN contract_instance_addresses address
           ON address.contract_instance_id = edge.to_contract_instance_id
          AND address.chain_id = edge.chain_id
         WHERE edge.chain_id = $1 AND edge.edge_kind = 'resolver'
           AND edge.deactivated_at IS NULL
           AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        edge,
        (
            1,
            "ens_v2_root_l1".into(),
            DISCOVERED_RESOLVER.into(),
            "reachable_from_root".into(),
        )
    );

    let record_topic = format!("{:#x}", TextChanged::SIGNATURE_HASH);
    let watch = load_watch_filter(scratch.pool(), chain, 0, 2).await?;

    assert!(!watch.includes(DISCOVERED_RESOLVER, &record_topic, 0));
    assert!(
        watch.includes(DISCOVERED_RESOLVER, &record_topic, 1),
        "a root-discovered resolver must use ens_v2_resolver_l1 event topics"
    );
    assert!(watch.includes(DISCOVERED_RESOLVER, &record_topic, 2));

    let record: (String, String, Uuid, String, String, String) = sqlx::query_as(
        "SELECT source_family, logical_name_id, resource_id,
                after_state ->> 'record_family', after_state ->> 'selector_key',
                after_state ->> 'value'
         FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RecordChanged'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(record.0, "ens_v2_resolver_l1");
    assert_eq!(record.1, pointer.2);
    assert_eq!(record.2, pointer.3);
    assert_eq!(record.3, "text");
    assert_eq!(record.4, "url");
    assert_eq!(record.5, "https://example.test");

    run_project(scratch.pool(), chain, 2, 0, 2).await?;
    assert_root_resolver_projection(scratch.pool(), chain, &pointer.2, pointer.3).await?;

    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Redo).await?;
    let replay_counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM discovery_edges
              WHERE chain_id = $1 AND edge_kind = 'resolver' AND deactivated_at IS NULL),
             (SELECT count(*) FROM contract_instance_addresses
              WHERE chain_id = $1 AND lower(address) = lower($2) AND deactivated_at IS NULL),
             (SELECT count(*) FROM normalized_events
              WHERE chain_id = $1 AND event_kind IN ('ResolverChanged', 'RecordChanged'))",
    )
    .bind(chain)
    .bind(DISCOVERED_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(replay_counts, (1, 1, 2));
    run_project(scratch.pool(), chain, 2, 0, 2).await?;
    assert_root_resolver_projection(scratch.pool(), chain, &pointer.2, pointer.3).await?;
    scratch.cleanup().await
}

async fn assert_root_resolver_projection(
    pool: &PgPool,
    chain_id: &str,
    logical_name_id: &str,
    resource_id: Uuid,
) -> Result<()> {
    let name: (Uuid, String, String) = sqlx::query_as(
        "SELECT resource_id, raw_name, declared_summary #>> '{resolver,address}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        name,
        (resource_id, "box".into(), DISCOVERED_RESOLVER.into())
    );

    let resolver: (String, String) = sqlx::query_as(
        "SELECT support_status, declared_summary #>> '{classification,source_family}'
         FROM resolver_current
         WHERE chain_id = $1 AND lower(resolver_address) = lower($2)",
    )
    .bind(chain_id)
    .bind(DISCOVERED_RESOLVER)
    .fetch_one(pool)
    .await?;
    assert_eq!(resolver, ("supported".into(), "ens_v2_resolver_l1".into()));

    let inventory: (serde_json::Value, serde_json::Value, String, String) = sqlx::query_as(
        "SELECT selectors, entries, provenance ->> 'logical_name_id',
                provenance ->> 'resolver_address'
         FROM record_inventory_current WHERE resource_id = $1",
    )
    .bind(resource_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(inventory.2, logical_name_id);
    assert_eq!(inventory.3, DISCOVERED_RESOLVER);
    assert!(
        inventory
            .0
            .as_array()
            .is_some_and(|selectors| selectors.iter().any(|selector| {
                selector["record_key"] == "text:url"
                    && selector["record_family"] == "text"
                    && selector["selector_key"] == "url"
            }))
    );
    assert!(
        inventory
            .1
            .as_array()
            .is_some_and(|entries| entries.iter().any(|entry| {
                entry["record_key"] == "text:url"
                    && entry["status"] == "success"
                    && entry["value"] == "https://example.test"
            }))
    );
    Ok(())
}

#[tokio::test]
async fn discovery_admission_applies_to_later_logs_in_the_same_batch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_discovery").await?;
    let chain = "interpret-discovery";
    seed_discovery_fixture(scratch.pool(), chain).await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let edge: (String, String) = sqlx::query_as(
        "
        SELECT edge.edge_kind, address.address
        FROM discovery_edges edge
        JOIN contract_instance_addresses address
          ON address.contract_instance_id = edge.to_contract_instance_id
        WHERE edge.chain_id = $1
          AND edge.deactivated_at IS NULL
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(edge, ("resolver".into(), DISCOVERED_RESOLVER.into()));
    let resolver_event: (String, i64) = sqlx::query_as(
        "
        SELECT source_family, count(*)
        FROM normalized_events
        WHERE chain_id = $1
          AND event_kind = 'RecordChanged'
        GROUP BY source_family
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(resolver_event, ("ens_v2_resolver_l1".into(), 1));
    scratch.cleanup().await
}

#[tokio::test]
async fn declared_v1_resolver_precedes_v2_discovery_and_preserves_topology() -> Result<()> {
    const CHAIN: &str = "ethereum-sepolia";
    const REGISTRY: &str = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    const RESOLVER: &str = "0xE99638b40E4Fff0129D56f03b55b6bbC4BBE49b5";
    const FIRST_BLOCK: i64 = 11_163_391;

    let scratch = ScratchDatabase::create("production_interpret_declared_v1_resolver").await?;
    let profile = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(scratch.pool(), &load_repository(profile)?).await?;
    for block in FIRST_BLOCK..=FIRST_BLOCK + 2 {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(CHAIN, block))
        .bind((block > FIRST_BLOCK).then(|| block_hash(CHAIN, block - 1)))
        .bind(block)
        .execute(scratch.pool())
        .await?;
        sqlx::query(
            "INSERT INTO raw_transactions (
                 chain_id, block_hash, block_number, transaction_hash,
                 transaction_index, from_address, to_address
             ) VALUES ($1, $2, $3, $4, 0, $5, $6)",
        )
        .bind(CHAIN)
        .bind(block_hash(CHAIN, block))
        .bind(block)
        .bind(format!("{CHAIN}-transaction-{block}"))
        .bind(SENDER)
        .bind(if block == FIRST_BLOCK + 2 {
            RESOLVER
        } else {
            REGISTRY
        })
        .execute(scratch.pool())
        .await?;
    }

    let token_id = versioned_token("alice", 1);
    let owner: Address = SENDER.parse()?;
    for (log_index, fact) in [
        v2_registry_events::LabelRegistered {
            tokenId: token_id,
            labelHash: keccak256(b"alice"),
            label: "alice".to_owned(),
            owner,
            expiry: 4_000_000_000,
            sender: owner,
        }
        .encode_log_data(),
        v2_registry_events::TokenResource {
            tokenId: token_id,
            resource: U256::from(5_031),
        }
        .encode_log_data(),
    ]
    .into_iter()
    .enumerate()
    {
        insert_log_at(
            scratch.pool(),
            CHAIN,
            FIRST_BLOCK,
            &format!("{CHAIN}-transaction-{FIRST_BLOCK}"),
            i64::try_from(log_index)?,
            REGISTRY,
            fact.topics(),
            fact.data.as_ref(),
        )
        .await?;
    }
    let pointer = ResolverUpdated {
        tokenId: token_id,
        resolver: RESOLVER.parse()?,
        sender: owner,
    }
    .encode_log_data();
    insert_log_at(
        scratch.pool(),
        CHAIN,
        FIRST_BLOCK + 1,
        &format!("{CHAIN}-transaction-{}", FIRST_BLOCK + 1),
        0,
        REGISTRY,
        pointer.topics(),
        pointer.data.as_ref(),
    )
    .await?;
    let record = TextChanged {
        node: raw_namehash(&[b"alice", b"eth"]),
        indexedKey: keccak256(b"url"),
        key: "url".into(),
        value: "https://declared-v1.example.test".into(),
    }
    .encode_log_data();
    insert_log_at(
        scratch.pool(),
        CHAIN,
        FIRST_BLOCK + 2,
        &format!("{CHAIN}-transaction-{}", FIRST_BLOCK + 2),
        0,
        RESOLVER,
        record.topics(),
        record.data.as_ref(),
    )
    .await?;

    run_engine(
        scratch.pool(),
        CHAIN,
        FIRST_BLOCK,
        FIRST_BLOCK + 2,
        InterpretRunMode::Normal,
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        FIRST_BLOCK + 2,
        FIRST_BLOCK,
        FIRST_BLOCK + 2,
    )
    .await?;
    let projected_family: String = sqlx::query_scalar(
        "SELECT declared_summary #>> '{classification,source_family}'
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(projected_family, "ens_v1_resolver_l1");
    let projected_resolver: (String, String, String, String, bool, bool) = sqlx::query_as(
        "SELECT current.support_status,
                    current.declared_summary #>> '{classification,basis}',
                    current.declared_summary #>> '{classification,role}',
                    manifest.source_family,
                    current.declared_summary #> '{classification,implementation}' IS NULL,
                    current.provenance -> 'upgrade_event_id' IS NULL
             FROM resolver_current current
             JOIN manifest_versions manifest
               ON manifest.manifest_id = (current.provenance ->> 'manifest_id')::bigint
             WHERE current.chain_id = $1
               AND current.resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        projected_resolver,
        (
            "supported".into(),
            "manifest_declared_address".into(),
            "public_resolver".into(),
            "ens_v1_resolver_l1".into(),
            true,
            true,
        )
    );
    let record_event: (String, String, String) = sqlx::query_as(
        "SELECT source_family, after_state ->> 'record_key', after_state ->> 'value'
         FROM normalized_events
         WHERE chain_id = $1 AND block_number = $2 AND event_kind = 'RecordChanged'",
    )
    .bind(CHAIN)
    .bind(FIRST_BLOCK + 2)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        record_event,
        (
            "ens_v1_resolver_l1".into(),
            "text:url".into(),
            "https://declared-v1.example.test".into(),
        )
    );
    let topology: (String, String, i64) = sqlx::query_as(
        "SELECT manifest.source_family, lower(address.address), edge.active_from_block_number
         FROM discovery_edges edge
         JOIN manifest_versions manifest ON manifest.manifest_id = edge.source_manifest_id
         JOIN contract_instance_addresses address
           ON address.contract_instance_id = edge.to_contract_instance_id
         WHERE edge.chain_id = $1 AND edge.edge_kind = 'resolver'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        topology,
        (
            "ens_v2_registry_l1".into(),
            RESOLVER.to_ascii_lowercase(),
            FIRST_BLOCK + 1,
        )
    );
    let pointer_events: (i64, Option<String>, i64) = sqlx::query_as(
        "SELECT count(*), min(logical_name_id), min(normalized_event_id)
         FROM normalized_events
         WHERE chain_id = $1 AND block_number = $2
           AND source_family = 'ens_v2_registry_l1'
           AND event_kind = 'ResolverChanged'
           AND lower(after_state ->> 'resolver') = lower($3)",
    )
    .bind(CHAIN)
    .bind(FIRST_BLOCK + 1)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        pointer_events,
        (
            1,
            Some(format!("ens:{:#x}", raw_namehash(&[b"alice", b"eth"]))),
            pointer_events.2,
        )
    );
    let candidate_event_ids: Value = sqlx::query_scalar(
        "SELECT provenance -> 'candidate_event_ids'
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert!(candidate_event_ids.as_array().is_some_and(|event_ids| {
        event_ids
            .iter()
            .any(|event_id| event_id == pointer_events.2)
    }));
    let inventory: (String, Value) = sqlx::query_as(
        "SELECT inventory.support_status, inventory.entries
         FROM record_inventory_current inventory
         JOIN name_current name ON name.resource_id = inventory.resource_id
         WHERE name.logical_name_id = $1",
    )
    .bind(format!("ens:{:#x}", raw_namehash(&[b"alice", b"eth"])))
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(inventory.0, "supported");
    assert!(inventory.1.as_array().is_some_and(|entries| {
        entries.iter().any(|entry| {
            entry["record_key"] == "text:url"
                && entry["value"] == "https://declared-v1.example.test"
        })
    }));
    scratch.cleanup().await
}

#[tokio::test]
async fn discovery_admitted_malformed_log_is_recorded_once_across_replay() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_decode_skip").await?;
    let chain = "interpret-decode-skip";
    seed_discovery_fixture(scratch.pool(), chain).await?;
    sqlx::query(
        "UPDATE raw_logs
         SET topics = array_append(topics, $2)
         WHERE chain_id = $1 AND log_index = 1",
    )
    .bind(chain)
    .bind(format!("{:#x}", B256::repeat_byte(0xff)))
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let skip: (i64, String, String, bool, String) = sqlx::query_as(
        "SELECT count(*), min(emitting_address), min(source_family),
                bool_and(NOT match_all), min(interpreter_content_hash)
         FROM interpret_decode_skips
         WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        skip,
        (
            1,
            DISCOVERED_RESOLVER.into(),
            "ens_v2_resolver_l1".into(),
            true,
            INTERPRETER_CONTENT_HASH.into(),
        )
    );
    let record_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RecordChanged'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(record_events, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn manifest_declared_malformed_log_remains_fatal() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_declared_malformed").await?;
    let chain = "interpret-declared-malformed";
    seed_discovery_fixture(scratch.pool(), chain).await?;
    sqlx::query(
        "UPDATE raw_logs
         SET topics = array_append(topics, $2)
         WHERE chain_id = $1 AND log_index = 0",
    )
    .bind(chain)
    .bind(format!("{:#x}", B256::repeat_byte(0xff)))
    .execute(scratch.pool())
    .await?;

    let error = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await
        .expect_err("a malformed log from a manifest-declared emitter must halt interpretation");
    assert_eq!(error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("ens_v2_registry_l1 adapter failed")
    );
    assert!(
        error
            .to_string()
            .contains("interpret-declared-malformed-block-1:0")
    );
    let skips: i64 =
        sqlx::query_scalar("SELECT count(*) FROM interpret_decode_skips WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(skips, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn discovery_update_preserves_sibling_observation_key() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_discovery_siblings").await?;
    let chain = "interpret-discovery-siblings";
    seed_sibling_discovery_fixture(scratch.pool(), chain).await?;

    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let active_targets: Vec<String> = sqlx::query_scalar(
        "
        SELECT lower(address.address)
        FROM discovery_edges edge
        JOIN contract_instance_addresses address
          ON address.contract_instance_id = edge.to_contract_instance_id
         AND address.chain_id = edge.chain_id
        WHERE edge.chain_id = $1
          AND edge.edge_kind = 'resolver'
          AND edge.deactivated_at IS NULL
        ORDER BY lower(address.address)
        ",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        active_targets,
        [
            "0x0000000000000000000000000000000000000052",
            "0x0000000000000000000000000000000000000053",
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn exact_discovery_redo_reuses_the_existing_edge_epoch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_discovery_redo").await?;
    let chain = "interpret-discovery-redo";
    seed_discovery_fixture(scratch.pool(), chain).await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    let original_edge_id: i64 =
        sqlx::query_scalar("SELECT discovery_edge_id FROM discovery_edges WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;

    run_engine(scratch.pool(), chain, 1, 1, InterpretRunMode::Redo).await?;

    let edges: Vec<(i64, String, bool)> = sqlx::query_as(
        "
        SELECT discovery_edge_id, canonicality_state::text, deactivated_at IS NULL
        FROM discovery_edges
        WHERE chain_id = $1
        ORDER BY discovery_edge_id
        ",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(edges, vec![(original_edge_id, "canonical".into(), true)]);
    scratch.cleanup().await
}

#[tokio::test]
async fn partial_discovery_redo_caps_the_replayed_edge_at_its_surviving_successor() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_discovery_successor").await?;
    let chain = "interpret-discovery-successor";
    seed_sibling_discovery_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    run_engine(scratch.pool(), chain, 1, 1, InterpretRunMode::Redo).await?;

    let epochs: Vec<(String, i64, Option<i64>, bool)> = sqlx::query_as(
        "
        SELECT lower(address.address),
               edge.active_from_block_number,
               edge.active_to_block_number,
               edge.deactivated_at IS NULL
        FROM discovery_edges edge
        JOIN contract_instance_addresses address
          ON address.contract_instance_id = edge.to_contract_instance_id
         AND address.chain_id = edge.chain_id
        WHERE edge.chain_id = $1
          AND edge.edge_kind = 'resolver'
          AND lower(address.address) IN (
              '0x0000000000000000000000000000000000000051',
              '0x0000000000000000000000000000000000000053'
          )
          AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY edge.active_from_block_number
        ",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        epochs,
        [
            (
                "0x0000000000000000000000000000000000000051".into(),
                1,
                Some(2),
                false,
            ),
            (
                "0x0000000000000000000000000000000000000053".into(),
                2,
                None,
                true,
            ),
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn discovery_open_redo_preserves_a_terminal_close_after_the_range() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_discovery_terminal").await?;
    let chain = "interpret-discovery-terminal";
    seed_discovery_fixture(scratch.pool(), chain).await?;
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number,
            block_timestamp, canonicality_state
        )
        VALUES ($1, $2, $3, 2, to_timestamp(2), 'canonical')
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 2))
    .bind(block_hash(chain, 1))
    .execute(scratch.pool())
    .await?;
    let transaction_hash = format!("{chain}-transaction-2");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash,
            transaction_index, from_address, to_address
        )
        VALUES ($1, $2, 2, $3, 0, $4, $5)
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 2))
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(CONTRACT)
    .execute(scratch.pool())
    .await?;
    let close = ResolverUpdated {
        tokenId: U256::from(7),
        resolver: Address::ZERO,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_log_at(
        scratch.pool(),
        chain,
        2,
        &transaction_hash,
        0,
        CONTRACT,
        close.topics(),
        close.data.as_ref(),
    )
    .await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    run_engine(scratch.pool(), chain, 1, 1, InterpretRunMode::Redo).await?;

    let close: (Option<i64>, bool) = sqlx::query_as(
        "
        SELECT active_to_block_number, deactivated_at IS NOT NULL
        FROM discovery_edges
        WHERE chain_id = $1
          AND edge_kind = 'resolver'
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(close, (Some(2), true));
    scratch.cleanup().await
}

#[tokio::test]
async fn discovery_backfill_does_not_cross_a_retained_address_epoch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_address_epoch").await?;
    let chain = "interpret-address-epoch";
    seed_discovery_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    sqlx::query(
        "
        UPDATE contract_instance_addresses
        SET active_from_block_number = 0,
            active_from_block_hash = NULL,
            active_to_block_number = 100,
            active_to_block_hash = NULL,
            deactivated_at = now()
        WHERE chain_id = $1
          AND lower(address) = lower($2)
        ",
    )
    .bind(chain)
    .bind(DISCOVERED_RESOLVER)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address,
            active_from_block_number, source_manifest_id, provenance
        )
        SELECT contract_instance_id, chain_id, address, 101,
               source_manifest_id, jsonb_build_object('kind', 'manifest')
        FROM contract_instance_addresses
        WHERE chain_id = $1
          AND lower(address) = lower($2)
        ",
    )
    .bind(chain)
    .bind(DISCOVERED_RESOLVER)
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 1, 1, InterpretRunMode::Redo).await?;

    let epochs: Vec<DiscoveryEpochRow> = sqlx::query_as(
        "
        SELECT active_from_block_number, active_to_block_number,
               deactivated_at IS NULL, active_from_block_hash,
               provenance ->> 'source'
        FROM contract_instance_addresses
        WHERE chain_id = $1
          AND lower(address) = lower($2)
        ORDER BY active_from_block_number
        ",
    )
    .bind(chain)
    .bind(DISCOVERED_RESOLVER)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        epochs,
        [
            (0, Some(100), false, None, Some("raw_log".into())),
            (101, None, true, None, Some("raw_log".into())),
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn partial_identity_redo_caps_a_binding_at_its_surviving_successor() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_binding_successor").await?;
    let chain = "interpret-binding-successor";
    seed_fixture(
        scratch.pool(),
        chain,
        &[(1, "alice"), (2, "alice"), (3, "alice")],
    )
    .await?;
    run_engine(scratch.pool(), chain, 0, 3, InterpretRunMode::Normal).await?;

    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Redo).await?;

    let epochs: Vec<(time::OffsetDateTime, Option<time::OffsetDateTime>)> = sqlx::query_as(
        "
        SELECT active_from, active_to
        FROM surface_bindings
        WHERE chain_id = $1
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY active_from
        ",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(epochs.len(), 3);
    assert_eq!(epochs[0].1, Some(epochs[1].0));
    assert_eq!(epochs[1].1, Some(epochs[2].0));
    assert!(epochs[2].1.is_none());
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_reanchors_a_stable_binding_after_same_timestamp_predecessor_removal() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_binding_reanchor").await?;
    let chain = "interpret-binding-reanchor";
    seed_fixture_with_timestamps(
        scratch.pool(),
        chain,
        &[(1, "alice"), (2, "alice")],
        &[(1, 42), (2, 42)],
    )
    .await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let (binding_id, old_start): (Uuid, time::OffsetDateTime) = sqlx::query_as(
        "SELECT surface_binding_id, active_from
         FROM surface_bindings
         WHERE chain_id = $1 AND block_number = 2",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Redo).await?;

    let (new_start, state): (time::OffsetDateTime, String) = sqlx::query_as(
        "SELECT active_from, canonicality_state::text
         FROM surface_bindings
         WHERE surface_binding_id = $1",
    )
    .bind(binding_id)
    .fetch_one(scratch.pool())
    .await?;
    assert!(new_start < old_start);
    assert_eq!(new_start.unix_timestamp(), 42);
    assert_eq!(state, "canonical");
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_keeps_reproduced_identity_anchors_at_their_first_derivation_block() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_identity_anchor").await?;
    let chain = "interpret-identity-anchor";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let before = identity_anchors(scratch.pool(), chain).await?;
    assert!(
        before
            .iter()
            .any(|(table, _, block, state)| table == "name_surfaces"
                && *block == 1
                && state == "canonical"),
        "the full walk anchors alice's name surface at block 1 with a later reference: {before:?}"
    );

    run_engine(scratch.pool(), chain, 1, 1, InterpretRunMode::Redo).await?;

    let after = identity_anchors(scratch.pool(), chain).await?;
    assert_eq!(
        before, after,
        "identities the redo replay reproduces must keep their first derivation anchor"
    );
    scratch.cleanup().await
}

async fn identity_anchors(
    pool: &PgPool,
    chain_id: &str,
) -> Result<Vec<(String, String, i64, String)>> {
    let mut rows = Vec::new();
    for (table, id_column) in [
        ("name_surfaces", "logical_name_id"),
        ("resources", "resource_id"),
        ("token_lineages", "token_lineage_id"),
    ] {
        let statement = format!(
            "SELECT '{table}', {id_column}::text, block_number, canonicality_state::text
             FROM {table} WHERE chain_id = $1 ORDER BY {id_column}"
        );
        rows.extend(
            sqlx::query_as::<_, (String, String, i64, String)>(&statement)
                .bind(chain_id)
                .fetch_all(pool)
                .await?,
        );
    }
    rows.sort();
    Ok(rows)
}

#[tokio::test]
async fn suffix_redo_keeps_identities_anchored_before_its_range_at_their_first_derivation_block()
-> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_suffix_redo_anchor").await?;
    let chain = "interpret-suffix-redo-anchor";
    seed_prior_owner_registration_fixture(
        scratch.pool(),
        chain,
        PriorOwnerRegistrationFlow::LegacyTwoOwnerChanges,
        REGISTRANT,
    )
    .await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    run_engine(scratch.pool(), chain, 3, 3, InterpretRunMode::Normal).await?;

    let retained_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events
         WHERE chain_id = $1
           AND source_family = 'ens_v1_registry_l1'
           AND event_kind = 'PermissionChanged'
           AND after_state ->> 'subject' = $2
           AND block_number = 1",
    )
    .bind(chain)
    .bind(PRIOR_REGISTRY_OWNER)
    .fetch_one(scratch.pool())
    .await?;
    let before = identity_anchors(scratch.pool(), chain).await?;
    let retained_anchor: (i64, String) = sqlx::query_as(
        "SELECT block_number, block_hash FROM resources WHERE chain_id = $1 AND resource_id = $2",
    )
    .bind(chain)
    .bind(retained_resource)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        retained_anchor.0, 1,
        "the superseded registry resource stays anchored at its first derivation block"
    );

    run_engine(scratch.pool(), chain, 2, 3, InterpretRunMode::Redo).await?;

    let after = identity_anchors(scratch.pool(), chain).await?;
    assert_eq!(
        before, after,
        "a redo starting after an identity's first derivation block must not move that anchor"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn discovery_redo_reopens_an_edge_when_the_replacement_range_omits_its_close() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_discovery_reopen").await?;
    let chain = "interpret-discovery-reopen";
    seed_discovery_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let block_two_hash = block_hash(chain, 2);
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number,
            block_timestamp, canonicality_state
        )
        VALUES ($1, $2, $3, 2, to_timestamp(2), 'canonical')
        ",
    )
    .bind(chain)
    .bind(&block_two_hash)
    .bind(block_hash(chain, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        UPDATE discovery_edges
        SET active_to_block_number = 2,
            active_to_block_hash = $2,
            deactivated_at = now()
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .bind(&block_two_hash)
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 1, 2, InterpretRunMode::Redo).await?;

    let edge: (String, bool, bool) = sqlx::query_as(
        "
        SELECT canonicality_state::text,
               active_to_block_number IS NULL,
               deactivated_at IS NULL
        FROM discovery_edges
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(edge, ("canonical".into(), true, true));
    scratch.cleanup().await
}

#[tokio::test]
async fn ens_v2_resource_identity_and_terminal_binding_round_trip() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_v2_lifecycle").await?;
    let chain = "interpret-v2-lifecycle";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'safe' WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'finalized' WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let resource_ids: Vec<Uuid> = sqlx::query_scalar(
        "
        SELECT DISTINCT resource_id
        FROM normalized_events
        WHERE chain_id = $1
          AND event_kind IN (
              'TokenResourceLinked',
              'PermissionChanged',
              'TokenControlTransferred'
          )
        ORDER BY resource_id
        ",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(resource_ids.len(), 1);
    let resource: (Uuid, Option<Uuid>, i64, String) = sqlx::query_as(
        "SELECT resource_id, token_lineage_id, block_number, canonicality_state::text \
         FROM resources WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(resource.0, resource_ids[0]);
    assert!(resource.1.is_some());
    assert_eq!((resource.2, resource.3.as_str()), (1, "finalized"));
    let lineage_anchor: (i64, String) = sqlx::query_as(
        "SELECT block_number, canonicality_state::text FROM token_lineages WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(lineage_anchor, (1, "finalized".into()));
    let binding: (bool, String) = sqlx::query_as(
        "
        SELECT active_to IS NOT NULL, canonicality_state::text
        FROM surface_bindings
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(binding, (true, "finalized".into()));
    let terminal_events: Vec<String> = sqlx::query_scalar(
        "
        SELECT event_kind
        FROM normalized_events
        WHERE chain_id = $1
          AND event_kind IN ('RegistrationReleased', 'SurfaceUnbound')
        ORDER BY event_kind
        ",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(terminal_events, ["RegistrationReleased", "SurfaceUnbound"]);
    scratch.cleanup().await
}

#[tokio::test]
async fn quiet_v2_expiry_reorg_matches_full_redo_on_the_winning_fork() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_v2_expiry_reorg").await?;
    let chain = "interpret-v2-expiry-reorg";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    let child = ANNOUNCED_REGISTRY;
    sqlx::query(
        "WITH root AS (
             SELECT manifest.manifest_id, declaration.contract_instance_id
             FROM manifest_versions manifest JOIN manifest_contract_instances declaration USING (manifest_id, chain_id)
             WHERE manifest.chain_id = $1 AND declaration.role = 'registry'
         ), instance AS (
             INSERT INTO contract_instances VALUES ($2, $1, 'contract', '{}'::jsonb, now()) RETURNING contract_instance_id
         ), address AS (
             INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number, source_manifest_id, provenance)
             SELECT instance.contract_instance_id, $1, $3, 0, root.manifest_id, '{}'::jsonb FROM root, instance
         ), edge AS (
             INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id, discovery_source, admission_basis, source_manifest_id, active_from_block_number, active_from_block_hash, canonicality_state, provenance)
             SELECT $1, 'registry_announcement', root.contract_instance_id, instance.contract_instance_id, 'RegistryCreated', 'reachable_from_root', root.manifest_id, 0, $4, 'canonical', '{\"observation_key\":\"fixture-child\"}'::jsonb FROM root, instance
         )
         INSERT INTO manifest_discovery_rules (manifest_id, edge_kind, from_role, admission) SELECT manifest_id, 'subregistry', 'registry', 'linked_subregistry_event' FROM root",
    )
    .bind(chain)
    .bind(Uuid::new_v4())
    .bind(child)
    .bind(block_hash(chain, 0))
    .execute(scratch.pool())
    .await?;
    let registration = v2_registry_events::LabelRegistered {
        tokenId: versioned_token("alice", 1),
        labelHash: keccak256(b"alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000061".parse()?,
        expiry: 2,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    sqlx::query(
        "UPDATE raw_logs SET data = $2 WHERE chain_id = $1 AND block_number = 1 AND log_index = 0",
    )
    .bind(chain)
    .bind(registration.data.to_vec())
    .execute(scratch.pool())
    .await?;
    sqlx::query("DELETE FROM raw_logs WHERE chain_id = $1 AND block_number = 2")
        .bind(chain)
        .execute(scratch.pool())
        .await?;
    let topology = [
        v2_registry_events::LabelRegistered {
            tokenId: versioned_token("leaf", 1),
            labelHash: keccak256(b"leaf"),
            label: "leaf".into(),
            owner: "0x0000000000000000000000000000000000000061".parse()?,
            expiry: 100,
            sender: SENDER.parse()?,
        }
        .encode_log_data(),
        v2_registry_events::TokenResource {
            tokenId: versioned_token("leaf", 1),
            resource: U256::from(6001),
        }
        .encode_log_data(),
        v2_registry_events::SubregistryUpdated {
            tokenId: versioned_token("alice", 1),
            subregistry: child.parse()?,
            sender: SENDER.parse()?,
        }
        .encode_log_data(),
        v2_registry_events::ParentUpdated {
            parent: CONTRACT.parse()?,
            label: "alice".into(),
            sender: SENDER.parse()?,
        }
        .encode_log_data(),
    ];
    for (offset, event) in topology.into_iter().enumerate() {
        insert_log_at(
            scratch.pool(),
            chain,
            1,
            &format!("{chain}-transaction-1"),
            i64::try_from(offset)? + 4,
            if offset == 2 { CONTRACT } else { child },
            event.topics(),
            event.data.as_ref(),
        )
        .await?;
    }
    run_engine(scratch.pool(), chain, 1, 2, InterpretRunMode::Normal).await?;
    assert_eq!(readable_v2_expiries(scratch.pool(), chain).await?.len(), 5);

    let mut reorg = scratch.pool().begin().await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'orphaned' WHERE chain_id = $1 AND block_number = 2")
        .bind(chain).execute(&mut *reorg).await?;
    sqlx::query(
        "WITH replacements(block_hash, parent_hash, block_number, block_timestamp) AS (
             VALUES ($2, $3, 2, 1), ($4, $2, 3, 2)
         ) INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state) SELECT $1, block_hash, parent_hash,
             block_number, to_timestamp(block_timestamp), 'canonical' FROM replacements",
    )
    .bind(chain)
    .bind(format!("{chain}-replacement-2"))
    .bind(block_hash(chain, 1))
    .bind(format!("{chain}-replacement-3"))
    .execute(&mut *reorg)
    .await?;
    reorg.commit().await?;
    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Redo).await?;
    assert_eq!(readable_v2_expiries(scratch.pool(), chain).await?.len(), 0);
    run_engine(scratch.pool(), chain, 3, 3, InterpretRunMode::Normal).await?;
    let incremental = readable_v2_expiries(scratch.pool(), chain).await?;
    assert_eq!(
        incremental
            .iter()
            .map(|(_, block, _)| *block)
            .collect::<Vec<_>>(),
        [3, 3, 3, 3, 3]
    );
    run_engine(scratch.pool(), chain, 1, 3, InterpretRunMode::Redo).await?;
    assert_eq!(
        readable_v2_expiries(scratch.pool(), chain).await?,
        incremental
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn compatible_later_name_observation_preserves_the_finalized_first_anchor() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_surface_anchor").await?;
    let chain = "interpret-surface-anchor";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "alice")]).await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'safe' WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'finalized' WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;

    let anchor: (i64, String, String) = sqlx::query_as(
        "SELECT block_number, block_hash, canonicality_state::text \
         FROM name_surfaces WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(anchor, (1, block_hash(chain, 1), "finalized".into()));
    scratch.cleanup().await
}

#[tokio::test]
async fn partial_redo_restores_resource_and_token_anchors_from_150_to_250() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_anchor_150_250").await?;
    let chain = "interpret-anchor-150-250";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp,
            canonicality_state
        )
        SELECT $1,
               $1 || '-block-' || height::text,
               $1 || '-block-' || (height - 1)::text,
               height,
               to_timestamp(height),
               'canonical'::canonicality_state
        FROM generate_series(3, 250) AS height
        ",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let (resource_id, token_lineage_id): (Uuid, Uuid) =
        sqlx::query_as("SELECT resource_id, token_lineage_id FROM resources WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    sqlx::query(
        "
        INSERT INTO normalized_events (
            event_identity, namespace, logical_name_id, resource_id, event_kind,
            source_family, manifest_version, source_manifest_id, chain_id,
            block_number, block_hash, raw_fact_ref, derivation_kind,
            canonicality_state, before_state, after_state
        )
        SELECT 'anchor-observation-250', namespace, logical_name_id, $2,
               event_kind, source_family, manifest_version, source_manifest_id,
               chain_id, 250, $3, jsonb_build_object('kind', 'raw_block'),
               derivation_kind, 'canonical', '{}'::jsonb,
               jsonb_build_object('token_lineage_id', $4::text)
        FROM normalized_events
        WHERE chain_id = $1 AND resource_id = $2
        ORDER BY normalized_event_id
        LIMIT 1
        ",
    )
    .bind(chain)
    .bind(resource_id)
    .bind(block_hash(chain, 250))
    .bind(token_lineage_id)
    .execute(scratch.pool())
    .await?;
    // The surface heal only accepts an observation that re-states the surface body, so plant a
    // body-carrying preimage observation alongside the resource/token observation.
    sqlx::query(
        "
        INSERT INTO normalized_events (
            event_identity, namespace, logical_name_id, resource_id, event_kind,
            source_family, manifest_version, source_manifest_id, chain_id,
            block_number, block_hash, raw_fact_ref, derivation_kind,
            canonicality_state, before_state, after_state
        )
        SELECT 'surface-observation-250', namespace, logical_name_id, resource_id,
               event_kind, source_family, manifest_version, source_manifest_id,
               chain_id, 250, $2, jsonb_build_object('kind', 'raw_block'),
               derivation_kind, 'canonical', before_state, after_state
        FROM normalized_events
        WHERE chain_id = $1 AND event_kind = 'PreimageObserved'
        ORDER BY normalized_event_id
        LIMIT 1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 250))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "DELETE FROM normalized_events WHERE chain_id = $1 AND event_identity NOT IN ('anchor-observation-250', 'surface-observation-250')",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    for table in ["name_surfaces", "resources", "token_lineages"] {
        sqlx::query(&format!(
            "UPDATE {table} SET block_number = 150, block_hash = $2, canonicality_state = 'canonical' WHERE chain_id = $1"
        ))
        .bind(chain)
        .bind(block_hash(chain, 150))
        .execute(scratch.pool())
        .await?;
    }
    sqlx::query(
        "
        UPDATE surface_bindings
        SET block_number = 150,
            block_hash = $2,
            active_to = NULL,
            canonicality_state = 'canonical'
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 150))
    .execute(scratch.pool())
    .await?;
    // The redone range deliberately holds no raw facts re-deriving these identities, so the
    // replay leaves them orphaned and the completion heal re-anchors all three to the surviving
    // observation at 250. An identity the replay does reproduce keeps its first derivation
    // anchor (covered by redo_keeps_reproduced_identity_anchors_at_their_first_derivation_block).
    for redo_attempt in 1..=2 {
        run_engine(scratch.pool(), chain, 100, 200, InterpretRunMode::Redo).await?;

        for table in ["name_surfaces", "resources", "token_lineages"] {
            let anchor: (i64, String) = sqlx::query_as(&format!(
                "SELECT block_number, block_hash FROM {table} WHERE chain_id = $1"
            ))
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
            assert_eq!(
                anchor,
                (250, block_hash(chain, 250)),
                "{table} after redo attempt {redo_attempt}"
            );
        }
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn partial_redo_reanchors_shadow_deactivation_time_to_surviving_observation() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_shadow_redo_reanchor").await?;
    let chain = "interpret-shadow-redo-reanchor";
    let hostile_label = "bad\u{1}";
    seed_fixture(scratch.pool(), chain, &[(1, hostile_label)]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp,
            canonicality_state
        )
        SELECT $1,
               $1 || '-block-' || height::text,
               $1 || '-block-' || (height - 1)::text,
               height,
               to_timestamp(height),
               'canonical'::canonicality_state
        FROM generate_series(2, 250) AS height
        ",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        INSERT INTO normalized_events (
            event_identity, namespace, logical_name_id, resource_id, event_kind,
            source_family, manifest_version, source_manifest_id, chain_id,
            block_number, block_hash, raw_fact_ref, derivation_kind,
            canonicality_state, before_state, after_state
        )
        SELECT 'shadow-anchor-observation-250', namespace, logical_name_id, resource_id,
               event_kind, source_family, manifest_version, source_manifest_id, chain_id,
               250, $2, jsonb_build_object('kind', 'raw_block'),
               derivation_kind, 'canonical', '{}'::jsonb, after_state
        FROM normalized_events
        WHERE chain_id = $1 AND logical_name_id IS NOT NULL AND event_kind = 'PreimageObserved'
        ORDER BY normalized_event_id
        LIMIT 1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 250))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "DELETE FROM normalized_events WHERE chain_id = $1 AND event_identity <> 'shadow-anchor-observation-250'",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        UPDATE name_surfaces
        SET block_number = 150,
            block_hash = $2,
            deactivated_at = to_timestamp(150),
            canonicality_state = 'canonical'
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 150))
    .execute(scratch.pool())
    .await?;

    // The redone range deliberately holds no raw facts reproducing the shadow surface, so the
    // replay leaves it orphaned and the completion heal re-anchors it to the surviving
    // observation. An identity the replay does reproduce keeps its first derivation anchor
    // (covered by redo_keeps_reproduced_identity_anchors_at_their_first_derivation_block).
    run_engine(scratch.pool(), chain, 100, 200, InterpretRunMode::Redo).await?;

    let anchor: (i64, i64) = sqlx::query_as(
        "
        SELECT block_number, extract(epoch FROM deactivated_at)::bigint
        FROM name_surfaces
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(anchor, (250, 250));
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_heal_leaves_a_surface_orphaned_without_a_body_carrying_observation() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_surface_heal_body").await?;
    let chain = "interpret-surface-heal-body";
    let hostile_label = "bad\u{1}";
    seed_fixture(scratch.pool(), chain, &[(1, hostile_label)]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp,
            canonicality_state
        )
        SELECT $1,
               $1 || '-block-' || height::text,
               $1 || '-block-' || (height - 1)::text,
               height,
               to_timestamp(height),
               'canonical'::canonicality_state
        FROM generate_series(2, 250) AS height
        ",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    // A reference that carries no surface body: production only persists one alongside a
    // surface re-emission, and none was derived here.
    sqlx::query(
        "
        INSERT INTO normalized_events (
            event_identity, namespace, logical_name_id, resource_id, event_kind,
            source_family, manifest_version, source_manifest_id, chain_id,
            block_number, block_hash, raw_fact_ref, derivation_kind,
            canonicality_state, before_state, after_state
        )
        SELECT 'permission-observation-250', namespace, logical_name_id, NULL,
               'PermissionChanged', source_family, manifest_version, source_manifest_id,
               chain_id, 250, $2, jsonb_build_object('kind', 'raw_block'),
               derivation_kind, 'canonical', '{}'::jsonb, '{}'::jsonb
        FROM normalized_events
        WHERE chain_id = $1 AND logical_name_id IS NOT NULL
        ORDER BY normalized_event_id
        LIMIT 1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 250))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "DELETE FROM normalized_events WHERE chain_id = $1 AND event_identity <> 'permission-observation-250'",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        UPDATE name_surfaces
        SET block_number = 150,
            block_hash = $2,
            deactivated_at = to_timestamp(150),
            canonicality_state = 'canonical'
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 150))
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 100, 200, InterpretRunMode::Redo).await?;

    let row: (i64, String, i64) = sqlx::query_as(
        "
        SELECT block_number,
               canonicality_state::text,
               extract(epoch FROM deactivated_at)::bigint
        FROM name_surfaces
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        row,
        (150, "orphaned".to_owned(), 150),
        "a reference carrying no surface body must not anchor or deactivate the surface"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_without_the_orphaned_terminal_fact_reopens_the_prior_binding() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_terminal_redo").await?;
    let chain = "interpret-terminal-redo";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'
        WHERE chain_id = $1
          AND block_number = 2
        ",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let winning_hash = format!("{chain}-winning-2");
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number,
            block_timestamp, canonicality_state
        )
        VALUES ($1, $2, $3, 2, to_timestamp(2), 'canonical')
        ",
    )
    .bind(chain)
    .bind(&winning_hash)
    .bind(block_hash(chain, 1))
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Redo).await?;

    let open: bool =
        sqlx::query_scalar("SELECT active_to IS NULL FROM surface_bindings WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    assert!(open);
    let terminal_count: i64 = sqlx::query_scalar(
        "
        SELECT count(*)
        FROM normalized_events
        WHERE chain_id = $1
          AND event_kind IN ('RegistrationReleased', 'SurfaceUnbound')
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(terminal_count, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn raw_log_loser_departure_reorg_reopens_the_survivor_binding() -> Result<()> {
    assert_loser_departure_reorg_reopens_the_survivor(false).await
}

#[tokio::test]
async fn boundary_expiry_loser_departure_reorg_reopens_the_survivor_binding() -> Result<()> {
    assert_loser_departure_reorg_reopens_the_survivor(true).await
}

async fn assert_loser_departure_reorg_reopens_the_survivor(boundary_expiry: bool) -> Result<()> {
    let fixture = if boundary_expiry {
        "production_interpret_boundary_survivor_redo"
    } else {
        "production_interpret_log_survivor_redo"
    };
    let chain = if boundary_expiry {
        "interpret-boundary-survivor-redo"
    } else {
        "interpret-log-survivor-redo"
    };
    let departure_block = if boundary_expiry { 6 } else { 3 };
    let scratch = ScratchDatabase::create(fixture).await?;
    seed_contested_v2_departure_fixture(scratch.pool(), chain, boundary_expiry).await?;

    run_engine(
        scratch.pool(),
        chain,
        if boundary_expiry { 1 } else { 0 },
        departure_block,
        InterpretRunMode::Normal,
    )
    .await?;

    let (reasserted_binding_id, survivor_resource_id, logical_name_id): (Uuid, Uuid, String) =
        sqlx::query_as(
            "SELECT surface_binding_id, resource_id, logical_name_id
         FROM surface_bindings
         WHERE chain_id = $1 AND block_number = $2",
        )
        .bind(chain)
        .bind(departure_block)
        .fetch_one(scratch.pool())
        .await?;
    let survivor_binding_id: Uuid = sqlx::query_scalar(
        "SELECT surface_binding_id
         FROM surface_bindings
         WHERE chain_id = $1
           AND logical_name_id = $2
           AND resource_id = $3
           AND block_number < $4
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
         ORDER BY active_from DESC
         LIMIT 1",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .bind(survivor_resource_id)
    .bind(departure_block)
    .fetch_one(scratch.pool())
    .await?;
    let forward_open: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT surface_binding_id, resource_id
         FROM surface_bindings
         WHERE chain_id = $1
           AND logical_name_id = $2
           AND authority_arm = 'ens_v2'
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
           AND active_to IS NULL
         ORDER BY surface_binding_id",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        forward_open,
        [(reasserted_binding_id, survivor_resource_id)],
        "the loser departure must leave only its reasserted survivor binding open"
    );
    let close_marker: (String, Uuid) = sqlx::query_as(
        "SELECT after_state ->> 'closed_authority_arm',
                (after_state ->> 'surface_binding_id')::uuid
         FROM normalized_events
         WHERE chain_id = $1
           AND block_number = $2
           AND event_kind = 'PreimageObserved'
           AND after_state ->> 'arm_wide_binding_close' = 'true'",
    )
    .bind(chain)
    .bind(departure_block)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        close_marker,
        ("ens_v2".to_owned(), reasserted_binding_id),
        "redo evidence must name the closed arm and exempt replacement binding"
    );

    replace_departure_block(scratch.pool(), chain, departure_block, false).await?;
    run_engine(
        scratch.pool(),
        chain,
        departure_block,
        departure_block,
        InterpretRunMode::Redo,
    )
    .await?;

    let reasserted_state: String = sqlx::query_scalar(
        "SELECT canonicality_state::text
         FROM surface_bindings
         WHERE surface_binding_id = $1",
    )
    .bind(reasserted_binding_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        reasserted_state, "orphaned",
        "the replacement block must orphan the removed departure's reassertion"
    );
    let replacement_open: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT surface_binding_id, resource_id
         FROM surface_bindings
         WHERE chain_id = $1
           AND logical_name_id = $2
           AND authority_arm = 'ens_v2'
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
           AND active_to IS NULL
         ORDER BY surface_binding_id",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        replacement_open,
        [(survivor_binding_id, survivor_resource_id)],
        "removing the loser departure must reopen the pre-departure survivor binding"
    );

    replace_departure_block(scratch.pool(), chain, departure_block, true).await?;
    run_engine(
        scratch.pool(),
        chain,
        departure_block,
        departure_block,
        InterpretRunMode::Redo,
    )
    .await?;

    let reapplied_open: Vec<(Uuid, Uuid, i64)> = sqlx::query_as(
        "SELECT surface_binding_id, resource_id, block_number
         FROM surface_bindings
         WHERE chain_id = $1
           AND logical_name_id = $2
           AND authority_arm = 'ens_v2'
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
           AND active_to IS NULL
         ORDER BY surface_binding_id",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        reapplied_open,
        [(reasserted_binding_id, survivor_resource_id, departure_block)],
        "reapplying the departure must converge on the original deterministic reassertion"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_arm_wide_reopen_requires_the_planted_preimage_marker() -> Result<()> {
    for marker in [
        ReplayMarker::Absent,
        ReplayMarker::Valid,
        ReplayMarker::NullBinding,
        ReplayMarker::Malformed,
        ReplayMarker::WrongArm,
        ReplayMarker::WrongRange,
        ReplayMarker::WrongName,
        ReplayMarker::UnknownBinding,
        ReplayMarker::WrongBinding,
        ReplayMarker::WrongLineage,
    ] {
        let fixture = format!("production_interpret_replay_marker_{}", marker.label());
        let chain = format!("interpret-replay-marker-{}", marker.label());
        let scratch = ScratchDatabase::create(&fixture).await?;
        let (arm_wide_predecessor, migration_predecessor) =
            seed_planted_marker_redo_fixture(scratch.pool(), &chain, marker).await?;

        run_engine(scratch.pool(), &chain, 2, 2, InterpretRunMode::Redo).await?;

        let arm_wide_reopened: bool = sqlx::query_scalar(
            "SELECT active_to IS NULL FROM surface_bindings WHERE surface_binding_id = $1",
        )
        .bind(arm_wide_predecessor)
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(
            arm_wide_reopened,
            marker == ReplayMarker::Valid,
            "the activated MigrationApplied boundary must not perform the arm-wide reopen; only \
             the planted PreimageObserved marker may do so"
        );

        let migration_reopened: bool = sqlx::query_scalar(
            "SELECT active_to IS NULL FROM surface_bindings WHERE surface_binding_id = $1",
        )
        .bind(migration_predecessor)
        .fetch_one(scratch.pool())
        .await?;
        assert!(
            migration_reopened,
            "the activated boundary still reopens its own recorded ENSv1 predecessor"
        );
        scratch.cleanup().await?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplayMarker {
    Absent,
    Valid,
    NullBinding,
    Malformed,
    WrongArm,
    WrongRange,
    WrongName,
    UnknownBinding,
    WrongBinding,
    WrongLineage,
}

impl ReplayMarker {
    fn label(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Valid => "valid",
            Self::NullBinding => "null-binding",
            Self::Malformed => "malformed",
            Self::WrongArm => "wrong-arm",
            Self::WrongRange => "wrong-range",
            Self::WrongName => "wrong-name",
            Self::UnknownBinding => "unknown-binding",
            Self::WrongBinding => "wrong-binding",
            Self::WrongLineage => "wrong-lineage",
        }
    }
}

async fn seed_planted_marker_redo_fixture(
    pool: &PgPool,
    chain: &str,
    marker: ReplayMarker,
) -> Result<(Uuid, Uuid)> {
    let logical_name_id = "ens:0xplanted-marker";
    let arm_wide_predecessor = Uuid::parse_str("00000000-0000-0000-0000-000000000101")?;
    let migration_predecessor = Uuid::parse_str("00000000-0000-0000-0000-000000000102")?;
    let replacement = Uuid::parse_str("00000000-0000-0000-0000-000000000103")?;
    let wrong_lineage_replacement = Uuid::parse_str("00000000-0000-0000-0000-000000000104")?;
    let arm_wide_resource = Uuid::parse_str("00000000-0000-0000-0000-000000000201")?;
    let migration_resource = Uuid::parse_str("00000000-0000-0000-0000-000000000202")?;
    let replacement_resource = Uuid::parse_str("00000000-0000-0000-0000-000000000203")?;

    insert_manifest(
        pool,
        chain,
        "ens_v2_registry_l1",
        "tests/planted-marker-redo.toml",
        json!({
            "manifest_version":1,
            "namespace":"ens",
            "source_family":"ens_v2_registry_l1",
            "chain":chain,
            "deployment_epoch":"fixture",
            "rollout_status":"active",
            "normalizer_version":NORMALIZER,
            "capability_flags":{},
            "roots":[],
            "contracts":[],
            "discovery_rules":[],
            "abi":{"events":[],"calls":[]}
        }),
    )
    .await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES
             ($1, $2, NULL, 1, to_timestamp(1), 'canonical'),
             ($1, $3, $2, 2, to_timestamp(2), 'canonical'),
             ($1, $4, $2, 2, to_timestamp(2), 'orphaned')",
    )
    .bind(chain)
    .bind(block_hash(chain, 1))
    .bind(block_hash(chain, 2))
    .bind(block_hash(chain, 20))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens', 'planted-marker.eth', ARRAY['planted-marker','eth'],
             decode('00', 'hex'), '0xplanted-marker', ARRAY['0xplanted-marker','0xeth'],
             $2, 'active', $3, $4, 1, 'canonical'
         ), (
             'ens:0xwrong-marker', 'ens', 'wrong-marker.eth', ARRAY['wrong-marker','eth'],
             decode('01', 'hex'), '0xwrong-marker', ARRAY['0xwrong-marker','0xeth'],
             $2, 'active', $3, $4, 1, 'canonical'
         )",
    )
    .bind(logical_name_id)
    .bind(NORMALIZER)
    .bind(chain)
    .bind(block_hash(chain, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) SELECT resource_id, $1, $2, 1, 'canonical'
           FROM unnest($3::uuid[]) AS resource_id",
    )
    .bind(chain)
    .bind(block_hash(chain, 1))
    .bind([arm_wide_resource, migration_resource, replacement_resource])
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, active_to, chain_id, block_hash,
             block_number, provenance, canonicality_state
         ) VALUES
             ($1, $4, $5, 'declared_registry_path', 'ens_v2', to_timestamp(1),
              to_timestamp(2), $6, $7, 1, '{}'::jsonb, 'canonical'),
             ($2, $4, $8, 'declared_registry_path', 'ens_v1', to_timestamp(1),
              to_timestamp(2) + interval '1 microsecond', $6, $7, 1,
              '{}'::jsonb, 'canonical'),
             ($3, $4, $9, 'declared_registry_path', 'ens_v2', to_timestamp(2),
              NULL, $6, $10, 2,
              '{\"transaction_index\":0,\"log_index\":0}'::jsonb, 'canonical')",
    )
    .bind(arm_wide_predecessor)
    .bind(migration_predecessor)
    .bind(replacement)
    .bind(logical_name_id)
    .bind(arm_wide_resource)
    .bind(chain)
    .bind(block_hash(chain, 1))
    .bind(migration_resource)
    .bind(replacement_resource)
    .bind(block_hash(chain, 2))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, active_to, chain_id, block_hash,
             block_number, provenance, canonicality_state
         ) VALUES (
             $1, $2, $3, 'declared_registry_path', 'ens_v2', to_timestamp(2),
             NULL, $4, $5, 2,
             '{\"transaction_index\":0,\"log_index\":0}'::jsonb, 'orphaned'
         )",
    )
    .bind(wrong_lineage_replacement)
    .bind(logical_name_id)
    .bind(replacement_resource)
    .bind(chain)
    .bind(block_hash(chain, 20))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, raw_fact_ref, derivation_kind,
             canonicality_state, before_state, after_state, consumer_visibility
         ) VALUES (
             $1, 'ens', $2, 'MigrationApplied', 'ens_v2_migration_l1', 1,
             $3, 2, $4, $5, 0, 1, '{}'::jsonb, 'ens_v2_migration',
             'canonical', '{}'::jsonb,
             jsonb_build_object(
                 'predecessor_binding', jsonb_build_object('authority_epoch', 'ens_v1'),
                 'successor_binding', jsonb_build_object(
                     'authority_epoch', 'ens_v2', 'binding_id', $6::text
                 )
             ),
             'activated'
         )",
    )
    .bind(format!("{chain}:activated-migration-boundary"))
    .bind(logical_name_id)
    .bind(chain)
    .bind(block_hash(chain, 2))
    .bind(format!("{chain}:migration-transaction"))
    .bind(replacement)
    .execute(pool)
    .await?;
    if marker != ReplayMarker::Absent {
        let marker_name = if marker == ReplayMarker::WrongName {
            "ens:0xwrong-marker"
        } else {
            logical_name_id
        };
        let marker_block = if marker == ReplayMarker::WrongRange {
            1
        } else {
            2
        };
        let marker_hash = block_hash(chain, marker_block);
        let closed_arm = if marker == ReplayMarker::WrongArm {
            "ens_v1"
        } else {
            "ens_v2"
        };
        let opened_binding = match marker {
            ReplayMarker::NullBinding => None,
            ReplayMarker::Malformed => Some("not-a-uuid".to_owned()),
            ReplayMarker::UnknownBinding => Some("00000000-0000-0000-0000-000000000199".to_owned()),
            ReplayMarker::WrongBinding => Some(migration_predecessor.to_string()),
            ReplayMarker::WrongLineage => Some(wrong_lineage_replacement.to_string()),
            _ => Some(replacement.to_string()),
        };
        sqlx::query(
            "INSERT INTO normalized_events (
                 event_identity, namespace, logical_name_id, event_kind, source_family,
                 manifest_version, chain_id, block_number, block_hash, transaction_hash,
                 transaction_index, log_index, raw_fact_ref, derivation_kind,
                 canonicality_state, before_state, after_state, consumer_visibility
             ) VALUES (
                 $1, 'ens', $2, 'PreimageObserved', 'ens_v2_registry_l1', 1,
                 $3, $4, $5, $6, 0, 0, '{}'::jsonb, 'raw_log_preimage_observation',
                 'canonical', '{}'::jsonb,
                 jsonb_build_object(
                     'arm_wide_binding_close', true,
                     'closed_authority_arm', $7::text,
                     'surface_binding_id', $8::text
                 ),
                 'activated'
             )",
        )
        .bind(format!("{chain}:planted-arm-wide-marker"))
        .bind(marker_name)
        .bind(chain)
        .bind(marker_block)
        .bind(marker_hash)
        .bind(format!("{chain}:marker-transaction"))
        .bind(closed_arm)
        .bind(opened_binding)
        .execute(pool)
        .await?;
    }
    Ok((arm_wide_predecessor, migration_predecessor))
}

#[tokio::test]
async fn binding_open_redo_preserves_a_terminal_close_after_the_range() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_binding_terminal").await?;
    let chain = "interpret-binding-terminal";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    let before: time::OffsetDateTime =
        sqlx::query_scalar("SELECT active_to FROM surface_bindings WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;

    run_engine(scratch.pool(), chain, 1, 1, InterpretRunMode::Redo).await?;

    let after: time::OffsetDateTime =
        sqlx::query_scalar("SELECT active_to FROM surface_bindings WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(after, before);
    let terminal_events: i64 = sqlx::query_scalar(
        "
        SELECT count(*)
        FROM normalized_events
        WHERE chain_id = $1
          AND block_number = 2
          AND event_kind IN ('RegistrationReleased', 'SurfaceUnbound')
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(terminal_events, 2);
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_ignores_raw_facts_on_orphaned_lineage() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_canonical").await?;
    let chain = "interpret-canonical";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    insert_orphaned_registration(scratch.pool(), chain, 1, "bob").await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let registrations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE event_kind = 'RegistrationGranted'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(registrations, 1);
    let orphan_preimage: i64 =
        sqlx::query_scalar("SELECT count(*) FROM label_preimages WHERE decoded_label = 'bob'")
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(orphan_preimage, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn mid_batch_orphaning_aborts_the_write_as_transient() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_mid_batch_orphan").await?;
    let chain = "interpret-mid-batch-orphan";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    let mut reorg = scratch.pool().begin().await?;
    sqlx::query(
        "
        SELECT block_hash
        FROM chain_lineage
        WHERE chain_id = $1 AND block_number = 1
        FOR UPDATE
        ",
    )
    .bind(chain)
    .fetch_one(&mut *reorg)
    .await?;

    let pool = scratch.pool().clone();
    let chain_for_task = chain.to_owned();
    let running = tokio::spawn(async move {
        Engine::new(pool)
            .run_batch(BatchRequest {
                chain_id: chain_for_task,
                from_block: 0,
                to_block: 1,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await
    });
    let mut reached_write_revalidation = false;
    for _ in 0..200 {
        reached_write_revalidation = sqlx::query_scalar(
            "
            SELECT EXISTS (
                SELECT 1
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND wait_event_type = 'Lock'
                  AND query LIKE '%FOR SHARE%'
            )
            ",
        )
        .fetch_one(scratch.pool())
        .await?;
        if reached_write_revalidation {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        reached_write_revalidation,
        "interpretation must reach write-time lineage revalidation"
    );
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned' WHERE chain_id = $1 AND block_number = 1",
    )
    .bind(chain)
    .execute(&mut *reorg)
    .await?;
    reorg.commit().await?;

    let error = running
        .await?
        .expect_err("an orphaned loaded batch must be retried");
    assert_eq!(error.kind(), InterpretErrorKind::Transient);
    assert!(error.to_string().contains("lineage changed before write"));
    let written: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(written, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn reorg_between_block_and_log_loads_is_transient_and_retries() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_load_snapshot_reorg").await?;
    let chain = "interpret-load-snapshot-reorg";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    insert_orphaned_registration(scratch.pool(), chain, 1, "bob").await?;

    let mut raw_log_lock = scratch.pool().begin().await?;
    sqlx::query("LOCK TABLE raw_logs IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *raw_log_lock)
        .await?;
    let pool = scratch.pool().clone();
    let chain_for_task = chain.to_owned();
    let running = tokio::spawn(async move {
        Engine::new(pool)
            .run_batch(BatchRequest {
                chain_id: chain_for_task,
                from_block: 0,
                to_block: 1,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await
    });

    let mut waiting_for_raw_logs = false;
    for _ in 0..200 {
        waiting_for_raw_logs = sqlx::query_scalar(
            "
            SELECT EXISTS (
                SELECT 1
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND wait_event_type = 'Lock'
                  AND query LIKE '%FROM raw_logs raw%'
            )
            ",
        )
        .fetch_one(scratch.pool())
        .await?;
        if waiting_for_raw_logs {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        waiting_for_raw_logs,
        "interpretation must reach the raw-log load"
    );

    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = CASE
            WHEN block_hash = $2 THEN 'canonical'::canonicality_state
            ELSE 'orphaned'::canonicality_state
        END
        WHERE chain_id = $1 AND block_number = 1
        ",
    )
    .bind(chain)
    .bind(format!("{chain}-orphan-1"))
    .execute(scratch.pool())
    .await?;
    raw_log_lock.commit().await?;

    let error = running
        .await?
        .expect_err("lineage churn during load must retry");
    assert_eq!(error.kind(), InterpretErrorKind::Transient);
    let written: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(written, 0);

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    let labels: Vec<String> = sqlx::query_scalar(
        "SELECT decoded_label FROM label_preimages WHERE decoded_label IS NOT NULL ORDER BY decoded_label",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert!(labels.iter().any(|label| label == "bob"));
    assert!(!labels.iter().any(|label| label == "alice"));
    scratch.cleanup().await
}

#[tokio::test]
async fn aba_reorg_between_marker_selection_and_snapshot_load_is_transient() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_load_snapshot_aba").await?;
    let chain = "interpret-load-snapshot-aba";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    insert_orphaned_registration(scratch.pool(), chain, 1, "bob").await?;

    let (engine_pool, batch_acquire_reached, release_batch_acquire) =
        pool_with_acquire_gate(scratch.pool(), 3).await?;

    let mut raw_log_gate = scratch.pool().begin().await?;
    sqlx::query("LOCK TABLE raw_logs IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *raw_log_gate)
        .await?;
    let pool = engine_pool.clone();
    let chain_for_task = chain.to_owned();
    let running = tokio::spawn(async move {
        Engine::new(pool)
            .run_batch(BatchRequest {
                chain_id: chain_for_task,
                from_block: 0,
                to_block: 1,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await
    });

    batch_acquire_reached.notified().await;
    switch_live_lineage(scratch.pool(), chain, &format!("{chain}-orphan-1")).await?;
    release_batch_acquire.notify_one();

    wait_for_locked_query(scratch.pool(), "%FROM raw_logs raw%").await?;
    switch_live_lineage(scratch.pool(), chain, &block_hash(chain, 1)).await?;
    raw_log_gate.commit().await?;

    let error = running
        .await?
        .expect_err("an ABA lineage change must not mix selected and loaded blocks");
    assert_eq!(error.kind(), InterpretErrorKind::Transient);
    assert!(
        error
            .to_string()
            .contains("changed while loading raw facts")
    );
    let written: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(written, 0);
    engine_pool.close().await;
    scratch.cleanup().await
}

#[tokio::test]
async fn changed_resume_marker_before_snapshot_is_transient_without_writes() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_resume_snapshot_reorg").await?;
    let chain = "interpret-resume-snapshot-reorg";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "alice")]).await?;
    insert_orphaned_registration(scratch.pool(), chain, 1, "bob").await?;
    insert_orphaned_registration_with_parent(
        scratch.pool(),
        chain,
        2,
        "bob",
        &format!("{chain}-orphan-1"),
    )
    .await?;

    let (engine_pool, suffix_selection_reached, release_suffix_selection) =
        pool_with_acquire_gate(scratch.pool(), 3).await?;
    let pool = engine_pool.clone();
    let chain_for_task = chain.to_owned();
    let running = tokio::spawn(async move {
        Engine::new(pool)
            .run_batch(BatchRequest {
                chain_id: chain_for_task,
                from_block: 0,
                to_block: 2,
                resume_current: Some(Marker {
                    number: 1,
                    hash: block_hash(chain, 1),
                }),
                mode: InterpretRunMode::Normal,
            })
            .await
    });

    suffix_selection_reached.notified().await;
    let mut reorg = scratch.pool().begin().await?;
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE chain_id = $1 AND block_number BETWEEN 1 AND 2
        ",
    )
    .bind(chain)
    .execute(&mut *reorg)
    .await?;
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'canonical'::canonicality_state
        WHERE chain_id = $1
          AND block_hash IN ($2, $3)
        ",
    )
    .bind(chain)
    .bind(format!("{chain}-orphan-1"))
    .bind(format!("{chain}-orphan-2"))
    .execute(&mut *reorg)
    .await?;
    reorg.commit().await?;
    release_suffix_selection.notify_one();

    let error = running
        .await?
        .expect_err("a replaced resume block must abort the resumed suffix");
    assert_eq!(error.kind(), InterpretErrorKind::Transient);
    assert!(
        error
            .to_string()
            .contains("changed before the input snapshot")
    );
    let written: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(written, 0);
    engine_pool.close().await;
    scratch.cleanup().await
}

#[tokio::test]
async fn prior_state_ignores_events_whose_lineage_is_no_longer_live() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_prior_live_lineage").await?;
    let chain = "interpret-prior-live-lineage";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    insert_orphaned_registration(scratch.pool(), chain, 1, "alice").await?;
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = CASE
            WHEN block_hash = $2 THEN 'canonical'::canonicality_state
            ELSE 'orphaned'::canonicality_state
        END
        WHERE chain_id = $1 AND block_number = 1
        ",
    )
    .bind(chain)
    .bind(format!("{chain}-orphan-1"))
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 2, 2, InterpretRunMode::Normal).await?;
    let prior_registrant: Option<String> = sqlx::query_scalar(
        "
        SELECT before_state ->> 'registrant'
        FROM normalized_events
        WHERE chain_id = $1
          AND block_number = 2
          AND event_kind = 'RegistrationGranted'
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(prior_registrant, None);
    scratch.cleanup().await
}

#[tokio::test]
async fn orphaning_epoch_forces_full_cached_dependency_revalidation() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_prior_epoch").await?;
    let chain = "interpret-prior-epoch";
    seed_fixture(scratch.pool(), chain, &[(1, "alice"), (1001, "alice")]).await?;
    let stable_boundary = BlockMarker::new(999, block_hash(chain, 999))?;
    publish_heads(
        scratch.pool(),
        chain,
        &HeadMarkers {
            latest: BlockMarker::new(1001, block_hash(chain, 1001))?,
            safe: Some(stable_boundary.clone()),
            finalized: None,
        },
    )
    .await?;

    let engine = Engine::new(scratch.pool().clone());
    let first = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1001,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(!first.complete);
    assert_eq!(first.current.number, 499);

    let second = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1001,
            resume_current: Some(first.current),
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(!second.complete);
    assert_eq!(second.current.number, 999);

    let replacement_1000 = format!("{chain}-replacement-1000");
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp,
            canonicality_state
        )
        VALUES ($1, $2, $3, 1000, to_timestamp(1000), 'observed')
        ",
    )
    .bind(chain)
    .bind(&replacement_1000)
    .bind(&stable_boundary.hash)
    .execute(scratch.pool())
    .await?;
    insert_orphaned_registration_with_parent(
        scratch.pool(),
        chain,
        1001,
        "alice",
        &replacement_1000,
    )
    .await?;
    let replacement_1001 = format!("{chain}-orphan-1001");
    publish_heads(
        scratch.pool(),
        chain,
        &HeadMarkers {
            latest: BlockMarker::new(1001, &replacement_1001)?,
            safe: Some(stable_boundary.clone()),
            finalized: None,
        },
    )
    .await?;
    let epoch_after_orphaning: i64 =
        sqlx::query_scalar("SELECT lineage_orphaning_epoch FROM chain_heads WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(epoch_after_orphaning, 1);

    insert_orphaned_registration(scratch.pool(), chain, 1, "bob").await?;
    switch_live_lineage(scratch.pool(), chain, &format!("{chain}-orphan-1")).await?;

    let third = engine
        .run_batch(BatchRequest {
            chain_id: chain.to_owned(),
            from_block: 0,
            to_block: 1001,
            resume_current: Some(second.current),
            mode: InterpretRunMode::Normal,
        })
        .await?;
    assert!(third.complete);
    let prior_registrant: Option<String> = sqlx::query_scalar(
        "
        SELECT before_state ->> 'registrant'
        FROM normalized_events
        WHERE chain_id = $1
          AND block_hash = $2
          AND event_kind = 'RegistrationGranted'
        ",
    )
    .bind(chain)
    .bind(replacement_1001)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(prior_registrant, None);
    scratch.cleanup().await
}

#[tokio::test]
async fn orphaned_stable_identities_reanchor_to_the_winning_observation() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_identity_reanchor").await?;
    let chain = "interpret-identity-reanchor";
    seed_fixture(scratch.pool(), chain, &[(1, "bad\u{1}")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    sqlx::query(
        "
        UPDATE resources
        SET block_hash = $2,
            block_number = 0,
            provenance = '{\"branch\":\"losing\"}'::jsonb,
            canonicality_state = 'orphaned'
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 0))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        UPDATE token_lineages
        SET block_hash = $2,
            block_number = 0,
            provenance = '{\"branch\":\"losing\"}'::jsonb,
            canonicality_state = 'orphaned'
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 0))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        UPDATE name_surfaces
        SET block_hash = $2,
            block_number = 0,
            provenance = '{\"branch\":\"losing\"}'::jsonb,
            canonicality_state = 'orphaned',
            deactivated_at = to_timestamp(999)
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .bind(block_hash(chain, 0))
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let resource_anchor: (i64, bool, String) = sqlx::query_as(
        "
        SELECT block_number,
               provenance ->> 'branch' IS NULL,
               canonicality_state::text
        FROM resources
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let lineage_anchor: (i64, bool, String) = sqlx::query_as(
        "
        SELECT block_number,
               provenance ->> 'branch' IS NULL,
               canonicality_state::text
        FROM token_lineages
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(resource_anchor, (1, true, "canonical".into()));
    assert_eq!(lineage_anchor, (1, true, "canonical".into()));
    let surface_anchor: (i64, bool, String, time::OffsetDateTime) = sqlx::query_as(
        "
        SELECT block_number,
               provenance ->> 'branch' IS NULL,
               canonicality_state::text,
               deactivated_at
        FROM name_surfaces
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(surface_anchor.0, 1);
    assert!(surface_anchor.1);
    assert_eq!(surface_anchor.2, "canonical");
    assert_eq!(surface_anchor.3.unix_timestamp(), 1);
    scratch.cleanup().await
}

#[tokio::test]
async fn readable_closed_binding_is_not_reopened_by_idempotent_replay() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_closed_binding").await?;
    let chain = "interpret-closed-binding";
    seed_fixture(scratch.pool(), chain, &[(1, "alice")]).await?;
    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;
    sqlx::query(
        "
        UPDATE surface_bindings
        SET active_to = active_from + interval '1 second'
        WHERE chain_id = $1
        ",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    run_engine(scratch.pool(), chain, 0, 1, InterpretRunMode::Normal).await?;

    let remains_closed: bool = sqlx::query_scalar(
        "SELECT active_to IS NOT NULL FROM surface_bindings WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(remains_closed);
    scratch.cleanup().await
}

async fn run_engine(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    mode: InterpretRunMode,
) -> Result<()> {
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: chain_id.to_owned(),
            from_block,
            to_block,
            resume_current: None,
            mode,
        })
        .await?;
    assert!(outcome.complete);
    assert_eq!(outcome.current, outcome.target);
    Ok(())
}

async fn seed_pre_surface_pointer(pool: &PgPool) -> Result<()> {
    const CHAIN: &str = "ethereum-mainnet";
    const REGISTRY_ADDRESS: &str = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    const CONTROLLER: &str = "0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5";
    let profile = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    sync_schema_v2_repository(pool, &load_repository(profile)?).await?;
    sqlx::query(
        "WITH declarations AS (
           UPDATE manifest_contract_instances SET start_block_number = 0
           WHERE chain_id = $1 AND lower(declared_address) IN (lower($2), lower($3))
           RETURNING contract_instance_id
         )
         UPDATE contract_instance_addresses SET active_from_block_number = 0
         WHERE contract_instance_id IN (SELECT contract_instance_id FROM declarations)",
    )
    .bind(CHAIN)
    .bind(REGISTRY_ADDRESS)
    .bind(CONTROLLER)
    .execute(pool)
    .await?;
    sqlx::query(
        "WITH blocks(block_number, emitter) AS (VALUES (0::bigint, $2), (1, $2), (2, $3)),
         lineage AS (
           INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state)
           SELECT $1, $1 || '-block-' || block_number,
                  CASE WHEN block_number > 0 THEN $1 || '-block-' || (block_number - 1) END,
                  block_number, to_timestamp(block_number), 'canonical'::canonicality_state FROM blocks
           RETURNING block_number
         )
         INSERT INTO raw_transactions (chain_id, block_hash, block_number, transaction_hash, transaction_index, from_address, to_address)
         SELECT $1, $1 || '-block-' || blocks.block_number, blocks.block_number,
                $1 || '-pointer-transaction-' || blocks.block_number, 0, $4, emitter
         FROM blocks JOIN lineage USING (block_number)",
    )
    .bind(CHAIN)
    .bind(REGISTRY_ADDRESS)
    .bind(CONTROLLER)
    .bind(SENDER)
    .execute(pool)
    .await?;
    let node = raw_namehash(&[b"pointer", b"eth"]);
    let facts = [
        (
            REGISTRY_ADDRESS,
            NewResolver {
                node,
                resolver: DISCOVERED_RESOLVER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            REGISTRY_ADDRESS,
            Transfer {
                node,
                owner: REGISTRANT.parse()?,
            }
            .encode_log_data(),
        ),
        (
            CONTROLLER,
            NameRenewed {
                name: "pointer".into(),
                label: keccak256(b"pointer"),
                cost: U256::from(1),
                expires: U256::from(1_000_000),
            }
            .encode_log_data(),
        ),
    ];
    for (block, (emitter, fact)) in facts.into_iter().enumerate() {
        let block = i64::try_from(block)?;
        let transaction = format!("{CHAIN}-pointer-transaction-{block}");
        insert_log_at(
            pool,
            CHAIN,
            block,
            &transaction,
            0,
            emitter,
            fact.topics(),
            fact.data.as_ref(),
        )
        .await?;
    }
    Ok(())
}

async fn seed_old_registry_resolver_handoff(pool: &PgPool) -> Result<()> {
    const CHAIN: &str = "ethereum-mainnet";
    const REGISTRY_ADDRESS: &str = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    const OLD_REGISTRY_ADDRESS: &str = "0x314159265dd8dbb310642f98f50c066173c1259b";
    const CONTROLLER: &str = "0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5";
    const REGISTRAR: &str = "0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85";
    const OLD_RESOLVER: &str = "0xF29100983E058B709F3D539b0c765937B804AC15";
    let profile = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    sync_schema_v2_repository(pool, &load_repository(profile)?).await?;
    sqlx::query(
        "WITH declarations AS (
           UPDATE manifest_contract_instances SET start_block_number = 0
           WHERE chain_id = $1
             AND lower(declared_address) IN (lower($2), lower($3), lower($4), lower($5))
           RETURNING contract_instance_id
         )
         UPDATE contract_instance_addresses SET active_from_block_number = 0
         WHERE contract_instance_id IN (SELECT contract_instance_id FROM declarations)",
    )
    .bind(CHAIN)
    .bind(REGISTRY_ADDRESS)
    .bind(OLD_REGISTRY_ADDRESS)
    .bind(CONTROLLER)
    .bind(REGISTRAR)
    .execute(pool)
    .await?;
    sqlx::query(
        "WITH blocks(block_number, block_timestamp, emitter) AS (
           VALUES (0::bigint, 0::bigint, $2), (1, 1, $2), (2, 2, $3),
                  (3, 3, $4), (4, 4, $5), (5, 5, $5), (6, 7776101, $5)
         ), lineage AS (
           INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp,
             canonicality_state
           )
           SELECT $1, $1 || '-block-' || block_number,
                  CASE WHEN block_number > 0
                    THEN $1 || '-block-' || (block_number - 1)
                  END,
                  block_number, to_timestamp(block_timestamp),
                  'canonical'::canonicality_state
           FROM blocks RETURNING block_number
         )
         INSERT INTO raw_transactions (
           chain_id, block_hash, block_number, transaction_hash,
           transaction_index, from_address, to_address
         )
         SELECT $1, $1 || '-block-' || blocks.block_number,
                blocks.block_number,
                $1 || '-old-handoff-transaction-' || blocks.block_number,
                0, $6, emitter
         FROM blocks JOIN lineage USING (block_number)",
    )
    .bind(CHAIN)
    .bind(OLD_REGISTRY_ADDRESS)
    .bind(CONTROLLER)
    .bind(REGISTRAR)
    .bind(REGISTRY_ADDRESS)
    .bind(SENDER)
    .execute(pool)
    .await?;
    let node = raw_namehash(&[b"pointer", b"eth"]);
    let facts = vec![
        (
            0,
            0,
            OLD_REGISTRY_ADDRESS,
            NewOwner {
                node: raw_namehash(&[b"eth"]),
                label: keccak256(b"pointer"),
                owner: PRIOR_REGISTRY_OWNER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            1,
            0,
            OLD_REGISTRY_ADDRESS,
            NewResolver {
                node,
                resolver: OLD_RESOLVER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            2,
            0,
            CONTROLLER,
            NameRenewed {
                name: "pointer".into(),
                label: keccak256(b"pointer"),
                cost: U256::from(1),
                expires: U256::from(1_000_000),
            }
            .encode_log_data(),
        ),
        (
            3,
            0,
            REGISTRAR,
            base_registrar_events::Transfer {
                from: PRIOR_REGISTRY_OWNER.parse()?,
                to: PRIOR_REGISTRY_OWNER.parse()?,
                tokenId: U256::from_be_slice(keccak256(b"pointer").as_slice()),
            }
            .encode_log_data(),
        ),
        (
            4,
            0,
            REGISTRY_ADDRESS,
            NewOwner {
                node: raw_namehash(&[b"eth"]),
                label: keccak256(b"pointer"),
                owner: CONTROLLER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            4,
            1,
            REGISTRY_ADDRESS,
            NewResolver {
                node,
                resolver: Address::ZERO,
            }
            .encode_log_data(),
        ),
        (
            4,
            2,
            REGISTRY_ADDRESS,
            NewOwner {
                node: raw_namehash(&[b"eth"]),
                label: keccak256(b"pointer"),
                owner: REGISTRANT.parse()?,
            }
            .encode_log_data(),
        ),
        (
            4,
            3,
            CONTROLLER,
            legacy_registrar_controller_events::NameRegistered {
                name: "pointer".into(),
                label: keccak256(b"pointer"),
                owner: REGISTRANT.parse()?,
                cost: U256::from(1),
                expires: U256::from(100),
            }
            .encode_log_data(),
        ),
        (
            5,
            0,
            REGISTRY_ADDRESS,
            Transfer {
                node,
                owner: Address::ZERO,
            }
            .encode_log_data(),
        ),
        (
            6,
            0,
            REGISTRY_ADDRESS,
            Transfer {
                node: B256::ZERO,
                owner: REGISTRANT.parse()?,
            }
            .encode_log_data(),
        ),
    ];
    for (block, log_index, emitter, fact) in facts {
        insert_log_at(
            pool,
            CHAIN,
            block,
            &format!("{CHAIN}-old-handoff-transaction-{block}"),
            log_index,
            emitter,
            fact.topics(),
            fact.data.as_ref(),
        )
        .await?;
    }
    Ok(())
}

async fn seed_pre_surface_registry_fallback(pool: &PgPool) -> Result<()> {
    const CHAIN: &str = "ethereum-mainnet";
    const REGISTRY_ADDRESS: &str = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    const LEGACY_CONTROLLER: &str = "0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5";
    let profile = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    sync_schema_v2_repository(pool, &load_repository(profile)?).await?;
    sqlx::query(
        "WITH declarations AS (
           UPDATE manifest_contract_instances SET start_block_number = 0
           WHERE chain_id = $1 AND lower(declared_address) IN (lower($2), lower($3))
           RETURNING contract_instance_id
         )
         UPDATE contract_instance_addresses SET active_from_block_number = 0
         WHERE contract_instance_id IN (SELECT contract_instance_id FROM declarations)",
    )
    .bind(CHAIN)
    .bind(REGISTRY_ADDRESS)
    .bind(LEGACY_CONTROLLER)
    .execute(pool)
    .await?;
    sqlx::query(
        "WITH blocks(block_number, block_timestamp, emitter) AS (
           VALUES (0::bigint, 0::bigint, $2), (1, 1, $3), (2, 7776101, $2)
         ), lineage AS (
           INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp,
             canonicality_state
           )
           SELECT $1, $1 || '-block-' || block_number,
                  CASE WHEN block_number > 0
                    THEN $1 || '-block-' || (block_number - 1)
                  END,
                  block_number, to_timestamp(block_timestamp),
                  'canonical'::canonicality_state
           FROM blocks RETURNING block_number
         )
         INSERT INTO raw_transactions (
           chain_id, block_hash, block_number, transaction_hash,
           transaction_index, from_address, to_address
         )
         SELECT $1, $1 || '-block-' || blocks.block_number,
                blocks.block_number,
                $1 || '-fallback-transaction-' || blocks.block_number,
                0, $4, emitter
         FROM blocks JOIN lineage USING (block_number)",
    )
    .bind(CHAIN)
    .bind(REGISTRY_ADDRESS)
    .bind(LEGACY_CONTROLLER)
    .bind(SENDER)
    .execute(pool)
    .await?;
    let facts = [
        (
            REGISTRY_ADDRESS,
            NewOwner {
                node: raw_namehash(&[b"eth"]),
                label: keccak256(b"pointer"),
                owner: PRIOR_REGISTRY_OWNER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            LEGACY_CONTROLLER,
            legacy_registrar_controller_events::NameRegistered {
                name: "pointer".into(),
                label: keccak256(b"pointer"),
                owner: REGISTRANT.parse()?,
                cost: U256::from(1),
                expires: U256::from(100),
            }
            .encode_log_data(),
        ),
        (
            REGISTRY_ADDRESS,
            Transfer {
                node: B256::ZERO,
                owner: REGISTRANT.parse()?,
            }
            .encode_log_data(),
        ),
    ];
    for (block, (emitter, fact)) in facts.into_iter().enumerate() {
        let block = i64::try_from(block)?;
        insert_log_at(
            pool,
            CHAIN,
            block,
            &format!("{CHAIN}-fallback-transaction-{block}"),
            0,
            emitter,
            fact.topics(),
            fact.data.as_ref(),
        )
        .await?;
    }
    Ok(())
}

async fn live_pre_surface_registry_fallback(
    label: &str,
    split: bool,
) -> Result<(ScratchDatabase, Value)> {
    let scratch =
        ScratchDatabase::create(&format!("pre_surface_registry_fallback_{label}")).await?;
    seed_pre_surface_registry_fallback(scratch.pool()).await?;
    run_engine(
        scratch.pool(),
        "ethereum-mainnet",
        0,
        if split { 1 } else { 2 },
        InterpretRunMode::Normal,
    )
    .await?;
    if split {
        run_engine(
            scratch.pool(),
            "ethereum-mainnet",
            2,
            2,
            InterpretRunMode::Normal,
        )
        .await?;
    }
    complete_interpret_state(&scratch, 2).await?;
    let snapshot = materialization_snapshot(scratch.pool()).await?;
    Ok((scratch, snapshot))
}

async fn live_materialization(label: &str, split: bool) -> Result<(ScratchDatabase, Value)> {
    let scratch = ScratchDatabase::create(&format!("pre_surface_pointer_{label}")).await?;
    seed_pre_surface_pointer(scratch.pool()).await?;
    run_engine(
        scratch.pool(),
        "ethereum-mainnet",
        0,
        if split { 1 } else { 2 },
        InterpretRunMode::Normal,
    )
    .await?;
    if split {
        run_engine(
            scratch.pool(),
            "ethereum-mainnet",
            2,
            2,
            InterpretRunMode::Normal,
        )
        .await?;
    }
    complete_interpret_state(&scratch, 2).await?;
    let snapshot = materialization_snapshot(scratch.pool()).await?;
    Ok((scratch, snapshot))
}

async fn complete_interpret_state(scratch: &ScratchDatabase, head: i64) -> Result<()> {
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain("ethereum-mainnet")
        .await?;
    seed_completed_phase_extent_at(
        scratch.pool(),
        "ethereum-mainnet",
        head,
        INTERPRETER_CONTENT_HASH,
    )
    .await
}

async fn materialization_snapshot(pool: &PgPool) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
           'normalized_events', (SELECT jsonb_agg(to_jsonb(row) - ARRAY['normalized_event_id','observed_at'] ORDER BY event_identity) FROM normalized_events row WHERE chain_id = 'ethereum-mainnet'),
           'resources', (SELECT jsonb_agg(to_jsonb(row) - ARRAY['observed_at','inserted_at'] ORDER BY resource_id) FROM resources row WHERE chain_id = 'ethereum-mainnet'),
           'name_surfaces', (SELECT jsonb_agg(to_jsonb(row) - ARRAY['observed_at','inserted_at'] ORDER BY logical_name_id) FROM name_surfaces row WHERE chain_id = 'ethereum-mainnet'),
           'surface_bindings', (SELECT jsonb_agg(to_jsonb(row) - ARRAY['observed_at','inserted_at'] ORDER BY surface_binding_id) FROM surface_bindings row WHERE chain_id = 'ethereum-mainnet'),
           'phase_state', (SELECT jsonb_agg(jsonb_build_object('phase',phase_name,'status',phase_status,'current',current_block_number,'target',target_block_number,'hash',input_content_hash,'redo',redo_in_progress) ORDER BY phase_name) FROM chain_phase_state WHERE chain_id = 'ethereum-mainnet' AND phase_name IN ('ingest','interpret')),
           'project_inputs', (SELECT jsonb_agg(jsonb_build_object('event_identity',event_identity,'logical_name_id',logical_name_id,'resource_id',resource_id,'source_manifest_id',source_manifest_id,'after_state',after_state) ORDER BY event_identity) FROM normalized_events WHERE chain_id = 'ethereum-mainnet' AND event_kind = 'ResolverChanged'),
           'pointer_count', (SELECT count(*) FROM normalized_events WHERE chain_id = 'ethereum-mainnet' AND event_kind = 'ResolverChanged' AND after_state ->> 'state_derived' = 'true'),
           'binding_count', (SELECT count(*) FROM surface_bindings WHERE chain_id = 'ethereum-mainnet' AND active_to IS NULL)
         )",
    )
    .fetch_one(pool)
    .await?)
}

async fn raw_log_digest(pool: &PgPool) -> Result<(String, i64)> {
    Ok(sqlx::query_as(
        "SELECT md5(jsonb_agg(to_jsonb(row) ORDER BY block_number, transaction_index, log_index)::text), count(*) FROM raw_logs row WHERE chain_id = 'ethereum-mainnet'",
    )
    .fetch_one(pool)
    .await?)
}

async fn readable_v2_expiries(pool: &PgPool, chain_id: &str) -> Result<Vec<(String, i64, String)>> {
    Ok(sqlx::query_as(
        "SELECT event.event_kind, event.block_number, event.event_identity
         FROM normalized_events event JOIN chain_lineage lineage USING (chain_id, block_number, block_hash)
         WHERE event.chain_id = $1 AND event.after_state ->> 'source_event' = 'RegistryPathExpired'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         ORDER BY event.event_kind",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?)
}

async fn run_project(
    pool: &PgPool,
    chain_id: &str,
    target_block: i64,
    affected_from_block: i64,
    affected_to_block: i64,
) -> Result<()> {
    let outcome = ProjectEngine::new(pool.clone())
        .run_batch(ProjectBatchRequest {
            chain_id: chain_id.to_owned(),
            target_block,
            affected_from_block,
            affected_to_block,
            resume_current: None,
            mode: ProjectRunMode::Normal,
        })
        .await?;
    assert!(outcome.complete);
    assert_eq!(outcome.current, outcome.target);
    Ok(())
}

async fn seed_discovery_fixture(pool: &PgPool, chain_id: &str) -> Result<()> {
    for block in 0..=1 {
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
        .bind(block)
        .execute(pool)
        .await?;
    }
    let registry_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(registry_instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    let registry_payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_registry_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registry",
            "address": CONTRACT,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [{
            "edge_kind": "resolver",
            "from_role": "registry",
            "admission": "reachable_from_root"
        }],
        "abi": { "events": [{
            "name": "ResolverUpdated",
            "fragment": "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
            "emitter_roles": ["registry"],
            "normalized_events": ["ResolverChanged"]
        }], "calls": [] }
    });
    let resolver_payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_resolver_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": { "events": [{
            "name": "TextChanged",
            "fragment": "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            "emitter_roles": [],
            "normalized_events": ["RecordChanged"]
        }], "calls": [] }
    });
    let registry_manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v2_registry_l1",
        "tests/discovery-registry.toml",
        registry_payload,
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_discovery_rules (
            manifest_id, edge_kind, from_role, admission
        )
        VALUES ($1, 'resolver', 'registry', 'reachable_from_root')
        ",
    )
    .bind(registry_manifest_id)
    .execute(pool)
    .await?;
    insert_manifest(
        pool,
        chain_id,
        "ens_v2_resolver_l1",
        "tests/discovery-resolver.toml",
        resolver_payload,
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind, start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)
        ",
    )
    .bind(registry_manifest_id)
    .bind(chain_id)
    .bind(registry_instance_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address, active_from_block_number,
            source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
        ",
    )
    .bind(registry_instance_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(registry_manifest_id)
    .execute(pool)
    .await?;

    let transaction_hash = format!("{chain_id}-transaction-1");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash, transaction_index,
            from_address, to_address
        )
        VALUES ($1, $2, 1, $3, 0, $4, $5)
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 1))
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    let resolver_address = DISCOVERED_RESOLVER.parse::<Address>()?;
    let discovery = ResolverUpdated {
        tokenId: U256::from(7),
        resolver: resolver_address,
        sender: SENDER.parse::<Address>()?,
    }
    .encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        0,
        CONTRACT,
        discovery.topics(),
        discovery.data.as_ref(),
    )
    .await?;
    let record = TextChanged {
        node: B256::from(keccak256(b"alice.eth")),
        indexedKey: keccak256(b"url"),
        key: "url".into(),
        value: "https://example.test".into(),
    }
    .encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        1,
        DISCOVERED_RESOLVER,
        record.topics(),
        record.data.as_ref(),
    )
    .await
}

async fn seed_root_resolver_watch_fixture(pool: &PgPool, chain_id: &str) -> Result<()> {
    let root_id = Uuid::new_v4();
    for block in 0..=2 {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number, block_timestamp,
                 canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
        .bind(block)
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind, provenance
         ) VALUES ($1, $2, 'contract', '{}'::jsonb)",
    )
    .bind(root_id)
    .bind(chain_id)
    .execute(pool)
    .await?;
    let root_payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_root_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [{
            "name": "RootRegistry",
            "address": CONTRACT,
            "start_block": 0
        }],
        "contracts": [{
            "role": "root_registry",
            "address": CONTRACT,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [{
            "edge_kind": "resolver",
            "from_role": "root_registry",
            "admission": "reachable_from_root"
        }],
        "abi": { "events": [
            {
                "name": "LabelRegistered",
                "fragment": "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                "emitter_roles": ["root_registry"],
                "normalized_events": ["RegistrationGranted", "PreimageObserved"]
            },
            {
                "name": "TokenResource",
                "fragment": "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                "emitter_roles": ["root_registry"],
                "normalized_events": ["TokenResourceLinked"]
            },
            {
                "name": "ResolverUpdated",
                "fragment": "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
                "emitter_roles": ["root_registry"],
                "normalized_events": ["ResolverChanged"]
            }
        ], "calls": [] }
    });
    let resolver_payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_resolver_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "resolver_implementations": [{
            "role": "permissioned_resolver",
            "address": "0x0000000000000000000000000000000000000077"
        }],
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": { "events": [
            {
                "name": "Upgraded",
                "fragment": "event Upgraded(address indexed implementation)",
                "emitter_roles": [],
                "normalized_events": ["Upgraded"]
            },
            {
                "name": "TextChanged",
                "fragment": "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
                "emitter_roles": [],
                "normalized_events": ["RecordChanged"]
            }
        ], "calls": [] }
    });
    let root_manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v2_root_l1",
        "tests/root-resolver-root.toml",
        root_payload,
    )
    .await?;
    insert_manifest(
        pool,
        chain_id,
        "ens_v2_resolver_l1",
        "tests/root-resolver-target.toml",
        resolver_payload,
    )
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, raw_fact_ref,
             derivation_kind, canonicality_state, before_state, after_state
         )
         SELECT $1 || ':manifest:' || manifest.manifest_id::text,
                manifest.namespace, 'SourceManifestUpdated', manifest.source_family,
                manifest.manifest_version, manifest.manifest_id, manifest.chain_id,
                jsonb_build_object(
                    'manifest_id', manifest.manifest_id,
                    'namespace', manifest.namespace,
                    'source_family', manifest.source_family,
                    'chain', manifest.chain_id,
                    'deployment_epoch', manifest.deployment_label
                ), 'manifest_sync', 'finalized', '{}'::jsonb,
                jsonb_build_object(
                    'manifest_version', manifest.manifest_version,
                    'normalizer_version', manifest.normalizer_version,
                    'rollout_status', manifest.rollout_status,
                    'manifest_payload', manifest.manifest_payload
                )
         FROM manifest_versions manifest
         WHERE manifest.chain_id = $1",
    )
    .bind(chain_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'root_registry', $3, $4,
                'root_registry', 'none', 0)
        ",
    )
    .bind(root_manifest_id)
    .bind(chain_id)
    .bind(root_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_discovery_rules (
            manifest_id, edge_kind, from_role, admission
        )
        VALUES ($1, 'resolver', 'root_registry', 'reachable_from_root')
        ",
    )
    .bind(root_manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id, provenance
         ) VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(root_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(root_manifest_id)
    .execute(pool)
    .await?;

    for (block, to_address) in [(1, CONTRACT), (2, DISCOVERED_RESOLVER)] {
        sqlx::query(
            "INSERT INTO raw_transactions (
                 chain_id, block_hash, block_number, transaction_hash,
                 transaction_index, from_address, to_address
             ) VALUES ($1, $2, $3, $4, 0, $5, $6)",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind(block)
        .bind(format!("{chain_id}-transaction-{block}"))
        .bind(SENDER)
        .bind(to_address)
        .execute(pool)
        .await?;
    }

    let token_id = versioned_token("box", 1);
    let root_facts = [
        v2_registry_events::LabelRegistered {
            tokenId: token_id,
            labelHash: keccak256(b"box"),
            label: "box".to_owned(),
            owner: SENDER.parse()?,
            expiry: 100,
            sender: SENDER.parse()?,
        }
        .encode_log_data(),
        v2_registry_events::TokenResource {
            tokenId: token_id,
            resource: U256::from(374),
        }
        .encode_log_data(),
        ResolverUpdated {
            tokenId: token_id,
            resolver: DISCOVERED_RESOLVER.parse()?,
            sender: SENDER.parse()?,
        }
        .encode_log_data(),
    ];
    for (log_index, fact) in root_facts.into_iter().enumerate() {
        insert_log_at(
            pool,
            chain_id,
            1,
            &format!("{chain_id}-transaction-1"),
            i64::try_from(log_index)?,
            CONTRACT,
            fact.topics(),
            fact.data.as_ref(),
        )
        .await?;
    }

    let resolver_facts = [
        Upgraded {
            implementation: "0x0000000000000000000000000000000000000077".parse()?,
        }
        .encode_log_data(),
        TextChanged {
            node: raw_namehash(&[b"box"]),
            indexedKey: keccak256(b"url"),
            key: "url".to_owned(),
            value: "https://example.test".to_owned(),
        }
        .encode_log_data(),
    ];
    for (log_index, fact) in resolver_facts.into_iter().enumerate() {
        insert_log_at(
            pool,
            chain_id,
            2,
            &format!("{chain_id}-transaction-2"),
            i64::try_from(log_index)?,
            DISCOVERED_RESOLVER,
            fact.topics(),
            fact.data.as_ref(),
        )
        .await?;
    }
    Ok(())
}

async fn seed_sibling_discovery_fixture(pool: &PgPool, chain_id: &str) -> Result<()> {
    for block in 0..=2 {
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
        .bind(block)
        .execute(pool)
        .await?;
    }
    let registry_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(registry_instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_registry_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registry",
            "address": CONTRACT,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [{
            "edge_kind": "resolver",
            "from_role": "registry",
            "admission": "reachable_from_root"
        }],
        "abi": { "events": [{
            "name": "ResolverUpdated",
            "fragment": "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
            "emitter_roles": ["registry"],
            "normalized_events": ["ResolverChanged"]
        }], "calls": [] }
    });
    let manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v2_registry_l1",
        "tests/discovery-siblings.toml",
        payload,
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_discovery_rules (
            manifest_id, edge_kind, from_role, admission
        )
        VALUES ($1, 'resolver', 'registry', 'reachable_from_root')
        ",
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(registry_instance_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address,
            active_from_block_number, source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
        ",
    )
    .bind(registry_instance_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;

    for block in 1..=2 {
        sqlx::query(
            "
            INSERT INTO raw_transactions (
                chain_id, block_hash, block_number, transaction_hash,
                transaction_index, from_address, to_address
            )
            VALUES ($1, $2, $3, $4, 0, $5, $6)
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind(block)
        .bind(format!("{chain_id}-transaction-{block}"))
        .bind(SENDER)
        .bind(CONTRACT)
        .execute(pool)
        .await?;
    }
    let token_a = versioned_token("alice", 1);
    let token_b = versioned_token("bob", 1);
    for (block, log_index, token_id, target) in [
        (1, 0, token_a, "0x0000000000000000000000000000000000000051"),
        (1, 1, token_b, "0x0000000000000000000000000000000000000052"),
        (
            2,
            0,
            versioned_token("alice", 2),
            "0x0000000000000000000000000000000000000053",
        ),
    ] {
        let event = ResolverUpdated {
            tokenId: token_id,
            resolver: target.parse()?,
            sender: SENDER.parse()?,
        }
        .encode_log_data();
        insert_log_at(
            pool,
            chain_id,
            block,
            &format!("{chain_id}-transaction-{block}"),
            log_index,
            CONTRACT,
            event.topics(),
            event.data.as_ref(),
        )
        .await?;
    }
    Ok(())
}

async fn seed_v2_lifecycle_fixture(pool: &PgPool, chain_id: &str) -> Result<()> {
    for block in 0..=2 {
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
        .bind(block)
        .execute(pool)
        .await?;
    }
    let registry_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(registry_instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_registry_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registry",
            "address": CONTRACT,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": { "events": [
            {
                "name": "LabelRegistered",
                "fragment": "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                "emitter_roles": ["registry"],
                "normalized_events": ["RegistrationGranted"]
            },
            {
                "name": "TokenResource",
                "fragment": "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                "emitter_roles": ["registry"],
                "normalized_events": ["TokenResourceLinked"]
            },
            // (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L56-L60 @ ens_v2@a971bd64)
            { "name": "SubregistryUpdated", "fragment": "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)", "emitter_roles": ["registry"], "normalized_events": ["SubregistryChanged"] },
            // (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L88 @ ens_v2@a971bd64)
            { "name": "ParentUpdated", "fragment": "event ParentUpdated(address indexed parent, string label, address indexed sender)", "emitter_roles": ["registry"], "normalized_events": ["ParentChanged"] },
            {
                "name": "EACRolesChanged",
                "fragment": "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                "emitter_roles": ["registry"],
                "normalized_events": ["PermissionChanged", "RootPermissionChanged"]
            },
            {
                "name": "TransferSingle",
                "fragment": "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                "emitter_roles": ["registry"],
                "normalized_events": ["TokenControlTransferred"]
            },
            {
                "name": "LabelUnregistered",
                "fragment": "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
                "emitter_roles": ["registry"],
                "normalized_events": ["RegistrationReleased"]
            }
        ], "calls": [] }
    });
    let manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v2_registry_l1",
        "tests/v2-lifecycle.toml",
        payload,
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(registry_instance_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address,
            active_from_block_number, source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
        ",
    )
    .bind(registry_instance_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    for block in 1..=2 {
        sqlx::query(
            "
            INSERT INTO raw_transactions (
                chain_id, block_hash, block_number, transaction_hash,
                transaction_index, from_address, to_address
            )
            VALUES ($1, $2, $3, $4, 0, $5, $6)
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind(block)
        .bind(format!("{chain_id}-transaction-{block}"))
        .bind(SENDER)
        .bind(CONTRACT)
        .execute(pool)
        .await?;
    }
    let label = "alice";
    let token_id = versioned_token(label, 1);
    let resource_id = U256::from(5001);
    let owner: Address = "0x0000000000000000000000000000000000000061".parse()?;
    let recipient: Address = "0x0000000000000000000000000000000000000062".parse()?;
    let block_one = format!("{chain_id}-transaction-1");
    let facts = [
        v2_registry_events::LabelRegistered {
            tokenId: token_id,
            labelHash: keccak256(label.as_bytes()),
            label: label.to_owned(),
            owner,
            expiry: 100,
            sender: SENDER.parse()?,
        }
        .encode_log_data(),
        v2_registry_events::TokenResource {
            tokenId: token_id,
            resource: resource_id,
        }
        .encode_log_data(),
        v2_registry_events::EACRolesChanged {
            resource: resource_id,
            account: owner,
            oldRoleBitmap: U256::ZERO,
            newRoleBitmap: U256::from(1),
        }
        .encode_log_data(),
        v2_registry_events::TransferSingle {
            operator: owner,
            from: owner,
            to: recipient,
            id: token_id,
            value: U256::from(1),
        }
        .encode_log_data(),
    ];
    for (log_index, fact) in facts.into_iter().enumerate() {
        insert_log_at(
            pool,
            chain_id,
            1,
            &block_one,
            i64::try_from(log_index)?,
            CONTRACT,
            fact.topics(),
            fact.data.as_ref(),
        )
        .await?;
    }
    let release = v2_registry_events::LabelUnregistered {
        tokenId: token_id,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_log_at(
        pool,
        chain_id,
        2,
        &format!("{chain_id}-transaction-2"),
        0,
        CONTRACT,
        release.topics(),
        release.data.as_ref(),
    )
    .await
}

async fn seed_contested_v2_departure_fixture(
    pool: &PgPool,
    chain_id: &str,
    boundary_expiry: bool,
) -> Result<()> {
    if boundary_expiry {
        return seed_contested_v2_expiry_fixture(pool, chain_id).await;
    }
    seed_v2_lifecycle_fixture(pool, chain_id).await?;
    let manifest_id: i64 =
        sqlx::query_scalar("SELECT manifest_id FROM manifest_versions WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(pool)
            .await?;
    let rival_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(rival_instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind,
             start_block_number
         ) VALUES ($1, $2, 'contract', 'rival_registry', $3, $4, 'registry', 'none', 0)",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(rival_instance_id)
    .bind(RIVAL_REGISTRY)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address,
             active_from_block_number, source_manifest_id, provenance
         ) VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(rival_instance_id)
    .bind(chain_id)
    .bind(RIVAL_REGISTRY)
    .bind(manifest_id)
    .execute(pool)
    .await?;

    sqlx::query("DELETE FROM raw_logs WHERE chain_id = $1 AND block_number = 2")
        .bind(chain_id)
        .execute(pool)
        .await?;
    sqlx::query(
        "UPDATE raw_transactions SET to_address = $2
         WHERE chain_id = $1 AND block_number = 2",
    )
    .bind(chain_id)
    .bind(RIVAL_REGISTRY)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp,
             canonicality_state
         ) VALUES ($1, $2, $3, 3, to_timestamp(3), 'canonical')",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 3))
    .bind(block_hash(chain_id, 2))
    .execute(pool)
    .await?;

    let token_id = versioned_token("alice", 1);
    let owner: Address = "0x0000000000000000000000000000000000000061".parse()?;
    let rival_facts = [
        v2_registry_events::LabelRegistered {
            tokenId: token_id,
            labelHash: keccak256(b"alice"),
            label: "alice".to_owned(),
            owner,
            expiry: 100,
            sender: SENDER.parse()?,
        }
        .encode_log_data(),
        v2_registry_events::TokenResource {
            tokenId: token_id,
            resource: U256::from(6001),
        }
        .encode_log_data(),
    ];
    for (log_index, fact) in rival_facts.into_iter().enumerate() {
        insert_log_at(
            pool,
            chain_id,
            2,
            &format!("{chain_id}-transaction-2"),
            i64::try_from(log_index)?,
            RIVAL_REGISTRY,
            fact.topics(),
            fact.data.as_ref(),
        )
        .await?;
    }

    let transaction_hash = format!("{chain_id}-transaction-3");
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, from_address, to_address
         ) VALUES ($1, $2, 3, $3, 0, $4, $5)",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 3))
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    let release = v2_registry_events::LabelUnregistered {
        tokenId: token_id,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_log_at(
        pool,
        chain_id,
        3,
        &transaction_hash,
        0,
        CONTRACT,
        release.topics(),
        release.data.as_ref(),
    )
    .await?;
    Ok(())
}

async fn seed_contested_v2_expiry_fixture(pool: &PgPool, chain_id: &str) -> Result<()> {
    const CLAIM_ROOT: &str = "0x0000000000000000000000000000000000000058";
    const CLAIM_REGISTRY: &str = "0x0000000000000000000000000000000000000059";
    seed_v2_lifecycle_fixture(pool, chain_id).await?;
    sqlx::query("DELETE FROM raw_logs WHERE chain_id = $1")
        .bind(chain_id)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM raw_transactions WHERE chain_id = $1")
        .bind(chain_id)
        .execute(pool)
        .await?;
    for block in 3..=6 {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number, block_timestamp,
                 canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind(block_hash(chain_id, block - 1))
        .bind(block)
        .execute(pool)
        .await?;
    }
    let registry_manifest_id: i64 =
        sqlx::query_scalar("SELECT manifest_id FROM manifest_versions WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(pool)
            .await?;
    let mut root_payload: serde_json::Value =
        sqlx::query_scalar("SELECT manifest_payload FROM manifest_versions WHERE manifest_id = $1")
            .bind(registry_manifest_id)
            .fetch_one(pool)
            .await?;
    root_payload["source_family"] = json!("ens_v2_root_l1");
    root_payload["contracts"] = json!([{
        "role":"registry",
        "address":CLAIM_ROOT,
        "proxy_kind":"none",
        "implementation":null,
        "start_block":0,
    }]);
    let root_manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v2_root_l1",
        "tests/v2-contested-root.toml",
        root_payload,
    )
    .await?;
    sqlx::query(
        "INSERT INTO manifest_discovery_rules (
             manifest_id, edge_kind, from_role, admission
         ) VALUES ($1, 'subregistry', 'registry', 'linked_subregistry_event')",
    )
    .bind(root_manifest_id)
    .execute(pool)
    .await?;
    let declarations = [
        (registry_manifest_id, "survivor_registry", RIVAL_REGISTRY),
        (root_manifest_id, "claim_root", CLAIM_ROOT),
    ];
    for (source_manifest_id, declaration_name, address) in declarations {
        let instance_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())",
        )
        .bind(instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO manifest_contract_instances (
                 manifest_id, chain_id, declaration_kind, declaration_name,
                 contract_instance_id, declared_address, role, proxy_kind,
                 start_block_number
             ) VALUES ($1, $2, 'contract', $3, $4, $5, 'registry', 'none', 0)",
        )
        .bind(source_manifest_id)
        .bind(chain_id)
        .bind(declaration_name)
        .bind(instance_id)
        .bind(address)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO contract_instance_addresses (
                 contract_instance_id, chain_id, address,
                 active_from_block_number, source_manifest_id, provenance
             ) VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
        )
        .bind(instance_id)
        .bind(chain_id)
        .bind(address)
        .bind(source_manifest_id)
        .execute(pool)
        .await?;
    }
    let claim_root_instance_id: Uuid = sqlx::query_scalar(
        "SELECT contract_instance_id
         FROM manifest_contract_instances
         WHERE manifest_id = $1 AND declaration_name = 'claim_root'",
    )
    .bind(root_manifest_id)
    .fetch_one(pool)
    .await?;
    let claim_registry_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(claim_registry_instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address,
             active_from_block_number, source_manifest_id, provenance
         ) VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(claim_registry_instance_id)
    .bind(chain_id)
    .bind(CLAIM_REGISTRY)
    .bind(registry_manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO discovery_edges (
             chain_id, edge_kind, from_contract_instance_id,
             to_contract_instance_id, discovery_source, admission_basis,
             source_manifest_id, active_from_block_number,
             active_from_block_hash, canonicality_state, provenance
         ) VALUES (
             $1, 'registry_announcement', $2, $3, 'RegistryCreated',
             'reachable_from_root', $4, 0, $5, 'canonical',
             '{\"observation_key\":\"contested-claim-registry\"}'::jsonb
         )",
    )
    .bind(chain_id)
    .bind(claim_root_instance_id)
    .bind(claim_registry_instance_id)
    .bind(registry_manifest_id)
    .bind(block_hash(chain_id, 0))
    .execute(pool)
    .await?;
    let transactions = [
        (1, RIVAL_REGISTRY),
        (2, CLAIM_ROOT),
        (3, CLAIM_REGISTRY),
        (4, CLAIM_ROOT),
        (5, CLAIM_REGISTRY),
    ];
    for (block, to_address) in transactions {
        sqlx::query(
            "INSERT INTO raw_transactions (
                 chain_id, block_hash, block_number, transaction_hash,
                 transaction_index, from_address, to_address
             ) VALUES ($1, $2, $3, $4, 0, $5, $6)",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind(block)
        .bind(format!("{chain_id}-transaction-{block}"))
        .bind(SENDER)
        .bind(to_address)
        .execute(pool)
        .await?;
    }

    let owner: Address = "0x0000000000000000000000000000000000000061".parse()?;
    let alpha = versioned_token("alpha", 1);
    let eth = versioned_token("eth", 1);
    // The claim-path registration deliberately has no linked resource, so the fixture keeps the
    // resource-linked survivor current. Crossing the stored parent-expiry boundary closes the
    // fixture's ENSv2 bindings for the name and must reassert that survivor.
    let logs = [
        (
            1,
            0,
            RIVAL_REGISTRY,
            v2_registry_events::LabelRegistered {
                tokenId: alpha,
                labelHash: keccak256(b"alpha"),
                label: "alpha".to_owned(),
                owner,
                expiry: 100,
                sender: SENDER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            1,
            1,
            RIVAL_REGISTRY,
            v2_registry_events::TokenResource {
                tokenId: alpha,
                resource: U256::from(7001),
            }
            .encode_log_data(),
        ),
        (
            2,
            0,
            CLAIM_ROOT,
            v2_registry_events::LabelRegistered {
                tokenId: eth,
                labelHash: keccak256(b"eth"),
                label: "eth".to_owned(),
                owner,
                expiry: 6,
                sender: SENDER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            3,
            0,
            CLAIM_REGISTRY,
            v2_registry_events::LabelRegistered {
                tokenId: alpha,
                labelHash: keccak256(b"alpha"),
                label: "alpha".to_owned(),
                owner,
                expiry: 100,
                sender: SENDER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            4,
            0,
            CLAIM_ROOT,
            v2_registry_events::SubregistryUpdated {
                tokenId: eth,
                subregistry: CLAIM_REGISTRY.parse()?,
                sender: SENDER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            5,
            0,
            CLAIM_REGISTRY,
            v2_registry_events::ParentUpdated {
                parent: CLAIM_ROOT.parse()?,
                label: "eth".to_owned(),
                sender: SENDER.parse()?,
            }
            .encode_log_data(),
        ),
    ];
    for (block, log_index, emitter, log) in logs {
        insert_log_at(
            pool,
            chain_id,
            block,
            &format!("{chain_id}-transaction-{block}"),
            log_index,
            emitter,
            log.topics(),
            log.data.as_ref(),
        )
        .await?;
    }
    Ok(())
}

async fn replace_departure_block(
    pool: &PgPool,
    chain_id: &str,
    departure_block: i64,
    reapply: bool,
) -> Result<()> {
    let replacement_hash = format!("{chain_id}-replacement-{departure_block}");
    let mut transaction = pool.begin().await?;
    if reapply {
        sqlx::query(
            "UPDATE chain_lineage
             SET canonicality_state = 'orphaned'
             WHERE chain_id = $1 AND block_hash = $2 AND block_number = $3",
        )
        .bind(chain_id)
        .bind(&replacement_hash)
        .bind(departure_block)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE chain_lineage
             SET canonicality_state = 'canonical'
             WHERE chain_id = $1 AND block_hash = $2 AND block_number = $3",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, departure_block))
        .bind(departure_block)
        .execute(&mut *transaction)
        .await?;
    } else {
        sqlx::query(
            "UPDATE chain_lineage
             SET canonicality_state = 'orphaned'
             WHERE chain_id = $1 AND block_number = $2",
        )
        .bind(chain_id)
        .bind(departure_block)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number, block_timestamp,
                 canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
        )
        .bind(chain_id)
        .bind(&replacement_hash)
        .bind(block_hash(chain_id, departure_block - 1))
        .bind(departure_block)
        .bind(departure_block - 1)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn seed_announcement_fixture(pool: &PgPool, chain_id: &str) -> Result<()> {
    for block in 0..=1 {
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
        .bind(block)
        .execute(pool)
        .await?;
    }
    let anchor_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(anchor_instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_registry_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registry",
            "address": CONTRACT,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [{
            "edge_kind": "registry_announcement",
            "from_role": "registry",
            "admission": "reachable_from_root"
        }],
        "abi": { "events": [
            {
                "name": "RegistryCreated",
                "fragment": "event RegistryCreated()",
                "emitter_roles": [],
                "normalized_events": ["RegistryCreated"]
            },
            {
                "name": "LabelRegistered",
                "fragment": "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                "emitter_roles": ["registry"],
                "normalized_events": ["RegistrationGranted"]
            }
        ], "calls": [] }
    });
    let manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v2_registry_l1",
        "tests/announcement-registry.toml",
        payload,
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(anchor_instance_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address,
            active_from_block_number, source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
        ",
    )
    .bind(anchor_instance_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_discovery_rules (
            manifest_id, edge_kind, from_role, admission
        )
        VALUES ($1, 'registry_announcement', 'registry', 'reachable_from_root')
        ",
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;

    let transaction_hash = format!("{chain_id}-transaction-1");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash,
            transaction_index, from_address, to_address
        )
        VALUES ($1, $2, 1, $3, 0, $4, $5)
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 1))
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(ANNOUNCED_REGISTRY)
    .execute(pool)
    .await?;
    let announcement = RegistryCreated {}.encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        0,
        ANNOUNCED_REGISTRY,
        announcement.topics(),
        announcement.data.as_ref(),
    )
    .await
}

async fn seed_hostile_resolver_strings(pool: &PgPool, chain_id: &str) -> Result<()> {
    for block in 0..=1 {
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
        .bind(block)
        .execute(pool)
        .await?;
    }
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_resolver_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": { "events": [
            {
                "name": "TextChanged",
                "fragment": "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
                "emitter_roles": [],
                "normalized_events": ["RecordChanged"]
            },
            {
                "name": "NameChanged",
                "fragment": "event NameChanged(bytes32 indexed node, string name)",
                "emitter_roles": [],
                "normalized_events": ["RecordChanged"]
            }
        ], "calls": [] }
    });
    insert_manifest(
        pool,
        chain_id,
        "ens_v1_resolver_l1",
        "tests/hostile-resolver-strings.toml",
        payload,
    )
    .await?;
    let transaction_hash = format!("{chain_id}-transaction-1");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash,
            transaction_index, from_address, to_address
        )
        VALUES ($1, $2, 1, $3, 0, $4, $5)
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 1))
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    let node = B256::repeat_byte(0x31);
    let logs = vec![
        with_topic0(
            raw_string_events::RawTextChanged {
                node,
                indexedKey: keccak256(b"url"),
                key: b"url".to_vec().into(),
                value: b"a\0b".to_vec().into(),
            }
            .encode_log_data(),
            TextChanged::SIGNATURE_HASH,
        ),
        with_topic0(
            raw_string_events::RawTextChanged {
                node,
                indexedKey: keccak256(b"x\0y"),
                key: b"x\0y".to_vec().into(),
                value: b"ok".to_vec().into(),
            }
            .encode_log_data(),
            TextChanged::SIGNATURE_HASH,
        ),
        with_topic0(
            raw_string_events::RawNameChanged {
                node,
                name: b"bad\0name".to_vec().into(),
            }
            .encode_log_data(),
            NameChanged::SIGNATURE_HASH,
        ),
        with_topic0(
            raw_string_events::RawNameChanged {
                node,
                name: vec![0xff, 0xfe].into(),
            }
            .encode_log_data(),
            NameChanged::SIGNATURE_HASH,
        ),
    ];
    for (log_index, encoded) in logs.into_iter().enumerate() {
        insert_log(
            pool,
            chain_id,
            &transaction_hash,
            i64::try_from(log_index)?,
            CONTRACT,
            encoded.topics(),
            encoded.data.as_ref(),
        )
        .await?;
    }
    Ok(())
}

fn with_topic0(mut encoded: alloy_primitives::LogData, topic0: B256) -> alloy_primitives::LogData {
    encoded.topics_mut()[0] = topic0;
    encoded
}

async fn seed_hostile_wrapper_fixture(
    pool: &PgPool,
    chain_id: &str,
    raw_label: &[u8],
) -> Result<()> {
    seed_fixture(pool, chain_id, &[]).await?;
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp,
            canonicality_state
        )
        VALUES ($1, $2, $3, 1, to_timestamp(1), 'canonical')
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 1))
    .bind(block_hash(chain_id, 0))
    .execute(pool)
    .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_wrapper_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "name_wrapper",
            "address": ANNOUNCED_REGISTRY,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": { "events": [{
            "name": "NameWrapped",
            "fragment": "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
            "emitter_roles": ["name_wrapper"],
            "normalized_events": [
                "TokenControlTransferred",
                "ExpiryChanged",
                "PermissionScopeChanged",
                "SurfaceBound",
                "AuthorityEpochChanged",
                "PreimageObserved"
            ]
        }], "calls": [] }
    });
    let manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v1_wrapper_l1",
        "tests/hostile-wrapper.toml",
        payload,
    )
    .await?;
    let instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'name_wrapper', $3, $4,
                'name_wrapper', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(instance_id)
    .bind(ANNOUNCED_REGISTRY)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address,
            active_from_block_number, source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
        ",
    )
    .bind(instance_id)
    .bind(chain_id)
    .bind(ANNOUNCED_REGISTRY)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    let mut dns_name = Vec::with_capacity(raw_label.len() + 6);
    dns_name.push(u8::try_from(raw_label.len())?);
    dns_name.extend_from_slice(raw_label);
    dns_name.extend_from_slice(b"\x03eth\0");
    let node = raw_namehash(&[raw_label, b"eth"]);
    let encoded = NameWrapped {
        node,
        name: dns_name.into(),
        owner: CONTRACT.parse()?,
        fuses: 1,
        expiry: 42,
    }
    .encode_log_data();
    let transaction_hash = format!("{chain_id}-transaction-1");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash,
            transaction_index, from_address, to_address
        )
        VALUES ($1, $2, 1, $3, 0, $4, $5)
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 1))
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(ANNOUNCED_REGISTRY)
    .execute(pool)
    .await?;
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        0,
        ANNOUNCED_REGISTRY,
        encoded.topics(),
        encoded.data.as_ref(),
    )
    .await
}

fn raw_namehash(labels: &[&[u8]]) -> B256 {
    labels.iter().rev().fold(B256::ZERO, |node, label| {
        let mut input = [0_u8; 64];
        input[..32].copy_from_slice(node.as_slice());
        input[32..].copy_from_slice(keccak256(label).as_slice());
        B256::from(keccak256(input))
    })
}

async fn insert_manifest(
    pool: &PgPool,
    chain_id: &str,
    source_family: &str,
    file_path: &str,
    payload: serde_json::Value,
) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "
        INSERT INTO manifest_versions (
            manifest_version, namespace, source_family, chain_id, deployment_label,
            rollout_status, normalizer_version, file_path, manifest_payload
        )
        VALUES (1, 'ens', $2, $1, 'fixture', 'active', $3, $4, $5)
        RETURNING manifest_id
        ",
    )
    .bind(chain_id)
    .bind(source_family)
    .bind(NORMALIZER)
    .bind(file_path)
    .bind(payload)
    .fetch_one(pool)
    .await?)
}

async fn insert_log(
    pool: &PgPool,
    chain_id: &str,
    transaction_hash: &str,
    log_index: i64,
    emitting_address: &str,
    topics: &[B256],
    data: &[u8],
) -> Result<()> {
    insert_log_at(
        pool,
        chain_id,
        1,
        transaction_hash,
        log_index,
        emitting_address,
        topics,
        data,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_log_at(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
    transaction_hash: &str,
    log_index: i64,
    emitting_address: &str,
    topics: &[B256],
    data: &[u8],
) -> Result<()> {
    let topics = topics
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    sqlx::query(
        "
        INSERT INTO raw_logs (
            chain_id, block_hash, block_number, transaction_hash, transaction_index,
            log_index, emitting_address, topics, data
        )
        VALUES ($1, $2, $3, $4, 0, $5, $6, $7, $8)
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, block_number))
    .bind(block_number)
    .bind(transaction_hash)
    .bind(log_index)
    .bind(emitting_address)
    .bind(topics)
    .bind(data)
    .execute(pool)
    .await?;
    Ok(())
}

fn versioned_token(label: &str, version: u32) -> U256 {
    let mut bytes = *keccak256(label.as_bytes());
    bytes[28..].copy_from_slice(&version.to_be_bytes());
    U256::from_be_bytes(bytes)
}

async fn insert_orphaned_registration(
    pool: &PgPool,
    chain_id: &str,
    block: i64,
    label: &str,
) -> Result<()> {
    insert_orphaned_registration_with_parent(
        pool,
        chain_id,
        block,
        label,
        &block_hash(chain_id, block - 1),
    )
    .await
}

async fn insert_orphaned_registration_with_parent(
    pool: &PgPool,
    chain_id: &str,
    block: i64,
    label: &str,
    parent_hash: &str,
) -> Result<()> {
    let orphan_hash = format!("{chain_id}-orphan-{block}");
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, to_timestamp($4), 'orphaned')
        ",
    )
    .bind(chain_id)
    .bind(&orphan_hash)
    .bind(parent_hash)
    .bind(block)
    .execute(pool)
    .await?;
    let transaction_hash = format!("{chain_id}-orphan-transaction-{block}");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash, transaction_index,
            from_address, to_address
        )
        VALUES ($1, $2, $3, $4, 0, $5, $6)
        ",
    )
    .bind(chain_id)
    .bind(&orphan_hash)
    .bind(block)
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    let encoded = NameRegistered {
        name: label.to_owned(),
        label: B256::from(keccak256(label.as_bytes())),
        owner: CONTRACT.parse::<Address>()?,
        expires: U256::from(1_000_000u64),
    }
    .encode_log_data();
    let topics = encoded
        .topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    sqlx::query(
        "
        INSERT INTO raw_logs (
            chain_id, block_hash, block_number, transaction_hash, transaction_index,
            log_index, emitting_address, topics, data
        )
        VALUES ($1, $2, $3, $4, 0, 0, $5, $6, $7)
        ",
    )
    .bind(chain_id)
    .bind(orphan_hash)
    .bind(block)
    .bind(transaction_hash)
    .bind(CONTRACT)
    .bind(topics)
    .bind(encoded.data.to_vec())
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_fixture(pool: &PgPool, chain_id: &str, labels: &[(i64, &str)]) -> Result<()> {
    seed_fixture_with_timestamps(pool, chain_id, labels, &[]).await
}

async fn seed_same_transaction_registration_fixture(pool: &PgPool, chain_id: &str) -> Result<()> {
    seed_fixture(pool, chain_id, &[(1, "alice")]).await?;
    sqlx::query("UPDATE raw_logs SET log_index = 1 WHERE chain_id = $1")
        .bind(chain_id)
        .execute(pool)
        .await?;

    let contract_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(contract_instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_registry_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registry",
            "address": REGISTRY,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": { "events": [{
            "name": "NewOwner",
            "fragment": "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
            "emitter_roles": ["registry"],
            "normalized_events": [
                "SubregistryChanged",
                "AuthorityTransferred",
                "PermissionChanged",
                "SurfaceUnbound",
                "SurfaceBound",
                "AuthorityEpochChanged",
                "ResolverChanged"
            ]
        }], "calls": [] }
    });
    let manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v1_registry_l1",
        &format!("tests/{chain_id}-registry.toml"),
        payload,
    )
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind, start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(contract_instance_id)
    .bind(REGISTRY)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address, active_from_block_number,
            source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
        ",
    )
    .bind(contract_instance_id)
    .bind(chain_id)
    .bind(REGISTRY)
    .bind(manifest_id)
    .execute(pool)
    .await?;

    // Registrar registration calls the registry before emitting NameRegistered, and that registry
    // call emits NewOwner for the registered subnode.
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L149 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L152 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L82 @ ens_v1@91c966f)
    let transaction_hash = format!("{chain_id}-transaction-1");
    let encoded = NewOwner {
        node: raw_namehash(&[b"eth"]),
        label: B256::from(keccak256(b"alice")),
        owner: CONTRACT.parse()?,
    }
    .encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        0,
        REGISTRY,
        encoded.topics(),
        encoded.data.as_ref(),
    )
    .await
}

async fn seed_legacy_register_with_config_fixture(pool: &PgPool, chain_id: &str) -> Result<B256> {
    seed_same_transaction_registration_fixture(pool, chain_id).await?;

    let resolver_payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_resolver_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": { "events": [{
            "name": "AddrChanged",
            "fragment": "event AddrChanged(bytes32 indexed node, address a)",
            "emitter_roles": [],
            "normalized_events": ["RecordChanged"]
        }], "calls": [] }
    });
    insert_manifest(
        pool,
        chain_id,
        "ens_v1_resolver_l1",
        &format!("tests/{chain_id}-resolver.toml"),
        resolver_payload,
    )
    .await?;

    let label = "alice";
    let labelhash = B256::from(keccak256(label.as_bytes()));
    let node = raw_namehash(&[label.as_bytes(), b"eth"]);
    let registration = NameRegistered {
        name: label.to_owned(),
        label: labelhash,
        owner: REGISTRANT.parse()?,
        expires: U256::from(1_000_001u64),
    }
    .encode_log_data();
    let registration_topics = registration
        .topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    sqlx::query(
        "UPDATE raw_logs
         SET log_index = 3, topics = $3, data = $4
         WHERE chain_id = $1 AND emitting_address = $2",
    )
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(registration_topics)
    .bind(registration.data.to_vec())
    .execute(pool)
    .await?;

    let transaction_hash = format!("{chain_id}-transaction-1");
    let address_record = AddrChanged {
        node,
        a: REGISTRANT.parse()?,
    }
    .encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        1,
        DISCOVERED_RESOLVER,
        address_record.topics(),
        address_record.data.as_ref(),
    )
    .await?;
    let final_owner = NewOwner {
        node: raw_namehash(&[b"eth"]),
        label: labelhash,
        owner: REGISTRANT.parse()?,
    }
    .encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        2,
        REGISTRY,
        final_owner.topics(),
        final_owner.data.as_ref(),
    )
    .await?;
    Ok(node)
}

async fn seed_reverse_node_resolver_burst_fixture(pool: &PgPool, chain_id: &str) -> Result<B256> {
    const RESOLVER: &str = "0x0000000000000000000000000000000000000051";
    const REVERSE_REGISTRAR: &str = "0x0000000000000000000000000000000000000052";

    seed_same_transaction_registration_fixture(pool, chain_id).await?;
    sqlx::query(
        "UPDATE raw_logs
         SET log_index = 7
         WHERE chain_id = $1 AND emitting_address = $2",
    )
    .bind(chain_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;

    let resolver_payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_resolver_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": { "events": [
            {
                "name": "AddrChanged",
                "fragment": "event AddrChanged(bytes32 indexed node, address a)",
                "emitter_roles": [],
                "normalized_events": ["RecordChanged"]
            },
            {
                "name": "TextChanged",
                "fragment": "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
                "emitter_roles": [],
                "normalized_events": ["RecordChanged"]
            },
            {
                "name": "NameChanged",
                "fragment": "event NameChanged(bytes32 indexed node, string name)",
                "emitter_roles": [],
                "normalized_events": ["RecordChanged"]
            }
        ], "calls": [] }
    });
    insert_manifest(
        pool,
        chain_id,
        "ens_v1_resolver_l1",
        &format!("tests/{chain_id}-resolver.toml"),
        resolver_payload,
    )
    .await?;

    let reverse_payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_reverse_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "reverse_registrar",
            "address": REVERSE_REGISTRAR,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": { "events": [{
            "name": "ReverseClaimed",
            "fragment": "event ReverseClaimed(address indexed addr, bytes32 indexed node)",
            "emitter_roles": ["reverse_registrar"],
            "normalized_events": ["ReverseChanged"]
        }], "calls": [] }
    });
    let reverse_manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v1_reverse_l1",
        &format!("tests/{chain_id}-reverse.toml"),
        reverse_payload,
    )
    .await?;
    let reverse_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(reverse_instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind, start_block_number
         )
         VALUES ($1, $2, 'contract', 'reverse_registrar', $3, $4,
                 'reverse_registrar', 'none', 0)",
    )
    .bind(reverse_manifest_id)
    .bind(chain_id)
    .bind(reverse_instance_id)
    .bind(REVERSE_REGISTRAR)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id, provenance
         )
         VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(reverse_instance_id)
    .bind(chain_id)
    .bind(REVERSE_REGISTRAR)
    .bind(reverse_manifest_id)
    .execute(pool)
    .await?;

    let transaction_hash = format!("{chain_id}-transaction-1");
    let forward_node = raw_namehash(&[b"alice", b"eth"]);
    let addr_record = AddrChanged {
        node: forward_node,
        a: REGISTRANT.parse()?,
    }
    .encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        1,
        RESOLVER,
        addr_record.topics(),
        addr_record.data.as_ref(),
    )
    .await?;
    let text_record = TextChanged {
        node: forward_node,
        indexedKey: keccak256(b"avatar"),
        key: "avatar".to_owned(),
        value: "ipfs://avatar".to_owned(),
    }
    .encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        2,
        RESOLVER,
        text_record.topics(),
        text_record.data.as_ref(),
    )
    .await?;

    let reverse_label = SENDER.trim_start_matches("0x");
    let reverse_node = raw_namehash(&[reverse_label.as_bytes(), b"addr", b"reverse"]);
    let reverse_owner = NewOwner {
        node: raw_namehash(&[b"addr", b"reverse"]),
        label: keccak256(reverse_label.as_bytes()),
        owner: SENDER.parse()?,
    }
    .encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        3,
        REGISTRY,
        reverse_owner.topics(),
        reverse_owner.data.as_ref(),
    )
    .await?;
    let reverse_claim = ReverseClaimed {
        addr: SENDER.parse()?,
        node: reverse_node,
    }
    .encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        4,
        REVERSE_REGISTRAR,
        reverse_claim.topics(),
        reverse_claim.data.as_ref(),
    )
    .await?;
    let reverse_name = NameChanged {
        node: reverse_node,
        name: "alice.eth".to_owned(),
    }
    .encode_log_data();
    insert_log(
        pool,
        chain_id,
        &transaction_hash,
        5,
        RESOLVER,
        reverse_name.topics(),
        reverse_name.data.as_ref(),
    )
    .await?;
    Ok(reverse_node)
}

async fn seed_prior_owner_registration_fixture(
    pool: &PgPool,
    chain_id: &str,
    flow: PriorOwnerRegistrationFlow,
    final_registry_owner: &str,
) -> Result<()> {
    seed_fixture(pool, chain_id, &[(2, "alice")]).await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp,
             canonicality_state
         )
         VALUES ($1, $2, $3, 3, to_timestamp(3), 'canonical')",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, 3))
    .bind(block_hash(chain_id, 2))
    .execute(pool)
    .await?;

    let wrapped = final_registry_owner == WRAPPED_REGISTRY_OWNER;
    let registration_log_index = match flow {
        PriorOwnerRegistrationFlow::LegacyTwoOwnerChanges => 2,
        PriorOwnerRegistrationFlow::ModernSingleOwnerChange => 1,
    } + i64::from(wrapped);
    let registration = NameRegistered {
        name: "alice".to_owned(),
        label: B256::from(keccak256(b"alice")),
        owner: REGISTRANT.parse()?,
        expires: U256::from(1_000_002_u64),
    }
    .encode_log_data();
    let registration_topics = registration
        .topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    sqlx::query(
        "UPDATE raw_logs
         SET log_index = $2, topics = $3, data = $4
         WHERE chain_id = $1 AND block_number = 2",
    )
    .bind(chain_id)
    .bind(registration_log_index)
    .bind(registration_topics)
    .bind(registration.data.to_vec())
    .execute(pool)
    .await?;

    let contract_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(contract_instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_registry_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registry",
            "address": REGISTRY,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": { "events": [{
            "name": "NewOwner",
            "fragment": "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
            "emitter_roles": ["registry"],
            "normalized_events": [
                "SubregistryChanged",
                "AuthorityTransferred",
                "PermissionChanged",
                "SurfaceUnbound",
                "SurfaceBound",
                "AuthorityEpochChanged",
                "ResolverChanged"
            ]
        }], "calls": [] }
    });
    let manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v1_registry_l1",
        &format!("tests/{chain_id}-registry.toml"),
        payload,
    )
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind, start_block_number
         )
         VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(contract_instance_id)
    .bind(REGISTRY)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id, provenance
         )
         VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(contract_instance_id)
    .bind(chain_id)
    .bind(REGISTRY)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    if wrapped {
        insert_name_wrapper_manifest(pool, chain_id).await?;
    }

    let owner_change = |owner: &str| -> Result<alloy_primitives::LogData> {
        Ok(NewOwner {
            node: raw_namehash(&[b"eth"]),
            label: B256::from(keccak256(b"alice")),
            owner: owner.parse()?,
        }
        .encode_log_data())
    };
    insert_registry_transaction(pool, chain_id, 1, "prior-owner").await?;
    let prior_owner = owner_change(PRIOR_REGISTRY_OWNER)?;
    insert_log_at(
        pool,
        chain_id,
        1,
        &format!("{chain_id}-prior-owner"),
        0,
        REGISTRY,
        prior_owner.topics(),
        prior_owner.data.as_ref(),
    )
    .await?;
    let registration_transaction = format!("{chain_id}-transaction-2");
    if matches!(flow, PriorOwnerRegistrationFlow::LegacyTwoOwnerChanges) {
        let transient_owner = owner_change(CONTRACT)?;
        insert_log_at(
            pool,
            chain_id,
            2,
            &registration_transaction,
            0,
            REGISTRY,
            transient_owner.topics(),
            transient_owner.data.as_ref(),
        )
        .await?;
    }
    let incoming_owner = owner_change(final_registry_owner)?;
    insert_log_at(
        pool,
        chain_id,
        2,
        &registration_transaction,
        registration_log_index - 1 - i64::from(wrapped),
        REGISTRY,
        incoming_owner.topics(),
        incoming_owner.data.as_ref(),
    )
    .await?;
    if wrapped {
        let name_wrapped = NameWrapped {
            node: raw_namehash(&[b"alice", b"eth"]),
            name: b"\x05alice\x03eth\0".to_vec().into(),
            owner: REGISTRANT.parse()?,
            fuses: 1,
            expiry: 1_000_002,
        }
        .encode_log_data();
        insert_log_at(
            pool,
            chain_id,
            2,
            &registration_transaction,
            registration_log_index - 1,
            WRAPPED_REGISTRY_OWNER,
            name_wrapped.topics(),
            name_wrapped.data.as_ref(),
        )
        .await?;
    }

    insert_registry_transaction(pool, chain_id, 3, "reminted-owner").await?;
    let reminted_owner = owner_change(REMINTED_REGISTRY_OWNER)?;
    insert_log_at(
        pool,
        chain_id,
        3,
        &format!("{chain_id}-reminted-owner"),
        0,
        REGISTRY,
        reminted_owner.topics(),
        reminted_owner.data.as_ref(),
    )
    .await?;
    Ok(())
}

async fn insert_name_wrapper_manifest(pool: &PgPool, chain_id: &str) -> Result<()> {
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_wrapper_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "name_wrapper",
            "address": WRAPPED_REGISTRY_OWNER,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": { "events": [{
            "name": "NameWrapped",
            "fragment": "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
            "emitter_roles": ["name_wrapper"],
            "normalized_events": [
                "TokenControlTransferred",
                "ExpiryChanged",
                "PermissionScopeChanged",
                "SurfaceUnbound",
                "SurfaceBound",
                "AuthorityEpochChanged",
                "ResolverChanged",
                "PreimageObserved"
            ]
        }], "calls": [] }
    });
    let manifest_id = insert_manifest(
        pool,
        chain_id,
        "ens_v1_wrapper_l1",
        &format!("tests/{chain_id}-wrapper.toml"),
        payload,
    )
    .await?;
    let contract_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(contract_instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind, start_block_number
         )
         VALUES ($1, $2, 'contract', 'name_wrapper', $3, $4, 'name_wrapper', 'none', 0)",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(contract_instance_id)
    .bind(WRAPPED_REGISTRY_OWNER)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id, provenance
         )
         VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(contract_instance_id)
    .bind(chain_id)
    .bind(WRAPPED_REGISTRY_OWNER)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_registry_transaction(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
    suffix: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash, transaction_index,
             from_address, to_address
         )
         VALUES ($1, $2, $3, $4, 0, $5, $6)",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, block_number))
    .bind(block_number)
    .bind(format!("{chain_id}-{suffix}"))
    .bind(SENDER)
    .bind(REGISTRY)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_fixture_with_timestamps(
    pool: &PgPool,
    chain_id: &str,
    labels: &[(i64, &str)],
    timestamp_overrides: &[(i64, i64)],
) -> Result<()> {
    let through = labels.iter().map(|(block, _)| *block).max().unwrap_or(0);
    for block in 0..=through {
        let block_timestamp = timestamp_overrides
            .iter()
            .find_map(|(candidate, timestamp)| (*candidate == block).then_some(*timestamp))
            .unwrap_or(block);
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')
            ",
        )
        .bind(chain_id)
        .bind(block_hash(chain_id, block))
        .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
        .bind(block)
        .bind(block_timestamp)
        .execute(pool)
        .await?;
    }
    let contract_instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(contract_instance_id)
        .bind(chain_id)
        .execute(pool)
        .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_registrar_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registrar",
            "address": CONTRACT,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": { "events": [{
            "name": "NameRegistered",
            "fragment": "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            "emitter_roles": ["registrar"],
            "normalized_events": ["RegistrationGranted"]
        }], "calls": [] }
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "
        INSERT INTO manifest_versions (
            manifest_version, namespace, source_family, chain_id, deployment_label,
            rollout_status, normalizer_version, file_path, manifest_payload
        )
        VALUES (1, 'ens', 'ens_v1_registrar_l1', $1, 'fixture', 'active', $2, $3, $4)
        RETURNING manifest_id
        ",
    )
    .bind(chain_id)
    .bind(NORMALIZER)
    .bind(format!("tests/{chain_id}.toml"))
    .bind(payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind, start_block_number
        )
        VALUES ($1, $2, 'contract', 'registrar', $3, $4, 'registrar', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(contract_instance_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address, active_from_block_number,
            source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
        ",
    )
    .bind(contract_instance_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    for (block, label) in labels {
        insert_name_registered(pool, chain_id, *block, label).await?;
    }
    Ok(())
}

async fn insert_name_registered(
    pool: &PgPool,
    chain_id: &str,
    block: i64,
    label: &str,
) -> Result<()> {
    let transaction_hash = format!("{chain_id}-transaction-{block}");
    sqlx::query(
        "
        INSERT INTO raw_transactions (
            chain_id, block_hash, block_number, transaction_hash, transaction_index,
            from_address, to_address
        )
        VALUES ($1, $2, $3, $4, 0, $5, $6)
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, block))
    .bind(block)
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    let encoded = NameRegistered {
        name: label.to_owned(),
        label: B256::from(keccak256(label.as_bytes())),
        owner: CONTRACT.parse::<Address>()?,
        expires: U256::from(1_000_000u64 + block as u64),
    }
    .encode_log_data();
    let topics = encoded
        .topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    sqlx::query(
        "
        INSERT INTO raw_logs (
            chain_id, block_hash, block_number, transaction_hash, transaction_index,
            log_index, emitting_address, topics, data
        )
        VALUES ($1, $2, $3, $4, 0, 0, $5, $6, $7)
        ",
    )
    .bind(chain_id)
    .bind(block_hash(chain_id, block))
    .bind(block)
    .bind(transaction_hash)
    .bind(CONTRACT)
    .bind(topics)
    .bind(encoded.data.to_vec())
    .execute(pool)
    .await?;
    Ok(())
}

async fn replace_registered_label(
    pool: &PgPool,
    chain_id: &str,
    block: i64,
    label: &str,
) -> Result<()> {
    let encoded = NameRegistered {
        name: label.to_owned(),
        label: B256::from(keccak256(label.as_bytes())),
        owner: CONTRACT.parse::<Address>()?,
        expires: U256::from(1_000_000u64 + block as u64),
    }
    .encode_log_data();
    let topics = encoded
        .topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    sqlx::query(
        "UPDATE raw_logs SET topics = $3, data = $4 \
         WHERE chain_id = $1 AND block_number = $2",
    )
    .bind(chain_id)
    .bind(block)
    .bind(topics)
    .bind(encoded.data.to_vec())
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_completed_phase_extent(pool: &PgPool, chain_id: &str, hash: &str) -> Result<()> {
    seed_completed_phase_extent_at(pool, chain_id, 2, hash).await
}

async fn seed_completed_phase_extent_at(
    pool: &PgPool,
    chain_id: &str,
    head: i64,
    hash: &str,
) -> Result<()> {
    let head_hash = block_hash(chain_id, head);
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'completed',
            current_block_number = $2,
            current_block_hash = $3,
            target_block_number = $2,
            target_block_hash = $3,
            live_handoff_block_number = CASE WHEN phase_name = 'ingest' THEN $2 END,
            live_handoff_block_hash = CASE WHEN phase_name = 'ingest' THEN $3 END,
            input_content_hash = CASE WHEN phase_name = 'interpret' THEN $4 END,
            started_at = now(),
            finished_at = now()
        WHERE chain_id = $1
          AND phase_name IN ('ingest', 'interpret')
        ",
    )
    .bind(chain_id)
    .bind(head)
    .bind(&head_hash)
    .bind(hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO ingest_cursors (
            chain_id, source_key, source_kind, seed_basis, start_block_number,
            next_block_number, target_block_number, last_processed_block_number,
            last_processed_block_hash
        )
        VALUES ($1, 'source', 'test', 'ethereum_head', 0, $2, $3, $3, $4)
        ",
    )
    .bind(chain_id)
    .bind(head.saturating_add(1))
    .bind(head)
    .bind(head_hash)
    .execute(pool)
    .await?;
    Ok(())
}

async fn initialize_completed_recompute_extent(
    pool: &PgPool,
    chain_id: &str,
    head: i64,
) -> Result<()> {
    let store = PhaseStore::new(pool.clone());
    store.initialize_chain(chain_id).await?;
    seed_completed_phase_extent_at(pool, chain_id, head, INTERPRETER_CONTENT_HASH).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             current_block_number = $2,
             current_block_hash = $3,
             target_block_number = $2,
             target_block_hash = $3,
             input_content_hash = $4,
             started_at = now(),
             finished_at = now()
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain_id)
    .bind(head)
    .bind(block_hash(chain_id, head))
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    Ok(())
}

struct InterruptAfterRecomputePhase {
    engine: Engine,
}

impl Phase for InterruptAfterRecomputePhase {
    fn name(&self) -> PhaseName {
        PhaseName::Interpret
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let range = context.mode.range().ok_or_else(|| {
                RunnerError::data_integrity("injected recompute phase requires a bounded range")
            })?;
            self.engine
                .run_batch(BatchRequest {
                    chain_id: context.chain_id,
                    from_block: range.from,
                    to_block: range.to,
                    resume_current: context.resume.current.map(|marker| Marker {
                        number: marker.number,
                        hash: marker.hash,
                    }),
                    mode: InterpretRunMode::RecomputeFlags,
                })
                .await
                .map_err(|error| RunnerError::new(ErrorKind::DataIntegrity, error.to_string()))?;
            Err(RunnerError::data_integrity(
                "injected recompute interruption after the interpret write committed",
            ))
        })
    }
}

struct BlockingInterpretPhase {
    inner: InterpretPhase,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Phase for BlockingInterpretPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Interpret
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let outcome = self.inner.run_batch(context).await?;
            self.entered.notify_one();
            self.release.notified().await;
            Ok(outcome)
        })
    }
}

struct FailProjectOncePhase {
    inner: ProjectPhase,
    attempts: AtomicUsize,
}

impl Phase for FailProjectOncePhase {
    fn name(&self) -> PhaseName {
        PhaseName::Project
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(RunnerError::data_integrity(
                    "injected project refresh failure",
                ));
            }
            self.inner.run_batch(context).await
        })
    }
}

struct CancelAfterProjectPhase {
    inner: ProjectPhase,
    cancellation: CancellationToken,
}

impl Phase for CancelAfterProjectPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Project
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let outcome = self.inner.run_batch(context).await?;
            self.cancellation.cancel();
            Ok(outcome)
        })
    }
}

fn recompute_runner(
    scratch: &ScratchDatabase,
    _chain_id: &str,
    instance_id: &str,
) -> Result<PhaseRunner> {
    Ok(PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(InterpretPhase::new(scratch.pool().clone())),
            Arc::new(ProjectPhase::new(scratch.pool().clone())),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        instance_id,
        test_timing(),
    )?)
}

async fn assert_transition_redo(pool: &PgPool, chain_id: &str, from: i64) -> Result<()> {
    let markers: Vec<(String, String, i64, i64, bool)> = sqlx::query_as(
        "SELECT phase_name, redo_mode, redo_from_block_number, redo_to_block_number,
                last_error LIKE 'required downstream redo%'
         FROM chain_phase_state
         WHERE chain_id = $1
           AND phase_name IN ('interpret', 'project')
           AND redo_in_progress
         ORDER BY phase_name",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?;
    assert_eq!(
        markers,
        vec![
            ("interpret".into(), "redo".into(), from, from, true),
            ("project".into(), "redo".into(), from, from, true),
        ]
    );
    Ok(())
}

async fn assert_no_interpret_project_redo(pool: &PgPool, chain_id: &str) -> Result<()> {
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM chain_phase_state
         WHERE chain_id = $1
           AND phase_name IN ('interpret', 'project')
           AND redo_in_progress",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(pending, 0);
    Ok(())
}

async fn insert_competing_live_block(pool: &PgPool, chain_id: &str, block: i64) -> Result<()> {
    sqlx::query(
        "
        INSERT INTO chain_lineage (
            chain_id, block_hash, parent_hash, block_number, block_timestamp,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, to_timestamp($4), 'safe')
        ",
    )
    .bind(chain_id)
    .bind(format!("{chain_id}-competing-{block}"))
    .bind((block > 0).then(|| block_hash(chain_id, block - 1)))
    .bind(block)
    .execute(pool)
    .await?;
    Ok(())
}

async fn permit_duplicate_live_heights_for_corruption_test(pool: &PgPool) -> Result<()> {
    sqlx::query("DROP INDEX chain_lineage_readable_height_idx")
        .execute(pool)
        .await?;
    Ok(())
}

async fn pool_with_acquire_gate(
    source: &PgPool,
    gated_acquisition: usize,
) -> Result<(PgPool, Arc<Notify>, Arc<Notify>)> {
    let acquire_count = Arc::new(AtomicUsize::new(0));
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let count_for_hook = Arc::clone(&acquire_count);
    let reached_for_hook = Arc::clone(&reached);
    let release_for_hook = Arc::clone(&release);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .test_before_acquire(false)
        .before_acquire(move |_connection, _metadata| {
            let reached = Arc::clone(&reached_for_hook);
            let release = Arc::clone(&release_for_hook);
            let acquisition = count_for_hook.fetch_add(1, Ordering::SeqCst) + 1;
            Box::pin(async move {
                if acquisition == gated_acquisition {
                    reached.notify_one();
                    release.notified().await;
                }
                Ok(true)
            })
        })
        .connect_with(source.connect_options().as_ref().clone())
        .await?;
    drop(pool.acquire().await?);
    acquire_count.store(0, Ordering::SeqCst);
    Ok((pool, reached, release))
}

async fn wait_for_locked_query(pool: &PgPool, query_pattern: &str) -> Result<()> {
    for _ in 0..200 {
        let waiting: bool = sqlx::query_scalar(
            "
            SELECT EXISTS (
                SELECT 1
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND wait_event_type = 'Lock'
                  AND query LIKE $1
            )
            ",
        )
        .bind(query_pattern)
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("interpretation did not reach locked query matching {query_pattern}")
}

async fn switch_live_lineage(pool: &PgPool, chain_id: &str, live_hash: &str) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE chain_id = $1
          AND block_number = 1
          AND block_hash <> $2
        ",
    )
    .bind(chain_id)
    .bind(live_hash)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'canonical'::canonicality_state
        WHERE chain_id = $1
          AND block_number = 1
          AND block_hash = $2
        ",
    )
    .bind(chain_id)
    .bind(live_hash)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

fn chain_config(chain_id: &str) -> phase_runner::error::RunnerResult<ChainConfig> {
    ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "source",
            "test",
            SeedBasis::EthereumHead,
            0,
            "http://source.invalid",
        )?],
        false,
    )
}

fn test_timing() -> TimingConfig {
    TimingConfig {
        initial_backoff: Duration::from_millis(1),
        maximum_backoff: Duration::from_millis(4),
        live_poll_interval: Duration::from_millis(1),
    }
}

fn block_hash(chain_id: &str, block: i64) -> String {
    format!("{chain_id}-block-{block}")
}
