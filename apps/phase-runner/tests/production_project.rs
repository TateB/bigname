#[allow(dead_code)]
mod support;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::Result;
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput as AdapterBatchInput, DiscoveryRuleInput, ManifestInput,
    RawBlockInput as AdapterRawBlockInput, RawLogInput as AdapterRawLogInput,
    interpret_schema_v2_batch, seam::REDO_RESOLVER_EVIDENCE_SELECT_SQL,
};
use bigname_domain::resolver_read::{
    ENSIP19_DEFAULT_RECORD_KEY, IndexedRecordStatus, ResolverReadFeature, evaluate_indexed_record,
};
use bigname_interpret::{
    BatchRequest as InterpretRequest, Engine as InterpretEngine, Marker as InterpretMarker,
    RunMode as InterpretRunMode,
};
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use bigname_project::{
    BatchRequest, DUAL_CURRENT_CHILD_AUTHORITY, DUAL_CURRENT_EXACT_NAME_AUTHORITY, Engine, Marker,
    RunMode,
};
use bigname_storage::{
    NameCurrentRow, READABLE_REVERSE_IDENTITY_CTES, record_version_boundary_storage_key,
    resolution_verified_support_boundary,
};
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    error::ErrorKind as RunnerErrorKind,
    heads::{BlockMarker, HeadMarkers, publish_heads},
    interpret_phase::InterpretPhase,
    phase::{
        BlockRange, LoopbackPhase, Phase, PhaseContext, PhaseName, PhaseResume, PhaseSet,
        RunMode as PhaseRunMode,
    },
    project_phase::ProjectPhase,
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool, Row,
    types::{Uuid, time},
};
use tokio_util::sync::CancellationToken;

use support::ScratchDatabase;

const CHAIN: &str = "project-fixture";
const BASE_CHAIN: &str = "base-mainnet";
const ETHEREUM_CHAIN: &str = "ethereum-mainnet";
const OWNER: &str = "0x00000000000000000000000000000000000000a1";
const TRANSFER_OWNER: &str = "0x00000000000000000000000000000000000000a2";
const RESOLVER: &str = "0x00000000000000000000000000000000000000b1";
const BASENAMES_RESOLVER: &str = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd";
const BASENAMES_L1_RESOLVER: &str = "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31";
const REGISTRAR: &str = "0x0000000000000000000000000000000000000042";
const SENDER: &str = "0x0000000000000000000000000000000000000043";
const REGISTRY: &str = "0x0000000000000000000000000000000000000044";
const WRAPPER: &str = "0x0000000000000000000000000000000000000045";
const REVERSE_REGISTRAR: &str = "0x0000000000000000000000000000000000000046";
const V2_REGISTRY: &str = "0x0000000000000000000000000000000000000047";
const NORMALIZER: &str = "ensip15@ens-normalize-0.1.1";
const RESOURCE: &str = "00000000-0000-0000-0000-000000000011";
const SURFACE_BINDING: &str = "00000000-0000-0000-0000-000000000012";
const TOKEN_LINEAGE: &str = "00000000-0000-0000-0000-000000000013";
const BASENAMES_RESOURCE: &str = "00000000-0000-0000-0000-000000000031";
const EQUIVALENCE_BOB_RESOURCE: &str = "00000000-0000-0000-0000-0000000000b0";
const EQUIVALENCE_BOB_BINDING: &str = "00000000-0000-0000-0000-0000000000b1";
const EQUIVALENCE_PARENT_BINDING: &str = "00000000-0000-0000-0000-0000000000b3";
const EQUIVALENCE_PARENT_RESOURCE: &str = "00000000-0000-0000-0000-0000000000b6";
const EQUIVALENCE_TRANSFER_RESOURCE: &str = "00000000-0000-0000-0000-0000000000b4";
const EQUIVALENCE_TRANSFER_BINDING: &str = "00000000-0000-0000-0000-0000000000b5";
const EQUIVALENCE_V2_RESOLVER: &str = "0x00000000000000000000000000000000000000b2";
const EQUIVALENCE_TRANSFER_RESOLVER: &str = "0x00000000000000000000000000000000000000b3";
const PERMISSION_ONLY_RESOLVER: &str = "0x00000000000000000000000000000000000000b4";
const EQUIVALENCE_V2_IMPLEMENTATION: &str = "0x00000000000000000000000000000000000000c2";
const PERMISSION_ONLY_RESOURCE: &str = "00000000-0000-0000-0000-0000000000c0";
const PERMISSION_ONLY_BINDING: &str = "00000000-0000-0000-0000-0000000000c1";
const FAMILY_BINDING_NAME: &str = "ens:0xfamily-binding";
const FAMILY_BINDING_RESOURCE: &str = "00000000-0000-0000-0000-0000000000d5";
const FAMILY_BINDING_ID: &str = "00000000-0000-0000-0000-0000000000d6";
const FAMILY_SURVIVOR_NAME: &str = "ens:0xfamily-survivor";
const HISTORY_RESOLVER: &str = "0x00000000000000000000000000000000000000d7";
const HISTORY_REVOKED_RESOURCE: &str = "00000000-0000-0000-0000-0000000000d7";
const HISTORY_LIVE_RESOURCE: &str = "00000000-0000-0000-0000-0000000000d8";
const V2_INVERSE_RESOLVER: &str = "0x00000000000000000000000000000000000000d9";
const V2_INVERSE_IMPLEMENTATION: &str = "0x00000000000000000000000000000000000000da";
const V2_INVERSE_RESOURCE: &str = "00000000-0000-0000-0000-0000000000d9";
const UNLINKED_RESOLVER: &str = "0x00000000000000000000000000000000000000db";
const FAMILY_PERMISSION_RESOLVER: &str = "0x00000000000000000000000000000000000000dc";
const FAMILY_POINTER_RESOLVER: &str = "0x00000000000000000000000000000000000000dd";
const EMITTER_ONLY_RESOLVER: &str = "0x00000000000000000000000000000000000000de";
const FAMILY_PERMISSION_RESOURCE: &str = "00000000-0000-0000-0000-0000000000dc";

type ChildLabelRow = (Vec<u8>, Option<String>, Vec<u8>, Option<String>);
type OptionalChildLabelRow = (
    Option<Vec<u8>>,
    Option<String>,
    Option<Vec<u8>>,
    Option<String>,
);
type TopologyOnlyChildRow = (
    Option<Vec<u8>>,
    Option<String>,
    Option<Vec<u8>>,
    Option<String>,
    String,
    String,
);
type PrimaryClaimRow = (String, String, Option<String>, bool, Option<String>);

#[derive(Clone, Copy, Debug)]
enum EnsArmSet {
    Empty,
    V1,
    V2,
    Both,
}

impl EnsArmSet {
    fn includes_v1(self) -> bool {
        matches!(self, Self::V1 | Self::Both)
    }

    fn includes_v2(self) -> bool {
        matches!(self, Self::V2 | Self::Both)
    }

    fn label(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::V1 => "v1",
            Self::V2 => "v2",
            Self::Both => "v1_v2",
        }
    }
}

#[derive(Debug)]
struct ClassifierBindings {
    v1: Option<(Uuid, Uuid)>,
    v2: Option<(Uuid, Uuid)>,
}

sol! {
    event NameRegistered(
        string name,
        bytes32 indexed label,
        address indexed owner,
        uint256 expires
    );
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event NameWrapped(
        bytes32 indexed node,
        bytes name,
        address owner,
        uint32 fuses,
        uint64 expiry
    );
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);
    event TextChanged(
        bytes32 indexed node,
        string indexed indexedKey,
        string key,
        string value
    );
    event ReverseClaimed(address indexed addr, bytes32 indexed node);
    event LabelRegistered(
        uint256 indexed tokenId,
        bytes32 indexed labelHash,
        string label,
        address owner,
        uint64 expiry,
        address indexed sender
    );
    event LabelReserved(
        uint256 indexed tokenId,
        bytes32 indexed labelHash,
        string label,
        uint64 expiry,
        address indexed sender
    );
    event ResolverUpdated(
        uint256 indexed tokenId,
        address indexed resolver,
        address indexed sender
    );
    event ExpiryUpdated(
        uint256 indexed tokenId,
        uint64 indexed newExpiry,
        address indexed sender
    );
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
}

#[tokio::test]
async fn canonical_fixture_builds_all_eight_projection_families() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_all_builders").await?;
    seed_project_fixture(scratch.pool()).await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    let grouped_builder_snapshot: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object(
             'account_permissions', COALESCE((
                 SELECT jsonb_agg(
                     to_jsonb(account_permission) - 'last_recomputed_at' - 'inserted_at'
                     ORDER BY chain_id, authority_kind, authority_contract, owner, subject,
                         relation_kind
                 ) FROM account_permission_state_current account_permission
             ), '[]'::jsonb),
             'permissions', COALESCE((
                 SELECT jsonb_agg(
                     to_jsonb(permission) - 'last_recomputed_at' - 'inserted_at'
                     ORDER BY resource_id, subject, scope
                 ) FROM permissions_current permission
             ), '[]'::jsonb),
             'permission_summaries', COALESCE((
                 SELECT jsonb_agg(
                     to_jsonb(summary) - 'last_recomputed_at' ORDER BY resource_id
                 ) FROM permissions_current_resource_summary summary
             ), '[]'::jsonb),
             'resolvers', COALESCE((
                 SELECT jsonb_agg(
                     (to_jsonb(resolver) - 'last_recomputed_at' - 'inserted_at' -
                         'declared_summary') || jsonb_build_object(
                             'declared_summary', declared_summary - 'bindings' - 'aliases' -
                                 'permissions' - 'role_holders'
                         )
                     ORDER BY chain_id, resolver_address
                 ) FROM resolver_current resolver
             ), '[]'::jsonb),
             'primary_names', COALESCE((
                 SELECT jsonb_agg(
                     to_jsonb(primary_name) -
                         'reverse_hydration_attempted_block_number' -
                         'reverse_hydration_attempted_block_hash' -
                         'reverse_hydration_attempt_ordinal'
                     ORDER BY address, coin_type, namespace)
                 FROM primary_names_current primary_name
             ), '[]'::jsonb)
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        grouped_builder_snapshot,
        json!({
            "account_permissions": [],
            "permission_summaries": [{
                "authority_kind": "registrar",
                "canonicality_summary": {
                    "state": "canonical_lineage",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "chain_positions": {
                    "block_hash": "project-fixture-block-1",
                    "block_number": 1,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "manifest_version": 1,
                "provenance": {
                    "authority_event_id": 6,
                    "chain_id": "project-fixture",
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "registry_binding_clear_event_id": 3
                },
                "registry_binding_chain_positions": null,
                "registry_binding_provenance": null,
                "registry_contract": null,
                "registry_owner": null,
                "resource_id": RESOURCE,
                "root_resource_id": null,
                "support_status": "unsupported",
                "unsupported_reason": "operator_approval_surfaces_not_ingested"
            }],
            "permissions": [{
                "canonicality_summary": {
                    "state": "canonical",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "chain_positions": {
                    "block_hash": "project-fixture-block-1",
                    "block_number": 1,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "effective_powers": ["resource_control"],
                "grant_source": {"kind": "fixture"},
                "inheritance_path": [],
                "manifest_version": 1,
                "provenance": {
                    "chain_id": "project-fixture",
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "derivation_kind": "permissions_current_rebuild",
                    "manifest_versions": [{
                        "manifest_version": 1,
                        "source_family": "ens_v1_registrar_l1",
                        "source_manifest_id": null
                    }],
                    "normalized_event_ids": [6],
                    "permission_manifest_versions": [{
                        "manifest_version": 1,
                        "source_family": "ens_v1_registrar_l1",
                        "source_manifest_id": null
                    }],
                    "raw_fact_refs": [{}]
                },
                "resource_id": RESOURCE,
                "revocation_source": null,
                "scope": "resource",
                "scope_detail": {"kind": "resource"},
                "scope_kind": "resource",
                "subject": OWNER,
                "transfer_behavior": {"mode": "replace_on_authority_change"}
            }],
            "primary_names": [{
                "address": OWNER,
                "claim_name_is_normalized": true,
                "claim_provenance": {
                    "chain_id": "project-fixture",
                    "claim_event_id": 10,
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "reverse_event_id": 9,
                    "source_family": "ens_v1_reverse_l1",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "claim_status": "success",
                "coin_type": "60",
                "namespace": "ens",
                "raw_claim_name": "alice.eth",
                "unsupported_reason": null
            }],
            "resolvers": [{
                "canonicality_summary": {
                    "state": "canonical_lineage",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "chain_id": "project-fixture",
                "chain_positions": {
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "declared_summary": {
                    "classification": {
                        "basis": "manifest_declared_address",
                        "read_features": [],
                        "role": "public_resolver",
                        "source_family": "ens_v1_resolver_l1"
                    },
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "event_summary": {
                        "status": "unsupported",
                        "unsupported_reason":
                            "resolver_binding_enumeration_not_projected"
                    }
                },
                "manifest_version": 1,
                "provenance": {
                    "candidate_event_ids": [7],
                    "chain_id": "project-fixture",
                    "manifest_event_id": 2,
                    "manifest_id": 2
                },
                "resolver_address": RESOLVER,
                "support_status": "supported",
                "unsupported_reason": null
            }]
        })
    );

    let counts: (i64, i64, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT count(*) FROM name_current),
            (SELECT count(*) FROM children_current),
            (SELECT count(*) FROM permissions_current),
            (SELECT count(*) FROM account_permission_state_current),
            (SELECT count(*) FROM permissions_current_resource_summary),
            (SELECT count(*) FROM record_inventory_current),
            (SELECT count(*) FROM resolver_current),
            (SELECT count(*) FROM address_names_current),
            (SELECT count(*) FROM primary_names_current)",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(counts, (2, 2, 1, 0, 1, 1, 1, 3, 1));

    let evidence: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT family, status, exhaustiveness
         FROM (
             SELECT 'name' AS family,
                    min(declared_summary -> 'coverage' ->> 'status') AS status,
                    min(declared_summary -> 'coverage' ->> 'exhaustiveness') AS exhaustiveness
             FROM name_current
             UNION ALL
             SELECT 'children', min(provenance -> 'coverage' ->> 'status'),
                    min(provenance -> 'coverage' ->> 'exhaustiveness')
             FROM children_current
             UNION ALL
             SELECT 'permissions', min(provenance -> 'coverage' ->> 'status'),
                    min(provenance -> 'coverage' ->> 'exhaustiveness')
             FROM permissions_current
             UNION ALL
             SELECT 'permission_summary', min(provenance -> 'coverage' ->> 'status'),
                    min(provenance -> 'coverage' ->> 'exhaustiveness')
             FROM permissions_current_resource_summary
             UNION ALL
             SELECT 'record_inventory', min(provenance -> 'coverage' ->> 'status'),
                    min(provenance -> 'coverage' ->> 'exhaustiveness')
             FROM record_inventory_current
             UNION ALL
             SELECT 'resolver', min(declared_summary -> 'coverage' ->> 'status'),
                    min(declared_summary -> 'coverage' ->> 'exhaustiveness')
             FROM resolver_current
             UNION ALL
             SELECT 'address_names', min(provenance -> 'coverage' ->> 'status'),
                    min(provenance -> 'coverage' ->> 'exhaustiveness')
             FROM address_names_current
             UNION ALL
             SELECT 'primary_names', min(claim_provenance -> 'coverage' ->> 'status'),
                    min(claim_provenance -> 'coverage' ->> 'exhaustiveness')
             FROM primary_names_current
         ) coverage
         ORDER BY family",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(evidence.len(), 8);
    assert!(evidence.iter().all(
        |(_, status, exhaustiveness)| status == "projected" && exhaustiveness == "not_asserted"
    ));
    let resolver_status: (String, String) = sqlx::query_as(
        "SELECT support_status,
                declared_summary -> 'classification' ->> 'basis'
         FROM resolver_current WHERE resolver_address = lower($1)",
    )
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        resolver_status,
        ("supported".into(), "manifest_declared_address".into())
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn recompute_flags_refreshes_same_class_flags_and_primary_projection_without_replay()
-> Result<()> {
    let scratch = ScratchDatabase::create("production_project_recompute_flags").await?;
    seed_project_fixture(scratch.pool()).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(CHAIN).await?;
    seed_completed_project_extent(scratch.pool(), CHAIN, 3).await?;
    sqlx::query(
        "UPDATE chain_phase_state SET settled_while_unconfigured = TRUE
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;

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
        "UPDATE name_surfaces SET normalizer_version = 'stale-version' WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE primary_names_current SET claim_name_is_normalized = false WHERE namespace = 'ens'",
    )
    .execute(scratch.pool())
    .await?;

    let events_before: Vec<(i64, Value, Value, String)> = sqlx::query_as(
        "SELECT normalized_event_id, before_state, after_state, canonicality_state::text
         FROM normalized_events WHERE chain_id = $1 ORDER BY normalized_event_id",
    )
    .bind(CHAIN)
    .fetch_all(scratch.pool())
    .await?;
    let anchors_before: Vec<(String, String, i64, Value, time::OffsetDateTime)> = sqlx::query_as(
        "SELECT logical_name_id, block_hash, block_number, provenance, observed_at
         FROM name_surfaces WHERE chain_id = $1 ORDER BY logical_name_id",
    )
    .bind(CHAIN)
    .fetch_all(scratch.pool())
    .await?;
    let bindings_before: Vec<(Uuid, String, i64, Option<time::OffsetDateTime>, String)> =
        sqlx::query_as(
            "SELECT surface_binding_id, block_hash, block_number, active_to,
                    canonicality_state::text
             FROM surface_bindings WHERE chain_id = $1 ORDER BY surface_binding_id",
        )
        .bind(CHAIN)
        .fetch_all(scratch.pool())
        .await?;

    let phases = PhaseSet::with_ingest_interpret_and_project(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.pool().clone())),
        Arc::new(ProjectPhase::new(scratch.pool().clone())),
    )?;
    PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-project-recompute-flags",
        test_timing(),
    )?
    .redo(
        &chain_config(CHAIN)?,
        RedoPhase::RecomputeFlags,
        BlockRange::new(0, 3)?,
        CancellationToken::new(),
    )
    .await?;

    let label: (String, bool, Option<String>) = sqlx::query_as(
        "SELECT normalizer_version, normalized_under_version, normalization_error
         FROM label_preimages WHERE decoded_label = 'alice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(label, (NORMALIZER.into(), true, None));
    let stale_surfaces: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM name_surfaces
         WHERE chain_id = $1 AND normalizer_version <> $2",
    )
    .bind(CHAIN)
    .bind(NORMALIZER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(stale_surfaces, 0);
    let claim_is_normalized: bool = sqlx::query_scalar(
        "SELECT claim_name_is_normalized FROM primary_names_current WHERE namespace = 'ens'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(claim_is_normalized);
    let pending_redos: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project') AND redo_in_progress",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(pending_redos, 0, "same-class names must not stamp replay");
    let project_settlement: Option<bool> = sqlx::query_scalar(
        "SELECT settled_while_unconfigured FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        project_settlement, None,
        "a successful staged Project refresh must clear settlement provenance"
    );

    let events_after: Vec<(i64, Value, Value, String)> = sqlx::query_as(
        "SELECT normalized_event_id, before_state, after_state, canonicality_state::text
         FROM normalized_events WHERE chain_id = $1 ORDER BY normalized_event_id",
    )
    .bind(CHAIN)
    .fetch_all(scratch.pool())
    .await?;
    let anchors_after: Vec<(String, String, i64, Value, time::OffsetDateTime)> = sqlx::query_as(
        "SELECT logical_name_id, block_hash, block_number, provenance, observed_at
         FROM name_surfaces WHERE chain_id = $1 ORDER BY logical_name_id",
    )
    .bind(CHAIN)
    .fetch_all(scratch.pool())
    .await?;
    let bindings_after: Vec<(Uuid, String, i64, Option<time::OffsetDateTime>, String)> =
        sqlx::query_as(
            "SELECT surface_binding_id, block_hash, block_number, active_to,
                    canonicality_state::text
             FROM surface_bindings WHERE chain_id = $1 ORDER BY surface_binding_id",
        )
        .bind(CHAIN)
        .fetch_all(scratch.pool())
        .await?;
    assert_eq!(events_after, events_before);
    assert_eq!(anchors_after, anchors_before);
    assert_eq!(bindings_after, bindings_before);
    scratch.cleanup().await
}

#[tokio::test]
async fn permission_builder_preserves_grouped_history_output_exactly() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_permission_history").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_namespaced_event(
        scratch.pool(),
        "ens",
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registry_l1",
        3,
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control","set_resolver"],
            "grant_source":{"kind":"tie_break_winner"},
            "revocation_source":null,
            "inheritance_path":["tie_break_winner"],
            "transfer_behavior":"retain"
        }),
        json!({"history":"tie_break_winner"}),
    )
    .await?;
    insert_namespaced_event(
        scratch.pool(),
        "ens",
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registry_l1",
        2,
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["set_resolver"],
            "grant_source":{"kind":"later_inserted_loser"},
            "revocation_source":null,
            "inheritance_path":["later_inserted_loser"],
            "transfer_behavior":"retain"
        }),
        json!({"history":"later_inserted_loser"}),
    )
    .await?;
    let positioned = sqlx::query(
        "UPDATE normalized_events event
         SET transaction_hash = $2,
             transaction_index = position.transaction_index,
             log_index = position.log_index
         FROM (VALUES
             ('tie_break_winner', 5::bigint, 9::bigint),
             ('later_inserted_loser', 5::bigint, 8::bigint)
         ) position(history, transaction_index, log_index)
         WHERE event.chain_id = $1
           AND event.raw_fact_ref ->> 'history' = position.history",
    )
    .bind(CHAIN)
    .bind(format!("{CHAIN}-permission-history-transaction"))
    .execute(scratch.pool())
    .await?
    .rows_affected();
    assert_eq!(positioned, 2);

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let snapshot: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object(
             'permission', (
                 SELECT to_jsonb(permission) - 'last_recomputed_at' - 'inserted_at'
                 FROM permissions_current permission
                 WHERE resource_id = $1 AND subject = lower($2) AND scope = 'resource'
             ),
             'resource_summary', (
                 SELECT to_jsonb(summary) - 'last_recomputed_at'
                 FROM permissions_current_resource_summary summary
                 WHERE resource_id = $1
             )
         )",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(OWNER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        snapshot,
        json!({
            "permission": {
                "canonicality_summary": {
                    "state": "canonical",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "chain_positions": {
                    "block_hash": "project-fixture-block-3",
                    "block_number": 3,
                    "log_index": 9,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3,
                    "transaction_index": 5
                },
                "effective_powers": ["resource_control", "set_resolver"],
                "grant_source": {"kind": "tie_break_winner"},
                "inheritance_path": ["tie_break_winner"],
                "manifest_version": 3,
                "provenance": {
                    "chain_id": "project-fixture",
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "derivation_kind": "permissions_current_rebuild",
                    "manifest_versions": [
                        {
                            "manifest_version": 1,
                            "source_family": "ens_v1_registrar_l1",
                            "source_manifest_id": null
                        },
                        {
                            "manifest_version": 3,
                            "source_family": "ens_v1_registry_l1",
                            "source_manifest_id": null
                        },
                        {
                            "manifest_version": 2,
                            "source_family": "ens_v1_registry_l1",
                            "source_manifest_id": null
                        }
                    ],
                    "normalized_event_ids": [6, 13, 14],
                    "permission_manifest_versions": [
                        {
                            "manifest_version": 1,
                            "source_family": "ens_v1_registrar_l1",
                            "source_manifest_id": null
                        },
                        {
                            "manifest_version": 3,
                            "source_family": "ens_v1_registry_l1",
                            "source_manifest_id": null
                        },
                        {
                            "manifest_version": 2,
                            "source_family": "ens_v1_registry_l1",
                            "source_manifest_id": null
                        }
                    ],
                    "raw_fact_refs": [
                        {},
                        {"history": "tie_break_winner"},
                        {"history": "later_inserted_loser"}
                    ]
                },
                "resource_id": RESOURCE,
                "revocation_source": null,
                "scope": "resource",
                "scope_detail": {"kind": "resource"},
                "scope_kind": "resource",
                "subject": OWNER,
                "transfer_behavior": {"mode": "retain"}
            },
            "resource_summary": {
                "authority_kind": "registrar",
                "canonicality_summary": {
                    "state": "canonical_lineage",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "chain_positions": {
                    "block_hash": "project-fixture-block-3",
                    "block_number": 3,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "manifest_version": 3,
                "provenance": {
                    "authority_event_id": 13,
                    "chain_id": "project-fixture",
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "history": "tie_break_winner",
                    "registry_binding_clear_event_id": 3
                },
                "registry_binding_chain_positions": null,
                "registry_binding_provenance": null,
                "registry_contract": null,
                "registry_owner": null,
                "resource_id": RESOURCE,
                "root_resource_id": null,
                "support_status": "unsupported",
                "unsupported_reason": "operator_approval_surfaces_not_ingested"
            }
        })
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn marker_only_permission_is_retained_without_a_grant_source() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_permission_marker").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v2_registry_l1",
        json!({
            "subject":OWNER, "scope":{"kind":"resource"},
            "effective_powers":["was_reserved"], "grant_source":null,
            "revocation_source":null, "inheritance_path":[], "transfer_behavior":"retain"
        }),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let row: (Value, Option<Value>) = sqlx::query_as(
        "SELECT effective_powers, grant_source FROM permissions_current
         WHERE resource_id = $1 AND subject = lower($2)",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(OWNER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(row, (json!(["was_reserved"]), Some(json!({}))));
    scratch.cleanup().await
}

#[tokio::test]
async fn shadow_identity_labels_retain_bytes_and_decode_only_exact_text() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_shadow_labels").await?;
    seed_project_fixture(scratch.pool()).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    let labels: Vec<ChildLabelRow> = sqlx::query_as(
        "SELECT raw_label, decoded_label, raw_name, decoded_name
         FROM children_current
         ORDER BY child_logical_name_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        labels,
        vec![
            (
                b"alice".to_vec(),
                Some("alice".into()),
                b"alice.eth".to_vec(),
                Some("alice.eth".into())
            ),
            (
                vec![0xff, 0x00],
                None,
                vec![0xff, 0x00, b'.', b'e', b't', b'h'],
                None
            )
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn primary_name_builder_preserves_success_invalid_blank_and_byte_only_claims() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_primary_claims").await?;
    seed_project_fixture(scratch.pool()).await?;
    for (index, address, raw_name) in [
        (
            10,
            "0x0000000000000000000000000000000000000010",
            Some("Alice.eth"),
        ),
        (
            11,
            "0x0000000000000000000000000000000000000011",
            Some("bad name.eth"),
        ),
        (
            12,
            "0x0000000000000000000000000000000000000012",
            Some("  \t"),
        ),
        (13, "0x0000000000000000000000000000000000000013", None),
    ] {
        insert_event(
            scratch.pool(),
            CHAIN,
            3,
            None,
            None,
            "ReverseChanged",
            "ens_v1_reverse_l1",
            json!({"address":address,"coin_type":"60","namespace":"ens"}),
            json!({}),
        )
        .await?;
        let mut claim = json!({
            "record_key":"name",
            "primary_claim_source":{
                "address":address,
                "coin_type":"60",
                "namespace":"ens"
            }
        });
        if let Some(raw_name) = raw_name {
            claim["raw_name"] = json!(raw_name);
        } else {
            claim["raw_name_bytes"] = json!({"encoding":"hex","bytes":"0xff00"});
        }
        insert_event(
            scratch.pool(),
            CHAIN,
            3,
            None,
            None,
            "RecordChanged",
            "ens_v1_reverse_l1",
            claim,
            json!({"fixture_index":index}),
        )
        .await?;
    }
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        None,
        None,
        "RecordChanged",
        "ens_v1_reverse_l1",
        json!({
            "raw_name":"stale.eth",
            "primary_claim_source":{
                "address":OWNER,
                "coin_type":"60",
                "namespace":"ens",
                "claim_provenance":{"history":"stale"}
            }
        }),
        json!({"history":"stale"}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        None,
        None,
        "RecordChanged",
        "ens_v1_reverse_l1",
        json!({
            "raw_name":"latest.eth",
            "primary_claim_source":{
                "address":OWNER,
                "coin_type":"60",
                "namespace":"ens",
                "claim_provenance":{
                    "history":"latest",
                    "source_family":"ens_v1_reverse_l1"
                }
            }
        }),
        json!({"history":"latest"}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let full_claims: Value = sqlx::query_scalar(
        "SELECT jsonb_agg(
             to_jsonb(primary_name) -
                 'reverse_hydration_attempted_block_number' -
                 'reverse_hydration_attempted_block_hash' -
                 'reverse_hydration_attempt_ordinal'
             ORDER BY address, coin_type, namespace)
         FROM primary_names_current primary_name",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        full_claims,
        json!([
            {
                "address": "0x0000000000000000000000000000000000000010",
                "claim_name_is_normalized": false,
                "claim_provenance": {
                    "chain_id": "project-fixture",
                    "claim_event_id": 14,
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "reverse_event_id": 13,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "claim_status": "success",
                "coin_type": "60",
                "namespace": "ens",
                "raw_claim_name": "Alice.eth",
                "unsupported_reason": null
            },
            {
                "address": "0x0000000000000000000000000000000000000011",
                "claim_name_is_normalized": false,
                "claim_provenance": {
                    "chain_id": "project-fixture",
                    "claim_event_id": 16,
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "reverse_event_id": 15,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "claim_status": "invalid_name",
                "coin_type": "60",
                "namespace": "ens",
                "raw_claim_name": "bad name.eth",
                "unsupported_reason": null
            },
            {
                "address": "0x0000000000000000000000000000000000000012",
                "claim_name_is_normalized": false,
                "claim_provenance": {
                    "chain_id": "project-fixture",
                    "claim_event_id": 18,
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "reverse_event_id": 17,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "claim_status": "not_found",
                "coin_type": "60",
                "namespace": "ens",
                "raw_claim_name": null,
                "unsupported_reason": null
            },
            {
                "address": "0x0000000000000000000000000000000000000013",
                "claim_name_is_normalized": false,
                "claim_provenance": {
                    "chain_id": "project-fixture",
                    "claim_event_id": 20,
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "reverse_event_id": 19,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "claim_status": "unsupported",
                "coin_type": "60",
                "namespace": "ens",
                "raw_claim_name": null,
                "unsupported_reason": "claim_name_not_decodable"
            },
            {
                "address": OWNER,
                "claim_name_is_normalized": true,
                "claim_provenance": {
                    "chain_id": "project-fixture",
                    "claim_event_id": 22,
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "history": "latest",
                    "reverse_event_id": 9,
                    "source_family": "ens_v1_reverse_l1",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "claim_status": "success",
                "coin_type": "60",
                "namespace": "ens",
                "raw_claim_name": "latest.eth",
                "unsupported_reason": null
            }
        ])
    );
    let claims: Vec<PrimaryClaimRow> = sqlx::query_as(
        "SELECT address, claim_status, raw_claim_name,
                claim_name_is_normalized, unsupported_reason
         FROM primary_names_current
         WHERE address <> lower($1)
         ORDER BY address",
    )
    .bind(OWNER)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        claims,
        vec![
            (
                "0x0000000000000000000000000000000000000010".into(),
                "success".into(),
                Some("Alice.eth".into()),
                false,
                None
            ),
            (
                "0x0000000000000000000000000000000000000011".into(),
                "invalid_name".into(),
                Some("bad name.eth".into()),
                false,
                None
            ),
            (
                "0x0000000000000000000000000000000000000012".into(),
                "not_found".into(),
                None,
                false,
                None
            ),
            (
                "0x0000000000000000000000000000000000000013".into(),
                "unsupported".into(),
                None,
                false,
                Some("claim_name_not_decodable".into())
            )
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_rejects_an_observed_only_range_end_before_claiming_state() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_observed_redo_end").await?;
    let chain = "project-observed-redo-end";
    seed_lineage(scratch.pool(), chain, 2).await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, 3, to_timestamp(3), 'observed')",
    )
    .bind(chain)
    .bind(block_hash(chain, 3))
    .bind(block_hash(chain, 2))
    .execute(scratch.pool())
    .await?;
    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_project_extent(scratch.pool(), chain, 3).await?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
            Arc::new(ProjectPhase::new(scratch.pool().clone())),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "production-project-observed-redo-end",
        test_timing(),
    )?;

    let error = runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(3, 3)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("observed staging must not become a project redo marker");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("not readable (canonical, safe, or finalized)")
    );
    assert!(!error.to_string().contains("not canonical"));
    let state: (String, bool, Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT phase_status, redo_in_progress, redo_mode, redo_current_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state, ("completed".into(), false, None, None));
    scratch.cleanup().await
}

#[tokio::test]
async fn record_version_boundary_excludes_prior_records_and_keeps_later_records() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_record_version").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RecordVersionChanged",
        "ens_v1_resolver_l1",
        json!({"resolver":RESOLVER,"record_version":"1"}),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:avatar",
            "record_family":"text",
            "selector_key":"avatar",
            "value_retained":true,
            "value":"ipfs://avatar"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let inventory: (String, Value, Value, Value) = sqlx::query_as(
        "SELECT record_version_boundary_key, record_version_boundary,
                selectors, entries
         FROM record_inventory_current",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        inventory.0,
        record_version_boundary_storage_key(&inventory.1, Uuid::parse_str(RESOURCE)?)?
    );
    assert_eq!(
        inventory.1["event_kind"],
        Value::String("RecordVersionChanged".into())
    );
    assert_eq!(inventory.1["logical_name_id"], "ens:0xalice");
    assert_eq!(inventory.1["resource_id"], RESOURCE);
    assert_eq!(
        inventory.2,
        json!([{
            "record_key":"text:avatar",
            "record_family":"text",
            "selector_key":"avatar",
            "cacheable":true
        }])
    );
    assert_eq!(inventory.3.as_array().map(Vec::len), Some(1));
    assert_eq!(inventory.3[0]["record_key"], "text:avatar");
    scratch.cleanup().await
}

#[tokio::test]
async fn resolver_clear_retracts_inventory_instead_of_reviving_an_older_pointer() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_resolver_clear").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":"0x0000000000000000000000000000000000000000"}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let inventory_count: i64 = sqlx::query_scalar("SELECT count(*) FROM record_inventory_current")
        .fetch_one(scratch.pool())
        .await?;
    let zero_resolver_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM resolver_current
         WHERE resolver_address = '0x0000000000000000000000000000000000000000'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!((inventory_count, zero_resolver_count), (0, 0));
    scratch.cleanup().await
}

#[tokio::test]
async fn undeclared_v1_resolver_is_explicitly_unsupported_without_code_hash_evidence() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_project_undeclared_resolver").await?;
    seed_project_fixture(scratch.pool()).await?;
    let unknown = "0x00000000000000000000000000000000000000b2";
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":unknown}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let classification: (String, Option<String>, String) = sqlx::query_as(
        "SELECT support_status, unsupported_reason,
                declared_summary -> 'classification' ->> 'basis'
         FROM resolver_current WHERE resolver_address = $1",
    )
    .bind(unknown)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        classification,
        (
            "unsupported".into(),
            Some("resolver_not_declared".into()),
            "manifest_declared_address".into()
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn exact_name_support_keeps_conflicting_and_unpromoted_v2_status_explicit() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_name_support").await?;
    let chain = "project-name-support";
    let logical_name_id = seed_dual_open_cross_arm_fixture(scratch.pool(), chain, 4).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    let mixed: (String, Option<String>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        mixed,
        (
            "unsupported".into(),
            Some("conflicting_current_ens_authority".into())
        )
    );

    sqlx::query(
        "DELETE FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v1'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET source_manifest_id = NULL,
             source_family = CASE
             WHEN event_kind IN ('RegistrationGranted', 'RegistrationRenewed')
                 THEN 'ens_v2_registrar_l1'
             ELSE 'ens_v2_registry_l1'
         END
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 5,
            hash: block_hash(chain, 5),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    let shadow: (String, Option<String>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        shadow,
        (
            "unsupported".into(),
            Some("ensv2_exact_name_profile_shadow".into())
        )
    );
    scratch.cleanup().await?;

    let sepolia = ScratchDatabase::create("production_project_name_support_sepolia").await?;
    let sepolia_name =
        seed_dual_open_cross_arm_fixture(sepolia.pool(), "ethereum-sepolia", 4).await?;
    InterpretEngine::new(sepolia.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: "ethereum-sepolia".into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(
        sepolia.pool(),
        "ethereum-sepolia",
        None,
        RunMode::Normal,
        0,
        5,
    )
    .await?;
    let sepolia_reason: String = sqlx::query_scalar(
        "SELECT unsupported_reason FROM name_current WHERE logical_name_id = $1",
    )
    .bind(sepolia_name)
    .fetch_one(sepolia.pool())
    .await?;
    assert_eq!(sepolia_reason, "independent_ens_deployments_overlap");
    sepolia.cleanup().await
}

#[tokio::test]
async fn exact_name_no_proof_mixed_history_ignores_the_remaining_open_arm() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_name_support_selected_arm").await?;
    let chain = "project-name-support-selected-arm";
    let logical_name_id = seed_dual_open_cross_arm_fixture(scratch.pool(), chain, 4).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    sqlx::query(
        "DELETE FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v2'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(scratch.pool())
    .await?;

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    let support: (String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason,
                provenance #>> '{authority_selection,authority_arm}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        support,
        (
            "unsupported".into(),
            Some("conflicting_current_ens_authority".into()),
            None,
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn authority_epoch_rebind_updates_the_current_registration_authority() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_authority_epoch").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityEpochChanged",
        "ens_v1_registry_l1",
        json!({
            "authority_kind":"registry_only",
            "authority_key":"registry:project-fixture:0xalice",
            "registry_owner":OWNER
        }),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let registration: Value = sqlx::query_scalar(
        "SELECT declared_summary -> 'registration'
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(registration["authority_kind"], "registry_only");
    assert_eq!(
        registration["authority_key"],
        "registry:project-fixture:0xalice"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn expiry_fold_ignores_malformed_updates_and_clears_unrepresentable_numbers() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_expiry_fold").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":1_900_000_000_i64}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RegistrationRenewed",
        "ens_v1_registrar_l1",
        json!({"expiry":"not-a-number"}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let summary: Value = sqlx::query_scalar(
        "SELECT declared_summary
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(summary["registration"]["expiry"], 1_900_000_000_i64);
    assert_eq!(summary["control"]["expiry"], "2030-03-17T17:46:40Z");

    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":u64::MAX}),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let summary: Value = sqlx::query_scalar(
        "SELECT declared_summary
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(summary["registration"]["expiry"], Value::Null);
    assert_eq!(
        summary["registration"]["latest_event_kind"],
        "ExpiryChanged"
    );
    assert_eq!(summary["control"]["expiry"], Value::Null);
    scratch.cleanup().await
}

#[tokio::test]
async fn permission_support_marks_approvals_partial_without_hiding_known_controllers() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_project_permission_support").await?;
    seed_project_fixture(scratch.pool()).await?;
    for (resource_id, authority_kind) in [
        ("00000000-0000-0000-0000-000000000021", "registrar"),
        ("00000000-0000-0000-0000-000000000022", "registry"),
        ("00000000-0000-0000-0000-000000000023", "registry_only"),
        ("00000000-0000-0000-0000-000000000024", "registry_owner"),
        ("00000000-0000-0000-0000-000000000025", "registrant"),
        ("00000000-0000-0000-0000-000000000026", "resolver"),
        ("00000000-0000-0000-0000-000000000027", "ens_v2_registry"),
        ("00000000-0000-0000-0000-000000000028", "wrapper"),
        ("00000000-0000-0000-0000-000000000029", "future_authority"),
    ] {
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number,
                 provenance, canonicality_state
             ) VALUES (
                 $1, $2, $3, 1,
                 jsonb_build_object(
                     'authority_kind', $4::text,
                     'source_family', 'ens_v1_registrar_l1',
                     'manifest_version', 1
                 ),
                 'canonical'
             )",
        )
        .bind(Uuid::parse_str(resource_id)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .bind(authority_kind)
        .execute(scratch.pool())
        .await?;
    }

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let support: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT authority_kind, support_status, unsupported_reason
         FROM permissions_current_resource_summary
         WHERE resource_id BETWEEN
             '00000000-0000-0000-0000-000000000021'::uuid AND
             '00000000-0000-0000-0000-000000000029'::uuid
         ORDER BY resource_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        support,
        vec![
            (
                "registrar".into(),
                "unsupported".into(),
                Some("operator_approval_surfaces_not_ingested".into())
            ),
            (
                "registry".into(),
                "unsupported".into(),
                Some("operator_approval_surfaces_not_ingested".into())
            ),
            (
                "registry_only".into(),
                "unsupported".into(),
                Some("operator_approval_surfaces_not_ingested".into())
            ),
            (
                "registry_owner".into(),
                "unsupported".into(),
                Some("operator_approval_surfaces_not_ingested".into())
            ),
            (
                "registrant".into(),
                "unsupported".into(),
                Some("operator_approval_surfaces_not_ingested".into())
            ),
            (
                "resolver".into(),
                "unsupported".into(),
                Some("operator_approval_surfaces_not_ingested".into())
            ),
            (
                "ens_v2_registry".into(),
                "unsupported".into(),
                Some("operator_approval_surfaces_not_ingested".into())
            ),
            (
                "wrapper".into(),
                "unsupported".into(),
                Some("ensv1_wrapper_holder_permissions_not_projected".into())
            ),
            (
                "future_authority".into(),
                "unsupported".into(),
                Some("resource_permission_authority_not_projected".into())
            ),
        ]
    );
    let controller_support: (String, Option<String>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason
         FROM address_names_current
         WHERE logical_name_id = 'ens:0xalice'
           AND relation = 'effective_controller'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(controller_support, ("supported".into(), None));

    let readable_controller_query = format!(
        "WITH {READABLE_REVERSE_IDENTITY_CTES}
         SELECT EXISTS (
             SELECT 1 FROM readable_relations
             WHERE address = $1 AND relation = 'effective_controller'
         )"
    );
    let readable_controller: bool = sqlx::query_scalar(&readable_controller_query)
        .bind(OWNER)
        .fetch_one(scratch.pool())
        .await?;
    assert!(
        readable_controller,
        "partial permission enumeration must not hide a known effective controller"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn retained_name_and_resolver_summary_sections_are_projected() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_retained_summaries").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_resolver_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resolver","chain_id":CHAIN,"resolver_address":RESOLVER},
            "effective_powers":["resolver_control"],
            "grant_source":{"kind":"fixture"},
            "inheritance_path":[],
            "transfer_behavior":"retain"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AliasChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "active":true,
            "from_name":"alias.alice.eth",
            "to_name":"alice.eth",
            "to_logical_name_id":"ens:0xalice",
            "to_resource_id":RESOURCE
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let summaries: (Value, Value) = sqlx::query_as(
        "SELECT
             (SELECT declared_summary FROM name_current
              WHERE logical_name_id = 'ens:0xalice'),
             (SELECT declared_summary FROM resolver_current
              WHERE resolver_address = $1)",
    )
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;

    assert_eq!(summaries.0["registration"]["registrant"], OWNER);
    assert_eq!(summaries.0["control"]["registry_owner"], OWNER);
    assert_eq!(summaries.0["resolver"]["address"], RESOLVER);
    assert_eq!(summaries.0["record_inventory"]["status"], "unsupported");
    assert!(summaries.0["history"]["surface_head"].is_object());
    assert!(summaries.0["history"]["resource_head"].is_object());

    assert_eq!(
        summaries.1["classification"]["basis"],
        "manifest_declared_address"
    );
    for section in [
        "bindings",
        "aliases",
        "permissions",
        "role_holders",
        "event_summary",
    ] {
        assert_eq!(summaries.1[section]["status"], "unsupported");
        assert_eq!(
            summaries.1[section]["unsupported_reason"],
            "resolver_binding_enumeration_not_projected"
        );
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn ens_v1_release_retains_its_last_epoch_fields() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_v1_release_fields").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RegistrationReleased",
        "ens_v1_registrar_l1",
        json!({"status":"released","released_at":3}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM name_current
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;

    assert_eq!(summary["registration"]["status"], "released");
    assert_eq!(summary["registration"]["registrant"], OWNER);
    assert_eq!(summary["resolver"]["address"], RESOLVER);
    scratch.cleanup().await
}

#[tokio::test]
async fn name_current_projects_retained_alias_resolution_topology() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_name_alias_topology").await?;
    seed_project_fixture(scratch.pool()).await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET binding_kind = 'resolver_alias_path'
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AliasChanged",
        "ens_v2_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "active":true,
            "alias_state":"active",
            "from_name":"alice.eth",
            "from_namehash":"0xalice",
            "to_name":"profile.alice.eth",
            "to_namehash":"0xprofile-alice",
            "to_logical_name_id":"ens:0xprofile-alice",
            "to_resource_id":RESOURCE
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let topology: Value = sqlx::query_scalar(
        "SELECT declared_summary -> 'topology'
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;

    assert_eq!(
        topology["registry_path"][0]["logical_name_id"],
        "ens:0xalice"
    );
    assert_eq!(topology["resolver_path"][0]["address"], RESOLVER);
    assert_eq!(
        topology["alias"]["final_target"]["logical_name_id"],
        "ens:0xprofile-alice"
    );
    assert_eq!(topology["wildcard"]["source"], Value::Null);
    assert_eq!(topology["transport"]["source_chain_id"], Value::Null);
    assert_eq!(
        topology["version_boundaries"]["record_version_boundary"]["resource_id"],
        RESOURCE
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_ignores_row_local_readable_wildcard_binding_on_orphaned_lineage() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_project_orphaned_wildcard_scope").await?;
    seed_project_fixture(scratch.pool()).await?;

    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, 'project-fixture-losing-block-1', NULL, 1,
                   to_timestamp(1), 'orphaned')",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET binding_kind = 'observed_wildcard_path',
             block_hash = 'project-fixture-losing-block-1'
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .execute(scratch.pool())
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query(
        "UPDATE name_current
         SET raw_name = 'sentinel.eth'
         WHERE logical_name_id = 'ens:0xeth'",
    )
    .execute(scratch.pool())
    .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;

    let retained_raw_name: String =
        sqlx::query_scalar("SELECT raw_name FROM name_current WHERE logical_name_id = 'ens:0xeth'")
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(retained_raw_name, "sentinel.eth");
    let binding_state: String = sqlx::query_scalar(
        "SELECT canonicality_state::TEXT
         FROM surface_bindings
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(binding_state, "canonical");

    scratch.cleanup().await
}

#[tokio::test]
async fn alias_projection_keeps_only_latest_state_and_honors_tombstones() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_alias_tombstone").await?;
    seed_basenames_project_fixture(scratch.pool()).await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET binding_kind = 'resolver_alias_path'
         WHERE logical_name_id = 'basenames:0xalice-base'",
    )
    .execute(scratch.pool())
    .await?;
    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        2,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "AliasChanged",
        "basenames_base_resolver",
        1,
        json!({
            "resolver":BASENAMES_RESOLVER,
            "active":true,
            "from_name":"alice.base.eth",
            "from_namehash":"0xalice-base",
            "to_name":"old.base.eth",
            "to_logical_name_id":"basenames:0xold-base",
            "to_resource_id":BASENAMES_RESOURCE
        }),
        json!({"emitting_address":BASENAMES_RESOLVER}),
    )
    .await?;
    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        3,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "AliasChanged",
        "basenames_base_resolver",
        1,
        json!({
            "resolver":BASENAMES_RESOLVER,
            "active":true,
            "from_name":"alice.base.eth",
            "from_namehash":"0xalice-base",
            "to_name":"new.base.eth",
            "to_logical_name_id":"basenames:0xnew-base",
            "to_resource_id":BASENAMES_RESOURCE
        }),
        json!({"emitting_address":BASENAMES_RESOLVER}),
    )
    .await?;

    run_project(scratch.pool(), BASE_CHAIN, None, RunMode::Normal, 0, 3).await?;
    let (topology, aliases): (Value, Value) = sqlx::query_as(
        "SELECT
             (SELECT declared_summary -> 'topology'
              FROM name_current
              WHERE logical_name_id = 'basenames:0xalice-base'),
             (SELECT declared_summary -> 'aliases' -> 'items'
              FROM resolver_current
              WHERE chain_id = $1 AND resolver_address = $2)",
    )
    .bind(BASE_CHAIN)
    .bind(BASENAMES_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        topology["alias"]["final_target"]["logical_name_id"],
        "basenames:0xnew-base"
    );
    let alias_events = aliases
        .as_array()
        .expect("resolver aliases are an array")
        .iter()
        .filter(|item| item["latest_event_kind"] == "AliasChanged")
        .collect::<Vec<_>>();
    assert_eq!(alias_events.len(), 1);
    assert_eq!(
        alias_events[0]["to_logical_name_id"],
        "basenames:0xnew-base"
    );

    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        4,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "AliasChanged",
        "basenames_base_resolver",
        1,
        json!({
            "resolver":BASENAMES_RESOLVER,
            "active":false,
            "alias_state":"removed",
            "from_name":"alice.base.eth",
            "from_namehash":"0xalice-base"
        }),
        json!({"emitting_address":BASENAMES_RESOLVER}),
    )
    .await?;
    run_project(
        scratch.pool(),
        BASE_CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(BASE_CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let (topology, aliases): (Option<Value>, Value) = sqlx::query_as(
        "SELECT
             (SELECT declared_summary -> 'topology'
              FROM name_current
              WHERE logical_name_id = 'basenames:0xalice-base'),
             (SELECT declared_summary -> 'aliases' -> 'items'
              FROM resolver_current
              WHERE chain_id = $1 AND resolver_address = $2)",
    )
    .bind(BASE_CHAIN)
    .bind(BASENAMES_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert!(topology.is_none() || topology == Some(Value::Null));
    assert!(
        aliases
            .as_array()
            .expect("resolver aliases are an array")
            .iter()
            .all(|item| item["latest_event_kind"] != "AliasChanged")
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn basenames_projection_retains_execution_admission_and_both_chain_positions() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_basenames_support").await?;
    seed_basenames_project_fixture(scratch.pool()).await?;
    run_project(scratch.pool(), BASE_CHAIN, None, RunMode::Normal, 0, 2).await?;

    let row = load_name_current_row(scratch.pool(), "basenames:0xalice-base").await?;
    assert!(
        row.provenance["manifest_versions"]
            .as_array()
            .is_some_and(|versions| versions.iter().any(|version| {
                version["source_family"] == "basenames_execution"
                    && version["manifest_version"] == 2
                    && version["chain"] == ETHEREUM_CHAIN
                    && version["deployment_epoch"] == "basenames_v1"
            }))
    );
    assert!(row.chain_positions.as_object().is_some_and(|positions| {
        positions
            .values()
            .any(|position| position["chain_id"] == BASE_CHAIN)
            && positions
                .values()
                .any(|position| position["chain_id"] == ETHEREUM_CHAIN)
    }));

    assert_eq!(
        row.declared_summary["topology"]["transport"]["contract_address"], BASENAMES_L1_RESOLVER,
        "Project must publish the domain serializer's lowercase transport address"
    );
    assert!(
        resolution_verified_support_boundary(&row, None).is_some(),
        "projected Basenames rows must remain inside the retained verified-resolution support class"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn basenames_ownerless_serving_retains_verified_transport_support() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_basenames_ownerless").await?;
    seed_basenames_project_fixture(scratch.pool()).await?;
    sqlx::query("UPDATE surface_bindings SET active_to = to_timestamp(3)")
        .execute(scratch.pool())
        .await?;
    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        3,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "AuthorityTransferred",
        "basenames_base_registry",
        1,
        json!({
            "owner":"0x0000000000000000000000000000000000000000",
            "owner_getter":"0x0000000000000000000000000000000000000000",
            "owner_getter_reason":"literal_zero"
        }),
        json!({"emitting_address":"0x4200000000000000000000000000000000000002"}),
    )
    .await?;
    run_project(scratch.pool(), BASE_CHAIN, None, RunMode::Normal, 0, 3).await?;

    let row = load_name_current_row(scratch.pool(), "basenames:0xalice-base").await?;
    assert_eq!(row.resource_id, None);
    assert_eq!(row.surface_binding_id, None);
    assert_eq!(row.binding_kind, None);
    assert_eq!(row.declared_summary["topology"]["registry_path"], json!([]));
    assert_eq!(
        row.serving_resource_id,
        Some(Uuid::parse_str(BASENAMES_RESOURCE)?)
    );
    assert!(
        resolution_verified_support_boundary(&row, None).is_some(),
        "ownerless Basenames serving must retain execution manifest provenance, both chain positions, and transport topology: {row:?}"
    );

    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        4,
        None,
        None,
        "RecordVersionChanged",
        "basenames_base_resolver",
        1,
        json!({"node":"0xalice-base", "record_version":"1"}),
        json!({"emitting_address":BASENAMES_RESOLVER}),
    )
    .await?;
    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        4,
        None,
        None,
        "RecordChanged",
        "basenames_base_resolver",
        1,
        json!({
            "node":"0xalice-base",
            "record_family":"text",
            "record_key":"text:description",
            "selector_key":"description",
            "value":"readable after version"
        }),
        json!({"emitting_address":BASENAMES_RESOLVER}),
    )
    .await?;
    run_project(
        scratch.pool(),
        BASE_CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(BASE_CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let (inventory_boundary, inventory_value): (Value, String) = sqlx::query_as(
        "SELECT record_version_boundary, entries -> 0 ->> 'value'
         FROM record_inventory_current WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(BASENAMES_RESOURCE)?)
    .fetch_one(scratch.pool())
    .await?;
    let row = load_name_current_row(scratch.pool(), "basenames:0xalice-base").await?;
    assert_eq!(inventory_value, "readable after version");
    assert_eq!(
        row.declared_summary["topology"]["version_boundaries"]["record_version_boundary"],
        inventory_boundary,
        "verified Basenames lookup requires topology and inventory to select the same record boundary"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn target_block_uses_the_last_same_block_surface_binding() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_same_block_binding").await?;
    seed_project_fixture(scratch.pool()).await?;
    let successor_resource = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 3, 'canonical')",
    )
    .bind(successor_resource)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 3))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET active_to = to_timestamp(3) + interval '5 microseconds'
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, 'ens:0xalice', $2, 'declared_registry_path',
                   'ens_v1', to_timestamp(3) + interval '5 microseconds', $3, $4, 3,
                   'canonical')",
    )
    .bind(Uuid::new_v4())
    .bind(successor_resource)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 3))
    .execute(scratch.pool())
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let projected_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM name_current
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(projected_resource, successor_resource);
    scratch.cleanup().await
}

#[tokio::test]
async fn shared_ensv1_resolver_keeps_fan_in_sections_explicitly_unsupported() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_shared_v1_resolver").await?;
    seed_project_fixture(scratch.pool()).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    let (support_status, summary): (String, Value) = sqlx::query_as(
        "SELECT support_status, declared_summary
         FROM resolver_current WHERE resolver_address = $1",
    )
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(support_status, "supported");
    for section in [
        "bindings",
        "aliases",
        "permissions",
        "role_holders",
        "event_summary",
    ] {
        assert_eq!(summary[section]["status"], "unsupported", "{section}");
        assert_eq!(
            summary[section]["unsupported_reason"], "resolver_binding_enumeration_not_projected",
            "{section}"
        );
        assert!(summary[section].get("count").is_none(), "{section}");
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn resolver_embedded_collections_report_totals_and_cap_samples() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_resolver_summary_cap").await?;
    seed_basenames_project_fixture(scratch.pool()).await?;

    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         )
         SELECT ('10000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0'))::uuid,
                $1, $2, 1, 'canonical'
         FROM generate_series(1, 100) AS ordinal",
    )
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         )
         SELECT 'basenames:0xsample' || lpad(ordinal::text, 3, '0'),
                'basenames', 'sample-' || lpad(ordinal::text, 3, '0') || '.base.eth',
                ARRAY['sample-' || lpad(ordinal::text, 3, '0'), 'base', 'eth'],
                decode('00', 'hex'), '0xsample' || lpad(ordinal::text, 3, '0'),
                ARRAY['0xsample' || lpad(ordinal::text, 3, '0'), '0xbase', '0xeth'],
                $1, 'active', $2, $3, 1, 'canonical'
         FROM generate_series(1, 100) AS ordinal",
    )
    .bind(NORMALIZER)
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state
         )
         SELECT ('20000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0'))::uuid,
                'basenames:0xsample' || lpad(ordinal::text, 3, '0'),
                ('10000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0'))::uuid,
                CASE WHEN ordinal = 100 THEN 'resolver_alias_path'
                     ELSE 'declared_registry_path' END,
                'basenames', to_timestamp(1), $1, $2, 1, 'canonical'
         FROM generate_series(1, 100) AS ordinal",
    )
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             raw_fact_ref, derivation_kind, canonicality_state, before_state, after_state
         )
         SELECT $1 || ':ResolverChanged:sample:' || ordinal,
                'basenames', 'basenames:0xsample' || lpad(ordinal::text, 3, '0'),
                ('10000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0'))::uuid,
                'ResolverChanged', 'basenames_base_registry', 1, $1, 2, $2,
                '{}'::jsonb, 'ens_v1_unwrapped_authority', 'canonical', '{}'::jsonb,
                jsonb_build_object('resolver', $3::text)
         FROM generate_series(1, 100) AS ordinal",
    )
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 2))
    .bind(BASENAMES_RESOLVER)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             raw_fact_ref, derivation_kind, canonicality_state, before_state, after_state
         )
         SELECT $1 || ':AliasChanged:sample:' || ordinal,
                'basenames', 'basenames:0xsample' || lpad(ordinal::text, 3, '0'),
                ('10000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0'))::uuid,
                'AliasChanged', 'basenames_base_resolver', 1, $1, 3, $2,
                jsonb_build_object('emitting_address', $3::text),
                'ens_v1_unwrapped_authority', 'canonical', '{}'::jsonb,
                jsonb_build_object(
                    'resolver', $3::text, 'active', true,
                    'from_name', 'alias-' || lpad(ordinal::text, 3, '0') || '.base.eth',
                    'to_name', 'sample-' || lpad(ordinal::text, 3, '0') || '.base.eth',
                    'to_logical_name_id',
                        'basenames:0xsample' || lpad(ordinal::text, 3, '0'),
                    'to_resource_id',
                        '10000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0')
                )
         FROM generate_series(1, 100) AS ordinal",
    )
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 3))
    .bind(BASENAMES_RESOLVER)
    .execute(scratch.pool())
    .await?;
    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        3,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "AliasChanged",
        "basenames_base_resolver",
        1,
        json!({
            "resolver":BASENAMES_RESOLVER,
            "active":true,
            "from_name":"alias.alice.base.eth",
            "to_name":"alice.base.eth",
            "to_logical_name_id":"basenames:0xalice-base",
            "to_resource_id":BASENAMES_RESOURCE
        }),
        json!({"emitting_address":BASENAMES_RESOLVER}),
    )
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             raw_fact_ref, derivation_kind, canonicality_state, before_state, after_state
         )
         SELECT $1 || ':PermissionChanged:sample:' || ordinal,
                'basenames', 'basenames:0xsample' || lpad(ordinal::text, 3, '0'),
                ('10000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0'))::uuid,
                'PermissionChanged', 'basenames_base_resolver', 1, $1, 4, $2,
                jsonb_build_object('emitting_address', $3::text),
                'ens_v1_unwrapped_authority', 'canonical', '{}'::jsonb,
                jsonb_build_object(
                    'subject', '0x' || lpad(to_hex(ordinal), 40, '0'),
                    'scope', jsonb_build_object(
                        'kind', 'resolver', 'chain_id', $1::text,
                        'resolver_address', $3::text
                    ),
                    'effective_powers', jsonb_build_array('record_write'),
                    'grant_source', jsonb_build_object('kind', 'fixture'),
                    'inheritance_path', '[]'::jsonb,
                    'transfer_behavior', 'replace_on_authority_change'
                )
         FROM generate_series(1, 100) AS ordinal",
    )
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 4))
    .bind(BASENAMES_RESOLVER)
    .execute(scratch.pool())
    .await?;
    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        4,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "PermissionChanged",
        "basenames_base_resolver",
        1,
        json!({
            "subject":"0x0000000000000000000000000000000000000000",
            "scope":{
                "kind":"resolver",
                "chain_id":BASE_CHAIN,
                "resolver_address":BASENAMES_RESOLVER
            },
            "effective_powers":["record_write","set_resolver"],
            "grant_source":{"kind":"fixture"},
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":BASENAMES_RESOLVER}),
    )
    .await?;

    run_project(scratch.pool(), BASE_CHAIN, None, RunMode::Normal, 0, 4).await?;
    let summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = $2",
    )
    .bind(BASE_CHAIN)
    .bind(BASENAMES_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;

    let bindings = &summary["bindings"];
    assert_eq!(bindings["count"], 101);
    assert_eq!(bindings["total_count"], 101);
    assert_eq!(bindings["sample_limit"], 100);
    assert_eq!(bindings["sample_count"], 100);
    assert_eq!(bindings["truncated"], true);
    let items = bindings["items"].as_array().expect("binding sample array");
    assert_eq!(items.len(), 100);
    assert_eq!(items[0]["raw_name"], "alice.base.eth");
    assert_eq!(items[99]["raw_name"], "sample-099.base.eth");
    assert!(
        items
            .iter()
            .all(|item| item["raw_name"] != "sample-100.base.eth")
    );

    assert_eq!(summary["aliases"]["total_count"], 102);
    assert_eq!(summary["aliases"]["sample_limit"], 100);
    assert_eq!(summary["aliases"]["sample_count"], 100);
    assert_eq!(summary["aliases"]["truncated"], true);
    let aliases = summary["aliases"]["items"]
        .as_array()
        .expect("alias sample array");
    assert_eq!(aliases[0]["binding_kind"], "resolver_alias_path");
    assert_eq!(aliases[0]["raw_name"], "sample-100.base.eth");
    assert_eq!(aliases[1]["from_name"], "alias.alice.base.eth");
    assert_eq!(aliases[99]["from_name"], "alias-098.base.eth");
    assert!(aliases.iter().all(|item| {
        item["from_name"] != "alias-099.base.eth" && item["from_name"] != "alias-100.base.eth"
    }));

    for section in ["permissions", "role_holders"] {
        assert_eq!(summary[section]["total_count"], 101, "{section}");
        assert_eq!(summary[section]["sample_limit"], 100, "{section}");
        assert_eq!(summary[section]["sample_count"], 100, "{section}");
        assert_eq!(summary[section]["truncated"], true, "{section}");
        assert_eq!(
            summary[section]["items"].as_array().map(Vec::len),
            Some(100)
        );
    }
    assert!(
        summary["role_holders"]["items"][0]
            .get("resource_ids")
            .is_none()
    );
    let role_holders = summary["role_holders"]["items"]
        .as_array()
        .expect("role-holder sample array");
    assert_eq!(
        role_holders[0]["subject"],
        "0x0000000000000000000000000000000000000000"
    );
    assert_eq!(
        role_holders[99]["subject"],
        "0x0000000000000000000000000000000000000063"
    );
    assert!(
        role_holders
            .iter()
            .all(|item| { item["subject"] != "0x0000000000000000000000000000000000000064" })
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn children_follow_topology_tombstones_not_surface_suffixes() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_children_tombstone").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xalice",
            "labelhash":"0xalice-label",
            "owner":"0x0000000000000000000000000000000000000000"
        }),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let children: Vec<String> = sqlx::query_scalar(
        "SELECT child_logical_name_id FROM children_current
         ORDER BY child_logical_name_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(children, vec!["ens:0xhostile"]);
    let surface_still_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM name_surfaces
             WHERE logical_name_id = 'ens:0xalice'
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(surface_still_exists);
    scratch.cleanup().await
}

#[tokio::test]
async fn topology_only_child_is_published_then_upgraded_by_a_late_preimage() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_topology_only_child").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 5).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        None,
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xmystery",
            "labelhash":"0xmystery-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;

    let topology_only: TopologyOnlyChildRow = sqlx::query_as(
        "SELECT raw_name, decoded_name, raw_label, decoded_label, namehash, labelhash
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xeth'
           AND child_logical_name_id = 'ens:0xmystery'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        topology_only,
        (
            None,
            None,
            None,
            None,
            "0xmystery".into(),
            "0xmystery-label".into()
        )
    );

    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority
         ) VALUES (
             '0xmystery-label', convert_to('mystery', 'UTF8'), 'mystery', $1,
             true, 'fixture', 1
         )",
    )
    .bind(NORMALIZER)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES (
             'ens:0xmystery', 'ens', 'mystery.eth', ARRAY['mystery','eth'],
             decode('00', 'hex'), '0xmystery',
             ARRAY['0xmystery-label','0xeth'], $1, 'active', $2, $3, 5,
             'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 5))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        5,
        Some("ens:0xmystery"),
        None,
        "PreimageObserved",
        "ens_v1_registry_l1",
        json!({
            "labelhash":"0xmystery-label",
            "raw_labels_hex":["6d797374657279","657468"]
        }),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;

    let upgraded: OptionalChildLabelRow = sqlx::query_as(
        "SELECT raw_name, decoded_name, raw_label, decoded_label
             FROM children_current
             WHERE parent_logical_name_id = 'ens:0xeth'
               AND child_logical_name_id = 'ens:0xmystery'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        upgraded,
        (
            Some(b"mystery.eth".to_vec()),
            Some("mystery.eth".into()),
            Some(b"mystery".to_vec()),
            Some("mystery".into())
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn parent_preimage_incrementally_publishes_an_existing_child_edge() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_parent_preimage_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 5).await?;
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority
         ) VALUES (
             '0xleaf-label', convert_to('leaf', 'UTF8'), 'leaf', $1,
             true, 'fixture', 1
         )",
    )
    .bind(NORMALIZER)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        None,
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xparent",
            "child_node":"0xleaf",
            "labelhash":"0xleaf-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    let absent_before_parent: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS (
             SELECT 1 FROM children_current
             WHERE child_logical_name_id = 'ens:0xleaf'
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(absent_before_parent);

    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES (
             'ens:0xparent', 'ens', 'parent.eth', ARRAY['parent','eth'],
             decode('00', 'hex'), '0xparent',
             ARRAY['0xparent-label','0xeth'], $1, 'active', $2, $3, 5,
             'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 5))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        5,
        Some("ens:0xparent"),
        None,
        "PreimageObserved",
        "ens_v1_registry_l1",
        json!({
            "labelhash":"0xparent-label",
            "raw_labels_hex":["706172656e74","657468"]
        }),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;

    let child: OptionalChildLabelRow = sqlx::query_as(
        "SELECT raw_name, decoded_name, raw_label, decoded_label
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xparent'
           AND child_logical_name_id = 'ens:0xleaf'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        child,
        (
            Some(b"leaf.parent.eth".to_vec()),
            Some("leaf.parent.eth".into()),
            Some(b"leaf".to_vec()),
            Some("leaf".into())
        )
    );
    scratch.cleanup().await
}

/// Seeds one parent-child pair stated on both authority arms, with the ENSv1
/// relation restated at `v1_block` and the child's activated ENSv2 migration
/// boundary at `boundary_block`.
async fn seed_child_authority_fixture(
    pool: &PgPool,
    v1_block: i64,
    boundary_block: i64,
) -> Result<()> {
    let subregistry_instance = Uuid::parse_str("00000000-0000-0000-0000-0000000000e3")?;
    let subregistry_address = "0x00000000000000000000000000000000000000e3";
    for block in 4..=6 {
        insert_lineage_block(pool, CHAIN, block).await?;
    }
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, $2, 'contract')",
    )
    .bind(subregistry_instance)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number
         ) VALUES ($1, $2, $3, 0)",
    )
    .bind(subregistry_instance)
    .bind(CHAIN)
    .bind(subregistry_address)
    .execute(pool)
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xeth"),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":subregistry_address}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xalice"),
        None,
        "RegistrationGranted",
        "ens_v2_registry_l1",
        json!({
            "registry_contract_instance_id":subregistry_instance,
            "label":"alice",
            "registrant":OWNER
        }),
        json!({}),
    )
    .await?;
    // The ENSv1 relation is restated later than the ENSv2 one, so a recency
    // tie-break would publish ENSv1.
    insert_event(
        pool,
        CHAIN,
        v1_block,
        Some("ens:0xalice"),
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xalice",
            "labelhash":"0xalice-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    insert_migration_boundary(pool, "ens:0xalice", boundary_block).await
}

async fn insert_migration_boundary(pool: &PgPool, child: &str, block: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, raw_fact_ref, derivation_kind,
             canonicality_state, before_state, after_state, migration_correlation_ids,
             consumer_visibility
         ) VALUES (
             $1, 'ens', $2, 'MigrationApplied', 'ens_v2_migration_l1', 1, $3, $4, $5,
             $6, 0, 0, '{}'::jsonb, 'ens_v2_migration', 'canonical',
             jsonb_build_object('authority_epoch', 'ens_v1'),
             jsonb_build_object(
                 'migration_path', 'locked_child',
                 'successor_binding', jsonb_build_object('authority_epoch', 'ens_v2')
             ),
             ARRAY['child-authority-fixture']::text[], 'activated'
         )",
    )
    .bind(format!("{CHAIN}:MigrationApplied:{child}"))
    .bind(child)
    .bind(CHAIN)
    .bind(block)
    .bind(block_hash(CHAIN, block))
    .bind(format!("{CHAIN}-child-boundary-tx"))
    .execute(pool)
    .await?;
    Ok(())
}

async fn child_relation(pool: &PgPool) -> Result<Option<(Option<String>, Option<String>)>> {
    Ok(sqlx::query_as(
        "SELECT owner, registrant FROM children_current
         WHERE parent_logical_name_id = 'ens:0xeth'
           AND child_logical_name_id = 'ens:0xalice'",
    )
    .fetch_optional(pool)
    .await?)
}

// The child's own authority selects the published relation. The ENSv1 relation is
// newer here, so a surviving recency tie-break would publish it.
#[tokio::test]
async fn a_proven_child_publishes_its_ens_v2_relation_over_a_newer_ens_v1_one() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_child_authority").await?;
    seed_project_fixture(scratch.pool()).await?;
    seed_child_authority_fixture(scratch.pool(), 2, 3).await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let published = child_relation(scratch.pool())
        .await?
        .expect("the proven child publishes exactly one relation");
    assert_eq!(
        published,
        (None, Some(OWNER.to_owned())),
        "the ENSv2 relation is published and the retained ENSv1 one is residue"
    );
    scratch.cleanup().await
}

// Release removes the child rather than restoring the ENSv1 relation the migration
// left behind.
#[tokio::test]
async fn a_released_v2_child_publishes_no_relation_and_never_falls_back() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_child_released").await?;
    seed_project_fixture(scratch.pool()).await?;
    seed_child_authority_fixture(scratch.pool(), 2, 3).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        None,
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({
            "registry_contract_instance_id":"00000000-0000-0000-0000-0000000000e3"
        }),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    assert_eq!(
        child_relation(scratch.pool()).await?,
        None,
        "a released ENSv2 child publishes nothing"
    );
    scratch.cleanup().await
}

// An ENSv1 relation asserted after the child's ENSv2 authority began cannot be
// reconciled as residue, and selection must not silently drop it.
#[tokio::test]
async fn a_post_boundary_ens_v1_child_relation_blocks_mainnet_publication() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_child_dual_current").await?;
    seed_project_fixture(scratch.pool()).await?;
    seed_child_authority_fixture(scratch.pool(), 5, 3).await?;

    let failure = run_project_phase(scratch.pool(), CHAIN, 5)
        .await
        .expect_err("a post-boundary ENSv1 child relation must not publish");
    assert!(
        failure
            .to_string()
            .contains("after the child's ENSv2 authority began"),
        "unexpected failure: {failure}"
    );
    let published: i64 = sqlx::query_scalar("SELECT count(*) FROM children_current")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(published, 0, "an aborted generation publishes no rows");

    let rows = generation_failure_rows(scratch.pool(), CHAIN).await?;
    assert_eq!(rows.len(), 1);
    let (_, _, _, failure_kind, fingerprint, name, evidence) = rows[0].clone();
    assert_eq!(failure_kind, DUAL_CURRENT_CHILD_AUTHORITY);
    assert_eq!(fingerprint.len(), 64);
    assert_eq!(name, "ens:0xalice");
    assert_eq!(evidence["parent_logical_name_id"], json!("ens:0xeth"));
    assert_eq!(
        evidence["authority_proof_kind"],
        json!("migration_authority_transition")
    );
    assert_eq!(evidence["predecessor"]["authority_arm"], json!("ens_v1"));
    assert_eq!(evidence["successor"]["authority_arm"], json!("ens_v2"));
    // The stable event keys are what stays resolvable once a redo drops the generated ids.
    for side in ["predecessor", "successor"] {
        let identity = evidence[side]["event_identity"]
            .as_str()
            .unwrap_or_else(|| panic!("{side} evidence keeps its event identity"));
        let known: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM normalized_events WHERE event_identity = $1)",
        )
        .bind(identity)
        .fetch_one(scratch.pool())
        .await?;
        assert!(known, "{side} event identity {identity} resolves");
    }
    assert!(evidence["predecessor"]["block_number"].is_number());
    assert!(evidence["authority_epoch_start_position"]["block_number"].is_number());
    // The proof's own block identity and canonicality are durable, so the row stays
    // resolvable through lineage once a later reorganization moves the proof.
    let proof = evidence["authority_proof"].clone();
    assert_eq!(proof["proof_kind"], json!("migration_authority_transition"));
    assert_eq!(proof["block_number"], json!(3));
    assert_eq!(proof["block_hash"], json!(block_hash(CHAIN, 3)));
    assert_eq!(proof["canonicality_state"], json!("canonical"));
    assert_eq!(evidence["target"]["canonicality_state"], json!("canonical"));
    let resolvable: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM chain_lineage
             WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
         )",
    )
    .bind(CHAIN)
    .bind(proof["block_number"].as_i64().expect("proof block number"))
    .bind(proof["block_hash"].as_str().expect("proof block hash"))
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        resolvable,
        "the recorded proof block resolves through lineage"
    );

    run_project_phase(scratch.pool(), CHAIN, 5)
        .await
        .expect_err("the retried generation still fails");
    assert_eq!(
        generation_failure_rows(scratch.pool(), CHAIN).await?,
        rows,
        "a retried generation records no second row for the same conflict"
    );
    scratch.cleanup().await
}

/// Seeds the same parent-child pair, but with the child's ENSv2 authority proven by a
/// positive ENSv2 child registration under a migrated parent registry instead of by the
/// child's own migration boundary. The ENSv1 relation is restated at `v1_block`.
async fn seed_positive_child_authority_fixture(
    pool: &PgPool,
    v1_block: i64,
    parent_path: &str,
) -> Result<()> {
    let subregistry_instance = Uuid::parse_str("00000000-0000-0000-0000-0000000000e4")?;
    let subregistry_address = "0x00000000000000000000000000000000000000e4";
    for block in 4..=6 {
        insert_lineage_block(pool, CHAIN, block).await?;
    }
    let registry_manifest = insert_manifest(
        pool,
        CHAIN,
        "ens_v2_registry_l1",
        "tests/project-v2-registry.toml",
        json!({"contracts":[]}),
    )
    .await?;
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, $2, 'contract')",
    )
    .bind(subregistry_instance)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number
         ) VALUES ($1, $2, $3, 0)",
    )
    .bind(subregistry_instance)
    .bind(CHAIN)
    .bind(subregistry_address)
    .execute(pool)
    .await?;
    // The parent's registry was created by its own migration, which is what lets a positive
    // ENSv2 registration under it stand as the child's authority proof.
    sqlx::query(
        "INSERT INTO migration_discovery_associations (
             logical_edge_identity, migration_correlation_id, correlation_kind,
             registry_contract_instance_id, registry_address, source_manifest_id,
             evidence_refs, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, canonicality_state, consumer_visibility,
             interpreter_content_hash
         ) VALUES (
             $1, $2, 'migration_registry_creation', $3, lower($4), $5,
             '[{\"event_identity\":\"positive-child-registry-proof\"}]'::jsonb,
             $6, 1, $7, $8, 0, 0, 'canonical', 'candidate', $9
         )",
    )
    .bind(format!("{CHAIN}:positive-child-registry-edge"))
    .bind(format!("{CHAIN}:positive-child-registry-correlation"))
    .bind(subregistry_instance)
    .bind(subregistry_address)
    .bind(registry_manifest)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .bind(format!("{CHAIN}:positive-child-registry-tx"))
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO discovery_edges (
             chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id,
             discovery_source, admission_basis, source_manifest_id,
             active_from_block_number, active_from_block_hash, canonicality_state,
             provenance
         ) VALUES (
             $1, 'registry_announcement', $2, $2, 'RegistryCreated',
             'reachable_from_root', $3, 1, $4, 'canonical',
             '{\"transaction_index\":0,\"log_index\":0}'::jsonb
         )",
    )
    .bind(CHAIN)
    .bind(subregistry_instance)
    .bind(registry_manifest)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xeth"),
        None,
        "MigrationApplied",
        "ens_v2_migration_l1",
        json!({
            "successor_binding":{"authority_epoch":"ens_v2"},
            "migration_path":parent_path,
            "successor_registry_contract_instance_id":subregistry_instance,
            "evidence":[{"event_identity":"positive-child-registry-proof"}]
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xeth"),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":subregistry_address}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RegistrationGranted",
        "ens_v2_registry_l1",
        json!({
            "registry_contract_instance_id":subregistry_instance,
            "status":"registered",
            "label":"alice",
            "registrant":OWNER
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":65536,"wrapper_state":"emancipated"}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_wrapper_l1",
        json!({"expiry":2_000_000_000_i64}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        v1_block,
        Some("ens:0xalice"),
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xalice",
            "labelhash":"0xalice-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await
}

/// Replays what a redo does to the events in a block range: deletes them and writes them
/// back with identical content. `normalized_event_id` is a generated identity, so every
/// re-inserted row gets a new one while its `event_identity` stays put.
async fn reinsert_events_with_new_ids(pool: &PgPool, chain: &str, to_block: i64) -> Result<()> {
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = 'normalized_events'
           AND is_identity = 'NO' AND is_generated = 'NEVER'
         ORDER BY ordinal_position",
    )
    .fetch_all(pool)
    .await?;
    let list = columns.join(", ");
    sqlx::query(&format!(
        "CREATE TABLE redo_replay AS SELECT {list} FROM normalized_events
         WHERE chain_id = $1 AND block_number <= $2"
    ))
    .bind(chain)
    .bind(to_block)
    .execute(pool)
    .await?;
    sqlx::query("DELETE FROM normalized_events WHERE chain_id = $1 AND block_number <= $2")
        .bind(chain)
        .bind(to_block)
        .execute(pool)
        .await?;
    sqlx::query(&format!(
        "INSERT INTO normalized_events ({list}) SELECT {list} FROM redo_replay"
    ))
    .execute(pool)
    .await?;
    sqlx::query("DROP TABLE redo_replay").execute(pool).await?;
    Ok(())
}

async fn clone_event_with_identity(
    pool: &PgPool,
    source_identity: &str,
    cloned_identity: &str,
) -> Result<()> {
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = 'normalized_events'
           AND column_name NOT IN ('normalized_event_id', 'event_identity')
           AND is_identity = 'NO' AND is_generated = 'NEVER'
         ORDER BY ordinal_position",
    )
    .fetch_all(pool)
    .await?;
    let list = columns.join(", ");
    sqlx::query(&format!(
        "INSERT INTO normalized_events (event_identity, {list})
         SELECT $1, {list} FROM normalized_events WHERE event_identity = $2"
    ))
    .bind(cloned_identity)
    .bind(source_identity)
    .execute(pool)
    .await?;
    Ok(())
}

async fn stage_same_position_child_candidates_per_arm(pool: &PgPool) -> Result<()> {
    let v1_source: String = sqlx::query_scalar(
        "SELECT event_identity FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'SubregistryChanged'
           AND source_family = 'ens_v1_registry_l1'
           AND block_number = 5
           AND after_state ->> 'child_node' = '0xalice'",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET event_identity = 'child-conflict-v1-a',
             transaction_hash = 'child-conflict-v1-position',
             transaction_index = 0, log_index = 0
         WHERE event_identity = $1",
    )
    .bind(v1_source)
    .execute(pool)
    .await?;
    clone_event_with_identity(pool, "child-conflict-v1-a", "child-conflict-v1-z").await?;

    let v2_source: String = sqlx::query_scalar(
        "SELECT event_identity FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RegistrationGranted'
           AND source_family = 'ens_v2_registry_l1'
           AND logical_name_id = 'ens:0xalice'
           AND block_number = 1",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET event_identity = 'child-conflict-v2-a',
             transaction_hash = 'child-conflict-v2-position',
             transaction_index = 0, log_index = 0
         WHERE event_identity = $1",
    )
    .bind(v2_source)
    .execute(pool)
    .await?;
    clone_event_with_identity(pool, "child-conflict-v2-a", "child-conflict-v2-z").await?;
    Ok(())
}

async fn reinsert_events_in_reverse_identity_order(
    pool: &PgPool,
    chain: &str,
    to_block: i64,
) -> Result<()> {
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = 'normalized_events'
           AND is_identity = 'NO' AND is_generated = 'NEVER'
         ORDER BY ordinal_position",
    )
    .fetch_all(pool)
    .await?;
    let list = columns.join(", ");
    sqlx::query(&format!(
        "CREATE TABLE redo_replay AS SELECT {list} FROM normalized_events
         WHERE chain_id = $1 AND block_number <= $2"
    ))
    .bind(chain)
    .bind(to_block)
    .execute(pool)
    .await?;
    sqlx::query("DELETE FROM normalized_events WHERE chain_id = $1 AND block_number <= $2")
        .bind(chain)
        .bind(to_block)
        .execute(pool)
        .await?;
    sqlx::query(&format!(
        "INSERT INTO normalized_events ({list})
         SELECT {list} FROM redo_replay ORDER BY event_identity DESC"
    ))
    .execute(pool)
    .await?;
    sqlx::query("DROP TABLE redo_replay").execute(pool).await?;
    Ok(())
}

// The audit row is keyed by a fingerprint of the conflict, so the same semantic conflict
// after a redo must hash to the same value. Generated row ids do not survive a redo; the
// event identities the fingerprint is built from do.
#[tokio::test]
async fn a_replayed_child_conflict_keeps_its_fingerprint_and_records_no_second_row() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_child_replay_fingerprint").await?;
    seed_project_fixture(scratch.pool()).await?;
    seed_child_authority_fixture(scratch.pool(), 5, 3).await?;

    run_project_phase(scratch.pool(), CHAIN, 5)
        .await
        .expect_err("a post-boundary ENSv1 child relation must not publish");
    let first = generation_failure_rows(scratch.pool(), CHAIN).await?;
    assert_eq!(first.len(), 1);
    let before: Vec<i64> = sqlx::query_scalar(
        "SELECT normalized_event_id FROM normalized_events
         WHERE chain_id = $1 AND block_number <= 5 ORDER BY normalized_event_id",
    )
    .bind(CHAIN)
    .fetch_all(scratch.pool())
    .await?;

    reinsert_events_with_new_ids(scratch.pool(), CHAIN, 5).await?;
    let after: Vec<i64> = sqlx::query_scalar(
        "SELECT normalized_event_id FROM normalized_events
         WHERE chain_id = $1 AND block_number <= 5 ORDER BY normalized_event_id",
    )
    .bind(CHAIN)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(after.len(), before.len());
    assert!(
        after.iter().all(|id| !before.contains(id)),
        "the replay must hand every event a new generated id"
    );

    run_project_phase(scratch.pool(), CHAIN, 5)
        .await
        .expect_err("the replayed conflict still blocks publication");
    let second = generation_failure_rows(scratch.pool(), CHAIN).await?;
    assert_eq!(
        second.len(),
        1,
        "the same conflict after a replay records no second audit row"
    );
    assert_eq!(
        second[0].4, first[0].4,
        "the conflict fingerprint survives the replay"
    );
    assert_eq!(second[0].6, first[0].6, "so does its evidence payload");
    scratch.cleanup().await
}

// This synthetic duplicate stages multiple rows for each arm at one exact chain position to
// isolate the final tie-break. A replay may assign their generated database IDs in any order, so
// the chosen conflict witness must follow stable event identity rather than insertion order.
#[tokio::test]
async fn same_position_multi_candidate_child_conflict_is_replay_stable_within_each_arm()
-> Result<()> {
    let scratch =
        ScratchDatabase::create("production_project_child_multi_candidate_replay").await?;
    seed_project_fixture(scratch.pool()).await?;
    seed_child_authority_fixture(scratch.pool(), 5, 3).await?;
    stage_same_position_child_candidates_per_arm(scratch.pool()).await?;

    run_project_phase(scratch.pool(), CHAIN, 5)
        .await
        .expect_err("the multi-candidate conflict must block publication");
    let first = generation_failure_rows(scratch.pool(), CHAIN).await?;
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0].6["predecessor"]["event_identity"],
        "child-conflict-v1-z"
    );
    assert_eq!(
        first[0].6["successor"]["event_identity"],
        "child-conflict-v2-z"
    );

    reinsert_events_in_reverse_identity_order(scratch.pool(), CHAIN, 5).await?;
    run_project_phase(scratch.pool(), CHAIN, 5)
        .await
        .expect_err("the replayed multi-candidate conflict must still block publication");
    let replayed = generation_failure_rows(scratch.pool(), CHAIN).await?;
    assert_eq!(
        replayed.len(),
        1,
        "one semantic conflict has one audit identity"
    );
    assert_eq!(
        replayed[0].4, first[0].4,
        "the witness fingerprint is replay-stable"
    );
    assert_eq!(
        replayed[0].6, first[0].6,
        "the witness evidence is replay-stable"
    );
    scratch.cleanup().await
}

// There is no corresponding same-arm publish fixture with two registry contract instances at one
// address. `contract_instance_addresses_active_idx` admits at most one non-deactivated instance
// for a chain/address, and `ranked_v2_registrations` keeps one current row per child and admitted
// instance, so that proposed second candidate cannot reach `publish` after candidate construction.

// Parent reachability is applied before the child's positive ENSv2 authority proof. An ENSv1
// relation restated beneath an unlocked migrated parent is unreachable, so it cannot become a
// dual-current contradiction or suppress the reachable ENSv2 relation.
#[tokio::test]
async fn parent_reachability_filters_before_positive_v2_child_integrity() -> Result<()> {
    for path in ["unlocked_wrapped", "locked_wrapped"] {
        let scratch =
            ScratchDatabase::create(&format!("production_project_positive_child_{path}")).await?;
        seed_project_fixture(scratch.pool()).await?;
        seed_positive_child_authority_fixture(scratch.pool(), 5, path).await?;

        run_project_phase(scratch.pool(), CHAIN, 5).await?;
        assert_eq!(
            child_relation(scratch.pool()).await?,
            Some((None, Some(OWNER.to_owned()))),
            "the ENSv2 relation publishes after the ENSv1 arm is filtered for {path}"
        );
        assert!(
            generation_failure_rows(scratch.pool(), CHAIN)
                .await?
                .is_empty()
        );
        scratch.cleanup().await?;
    }
    let control = ScratchDatabase::create("production_project_positive_child_control").await?;
    seed_project_fixture(control.pool()).await?;
    seed_positive_child_authority_fixture(control.pool(), 5, "locked_wrapped").await?;
    sqlx::query("DELETE FROM normalized_events WHERE source_family = 'ens_v2_registry_l1' AND event_kind = 'RegistrationGranted' AND logical_name_id = 'ens:0xalice' AND after_state ->> 'registry_contract_instance_id' = '00000000-0000-0000-0000-0000000000e4'")
        .execute(control.pool()).await?;
    run_project(control.pool(), CHAIN, None, RunMode::Normal, 0, 5).await?;
    assert_eq!(
        child_relation(control.pool()).await?,
        Some((Some(OWNER.to_owned()), None))
    );
    control.cleanup().await?;
    Ok(())
}

// Basenames subnames are their own authority arm. The child's authority selects `basenames`,
// so a Basenames-derived relation publishes only because it is staged under that arm.
#[tokio::test]
async fn a_basenames_child_publishes_under_its_own_authority_arm() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_basenames_children").await?;
    seed_basenames_project_fixture(scratch.pool()).await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'basenames:0xbase', 'basenames', 'base.eth', ARRAY['base','eth'],
             decode('00', 'hex'), '0xbase', ARRAY['0xbase','0xeth'], $1, 'active',
             $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        3,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "SubregistryChanged",
        "basenames_base_registry",
        1,
        json!({
            "node":"0xbase",
            "child_node":"0xalice-base",
            "labelhash":"0xalice-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), BASE_CHAIN, None, RunMode::Normal, 0, 3).await?;
    let published: Option<(String,)> = sqlx::query_as(
        "SELECT owner FROM children_current
         WHERE parent_logical_name_id = 'basenames:0xbase'
           AND child_logical_name_id = 'basenames:0xalice-base'",
    )
    .fetch_optional(scratch.pool())
    .await?;
    assert_eq!(
        published,
        Some((OWNER.to_owned(),)),
        "the Basenames relation publishes under the arm its own authority selects"
    );
    scratch.cleanup().await
}

// Sepolia selects the proven relation but does not run the Mainnet publication guardrail.
#[tokio::test]
async fn a_sepolia_child_overlap_selects_without_blocking_publication() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_child_sepolia").await?;
    seed_project_fixture(scratch.pool()).await?;
    seed_child_authority_fixture(scratch.pool(), 5, 3).await?;
    declare_sepolia_post_audit_profile(scratch.pool(), CHAIN).await?;

    run_project_phase(scratch.pool(), CHAIN, 5).await?;
    assert_eq!(
        child_relation(scratch.pool()).await?,
        Some((None, Some(OWNER.to_owned()))),
        "Sepolia still selects the proven ENSv2 relation"
    );
    assert!(
        generation_failure_rows(scratch.pool(), CHAIN)
            .await?
            .is_empty(),
        "Sepolia records no publication-blocking failure"
    );
    scratch.cleanup().await
}

// `alice` carries ENSv1 history and takes an ENSv2 registration with no migration
// proof, so per-child authority omits it as an unsupported mixed corpus rather than
// ranking the two arms. The renewal of that invisible child must still leave the
// clean sibling `bob` published.
#[tokio::test]
async fn incremental_v2_child_renewal_retains_sibling_edges() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_v2_sibling_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    let subregistry_instance = Uuid::parse_str("00000000-0000-0000-0000-0000000000e2")?;
    let subregistry_address = "0x00000000000000000000000000000000000000e2";
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind
         ) VALUES ($1, $2, 'contract')",
    )
    .bind(subregistry_instance)
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number
         ) VALUES ($1, $2, $3, 0)",
    )
    .bind(subregistry_instance)
    .bind(CHAIN)
    .bind(subregistry_address)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES (
             'ens:0xbob', 'ens', 'bob.eth', ARRAY['bob','eth'],
             decode('00', 'hex'), '0xbob', ARRAY['0xbob-label','0xeth'],
             $1, 'active', $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority
         ) VALUES (
             '0xbob-label', convert_to('bob', 'UTF8'), 'bob', $1,
             true, 'fixture', 1
         )",
    )
    .bind(NORMALIZER)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        1,
        Some("ens:0xeth"),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":subregistry_address}),
        json!({}),
    )
    .await?;
    for (logical_name_id, label, registrant) in [
        ("ens:0xalice", "alice", OWNER),
        (
            "ens:0xbob",
            "bob",
            "0x00000000000000000000000000000000000000b0",
        ),
    ] {
        insert_event(
            scratch.pool(),
            CHAIN,
            1,
            Some(logical_name_id),
            None,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            json!({
                "registry_contract_instance_id":subregistry_instance,
                "label":label,
                "registrant":registrant
            }),
            json!({}),
        )
        .await?;
    }
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let before: Vec<String> = sqlx::query_scalar(
        "SELECT child_logical_name_id FROM children_current
         WHERE parent_logical_name_id = 'ens:0xeth'
           AND child_logical_name_id IN ('ens:0xalice', 'ens:0xbob')
         ORDER BY child_logical_name_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(before, vec!["ens:0xbob"]);

    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        None,
        "RegistrationRenewed",
        "ens_v2_registry_l1",
        json!({
            "registry_contract_instance_id":subregistry_instance,
            "registrant":OWNER
        }),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;

    let after: Vec<String> = sqlx::query_scalar(
        "SELECT child_logical_name_id FROM children_current
         WHERE parent_logical_name_id = 'ens:0xeth'
           AND child_logical_name_id IN ('ens:0xalice', 'ens:0xbob')
         ORDER BY child_logical_name_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(after, before);
    scratch.cleanup().await
}

#[tokio::test]
async fn incremental_topology_closure_rebuilds_the_whole_v1_subtree() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_v1_topology_fixpoint").await?;
    let full = ScratchDatabase::create("production_project_v1_topology_fixpoint_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        seed_deep_v1_topology_fixture(pool).await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 6).await?;
    assert_eq!(
        deep_v1_topology_edges(incremental.pool()).await?,
        expected_deep_v1_topology_edges(),
        "the initial full build must contain the complete v1 subtree"
    );

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 6,
            hash: block_hash(CHAIN, 6),
        }),
        RunMode::Normal,
        7,
        7,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 7).await?;

    let incremental_edges = deep_v1_topology_edges(incremental.pool()).await?;
    let full_edges = deep_v1_topology_edges(full.pool()).await?;
    assert_eq!(full_edges, expected_deep_v1_topology_edges());
    assert_eq!(
        incremental_edges, full_edges,
        "a narrow topology tick dropped an edge outside its frozen event set"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn candidate_topology_cannot_widen_incremental_child_scope() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_candidate_topology_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    seed_deep_v1_topology_fixture(scratch.pool()).await?;
    seed_candidate_topology_bridge(scratch.pool()).await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 6).await?;
    let before: i64 = sqlx::query_scalar(
        "SELECT (chain_positions ->> 'target_block_number')::bigint
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xremote-parent'
           AND child_logical_name_id = 'ens:0xremote-child'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(before, 6);

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 6,
            hash: block_hash(CHAIN, 6),
        }),
        RunMode::Normal,
        7,
        7,
    )
    .await?;

    let after: i64 = sqlx::query_scalar(
        "SELECT (chain_positions ->> 'target_block_number')::bigint
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xremote-parent'
           AND child_logical_name_id = 'ens:0xremote-child'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        after, 6,
        "candidate-only topology evidence changed an unrelated projection row"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn orphaned_topology_cannot_widen_incremental_child_scope() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_orphaned_topology_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    seed_deep_v1_topology_fixture(scratch.pool()).await?;
    seed_orphaned_topology_bridge(scratch.pool()).await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 6).await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 6,
            hash: block_hash(CHAIN, 6),
        }),
        RunMode::Normal,
        7,
        7,
    )
    .await?;

    let target: i64 = sqlx::query_scalar(
        "SELECT (chain_positions ->> 'target_block_number')::bigint
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xremote-parent'
           AND child_logical_name_id = 'ens:0xremote-child'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        target, 6,
        "orphaned topology evidence changed an unrelated projection row"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn current_window_orphaned_topology_cannot_widen_child_scope() -> Result<()> {
    const ORPHANED_HASH: &str = "project-fixture-orphaned-block-7";
    let scratch =
        ScratchDatabase::create("production_project_current_orphaned_topology_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    seed_deep_v1_topology_fixture(scratch.pool()).await?;
    seed_candidate_topology_bridge(scratch.pool()).await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, 7, to_timestamp(7), 'orphaned')",
    )
    .bind(CHAIN)
    .bind(ORPHANED_HASH)
    .bind(block_hash(CHAIN, 6))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        7,
        Some("ens:0xremote-parent"),
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xn1",
            "child_node":"0xremote-parent",
            "labelhash":"0xremote-parent-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET block_hash = $2
         WHERE chain_id = $1
           AND block_number = 7
           AND event_kind = 'SubregistryChanged'
           AND after_state ->> 'node' = '0xn1'",
    )
    .bind(CHAIN)
    .bind(ORPHANED_HASH)
    .execute(scratch.pool())
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 6).await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 6,
            hash: block_hash(CHAIN, 6),
        }),
        RunMode::Normal,
        7,
        7,
    )
    .await?;

    let target: i64 = sqlx::query_scalar(
        "SELECT (chain_positions ->> 'target_block_number')::bigint
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xremote-parent'
           AND child_logical_name_id = 'ens:0xremote-child'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        target, 6,
        "current-window orphaned topology changed an unrelated projection row"
    );

    scratch.cleanup().await
}

fn expected_deep_v1_topology_edges() -> Vec<(String, String)> {
    (0..4)
        .map(|index| {
            let parent = if index == 0 {
                "ens:0xeth".to_owned()
            } else {
                format!("ens:0xn{index}")
            };
            (parent, format!("ens:0xn{}", index + 1))
        })
        .collect()
}

async fn deep_v1_topology_edges(pool: &PgPool) -> Result<Vec<(String, String)>> {
    Ok(sqlx::query_as(
        "SELECT parent_logical_name_id, child_logical_name_id
         FROM children_current
         WHERE parent_logical_name_id IN (
             'ens:0xeth', 'ens:0xn1', 'ens:0xn2', 'ens:0xn3'
         )
           AND child_logical_name_id IN (
             'ens:0xn1', 'ens:0xn2', 'ens:0xn3', 'ens:0xn4'
         )
         ORDER BY parent_logical_name_id, child_logical_name_id",
    )
    .fetch_all(pool)
    .await?)
}

async fn seed_deep_v1_topology_fixture(pool: &PgPool) -> Result<()> {
    for block in 4..=7 {
        insert_lineage_block(pool, CHAIN, block).await?;
    }
    for index in 1..=4 {
        let node = format!("0xn{index}");
        let logical_name_id = format!("ens:{node}");
        let raw_name = format!("n{index}.eth");
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES (
                 $1, 'ens', $2, ARRAY[$2], decode('00', 'hex'), $3,
                 ARRAY[$3], $4, 'active', $5, $6, 1, 'canonical'
             )",
        )
        .bind(logical_name_id)
        .bind(raw_name)
        .bind(node)
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
    }
    for index in 1..=4 {
        let labelhash = format!("0xn{index}-label");
        let label = format!("n{index}");
        sqlx::query(
            "INSERT INTO label_preimages (
                 labelhash, raw_label, decoded_label, normalizer_version,
                 normalized_under_version, source_kind, source_priority
             ) VALUES ($1, convert_to($2, 'UTF8'), $2, $3, true, 'fixture', 1)",
        )
        .bind(labelhash)
        .bind(label)
        .bind(NORMALIZER)
        .execute(pool)
        .await?;
    }
    for index in 0..4 {
        let parent = if index == 0 {
            "0xeth".to_owned()
        } else {
            format!("0xn{index}")
        };
        let child = format!("0xn{}", index + 1);
        insert_event(
            pool,
            CHAIN,
            index + 3,
            Some(&format!("ens:{parent}")),
            None,
            "SubregistryChanged",
            "ens_v1_registry_l1",
            json!({
                "node":parent,
                "child_node":child,
                "labelhash":format!("0xn{}-label", index + 1),
                "owner":OWNER
            }),
            json!({}),
        )
        .await?;
    }
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xeth"),
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xn1",
            "labelhash":"0xn1-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    Ok(())
}

async fn seed_candidate_topology_bridge(pool: &PgPool) -> Result<()> {
    seed_remote_topology_bridge(pool).await?;
    sqlx::query(
        "UPDATE normalized_events
         SET consumer_visibility = 'candidate',
             migration_correlation_ids = ARRAY['candidate-topology']::text[]
         WHERE chain_id = $1
           AND block_number = 2
           AND event_kind = 'SubregistryChanged'
           AND after_state ->> 'node' = '0xn1'",
    )
    .bind(CHAIN)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_orphaned_topology_bridge(pool: &PgPool) -> Result<()> {
    const ORPHANED_HASH: &str = "project-fixture-orphaned-block-2";
    seed_remote_topology_bridge(pool).await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, 2, to_timestamp(2), 'orphaned')",
    )
    .bind(CHAIN)
    .bind(ORPHANED_HASH)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET block_hash = $2
         WHERE chain_id = $1
           AND block_number = 2
           AND event_kind = 'SubregistryChanged'
           AND after_state ->> 'node' = '0xn1'",
    )
    .bind(CHAIN)
    .bind(ORPHANED_HASH)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_remote_topology_bridge(pool: &PgPool) -> Result<()> {
    for (logical_name_id, raw_name, namehash) in [
        ("ens:0xremote-parent", "remote.eth", "0xremote-parent"),
        ("ens:0xremote-child", "child.remote.eth", "0xremote-child"),
    ] {
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES (
                 $1, 'ens', $2, ARRAY[$2], decode('00', 'hex'), $3,
                 ARRAY[$3], $4, 'active', $5, $6, 1, 'canonical'
             )",
        )
        .bind(logical_name_id)
        .bind(raw_name)
        .bind(namehash)
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
    }
    for (labelhash, label) in [
        ("0xremote-parent-label", "remote"),
        ("0xremote-child-label", "child"),
    ] {
        sqlx::query(
            "INSERT INTO label_preimages (
                 labelhash, raw_label, decoded_label, normalizer_version,
                 normalized_under_version, source_kind, source_priority
             ) VALUES ($1, convert_to($2, 'UTF8'), $2, $3, true, 'fixture', 1)",
        )
        .bind(labelhash)
        .bind(label)
        .bind(NORMALIZER)
        .execute(pool)
        .await?;
    }
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xremote-parent"),
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xremote-parent",
            "child_node":"0xremote-child",
            "labelhash":"0xremote-child-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xn1"),
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xn1",
            "child_node":"0xremote-parent",
            "labelhash":"0xremote-parent-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn incremental_v2_subregistry_flip_replaces_children_in_both_directions() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_v2_subregistry_flip").await?;
    let full = ScratchDatabase::create("production_project_v2_subregistry_flip_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        seed_v2_subregistry_flip_fixture(pool).await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    assert_eq!(
        v2_flip_children(incremental.pool()).await?,
        vec!["ens:0xflip-c0"],
        "the initial S1 pointer must select c0"
    );

    for (target, expected) in [
        (4, vec!["ens:0xflip-c1", "ens:0xflip-shared"]),
        (5, vec!["ens:0xflip-c0"]),
        (6, vec!["ens:0xflip-c0"]),
    ] {
        run_project(
            incremental.pool(),
            CHAIN,
            Some(Marker {
                number: target - 1,
                hash: block_hash(CHAIN, target - 1),
            }),
            RunMode::Normal,
            target,
            target,
        )
        .await?;
        run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, target).await?;

        let incremental_children = v2_flip_children(incremental.pool()).await?;
        let full_children = v2_flip_children(full.pool()).await?;
        assert_eq!(
            full_children, expected,
            "unexpected full rebuild at {target}"
        );
        assert_eq!(
            incremental_children, full_children,
            "incremental child edges diverged after the subregistry tick at {target}"
        );
    }

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn direct_v2_unregister_suppresses_the_selected_registry_child() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_v2_direct_unregister").await?;
    let full = ScratchDatabase::create("production_project_v2_direct_unregister_full").await?;
    let release_after_state = interpreted_direct_v2_release_after_state()?;
    assert_eq!(
        release_after_state
            .get("registry_contract_instance_id")
            .and_then(Value::as_str),
        Some("00000000-0000-0000-0000-000000000f01"),
        "direct unregister must retain its emitting registry identity"
    );
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        seed_v2_subregistry_flip_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            3,
            Some("ens:0xflip-c0"),
            None,
            "RegistrationReleased",
            "ens_v2_registry_l1",
            release_after_state.clone(),
            json!({}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 2).await?;
    assert!(
        v2_flip_children(incremental.pool())
            .await?
            .contains(&"ens:0xflip-c0".to_owned()),
        "the directly registered child must exist before unregister"
    );
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 2,
            hash: block_hash(CHAIN, 2),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    assert_eq!(
        v2_flip_children(incremental.pool()).await?,
        Vec::<String>::new(),
        "incremental Project retained a directly unregistered ENSv2 child"
    );
    assert_eq!(
        v2_flip_children(full.pool()).await?,
        Vec::<String>::new(),
        "a full rebuild retained a directly unregistered ENSv2 child"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

fn interpreted_direct_v2_release_after_state() -> Result<Value> {
    const ADAPTER_CHAIN: &str = "ethereum-sepolia";
    const REGISTRY_ADDRESS: &str = "0x00000000000000000000000000000000000020aa";
    const REGISTRY_INSTANCE: &str = "00000000-0000-0000-0000-000000000f01";
    let repository = load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )?;
    let loaded = repository
        .manifests()
        .iter()
        .find(|loaded| {
            loaded.manifest.chain == ADAPTER_CHAIN
                && loaded.manifest.source_family == "ens_v2_registry_l1"
                && loaded.version_tag == "v2"
        })
        .expect("the checked-in post-audit ENSv2 registry manifest must exist");
    let mut payload = serde_json::to_value(&loaded.manifest)?;
    payload["manifest_version"] = Value::from(1);
    let manifest = ManifestInput {
        manifest_id: 1,
        manifest_version: 1,
        namespace: loaded.manifest.namespace.clone(),
        source_family: loaded.manifest.source_family.clone(),
        chain_id: loaded.manifest.chain.clone(),
        deployment_label: loaded.manifest.deployment_epoch.clone(),
        normalizer_version: loaded.manifest.normalizer_version.clone(),
        payload_json: serde_json::to_string(&payload)?,
    };
    let discovery_rules = loaded
        .manifest
        .discovery_rules
        .iter()
        .map(|rule| DiscoveryRuleInput {
            manifest_id: 1,
            edge_kind: rule.edge_kind.clone(),
            from_role: Some(rule.from_role.clone()),
            admission: rule.admission.clone(),
        })
        .collect();
    let token_id = U256::from(101_u64);
    let owner = OWNER.parse::<Address>()?;
    let sender = TRANSFER_OWNER.parse::<Address>()?;
    let label = "direct".to_owned();
    let label_hash = keccak256(label.as_bytes());
    let logs = [
        LabelRegistered {
            tokenId: token_id,
            labelHash: label_hash,
            label,
            owner,
            expiry: 1_800_000_000,
            sender,
        }
        .encode_log_data(),
        TokenResource {
            tokenId: token_id,
            resource: U256::from(5_001_u64),
        }
        .encode_log_data(),
        LabelUnregistered {
            tokenId: token_id,
            sender,
        }
        .encode_log_data(),
    ];
    let raw_logs = logs
        .into_iter()
        .enumerate()
        .map(|(log_index, log)| AdapterRawLogInput {
            chain_id: ADAPTER_CHAIN.into(),
            block_hash: "adapter-direct-unregister-block".into(),
            block_number: 1,
            block_timestamp: time::OffsetDateTime::from_unix_timestamp(1_700_000_000)
                .expect("fixture timestamp must be valid"),
            canonicality_state: "canonical".into(),
            transaction_hash: "adapter-direct-unregister-transaction".into(),
            transaction_index: 0,
            log_index: i64::try_from(log_index).expect("fixture log index fits i64"),
            emitting_address: REGISTRY_ADDRESS.into(),
            topics: log.topics().iter().map(ToString::to_string).collect(),
            data: log.data.to_vec(),
        })
        .collect();
    let output = interpret_schema_v2_batch(AdapterBatchInput {
        chain_id: ADAPTER_CHAIN.into(),
        manifests: vec![manifest],
        discovery_rules,
        admissions: vec![AddressAdmissionInput {
            address: REGISTRY_ADDRESS.into(),
            contract_instance_id: Uuid::parse_str(REGISTRY_INSTANCE)?,
            source_manifest_id: Some(1),
            role: Some("registry".into()),
            discovery_edge_kind: None,
            discovery_from_contract_instance_id: None,
            discovery_observation_key: None,
            active_from_block: Some(0),
            active_to_block: None,
        }],
        prior_events: Vec::new(),
        blocks: vec![AdapterRawBlockInput {
            chain_id: ADAPTER_CHAIN.into(),
            block_hash: "adapter-direct-unregister-block".into(),
            block_number: 1,
            block_timestamp: time::OffsetDateTime::from_unix_timestamp(1_700_000_000)
                .expect("fixture timestamp must be valid"),
            canonicality_state: "canonical".into(),
        }],
        raw_logs,
    })?;
    Ok(output
        .normalized_events
        .into_iter()
        .find(|event| event.event_kind == "RegistrationReleased")
        .expect("LabelUnregistered must derive RegistrationReleased")
        .after_state)
}

async fn v2_flip_children(pool: &PgPool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT child_logical_name_id
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xflip-parent'
         ORDER BY child_logical_name_id",
    )
    .fetch_all(pool)
    .await?)
}

async fn seed_v2_subregistry_flip_fixture(pool: &PgPool) -> Result<()> {
    const S1_ADDRESS: &str = "0x0000000000000000000000000000000000000f01";
    const S2_ADDRESS: &str = "0x0000000000000000000000000000000000000f02";
    let s1 = Uuid::parse_str("00000000-0000-0000-0000-000000000f01")?;
    let s2 = Uuid::parse_str("00000000-0000-0000-0000-000000000f02")?;

    for block in 4..=6 {
        insert_lineage_block(pool, CHAIN, block).await?;
    }
    for (instance, address) in [(s1, S1_ADDRESS), (s2, S2_ADDRESS)] {
        sqlx::query(
            "INSERT INTO contract_instances (
                 contract_instance_id, chain_id, contract_kind
             ) VALUES ($1, $2, 'contract')",
        )
        .bind(instance)
        .bind(CHAIN)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO contract_instance_addresses (
                 contract_instance_id, chain_id, address, active_from_block_number
             ) VALUES ($1, $2, $3, 0)",
        )
        .bind(instance)
        .bind(CHAIN)
        .bind(address)
        .execute(pool)
        .await?;
    }
    for (logical_name_id, raw_name, raw_labels, namehash, labelhashes) in [
        (
            "ens:0xflip-parent",
            "flip.eth",
            vec!["flip", "eth"],
            "0xflip-parent",
            vec!["0xflip-parent-label", "0xeth"],
        ),
        (
            "ens:0xflip-c0",
            "c0.flip.eth",
            vec!["c0", "flip", "eth"],
            "0xflip-c0",
            vec!["0xflip-c0-label", "0xflip-parent-label", "0xeth"],
        ),
        (
            "ens:0xflip-c1",
            "c1.flip.eth",
            vec!["c1", "flip", "eth"],
            "0xflip-c1",
            vec!["0xflip-c1-label", "0xflip-parent-label", "0xeth"],
        ),
        (
            "ens:0xflip-shared",
            "shared.flip.eth",
            vec!["shared", "flip", "eth"],
            "0xflip-shared",
            vec!["0xflip-shared-label", "0xflip-parent-label", "0xeth"],
        ),
    ] {
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES ($1, 'ens', $2, $3, decode('00', 'hex'), $4, $5, $6,
                       'active', $7, $8, 1, 'canonical')",
        )
        .bind(logical_name_id)
        .bind(raw_name)
        .bind(raw_labels)
        .bind(namehash)
        .bind(labelhashes)
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority
         ) VALUES
             ('0xflip-c0-label', convert_to('c0', 'UTF8'), 'c0', $1, true, 'fixture', 1),
             ('0xflip-c1-label', convert_to('c1', 'UTF8'), 'c1', $1, true, 'fixture', 1),
             ('0xflip-shared-label', convert_to('shared', 'UTF8'), 'shared', $1,
              true, 'fixture', 1)",
    )
    .bind(NORMALIZER)
    .execute(pool)
    .await?;

    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xflip-parent"),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":S1_ADDRESS}),
        json!({}),
    )
    .await?;
    for (block, logical_name_id, instance) in [
        (1, "ens:0xflip-c0", s1),
        (2, "ens:0xflip-c1", s2),
        (1, "ens:0xflip-shared", s2),
        (2, "ens:0xflip-shared", s1),
    ] {
        insert_event(
            pool,
            CHAIN,
            block,
            Some(logical_name_id),
            None,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            json!({
                "registry_contract_instance_id":instance,
                "registrant":OWNER
            }),
            json!({}),
        )
        .await?;
    }
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xflip-shared"),
        None,
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({
            "registry_contract_instance_id":s1,
            "registrant":OWNER
        }),
        json!({}),
    )
    .await?;
    for (block, before, after) in [(4, S1_ADDRESS, S2_ADDRESS), (5, S2_ADDRESS, S1_ADDRESS)] {
        insert_event(
            pool,
            CHAIN,
            block,
            Some("ens:0xflip-parent"),
            None,
            "SubregistryChanged",
            "ens_v2_registry_l1",
            json!({"subregistry":after}),
            json!({}),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events
             SET before_state = jsonb_build_object('subregistry', lower($1))
             WHERE chain_id = $2 AND block_number = $3
               AND logical_name_id = 'ens:0xflip-parent'
               AND event_kind = 'SubregistryChanged'",
        )
        .bind(before)
        .bind(CHAIN)
        .bind(block)
        .execute(pool)
        .await?;
    }
    Ok(())
}

fn expected_wrapper_fuses(fuses: u32) -> Value {
    json!({
        "fuses": fuses,
        "cannot_unwrap": fuses & 1 != 0,
        "cannot_burn_fuses": fuses & 2 != 0,
        "cannot_transfer": fuses & 4 != 0,
        "cannot_set_resolver": fuses & 8 != 0,
        "cannot_set_ttl": fuses & 16 != 0,
        "cannot_create_subdomain": fuses & 32 != 0,
        "cannot_approve": fuses & 64 != 0,
        "parent_cannot_control": fuses & 65_536 != 0,
        "is_dot_eth": fuses & 131_072 != 0,
        "can_extend_expiry": fuses & 262_144 != 0,
    })
}

#[tokio::test]
async fn wrapper_states_and_expiry_gate_permissions_and_controller_relations() -> Result<()> {
    let cases = [
        (
            "wrapped_expired",
            0,
            "wrapped",
            2,
            Some("wrapped"),
            9,
            true,
            true,
        ),
        (
            "wrapped_expired_parent_fuse",
            262_144,
            "wrapped",
            2,
            Some("wrapped"),
            9,
            true,
            true,
        ),
        (
            "emancipated",
            65_536,
            "emancipated",
            3,
            Some("emancipated"),
            9,
            true,
            true,
        ),
        (
            "dot_eth_grace_boundary",
            196_608,
            "emancipated",
            7_776_003,
            Some("emancipated"),
            9,
            true,
            true,
        ),
        (
            "dot_eth_grace",
            196_608,
            "emancipated",
            7_776_002,
            Some("emancipated"),
            1,
            false,
            true,
        ),
        (
            "locked",
            65_537,
            "locked",
            3,
            Some("locked"),
            7,
            false,
            true,
        ),
        (
            "locked_max_expiry",
            65_537,
            "locked",
            u64::MAX,
            Some("locked"),
            7,
            false,
            true,
        ),
        (
            "cannot_burn_fuses",
            65_539,
            "locked",
            3,
            Some("locked"),
            6,
            false,
            true,
        ),
        (
            "cannot_transfer",
            65_541,
            "locked",
            3,
            Some("locked"),
            6,
            false,
            true,
        ),
        (
            "cannot_set_resolver",
            65_545,
            "locked",
            3,
            Some("locked"),
            5,
            false,
            true,
        ),
        (
            "cannot_set_ttl",
            65_553,
            "locked",
            3,
            Some("locked"),
            6,
            false,
            true,
        ),
        (
            "cannot_create_subdomain",
            65_569,
            "locked",
            3,
            Some("locked"),
            6,
            false,
            true,
        ),
        (
            "cannot_approve",
            65_601,
            "locked",
            3,
            Some("locked"),
            6,
            false,
            true,
        ),
        (
            "can_extend_expiry",
            327_681,
            "locked",
            3,
            Some("locked"),
            7,
            false,
            true,
        ),
        ("locked_expired", 65_537, "locked", 2, None, 0, false, false),
    ];

    for (
        label,
        fuses,
        state,
        expiry,
        expected_state,
        power_count,
        has_controller,
        has_token_holder,
    ) in cases
    {
        let scratch = ScratchDatabase::create(&format!("production_project_fuse_{label}")).await?;
        seed_project_fixture(scratch.pool()).await?;
        insert_event(
            scratch.pool(),
            CHAIN,
            2,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "PermissionChanged",
            "ens_v1_wrapper_l1",
            json!({
                "subject":OWNER,
                "scope":{"kind":"resource"},
                "effective_powers":[
                    "resource_control", "resolver_control", "set_resolver", "set_ttl",
                    "create_subnames", "transfer", "unwrap", "burn_fuses", "approve"
                ],
                "grant_source":{"kind":"fixture"},
                "revocation_source":null,
                "inheritance_path":[],
                "transfer_behavior":"replace_on_authority_change"
            }),
            json!({}),
        )
        .await?;
        insert_event(
            scratch.pool(),
            CHAIN,
            3,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            json!({"expiry":expiry}),
            json!({}),
        )
        .await?;
        insert_event(
            scratch.pool(),
            CHAIN,
            3,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            json!({"fuses":fuses,"wrapper_state":state}),
            json!({}),
        )
        .await?;

        run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
        let projected_state: Option<String> = sqlx::query_scalar(
            "SELECT declared_summary ->> 'wrapper_state' FROM name_current
             WHERE logical_name_id = 'ens:0xalice'",
        )
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(projected_state.as_deref(), expected_state, "{label}");
        let projected_fuses: Option<Value> = sqlx::query_scalar(
            "SELECT declared_summary -> 'wrapper_fuses' FROM name_current
             WHERE logical_name_id = 'ens:0xalice'",
        )
        .fetch_one(scratch.pool())
        .await?;
        let expected_effective_fuses = expected_state.map(|_| {
            expected_wrapper_fuses(if expiry < 3 {
                0
            } else {
                u32::try_from(fuses).expect("fixture fuses fit uint32")
            })
        });
        assert_eq!(projected_fuses, expected_effective_fuses, "{label}");
        let effective_powers: Option<Value> = sqlx::query_scalar(
            "SELECT effective_powers FROM permissions_current WHERE resource_id = $1",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .fetch_optional(scratch.pool())
        .await?;
        assert_eq!(
            effective_powers
                .as_ref()
                .and_then(Value::as_array)
                .map_or(0, Vec::len),
            power_count,
            "{label}"
        );
        let stored_permissions = bigname_storage::load_permissions_current(
            scratch.pool(),
            Uuid::parse_str(RESOURCE)?,
            None,
            None,
        )
        .await?;
        assert_eq!(
            stored_permissions
                .first()
                .map(|permission| permission.effective_powers.clone()),
            effective_powers,
            "Project-built permission rows must pass phase canonicality admission for {label}"
        );
        assert!(
            bigname_storage::load_permissions_current_resource_summary(
                scratch.pool(),
                Uuid::parse_str(RESOURCE)?,
            )
            .await?
            .is_some(),
            "Project-built permission summaries must pass phase canonicality admission for {label}"
        );
        if label == "dot_eth_grace" {
            assert_eq!(effective_powers, Some(json!(["approve"])));
        }
        let controller_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM address_names_current
                 WHERE logical_name_id = 'ens:0xalice'
                   AND relation = 'effective_controller'
             )",
        )
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(controller_exists, has_controller, "{label}");
        let token_holder_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM address_names_current
                 WHERE logical_name_id = 'ens:0xalice'
                   AND relation = 'token_holder'
             )",
        )
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(token_holder_exists, has_token_holder, "{label}");
        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn incremental_project_revisits_wrapper_timestamp_boundaries() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_wrapper_time_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, 5, to_timestamp(7776004), 'canonical')",
    )
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 5))
    .bind(block_hash(CHAIN, 4))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_wrapper_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":[
                "resource_control", "resolver_control", "set_resolver", "set_ttl",
                "create_subnames", "transfer", "unwrap", "burn_fuses", "approve"
            ],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"source_event":"NameRenewed","authority_kind":"wrapper","expiry":7_776_003}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":196_608,"wrapper_state":"emancipated"}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let grace_powers: Value = sqlx::query_scalar(
        "SELECT effective_powers FROM permissions_current WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(grace_powers, json!(["approve"]));
    let grace_controller: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM address_names_current
             WHERE logical_name_id = 'ens:0xalice'
               AND relation = 'effective_controller'
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(!grace_controller);

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    let expired_state: Option<String> = sqlx::query_scalar(
        "SELECT declared_summary ->> 'wrapper_state' FROM name_current
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(expired_state, None);
    let expired_relations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM address_names_current
         WHERE logical_name_id = 'ens:0xalice'
           AND relation IN ('effective_controller', 'token_holder')",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(expired_relations, 0);
    let expired_permissions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM permissions_current WHERE resource_id = $1")
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(expired_permissions, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn event_free_wrapper_boundary_refreshes_resolver_permission_summary() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_wrapper_resolver_summary").await?;
    let full = ScratchDatabase::create("production_project_wrapper_resolver_summary_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
        extend_wrapper_resolver_summary_fixture(pool).await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    let event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND block_number = 5",
    )
    .bind(CHAIN)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(event_count, 0, "the expiry transition must be event-free");

    let pre_boundary_powers: Value = sqlx::query_scalar(
        "SELECT effective_powers
         FROM permissions_current
         WHERE resource_id = $1
           AND scope_kind = 'resolver'
           AND lower(scope_detail ->> 'resolver_address') = lower($2)",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(pre_boundary_powers, json!(["approve"]));
    let record_uses_resolver: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM record_inventory_current
             WHERE resource_id = $1
               AND lower(provenance ->> 'resolver_address') = lower($2)
         )",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    assert!(
        record_uses_resolver,
        "the v2 resolver must also serve records"
    );

    sqlx::query(
        "UPDATE resolver_current
         SET declared_summary = jsonb_set(
             declared_summary, '{passthrough_guard}', 'true'::jsonb
         )
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .execute(incremental.pool())
    .await?;

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 5).await?;

    for pool in [incremental.pool(), full.pool()] {
        let resolver_permission_count: i64 = sqlx::query_scalar(
            "SELECT count(*)
             FROM permissions_current
             WHERE resource_id = $1
               AND scope_kind = 'resolver'
               AND lower(scope_detail ->> 'resolver_address') = lower($2)",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .bind(EQUIVALENCE_V2_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(resolver_permission_count, 0);
    }

    let incremental_counts =
        resolver_summary_counts(incremental.pool(), CHAIN, EQUIVALENCE_V2_RESOLVER).await?;
    let full_counts = resolver_summary_counts(full.pool(), CHAIN, EQUIVALENCE_V2_RESOLVER).await?;
    assert_eq!(full_counts, (0, 0, None));
    assert_eq!(
        incremental_counts, full_counts,
        "an event-free wrapper boundary left a stale resolver permission summary"
    );

    let incremental_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    let full_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(full.pool())
    .await?;
    assert_eq!(
        incremental_summary, full_summary,
        "event-free binding, alias, permission, role, and event summaries diverged"
    );

    let unscoped_permission_resolver_kept_passthrough: bool = sqlx::query_scalar(
        "SELECT declared_summary @> '{\"passthrough_guard\":true}'::jsonb
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    assert!(
        unscoped_permission_resolver_kept_passthrough,
        "a boundary-scoped resource without resolver permissions lost passthrough"
    );
    let bob_target: i64 = sqlx::query_scalar(
        "SELECT (chain_positions ->> 'target_block_number')::bigint
         FROM record_inventory_current WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_BOB_RESOURCE)?)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(
        bob_target, 5,
        "the passthrough guard resource must be scoped"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn lapsed_binding_wrapper_boundary_inventory_matches_full_rebuild() -> Result<()> {
    let incremental = ScratchDatabase::create("project_lapsed_binding_wrapper_incremental").await?;
    let full = ScratchDatabase::create("project_lapsed_binding_wrapper_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_pre_surface_wrapper_boundary_fixture(pool).await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    assert!(
        record_inventory_snapshot(incremental.pool(), RESOURCE)
            .await?
            .is_some(),
        "the pre-boundary build must retain the surfaced inventory"
    );
    for pool in [incremental.pool(), full.pool()] {
        lapse_alice_binding(pool).await?;
    }
    assert_lapsed_surface_without_replacement(incremental.pool()).await?;
    let boundary_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND block_number = 5",
    )
    .bind(CHAIN)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(
        boundary_events, 0,
        "the wrapper transition must be event-free"
    );

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 5).await?;

    assert_eq!(
        record_inventory_snapshot(incremental.pool(), RESOURCE).await?,
        record_inventory_snapshot(full.pool(), RESOURCE).await?,
        "a resource-only wrapper boundary dropped inventory retained by a full rebuild"
    );
    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn pointer_name_scope_closes_over_a_successor_resource() -> Result<()> {
    const SUCCESSOR_RESOURCE: &str = "00000000-0000-0000-0000-0000000000e1";
    const SUCCESSOR_BINDING: &str = "00000000-0000-0000-0000-0000000000e2";
    const SUCCESSOR_LINEAGE: &str = "00000000-0000-0000-0000-0000000000e3";
    const SECOND_SUCCESSOR_RESOURCE: &str = "00000000-0000-0000-0000-0000000000e4";
    const SECOND_SUCCESSOR_BINDING: &str = "00000000-0000-0000-0000-0000000000e5";
    const SECOND_SUCCESSOR_LINEAGE: &str = "00000000-0000-0000-0000-0000000000e6";
    const HISTORICAL_SECOND_BINDING: &str = "00000000-0000-0000-0000-0000000000e7";

    let incremental = ScratchDatabase::create("project_pointer_successor_incremental").await?;
    let full = ScratchDatabase::create("project_pointer_successor_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_pre_surface_wrapper_boundary_fixture(pool).await?;
        sqlx::query(
            "UPDATE surface_bindings SET active_to = to_timestamp(7776002)
             WHERE surface_binding_id = $1",
        )
        .bind(Uuid::parse_str(SURFACE_BINDING)?)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO token_lineages (
                 token_lineage_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, $2, $3, 4, 'canonical')",
        )
        .bind(Uuid::parse_str(SUCCESSOR_LINEAGE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 4))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES (
                 'ens:0xbob', 'ens', 'bob.eth', ARRAY['bob', 'eth'],
                 decode('00', 'hex'), '0xbob', ARRAY['0xbob-label', '0xeth'], $1,
                 'active', $2, $3, 4, 'canonical'
             )",
        )
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 4))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO token_lineages (
                 token_lineage_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, $2, $3, 4, 'canonical')",
        )
        .bind(Uuid::parse_str(SECOND_SUCCESSOR_LINEAGE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 4))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, token_lineage_id, chain_id, block_hash,
                 block_number, canonicality_state
             ) VALUES ($1, $2, $3, $4, 4, 'canonical')",
        )
        .bind(Uuid::parse_str(SECOND_SUCCESSOR_RESOURCE)?)
        .bind(Uuid::parse_str(SECOND_SUCCESSOR_LINEAGE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 4))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind,
                 authority_arm, active_from, chain_id, block_hash,
                 block_number, canonicality_state
             ) VALUES (
                 $1, 'ens:0xbob', $2, 'declared_registry_path', 'ens_v1',
                 to_timestamp(7776003), $3, $4, 4, 'canonical'
             )",
        )
        .bind(Uuid::parse_str(SECOND_SUCCESSOR_BINDING)?)
        .bind(Uuid::parse_str(SECOND_SUCCESSOR_RESOURCE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 4))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, token_lineage_id, chain_id, block_hash,
                 block_number, canonicality_state
             ) VALUES ($1, $2, $3, $4, 4, 'canonical')",
        )
        .bind(Uuid::parse_str(SUCCESSOR_RESOURCE)?)
        .bind(Uuid::parse_str(SUCCESSOR_LINEAGE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 4))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind,
                 authority_arm, active_from, active_to, chain_id, block_hash,
                 block_number, canonicality_state
             ) VALUES (
                 $1, 'ens:0xbob', $2, 'declared_registry_path', 'ens_v1',
                 to_timestamp(7776001), to_timestamp(7776003),
                 $3, $4, 4, 'canonical'
             )",
        )
        .bind(Uuid::parse_str(HISTORICAL_SECOND_BINDING)?)
        .bind(Uuid::parse_str(SUCCESSOR_RESOURCE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 4))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind,
                 authority_arm, active_from, chain_id, block_hash,
                 block_number, canonicality_state
             ) VALUES (
                 $1, 'ens:0xalice', $2, 'declared_registry_path', 'ens_v1',
                 to_timestamp(7776002), $3, $4, 4, 'canonical'
             )",
        )
        .bind(Uuid::parse_str(SUCCESSOR_BINDING)?)
        .bind(Uuid::parse_str(SUCCESSOR_RESOURCE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 4))
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            4,
            Some("ens:0xbob"),
            Some(SUCCESSOR_RESOURCE),
            "ResolverChanged",
            "ens_v1_registry_l1",
            json!({"resolver":RESOLVER}),
            json!({}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    let event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND block_number = 5",
    )
    .bind(CHAIN)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(event_count, 0, "the resource boundary must be event-free");

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 5).await?;

    let incremental_identity: (Option<Uuid>, Option<Uuid>) = sqlx::query_as(
        "SELECT resource_id, token_lineage_id FROM name_current
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(incremental.pool())
    .await?;
    let full_identity: (Option<Uuid>, Option<Uuid>) = sqlx::query_as(
        "SELECT resource_id, token_lineage_id FROM name_current
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(full.pool())
    .await?;
    assert_eq!(
        incremental_identity, full_identity,
        "pointer-name expansion failed to stage the name's active successor resource"
    );
    assert_eq!(
        full_identity,
        (
            Some(Uuid::parse_str(SUCCESSOR_RESOURCE)?),
            Some(Uuid::parse_str(SUCCESSOR_LINEAGE)?),
        )
    );
    let incremental_second: (Option<Uuid>, Option<Uuid>, Option<i64>) = sqlx::query_as(
        "SELECT resource_id, token_lineage_id,
                (canonicality_summary ->> 'target_block_number')::bigint
         FROM name_current WHERE logical_name_id = 'ens:0xbob'",
    )
    .fetch_one(incremental.pool())
    .await?;
    let full_second: (Option<Uuid>, Option<Uuid>, Option<i64>) = sqlx::query_as(
        "SELECT resource_id, token_lineage_id,
                (canonicality_summary ->> 'target_block_number')::bigint
         FROM name_current WHERE logical_name_id = 'ens:0xbob'",
    )
    .fetch_one(full.pool())
    .await?;
    assert_eq!(
        incremental_second, full_second,
        "scope closure stopped before the second pointer-name/resource hop"
    );
    assert_eq!(
        full_second,
        (
            Some(Uuid::parse_str(SECOND_SUCCESSOR_RESOURCE)?),
            Some(Uuid::parse_str(SECOND_SUCCESSOR_LINEAGE)?),
            Some(5),
        )
    );
    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn lapsed_binding_record_retraction_inventory_matches_full_rebuild() -> Result<()> {
    let incremental =
        ScratchDatabase::create("project_lapsed_binding_retraction_incremental").await?;
    let full = ScratchDatabase::create("project_lapsed_binding_retraction_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_pre_surface_wrapper_boundary_fixture(pool).await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    assert!(
        record_inventory_snapshot(incremental.pool(), RESOURCE)
            .await?
            .is_some(),
        "the first build must retain inventory citing the record event"
    );
    for pool in [incremental.pool(), full.pool()] {
        lapse_alice_binding(pool).await?;
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND block_number = 1
               AND event_kind = 'RecordChanged'
               AND logical_name_id IS NULL
               AND after_state ->> 'record_key' = 'text:before-surface'",
        )
        .bind(CHAIN)
        .execute(pool)
        .await?;
    }
    assert_lapsed_surface_without_replacement(incremental.pool()).await?;

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Redo,
        1,
        1,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;

    assert_eq!(
        record_inventory_snapshot(incremental.pool(), RESOURCE).await?,
        record_inventory_snapshot(full.pool(), RESOURCE).await?,
        "redo resource scope dropped pointer inventory retained by a full rebuild"
    );
    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn retracted_pointer_selects_another_surface_of_the_same_resource() -> Result<()> {
    let incremental = ScratchDatabase::create("project_shared_resource_redo_incremental").await?;
    let full = ScratchDatabase::create("project_shared_resource_redo_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES (
                 'ens:0xbob', 'ens', 'bob.eth', ARRAY['bob', 'eth'],
                 decode('00', 'hex'), '0xbob', ARRAY['0xbob-label', '0xeth'], $1,
                 'active', $2, $3, 1, 'canonical'
             )",
        )
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind,
                 authority_arm, active_from, active_to, chain_id, block_hash,
                 block_number, canonicality_state
             ) VALUES (
                 $1, 'ens:0xbob', $2, 'declared_registry_path', 'ens_v1',
                 to_timestamp(1), to_timestamp(2), $3, $4, 1, 'canonical'
             )",
        )
        .bind(Uuid::parse_str(EQUIVALENCE_BOB_BINDING)?)
        .bind(Uuid::parse_str(RESOURCE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some("ens:0xbob"),
            Some(RESOURCE),
            "ResolverChanged",
            "ens_v1_registry_l1",
            json!({"resolver":RESOLVER}),
            json!({}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({
                "node":"0xbob",
                "resolver":RESOLVER,
                "record_key":"text:shared-resource",
                "record_family":"text",
                "selector_key":"shared-resource",
                "value_retained":true,
                "value":"bob-survives"
            }),
            json!({"emitting_address":RESOLVER}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let initial_name: String = sqlx::query_scalar(
        "SELECT provenance ->> 'logical_name_id'
         FROM record_inventory_current WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(initial_name, "ens:0xalice");

    for pool in [incremental.pool(), full.pool()] {
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND block_number = 2
               AND logical_name_id = 'ens:0xalice'
               AND event_kind = 'ResolverChanged'",
        )
        .bind(CHAIN)
        .execute(pool)
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    let full_name: String = sqlx::query_scalar(
        "SELECT provenance ->> 'logical_name_id'
         FROM record_inventory_current WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(full.pool())
    .await?;
    assert_eq!(full_name, "ens:0xbob");
    assert_eq!(
        record_inventory_snapshot(incremental.pool(), RESOURCE).await?,
        record_inventory_snapshot(full.pool(), RESOURCE).await?,
        "redo failed to stage the surviving surface and its pre-surface record"
    );
    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn orphaned_latest_pointer_surface_falls_back_incrementally() -> Result<()> {
    const FALLBACK_RESOLVER: &str = "0x00000000000000000000000000000000000000b8";
    let incremental = ScratchDatabase::create("project_orphaned_pointer_incremental").await?;
    let full = ScratchDatabase::create("project_orphaned_pointer_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_pre_surface_wrapper_boundary_fixture(pool).await?;
        sqlx::query(
            "UPDATE manifest_versions
             SET manifest_payload = jsonb_set(
                 manifest_payload,
                 '{contracts}',
                 (manifest_payload -> 'contracts') || jsonb_build_array(
                     jsonb_build_object(
                         'role', 'public_resolver',
                         'address', lower($1::text),
                         'proxy_kind', 'none'
                     )
                 )
             )
             WHERE chain_id = $2 AND source_family = 'ens_v1_resolver_l1'",
        )
        .bind(FALLBACK_RESOLVER)
        .bind(CHAIN)
        .execute(pool)
        .await?;
        sqlx::query(
            "UPDATE normalized_events
             SET after_state = jsonb_set(
                 after_state,
                 '{manifest_payload,contracts}',
                 (after_state -> 'manifest_payload' -> 'contracts') || jsonb_build_array(
                     jsonb_build_object(
                         'role', 'public_resolver',
                         'address', lower($1::text),
                         'proxy_kind', 'none'
                     )
                 )
             )
             WHERE chain_id = $2 AND event_kind = 'SourceManifestUpdated'
               AND source_family = 'ens_v1_resolver_l1'",
        )
        .bind(FALLBACK_RESOLVER)
        .bind(CHAIN)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, deactivation_reason, deactivated_at,
                 chain_id, block_hash, block_number, canonicality_state
             ) VALUES (
                 'ens:0xbob', 'ens', 'bob.eth', ARRAY['bob', 'eth'],
                 decode('00', 'hex'), '0xbob', ARRAY['0xbob-label', '0xeth'], $1,
                 'shadow', 'fixture_shadow', to_timestamp(1),
                 $2, $3, 1, 'canonical'
             )",
        )
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind,
                 authority_arm, active_from, active_to, chain_id, block_hash,
                 block_number, canonicality_state
             ) VALUES (
                 $1, 'ens:0xbob', $2, 'declared_registry_path', 'ens_v1',
                 to_timestamp(1), to_timestamp(2), $3, $4, 1, 'canonical'
             )",
        )
        .bind(Uuid::parse_str(EQUIVALENCE_BOB_BINDING)?)
        .bind(Uuid::parse_str(RESOURCE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some("ens:0xbob"),
            Some(RESOURCE),
            "ResolverChanged",
            "ens_v1_registry_l1",
            json!({"resolver":FALLBACK_RESOLVER}),
            json!({}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({
                "node":"0xbob",
                "resolver":FALLBACK_RESOLVER,
                "record_key":"text:orphan-fallback",
                "record_family":"text",
                "selector_key":"orphan-fallback",
                "value_retained":true,
                "value":"bob-fallback"
            }),
            json!({"emitting_address":FALLBACK_RESOLVER}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    let initial_name: String = sqlx::query_scalar(
        "SELECT provenance ->> 'logical_name_id'
         FROM record_inventory_current WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(initial_name, "ens:0xalice");

    for pool in [incremental.pool(), full.pool()] {
        lapse_alice_binding(pool).await?;
        sqlx::query(
            "UPDATE name_surfaces SET canonicality_state = 'orphaned'
             WHERE chain_id = $1 AND logical_name_id = 'ens:0xalice'",
        )
        .bind(CHAIN)
        .execute(pool)
        .await?;
    }
    let target_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND block_number = 5",
    )
    .bind(CHAIN)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(target_events, 0, "the wrapper boundary must be event-free");

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 5).await?;

    let incremental_fallback: (String, Option<String>, String, String) = sqlx::query_as(
        "SELECT support_status, unsupported_reason,
                provenance ->> 'logical_name_id', provenance ->> 'resolver_address'
         FROM record_inventory_current WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(
        incremental_fallback,
        (
            "supported".to_owned(),
            None,
            "ens:0xbob".to_owned(),
            FALLBACK_RESOLVER.to_owned(),
        )
    );
    assert_eq!(
        record_entry_pairs(incremental.pool(), RESOURCE).await?,
        vec![("text:orphan-fallback".to_owned(), "bob-fallback".to_owned(),)]
    );
    let full_name: String = sqlx::query_scalar(
        "SELECT provenance ->> 'logical_name_id'
         FROM record_inventory_current WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(full.pool())
    .await?;
    assert_eq!(full_name, "ens:0xbob");
    assert_eq!(
        record_inventory_snapshot(incremental.pool(), RESOURCE).await?,
        record_inventory_snapshot(full.pool(), RESOURCE).await?,
        "incremental publication omitted the earlier surfaced pointer fallback"
    );
    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn name_only_scope_closes_over_the_resources_latest_surface() -> Result<()> {
    let incremental = ScratchDatabase::create("project_name_resource_surface_incremental").await?;
    let full = ScratchDatabase::create("project_name_resource_surface_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        insert_lineage_block(pool, CHAIN, 4).await?;
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES (
                 'ens:0xbob', 'ens', 'bob.eth', ARRAY['bob', 'eth'],
                 decode('00', 'hex'), '0xbob', ARRAY['0xbob-label', '0xeth'], $1,
                 'active', $2, $3, 1, 'canonical'
             )",
        )
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind,
                 authority_arm, active_from, active_to, chain_id, block_hash,
                 block_number, canonicality_state
             ) VALUES (
                 $1, 'ens:0xbob', $2, 'declared_registry_path', 'ens_v1',
                 to_timestamp(1), to_timestamp(4), $3, $4, 1, 'canonical'
             )",
        )
        .bind(Uuid::parse_str(EQUIVALENCE_BOB_BINDING)?)
        .bind(Uuid::parse_str(RESOURCE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            3,
            Some("ens:0xbob"),
            Some(RESOURCE),
            "ResolverChanged",
            "ens_v1_registry_l1",
            json!({"resolver":RESOLVER}),
            json!({}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({
                "node":"0xbob",
                "resolver":RESOLVER,
                "record_key":"text:name-only-closure",
                "record_family":"text",
                "selector_key":"name-only-closure",
                "value_retained":true,
                "value":"bob-remains-current"
            }),
            json!({"emitting_address":RESOLVER}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            4,
            Some("ens:0xalice"),
            None,
            "PreimageObserved",
            "ens_v1_registry_l1",
            json!({"labelhash":"0xalice-label","raw_labels_hex":["616c696365"]}),
            json!({}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let initial_name: String = sqlx::query_scalar(
        "SELECT provenance ->> 'logical_name_id'
         FROM record_inventory_current WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(initial_name, "ens:0xbob");

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;

    assert_eq!(
        record_inventory_snapshot(incremental.pool(), RESOURCE).await?,
        record_inventory_snapshot(full.pool(), RESOURCE).await?,
        "name-to-resource scope failed to stage the resource's latest lapsed surface"
    );
    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn children_only_scope_stages_pre_surface_record_history() -> Result<()> {
    let incremental = ScratchDatabase::create("project_children_only_record_incremental").await?;
    let full = ScratchDatabase::create("project_children_only_record_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_pre_surface_wrapper_boundary_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            5,
            Some("ens:0xeth"),
            None,
            "PreimageObserved",
            "ens_v1_registry_l1",
            json!({"labelhash":"0xeth","raw_labels_hex":["657468"]}),
            json!({}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    for pool in [incremental.pool(), full.pool()] {
        lapse_alice_binding(pool).await?;
    }
    assert_lapsed_surface_without_replacement(incremental.pool()).await?;
    let child_events_at_target: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND block_number = 5
           AND logical_name_id = 'ens:0xalice'",
    )
    .bind(CHAIN)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(
        child_events_at_target, 0,
        "the child must be topology-scoped only"
    );

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 5).await?;

    assert_eq!(
        record_inventory_snapshot(incremental.pool(), RESOURCE).await?,
        record_inventory_snapshot(full.pool(), RESOURCE).await?,
        "children-only surface scope omitted pre-surface record history"
    );
    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn pre_surface_attribution_isolated_by_node_and_resolver_in_project_db_fixture() -> Result<()>
{
    let scratch = ScratchDatabase::create("project_pre_surface_node_resolver_isolation").await?;
    seed_project_fixture(scratch.pool()).await?;
    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RecordChanged'
           AND logical_name_id = 'ens:0xalice'",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    let second_resolver = json!([{
        "role":"public_resolver",
        "address":EQUIVALENCE_V2_RESOLVER,
        "proxy_kind":"none"
    }]);
    sqlx::query(
        "UPDATE manifest_versions
         SET manifest_payload = jsonb_set(
             manifest_payload, '{contracts}',
             COALESCE(manifest_payload -> 'contracts', '[]'::jsonb) || $1::jsonb
         )
         WHERE chain_id = $2 AND source_family = 'ens_v1_resolver_l1'",
    )
    .bind(&second_resolver)
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET after_state = jsonb_set(
             after_state, '{manifest_payload,contracts}',
             COALESCE(after_state #> '{manifest_payload,contracts}', '[]'::jsonb)
                 || $1::jsonb
         )
         WHERE chain_id = $2 AND source_family = 'ens_v1_resolver_l1'
           AND event_kind = 'SourceManifestUpdated'",
    )
    .bind(&second_resolver)
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_BOB_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES (
             'ens:0xbob', 'ens', 'bob.eth', ARRAY['bob', 'eth'],
             decode('00', 'hex'), '0xbob', ARRAY['0xbob-label', '0xeth'], $1,
             'active', $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES ($1, 'ens:0xbob', $2, 'declared_registry_path',
                   'ens_v1', to_timestamp(1), $3, $4, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_BOB_BINDING)?)
    .bind(Uuid::parse_str(EQUIVALENCE_BOB_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":EQUIVALENCE_V2_RESOLVER}),
        json!({}),
    )
    .await?;
    for (node, resolver, key, value) in [
        ("0xalice", RESOLVER, "text:alice", "alice-current"),
        (
            "0xalice",
            EQUIVALENCE_V2_RESOLVER,
            "text:alice-wrong-resolver",
            "alice-wrong-resolver",
        ),
        ("0xbob", EQUIVALENCE_V2_RESOLVER, "text:bob", "bob-current"),
        (
            "0xbob",
            RESOLVER,
            "text:bob-wrong-resolver",
            "bob-wrong-resolver",
        ),
    ] {
        insert_event(
            scratch.pool(),
            CHAIN,
            1,
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({
                "node":node,
                "resolver":resolver,
                "record_key":key,
                "record_family":"text",
                "selector_key":key.trim_start_matches("text:"),
                "value_retained":true,
                "value":value
            }),
            json!({"emitting_address":resolver}),
        )
        .await?;
    }

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    assert_eq!(
        record_entry_pairs(scratch.pool(), RESOURCE).await?,
        vec![("text:alice".into(), "alice-current".into())]
    );
    assert_eq!(
        record_entry_pairs(scratch.pool(), EQUIVALENCE_BOB_RESOURCE).await?,
        vec![("text:bob".into(), "bob-current".into())]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn undeclared_ens_v2_pointer_does_not_claim_unlinked_ens_v1_pre_surface_records() -> Result<()>
{
    const ENS_V2_RESOURCE: &str = "00000000-0000-0000-0000-0000000000e2";
    const UNDECLARED: &str = "0x00000000000000000000000000000000000000e2";
    let incremental = ScratchDatabase::create("project_cross_family_incremental").await?;
    let full = ScratchDatabase::create("project_cross_family_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND event_kind = 'RecordChanged'
               AND logical_name_id = 'ens:0xalice'",
        )
        .bind(CHAIN)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, $2, $3, 1, 'canonical')",
        )
        .bind(Uuid::parse_str(ENS_V2_RESOURCE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            3,
            Some("ens:0xalice"),
            Some(ENS_V2_RESOURCE),
            "ResolverChanged",
            "ens_v2_registry_l1",
            json!({"resolver":UNDECLARED}),
            json!({}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({
                "node":"0xalice",
                "resolver":UNDECLARED,
                "record_key":"text:undeclared-v1-only",
                "record_family":"text",
                "selector_key":"undeclared-v1-only",
                "value_retained":true,
                "value":"undeclared-v1-value"
            }),
            json!({"emitting_address":UNDECLARED}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({
                "node":"0xalice",
                "resolver":RESOLVER,
                "record_key":"text:ens-v1-only",
                "record_family":"text",
                "selector_key":"ens-v1-only",
                "value_retained":true,
                "value":"ens-v1-value"
            }),
            json!({"emitting_address":RESOLVER}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 2).await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 2,
            hash: block_hash(CHAIN, 2),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    assert_eq!(
        record_entry_pairs(incremental.pool(), RESOURCE).await?,
        vec![("text:ens-v1-only".into(), "ens-v1-value".into())],
        "the ENSv1 pointer must retain node-based pre-surface attribution"
    );
    for pool in [incremental.pool(), full.pool()] {
        assert!(
            record_inventory_snapshot(pool, ENS_V2_RESOURCE)
                .await?
                .is_some(),
            "the ENSv2 pointer must retain an explicitly empty inventory row"
        );
        assert_eq!(
            record_entry_pairs(pool, ENS_V2_RESOURCE).await?,
            Vec::<(String, String)>::new(),
            "an ENSv2 pointer must not claim NULL-linked ENSv1 resolver records"
        );
    }
    assert_eq!(
        record_inventory_snapshot(incremental.pool(), ENS_V2_RESOURCE).await?,
        record_inventory_snapshot(full.pool(), ENS_V2_RESOURCE).await?,
        "incremental cross-family isolation diverged from a fresh rebuild"
    );
    incremental.cleanup().await?;
    full.cleanup().await
}

async fn seed_pre_surface_wrapper_boundary_fixture(pool: &PgPool) -> Result<()> {
    seed_project_fixture(pool).await?;
    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RecordChanged'
           AND logical_name_id = 'ens:0xalice'",
    )
    .bind(CHAIN)
    .execute(pool)
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        None,
        None,
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "node":"0xalice",
            "resolver":RESOLVER,
            "record_key":"text:before-surface",
            "record_family":"text",
            "selector_key":"before-surface",
            "value_retained":true,
            "value":"retained-before-surface"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;
    for (block, timestamp) in [(4, 7_776_001), (5, 7_776_004)] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(CHAIN, block))
        .bind(block_hash(CHAIN, block - 1))
        .bind(block)
        .bind(timestamp)
        .execute(pool)
        .await?;
    }
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_wrapper_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control","resolver_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({
            "source_event":"NameRenewed",
            "authority_kind":"wrapper",
            "expiry":7_776_003
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":196_608,"wrapper_state":"emancipated"}),
        json!({}),
    )
    .await?;
    Ok(())
}

async fn lapse_alice_binding(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(4)
         WHERE chain_id = $1 AND logical_name_id = 'ens:0xalice'",
    )
    .bind(CHAIN)
    .execute(pool)
    .await?;
    Ok(())
}

async fn assert_lapsed_surface_without_replacement(pool: &PgPool) -> Result<()> {
    let fixture: (bool, i64) = sqlx::query_as(
        "SELECT EXISTS (
             SELECT 1 FROM name_surfaces
             WHERE chain_id = $1 AND logical_name_id = 'ens:0xalice'
         ), (
             SELECT count(*) FROM surface_bindings
             WHERE chain_id = $1 AND logical_name_id = 'ens:0xalice'
               AND active_to IS NULL
         )",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    assert_eq!(fixture, (true, 0));
    Ok(())
}

async fn record_inventory_snapshot(pool: &PgPool, resource_id: &str) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(row) - 'inserted_at' - 'last_recomputed_at'
         FROM record_inventory_current row WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(resource_id)?)
    .fetch_optional(pool)
    .await?)
}

async fn record_entry_pairs(pool: &PgPool, resource_id: &str) -> Result<Vec<(String, String)>> {
    Ok(sqlx::query_as(
        "SELECT entry ->> 'record_key', entry ->> 'value'
         FROM record_inventory_current inventory
         CROSS JOIN LATERAL jsonb_array_elements(inventory.entries) entry
         WHERE inventory.resource_id = $1
         ORDER BY entry ->> 'record_key'",
    )
    .bind(Uuid::parse_str(resource_id)?)
    .fetch_all(pool)
    .await?)
}

#[tokio::test]
async fn event_free_wrapper_boundary_scopes_permission_named_resolver() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_permission_only_resolver_scope").await?;
    let full =
        ScratchDatabase::create("production_project_permission_only_resolver_scope_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
        extend_permission_only_resolver_fixture(pool).await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    assert_eq!(
        resolver_summary_counts(incremental.pool(), CHAIN, PERMISSION_ONLY_RESOLVER).await?,
        (1, 1, Some("approve".into()))
    );
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 5).await?;

    for pool in [incremental.pool(), full.pool()] {
        let live_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM permissions_current
             WHERE resource_id = $1 AND scope_kind = 'resolver'
               AND lower(scope_detail ->> 'resolver_address') = lower($2)",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .bind(PERMISSION_ONLY_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(live_rows, 0);
    }
    let incremental_counts =
        resolver_summary_counts(incremental.pool(), CHAIN, PERMISSION_ONLY_RESOLVER).await?;
    let full_counts = resolver_summary_counts(full.pool(), CHAIN, PERMISSION_ONLY_RESOLVER).await?;
    assert_eq!(full_counts, (0, 0, None));
    assert_eq!(
        incremental_counts, full_counts,
        "a resolver named only by scoped permission history was not rebuilt"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn wrapper_renewal_refreshes_resurrected_resolver_permission_summary() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_resolver_permission_resurrection").await?;
    let full =
        ScratchDatabase::create("production_project_resolver_permission_resurrection_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
        extend_wrapper_resolver_summary_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            6,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "ExpiryChanged",
            "ens_v1_registrar_l1",
            json!({
                "source_event":"NameRenewed",
                "authority_kind":"wrapper",
                "expiry":9_000_000
            }),
            json!({}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    assert_eq!(
        resolver_summary_counts(incremental.pool(), CHAIN, EQUIVALENCE_V2_RESOLVER).await?,
        (0, 0, None),
        "the expiry boundary must clear the resolver summary before renewal"
    );

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 5,
            hash: block_hash(CHAIN, 5),
        }),
        RunMode::Normal,
        6,
        6,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 6).await?;
    for pool in [incremental.pool(), full.pool()] {
        let powers: Value = sqlx::query_scalar(
            "SELECT effective_powers FROM permissions_current
             WHERE resource_id = $1 AND scope_kind = 'resolver'
               AND lower(scope_detail ->> 'resolver_address') = lower($2)",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .bind(EQUIVALENCE_V2_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(powers, json!(["approve"]));
    }
    let incremental_counts =
        resolver_summary_counts(incremental.pool(), CHAIN, EQUIVALENCE_V2_RESOLVER).await?;
    let full_counts = resolver_summary_counts(full.pool(), CHAIN, EQUIVALENCE_V2_RESOLVER).await?;
    assert_eq!(full_counts, (1, 1, Some("approve".into())));
    assert_eq!(
        incremental_counts, full_counts,
        "a renewed resolver permission row was absent from its resolver summary"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

async fn resolver_summary_counts(
    pool: &PgPool,
    chain: &str,
    resolver: &str,
) -> Result<(i64, i64, Option<String>)> {
    Ok(sqlx::query_as(
        "SELECT
             (declared_summary #>> '{permissions,count}')::bigint,
             (declared_summary #>> '{role_holders,count}')::bigint,
             declared_summary #>> '{role_holders,items,0,effective_powers,0}'
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(chain)
    .bind(resolver)
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
async fn incremental_resolver_builder_stages_only_the_scoped_discovered_resolver() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_resolver_candidate_scope").await?;
    seed_project_fixture(scratch.pool()).await?;

    let resolver_manifest: i64 = sqlx::query_scalar(
        "SELECT source_manifest_id
         FROM normalized_events
         WHERE chain_id = $1
           AND event_kind = 'SourceManifestUpdated'
           AND source_family = 'ens_v1_resolver_l1'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    let source_instance = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind
         ) VALUES ($1, $2, 'contract')",
    )
    .bind(source_instance)
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    for address in [
        "0x00000000000000000000000000000000000000B1",
        "0x00000000000000000000000000000000000000C2",
    ] {
        let resolver_instance = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO contract_instances (
                 contract_instance_id, chain_id, contract_kind
             ) VALUES ($1, $2, 'contract')",
        )
        .bind(resolver_instance)
        .bind(CHAIN)
        .execute(scratch.pool())
        .await?;
        sqlx::query(
            "INSERT INTO contract_instance_addresses (
                 contract_instance_id, chain_id, address,
                 active_from_block_number, active_from_block_hash,
                 source_manifest_id
             ) VALUES ($1, $2, $3, 1, $4, $5)",
        )
        .bind(resolver_instance)
        .bind(CHAIN)
        .bind(address)
        .bind(block_hash(CHAIN, 1))
        .bind(resolver_manifest)
        .execute(scratch.pool())
        .await?;
        sqlx::query(
            "INSERT INTO discovery_edges (
                 chain_id, edge_kind, from_contract_instance_id,
                 to_contract_instance_id, discovery_source, admission_basis,
                 source_manifest_id, active_from_block_number,
                 active_from_block_hash, canonicality_state
             ) VALUES (
                 $1, 'resolver', $2, $3, 'fixture', 'reachable_from_root',
                 $4, 1, $5, 'canonical'
             )",
        )
        .bind(CHAIN)
        .bind(source_instance)
        .bind(resolver_instance)
        .bind(resolver_manifest)
        .bind(block_hash(CHAIN, 1))
        .execute(scratch.pool())
        .await?;
    }

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        None,
        None,
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({"selector":"text:url","value":"https://example.test"}),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    sqlx::query("CREATE SEQUENCE resolver_stage_candidate_count")
        .execute(scratch.pool())
        .await?;
    sqlx::query(
        "ALTER TABLE resolver_current
         ALTER COLUMN last_recomputed_at SET DEFAULT
             to_timestamp(nextval('resolver_stage_candidate_count')::double precision)",
    )
    .execute(scratch.pool())
    .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;

    let staged_candidates: (i64, bool) =
        sqlx::query_as("SELECT last_value, is_called FROM resolver_stage_candidate_count")
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(staged_candidates, (1, true));
    let scoped_resolver: (String, String) = sqlx::query_as(
        "SELECT support_status,
                declared_summary -> 'classification' ->> 'source_family'
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        scoped_resolver,
        ("supported".into(), "ens_v1_resolver_l1".into())
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn unwrapped_registration_ignores_the_prior_wrapper_grace_expiry() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_unwrapped_registrar_expiry").await?;
    seed_project_fixture(scratch.pool()).await?;
    let wrapper_resource = Uuid::new_v4();
    let wrapper_resource_text = wrapper_resource.to_string();
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 2, 'canonical')",
    )
    .bind(wrapper_resource)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 2))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":1_000}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(&wrapper_resource_text),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({
            "source_event":"NameRenewed",
            "authority_kind":"wrapper",
            "registrar_expiry":1_000,
            "expiry":7_777_000
        }),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let projected_expiry: i64 = sqlx::query_scalar(
        "SELECT (declared_summary -> 'registration' ->> 'expiry')::BIGINT
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(projected_expiry, 1_000);
    scratch.cleanup().await
}

#[tokio::test]
async fn wrapped_registration_separates_registrar_and_wrapper_expiry() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_wrapped_expiry_split").await?;
    seed_project_fixture(scratch.pool()).await?;
    let wrapper_resource = Uuid::new_v4();
    let wrapper_resource_text = wrapper_resource.to_string();
    let superseded_registrar_resource = Uuid::new_v4();
    let superseded_registrar_resource_text = superseded_registrar_resource.to_string();
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 2, 'canonical')",
    )
    .bind(wrapper_resource)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 2))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 0, 'canonical')",
    )
    .bind(superseded_registrar_resource)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 0))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(2)
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, active_to, chain_id, block_hash,
             block_number, canonicality_state
         ) VALUES ($1, 'ens:0xalice', $2, 'declared_registry_path',
                   'ens_v1', to_timestamp(0), to_timestamp(1), $3, $4, 0,
                   'canonical')",
    )
    .bind(Uuid::new_v4())
    .bind(superseded_registrar_resource)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 0))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES ($1, 'ens:0xalice', $2, 'declared_registry_path',
                   'ens_v1', to_timestamp(2), $3, $4, 2, 'canonical')",
    )
    .bind(Uuid::new_v4())
    .bind(wrapper_resource)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 2))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(&wrapper_resource_text),
        "ExpiryChanged",
        "ens_v1_wrapper_l1",
        json!({"authority_kind":"wrapper","expiry":500}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(&wrapper_resource_text),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":196_609,"wrapper_state":"locked"}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"source_event":"NameRenewed","authority_kind":"registrar","expiry":1_000}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(&superseded_registrar_resource_text),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"source_event":"NameRenewed","authority_kind":"registrar","expiry":9_999}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM name_current
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(summary["registration"]["expiry"], 1_000);
    assert_eq!(summary["wrapper_state"], "locked");
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_without_resume_revisits_wrapper_timestamp_boundaries() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_wrapper_redo_time_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_wrapper_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":[
                "resource_control", "resolver_control", "set_resolver", "set_ttl",
                "create_subnames", "transfer", "unwrap", "burn_fuses", "approve"
            ],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_wrapper_l1",
        json!({"expiry":7_776_003}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":196_608,"wrapper_state":"emancipated"}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Redo, 4, 4).await?;

    let powers: Value = sqlx::query_scalar(
        "SELECT effective_powers FROM permissions_current WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(powers, json!(["approve"]));
    let controller: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM address_names_current
             WHERE logical_name_id = 'ens:0xalice'
               AND relation = 'effective_controller'
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(!controller);
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_removes_a_retracted_wrapper_expiry_boundary() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_wrapper_expiry_retraction").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        None,
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_wrapper_l1",
        json!({"expiry":7_776_003}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        None,
        Some(RESOURCE),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":196_608,"wrapper_state":"emancipated"}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        None,
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registrar_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control"],
            "grant_source":{"kind":"later_authority"},
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT provenance ? 'wrapper_expiry_boundary'
             FROM permissions_current_resource_summary WHERE resource_id = $1",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .fetch_one(scratch.pool())
        .await?
    );

    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND block_number = 3
           AND event_kind IN ('ExpiryChanged', 'PermissionScopeChanged')",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Redo,
        3,
        3,
    )
    .await?;

    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT provenance ? 'wrapper_expiry_boundary'
             FROM permissions_current_resource_summary WHERE resource_id = $1",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .fetch_one(scratch.pool())
        .await?
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn address_relations_fold_token_and_authority_transfers_in_order() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_address_transfer").await?;
    seed_project_fixture(scratch.pool()).await?;
    let holder = "0x00000000000000000000000000000000000000a2";
    let controller = "0x00000000000000000000000000000000000000a3";
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "TokenControlTransferred",
        "ens_v1_registrar_l1",
        json!({"from":OWNER,"to":holder}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        json!({"owner":controller}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let relations: Vec<(String, String)> = sqlx::query_as(
        "SELECT relation, address FROM address_names_current
         WHERE logical_name_id = 'ens:0xalice'
         ORDER BY relation",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        relations,
        vec![
            ("effective_controller".into(), controller.into()),
            ("registrant".into(), holder.into()),
            ("token_holder".into(), holder.into()),
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn resource_control_revocation_clears_a_non_token_controller() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_address_revocation").await?;
    seed_project_fixture(scratch.pool()).await?;
    sqlx::query("UPDATE resources SET token_lineage_id = NULL WHERE resource_id = $1")
        .bind(Uuid::parse_str(RESOURCE)?)
        .execute(scratch.pool())
        .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registry_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":[],
            "revocation_source":{"kind":"fixture"},
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let relation_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM address_names_current
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(relation_count, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn record_inventory_restores_same_resolver_records_across_resources() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_cross_resource_records").await?;
    seed_project_fixture(scratch.pool()).await?;
    let predecessor = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(predecessor)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        1,
        Some("ens:0xalice"),
        Some(&predecessor.to_string()),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:email",
            "record_family":"text",
            "selector_key":"email",
            "value_retained":true,
            "value":"alice@example.test"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let entries: Value =
        sqlx::query_scalar("SELECT entries FROM record_inventory_current WHERE resource_id = $1")
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(scratch.pool())
            .await?;
    let keys = entries
        .as_array()
        .expect("record entries are an array")
        .iter()
        .map(|entry| entry["record_key"].as_str().unwrap_or_default())
        .collect::<Vec<_>>();
    assert_eq!(keys, vec!["text:email", "text:url"]);
    scratch.cleanup().await
}

#[tokio::test]
async fn record_inventory_normalizes_empty_address_shapes_and_coin60_siblings() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_empty_address_shapes").await?;
    seed_project_fixture(scratch.pool()).await?;
    for (source_family, after_state) in [
        (
            "ens_v1_resolver_l1",
            json!({
                "resolver":RESOLVER,
                "source_event":"AddressChanged",
                "record_key":"addr:2147483649",
                "record_family":"addr",
                "selector_key":"2147483649",
                "value":{"encoding":"hex","bytes":"0x"}
            }),
        ),
        (
            "ens_v2_resolver_l1",
            json!({
                "resolver":RESOLVER,
                "source_event":"AddressChanged",
                "record_key":"addr:2147483650",
                "record_family":"addr",
                "selector_key":"2147483650",
                "address_bytes_hex":"0x"
            }),
        ),
        (
            "ens_v1_resolver_l1",
            json!({
                "resolver":RESOLVER,
                "source_event":"AddressChanged",
                "record_key":"addr:2147483651",
                "record_family":"addr",
                "selector_key":"2147483651",
                "value":{"encoding":"hex","bytes":"0x1234"}
            }),
        ),
        (
            "ens_v1_resolver_l1",
            json!({
                "resolver":RESOLVER,
                "source_event":"AddressChanged",
                "record_key":"addr:60",
                "record_family":"addr",
                "selector_key":"60",
                "value":{"encoding":"hex","bytes":"0x"}
            }),
        ),
        (
            "ens_v1_resolver_l1",
            json!({
                "resolver":RESOLVER,
                "source_event":"AddrChanged",
                "record_key":"addr:60",
                "record_family":"addr",
                "selector_key":"60",
                "value":"0x0000000000000000000000000000000000000000"
            }),
        ),
    ] {
        insert_event(
            scratch.pool(),
            CHAIN,
            3,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "RecordChanged",
            source_family,
            after_state,
            json!({"emitting_address":RESOLVER}),
        )
        .await?;
    }
    sqlx::query(
        "UPDATE normalized_events
         SET transaction_index = 0,
             transaction_hash = '0xcoin60pair',
             log_index = CASE after_state ->> 'source_event'
                 WHEN 'AddressChanged' THEN 10
                 ELSE 11
             END
         WHERE chain_id = $1
           AND block_number = 3
           AND after_state ->> 'record_key' = 'addr:60'",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let entries: Value =
        sqlx::query_scalar("SELECT entries FROM record_inventory_current WHERE resource_id = $1")
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(scratch.pool())
            .await?;
    for key in ["addr:60", "addr:2147483649", "addr:2147483650"] {
        let entry = entries
            .as_array()
            .expect("entries array")
            .iter()
            .find(|entry| entry["record_key"] == key)
            .unwrap_or_else(|| panic!("missing {key}"));
        assert_eq!(
            entry["status"],
            json!("not_found"),
            "wrong status for {key}"
        );
        assert!(entry.get("value").is_none(), "empty {key} retained a value");
    }
    let nonempty = entries
        .as_array()
        .expect("entries array")
        .iter()
        .find(|entry| entry["record_key"] == "addr:2147483651")
        .expect("missing nonempty v1-shape address");
    assert_eq!(nonempty["status"], json!("success"));
    assert_eq!(
        nonempty["value"],
        json!({"encoding":"hex","bytes":"0x1234"})
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn record_inventory_coin60_pair_does_not_override_a_later_same_transaction_write()
-> Result<()> {
    let scratch = ScratchDatabase::create("production_project_coin60_pair_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    for after_state in [
        json!({
            "resolver":RESOLVER,
            "source_event":"AddressChanged",
            "record_key":"addr:60",
            "record_family":"addr",
            "selector_key":"60",
            "value":{"encoding":"hex","bytes":"0x"}
        }),
        json!({
            "resolver":RESOLVER,
            "source_event":"AddrChanged",
            "record_key":"addr:60",
            "record_family":"addr",
            "selector_key":"60",
            "value":"0x0000000000000000000000000000000000000000"
        }),
        json!({
            "resolver":RESOLVER,
            "source_event":"AddrChanged",
            "record_key":"addr:60",
            "record_family":"addr",
            "selector_key":"60",
            "value":"0x0000000000000000000000000000000000000def"
        }),
    ] {
        insert_event(
            scratch.pool(),
            CHAIN,
            3,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "RecordChanged",
            "ens_v1_resolver_l1",
            after_state,
            json!({"emitting_address":RESOLVER}),
        )
        .await?;
    }
    sqlx::query(
        "UPDATE normalized_events
         SET transaction_index = 0,
             transaction_hash = '0xcoin60scope',
             log_index = CASE
                 WHEN after_state ->> 'source_event' = 'AddressChanged' THEN 10
                 WHEN after_state ->> 'value' =
                      '0x0000000000000000000000000000000000000000' THEN 11
                 ELSE 12
             END
         WHERE chain_id = $1
           AND block_number = 3
           AND after_state ->> 'record_key' = 'addr:60'",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let entries: Value =
        sqlx::query_scalar("SELECT entries FROM record_inventory_current WHERE resource_id = $1")
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(scratch.pool())
            .await?;
    let addr60 = entries
        .as_array()
        .expect("entries array")
        .iter()
        .find(|entry| entry["record_key"] == "addr:60")
        .expect("missing addr:60");
    assert_eq!(addr60["status"], json!("success"));
    assert_eq!(
        addr60["value"],
        json!("0x0000000000000000000000000000000000000def")
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn record_inventory_chain_position_keeps_block_number_and_hash_paired() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_record_position_pair").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":RESOLVER}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let position: (i64, String) = sqlx::query_as(
        "SELECT (chain_positions ->> 'block_number')::bigint,
                chain_positions ->> 'block_hash'
         FROM record_inventory_current
         WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(position, (3, block_hash(CHAIN, 3)));
    scratch.cleanup().await
}

#[tokio::test]
async fn phase_runner_redo_repairs_only_the_range_and_retracts_orphaned_output() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_range_redo").await?;
    seed_project_fixture(scratch.pool()).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query(
        "UPDATE name_current SET raw_name = CASE logical_name_id
             WHEN 'ens:0xalice' THEN 'tampered-alice.eth'
             ELSE 'tampered-eth' END",
    )
    .execute(scratch.pool())
    .await?;

    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(CHAIN).await?;
    seed_completed_project_extent(scratch.pool(), CHAIN, 3).await?;
    let phases = PhaseSet::with_ingest_interpret_and_project(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(ProjectPhase::new(scratch.pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-project-redo",
        test_timing(),
    )?;
    let chain = chain_config(CHAIN)?;
    runner
        .redo(
            &chain,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(2, 2)?,
            CancellationToken::new(),
        )
        .await?;

    let names: Vec<(String, String)> = sqlx::query_as(
        "SELECT logical_name_id, raw_name FROM name_current ORDER BY logical_name_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        names,
        vec![
            ("ens:0xalice".into(), "alice.eth".into()),
            ("ens:0xeth".into(), "tampered-eth".into()),
        ]
    );

    sqlx::query(
        "UPDATE normalized_events
         SET canonicality_state = 'orphaned'
         WHERE chain_id = $1
           AND block_number = 3
           AND event_kind IN ('ReverseChanged', 'RecordChanged')",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    runner
        .redo(
            &chain,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(3, 3)?,
            CancellationToken::new(),
        )
        .await?;
    let primary_count: i64 = sqlx::query_scalar("SELECT count(*) FROM primary_names_current")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(primary_count, 0);
    let state: (String, bool, Option<String>) = sqlx::query_as(
        "SELECT phase_status, redo_in_progress, input_content_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
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
async fn incremental_project_does_not_rebuild_unaffected_chain_history() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_incremental_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    let unrelated_lineage = Uuid::new_v4();
    let unrelated_resource = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO token_lineages (
             token_lineage_id, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(unrelated_lineage)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, token_lineage_id, chain_id, block_hash, block_number,
             provenance, canonicality_state
         ) VALUES ($1, $2, $3, $4, 1,
                   '{\"manifest_version\":\"unrelated-history-sentinel\"}',
                   'canonical')",
    )
    .bind(unrelated_resource)
    .bind(unrelated_lineage)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    let successor = "0x00000000000000000000000000000000000000a9";
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":successor}),
        json!({}),
    )
    .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let owner: String = sqlx::query_scalar(
        "SELECT declared_summary -> 'control' ->> 'registry_owner'
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(owner, successor);
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
             SELECT 1 FROM permissions_current_resource_summary
             WHERE resource_id = $1
         )",
        )
        .bind(unrelated_resource)
        .fetch_one(scratch.pool())
        .await?
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn non_resolver_emitters_do_not_create_resolver_rows() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_non_resolver_emitter").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registrar_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control"],
            "grant_source":{"kind":"fixture"},
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    let projected: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)
         )",
    )
    .bind(CHAIN)
    .bind(REGISTRAR)
    .fetch_one(scratch.pool())
    .await?;
    assert!(!projected);
    scratch.cleanup().await
}

#[tokio::test]
async fn non_resolver_resource_delta_preserves_record_inventory_classification() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_resource_delta_classification").await?;
    let full =
        ScratchDatabase::create("production_project_resource_delta_classification_full").await?;
    seed_project_fixture(incremental.pool()).await?;
    seed_project_fixture(full.pool()).await?;
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    for pool in [incremental.pool(), full.pool()] {
        insert_lineage_block(pool, CHAIN, 4).await?;
        insert_event(
            pool,
            CHAIN,
            4,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "AuthorityTransferred",
            "ens_v1_registrar_l1",
            json!({"owner":"0x00000000000000000000000000000000000000a4"}),
            json!({}),
        )
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;

    let incremental_inventory: Value = sqlx::query_scalar(
        "SELECT to_jsonb(inventory) - 'last_recomputed_at' - 'inserted_at'
         FROM record_inventory_current inventory WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(incremental.pool())
    .await?;
    let full_inventory: Value = sqlx::query_scalar(
        "SELECT to_jsonb(inventory) - 'last_recomputed_at' - 'inserted_at'
         FROM record_inventory_current inventory WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(full.pool())
    .await?;

    assert_eq!(incremental_inventory, full_inventory);
    assert_eq!(incremental_inventory["support_status"], json!("supported"));
    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn project_redo_restores_the_surviving_resolver_pointer_like_a_full_rebuild() -> Result<()> {
    const LOSING_RESOLVER: &str = "0x00000000000000000000000000000000000000b9";

    let incremental = ScratchDatabase::create("production_project_resolver_pointer_redo").await?;
    let full = ScratchDatabase::create("production_project_resolver_pointer_redo_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        insert_lineage_block(pool, CHAIN, 4).await?;
    }
    insert_event(
        incremental.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":LOSING_RESOLVER}),
        json!({}),
    )
    .await?;
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;

    capture_resolver_redo_evidence(incremental.pool(), CHAIN, 4, 4).await?;
    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND block_number = 4
           AND event_kind = 'ResolverChanged'",
    )
    .bind(CHAIN)
    .execute(incremental.pool())
    .await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Redo,
        4,
        4,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(full.pool()).await?;
    assert_eq!(
        serving_table_snapshot(incremental.pool()).await?,
        serving_table_snapshot(full.pool()).await?,
        "redo of a removed resolver pointer diverged from the surviving full rebuild"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn orphaned_later_pointer_does_not_hide_node_attributed_records_incrementally() -> Result<()>
{
    const ORPHANED_RESOLVER: &str = "0x00000000000000000000000000000000000000b9";
    const ORPHANED_HASH: &str = "project-fixture-orphaned-block-4";

    let incremental =
        ScratchDatabase::create("project_orphaned_pointer_record_incremental").await?;
    let full = ScratchDatabase::create("project_orphaned_pointer_record_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            1,
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({
                "node":"0xalice",
                "resolver":RESOLVER,
                "record_key":"text:before-surface",
                "record_family":"text",
                "selector_key":"before-surface",
                "value_retained":true,
                "value":"survives-orphaned-pointer"
            }),
            json!({"emitting_address":RESOLVER}),
        )
        .await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    for pool in [incremental.pool(), full.pool()] {
        insert_lineage_block(pool, CHAIN, 4).await?;
        insert_lineage_block(pool, CHAIN, 5).await?;
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, 4, to_timestamp(4), 'orphaned')",
        )
        .bind(CHAIN)
        .bind(ORPHANED_HASH)
        .bind(block_hash(CHAIN, 3))
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            4,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "ResolverChanged",
            "ens_v1_registry_l1",
            json!({"resolver":ORPHANED_RESOLVER}),
            json!({}),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events SET block_hash = $1
             WHERE chain_id = $2 AND block_number = 4
               AND event_kind = 'ResolverChanged'",
        )
        .bind(ORPHANED_HASH)
        .bind(CHAIN)
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            5,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "AuthorityTransferred",
            "ens_v1_registrar_l1",
            json!({"owner":TRANSFER_OWNER}),
            json!({}),
        )
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        5,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 5).await?;

    let inventory_sql = "SELECT to_jsonb(row) - 'inserted_at' - 'last_recomputed_at'
         FROM record_inventory_current row WHERE resource_id = $1";
    let resource_id = Uuid::parse_str(RESOURCE).expect("fixture UUID");
    let incremental_inventory: Value = sqlx::query_scalar(inventory_sql)
        .bind(resource_id)
        .fetch_one(incremental.pool())
        .await?;
    let full_inventory: Value = sqlx::query_scalar(inventory_sql)
        .bind(resource_id)
        .fetch_one(full.pool())
        .await?;
    assert_eq!(incremental_inventory, full_inventory);
    assert!(
        incremental_inventory["entries"]
            .as_array()
            .is_some_and(|entries| {
                entries.iter().any(|entry| {
                    entry["record_key"] == "text:before-surface"
                        && entry["value"] == "survives-orphaned-pointer"
                })
            })
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn candidate_resolver_pointer_cannot_change_resource_delta_scope() -> Result<()> {
    const CANDIDATE_RESOLVER: &str = "0x00000000000000000000000000000000000000b9";

    let scratch = ScratchDatabase::create("production_project_candidate_pointer").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":"0x00000000000000000000000000000000000000a4"}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":CANDIDATE_RESOLVER}),
        json!({}),
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET consumer_visibility = 'candidate',
             migration_correlation_ids = ARRAY['candidate-fixture']::text[]
         WHERE chain_id = $1 AND block_number = 4
           AND event_kind = 'ResolverChanged'",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query(
        "INSERT INTO resolver_current (
             chain_id, resolver_address, declared_summary, support_status,
             unsupported_reason, provenance, chain_positions,
             canonicality_summary, manifest_version
         ) SELECT
             chain_id, lower($2), declared_summary || '{\"sentinel\":true}'::jsonb,
             support_status, unsupported_reason, provenance, chain_positions,
             canonicality_summary, manifest_version
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($3)",
    )
    .bind(CHAIN)
    .bind(CANDIDATE_RESOLVER)
    .bind(RESOLVER)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let sentinel: Option<Value> = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(CANDIDATE_RESOLVER)
    .fetch_optional(scratch.pool())
    .await?;
    assert_eq!(
        sentinel
            .as_ref()
            .and_then(|summary| summary.get("sentinel")),
        Some(&json!(true)),
        "candidate resolver evidence changed an unscoped resolver row"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn surface_unbind_rebuilds_the_pointer_resolver_binding_summary() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_surface_unbind_scope").await?;
    let full = ScratchDatabase::create("production_project_surface_unbind_scope_full").await?;

    for pool in [incremental.pool(), full.pool()] {
        seed_basenames_project_fixture(pool).await?;
    }
    run_project(incremental.pool(), BASE_CHAIN, None, RunMode::Normal, 0, 2).await?;

    for pool in [incremental.pool(), full.pool()] {
        sqlx::query(
            "UPDATE surface_bindings SET active_to = to_timestamp(3)
             WHERE logical_name_id = 'basenames:0xalice-base'",
        )
        .execute(pool)
        .await?;
        insert_namespaced_event(
            pool,
            "basenames",
            BASE_CHAIN,
            3,
            Some("basenames:0xalice-base"),
            Some(BASENAMES_RESOURCE),
            "SurfaceUnbound",
            "basenames_base_registry",
            1,
            json!({}),
            json!({}),
        )
        .await?;
    }

    run_project(
        incremental.pool(),
        BASE_CHAIN,
        Some(Marker {
            number: 2,
            hash: block_hash(BASE_CHAIN, 2),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    run_project(full.pool(), BASE_CHAIN, None, RunMode::Normal, 0, 3).await?;

    let incremental_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(BASE_CHAIN)
    .bind(BASENAMES_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    let full_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(BASE_CHAIN)
    .bind(BASENAMES_RESOLVER)
    .fetch_one(full.pool())
    .await?;

    assert_eq!(incremental_summary, full_summary);
    assert!(full_summary["bindings"]["items"][0]["resource_id"].is_null());
    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn unlinked_record_delta_does_not_discover_a_resolver() -> Result<()> {
    const UNLINKED_RESOLVER: &str = "0x00000000000000000000000000000000000000b9";

    let incremental = ScratchDatabase::create("production_project_unlinked_record_delta").await?;
    let full = ScratchDatabase::create("production_project_unlinked_record_delta_full").await?;
    seed_project_fixture(incremental.pool()).await?;
    seed_project_fixture(full.pool()).await?;
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    for pool in [incremental.pool(), full.pool()] {
        insert_lineage_block(pool, CHAIN, 4).await?;
        insert_event(
            pool,
            CHAIN,
            4,
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({
                "resolver":UNLINKED_RESOLVER,
                "record_key":"text:url",
                "record_family":"text",
                "selector_key":"url",
                "value_retained":true,
                "value":"https://unlinked.example.test"
            }),
            json!({"emitting_address":UNLINKED_RESOLVER}),
        )
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;

    let projected: (bool, bool) = (
        sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM resolver_current
                 WHERE chain_id = $1 AND resolver_address = lower($2)
             )",
        )
        .bind(CHAIN)
        .bind(UNLINKED_RESOLVER)
        .fetch_one(incremental.pool())
        .await?,
        sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM resolver_current
                 WHERE chain_id = $1 AND resolver_address = lower($2)
             )",
        )
        .bind(CHAIN)
        .bind(UNLINKED_RESOLVER)
        .fetch_one(full.pool())
        .await?,
    );
    assert_eq!(projected, (false, false));

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn record_delta_rebuilds_emitter_but_not_other_current_resolver_dependents() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_record_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    let bob_resource = "00000000-0000-0000-0000-0000000000b0";
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(bob_resource)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'ens:0xbob', 'ens', 'bob.eth', ARRAY['bob', 'eth'],
             decode('00', 'hex'), '0xbob', ARRAY['0xbob-label', '0xeth'], $1,
             'active', $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens:0xbob', $2, 'declared_registry_path',
             'ens_v1', to_timestamp(1), $3, $4, 1, 'canonical'
         )",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::parse_str(bob_resource)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xbob"),
        Some(bob_resource),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xbob"),
        Some(bob_resource),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:description",
            "record_family":"text",
            "selector_key":"description",
            "value_retained":true,
            "value":"bob"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let resolver_summary_before: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE name_current SET last_recomputed_at = to_timestamp(0)
         WHERE logical_name_id = 'ens:0xbob'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE record_inventory_current SET last_recomputed_at = to_timestamp(0)
         WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(bob_resource)?)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE resolver_current SET last_recomputed_at = to_timestamp(0)
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:url",
            "record_family":"text",
            "selector_key":"url",
            "value_retained":true,
            "value":"https://changed.example.test"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;

    let clocks: (bool, bool, bool) = sqlx::query_as(
        "SELECT
             (SELECT last_recomputed_at = to_timestamp(0) FROM name_current
              WHERE logical_name_id = 'ens:0xbob'),
             (SELECT last_recomputed_at = to_timestamp(0) FROM record_inventory_current
              WHERE resource_id = $1),
             (SELECT last_recomputed_at > to_timestamp(0) FROM resolver_current
              WHERE chain_id = $2 AND resolver_address = lower($3))",
    )
    .bind(Uuid::parse_str(bob_resource)?)
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(clocks, (true, true, true));
    let resolver_summary_after: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(resolver_summary_after, resolver_summary_before);
    scratch.cleanup().await
}

#[tokio::test]
async fn incremental_resolver_binding_summary_isolated_by_chain() -> Result<()> {
    const OTHER_CHAIN: &str = "other-project-fixture";

    let scratch = ScratchDatabase::create("production_project_resolver_chain_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    seed_lineage(scratch.pool(), OTHER_CHAIN, 1).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'ens:0xforeign', 'ens', 'foreign.eth', ARRAY['foreign', 'eth'],
             decode('00', 'hex'), '0xforeign', ARRAY['0xforeign-label', '0xeth'], $1,
             'active', $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(OTHER_CHAIN)
    .bind(block_hash(OTHER_CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO resolver_current (
             chain_id, resolver_address, declared_summary, support_status,
             unsupported_reason, provenance, chain_positions,
             canonicality_summary, manifest_version
         )
         SELECT $1, resolver_address,
                jsonb_set(
                    declared_summary,
                    '{classification,source_family}',
                    '\"basenames_base_resolver\"'::jsonb
                ),
                support_status, unsupported_reason, provenance, chain_positions,
                canonicality_summary, manifest_version
         FROM resolver_current
         WHERE chain_id = $2 AND resolver_address = lower($3)",
    )
    .bind(OTHER_CHAIN)
    .bind(CHAIN)
    .bind(RESOLVER)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_current (
             logical_name_id, namespace, raw_name, namehash,
             declared_summary, support_status, provenance, chain_positions,
             canonicality_summary, manifest_version
         ) VALUES (
             'ens:0xforeign', 'ens', 'foreign.eth', '0xforeign',
             jsonb_build_object(
                 'resolver', jsonb_build_object('chain_id', $1, 'address', lower($2))
             ),
             'supported', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1
         )",
    )
    .bind(OTHER_CHAIN)
    .bind(RESOLVER)
    .execute(scratch.pool())
    .await?;

    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_resolver_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resolver","chain_id":CHAIN,"resolver_address":RESOLVER},
            "effective_powers":["record_write"],
            "grant_source":{"kind":"fixture"},
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let incremental: Option<Value> = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_optional(scratch.pool())
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    let full: Option<Value> = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_optional(scratch.pool())
    .await?;

    assert_eq!(incremental, full);
    scratch.cleanup().await
}

#[tokio::test]
async fn incremental_wrapper_expiry_only_tick_retains_child_families() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_wrapper_child_scope").await?;
    let full = ScratchDatabase::create("production_project_wrapper_child_scope_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
    }

    for target in 1..=4 {
        let previous = (target > 1).then(|| Marker {
            number: target - 1,
            hash: block_hash(CHAIN, target - 1),
        });
        run_project(
            incremental.pool(),
            CHAIN,
            previous,
            RunMode::Normal,
            target,
            target,
        )
        .await?;
    }

    let mut incremental_children = Vec::new();
    let mut full_children = Vec::new();
    for target in [5, 6] {
        let event_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM normalized_events
             WHERE chain_id = $1 AND block_number = $2",
        )
        .bind(CHAIN)
        .bind(target)
        .fetch_one(incremental.pool())
        .await?;
        assert_eq!(event_count, 0, "the {target} tick must be event-free");

        run_project(
            incremental.pool(),
            CHAIN,
            Some(Marker {
                number: target - 1,
                hash: block_hash(CHAIN, target - 1),
            }),
            RunMode::Normal,
            target,
            target,
        )
        .await?;
        run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, target).await?;

        let incremental_v1 = selected_children(
            incremental.pool(),
            "ens:0xeth",
            &["ens:0xalice", "ens:0xhostile"],
        )
        .await?;
        let full_v1 =
            selected_children(full.pool(), "ens:0xeth", &["ens:0xalice", "ens:0xhostile"]).await?;
        let incremental_v2 = selected_children(
            incremental.pool(),
            "ens:0xequivalence-parent",
            &["ens:0xequivalence-c0", "ens:0xequivalence-c1"],
        )
        .await?;
        let full_v2 = selected_children(
            full.pool(),
            "ens:0xequivalence-parent",
            &["ens:0xequivalence-c0", "ens:0xequivalence-c1"],
        )
        .await?;
        incremental_children.push((target, incremental_v1, incremental_v2));
        full_children.push((target, full_v1, full_v2));
    }
    assert_eq!(
        full_children,
        vec![
            (
                5,
                vec!["ens:0xalice".into(), "ens:0xhostile".into()],
                vec!["ens:0xequivalence-c1".into()]
            ),
            (
                6,
                vec!["ens:0xalice".into(), "ens:0xhostile".into()],
                vec!["ens:0xequivalence-c1".into()]
            ),
        ]
    );
    assert_eq!(
        incremental_children, full_children,
        "the expiry tick or its event-free successor lost a child family"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

async fn selected_children(
    pool: &PgPool,
    parent: &str,
    candidates: &[&str],
) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT child_logical_name_id
         FROM children_current
         WHERE parent_logical_name_id = $1
           AND child_logical_name_id = ANY($2)
         ORDER BY child_logical_name_id",
    )
    .bind(parent)
    .bind(candidates)
    .fetch_all(pool)
    .await?)
}

#[tokio::test]
async fn incremental_sibling_update_retains_unbound_migrated_name_v2_subnames() -> Result<()> {
    let incremental = ScratchDatabase::create("project_unbound_migrated_subnames").await?;
    let full = ScratchDatabase::create("project_unbound_migrated_subnames_full").await?;

    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, $2, $3, 1, 'canonical')",
        )
        .bind(Uuid::parse_str(EQUIVALENCE_PARENT_RESOURCE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        sqlx::query(
            "UPDATE surface_bindings SET resource_id = $1
             WHERE surface_binding_id = $2",
        )
        .bind(Uuid::parse_str(EQUIVALENCE_PARENT_RESOURCE)?)
        .bind(Uuid::parse_str(EQUIVALENCE_PARENT_BINDING)?)
        .execute(pool)
        .await?;
        sqlx::query(
            "UPDATE normalized_events SET resource_id = $1
             WHERE chain_id = $2
               AND logical_name_id = 'ens:0xequivalence-parent'
               AND resource_id = $3",
        )
        .bind(Uuid::parse_str(EQUIVALENCE_PARENT_RESOURCE)?)
        .bind(CHAIN)
        .bind(Uuid::parse_str(RESOURCE)?)
        .execute(pool)
        .await?;

        let binding_resources: Vec<Uuid> = sqlx::query_scalar(
            "SELECT resource_id FROM surface_bindings
             WHERE logical_name_id IN ('ens:0xalice', 'ens:0xequivalence-parent')
             ORDER BY logical_name_id",
        )
        .fetch_all(pool)
        .await?;
        assert_eq!(binding_resources.len(), 2);
        assert_ne!(binding_resources[0], binding_resources[1]);

        insert_lineage_block(pool, CHAIN, 9).await?;
        insert_event(
            pool,
            CHAIN,
            9,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "AuthorityTransferred",
            "ens_v1_registrar_l1",
            json!({"owner":"0x0000000000000000000000000000000000000099"}),
            json!({}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 8).await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 8,
            hash: block_hash(CHAIN, 8),
        }),
        RunMode::Normal,
        9,
        9,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 9).await?;

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(full.pool()).await?;
    let incremental_children: Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(child) ORDER BY child_logical_name_id), '[]'::jsonb)
         FROM children_current child
         WHERE parent_logical_name_id = 'ens:0xequivalence-parent'",
    )
    .fetch_one(incremental.pool())
    .await?;
    let rebuilt_children: Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(child) ORDER BY child_logical_name_id), '[]'::jsonb)
         FROM children_current child
         WHERE parent_logical_name_id = 'ens:0xequivalence-parent'",
    )
    .fetch_one(full.pool())
    .await?;
    assert_eq!(
        incremental_children, rebuilt_children,
        "an unrelated sibling update must retain the migrated name's ENSv2 subname records"
    );
    assert_eq!(rebuilt_children.as_array().map(Vec::len), Some(1));

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn incremental_ticks_match_one_full_rebuild_across_all_eight_serving_tables() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_incremental_equivalence").await?;
    let full = ScratchDatabase::create("production_project_full_equivalence").await?;
    seed_project_fixture(incremental.pool()).await?;
    seed_project_fixture(full.pool()).await?;
    for pool in [incremental.pool(), full.pool()] {
        extend_incremental_equivalence_fixture(pool).await?;
        extend_equivalence_registrar_transfer_fixture(pool).await?;
    }

    for target in 1..=8 {
        let previous = (target > 1).then(|| Marker {
            number: target - 1,
            hash: block_hash(CHAIN, target - 1),
        });
        run_project(
            incremental.pool(),
            CHAIN,
            previous,
            RunMode::Normal,
            target,
            target,
        )
        .await?;

        if target == 3 {
            let shared_pointer_count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM name_current
                 WHERE declared_summary #>> '{resolver,address}' = lower($1)",
            )
            .bind(RESOLVER)
            .fetch_one(incremental.pool())
            .await?;
            assert_eq!(shared_pointer_count, 2, "fixture must share the resolver");
        }
        if target == 4 {
            let pointers: Vec<(String, String)> = sqlx::query_as(
                "SELECT logical_name_id,
                        declared_summary #>> '{resolver,address}'
                 FROM name_current
                 WHERE logical_name_id IN ('ens:0xalice', 'ens:0xbob')
                 ORDER BY logical_name_id",
            )
            .fetch_all(incremental.pool())
            .await?;
            assert_eq!(
                pointers,
                vec![
                    ("ens:0xalice".into(), EQUIVALENCE_V2_RESOLVER.into()),
                    ("ens:0xbob".into(), RESOLVER.into()),
                ],
                "ResolverChanged must replace only Alice's current pointer"
            );
            let grace_powers: Value = sqlx::query_scalar(
                "SELECT effective_powers FROM permissions_current WHERE resource_id = $1",
            )
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(incremental.pool())
            .await?;
            assert_eq!(grace_powers, json!(["approve"]));
            let children: Vec<String> = sqlx::query_scalar(
                "SELECT child_logical_name_id FROM children_current
                 WHERE parent_logical_name_id = 'ens:0xequivalence-parent'
                 ORDER BY child_logical_name_id",
            )
            .fetch_all(incremental.pool())
            .await?;
            assert_eq!(
                children,
                vec!["ens:0xequivalence-c1"],
                "SubregistryChanged must stage the newly selected registry's children"
            );
        }
        if target == 5 {
            let event_count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM normalized_events
                 WHERE chain_id = $1 AND block_number = 5",
            )
            .bind(CHAIN)
            .fetch_one(incremental.pool())
            .await?;
            assert_eq!(event_count, 0, "expiry must cross in an event-free window");
            let wrapper_permission_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1 FROM permissions_current WHERE resource_id = $1
                 )",
            )
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(incremental.pool())
            .await?;
            assert!(
                !wrapper_permission_exists,
                "expiry boundary must be crossed"
            );
            let eth_children = selected_children(
                incremental.pool(),
                "ens:0xeth",
                &["ens:0xalice", "ens:0xbob", "ens:0xhostile"],
            )
            .await?;
            assert_eq!(
                eth_children,
                vec!["ens:0xalice", "ens:0xbob", "ens:0xhostile"],
                "closure-only expiry scope must retain the whole .eth child family"
            );
            let v2_children = selected_children(
                incremental.pool(),
                "ens:0xequivalence-parent",
                &["ens:0xequivalence-c0", "ens:0xequivalence-c1"],
            )
            .await?;
            assert_eq!(
                v2_children,
                vec!["ens:0xequivalence-c1"],
                "closure-only expiry scope must retain the selected ENSv2 registry family"
            );
        }
        if target == 6 {
            let event_count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM normalized_events
                 WHERE chain_id = $1 AND block_number = 6",
            )
            .bind(CHAIN)
            .fetch_one(incremental.pool())
            .await?;
            assert_eq!(event_count, 0, "the follow-up tick must remain event-free");
        }
        if target == 7 {
            let inventory_status: String = sqlx::query_scalar(
                "SELECT support_status FROM record_inventory_current WHERE resource_id = $1",
            )
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(incremental.pool())
            .await?;
            assert_eq!(
                inventory_status, "supported",
                "Upgraded must rebuild the resolver's current resource dependents"
            );
            let transferred_resolver_permission: (Option<String>, Option<String>, Option<String>) =
                sqlx::query_as(
                    "SELECT
                         declared_summary #>> '{permissions,items,0,subject}',
                         declared_summary #>>
                             '{permissions,items,0,grant_source,source_event_kind}',
                         declared_summary #>> '{role_holders,items,0,subject}'
                     FROM resolver_current
                     WHERE chain_id = $1 AND resolver_address = lower($2)",
                )
                .bind(CHAIN)
                .bind(EQUIVALENCE_TRANSFER_RESOLVER)
                .fetch_one(incremental.pool())
                .await?;
            assert_eq!(
                transferred_resolver_permission,
                (
                    Some(TRANSFER_OWNER.into()),
                    Some("TokenControlTransferred".into()),
                    Some(TRANSFER_OWNER.into())
                ),
                "registrar transfer must refresh the resolver permission summary"
            );
        }
        if target == 8 {
            let children: Vec<String> = sqlx::query_scalar(
                "SELECT child_logical_name_id FROM children_current
                 WHERE parent_logical_name_id = 'ens:0xequivalence-parent'
                 ORDER BY child_logical_name_id",
            )
            .fetch_all(incremental.pool())
            .await?;
            assert_eq!(
                children,
                vec!["ens:0xequivalence-c1"],
                "SubregistryChanged must replace the old registry's children"
            );
        }
    }
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 8).await?;

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(full.pool()).await?;
    assert_eq!(
        serving_table_snapshot(incremental.pool()).await?,
        serving_table_snapshot(full.pool()).await?,
        "incremental Project output diverged from a full rebuild"
    );

    let resolver_states: Vec<(String, String)> = sqlx::query_as(
        "SELECT resolver_address, support_status
         FROM resolver_current
         WHERE chain_id = $1
           AND resolver_address IN (lower($2), lower($3), lower($4))
         ORDER BY resolver_address",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .bind(EQUIVALENCE_TRANSFER_RESOLVER)
    .fetch_all(incremental.pool())
    .await?;
    assert_eq!(
        resolver_states,
        vec![
            (RESOLVER.into(), "supported".into()),
            (EQUIVALENCE_V2_RESOLVER.into(), "supported".into()),
            (EQUIVALENCE_TRANSFER_RESOLVER.into(), "supported".into()),
        ],
        "the shared and moved resolver rows must survive"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn narrow_final_tick_matches_full_rebuild_across_all_eight_serving_tables() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_narrow_final_equivalence").await?;
    let full = ScratchDatabase::create("production_project_narrow_final_equivalence_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
        extend_equivalence_registrar_transfer_fixture(pool).await?;
        extend_narrow_final_equivalence_fixture(pool).await?;
    }

    for target in 1..=10 {
        let previous = (target > 1).then(|| Marker {
            number: target - 1,
            hash: block_hash(CHAIN, target - 1),
        });
        run_project(
            incremental.pool(),
            CHAIN,
            previous,
            RunMode::Normal,
            target,
            target,
        )
        .await?;
    }
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 10).await?;

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(full.pool()).await?;
    assert_ne!(
        serving_table_snapshot(incremental.pool()).await?,
        serving_table_snapshot(full.pool()).await?,
        "the narrow final tick must leave at least one unscoped row at its prior stamp"
    );
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(full.pool()).await?,
        "narrow incremental Project output diverged semantically from a full rebuild"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn topology_staged_sibling_is_not_double_counted_in_resolver_bindings() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_topology_binding_probe").await?;
    let full = ScratchDatabase::create("production_project_topology_binding_probe_full").await?;

    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            6,
            Some("ens:0xbob"),
            Some(EQUIVALENCE_BOB_RESOURCE),
            "ResolverChanged",
            "ens_v2_registry_l1",
            json!({"resolver":EQUIVALENCE_V2_RESOLVER}),
            json!({}),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events SET before_state = jsonb_build_object('resolver', lower($1))
             WHERE chain_id = $2 AND block_number = 6
               AND logical_name_id = 'ens:0xbob' AND event_kind = 'ResolverChanged'",
        )
        .bind(RESOLVER)
        .bind(CHAIN)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, 9, to_timestamp(7776008), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 9))
        .bind(block_hash(CHAIN, 8))
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            9,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "SubregistryChanged",
            "ens_v1_registry_l1",
            json!({
                "node":"0xeth",
                "child_node":"0xalice",
                "labelhash":"0xalice-label",
                "owner":OWNER
            }),
            json!({}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            9,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "PermissionChanged",
            "ens_v2_resolver_l1",
            json!({
                "subject":OWNER,
                "scope":{
                    "kind":"resolver",
                    "chain_id":CHAIN,
                    "resolver_address":EQUIVALENCE_V2_RESOLVER
                },
                "effective_powers":["record_write"],
                "grant_source":{"kind":"fixture"},
                "inheritance_path":[],
                "transfer_behavior":"retain"
            }),
            json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 8).await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 8,
            hash: block_hash(CHAIN, 8),
        }),
        RunMode::Normal,
        9,
        9,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 9).await?;

    let incremental_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    let full_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(full.pool())
    .await?;

    assert_eq!(
        incremental_summary, full_summary,
        "topology-only staging must not duplicate a retained resolver binding"
    );
    assert_eq!(full_summary["bindings"]["count"], json!(2));

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn topology_staged_sibling_is_not_double_counted_in_resolver_permissions() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_topology_permission_probe").await?;
    let full = ScratchDatabase::create("production_project_topology_permission_probe_full").await?;

    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            6,
            Some("ens:0xbob"),
            Some(EQUIVALENCE_BOB_RESOURCE),
            "PermissionChanged",
            "ens_v2_resolver_l1",
            json!({
                "subject":OWNER,
                "scope":{
                    "kind":"resolver",
                    "chain_id":CHAIN,
                    "resolver_address":EQUIVALENCE_V2_RESOLVER
                },
                "effective_powers":["record_write"],
                "grant_source":{"kind":"fixture"},
                "inheritance_path":[],
                "transfer_behavior":"retain"
            }),
            json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
        )
        .await?;
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, 9, to_timestamp(7776008), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 9))
        .bind(block_hash(CHAIN, 8))
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            9,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "SubregistryChanged",
            "ens_v1_registry_l1",
            json!({
                "node":"0xeth",
                "child_node":"0xalice",
                "labelhash":"0xalice-label",
                "owner":OWNER
            }),
            json!({}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            9,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "PermissionChanged",
            "ens_v2_resolver_l1",
            json!({
                "subject":OWNER,
                "scope":{
                    "kind":"resolver",
                    "chain_id":CHAIN,
                    "resolver_address":EQUIVALENCE_V2_RESOLVER
                },
                "effective_powers":["record_write"],
                "grant_source":{"kind":"fixture"},
                "inheritance_path":[],
                "transfer_behavior":"retain"
            }),
            json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 8).await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 8,
            hash: block_hash(CHAIN, 8),
        }),
        RunMode::Normal,
        9,
        9,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 9).await?;

    let incremental_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    let full_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(full.pool())
    .await?;

    assert_eq!(full_summary["permissions"]["count"], json!(1));
    assert_eq!(
        incremental_summary["permissions"]["count"], full_summary["permissions"]["count"],
        "topology-only staging double-counted a retained resolver permission"
    );
    assert_eq!(
        incremental_summary, full_summary,
        "topology-only staging changed resolver permission, event, or role summaries"
    );

    for pool in [incremental.pool(), full.pool()] {
        let durable_permissions: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM permissions_current
             WHERE resource_id = $1 AND scope_kind = 'resolver'
               AND lower(scope_detail ->> 'resolver_address') = lower($2)",
        )
        .bind(Uuid::parse_str(EQUIVALENCE_BOB_RESOURCE)?)
        .bind(EQUIVALENCE_V2_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(durable_permissions, 1);
    }

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn atomic_swap_never_exposes_a_partially_replaced_projection_set() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_atomic_swap").await?;
    seed_project_fixture(scratch.pool()).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let before = projection_counts(scratch.pool()).await?;
    let before_authority = projection_authority_pair(scratch.pool()).await?;
    assert_eq!(before_authority, (OWNER.into(), OWNER.into()));
    let successor = "0x00000000000000000000000000000000000000a2";
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        json!({"owner":successor}),
        json!({}),
    )
    .await?;

    sqlx::raw_sql(
        "CREATE FUNCTION pause_project_publication() RETURNS trigger AS $$
         BEGIN
             PERFORM pg_sleep(1.0);
             RETURN NEW;
         END $$ LANGUAGE plpgsql;
         CREATE TRIGGER pause_project_publication
         BEFORE INSERT ON name_current
         FOR EACH STATEMENT EXECUTE FUNCTION pause_project_publication();",
    )
    .execute(scratch.pool())
    .await?;

    let engine = Engine::new(scratch.pool().clone());
    let rebuild = tokio::spawn(async move {
        engine
            .run_batch(BatchRequest {
                chain_id: CHAIN.into(),
                target_block: 3,
                affected_from_block: 0,
                affected_to_block: 3,
                resume_current: None,
                mode: RunMode::Normal,
            })
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(projection_counts(scratch.pool()).await?, before);
    assert_eq!(
        projection_authority_pair(scratch.pool()).await?,
        before_authority,
        "reader must retain the complete prior projection set"
    );
    rebuild.await??;
    assert_eq!(projection_counts(scratch.pool()).await?, before);
    assert_eq!(
        projection_authority_pair(scratch.pool()).await?,
        (successor.into(), successor.into()),
        "reader must observe the complete successor projection set"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn manifest_admission_reclassifies_v2_resolver_inline_without_a_queue() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_reconvergence").await?;
    let chain = "project-reconvergence";
    let resolver_manifest = seed_reconvergence_fixture(scratch.pool(), chain).await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 1).await?;
    let before: String = sqlx::query_scalar("SELECT support_status FROM resolver_current")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(before, "unsupported");
    let resolver_count: i64 = sqlx::query_scalar("SELECT count(*) FROM resolver_current")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(
        resolver_count, 1,
        "registry proxy upgrades are not resolvers"
    );

    sqlx::query(
        r#"UPDATE manifest_versions
         SET manifest_payload = jsonb_set(
             manifest_payload,
             '{resolver_implementations}',
             '[{"role":"permissioned_resolver","address":"0x00000000000000000000000000000000000000c1","read_features":["ensip19_default_address"]}]'::jsonb
         )
         WHERE chain_id = $1 AND source_family = 'ens_v2_resolver_l1'"#,
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let manifest_payload: Value =
        sqlx::query_scalar("SELECT manifest_payload FROM manifest_versions WHERE manifest_id = $1")
            .bind(resolver_manifest)
            .fetch_one(scratch.pool())
            .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 1).await?;
    let without_event: String = sqlx::query_scalar("SELECT support_status FROM resolver_current")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(
        without_event, "unsupported",
        "manifest bytes alone do not bypass SourceManifestUpdated"
    );
    insert_manifest_update_event(
        scratch.pool(),
        chain,
        "ens_v2_resolver_l1",
        resolver_manifest,
        manifest_payload,
    )
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 1,
            hash: block_hash(chain, 1),
        }),
        RunMode::Normal,
        0,
        0,
    )
    .await?;
    let after: (String, String, Value) = sqlx::query_as(
        "SELECT support_status,
                declared_summary -> 'classification' ->> 'basis',
                declared_summary -> 'classification' -> 'read_features'
         FROM resolver_current",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        after,
        (
            "supported".into(),
            "erc1967_upgraded_history".into(),
            json!(["ensip19_default_address"]),
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn authority_selector_uses_successor_after_closed_predecessor_across_positions() -> Result<()>
{
    for (database_prefix, chain, v2_block, v1_block, boundary_block) in [
        (
            "production_project_authority_v2_older",
            "project-authority-v2-older",
            1,
            3,
            5,
        ),
        (
            "production_project_authority_v2_newer",
            "project-authority-v2-newer",
            4,
            1,
            4,
        ),
    ] {
        let scratch = ScratchDatabase::create(database_prefix).await?;
        let logical_name_id = seed_closed_predecessor_cross_arm_fixture(
            scratch.pool(),
            chain,
            v2_block,
            v1_block,
            boundary_block,
        )
        .await?;
        sqlx::query(
            "UPDATE surface_bindings
             SET surface_binding_id = CASE authority_arm
                 WHEN 'ens_v2' THEN '00000000-0000-0000-0000-000000000010'::uuid
                 ELSE 'ffffffff-ffff-ffff-ffff-fffffffffff0'::uuid
             END
             WHERE chain_id = $1 AND logical_name_id = $2",
        )
        .bind(chain)
        .bind(&logical_name_id)
        .execute(scratch.pool())
        .await?;

        let bindings: Vec<(
            Uuid,
            String,
            time::OffsetDateTime,
            Option<time::OffsetDateTime>,
        )> = sqlx::query_as(
            "SELECT DISTINCT ON (authority_arm)
                    surface_binding_id, authority_arm, active_from, active_to
             FROM surface_bindings
             WHERE chain_id = $1 AND logical_name_id = $2
             ORDER BY authority_arm, block_number DESC, surface_binding_id DESC",
        )
        .bind(chain)
        .bind(&logical_name_id)
        .fetch_all(scratch.pool())
        .await?;
        assert_eq!(
            bindings
                .iter()
                .map(|(_, arm, _, _)| arm.as_str())
                .collect::<Vec<_>>(),
            ["ens_v1", "ens_v2"],
            "the post-transition fixture must retain both authority histories"
        );
        assert!(
            bindings[0].3.is_some(),
            "the ENSv1 predecessor is closed: {bindings:?}"
        );
        assert!(bindings[1].3.is_none(), "the ENSv2 successor is current");
        if database_prefix.ends_with("v2_older") {
            assert!(
                bindings[1].2 < bindings[0].2,
                "the older-successor case must place ENSv2 before ENSv1: {bindings:?}"
            );
        }

        let (v2_binding, v2_resource, proof_identity) = insert_activated_authority_proof(
            scratch.pool(),
            chain,
            &logical_name_id,
            "unwrapped",
            Some(boundary_block),
        )
        .await?;
        let proof_block: i64 = sqlx::query_scalar(
            "SELECT block_number FROM normalized_events WHERE event_identity = $1",
        )
        .bind(&proof_identity)
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(proof_block, boundary_block);
        run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
        let projected: (Uuid, Uuid, Value) = sqlx::query_as(
            "SELECT surface_binding_id, resource_id,
                    provenance -> 'authority_selection'
             FROM name_current WHERE logical_name_id = $1",
        )
        .bind(&logical_name_id)
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(projected.0, v2_binding);
        assert_ne!(projected.0, bindings.iter().map(|row| row.0).max().unwrap());
        assert_eq!(projected.1, v2_resource);
        assert_eq!(projected.2["authority_arm"], "ens_v2");
        assert_eq!(projected.2["proof_kind"], "migration_authority_transition");
        assert_eq!(projected.2["proof_event_identity"], proof_identity);
        assert_eq!(projected.2["transition_id"], "authority-proof-fixture");
        let leaked_registry_owner: Option<String> = sqlx::query_scalar(
            "SELECT registry_owner FROM permissions_current_resource_summary
             WHERE resource_id = $1",
        )
        .bind(v2_resource)
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(
            leaked_registry_owner, None,
            "ENSv1 registry evidence must not bind the selected ENSv2 resource"
        );

        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn authority_selector_follows_post_migration_v2_binding_churn() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_v2_binding_churn").await?;
    let chain = "authority-v2-binding-churn";
    let logical_name_id =
        seed_closed_predecessor_cross_arm_fixture(scratch.pool(), chain, 4, 1, 4).await?;
    let (successor_binding, successor_resource, _) = insert_activated_authority_proof(
        scratch.pool(),
        chain,
        &logical_name_id,
        "unwrapped",
        None,
    )
    .await?;
    let churned_binding = Uuid::parse_str("00000000-0000-0000-0000-000000000020")?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(5)
         WHERE surface_binding_id = $1",
    )
    .bind(successor_binding)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number,
             provenance, canonicality_state
         )
         SELECT $1, logical_name_id, resource_id, binding_kind, authority_arm,
                to_timestamp(5), chain_id, $2, 5,
                jsonb_build_object('transaction_index', 0, 'log_index', 0),
                canonicality_state
         FROM surface_bindings WHERE surface_binding_id = $3",
    )
    .bind(churned_binding)
    .bind(block_hash(chain, 5))
    .bind(successor_binding)
    .execute(scratch.pool())
    .await?;

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    let projected: (Uuid, Uuid) = sqlx::query_as(
        "SELECT surface_binding_id, resource_id
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(projected, (churned_binding, successor_resource));
    assert_ne!(projected.0, successor_binding);

    scratch.cleanup().await
}

#[tokio::test]
async fn equal_position_v1_residue_suppresses_released_v2_authority() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_equal_position_residue").await?;
    let chain = "authority-equal-position-residue";
    let logical_name_id = seed_dual_open_cross_arm_fixture(scratch.pool(), chain, 4).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET authority_arm = 'ens_v2', active_to = to_timestamp(3)
         WHERE chain_id = $1 AND logical_name_id = $2
           AND authority_arm = 'ens_v1'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE normalized_events SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND logical_name_id = $2
           AND source_family LIKE 'ens_v1_%'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(scratch.pool())
    .await?;
    let released_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2
         ORDER BY block_number DESC, surface_binding_id DESC LIMIT 1",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    insert_lineage_block(scratch.pool(), chain, 6).await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(6)
         WHERE chain_id = $1 AND logical_name_id = $2
           AND authority_arm = 'ens_v2' AND active_to IS NULL",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(scratch.pool())
    .await?;
    let boundary_ref = json!({
        "kind": "raw_block",
        "chain_id": chain,
        "block_hash": block_hash(chain, 6),
        "block_number": 6,
        "state_scope": "name_authority"
    });
    // Boundary materialization emits lifecycle facts at block scope, so both
    // facts deliberately share the production `(block, NULL, NULL)` position.
    insert_event(
        scratch.pool(),
        chain,
        6,
        Some(&logical_name_id),
        Some(&released_resource.to_string()),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({"status":"released","released_at":6}),
        boundary_ref.clone(),
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        6,
        Some(&logical_name_id),
        None,
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":9_999}),
        boundary_ref,
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET derivation_kind = CASE event_kind
             WHEN 'RegistrationReleased' THEN 'ens_v2_registry_resource_surface'
             ELSE 'ens_v1_unwrapped_authority'
         END
         WHERE chain_id = $1 AND logical_name_id = $2 AND block_number = 6
           AND event_kind IN ('RegistrationReleased', 'ExpiryChanged')",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(scratch.pool())
    .await?;
    let boundary_positions: Vec<(Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT transaction_index, log_index FROM normalized_events
         WHERE chain_id = $1 AND logical_name_id = $2 AND block_number = 6
           AND event_kind IN ('RegistrationReleased', 'ExpiryChanged')
         ORDER BY event_kind",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(boundary_positions, vec![(None, None), (None, None)]);

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 6).await?;
    let projected: (String, Option<String>, Option<Uuid>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason, resource_id
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        projected,
        (
            "unsupported".into(),
            Some("conflicting_current_ens_authority".into()),
            None,
        )
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn authority_epoch_ignores_release_from_a_superseded_same_arm_resource() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_same_arm_resource").await?;
    let chain = "authority-same-arm-resource";
    let logical_name_id = seed_dual_open_cross_arm_fixture(scratch.pool(), chain, 4).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET authority_arm = 'ens_v2', active_to = to_timestamp(3)
         WHERE chain_id = $1 AND logical_name_id = $2
           AND authority_arm = 'ens_v1'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE normalized_events SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND logical_name_id = $2
           AND source_family LIKE 'ens_v1_%'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(scratch.pool())
    .await?;
    let selected_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2 AND active_to IS NULL
         ORDER BY block_number DESC LIMIT 1",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    let superseded_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2 AND resource_id <> $3
         ORDER BY block_number LIMIT 1",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .bind(selected_resource)
    .fetch_one(scratch.pool())
    .await?;
    insert_lineage_block(scratch.pool(), chain, 6).await?;
    insert_event(
        scratch.pool(),
        chain,
        6,
        Some(&logical_name_id),
        Some(&superseded_resource.to_string()),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({"status":"released","released_at":6}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 6).await?;
    let projected: (Uuid, String, String) = sqlx::query_as(
        "SELECT resource_id, declared_summary #>> '{registration,status}',
                declared_summary #>> '{control,registry_owner}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(projected.0, selected_resource);
    assert_eq!(projected.1, "active");
    assert_eq!(projected.2, OWNER);

    for block in 7..=9 {
        insert_lineage_block(scratch.pool(), chain, block).await?;
    }
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(7)
         WHERE chain_id = $1 AND logical_name_id = $2 AND resource_id = $3",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .bind(selected_resource)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        7,
        Some(&logical_name_id),
        Some(&selected_resource.to_string()),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({"status":"released","released_at":7}),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 6,
            hash: block_hash(chain, 6),
        }),
        RunMode::Normal,
        7,
        7,
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        8,
        Some(&logical_name_id),
        Some(&superseded_resource.to_string()),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({"status":"released","released_at":8}),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 7,
            hash: block_hash(chain, 7),
        }),
        RunMode::Normal,
        8,
        8,
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        9,
        Some(&logical_name_id),
        None,
        "RecordChanged",
        "ens_v2_resolver_l1",
        json!({"record_key":"text:scope-probe"}),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 8,
            hash: block_hash(chain, 8),
        }),
        RunMode::Normal,
        9,
        9,
    )
    .await?;
    normalize_projection_clocks(scratch.pool()).await?;
    let incremental: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(incremental["resource_id"], selected_resource.to_string());
    assert_eq!(
        incremental["declared_summary"]["registration"]["status"],
        "released"
    );

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 9).await?;
    normalize_projection_clocks(scratch.pool()).await?;
    let rebuilt: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        incremental, rebuilt,
        "a superseded closed v2 resource retook authority after incremental scope narrowed"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn proofless_v2_release_retains_closed_authority_after_later_v1_residue() -> Result<()> {
    for (database_prefix, chain, release_family) in [
        (
            "project_authority_proofless_v2_registry_release",
            "authority-proofless-v2-registry-release",
            "ens_v2_registry_l1",
        ),
        (
            "project_authority_proofless_v2_root_release",
            "authority-proofless-v2-root-release",
            "ens_v2_root_l1",
        ),
    ] {
        let scratch = ScratchDatabase::create(database_prefix).await?;
        let logical_name_id = seed_dual_open_cross_arm_fixture(scratch.pool(), chain, 4).await?;
        InterpretEngine::new(scratch.pool().clone())
            .run_batch(InterpretRequest {
                chain_id: chain.into(),
                from_block: 0,
                to_block: 5,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await?;
        if release_family == "ens_v2_root_l1" {
            sqlx::query(
                "UPDATE normalized_events
                 SET source_family = 'ens_v2_root_l1', source_manifest_id = NULL
                 WHERE chain_id = $1 AND logical_name_id = $2
                   AND source_family LIKE 'ens_v2_%'",
            )
            .bind(chain)
            .bind(&logical_name_id)
            .execute(scratch.pool())
            .await?;
        }
        sqlx::query(
            "UPDATE surface_bindings
             SET authority_arm = 'ens_v2', active_to = to_timestamp(3)
             WHERE chain_id = $1 AND logical_name_id = $2
               AND authority_arm = 'ens_v1'",
        )
        .bind(chain)
        .bind(&logical_name_id)
        .execute(scratch.pool())
        .await?;
        sqlx::query(
            "UPDATE normalized_events SET canonicality_state = 'orphaned'
             WHERE chain_id = $1 AND logical_name_id = $2
               AND source_family LIKE 'ens_v1_%'",
        )
        .bind(chain)
        .bind(&logical_name_id)
        .execute(scratch.pool())
        .await?;
        let released_resource: Uuid = sqlx::query_scalar(
            "SELECT resource_id FROM surface_bindings
             WHERE chain_id = $1 AND logical_name_id = $2
             ORDER BY block_number DESC, surface_binding_id DESC LIMIT 1",
        )
        .bind(chain)
        .bind(&logical_name_id)
        .fetch_one(scratch.pool())
        .await?;
        for block in 6..=7 {
            insert_lineage_block(scratch.pool(), chain, block).await?;
        }
        insert_event(
            scratch.pool(),
            chain,
            6,
            Some(&logical_name_id),
            Some(&released_resource.to_string()),
            "RegistrationReleased",
            release_family,
            json!({"status":"released","released_at":6}),
            json!({}),
        )
        .await?;
        sqlx::query(
            "UPDATE surface_bindings SET active_to = to_timestamp(6)
             WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v2'
               AND active_to IS NULL",
        )
        .bind(chain)
        .bind(&logical_name_id)
        .execute(scratch.pool())
        .await?;
        insert_event(
            scratch.pool(),
            chain,
            7,
            Some(&logical_name_id),
            None,
            "ExpiryChanged",
            "ens_v1_registrar_l1",
            json!({"expiry":9_999}),
            json!({}),
        )
        .await?;

        run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 7).await?;
        let projected: (Uuid, Value, Value) = sqlx::query_as(
            "SELECT resource_id, declared_summary,
                    provenance -> 'authority_selection'
             FROM name_current WHERE logical_name_id = $1",
        )
        .bind(&logical_name_id)
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(projected.0, released_resource, "{release_family}");
        assert_eq!(
            projected.1["registration"]["status"], "released",
            "{release_family}"
        );
        assert!(
            projected.1["registration"]["expiry"].is_null(),
            "{release_family}"
        );
        assert_eq!(projected.2["authority_arm"], "ens_v2", "{release_family}");
        assert_eq!(
            projected.2["lifecycle_state"], "unregistered",
            "{release_family}"
        );
        assert!(projected.2.get("proof_kind").is_none(), "{release_family}");
        let source_classes = projected.1["coverage"]["source_classes_considered"]
            .as_array()
            .expect("coverage source classes must be an array");
        assert!(
            source_classes.contains(&json!(release_family)),
            "coverage omitted {release_family}: {source_classes:?}"
        );
        assert_eq!(
            projected.1["coverage"]["enumeration_basis"], "exact_name_profile",
            "{release_family}"
        );
        assert!(
            bigname_storage::load_name_current(scratch.pool(), &logical_name_id)
                .await?
                .is_some(),
            "the {release_family} closed ENSv2 authority tombstone must remain readable"
        );

        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn released_v2_regime_carries_regrant_after_v1_residue() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_released_regime_regrant").await?;
    let chain = "authority-released-regime-regrant";
    let (logical_name_id, released_resource) =
        seed_proofless_released_v2_authority(scratch.pool(), chain).await?;
    for block in 7..=8 {
        insert_lineage_block(scratch.pool(), chain, block).await?;
    }

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 6).await?;
    insert_event(
        scratch.pool(),
        chain,
        7,
        Some(&logical_name_id),
        None,
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":9_999}),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 6,
            hash: block_hash(chain, 6),
        }),
        RunMode::Normal,
        7,
        7,
    )
    .await?;
    let residue_tombstone: (Uuid, Option<String>) = sqlx::query_as(
        "SELECT resource_id, declared_summary #>> '{registration,status}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(residue_tombstone.0, released_resource);
    assert_eq!(residue_tombstone.1.as_deref(), Some("released"));

    let (regrant_binding, regrant_resource) =
        insert_v2_regrant(scratch.pool(), chain, &logical_name_id, 8).await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 7,
            hash: block_hash(chain, 7),
        }),
        RunMode::Normal,
        8,
        8,
    )
    .await?;
    normalize_projection_clocks(scratch.pool()).await?;
    let incremental: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_active_v2_regrant(&incremental, regrant_binding, regrant_resource);
    assert!(
        bigname_storage::load_name_current(scratch.pool(), &logical_name_id)
            .await?
            .is_some(),
        "the re-granted ENSv2 registration must remain readable"
    );

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 8).await?;
    normalize_projection_clocks(scratch.pool()).await?;
    let rebuilt: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        incremental, rebuilt,
        "released-regime re-grant splits must equal a full rebuild"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn released_v2_regime_carries_regrant_without_v1_residue() -> Result<()> {
    let scratch =
        ScratchDatabase::create("project_authority_released_regime_clean_regrant").await?;
    let chain = "authority-released-regime-clean-regrant";
    let (logical_name_id, _) = seed_proofless_released_v2_authority(scratch.pool(), chain).await?;
    for block in 7..=8 {
        insert_lineage_block(scratch.pool(), chain, block).await?;
    }
    let (regrant_binding, regrant_resource) =
        insert_v2_regrant(scratch.pool(), chain, &logical_name_id, 8).await?;

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 8).await?;
    let projected: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_active_v2_regrant(&projected, regrant_binding, regrant_resource);
    scratch.cleanup().await
}

#[tokio::test]
async fn released_v2_regime_regrant_releases_into_a_fresh_tombstone() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_released_regime_rerelease").await?;
    let chain = "authority-released-regime-rerelease";
    let (logical_name_id, released_resource) =
        seed_proofless_released_v2_authority(scratch.pool(), chain).await?;
    for block in 7..=10 {
        insert_lineage_block(scratch.pool(), chain, block).await?;
    }
    let (regrant_binding, regrant_resource) =
        insert_v2_regrant(scratch.pool(), chain, &logical_name_id, 8).await?;
    insert_event(
        scratch.pool(),
        chain,
        9,
        Some(&logical_name_id),
        Some(&regrant_resource.to_string()),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({"status":"released","released_at":9}),
        json!({}),
    )
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(9)
         WHERE surface_binding_id = $1",
    )
    .bind(regrant_binding)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        10,
        Some(&logical_name_id),
        None,
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":9_999}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 10).await?;
    let projected: (Uuid, Value, Value, Option<String>) = sqlx::query_as(
        "SELECT resource_id, declared_summary,
                provenance -> 'authority_selection', unsupported_reason
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(projected.0, regrant_resource);
    assert_ne!(projected.0, released_resource);
    assert_eq!(projected.1["registration"]["status"], "released");
    assert!(projected.1["registration"]["expiry"].is_null());
    assert!(projected.1["registration"]["registrant"].is_null());
    assert_eq!(projected.2["authority_arm"], "ens_v2");
    assert_eq!(projected.2["lifecycle_state"], "unregistered");
    assert!(projected.2.get("proof_kind").is_none());
    assert_eq!(
        projected.3.as_deref(),
        Some("ensv2_exact_name_profile_shadow")
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn equal_position_v1_residue_suppresses_regime_regrant_carry() -> Result<()> {
    let scratch =
        ScratchDatabase::create("project_authority_regime_equal_position_regrant").await?;
    let chain = "authority-regime-equal-position-regrant";
    let (logical_name_id, _) = seed_proofless_released_v2_authority(scratch.pool(), chain).await?;
    // Boundary materialization emits lifecycle facts at block scope, so this v1
    // fact deliberately shares the release's production `(block, NULL, NULL)`
    // position; equal-position v1 activity counts as at-or-before the release
    // and must keep the release out of the regime.
    insert_event(
        scratch.pool(),
        chain,
        6,
        Some(&logical_name_id),
        None,
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":9_999}),
        json!({
            "kind": "raw_block",
            "chain_id": chain,
            "block_hash": block_hash(chain, 6),
            "block_number": 6,
            "state_scope": "name_authority"
        }),
    )
    .await?;
    let boundary_positions: Vec<(Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT transaction_index, log_index FROM normalized_events
         WHERE chain_id = $1 AND logical_name_id = $2 AND block_number = 6
           AND event_kind IN ('RegistrationReleased', 'ExpiryChanged')
         ORDER BY event_kind",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(boundary_positions, vec![(None, None), (None, None)]);
    for block in 7..=8 {
        insert_lineage_block(scratch.pool(), chain, block).await?;
    }
    insert_v2_regrant(scratch.pool(), chain, &logical_name_id, 8).await?;

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 8).await?;
    let projected: (String, Option<String>, Option<Uuid>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason, resource_id
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        projected,
        (
            "unsupported".into(),
            Some("conflicting_current_ens_authority".into()),
            None,
        )
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn earlier_other_resource_grant_disqualifies_regime_carry() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_regime_other_resource_grant").await?;
    let chain = "authority-regime-other-resource-grant";
    let (logical_name_id, _) = seed_proofless_released_v2_authority(scratch.pool(), chain).await?;
    // A canonical ENSv2 grant on a different resource at-or-before the release
    // means the release did not close out the whole v2 story: it must not
    // qualify the name for the released-v2 regime.
    let other_resource = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 5, 'canonical')",
    )
    .bind(other_resource)
    .bind(chain)
    .bind(block_hash(chain, 5))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        5,
        Some(&logical_name_id),
        Some(&other_resource.to_string()),
        "RegistrationGranted",
        "ens_v2_registry_l1",
        json!({"status":"registered","registrant":OWNER,"expiry":5_000_000_000_u64}),
        json!({}),
    )
    .await?;
    for block in 7..=8 {
        insert_lineage_block(scratch.pool(), chain, block).await?;
    }
    insert_event(
        scratch.pool(),
        chain,
        7,
        Some(&logical_name_id),
        None,
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":9_999}),
        json!({}),
    )
    .await?;
    insert_v2_regrant(scratch.pool(), chain, &logical_name_id, 8).await?;

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 8).await?;
    let projected: (String, Option<String>, Option<Uuid>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason, resource_id
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        projected,
        (
            "unsupported".into(),
            Some("conflicting_current_ens_authority".into()),
            None,
        )
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn candidate_authority_is_inert_and_activation_names_every_changed_row() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_candidate_parity").await?;
    let chain = "authority-candidate-parity";
    let logical_name_id =
        seed_closed_predecessor_cross_arm_fixture(scratch.pool(), chain, 2, 1, 2).await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    normalize_projection_clocks(scratch.pool()).await?;
    let before: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;

    let (_, _, proof_identity) = insert_activated_authority_proof(
        scratch.pool(),
        chain,
        &logical_name_id,
        "unwrapped",
        None,
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events SET consumer_visibility = 'candidate'
         WHERE event_identity = $1",
    )
    .bind(&proof_identity)
    .execute(scratch.pool())
    .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    normalize_projection_clocks(scratch.pool()).await?;
    let candidate: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(candidate, before);

    sqlx::query(
        "UPDATE normalized_events SET consumer_visibility = 'activated'
         WHERE event_identity = $1",
    )
    .bind(&proof_identity)
    .execute(scratch.pool())
    .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    normalize_projection_clocks(scratch.pool()).await?;
    let activated: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_ne!(activated, before);
    assert_eq!(
        activated.pointer("/provenance/authority_selection/proof_event_identity"),
        Some(&json!(proof_identity))
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn authority_epoch_keeps_migration_fields_atomic_and_release_sticky() -> Result<()> {
    for (prefix, chain, migration_path) in [
        (
            "project_authority_unwrapped",
            "authority-unwrapped",
            "unwrapped",
        ),
        (
            "project_authority_unlocked_wrapped",
            "authority-unlocked-wrapped",
            "unlocked_wrapped",
        ),
        (
            "project_authority_locked_wrapped",
            "authority-locked-wrapped",
            "locked_wrapped",
        ),
    ] {
        let scratch = ScratchDatabase::create(prefix).await?;
        let (logical_name_id, proof_identity) =
            seed_authority_lifecycle_fixture(scratch.pool(), chain, migration_path).await?;

        run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 7).await?;
        let at_renewal: (Value, Value, Value) = sqlx::query_as(
            "SELECT declared_summary, provenance -> 'authority_selection',
                    provenance -> 'selected_event_ids'
             FROM name_current WHERE logical_name_id = $1",
        )
        .bind(&logical_name_id)
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(at_renewal.0["registration"]["status"], "active");
        assert_eq!(at_renewal.0["registration"]["expiry"], 2_222);
        assert_eq!(at_renewal.0["registration"]["registrant"], OWNER);
        assert_eq!(
            at_renewal.0["registration"]["latest_event_kind"], "RegistrationRenewed",
            "the selected ENSv2 lifecycle must report its renewal head",
        );
        let preserves_first_observation: bool = sqlx::query_scalar(
            "SELECT (declared_summary #>> '{registration,created_at}')::timestamptz =
                    (SELECT lineage.block_timestamp
                     FROM normalized_events event
                     JOIN chain_lineage lineage
                       ON lineage.chain_id = event.chain_id
                      AND lineage.block_number = event.block_number
                      AND lineage.block_hash = event.block_hash
                     WHERE event.chain_id = $1 AND event.logical_name_id = $2
                       AND event.consumer_visibility = 'activated'
                     ORDER BY event.block_number, event.transaction_index NULLS FIRST,
                              event.log_index NULLS FIRST, event.normalized_event_id
                     LIMIT 1)
             FROM name_current WHERE logical_name_id = $2",
        )
        .bind(chain)
        .bind(&logical_name_id)
        .fetch_one(scratch.pool())
        .await?;
        assert!(
            preserves_first_observation,
            "authority selection changed the name's original creation time"
        );
        assert_eq!(at_renewal.0["control"]["registry_owner"], OWNER);
        assert_eq!(at_renewal.0["resolver"]["address"], EQUIVALENCE_V2_RESOLVER);
        assert_eq!(at_renewal.1["authority_arm"], "ens_v2");
        assert_eq!(at_renewal.1["proof_event_identity"], proof_identity);
        assert!(at_renewal.0.get("wrapper_state").is_none());
        if migration_path != "unwrapped" {
            let wrapper_event: i64 = sqlx::query_scalar(
                "SELECT normalized_event_id FROM normalized_events
                 WHERE chain_id = $1 AND logical_name_id = $2
                   AND event_kind = 'PermissionScopeChanged'",
            )
            .bind(chain)
            .bind(&logical_name_id)
            .fetch_one(scratch.pool())
            .await?;
            assert!(
                at_renewal
                    .2
                    .as_array()
                    .is_some_and(|events| { events.contains(&json!(wrapper_event)) })
            );
        }

        run_project(
            scratch.pool(),
            chain,
            Some(Marker {
                number: 7,
                hash: block_hash(chain, 7),
            }),
            RunMode::Normal,
            8,
            8,
        )
        .await?;
        run_project(
            scratch.pool(),
            chain,
            Some(Marker {
                number: 8,
                hash: block_hash(chain, 8),
            }),
            RunMode::Normal,
            9,
            9,
        )
        .await?;
        let after_residue: (Value, Value) = sqlx::query_as(
            "SELECT declared_summary, provenance -> 'authority_selection'
             FROM name_current WHERE logical_name_id = $1",
        )
        .bind(&logical_name_id)
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(after_residue.0["registration"]["status"], "released");
        assert!(after_residue.0["registration"]["expiry"].is_null());
        assert!(after_residue.0["registration"]["registrant"].is_null());
        assert!(after_residue.0["control"]["registry_owner"].is_null());
        assert!(after_residue.0["resolver"]["address"].is_null());
        assert_eq!(after_residue.1["authority_arm"], "ens_v2");
        assert_eq!(after_residue.1["lifecycle_state"], "unregistered");
        assert!(
            bigname_storage::load_name_current(scratch.pool(), &logical_name_id)
                .await?
                .is_some(),
            "released v2 authority disappeared from the normal storage read"
        );
        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn authority_epoch_incremental_splits_equal_rebuilds() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_incremental").await?;
    let chain = "authority-incremental";
    let (logical_name_id, _) =
        seed_authority_lifecycle_fixture(scratch.pool(), chain, "unwrapped").await?;
    let mut previous = None;
    for target in [3, 4, 6, 7, 8, 9] {
        let from = previous.map_or(0, |block| block + 1);
        let resume = previous.map(|number| Marker {
            number,
            hash: block_hash(chain, number),
        });
        run_project(scratch.pool(), chain, resume, RunMode::Normal, from, target).await?;
        normalize_projection_clocks(scratch.pool()).await?;
        let incremental: Value = sqlx::query_scalar(
            "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
        )
        .bind(&logical_name_id)
        .fetch_one(scratch.pool())
        .await?;

        run_project(scratch.pool(), chain, None, RunMode::Normal, 0, target).await?;
        normalize_projection_clocks(scratch.pool()).await?;
        let rebuilt: Value = sqlx::query_scalar(
            "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
        )
        .bind(&logical_name_id)
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(incremental, rebuilt, "authority split at block {target}");
        previous = Some(target);
    }
    scratch.cleanup().await
}

// Seeds the migrated name at its selected ENSv2 epoch, then appends one ENSv1
// residue event on the superseded resource. `insert_event` leaves the position
// indexes null, so a same-block residue outranks the block-7 ENSv2 events by
// insertion order: any collection that ranks the name's events by recency
// instead of consuming the selected resource picks the residue.
async fn seed_late_v1_residue(
    pool: &PgPool,
    chain: &str,
    event_kind: &str,
    after_state: Value,
) -> Result<String> {
    let (logical_name_id, _) = seed_authority_lifecycle_fixture(pool, chain, "unwrapped").await?;
    let v1_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v1'
         ORDER BY block_number DESC LIMIT 1",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_one(pool)
    .await?;
    insert_event(
        pool,
        chain,
        7,
        Some(&logical_name_id),
        Some(&v1_resource.to_string()),
        event_kind,
        "ens_v1_registrar_l1",
        after_state,
        json!({}),
    )
    .await?;
    Ok(logical_name_id)
}

async fn address_relation_holders(
    pool: &PgPool,
    logical_name_id: &str,
    relation: &str,
) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT address FROM address_names_current
         WHERE logical_name_id = $1 AND relation = $2
         ORDER BY address",
    )
    .bind(logical_name_id)
    .bind(relation)
    .fetch_all(pool)
    .await?)
}

// The selected ENSv2 registrant is the only registrant relation the migrated
// name publishes; a later ENSv1 grant on the superseded resource is history.
#[tokio::test]
async fn selected_authority_constrains_the_address_registrant_lateral() -> Result<()> {
    let scratch = ScratchDatabase::create("project_fanout_registrant").await?;
    let chain = "fanout-registrant";
    let logical_name_id = seed_late_v1_residue(
        scratch.pool(),
        chain,
        "RegistrationGranted",
        json!({"status":"registered","registrant":TRANSFER_OWNER}),
    )
    .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 7).await?;

    // Anti-vacuity: the name is projected under the ENSv2 arm the residue contradicts.
    let selected: (Value, Value) = sqlx::query_as(
        "SELECT declared_summary -> 'registration' -> 'registrant',
                provenance -> 'authority_selection' -> 'authority_arm'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(selected, (json!(OWNER), json!("ens_v2")));

    assert_eq!(
        address_relation_holders(scratch.pool(), &logical_name_id, "registrant").await?,
        vec![OWNER.to_owned()]
    );
    scratch.cleanup().await
}

// Token-holder membership follows the selected resource's token lineage, so a
// superseded-resource token transfer moves no current address relation.
#[tokio::test]
async fn selected_authority_constrains_the_address_token_holder_lateral() -> Result<()> {
    let scratch = ScratchDatabase::create("project_fanout_token_holder").await?;
    let chain = "fanout-token-holder";
    let logical_name_id = seed_late_v1_residue(
        scratch.pool(),
        chain,
        "TokenControlTransferred",
        json!({"from":OWNER,"to":TRANSFER_OWNER}),
    )
    .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 7).await?;

    let arm: Value = sqlx::query_scalar(
        "SELECT provenance -> 'authority_selection' -> 'authority_arm'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(arm, json!("ens_v2"));

    assert_eq!(
        address_relation_holders(scratch.pool(), &logical_name_id, "token_holder").await?,
        vec![OWNER.to_owned()]
    );
    scratch.cleanup().await
}

// The controller fold reads the same selected event set: a superseded-resource
// authority transfer cannot install itself as the name's effective controller.
#[tokio::test]
async fn selected_authority_constrains_the_effective_controller_fold() -> Result<()> {
    let scratch = ScratchDatabase::create("project_fanout_controller").await?;
    let chain = "fanout-controller";
    let logical_name_id = seed_late_v1_residue(
        scratch.pool(),
        chain,
        "AuthorityTransferred",
        json!({"owner":TRANSFER_OWNER}),
    )
    .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 7).await?;

    let owner: Value = sqlx::query_scalar(
        "SELECT declared_summary -> 'control' -> 'registry_owner'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(owner, json!(OWNER));

    assert_eq!(
        address_relation_holders(scratch.pool(), &logical_name_id, "effective_controller").await?,
        vec![OWNER.to_owned()]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn positive_v2_child_registration_establishes_authority_without_child_migration() -> Result<()>
{
    let scratch = ScratchDatabase::create("project_authority_positive_child").await?;
    let chain = "authority-positive-child";
    let logical_name_id = seed_dual_open_cross_arm_fixture(scratch.pool(), chain, 4).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    let (registry_instance, registry_address, registry_manifest): (String, String, i64) =
        sqlx::query_as(
            "SELECT event.after_state ->> 'registry_contract_instance_id', address.address,
                    event.source_manifest_id
         FROM normalized_events event
         JOIN contract_instance_addresses address
           ON address.contract_instance_id::text =
              event.after_state ->> 'registry_contract_instance_id'
          AND address.chain_id = event.chain_id
          AND address.deactivated_at IS NULL
         WHERE event.chain_id = $1 AND event.logical_name_id = $2
           AND event.source_family = 'ens_v2_registry_l1'
           AND event.event_kind = 'RegistrationGranted' AND event.resource_id IS NOT NULL
         ORDER BY event.normalized_event_id DESC LIMIT 1",
        )
        .bind(chain)
        .bind(&logical_name_id)
        .fetch_one(scratch.pool())
        .await?;
    let parent_id = format!("ens:{:#x}", raw_namehash(&[b"eth"]));
    insert_event(
        scratch.pool(),
        chain,
        3,
        Some(&parent_id),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":registry_address}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        3,
        Some(&parent_id),
        None,
        "MigrationApplied",
        "ens_v2_migration_l1",
        json!({
            "successor_registry_contract_instance_id":Uuid::new_v4(),
            "migration_path":"locked_wrapped",
            "fixture_child_registry_contract_instance_id":registry_instance,
            "evidence":[{"event_identity":"fixture-migration-registry-proof"}]
        }),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    let ordinary_registry_reason: String = sqlx::query_scalar(
        "SELECT unsupported_reason FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        ordinary_registry_reason, "conflicting_current_ens_authority",
        "an ordinary ENSv2 registry must not establish child authority"
    );

    sqlx::query(
        "INSERT INTO migration_discovery_associations (
             logical_edge_identity, migration_correlation_id, correlation_kind,
             registry_contract_instance_id, registry_address, source_manifest_id,
             evidence_refs, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, canonicality_state, consumer_visibility,
             interpreter_content_hash
         ) VALUES (
             $1, $2, 'migration_registry_creation', $3, lower($4), $5,
             '[{\"event_identity\":\"fixture-migration-registry-proof\"}]'::jsonb,
             $6, $7, $8, $9, $10, $11, 'canonical', 'candidate', $12
         )",
    )
    .bind(format!("{chain}:migration-registry-edge"))
    .bind(format!("{chain}:migration-registry-correlation"))
    .bind(Uuid::parse_str(&registry_instance)?)
    .bind(&registry_address)
    .bind(registry_manifest)
    .bind(chain)
    .bind(2_i64)
    .bind(block_hash(chain, 2))
    .bind(format!("{chain}:migration-registry-tx"))
    .bind(0_i64)
    .bind(0_i64)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 5,
            hash: block_hash(chain, 5),
        }),
        RunMode::Normal,
        2,
        2,
    )
    .await?;
    let association_only_reason: String = sqlx::query_scalar(
        "SELECT unsupported_reason
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        association_only_reason, "conflicting_current_ens_authority",
        "a diagnostic association without its admitted edge is not readable"
    );
    sqlx::query(
        "INSERT INTO discovery_edges (
             chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id,
             discovery_source, admission_basis, source_manifest_id,
             active_from_block_number, active_from_block_hash, canonicality_state,
             provenance
         ) VALUES (
             $1, 'registry_announcement', $2, $2, 'RegistryCreated',
             'reachable_from_root', $3, 2, $4, 'canonical',
             '{\"transaction_index\":0,\"log_index\":0}'::jsonb
         )",
    )
    .bind(chain)
    .bind(Uuid::parse_str(&registry_instance)?)
    .bind(registry_manifest)
    .bind(block_hash(chain, 2))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        3,
        Some(&parent_id),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":"0x0000000000000000000000000000000000000000"}),
        json!({}),
    )
    .await?;
    let detached_event_identity: String = sqlx::query_scalar(
        "SELECT event_identity FROM normalized_events
         WHERE chain_id = $1 AND logical_name_id = $2
           AND event_kind = 'SubregistryChanged'
           AND after_state ->> 'subregistry' =
               '0x0000000000000000000000000000000000000000'
         ORDER BY normalized_event_id DESC LIMIT 1",
    )
    .bind(chain)
    .bind(&parent_id)
    .fetch_one(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 5,
            hash: block_hash(chain, 5),
        }),
        RunMode::Normal,
        2,
        2,
    )
    .await?;
    let detached_reason: String = sqlx::query_scalar(
        "SELECT unsupported_reason FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        detached_reason, "conflicting_current_ens_authority",
        "a registry detached before the child grant is not current at proof"
    );
    sqlx::query(
        "UPDATE normalized_events SET canonicality_state = 'orphaned'
         WHERE event_identity = $1",
    )
    .bind(&detached_event_identity)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 5,
            hash: block_hash(chain, 5),
        }),
        RunMode::Redo,
        3,
        3,
    )
    .await?;
    let admitted_registry_proof: Option<String> = sqlx::query_scalar(
        "SELECT provenance #>> '{authority_selection,proof_kind}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        admitted_registry_proof.as_deref(),
        Some("positive_v2_child_registration")
    );
    sqlx::query(
        "UPDATE normalized_events
         SET consumer_visibility = 'candidate',
             migration_correlation_ids = ARRAY[$3]::text[]
         WHERE chain_id = $1 AND logical_name_id = $2
           AND event_kind = 'MigrationApplied'",
    )
    .bind(chain)
    .bind(&parent_id)
    .bind(format!("{chain}:parent-transition"))
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 5,
            hash: block_hash(chain, 5),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    let candidate_reason: String = sqlx::query_scalar(
        "SELECT unsupported_reason FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(candidate_reason, "conflicting_current_ens_authority");

    sqlx::query(
        "UPDATE normalized_events SET consumer_visibility = 'activated'
         WHERE chain_id = $1 AND logical_name_id = $2
           AND event_kind = 'MigrationApplied'",
    )
    .bind(chain)
    .bind(&parent_id)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 5,
            hash: block_hash(chain, 5),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    normalize_projection_clocks(scratch.pool()).await?;
    let incremental: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    normalize_projection_clocks(scratch.pool()).await?;
    let rebuilt: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(incremental, rebuilt, "incremental child proof diverged");
    let authority: Value = sqlx::query_scalar(
        "SELECT provenance -> 'authority_selection'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(authority["authority_arm"], "ens_v2");
    assert_eq!(authority["proof_kind"], "positive_v2_child_registration");

    insert_lineage_block(scratch.pool(), chain, 6).await?;
    insert_event(
        scratch.pool(),
        chain,
        6,
        Some(&parent_id),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":"0x0000000000000000000000000000000000000000"}),
        json!({}),
    )
    .await?;
    sqlx::query(
        "UPDATE manifest_versions SET rollout_status = 'deprecated' WHERE manifest_id = $1",
    )
    .bind(registry_manifest)
    .execute(scratch.pool())
    .await?;
    insert_namespaced_manifest(
        scratch.pool(),
        "ens",
        chain,
        "ens_v2_registry_l1",
        2,
        "fixture-rotation",
        "tests/project-v2-registry-rotation.toml",
        json!({"contracts":[{"role":"registry","address":registry_address}]}),
    )
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, raw_fact_ref, derivation_kind,
             canonicality_state, before_state, after_state
         ) VALUES (
             $1, 'ens', 'SourceManifestUpdated', 'ens_v2_registry_l1', 1,
             $2, $3, '{}'::jsonb, 'manifest_sync', 'finalized', '{}'::jsonb,
             '{\"rollout_status\":\"deprecated\",\"manifest_payload\":{}}'::jsonb
         )",
    )
    .bind(format!("{chain}:SourceManifestUpdated:retired"))
    .bind(registry_manifest)
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 6).await?;
    let after_rotation: Value = sqlx::query_scalar(
        "SELECT provenance -> 'authority_selection'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(after_rotation["authority_arm"], "ens_v2");
    assert_eq!(
        after_rotation["proof_kind"],
        "positive_v2_child_registration"
    );

    for block in 7..=8 {
        insert_lineage_block(scratch.pool(), chain, block).await?;
    }
    let v2_resource: String = sqlx::query_scalar(
        "SELECT resource_id::text FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v2'
         ORDER BY block_number DESC LIMIT 1",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        7,
        Some(&logical_name_id),
        Some(&v2_resource),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({"status":"released","released_at":7}),
        json!({}),
    )
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(7)
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v2'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        8,
        Some(&logical_name_id),
        None,
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":8_888}),
        json!({}),
    )
    .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 8).await?;
    let released: (Value, Value) = sqlx::query_as(
        "SELECT declared_summary, provenance -> 'authority_selection'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(released.0["registration"]["status"], "released");
    assert!(released.0["registration"]["expiry"].is_null());
    assert_eq!(released.1["authority_arm"], "ens_v2");
    assert_eq!(released.1["lifecycle_state"], "unregistered");
    let child_migrations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND logical_name_id = $2 AND event_kind = 'MigrationApplied'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(child_migrations, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn authority_classifier_covers_every_ens_binding_event_arm_combination() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_classifier_matrix").await?;
    let chain = "ethereum-sepolia";
    seed_lineage(scratch.pool(), chain, 5).await?;
    declare_sepolia_post_audit_profile(scratch.pool(), chain).await?;
    insert_namespaced_manifest(
        scratch.pool(),
        "ens",
        chain,
        "ens_v2_registry_l1",
        1,
        "ens_v2_sepolia_post_audit",
        "tests/project-authority-classifier-v2-registry.toml",
        json!({}),
    )
    .await?;
    insert_namespaced_manifest(
        scratch.pool(),
        "ens",
        chain,
        "ens_v2_registrar_l1",
        1,
        "ens_v2_sepolia_post_audit",
        "tests/project-authority-classifier-v2-registrar.toml",
        json!({"capability_flags":{"exact_name_profile":{"status":"supported"}}}),
    )
    .await?;

    let arms = [
        EnsArmSet::Empty,
        EnsArmSet::V1,
        EnsArmSet::V2,
        EnsArmSet::Both,
    ];
    let mut expected = Vec::new();
    for bindings in arms {
        for events in arms {
            let case = format!("b_{}_e_{}", bindings.label(), events.label());
            let logical_name_id = format!(
                "ens:{:#x}",
                raw_namehash(&[case.as_bytes(), b"classifier", b"eth"])
            );
            let seeded = seed_authority_classifier_case(
                scratch.pool(),
                chain,
                &logical_name_id,
                bindings,
                events,
                1,
                None,
            )
            .await?;
            let has_v1 = bindings.includes_v1() || events.includes_v1();
            let has_v2 = bindings.includes_v2() || events.includes_v2();
            let selected_arm = if has_v1 && has_v2 {
                None
            } else if bindings.includes_v1() || events.includes_v1() {
                Some("ens_v1")
            } else if bindings.includes_v2() || events.includes_v2() {
                Some("ens_v2")
            } else {
                None
            };
            let selected_binding = match selected_arm {
                Some("ens_v1") => seeded.v1,
                Some("ens_v2") => seeded.v2,
                _ => None,
            };
            let reason = if has_v1 && has_v2 {
                Some("independent_ens_deployments_overlap")
            } else if selected_binding.is_none() {
                Some("current_authority_not_projected")
            } else {
                None
            };
            expected.push((
                case,
                logical_name_id,
                selected_arm.map(str::to_owned),
                selected_binding,
                reason.map(str::to_owned),
            ));
        }
    }

    let proof_name = format!(
        "ens:{:#x}",
        raw_namehash(&[b"activated-migration-proof", b"classifier", b"eth"])
    );
    let proof_bindings = seed_authority_classifier_case(
        scratch.pool(),
        chain,
        &proof_name,
        EnsArmSet::Both,
        EnsArmSet::Both,
        1,
        Some(5),
    )
    .await?;
    let (proof_binding, proof_resource, _) =
        insert_activated_authority_proof(scratch.pool(), chain, &proof_name, "unwrapped", None)
            .await?;
    assert_eq!(proof_bindings.v2, Some((proof_binding, proof_resource)));

    let released_name = format!(
        "ens:{:#x}",
        raw_namehash(&[b"qualifying-released-v2", b"classifier", b"eth"])
    );
    let released_bindings = seed_authority_classifier_case(
        scratch.pool(),
        chain,
        &released_name,
        EnsArmSet::V2,
        EnsArmSet::V2,
        1,
        None,
    )
    .await?;
    let (released_binding, released_resource) = released_bindings.v2.unwrap();
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(3) WHERE surface_binding_id = $1",
    )
    .bind(released_binding)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        3,
        Some(&released_name),
        Some(&released_resource.to_string()),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({"status":"released"}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        4,
        Some(&released_name),
        None,
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":4_000}),
        json!({}),
    )
    .await?;

    let regime_name = format!(
        "ens:{:#x}",
        raw_namehash(&[b"carried-released-v2-regime", b"classifier", b"eth"])
    );
    let regime_bindings = seed_authority_classifier_case(
        scratch.pool(),
        chain,
        &regime_name,
        EnsArmSet::V2,
        EnsArmSet::V2,
        1,
        None,
    )
    .await?;
    let (old_regime_binding, old_regime_resource) = regime_bindings.v2.unwrap();
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(3) WHERE surface_binding_id = $1",
    )
    .bind(old_regime_binding)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        3,
        Some(&regime_name),
        Some(&old_regime_resource.to_string()),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({"status":"released"}),
        json!({}),
    )
    .await?;
    let new_regime_resource = Uuid::new_v4();
    let new_regime_binding = Uuid::new_v4();
    insert_classifier_resource_and_binding(
        scratch.pool(),
        chain,
        &regime_name,
        "ens_v2",
        new_regime_resource,
        new_regime_binding,
        4,
        None,
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        4,
        Some(&regime_name),
        Some(&new_regime_resource.to_string()),
        "RegistrationGranted",
        "ens_v2_registry_l1",
        json!({"status":"registered","registrant":OWNER}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        5,
        Some(&regime_name),
        None,
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":5_000}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    for (case, logical_name_id, arm, binding, reason) in expected {
        let actual: (
            Option<String>,
            String,
            Option<String>,
            Option<Uuid>,
            Option<Uuid>,
        ) = sqlx::query_as(
            "SELECT provenance #>> '{authority_selection,authority_arm}',
                    support_status, unsupported_reason, surface_binding_id, resource_id
             FROM name_current WHERE logical_name_id = $1",
        )
        .bind(&logical_name_id)
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(actual.0, arm, "{case}: selected authority arm");
        assert_eq!(
            actual.1,
            if binding.is_some() {
                "supported"
            } else {
                "unsupported"
            },
            "{case}: support status"
        );
        assert_eq!(actual.2, reason, "{case}: unsupported reason");
        assert_eq!(actual.3, binding.map(|row| row.0), "{case}: binding");
        assert_eq!(actual.4, binding.map(|row| row.1), "{case}: resource");
    }
    for (case, name, binding, resource, proof_kind) in [
        (
            "activated_migration_proof",
            proof_name.as_str(),
            proof_binding,
            proof_resource,
            Some("migration_authority_transition"),
        ),
        (
            "qualifying_released_v2_authority",
            released_name.as_str(),
            released_binding,
            released_resource,
            None,
        ),
        (
            "carried_released_v2_regime",
            regime_name.as_str(),
            new_regime_binding,
            new_regime_resource,
            None,
        ),
    ] {
        type AuthorityOverrideRow = (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<Uuid>,
            Option<Uuid>,
        );
        let actual: AuthorityOverrideRow = sqlx::query_as(
            "SELECT provenance #>> '{authority_selection,authority_arm}',
                        provenance #>> '{authority_selection,proof_kind}',
                        unsupported_reason, surface_binding_id, resource_id
                 FROM name_current WHERE logical_name_id = $1",
        )
        .bind(name)
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(actual.0.as_deref(), Some("ens_v2"), "{case}: arm");
        assert_eq!(actual.1.as_deref(), proof_kind, "{case}: proof kind");
        assert_eq!(actual.2, None, "{case}: unsupported reason");
        assert_eq!(actual.3, Some(binding), "{case}: binding");
        assert_eq!(actual.4, Some(resource), "{case}: resource");
    }

    scratch.cleanup().await
}

#[tokio::test]
async fn sepolia_live_v1_plus_new_v2_reservation_selects_v1() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_new_reservation").await?;
    let chain = "project-authority-new-reservation";
    let source_family = "ens_v2_registry_l1";
    let (logical_name_id, _) =
        seed_raw_v2_reservation_fixture(scratch.pool(), chain, source_family).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 6,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 6).await?;
    assert_reservation_selects_v1(scratch.pool(), chain, &logical_name_id, source_family).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn sepolia_live_v1_plus_reserved_expiry_resync_selects_v1() -> Result<()> {
    for (fixture, source_family) in [
        ("registry", "ens_v2_registry_l1"),
        ("root", "ens_v2_root_l1"),
    ] {
        let scratch =
            ScratchDatabase::create(&format!("project_authority_reservation_resync_{fixture}"))
                .await?;
        let chain = if source_family == "ens_v2_root_l1" {
            "ethereum-sepolia".to_owned()
        } else {
            format!("project-authority-reservation-resync-{fixture}")
        };
        let (logical_name_id, token_id) =
            seed_raw_v2_reservation_fixture(scratch.pool(), &chain, source_family).await?;
        InterpretEngine::new(scratch.pool().clone())
            .run_batch(InterpretRequest {
                chain_id: chain.clone(),
                from_block: 0,
                to_block: 6,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await?;
        insert_raw_v2_reservation_expiry(scratch.pool(), &chain, token_id, 4_100_000_000).await?;
        InterpretEngine::new(scratch.pool().clone())
            .run_batch(InterpretRequest {
                chain_id: chain.clone(),
                from_block: 7,
                to_block: 7,
                resume_current: Some(InterpretMarker {
                    number: 6,
                    hash: block_hash(&chain, 6),
                }),
                mode: InterpretRunMode::Normal,
            })
            .await?;

        let event_kinds: Vec<String> = sqlx::query_scalar(
            "SELECT event_kind FROM normalized_events
             WHERE chain_id = $1 AND logical_name_id = $2 AND block_number = 7
             ORDER BY event_kind",
        )
        .bind(&chain)
        .bind(&logical_name_id)
        .fetch_all(scratch.pool())
        .await?;
        assert_eq!(event_kinds, vec!["ExpiryChanged"], "{fixture}");
        run_project(scratch.pool(), &chain, None, RunMode::Normal, 0, 7).await?;
        if source_family == "ens_v2_root_l1" {
            let selected: (Option<String>, String, Option<String>) = sqlx::query_as(
                "SELECT provenance #>> '{authority_selection,authority_arm}',
                        support_status, unsupported_reason
                 FROM name_current WHERE logical_name_id = $1",
            )
            .bind(&logical_name_id)
            .fetch_one(scratch.pool())
            .await?;
            assert_eq!(
                selected,
                (Some("ens_v1".into()), "supported".into(), None),
                "{fixture}"
            );
        } else {
            assert_reservation_selects_v1(scratch.pool(), &chain, &logical_name_id, source_family)
                .await?;
        }
        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn sepolia_live_v1_plus_released_v2_reservation_selects_v1() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_reservation_release").await?;
    let chain = "project-authority-reservation-release";
    let source_family = "ens_v2_registry_l1";
    let (logical_name_id, token_id) =
        seed_raw_v2_reservation_fixture(scratch.pool(), chain, source_family).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 6,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    insert_raw_v2_reservation_release(scratch.pool(), chain, token_id).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 7,
            to_block: 7,
            resume_current: Some(InterpretMarker {
                number: 6,
                hash: block_hash(chain, 6),
            }),
            mode: InterpretRunMode::Normal,
        })
        .await?;
    let releases: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND logical_name_id = $2
           AND event_kind = 'RegistrationReleased'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(releases, 1);
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 7).await?;
    let selected: (Option<String>, String, Option<String>) = sqlx::query_as(
        "SELECT provenance #>> '{authority_selection,authority_arm}',
                support_status, unsupported_reason
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(selected, (Some("ens_v1".into()), "supported".into(), None));
    let v2_bindings: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v2'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(v2_bindings, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn reservation_release_cannot_borrow_a_later_same_resource_registration() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_reservation_release_reuse").await?;
    let chain = "ethereum-sepolia";
    let logical_name_id =
        seed_raw_reservation_release_then_registration_before_v1(scratch.pool(), chain).await?;

    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 3,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;

    let causal_shape: (Uuid, Uuid, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT release.resource_id, registration.resource_id,
                release.block_number, release.transaction_index, release.log_index,
                binding.block_number,
                (binding.provenance ->> 'transaction_index')::bigint,
                (binding.provenance ->> 'log_index')::bigint
         FROM normalized_events release
         JOIN normalized_events registration
           ON registration.chain_id = release.chain_id
          AND registration.logical_name_id = release.logical_name_id
          AND registration.event_kind = 'RegistrationGranted'
         JOIN surface_bindings binding
           ON binding.chain_id = registration.chain_id
          AND binding.logical_name_id = registration.logical_name_id
          AND binding.authority_arm = 'ens_v2'
          AND binding.resource_id = registration.resource_id
         WHERE release.chain_id = $1
           AND release.logical_name_id = $2
           AND release.event_kind = 'RegistrationReleased'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        causal_shape.0, causal_shape.1,
        "the registration must reuse the released reservation resource"
    );
    assert_eq!(causal_shape.2, 2, "the reservation release is in block 2");
    assert_eq!(
        causal_shape.3, 1,
        "the reservation release transaction index is 1"
    );
    assert_eq!(causal_shape.4, 1, "the reservation release log index is 1");
    assert_eq!(
        causal_shape.5, 2,
        "the first matching binding shares the block"
    );
    assert_eq!(
        causal_shape.6, 2,
        "the matching binding transaction index is later in block 2"
    );
    assert_eq!(
        causal_shape.7, 3,
        "the matching binding starts at the later resource-link log"
    );

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 3).await?;
    let selected: (Option<String>, String, Option<String>) = sqlx::query_as(
        "SELECT provenance #>> '{authority_selection,authority_arm}',
                support_status, unsupported_reason
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        selected,
        (
            None,
            "unsupported".into(),
            Some("independent_ens_deployments_overlap".into()),
        ),
        "a later same-resource registration cannot retroactively qualify a reservation release",
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn reservation_release_same_resource_incremental_matches_fresh() -> Result<()> {
    let incremental =
        ScratchDatabase::create("project_authority_release_reuse_incremental").await?;
    let fresh = ScratchDatabase::create("project_authority_release_reuse_fresh").await?;
    let chain = "ethereum-sepolia";
    let incremental_name =
        seed_raw_reservation_release_then_registration_before_v1(incremental.pool(), chain).await?;
    let fresh_name =
        seed_raw_reservation_release_then_registration_before_v1(fresh.pool(), chain).await?;
    assert_eq!(incremental_name, fresh_name);

    InterpretEngine::new(incremental.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 1,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(incremental.pool(), chain, None, RunMode::Normal, 0, 1).await?;
    InterpretEngine::new(incremental.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 2,
            to_block: 2,
            resume_current: Some(InterpretMarker {
                number: 1,
                hash: block_hash(chain, 1),
            }),
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(
        incremental.pool(),
        chain,
        Some(Marker {
            number: 1,
            hash: block_hash(chain, 1),
        }),
        RunMode::Normal,
        2,
        2,
    )
    .await?;
    InterpretEngine::new(incremental.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 3,
            to_block: 3,
            resume_current: Some(InterpretMarker {
                number: 2,
                hash: block_hash(chain, 2),
            }),
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(
        incremental.pool(),
        chain,
        Some(Marker {
            number: 2,
            hash: block_hash(chain, 2),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;

    InterpretEngine::new(fresh.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 3,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(fresh.pool(), chain, None, RunMode::Normal, 0, 3).await?;

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(fresh.pool()).await?,
        "incremental reservation release reuse diverged from a fresh rebuild",
    );
    let incremental_authority: Value = sqlx::query_scalar(
        "SELECT provenance -> 'authority_selection'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&incremental_name)
    .fetch_one(incremental.pool())
    .await?;
    let fresh_authority: Value = sqlx::query_scalar(
        "SELECT provenance -> 'authority_selection'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&fresh_name)
    .fetch_one(fresh.pool())
    .await?;
    assert_eq!(incremental_authority, fresh_authority);
    assert!(incremental_authority["authority_arm"].is_null());

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn reservation_release_event_vote_requires_a_preexisting_binding() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_release_event_causality").await?;
    let chain = "project-authority-release-event-causality";
    seed_lineage(scratch.pool(), chain, 4).await?;
    declare_sepolia_post_audit_profile(scratch.pool(), chain).await?;
    insert_namespaced_manifest(
        scratch.pool(),
        "ens",
        chain,
        "ens_v2_registry_l1",
        1,
        "ens_v2_sepolia_post_audit",
        "tests/project-authority-release-event-causality.toml",
        json!({}),
    )
    .await?;
    insert_namespaced_manifest(
        scratch.pool(),
        "ens",
        chain,
        "ens_v2_registrar_l1",
        1,
        "ens_v2_sepolia_post_audit",
        "tests/project-authority-release-event-causality-registrar.toml",
        json!({"capability_flags":{"exact_name_profile":{"status":"supported"}}}),
    )
    .await?;
    let logical_name_id = format!(
        "ens:{:#x}",
        raw_namehash(&[b"release-event-causality", b"classifier", b"eth"])
    );
    seed_authority_classifier_case(
        scratch.pool(),
        chain,
        &logical_name_id,
        EnsArmSet::Empty,
        EnsArmSet::Empty,
        1,
        None,
    )
    .await?;

    let future_resource = Uuid::new_v4();
    let future_binding = Uuid::new_v4();
    insert_classifier_resource_and_binding(
        scratch.pool(),
        chain,
        &logical_name_id,
        "ens_v2",
        future_resource,
        future_binding,
        2,
        None,
    )
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(3)
         WHERE surface_binding_id = $1",
    )
    .bind(future_binding)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        2,
        Some(&logical_name_id),
        Some(&future_resource.to_string()),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({"status":"released"}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        3,
        Some(&logical_name_id),
        Some(&future_resource.to_string()),
        "RegistrationReserved",
        "ens_v2_registry_l1",
        json!({"status":"reserved"}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        4,
        Some(&logical_name_id),
        None,
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":4_000}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 4).await?;
    let selected: (Option<String>, String, Option<String>) = sqlx::query_as(
        "SELECT provenance #>> '{authority_selection,authority_arm}',
                support_status, unsupported_reason
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        selected,
        (
            Some("ens_v1".into()),
            "unsupported".into(),
            Some("current_authority_not_projected".into()),
        ),
        "a release cannot borrow a later closed binding to create an ENSv2 event vote",
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn reservation_era_selection_incremental_matches_fresh() -> Result<()> {
    let incremental = ScratchDatabase::create("project_reservation_incremental").await?;
    let fresh = ScratchDatabase::create("project_reservation_fresh").await?;
    let chain = "project-reservation-convergence";
    let (incremental_name, incremental_token) =
        seed_raw_v2_reservation_fixture(incremental.pool(), chain, "ens_v2_registry_l1").await?;
    let (fresh_name, fresh_token) =
        seed_raw_v2_reservation_fixture(fresh.pool(), chain, "ens_v2_registry_l1").await?;
    assert_eq!(incremental_name, fresh_name);

    InterpretEngine::new(incremental.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 6,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(incremental.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    run_project(
        incremental.pool(),
        chain,
        Some(Marker {
            number: 5,
            hash: block_hash(chain, 5),
        }),
        RunMode::Normal,
        6,
        6,
    )
    .await?;
    insert_raw_v2_reservation_expiry(incremental.pool(), chain, incremental_token, 4_100_000_000)
        .await?;
    InterpretEngine::new(incremental.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 7,
            to_block: 7,
            resume_current: Some(InterpretMarker {
                number: 6,
                hash: block_hash(chain, 6),
            }),
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(
        incremental.pool(),
        chain,
        Some(Marker {
            number: 6,
            hash: block_hash(chain, 6),
        }),
        RunMode::Normal,
        7,
        7,
    )
    .await?;

    insert_raw_v2_reservation_expiry(fresh.pool(), chain, fresh_token, 4_100_000_000).await?;
    InterpretEngine::new(fresh.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 7,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(fresh.pool(), chain, None, RunMode::Normal, 0, 7).await?;

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(fresh.pool()).await?,
        "incremental reservation selection diverged from a fresh rebuild"
    );
    let incremental_authority: Value = sqlx::query_scalar(
        "SELECT provenance -> 'authority_selection'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&incremental_name)
    .fetch_one(incremental.pool())
    .await?;
    let fresh_authority: Value = sqlx::query_scalar(
        "SELECT provenance -> 'authority_selection'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&fresh_name)
    .fetch_one(fresh.pool())
    .await?;
    assert_eq!(incremental_authority, fresh_authority);
    assert_eq!(fresh_authority["authority_arm"], "ens_v1");

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn identity_only_name_has_no_projected_current_authority() -> Result<()> {
    let scratch = ScratchDatabase::create("project_authority_identity_only").await?;
    seed_project_fixture(scratch.pool()).await?;
    sqlx::query("DELETE FROM normalized_events WHERE logical_name_id = 'ens:0xalice'")
        .execute(scratch.pool())
        .await?;
    sqlx::query("DELETE FROM surface_bindings WHERE logical_name_id = 'ens:0xalice'")
        .execute(scratch.pool())
        .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let support: (String, Option<String>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        support,
        (
            "unsupported".into(),
            Some("current_authority_not_projected".into())
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn bindingless_resolver_summary_ignores_selected_head_resource_shape() -> Result<()> {
    const SELECTED_RESOURCE: &str = "00000000-0000-0000-0000-0000000008f1";
    let incremental = ScratchDatabase::create("project_bindingless_resolver_head_resource").await?;
    let fresh = ScratchDatabase::create("project_bindingless_resolver_head_resource_fresh").await?;
    let chain = "project-bindingless-resolver-head-resource";
    let resourceful = format!(
        "ens:{:#x}",
        raw_namehash(&[b"resourceful-head", b"classifier", b"eth"])
    );
    let resourceless = format!(
        "ens:{:#x}",
        raw_namehash(&[b"resourceless-head", b"classifier", b"eth"])
    );

    for pool in [incremental.pool(), fresh.pool()] {
        seed_lineage(pool, chain, 3).await?;
        declare_sepolia_post_audit_profile(pool, chain).await?;
        insert_namespaced_manifest(
            pool,
            "ens",
            chain,
            "ens_v2_registry_l1",
            1,
            "ens_v2_sepolia_post_audit",
            "tests/project-bindingless-resolver-head-resource-registry.toml",
            json!({}),
        )
        .await?;
        insert_namespaced_manifest(
            pool,
            "ens",
            chain,
            "ens_v2_registrar_l1",
            1,
            "ens_v2_sepolia_post_audit",
            "tests/project-bindingless-resolver-head-resource-registrar.toml",
            json!({"capability_flags":{"exact_name_profile":{"status":"supported"}}}),
        )
        .await?;
        seed_authority_classifier_case(
            pool,
            chain,
            &resourceful,
            EnsArmSet::Empty,
            EnsArmSet::V2,
            1,
            None,
        )
        .await?;
        seed_authority_classifier_case(
            pool,
            chain,
            &resourceless,
            EnsArmSet::Empty,
            EnsArmSet::V2,
            1,
            None,
        )
        .await?;
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, $2, $3, 2, 'canonical')",
        )
        .bind(Uuid::parse_str(SELECTED_RESOURCE)?)
        .bind(chain)
        .bind(block_hash(chain, 2))
        .execute(pool)
        .await?;
        sqlx::query(
            "UPDATE normalized_events SET resource_id = $1
             WHERE chain_id = $2 AND logical_name_id = $3
               AND event_kind = 'RegistrationGranted'",
        )
        .bind(Uuid::parse_str(SELECTED_RESOURCE)?)
        .bind(chain)
        .bind(&resourceful)
        .execute(pool)
        .await?;
        for logical_name_id in [&resourceful, &resourceless] {
            insert_event(
                pool,
                chain,
                3,
                Some(logical_name_id),
                None,
                "ResolverChanged",
                "ens_v2_registry_l1",
                json!({"resolver":RESOLVER}),
                json!({}),
            )
            .await?;
        }
    }

    run_project(incremental.pool(), chain, None, RunMode::Normal, 0, 2).await?;
    run_project(
        incremental.pool(),
        chain,
        Some(Marker {
            number: 2,
            hash: block_hash(chain, 2),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    run_project(fresh.pool(), chain, None, RunMode::Normal, 0, 3).await?;

    for pool in [incremental.pool(), fresh.pool()] {
        for logical_name_id in [&resourceful, &resourceless] {
            let summary: (String, Option<String>, Option<String>) = sqlx::query_as(
                "SELECT support_status, unsupported_reason,
                        declared_summary #>> '{resolver,address}'
                 FROM name_current WHERE logical_name_id = $1",
            )
            .bind(logical_name_id)
            .fetch_one(pool)
            .await?;
            assert_eq!(
                summary,
                (
                    "unsupported".into(),
                    Some("current_authority_not_projected".into()),
                    Some(RESOLVER.into()),
                ),
                "bindingless resolver summary changed with the selected head's resource shape",
            );
        }
    }
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(fresh.pool()).await?,
        "incremental bindingless resolver publication diverged from a fresh rebuild",
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn raw_ingest_fixture_flows_through_interpret_then_project() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_raw_flow").await?;
    let chain = "project-raw-flow";
    seed_raw_registration_fixture(scratch.pool(), chain).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;

    let outputs: (i64, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT count(*) FROM name_current),
            (SELECT count(*) FROM children_current),
            (SELECT count(*) FROM permissions_current),
            (SELECT count(*) FROM permissions_current_resource_summary),
            (SELECT count(*) FROM record_inventory_current),
            (SELECT count(*) FROM resolver_current),
            (SELECT count(*) FROM address_names_current),
            (SELECT count(*) FROM primary_names_current)",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        [
            outputs.0, outputs.1, outputs.2, outputs.3, outputs.4, outputs.5, outputs.6, outputs.7,
        ]
        .into_iter()
        .all(|count| count > 0),
        "every projection table must be populated from interpreted raw facts: {outputs:?}"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn checked_in_sepolia_v1_resolver_logs_flow_through_interpret_and_project() -> Result<()> {
    const CHAIN: &str = "ethereum-sepolia";
    const REGISTRY: &str = "0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e";
    const REGISTRAR: &str = "0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85";
    const WRAPPER: &str = "0x0635513f179D50A207757E05759CbD106d7dFcE8";
    const RESOLVER: &str = "0xE99638b40E4Fff0129D56f03b55b6bbC4BBE49b5";
    const FIRST_BLOCK: i64 = 8_580_001;

    let scratch = ScratchDatabase::create("production_project_sepolia_v1_resolver").await?;
    let profile = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(scratch.pool(), &load_repository(profile)?).await?;
    for block in FIRST_BLOCK..=FIRST_BLOCK + 3 {
        insert_lineage_block(scratch.pool(), CHAIN, block).await?;
    }

    let eth_node = raw_namehash(&[b"eth"]);
    let alice_node = raw_namehash(&[b"alice", b"eth"]);
    let wrapped = NameWrapped {
        node: alice_node,
        name: b"\x05alice\x03eth\0".to_vec().into(),
        owner: OWNER.parse::<Address>()?,
        fuses: 0,
        expiry: 4_000_000_000,
    }
    .encode_log_data();
    insert_raw_event(
        scratch.pool(),
        CHAIN,
        FIRST_BLOCK,
        WRAPPER,
        wrapped.topics(),
        wrapped.data.as_ref(),
    )
    .await?;
    let default_address = AddressChanged {
        node: alice_node,
        coinType: U256::from(1_u64 << 31),
        newAddress: vec![0_u8; 20].into(),
    }
    .encode_log_data();
    insert_raw_event_at(
        scratch.pool(),
        CHAIN,
        FIRST_BLOCK + 3,
        1,
        1,
        RESOLVER,
        default_address.topics(),
        default_address.data.as_ref(),
    )
    .await?;
    let registration = NameRegistered {
        name: "alice".into(),
        label: B256::from(keccak256(b"alice")),
        owner: OWNER.parse::<Address>()?,
        expires: U256::from(4_000_000_000_u64),
    }
    .encode_log_data();
    insert_raw_event(
        scratch.pool(),
        CHAIN,
        FIRST_BLOCK + 1,
        REGISTRAR,
        registration.topics(),
        registration.data.as_ref(),
    )
    .await?;
    let child = NewOwner {
        node: eth_node,
        label: B256::from(keccak256(b"alice")),
        owner: OWNER.parse::<Address>()?,
    }
    .encode_log_data();
    insert_raw_event_at(
        scratch.pool(),
        CHAIN,
        FIRST_BLOCK + 1,
        1,
        1,
        REGISTRY,
        child.topics(),
        child.data.as_ref(),
    )
    .await?;
    let resolver = NewResolver {
        node: alice_node,
        resolver: RESOLVER.parse::<Address>()?,
    }
    .encode_log_data();
    insert_raw_event(
        scratch.pool(),
        CHAIN,
        FIRST_BLOCK + 2,
        REGISTRY,
        resolver.topics(),
        resolver.data.as_ref(),
    )
    .await?;
    let record = TextChanged {
        node: alice_node,
        indexedKey: keccak256(b"url"),
        key: "url".into(),
        value: "https://sepolia-v1.example.test".into(),
    }
    .encode_log_data();
    insert_raw_event(
        scratch.pool(),
        CHAIN,
        FIRST_BLOCK + 3,
        RESOLVER,
        record.topics(),
        record.data.as_ref(),
    )
    .await?;

    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: CHAIN.into(),
            from_block: FIRST_BLOCK,
            to_block: FIRST_BLOCK + 3,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    let interpreted: (String, String, String, String) = sqlx::query_as(
        "SELECT source_family, lower(raw_fact_ref ->> 'emitting_address'),
                after_state ->> 'record_key', after_state ->> 'value'
         FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RecordChanged'
           AND block_number = $2 AND after_state ->> 'record_key' = 'text:url'",
    )
    .bind(CHAIN)
    .bind(FIRST_BLOCK + 3)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        interpreted,
        (
            "ens_v1_resolver_l1".into(),
            RESOLVER.to_ascii_lowercase(),
            "text:url".into(),
            "https://sepolia-v1.example.test".into(),
        )
    );

    run_project(
        scratch.pool(),
        CHAIN,
        None,
        RunMode::Normal,
        FIRST_BLOCK,
        FIRST_BLOCK + 3,
    )
    .await?;
    let resolver_row: (String, String, String, Value) = sqlx::query_as(
        "SELECT support_status,
                declared_summary #>> '{classification,source_family}',
                declared_summary #>> '{classification,role}',
                declared_summary #> '{classification,read_features}'
         FROM resolver_current current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        resolver_row,
        (
            "supported".into(),
            "ens_v1_resolver_l1".into(),
            "public_resolver".into(),
            json!(["ensip19_default_address"]),
        )
    );
    let binding: (String, Uuid) = sqlx::query_as(
        "SELECT lower(declared_summary -> 'resolver' ->> 'address'), resource_id
         FROM name_current WHERE lower(raw_name) = 'alice.eth'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(binding.0, RESOLVER.to_ascii_lowercase());
    let inventory: (Value, Value) = sqlx::query_as(
        "SELECT entries, provenance
         FROM record_inventory_current
         WHERE resource_id = $1",
    )
    .bind(binding.1)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        inventory.1["read_rules"],
        json!([{
            "kind": "ensip19_default_address",
            "source_record_key": ENSIP19_DEFAULT_RECORD_KEY,
        }])
    );
    let extended_coin_type = (1_u64 << 31) | 10;
    let derived = evaluate_indexed_record(
        &inventory.0,
        &inventory.1,
        &inventory.1["coverage"],
        &format!("addr:{extended_coin_type}"),
        "addr",
        Some(&extended_coin_type.to_string()),
    );
    assert_eq!(derived.status, IndexedRecordStatus::Success);
    assert_eq!(
        derived.value,
        Some(json!("0x0000000000000000000000000000000000000000"))
    );
    assert_eq!(
        derived.derivation.expect("ENSIP-19 derivation").rule,
        ResolverReadFeature::Ensip19DefaultAddress
    );
    let coin_60 = evaluate_indexed_record(
        &inventory.0,
        &inventory.1,
        &inventory.1["coverage"],
        "addr:60",
        "addr",
        Some("60"),
    );
    assert_eq!(coin_60.status, IndexedRecordStatus::NotFound);
    assert_eq!(
        coin_60
            .derivation
            .expect("coin-60 ENSIP-19 derivation")
            .rule,
        ResolverReadFeature::Ensip19DefaultAddress
    );
    assert!(
        inventory.0.as_array().is_some_and(|entries| {
            entries.iter().any(|entry| {
                entry["record_key"] == "text:url"
                    && entry["value"] == "https://sepolia-v1.example.test"
            })
        }),
        "the original text record must remain in the projected inventory"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn declared_v1_shared_resolver_reclassifies_both_v2_pointer_origins_and_converges()
-> Result<()> {
    const CHAIN: &str = "project-declared-v1-shared";

    let incremental = ScratchDatabase::create("project_declared_v1_shared_incremental").await?;
    let fresh = ScratchDatabase::create("project_declared_v1_shared_fresh").await?;
    let manifests = seed_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN).await?;

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let interim: (String, String, i64, i64) = sqlx::query_as(
        "SELECT resolver.declared_summary #>> '{classification,source_family}',
                resolver.support_status,
                (SELECT count(*) FROM record_inventory_current
                 WHERE resource_id IN ($3::uuid, $4::uuid)
                   AND jsonb_array_length(entries) = 0),
                (SELECT count(*) FROM resolver_current
                 WHERE chain_id = $1 AND resolver_address = lower($2))
         FROM resolver_current resolver
         WHERE resolver.chain_id = $1 AND resolver.resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(DECLARED_V1_SHARED_RESOLVER)
    .bind(DECLARED_V1_ALICE_RESOURCE)
    .bind(DECLARED_V1_BOB_RESOURCE)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(
        interim,
        ("ens_v2_resolver_l1".into(), "unsupported".into(), 2, 1),
        "a foreign-namespace declaration must not override ENS discovery",
    );
    sqlx::query("DELETE FROM record_inventory_current WHERE resource_id = $1::uuid")
        .bind(DECLARED_V1_BOB_RESOURCE)
        .execute(incremental.pool())
        .await?;
    activate_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN, manifests).await?;

    for pool in [incremental.pool(), fresh.pool()] {
        let projected_family: String = sqlx::query_scalar(
            "SELECT declared_summary #>> '{classification,source_family}'
             FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)",
        )
        .bind(CHAIN)
        .bind(DECLARED_V1_SHARED_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(projected_family, "ens_v1_resolver_l1");
        let classification: (i64, String, String, String, i64) = sqlx::query_as(
            "SELECT count(*),
                    min(support_status),
                    min(declared_summary #>> '{classification,basis}'),
                    min(declared_summary #>> '{classification,role}'),
                    (SELECT count(*) FROM resolver_current
                     WHERE chain_id = $1
                       AND resolver_address = lower($3)
                       AND declared_summary #>> '{classification,source_family}' =
                           'ens_v2_resolver_l1'
                       AND support_status = 'unsupported')
             FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)",
        )
        .bind(CHAIN)
        .bind(DECLARED_V1_SHARED_RESOLVER)
        .bind(DECLARED_V1_UNDECLARED_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            classification,
            (
                1,
                "supported".into(),
                "manifest_declared_address".into(),
                "public_resolver".into(),
                1,
            ),
        );
        let inventories: Vec<(Uuid, String, String)> = sqlx::query_as(
            "SELECT resource_id, support_status,
                    entries -> 0 ->> 'value'
             FROM record_inventory_current
             WHERE resource_id IN ($1::uuid, $2::uuid)
             ORDER BY resource_id",
        )
        .bind(DECLARED_V1_ALICE_RESOURCE)
        .bind(DECLARED_V1_BOB_RESOURCE)
        .fetch_all(pool)
        .await?;
        assert_eq!(
            inventories,
            vec![
                (
                    Uuid::parse_str(DECLARED_V1_ALICE_RESOURCE)?,
                    "supported".into(),
                    "https://alice.shared.example.test".into(),
                ),
                (
                    Uuid::parse_str(DECLARED_V1_BOB_RESOURCE)?,
                    "supported".into(),
                    "https://bob.shared.example.test".into(),
                ),
            ],
            "each pointer origin must receive only its node-matched v1 record",
        );
    }

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(fresh.pool()).await?,
        "manifest-only incremental reclassification diverged from a fresh rebuild",
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn declared_v1_node_only_record_update_matches_fresh_projection() -> Result<()> {
    assert_declared_v1_node_only_delta(
        "project-declared-v1-node-record-update",
        DeclaredV1NodeOnlyDelta::RecordUpdate,
    )
    .await
}

#[tokio::test]
async fn declared_v1_node_only_version_reset_matches_fresh_projection() -> Result<()> {
    assert_declared_v1_node_only_delta(
        "project-declared-v1-node-version-reset",
        DeclaredV1NodeOnlyDelta::VersionReset,
    )
    .await
}

#[tokio::test]
async fn declared_v1_node_only_unrelated_record_preserves_inventory_clock() -> Result<()> {
    assert_declared_v1_node_only_delta(
        "project-declared-v1-unrelated-node-record",
        DeclaredV1NodeOnlyDelta::UnrelatedRecord,
    )
    .await
}

#[tokio::test]
async fn declared_v1_node_only_update_rebuilds_retained_pointer_resource() -> Result<()> {
    const CHAIN: &str = "project-declared-v1-retained-pointer-resource";
    const REPLACEMENT_RESOURCE: &str = "00000000-0000-0000-0000-000000000d03";
    const ALIAS_BINDING: &str = "00000000-0000-0000-0000-000000000d13";
    const REPLACEMENT_BINDING: &str = "00000000-0000-0000-0000-000000000d14";

    let incremental = ScratchDatabase::create("project_declared_v1_retained_incremental").await?;
    let fresh = ScratchDatabase::create("project_declared_v1_retained_fresh").await?;
    let manifests = seed_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN).await?;
    let alice_node = format!("{:#x}", raw_namehash(&[b"alice", b"eth"]));
    let alice_name = format!("ens:{alice_node}");
    let alias_node = format!("{:#x}", raw_namehash(&[b"alias", b"eth"]));
    let alias_name = format!("ens:{alias_node}");

    for (pool, manifest_id) in [
        (incremental.pool(), manifests.0),
        (fresh.pool(), manifests.1),
    ] {
        insert_manifest_update_event(
            pool,
            CHAIN,
            "ens_v1_resolver_l1",
            manifest_id,
            resolver_declaration_payload("ens", CHAIN, DECLARED_V1_SHARED_RESOLVER),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events
             SET after_state = jsonb_set(after_state, '{value}', '\"old\"')
             WHERE chain_id = $1 AND event_kind = 'RecordChanged'
               AND after_state ->> 'node' = $2",
        )
        .bind(CHAIN)
        .bind(&alice_node)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES (
                 $1, 'ens', 'alias.eth', ARRAY['alias', 'eth'],
                 decode('00', 'hex'), $2, ARRAY[$3, $4], $5,
                 'active', $6, $7, 1, 'canonical'
             )",
        )
        .bind(&alias_name)
        .bind(&alias_node)
        .bind(format!("{:#x}", keccak256(b"alias")))
        .bind(format!("{:#x}", keccak256(b"eth")))
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind,
                 authority_arm, active_from, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES (
                 $1, $2, $3::uuid, 'declared_registry_path', 'ens_v2',
                 to_timestamp(1), $4, $5, 1, 'canonical'
             )",
        )
        .bind(Uuid::parse_str(ALIAS_BINDING)?)
        .bind(&alias_name)
        .bind(DECLARED_V1_ALICE_RESOURCE)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    for pool in [incremental.pool(), fresh.pool()] {
        insert_lineage_block(pool, CHAIN, 4).await?;
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1::uuid, $2, $3, 4, 'canonical')",
        )
        .bind(REPLACEMENT_RESOURCE)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 4))
        .execute(pool)
        .await?;
        sqlx::query(
            "UPDATE surface_bindings SET active_to = to_timestamp(4)
             WHERE logical_name_id = $1 AND resource_id = $2::uuid",
        )
        .bind(&alice_name)
        .bind(DECLARED_V1_ALICE_RESOURCE)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind,
                 authority_arm, active_from, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES (
                 $1, $2, $3::uuid, 'declared_registry_path', 'ens_v2',
                 to_timestamp(4), $4, $5, 4, 'canonical'
             )",
        )
        .bind(Uuid::parse_str(REPLACEMENT_BINDING)?)
        .bind(&alice_name)
        .bind(REPLACEMENT_RESOURCE)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 4))
        .execute(pool)
        .await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let rebound_resources: Vec<(String, Uuid)> = sqlx::query_as(
        "SELECT logical_name_id, resource_id
         FROM name_current WHERE logical_name_id IN ($1, $2)
         ORDER BY CASE WHEN logical_name_id = $1 THEN 0 ELSE 1 END",
    )
    .bind(&alice_name)
    .bind(&alias_name)
    .fetch_all(incremental.pool())
    .await?;
    assert_eq!(
        rebound_resources,
        vec![
            (alice_name.clone(), Uuid::parse_str(REPLACEMENT_RESOURCE)?),
            (
                alias_name.clone(),
                Uuid::parse_str(DECLARED_V1_ALICE_RESOURCE)?,
            ),
        ],
        "the pointer name must rebind while another name retains the old resource",
    );

    for pool in [incremental.pool(), fresh.pool()] {
        insert_lineage_block(pool, CHAIN, 5).await?;
        insert_event(
            pool,
            CHAIN,
            5,
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({
                "node": alice_node,
                "resolver": DECLARED_V1_SHARED_RESOLVER,
                "record_key": "text:url",
                "record_family": "text",
                "selector_key": "url",
                "value_retained": true,
                "value": "new"
            }),
            json!({"emitting_address": DECLARED_V1_SHARED_RESOLVER}),
        )
        .await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 5).await?;

    for pool in [incremental.pool(), fresh.pool()] {
        let value: Option<String> = sqlx::query_scalar(
            "SELECT entries -> 0 ->> 'value'
             FROM record_inventory_current WHERE resource_id = $1::uuid",
        )
        .bind(DECLARED_V1_ALICE_RESOURCE)
        .fetch_one(pool)
        .await?;
        assert_eq!(value.as_deref(), Some("new"));
    }
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(fresh.pool()).await?,
        "retained pointer resource update diverged from a fresh rebuild",
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn foreign_namespace_v2_pointer_matches_fresh_declared_v1_attribution() -> Result<()> {
    const CHAIN: &str = "project-declared-v1-foreign-pointer";

    let incremental = ScratchDatabase::create("project_declared_v1_foreign_incremental").await?;
    let fresh = ScratchDatabase::create("project_declared_v1_foreign_fresh").await?;
    let manifests = seed_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN).await?;
    for pool in [incremental.pool(), fresh.pool()] {
        sqlx::query(
            "UPDATE normalized_events SET namespace = 'foreign'
             WHERE chain_id = $1 AND resource_id = $2::uuid
               AND event_kind = 'ResolverChanged'",
        )
        .bind(CHAIN)
        .bind(DECLARED_V1_BOB_RESOURCE)
        .execute(pool)
        .await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    activate_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN, manifests).await?;
    for pool in [incremental.pool(), fresh.pool()] {
        let bob_inventory: (i32, String, Option<String>) = sqlx::query_as(
            "SELECT jsonb_array_length(entries), support_status, unsupported_reason
             FROM record_inventory_current WHERE resource_id = $1::uuid",
        )
        .bind(DECLARED_V1_BOB_RESOURCE)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            bob_inventory,
            (
                0,
                "unsupported".into(),
                Some("resolver_classification_missing".into()),
            ),
            "a foreign-namespace pointer must not claim authoritative v1 coverage",
        );
    }
    let clocks_before = declared_v1_shared_clocks(incremental.pool(), CHAIN).await?;
    insert_lineage_block(incremental.pool(), CHAIN, 4).await?;
    sqlx::query("SELECT pg_sleep(0.01)")
        .execute(incremental.pool())
        .await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let clocks_after = declared_v1_shared_clocks(incremental.pool(), CHAIN).await?;
    assert_eq!(
        clocks_after, clocks_before,
        "a foreign declaration must not keep invalidating an unchanged resolver",
    );
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(fresh.pool()).await?,
        "foreign-namespace pointer attribution diverged from a fresh rebuild",
    );
    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn cross_namespace_declared_resolver_collapse_is_deterministic_and_converges() -> Result<()> {
    const CHAIN: &str = "project-declared-v1-cross-namespace";

    let incremental = ScratchDatabase::create("project_declared_v1_cross_ns_incremental").await?;
    let fresh = ScratchDatabase::create("project_declared_v1_cross_ns_fresh").await?;
    let ens_manifests =
        seed_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN).await?;
    let mut basenames_manifests = Vec::new();
    for pool in [incremental.pool(), fresh.pool()] {
        basenames_manifests
            .push(add_shared_resolver_discovery_namespace(pool, CHAIN, "basenames").await?);
        sqlx::query(
            "UPDATE normalized_events
             SET namespace = 'basenames', source_family = 'basenames_base_registry'
             WHERE chain_id = $1 AND resource_id = $2::uuid
               AND event_kind = 'ResolverChanged'",
        )
        .bind(CHAIN)
        .bind(DECLARED_V1_BOB_RESOURCE)
        .execute(pool)
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let interim: (i64, String) = sqlx::query_as(
        "SELECT (provenance ->> 'manifest_id')::bigint, support_status
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(DECLARED_V1_SHARED_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(
        interim,
        (basenames_manifests[0], "supported".into()),
        "the sole applicable same-namespace declaration must win before the second declaration",
    );

    activate_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN, ens_manifests)
        .await?;
    let mut outcomes = Vec::new();
    for ((pool, ens_manifest_id), basenames_manifest_id) in [
        (incremental.pool(), ens_manifests.0),
        (fresh.pool(), ens_manifests.1),
    ]
    .into_iter()
    .zip(basenames_manifests)
    {
        assert!(
            ens_manifest_id < basenames_manifest_id,
            "the fixture must make the ENS declaration the manifest-identity tie-break winner",
        );
        let winner: (i64, i64, String, String, String) = sqlx::query_as(
            "SELECT count(*), min((provenance ->> 'manifest_id')::bigint),
                    min(declared_summary #>> '{classification,source_family}'),
                    min(declared_summary #>> '{classification,basis}'),
                    min(support_status)
             FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)",
        )
        .bind(CHAIN)
        .bind(DECLARED_V1_SHARED_RESOLVER)
        .fetch_one(pool)
        .await?;
        let losing_inventory: (i32, String, Option<String>) = sqlx::query_as(
            "SELECT jsonb_array_length(entries), support_status, unsupported_reason
             FROM record_inventory_current WHERE resource_id = $1::uuid",
        )
        .bind(DECLARED_V1_BOB_RESOURCE)
        .fetch_one(pool)
        .await?;
        outcomes.push((winner, losing_inventory, ens_manifest_id));
    }
    assert_eq!(
        outcomes[0].0, outcomes[1].0,
        "cross-namespace declaration winner varied between incremental and fresh walks",
    );
    assert_eq!(
        outcomes[0].1, outcomes[1].1,
        "losing-namespace attribution varied between incremental and fresh walks",
    );
    for (winner, losing_inventory, ens_manifest_id) in outcomes {
        assert_eq!(
            winner,
            (
                1,
                ens_manifest_id,
                "ens_v1_resolver_l1".into(),
                "manifest_declared_address".into(),
                "supported".into(),
            ),
            "equal-rank declarations must collapse to the lower manifest identity",
        );
        assert_eq!(
            losing_inventory,
            (
                0,
                "unsupported".into(),
                Some("resolver_classification_missing".into()),
            ),
            "the pointer in the losing declaration namespace must remain unsupported",
        );
    }

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(fresh.pool()).await?,
        "cross-namespace declaration collapse changed across incremental boundaries",
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn declaration_winner_requires_same_namespace_admission_after_close() -> Result<()> {
    const CHAIN: &str = "project-declared-v1-same-namespace-close";

    let incremental = ScratchDatabase::create("project_declared_v1_same_ns_incremental").await?;
    let fresh = ScratchDatabase::create("project_declared_v1_same_ns_fresh").await?;
    let manifests = seed_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN).await?;
    for pool in [incremental.pool(), fresh.pool()] {
        deactivate_foreign_declared_v1_manifest(pool, CHAIN).await?;
        add_shared_resolver_discovery_namespace(pool, CHAIN, "basenames").await?;
        insert_namespaced_manifest(
            pool,
            "basenames",
            CHAIN,
            "ens_v2_resolver_l1",
            1,
            "fixture",
            "tests/project-shared-cross-namespace-v2-resolver.toml",
            json!({"resolver_implementations": []}),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events
             SET after_state = jsonb_set(after_state, '{rollout_status}', '\"deprecated\"')
             WHERE chain_id = $1 AND namespace = 'ens'
               AND event_kind = 'SourceManifestUpdated'
               AND source_family = 'ens_v2_resolver_l1'",
        )
        .bind(CHAIN)
        .execute(pool)
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    activate_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN, manifests).await?;

    for pool in [incremental.pool(), fresh.pool()] {
        insert_lineage_block(pool, CHAIN, 4).await?;
        set_shared_resolver_discovery_namespace_end(pool, CHAIN, "ens", Some(4)).await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;

    for pool in [incremental.pool(), fresh.pool()] {
        let winner: (String, String, Option<String>) = sqlx::query_as(
            "SELECT declared_summary #>> '{classification,source_family}',
                    support_status,
                    provenance ->> 'classification_admission_namespace'
             FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)",
        )
        .bind(CHAIN)
        .bind(DECLARED_V1_SHARED_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            winner,
            ("ens_v2_resolver_l1".into(), "unsupported".into(), None),
            "a foreign-namespace admission must not preserve the ENS declaration winner",
        );
    }

    let clocks_before = declared_v1_shared_clocks(incremental.pool(), CHAIN).await?;
    insert_lineage_block(incremental.pool(), CHAIN, 5).await?;
    sqlx::query("SELECT pg_sleep(0.01)")
        .execute(incremental.pool())
        .await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    assert_eq!(
        declared_v1_shared_clocks(incremental.pool(), CHAIN).await?,
        clocks_before,
        "a foreign-namespace admission must not re-scope an unchanged resolver",
    );

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(fresh.pool()).await?,
        "same-namespace declaration eligibility diverged across incremental boundaries",
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn same_namespace_declared_resolver_uses_greatest_applicable_start_role() -> Result<()> {
    assert_same_namespace_declared_resolver_role(
        "project-declared-v1-multi-role-start",
        ("later_start_resolver_role", 1),
        ("earlier_start_resolver_role", 0),
        "later_start_resolver_role",
    )
    .await
}

#[tokio::test]
async fn same_namespace_declared_resolver_uses_later_equal_start_role() -> Result<()> {
    assert_same_namespace_declared_resolver_role(
        "project-declared-v1-multi-role-order",
        ("first_equal_start_role", 0),
        ("later_equal_start_role", 0),
        "later_equal_start_role",
    )
    .await
}

async fn assert_same_namespace_declared_resolver_role(
    chain: &str,
    first_role: (&str, i64),
    second_role: (&str, i64),
    expected_role: &str,
) -> Result<()> {
    let first = ScratchDatabase::create(&format!("{chain}_first")).await?;
    let second = ScratchDatabase::create(&format!("{chain}_second")).await?;
    let manifests = seed_declared_v1_shared_pair(first.pool(), second.pool(), chain).await?;

    for (pool, manifest_id) in [(first.pool(), manifests.0), (second.pool(), manifests.1)] {
        let mut payload = resolver_declaration_payload("ens", chain, DECLARED_V1_SHARED_RESOLVER);
        payload["contracts"] = json!([
            {
                "role": first_role.0,
                "address": DECLARED_V1_SHARED_RESOLVER,
                "proxy_kind": "none",
                "start_block": first_role.1
            },
            {
                "role": second_role.0,
                "address": DECLARED_V1_SHARED_RESOLVER,
                "proxy_kind": "none",
                "start_block": second_role.1
            }
        ]);
        insert_manifest_update_event(pool, chain, "ens_v1_resolver_l1", manifest_id, payload)
            .await?;
        run_project(pool, chain, None, RunMode::Normal, 0, 3).await?;
    }

    let mut roles = Vec::new();
    for pool in [first.pool(), second.pool()] {
        roles.push(
            sqlx::query_scalar::<_, String>(
                "SELECT declared_summary #>> '{classification,role}'
                 FROM resolver_current
                 WHERE chain_id = $1 AND resolver_address = lower($2)",
            )
            .bind(chain)
            .bind(DECLARED_V1_SHARED_RESOLVER)
            .fetch_one(pool)
            .await?,
        );
    }
    assert_eq!(roles, vec![expected_role, expected_role]);

    first.cleanup().await?;
    second.cleanup().await
}

#[tokio::test]
async fn declared_resolver_last_discovery_close_matches_fresh_rebuild() -> Result<()> {
    const CHAIN: &str = "project-declared-v1-discovery-close";

    let incremental = ScratchDatabase::create("project_declared_v1_close_incremental").await?;
    let fresh = ScratchDatabase::create("project_declared_v1_close_fresh").await?;
    let manifests = seed_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN).await?;
    for pool in [incremental.pool(), fresh.pool()] {
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND event_kind IN (
                 'ResolverChanged', 'RecordChanged', 'RecordVersionChanged'
             )",
        )
        .bind(CHAIN)
        .execute(pool)
        .await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    activate_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN, manifests).await?;

    for pool in [incremental.pool(), fresh.pool()] {
        insert_lineage_block(pool, CHAIN, 4).await?;
        set_shared_resolver_discovery_end(pool, CHAIN, Some(4)).await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;

    let incremental_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(DECLARED_V1_SHARED_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    let fresh_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(DECLARED_V1_SHARED_RESOLVER)
    .fetch_one(fresh.pool())
    .await?;
    assert_eq!(
        fresh_count, 0,
        "a declaration alone must not admit a resolver"
    );
    assert_eq!(
        incremental_count, fresh_count,
        "closing the last discovery admission left a stale declared resolver",
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn root_origin_declared_resolver_noop_preserves_clocks_and_converges() -> Result<()> {
    const CHAIN: &str = "project-declared-v1-root-origin";

    let incremental = ScratchDatabase::create("project_declared_v1_root_incremental").await?;
    let fresh = ScratchDatabase::create("project_declared_v1_root_fresh").await?;
    let manifests = seed_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN).await?;
    for pool in [incremental.pool(), fresh.pool()] {
        set_shared_resolver_discovery_origin(pool, CHAIN, "ens_v2_root_l1").await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    activate_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN, manifests).await?;

    for pool in [incremental.pool(), fresh.pool()] {
        let winner: (String, Option<String>) = sqlx::query_as(
            "SELECT support_status,
                    provenance ->> 'classification_admission_namespace'
             FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)",
        )
        .bind(CHAIN)
        .bind(DECLARED_V1_SHARED_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(winner, ("supported".into(), Some("ens".into())));
    }
    let clocks_before = declared_v1_shared_clocks(incremental.pool(), CHAIN).await?;
    insert_lineage_block(incremental.pool(), CHAIN, 4).await?;
    sqlx::query("SELECT pg_sleep(0.01)")
        .execute(incremental.pool())
        .await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    assert_eq!(
        declared_v1_shared_clocks(incremental.pool(), CHAIN).await?,
        clocks_before,
        "a root-origin declaration winner must not re-scope on a no-op batch",
    );
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(fresh.pool()).await?,
        "root-origin declaration classification diverged from a fresh rebuild",
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn observed_declared_resolver_close_and_reopen_converges() -> Result<()> {
    const CHAIN: &str = "project-declared-v1-observed-reopen";

    let incremental = ScratchDatabase::create("project_declared_v1_reopen_incremental").await?;
    let fresh = ScratchDatabase::create("project_declared_v1_reopen_fresh").await?;
    let manifests = seed_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN).await?;
    for pool in [incremental.pool(), fresh.pool()] {
        deactivate_foreign_declared_v1_manifest(pool, CHAIN).await?;
        sqlx::query(
            "UPDATE normalized_events SET source_family = 'ens_v1_registry_l1'
             WHERE chain_id = $1 AND event_kind = 'ResolverChanged'",
        )
        .bind(CHAIN)
        .execute(pool)
        .await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    activate_declared_v1_shared_pair(incremental.pool(), fresh.pool(), CHAIN, manifests).await?;

    for pool in [incremental.pool(), fresh.pool()] {
        insert_lineage_block(pool, CHAIN, 4).await?;
        set_shared_resolver_discovery_end(pool, CHAIN, Some(4)).await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    for pool in [incremental.pool(), fresh.pool()] {
        let closed: (String, Option<String>) = sqlx::query_as(
            "SELECT support_status,
                    provenance ->> 'classification_admission_namespace'
             FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)",
        )
        .bind(CHAIN)
        .bind(DECLARED_V1_SHARED_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(closed, ("supported".into(), None));
    }

    for pool in [incremental.pool(), fresh.pool()] {
        insert_lineage_block(pool, CHAIN, 5).await?;
        set_shared_resolver_discovery_end(pool, CHAIN, None).await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 5).await?;
    for pool in [incremental.pool(), fresh.pool()] {
        let reopened_namespace: Option<String> = sqlx::query_scalar(
            "SELECT provenance ->> 'classification_admission_namespace'
             FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)",
        )
        .bind(CHAIN)
        .bind(DECLARED_V1_SHARED_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(reopened_namespace.as_deref(), Some("ens"));
    }
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(fresh.pool()).await?,
        "resolver admission reopen diverged from a fresh rebuild",
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn registrar_transfer_nested_resolver_scope_matches_full_rebuild() -> Result<()> {
    assert_registrar_transfer_matches_full_rebuild("project-resolver-transfer", false).await
}

#[tokio::test]
async fn registrar_transfer_authority_flip_matches_full_rebuild() -> Result<()> {
    assert_registrar_transfer_matches_full_rebuild("project-resolver-transfer-flip", true).await
}

#[tokio::test]
async fn registrar_reregistration_moves_registry_binding_per_block() -> Result<()> {
    let scratch = ScratchDatabase::create("project-registrar-reregistration-binding").await?;
    let splits = vec![
        (
            11,
            ScratchDatabase::create("project-registrar-binding-split-11").await?,
        ),
        (
            13,
            ScratchDatabase::create("project-registrar-binding-split-13").await?,
        ),
        (
            14,
            ScratchDatabase::create("project-registrar-binding-split-14").await?,
        ),
    ];
    let chain = "project-registrar-reregistration-binding";
    let fixture: Value = serde_json::from_str(include_str!(
        "fixtures/registry-binding-reregistration.json"
    ))?;
    let blocks = serde_json::from_value::<Vec<i64>>(fixture["blocks"].clone())?;
    for pool in std::iter::once(scratch.pool()).chain(splits.iter().map(|(_, db)| db.pool())) {
        seed_raw_registrar_transfer_fixture(pool, chain, true).await?;
        for table in ["raw_logs", "raw_transactions"] {
            sqlx::query(&format!("DELETE FROM {table} WHERE chain_id = $1"))
                .bind(chain)
                .execute(pool)
                .await?;
        }
        for block in 6..=*blocks.last().unwrap() {
            insert_lineage_block(pool, chain, block).await?;
        }
        sqlx::raw_sql(
            "ALTER TABLE chain_lineage DISABLE TRIGGER chain_lineage_protect_identity;
        UPDATE chain_lineage SET block_timestamp = to_timestamp(CASE block_number
        WHEN 2 THEN 8000000 WHEN 3 THEN 8000001 WHEN 4 THEN 8000002
        WHEN 5 THEN 16000004 WHEN 6 THEN 16000005 WHEN 7 THEN 16000006
        WHEN 8 THEN 16000007 ELSE block_number END);
        ALTER TABLE chain_lineage ENABLE TRIGGER chain_lineage_protect_identity;",
        )
        .execute(pool)
        .await?;
        for fact in fixture["facts"].as_array().unwrap() {
            let topics = serde_json::from_value::<Vec<String>>(fact["topics"].clone())?
                .iter()
                .map(|topic| topic.parse())
                .collect::<Result<Vec<B256>, _>>()?;
            let data = alloy_primitives::hex::decode(
                fact["data"].as_str().unwrap().trim_start_matches("0x"),
            )?;
            insert_raw_event_at(
                pool,
                chain,
                fact["block"].as_i64().unwrap(),
                0,
                fact["log"].as_i64().unwrap(),
                fact["emitter"].as_str().unwrap(),
                &topics,
                &data,
            )
            .await?;
        }
    }
    let expected: Vec<Vec<String>> = serde_json::from_value(fixture["expected"].clone())?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: *blocks.last().unwrap(),
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    let full_payload = registry_transition_payload(scratch.pool(), chain).await?;
    assert_eq!(full_payload["owner_getter"], TRANSFER_OWNER);
    assert_eq!(full_payload["registry_contract"], REGISTRY);
    for (split, database) in &splits {
        let first = InterpretEngine::new(database.pool().clone())
            .run_batch(InterpretRequest {
                chain_id: chain.into(),
                from_block: 0,
                to_block: *split,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await?;
        InterpretEngine::new(database.pool().clone())
            .run_batch(InterpretRequest {
                chain_id: chain.into(),
                from_block: split + 1,
                to_block: 24,
                resume_current: Some(first.current),
                mode: InterpretRunMode::Normal,
            })
            .await?;
        assert_eq!(
            registry_transition_payload(database.pool(), chain).await?,
            full_payload,
            "split after block {split}"
        );
    }
    let incremental = &splits[2].1;
    let logical_name_id: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RegistrationGranted' LIMIT 1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    for (index, block) in blocks.into_iter().enumerate() {
        run_project(scratch.pool(), chain, None, RunMode::Normal, 0, block).await?;
        let rows = registry_binding_rows(scratch.pool(), &logical_name_id).await?;
        let owners = rows
            .iter()
            .filter_map(|row| row.1.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            owners, expected[index],
            "registry owners at block {block}: {rows:?}"
        );
        let selected: Option<Uuid> = sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT resource_id FROM name_current WHERE logical_name_id = $1",
        )
        .bind(&logical_name_id)
        .fetch_one(scratch.pool())
        .await?;
        if let Some(selected) = selected {
            assert!(
                rows.iter().all(|row| row.1.is_none() || row.0 == selected),
                "binding is not on selected resource at block {block}: selected={selected:?} rows={rows:?}"
            );
        }
        assert!(rows.iter().all(|row| row.1.is_none()
            || row.2.as_deref() == Some("0x0000000000000000000000000000000000000044")));
    }
    run_project(incremental.pool(), chain, None, RunMode::Normal, 0, 14).await?;
    run_project(
        incremental.pool(),
        chain,
        Some(Marker {
            number: 14,
            hash: block_hash(chain, 14),
        }),
        RunMode::Normal,
        15,
        24,
    )
    .await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(scratch.pool()).await?;
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(scratch.pool()).await?
    );
    InterpretEngine::new(incremental.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 24,
            resume_current: None,
            mode: InterpretRunMode::Redo,
        })
        .await?;
    assert_eq!(
        registry_transition_payload(incremental.pool(), chain).await?,
        full_payload
    );
    run_project(incremental.pool(), chain, None, RunMode::Normal, 0, 24).await?;
    assert_eq!(
        registry_binding_rows(incremental.pool(), &logical_name_id).await?,
        registry_binding_rows(scratch.pool(), &logical_name_id).await?
    );
    for (_, database) in splits {
        database.cleanup().await?;
    }
    scratch.cleanup().await
}
async fn registry_transition_payload(pool: &PgPool, chain: &str) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE chain_id = $1 AND block_number = 15 AND event_kind = 'SurfaceBound' AND source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar')")
        .bind(chain).fetch_one(pool).await?)
}
async fn registry_binding_rows(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Vec<(Uuid, Option<String>, Option<String>)>> {
    Ok(sqlx::query_as("SELECT resource_id, registry_owner, registry_contract FROM permissions_current_resource_summary WHERE resource_id IN (SELECT resource_id FROM normalized_events WHERE logical_name_id = $1) ORDER BY resource_id")
        .bind(logical_name_id).fetch_all(pool).await?)
}

async fn assert_registrar_transfer_matches_full_rebuild(
    chain: &str,
    include_registry_owner: bool,
) -> Result<()> {
    let incremental = ScratchDatabase::create(&format!("{chain}-incremental")).await?;
    let full = ScratchDatabase::create(&format!("{chain}-full")).await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_raw_registrar_transfer_fixture(pool, chain, include_registry_owner).await?;
        InterpretEngine::new(pool.clone())
            .run_batch(InterpretRequest {
                chain_id: chain.into(),
                from_block: 0,
                to_block: 5,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await?;
    }

    let resolver_permissions: Vec<(Uuid, Value, Value, String, Option<String>)> = sqlx::query_as(
        "SELECT resource_id, before_state, after_state, source_family,
                raw_fact_ref ->> 'emitting_address'
         FROM normalized_events
         WHERE chain_id = $1 AND block_number = 5
           AND event_kind = 'PermissionChanged'
           AND COALESCE(
               after_state #>> '{scope,kind}',
               before_state #>> '{scope,kind}'
           ) = 'resolver'
         ORDER BY normalized_event_id",
    )
    .bind(chain)
    .fetch_all(incremental.pool())
    .await?;
    assert_eq!(
        resolver_permissions.len(),
        2,
        "authority change must emit one resolver revoke and one resolver grant"
    );
    assert!(
        resolver_permissions
            .iter()
            .all(|(_, _, _, family, emitter)| {
                family == "basenames_base_registrar" && emitter.as_deref() == Some(REGISTRAR)
            })
    );
    let revoke = resolver_permissions
        .iter()
        .find(|(_, before, _, _, _)| {
            before.pointer("/subject").and_then(Value::as_str) == Some(OWNER)
        })
        .expect("old-authority resolver revoke");
    assert_eq!(
        revoke.1.pointer("/scope/resolver_address"),
        Some(&json!(RESOLVER))
    );
    assert_eq!(revoke.1["effective_powers"], json!(["resolver_control"]));
    assert_eq!(revoke.2["effective_powers"], json!([]));
    let expected_grantee = if include_registry_owner {
        OWNER
    } else {
        TRANSFER_OWNER
    };
    let grant = resolver_permissions
        .iter()
        .find(|(_, _, after, _, _)| {
            after.pointer("/subject").and_then(Value::as_str) == Some(expected_grantee)
                && after["effective_powers"] == json!(["resolver_control"])
        })
        .expect("new-authority resolver grant");
    assert_eq!(
        grant.2.pointer("/scope/resolver_address"),
        Some(&json!(RESOLVER))
    );
    assert_eq!(
        grant.2.pointer("/grant_source/source_event_kind"),
        Some(&json!("TokenControlTransferred"))
    );
    assert_eq!(revoke.0 == grant.0, !include_registry_owner);
    assert!(resolver_permissions.iter().all(|(_, before, after, _, _)| {
        before.get("resolver").is_none() && after.get("resolver").is_none()
    }));
    let surface_event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND block_number = 5
           AND event_kind IN ('SurfaceBound', 'SurfaceUnbound')",
    )
    .bind(chain)
    .fetch_one(incremental.pool())
    .await?;
    if include_registry_owner {
        assert!(
            surface_event_count > 0,
            "authority mismatch must flip the surface"
        );
    } else {
        assert_eq!(
            surface_event_count, 0,
            "registrar authority must be retained"
        );
    }

    run_project(incremental.pool(), chain, None, RunMode::Normal, 0, 4).await?;
    let pointer_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM record_inventory_current
             WHERE provenance ->> 'chain_id' = $1
               AND lower(provenance ->> 'resolver_address') = lower($2)
         )",
    )
    .bind(chain)
    .bind(RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    assert!(
        pointer_exists,
        "fixture must scope the resolver through its resource pointer"
    );
    run_project(
        incremental.pool(),
        chain,
        Some(Marker {
            number: 4,
            hash: block_hash(chain, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    run_project(full.pool(), chain, None, RunMode::Normal, 0, 5).await?;

    for pool in [incremental.pool(), full.pool()] {
        let current_permission: (Uuid, String, Option<String>, bool) = sqlx::query_as(
            "SELECT resource_id, subject, grant_source ->> 'source_event_kind', (NOT EXISTS (
                 SELECT 1 FROM permissions_current retired WHERE retired.resource_id = $2
                   AND lower(retired.subject) = lower($3)
                   AND retired.effective_powers ? 'resource_control') AND EXISTS (
                 SELECT 1 FROM permissions_current active
                 WHERE active.resource_id = permissions_current.resource_id
                   AND lower(active.subject) = lower(permissions_current.subject)
                   AND active.effective_powers ? 'resource_control'))
             FROM permissions_current
             WHERE scope_kind = 'resolver'
               AND resource_id = (
                   SELECT resource_id FROM name_current
                   WHERE raw_name = 'alice.base.eth'
               )
               AND lower(scope_detail ->> 'resolver_address') = lower($1)",
        )
        .bind(RESOLVER)
        .bind(revoke.0)
        .bind(OWNER)
        .fetch_one(pool)
        .await?;
        let expected_permission = (
            grant.0,
            expected_grantee.into(),
            Some("TokenControlTransferred".into()),
            true,
        );
        assert_eq!(current_permission, expected_permission);
    }

    let incremental_resolver = resolver_permission_summary(incremental.pool(), chain).await?;
    let full_resolver = resolver_permission_summary(full.pool(), chain).await?;
    for summary in [&incremental_resolver, &full_resolver] {
        assert_eq!(
            summary.pointer("/permissions/items/0/subject"),
            Some(&json!(expected_grantee))
        );
        assert_eq!(
            summary.pointer("/permissions/items/0/grant_source/source_event_kind"),
            Some(&json!("TokenControlTransferred"))
        );
        assert_eq!(
            summary.pointer("/role_holders/items/0/subject"),
            Some(&json!(expected_grantee))
        );
    }

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(full.pool()).await?;
    let incremental_snapshot = serving_table_snapshot(incremental.pool()).await?;
    let full_snapshot = serving_table_snapshot(full.pool()).await?;
    for ((incremental_table, incremental_rows), (full_table, full_rows)) in
        incremental_snapshot.iter().zip(full_snapshot.iter())
    {
        assert_eq!(incremental_table, full_table);
        assert_eq!(
            incremental_rows, full_rows,
            "registrar-transfer incremental {incremental_table} diverged from a full rebuild"
        );
    }

    incremental.cleanup().await?;
    full.cleanup().await
}

async fn resolver_permission_summary(pool: &PgPool, chain: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
             'permissions', declared_summary -> 'permissions',
             'role_holders', declared_summary -> 'role_holders'
         )
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(chain)
    .bind(RESOLVER)
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
async fn interpret_data_repair_redo_cascades_to_project_without_an_operator_step() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_interpret_redo_cascade").await?;
    let chain = "project-interpret-redo-cascade";
    seed_raw_registration_fixture(scratch.pool(), chain).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    sqlx::query("UPDATE name_current SET raw_name = 'tampered.eth' WHERE raw_name = 'alice.eth'")
        .execute(scratch.pool())
        .await?;

    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_project_extent(scratch.pool(), chain, 5).await?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(InterpretPhase::new(scratch.pool().clone())),
            Arc::new(ProjectPhase::new(scratch.pool().clone())),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "production-interpret-redo-cascade",
        test_timing(),
    )?;
    runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(2, 2)?,
            CancellationToken::new(),
        )
        .await?;

    let names: Vec<String> =
        sqlx::query_scalar("SELECT raw_name FROM name_current ORDER BY raw_name")
            .fetch_all(scratch.pool())
            .await?;
    assert_eq!(names, vec!["alice.eth", "eth"]);
    let project_state: (String, bool) = sqlx::query_as(
        "SELECT phase_status, redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(project_state, ("completed".into(), false));
    scratch.cleanup().await
}

#[tokio::test]
async fn reorg_below_interpret_cursor_rederives_winning_fork_through_project() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_reorg_cascade").await?;
    let chain = "project-reorg-cascade";
    seed_raw_registration_fixture(scratch.pool(), chain).await?;
    let alice_node = raw_namehash(&[b"alice", b"eth"]);
    let losing_record = TextChanged {
        node: alice_node,
        indexedKey: keccak256(b"avatar"),
        key: "avatar".into(),
        value: "ipfs://losing-fork".into(),
    }
    .encode_log_data();
    insert_raw_event_at(
        scratch.pool(),
        chain,
        5,
        1,
        1,
        RESOLVER,
        losing_record.topics(),
        losing_record.data.as_ref(),
    )
    .await?;
    publish_heads(
        scratch.pool(),
        chain,
        &HeadMarkers {
            latest: BlockMarker::new(5, block_hash(chain, 5))?,
            safe: Some(BlockMarker::new(4, block_hash(chain, 4))?),
            finalized: Some(BlockMarker::new(4, block_hash(chain, 4))?),
        },
    )
    .await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    let before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM primary_names_current WHERE namespace = 'ens'")
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(before, 1);
    let losing_record_before: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM record_inventory_current
             WHERE entries @> '[{\"record_key\":\"text:avatar\"}]'::jsonb
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(losing_record_before);

    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_project_extent(scratch.pool(), chain, 5).await?;
    let winning_hash = format!("{chain}-winning-block-5");
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, 5, to_timestamp(5), 'observed')",
    )
    .bind(chain)
    .bind(&winning_hash)
    .bind(block_hash(chain, 4))
    .execute(scratch.pool())
    .await?;
    publish_heads(
        scratch.pool(),
        chain,
        &HeadMarkers {
            latest: BlockMarker::new(5, winning_hash.clone())?,
            safe: Some(BlockMarker::new(4, block_hash(chain, 4))?),
            finalized: Some(BlockMarker::new(4, block_hash(chain, 4))?),
        },
    )
    .await?;

    let runner = PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(InterpretPhase::new(scratch.pool().clone())),
            Arc::new(ProjectPhase::new(scratch.pool().clone())),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "production-reorg-cascade",
        test_timing(),
    )?;
    let terminal = runner
        .run_chain(&chain_config(chain)?, CancellationToken::new())
        .await
        .expect_err("the intentionally unavailable verify/live slot stops after re-derivation");
    assert_eq!(
        terminal.kind(),
        phase_runner::error::ErrorKind::Configuration
    );

    let readable_reverse: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'ReverseChanged'
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(readable_reverse, 0);
    let projected: i64 =
        sqlx::query_scalar("SELECT count(*) FROM primary_names_current WHERE namespace = 'ens'")
            .fetch_one(scratch.pool())
            .await?;
    let reverse_states: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_identity, canonicality_state::text FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'ReverseChanged'",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    let primary_rows: Vec<(String, Value)> = sqlx::query_as(
        "SELECT address, claim_provenance FROM primary_names_current WHERE namespace = 'ens'",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        projected, 0,
        "reverse events: {reverse_states:?}; primary rows: {primary_rows:?}"
    );
    let losing_record_after: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM record_inventory_current
             WHERE entries @> '[{\"record_key\":\"text:avatar\"}]'::jsonb
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        !losing_record_after,
        "record inventory retained a normalized event deleted by interpret redo"
    );
    let states: Vec<(String, String, bool, Option<String>)> = sqlx::query_as(
        "SELECT phase_name, phase_status, redo_in_progress, current_block_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        states,
        vec![
            (
                "interpret".into(),
                "completed".into(),
                false,
                Some(winning_hash.clone()),
            ),
            (
                "project".into(),
                "completed".into(),
                false,
                Some(winning_hash),
            ),
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_retains_permission_only_resolver_with_a_surviving_grant() -> Result<()> {
    const RESOURCE_A: &str = "00000000-0000-0000-0000-0000000000d0";
    const RESOURCE_B: &str = "00000000-0000-0000-0000-0000000000d1";
    const SHARED_RESOLVER: &str = "0x00000000000000000000000000000000000000c9";
    const RETRACTED_RESOLVER: &str = "0x00000000000000000000000000000000000000ca";
    const UPGRADED_RESOLVER: &str = "0x00000000000000000000000000000000000000cb";

    let incremental =
        ScratchDatabase::create("production_project_redo_permission_resolver").await?;
    let full = ScratchDatabase::create("production_project_redo_permission_resolver_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        insert_manifest(
            pool,
            CHAIN,
            "ens_v2_resolver_l1",
            "tests/project-redo-v2-resolver.toml",
            json!({
                "resolver_implementations":[{
                    "role":"permissioned_resolver",
                    "address":EQUIVALENCE_V2_IMPLEMENTATION
                }]
            }),
        )
        .await?;
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES
                 ($1, $3, $4, 1, 'canonical'),
                 ($2, $3, $5, 2, 'canonical')",
        )
        .bind(Uuid::parse_str(RESOURCE_A)?)
        .bind(Uuid::parse_str(RESOURCE_B)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .bind(block_hash(CHAIN, 2))
        .execute(pool)
        .await?;
        for (block, resource, resolver, source_family) in [
            (1, RESOURCE_A, SHARED_RESOLVER, "ens_v1_registrar_l1"),
            (1, RESOURCE_A, UPGRADED_RESOLVER, "ens_v2_resolver_l1"),
            (2, RESOURCE_B, SHARED_RESOLVER, "ens_v1_registrar_l1"),
            (2, RESOURCE_B, RETRACTED_RESOLVER, "ens_v1_registrar_l1"),
        ] {
            insert_event(
                pool,
                CHAIN,
                block,
                None,
                Some(resource),
                "PermissionChanged",
                source_family,
                json!({
                    "subject":OWNER,
                    "scope":{
                        "kind":"resolver",
                        "chain_id":CHAIN,
                        "resolver_address":resolver
                    },
                    "effective_powers":["resolver_control"],
                    "grant_source":{"kind":"fixture"},
                    "revocation_source":null,
                    "inheritance_path":[],
                    "transfer_behavior":"retain"
                }),
                json!({}),
            )
            .await?;
        }
        insert_event(
            pool,
            CHAIN,
            2,
            None,
            None,
            "Upgraded",
            "ens_v2_resolver_l1",
            json!({
                "proxy_address":UPGRADED_RESOLVER,
                "implementation":EQUIVALENCE_V2_IMPLEMENTATION
            }),
            json!({"emitting_address":UPGRADED_RESOLVER}),
        )
        .await?;
        run_project(pool, CHAIN, None, RunMode::Normal, 0, 3).await?;

        let initial: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT resolver_address, unsupported_reason
             FROM resolver_current
             WHERE chain_id = $1 AND resolver_address IN (lower($2), lower($3))
             ORDER BY resolver_address",
        )
        .bind(CHAIN)
        .bind(SHARED_RESOLVER)
        .bind(RETRACTED_RESOLVER)
        .fetch_all(pool)
        .await?;
        assert_eq!(
            initial,
            vec![
                (SHARED_RESOLVER.into(), Some("resolver_not_declared".into())),
                (
                    RETRACTED_RESOLVER.into(),
                    Some("resolver_not_declared".into())
                ),
            ]
        );
        let initial_upgrade: (String, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT support_status, unsupported_reason,
                    declared_summary #>> '{classification,role}'
             FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)",
        )
        .bind(CHAIN)
        .bind(UPGRADED_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            initial_upgrade,
            (
                "supported".into(),
                None,
                Some("permissioned_resolver".into())
            )
        );

        capture_resolver_redo_evidence(pool, CHAIN, 2, 2).await?;
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND resource_id = $2
               AND event_kind = 'PermissionChanged'",
        )
        .bind(CHAIN)
        .bind(Uuid::parse_str(RESOURCE_B)?)
        .execute(pool)
        .await?;
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND event_kind = 'Upgraded'
               AND lower(after_state ->> 'proxy_address') = lower($2)",
        )
        .bind(CHAIN)
        .bind(UPGRADED_RESOLVER)
        .execute(pool)
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    let incremental_resolvers: Vec<String> = sqlx::query_scalar(
        "SELECT resolver_address FROM resolver_current
         WHERE chain_id = $1 AND resolver_address IN (lower($2), lower($3))
         ORDER BY resolver_address",
    )
    .bind(CHAIN)
    .bind(SHARED_RESOLVER)
    .bind(RETRACTED_RESOLVER)
    .fetch_all(incremental.pool())
    .await?;
    let full_resolvers: Vec<String> = sqlx::query_scalar(
        "SELECT resolver_address FROM resolver_current
         WHERE chain_id = $1 AND resolver_address IN (lower($2), lower($3))
         ORDER BY resolver_address",
    )
    .bind(CHAIN)
    .bind(SHARED_RESOLVER)
    .bind(RETRACTED_RESOLVER)
    .fetch_all(full.pool())
    .await?;
    assert_eq!(full_resolvers, vec![SHARED_RESOLVER.to_owned()]);
    assert_eq!(
        incremental_resolvers, full_resolvers,
        "redo deleted a permission-only resolver with a surviving retained grant"
    );

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(full.pool()).await?;
    let incremental_upgrade: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM resolver_current current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(UPGRADED_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    let full_upgrade: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM resolver_current current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(UPGRADED_RESOLVER)
    .fetch_one(full.pool())
    .await?;
    assert_eq!(
        full_upgrade.pointer("/declared_summary/classification/role"),
        None
    );
    assert_eq!(
        incremental_upgrade, full_upgrade,
        "redo retained resolver classification metadata from retracted upgrade evidence"
    );
    let incremental_survivors: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM resolver_current current
         WHERE chain_id = $1 AND resolver_address IN (lower($2), lower($3))
         ORDER BY resolver_address",
    )
    .bind(CHAIN)
    .bind(SHARED_RESOLVER)
    .bind(RETRACTED_RESOLVER)
    .fetch_all(incremental.pool())
    .await?;
    let full_survivors: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM resolver_current current
         WHERE chain_id = $1 AND resolver_address IN (lower($2), lower($3))
         ORDER BY resolver_address",
    )
    .bind(CHAIN)
    .bind(SHARED_RESOLVER)
    .bind(RETRACTED_RESOLVER)
    .fetch_all(full.pool())
    .await?;
    assert_eq!(
        incremental_survivors, full_survivors,
        "redo survivor rows did not match a full rebuild"
    );

    for pool in [incremental.pool(), full.pool()] {
        let surviving_permission_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM permissions_current
             WHERE resource_id = $1 AND scope_kind = 'resolver'
               AND lower(scope_detail ->> 'resolver_address') = lower($2)",
        )
        .bind(Uuid::parse_str(RESOURCE_A)?)
        .bind(SHARED_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(surviving_permission_count, 1);
    }

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn project_redo_rederives_permission_only_resolver_family_from_surviving_grant() -> Result<()>
{
    const RESOURCE_A: &str = "00000000-0000-0000-0000-0000000000d2";
    const RESOLVER_ADDRESS: &str = "0x00000000000000000000000000000000000000cc";

    let incremental = ScratchDatabase::create("production_project_redo_permission_family").await?;
    let full = ScratchDatabase::create("production_project_redo_permission_family_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        insert_manifest(
            pool,
            CHAIN,
            "ens_v2_resolver_l1",
            "tests/project-redo-family-v2-resolver.toml",
            json!({
                "resolver_implementations":[{
                    "role":"permissioned_resolver",
                    "address":EQUIVALENCE_V2_IMPLEMENTATION
                }]
            }),
        )
        .await?;
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, $2, $3, 1, 'canonical')",
        )
        .bind(Uuid::parse_str(RESOURCE_A)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            None,
            Some(RESOURCE_A),
            "PermissionChanged",
            "ens_v1_registrar_l1",
            json!({
                "subject":OWNER,
                "scope":{
                    "kind":"resolver",
                    "chain_id":CHAIN,
                    "resolver_address":RESOLVER_ADDRESS
                },
                "effective_powers":["resolver_control"],
                "grant_source":{"kind":"fixture"},
                "revocation_source":null,
                "inheritance_path":[],
                "transfer_behavior":"retain"
            }),
            json!({}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            2,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "PermissionChanged",
            "ens_v2_resolver_l1",
            json!({
                "subject":OWNER,
                "scope":{
                    "kind":"resolver",
                    "chain_id":CHAIN,
                    "resolver_address":RESOLVER_ADDRESS
                },
                "effective_powers":["resolver_control"],
                "grant_source":{"kind":"fixture"},
                "revocation_source":null,
                "inheritance_path":[],
                "transfer_behavior":"retain"
            }),
            json!({}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            2,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "ResolverChanged",
            "ens_v2_registry_l1",
            json!({"resolver":RESOLVER_ADDRESS}),
            json!({}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let initial_family: String = sqlx::query_scalar(
        "SELECT declared_summary #>> '{classification,source_family}'
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER_ADDRESS)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(initial_family, "ens_v2_resolver_l1");
    let retained_families: Value = sqlx::query_scalar(
        "SELECT provenance -> 'permission_manifest_versions'
         FROM permissions_current
         WHERE resource_id = $1 AND scope_kind = 'resolver'
           AND lower(scope_detail ->> 'resolver_address') = lower($2)",
    )
    .bind(Uuid::parse_str(RESOURCE_A)?)
    .bind(RESOLVER_ADDRESS)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(retained_families.as_array().map(Vec::len), Some(1));
    assert_eq!(
        retained_families.pointer("/0/source_family"),
        Some(&json!("ens_v1_registrar_l1"))
    );
    // Force the redo fallback to use the untouched current permission's stored provenance.
    // Candidate representatives are independently covered below; this fixture isolates the
    // retained-row family mapping so its v1 branch cannot be masked by a staged citation.
    sqlx::query(
        "UPDATE resolver_current
         SET provenance = provenance - 'candidate_event_ids'
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER_ADDRESS)
    .execute(incremental.pool())
    .await?;

    for pool in [incremental.pool(), full.pool()] {
        capture_resolver_redo_evidence(pool, CHAIN, 2, 2).await?;
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND resource_id = $2
               AND event_kind = 'ResolverChanged'
               AND lower(after_state ->> 'resolver') = lower($3)",
        )
        .bind(CHAIN)
        .bind(Uuid::parse_str(RESOURCE)?)
        .bind(RESOLVER_ADDRESS)
        .execute(pool)
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(full.pool()).await?;
    let incremental_resolver: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM resolver_current current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER_ADDRESS)
    .fetch_one(incremental.pool())
    .await?;
    let full_resolver: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM resolver_current current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER_ADDRESS)
    .fetch_one(full.pool())
    .await?;
    assert_eq!(
        full_resolver.pointer("/declared_summary/classification/source_family"),
        Some(&json!("ens_v1_resolver_l1"))
    );
    assert_eq!(
        full_resolver.pointer("/declared_summary/classification/role"),
        None
    );
    assert_eq!(
        incremental_resolver, full_resolver,
        "redo fallback did not rederive the resolver family from surviving permission evidence"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn redo_retracting_min_family_pointer_matches_fresh_and_warm_rebuild() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_binding_family_redo").await?;
    let warm = ScratchDatabase::create("production_project_binding_family_warm").await?;
    let fresh = ScratchDatabase::create("production_project_binding_family_fresh").await?;
    for pool in [incremental.pool(), warm.pool(), fresh.pool()] {
        seed_cross_family_binding_fixture(pool).await?;
    }
    for pool in [incremental.pool(), warm.pool()] {
        run_project(pool, CHAIN, None, RunMode::Normal, 0, 3).await?;
        let initial: (String, String) = sqlx::query_as(
            "SELECT declared_summary #>> '{classification,source_family}', support_status
             FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)",
        )
        .bind(CHAIN)
        .bind(RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            initial,
            ("basenames_base_resolver".into(), "unsupported".into())
        );
    }
    for pool in [incremental.pool(), warm.pool(), fresh.pool()] {
        capture_resolver_redo_evidence(pool, CHAIN, 3, 3).await?;
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND logical_name_id = $2
               AND event_kind = 'ResolverChanged'",
        )
        .bind(CHAIN)
        .bind(FAMILY_BINDING_NAME)
        .execute(pool)
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        3,
        3,
    )
    .await?;
    run_project(warm.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    for pool in [incremental.pool(), warm.pool(), fresh.pool()] {
        normalize_projection_clocks(pool).await?;
    }
    let incremental_row = resolver_projection_row(incremental.pool(), RESOLVER).await?;
    let warm_row = resolver_projection_row(warm.pool(), RESOLVER).await?;
    let fresh_row = resolver_projection_row(fresh.pool(), RESOLVER).await?;
    assert_eq!(
        fresh_row
            .as_ref()
            .and_then(|row| row.pointer("/declared_summary/classification/source_family")),
        Some(&json!("ens_v1_resolver_l1"))
    );
    assert_eq!(
        fresh_row.as_ref().and_then(|row| row.get("support_status")),
        Some(&json!("supported"))
    );
    assert_eq!(
        (incremental_row, warm_row),
        (fresh_row.clone(), fresh_row),
        "redo and a warm rebuild retained the retracted pointer's prior resolver family"
    );

    incremental.cleanup().await?;
    warm.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn normal_pointer_move_rederives_retained_binding_family() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_binding_family_tick").await?;
    let fresh = ScratchDatabase::create("production_project_binding_family_tick_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_cross_family_binding_fixture(pool).await?;
        insert_lineage_block(pool, CHAIN, 4).await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    for pool in [incremental.pool(), fresh.pool()] {
        insert_event(
            pool,
            CHAIN,
            4,
            Some(FAMILY_BINDING_NAME),
            Some(FAMILY_BINDING_RESOURCE),
            "ResolverChanged",
            "basenames_base_registry",
            json!({"resolver":"0x0000000000000000000000000000000000000000"}),
            json!({}),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events
             SET before_state = jsonb_build_object('resolver', lower($1))
             WHERE chain_id = $2 AND block_number = 4
               AND logical_name_id = $3 AND event_kind = 'ResolverChanged'",
        )
        .bind(RESOLVER)
        .bind(CHAIN)
        .bind(FAMILY_BINDING_NAME)
        .execute(pool)
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    let survivor: (String, String, Option<String>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason,
                provenance ->> 'resolver_pointer_source_family'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(FAMILY_SURVIVOR_NAME)
    .fetch_one(fresh.pool())
    .await?;
    assert_eq!(
        survivor,
        (
            "unsupported".into(),
            "current_authority_not_projected".into(),
            Some("ens_v1_registry_l1".into()),
        ),
        "a bindingless resolver pointer is classification evidence, not projected authority"
    );
    let retained_target: String = sqlx::query_scalar(
        "SELECT chain_positions #>> ARRAY[$1, 'block_number']
         FROM name_current WHERE logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(FAMILY_SURVIVOR_NAME)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(
        retained_target, "4",
        "address-level resolver classification changes must rebuild every current pointer"
    );
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    let incremental_row = resolver_projection_row(incremental.pool(), RESOLVER).await?;
    let fresh_row = resolver_projection_row(fresh.pool(), RESOLVER).await?;
    assert_eq!(
        fresh_row
            .as_ref()
            .and_then(|row| row.pointer("/declared_summary/classification/source_family")),
        Some(&json!("ens_v1_resolver_l1"))
    );
    assert_eq!(
        incremental_row, fresh_row,
        "a normal tick used pre-swap resolver metadata for an untouched surviving binding"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn manifest_rotation_keeps_resolver_from_fully_revoked_permission_history() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_permission_history_rotation").await?;
    let fresh =
        ScratchDatabase::create("production_project_permission_history_rotation_fresh").await?;
    let incremental_manifest = seed_permission_history_fixture(incremental.pool(), false).await?;
    let fresh_manifest = seed_permission_history_fixture(fresh.pool(), false).await?;
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    assert!(
        resolver_projection_row(incremental.pool(), HISTORY_RESOLVER)
            .await?
            .is_some()
    );
    assert_eq!(
        permission_rows_for_resolver(incremental.pool(), HISTORY_RESOLVER).await?,
        0
    );
    rotate_history_resolver_manifest(incremental.pool(), incremental_manifest).await?;
    rotate_history_resolver_manifest(fresh.pool(), fresh_manifest).await?;

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    let incremental_row = resolver_projection_row(incremental.pool(), HISTORY_RESOLVER).await?;
    let fresh_row = resolver_projection_row(fresh.pool(), HISTORY_RESOLVER).await?;
    assert!(fresh_row.is_some());
    assert_eq!(
        incremental_row, fresh_row,
        "manifest rotation dropped a resolver supported only by fully revoked permission history"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn redo_removes_resolver_whose_revoked_permission_history_was_retracted() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_permission_history_retracted").await?;
    let fresh =
        ScratchDatabase::create("production_project_permission_history_retracted_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_permission_history_fixture(pool, false).await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    assert!(
        resolver_projection_row(incremental.pool(), HISTORY_RESOLVER)
            .await?
            .is_some()
    );
    for pool in [incremental.pool(), fresh.pool()] {
        capture_resolver_redo_evidence(pool, CHAIN, 1, 2).await?;
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND resource_id = $2
               AND event_kind = 'PermissionChanged'",
        )
        .bind(CHAIN)
        .bind(Uuid::parse_str(HISTORY_REVOKED_RESOURCE)?)
        .execute(pool)
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        1,
        2,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    assert_eq!(
        resolver_projection_row(incremental.pool(), HISTORY_RESOLVER).await?,
        resolver_projection_row(fresh.pool(), HISTORY_RESOLVER).await?,
        "redo retained a resolver after its fully revoked permission history disappeared"
    );
    assert!(
        resolver_projection_row(fresh.pool(), HISTORY_RESOLVER)
            .await?
            .is_none()
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn redo_restores_a_permission_partition_when_its_revoke_is_retracted() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_permission_partition_retracted").await?;
    let fresh =
        ScratchDatabase::create("production_project_permission_partition_retracted_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_permission_history_fixture(pool, false).await?;
        insert_event(
            pool,
            CHAIN,
            3,
            None,
            Some(HISTORY_REVOKED_RESOURCE),
            "PermissionChanged",
            "ens_v1_registrar_l1",
            json!({
                "subject":TRANSFER_OWNER,
                "scope":{
                    "kind":"resolver",
                    "chain_id":CHAIN,
                    "resolver_address":HISTORY_RESOLVER
                },
                "effective_powers":["resolver_control"],
                "grant_source":{"kind":"fixture"},
                "revocation_source":null,
                "inheritance_path":[],
                "transfer_behavior":"retain"
            }),
            json!({}),
        )
        .await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    for pool in [incremental.pool(), fresh.pool()] {
        capture_resolver_redo_evidence(pool, CHAIN, 2, 2).await?;
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND resource_id = $2
               AND event_kind = 'PermissionChanged' AND block_number = 2",
        )
        .bind(CHAIN)
        .bind(Uuid::parse_str(HISTORY_REVOKED_RESOURCE)?)
        .execute(pool)
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let incremental_subjects: Vec<String> = sqlx::query_scalar(
        "SELECT subject FROM permissions_current
         WHERE resource_id = $1 AND scope_kind = 'resolver'
         ORDER BY subject",
    )
    .bind(Uuid::parse_str(HISTORY_REVOKED_RESOURCE)?)
    .fetch_all(incremental.pool())
    .await?;
    let fresh_subjects: Vec<String> = sqlx::query_scalar(
        "SELECT subject FROM permissions_current
         WHERE resource_id = $1 AND scope_kind = 'resolver'
         ORDER BY subject",
    )
    .bind(Uuid::parse_str(HISTORY_REVOKED_RESOURCE)?)
    .fetch_all(fresh.pool())
    .await?;
    assert_eq!(fresh_subjects, vec![OWNER, TRANSFER_OWNER]);
    assert_eq!(
        incremental_subjects, fresh_subjects,
        "redo missed a retracted revoke outside the resolver's representative family event"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn redo_removes_resolver_created_only_by_an_unlinked_pointer_event() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_unlinked_resolver_retracted").await?;
    let fresh =
        ScratchDatabase::create("production_project_unlinked_resolver_retracted_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_project_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            2,
            None,
            None,
            "ResolverChanged",
            "ens_v1_registry_l1",
            json!({"resolver":UNLINKED_RESOLVER}),
            json!({}),
        )
        .await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    assert!(
        resolver_projection_row(incremental.pool(), UNLINKED_RESOLVER)
            .await?
            .is_some()
    );
    for pool in [incremental.pool(), fresh.pool()] {
        capture_resolver_redo_evidence(pool, CHAIN, 2, 2).await?;
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND event_kind = 'ResolverChanged'
               AND after_state ->> 'resolver' = lower($2)",
        )
        .bind(CHAIN)
        .bind(UNLINKED_RESOLVER)
        .execute(pool)
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    assert_eq!(
        resolver_projection_row(incremental.pool(), UNLINKED_RESOLVER).await?,
        resolver_projection_row(fresh.pool(), UNLINKED_RESOLVER).await?,
        "redo retained a resolver after its only unlinked pointer event disappeared"
    );
    assert!(
        resolver_projection_row(fresh.pool(), UNLINKED_RESOLVER)
            .await?
            .is_none()
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn manifest_rotation_retains_an_unlinked_pointer_candidate() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_unlinked_resolver_rotation").await?;
    let fresh =
        ScratchDatabase::create("production_project_unlinked_resolver_rotation_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_project_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            2,
            None,
            None,
            "ResolverChanged",
            "ens_v1_registry_l1",
            json!({"resolver":UNLINKED_RESOLVER}),
            json!({}),
        )
        .await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    assert!(
        resolver_projection_row(incremental.pool(), UNLINKED_RESOLVER)
            .await?
            .is_some()
    );
    for pool in [incremental.pool(), fresh.pool()] {
        let manifest_id: i64 = sqlx::query_scalar(
            "SELECT manifest_id FROM manifest_versions
             WHERE chain_id = $1 AND source_family = 'ens_v1_resolver_l1'",
        )
        .bind(CHAIN)
        .fetch_one(pool)
        .await?;
        insert_manifest_update_event(
            pool,
            CHAIN,
            "ens_v1_resolver_l1",
            manifest_id,
            json!({
                "contracts":[{
                    "role":"public_resolver",
                    "address":RESOLVER,
                    "proxy_kind":"none"
                }]
            }),
        )
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    let fresh_row = resolver_projection_row(fresh.pool(), UNLINKED_RESOLVER).await?;
    assert!(fresh_row.is_some());
    assert_eq!(
        resolver_projection_row(incremental.pool(), UNLINKED_RESOLVER).await?,
        fresh_row,
        "manifest rotation dropped an unlinked historical pointer candidate"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn manifest_rotation_uses_revoked_and_live_permission_families() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_permission_history_family").await?;
    let fresh =
        ScratchDatabase::create("production_project_permission_history_family_fresh").await?;
    let incremental_manifest = seed_permission_history_fixture(incremental.pool(), true).await?;
    let fresh_manifest = seed_permission_history_fixture(fresh.pool(), true).await?;
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    assert_eq!(
        permission_rows_for_resolver(incremental.pool(), HISTORY_RESOLVER).await?,
        1
    );
    rotate_history_resolver_manifest(incremental.pool(), incremental_manifest).await?;
    rotate_history_resolver_manifest(fresh.pool(), fresh_manifest).await?;

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    let incremental_row = resolver_projection_row(incremental.pool(), HISTORY_RESOLVER).await?;
    let fresh_row = resolver_projection_row(fresh.pool(), HISTORY_RESOLVER).await?;
    assert_eq!(
        fresh_row
            .as_ref()
            .and_then(|row| row.pointer("/declared_summary/classification/source_family")),
        Some(&json!("ens_v1_resolver_l1"))
    );
    assert_eq!(
        incremental_row, fresh_row,
        "manifest rotation ignored a revoked lower-family permission partition"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn late_permission_resolver_scope_closes_revoked_family_history() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_permission_history_late_scope").await?;
    let fresh =
        ScratchDatabase::create("production_project_permission_history_late_scope_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_permission_history_fixture(pool, true).await?;
        insert_lineage_block(pool, CHAIN, 4).await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    assert_eq!(
        resolver_projection_row(incremental.pool(), HISTORY_RESOLVER)
            .await?
            .as_ref()
            .and_then(|row| row.pointer("/declared_summary/classification/source_family")),
        Some(&json!("ens_v1_resolver_l1"))
    );
    for pool in [incremental.pool(), fresh.pool()] {
        insert_event(
            pool,
            CHAIN,
            4,
            None,
            Some(HISTORY_LIVE_RESOURCE),
            "RecordChanged",
            "ens_v2_resolver_l1",
            json!({
                "resolver":V2_INVERSE_RESOLVER,
                "record_key":"text:late-scope",
                "record_family":"text",
                "selector_key":"late-scope",
                "value_retained":true,
                "value":"changed"
            }),
            json!({"emitting_address":V2_INVERSE_RESOLVER}),
        )
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    let incremental_row = resolver_projection_row(incremental.pool(), HISTORY_RESOLVER).await?;
    let fresh_row = resolver_projection_row(fresh.pool(), HISTORY_RESOLVER).await?;
    assert_eq!(
        fresh_row
            .as_ref()
            .and_then(|row| row.pointer("/declared_summary/classification/source_family")),
        Some(&json!("ens_v1_resolver_l1"))
    );
    assert_eq!(
        incremental_row, fresh_row,
        "a resolver discovered from a scoped resource missed another resource's revoked family"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn record_only_resolver_with_permission_history_keeps_passthrough() -> Result<()> {
    let scratch =
        ScratchDatabase::create("production_project_permission_history_passthrough").await?;
    seed_permission_history_fixture(scratch.pool(), false).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query(
        "UPDATE resolver_current
         SET declared_summary = declared_summary || '{\"passthrough_guard\":true}'::jsonb
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(HISTORY_RESOLVER)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":HISTORY_RESOLVER,
            "record_key":"text:passthrough",
            "record_family":"text",
            "selector_key":"passthrough",
            "value_retained":true,
            "value":"changed"
        }),
        json!({"emitting_address":HISTORY_RESOLVER}),
    )
    .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let kept_passthrough: bool = sqlx::query_scalar(
        "SELECT declared_summary @> '{\"passthrough_guard\":true}'::jsonb
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(HISTORY_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        kept_passthrough,
        "record-only resolver scope expanded unrelated permission history"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn empty_window_crossing_declaration_boundary_matches_fresh_projection() -> Result<()> {
    assert_declaration_boundary_converges(false).await
}

#[tokio::test]
async fn record_only_window_crossing_declaration_boundary_matches_fresh_projection() -> Result<()> {
    assert_declaration_boundary_converges(true).await
}

async fn assert_declaration_boundary_converges(record_only: bool) -> Result<()> {
    let suffix = if record_only { "record" } else { "empty" };
    let incremental =
        ScratchDatabase::create(&format!("production_project_declaration_boundary_{suffix}"))
            .await?;
    let fresh = ScratchDatabase::create(&format!(
        "production_project_declaration_boundary_{suffix}_fresh"
    ))
    .await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_declaration_boundary_fixture(pool).await?;
        if record_only {
            insert_event(
                pool,
                CHAIN,
                20,
                Some("ens:0xalice"),
                Some(RESOURCE),
                "RecordChanged",
                "ens_v1_resolver_l1",
                json!({
                    "resolver":RESOLVER,
                    "record_key":"text:boundary",
                    "record_family":"text",
                    "selector_key":"boundary",
                    "value_retained":true,
                    "value":"changed"
                }),
                json!({"emitting_address":RESOLVER}),
            )
            .await?;
        }
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 19).await?;
    assert_eq!(
        resolver_projection_row(incremental.pool(), RESOLVER)
            .await?
            .as_ref()
            .and_then(|row| row.pointer("/declared_summary/classification/role")),
        Some(&json!("old_resolver"))
    );
    if !record_only {
        let boundary_events: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM normalized_events
             WHERE chain_id = $1 AND block_number = 20",
        )
        .bind(CHAIN)
        .fetch_one(incremental.pool())
        .await?;
        assert_eq!(
            boundary_events, 0,
            "the empty boundary window gained an event"
        );
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 19,
            hash: block_hash(CHAIN, 19),
        }),
        RunMode::Normal,
        20,
        20,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 20).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    let incremental_row = resolver_projection_row(incremental.pool(), RESOLVER).await?;
    let fresh_row = resolver_projection_row(fresh.pool(), RESOLVER).await?;
    assert_eq!(
        fresh_row
            .as_ref()
            .and_then(|row| row.pointer("/declared_summary/classification/role")),
        Some(&json!("new_resolver"))
    );
    assert_eq!(
        incremental_row, fresh_row,
        "{suffix} window failed to reclassify at the declaration boundary"
    );
    if !record_only {
        run_project(
            incremental.pool(),
            CHAIN,
            Some(Marker {
                number: 20,
                hash: block_hash(CHAIN, 20),
            }),
            RunMode::Normal,
            21,
            21,
        )
        .await?;
        let resolver_stayed_quiet: bool = sqlx::query_scalar(
            "SELECT last_recomputed_at = to_timestamp(0)
             FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)",
        )
        .bind(CHAIN)
        .bind(RESOLVER)
        .fetch_one(incremental.pool())
        .await?;
        assert!(
            resolver_stayed_quiet,
            "a batch without a declaration boundary rebuilt the resolver"
        );
    }

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn record_only_resource_keeps_unrelated_permission_resolver_passthrough() -> Result<()> {
    let scratch =
        ScratchDatabase::create("production_project_unrelated_permission_history_passthrough")
            .await?;
    seed_permission_history_fixture(scratch.pool(), true).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query(
        "UPDATE resolver_current
         SET declared_summary = declared_summary || '{\"passthrough_guard\":true}'::jsonb
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(HISTORY_RESOLVER)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        None,
        Some(HISTORY_LIVE_RESOURCE),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:unrelated-fan-in",
            "record_family":"text",
            "selector_key":"unrelated-fan-in",
            "value_retained":true,
            "value":"changed"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let kept_passthrough: bool = sqlx::query_scalar(
        "SELECT declared_summary @> '{\"passthrough_guard\":true}'::jsonb
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(HISTORY_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        kept_passthrough,
        "record-only scope rebuilt a resolver named only by unrelated permission history"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn resource_permission_emitter_keeps_existing_pointer_resolver_passthrough() -> Result<()> {
    let scratch =
        ScratchDatabase::create("production_project_resource_permission_emitter_passthrough")
            .await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query(
        "UPDATE resolver_current
         SET declared_summary = declared_summary || '{\"passthrough_guard\":true}'::jsonb
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_resolver_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"retain"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let kept_passthrough: bool = sqlx::query_scalar(
        "SELECT declared_summary @> '{\"passthrough_guard\":true}'::jsonb
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        kept_passthrough,
        "resource-scoped permission emitter rebuilt the pointer resolver"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn window_alias_change_denies_resolver_passthrough() -> Result<()> {
    assert_window_resolver_event_denies_passthrough("alias").await
}

#[tokio::test]
async fn window_resolver_change_denies_resolver_passthrough() -> Result<()> {
    assert_window_resolver_event_denies_passthrough("resolver").await
}

async fn assert_window_resolver_event_denies_passthrough(event_kind: &str) -> Result<()> {
    let incremental = ScratchDatabase::create(&format!(
        "production_project_{event_kind}_denies_passthrough"
    ))
    .await?;
    let fresh = ScratchDatabase::create(&format!(
        "production_project_{event_kind}_denies_passthrough_fresh"
    ))
    .await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_project_fixture(pool).await?;
        insert_lineage_block(pool, CHAIN, 4).await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query(
        "UPDATE resolver_current
         SET declared_summary = declared_summary || '{\"passthrough_guard\":true}'::jsonb
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .execute(incremental.pool())
    .await?;

    for pool in [incremental.pool(), fresh.pool()] {
        match event_kind {
            "alias" => {
                insert_event(
                    pool,
                    CHAIN,
                    4,
                    Some("ens:0xalice"),
                    Some(RESOURCE),
                    "AliasChanged",
                    "ens_v2_resolver_l1",
                    json!({
                        "resolver":RESOLVER,
                        "active":true,
                        "alias_state":"active",
                        "from_name":"alice.eth",
                        "from_namehash":"0xalice",
                        "to_name":"profile.alice.eth",
                        "to_namehash":"0xprofile-alice",
                        "to_logical_name_id":"ens:0xprofile-alice",
                        "to_resource_id":RESOURCE
                    }),
                    json!({"emitting_address":RESOLVER}),
                )
                .await?;
                let alias_family: String = sqlx::query_scalar(
                    "SELECT source_family FROM normalized_events
                     WHERE chain_id = $1 AND block_number = 4
                       AND event_kind = 'AliasChanged'",
                )
                .bind(CHAIN)
                .fetch_one(pool)
                .await?;
                assert_eq!(
                    alias_family, "ens_v2_resolver_l1",
                    "alias denial must exercise the admitted resolver family"
                );
            }
            "resolver" => {
                insert_event(
                    pool,
                    CHAIN,
                    4,
                    Some("ens:0xalice"),
                    Some(RESOURCE),
                    "ResolverChanged",
                    "ens_v1_registry_l1",
                    json!({"resolver":RESOLVER}),
                    json!({}),
                )
                .await?;
            }
            unexpected => panic!("unexpected resolver event kind {unexpected}"),
        }
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    let incremental_row = resolver_projection_row(incremental.pool(), RESOLVER).await?;
    let fresh_row = resolver_projection_row(fresh.pool(), RESOLVER).await?;
    assert!(
        fresh_row
            .as_ref()
            .and_then(|row| row.pointer("/declared_summary/passthrough_guard"))
            .is_none()
    );
    assert_eq!(
        incremental_row, fresh_row,
        "window {event_kind} event incorrectly carried the old resolver summary through"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn record_only_shared_resolver_does_not_republish_permission_history_resources() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_project_record_only_shared_resolver").await?;
    seed_permission_history_fixture(scratch.pool(), true).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query(
        "UPDATE resolver_current
         SET declared_summary = declared_summary || '{\"passthrough_guard\":true}'::jsonb
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(HISTORY_RESOLVER)
    .execute(scratch.pool())
    .await?;
    let untouched_before: Value = sqlx::query_scalar(
        "SELECT to_jsonb(summary) - 'last_recomputed_at'
         FROM permissions_current_resource_summary summary
         WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(HISTORY_REVOKED_RESOURCE)?)
    .fetch_one(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        None,
        Some(HISTORY_LIVE_RESOURCE),
        "RecordChanged",
        "ens_v2_resolver_l1",
        json!({
            "resolver":HISTORY_RESOLVER,
            "record_key":"text:fan-in",
            "record_family":"text",
            "selector_key":"fan-in",
            "value_retained":true,
            "value":"changed"
        }),
        json!({"emitting_address":HISTORY_RESOLVER}),
    )
    .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let kept_passthrough: bool = sqlx::query_scalar(
        "SELECT declared_summary @> '{\"passthrough_guard\":true}'::jsonb
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(HISTORY_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        kept_passthrough,
        "record-only scope rebuilt the shared resolver"
    );
    let untouched_after: Value = sqlx::query_scalar(
        "SELECT to_jsonb(summary) - 'last_recomputed_at'
         FROM permissions_current_resource_summary summary
         WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(HISTORY_REVOKED_RESOURCE)?)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        untouched_after, untouched_before,
        "resolver candidate fan-in republished an input-only resource"
    );

    scratch.cleanup().await
}

#[tokio::test]
#[ignore = "100k-partition Project cost diagnostic"]
async fn shared_resolver_100k_permission_fan_in_stays_bounded() -> Result<()> {
    const PARTITIONS: i64 = 100_000;
    let scratch = ScratchDatabase::create("production_project_100k_resolver_fan_in").await?;
    seed_project_fixture(scratch.pool()).await?;
    for block in [4, 5] {
        insert_lineage_block(scratch.pool(), CHAIN, block).await?;
    }
    sqlx::query(
        r#"WITH generated AS (
               SELECT value,
                      (substr(digest, 1, 8) || '-' || substr(digest, 9, 4) || '-' ||
                       substr(digest, 13, 4) || '-' || substr(digest, 17, 4) || '-' ||
                       substr(digest, 21, 12))::uuid AS resource_id
               FROM (
                   SELECT value, md5('resolver-fan-in-' || value::text) AS digest
                   FROM generate_series(1, $1) value
               ) values
           )
           INSERT INTO resources (
               resource_id, chain_id, block_hash, block_number, canonicality_state
           )
           SELECT resource_id, $2, $3, 1, 'canonical' FROM generated"#,
    )
    .bind(PARTITIONS)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        r#"WITH manifest AS (
               SELECT manifest_id, manifest_version
               FROM manifest_versions
               WHERE chain_id = $2 AND source_family = 'ens_v1_registrar_l1'
               ORDER BY manifest_version DESC LIMIT 1
           ), generated AS (
               SELECT value,
                      (substr(digest, 1, 8) || '-' || substr(digest, 9, 4) || '-' ||
                       substr(digest, 13, 4) || '-' || substr(digest, 17, 4) || '-' ||
                       substr(digest, 21, 12))::uuid AS resource_id
               FROM (
                   SELECT value, md5('resolver-fan-in-' || value::text) AS digest
                   FROM generate_series(1, $1) value
               ) values
           )
           INSERT INTO normalized_events (
               event_identity, namespace, resource_id, event_kind, source_family,
               manifest_version, source_manifest_id, chain_id, block_number, block_hash,
               transaction_hash, transaction_index, log_index, raw_fact_ref,
               derivation_kind, canonicality_state, before_state, after_state,
               consumer_visibility
           )
           SELECT 'resolver-fan-in-permission-' || generated.value,
                  'ens', generated.resource_id, 'PermissionChanged',
                  'ens_v1_registrar_l1', manifest.manifest_version, manifest.manifest_id,
                  $2, 1, $3, 'resolver-fan-in-tx-' || generated.value, 0,
                  generated.value, '{}'::jsonb, 'ens_v1_unwrapped_authority',
                  'canonical', '{}'::jsonb,
                  jsonb_build_object(
                      'subject', '0x' || lpad(to_hex(generated.value), 40, '0'),
                      'scope', jsonb_build_object(
                          'kind', 'resolver', 'chain_id', $2,
                          'resolver_address', lower($4)
                      ),
                      'effective_powers', jsonb_build_array('resolver_control'),
                      'grant_source', jsonb_build_object('kind', 'fixture'),
                      'revocation_source', NULL,
                      'inheritance_path', '[]'::jsonb,
                      'transfer_behavior', 'retain'
                  ), 'activated'
           FROM generated CROSS JOIN manifest"#,
    )
    .bind(PARTITIONS)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .bind(HISTORY_RESOLVER)
    .execute(scratch.pool())
    .await?;

    let full_started = Instant::now();
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let full_elapsed = full_started.elapsed();
    let first_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events
         WHERE event_identity = 'resolver-fan-in-permission-1'",
    )
    .fetch_one(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        None,
        Some(first_resource.to_string().as_str()),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":HISTORY_RESOLVER,
            "record_key":"text:bounded",
            "record_family":"text",
            "selector_key":"bounded",
            "value_retained":true,
            "value":"changed"
        }),
        json!({"emitting_address":HISTORY_RESOLVER}),
    )
    .await?;
    let tick_started = Instant::now();
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let tick_elapsed = tick_started.elapsed();

    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 5, 'canonical')",
    )
    .bind(Uuid::parse_str(FAMILY_PERMISSION_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 5))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        5,
        None,
        Some(FAMILY_PERMISSION_RESOURCE),
        "PermissionChanged",
        "ens_v1_registrar_l1",
        json!({
            "subject":OWNER,
            "scope":{
                "kind":"resolver", "chain_id":CHAIN,
                "resolver_address":HISTORY_RESOLVER
            },
            "effective_powers":["resolver_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"retain"
        }),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    capture_resolver_redo_evidence(scratch.pool(), CHAIN, 5, 5).await?;
    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND block_number = 5
           AND event_kind = 'PermissionChanged'",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    let redo_started = Instant::now();
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 5,
            hash: block_hash(CHAIN, 5),
        }),
        RunMode::Redo,
        5,
        5,
    )
    .await?;
    let redo_elapsed = redo_started.elapsed();
    let (citation_count, citation_bytes): (i64, i64) = sqlx::query_as(
        "SELECT jsonb_array_length(COALESCE(
                    provenance -> 'candidate_event_ids', '[]'::jsonb
                ))::bigint,
                octet_length(COALESCE(
                    provenance -> 'candidate_event_ids', '[]'::jsonb
                )::text)::bigint
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(HISTORY_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    eprintln!(
        "100k resolver fan-in: full={full_elapsed:?} tick={tick_elapsed:?} \
         redo={redo_elapsed:?} citations={citation_count} bytes={citation_bytes}"
    );
    assert!(citation_count <= 3);
    assert!(citation_bytes < 256);
    assert!(tick_elapsed < full_elapsed);
    assert!(redo_elapsed < full_elapsed);

    scratch.cleanup().await
}

#[tokio::test]
async fn redo_rebuilds_resolver_for_a_retracted_nonrepresentative_pointer() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_nonrepresentative_pointer").await?;
    let fresh =
        ScratchDatabase::create("production_project_nonrepresentative_pointer_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_project_fixture(pool).await?;
        for block in [1, 2] {
            insert_event(
                pool,
                CHAIN,
                block,
                None,
                None,
                "ResolverChanged",
                "ens_v1_registry_l1",
                json!({"resolver":UNLINKED_RESOLVER}),
                json!({}),
            )
            .await?;
        }
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query(
        "UPDATE resolver_current
         SET declared_summary = declared_summary || '{\"redo_guard\":true}'::jsonb
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(UNLINKED_RESOLVER)
    .execute(incremental.pool())
    .await?;
    for pool in [incremental.pool(), fresh.pool()] {
        capture_resolver_redo_evidence(pool, CHAIN, 2, 2).await?;
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND block_number = 2
               AND event_kind = 'ResolverChanged'
               AND after_state ->> 'resolver' = lower($2)",
        )
        .bind(CHAIN)
        .bind(UNLINKED_RESOLVER)
        .execute(pool)
        .await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        resolver_projection_row(incremental.pool(), UNLINKED_RESOLVER).await?,
        resolver_projection_row(fresh.pool(), UNLINKED_RESOLVER).await?,
        "redo ignored a removed pointer event that was not the stored family representative"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn normal_catchup_consumes_redo_evidence_above_the_prior_project_head() -> Result<()> {
    const FIRST_RESOLVER: &str = "0x00000000000000000000000000000000000000d1";
    const SECOND_RESOLVER: &str = "0x00000000000000000000000000000000000000d2";

    let scratch = ScratchDatabase::create("production_project_redo_handoff_catchup").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        None,
        None,
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":FIRST_RESOLVER}),
        json!({}),
    )
    .await?;

    // Interpret redoes through block 4 while Project is still published only through block 3.
    // The re-derived event keeps its identity but changes the resolver address.
    capture_resolver_redo_evidence(scratch.pool(), CHAIN, 4, 4).await?;
    sqlx::query(
        "INSERT INTO project_redo_expiry_roots (
             chain_id, event_identity, block_number, logical_name_id
         ) VALUES (
             $1, 'catchup-path-expiry-name', 4,
             'ens:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
         )",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET after_state = jsonb_build_object('resolver', lower($2))
         WHERE chain_id = $1 AND block_number = 4
           AND event_kind = 'ResolverChanged'",
    )
    .bind(CHAIN)
    .bind(SECOND_RESOLVER)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let second_projected: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)
         )",
    )
    .bind(CHAIN)
    .bind(SECOND_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        second_projected,
        "normal catch-up did not publish the re-derived resolver"
    );
    let expiry_handoff_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM project_redo_expiry_roots WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(
        expiry_handoff_rows, 0,
        "normal catch-up did not consume the path-expiry name handoff"
    );

    // A later redo removes that same event. Its fresh resolver address must replace the
    // first redo's handoff before Project decides which row to retract.
    capture_resolver_redo_evidence(scratch.pool(), CHAIN, 4, 4).await?;
    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND block_number = 4
           AND event_kind = 'ResolverChanged'",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Redo,
        4,
        4,
    )
    .await?;
    let second_remains: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)
         )",
    )
    .bind(CHAIN)
    .bind(SECOND_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        !second_remains,
        "stale redo handoff left the re-derived resolver published"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn redo_retracts_unlinked_alias_from_resolver_summary() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_retracted_alias").await?;
    let fresh = ScratchDatabase::create("production_project_retracted_alias_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_v2_permission_inverse_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            3,
            None,
            None,
            "AliasChanged",
            "ens_v2_resolver_l1",
            json!({
                "resolver":V2_INVERSE_RESOLVER,
                "active":true,
                "alias_state":"active",
                "from_name":"unlinked.alias.eth",
                "to_name":"target.eth"
            }),
            json!({"emitting_address":V2_INVERSE_RESOLVER}),
        )
        .await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let initial_alias_count: i64 = sqlx::query_scalar(
        "SELECT (declared_summary #>> '{aliases,count}')::bigint
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(V2_INVERSE_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(initial_alias_count, 1, "fixture must publish the alias");

    for pool in [incremental.pool(), fresh.pool()] {
        capture_resolver_redo_evidence(pool, CHAIN, 3, 3).await?;
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND block_number = 3
               AND event_kind = 'AliasChanged'
               AND raw_fact_ref ->> 'emitting_address' = lower($2)",
        )
        .bind(CHAIN)
        .bind(V2_INVERSE_RESOLVER)
        .execute(pool)
        .await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        3,
        3,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        resolver_projection_row(incremental.pool(), V2_INVERSE_RESOLVER).await?,
        resolver_projection_row(fresh.pool(), V2_INVERSE_RESOLVER).await?,
        "redo kept an alias event that disappeared from unlinked resolver history"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn resolver_candidate_citations_keep_permission_and_pointer_families_distinct() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_project_candidate_family_citations").await?;
    seed_project_fixture(scratch.pool()).await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(FAMILY_PERMISSION_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    for (block, family) in [(1, "ens_v1_registrar_l1"), (2, "ens_v2_resolver_l1")] {
        insert_event(
            scratch.pool(),
            CHAIN,
            block,
            None,
            Some(FAMILY_PERMISSION_RESOURCE),
            "PermissionChanged",
            family,
            json!({
                "subject":OWNER,
                "scope":{
                    "kind":"resolver",
                    "chain_id":CHAIN,
                    "resolver_address":FAMILY_PERMISSION_RESOLVER
                },
                "effective_powers":["resolver_control"],
                "grant_source":{"kind":"fixture"},
                "revocation_source":null,
                "inheritance_path":[],
                "transfer_behavior":"retain"
            }),
            json!({}),
        )
        .await?;
    }
    for (block, family) in [(1, "ens_v1_registry_l1"), (2, "ens_v2_registry_l1")] {
        insert_event(
            scratch.pool(),
            CHAIN,
            block,
            None,
            None,
            "ResolverChanged",
            family,
            json!({"resolver":FAMILY_POINTER_RESOLVER}),
            json!({}),
        )
        .await?;
    }
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    for resolver in [FAMILY_PERMISSION_RESOLVER, FAMILY_POINTER_RESOLVER] {
        let row = resolver_projection_row(scratch.pool(), resolver)
            .await?
            .expect("family fixture resolver is projected");
        assert_eq!(
            row.pointer("/declared_summary/classification/source_family"),
            Some(&json!("ens_v1_resolver_l1"))
        );
        assert_eq!(
            row.pointer("/provenance/candidate_event_ids")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(2),
            "candidate provenance merged two source families"
        );
    }

    scratch.cleanup().await
}

#[tokio::test]
async fn resource_scoped_permission_emitter_is_not_resolver_candidate_evidence() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_emitter_only_permission").await?;
    let fresh = ScratchDatabase::create("production_project_emitter_only_permission_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_project_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            1,
            None,
            Some(RESOURCE),
            "PermissionChanged",
            "ens_v1_resolver_l1",
            json!({
                "subject":OWNER,
                "scope":{"kind":"resource"},
                "effective_powers":["resource_control"],
                "grant_source":{"kind":"fixture"},
                "revocation_source":null,
                "inheritance_path":[],
                "transfer_behavior":"retain"
            }),
            json!({"emitting_address":EMITTER_ONLY_RESOLVER}),
        )
        .await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions
         WHERE chain_id = $1 AND source_family = 'ens_v1_resolver_l1'",
    )
    .bind(CHAIN)
    .fetch_one(incremental.pool())
    .await?;
    for pool in [incremental.pool(), fresh.pool()] {
        insert_manifest_update_event(
            pool,
            CHAIN,
            "ens_v1_resolver_l1",
            manifest_id,
            json!({"contracts":[]}),
        )
        .await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    assert_eq!(
        resolver_projection_row(incremental.pool(), EMITTER_ONLY_RESOLVER).await?,
        resolver_projection_row(fresh.pool(), EMITTER_ONLY_RESOLVER).await?,
        "emitting-address-only permission evidence diverged across incremental and full builds"
    );
    assert!(
        resolver_projection_row(fresh.pool(), EMITTER_ONLY_RESOLVER)
            .await?
            .is_none()
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn redo_drops_v2_resolver_after_its_final_permission_tie_is_retracted() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_v2_permission_inverse").await?;
    let fresh = ScratchDatabase::create("production_project_v2_permission_inverse_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_v2_permission_inverse_fixture(pool).await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let initial = resolver_projection_row(incremental.pool(), V2_INVERSE_RESOLVER)
        .await?
        .expect("the live v2 permission and upgrade create a resolver row");
    assert_eq!(initial["support_status"], "supported");
    assert_eq!(
        initial.pointer("/declared_summary/permissions/count"),
        Some(&json!(1))
    );
    for pool in [incremental.pool(), fresh.pool()] {
        capture_resolver_redo_evidence(pool, CHAIN, 1, 2).await?;
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND (
                 resource_id = $2 OR lower(after_state ->> 'proxy_address') = lower($3)
             )",
        )
        .bind(CHAIN)
        .bind(Uuid::parse_str(V2_INVERSE_RESOURCE)?)
        .bind(V2_INVERSE_RESOLVER)
        .execute(pool)
        .await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        1,
        2,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    assert_eq!(
        resolver_projection_row(incremental.pool(), V2_INVERSE_RESOLVER).await?,
        resolver_projection_row(fresh.pool(), V2_INVERSE_RESOLVER).await?
    );
    assert!(
        resolver_projection_row(fresh.pool(), V2_INVERSE_RESOLVER)
            .await?
            .is_none()
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn interpret_redo_retracts_v2_permission_family_like_fresh_rebuild() -> Result<()> {
    const SURVIVING_RESOURCE: &str = "00000000-0000-0000-0000-0000000000da";

    let incremental = ScratchDatabase::create("production_project_v2_interpret_redo").await?;
    let fresh = ScratchDatabase::create("production_project_v2_interpret_redo_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_v2_permission_inverse_fixture(pool).await?;
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, $2, $3, 3, 'canonical')",
        )
        .bind(Uuid::parse_str(SURVIVING_RESOURCE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 3))
        .execute(pool)
        .await?;
        for powers in [json!(["resolver_control"]), json!([])] {
            insert_event(
                pool,
                CHAIN,
                3,
                None,
                Some(SURVIVING_RESOURCE),
                "PermissionChanged",
                "ens_v2_resolver_l1",
                json!({
                    "subject":OWNER,
                    "scope":{
                        "kind":"resolver",
                        "chain_id":CHAIN,
                        "resolver_address":V2_INVERSE_RESOLVER
                    },
                    "effective_powers":powers,
                    "grant_source":{"kind":"fixture"},
                    "revocation_source":{"kind":"fixture"},
                    "inheritance_path":[],
                    "transfer_behavior":"retain"
                }),
                json!({}),
            )
            .await?;
        }
        run_project(pool, CHAIN, None, RunMode::Normal, 0, 3).await?;
        let outcome = InterpretEngine::new(pool.clone())
            .run_batch(InterpretRequest {
                chain_id: CHAIN.into(),
                from_block: 1,
                to_block: 2,
                resume_current: None,
                mode: InterpretRunMode::Redo,
            })
            .await?;
        assert!(outcome.complete);
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        1,
        2,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        permission_projection_snapshot(incremental.pool()).await?,
        permission_projection_snapshot(fresh.pool()).await?,
        "v2-family Interpret redo diverged from fresh permission projections"
    );
    let incremental_resolver =
        resolver_projection_row(incremental.pool(), V2_INVERSE_RESOLVER).await?;
    let fresh_resolver = resolver_projection_row(fresh.pool(), V2_INVERSE_RESOLVER).await?;
    assert_eq!(
        fresh_resolver
            .as_ref()
            .and_then(|row| row.pointer("/declared_summary/classification/source_family")),
        Some(&json!("ens_v2_resolver_l1"))
    );
    assert_eq!(
        incremental_resolver, fresh_resolver,
        "v2-family redo failed to stage the surviving family representative"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn full_rebuild_consumes_redo_evidence_before_overlapping_capture() -> Result<()> {
    const FIRST: &str = "0x00000000000000000000000000000000000000e1";
    const SECOND: &str = "0x00000000000000000000000000000000000000e2";

    let scratch = ScratchDatabase::create("production_project_full_consumes_redo_evidence").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        None,
        None,
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":FIRST}),
        json!({}),
    )
    .await?;
    let event_identity: String = sqlx::query_scalar(
        "SELECT event_identity FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'ResolverChanged'
           AND lower(after_state ->> 'resolver') = lower($2)
         ORDER BY normalized_event_id DESC LIMIT 1",
    )
    .bind(CHAIN)
    .bind(FIRST)
    .fetch_one(scratch.pool())
    .await?;
    capture_resolver_redo_evidence(scratch.pool(), CHAIN, 2, 2).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let consumed_after_full: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS (
             SELECT 1 FROM project_redo_resolver_evidence
             WHERE chain_id = $1 AND event_identity = $2
         )",
    )
    .bind(CHAIN)
    .bind(&event_identity)
    .fetch_one(scratch.pool())
    .await?;

    sqlx::query(
        "UPDATE normalized_events
         SET after_state = jsonb_build_object('resolver', lower($2))
         WHERE chain_id = $1 AND event_identity = $3",
    )
    .bind(CHAIN)
    .bind(SECOND)
    .bind(&event_identity)
    .execute(scratch.pool())
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    capture_resolver_redo_evidence(scratch.pool(), CHAIN, 2, 2).await?;
    let captured_address: Option<String> = sqlx::query_scalar(
        "SELECT after_resolver_address FROM project_redo_resolver_evidence
         WHERE chain_id = $1 AND event_identity = $2",
    )
    .bind(CHAIN)
    .bind(&event_identity)
    .fetch_one(scratch.pool())
    .await?;
    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND event_identity = $2",
    )
    .bind(CHAIN)
    .bind(&event_identity)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    let second_survived: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)
         )",
    )
    .bind(CHAIN)
    .bind(SECOND)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        (consumed_after_full, captured_address, second_survived),
        (true, Some(SECOND.into()), false),
        "full rebuild left stale redo evidence ahead of an overlapping capture"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn redo_same_identity_different_resolver_retracts_the_losing_address() -> Result<()> {
    const LOSING: &str = "0x00000000000000000000000000000000000000e3";
    const WINNING: &str = "0x00000000000000000000000000000000000000e4";

    let incremental = ScratchDatabase::create("production_project_redo_changed_resolver").await?;
    let fresh = ScratchDatabase::create("production_project_redo_changed_resolver_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_project_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            2,
            None,
            None,
            "ResolverChanged",
            "ens_v1_registry_l1",
            json!({"resolver":LOSING}),
            json!({}),
        )
        .await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    capture_resolver_redo_evidence(incremental.pool(), CHAIN, 2, 2).await?;
    for pool in [incremental.pool(), fresh.pool()] {
        sqlx::query(
            "UPDATE normalized_events
             SET after_state = jsonb_build_object('resolver', lower($2))
             WHERE chain_id = $1 AND event_kind = 'ResolverChanged'
               AND lower(after_state ->> 'resolver') = lower($3)",
        )
        .bind(CHAIN)
        .bind(WINNING)
        .bind(LOSING)
        .execute(pool)
        .await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    let incremental_rows = (
        resolver_projection_row(incremental.pool(), LOSING).await?,
        resolver_projection_row(incremental.pool(), WINNING).await?,
    );
    let fresh_rows = (
        resolver_projection_row(fresh.pool(), LOSING).await?,
        resolver_projection_row(fresh.pool(), WINNING).await?,
    );
    assert!(fresh_rows.0.is_none());
    assert!(fresh_rows.1.is_some());
    assert_eq!(
        incremental_rows, fresh_rows,
        "same-identity resolver replacement kept the losing address published"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn scoped_v2_permission_revoke_does_not_retain_pre_swap_summary_row() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_v2_permission_revoke").await?;
    let fresh = ScratchDatabase::create("production_project_v2_permission_revoke_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_v2_permission_inverse_fixture(pool).await?;
        insert_lineage_block(pool, CHAIN, 4).await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    for pool in [incremental.pool(), fresh.pool()] {
        insert_event(
            pool,
            CHAIN,
            4,
            None,
            Some(V2_INVERSE_RESOURCE),
            "PermissionChanged",
            "ens_v2_resolver_l1",
            json!({
                "subject":OWNER,
                "scope":{
                    "kind":"resolver",
                    "chain_id":CHAIN,
                    "resolver_address":V2_INVERSE_RESOLVER
                },
                "effective_powers":[],
                "grant_source":{"kind":"fixture"},
                "revocation_source":{"kind":"fixture"},
                "inheritance_path":[],
                "transfer_behavior":"retain"
            }),
            json!({}),
        )
        .await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    let incremental_row = resolver_projection_row(incremental.pool(), V2_INVERSE_RESOLVER).await?;
    let fresh_row = resolver_projection_row(fresh.pool(), V2_INVERSE_RESOLVER).await?;
    assert_eq!(
        fresh_row
            .as_ref()
            .and_then(|row| row.pointer("/declared_summary/permissions/count")),
        Some(&json!(0))
    );
    assert_eq!(
        incremental_row, fresh_row,
        "a scoped v2 revoke retained the pre-swap permission summary row"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn project_redo_retracts_a_missing_primary_claim_with_surviving_reverse() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_missing_primary_claim").await?;
    seed_project_fixture(scratch.pool()).await?;
    sqlx::query(
        "UPDATE normalized_events
         SET block_number = 2, block_hash = $2
         WHERE chain_id = $1 AND event_kind = 'ReverseChanged'",
    )
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 2))
    .execute(scratch.pool())
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let before: (String, Option<String>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name
         FROM primary_names_current WHERE namespace = 'ens'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(before, ("success".to_owned(), Some("alice.eth".to_owned())));

    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RecordChanged'
           AND after_state ? 'primary_claim_source'",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        3,
        3,
    )
    .await?;

    let after: (String, Option<String>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name
         FROM primary_names_current WHERE namespace = 'ens'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(after, ("not_found".to_owned(), None));
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_retracts_a_missing_reverse_resolver_pointer() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_missing_reverse_resolver").await?;
    seed_project_fixture(scratch.pool()).await?;
    let address = "0x00000000000000000000000000000000000000a2";
    let reverse_node = "0x2223456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    insert_event(
        scratch.pool(),
        CHAIN,
        1,
        None,
        None,
        "ReverseChanged",
        "ens_v1_reverse_l1",
        json!({
            "address":address,
            "coin_type":"60",
            "namespace":"ens",
            "reverse_node":reverse_node
        }),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        None,
        None,
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"node":reverse_node,"resolver":RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        None,
        None,
        "RecordChanged",
        "ens_v1_reverse_l1",
        json!({
            "raw_name":"alice.eth",
            "primary_claim_source":{
                "address":address,
                "coin_type":"60",
                "namespace":"ens"
            }
        }),
        json!({}),
    )
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let before: (Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT claim_provenance ->> 'resolver_address',
                NULLIF(claim_provenance ->> 'resolver_event_id', '')::bigint
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(address)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(before.0.as_deref(), Some(RESOLVER));
    assert!(before.1.is_some());

    capture_resolver_redo_evidence(scratch.pool(), CHAIN, 2, 2).await?;
    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND block_number = 2
           AND event_kind = 'ResolverChanged'
           AND after_state ->> 'node' = $2",
    )
    .bind(CHAIN)
    .bind(reverse_node)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;

    let after: (Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT claim_provenance ->> 'resolver_address',
                NULLIF(claim_provenance ->> 'resolver_event_id', '')::bigint
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(address)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(after, (None, None));
    scratch.cleanup().await
}

#[tokio::test]
async fn mixed_authority_survives_v2_expiry_as_explicitly_unsupported() -> Result<()> {
    let scratch = ScratchDatabase::create("project_mixed_authority_v2_expiry").await?;
    let chain = "project-mixed-authority-v2-expiry";
    let logical_name_id = seed_dual_open_cross_arm_fixture(scratch.pool(), chain, 4).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    let initial_reason: Option<String> = sqlx::query_scalar(
        "SELECT unsupported_reason FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        initial_reason.as_deref(),
        Some("conflicting_current_ens_authority")
    );

    insert_lineage_block(scratch.pool(), chain, 6).await?;
    let v2_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v2'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(6)
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v2'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        6,
        Some(&logical_name_id),
        Some(&v2_resource.to_string()),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({
            "source_event":"RegistryPathExpired",
            "derived_from":"interpreter_state",
            "terminal_reason":"registry_name_binding_expired",
            "status":"released"
        }),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 5,
            hash: block_hash(chain, 5),
        }),
        RunMode::Normal,
        6,
        6,
    )
    .await?;
    normalize_projection_clocks(scratch.pool()).await?;
    let incremental: Option<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_optional(scratch.pool())
    .await?;
    let incremental = incremental.expect("mixed authority must remain explicitly unsupported");
    assert_eq!(
        incremental["unsupported_reason"],
        "conflicting_current_ens_authority"
    );

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 6).await?;
    normalize_projection_clocks(scratch.pool()).await?;
    let fresh: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(incremental, fresh);
    scratch.cleanup().await
}

#[tokio::test]
async fn mixed_authority_expiry_release_preserves_guarded_summary_fields() -> Result<()> {
    let incremental = ScratchDatabase::create("project_mixed_authority_summary_guard").await?;
    let fresh = ScratchDatabase::create("project_mixed_authority_summary_guard_fresh").await?;
    let chain = "project-mixed-authority-summary-guard";
    let mut logical_name_id = String::new();

    for pool in [incremental.pool(), fresh.pool()] {
        let seeded_name = seed_dual_open_cross_arm_fixture(pool, chain, 4).await?;
        if logical_name_id.is_empty() {
            logical_name_id = seeded_name;
        } else {
            assert_eq!(logical_name_id, seeded_name);
        }
        InterpretEngine::new(pool.clone())
            .run_batch(InterpretRequest {
                chain_id: chain.into(),
                from_block: 0,
                to_block: 5,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await?;
        insert_lineage_block(pool, chain, 6).await?;
        let v2_resource: Uuid = sqlx::query_scalar(
            "SELECT resource_id FROM surface_bindings
             WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v2'",
        )
        .bind(chain)
        .bind(&logical_name_id)
        .fetch_one(pool)
        .await?;
        sqlx::query(
            "UPDATE surface_bindings SET active_to = to_timestamp(6)
             WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v2'",
        )
        .bind(chain)
        .bind(&logical_name_id)
        .execute(pool)
        .await?;
        insert_event(
            pool,
            chain,
            6,
            Some(&logical_name_id),
            Some(&v2_resource.to_string()),
            "RegistrationReleased",
            "ens_v2_registry_l1",
            json!({
                "source_event":"RegistryPathExpired",
                "derived_from":"interpreter_state",
                "terminal_reason":"registry_name_binding_expired",
                "expiry":4_000_000_000_i64,
                "status":"released"
            }),
            json!({}),
        )
        .await?;
    }

    run_project(incremental.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    run_project(
        incremental.pool(),
        chain,
        Some(Marker {
            number: 5,
            hash: block_hash(chain, 5),
        }),
        RunMode::Normal,
        6,
        6,
    )
    .await?;
    run_project(fresh.pool(), chain, None, RunMode::Normal, 0, 6).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;

    let incremental_row: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(incremental.pool())
    .await?;
    let fresh_row: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(fresh.pool())
    .await?;
    assert_eq!(incremental_row, fresh_row);
    assert_eq!(
        incremental_row["unsupported_reason"],
        "conflicting_current_ens_authority"
    );
    assert_eq!(
        (
            incremental_row.pointer("/declared_summary/registration/registrant"),
            incremental_row.pointer("/declared_summary/registration/expiry"),
            incremental_row.pointer("/declared_summary/registration/authority_kind"),
            incremental_row.pointer("/declared_summary/registration/authority_key"),
            incremental_row.pointer("/declared_summary/control/status"),
        ),
        (
            Some(&Value::Null),
            Some(&json!(4_000_000_000_i64)),
            Some(&Value::Null),
            Some(&Value::Null),
            Some(&json!("released")),
        ),
        "an unresolved mixed-authority release must preserve its exact summary fields",
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn surviving_reservation_drives_summary_after_other_resource_expires() -> Result<()> {
    const REGISTERED_RESOURCE: &str = "00000000-0000-0000-0000-0000000008a1";
    const RESERVED_RESOURCE: &str = "00000000-0000-0000-0000-0000000008a2";
    const RESERVED_LINEAGE: &str = "00000000-0000-0000-0000-0000000008a4";
    const BINDING: &str = "00000000-0000-0000-0000-0000000008a3";
    let scratch = ScratchDatabase::create("project_registration_reservation_lifecycle").await?;
    let chain = "project-registration-reservation-lifecycle";
    let logical_name_id = "ens:0x8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a";
    seed_lineage(scratch.pool(), chain, 3).await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, 'ens', 'combined.eth', ARRAY['combined','eth'],
             decode('00','hex'), $2, ARRAY['0xcombined','0xeth'], $3,
             'active', $4, $5, 1, 'canonical')",
    )
    .bind(logical_name_id)
    .bind(logical_name_id.trim_start_matches("ens:"))
    .bind(NORMALIZER)
    .bind(chain)
    .bind(block_hash(chain, 1))
    .execute(scratch.pool())
    .await?;
    insert_classifier_resource_and_binding(
        scratch.pool(),
        chain,
        logical_name_id,
        "ens_v2",
        Uuid::parse_str(REGISTERED_RESOURCE)?,
        Uuid::parse_str(BINDING)?,
        1,
        None,
    )
    .await?;
    sqlx::query(
        "INSERT INTO token_lineages (
             token_lineage_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 2, 'canonical')",
    )
    .bind(Uuid::parse_str(RESERVED_LINEAGE)?)
    .bind(chain)
    .bind(block_hash(chain, 2))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, token_lineage_id, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES ($1, $2, $3, $4, 2, 'canonical')",
    )
    .bind(Uuid::parse_str(RESERVED_RESOURCE)?)
    .bind(Uuid::parse_str(RESERVED_LINEAGE)?)
    .bind(chain)
    .bind(block_hash(chain, 2))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        1,
        Some(logical_name_id),
        Some(REGISTERED_RESOURCE),
        "RegistrationGranted",
        "ens_v2_registry_l1",
        json!({
            "status":"registered",
            "expiry":3,
            "token_id":"0x01",
            "registry":"0xregistry",
            "registrant":OWNER,
            "authority_kind":"ens_v2_registry",
            "authority_key":"expired-registration"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        1,
        Some(logical_name_id),
        Some(REGISTERED_RESOURCE),
        "ResolverChanged",
        "ens_v2_registry_l1",
        json!({"resolver":RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        2,
        Some(logical_name_id),
        Some(RESERVED_RESOURCE),
        "RegistrationReserved",
        "ens_v2_registry_l1",
        json!({"status":"reserved","expiry":100,"token_id":"0x02","registry":"0xregistry"}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        2,
        Some(logical_name_id),
        Some(RESERVED_RESOURCE),
        "ResolverChanged",
        "ens_v2_registry_l1",
        json!({"resolver":RESOLVER}),
        json!({}),
    )
    .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 2).await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(3) WHERE surface_binding_id = $1",
    )
    .bind(Uuid::parse_str(BINDING)?)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        3,
        Some(logical_name_id),
        Some(REGISTERED_RESOURCE),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({
            "source_event":"RegistryPathExpired",
            "derived_from":"interpreter_state",
            "terminal_reason":"registry_name_binding_expired",
            "status":"released"
        }),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 2,
            hash: block_hash(chain, 2),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    normalize_projection_clocks(scratch.pool()).await?;
    let incremental: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        incremental["declared_summary"]["registration"]["status"],
        "reserved"
    );
    assert_eq!(
        incremental["declared_summary"]["registration"]["latest_event_kind"],
        "RegistrationReserved"
    );
    assert_eq!(
        incremental["declared_summary"]["registration"]["expiry"],
        100
    );
    assert_eq!(
        incremental["declared_summary"]["control"]["status"],
        "reserved"
    );
    assert_eq!(
        incremental["declared_summary"]["registration"]["registrant"],
        Value::Null
    );
    assert_eq!(
        incremental["declared_summary"]["registration"]["registered_at"],
        Value::Null
    );
    assert_eq!(
        incremental["declared_summary"]["registration"]["authority_kind"],
        Value::Null
    );
    assert_eq!(
        incremental["declared_summary"]["control"]["registrant"],
        Value::Null
    );
    assert_eq!(
        incremental["declared_summary"]["resolver"]["address"],
        Value::Null
    );
    assert_eq!(
        incremental["resource_id"],
        Value::Null,
        "the reservation-selected row exposed a registration resource",
    );
    assert_eq!(incremental["token_lineage_id"], Value::Null);
    assert_eq!(incremental["surface_binding_id"], Value::Null);
    assert_eq!(incremental["binding_kind"], Value::Null);

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 3).await?;
    normalize_projection_clocks(scratch.pool()).await?;
    let fresh: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(incremental, fresh);
    scratch.cleanup().await
}

#[tokio::test]
async fn state_derived_expiry_for_another_resource_does_not_delete_the_current_binding()
-> Result<()> {
    const CURRENT_RESOURCE: &str = "00000000-0000-0000-0000-0000000008b1";
    const EXPIRED_RESOURCE: &str = "00000000-0000-0000-0000-0000000008b2";
    const CURRENT_BINDING: &str = "00000000-0000-0000-0000-0000000008b3";
    let scratch = ScratchDatabase::create("project_expiry_other_resource_guard").await?;
    let chain = "project-expiry-other-resource-guard";
    let logical_name_id = "ens:0x8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b";
    seed_lineage(scratch.pool(), chain, 2).await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, 'ens', 'current-binding.eth', ARRAY['current-binding','eth'],
             decode('00','hex'), $2, ARRAY['0xcurrent-binding','0xeth'], $3,
             'active', $4, $5, 1, 'canonical')",
    )
    .bind(logical_name_id)
    .bind(logical_name_id.trim_start_matches("ens:"))
    .bind(NORMALIZER)
    .bind(chain)
    .bind(block_hash(chain, 1))
    .execute(scratch.pool())
    .await?;
    insert_classifier_resource_and_binding(
        scratch.pool(),
        chain,
        logical_name_id,
        "ens_v2",
        Uuid::parse_str(CURRENT_RESOURCE)?,
        Uuid::parse_str(CURRENT_BINDING)?,
        1,
        None,
    )
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(EXPIRED_RESOURCE)?)
    .bind(chain)
    .bind(block_hash(chain, 1))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        1,
        Some(logical_name_id),
        Some(CURRENT_RESOURCE),
        "AuthorityEpochChanged",
        "ens_v2_registry_l1",
        json!({
            "authority_kind":"ens_v2_registry",
            "authority_key":"current-binding",
            "registry_owner":OWNER,
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        1,
        Some(logical_name_id),
        Some(CURRENT_RESOURCE),
        "AuthorityTransferred",
        "ens_v2_registry_l1",
        json!({"owner":OWNER,"registry_owner":OWNER}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        1,
        Some(logical_name_id),
        Some(CURRENT_RESOURCE),
        "ResolverChanged",
        "ens_v2_registry_l1",
        json!({"resolver":RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        1,
        Some(logical_name_id),
        Some(EXPIRED_RESOURCE),
        "RegistrationGranted",
        "ens_v2_registry_l1",
        json!({
            "status":"registered",
            "expiry":100,
            "token_id":"0x8b02",
            "registry_contract_instance_id":"0xregistry"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        chain,
        2,
        Some(logical_name_id),
        Some(EXPIRED_RESOURCE),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({
            "source_event":"RegistryPathExpired",
            "derived_from":"interpreter_state",
            "terminal_reason":"registry_name_binding_expired",
            "token_id":"0x8b02",
            "registry_contract_instance_id":"0xregistry"
        }),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 2).await?;
    let row: Value = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        row["surface_binding_id"], CURRENT_BINDING,
        "an unrelated expiry release replaced the current surface binding",
    );
    assert_eq!(row["resource_id"], CURRENT_RESOURCE);
    assert_eq!(row["binding_kind"], "declared_registry_path");
    assert_eq!(row["declared_summary"]["registration"]["status"], "active");
    assert_eq!(row["declared_summary"]["resolver"]["address"], RESOLVER);
    assert_eq!(
        address_relation_holders(scratch.pool(), logical_name_id, "effective_controller").await?,
        vec![OWNER],
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn expiry_release_redo_restores_deleted_name_like_fresh_rebuild() -> Result<()> {
    let incremental = ScratchDatabase::create("project_expiry_name_redo").await?;
    let fresh = ScratchDatabase::create("project_expiry_name_redo_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_expiry_release_redo_fixture(pool).await?;
    }
    publish_and_retract_expiry_fixture(incremental.pool()).await?;
    assert_eq!(expiry_fixture_counts(incremental.pool()).await?.0, 0);
    for pool in [incremental.pool(), fresh.pool()] {
        seed_later_expiry_fixture_event(pool).await?;
    }
    advance_expiry_fixture_beyond_redo(incremental.pool()).await?;
    assert_eq!(expiry_fixture_counts(incremental.pool()).await?.0, 0);
    for pool in [incremental.pool(), fresh.pool()] {
        orphan_expiry_release_and_reopen_binding(pool).await?;
    }
    run_project(
        incremental.pool(),
        "project-expiry-release-redo",
        Some(Marker {
            number: 3,
            hash: block_hash("project-expiry-release-redo", 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    run_project(
        fresh.pool(),
        "project-expiry-release-redo",
        None,
        RunMode::Normal,
        0,
        3,
    )
    .await?;
    assert_eq!(expiry_fixture_counts(fresh.pool()).await?.0, 1);
    assert_eq!(
        expiry_fixture_name(incremental.pool()).await?,
        expiry_fixture_name(fresh.pool()).await?,
        "redo failed to restore the reopened name"
    );
    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn expiry_release_redo_uses_displaced_branch_timestamps_for_name_scope() -> Result<()> {
    const CHAIN: &str = "project-expiry-timestamp-reorg";
    const NAME: &str = "ens:0x8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e";
    const REPLACEMENT_TWO: &str =
        "0x8e02000000000000000000000000000000000000000000000000000000000000";
    const REPLACEMENT_THREE: &str =
        "0x8e03000000000000000000000000000000000000000000000000000000000000";
    let incremental = ScratchDatabase::create("project_expiry_timestamp_reorg").await?;
    let fresh = ScratchDatabase::create("project_expiry_timestamp_reorg_fresh").await?;

    for pool in [incremental.pool(), fresh.pool()] {
        for (number, timestamp) in [(0_i64, 0_i64), (1, 10), (2, 30), (3, 40)] {
            sqlx::query(
                "INSERT INTO chain_lineage (
                     chain_id, block_hash, parent_hash, block_number,
                     block_timestamp, canonicality_state
                 ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
            )
            .bind(CHAIN)
            .bind(block_hash(CHAIN, number))
            .bind((number > 0).then(|| block_hash(CHAIN, number - 1)))
            .bind(number)
            .bind(timestamp)
            .execute(pool)
            .await?;
        }
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
                 namehash, labelhashes, normalizer_version, visibility_state,
                 chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, 'ens', 'timestamp-reorg.eth', ARRAY['timestamp-reorg','eth'],
                 decode('00','hex'), $2, ARRAY['0xtimestamp-reorg','0xeth'], $3,
                 'active', $4, $5, 1, 'canonical')",
        )
        .bind(NAME)
        .bind(NAME.trim_start_matches("ens:"))
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some(NAME),
            None,
            "RegistrationReserved",
            "ens_v2_registry_l1",
            json!({
                "status":"reserved",
                "expiry":20,
                "token_id":"0x8e",
                "registry":"0xregistry",
                "reservation_resource":true
            }),
            json!({"fixture":"expiry-timestamp-registration"}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            2,
            Some(NAME),
            None,
            "RegistrationReleased",
            "ens_v2_registry_l1",
            json!({
                "source_event":"RegistryPathExpired",
                "derived_from":"interpreter_state",
                "terminal_reason":"registry_name_binding_expired",
                "token_id":"0x8e",
                "registry":"0xregistry",
                "status":"released"
            }),
            json!({"fixture":"expiry-timestamp-release"}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 1).await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 1,
            hash: block_hash(CHAIN, 1),
        }),
        RunMode::Normal,
        2,
        2,
    )
    .await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 2,
            hash: block_hash(CHAIN, 2),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    let expired_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_current WHERE logical_name_id = $1")
            .bind(NAME)
            .fetch_one(incremental.pool())
            .await?;
    assert_eq!(expired_count, 0);

    for pool in [incremental.pool(), fresh.pool()] {
        sqlx::query(
            "UPDATE chain_lineage SET canonicality_state = 'orphaned'
             WHERE chain_id = $1 AND block_number BETWEEN 2 AND 3",
        )
        .bind(CHAIN)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $4, 2, to_timestamp(15), 'canonical'),
                      ($1, $3, $2, 3, to_timestamp(16), 'canonical')",
        )
        .bind(CHAIN)
        .bind(REPLACEMENT_TWO)
        .bind(REPLACEMENT_THREE)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND block_number = 2
               AND event_kind = 'RegistrationReleased'",
        )
        .bind(CHAIN)
        .execute(pool)
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: REPLACEMENT_THREE.into(),
        }),
        RunMode::Redo,
        2,
        3,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    let incremental_row: Option<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(NAME)
    .fetch_optional(incremental.pool())
    .await?;
    let fresh_row: Option<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(NAME)
    .fetch_optional(fresh.pool())
    .await?;
    assert!(fresh_row.is_some());
    assert_eq!(
        incremental_row, fresh_row,
        "redo ignored the displaced branch's expiry-crossing timestamp"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn expiry_redo_does_not_rebuild_a_released_resource_linked_lifecycle() -> Result<()> {
    const CHAIN: &str = "project-expiry-linked-lifecycle";
    const NAME: &str = "ens:0x8989898989898989898989898989898989898989898989898989898989898989";
    const RESOURCE: &str = "00000000-0000-0000-0000-000000008f05";
    const REGISTRY_INSTANCE: &str = "00000000-0000-0000-0000-000000008f06";
    let scratch = ScratchDatabase::create("project_expiry_linked_lifecycle").await?;
    for (number, timestamp) in [(0_i64, 0_i64), (1, 10), (2, 15), (3, 25)] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(CHAIN, number))
        .bind((number > 0).then(|| block_hash(CHAIN, number - 1)))
        .bind(number)
        .bind(timestamp)
        .execute(scratch.pool())
        .await?;
    }
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, 'ens', 'linked-release.eth', ARRAY['linked-release','eth'],
             decode('00','hex'), $2, ARRAY['0xlinked-release','0xeth'], $3,
             'active', $4, $5, 1, 'canonical')",
    )
    .bind(NAME)
    .bind(NAME.trim_start_matches("ens:"))
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    for resource_id in [None, Some(RESOURCE)] {
        insert_event(
            scratch.pool(),
            CHAIN,
            1,
            Some(NAME),
            resource_id,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            json!({
                "status":"registered",
                "expiry":20,
                "token_id":"0xlinked",
                "registry":"0xregistry",
                "registry_contract_instance_id":REGISTRY_INSTANCE
            }),
            json!({"fixture":"expiry-linked-registration"}),
        )
        .await?;
    }
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some(NAME),
        Some(RESOURCE),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({
            "source_event":"LabelUnregistered",
            "terminal_reason":"registry_name_binding_changed",
            "status":"released",
            "token_id":"0xlinked",
            "registry_contract_instance_id":REGISTRY_INSTANCE
        }),
        json!({"fixture":"expiry-linked-release"}),
    )
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query("UPDATE name_current SET raw_name = 'linked-release-unchanged.eth' WHERE logical_name_id = $1")
        .bind(NAME)
        .execute(scratch.pool())
        .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        3,
        3,
    )
    .await?;
    let raw_name: String =
        sqlx::query_scalar("SELECT raw_name FROM name_current WHERE logical_name_id = $1")
            .bind(NAME)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(raw_name, "linked-release-unchanged.eth");

    scratch.cleanup().await
}

#[tokio::test]
async fn expiry_redo_does_not_rebuild_a_released_resourceless_lifecycle() -> Result<()> {
    const CHAIN: &str = "project-expiry-resourceless-lifecycle";
    const NAME: &str = "ens:0x8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f";
    const REGISTRY_INSTANCE: &str = "00000000-0000-0000-0000-000000008f07";
    let scratch = ScratchDatabase::create("project_expiry_resourceless_lifecycle").await?;
    for (number, timestamp) in [(0_i64, 0_i64), (1, 10), (2, 15), (3, 25)] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(CHAIN, number))
        .bind((number > 0).then(|| block_hash(CHAIN, number - 1)))
        .bind(number)
        .bind(timestamp)
        .execute(scratch.pool())
        .await?;
    }
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, 'ens', 'resourceless-release.eth',
             ARRAY['resourceless-release','eth'], decode('00','hex'), $2,
             ARRAY['0xresourceless-release','0xeth'], $3, 'active', $4, $5, 1,
             'canonical')",
    )
    .bind(NAME)
    .bind(NAME.trim_start_matches("ens:"))
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        1,
        Some(NAME),
        None,
        "RegistrationGranted",
        "ens_v2_registry_l1",
        json!({
            "status":"registered",
            "expiry":20,
            "token_id":"0xresourceless",
            "registry_contract_instance_id":REGISTRY_INSTANCE
        }),
        json!({"fixture":"expiry-resourceless-registration"}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some(NAME),
        None,
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({
            "source_event":"LabelUnregistered",
            "sender":"0x00000000000000000000000000000000000000aa",
            "registry_contract_instance_id":REGISTRY_INSTANCE,
            "token_id":"0xresourceless"
        }),
        json!({"fixture":"expiry-resourceless-release"}),
    )
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query(
        "UPDATE name_current SET raw_name = 'resourceless-release-unchanged.eth'
         WHERE logical_name_id = $1",
    )
    .bind(NAME)
    .execute(scratch.pool())
    .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        3,
        3,
    )
    .await?;
    let raw_name: String =
        sqlx::query_scalar("SELECT raw_name FROM name_current WHERE logical_name_id = $1")
            .bind(NAME)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(
        raw_name, "resourceless-release-unchanged.eth",
        "a status-less RegistrationReleased head entered expiry redo scope",
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn name_current_latest_event_stays_with_selected_resourceless_lifecycle() -> Result<()> {
    const CHAIN: &str = "project-resourceless-lifecycle-head";
    const NAME: &str = "ens:0x8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e";
    const REGISTRY_INSTANCE: &str = "00000000-0000-0000-0000-000000008f08";
    let scratch = ScratchDatabase::create("project_resourceless_lifecycle_head").await?;
    for number in 0_i64..=2 {
        insert_lineage_block(scratch.pool(), CHAIN, number).await?;
    }
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, 'ens', 'resourceless-head.eth', ARRAY['resourceless-head','eth'],
             decode('00','hex'), $2, ARRAY['0xresourceless-head','0xeth'], $3,
             'active', $4, $5, 1, 'canonical')",
    )
    .bind(NAME)
    .bind(NAME.trim_start_matches("ens:"))
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        1,
        Some(NAME),
        None,
        "RegistrationGranted",
        "ens_v2_registry_l1",
        json!({
            "status":"registered", "expiry":5_000_000_000_u64,
            "token_id":"0xselected", "registry_contract_instance_id":REGISTRY_INSTANCE
        }),
        json!({"fixture":"selected-resourceless-lifecycle"}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some(NAME),
        None,
        "RegistrationRenewed",
        "ens_v2_registry_l1",
        json!({
            "status":"registered", "expiry":5_000_000_001_u64,
            "token_id":"0xother", "registry_contract_instance_id":REGISTRY_INSTANCE
        }),
        json!({"fixture":"other-resourceless-lifecycle"}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 2).await?;
    let (latest_event_kind, expiry): (String, Value) = sqlx::query_as(
        "SELECT declared_summary #>> '{registration,latest_event_kind}',
                declared_summary #> '{registration,expiry}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(NAME)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(latest_event_kind, "RegistrationGranted");
    assert_eq!(expiry, json!(5_000_000_000_u64));

    scratch.cleanup().await
}

#[tokio::test]
async fn ancestor_expiry_release_redo_restores_descendant_with_later_local_expiry() -> Result<()> {
    assert_ancestor_expiry_release_redo_restores_descendant(
        "project_ancestor_expiry_reorg",
        None,
        "RegistrationGranted",
        false,
    )
    .await
}

#[tokio::test]
async fn ancestor_expiry_release_redo_restores_descendant_after_replacement_renewal() -> Result<()>
{
    assert_ancestor_expiry_release_redo_restores_descendant(
        "project_ancestor_expiry_reorg_renewed",
        Some((2, 200)),
        "RegistrationGranted",
        false,
    )
    .await
}

#[tokio::test]
async fn ancestor_expiry_subrange_redo_restores_descendant_after_later_renewal() -> Result<()> {
    assert_ancestor_expiry_release_redo_restores_descendant(
        "project_ancestor_expiry_subrange_renewed",
        Some((3, 200)),
        "RegistrationGranted",
        false,
    )
    .await
}

#[tokio::test]
async fn ancestor_expiry_release_redo_restores_reserved_descendant_subtree() -> Result<()> {
    assert_ancestor_expiry_release_redo_restores_descendant(
        "project_ancestor_expiry_reserved_reorg",
        None,
        "RegistrationReserved",
        false,
    )
    .await
}

#[tokio::test]
async fn orphaned_shortened_ancestor_expiry_restores_descendants_after_interpret_redo() -> Result<()>
{
    assert_ancestor_expiry_release_redo_restores_descendant(
        "project_ancestor_orphan_only_expiry_reorg",
        None,
        "RegistrationGranted",
        true,
    )
    .await
}

async fn assert_ancestor_expiry_release_redo_restores_descendant(
    fixture_name: &str,
    replacement_parent_renewal: Option<(i64, i64)>,
    child_registration_kind: &str,
    orphan_only_shortening: bool,
) -> Result<()> {
    const CHAIN: &str = "project-ancestor-expiry-reorg";
    const PARENT: &str = "ens:0x8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f";
    const CHILD: &str = "ens:0x8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c";
    const GRANDCHILD: &str =
        "ens:0x8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d";
    const SENTINEL: &str = "ens:0x8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e";
    const CHILD_REGISTRY: &str = "0x0000000000000000000000000000000000008f01";
    const CHILD_REGISTRY_INSTANCE: &str = "00000000-0000-0000-0000-000000008f01";
    const GRANDCHILD_REGISTRY: &str = "0x0000000000000000000000000000000000008f02";
    const GRANDCHILD_REGISTRY_INSTANCE: &str = "00000000-0000-0000-0000-000000008f02";
    const CHILD_RESERVED_RESOURCE: &str = "00000000-0000-0000-0000-000000008f03";
    const RELEASED_CHILD_RESOURCE: &str = "00000000-0000-0000-0000-000000008f07";
    const FORMER_CHILD: &str =
        "ens:0x8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a";
    const RELEASED_CHILD: &str =
        "ens:0x8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b";
    const FORMER_REGISTRY: &str = "0x0000000000000000000000000000000000008f04";
    const FORMER_REGISTRY_INSTANCE: &str = "00000000-0000-0000-0000-000000008f04";
    const REPLACEMENT_TWO: &str =
        "0x8f02000000000000000000000000000000000000000000000000000000000000";
    const REPLACEMENT_THREE: &str =
        "0x8f03000000000000000000000000000000000000000000000000000000000000";
    let incremental = ScratchDatabase::create(fixture_name).await?;
    let fresh = ScratchDatabase::create(&format!("{fixture_name}_fresh")).await?;

    for pool in [incremental.pool(), fresh.pool()] {
        declare_sepolia_post_audit_profile(pool, CHAIN).await?;
        for (number, timestamp) in [(0_i64, 0_i64), (1, 10), (2, 30), (3, 40)] {
            sqlx::query(
                "INSERT INTO chain_lineage (
                     chain_id, block_hash, parent_hash, block_number,
                     block_timestamp, canonicality_state
                 ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
            )
            .bind(CHAIN)
            .bind(block_hash(CHAIN, number))
            .bind((number > 0).then(|| block_hash(CHAIN, number - 1)))
            .bind(number)
            .bind(timestamp)
            .execute(pool)
            .await?;
        }
        for (logical_name_id, raw_name, raw_labels, labelhashes) in [
            (
                PARENT,
                "parent.eth",
                vec!["parent", "eth"],
                vec!["0xparent", "0xeth"],
            ),
            (
                CHILD,
                "child.parent.eth",
                vec!["child", "parent", "eth"],
                vec!["0xchild", "0xparent", "0xeth"],
            ),
            (
                GRANDCHILD,
                "grandchild.child.parent.eth",
                vec!["grandchild", "child", "parent", "eth"],
                vec!["0xgrandchild", "0xchild", "0xparent", "0xeth"],
            ),
            (
                SENTINEL,
                "unrelated.eth",
                vec!["unrelated", "eth"],
                vec!["0xunrelated", "0xeth"],
            ),
            (
                FORMER_CHILD,
                "former.parent.eth",
                vec!["former", "parent", "eth"],
                vec!["0xformer", "0xparent", "0xeth"],
            ),
            (
                RELEASED_CHILD,
                "released.parent.eth",
                vec!["released", "parent", "eth"],
                vec!["0xreleased", "0xparent", "0xeth"],
            ),
        ] {
            sqlx::query(
                "INSERT INTO name_surfaces (
                     logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
                     namehash, labelhashes, normalizer_version, visibility_state,
                     chain_id, block_hash, block_number, canonicality_state
                 ) VALUES ($1, 'ens', $2, $3, decode('00','hex'), $4,
                     $5, $6, 'active', $7, $8, 1, 'canonical')",
            )
            .bind(logical_name_id)
            .bind(raw_name)
            .bind(raw_labels)
            .bind(logical_name_id.trim_start_matches("ens:"))
            .bind(labelhashes)
            .bind(NORMALIZER)
            .bind(CHAIN)
            .bind(block_hash(CHAIN, 1))
            .execute(pool)
            .await?;
        }
        for (instance, address) in [
            (CHILD_REGISTRY_INSTANCE, CHILD_REGISTRY),
            (GRANDCHILD_REGISTRY_INSTANCE, GRANDCHILD_REGISTRY),
            (FORMER_REGISTRY_INSTANCE, FORMER_REGISTRY),
        ] {
            sqlx::query(
                "INSERT INTO contract_instances (
                     contract_instance_id, chain_id, contract_kind, provenance
                 ) VALUES ($1, $2, 'contract', '{\"fixture\":true}'::jsonb)",
            )
            .bind(Uuid::parse_str(instance)?)
            .bind(CHAIN)
            .execute(pool)
            .await?;
            sqlx::query(
                "INSERT INTO contract_instance_addresses (
                     contract_instance_id, chain_id, address,
                     active_from_block_number, active_from_block_hash, provenance
                 ) VALUES ($1, $2, $3, 1, $4, '{\"fixture\":true}'::jsonb)",
            )
            .bind(Uuid::parse_str(instance)?)
            .bind(CHAIN)
            .bind(address)
            .bind(block_hash(CHAIN, 1))
            .execute(pool)
            .await?;
        }
        if child_registration_kind == "RegistrationReserved" {
            sqlx::query(
                "INSERT INTO resources (
                     resource_id, chain_id, block_hash, block_number, canonicality_state
                 ) VALUES ($1, $2, $3, 1, 'canonical')",
            )
            .bind(Uuid::parse_str(CHILD_RESERVED_RESOURCE)?)
            .bind(CHAIN)
            .bind(block_hash(CHAIN, 1))
            .execute(pool)
            .await?;
        }
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, $2, $3, 1, 'canonical')",
        )
        .bind(Uuid::parse_str(RELEASED_CHILD_RESOURCE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some(PARENT),
            None,
            "RegistrationGranted",
            "ens_v2_root_l1",
            json!({
                "status":"registered",
                "expiry":if orphan_only_shortening { 200 } else { 20 },
                "token_id":"0xparent",
                "registry":"0xrootregistry"
            }),
            json!({"fixture":"ancestor-expiry-parent-registration"}),
        )
        .await?;
        if orphan_only_shortening {
            insert_event(
                pool,
                CHAIN,
                2,
                Some(PARENT),
                None,
                "ExpiryChanged",
                "ens_v2_root_l1",
                json!({
                    "source_event":"ExpiryUpdated",
                    "expiry":20,
                    "token_id":"0xparent",
                    "registry":"0xrootregistry"
                }),
                json!({"fixture":"ancestor-expiry-orphaned-shortening"}),
            )
            .await?;
        }
        insert_event(
            pool,
            CHAIN,
            1,
            Some(CHILD),
            (child_registration_kind == "RegistrationReserved").then_some(CHILD_RESERVED_RESOURCE),
            "SubregistryChanged",
            "ens_v2_registry_l1",
            json!({
                "source_event":"SubregistryUpdated",
                "subregistry":GRANDCHILD_REGISTRY
            }),
            json!({"fixture":"ancestor-expiry-child-subregistry"}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some(PARENT),
            None,
            "SubregistryChanged",
            "ens_v2_root_l1",
            json!({
                "source_event":"SubregistryUpdated",
                "subregistry":FORMER_REGISTRY
            }),
            json!({"fixture":"ancestor-expiry-former-subregistry"}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some(PARENT),
            None,
            "SubregistryChanged",
            "ens_v2_root_l1",
            json!({
                "source_event":"SubregistryUpdated",
                "subregistry":CHILD_REGISTRY
            }),
            json!({"fixture":"ancestor-expiry-parent-subregistry"}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some(CHILD),
            (child_registration_kind == "RegistrationReserved").then_some(CHILD_RESERVED_RESOURCE),
            child_registration_kind,
            "ens_v2_registry_l1",
            json!({
                "status":if child_registration_kind == "RegistrationReserved" {
                    "reserved"
                } else {
                    "registered"
                },
                "reservation_resource":child_registration_kind == "RegistrationReserved",
                "expiry":100,
                "token_id":"0xchild",
                "registry":"0xchildregistry",
                "registry_contract_instance_id":CHILD_REGISTRY_INSTANCE,
                "parent_logical_name_id":PARENT
            }),
            json!({"fixture":"ancestor-expiry-child-registration"}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some(FORMER_CHILD),
            None,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            json!({
                "status":"registered",
                "expiry":500,
                "token_id":"0xformer",
                "registry":"0xformerregistry",
                "registry_contract_instance_id":FORMER_REGISTRY_INSTANCE,
                "parent_logical_name_id":PARENT
            }),
            json!({"fixture":"ancestor-expiry-former-registration"}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some(RELEASED_CHILD),
            None,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            json!({
                "status":"registered",
                "expiry":500,
                "token_id":"0xreleased",
                "registry":"0xchildregistry",
                "registry_contract_instance_id":CHILD_REGISTRY_INSTANCE,
                "parent_logical_name_id":PARENT
            }),
            json!({"fixture":"ancestor-expiry-current-registry-released-registration"}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some(RELEASED_CHILD),
            Some(RELEASED_CHILD_RESOURCE),
            "RegistrationGranted",
            "ens_v2_registry_l1",
            json!({
                "status":"registered",
                "expiry":500,
                "token_id":"0xreleased",
                "registry":"0xchildregistry",
                "registry_contract_instance_id":CHILD_REGISTRY_INSTANCE,
                "parent_logical_name_id":PARENT
            }),
            json!({"fixture":"ancestor-expiry-current-registry-linked-registration"}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some(RELEASED_CHILD),
            Some(RELEASED_CHILD_RESOURCE),
            "RegistrationReleased",
            "ens_v2_registry_l1",
            json!({
                "source_event":"LabelUnregistered",
                "terminal_reason":"registry_name_binding_changed",
                "status":"released",
                "token_id":"0xreleased",
                "registry_contract_instance_id":CHILD_REGISTRY_INSTANCE
            }),
            json!({"fixture":"ancestor-expiry-current-registry-released"}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some(FORMER_CHILD),
            None,
            "RegistrationReleased",
            "ens_v2_registry_l1",
            json!({
                "source_event":"LabelUnregistered",
                "sender":OWNER,
                "token_id":"0xformer",
                "registry_contract_instance_id":FORMER_REGISTRY_INSTANCE
            }),
            json!({"fixture":"ancestor-expiry-former-release"}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some(GRANDCHILD),
            None,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            json!({
                "status":"registered",
                "expiry":150,
                "token_id":"0xgrandchild",
                "registry":"0xgrandchildregistry",
                "registry_contract_instance_id":GRANDCHILD_REGISTRY_INSTANCE,
                "parent_logical_name_id":CHILD
            }),
            json!({"fixture":"ancestor-expiry-grandchild-registration"}),
        )
        .await?;
        if child_registration_kind == "RegistrationGranted" {
            insert_event(
                pool,
                CHAIN,
                1,
                Some(CHILD),
                None,
                "RegistrationGranted",
                "ens_v2_registry_l1",
                json!({
                    "status":"registered",
                    "expiry":90,
                    "token_id":"0xchild-released",
                    "registry":"0xchildregistry",
                    "registry_contract_instance_id":CHILD_REGISTRY_INSTANCE,
                    "parent_logical_name_id":PARENT
                }),
                json!({"fixture":"ancestor-expiry-child-released-registration"}),
            )
            .await?;
            insert_event(
                pool,
                CHAIN,
                1,
                Some(CHILD),
                None,
                "RegistrationReleased",
                "ens_v2_registry_l1",
                json!({
                    "source_event":"LabelUnregistered",
                    "sender":OWNER,
                    "token_id":"0xchild-released",
                    "registry_contract_instance_id":CHILD_REGISTRY_INSTANCE
                }),
                json!({"fixture":"ancestor-expiry-child-released"}),
            )
            .await?;
        }
        insert_event(
            pool,
            CHAIN,
            1,
            Some(SENTINEL),
            None,
            "RegistrationGranted",
            "ens_v2_root_l1",
            json!({
                "status":"registered",
                "expiry":500,
                "token_id":"0xunrelated",
                "registry":"0xrootregistry"
            }),
            json!({"fixture":"unrelated-scope-sentinel"}),
        )
        .await?;
        for (logical_name_id, token_id, registry, expiry) in [
            (PARENT, "0xparent", "0xrootregistry", 20),
            (CHILD, "0xchild", "0xchildregistry", 100),
            (GRANDCHILD, "0xgrandchild", "0xgrandchildregistry", 150),
        ] {
            insert_event(
                pool,
                CHAIN,
                2,
                Some(logical_name_id),
                (logical_name_id == CHILD && child_registration_kind == "RegistrationReserved")
                    .then_some(CHILD_RESERVED_RESOURCE),
                "RegistrationReleased",
                "ens_v2_registry_l1",
                json!({
                    "source_event":"RegistryPathExpired",
                    "derived_from":"interpreter_state",
                    "terminal_reason":"registry_name_binding_expired",
                    "status":"released",
                    "expiry":expiry,
                    "token_id":token_id,
                    "registry":registry,
                    "registry_contract_instance_id":match logical_name_id {
                        CHILD => Value::String(CHILD_REGISTRY_INSTANCE.into()),
                        GRANDCHILD => Value::String(GRANDCHILD_REGISTRY_INSTANCE.into()),
                        _ => Value::Null,
                    }
                }),
                json!({"fixture":"ancestor-expiry-release"}),
            )
            .await?;
        }
        insert_event(
            pool,
            CHAIN,
            2,
            Some(PARENT),
            None,
            "SubregistryChanged",
            "ens_v2_root_l1",
            json!({
                "source_event":"RegistryPathExpired",
                "derived_from":"interpreter_state",
                "terminal_reason":"registry_name_binding_expired",
                "expiry":20,
                "subregistry":null
            }),
            json!({"fixture":"ancestor-expiry-subregistry-release"}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            2,
            Some(CHILD),
            (child_registration_kind == "RegistrationReserved").then_some(CHILD_RESERVED_RESOURCE),
            "SubregistryChanged",
            "ens_v2_registry_l1",
            json!({
                "source_event":"RegistryPathExpired",
                "derived_from":"interpreter_state",
                "terminal_reason":"registry_name_binding_expired",
                "expiry":100,
                "subregistry":null
            }),
            json!({"fixture":"ancestor-expiry-grandchild-subregistry-release"}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 1).await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 1,
            hash: block_hash(CHAIN, 1),
        }),
        RunMode::Normal,
        2,
        2,
    )
    .await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 2,
            hash: block_hash(CHAIN, 2),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    let expired_child_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_current WHERE logical_name_id = $1")
            .bind(CHILD)
            .fetch_one(incremental.pool())
            .await?;
    assert_eq!(expired_child_count, 0);
    let expired_grandchild_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_current WHERE logical_name_id = $1")
            .bind(GRANDCHILD)
            .fetch_one(incremental.pool())
            .await?;
    assert_eq!(expired_grandchild_count, 0);

    for pool in [incremental.pool(), fresh.pool()] {
        sqlx::query(
            "UPDATE chain_lineage SET canonicality_state = 'orphaned'
             WHERE chain_id = $1 AND block_number BETWEEN 2 AND 3",
        )
        .bind(CHAIN)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $4, 2, to_timestamp(15), 'canonical'),
                      ($1, $3, $2, 3, to_timestamp(16), 'canonical')",
        )
        .bind(CHAIN)
        .bind(REPLACEMENT_TWO)
        .bind(REPLACEMENT_THREE)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        let interpret = InterpretEngine::new(pool.clone())
            .run_batch(InterpretRequest {
                chain_id: CHAIN.into(),
                from_block: 2,
                to_block: 3,
                resume_current: None,
                mode: InterpretRunMode::Redo,
            })
            .await?;
        assert!(interpret.complete);
        if let Some((renewal_block, expiry)) = replacement_parent_renewal {
            insert_event(
                pool,
                CHAIN,
                renewal_block,
                Some(PARENT),
                None,
                "RegistrationRenewed",
                "ens_v2_root_l1",
                json!({
                    "expiry":expiry,
                    "token_id":"0xparent",
                    "registry":"0xrootregistry"
                }),
                json!({"fixture":"replacement-parent-renewal"}),
            )
            .await?;
            sqlx::query(
                "UPDATE normalized_events SET block_hash = $1
                 WHERE chain_id = $2
                   AND block_number = $3
                   AND raw_fact_ref ->> 'fixture' = 'replacement-parent-renewal'",
            )
            .bind(if renewal_block == 2 {
                REPLACEMENT_TWO
            } else {
                REPLACEMENT_THREE
            })
            .bind(CHAIN)
            .bind(renewal_block)
            .execute(pool)
            .await?;
        }
    }

    sqlx::query(
        "UPDATE name_current SET raw_name = 'sentinel-unchanged.eth' WHERE logical_name_id = $1",
    )
    .bind(SENTINEL)
    .execute(incremental.pool())
    .await?;
    sqlx::query(
        "UPDATE name_current SET raw_name = 'former-sentinel-unchanged.eth'
         WHERE logical_name_id = $1",
    )
    .bind(FORMER_CHILD)
    .execute(incremental.pool())
    .await?;
    sqlx::query(
        "UPDATE name_current SET raw_name = 'released-sentinel-unchanged.eth'
         WHERE logical_name_id = $1",
    )
    .bind(RELEASED_CHILD)
    .execute(incremental.pool())
    .await?;

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: REPLACEMENT_THREE.into(),
        }),
        RunMode::Redo,
        2,
        if replacement_parent_renewal.is_some_and(|(block, _)| block > 2) {
            2
        } else {
            3
        },
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    for descendant in [CHILD, GRANDCHILD] {
        let incremental_descendant: Option<Value> = sqlx::query_scalar(
            "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
        )
        .bind(descendant)
        .fetch_optional(incremental.pool())
        .await?;
        let fresh_descendant: Option<Value> = sqlx::query_scalar(
            "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
        )
        .bind(descendant)
        .fetch_optional(fresh.pool())
        .await?;
        assert!(fresh_descendant.is_some());
        assert_eq!(
            incremental_descendant, fresh_descendant,
            "redo failed to restore descendant {descendant} removed by ancestor expiry"
        );
    }
    let incremental_edge: Option<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM children_current current
         WHERE parent_logical_name_id = $1 AND child_logical_name_id = $2",
    )
    .bind(CHILD)
    .bind(GRANDCHILD)
    .fetch_optional(incremental.pool())
    .await?;
    let fresh_edge: Option<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM children_current current
         WHERE parent_logical_name_id = $1 AND child_logical_name_id = $2",
    )
    .bind(CHILD)
    .bind(GRANDCHILD)
    .fetch_optional(fresh.pool())
    .await?;
    assert!(fresh_edge.is_some());
    assert_eq!(
        incremental_edge, fresh_edge,
        "redo failed to restore the recovered descendant's child edge"
    );
    if child_registration_kind == "RegistrationReserved" {
        for pool in [incremental.pool(), fresh.pool()] {
            let reserved_edge_count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM children_current
                 WHERE parent_logical_name_id = $1 AND child_logical_name_id = $2",
            )
            .bind(PARENT)
            .bind(CHILD)
            .fetch_one(pool)
            .await?;
            assert_eq!(reserved_edge_count, 0);
        }
    }
    let sentinel_name: String =
        sqlx::query_scalar("SELECT raw_name FROM name_current WHERE logical_name_id = $1")
            .bind(SENTINEL)
            .fetch_one(incremental.pool())
            .await?;
    assert_eq!(sentinel_name, "sentinel-unchanged.eth");
    let former_name: String =
        sqlx::query_scalar("SELECT raw_name FROM name_current WHERE logical_name_id = $1")
            .bind(FORMER_CHILD)
            .fetch_one(incremental.pool())
            .await?;
    assert_eq!(former_name, "former-sentinel-unchanged.eth");
    let released_name: String =
        sqlx::query_scalar("SELECT raw_name FROM name_current WHERE logical_name_id = $1")
            .bind(RELEASED_CHILD)
            .fetch_one(incremental.pool())
            .await?;
    assert_eq!(released_name, "released-sentinel-unchanged.eth");

    for pool in [incremental.pool(), fresh.pool()] {
        let expiry_handoff_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM project_redo_expiry_roots WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            expiry_handoff_rows, 0,
            "Project publication did not consume the path-expiry name handoff"
        );
    }

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn expiry_release_redo_restores_deleted_permissions_like_fresh_rebuild() -> Result<()> {
    let incremental = ScratchDatabase::create("project_expiry_permission_redo").await?;
    let fresh = ScratchDatabase::create("project_expiry_permission_redo_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_expiry_release_redo_fixture(pool).await?;
    }
    publish_and_retract_expiry_fixture(incremental.pool()).await?;
    assert_eq!(expiry_fixture_counts(incremental.pool()).await?.1, 0);
    for pool in [incremental.pool(), fresh.pool()] {
        seed_later_expiry_fixture_event(pool).await?;
    }
    advance_expiry_fixture_beyond_redo(incremental.pool()).await?;
    assert_eq!(expiry_fixture_counts(incremental.pool()).await?.1, 0);
    for pool in [incremental.pool(), fresh.pool()] {
        orphan_expiry_release_and_reopen_binding(pool).await?;
    }
    run_project(
        incremental.pool(),
        "project-expiry-release-redo",
        Some(Marker {
            number: 3,
            hash: block_hash("project-expiry-release-redo", 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    run_project(
        fresh.pool(),
        "project-expiry-release-redo",
        None,
        RunMode::Normal,
        0,
        3,
    )
    .await?;
    assert_eq!(expiry_fixture_counts(fresh.pool()).await?.1, 1);
    assert_eq!(
        expiry_fixture_permissions(incremental.pool()).await?,
        expiry_fixture_permissions(fresh.pool()).await?,
        "redo failed to restore the reopened resource permissions"
    );
    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn expiry_release_subrange_redo_removes_revival_only_provenance_like_fresh_rebuild()
-> Result<()> {
    let incremental = ScratchDatabase::create("project_expiry_revival_subrange_redo").await?;
    let fresh = ScratchDatabase::create("project_expiry_revival_subrange_redo_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_expiry_revival_subrange_fixture(pool).await?;
    }
    run_project(
        incremental.pool(),
        EXPIRY_REDO_CHAIN,
        None,
        RunMode::Normal,
        0,
        3,
    )
    .await?;
    let revived: i64 =
        sqlx::query_scalar("SELECT count(*) FROM permissions_current WHERE resource_id = $1")
            .bind(Uuid::parse_str(EXPIRY_REDO_RESOURCE)?)
            .fetch_one(incremental.pool())
            .await?;
    assert_eq!(revived, 1, "fixture must revive the retired permission");

    sqlx::query(
        "INSERT INTO project_redo_expiry_roots (
             chain_id, event_identity, block_number, logical_name_id, resource_id
         )
         SELECT chain_id, event_identity, block_number, logical_name_id, resource_id
         FROM normalized_events
         WHERE chain_id = $1 AND block_number = 2
           AND event_kind = 'RegistrationReleased'",
    )
    .bind(EXPIRY_REDO_CHAIN)
    .execute(incremental.pool())
    .await?;
    for pool in [incremental.pool(), fresh.pool()] {
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND block_number = 2
               AND event_kind = 'RegistrationReleased'",
        )
        .bind(EXPIRY_REDO_CHAIN)
        .execute(pool)
        .await?;
    }

    run_project(
        incremental.pool(),
        EXPIRY_REDO_CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(EXPIRY_REDO_CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    run_project(fresh.pool(), EXPIRY_REDO_CHAIN, None, RunMode::Normal, 0, 3).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    let incremental_snapshot =
        summary_only_permission_snapshot(incremental.pool(), EXPIRY_REDO_RESOURCE).await?;
    let fresh_snapshot =
        summary_only_permission_snapshot(fresh.pool(), EXPIRY_REDO_RESOURCE).await?;
    assert_eq!(fresh_snapshot.0.as_array().map(Vec::len), Some(1));
    assert_eq!(
        incremental_snapshot, fresh_snapshot,
        "subrange redo retained revival-only permission provenance after its release was retracted"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn project_only_subrange_redo_scopes_resource_only_orphaned_expiry_release() -> Result<()> {
    let incremental = ScratchDatabase::create("project_expiry_resource_only_project_redo").await?;
    let fresh = ScratchDatabase::create("project_expiry_resource_only_project_redo_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_expiry_revival_subrange_fixture(pool).await?;
    }
    run_project(
        incremental.pool(),
        EXPIRY_REDO_CHAIN,
        None,
        RunMode::Normal,
        0,
        3,
    )
    .await?;
    for pool in [incremental.pool(), fresh.pool()] {
        sqlx::query(
            "UPDATE normalized_events SET canonicality_state = 'orphaned'
             WHERE chain_id = $1
               AND raw_fact_ref ->> 'fixture' = 'expiry-redo-release'",
        )
        .bind(EXPIRY_REDO_CHAIN)
        .execute(pool)
        .await?;
    }

    run_project(
        incremental.pool(),
        EXPIRY_REDO_CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(EXPIRY_REDO_CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    run_project(fresh.pool(), EXPIRY_REDO_CHAIN, None, RunMode::Normal, 0, 3).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        summary_only_permission_snapshot(incremental.pool(), EXPIRY_REDO_RESOURCE).await?,
        summary_only_permission_snapshot(fresh.pool(), EXPIRY_REDO_RESOURCE).await?,
        "Project-only redo did not rebuild the resource from its orphaned expiry release"
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn expiry_release_redo_restores_ownerless_reservation_like_fresh_rebuild() -> Result<()> {
    const CHAIN: &str = "project-ownerless-expiry-redo";
    const NAME: &str = "ens:0x8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d";
    const RESOURCE: &str = "00000000-0000-0000-0000-0000000008d1";
    let incremental = ScratchDatabase::create("project_ownerless_expiry_redo").await?;
    let fresh = ScratchDatabase::create("project_ownerless_expiry_redo_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_lineage(pool, CHAIN, 3).await?;
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
                 namehash, labelhashes, normalizer_version, visibility_state,
                 chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, 'ens', 'ownerless.eth', ARRAY['ownerless','eth'],
                 decode('00','hex'), $2, ARRAY['0xownerless','0xeth'], $3,
                 'active', $4, $5, 1, 'canonical')",
        )
        .bind(NAME)
        .bind(NAME.trim_start_matches("ens:"))
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, $2, $3, 1, 'canonical')",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some(NAME),
            Some(RESOURCE),
            "RegistrationReserved",
            "ens_v2_registry_l1",
            json!({
                "status":"reserved",
                "expiry":2,
                "token_id":"0x8d",
                "reservation_resource":true
            }),
            json!({"fixture":"ownerless-expiry-reservation"}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            1,
            Some(NAME),
            Some(RESOURCE),
            "ExpiryChanged",
            "ens_v2_registry_l1",
            json!({"source_event":"ExpiryUpdated","expiry":2,"token_id":"0x8d"}),
            json!({"fixture":"ownerless-expiry-renewal"}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            2,
            Some(NAME),
            Some(RESOURCE),
            "RegistrationReleased",
            "ens_v2_registry_l1",
            json!({
                "source_event":"RegistryPathExpired",
                "derived_from":"interpreter_state",
                "terminal_reason":"registry_name_binding_expired",
                "status":"released"
            }),
            json!({"fixture":"ownerless-expiry-release"}),
        )
        .await?;
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 1).await?;
    let live_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_current WHERE logical_name_id = $1")
            .bind(NAME)
            .fetch_one(incremental.pool())
            .await?;
    assert_eq!(live_count, 1);
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 1,
            hash: block_hash(CHAIN, 1),
        }),
        RunMode::Normal,
        2,
        2,
    )
    .await?;
    let expired_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_current WHERE logical_name_id = $1")
            .bind(NAME)
            .fetch_one(incremental.pool())
            .await?;
    assert_eq!(expired_count, 0);
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 2,
            hash: block_hash(CHAIN, 2),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    for pool in [incremental.pool(), fresh.pool()] {
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND block_number = 2
               AND event_kind = 'RegistrationReleased'",
        )
        .bind(CHAIN)
        .execute(pool)
        .await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    let incremental_row: Option<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(NAME)
    .fetch_optional(incremental.pool())
    .await?;
    let fresh_row: Option<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(NAME)
    .fetch_optional(fresh.pool())
    .await?;
    assert!(fresh_row.is_some());
    assert_eq!(incremental_row, fresh_row);
    incremental.cleanup().await?;
    fresh.cleanup().await
}

#[tokio::test]
async fn redo_restores_summary_only_permission_retraction_like_fresh_rebuild() -> Result<()> {
    const CHAIN: &str = "project-summary-only-permission-redo";
    const RESOURCE: &str = "00000000-0000-0000-0000-0000000008c1";
    let incremental = ScratchDatabase::create("project_summary_only_permission_redo").await?;
    let fresh = ScratchDatabase::create("project_summary_only_permission_redo_fresh").await?;
    for pool in [incremental.pool(), fresh.pool()] {
        seed_lineage(pool, CHAIN, 3).await?;
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, $2, $3, 1, 'canonical')",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
        for (block, powers) in [(1, json!(["resource_control"])), (2, json!([]))] {
            insert_event(
                pool,
                CHAIN,
                block,
                None,
                Some(RESOURCE),
                "PermissionChanged",
                "ens_v2_registry_l1",
                json!({
                    "subject":OWNER,
                    "scope":{"kind":"resource"},
                    "effective_powers":powers,
                    "grant_source":{"kind":"fixture"},
                    "revocation_source":{"kind":"fixture"},
                    "inheritance_path":[],
                    "transfer_behavior":"retain"
                }),
                json!({"fixture":format!("summary-only-{block}")}),
            )
            .await?;
        }
    }
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 2).await?;
    let retired_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM permissions_current WHERE resource_id = $1")
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(incremental.pool())
            .await?;
    assert_eq!(retired_count, 0);
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let republished_target: i64 = sqlx::query_scalar(
        "SELECT (chain_positions ->> 'target_block_number')::bigint
         FROM permissions_current_resource_summary WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(republished_target, 3);
    for pool in [incremental.pool(), fresh.pool()] {
        sqlx::query(
            "DELETE FROM normalized_events
             WHERE chain_id = $1 AND resource_id = $2 AND block_number = 2",
        )
        .bind(CHAIN)
        .bind(Uuid::parse_str(RESOURCE)?)
        .execute(pool)
        .await?;
    }
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;
    run_project(fresh.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    let incremental_snapshot =
        summary_only_permission_snapshot(incremental.pool(), RESOURCE).await?;
    let fresh_snapshot = summary_only_permission_snapshot(fresh.pool(), RESOURCE).await?;
    assert_eq!(fresh_snapshot.0.as_array().map(Vec::len), Some(1));
    assert_eq!(
        incremental_snapshot, fresh_snapshot,
        "redo retained a summary-only revoked permission state"
    );
    incremental.cleanup().await?;
    fresh.cleanup().await
}

async fn summary_only_permission_snapshot(pool: &PgPool, resource: &str) -> Result<(Value, Value)> {
    let resource = Uuid::parse_str(resource)?;
    let permissions = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY subject, scope), '[]'::jsonb)
         FROM permissions_current row WHERE resource_id = $1",
    )
    .bind(resource)
    .fetch_one(pool)
    .await?;
    let summary = sqlx::query_scalar(
        "SELECT to_jsonb(row) FROM permissions_current_resource_summary row
         WHERE resource_id = $1",
    )
    .bind(resource)
    .fetch_one(pool)
    .await?;
    Ok((permissions, summary))
}

async fn run_project(
    pool: &PgPool,
    chain_id: &str,
    resume_current: Option<Marker>,
    mode: RunMode,
    affected_from_block: i64,
    affected_to_block: i64,
) -> Result<()> {
    let target_block = resume_current.as_ref().map_or(affected_to_block, |marker| {
        marker.number.max(affected_to_block)
    });
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: chain_id.into(),
            target_block,
            affected_from_block,
            affected_to_block,
            resume_current,
            mode,
        })
        .await?;
    assert!(outcome.complete);
    Ok(())
}

const EXPIRY_REDO_CHAIN: &str = "project-expiry-release-redo";
const EXPIRY_REDO_NAME: &str =
    "ens:0x8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b";
const EXPIRY_REDO_RESOURCE: &str = "00000000-0000-0000-0000-0000000008b1";
const EXPIRY_REDO_BINDING: &str = "00000000-0000-0000-0000-0000000008b2";
const EXPIRY_REDO_CURRENT_RESOURCE: &str = "00000000-0000-0000-0000-0000000008c1";
const EXPIRY_REDO_CURRENT_BINDING: &str = "00000000-0000-0000-0000-0000000008c2";

async fn seed_expiry_revival_subrange_fixture(pool: &PgPool) -> Result<()> {
    seed_expiry_release_redo_fixture(pool).await?;
    sqlx::query(
        "UPDATE normalized_events SET logical_name_id = NULL
         WHERE chain_id = $1 AND raw_fact_ref ->> 'fixture' = 'expiry-redo-release'",
    )
    .bind(EXPIRY_REDO_CHAIN)
    .execute(pool)
    .await?;
    insert_classifier_resource_and_binding(
        pool,
        EXPIRY_REDO_CHAIN,
        EXPIRY_REDO_NAME,
        "ens_v2",
        Uuid::parse_str(EXPIRY_REDO_CURRENT_RESOURCE)?,
        Uuid::parse_str(EXPIRY_REDO_CURRENT_BINDING)?,
        3,
        None,
    )
    .await?;
    insert_event(
        pool,
        EXPIRY_REDO_CHAIN,
        3,
        Some(EXPIRY_REDO_NAME),
        Some(EXPIRY_REDO_CURRENT_RESOURCE),
        "RegistrationGranted",
        "ens_v2_registry_l1",
        json!({
            "source_event":"TokenResource",
            "status":"registered",
            "expiry":4_000_000_000_u64,
            "token_id":"0x02"
        }),
        json!({"fixture":"expiry-redo-current-holder"}),
    )
    .await?;
    insert_event(
        pool,
        EXPIRY_REDO_CHAIN,
        3,
        Some(EXPIRY_REDO_NAME),
        Some(EXPIRY_REDO_RESOURCE),
        "RegistrationRenewed",
        "ens_v2_registry_l1",
        json!({
            "source_event":"ExpiryUpdated",
            "status":"registered",
            "expiry":4_000_000_000_u64,
            "token_id":"0x01",
            "revived_from_expiry":true
        }),
        json!({"fixture":"expiry-redo-later-revival"}),
    )
    .await?;
    Ok(())
}

async fn seed_expiry_release_redo_fixture(pool: &PgPool) -> Result<()> {
    seed_lineage(pool, EXPIRY_REDO_CHAIN, 3).await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, 'ens', 'redo.eth', ARRAY['redo','eth'], decode('00','hex'),
             $2, ARRAY['0xredo','0xeth'], $3, 'active', $4, $5, 1, 'canonical')",
    )
    .bind(EXPIRY_REDO_NAME)
    .bind(EXPIRY_REDO_NAME.trim_start_matches("ens:"))
    .bind(NORMALIZER)
    .bind(EXPIRY_REDO_CHAIN)
    .bind(block_hash(EXPIRY_REDO_CHAIN, 1))
    .execute(pool)
    .await?;
    insert_classifier_resource_and_binding(
        pool,
        EXPIRY_REDO_CHAIN,
        EXPIRY_REDO_NAME,
        "ens_v2",
        Uuid::parse_str(EXPIRY_REDO_RESOURCE)?,
        Uuid::parse_str(EXPIRY_REDO_BINDING)?,
        1,
        None,
    )
    .await?;
    insert_event(
        pool,
        EXPIRY_REDO_CHAIN,
        1,
        Some(EXPIRY_REDO_NAME),
        Some(EXPIRY_REDO_RESOURCE),
        "RegistrationGranted",
        "ens_v2_registry_l1",
        json!({"status":"registered","token_id":"0x01","registry":"0xregistry"}),
        json!({"fixture":"expiry-redo-registration"}),
    )
    .await?;
    insert_event(
        pool,
        EXPIRY_REDO_CHAIN,
        1,
        Some(EXPIRY_REDO_NAME),
        Some(EXPIRY_REDO_RESOURCE),
        "PermissionChanged",
        "ens_v2_registry_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"retain"
        }),
        json!({"fixture":"expiry-redo-permission"}),
    )
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(2) WHERE surface_binding_id = $1",
    )
    .bind(Uuid::parse_str(EXPIRY_REDO_BINDING)?)
    .execute(pool)
    .await?;
    insert_event(
        pool,
        EXPIRY_REDO_CHAIN,
        2,
        Some(EXPIRY_REDO_NAME),
        Some(EXPIRY_REDO_RESOURCE),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({
            "source_event":"RegistryPathExpired",
            "derived_from":"interpreter_state",
            "terminal_reason":"registry_name_binding_expired",
            "status":"released"
        }),
        json!({"fixture":"expiry-redo-release"}),
    )
    .await?;
    Ok(())
}

async fn seed_later_expiry_fixture_event(pool: &PgPool) -> Result<()> {
    insert_event(
        pool,
        EXPIRY_REDO_CHAIN,
        3,
        Some(EXPIRY_REDO_NAME),
        Some(EXPIRY_REDO_RESOURCE),
        "PermissionChanged",
        "ens_v2_registry_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control"],
            "grant_source":{"kind":"fixture","refresh":true},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"retain"
        }),
        json!({"fixture":"expiry-redo-later-permission"}),
    )
    .await
}

async fn advance_expiry_fixture_beyond_redo(pool: &PgPool) -> Result<()> {
    run_project(
        pool,
        EXPIRY_REDO_CHAIN,
        Some(Marker {
            number: 2,
            hash: block_hash(EXPIRY_REDO_CHAIN, 2),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await
}

async fn publish_and_retract_expiry_fixture(pool: &PgPool) -> Result<()> {
    run_project(pool, EXPIRY_REDO_CHAIN, None, RunMode::Normal, 0, 1).await?;
    assert_eq!(expiry_fixture_counts(pool).await?, (1, 1));
    run_project(
        pool,
        EXPIRY_REDO_CHAIN,
        Some(Marker {
            number: 1,
            hash: block_hash(EXPIRY_REDO_CHAIN, 1),
        }),
        RunMode::Normal,
        2,
        2,
    )
    .await?;
    Ok(())
}

async fn orphan_expiry_release_and_reopen_binding(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND block_number = 2
           AND event_kind = 'RegistrationReleased'",
    )
    .bind(EXPIRY_REDO_CHAIN)
    .execute(pool)
    .await?;
    sqlx::query("UPDATE surface_bindings SET active_to = NULL WHERE surface_binding_id = $1")
        .bind(Uuid::parse_str(EXPIRY_REDO_BINDING)?)
        .execute(pool)
        .await?;
    Ok(())
}

async fn expiry_fixture_counts(pool: &PgPool) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT (SELECT count(*) FROM name_current WHERE logical_name_id = $1),
                (SELECT count(*) FROM permissions_current WHERE resource_id = $2)",
    )
    .bind(EXPIRY_REDO_NAME)
    .bind(Uuid::parse_str(EXPIRY_REDO_RESOURCE)?)
    .fetch_one(pool)
    .await?)
}

async fn expiry_fixture_name(pool: &PgPool) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(current) - 'last_recomputed_at' - 'inserted_at' -
                'chain_positions' - 'canonicality_summary'
         FROM name_current current WHERE logical_name_id = $1",
    )
    .bind(EXPIRY_REDO_NAME)
    .fetch_optional(pool)
    .await?)
}

async fn expiry_fixture_permissions(pool: &PgPool) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(current) - 'last_recomputed_at' - 'inserted_at' -
                'chain_positions' - 'canonicality_summary'
         FROM permissions_current current WHERE resource_id = $1
         ORDER BY subject, scope",
    )
    .bind(Uuid::parse_str(EXPIRY_REDO_RESOURCE)?)
    .fetch_all(pool)
    .await?)
}

#[tokio::test]
async fn mainnet_dual_current_aborts_the_generation_and_appends_one_audit_row() -> Result<()> {
    let scratch = ScratchDatabase::create("project_dual_current_audit").await?;
    let chain = "project-dual-current-audit";
    let logical_name_id = seed_mainnet_dual_current_conflict(scratch.pool(), chain).await?;

    let failure = run_project_phase(scratch.pool(), chain, 5)
        .await
        .expect_err("a mainnet dual-current name must not publish");
    assert!(
        failure.to_string().contains("both authority arms"),
        "unexpected failure: {failure}"
    );

    let published: i64 = sqlx::query_scalar("SELECT count(*) FROM name_current")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(
        published, 0,
        "an aborted generation publishes no partial rows"
    );

    let rows = generation_failure_rows(scratch.pool(), chain).await?;
    assert_eq!(rows.len(), 1);
    let (target_block, target_hash, content_hash, failure_kind, fingerprint, name, evidence) =
        rows[0].clone();
    assert_eq!(target_block, 5);
    assert_eq!(target_hash, block_hash(chain, 5));
    assert_eq!(content_hash, INTERPRETER_CONTENT_HASH);
    assert_eq!(failure_kind, DUAL_CURRENT_EXACT_NAME_AUTHORITY);
    assert_eq!(fingerprint.len(), 64);
    assert_eq!(name, logical_name_id);
    assert_eq!(evidence["predecessor"]["authority_arm"], json!("ens_v1"));
    assert_eq!(evidence["successor"]["authority_arm"], json!("ens_v2"));
    assert_eq!(
        evidence["target"]["block_hash"],
        json!(block_hash(chain, 5))
    );
    assert!(evidence["boundary"]["event_identity"].is_string());
    assert!(evidence["boundary"]["block_hash"].is_string());
    for arm in ["predecessor", "successor"] {
        assert!(evidence[arm]["surface_binding_id"].is_string(), "{arm}");
        assert!(evidence[arm]["resource_id"].is_string(), "{arm}");
        assert!(evidence[arm]["block_number"].is_number(), "{arm}");
        assert!(evidence[arm]["canonicality_state"].is_string(), "{arm}");
    }

    let retry = run_project_phase(scratch.pool(), chain, 5)
        .await
        .expect_err("the retried generation still fails");
    assert!(retry.to_string().contains("both authority arms"));
    assert_eq!(
        generation_failure_rows(scratch.pool(), chain).await?,
        rows,
        "a retried generation records no second row for the same conflict"
    );

    scratch.cleanup().await
}

// The one ON CONFLICT semantic the storage contract promises: a retry at the
// same target appends a different name's conflict instead of swallowing it.
#[tokio::test]
async fn a_second_conflicting_name_appends_its_own_evidence_at_the_same_target() -> Result<()> {
    let scratch = ScratchDatabase::create("project_dual_current_second_name").await?;
    let chain = "project-dual-current-second-name";
    let first = seed_mainnet_dual_current_conflict(scratch.pool(), chain).await?;
    let second = clone_dual_current_conflict(scratch.pool(), chain, &first, "bob").await?;

    run_project_phase(scratch.pool(), chain, 5)
        .await
        .expect_err("two conflicting names still block publication");
    let recorded = generation_failure_rows(scratch.pool(), chain).await?;
    assert_eq!(recorded.len(), 1, "one failed generation writes one row");
    let blocked = recorded[0].5.clone();
    let lexicographically_first = first.clone().min(second.clone());
    assert_eq!(
        blocked, lexicographically_first,
        "the recorded conflict is deterministic"
    );

    // Resolve only the recorded name; the other conflict remains.
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(4)
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v1'",
    )
    .bind(chain)
    .bind(&blocked)
    .execute(scratch.pool())
    .await?;

    run_project_phase(scratch.pool(), chain, 5)
        .await
        .expect_err("the remaining conflict still blocks the same target");
    let appended = generation_failure_rows(scratch.pool(), chain).await?;
    assert_eq!(
        appended.len(),
        2,
        "a different conflict at the same target appends its own evidence"
    );
    let names: Vec<String> = appended.iter().map(|row| row.5.clone()).collect();
    assert!(
        names.contains(&first) && names.contains(&second),
        "{names:?}"
    );
    assert!(
        appended.iter().all(|row| row.0 == 5),
        "both rows belong to the same target"
    );
    assert_ne!(
        appended[0].4, appended[1].4,
        "distinct conflicts fingerprint distinctly"
    );

    scratch.cleanup().await
}

// A failed audit write must not mask or downgrade the invariant diagnosis: the
// runner has to stop on the integrity failure, not retry it as transient.
#[tokio::test]
async fn a_failed_audit_write_keeps_the_non_retryable_invariant_failure() -> Result<()> {
    let scratch = ScratchDatabase::create("project_dual_current_audit_unwritable").await?;
    let chain = "project-dual-current-unwritable";
    seed_mainnet_dual_current_conflict(scratch.pool(), chain).await?;
    sqlx::query("DROP TABLE project_generation_failures")
        .execute(scratch.pool())
        .await?;

    let failure = run_project_phase(scratch.pool(), chain, 5)
        .await
        .expect_err("the generation still fails");
    assert_eq!(
        failure.kind(),
        RunnerErrorKind::DataIntegrity,
        "an unwritable audit must not make the failure retryable"
    );
    let message = failure.to_string();
    assert!(
        message.contains("both authority arms"),
        "the invariant diagnosis survives: {message}"
    );
    assert!(
        message.contains("record projection generation failure"),
        "the audit failure is reported alongside: {message}"
    );

    scratch.cleanup().await
}

// A post-audit Sepolia manifest declared for a different chain must not
// reclassify this one: the deployment profile is per projected chain.
#[tokio::test]
async fn a_foreign_chain_sepolia_manifest_keeps_the_mainnet_profile() -> Result<()> {
    let scratch = ScratchDatabase::create("project_dual_current_foreign_label").await?;
    let chain = "project-dual-current-foreign";
    seed_mainnet_dual_current_conflict(scratch.pool(), chain).await?;
    declare_sepolia_post_audit_profile(scratch.pool(), "project-dual-current-elsewhere").await?;

    let failure = run_project_phase(scratch.pool(), chain, 5)
        .await
        .expect_err("a foreign sepolia label must not disable the assertion");
    assert!(
        failure.to_string().contains("both authority arms"),
        "unexpected failure: {failure}"
    );
    assert_eq!(
        generation_failure_rows(scratch.pool(), chain).await?.len(),
        1,
        "the mainnet assertion still records its evidence"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn a_foreign_chain_sepolia_manifest_keeps_the_mainnet_mixed_corpus_reason() -> Result<()> {
    let scratch = ScratchDatabase::create("project_dual_current_foreign_reason").await?;
    let chain = "project-dual-current-foreign-reason";
    let logical_name_id = seed_dual_open_cross_arm_fixture(scratch.pool(), chain, 4).await?;
    declare_sepolia_post_audit_profile(scratch.pool(), "project-dual-current-elsewhere").await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;

    run_project_phase(scratch.pool(), chain, 5).await?;

    let reason: Option<String> = sqlx::query_scalar(
        "SELECT unsupported_reason FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        reason.as_deref(),
        Some("conflicting_current_ens_authority"),
        "a proofless mainnet mixed corpus keeps its own reason"
    );

    scratch.cleanup().await
}

// The false-positive guard: a Mainnet name whose predecessor binding closed at
// the boundary is the ordinary migrated shape and must keep publishing.
#[tokio::test]
async fn a_closed_predecessor_publishes_on_mainnet_without_an_audit_row() -> Result<()> {
    let scratch = ScratchDatabase::create("project_dual_current_migrated").await?;
    let chain = "project-dual-current-migrated";
    let logical_name_id = seed_mainnet_dual_current_conflict(scratch.pool(), chain).await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(4)
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v1'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(scratch.pool())
    .await?;

    run_project_phase(scratch.pool(), chain, 5).await?;

    let published: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_current WHERE logical_name_id = $1")
            .bind(&logical_name_id)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(published, 1, "the migrated name still publishes on mainnet");
    assert!(
        generation_failure_rows(scratch.pool(), chain)
            .await?
            .is_empty(),
        "a closed predecessor is not an invariant failure"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn sepolia_profile_publishes_the_same_proven_dual_current_corpus() -> Result<()> {
    let scratch = ScratchDatabase::create("project_dual_current_sepolia").await?;
    let chain = "project-dual-current-sepolia";
    let logical_name_id = seed_dual_open_cross_arm_fixture(scratch.pool(), chain, 4).await?;
    declare_sepolia_post_audit_profile(scratch.pool(), chain).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    insert_activated_authority_proof(scratch.pool(), chain, &logical_name_id, "unwrapped", None)
        .await?;

    run_project_phase(scratch.pool(), chain, 5).await?;

    let published: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_current WHERE logical_name_id = $1")
            .bind(&logical_name_id)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(published, 1, "Sepolia keeps selecting past the boundary");
    assert!(
        generation_failure_rows(scratch.pool(), chain)
            .await?
            .is_empty(),
        "the assertion is Mainnet-scoped"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn generation_failure_audit_survives_orphaning_and_a_later_success() -> Result<()> {
    let scratch = ScratchDatabase::create("project_dual_current_reorg").await?;
    let chain = "project-dual-current-reorg";
    let logical_name_id = seed_mainnet_dual_current_conflict(scratch.pool(), chain).await?;
    run_project_phase(scratch.pool(), chain, 5)
        .await
        .expect_err("the conflicting generation fails");
    let recorded = generation_failure_rows(scratch.pool(), chain).await?;
    assert_eq!(recorded.len(), 1);
    let captured_at_failure = recorded[0].6["predecessor"]["canonicality_state"]
        .as_str()
        .expect("the evidence captures the canonicality observed at failure")
        .to_owned();
    assert!(
        ["canonical", "safe", "finalized"].contains(&captured_at_failure.as_str()),
        "the conflicting binding was readable when the generation failed: \
         {captured_at_failure}"
    );

    // A reorg orphans the conflicting ENSv1 binding, which leaves the staged
    // candidate set and resolves the conflict.
    sqlx::query(
        "UPDATE surface_bindings SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v1'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(scratch.pool())
    .await?;

    run_project_phase(scratch.pool(), chain, 5).await?;

    let published: i64 =
        sqlx::query_scalar("SELECT count(*) FROM name_current WHERE logical_name_id = $1")
            .bind(&logical_name_id)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(published, 1, "the next generation publishes");
    assert_eq!(
        generation_failure_rows(scratch.pool(), chain).await?,
        recorded,
        "a later success never deletes or rewrites the audit row"
    );
    let (binding_state, lineage_state): (String, String) = sqlx::query_as(
        "SELECT binding.canonicality_state::text, lineage.canonicality_state::text
         FROM project_generation_failures failure
         JOIN surface_bindings binding
           ON binding.surface_binding_id =
              (failure.evidence -> 'predecessor' ->> 'surface_binding_id')::uuid
         JOIN chain_lineage lineage
           ON lineage.chain_id = failure.chain_id
          AND lineage.block_hash = failure.target_block_hash
         WHERE failure.chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        binding_state, "orphaned",
        "the retained evidence names the now-orphaned binding"
    );
    assert!(
        ["canonical", "safe", "finalized"].contains(&lineage_state.as_str()),
        "the recorded target hash stays resolvable through lineage: {lineage_state}"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn a_crash_before_the_audit_insert_records_the_evidence_on_retry() -> Result<()> {
    let scratch = ScratchDatabase::create("project_dual_current_crash").await?;
    let chain = "project-dual-current-crash";
    seed_mainnet_dual_current_conflict(scratch.pool(), chain).await?;

    // The generation transaction rolls back on its own; a crash before the
    // phase appends its evidence leaves no row behind.
    let rolled_back = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.into(),
            target_block: 5,
            affected_from_block: 0,
            affected_to_block: 5,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await
        .expect_err("the generation aborts before publish");
    assert!(rolled_back.generation_failure_evidence().is_some());
    assert!(
        generation_failure_rows(scratch.pool(), chain)
            .await?
            .is_empty(),
        "the generation transaction writes no evidence itself"
    );
    let published: i64 = sqlx::query_scalar("SELECT count(*) FROM name_current")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(published, 0);

    run_project_phase(scratch.pool(), chain, 5)
        .await
        .expect_err("the retried generation still fails");
    assert_eq!(
        generation_failure_rows(scratch.pool(), chain).await?.len(),
        1
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn the_failure_fingerprint_is_stable_across_repeated_generations() -> Result<()> {
    let scratch = ScratchDatabase::create("project_dual_current_fingerprint").await?;
    let chain = "project-dual-current-fingerprint";
    seed_mainnet_dual_current_conflict(scratch.pool(), chain).await?;

    // An incremental generation resuming past a clean target reaches the same
    // conflict and fingerprints it identically to a full rebuild.
    let incremental = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain.into(),
            target_block: 5,
            affected_from_block: 4,
            affected_to_block: 5,
            resume_current: Some(Marker {
                number: 3,
                hash: block_hash(chain, 3),
            }),
            mode: RunMode::Normal,
        })
        .await
        .expect_err("the incremental generation reaches the same conflict");
    let incremental_fingerprint = incremental
        .generation_failure_evidence()
        .expect("the incremental failure carries evidence")
        .failure_fingerprint
        .clone();

    let mut fingerprints = Vec::new();
    for target in [5, 5, 4] {
        let error = Engine::new(scratch.pool().clone())
            .run_batch(BatchRequest {
                chain_id: chain.into(),
                target_block: target,
                affected_from_block: 0,
                affected_to_block: target,
                resume_current: None,
                mode: RunMode::Normal,
            })
            .await
            .expect_err("the conflict blocks every target past the boundary");
        let evidence = error
            .generation_failure_evidence()
            .expect("the failure carries evidence");
        fingerprints.push((target, evidence.failure_fingerprint.clone()));
    }
    assert_eq!(
        fingerprints[0].1, fingerprints[1].1,
        "the same conflict fingerprints identically across rebuilds"
    );
    assert_eq!(
        fingerprints[1].1, fingerprints[2].1,
        "the fingerprint covers the conflict, not the target block"
    );
    assert_eq!(
        incremental_fingerprint, fingerprints[0].1,
        "an incremental generation fingerprints the conflict like a full rebuild"
    );

    scratch.cleanup().await
}

// Drives the Project phase rather than the engine directly: the post-rollback
// audit write belongs to the phase, not to the generation transaction.
async fn run_project_phase(
    pool: &PgPool,
    chain: &str,
    target: i64,
) -> phase_runner::error::RunnerResult<()> {
    let marker = BlockMarker::new(target, block_hash(chain, target))?;
    ProjectPhase::new(pool.clone())
        .run_batch(PhaseContext {
            chain_id: chain.to_owned(),
            phase: PhaseName::Project,
            mode: PhaseRunMode::Normal,
            redo_attempt: None,
            sources: Arc::from([SourceConfig::new(
                chain,
                "rpc",
                "rpc",
                SeedBasis::NewSignatureRange,
                0,
                "http://127.0.0.1:1",
            )?]),
            available_heads: Some(HeadMarkers {
                latest: marker,
                safe: None,
                finalized: None,
            }),
            live_handoff: None,
            resume: PhaseResume::default(),
        })
        .await?;
    Ok(())
}

// A Mainnet corpus whose ENSv1 and ENSv2 bindings both stay current past an
// activated migration boundary. No deployment-profile manifest is declared, so
// the chain classifies as mainnet and the publication assertion applies.
async fn seed_mainnet_dual_current_conflict(pool: &PgPool, chain: &str) -> Result<String> {
    let logical_name_id = seed_dual_open_cross_arm_fixture(pool, chain, 4).await?;
    InterpretEngine::new(pool.clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    insert_activated_authority_proof(pool, chain, &logical_name_id, "unwrapped", None).await?;
    Ok(logical_name_id)
}

// Clones a seeded dual-current corpus onto a second logical name at the same
// target, so one generation faces two independent conflicts.
async fn clone_dual_current_conflict(
    pool: &PgPool,
    chain: &str,
    source_logical_name_id: &str,
    label: &str,
) -> Result<String> {
    let namehash = format!("{:#x}", raw_namehash(&[label.as_bytes(), b"eth"]));
    let labelhash = format!("{:#x}", keccak256(label.as_bytes()));
    let logical_name_id = format!("ens:{namehash}");
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         )
         SELECT $2, namespace, $3 || '.eth', ARRAY[$3, 'eth'], dns_encoded_name,
                $4, ARRAY[$5] || labelhashes[2:], normalizer_version,
                visibility_state, chain_id, block_hash, block_number,
                canonicality_state
         FROM name_surfaces WHERE logical_name_id = $1",
    )
    .bind(source_logical_name_id)
    .bind(&logical_name_id)
    .bind(label)
    .bind(&namehash)
    .bind(&labelhash)
    .execute(pool)
    .await?;

    let bindings: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT surface_binding_id, authority_arm FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2 AND active_to IS NULL
         ORDER BY authority_arm",
    )
    .bind(chain)
    .bind(source_logical_name_id)
    .fetch_all(pool)
    .await?;
    let mut successor_binding = None;
    for (source_binding, arm) in &bindings {
        let cloned = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind,
                 authority_arm, active_from, active_to, chain_id, block_hash,
                 block_number, provenance, canonicality_state
             )
             SELECT $1, $2, resource_id, binding_kind, authority_arm, active_from,
                    active_to, chain_id, block_hash, block_number, provenance,
                    canonicality_state
             FROM surface_bindings WHERE surface_binding_id = $3",
        )
        .bind(cloned)
        .bind(&logical_name_id)
        .bind(source_binding)
        .execute(pool)
        .await?;
        if arm == "ens_v2" {
            successor_binding = Some(cloned);
        }
    }
    let successor_binding = successor_binding.expect("the clone keeps an open ENSv2 arm");

    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, raw_fact_ref,
             derivation_kind, canonicality_state, before_state, after_state,
             migration_correlation_ids, consumer_visibility
         )
         SELECT event_identity || ':' || $2, namespace, $2, resource_id, event_kind,
                source_family, manifest_version, chain_id, block_number, block_hash,
                transaction_hash, transaction_index, log_index, raw_fact_ref,
                derivation_kind, canonicality_state, before_state,
                jsonb_set(
                    after_state, '{successor_binding,binding_id}', to_jsonb($3::text)
                ),
                migration_correlation_ids, consumer_visibility
         FROM normalized_events
         WHERE chain_id = $1 AND logical_name_id = $4
           AND event_kind = 'MigrationApplied'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .bind(successor_binding.to_string())
    .bind(source_logical_name_id)
    .execute(pool)
    .await?;

    Ok(logical_name_id)
}

async fn generation_failure_rows(
    pool: &PgPool,
    chain: &str,
) -> Result<Vec<(i64, String, String, String, String, String, Value)>> {
    Ok(sqlx::query_as(
        "SELECT target_block_number, target_block_hash, interpreter_content_hash,
                failure_kind, failure_fingerprint, logical_name_id, evidence
         FROM project_generation_failures
         WHERE chain_id = $1
         ORDER BY target_block_number, failure_fingerprint",
    )
    .bind(chain)
    .fetch_all(pool)
    .await?)
}

async fn projection_counts(pool: &PgPool) -> Result<(i64, i64, i64, i64, i64, i64, i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT
            (SELECT count(*) FROM name_current),
            (SELECT count(*) FROM children_current),
            (SELECT count(*) FROM permissions_current),
            (SELECT count(*) FROM permissions_current_resource_summary),
            (SELECT count(*) FROM record_inventory_current),
            (SELECT count(*) FROM resolver_current),
            (SELECT count(*) FROM address_names_current),
            (SELECT count(*) FROM primary_names_current)",
    )
    .fetch_one(pool)
    .await?)
}

async fn permission_projection_snapshot(pool: &PgPool) -> Result<(Value, Value)> {
    Ok(sqlx::query_as(
        "SELECT
             COALESCE((
                 SELECT jsonb_agg(to_jsonb(row) ORDER BY resource_id, subject, scope)
                 FROM permissions_current row
             ), '[]'::jsonb),
             COALESCE((
                 SELECT jsonb_agg(to_jsonb(row) ORDER BY resource_id)
                 FROM permissions_current_resource_summary row
             ), '[]'::jsonb)",
    )
    .fetch_one(pool)
    .await?)
}

async fn normalize_projection_clocks(pool: &PgPool) -> Result<()> {
    for statement in [
        "UPDATE name_current SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)",
        "UPDATE children_current SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)",
        "UPDATE permissions_current SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)",
        "UPDATE permissions_current_resource_summary SET last_recomputed_at = to_timestamp(0)",
        "UPDATE record_inventory_current SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)",
        "UPDATE resolver_current SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)",
        "UPDATE address_names_current SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    Ok(())
}

async fn serving_table_snapshot(pool: &PgPool) -> Result<Vec<(String, Value)>> {
    let tables = [
        ("name_current", "logical_name_id"),
        (
            "children_current",
            "parent_logical_name_id, child_logical_name_id, surface_class",
        ),
        ("permissions_current", "resource_id, subject, scope"),
        ("permissions_current_resource_summary", "resource_id"),
        (
            "record_inventory_current",
            "resource_id, record_version_boundary_key",
        ),
        ("resolver_current", "chain_id, resolver_address"),
        (
            "address_names_current",
            "address, logical_name_id, relation",
        ),
        ("primary_names_current", "address, coin_type, namespace"),
    ];
    let mut snapshot = Vec::with_capacity(tables.len());
    for (table, order) in tables {
        let statement = format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY {order}), '[]'::jsonb)
             FROM {table} row"
        );
        snapshot.push((
            table.to_owned(),
            sqlx::query_scalar(&statement).fetch_one(pool).await?,
        ));
    }
    Ok(snapshot)
}

async fn serving_table_snapshot_without_vintage_stamps(
    pool: &PgPool,
) -> Result<Vec<(String, Value)>> {
    let tables = [
        ("name_current", "logical_name_id"),
        (
            "children_current",
            "parent_logical_name_id, child_logical_name_id, surface_class",
        ),
        ("permissions_current", "resource_id, subject, scope"),
        ("permissions_current_resource_summary", "resource_id"),
        (
            "record_inventory_current",
            "resource_id, record_version_boundary_key",
        ),
        ("resolver_current", "chain_id, resolver_address"),
        (
            "address_names_current",
            "address, logical_name_id, relation",
        ),
        ("primary_names_current", "address, coin_type, namespace"),
    ];
    let mut snapshot = Vec::with_capacity(tables.len());
    for (table, order) in tables {
        let statement = format!(
            "SELECT COALESCE(jsonb_agg(
                 (
                     to_jsonb(row)
                     - 'chain_positions'
                     #- '{{canonicality_summary,target_block_number}}'
                     #- '{{canonicality_summary,target_block_hash}}'
                     #- '{{claim_provenance,target_block_number}}'
                     #- '{{claim_provenance,target_block_hash}}'
                 ) ORDER BY {order}
             ), '[]'::jsonb)
             FROM {table} row"
        );
        snapshot.push((
            table.to_owned(),
            sqlx::query_scalar(&statement).fetch_one(pool).await?,
        ));
    }
    Ok(snapshot)
}

async fn projection_authority_pair(pool: &PgPool) -> Result<(String, String)> {
    Ok(sqlx::query_as(
        "SELECT
             (SELECT declared_summary -> 'control' ->> 'registry_owner'
              FROM name_current
              WHERE logical_name_id = 'ens:0xalice'),
             (SELECT address
              FROM address_names_current
              WHERE logical_name_id = 'ens:0xalice'
                AND relation = 'effective_controller')",
    )
    .fetch_one(pool)
    .await?)
}

async fn seed_basenames_project_fixture(pool: &PgPool) -> Result<()> {
    seed_lineage(pool, BASE_CHAIN, 4).await?;
    seed_lineage(pool, ETHEREUM_CHAIN, 4).await?;
    insert_namespaced_manifest(
        pool,
        "basenames",
        BASE_CHAIN,
        "basenames_base_resolver",
        1,
        "basenames_v1",
        "tests/project-basenames-base-resolver.toml",
        json!({
            "manifest_version":1,
            "namespace":"basenames",
            "source_family":"basenames_base_resolver",
            "chain":BASE_CHAIN,
            "deployment_epoch":"basenames_v1",
            "rollout_status":"active",
            "normalizer_version":NORMALIZER,
            "contracts":[{
                "role":"l2_resolver",
                "address":BASENAMES_RESOLVER,
                "proxy_kind":"none",
                "start_block":0
            }]
        }),
    )
    .await?;
    insert_namespaced_manifest(
        pool,
        "basenames",
        ETHEREUM_CHAIN,
        "basenames_execution",
        2,
        "basenames_v1",
        "tests/project-basenames-execution.toml",
        json!({
            "manifest_version":2,
            "namespace":"basenames",
            "source_family":"basenames_execution",
            "chain":ETHEREUM_CHAIN,
            "deployment_epoch":"basenames_v1",
            "rollout_status":"active",
            "normalizer_version":NORMALIZER,
            "capability_flags":{"verified_resolution":{"status":"supported"}},
            "contracts":[{
                "role":"l1_resolver",
                "address":BASENAMES_L1_RESOLVER,
                "proxy_kind":"none",
                "start_block":0
            }]
        }),
    )
    .await?;

    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(BASENAMES_RESOURCE)?)
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES (
             'basenames:0xalice-base', 'basenames', 'alice.base.eth',
             ARRAY['alice','base','eth'], decode('00', 'hex'), '0xalice-base',
             ARRAY['0xalice-label','0xbase','0xeth'], $1, 'active', $2, $3, 1,
             'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, 'basenames:0xalice-base', $2, 'declared_registry_path',
                   'basenames', to_timestamp(1), $3, $4, 1, 'canonical')",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::parse_str(BASENAMES_RESOURCE)?)
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 1))
    .execute(pool)
    .await?;
    insert_namespaced_event(
        pool,
        "basenames",
        BASE_CHAIN,
        2,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "ResolverChanged",
        "basenames_base_registry",
        1,
        json!({"resolver":BASENAMES_RESOLVER}),
        json!({}),
    )
    .await?;
    Ok(())
}

async fn load_name_current_row(pool: &PgPool, logical_name_id: &str) -> Result<NameCurrentRow> {
    let projected = sqlx::query(
        "SELECT namespace, raw_name, namehash, surface_binding_id, resource_id,
                serving_resource_id, token_lineage_id, binding_kind, declared_summary,
                provenance, chain_positions, canonicality_summary, manifest_version,
                last_recomputed_at
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_one(pool)
    .await?;
    let declared_summary: Value = projected.try_get("declared_summary")?;
    Ok(NameCurrentRow {
        logical_name_id: logical_name_id.into(),
        namespace: projected.try_get("namespace")?,
        canonical_display_name: projected.try_get("raw_name")?,
        normalized_name: projected.try_get("raw_name")?,
        namehash: projected.try_get("namehash")?,
        surface_binding_id: projected.try_get("surface_binding_id")?,
        resource_id: projected.try_get("resource_id")?,
        serving_resource_id: projected.try_get("serving_resource_id")?,
        token_lineage_id: projected.try_get("token_lineage_id")?,
        binding_kind: projected.try_get("binding_kind")?,
        coverage: declared_summary["coverage"].clone(),
        declared_summary,
        provenance: projected.try_get("provenance")?,
        chain_positions: projected.try_get("chain_positions")?,
        canonicality_summary: projected.try_get("canonicality_summary")?,
        manifest_version: projected.try_get("manifest_version")?,
        last_recomputed_at: projected.try_get("last_recomputed_at")?,
    })
}

async fn extend_narrow_final_equivalence_fixture(pool: &PgPool) -> Result<()> {
    const S1_ADDRESS: &str = "0x0000000000000000000000000000000000000e01";
    const S2_ADDRESS: &str = "0x0000000000000000000000000000000000000e02";

    for (block, timestamp) in [(9, 7_776_008), (10, 7_776_009)] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(CHAIN, block))
        .bind(block_hash(CHAIN, block - 1))
        .bind(block)
        .bind(timestamp)
        .execute(pool)
        .await?;
    }

    insert_event(
        pool,
        CHAIN,
        9,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "AliasChanged",
        "basenames_base_resolver",
        json!({
            "resolver":EQUIVALENCE_TRANSFER_RESOLVER,
            "active":true,
            "alias_state":"active",
            "from_name":"transfer.eth",
            "from_namehash":"0xtransfer",
            "to_name":"bob.eth",
            "to_namehash":"0xbob",
            "to_logical_name_id":"ens:0xbob",
            "to_resource_id":EQUIVALENCE_BOB_RESOURCE
        }),
        json!({"emitting_address":EQUIVALENCE_TRANSFER_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        10,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "AliasChanged",
        "basenames_base_resolver",
        json!({
            "resolver":EQUIVALENCE_TRANSFER_RESOLVER,
            "active":false,
            "alias_state":"removed",
            "from_name":"transfer.eth",
            "from_namehash":"0xtransfer"
        }),
        json!({"emitting_address":EQUIVALENCE_TRANSFER_RESOLVER}),
    )
    .await?;

    for (block, before, after) in [(9, OWNER, TRANSFER_OWNER), (10, TRANSFER_OWNER, OWNER)] {
        insert_event(
            pool,
            CHAIN,
            block,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "AuthorityTransferred",
            "ens_v1_registrar_l1",
            json!({"owner":after}),
            json!({}),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events
             SET before_state = jsonb_build_object('owner', lower($1))
             WHERE chain_id = $2 AND block_number = $3
               AND logical_name_id = 'ens:0xalice'
               AND event_kind = 'AuthorityTransferred'",
        )
        .bind(before)
        .bind(CHAIN)
        .bind(block)
        .execute(pool)
        .await?;
    }

    for (block, before, after) in [(9, S2_ADDRESS, S1_ADDRESS), (10, S1_ADDRESS, S2_ADDRESS)] {
        insert_event(
            pool,
            CHAIN,
            block,
            Some("ens:0xequivalence-parent"),
            None,
            "SubregistryChanged",
            "ens_v2_registry_l1",
            json!({"subregistry":after}),
            json!({}),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events
             SET before_state = jsonb_build_object('subregistry', lower($1))
             WHERE chain_id = $2 AND block_number = $3
               AND logical_name_id = 'ens:0xequivalence-parent'
               AND event_kind = 'SubregistryChanged'",
        )
        .bind(before)
        .bind(CHAIN)
        .bind(block)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn extend_incremental_equivalence_fixture(pool: &PgPool) -> Result<()> {
    insert_manifest(
        pool,
        CHAIN,
        "ens_v2_resolver_l1",
        "tests/project-equivalence-v2-resolver.toml",
        json!({
            "resolver_implementations":[{
                "role":"permissioned_resolver",
                "address":EQUIVALENCE_V2_IMPLEMENTATION
            }]
        }),
    )
    .await?;
    for (block, timestamp) in [
        (4, 7_776_001),
        (5, 7_776_004),
        (6, 7_776_005),
        (7, 7_776_006),
        (8, 7_776_007),
    ] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(CHAIN, block))
        .bind(block_hash(CHAIN, block - 1))
        .bind(block)
        .bind(timestamp)
        .execute(pool)
        .await?;
    }

    seed_equivalence_subregistry_flip(pool).await?;

    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_BOB_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'ens:0xbob', 'ens', 'bob.eth', ARRAY['bob', 'eth'],
             decode('00', 'hex'), '0xbob', ARRAY['0xbob-label', '0xeth'], $1,
             'active', $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens:0xbob', $2, 'declared_registry_path',
             'ens_v1', to_timestamp(1), $3, $4, 1, 'canonical'
         )",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_BOB_BINDING)?)
    .bind(Uuid::parse_str(EQUIVALENCE_BOB_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority
         ) VALUES (
             '0xbob-label', convert_to('bob', 'UTF8'), 'bob', $1,
             true, 'fixture', 1
         )",
    )
    .bind(NORMALIZER)
    .execute(pool)
    .await?;

    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xbob",
            "labelhash":"0xbob-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":OWNER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "RegistrationGranted",
        "ens_v1_registrar_l1",
        json!({"authority_kind":"registrar","registrant":OWNER,"status":"registered"}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":OWNER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "PermissionChanged",
        "ens_v1_registrar_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:url",
            "record_family":"text",
            "selector_key":"url",
            "value_retained":true,
            "value":"https://bob.example.test"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:url",
            "record_family":"text",
            "selector_key":"url",
            "value_retained":true,
            "value":"https://equivalence.example.test"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_wrapper_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":[
                "resource_control", "resolver_control", "set_resolver", "set_ttl",
                "create_subnames", "transfer", "unwrap", "burn_fuses", "approve"
            ],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"source_event":"NameRenewed","authority_kind":"wrapper","expiry":7776003}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":196608,"wrapper_state":"emancipated"}),
        json!({}),
    )
    .await?;

    insert_event(
        pool,
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v2_registry_l1",
        json!({"resolver":EQUIVALENCE_V2_RESOLVER}),
        json!({}),
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events SET before_state = jsonb_build_object('resolver', lower($1))
         WHERE chain_id = $2 AND block_number = 4
           AND logical_name_id = 'ens:0xalice' AND event_kind = 'ResolverChanged'",
    )
    .bind(RESOLVER)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AliasChanged",
        "ens_v2_resolver_l1",
        json!({
            "resolver":EQUIVALENCE_V2_RESOLVER,
            "active":true,
            "alias_state":"active",
            "from_name":"alice.eth",
            "from_namehash":"0xalice",
            "to_name":"bob.eth",
            "to_namehash":"0xbob",
            "to_logical_name_id":"ens:0xbob",
            "to_resource_id":EQUIVALENCE_BOB_RESOURCE
        }),
        json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        None,
        None,
        "Upgraded",
        "ens_v2_resolver_l1",
        json!({
            "proxy_address":EQUIVALENCE_V2_RESOLVER,
            "implementation":EQUIVALENCE_V2_IMPLEMENTATION
        }),
        json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":"0x00000000000000000000000000000000000000a7"}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xeth"),
        None,
        "PreimageObserved",
        "ens_v1_registry_l1",
        json!({"labelhash":"0xeth","raw_labels_hex":["657468"]}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        None,
        None,
        "ReverseChanged",
        "ens_v1_reverse_l1",
        json!({
            "address":OWNER,
            "coin_type":"60",
            "namespace":"ens",
            "claim_provenance":{"source_family":"ens_v1_reverse_l1"}
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        None,
        None,
        "RecordChanged",
        "ens_v1_reverse_l1",
        json!({
            "raw_name":"alice.eth",
            "primary_claim_source":{
                "address":OWNER,
                "coin_type":"60",
                "namespace":"ens",
                "claim_provenance":{"source_family":"ens_v1_reverse_l1"}
            }
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        8,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":OWNER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        8,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":"0x00000000000000000000000000000000000000a7"}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        8,
        Some("ens:0xeth"),
        None,
        "PreimageObserved",
        "ens_v1_registry_l1",
        json!({"labelhash":"0xeth","raw_labels_hex":["657468"]}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        8,
        None,
        None,
        "ReverseChanged",
        "ens_v1_reverse_l1",
        json!({
            "address":OWNER,
            "coin_type":"60",
            "namespace":"ens",
            "claim_provenance":{"source_family":"ens_v1_reverse_l1"}
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        8,
        None,
        None,
        "RecordChanged",
        "ens_v1_reverse_l1",
        json!({
            "raw_name":"alice.eth",
            "primary_claim_source":{
                "address":OWNER,
                "coin_type":"60",
                "namespace":"ens",
                "claim_provenance":{"source_family":"ens_v1_reverse_l1"}
            }
        }),
        json!({}),
    )
    .await?;
    for logical_name_id in [
        "ens:0xequivalence-parent",
        "ens:0xequivalence-c0",
        "ens:0xequivalence-c1",
    ] {
        insert_event(
            pool,
            CHAIN,
            8,
            Some(logical_name_id),
            None,
            "PreimageObserved",
            "ens_v2_registry_l1",
            json!({"raw_labels_hex":[]}),
            json!({}),
        )
        .await?;
    }
    Ok(())
}

async fn extend_wrapper_resolver_summary_fixture(pool: &PgPool) -> Result<()> {
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registrar_l1",
        json!({
            "subject":OWNER,
            "scope":{
                "kind":"resolver",
                "chain_id":CHAIN,
                "resolver_address":EQUIVALENCE_V2_RESOLVER
            },
            "effective_powers":["approve","resolver_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RecordChanged",
        "ens_v2_resolver_l1",
        json!({
            "resolver":EQUIVALENCE_V2_RESOLVER,
            "record_key":"text:boundary",
            "record_family":"text",
            "selector_key":"boundary",
            "value_retained":true,
            "value":"before-expiry"
        }),
        json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        None,
        None,
        "Upgraded",
        "ens_v2_resolver_l1",
        json!({
            "proxy_address":EQUIVALENCE_V2_RESOLVER,
            "implementation":EQUIVALENCE_V2_IMPLEMENTATION
        }),
        json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
    )
    .await?;

    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "PermissionChanged",
        "ens_v1_wrapper_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["approve","resolver_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({
            "source_event":"NameRenewed",
            "authority_kind":"wrapper",
            "expiry":7_776_003
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":196_608,"wrapper_state":"emancipated"}),
        json!({}),
    )
    .await?;
    Ok(())
}

async fn extend_permission_only_resolver_fixture(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(PERMISSION_ONLY_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES (
             'ens:0xpermission-only', 'ens', 'permission-only.eth',
             ARRAY['permission-only', 'eth'], decode('00', 'hex'),
             '0xpermission-only', ARRAY['0xpermission-only-label', '0xeth'], $1,
             'active', $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens:0xpermission-only', $2, 'declared_registry_path',
             'ens_v2', to_timestamp(1), $3, $4, 1, 'canonical'
         )",
    )
    .bind(Uuid::parse_str(PERMISSION_ONLY_BINDING)?)
    .bind(Uuid::parse_str(PERMISSION_ONLY_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xpermission-only"),
        Some(PERMISSION_ONLY_RESOURCE),
        "ResolverChanged",
        "ens_v2_registry_l1",
        json!({"resolver":PERMISSION_ONLY_RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xpermission-only"),
        Some(PERMISSION_ONLY_RESOURCE),
        "RecordChanged",
        "ens_v2_resolver_l1",
        json!({
            "resolver":PERMISSION_ONLY_RESOLVER,
            "record_key":"text:permission-only",
            "record_family":"text",
            "selector_key":"permission-only",
            "value_retained":true,
            "value":"outside-boundary-scope"
        }),
        json!({"emitting_address":PERMISSION_ONLY_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        None,
        None,
        "Upgraded",
        "ens_v2_resolver_l1",
        json!({
            "proxy_address":PERMISSION_ONLY_RESOLVER,
            "implementation":EQUIVALENCE_V2_IMPLEMENTATION
        }),
        json!({"emitting_address":PERMISSION_ONLY_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registrar_l1",
        json!({
            "subject":OWNER,
            "scope":{
                "kind":"resolver",
                "chain_id":CHAIN,
                "resolver_address":PERMISSION_ONLY_RESOLVER
            },
            "effective_powers":["approve","resolver_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    Ok(())
}

async fn extend_equivalence_registrar_transfer_fixture(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_TRANSFER_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'ens:0xtransfer', 'ens', 'transfer.eth', ARRAY['transfer', 'eth'],
             decode('00', 'hex'), '0xtransfer', ARRAY['0xtransfer-label', '0xeth'],
             $1, 'active', $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens:0xtransfer', $2, 'declared_registry_path',
             'ens_v1', to_timestamp(1), $3, $4, 1, 'canonical'
         )",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_TRANSFER_BINDING)?)
    .bind(Uuid::parse_str(EQUIVALENCE_TRANSFER_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    insert_manifest(
        pool,
        CHAIN,
        "basenames_base_resolver",
        "tests/project-equivalence-basenames-resolver.toml",
        json!({
            "contracts":[{
                "role":"l2_resolver",
                "address":EQUIVALENCE_TRANSFER_RESOLVER,
                "proxy_kind":"none"
            }]
        }),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "RegistrationGranted",
        "basenames_base_registrar",
        json!({
            "authority_kind":"registrar",
            "registrant":OWNER,
            "status":"registered"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "AuthorityTransferred",
        "basenames_base_registrar",
        json!({"owner":OWNER}),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "PermissionChanged",
        "basenames_base_registrar",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "ResolverChanged",
        "basenames_base_registry",
        json!({"resolver":EQUIVALENCE_TRANSFER_RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "RecordChanged",
        "basenames_base_resolver",
        json!({
            "resolver":EQUIVALENCE_TRANSFER_RESOLVER,
            "record_key":"text:url",
            "record_family":"text",
            "selector_key":"url",
            "value_retained":true,
            "value":"https://transfer.example.test"
        }),
        json!({"emitting_address":EQUIVALENCE_TRANSFER_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "PermissionChanged",
        "basenames_base_registry",
        json!({
            "subject":OWNER,
            "scope":{
                "kind":"resolver",
                "chain_id":CHAIN,
                "resolver_address":EQUIVALENCE_TRANSFER_RESOLVER
            },
            "effective_powers":["resolver_control"],
            "grant_source":{
                "kind":"ens_v1_authority",
                "authority_kind":"registrar",
                "authority_key":"equivalence-transfer",
                "source_event_kind":"ResolverChanged"
            },
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":REGISTRY}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "AuthorityTransferred",
        "basenames_base_registrar",
        json!({"owner":TRANSFER_OWNER}),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        8,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "AuthorityTransferred",
        "basenames_base_registrar",
        json!({"owner":TRANSFER_OWNER}),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "PermissionChanged",
        "basenames_base_registrar",
        json!({
            "subject":OWNER,
            "scope":{
                "kind":"resolver",
                "chain_id":CHAIN,
                "resolver_address":EQUIVALENCE_TRANSFER_RESOLVER
            },
            "effective_powers":[],
            "grant_source":null,
            "revocation_source":{
                "kind":"ens_v1_authority",
                "authority_kind":"registrar",
                "authority_key":"equivalence-transfer",
                "source_event_kind":"TokenControlTransferred"
            },
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET before_state = jsonb_build_object(
             'subject', $1::text,
             'scope', jsonb_build_object(
                 'kind', 'resolver', 'chain_id', $2::text,
                 'resolver_address', lower($3::text)
             ),
             'effective_powers', jsonb_build_array('resolver_control'),
             'grant_source', jsonb_build_object(
                 'kind', 'ens_v1_authority',
                 'authority_kind', 'registrar',
                 'authority_key', 'equivalence-transfer',
                 'source_event_kind', 'TokenControlTransferred'
             ),
             'revocation_source', NULL,
             'inheritance_path', '[]'::jsonb,
             'transfer_behavior', 'replace_on_authority_change'
         )
         WHERE chain_id = $2 AND block_number = 7
           AND logical_name_id = 'ens:0xtransfer'
           AND event_kind = 'PermissionChanged'
           AND after_state ->> 'subject' = $1",
    )
    .bind(OWNER)
    .bind(CHAIN)
    .bind(EQUIVALENCE_TRANSFER_RESOLVER)
    .execute(pool)
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "PermissionChanged",
        "basenames_base_registrar",
        json!({
            "subject":TRANSFER_OWNER,
            "scope":{
                "kind":"resolver",
                "chain_id":CHAIN,
                "resolver_address":EQUIVALENCE_TRANSFER_RESOLVER
            },
            "effective_powers":["resolver_control"],
            "grant_source":{
                "kind":"ens_v1_authority",
                "authority_kind":"registrar",
                "authority_key":"equivalence-transfer",
                "source_event_kind":"TokenControlTransferred"
            },
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;
    Ok(())
}

async fn seed_equivalence_subregistry_flip(pool: &PgPool) -> Result<()> {
    const S1_ADDRESS: &str = "0x0000000000000000000000000000000000000e01";
    const S2_ADDRESS: &str = "0x0000000000000000000000000000000000000e02";
    let s1 = Uuid::parse_str("00000000-0000-0000-0000-000000000e01")?;
    let s2 = Uuid::parse_str("00000000-0000-0000-0000-000000000e02")?;

    for (instance, address) in [(s1, S1_ADDRESS), (s2, S2_ADDRESS)] {
        sqlx::query(
            "INSERT INTO contract_instances (
                 contract_instance_id, chain_id, contract_kind
             ) VALUES ($1, $2, 'contract')",
        )
        .bind(instance)
        .bind(CHAIN)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO contract_instance_addresses (
                 contract_instance_id, chain_id, address, active_from_block_number
             ) VALUES ($1, $2, $3, 0)",
        )
        .bind(instance)
        .bind(CHAIN)
        .bind(address)
        .execute(pool)
        .await?;
    }
    for (logical_name_id, raw_name, raw_labels, namehash, labelhashes) in [
        (
            "ens:0xequivalence-parent",
            "equivalence.eth",
            vec!["equivalence", "eth"],
            "0xequivalence-parent",
            vec!["0xequivalence-parent-label", "0xeth"],
        ),
        (
            "ens:0xequivalence-c0",
            "c0.equivalence.eth",
            vec!["c0", "equivalence", "eth"],
            "0xequivalence-c0",
            vec![
                "0xequivalence-c0-label",
                "0xequivalence-parent-label",
                "0xeth",
            ],
        ),
        (
            "ens:0xequivalence-c1",
            "c1.equivalence.eth",
            vec!["c1", "equivalence", "eth"],
            "0xequivalence-c1",
            vec![
                "0xequivalence-c1-label",
                "0xequivalence-parent-label",
                "0xeth",
            ],
        ),
    ] {
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES ($1, 'ens', $2, $3, decode('00', 'hex'), $4, $5, $6,
                       'active', $7, $8, 1, 'canonical')",
        )
        .bind(logical_name_id)
        .bind(raw_name)
        .bind(raw_labels)
        .bind(namehash)
        .bind(labelhashes)
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens:0xequivalence-parent', $2, 'declared_registry_path',
             'ens_v1', to_timestamp(7776002), $3, $4, 1, 'canonical'
         )",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_PARENT_BINDING)?)
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority
         ) VALUES
             ('0xequivalence-parent-label', convert_to('equivalence', 'UTF8'), 'equivalence', $1,
              true, 'fixture', 1),
             ('0xequivalence-c0-label', convert_to('c0', 'UTF8'), 'c0', $1,
              true, 'fixture', 1),
             ('0xequivalence-c1-label', convert_to('c1', 'UTF8'), 'c1', $1,
              true, 'fixture', 1)",
    )
    .bind(NORMALIZER)
    .execute(pool)
    .await?;

    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xequivalence-parent"),
        Some(RESOURCE),
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xequivalence-parent",
            "labelhash":"0xequivalence-parent-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xequivalence-parent"),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":S1_ADDRESS}),
        json!({}),
    )
    .await?;
    for (block, logical_name_id, instance) in [
        (1, "ens:0xequivalence-c0", s1),
        (2, "ens:0xequivalence-c1", s2),
    ] {
        insert_event(
            pool,
            CHAIN,
            block,
            Some(logical_name_id),
            None,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            json!({
                "registry_contract_instance_id":instance,
                "registrant":OWNER
            }),
            json!({}),
        )
        .await?;
    }
    insert_event(
        pool,
        CHAIN,
        4,
        Some("ens:0xequivalence-parent"),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":S2_ADDRESS}),
        json!({}),
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET before_state = jsonb_build_object('subregistry', lower($1))
         WHERE chain_id = $2 AND block_number = 4
           AND logical_name_id = 'ens:0xequivalence-parent'
           AND event_kind = 'SubregistryChanged'",
    )
    .bind(S1_ADDRESS)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_project_fixture(pool: &PgPool) -> Result<()> {
    seed_lineage(pool, CHAIN, 3).await?;
    let registrar_manifest = insert_manifest(
        pool,
        CHAIN,
        "ens_v1_registrar_l1",
        "tests/project-registrar.toml",
        json!({"contracts":[]}),
    )
    .await?;
    let resolver_manifest = insert_manifest(
        pool,
        CHAIN,
        "ens_v1_resolver_l1",
        "tests/project-resolver.toml",
        json!({"contracts":[{"role":"public_resolver","address":RESOLVER,"proxy_kind":"none"}]}),
    )
    .await?;
    let resolver_instance = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, $2, 'contract')",
    )
    .bind(resolver_instance)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind
         ) VALUES ($1, $2, 'contract', 'public_resolver', $3, $4,
                   'public_resolver', 'none')",
    )
    .bind(resolver_manifest)
    .bind(CHAIN)
    .bind(resolver_instance)
    .bind(RESOLVER)
    .execute(pool)
    .await?;
    let _ = registrar_manifest;

    let token_lineage_id = Uuid::parse_str(TOKEN_LINEAGE)?;
    sqlx::query(
        "INSERT INTO token_lineages (
             token_lineage_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(token_lineage_id)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, token_lineage_id, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES ($1, $2, $3, $4, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(token_lineage_id)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    for (logical, raw_name, raw_labels, namehash, labelhashes, visibility) in [
        (
            "ens:0xeth",
            "eth",
            vec!["eth"],
            "0xeth",
            vec!["0xeth"],
            "active",
        ),
        (
            "ens:0xalice",
            "alice.eth",
            vec!["alice", "eth"],
            "0xalice",
            vec!["0xalice-label", "0xeth"],
            "active",
        ),
    ] {
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES ($1, 'ens', $2, $3, decode('00', 'hex'), $4, $5, $6, $7,
                       $8, $9, 1, 'canonical')",
        )
        .bind(logical)
        .bind(raw_name)
        .bind(raw_labels)
        .bind(namehash)
        .bind(labelhashes)
        .bind(NORMALIZER)
        .bind(visibility)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
    }
    sqlx::query(
        r#"INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, normalization_errors, deactivation_reason,
             deactivated_at, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'ens:0xhostile', 'ens', '', ARRAY[]::text[], decode('00', 'hex'), '0xhostile',
             ARRAY[]::text[], $1, 'shadow', '[{"error":"hostile"}]',
             'normalization_gate', now(), $2, $3, 3, 'canonical'
         )"#,
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 3))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, normalization_error, source_kind, source_priority
         ) VALUES
             ('0xalice-label', convert_to('alice', 'UTF8'), 'alice', $1, true, NULL, 'fixture', 1),
             ('0xhostile-label', decode('ff00', 'hex'), NULL, $1, false, 'hostile', 'fixture', 1)",
    )
    .bind(NORMALIZER)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, 'ens:0xalice', $2, 'declared_registry_path',
                   'ens_v1', to_timestamp(1), $3, $4, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(SURFACE_BINDING)?)
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;

    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xalice",
            "labelhash":"0xalice-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RegistrationGranted",
        "ens_v1_registrar_l1",
        json!({"authority_kind":"registrar","registrant":OWNER,"status":"registered"}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":OWNER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registrar_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(pool, CHAIN, 2, Some("ens:0xalice"), Some(RESOURCE), "RecordChanged", "ens_v1_resolver_l1", json!({"resolver":RESOLVER,"record_key":"text:url","record_family":"text","selector_key":"url","value_retained":true,"value":"https://example.test"}), json!({"emitting_address":RESOLVER})).await?;
    insert_event(pool, CHAIN, 3, None, None, "ReverseChanged", "ens_v1_reverse_l1", json!({"address":OWNER,"coin_type":"60","namespace":"ens","claim_provenance":{"source_family":"ens_v1_reverse_l1"}}), json!({})).await?;
    insert_event(pool, CHAIN, 3, None, None, "RecordChanged", "ens_v1_reverse_l1", json!({"raw_name":"alice.eth","primary_claim_source":{"address":OWNER,"coin_type":"60","namespace":"ens","claim_provenance":{"source_family":"ens_v1_reverse_l1"}}}), json!({})).await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xhostile"),
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xhostile",
            "labelhash":"0xhostile-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xhostile"),
        None,
        "PreimageObserved",
        "ens_v1_wrapper_l1",
        json!({"raw_labels_hex":["ff00","657468"]}),
        json!({}),
    )
    .await?;
    Ok(())
}

async fn seed_declaration_boundary_fixture(pool: &PgPool) -> Result<()> {
    seed_project_fixture(pool).await?;
    for number in 4..=21 {
        insert_lineage_block(pool, CHAIN, number).await?;
    }
    let contracts = json!([
        {
            "role":"old_resolver",
            "address":RESOLVER,
            "proxy_kind":"none",
            "read_features":["ensip19_default_address"],
            "start_block":10
        },
        {
            "role":"new_resolver",
            "address":RESOLVER,
            "proxy_kind":"none",
            "read_features":["ensip19_default_address"],
            "start_block":20
        }
    ]);
    sqlx::query(
        "UPDATE manifest_versions
         SET manifest_payload = jsonb_set(manifest_payload, '{contracts}', $1)
         WHERE chain_id = $2 AND source_family = 'ens_v1_resolver_l1'",
    )
    .bind(&contracts)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET after_state = jsonb_set(after_state, '{manifest_payload,contracts}', $1)
         WHERE chain_id = $2 AND source_family = 'ens_v1_resolver_l1'
           AND event_kind = 'SourceManifestUpdated'",
    )
    .bind(contracts)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    Ok(())
}

const DECLARED_V1_SHARED_RESOLVER: &str = "0x0000000000000000000000000000000000000d01";
const DECLARED_V1_UNDECLARED_RESOLVER: &str = "0x0000000000000000000000000000000000000d02";
const DECLARED_V1_ALICE_RESOURCE: &str = "00000000-0000-0000-0000-000000000d01";
const DECLARED_V1_BOB_RESOURCE: &str = "00000000-0000-0000-0000-000000000d02";

#[derive(Clone, Copy)]
enum DeclaredV1NodeOnlyDelta {
    RecordUpdate,
    VersionReset,
    UnrelatedRecord,
}

async fn assert_declared_v1_node_only_delta(
    chain: &str,
    delta: DeclaredV1NodeOnlyDelta,
) -> Result<()> {
    let incremental = ScratchDatabase::create(&format!("{chain}-incremental")).await?;
    let fresh = ScratchDatabase::create(&format!("{chain}-fresh")).await?;
    let manifests = seed_declared_v1_shared_pair(incremental.pool(), fresh.pool(), chain).await?;
    let alice_node = format!("{:#x}", raw_namehash(&[b"alice", b"eth"]));

    for (pool, manifest_id) in [
        (incremental.pool(), manifests.0),
        (fresh.pool(), manifests.1),
    ] {
        insert_manifest_update_event(
            pool,
            chain,
            "ens_v1_resolver_l1",
            manifest_id,
            resolver_declaration_payload("ens", chain, DECLARED_V1_SHARED_RESOLVER),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events
             SET after_state = jsonb_set(after_state, '{value}', '\"old\"')
             WHERE chain_id = $1 AND event_kind = 'RecordChanged'
               AND after_state ->> 'node' = $2",
        )
        .bind(chain)
        .bind(&alice_node)
        .execute(pool)
        .await?;
    }

    run_project(incremental.pool(), chain, None, RunMode::Normal, 0, 3).await?;
    let before: (Option<String>, String) = sqlx::query_as(
        "SELECT entries -> 0 ->> 'value', last_recomputed_at::text
         FROM record_inventory_current WHERE resource_id = $1::uuid",
    )
    .bind(DECLARED_V1_ALICE_RESOURCE)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(before.0.as_deref(), Some("old"));

    for pool in [incremental.pool(), fresh.pool()] {
        insert_lineage_block(pool, chain, 4).await?;
        let (event_kind, node, after_state) = match delta {
            DeclaredV1NodeOnlyDelta::RecordUpdate => (
                "RecordChanged",
                alice_node.clone(),
                json!({
                    "node": alice_node,
                    "resolver": DECLARED_V1_SHARED_RESOLVER,
                    "record_key": "text:url",
                    "record_family": "text",
                    "selector_key": "url",
                    "value_retained": true,
                    "value": "new"
                }),
            ),
            DeclaredV1NodeOnlyDelta::VersionReset => (
                "RecordVersionChanged",
                alice_node.clone(),
                json!({
                    "node": alice_node,
                    "resolver": DECLARED_V1_SHARED_RESOLVER,
                    "record_version": 2
                }),
            ),
            DeclaredV1NodeOnlyDelta::UnrelatedRecord => {
                let node = format!("{:#x}", raw_namehash(&[b"unrelated", b"eth"]));
                (
                    "RecordChanged",
                    node.clone(),
                    json!({
                        "node": node,
                        "resolver": DECLARED_V1_SHARED_RESOLVER,
                        "record_key": "text:url",
                        "record_family": "text",
                        "selector_key": "url",
                        "value_retained": true,
                        "value": "unrelated"
                    }),
                )
            }
        };
        insert_event(
            pool,
            chain,
            4,
            None,
            None,
            event_kind,
            "ens_v1_resolver_l1",
            after_state,
            json!({"emitting_address": DECLARED_V1_SHARED_RESOLVER, "node": node}),
        )
        .await?;
    }

    sqlx::query("SELECT pg_sleep(0.01)")
        .execute(incremental.pool())
        .await?;
    run_project(
        incremental.pool(),
        chain,
        Some(Marker {
            number: 3,
            hash: block_hash(chain, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    run_project(fresh.pool(), chain, None, RunMode::Normal, 0, 4).await?;

    let incremental_after: (Option<String>, String) = sqlx::query_as(
        "SELECT entries -> 0 ->> 'value', last_recomputed_at::text
         FROM record_inventory_current WHERE resource_id = $1::uuid",
    )
    .bind(DECLARED_V1_ALICE_RESOURCE)
    .fetch_one(incremental.pool())
    .await?;
    let fresh_value: Option<String> = sqlx::query_scalar(
        "SELECT entries -> 0 ->> 'value'
         FROM record_inventory_current WHERE resource_id = $1::uuid",
    )
    .bind(DECLARED_V1_ALICE_RESOURCE)
    .fetch_one(fresh.pool())
    .await?;
    match delta {
        DeclaredV1NodeOnlyDelta::RecordUpdate => {
            assert_eq!(incremental_after.0.as_deref(), Some("new"));
            assert_eq!(fresh_value.as_deref(), Some("new"));
        }
        DeclaredV1NodeOnlyDelta::VersionReset => {
            assert_eq!(incremental_after.0, None);
            assert_eq!(fresh_value, None);
        }
        DeclaredV1NodeOnlyDelta::UnrelatedRecord => {
            assert_eq!(incremental_after.0.as_deref(), Some("old"));
            assert_eq!(fresh_value.as_deref(), Some("old"));
            assert_eq!(
                incremental_after.1, before.1,
                "a node-only change for another name must not republish this inventory",
            );
        }
    }

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(fresh.pool()).await?;
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(fresh.pool()).await?,
        "node-only record delta diverged from a fresh rebuild",
    );

    incremental.cleanup().await?;
    fresh.cleanup().await
}

fn resolver_declaration_payload(namespace: &str, chain: &str, address: &str) -> Value {
    json!({
        "manifest_version": 1,
        "namespace": namespace,
        "source_family": "ens_v1_resolver_l1",
        "chain": chain,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "public_resolver",
            "address": address,
            "proxy_kind": "none",
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": {"events": [], "calls": []}
    })
}

async fn seed_declared_v1_shared_pair(
    incremental: &PgPool,
    fresh: &PgPool,
    chain: &str,
) -> Result<(i64, i64)> {
    let seed = |pool| {
        seed_declared_v1_shared_resolver_fixture(
            pool,
            chain,
            DECLARED_V1_SHARED_RESOLVER,
            DECLARED_V1_UNDECLARED_RESOLVER,
            DECLARED_V1_ALICE_RESOURCE,
            DECLARED_V1_BOB_RESOURCE,
        )
    };
    Ok((seed(incremental).await?, seed(fresh).await?))
}

async fn activate_declared_v1_shared_pair(
    incremental: &PgPool,
    fresh: &PgPool,
    chain: &str,
    manifests: (i64, i64),
) -> Result<()> {
    for (pool, manifest_id) in [(incremental, manifests.0), (fresh, manifests.1)] {
        insert_manifest_update_event(
            pool,
            chain,
            "ens_v1_resolver_l1",
            manifest_id,
            resolver_declaration_payload("ens", chain, DECLARED_V1_SHARED_RESOLVER),
        )
        .await?;
    }
    run_project(
        incremental,
        chain,
        Some(Marker {
            number: 3,
            hash: block_hash(chain, 3),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    run_project(fresh, chain, None, RunMode::Normal, 0, 3).await
}

async fn declared_v1_shared_clocks(pool: &PgPool, chain: &str) -> Result<(String, String)> {
    Ok(sqlx::query_as(
        "SELECT resolver.last_recomputed_at::text,
                (SELECT max(last_recomputed_at)::text
                 FROM record_inventory_current
                 WHERE resource_id IN ($3::uuid, $4::uuid))
         FROM resolver_current resolver
         WHERE resolver.chain_id = $1 AND resolver.resolver_address = lower($2)",
    )
    .bind(chain)
    .bind(DECLARED_V1_SHARED_RESOLVER)
    .bind(DECLARED_V1_ALICE_RESOURCE)
    .bind(DECLARED_V1_BOB_RESOURCE)
    .fetch_one(pool)
    .await?)
}

async fn set_shared_resolver_discovery_origin(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
) -> Result<()> {
    let origin_manifest: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions
         WHERE chain_id = $1 AND namespace = 'ens' AND source_family = $2",
    )
    .bind(chain)
    .bind(source_family)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "UPDATE discovery_edges edge SET source_manifest_id = $3
         FROM contract_instance_addresses address
         WHERE edge.chain_id = $1
           AND edge.to_contract_instance_id = address.contract_instance_id
           AND address.chain_id = edge.chain_id
           AND lower(address.address) = lower($2)",
    )
    .bind(chain)
    .bind(DECLARED_V1_SHARED_RESOLVER)
    .bind(origin_manifest)
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_shared_resolver_discovery_end(
    pool: &PgPool,
    chain: &str,
    active_to: Option<i64>,
) -> Result<()> {
    sqlx::query(
        "UPDATE discovery_edges edge
         SET active_to_block_number = $3, active_to_block_hash = $4
         FROM contract_instance_addresses address
         WHERE edge.chain_id = $1
           AND edge.to_contract_instance_id = address.contract_instance_id
           AND address.chain_id = edge.chain_id
           AND lower(address.address) = lower($2)",
    )
    .bind(chain)
    .bind(DECLARED_V1_SHARED_RESOLVER)
    .bind(active_to)
    .bind(active_to.map(|block| block_hash(chain, block)))
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_shared_resolver_discovery_namespace_end(
    pool: &PgPool,
    chain: &str,
    namespace: &str,
    active_to: Option<i64>,
) -> Result<()> {
    sqlx::query(
        "UPDATE discovery_edges edge
         SET active_to_block_number = $4, active_to_block_hash = $5
         FROM contract_instance_addresses address, manifest_versions origin
         WHERE edge.chain_id = $1
           AND edge.to_contract_instance_id = address.contract_instance_id
           AND address.chain_id = edge.chain_id
           AND lower(address.address) = lower($2)
           AND origin.manifest_id = edge.source_manifest_id
           AND origin.namespace = $3",
    )
    .bind(chain)
    .bind(DECLARED_V1_SHARED_RESOLVER)
    .bind(namespace)
    .bind(active_to)
    .bind(active_to.map(|block| block_hash(chain, block)))
    .execute(pool)
    .await?;
    Ok(())
}

async fn deactivate_foreign_declared_v1_manifest(pool: &PgPool, chain: &str) -> Result<()> {
    sqlx::query(
        "UPDATE normalized_events
         SET after_state = jsonb_set(after_state, '{rollout_status}', '\"deprecated\"')
         WHERE chain_id = $1 AND namespace = 'basenames'
           AND event_kind = 'SourceManifestUpdated'
           AND source_family = 'ens_v1_resolver_l1'",
    )
    .bind(chain)
    .execute(pool)
    .await?;
    Ok(())
}

async fn add_shared_resolver_discovery_namespace(
    pool: &PgPool,
    chain: &str,
    namespace: &str,
) -> Result<i64> {
    let declaration_manifest: i64 = sqlx::query_scalar(
        "SELECT manifest_id
         FROM manifest_versions
         WHERE chain_id = $1 AND namespace = $2
           AND source_family = 'ens_v1_resolver_l1'",
    )
    .bind(chain)
    .bind(namespace)
    .fetch_one(pool)
    .await?;
    let origin_manifest = insert_namespaced_manifest(
        pool,
        namespace,
        chain,
        "ens_v2_registry_l1",
        1,
        "fixture",
        "tests/project-shared-cross-namespace-v2-registry.toml",
        json!({"contracts": []}),
    )
    .await?;
    let resolver_instance: Uuid = sqlx::query_scalar(
        "SELECT contract_instance_id
         FROM contract_instance_addresses
         WHERE chain_id = $1 AND address = lower($2)",
    )
    .bind(chain)
    .bind(DECLARED_V1_SHARED_RESOLVER)
    .fetch_one(pool)
    .await?;
    let source_instance = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind
         ) VALUES ($1, $2, 'contract')",
    )
    .bind(source_instance)
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO discovery_edges (
             chain_id, edge_kind, from_contract_instance_id,
             to_contract_instance_id, discovery_source, admission_basis,
             source_manifest_id, active_from_block_number,
             active_from_block_hash, canonicality_state
         ) VALUES (
             $1, 'resolver', $2, $3, 'fixture', 'reachable_from_root',
             $4, 1, $5, 'canonical'
         )",
    )
    .bind(chain)
    .bind(source_instance)
    .bind(resolver_instance)
    .bind(origin_manifest)
    .bind(block_hash(chain, 1))
    .execute(pool)
    .await?;
    Ok(declaration_manifest)
}

async fn seed_declared_v1_shared_resolver_fixture(
    pool: &PgPool,
    chain: &str,
    shared_resolver: &str,
    undeclared_resolver: &str,
    alice_resource: &str,
    bob_resource: &str,
) -> Result<i64> {
    seed_lineage(pool, chain, 3).await?;
    let origin_manifest = insert_namespaced_manifest(
        pool,
        "ens",
        chain,
        "ens_v2_registry_l1",
        1,
        "fixture",
        "tests/project-shared-v2-registry.toml",
        json!({"contracts": []}),
    )
    .await?;
    insert_namespaced_manifest(
        pool,
        "ens",
        chain,
        "ens_v2_root_l1",
        1,
        "fixture",
        "tests/project-shared-v2-root.toml",
        json!({"contracts": []}),
    )
    .await?;
    insert_namespaced_manifest(
        pool,
        "ens",
        chain,
        "ens_v2_resolver_l1",
        1,
        "fixture",
        "tests/project-shared-v2-resolver.toml",
        json!({"resolver_implementations": []}),
    )
    .await?;
    let v1_manifest = insert_namespaced_manifest(
        pool,
        "ens",
        chain,
        "ens_v1_resolver_l1",
        1,
        "fixture",
        "tests/project-shared-v1-resolver.toml",
        json!({"contracts": []}),
    )
    .await?;
    insert_namespaced_manifest(
        pool,
        "basenames",
        chain,
        "ens_v1_resolver_l1",
        1,
        "fixture",
        "tests/project-shared-foreign-v1-resolver.toml",
        resolver_declaration_payload("basenames", chain, shared_resolver),
    )
    .await?;

    let source_instance = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind
         ) VALUES ($1, $2, 'contract')",
    )
    .bind(source_instance)
    .bind(chain)
    .execute(pool)
    .await?;
    for address in [shared_resolver, undeclared_resolver] {
        let resolver_instance = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO contract_instances (
                 contract_instance_id, chain_id, contract_kind
             ) VALUES ($1, $2, 'contract')",
        )
        .bind(resolver_instance)
        .bind(chain)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO contract_instance_addresses (
                 contract_instance_id, chain_id, address,
                 active_from_block_number, active_from_block_hash,
                 source_manifest_id
             ) VALUES ($1, $2, lower($3), 1, $4, $5)",
        )
        .bind(resolver_instance)
        .bind(chain)
        .bind(address)
        .bind(block_hash(chain, 1))
        .bind(origin_manifest)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO discovery_edges (
                 chain_id, edge_kind, from_contract_instance_id,
                 to_contract_instance_id, discovery_source, admission_basis,
                 source_manifest_id, active_from_block_number,
                 active_from_block_hash, canonicality_state
             ) VALUES (
                 $1, 'resolver', $2, $3, 'fixture', 'reachable_from_root',
                 $4, 1, $5, 'canonical'
             )",
        )
        .bind(chain)
        .bind(source_instance)
        .bind(resolver_instance)
        .bind(origin_manifest)
        .bind(block_hash(chain, 1))
        .execute(pool)
        .await?;
    }

    for (resource, label) in [(alice_resource, "alice"), (bob_resource, "bob")] {
        let node = format!("{:#x}", raw_namehash(&[label.as_bytes(), b"eth"]));
        let logical_name_id = format!("ens:{node}");
        let binding_id = if label == "alice" {
            Uuid::parse_str("00000000-0000-0000-0000-000000000d11")?
        } else {
            Uuid::parse_str("00000000-0000-0000-0000-000000000d12")?
        };
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES ($1::uuid, $2, $3, 1, 'canonical')",
        )
        .bind(resource)
        .bind(chain)
        .bind(block_hash(chain, 1))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES (
                 $1, 'ens', $2, ARRAY[$3, 'eth'], decode('00', 'hex'), $4,
                 ARRAY[$5, $6], $7, 'active', $8, $9, 1, 'canonical'
             )",
        )
        .bind(&logical_name_id)
        .bind(format!("{label}.eth"))
        .bind(label)
        .bind(&node)
        .bind(format!("{:#x}", keccak256(label.as_bytes())))
        .bind(format!("{:#x}", keccak256(b"eth")))
        .bind(NORMALIZER)
        .bind(chain)
        .bind(block_hash(chain, 1))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind,
                 authority_arm, active_from, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES (
                 $1, $2, $3::uuid, 'declared_registry_path', 'ens_v2',
                 to_timestamp(1), $4, $5, 1, 'canonical'
             )",
        )
        .bind(binding_id)
        .bind(&logical_name_id)
        .bind(resource)
        .bind(chain)
        .bind(block_hash(chain, 1))
        .execute(pool)
        .await?;
        let pointer_family = if label == "alice" {
            "ens_v2_registry_l1"
        } else {
            "ens_v2_root_l1"
        };
        insert_event(
            pool,
            chain,
            2,
            Some(&logical_name_id),
            Some(resource),
            "ResolverChanged",
            pointer_family,
            json!({"resolver": shared_resolver}),
            json!({}),
        )
        .await?;
        insert_event(
            pool,
            chain,
            3,
            None,
            None,
            "RecordVersionChanged",
            "ens_v1_resolver_l1",
            json!({
                "node": node,
                "resolver": shared_resolver,
                "record_version": 1
            }),
            json!({"emitting_address": shared_resolver}),
        )
        .await?;
        insert_event(
            pool,
            chain,
            3,
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({
                "node": node,
                "resolver": shared_resolver,
                "record_key": "text:url",
                "record_family": "text",
                "selector_key": "url",
                "value_retained": true,
                "value": format!("https://{label}.shared.example.test")
            }),
            json!({"emitting_address": shared_resolver}),
        )
        .await?;
    }
    Ok(v1_manifest)
}

async fn seed_reconvergence_fixture(pool: &PgPool, chain: &str) -> Result<i64> {
    seed_lineage(pool, chain, 1).await?;
    let resolver_manifest = insert_manifest(
        pool,
        chain,
        "ens_v2_resolver_l1",
        "tests/project-v2-resolver.toml",
        json!({"resolver_implementations":[]}),
    )
    .await?;
    insert_event(
        pool,
        chain,
        1,
        None,
        None,
        "Upgraded",
        "ens_v2_resolver_l1",
        json!({
            "proxy_address":"0x00000000000000000000000000000000000000d1",
            "implementation":"0x00000000000000000000000000000000000000c1"
        }),
        json!({"emitting_address":"0x00000000000000000000000000000000000000d1"}),
    )
    .await?;
    insert_event(
        pool,
        chain,
        1,
        None,
        None,
        "Upgraded",
        "ens_v2_registry_l1",
        json!({
            "proxy_address":"0x00000000000000000000000000000000000000e1",
            "implementation":"0x00000000000000000000000000000000000000e2"
        }),
        json!({"emitting_address":"0x00000000000000000000000000000000000000e1"}),
    )
    .await?;
    Ok(resolver_manifest)
}

async fn seed_raw_registrar_transfer_fixture(
    pool: &PgPool,
    chain: &str,
    include_registry_owner: bool,
) -> Result<()> {
    // BaseRegistrar inherits this ERC-721 transfer path, which updates token ownership and emits
    // Transfer; registry ownership changes through the separate reclaim call.
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L24 @ basenames@1809bbc)
    // (upstream: .refs/basenames/lib/solady/src/tokens/ERC721.sol:L287 @ basenames@1809bbc)
    // (upstream: .refs/basenames/lib/solady/src/tokens/ERC721.sol:L307 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L321 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L329 @ basenames@1809bbc)
    seed_lineage(pool, chain, 5).await?;
    insert_declared_source_manifest_events(
        pool,
        "basenames",
        chain,
        "basenames_base_registrar",
        "registrar",
        REGISTRAR,
        &[
            (
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &[
                    "RegistrationGranted",
                    "ExpiryChanged",
                    "PermissionChanged",
                    "SurfaceUnbound",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                    "PreimageObserved",
                ],
            ),
            (
                "Transfer",
                "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
                &["registrar"],
                &[
                    "TokenControlTransferred",
                    "PermissionChanged",
                    "SurfaceUnbound",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                ],
            ),
        ],
    )
    .await?;
    insert_declared_source_manifest_events(
        pool,
        "basenames",
        chain,
        "basenames_base_registry",
        "registry",
        REGISTRY,
        &[
            (
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry"],
                &[
                    "SubregistryChanged",
                    "AuthorityTransferred",
                    "PermissionChanged",
                ],
            ),
            (
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry"],
                &["ResolverChanged", "PermissionChanged"],
            ),
            (
                "Transfer",
                "event Transfer(bytes32 indexed node, address owner)",
                &["registry"],
                &["AuthorityTransferred", "PermissionChanged"],
            ),
        ],
    )
    .await?;
    insert_declared_source_manifest_events(
        pool,
        "basenames",
        chain,
        "basenames_base_resolver",
        "l2_resolver",
        RESOLVER,
        &[(
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            &[],
            &["RecordChanged"],
        )],
    )
    .await?;

    let base_eth_node = raw_namehash(&[b"base", b"eth"]);
    let alice_node = raw_namehash(&[b"alice", b"base", b"eth"]);
    let alice_label = B256::from(keccak256(b"alice"));
    if include_registry_owner {
        let registry_owner = NewOwner {
            node: base_eth_node,
            label: alice_label,
            owner: OWNER.parse::<Address>()?,
        }
        .encode_log_data();
        insert_raw_event(
            pool,
            chain,
            1,
            REGISTRY,
            registry_owner.topics(),
            registry_owner.data.as_ref(),
        )
        .await?;
    }
    let registration = NameRegistered {
        name: "alice".into(),
        label: alice_label,
        owner: OWNER.parse::<Address>()?,
        expires: U256::from(4_000_000_000_u64),
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        2,
        REGISTRAR,
        registration.topics(),
        registration.data.as_ref(),
    )
    .await?;
    let resolver = NewResolver {
        node: alice_node,
        resolver: RESOLVER.parse::<Address>()?,
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        3,
        REGISTRY,
        resolver.topics(),
        resolver.data.as_ref(),
    )
    .await?;
    let record = TextChanged {
        node: alice_node,
        indexedKey: keccak256(b"url"),
        key: "url".into(),
        value: "https://resolver-transfer.example.test".into(),
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        4,
        RESOLVER,
        record.topics(),
        record.data.as_ref(),
    )
    .await?;
    let transfer = Transfer {
        from: OWNER.parse::<Address>()?,
        to: TRANSFER_OWNER.parse::<Address>()?,
        tokenId: U256::from_be_bytes(alice_label.0),
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        5,
        REGISTRAR,
        transfer.topics(),
        transfer.data.as_ref(),
    )
    .await?;
    Ok(())
}

async fn seed_raw_registration_fixture(pool: &PgPool, chain: &str) -> Result<()> {
    seed_lineage(pool, chain, 5).await?;
    insert_declared_source_manifest(
        pool,
        chain,
        "ens_v1_wrapper_l1",
        "name_wrapper",
        WRAPPER,
        "NameWrapped",
        "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
        &["name_wrapper"],
        &[
            "TokenControlTransferred",
            "ExpiryChanged",
            "PermissionScopeChanged",
            "SurfaceUnbound",
            "SurfaceBound",
            "AuthorityEpochChanged",
            "ResolverChanged",
            "PreimageObserved",
        ],
    )
    .await?;
    insert_declared_source_manifest(
        pool,
        chain,
        "ens_v1_registrar_l1",
        "registrar",
        REGISTRAR,
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &[
            "RegistrationGranted",
            "ExpiryChanged",
            "PermissionChanged",
            "SurfaceUnbound",
            "SurfaceBound",
            "AuthorityEpochChanged",
            "ResolverChanged",
            "PreimageObserved",
        ],
    )
    .await?;
    insert_declared_source_manifest(
        pool,
        chain,
        "ens_v1_registry_l1",
        "registry",
        REGISTRY,
        "NewResolver",
        "event NewResolver(bytes32 indexed node, address resolver)",
        &["registry"],
        &["ResolverChanged", "PermissionChanged"],
    )
    .await?;
    insert_declared_source_manifest(
        pool,
        chain,
        "ens_v1_resolver_l1",
        "public_resolver",
        RESOLVER,
        "TextChanged",
        "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
        &[],
        &["RecordChanged"],
    )
    .await?;
    insert_declared_source_manifest(
        pool,
        chain,
        "ens_v1_reverse_l1",
        "reverse_registrar",
        REVERSE_REGISTRAR,
        "ReverseClaimed",
        "event ReverseClaimed(address indexed addr, bytes32 indexed node)",
        &["reverse_registrar"],
        &["ReverseChanged"],
    )
    .await?;

    let eth_node = raw_namehash(&[b"eth"]);
    let alice_node = raw_namehash(&[b"alice", b"eth"]);
    let wrapped = NameWrapped {
        node: eth_node,
        name: b"\x03eth\0".to_vec().into(),
        owner: OWNER.parse::<Address>()?,
        fuses: 0,
        expiry: 4_000_000_000,
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        1,
        WRAPPER,
        wrapped.topics(),
        wrapped.data.as_ref(),
    )
    .await?;

    let registration = NameRegistered {
        name: "alice".into(),
        label: B256::from(keccak256(b"alice")),
        owner: OWNER.parse::<Address>()?,
        expires: U256::from(4_000_000_000u64),
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        2,
        REGISTRAR,
        registration.topics(),
        registration.data.as_ref(),
    )
    .await?;

    let child = NewOwner {
        node: eth_node,
        label: B256::from(keccak256(b"alice")),
        owner: OWNER.parse::<Address>()?,
    }
    .encode_log_data();
    insert_raw_event_at(
        pool,
        chain,
        2,
        1,
        1,
        REGISTRY,
        child.topics(),
        child.data.as_ref(),
    )
    .await?;

    let resolver = NewResolver {
        node: alice_node,
        resolver: RESOLVER.parse::<Address>()?,
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        3,
        REGISTRY,
        resolver.topics(),
        resolver.data.as_ref(),
    )
    .await?;

    let record = TextChanged {
        node: alice_node,
        indexedKey: keccak256(b"url"),
        key: "url".into(),
        value: "https://example.test".into(),
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        4,
        RESOLVER,
        record.topics(),
        record.data.as_ref(),
    )
    .await?;

    let reverse_label = OWNER.trim_start_matches("0x").as_bytes();
    let reverse = ReverseClaimed {
        addr: OWNER.parse::<Address>()?,
        node: raw_namehash(&[reverse_label, b"addr", b"reverse"]),
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        5,
        REVERSE_REGISTRAR,
        reverse.topics(),
        reverse.data.as_ref(),
    )
    .await?;
    Ok(())
}

// Recorded Sepolia premigration population and operation split:
// (upstream: .refs/ens_v2/contracts/deployments/sepolia/.premigration.json:L2-L17 @ ens_v2@a971bd64)
// This is population and operation-class evidence only; the fixture declares test addresses and
// does not treat the live redeployment as admitted deployment evidence.
async fn seed_raw_v2_reservation_fixture(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
) -> Result<(String, U256)> {
    seed_raw_registration_fixture(pool, chain).await?;
    insert_lineage_block(pool, chain, 6).await?;
    insert_lineage_block(pool, chain, 7).await?;
    if source_family != "ens_v2_root_l1" {
        declare_sepolia_post_audit_profile(pool, chain).await?;
    }
    let role = if source_family == "ens_v2_root_l1" {
        "root_registry"
    } else {
        "registry"
    };
    insert_declared_source_manifest_events(
        pool,
        "ens",
        chain,
        source_family,
        role,
        V2_REGISTRY,
        &[
            (
                "LabelReserved",
                "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)",
                &[role],
                &["RegistrationReserved"],
            ),
            (
                "ResolverUpdated",
                "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
                &[role],
                &["ResolverChanged"],
            ),
            (
                "ExpiryUpdated",
                "event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)",
                &[role],
                &["ExpiryChanged", "RegistrationRenewed"],
            ),
            (
                "LabelUnregistered",
                "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
                &[role],
                &["RegistrationReleased"],
            ),
        ],
    )
    .await?;
    let registry_manifest: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions
         WHERE chain_id = $1 AND source_family = $2",
    )
    .bind(chain)
    .bind(source_family)
    .fetch_one(pool)
    .await?;
    if source_family == "ens_v2_root_l1" {
        sqlx::query(
            "UPDATE manifest_versions
             SET deployment_label = 'ens_v2_sepolia_post_audit',
                 manifest_payload = jsonb_set(
                     manifest_payload, '{deployment_epoch}',
                     '\"ens_v2_sepolia_post_audit\"'::jsonb
                 )
             WHERE manifest_id = $1",
        )
        .bind(registry_manifest)
        .execute(pool)
        .await?;
    }
    let resolver_rule = json!({
        "edge_kind": "resolver",
        "from_role": role,
        "admission": "reachable_from_root"
    });
    sqlx::query(
        "UPDATE manifest_versions
         SET manifest_payload = jsonb_set(
             manifest_payload, '{discovery_rules}', jsonb_build_array($2::jsonb)
         )
         WHERE manifest_id = $1",
    )
    .bind(registry_manifest)
    .bind(&resolver_rule)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_discovery_rules (
             manifest_id, edge_kind, from_role, admission, rule_payload
         ) VALUES ($1, 'resolver', $2, 'reachable_from_root', $3)",
    )
    .bind(registry_manifest)
    .bind(role)
    .bind(resolver_rule)
    .execute(pool)
    .await?;

    let label = if source_family == "ens_v2_root_l1" {
        "eth"
    } else {
        "alice"
    };
    let mut token_bytes = *keccak256(label.as_bytes());
    token_bytes[28..].copy_from_slice(&0_u32.to_be_bytes());
    let token_id = U256::from_be_bytes(token_bytes);
    let reserved = LabelReserved {
        tokenId: token_id,
        labelHash: keccak256(label.as_bytes()),
        label: label.into(),
        expiry: 4_000_000_000,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_raw_event_at(
        pool,
        chain,
        6,
        1,
        1,
        V2_REGISTRY,
        reserved.topics(),
        reserved.data.as_ref(),
    )
    .await?;
    let resolver = ResolverUpdated {
        tokenId: token_id,
        resolver: EQUIVALENCE_V2_RESOLVER.parse()?,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_raw_event_at(
        pool,
        chain,
        6,
        1,
        2,
        V2_REGISTRY,
        resolver.topics(),
        resolver.data.as_ref(),
    )
    .await?;

    let raw_labels: &[&[u8]] = if source_family == "ens_v2_root_l1" {
        &[b"eth"]
    } else {
        &[b"alice", b"eth"]
    };
    Ok((format!("ens:{:#x}", raw_namehash(raw_labels)), token_id))
}

async fn seed_authority_classifier_case(
    pool: &PgPool,
    chain: &str,
    logical_name_id: &str,
    bindings: EnsArmSet,
    events: EnsArmSet,
    v2_binding_block: i64,
    v1_active_to: Option<i64>,
) -> Result<ClassifierBindings> {
    let namehash = logical_name_id
        .strip_prefix("ens:")
        .expect("classifier logical IDs use the ENS namespace");
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens', 'classifier.eth', ARRAY['classifier','eth'],
             decode('00', 'hex'), $2, ARRAY[$2,'0xeth'], $3,
             'active', $4, $5, 1, 'canonical'
         )",
    )
    .bind(logical_name_id)
    .bind(namehash)
    .bind(NORMALIZER)
    .bind(chain)
    .bind(block_hash(chain, 1))
    .execute(pool)
    .await?;

    let mut seeded = ClassifierBindings { v1: None, v2: None };
    if bindings.includes_v1() {
        let resource = Uuid::new_v4();
        let binding = Uuid::new_v4();
        insert_classifier_resource_and_binding(
            pool,
            chain,
            logical_name_id,
            "ens_v1",
            resource,
            binding,
            1,
            v1_active_to,
        )
        .await?;
        seeded.v1 = Some((binding, resource));
    }
    if bindings.includes_v2() {
        let resource = Uuid::new_v4();
        let binding = Uuid::new_v4();
        insert_classifier_resource_and_binding(
            pool,
            chain,
            logical_name_id,
            "ens_v2",
            resource,
            binding,
            v2_binding_block,
            None,
        )
        .await?;
        seeded.v2 = Some((binding, resource));
    }
    if events.includes_v1() {
        let resource_id = seeded.v1.map(|row| row.1.to_string());
        insert_event(
            pool,
            chain,
            2,
            Some(logical_name_id),
            resource_id.as_deref(),
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            json!({"status":"registered","registrant":OWNER}),
            json!({}),
        )
        .await?;
    }
    if events.includes_v2() {
        let resource_id = seeded.v2.map(|row| row.1.to_string());
        insert_event(
            pool,
            chain,
            v2_binding_block,
            Some(logical_name_id),
            resource_id.as_deref(),
            "RegistrationGranted",
            "ens_v2_registry_l1",
            json!({"status":"registered","registrant":OWNER}),
            json!({}),
        )
        .await?;
    }
    for source_family in ["ens_v2_registry_l1", "ens_v2_registrar_l1"] {
        let manifest_id: i64 = sqlx::query_scalar(
            "SELECT manifest_id FROM manifest_versions
             WHERE chain_id = $1 AND source_family = $2
               AND deployment_label = 'ens_v2_sepolia_post_audit'",
        )
        .bind(chain)
        .bind(source_family)
        .fetch_one(pool)
        .await?;
        insert_event(
            pool,
            chain,
            1,
            Some(logical_name_id),
            None,
            "PreimageObserved",
            source_family,
            json!({"fixture":"exact_name_profile_admission"}),
            json!({}),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events SET source_manifest_id = $1
             WHERE chain_id = $2 AND logical_name_id = $3
               AND source_family = $4 AND event_kind = 'PreimageObserved'",
        )
        .bind(manifest_id)
        .bind(chain)
        .bind(logical_name_id)
        .bind(source_family)
        .execute(pool)
        .await?;
    }
    Ok(seeded)
}

#[allow(clippy::too_many_arguments)]
async fn insert_classifier_resource_and_binding(
    pool: &PgPool,
    chain: &str,
    logical_name_id: &str,
    authority_arm: &str,
    resource_id: Uuid,
    binding_id: Uuid,
    block_number: i64,
    active_to: Option<i64>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, $4, 'canonical')",
    )
    .bind(resource_id)
    .bind(chain)
    .bind(block_hash(chain, block_number))
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query("WITH lineage AS (INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number, canonicality_state) SELECT resource_id, chain_id, block_hash, block_number, canonicality_state FROM resources WHERE resource_id = $1) UPDATE resources SET token_lineage_id = $1 WHERE resource_id = $1").bind(resource_id).execute(pool).await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, active_to, chain_id, block_hash, block_number,
             provenance, canonicality_state
         ) VALUES (
             $1, $2, $3, 'declared_registry_path', $4, to_timestamp($5),
             CASE WHEN $8::bigint IS NULL THEN NULL ELSE to_timestamp($8) END,
             $6, $7, $5, '{\"transaction_index\":0,\"log_index\":0}'::jsonb,
             'canonical'
         )",
    )
    .bind(binding_id)
    .bind(logical_name_id)
    .bind(resource_id)
    .bind(authority_arm)
    .bind(block_number)
    .bind(chain)
    .bind(block_hash(chain, block_number))
    .bind(active_to)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_raw_v2_reservation_expiry(
    pool: &PgPool,
    chain: &str,
    token_id: U256,
    new_expiry: u64,
) -> Result<()> {
    let updated = ExpiryUpdated {
        tokenId: token_id,
        newExpiry: new_expiry,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        7,
        V2_REGISTRY,
        updated.topics(),
        updated.data.as_ref(),
    )
    .await
}

async fn insert_raw_v2_reservation_release(
    pool: &PgPool,
    chain: &str,
    token_id: U256,
) -> Result<()> {
    let released = LabelUnregistered {
        tokenId: token_id,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        7,
        V2_REGISTRY,
        released.topics(),
        released.data.as_ref(),
    )
    .await
}

async fn seed_raw_reservation_release_then_registration_before_v1(
    pool: &PgPool,
    chain: &str,
) -> Result<String> {
    seed_lineage(pool, chain, 3).await?;
    insert_declared_source_manifest_events(
        pool,
        "ens",
        chain,
        "ens_v2_root_l1",
        "root_registry",
        V2_REGISTRY,
        &[
            (
                "LabelReserved",
                "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)",
                &["root_registry"],
                &["RegistrationReserved"],
            ),
            (
                "LabelUnregistered",
                "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
                &["root_registry"],
                &["RegistrationReleased"],
            ),
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["root_registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["root_registry"],
                &["TokenResourceLinked"],
            ),
        ],
    )
    .await?;
    let v2_manifest_id: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions
         WHERE chain_id = $1 AND source_family = 'ens_v2_root_l1'",
    )
    .bind(chain)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "UPDATE manifest_versions
         SET deployment_label = 'ens_v2_sepolia_post_audit',
             manifest_payload = jsonb_set(
                 manifest_payload, '{deployment_epoch}',
                 '\"ens_v2_sepolia_post_audit\"'::jsonb
             )
         WHERE manifest_id = $1",
    )
    .bind(v2_manifest_id)
    .execute(pool)
    .await?;
    insert_declared_source_manifest(
        pool,
        chain,
        "ens_v1_wrapper_l1",
        "name_wrapper",
        WRAPPER,
        "NameWrapped",
        "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
        &["name_wrapper"],
        &[
            "TokenControlTransferred",
            "ExpiryChanged",
            "PermissionScopeChanged",
            "SurfaceUnbound",
            "SurfaceBound",
            "AuthorityEpochChanged",
            "ResolverChanged",
            "PreimageObserved",
        ],
    )
    .await?;

    let label = "ordinary";
    let mut token_bytes = *keccak256(label.as_bytes());
    token_bytes[28..].copy_from_slice(&0_u32.to_be_bytes());
    let token_id = U256::from_be_bytes(token_bytes);
    let label_hash = keccak256(label.as_bytes());
    let reserved = LabelReserved {
        tokenId: token_id,
        labelHash: label_hash,
        label: label.into(),
        expiry: 4_000_000_000,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_raw_event_at(
        pool,
        chain,
        1,
        1,
        1,
        V2_REGISTRY,
        reserved.topics(),
        reserved.data.as_ref(),
    )
    .await?;
    let released = LabelUnregistered {
        tokenId: token_id,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_raw_event_at(
        pool,
        chain,
        2,
        1,
        1,
        V2_REGISTRY,
        released.topics(),
        released.data.as_ref(),
    )
    .await?;
    let registered = LabelRegistered {
        tokenId: token_id,
        labelHash: label_hash,
        label: label.into(),
        owner: OWNER.parse()?,
        expiry: 4_000_000_000,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_raw_event_at(
        pool,
        chain,
        2,
        2,
        2,
        V2_REGISTRY,
        registered.topics(),
        registered.data.as_ref(),
    )
    .await?;
    let linked = TokenResource {
        tokenId: token_id,
        resource: token_id,
    }
    .encode_log_data();
    insert_raw_event_at(
        pool,
        chain,
        2,
        2,
        3,
        V2_REGISTRY,
        linked.topics(),
        linked.data.as_ref(),
    )
    .await?;
    let wrapped = NameWrapped {
        node: raw_namehash(&[b"ordinary"]),
        name: b"\x08ordinary\0".to_vec().into(),
        owner: OWNER.parse()?,
        fuses: 0,
        expiry: 4_000_000_000,
    }
    .encode_log_data();
    insert_raw_event_at(
        pool,
        chain,
        3,
        1,
        1,
        WRAPPER,
        wrapped.topics(),
        wrapped.data.as_ref(),
    )
    .await?;

    Ok(format!("ens:{:#x}", raw_namehash(&[b"ordinary"])))
}

async fn assert_reservation_selects_v1(
    pool: &PgPool,
    chain: &str,
    logical_name_id: &str,
    source_family: &str,
) -> Result<()> {
    let v1_binding: (Uuid, Uuid) = sqlx::query_as(
        "SELECT surface_binding_id, resource_id
         FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2
           AND authority_arm = 'ens_v1' AND active_to IS NULL",
    )
    .bind(chain)
    .bind(logical_name_id)
    .fetch_one(pool)
    .await?;
    type SelectedReservationRow = (
        Option<String>,
        String,
        Option<String>,
        Option<Uuid>,
        Option<Uuid>,
        Option<String>,
    );
    let selected: SelectedReservationRow = sqlx::query_as(
        "SELECT provenance #>> '{authority_selection,authority_arm}',
                support_status, unsupported_reason, surface_binding_id, resource_id,
                declared_summary #>> '{resolver,address}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        selected,
        (
            Some("ens_v1".into()),
            "supported".into(),
            None,
            Some(v1_binding.0),
            Some(v1_binding.1),
            Some(RESOLVER.into()),
        )
    );
    assert_eq!(
        record_entry_pairs(pool, &v1_binding.1.to_string()).await?,
        vec![("text:url".into(), "https://example.test".into())]
    );

    let (v2_bindings, reservations, reservation_resources, mirror_resolvers): (i64, i64, i64, i64) =
        sqlx::query_as(
            "SELECT
             (SELECT count(*) FROM surface_bindings
              WHERE chain_id = $1 AND logical_name_id = $2
                AND authority_arm = 'ens_v2'),
             (SELECT count(*) FROM normalized_events
              WHERE chain_id = $1 AND logical_name_id = $2
                AND event_kind = 'RegistrationReserved'),
             (SELECT count(*) FROM normalized_events event
              JOIN resources resource ON resource.resource_id = event.resource_id
              WHERE event.chain_id = $1 AND event.logical_name_id = $2
                AND event.event_kind = 'RegistrationReserved'),
             (SELECT count(*) FROM normalized_events
              WHERE chain_id = $1 AND logical_name_id = $2
                AND event_kind = 'ResolverChanged'
                AND source_family = $3)",
        )
        .bind(chain)
        .bind(logical_name_id)
        .bind(source_family)
        .fetch_one(pool)
        .await?;
    assert_eq!(
        v2_bindings, 0,
        "a reservation must not synthesize a binding"
    );
    assert_eq!(reservations, 1, "reservation history remains audit-visible");
    assert_eq!(
        reservation_resources, 1,
        "the reservation resource remains retained"
    );
    assert_eq!(
        mirror_resolvers, 1,
        "the reservation resolver remains retained"
    );
    Ok(())
}

async fn seed_dual_open_cross_arm_fixture(
    pool: &PgPool,
    chain: &str,
    v2_block: i64,
) -> Result<String> {
    seed_raw_registration_fixture(pool, chain).await?;
    // (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L18-L25 @ ens_v2@a971bd64)
    // (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L36-L39 @ ens_v2@a971bd64)
    // (upstream: .refs/ens_v2/contracts/src/utils/LibLabel.sol:L7-L17 @ ens_v2@a971bd64)
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L426-L471 @ ens_v2@a971bd64)
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L632-L651 @ ens_v2@a971bd64)
    insert_declared_source_manifest_events(
        pool,
        "ens",
        chain,
        "ens_v2_registry_l1",
        "registry",
        V2_REGISTRY,
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
        ],
    )
    .await?;

    let label = "alice";
    let mut token_bytes = *keccak256(label.as_bytes());
    token_bytes[28..].copy_from_slice(&0_u32.to_be_bytes());
    let token_id = U256::from_be_bytes(token_bytes);
    let registered = LabelRegistered {
        tokenId: token_id,
        labelHash: keccak256(label.as_bytes()),
        label: label.into(),
        owner: OWNER.parse()?,
        expiry: 4_000_000_000,
        sender: SENDER.parse()?,
    }
    .encode_log_data();
    insert_raw_event_at(
        pool,
        chain,
        v2_block,
        1,
        1,
        V2_REGISTRY,
        registered.topics(),
        registered.data.as_ref(),
    )
    .await?;
    let linked = TokenResource {
        tokenId: token_id,
        resource: token_id,
    }
    .encode_log_data();
    insert_raw_event_at(
        pool,
        chain,
        v2_block,
        1,
        2,
        V2_REGISTRY,
        linked.topics(),
        linked.data.as_ref(),
    )
    .await?;

    Ok(format!("ens:{:#x}", raw_namehash(&[b"alice", b"eth"])))
}

// Hand-seed Project's intended post-transition state: a closed ENSv1 predecessor
// and current ENSv2 successor. Interpret-driven publication proof is deferred to
// #822 (`unwrapped`) and the #503 e2e harness (wrapper/locked).
async fn seed_closed_predecessor_cross_arm_fixture(
    pool: &PgPool,
    chain: &str,
    v2_block: i64,
    v1_block: i64,
    boundary_block: i64,
) -> Result<String> {
    seed_lineage(pool, chain, 5).await?;
    declare_sepolia_post_audit_profile(pool, chain).await?;
    for source_family in ["ens_v2_registry_l1", "ens_v2_registrar_l1"] {
        insert_namespaced_manifest(
            pool,
            "ens",
            chain,
            source_family,
            1,
            "ens_v2_sepolia_post_audit",
            &format!("tests/{chain}-{source_family}.toml"),
            json!({"capability_flags":{"exact_name_profile":{"status":"supported"}}}),
        )
        .await?;
    }
    let logical_name_id = format!("ens:{:#x}", raw_namehash(&[b"alice", b"eth"]));
    let seeded = seed_authority_classifier_case(
        pool,
        chain,
        &logical_name_id,
        EnsArmSet::Both,
        EnsArmSet::Both,
        v2_block,
        Some(boundary_block),
    )
    .await?;
    if v1_block != 1 {
        let (binding, resource) = seeded.v1.expect("seeded ENSv1 binding");
        sqlx::query(
            "UPDATE surface_bindings
             SET active_from = to_timestamp($1), block_number = $1, block_hash = $2
             WHERE surface_binding_id = $3",
        )
        .bind(v1_block)
        .bind(block_hash(chain, v1_block))
        .bind(binding)
        .execute(pool)
        .await?;
        sqlx::query(
            "UPDATE resources SET block_number = $1, block_hash = $2 WHERE resource_id = $3",
        )
        .bind(v1_block)
        .bind(block_hash(chain, v1_block))
        .bind(resource)
        .execute(pool)
        .await?;
    }
    sqlx::query("UPDATE normalized_events SET transaction_hash = concat(chain_id, '-v2-registration'), transaction_index = 0, log_index = 0 WHERE chain_id = $1 AND logical_name_id = $2 AND source_family = 'ens_v2_registry_l1' AND event_kind = 'RegistrationGranted'").bind(chain).bind(&logical_name_id).execute(pool).await?;
    assert!(seeded.v1.is_some());
    assert!(seeded.v2.is_some());
    Ok(logical_name_id)
}

// An inert manifest whose deployment epoch makes Project classify the chain
// under the [Sepolia deployment profile](../../../docs/glossary.md#deployment-profile).
// Selector-only fixtures normally seed the intended closed-predecessor state;
// dedicated Sepolia tests retain both arms to pin publication without the
// Mainnet guardrail. Declare this before the first projection so a
// deployment-profile-sensitive field cannot change mid-test.
async fn declare_sepolia_post_audit_profile(pool: &PgPool, chain: &str) -> Result<()> {
    insert_namespaced_manifest(
        pool,
        "ens",
        chain,
        "ens_v2_root_l1",
        1,
        "ens_v2_sepolia_post_audit",
        &format!("tests/raw-{chain}-sepolia-post-audit.toml"),
        json!({
            "manifest_version": 1,
            "namespace": "ens",
            "source_family": "ens_v2_root_l1",
            "chain": chain,
            "deployment_epoch": "ens_v2_sepolia_post_audit",
            "rollout_status": "active",
            "normalizer_version": NORMALIZER,
            "capability_flags": {},
            "roots": [],
            "contracts": [],
            "discovery_rules": [],
            "abi": {"events": [], "calls": []}
        }),
    )
    .await?;
    Ok(())
}

// A proofless released-v2-authority starting point: the fixture's v1 arm is
// withdrawn (binding closed as v2, v1 events orphaned) and the v2 registration
// is released at block 6 with no transition proof.
async fn seed_proofless_released_v2_authority(
    pool: &PgPool,
    chain: &str,
) -> Result<(String, Uuid)> {
    let logical_name_id = seed_dual_open_cross_arm_fixture(pool, chain, 4).await?;
    InterpretEngine::new(pool.clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET authority_arm = 'ens_v2', active_to = to_timestamp(3)
         WHERE chain_id = $1 AND logical_name_id = $2
           AND authority_arm = 'ens_v1'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE normalized_events SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND logical_name_id = $2
           AND source_family LIKE 'ens_v1_%'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(pool)
    .await?;
    let released_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2
         ORDER BY block_number DESC, surface_binding_id DESC LIMIT 1",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_one(pool)
    .await?;
    insert_lineage_block(pool, chain, 6).await?;
    insert_event(
        pool,
        chain,
        6,
        Some(&logical_name_id),
        Some(&released_resource.to_string()),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({"status":"released","released_at":6}),
        json!({}),
    )
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(6)
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v2'
           AND active_to IS NULL",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(pool)
    .await?;
    Ok((logical_name_id, released_resource))
}

// A positive ENSv2 re-registration on a fresh resource, bound at `block`.
async fn insert_v2_regrant(
    pool: &PgPool,
    chain: &str,
    logical_name_id: &str,
    block: i64,
) -> Result<(Uuid, Uuid)> {
    let regrant_binding = Uuid::new_v4();
    let regrant_resource = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, $4, 'canonical')",
    )
    .bind(regrant_resource)
    .bind(chain)
    .bind(block_hash(chain, block))
    .bind(block)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number,
             provenance, canonicality_state
         )
         SELECT $1, logical_name_id, $2, binding_kind, authority_arm,
                to_timestamp($4), chain_id, $3, $4,
                jsonb_build_object('transaction_index', 0, 'log_index', 0),
                canonicality_state
         FROM surface_bindings
         WHERE chain_id = $5 AND logical_name_id = $6 AND authority_arm = 'ens_v2'
         ORDER BY block_number DESC, surface_binding_id DESC
         LIMIT 1",
    )
    .bind(regrant_binding)
    .bind(regrant_resource)
    .bind(block_hash(chain, block))
    .bind(block)
    .bind(chain)
    .bind(logical_name_id)
    .execute(pool)
    .await?;
    insert_event(
        pool,
        chain,
        block,
        Some(logical_name_id),
        Some(&regrant_resource.to_string()),
        "RegistrationGranted",
        "ens_v2_registry_l1",
        json!({"status":"registered","registrant":OWNER,"expiry":5_000_000_000_u64}),
        json!({}),
    )
    .await?;
    Ok((regrant_binding, regrant_resource))
}

fn assert_active_v2_regrant(row: &Value, regrant_binding: Uuid, regrant_resource: Uuid) {
    let summary = &row["declared_summary"]["registration"];
    let selection = &row["provenance"]["authority_selection"];
    assert_eq!(row["resource_id"], regrant_resource.to_string());
    assert_eq!(row["surface_binding_id"], regrant_binding.to_string());
    assert_eq!(summary["status"], "active");
    assert_eq!(summary["registrant"], OWNER);
    assert_eq!(summary["expiry"], 5_000_000_000_u64);
    assert_eq!(selection["authority_arm"], "ens_v2");
    assert_eq!(selection["lifecycle_state"], "registered");
    assert!(selection.get("proof_kind").is_none());
    assert_eq!(row["unsupported_reason"], "ensv2_exact_name_profile_shadow");
}

async fn insert_activated_authority_proof(
    pool: &PgPool,
    chain: &str,
    logical_name_id: &str,
    migration_path: &str,
    boundary_block: Option<i64>,
) -> Result<(Uuid, Uuid, String)> {
    let (binding_id, resource_id, block_number, transaction_index, log_index): (
        Uuid,
        Uuid,
        i64,
        i64,
        i64,
    ) = sqlx::query_as(
        "SELECT surface_binding_id, resource_id, block_number,
                COALESCE((provenance ->> 'transaction_index')::bigint, 1),
                COALESCE((provenance ->> 'log_index')::bigint, 1)
         FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v2'
         ORDER BY block_number DESC, surface_binding_id DESC
         LIMIT 1",
    )
    .bind(chain)
    .bind(logical_name_id)
    .fetch_one(pool)
    .await?;
    let block_number = boundary_block.unwrap_or(block_number);
    let proof_identity = format!("{chain}:MigrationApplied:authority-proof-fixture");
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, raw_fact_ref,
             derivation_kind, canonicality_state, before_state, after_state,
             migration_correlation_ids, consumer_visibility
         ) VALUES (
             $1, 'ens', $2, $3, 'MigrationApplied', 'ens_v2_migration_l1', 1,
             $4, $5, $6, $7, $8, $9, '{}'::jsonb, 'ens_v2_migration',
             'canonical', jsonb_build_object('authority_epoch', 'ens_v1'),
             jsonb_build_object(
                 'migration_path', $11::text,
                 'successor_binding', jsonb_build_object(
                     'authority_epoch', 'ens_v2', 'binding_id', $10::text,
                     'resource_id', $3::text
                 )
             ), ARRAY['authority-proof-fixture']::text[], 'activated'
         )",
    )
    .bind(&proof_identity)
    .bind(logical_name_id)
    .bind(resource_id)
    .bind(chain)
    .bind(block_number)
    .bind(block_hash(chain, block_number))
    .bind(format!("{chain}-authority-proof-tx"))
    .bind(transaction_index)
    .bind(log_index)
    .bind(binding_id)
    .bind(migration_path)
    .execute(pool)
    .await?;
    Ok((binding_id, resource_id, proof_identity))
}

async fn seed_authority_lifecycle_fixture(
    pool: &PgPool,
    chain: &str,
    migration_path: &str,
) -> Result<(String, String)> {
    let logical_name_id = seed_closed_predecessor_cross_arm_fixture(pool, chain, 4, 1, 4).await?;
    for block in 6..=9 {
        insert_lineage_block(pool, chain, block).await?;
    }
    let (_, v2_resource, proof_identity) =
        insert_activated_authority_proof(pool, chain, &logical_name_id, migration_path, None)
            .await?;
    let v1_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v1'
         ORDER BY block_number DESC LIMIT 1",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .fetch_one(pool)
    .await?;
    if migration_path != "unwrapped" {
        insert_event(
            pool,
            chain,
            3,
            Some(&logical_name_id),
            Some(&v1_resource.to_string()),
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            json!({
                "fuses":if migration_path == "locked_wrapped" {196_609} else {0},
                "wrapper_state":if migration_path == "locked_wrapped" {"locked"} else {"wrapped"}
            }),
            json!({}),
        )
        .await?;
    }
    for (kind, after) in [
        (
            "RegistrationGranted",
            json!({"status":"registered","registrant":TRANSFER_OWNER,"owner":"graveyard"}),
        ),
        ("ExpiryChanged", json!({"expiry":1_111})),
        ("AuthorityTransferred", json!({"owner":TRANSFER_OWNER})),
        ("ResolverChanged", json!({"resolver":RESOLVER})),
    ] {
        insert_event(
            pool,
            chain,
            6,
            Some(&logical_name_id),
            Some(&v1_resource.to_string()),
            kind,
            "ens_v1_registrar_l1",
            after,
            json!({}),
        )
        .await?;
    }
    for (kind, after) in [
        (
            "RegistrationRenewed",
            json!({"status":"registered","registrant":OWNER,"expiry":2_222}),
        ),
        ("AuthorityTransferred", json!({"owner":OWNER})),
        (
            "ResolverChanged",
            json!({"resolver":EQUIVALENCE_V2_RESOLVER}),
        ),
    ] {
        insert_event(
            pool,
            chain,
            7,
            Some(&logical_name_id),
            Some(&v2_resource.to_string()),
            kind,
            "ens_v2_registry_l1",
            after,
            json!({}),
        )
        .await?;
    }
    insert_event(
        pool,
        chain,
        8,
        Some(&logical_name_id),
        Some(&v2_resource.to_string()),
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({"status":"released","released_at":8}),
        json!({}),
    )
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(8)
         WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v2'",
    )
    .bind(chain)
    .bind(&logical_name_id)
    .execute(pool)
    .await?;
    insert_event(
        pool,
        chain,
        9,
        Some(&logical_name_id),
        Some(&v1_resource.to_string()),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":3_333}),
        json!({}),
    )
    .await?;
    Ok((logical_name_id, proof_identity))
}

#[allow(clippy::too_many_arguments)]
async fn insert_declared_source_manifest_events(
    pool: &PgPool,
    namespace: &str,
    chain: &str,
    source_family: &str,
    role: &str,
    address: &str,
    events: &[(&str, &str, &[&str], &[&str])],
) -> Result<()> {
    let abi_events: Vec<Value> = events
        .iter()
        .map(|(name, fragment, emitter_roles, normalized_events)| {
            json!({
                "name": name,
                "fragment": fragment,
                "emitter_roles": emitter_roles,
                "normalized_events": normalized_events
            })
        })
        .collect();
    let payload = json!({
        "manifest_version": 1,
        "namespace": namespace,
        "source_family": source_family,
        "chain": chain,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": role,
            "address": address,
            "proxy_kind": "none",
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": {"events": abi_events, "calls": []}
    });
    let manifest = insert_namespaced_manifest(
        pool,
        namespace,
        chain,
        source_family,
        1,
        "fixture",
        &format!("tests/raw-{source_family}.toml"),
        payload,
    )
    .await?;
    let instance = fixture_contract_instance_id(chain, source_family, role, address);
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind
         ) VALUES ($1, $2, 'contract')",
    )
    .bind(instance)
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind,
             start_block_number
         ) VALUES ($1, $2, 'contract', $3, $4, $5, $3, 'none', 0)",
    )
    .bind(manifest)
    .bind(chain)
    .bind(role)
    .bind(instance)
    .bind(address)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id
         ) VALUES ($1, $2, $3, 0, $4)",
    )
    .bind(instance)
    .bind(chain)
    .bind(address)
    .bind(manifest)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_declared_source_manifest(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
    role: &str,
    address: &str,
    event_name: &str,
    event_fragment: &str,
    emitter_roles: &[&str],
    normalized_events: &[&str],
) -> Result<()> {
    let mut abi_events = vec![json!({
        "name": event_name,
        "fragment": event_fragment,
        "emitter_roles": emitter_roles,
        "normalized_events": normalized_events
    })];
    if source_family == "ens_v1_registry_l1" {
        abi_events.push(json!({
            "name": "NewOwner",
            "fragment": "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
            "emitter_roles": ["registry"],
            "normalized_events": ["SubregistryChanged", "AuthorityTransferred"]
        }));
    }
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": source_family,
        "chain": chain,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": role,
            "address": address,
            "proxy_kind": "none",
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": {"events": abi_events, "calls": []}
    });
    let manifest = insert_manifest(
        pool,
        chain,
        source_family,
        &format!("tests/raw-{source_family}.toml"),
        payload,
    )
    .await?;
    let instance = fixture_contract_instance_id(chain, source_family, role, address);
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind
         ) VALUES ($1, $2, 'contract')",
    )
    .bind(instance)
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind,
             start_block_number
         ) VALUES ($1, $2, 'contract', $3, $4, $5, $3, 'none', 0)",
    )
    .bind(manifest)
    .bind(chain)
    .bind(role)
    .bind(instance)
    .bind(address)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id
         ) VALUES ($1, $2, $3, 0, $4)",
    )
    .bind(instance)
    .bind(chain)
    .bind(address)
    .bind(manifest)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_raw_event(
    pool: &PgPool,
    chain: &str,
    block_number: i64,
    emitting_address: &str,
    topics: &[B256],
    data: &[u8],
) -> Result<()> {
    insert_raw_event_at(
        pool,
        chain,
        block_number,
        0,
        0,
        emitting_address,
        topics,
        data,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_raw_event_at(
    pool: &PgPool,
    chain: &str,
    block_number: i64,
    transaction_index: i64,
    log_index: i64,
    emitting_address: &str,
    topics: &[B256],
    data: &[u8],
) -> Result<()> {
    let transaction_hash = format!("{chain}-transaction-{block_number}-{transaction_index}");
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, from_address, to_address
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT DO NOTHING",
    )
    .bind(chain)
    .bind(block_hash(chain, block_number))
    .bind(block_number)
    .bind(&transaction_hash)
    .bind(transaction_index)
    .bind(SENDER)
    .bind(emitting_address)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO raw_logs (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, emitting_address, topics, data
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(chain)
    .bind(block_hash(chain, block_number))
    .bind(block_number)
    .bind(transaction_hash)
    .bind(transaction_index)
    .bind(log_index)
    .bind(emitting_address)
    .bind(
        topics
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>(),
    )
    .bind(data)
    .execute(pool)
    .await?;
    Ok(())
}

fn raw_namehash(labels: &[&[u8]]) -> B256 {
    let mut node = B256::ZERO;
    for label in labels.iter().rev() {
        let mut input = [0_u8; 64];
        input[..32].copy_from_slice(node.as_slice());
        input[32..].copy_from_slice(keccak256(label).as_slice());
        node = keccak256(input);
    }
    node
}

async fn seed_lineage(pool: &PgPool, chain: &str, through: i64) -> Result<()> {
    for number in 0..=through {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')",
        )
        .bind(chain)
        .bind(block_hash(chain, number))
        .bind((number > 0).then(|| block_hash(chain, number - 1)))
        .bind(number)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn insert_lineage_block(pool: &PgPool, chain: &str, number: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')",
    )
    .bind(chain)
    .bind(block_hash(chain, number))
    .bind((number > 0).then(|| block_hash(chain, number - 1)))
    .bind(number)
    .execute(pool)
    .await?;
    Ok(())
}

async fn resolver_projection_row(pool: &PgPool, resolver_address: &str) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(current) FROM resolver_current current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(resolver_address)
    .fetch_optional(pool)
    .await?)
}

async fn permission_rows_for_resolver(pool: &PgPool, resolver_address: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM permissions_current
         WHERE scope_kind = 'resolver'
           AND scope_detail ->> 'chain_id' = $1
           AND lower(scope_detail ->> 'resolver_address') = lower($2)",
    )
    .bind(CHAIN)
    .bind(resolver_address)
    .fetch_one(pool)
    .await?)
}

async fn capture_resolver_redo_evidence(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    let statement = format!(
        r#"INSERT INTO project_redo_resolver_evidence (
               chain_id, event_identity, block_number, event_kind,
               source_family, resource_id,
               before_resolver_address, after_resolver_address
           )
           {REDO_RESOLVER_EVIDENCE_SELECT_SQL}
           ON CONFLICT (chain_id, event_identity) DO NOTHING"#,
    );
    sqlx::query(&statement)
        .bind(chain_id)
        .bind(from_block)
        .bind(to_block)
        .execute(pool)
        .await?;
    Ok(())
}

async fn seed_cross_family_binding_fixture(pool: &PgPool) -> Result<()> {
    seed_project_fixture(pool).await?;
    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND logical_name_id = 'ens:0xalice'
           AND event_kind IN ('ResolverChanged', 'RecordChanged')",
    )
    .bind(CHAIN)
    .execute(pool)
    .await?;
    insert_manifest(
        pool,
        CHAIN,
        "basenames_base_resolver",
        "tests/project-cross-family-basenames-resolver.toml",
        json!({
            "contracts":[{
                "role":"l2_resolver",
                "address":BASENAMES_RESOLVER,
                "proxy_kind":"none"
            }]
        }),
    )
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(FAMILY_BINDING_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens', 'family-binding.eth', ARRAY['family-binding','eth'],
             decode('00', 'hex'), '0xfamily-binding',
             ARRAY['0xfamily-binding-label','0xeth'], $2, 'active', $3, $4, 1,
             'canonical'
         ), (
             $5, 'ens', 'family-survivor.eth', ARRAY['family-survivor','eth'],
             decode('00', 'hex'), '0xfamily-survivor',
             ARRAY['0xfamily-survivor-label','0xeth'], $2, 'active', $3, $4, 1,
             'canonical'
         )",
    )
    .bind(FAMILY_BINDING_NAME)
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .bind(FAMILY_SURVIVOR_NAME)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 'declared_registry_path', 'ens_v1', to_timestamp(1),
                   $4, $5, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(FAMILY_BINDING_ID)?)
    .bind(FAMILY_BINDING_NAME)
    .bind(Uuid::parse_str(FAMILY_BINDING_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some(FAMILY_SURVIVOR_NAME),
        None,
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some(FAMILY_BINDING_NAME),
        Some(FAMILY_BINDING_RESOURCE),
        "RegistrationGranted",
        "ens_v1_registrar_l1",
        json!({"authority_kind":"registrar","registrant":OWNER,"status":"registered"}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some(FAMILY_BINDING_NAME),
        Some(FAMILY_BINDING_RESOURCE),
        "ResolverChanged",
        "basenames_base_registry",
        json!({"resolver":RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some(FAMILY_BINDING_NAME),
        Some(FAMILY_BINDING_RESOURCE),
        "RecordChanged",
        "basenames_base_resolver",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:family",
            "record_family":"text",
            "selector_key":"family",
            "value_retained":true,
            "value":"basenames-side"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;
    Ok(())
}

fn history_resolver_manifest_payload(rotation: i64) -> Value {
    json!({
        "rotation":rotation,
        "contracts":[
            {"role":"public_resolver","address":RESOLVER,"proxy_kind":"none"},
            {"role":"public_resolver","address":HISTORY_RESOLVER,"proxy_kind":"none"}
        ]
    })
}

async fn seed_permission_history_fixture(pool: &PgPool, mixed: bool) -> Result<i64> {
    seed_project_fixture(pool).await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions
         WHERE chain_id = $1 AND source_family = 'ens_v1_resolver_l1'",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    insert_manifest_update_event(
        pool,
        CHAIN,
        "ens_v1_resolver_l1",
        manifest_id,
        history_resolver_manifest_payload(0),
    )
    .await?;
    if mixed {
        insert_manifest(
            pool,
            CHAIN,
            "ens_v2_resolver_l1",
            "tests/project-permission-history-v2-resolver.toml",
            json!({"resolver_implementations":[]}),
        )
        .await?;
    }
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(HISTORY_REVOKED_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    if mixed {
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, $2, $3, 1, 'canonical')",
        )
        .bind(Uuid::parse_str(HISTORY_LIVE_RESOURCE)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
    }
    for (block, powers) in [(1, json!(["resolver_control"])), (2, json!([]))] {
        insert_event(
            pool,
            CHAIN,
            block,
            None,
            Some(HISTORY_REVOKED_RESOURCE),
            "PermissionChanged",
            "ens_v1_registrar_l1",
            json!({
                "subject":OWNER,
                "scope":{
                    "kind":"resolver",
                    "chain_id":CHAIN,
                    "resolver_address":HISTORY_RESOLVER
                },
                "effective_powers":powers,
                "grant_source":{"kind":"fixture"},
                "revocation_source":null,
                "inheritance_path":[],
                "transfer_behavior":"retain"
            }),
            json!({}),
        )
        .await?;
    }
    if mixed {
        insert_event(
            pool,
            CHAIN,
            1,
            None,
            Some(HISTORY_LIVE_RESOURCE),
            "PermissionChanged",
            "ens_v2_resolver_l1",
            json!({
                "subject":OWNER,
                "scope":{
                    "kind":"resolver",
                    "chain_id":CHAIN,
                    "resolver_address":HISTORY_RESOLVER
                },
                "effective_powers":["resolver_control"],
                "grant_source":{"kind":"fixture"},
                "revocation_source":null,
                "inheritance_path":[],
                "transfer_behavior":"retain"
            }),
            json!({}),
        )
        .await?;
    }
    Ok(manifest_id)
}

async fn rotate_history_resolver_manifest(pool: &PgPool, manifest_id: i64) -> Result<()> {
    insert_manifest_update_event(
        pool,
        CHAIN,
        "ens_v1_resolver_l1",
        manifest_id,
        history_resolver_manifest_payload(1),
    )
    .await
}

async fn seed_v2_permission_inverse_fixture(pool: &PgPool) -> Result<()> {
    seed_project_fixture(pool).await?;
    insert_manifest(
        pool,
        CHAIN,
        "ens_v2_resolver_l1",
        "tests/project-v2-permission-inverse.toml",
        json!({
            "resolver_implementations":[{
                "role":"permissioned_resolver",
                "address":V2_INVERSE_IMPLEMENTATION
            }]
        }),
    )
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(V2_INVERSE_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        None,
        Some(V2_INVERSE_RESOURCE),
        "PermissionChanged",
        "ens_v2_resolver_l1",
        json!({
            "subject":OWNER,
            "scope":{
                "kind":"resolver",
                "chain_id":CHAIN,
                "resolver_address":V2_INVERSE_RESOLVER
            },
            "effective_powers":["resolver_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"retain"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        None,
        None,
        "Upgraded",
        "ens_v2_resolver_l1",
        json!({
            "proxy_address":V2_INVERSE_RESOLVER,
            "implementation":V2_INVERSE_IMPLEMENTATION
        }),
        json!({"emitting_address":V2_INVERSE_RESOLVER}),
    )
    .await?;
    Ok(())
}

async fn seed_completed_project_extent(pool: &PgPool, chain: &str, head: i64) -> Result<()> {
    let hash = block_hash(chain, head);
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             current_block_number = $2,
             current_block_hash = $3,
             target_block_number = $2,
             target_block_hash = $3,
             live_handoff_block_number = CASE
                 WHEN phase_name = 'ingest' THEN $2
             END,
             live_handoff_block_hash = CASE
                 WHEN phase_name = 'ingest' THEN $3
             END,
             input_content_hash = CASE
                 WHEN phase_name IN ('interpret', 'project') THEN $4
             END,
             started_at = now(),
             finished_at = now()
         WHERE chain_id = $1
           AND phase_name IN ('ingest', 'interpret', 'project')",
    )
    .bind(chain)
    .bind(head)
    .bind(&hash)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'source', 'test', 'new_signature_range', 0,
                   $2, $3, $3, $4)",
    )
    .bind(chain)
    .bind(head.saturating_add(1))
    .bind(head)
    .bind(hash)
    .execute(pool)
    .await?;
    Ok(())
}

fn chain_config(chain: &str) -> phase_runner::error::RunnerResult<ChainConfig> {
    ChainConfig::new(
        chain,
        vec![SourceConfig::new(
            chain,
            "source",
            "test",
            SeedBasis::NewSignatureRange,
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

async fn insert_manifest(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
    file_path: &str,
    payload: Value,
) -> Result<i64> {
    let manifest_id = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version,
             file_path, manifest_payload
         ) VALUES (1, 'ens', $1, $2, 'fixture', 'active', $3, $4, $5)
         RETURNING manifest_id",
    )
    .bind(source_family)
    .bind(chain)
    .bind(NORMALIZER)
    .bind(file_path)
    .bind(&payload)
    .fetch_one(pool)
    .await?;
    insert_manifest_update_event(pool, chain, source_family, manifest_id, payload).await?;
    Ok(manifest_id)
}

#[allow(clippy::too_many_arguments)]
async fn insert_namespaced_manifest(
    pool: &PgPool,
    namespace: &str,
    chain: &str,
    source_family: &str,
    manifest_version: i64,
    deployment_label: &str,
    file_path: &str,
    payload: Value,
) -> Result<i64> {
    let manifest_id = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version,
             file_path, manifest_payload
         ) VALUES ($1, $2, $3, $4, $5, 'active', $6, $7, $8)
         RETURNING manifest_id",
    )
    .bind(manifest_version)
    .bind(namespace)
    .bind(source_family)
    .bind(chain)
    .bind(deployment_label)
    .bind(NORMALIZER)
    .bind(file_path)
    .bind(&payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, raw_fact_ref, derivation_kind,
             canonicality_state, before_state, after_state
         ) VALUES ($1, $2, 'SourceManifestUpdated', $3, $4, $5, $6, $7,
                   'manifest_sync', 'finalized', '{}'::jsonb, $8)",
    )
    .bind(format!("{chain}:SourceManifestUpdated:{}", Uuid::new_v4()))
    .bind(namespace)
    .bind(source_family)
    .bind(manifest_version)
    .bind(manifest_id)
    .bind(chain)
    .bind(json!({
        "manifest_id": manifest_id,
        "namespace": namespace,
        "source_family": source_family,
        "chain": chain,
        "deployment_epoch": deployment_label
    }))
    .bind(json!({
        "manifest_version": manifest_version,
        "normalizer_version": NORMALIZER,
        "rollout_status": "active",
        "manifest_payload": payload
    }))
    .execute(pool)
    .await?;
    Ok(manifest_id)
}

async fn insert_manifest_update_event(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
    manifest_id: i64,
    manifest_payload: Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, raw_fact_ref, derivation_kind,
             canonicality_state, before_state, after_state
         ) VALUES ($1, 'ens', 'SourceManifestUpdated', $2, 1, $3, $4, $5,
                   'manifest_sync', 'finalized', '{}'::jsonb, $6)",
    )
    .bind(format!("{chain}:SourceManifestUpdated:{}", Uuid::new_v4()))
    .bind(source_family)
    .bind(manifest_id)
    .bind(chain)
    .bind(json!({
        "manifest_id": manifest_id,
        "namespace": "ens",
        "source_family": source_family,
        "chain": chain,
        "deployment_epoch": "fixture"
    }))
    .bind(json!({
        "manifest_version": 1,
        "normalizer_version": NORMALIZER,
        "rollout_status": "active",
        "manifest_payload": manifest_payload
    }))
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_event(
    pool: &PgPool,
    chain: &str,
    ordinal: i64,
    logical_name_id: Option<&str>,
    resource_id: Option<&str>,
    event_kind: &str,
    source_family: &str,
    after_state: Value,
    raw_fact_ref: Value,
) -> Result<()> {
    insert_namespaced_event(
        pool,
        "ens",
        chain,
        ordinal,
        logical_name_id,
        resource_id,
        event_kind,
        source_family,
        1,
        after_state,
        raw_fact_ref,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_namespaced_event(
    pool: &PgPool,
    namespace: &str,
    chain: &str,
    ordinal: i64,
    logical_name_id: Option<&str>,
    resource_id: Option<&str>,
    event_kind: &str,
    source_family: &str,
    manifest_version: i64,
    after_state: Value,
    raw_fact_ref: Value,
) -> Result<()> {
    let blockless = event_kind == "SourceManifestUpdated";
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             raw_fact_ref, derivation_kind, canonicality_state, before_state, after_state
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                   $12, $13::canonicality_state, '{}'::jsonb, $14)",
    )
    .bind(format!("{chain}:{event_kind}:{ordinal}:{}", Uuid::new_v4()))
    .bind(namespace)
    .bind(logical_name_id)
    .bind(resource_id.map(Uuid::parse_str).transpose()?)
    .bind(event_kind)
    .bind(source_family)
    .bind(manifest_version)
    .bind(chain)
    .bind((!blockless).then_some(ordinal))
    .bind((!blockless).then(|| block_hash(chain, ordinal)))
    .bind(raw_fact_ref)
    .bind(if blockless {
        "manifest_sync"
    } else {
        "ens_v1_unwrapped_authority"
    })
    .bind(if blockless { "finalized" } else { "canonical" })
    .bind(after_state)
    .execute(pool)
    .await?;
    Ok(())
}

fn block_hash(chain: &str, number: i64) -> String {
    format!("{chain}-block-{number}")
}

fn fixture_contract_instance_id(
    chain: &str,
    source_family: &str,
    role: &str,
    address: &str,
) -> Uuid {
    let digest = keccak256(format!(
        "{chain}:{source_family}:{role}:{}",
        address.to_ascii_lowercase()
    ));
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}
