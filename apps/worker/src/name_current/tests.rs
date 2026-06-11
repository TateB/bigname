use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigname_storage::{
    ChainLineageBlock, NameSurface, NormalizedEvent, RawBlock, RawLog, Resource, SurfaceBinding,
    TokenLineage, default_database_url, label_preimage_from_label, load_name_current,
    upsert_chain_lineage_blocks, upsert_name_current_rows, upsert_name_surfaces,
    upsert_normalized_events, upsert_raw_blocks, upsert_raw_logs, upsert_resources,
    upsert_surface_bindings, upsert_token_lineages,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use super::*;

const SOURCE_FAMILY_ENS_V2_RESOLVER: &str = "ens_v2_resolver";
const EXACT_NAME_PROFILE_CAPABILITY: &str = "exact_name_profile";

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for worker name_current tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_worker_name_current_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for worker name_current tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect worker name_current test pool")?;

        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for worker name_current tests")?;

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn cleanup(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            self.database_name
        ))
        .execute(&self.admin_pool)
        .await
        .with_context(|| format!("failed to drop test database {}", self.database_name))?;
        self.admin_pool.close().await;
        Ok(())
    }
}

#[tokio::test]
async fn rebuilds_first_registration_into_name_current() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x1100, 0x2200, 0x3300);

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xname", 100, 1_717_171_700),
            raw_block("ethereum-mainnet", "0xgrant", 101, 1_717_171_701),
        ],
    )
    .await?;
    seed_identity(database.pool(), &binding, "0xgrant", 101, 1_717_171_701).await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "grant-1",
                "RegistrationGranted",
                "0xgrant",
                101,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:alice",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_800_000_000_i64,
                }),
            ),
            authority_event(
                &binding,
                "epoch-1",
                "AuthorityEpochChanged",
                "0xgrant",
                101,
                None,
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:alice",
                }),
            ),
            authority_event(
                &binding,
                "bound-1",
                "SurfaceBound",
                "0xgrant",
                101,
                None,
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:alice",
                    "active_from": 1_717_171_701_i64,
                    "binding_kind": "declared_registry_path",
                }),
            ),
        ],
    )
    .await?;

    let summary = rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;
    assert_eq!(summary.requested_name_count, 1);
    assert_eq!(summary.upserted_row_count, 1);

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt row must exist")?;
    assert_eq!(row.surface_binding_id, Some(binding.surface_binding_id));
    assert_eq!(row.resource_id, Some(binding.resource_id));
    assert_eq!(row.token_lineage_id, Some(binding.token_lineage_id));
    assert_eq!(
        row.binding_kind,
        Some(SurfaceBindingKind::DeclaredRegistryPath)
    );
    assert_eq!(
        row.declared_summary["registration"]["status"],
        Value::String("active".to_owned())
    );
    assert_eq!(
        row.declared_summary["registration"]["authority_kind"],
        Value::String("registrar".to_owned())
    );
    assert_eq!(
        row.declared_summary["registration"]["registrant"],
        Value::String("0x0000000000000000000000000000000000000aaa".to_owned())
    );
    assert_eq!(
        row.declared_summary["control"]["expiry"],
        Value::String(format_timestamp(timestamp(1_800_000_000)))
    );
    assert_eq!(
        row.declared_summary["resolver"],
        json!({
            "chain_id": Value::Null,
            "address": Value::Null,
            "latest_event_kind": Value::Null,
        })
    );
    assert_eq!(
        row.declared_summary["record_inventory"]["status"],
        Value::String("unsupported".to_owned())
    );
    assert!(
        row.declared_summary["resolver"]
            .as_object()
            .and_then(|value| value.get("status"))
            .is_none()
    );
    assert!(
        row.declared_summary["history"]
            .as_object()
            .and_then(|value| value.get("status"))
            .is_none()
    );
    assert!(row.declared_summary["history"]["surface_head"].is_object());
    assert!(row.declared_summary["history"]["resource_head"].is_object());
    assert_eq!(row.coverage["status"], Value::String("full".to_owned()));
    assert_eq!(row.coverage["unsupported_reason"], Value::Null);
    assert_eq!(row.manifest_version, 3);

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_ignores_suppressed_old_registry_raw_facts_after_migration() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new("ens:migrated.eth", "migrated.eth", 0x1110, 0x2220, 0x3330);
    let current_owner = "0x00000000000000000000000000000000000000aa";
    let suppressed_owner = "0x00000000000000000000000000000000000000bb";
    let current_resolver = "0x00000000000000000000000000000000000000cc";
    let suppressed_resolver = "0x00000000000000000000000000000000000000dd";

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block(
                "ethereum-mainnet",
                "0xensold-current-grant",
                100,
                1_776_300_100,
            ),
            raw_block(
                "ethereum-mainnet",
                "0xensold-current-owner",
                101,
                1_776_300_101,
            ),
            raw_block(
                "ethereum-mainnet",
                "0xensold-current-resolver",
                102,
                1_776_300_102,
            ),
            raw_block(
                "ethereum-mainnet",
                "0xensold-suppressed-old",
                500,
                1_776_300_500,
            ),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[old_registry_raw_log(
            "suppressed-name-current",
            "0xensold-suppressed-old",
            500,
            7,
            suppressed_owner,
            suppressed_resolver,
        )],
    )
    .await?;
    seed_identity(
        database.pool(),
        &binding,
        "0xensold-current-grant",
        100,
        1_776_300_100,
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            with_source_family(
                authority_event(
                    &binding,
                    "ensold-current-grant",
                    "RegistrationGranted",
                    "0xensold-current-grant",
                    100,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registry",
                        "authority_key": "registry:ethereum-mainnet:migrated.eth",
                        "registrant": current_owner,
                        "expiry": 1_900_000_000_i64,
                    }),
                ),
                "ens_v1_registry_l1",
            ),
            with_source_family(
                authority_event(
                    &binding,
                    "ensold-current-owner",
                    "AuthorityTransferred",
                    "0xensold-current-owner",
                    101,
                    Some(0),
                    json!({}),
                    json!({
                        "owner": current_owner,
                    }),
                ),
                "ens_v1_registry_l1",
            ),
            with_source_family(
                resolver_event(
                    &binding,
                    "ensold-current-resolver",
                    current_resolver,
                    "0xensold-current-resolver",
                    102,
                    0,
                ),
                "ens_v1_registry_l1",
            ),
        ],
    )
    .await?;

    let summary = rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;
    assert_eq!(summary.upserted_row_count, 1);

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt migrated ENSv1 row must exist")?;
    assert_eq!(
        row.declared_summary["registration"]["registrant"],
        json!(current_owner)
    );
    assert_eq!(
        row.declared_summary["control"]["registry_owner"],
        json!(current_owner)
    );
    assert_eq!(
        row.declared_summary["resolver"]["address"],
        json!(current_resolver)
    );
    assert_eq!(
        row.declared_summary["resolver"]["latest_event_kind"],
        json!(EVENT_KIND_RESOLVER_CHANGED)
    );
    assert_eq!(
        row.declared_summary["history"]["surface_head"]["event_kind"],
        json!(EVENT_KIND_RESOLVER_CHANGED)
    );
    assert_eq!(
        row.declared_summary["history"]["surface_head"]["chain_position"]["block_number"],
        json!(102)
    );
    assert_eq!(
        row.declared_summary["history"]["resource_head"]["event_kind"],
        json!(EVENT_KIND_RESOLVER_CHANGED)
    );
    assert_eq!(row.chain_positions["ethereum"]["block_number"], json!(102));
    assert_eq!(row.coverage["status"], json!("full"));
    assert_eq!(row.coverage["unsupported_reason"], Value::Null);
    assert_eq!(
        row.coverage["source_classes_considered"],
        json!(["ensv1_registry_path"])
    );
    assert_eq!(row.last_recomputed_at, timestamp(1_776_300_102));
    assert_eq!(
        row.provenance["normalized_event_ids"]
            .as_array()
            .map(Vec::len),
        Some(3)
    );

    let projection_json = serde_json::to_string(&json!({
        "declared_summary": row.declared_summary,
        "provenance": row.provenance,
        "coverage": row.coverage,
        "chain_positions": row.chain_positions,
        "canonicality_summary": row.canonicality_summary,
    }))?;
    assert!(!projection_json.contains("0xensold-suppressed-old"));
    assert!(!projection_json.contains(suppressed_owner));
    assert!(!projection_json.contains(suppressed_resolver));
    assert!(!projection_json.contains("suppressed-name-current"));

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_preserves_ens_v2_resource_identity_across_token_regeneration() -> Result<()> {
    let database = TestDatabase::new().await?;
    let (registry_manifest_id, registrar_manifest_id) =
        seed_ens_v2_exact_name_profile_manifests(database.pool()).await?;
    let binding =
        IdentityBinding::new("ens:bob.alice.eth", "bob.alice.eth", 0x9100, 0x9200, 0x9300);

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-sepolia", "0xensv2-surface", 700, 1_717_172_700),
            raw_block("ethereum-sepolia", "0xensv2-link", 701, 1_717_172_701),
            raw_block("ethereum-sepolia", "0xensv2-regen", 702, 1_717_172_702),
            raw_block("ethereum-sepolia", "0xensv2-renew", 703, 1_717_172_703),
        ],
    )
    .await?;
    upsert_token_lineages(
        database.pool(),
        &[TokenLineage {
            token_lineage_id: binding.token_lineage_id,
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xensv2-link".to_owned(),
            block_number: 701,
            provenance: json!({
                "adapter": ENS_V2_REGISTRY_DERIVATION_KIND,
                "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                "current_token_id": "0x0000000000000000000000000000000000000000000000000000000000000a02",
            }),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[Resource {
            resource_id: binding.resource_id,
            token_lineage_id: Some(binding.token_lineage_id),
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xensv2-link".to_owned(),
            block_number: 701,
            provenance: json!({
                "adapter": ENS_V2_REGISTRY_DERIVATION_KIND,
                "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                "current_token_id": "0x0000000000000000000000000000000000000000000000000000000000000a02",
            }),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[NameSurface {
            logical_name_id: binding.logical_name_id.clone(),
            namespace: "ens".to_owned(),
            input_name: binding.display_name.clone(),
            canonical_display_name: "Bob.alice.eth".to_owned(),
            normalized_name: binding.display_name.clone(),
            dns_encoded_name: binding.display_name.as_bytes().to_vec(),
            namehash: format!("namehash:{}", binding.display_name),
            labelhashes: labelhashes_for_name(&binding.display_name),
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xensv2-surface".to_owned(),
            block_number: 700,
            provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[SurfaceBinding {
            surface_binding_id: binding.surface_binding_id,
            logical_name_id: binding.logical_name_id.clone(),
            resource_id: binding.resource_id,
            binding_kind: SurfaceBindingKind::LinkedSubregistryPath,
            active_from: timestamp(1_717_172_701),
            active_to: None,
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xensv2-link".to_owned(),
            block_number: 701,
            provenance: json!({
                "adapter": ENS_V2_REGISTRY_DERIVATION_KIND,
                "binding_kind": "linked_subregistry_path",
            }),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            with_source_manifest_id(
                ens_v2_registry_event(
                    &binding,
                    "token-resource",
                    "TokenResourceLinked",
                    "0xensv2-link",
                    701,
                    0,
                    json!({}),
                    json!({
                        "token_id": "0x0000000000000000000000000000000000000000000000000000000000000a01",
                        "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                        "resource_id": binding.resource_id.to_string(),
                    }),
                ),
                registry_manifest_id,
            ),
            with_source_manifest_id(
                ens_v2_registry_event(
                    &binding,
                    "grant",
                    "RegistrationGranted",
                    "0xensv2-link",
                    701,
                    1,
                    json!({}),
                    json!({
                        "authority_kind": "ens_v2_registry",
                        "authority_key": "ens-v2-registry:ethereum-sepolia:user-registry:0xeac",
                        "registrant": "0x0000000000000000000000000000000000000b0b",
                        "expiry": 1_900_000_000_i64,
                    }),
                ),
                registry_manifest_id,
            ),
            with_source_manifest_id(
                ens_v2_registry_event(
                    &binding,
                    "regen",
                    "TokenRegenerated",
                    "0xensv2-regen",
                    702,
                    0,
                    json!({
                        "token_id": "0x0000000000000000000000000000000000000000000000000000000000000a01",
                    }),
                    json!({
                        "old_token_id": "0x0000000000000000000000000000000000000000000000000000000000000a01",
                        "new_token_id": "0x0000000000000000000000000000000000000000000000000000000000000a02",
                        "resource_id": binding.resource_id.to_string(),
                    }),
                ),
                registry_manifest_id,
            ),
            with_source_manifest_id(
                ens_v2_registrar_event(
                    &binding,
                    "renew",
                    "RegistrationRenewed",
                    "0xensv2-renew",
                    703,
                    0,
                    json!({}),
                    json!({
                        "duration": 31_536_000_i64,
                        "expiry": 1_931_536_000_i64,
                    }),
                ),
                registrar_manifest_id,
            ),
        ],
    )
    .await?;

    let summary = rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;
    assert_eq!(summary.upserted_row_count, 1);
    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt ENSv2 row must exist")?;
    assert_eq!(row.resource_id, Some(binding.resource_id));
    assert_eq!(row.token_lineage_id, Some(binding.token_lineage_id));
    assert_eq!(
        row.binding_kind,
        Some(SurfaceBindingKind::LinkedSubregistryPath)
    );
    assert_eq!(
        row.declared_summary["registration"]["authority_kind"],
        Value::String("ens_v2_registry".to_owned())
    );
    assert_eq!(
        row.declared_summary["registration"]["registrant"],
        Value::String("0x0000000000000000000000000000000000000b0b".to_owned())
    );
    assert!(
        row.provenance["normalized_event_ids"]
            .as_array()
            .is_some_and(|ids| ids.len() >= 4)
    );
    assert_eq!(row.coverage["status"], Value::String("full".to_owned()));
    assert_eq!(
        row.coverage["exhaustiveness"],
        Value::String("authoritative".to_owned())
    );
    assert_eq!(
        row.coverage["source_classes_considered"],
        json!(["ens_v2_registry_l1", "ens_v2_registrar_l1"])
    );
    assert_eq!(row.coverage["unsupported_reason"], Value::Null);
    assert_eq!(
        row.coverage["enumeration_basis"],
        Value::String("exact_name_profile".to_owned())
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_ignores_deprecated_ens_v2_registrar_shadow_events_after_supported_promotion()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
        1,
        MANIFEST_ROLLOUT_STATUS_ACTIVE,
    )
    .await?;
    let deprecated_registrar_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
        1,
        "deprecated",
    )
    .await?;
    insert_capability_flag(
        database.pool(),
        deprecated_registrar_manifest_id,
        EXACT_NAME_PROFILE_CAPABILITY,
        "shadow",
    )
    .await?;
    let supported_registrar_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
        2,
        MANIFEST_ROLLOUT_STATUS_ACTIVE,
    )
    .await?;
    insert_capability_flag(
        database.pool(),
        supported_registrar_manifest_id,
        EXACT_NAME_PROFILE_CAPABILITY,
        CAPABILITY_STATUS_SUPPORTED,
    )
    .await?;

    let binding = IdentityBinding::new(
        "ens:promotion.alice.eth",
        "promotion.alice.eth",
        0x9150,
        0x9250,
        0x9350,
    );

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block(
                ETHEREUM_SEPOLIA_CHAIN_ID,
                "0xensv2-promotion-surface",
                820,
                1_717_172_820,
            ),
            raw_block(
                ETHEREUM_SEPOLIA_CHAIN_ID,
                "0xensv2-promotion-link",
                821,
                1_717_172_821,
            ),
            raw_block(
                ETHEREUM_SEPOLIA_CHAIN_ID,
                "0xensv2-promotion-deprecated",
                822,
                1_717_172_822,
            ),
            raw_block(
                ETHEREUM_SEPOLIA_CHAIN_ID,
                "0xensv2-promotion-supported",
                823,
                1_717_172_823,
            ),
        ],
    )
    .await?;
    upsert_token_lineages(
        database.pool(),
        &[TokenLineage {
            token_lineage_id: binding.token_lineage_id,
            chain_id: ETHEREUM_SEPOLIA_CHAIN_ID.to_owned(),
            block_hash: "0xensv2-promotion-link".to_owned(),
            block_number: 821,
            provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[Resource {
            resource_id: binding.resource_id,
            token_lineage_id: Some(binding.token_lineage_id),
            chain_id: ETHEREUM_SEPOLIA_CHAIN_ID.to_owned(),
            block_hash: "0xensv2-promotion-link".to_owned(),
            block_number: 821,
            provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[NameSurface {
            logical_name_id: binding.logical_name_id.clone(),
            namespace: ENS_NAMESPACE.to_owned(),
            input_name: binding.display_name.clone(),
            canonical_display_name: binding.display_name.clone(),
            normalized_name: binding.display_name.clone(),
            dns_encoded_name: binding.display_name.as_bytes().to_vec(),
            namehash: format!("namehash:{}", binding.display_name),
            labelhashes: labelhashes_for_name(&binding.display_name),
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: ETHEREUM_SEPOLIA_CHAIN_ID.to_owned(),
            block_hash: "0xensv2-promotion-surface".to_owned(),
            block_number: 820,
            provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[SurfaceBinding {
            surface_binding_id: binding.surface_binding_id,
            logical_name_id: binding.logical_name_id.clone(),
            resource_id: binding.resource_id,
            binding_kind: SurfaceBindingKind::LinkedSubregistryPath,
            active_from: timestamp(1_717_172_821),
            active_to: None,
            chain_id: ETHEREUM_SEPOLIA_CHAIN_ID.to_owned(),
            block_hash: "0xensv2-promotion-link".to_owned(),
            block_number: 821,
            provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;

    let mut deprecated_registrar_event = ens_v2_registrar_event(
        &binding,
        "deprecated-renew",
        "RegistrationRenewed",
        "0xensv2-promotion-deprecated",
        822,
        0,
        json!({}),
        json!({
            "duration": 31_536_000_i64,
            "expiry": 1_920_000_000_i64,
        }),
    );
    deprecated_registrar_event.manifest_version = 1;
    seed_events(
        database.pool(),
        &[
            with_source_manifest_id(
                ens_v2_registry_event(
                    &binding,
                    "promotion-grant",
                    "RegistrationGranted",
                    "0xensv2-promotion-link",
                    821,
                    0,
                    json!({}),
                    json!({
                        "authority_kind": "ens_v2_registry",
                        "authority_key": "ens-v2-registry:ethereum-sepolia:user-registry:0xeac",
                        "registrant": "0x0000000000000000000000000000000000000b0b",
                        "expiry": 1_900_000_000_i64,
                    }),
                ),
                registry_manifest_id,
            ),
            with_source_manifest_id(deprecated_registrar_event, deprecated_registrar_manifest_id),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;
    let stale_only_row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt stale ENSv2 row must exist")?;
    assert_eq!(
        stale_only_row.coverage["status"],
        Value::String("unsupported".to_owned())
    );
    assert_eq!(
        stale_only_row.coverage["unsupported_reason"],
        Value::String("ensv2_exact_name_profile_shadow".to_owned())
    );

    seed_events(
        database.pool(),
        &[with_source_manifest_id(
            ens_v2_registrar_event(
                &binding,
                "supported-renew",
                "RegistrationRenewed",
                "0xensv2-promotion-supported",
                823,
                0,
                json!({}),
                json!({
                    "duration": 31_536_000_i64,
                    "expiry": 1_931_536_000_i64,
                }),
            ),
            supported_registrar_manifest_id,
        )],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;
    let promoted_row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt promoted ENSv2 row must exist")?;
    assert_eq!(
        promoted_row.coverage["status"],
        Value::String("full".to_owned())
    );
    assert_eq!(promoted_row.coverage["unsupported_reason"], Value::Null);
    assert_eq!(
        promoted_row.coverage["enumeration_basis"],
        Value::String("exact_name_profile".to_owned())
    );
    assert_eq!(
        promoted_row.chain_positions["ethereum-sepolia"]["chain_id"],
        Value::String(ETHEREUM_SEPOLIA_CHAIN_ID.to_owned())
    );
    assert!(promoted_row.chain_positions.get("ethereum").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_keeps_ens_v2_registry_only_exact_name_coverage_shadow() -> Result<()> {
    let database = TestDatabase::new().await?;
    let (registry_manifest_id, _) =
        seed_ens_v2_exact_name_profile_manifests(database.pool()).await?;
    let binding = IdentityBinding::new(
        "ens:registry-only.alice.eth",
        "registry-only.alice.eth",
        0x9140,
        0x9240,
        0x9340,
    );

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block(
                "ethereum-sepolia",
                "0xensv2-registry-only-surface",
                710,
                1_717_172_710,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xensv2-registry-only-link",
                711,
                1_717_172_711,
            ),
        ],
    )
    .await?;
    upsert_token_lineages(
        database.pool(),
        &[TokenLineage {
            token_lineage_id: binding.token_lineage_id,
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xensv2-registry-only-link".to_owned(),
            block_number: 711,
            provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[Resource {
            resource_id: binding.resource_id,
            token_lineage_id: Some(binding.token_lineage_id),
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xensv2-registry-only-link".to_owned(),
            block_number: 711,
            provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[NameSurface {
            logical_name_id: binding.logical_name_id.clone(),
            namespace: "ens".to_owned(),
            input_name: binding.display_name.clone(),
            canonical_display_name: binding.display_name.clone(),
            normalized_name: binding.display_name.clone(),
            dns_encoded_name: binding.display_name.as_bytes().to_vec(),
            namehash: format!("namehash:{}", binding.display_name),
            labelhashes: labelhashes_for_name(&binding.display_name),
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xensv2-registry-only-surface".to_owned(),
            block_number: 710,
            provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[SurfaceBinding {
            surface_binding_id: binding.surface_binding_id,
            logical_name_id: binding.logical_name_id.clone(),
            resource_id: binding.resource_id,
            binding_kind: SurfaceBindingKind::LinkedSubregistryPath,
            active_from: timestamp(1_717_172_711),
            active_to: None,
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xensv2-registry-only-link".to_owned(),
            block_number: 711,
            provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    seed_events(
        database.pool(),
        &[with_source_manifest_id(
            ens_v2_registry_event(
                &binding,
                "registry-only-grant",
                "RegistrationGranted",
                "0xensv2-registry-only-link",
                711,
                0,
                json!({}),
                json!({
                    "authority_kind": "ens_v2_registry",
                    "authority_key": "ens-v2-registry:ethereum-sepolia:user-registry:0xeac",
                    "registrant": "0x0000000000000000000000000000000000000b0b",
                    "expiry": 1_900_000_000_i64,
                }),
            ),
            registry_manifest_id,
        )],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt registry-only ENSv2 row must exist")?;
    assert_eq!(
        row.coverage["status"],
        Value::String("unsupported".to_owned())
    );
    assert_eq!(
        row.coverage["exhaustiveness"],
        Value::String("not_applicable".to_owned())
    );
    assert_eq!(
        row.coverage["source_classes_considered"],
        json!(["ensv2_registry_resource_surface"])
    );
    assert_eq!(
        row.coverage["unsupported_reason"],
        Value::String("ensv2_exact_name_profile_shadow".to_owned())
    );

    database.cleanup().await
}

#[test]
fn exact_name_coverage_rejects_mixed_ensv1_ensv2_corpus() {
    let coverage = build_exact_name_coverage(
        ENS_NAMESPACE,
        &[
            coverage_event("ens_v1_registrar_l1", "ethereum-mainnet"),
            coverage_event(SOURCE_FAMILY_ENS_V2_REGISTRY_L1, "ethereum-sepolia"),
            coverage_event(SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, "ethereum-sepolia"),
        ],
    );

    assert_eq!(coverage["status"], Value::String("unsupported".to_owned()));
    assert_eq!(
        coverage["unsupported_reason"],
        Value::String("mixed_ensv1_ensv2_exact_name_corpus".to_owned())
    );
    assert_eq!(
        coverage["source_classes_considered"],
        json!(["ens_v2_registry_l1", "ens_v2_registrar_l1"])
    );
    assert_eq!(
        coverage["enumeration_basis"],
        Value::String("exact_name_profile".to_owned())
    );
}

#[test]
fn exact_name_coverage_rejects_ensv2_shadow_manifest_capability() {
    let coverage = build_exact_name_coverage(
        ENS_NAMESPACE,
        &[
            selected_ens_v2_coverage_event(SOURCE_FAMILY_ENS_V2_REGISTRY_L1, 1, 100, None),
            selected_ens_v2_coverage_event(
                SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
                2,
                101,
                Some("shadow"),
            ),
        ],
    );

    assert_eq!(coverage["status"], Value::String("unsupported".to_owned()));
    assert_eq!(
        coverage["unsupported_reason"],
        Value::String("ensv2_exact_name_profile_shadow".to_owned())
    );
}

#[test]
fn exact_name_coverage_rejects_ensv2_manifest_version_drift() {
    let mut drifted_registrar = selected_ens_v2_coverage_event(
        SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
        2,
        101,
        Some(CAPABILITY_STATUS_SUPPORTED),
    );
    drifted_registrar.source_manifest_version = Some(99);

    let coverage = build_exact_name_coverage(
        ENS_NAMESPACE,
        &[
            selected_ens_v2_coverage_event(SOURCE_FAMILY_ENS_V2_REGISTRY_L1, 1, 100, None),
            drifted_registrar,
        ],
    );

    assert_eq!(coverage["status"], Value::String("unsupported".to_owned()));
    assert_eq!(
        coverage["unsupported_reason"],
        Value::String("ensv2_exact_name_profile_shadow".to_owned())
    );
}

#[test]
fn exact_name_coverage_rejects_ensv2_missing_manifest_linkage() {
    let mut unlinked_registrar =
        coverage_event(SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, ETHEREUM_SEPOLIA_CHAIN_ID);
    unlinked_registrar.exact_name_profile_status = Some(CAPABILITY_STATUS_SUPPORTED.to_owned());

    let coverage = build_exact_name_coverage(
        ENS_NAMESPACE,
        &[
            selected_ens_v2_coverage_event(SOURCE_FAMILY_ENS_V2_REGISTRY_L1, 1, 100, None),
            unlinked_registrar,
        ],
    );

    assert_eq!(coverage["status"], Value::String("unsupported".to_owned()));
    assert_eq!(
        coverage["unsupported_reason"],
        Value::String("ensv2_exact_name_profile_shadow".to_owned())
    );
}

#[tokio::test]
async fn rebuild_projects_current_resolver_summary() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new("ens:resolver.eth", "resolver.eth", 0x3100, 0x3200, 0x3300);
    let resolver_address = "0x0000000000000000000000000000000000000abc";

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xgrant", 210, 1_717_171_710),
            raw_block("ethereum-mainnet", "0xresolver", 211, 1_717_171_711),
        ],
    )
    .await?;
    seed_identity(database.pool(), &binding, "0xgrant", 210, 1_717_171_710).await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "grant-resolver",
                "RegistrationGranted",
                "0xgrant",
                210,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:resolver",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_800_000_000_i64,
                }),
            ),
            resolver_event(
                &binding,
                "resolver-change",
                resolver_address,
                "0xresolver",
                211,
                0,
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt row must exist")?;
    assert_eq!(
        row.declared_summary["resolver"],
        json!({
            "chain_id": "ethereum-mainnet",
            "address": resolver_address,
            "latest_event_kind": EVENT_KIND_RESOLVER_CHANGED,
        })
    );
    assert_eq!(row.coverage["unsupported_reason"], Value::Null);

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_prefers_explicit_registry_resolver_over_authority_epoch_boundary() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new(
        "ens:wrapped-resolver.eth",
        "wrapped-resolver.eth",
        0x3110,
        0x3210,
        0x3310,
    );
    let stale_boundary_resolver = "0x0000000000000000000000000000000000000abc";
    let registry_resolver = "0x0000000000000000000000000000000000000def";

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xgrant", 212, 1_717_171_712),
            raw_block("ethereum-mainnet", "0xresolver", 213, 1_717_171_713),
        ],
    )
    .await?;
    seed_identity(database.pool(), &binding, "0xgrant", 212, 1_717_171_712).await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "grant-wrapped-resolver",
                "RegistrationGranted",
                "0xgrant",
                212,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "wrapper",
                    "authority_key": "wrapper:ethereum-mainnet:16:wrapped-resolver",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_800_000_000_i64,
                }),
            ),
            with_source_family(
                resolver_event(
                    &binding,
                    "explicit-registry-resolver",
                    registry_resolver,
                    "0xresolver",
                    213,
                    188,
                ),
                "ens_v1_registry_l1",
            ),
            with_source_family(
                authority_event(
                    &binding,
                    "authority-boundary-resolver",
                    EVENT_KIND_RESOLVER_CHANGED,
                    "0xresolver",
                    213,
                    None,
                    json!({
                        "resolver": Value::Null,
                    }),
                    json!({
                        "namehash": format!("namehash:{}", binding.display_name),
                        "resolver": stale_boundary_resolver,
                        "source_event": "AuthorityEpochChanged",
                    }),
                ),
                "ens_v1_wrapper_l1",
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt row must exist")?;
    assert_eq!(
        row.declared_summary["resolver"],
        json!({
            "chain_id": "ethereum-mainnet",
            "address": registry_resolver,
            "latest_event_kind": EVENT_KIND_RESOLVER_CHANGED,
        })
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_accepts_later_authority_epoch_resolver_boundary() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new(
        "ens:later-boundary.eth",
        "later-boundary.eth",
        0x3120,
        0x3220,
        0x3320,
    );
    let registry_resolver = "0x0000000000000000000000000000000000000def";
    let later_boundary_resolver = "0x0000000000000000000000000000000000001234";

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xgrant", 212, 1_717_171_712),
            raw_block("ethereum-mainnet", "0xresolver", 213, 1_717_171_713),
            raw_block("ethereum-mainnet", "0xboundary", 214, 1_717_171_714),
        ],
    )
    .await?;
    seed_identity(database.pool(), &binding, "0xgrant", 212, 1_717_171_712).await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "grant-later-boundary",
                "RegistrationGranted",
                "0xgrant",
                212,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "wrapper",
                    "authority_key": "wrapper:ethereum-mainnet:16:later-boundary",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_800_000_000_i64,
                }),
            ),
            with_source_family(
                resolver_event(
                    &binding,
                    "explicit-registry-resolver",
                    registry_resolver,
                    "0xresolver",
                    213,
                    188,
                ),
                "ens_v1_registry_l1",
            ),
            with_source_family(
                authority_event(
                    &binding,
                    "later-authority-boundary-resolver",
                    EVENT_KIND_RESOLVER_CHANGED,
                    "0xboundary",
                    214,
                    None,
                    json!({
                        "resolver": registry_resolver,
                    }),
                    json!({
                        "namehash": format!("namehash:{}", binding.display_name),
                        "resolver": later_boundary_resolver,
                        "source_event": "AuthorityEpochChanged",
                    }),
                ),
                "ens_v1_wrapper_l1",
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt row must exist")?;
    assert_eq!(
        row.declared_summary["resolver"],
        json!({
            "chain_id": "ethereum-mainnet",
            "address": later_boundary_resolver,
            "latest_event_kind": EVENT_KIND_RESOLVER_CHANGED,
        })
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_projects_null_resolver_summary_for_zero_address() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new(
        "ens:no-resolver.eth",
        "no-resolver.eth",
        0x3400,
        0x3500,
        0x3600,
    );

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xgrant", 220, 1_717_171_720),
            raw_block("ethereum-mainnet", "0xresolver", 221, 1_717_171_721),
        ],
    )
    .await?;
    seed_identity(database.pool(), &binding, "0xgrant", 220, 1_717_171_720).await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "grant-null-resolver",
                "RegistrationGranted",
                "0xgrant",
                220,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:no-resolver",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_800_000_000_i64,
                }),
            ),
            resolver_event(
                &binding,
                "resolver-cleared",
                ZERO_ADDRESS,
                "0xresolver",
                221,
                0,
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt row must exist")?;
    assert_eq!(
        row.declared_summary["resolver"],
        json!({
            "chain_id": Value::Null,
            "address": Value::Null,
            "latest_event_kind": EVENT_KIND_RESOLVER_CHANGED,
        })
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_projects_supported_alias_only_topology_from_alias_changed() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x3410, 0x3510, 0x3610);

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xgrant", 230, 1_717_171_730),
            raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xresolver", 231, 1_717_171_731),
            raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xalias", 232, 1_717_171_732),
            raw_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xbinding-alias",
                233,
                1_717_171_733,
            ),
        ],
    )
    .await?;
    upsert_token_lineages(
        database.pool(),
        &[token_lineage(binding.token_lineage_id, "0xgrant", 230)],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[resource(
            binding.resource_id,
            binding.token_lineage_id,
            "0xgrant",
            230,
        )],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            &binding.logical_name_id,
            &binding.display_name,
            "0xgrant",
            230,
        )],
    )
    .await?;
    let alias_binding_id = Uuid::from_u128(0x3611);
    let mut alias_binding = surface_binding(
        &IdentityBinding {
            surface_binding_id: alias_binding_id,
            ..binding.clone()
        },
        1_717_171_733,
        None,
        "0xbinding-alias",
        233,
    );
    alias_binding.binding_kind = SurfaceBindingKind::ResolverAliasPath;
    upsert_surface_bindings(database.pool(), &[alias_binding]).await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "grant-alias",
                "RegistrationGranted",
                "0xgrant",
                230,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:alice",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_800_000_000_i64,
                }),
            ),
            resolver_event(
                &binding,
                "alias-resolver",
                "0x0000000000000000000000000000000000000abc",
                "0xresolver",
                231,
                0,
            ),
            ens_v2_alias_event(
                &binding,
                "alias-project",
                "0xalias",
                232,
                0,
                json!({
                    "active": true,
                    "alias_state": "active",
                    "to_name": "profile.alice.eth",
                    "to_logical_name_id": "ens:profile.alice.eth",
                    "to_normalized_name": "profile.alice.eth",
                    "to_canonical_display_name": "Profile.alice.eth",
                    "to_namehash": "namehash:profile.alice.eth",
                    "to_resource_id": binding.resource_id.to_string(),
                }),
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt alias-only row must exist")?;
    let topology = row
        .declared_summary
        .get("topology")
        .context("supported alias-only row must project topology")?;
    assert_eq!(
        row.binding_kind,
        Some(SurfaceBindingKind::ResolverAliasPath)
    );
    assert_eq!(
        topology["alias"]["final_target"]["logical_name_id"],
        json!("ens:profile.alice.eth")
    );
    assert_eq!(
        topology["resolver_path"][0]["logical_name_id"],
        json!(binding.logical_name_id.clone())
    );
    assert_eq!(topology["wildcard"], empty_wildcard_detail());
    assert_eq!(topology["transport"], empty_transport_detail());
    assert_eq!(
        topology["version_boundaries"]["record_version_boundary"]["chain_position"]["block_hash"],
        json!("0xbinding-alias")
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_projects_supported_wildcard_topology_from_real_ancestor_inputs() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x3420, 0x3520, 0x3620);
    let wildcard_source = IdentityBinding::new("ens:eth", "eth", 0x4420, 0x4520, 0x4620);

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xsource", 239, 1_717_171_739),
            raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xgrant", 240, 1_717_171_740),
            raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xresolver", 241, 1_717_171_741),
            raw_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xbinding-wildcard",
                242,
                1_717_171_742,
            ),
        ],
    )
    .await?;
    seed_identity(
        database.pool(),
        &wildcard_source,
        "0xsource",
        239,
        1_717_171_739,
    )
    .await?;
    upsert_token_lineages(
        database.pool(),
        &[token_lineage(binding.token_lineage_id, "0xgrant", 240)],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[resource(
            binding.resource_id,
            binding.token_lineage_id,
            "0xgrant",
            240,
        )],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            &binding.logical_name_id,
            &binding.display_name,
            "0xgrant",
            240,
        )],
    )
    .await?;
    let wildcard_binding_id = Uuid::from_u128(0x3621);
    let mut wildcard_binding = surface_binding(
        &IdentityBinding {
            surface_binding_id: wildcard_binding_id,
            ..binding.clone()
        },
        1_717_171_742,
        None,
        "0xbinding-wildcard",
        242,
    );
    wildcard_binding.binding_kind = SurfaceBindingKind::ObservedWildcardPath;
    upsert_surface_bindings(database.pool(), &[wildcard_binding]).await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "grant-wildcard",
                "RegistrationGranted",
                "0xgrant",
                240,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:alice",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_800_000_000_i64,
                }),
            ),
            resolver_event(
                &wildcard_source,
                "wildcard-resolver",
                "0x0000000000000000000000000000000000000def",
                "0xresolver",
                241,
                0,
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt wildcard-derived row must exist")?;
    let topology = row
        .declared_summary
        .get("topology")
        .context("supported wildcard-derived row must project topology")?;
    assert_eq!(
        row.binding_kind,
        Some(SurfaceBindingKind::ObservedWildcardPath)
    );
    assert_eq!(
        topology["resolver_path"][0]["logical_name_id"],
        json!("ens:eth")
    );
    assert_eq!(
        topology["resolver_path"][0]["resource_id"],
        json!(wildcard_source.resource_id.to_string())
    );
    assert_eq!(
        topology["resolver_path"][0]["address"],
        json!("0x0000000000000000000000000000000000000def")
    );
    assert_eq!(
        topology["wildcard"]["source"]["logical_name_id"],
        json!("ens:eth")
    );
    assert_eq!(
        topology["wildcard"]["source"]["resource_id"],
        json!(wildcard_source.resource_id.to_string())
    );
    assert_eq!(topology["wildcard"]["matched_labels"], json!(["alice"]));
    assert_eq!(topology["alias"], empty_alias_detail());
    assert_eq!(topology["transport"], empty_transport_detail());
    assert_eq!(
        topology["version_boundaries"]["record_version_boundary"]["logical_name_id"],
        json!("ens:eth")
    );
    assert_eq!(
        topology["version_boundaries"]["record_version_boundary"]["resource_id"],
        json!(wildcard_source.resource_id.to_string())
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_projects_basenames_base_authority_into_name_current() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new(
        "basenames:alice.base.eth",
        "alice.base.eth",
        0x4401,
        0x4402,
        0x4403,
    );

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("base-mainnet", "0xbase-surface", 500, 1_717_172_500),
            raw_block("base-mainnet", "0xbase-grant", 501, 1_717_172_501),
            raw_block("base-mainnet", "0xbase-transfer", 502, 1_717_172_502),
            raw_block("base-mainnet", "0xbase-resolver", 503, 1_717_172_503),
        ],
    )
    .await?;
    seed_basenames_identity(
        database.pool(),
        &binding,
        "0xbase-grant",
        501,
        1_717_172_501,
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            basenames_authority_event(
                &binding,
                "base-grant",
                "RegistrationGranted",
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                "0xbase-grant",
                501,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:base-mainnet:alice",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_900_000_000_i64,
                }),
            ),
            basenames_authority_event(
                &binding,
                "base-transfer",
                "AuthorityTransferred",
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
                "0xbase-transfer",
                502,
                Some(0),
                json!({}),
                json!({
                    "owner": "0x0000000000000000000000000000000000000bbb",
                }),
            ),
            basenames_resolver_event(
                &binding,
                "base-resolver",
                "0x0000000000000000000000000000000000000abc",
                "0xbase-resolver",
                503,
                0,
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt basenames row must exist")?;
    assert_eq!(row.namespace, BASENAMES_NAMESPACE);
    assert_eq!(row.surface_binding_id, Some(binding.surface_binding_id));
    assert_eq!(row.resource_id, Some(binding.resource_id));
    assert_eq!(row.token_lineage_id, Some(binding.token_lineage_id));
    assert_eq!(
        row.declared_summary["registration"]["status"],
        Value::String("active".to_owned())
    );
    assert_eq!(
        row.declared_summary["registration"]["authority_key"],
        Value::String("registrar:base-mainnet:alice".to_owned())
    );
    assert_eq!(
        row.declared_summary["control"]["registry_owner"],
        Value::String("0x0000000000000000000000000000000000000bbb".to_owned())
    );
    assert_eq!(
        row.declared_summary["resolver"],
        json!({
            "chain_id": "base-mainnet",
            "address": "0x0000000000000000000000000000000000000abc",
            "latest_event_kind": EVENT_KIND_RESOLVER_CHANGED,
        })
    );
    assert_eq!(
        row.coverage["source_classes_considered"],
        json!(["ensv1_registry_path"])
    );
    assert_eq!(
        row.chain_positions["base"]["chain_id"],
        Value::String("base-mainnet".to_owned())
    );
    assert!(row.declared_summary.get("topology").is_none());
    assert_eq!(row.coverage["unsupported_reason"], Value::Null);

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_projects_supported_basenames_transport_topology_from_frozen_inputs() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new(
        "basenames:alice.base.eth",
        "alice.base.eth",
        0x4411,
        0x4412,
        0x4413,
    );

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-surface", 500, 1_717_172_510),
            raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-binding", 511, 1_717_172_511),
            raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-resolver", 512, 1_717_172_512),
            raw_block(
                BASE_MAINNET_CHAIN_ID,
                "0xbase-record-version",
                513,
                1_717_172_513,
            ),
            raw_block(
                BASE_MAINNET_CHAIN_ID,
                "0xbase-supported-binding",
                514,
                1_717_172_514,
            ),
            raw_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xbasenamesl1-future",
                21_000_100,
                1_776_387_700,
            ),
        ],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        ETHEREUM_MAINNET_CHAIN_ID,
        "0xbasenamesl1-future",
        21_000_100,
    )
    .await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[chain_lineage_block(
            ETHEREUM_MAINNET_CHAIN_ID,
            "0xbasenamesl1-compatible",
            21_000_099,
            1_717_172_513,
        )],
    )
    .await?;
    upsert_token_lineages(
        database.pool(),
        &[TokenLineage {
            token_lineage_id: binding.token_lineage_id,
            chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
            block_hash: "0xbase-binding".to_owned(),
            block_number: 511,
            provenance: json!({"source": "worker_name_current_test", "kind": "token_lineage"}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[Resource {
            resource_id: binding.resource_id,
            token_lineage_id: Some(binding.token_lineage_id),
            chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
            block_hash: "0xbase-binding".to_owned(),
            block_number: 511,
            provenance: json!({"source": "worker_name_current_test", "kind": "resource"}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[NameSurface {
            logical_name_id: binding.logical_name_id.clone(),
            namespace: BASENAMES_NAMESPACE.to_owned(),
            input_name: binding.display_name.clone(),
            canonical_display_name: "Alice.base.eth".to_owned(),
            normalized_name: binding.display_name.clone(),
            dns_encoded_name: binding.display_name.as_bytes().to_vec(),
            namehash: format!("namehash:{}", binding.display_name),
            labelhashes: labelhashes_for_name(&binding.display_name),
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
            block_hash: "0xbase-surface".to_owned(),
            block_number: 500,
            provenance: json!({"source": "worker_name_current_test", "kind": "name_surface"}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    let basenames_binding = SurfaceBinding {
        surface_binding_id: Uuid::from_u128(0x4414),
        logical_name_id: binding.logical_name_id.clone(),
        resource_id: binding.resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: timestamp(1_717_172_514),
        active_to: None,
        chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
        block_hash: "0xbase-supported-binding".to_owned(),
        block_number: 514,
        provenance: json!({"source": "worker_name_current_test", "kind": "surface_binding"}),
        canonicality_state: CanonicalityState::Finalized,
    };
    upsert_surface_bindings(database.pool(), std::slice::from_ref(&basenames_binding)).await?;
    insert_basenames_execution_manifest_version(database.pool(), 2, MANIFEST_ROLLOUT_STATUS_ACTIVE)
        .await?;
    seed_events(
        database.pool(),
        &[
            basenames_authority_event(
                &binding,
                "supported-base-grant",
                "RegistrationGranted",
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                "0xbase-binding",
                511,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:base-mainnet:alice",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_900_000_000_i64,
                }),
            ),
            basenames_resolver_event(
                &binding,
                "supported-base-resolver",
                "0x0000000000000000000000000000000000000abc",
                "0xbase-resolver",
                512,
                0,
            ),
            basenames_record_version_event(
                &binding,
                "supported-base-record-version",
                "0xbase-record-version",
                513,
                0,
                7,
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt supported basenames row must exist")?;
    let topology = row
        .declared_summary
        .get("topology")
        .context("supported basenames row must project topology")?;
    assert_eq!(
        topology["transport"],
        json!({
            "source_chain_id": BASE_MAINNET_CHAIN_ID,
            "target_chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "contract_address": BASENAMES_L1_RESOLVER_ADDRESS,
            "latest_event_kind": Value::Null,
        })
    );
    assert_eq!(
        row.chain_positions["base"]["chain_id"],
        json!(BASE_MAINNET_CHAIN_ID)
    );
    assert_eq!(
        row.chain_positions["ethereum"]["block_hash"],
        json!("0xbasenamesl1-compatible")
    );
    assert_eq!(
        row.chain_positions["ethereum"]["block_number"],
        json!(21_000_099)
    );
    assert_eq!(
        row.chain_positions["base"]["block_hash"],
        json!("0xbase-supported-binding")
    );
    assert_eq!(
        topology["version_boundaries"]["record_version_boundary"]["event_kind"],
        json!(EVENT_KIND_RECORD_VERSION_CHANGED)
    );
    assert_eq!(
        topology["version_boundaries"]["record_version_boundary"]["chain_position"]["block_hash"],
        json!("0xbase-record-version")
    );
    assert!(
        row.provenance["manifest_versions"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| {
                item.get("source_family").and_then(Value::as_str)
                    == Some(SOURCE_FAMILY_BASENAMES_EXECUTION)
                    && item.get("manifest_version").and_then(Value::as_i64) == Some(2)
            }))
    );
    assert_eq!(
        row.canonicality_summary["chains"][BASE_MAINNET_CHAIN_ID],
        json!("finalized")
    );
    assert_eq!(
        row.canonicality_summary["chains"][ETHEREUM_MAINNET_CHAIN_ID],
        json!("finalized")
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_projects_basenames_base_authority_control_vectors_into_name_current() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let nft_only = IdentityBinding::new(
        "basenames:nft-only.base.eth",
        "nft-only.base.eth",
        0x4411,
        0x4412,
        0x4413,
    );
    let management_only = IdentityBinding::new(
        "basenames:management-only.base.eth",
        "management-only.base.eth",
        0x4421,
        0x4422,
        0x4423,
    );
    let full_transfer = IdentityBinding::new(
        "basenames:full-transfer.base.eth",
        "full-transfer.base.eth",
        0x4431,
        0x4432,
        0x4433,
    );

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("base-mainnet", "0xbase-surface", 500, 1_717_182_500),
            raw_block("base-mainnet", "0xnft-grant", 501, 1_717_182_501),
            raw_block("base-mainnet", "0xnft-manager", 502, 1_717_182_502),
            raw_block("base-mainnet", "0xnft-transfer", 503, 1_717_182_503),
            raw_block("base-mainnet", "0xmgmt-grant", 511, 1_717_182_511),
            raw_block("base-mainnet", "0xmgmt-manager", 512, 1_717_182_512),
            raw_block("base-mainnet", "0xfull-grant", 521, 1_717_182_521),
            raw_block("base-mainnet", "0xfull-manager", 522, 1_717_182_522),
            raw_block("base-mainnet", "0xfull-transfer", 523, 1_717_182_523),
            raw_block("base-mainnet", "0xfull-manager-final", 524, 1_717_182_524),
        ],
    )
    .await?;
    seed_basenames_identity(
        database.pool(),
        &nft_only,
        "0xnft-grant",
        501,
        1_717_182_501,
    )
    .await?;
    seed_basenames_identity(
        database.pool(),
        &management_only,
        "0xmgmt-grant",
        511,
        1_717_182_511,
    )
    .await?;
    seed_basenames_identity(
        database.pool(),
        &full_transfer,
        "0xfull-grant",
        521,
        1_717_182_521,
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            basenames_authority_event(
                &nft_only,
                "nft-grant",
                "RegistrationGranted",
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                "0xnft-grant",
                501,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:base-mainnet:nft-only",
                    "registrant": "0x00000000000000000000000000000000000000a1",
                }),
            ),
            basenames_authority_event(
                &nft_only,
                "nft-manager",
                "AuthorityTransferred",
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
                "0xnft-manager",
                502,
                Some(0),
                json!({
                    "owner": "0x00000000000000000000000000000000000000a1",
                }),
                json!({
                    "owner": "0x00000000000000000000000000000000000000b1",
                }),
            ),
            basenames_authority_event(
                &nft_only,
                "nft-transfer",
                "TokenControlTransferred",
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                "0xnft-transfer",
                503,
                Some(0),
                json!({
                    "from": "0x00000000000000000000000000000000000000a1",
                }),
                json!({
                    "to": "0x00000000000000000000000000000000000000c1",
                }),
            ),
            basenames_authority_event(
                &management_only,
                "mgmt-grant",
                "RegistrationGranted",
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                "0xmgmt-grant",
                511,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:base-mainnet:management-only",
                    "registrant": "0x00000000000000000000000000000000000000a2",
                }),
            ),
            basenames_authority_event(
                &management_only,
                "mgmt-manager",
                "AuthorityTransferred",
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
                "0xmgmt-manager",
                512,
                Some(0),
                json!({
                    "owner": "0x00000000000000000000000000000000000000a2",
                }),
                json!({
                    "owner": "0x00000000000000000000000000000000000000b2",
                }),
            ),
            basenames_authority_event(
                &full_transfer,
                "full-grant",
                "RegistrationGranted",
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                "0xfull-grant",
                521,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:base-mainnet:full-transfer",
                    "registrant": "0x00000000000000000000000000000000000000a3",
                }),
            ),
            basenames_authority_event(
                &full_transfer,
                "full-manager",
                "AuthorityTransferred",
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
                "0xfull-manager",
                522,
                Some(0),
                json!({
                    "owner": "0x00000000000000000000000000000000000000a3",
                }),
                json!({
                    "owner": "0x00000000000000000000000000000000000000b3",
                }),
            ),
            basenames_authority_event(
                &full_transfer,
                "full-transfer",
                "TokenControlTransferred",
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                "0xfull-transfer",
                523,
                Some(0),
                json!({
                    "from": "0x00000000000000000000000000000000000000a3",
                }),
                json!({
                    "to": "0x00000000000000000000000000000000000000c3",
                }),
            ),
            basenames_authority_event(
                &full_transfer,
                "full-manager-final",
                "AuthorityTransferred",
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
                "0xfull-manager-final",
                524,
                Some(0),
                json!({
                    "owner": "0x00000000000000000000000000000000000000b3",
                }),
                json!({
                    "owner": "0x00000000000000000000000000000000000000c3",
                }),
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), None).await?;

    let nft_only_row = load_name_current(database.pool(), &nft_only.logical_name_id)
        .await?
        .context("nft-only basenames row must exist")?;
    assert_eq!(nft_only_row.namespace, BASENAMES_NAMESPACE);
    assert_eq!(
        nft_only_row.declared_summary["control"]["registrant"],
        Value::String("0x00000000000000000000000000000000000000c1".to_owned())
    );
    assert_eq!(
        nft_only_row.declared_summary["control"]["registry_owner"],
        Value::String("0x00000000000000000000000000000000000000b1".to_owned())
    );
    assert_eq!(
        nft_only_row.declared_summary["control"]["latest_event_kind"],
        Value::String("TokenControlTransferred".to_owned())
    );

    let management_only_row = load_name_current(database.pool(), &management_only.logical_name_id)
        .await?
        .context("management-only basenames row must exist")?;
    assert_eq!(management_only_row.namespace, BASENAMES_NAMESPACE);
    assert_eq!(
        management_only_row.declared_summary["control"]["registrant"],
        Value::String("0x00000000000000000000000000000000000000a2".to_owned())
    );
    assert_eq!(
        management_only_row.declared_summary["control"]["registry_owner"],
        Value::String("0x00000000000000000000000000000000000000b2".to_owned())
    );
    assert_eq!(
        management_only_row.declared_summary["control"]["latest_event_kind"],
        Value::String("AuthorityTransferred".to_owned())
    );

    let full_transfer_row = load_name_current(database.pool(), &full_transfer.logical_name_id)
        .await?
        .context("full-transfer basenames row must exist")?;
    assert_eq!(full_transfer_row.namespace, BASENAMES_NAMESPACE);
    assert_eq!(
        full_transfer_row.declared_summary["control"]["registrant"],
        Value::String("0x00000000000000000000000000000000000000c3".to_owned())
    );
    assert_eq!(
        full_transfer_row.declared_summary["control"]["registry_owner"],
        Value::String("0x00000000000000000000000000000000000000c3".to_owned())
    );
    assert_eq!(
        full_transfer_row.declared_summary["control"]["latest_event_kind"],
        Value::String("AuthorityTransferred".to_owned())
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_keeps_same_binding_for_renewal_and_transfer() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x4100, 0x4200, 0x4300);

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xgrant", 201, 1_717_171_801),
            raw_block("ethereum-mainnet", "0xrenew", 202, 1_717_171_802),
            raw_block("ethereum-mainnet", "0xtransfer", 203, 1_717_171_803),
        ],
    )
    .await?;
    seed_identity(database.pool(), &binding, "0xgrant", 201, 1_717_171_801).await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "grant-2",
                "RegistrationGranted",
                "0xgrant",
                201,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:alice",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_800_000_000_i64,
                }),
            ),
            authority_event(
                &binding,
                "renew-2",
                "RegistrationRenewed",
                "0xrenew",
                202,
                Some(1),
                json!({
                    "expiry": 1_800_000_000_i64,
                }),
                json!({
                    "expiry": 1_900_000_000_i64,
                }),
            ),
            authority_event(
                &binding,
                "expiry-2",
                "ExpiryChanged",
                "0xrenew",
                202,
                Some(2),
                json!({
                    "expiry": 1_800_000_000_i64,
                }),
                json!({
                    "expiry": 1_900_000_000_i64,
                }),
            ),
            authority_event(
                &binding,
                "transfer-2",
                "TokenControlTransferred",
                "0xtransfer",
                203,
                Some(0),
                json!({
                    "from": "0x0000000000000000000000000000000000000aaa",
                }),
                json!({
                    "to": "0x0000000000000000000000000000000000000bbb",
                }),
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt row must exist")?;
    assert_eq!(row.surface_binding_id, Some(binding.surface_binding_id));
    assert_eq!(row.resource_id, Some(binding.resource_id));
    assert_eq!(row.token_lineage_id, Some(binding.token_lineage_id));
    assert_eq!(
        row.declared_summary["registration"]["expiry"],
        Value::Number(1_900_000_000_i64.into())
    );
    assert_eq!(
        row.declared_summary["registration"]["registrant"],
        Value::String("0x0000000000000000000000000000000000000bbb".to_owned())
    );
    assert_eq!(
        row.declared_summary["control"]["registrant"],
        Value::String("0x0000000000000000000000000000000000000bbb".to_owned())
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_projects_registry_owner_from_authority_epoch_change() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new(
        "ens:controller.eth",
        "controller.eth",
        0x7100,
        0x7200,
        0x7300,
    );
    let registry_owner = "0x0000000000000000000000000000000000000aaa";

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xgrant", 301, 1_717_171_901),
            raw_block("ethereum-mainnet", "0xregistry-owner", 302, 1_717_171_902),
        ],
    )
    .await?;
    seed_identity(database.pool(), &binding, "0xgrant", 301, 1_717_171_901).await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "grant-controller",
                "RegistrationGranted",
                "0xgrant",
                301,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:controller",
                    "registrant": "0x0000000000000000000000000000000000000bbb",
                    "expiry": 1_800_000_000_i64,
                }),
            ),
            authority_event(
                &binding,
                "registry-owner-epoch",
                "AuthorityEpochChanged",
                "0xregistry-owner",
                302,
                None,
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:controller",
                }),
                json!({
                    "authority_kind": "registry_only",
                    "authority_key": "registry-only:ethereum-mainnet:0xcontroller",
                    "registry_owner": registry_owner,
                }),
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt row must exist")?;
    assert_eq!(
        row.declared_summary["control"]["registry_owner"],
        Value::String(registry_owner.to_owned())
    );
    assert_eq!(
        row.declared_summary["control"]["latest_event_kind"],
        Value::String("AuthorityEpochChanged".to_owned())
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_keeps_resource_permission_manager_out_of_registry_owner() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new("ens:manager.eth", "manager.eth", 0x9100, 0x9200, 0x9300);
    let token_holder = "0x0000000000000000000000000000000000000bbb";
    let registry_owner = "0x0000000000000000000000000000000000000aaa";
    let controller = "0x0000000000000000000000000000000000000ccc";
    let old_token_lineage_id = Uuid::from_u128(0x9101);
    let old_resource_id = Uuid::from_u128(0x9201);

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xmanager-grant", 501, 1_717_171_901),
            raw_block("ethereum-mainnet", "0xmanager-owner", 502, 1_717_171_902),
            raw_block("ethereum-mainnet", "0xmanager-control", 503, 1_717_171_903),
            raw_block(
                "ethereum-mainnet",
                "0xmanager-old-control",
                504,
                1_717_171_904,
            ),
            raw_block(
                "ethereum-mainnet",
                "0xmanager-downgrade",
                505,
                1_717_171_905,
            ),
        ],
    )
    .await?;
    seed_identity(
        database.pool(),
        &binding,
        "0xmanager-grant",
        501,
        1_717_171_901,
    )
    .await?;
    upsert_token_lineages(
        database.pool(),
        &[token_lineage(
            old_token_lineage_id,
            "0xmanager-old-control",
            504,
        )],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[resource(
            old_resource_id,
            old_token_lineage_id,
            "0xmanager-old-control",
            504,
        )],
    )
    .await?;
    let mut old_resource_control = with_source_family(
        authority_event(
            &binding,
            "manager-old-control",
            "PermissionChanged",
            "0xmanager-old-control",
            504,
            Some(0),
            json!({}),
            json!({
                "scope": {"kind": "resource"},
                "subject": token_holder,
                "effective_powers": ["resource_control"],
                "grant_source": {
                    "kind": "ens_v1_authority",
                    "authority_kind": "registrar",
                    "source_event_kind": "TokenControlTransferred"
                }
            }),
        ),
        "ens_v1_registry_l1",
    );
    old_resource_control.resource_id = Some(old_resource_id);
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "manager-grant",
                "RegistrationGranted",
                "0xmanager-grant",
                501,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:manager",
                    "registrant": token_holder,
                    "expiry": 1_800_000_000_i64,
                }),
            ),
            with_source_family(
                authority_event(
                    &binding,
                    "manager-owner",
                    "AuthorityTransferred",
                    "0xmanager-owner",
                    502,
                    Some(0),
                    json!({
                        "owner": token_holder,
                    }),
                    json!({
                        "owner": registry_owner,
                    }),
                ),
                "ens_v1_registry_l1",
            ),
            with_source_family(
                authority_event(
                    &binding,
                    "manager-control",
                    "PermissionChanged",
                    "0xmanager-control",
                    503,
                    Some(0),
                    json!({
                        "scope": {"kind": "resource"},
                        "subject": token_holder,
                        "effective_powers": ["resource_control"],
                    }),
                    json!({
                        "scope": {"kind": "resource"},
                        "subject": controller,
                        "effective_powers": ["resource_control"],
                        "grant_source": {
                            "kind": "ens_v1_authority",
                            "authority_kind": "registry_only",
                            "source_event_kind": "AuthorityTransferred"
                        }
                    }),
                ),
                "ens_v1_registry_l1",
            ),
            old_resource_control,
            with_source_family(
                authority_event(
                    &binding,
                    "manager-control-downgrade",
                    "PermissionChanged",
                    "0xmanager-downgrade",
                    505,
                    Some(0),
                    json!({
                        "scope": {"kind": "resource"},
                        "subject": controller,
                        "effective_powers": ["resource_control", "set_records"],
                    }),
                    json!({
                        "scope": {"kind": "resource"},
                        "subject": controller,
                        "effective_powers": ["set_records"],
                        "revocation_source": {
                            "kind": "ens_v1_authority",
                            "authority_kind": "registry_only",
                            "source_event_kind": "AuthorityTransferred"
                        }
                    }),
                ),
                "ens_v1_registry_l1",
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt manager row must exist")?;
    assert_eq!(
        row.declared_summary["registration"]["registrant"],
        Value::String(token_holder.to_owned())
    );
    assert_eq!(
        row.declared_summary["control"]["registry_owner"],
        Value::String(registry_owner.to_owned())
    );
    assert_eq!(
        row.declared_summary["control"]["latest_event_kind"],
        Value::String("AuthorityTransferred".to_owned())
    );
    assert!(
        !row.provenance.to_string().contains(controller),
        "name_current provenance should not inline resource permission manager events"
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_switches_to_rebound_authority_epoch_binding() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x5100, 0x5200, 0x5300);
    let rebound = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x6100, 0x6200, 0x6300);

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xgrant", 301, 1_717_171_901),
            raw_block("ethereum-mainnet", "0xrebind", 302, 1_717_171_902),
        ],
    )
    .await?;
    seed_identity(database.pool(), &binding, "0xgrant", 301, 1_717_171_901).await?;
    seed_rebound_identity(
        database.pool(),
        &binding,
        &rebound,
        "0xrebind",
        302,
        1_717_171_902,
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "grant-3",
                "RegistrationGranted",
                "0xgrant",
                301,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:alice",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_800_000_000_i64,
                }),
            ),
            authority_event(
                &binding,
                "release-3",
                "RegistrationReleased",
                "0xrebind",
                302,
                None,
                json!({
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_800_000_000_i64,
                }),
                json!({
                    "released_at": 1_717_171_902_i64,
                }),
            ),
            authority_event(
                &binding,
                "epoch-3",
                "AuthorityEpochChanged",
                "0xrebind",
                302,
                None,
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:alice",
                }),
                json!({
                    "authority_kind": "registry_only",
                    "authority_key": "registry:ethereum-mainnet:alice",
                    "status": "wrapped",
                    "expiry": 1_900_000_000_i64,
                }),
            ),
            authority_event(
                &binding,
                "transfer-3",
                "AuthorityTransferred",
                "0xrebind",
                302,
                Some(0),
                json!({
                    "owner": "0x0000000000000000000000000000000000000aaa",
                }),
                json!({
                    "owner": "0x0000000000000000000000000000000000000ccc",
                }),
            ),
            authority_event(
                &binding,
                "unbound-3",
                "SurfaceUnbound",
                "0xrebind",
                302,
                None,
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:alice",
                }),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:alice",
                    "active_to": 1_717_171_902_i64,
                }),
            ),
            authority_event(
                &rebound,
                "bound-3",
                "SurfaceBound",
                "0xrebind",
                302,
                None,
                json!({}),
                json!({
                    "authority_kind": "registry_only",
                    "authority_key": "registry:ethereum-mainnet:alice",
                    "active_from": 1_717_171_902_i64,
                    "binding_kind": "declared_registry_path",
                }),
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt row must exist")?;
    assert_eq!(row.surface_binding_id, Some(rebound.surface_binding_id));
    assert_eq!(row.resource_id, Some(rebound.resource_id));
    assert_eq!(row.token_lineage_id, Some(rebound.token_lineage_id));
    assert_eq!(
        row.declared_summary["registration"]["authority_kind"],
        Value::String("registry_only".to_owned())
    );
    assert_eq!(
        row.declared_summary["registration"]["status"],
        Value::String("released".to_owned())
    );
    assert_eq!(
        row.declared_summary["control"]["registry_owner"],
        Value::String("0x0000000000000000000000000000000000000ccc".to_owned())
    );
    assert_eq!(
        row.declared_summary["control"]["status"],
        Value::String("wrapped".to_owned())
    );
    assert_eq!(
        row.declared_summary["control"]["expiry"],
        Value::String(format_timestamp(timestamp(1_900_000_000)))
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_preserves_observed_wildcard_binding_kind() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new("ens:wildcard.eth", "wildcard.eth", 0x3301, 0x3302, 0x3303);

    seed_raw_blocks(
        database.pool(),
        &[raw_block("ethereum-mainnet", "0xwild", 241, 1_717_171_741)],
    )
    .await?;
    upsert_token_lineages(
        database.pool(),
        &[token_lineage(binding.token_lineage_id, "0xwild", 241)],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[resource(
            binding.resource_id,
            binding.token_lineage_id,
            "0xwild",
            241,
        )],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            &binding.logical_name_id,
            &binding.display_name,
            "0xwild",
            241,
        )],
    )
    .await?;

    let mut wildcard_binding = surface_binding(&binding, 1_717_171_741, None, "0xwild", 241);
    wildcard_binding.binding_kind = SurfaceBindingKind::ObservedWildcardPath;
    upsert_surface_bindings(database.pool(), &[wildcard_binding]).await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt row must exist")?;
    assert_eq!(
        row.binding_kind,
        Some(SurfaceBindingKind::ObservedWildcardPath)
    );
    assert_eq!(row.coverage["status"], Value::String("full".to_owned()));
    assert_eq!(row.coverage["unsupported_reason"], Value::Null);

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_history_heads_match_canonical_name_history_ordering() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new("ens:history.eth", "history.eth", 0x8100, 0x8200, 0x8300);
    let historical_resource_id = Uuid::from_u128(0x8400);

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xgrant", 510, 1_717_172_110),
            raw_block("ethereum-mainnet", "0xsurface", 511, 1_717_172_111),
            raw_block("ethereum-mainnet", "0xresource", 512, 1_717_172_112),
        ],
    )
    .await?;
    seed_identity(database.pool(), &binding, "0xgrant", 510, 1_717_172_110).await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "grant-history",
                "RegistrationGranted",
                "0xgrant",
                510,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:history",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_800_000_000_i64,
                }),
            ),
            history_event(
                "surface-head",
                Some(&binding.logical_name_id),
                Some(historical_resource_id),
                Some("ethereum-mainnet"),
                Some(511),
                Some("0xsurface"),
                Some("0xtx511"),
                Some(0),
            ),
            history_event(
                "resource-head",
                Some("ens:other.eth"),
                Some(binding.resource_id),
                Some("ethereum-mainnet"),
                Some(512),
                Some("0xresource"),
                Some("0xtx512"),
                Some(0),
            ),
        ],
    )
    .await?;

    rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("rebuilt row must exist")?;
    let resource_ids = load_name_resource_ids(database.pool(), &binding.logical_name_id).await?;
    let expected_surface_head = load_name_history_head(
        database.pool(),
        &binding.logical_name_id,
        &resource_ids,
        HistoryScope::Surface,
        true,
    )
    .await?
    .context("surface head must exist")?;
    let expected_resource_head = load_name_history_head(
        database.pool(),
        &binding.logical_name_id,
        &resource_ids,
        HistoryScope::Resource,
        true,
    )
    .await?
    .context("resource head must exist")?;

    assert_eq!(
        row.declared_summary["history"]["surface_head"],
        history_pointer_json(&history_pointer_from_event(&expected_surface_head))
    );
    assert_eq!(
        row.declared_summary["history"]["resource_head"],
        history_pointer_json(&history_pointer_from_event(&expected_resource_head))
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_is_idempotent() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x7100, 0x7200, 0x7300);

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xgrant", 401, 1_717_172_001),
            raw_block("ethereum-mainnet", "0xrenew", 402, 1_717_172_002),
        ],
    )
    .await?;
    seed_identity(database.pool(), &binding, "0xgrant", 401, 1_717_172_001).await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "grant-4",
                "RegistrationGranted",
                "0xgrant",
                401,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:alice",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                    "expiry": 1_800_000_000_i64,
                }),
            ),
            authority_event(
                &binding,
                "renew-4",
                "RegistrationRenewed",
                "0xrenew",
                402,
                Some(1),
                json!({
                    "expiry": 1_800_000_000_i64,
                }),
                json!({
                    "expiry": 1_900_000_000_i64,
                }),
            ),
        ],
    )
    .await?;

    let first = rebuild_name_current(database.pool(), None).await?;
    assert_eq!(first.upserted_row_count, 1);
    let first_row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("first rebuild row must exist")?;

    let second = rebuild_name_current(database.pool(), None).await?;
    assert_eq!(second.upserted_row_count, 1);
    let second_row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("second rebuild row must exist")?;

    assert_eq!(first_row, second_row);

    database.cleanup().await
}

#[tokio::test]
async fn full_rebuild_keeps_visible_rows_when_projection_build_fails() -> Result<()> {
    let database = TestDatabase::new().await?;
    let stable = IdentityBinding::new("ens:stable.eth", "stable.eth", 0x8110, 0x8210, 0x8310);
    let broken = IdentityBinding::new("ens:broken.eth", "broken.eth", 0x8120, 0x8220, 0x8320);

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xstable-grant", 411, 1_717_172_011),
            raw_block("ethereum-mainnet", "0xbroken-grant", 412, 1_717_172_012),
        ],
    )
    .await?;
    seed_identity(
        database.pool(),
        &stable,
        "0xstable-grant",
        411,
        1_717_172_011,
    )
    .await?;
    seed_identity(
        database.pool(),
        &broken,
        "0xbroken-grant",
        412,
        1_717_172_012,
    )
    .await?;
    upsert_name_current_rows(
        database.pool(),
        &[NameCurrentRow {
            logical_name_id: stable.logical_name_id.clone(),
            namespace: "ens".to_owned(),
            canonical_display_name: "stable.eth".to_owned(),
            normalized_name: "stable.eth".to_owned(),
            namehash: "node:stable.eth".to_owned(),
            surface_binding_id: None,
            resource_id: None,
            token_lineage_id: None,
            binding_kind: None,
            declared_summary: json!({"status": "stable-before-full-rebuild"}),
            provenance: json!({"derivation_kind": NAME_CURRENT_DERIVATION_KIND}),
            coverage: json!({"status": "supported"}),
            chain_positions: json!({}),
            canonicality_summary: json!({
                "status": "finalized",
                "chains": {"ethereum-mainnet": "finalized"}
            }),
            manifest_version: 1,
            last_recomputed_at: timestamp(1_717_172_011),
        }],
    )
    .await?;
    let mut broken_resolver = resolver_event(
        &broken,
        "resolver-missing-chain",
        "0x0000000000000000000000000000000000000def",
        "0xbroken-resolver",
        413,
        0,
    );
    broken_resolver.chain_id = None;
    seed_events(database.pool(), &[broken_resolver]).await?;

    let error = rebuild_name_current(database.pool(), None)
        .await
        .expect_err("full rebuild should fail when one projected row cannot be built");
    assert!(error.to_string().contains("ResolverChanged event"));

    let stable_row = load_name_current(database.pool(), &stable.logical_name_id)
        .await?
        .context("pre-existing row should remain visible after failed full rebuild")?;
    assert_eq!(
        stable_row.declared_summary["status"],
        json!("stable-before-full-rebuild")
    );
    assert_eq!(
        load_name_current(database.pool(), &broken.logical_name_id).await?,
        None
    );

    database.cleanup().await
}

#[tokio::test]
async fn keyed_rebuild_keeps_visible_row_when_projection_build_fails() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x8100, 0x8200, 0x8300);

    seed_raw_blocks(
        database.pool(),
        &[raw_block("ethereum-mainnet", "0xgrant", 401, 1_717_172_001)],
    )
    .await?;
    seed_identity(database.pool(), &binding, "0xgrant", 401, 1_717_172_001).await?;
    upsert_name_current_rows(
        database.pool(),
        &[NameCurrentRow {
            logical_name_id: binding.logical_name_id.clone(),
            namespace: "ens".to_owned(),
            canonical_display_name: "alice.eth".to_owned(),
            normalized_name: "alice.eth".to_owned(),
            namehash: "node:alice.eth".to_owned(),
            surface_binding_id: None,
            resource_id: None,
            token_lineage_id: None,
            binding_kind: None,
            declared_summary: json!({"status": "stale"}),
            provenance: json!({"derivation_kind": NAME_CURRENT_DERIVATION_KIND}),
            coverage: json!({"status": "supported"}),
            chain_positions: json!({}),
            canonicality_summary: json!({
                "status": "finalized",
                "chains": {"ethereum-mainnet": "finalized"}
            }),
            manifest_version: 1,
            last_recomputed_at: timestamp(1_717_172_001),
        }],
    )
    .await?;
    seed_events(
        database.pool(),
        &[NormalizedEvent {
            event_identity: "resolver-missing-chain".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some(binding.logical_name_id.clone()),
            resource_id: Some(binding.resource_id),
            event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
            source_family: "ens_v1_registry_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: None,
            block_number: Some(402),
            block_hash: Some("0xresolver".to_owned()),
            transaction_hash: Some("0xtxresolver".to_owned()),
            log_index: Some(0),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "block_hash": "0xresolver",
                "log_index": 0
            }),
            derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000def"
            }),
        }],
    )
    .await?;

    let error = rebuild_name_current(database.pool(), Some(&binding.logical_name_id))
        .await
        .expect_err("rebuild should fail when a resolver event is missing chain_id");
    assert!(error.to_string().contains("ResolverChanged event"));

    let row = load_name_current(database.pool(), &binding.logical_name_id)
        .await?
        .context("stale visible row should still exist after failed rebuild")?;
    assert_eq!(row.declared_summary["status"], json!("stale"));

    database.cleanup().await
}

#[derive(Clone, Debug)]
struct IdentityBinding {
    logical_name_id: String,
    display_name: String,
    token_lineage_id: Uuid,
    resource_id: Uuid,
    surface_binding_id: Uuid,
}

impl IdentityBinding {
    fn new(
        logical_name_id: &str,
        display_name: &str,
        token_lineage: u128,
        resource: u128,
        binding: u128,
    ) -> Self {
        Self {
            logical_name_id: logical_name_id.to_owned(),
            display_name: display_name.to_owned(),
            token_lineage_id: Uuid::from_u128(token_lineage),
            resource_id: Uuid::from_u128(resource),
            surface_binding_id: Uuid::from_u128(binding),
        }
    }
}

async fn seed_identity(
    pool: &PgPool,
    binding: &IdentityBinding,
    block_hash: &str,
    block_number: i64,
    block_timestamp: i64,
) -> Result<()> {
    upsert_token_lineages(
        pool,
        &[token_lineage(
            binding.token_lineage_id,
            block_hash,
            block_number,
        )],
    )
    .await?;
    upsert_resources(
        pool,
        &[resource(
            binding.resource_id,
            binding.token_lineage_id,
            block_hash,
            block_number,
        )],
    )
    .await?;
    upsert_name_surfaces(
        pool,
        &[name_surface(
            &binding.logical_name_id,
            &binding.display_name,
            block_hash,
            block_number,
        )],
    )
    .await?;
    upsert_surface_bindings(
        pool,
        &[surface_binding(
            binding,
            block_timestamp,
            None,
            block_hash,
            block_number,
        )],
    )
    .await?;
    Ok(())
}

async fn seed_rebound_identity(
    pool: &PgPool,
    first: &IdentityBinding,
    rebound: &IdentityBinding,
    block_hash: &str,
    block_number: i64,
    block_timestamp: i64,
) -> Result<()> {
    upsert_token_lineages(
        pool,
        &[token_lineage(
            rebound.token_lineage_id,
            block_hash,
            block_number,
        )],
    )
    .await?;
    upsert_resources(
        pool,
        &[resource(
            rebound.resource_id,
            rebound.token_lineage_id,
            block_hash,
            block_number,
        )],
    )
    .await?;
    upsert_surface_bindings(
        pool,
        &[
            surface_binding(
                first,
                1_717_171_901,
                Some(timestamp(block_timestamp)),
                "0xgrant",
                301,
            ),
            surface_binding(rebound, block_timestamp, None, block_hash, block_number),
        ],
    )
    .await?;
    Ok(())
}

async fn seed_basenames_identity(
    pool: &PgPool,
    binding: &IdentityBinding,
    block_hash: &str,
    block_number: i64,
    _block_timestamp: i64,
) -> Result<()> {
    upsert_token_lineages(
        pool,
        &[TokenLineage {
            token_lineage_id: binding.token_lineage_id,
            chain_id: "base-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_name_current_test", "kind": "token_lineage"}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_resources(
        pool,
        &[Resource {
            resource_id: binding.resource_id,
            token_lineage_id: Some(binding.token_lineage_id),
            chain_id: "base-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_name_current_test", "kind": "resource"}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_name_surfaces(
        pool,
        &[NameSurface {
            logical_name_id: binding.logical_name_id.clone(),
            namespace: BASENAMES_NAMESPACE.to_owned(),
            input_name: binding.display_name.clone(),
            canonical_display_name: "Alice.base.eth".to_owned(),
            normalized_name: binding.display_name.clone(),
            dns_encoded_name: binding.display_name.as_bytes().to_vec(),
            namehash: format!("namehash:{}", binding.display_name),
            labelhashes: labelhashes_for_name(&binding.display_name),
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-surface".to_owned(),
            block_number: 500,
            provenance: json!({"source": "worker_name_current_test", "kind": "name_surface"}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_surface_bindings(
        pool,
        &[SurfaceBinding {
            surface_binding_id: binding.surface_binding_id,
            logical_name_id: binding.logical_name_id.clone(),
            resource_id: binding.resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_172_501),
            active_to: None,
            chain_id: "base-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_name_current_test", "kind": "surface_binding"}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    Ok(())
}

async fn seed_raw_blocks(pool: &PgPool, blocks: &[RawBlock]) -> Result<()> {
    upsert_raw_blocks(pool, blocks).await?;
    Ok(())
}

async fn seed_raw_logs(pool: &PgPool, logs: &[RawLog]) -> Result<()> {
    upsert_raw_logs(pool, logs).await?;
    Ok(())
}

async fn seed_events(pool: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
    upsert_normalized_events(pool, events).await?;
    Ok(())
}

async fn insert_chain_checkpoint(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO chain_checkpoints (
            chain_id,
            finalized_block_hash,
            finalized_block_number
        )
        VALUES ($1, $2, $3)
        ON CONFLICT (chain_id)
        DO UPDATE SET
            finalized_block_hash = EXCLUDED.finalized_block_hash,
            finalized_block_number = EXCLUDED.finalized_block_number
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(block_number)
    .execute(pool)
    .await
    .with_context(|| format!("failed to insert chain checkpoint for {chain_id}"))?;

    Ok(())
}

async fn seed_ens_v2_exact_name_profile_manifests(pool: &PgPool) -> Result<(i64, i64)> {
    let registry_manifest_id = insert_manifest_version(
        pool,
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
        1,
        MANIFEST_ROLLOUT_STATUS_ACTIVE,
    )
    .await?;
    let registrar_manifest_id = insert_manifest_version(
        pool,
        SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
        2,
        MANIFEST_ROLLOUT_STATUS_ACTIVE,
    )
    .await?;
    insert_capability_flag(
        pool,
        registrar_manifest_id,
        EXACT_NAME_PROFILE_CAPABILITY,
        CAPABILITY_STATUS_SUPPORTED,
    )
    .await?;

    Ok((registry_manifest_id, registrar_manifest_id))
}

async fn insert_manifest_version(
    pool: &PgPool,
    source_family: &str,
    manifest_version: i64,
    rollout_status: &str,
) -> Result<i64> {
    sqlx::query(
        r#"
        INSERT INTO manifest_versions (
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            rollout_status,
            normalizer_version,
            file_path,
            manifest_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6::manifest_rollout_status, $7, $8, $9::jsonb)
        RETURNING manifest_id
        "#,
    )
    .bind(manifest_version)
    .bind(ENS_NAMESPACE)
    .bind(source_family)
    .bind(ETHEREUM_SEPOLIA_CHAIN_ID)
    .bind(SELECTED_ENS_V2_EXACT_NAME_DEPLOYMENT_EPOCH)
    .bind(rollout_status)
    .bind("ensip15@ens-normalize-0.1.1")
    .bind(format!(
        "tests/{source_family}/ens-v2-sepolia-dev-v{manifest_version}.toml"
    ))
    .bind(json!({}))
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to insert manifest_version for {source_family}"))?
    .try_get("manifest_id")
    .context("failed to read manifest_id")
}

async fn insert_basenames_execution_manifest_version(
    pool: &PgPool,
    manifest_version: i64,
    rollout_status: &str,
) -> Result<i64> {
    let manifest_id = sqlx::query(
        r#"
        INSERT INTO manifest_versions (
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            rollout_status,
            normalizer_version,
            file_path,
            manifest_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6::manifest_rollout_status, $7, $8, $9::jsonb)
        RETURNING manifest_id
        "#,
    )
    .bind(manifest_version)
    .bind(BASENAMES_NAMESPACE)
    .bind(SOURCE_FAMILY_BASENAMES_EXECUTION)
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(BASENAMES_V1_DEPLOYMENT_EPOCH)
    .bind(rollout_status)
    .bind("ensip15@ens-normalize-0.1.1")
    .bind(format!(
        "tests/{}/{}-v{manifest_version}.toml",
        SOURCE_FAMILY_BASENAMES_EXECUTION, BASENAMES_V1_DEPLOYMENT_EPOCH
    ))
    .bind(json!({}))
    .fetch_one(pool)
    .await
    .context("failed to insert basenames_execution manifest_version")?
    .try_get("manifest_id")
    .context("failed to read basenames_execution manifest_id")?;
    insert_capability_flag(
        pool,
        manifest_id,
        VERIFIED_RESOLUTION_CAPABILITY,
        CAPABILITY_STATUS_SUPPORTED,
    )
    .await?;
    insert_basenames_execution_manifest_contract(pool, manifest_id, manifest_version).await?;
    Ok(manifest_id)
}

async fn insert_basenames_execution_manifest_contract(
    pool: &PgPool,
    manifest_id: i64,
    manifest_version: i64,
) -> Result<()> {
    let contract_instance_id =
        Uuid::from_u128(0x0b45_0000_0000_0000_0000_0000_0000_0000 + manifest_version as u128);
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind,
            provenance
        )
        VALUES ($1, $2, 'contract', $3::jsonb)
        ON CONFLICT (contract_instance_id) DO NOTHING
        "#,
    )
    .bind(contract_instance_id)
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(json!({"seed": "worker_name_current_basenames_execution"}))
    .execute(pool)
    .await
    .context("failed to insert basenames_execution contract_instance")?;

    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            role,
            proxy_kind
        )
        VALUES ($1, 'contract', 'l1_resolver', $2, $3, 'l1_resolver', 'none')
        "#,
    )
    .bind(manifest_id)
    .bind(contract_instance_id)
    .bind(BASENAMES_L1_RESOLVER_ADDRESS.to_ascii_lowercase())
    .execute(pool)
    .await
    .context("failed to insert basenames_execution manifest_contract_instance")?;

    Ok(())
}

async fn insert_capability_flag(
    pool: &PgPool,
    manifest_id: i64,
    capability_name: &str,
    status: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO manifest_capability_flags (
            manifest_id,
            capability_name,
            status,
            notes
        )
        VALUES ($1, $2, $3::capability_support_status, NULL)
        "#,
    )
    .bind(manifest_id)
    .bind(capability_name)
    .bind(status)
    .execute(pool)
    .await
    .with_context(|| format!("failed to insert capability flag {capability_name}"))?;

    Ok(())
}

fn raw_block(chain_id: &str, block_hash: &str, block_number: i64, unix_timestamp: i64) -> RawBlock {
    RawBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: None,
        block_number,
        block_timestamp: timestamp(unix_timestamp),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn old_registry_raw_log(
    identity_suffix: &str,
    block_hash: &str,
    block_number: i64,
    log_index: i64,
    owner: &str,
    resolver: &str,
) -> RawLog {
    RawLog {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("tx:ensold:{identity_suffix}"),
        transaction_index: 0,
        log_index,
        emitting_address: "0x0000000000000000000000000000000000000f01".to_owned(),
        topics: vec![
            "ENSRegistryOld".to_owned(),
            identity_suffix.to_owned(),
            owner.to_owned(),
            resolver.to_owned(),
        ],
        data: format!("suppressed-old-registry:{identity_suffix}").into_bytes(),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn chain_lineage_block(
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    unix_timestamp: i64,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: Some(format!("0xparent{block_number:08x}")),
        block_number,
        block_timestamp: timestamp(unix_timestamp),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn token_lineage(token_lineage_id: Uuid, block_hash: &str, block_number: i64) -> TokenLineage {
    TokenLineage {
        token_lineage_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"source": "worker_name_current_test", "kind": "token_lineage"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn resource(
    resource_id: Uuid,
    token_lineage_id: Uuid,
    block_hash: &str,
    block_number: i64,
) -> Resource {
    Resource {
        resource_id,
        token_lineage_id: Some(token_lineage_id),
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"source": "worker_name_current_test", "kind": "resource"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn name_surface(
    logical_name_id: &str,
    display_name: &str,
    block_hash: &str,
    block_number: i64,
) -> NameSurface {
    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        input_name: display_name.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        dns_encoded_name: display_name.as_bytes().to_vec(),
        namehash: format!("namehash:{display_name}"),
        labelhashes: labelhashes_for_name(display_name),
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"source": "worker_name_current_test", "kind": "name_surface"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn labelhashes_for_name(name: &str) -> Vec<String> {
    name.split('.').map(labelhash_for_label).collect()
}

fn labelhash_for_label(label: &str) -> String {
    label_preimage_from_label(label, "worker_name_current_test", 1, json!({}))
        .expect("test label must hash")
        .labelhash
}

fn surface_binding(
    binding: &IdentityBinding,
    active_from_unix: i64,
    active_to: Option<OffsetDateTime>,
    block_hash: &str,
    block_number: i64,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id: binding.surface_binding_id,
        logical_name_id: binding.logical_name_id.clone(),
        resource_id: binding.resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: timestamp(active_from_unix),
        active_to,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"source": "worker_name_current_test", "kind": "surface_binding"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

#[allow(clippy::too_many_arguments)]
fn authority_event(
    binding: &IdentityBinding,
    identity_suffix: &str,
    event_kind: &str,
    block_hash: &str,
    block_number: i64,
    log_index: Option<i64>,
    before_state: Value,
    after_state: Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("worker-test:{event_kind}:{identity_suffix}"),
        namespace: "ens".to_owned(),
        logical_name_id: Some(binding.logical_name_id.clone()),
        resource_id: Some(binding.resource_id),
        event_kind: event_kind.to_owned(),
        source_family: "ens_v1_registrar_l1".to_owned(),
        manifest_version: 3,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(format!("tx:{identity_suffix}")),
        log_index,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_hash": block_hash,
            "block_number": block_number,
            "transaction_hash": format!("tx:{identity_suffix}"),
            "log_index": log_index,
        }),
        derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state,
        after_state,
    }
}

#[allow(clippy::too_many_arguments)]
fn ens_v2_registry_event(
    binding: &IdentityBinding,
    identity_suffix: &str,
    event_kind: &str,
    block_hash: &str,
    block_number: i64,
    log_index: i64,
    before_state: Value,
    after_state: Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("worker-test:ens-v2:{event_kind}:{identity_suffix}"),
        namespace: "ens".to_owned(),
        logical_name_id: Some(binding.logical_name_id.clone()),
        resource_id: Some(binding.resource_id),
        event_kind: event_kind.to_owned(),
        source_family: "ens_v2_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-sepolia".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(format!("tx:ens-v2:{identity_suffix}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-sepolia",
            "block_hash": block_hash,
            "block_number": block_number,
            "transaction_hash": format!("tx:ens-v2:{identity_suffix}"),
            "log_index": log_index,
        }),
        derivation_kind: ENS_V2_REGISTRY_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state,
        after_state,
    }
}

#[allow(clippy::too_many_arguments)]
fn ens_v2_registrar_event(
    binding: &IdentityBinding,
    identity_suffix: &str,
    event_kind: &str,
    block_hash: &str,
    block_number: i64,
    log_index: i64,
    before_state: Value,
    after_state: Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("worker-test:ens-v2-registrar:{event_kind}:{identity_suffix}"),
        namespace: "ens".to_owned(),
        logical_name_id: Some(binding.logical_name_id.clone()),
        resource_id: Some(binding.resource_id),
        event_kind: event_kind.to_owned(),
        source_family: SOURCE_FAMILY_ENS_V2_REGISTRAR_L1.to_owned(),
        manifest_version: 2,
        source_manifest_id: None,
        chain_id: Some("ethereum-sepolia".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(format!("tx:ens-v2-registrar:{identity_suffix}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-sepolia",
            "block_hash": block_hash,
            "block_number": block_number,
            "transaction_hash": format!("tx:ens-v2-registrar:{identity_suffix}"),
            "log_index": log_index,
        }),
        derivation_kind: ENS_V2_REGISTRAR_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state,
        after_state,
    }
}

fn ens_v2_alias_event(
    binding: &IdentityBinding,
    identity_suffix: &str,
    block_hash: &str,
    block_number: i64,
    log_index: i64,
    after_state: Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("worker-test:ens-v2:{EVENT_KIND_ALIAS_CHANGED}:{identity_suffix}"),
        namespace: ENS_NAMESPACE.to_owned(),
        logical_name_id: Some(binding.logical_name_id.clone()),
        resource_id: Some(binding.resource_id),
        event_kind: EVENT_KIND_ALIAS_CHANGED.to_owned(),
        source_family: SOURCE_FAMILY_ENS_V2_RESOLVER.to_owned(),
        manifest_version: 5,
        source_manifest_id: None,
        chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(format!("tx:ens-v2-alias:{identity_suffix}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "block_hash": block_hash,
            "block_number": block_number,
            "transaction_hash": format!("tx:ens-v2-alias:{identity_suffix}"),
            "log_index": log_index,
        }),
        derivation_kind: ENS_V2_RESOLVER_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state,
    }
}

fn with_source_manifest_id(mut event: NormalizedEvent, source_manifest_id: i64) -> NormalizedEvent {
    event.source_manifest_id = Some(source_manifest_id);
    event
}

fn with_source_family(mut event: NormalizedEvent, source_family: &str) -> NormalizedEvent {
    event.source_family = source_family.to_owned();
    event
}

fn coverage_event(source_family: &str, chain_id: &str) -> RelevantEvent {
    RelevantEvent {
        normalized_event_id: 1,
        resource_id: None,
        event_kind: "RegistrationGranted".to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        source_manifest_version: None,
        source_manifest_namespace: None,
        source_manifest_source_family: None,
        source_manifest_chain: None,
        source_manifest_deployment_epoch: None,
        source_manifest_rollout_status: None,
        exact_name_profile_status: None,
        chain_id: Some(chain_id.to_owned()),
        block_number: Some(1),
        block_hash: Some(format!("0x{chain_id}")),
        block_timestamp: None,
        raw_fact_ref: json!({"kind": "raw_log"}),
        canonicality_state: CanonicalityState::Finalized,
        after_state: json!({}),
    }
}

fn selected_ens_v2_coverage_event(
    source_family: &str,
    manifest_version: i64,
    source_manifest_id: i64,
    exact_name_profile_status: Option<&str>,
) -> RelevantEvent {
    let mut event = coverage_event(source_family, ETHEREUM_SEPOLIA_CHAIN_ID);
    event.manifest_version = manifest_version;
    event.source_manifest_id = Some(source_manifest_id);
    event.source_manifest_version = Some(manifest_version);
    event.source_manifest_namespace = Some(ENS_NAMESPACE.to_owned());
    event.source_manifest_source_family = Some(source_family.to_owned());
    event.source_manifest_chain = Some(ETHEREUM_SEPOLIA_CHAIN_ID.to_owned());
    event.source_manifest_deployment_epoch =
        Some(SELECTED_ENS_V2_EXACT_NAME_DEPLOYMENT_EPOCH.to_owned());
    event.source_manifest_rollout_status = Some(MANIFEST_ROLLOUT_STATUS_ACTIVE.to_owned());
    event.exact_name_profile_status = exact_name_profile_status.map(str::to_owned);
    event
}

fn resolver_event(
    binding: &IdentityBinding,
    identity_suffix: &str,
    resolver_address: &str,
    block_hash: &str,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("worker-test:{EVENT_KIND_RESOLVER_CHANGED}:{identity_suffix}"),
        namespace: "ens".to_owned(),
        logical_name_id: Some(binding.logical_name_id.clone()),
        resource_id: Some(binding.resource_id),
        event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
        source_family: "ens_v1_unwrapped_authority".to_owned(),
        manifest_version: 4,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(format!("tx:{identity_suffix}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_hash": block_hash,
            "block_number": block_number,
            "transaction_hash": format!("tx:{identity_suffix}"),
            "log_index": log_index,
        }),
        derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "resolver": resolver_address,
            "namehash": format!("namehash:{}", binding.display_name),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn basenames_authority_event(
    binding: &IdentityBinding,
    identity_suffix: &str,
    event_kind: &str,
    source_family: &str,
    block_hash: &str,
    block_number: i64,
    log_index: Option<i64>,
    before_state: Value,
    after_state: Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("worker-test:{event_kind}:{identity_suffix}"),
        namespace: BASENAMES_NAMESPACE.to_owned(),
        logical_name_id: Some(binding.logical_name_id.clone()),
        resource_id: Some(binding.resource_id),
        event_kind: event_kind.to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: 3,
        source_manifest_id: None,
        chain_id: Some("base-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(format!("tx:{identity_suffix}")),
        log_index,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "base-mainnet",
            "block_hash": block_hash,
            "block_number": block_number,
            "transaction_hash": format!("tx:{identity_suffix}"),
            "log_index": log_index,
        }),
        derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state,
        after_state,
    }
}

fn basenames_resolver_event(
    binding: &IdentityBinding,
    identity_suffix: &str,
    resolver_address: &str,
    block_hash: &str,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("worker-test:{EVENT_KIND_RESOLVER_CHANGED}:{identity_suffix}"),
        namespace: BASENAMES_NAMESPACE.to_owned(),
        logical_name_id: Some(binding.logical_name_id.clone()),
        resource_id: Some(binding.resource_id),
        event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
        source_family: SOURCE_FAMILY_BASENAMES_BASE_RESOLVER.to_owned(),
        manifest_version: 4,
        source_manifest_id: None,
        chain_id: Some("base-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(format!("tx:{identity_suffix}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "base-mainnet",
            "block_hash": block_hash,
            "block_number": block_number,
            "transaction_hash": format!("tx:{identity_suffix}"),
            "log_index": log_index,
        }),
        derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "resolver": resolver_address,
            "namehash": format!("namehash:{}", binding.display_name),
        }),
    }
}

fn basenames_record_version_event(
    binding: &IdentityBinding,
    identity_suffix: &str,
    block_hash: &str,
    block_number: i64,
    log_index: i64,
    record_version: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!(
            "worker-test:{EVENT_KIND_RECORD_VERSION_CHANGED}:{identity_suffix}"
        ),
        namespace: BASENAMES_NAMESPACE.to_owned(),
        logical_name_id: Some(binding.logical_name_id.clone()),
        resource_id: Some(binding.resource_id),
        event_kind: EVENT_KIND_RECORD_VERSION_CHANGED.to_owned(),
        source_family: SOURCE_FAMILY_BASENAMES_BASE_RESOLVER.to_owned(),
        manifest_version: 4,
        source_manifest_id: None,
        chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(format!("tx:{identity_suffix}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": BASE_MAINNET_CHAIN_ID,
            "block_hash": block_hash,
            "block_number": block_number,
            "transaction_hash": format!("tx:{identity_suffix}"),
            "log_index": log_index,
        }),
        derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({
            "record_version": record_version - 1,
        }),
        after_state: json!({
            "record_version": record_version,
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn history_event(
    identity_suffix: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    chain_id: Option<&str>,
    block_number: Option<i64>,
    block_hash: Option<&str>,
    transaction_hash: Option<&str>,
    log_index: Option<i64>,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("worker-test:history:{identity_suffix}"),
        namespace: "ens".to_owned(),
        logical_name_id: logical_name_id.map(str::to_owned),
        resource_id,
        event_kind: "HistoryEvent".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 5,
        source_manifest_id: None,
        chain_id: chain_id.map(str::to_owned),
        block_number,
        block_hash: block_hash.map(str::to_owned),
        transaction_hash: transaction_hash.map(str::to_owned),
        log_index,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "event_identity": identity_suffix,
            "transaction_hash": transaction_hash,
        }),
        derivation_kind: "history_test".to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({}),
    }
}

fn timestamp(value: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(value).expect("timestamp must be valid")
}
