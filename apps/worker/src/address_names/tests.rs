use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigname_storage::{
    AddressNameRelation, CanonicalityState, NameSurface, NormalizedEvent, RawBlock, Resource,
    SurfaceBinding, SurfaceBindingKind, TokenLineage, default_database_url,
    label_preimage_from_label, load_address_names_current, upsert_name_surfaces,
    upsert_normalized_events, upsert_raw_blocks, upsert_resources, upsert_surface_bindings,
    upsert_token_lineages,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};
use uuid::Uuid;

use super::{
    constants::{
        ADDRESS_NAMES_CURRENT_DERIVATION_KIND, ADDRESS_NAMES_ENUMERATION_BASIS,
        BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY, BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
        ENS_V1_AUTHORITY_DERIVATION_KIND, ENS_V1_REGISTRAR_SOURCE_FAMILY,
        ENS_V1_REGISTRY_SOURCE_FAMILY, ENS_V2_REGISTRY_DERIVATION_KIND,
        ENS_V2_REGISTRY_SOURCE_FAMILY,
    },
    rebuild_address_names_current,
};

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
            .context("failed to parse database URL for worker address_names tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_worker_address_names_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for worker address_names tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect worker address_names test pool")?;

        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for worker address_names tests")?;

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
async fn rebuilds_current_token_holder_and_registry_controller_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let tokenized =
        IdentityBinding::new("ens:alpha.eth", "alpha.eth", Some(0x1100), 0x2200, 0x3300);
    let registry_only = IdentityBinding::new("ens:beta.eth", "beta.eth", None, 0x4400, 0x5500);

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xalpha-grant", 100, 1_717_180_100),
            raw_block("ethereum-mainnet", "0xalpha-transfer", 101, 1_717_180_101),
            raw_block("ethereum-mainnet", "0xbeta-control", 102, 1_717_180_102),
        ],
    )
    .await?;
    seed_identity(
        database.pool(),
        &tokenized,
        "0xalpha-grant",
        100,
        1_717_180_100,
    )
    .await?;
    seed_identity(
        database.pool(),
        &registry_only,
        "0xbeta-control",
        102,
        1_717_180_102,
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &tokenized,
                "grant",
                "RegistrationGranted",
                ENS_V1_REGISTRAR_SOURCE_FAMILY,
                "0xalpha-grant",
                100,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:alpha",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                }),
            ),
            authority_event(
                &tokenized,
                "transfer",
                "TokenControlTransferred",
                ENS_V1_REGISTRAR_SOURCE_FAMILY,
                "0xalpha-transfer",
                101,
                Some(0),
                json!({
                    "from": "0x0000000000000000000000000000000000000aaa",
                }),
                json!({
                    "to": "0x0000000000000000000000000000000000000bbb",
                }),
            ),
            authority_event(
                &registry_only,
                "epoch",
                "AuthorityEpochChanged",
                ENS_V1_REGISTRY_SOURCE_FAMILY,
                "0xbeta-control",
                102,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registry_only",
                    "authority_key": "registry:ethereum-mainnet:beta",
                }),
            ),
            authority_event(
                &registry_only,
                "owner",
                "AuthorityTransferred",
                ENS_V1_REGISTRY_SOURCE_FAMILY,
                "0xbeta-control",
                102,
                Some(1),
                json!({
                    "owner": "0x0000000000000000000000000000000000000aaa",
                }),
                json!({
                    "owner": "0x0000000000000000000000000000000000000ccc",
                }),
            ),
        ],
    )
    .await?;

    let summary = rebuild_address_names_current(database.pool(), None).await?;
    assert_eq!(summary.requested_address_count, 2);
    assert_eq!(summary.upserted_row_count, 4);

    let token_rows = load_address_names_current(
        database.pool(),
        "0x0000000000000000000000000000000000000bbb",
        None,
        None,
    )
    .await?;
    assert_eq!(token_rows.len(), 3);
    assert_eq!(
        token_rows
            .iter()
            .map(|row| row.relation)
            .collect::<Vec<_>>(),
        vec![
            AddressNameRelation::Registrant,
            AddressNameRelation::TokenHolder,
            AddressNameRelation::EffectiveController,
        ]
    );
    assert!(
        token_rows
            .iter()
            .all(|row| row.logical_name_id == "ens:alpha.eth")
    );
    assert!(
        token_rows
            .iter()
            .all(|row| row.token_lineage_id == tokenized.token_lineage_id)
    );
    assert!(
        token_rows
            .iter()
            .all(|row| row.provenance["derivation_kind"]
                == Value::String(ADDRESS_NAMES_CURRENT_DERIVATION_KIND.to_owned()))
    );
    assert!(
        token_rows
            .iter()
            .all(|row| row.coverage["enumeration_basis"]
                == Value::String(ADDRESS_NAMES_ENUMERATION_BASIS.to_owned()))
    );

    let controller_rows = load_address_names_current(
        database.pool(),
        "0x0000000000000000000000000000000000000ccc",
        None,
        None,
    )
    .await?;
    assert_eq!(controller_rows.len(), 1);
    assert_eq!(
        controller_rows[0].relation,
        AddressNameRelation::EffectiveController
    );
    assert_eq!(controller_rows[0].logical_name_id, "ens:beta.eth");
    assert_eq!(controller_rows[0].token_lineage_id, None);

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_uses_resource_permission_subject_as_tokenized_effective_controller() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new(
        "ens:manager.eth",
        "manager.eth",
        Some(0x9100),
        0x9200,
        0x9300,
    );
    let token_holder = "0x0000000000000000000000000000000000000bbb";
    let controller = "0x0000000000000000000000000000000000000ccc";
    let old_token_lineage_id = Uuid::from_u128(0x9101);
    let old_resource_id = Uuid::from_u128(0x9201);

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xmanager-grant", 500, 1_717_180_500),
            raw_block("ethereum-mainnet", "0xmanager-control", 501, 1_717_180_501),
            raw_block(
                "ethereum-mainnet",
                "0xmanager-old-control",
                502,
                1_717_180_502,
            ),
            raw_block("ethereum-mainnet", "0xmanager-revoke", 503, 1_717_180_503),
        ],
    )
    .await?;
    seed_identity(
        database.pool(),
        &binding,
        "0xmanager-grant",
        500,
        1_717_180_500,
    )
    .await?;
    upsert_token_lineages(
        database.pool(),
        &[token_lineage(
            &binding,
            old_token_lineage_id,
            "0xmanager-old-control",
            502,
        )],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[resource(
            &binding,
            old_resource_id,
            Some(old_token_lineage_id),
            "0xmanager-old-control",
            502,
        )],
    )
    .await?;
    let mut old_resource_control = authority_event(
        &binding,
        "manager-old-resource-control",
        "PermissionChanged",
        ENS_V1_REGISTRY_SOURCE_FAMILY,
        "0xmanager-old-control",
        502,
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
    );
    old_resource_control.resource_id = Some(old_resource_id);
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "manager-grant",
                "RegistrationGranted",
                ENS_V1_REGISTRAR_SOURCE_FAMILY,
                "0xmanager-grant",
                500,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:manager",
                    "registrant": token_holder,
                }),
            ),
            authority_event(
                &binding,
                "manager-resource-control",
                "PermissionChanged",
                ENS_V1_REGISTRY_SOURCE_FAMILY,
                "0xmanager-control",
                501,
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
            old_resource_control,
        ],
    )
    .await?;

    let controller_summary =
        rebuild_address_names_current(database.pool(), Some(controller)).await?;
    assert_eq!(controller_summary.upserted_row_count, 1);
    let controller_rows =
        load_address_names_current(database.pool(), controller, Some("ens"), None).await?;
    assert_eq!(controller_rows.len(), 1);
    assert_eq!(
        controller_rows[0].relation,
        AddressNameRelation::EffectiveController
    );
    assert_eq!(controller_rows[0].logical_name_id, binding.logical_name_id);

    let holder_summary = rebuild_address_names_current(database.pool(), Some(token_holder)).await?;
    assert_eq!(holder_summary.upserted_row_count, 2);
    let holder_rows =
        load_address_names_current(database.pool(), token_holder, Some("ens"), None).await?;
    assert_eq!(
        holder_rows
            .iter()
            .map(|row| row.relation)
            .collect::<Vec<_>>(),
        vec![
            AddressNameRelation::Registrant,
            AddressNameRelation::TokenHolder,
        ]
    );

    seed_events(
        database.pool(),
        &[authority_event(
            &binding,
            "manager-resource-control-revoked",
            "PermissionChanged",
            ENS_V1_REGISTRY_SOURCE_FAMILY,
            "0xmanager-revoke",
            503,
            Some(0),
            json!({
                "scope": {"kind": "resource"},
                "subject": controller,
                "effective_powers": ["resource_control"],
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
        )],
    )
    .await?;

    let controller_summary =
        rebuild_address_names_current(database.pool(), Some(controller)).await?;
    assert_eq!(controller_summary.upserted_row_count, 0);
    assert_eq!(controller_summary.deleted_row_count, 1);
    let controller_rows =
        load_address_names_current(database.pool(), controller, Some("ens"), None).await?;
    assert!(controller_rows.is_empty());

    let holder_summary = rebuild_address_names_current(database.pool(), Some(token_holder)).await?;
    assert_eq!(holder_summary.upserted_row_count, 3);
    let holder_rows =
        load_address_names_current(database.pool(), token_holder, Some("ens"), None).await?;
    assert_eq!(
        holder_rows
            .iter()
            .map(|row| row.relation)
            .collect::<Vec<_>>(),
        vec![
            AddressNameRelation::Registrant,
            AddressNameRelation::TokenHolder,
            AddressNameRelation::EffectiveController,
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn wrapper_fuse_burn_drops_address_names_effective_controller() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new(
        "ens:fused-controller.eth",
        "fused-controller.eth",
        Some(0x9110),
        0x9210,
        0x9310,
    );
    let token_holder = "0x0000000000000000000000000000000000000bbb";
    let controller = "0x0000000000000000000000000000000000000ccc";

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xfused-grant", 510, 1_717_180_510),
            raw_block("ethereum-mainnet", "0xfused-control", 511, 1_717_180_511),
            raw_block("ethereum-mainnet", "0xfused-scope", 512, 1_717_180_512),
        ],
    )
    .await?;
    seed_identity(
        database.pool(),
        &binding,
        "0xfused-grant",
        510,
        1_717_180_510,
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &binding,
                "fused-grant",
                "RegistrationGranted",
                ENS_V1_REGISTRAR_SOURCE_FAMILY,
                "0xfused-grant",
                510,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:ethereum-mainnet:7:fused-controller",
                    "registrant": token_holder,
                }),
            ),
            authority_event(
                &binding,
                "fused-resource-control",
                "PermissionChanged",
                ENS_V1_REGISTRY_SOURCE_FAMILY,
                "0xfused-control",
                511,
                Some(0),
                json!({}),
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
        ],
    )
    .await?;

    let controller_summary =
        rebuild_address_names_current(database.pool(), Some(controller)).await?;
    assert_eq!(controller_summary.upserted_row_count, 1);
    let controller_rows =
        load_address_names_current(database.pool(), controller, Some("ens"), None).await?;
    assert_eq!(controller_rows.len(), 1);
    assert_eq!(
        controller_rows[0].relation,
        AddressNameRelation::EffectiveController
    );

    seed_events(
        database.pool(),
        &[authority_event(
            &binding,
            "fused-resource-control-masked",
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            "0xfused-scope",
            512,
            Some(0),
            json!({}),
            json!({
                "scope": {"kind": "resource"},
                "fuses": 8,
                "namehash": "0xwrapped",
                "authority_kind": "name_wrapper",
                "authority_key": "wrapped"
            }),
        )],
    )
    .await?;

    let controller_summary =
        rebuild_address_names_current(database.pool(), Some(controller)).await?;
    assert_eq!(controller_summary.upserted_row_count, 0);
    assert_eq!(controller_summary.deleted_row_count, 1);
    let controller_rows =
        load_address_names_current(database.pool(), controller, Some("ens"), None).await?;
    assert!(
        controller_rows.is_empty(),
        "address_names_current must not keep effective-controller rows masked by wrapper fuses"
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_one_address_refreshes_deleted_and_new_relation_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::new("ens:alpha.eth", "alpha.eth", Some(0x6100), 0x6200, 0x6300);
    let old_holder = "0x0000000000000000000000000000000000000aaa";
    let new_holder = "0x0000000000000000000000000000000000000bbb";

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xgrant", 200, 1_717_180_200),
            raw_block("ethereum-mainnet", "0xtransfer", 201, 1_717_180_201),
        ],
    )
    .await?;
    seed_identity(database.pool(), &binding, "0xgrant", 200, 1_717_180_200).await?;
    seed_events(
        database.pool(),
        &[authority_event(
            &binding,
            "grant",
            "RegistrationGranted",
            ENS_V1_REGISTRAR_SOURCE_FAMILY,
            "0xgrant",
            200,
            Some(0),
            json!({}),
            json!({
                "authority_kind": "registrar",
                "authority_key": "registrar:ethereum-mainnet:7:alpha",
                "registrant": old_holder,
            }),
        )],
    )
    .await?;

    rebuild_address_names_current(database.pool(), None).await?;
    assert_eq!(
        load_address_names_current(database.pool(), old_holder, None, None)
            .await?
            .len(),
        3
    );

    seed_events(
        database.pool(),
        &[authority_event(
            &binding,
            "transfer",
            "TokenControlTransferred",
            ENS_V1_REGISTRAR_SOURCE_FAMILY,
            "0xtransfer",
            201,
            Some(0),
            json!({
                "from": old_holder,
            }),
            json!({
                "to": new_holder,
            }),
        )],
    )
    .await?;

    let old_summary = rebuild_address_names_current(database.pool(), Some(old_holder)).await?;
    assert_eq!(old_summary.deleted_row_count, 3);
    assert_eq!(old_summary.upserted_row_count, 0);
    assert!(
        load_address_names_current(database.pool(), old_holder, None, None)
            .await?
            .is_empty()
    );

    let new_summary = rebuild_address_names_current(database.pool(), Some(new_holder)).await?;
    assert_eq!(new_summary.upserted_row_count, 3);
    let new_rows = load_address_names_current(database.pool(), new_holder, None, None).await?;
    assert_eq!(new_rows.len(), 3);
    assert!(
        new_rows
            .iter()
            .all(|row| row.logical_name_id == binding.logical_name_id)
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuilds_basenames_base_authority_rows_without_leaking_ignored_state() -> Result<()> {
    let database = TestDatabase::new().await?;
    let tokenized = IdentityBinding::with_namespace_and_chain(
        "basenames",
        "base-mainnet",
        "basenames:alice.base.eth",
        "alice.base.eth",
        Some(0x7100),
        0x7200,
        0x7300,
    );
    let registry_only = IdentityBinding::with_namespace_and_chain(
        "basenames",
        "base-mainnet",
        "basenames:beta.base.eth",
        "beta.base.eth",
        None,
        0x7400,
        0x7500,
    );

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("base-mainnet", "0xbase-grant", 400, 1_717_180_400),
            raw_block("base-mainnet", "0xbase-transfer", 401, 1_717_180_401),
            raw_block("base-mainnet", "0xbase-registry", 402, 1_717_180_402),
            raw_block("base-mainnet", "0xbase-ignored", 403, 1_717_180_403),
        ],
    )
    .await?;
    seed_identity(
        database.pool(),
        &tokenized,
        "0xbase-grant",
        400,
        1_717_180_400,
    )
    .await?;
    seed_identity(
        database.pool(),
        &registry_only,
        "0xbase-registry",
        402,
        1_717_180_402,
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &tokenized,
                "grant",
                "RegistrationGranted",
                BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                "0xbase-grant",
                400,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:base-mainnet:alice",
                    "registrant": "0x0000000000000000000000000000000000000aaa",
                }),
            ),
            authority_event(
                &tokenized,
                "transfer",
                "TokenControlTransferred",
                BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                "0xbase-transfer",
                401,
                Some(0),
                json!({
                    "from": "0x0000000000000000000000000000000000000aaa",
                }),
                json!({
                    "to": "0x0000000000000000000000000000000000000bbb",
                }),
            ),
            ignored_event(
                &tokenized,
                "reverse-claim",
                "ens_v1_reverse_claim",
                "ignored_projection_state",
                "AuthorityTransferred",
                "0xbase-ignored",
                403,
                Some(0),
                json!({
                    "owner": "0x0000000000000000000000000000000000000ddd",
                }),
            ),
            ignored_event(
                &tokenized,
                "transport",
                "basenames_l1_compat_projection",
                "ignored_projection_state",
                "AuthorityTransferred",
                "0xbase-ignored",
                403,
                Some(1),
                json!({
                    "owner": "0x0000000000000000000000000000000000000eee",
                }),
            ),
            authority_event(
                &registry_only,
                "epoch",
                "AuthorityEpochChanged",
                BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                "0xbase-registry",
                402,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registry_only",
                    "authority_key": "registry:base-mainnet:beta",
                }),
            ),
            authority_event(
                &registry_only,
                "owner",
                "AuthorityTransferred",
                BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                "0xbase-registry",
                402,
                Some(1),
                json!({
                    "owner": "0x0000000000000000000000000000000000000aaa",
                }),
                json!({
                    "owner": "0x0000000000000000000000000000000000000ccc",
                }),
            ),
            ignored_event(
                &registry_only,
                "primary-family-reuse",
                ENS_V1_AUTHORITY_DERIVATION_KIND,
                "basenames_base_primary",
                "AuthorityTransferred",
                "0xbase-ignored",
                403,
                Some(2),
                json!({
                    "owner": "0x0000000000000000000000000000000000000ddd",
                }),
            ),
        ],
    )
    .await?;

    let summary = rebuild_address_names_current(database.pool(), None).await?;
    assert_eq!(summary.requested_address_count, 2);
    assert_eq!(summary.upserted_row_count, 4);

    let token_rows = load_address_names_current(
        database.pool(),
        "0x0000000000000000000000000000000000000bbb",
        Some("basenames"),
        None,
    )
    .await?;
    assert_eq!(token_rows.len(), 3);
    assert!(
        token_rows
            .iter()
            .all(|row| row.logical_name_id == tokenized.logical_name_id)
    );
    assert!(
        token_rows
            .iter()
            .all(|row| row.chain_positions.get("base").is_some())
    );
    assert!(token_rows.iter().all(|row| {
        row.provenance["normalized_event_ids"]
            .as_array()
            .is_some_and(|values| values.len() == 2)
    }));
    assert!(token_rows.iter().all(|row| {
        row.provenance["raw_fact_refs"]
            .as_array()
            .is_some_and(|values| values.len() == 2)
    }));

    let controller_rows = load_address_names_current(
        database.pool(),
        "0x0000000000000000000000000000000000000ccc",
        Some("basenames"),
        None,
    )
    .await?;
    assert_eq!(controller_rows.len(), 1);
    assert_eq!(
        controller_rows[0].relation,
        AddressNameRelation::EffectiveController
    );
    assert_eq!(
        controller_rows[0].logical_name_id,
        registry_only.logical_name_id
    );
    assert_eq!(controller_rows[0].token_lineage_id, None);

    database.cleanup().await
}

#[tokio::test]
async fn rebuilds_basenames_base_authority_control_vector_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let nft_only = IdentityBinding::with_namespace_and_chain(
        "basenames",
        "base-mainnet",
        "basenames:nft-only.base.eth",
        "nft-only.base.eth",
        Some(0x7600),
        0x7601,
        0x7602,
    );
    let management_only = IdentityBinding::with_namespace_and_chain(
        "basenames",
        "base-mainnet",
        "basenames:management-only.base.eth",
        "management-only.base.eth",
        Some(0x7610),
        0x7611,
        0x7612,
    );
    let full_transfer = IdentityBinding::with_namespace_and_chain(
        "basenames",
        "base-mainnet",
        "basenames:full-transfer.base.eth",
        "full-transfer.base.eth",
        Some(0x7620),
        0x7621,
        0x7622,
    );

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("base-mainnet", "0xnft-grant", 500, 1_717_181_500),
            raw_block("base-mainnet", "0xnft-manager", 501, 1_717_181_501),
            raw_block("base-mainnet", "0xnft-transfer", 502, 1_717_181_502),
            raw_block("base-mainnet", "0xmgmt-grant", 510, 1_717_181_510),
            raw_block("base-mainnet", "0xmgmt-manager", 511, 1_717_181_511),
            raw_block("base-mainnet", "0xfull-grant", 520, 1_717_181_520),
            raw_block("base-mainnet", "0xfull-manager", 521, 1_717_181_521),
            raw_block("base-mainnet", "0xfull-transfer", 522, 1_717_181_522),
            raw_block("base-mainnet", "0xfull-manager-final", 523, 1_717_181_523),
        ],
    )
    .await?;
    seed_identity(
        database.pool(),
        &nft_only,
        "0xnft-grant",
        500,
        1_717_181_500,
    )
    .await?;
    seed_identity(
        database.pool(),
        &management_only,
        "0xmgmt-grant",
        510,
        1_717_181_510,
    )
    .await?;
    seed_identity(
        database.pool(),
        &full_transfer,
        "0xfull-grant",
        520,
        1_717_181_520,
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            authority_event(
                &nft_only,
                "nft-grant",
                "RegistrationGranted",
                BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                "0xnft-grant",
                500,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:base-mainnet:nft-only",
                    "registrant": "0x00000000000000000000000000000000000000a1",
                }),
            ),
            authority_event(
                &nft_only,
                "nft-manager",
                "AuthorityTransferred",
                BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                "0xnft-manager",
                501,
                Some(0),
                json!({
                    "owner": "0x00000000000000000000000000000000000000a1",
                }),
                json!({
                    "owner": "0x00000000000000000000000000000000000000b1",
                }),
            ),
            authority_event(
                &nft_only,
                "nft-transfer",
                "TokenControlTransferred",
                BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                "0xnft-transfer",
                502,
                Some(0),
                json!({
                    "from": "0x00000000000000000000000000000000000000a1",
                }),
                json!({
                    "to": "0x00000000000000000000000000000000000000c1",
                }),
            ),
            authority_event(
                &management_only,
                "mgmt-grant",
                "RegistrationGranted",
                BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                "0xmgmt-grant",
                510,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:base-mainnet:management-only",
                    "registrant": "0x00000000000000000000000000000000000000a2",
                }),
            ),
            authority_event(
                &management_only,
                "mgmt-manager",
                "AuthorityTransferred",
                BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                "0xmgmt-manager",
                511,
                Some(0),
                json!({
                    "owner": "0x00000000000000000000000000000000000000a2",
                }),
                json!({
                    "owner": "0x00000000000000000000000000000000000000b2",
                }),
            ),
            authority_event(
                &full_transfer,
                "full-grant",
                "RegistrationGranted",
                BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                "0xfull-grant",
                520,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:base-mainnet:full-transfer",
                    "registrant": "0x00000000000000000000000000000000000000a3",
                }),
            ),
            authority_event(
                &full_transfer,
                "full-manager",
                "AuthorityTransferred",
                BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                "0xfull-manager",
                521,
                Some(0),
                json!({
                    "owner": "0x00000000000000000000000000000000000000a3",
                }),
                json!({
                    "owner": "0x00000000000000000000000000000000000000b3",
                }),
            ),
            authority_event(
                &full_transfer,
                "full-transfer",
                "TokenControlTransferred",
                BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
                "0xfull-transfer",
                522,
                Some(0),
                json!({
                    "from": "0x00000000000000000000000000000000000000a3",
                }),
                json!({
                    "to": "0x00000000000000000000000000000000000000c3",
                }),
            ),
            authority_event(
                &full_transfer,
                "full-manager-final",
                "AuthorityTransferred",
                BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                "0xfull-manager-final",
                523,
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

    let summary = rebuild_address_names_current(database.pool(), None).await?;
    assert_eq!(summary.requested_address_count, 5);
    assert_eq!(summary.upserted_row_count, 9);

    let nft_holder_rows = load_address_names_current(
        database.pool(),
        "0x00000000000000000000000000000000000000c1",
        Some("basenames"),
        None,
    )
    .await?;
    assert_eq!(nft_holder_rows.len(), 2);
    assert_eq!(
        nft_holder_rows
            .iter()
            .map(|row| row.relation)
            .collect::<Vec<_>>(),
        vec![
            AddressNameRelation::Registrant,
            AddressNameRelation::TokenHolder,
        ]
    );
    assert!(
        nft_holder_rows
            .iter()
            .all(|row| row.logical_name_id == nft_only.logical_name_id)
    );
    let nft_controller_rows = load_address_names_current(
        database.pool(),
        "0x00000000000000000000000000000000000000b1",
        Some("basenames"),
        None,
    )
    .await?;
    assert_eq!(nft_controller_rows.len(), 1);
    assert_eq!(
        nft_controller_rows[0].relation,
        AddressNameRelation::EffectiveController
    );
    assert_eq!(
        nft_controller_rows[0].logical_name_id,
        nft_only.logical_name_id
    );

    let management_holder_rows = load_address_names_current(
        database.pool(),
        "0x00000000000000000000000000000000000000a2",
        Some("basenames"),
        None,
    )
    .await?;
    assert_eq!(management_holder_rows.len(), 2);
    assert_eq!(
        management_holder_rows
            .iter()
            .map(|row| row.relation)
            .collect::<Vec<_>>(),
        vec![
            AddressNameRelation::Registrant,
            AddressNameRelation::TokenHolder,
        ]
    );
    assert!(
        management_holder_rows
            .iter()
            .all(|row| row.logical_name_id == management_only.logical_name_id)
    );
    let management_controller_rows = load_address_names_current(
        database.pool(),
        "0x00000000000000000000000000000000000000b2",
        Some("basenames"),
        None,
    )
    .await?;
    assert_eq!(management_controller_rows.len(), 1);
    assert_eq!(
        management_controller_rows[0].relation,
        AddressNameRelation::EffectiveController
    );
    assert_eq!(
        management_controller_rows[0].logical_name_id,
        management_only.logical_name_id
    );

    let full_rows = load_address_names_current(
        database.pool(),
        "0x00000000000000000000000000000000000000c3",
        Some("basenames"),
        None,
    )
    .await?;
    assert_eq!(full_rows.len(), 3);
    assert_eq!(
        full_rows.iter().map(|row| row.relation).collect::<Vec<_>>(),
        vec![
            AddressNameRelation::Registrant,
            AddressNameRelation::TokenHolder,
            AddressNameRelation::EffectiveController,
        ]
    );
    assert!(
        full_rows
            .iter()
            .all(|row| row.logical_name_id == full_transfer.logical_name_id)
    );
    assert!(
        load_address_names_current(
            database.pool(),
            "0x00000000000000000000000000000000000000b3",
            Some("basenames"),
            None,
        )
        .await?
        .is_empty()
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuilds_ensv2_address_names_from_registry_resource_surface_events() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding = IdentityBinding::with_namespace_and_chain(
        "ens",
        "ethereum-sepolia",
        "ens:bob.alice.eth",
        "bob.alice.eth",
        Some(0x9600),
        0x9700,
        0x9800,
    );
    let holder = "0x0000000000000000000000000000000000000b0b";
    let controller = "0x0000000000000000000000000000000000000c0c";

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-sepolia", "0xensv2-link", 700, 1_717_182_700),
            raw_block("ethereum-sepolia", "0xensv2-regen", 701, 1_717_182_701),
        ],
    )
    .await?;
    seed_identity(
        database.pool(),
        &binding,
        "0xensv2-link",
        700,
        1_717_182_700,
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            ensv2_registry_event(
                &binding,
                "grant",
                "RegistrationGranted",
                "0xensv2-link",
                700,
                Some(0),
                json!({}),
                json!({
                    "authority_kind": "ens_v2_registry",
                    "authority_key": "ens-v2-registry:ethereum-sepolia:user-registry:0xeac",
                    "registrant": holder,
                    "expiry": 1_900_000_000_i64,
                    "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                }),
            ),
            ensv2_registry_event(
                &binding,
                "controller",
                "AuthorityTransferred",
                "0xensv2-link",
                700,
                Some(1),
                json!({
                    "owner": holder,
                }),
                json!({
                    "owner": controller,
                    "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                }),
            ),
            ensv2_registry_event(
                &binding,
                "regen",
                "TokenRegenerated",
                "0xensv2-regen",
                701,
                Some(0),
                json!({
                    "token_id": "0x01",
                }),
                json!({
                    "old_token_id": "0x01",
                    "new_token_id": "0x02",
                    "resource_id": binding.resource_id.to_string(),
                }),
            ),
        ],
    )
    .await?;

    let summary = rebuild_address_names_current(database.pool(), None).await?;
    assert_eq!(summary.requested_address_count, 2);
    assert_eq!(summary.upserted_row_count, 3);

    let holder_rows =
        load_address_names_current(database.pool(), holder, Some("ens"), None).await?;
    assert_eq!(holder_rows.len(), 2);
    assert_eq!(
        holder_rows
            .iter()
            .map(|row| row.relation)
            .collect::<Vec<_>>(),
        vec![
            AddressNameRelation::Registrant,
            AddressNameRelation::TokenHolder,
        ]
    );
    assert!(
        holder_rows
            .iter()
            .all(|row| row.logical_name_id == binding.logical_name_id)
    );
    assert!(
        holder_rows
            .iter()
            .all(|row| row.resource_id == binding.resource_id)
    );
    assert!(holder_rows.iter().all(|row| {
        row.coverage["source_classes_considered"] == json!(["ensv2_registry_resource_surface"])
    }));
    assert!(holder_rows.iter().all(|row| {
        row.provenance["normalized_event_ids"]
            .as_array()
            .is_some_and(|values| values.len() == 3)
    }));
    assert!(
        holder_rows
            .iter()
            .all(|row| row.chain_positions["ethereum"]["chain_id"] == json!("ethereum-sepolia"))
    );

    let controller_rows =
        load_address_names_current(database.pool(), controller, Some("ens"), None).await?;
    assert_eq!(controller_rows.len(), 1);
    assert_eq!(
        controller_rows[0].relation,
        AddressNameRelation::EffectiveController
    );
    assert_eq!(controller_rows[0].logical_name_id, binding.logical_name_id);

    database.cleanup().await
}

#[derive(Clone, Debug)]
struct IdentityBinding {
    namespace: String,
    chain_id: String,
    logical_name_id: String,
    display_name: String,
    token_lineage_id: Option<Uuid>,
    resource_id: Uuid,
    surface_binding_id: Uuid,
}

impl IdentityBinding {
    fn new(
        logical_name_id: &str,
        display_name: &str,
        token_lineage: Option<u128>,
        resource: u128,
        binding: u128,
    ) -> Self {
        Self::with_namespace_and_chain(
            "ens",
            "ethereum-mainnet",
            logical_name_id,
            display_name,
            token_lineage,
            resource,
            binding,
        )
    }

    fn with_namespace_and_chain(
        namespace: &str,
        chain_id: &str,
        logical_name_id: &str,
        display_name: &str,
        token_lineage: Option<u128>,
        resource: u128,
        binding: u128,
    ) -> Self {
        Self {
            namespace: namespace.to_owned(),
            chain_id: chain_id.to_owned(),
            logical_name_id: logical_name_id.to_owned(),
            display_name: display_name.to_owned(),
            token_lineage_id: token_lineage.map(Uuid::from_u128),
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
    if let Some(token_lineage_id) = binding.token_lineage_id {
        upsert_token_lineages(
            pool,
            &[token_lineage(
                binding,
                token_lineage_id,
                block_hash,
                block_number,
            )],
        )
        .await?;
    }
    upsert_resources(
        pool,
        &[resource(
            binding,
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
            binding,
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
            block_hash,
            block_number,
        )],
    )
    .await?;
    Ok(())
}

async fn seed_raw_blocks(pool: &PgPool, blocks: &[RawBlock]) -> Result<()> {
    upsert_raw_blocks(pool, blocks).await?;
    Ok(())
}

async fn seed_events(pool: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
    upsert_normalized_events(pool, events).await?;
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

fn token_lineage(
    binding: &IdentityBinding,
    token_lineage_id: Uuid,
    block_hash: &str,
    block_number: i64,
) -> TokenLineage {
    TokenLineage {
        token_lineage_id,
        chain_id: binding.chain_id.clone(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"source": "worker_address_names_test", "kind": "token_lineage"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn resource(
    binding: &IdentityBinding,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    block_hash: &str,
    block_number: i64,
) -> Resource {
    Resource {
        resource_id,
        token_lineage_id,
        chain_id: binding.chain_id.clone(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"source": "worker_address_names_test", "kind": "resource"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn name_surface(
    binding: &IdentityBinding,
    logical_name_id: &str,
    display_name: &str,
    block_hash: &str,
    block_number: i64,
) -> NameSurface {
    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: binding.namespace.clone(),
        input_name: display_name.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        dns_encoded_name: display_name.as_bytes().to_vec(),
        namehash: format!("namehash:{display_name}"),
        labelhashes: labelhashes_for_name(display_name),
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: binding.chain_id.clone(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"source": "worker_address_names_test", "kind": "name_surface"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn labelhashes_for_name(name: &str) -> Vec<String> {
    name.split('.').map(labelhash_for_label).collect()
}

fn labelhash_for_label(label: &str) -> String {
    label_preimage_from_label(label, "worker_address_names_test", 1, json!({}))
        .expect("test label must hash")
        .labelhash
}

fn surface_binding(
    binding: &IdentityBinding,
    active_from_unix: i64,
    block_hash: &str,
    block_number: i64,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id: binding.surface_binding_id,
        logical_name_id: binding.logical_name_id.clone(),
        resource_id: binding.resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: timestamp(active_from_unix),
        active_to: None,
        chain_id: binding.chain_id.clone(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"source": "worker_address_names_test", "kind": "surface_binding"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

#[allow(clippy::too_many_arguments)]
fn authority_event(
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
        event_identity: format!("worker-address-names:{event_kind}:{identity_suffix}"),
        namespace: binding.namespace.clone(),
        logical_name_id: Some(binding.logical_name_id.clone()),
        resource_id: Some(binding.resource_id),
        event_kind: event_kind.to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: 3,
        source_manifest_id: None,
        chain_id: Some(binding.chain_id.clone()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(format!("tx:{identity_suffix}")),
        log_index,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": binding.chain_id,
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
fn ensv2_registry_event(
    binding: &IdentityBinding,
    identity_suffix: &str,
    event_kind: &str,
    block_hash: &str,
    block_number: i64,
    log_index: Option<i64>,
    before_state: Value,
    after_state: Value,
) -> NormalizedEvent {
    let mut event = authority_event(
        binding,
        identity_suffix,
        event_kind,
        ENS_V2_REGISTRY_SOURCE_FAMILY,
        block_hash,
        block_number,
        log_index,
        before_state,
        after_state,
    );
    event.derivation_kind = ENS_V2_REGISTRY_DERIVATION_KIND.to_owned();
    event
}

#[allow(clippy::too_many_arguments)]
fn ignored_event(
    binding: &IdentityBinding,
    identity_suffix: &str,
    derivation_kind: &str,
    source_family: &str,
    event_kind: &str,
    block_hash: &str,
    block_number: i64,
    log_index: Option<i64>,
    after_state: Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("worker-address-names:ignored:{event_kind}:{identity_suffix}"),
        namespace: binding.namespace.clone(),
        logical_name_id: Some(binding.logical_name_id.clone()),
        resource_id: Some(binding.resource_id),
        event_kind: event_kind.to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: 3,
        source_manifest_id: None,
        chain_id: Some(binding.chain_id.clone()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(format!("tx:ignored:{identity_suffix}")),
        log_index,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": binding.chain_id,
            "block_hash": block_hash,
            "block_number": block_number,
            "transaction_hash": format!("tx:ignored:{identity_suffix}"),
            "log_index": log_index,
        }),
        derivation_kind: derivation_kind.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state,
    }
}

fn timestamp(value: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(value).expect("timestamp must be valid")
}
