use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde_json::{Map, Value as JsonValue, json};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};
use uuid::Uuid;

use super::*;
use crate::{
    CanonicalityState, NameCurrentRow, NameSurface, Resource, SurfaceBinding, SurfaceBindingKind,
    TokenLineage, default_database_url, upsert_name_current_rows, upsert_name_surfaces,
    upsert_resources, upsert_surface_bindings, upsert_token_lineages,
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
            .context("failed to parse database URL for address_names_current tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_addr_names_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for address_names_current tests")?;

        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            database_name
        ))
        .execute(&admin_pool)
        .await
        .with_context(|| format!("failed to drop stale test database {database_name}"))?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect address_names_current test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for address_names_current tests")?;

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

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

fn token_lineage(token_lineage_id: Uuid, canonicality_state: CanonicalityState) -> TokenLineage {
    TokenLineage {
        token_lineage_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0xlineage{}", token_lineage_id.simple()),
        block_number: 21_100_000,
        provenance: json!({"source": "address_names_current_test", "anchor": "token_lineage"}),
        canonicality_state,
    }
}

fn resource(
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    canonicality_state: CanonicalityState,
) -> Resource {
    Resource {
        resource_id,
        token_lineage_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0xresource{}", resource_id.simple()),
        block_number: 21_100_001,
        provenance: json!({"source": "address_names_current_test", "anchor": "resource"}),
        canonicality_state,
    }
}

fn name_surface(
    logical_name_id: &str,
    display_name: &str,
    canonicality_state: CanonicalityState,
) -> NameSurface {
    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        input_name: display_name.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        dns_encoded_name: display_name.as_bytes().to_vec(),
        namehash: format!("namehash:{display_name}"),
        labelhashes: vec![format!("labelhash:{display_name}")],
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0xsurface:{display_name}"),
        block_number: 21_100_002,
        provenance: json!({"source": "address_names_current_test", "anchor": "surface"}),
        canonicality_state,
    }
}

fn surface_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
    canonicality_state: CanonicalityState,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: timestamp(1_717_171_700),
        active_to: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0xbinding{}", surface_binding_id.simple()),
        block_number: 21_100_003,
        provenance: json!({"source": "address_names_current_test", "anchor": "binding"}),
        canonicality_state,
    }
}

async fn seed_relation_references(
    database: &TestDatabase,
    logical_name_id: &str,
    display_name: &str,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    surface_binding_id: Uuid,
    canonicality_state: CanonicalityState,
) -> Result<()> {
    if let Some(token_lineage_id) = token_lineage_id {
        upsert_token_lineages(
            database.pool(),
            &[token_lineage(token_lineage_id, canonicality_state)],
        )
        .await?;
    }
    upsert_resources(
        database.pool(),
        &[resource(resource_id, token_lineage_id, canonicality_state)],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            logical_name_id,
            display_name,
            canonicality_state,
        )],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[surface_binding(
            surface_binding_id,
            logical_name_id,
            resource_id,
            canonicality_state,
        )],
    )
    .await?;
    Ok(())
}

struct AddressNameCurrentRowSeed<'a> {
    address: &'a str,
    logical_name_id: &'a str,
    display_name: &'a str,
    relation: AddressNameRelation,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    manifest_version: i64,
}

fn address_name_current_row(seed: AddressNameCurrentRowSeed<'_>) -> AddressNameCurrentRow {
    AddressNameCurrentRow {
        address: seed.address.to_owned(),
        logical_name_id: seed.logical_name_id.to_owned(),
        relation: seed.relation,
        namespace: "ens".to_owned(),
        canonical_display_name: seed.display_name.to_owned(),
        normalized_name: seed.display_name.to_owned(),
        namehash: format!("namehash:{}", seed.display_name),
        surface_binding_id: seed.surface_binding_id,
        resource_id: seed.resource_id,
        token_lineage_id: seed.token_lineage_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        provenance: json!({
            "normalized_event_ids": [seed.manifest_version],
            "derivation_kind": "address_names_current_rebuild"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "enumeration_basis": "address_collection"
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_100_003,
                "block_hash": format!("0xbinding{}", seed.surface_binding_id.simple()),
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: seed.manifest_version,
        last_recomputed_at: timestamp(1_717_171_717 + seed.manifest_version),
    }
}

fn name_current_row(row: &AddressNameCurrentRow) -> NameCurrentRow {
    NameCurrentRow {
        logical_name_id: row.logical_name_id.clone(),
        namespace: row.namespace.clone(),
        canonical_display_name: row.canonical_display_name.clone(),
        normalized_name: row.normalized_name.clone(),
        namehash: row.namehash.clone(),
        surface_binding_id: Some(row.surface_binding_id),
        resource_id: Some(row.resource_id),
        token_lineage_id: row.token_lineage_id,
        binding_kind: Some(row.binding_kind),
        declared_summary: json!({
            "registration": {
                "status": "registered"
            }
        }),
        provenance: row.provenance.clone(),
        coverage: row.coverage.clone(),
        chain_positions: row.chain_positions.clone(),
        canonicality_summary: row.canonicality_summary.clone(),
        manifest_version: row.manifest_version,
        last_recomputed_at: row.last_recomputed_at,
    }
}

async fn identity_count(pool: &PgPool, address: &str, roles: &str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT total_count
        FROM address_names_current_identity_counts
        WHERE address = $1 AND roles = $2
        "#,
    )
    .bind(address)
    .bind(roles)
    .fetch_optional(pool)
    .await
    .map(|count| count.unwrap_or_default())
    .context("failed to load address_names_current identity count")
}

async fn identity_feed_count(pool: &PgPool, address: &str, roles: &str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM address_names_current_identity_feed
        WHERE address = $1 AND roles = $2
        "#,
    )
    .bind(address)
    .bind(roles)
    .fetch_one(pool)
    .await
    .context("failed to count address_names_current identity feed rows")
}

fn expected_summary(entries: &[AddressNameCurrentEntry]) -> AddressNamesCurrentSummary {
    AddressNamesCurrentSummary {
        grouped_entry_count: entries.len() as u64,
        provenance: AddressNamesCurrentProvenanceSummary {
            normalized_event_ids: collect_provenance_values(entries, "normalized_event_ids"),
            raw_fact_refs: collect_provenance_values(entries, "raw_fact_refs"),
            manifest_versions: collect_provenance_values(entries, "manifest_versions"),
            derivation_kind: entries
                .iter()
                .filter_map(|entry| {
                    entry
                        .provenance
                        .get("derivation_kind")
                        .and_then(JsonValue::as_str)
                        .map(str::to_owned)
                })
                .next(),
        },
        chain_positions: expected_chain_positions(entries),
        consistency: expected_consistency(entries).to_owned(),
        last_recomputed_at: entries.iter().map(|entry| entry.last_recomputed_at).max(),
    }
}

fn collect_provenance_values(entries: &[AddressNameCurrentEntry], key: &str) -> JsonValue {
    let mut deduped = Vec::new();
    for entry in entries {
        let Some(values) = entry.provenance.get(key).and_then(JsonValue::as_array) else {
            continue;
        };
        for value in values {
            if !deduped.contains(value) {
                deduped.push(value.clone());
            }
        }
    }
    JsonValue::Array(deduped)
}

fn expected_chain_positions(entries: &[AddressNameCurrentEntry]) -> JsonValue {
    let mut chain_positions = BTreeMap::<String, (i64, String, JsonValue)>::new();
    for entry in entries {
        let Some(position_values) = entry.chain_positions.as_object() else {
            continue;
        };
        for (slot, position_value) in position_values {
            let Some(block_number) = position_value
                .get("block_number")
                .and_then(JsonValue::as_i64)
            else {
                continue;
            };
            let Some(block_hash) = position_value
                .get("block_hash")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
            else {
                continue;
            };
            if position_value
                .get("chain_id")
                .and_then(JsonValue::as_str)
                .is_none()
                || position_value
                    .get("timestamp")
                    .and_then(JsonValue::as_str)
                    .is_none()
            {
                continue;
            }

            match chain_positions.get(slot) {
                Some((existing_block_number, existing_block_hash, _))
                    if *existing_block_number > block_number
                        || (*existing_block_number == block_number
                            && existing_block_hash >= &block_hash) => {}
                _ => {
                    chain_positions.insert(
                        slot.clone(),
                        (block_number, block_hash, position_value.clone()),
                    );
                }
            }
        }
    }

    JsonValue::Object(
        chain_positions
            .into_iter()
            .map(|(slot, (_, _, value))| (slot, value))
            .collect::<Map<_, _>>(),
    )
}

fn expected_consistency(entries: &[AddressNameCurrentEntry]) -> &'static str {
    let mut consistency = "finalized";
    let mut saw_any = false;

    for entry in entries {
        saw_any = true;
        match entry
            .canonicality_summary
            .get("status")
            .and_then(JsonValue::as_str)
        {
            Some("safe") => consistency = "safe",
            Some("finalized") => {}
            _ => return "head",
        }
    }

    if saw_any { consistency } else { "head" }
}

#[tokio::test]
async fn address_names_current_upsert_replaces_existing_relation_row() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let logical_name_id = "ens:alice.eth";
    let token_lineage_id = Uuid::from_u128(0x1001);
    let resource_id = Uuid::from_u128(0x2001);
    let surface_binding_id = Uuid::from_u128(0x3001);

    seed_relation_references(
        &database,
        logical_name_id,
        "alice.eth",
        resource_id,
        Some(token_lineage_id),
        surface_binding_id,
        CanonicalityState::Finalized,
    )
    .await?;

    let first = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id,
        display_name: "alice.eth",
        relation: AddressNameRelation::Registrant,
        surface_binding_id,
        resource_id,
        token_lineage_id: Some(token_lineage_id),
        manifest_version: 1,
    });
    upsert_address_names_current_rows(database.pool(), std::slice::from_ref(&first)).await?;

    let mut replacement = first.clone();
    replacement.coverage = json!({
        "status": "partial",
        "exhaustiveness": "authoritative",
        "enumeration_basis": "address_collection"
    });
    replacement.manifest_version = 2;

    let updated =
        upsert_address_names_current_rows(database.pool(), std::slice::from_ref(&replacement))
            .await?;
    assert_eq!(updated, vec![replacement.clone()]);

    let loaded = load_address_names_current(database.pool(), address, None, None).await?;
    assert_eq!(loaded, vec![replacement]);

    database.cleanup().await
}

#[tokio::test]
async fn address_names_current_filters_noncanonical_supporting_identity_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    let canonical_logical_name_id = "ens:alice.eth";
    let canonical_token_lineage_id = Uuid::from_u128(0x1101);
    let canonical_resource_id = Uuid::from_u128(0x1201);
    let canonical_surface_binding_id = Uuid::from_u128(0x1301);
    seed_relation_references(
        &database,
        canonical_logical_name_id,
        "alice.eth",
        canonical_resource_id,
        Some(canonical_token_lineage_id),
        canonical_surface_binding_id,
        CanonicalityState::Finalized,
    )
    .await?;

    let noncanonical_logical_name_id = "ens:bob.eth";
    let noncanonical_token_lineage_id = Uuid::from_u128(0x2101);
    let noncanonical_resource_id = Uuid::from_u128(0x2201);
    let noncanonical_surface_binding_id = Uuid::from_u128(0x2301);
    seed_relation_references(
        &database,
        noncanonical_logical_name_id,
        "bob.eth",
        noncanonical_resource_id,
        Some(noncanonical_token_lineage_id),
        noncanonical_surface_binding_id,
        CanonicalityState::Orphaned,
    )
    .await?;

    let canonical = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: canonical_logical_name_id,
        display_name: "alice.eth",
        relation: AddressNameRelation::Registrant,
        surface_binding_id: canonical_surface_binding_id,
        resource_id: canonical_resource_id,
        token_lineage_id: Some(canonical_token_lineage_id),
        manifest_version: 1,
    });
    let noncanonical = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: noncanonical_logical_name_id,
        display_name: "bob.eth",
        relation: AddressNameRelation::TokenHolder,
        surface_binding_id: noncanonical_surface_binding_id,
        resource_id: noncanonical_resource_id,
        token_lineage_id: Some(noncanonical_token_lineage_id),
        manifest_version: 1,
    });
    upsert_address_names_current_rows(database.pool(), &[canonical.clone(), noncanonical.clone()])
        .await?;

    assert_eq!(
        load_address_names_current(database.pool(), address, None, None).await?,
        vec![canonical.clone()]
    );
    assert_eq!(
        load_address_names_current_including_noncanonical(database.pool(), address, None, None)
            .await?,
        vec![canonical, noncanonical]
    );

    database.cleanup().await
}

#[tokio::test]
async fn address_names_current_full_rebuild_counts_use_name_current_readability() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    seed_relation_references(
        &database,
        "ens:visible.eth",
        "visible.eth",
        Uuid::from_u128(0x2501),
        Some(Uuid::from_u128(0x2401)),
        Uuid::from_u128(0x2601),
        CanonicalityState::Finalized,
    )
    .await?;
    seed_relation_references(
        &database,
        "ens:hidden.eth",
        "hidden.eth",
        Uuid::from_u128(0x3501),
        Some(Uuid::from_u128(0x3401)),
        Uuid::from_u128(0x3601),
        CanonicalityState::Finalized,
    )
    .await?;

    let visible = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:visible.eth",
        display_name: "visible.eth",
        relation: AddressNameRelation::Registrant,
        surface_binding_id: Uuid::from_u128(0x2601),
        resource_id: Uuid::from_u128(0x2501),
        token_lineage_id: Some(Uuid::from_u128(0x2401)),
        manifest_version: 1,
    });
    let hidden = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:hidden.eth",
        display_name: "hidden.eth",
        relation: AddressNameRelation::TokenHolder,
        surface_binding_id: Uuid::from_u128(0x3601),
        resource_id: Uuid::from_u128(0x3501),
        token_lineage_id: Some(Uuid::from_u128(0x3401)),
        manifest_version: 1,
    });

    upsert_name_current_rows(database.pool(), &[name_current_row(&visible)]).await?;
    let rebuild = begin_address_names_current_full_rebuild(database.pool()).await?;
    assert_eq!(rebuild.previous_row_count(), 0);
    insert_address_names_current_full_rebuild_rows(
        database.pool(),
        &rebuild,
        &[visible.clone(), hidden],
    )
    .await?;
    publish_address_names_current_full_rebuild(database.pool(), &rebuild).await?;
    drop_address_names_current_full_rebuild(database.pool(), &rebuild).await?;

    let owned_total = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT total_count
        FROM address_names_current_identity_counts
        WHERE address = $1 AND roles = 'owned'
        "#,
    )
    .bind(address)
    .fetch_optional(database.pool())
    .await?
    .unwrap_or_default();
    let both_total = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT total_count
        FROM address_names_current_identity_counts
        WHERE address = $1 AND roles = 'both'
        "#,
    )
    .bind(address)
    .fetch_optional(database.pool())
    .await?
    .unwrap_or_default();
    let owned_feed_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM address_names_current_identity_feed
        WHERE address = $1 AND roles = 'owned' AND coin_type = ''
        "#,
    )
    .bind(address)
    .fetch_one(database.pool())
    .await?;

    assert_eq!(owned_total, 1);
    assert_eq!(both_total, 1);
    assert_eq!(owned_feed_count, 1);

    database.cleanup().await
}

#[tokio::test]
async fn address_names_current_full_rebuild_keeps_public_rows_until_publish() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    seed_relation_references(
        &database,
        "ens:existing.eth",
        "existing.eth",
        Uuid::from_u128(0x4501),
        Some(Uuid::from_u128(0x4401)),
        Uuid::from_u128(0x4601),
        CanonicalityState::Finalized,
    )
    .await?;
    seed_relation_references(
        &database,
        "ens:replacement.eth",
        "replacement.eth",
        Uuid::from_u128(0x5501),
        Some(Uuid::from_u128(0x5401)),
        Uuid::from_u128(0x5601),
        CanonicalityState::Finalized,
    )
    .await?;

    let existing = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:existing.eth",
        display_name: "existing.eth",
        relation: AddressNameRelation::Registrant,
        surface_binding_id: Uuid::from_u128(0x4601),
        resource_id: Uuid::from_u128(0x4501),
        token_lineage_id: Some(Uuid::from_u128(0x4401)),
        manifest_version: 1,
    });
    let replacement = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:replacement.eth",
        display_name: "replacement.eth",
        relation: AddressNameRelation::TokenHolder,
        surface_binding_id: Uuid::from_u128(0x5601),
        resource_id: Uuid::from_u128(0x5501),
        token_lineage_id: Some(Uuid::from_u128(0x5401)),
        manifest_version: 2,
    });
    upsert_name_current_rows(
        database.pool(),
        &[name_current_row(&existing), name_current_row(&replacement)],
    )
    .await?;
    upsert_address_names_current_rows(database.pool(), std::slice::from_ref(&existing)).await?;
    rebuild_address_names_current_identity_sidecars(database.pool()).await?;

    let rebuild = begin_address_names_current_full_rebuild(database.pool()).await?;
    assert_eq!(rebuild.previous_row_count(), 1);
    insert_address_names_current_full_rebuild_rows(
        database.pool(),
        &rebuild,
        std::slice::from_ref(&replacement),
    )
    .await?;

    assert_eq!(
        load_address_names_current(database.pool(), address, None, None).await?,
        vec![existing.clone()]
    );
    let owned_total = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT total_count
        FROM address_names_current_identity_counts
        WHERE address = $1 AND roles = 'owned'
        "#,
    )
    .bind(address)
    .fetch_optional(database.pool())
    .await?
    .unwrap_or_default();
    assert_eq!(owned_total, 1);

    drop_address_names_current_full_rebuild(database.pool(), &rebuild).await?;
    assert_eq!(
        load_address_names_current(database.pool(), address, None, None).await?,
        vec![existing]
    );

    database.cleanup().await
}

#[tokio::test]
async fn address_names_current_address_replacement_swaps_one_address_and_sidecars() -> Result<()> {
    let database = TestDatabase::new().await?;
    let target = "0x0000000000000000000000000000000000000abc";
    let other = "0x0000000000000000000000000000000000000def";

    for (logical_name_id, display_name, resource_id, token_lineage_id, surface_binding_id) in [
        (
            "ens:old-owned.eth",
            "old-owned.eth",
            Uuid::from_u128(0x6501),
            Uuid::from_u128(0x6401),
            Uuid::from_u128(0x6601),
        ),
        (
            "ens:old-managed.eth",
            "old-managed.eth",
            Uuid::from_u128(0x7501),
            Uuid::from_u128(0x7401),
            Uuid::from_u128(0x7601),
        ),
        (
            "ens:replacement.eth",
            "replacement.eth",
            Uuid::from_u128(0x8501),
            Uuid::from_u128(0x8401),
            Uuid::from_u128(0x8601),
        ),
        (
            "ens:other.eth",
            "other.eth",
            Uuid::from_u128(0x9501),
            Uuid::from_u128(0x9401),
            Uuid::from_u128(0x9601),
        ),
    ] {
        seed_relation_references(
            &database,
            logical_name_id,
            display_name,
            resource_id,
            Some(token_lineage_id),
            surface_binding_id,
            CanonicalityState::Finalized,
        )
        .await?;
    }

    let old_owned = address_name_current_row(AddressNameCurrentRowSeed {
        address: target,
        logical_name_id: "ens:old-owned.eth",
        display_name: "old-owned.eth",
        relation: AddressNameRelation::Registrant,
        surface_binding_id: Uuid::from_u128(0x6601),
        resource_id: Uuid::from_u128(0x6501),
        token_lineage_id: Some(Uuid::from_u128(0x6401)),
        manifest_version: 1,
    });
    let old_managed = address_name_current_row(AddressNameCurrentRowSeed {
        address: target,
        logical_name_id: "ens:old-managed.eth",
        display_name: "old-managed.eth",
        relation: AddressNameRelation::EffectiveController,
        surface_binding_id: Uuid::from_u128(0x7601),
        resource_id: Uuid::from_u128(0x7501),
        token_lineage_id: Some(Uuid::from_u128(0x7401)),
        manifest_version: 1,
    });
    let replacement_row = address_name_current_row(AddressNameCurrentRowSeed {
        address: target,
        logical_name_id: "ens:replacement.eth",
        display_name: "replacement.eth",
        relation: AddressNameRelation::TokenHolder,
        surface_binding_id: Uuid::from_u128(0x8601),
        resource_id: Uuid::from_u128(0x8501),
        token_lineage_id: Some(Uuid::from_u128(0x8401)),
        manifest_version: 2,
    });
    let other_row = address_name_current_row(AddressNameCurrentRowSeed {
        address: other,
        logical_name_id: "ens:other.eth",
        display_name: "other.eth",
        relation: AddressNameRelation::Registrant,
        surface_binding_id: Uuid::from_u128(0x9601),
        resource_id: Uuid::from_u128(0x9501),
        token_lineage_id: Some(Uuid::from_u128(0x9401)),
        manifest_version: 1,
    });

    upsert_name_current_rows(
        database.pool(),
        &[
            name_current_row(&old_owned),
            name_current_row(&old_managed),
            name_current_row(&replacement_row),
            name_current_row(&other_row),
        ],
    )
    .await?;
    upsert_address_names_current_rows(
        database.pool(),
        &[old_owned, old_managed, other_row.clone()],
    )
    .await?;
    rebuild_address_names_current_identity_sidecars(database.pool()).await?;

    let replacement =
        begin_address_names_current_address_replacement(database.pool(), target).await?;
    insert_address_names_current_address_replacement_rows(
        database.pool(),
        &replacement,
        std::slice::from_ref(&replacement_row),
    )
    .await?;
    let (deleted_row_count, inserted_row_count) =
        publish_address_names_current_address_replacement(database.pool(), &replacement).await?;
    drop_address_names_current_address_replacement(database.pool(), &replacement).await?;

    assert_eq!((deleted_row_count, inserted_row_count), (2, 1));
    assert_eq!(
        load_address_names_current(database.pool(), target, None, None).await?,
        vec![replacement_row]
    );
    assert_eq!(
        load_address_names_current(database.pool(), other, None, None).await?,
        vec![other_row]
    );
    assert_eq!(identity_count(database.pool(), target, "owned").await?, 1);
    assert_eq!(identity_count(database.pool(), target, "managed").await?, 0);
    assert_eq!(identity_count(database.pool(), target, "both").await?, 1);
    assert_eq!(
        identity_feed_count(database.pool(), target, "owned").await?,
        1
    );
    assert_eq!(
        identity_feed_count(database.pool(), target, "managed").await?,
        0
    );
    assert_eq!(
        identity_feed_count(database.pool(), target, "both").await?,
        1
    );
    assert_eq!(identity_count(database.pool(), other, "owned").await?, 1);
    assert_eq!(
        identity_feed_count(database.pool(), other, "both").await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn address_names_current_address_replacement_matches_trigger_lock_order() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let replacement =
        begin_address_names_current_address_replacement(database.pool(), address).await?;
    let mut writer = database.pool().begin().await?;

    sqlx::query(
        r#"
        LOCK TABLE address_names_current IN ROW EXCLUSIVE MODE
        "#,
    )
    .execute(&mut *writer)
    .await
    .context("failed to simulate trigger-maintained address_names_current write lock")?;

    let publish_task = {
        let pool = database.pool().clone();
        let replacement = replacement.clone();
        tokio::spawn(async move {
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                publish_address_names_current_address_replacement(&pool, &replacement),
            )
            .await
        })
    };

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    sqlx::query("SET LOCAL lock_timeout = '500ms'")
        .execute(&mut *writer)
        .await
        .context("failed to set lock timeout for trigger lock-order regression")?;
    sqlx::query(
        r#"
        SELECT address_names_current_identity_counts_lock_address($1)
        "#,
    )
    .bind(address)
    .execute(&mut *writer)
    .await
    .context("address replacement publish held the advisory lock before the table lock")?;

    writer
        .rollback()
        .await
        .context("failed to release simulated writer lock")?;

    let publish_summary = publish_task
        .await
        .context("address replacement publish task failed")?
        .context("address replacement publish timed out behind writer lock")??;
    assert_eq!(publish_summary, (0, 0));

    drop_address_names_current_address_replacement(database.pool(), &replacement).await?;
    database.cleanup().await
}

#[tokio::test]
async fn address_names_current_address_replacement_can_clear_an_address() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    seed_relation_references(
        &database,
        "ens:cleared.eth",
        "cleared.eth",
        Uuid::from_u128(0xa501),
        Some(Uuid::from_u128(0xa401)),
        Uuid::from_u128(0xa601),
        CanonicalityState::Finalized,
    )
    .await?;

    let row = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:cleared.eth",
        display_name: "cleared.eth",
        relation: AddressNameRelation::Registrant,
        surface_binding_id: Uuid::from_u128(0xa601),
        resource_id: Uuid::from_u128(0xa501),
        token_lineage_id: Some(Uuid::from_u128(0xa401)),
        manifest_version: 1,
    });

    upsert_name_current_rows(database.pool(), &[name_current_row(&row)]).await?;
    upsert_address_names_current_rows(database.pool(), std::slice::from_ref(&row)).await?;
    rebuild_address_names_current_identity_sidecars(database.pool()).await?;

    let replacement =
        begin_address_names_current_address_replacement(database.pool(), address).await?;
    let (deleted_row_count, inserted_row_count) =
        publish_address_names_current_address_replacement(database.pool(), &replacement).await?;
    drop_address_names_current_address_replacement(database.pool(), &replacement).await?;

    assert_eq!((deleted_row_count, inserted_row_count), (1, 0));
    assert!(
        load_address_names_current(database.pool(), address, None, None)
            .await?
            .is_empty()
    );
    assert_eq!(identity_count(database.pool(), address, "owned").await?, 0);
    assert_eq!(
        identity_feed_count(database.pool(), address, "both").await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn address_names_current_load_orders_by_display_name_then_relation() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    seed_relation_references(
        &database,
        "ens:bob.eth",
        "bob.eth",
        Uuid::from_u128(0x3201),
        Some(Uuid::from_u128(0x3101)),
        Uuid::from_u128(0x3301),
        CanonicalityState::Finalized,
    )
    .await?;
    seed_relation_references(
        &database,
        "ens:alice.eth",
        "alice.eth",
        Uuid::from_u128(0x4201),
        Some(Uuid::from_u128(0x4101)),
        Uuid::from_u128(0x4301),
        CanonicalityState::Finalized,
    )
    .await?;

    let bob = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:bob.eth",
        display_name: "bob.eth",
        relation: AddressNameRelation::TokenHolder,
        surface_binding_id: Uuid::from_u128(0x3301),
        resource_id: Uuid::from_u128(0x3201),
        token_lineage_id: Some(Uuid::from_u128(0x3101)),
        manifest_version: 1,
    });
    let alice_controller = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:alice.eth",
        display_name: "alice.eth",
        relation: AddressNameRelation::EffectiveController,
        surface_binding_id: Uuid::from_u128(0x4301),
        resource_id: Uuid::from_u128(0x4201),
        token_lineage_id: Some(Uuid::from_u128(0x4101)),
        manifest_version: 1,
    });
    let alice_registrant = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:alice.eth",
        display_name: "alice.eth",
        relation: AddressNameRelation::Registrant,
        surface_binding_id: Uuid::from_u128(0x4301),
        resource_id: Uuid::from_u128(0x4201),
        token_lineage_id: Some(Uuid::from_u128(0x4101)),
        manifest_version: 1,
    });
    let alice_token_holder = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:alice.eth",
        display_name: "alice.eth",
        relation: AddressNameRelation::TokenHolder,
        surface_binding_id: Uuid::from_u128(0x4301),
        resource_id: Uuid::from_u128(0x4201),
        token_lineage_id: Some(Uuid::from_u128(0x4101)),
        manifest_version: 1,
    });
    upsert_address_names_current_rows(
        database.pool(),
        &[
            bob.clone(),
            alice_controller.clone(),
            alice_token_holder.clone(),
            alice_registrant.clone(),
        ],
    )
    .await?;

    assert_eq!(
        load_address_names_current(database.pool(), address, None, None).await?,
        vec![alice_registrant, alice_token_holder, alice_controller, bob]
    );

    database.cleanup().await
}

#[tokio::test]
async fn collapse_address_name_rows_dedupes_surface_and_resource_views() -> Result<()> {
    let address = "0x0000000000000000000000000000000000000abc";
    let shared_resource_id = Uuid::from_u128(0x5201);
    let shared_token_lineage_id = Uuid::from_u128(0x5101);

    let alpha_registrant = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:alpha.eth",
        display_name: "alpha.eth",
        relation: AddressNameRelation::Registrant,
        surface_binding_id: Uuid::from_u128(0x5301),
        resource_id: shared_resource_id,
        token_lineage_id: Some(shared_token_lineage_id),
        manifest_version: 1,
    });
    let alpha_token_holder = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:alpha.eth",
        display_name: "alpha.eth",
        relation: AddressNameRelation::TokenHolder,
        surface_binding_id: Uuid::from_u128(0x5301),
        resource_id: shared_resource_id,
        token_lineage_id: Some(shared_token_lineage_id),
        manifest_version: 1,
    });
    let beta_controller = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:beta.eth",
        display_name: "beta.eth",
        relation: AddressNameRelation::EffectiveController,
        surface_binding_id: Uuid::from_u128(0x6301),
        resource_id: shared_resource_id,
        token_lineage_id: Some(shared_token_lineage_id),
        manifest_version: 1,
    });

    let surface_entries = collapse_address_name_current_rows(
        &[
            beta_controller.clone(),
            alpha_token_holder.clone(),
            alpha_registrant.clone(),
            alpha_token_holder.clone(),
        ],
        AddressNamesCurrentDedupe::Surface,
    );
    assert_eq!(surface_entries.len(), 2);
    assert_eq!(surface_entries[0].logical_name_id, "ens:alpha.eth");
    assert_eq!(
        surface_entries[0].relations,
        vec![
            AddressNameRelation::Registrant,
            AddressNameRelation::TokenHolder
        ]
    );
    assert_eq!(surface_entries[1].logical_name_id, "ens:beta.eth");
    assert_eq!(
        surface_entries[1].relations,
        vec![AddressNameRelation::EffectiveController]
    );

    let resource_entries = collapse_address_name_current_rows(
        &[beta_controller, alpha_token_holder, alpha_registrant],
        AddressNamesCurrentDedupe::Resource,
    );
    assert_eq!(resource_entries.len(), 1);
    assert_eq!(resource_entries[0].logical_name_id, "ens:alpha.eth");
    assert_eq!(
        resource_entries[0].relations,
        vec![
            AddressNameRelation::Registrant,
            AddressNameRelation::TokenHolder,
            AddressNameRelation::EffectiveController
        ]
    );

    Ok(())
}

#[tokio::test]
async fn address_names_current_page_groups_after_filters_and_matches_full_summary() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    seed_relation_references(
        &database,
        "ens:alpha.eth",
        "alpha.eth",
        Uuid::from_u128(0x9201),
        Some(Uuid::from_u128(0x9101)),
        Uuid::from_u128(0x9301),
        CanonicalityState::Finalized,
    )
    .await?;
    seed_relation_references(
        &database,
        "ens:beta.eth",
        "beta.eth",
        Uuid::from_u128(0xa201),
        Some(Uuid::from_u128(0xa101)),
        Uuid::from_u128(0xa301),
        CanonicalityState::Finalized,
    )
    .await?;
    seed_relation_references(
        &database,
        "ens:delta.eth",
        "delta.eth",
        Uuid::from_u128(0xb201),
        Some(Uuid::from_u128(0xb101)),
        Uuid::from_u128(0xb301),
        CanonicalityState::Finalized,
    )
    .await?;

    let alpha_registrant = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:alpha.eth",
        display_name: "alpha.eth",
        relation: AddressNameRelation::Registrant,
        surface_binding_id: Uuid::from_u128(0x9301),
        resource_id: Uuid::from_u128(0x9201),
        token_lineage_id: Some(Uuid::from_u128(0x9101)),
        manifest_version: 1,
    });
    let mut alpha_token_holder = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:alpha.eth",
        display_name: "alpha.eth",
        relation: AddressNameRelation::TokenHolder,
        surface_binding_id: Uuid::from_u128(0x9301),
        resource_id: Uuid::from_u128(0x9201),
        token_lineage_id: Some(Uuid::from_u128(0x9101)),
        manifest_version: 2,
    });
    alpha_token_holder.provenance = json!({
        "normalized_event_ids": ["alpha-token", "shared"],
        "raw_fact_refs": [{"log": "alpha"}],
        "manifest_versions": [2],
        "derivation_kind": "address_names_current_rebuild"
    });
    alpha_token_holder.chain_positions = json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_100_010,
            "block_hash": "0xaaa",
            "timestamp": "2026-04-17T00:00:10Z"
        }
    });
    alpha_token_holder.canonicality_summary = json!({
        "status": "safe",
        "chains": {
            "ethereum-mainnet": "safe"
        }
    });

    let beta_controller = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:beta.eth",
        display_name: "beta.eth",
        relation: AddressNameRelation::EffectiveController,
        surface_binding_id: Uuid::from_u128(0xa301),
        resource_id: Uuid::from_u128(0xa201),
        token_lineage_id: Some(Uuid::from_u128(0xa101)),
        manifest_version: 3,
    });
    let mut delta_token_holder = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:delta.eth",
        display_name: "delta.eth",
        relation: AddressNameRelation::TokenHolder,
        surface_binding_id: Uuid::from_u128(0xb301),
        resource_id: Uuid::from_u128(0xb201),
        token_lineage_id: Some(Uuid::from_u128(0xb101)),
        manifest_version: 4,
    });
    delta_token_holder.provenance = json!({
        "normalized_event_ids": ["shared", "delta-token"],
        "raw_fact_refs": [{"log": "delta"}],
        "manifest_versions": [4],
        "derivation_kind": "address_names_current_rebuild"
    });
    delta_token_holder.chain_positions = json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_100_011,
            "block_hash": "0xbbb",
            "timestamp": "2026-04-17T00:00:11Z"
        }
    });

    upsert_address_names_current_rows(
        database.pool(),
        &[
            alpha_registrant,
            alpha_token_holder,
            beta_controller,
            delta_token_holder,
        ],
    )
    .await?;

    let filtered_rows = load_address_names_current(
        database.pool(),
        address,
        Some("ens"),
        Some(AddressNameRelation::TokenHolder),
    )
    .await?;
    let expected_entries =
        collapse_address_name_current_rows(&filtered_rows, AddressNamesCurrentDedupe::Surface);
    assert_eq!(expected_entries.len(), 2);

    let first_page = load_address_names_current_page(
        database.pool(),
        address,
        Some("ens"),
        Some(AddressNameRelation::TokenHolder),
        AddressNamesCurrentDedupe::Surface,
        None,
        1,
    )
    .await?;
    assert_eq!(first_page.entries, expected_entries[..1].to_vec());
    assert_eq!(first_page.summary, expected_summary(&expected_entries));
    assert_eq!(
        first_page.next_cursor,
        Some(address_names_current_cursor_from_entry(
            &expected_entries[0]
        ))
    );
    assert_eq!(
        first_page.entries[0].relations,
        vec![AddressNameRelation::TokenHolder]
    );

    let second_page = load_address_names_current_page(
        database.pool(),
        address,
        Some("ens"),
        Some(AddressNameRelation::TokenHolder),
        AddressNamesCurrentDedupe::Surface,
        first_page.next_cursor.as_ref(),
        1,
    )
    .await?;
    assert_eq!(second_page.entries, expected_entries[1..].to_vec());
    assert_eq!(second_page.next_cursor, None);
    assert_eq!(second_page.summary, expected_summary(&expected_entries));

    let invalid_cursor = AddressNamesCurrentCursor {
        canonical_display_name: "missing.eth".to_owned(),
        logical_name_id: "ens:missing.eth".to_owned(),
        resource_id: Uuid::from_u128(0xffff),
    };
    let error = load_address_names_current_page(
        database.pool(),
        address,
        Some("ens"),
        Some(AddressNameRelation::TokenHolder),
        AddressNamesCurrentDedupe::Surface,
        Some(&invalid_cursor),
        1,
    )
    .await
    .expect_err("missing grouped cursor must be rejected");
    assert!(
        format!("{error:#}").contains("cursor does not match a grouped entry"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn address_names_current_page_resource_dedupe_matches_collapsed_full_read() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let shared_resource_id = Uuid::from_u128(0xc201);
    let shared_token_lineage_id = Uuid::from_u128(0xc101);

    seed_relation_references(
        &database,
        "ens:alpha.eth",
        "alpha.eth",
        shared_resource_id,
        Some(shared_token_lineage_id),
        Uuid::from_u128(0xc301),
        CanonicalityState::Finalized,
    )
    .await?;
    seed_relation_references(
        &database,
        "ens:beta.eth",
        "beta.eth",
        shared_resource_id,
        Some(shared_token_lineage_id),
        Uuid::from_u128(0xd301),
        CanonicalityState::Finalized,
    )
    .await?;

    let alpha_registrant = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:alpha.eth",
        display_name: "alpha.eth",
        relation: AddressNameRelation::Registrant,
        surface_binding_id: Uuid::from_u128(0xc301),
        resource_id: shared_resource_id,
        token_lineage_id: Some(shared_token_lineage_id),
        manifest_version: 1,
    });
    let alpha_token_holder = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:alpha.eth",
        display_name: "alpha.eth",
        relation: AddressNameRelation::TokenHolder,
        surface_binding_id: Uuid::from_u128(0xc301),
        resource_id: shared_resource_id,
        token_lineage_id: Some(shared_token_lineage_id),
        manifest_version: 2,
    });
    let beta_controller = address_name_current_row(AddressNameCurrentRowSeed {
        address,
        logical_name_id: "ens:beta.eth",
        display_name: "beta.eth",
        relation: AddressNameRelation::EffectiveController,
        surface_binding_id: Uuid::from_u128(0xd301),
        resource_id: shared_resource_id,
        token_lineage_id: Some(shared_token_lineage_id),
        manifest_version: 3,
    });
    upsert_address_names_current_rows(
        database.pool(),
        &[
            beta_controller,
            alpha_token_holder.clone(),
            alpha_registrant.clone(),
        ],
    )
    .await?;

    let rows = load_address_names_current(database.pool(), address, Some("ens"), None).await?;
    let expected_entries =
        collapse_address_name_current_rows(&rows, AddressNamesCurrentDedupe::Resource);

    let page = load_address_names_current_page(
        database.pool(),
        address,
        Some("ens"),
        None,
        AddressNamesCurrentDedupe::Resource,
        None,
        10,
    )
    .await?;
    assert_eq!(page.entries, expected_entries);
    assert_eq!(page.summary, expected_summary(&page.entries));
    assert_eq!(page.next_cursor, None);
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].logical_name_id, "ens:alpha.eth");
    assert_eq!(
        page.entries[0].relations,
        vec![
            AddressNameRelation::Registrant,
            AddressNameRelation::TokenHolder,
            AddressNameRelation::EffectiveController
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn address_names_current_delete_and_clear_support_rebuild_workflows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let first_address = "0x0000000000000000000000000000000000000abc";
    let second_address = "0x0000000000000000000000000000000000000def";

    seed_relation_references(
        &database,
        "ens:alice.eth",
        "alice.eth",
        Uuid::from_u128(0x7201),
        Some(Uuid::from_u128(0x7101)),
        Uuid::from_u128(0x7301),
        CanonicalityState::Finalized,
    )
    .await?;
    seed_relation_references(
        &database,
        "ens:bob.eth",
        "bob.eth",
        Uuid::from_u128(0x8201),
        Some(Uuid::from_u128(0x8101)),
        Uuid::from_u128(0x8301),
        CanonicalityState::Finalized,
    )
    .await?;

    let first = address_name_current_row(AddressNameCurrentRowSeed {
        address: first_address,
        logical_name_id: "ens:alice.eth",
        display_name: "alice.eth",
        relation: AddressNameRelation::Registrant,
        surface_binding_id: Uuid::from_u128(0x7301),
        resource_id: Uuid::from_u128(0x7201),
        token_lineage_id: Some(Uuid::from_u128(0x7101)),
        manifest_version: 1,
    });
    let second = address_name_current_row(AddressNameCurrentRowSeed {
        address: second_address,
        logical_name_id: "ens:bob.eth",
        display_name: "bob.eth",
        relation: AddressNameRelation::TokenHolder,
        surface_binding_id: Uuid::from_u128(0x8301),
        resource_id: Uuid::from_u128(0x8201),
        token_lineage_id: Some(Uuid::from_u128(0x8101)),
        manifest_version: 1,
    });
    upsert_address_names_current_rows(database.pool(), &[first, second.clone()]).await?;

    assert_eq!(
        delete_address_names_current(database.pool(), first_address).await?,
        1
    );
    assert!(
        load_address_names_current(database.pool(), first_address, None, None)
            .await?
            .is_empty()
    );

    assert_eq!(clear_address_names_current(database.pool()).await?, 1);
    assert!(
        load_address_names_current(database.pool(), second_address, None, None)
            .await?
            .is_empty()
    );

    database.cleanup().await
}
