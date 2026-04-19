use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use anyhow::{Context, Result};
use bigname_storage::{
    CanonicalityState, ChildrenCurrentRow, clear_children_current, delete_children_current,
    load_canonical_declared_child_sources, load_children_current, load_raw_block,
    upsert_children_current_rows, upsert_name_surfaces, upsert_normalized_events,
    upsert_raw_blocks,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::{OffsetDateTime, UtcOffset},
};

const DECLARED_SURFACE_CLASS: &str = "declared";
const CHILDREN_CURRENT_DERIVATION_KIND: &str = "children_current_rebuild";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChildrenCurrentRebuildSummary {
    pub requested_parent_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

pub async fn rebuild_children_current(
    pool: &PgPool,
    parent_logical_name_id: Option<&str>,
) -> Result<ChildrenCurrentRebuildSummary> {
    match parent_logical_name_id {
        Some(parent_logical_name_id) => rebuild_one_parent(pool, parent_logical_name_id).await,
        None => rebuild_all_parents(pool).await,
    }
}

async fn rebuild_all_parents(pool: &PgPool) -> Result<ChildrenCurrentRebuildSummary> {
    let deleted_row_count = clear_children_current(pool).await?;
    let sources = load_canonical_declared_child_sources(pool, None).await?;
    let requested_parent_count = sources
        .iter()
        .map(|source| source.parent_logical_name_id.clone())
        .collect::<BTreeSet<_>>()
        .len();
    let rows = build_children_rows(pool, &sources).await?;
    let upserted_row_count = upsert_children_current_rows(pool, &rows).await?.len();

    Ok(ChildrenCurrentRebuildSummary {
        requested_parent_count,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn rebuild_one_parent(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<ChildrenCurrentRebuildSummary> {
    let deleted_row_count = delete_children_current(pool, parent_logical_name_id).await?;
    let sources = load_canonical_declared_child_sources(pool, Some(parent_logical_name_id)).await?;
    if sources.is_empty() {
        return Ok(ChildrenCurrentRebuildSummary {
            requested_parent_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    }

    let rows = build_children_rows(pool, &sources).await?;
    let upserted_row_count = upsert_children_current_rows(pool, &rows).await?.len();

    Ok(ChildrenCurrentRebuildSummary {
        requested_parent_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn build_children_rows(
    pool: &PgPool,
    sources: &[bigname_storage::DeclaredChildEventSource],
) -> Result<Vec<ChildrenCurrentRow>> {
    let mut block_cache = BTreeMap::new();
    let mut rows = Vec::with_capacity(sources.len());

    for source in sources {
        rows.push(build_children_row(pool, source, &mut block_cache).await?);
    }

    Ok(rows)
}

async fn build_children_row(
    pool: &PgPool,
    source: &bigname_storage::DeclaredChildEventSource,
    block_cache: &mut BTreeMap<(String, String), bigname_storage::RawBlock>,
) -> Result<ChildrenCurrentRow> {
    let block = load_source_block(pool, source, block_cache).await?;

    Ok(ChildrenCurrentRow {
        parent_logical_name_id: source.parent_logical_name_id.clone(),
        child_logical_name_id: source.child_logical_name_id.clone(),
        surface_class: DECLARED_SURFACE_CLASS.to_owned(),
        namespace: source.namespace.clone(),
        canonical_display_name: source.canonical_display_name.clone(),
        normalized_name: source.normalized_name.clone(),
        namehash: source.namehash.clone(),
        provenance: build_provenance(source),
        chain_positions: build_chain_positions(source, &block),
        canonicality_summary: build_canonicality_summary(source, block.canonicality_state),
        manifest_version: source.manifest_version,
        last_recomputed_at: block.block_timestamp,
    })
}

async fn load_source_block(
    pool: &PgPool,
    source: &bigname_storage::DeclaredChildEventSource,
    block_cache: &mut BTreeMap<(String, String), bigname_storage::RawBlock>,
) -> Result<bigname_storage::RawBlock> {
    let cache_key = (source.chain_id.clone(), source.block_hash.clone());
    if let Some(block) = block_cache.get(&cache_key) {
        return Ok(block.clone());
    }

    let block = load_raw_block(pool, &source.chain_id, &source.block_hash)
        .await
        .with_context(|| {
            format!(
                "failed to load raw block for child source {} on chain {} block {}",
                source.event_identity, source.chain_id, source.block_hash
            )
        })?
        .with_context(|| {
            format!(
                "missing raw block for child source {} on chain {} block {}",
                source.event_identity, source.chain_id, source.block_hash
            )
        })?;

    block_cache.insert(cache_key, block.clone());
    Ok(block)
}

fn build_provenance(source: &bigname_storage::DeclaredChildEventSource) -> Value {
    json!({
        "normalized_event_ids": [source.normalized_event_id],
        "raw_fact_refs": [source.raw_fact_ref.clone()],
        "manifest_versions": [{
            "source_manifest_id": source.source_manifest_id,
            "source_family": source.source_family,
            "manifest_version": source.manifest_version,
        }],
        "execution_trace_id": Value::Null,
        "derivation_kind": CHILDREN_CURRENT_DERIVATION_KIND,
    })
}

fn build_chain_positions(
    source: &bigname_storage::DeclaredChildEventSource,
    block: &bigname_storage::RawBlock,
) -> Value {
    json!({
        chain_slot(&source.chain_id): {
            "chain_id": source.chain_id,
            "block_number": source.block_number,
            "block_hash": source.block_hash,
            "timestamp": format_timestamp(block.block_timestamp),
        }
    })
}

fn build_canonicality_summary(
    source: &bigname_storage::DeclaredChildEventSource,
    state: CanonicalityState,
) -> Value {
    json!({
        "status": state.as_str(),
        "chains": {
            source.chain_id.clone(): state.as_str(),
        }
    })
}

fn chain_slot(chain_id: &str) -> &str {
    match chain_id {
        "ethereum-mainnet" => "ethereum",
        "base-mainnet" => "base",
        _ => chain_id,
    }
}

fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
    use bigname_storage::{NameSurface, NormalizedEvent, RawBlock, default_database_url};

    use super::*;

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
                .context("failed to parse database URL for worker children_current tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_worker_children_current_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for worker children_current tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect worker children_current test pool")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for worker children_current tests")?;

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
    async fn rebuilds_declared_children_for_one_parent() -> Result<()> {
        let database = TestDatabase::new().await?;
        let parent = "ens:parent.eth";

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xblock64", 100, 1_717_172_100),
                raw_block("ethereum-mainnet", "0xblock65", 101, 1_717_172_101),
                raw_block("ethereum-mainnet", "0xblock66", 102, 1_717_172_102),
            ],
        )
        .await?;
        seed_name_surfaces(
            database.pool(),
            &[
                name_surface(parent, "parent.eth", "node:parent.eth", 10),
                name_surface(
                    "ens:alice.parent.eth",
                    "alice.parent.eth",
                    "node:alice.parent.eth",
                    11,
                ),
                name_surface(
                    "ens:bob.parent.eth",
                    "bob.parent.eth",
                    "node:bob.parent.eth",
                    12,
                ),
                name_surface(
                    "ens:carol.parent.eth",
                    "carol.parent.eth",
                    "node:carol.parent.eth",
                    13,
                ),
            ],
        )
        .await?;
        seed_subregistry_events(
            database.pool(),
            &[
                subregistry_event(
                    "ens",
                    "alice-active",
                    "node:parent.eth",
                    "node:alice.parent.eth",
                    100,
                    0,
                    false,
                    true,
                ),
                subregistry_event(
                    "ens",
                    "bob-tombstoned",
                    "node:parent.eth",
                    "node:bob.parent.eth",
                    101,
                    0,
                    true,
                    false,
                ),
                subregistry_event(
                    "ens",
                    "carol-active",
                    "node:parent.eth",
                    "node:carol.parent.eth",
                    102,
                    0,
                    false,
                    true,
                ),
            ],
        )
        .await?;

        let summary = rebuild_children_current(database.pool(), Some(parent)).await?;
        assert_eq!(summary.requested_parent_count, 1);
        assert_eq!(summary.upserted_row_count, 2);
        assert_eq!(summary.deleted_row_count, 0);

        let rows = load_children_current(database.pool(), parent).await?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].child_logical_name_id, "ens:alice.parent.eth");
        assert_eq!(rows[0].surface_class, DECLARED_SURFACE_CLASS);
        assert_eq!(
            rows[0].chain_positions["ethereum"]["block_number"],
            json!(100)
        );
        assert_eq!(rows[0].canonicality_summary["status"], json!("finalized"));
        assert_eq!(
            rows[0].provenance["derivation_kind"],
            json!(CHILDREN_CURRENT_DERIVATION_KIND)
        );
        assert_eq!(rows[1].child_logical_name_id, "ens:carol.parent.eth");
        assert_eq!(rows[1].last_recomputed_at, timestamp(1_717_172_102));

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_all_clears_stale_rows_and_is_idempotent() -> Result<()> {
        let database = TestDatabase::new().await?;
        let parent = "ens:parent.eth";
        let stale_parent = "ens:stale.eth";

        seed_raw_blocks(
            database.pool(),
            &[raw_block(
                "ethereum-mainnet",
                "0xblock6e",
                110,
                1_717_172_110,
            )],
        )
        .await?;
        seed_name_surfaces(
            database.pool(),
            &[
                name_surface(parent, "parent.eth", "node:parent.eth", 20),
                name_surface(stale_parent, "stale.eth", "node:stale.eth", 21),
                name_surface(
                    "ens:alice.parent.eth",
                    "alice.parent.eth",
                    "node:alice.parent.eth",
                    22,
                ),
                name_surface(
                    "ens:stale-child.stale.eth",
                    "stale-child.stale.eth",
                    "node:stale-child.stale.eth",
                    23,
                ),
            ],
        )
        .await?;
        upsert_children_current_rows(
            database.pool(),
            &[ChildrenCurrentRow {
                parent_logical_name_id: stale_parent.to_owned(),
                child_logical_name_id: "ens:stale-child.stale.eth".to_owned(),
                surface_class: DECLARED_SURFACE_CLASS.to_owned(),
                namespace: "ens".to_owned(),
                canonical_display_name: "stale-child.stale.eth".to_owned(),
                normalized_name: "stale-child.stale.eth".to_owned(),
                namehash: "node:stale-child.stale.eth".to_owned(),
                provenance: json!({
                    "normalized_event_ids": [1],
                    "raw_fact_refs": [],
                    "manifest_versions": [],
                    "execution_trace_id": Value::Null,
                    "derivation_kind": CHILDREN_CURRENT_DERIVATION_KIND,
                }),
                chain_positions: json!({
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 1,
                        "block_hash": "0xstale",
                        "timestamp": "2026-04-17T00:00:01Z"
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {"ethereum-mainnet": "finalized"}
                }),
                manifest_version: 1,
                last_recomputed_at: timestamp(1_717_172_001),
            }],
        )
        .await?;
        seed_subregistry_events(
            database.pool(),
            &[subregistry_event(
                "ens",
                "alice-active",
                "node:parent.eth",
                "node:alice.parent.eth",
                110,
                0,
                false,
                true,
            )],
        )
        .await?;

        let first = rebuild_children_current(database.pool(), None).await?;
        assert_eq!(first.requested_parent_count, 1);
        assert_eq!(first.upserted_row_count, 1);
        assert_eq!(first.deleted_row_count, 1);

        let first_rows = load_children_current(database.pool(), parent).await?;
        assert!(
            load_children_current(database.pool(), stale_parent)
                .await?
                .is_empty()
        );

        let second = rebuild_children_current(database.pool(), None).await?;
        assert_eq!(second.requested_parent_count, 1);
        assert_eq!(second.upserted_row_count, 1);
        assert_eq!(second.deleted_row_count, 1);

        let second_rows = load_children_current(database.pool(), parent).await?;
        assert_eq!(first_rows, second_rows);

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuilds_basenames_declared_children_from_base_authority_sources() -> Result<()> {
        let database = TestDatabase::new().await?;
        let parent = "basenames:base.eth";

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("base-mainnet", "0xblockc8", 200, 1_717_172_200),
                raw_block("base-mainnet", "0xblockc9", 201, 1_717_172_201),
                raw_block("base-mainnet", "0xblockca", 202, 1_717_172_202),
            ],
        )
        .await?;
        seed_name_surfaces(
            database.pool(),
            &[
                name_surface(parent, "base.eth", "node:base.eth", 30),
                name_surface(
                    "basenames:alice.base.eth",
                    "alice.base.eth",
                    "node:alice.base.eth",
                    31,
                ),
                name_surface(
                    "basenames:bob.base.eth",
                    "bob.base.eth",
                    "node:bob.base.eth",
                    32,
                ),
                name_surface(
                    "basenames:carol.base.eth",
                    "carol.base.eth",
                    "node:carol.base.eth",
                    33,
                ),
            ],
        )
        .await?;
        seed_subregistry_events(
            database.pool(),
            &[
                subregistry_event(
                    "basenames",
                    "alice-active",
                    "node:base.eth",
                    "node:alice.base.eth",
                    200,
                    0,
                    false,
                    true,
                ),
                subregistry_event(
                    "basenames",
                    "bob-tombstoned",
                    "node:base.eth",
                    "node:bob.base.eth",
                    201,
                    0,
                    true,
                    false,
                ),
                subregistry_event(
                    "basenames",
                    "carol-active",
                    "node:base.eth",
                    "node:carol.base.eth",
                    202,
                    0,
                    false,
                    true,
                ),
            ],
        )
        .await?;

        let summary = rebuild_children_current(database.pool(), Some(parent)).await?;
        assert_eq!(summary.requested_parent_count, 1);
        assert_eq!(summary.upserted_row_count, 2);
        assert_eq!(summary.deleted_row_count, 0);

        let rows = load_children_current(database.pool(), parent).await?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].child_logical_name_id, "basenames:alice.base.eth");
        assert_eq!(rows[0].namespace, "basenames");
        assert_eq!(rows[0].surface_class, DECLARED_SURFACE_CLASS);
        assert_eq!(rows[0].chain_positions["base"]["block_number"], json!(200));
        assert_eq!(
            rows[0].provenance["manifest_versions"][0]["source_family"],
            json!("basenames_base_registry")
        );
        assert_eq!(
            rows[0].canonicality_summary["chains"]["base-mainnet"],
            json!("finalized")
        );
        assert_eq!(rows[1].child_logical_name_id, "basenames:carol.base.eth");
        assert_eq!(rows[1].last_recomputed_at, timestamp(1_717_172_202));

        database.cleanup().await
    }

    async fn seed_raw_blocks(pool: &PgPool, blocks: &[RawBlock]) -> Result<()> {
        upsert_raw_blocks(pool, blocks).await?;
        Ok(())
    }

    async fn seed_name_surfaces(pool: &PgPool, surfaces: &[NameSurface]) -> Result<()> {
        upsert_name_surfaces(pool, surfaces).await?;
        Ok(())
    }

    async fn seed_subregistry_events(pool: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
        upsert_normalized_events(pool, events).await?;
        Ok(())
    }

    fn raw_block(
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
        unix_timestamp: i64,
    ) -> RawBlock {
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

    fn name_surface(
        logical_name_id: &str,
        display_name: &str,
        namehash: &str,
        block_number: i64,
    ) -> NameSurface {
        let namespace = logical_name_id
            .split_once(':')
            .map(|(namespace, _)| namespace)
            .expect("logical_name_id must include namespace")
            .to_owned();
        let chain_id = chain_id_for_namespace(&namespace).to_owned();

        NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace,
            input_name: display_name.to_owned(),
            canonical_display_name: display_name.to_owned(),
            normalized_name: display_name.to_owned(),
            dns_encoded_name: display_name.as_bytes().to_vec(),
            namehash: namehash.to_owned(),
            labelhashes: vec![format!("labelhash:{display_name}")],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id,
            block_hash: format!("0xsurface{block_number:02x}"),
            block_number,
            provenance: json!({"source": "worker_children_current_test", "kind": "name_surface"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn subregistry_event(
        namespace: &str,
        event_identity: &str,
        parent_namehash: &str,
        child_namehash: &str,
        block_number: i64,
        log_index: i64,
        tombstone: bool,
        active_edge: bool,
    ) -> NormalizedEvent {
        assert!(
            !(tombstone && active_edge),
            "test subregistry_event cannot be both tombstoned and active"
        );
        let chain_id = chain_id_for_namespace(namespace);

        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: namespace.to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: "SubregistryChanged".to_owned(),
            source_family: source_family_for_namespace(namespace).to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some(chain_id.to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xblock{block_number:02x}")),
            transaction_hash: Some(format!("0xtx{block_number:02x}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": chain_id,
                "block_number": block_number,
                "log_index": log_index
            }),
            derivation_kind: "ens_v1_subregistry_changed".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "source_event": "NewOwner",
                "edge_kind": "subregistry",
                "parent_node": parent_namehash,
                "child_node": child_namehash,
                "labelhash": format!("labelhash:{child_namehash}"),
                "owner": "0x0000000000000000000000000000000000000001",
                "tombstone": tombstone,
                "active_edge": active_edge
            }),
        }
    }

    fn chain_id_for_namespace(namespace: &str) -> &'static str {
        match namespace {
            "basenames" => "base-mainnet",
            _ => "ethereum-mainnet",
        }
    }

    fn source_family_for_namespace(namespace: &str) -> &'static str {
        match namespace {
            "basenames" => "basenames_base_registry",
            _ => "ens_v1_registry_l1",
        }
    }

    fn timestamp(value: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(value).expect("timestamp must be valid")
    }
}
