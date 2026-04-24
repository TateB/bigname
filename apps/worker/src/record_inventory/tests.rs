use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::Result;
use bigname_storage::{
    ChainLineageBlock, NormalizedEvent, RawBlock, RawCodeHash, RawLog, Resource,
    default_database_url, load_record_inventory_current, upsert_chain_lineage_blocks,
    upsert_normalized_events, upsert_raw_blocks, upsert_raw_code_hashes, upsert_raw_logs,
    upsert_resources,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

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
            .context("failed to parse database URL for worker record_inventory_current tests")?;
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bg_wr_{}_{}_{}",
            std::process::id(),
            sequence,
            &Uuid::new_v4().simple().to_string()[..8]
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for worker record_inventory_current tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect worker record_inventory_current test pool")?;

        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for worker record_inventory_current tests")?;

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
async fn full_rebuild_projects_current_rows_for_all_target_resources() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_a = Uuid::from_u128(0x9100);
    let resource_b = Uuid::from_u128(0x9200);

    seed_resources(database.pool(), &[resource_a, resource_b]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1000", 1000, 1_776_200_000),
            raw_block("ethereum-mainnet", "0xrec1001", 1001, 1_776_200_001),
            raw_block("ethereum-mainnet", "0xrec1002", 1002, 1_776_200_002),
            raw_block("ethereum-mainnet", "0xrec1003", 1003, 1_776_200_003),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            record_version_changed_event("res-a-boundary", "ens:alice.eth", resource_a, 7, 1000, 0),
            record_changed_event(
                "res-a-text",
                "ens:alice.eth",
                resource_a,
                "text",
                "text",
                None,
                1001,
                0,
            ),
            record_version_changed_event("res-b-boundary", "ens:bob.eth", resource_b, 11, 1002, 0),
            record_changed_event(
                "res-b-native-addr",
                "ens:bob.eth",
                resource_b,
                "addr:60",
                "addr",
                Some("60"),
                1003,
                0,
            ),
        ],
    )
    .await?;

    let summary = rebuild_record_inventory_current(database.pool(), None).await?;
    assert_eq!(summary.requested_resource_count, 2);
    assert_eq!(summary.upserted_row_count, 2);
    assert_eq!(summary.deleted_row_count, 0);

    let row_a = load_record_inventory_current(
        database.pool(),
        resource_a,
        &record_version_boundary(
            "ens:alice.eth",
            resource_a,
            Some(1),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1000,
            "0xrec1000",
            1_776_200_000,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("resource_a row must exist")?;
    assert_eq!(
        row_a.selectors,
        json!([{
            "record_key": "text",
            "record_family": "text",
            "selector_key": null,
            "cacheable": true,
        }])
    );

    let row_b = load_record_inventory_current(
        database.pool(),
        resource_b,
        &record_version_boundary(
            "ens:bob.eth",
            resource_b,
            Some(3),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1002,
            "0xrec1002",
            1_776_200_002,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("resource_b row must exist")?;
    assert_eq!(
        row_b.selectors,
        json!([{
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "cacheable": true,
        }])
    );

    database.cleanup().await
}

#[tokio::test]
async fn keyed_rebuild_replaces_one_resource_without_touching_other_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_a = Uuid::from_u128(0x9300);
    let resource_b = Uuid::from_u128(0x9400);

    seed_resources(database.pool(), &[resource_a, resource_b]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1010", 1010, 1_776_200_010),
            raw_block("ethereum-mainnet", "0xrec1011", 1011, 1_776_200_011),
            raw_block("ethereum-mainnet", "0xrec1012", 1012, 1_776_200_012),
            raw_block("ethereum-mainnet", "0xrec1013", 1013, 1_776_200_013),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            record_version_changed_event("res-a-boundary", "ens:alice.eth", resource_a, 7, 1010, 0),
            record_changed_event(
                "res-a-text",
                "ens:alice.eth",
                resource_a,
                "text",
                "text",
                None,
                1011,
                0,
            ),
            record_version_changed_event("res-b-boundary", "ens:bob.eth", resource_b, 8, 1012, 0),
            record_changed_event(
                "res-b-addr",
                "ens:bob.eth",
                resource_b,
                "addr:60",
                "addr",
                Some("60"),
                1013,
                0,
            ),
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), None).await?;

    seed_raw_blocks(
        database.pool(),
        &[raw_block(
            "ethereum-mainnet",
            "0xrec1014",
            1014,
            1_776_200_014,
        )],
    )
    .await?;
    seed_events(
        database.pool(),
        &[record_changed_event(
            "res-a-native-addr",
            "ens:alice.eth",
            resource_a,
            "addr:60",
            "addr",
            Some("60"),
            1014,
            0,
        )],
    )
    .await?;

    let summary =
        rebuild_record_inventory_current(database.pool(), Some(&resource_a.to_string())).await?;
    assert_eq!(summary.requested_resource_count, 1);
    assert_eq!(summary.upserted_row_count, 1);
    assert_eq!(summary.deleted_row_count, 0);

    let row_a = load_record_inventory_current(
        database.pool(),
        resource_a,
        &record_version_boundary(
            "ens:alice.eth",
            resource_a,
            Some(1),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1010,
            "0xrec1010",
            1_776_200_010,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("resource_a row must still exist")?;
    assert_eq!(
        row_a.selectors,
        json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true,
            },
            {
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "cacheable": true,
            }
        ])
    );

    let row_b = load_record_inventory_current(
        database.pool(),
        resource_b,
        &record_version_boundary(
            "ens:bob.eth",
            resource_b,
            Some(3),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1012,
            "0xrec1012",
            1_776_200_012,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("resource_b row must remain untouched")?;
    assert_eq!(
        row_b.selectors,
        json!([{
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "cacheable": true,
        }])
    );

    database.cleanup().await
}

#[tokio::test]
async fn keyed_rebuild_keeps_visible_rows_when_projection_build_fails() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9450);
    let boundary = record_version_boundary(
        "ens:alice.eth",
        resource_id,
        Some(99),
        Some(EVENT_KIND_RECORD_VERSION_CHANGED),
        900,
        "0xstale-boundary",
        1_776_200_900,
        "ethereum-mainnet",
    );

    seed_resources(database.pool(), &[resource_id]).await?;
    upsert_record_inventory_current_rows(
        database.pool(),
        &[RecordInventoryCurrentRow {
            resource_id,
            record_version_boundary: boundary.clone(),
            enumeration_basis: json!({
                "observed_selectors": true,
                "capability_declared_families": true,
                "globally_enumerable": true,
            }),
            selectors: json!([]),
            explicit_gaps: json!([]),
            unsupported_families: json!([]),
            last_change: None,
            entries: json!([]),
            provenance: json!({"derivation_kind": RECORD_INVENTORY_CURRENT_DERIVATION_KIND}),
            coverage: json!({"enumeration_basis": RECORD_INVENTORY_ENUMERATION_BASIS}),
            chain_positions: json!({}),
            canonicality_summary: json!({"status": "finalized", "chains": {}}),
            manifest_version: 1,
            last_recomputed_at: OffsetDateTime::from_unix_timestamp(1_776_200_001)
                .expect("test timestamp must be valid"),
        }],
    )
    .await?;
    seed_events(
        database.pool(),
        &[record_version_changed_event(
            "missing-block-boundary",
            "ens:alice.eth",
            resource_id,
            100,
            1100,
            0,
        )],
    )
    .await?;

    let error = rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string()))
        .await
        .expect_err("rebuild should fail when the record boundary block is missing");
    assert!(
        error
            .to_string()
            .contains("record event must have a raw_blocks timestamp for chain_position")
    );

    let row = load_record_inventory_current(database.pool(), resource_id, &boundary)
        .await?
        .context("stale visible row should still exist after failed rebuild")?;
    assert_eq!(row.record_version_boundary, boundary);

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_surfaces_supported_selectors_gaps_and_unsupported_families() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9500);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1020", 1020, 1_776_200_020),
            raw_block("ethereum-mainnet", "0xrec1021", 1021, 1_776_200_021),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            record_version_changed_event("boundary", "ens:alice.eth", resource_id, 9, 1020, 0),
            record_changed_event(
                "multicoin",
                "ens:alice.eth",
                resource_id,
                "addr:61",
                "addr",
                Some("61"),
                1021,
                0,
            ),
            record_changed_event(
                "unsupported-avatar",
                "ens:alice.eth",
                resource_id,
                "avatar",
                "avatar",
                None,
                1021,
                1,
            ),
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:alice.eth",
            resource_id,
            Some(1),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1020,
            "0xrec1020",
            1_776_200_020,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("row must exist")?;

    assert_eq!(
        row.selectors,
        json!([{
            "record_key": "addr:61",
            "record_family": "addr",
            "selector_key": "61",
            "cacheable": true,
        }])
    );
    assert_eq!(
        row.explicit_gaps,
        json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "gap_reason": GAP_REASON_NOT_OBSERVED,
            },
            {
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "gap_reason": GAP_REASON_NOT_OBSERVED,
            }
        ])
    );
    assert_eq!(
        row.unsupported_families,
        json!([{
            "record_family": "avatar",
            "unsupported_reason": UNSUPPORTED_FAMILY_REASON,
        }])
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_resets_inventory_at_latest_record_version_boundary() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9600);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1030", 1030, 1_776_200_030),
            raw_block("ethereum-mainnet", "0xrec1031", 1031, 1_776_200_031),
            raw_block("ethereum-mainnet", "0xrec1032", 1032, 1_776_200_032),
            raw_block("ethereum-mainnet", "0xrec1033", 1033, 1_776_200_033),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            record_changed_event(
                "before-boundary-text",
                "ens:alice.eth",
                resource_id,
                "text",
                "text",
                None,
                1030,
                0,
            ),
            record_version_changed_event(
                "current-boundary",
                "ens:alice.eth",
                resource_id,
                12,
                1031,
                0,
            ),
            record_changed_event(
                "after-boundary-native-addr",
                "ens:alice.eth",
                resource_id,
                "addr:60",
                "addr",
                Some("60"),
                1032,
                0,
            ),
            record_changed_event(
                "after-boundary-text",
                "ens:alice.eth",
                resource_id,
                "text",
                "text",
                None,
                1033,
                0,
            ),
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:alice.eth",
            resource_id,
            Some(2),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1031,
            "0xrec1031",
            1_776_200_031,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("row must exist")?;

    assert_eq!(
        row.selectors,
        json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true,
            },
            {
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "cacheable": true,
            }
        ])
    );
    assert_eq!(
        row.record_version_boundary,
        record_version_boundary(
            "ens:alice.eth",
            resource_id,
            Some(2),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1031,
            "0xrec1031",
            1_776_200_031,
            "ethereum-mainnet",
        )
    );
    assert_eq!(
        row.chain_positions,
        json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 1033,
                "block_hash": "0xrec1033",
                "timestamp": "2026-04-14T20:53:53Z",
            }
        })
    );
    assert_eq!(
        row.last_change,
        Some(json!({
            "normalized_event_id": 4,
            "event_kind": EVENT_KIND_RECORD_CHANGED,
            "chain_position": {
                "chain_id": "ethereum-mainnet",
                "block_number": 1033,
                "block_hash": "0xrec1033",
                "timestamp": "2026-04-14T20:53:53Z",
            }
        }))
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_limits_cache_entries_to_cacheable_selectors() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9700);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1040", 1040, 1_776_200_040),
            raw_block("ethereum-mainnet", "0xrec1041", 1041, 1_776_200_041),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            record_version_changed_event("boundary", "ens:alice.eth", resource_id, 13, 1040, 0),
            record_changed_event(
                "text",
                "ens:alice.eth",
                resource_id,
                "text",
                "text",
                None,
                1041,
                0,
            ),
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:alice.eth",
            resource_id,
            Some(1),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1040,
            "0xrec1040",
            1_776_200_040,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("row must exist")?;

    assert_eq!(
        row.entries,
        json!([{
            "record_key": "text",
            "record_family": "text",
            "selector_key": null,
            "status": "unsupported",
            "unsupported_reason": CACHE_UNSUPPORTED_REASON_VALUE_NOT_RETAINED,
        }])
    );
    assert_eq!(
        row.explicit_gaps,
        json!([{
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "gap_reason": GAP_REASON_NOT_OBSERVED,
        }])
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_consumes_ensv2_resolver_record_events() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9701);
    let mut boundary =
        record_version_changed_event("ensv2-boundary", "ens:alice.eth", resource_id, 21, 1050, 0);
    boundary.derivation_kind = DERIVATION_KIND_ENS_V2_RESOLVER.to_owned();
    boundary.source_family = "ens_v2_resolver_l1".to_owned();
    let mut record = record_changed_event(
        "ensv2-record",
        "ens:alice.eth",
        resource_id,
        "addr:60",
        "addr",
        Some("60"),
        1051,
        0,
    );
    record.derivation_kind = DERIVATION_KIND_ENS_V2_RESOLVER.to_owned();
    record.source_family = "ens_v2_resolver_l1".to_owned();

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1050", 1050, 1_776_200_050),
            raw_block("ethereum-mainnet", "0xrec1051", 1051, 1_776_200_051),
        ],
    )
    .await?;
    seed_events(database.pool(), &[boundary, record]).await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:alice.eth",
            resource_id,
            Some(1),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1050,
            "0xrec1050",
            1_776_200_050,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("ENSv2 resolver row must exist")?;

    assert_eq!(row.selectors[0]["record_key"], json!("addr:60"));
    assert_eq!(row.entries[0]["record_key"], json!("addr:60"));
    assert_eq!(
        row.chain_positions,
        json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 1051,
                "block_hash": "0xrec1051",
                "timestamp": "2026-04-14T20:54:11Z",
            }
        })
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_projects_basenames_base_authority_record_inventory() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9800);
    let resolver_contract_instance_id = Uuid::from_u128(0x9801);
    let resolver_address = "0x00000000000000000000000000000000000000cc";

    insert_basenames_resolver_profile_seed(
        database.pool(),
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;
    seed_basenames_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("base-mainnet", "0xbase-rec1050", 1050, 1_776_200_050),
            raw_block("base-mainnet", "0xbase-rec1051", 1051, 1_776_200_051),
            raw_block("base-mainnet", "0xbase-rec1052", 1052, 1_776_200_052),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[
            raw_log(
                "base-mainnet",
                "0xbase-rec1050",
                1050,
                "0xbase-tx1050",
                0,
                resolver_address,
            ),
            raw_log(
                "base-mainnet",
                "0xbase-rec1051",
                1051,
                "0xbase-tx1051",
                0,
                resolver_address,
            ),
            raw_log(
                "base-mainnet",
                "0xbase-rec1052",
                1052,
                "0xbase-tx1052",
                0,
                resolver_address,
            ),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            basenames_record_version_changed_event(
                "base-boundary",
                "basenames:alice.base.eth",
                resource_id,
                21,
                1050,
                0,
            ),
            basenames_record_changed_event(
                "base-native-addr",
                "basenames:alice.base.eth",
                resource_id,
                "addr:60",
                "addr",
                Some("60"),
                1051,
                0,
            ),
            basenames_record_changed_event(
                "base-twitter",
                "basenames:alice.base.eth",
                resource_id,
                "text",
                "text",
                None,
                1052,
                0,
            ),
        ],
    )
    .await?;

    let summary =
        rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;
    assert_eq!(summary.requested_resource_count, 1);
    assert_eq!(summary.upserted_row_count, 1);
    assert_eq!(summary.deleted_row_count, 0);

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "basenames:alice.base.eth",
            resource_id,
            Some(1),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1050,
            "0xbase-rec1050",
            1_776_200_050,
            "base-mainnet",
        ),
    )
    .await?
    .context("basenames record_inventory_current row must exist")?;

    assert_eq!(
        row.selectors,
        json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true,
            },
            {
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "cacheable": true,
            }
        ])
    );
    assert_eq!(
        row.record_version_boundary,
        record_version_boundary(
            "basenames:alice.base.eth",
            resource_id,
            Some(1),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1050,
            "0xbase-rec1050",
            1_776_200_050,
            "base-mainnet",
        )
    );
    assert_eq!(
        row.coverage["source_classes_considered"],
        json!([SOURCE_FAMILY_BASENAMES_BASE_RESOLVER])
    );
    assert_eq!(
        row.chain_positions,
        json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 1052,
                "block_hash": "0xbase-rec1052",
                "timestamp": "2026-04-14T20:54:12Z",
            }
        })
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_adds_basenames_transport_position_from_lineage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x980a);
    let resolver_contract_instance_id = Uuid::from_u128(0x980b);
    let resolver_address = "0x00000000000000000000000000000000000000dd";

    insert_basenames_resolver_profile_seed(
        database.pool(),
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;
    seed_basenames_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-rec1055", 1055, 1_776_200_055),
            raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-rec1057", 1057, 1_776_200_057),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[
            raw_log(
                BASE_MAINNET_CHAIN_ID,
                "0xbase-rec1055",
                1055,
                "0xbase-tx1055",
                0,
                resolver_address,
            ),
            raw_log(
                BASE_MAINNET_CHAIN_ID,
                "0xbase-rec1057",
                1057,
                "0xbase-tx1057",
                0,
                resolver_address,
            ),
        ],
    )
    .await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            chain_lineage_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xeth-before-basenames-record",
                21_000_099,
                1_776_200_054,
            ),
            chain_lineage_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xeth-before-later-basenames-record",
                21_000_100,
                1_776_200_056,
            ),
            chain_lineage_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xeth-after-later-basenames-record",
                21_000_101,
                1_776_200_058,
            ),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            basenames_record_version_changed_event(
                "base-boundary-with-transport",
                "basenames:alice.base.eth",
                resource_id,
                21,
                1055,
                0,
            ),
            basenames_record_changed_event(
                "base-record-after-transport-boundary",
                "basenames:alice.base.eth",
                resource_id,
                "text",
                "text",
                None,
                1057,
                0,
            ),
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "basenames:alice.base.eth",
            resource_id,
            Some(1),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1055,
            "0xbase-rec1055",
            1_776_200_055,
            BASE_MAINNET_CHAIN_ID,
        ),
    )
    .await?
    .context("basenames transport-aware record_inventory_current row must exist")?;

    assert_eq!(
        row.chain_positions,
        json!({
            "base": {
                "chain_id": BASE_MAINNET_CHAIN_ID,
                "block_number": 1057,
                "block_hash": "0xbase-rec1057",
                "timestamp": "2026-04-14T20:54:17Z",
            },
            "ethereum": {
                "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                "block_number": 21_000_100,
                "block_hash": "0xeth-before-later-basenames-record",
                "timestamp": "2026-04-14T20:54:16Z",
            },
        })
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_keeps_unadmitted_basenames_dynamic_resolver_inventory_explicit() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9810);
    let resolver_address = "0x0000000000000000000000000000000000009811";

    seed_basenames_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[raw_block(
            "base-mainnet",
            "0xbase-rec1060",
            1060,
            1_776_200_060,
        )],
    )
    .await?;
    seed_events(
        database.pool(),
        &[basenames_resolver_changed_event(
            "base-pending-resolver",
            "basenames:pending.base.eth",
            resource_id,
            resolver_address,
            1060,
            0,
        )],
    )
    .await?;

    let summary =
        rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;
    assert_eq!(summary.requested_resource_count, 1);
    assert_eq!(summary.upserted_row_count, 1);

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "basenames:pending.base.eth",
            resource_id,
            None,
            None,
            1060,
            "0xbase-rec1060",
            1_776_200_060,
            "base-mainnet",
        ),
    )
    .await?
    .context("unadmitted Basenames resolver inventory row must exist")?;

    assert_eq!(row.selectors, json!([]));
    assert_eq!(
        row.unsupported_families,
        json!([
            {
                "record_family": "addr",
                "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            },
            {
                "record_family": "text",
                "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            }
        ])
    );
    assert_eq!(
        row.coverage["unsupported_reason"],
        json!(RESOLVER_FAMILY_PENDING_REASON)
    );
    assert_eq!(
        row.coverage["source_classes_considered"],
        json!([SOURCE_FAMILY_BASENAMES_BASE_REGISTRY])
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_basenames_dynamic_resolver_inventory_gates_supported_pending_and_unsupported_targets()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let supported_resource_id = Uuid::from_u128(0x9820);
    let pending_resource_id = Uuid::from_u128(0x9821);
    let unsupported_resource_id = Uuid::from_u128(0x9822);
    let seed_resolver_contract_instance_id = Uuid::from_u128(0x9823);
    let supported_resolver_contract_instance_id = Uuid::from_u128(0x9824);
    let pending_resolver_contract_instance_id = Uuid::from_u128(0x9825);
    let unsupported_resolver_contract_instance_id = Uuid::from_u128(0x9826);
    let seed_resolver_address = "0x0000000000000000000000000000000000009823";
    let supported_resolver_address = "0x0000000000000000000000000000000000009824";
    let pending_resolver_address = "0x0000000000000000000000000000000000009825";
    let unsupported_resolver_address = "0x0000000000000000000000000000000000009826";

    insert_basenames_dynamic_resolver_profile_fixture(
        database.pool(),
        seed_resolver_contract_instance_id,
        seed_resolver_address,
        &[
            (
                supported_resolver_contract_instance_id,
                supported_resolver_address,
            ),
            (
                pending_resolver_contract_instance_id,
                pending_resolver_address,
            ),
            (
                unsupported_resolver_contract_instance_id,
                unsupported_resolver_address,
            ),
        ],
        &[
            (supported_resolver_address, Some(BASENAMES_L2_CODE_HASH)),
            (pending_resolver_address, None),
            (unsupported_resolver_address, Some(UNSUPPORTED_CODE_HASH)),
        ],
    )
    .await?;
    seed_basenames_resources(
        database.pool(),
        &[
            supported_resource_id,
            pending_resource_id,
            unsupported_resource_id,
        ],
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("base-mainnet", "0xbase-rec1200", 1200, 1_776_200_200),
            raw_block("base-mainnet", "0xbase-rec1201", 1201, 1_776_200_201),
            raw_block("base-mainnet", "0xbase-rec1202", 1202, 1_776_200_202),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[raw_log(
            "base-mainnet",
            "0xbase-rec1201",
            1201,
            "0xbase-tx1201",
            0,
            supported_resolver_address,
        )],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            basenames_resolver_changed_event(
                "base-supported-resolver",
                "basenames:supported.base.eth",
                supported_resource_id,
                supported_resolver_address,
                1200,
                0,
            ),
            basenames_record_changed_event(
                "base-supported-text",
                "basenames:supported.base.eth",
                supported_resource_id,
                "text",
                "text",
                None,
                1201,
                0,
            ),
            basenames_resolver_changed_event(
                "base-pending-resolver",
                "basenames:pending.base.eth",
                pending_resource_id,
                pending_resolver_address,
                1202,
                0,
            ),
            basenames_resolver_changed_event(
                "base-unsupported-resolver",
                "basenames:unsupported.base.eth",
                unsupported_resource_id,
                unsupported_resolver_address,
                1202,
                1,
            ),
        ],
    )
    .await?;

    let summary = rebuild_record_inventory_current(database.pool(), None).await?;
    assert_eq!(summary.requested_resource_count, 3);
    assert_eq!(summary.upserted_row_count, 3);

    let supported_row = load_record_inventory_current(
        database.pool(),
        supported_resource_id,
        &record_version_boundary(
            "basenames:supported.base.eth",
            supported_resource_id,
            None,
            None,
            1200,
            "0xbase-rec1200",
            1_776_200_200,
            "base-mainnet",
        ),
    )
    .await?
    .context("supported Basenames resolver inventory row must exist")?;
    assert_eq!(
        supported_row.selectors,
        json!([{
            "record_key": "text",
            "record_family": "text",
            "selector_key": null,
            "cacheable": true,
        }])
    );
    assert_eq!(supported_row.coverage["unsupported_reason"], Value::Null);
    assert_eq!(
        supported_row.coverage["source_classes_considered"],
        json!([
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
        ])
    );

    for (resource_id, logical_name_id, block_hash) in [
        (
            pending_resource_id,
            "basenames:pending.base.eth",
            "0xbase-rec1202",
        ),
        (
            unsupported_resource_id,
            "basenames:unsupported.base.eth",
            "0xbase-rec1202",
        ),
    ] {
        let row = load_record_inventory_current(
            database.pool(),
            resource_id,
            &record_version_boundary(
                logical_name_id,
                resource_id,
                None,
                None,
                1202,
                block_hash,
                1_776_200_202,
                "base-mainnet",
            ),
        )
        .await?
        .with_context(|| format!("{logical_name_id} inventory row must exist"))?;
        assert_eq!(row.selectors, json!([]));
        assert_eq!(
            row.unsupported_families,
            json!([
                {
                    "record_family": "addr",
                    "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
                },
                {
                    "record_family": "text",
                    "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
                }
            ])
        );
        assert_eq!(
            row.coverage["unsupported_reason"],
            json!(RESOLVER_FAMILY_PENDING_REASON)
        );
        assert_eq!(
            row.last_change
                .as_ref()
                .and_then(|value| value.get("chain_position"))
                .and_then(|value| value.get("block_hash")),
            Some(&json!(block_hash))
        );
        assert_eq!(
            row.last_change
                .as_ref()
                .and_then(|value| value.get("chain_position"))
                .and_then(|value| value.get("block_number")),
            Some(&json!(1202))
        );
        assert_eq!(
            row.last_change
                .as_ref()
                .and_then(|value| value.get("event_kind")),
            Some(&json!(EVENT_KIND_RESOLVER_CHANGED))
        );
        assert_eq!(
            row.last_change
                .as_ref()
                .and_then(|value| value.get("chain_position"))
                .and_then(|value| value.get("timestamp")),
            Some(&json!("2026-04-14T20:56:42Z"))
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_keeps_pending_ensv1_dynamic_resolver_inventory_explicit() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9900);
    let registry_contract_instance_id = Uuid::from_u128(0x9901);
    let public_resolver_contract_instance_id = Uuid::from_u128(0x9902);
    let registry_address = "0x0000000000000000000000000000000000009901";
    let public_resolver_address = "0x0000000000000000000000000000000000009902";
    let pending_resolver_address = "0x0000000000000000000000000000000000009903";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        "ens_v1_registry_l1",
        "manifests/ens/ens_v1_registry_l1/v2.toml",
    )
    .await?;
    let resolver_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
        "manifests/ens/ens_v1_resolver_l1/v1.toml",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        registry_address,
        registry_manifest_id,
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        public_resolver_contract_instance_id,
        public_resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver",
        public_resolver_contract_instance_id,
        public_resolver_address,
    )
    .await?;

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[raw_block(
            "ethereum-mainnet",
            "0xrec1060",
            1060,
            1_776_200_060,
        )],
    )
    .await?;
    seed_events(
        database.pool(),
        &[resolver_changed_event(
            "pending-resolver",
            "ens:pending.eth",
            resource_id,
            pending_resolver_address,
            registry_manifest_id,
            1060,
            0,
        )],
    )
    .await?;

    let summary =
        rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;
    assert_eq!(summary.requested_resource_count, 1);
    assert_eq!(summary.upserted_row_count, 1);

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:pending.eth",
            resource_id,
            None,
            None,
            1060,
            "0xrec1060",
            1_776_200_060,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("pending resolver inventory row must exist")?;

    assert_eq!(row.selectors, json!([]));
    assert_eq!(
        row.explicit_gaps,
        json!([{
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "gap_reason": GAP_REASON_NOT_OBSERVED,
        }])
    );
    assert_eq!(
        row.unsupported_families,
        json!([
            {
                "record_family": "addr",
                "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            },
            {
                "record_family": "text",
                "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            }
        ])
    );
    assert_eq!(
        row.coverage["unsupported_reason"],
        json!(RESOLVER_FAMILY_PENDING_REASON)
    );
    assert_eq!(
        row.last_change
            .as_ref()
            .and_then(|value| value.get("event_kind")),
        Some(&json!(EVENT_KIND_RESOLVER_CHANGED))
    );

    database.cleanup().await
}

async fn seed_resources(database: &PgPool, resource_ids: &[Uuid]) -> Result<()> {
    let resources = resource_ids
        .iter()
        .enumerate()
        .map(|(index, resource_id)| Resource {
            resource_id: *resource_id,
            token_lineage_id: None,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: format!("0xresource{index:02x}"),
            block_number: 30_000_000 + index as i64,
            provenance: json!({
                "source": "worker_record_inventory_current_test",
                "anchor": "resource",
            }),
            canonicality_state: CanonicalityState::Finalized,
        })
        .collect::<Vec<_>>();
    upsert_resources(database, &resources).await?;
    Ok(())
}

async fn seed_basenames_resources(database: &PgPool, resource_ids: &[Uuid]) -> Result<()> {
    let resources = resource_ids
        .iter()
        .enumerate()
        .map(|(index, resource_id)| Resource {
            resource_id: *resource_id,
            token_lineage_id: None,
            chain_id: "base-mainnet".to_owned(),
            block_hash: format!("0xbase-resource{index:02x}"),
            block_number: 40_000_000 + index as i64,
            provenance: json!({
                "source": "worker_record_inventory_current_test",
                "anchor": "basenames_resource",
            }),
            canonicality_state: CanonicalityState::Finalized,
        })
        .collect::<Vec<_>>();
    upsert_resources(database, &resources).await?;
    Ok(())
}

async fn seed_raw_blocks(database: &PgPool, blocks: &[RawBlock]) -> Result<()> {
    upsert_raw_blocks(database, blocks).await?;
    Ok(())
}

async fn seed_raw_logs(database: &PgPool, logs: &[RawLog]) -> Result<()> {
    upsert_raw_logs(database, logs).await?;
    Ok(())
}

async fn seed_events(database: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
    upsert_normalized_events(database, events).await?;
    Ok(())
}

const BASENAMES_L2_CODE_HASH: &str =
    "0x1111111111111111111111111111111111111111111111111111111111111111";
const UNSUPPORTED_CODE_HASH: &str =
    "0x2222222222222222222222222222222222222222222222222222222222222222";

async fn insert_basenames_dynamic_resolver_profile_fixture(
    pool: &PgPool,
    seed_contract_instance_id: Uuid,
    seed_address: &str,
    dynamic_resolvers: &[(Uuid, &str)],
    code_hashes: &[(&str, Option<&str>)],
) -> Result<()> {
    let resolver_manifest_id =
        insert_basenames_resolver_profile_seed(pool, seed_contract_instance_id, seed_address)
            .await?;
    let registry_manifest_id = sqlx::query(
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
        VALUES (
            1,
            'basenames',
            $1,
            'base-mainnet',
            'basenames_v1',
            'active',
            'uts46-v1',
            'manifests/basenames/basenames_base_registry/v1.toml',
            '{}'::jsonb
        )
        RETURNING manifest_id
        "#,
    )
    .bind(SOURCE_FAMILY_BASENAMES_BASE_REGISTRY)
    .fetch_one(pool)
    .await
    .context("failed to insert Basenames registry manifest")?
    .try_get::<i64, _>("manifest_id")
    .context("failed to read Basenames registry manifest_id")?;
    let registry_contract_instance_id = Uuid::from_u128(0x98ff);

    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
        VALUES ($1, 'base-mainnet', 'root', '{}'::jsonb)
        "#,
    )
    .bind(registry_contract_instance_id)
    .execute(pool)
    .await
    .context("failed to insert Basenames registry contract_instance")?;

    for (contract_instance_id, address) in dynamic_resolvers {
        sqlx::query(
            r#"
            INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
            VALUES ($1, 'base-mainnet', 'contract', '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .execute(pool)
        .await
        .context("failed to insert Basenames dynamic resolver contract_instance")?;
        sqlx::query(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                source_manifest_id,
                provenance
            )
            VALUES ($1, 'base-mainnet', lower($2), $3, '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .bind(address)
        .bind(resolver_manifest_id)
        .execute(pool)
        .await
        .context("failed to insert Basenames dynamic resolver contract_instance_address")?;
        sqlx::query(
            r#"
            INSERT INTO discovery_edges (
                chain_id,
                edge_kind,
                from_contract_instance_id,
                to_contract_instance_id,
                discovery_source,
                source_manifest_id,
                admission,
                provenance
            )
            VALUES (
                'base-mainnet',
                'resolver',
                $1,
                $2,
                $3,
                $4,
                'test',
                '{}'::jsonb
            )
            "#,
        )
        .bind(registry_contract_instance_id)
        .bind(contract_instance_id)
        .bind(format!("test:basenames-dynamic-resolver:{address}"))
        .bind(registry_manifest_id)
        .execute(pool)
        .await
        .context("failed to insert Basenames dynamic resolver discovery_edge")?;
    }

    let mut raw_code_hashes = vec![basenames_raw_code_hash(
        seed_address,
        BASENAMES_L2_CODE_HASH,
    )];
    raw_code_hashes.extend(code_hashes.iter().filter_map(|(address, code_hash)| {
        code_hash.map(|code_hash| basenames_raw_code_hash(address, code_hash))
    }));
    upsert_raw_code_hashes(pool, &raw_code_hashes).await?;

    Ok(())
}

async fn insert_basenames_resolver_profile_seed(
    pool: &PgPool,
    contract_instance_id: Uuid,
    address: &str,
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
        VALUES (
            1,
            'basenames',
            $1,
            'base-mainnet',
            'basenames_v1',
            'active',
            'uts46-v1',
            'manifests/basenames/basenames_base_resolver/v1.toml',
            '{}'::jsonb
        )
        RETURNING manifest_id
        "#,
    )
    .bind(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER)
    .fetch_one(pool)
    .await
    .context("failed to insert Basenames resolver manifest")?
    .try_get::<i64, _>("manifest_id")
    .context("failed to read Basenames resolver manifest_id")?;

    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
        VALUES ($1, 'base-mainnet', 'contract', '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await
    .context("failed to insert Basenames resolver contract_instance")?;

    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            source_manifest_id,
            provenance
        )
        VALUES ($1, 'base-mainnet', lower($2), $3, '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(address)
    .bind(manifest_id)
    .execute(pool)
    .await
    .context("failed to insert Basenames resolver contract_instance_address")?;

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
        VALUES ($1, 'contract', 'resolver', $2, lower($3), 'resolver', 'none')
        "#,
    )
    .bind(manifest_id)
    .bind(contract_instance_id)
    .bind(address)
    .execute(pool)
    .await
    .context("failed to insert Basenames resolver manifest_contract_instance")?;

    Ok(manifest_id)
}

async fn insert_manifest_version(
    pool: &PgPool,
    source_family: &str,
    file_path: &str,
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
        VALUES (1, 'ens', $1, 'ethereum-mainnet', 'ens_v1', 'active', 'uts46-v1', $2, '{}'::jsonb)
        RETURNING manifest_id
        "#,
    )
    .bind(source_family)
    .bind(file_path)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to insert manifest_version for {source_family}"))?
    .try_get::<i64, _>("manifest_id")
    .context("failed to read manifest_id")
}

async fn insert_contract_instance(
    pool: &PgPool,
    contract_instance_id: Uuid,
    address: &str,
    source_manifest_id: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
        VALUES ($1, 'ethereum-mainnet', 'contract', '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await
    .context("failed to insert contract_instance")?;

    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            source_manifest_id,
            provenance
        )
        VALUES ($1, 'ethereum-mainnet', lower($2), $3, '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(address)
    .bind(source_manifest_id)
    .execute(pool)
    .await
    .context("failed to insert contract_instance_address")?;

    Ok(())
}

async fn insert_manifest_contract_instance(
    pool: &PgPool,
    manifest_id: i64,
    role: &str,
    contract_instance_id: Uuid,
    address: &str,
) -> Result<()> {
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
        VALUES ($1, 'contract', $2, $3, lower($4), $2, 'none')
        "#,
    )
    .bind(manifest_id)
    .bind(role)
    .bind(contract_instance_id)
    .bind(address)
    .execute(pool)
    .await
    .context("failed to insert manifest_contract_instance")?;
    Ok(())
}

fn raw_block(chain_id: &str, block_hash: &str, block_number: i64, timestamp: i64) -> RawBlock {
    RawBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: Some(format!("0xparent{block_number:08x}")),
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(timestamp)
            .expect("test block timestamp must be valid"),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn chain_lineage_block(
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    timestamp: i64,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: Some(format!("0xparent{block_number:08x}")),
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(timestamp)
            .expect("test lineage timestamp must be valid"),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn raw_log(
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    transaction_hash: &str,
    log_index: i64,
    emitting_address: &str,
) -> RawLog {
    RawLog {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: transaction_hash.to_owned(),
        transaction_index: 0,
        log_index,
        emitting_address: emitting_address.to_owned(),
        topics: vec![],
        data: vec![],
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn basenames_raw_code_hash(address: &str, code_hash: &str) -> RawCodeHash {
    RawCodeHash {
        chain_id: "base-mainnet".to_owned(),
        block_hash: "0xbase-code-hash".to_owned(),
        block_number: 41,
        contract_address: address.to_owned(),
        code_hash: code_hash.to_owned(),
        code_byte_length: 5,
        canonicality_state: CanonicalityState::Finalized,
    }
}

#[allow(clippy::too_many_arguments)]
fn record_changed_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    record_key: &str,
    record_family: &str,
    selector_key: Option<&str>,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: Some(resource_id),
        event_kind: EVENT_KIND_RECORD_CHANGED.to_owned(),
        source_family: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xrec{block_number}")),
        transaction_hash: Some(format!("0xtx{block_number}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_hash": format!("0xrec{block_number}"),
            "log_index": log_index,
        }),
        derivation_kind: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "record_key": record_key,
            "record_family": record_family,
            "selector_key": selector_key,
        }),
    }
}

fn resolver_changed_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    resolver_address: &str,
    source_manifest_id: i64,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: Some(resource_id),
        event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: Some(source_manifest_id),
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xrec{block_number}")),
        transaction_hash: Some(format!("0xtx{block_number}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_hash": format!("0xrec{block_number}"),
            "log_index": log_index,
        }),
        derivation_kind: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "resolver": resolver_address,
            "namehash": format!("namehash:{logical_name_id}"),
        }),
    }
}

fn basenames_resolver_changed_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    resolver_address: &str,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: BASENAMES_NAMESPACE.to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: Some(resource_id),
        event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
        source_family: SOURCE_FAMILY_BASENAMES_BASE_REGISTRY.to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("base-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xbase-rec{block_number}")),
        transaction_hash: Some(format!("0xbase-tx{block_number}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "base-mainnet",
            "block_hash": format!("0xbase-rec{block_number}"),
            "log_index": log_index,
        }),
        derivation_kind: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "resolver": resolver_address,
            "namehash": format!("namehash:{logical_name_id}"),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn basenames_record_changed_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    record_key: &str,
    record_family: &str,
    selector_key: Option<&str>,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "basenames".to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: Some(resource_id),
        event_kind: EVENT_KIND_RECORD_CHANGED.to_owned(),
        source_family: SOURCE_FAMILY_BASENAMES_BASE_RESOLVER.to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("base-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xbase-rec{block_number}")),
        transaction_hash: Some(format!("0xbase-tx{block_number}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "base-mainnet",
            "block_hash": format!("0xbase-rec{block_number}"),
            "log_index": log_index,
        }),
        derivation_kind: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "record_key": record_key,
            "record_family": record_family,
            "selector_key": selector_key,
        }),
    }
}

fn record_version_changed_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    record_version: i64,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: Some(resource_id),
        event_kind: EVENT_KIND_RECORD_VERSION_CHANGED.to_owned(),
        source_family: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xrec{block_number}")),
        transaction_hash: Some(format!("0xtx{block_number}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_hash": format!("0xrec{block_number}"),
            "log_index": log_index,
        }),
        derivation_kind: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({
            "record_version": record_version - 1,
        }),
        after_state: json!({
            "record_version": record_version,
        }),
    }
}

fn basenames_record_version_changed_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    record_version: i64,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "basenames".to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: Some(resource_id),
        event_kind: EVENT_KIND_RECORD_VERSION_CHANGED.to_owned(),
        source_family: SOURCE_FAMILY_BASENAMES_BASE_RESOLVER.to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("base-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xbase-rec{block_number}")),
        transaction_hash: Some(format!("0xbase-tx{block_number}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "base-mainnet",
            "block_hash": format!("0xbase-rec{block_number}"),
            "log_index": log_index,
        }),
        derivation_kind: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
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
fn record_version_boundary(
    logical_name_id: &str,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
    block_number: i64,
    block_hash: &str,
    timestamp: i64,
    chain_id: &str,
) -> Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": {
            "chain_id": chain_id,
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": format_timestamp(
                OffsetDateTime::from_unix_timestamp(timestamp)
                    .expect("test timestamp must be valid"),
            ),
        }
    })
}
