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
    let missing_resource = Uuid::from_u128(0x9201);

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
            record_changed_event(
                "missing-resource-text",
                "ens:missing.eth",
                missing_resource,
                "text",
                "text",
                None,
                1003,
                1,
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
            .contains("record event must have a chain_lineage timestamp for chain_position")
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
    let mut contenthash_record = record_changed_event(
        "contenthash",
        "ens:alice.eth",
        resource_id,
        "contenthash",
        "contenthash",
        None,
        1021,
        1,
    );
    contenthash_record.after_state["contenthash_hex"] = json!("0xe30101701220");

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
            contenthash_record,
            record_changed_event(
                "unsupported-avatar",
                "ens:alice.eth",
                resource_id,
                "avatar",
                "avatar",
                None,
                1021,
                2,
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
        json!([
            {
                "record_key": "addr:61",
                "record_family": "addr",
                "selector_key": "61",
                "cacheable": true,
            },
            {
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "cacheable": true,
            }
        ])
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
    assert_eq!(
        row.entries,
        json!([
            {
                "record_key": "addr:61",
                "record_family": "addr",
                "selector_key": "61",
                "status": "unsupported",
                "unsupported_reason": CACHE_UNSUPPORTED_REASON_VALUE_NOT_RETAINED,
            },
            {
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "status": "success",
                "value": {
                    "encoding": "hex",
                    "bytes": "0xe30101701220",
                },
            }
        ])
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_retains_addr_and_contenthash_values_from_event_payloads() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9510);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1025", 1025, 1_776_200_025),
            raw_block("ethereum-mainnet", "0xrec1026", 1026, 1_776_200_026),
        ],
    )
    .await?;

    // Mirror the real adapter payloads: text retains `value`; addr/contenthash carry their raw
    // payload in `address_bytes_hex`/`contenthash_hex` with no `value` key.
    let mut addr_event = record_changed_event(
        "addr-bytes",
        "ens:retained.eth",
        resource_id,
        "addr:60",
        "addr",
        Some("60"),
        1026,
        0,
    );
    addr_event.after_state["value_retained"] = json!(false);
    addr_event.after_state["address_bytes_hex"] =
        json!("0x4ce0fb7f128b9f1c6f8f8d5144203801936027b8");
    let mut contenthash_event = record_changed_event(
        "contenthash-bytes",
        "ens:retained.eth",
        resource_id,
        "contenthash",
        "contenthash",
        None,
        1026,
        1,
    );
    contenthash_event.after_state["value_retained"] = json!(false);
    contenthash_event.after_state["contenthash_hex"] = json!("0xe30101701220aabbccdd");
    let mut text_event = record_changed_event(
        "text-value",
        "ens:retained.eth",
        resource_id,
        "text:avatar",
        "text",
        Some("avatar"),
        1026,
        2,
    );
    text_event.after_state["value_retained"] = json!(true);
    text_event.after_state["value"] = json!("https://example.com/a.png");

    seed_events(
        database.pool(),
        &[
            record_version_changed_event(
                "retained-boundary",
                "ens:retained.eth",
                resource_id,
                9,
                1025,
                0,
            ),
            addr_event,
            contenthash_event,
            text_event,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:retained.eth",
            resource_id,
            Some(1),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1025,
            "0xrec1025",
            1_776_200_025,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("row must exist")?;

    // Contenthash is a supported selector family; nothing lands in unsupported_families.
    assert_eq!(
        row.selectors,
        json!([
            {"record_key": "addr:60", "record_family": "addr", "selector_key": "60", "cacheable": true},
            {"record_key": "contenthash", "record_family": "contenthash", "selector_key": null, "cacheable": true},
            {"record_key": "text:avatar", "record_family": "text", "selector_key": "avatar", "cacheable": true},
        ])
    );
    assert_eq!(row.unsupported_families, json!([]));
    // All three entries retain values: text verbatim, addr/contenthash as their raw payload hex.
    assert_eq!(
        row.entries,
        json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "success",
                "value": "0x4ce0fb7f128b9f1c6f8f8d5144203801936027b8",
            },
            {
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "status": "success",
                "value": {
                    "encoding": "hex",
                    "bytes": "0xe30101701220aabbccdd",
                },
            },
            {
                "record_key": "text:avatar",
                "record_family": "text",
                "selector_key": "avatar",
                "status": "success",
                "value": "https://example.com/a.png",
            },
        ])
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
        json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "gap_reason": GAP_REASON_NOT_OBSERVED,
            },
            {
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "gap_reason": GAP_REASON_NOT_OBSERVED,
            }
        ])
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_retains_selector_specific_text_record_values() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9702);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1045", 1045, 1_776_200_045),
            raw_block("ethereum-mainnet", "0xrec1046", 1046, 1_776_200_046),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            record_version_changed_event("boundary", "ens:alice.eth", resource_id, 14, 1045, 0),
            record_changed_event_with_value(
                "avatar",
                "ens:alice.eth",
                resource_id,
                "text:avatar",
                "text",
                Some("avatar"),
                json!("https://euc.li/alice.eth"),
                1046,
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
            1045,
            "0xrec1045",
            1_776_200_045,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("row must exist")?;

    assert_eq!(
        row.selectors,
        json!([{
            "record_key": "text:avatar",
            "record_family": "text",
            "selector_key": "avatar",
            "cacheable": true,
        }])
    );
    assert_eq!(
        row.entries,
        json!([{
            "record_key": "text:avatar",
            "record_family": "text",
            "selector_key": "avatar",
            "status": "success",
            "value": "https://euc.li/alice.eth",
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
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "gap_reason": GAP_REASON_NOT_OBSERVED,
            }
        ])
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_treats_empty_contenthash_bytes_as_not_found() -> Result<()> {
    let database = TestDatabase::new().await?;
    let contenthash_hex_resource_id = Uuid::from_u128(0x9703);
    let retained_value_resource_id = Uuid::from_u128(0x9704);
    let mut non_empty_contenthash_hex = record_changed_event(
        "contenthash-hex-set",
        "ens:contenthash-hex.eth",
        contenthash_hex_resource_id,
        "contenthash",
        "contenthash",
        None,
        1047,
        0,
    );
    non_empty_contenthash_hex.after_state["contenthash_hex"] = json!("0xe30101701220");
    let mut empty_contenthash_hex = record_changed_event(
        "contenthash-hex-clear",
        "ens:contenthash-hex.eth",
        contenthash_hex_resource_id,
        "contenthash",
        "contenthash",
        None,
        1048,
        0,
    );
    empty_contenthash_hex.after_state["contenthash_hex"] = json!("0x");

    seed_resources(
        database.pool(),
        &[contenthash_hex_resource_id, retained_value_resource_id],
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1046", 1046, 1_776_200_046),
            raw_block("ethereum-mainnet", "0xrec1047", 1047, 1_776_200_047),
            raw_block("ethereum-mainnet", "0xrec1048", 1048, 1_776_200_048),
            raw_block("ethereum-mainnet", "0xrec1049", 1049, 1_776_200_049),
            raw_block("ethereum-mainnet", "0xrec1050", 1050, 1_776_200_050),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            record_version_changed_event(
                "contenthash-hex-boundary",
                "ens:contenthash-hex.eth",
                contenthash_hex_resource_id,
                15,
                1046,
                0,
            ),
            record_version_changed_event(
                "contenthash-value-boundary",
                "ens:contenthash-value.eth",
                retained_value_resource_id,
                16,
                1046,
                1,
            ),
            non_empty_contenthash_hex,
            empty_contenthash_hex,
            record_changed_event_with_value(
                "contenthash-value-set",
                "ens:contenthash-value.eth",
                retained_value_resource_id,
                "contenthash",
                "contenthash",
                None,
                json!({
                    "encoding": "hex",
                    "bytes": "0xe30101701220",
                }),
                1049,
                0,
            ),
            record_changed_event_with_value(
                "contenthash-value-clear",
                "ens:contenthash-value.eth",
                retained_value_resource_id,
                "contenthash",
                "contenthash",
                None,
                json!({
                    "encoding": "hex",
                    "bytes": "",
                }),
                1050,
                0,
            ),
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), None).await?;

    let contenthash_hex_row = load_record_inventory_current(
        database.pool(),
        contenthash_hex_resource_id,
        &record_version_boundary(
            "ens:contenthash-hex.eth",
            contenthash_hex_resource_id,
            Some(1),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1046,
            "0xrec1046",
            1_776_200_046,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("contenthash_hex row must exist")?;
    assert_eq!(
        contenthash_hex_row.entries,
        json!([{
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "status": "not_found",
        }])
    );

    let retained_value_row = load_record_inventory_current(
        database.pool(),
        retained_value_resource_id,
        &record_version_boundary(
            "ens:contenthash-value.eth",
            retained_value_resource_id,
            Some(2),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1046,
            "0xrec1046",
            1_776_200_046,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("retained value row must exist")?;
    assert_eq!(
        retained_value_row.entries,
        json!([{
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "status": "not_found",
        }])
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_treats_empty_addr_bytes_as_not_found() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9705);

    // Set a coin address, then clear it with empty bytes (`0x`) — ENS's deletion signal. The
    // cleared record must project as `not_found`, not `success("0x")`.
    let mut set_addr = record_changed_event(
        "addr-set",
        "ens:cleared-addr.eth",
        resource_id,
        "addr:60",
        "addr",
        Some("60"),
        1052,
        0,
    );
    set_addr.after_state["value_retained"] = json!(false);
    set_addr.after_state["address_bytes_hex"] = json!("0x4ce0fb7f128b9f1c6f8f8d5144203801936027b8");
    let mut clear_addr = record_changed_event(
        "addr-clear",
        "ens:cleared-addr.eth",
        resource_id,
        "addr:60",
        "addr",
        Some("60"),
        1053,
        0,
    );
    clear_addr.after_state["value_retained"] = json!(false);
    clear_addr.after_state["address_bytes_hex"] = json!("0x");

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1051", 1051, 1_776_200_051),
            raw_block("ethereum-mainnet", "0xrec1052", 1052, 1_776_200_052),
            raw_block("ethereum-mainnet", "0xrec1053", 1053, 1_776_200_053),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            record_version_changed_event(
                "cleared-addr-boundary",
                "ens:cleared-addr.eth",
                resource_id,
                17,
                1051,
                0,
            ),
            set_addr,
            clear_addr,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:cleared-addr.eth",
            resource_id,
            Some(1),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1051,
            "0xrec1051",
            1_776_200_051,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("cleared addr row must exist")?;

    assert_eq!(
        row.entries,
        json!([{
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "not_found",
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
    insert_basenames_execution_manifest(database.pool()).await?;
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
async fn rebuild_omits_basenames_transport_position_without_execution_manifest() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x980c);
    let resolver_contract_instance_id = Uuid::from_u128(0x980d);
    let resolver_address = "0x00000000000000000000000000000000000000ee";

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
            raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-rec1065", 1065, 1_776_200_065),
            raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-rec1067", 1067, 1_776_200_067),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[
            raw_log(
                BASE_MAINNET_CHAIN_ID,
                "0xbase-rec1065",
                1065,
                "0xbase-tx1065",
                0,
                resolver_address,
            ),
            raw_log(
                BASE_MAINNET_CHAIN_ID,
                "0xbase-rec1067",
                1067,
                "0xbase-tx1067",
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
                "0xeth-before-unadmitted-basenames-record",
                21_000_200,
                1_776_200_066,
            ),
            chain_lineage_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xeth-after-unadmitted-basenames-record",
                21_000_201,
                1_776_200_068,
            ),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            basenames_record_version_changed_event(
                "base-boundary-without-transport",
                "basenames:bob.base.eth",
                resource_id,
                31,
                1065,
                0,
            ),
            basenames_record_changed_event(
                "base-record-after-unadmitted-transport-boundary",
                "basenames:bob.base.eth",
                resource_id,
                "text",
                "text",
                None,
                1067,
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
            "basenames:bob.base.eth",
            resource_id,
            Some(1),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1065,
            "0xbase-rec1065",
            1_776_200_065,
            BASE_MAINNET_CHAIN_ID,
        ),
    )
    .await?
    .context("basenames transport-gated record_inventory_current row must exist")?;

    assert_eq!(
        row.chain_positions,
        json!({
            "base": {
                "chain_id": BASE_MAINNET_CHAIN_ID,
                "block_number": 1067,
                "block_hash": "0xbase-rec1067",
                "timestamp": "2026-04-14T20:54:27Z",
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
                "record_family": "contenthash",
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
async fn rebuild_keeps_newer_record_version_boundary_for_pending_resolver() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9815);
    let resolver_address = "0x0000000000000000000000000000000000009816";

    seed_basenames_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("base-mainnet", "0xbase-rec1070", 1070, 1_776_200_070),
            raw_block("base-mainnet", "0xbase-rec1071", 1071, 1_776_200_071),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            basenames_resolver_changed_event(
                "base-pending-resolver-before-boundary",
                "basenames:pending-boundary.base.eth",
                resource_id,
                resolver_address,
                1070,
                0,
            ),
            basenames_record_version_changed_event(
                "base-newer-record-version-boundary",
                "basenames:pending-boundary.base.eth",
                resource_id,
                2,
                1071,
                0,
            ),
        ],
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
            "basenames:pending-boundary.base.eth",
            resource_id,
            Some(2),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            1071,
            "0xbase-rec1071",
            1_776_200_071,
            "base-mainnet",
        ),
    )
    .await?
    .context("pending resolver row must keep the newer record-version boundary")?;

    assert_eq!(
        row.last_change
            .as_ref()
            .and_then(|value| value.get("event_kind")),
        Some(&json!(EVENT_KIND_RECORD_VERSION_CHANGED))
    );
    assert_eq!(
        row.chain_positions.pointer("/base/block_hash"),
        Some(&json!("0xbase-rec1071"))
    );
    assert_eq!(
        row.unsupported_families,
        json!([
            {
                "record_family": "addr",
                "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            },
            {
                "record_family": "contenthash",
                "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            },
            {
                "record_family": "text",
                "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            }
        ])
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

    for (resource_id, logical_name_id, block_hash, unsupported_reason) in [
        (
            pending_resource_id,
            "basenames:pending.base.eth",
            "0xbase-rec1202",
            RESOLVER_FAMILY_PENDING_REASON,
        ),
        (
            unsupported_resource_id,
            "basenames:unsupported.base.eth",
            "0xbase-rec1202",
            RESOLVER_FAMILY_UNSUPPORTED_REASON,
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
                    "unsupported_reason": unsupported_reason,
                },
                {
                    "record_family": "contenthash",
                    "unsupported_reason": unsupported_reason,
                },
                {
                    "record_family": "text",
                    "unsupported_reason": unsupported_reason,
                }
            ])
        );
        assert_eq!(
            row.coverage["unsupported_reason"],
            json!(unsupported_reason)
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
    assert_eq!(row.explicit_gaps, json!([]));
    assert_eq!(
        row.unsupported_families,
        json!([
            {
                "record_family": "addr",
                "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            },
            {
                "record_family": "contenthash",
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

#[tokio::test]
async fn rebuild_keeps_observed_addr_record_for_unknown_ensv1_current_resolver() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9905);
    let registry_contract_instance_id = Uuid::from_u128(0x9906);
    let registry_address = "0x0000000000000000000000000000000000009906";
    let unknown_resolver_address = "0x0000000000000000000000000000000000009907";

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

    let mut addr_record = record_changed_event(
        "unknown-resolver-addr",
        "ens:unknown-resolver.eth",
        resource_id,
        "addr:60",
        "addr",
        Some("60"),
        1061,
        0,
    );
    addr_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    addr_record.source_manifest_id = Some(resolver_manifest_id);
    addr_record.after_state.as_object_mut().unwrap().insert(
        "value".to_owned(),
        json!({
            "coin_type": "60",
            "value": "0x0000000000000000000000000000000000009907",
        }),
    );
    let mut data_record = record_changed_event(
        "unknown-resolver-data",
        "ens:unknown-resolver.eth",
        resource_id,
        "data:avatar",
        "data",
        Some("avatar"),
        1061,
        1,
    );
    data_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    data_record.source_manifest_id = Some(resolver_manifest_id);
    data_record.after_state.as_object_mut().unwrap().insert(
        "value".to_owned(),
        json!({
            "indexed_data_hash": "0x0000000000000000000000000000000000000000000000000000000000009908",
        }),
    );

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1060", 1060, 1_776_200_060),
            raw_block("ethereum-mainnet", "0xrec1061", 1061, 1_776_200_061),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[
            raw_log(
                "ethereum-mainnet",
                "0xrec1061",
                1061,
                "0xtx1061",
                0,
                unknown_resolver_address,
            ),
            raw_log(
                "ethereum-mainnet",
                "0xrec1061",
                1061,
                "0xtx1061",
                1,
                unknown_resolver_address,
            ),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            resolver_changed_event(
                "unknown-current-resolver",
                "ens:unknown-resolver.eth",
                resource_id,
                unknown_resolver_address,
                registry_manifest_id,
                1060,
                0,
            ),
            addr_record,
            data_record,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:unknown-resolver.eth",
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
    .context("unknown current resolver row with observed addr event must exist")?;

    assert_eq!(
        row.selectors,
        json!([{
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "cacheable": true,
        }])
    );
    assert_eq!(
        row.entries,
        json!([{
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000009907",
            }
        }])
    );
    assert_eq!(row.explicit_gaps, json!([]));
    assert_eq!(
        row.unsupported_families,
        json!([
            {
                "record_family": "addr",
                "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            },
            {
                "record_family": "contenthash",
                "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            },
            {
                "record_family": "data",
                "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            },
            {
                "record_family": "text",
                "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            }
        ])
    );
    assert_eq!(row.coverage["status"], json!("partial"));
    assert_eq!(
        row.coverage["unsupported_reason"],
        json!(RESOLVER_FAMILY_PENDING_REASON)
    );
    assert_eq!(row.provenance["normalized_event_ids"], json!([1, 2, 3]));

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_surfaces_dataresolver_and_ignores_pubkey_for_known_ensv1_resolver() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9909);
    let registry_contract_instance_id = Uuid::from_u128(0x990a);
    let public_resolver_contract_instance_id = Uuid::from_u128(0x990b);
    let registry_address = "0x000000000000000000000000000000000000990a";
    let public_resolver_address = "0x000000000000000000000000000000000000990b";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        "ens_v1_registry_l1",
        "manifests/ens/ens_v1_registry_l1/v3.toml",
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

    let mut data_record = record_changed_event(
        "known-resolver-data",
        "ens:dataresolver.eth",
        resource_id,
        "data:avatar",
        "data",
        Some("avatar"),
        1061,
        0,
    );
    data_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    data_record.source_manifest_id = Some(resolver_manifest_id);
    data_record.after_state.as_object_mut().unwrap().insert(
        "value".to_owned(),
        json!({
            "indexed_data_hash": "0x000000000000000000000000000000000000000000000000000000000000990c",
        }),
    );
    let mut pubkey_record = record_changed_event(
        "known-resolver-pubkey",
        "ens:dataresolver.eth",
        resource_id,
        "pubkey",
        "pubkey",
        None,
        1061,
        1,
    );
    pubkey_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    pubkey_record.source_manifest_id = Some(resolver_manifest_id);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1060", 1060, 1_776_200_060),
            raw_block("ethereum-mainnet", "0xrec1061", 1061, 1_776_200_061),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[
            raw_log(
                "ethereum-mainnet",
                "0xrec1061",
                1061,
                "0xtx1061",
                0,
                public_resolver_address,
            ),
            raw_log(
                "ethereum-mainnet",
                "0xrec1061",
                1061,
                "0xtx1061",
                1,
                public_resolver_address,
            ),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            resolver_changed_event(
                "known-current-resolver",
                "ens:dataresolver.eth",
                resource_id,
                public_resolver_address,
                registry_manifest_id,
                1060,
                0,
            ),
            data_record,
            pubkey_record,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:dataresolver.eth",
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
    .context("known current resolver row with DataResolver event must exist")?;

    assert_eq!(row.selectors, json!([]));
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
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
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
            "record_family": "data",
            "unsupported_reason": RESOLVER_FAMILY_UNSUPPORTED_REASON,
        }])
    );
    assert_eq!(row.entries, json!([]));
    assert_eq!(row.provenance["normalized_event_ids"], json!([1, 2]));
    assert_eq!(
        row.last_change
            .as_ref()
            .and_then(|value| value.get("event_kind")),
        Some(&json!(EVENT_KIND_RECORD_CHANGED))
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_supports_known_legacy_ensv1_resolver_without_latest_capabilities() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9910);
    let registry_contract_instance_id = Uuid::from_u128(0x9911);
    let legacy_resolver_contract_instance_id = Uuid::from_u128(0x9912);
    let registry_address = "0x0000000000000000000000000000000000009911";
    let legacy_resolver_address = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        "ens_v1_registry_l1",
        "manifests/ens/ens_v1_registry_l1/v3.toml",
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
        legacy_resolver_contract_instance_id,
        legacy_resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver_4976fb03",
        legacy_resolver_contract_instance_id,
        legacy_resolver_address,
    )
    .await?;

    let mut addr_record = record_changed_event(
        "legacy-addr",
        "ens:taytems.eth",
        resource_id,
        "addr:60",
        "addr",
        Some("60"),
        1071,
        0,
    );
    addr_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    addr_record.source_manifest_id = Some(resolver_manifest_id);
    let mut text_record = record_changed_event(
        "legacy-text",
        "ens:taytems.eth",
        resource_id,
        "text",
        "text",
        None,
        1072,
        0,
    );
    text_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    text_record.source_manifest_id = Some(resolver_manifest_id);
    let mut name_record = record_changed_event(
        "legacy-name",
        "ens:taytems.eth",
        resource_id,
        "name",
        "name",
        None,
        1073,
        0,
    );
    name_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    name_record.source_manifest_id = Some(resolver_manifest_id);
    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1070", 1070, 1_776_200_070),
            raw_block("ethereum-mainnet", "0xrec1071", 1071, 1_776_200_071),
            raw_block("ethereum-mainnet", "0xrec1072", 1072, 1_776_200_072),
            raw_block("ethereum-mainnet", "0xrec1073", 1073, 1_776_200_073),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            resolver_changed_event(
                "legacy-resolver",
                "ens:taytems.eth",
                resource_id,
                legacy_resolver_address,
                registry_manifest_id,
                1070,
                0,
            ),
            addr_record,
            text_record,
            name_record,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:taytems.eth",
            resource_id,
            None,
            None,
            1070,
            "0xrec1070",
            1_776_200_070,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("legacy resolver row must exist")?;

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
        json!([{
            "record_family": "name",
            "unsupported_reason": UNSUPPORTED_FAMILY_REASON,
        }])
    );
    assert_eq!(row.coverage.get("unsupported_reason"), Some(&json!(null)));
    assert_eq!(row.record_version_boundary["event_kind"], json!(null));
    assert_eq!(row.provenance["normalized_event_ids"], json!([1, 2, 3, 4]));

    let legacy_resolver_address = legacy_resolver_address.to_ascii_lowercase();
    let admissions =
        bigname_manifests::load_ens_v1_public_resolver_profile_admissions(database.pool()).await?;
    let feature_statuses = admissions
        .iter()
        .filter(|admission| admission.address == legacy_resolver_address)
        .map(|admission| (admission.fact_family.as_str(), admission.status.as_str()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(feature_statuses["resolver_record_version"], "unsupported");
    assert_eq!(
        feature_statuses["resolver_feature:name_wrapper_aware"],
        "unsupported"
    );
    assert_eq!(
        feature_statuses["resolver_feature:default_coin_type"],
        "unsupported"
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_uses_ensv1_registrar_resolver_binding_without_raw_logs() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9915);
    let resolver_contract_instance_id = Uuid::from_u128(0x9916);
    let resolver_address = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41";

    let registrar_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
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
        resolver_contract_instance_id,
        resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver_4976fb03",
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;

    let mut resolver_event = resolver_changed_event(
        "registrar-resolver",
        "ens:taytems.eth",
        resource_id,
        resolver_address,
        registrar_manifest_id,
        1075,
        0,
    );
    resolver_event.source_family = SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned();
    let mut text_record = record_changed_event_with_value(
        "registrar-resolver-avatar",
        "ens:taytems.eth",
        resource_id,
        "text:avatar",
        "text",
        Some("avatar"),
        json!("https://euc.li/taytems.eth"),
        1076,
        0,
    );
    text_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    text_record.source_manifest_id = Some(resolver_manifest_id);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1075", 1075, 1_776_200_075),
            raw_block("ethereum-mainnet", "0xrec1076", 1076, 1_776_200_076),
        ],
    )
    .await?;
    seed_events(database.pool(), &[resolver_event, text_record]).await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:taytems.eth",
            resource_id,
            None,
            None,
            1075,
            "0xrec1075",
            1_776_200_075,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("registrar resolver row must exist")?;

    assert_eq!(
        row.selectors,
        json!([{
            "record_key": "text:avatar",
            "record_family": "text",
            "selector_key": "avatar",
            "cacheable": true,
        }])
    );
    assert_eq!(
        row.entries,
        json!([{
            "record_key": "text:avatar",
            "record_family": "text",
            "selector_key": "avatar",
            "status": "success",
            "value": "https://euc.li/taytems.eth",
        }])
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_keeps_records_across_same_resolver_refresh() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9923);
    let resolver_contract_instance_id = Uuid::from_u128(0x9924);
    let resolver_address = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "manifests/ens/ens_v1_registry_l1/v3.toml",
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
        resolver_contract_instance_id,
        resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver_4976fb03",
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;

    let mut email_record = record_changed_event_with_value(
        "same-resolver-email",
        "ens:taytems.eth",
        resource_id,
        "text:email",
        "text",
        Some("email"),
        json!("hello@taytems.xyz"),
        1071,
        0,
    );
    email_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    email_record.source_manifest_id = Some(resolver_manifest_id);
    let mut twitter_record = record_changed_event_with_value(
        "same-resolver-twitter",
        "ens:taytems.eth",
        resource_id,
        "text:com.twitter",
        "text",
        Some("com.twitter"),
        json!("taytems"),
        1073,
        0,
    );
    twitter_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    twitter_record.source_manifest_id = Some(resolver_manifest_id);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1070", 1070, 1_776_200_070),
            raw_block("ethereum-mainnet", "0xrec1071", 1071, 1_776_200_071),
            raw_block("ethereum-mainnet", "0xrec1072", 1072, 1_776_200_072),
            raw_block("ethereum-mainnet", "0xrec1073", 1073, 1_776_200_073),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[
            raw_log(
                "ethereum-mainnet",
                "0xrec1071",
                1071,
                "0xtx1071",
                0,
                resolver_address,
            ),
            raw_log(
                "ethereum-mainnet",
                "0xrec1073",
                1073,
                "0xtx1073",
                0,
                resolver_address,
            ),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            resolver_changed_event(
                "same-resolver-initial",
                "ens:taytems.eth",
                resource_id,
                resolver_address,
                registry_manifest_id,
                1070,
                0,
            ),
            email_record,
            resolver_changed_event(
                "same-resolver-refresh",
                "ens:taytems.eth",
                resource_id,
                resolver_address,
                registry_manifest_id,
                1072,
                0,
            ),
            twitter_record,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:taytems.eth",
            resource_id,
            None,
            None,
            1072,
            "0xrec1072",
            1_776_200_072,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("same-resolver refresh row must exist")?;

    assert_eq!(
        row.selectors,
        json!([
            {
                "record_key": "text:com.twitter",
                "record_family": "text",
                "selector_key": "com.twitter",
                "cacheable": true,
            },
            {
                "record_key": "text:email",
                "record_family": "text",
                "selector_key": "email",
                "cacheable": true,
            }
        ])
    );
    assert_eq!(
        row.entries,
        json!([
            {
                "record_key": "text:com.twitter",
                "record_family": "text",
                "selector_key": "com.twitter",
                "status": "success",
                "value": "taytems",
            },
            {
                "record_key": "text:email",
                "record_family": "text",
                "selector_key": "email",
                "status": "success",
                "value": "hello@taytems.xyz",
            }
        ])
    );
    assert_eq!(row.provenance["normalized_event_ids"], json!([1, 2, 3, 4]));

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_drops_records_from_prior_same_address_resolver_tenure() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9928);
    let resolver_a_contract_instance_id = Uuid::from_u128(0x9929);
    let resolver_b_contract_instance_id = Uuid::from_u128(0x9930);
    let resolver_a_address = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41";
    let resolver_b_address = "0x0000000000000000000000000000000000009930";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "manifests/ens/ens_v1_registry_l1/v3.toml",
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
        resolver_a_contract_instance_id,
        resolver_a_address,
        resolver_manifest_id,
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_b_contract_instance_id,
        resolver_b_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver_4976fb03",
        resolver_a_contract_instance_id,
        resolver_a_address,
    )
    .await?;

    let mut stale_email_record = record_changed_event_with_value(
        "prior-tenure-email",
        "ens:taytems.eth",
        resource_id,
        "text:email",
        "text",
        Some("email"),
        json!("old@taytems.xyz"),
        1091,
        0,
    );
    stale_email_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    stale_email_record.source_manifest_id = Some(resolver_manifest_id);
    let mut current_twitter_record = record_changed_event_with_value(
        "current-tenure-twitter",
        "ens:taytems.eth",
        resource_id,
        "text:com.twitter",
        "text",
        Some("com.twitter"),
        json!("taytems"),
        1095,
        0,
    );
    current_twitter_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    current_twitter_record.source_manifest_id = Some(resolver_manifest_id);
    let mut current_github_record = record_changed_event_with_value(
        "current-tenure-github",
        "ens:taytems.eth",
        resource_id,
        "text:com.github",
        "text",
        Some("com.github"),
        json!("taytems"),
        1097,
        0,
    );
    current_github_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    current_github_record.source_manifest_id = Some(resolver_manifest_id);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1090", 1090, 1_776_200_090),
            raw_block("ethereum-mainnet", "0xrec1091", 1091, 1_776_200_091),
            raw_block("ethereum-mainnet", "0xrec1092", 1092, 1_776_200_092),
            raw_block("ethereum-mainnet", "0xrec1094", 1094, 1_776_200_094),
            raw_block("ethereum-mainnet", "0xrec1095", 1095, 1_776_200_095),
            raw_block("ethereum-mainnet", "0xrec1096", 1096, 1_776_200_096),
            raw_block("ethereum-mainnet", "0xrec1097", 1097, 1_776_200_097),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[
            raw_log(
                "ethereum-mainnet",
                "0xrec1091",
                1091,
                "0xtx1091",
                0,
                resolver_a_address,
            ),
            raw_log(
                "ethereum-mainnet",
                "0xrec1095",
                1095,
                "0xtx1095",
                0,
                resolver_a_address,
            ),
            raw_log(
                "ethereum-mainnet",
                "0xrec1097",
                1097,
                "0xtx1097",
                0,
                resolver_a_address,
            ),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            resolver_changed_event(
                "initial-resolver-a",
                "ens:taytems.eth",
                resource_id,
                resolver_a_address,
                registry_manifest_id,
                1090,
                0,
            ),
            stale_email_record,
            resolver_changed_event(
                "resolver-b",
                "ens:taytems.eth",
                resource_id,
                resolver_b_address,
                registry_manifest_id,
                1092,
                0,
            ),
            resolver_changed_event(
                "resolver-a-again",
                "ens:taytems.eth",
                resource_id,
                resolver_a_address,
                registry_manifest_id,
                1094,
                0,
            ),
            current_twitter_record,
            resolver_changed_event(
                "resolver-a-refresh",
                "ens:taytems.eth",
                resource_id,
                resolver_a_address,
                registry_manifest_id,
                1096,
                0,
            ),
            current_github_record,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:taytems.eth",
            resource_id,
            None,
            None,
            1096,
            "0xrec1096",
            1_776_200_096,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("current resolver tenure row must exist")?;

    assert_eq!(
        row.selectors,
        json!([
            {
                "record_key": "text:com.github",
                "record_family": "text",
                "selector_key": "com.github",
                "cacheable": true,
            },
            {
                "record_key": "text:com.twitter",
                "record_family": "text",
                "selector_key": "com.twitter",
                "cacheable": true,
            }
        ])
    );
    assert_eq!(
        row.entries,
        json!([
            {
                "record_key": "text:com.github",
                "record_family": "text",
                "selector_key": "com.github",
                "status": "success",
                "value": "taytems",
            },
            {
                "record_key": "text:com.twitter",
                "record_family": "text",
                "selector_key": "com.twitter",
                "status": "success",
                "value": "taytems",
            }
        ])
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_does_not_pull_future_resolver_records_from_successor_resource() -> Result<()> {
    let database = TestDatabase::new().await?;
    let predecessor_resource_id = Uuid::from_u128(0x992a);
    let successor_resource_id = Uuid::from_u128(0x992b);
    let resolver_contract_instance_id = Uuid::from_u128(0x992c);
    let resolver_address = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "manifests/ens/ens_v1_registry_l1/v3.toml",
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
        resolver_contract_instance_id,
        resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver_4976fb03",
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;

    let mut predecessor_email_record = record_changed_event_with_value(
        "predecessor-email",
        "ens:taytems.eth",
        predecessor_resource_id,
        "text:email",
        "text",
        Some("email"),
        json!("hello@taytems.xyz"),
        1099,
        0,
    );
    predecessor_email_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    predecessor_email_record.source_manifest_id = Some(resolver_manifest_id);
    let mut successor_twitter_record = record_changed_event_with_value(
        "successor-twitter",
        "ens:taytems.eth",
        successor_resource_id,
        "text:com.twitter",
        "text",
        Some("com.twitter"),
        json!("taytems"),
        1101,
        0,
    );
    successor_twitter_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    successor_twitter_record.source_manifest_id = Some(resolver_manifest_id);

    seed_resources(
        database.pool(),
        &[predecessor_resource_id, successor_resource_id],
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1098", 1098, 1_776_200_098),
            raw_block("ethereum-mainnet", "0xrec1099", 1099, 1_776_200_099),
            raw_block("ethereum-mainnet", "0xrec1100", 1100, 1_776_200_100),
            raw_block("ethereum-mainnet", "0xrec1101", 1101, 1_776_200_101),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[
            raw_log(
                "ethereum-mainnet",
                "0xrec1099",
                1099,
                "0xtx1099",
                0,
                resolver_address,
            ),
            raw_log(
                "ethereum-mainnet",
                "0xrec1101",
                1101,
                "0xtx1101",
                0,
                resolver_address,
            ),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            resolver_changed_event(
                "predecessor-resolver",
                "ens:taytems.eth",
                predecessor_resource_id,
                resolver_address,
                registry_manifest_id,
                1098,
                0,
            ),
            predecessor_email_record,
            resolver_changed_event(
                "successor-resolver",
                "ens:taytems.eth",
                successor_resource_id,
                resolver_address,
                registry_manifest_id,
                1100,
                0,
            ),
            successor_twitter_record,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&predecessor_resource_id.to_string()))
        .await?;

    let row = load_record_inventory_current(
        database.pool(),
        predecessor_resource_id,
        &record_version_boundary(
            "ens:taytems.eth",
            predecessor_resource_id,
            None,
            None,
            1098,
            "0xrec1098",
            1_776_200_098,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("predecessor resource row must not include successor records")?;

    assert_eq!(
        row.entries,
        json!([{
            "record_key": "text:email",
            "record_family": "text",
            "selector_key": "email",
            "status": "success",
            "value": "hello@taytems.xyz",
        }])
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_uses_cross_resource_resolver_boundaries_for_predecessor_records() -> Result<()> {
    let database = TestDatabase::new().await?;
    let predecessor_resource_id = Uuid::from_u128(0x992d);
    let current_resource_id = Uuid::from_u128(0x992e);
    let resolver_a_contract_instance_id = Uuid::from_u128(0x992f);
    let resolver_b_contract_instance_id = Uuid::from_u128(0x9931);
    let resolver_a_address = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41";
    let resolver_b_address = "0x0000000000000000000000000000000000009931";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "manifests/ens/ens_v1_registry_l1/v3.toml",
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
        resolver_a_contract_instance_id,
        resolver_a_address,
        resolver_manifest_id,
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_b_contract_instance_id,
        resolver_b_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver_4976fb03",
        resolver_a_contract_instance_id,
        resolver_a_address,
    )
    .await?;

    let mut stale_email_record = record_changed_event_with_value(
        "cross-resource-prior-tenure-email",
        "ens:taytems.eth",
        predecessor_resource_id,
        "text:email",
        "text",
        Some("email"),
        json!("old@taytems.xyz"),
        1103,
        0,
    );
    stale_email_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    stale_email_record.source_manifest_id = Some(resolver_manifest_id);
    let mut current_twitter_record = record_changed_event_with_value(
        "cross-resource-current-twitter",
        "ens:taytems.eth",
        current_resource_id,
        "text:com.twitter",
        "text",
        Some("com.twitter"),
        json!("taytems"),
        1106,
        0,
    );
    current_twitter_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    current_twitter_record.source_manifest_id = Some(resolver_manifest_id);

    seed_resources(
        database.pool(),
        &[predecessor_resource_id, current_resource_id],
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1102", 1102, 1_776_200_102),
            raw_block("ethereum-mainnet", "0xrec1103", 1103, 1_776_200_103),
            raw_block("ethereum-mainnet", "0xrec1104", 1104, 1_776_200_104),
            raw_block("ethereum-mainnet", "0xrec1105", 1105, 1_776_200_105),
            raw_block("ethereum-mainnet", "0xrec1106", 1106, 1_776_200_106),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[
            raw_log(
                "ethereum-mainnet",
                "0xrec1103",
                1103,
                "0xtx1103",
                0,
                resolver_a_address,
            ),
            raw_log(
                "ethereum-mainnet",
                "0xrec1106",
                1106,
                "0xtx1106",
                0,
                resolver_a_address,
            ),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            resolver_changed_event(
                "cross-resource-predecessor-resolver-a",
                "ens:taytems.eth",
                predecessor_resource_id,
                resolver_a_address,
                registry_manifest_id,
                1102,
                0,
            ),
            stale_email_record,
            resolver_changed_event(
                "cross-resource-predecessor-resolver-b",
                "ens:taytems.eth",
                predecessor_resource_id,
                resolver_b_address,
                registry_manifest_id,
                1104,
                0,
            ),
            resolver_changed_event(
                "cross-resource-current-resolver-a",
                "ens:taytems.eth",
                current_resource_id,
                resolver_a_address,
                registry_manifest_id,
                1105,
                0,
            ),
            current_twitter_record,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&current_resource_id.to_string()))
        .await?;

    let row = load_record_inventory_current(
        database.pool(),
        current_resource_id,
        &record_version_boundary(
            "ens:taytems.eth",
            current_resource_id,
            None,
            None,
            1105,
            "0xrec1105",
            1_776_200_105,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("current resource row must use cross-resource resolver boundaries")?;

    assert_eq!(
        row.entries,
        json!([{
            "record_key": "text:com.twitter",
            "record_family": "text",
            "selector_key": "com.twitter",
            "status": "success",
            "value": "taytems",
        }])
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_ignores_predecessor_record_version_boundary_before_current_resolver() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let predecessor_resource_id = Uuid::from_u128(0x9932);
    let current_resource_id = Uuid::from_u128(0x9933);
    let resolver_contract_instance_id = Uuid::from_u128(0x9934);
    let resolver_address = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "manifests/ens/ens_v1_registry_l1/v3.toml",
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
        resolver_contract_instance_id,
        resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver_4976fb03",
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;

    let mut predecessor_version = record_version_changed_event(
        "predecessor-version-before-current-resolver",
        "ens:taytems.eth",
        predecessor_resource_id,
        2,
        1111,
        0,
    );
    predecessor_version.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    predecessor_version.source_manifest_id = Some(resolver_manifest_id);
    let mut predecessor_email = record_changed_event_with_value(
        "predecessor-email-after-version",
        "ens:taytems.eth",
        predecessor_resource_id,
        "text:email",
        "text",
        Some("email"),
        json!("hello@taytems.xyz"),
        1111,
        1,
    );
    predecessor_email.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    predecessor_email.source_manifest_id = Some(resolver_manifest_id);
    let mut current_twitter = record_changed_event_with_value(
        "current-twitter-after-current-resolver",
        "ens:taytems.eth",
        current_resource_id,
        "text:com.twitter",
        "text",
        Some("com.twitter"),
        json!("taytems"),
        1113,
        0,
    );
    current_twitter.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    current_twitter.source_manifest_id = Some(resolver_manifest_id);

    seed_resources(
        database.pool(),
        &[predecessor_resource_id, current_resource_id],
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1110", 1110, 1_776_200_110),
            raw_block("ethereum-mainnet", "0xrec1111", 1111, 1_776_200_111),
            raw_block("ethereum-mainnet", "0xrec1112", 1112, 1_776_200_112),
            raw_block("ethereum-mainnet", "0xrec1113", 1113, 1_776_200_113),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[
            raw_log(
                "ethereum-mainnet",
                "0xrec1111",
                1111,
                "0xtx1111",
                0,
                resolver_address,
            ),
            raw_log(
                "ethereum-mainnet",
                "0xrec1111",
                1111,
                "0xtx1111",
                1,
                resolver_address,
            ),
            raw_log(
                "ethereum-mainnet",
                "0xrec1113",
                1113,
                "0xtx1113",
                0,
                resolver_address,
            ),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            resolver_changed_event(
                "predecessor-same-resolver",
                "ens:taytems.eth",
                predecessor_resource_id,
                resolver_address,
                registry_manifest_id,
                1110,
                0,
            ),
            predecessor_version,
            predecessor_email,
            resolver_changed_event(
                "current-same-resolver",
                "ens:taytems.eth",
                current_resource_id,
                resolver_address,
                registry_manifest_id,
                1112,
                0,
            ),
            current_twitter,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&current_resource_id.to_string()))
        .await?;

    let row = load_record_inventory_current(
        database.pool(),
        current_resource_id,
        &record_version_boundary(
            "ens:taytems.eth",
            current_resource_id,
            None,
            None,
            1112,
            "0xrec1112",
            1_776_200_112,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("current resource row must use its own resolver boundary")?;

    assert_eq!(
        row.entries,
        json!([
            {
                "record_key": "text:com.twitter",
                "record_family": "text",
                "selector_key": "com.twitter",
                "status": "success",
                "value": "taytems",
            },
            {
                "record_key": "text:email",
                "record_family": "text",
                "selector_key": "email",
                "status": "success",
                "value": "hello@taytems.xyz",
            }
        ])
    );

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_keeps_current_resolver_records_from_predecessor_resource() -> Result<()> {
    let database = TestDatabase::new().await?;
    let predecessor_resource_id = Uuid::from_u128(0x9925);
    let current_resource_id = Uuid::from_u128(0x9926);
    let resolver_contract_instance_id = Uuid::from_u128(0x9927);
    let resolver_address = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "manifests/ens/ens_v1_registry_l1/v3.toml",
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
        resolver_contract_instance_id,
        resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver_4976fb03",
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;

    let mut telegram_record = record_changed_event_with_value(
        "predecessor-telegram",
        "ens:taytems.eth",
        predecessor_resource_id,
        "text:org.telegram",
        "text",
        Some("org.telegram"),
        json!("taytemss"),
        1081,
        0,
    );
    telegram_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    telegram_record.source_manifest_id = Some(resolver_manifest_id);
    let mut twitter_record = record_changed_event_with_value(
        "current-twitter",
        "ens:taytems.eth",
        current_resource_id,
        "text:com.twitter",
        "text",
        Some("com.twitter"),
        json!("taytems"),
        1083,
        0,
    );
    twitter_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    twitter_record.source_manifest_id = Some(resolver_manifest_id);

    seed_resources(
        database.pool(),
        &[predecessor_resource_id, current_resource_id],
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1081", 1081, 1_776_200_081),
            raw_block("ethereum-mainnet", "0xrec1082", 1082, 1_776_200_082),
            raw_block("ethereum-mainnet", "0xrec1083", 1083, 1_776_200_083),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[
            raw_log(
                "ethereum-mainnet",
                "0xrec1081",
                1081,
                "0xtx1081",
                0,
                resolver_address,
            ),
            raw_log(
                "ethereum-mainnet",
                "0xrec1083",
                1083,
                "0xtx1083",
                0,
                resolver_address,
            ),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            telegram_record,
            resolver_changed_event(
                "current-resolver",
                "ens:taytems.eth",
                current_resource_id,
                resolver_address,
                registry_manifest_id,
                1082,
                0,
            ),
            twitter_record,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&current_resource_id.to_string()))
        .await?;

    let row = load_record_inventory_current(
        database.pool(),
        current_resource_id,
        &record_version_boundary(
            "ens:taytems.eth",
            current_resource_id,
            None,
            None,
            1082,
            "0xrec1082",
            1_776_200_082,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("current resource row must include predecessor resolver records")?;

    assert_eq!(
        row.entries,
        json!([
            {
                "record_key": "text:com.twitter",
                "record_family": "text",
                "selector_key": "com.twitter",
                "status": "success",
                "value": "taytems",
            },
            {
                "record_key": "text:org.telegram",
                "record_family": "text",
                "selector_key": "org.telegram",
                "status": "success",
                "value": "taytemss",
            }
        ])
    );

    database.cleanup().await
}

#[tokio::test]
async fn hydrate_text_values_fills_selectorized_ensv1_public_resolver_cache() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9917);
    let resolver_contract_instance_id = Uuid::from_u128(0x9918);
    let resolver_address = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "manifests/ens/ens_v1_registry_l1/v3.toml",
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
        resolver_contract_instance_id,
        resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver_4976fb03",
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;

    let mut text_record = record_changed_event(
        "hydrated-avatar",
        "ens:taytems.eth",
        resource_id,
        "text:avatar",
        "text",
        Some("avatar"),
        1077,
        0,
    );
    text_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    text_record.source_manifest_id = Some(resolver_manifest_id);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1076", 1076, 1_776_200_076),
            raw_block("ethereum-mainnet", "0xrec1077", 1077, 1_776_200_077),
        ],
    )
    .await?;
    seed_chain_checkpoint(database.pool(), "ethereum-mainnet", "0xrec1077", 1077).await?;
    seed_events(
        database.pool(),
        &[
            resolver_changed_event(
                "hydrated-resolver",
                "ens:taytems.eth",
                resource_id,
                resolver_address,
                registry_manifest_id,
                1076,
                0,
            ),
            text_record,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let boundary = record_version_boundary(
        "ens:taytems.eth",
        resource_id,
        None,
        None,
        1076,
        "0xrec1076",
        1_776_200_076,
        "ethereum-mainnet",
    );
    let row = load_record_inventory_current(database.pool(), resource_id, &boundary)
        .await?
        .context("record_inventory_current row before hydration must exist")?;
    assert_eq!(
        row.entries,
        json!([{
            "record_key": "text:avatar",
            "record_family": "text",
            "selector_key": "avatar",
            "status": "unsupported",
            "unsupported_reason": CACHE_UNSUPPORTED_REASON_VALUE_NOT_RETAINED,
        }])
    );

    let summary = hydration::tests_support::hydrate_with_values(
        database.pool(),
        Some(&resource_id.to_string()),
        &[(
            resolver_address,
            "taytems.eth",
            "avatar",
            "https://euc.li/taytems.eth",
        )],
    )
    .await?;
    assert_eq!(
        summary,
        RecordInventoryTextHydrationSummary {
            candidate_row_count: 1,
            candidate_entry_count: 1,
            hydrated_entry_count: 1,
            not_found_entry_count: 0,
            skipped_entry_count: 0,
            failed_entry_count: 0,
            updated_row_count: 1,
        }
    );

    let hydrated_row = load_record_inventory_current(database.pool(), resource_id, &boundary)
        .await?
        .context("record_inventory_current row after hydration must exist")?;
    assert_eq!(
        hydrated_row.entries,
        json!([{
            "record_key": "text:avatar",
            "record_family": "text",
            "selector_key": "avatar",
            "status": "success",
            "value": "https://euc.li/taytems.eth",
        }])
    );

    database.cleanup().await
}

#[tokio::test]
async fn hydrate_text_values_skips_malformed_resolver_text_strings() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9919);
    let resolver_contract_instance_id = Uuid::from_u128(0x991a);
    let resolver_address = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "manifests/ens/ens_v1_registry_l1/v3.toml",
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
        resolver_contract_instance_id,
        resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver_4976fb03",
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;

    let mut text_record = record_changed_event(
        "malformed-token",
        "ens:ilmarefilm.eth",
        resource_id,
        "text:token",
        "text",
        Some("token"),
        2077,
        0,
    );
    text_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    text_record.source_manifest_id = Some(resolver_manifest_id);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec2076", 2076, 1_776_200_076),
            raw_block("ethereum-mainnet", "0xrec2077", 2077, 1_776_200_077),
        ],
    )
    .await?;
    seed_chain_checkpoint(database.pool(), "ethereum-mainnet", "0xrec2077", 2077).await?;
    seed_events(
        database.pool(),
        &[
            resolver_changed_event(
                "malformed-resolver",
                "ens:ilmarefilm.eth",
                resource_id,
                resolver_address,
                registry_manifest_id,
                2076,
                0,
            ),
            text_record,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let boundary = record_version_boundary(
        "ens:ilmarefilm.eth",
        resource_id,
        None,
        None,
        2076,
        "0xrec2076",
        1_776_200_076,
        "ethereum-mainnet",
    );
    let before = load_record_inventory_current(database.pool(), resource_id, &boundary)
        .await?
        .context("record_inventory_current row before hydration must exist")?;

    let summary = hydration::tests_support::hydrate_with_failed_values(
        database.pool(),
        Some(&resource_id.to_string()),
        &[(
            resolver_address,
            "ilmarefilm.eth",
            "token",
            "resolver text call return data is malformed: string return data is not valid ABI string",
        )],
    )
    .await?;
    assert_eq!(
        summary,
        RecordInventoryTextHydrationSummary {
            candidate_row_count: 1,
            candidate_entry_count: 1,
            hydrated_entry_count: 0,
            not_found_entry_count: 0,
            skipped_entry_count: 1,
            failed_entry_count: 0,
            updated_row_count: 0,
        }
    );

    let after = load_record_inventory_current(database.pool(), resource_id, &boundary)
        .await?
        .context("record_inventory_current row after hydration must exist")?;
    assert_eq!(after.entries, before.entries);

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_rejects_multicoin_addr_for_eth_only_legacy_ensv1_resolver() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9920);
    let registry_contract_instance_id = Uuid::from_u128(0x9921);
    let legacy_resolver_contract_instance_id = Uuid::from_u128(0x9922);
    let registry_address = "0x0000000000000000000000000000000000009921";
    let legacy_resolver_address = "0x5FfC014343cd971B7eb70732021E26C35B744cc4";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        "ens_v1_registry_l1",
        "manifests/ens/ens_v1_registry_l1/v3.toml",
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
        legacy_resolver_contract_instance_id,
        legacy_resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver_5ffc0143",
        legacy_resolver_contract_instance_id,
        legacy_resolver_address,
    )
    .await?;

    let mut unsupported_multicoin_record = record_changed_event(
        "legacy-multicoin-unsupported",
        "ens:eth-only.eth",
        resource_id,
        "addr:61",
        "addr",
        Some("61"),
        1081,
        0,
    );
    unsupported_multicoin_record.source_family = SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned();
    unsupported_multicoin_record.source_manifest_id = Some(resolver_manifest_id);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xrec1080", 1080, 1_776_200_080),
            raw_block("ethereum-mainnet", "0xrec1081", 1081, 1_776_200_081),
        ],
    )
    .await?;
    seed_events(
        database.pool(),
        &[
            resolver_changed_event(
                "eth-only-resolver",
                "ens:eth-only.eth",
                resource_id,
                legacy_resolver_address,
                registry_manifest_id,
                1080,
                0,
            ),
            unsupported_multicoin_record,
        ],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:eth-only.eth",
            resource_id,
            None,
            None,
            1080,
            "0xrec1080",
            1_776_200_080,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("ETH-only legacy resolver row must exist")?;

    assert_eq!(row.selectors, json!([]));
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
            "record_family": "contenthash",
            "unsupported_reason": RESOLVER_FAMILY_UNSUPPORTED_REASON,
        }])
    );
    assert_eq!(row.coverage["status"], json!("partial"));
    assert_eq!(row.coverage["exhaustiveness"], json!("best_effort"));
    assert_eq!(
        row.coverage["unsupported_reason"],
        json!(RESOLVER_FAMILY_UNSUPPORTED_REASON)
    );
    assert_eq!(row.provenance["normalized_event_ids"], json!([1]));

    let legacy_resolver_address = legacy_resolver_address.to_ascii_lowercase();
    let admissions =
        bigname_manifests::load_ens_v1_public_resolver_profile_admissions(database.pool()).await?;
    let feature_statuses = admissions
        .iter()
        .filter(|admission| admission.address == legacy_resolver_address)
        .map(|admission| (admission.fact_family.as_str(), admission.status.as_str()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        feature_statuses["resolver_record:multicoin_addr"],
        "unsupported"
    );
    assert_eq!(feature_statuses["resolver_record:addr"], "supported");
    assert_eq!(feature_statuses["resolver_record:text"], "supported");
    assert_eq!(
        feature_statuses["resolver_record:contenthash"],
        "unsupported"
    );
    assert_eq!(feature_statuses["resolver_record:data"], "unsupported");

    database.cleanup().await
}

#[tokio::test]
async fn rebuild_keeps_unsupported_legacy_ensv1_resolver_family_explicit() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x9930);
    let registry_contract_instance_id = Uuid::from_u128(0x9931);
    let legacy_resolver_contract_instance_id = Uuid::from_u128(0x9932);
    let registry_address = "0x0000000000000000000000000000000000009931";
    let legacy_resolver_address = "0x1da022710dF5002339274AaDEe8D58218e9D6AB5";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        "ens_v1_registry_l1",
        "manifests/ens/ens_v1_registry_l1/v3.toml",
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
        legacy_resolver_contract_instance_id,
        legacy_resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver_1da02271",
        legacy_resolver_contract_instance_id,
        legacy_resolver_address,
    )
    .await?;

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[raw_block(
            "ethereum-mainnet",
            "0xrec1090",
            1090,
            1_776_200_090,
        )],
    )
    .await?;
    seed_events(
        database.pool(),
        &[resolver_changed_event(
            "addr-only-legacy-resolver",
            "ens:addr-only.eth",
            resource_id,
            legacy_resolver_address,
            registry_manifest_id,
            1090,
            0,
        )],
    )
    .await?;

    rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

    let row = load_record_inventory_current(
        database.pool(),
        resource_id,
        &record_version_boundary(
            "ens:addr-only.eth",
            resource_id,
            None,
            None,
            1090,
            "0xrec1090",
            1_776_200_090,
            "ethereum-mainnet",
        ),
    )
    .await?
    .context("addr-only legacy resolver row must exist")?;

    assert_eq!(row.selectors, json!([]));
    assert_eq!(
        row.explicit_gaps,
        json!([{
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "gap_reason": GAP_REASON_NOT_OBSERVED,
        }])
    );
    assert_eq!(
        row.unsupported_families,
        json!([
            {
                "record_family": "contenthash",
                "unsupported_reason": RESOLVER_FAMILY_UNSUPPORTED_REASON,
            },
            {
                "record_family": "text",
                "unsupported_reason": RESOLVER_FAMILY_UNSUPPORTED_REASON,
            }
        ])
    );
    assert_eq!(
        row.coverage["unsupported_reason"],
        json!(RESOLVER_FAMILY_UNSUPPORTED_REASON)
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

async fn seed_chain_checkpoint(
    database: &PgPool,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO chain_checkpoints (
            chain_id,
            canonical_block_hash,
            canonical_block_number
        )
        VALUES ($1, $2, $3)
        ON CONFLICT (chain_id)
        DO UPDATE SET
            canonical_block_hash = EXCLUDED.canonical_block_hash,
            canonical_block_number = EXCLUDED.canonical_block_number
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(block_number)
    .execute(database)
    .await
    .context("failed to seed chain checkpoint")?;
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
            'ensip15@ens-normalize-0.1.1',
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
            'ensip15@ens-normalize-0.1.1',
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

async fn insert_basenames_execution_manifest(pool: &PgPool) -> Result<i64> {
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
            'ethereum-mainnet',
            'basenames_v1',
            'active',
            'ensip15@ens-normalize-0.1.1',
            'manifests/basenames/basenames_execution/v1.toml',
            '{}'::jsonb
        )
        RETURNING manifest_id
        "#,
    )
    .bind(SOURCE_FAMILY_BASENAMES_EXECUTION)
    .fetch_one(pool)
    .await
    .context("failed to insert Basenames execution manifest")?
    .try_get::<i64, _>("manifest_id")
    .context("failed to read Basenames execution manifest_id")?;
    let contract_instance_id = Uuid::from_u128(0x98fe);

    sqlx::query(
        r#"
        INSERT INTO manifest_capability_flags (
            manifest_id,
            capability_name,
            status,
            notes
        )
        VALUES ($1, $2, 'supported'::capability_support_status, NULL)
        "#,
    )
    .bind(manifest_id)
    .bind(VERIFIED_RESOLUTION_CAPABILITY)
    .execute(pool)
    .await
    .context("failed to insert Basenames execution capability flag")?;

    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
        VALUES ($1, 'ethereum-mainnet', 'contract', '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await
    .context("failed to insert Basenames execution contract_instance")?;

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
        VALUES ($1, 'contract', 'l1_resolver', $2, lower($3), 'l1_resolver', 'none')
        "#,
    )
    .bind(manifest_id)
    .bind(contract_instance_id)
    .bind(BASENAMES_L1_RESOLVER_ADDRESS)
    .execute(pool)
    .await
    .context("failed to insert Basenames execution manifest_contract_instance")?;

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
        VALUES (1, 'ens', $1, 'ethereum-mainnet', 'ens_v1', 'active', 'ensip15@ens-normalize-0.1.1', $2, '{}'::jsonb)
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

#[allow(clippy::too_many_arguments)]
fn record_changed_event_with_value(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    record_key: &str,
    record_family: &str,
    selector_key: Option<&str>,
    value: Value,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    let mut event = record_changed_event(
        event_identity,
        logical_name_id,
        resource_id,
        record_key,
        record_family,
        selector_key,
        block_number,
        log_index,
    );
    event.after_state["value"] = value;
    event
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
