mod support;

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;

use super::*;
use support::*;

#[test]
fn all_current_projection_json_summary_has_frozen_shape_order_counts_and_totals() -> Result<()> {
    let summary = AllCurrentProjectionsReplaySummary {
        steps: vec![
            CurrentProjectionReplayStepSummary {
                projection: "name_current",
                requested_key_count: 2,
                upserted_row_count: 2,
                deleted_row_count: 0,
            },
            CurrentProjectionReplayStepSummary {
                projection: "children_current",
                requested_key_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
            },
            CurrentProjectionReplayStepSummary {
                projection: "permissions_current",
                requested_key_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
            },
            CurrentProjectionReplayStepSummary {
                projection: "record_inventory_current",
                requested_key_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
            },
            CurrentProjectionReplayStepSummary {
                projection: "resolver_current",
                requested_key_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
            },
            CurrentProjectionReplayStepSummary {
                projection: "address_names_current",
                requested_key_count: 2,
                upserted_row_count: 3,
                deleted_row_count: 0,
            },
            CurrentProjectionReplayStepSummary {
                projection: "primary_names_current",
                requested_key_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
            },
        ],
    };

    let encoded = summary.json_summary_string()?;
    let value: Value = serde_json::from_str(&encoded)?;
    assert_json_object_fields(&value, ["command", "projections", "totals"]);
    assert_eq!(value["command"], "all-current-projections");

    let projections = value["projections"]
        .as_array()
        .context("projections must be an array")?;
    let projection_order = projections
        .iter()
        .map(|projection| {
            projection["projection"]
                .as_str()
                .context("projection name must be a string")
        })
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(projection_order, ALL_CURRENT_PROJECTION_JSON_ORDER.to_vec());

    let expected_counts = BTreeMap::from([
        ("address_names_current", (2, 3, 0)),
        ("children_current", (1, 1, 0)),
        ("coverage_current", (0, 0, 0)),
        ("name_current", (2, 2, 0)),
        ("permissions_current", (1, 1, 0)),
        ("primary_names_current", (1, 1, 0)),
        ("record_inventory_current", (1, 1, 0)),
        ("resolver_current", (1, 1, 0)),
        ("surface_bindings_current", (0, 0, 0)),
    ]);

    for projection in projections {
        assert_json_object_fields(
            projection,
            ["projection", "requested", "upserted", "deleted"],
        );
        let projection_name = projection["projection"]
            .as_str()
            .context("projection name must be a string")?;
        let (requested, upserted, deleted) = expected_counts
            .get(projection_name)
            .copied()
            .with_context(|| format!("unexpected projection {projection_name}"))?;
        assert_eq!(projection["requested"].as_u64(), Some(requested));
        assert_eq!(projection["upserted"].as_u64(), Some(upserted));
        assert_eq!(projection["deleted"].as_u64(), Some(deleted));
    }

    assert_json_object_fields(&value["totals"], ["requested", "upserted", "deleted"]);
    assert_eq!(value["totals"]["requested"].as_u64(), Some(9));
    assert_eq!(value["totals"]["upserted"].as_u64(), Some(10));
    assert_eq!(value["totals"]["deleted"].as_u64(), Some(0));

    Ok(())
}

#[tokio::test]
async fn all_current_projection_replay_clears_stale_rows_and_is_idempotent() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;

    let first_summary = rebuild_all_current_projections(database.pool(), None).await?;
    assert_eq!(
        first_summary.projection_order(),
        ALL_CURRENT_PROJECTION_ORDER
    );
    assert_eq!(first_summary.total_deleted_row_count(), 0);

    let first_snapshot = load_api_visible_projection_snapshot(database.pool()).await?;
    assert_projection_counts(
        &first_snapshot,
        [
            ("name_current", 2),
            ("children_current", 1),
            ("permissions_current", 1),
            ("record_inventory_current", 1),
            ("resolver_current", 1),
            ("address_names_current", 3),
            ("primary_names_current", 1),
        ],
    );

    insert_stale_projection_rows(database.pool()).await?;
    let stale_snapshot = load_api_visible_projection_snapshot(database.pool()).await?;
    for projection in ALL_CURRENT_PROJECTION_ORDER {
        assert!(
            stale_snapshot.row_count(projection) > first_snapshot.row_count(projection),
            "{projection} should contain an injected stale row before replay"
        );
    }

    let second_summary = rebuild_all_current_projections(database.pool(), None).await?;
    assert_eq!(
        second_summary.projection_order(),
        ALL_CURRENT_PROJECTION_ORDER
    );
    assert!(second_summary.total_deleted_row_count() >= 6);
    let second_snapshot = load_api_visible_projection_snapshot(database.pool()).await?;
    assert_eq!(first_snapshot, second_snapshot);

    let third_summary = rebuild_all_current_projections(database.pool(), None).await?;
    assert_eq!(
        third_summary.projection_order(),
        ALL_CURRENT_PROJECTION_ORDER
    );
    let third_snapshot = load_api_visible_projection_snapshot(database.pool()).await?;
    assert_eq!(second_snapshot.row_counts(), third_snapshot.row_counts());
    assert_eq!(second_snapshot, third_snapshot);

    let target_block = Some(108);
    let targeted_summary =
        rebuild_pending_all_current_projections(database.pool(), target_block, None).await?;
    assert_eq!(
        targeted_summary.projection_order(),
        ALL_CURRENT_PROJECTION_ORDER
    );
    assert!(targeted_summary.total_requested_key_count() > 0);
    assert_eq!(
        third_snapshot,
        load_api_visible_projection_snapshot(database.pool()).await?
    );

    let status_rows = load_replay_status_rows(database.pool()).await?;
    assert_eq!(status_rows.len(), ALL_CURRENT_PROJECTION_ORDER.len());
    for projection in ALL_CURRENT_PROJECTION_ORDER {
        let (version, completed_target_block) = status_rows
            .get(*projection)
            .with_context(|| format!("missing replay status row for {projection}"))?;
        assert_eq!(*version, super::progress::CURRENT_PROJECTION_REPLAY_VERSION);
        assert_eq!(*completed_target_block, target_block);
    }

    let skipped_summary =
        rebuild_pending_all_current_projections(database.pool(), target_block, None).await?;
    assert_eq!(
        skipped_summary.projection_order(),
        ALL_CURRENT_PROJECTION_ORDER
    );
    assert_eq!(skipped_summary.total_requested_key_count(), 0);
    assert_eq!(skipped_summary.total_upserted_row_count(), 0);
    assert_eq!(skipped_summary.total_deleted_row_count(), 0);
    assert_eq!(
        third_snapshot,
        load_api_visible_projection_snapshot(database.pool()).await?
    );

    let advanced_summary =
        rebuild_pending_all_current_projections(database.pool(), Some(109), None).await?;
    assert_eq!(
        advanced_summary.projection_order(),
        ALL_CURRENT_PROJECTION_ORDER
    );
    assert!(advanced_summary.total_requested_key_count() > 0);
    assert!(advanced_summary.total_upserted_row_count() > 0);
    assert_eq!(
        third_snapshot,
        load_api_visible_projection_snapshot(database.pool()).await?
    );
    let status_rows = load_replay_status_rows(database.pool()).await?;
    for projection in ALL_CURRENT_PROJECTION_ORDER {
        let (_, completed_target_block) = status_rows
            .get(*projection)
            .with_context(|| format!("missing replay status row for {projection}"))?;
        assert_eq!(*completed_target_block, Some(109));
    }

    database.cleanup().await
}

async fn load_replay_status_rows(pool: &PgPool) -> Result<BTreeMap<String, (i32, Option<i64>)>> {
    let rows = sqlx::query_as::<_, (String, i32, Option<i64>)>(
        r#"
        SELECT projection, replay_version, completed_normalized_target_block
        FROM current_projection_replay_status
        ORDER BY projection
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load current projection replay status rows")?;

    Ok(rows
        .into_iter()
        .map(|(projection, version, target_block)| (projection, (version, target_block)))
        .collect())
}
