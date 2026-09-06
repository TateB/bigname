use anyhow::{Context, Result};
use bigname_storage::{
    ChainPositions, SnapshotProjectionRead, SnapshotSelectionErrorKind,
    ensure_projection_chain_positions_match, load_record_inventory_current_for_snapshot,
    record_version_boundary_storage_key,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};
use uuid::Uuid;

const CHAIN_ID: &str = "ethereum-mainnet";
const BLOCK_HASH: &str = "0x4a98000000000000000000000000000000000000000000000000000000000000";
const RESOURCE_ID: Uuid = Uuid::from_u128(0x857);
const TIMESTAMP_Z: &str = "2025-06-15T15:07:42Z";
const TIMESTAMP_OFFSET: &str = "2025-06-15T15:07:42+00:00";

const BASELINE: &[&str] = &[
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
    include_str!("../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
    include_str!("../../../schema-v2/baseline/12_project_generation_failures.sql"),
    include_str!("../../../schema-v2/baseline/13_interpret_decode_skips.sql"),
    include_str!("../../../schema-v2/baseline/14_discovery_watch_admissions.sql"),
];

#[tokio::test]
async fn postgres_timestamp_offset_matches_snapshot_while_phase_target_short_circuits() -> Result<()>
{
    let database = TestDatabase::create(
        TestDatabaseConfig::new("snapshot_selection_timestamp").pool_max_connections(1),
    )
    .await?;
    let pool = database.pool().clone();

    let observed = exercise_project_shapes(&pool).await;
    drop(pool);
    database.cleanup().await?;
    let (postgres_timestamp, name_match, phase_target, inventory_read) = observed?;

    assert_eq!(postgres_timestamp, TIMESTAMP_OFFSET);
    let phase_target_error = ensure_projection_chain_positions_match(
        "record_inventory_current",
        &phase_target,
        &selected_positions(),
    )
    .expect_err("Project's phase-target shape is not parsed as chain positions");
    assert_eq!(phase_target_error.kind(), SnapshotSelectionErrorKind::Stale);
    assert!(matches!(inventory_read, SnapshotProjectionRead::Found(_)));

    if let Err(error) = name_match {
        panic!(
            "PostgreSQL timestamp {postgres_timestamp} and selected {TIMESTAMP_Z} must match; \
             observed storage error kind {:?}: {}",
            error.kind(),
            error.message()
        );
    }
    Ok(())
}

async fn exercise_project_shapes(
    pool: &PgPool,
) -> Result<(
    String,
    bigname_storage::SnapshotSelectionResult<()>,
    Value,
    SnapshotProjectionRead<bigname_storage::RecordInventoryCurrentRow>,
)> {
    install_schema(pool).await?;
    sqlx::query("SET TIME ZONE 'UTC'").execute(pool).await?;
    sqlx::query(
        "CREATE TEMP TABLE projected_positions (
             family text PRIMARY KEY,
             chain_positions jsonb NOT NULL
         )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO projected_positions (family, chain_positions)
         VALUES (
             'name_current',
             jsonb_build_object(
                 'ethereum', jsonb_build_object(
                     'chain_id', $1::text,
                     'block_number', 27::bigint,
                     'block_hash', $2::text,
                     'timestamp', TIMESTAMPTZ '2025-06-15 15:07:42+00'
                 )
             )
         )",
    )
    .bind(CHAIN_ID)
    .bind(BLOCK_HASH)
    .execute(pool)
    .await?;
    let name_positions: Value = sqlx::query_scalar(
        "SELECT chain_positions FROM projected_positions WHERE family = 'name_current'",
    )
    .fetch_one(pool)
    .await?;
    let postgres_timestamp = name_positions
        .pointer("/ethereum/timestamp")
        .and_then(Value::as_str)
        .context("PostgreSQL name_current shape omitted its timestamp")?
        .to_owned();
    let selected = selected_positions();
    let name_match =
        ensure_projection_chain_positions_match("name_current", &name_positions, &selected);

    let boundary = record_version_boundary();
    let boundary_key = record_version_boundary_storage_key(&boundary, RESOURCE_ID)?;
    sqlx::query(
        "INSERT INTO bigname_phase.chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, $2, 27, TIMESTAMPTZ '2025-06-15 15:07:42+00', 'canonical')",
    )
    .bind(CHAIN_ID)
    .bind(BLOCK_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 27, 'canonical')",
    )
    .bind(RESOURCE_ID)
    .bind(CHAIN_ID)
    .bind(BLOCK_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.record_inventory_current (
             resource_id, record_version_boundary_key, record_version_boundary,
             support_status, provenance, chain_positions, canonicality_summary,
             manifest_version
         ) VALUES (
             $1, $2, $3, 'supported', jsonb_build_object('chain_id', $4::text),
             jsonb_strip_nulls(jsonb_build_object(
                 'block_number', 27::bigint,
                 'block_hash', $5::text,
                 'target_block_number', 27::bigint,
                 'target_block_hash', $5::text
             )),
             jsonb_build_object(
                 'state', 'canonical_lineage',
                 'target_block_number', 27::bigint,
                 'target_block_hash', $5::text
             ),
             1
         )",
    )
    .bind(RESOURCE_ID)
    .bind(boundary_key)
    .bind(&boundary)
    .bind(CHAIN_ID)
    .bind(BLOCK_HASH)
    .execute(pool)
    .await?;
    let phase_target: Value = sqlx::query_scalar(
        "SELECT chain_positions
         FROM bigname_phase.record_inventory_current
         WHERE resource_id = $1",
    )
    .bind(RESOURCE_ID)
    .fetch_one(pool)
    .await?;
    let inventory_read =
        load_record_inventory_current_for_snapshot(pool, RESOURCE_ID, &boundary, &selected).await?;

    Ok((postgres_timestamp, name_match, phase_target, inventory_read))
}

async fn install_schema(pool: &PgPool) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await?;
    for script in BASELINE {
        raw_sql(script).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    Ok(())
}

fn selected_positions() -> ChainPositions {
    ChainPositions::from_value(&json!({
        "ethereum": {
            "chain_id": CHAIN_ID,
            "block_number": 27,
            "block_hash": BLOCK_HASH,
            "timestamp": TIMESTAMP_Z
        }
    }))
    .expect("selected Z timestamp must decode")
}

fn record_version_boundary() -> Value {
    json!({
        "logical_name_id": "ens:857-snapshot-offset",
        "resource_id": RESOURCE_ID,
        "normalized_event_id": null,
        "event_kind": null,
        "chain_position": {
            "chain_id": CHAIN_ID,
            "block_number": 27,
            "block_hash": BLOCK_HASH,
            "timestamp": TIMESTAMP_OFFSET
        }
    })
}
