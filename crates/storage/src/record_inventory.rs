use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};
use uuid::Uuid;

use crate::projection_helpers::{
    POSTGRES_MAX_BIND_PARAMETERS, require_resource_json_array, require_resource_json_object,
    serialize_jsonb_field, serialize_optional_jsonb_field,
};
use crate::snapshot_selection::{
    ChainPositions, SnapshotProjectionRead, SnapshotSelectionError,
    ensure_projection_chain_positions_match,
};

const DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER: &str = r#"
  AND resource.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
"#;

const RECORD_INVENTORY_CURRENT_UPSERT_COLUMN_COUNT: usize = 15;
const RECORD_INVENTORY_CURRENT_UPSERT_MAX_ROWS: usize =
    (POSTGRES_MAX_BIND_PARAMETERS - 1) / RECORD_INVENTORY_CURRENT_UPSERT_COLUMN_COUNT;

/// Persisted record-inventory and cache projection row keyed by resource and version boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordInventoryCurrentRow {
    pub resource_id: Uuid,
    pub record_version_boundary: Value,
    pub enumeration_basis: Value,
    pub selectors: Value,
    pub explicit_gaps: Value,
    pub unsupported_families: Value,
    pub last_change: Option<Value>,
    pub entries: Value,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Load one record-inventory projection row by resource and exact version boundary.
pub async fn load_record_inventory_current(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let record_version_boundary_key = record_version_boundary_storage_key(
        record_version_boundary,
        resource_id,
    )
    .with_context(|| {
        format!(
            "failed to derive record_inventory_current boundary key for resource_id {resource_id}"
        )
    })?;

    let row = sqlx::query(&format!(
        r#"
        SELECT
            ric.resource_id,
            ric.record_version_boundary,
            ric.enumeration_basis,
            ric.selectors,
            ric.explicit_gaps,
            ric.unsupported_families,
            ric.last_change,
            ric.entries,
            ric.provenance,
            ric.coverage,
            ric.chain_positions,
            ric.canonicality_summary,
            ric.manifest_version,
            ric.last_recomputed_at
        FROM record_inventory_current ric
        JOIN resources resource
          ON resource.resource_id = ric.resource_id
        WHERE ric.resource_id = $1
          AND ric.record_version_boundary_key = $2
        {DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER}
        "#,
    ))
    .bind(resource_id)
    .bind(&record_version_boundary_key)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load record_inventory_current row for resource_id {resource_id}")
    })?;

    row.map(decode_record_inventory_current_row).transpose()
}

/// Load one record-inventory projection row only if it is eligible for the selected snapshot.
///
/// A present row with different chain-position context is reported as `stale`
/// instead of being joined into an exact-name response for another snapshot.
pub async fn load_record_inventory_current_for_snapshot(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &Value,
    selected_chain_positions: &ChainPositions,
) -> std::result::Result<SnapshotProjectionRead<RecordInventoryCurrentRow>, SnapshotSelectionError>
{
    let row = load_record_inventory_current(pool, resource_id, record_version_boundary)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load record_inventory_current row for resource_id {resource_id}: {error}"
            ))
        })?;

    let Some(row) = row else {
        return Ok(SnapshotProjectionRead::NotFound);
    };

    ensure_projection_chain_positions_match(
        "record_inventory_current",
        &row.chain_positions,
        selected_chain_positions,
    )?;
    Ok(SnapshotProjectionRead::Found(row))
}

/// Insert or replace record-inventory projection rows for one or more resource and boundary keys.
pub async fn upsert_record_inventory_current_rows(
    pool: &PgPool,
    rows: &[RecordInventoryCurrentRow],
) -> Result<Vec<RecordInventoryCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let prepared_rows = prepare_record_inventory_current_upsert_rows(rows)?;
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for record_inventory_current upsert")?;

    let mut snapshots = Vec::with_capacity(prepared_rows.len());
    let mut batch = Vec::with_capacity(
        prepared_rows
            .len()
            .min(RECORD_INVENTORY_CURRENT_UPSERT_MAX_ROWS),
    );
    let mut batch_keys = BTreeSet::new();

    for row in &prepared_rows {
        let key = row.storage_key();
        if batch.len() == RECORD_INVENTORY_CURRENT_UPSERT_MAX_ROWS || batch_keys.contains(&key) {
            snapshots
                .extend(upsert_record_inventory_current_row_batch(&mut transaction, &batch).await?);
            batch.clear();
            batch_keys.clear();
        }

        batch_keys.insert(key);
        batch.push(row);
    }

    if !batch.is_empty() {
        snapshots
            .extend(upsert_record_inventory_current_row_batch(&mut transaction, &batch).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit record_inventory_current upsert")?;

    Ok(snapshots)
}

/// Delete one record-inventory projection row so a worker can rebuild that exact key.
pub async fn delete_record_inventory_current(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<u64> {
    let record_version_boundary_key = record_version_boundary_storage_key(
        record_version_boundary,
        resource_id,
    )
    .with_context(|| {
        format!(
            "failed to derive record_inventory_current delete key for resource_id {resource_id}"
        )
    })?;

    sqlx::query(
        r#"
        DELETE FROM record_inventory_current
        WHERE resource_id = $1
          AND record_version_boundary_key = $2
        "#,
    )
    .bind(resource_id)
    .bind(&record_version_boundary_key)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete record_inventory_current row for resource_id {resource_id}")
    })
    .map(|result| result.rows_affected())
}

/// Clear the record-inventory projection so a worker can perform a one-shot rebuild.
pub async fn clear_record_inventory_current(pool: &PgPool) -> Result<u64> {
    sqlx::query("DELETE FROM record_inventory_current")
        .execute(pool)
        .await
        .context("failed to clear record_inventory_current rows")
        .map(|result| result.rows_affected())
}

#[derive(Clone, Debug)]
struct RecordInventoryCurrentUpsertRow {
    input_index: usize,
    resource_id: Uuid,
    record_version_boundary_key: String,
    record_version_boundary: String,
    enumeration_basis: String,
    selectors: String,
    explicit_gaps: String,
    unsupported_families: String,
    last_change: Option<String>,
    entries: String,
    provenance: String,
    coverage: String,
    chain_positions: String,
    canonicality_summary: String,
    manifest_version: i64,
    last_recomputed_at: OffsetDateTime,
}

impl RecordInventoryCurrentUpsertRow {
    fn storage_key(&self) -> (Uuid, String) {
        (self.resource_id, self.record_version_boundary_key.clone())
    }
}

fn prepare_record_inventory_current_upsert_rows(
    rows: &[RecordInventoryCurrentRow],
) -> Result<Vec<RecordInventoryCurrentUpsertRow>> {
    rows.iter()
        .enumerate()
        .map(|(input_index, row)| prepare_record_inventory_current_upsert_row(input_index, row))
        .collect()
}

fn prepare_record_inventory_current_upsert_row(
    input_index: usize,
    row: &RecordInventoryCurrentRow,
) -> Result<RecordInventoryCurrentUpsertRow> {
    validate_record_inventory_current_row(row)?;

    let record_version_boundary_key =
        record_version_boundary_storage_key(&row.record_version_boundary, row.resource_id)
            .with_context(|| {
                format!(
                    "failed to derive record_inventory_current boundary key for resource_id {}",
                    row.resource_id
                )
            })?;
    let record_version_boundary = serialize_jsonb_field(
        &row.record_version_boundary,
        "failed to serialize record_inventory_current record_version_boundary",
    )?;
    let enumeration_basis = serialize_jsonb_field(
        &row.enumeration_basis,
        "failed to serialize record_inventory_current enumeration_basis",
    )?;
    let selectors = serialize_jsonb_field(
        &row.selectors,
        "failed to serialize record_inventory_current selectors",
    )?;
    let explicit_gaps = serialize_jsonb_field(
        &row.explicit_gaps,
        "failed to serialize record_inventory_current explicit_gaps",
    )?;
    let unsupported_families = serialize_jsonb_field(
        &row.unsupported_families,
        "failed to serialize record_inventory_current unsupported_families",
    )?;
    let last_change = serialize_optional_jsonb_field(
        row.last_change.as_ref(),
        "failed to serialize record_inventory_current last_change",
    )?;
    let entries = serialize_jsonb_field(
        &row.entries,
        "failed to serialize record_inventory_current entries",
    )?;
    let provenance = serialize_jsonb_field(
        &row.provenance,
        "failed to serialize record_inventory_current provenance",
    )?;
    let coverage = serialize_jsonb_field(
        &row.coverage,
        "failed to serialize record_inventory_current coverage",
    )?;
    let chain_positions = serialize_jsonb_field(
        &row.chain_positions,
        "failed to serialize record_inventory_current chain_positions",
    )?;
    let canonicality_summary = serialize_jsonb_field(
        &row.canonicality_summary,
        "failed to serialize record_inventory_current canonicality_summary",
    )?;

    Ok(RecordInventoryCurrentUpsertRow {
        input_index,
        resource_id: row.resource_id,
        record_version_boundary_key,
        record_version_boundary,
        enumeration_basis,
        selectors,
        explicit_gaps,
        unsupported_families,
        last_change,
        entries,
        provenance,
        coverage,
        chain_positions,
        canonicality_summary,
        manifest_version: row.manifest_version,
        last_recomputed_at: row.last_recomputed_at,
    })
}

async fn upsert_record_inventory_current_row_batch(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[&RecordInventoryCurrentUpsertRow],
) -> Result<Vec<RecordInventoryCurrentRow>> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO record_inventory_current (
            resource_id,
            record_version_boundary_key,
            record_version_boundary,
            enumeration_basis,
            selectors,
            explicit_gaps,
            unsupported_families,
            last_change,
            entries,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        )
        "#,
    );

    builder.push_values(rows.iter().copied(), |mut values, row| {
        values.push_bind(row.resource_id);
        values.push_bind(&row.record_version_boundary_key);
        values
            .push_bind(&row.record_version_boundary)
            .push_unseparated("::jsonb");
        values
            .push_bind(&row.enumeration_basis)
            .push_unseparated("::jsonb");
        values.push_bind(&row.selectors).push_unseparated("::jsonb");
        values
            .push_bind(&row.explicit_gaps)
            .push_unseparated("::jsonb");
        values
            .push_bind(&row.unsupported_families)
            .push_unseparated("::jsonb");
        values
            .push_bind(row.last_change.as_deref())
            .push_unseparated("::jsonb");
        values.push_bind(&row.entries).push_unseparated("::jsonb");
        values
            .push_bind(&row.provenance)
            .push_unseparated("::jsonb");
        values.push_bind(&row.coverage).push_unseparated("::jsonb");
        values
            .push_bind(&row.chain_positions)
            .push_unseparated("::jsonb");
        values
            .push_bind(&row.canonicality_summary)
            .push_unseparated("::jsonb");
        values.push_bind(row.manifest_version);
        values.push_bind(row.last_recomputed_at);
    });

    builder.push(
        r#"
        ON CONFLICT (resource_id, record_version_boundary_key) DO UPDATE
        SET
            record_version_boundary = EXCLUDED.record_version_boundary,
            enumeration_basis = EXCLUDED.enumeration_basis,
            selectors = EXCLUDED.selectors,
            explicit_gaps = EXCLUDED.explicit_gaps,
            unsupported_families = EXCLUDED.unsupported_families,
            last_change = EXCLUDED.last_change,
            entries = EXCLUDED.entries,
            provenance = EXCLUDED.provenance,
            coverage = EXCLUDED.coverage,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING
            resource_id,
            record_version_boundary_key,
            record_version_boundary,
            enumeration_basis,
            selectors,
            explicit_gaps,
            unsupported_families,
            last_change,
            entries,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        "#,
    );

    let returned_rows = builder
        .build()
        .fetch_all(&mut **executor)
    .await
    .with_context(|| {
        let first_input_index = rows.first().map(|row| row.input_index).unwrap_or_default();
        let last_input_index = rows.last().map(|row| row.input_index).unwrap_or(first_input_index);
        format!(
            "failed to upsert record_inventory_current rows for input indexes {first_input_index}..={last_input_index}"
        )
    })?;

    remap_record_inventory_current_snapshots(rows, returned_rows)
}

fn remap_record_inventory_current_snapshots(
    rows: &[&RecordInventoryCurrentUpsertRow],
    returned_rows: Vec<PgRow>,
) -> Result<Vec<RecordInventoryCurrentRow>> {
    if returned_rows.len() != rows.len() {
        bail!(
            "record_inventory_current upsert returned {} snapshots for {} input rows",
            returned_rows.len(),
            rows.len()
        );
    }

    let mut snapshots_by_key = BTreeMap::new();
    for returned_row in returned_rows {
        let snapshot = decode_record_inventory_current_row(returned_row)?;
        let key = (
            snapshot.resource_id,
            record_version_boundary_storage_key(
                &snapshot.record_version_boundary,
                snapshot.resource_id,
            )?,
        );
        if snapshots_by_key.insert(key, snapshot).is_some() {
            bail!("record_inventory_current upsert returned duplicate snapshots for one key");
        }
    }

    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        let key = row.storage_key();
        let snapshot = snapshots_by_key.remove(&key).with_context(|| {
            format!(
                "record_inventory_current upsert did not return snapshot for resource_id {}",
                row.resource_id
            )
        })?;
        snapshots.push(snapshot);
    }

    if !snapshots_by_key.is_empty() {
        bail!("record_inventory_current upsert returned snapshots for unexpected keys");
    }

    Ok(snapshots)
}

fn validate_record_inventory_current_row(row: &RecordInventoryCurrentRow) -> Result<()> {
    decode_record_version_boundary(&row.record_version_boundary, Some(row.resource_id))
        .context("record_inventory_current row has invalid record_version_boundary")?;

    if row.manifest_version <= 0 {
        bail!(
            "record_inventory_current row for resource_id {} has non-positive manifest_version {}",
            row.resource_id,
            row.manifest_version
        );
    }

    validate_enumeration_basis(&row.enumeration_basis, row.resource_id)?;
    let cacheable_selector_keys = validate_selector_array(&row.selectors, row.resource_id)?;
    validate_explicit_gap_array(&row.explicit_gaps, row.resource_id)?;
    validate_unsupported_families(&row.unsupported_families, row.resource_id)?;
    validate_last_change(&row.last_change, row.resource_id)?;
    validate_entries(&row.entries, row.resource_id, &cacheable_selector_keys)?;
    require_resource_json_object(
        &row.provenance,
        "provenance",
        "record_inventory_current",
        row.resource_id,
    )?;
    require_resource_json_object(
        &row.coverage,
        "coverage",
        "record_inventory_current",
        row.resource_id,
    )?;
    require_resource_json_object(
        &row.chain_positions,
        "chain_positions",
        "record_inventory_current",
        row.resource_id,
    )?;
    require_resource_json_object(
        &row.canonicality_summary,
        "canonicality_summary",
        "record_inventory_current",
        row.resource_id,
    )?;

    Ok(())
}

fn decode_record_inventory_current_row(row: PgRow) -> Result<RecordInventoryCurrentRow> {
    let resource_id: Uuid = row
        .try_get("resource_id")
        .context("record_inventory_current row missing resource_id")?;
    let record_version_boundary: Value = row
        .try_get("record_version_boundary")
        .context("record_inventory_current row missing record_version_boundary")?;
    let boundary_key = record_version_boundary_storage_key(&record_version_boundary, resource_id)?;
    let stored_boundary_key: String = row
        .try_get("record_version_boundary_key")
        .unwrap_or_else(|_| boundary_key.clone());
    if stored_boundary_key != boundary_key {
        bail!(
            "record_inventory_current boundary mismatch for resource_id {}: stored {}, decoded {}",
            resource_id,
            stored_boundary_key,
            boundary_key
        );
    }

    let snapshot = RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary,
        enumeration_basis: row
            .try_get("enumeration_basis")
            .context("record_inventory_current row missing enumeration_basis")?,
        selectors: row
            .try_get("selectors")
            .context("record_inventory_current row missing selectors")?,
        explicit_gaps: row
            .try_get("explicit_gaps")
            .context("record_inventory_current row missing explicit_gaps")?,
        unsupported_families: row
            .try_get("unsupported_families")
            .context("record_inventory_current row missing unsupported_families")?,
        last_change: row
            .try_get("last_change")
            .context("record_inventory_current row missing last_change")?,
        entries: row
            .try_get("entries")
            .context("record_inventory_current row missing entries")?,
        provenance: row
            .try_get("provenance")
            .context("record_inventory_current row missing provenance")?,
        coverage: row
            .try_get("coverage")
            .context("record_inventory_current row missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("record_inventory_current row missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("record_inventory_current row missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("record_inventory_current row missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("record_inventory_current row missing last_recomputed_at")?,
    };

    validate_record_inventory_current_row(&snapshot)?;
    Ok(snapshot)
}

fn record_version_boundary_storage_key(
    record_version_boundary: &Value,
    expected_resource_id: Uuid,
) -> Result<String> {
    let boundary =
        decode_record_version_boundary(record_version_boundary, Some(expected_resource_id))
            .context("record_inventory_current record_version_boundary key derivation failed")?;

    let mut key = String::new();
    append_key_part(&mut key, &boundary.logical_name_id);
    append_key_part(&mut key, &boundary.resource_id.to_string());
    append_key_part(
        &mut key,
        &boundary
            .normalized_event_id
            .map(|value| value.to_string())
            .unwrap_or_default(),
    );
    append_key_part(&mut key, boundary.event_kind.as_deref().unwrap_or_default());
    append_key_part(&mut key, &boundary.chain_position.chain_id);
    append_key_part(&mut key, &boundary.chain_position.block_number.to_string());
    append_key_part(&mut key, &boundary.chain_position.block_hash);
    append_key_part(&mut key, &boundary.chain_position.timestamp);
    Ok(key)
}

fn append_key_part(buffer: &mut String, value: &str) {
    write!(buffer, "{}:{value};", value.len()).expect("string write to key buffer must succeed");
}

fn validate_enumeration_basis(value: &Value, resource_id: Uuid) -> Result<()> {
    let object = require_resource_json_object(
        value,
        "enumeration_basis",
        "record_inventory_current",
        resource_id,
    )?;
    required_bool_field(object, "observed_selectors", "enumeration_basis")?;
    required_bool_field(object, "capability_declared_families", "enumeration_basis")?;
    required_bool_field(object, "globally_enumerable", "enumeration_basis")?;
    Ok(())
}

fn validate_selector_array(value: &Value, resource_id: Uuid) -> Result<BTreeSet<String>> {
    let items =
        require_resource_json_array(value, "selectors", "record_inventory_current", resource_id)?;
    let mut previous_record_key: Option<&str> = None;
    let mut cacheable_record_keys = BTreeSet::new();

    for (index, item) in items.iter().enumerate() {
        let object = item.as_object().with_context(|| {
            format!(
                "record_inventory_current row for resource_id {} selectors[{index}] must be a JSON object",
                resource_id
            )
        })?;
        let record_key = validate_selector_identity(
            object,
            "selectors",
            index,
            resource_id,
            SelectorFieldExpectation::CacheableOnly,
        )?;
        if let Some(previous_record_key) = previous_record_key
            && record_key <= previous_record_key
        {
            bail!(
                "record_inventory_current row for resource_id {} selectors must be sorted by record_key ascending",
                resource_id
            );
        }
        if required_bool_field(object, "cacheable", "selector entry")? {
            cacheable_record_keys.insert(record_key.to_owned());
        }
        previous_record_key = Some(record_key);
    }

    Ok(cacheable_record_keys)
}

fn validate_explicit_gap_array(value: &Value, resource_id: Uuid) -> Result<()> {
    let items = require_resource_json_array(
        value,
        "explicit_gaps",
        "record_inventory_current",
        resource_id,
    )?;
    let mut previous_record_key: Option<&str> = None;

    for (index, item) in items.iter().enumerate() {
        let object = item.as_object().with_context(|| {
            format!(
                "record_inventory_current row for resource_id {} explicit_gaps[{index}] must be a JSON object",
                resource_id
            )
        })?;
        let record_key = validate_selector_identity(
            object,
            "explicit_gaps",
            index,
            resource_id,
            SelectorFieldExpectation::GapReasonOnly,
        )?;
        if let Some(previous_record_key) = previous_record_key
            && record_key <= previous_record_key
        {
            bail!(
                "record_inventory_current row for resource_id {} explicit_gaps must be sorted by record_key ascending",
                resource_id
            );
        }
        previous_record_key = Some(record_key);
    }

    Ok(())
}

fn validate_unsupported_families(value: &Value, resource_id: Uuid) -> Result<()> {
    let items = require_resource_json_array(
        value,
        "unsupported_families",
        "record_inventory_current",
        resource_id,
    )?;
    let mut previous_record_family: Option<&str> = None;

    for (index, item) in items.iter().enumerate() {
        let object = item.as_object().with_context(|| {
            format!(
                "record_inventory_current row for resource_id {} unsupported_families[{index}] must be a JSON object",
                resource_id
            )
        })?;
        let record_family = required_string_field(
            object,
            "record_family",
            "record_inventory_current unsupported_families entry",
        )?;
        required_string_field(
            object,
            "unsupported_reason",
            "record_inventory_current unsupported_families entry",
        )?;
        if let Some(previous_record_family) = previous_record_family
            && record_family <= previous_record_family
        {
            bail!(
                "record_inventory_current row for resource_id {} unsupported_families must be sorted by record_family ascending",
                resource_id
            );
        }
        previous_record_family = Some(record_family);
    }

    Ok(())
}

fn validate_last_change(value: &Option<Value>, resource_id: Uuid) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };

    let object = require_resource_json_object(
        value,
        "last_change",
        "record_inventory_current",
        resource_id,
    )?;
    required_positive_i64_field(object, "normalized_event_id", "last_change")?;
    required_string_field(object, "event_kind", "last_change")?;
    decode_chain_position(
        object
            .get("chain_position")
            .with_context(|| "last_change must include chain_position".to_owned())?,
        "last_change.chain_position",
    )?;
    Ok(())
}

fn validate_entries(
    value: &Value,
    resource_id: Uuid,
    expected_record_keys: &BTreeSet<String>,
) -> Result<()> {
    let items =
        require_resource_json_array(value, "entries", "record_inventory_current", resource_id)?;
    let mut seen_record_keys = BTreeSet::new();

    for (index, item) in items.iter().enumerate() {
        let object = item.as_object().with_context(|| {
            format!(
                "record_inventory_current row for resource_id {} entries[{index}] must be a JSON object",
                resource_id
            )
        })?;
        let record_key = validate_selector_identity(
            object,
            "entries",
            index,
            resource_id,
            SelectorFieldExpectation::StatusDriven,
        )?;
        if !seen_record_keys.insert(record_key.to_owned()) {
            bail!(
                "record_inventory_current row for resource_id {} entries must not duplicate record_key {}",
                resource_id,
                record_key
            );
        }
    }

    let missing_record_keys = expected_record_keys
        .difference(&seen_record_keys)
        .cloned()
        .collect::<Vec<_>>();
    let extra_record_keys = seen_record_keys
        .difference(expected_record_keys)
        .cloned()
        .collect::<Vec<_>>();
    if !missing_record_keys.is_empty() || !extra_record_keys.is_empty() {
        let mut drift = Vec::new();
        if !missing_record_keys.is_empty() {
            drift.push(format!(
                "missing cacheable selectors [{}]",
                missing_record_keys.join(", ")
            ));
        }
        if !extra_record_keys.is_empty() {
            drift.push(format!(
                "extra selectors outside cacheable selector space [{}]",
                extra_record_keys.join(", ")
            ));
        }
        bail!(
            "record_inventory_current row for resource_id {} entries must match the cacheable selectors surfaced by selectors ({})",
            resource_id,
            drift.join("; ")
        );
    }

    Ok(())
}

fn validate_selector_identity<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    index: usize,
    resource_id: Uuid,
    expectation: SelectorFieldExpectation,
) -> Result<&'a str> {
    let record_key = required_string_field(
        object,
        "record_key",
        "record_inventory_current selector entry",
    )?;
    let record_family = required_string_field(
        object,
        "record_family",
        "record_inventory_current selector entry",
    )?;
    let selector_key = optional_string_field(
        object,
        "selector_key",
        "record_inventory_current selector entry",
    )?;
    let expected_record_key = match selector_key {
        Some(selector_key) => format!("{record_family}:{selector_key}"),
        None => record_family.to_owned(),
    };
    if record_key != expected_record_key {
        bail!(
            "record_inventory_current row for resource_id {} {}[{index}] record_key {} must match selector identity {}",
            resource_id,
            field_name,
            record_key,
            expected_record_key
        );
    }

    match expectation {
        SelectorFieldExpectation::CacheableOnly => {
            required_bool_field(object, "cacheable", "selector entry")?;
        }
        SelectorFieldExpectation::GapReasonOnly => {
            required_string_field(object, "gap_reason", "explicit_gap entry")?;
        }
        SelectorFieldExpectation::StatusDriven => {
            let status = required_string_field(object, "status", "record_cache entry")?.to_owned();
            match status.as_str() {
                "success" => {
                    if !object.contains_key("value") {
                        bail!(
                            "record_inventory_current row for resource_id {} entries[{index}] with status success must include value",
                            resource_id
                        );
                    }
                    if object.contains_key("unsupported_reason") {
                        bail!(
                            "record_inventory_current row for resource_id {} entries[{index}] with status success must not include unsupported_reason",
                            resource_id
                        );
                    }
                }
                "not_found" => {
                    if object.contains_key("value") {
                        bail!(
                            "record_inventory_current row for resource_id {} entries[{index}] with status not_found must not include value",
                            resource_id
                        );
                    }
                    if object.contains_key("unsupported_reason") {
                        bail!(
                            "record_inventory_current row for resource_id {} entries[{index}] with status not_found must not include unsupported_reason",
                            resource_id
                        );
                    }
                }
                "unsupported" => {
                    if object.contains_key("value") {
                        bail!(
                            "record_inventory_current row for resource_id {} entries[{index}] with status unsupported must not include value",
                            resource_id
                        );
                    }
                    required_string_field(
                        object,
                        "unsupported_reason",
                        "record_cache entry unsupported_reason",
                    )?;
                }
                _ => {
                    bail!(
                        "record_inventory_current row for resource_id {} entries[{index}] has unsupported status {}",
                        resource_id,
                        status
                    );
                }
            }
        }
    }

    Ok(record_key)
}

fn required_string_field<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<&'a str> {
    object
        .get(field_name)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("{context} must include non-empty string field {field_name}"))
}

fn optional_string_field<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<Option<&'a str>> {
    match object.get(field_name) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value)),
        Some(_) => bail!("{context} field {field_name} must be null or non-empty string"),
    }
}

fn required_bool_field(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<bool> {
    object
        .get(field_name)
        .and_then(Value::as_bool)
        .with_context(|| format!("{context} must include boolean field {field_name}"))
}

fn required_positive_i64_field(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<i64> {
    object
        .get(field_name)
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .with_context(|| format!("{context} must include positive integer field {field_name}"))
}

fn decode_record_version_boundary(
    value: &Value,
    expected_resource_id: Option<Uuid>,
) -> Result<RecordVersionBoundaryParts> {
    let object = value
        .as_object()
        .with_context(|| "record_version_boundary must be a JSON object".to_owned())?;
    let logical_name_id =
        required_string_field(object, "logical_name_id", "record_version_boundary")?.to_owned();
    let resource_id = Uuid::parse_str(required_string_field(
        object,
        "resource_id",
        "record_version_boundary",
    )?)
    .context("record_version_boundary resource_id must be a UUID")?;
    if let Some(expected_resource_id) = expected_resource_id
        && resource_id != expected_resource_id
    {
        bail!(
            "record_version_boundary resource_id {} does not match storage key resource_id {}",
            resource_id,
            expected_resource_id
        );
    }

    let normalized_event_id = match object.get("normalized_event_id") {
        Some(Value::Null) => None,
        Some(value) => Some(value.as_i64().filter(|value| *value > 0).with_context(|| {
            "record_version_boundary normalized_event_id must be null or positive integer"
                .to_owned()
        })?),
        None => bail!("record_version_boundary must include normalized_event_id"),
    };
    let event_kind = match object.get("event_kind") {
        Some(Value::Null) => None,
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
        Some(_) => {
            bail!("record_version_boundary event_kind must be null or non-empty string");
        }
        None => bail!("record_version_boundary must include event_kind"),
    };
    if normalized_event_id.is_some() != event_kind.is_some() {
        bail!(
            "record_version_boundary normalized_event_id and event_kind must both be present or both be null"
        );
    }

    let chain_position = decode_chain_position(
        object
            .get("chain_position")
            .with_context(|| "record_version_boundary must include chain_position".to_owned())?,
        "record_version_boundary.chain_position",
    )?;

    Ok(RecordVersionBoundaryParts {
        logical_name_id,
        resource_id,
        normalized_event_id,
        event_kind,
        chain_position,
    })
}

fn decode_chain_position(value: &Value, context: &str) -> Result<ChainPositionParts> {
    let object = value
        .as_object()
        .with_context(|| format!("{context} must be a JSON object"))?;
    let chain_id = required_string_field(object, "chain_id", context)?.to_owned();
    let block_number = object
        .get("block_number")
        .and_then(Value::as_i64)
        .filter(|value| *value >= 0)
        .with_context(|| format!("{context} must include non-negative integer block_number"))?;
    let block_hash = required_string_field(object, "block_hash", context)?.to_owned();
    let timestamp = required_string_field(object, "timestamp", context)?.to_owned();
    Ok(ChainPositionParts {
        chain_id,
        block_number,
        block_hash,
        timestamp,
    })
}

#[derive(Clone, Debug)]
struct RecordVersionBoundaryParts {
    logical_name_id: String,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<String>,
    chain_position: ChainPositionParts,
}

#[derive(Clone, Debug)]
struct ChainPositionParts {
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectorFieldExpectation {
    CacheableOnly,
    GapReasonOnly,
    StatusDriven,
}

#[cfg(test)]
mod tests;
