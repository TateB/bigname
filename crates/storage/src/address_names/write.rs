use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, Transaction};

use super::{decode::decode_address_name_current_row, types::AddressNameCurrentRow};
use crate::projection_helpers::{require_json_object, serialize_jsonb_field};

/// Insert or replace address-name relation rows for one or more address collection keys.
pub async fn upsert_address_names_current_rows(
    pool: &PgPool,
    rows: &[AddressNameCurrentRow],
) -> Result<Vec<AddressNameCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for address_names_current upsert")?;

    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        validate_address_name_current_row(row)?;
        snapshots.push(upsert_address_name_current_row(&mut transaction, row).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit address_names_current upsert")?;

    Ok(snapshots)
}

/// Delete all current address-name relation rows for one address so a worker can rebuild the key.
pub async fn delete_address_names_current(pool: &PgPool, address: &str) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM address_names_current
        WHERE address = $1
        "#,
    )
    .bind(address)
    .execute(pool)
    .await
    .with_context(|| format!("failed to delete address_names_current rows for address {address}"))
    .map(|result| result.rows_affected())
}

/// Clear the current address-name projection so a worker can perform a one-shot rebuild.
pub async fn clear_address_names_current(pool: &PgPool) -> Result<u64> {
    sqlx::query("DELETE FROM address_names_current")
        .execute(pool)
        .await
        .context("failed to clear address_names_current rows")
        .map(|result| result.rows_affected())
}

const ADDRESS_NAMES_CURRENT_SIDECAR_TRIGGERS: &[&str] = &[
    "address_names_current_identity_counts_after_delete",
    "address_names_current_identity_counts_after_insert",
    "address_names_current_identity_counts_after_update",
    "address_names_current_identity_feed_after_insert_delete",
    "address_names_current_identity_feed_after_anchor_update",
];

pub(super) async fn set_address_names_current_sidecar_triggers(
    transaction: &mut Transaction<'_, Postgres>,
    enabled: bool,
) -> Result<()> {
    let action = if enabled { "ENABLE" } else { "DISABLE" };
    for trigger in ADDRESS_NAMES_CURRENT_SIDECAR_TRIGGERS {
        let sql = format!("ALTER TABLE address_names_current {action} TRIGGER {trigger}");
        sqlx::query(&sql)
            .execute(&mut **transaction)
            .await
            .with_context(|| {
                format!(
                    "failed to {} address_names_current sidecar trigger {}",
                    action.to_ascii_lowercase(),
                    trigger
                )
            })?;
    }
    Ok(())
}

async fn upsert_address_name_current_row(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &AddressNameCurrentRow,
) -> Result<AddressNameCurrentRow> {
    upsert_address_name_current_row_into_table(executor, "address_names_current", row).await
}

pub(super) async fn upsert_address_name_current_row_into_table(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target_table_sql: &str,
    row: &AddressNameCurrentRow,
) -> Result<AddressNameCurrentRow> {
    validate_address_name_current_row(row)?;

    let provenance = serialize_jsonb_field(
        &row.provenance,
        "failed to serialize address_names_current provenance",
    )?;
    let coverage = serialize_jsonb_field(
        &row.coverage,
        "failed to serialize address_names_current coverage",
    )?;
    let chain_positions = serialize_jsonb_field(
        &row.chain_positions,
        "failed to serialize address_names_current chain_positions",
    )?;
    let canonicality_summary = serialize_jsonb_field(
        &row.canonicality_summary,
        "failed to serialize address_names_current canonicality_summary",
    )?;

    let query = format!(
        r#"
        INSERT INTO {target_table_sql} (
            address,
            logical_name_id,
            relation,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12::jsonb,
            $13::jsonb,
            $14::jsonb,
            $15::jsonb,
            $16,
            $17
        )
        ON CONFLICT (address, logical_name_id, relation) DO UPDATE
        SET
            namespace = EXCLUDED.namespace,
            canonical_display_name = EXCLUDED.canonical_display_name,
            normalized_name = EXCLUDED.normalized_name,
            namehash = EXCLUDED.namehash,
            surface_binding_id = EXCLUDED.surface_binding_id,
            resource_id = EXCLUDED.resource_id,
            token_lineage_id = EXCLUDED.token_lineage_id,
            binding_kind = EXCLUDED.binding_kind,
            provenance = EXCLUDED.provenance,
            coverage = EXCLUDED.coverage,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING
            address,
            logical_name_id,
            relation,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        "#,
    );

    let snapshot = sqlx::query(&query)
    .bind(&row.address)
    .bind(&row.logical_name_id)
    .bind(row.relation.as_str())
    .bind(&row.namespace)
    .bind(&row.canonical_display_name)
    .bind(&row.normalized_name)
    .bind(&row.namehash)
    .bind(row.surface_binding_id)
    .bind(row.resource_id)
    .bind(row.token_lineage_id)
    .bind(row.binding_kind.as_str())
    .bind(provenance)
    .bind(coverage)
    .bind(chain_positions)
    .bind(canonicality_summary)
    .bind(row.manifest_version)
    .bind(row.last_recomputed_at)
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert address_names_current row for address {} logical_name_id {} relation {}",
            row.address,
            row.logical_name_id,
            row.relation.as_str()
        )
    })?;

    decode_address_name_current_row(snapshot)
}

fn validate_address_name_current_row(row: &AddressNameCurrentRow) -> Result<()> {
    if row.address.trim().is_empty() {
        bail!("address_names_current row must include address");
    }
    if row.logical_name_id.trim().is_empty() {
        bail!("address_names_current row must include logical_name_id");
    }
    if row.namespace.trim().is_empty() {
        bail!(
            "address_names_current row {} {} must include namespace",
            row.address,
            row.logical_name_id
        );
    }
    if row.normalized_name.trim().is_empty() {
        bail!(
            "address_names_current row {} {} must include normalized_name",
            row.address,
            row.logical_name_id
        );
    }
    if row.canonical_display_name.trim().is_empty() {
        bail!(
            "address_names_current row {} {} must include canonical_display_name",
            row.address,
            row.logical_name_id
        );
    }
    if row.namehash.trim().is_empty() {
        bail!(
            "address_names_current row {} {} must include namehash",
            row.address,
            row.logical_name_id
        );
    }
    if row.logical_name_id != format!("{}:{}", row.namespace, row.normalized_name) {
        bail!(
            "address_names_current row {} {} does not match namespace {} and normalized_name {}",
            row.address,
            row.logical_name_id,
            row.namespace,
            row.normalized_name
        );
    }
    if row.manifest_version <= 0 {
        bail!(
            "address_names_current row {} {} has non-positive manifest_version {}",
            row.address,
            row.logical_name_id,
            row.manifest_version
        );
    }

    require_json_object(&row.provenance, || {
        format!(
            "address_names_current row {} {} field provenance must be a JSON object",
            row.address, row.logical_name_id
        )
    })?;
    require_json_object(&row.coverage, || {
        format!(
            "address_names_current row {} {} field coverage must be a JSON object",
            row.address, row.logical_name_id
        )
    })?;
    require_json_object(&row.chain_positions, || {
        format!(
            "address_names_current row {} {} field chain_positions must be a JSON object",
            row.address, row.logical_name_id
        )
    })?;
    require_json_object(&row.canonicality_summary, || {
        format!(
            "address_names_current row {} {} field canonicality_summary must be a JSON object",
            row.address, row.logical_name_id
        )
    })?;

    Ok(())
}
