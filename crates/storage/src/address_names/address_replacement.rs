use anyhow::{Context, Result, bail};
use sqlx::PgPool;
use uuid::Uuid;

use super::{types::AddressNameCurrentRow, write};

#[derive(Clone, Debug)]
pub struct AddressNamesCurrentAddressReplacement {
    address: String,
    table_name: String,
}

impl AddressNamesCurrentAddressReplacement {
    pub fn address(&self) -> &str {
        &self.address
    }

    fn table_sql(&self) -> String {
        quote_identifier(&self.table_name).expect("generated table name must be safe")
    }
}

pub async fn begin_address_names_current_address_replacement(
    pool: &PgPool,
    address: &str,
) -> Result<AddressNamesCurrentAddressReplacement> {
    if address.trim().is_empty() {
        bail!("address_names_current address replacement requires an address");
    }

    let table_name = create_address_names_current_staging_table(pool).await?;

    Ok(AddressNamesCurrentAddressReplacement {
        address: address.to_owned(),
        table_name,
    })
}

pub async fn drop_address_names_current_address_replacement(
    pool: &PgPool,
    replacement: &AddressNamesCurrentAddressReplacement,
) -> Result<()> {
    sqlx::query(&format!("DROP TABLE IF EXISTS {}", replacement.table_sql()))
        .execute(pool)
        .await
        .context("failed to drop address_names_current address replacement staging table")?;
    Ok(())
}

pub async fn insert_address_names_current_address_replacement_rows(
    pool: &PgPool,
    replacement: &AddressNamesCurrentAddressReplacement,
    rows: &[AddressNameCurrentRow],
) -> Result<Vec<AddressNameCurrentRow>> {
    validate_address_replacement_rows(replacement.address(), rows)?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let table_sql = replacement.table_sql();
    let mut transaction = pool.begin().await.context(
        "failed to open transaction for address_names_current address replacement staging",
    )?;

    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        snapshots.push(
            write::upsert_address_name_current_row_into_table(&mut transaction, &table_sql, row)
                .await?,
        );
    }

    transaction
        .commit()
        .await
        .context("failed to commit address_names_current address replacement staging batch")?;

    Ok(snapshots)
}

pub async fn publish_address_names_current_address_replacement(
    pool: &PgPool,
    replacement: &AddressNamesCurrentAddressReplacement,
) -> Result<(u64, u64)> {
    let address = replacement.address();
    let table_sql = replacement.table_sql();
    let mut transaction = pool.begin().await.context(
        "failed to open transaction for address_names_current address replacement publish",
    )?;

    // Match trigger-maintained writes: take the table lock before the per-address
    // advisory lock, so mixed-version/direct writers cannot deadlock with publish.
    write::set_address_names_current_sidecar_triggers(&mut transaction, false).await?;

    sqlx::query(
        r#"
        SELECT address_names_current_identity_counts_lock_address($1)
        "#,
    )
    .bind(address)
    .execute(&mut *transaction)
    .await
    .with_context(|| {
        format!("failed to acquire address_names_current sidecar lock for address {address}")
    })?;

    let deleted_row_count = sqlx::query(
        r#"
        DELETE FROM address_names_current
        WHERE address = $1
        "#,
    )
    .bind(address)
    .execute(&mut *transaction)
    .await
    .with_context(|| {
        format!("failed to delete existing address_names_current rows for address {address}")
    })?
    .rows_affected();

    let inserted_row_count = sqlx::query(&format!(
        r#"
        INSERT INTO address_names_current (
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
        SELECT
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
        FROM {table_sql}
        WHERE address = $1
        "#
    ))
    .bind(address)
    .execute(&mut *transaction)
    .await
    .with_context(|| {
        format!("failed to publish staged address_names_current rows for address {address}")
    })?
    .rows_affected();

    sqlx::query(
        r#"
        SELECT address_names_current_identity_counts_recompute_address($1)
        "#,
    )
    .bind(address)
    .execute(&mut *transaction)
    .await
    .with_context(|| {
        format!("failed to refresh address_names_current identity counts for address {address}")
    })?;

    sqlx::query(
        r#"
        SELECT address_names_current_identity_feed_recompute_address($1)
        "#,
    )
    .bind(address)
    .execute(&mut *transaction)
    .await
    .with_context(|| {
        format!("failed to refresh address_names_current identity feed for address {address}")
    })?;

    write::set_address_names_current_sidecar_triggers(&mut transaction, true).await?;

    transaction
        .commit()
        .await
        .context("failed to commit address_names_current address replacement publish")?;

    Ok((deleted_row_count, inserted_row_count))
}

fn validate_address_replacement_rows(address: &str, rows: &[AddressNameCurrentRow]) -> Result<()> {
    for row in rows {
        if row.address != address {
            bail!(
                "address_names_current address replacement for {address} received row for {}",
                row.address
            );
        }
    }
    Ok(())
}

async fn create_address_names_current_staging_table(pool: &PgPool) -> Result<String> {
    let table_name = format!("address_names_current_rebuild_{}", Uuid::new_v4().simple());
    let table_sql = quote_identifier(&table_name)?;

    sqlx::query(&format!(
        r#"
        CREATE UNLOGGED TABLE {table_sql} (
            LIKE address_names_current INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES
        )
        "#
    ))
    .execute(pool)
    .await
    .context("failed to create address_names_current address replacement staging table")?;

    Ok(table_name)
}

fn quote_identifier(identifier: &str) -> Result<String> {
    if identifier.is_empty()
        || !identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("unsafe SQL identifier {identifier:?}");
    }
    Ok(format!("\"{identifier}\""))
}
