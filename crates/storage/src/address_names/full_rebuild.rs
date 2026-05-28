use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{types::AddressNameCurrentRow, write};

#[derive(Clone, Debug)]
pub struct AddressNamesCurrentFullRebuild {
    table_name: String,
    previous_row_count: u64,
}

impl AddressNamesCurrentFullRebuild {
    pub fn previous_row_count(&self) -> u64 {
        self.previous_row_count
    }

    fn table_sql(&self) -> String {
        quote_identifier(&self.table_name).expect("generated table name must be safe")
    }
}

pub async fn begin_address_names_current_full_rebuild(
    pool: &PgPool,
) -> Result<AddressNamesCurrentFullRebuild> {
    let previous_row_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM address_names_current
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to count address_names_current rows before full rebuild")?;

    let table_name = create_address_names_current_staging_table(pool, "full rebuild").await?;

    Ok(AddressNamesCurrentFullRebuild {
        table_name,
        previous_row_count: previous_row_count as u64,
    })
}

pub async fn drop_address_names_current_full_rebuild(
    pool: &PgPool,
    rebuild: &AddressNamesCurrentFullRebuild,
) -> Result<()> {
    drop_address_names_current_staging_table(pool, &rebuild.table_sql(), "full rebuild").await
}

pub async fn insert_address_names_current_full_rebuild_rows(
    pool: &PgPool,
    rebuild: &AddressNamesCurrentFullRebuild,
    rows: &[AddressNameCurrentRow],
) -> Result<Vec<AddressNameCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let table_sql = rebuild.table_sql();
    insert_address_names_current_staging_rows(pool, &table_sql, rows, "full rebuild").await
}

async fn create_address_names_current_staging_table(
    pool: &PgPool,
    purpose: &str,
) -> Result<String> {
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
    .with_context(|| format!("failed to create address_names_current {purpose} staging table"))?;

    Ok(table_name)
}

async fn drop_address_names_current_staging_table(
    pool: &PgPool,
    table_sql: &str,
    purpose: &str,
) -> Result<()> {
    sqlx::query(&format!("DROP TABLE IF EXISTS {table_sql}"))
        .execute(pool)
        .await
        .with_context(|| format!("failed to drop address_names_current {purpose} staging table"))?;
    Ok(())
}

async fn insert_address_names_current_staging_rows(
    pool: &PgPool,
    table_sql: &str,
    rows: &[AddressNameCurrentRow],
    purpose: &str,
) -> Result<Vec<AddressNameCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool.begin().await.with_context(|| {
        format!("failed to open transaction for address_names_current {purpose} staging")
    })?;

    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        snapshots.push(
            write::upsert_address_name_current_row_into_table(&mut transaction, table_sql, row)
                .await?,
        );
    }

    transaction.commit().await.with_context(|| {
        format!("failed to commit address_names_current {purpose} staging batch")
    })?;

    Ok(snapshots)
}

pub async fn publish_address_names_current_full_rebuild(
    pool: &PgPool,
    rebuild: &AddressNamesCurrentFullRebuild,
) -> Result<(u64, u64)> {
    let table_sql = rebuild.table_sql();

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for address_names_current full rebuild publish")?;

    write::set_address_names_current_sidecar_triggers(&mut transaction, false).await?;

    sqlx::query(
        r#"
        TRUNCATE TABLE
            address_names_current,
            address_names_current_identity_counts,
            address_names_current_identity_feed
        "#,
    )
    .execute(&mut *transaction)
    .await
    .context("failed to truncate address_names_current projection and identity sidecars")?;

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
        "#
    ))
    .execute(&mut *transaction)
    .await
    .context("failed to publish staged address_names_current rows")?
    .rows_affected();

    write::set_address_names_current_sidecar_triggers(&mut transaction, true).await?;
    rebuild_address_names_current_identity_sidecars_in_transaction(&mut transaction).await?;

    transaction
        .commit()
        .await
        .context("failed to commit address_names_current full rebuild publish")?;

    Ok((rebuild.previous_row_count, inserted_row_count))
}

/// Rebuild address-name identity sidecars from the current public projection.
pub async fn rebuild_address_names_current_identity_sidecars(pool: &PgPool) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for address_names_current sidecar rebuild")?;

    rebuild_address_names_current_identity_sidecars_in_transaction(&mut transaction).await?;

    transaction
        .commit()
        .await
        .context("failed to commit address_names_current sidecar rebuild")?;

    Ok(())
}

async fn rebuild_address_names_current_identity_sidecars_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        r#"
        TRUNCATE TABLE address_names_current_identity_counts
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to truncate address_names_current_identity_counts")?;

    sqlx::query(
        r#"
        WITH relation_groups AS (
            SELECT
                anc.address,
                anc.logical_name_id,
                BOOL_OR(anc.relation IN ('registrant', 'token_holder')) AS owned,
                BOOL_OR(anc.relation = 'effective_controller') AS managed
            FROM address_names_current anc
            JOIN name_surfaces surface
              ON surface.logical_name_id = anc.logical_name_id
            JOIN resources resource
              ON resource.resource_id = anc.resource_id
            JOIN surface_bindings binding
              ON binding.surface_binding_id = anc.surface_binding_id
            LEFT JOIN token_lineages token_lineage
              ON token_lineage.token_lineage_id = anc.token_lineage_id
            JOIN name_current identity_nc
              ON identity_nc.logical_name_id = anc.logical_name_id
            JOIN name_surfaces identity_nc_surface
              ON identity_nc_surface.logical_name_id = identity_nc.logical_name_id
            LEFT JOIN resources identity_nc_resource
              ON identity_nc_resource.resource_id = identity_nc.resource_id
            LEFT JOIN surface_bindings identity_nc_binding
              ON identity_nc_binding.surface_binding_id = identity_nc.surface_binding_id
            LEFT JOIN token_lineages identity_nc_token_lineage
              ON identity_nc_token_lineage.token_lineage_id = identity_nc.token_lineage_id
            WHERE surface.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND resource.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND binding.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND (
                  anc.token_lineage_id IS NULL
                  OR token_lineage.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
              )
              AND identity_nc_surface.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND (
                  identity_nc.surface_binding_id IS NULL
                  OR (
                      identity_nc_resource.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                      AND identity_nc_binding.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                      AND (
                          identity_nc.token_lineage_id IS NULL
                          OR identity_nc_token_lineage.canonicality_state IN (
                              'canonical'::canonicality_state,
                              'safe'::canonicality_state,
                              'finalized'::canonicality_state
                          )
                      )
                  )
              )
            GROUP BY anc.address, anc.logical_name_id
        ),
        counts AS (
            SELECT address, 'owned'::text AS roles, COUNT(*)::bigint AS total_count
            FROM relation_groups
            WHERE owned
            GROUP BY address
            UNION ALL
            SELECT address, 'managed'::text AS roles, COUNT(*)::bigint AS total_count
            FROM relation_groups
            WHERE managed
            GROUP BY address
            UNION ALL
            SELECT address, 'both'::text AS roles, COUNT(*)::bigint AS total_count
            FROM relation_groups
            GROUP BY address
        )
        INSERT INTO address_names_current_identity_counts (address, roles, total_count)
        SELECT address, roles, total_count
        FROM counts
        WHERE total_count > 0
        ON CONFLICT (address, roles) DO UPDATE
        SET
            total_count = EXCLUDED.total_count,
            updated_at = now()
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to rebuild address_names_current_identity_counts")?;

    sqlx::query(
        r#"
        TRUNCATE TABLE address_names_current_identity_feed
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to truncate address_names_current_identity_feed")?;

    sqlx::query(
        r#"
        INSERT INTO address_names_current_identity_feed (
            address,
            roles,
            coin_type,
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            chain_positions,
            coverage,
            is_primary,
            relation_facets,
            last_recomputed_at
        )
        SELECT
            candidate.address,
            candidate.roles,
            candidate.coin_type,
            candidate.logical_name_id,
            candidate.namespace,
            candidate.canonical_display_name,
            candidate.normalized_name,
            candidate.namehash,
            candidate.chain_positions,
            candidate.coverage,
            candidate.is_primary,
            candidate.relation_facets,
            now()
        FROM address_names_current_identity_feed_candidate_rows(NULL::text) candidate
        ON CONFLICT (address, roles, coin_type) DO UPDATE
        SET
            logical_name_id = EXCLUDED.logical_name_id,
            namespace = EXCLUDED.namespace,
            canonical_display_name = EXCLUDED.canonical_display_name,
            normalized_name = EXCLUDED.normalized_name,
            namehash = EXCLUDED.namehash,
            chain_positions = EXCLUDED.chain_positions,
            coverage = EXCLUDED.coverage,
            is_primary = EXCLUDED.is_primary,
            relation_facets = EXCLUDED.relation_facets,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        "#,
    )
    .execute(&mut **transaction)
    .await
    .context("failed to rebuild address_names_current_identity_feed")?;

    Ok(())
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
