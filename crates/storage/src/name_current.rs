use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Row, postgres::PgRow};
use uuid::Uuid;

use crate::SurfaceBindingKind;
use crate::snapshot_selection::{
    ChainPositions, SnapshotProjectionRead, SnapshotSelectionError,
    ensure_projection_chain_positions_match,
};

const DEFAULT_NAME_CURRENT_READ_FILTER: &str = r#"
  AND surface.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND (
      nc.surface_binding_id IS NULL
      OR (
          resource.canonicality_state IN (
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
              nc.token_lineage_id IS NULL
              OR token_lineage.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
          )
      )
  )
"#;

/// Persisted current exact-name projection row served by API reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentRow {
    pub logical_name_id: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub surface_binding_id: Option<Uuid>,
    pub resource_id: Option<Uuid>,
    pub token_lineage_id: Option<Uuid>,
    pub binding_kind: Option<SurfaceBindingKind>,
    pub declared_summary: Value,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

impl NameCurrentRow {
    /// Load current exact-name projection rows keyed by logical name identity.
    ///
    /// Missing rows are omitted. Duplicate requested ids collapse into one map entry, and map
    /// iteration is sorted by `logical_name_id`; callers that need page order should iterate the
    /// original page and look up rows in the returned map.
    pub async fn load_by_logical_name_ids(
        pool: &PgPool,
        logical_name_ids: &[String],
    ) -> Result<BTreeMap<String, NameCurrentRow>> {
        load_name_current_by_logical_name_ids(pool, logical_name_ids).await
    }
}

/// Load one current exact-name projection row by deterministic logical name identity.
pub async fn load_name_current(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<NameCurrentRow>> {
    let row = sqlx::query(&format!(
        r#"
        SELECT
            nc.logical_name_id,
            nc.namespace,
            nc.canonical_display_name,
            nc.normalized_name,
            nc.namehash,
            nc.surface_binding_id,
            nc.resource_id,
            nc.token_lineage_id,
            nc.binding_kind,
            nc.declared_summary,
            nc.provenance,
            nc.coverage,
            nc.chain_positions,
            nc.canonicality_summary,
            nc.manifest_version,
            nc.last_recomputed_at
        FROM name_current nc
        JOIN name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        WHERE nc.logical_name_id = $1
        {DEFAULT_NAME_CURRENT_READ_FILTER}
        "#,
    ))
    .bind(logical_name_id)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load name_current row for logical_name_id {logical_name_id}")
    })?;

    row.map(decode_name_current_row).transpose()
}

/// Load one exact-name projection row only if it is eligible for the selected snapshot.
///
/// Missing rows stay distinguishable from stale rows so API callers can preserve
/// the route-specific `not_found` behavior without filling stale snapshots from
/// raw facts.
pub async fn load_name_current_for_snapshot(
    pool: &PgPool,
    logical_name_id: &str,
    selected_chain_positions: &ChainPositions,
) -> std::result::Result<SnapshotProjectionRead<NameCurrentRow>, SnapshotSelectionError> {
    let row = load_name_current(pool, logical_name_id)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load name_current row for logical_name_id {logical_name_id}: {error}"
            ))
        })?;

    let Some(row) = row else {
        return Ok(SnapshotProjectionRead::NotFound);
    };

    ensure_projection_chain_positions_match(
        "name_current",
        &row.chain_positions,
        selected_chain_positions,
    )?;
    Ok(SnapshotProjectionRead::Found(row))
}

/// Load current exact-name projection rows for a set of logical name identities.
///
/// The returned map is keyed by `logical_name_id`, so duplicate requested ids collapse into one
/// found row and missing rows are omitted. Iteration order is deterministic `BTreeMap` key order;
/// callers that need request or page order should iterate their original ids and look up into the
/// map.
pub async fn load_name_current_by_logical_name_ids(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, NameCurrentRow>> {
    if logical_name_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query(&format!(
        r#"
        SELECT
            nc.logical_name_id,
            nc.namespace,
            nc.canonical_display_name,
            nc.normalized_name,
            nc.namehash,
            nc.surface_binding_id,
            nc.resource_id,
            nc.token_lineage_id,
            nc.binding_kind,
            nc.declared_summary,
            nc.provenance,
            nc.coverage,
            nc.chain_positions,
            nc.canonicality_summary,
            nc.manifest_version,
            nc.last_recomputed_at
        FROM name_current nc
        JOIN name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        WHERE nc.logical_name_id = ANY($1::TEXT[])
        {DEFAULT_NAME_CURRENT_READ_FILTER}
        ORDER BY nc.logical_name_id
        "#,
    ))
    .bind(logical_name_ids)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load name_current rows for {} logical_name_id values",
            logical_name_ids.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let row = decode_name_current_row(row)?;
            Ok((row.logical_name_id.clone(), row))
        })
        .collect()
}

/// Insert or replace projection rows for exact-name current reads.
pub async fn upsert_name_current_rows(
    pool: &PgPool,
    rows: &[NameCurrentRow],
) -> Result<Vec<NameCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for name_current upsert")?;

    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        validate_name_current_row(row)?;
        snapshots.push(upsert_name_current_row(&mut transaction, row).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit name_current upsert")?;

    Ok(snapshots)
}

/// Delete one current exact-name projection row so a worker can rebuild the key.
pub async fn delete_name_current(pool: &PgPool, logical_name_id: &str) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM name_current
        WHERE logical_name_id = $1
        "#,
    )
    .bind(logical_name_id)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete name_current row for logical_name_id {logical_name_id}")
    })
    .map(|result| result.rows_affected())
}

/// Clear the exact-name current projection so a worker can perform a one-shot rebuild.
pub async fn clear_name_current(pool: &PgPool) -> Result<u64> {
    sqlx::query("DELETE FROM name_current")
        .execute(pool)
        .await
        .context("failed to clear name_current rows")
        .map(|result| result.rows_affected())
}

async fn upsert_name_current_row(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &NameCurrentRow,
) -> Result<NameCurrentRow> {
    let declared_summary = serde_json::to_string(&row.declared_summary)
        .context("failed to serialize name_current declared_summary")?;
    let provenance = serde_json::to_string(&row.provenance)
        .context("failed to serialize name_current provenance")?;
    let coverage = serde_json::to_string(&row.coverage)
        .context("failed to serialize name_current coverage")?;
    let chain_positions = serde_json::to_string(&row.chain_positions)
        .context("failed to serialize name_current chain_positions")?;
    let canonicality_summary = serde_json::to_string(&row.canonicality_summary)
        .context("failed to serialize name_current canonicality_summary")?;

    let snapshot = sqlx::query(
        r#"
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            declared_summary,
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
            $10::jsonb,
            $11::jsonb,
            $12::jsonb,
            $13::jsonb,
            $14::jsonb,
            $15,
            $16
        )
        ON CONFLICT (logical_name_id) DO UPDATE
        SET
            namespace = EXCLUDED.namespace,
            canonical_display_name = EXCLUDED.canonical_display_name,
            normalized_name = EXCLUDED.normalized_name,
            namehash = EXCLUDED.namehash,
            surface_binding_id = EXCLUDED.surface_binding_id,
            resource_id = EXCLUDED.resource_id,
            token_lineage_id = EXCLUDED.token_lineage_id,
            binding_kind = EXCLUDED.binding_kind,
            declared_summary = EXCLUDED.declared_summary,
            provenance = EXCLUDED.provenance,
            coverage = EXCLUDED.coverage,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        "#,
    )
    .bind(&row.logical_name_id)
    .bind(&row.namespace)
    .bind(&row.canonical_display_name)
    .bind(&row.normalized_name)
    .bind(&row.namehash)
    .bind(row.surface_binding_id)
    .bind(row.resource_id)
    .bind(row.token_lineage_id)
    .bind(row.binding_kind.map(SurfaceBindingKind::as_str))
    .bind(declared_summary)
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
            "failed to upsert name_current row for logical_name_id {}",
            row.logical_name_id
        )
    })?;

    decode_name_current_row(snapshot)
}

fn validate_name_current_row(row: &NameCurrentRow) -> Result<()> {
    if row.logical_name_id.trim().is_empty() {
        bail!("name_current row must include logical_name_id");
    }
    if row.namespace.trim().is_empty() {
        bail!(
            "name_current row {} must include namespace",
            row.logical_name_id
        );
    }
    if row.normalized_name.trim().is_empty() {
        bail!(
            "name_current row {} must include normalized_name",
            row.logical_name_id
        );
    }
    if row.canonical_display_name.trim().is_empty() {
        bail!(
            "name_current row {} must include canonical_display_name",
            row.logical_name_id
        );
    }
    if row.namehash.trim().is_empty() {
        bail!(
            "name_current row {} must include namehash",
            row.logical_name_id
        );
    }
    if row.logical_name_id != format!("{}:{}", row.namespace, row.normalized_name) {
        bail!(
            "name_current row {} does not match namespace {} and normalized_name {}",
            row.logical_name_id,
            row.namespace,
            row.normalized_name
        );
    }
    if row.manifest_version <= 0 {
        bail!(
            "name_current row {} has non-positive manifest_version {}",
            row.logical_name_id,
            row.manifest_version
        );
    }

    let has_binding_ref =
        row.surface_binding_id.is_some() || row.resource_id.is_some() || row.binding_kind.is_some();
    if has_binding_ref
        && (row.surface_binding_id.is_none()
            || row.resource_id.is_none()
            || row.binding_kind.is_none())
    {
        bail!(
            "name_current row {} must provide surface_binding_id, resource_id, and binding_kind together",
            row.logical_name_id
        );
    }
    if row.token_lineage_id.is_some() && row.resource_id.is_none() {
        bail!(
            "name_current row {} cannot set token_lineage_id without resource_id",
            row.logical_name_id
        );
    }

    ensure_json_object(
        &row.declared_summary,
        "declared_summary",
        &row.logical_name_id,
    )?;
    ensure_json_object(&row.provenance, "provenance", &row.logical_name_id)?;
    ensure_json_object(&row.coverage, "coverage", &row.logical_name_id)?;
    ensure_json_object(
        &row.chain_positions,
        "chain_positions",
        &row.logical_name_id,
    )?;
    ensure_json_object(
        &row.canonicality_summary,
        "canonicality_summary",
        &row.logical_name_id,
    )?;

    Ok(())
}

fn ensure_json_object(value: &Value, field_name: &str, logical_name_id: &str) -> Result<()> {
    if !value.is_object() {
        bail!(
            "name_current row {} field {} must be a JSON object",
            logical_name_id,
            field_name
        );
    }

    Ok(())
}

fn decode_name_current_row(row: PgRow) -> Result<NameCurrentRow> {
    let binding_kind = row
        .try_get::<Option<String>, _>("binding_kind")
        .context("missing binding_kind")?
        .map(|value| parse_surface_binding_kind(&value))
        .transpose()?;

    Ok(NameCurrentRow {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        surface_binding_id: row
            .try_get("surface_binding_id")
            .context("missing surface_binding_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        token_lineage_id: row
            .try_get("token_lineage_id")
            .context("missing token_lineage_id")?,
        binding_kind,
        declared_summary: row
            .try_get("declared_summary")
            .context("missing declared_summary")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        coverage: row.try_get("coverage").context("missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

fn parse_surface_binding_kind(value: &str) -> Result<SurfaceBindingKind> {
    match value {
        "declared_registry_path" => Ok(SurfaceBindingKind::DeclaredRegistryPath),
        "linked_subregistry_path" => Ok(SurfaceBindingKind::LinkedSubregistryPath),
        "resolver_alias_path" => Ok(SurfaceBindingKind::ResolverAliasPath),
        "observed_wildcard_path" => Ok(SurfaceBindingKind::ObservedWildcardPath),
        "migration_rebind" => Ok(SurfaceBindingKind::MigrationRebind),
        "observed_only" => Ok(SurfaceBindingKind::ObservedOnly),
        _ => bail!("unknown surface binding kind {value}"),
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use serde_json::json;

    use super::*;
    use crate::{
        CanonicalityState, ChainPositions, NameSurface, Resource, SnapshotProjectionRead,
        SnapshotSelectionErrorKind, SurfaceBinding, TokenLineage, upsert_name_surfaces,
        upsert_resources, upsert_surface_bindings, upsert_token_lineages,
    };

    async fn test_database() -> Result<TestDatabase> {
        TestDatabase::create_migrated(
            TestDatabaseConfig::new("bigname_storage_name_current_test")
                .admin_database("postgres")
                .pool_max_connections(5)
                .parse_context("failed to parse database URL for name_current tests")
                .admin_connect_context("failed to connect admin pool for name_current tests")
                .pool_connect_context("failed to connect name_current test pool"),
            &crate::MIGRATOR,
            "failed to apply migrations for name_current tests",
        )
        .await
    }

    fn timestamp(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
    }

    fn token_lineage(token_lineage_id: Uuid) -> TokenLineage {
        TokenLineage {
            token_lineage_id,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xlineage".to_owned(),
            block_number: 21_000_000,
            provenance: json!({"source": "name_current_test", "anchor": "token_lineage"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn resource(resource_id: Uuid, token_lineage_id: Option<Uuid>) -> Resource {
        Resource {
            resource_id,
            token_lineage_id,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xresource".to_owned(),
            block_number: 21_000_001,
            provenance: json!({"source": "name_current_test", "anchor": "resource"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn name_surface(logical_name_id: &str, display_name: &str) -> NameSurface {
        NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "ens".to_owned(),
            input_name: display_name.to_owned(),
            canonical_display_name: display_name.to_owned(),
            normalized_name: display_name.to_owned(),
            dns_encoded_name: display_name.as_bytes().to_vec(),
            namehash: format!("namehash:{display_name}"),
            labelhashes: vec![format!("labelhash:{display_name}")],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xsurface".to_owned(),
            block_number: 21_000_002,
            provenance: json!({"source": "name_current_test", "anchor": "surface"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn surface_binding(
        surface_binding_id: Uuid,
        logical_name_id: &str,
        resource_id: Uuid,
        active_from: OffsetDateTime,
        active_to: Option<OffsetDateTime>,
        block_hash: &str,
        block_number: i64,
    ) -> SurfaceBinding {
        SurfaceBinding {
            surface_binding_id,
            logical_name_id: logical_name_id.to_owned(),
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from,
            active_to,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "name_current_test", "anchor": "binding"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    async fn seed_binding_references(
        database: &TestDatabase,
        logical_name_id: &str,
        display_name: &str,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        upsert_token_lineages(database.pool(), &[token_lineage(token_lineage_id)]).await?;
        upsert_resources(
            database.pool(),
            &[resource(resource_id, Some(token_lineage_id))],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[name_surface(logical_name_id, display_name)],
        )
        .await?;
        upsert_surface_bindings(
            database.pool(),
            &[surface_binding(
                surface_binding_id,
                logical_name_id,
                resource_id,
                timestamp(1_717_171_700),
                None,
                "0xbinding",
                21_000_003,
            )],
        )
        .await?;
        Ok(())
    }

    async fn orphan_resource(database: &TestDatabase, resource_id: Uuid) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE resources
            SET canonicality_state = 'orphaned'::canonicality_state
            WHERE resource_id = $1
            "#,
        )
        .bind(resource_id)
        .execute(database.pool())
        .await?;
        Ok(())
    }

    fn name_current_row(
        logical_name_id: &str,
        surface_binding_id: Uuid,
        resource_id: Uuid,
        token_lineage_id: Uuid,
    ) -> NameCurrentRow {
        NameCurrentRow {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "ens".to_owned(),
            canonical_display_name: "alice.eth".to_owned(),
            normalized_name: "alice.eth".to_owned(),
            namehash: "namehash:alice.eth".to_owned(),
            surface_binding_id: Some(surface_binding_id),
            resource_id: Some(resource_id),
            token_lineage_id: Some(token_lineage_id),
            binding_kind: Some(SurfaceBindingKind::DeclaredRegistryPath),
            declared_summary: json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar"
                },
                "resolver": {
                    "address": "0x0000000000000000000000000000000000000abc"
                }
            }),
            provenance: json!({
                "normalized_event_ids": [101, 102],
                "raw_fact_refs": [{"kind": "log", "chain_id": "ethereum-mainnet", "block_hash": "0xabc"}],
                "manifest_versions": [{"source_manifest_id": 7, "manifest_version": 3}],
                "execution_trace_id": null,
                "derivation_kind": "projection_apply"
            }),
            coverage: json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["ensv1_registry_path"],
                "unsupported_reason": null,
                "enumeration_basis": "exact_name"
            }),
            chain_positions: json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": "0xbinding",
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            }),
            canonicality_summary: json!({
                "status": "finalized",
                "chains": {
                    "ethereum-mainnet": "finalized"
                }
            }),
            manifest_version: 3,
            last_recomputed_at: timestamp(1_717_171_717),
        }
    }

    #[tokio::test]
    async fn name_current_upserts_and_loads_exact_name_projection() -> Result<()> {
        let database = test_database().await?;
        let logical_name_id = "ens:alice.eth";
        let token_lineage_id = Uuid::from_u128(0x1100);
        let resource_id = Uuid::from_u128(0x2200);
        let surface_binding_id = Uuid::from_u128(0x3300);

        seed_binding_references(
            &database,
            logical_name_id,
            "alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

        let expected = name_current_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        );
        let inserted =
            upsert_name_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;
        assert_eq!(inserted, vec![expected.clone()]);

        let loaded = load_name_current(database.pool(), logical_name_id).await?;
        assert_eq!(loaded, Some(expected));

        database.cleanup().await
    }

    #[tokio::test]
    async fn name_current_snapshot_read_fails_stale_on_position_mismatch() -> Result<()> {
        let database = test_database().await?;
        let logical_name_id = "ens:alice.eth";
        let token_lineage_id = Uuid::from_u128(0x1110);
        let resource_id = Uuid::from_u128(0x2220);
        let surface_binding_id = Uuid::from_u128(0x3330);

        seed_binding_references(
            &database,
            logical_name_id,
            "alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

        let expected = name_current_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        );
        upsert_name_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;

        let selected = ChainPositions::from_value(&expected.chain_positions)?;
        assert_eq!(
            load_name_current_for_snapshot(database.pool(), logical_name_id, &selected).await?,
            SnapshotProjectionRead::Found(expected)
        );

        let stale_selected = ChainPositions::from_value(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_004,
                "block_hash": "0xnewer",
                "timestamp": "2026-04-17T00:00:04Z"
            }
        }))?;
        let error =
            load_name_current_for_snapshot(database.pool(), logical_name_id, &stale_selected)
                .await
                .expect_err("mismatched selected snapshot must be stale");
        assert_eq!(error.kind(), SnapshotSelectionErrorKind::Stale);

        database.cleanup().await
    }

    #[tokio::test]
    async fn name_current_batch_loads_found_rows_by_logical_name_id() -> Result<()> {
        let database = test_database().await?;
        let alice_logical_name_id = "ens:alice.eth";
        let bob_logical_name_id = "ens:bob.eth";

        seed_binding_references(
            &database,
            alice_logical_name_id,
            "alice.eth",
            Uuid::from_u128(0x9200),
            Uuid::from_u128(0x9100),
            Uuid::from_u128(0x9300),
        )
        .await?;
        seed_binding_references(
            &database,
            bob_logical_name_id,
            "bob.eth",
            Uuid::from_u128(0xa200),
            Uuid::from_u128(0xa100),
            Uuid::from_u128(0xa300),
        )
        .await?;

        let alice = name_current_row(
            alice_logical_name_id,
            Uuid::from_u128(0x9300),
            Uuid::from_u128(0x9200),
            Uuid::from_u128(0x9100),
        );
        let mut bob = name_current_row(
            bob_logical_name_id,
            Uuid::from_u128(0xa300),
            Uuid::from_u128(0xa200),
            Uuid::from_u128(0xa100),
        );
        bob.canonical_display_name = "bob.eth".to_owned();
        bob.normalized_name = "bob.eth".to_owned();
        bob.namehash = "namehash:bob.eth".to_owned();

        upsert_name_current_rows(database.pool(), &[alice.clone(), bob.clone()]).await?;

        let requested = vec![
            bob_logical_name_id.to_owned(),
            "ens:missing.eth".to_owned(),
            alice_logical_name_id.to_owned(),
            bob_logical_name_id.to_owned(),
        ];
        let loaded = load_name_current_by_logical_name_ids(database.pool(), &requested).await?;

        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded.keys().cloned().collect::<Vec<_>>(),
            vec![
                alice_logical_name_id.to_owned(),
                bob_logical_name_id.to_owned()
            ]
        );
        assert_eq!(loaded.get(alice_logical_name_id), Some(&alice));
        assert_eq!(loaded.get(bob_logical_name_id), Some(&bob));
        assert!(!loaded.contains_key("ens:missing.eth"));
        assert_eq!(
            NameCurrentRow::load_by_logical_name_ids(database.pool(), &requested).await?,
            loaded
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn name_current_excludes_rows_with_orphaned_backing_resources() -> Result<()> {
        let database = test_database().await?;
        let logical_name_id = "ens:alice.eth";
        let token_lineage_id = Uuid::from_u128(0xb100);
        let resource_id = Uuid::from_u128(0xb200);
        let surface_binding_id = Uuid::from_u128(0xb300);

        seed_binding_references(
            &database,
            logical_name_id,
            "alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
        upsert_name_current_rows(
            database.pool(),
            &[name_current_row(
                logical_name_id,
                surface_binding_id,
                resource_id,
                token_lineage_id,
            )],
        )
        .await?;

        orphan_resource(&database, resource_id).await?;

        assert_eq!(
            load_name_current(database.pool(), logical_name_id).await?,
            None
        );

        let loaded =
            load_name_current_by_logical_name_ids(database.pool(), &[logical_name_id.to_owned()])
                .await?;
        assert!(loaded.is_empty());
        assert_eq!(
            NameCurrentRow::load_by_logical_name_ids(
                database.pool(),
                &[logical_name_id.to_owned()]
            )
            .await?,
            loaded
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn name_current_upsert_replaces_existing_projection_row() -> Result<()> {
        let database = test_database().await?;
        let logical_name_id = "ens:alice.eth";
        let first_token_lineage_id = Uuid::from_u128(0x4100);
        let first_resource_id = Uuid::from_u128(0x4200);
        let first_surface_binding_id = Uuid::from_u128(0x4300);

        seed_binding_references(
            &database,
            logical_name_id,
            "alice.eth",
            first_resource_id,
            first_token_lineage_id,
            first_surface_binding_id,
        )
        .await?;

        let first = name_current_row(
            logical_name_id,
            first_surface_binding_id,
            first_resource_id,
            first_token_lineage_id,
        );
        upsert_name_current_rows(database.pool(), std::slice::from_ref(&first)).await?;

        let mut replacement = name_current_row(
            logical_name_id,
            first_surface_binding_id,
            first_resource_id,
            first_token_lineage_id,
        );
        replacement.declared_summary = json!({
            "registration": {
                "status": "wrapped",
                "authority_kind": "wrapper"
            }
        });
        replacement.coverage = json!({
            "status": "partial",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path", "wrapped_name"],
            "unsupported_reason": null,
            "enumeration_basis": "exact_name"
        });
        replacement.manifest_version = 4;

        let updated =
            upsert_name_current_rows(database.pool(), std::slice::from_ref(&replacement)).await?;
        assert_eq!(updated, vec![replacement.clone()]);
        assert_eq!(
            load_name_current(database.pool(), logical_name_id).await?,
            Some(replacement)
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn name_current_delete_and_clear_support_rebuild_workflows() -> Result<()> {
        let database = test_database().await?;
        let first_logical_name_id = "ens:alice.eth";
        let second_logical_name_id = "ens:bob.eth";

        seed_binding_references(
            &database,
            first_logical_name_id,
            "alice.eth",
            Uuid::from_u128(0x6200),
            Uuid::from_u128(0x6100),
            Uuid::from_u128(0x6300),
        )
        .await?;
        seed_binding_references(
            &database,
            second_logical_name_id,
            "bob.eth",
            Uuid::from_u128(0x7200),
            Uuid::from_u128(0x7100),
            Uuid::from_u128(0x7300),
        )
        .await?;

        let first = name_current_row(
            first_logical_name_id,
            Uuid::from_u128(0x6300),
            Uuid::from_u128(0x6200),
            Uuid::from_u128(0x6100),
        );
        let mut second = name_current_row(
            second_logical_name_id,
            Uuid::from_u128(0x7300),
            Uuid::from_u128(0x7200),
            Uuid::from_u128(0x7100),
        );
        second.canonical_display_name = "bob.eth".to_owned();
        second.normalized_name = "bob.eth".to_owned();
        second.namehash = "namehash:bob.eth".to_owned();
        second.chain_positions = json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_004,
                "block_hash": "0xbbbb",
                "timestamp": "2026-04-17T00:00:04Z"
            }
        });

        upsert_name_current_rows(database.pool(), &[first, second]).await?;

        assert_eq!(
            delete_name_current(database.pool(), first_logical_name_id).await?,
            1
        );
        assert_eq!(
            load_name_current(database.pool(), first_logical_name_id).await?,
            None
        );

        assert_eq!(clear_name_current(database.pool()).await?, 1);
        assert_eq!(
            load_name_current(database.pool(), second_logical_name_id).await?,
            None
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn name_current_rejects_partial_binding_refs() -> Result<()> {
        let database = test_database().await?;
        let logical_name_id = "ens:alice.eth";

        upsert_name_surfaces(
            database.pool(),
            &[name_surface(logical_name_id, "alice.eth")],
        )
        .await?;

        let invalid = NameCurrentRow {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "ens".to_owned(),
            canonical_display_name: "alice.eth".to_owned(),
            normalized_name: "alice.eth".to_owned(),
            namehash: "namehash:alice.eth".to_owned(),
            surface_binding_id: None,
            resource_id: Some(Uuid::from_u128(0x8200)),
            token_lineage_id: None,
            binding_kind: Some(SurfaceBindingKind::DeclaredRegistryPath),
            declared_summary: json!({}),
            provenance: json!({}),
            coverage: json!({}),
            chain_positions: json!({}),
            canonicality_summary: json!({}),
            manifest_version: 1,
            last_recomputed_at: timestamp(1_717_171_800),
        };

        let error = upsert_name_current_rows(database.pool(), &[invalid])
            .await
            .expect_err("partial binding refs must be rejected");
        assert!(
            error.to_string().contains(
                "must provide surface_binding_id, resource_id, and binding_kind together"
            ),
            "unexpected error: {error:#}"
        );

        database.cleanup().await
    }
}
