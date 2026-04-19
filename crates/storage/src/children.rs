use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Row, postgres::PgRow};

const DECLARED_SURFACE_CLASS: &str = "declared";
const SUBREGISTRY_EVENT_KIND: &str = "SubregistryChanged";
const SUBREGISTRY_DERIVATION_KIND: &str = "ens_v1_subregistry_changed";
const ENSV1_SUBREGISTRY_SOURCE_FAMILY: &str = "ens_v1_registry_l1";
const BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY: &str = "basenames_base_registry";
const DEFAULT_CHILDREN_CURRENT_READ_FILTER: &str = r#"
  AND parent.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND child.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
"#;

/// Persisted current child-collection row for declared direct children only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenCurrentRow {
    pub parent_logical_name_id: String,
    pub child_logical_name_id: String,
    pub surface_class: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub provenance: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Canonical declared-child subregistry event seed for rebuilding declared child rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeclaredChildEventSource {
    pub parent_logical_name_id: String,
    pub child_logical_name_id: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub normalized_event_id: i64,
    pub event_identity: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub log_index: i64,
    pub raw_fact_ref: Value,
}

/// Load declared direct child rows for one parent from the default canonical read set.
pub async fn load_children_current(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<Vec<ChildrenCurrentRow>> {
    load_children_current_internal(pool, parent_logical_name_id, false).await
}

/// Load declared direct child rows for one parent, including noncanonical parent or child surfaces.
pub async fn load_children_current_including_noncanonical(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<Vec<ChildrenCurrentRow>> {
    load_children_current_internal(pool, parent_logical_name_id, true).await
}

/// Insert or replace current declared child rows for one or more parents.
pub async fn upsert_children_current_rows(
    pool: &PgPool,
    rows: &[ChildrenCurrentRow],
) -> Result<Vec<ChildrenCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for children_current upsert")?;

    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        validate_children_current_row(row)?;
        snapshots.push(upsert_children_current_row(&mut transaction, row).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit children_current upsert")?;

    Ok(snapshots)
}

/// Delete all declared child rows for one parent so a worker can rebuild that collection key.
pub async fn delete_children_current(pool: &PgPool, parent_logical_name_id: &str) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM children_current
        WHERE parent_logical_name_id = $1
          AND surface_class = $2
        "#,
    )
    .bind(parent_logical_name_id)
    .bind(DECLARED_SURFACE_CLASS)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to delete children_current rows for parent_logical_name_id {parent_logical_name_id}"
        )
    })
    .map(|result| result.rows_affected())
}

/// Clear the declared direct-child projection so a worker can perform a one-shot rebuild.
pub async fn clear_children_current(pool: &PgPool) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM children_current
        WHERE surface_class = $1
        "#,
    )
    .bind(DECLARED_SURFACE_CLASS)
    .execute(pool)
    .await
    .context("failed to clear children_current rows")
    .map(|result| result.rows_affected())
}

/// Load the latest canonical declared-child subregistry event per child surface.
pub async fn load_canonical_declared_child_sources(
    pool: &PgPool,
    parent_logical_name_id: Option<&str>,
) -> Result<Vec<DeclaredChildEventSource>> {
    let rows = sqlx::query(
        r#"
        WITH ranked_sources AS (
            SELECT
                parent.logical_name_id AS parent_logical_name_id,
                child.logical_name_id AS child_logical_name_id,
                child.namespace,
                child.canonical_display_name,
                child.normalized_name,
                child.namehash,
                ne.normalized_event_id,
                ne.event_identity,
                ne.source_family,
                ne.manifest_version,
                ne.source_manifest_id,
                ne.chain_id,
                ne.block_number,
                ne.block_hash,
                ne.transaction_hash,
                ne.log_index,
                ne.raw_fact_ref,
                COALESCE((ne.after_state ->> 'tombstone')::BOOLEAN, FALSE) AS tombstone,
                COALESCE((ne.after_state ->> 'active_edge')::BOOLEAN, FALSE) AS active_edge,
                ROW_NUMBER() OVER (
                    PARTITION BY child.logical_name_id
                    ORDER BY
                        ne.block_number DESC,
                        ne.log_index DESC,
                        ne.normalized_event_id DESC
                ) AS current_child_rank
            FROM normalized_events ne
            JOIN name_surfaces parent
              ON parent.namehash = ne.after_state ->> 'parent_node'
            JOIN name_surfaces child
              ON child.namehash = ne.after_state ->> 'child_node'
            WHERE ne.event_kind = $1
              AND ne.derivation_kind = $2
              AND ne.source_family IN ($3, $4)
              AND parent.namespace = child.namespace
              AND parent.namespace = ne.namespace
              AND child.namespace = ne.namespace
              AND parent.chain_id = child.chain_id
              AND parent.chain_id = ne.chain_id
              AND child.chain_id = ne.chain_id
              AND ne.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
              AND parent.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
              AND child.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
        )
        SELECT
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            normalized_event_id,
            event_identity,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref
        FROM ranked_sources
        WHERE current_child_rank = 1
          AND tombstone = FALSE
          AND active_edge = TRUE
          AND ($5::TEXT IS NULL OR parent_logical_name_id = $5)
        ORDER BY
            parent_logical_name_id ASC,
            canonical_display_name ASC,
            child_logical_name_id ASC
        "#,
    )
    .bind(SUBREGISTRY_EVENT_KIND)
    .bind(SUBREGISTRY_DERIVATION_KIND)
    .bind(ENSV1_SUBREGISTRY_SOURCE_FAMILY)
    .bind(BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY)
    .bind(parent_logical_name_id)
    .fetch_all(pool)
    .await
    .with_context(|| match parent_logical_name_id {
        Some(parent_logical_name_id) => format!(
            "failed to load canonical declared child sources for parent_logical_name_id {parent_logical_name_id}"
        ),
        None => "failed to load canonical declared child sources".to_owned(),
    })?;

    rows.into_iter()
        .map(decode_declared_child_event_source)
        .collect()
}

/// Back-compat alias for the generalized declared-child source loader.
pub async fn load_canonical_ens_v1_declared_child_sources(
    pool: &PgPool,
    parent_logical_name_id: Option<&str>,
) -> Result<Vec<DeclaredChildEventSource>> {
    load_canonical_declared_child_sources(pool, parent_logical_name_id).await
}

async fn load_children_current_internal(
    pool: &PgPool,
    parent_logical_name_id: &str,
    include_noncanonical: bool,
) -> Result<Vec<ChildrenCurrentRow>> {
    let read_filter = if include_noncanonical {
        ""
    } else {
        DEFAULT_CHILDREN_CURRENT_READ_FILTER
    };

    let query = format!(
        r#"
        SELECT
            cc.parent_logical_name_id,
            cc.child_logical_name_id,
            cc.surface_class,
            cc.namespace,
            cc.canonical_display_name,
            cc.normalized_name,
            cc.namehash,
            cc.provenance,
            cc.chain_positions,
            cc.canonicality_summary,
            cc.manifest_version,
            cc.last_recomputed_at
        FROM children_current cc
        JOIN name_surfaces parent
          ON parent.logical_name_id = cc.parent_logical_name_id
        JOIN name_surfaces child
          ON child.logical_name_id = cc.child_logical_name_id
        WHERE cc.parent_logical_name_id = $1
          AND cc.surface_class = $2
        {read_filter}
        ORDER BY
            cc.canonical_display_name ASC,
            cc.child_logical_name_id ASC
        "#
    );

    let rows = sqlx::query(&query)
        .bind(parent_logical_name_id)
        .bind(DECLARED_SURFACE_CLASS)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load children_current rows for parent_logical_name_id {parent_logical_name_id}"
            )
        })?;

    rows.into_iter().map(decode_children_current_row).collect()
}

async fn upsert_children_current_row(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &ChildrenCurrentRow,
) -> Result<ChildrenCurrentRow> {
    let provenance = serde_json::to_string(&row.provenance)
        .context("failed to serialize children_current provenance")?;
    let chain_positions = serde_json::to_string(&row.chain_positions)
        .context("failed to serialize children_current chain_positions")?;
    let canonicality_summary = serde_json::to_string(&row.canonicality_summary)
        .context("failed to serialize children_current canonicality_summary")?;

    let snapshot = sqlx::query(
        r#"
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            surface_class,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            provenance,
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
            $8::jsonb,
            $9::jsonb,
            $10::jsonb,
            $11,
            $12
        )
        ON CONFLICT (parent_logical_name_id, child_logical_name_id, surface_class) DO UPDATE
        SET
            namespace = EXCLUDED.namespace,
            canonical_display_name = EXCLUDED.canonical_display_name,
            normalized_name = EXCLUDED.normalized_name,
            namehash = EXCLUDED.namehash,
            provenance = EXCLUDED.provenance,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING
            parent_logical_name_id,
            child_logical_name_id,
            surface_class,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            provenance,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        "#,
    )
    .bind(&row.parent_logical_name_id)
    .bind(&row.child_logical_name_id)
    .bind(&row.surface_class)
    .bind(&row.namespace)
    .bind(&row.canonical_display_name)
    .bind(&row.normalized_name)
    .bind(&row.namehash)
    .bind(provenance)
    .bind(chain_positions)
    .bind(canonicality_summary)
    .bind(row.manifest_version)
    .bind(row.last_recomputed_at)
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert children_current row for parent_logical_name_id {} child_logical_name_id {}",
            row.parent_logical_name_id,
            row.child_logical_name_id
        )
    })?;

    decode_children_current_row(snapshot)
}

fn validate_children_current_row(row: &ChildrenCurrentRow) -> Result<()> {
    if row.parent_logical_name_id.trim().is_empty() {
        bail!("children_current row must include parent_logical_name_id");
    }
    if row.child_logical_name_id.trim().is_empty() {
        bail!("children_current row must include child_logical_name_id");
    }
    if row.parent_logical_name_id == row.child_logical_name_id {
        bail!(
            "children_current row {} cannot target itself as a child",
            row.parent_logical_name_id
        );
    }
    if row.surface_class != DECLARED_SURFACE_CLASS {
        bail!(
            "children_current row {} -> {} must use declared surface_class",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.namespace.trim().is_empty() {
        bail!(
            "children_current row {} -> {} must include namespace",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.normalized_name.trim().is_empty() {
        bail!(
            "children_current row {} -> {} must include normalized_name",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.canonical_display_name.trim().is_empty() {
        bail!(
            "children_current row {} -> {} must include canonical_display_name",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.namehash.trim().is_empty() {
        bail!(
            "children_current row {} -> {} must include namehash",
            row.parent_logical_name_id,
            row.child_logical_name_id
        );
    }
    if row.child_logical_name_id != format!("{}:{}", row.namespace, row.normalized_name) {
        bail!(
            "children_current row {} -> {} does not match namespace {} and normalized_name {}",
            row.parent_logical_name_id,
            row.child_logical_name_id,
            row.namespace,
            row.normalized_name
        );
    }
    if row.manifest_version <= 0 {
        bail!(
            "children_current row {} -> {} has non-positive manifest_version {}",
            row.parent_logical_name_id,
            row.child_logical_name_id,
            row.manifest_version
        );
    }

    ensure_json_object(
        &row.provenance,
        "provenance",
        &row.parent_logical_name_id,
        &row.child_logical_name_id,
    )?;
    ensure_json_object(
        &row.chain_positions,
        "chain_positions",
        &row.parent_logical_name_id,
        &row.child_logical_name_id,
    )?;
    ensure_json_object(
        &row.canonicality_summary,
        "canonicality_summary",
        &row.parent_logical_name_id,
        &row.child_logical_name_id,
    )?;

    Ok(())
}

fn ensure_json_object(
    value: &Value,
    field_name: &str,
    parent_logical_name_id: &str,
    child_logical_name_id: &str,
) -> Result<()> {
    if !value.is_object() {
        bail!(
            "children_current row {} -> {} field {} must be a JSON object",
            parent_logical_name_id,
            child_logical_name_id,
            field_name
        );
    }

    Ok(())
}

fn decode_children_current_row(row: PgRow) -> Result<ChildrenCurrentRow> {
    let surface_class = row
        .try_get::<String, _>("surface_class")
        .context("missing surface_class")?;
    if surface_class != DECLARED_SURFACE_CLASS {
        bail!("unknown children_current surface_class {surface_class}");
    }

    Ok(ChildrenCurrentRow {
        parent_logical_name_id: row
            .try_get("parent_logical_name_id")
            .context("missing parent_logical_name_id")?,
        child_logical_name_id: row
            .try_get("child_logical_name_id")
            .context("missing child_logical_name_id")?,
        surface_class,
        namespace: row.try_get("namespace").context("missing namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
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

fn decode_declared_child_event_source(row: PgRow) -> Result<DeclaredChildEventSource> {
    Ok(DeclaredChildEventSource {
        parent_logical_name_id: row
            .try_get("parent_logical_name_id")
            .context("missing parent_logical_name_id")?,
        child_logical_name_id: row
            .try_get("child_logical_name_id")
            .context("missing child_logical_name_id")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        normalized_event_id: row
            .try_get("normalized_event_id")
            .context("missing normalized_event_id")?,
        event_identity: row
            .try_get("event_identity")
            .context("missing event_identity")?,
        source_family: row
            .try_get("source_family")
            .context("missing source_family")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        source_manifest_id: row
            .try_get("source_manifest_id")
            .context("missing source_manifest_id")?,
        chain_id: row
            .try_get::<Option<String>, _>("chain_id")
            .context("missing chain_id")?
            .context("declared child source is missing chain_id")?,
        block_number: row
            .try_get::<Option<i64>, _>("block_number")
            .context("missing block_number")?
            .context("declared child source is missing block_number")?,
        block_hash: row
            .try_get::<Option<String>, _>("block_hash")
            .context("missing block_hash")?
            .context("declared child source is missing block_hash")?,
        transaction_hash: row
            .try_get::<Option<String>, _>("transaction_hash")
            .context("missing transaction_hash")?
            .context("declared child source is missing transaction_hash")?,
        log_index: row
            .try_get::<Option<i64>, _>("log_index")
            .context("missing log_index")?
            .context("declared child source is missing log_index")?,
        raw_fact_ref: row
            .try_get("raw_fact_ref")
            .context("missing raw_fact_ref")?,
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use anyhow::Result;
    use serde_json::json;
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
        types::Uuid,
    };

    use super::*;
    use crate::{
        CanonicalityState, NameSurface, NormalizedEvent, default_database_url,
        upsert_name_surfaces, upsert_normalized_events,
    };

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
                .context("failed to parse database URL for children_current tests")?;
            let database_name = format!(
                "bigname_storage_children_current_test_{}_{}",
                std::process::id(),
                Uuid::new_v4().simple()
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for children_current tests")?;

            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                database_name
            ))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to drop stale test database {database_name}"))?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect children_current test pool")?;

            crate::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for children_current tests")?;

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

    fn timestamp(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
    }

    fn name_surface(
        logical_name_id: &str,
        display_name: &str,
        namehash: &str,
        block_number: i64,
        canonicality_state: CanonicalityState,
    ) -> NameSurface {
        name_surface_on_chain(
            logical_name_id,
            display_name,
            namehash,
            "ethereum-mainnet",
            block_number,
            canonicality_state,
        )
    }

    fn name_surface_on_chain(
        logical_name_id: &str,
        display_name: &str,
        namehash: &str,
        chain_id: &str,
        block_number: i64,
        canonicality_state: CanonicalityState,
    ) -> NameSurface {
        let namespace = logical_name_id
            .split_once(':')
            .map(|(namespace, _)| namespace)
            .expect("logical_name_id must include namespace")
            .to_owned();

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
            chain_id: chain_id.to_owned(),
            block_hash: format!("0xsurface{block_number:02x}"),
            block_number,
            provenance: json!({"source": "children_current_test", "kind": "surface"}),
            canonicality_state,
        }
    }

    fn children_current_row(
        parent_logical_name_id: &str,
        child_logical_name_id: &str,
        display_name: &str,
        namehash: &str,
        block_number: i64,
    ) -> ChildrenCurrentRow {
        ChildrenCurrentRow {
            parent_logical_name_id: parent_logical_name_id.to_owned(),
            child_logical_name_id: child_logical_name_id.to_owned(),
            surface_class: DECLARED_SURFACE_CLASS.to_owned(),
            namespace: "ens".to_owned(),
            canonical_display_name: display_name.to_owned(),
            normalized_name: display_name.to_owned(),
            namehash: namehash.to_owned(),
            provenance: json!({
                "normalized_event_ids": [block_number],
                "derivation_kind": "children_current_rebuild"
            }),
            chain_positions: json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": block_number,
                    "block_hash": format!("0xblock{block_number:02x}"),
                    "timestamp": "2026-04-17T00:00:00Z"
                }
            }),
            canonicality_summary: json!({
                "status": "finalized",
                "chains": {
                    "ethereum-mainnet": "finalized"
                }
            }),
            manifest_version: 1,
            last_recomputed_at: timestamp(1_717_172_000 + block_number),
        }
    }

    fn subregistry_event(
        event_identity: &str,
        namespace: &str,
        source_family: &str,
        chain_id: &str,
        parent_namehash: &str,
        child_namehash: &str,
        block_number: i64,
        log_index: i64,
        canonicality_state: CanonicalityState,
        tombstone: bool,
        active_edge: bool,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: namespace.to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: SUBREGISTRY_EVENT_KIND.to_owned(),
            source_family: source_family.to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some(chain_id.to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xeventblock{block_number:02x}")),
            transaction_hash: Some(format!("0xtx{block_number:02x}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": chain_id,
                "block_number": block_number,
                "log_index": log_index
            }),
            derivation_kind: SUBREGISTRY_DERIVATION_KIND.to_owned(),
            canonicality_state,
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

    #[tokio::test]
    async fn children_current_upserts_and_loads_declared_rows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let parent_logical_name_id = "ens:parent.eth";
        let child_logical_name_id = "ens:alice.parent.eth";

        upsert_name_surfaces(
            database.pool(),
            &[
                name_surface(
                    parent_logical_name_id,
                    "parent.eth",
                    "node:parent.eth",
                    10,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    child_logical_name_id,
                    "alice.parent.eth",
                    "node:alice.parent.eth",
                    11,
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;

        let expected = children_current_row(
            parent_logical_name_id,
            child_logical_name_id,
            "alice.parent.eth",
            "node:alice.parent.eth",
            11,
        );

        let inserted =
            upsert_children_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;
        assert_eq!(inserted, vec![expected.clone()]);
        assert_eq!(
            load_children_current(database.pool(), parent_logical_name_id).await?,
            vec![expected.clone()]
        );

        assert_eq!(
            delete_children_current(database.pool(), parent_logical_name_id).await?,
            1
        );
        assert!(
            load_children_current(database.pool(), parent_logical_name_id)
                .await?
                .is_empty()
        );

        upsert_children_current_rows(database.pool(), &[expected]).await?;
        assert_eq!(clear_children_current(database.pool()).await?, 1);

        database.cleanup().await
    }

    #[tokio::test]
    async fn children_current_load_orders_by_display_name() -> Result<()> {
        let database = TestDatabase::new().await?;
        let parent_logical_name_id = "ens:parent.eth";

        upsert_name_surfaces(
            database.pool(),
            &[
                name_surface(
                    parent_logical_name_id,
                    "parent.eth",
                    "node:parent.eth",
                    20,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    "ens:bob.parent.eth",
                    "bob.parent.eth",
                    "node:bob.parent.eth",
                    21,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    "ens:alice.parent.eth",
                    "alice.parent.eth",
                    "node:alice.parent.eth",
                    22,
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;

        let bob = children_current_row(
            parent_logical_name_id,
            "ens:bob.parent.eth",
            "bob.parent.eth",
            "node:bob.parent.eth",
            21,
        );
        let alice = children_current_row(
            parent_logical_name_id,
            "ens:alice.parent.eth",
            "alice.parent.eth",
            "node:alice.parent.eth",
            22,
        );
        upsert_children_current_rows(database.pool(), &[bob.clone(), alice.clone()]).await?;

        assert_eq!(
            load_children_current(database.pool(), parent_logical_name_id).await?,
            vec![alice, bob]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn children_current_declared_child_sources_filter_noncanonical_events_and_reassignments()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let parent_a = "ens:parent.eth";
        let parent_b = "ens:other.eth";
        let child_alice = "ens:alice.parent.eth";
        let child_bob = "ens:bob.parent.eth";
        let child_carla = "ens:carla.parent.eth";

        upsert_name_surfaces(
            database.pool(),
            &[
                name_surface(
                    parent_a,
                    "parent.eth",
                    "node:parent.eth",
                    30,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    parent_b,
                    "other.eth",
                    "node:other.eth",
                    31,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    child_alice,
                    "alice.parent.eth",
                    "node:alice.parent.eth",
                    32,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    child_bob,
                    "bob.parent.eth",
                    "node:bob.parent.eth",
                    33,
                    CanonicalityState::Finalized,
                ),
                name_surface(
                    child_carla,
                    "carla.parent.eth",
                    "node:carla.parent.eth",
                    34,
                    CanonicalityState::Observed,
                ),
            ],
        )
        .await?;

        upsert_normalized_events(
            database.pool(),
            &[
                subregistry_event(
                    "alice-parent-a",
                    "ens",
                    ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                    "ethereum-mainnet",
                    "node:parent.eth",
                    "node:alice.parent.eth",
                    100,
                    0,
                    CanonicalityState::Finalized,
                    false,
                    true,
                ),
                subregistry_event(
                    "alice-parent-b",
                    "ens",
                    ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                    "ethereum-mainnet",
                    "node:other.eth",
                    "node:alice.parent.eth",
                    101,
                    0,
                    CanonicalityState::Finalized,
                    false,
                    true,
                ),
                subregistry_event(
                    "bob-observed",
                    "ens",
                    ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                    "ethereum-mainnet",
                    "node:other.eth",
                    "node:bob.parent.eth",
                    102,
                    0,
                    CanonicalityState::Observed,
                    false,
                    true,
                ),
                subregistry_event(
                    "carla-finalized",
                    "ens",
                    ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                    "ethereum-mainnet",
                    "node:other.eth",
                    "node:carla.parent.eth",
                    103,
                    0,
                    CanonicalityState::Finalized,
                    false,
                    true,
                ),
                subregistry_event(
                    "alice-orphaned",
                    "ens",
                    ENSV1_SUBREGISTRY_SOURCE_FAMILY,
                    "ethereum-mainnet",
                    "node:parent.eth",
                    "node:alice.parent.eth",
                    104,
                    0,
                    CanonicalityState::Orphaned,
                    false,
                    true,
                ),
            ],
        )
        .await?;

        assert!(
            load_canonical_ens_v1_declared_child_sources(database.pool(), Some(parent_a))
                .await?
                .is_empty()
        );

        let current =
            load_canonical_ens_v1_declared_child_sources(database.pool(), Some(parent_b)).await?;
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].parent_logical_name_id, parent_b);
        assert_eq!(current[0].child_logical_name_id, child_alice);
        assert_eq!(current[0].event_identity, "alice-parent-b");

        database.cleanup().await
    }

    #[tokio::test]
    async fn children_current_declared_child_sources_include_basenames_base_registry() -> Result<()>
    {
        let database = TestDatabase::new().await?;
        let parent = "basenames:base.eth";
        let child = "basenames:alice.base.eth";
        let colliding_ens_parent = "ens:base.eth";
        let colliding_ens_child = "ens:alice.base.eth";

        upsert_name_surfaces(
            database.pool(),
            &[
                name_surface_on_chain(
                    colliding_ens_parent,
                    "base.eth",
                    "node:base.eth",
                    "ethereum-mainnet",
                    39,
                    CanonicalityState::Finalized,
                ),
                name_surface_on_chain(
                    parent,
                    "base.eth",
                    "node:base.eth",
                    "base-mainnet",
                    40,
                    CanonicalityState::Finalized,
                ),
                name_surface_on_chain(
                    colliding_ens_child,
                    "alice.base.eth",
                    "node:alice.base.eth",
                    "ethereum-mainnet",
                    40,
                    CanonicalityState::Finalized,
                ),
                name_surface_on_chain(
                    child,
                    "alice.base.eth",
                    "node:alice.base.eth",
                    "base-mainnet",
                    41,
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;

        upsert_normalized_events(
            database.pool(),
            &[
                subregistry_event(
                    "alice-base-registry",
                    "basenames",
                    BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY,
                    "base-mainnet",
                    "node:base.eth",
                    "node:alice.base.eth",
                    200,
                    0,
                    CanonicalityState::Finalized,
                    false,
                    true,
                ),
                subregistry_event(
                    "alice-base-primary",
                    "basenames",
                    "basenames_base_primary",
                    "base-mainnet",
                    "node:base.eth",
                    "node:alice.base.eth",
                    201,
                    0,
                    CanonicalityState::Finalized,
                    false,
                    true,
                ),
            ],
        )
        .await?;

        assert!(
            load_canonical_declared_child_sources(database.pool(), Some(colliding_ens_parent))
                .await?
                .is_empty()
        );

        let current = load_canonical_declared_child_sources(database.pool(), Some(parent)).await?;
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].parent_logical_name_id, parent);
        assert_eq!(current[0].child_logical_name_id, child);
        assert_eq!(
            current[0].source_family,
            BASENAMES_BASE_SUBREGISTRY_SOURCE_FAMILY
        );
        assert_eq!(current[0].namespace, "basenames");
        assert_eq!(current[0].chain_id, "base-mainnet");
        assert_eq!(current[0].event_identity, "alice-base-registry");

        database.cleanup().await
    }
}
