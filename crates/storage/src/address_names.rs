use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};
use uuid::Uuid;

use crate::{
    SurfaceBindingKind,
    projection_helpers::{
        checked_page_limit_i64_from_usize, checked_page_size_usize, require_json_object,
        serialize_jsonb_field,
    },
};

const DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER: &str = r#"
  AND surface.canonicality_state IN (
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
"#;

/// Persisted ENSv1 address-to-surface relation row for current address collections.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNameCurrentRow {
    pub address: String,
    pub logical_name_id: String,
    pub relation: AddressNameRelation,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub surface_binding_id: Uuid,
    pub resource_id: Uuid,
    pub token_lineage_id: Option<Uuid>,
    pub binding_kind: SurfaceBindingKind,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Supported current-relation facets for the first ENSv1 address-name slice.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AddressNameRelation {
    Registrant,
    TokenHolder,
    EffectiveController,
}

impl AddressNameRelation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registrant => "registrant",
            Self::TokenHolder => "token_holder",
            Self::EffectiveController => "effective_controller",
        }
    }

    const fn sort_rank(self) -> u8 {
        match self {
            Self::Registrant => 0,
            Self::TokenHolder => 1,
            Self::EffectiveController => 2,
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "registrant" => Ok(Self::Registrant),
            "token_holder" => Ok(Self::TokenHolder),
            "effective_controller" => Ok(Self::EffectiveController),
            _ => bail!("unknown address_names_current relation {value}"),
        }
    }
}

/// Storage-local grouping mode for collapsing relation rows into stable collection representatives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddressNamesCurrentDedupe {
    Surface,
    Resource,
}

impl AddressNamesCurrentDedupe {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Surface => "surface",
            Self::Resource => "resource",
        }
    }
}

/// Storage-local grouped collection item built from one or more relation rows.
///
/// Non-relation fields come from the stable representative row chosen by the default collection
/// sort order. This helper exists for storage-side dedupe only; it does not define public API
/// representative-selection semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNameCurrentEntry {
    pub address: String,
    pub logical_name_id: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub surface_binding_id: Uuid,
    pub resource_id: Uuid,
    pub token_lineage_id: Option<Uuid>,
    pub binding_kind: SurfaceBindingKind,
    pub relations: Vec<AddressNameRelation>,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Keyset cursor fields for storage-side address-name collection pagination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNamesCurrentCursor {
    pub canonical_display_name: String,
    pub logical_name_id: String,
    pub resource_id: Uuid,
}

/// Compact metadata for the full filtered grouped address-name collection.
///
/// These fields are derived from the same representative rows returned by
/// [`collapse_address_name_current_rows`], but without returning every grouped entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNamesCurrentSummary {
    pub grouped_entry_count: u64,
    pub provenance: AddressNamesCurrentProvenanceSummary,
    pub chain_positions: Value,
    pub consistency: String,
    pub last_recomputed_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNamesCurrentProvenanceSummary {
    pub normalized_event_ids: Value,
    pub raw_fact_refs: Value,
    pub manifest_versions: Value,
    pub derivation_kind: Option<String>,
}

/// Bounded page of grouped current address-name entries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressNamesCurrentPage {
    pub entries: Vec<AddressNameCurrentEntry>,
    pub next_cursor: Option<AddressNamesCurrentCursor>,
    pub summary: AddressNamesCurrentSummary,
}

/// Load current address-name relation rows from the default canonical read set.
pub async fn load_address_names_current(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
) -> Result<Vec<AddressNameCurrentRow>> {
    load_address_names_current_internal(pool, address, namespace, relation, false).await
}

/// Load current address-name relation rows, including noncanonical supporting identity rows.
pub async fn load_address_names_current_including_noncanonical(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
) -> Result<Vec<AddressNameCurrentRow>> {
    load_address_names_current_internal(pool, address, namespace, relation, true).await
}

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

/// Load a bounded page of grouped current address-name entries from the default canonical read set.
pub async fn load_address_names_current_page(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
    dedupe_by: AddressNamesCurrentDedupe,
    cursor: Option<&AddressNamesCurrentCursor>,
    page_size: u64,
) -> Result<AddressNamesCurrentPage> {
    let page_size = checked_page_size_usize(
        page_size,
        "address_names_current page_size must be positive",
        "address_names_current page_size does not fit in usize",
    )?;
    let page_limit = checked_page_limit_i64_from_usize(
        page_size,
        "address_names_current page_size is too large",
        "address_names_current page_size exceeds SQL limit",
    )?;

    let summary =
        load_address_names_current_summary(pool, address, namespace, relation, dedupe_by).await?;

    if let Some(cursor) = cursor {
        ensure_address_names_current_cursor_exists(
            pool, address, namespace, relation, dedupe_by, cursor,
        )
        .await?;
    }

    let mut builder = QueryBuilder::<Postgres>::new("");
    push_address_names_current_grouped_entries_cte(
        &mut builder,
        address,
        namespace,
        relation,
        dedupe_by,
    );
    builder.push(
        r#"
        SELECT
            address,
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            relations,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        FROM entries
        "#,
    );
    if let Some(cursor) = cursor {
        push_address_names_current_cursor_after(&mut builder, cursor);
    }
    builder.push(
        r#"
        ORDER BY
            canonical_display_name ASC,
            logical_name_id ASC,
            resource_id::TEXT ASC
        LIMIT
        "#,
    );
    builder.push_bind(page_limit);

    let rows = builder.build().fetch_all(pool).await.with_context(|| {
        let mut parts = vec![format!("address {address}")];
        if let Some(namespace) = namespace {
            parts.push(format!("namespace {namespace}"));
        }
        if let Some(relation) = relation {
            parts.push(format!("relation {}", relation.as_str()));
        }
        parts.push(format!("dedupe_by {}", dedupe_by.as_str()));
        format!(
            "failed to load address_names_current grouped page for {}",
            parts.join(" ")
        )
    })?;

    let mut entries = rows
        .into_iter()
        .map(decode_address_name_current_entry)
        .collect::<Result<Vec<_>>>()?;
    let next_cursor = if entries.len() > page_size {
        entries.truncate(page_size);
        entries.last().map(address_names_current_cursor_from_entry)
    } else {
        None
    };

    Ok(AddressNamesCurrentPage {
        entries,
        next_cursor,
        summary,
    })
}

/// Collapse relation rows into stable storage-local collection representatives.
pub fn collapse_address_name_current_rows(
    rows: &[AddressNameCurrentRow],
    dedupe_by: AddressNamesCurrentDedupe,
) -> Vec<AddressNameCurrentEntry> {
    let mut groups = BTreeMap::<AddressNameGroupKey, GroupAccumulator>::new();

    for row in rows {
        let group_key = match dedupe_by {
            AddressNamesCurrentDedupe::Surface => AddressNameGroupKey::Surface {
                address: row.address.clone(),
                logical_name_id: row.logical_name_id.clone(),
            },
            AddressNamesCurrentDedupe::Resource => AddressNameGroupKey::Resource {
                address: row.address.clone(),
                resource_id: row.resource_id.to_string(),
            },
        };

        match groups.get_mut(&group_key) {
            Some(group) => {
                group.relations.insert(row.relation);
                if compare_row_sort_key(row, &group.representative) == Ordering::Less {
                    group.representative = row.clone();
                }
            }
            None => {
                groups.insert(
                    group_key,
                    GroupAccumulator {
                        representative: row.clone(),
                        relations: BTreeSet::from([row.relation]),
                    },
                );
            }
        }
    }

    let mut entries = groups
        .into_values()
        .map(|group| {
            let representative = group.representative;
            AddressNameCurrentEntry {
                address: representative.address,
                logical_name_id: representative.logical_name_id,
                namespace: representative.namespace,
                canonical_display_name: representative.canonical_display_name,
                normalized_name: representative.normalized_name,
                namehash: representative.namehash,
                surface_binding_id: representative.surface_binding_id,
                resource_id: representative.resource_id,
                token_lineage_id: representative.token_lineage_id,
                binding_kind: representative.binding_kind,
                relations: group.relations.into_iter().collect(),
                provenance: representative.provenance,
                coverage: representative.coverage,
                chain_positions: representative.chain_positions,
                canonicality_summary: representative.canonicality_summary,
                manifest_version: representative.manifest_version,
                last_recomputed_at: representative.last_recomputed_at,
            }
        })
        .collect::<Vec<_>>();

    entries.sort_by(compare_entry_sort_key);
    entries
}

async fn load_address_names_current_summary(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
    dedupe_by: AddressNamesCurrentDedupe,
) -> Result<AddressNamesCurrentSummary> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_address_names_current_grouped_entries_cte(
        &mut builder,
        address,
        namespace,
        relation,
        dedupe_by,
    );
    builder.push(
        r#",
        ordered_entries AS (
            SELECT
                entries.*,
                ROW_NUMBER() OVER (
                    ORDER BY
                        canonical_display_name ASC,
                        logical_name_id ASC,
                        resource_id::TEXT ASC
                ) AS entry_position
            FROM entries
        ),
        normalized_event_id_values AS (
            SELECT DISTINCT ON (value)
                value,
                entry_position,
                value_position
            FROM ordered_entries
            CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
                CASE
                    WHEN JSONB_TYPEOF(provenance -> 'normalized_event_ids') = 'array'
                    THEN provenance -> 'normalized_event_ids'
                    ELSE '[]'::JSONB
                END
            ) WITH ORDINALITY AS provenance_values(value, value_position)
            ORDER BY value, entry_position ASC, value_position ASC
        ),
        raw_fact_ref_values AS (
            SELECT DISTINCT ON (value)
                value,
                entry_position,
                value_position
            FROM ordered_entries
            CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
                CASE
                    WHEN JSONB_TYPEOF(provenance -> 'raw_fact_refs') = 'array'
                    THEN provenance -> 'raw_fact_refs'
                    ELSE '[]'::JSONB
                END
            ) WITH ORDINALITY AS provenance_values(value, value_position)
            ORDER BY value, entry_position ASC, value_position ASC
        ),
        manifest_version_values AS (
            SELECT DISTINCT ON (value)
                value,
                entry_position,
                value_position
            FROM ordered_entries
            CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
                CASE
                    WHEN JSONB_TYPEOF(provenance -> 'manifest_versions') = 'array'
                    THEN provenance -> 'manifest_versions'
                    ELSE '[]'::JSONB
                END
            ) WITH ORDINALITY AS provenance_values(value, value_position)
            ORDER BY value, entry_position ASC, value_position ASC
        ),
        chain_position_values AS (
            SELECT
                slot,
                position_value,
                (position_value ->> 'block_number')::BIGINT AS block_number,
                position_value ->> 'block_hash' AS block_hash
            FROM ordered_entries
            CROSS JOIN LATERAL JSONB_EACH(chain_positions) AS positions(slot, position_value)
            WHERE position_value ? 'chain_id'
              AND position_value ? 'block_number'
              AND position_value ? 'block_hash'
              AND position_value ? 'timestamp'
              AND JSONB_TYPEOF(position_value -> 'block_number') = 'number'
        ),
        chain_position_heads AS (
            SELECT DISTINCT ON (slot)
                slot,
                position_value
            FROM chain_position_values
            ORDER BY slot, block_number DESC, block_hash DESC
        )
        SELECT
            (SELECT COUNT(*)::BIGINT FROM ordered_entries) AS grouped_entry_count,
            COALESCE(
                (
                    SELECT JSONB_AGG(value ORDER BY entry_position ASC, value_position ASC)
                    FROM normalized_event_id_values
                ),
                '[]'::JSONB
            ) AS provenance_normalized_event_ids,
            COALESCE(
                (
                    SELECT JSONB_AGG(value ORDER BY entry_position ASC, value_position ASC)
                    FROM raw_fact_ref_values
                ),
                '[]'::JSONB
            ) AS provenance_raw_fact_refs,
            COALESCE(
                (
                    SELECT JSONB_AGG(value ORDER BY entry_position ASC, value_position ASC)
                    FROM manifest_version_values
                ),
                '[]'::JSONB
            ) AS provenance_manifest_versions,
            (
                SELECT provenance ->> 'derivation_kind'
                FROM ordered_entries
                WHERE JSONB_TYPEOF(provenance -> 'derivation_kind') = 'string'
                ORDER BY entry_position ASC
                LIMIT 1
            ) AS provenance_derivation_kind,
            COALESCE(
                (
                    SELECT JSONB_OBJECT_AGG(slot, position_value ORDER BY slot)
                    FROM chain_position_heads
                ),
                '{}'::JSONB
            ) AS chain_positions,
            CASE
                WHEN (SELECT COUNT(*) FROM ordered_entries) = 0 THEN 'head'
                WHEN EXISTS (
                    SELECT 1
                    FROM ordered_entries
                    WHERE COALESCE(canonicality_summary ->> 'status', '') NOT IN ('safe', 'finalized')
                ) THEN 'head'
                WHEN EXISTS (
                    SELECT 1
                    FROM ordered_entries
                    WHERE canonicality_summary ->> 'status' = 'safe'
                ) THEN 'safe'
                ELSE 'finalized'
            END AS consistency,
            (SELECT MAX(last_recomputed_at) FROM ordered_entries) AS last_recomputed_at
        "#,
    );

    let row = builder.build().fetch_one(pool).await.with_context(|| {
        let mut parts = vec![format!("address {address}")];
        if let Some(namespace) = namespace {
            parts.push(format!("namespace {namespace}"));
        }
        if let Some(relation) = relation {
            parts.push(format!("relation {}", relation.as_str()));
        }
        parts.push(format!("dedupe_by {}", dedupe_by.as_str()));
        format!(
            "failed to load address_names_current grouped summary for {}",
            parts.join(" ")
        )
    })?;

    decode_address_names_current_summary(row)
}

async fn ensure_address_names_current_cursor_exists(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
    dedupe_by: AddressNamesCurrentDedupe,
    cursor: &AddressNamesCurrentCursor,
) -> Result<()> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_address_names_current_grouped_entries_cte(
        &mut builder,
        address,
        namespace,
        relation,
        dedupe_by,
    );
    builder.push(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM entries
            WHERE canonical_display_name =
        "#,
    );
    builder.push(" ");
    builder.push_bind(&cursor.canonical_display_name);
    builder.push(" AND logical_name_id = ");
    builder.push_bind(&cursor.logical_name_id);
    builder.push(" AND resource_id::TEXT = ");
    builder.push_bind(cursor.resource_id.to_string());
    builder.push(
        r#"
        ) AS cursor_exists
        "#,
    );

    let row = builder.build().fetch_one(pool).await.with_context(|| {
        let mut parts = vec![format!("address {address}")];
        if let Some(namespace) = namespace {
            parts.push(format!("namespace {namespace}"));
        }
        if let Some(relation) = relation {
            parts.push(format!("relation {}", relation.as_str()));
        }
        parts.push(format!("dedupe_by {}", dedupe_by.as_str()));
        format!(
            "failed to validate address_names_current grouped page cursor for {}",
            parts.join(" ")
        )
    })?;

    if row
        .try_get::<bool, _>("cursor_exists")
        .context("missing cursor_exists")?
    {
        Ok(())
    } else {
        bail!("address_names_current page cursor does not match a grouped entry")
    }
}

fn push_address_names_current_grouped_entries_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
    namespace: Option<&'a str>,
    relation: Option<AddressNameRelation>,
    dedupe_by: AddressNamesCurrentDedupe,
) {
    builder.push(
        r#"
        WITH filtered AS (
            SELECT
                anc.address,
                anc.logical_name_id,
                anc.relation,
                anc.namespace,
                anc.canonical_display_name,
                anc.normalized_name,
                anc.namehash,
                anc.surface_binding_id,
                anc.resource_id,
                anc.token_lineage_id,
                anc.binding_kind,
                anc.provenance,
                anc.coverage,
                anc.chain_positions,
                anc.canonicality_summary,
                anc.manifest_version,
                anc.last_recomputed_at,
                CASE anc.relation
                    WHEN 'registrant' THEN 0
                    WHEN 'token_holder' THEN 1
                    WHEN 'effective_controller' THEN 2
                    ELSE 99
                END AS relation_rank
            FROM address_names_current anc
            JOIN name_surfaces surface
              ON surface.logical_name_id = anc.logical_name_id
            JOIN resources resource
              ON resource.resource_id = anc.resource_id
            JOIN surface_bindings binding
              ON binding.surface_binding_id = anc.surface_binding_id
            LEFT JOIN token_lineages token_lineage
              ON token_lineage.token_lineage_id = anc.token_lineage_id
            WHERE anc.address =
        "#,
    );
    builder.push(" ");
    builder.push_bind(address);

    if let Some(namespace) = namespace {
        builder.push(" AND anc.namespace = ");
        builder.push_bind(namespace);
    }
    if let Some(relation) = relation {
        builder.push(" AND anc.relation = ");
        builder.push_bind(relation.as_str());
    }
    builder.push(DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER);

    match dedupe_by {
        AddressNamesCurrentDedupe::Surface => builder.push(
            r#"
        ),
        representatives AS (
            SELECT DISTINCT ON (address, logical_name_id)
                address,
                logical_name_id,
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
            FROM filtered
            ORDER BY
                address ASC,
                logical_name_id ASC,
                canonical_display_name ASC,
                relation_rank ASC
        ),
        relation_values AS (
            SELECT
                address,
                logical_name_id,
                relation,
                MIN(relation_rank) AS relation_rank
            FROM filtered
            GROUP BY address, logical_name_id, relation
        ),
        relation_facets AS (
            SELECT
                address,
                logical_name_id,
                ARRAY_AGG(relation ORDER BY relation_rank ASC) AS relations
            FROM relation_values
            GROUP BY address, logical_name_id
        ),
        entries AS (
            SELECT
                representatives.address,
                representatives.logical_name_id,
                representatives.namespace,
                representatives.canonical_display_name,
                representatives.normalized_name,
                representatives.namehash,
                representatives.surface_binding_id,
                representatives.resource_id,
                representatives.token_lineage_id,
                representatives.binding_kind,
                relation_facets.relations,
                representatives.provenance,
                representatives.coverage,
                representatives.chain_positions,
                representatives.canonicality_summary,
                representatives.manifest_version,
                representatives.last_recomputed_at
            FROM representatives
            JOIN relation_facets
              ON relation_facets.address = representatives.address
             AND relation_facets.logical_name_id = representatives.logical_name_id
        )
            "#,
        ),
        AddressNamesCurrentDedupe::Resource => builder.push(
            r#"
        ),
        representatives AS (
            SELECT DISTINCT ON (address, resource_id)
                address,
                logical_name_id,
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
            FROM filtered
            ORDER BY
                address ASC,
                resource_id ASC,
                canonical_display_name ASC,
                logical_name_id ASC,
                relation_rank ASC
        ),
        relation_values AS (
            SELECT
                address,
                resource_id,
                relation,
                MIN(relation_rank) AS relation_rank
            FROM filtered
            GROUP BY address, resource_id, relation
        ),
        relation_facets AS (
            SELECT
                address,
                resource_id,
                ARRAY_AGG(relation ORDER BY relation_rank ASC) AS relations
            FROM relation_values
            GROUP BY address, resource_id
        ),
        entries AS (
            SELECT
                representatives.address,
                representatives.logical_name_id,
                representatives.namespace,
                representatives.canonical_display_name,
                representatives.normalized_name,
                representatives.namehash,
                representatives.surface_binding_id,
                representatives.resource_id,
                representatives.token_lineage_id,
                representatives.binding_kind,
                relation_facets.relations,
                representatives.provenance,
                representatives.coverage,
                representatives.chain_positions,
                representatives.canonicality_summary,
                representatives.manifest_version,
                representatives.last_recomputed_at
            FROM representatives
            JOIN relation_facets
              ON relation_facets.address = representatives.address
             AND relation_facets.resource_id = representatives.resource_id
        )
            "#,
        ),
    };
}

fn push_address_names_current_cursor_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    cursor: &'a AddressNamesCurrentCursor,
) {
    let cursor_resource_id = cursor.resource_id.to_string();
    builder.push(
        r#"
        WHERE (
            canonical_display_name >
        "#,
    );
    builder.push(" ");
    builder.push_bind(&cursor.canonical_display_name);
    builder.push(" OR (canonical_display_name = ");
    builder.push_bind(&cursor.canonical_display_name);
    builder.push(" AND logical_name_id > ");
    builder.push_bind(&cursor.logical_name_id);
    builder.push(") OR (canonical_display_name = ");
    builder.push_bind(&cursor.canonical_display_name);
    builder.push(" AND logical_name_id = ");
    builder.push_bind(&cursor.logical_name_id);
    builder.push(" AND resource_id::TEXT > ");
    builder.push_bind(cursor_resource_id);
    builder.push("))");
}

async fn load_address_names_current_internal(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
    include_noncanonical: bool,
) -> Result<Vec<AddressNameCurrentRow>> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            anc.address,
            anc.logical_name_id,
            anc.relation,
            anc.namespace,
            anc.canonical_display_name,
            anc.normalized_name,
            anc.namehash,
            anc.surface_binding_id,
            anc.resource_id,
            anc.token_lineage_id,
            anc.binding_kind,
            anc.provenance,
            anc.coverage,
            anc.chain_positions,
            anc.canonicality_summary,
            anc.manifest_version,
            anc.last_recomputed_at
        FROM address_names_current anc
        JOIN name_surfaces surface
          ON surface.logical_name_id = anc.logical_name_id
        JOIN resources resource
          ON resource.resource_id = anc.resource_id
        JOIN surface_bindings binding
          ON binding.surface_binding_id = anc.surface_binding_id
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = anc.token_lineage_id
        WHERE anc.address = 
        "#,
    );
    builder.push_bind(address);

    if let Some(namespace) = namespace {
        builder.push(" AND anc.namespace = ");
        builder.push_bind(namespace);
    }
    if let Some(relation) = relation {
        builder.push(" AND anc.relation = ");
        builder.push_bind(relation.as_str());
    }
    if !include_noncanonical {
        builder.push(DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER);
    }

    builder.push(
        r#"
        ORDER BY
            anc.canonical_display_name ASC,
            anc.logical_name_id ASC,
            CASE anc.relation
                WHEN 'registrant' THEN 0
                WHEN 'token_holder' THEN 1
                WHEN 'effective_controller' THEN 2
                ELSE 99
            END ASC
        "#,
    );

    let rows = builder.build().fetch_all(pool).await.with_context(|| {
        let mut parts = vec![format!("address {address}")];
        if let Some(namespace) = namespace {
            parts.push(format!("namespace {namespace}"));
        }
        if let Some(relation) = relation {
            parts.push(format!("relation {}", relation.as_str()));
        }
        format!(
            "failed to load address_names_current rows for {}",
            parts.join(" ")
        )
    })?;

    rows.into_iter()
        .map(decode_address_name_current_row)
        .collect()
}

async fn upsert_address_name_current_row(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &AddressNameCurrentRow,
) -> Result<AddressNameCurrentRow> {
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

    let snapshot = sqlx::query(
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
    )
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

fn decode_address_name_current_row(row: PgRow) -> Result<AddressNameCurrentRow> {
    let relation = row
        .try_get::<String, _>("relation")
        .context("missing relation")
        .and_then(|value| AddressNameRelation::parse(&value))?;
    let binding_kind = row
        .try_get::<String, _>("binding_kind")
        .context("missing binding_kind")
        .and_then(|value| parse_surface_binding_kind(&value))?;

    Ok(AddressNameCurrentRow {
        address: row.try_get("address").context("missing address")?,
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        relation,
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

fn decode_address_name_current_entry(row: PgRow) -> Result<AddressNameCurrentEntry> {
    let binding_kind = row
        .try_get::<String, _>("binding_kind")
        .context("missing binding_kind")
        .and_then(|value| parse_surface_binding_kind(&value))?;
    let relations = row
        .try_get::<Vec<String>, _>("relations")
        .context("missing relations")?
        .into_iter()
        .map(|value| AddressNameRelation::parse(&value))
        .collect::<Result<Vec<_>>>()?;

    Ok(AddressNameCurrentEntry {
        address: row.try_get("address").context("missing address")?,
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
        relations,
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

fn decode_address_names_current_summary(row: PgRow) -> Result<AddressNamesCurrentSummary> {
    let grouped_entry_count = row
        .try_get::<i64, _>("grouped_entry_count")
        .context("missing grouped_entry_count")?;
    let grouped_entry_count =
        u64::try_from(grouped_entry_count).context("negative grouped_entry_count")?;

    Ok(AddressNamesCurrentSummary {
        grouped_entry_count,
        provenance: AddressNamesCurrentProvenanceSummary {
            normalized_event_ids: row
                .try_get("provenance_normalized_event_ids")
                .context("missing provenance_normalized_event_ids")?,
            raw_fact_refs: row
                .try_get("provenance_raw_fact_refs")
                .context("missing provenance_raw_fact_refs")?,
            manifest_versions: row
                .try_get("provenance_manifest_versions")
                .context("missing provenance_manifest_versions")?,
            derivation_kind: row
                .try_get("provenance_derivation_kind")
                .context("missing provenance_derivation_kind")?,
        },
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        consistency: row.try_get("consistency").context("missing consistency")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

fn address_names_current_cursor_from_entry(
    entry: &AddressNameCurrentEntry,
) -> AddressNamesCurrentCursor {
    AddressNamesCurrentCursor {
        canonical_display_name: entry.canonical_display_name.clone(),
        logical_name_id: entry.logical_name_id.clone(),
        resource_id: entry.resource_id,
    }
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

fn compare_row_sort_key(left: &AddressNameCurrentRow, right: &AddressNameCurrentRow) -> Ordering {
    left.address
        .cmp(&right.address)
        .then_with(|| {
            left.canonical_display_name
                .cmp(&right.canonical_display_name)
        })
        .then_with(|| left.logical_name_id.cmp(&right.logical_name_id))
        .then_with(|| left.relation.sort_rank().cmp(&right.relation.sort_rank()))
}

fn compare_entry_sort_key(
    left: &AddressNameCurrentEntry,
    right: &AddressNameCurrentEntry,
) -> Ordering {
    left.address
        .cmp(&right.address)
        .then_with(|| {
            left.canonical_display_name
                .cmp(&right.canonical_display_name)
        })
        .then_with(|| left.logical_name_id.cmp(&right.logical_name_id))
        .then_with(|| {
            left.resource_id
                .to_string()
                .cmp(&right.resource_id.to_string())
        })
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum AddressNameGroupKey {
    Surface {
        address: String,
        logical_name_id: String,
    },
    Resource {
        address: String,
        resource_id: String,
    },
}

#[derive(Clone, Debug)]
struct GroupAccumulator {
    representative: AddressNameCurrentRow,
    relations: BTreeSet<AddressNameRelation>,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
    use serde_json::{Map, Value as JsonValue, json};
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };

    use super::*;
    use crate::{
        CanonicalityState, NameSurface, Resource, SurfaceBinding, TokenLineage,
        default_database_url, upsert_name_surfaces, upsert_resources, upsert_surface_bindings,
        upsert_token_lineages,
    };

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
                .context("failed to parse database URL for address_names_current tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_addr_names_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for address_names_current tests")?;

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
                .context("failed to connect address_names_current test pool")?;

            crate::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for address_names_current tests")?;

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

    fn token_lineage(
        token_lineage_id: Uuid,
        canonicality_state: CanonicalityState,
    ) -> TokenLineage {
        TokenLineage {
            token_lineage_id,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: format!("0xlineage{}", token_lineage_id.simple()),
            block_number: 21_100_000,
            provenance: json!({"source": "address_names_current_test", "anchor": "token_lineage"}),
            canonicality_state,
        }
    }

    fn resource(
        resource_id: Uuid,
        token_lineage_id: Option<Uuid>,
        canonicality_state: CanonicalityState,
    ) -> Resource {
        Resource {
            resource_id,
            token_lineage_id,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: format!("0xresource{}", resource_id.simple()),
            block_number: 21_100_001,
            provenance: json!({"source": "address_names_current_test", "anchor": "resource"}),
            canonicality_state,
        }
    }

    fn name_surface(
        logical_name_id: &str,
        display_name: &str,
        canonicality_state: CanonicalityState,
    ) -> NameSurface {
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
            block_hash: format!("0xsurface:{display_name}"),
            block_number: 21_100_002,
            provenance: json!({"source": "address_names_current_test", "anchor": "surface"}),
            canonicality_state,
        }
    }

    fn surface_binding(
        surface_binding_id: Uuid,
        logical_name_id: &str,
        resource_id: Uuid,
        canonicality_state: CanonicalityState,
    ) -> SurfaceBinding {
        SurfaceBinding {
            surface_binding_id,
            logical_name_id: logical_name_id.to_owned(),
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_171_700),
            active_to: None,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: format!("0xbinding{}", surface_binding_id.simple()),
            block_number: 21_100_003,
            provenance: json!({"source": "address_names_current_test", "anchor": "binding"}),
            canonicality_state,
        }
    }

    async fn seed_relation_references(
        database: &TestDatabase,
        logical_name_id: &str,
        display_name: &str,
        resource_id: Uuid,
        token_lineage_id: Option<Uuid>,
        surface_binding_id: Uuid,
        canonicality_state: CanonicalityState,
    ) -> Result<()> {
        if let Some(token_lineage_id) = token_lineage_id {
            upsert_token_lineages(
                database.pool(),
                &[token_lineage(token_lineage_id, canonicality_state)],
            )
            .await?;
        }
        upsert_resources(
            database.pool(),
            &[resource(resource_id, token_lineage_id, canonicality_state)],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[name_surface(
                logical_name_id,
                display_name,
                canonicality_state,
            )],
        )
        .await?;
        upsert_surface_bindings(
            database.pool(),
            &[surface_binding(
                surface_binding_id,
                logical_name_id,
                resource_id,
                canonicality_state,
            )],
        )
        .await?;
        Ok(())
    }

    struct AddressNameCurrentRowSeed<'a> {
        address: &'a str,
        logical_name_id: &'a str,
        display_name: &'a str,
        relation: AddressNameRelation,
        surface_binding_id: Uuid,
        resource_id: Uuid,
        token_lineage_id: Option<Uuid>,
        manifest_version: i64,
    }

    fn address_name_current_row(seed: AddressNameCurrentRowSeed<'_>) -> AddressNameCurrentRow {
        AddressNameCurrentRow {
            address: seed.address.to_owned(),
            logical_name_id: seed.logical_name_id.to_owned(),
            relation: seed.relation,
            namespace: "ens".to_owned(),
            canonical_display_name: seed.display_name.to_owned(),
            normalized_name: seed.display_name.to_owned(),
            namehash: format!("namehash:{}", seed.display_name),
            surface_binding_id: seed.surface_binding_id,
            resource_id: seed.resource_id,
            token_lineage_id: seed.token_lineage_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            provenance: json!({
                "normalized_event_ids": [seed.manifest_version],
                "derivation_kind": "address_names_current_rebuild"
            }),
            coverage: json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "enumeration_basis": "address_collection"
            }),
            chain_positions: json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_100_003,
                    "block_hash": format!("0xbinding{}", seed.surface_binding_id.simple()),
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            }),
            canonicality_summary: json!({
                "status": "finalized",
                "chains": {
                    "ethereum-mainnet": "finalized"
                }
            }),
            manifest_version: seed.manifest_version,
            last_recomputed_at: timestamp(1_717_171_717 + seed.manifest_version),
        }
    }

    fn expected_summary(entries: &[AddressNameCurrentEntry]) -> AddressNamesCurrentSummary {
        AddressNamesCurrentSummary {
            grouped_entry_count: entries.len() as u64,
            provenance: AddressNamesCurrentProvenanceSummary {
                normalized_event_ids: collect_provenance_values(entries, "normalized_event_ids"),
                raw_fact_refs: collect_provenance_values(entries, "raw_fact_refs"),
                manifest_versions: collect_provenance_values(entries, "manifest_versions"),
                derivation_kind: entries
                    .iter()
                    .filter_map(|entry| {
                        entry
                            .provenance
                            .get("derivation_kind")
                            .and_then(JsonValue::as_str)
                            .map(str::to_owned)
                    })
                    .next(),
            },
            chain_positions: expected_chain_positions(entries),
            consistency: expected_consistency(entries).to_owned(),
            last_recomputed_at: entries.iter().map(|entry| entry.last_recomputed_at).max(),
        }
    }

    fn collect_provenance_values(entries: &[AddressNameCurrentEntry], key: &str) -> JsonValue {
        let mut deduped = Vec::new();
        for entry in entries {
            let Some(values) = entry.provenance.get(key).and_then(JsonValue::as_array) else {
                continue;
            };
            for value in values {
                if !deduped.contains(value) {
                    deduped.push(value.clone());
                }
            }
        }
        JsonValue::Array(deduped)
    }

    fn expected_chain_positions(entries: &[AddressNameCurrentEntry]) -> JsonValue {
        let mut chain_positions = BTreeMap::<String, (i64, String, JsonValue)>::new();
        for entry in entries {
            let Some(position_values) = entry.chain_positions.as_object() else {
                continue;
            };
            for (slot, position_value) in position_values {
                let Some(block_number) = position_value
                    .get("block_number")
                    .and_then(JsonValue::as_i64)
                else {
                    continue;
                };
                let Some(block_hash) = position_value
                    .get("block_hash")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned)
                else {
                    continue;
                };
                if position_value
                    .get("chain_id")
                    .and_then(JsonValue::as_str)
                    .is_none()
                    || position_value
                        .get("timestamp")
                        .and_then(JsonValue::as_str)
                        .is_none()
                {
                    continue;
                }

                match chain_positions.get(slot) {
                    Some((existing_block_number, existing_block_hash, _))
                        if *existing_block_number > block_number
                            || (*existing_block_number == block_number
                                && existing_block_hash >= &block_hash) => {}
                    _ => {
                        chain_positions.insert(
                            slot.clone(),
                            (block_number, block_hash, position_value.clone()),
                        );
                    }
                }
            }
        }

        JsonValue::Object(
            chain_positions
                .into_iter()
                .map(|(slot, (_, _, value))| (slot, value))
                .collect::<Map<_, _>>(),
        )
    }

    fn expected_consistency(entries: &[AddressNameCurrentEntry]) -> &'static str {
        let mut consistency = "finalized";
        let mut saw_any = false;

        for entry in entries {
            saw_any = true;
            match entry
                .canonicality_summary
                .get("status")
                .and_then(JsonValue::as_str)
            {
                Some("safe") => consistency = "safe",
                Some("finalized") => {}
                _ => return "head",
            }
        }

        if saw_any { consistency } else { "head" }
    }

    #[tokio::test]
    async fn address_names_current_upsert_replaces_existing_relation_row() -> Result<()> {
        let database = TestDatabase::new().await?;
        let address = "0x0000000000000000000000000000000000000abc";
        let logical_name_id = "ens:alice.eth";
        let token_lineage_id = Uuid::from_u128(0x1001);
        let resource_id = Uuid::from_u128(0x2001);
        let surface_binding_id = Uuid::from_u128(0x3001);

        seed_relation_references(
            &database,
            logical_name_id,
            "alice.eth",
            resource_id,
            Some(token_lineage_id),
            surface_binding_id,
            CanonicalityState::Finalized,
        )
        .await?;

        let first = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id,
            display_name: "alice.eth",
            relation: AddressNameRelation::Registrant,
            surface_binding_id,
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            manifest_version: 1,
        });
        upsert_address_names_current_rows(database.pool(), std::slice::from_ref(&first)).await?;

        let mut replacement = first.clone();
        replacement.coverage = json!({
            "status": "partial",
            "exhaustiveness": "authoritative",
            "enumeration_basis": "address_collection"
        });
        replacement.manifest_version = 2;

        let updated =
            upsert_address_names_current_rows(database.pool(), std::slice::from_ref(&replacement))
                .await?;
        assert_eq!(updated, vec![replacement.clone()]);

        let loaded = load_address_names_current(database.pool(), address, None, None).await?;
        assert_eq!(loaded, vec![replacement]);

        database.cleanup().await
    }

    #[tokio::test]
    async fn address_names_current_filters_noncanonical_supporting_identity_rows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let address = "0x0000000000000000000000000000000000000abc";

        let canonical_logical_name_id = "ens:alice.eth";
        let canonical_token_lineage_id = Uuid::from_u128(0x1101);
        let canonical_resource_id = Uuid::from_u128(0x1201);
        let canonical_surface_binding_id = Uuid::from_u128(0x1301);
        seed_relation_references(
            &database,
            canonical_logical_name_id,
            "alice.eth",
            canonical_resource_id,
            Some(canonical_token_lineage_id),
            canonical_surface_binding_id,
            CanonicalityState::Finalized,
        )
        .await?;

        let noncanonical_logical_name_id = "ens:bob.eth";
        let noncanonical_token_lineage_id = Uuid::from_u128(0x2101);
        let noncanonical_resource_id = Uuid::from_u128(0x2201);
        let noncanonical_surface_binding_id = Uuid::from_u128(0x2301);
        seed_relation_references(
            &database,
            noncanonical_logical_name_id,
            "bob.eth",
            noncanonical_resource_id,
            Some(noncanonical_token_lineage_id),
            noncanonical_surface_binding_id,
            CanonicalityState::Orphaned,
        )
        .await?;

        let canonical = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: canonical_logical_name_id,
            display_name: "alice.eth",
            relation: AddressNameRelation::Registrant,
            surface_binding_id: canonical_surface_binding_id,
            resource_id: canonical_resource_id,
            token_lineage_id: Some(canonical_token_lineage_id),
            manifest_version: 1,
        });
        let noncanonical = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: noncanonical_logical_name_id,
            display_name: "bob.eth",
            relation: AddressNameRelation::TokenHolder,
            surface_binding_id: noncanonical_surface_binding_id,
            resource_id: noncanonical_resource_id,
            token_lineage_id: Some(noncanonical_token_lineage_id),
            manifest_version: 1,
        });
        upsert_address_names_current_rows(
            database.pool(),
            &[canonical.clone(), noncanonical.clone()],
        )
        .await?;

        assert_eq!(
            load_address_names_current(database.pool(), address, None, None).await?,
            vec![canonical.clone()]
        );
        assert_eq!(
            load_address_names_current_including_noncanonical(database.pool(), address, None, None)
                .await?,
            vec![canonical, noncanonical]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn address_names_current_load_orders_by_display_name_then_relation() -> Result<()> {
        let database = TestDatabase::new().await?;
        let address = "0x0000000000000000000000000000000000000abc";

        seed_relation_references(
            &database,
            "ens:bob.eth",
            "bob.eth",
            Uuid::from_u128(0x3201),
            Some(Uuid::from_u128(0x3101)),
            Uuid::from_u128(0x3301),
            CanonicalityState::Finalized,
        )
        .await?;
        seed_relation_references(
            &database,
            "ens:alice.eth",
            "alice.eth",
            Uuid::from_u128(0x4201),
            Some(Uuid::from_u128(0x4101)),
            Uuid::from_u128(0x4301),
            CanonicalityState::Finalized,
        )
        .await?;

        let bob = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:bob.eth",
            display_name: "bob.eth",
            relation: AddressNameRelation::TokenHolder,
            surface_binding_id: Uuid::from_u128(0x3301),
            resource_id: Uuid::from_u128(0x3201),
            token_lineage_id: Some(Uuid::from_u128(0x3101)),
            manifest_version: 1,
        });
        let alice_controller = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:alice.eth",
            display_name: "alice.eth",
            relation: AddressNameRelation::EffectiveController,
            surface_binding_id: Uuid::from_u128(0x4301),
            resource_id: Uuid::from_u128(0x4201),
            token_lineage_id: Some(Uuid::from_u128(0x4101)),
            manifest_version: 1,
        });
        let alice_registrant = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:alice.eth",
            display_name: "alice.eth",
            relation: AddressNameRelation::Registrant,
            surface_binding_id: Uuid::from_u128(0x4301),
            resource_id: Uuid::from_u128(0x4201),
            token_lineage_id: Some(Uuid::from_u128(0x4101)),
            manifest_version: 1,
        });
        let alice_token_holder = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:alice.eth",
            display_name: "alice.eth",
            relation: AddressNameRelation::TokenHolder,
            surface_binding_id: Uuid::from_u128(0x4301),
            resource_id: Uuid::from_u128(0x4201),
            token_lineage_id: Some(Uuid::from_u128(0x4101)),
            manifest_version: 1,
        });
        upsert_address_names_current_rows(
            database.pool(),
            &[
                bob.clone(),
                alice_controller.clone(),
                alice_token_holder.clone(),
                alice_registrant.clone(),
            ],
        )
        .await?;

        assert_eq!(
            load_address_names_current(database.pool(), address, None, None).await?,
            vec![alice_registrant, alice_token_holder, alice_controller, bob]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn collapse_address_name_rows_dedupes_surface_and_resource_views() -> Result<()> {
        let address = "0x0000000000000000000000000000000000000abc";
        let shared_resource_id = Uuid::from_u128(0x5201);
        let shared_token_lineage_id = Uuid::from_u128(0x5101);

        let alpha_registrant = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:alpha.eth",
            display_name: "alpha.eth",
            relation: AddressNameRelation::Registrant,
            surface_binding_id: Uuid::from_u128(0x5301),
            resource_id: shared_resource_id,
            token_lineage_id: Some(shared_token_lineage_id),
            manifest_version: 1,
        });
        let alpha_token_holder = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:alpha.eth",
            display_name: "alpha.eth",
            relation: AddressNameRelation::TokenHolder,
            surface_binding_id: Uuid::from_u128(0x5301),
            resource_id: shared_resource_id,
            token_lineage_id: Some(shared_token_lineage_id),
            manifest_version: 1,
        });
        let beta_controller = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:beta.eth",
            display_name: "beta.eth",
            relation: AddressNameRelation::EffectiveController,
            surface_binding_id: Uuid::from_u128(0x6301),
            resource_id: shared_resource_id,
            token_lineage_id: Some(shared_token_lineage_id),
            manifest_version: 1,
        });

        let surface_entries = collapse_address_name_current_rows(
            &[
                beta_controller.clone(),
                alpha_token_holder.clone(),
                alpha_registrant.clone(),
                alpha_token_holder.clone(),
            ],
            AddressNamesCurrentDedupe::Surface,
        );
        assert_eq!(surface_entries.len(), 2);
        assert_eq!(surface_entries[0].logical_name_id, "ens:alpha.eth");
        assert_eq!(
            surface_entries[0].relations,
            vec![
                AddressNameRelation::Registrant,
                AddressNameRelation::TokenHolder
            ]
        );
        assert_eq!(surface_entries[1].logical_name_id, "ens:beta.eth");
        assert_eq!(
            surface_entries[1].relations,
            vec![AddressNameRelation::EffectiveController]
        );

        let resource_entries = collapse_address_name_current_rows(
            &[beta_controller, alpha_token_holder, alpha_registrant],
            AddressNamesCurrentDedupe::Resource,
        );
        assert_eq!(resource_entries.len(), 1);
        assert_eq!(resource_entries[0].logical_name_id, "ens:alpha.eth");
        assert_eq!(
            resource_entries[0].relations,
            vec![
                AddressNameRelation::Registrant,
                AddressNameRelation::TokenHolder,
                AddressNameRelation::EffectiveController
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn address_names_current_page_groups_after_filters_and_matches_full_summary() -> Result<()>
    {
        let database = TestDatabase::new().await?;
        let address = "0x0000000000000000000000000000000000000abc";

        seed_relation_references(
            &database,
            "ens:alpha.eth",
            "alpha.eth",
            Uuid::from_u128(0x9201),
            Some(Uuid::from_u128(0x9101)),
            Uuid::from_u128(0x9301),
            CanonicalityState::Finalized,
        )
        .await?;
        seed_relation_references(
            &database,
            "ens:beta.eth",
            "beta.eth",
            Uuid::from_u128(0xa201),
            Some(Uuid::from_u128(0xa101)),
            Uuid::from_u128(0xa301),
            CanonicalityState::Finalized,
        )
        .await?;
        seed_relation_references(
            &database,
            "ens:delta.eth",
            "delta.eth",
            Uuid::from_u128(0xb201),
            Some(Uuid::from_u128(0xb101)),
            Uuid::from_u128(0xb301),
            CanonicalityState::Finalized,
        )
        .await?;

        let alpha_registrant = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:alpha.eth",
            display_name: "alpha.eth",
            relation: AddressNameRelation::Registrant,
            surface_binding_id: Uuid::from_u128(0x9301),
            resource_id: Uuid::from_u128(0x9201),
            token_lineage_id: Some(Uuid::from_u128(0x9101)),
            manifest_version: 1,
        });
        let mut alpha_token_holder = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:alpha.eth",
            display_name: "alpha.eth",
            relation: AddressNameRelation::TokenHolder,
            surface_binding_id: Uuid::from_u128(0x9301),
            resource_id: Uuid::from_u128(0x9201),
            token_lineage_id: Some(Uuid::from_u128(0x9101)),
            manifest_version: 2,
        });
        alpha_token_holder.provenance = json!({
            "normalized_event_ids": ["alpha-token", "shared"],
            "raw_fact_refs": [{"log": "alpha"}],
            "manifest_versions": [2],
            "derivation_kind": "address_names_current_rebuild"
        });
        alpha_token_holder.chain_positions = json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_100_010,
                "block_hash": "0xaaa",
                "timestamp": "2026-04-17T00:00:10Z"
            }
        });
        alpha_token_holder.canonicality_summary = json!({
            "status": "safe",
            "chains": {
                "ethereum-mainnet": "safe"
            }
        });

        let beta_controller = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:beta.eth",
            display_name: "beta.eth",
            relation: AddressNameRelation::EffectiveController,
            surface_binding_id: Uuid::from_u128(0xa301),
            resource_id: Uuid::from_u128(0xa201),
            token_lineage_id: Some(Uuid::from_u128(0xa101)),
            manifest_version: 3,
        });
        let mut delta_token_holder = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:delta.eth",
            display_name: "delta.eth",
            relation: AddressNameRelation::TokenHolder,
            surface_binding_id: Uuid::from_u128(0xb301),
            resource_id: Uuid::from_u128(0xb201),
            token_lineage_id: Some(Uuid::from_u128(0xb101)),
            manifest_version: 4,
        });
        delta_token_holder.provenance = json!({
            "normalized_event_ids": ["shared", "delta-token"],
            "raw_fact_refs": [{"log": "delta"}],
            "manifest_versions": [4],
            "derivation_kind": "address_names_current_rebuild"
        });
        delta_token_holder.chain_positions = json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_100_011,
                "block_hash": "0xbbb",
                "timestamp": "2026-04-17T00:00:11Z"
            }
        });

        upsert_address_names_current_rows(
            database.pool(),
            &[
                alpha_registrant,
                alpha_token_holder,
                beta_controller,
                delta_token_holder,
            ],
        )
        .await?;

        let filtered_rows = load_address_names_current(
            database.pool(),
            address,
            Some("ens"),
            Some(AddressNameRelation::TokenHolder),
        )
        .await?;
        let expected_entries =
            collapse_address_name_current_rows(&filtered_rows, AddressNamesCurrentDedupe::Surface);
        assert_eq!(expected_entries.len(), 2);

        let first_page = load_address_names_current_page(
            database.pool(),
            address,
            Some("ens"),
            Some(AddressNameRelation::TokenHolder),
            AddressNamesCurrentDedupe::Surface,
            None,
            1,
        )
        .await?;
        assert_eq!(first_page.entries, expected_entries[..1].to_vec());
        assert_eq!(first_page.summary, expected_summary(&expected_entries));
        assert_eq!(
            first_page.next_cursor,
            Some(address_names_current_cursor_from_entry(
                &expected_entries[0]
            ))
        );
        assert_eq!(
            first_page.entries[0].relations,
            vec![AddressNameRelation::TokenHolder]
        );

        let second_page = load_address_names_current_page(
            database.pool(),
            address,
            Some("ens"),
            Some(AddressNameRelation::TokenHolder),
            AddressNamesCurrentDedupe::Surface,
            first_page.next_cursor.as_ref(),
            1,
        )
        .await?;
        assert_eq!(second_page.entries, expected_entries[1..].to_vec());
        assert_eq!(second_page.next_cursor, None);
        assert_eq!(second_page.summary, expected_summary(&expected_entries));

        let invalid_cursor = AddressNamesCurrentCursor {
            canonical_display_name: "missing.eth".to_owned(),
            logical_name_id: "ens:missing.eth".to_owned(),
            resource_id: Uuid::from_u128(0xffff),
        };
        let error = load_address_names_current_page(
            database.pool(),
            address,
            Some("ens"),
            Some(AddressNameRelation::TokenHolder),
            AddressNamesCurrentDedupe::Surface,
            Some(&invalid_cursor),
            1,
        )
        .await
        .expect_err("missing grouped cursor must be rejected");
        assert!(
            format!("{error:#}").contains("cursor does not match a grouped entry"),
            "unexpected error: {error:#}"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn address_names_current_page_resource_dedupe_matches_collapsed_full_read() -> Result<()>
    {
        let database = TestDatabase::new().await?;
        let address = "0x0000000000000000000000000000000000000abc";
        let shared_resource_id = Uuid::from_u128(0xc201);
        let shared_token_lineage_id = Uuid::from_u128(0xc101);

        seed_relation_references(
            &database,
            "ens:alpha.eth",
            "alpha.eth",
            shared_resource_id,
            Some(shared_token_lineage_id),
            Uuid::from_u128(0xc301),
            CanonicalityState::Finalized,
        )
        .await?;
        seed_relation_references(
            &database,
            "ens:beta.eth",
            "beta.eth",
            shared_resource_id,
            Some(shared_token_lineage_id),
            Uuid::from_u128(0xd301),
            CanonicalityState::Finalized,
        )
        .await?;

        let alpha_registrant = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:alpha.eth",
            display_name: "alpha.eth",
            relation: AddressNameRelation::Registrant,
            surface_binding_id: Uuid::from_u128(0xc301),
            resource_id: shared_resource_id,
            token_lineage_id: Some(shared_token_lineage_id),
            manifest_version: 1,
        });
        let alpha_token_holder = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:alpha.eth",
            display_name: "alpha.eth",
            relation: AddressNameRelation::TokenHolder,
            surface_binding_id: Uuid::from_u128(0xc301),
            resource_id: shared_resource_id,
            token_lineage_id: Some(shared_token_lineage_id),
            manifest_version: 2,
        });
        let beta_controller = address_name_current_row(AddressNameCurrentRowSeed {
            address,
            logical_name_id: "ens:beta.eth",
            display_name: "beta.eth",
            relation: AddressNameRelation::EffectiveController,
            surface_binding_id: Uuid::from_u128(0xd301),
            resource_id: shared_resource_id,
            token_lineage_id: Some(shared_token_lineage_id),
            manifest_version: 3,
        });
        upsert_address_names_current_rows(
            database.pool(),
            &[
                beta_controller,
                alpha_token_holder.clone(),
                alpha_registrant.clone(),
            ],
        )
        .await?;

        let rows = load_address_names_current(database.pool(), address, Some("ens"), None).await?;
        let expected_entries =
            collapse_address_name_current_rows(&rows, AddressNamesCurrentDedupe::Resource);

        let page = load_address_names_current_page(
            database.pool(),
            address,
            Some("ens"),
            None,
            AddressNamesCurrentDedupe::Resource,
            None,
            10,
        )
        .await?;
        assert_eq!(page.entries, expected_entries);
        assert_eq!(page.summary, expected_summary(&page.entries));
        assert_eq!(page.next_cursor, None);
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].logical_name_id, "ens:alpha.eth");
        assert_eq!(
            page.entries[0].relations,
            vec![
                AddressNameRelation::Registrant,
                AddressNameRelation::TokenHolder,
                AddressNameRelation::EffectiveController
            ]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn address_names_current_delete_and_clear_support_rebuild_workflows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let first_address = "0x0000000000000000000000000000000000000abc";
        let second_address = "0x0000000000000000000000000000000000000def";

        seed_relation_references(
            &database,
            "ens:alice.eth",
            "alice.eth",
            Uuid::from_u128(0x7201),
            Some(Uuid::from_u128(0x7101)),
            Uuid::from_u128(0x7301),
            CanonicalityState::Finalized,
        )
        .await?;
        seed_relation_references(
            &database,
            "ens:bob.eth",
            "bob.eth",
            Uuid::from_u128(0x8201),
            Some(Uuid::from_u128(0x8101)),
            Uuid::from_u128(0x8301),
            CanonicalityState::Finalized,
        )
        .await?;

        let first = address_name_current_row(AddressNameCurrentRowSeed {
            address: first_address,
            logical_name_id: "ens:alice.eth",
            display_name: "alice.eth",
            relation: AddressNameRelation::Registrant,
            surface_binding_id: Uuid::from_u128(0x7301),
            resource_id: Uuid::from_u128(0x7201),
            token_lineage_id: Some(Uuid::from_u128(0x7101)),
            manifest_version: 1,
        });
        let second = address_name_current_row(AddressNameCurrentRowSeed {
            address: second_address,
            logical_name_id: "ens:bob.eth",
            display_name: "bob.eth",
            relation: AddressNameRelation::TokenHolder,
            surface_binding_id: Uuid::from_u128(0x8301),
            resource_id: Uuid::from_u128(0x8201),
            token_lineage_id: Some(Uuid::from_u128(0x8101)),
            manifest_version: 1,
        });
        upsert_address_names_current_rows(database.pool(), &[first, second.clone()]).await?;

        assert_eq!(
            delete_address_names_current(database.pool(), first_address).await?,
            1
        );
        assert!(
            load_address_names_current(database.pool(), first_address, None, None)
                .await?
                .is_empty()
        );

        assert_eq!(clear_address_names_current(database.pool()).await?, 1);
        assert!(
            load_address_names_current(database.pool(), second_address, None, None)
                .await?
                .is_empty()
        );

        database.cleanup().await
    }
}
