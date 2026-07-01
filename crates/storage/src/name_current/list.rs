use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder, postgres::PgRow, types::time::OffsetDateTime};

use super::{NameCurrentRow, decode_name_current_row};
use crate::{
    AddressNameRelation,
    projection_helpers::{checked_page_limit_i64_from_usize, checked_page_size_usize},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameCurrentListSort {
    Name,
    ExpiryDate,
    RegistrationDate,
    CreatedAt,
}

impl NameCurrentListSort {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::ExpiryDate => "expiry_date",
            Self::RegistrationDate => "registration_date",
            Self::CreatedAt => "created_at",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameCurrentListOrder {
    Asc,
    Desc,
}

impl NameCurrentListOrder {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameCurrentAddressRelationFilter {
    Relation(AddressNameRelation),
    Any,
}

impl NameCurrentAddressRelationFilter {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Relation(relation) => relation.as_str(),
            Self::Any => "any",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentAddressFilter {
    pub address: String,
    pub relation: NameCurrentAddressRelationFilter,
    /// When set (and non-empty), the address-membership CTE matches any address in this list
    /// (`anc.address = ANY($list)`), superseding `address`. Backs the subgraph `owner_in` filter.
    pub addresses: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NameCurrentListFilter {
    pub namespace: Option<String>,
    /// Namespace set for public search reads. When non-empty, binds `nc.namespace = ANY($list)`
    /// and supersedes `namespace`, keeping a storage-ordered page in one DB query.
    pub namespaces: Option<Vec<String>>,
    pub name: Option<String>,
    pub prefix: Option<String>,
    pub contains: Option<String>,
    pub contains_nocase: Option<String>,
    pub resolver: Option<String>,
    pub address: Option<NameCurrentAddressFilter>,
    /// When `Some(true)`, restrict to names whose declared registration authority is the ENS v2
    /// registry (`declared_summary.registration.authority_kind = 'ens_v2_registry'`). Backs the
    /// subgraph `isMigrated` filter. `Some(false)` / `None` apply no migration predicate.
    pub is_migrated: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NameCurrentListCursorValue {
    Name(String),
    Timestamp(Option<OffsetDateTime>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentListCursor {
    pub sort_value: NameCurrentListCursorValue,
    pub namespace: String,
    pub normalized_name: String,
    pub namehash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentListRow {
    pub row: NameCurrentRow,
    pub labelhash: Option<String>,
    pub token_id: Option<String>,
    pub owner: Option<String>,
    pub registrant: Option<String>,
    pub created_at: Option<OffsetDateTime>,
    pub registration_date: Option<OffsetDateTime>,
    pub expiry_date: Option<OffsetDateTime>,
    pub resolver_address: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentListPage {
    pub rows: Vec<NameCurrentListRow>,
    pub next_cursor: Option<NameCurrentListCursor>,
    pub total_count: Option<u64>,
}

const DEFAULT_NAME_CURRENT_LIST_READ_FILTER: &str = r#"
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
          AND binding.active_to IS NULL
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

const DEFAULT_ADDRESS_NAMES_MEMBERSHIP_READ_FILTER: &str = r#"
  AND membership_surface.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND membership_resource.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND membership_binding.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND membership_binding.active_to IS NULL
  AND (
      anc.token_lineage_id IS NULL
      OR membership_token_lineage.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
  )
"#;

/// Shared projection of the derived list columns from the `filtered_names` CTE. Every list/count
/// read path appends its own `WHERE` / `ORDER BY` / `LIMIT` after this so the column shape that
/// [`decode_name_current_list_row`] decodes stays identical across cursor, offset, and by-namehash
/// reads.
const NAME_CURRENT_LIST_SELECT: &str = r#"
        SELECT
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
            last_recomputed_at,
            labelhash,
            token_id,
            owner,
            registrant,
            created_at,
            registration_date,
            expiry_date,
            resolver_address
        FROM filtered_names
"#;

pub async fn load_name_current_list_page(
    pool: &PgPool,
    filter: &NameCurrentListFilter,
    sort: NameCurrentListSort,
    order: NameCurrentListOrder,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
    include_total_count: bool,
) -> Result<NameCurrentListPage> {
    let page_size = checked_page_size_usize(
        page_size,
        "name_current list page_size must be positive",
        "name_current list page_size does not fit in usize",
    )?;
    let page_limit = checked_page_limit_i64_from_usize(
        page_size,
        "name_current list page_size is too large",
        "name_current list page_size exceeds SQL limit",
    )?;
    let total_count = if include_total_count {
        Some(count_name_current_list(pool, filter).await?)
    } else {
        None
    };

    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_name_current_cte(&mut builder, filter);
    builder.push(NAME_CURRENT_LIST_SELECT);
    builder.push(" WHERE TRUE ");
    if let Some(cursor) = cursor {
        push_name_current_list_cursor_after(&mut builder, sort, order, cursor);
    }
    push_name_current_list_order(&mut builder, sort, order);
    builder.push(" LIMIT ");
    builder.push_bind(page_limit);

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load name_current compact page for {filter:?}"))?;
    let mut rows = rows
        .into_iter()
        .map(decode_name_current_list_row)
        .collect::<Result<Vec<_>>>()?;
    let next_cursor = if rows.len() > page_size {
        rows.truncate(page_size);
        rows.last()
            .map(|row| name_current_list_cursor_from_row(row, sort))
    } else {
        None
    };

    Ok(NameCurrentListPage {
        rows,
        next_cursor,
        total_count,
    })
}

/// Load a single derived list row by EIP-137 namehash (case-insensitive), or `None` if no current
/// name matches. Reuses the list CTE so the caller gets the same derived
/// owner / token_id / dates / resolver_address as paged reads, rather than the bare projection row
/// from [`load_name_current`](super::load_name_current).
pub async fn load_name_current_list_row_by_namehash(
    pool: &PgPool,
    namehash: &str,
) -> Result<Option<NameCurrentListRow>> {
    let filter = NameCurrentListFilter::default();
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_name_current_cte(&mut builder, &filter);
    builder.push(NAME_CURRENT_LIST_SELECT);
    builder.push(" WHERE LOWER(namehash) = LOWER(");
    builder.push_bind(namehash);
    builder.push(") LIMIT 1");

    let row = builder
        .build()
        .fetch_optional(pool)
        .await
        .with_context(|| {
            format!("failed to load name_current compact row for namehash {namehash}")
        })?;
    row.map(decode_name_current_list_row).transpose()
}

/// Load a single derived list row by normalized ENS name within a namespace, or `None` if no current
/// name matches. Backs the Manager's `domain(id: "alice.eth")`, which passes the ENS *name* string
/// rather than the namehash. Mirrors [`load_name_current_list_row_by_namehash`] but keys on
/// `normalized_name` (covered by the `(namespace, normalized_name)` index); the name and namespace
/// are bound through the shared filter predicates, so the lookup stays injection-safe and produces
/// the same derived columns as the paged reads.
pub async fn load_name_current_list_row_by_name(
    pool: &PgPool,
    namespace: &str,
    name: &str,
) -> Result<Option<NameCurrentListRow>> {
    let filter = NameCurrentListFilter {
        namespace: Some(namespace.to_owned()),
        name: Some(name.to_owned()),
        ..Default::default()
    };
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_name_current_cte(&mut builder, &filter);
    builder.push(NAME_CURRENT_LIST_SELECT);
    builder.push(" LIMIT 1");

    let row = builder
        .build()
        .fetch_optional(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load name_current compact row for name {name} in namespace {namespace}"
            )
        })?;
    row.map(decode_name_current_list_row).transpose()
}

/// Load a derived list page by absolute `LIMIT`/`OFFSET` instead of a keyset cursor. Applies the
/// same ordering and total-order tie-break as [`load_name_current_list_page`], so windows are
/// stable and disjoint across offsets. Bridges the subgraph `first`/`skip` paging onto the
/// otherwise cursor-only storage; the caller counts separately via [`count_name_current_list`] when
/// a total is needed.
pub async fn load_name_current_list_page_offset(
    pool: &PgPool,
    filter: &NameCurrentListFilter,
    sort: NameCurrentListSort,
    order: NameCurrentListOrder,
    limit: u64,
    offset: u64,
) -> Result<Vec<NameCurrentListRow>> {
    let page_size = checked_page_size_usize(
        limit,
        "name_current offset page limit must be positive",
        "name_current offset page limit does not fit in usize",
    )?;
    // Bind the exact LIMIT — unlike the keyset reader, offset paging must not fetch the extra
    // sentinel row `checked_page_limit_i64_from_usize` adds for next-page detection.
    let page_limit =
        i64::try_from(page_size).context("name_current offset page limit exceeds SQL limit")?;
    let page_offset = i64::try_from(offset).context("name_current offset does not fit in i64")?;

    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_name_current_cte(&mut builder, filter);
    builder.push(NAME_CURRENT_LIST_SELECT);
    builder.push(" WHERE TRUE ");
    push_name_current_list_order(&mut builder, sort, order);
    builder.push(" LIMIT ");
    builder.push_bind(page_limit);
    builder.push(" OFFSET ");
    builder.push_bind(page_offset);

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load name_current offset page for {filter:?}"))?;
    rows.into_iter().map(decode_name_current_list_row).collect()
}

pub async fn count_name_current_list(pool: &PgPool, filter: &NameCurrentListFilter) -> Result<u64> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_name_current_cte(&mut builder, filter);
    builder.push(
        r#"
        SELECT COUNT(*)::BIGINT AS total_count
        FROM filtered_names
        "#,
    );

    let row = builder
        .build()
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to count name_current compact rows for {filter:?}"))?;
    let total_count = crate::sql_row::get::<i64>(&row, "total_count")?;
    u64::try_from(total_count).context("negative name_current compact total_count")
}

fn push_filtered_name_current_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a NameCurrentListFilter,
) {
    builder.push("WITH ");
    if let Some(address_filter) = filter.address.as_ref() {
        let address_namespace = match filter.namespaces.as_ref() {
            Some(namespaces) if !namespaces.is_empty() => None,
            _ => filter.namespace.as_deref(),
        };
        push_address_membership_cte(builder, address_filter, address_namespace);
        builder.push(", ");
    }
    builder.push(
        r#"
        filtered_names AS (
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
                nc.last_recomputed_at,
                surface.labelhashes[1] AS labelhash,
                NULLIF(COALESCE(
                    nc.declared_summary #>> '{authority,token_id}',
                    nc.declared_summary #>> '{registration,token_id}',
                    nc.declared_summary #>> '{registration,upstream_resource}',
                    nc.declared_summary #>> '{control,token_id}'
                ), '') AS token_id,
                NULLIF(LOWER(COALESCE(
                    nc.declared_summary #>> '{control,registry_owner}',
                    nc.declared_summary #>> '{control,owner}'
                )), '') AS owner,
                NULLIF(LOWER(COALESCE(
                    nc.declared_summary #>> '{control,registrant}',
                    nc.declared_summary #>> '{registration,registrant}'
                )), '') AS registrant,
                COALESCE(
                    "#,
    );
    push_json_timestamp_expr(builder, &["registration", "created_at"]);
    builder.push(", ");
    push_json_timestamp_expr(builder, &["history", "created_at"]);
    builder.push(
        r#",
                    surface_block.block_timestamp
                ) AS created_at,
                COALESCE(
                    "#,
    );
    push_json_timestamp_expr(builder, &["registration", "registration_date"]);
    builder.push(", ");
    push_json_timestamp_expr(builder, &["registration", "registered_at"]);
    builder.push(
        r#"
                ) AS registration_date,
                COALESCE(
                    "#,
    );
    push_json_timestamp_expr(builder, &["registration", "expiry_date"]);
    builder.push(", ");
    push_json_timestamp_expr(builder, &["registration", "expiry"]);
    builder.push(", ");
    push_json_timestamp_expr(builder, &["control", "expiry_date"]);
    builder.push(", ");
    push_json_timestamp_expr(builder, &["control", "expiry"]);
    builder.push(
        r#"
                ) AS expiry_date,
                NULLIF(LOWER(nc.declared_summary #>> '{resolver,address}'), '') AS resolver_address
            FROM name_current nc
            JOIN name_surfaces surface
              ON surface.logical_name_id = nc.logical_name_id
            LEFT JOIN chain_lineage surface_block
              ON surface_block.chain_id = surface.chain_id
             AND surface_block.block_hash = surface.block_hash
            LEFT JOIN resources resource
              ON resource.resource_id = nc.resource_id
            LEFT JOIN surface_bindings binding
              ON binding.surface_binding_id = nc.surface_binding_id
            LEFT JOIN token_lineages token_lineage
              ON token_lineage.token_lineage_id = nc.token_lineage_id
        "#,
    );
    if filter.address.is_some() {
        builder.push(
            r#"
            JOIN address_membership
              ON address_membership.logical_name_id = nc.logical_name_id
            "#,
        );
    }
    builder.push(" WHERE TRUE ");
    builder.push(DEFAULT_NAME_CURRENT_LIST_READ_FILTER);
    push_name_current_filter_predicates(builder, filter);
    builder.push(")");
}

fn push_address_membership_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address_filter: &'a NameCurrentAddressFilter,
    namespace: Option<&'a str>,
) {
    builder.push(
        r#"
        address_membership AS (
            SELECT DISTINCT anc.logical_name_id
            FROM address_names_current anc
            JOIN name_surfaces membership_surface
              ON membership_surface.logical_name_id = anc.logical_name_id
            JOIN resources membership_resource
              ON membership_resource.resource_id = anc.resource_id
            JOIN surface_bindings membership_binding
              ON membership_binding.surface_binding_id = anc.surface_binding_id
            LEFT JOIN token_lineages membership_token_lineage
              ON membership_token_lineage.token_lineage_id = anc.token_lineage_id
            WHERE "#,
    );
    match address_filter.addresses.as_ref() {
        Some(addresses) => {
            builder.push("anc.address = ANY(");
            builder.push_bind(addresses.as_slice());
            builder.push(")");
        }
        None => {
            builder.push("anc.address = ");
            builder.push_bind(&address_filter.address);
        }
    }
    if let Some(namespace) = namespace {
        builder.push(" AND anc.namespace = ");
        builder.push_bind(namespace);
    }
    if let NameCurrentAddressRelationFilter::Relation(relation) = address_filter.relation {
        builder.push(" AND anc.relation = ");
        builder.push_bind(relation.as_str());
    }
    builder.push(DEFAULT_ADDRESS_NAMES_MEMBERSHIP_READ_FILTER);
    builder.push(")");
}

fn push_name_current_filter_predicates<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a NameCurrentListFilter,
) {
    if let Some(namespaces) = filter
        .namespaces
        .as_ref()
        .filter(|namespaces| !namespaces.is_empty())
    {
        builder.push(" AND nc.namespace = ANY(");
        builder.push_bind(namespaces.as_slice());
        builder.push(")");
    } else if let Some(namespace) = filter.namespace.as_deref() {
        builder.push(" AND nc.namespace = ");
        builder.push_bind(namespace);
    }
    if let Some(name) = filter.name.as_deref() {
        builder.push(" AND nc.normalized_name = ");
        builder.push_bind(name);
    }
    if let Some(prefix) = filter.prefix.as_deref() {
        builder.push(" AND nc.normalized_name LIKE ");
        builder.push_bind(format!("{}%", escape_like_pattern(prefix)));
        builder.push(" ESCAPE '\\'");
    }
    if let Some(contains) = filter.contains.as_deref() {
        builder.push(" AND nc.normalized_name LIKE ");
        builder.push_bind(format!("%{}%", escape_like_pattern(contains)));
        builder.push(" ESCAPE '\\'");
    }
    if let Some(contains_nocase) = filter.contains_nocase.as_deref() {
        builder.push(" AND LOWER(nc.normalized_name) LIKE ");
        builder.push_bind(format!(
            "%{}%",
            escape_like_pattern(&contains_nocase.to_ascii_lowercase())
        ));
        builder.push(" ESCAPE '\\'");
    }
    if let Some(resolver) = filter.resolver.as_deref() {
        builder.push(" AND LOWER(nc.declared_summary #>> '{resolver,address}') = ");
        builder.push_bind(resolver);
    }
    if filter.is_migrated == Some(true) {
        builder.push(
            " AND (nc.declared_summary #>> '{registration,authority_kind}') = 'ens_v2_registry'",
        );
    }
}

include!("list_paging.rs");

#[cfg(test)]
mod tests;
