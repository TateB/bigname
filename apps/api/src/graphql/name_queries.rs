use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow, types::Uuid};

use bigname_storage::{
    AddressNameRelation, DEFAULT_ADDRESS_NAMES_MEMBERSHIP_READ_FILTER,
    DEFAULT_NAME_CURRENT_LINEAGE_JOINS, DEFAULT_NAME_CURRENT_READ_FILTER,
    NameCurrentAddressRelationFilter, NameCurrentListFilter, NameCurrentListOrder,
    NameCurrentListRow, NameCurrentListSort, NameCurrentRow, SurfaceBindingKind,
};

use super::convert::ZERO_ADDRESS;
use super::effective_owner_filter as owner;
use super::generated_filter_ops::{GeneratedDomainFilter, push_generated_domain_filters};
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhaseGraphqlNameListRow {
    pub row: NameCurrentListRow,
    pub membership_targets: Vec<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhaseGraphqlNameCount {
    pub total_count: u64,
    pub name_targets: Vec<PhaseGraphqlNameCountTarget>,
    pub membership_targets: Vec<Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct PhaseGraphqlNameCountTarget {
    pub namespace: String,
    pub chain_positions: Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratedDomainSort {
    Id,
    Storage(NameCurrentListSort),
    Owner,
    OwnerId,
    Resolver,
}

const SELECT_NAMES: &str = r#"
    SELECT logical_name_id, namespace, canonical_display_name, normalized_name,
           namehash, surface_binding_id, resource_id, serving_resource_id,
           token_lineage_id,
           binding_kind, declared_summary, provenance, chain_positions,
           canonicality_summary, manifest_version, last_recomputed_at,
           support_status, unsupported_reason, labelhash, token_id, owner,
           registrant, created_at, registration_date, expiry_date,
           resolver_address, membership_targets
    FROM filtered_names
"#;

pub async fn load_phase_graphql_name_row_by_name(
    pool: &PgPool,
    namespace: &str,
    name: &str,
) -> Result<Option<PhaseGraphqlNameListRow>> {
    let filter = NameCurrentListFilter {
        namespace: Some(namespace.to_owned()),
        name: Some(name.to_owned()),
        ..Default::default()
    };
    load_one(pool, &filter, None).await
}

pub async fn load_phase_graphql_name_row_by_namehash(
    pool: &PgPool,
    namespace: &str,
    namehash: &str,
) -> Result<Option<PhaseGraphqlNameListRow>> {
    let filter = NameCurrentListFilter {
        namespace: Some(namespace.to_owned()),
        ..Default::default()
    };
    load_one(pool, &filter, Some(namehash)).await
}

async fn load_one(
    pool: &PgPool,
    filter: &NameCurrentListFilter,
    namehash: Option<&str>,
) -> Result<Option<PhaseGraphqlNameListRow>> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_names(&mut builder, filter, namehash, None, None, false);
    builder.push(SELECT_NAMES);
    builder.push(" LIMIT 1");
    let row = builder
        .build()
        .fetch_optional(pool)
        .await
        .context("failed to load schema-v2 GraphQL name row")?;
    row.map(decode_row).transpose()
}

#[path = "owner_witness_validation.rs"]
pub(super) mod owner_witness_validation;
#[cfg(test)]
pub use owner_witness_validation::explain_phase_graphql_name_list_page;
pub use owner_witness_validation::load_phase_graphql_name_list_page_offset;

fn indexed_page(sort: GeneratedDomainSort, filter: &GeneratedDomainFilter) -> bool {
    sort == GeneratedDomainSort::Id || filter.has_bounded_id_predicate()
}

pub async fn count_phase_graphql_name_list(
    pool: &PgPool,
    filter: &NameCurrentListFilter,
    snapshot_chain_ids: &[String],
) -> Result<PhaseGraphqlNameCount> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_names(
        &mut builder,
        filter,
        None,
        None,
        Some(snapshot_chain_ids),
        false,
    );
    builder.push(
        r#"
        , distinct_name_targets AS (
            SELECT DISTINCT namespace, position.key AS slot,
                   position.value AS target
            FROM filtered_names
            CROSS JOIN LATERAL JSONB_EACH(chain_positions) AS position
        ), ranked_name_targets AS (
            SELECT namespace, slot, target,
                   ROW_NUMBER() OVER (
                       PARTITION BY namespace, slot, target ->> 'chain_id'
                       ORDER BY (target ->> 'block_number')::BIGINT DESC,
                                target ->> 'block_hash' DESC
                   ) AS target_rank
            FROM distinct_name_targets
        ), distinct_membership_targets AS (
            SELECT DISTINCT namespace, target
            FROM filtered_names membership_name
            CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
                membership_name.membership_targets
            ) AS target
        ), ranked_membership_targets AS (
            SELECT namespace, target,
                   ROW_NUMBER() OVER (
                       PARTITION BY namespace
                       ORDER BY (target ->> 'target_block_number')::BIGINT DESC,
                                target ->> 'target_block_hash' DESC
                   ) AS target_rank
            FROM distinct_membership_targets
        )
        SELECT (SELECT COUNT(*)::BIGINT FROM filtered_names) AS total_count,
               COALESCE((
                   SELECT JSONB_AGG(
                       JSONB_BUILD_OBJECT(
                           'namespace', namespace,
                           'chain_positions', JSONB_BUILD_OBJECT(slot, target)
                       ) ORDER BY namespace, slot, target_rank
                   )
                   FROM ranked_name_targets
                   WHERE target_rank <= 2
               ), '[]'::JSONB) AS name_targets,
               COALESCE((
                   SELECT JSONB_AGG(target ORDER BY namespace, target_rank)
                   FROM ranked_membership_targets
                   WHERE target_rank <= 2
               ), '[]'::JSONB) AS membership_targets
        "#,
    );
    let row = builder
        .build()
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to count schema-v2 GraphQL names for {filter:?}"))?;
    let count: i64 = row.try_get("total_count")?;
    let name_targets = serde_json::from_value(row.try_get("name_targets")?)
        .context("invalid schema-v2 GraphQL name count targets")?;
    let membership_targets = serde_json::from_value(row.try_get("membership_targets")?)
        .context("invalid schema-v2 GraphQL membership count targets")?;
    Ok(PhaseGraphqlNameCount {
        total_count: u64::try_from(count).context("negative schema-v2 GraphQL name count")?,
        name_targets,
        membership_targets,
    })
}

pub(crate) fn push_filtered_names<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a NameCurrentListFilter,
    namehash: Option<&str>,
    generated_filter: Option<&'a GeneratedDomainFilter>,
    snapshot_chain_ids: Option<&'a [String]>,
    indexed_page: bool,
) {
    builder.push("WITH ");
    let owner_filter = owner::active_owner_filter(generated_filter);
    owner::push_effective_owner_cte_predicates(builder, owner_filter, snapshot_chain_ids);
    if let Some(address) = owner::legacy_address_filter(owner_filter, filter.address.as_ref()) {
        builder.push(
            "address_membership AS (SELECT anc.logical_name_id, \
             JSONB_AGG( \
                 chain_positions || JSONB_BUILD_OBJECT( \
                     'chain_id', anc.provenance ->> 'chain_id' \
                 ) ORDER BY address, relation \
             ) AS membership_targets \
             FROM bigname_phase.address_names_current anc \
             JOIN bigname_phase.name_surfaces membership_surface \
               ON membership_surface.logical_name_id = anc.logical_name_id \
             JOIN bigname_phase.resources membership_resource \
               ON membership_resource.resource_id = anc.resource_id \
             JOIN bigname_phase.surface_bindings membership_binding \
               ON membership_binding.surface_binding_id = anc.surface_binding_id \
             LEFT JOIN bigname_phase.token_lineages membership_token_lineage \
               ON membership_token_lineage.token_lineage_id = anc.token_lineage_id \
             JOIN bigname_phase.chain_lineage membership_surface_lineage \
               ON membership_surface_lineage.chain_id = membership_surface.chain_id \
              AND membership_surface_lineage.block_hash = membership_surface.block_hash \
             JOIN bigname_phase.chain_lineage membership_resource_lineage \
               ON membership_resource_lineage.chain_id = membership_resource.chain_id \
              AND membership_resource_lineage.block_hash = membership_resource.block_hash \
             JOIN bigname_phase.chain_lineage membership_binding_lineage \
               ON membership_binding_lineage.chain_id = membership_binding.chain_id \
              AND membership_binding_lineage.block_hash = membership_binding.block_hash \
             LEFT JOIN bigname_phase.chain_lineage membership_token_lineage_lineage \
               ON membership_token_lineage_lineage.chain_id = \
                  membership_token_lineage.chain_id \
              AND membership_token_lineage_lineage.block_hash = \
                  membership_token_lineage.block_hash \
             WHERE anc.support_status = 'supported' AND ",
        );
        let indexed_address_column = matches!(
            address.relation,
            NameCurrentAddressRelationFilter::Relation(AddressNameRelation::EffectiveController)
        );
        // Effective-controller writers store lowercase addresses, so this path can use the
        // leading address column of the phase table's primary key. Legacy relations retain the
        // lower(address) expression-index path.
        let address_column = if indexed_address_column {
            "anc.address"
        } else {
            "LOWER(address)"
        };
        match address.addresses.as_ref() {
            Some(addresses) => {
                builder.push(address_column);
                builder.push(" = ANY(");
                builder.push_bind(addresses.as_slice());
                builder.push(")");
            }
            None => {
                builder.push(address_column);
                builder.push(" = ");
                builder.push_bind(&address.address);
            }
        }
        if let NameCurrentAddressRelationFilter::Relation(relation) = address.relation {
            builder.push(" AND relation = ");
            builder.push_bind(relation.as_str());
        }
        if let Some(chain_ids) = snapshot_chain_ids {
            builder.push(" AND anc.provenance ->> 'chain_id' = ANY(");
            builder.push_bind(chain_ids);
            builder.push(")");
        }
        builder.push(DEFAULT_ADDRESS_NAMES_MEMBERSHIP_READ_FILTER);
        builder.push(" GROUP BY anc.logical_name_id), ");
    }
    builder.push(
        r#"filtered_names AS (
        SELECT nc.logical_name_id, nc.namespace, nc.raw_name AS canonical_display_name,
               nc.raw_name AS normalized_name, nc.namehash,
               nc.surface_binding_id, nc.resource_id, nc.serving_resource_id,
               nc.token_lineage_id,
               nc.binding_kind, nc.declared_summary, nc.provenance,
               nc.chain_positions, nc.canonicality_summary, nc.manifest_version,
               nc.last_recomputed_at, nc.support_status, nc.unsupported_reason,
               NULL::TEXT AS labelhash,
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
               COALESCE("#,
    );
    push_json_timestamp(builder, &["registration", "created_at"]);
    builder.push(", ");
    push_json_timestamp(builder, &["history", "created_at"]);
    builder.push(", TO_TIMESTAMP(0)) AS created_at, COALESCE(");
    push_json_timestamp(builder, &["registration", "registration_date"]);
    builder.push(", ");
    push_json_timestamp(builder, &["registration", "registered_at"]);
    builder.push(") AS registration_date, COALESCE(");
    push_json_timestamp(builder, &["registration", "expiry_date"]);
    builder.push(", ");
    push_json_timestamp(builder, &["registration", "expiry"]);
    builder.push(", ");
    push_json_timestamp(builder, &["control", "expiry_date"]);
    builder.push(", ");
    push_json_timestamp(builder, &["control", "expiry"]);
    builder.push(
        r#") AS expiry_date,
           NULLIF(LOWER(nc.declared_summary #>> '{resolver,address}'), '') AS resolver_address,"#,
    );
    owner::push_effective_owner_membership_targets(builder, owner_filter, filter.address.is_some());
    builder.push(" FROM bigname_phase.name_current nc ");
    owner::push_effective_owner_lateral_join(builder, owner_filter);
    if indexed_page {
        builder.push(" JOIN LATERAL (SELECT 1 FROM bigname_phase.name_surfaces surface ");
    } else {
        builder.push(" JOIN bigname_phase.name_surfaces surface ON surface.logical_name_id = nc.logical_name_id ");
    }
    builder.push(
        " LEFT JOIN bigname_phase.resources resource \
            ON resource.resource_id = nc.resource_id \
          LEFT JOIN bigname_phase.surface_bindings binding \
            ON binding.surface_binding_id = nc.surface_binding_id \
          LEFT JOIN bigname_phase.token_lineages token_lineage \
            ON token_lineage.token_lineage_id = nc.token_lineage_id ",
    );
    builder.push(DEFAULT_NAME_CURRENT_LINEAGE_JOINS);
    if indexed_page {
        builder.push(" WHERE surface.namespace = nc.namespace AND surface.namehash = nc.namehash");
        builder.push(DEFAULT_NAME_CURRENT_READ_FILTER);
        builder.push(" OFFSET 0) name_guard ON TRUE");
    }
    if owner_filter.is_none() && filter.address.is_some() {
        builder.push(
            " JOIN address_membership \
               ON address_membership.logical_name_id = nc.logical_name_id",
        );
    }
    builder.push(" WHERE nc.support_status = 'supported'");
    if !indexed_page {
        builder.push(DEFAULT_NAME_CURRENT_READ_FILTER);
    }
    if let Some(chain_ids) = snapshot_chain_ids {
        builder.push(
            " AND nc.chain_positions <> '{}'::JSONB \
             AND NOT EXISTS (SELECT 1 FROM JSONB_EACH(nc.chain_positions) position \
             WHERE position.value ->> 'chain_id' IS NULL \
                OR position.value ->> 'chain_id' <> ALL(",
        );
        builder.push_bind(chain_ids);
        builder.push("))");
    }
    push_filters(builder, filter);
    if let Some(namehash) = namehash {
        builder.push(" AND nc.namehash = ");
        builder.push_bind(bigname_storage::normalize_evm_b256(namehash));
    }
    if let Some(filter) = generated_filter {
        push_generated_domain_filters(builder, filter);
    }
    builder.push(")");
}

fn push_filters<'a>(builder: &mut QueryBuilder<'a, Postgres>, filter: &'a NameCurrentListFilter) {
    if let Some(namespaces) = filter
        .namespaces
        .as_ref()
        .filter(|values| !values.is_empty())
    {
        builder.push(" AND nc.namespace = ANY(");
        builder.push_bind(namespaces.as_slice());
        builder.push(")");
    } else if let Some(namespace) = filter.namespace.as_deref() {
        builder.push(" AND nc.namespace = ");
        builder.push_bind(namespace);
    }
    if let Some(name) = filter.name.as_deref() {
        match graphql_namehash(name) {
            Some(namehash) => {
                builder.push(" AND nc.namehash = ");
                builder.push_bind(namehash);
            }
            None => {
                builder.push(" AND FALSE");
            }
        }
    }
    if let Some(contains) = filter.contains.as_deref() {
        builder.push(" AND nc.raw_name LIKE ");
        builder.push_bind(format!("%{}%", escape_like(contains)));
        builder.push(" ESCAPE '\\'");
    }
    if filter.is_migrated == Some(true) {
        builder.push(
            " AND nc.declared_summary #>> '{registration,authority_kind}' = 'ens_v2_registry'",
        );
    }
}

fn graphql_namehash(name: &str) -> Option<String> {
    let normalized = bigname_domain::normalization::normalize_name(name).ok()?;
    let labels = normalized
        .normalized_labels
        .iter()
        .map(|label| label.as_bytes())
        .collect::<Vec<_>>();
    Some(format!(
        "{:#x}",
        bigname_storage::ens_namehash_label_bytes(&labels)
    ))
}

fn push_json_timestamp(builder: &mut QueryBuilder<'_, Postgres>, path: &[&str]) {
    let path = format!("'{{{}}}'", path.join(","));
    builder.push("CASE WHEN JSONB_TYPEOF(nc.declared_summary #> ");
    builder.push(path.as_str());
    builder.push(") = 'number' THEN TO_TIMESTAMP((nc.declared_summary #>> ");
    builder.push(path.as_str());
    builder.push(")::DOUBLE PRECISION) WHEN JSONB_TYPEOF(nc.declared_summary #> ");
    builder.push(path.as_str());
    builder.push(") = 'string' AND nc.declared_summary #>> ");
    builder.push(path.as_str());
    builder.push(" ~ '^[0-9]+(\\.[0-9]+)?$' THEN TO_TIMESTAMP((nc.declared_summary #>> ");
    builder.push(path.as_str());
    builder.push(")::DOUBLE PRECISION) WHEN JSONB_TYPEOF(nc.declared_summary #> ");
    builder.push(path.as_str());
    builder.push(") = 'string' AND nc.declared_summary #>> ");
    builder.push(path.as_str());
    builder.push(" ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$' THEN (nc.declared_summary #>> ");
    builder.push(path.as_str());
    builder.push(")::TIMESTAMPTZ ELSE NULL END");
}

fn push_order(
    builder: &mut QueryBuilder<'_, Postgres>,
    sort: GeneratedDomainSort,
    order: NameCurrentListOrder,
) {
    let direction = match order {
        NameCurrentListOrder::Asc => "ASC",
        NameCurrentListOrder::Desc => "DESC",
    };
    let column = match sort {
        GeneratedDomainSort::Id => "namehash".to_owned(),
        GeneratedDomainSort::Storage(NameCurrentListSort::Name) => {
            "canonical_display_name COLLATE \"C\"".to_owned()
        }
        GeneratedDomainSort::Storage(NameCurrentListSort::ExpiryDate) => "expiry_date".to_owned(),
        GeneratedDomainSort::Storage(NameCurrentListSort::RegistrationDate) => "registration_date".to_owned(),
        GeneratedDomainSort::Storage(NameCurrentListSort::CreatedAt) => "created_at".to_owned(),
        GeneratedDomainSort::Owner | GeneratedDomainSort::OwnerId => format!("COALESCE(owner, registrant, '{ZERO_ADDRESS}') COLLATE \"C\""),
        GeneratedDomainSort::Resolver => "CASE WHEN resolver_address IS NULL THEN NULL ELSE resolver_address || '-' || namehash END COLLATE \"C\"".to_owned(),
    };
    builder.push(" ORDER BY ");
    if matches!(
        sort,
        GeneratedDomainSort::Storage(
            NameCurrentListSort::ExpiryDate | NameCurrentListSort::RegistrationDate
        )
    ) {
        builder.push(match order {
            NameCurrentListOrder::Asc => {
                format!("CASE WHEN {column} IS NULL THEN 1 ELSE 0 END ASC, ")
            }
            NameCurrentListOrder::Desc => {
                format!("CASE WHEN {column} IS NULL THEN 0 ELSE 1 END ASC, ")
            }
        });
    }
    builder.push(column);
    builder.push(" ");
    builder.push(direction);
    if !matches!(sort, GeneratedDomainSort::Id) {
        builder.push(", namehash ");
        builder.push(direction);
    }
    builder.push(", namespace ASC");
}

fn decode_row(row: PgRow) -> Result<PhaseGraphqlNameListRow> {
    let raw_name: String = row.try_get("canonical_display_name")?;
    let normalized = bigname_domain::normalization::normalize_name(&raw_name).ok();
    let labelhash = normalized
        .as_ref()
        .and_then(|name| name.normalized_labels.first())
        .map(|label| {
            format!(
                "0x{}",
                alloy_primitives::hex::encode(alloy_primitives::keccak256(label.as_bytes()))
            )
        });
    let normalized_name = normalized
        .map(|name| name.normalized_name)
        .unwrap_or(row.try_get("normalized_name")?);
    let support_status: String = row.try_get("support_status")?;
    let unsupported_reason: Option<String> = row.try_get("unsupported_reason")?;
    let binding_kind = row
        .try_get::<Option<String>, _>("binding_kind")?
        .map(|value| SurfaceBindingKind::parse(&value))
        .transpose()?;
    let current = NameCurrentRow {
        logical_name_id: row.try_get("logical_name_id")?,
        namespace: row.try_get("namespace")?,
        canonical_display_name: raw_name,
        normalized_name,
        namehash: row.try_get("namehash")?,
        surface_binding_id: row.try_get("surface_binding_id")?,
        resource_id: row.try_get::<Option<Uuid>, _>("resource_id")?,
        serving_resource_id: row.try_get("serving_resource_id")?,
        token_lineage_id: row.try_get("token_lineage_id")?,
        binding_kind,
        declared_summary: row.try_get("declared_summary")?,
        provenance: row.try_get("provenance")?,
        coverage: if support_status == "supported" {
            json!({"status": "projected", "exhaustiveness": "not_asserted"})
        } else {
            json!({"status": "unsupported", "exhaustiveness": "not_asserted", "unsupported_reason": unsupported_reason})
        },
        chain_positions: row.try_get("chain_positions")?,
        canonicality_summary: row.try_get("canonicality_summary")?,
        manifest_version: row.try_get("manifest_version")?,
        last_recomputed_at: row.try_get("last_recomputed_at")?,
    };
    let membership_targets: Value = row.try_get("membership_targets")?;
    Ok(PhaseGraphqlNameListRow {
        row: NameCurrentListRow {
            row: current,
            labelhash,
            token_id: row.try_get("token_id")?,
            owner: row.try_get("owner")?,
            registrant: row.try_get("registrant")?,
            created_at: row.try_get("created_at")?,
            registration_date: row.try_get("registration_date")?,
            expiry_date: row.try_get("expiry_date")?,
            resolver_address: row.try_get("resolver_address")?,
        },
        membership_targets: membership_targets
            .as_array()
            .cloned()
            .context("schema-v2 GraphQL membership targets must be an array")?,
    })
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('%', r"\%")
        .replace('_', r"\_")
}
