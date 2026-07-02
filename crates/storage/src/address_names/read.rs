use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::{
    decode::decode_address_name_current_row,
    query::DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER,
    types::{AddressNameCurrentRow, AddressNameRelation},
};

/// Load current address-name relation rows from the default canonical read set.
pub async fn load_address_names_current(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
) -> Result<Vec<AddressNameCurrentRow>> {
    let relations = relation.into_iter().collect::<Vec<_>>();
    let relations = (!relations.is_empty()).then_some(relations.as_slice());
    load_address_names_current_for_relations(pool, address, namespace, relations).await
}

/// Load current address-name relation rows, including noncanonical supporting identity rows.
pub async fn load_address_names_current_including_noncanonical(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
) -> Result<Vec<AddressNameCurrentRow>> {
    let relations = relation.into_iter().collect::<Vec<_>>();
    let relations = (!relations.is_empty()).then_some(relations.as_slice());
    load_address_names_current_including_noncanonical_for_relations(
        pool, address, namespace, relations,
    )
    .await
}

/// Load current address-name relation rows from the default canonical read set.
pub async fn load_address_names_current_for_relations(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
) -> Result<Vec<AddressNameCurrentRow>> {
    load_address_names_current_internal(pool, address, namespace, relations, false).await
}

/// Load current address-name relation rows, including noncanonical supporting identity rows.
pub async fn load_address_names_current_including_noncanonical_for_relations(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
) -> Result<Vec<AddressNameCurrentRow>> {
    load_address_names_current_internal(pool, address, namespace, relations, true).await
}

async fn load_address_names_current_internal(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
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
    if let Some(relations) = relations.filter(|relations| !relations.is_empty()) {
        let relation_values = relations
            .iter()
            .map(|relation| relation.as_str().to_owned())
            .collect::<Vec<_>>();
        builder.push(" AND anc.relation::TEXT = ANY(");
        builder.push_bind(relation_values);
        builder.push(")");
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
        if let Some(relations) = relations.filter(|relations| !relations.is_empty()) {
            parts.push(format!(
                "relations {}",
                relations
                    .iter()
                    .map(|relation| relation.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
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
