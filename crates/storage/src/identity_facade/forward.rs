use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use sqlx::{PgPool, postgres::PgRow};
use uuid::Uuid;

use crate::{address_names::AddressNameRelation, name_current::DEFAULT_NAME_CURRENT_READ_FILTER};

use super::{
    DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER, DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER,
    IdentityAddressRelationRow, IdentityNameCurrentRow, IdentityNameRecordRow,
    IdentityRecordInventoryRow, dedupe_in_order,
};

pub async fn load_identity_records_by_names(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<Vec<IdentityNameRecordRow>> {
    let requested_ids = dedupe_in_order(logical_name_ids.iter().cloned());
    if requested_ids.is_empty() {
        return Ok(Vec::new());
    }

    let (name_rows, relations) = futures_util::try_join!(
        load_identity_name_current_rows(pool, &requested_ids),
        load_identity_address_relations_by_logical_names(pool, &requested_ids),
    )?;
    let inventory_resource_ids = name_rows
        .values()
        .filter_map(|row| row.resource_id)
        .collect::<Vec<_>>();
    let inventories =
        load_record_inventory_current_by_resource_ids(pool, &inventory_resource_ids).await?;
    let relations_by_name = relations.into_iter().fold(
        BTreeMap::<String, Vec<IdentityAddressRelationRow>>::new(),
        |mut grouped, relation| {
            grouped
                .entry(relation.logical_name_id.clone())
                .or_default()
                .push(relation);
            grouped
        },
    );

    let records = requested_ids
        .into_iter()
        .filter_map(|logical_name_id| {
            let row = name_rows.get(&logical_name_id)?.clone();
            let record_inventory_current = row
                .resource_id
                .and_then(|resource_id| inventories.by_resource.get(&resource_id).cloned());
            Some(IdentityNameRecordRow {
                row,
                record_inventory_current,
                relations: relations_by_name
                    .get(&logical_name_id)
                    .cloned()
                    .unwrap_or_default(),
            })
        })
        .collect();

    Ok(records)
}

#[derive(Default)]
struct IdentityRecordInventoryLookup {
    by_resource: BTreeMap<Uuid, IdentityRecordInventoryRow>,
}

async fn load_identity_name_current_rows(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, IdentityNameCurrentRow>> {
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
            surface.labelhashes[1] AS labelhash,
            array_length(surface.labelhashes, 1) AS labelhash_count,
            nc.resource_id,
            nc.declared_summary,
            nc.coverage,
            nc.chain_positions,
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
            "failed to batch load identity name_current rows for {} logical_name_id values",
            logical_name_ids.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let row = decode_identity_name_current_row(row)?;
            Ok((row.logical_name_id.clone(), row))
        })
        .collect()
}

async fn load_record_inventory_current_by_resource_ids(
    pool: &PgPool,
    resource_ids: &[Uuid],
) -> Result<IdentityRecordInventoryLookup> {
    if resource_ids.is_empty() {
        return Ok(IdentityRecordInventoryLookup::default());
    }

    let requested_resource_ids = resource_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ric.resource_id,
            ric.unsupported_families,
            ric.entries,
            ric.chain_positions,
            ric.last_recomputed_at
        FROM record_inventory_current ric
        JOIN resources resource
          ON resource.resource_id = ric.resource_id
        WHERE ric.resource_id = ANY($1::UUID[])
        {DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER}
        ORDER BY ric.resource_id::TEXT, ric.record_version_boundary_key
        "#,
    ))
    .bind(&requested_resource_ids)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to batch load record_inventory_current rows for {} resources",
            requested_resource_ids.len()
        )
    })?;

    let mut inventories = IdentityRecordInventoryLookup::default();
    for row in rows {
        let inventory = decode_record_inventory_current_row(row)?;
        inventories
            .by_resource
            .entry(inventory.resource_id)
            .or_insert_with(|| inventory.clone());
    }

    Ok(inventories)
}

async fn load_identity_address_relations_by_logical_names(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<Vec<IdentityAddressRelationRow>> {
    if logical_name_ids.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(&format!(
        r#"
        SELECT
            anc.address,
            anc.logical_name_id,
            anc.relation
        FROM address_names_current anc
        JOIN name_surfaces surface
          ON surface.logical_name_id = anc.logical_name_id
        JOIN resources resource
          ON resource.resource_id = anc.resource_id
        JOIN surface_bindings binding
          ON binding.surface_binding_id = anc.surface_binding_id
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = anc.token_lineage_id
        WHERE anc.logical_name_id = ANY($1::TEXT[])
        {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}
        ORDER BY
            anc.address ASC,
            anc.logical_name_id ASC,
            CASE anc.relation
                WHEN 'registrant' THEN 0
                WHEN 'token_holder' THEN 1
                WHEN 'effective_controller' THEN 2
                ELSE 99
            END ASC
        "#,
    ))
    .bind(logical_name_ids)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to batch load address_names_current relation rows for {} logical_name_ids",
            logical_name_ids.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let relation: String = crate::sql_row::get(&row, "relation")?;
            Ok(IdentityAddressRelationRow {
                address: crate::sql_row::get::<String>(&row, "address")?.to_ascii_lowercase(),
                logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
                relation: parse_address_name_relation(&relation)?,
            })
        })
        .collect()
}

fn decode_identity_name_current_row(row: PgRow) -> Result<IdentityNameCurrentRow> {
    Ok(IdentityNameCurrentRow {
        logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
        namespace: crate::sql_row::get(&row, "namespace")?,
        canonical_display_name: crate::sql_row::get(&row, "canonical_display_name")?,
        normalized_name: crate::sql_row::get(&row, "normalized_name")?,
        namehash: crate::sql_row::get(&row, "namehash")?,
        labelhash: crate::sql_row::get(&row, "labelhash")?,
        labelhash_count: crate::sql_row::get(&row, "labelhash_count")?,
        resource_id: crate::sql_row::get(&row, "resource_id")?,
        declared_summary: crate::sql_row::get(&row, "declared_summary")?,
        coverage: crate::sql_row::get(&row, "coverage")?,
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
    })
}

fn decode_record_inventory_current_row(row: PgRow) -> Result<IdentityRecordInventoryRow> {
    Ok(IdentityRecordInventoryRow {
        resource_id: crate::sql_row::get(&row, "resource_id")?,
        unsupported_families: crate::sql_row::get(&row, "unsupported_families")?,
        entries: crate::sql_row::get(&row, "entries")?,
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
    })
}

fn parse_address_name_relation(value: &str) -> Result<AddressNameRelation> {
    match value {
        "registrant" => Ok(AddressNameRelation::Registrant),
        "token_holder" => Ok(AddressNameRelation::TokenHolder),
        "effective_controller" => Ok(AddressNameRelation::EffectiveController),
        _ => bail!("unknown identity address-name relation {value}"),
    }
}
