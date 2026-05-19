use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgRow};
use uuid::Uuid;

use crate::{
    address_names::AddressNameRelation, name_current::load_name_current_by_logical_name_ids,
    record_inventory::RecordInventoryCurrentRow, resolution_record_inventory_lookup_key,
};

use super::{
    DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER, DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER,
    IdentityAddressRelationRow, IdentityNameRecordRow, dedupe_in_order,
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
        load_name_current_by_logical_name_ids(pool, &requested_ids),
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
            let record_inventory_current = resolution_record_inventory_lookup_key(&row)
                .and_then(|(resource_id, boundary)| {
                    inventories
                        .by_lookup_key
                        .get(&(resource_id, stable_json_key(&boundary)))
                        .cloned()
                })
                .or_else(|| {
                    row.resource_id
                        .and_then(|resource_id| inventories.by_resource.get(&resource_id).cloned())
                });
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
    by_lookup_key: BTreeMap<(Uuid, String), RecordInventoryCurrentRow>,
    by_resource: BTreeMap<Uuid, RecordInventoryCurrentRow>,
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
            ric.record_version_boundary_key,
            ric.record_version_boundary,
            ric.enumeration_basis,
            ric.selectors,
            ric.explicit_gaps,
            ric.unsupported_families,
            ric.last_change,
            ric.entries,
            ric.provenance,
            ric.coverage,
            ric.chain_positions,
            ric.canonicality_summary,
            ric.manifest_version,
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
        let key = (
            inventory.resource_id,
            stable_json_key(&inventory.record_version_boundary),
        );
        inventories
            .by_resource
            .entry(inventory.resource_id)
            .or_insert_with(|| inventory.clone());
        inventories.by_lookup_key.insert(key, inventory);
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

fn decode_record_inventory_current_row(row: PgRow) -> Result<RecordInventoryCurrentRow> {
    Ok(RecordInventoryCurrentRow {
        resource_id: crate::sql_row::get(&row, "resource_id")?,
        record_version_boundary: crate::sql_row::get(&row, "record_version_boundary")?,
        enumeration_basis: crate::sql_row::get(&row, "enumeration_basis")?,
        selectors: crate::sql_row::get(&row, "selectors")?,
        explicit_gaps: crate::sql_row::get(&row, "explicit_gaps")?,
        unsupported_families: crate::sql_row::get(&row, "unsupported_families")?,
        last_change: crate::sql_row::get(&row, "last_change")?,
        entries: crate::sql_row::get(&row, "entries")?,
        provenance: crate::sql_row::get(&row, "provenance")?,
        coverage: crate::sql_row::get(&row, "coverage")?,
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        canonicality_summary: crate::sql_row::get(&row, "canonicality_summary")?,
        manifest_version: crate::sql_row::get(&row, "manifest_version")?,
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

fn stable_json_key(value: &Value) -> String {
    serde_json::to_string(value).expect("JSON values from storage must serialize")
}
