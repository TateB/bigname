use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgRow};
use uuid::Uuid;

use crate::{
    address_names::AddressNameRelation, name_current::DEFAULT_NAME_CURRENT_READ_FILTER,
    record_inventory::record_version_boundary_storage_key,
};

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
    let inventory_requests = name_rows
        .values()
        .filter_map(IdentityRecordInventoryRequest::from_name_row)
        .collect::<Vec<_>>();
    let inventories = load_record_inventory_current_by_requests(pool, &inventory_requests).await?;
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
            let record_inventory_current = row.resource_id.and_then(|resource_id| {
                inventories
                    .get(resource_id, row.record_inventory_boundary_key.as_deref())
                    .cloned()
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

pub async fn load_identity_name_feed_records_by_names(
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
            Some(IdentityNameRecordRow {
                row,
                record_inventory_current: None,
                relations: relations_by_name
                    .get(&logical_name_id)
                    .cloned()
                    .unwrap_or_default(),
            })
        })
        .collect();

    Ok(records)
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct IdentityRecordInventoryRequest {
    resource_id: Uuid,
    record_version_boundary_key: Option<String>,
}

impl IdentityRecordInventoryRequest {
    fn from_name_row(row: &IdentityNameCurrentRow) -> Option<Self> {
        Some(Self {
            resource_id: row.resource_id?,
            record_version_boundary_key: row.record_inventory_boundary_key.clone(),
        })
    }
}

#[derive(Default)]
struct IdentityRecordInventoryLookup {
    by_exact_key: BTreeMap<(Uuid, String), IdentityRecordInventoryRow>,
    by_unambiguous_resource: BTreeMap<Uuid, IdentityRecordInventoryRow>,
}

impl IdentityRecordInventoryLookup {
    fn get(
        &self,
        resource_id: Uuid,
        record_version_boundary_key: Option<&str>,
    ) -> Option<&IdentityRecordInventoryRow> {
        record_version_boundary_key
            .and_then(|key| self.by_exact_key.get(&(resource_id, key.to_owned())))
            .or_else(|| {
                record_version_boundary_key
                    .is_none()
                    .then(|| self.by_unambiguous_resource.get(&resource_id))
                    .flatten()
            })
    }
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

async fn load_record_inventory_current_by_requests(
    pool: &PgPool,
    requests: &[IdentityRecordInventoryRequest],
) -> Result<IdentityRecordInventoryLookup> {
    if requests.is_empty() {
        return Ok(IdentityRecordInventoryLookup::default());
    }

    let exact_requests = requests
        .iter()
        .filter_map(|request| {
            Some((
                request.resource_id,
                request.record_version_boundary_key.clone()?,
            ))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let legacy_resource_ids = requests
        .iter()
        .filter(|request| request.record_version_boundary_key.is_none())
        .map(|request| request.resource_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let mut inventories = IdentityRecordInventoryLookup::default();
    if !exact_requests.is_empty() {
        let resource_ids = exact_requests
            .iter()
            .map(|(resource_id, _)| *resource_id)
            .collect::<Vec<_>>();
        let boundary_keys = exact_requests
            .iter()
            .map(|(_, boundary_key)| boundary_key.clone())
            .collect::<Vec<_>>();
        let rows = sqlx::query(&format!(
            r#"
            WITH requested AS (
                SELECT *
                FROM UNNEST($1::UUID[], $2::TEXT[])
                  AS requested(resource_id, record_version_boundary_key)
            )
            SELECT
                ric.resource_id,
                ric.record_version_boundary_key,
                ric.unsupported_families,
                ric.entries,
                ric.chain_positions,
                ric.last_recomputed_at
            FROM requested
            JOIN record_inventory_current ric
              ON ric.resource_id = requested.resource_id
             AND ric.record_version_boundary_key = requested.record_version_boundary_key
            JOIN resources resource
              ON resource.resource_id = ric.resource_id
            WHERE TRUE
            {DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER}
            "#,
        ))
        .bind(&resource_ids)
        .bind(&boundary_keys)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to batch load exact record_inventory_current rows for {} identity resources",
                exact_requests.len()
            )
        })?;

        for row in rows {
            let resource_id = crate::sql_row::get::<Uuid>(&row, "resource_id")?;
            let boundary_key = crate::sql_row::get::<String>(&row, "record_version_boundary_key")?;
            let inventory = decode_record_inventory_current_row(row)?;
            inventories
                .by_exact_key
                .insert((resource_id, boundary_key), inventory);
        }
    }

    if legacy_resource_ids.is_empty() {
        return Ok(inventories);
    }

    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ric.resource_id,
            ric.record_version_boundary_key,
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
    .bind(&legacy_resource_ids)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to batch load legacy record_inventory_current rows for {} resources",
            legacy_resource_ids.len()
        )
    })?;

    let mut rows_by_resource = BTreeMap::<Uuid, Vec<IdentityRecordInventoryRow>>::new();
    for row in rows {
        let inventory = decode_record_inventory_current_row(row)?;
        rows_by_resource
            .entry(inventory.resource_id)
            .or_default()
            .push(inventory);
    }
    for (resource_id, rows) in rows_by_resource {
        if rows.len() == 1 {
            if let Some(inventory) = rows.into_iter().next() {
                inventories
                    .by_unambiguous_resource
                    .insert(resource_id, inventory);
            }
        }
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
    let logical_name_id: String = crate::sql_row::get(&row, "logical_name_id")?;
    let resource_id: Option<Uuid> = crate::sql_row::get(&row, "resource_id")?;
    let declared_summary: Value = crate::sql_row::get(&row, "declared_summary")?;
    let record_inventory_boundary_key =
        identity_record_inventory_boundary_key(&logical_name_id, resource_id, &declared_summary)?;

    Ok(IdentityNameCurrentRow {
        logical_name_id,
        namespace: crate::sql_row::get(&row, "namespace")?,
        canonical_display_name: crate::sql_row::get(&row, "canonical_display_name")?,
        normalized_name: crate::sql_row::get(&row, "normalized_name")?,
        namehash: crate::sql_row::get(&row, "namehash")?,
        labelhash: crate::sql_row::get(&row, "labelhash")?,
        labelhash_count: crate::sql_row::get(&row, "labelhash_count")?,
        resource_id,
        record_inventory_boundary_key,
        declared_summary,
        coverage: crate::sql_row::get(&row, "coverage")?,
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
    })
}

fn identity_record_inventory_boundary_key(
    logical_name_id: &str,
    resource_id: Option<Uuid>,
    declared_summary: &Value,
) -> Result<Option<String>> {
    let Some(resource_id) = resource_id else {
        return Ok(None);
    };
    let Some(record_version_boundary) =
        declared_summary.pointer("/topology/version_boundaries/record_version_boundary")
    else {
        return Ok(None);
    };
    record_version_boundary_storage_key(record_version_boundary, resource_id)
        .map(Some)
        .with_context(|| {
            format!(
                "failed to derive identity record_inventory_current boundary key for {logical_name_id}"
            )
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
