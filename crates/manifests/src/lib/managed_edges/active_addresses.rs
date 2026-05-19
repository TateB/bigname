use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::support::{ActiveAddressSpec, CurrentActiveAddressRow};

pub(crate) async fn reconcile_active_contract_instance_addresses(
    executor: &mut sqlx::postgres::PgConnection,
) -> Result<()> {
    let desired_specs = load_desired_active_address_specs(executor, false, &[]).await?;
    let existing_active = load_existing_active_address_rows(executor, false, &[]).await?;
    apply_active_address_reconciliation(executor, desired_specs, existing_active).await
}

pub(crate) async fn reconcile_active_contract_instance_addresses_for_ids(
    executor: &mut sqlx::postgres::PgConnection,
    contract_instance_ids: &HashSet<Uuid>,
) -> Result<()> {
    if contract_instance_ids.is_empty() {
        return Ok(());
    }

    let mut scoped_ids = contract_instance_ids.iter().copied().collect::<Vec<_>>();
    scoped_ids.sort();
    let desired_specs = load_desired_active_address_specs(executor, true, &scoped_ids).await?;
    let existing_active = load_existing_active_address_rows(executor, true, &scoped_ids).await?;
    apply_active_address_reconciliation(executor, desired_specs, existing_active).await
}

async fn apply_active_address_reconciliation(
    executor: &mut sqlx::postgres::PgConnection,
    desired_specs: Vec<ActiveAddressSpec>,
    existing_active: Vec<CurrentActiveAddressRow>,
) -> Result<()> {
    let desired_ids = desired_specs
        .iter()
        .map(|spec| spec.contract_instance_id)
        .collect::<HashSet<_>>();

    for existing_row in &existing_active {
        if desired_ids.contains(&existing_row.contract_instance_id) {
            continue;
        }

        sqlx::query(
            r#"
            UPDATE contract_instance_addresses
            SET deactivated_at = now()
            WHERE contract_instance_id = $1
              AND deactivated_at IS NULL
            "#,
        )
        .bind(existing_row.contract_instance_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to deactivate active address row for contract_instance_id {}",
                existing_row.contract_instance_id
            )
        })?;
    }

    let existing_active_map = existing_active
        .into_iter()
        .map(|row| (row.contract_instance_id, row))
        .collect::<HashMap<_, _>>();

    for desired_spec in desired_specs {
        let manifest_declared_active_from_block =
            manifest_declared_active_from_block(&desired_spec.provenance_json)?;
        if let Some(existing_row) = existing_active_map.get(&desired_spec.contract_instance_id) {
            if existing_row.chain != desired_spec.chain
                || existing_row.address != desired_spec.address
            {
                bail!(
                    "contract_instance_id {} changed address from {}:{} to {}:{}; successor addresses must rotate IDs",
                    desired_spec.contract_instance_id,
                    existing_row.chain,
                    existing_row.address,
                    desired_spec.chain,
                    desired_spec.address
                );
            }
            if let Some(active_from_block_number) = manifest_declared_active_from_block {
                sqlx::query(
                    r#"
                    UPDATE contract_instance_addresses
                    SET active_from_block_number = $2,
                        active_from_block_hash = NULL
                    WHERE contract_instance_id = $1
                      AND deactivated_at IS NULL
                      AND (
                          active_from_block_number IS DISTINCT FROM $2
                          OR active_from_block_hash IS NOT NULL
                      )
                    "#,
                )
                .bind(desired_spec.contract_instance_id)
                .bind(active_from_block_number)
                .execute(&mut *executor)
                .await
                .with_context(|| {
                    format!(
                        "failed to update manifest-declared active range for contract_instance_id {}",
                        desired_spec.contract_instance_id
                    )
                })?;
            }
            continue;
        }

        let active_from_block_number = manifest_declared_active_from_block.flatten();
        sqlx::query(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                source_manifest_id,
                active_from_block_number,
                provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            "#,
        )
        .bind(desired_spec.contract_instance_id)
        .bind(&desired_spec.chain)
        .bind(&desired_spec.address)
        .bind(desired_spec.source_manifest_id)
        .bind(active_from_block_number)
        .bind(&desired_spec.provenance_json)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to activate address {} for contract_instance_id {}",
                desired_spec.address, desired_spec.contract_instance_id
            )
        })?;
    }

    Ok(())
}

async fn load_existing_active_address_rows(
    executor: &mut sqlx::postgres::PgConnection,
    scoped: bool,
    scope_ids: &[Uuid],
) -> Result<Vec<CurrentActiveAddressRow>> {
    let existing_active_rows = sqlx::query(
        r#"
        SELECT contract_instance_id, chain_id, address
        FROM contract_instance_addresses
        WHERE deactivated_at IS NULL
          AND ($1::BOOLEAN = FALSE OR contract_instance_id = ANY($2::UUID[]))
        "#,
    )
    .bind(scoped)
    .bind(scope_ids)
    .fetch_all(&mut *executor)
    .await
    .context("failed to load active contract-instance addresses")?;

    existing_active_rows
        .into_iter()
        .map(|row| {
            Ok(CurrentActiveAddressRow {
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read active contract_instance_id")?,
                chain: row
                    .try_get("chain_id")
                    .context("failed to read active chain_id")?,
                address: row
                    .try_get("address")
                    .context("failed to read active address")?,
            })
        })
        .collect::<Result<Vec<_>>>()
}

async fn load_desired_active_address_specs(
    executor: &mut sqlx::postgres::PgConnection,
    scoped: bool,
    scope_ids: &[Uuid],
) -> Result<Vec<ActiveAddressSpec>> {
    let manifest_rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.chain,
            mci.declaration_kind,
            mci.declaration_name,
            mci.contract_instance_id,
            mci.declared_address,
            manifest_range.start_block AS manifest_start_block,
            mci.implementation_contract_instance_id,
            mci.declared_implementation_address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        LEFT JOIN LATERAL (
            SELECT (entry ->> 'start_block')::BIGINT AS start_block
            FROM jsonb_array_elements(
                CASE
                    WHEN mci.declaration_kind = 'root' THEN mv.manifest_payload -> 'roots'
                    ELSE mv.manifest_payload -> 'contracts'
                END
            ) entry
            WHERE (
                    mci.declaration_kind = 'root'
                    AND entry ->> 'name' = mci.declaration_name
                )
               OR (
                    mci.declaration_kind = 'contract'
                    AND entry ->> 'role' = mci.declaration_name
                )
            ORDER BY start_block NULLS LAST
            LIMIT 1
        ) manifest_range ON TRUE
        WHERE mv.rollout_status = 'active'
          AND (
              $1::BOOLEAN = FALSE
              OR mci.contract_instance_id = ANY($2::UUID[])
              OR mci.implementation_contract_instance_id = ANY($2::UUID[])
          )
        ORDER BY mv.manifest_id, mci.declaration_kind, mci.declaration_name
        "#,
    )
    .bind(scoped)
    .bind(scope_ids)
    .fetch_all(&mut *executor)
    .await
    .context("failed to load active manifest address specs")?;

    let discovery_endpoint_rows = if scoped {
        sqlx::query(
            r#"
            WITH scoped_ids AS (
                SELECT DISTINCT contract_instance_id
                FROM UNNEST($1::UUID[]) AS scope(contract_instance_id)
            )
            SELECT
                endpoint.source_manifest_id,
                cia.contract_instance_id,
                cia.chain_id,
                cia.address
            FROM scoped_ids scope
            JOIN LATERAL (
                SELECT de.source_manifest_id
                FROM discovery_edges de
                JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
                WHERE mv.rollout_status = 'active'
                  AND de.deactivated_at IS NULL
                  AND de.edge_kind <> 'migration'
                  AND de.to_contract_instance_id = scope.contract_instance_id
                ORDER BY de.source_manifest_id
                LIMIT 1
            ) endpoint ON TRUE
            JOIN LATERAL (
                SELECT contract_instance_id, chain_id, address
                FROM contract_instance_addresses
                WHERE contract_instance_id = scope.contract_instance_id
                ORDER BY (deactivated_at IS NULL) DESC, admitted_at DESC
                LIMIT 1
            ) cia ON TRUE
            "#,
        )
        .bind(scope_ids)
        .fetch_all(&mut *executor)
        .await
    } else {
        sqlx::query(
            r#"
            WITH active_discovery_endpoints AS (
                SELECT DISTINCT ON (de.to_contract_instance_id)
                    de.source_manifest_id,
                    de.to_contract_instance_id AS contract_instance_id
                FROM discovery_edges de
                JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
                WHERE mv.rollout_status = 'active'
                  AND de.deactivated_at IS NULL
                  AND de.edge_kind <> 'migration'
                ORDER BY de.to_contract_instance_id, de.source_manifest_id
            )
            SELECT
                endpoints.source_manifest_id,
                cia.contract_instance_id,
                cia.chain_id,
                cia.address
            FROM active_discovery_endpoints endpoints
            JOIN LATERAL (
                SELECT contract_instance_id, chain_id, address
                FROM contract_instance_addresses
                WHERE contract_instance_id = endpoints.contract_instance_id
                ORDER BY (deactivated_at IS NULL) DESC, admitted_at DESC
                LIMIT 1
            ) cia ON TRUE
            "#,
        )
        .fetch_all(&mut *executor)
        .await
    }
    .context("failed to load discovery-edge endpoint address specs")?;

    let mut specs = HashMap::<Uuid, ActiveAddressSpec>::new();

    for row in manifest_rows {
        let contract_instance_id = row
            .try_get::<Uuid, _>("contract_instance_id")
            .context("failed to read manifest contract_instance_id")?;
        let declaration_kind = row
            .try_get::<String, _>("declaration_kind")
            .context("failed to read declaration_kind")?;
        let declaration_name = row
            .try_get::<String, _>("declaration_name")
            .context("failed to read declaration_name")?;
        let chain: String = row.try_get("chain").context("failed to read chain")?;
        let manifest_id = row
            .try_get::<i64, _>("manifest_id")
            .context("failed to read manifest_id")?;
        let declared_address = row
            .try_get::<String, _>("declared_address")
            .context("failed to read declared_address")?;
        let manifest_start_block = row
            .try_get::<Option<i64>, _>("manifest_start_block")
            .context("failed to read manifest_start_block")?;

        if !scoped || scope_ids.contains(&contract_instance_id) {
            let provenance_json = manifest_declared_address_provenance(
                &declaration_kind,
                &declaration_name,
                manifest_start_block,
            )?;
            if let Some(existing_spec) = specs.get_mut(&contract_instance_id) {
                if manifest_start_block.is_some()
                    && manifest_declared_active_from_block(&existing_spec.provenance_json)?
                        .flatten()
                        .is_none()
                {
                    existing_spec.provenance_json = provenance_json;
                }
            } else {
                specs.insert(
                    contract_instance_id,
                    ActiveAddressSpec {
                        contract_instance_id,
                        chain: chain.clone(),
                        address: declared_address.clone(),
                        source_manifest_id: Some(manifest_id),
                        provenance_json,
                    },
                );
            }
        }

        let implementation_contract_instance_id = row
            .try_get::<Option<Uuid>, _>("implementation_contract_instance_id")
            .context("failed to read implementation_contract_instance_id")?;
        let declared_implementation_address = row
            .try_get::<Option<String>, _>("declared_implementation_address")
            .context("failed to read declared_implementation_address")?;
        if let (Some(implementation_contract_instance_id), Some(implementation_address)) = (
            implementation_contract_instance_id,
            declared_implementation_address,
        ) {
            if !scoped || scope_ids.contains(&implementation_contract_instance_id) {
                specs
                    .entry(implementation_contract_instance_id)
                    .or_insert(ActiveAddressSpec {
                        contract_instance_id: implementation_contract_instance_id,
                        chain: chain.clone(),
                        address: implementation_address.clone(),
                        source_manifest_id: Some(manifest_id),
                        provenance_json: serde_json::json!({
                            "source": "manifest_proxy_implementation",
                            "proxy_contract_instance_id": contract_instance_id,
                            "proxy_address": declared_address,
                        })
                        .to_string(),
                    });
            }
        }
    }

    for row in discovery_endpoint_rows {
        let contract_instance_id = row
            .try_get::<Uuid, _>("contract_instance_id")
            .context("failed to read discovery endpoint contract_instance_id")?;
        specs
            .entry(contract_instance_id)
            .or_insert(ActiveAddressSpec {
                contract_instance_id,
                chain: row
                    .try_get("chain_id")
                    .context("failed to read discovery endpoint chain_id")?,
                address: row
                    .try_get("address")
                    .context("failed to read discovery endpoint address")?,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("failed to read discovery endpoint source_manifest_id")?,
                provenance_json: serde_json::json!({
                    "source": "discovery_edge_endpoint",
                })
                .to_string(),
            });
    }

    Ok(specs.into_values().collect())
}

fn manifest_declared_address_provenance(
    declaration_kind: &str,
    declaration_name: &str,
    start_block: Option<i64>,
) -> Result<String> {
    let mut provenance = serde_json::json!({
        "source": "manifest_declared",
        "declaration_kind": declaration_kind,
        "declaration_name": declaration_name,
    });
    if let (Some(fields), Some(start_block)) = (provenance.as_object_mut(), start_block) {
        fields.insert("start_block".to_owned(), serde_json::json!(start_block));
    }
    serde_json::to_string(&provenance).context("failed to serialize manifest address provenance")
}

fn manifest_declared_active_from_block(provenance_json: &str) -> Result<Option<Option<i64>>> {
    let provenance = serde_json::from_str::<Value>(provenance_json)
        .context("failed to parse active address provenance")?;
    if provenance.get("source").and_then(Value::as_str) != Some("manifest_declared") {
        return Ok(None);
    }

    Ok(Some(
        provenance
            .get("start_block")
            .map(|value| {
                value
                    .as_i64()
                    .context("manifest_declared start_block provenance must be an integer")
            })
            .transpose()?,
    ))
}
