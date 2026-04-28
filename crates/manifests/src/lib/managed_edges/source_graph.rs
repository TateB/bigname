use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    MANIFEST_PROXY_IMPLEMENTATION_ADMISSION, MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE,
    MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND, MANIFEST_SUCCESSOR_ADMISSION,
    MANIFEST_SUCCESSOR_DISCOVERY_SOURCE, MANIFEST_SUCCESSOR_EDGE_KIND,
    support::{
        ExistingManagedEdge, ManagedEdgeSpec, ManifestLineageKey, ManifestTransition,
        OrderedManifestEntry,
    },
};

use super::active_addresses::reconcile_active_contract_instance_addresses;

pub(crate) async fn reconcile_manifest_source_graph(
    executor: &mut sqlx::postgres::PgConnection,
    in_place_transitions: &[ManifestTransition],
) -> Result<usize> {
    let desired_proxy_edges = load_desired_proxy_edges(executor).await?;
    let desired_successor_edges =
        load_desired_manifest_successor_edges(executor, in_place_transitions).await?;

    let mut cleared_edge_count = 0;
    cleared_edge_count += reconcile_managed_edges(
        executor,
        &desired_proxy_edges,
        MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE,
    )
    .await?;

    cleared_edge_count += reconcile_managed_edges(
        executor,
        &desired_successor_edges,
        MANIFEST_SUCCESSOR_DISCOVERY_SOURCE,
    )
    .await?;

    let stale_source_edge_count =
        deactivate_discovery_edges_without_active_source_manifest(executor).await?;
    cleared_edge_count += stale_source_edge_count;

    reconcile_active_contract_instance_addresses(executor).await?;

    Ok(cleared_edge_count)
}

async fn load_desired_proxy_edges(
    executor: &mut sqlx::postgres::PgConnection,
) -> Result<Vec<ManagedEdgeSpec>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.chain,
            mci.contract_instance_id,
            mci.implementation_contract_instance_id,
            mci.declaration_name,
            mci.proxy_kind,
            mci.declared_address,
            mci.declared_implementation_address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
          AND mci.declaration_kind = 'contract'
          AND mci.implementation_contract_instance_id IS NOT NULL
        ORDER BY mv.manifest_id, mci.declaration_name
        "#,
    )
    .fetch_all(executor)
    .await
    .context("failed to load desired proxy edges")?;

    rows.into_iter()
        .map(|row| {
            let implementation_contract_instance_id = row
                .try_get::<Uuid, _>("implementation_contract_instance_id")
                .context("failed to read implementation_contract_instance_id")?;
            let provenance_json = serde_json::json!({
                "source": "manifest_contract",
                "declaration_name": row
                    .try_get::<String, _>("declaration_name")
                    .context("failed to read declaration_name")?,
                "proxy_kind": row
                    .try_get::<String, _>("proxy_kind")
                    .context("failed to read proxy_kind")?,
                "from_address": row
                    .try_get::<String, _>("declared_address")
                    .context("failed to read declared_address")?,
                "to_address": row
                    .try_get::<Option<String>, _>("declared_implementation_address")
                    .context("failed to read declared_implementation_address")?,
            })
            .to_string();
            Ok(ManagedEdgeSpec {
                chain: row.try_get("chain").context("failed to read chain")?,
                edge_kind: MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND.to_owned(),
                from_contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read contract_instance_id")?,
                to_contract_instance_id: implementation_contract_instance_id,
                discovery_source: MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE.to_owned(),
                source_manifest_id: row
                    .try_get("manifest_id")
                    .context("failed to read manifest_id")?,
                admission: MANIFEST_PROXY_IMPLEMENTATION_ADMISSION.to_owned(),
                provenance_json,
            })
        })
        .collect()
}

async fn load_desired_manifest_successor_edges(
    executor: &mut sqlx::postgres::PgConnection,
    in_place_transitions: &[ManifestTransition],
) -> Result<Vec<ManagedEdgeSpec>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.manifest_version,
            mv.rollout_status::TEXT AS rollout_status,
            mv.namespace,
            mv.source_family,
            mv.chain,
            mv.deployment_epoch,
            mci.declaration_kind,
            mci.declaration_name,
            mci.contract_instance_id,
            mci.declared_address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        ORDER BY
            mv.namespace,
            mv.source_family,
            mv.chain,
            mv.deployment_epoch,
            mci.declaration_kind,
            mci.declaration_name,
            mv.manifest_version,
            mv.manifest_id
        "#,
    )
    .fetch_all(executor)
    .await
    .context("failed to load ordered manifest entries for successor continuity")?;

    let mut desired = HashSet::new();
    let mut last_by_lineage = HashMap::<ManifestLineageKey, OrderedManifestEntry>::new();

    for row in rows {
        let entry = OrderedManifestEntry {
            manifest_id: row
                .try_get("manifest_id")
                .context("failed to read manifest_id")?,
            manifest_version: row
                .try_get("manifest_version")
                .context("failed to read manifest_version")?,
            rollout_status: row
                .try_get("rollout_status")
                .context("failed to read rollout_status")?,
            chain: row.try_get("chain").context("failed to read chain")?,
            lineage_key: ManifestLineageKey {
                namespace: row
                    .try_get("namespace")
                    .context("failed to read namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read source_family")?,
                chain: row
                    .try_get("chain")
                    .context("failed to read lineage chain")?,
                deployment_epoch: row
                    .try_get("deployment_epoch")
                    .context("failed to read deployment_epoch")?,
                declaration_kind: row
                    .try_get("declaration_kind")
                    .context("failed to read declaration_kind")?,
                declaration_name: row
                    .try_get("declaration_name")
                    .context("failed to read declaration_name")?,
            },
            contract_instance_id: row
                .try_get("contract_instance_id")
                .context("failed to read contract_instance_id")?,
            declared_address: row
                .try_get("declared_address")
                .context("failed to read declared_address")?,
        };

        if let Some(previous_entry) =
            last_by_lineage.insert(entry.lineage_key.clone(), entry.clone())
            && entry.rollout_status == "active"
            && previous_entry.contract_instance_id != entry.contract_instance_id
            && previous_entry.declared_address != entry.declared_address
        {
            desired.insert(ManagedEdgeSpec {
                chain: entry.chain.clone(),
                edge_kind: MANIFEST_SUCCESSOR_EDGE_KIND.to_owned(),
                from_contract_instance_id: previous_entry.contract_instance_id,
                to_contract_instance_id: entry.contract_instance_id,
                discovery_source: MANIFEST_SUCCESSOR_DISCOVERY_SOURCE.to_owned(),
                source_manifest_id: entry.manifest_id,
                admission: MANIFEST_SUCCESSOR_ADMISSION.to_owned(),
                provenance_json: serde_json::json!({
                    "source": "manifest_successor",
                    "declaration_kind": entry.lineage_key.declaration_kind,
                    "declaration_name": entry.lineage_key.declaration_name,
                    "from_address": previous_entry.declared_address,
                    "to_address": entry.declared_address,
                    "manifest_version": entry.manifest_version,
                })
                .to_string(),
            });
        }
    }

    for transition in in_place_transitions {
        desired.insert(ManagedEdgeSpec {
            chain: transition.chain.clone(),
            edge_kind: MANIFEST_SUCCESSOR_EDGE_KIND.to_owned(),
            from_contract_instance_id: transition.from_contract_instance_id,
            to_contract_instance_id: transition.to_contract_instance_id,
            discovery_source: MANIFEST_SUCCESSOR_DISCOVERY_SOURCE.to_owned(),
            source_manifest_id: transition.source_manifest_id,
            admission: MANIFEST_SUCCESSOR_ADMISSION.to_owned(),
            provenance_json: serde_json::json!({
                "source": "manifest_successor",
                "declaration_kind": transition.declaration_kind,
                "declaration_name": transition.declaration_name,
                "from_address": transition.from_address,
                "to_address": transition.to_address,
                "manifest_update": "in_place",
            })
            .to_string(),
        });
    }

    Ok(desired.into_iter().collect())
}

async fn reconcile_managed_edges(
    executor: &mut sqlx::postgres::PgConnection,
    desired_edges: &[ManagedEdgeSpec],
    discovery_source: &str,
) -> Result<usize> {
    let existing_rows = sqlx::query(
        r#"
        SELECT
            discovery_edge_id,
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            provenance
        FROM discovery_edges
        WHERE discovery_source = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind(discovery_source)
    .fetch_all(&mut *executor)
    .await
    .with_context(|| {
        format!("failed to load active managed edges for discovery_source {discovery_source}")
    })?;

    let existing_edges = existing_rows
        .into_iter()
        .map(|row| {
            Ok(ExistingManagedEdge {
                discovery_edge_id: row
                    .try_get("discovery_edge_id")
                    .context("failed to read discovery_edge_id")?,
                spec: ManagedEdgeSpec {
                    chain: row.try_get("chain_id").context("failed to read chain_id")?,
                    edge_kind: row
                        .try_get("edge_kind")
                        .context("failed to read edge_kind")?,
                    from_contract_instance_id: row
                        .try_get("from_contract_instance_id")
                        .context("failed to read from_contract_instance_id")?,
                    to_contract_instance_id: row
                        .try_get("to_contract_instance_id")
                        .context("failed to read to_contract_instance_id")?,
                    discovery_source: row
                        .try_get("discovery_source")
                        .context("failed to read discovery_source")?,
                    source_manifest_id: row
                        .try_get::<Option<i64>, _>("source_manifest_id")
                        .context("failed to read source_manifest_id")?
                        .unwrap_or(-1),
                    admission: row
                        .try_get("admission")
                        .context("failed to read admission")?,
                    provenance_json: row
                        .try_get::<serde_json::Value, _>("provenance")
                        .context("failed to read provenance")?
                        .to_string(),
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let desired_set = desired_edges.iter().cloned().collect::<HashSet<_>>();
    let existing_set = existing_edges
        .iter()
        .map(|edge| edge.spec.clone())
        .collect::<HashSet<_>>();

    let mut cleared_edge_count = 0;
    for existing_edge in existing_edges {
        if desired_set.contains(&existing_edge.spec) {
            continue;
        }

        sqlx::query(
            r#"
            UPDATE discovery_edges
            SET deactivated_at = now()
            WHERE discovery_edge_id = $1
              AND deactivated_at IS NULL
            "#,
        )
        .bind(existing_edge.discovery_edge_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to deactivate managed discovery_edge_id {}",
                existing_edge.discovery_edge_id
            )
        })?;
        cleared_edge_count += 1;
    }

    for desired_edge in desired_edges {
        if existing_set.contains(desired_edge) {
            continue;
        }

        sqlx::query(
            r#"
            INSERT INTO discovery_edges (
                chain_id,
                edge_kind,
                from_contract_instance_id,
                to_contract_instance_id,
                discovery_source,
                source_manifest_id,
                admission,
                provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            "#,
        )
        .bind(&desired_edge.chain)
        .bind(&desired_edge.edge_kind)
        .bind(desired_edge.from_contract_instance_id)
        .bind(desired_edge.to_contract_instance_id)
        .bind(&desired_edge.discovery_source)
        .bind(desired_edge.source_manifest_id)
        .bind(&desired_edge.admission)
        .bind(&desired_edge.provenance_json)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert managed edge {} {} -> {}",
                desired_edge.edge_kind,
                desired_edge.from_contract_instance_id,
                desired_edge.to_contract_instance_id
            )
        })?;
    }

    Ok(cleared_edge_count)
}

async fn deactivate_discovery_edges_without_active_source_manifest(
    executor: &mut sqlx::postgres::PgConnection,
) -> Result<usize> {
    let result = sqlx::query(
        r#"
        UPDATE discovery_edges de
        SET deactivated_at = now()
        WHERE de.deactivated_at IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM manifest_versions mv
              WHERE mv.manifest_id = de.source_manifest_id
                AND mv.rollout_status = 'active'
          )
        "#,
    )
    .execute(&mut *executor)
    .await
    .context("failed to deactivate discovery edges without an active source manifest")?;

    Ok(result.rows_affected() as usize)
}
