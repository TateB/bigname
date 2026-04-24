use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{ManifestBootstrapTarget, normalize_address};

use super::types::ManifestBootstrapSkippedTarget;

const BOOTSTRAP_SKIP_REASON_UNKNOWN_START: &str = "unknown_start";

pub async fn load_manifest_declared_bootstrap_targets(
    pool: &PgPool,
    chain: &str,
) -> Result<Vec<ManifestBootstrapTarget>> {
    let rows = sqlx::query(
        r#"
        SELECT
            source_family,
            contract_instance_id,
            address,
            effective_from_block,
            effective_to_block
        FROM (
            SELECT
                mv.source_family AS source_family,
                mci.contract_instance_id AS contract_instance_id,
                cia.address AS address,
                CASE
                    WHEN cia.active_from_block_number IS NULL THEN manifest_range.start_block
                    ELSE GREATEST(manifest_range.start_block, cia.active_from_block_number)
                END AS effective_from_block,
                cia.active_to_block_number AS effective_to_block
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
            JOIN LATERAL (
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
            ) manifest_range ON manifest_range.start_block IS NOT NULL
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = mci.contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND mv.chain = $1
              AND (
                  cia.active_to_block_number IS NULL
                  OR manifest_range.start_block <= cia.active_to_block_number
              )
        ) bootstrap_targets
        ORDER BY source_family, contract_instance_id, address, effective_from_block, effective_to_block
        "#,
    )
    .bind(chain)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load manifest-declared bootstrap targets for chain {chain}")
    })?;

    let mut declared_starts_by_identity = BTreeMap::<(String, Uuid), i64>::new();
    let mut targets = BTreeSet::<ManifestBootstrapTarget>::new();
    for row in rows {
        let source_family = row
            .try_get::<String, _>("source_family")
            .context("failed to read bootstrap source_family")?;
        let contract_instance_id = row
            .try_get::<Uuid, _>("contract_instance_id")
            .context("failed to read bootstrap contract_instance_id")?;
        let effective_from_block = row
            .try_get::<i64, _>("effective_from_block")
            .context("failed to read bootstrap effective_from_block")?;
        let identity = (source_family.clone(), contract_instance_id);
        if let Some(existing_start) = declared_starts_by_identity.get(&identity) {
            if *existing_start != effective_from_block {
                bail!(
                    "conflicting start_block declarations for active source_family {} contract_instance_id {}",
                    source_family,
                    contract_instance_id
                );
            }
        } else {
            declared_starts_by_identity.insert(identity, effective_from_block);
        }

        targets.insert(ManifestBootstrapTarget {
            source_family,
            contract_instance_id,
            address: normalize_address(
                &row.try_get::<String, _>("address")
                    .context("failed to read bootstrap address")?,
            ),
            effective_from_block,
            effective_to_block: row
                .try_get("effective_to_block")
                .context("failed to read bootstrap effective_to_block")?,
        });
    }

    Ok(targets.into_iter().collect())
}

pub async fn load_manifest_skipped_bootstrap_targets(
    pool: &PgPool,
    chain: &str,
) -> Result<Vec<ManifestBootstrapSkippedTarget>> {
    let rows = sqlx::query(
        r#"
        WITH active_manifest_targets AS (
            SELECT
                mv.source_family AS source_family,
                mci.contract_instance_id AS contract_instance_id,
                cia.address AS address,
                manifest_range.start_block AS start_block
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
            JOIN LATERAL (
                SELECT (entry ->> 'start_block')::BIGINT AS start_block
                FROM jsonb_array_elements(
                    COALESCE(
                        CASE
                            WHEN mci.declaration_kind = 'root' THEN mv.manifest_payload -> 'roots'
                            ELSE mv.manifest_payload -> 'contracts'
                        END,
                        '[]'::jsonb
                    )
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
            ) manifest_range ON true
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = mci.contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND mv.chain = $1
        )
        SELECT
            source_family,
            contract_instance_id,
            address
        FROM active_manifest_targets skipped_target
        WHERE skipped_target.start_block IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM active_manifest_targets started_target
              WHERE started_target.source_family = skipped_target.source_family
                AND started_target.contract_instance_id = skipped_target.contract_instance_id
                AND started_target.start_block IS NOT NULL
          )
        ORDER BY source_family, contract_instance_id, address
        "#,
    )
    .bind(chain)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load skipped manifest-declared bootstrap targets for chain {chain}")
    })?;

    let mut targets = BTreeSet::<ManifestBootstrapSkippedTarget>::new();
    for row in rows {
        targets.insert(ManifestBootstrapSkippedTarget {
            source_family: row
                .try_get("source_family")
                .context("failed to read skipped bootstrap source_family")?,
            contract_instance_id: row
                .try_get("contract_instance_id")
                .context("failed to read skipped bootstrap contract_instance_id")?,
            address: normalize_address(
                &row.try_get::<String, _>("address")
                    .context("failed to read skipped bootstrap address")?,
            ),
            skip_reason: BOOTSTRAP_SKIP_REASON_UNKNOWN_START.to_owned(),
        });
    }

    Ok(targets.into_iter().collect())
}
