use std::collections::HashMap;

use super::super::{
    BASENAMES_BASE_REGISTRY_SOURCE_FAMILY, ENS_V1_REGISTRY_SOURCE_FAMILY, SUBREGISTRY_EDGE_KIND,
    hex_topic::normalize_address, scope::RegistryRawLogSourceScopeTarget,
};
use super::ActiveEmitter;
use anyhow::{Context, Result};
use sqlx::{PgPool, Row, postgres::PgRow};

pub(in crate::ens_v1_subregistry_discovery) async fn load_active_emitters(
    pool: &PgPool,
    chain: &str,
    source_scope: Option<&[RegistryRawLogSourceScopeTarget]>,
) -> Result<Vec<ActiveEmitter>> {
    let has_source_scope = source_scope.is_some();
    let source_scope = source_scope.unwrap_or(&[]);
    let scoped_source_families = source_scope
        .iter()
        .map(|target| target.source_family.clone())
        .collect::<Vec<_>>();
    let scoped_addresses = source_scope
        .iter()
        .map(|target| target.address.clone())
        .collect::<Vec<_>>();

    if has_source_scope {
        let mut manifest_emitters =
            load_manifest_declared_active_emitters(pool, chain, source_scope).await?;
        if source_scope_covered_by_emitters(source_scope, &manifest_emitters) {
            return Ok(manifest_emitters);
        }
        manifest_emitters
            .extend(load_scoped_discovery_active_emitters(pool, chain, source_scope).await?);
        sort_active_emitters(&mut manifest_emitters);
        return Ok(manifest_emitters);
    }

    let rows = sqlx::query(
        r#"
        SELECT
            chain,
            namespace,
            source_family,
            manifest_version,
            source_manifest_id,
            contract_instance_id,
            address,
            contract_role,
            active_from_block_number,
            active_to_block_number,
            source_rank
        FROM (
            SELECT
                mv.chain AS chain,
                mv.namespace AS namespace,
                mv.source_family AS source_family,
                mv.manifest_version AS manifest_version,
                mv.manifest_id AS source_manifest_id,
                mci.contract_instance_id AS contract_instance_id,
                cia.address AS address,
                COALESCE(mci.role, manifest_contract_role.role) AS contract_role,
                CASE
                    WHEN manifest_range.start_block IS NULL THEN cia.active_from_block_number
                    WHEN cia.active_from_block_number IS NULL THEN manifest_range.start_block
                    ELSE GREATEST(manifest_range.start_block, cia.active_from_block_number)
                END AS active_from_block_number,
                cia.active_to_block_number AS active_to_block_number,
                CASE WHEN mci.declaration_kind = 'root' THEN 0 ELSE 1 END::INT AS source_rank
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
            LEFT JOIN LATERAL (
                SELECT role
                FROM manifest_contract_instances role_mci
                WHERE role_mci.manifest_id = mv.manifest_id
                  AND role_mci.contract_instance_id = mci.contract_instance_id
                  AND role_mci.declaration_kind = 'contract'
                ORDER BY role_mci.declaration_name
                LIMIT 1
            ) manifest_contract_role ON TRUE
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = mci.contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND mv.chain = $1
              AND mv.source_family IN ($2, $3)
              AND (
                  $5::BOOLEAN = FALSE
                  OR EXISTS (
                      SELECT 1
                      FROM unnest($6::TEXT[], $7::TEXT[]) AS scoped(
                          source_family,
                          address
                      )
                      WHERE scoped.source_family = mv.source_family
                        AND scoped.address = lower(cia.address)
                  )
              )

            UNION

            SELECT
                de.chain_id AS chain,
                mv.namespace AS namespace,
                mv.source_family AS source_family,
                mv.manifest_version AS manifest_version,
                de.source_manifest_id AS source_manifest_id,
                de.to_contract_instance_id AS contract_instance_id,
                cia.address AS address,
                de.provenance ->> 'propagated_role' AS contract_role,
                CASE
                    WHEN de.active_from_block_number IS NULL THEN cia.active_from_block_number
                    WHEN cia.active_from_block_number IS NULL THEN de.active_from_block_number
                    ELSE GREATEST(de.active_from_block_number, cia.active_from_block_number)
                END AS active_from_block_number,
                CASE
                    WHEN de.active_to_block_number IS NULL THEN cia.active_to_block_number
                    WHEN cia.active_to_block_number IS NULL THEN de.active_to_block_number
                    ELSE LEAST(de.active_to_block_number, cia.active_to_block_number)
                END AS active_to_block_number,
                2::INT AS source_rank
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND de.deactivated_at IS NULL
              AND de.chain_id = $1
              AND de.edge_kind = $4
            AND mv.source_family IN ($2, $3)
              AND (
                  $5::BOOLEAN = FALSE
                  OR EXISTS (
                      SELECT 1
                      FROM unnest($6::TEXT[], $7::TEXT[]) AS scoped(
                          source_family,
                          address
                      )
                      WHERE scoped.source_family = mv.source_family
                        AND scoped.address = lower(cia.address)
                  )
              )
              AND (
                  de.active_from_block_number IS NULL
                  OR cia.active_to_block_number IS NULL
                  OR de.active_from_block_number <= cia.active_to_block_number
              )
              AND (
                  cia.active_from_block_number IS NULL
                  OR de.active_to_block_number IS NULL
                  OR cia.active_from_block_number <= de.active_to_block_number
              )
        ) registry_emitters
        ORDER BY lower(address), source_rank, source_manifest_id, contract_instance_id
        "#,
    )
    .bind(chain)
    .bind(ENS_V1_REGISTRY_SOURCE_FAMILY)
    .bind(BASENAMES_BASE_REGISTRY_SOURCE_FAMILY)
    .bind(SUBREGISTRY_EDGE_KIND)
    .bind(has_source_scope)
    .bind(&scoped_source_families)
    .bind(&scoped_addresses)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load active ENSv1 registry emitters for {chain}"))?;

    active_emitters_from_rows(rows)
}

async fn load_scoped_discovery_active_emitters(
    pool: &PgPool,
    chain: &str,
    source_scope: &[RegistryRawLogSourceScopeTarget],
) -> Result<Vec<ActiveEmitter>> {
    let scoped_source_families = source_scope
        .iter()
        .map(|target| target.source_family.clone())
        .collect::<Vec<_>>();
    let scoped_addresses = source_scope
        .iter()
        .map(|target| target.address.clone())
        .collect::<Vec<_>>();
    let scoped_from_blocks = source_scope
        .iter()
        .map(|target| target.effective_from_block)
        .collect::<Vec<_>>();
    let scoped_to_blocks = source_scope
        .iter()
        .map(|target| target.effective_to_block)
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        WITH scoped_targets AS (
            SELECT DISTINCT
                source_family,
                address,
                effective_from_block,
                effective_to_block
            FROM unnest($4::TEXT[], $5::TEXT[], $6::BIGINT[], $7::BIGINT[]) AS scoped(
                source_family,
                address,
                effective_from_block,
                effective_to_block
            )
        ),
        scoped_addresses AS (
            SELECT
                scoped.source_family AS scoped_source_family,
                scoped.effective_from_block,
                scoped.effective_to_block,
                cia.contract_instance_id,
                cia.chain_id,
                cia.address,
                cia.active_from_block_number AS address_active_from_block_number,
                cia.active_to_block_number AS address_active_to_block_number
            FROM scoped_targets scoped
            JOIN contract_instance_addresses cia
              ON cia.chain_id = $1
             AND cia.address = scoped.address
             AND cia.deactivated_at IS NULL
        )
        SELECT
            de.chain_id AS chain,
            mv.namespace AS namespace,
            mv.source_family AS source_family,
            mv.manifest_version AS manifest_version,
            de.source_manifest_id AS source_manifest_id,
            de.to_contract_instance_id AS contract_instance_id,
            scoped.address AS address,
            de.provenance ->> 'propagated_role' AS contract_role,
            CASE
                WHEN de.active_from_block_number IS NULL THEN scoped.address_active_from_block_number
                WHEN scoped.address_active_from_block_number IS NULL THEN de.active_from_block_number
                ELSE GREATEST(de.active_from_block_number, scoped.address_active_from_block_number)
            END AS active_from_block_number,
            CASE
                WHEN de.active_to_block_number IS NULL THEN scoped.address_active_to_block_number
                WHEN scoped.address_active_to_block_number IS NULL THEN de.active_to_block_number
                ELSE LEAST(de.active_to_block_number, scoped.address_active_to_block_number)
            END AS active_to_block_number,
            2::INT AS source_rank
        FROM scoped_addresses scoped
        JOIN discovery_edges de
          ON de.chain_id = scoped.chain_id
         AND de.to_contract_instance_id = scoped.contract_instance_id
         AND de.deactivated_at IS NULL
         AND de.edge_kind = $8
        JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
        WHERE mv.rollout_status = 'active'
          AND mv.source_family IN ($2, $3)
          AND scoped.scoped_source_family = mv.source_family
          AND (
              de.active_from_block_number IS NULL
              OR scoped.address_active_to_block_number IS NULL
              OR de.active_from_block_number <= scoped.address_active_to_block_number
          )
          AND (
              scoped.address_active_from_block_number IS NULL
              OR de.active_to_block_number IS NULL
              OR scoped.address_active_from_block_number <= de.active_to_block_number
          )
          AND scoped.effective_from_block <= COALESCE(
              CASE
                  WHEN de.active_to_block_number IS NULL THEN scoped.address_active_to_block_number
                  WHEN scoped.address_active_to_block_number IS NULL THEN de.active_to_block_number
                  ELSE LEAST(de.active_to_block_number, scoped.address_active_to_block_number)
              END,
              9223372036854775807
          )
          AND COALESCE(
              CASE
                  WHEN de.active_from_block_number IS NULL THEN scoped.address_active_from_block_number
                  WHEN scoped.address_active_from_block_number IS NULL THEN de.active_from_block_number
                  ELSE GREATEST(de.active_from_block_number, scoped.address_active_from_block_number)
              END,
              0
          ) <= scoped.effective_to_block
        ORDER BY lower(scoped.address), source_rank, source_manifest_id, contract_instance_id
        "#,
    )
    .bind(chain)
    .bind(ENS_V1_REGISTRY_SOURCE_FAMILY)
    .bind(BASENAMES_BASE_REGISTRY_SOURCE_FAMILY)
    .bind(&scoped_source_families)
    .bind(&scoped_addresses)
    .bind(&scoped_from_blocks)
    .bind(&scoped_to_blocks)
    .bind(SUBREGISTRY_EDGE_KIND)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load scoped discovery ENSv1 registry emitters for {chain}")
    })?;

    active_emitters_from_rows(rows)
}

async fn load_manifest_declared_active_emitters(
    pool: &PgPool,
    chain: &str,
    source_scope: &[RegistryRawLogSourceScopeTarget],
) -> Result<Vec<ActiveEmitter>> {
    let scoped_source_families = source_scope
        .iter()
        .map(|target| target.source_family.clone())
        .collect::<Vec<_>>();
    let scoped_addresses = source_scope
        .iter()
        .map(|target| target.address.clone())
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        SELECT
            mv.chain AS chain,
            mv.namespace AS namespace,
            mv.source_family AS source_family,
            mv.manifest_version AS manifest_version,
            mv.manifest_id AS source_manifest_id,
            mci.contract_instance_id AS contract_instance_id,
            cia.address AS address,
            COALESCE(mci.role, manifest_contract_role.role) AS contract_role,
            CASE
                WHEN manifest_range.start_block IS NULL THEN cia.active_from_block_number
                WHEN cia.active_from_block_number IS NULL THEN manifest_range.start_block
                ELSE GREATEST(manifest_range.start_block, cia.active_from_block_number)
            END AS active_from_block_number,
            cia.active_to_block_number AS active_to_block_number,
            CASE WHEN mci.declaration_kind = 'root' THEN 0 ELSE 1 END::INT AS source_rank
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
        LEFT JOIN LATERAL (
            SELECT role
            FROM manifest_contract_instances role_mci
            WHERE role_mci.manifest_id = mv.manifest_id
              AND role_mci.contract_instance_id = mci.contract_instance_id
              AND role_mci.declaration_kind = 'contract'
            ORDER BY role_mci.declaration_name
            LIMIT 1
        ) manifest_contract_role ON TRUE
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = mci.contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'
          AND mv.chain = $1
          AND mv.source_family IN ($2, $3)
          AND EXISTS (
              SELECT 1
              FROM unnest($4::TEXT[], $5::TEXT[]) AS scoped(
                  source_family,
                  address
              )
              WHERE scoped.source_family = mv.source_family
                AND scoped.address = lower(cia.address)
          )
        ORDER BY lower(cia.address), source_rank, source_manifest_id, contract_instance_id
        "#,
    )
    .bind(chain)
    .bind(ENS_V1_REGISTRY_SOURCE_FAMILY)
    .bind(BASENAMES_BASE_REGISTRY_SOURCE_FAMILY)
    .bind(&scoped_source_families)
    .bind(&scoped_addresses)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load manifest-declared ENSv1 registry emitters for {chain}")
    })?;

    active_emitters_from_rows(rows)
}

fn active_emitters_from_rows(rows: Vec<PgRow>) -> Result<Vec<ActiveEmitter>> {
    let mut emitters_by_scope =
        HashMap::<(String, String, Option<i64>, Option<i64>), ActiveEmitter>::new();
    for row in rows {
        let address = normalize_address(&row.try_get::<String, _>("address")?);
        let candidate = ActiveEmitter {
            address,
            contract_instance_id: row
                .try_get("contract_instance_id")
                .context("missing registry emitter contract_instance_id")?,
            source_manifest_id: row
                .try_get("source_manifest_id")
                .context("missing registry emitter source_manifest_id")?,
            namespace: row
                .try_get("namespace")
                .context("missing registry emitter namespace")?,
            source_family: row
                .try_get("source_family")
                .context("missing registry emitter source_family")?,
            manifest_version: row
                .try_get("manifest_version")
                .context("missing registry emitter manifest_version")?,
            contract_role: row
                .try_get("contract_role")
                .context("missing registry emitter contract_role")?,
            active_from_block_number: row
                .try_get("active_from_block_number")
                .context("missing registry emitter active_from_block_number")?,
            active_to_block_number: row
                .try_get("active_to_block_number")
                .context("missing registry emitter active_to_block_number")?,
            source_rank: row
                .try_get("source_rank")
                .context("missing registry emitter source_rank")?,
        };

        let scope_key = (
            candidate.source_family.clone(),
            candidate.address.clone(),
            candidate.active_from_block_number,
            candidate.active_to_block_number,
        );
        match emitters_by_scope.get(&scope_key) {
            Some(current) if !candidate_precedes(&candidate, current) => {}
            _ => {
                emitters_by_scope.insert(scope_key, candidate);
            }
        }
    }

    let mut emitters = emitters_by_scope.into_values().collect::<Vec<_>>();
    sort_active_emitters(&mut emitters);
    Ok(emitters)
}

fn sort_active_emitters(emitters: &mut [ActiveEmitter]) {
    emitters.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then(left.source_family.cmp(&right.source_family))
            .then(
                left.active_from_block_number
                    .cmp(&right.active_from_block_number),
            )
            .then(
                left.active_to_block_number
                    .cmp(&right.active_to_block_number),
            )
            .then(left.source_rank.cmp(&right.source_rank))
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
            .then(left.contract_instance_id.cmp(&right.contract_instance_id))
    });
}

fn source_scope_covered_by_emitters(
    source_scope: &[RegistryRawLogSourceScopeTarget],
    emitters: &[ActiveEmitter],
) -> bool {
    source_scope
        .iter()
        .filter(|target| {
            target.source_family == ENS_V1_REGISTRY_SOURCE_FAMILY
                || target.source_family == BASENAMES_BASE_REGISTRY_SOURCE_FAMILY
        })
        .all(|target| {
            emitters.iter().any(|emitter| {
                target.source_family == emitter.source_family
                    && target.address == emitter.address
                    && source_scope_target_intersects_emitter(target, emitter)
            })
        })
}

fn source_scope_target_intersects_emitter(
    target: &RegistryRawLogSourceScopeTarget,
    emitter: &ActiveEmitter,
) -> bool {
    let emitter_from = emitter.active_from_block_number.unwrap_or(0);
    let emitter_to = emitter.active_to_block_number.unwrap_or(i64::MAX);
    target.effective_from_block <= emitter_to && emitter_from <= target.effective_to_block
}

fn candidate_precedes(candidate: &ActiveEmitter, current: &ActiveEmitter) -> bool {
    (
        candidate.source_rank,
        candidate.source_manifest_id,
        candidate.contract_instance_id,
    ) < (
        current.source_rank,
        current.source_manifest_id,
        current.contract_instance_id,
    )
}
