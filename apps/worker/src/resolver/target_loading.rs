use bigname_storage::sql_row;
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bigname_storage::{
    CanonicalityState, PermissionsCurrentRow, SurfaceBindingKind,
    load_permissions_current_for_resolver_scope, load_permissions_current_resolver_targets,
    normalize_evm_address,
};
use serde_json::Value;
use sqlx::{PgPool, Row, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{
    CANONICAL_STATE_FILTER, EVENT_KIND_ALIAS_CHANGED, EVENT_KIND_RESOLVER_CHANGED,
    SOURCE_FAMILY_BASENAMES_BASE_REGISTRY, SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
    SOURCE_FAMILY_ENS_V1_REGISTRY_L1, SOURCE_FAMILY_ENS_V1_RESOLVER_L1, ZERO_ADDRESS,
    state_helpers::parse_canonicality_state,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ResolverTarget {
    pub(super) chain_id: String,
    pub(super) resolver_address: String,
    pub(super) profile_source_family: Option<String>,
    pub(super) enumerate_bindings: bool,
}

#[derive(Clone, Debug)]
pub(super) struct CurrentBindingSeed {
    pub(super) chain_id: String,
    pub(super) logical_name_id: String,
    pub(super) canonical_display_name: String,
    pub(super) normalized_name: String,
    pub(super) namehash: String,
    pub(super) resource_id: Uuid,
    pub(super) surface_binding_id: Uuid,
    pub(super) binding_kind: SurfaceBindingKind,
    pub(super) normalized_event_id: i64,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) source_manifest_id: Option<i64>,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) block_timestamp: Option<OffsetDateTime>,
    pub(super) raw_fact_ref: Value,
    pub(super) canonicality_state: CanonicalityState,
}

#[derive(Clone, Debug)]
pub(super) struct AliasSeed {
    pub(super) chain_id: String,
    pub(super) resolver_address: String,
    pub(super) normalized_event_id: i64,
    pub(super) logical_name_id: Option<String>,
    pub(super) resource_id: Option<Uuid>,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) source_manifest_id: Option<i64>,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) block_timestamp: Option<OffsetDateTime>,
    pub(super) raw_fact_ref: Value,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) after_state: Value,
}

pub(super) async fn load_target_resolvers(pool: &PgPool) -> Result<Vec<ResolverTarget>> {
    let rows = sqlx::query(&format!(
        r#"
        WITH current_bindings AS (
            SELECT logical_name_id, resource_id
            FROM surface_bindings
            WHERE active_to IS NULL
              AND canonicality_state {CANONICAL_STATE_FILTER}
        ),
        latest_resolver_events AS (
            SELECT DISTINCT ON (ne.logical_name_id, ne.resource_id)
                ne.logical_name_id,
                ne.resource_id,
                ne.chain_id,
                ne.source_family,
                LOWER(ne.after_state->>'resolver') AS resolver_address
            FROM normalized_events ne
            WHERE ne.event_kind = $1
              AND ne.logical_name_id IS NOT NULL
              AND ne.resource_id IS NOT NULL
              AND ne.chain_id IS NOT NULL
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
            ORDER BY
                ne.logical_name_id,
                ne.resource_id,
                ne.block_number DESC NULLS LAST,
                ne.log_index DESC NULLS LAST,
                ne.normalized_event_id DESC
        )
        SELECT DISTINCT chain_id, resolver_address, source_family
        FROM (
            SELECT
                lre.chain_id,
                lre.resolver_address,
                lre.source_family
            FROM latest_resolver_events lre
            INNER JOIN current_bindings cb
              ON cb.logical_name_id = lre.logical_name_id
             AND cb.resource_id = lre.resource_id
            WHERE lre.resolver_address IS NOT NULL
              AND lre.resolver_address <> ''
              AND lre.resolver_address <> $2
        ) targets
        ORDER BY chain_id, resolver_address
        "#
    ))
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(ZERO_ADDRESS)
    .fetch_all(pool)
    .await
    .context("failed to load resolver_current rebuild targets")?;

    let mut targets = BTreeMap::<(String, String), BTreeSet<String>>::new();
    for row in rows {
        let chain_id = sql_row::get(&row, "chain_id")?;
        let resolver_address =
            normalize_resolver_address(&sql_row::get::<String>(&row, "resolver_address")?);
        let source_family = sql_row::get::<String>(&row, "source_family")?;
        insert_target(
            &mut targets,
            chain_id,
            resolver_address,
            resolver_profile_source_family_for_event_source(&source_family),
        );
    }

    for (chain_id, resolver_address) in load_permissions_current_resolver_targets(pool).await? {
        insert_target(&mut targets, chain_id, resolver_address, None);
    }
    for target in load_alias_target_resolvers(pool).await? {
        let profile_source_family = target.profile_source_family;
        insert_target(
            &mut targets,
            target.chain_id,
            target.resolver_address,
            profile_source_family.as_deref(),
        );
    }

    Ok(targets
        .into_iter()
        .map(
            |((chain_id, resolver_address), source_families)| ResolverTarget {
                chain_id,
                resolver_address,
                profile_source_family: unique_source_family(source_families),
                enumerate_bindings: false,
            },
        )
        .collect())
}

async fn load_alias_target_resolvers(pool: &PgPool) -> Result<Vec<ResolverTarget>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT
            chain_id,
            LOWER(after_state->>'resolver') AS resolver_address,
            source_family
        FROM normalized_events
        WHERE event_kind = $1
          AND chain_id IS NOT NULL
          AND after_state->>'resolver' IS NOT NULL
          AND canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY chain_id, resolver_address
        "#
    ))
    .bind(EVENT_KIND_ALIAS_CHANGED)
    .fetch_all(pool)
    .await
    .context("failed to load AliasChanged resolver_current rebuild targets")?;

    rows.into_iter()
        .map(|row| {
            Ok(ResolverTarget {
                chain_id: sql_row::get(&row, "chain_id")?,
                resolver_address: normalize_resolver_address(&sql_row::get::<String>(
                    &row,
                    "resolver_address",
                )?),
                profile_source_family: row
                    .try_get::<String, _>("source_family")
                    .context("missing source_family")
                    .ok()
                    .and_then(|source_family| {
                        resolver_profile_source_family_for_event_source(&source_family)
                            .map(str::to_owned)
                    }),
                enumerate_bindings: false,
            })
        })
        .collect()
}

fn insert_target(
    targets: &mut BTreeMap<(String, String), BTreeSet<String>>,
    chain_id: String,
    resolver_address: String,
    profile_source_family: Option<&str>,
) {
    let source_families = targets.entry((chain_id, resolver_address)).or_default();
    if let Some(profile_source_family) = profile_source_family {
        source_families.insert(profile_source_family.to_owned());
    }
}

fn unique_source_family(source_families: BTreeSet<String>) -> Option<String> {
    let mut source_families = source_families.into_iter();
    let source_family = source_families.next()?;
    source_families.next().is_none().then_some(source_family)
}

fn resolver_profile_source_family_for_event_source(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1 | SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => {
            Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
        }
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY | SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => {
            Some(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER)
        }
        _ => None,
    }
}

pub(super) async fn load_current_bindings(
    pool: &PgPool,
    target: &ResolverTarget,
) -> Result<Vec<CurrentBindingSeed>> {
    let rows = sqlx::query(&format!(
        r#"
        WITH candidate_pairs AS (
            SELECT DISTINCT
                ne.logical_name_id,
                ne.resource_id
            FROM normalized_events ne
            WHERE ne.event_kind = $1
              AND ne.logical_name_id IS NOT NULL
              AND ne.resource_id IS NOT NULL
              AND ne.chain_id = $2
              AND ne.after_state->>'resolver' IS NOT NULL
              AND ne.after_state->>'resolver' <> ''
              AND LOWER(ne.after_state->>'resolver') = $3
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ),
        current_bindings AS (
            SELECT
                sb.logical_name_id,
                sb.resource_id,
                sb.surface_binding_id,
                sb.binding_kind,
                ns.canonical_display_name,
                ns.normalized_name,
                ns.namehash
            FROM surface_bindings sb
            INNER JOIN candidate_pairs candidate
              ON candidate.logical_name_id = sb.logical_name_id
             AND candidate.resource_id = sb.resource_id
            INNER JOIN name_surfaces ns
              ON ns.logical_name_id = sb.logical_name_id
             AND ns.canonicality_state {CANONICAL_STATE_FILTER}
            WHERE sb.active_to IS NULL
              AND sb.canonicality_state {CANONICAL_STATE_FILTER}
        ),
        latest_resolver_events AS (
            SELECT DISTINCT ON (ne.logical_name_id, ne.resource_id)
                ne.logical_name_id,
                ne.resource_id,
                ne.normalized_event_id,
                ne.source_family,
                ne.manifest_version,
                ne.source_manifest_id,
                ne.chain_id,
                ne.block_number,
                ne.block_hash,
                rb.block_timestamp,
                ne.raw_fact_ref,
                ne.canonicality_state::TEXT AS canonicality_state,
                LOWER(ne.after_state->>'resolver') AS resolver_address
            FROM normalized_events ne
            INNER JOIN candidate_pairs candidate
              ON candidate.logical_name_id = ne.logical_name_id
             AND candidate.resource_id = ne.resource_id
            LEFT JOIN chain_lineage rb
              ON rb.chain_id = ne.chain_id
             AND rb.block_hash = ne.block_hash
            WHERE ne.event_kind = $1
              AND ne.logical_name_id IS NOT NULL
              AND ne.resource_id IS NOT NULL
              AND ne.chain_id = $2
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
            ORDER BY
                ne.logical_name_id,
                ne.resource_id,
                ne.block_number DESC NULLS LAST,
                ne.log_index DESC NULLS LAST,
                ne.normalized_event_id DESC
        )
        SELECT
            cb.logical_name_id,
            cb.canonical_display_name,
            cb.normalized_name,
            cb.namehash,
            cb.resource_id,
            cb.surface_binding_id,
            cb.binding_kind,
            lre.normalized_event_id,
            lre.source_family,
            lre.manifest_version,
            lre.source_manifest_id,
            lre.chain_id,
            lre.block_number,
            lre.block_hash,
            lre.block_timestamp,
            lre.raw_fact_ref,
            lre.canonicality_state
        FROM current_bindings cb
        INNER JOIN latest_resolver_events lre
          ON lre.logical_name_id = cb.logical_name_id
         AND lre.resource_id = cb.resource_id
        WHERE lre.resolver_address = $3
        ORDER BY cb.canonical_display_name, cb.logical_name_id, cb.surface_binding_id
        "#
    ))
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(&target.chain_id)
    .bind(&target.resolver_address)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load current bindings for resolver {} on chain {}",
            target.resolver_address, target.chain_id
        )
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(CurrentBindingSeed {
                chain_id: row.try_get("chain_id")?,
                logical_name_id: row.try_get("logical_name_id")?,
                canonical_display_name: row.try_get("canonical_display_name")?,
                normalized_name: row.try_get("normalized_name")?,
                namehash: row.try_get("namehash")?,
                resource_id: row.try_get("resource_id")?,
                surface_binding_id: row.try_get("surface_binding_id")?,
                binding_kind: parse_surface_binding_kind(
                    &row.try_get::<String, _>("binding_kind")?,
                )?,
                normalized_event_id: row.try_get("normalized_event_id")?,
                source_family: row.try_get("source_family")?,
                manifest_version: row.try_get("manifest_version")?,
                source_manifest_id: row.try_get("source_manifest_id")?,
                block_number: row.try_get("block_number")?,
                block_hash: row.try_get("block_hash")?,
                block_timestamp: row.try_get("block_timestamp")?,
                raw_fact_ref: row.try_get("raw_fact_ref")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")?,
                )?,
            })
        })
        .collect()
}

pub(super) async fn load_resolver_permissions(
    pool: &PgPool,
    target: &ResolverTarget,
) -> Result<Vec<PermissionsCurrentRow>> {
    let mut rows = load_permissions_current_for_resolver_scope(
        pool,
        &target.chain_id,
        &target.resolver_address,
    )
    .await?;
    rows.sort_by(|left, right| {
        left.subject
            .cmp(&right.subject)
            .then_with(|| left.resource_id.cmp(&right.resource_id))
            .then_with(|| left.manifest_version.cmp(&right.manifest_version))
    });
    Ok(rows)
}

pub(super) async fn load_alias_events(
    pool: &PgPool,
    target: &ResolverTarget,
) -> Result<Vec<AliasSeed>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT ON (ne.after_state->>'from_dns_encoded_name')
            ne.normalized_event_id,
            ne.logical_name_id,
            ne.resource_id,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp,
            ne.raw_fact_ref,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.after_state,
            LOWER(ne.after_state->>'resolver') AS resolver_address
        FROM normalized_events ne
        LEFT JOIN chain_lineage rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE ne.event_kind = $1
          AND ne.chain_id = $2
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
          AND LOWER(ne.after_state->>'resolver') = $3
        ORDER BY
            ne.after_state->>'from_dns_encoded_name',
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        "#
    ))
    .bind(EVENT_KIND_ALIAS_CHANGED)
    .bind(&target.chain_id)
    .bind(&target.resolver_address)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load AliasChanged events for resolver {} on chain {}",
            target.resolver_address, target.chain_id
        )
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(AliasSeed {
                chain_id: row.try_get("chain_id")?,
                resolver_address: normalize_resolver_address(
                    &row.try_get::<String, _>("resolver_address")?,
                ),
                normalized_event_id: row.try_get("normalized_event_id")?,
                logical_name_id: row.try_get("logical_name_id")?,
                resource_id: row.try_get("resource_id")?,
                source_family: row.try_get("source_family")?,
                manifest_version: row.try_get("manifest_version")?,
                source_manifest_id: row.try_get("source_manifest_id")?,
                block_number: row.try_get("block_number")?,
                block_hash: row.try_get("block_hash")?,
                block_timestamp: row.try_get("block_timestamp")?,
                raw_fact_ref: row.try_get("raw_fact_ref")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")?,
                )?,
                after_state: row.try_get("after_state")?,
            })
        })
        .collect()
}

pub(super) fn normalize_resolver_address(value: &str) -> String {
    normalize_evm_address(value)
}

fn parse_surface_binding_kind(value: &str) -> Result<SurfaceBindingKind> {
    SurfaceBindingKind::parse(value)
}
