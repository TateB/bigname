use std::collections::{BTreeMap, HashMap};

use crate::adapter_manifest::load_required_active_manifest_event_topic0s_by_signature;
use crate::ens_v2_common::ActiveEmitter;
use crate::normalized_event_support::upsert_normalized_events_with_counts;
use anyhow::{Context, Result};
use bigname_storage::{Resource, upsert_resources};
use sqlx::{PgPool, types::Uuid};

mod constants;
mod decode;
mod hints;
mod load;
mod normalized;
#[cfg(test)]
mod tests;
mod types;
mod util;

use decode::build_permissions_observation;
use hints::{fallback_resource_hint, resolver_resource_hint};
use load::{load_active_emitters, load_permissions_raw_logs};
use normalized::{build_resource, permission_changed_event, remember_hint_and_resource};
use types::{PermissionsObservation, ResolverResourceHint};
use util::resource_is_root;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2PermissionsSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_resource_count: usize,
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, EnsV2PermissionsKindSyncSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2PermissionsKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

impl EnsV2PermissionsSyncSummary {
    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v2_permissions_with_scope(pool, chain, true, block_hashes, None, None).await
    }

    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v2_permissions_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            None,
        )
        .await
    }
}

pub async fn sync_ens_v2_permissions(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV2PermissionsSyncSummary> {
    sync_ens_v2_permissions_with_scope(pool, chain, false, &[], None, None).await
}

pub async fn sync_ens_v2_permissions_through_block(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
) -> Result<EnsV2PermissionsSyncSummary> {
    sync_ens_v2_permissions_with_scope(pool, chain, false, &[], None, Some(target_block_number))
        .await
}

async fn sync_ens_v2_permissions_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    max_block_number: Option<i64>,
) -> Result<EnsV2PermissionsSyncSummary> {
    let mut active_emitters = load_active_emitters(pool, chain).await?;
    if let Some(source_scope) = source_scope {
        active_emitters.retain(|emitter| permissions_scope_includes_emitter(source_scope, emitter));
    }
    if active_emitters.is_empty() {
        return Ok(empty_summary(0));
    }
    let manifest_ids = active_emitters
        .iter()
        .map(|emitter| emitter.source_manifest_id)
        .collect::<Vec<_>>();
    let event_topics = load_required_active_manifest_event_topic0s_by_signature(
        pool,
        &manifest_ids,
        &constants::ABI_EVENT_SIGNATURES,
        "ENSv2 permissions",
    )
    .await?;

    let raw_logs = load_permissions_raw_logs(
        pool,
        chain,
        &active_emitters,
        restrict_to_block_hashes,
        block_hashes,
        source_scope,
        max_block_number,
    )
    .await?;
    let scanned_log_count = raw_logs.len();
    if raw_logs.is_empty() {
        return Ok(empty_summary(scanned_log_count));
    }

    let mut matched_log_count = 0usize;
    let mut hints = HashMap::<(String, String), ResolverResourceHint>::new();
    let mut resources = BTreeMap::<Uuid, (Resource, ResolverResourceHint)>::new();
    let mut events = Vec::new();

    for raw_log in &raw_logs {
        let Some(observation) = build_permissions_observation(raw_log, &event_topics)? else {
            continue;
        };
        matched_log_count += 1;
        match observation {
            PermissionsObservation::NamedResource { resource, name } => {
                let hint = resolver_resource_hint(raw_log, resource, name, "name", None, None)?;
                remember_hint_and_resource(pool, raw_log, hint, &mut hints, &mut resources).await?;
            }
            PermissionsObservation::NamedTextResource {
                resource,
                name,
                key_hash,
                key,
            } => {
                let hint = resolver_resource_hint(
                    raw_log,
                    resource,
                    name,
                    "text",
                    Some(key),
                    Some(key_hash),
                )?;
                remember_hint_and_resource(pool, raw_log, hint, &mut hints, &mut resources).await?;
            }
            PermissionsObservation::NamedAddrResource {
                resource,
                name,
                coin_type,
            } => {
                let hint =
                    resolver_resource_hint(raw_log, resource, name, "addr", Some(coin_type), None)?;
                remember_hint_and_resource(pool, raw_log, hint, &mut hints, &mut resources).await?;
            }
            PermissionsObservation::EacRolesChanged {
                resource,
                account,
                old_role_bitmap,
                new_role_bitmap,
            } => {
                let key = (raw_log.emitting_address.clone(), resource.clone());
                let hint = hints.get(&key).cloned().unwrap_or_else(|| {
                    fallback_resource_hint(raw_log, resource.clone(), resource_is_root(&resource))
                });
                let resource_row =
                    build_resource(pool, raw_log, &hint)
                        .await
                        .with_context(|| {
                            format!(
                                "failed to build ENSv2 resolver permission resource {}",
                                hint.upstream_resource
                            )
                        })?;
                let resource_id = resource_row.resource_id;
                resources
                    .entry(resource_id)
                    .or_insert((resource_row, hint.clone()));
                events.push(permission_changed_event(
                    raw_log,
                    &hint,
                    resource_id,
                    account,
                    old_role_bitmap,
                    new_role_bitmap,
                )?);
            }
        }
    }

    let resources = resources
        .into_values()
        .map(|(resource, _)| resource)
        .collect::<Vec<_>>();
    upsert_resources(pool, &resources).await?;
    let counts = upsert_normalized_events_with_counts(pool, &events, "ENSv2 permissions").await?;
    let (total_synced_count, total_inserted_count, by_kind) = counts.into_parts_by_kind(
        |synced_count, inserted_count| EnsV2PermissionsKindSyncSummary {
            synced_count,
            inserted_count,
        },
    );

    Ok(EnsV2PermissionsSyncSummary {
        scanned_log_count,
        matched_log_count,
        total_resource_count: resources.len(),
        total_synced_count,
        total_inserted_count,
        by_kind,
    })
}

fn permissions_scope_includes_emitter(
    source_scope: &[(String, String, i64, i64)],
    emitter: &ActiveEmitter,
) -> bool {
    source_scope
        .iter()
        .any(|(source_family, address, from_block, to_block)| {
            source_family == &emitter.source_family
                && address.eq_ignore_ascii_case(&emitter.address)
                && from_block <= to_block
        })
}

fn empty_summary(scanned_log_count: usize) -> EnsV2PermissionsSyncSummary {
    EnsV2PermissionsSyncSummary {
        scanned_log_count,
        matched_log_count: 0,
        total_resource_count: 0,
        total_synced_count: 0,
        total_inserted_count: 0,
        by_kind: BTreeMap::new(),
    }
}
