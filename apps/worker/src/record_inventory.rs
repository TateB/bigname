use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use std::str::FromStr;

use anyhow::{Context, Result};
use bigname_storage::{
    CanonicalityState, RecordInventoryCurrentRow, clear_record_inventory_current,
    upsert_record_inventory_current_rows,
};
use serde_json::{Value, json};
#[cfg(test)]
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{
    PgPool, Row,
    types::time::{OffsetDateTime, UtcOffset},
};
use uuid::Uuid;

const EVENT_KIND_RECORD_CHANGED: &str = "RecordChanged";
const EVENT_KIND_RECORD_VERSION_CHANGED: &str = "RecordVersionChanged";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
const DERIVATION_KIND_DECLARED_AUTHORITY: &str = "ens_v1_unwrapped_authority";
const DERIVATION_KIND_ENS_V2_RESOLVER: &str = "ens_v2_resolver";
const ENS_NAMESPACE: &str = "ens";
const BASENAMES_NAMESPACE: &str = "basenames";
const SOURCE_FAMILY_ENS_V1_REGISTRY_L1: &str = "ens_v1_registry_l1";
const SOURCE_FAMILY_ENS_V1_RESOLVER_L1: &str = "ens_v1_resolver_l1";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";
const ETHEREUM_MAINNET_CHAIN_ID: &str = "ethereum-mainnet";
const BASE_MAINNET_CHAIN_ID: &str = "base-mainnet";
const ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE: &str = "public_resolver_compatible";
const BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE: &str = "l2_resolver_compatible";
const RECORD_INVENTORY_CURRENT_DERIVATION_KIND: &str = "record_inventory_current_rebuild";
const RECORD_INVENTORY_ENUMERATION_BASIS: &str = "declared_record_inventory";
const GAP_REASON_NOT_OBSERVED: &str = "not_observed_on_current_resolver";
const CACHE_UNSUPPORTED_REASON_VALUE_NOT_RETAINED: &str = "value_not_retained_in_normalized_events";
const UNSUPPORTED_FAMILY_REASON: &str = "record_family_not_supported_in_phase6_projection";
const RESOLVER_FAMILY_PENDING_REASON: &str = "resolver_family_pending";
const SUPPORTED_TEXT_RECORD_KEY: &str = "text";
const SUPPORTED_TEXT_RECORD_FAMILY: &str = "text";
const SUPPORTED_ADDR_RECORD_FAMILY: &str = "addr";
const UNSUPPORTED_CONTENTHASH_RECORD_KEY: &str = "contenthash";
const UNSUPPORTED_CONTENTHASH_RECORD_FAMILY: &str = "contenthash";
const SUPPORTED_NATIVE_ADDR_SELECTOR_KEY: &str = "60";
const RESOLVER_PROFILE_FACT_FAMILY_RECORD: &str = "resolver_record";
const RESOLVER_PROFILE_FACT_FAMILY_RECORD_VERSION: &str = "resolver_record_version";
const RESOLVER_PROFILE_STATUS_PENDING: &str = "pending";
const RESOLVER_PROFILE_STATUS_SUPPORTED: &str = "supported";
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecordInventoryCurrentRebuildSummary {
    pub requested_resource_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

#[derive(Clone, Debug)]
struct RelevantEvent {
    normalized_event_id: i64,
    logical_name_id: String,
    resource_id: Uuid,
    event_kind: String,
    source_family: String,
    manifest_version: i64,
    source_manifest_id: Option<i64>,
    chain_id: String,
    block_number: i64,
    block_hash: String,
    block_timestamp: Option<OffsetDateTime>,
    raw_fact_ref: Value,
    canonicality_state: CanonicalityState,
    after_state: Value,
    emitting_address: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct RecordSelector {
    record_key: String,
    record_family: String,
    selector_key: Option<String>,
}

#[derive(Clone, Debug)]
struct ChainPositionCandidate {
    slot: String,
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: String,
}

#[derive(Clone, Debug)]
struct ResolverProfileGate {
    admissions: BTreeMap<(String, String, String, String), String>,
}

impl ResolverProfileGate {
    async fn load(pool: &PgPool) -> Result<Self> {
        let mut admissions =
            bigname_manifests::load_ens_v1_public_resolver_profile_admissions(pool)
                .await
                .context("failed to load ENSv1 PublicResolver profile admissions")?
                .into_iter()
                .collect::<Vec<_>>();
        admissions.extend(
            bigname_manifests::load_basenames_l2_resolver_profile_admissions(pool)
                .await
                .context("failed to load Basenames L2Resolver profile admissions")?,
        );

        let admissions = admissions
            .into_iter()
            .filter(|admission| {
                resolver_profile_for_source_family(&admission.source_family)
                    .is_some_and(|profile| admission.profile == profile)
            })
            .map(|admission| {
                (
                    (
                        admission.chain,
                        admission.source_family,
                        normalize_address(&admission.address),
                        admission.fact_family,
                    ),
                    admission.status,
                )
            })
            .collect();

        Ok(Self { admissions })
    }

    fn status_for(
        &self,
        chain_id: &str,
        source_family: &str,
        resolver_address: &str,
        fact_family: &str,
    ) -> Option<&str> {
        self.admissions
            .get(&(
                chain_id.to_owned(),
                source_family.to_owned(),
                normalize_address(resolver_address),
                fact_family.to_owned(),
            ))
            .map(String::as_str)
    }

    fn allows_event(&self, event: &RelevantEvent) -> bool {
        let Some(source_family) = resolver_local_source_family(&event.source_family) else {
            return true;
        };

        let Some(fact_family) = resolver_fact_family_for_event(source_family, &event.event_kind)
        else {
            return true;
        };
        let Some(emitting_address) = event.emitting_address.as_deref() else {
            return false;
        };

        self.status_for(
            &event.chain_id,
            source_family,
            emitting_address,
            fact_family,
        ) == Some(RESOLVER_PROFILE_STATUS_SUPPORTED)
    }

    fn current_record_status(&self, event: &RelevantEvent) -> Option<&str> {
        if event.event_kind != EVENT_KIND_RESOLVER_CHANGED {
            return None;
        }

        let source_family = resolver_source_family_for_resolver_event(&event.source_family)?;
        let resolver_address = resolver_address_from_event(event)?;
        Some(
            self.status_for(
                &event.chain_id,
                source_family,
                &resolver_address,
                RESOLVER_PROFILE_FACT_FAMILY_RECORD,
            )
            .unwrap_or(RESOLVER_PROFILE_STATUS_PENDING),
        )
    }
}

pub async fn rebuild_record_inventory_current(
    pool: &PgPool,
    resource_id: Option<&str>,
) -> Result<RecordInventoryCurrentRebuildSummary> {
    match resource_id {
        Some(resource_id) => rebuild_one_resource(pool, resource_id).await,
        None => rebuild_all_resources(pool).await,
    }
}

async fn rebuild_all_resources(pool: &PgPool) -> Result<RecordInventoryCurrentRebuildSummary> {
    let profile_gate = ResolverProfileGate::load(pool).await?;
    let resource_ids = load_target_resource_ids(pool).await?;

    let mut rows = Vec::with_capacity(resource_ids.len());
    for resource_id in &resource_ids {
        if let Some(row) = build_row(pool, &profile_gate, *resource_id).await? {
            rows.push(row);
        }
    }

    let upserted_row_count = upsert_record_inventory_current_rows(pool, &rows)
        .await?
        .len();
    let deleted_row_count = delete_stale_record_inventory_current_rows(pool, &rows).await?;
    Ok(RecordInventoryCurrentRebuildSummary {
        requested_resource_count: resource_ids.len(),
        upserted_row_count,
        deleted_row_count,
    })
}

async fn rebuild_one_resource(
    pool: &PgPool,
    resource_id: &str,
) -> Result<RecordInventoryCurrentRebuildSummary> {
    let profile_gate = ResolverProfileGate::load(pool).await?;
    let resource_id = Uuid::parse_str(resource_id)
        .with_context(|| format!("resource_id must be a UUID: {resource_id}"))?;
    let Some(row) = build_row(pool, &profile_gate, resource_id).await? else {
        let deleted_row_count =
            delete_record_inventory_rows_for_resource(pool, resource_id).await?;
        return Ok(RecordInventoryCurrentRebuildSummary {
            requested_resource_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    };

    let upserted_row_count = upsert_record_inventory_current_rows(pool, std::slice::from_ref(&row))
        .await?
        .len();
    let deleted_row_count =
        delete_stale_record_inventory_current_rows_for_resource(pool, resource_id, &row).await?;
    Ok(RecordInventoryCurrentRebuildSummary {
        requested_resource_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn delete_record_inventory_rows_for_resource(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM record_inventory_current
        WHERE resource_id = $1
        "#,
    )
    .bind(resource_id)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete record_inventory_current rows for resource_id {resource_id}")
    })
    .map(|result| result.rows_affected())
}

async fn delete_stale_record_inventory_current_rows(
    pool: &PgPool,
    rows: &[RecordInventoryCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return clear_record_inventory_current(pool).await;
    }

    let resource_ids = rows.iter().map(|row| row.resource_id).collect::<Vec<_>>();
    let record_version_boundaries = rows
        .iter()
        .map(|row| {
            serde_json::to_string(&row.record_version_boundary)
                .context("failed to serialize record_inventory_current boundary for cleanup")
        })
        .collect::<Result<Vec<_>>>()?;

    sqlx::query(
        r#"
        DELETE FROM record_inventory_current current
        WHERE NOT EXISTS (
            SELECT 1
            FROM UNNEST($1::UUID[], $2::TEXT[]) AS replacement(
                resource_id,
                record_version_boundary
            )
            WHERE replacement.resource_id = current.resource_id
              AND replacement.record_version_boundary::JSONB = current.record_version_boundary
        )
        "#,
    )
    .bind(&resource_ids)
    .bind(&record_version_boundaries)
    .execute(pool)
    .await
    .context("failed to delete stale record_inventory_current rows after rebuild")
    .map(|result| result.rows_affected())
}

async fn delete_stale_record_inventory_current_rows_for_resource(
    pool: &PgPool,
    resource_id: Uuid,
    row: &RecordInventoryCurrentRow,
) -> Result<u64> {
    let record_version_boundary = serde_json::to_string(&row.record_version_boundary)
        .context("failed to serialize record_inventory_current boundary for cleanup")?;

    sqlx::query(
        r#"
        DELETE FROM record_inventory_current current
        WHERE current.resource_id = $1
          AND current.record_version_boundary <> $2::JSONB
        "#,
    )
    .bind(resource_id)
    .bind(record_version_boundary)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to delete stale record_inventory_current rows for resource_id {resource_id}"
        )
    })
    .map(|result| result.rows_affected())
}

async fn load_target_resource_ids(pool: &PgPool) -> Result<Vec<Uuid>> {
    let derivation_kinds = record_inventory_derivation_kinds();
    let resolver_event_namespaces = resolver_event_namespaces();
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT resource_id
        FROM normalized_events
        WHERE derivation_kind = ANY($1::TEXT[])
          AND event_kind IN ($2, $3, $4)
          AND (event_kind <> $4 OR namespace = ANY($5::TEXT[]))
          AND resource_id IS NOT NULL
          AND canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY resource_id
        "#
    ))
    .bind(&derivation_kinds)
    .bind(EVENT_KIND_RECORD_CHANGED)
    .bind(EVENT_KIND_RECORD_VERSION_CHANGED)
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(&resolver_event_namespaces)
    .fetch_all(pool)
    .await
    .context("failed to load record_inventory_current rebuild targets")?;

    rows.into_iter()
        .map(|row| row.try_get("resource_id").context("missing resource_id"))
        .collect()
}

async fn build_row(
    pool: &PgPool,
    profile_gate: &ResolverProfileGate,
    resource_id: Uuid,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let events = load_relevant_events(pool, resource_id).await?;
    if events.is_empty() {
        return Ok(None);
    }

    let latest_resolver_event = events
        .iter()
        .rev()
        .find(|event| event.event_kind == EVENT_KIND_RESOLVER_CHANGED);
    if let Some(resolver_event) = latest_resolver_event
        && profile_gate
            .current_record_status(resolver_event)
            .is_some_and(|status| status != RESOLVER_PROFILE_STATUS_SUPPORTED)
    {
        return build_pending_profile_row(pool, resource_id, resolver_event).await;
    }

    let boundary_index = events.iter().rposition(|event| {
        event.event_kind == EVENT_KIND_RECORD_VERSION_CHANGED
            || event.event_kind == EVENT_KIND_RESOLVER_CHANGED
    });
    let scoped_events = &events[boundary_index.unwrap_or(0)..];
    let boundary_anchor = match boundary_index {
        Some(index) => events
            .get(index)
            .context("record_inventory_current rebuild boundary index out of range")?,
        None => events
            .last()
            .context("record_inventory_current rebuild requires at least one event")?,
    };
    let has_record_version_boundary_pointer =
        boundary_anchor.event_kind == EVENT_KIND_RECORD_VERSION_CHANGED;
    let record_version_boundary =
        build_record_version_boundary(boundary_anchor, has_record_version_boundary_pointer)?;
    let record_change_events = scoped_events
        .iter()
        .filter(|event| {
            event.event_kind == EVENT_KIND_RECORD_CHANGED && profile_gate.allows_event(event)
        })
        .collect::<Vec<_>>();
    let provenance_events = scoped_events
        .iter()
        .filter(|event| {
            event.event_kind == EVENT_KIND_RESOLVER_CHANGED
                || resolver_local_source_family(&event.source_family).is_none()
                || profile_gate.allows_event(event)
        })
        .cloned()
        .collect::<Vec<_>>();

    let selectors = build_selectors(&record_change_events)?;
    let explicit_gaps = build_explicit_gaps(&selectors);
    let unsupported_families = build_unsupported_families(&record_change_events)?;
    let entries = build_entries(&selectors);
    let last_change = provenance_events
        .last()
        .map(build_last_change)
        .transpose()?;
    let chain_position_events = collect_chain_position_events(boundary_anchor, &provenance_events);
    let supplemental_chain_positions =
        load_basenames_transport_chain_positions(pool, &chain_position_events).await?;

    Ok(Some(RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary,
        enumeration_basis: json!({
            "observed_selectors": true,
            "capability_declared_families": true,
            "globally_enumerable": false,
        }),
        selectors: Value::Array(
            selectors
                .into_values()
                .map(|selector| {
                    json!({
                        "record_key": selector.record_key,
                        "record_family": selector.record_family,
                        "selector_key": selector.selector_key,
                        "cacheable": true,
                    })
                })
                .collect(),
        ),
        explicit_gaps: Value::Array(explicit_gaps),
        unsupported_families: Value::Array(unsupported_families),
        last_change,
        entries: Value::Array(entries),
        provenance: build_provenance(&provenance_events)?,
        coverage: build_coverage(&provenance_events),
        chain_positions: build_chain_positions(
            &chain_position_events,
            supplemental_chain_positions,
        ),
        canonicality_summary: build_canonicality_summary(&provenance_events),
        manifest_version: provenance_events
            .iter()
            .map(|event| event.manifest_version)
            .max()
            .unwrap_or(1),
        last_recomputed_at: provenance_events
            .iter()
            .filter_map(|event| event.block_timestamp)
            .max()
            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
    }))
}

async fn load_relevant_events(pool: &PgPool, resource_id: Uuid) -> Result<Vec<RelevantEvent>> {
    let derivation_kinds = record_inventory_derivation_kinds();
    let resolver_event_namespaces = resolver_event_namespaces();
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.logical_name_id,
            ne.resource_id,
            ne.event_kind,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            ne.log_index,
            rb.block_timestamp,
            ne.raw_fact_ref,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.after_state,
            LOWER(rl.emitting_address) AS emitting_address
        FROM normalized_events ne
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        LEFT JOIN raw_logs rl
          ON rl.chain_id = ne.chain_id
         AND rl.block_hash = ne.block_hash
         AND rl.log_index = ne.log_index
        WHERE ne.derivation_kind = ANY($1::TEXT[])
          AND ne.event_kind IN ($2, $3, $4)
          AND (ne.event_kind <> $4 OR ne.namespace = ANY($5::TEXT[]))
          AND ne.resource_id = $6
          AND ne.logical_name_id IS NOT NULL
          AND ne.chain_id IS NOT NULL
          AND ne.block_number IS NOT NULL
          AND ne.block_hash IS NOT NULL
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY
            ne.block_number ASC,
            ne.log_index ASC NULLS FIRST,
            ne.normalized_event_id ASC
        "#
    ))
    .bind(&derivation_kinds)
    .bind(EVENT_KIND_RECORD_CHANGED)
    .bind(EVENT_KIND_RECORD_VERSION_CHANGED)
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(&resolver_event_namespaces)
    .bind(resource_id)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load record_inventory_current events for resource_id {resource_id}")
    })?;

    rows.into_iter().map(decode_relevant_event).collect()
}

fn record_inventory_derivation_kinds() -> Vec<String> {
    vec![
        DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
        DERIVATION_KIND_ENS_V2_RESOLVER.to_owned(),
    ]
}

fn resolver_event_namespaces() -> Vec<String> {
    vec![ENS_NAMESPACE.to_owned(), BASENAMES_NAMESPACE.to_owned()]
}

fn resolver_profile_for_source_family(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => Some(ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE),
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some(BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE),
        _ => None,
    }
}

fn resolver_source_family_for_resolver_event(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1 => Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1),
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY => Some(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER),
        _ => None,
    }
}

fn resolver_local_source_family(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1),
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER),
        _ => None,
    }
}

fn resolver_fact_family_for_event(source_family: &str, event_kind: &str) -> Option<&'static str> {
    match (source_family, event_kind) {
        (_, EVENT_KIND_RECORD_CHANGED) => Some(RESOLVER_PROFILE_FACT_FAMILY_RECORD),
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, EVENT_KIND_RECORD_VERSION_CHANGED) => {
            Some(RESOLVER_PROFILE_FACT_FAMILY_RECORD_VERSION)
        }
        (SOURCE_FAMILY_BASENAMES_BASE_RESOLVER, EVENT_KIND_RECORD_VERSION_CHANGED) => {
            Some(RESOLVER_PROFILE_FACT_FAMILY_RECORD)
        }
        _ => None,
    }
}

async fn load_basenames_transport_chain_positions(
    pool: &PgPool,
    events: &[RelevantEvent],
) -> Result<Vec<ChainPositionCandidate>> {
    let Some(base_boundary) = events.iter().rev().find(|event| {
        event
            .logical_name_id
            .split_once(':')
            .map(|(namespace, _)| namespace)
            == Some(BASENAMES_NAMESPACE)
            && event.chain_id == BASE_MAINNET_CHAIN_ID
    }) else {
        return Ok(Vec::new());
    };

    let Some(upper_bound) = base_boundary.block_timestamp else {
        return Ok(Vec::new());
    };

    let row = sqlx::query(&format!(
        r#"
        SELECT
            chain_id,
            block_number,
            block_hash,
            block_timestamp
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_timestamp <= $2
          AND canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY block_timestamp DESC, block_number DESC, block_hash DESC
        LIMIT 1
        "#
    ))
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(upper_bound)
    .fetch_optional(pool)
    .await
    .context("failed to load Basenames Ethereum transport chain position")?;

    row.map(|row| {
        let chain_id = row
            .try_get::<String, _>("chain_id")
            .context("missing Basenames transport chain_id")?;
        let timestamp = row
            .try_get::<OffsetDateTime, _>("block_timestamp")
            .context("missing Basenames transport block_timestamp")?;
        Ok(ChainPositionCandidate {
            slot: chain_slot(&chain_id),
            chain_id,
            block_number: row
                .try_get("block_number")
                .context("missing Basenames transport block_number")?,
            block_hash: row
                .try_get("block_hash")
                .context("missing Basenames transport block_hash")?,
            timestamp: format_timestamp(timestamp),
        })
    })
    .transpose()
    .map(|candidate| candidate.into_iter().collect())
}

fn decode_relevant_event(row: sqlx::postgres::PgRow) -> Result<RelevantEvent> {
    Ok(RelevantEvent {
        normalized_event_id: row.try_get("normalized_event_id")?,
        logical_name_id: row
            .try_get::<Option<String>, _>("logical_name_id")?
            .context("record event must include logical_name_id")?,
        resource_id: row
            .try_get::<Option<Uuid>, _>("resource_id")?
            .context("record event must include resource_id")?,
        event_kind: row.try_get("event_kind")?,
        source_family: row.try_get("source_family")?,
        manifest_version: row.try_get("manifest_version")?,
        source_manifest_id: row.try_get("source_manifest_id")?,
        chain_id: row
            .try_get::<Option<String>, _>("chain_id")?
            .context("record event must include chain_id")?,
        block_number: row
            .try_get::<Option<i64>, _>("block_number")?
            .context("record event must include block_number")?,
        block_hash: row
            .try_get::<Option<String>, _>("block_hash")?
            .context("record event must include block_hash")?,
        block_timestamp: row.try_get("block_timestamp")?,
        raw_fact_ref: row.try_get("raw_fact_ref")?,
        canonicality_state: parse_canonicality_state(
            &row.try_get::<String, _>("canonicality_state")?,
        )?,
        after_state: row.try_get("after_state")?,
        emitting_address: row.try_get("emitting_address")?,
    })
}

async fn build_pending_profile_row(
    pool: &PgPool,
    resource_id: Uuid,
    resolver_event: &RelevantEvent,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let supplemental_chain_positions =
        load_basenames_transport_chain_positions(pool, std::slice::from_ref(resolver_event))
            .await?;

    Ok(Some(RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: build_record_version_boundary(resolver_event, false)?,
        enumeration_basis: json!({
            "observed_selectors": false,
            "capability_declared_families": true,
            "globally_enumerable": false,
        }),
        selectors: Value::Array(vec![]),
        explicit_gaps: Value::Array(vec![gap_value(
            UNSUPPORTED_CONTENTHASH_RECORD_KEY,
            UNSUPPORTED_CONTENTHASH_RECORD_FAMILY,
            None,
        )]),
        unsupported_families: Value::Array(vec![
            resolver_family_pending_value(SUPPORTED_ADDR_RECORD_FAMILY),
            resolver_family_pending_value(SUPPORTED_TEXT_RECORD_FAMILY),
        ]),
        last_change: Some(build_last_change(resolver_event)?),
        entries: Value::Array(vec![]),
        provenance: build_provenance(std::slice::from_ref(resolver_event))?,
        coverage: json!({
            "status": "partial",
            "exhaustiveness": "best_effort",
            "source_classes_considered": [resolver_event.source_family],
            "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            "enumeration_basis": RECORD_INVENTORY_ENUMERATION_BASIS,
        }),
        chain_positions: build_chain_positions(
            std::slice::from_ref(resolver_event),
            supplemental_chain_positions,
        ),
        canonicality_summary: build_canonicality_summary(std::slice::from_ref(resolver_event)),
        manifest_version: resolver_event.manifest_version,
        last_recomputed_at: resolver_event
            .block_timestamp
            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
    }))
}

fn collect_chain_position_events(
    boundary_anchor: &RelevantEvent,
    provenance_events: &[RelevantEvent],
) -> Vec<RelevantEvent> {
    let mut events = provenance_events.to_vec();
    if !events
        .iter()
        .any(|event| event.normalized_event_id == boundary_anchor.normalized_event_id)
    {
        events.push(boundary_anchor.clone());
    }
    events
}

fn build_record_version_boundary(
    event: &RelevantEvent,
    has_boundary_pointer: bool,
) -> Result<Value> {
    Ok(json!({
        "logical_name_id": event.logical_name_id,
        "resource_id": event.resource_id,
        "normalized_event_id": has_boundary_pointer.then_some(event.normalized_event_id),
        "event_kind": has_boundary_pointer.then_some(event.event_kind.clone()),
        "chain_position": chain_position_value(event)?,
    }))
}

fn build_selectors(
    record_change_events: &[&RelevantEvent],
) -> Result<BTreeMap<String, RecordSelector>> {
    let mut selectors = BTreeMap::new();

    for event in record_change_events {
        let selector = parse_record_selector(event)?;
        if is_supported_selector(&selector) {
            selectors.insert(selector.record_key.clone(), selector);
        }
    }

    Ok(selectors)
}

fn build_explicit_gaps(selectors: &BTreeMap<String, RecordSelector>) -> Vec<Value> {
    let mut gaps = Vec::new();
    let has_text = selectors.contains_key(SUPPORTED_TEXT_RECORD_KEY);
    let has_native_addr = selectors.contains_key(&supported_native_addr_record_key());

    if !has_native_addr {
        gaps.push(gap_value(
            &supported_native_addr_record_key(),
            SUPPORTED_ADDR_RECORD_FAMILY,
            Some(SUPPORTED_NATIVE_ADDR_SELECTOR_KEY),
        ));
    }
    if !has_text {
        gaps.push(gap_value(
            SUPPORTED_TEXT_RECORD_KEY,
            SUPPORTED_TEXT_RECORD_FAMILY,
            None,
        ));
    }

    gaps.sort_by(|left, right| {
        left["record_key"]
            .as_str()
            .cmp(&right["record_key"].as_str())
    });
    gaps
}

fn build_unsupported_families(record_change_events: &[&RelevantEvent]) -> Result<Vec<Value>> {
    let mut families = BTreeSet::new();

    for event in record_change_events {
        let selector = parse_record_selector(event)?;
        if !is_supported_selector(&selector) {
            families.insert(selector.record_family);
        }
    }

    Ok(families
        .into_iter()
        .map(|record_family| {
            json!({
                "record_family": record_family,
                "unsupported_reason": UNSUPPORTED_FAMILY_REASON,
            })
        })
        .collect())
}

fn build_entries(selectors: &BTreeMap<String, RecordSelector>) -> Vec<Value> {
    let mut entries = selectors
        .values()
        .map(|selector| {
            json!({
                "record_key": selector.record_key,
                "record_family": selector.record_family,
                "selector_key": selector.selector_key,
                "status": "unsupported",
                "unsupported_reason": CACHE_UNSUPPORTED_REASON_VALUE_NOT_RETAINED,
            })
        })
        .collect::<Vec<_>>();

    entries.sort_by(|left, right| {
        left["record_key"]
            .as_str()
            .cmp(&right["record_key"].as_str())
    });
    entries
}

fn build_last_change(event: &RelevantEvent) -> Result<Value> {
    Ok(json!({
        "normalized_event_id": event.normalized_event_id,
        "event_kind": event.event_kind,
        "chain_position": chain_position_value(event)?,
    }))
}

fn gap_value(record_key: &str, record_family: &str, selector_key: Option<&str>) -> Value {
    json!({
        "record_key": record_key,
        "record_family": record_family,
        "selector_key": selector_key,
        "gap_reason": GAP_REASON_NOT_OBSERVED,
    })
}

fn resolver_family_pending_value(record_family: &str) -> Value {
    json!({
        "record_family": record_family,
        "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
    })
}

fn resolver_address_from_event(event: &RelevantEvent) -> Option<String> {
    event
        .after_state
        .get("resolver")
        .and_then(Value::as_str)
        .map(normalize_address)
}

fn is_supported_selector(selector: &RecordSelector) -> bool {
    match selector.record_family.as_str() {
        SUPPORTED_TEXT_RECORD_FAMILY => {
            selector.record_key == SUPPORTED_TEXT_RECORD_KEY && selector.selector_key.is_none()
        }
        SUPPORTED_ADDR_RECORD_FAMILY => selector
            .selector_key
            .as_ref()
            .is_some_and(|selector_key| selector.record_key == format!("addr:{selector_key}")),
        _ => false,
    }
}

fn parse_record_selector(event: &RelevantEvent) -> Result<RecordSelector> {
    let object = event
        .after_state
        .as_object()
        .context("record event after_state must be an object")?;
    let record_key = object
        .get("record_key")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("record event after_state.record_key must be a non-empty string")?
        .to_owned();
    let record_family = object
        .get("record_family")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("record event after_state.record_family must be a non-empty string")?
        .to_owned();
    let selector_key = match object.get("selector_key") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
        Some(_) => {
            anyhow::bail!(
                "record event after_state.selector_key must be null or a non-empty string"
            )
        }
    };

    let expected_record_key = selector_key
        .as_ref()
        .map(|selector_key| format!("{record_family}:{selector_key}"))
        .unwrap_or_else(|| record_family.clone());
    if record_key != expected_record_key {
        anyhow::bail!(
            "record event selector identity mismatch: record_key {} must match {}",
            record_key,
            expected_record_key
        );
    }

    Ok(RecordSelector {
        record_key,
        record_family,
        selector_key,
    })
}

fn chain_position_value(event: &RelevantEvent) -> Result<Value> {
    let timestamp = event
        .block_timestamp
        .context("record event must have a raw_blocks timestamp for chain_position")?;
    Ok(json!({
        "chain_id": event.chain_id,
        "block_number": event.block_number,
        "block_hash": event.block_hash,
        "timestamp": format_timestamp(timestamp),
    }))
}

fn build_provenance(events: &[RelevantEvent]) -> Result<Value> {
    let normalized_event_ids = events
        .iter()
        .map(|event| Value::Number(event.normalized_event_id.into()))
        .collect::<Vec<_>>();
    let raw_fact_refs = dedupe_json_values(events.iter().map(|event| event.raw_fact_ref.clone()))?;
    let manifest_versions = dedupe_json_values(events.iter().map(|event| {
        json!({
            "source_manifest_id": event.source_manifest_id,
            "source_family": event.source_family,
            "manifest_version": event.manifest_version,
        })
    }))?;

    Ok(json!({
        "normalized_event_ids": dedupe_json_values(normalized_event_ids)?,
        "raw_fact_refs": raw_fact_refs,
        "manifest_versions": manifest_versions,
        "execution_trace_id": Value::Null,
        "derivation_kind": RECORD_INVENTORY_CURRENT_DERIVATION_KIND,
    }))
}

fn build_coverage(events: &[RelevantEvent]) -> Value {
    let source_classes_considered = events
        .iter()
        .map(|event| event.source_family.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(Value::String)
        .collect::<Vec<_>>();

    json!({
        "status": "full",
        "exhaustiveness": "authoritative",
        "source_classes_considered": source_classes_considered,
        "unsupported_reason": Value::Null,
        "enumeration_basis": RECORD_INVENTORY_ENUMERATION_BASIS,
    })
}

fn build_chain_positions(
    events: &[RelevantEvent],
    supplemental_candidates: Vec<ChainPositionCandidate>,
) -> Value {
    let mut chain_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    for event in events {
        let Some(timestamp) = event.block_timestamp else {
            continue;
        };
        let candidate = ChainPositionCandidate {
            slot: chain_slot(&event.chain_id),
            chain_id: event.chain_id.clone(),
            block_number: event.block_number,
            block_hash: event.block_hash.clone(),
            timestamp: format_timestamp(timestamp),
        };

        push_chain_position_candidate(&mut chain_positions, candidate);
    }

    for candidate in supplemental_candidates {
        push_chain_position_candidate(&mut chain_positions, candidate);
    }

    json!(
        chain_positions
            .into_iter()
            .map(|(slot, candidate)| {
                (
                    slot,
                    json!({
                        "chain_id": candidate.chain_id,
                        "block_number": candidate.block_number,
                        "block_hash": candidate.block_hash,
                        "timestamp": candidate.timestamp,
                    }),
                )
            })
            .collect::<serde_json::Map<String, Value>>()
    )
}

fn push_chain_position_candidate(
    chain_positions: &mut BTreeMap<String, ChainPositionCandidate>,
    candidate: ChainPositionCandidate,
) {
    match chain_positions.get(&candidate.slot) {
        Some(existing)
            if existing.block_number > candidate.block_number
                || (existing.block_number == candidate.block_number
                    && existing.block_hash >= candidate.block_hash) => {}
        _ => {
            chain_positions.insert(candidate.slot.clone(), candidate);
        }
    }
}

fn chain_slot(chain_id: &str) -> String {
    match chain_id {
        ETHEREUM_MAINNET_CHAIN_ID => "ethereum".to_owned(),
        BASE_MAINNET_CHAIN_ID => "base".to_owned(),
        _ => chain_id.to_owned(),
    }
}

fn build_canonicality_summary(events: &[RelevantEvent]) -> Value {
    let status = weakest_canonicality(events.iter().map(|event| event.canonicality_state))
        .unwrap_or(CanonicalityState::Canonical);

    let mut chain_states = BTreeMap::<String, CanonicalityState>::new();
    for event in events {
        let replacement = chain_states
            .get(&event.chain_id)
            .map(|current| {
                canonicality_rank(event.canonicality_state) < canonicality_rank(*current)
            })
            .unwrap_or(true);
        if replacement {
            chain_states.insert(event.chain_id.clone(), event.canonicality_state);
        }
    }

    json!({
        "status": status.as_str(),
        "chains": chain_states
            .into_iter()
            .map(|(chain_id, state)| (chain_id, Value::String(state.as_str().to_owned())))
            .collect::<serde_json::Map<String, Value>>(),
    })
}

fn weakest_canonicality(
    states: impl IntoIterator<Item = CanonicalityState>,
) -> Option<CanonicalityState> {
    states
        .into_iter()
        .min_by_key(|state| canonicality_rank(*state))
}

fn canonicality_rank(state: CanonicalityState) -> u8 {
    match state {
        CanonicalityState::Canonical => 0,
        CanonicalityState::Safe => 1,
        CanonicalityState::Finalized => 2,
        CanonicalityState::Observed => 3,
        CanonicalityState::Orphaned => 4,
    }
}

fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    match value {
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "observed" => Ok(CanonicalityState::Observed),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => anyhow::bail!("unknown canonicality_state value {value}"),
    }
}

fn supported_native_addr_record_key() -> String {
    format!("{SUPPORTED_ADDR_RECORD_FAMILY}:{SUPPORTED_NATIVE_ADDR_SELECTOR_KEY}")
}

fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

fn dedupe_json_values(values: impl IntoIterator<Item = Value>) -> Result<Vec<Value>> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();

    for value in values {
        let key = serde_json::to_string(&value).context("failed to serialize JSON value")?;
        if seen.insert(key) {
            deduped.push(value);
        }
    }

    Ok(deduped)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use anyhow::Result;
    use bigname_storage::{
        ChainLineageBlock, NormalizedEvent, RawBlock, RawCodeHash, RawLog, Resource,
        default_database_url, load_record_inventory_current, upsert_chain_lineage_blocks,
        upsert_normalized_events, upsert_raw_blocks, upsert_raw_code_hashes, upsert_raw_logs,
        upsert_resources,
    };

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url).context(
                "failed to parse database URL for worker record_inventory_current tests",
            )?;
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bg_wr_{}_{}_{}",
                std::process::id(),
                sequence,
                &Uuid::new_v4().simple().to_string()[..8]
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context(
                    "failed to connect admin pool for worker record_inventory_current tests",
                )?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect worker record_inventory_current test pool")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for worker record_inventory_current tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn full_rebuild_projects_current_rows_for_all_target_resources() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_a = Uuid::from_u128(0x9100);
        let resource_b = Uuid::from_u128(0x9200);

        seed_resources(database.pool(), &[resource_a, resource_b]).await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xrec1000", 1000, 1_776_200_000),
                raw_block("ethereum-mainnet", "0xrec1001", 1001, 1_776_200_001),
                raw_block("ethereum-mainnet", "0xrec1002", 1002, 1_776_200_002),
                raw_block("ethereum-mainnet", "0xrec1003", 1003, 1_776_200_003),
            ],
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                record_version_changed_event(
                    "res-a-boundary",
                    "ens:alice.eth",
                    resource_a,
                    7,
                    1000,
                    0,
                ),
                record_changed_event(
                    "res-a-text",
                    "ens:alice.eth",
                    resource_a,
                    "text",
                    "text",
                    None,
                    1001,
                    0,
                ),
                record_version_changed_event(
                    "res-b-boundary",
                    "ens:bob.eth",
                    resource_b,
                    11,
                    1002,
                    0,
                ),
                record_changed_event(
                    "res-b-native-addr",
                    "ens:bob.eth",
                    resource_b,
                    "addr:60",
                    "addr",
                    Some("60"),
                    1003,
                    0,
                ),
            ],
        )
        .await?;

        let summary = rebuild_record_inventory_current(database.pool(), None).await?;
        assert_eq!(summary.requested_resource_count, 2);
        assert_eq!(summary.upserted_row_count, 2);
        assert_eq!(summary.deleted_row_count, 0);

        let row_a = load_record_inventory_current(
            database.pool(),
            resource_a,
            &record_version_boundary(
                "ens:alice.eth",
                resource_a,
                Some(1),
                Some(EVENT_KIND_RECORD_VERSION_CHANGED),
                1000,
                "0xrec1000",
                1_776_200_000,
                "ethereum-mainnet",
            ),
        )
        .await?
        .context("resource_a row must exist")?;
        assert_eq!(
            row_a.selectors,
            json!([{
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "cacheable": true,
            }])
        );

        let row_b = load_record_inventory_current(
            database.pool(),
            resource_b,
            &record_version_boundary(
                "ens:bob.eth",
                resource_b,
                Some(3),
                Some(EVENT_KIND_RECORD_VERSION_CHANGED),
                1002,
                "0xrec1002",
                1_776_200_002,
                "ethereum-mainnet",
            ),
        )
        .await?
        .context("resource_b row must exist")?;
        assert_eq!(
            row_b.selectors,
            json!([{
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true,
            }])
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn keyed_rebuild_replaces_one_resource_without_touching_other_rows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_a = Uuid::from_u128(0x9300);
        let resource_b = Uuid::from_u128(0x9400);

        seed_resources(database.pool(), &[resource_a, resource_b]).await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xrec1010", 1010, 1_776_200_010),
                raw_block("ethereum-mainnet", "0xrec1011", 1011, 1_776_200_011),
                raw_block("ethereum-mainnet", "0xrec1012", 1012, 1_776_200_012),
                raw_block("ethereum-mainnet", "0xrec1013", 1013, 1_776_200_013),
            ],
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                record_version_changed_event(
                    "res-a-boundary",
                    "ens:alice.eth",
                    resource_a,
                    7,
                    1010,
                    0,
                ),
                record_changed_event(
                    "res-a-text",
                    "ens:alice.eth",
                    resource_a,
                    "text",
                    "text",
                    None,
                    1011,
                    0,
                ),
                record_version_changed_event(
                    "res-b-boundary",
                    "ens:bob.eth",
                    resource_b,
                    8,
                    1012,
                    0,
                ),
                record_changed_event(
                    "res-b-addr",
                    "ens:bob.eth",
                    resource_b,
                    "addr:60",
                    "addr",
                    Some("60"),
                    1013,
                    0,
                ),
            ],
        )
        .await?;

        rebuild_record_inventory_current(database.pool(), None).await?;

        seed_raw_blocks(
            database.pool(),
            &[raw_block(
                "ethereum-mainnet",
                "0xrec1014",
                1014,
                1_776_200_014,
            )],
        )
        .await?;
        seed_events(
            database.pool(),
            &[record_changed_event(
                "res-a-native-addr",
                "ens:alice.eth",
                resource_a,
                "addr:60",
                "addr",
                Some("60"),
                1014,
                0,
            )],
        )
        .await?;

        let summary =
            rebuild_record_inventory_current(database.pool(), Some(&resource_a.to_string()))
                .await?;
        assert_eq!(summary.requested_resource_count, 1);
        assert_eq!(summary.upserted_row_count, 1);
        assert_eq!(summary.deleted_row_count, 0);

        let row_a = load_record_inventory_current(
            database.pool(),
            resource_a,
            &record_version_boundary(
                "ens:alice.eth",
                resource_a,
                Some(1),
                Some(EVENT_KIND_RECORD_VERSION_CHANGED),
                1010,
                "0xrec1010",
                1_776_200_010,
                "ethereum-mainnet",
            ),
        )
        .await?
        .context("resource_a row must still exist")?;
        assert_eq!(
            row_a.selectors,
            json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "cacheable": true,
                },
                {
                    "record_key": "text",
                    "record_family": "text",
                    "selector_key": null,
                    "cacheable": true,
                }
            ])
        );

        let row_b = load_record_inventory_current(
            database.pool(),
            resource_b,
            &record_version_boundary(
                "ens:bob.eth",
                resource_b,
                Some(3),
                Some(EVENT_KIND_RECORD_VERSION_CHANGED),
                1012,
                "0xrec1012",
                1_776_200_012,
                "ethereum-mainnet",
            ),
        )
        .await?
        .context("resource_b row must remain untouched")?;
        assert_eq!(
            row_b.selectors,
            json!([{
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true,
            }])
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn keyed_rebuild_keeps_visible_rows_when_projection_build_fails() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x9450);
        let boundary = record_version_boundary(
            "ens:alice.eth",
            resource_id,
            Some(99),
            Some(EVENT_KIND_RECORD_VERSION_CHANGED),
            900,
            "0xstale-boundary",
            1_776_200_900,
            "ethereum-mainnet",
        );

        seed_resources(database.pool(), &[resource_id]).await?;
        upsert_record_inventory_current_rows(
            database.pool(),
            &[RecordInventoryCurrentRow {
                resource_id,
                record_version_boundary: boundary.clone(),
                enumeration_basis: json!({
                    "observed_selectors": true,
                    "capability_declared_families": true,
                    "globally_enumerable": true,
                }),
                selectors: json!([]),
                explicit_gaps: json!([]),
                unsupported_families: json!([]),
                last_change: None,
                entries: json!([]),
                provenance: json!({"derivation_kind": RECORD_INVENTORY_CURRENT_DERIVATION_KIND}),
                coverage: json!({"enumeration_basis": RECORD_INVENTORY_ENUMERATION_BASIS}),
                chain_positions: json!({}),
                canonicality_summary: json!({"status": "finalized", "chains": {}}),
                manifest_version: 1,
                last_recomputed_at: OffsetDateTime::from_unix_timestamp(1_776_200_001)
                    .expect("test timestamp must be valid"),
            }],
        )
        .await?;
        seed_events(
            database.pool(),
            &[record_version_changed_event(
                "missing-block-boundary",
                "ens:alice.eth",
                resource_id,
                100,
                1100,
                0,
            )],
        )
        .await?;

        let error =
            rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string()))
                .await
                .expect_err("rebuild should fail when the record boundary block is missing");
        assert!(
            error
                .to_string()
                .contains("record event must have a raw_blocks timestamp for chain_position")
        );

        let row = load_record_inventory_current(database.pool(), resource_id, &boundary)
            .await?
            .context("stale visible row should still exist after failed rebuild")?;
        assert_eq!(row.record_version_boundary, boundary);

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_surfaces_supported_selectors_gaps_and_unsupported_families() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x9500);

        seed_resources(database.pool(), &[resource_id]).await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xrec1020", 1020, 1_776_200_020),
                raw_block("ethereum-mainnet", "0xrec1021", 1021, 1_776_200_021),
            ],
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                record_version_changed_event("boundary", "ens:alice.eth", resource_id, 9, 1020, 0),
                record_changed_event(
                    "multicoin",
                    "ens:alice.eth",
                    resource_id,
                    "addr:61",
                    "addr",
                    Some("61"),
                    1021,
                    0,
                ),
                record_changed_event(
                    "unsupported-avatar",
                    "ens:alice.eth",
                    resource_id,
                    "avatar",
                    "avatar",
                    None,
                    1021,
                    1,
                ),
            ],
        )
        .await?;

        rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

        let row = load_record_inventory_current(
            database.pool(),
            resource_id,
            &record_version_boundary(
                "ens:alice.eth",
                resource_id,
                Some(1),
                Some(EVENT_KIND_RECORD_VERSION_CHANGED),
                1020,
                "0xrec1020",
                1_776_200_020,
                "ethereum-mainnet",
            ),
        )
        .await?
        .context("row must exist")?;

        assert_eq!(
            row.selectors,
            json!([{
                "record_key": "addr:61",
                "record_family": "addr",
                "selector_key": "61",
                "cacheable": true,
            }])
        );
        assert_eq!(
            row.explicit_gaps,
            json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "gap_reason": GAP_REASON_NOT_OBSERVED,
                },
                {
                    "record_key": "text",
                    "record_family": "text",
                    "selector_key": null,
                    "gap_reason": GAP_REASON_NOT_OBSERVED,
                }
            ])
        );
        assert_eq!(
            row.unsupported_families,
            json!([{
                "record_family": "avatar",
                "unsupported_reason": UNSUPPORTED_FAMILY_REASON,
            }])
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_resets_inventory_at_latest_record_version_boundary() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x9600);

        seed_resources(database.pool(), &[resource_id]).await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xrec1030", 1030, 1_776_200_030),
                raw_block("ethereum-mainnet", "0xrec1031", 1031, 1_776_200_031),
                raw_block("ethereum-mainnet", "0xrec1032", 1032, 1_776_200_032),
                raw_block("ethereum-mainnet", "0xrec1033", 1033, 1_776_200_033),
            ],
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                record_changed_event(
                    "before-boundary-text",
                    "ens:alice.eth",
                    resource_id,
                    "text",
                    "text",
                    None,
                    1030,
                    0,
                ),
                record_version_changed_event(
                    "current-boundary",
                    "ens:alice.eth",
                    resource_id,
                    12,
                    1031,
                    0,
                ),
                record_changed_event(
                    "after-boundary-native-addr",
                    "ens:alice.eth",
                    resource_id,
                    "addr:60",
                    "addr",
                    Some("60"),
                    1032,
                    0,
                ),
                record_changed_event(
                    "after-boundary-text",
                    "ens:alice.eth",
                    resource_id,
                    "text",
                    "text",
                    None,
                    1033,
                    0,
                ),
            ],
        )
        .await?;

        rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

        let row = load_record_inventory_current(
            database.pool(),
            resource_id,
            &record_version_boundary(
                "ens:alice.eth",
                resource_id,
                Some(2),
                Some(EVENT_KIND_RECORD_VERSION_CHANGED),
                1031,
                "0xrec1031",
                1_776_200_031,
                "ethereum-mainnet",
            ),
        )
        .await?
        .context("row must exist")?;

        assert_eq!(
            row.selectors,
            json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "cacheable": true,
                },
                {
                    "record_key": "text",
                    "record_family": "text",
                    "selector_key": null,
                    "cacheable": true,
                }
            ])
        );
        assert_eq!(
            row.record_version_boundary,
            record_version_boundary(
                "ens:alice.eth",
                resource_id,
                Some(2),
                Some(EVENT_KIND_RECORD_VERSION_CHANGED),
                1031,
                "0xrec1031",
                1_776_200_031,
                "ethereum-mainnet",
            )
        );
        assert_eq!(
            row.chain_positions,
            json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 1033,
                    "block_hash": "0xrec1033",
                    "timestamp": "2026-04-14T20:53:53Z",
                }
            })
        );
        assert_eq!(
            row.last_change,
            Some(json!({
                "normalized_event_id": 4,
                "event_kind": EVENT_KIND_RECORD_CHANGED,
                "chain_position": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 1033,
                    "block_hash": "0xrec1033",
                    "timestamp": "2026-04-14T20:53:53Z",
                }
            }))
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_limits_cache_entries_to_cacheable_selectors() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x9700);

        seed_resources(database.pool(), &[resource_id]).await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xrec1040", 1040, 1_776_200_040),
                raw_block("ethereum-mainnet", "0xrec1041", 1041, 1_776_200_041),
            ],
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                record_version_changed_event("boundary", "ens:alice.eth", resource_id, 13, 1040, 0),
                record_changed_event(
                    "text",
                    "ens:alice.eth",
                    resource_id,
                    "text",
                    "text",
                    None,
                    1041,
                    0,
                ),
            ],
        )
        .await?;

        rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

        let row = load_record_inventory_current(
            database.pool(),
            resource_id,
            &record_version_boundary(
                "ens:alice.eth",
                resource_id,
                Some(1),
                Some(EVENT_KIND_RECORD_VERSION_CHANGED),
                1040,
                "0xrec1040",
                1_776_200_040,
                "ethereum-mainnet",
            ),
        )
        .await?
        .context("row must exist")?;

        assert_eq!(
            row.entries,
            json!([{
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "status": "unsupported",
                "unsupported_reason": CACHE_UNSUPPORTED_REASON_VALUE_NOT_RETAINED,
            }])
        );
        assert_eq!(
            row.explicit_gaps,
            json!([{
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "gap_reason": GAP_REASON_NOT_OBSERVED,
            }])
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_consumes_ensv2_resolver_record_events() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x9701);
        let mut boundary = record_version_changed_event(
            "ensv2-boundary",
            "ens:alice.eth",
            resource_id,
            21,
            1050,
            0,
        );
        boundary.derivation_kind = DERIVATION_KIND_ENS_V2_RESOLVER.to_owned();
        boundary.source_family = "ens_v2_resolver_l1".to_owned();
        let mut record = record_changed_event(
            "ensv2-record",
            "ens:alice.eth",
            resource_id,
            "addr:60",
            "addr",
            Some("60"),
            1051,
            0,
        );
        record.derivation_kind = DERIVATION_KIND_ENS_V2_RESOLVER.to_owned();
        record.source_family = "ens_v2_resolver_l1".to_owned();

        seed_resources(database.pool(), &[resource_id]).await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xrec1050", 1050, 1_776_200_050),
                raw_block("ethereum-mainnet", "0xrec1051", 1051, 1_776_200_051),
            ],
        )
        .await?;
        seed_events(database.pool(), &[boundary, record]).await?;

        rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

        let row = load_record_inventory_current(
            database.pool(),
            resource_id,
            &record_version_boundary(
                "ens:alice.eth",
                resource_id,
                Some(1),
                Some(EVENT_KIND_RECORD_VERSION_CHANGED),
                1050,
                "0xrec1050",
                1_776_200_050,
                "ethereum-mainnet",
            ),
        )
        .await?
        .context("ENSv2 resolver row must exist")?;

        assert_eq!(row.selectors[0]["record_key"], json!("addr:60"));
        assert_eq!(row.entries[0]["record_key"], json!("addr:60"));
        assert_eq!(
            row.chain_positions,
            json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 1051,
                    "block_hash": "0xrec1051",
                    "timestamp": "2026-04-14T20:54:11Z",
                }
            })
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_projects_basenames_base_authority_record_inventory() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x9800);
        let resolver_contract_instance_id = Uuid::from_u128(0x9801);
        let resolver_address = "0x00000000000000000000000000000000000000cc";

        insert_basenames_resolver_profile_seed(
            database.pool(),
            resolver_contract_instance_id,
            resolver_address,
        )
        .await?;
        seed_basenames_resources(database.pool(), &[resource_id]).await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("base-mainnet", "0xbase-rec1050", 1050, 1_776_200_050),
                raw_block("base-mainnet", "0xbase-rec1051", 1051, 1_776_200_051),
                raw_block("base-mainnet", "0xbase-rec1052", 1052, 1_776_200_052),
            ],
        )
        .await?;
        seed_raw_logs(
            database.pool(),
            &[
                raw_log(
                    "base-mainnet",
                    "0xbase-rec1050",
                    1050,
                    "0xbase-tx1050",
                    0,
                    resolver_address,
                ),
                raw_log(
                    "base-mainnet",
                    "0xbase-rec1051",
                    1051,
                    "0xbase-tx1051",
                    0,
                    resolver_address,
                ),
                raw_log(
                    "base-mainnet",
                    "0xbase-rec1052",
                    1052,
                    "0xbase-tx1052",
                    0,
                    resolver_address,
                ),
            ],
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                basenames_record_version_changed_event(
                    "base-boundary",
                    "basenames:alice.base.eth",
                    resource_id,
                    21,
                    1050,
                    0,
                ),
                basenames_record_changed_event(
                    "base-native-addr",
                    "basenames:alice.base.eth",
                    resource_id,
                    "addr:60",
                    "addr",
                    Some("60"),
                    1051,
                    0,
                ),
                basenames_record_changed_event(
                    "base-twitter",
                    "basenames:alice.base.eth",
                    resource_id,
                    "text",
                    "text",
                    None,
                    1052,
                    0,
                ),
            ],
        )
        .await?;

        let summary =
            rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string()))
                .await?;
        assert_eq!(summary.requested_resource_count, 1);
        assert_eq!(summary.upserted_row_count, 1);
        assert_eq!(summary.deleted_row_count, 0);

        let row = load_record_inventory_current(
            database.pool(),
            resource_id,
            &record_version_boundary(
                "basenames:alice.base.eth",
                resource_id,
                Some(1),
                Some(EVENT_KIND_RECORD_VERSION_CHANGED),
                1050,
                "0xbase-rec1050",
                1_776_200_050,
                "base-mainnet",
            ),
        )
        .await?
        .context("basenames record_inventory_current row must exist")?;

        assert_eq!(
            row.selectors,
            json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "cacheable": true,
                },
                {
                    "record_key": "text",
                    "record_family": "text",
                    "selector_key": null,
                    "cacheable": true,
                }
            ])
        );
        assert_eq!(
            row.record_version_boundary,
            record_version_boundary(
                "basenames:alice.base.eth",
                resource_id,
                Some(1),
                Some(EVENT_KIND_RECORD_VERSION_CHANGED),
                1050,
                "0xbase-rec1050",
                1_776_200_050,
                "base-mainnet",
            )
        );
        assert_eq!(
            row.coverage["source_classes_considered"],
            json!([SOURCE_FAMILY_BASENAMES_BASE_RESOLVER])
        );
        assert_eq!(
            row.chain_positions,
            json!({
                "base": {
                    "chain_id": "base-mainnet",
                    "block_number": 1052,
                    "block_hash": "0xbase-rec1052",
                    "timestamp": "2026-04-14T20:54:12Z",
                }
            })
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_adds_basenames_transport_position_from_lineage() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x980a);
        let resolver_contract_instance_id = Uuid::from_u128(0x980b);
        let resolver_address = "0x00000000000000000000000000000000000000dd";

        insert_basenames_resolver_profile_seed(
            database.pool(),
            resolver_contract_instance_id,
            resolver_address,
        )
        .await?;
        seed_basenames_resources(database.pool(), &[resource_id]).await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-rec1055", 1055, 1_776_200_055),
                raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-rec1057", 1057, 1_776_200_057),
            ],
        )
        .await?;
        seed_raw_logs(
            database.pool(),
            &[
                raw_log(
                    BASE_MAINNET_CHAIN_ID,
                    "0xbase-rec1055",
                    1055,
                    "0xbase-tx1055",
                    0,
                    resolver_address,
                ),
                raw_log(
                    BASE_MAINNET_CHAIN_ID,
                    "0xbase-rec1057",
                    1057,
                    "0xbase-tx1057",
                    0,
                    resolver_address,
                ),
            ],
        )
        .await?;
        upsert_chain_lineage_blocks(
            database.pool(),
            &[
                chain_lineage_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xeth-before-basenames-record",
                    21_000_099,
                    1_776_200_054,
                ),
                chain_lineage_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xeth-before-later-basenames-record",
                    21_000_100,
                    1_776_200_056,
                ),
                chain_lineage_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xeth-after-later-basenames-record",
                    21_000_101,
                    1_776_200_058,
                ),
            ],
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                basenames_record_version_changed_event(
                    "base-boundary-with-transport",
                    "basenames:alice.base.eth",
                    resource_id,
                    21,
                    1055,
                    0,
                ),
                basenames_record_changed_event(
                    "base-record-after-transport-boundary",
                    "basenames:alice.base.eth",
                    resource_id,
                    "text",
                    "text",
                    None,
                    1057,
                    0,
                ),
            ],
        )
        .await?;

        rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string())).await?;

        let row = load_record_inventory_current(
            database.pool(),
            resource_id,
            &record_version_boundary(
                "basenames:alice.base.eth",
                resource_id,
                Some(1),
                Some(EVENT_KIND_RECORD_VERSION_CHANGED),
                1055,
                "0xbase-rec1055",
                1_776_200_055,
                BASE_MAINNET_CHAIN_ID,
            ),
        )
        .await?
        .context("basenames transport-aware record_inventory_current row must exist")?;

        assert_eq!(
            row.chain_positions,
            json!({
                "base": {
                    "chain_id": BASE_MAINNET_CHAIN_ID,
                    "block_number": 1057,
                    "block_hash": "0xbase-rec1057",
                    "timestamp": "2026-04-14T20:54:17Z",
                },
                "ethereum": {
                    "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                    "block_number": 21_000_100,
                    "block_hash": "0xeth-before-later-basenames-record",
                    "timestamp": "2026-04-14T20:54:16Z",
                },
            })
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_keeps_unadmitted_basenames_dynamic_resolver_inventory_explicit() -> Result<()>
    {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x9810);
        let resolver_address = "0x0000000000000000000000000000000000009811";

        seed_basenames_resources(database.pool(), &[resource_id]).await?;
        seed_raw_blocks(
            database.pool(),
            &[raw_block(
                "base-mainnet",
                "0xbase-rec1060",
                1060,
                1_776_200_060,
            )],
        )
        .await?;
        seed_events(
            database.pool(),
            &[basenames_resolver_changed_event(
                "base-pending-resolver",
                "basenames:pending.base.eth",
                resource_id,
                resolver_address,
                1060,
                0,
            )],
        )
        .await?;

        let summary =
            rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string()))
                .await?;
        assert_eq!(summary.requested_resource_count, 1);
        assert_eq!(summary.upserted_row_count, 1);

        let row = load_record_inventory_current(
            database.pool(),
            resource_id,
            &record_version_boundary(
                "basenames:pending.base.eth",
                resource_id,
                None,
                None,
                1060,
                "0xbase-rec1060",
                1_776_200_060,
                "base-mainnet",
            ),
        )
        .await?
        .context("unadmitted Basenames resolver inventory row must exist")?;

        assert_eq!(row.selectors, json!([]));
        assert_eq!(
            row.unsupported_families,
            json!([
                {
                    "record_family": "addr",
                    "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
                },
                {
                    "record_family": "text",
                    "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
                }
            ])
        );
        assert_eq!(
            row.coverage["unsupported_reason"],
            json!(RESOLVER_FAMILY_PENDING_REASON)
        );
        assert_eq!(
            row.coverage["source_classes_considered"],
            json!([SOURCE_FAMILY_BASENAMES_BASE_REGISTRY])
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_basenames_dynamic_resolver_inventory_gates_supported_pending_and_unsupported_targets()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let supported_resource_id = Uuid::from_u128(0x9820);
        let pending_resource_id = Uuid::from_u128(0x9821);
        let unsupported_resource_id = Uuid::from_u128(0x9822);
        let seed_resolver_contract_instance_id = Uuid::from_u128(0x9823);
        let supported_resolver_contract_instance_id = Uuid::from_u128(0x9824);
        let pending_resolver_contract_instance_id = Uuid::from_u128(0x9825);
        let unsupported_resolver_contract_instance_id = Uuid::from_u128(0x9826);
        let seed_resolver_address = "0x0000000000000000000000000000000000009823";
        let supported_resolver_address = "0x0000000000000000000000000000000000009824";
        let pending_resolver_address = "0x0000000000000000000000000000000000009825";
        let unsupported_resolver_address = "0x0000000000000000000000000000000000009826";

        insert_basenames_dynamic_resolver_profile_fixture(
            database.pool(),
            seed_resolver_contract_instance_id,
            seed_resolver_address,
            &[
                (
                    supported_resolver_contract_instance_id,
                    supported_resolver_address,
                ),
                (
                    pending_resolver_contract_instance_id,
                    pending_resolver_address,
                ),
                (
                    unsupported_resolver_contract_instance_id,
                    unsupported_resolver_address,
                ),
            ],
            &[
                (supported_resolver_address, Some(BASENAMES_L2_CODE_HASH)),
                (pending_resolver_address, None),
                (unsupported_resolver_address, Some(UNSUPPORTED_CODE_HASH)),
            ],
        )
        .await?;
        seed_basenames_resources(
            database.pool(),
            &[
                supported_resource_id,
                pending_resource_id,
                unsupported_resource_id,
            ],
        )
        .await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("base-mainnet", "0xbase-rec1200", 1200, 1_776_200_200),
                raw_block("base-mainnet", "0xbase-rec1201", 1201, 1_776_200_201),
                raw_block("base-mainnet", "0xbase-rec1202", 1202, 1_776_200_202),
            ],
        )
        .await?;
        seed_raw_logs(
            database.pool(),
            &[raw_log(
                "base-mainnet",
                "0xbase-rec1201",
                1201,
                "0xbase-tx1201",
                0,
                supported_resolver_address,
            )],
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                basenames_resolver_changed_event(
                    "base-supported-resolver",
                    "basenames:supported.base.eth",
                    supported_resource_id,
                    supported_resolver_address,
                    1200,
                    0,
                ),
                basenames_record_changed_event(
                    "base-supported-text",
                    "basenames:supported.base.eth",
                    supported_resource_id,
                    "text",
                    "text",
                    None,
                    1201,
                    0,
                ),
                basenames_resolver_changed_event(
                    "base-pending-resolver",
                    "basenames:pending.base.eth",
                    pending_resource_id,
                    pending_resolver_address,
                    1202,
                    0,
                ),
                basenames_resolver_changed_event(
                    "base-unsupported-resolver",
                    "basenames:unsupported.base.eth",
                    unsupported_resource_id,
                    unsupported_resolver_address,
                    1202,
                    1,
                ),
            ],
        )
        .await?;

        let summary = rebuild_record_inventory_current(database.pool(), None).await?;
        assert_eq!(summary.requested_resource_count, 3);
        assert_eq!(summary.upserted_row_count, 3);

        let supported_row = load_record_inventory_current(
            database.pool(),
            supported_resource_id,
            &record_version_boundary(
                "basenames:supported.base.eth",
                supported_resource_id,
                None,
                None,
                1200,
                "0xbase-rec1200",
                1_776_200_200,
                "base-mainnet",
            ),
        )
        .await?
        .context("supported Basenames resolver inventory row must exist")?;
        assert_eq!(
            supported_row.selectors,
            json!([{
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "cacheable": true,
            }])
        );
        assert_eq!(supported_row.coverage["unsupported_reason"], Value::Null);
        assert_eq!(
            supported_row.coverage["source_classes_considered"],
            json!([
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
                SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
            ])
        );

        for (resource_id, logical_name_id, block_hash) in [
            (
                pending_resource_id,
                "basenames:pending.base.eth",
                "0xbase-rec1202",
            ),
            (
                unsupported_resource_id,
                "basenames:unsupported.base.eth",
                "0xbase-rec1202",
            ),
        ] {
            let row = load_record_inventory_current(
                database.pool(),
                resource_id,
                &record_version_boundary(
                    logical_name_id,
                    resource_id,
                    None,
                    None,
                    1202,
                    block_hash,
                    1_776_200_202,
                    "base-mainnet",
                ),
            )
            .await?
            .with_context(|| format!("{logical_name_id} inventory row must exist"))?;
            assert_eq!(row.selectors, json!([]));
            assert_eq!(
                row.unsupported_families,
                json!([
                    {
                        "record_family": "addr",
                        "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
                    },
                    {
                        "record_family": "text",
                        "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
                    }
                ])
            );
            assert_eq!(
                row.coverage["unsupported_reason"],
                json!(RESOLVER_FAMILY_PENDING_REASON)
            );
            assert_eq!(
                row.last_change
                    .as_ref()
                    .and_then(|value| value.get("chain_position"))
                    .and_then(|value| value.get("block_hash")),
                Some(&json!(block_hash))
            );
            assert_eq!(
                row.last_change
                    .as_ref()
                    .and_then(|value| value.get("chain_position"))
                    .and_then(|value| value.get("block_number")),
                Some(&json!(1202))
            );
            assert_eq!(
                row.last_change
                    .as_ref()
                    .and_then(|value| value.get("event_kind")),
                Some(&json!(EVENT_KIND_RESOLVER_CHANGED))
            );
            assert_eq!(
                row.last_change
                    .as_ref()
                    .and_then(|value| value.get("chain_position"))
                    .and_then(|value| value.get("timestamp")),
                Some(&json!("2026-04-14T20:56:42Z"))
            );
        }

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_keeps_pending_ensv1_dynamic_resolver_inventory_explicit() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x9900);
        let registry_contract_instance_id = Uuid::from_u128(0x9901);
        let public_resolver_contract_instance_id = Uuid::from_u128(0x9902);
        let registry_address = "0x0000000000000000000000000000000000009901";
        let public_resolver_address = "0x0000000000000000000000000000000000009902";
        let pending_resolver_address = "0x0000000000000000000000000000000000009903";

        let registry_manifest_id = insert_manifest_version(
            database.pool(),
            "ens_v1_registry_l1",
            "manifests/ens/ens_v1_registry_l1/v2.toml",
        )
        .await?;
        let resolver_manifest_id = insert_manifest_version(
            database.pool(),
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            "manifests/ens/ens_v1_resolver_l1/v1.toml",
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            registry_contract_instance_id,
            registry_address,
            registry_manifest_id,
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            public_resolver_contract_instance_id,
            public_resolver_address,
            resolver_manifest_id,
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            resolver_manifest_id,
            "public_resolver",
            public_resolver_contract_instance_id,
            public_resolver_address,
        )
        .await?;

        seed_resources(database.pool(), &[resource_id]).await?;
        seed_raw_blocks(
            database.pool(),
            &[raw_block(
                "ethereum-mainnet",
                "0xrec1060",
                1060,
                1_776_200_060,
            )],
        )
        .await?;
        seed_events(
            database.pool(),
            &[resolver_changed_event(
                "pending-resolver",
                "ens:pending.eth",
                resource_id,
                pending_resolver_address,
                registry_manifest_id,
                1060,
                0,
            )],
        )
        .await?;

        let summary =
            rebuild_record_inventory_current(database.pool(), Some(&resource_id.to_string()))
                .await?;
        assert_eq!(summary.requested_resource_count, 1);
        assert_eq!(summary.upserted_row_count, 1);

        let row = load_record_inventory_current(
            database.pool(),
            resource_id,
            &record_version_boundary(
                "ens:pending.eth",
                resource_id,
                None,
                None,
                1060,
                "0xrec1060",
                1_776_200_060,
                "ethereum-mainnet",
            ),
        )
        .await?
        .context("pending resolver inventory row must exist")?;

        assert_eq!(row.selectors, json!([]));
        assert_eq!(
            row.explicit_gaps,
            json!([{
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "gap_reason": GAP_REASON_NOT_OBSERVED,
            }])
        );
        assert_eq!(
            row.unsupported_families,
            json!([
                {
                    "record_family": "addr",
                    "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
                },
                {
                    "record_family": "text",
                    "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
                }
            ])
        );
        assert_eq!(
            row.coverage["unsupported_reason"],
            json!(RESOLVER_FAMILY_PENDING_REASON)
        );
        assert_eq!(
            row.last_change
                .as_ref()
                .and_then(|value| value.get("event_kind")),
            Some(&json!(EVENT_KIND_RESOLVER_CHANGED))
        );

        database.cleanup().await
    }

    async fn seed_resources(database: &PgPool, resource_ids: &[Uuid]) -> Result<()> {
        let resources = resource_ids
            .iter()
            .enumerate()
            .map(|(index, resource_id)| Resource {
                resource_id: *resource_id,
                token_lineage_id: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: format!("0xresource{index:02x}"),
                block_number: 30_000_000 + index as i64,
                provenance: json!({
                    "source": "worker_record_inventory_current_test",
                    "anchor": "resource",
                }),
                canonicality_state: CanonicalityState::Finalized,
            })
            .collect::<Vec<_>>();
        upsert_resources(database, &resources).await?;
        Ok(())
    }

    async fn seed_basenames_resources(database: &PgPool, resource_ids: &[Uuid]) -> Result<()> {
        let resources = resource_ids
            .iter()
            .enumerate()
            .map(|(index, resource_id)| Resource {
                resource_id: *resource_id,
                token_lineage_id: None,
                chain_id: "base-mainnet".to_owned(),
                block_hash: format!("0xbase-resource{index:02x}"),
                block_number: 40_000_000 + index as i64,
                provenance: json!({
                    "source": "worker_record_inventory_current_test",
                    "anchor": "basenames_resource",
                }),
                canonicality_state: CanonicalityState::Finalized,
            })
            .collect::<Vec<_>>();
        upsert_resources(database, &resources).await?;
        Ok(())
    }

    async fn seed_raw_blocks(database: &PgPool, blocks: &[RawBlock]) -> Result<()> {
        upsert_raw_blocks(database, blocks).await?;
        Ok(())
    }

    async fn seed_raw_logs(database: &PgPool, logs: &[RawLog]) -> Result<()> {
        upsert_raw_logs(database, logs).await?;
        Ok(())
    }

    async fn seed_events(database: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
        upsert_normalized_events(database, events).await?;
        Ok(())
    }

    const BASENAMES_L2_CODE_HASH: &str =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    const UNSUPPORTED_CODE_HASH: &str =
        "0x2222222222222222222222222222222222222222222222222222222222222222";

    async fn insert_basenames_dynamic_resolver_profile_fixture(
        pool: &PgPool,
        seed_contract_instance_id: Uuid,
        seed_address: &str,
        dynamic_resolvers: &[(Uuid, &str)],
        code_hashes: &[(&str, Option<&str>)],
    ) -> Result<()> {
        let resolver_manifest_id =
            insert_basenames_resolver_profile_seed(pool, seed_contract_instance_id, seed_address)
                .await?;
        let registry_manifest_id = sqlx::query(
            r#"
            INSERT INTO manifest_versions (
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (
                1,
                'basenames',
                $1,
                'base-mainnet',
                'basenames_v1',
                'active',
                'uts46-v1',
                'manifests/basenames/basenames_base_registry/v1.toml',
                '{}'::jsonb
            )
            RETURNING manifest_id
            "#,
        )
        .bind(SOURCE_FAMILY_BASENAMES_BASE_REGISTRY)
        .fetch_one(pool)
        .await
        .context("failed to insert Basenames registry manifest")?
        .try_get::<i64, _>("manifest_id")
        .context("failed to read Basenames registry manifest_id")?;
        let registry_contract_instance_id = Uuid::from_u128(0x98ff);

        sqlx::query(
            r#"
            INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
            VALUES ($1, 'base-mainnet', 'root', '{}'::jsonb)
            "#,
        )
        .bind(registry_contract_instance_id)
        .execute(pool)
        .await
        .context("failed to insert Basenames registry contract_instance")?;

        for (contract_instance_id, address) in dynamic_resolvers {
            sqlx::query(
                r#"
                INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
                VALUES ($1, 'base-mainnet', 'contract', '{}'::jsonb)
                "#,
            )
            .bind(contract_instance_id)
            .execute(pool)
            .await
            .context("failed to insert Basenames dynamic resolver contract_instance")?;
            sqlx::query(
                r#"
                INSERT INTO contract_instance_addresses (
                    contract_instance_id,
                    chain_id,
                    address,
                    source_manifest_id,
                    provenance
                )
                VALUES ($1, 'base-mainnet', lower($2), $3, '{}'::jsonb)
                "#,
            )
            .bind(contract_instance_id)
            .bind(address)
            .bind(resolver_manifest_id)
            .execute(pool)
            .await
            .context("failed to insert Basenames dynamic resolver contract_instance_address")?;
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
                VALUES (
                    'base-mainnet',
                    'resolver',
                    $1,
                    $2,
                    $3,
                    $4,
                    'test',
                    '{}'::jsonb
                )
                "#,
            )
            .bind(registry_contract_instance_id)
            .bind(contract_instance_id)
            .bind(format!("test:basenames-dynamic-resolver:{address}"))
            .bind(registry_manifest_id)
            .execute(pool)
            .await
            .context("failed to insert Basenames dynamic resolver discovery_edge")?;
        }

        let mut raw_code_hashes = vec![basenames_raw_code_hash(
            seed_address,
            BASENAMES_L2_CODE_HASH,
        )];
        raw_code_hashes.extend(code_hashes.iter().filter_map(|(address, code_hash)| {
            code_hash.map(|code_hash| basenames_raw_code_hash(address, code_hash))
        }));
        upsert_raw_code_hashes(pool, &raw_code_hashes).await?;

        Ok(())
    }

    async fn insert_basenames_resolver_profile_seed(
        pool: &PgPool,
        contract_instance_id: Uuid,
        address: &str,
    ) -> Result<i64> {
        let manifest_id = sqlx::query(
            r#"
            INSERT INTO manifest_versions (
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (
                1,
                'basenames',
                $1,
                'base-mainnet',
                'basenames_v1',
                'active',
                'uts46-v1',
                'manifests/basenames/basenames_base_resolver/v1.toml',
                '{}'::jsonb
            )
            RETURNING manifest_id
            "#,
        )
        .bind(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER)
        .fetch_one(pool)
        .await
        .context("failed to insert Basenames resolver manifest")?
        .try_get::<i64, _>("manifest_id")
        .context("failed to read Basenames resolver manifest_id")?;

        sqlx::query(
            r#"
            INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
            VALUES ($1, 'base-mainnet', 'contract', '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .execute(pool)
        .await
        .context("failed to insert Basenames resolver contract_instance")?;

        sqlx::query(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                source_manifest_id,
                provenance
            )
            VALUES ($1, 'base-mainnet', lower($2), $3, '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .bind(address)
        .bind(manifest_id)
        .execute(pool)
        .await
        .context("failed to insert Basenames resolver contract_instance_address")?;

        sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
                declaration_kind,
                declaration_name,
                contract_instance_id,
                declared_address,
                role,
                proxy_kind
            )
            VALUES ($1, 'contract', 'resolver', $2, lower($3), 'resolver', 'none')
            "#,
        )
        .bind(manifest_id)
        .bind(contract_instance_id)
        .bind(address)
        .execute(pool)
        .await
        .context("failed to insert Basenames resolver manifest_contract_instance")?;

        Ok(manifest_id)
    }

    async fn insert_manifest_version(
        pool: &PgPool,
        source_family: &str,
        file_path: &str,
    ) -> Result<i64> {
        sqlx::query(
            r#"
            INSERT INTO manifest_versions (
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (1, 'ens', $1, 'ethereum-mainnet', 'ens_v1', 'active', 'uts46-v1', $2, '{}'::jsonb)
            RETURNING manifest_id
            "#,
        )
        .bind(source_family)
        .bind(file_path)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to insert manifest_version for {source_family}"))?
        .try_get::<i64, _>("manifest_id")
        .context("failed to read manifest_id")
    }

    async fn insert_contract_instance(
        pool: &PgPool,
        contract_instance_id: Uuid,
        address: &str,
        source_manifest_id: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
            VALUES ($1, 'ethereum-mainnet', 'contract', '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .execute(pool)
        .await
        .context("failed to insert contract_instance")?;

        sqlx::query(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                source_manifest_id,
                provenance
            )
            VALUES ($1, 'ethereum-mainnet', lower($2), $3, '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .bind(address)
        .bind(source_manifest_id)
        .execute(pool)
        .await
        .context("failed to insert contract_instance_address")?;

        Ok(())
    }

    async fn insert_manifest_contract_instance(
        pool: &PgPool,
        manifest_id: i64,
        role: &str,
        contract_instance_id: Uuid,
        address: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
                declaration_kind,
                declaration_name,
                contract_instance_id,
                declared_address,
                role,
                proxy_kind
            )
            VALUES ($1, 'contract', $2, $3, lower($4), $2, 'none')
            "#,
        )
        .bind(manifest_id)
        .bind(role)
        .bind(contract_instance_id)
        .bind(address)
        .execute(pool)
        .await
        .context("failed to insert manifest_contract_instance")?;
        Ok(())
    }

    fn raw_block(chain_id: &str, block_hash: &str, block_number: i64, timestamp: i64) -> RawBlock {
        RawBlock {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: Some(format!("0xparent{block_number:08x}")),
            block_number,
            block_timestamp: OffsetDateTime::from_unix_timestamp(timestamp)
                .expect("test block timestamp must be valid"),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn chain_lineage_block(
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
        timestamp: i64,
    ) -> ChainLineageBlock {
        ChainLineageBlock {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: Some(format!("0xparent{block_number:08x}")),
            block_number,
            block_timestamp: OffsetDateTime::from_unix_timestamp(timestamp)
                .expect("test lineage timestamp must be valid"),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn raw_log(
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
        transaction_hash: &str,
        log_index: i64,
        emitting_address: &str,
    ) -> RawLog {
        RawLog {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            transaction_hash: transaction_hash.to_owned(),
            transaction_index: 0,
            log_index,
            emitting_address: emitting_address.to_owned(),
            topics: vec![],
            data: vec![],
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn basenames_raw_code_hash(address: &str, code_hash: &str) -> RawCodeHash {
        RawCodeHash {
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-code-hash".to_owned(),
            block_number: 41,
            contract_address: address.to_owned(),
            code_hash: code_hash.to_owned(),
            code_byte_length: 5,
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record_changed_event(
        event_identity: &str,
        logical_name_id: &str,
        resource_id: Uuid,
        record_key: &str,
        record_family: &str,
        selector_key: Option<&str>,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(resource_id),
            event_kind: EVENT_KIND_RECORD_CHANGED.to_owned(),
            source_family: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xrec{block_number}")),
            transaction_hash: Some(format!("0xtx{block_number}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-mainnet",
                "block_hash": format!("0xrec{block_number}"),
                "log_index": log_index,
            }),
            derivation_kind: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "record_key": record_key,
                "record_family": record_family,
                "selector_key": selector_key,
            }),
        }
    }

    fn resolver_changed_event(
        event_identity: &str,
        logical_name_id: &str,
        resource_id: Uuid,
        resolver_address: &str,
        source_manifest_id: i64,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(resource_id),
            event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
            source_family: "ens_v1_registry_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: Some(source_manifest_id),
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xrec{block_number}")),
            transaction_hash: Some(format!("0xtx{block_number}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-mainnet",
                "block_hash": format!("0xrec{block_number}"),
                "log_index": log_index,
            }),
            derivation_kind: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "resolver": resolver_address,
                "namehash": format!("namehash:{logical_name_id}"),
            }),
        }
    }

    fn basenames_resolver_changed_event(
        event_identity: &str,
        logical_name_id: &str,
        resource_id: Uuid,
        resolver_address: &str,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: BASENAMES_NAMESPACE.to_owned(),
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(resource_id),
            event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
            source_family: SOURCE_FAMILY_BASENAMES_BASE_REGISTRY.to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("base-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xbase-rec{block_number}")),
            transaction_hash: Some(format!("0xbase-tx{block_number}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "base-mainnet",
                "block_hash": format!("0xbase-rec{block_number}"),
                "log_index": log_index,
            }),
            derivation_kind: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "resolver": resolver_address,
                "namehash": format!("namehash:{logical_name_id}"),
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn basenames_record_changed_event(
        event_identity: &str,
        logical_name_id: &str,
        resource_id: Uuid,
        record_key: &str,
        record_family: &str,
        selector_key: Option<&str>,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "basenames".to_owned(),
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(resource_id),
            event_kind: EVENT_KIND_RECORD_CHANGED.to_owned(),
            source_family: SOURCE_FAMILY_BASENAMES_BASE_RESOLVER.to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("base-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xbase-rec{block_number}")),
            transaction_hash: Some(format!("0xbase-tx{block_number}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "base-mainnet",
                "block_hash": format!("0xbase-rec{block_number}"),
                "log_index": log_index,
            }),
            derivation_kind: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "record_key": record_key,
                "record_family": record_family,
                "selector_key": selector_key,
            }),
        }
    }

    fn record_version_changed_event(
        event_identity: &str,
        logical_name_id: &str,
        resource_id: Uuid,
        record_version: i64,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(resource_id),
            event_kind: EVENT_KIND_RECORD_VERSION_CHANGED.to_owned(),
            source_family: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xrec{block_number}")),
            transaction_hash: Some(format!("0xtx{block_number}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-mainnet",
                "block_hash": format!("0xrec{block_number}"),
                "log_index": log_index,
            }),
            derivation_kind: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({
                "record_version": record_version - 1,
            }),
            after_state: json!({
                "record_version": record_version,
            }),
        }
    }

    fn basenames_record_version_changed_event(
        event_identity: &str,
        logical_name_id: &str,
        resource_id: Uuid,
        record_version: i64,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "basenames".to_owned(),
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(resource_id),
            event_kind: EVENT_KIND_RECORD_VERSION_CHANGED.to_owned(),
            source_family: SOURCE_FAMILY_BASENAMES_BASE_RESOLVER.to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("base-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xbase-rec{block_number}")),
            transaction_hash: Some(format!("0xbase-tx{block_number}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "base-mainnet",
                "block_hash": format!("0xbase-rec{block_number}"),
                "log_index": log_index,
            }),
            derivation_kind: DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({
                "record_version": record_version - 1,
            }),
            after_state: json!({
                "record_version": record_version,
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record_version_boundary(
        logical_name_id: &str,
        resource_id: Uuid,
        normalized_event_id: Option<i64>,
        event_kind: Option<&str>,
        block_number: i64,
        block_hash: &str,
        timestamp: i64,
        chain_id: &str,
    ) -> Value {
        json!({
            "logical_name_id": logical_name_id,
            "resource_id": resource_id.to_string(),
            "normalized_event_id": normalized_event_id,
            "event_kind": event_kind,
            "chain_position": {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": block_hash,
                "timestamp": format_timestamp(
                    OffsetDateTime::from_unix_timestamp(timestamp)
                        .expect("test timestamp must be valid"),
                ),
            }
        })
    }
}
