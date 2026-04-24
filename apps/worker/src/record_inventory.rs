use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bigname_storage::{
    CanonicalityState, RecordInventoryCurrentRow, clear_record_inventory_current,
    upsert_record_inventory_current_rows,
};
use serde_json::{Value, json};
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
mod tests;
