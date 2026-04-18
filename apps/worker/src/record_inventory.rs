use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use anyhow::{Context, Result};
use bigname_storage::{
    CanonicalityState, RecordInventoryCurrentRow, clear_record_inventory_current,
    upsert_record_inventory_current_rows,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::{OffsetDateTime, UtcOffset},
};
use uuid::Uuid;

const EVENT_KIND_RECORD_CHANGED: &str = "RecordChanged";
const EVENT_KIND_RECORD_VERSION_CHANGED: &str = "RecordVersionChanged";
const SOURCE_FAMILY_ENS_V1_UNWRAPPED_AUTHORITY: &str = "ens_v1_unwrapped_authority";
const RECORD_INVENTORY_CURRENT_DERIVATION_KIND: &str = "record_inventory_current_rebuild";
const RECORD_INVENTORY_ENUMERATION_BASIS: &str = "declared_record_inventory";
const GAP_REASON_NOT_OBSERVED: &str = "not_observed_on_current_resolver";
const CACHE_UNSUPPORTED_REASON_VALUE_NOT_RETAINED: &str = "value_not_retained_in_normalized_events";
const UNSUPPORTED_FAMILY_REASON: &str = "record_family_not_supported_in_phase6_projection";
const SUPPORTED_TEXT_RECORD_KEY: &str = "text";
const SUPPORTED_TEXT_RECORD_FAMILY: &str = "text";
const SUPPORTED_ADDR_RECORD_FAMILY: &str = "addr";
const SUPPORTED_NATIVE_ADDR_SELECTOR_KEY: &str = "60";
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
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct RecordSelector {
    record_key: String,
    record_family: String,
    selector_key: Option<String>,
}

#[derive(Clone, Debug)]
struct ChainPositionCandidate {
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: String,
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
    let resource_ids = load_target_resource_ids(pool).await?;
    let deleted_row_count = clear_record_inventory_current(pool).await?;

    let mut rows = Vec::with_capacity(resource_ids.len());
    for resource_id in &resource_ids {
        if let Some(row) = build_row(pool, *resource_id).await? {
            rows.push(row);
        }
    }

    let upserted_row_count = upsert_record_inventory_current_rows(pool, &rows)
        .await?
        .len();
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
    let resource_id = Uuid::parse_str(resource_id)
        .with_context(|| format!("resource_id must be a UUID: {resource_id}"))?;
    let deleted_row_count = delete_record_inventory_rows_for_resource(pool, resource_id).await?;

    let Some(row) = build_row(pool, resource_id).await? else {
        return Ok(RecordInventoryCurrentRebuildSummary {
            requested_resource_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    };

    let upserted_row_count = upsert_record_inventory_current_rows(pool, &[row])
        .await?
        .len();
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

async fn load_target_resource_ids(pool: &PgPool) -> Result<Vec<Uuid>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT resource_id
        FROM normalized_events
        WHERE source_family = $1
          AND event_kind IN ($2, $3)
          AND resource_id IS NOT NULL
          AND canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY resource_id
        "#
    ))
    .bind(SOURCE_FAMILY_ENS_V1_UNWRAPPED_AUTHORITY)
    .bind(EVENT_KIND_RECORD_CHANGED)
    .bind(EVENT_KIND_RECORD_VERSION_CHANGED)
    .fetch_all(pool)
    .await
    .context("failed to load record_inventory_current rebuild targets")?;

    rows.into_iter()
        .map(|row| row.try_get("resource_id").context("missing resource_id"))
        .collect()
}

async fn build_row(pool: &PgPool, resource_id: Uuid) -> Result<Option<RecordInventoryCurrentRow>> {
    let events = load_relevant_events(pool, resource_id).await?;
    if events.is_empty() {
        return Ok(None);
    }

    let boundary_index = events
        .iter()
        .rposition(|event| event.event_kind == EVENT_KIND_RECORD_VERSION_CHANGED);
    let scoped_events = &events[boundary_index.unwrap_or(0)..];
    let boundary_anchor = match boundary_index {
        Some(index) => events
            .get(index)
            .context("record_inventory_current rebuild boundary index out of range")?,
        None => events
            .last()
            .context("record_inventory_current rebuild requires at least one event")?,
    };
    let record_version_boundary =
        build_record_version_boundary(boundary_anchor, boundary_index.is_some())?;
    let record_change_events = scoped_events
        .iter()
        .filter(|event| event.event_kind == EVENT_KIND_RECORD_CHANGED)
        .collect::<Vec<_>>();

    let selectors = build_selectors(&record_change_events)?;
    let explicit_gaps = build_explicit_gaps(&selectors);
    let unsupported_families = build_unsupported_families(&record_change_events)?;
    let entries = build_entries(&selectors);
    let last_change = scoped_events
        .last()
        .map(|event| build_last_change(event))
        .transpose()?;

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
        provenance: build_provenance(scoped_events)?,
        coverage: build_coverage(scoped_events),
        chain_positions: build_chain_positions(scoped_events),
        canonicality_summary: build_canonicality_summary(scoped_events),
        manifest_version: scoped_events
            .iter()
            .map(|event| event.manifest_version)
            .max()
            .unwrap_or(1),
        last_recomputed_at: scoped_events
            .iter()
            .filter_map(|event| event.block_timestamp)
            .max()
            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
    }))
}

async fn load_relevant_events(pool: &PgPool, resource_id: Uuid) -> Result<Vec<RelevantEvent>> {
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
            ne.after_state
        FROM normalized_events ne
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE ne.source_family = $1
          AND ne.event_kind IN ($2, $3)
          AND ne.resource_id = $4
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
    .bind(SOURCE_FAMILY_ENS_V1_UNWRAPPED_AUTHORITY)
    .bind(EVENT_KIND_RECORD_CHANGED)
    .bind(EVENT_KIND_RECORD_VERSION_CHANGED)
    .bind(resource_id)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load record_inventory_current events for resource_id {resource_id}")
    })?;

    rows.into_iter().map(decode_relevant_event).collect()
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
    })
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

fn build_chain_positions(events: &[RelevantEvent]) -> Value {
    let mut chain_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    for event in events {
        let Some(timestamp) = event.block_timestamp else {
            continue;
        };
        let candidate = ChainPositionCandidate {
            chain_id: event.chain_id.clone(),
            block_number: event.block_number,
            block_hash: event.block_hash.clone(),
            timestamp: format_timestamp(timestamp),
        };

        match chain_positions.get(&candidate.chain_id) {
            Some(existing)
                if existing.block_number > candidate.block_number
                    || (existing.block_number == candidate.block_number
                        && existing.block_hash >= candidate.block_hash) => {}
            _ => {
                chain_positions.insert(candidate.chain_id.clone(), candidate);
            }
        }
    }

    json!(
        chain_positions
            .into_iter()
            .map(|(chain_id, candidate)| {
                (
                    chain_id,
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
        NormalizedEvent, RawBlock, Resource, default_database_url, load_record_inventory_current,
        upsert_normalized_events, upsert_raw_blocks, upsert_resources,
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
        assert_eq!(summary.deleted_row_count, 1);

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
            )
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

    async fn seed_raw_blocks(database: &PgPool, blocks: &[RawBlock]) -> Result<()> {
        upsert_raw_blocks(database, blocks).await?;
        Ok(())
    }

    async fn seed_events(database: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
        upsert_normalized_events(database, events).await?;
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
            source_family: SOURCE_FAMILY_ENS_V1_UNWRAPPED_AUTHORITY.to_owned(),
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
            derivation_kind: SOURCE_FAMILY_ENS_V1_UNWRAPPED_AUTHORITY.to_owned(),
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
            source_family: SOURCE_FAMILY_ENS_V1_UNWRAPPED_AUTHORITY.to_owned(),
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
            derivation_kind: SOURCE_FAMILY_ENS_V1_UNWRAPPED_AUTHORITY.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({
                "record_version": record_version - 1,
            }),
            after_state: json!({
                "record_version": record_version,
            }),
        }
    }

    fn record_version_boundary(
        logical_name_id: &str,
        resource_id: Uuid,
        normalized_event_id: Option<i64>,
        event_kind: Option<&str>,
        block_number: i64,
        block_hash: &str,
        timestamp: i64,
    ) -> Value {
        json!({
            "logical_name_id": logical_name_id,
            "resource_id": resource_id.to_string(),
            "normalized_event_id": normalized_event_id,
            "event_kind": event_kind,
            "chain_position": {
                "chain_id": "ethereum-mainnet",
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
