use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, PermissionScope, PermissionsCurrentRow, clear_permissions_current,
    delete_permissions_current, upsert_permissions_current_rows,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::{OffsetDateTime, UtcOffset},
};
use uuid::Uuid;

const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
const PERMISSIONS_CURRENT_DERIVATION_KIND: &str = "permissions_current_rebuild";
const PERMISSIONS_ENUMERATION_BASIS: &str = "resource_permissions";
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PermissionsCurrentRebuildSummary {
    pub requested_resource_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

#[derive(Clone, Debug)]
struct RelevantEvent {
    normalized_event_id: i64,
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
struct PermissionKey {
    subject: String,
    scope: String,
}

#[derive(Clone, Debug)]
struct ChainPositionCandidate {
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: OffsetDateTime,
}

pub async fn rebuild_permissions_current(
    pool: &PgPool,
    resource_id: Option<&str>,
) -> Result<PermissionsCurrentRebuildSummary> {
    match resource_id {
        Some(resource_id) => rebuild_one_resource(pool, resource_id).await,
        None => rebuild_all_resources(pool).await,
    }
}

async fn rebuild_all_resources(pool: &PgPool) -> Result<PermissionsCurrentRebuildSummary> {
    let resource_ids = load_target_resource_ids(pool).await?;
    let deleted_row_count = clear_permissions_current(pool).await?;
    let rows = build_rows(pool, &resource_ids).await?;
    let upserted_row_count = upsert_permissions_current_rows(pool, &rows).await?.len();

    Ok(PermissionsCurrentRebuildSummary {
        requested_resource_count: resource_ids.len(),
        upserted_row_count,
        deleted_row_count,
    })
}

async fn rebuild_one_resource(
    pool: &PgPool,
    resource_id: &str,
) -> Result<PermissionsCurrentRebuildSummary> {
    let resource_id = Uuid::parse_str(resource_id)
        .with_context(|| format!("resource_id must be a UUID: {resource_id}"))?;
    let deleted_row_count = delete_permissions_current(pool, resource_id).await?;
    let rows = build_rows(pool, &[resource_id]).await?;
    let upserted_row_count = upsert_permissions_current_rows(pool, &rows).await?.len();

    Ok(PermissionsCurrentRebuildSummary {
        requested_resource_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn build_rows(pool: &PgPool, resource_ids: &[Uuid]) -> Result<Vec<PermissionsCurrentRow>> {
    let mut rows = Vec::new();

    for resource_id in resource_ids {
        let events = load_permission_events(pool, *resource_id).await?;
        rows.extend(project_rows(*resource_id, &events)?);
    }

    Ok(rows)
}

fn project_rows(resource_id: Uuid, events: &[RelevantEvent]) -> Result<Vec<PermissionsCurrentRow>> {
    let mut latest_by_key = BTreeMap::<PermissionKey, usize>::new();
    let mut history_by_key = BTreeMap::<PermissionKey, Vec<&RelevantEvent>>::new();

    for (index, event) in events.iter().enumerate() {
        let subject = json_text(&event.after_state, &["subject"])?;
        let scope = parse_scope(&event.after_state)?;
        let key = PermissionKey {
            subject,
            scope: scope.storage_key(),
        };
        latest_by_key.insert(key.clone(), index);
        history_by_key.entry(key).or_default().push(event);
    }

    let mut rows = Vec::new();
    for (key, latest_index) in latest_by_key {
        let latest = &events[latest_index];
        let effective_powers = json_string_array(&latest.after_state, &["effective_powers"])?;
        if effective_powers.is_empty() {
            continue;
        }

        let history = history_by_key
            .get(&key)
            .context("missing permissions_current history for projected key")?;
        let scope = parse_scope(&latest.after_state)?;

        rows.push(PermissionsCurrentRow {
            resource_id,
            subject: key.subject,
            scope,
            effective_powers: Value::Array(
                effective_powers
                    .into_iter()
                    .map(Value::String)
                    .collect::<Vec<_>>(),
            ),
            grant_source: json_object_or_default(&latest.after_state, "grant_source"),
            revocation_source: json_optional_object(&latest.after_state, "revocation_source"),
            inheritance_path: latest
                .after_state
                .get("inheritance_path")
                .cloned()
                .unwrap_or_else(|| json!([])),
            transfer_behavior: json_object_or_default(&latest.after_state, "transfer_behavior"),
            provenance: build_provenance(history)?,
            coverage: build_coverage(history),
            chain_positions: build_chain_positions(history),
            canonicality_summary: build_canonicality_summary(history),
            manifest_version: history
                .iter()
                .map(|event| event.manifest_version)
                .max()
                .unwrap_or(1),
            last_recomputed_at: history
                .iter()
                .filter_map(|event| event.block_timestamp)
                .max()
                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
        });
    }

    Ok(rows)
}

async fn load_target_resource_ids(pool: &PgPool) -> Result<Vec<Uuid>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT resource_id
        FROM normalized_events
        WHERE event_kind = $1
          AND resource_id IS NOT NULL
          AND canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY resource_id
        "#
    ))
    .bind(EVENT_KIND_PERMISSION_CHANGED)
    .fetch_all(pool)
    .await
    .context("failed to load resource_ids for permissions_current rebuild")?;

    rows.into_iter()
        .map(|row| row.try_get("resource_id").context("missing resource_id"))
        .collect()
}

async fn load_permission_events(pool: &PgPool, resource_id: Uuid) -> Result<Vec<RelevantEvent>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.resource_id,
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
        WHERE ne.event_kind = $1
          AND ne.resource_id = $2
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY
            ne.block_number ASC NULLS FIRST,
            ne.log_index ASC NULLS FIRST,
            ne.normalized_event_id ASC
        "#
    ))
    .bind(EVENT_KIND_PERMISSION_CHANGED)
    .bind(resource_id)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load canonical PermissionChanged events for resource_id {resource_id}")
    })?;

    rows.into_iter().map(decode_relevant_event).collect()
}

fn decode_relevant_event(row: sqlx::postgres::PgRow) -> Result<RelevantEvent> {
    Ok(RelevantEvent {
        normalized_event_id: row.try_get("normalized_event_id")?,
        source_family: row.try_get("source_family")?,
        manifest_version: row.try_get("manifest_version")?,
        source_manifest_id: row.try_get("source_manifest_id")?,
        chain_id: row
            .try_get::<Option<String>, _>("chain_id")?
            .context("PermissionChanged rows must include chain_id")?,
        block_number: row
            .try_get::<Option<i64>, _>("block_number")?
            .context("PermissionChanged rows must include block_number")?,
        block_hash: row
            .try_get::<Option<String>, _>("block_hash")?
            .context("PermissionChanged rows must include block_hash")?,
        block_timestamp: row.try_get("block_timestamp")?,
        raw_fact_ref: row.try_get("raw_fact_ref")?,
        canonicality_state: parse_canonicality_state(
            &row.try_get::<String, _>("canonicality_state")?,
        )?,
        after_state: row.try_get("after_state")?,
    })
}

fn parse_scope(state: &Value) -> Result<PermissionScope> {
    let scope = state
        .get("scope")
        .and_then(Value::as_object)
        .context("PermissionChanged after_state.scope must be an object")?;
    let kind = scope
        .get("kind")
        .and_then(Value::as_str)
        .context("PermissionChanged after_state.scope.kind must be a string")?;

    match kind {
        "root" => Ok(PermissionScope::Root),
        "registry" => Ok(PermissionScope::Registry),
        "resource" => Ok(PermissionScope::Resource),
        "resolver" => Ok(PermissionScope::Resolver {
            chain_id: scope
                .get("chain_id")
                .and_then(Value::as_str)
                .context("resolver scope must include chain_id")?
                .to_owned(),
            resolver_address: scope
                .get("resolver_address")
                .and_then(Value::as_str)
                .context("resolver scope must include resolver_address")?
                .to_ascii_lowercase(),
        }),
        "record_manager" => Ok(PermissionScope::RecordManager {
            chain_id: scope
                .get("chain_id")
                .and_then(Value::as_str)
                .context("record_manager scope must include chain_id")?
                .to_owned(),
            manager_address: scope
                .get("manager_address")
                .and_then(Value::as_str)
                .context("record_manager scope must include manager_address")?
                .to_ascii_lowercase(),
        }),
        "migration_derived" => Ok(PermissionScope::MigrationDerived {
            predecessor_resource_id: Uuid::parse_str(
                scope
                    .get("predecessor_resource_id")
                    .and_then(Value::as_str)
                    .context("migration_derived scope must include predecessor_resource_id")?,
            )
            .context("migration_derived scope predecessor_resource_id must be a UUID")?,
        }),
        "transport_derived" => Ok(PermissionScope::TransportDerived {
            transport: scope
                .get("transport")
                .and_then(Value::as_str)
                .context("transport_derived scope must include transport")?
                .to_owned(),
        }),
        _ => bail!("unsupported PermissionChanged scope kind {kind}"),
    }
}

fn json_text(value: &Value, path: &[&str]) -> Result<String> {
    let mut current = value;
    for segment in path {
        current = current
            .get(*segment)
            .with_context(|| format!("missing PermissionChanged field {}", path.join(".")))?;
    }

    current.as_str().map(str::to_owned).with_context(|| {
        format!(
            "PermissionChanged field {} must be a string",
            path.join(".")
        )
    })
}

fn json_string_array(value: &Value, path: &[&str]) -> Result<Vec<String>> {
    let mut current = value;
    for segment in path {
        current = current
            .get(*segment)
            .with_context(|| format!("missing PermissionChanged field {}", path.join(".")))?;
    }

    current
        .as_array()
        .with_context(|| {
            format!(
                "PermissionChanged field {} must be an array",
                path.join(".")
            )
        })?
        .iter()
        .map(|item| {
            item.as_str().map(str::to_owned).with_context(|| {
                format!(
                    "PermissionChanged field {} must contain strings",
                    path.join(".")
                )
            })
        })
        .collect()
}

fn json_object_or_default(value: &Value, field: &str) -> Value {
    match value.get(field) {
        Some(Value::Object(_)) => value[field].clone(),
        _ => json!({}),
    }
}

fn json_optional_object(value: &Value, field: &str) -> Option<Value> {
    match value.get(field) {
        Some(Value::Object(_)) => Some(value[field].clone()),
        _ => None,
    }
}

fn build_provenance(events: &[&RelevantEvent]) -> Result<Value> {
    let normalized_event_ids = events
        .iter()
        .map(|event| event.normalized_event_id)
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
        "normalized_event_ids": normalized_event_ids,
        "raw_fact_refs": raw_fact_refs,
        "manifest_versions": manifest_versions,
        "execution_trace_id": Value::Null,
        "derivation_kind": PERMISSIONS_CURRENT_DERIVATION_KIND,
    }))
}

fn build_coverage(events: &[&RelevantEvent]) -> Value {
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
        "enumeration_basis": PERMISSIONS_ENUMERATION_BASIS,
    })
}

fn build_chain_positions(events: &[&RelevantEvent]) -> Value {
    let mut chain_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    for event in events {
        let Some(timestamp) = event.block_timestamp else {
            continue;
        };
        let candidate = ChainPositionCandidate {
            chain_id: event.chain_id.clone(),
            block_number: event.block_number,
            block_hash: event.block_hash.clone(),
            timestamp,
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
                        "timestamp": format_timestamp(candidate.timestamp),
                    }),
                )
            })
            .collect::<serde_json::Map<String, Value>>()
    )
}

fn build_canonicality_summary(events: &[&RelevantEvent]) -> Value {
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
        _ => bail!("unknown canonicality_state value {value}"),
    }
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
        NormalizedEvent, RawBlock, Resource, default_database_url, load_permissions_current,
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
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for worker permissions_current tests")?;
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bg_wp_{}_{}_{}",
                std::process::id(),
                sequence,
                &Uuid::new_v4().simple().to_string()[..8]
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for worker permissions_current tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect worker permissions_current test pool")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for worker permissions_current tests")?;

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
    async fn keyed_rebuild_keeps_active_rows_and_drops_revoked_rows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x7100);

        seed_resources(database.pool(), &[resource_id]).await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xperm0064", 100, 1_776_100_100),
                raw_block("ethereum-mainnet", "0xperm0065", 101, 1_776_100_101),
                raw_block("ethereum-mainnet", "0xperm0066", 102, 1_776_100_102),
            ],
        )
        .await?;
        seed_permission_events(
            database.pool(),
            &[
                permission_event(
                    "grant-resource",
                    resource_id,
                    "0x0000000000000000000000000000000000000abc",
                    json!({"kind": "resource"}),
                    json!(["set_records"]),
                    Some(json!({"kind": "normalized_event", "normalized_event_id": 1})),
                    None,
                    100,
                    0,
                ),
                permission_event(
                    "grant-resolver",
                    resource_id,
                    "0x0000000000000000000000000000000000000abc",
                    json!({
                        "kind": "resolver",
                        "chain_id": "ethereum-mainnet",
                        "resolver_address": "0x0000000000000000000000000000000000000def"
                    }),
                    json!(["set_resolver"]),
                    Some(json!({"kind": "normalized_event", "normalized_event_id": 2})),
                    None,
                    101,
                    0,
                ),
                permission_event(
                    "revoke-resource",
                    resource_id,
                    "0x0000000000000000000000000000000000000abc",
                    json!({"kind": "resource"}),
                    json!([]),
                    None,
                    Some(json!({"kind": "normalized_event", "normalized_event_id": 3})),
                    102,
                    0,
                ),
            ],
        )
        .await?;

        let summary =
            rebuild_permissions_current(database.pool(), Some(&resource_id.to_string())).await?;
        assert_eq!(summary.requested_resource_count, 1);
        assert_eq!(summary.upserted_row_count, 1);
        assert_eq!(summary.deleted_row_count, 0);

        let rows = load_permissions_current(database.pool(), resource_id, None, None).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].scope,
            PermissionScope::Resolver {
                chain_id: "ethereum-mainnet".to_owned(),
                resolver_address: "0x0000000000000000000000000000000000000def".to_owned(),
            }
        );
        assert_eq!(rows[0].effective_powers, json!(["set_resolver"]));
        assert_eq!(rows[0].provenance["normalized_event_ids"], json!([2]));
        assert_eq!(
            rows[0].coverage["enumeration_basis"],
            json!(PERMISSIONS_ENUMERATION_BASIS)
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn keyed_rebuild_moves_permission_rows_on_subject_change() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x7200);

        seed_resources(database.pool(), &[resource_id]).await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xperm006e", 110, 1_776_100_110),
                raw_block("ethereum-mainnet", "0xperm006f", 111, 1_776_100_111),
                raw_block("ethereum-mainnet", "0xperm0070", 112, 1_776_100_112),
            ],
        )
        .await?;
        seed_permission_events(
            database.pool(),
            &[
                permission_event(
                    "grant-old-subject",
                    resource_id,
                    "0x0000000000000000000000000000000000000aaa",
                    json!({"kind": "resource"}),
                    json!(["set_records"]),
                    Some(json!({"kind": "normalized_event", "normalized_event_id": 10})),
                    None,
                    110,
                    0,
                ),
                permission_event(
                    "revoke-old-subject",
                    resource_id,
                    "0x0000000000000000000000000000000000000aaa",
                    json!({"kind": "resource"}),
                    json!([]),
                    None,
                    Some(json!({"kind": "normalized_event", "normalized_event_id": 11})),
                    111,
                    0,
                ),
                permission_event(
                    "grant-new-subject",
                    resource_id,
                    "0x0000000000000000000000000000000000000bbb",
                    json!({"kind": "resource"}),
                    json!(["set_records"]),
                    Some(json!({"kind": "normalized_event", "normalized_event_id": 12})),
                    None,
                    112,
                    0,
                ),
            ],
        )
        .await?;

        let summary =
            rebuild_permissions_current(database.pool(), Some(&resource_id.to_string())).await?;
        assert_eq!(summary.upserted_row_count, 1);

        let rows = load_permissions_current(database.pool(), resource_id, None, None).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].subject,
            "0x0000000000000000000000000000000000000bbb"
        );
        assert_eq!(rows[0].scope, PermissionScope::Resource);
        assert_eq!(rows[0].effective_powers, json!(["set_records"]));

        database.cleanup().await
    }

    #[tokio::test]
    async fn keyed_rebuild_projects_resolver_scope_provenance_and_chain_positions() -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x7300);

        seed_resources(database.pool(), &[resource_id]).await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xperm0078", 120, 1_776_100_120),
                raw_block("ethereum-mainnet", "0xperm0079", 121, 1_776_100_121),
            ],
        )
        .await?;
        seed_permission_events(
            database.pool(),
            &[
                permission_event(
                    "resolver-grant-1",
                    resource_id,
                    "0x0000000000000000000000000000000000000abc",
                    json!({
                        "kind": "resolver",
                        "chain_id": "ethereum-mainnet",
                        "resolver_address": "0x0000000000000000000000000000000000000dEf"
                    }),
                    json!(["set_resolver"]),
                    Some(json!({"kind": "normalized_event", "normalized_event_id": 20})),
                    None,
                    120,
                    0,
                ),
                permission_event(
                    "resolver-grant-2",
                    resource_id,
                    "0x0000000000000000000000000000000000000abc",
                    json!({
                        "kind": "resolver",
                        "chain_id": "ethereum-mainnet",
                        "resolver_address": "0x0000000000000000000000000000000000000def"
                    }),
                    json!(["set_resolver", "set_records"]),
                    Some(json!({"kind": "normalized_event", "normalized_event_id": 21})),
                    None,
                    121,
                    0,
                ),
            ],
        )
        .await?;

        let summary =
            rebuild_permissions_current(database.pool(), Some(&resource_id.to_string())).await?;
        assert_eq!(summary.upserted_row_count, 1);

        let rows = load_permissions_current(database.pool(), resource_id, None, None).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].scope,
            PermissionScope::Resolver {
                chain_id: "ethereum-mainnet".to_owned(),
                resolver_address: "0x0000000000000000000000000000000000000def".to_owned(),
            }
        );
        assert_eq!(rows[0].provenance["normalized_event_ids"], json!([1, 2]));
        assert_eq!(
            rows[0].chain_positions["ethereum-mainnet"]["block_number"],
            json!(121)
        );
        assert_eq!(
            rows[0].chain_positions["ethereum-mainnet"]["timestamp"],
            json!(format_timestamp(timestamp(1_776_100_121)))
        );
        assert_eq!(rows[0].last_recomputed_at, timestamp(1_776_100_121));

        database.cleanup().await
    }

    #[tokio::test]
    async fn permissions_current_keyed_rebuild_projects_basenames_resolver_scope_from_permission_changed_rows()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let resource_id = Uuid::from_u128(0x73b0);

        seed_resources(database.pool(), &[resource_id]).await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("base-mainnet", "0xperm008c", 140, 1_776_100_140),
                raw_block("base-mainnet", "0xperm008d", 141, 1_776_100_141),
            ],
        )
        .await?;
        seed_permission_events(
            database.pool(),
            &[
                permission_event_with_context(
                    "basenames-resolver-grant-1",
                    "basenames",
                    "basenames_base_registry",
                    "base-mainnet",
                    3,
                    resource_id,
                    "0x0000000000000000000000000000000000000abc",
                    json!({
                        "kind": "resolver",
                        "chain_id": "base-mainnet",
                        "resolver_address": "0x0000000000000000000000000000000000000AbC"
                    }),
                    json!(["resolver_control"]),
                    Some(json!({"kind": "normalized_event", "normalized_event_id": 40})),
                    None,
                    140,
                    0,
                ),
                permission_event_with_context(
                    "basenames-resolver-grant-2",
                    "basenames",
                    "basenames_base_resolver",
                    "base-mainnet",
                    4,
                    resource_id,
                    "0x0000000000000000000000000000000000000abc",
                    json!({
                        "kind": "resolver",
                        "chain_id": "base-mainnet",
                        "resolver_address": "0x0000000000000000000000000000000000000abc"
                    }),
                    json!(["resolver_control", "resource_control"]),
                    Some(json!({"kind": "normalized_event", "normalized_event_id": 41})),
                    None,
                    141,
                    0,
                ),
            ],
        )
        .await?;

        let summary =
            rebuild_permissions_current(database.pool(), Some(&resource_id.to_string())).await?;
        assert_eq!(summary.upserted_row_count, 1);

        let rows = load_permissions_current(database.pool(), resource_id, None, None).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].scope,
            PermissionScope::Resolver {
                chain_id: "base-mainnet".to_owned(),
                resolver_address: "0x0000000000000000000000000000000000000abc".to_owned(),
            }
        );
        assert_eq!(
            rows[0].effective_powers,
            json!(["resolver_control", "resource_control"])
        );
        assert_eq!(rows[0].provenance["normalized_event_ids"], json!([1, 2]));
        assert_eq!(
            rows[0].coverage["source_classes_considered"],
            json!(["basenames_base_registry", "basenames_base_resolver"])
        );
        assert_eq!(
            rows[0].chain_positions["base-mainnet"]["block_number"],
            json!(141)
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn full_rebuild_clears_stale_rows_and_partitions_by_resource_id() -> Result<()> {
        let database = TestDatabase::new().await?;
        let first_resource_id = Uuid::from_u128(0x7400);
        let second_resource_id = Uuid::from_u128(0x7401);
        let stale_resource_id = Uuid::from_u128(0x74ff);

        seed_resources(
            database.pool(),
            &[first_resource_id, second_resource_id, stale_resource_id],
        )
        .await?;
        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xperm0082", 130, 1_776_100_130),
                raw_block("ethereum-mainnet", "0xperm0083", 131, 1_776_100_131),
            ],
        )
        .await?;
        upsert_permissions_current_rows(
            database.pool(),
            &[PermissionsCurrentRow {
                resource_id: stale_resource_id,
                subject: "0x0000000000000000000000000000000000000bad".to_owned(),
                scope: PermissionScope::Resource,
                effective_powers: json!(["stale"]),
                grant_source: json!({}),
                revocation_source: None,
                inheritance_path: json!([]),
                transfer_behavior: json!({}),
                provenance: json!({"derivation_kind": PERMISSIONS_CURRENT_DERIVATION_KIND}),
                coverage: json!({"enumeration_basis": PERMISSIONS_ENUMERATION_BASIS}),
                chain_positions: json!({}),
                canonicality_summary: json!({"status": "finalized", "chains": {}}),
                manifest_version: 1,
                last_recomputed_at: timestamp(1_776_100_001),
            }],
        )
        .await?;
        seed_permission_events(
            database.pool(),
            &[
                permission_event(
                    "resource-a",
                    first_resource_id,
                    "0x0000000000000000000000000000000000000abc",
                    json!({"kind": "resource"}),
                    json!(["set_records"]),
                    Some(json!({"kind": "normalized_event", "normalized_event_id": 30})),
                    None,
                    130,
                    0,
                ),
                permission_event(
                    "resource-b",
                    second_resource_id,
                    "0x0000000000000000000000000000000000000abc",
                    json!({"kind": "resource"}),
                    json!(["set_records"]),
                    Some(json!({"kind": "normalized_event", "normalized_event_id": 31})),
                    None,
                    131,
                    0,
                ),
            ],
        )
        .await?;

        let summary = rebuild_permissions_current(database.pool(), None).await?;
        assert_eq!(summary.requested_resource_count, 2);
        assert_eq!(summary.upserted_row_count, 2);
        assert_eq!(summary.deleted_row_count, 1);

        let first_rows =
            load_permissions_current(database.pool(), first_resource_id, None, None).await?;
        let second_rows =
            load_permissions_current(database.pool(), second_resource_id, None, None).await?;
        let stale_rows =
            load_permissions_current(database.pool(), stale_resource_id, None, None).await?;
        assert_eq!(first_rows.len(), 1);
        assert_eq!(second_rows.len(), 1);
        assert!(stale_rows.is_empty());
        assert_ne!(first_rows[0].resource_id, second_rows[0].resource_id);
        assert_eq!(first_rows[0].provenance["normalized_event_ids"], json!([1]));
        assert_eq!(
            second_rows[0].provenance["normalized_event_ids"],
            json!([2])
        );

        database.cleanup().await
    }

    async fn seed_resources(pool: &PgPool, resource_ids: &[Uuid]) -> Result<()> {
        let resources = resource_ids
            .iter()
            .enumerate()
            .map(|(index, resource_id)| Resource {
                resource_id: *resource_id,
                token_lineage_id: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: format!("0xresource{index:02x}"),
                block_number: 20_000 + index as i64,
                provenance: json!({"source": "worker_permissions_current_test"}),
                canonicality_state: CanonicalityState::Finalized,
            })
            .collect::<Vec<_>>();
        upsert_resources(pool, &resources).await?;
        Ok(())
    }

    async fn seed_raw_blocks(pool: &PgPool, blocks: &[RawBlock]) -> Result<()> {
        upsert_raw_blocks(pool, blocks).await?;
        Ok(())
    }

    async fn seed_permission_events(pool: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
        upsert_normalized_events(pool, events).await?;
        Ok(())
    }

    fn raw_block(
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
        unix_timestamp: i64,
    ) -> RawBlock {
        RawBlock {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: None,
            block_number,
            block_timestamp: timestamp(unix_timestamp),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn permission_event(
        event_identity: &str,
        resource_id: Uuid,
        subject: &str,
        scope: Value,
        effective_powers: Value,
        grant_source: Option<Value>,
        revocation_source: Option<Value>,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        permission_event_with_context(
            event_identity,
            "ens",
            "ens_v1_unwrapped_authority",
            "ethereum-mainnet",
            1,
            resource_id,
            subject,
            scope,
            effective_powers,
            grant_source,
            revocation_source,
            block_number,
            log_index,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn permission_event_with_context(
        event_identity: &str,
        namespace: &str,
        source_family: &str,
        chain_id: &str,
        manifest_version: i64,
        resource_id: Uuid,
        subject: &str,
        scope: Value,
        effective_powers: Value,
        grant_source: Option<Value>,
        revocation_source: Option<Value>,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: namespace.to_owned(),
            logical_name_id: Some(format!("{namespace}:{resource_id}")),
            resource_id: Some(resource_id),
            event_kind: EVENT_KIND_PERMISSION_CHANGED.to_owned(),
            source_family: source_family.to_owned(),
            manifest_version,
            source_manifest_id: None,
            chain_id: Some(chain_id.to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xperm{block_number:04x}")),
            transaction_hash: Some(format!("0xtx{block_number:04x}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": chain_id,
                "block_number": block_number,
                "log_index": log_index
            }),
            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "subject": subject,
                "scope": scope,
                "effective_powers": effective_powers,
                "grant_source": grant_source,
                "revocation_source": revocation_source,
                "inheritance_path": [{
                    "kind": "resource_authority",
                    "resource_id": resource_id
                }],
                "transfer_behavior": {
                    "kind": "resource_rebound"
                }
            }),
        }
    }

    fn timestamp(value: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(value).expect("timestamp must be valid")
    }
}
