use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};
use uuid::Uuid;

use crate::CanonicalityState;

/// Persisted adapter-owned normalized event used to rebuild projections.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedEvent {
    pub event_identity: String,
    pub namespace: String,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub event_kind: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: Option<String>,
    pub block_number: Option<i64>,
    pub block_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub log_index: Option<i64>,
    pub raw_fact_ref: Value,
    pub derivation_kind: String,
    pub canonicality_state: CanonicalityState,
    pub before_state: Value,
    pub after_state: Value,
}

/// Insert missing normalized events or refresh canonicality for existing rows.
pub async fn upsert_normalized_events(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<Vec<NormalizedEvent>> {
    if events.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for normalized-event upsert")?;

    let mut snapshots = Vec::with_capacity(events.len());
    for event in events {
        validate_normalized_event(event)?;
        snapshots.push(upsert_normalized_event(&mut transaction, event).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit normalized-event upsert")?;

    Ok(snapshots)
}

/// Load normalized events for one namespace in insertion order.
pub async fn load_normalized_events_by_namespace(
    pool: &PgPool,
    namespace: &str,
) -> Result<Vec<NormalizedEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state::TEXT AS canonicality_state,
            before_state,
            after_state
        FROM normalized_events
        WHERE namespace = $1
        ORDER BY normalized_event_id
        "#,
    )
    .bind(namespace)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load normalized events for namespace {namespace}"))?;

    rows.into_iter().map(decode_normalized_event).collect()
}

/// Count normalized events by kind for one namespace.
pub async fn load_normalized_event_counts_by_kind(
    pool: &PgPool,
    namespace: &str,
) -> Result<BTreeMap<String, usize>> {
    let rows = sqlx::query(
        r#"
        SELECT event_kind, COUNT(*)::BIGINT AS event_count
        FROM normalized_events
        WHERE namespace = $1
        GROUP BY event_kind
        ORDER BY event_kind
        "#,
    )
    .bind(namespace)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load normalized-event counts for namespace {namespace}"))?;

    rows.into_iter()
        .map(|row| {
            let event_kind = row
                .try_get::<String, _>("event_kind")
                .context("missing event_kind from normalized-event count row")?;
            let event_count = row
                .try_get::<i64, _>("event_count")
                .context("missing event_count from normalized-event count row")?;
            Ok((
                event_kind,
                usize::try_from(event_count)
                    .context("normalized-event count does not fit in usize")?,
            ))
        })
        .collect()
}

/// Mark block-derived normalized events on a losing branch `orphaned` until
/// `stop_before_hash` is reached.
pub async fn mark_block_derived_normalized_events_range_orphaned(
    pool: &PgPool,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<u64> {
    if stop_before_hash == Some(from_hash) {
        return Ok(0);
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for normalized-event orphaning")?;

    let block_hashes =
        load_raw_block_hash_path(&mut *transaction, chain_id, from_hash, stop_before_hash)
            .await
            .with_context(|| {
                format!(
                    "failed to load raw block hash path for normalized events on chain {chain_id} starting from block {from_hash}"
                )
            })?;
    if block_hashes.is_empty() {
        bail!("missing stored raw block for chain {chain_id} block {from_hash}");
    }

    let updated_count = sqlx::query(
        r#"
        UPDATE normalized_events
        SET
            canonicality_state = 'orphaned'::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state <> 'orphaned'::canonicality_state
        "#,
    )
    .bind(chain_id)
    .bind(&block_hashes)
    .execute(&mut *transaction)
    .await
    .with_context(|| {
        format!("failed to mark block-derived normalized events orphaned for chain {chain_id}")
    })?
    .rows_affected();

    transaction
        .commit()
        .await
        .context("failed to commit normalized-event orphaning")?;

    Ok(updated_count)
}

async fn upsert_normalized_event(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    event: &NormalizedEvent,
) -> Result<NormalizedEvent> {
    let raw_fact_ref = serde_json::to_string(&event.raw_fact_ref)
        .context("failed to serialize normalized-event raw_fact_ref")?;
    let before_state = serde_json::to_string(&event.before_state)
        .context("failed to serialize normalized-event before_state")?;
    let after_state = serde_json::to_string(&event.after_state)
        .context("failed to serialize normalized-event after_state")?;

    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state,
            before_state,
            after_state
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14::jsonb,
            $15,
            $16::canonicality_state,
            $17::jsonb,
            $18::jsonb
        )
        ON CONFLICT (event_identity) DO NOTHING
        RETURNING
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state::TEXT AS canonicality_state,
            before_state,
            after_state
        "#,
    )
    .bind(&event.event_identity)
    .bind(&event.namespace)
    .bind(&event.logical_name_id)
    .bind(event.resource_id)
    .bind(&event.event_kind)
    .bind(&event.source_family)
    .bind(event.manifest_version)
    .bind(event.source_manifest_id)
    .bind(&event.chain_id)
    .bind(event.block_number)
    .bind(&event.block_hash)
    .bind(&event.transaction_hash)
    .bind(event.log_index)
    .bind(raw_fact_ref)
    .bind(&event.derivation_kind)
    .bind(event.canonicality_state.as_str())
    .bind(before_state)
    .bind(after_state)
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert normalized event {} ({})",
            event.event_identity, event.event_kind
        )
    })? {
        return decode_normalized_event(snapshot);
    }

    let existing = load_normalized_event_internal(&mut **executor, &event.event_identity)
        .await?
        .with_context(|| {
            format!(
                "failed to reload existing normalized event {} after insert conflict",
                event.event_identity
            )
        })?;

    ensure_normalized_event_identity_matches(&existing, event)?;
    let next_state = merge_canonicality(existing.canonicality_state, event.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE normalized_events
        SET
            canonicality_state = $2::canonicality_state,
            observed_at = now()
        WHERE event_identity = $1
        RETURNING
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state::TEXT AS canonicality_state,
            before_state,
            after_state
        "#,
    )
    .bind(&event.event_identity)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh existing normalized event {} ({})",
            event.event_identity, event.event_kind
        )
    })?;

    decode_normalized_event(snapshot)
}

async fn load_normalized_event_internal<'e, E>(
    executor: E,
    event_identity: &str,
) -> Result<Option<NormalizedEvent>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state::TEXT AS canonicality_state,
            before_state,
            after_state
        FROM normalized_events
        WHERE event_identity = $1
        "#,
    )
    .bind(event_identity)
    .fetch_optional(executor)
    .await
    .with_context(|| format!("failed to load normalized event {event_identity}"))?;

    row.map(decode_normalized_event).transpose()
}

fn validate_normalized_event(event: &NormalizedEvent) -> Result<()> {
    if event.event_identity.is_empty() {
        bail!("normalized event has empty event_identity");
    }
    if event.namespace.is_empty() {
        bail!(
            "normalized event {} has empty namespace",
            event.event_identity
        );
    }
    if event.event_kind.is_empty() {
        bail!(
            "normalized event {} has empty event_kind",
            event.event_identity
        );
    }
    if event.source_family.is_empty() {
        bail!(
            "normalized event {} has empty source_family",
            event.event_identity
        );
    }
    if event.derivation_kind.is_empty() {
        bail!(
            "normalized event {} has empty derivation_kind",
            event.event_identity
        );
    }
    if event.manifest_version <= 0 {
        bail!(
            "normalized event {} has non-positive manifest_version {}",
            event.event_identity,
            event.manifest_version
        );
    }
    if event.block_number.is_some() != event.block_hash.is_some() {
        bail!(
            "normalized event {} must set block_number and block_hash together",
            event.event_identity
        );
    }
    if let Some(block_number) = event.block_number
        && block_number < 0
    {
        bail!(
            "normalized event {} has negative block_number {}",
            event.event_identity,
            block_number
        );
    }
    if let Some(log_index) = event.log_index {
        if log_index < 0 {
            bail!(
                "normalized event {} has negative log_index {}",
                event.event_identity,
                log_index
            );
        }
        if event.transaction_hash.is_none() {
            bail!(
                "normalized event {} has log_index without transaction_hash",
                event.event_identity
            );
        }
    }

    Ok(())
}

fn ensure_normalized_event_identity_matches(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
) -> Result<()> {
    if existing.namespace != incoming.namespace
        || existing.logical_name_id != incoming.logical_name_id
        || existing.resource_id != incoming.resource_id
        || existing.event_kind != incoming.event_kind
        || existing.source_family != incoming.source_family
        || existing.manifest_version != incoming.manifest_version
        || existing.source_manifest_id != incoming.source_manifest_id
        || existing.chain_id != incoming.chain_id
        || existing.block_number != incoming.block_number
        || existing.block_hash != incoming.block_hash
        || existing.transaction_hash != incoming.transaction_hash
        || existing.log_index != incoming.log_index
        || existing.raw_fact_ref != incoming.raw_fact_ref
        || existing.derivation_kind != incoming.derivation_kind
        || existing.before_state != incoming.before_state
        || existing.after_state != incoming.after_state
    {
        bail!(
            "normalized event identity mismatch for event {}",
            existing.event_identity
        );
    }

    Ok(())
}

fn merge_canonicality(
    current: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    match incoming {
        CanonicalityState::Orphaned => CanonicalityState::Orphaned,
        CanonicalityState::Observed => {
            if current == CanonicalityState::Orphaned {
                CanonicalityState::Observed
            } else {
                current
            }
        }
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized => {
            if current == CanonicalityState::Orphaned {
                incoming
            } else {
                current.promote_to(incoming)
            }
        }
    }
}

fn decode_normalized_event(row: PgRow) -> Result<NormalizedEvent> {
    Ok(NormalizedEvent {
        event_identity: row
            .try_get("event_identity")
            .context("missing event_identity")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        event_kind: row.try_get("event_kind").context("missing event_kind")?,
        source_family: row
            .try_get("source_family")
            .context("missing source_family")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        source_manifest_id: row
            .try_get("source_manifest_id")
            .context("missing source_manifest_id")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        transaction_hash: row
            .try_get("transaction_hash")
            .context("missing transaction_hash")?,
        log_index: row.try_get("log_index").context("missing log_index")?,
        raw_fact_ref: row
            .try_get("raw_fact_ref")
            .context("missing raw_fact_ref")?,
        derivation_kind: row
            .try_get("derivation_kind")
            .context("missing derivation_kind")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
        before_state: row
            .try_get("before_state")
            .context("missing before_state")?,
        after_state: row.try_get("after_state").context("missing after_state")?,
    })
}

async fn load_raw_block_hash_path<'e, E>(
    executor: E,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<Vec<String>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        WITH RECURSIVE raw_block_path AS (
            SELECT block_hash, parent_hash, 0 AS depth
            FROM raw_blocks
            WHERE chain_id = $1
              AND block_hash = $2

            UNION ALL

            SELECT parent.block_hash, parent.parent_hash, raw_block_path.depth + 1
            FROM raw_blocks parent
            JOIN raw_block_path
              ON parent.chain_id = $1
             AND parent.block_hash = raw_block_path.parent_hash
            WHERE $3::TEXT IS NULL
               OR parent.block_hash <> $3::TEXT
        )
        SELECT block_hash
        FROM raw_block_path
        ORDER BY depth
        "#,
    )
    .bind(chain_id)
    .bind(from_hash)
    .bind(stop_before_hash)
    .fetch_all(executor)
    .await?;

    rows.into_iter()
        .map(|row| {
            row.try_get::<String, _>("block_hash")
                .context("missing block_hash in raw block path")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
    use serde_json::json;
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };

    use super::*;
    use crate::{RawBlock, default_database_url, upsert_raw_blocks};

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
                .context("failed to parse database URL for normalized-event tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_storage_normalized_event_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for normalized-event tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect normalized-event test pool")?;

            crate::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for normalized-event tests")?;

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

    fn normalized_event(
        event_identity: &str,
        event_kind: &str,
        state: CanonicalityState,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: event_kind.to_owned(),
            source_family: "ens_v2_registry_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: None,
            block_hash: None,
            transaction_hash: None,
            log_index: None,
            raw_fact_ref: json!({}),
            derivation_kind: "manifest_sync".to_owned(),
            canonicality_state: state,
            before_state: json!({}),
            after_state: json!({"key": event_identity}),
        }
    }

    #[tokio::test]
    async fn upserts_and_loads_normalized_events() -> Result<()> {
        let database = TestDatabase::new().await?;

        let inserted = upsert_normalized_events(
            database.pool(),
            &[
                normalized_event(
                    "manifest:1:source_manifest",
                    "SourceManifestUpdated",
                    CanonicalityState::Finalized,
                ),
                normalized_event(
                    "manifest:1:capability:verified_resolution",
                    "CapabilityChanged",
                    CanonicalityState::Finalized,
                ),
            ],
        )
        .await?;
        assert_eq!(inserted.len(), 2);

        let loaded = load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(loaded, inserted);

        let counts = load_normalized_event_counts_by_kind(database.pool(), "ens").await?;
        assert_eq!(
            counts,
            BTreeMap::from([
                ("CapabilityChanged".to_owned(), 1_usize),
                ("SourceManifestUpdated".to_owned(), 1_usize),
            ])
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn normalized_event_upsert_rejects_identity_mismatch() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_normalized_events(
            database.pool(),
            &[normalized_event(
                "manifest:1:source_manifest",
                "SourceManifestUpdated",
                CanonicalityState::Finalized,
            )],
        )
        .await?;

        let mut conflicting = normalized_event(
            "manifest:1:source_manifest",
            "SourceManifestUpdated",
            CanonicalityState::Finalized,
        );
        conflicting.after_state = json!({"key": "different"});
        let error = upsert_normalized_events(database.pool(), &[conflicting])
            .await
            .expect_err("normalized-event identity mismatch must fail");

        assert!(
            error.to_string().contains(
                "normalized event identity mismatch for event manifest:1:source_manifest"
            ),
            "unexpected error: {error:#}"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn normalized_event_upsert_promotes_canonicality() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_normalized_events(
            database.pool(),
            &[normalized_event(
                "manifest:1:source_manifest",
                "SourceManifestUpdated",
                CanonicalityState::Canonical,
            )],
        )
        .await?;

        let promoted = upsert_normalized_events(
            database.pool(),
            &[normalized_event(
                "manifest:1:source_manifest",
                "SourceManifestUpdated",
                CanonicalityState::Finalized,
            )],
        )
        .await?;

        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0].canonicality_state, CanonicalityState::Finalized);

        database.cleanup().await
    }

    #[tokio::test]
    async fn orphan_range_marks_block_derived_normalized_events_orphaned() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_raw_blocks(
            database.pool(),
            &[
                RawBlock {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: "0x001".to_owned(),
                    parent_hash: None,
                    block_number: 1,
                    block_timestamp: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
                    logs_bloom: None,
                    transactions_root: None,
                    receipts_root: None,
                    state_root: None,
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawBlock {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: "0x002".to_owned(),
                    parent_hash: Some("0x001".to_owned()),
                    block_number: 2,
                    block_timestamp: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
                    logs_bloom: None,
                    transactions_root: None,
                    receipts_root: None,
                    state_root: None,
                    canonicality_state: CanonicalityState::Canonical,
                },
            ],
        )
        .await?;

        upsert_normalized_events(
            database.pool(),
            &[
                NormalizedEvent {
                    chain_id: Some("ethereum-mainnet".to_owned()),
                    block_number: Some(1),
                    block_hash: Some("0x001".to_owned()),
                    transaction_hash: Some("0xtx1".to_owned()),
                    log_index: Some(0),
                    event_identity: "preimage:0x001:0".to_owned(),
                    event_kind: "PreimageObserved".to_owned(),
                    ..normalized_event(
                        "preimage:0x001:0",
                        "PreimageObserved",
                        CanonicalityState::Canonical,
                    )
                },
                NormalizedEvent {
                    chain_id: Some("ethereum-mainnet".to_owned()),
                    block_number: Some(2),
                    block_hash: Some("0x002".to_owned()),
                    transaction_hash: Some("0xtx2".to_owned()),
                    log_index: Some(1),
                    event_identity: "preimage:0x002:1".to_owned(),
                    event_kind: "PreimageObserved".to_owned(),
                    ..normalized_event(
                        "preimage:0x002:1",
                        "PreimageObserved",
                        CanonicalityState::Finalized,
                    )
                },
            ],
        )
        .await?;

        let orphaned_count = mark_block_derived_normalized_events_range_orphaned(
            database.pool(),
            "ethereum-mainnet",
            "0x002",
            Some("0x001"),
        )
        .await?;
        assert_eq!(orphaned_count, 1);

        let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].canonicality_state, CanonicalityState::Canonical);
        assert_eq!(events[1].canonicality_state, CanonicalityState::Orphaned);

        database.cleanup().await
    }
}
