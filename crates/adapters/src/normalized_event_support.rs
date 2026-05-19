use std::{
    collections::{BTreeMap, HashSet},
    time::Instant,
};

use anyhow::{Context, Result, bail};
use bigname_storage::{NormalizedEvent, upsert_normalized_events};
use sqlx::PgPool;
use tracing::info;

pub(crate) struct NormalizedEventSyncCounts {
    pub(crate) synced_by_kind: BTreeMap<String, usize>,
    pub(crate) inserted_by_kind: BTreeMap<String, usize>,
    pub(crate) total_synced_count: usize,
    pub(crate) total_inserted_count: usize,
}

impl NormalizedEventSyncCounts {
    pub(crate) fn into_parts_by_kind<T>(
        self,
        build_kind_summary: impl Fn(usize, usize) -> T,
    ) -> (usize, usize, BTreeMap<String, T>) {
        let Self {
            synced_by_kind,
            inserted_by_kind,
            total_synced_count,
            total_inserted_count,
        } = self;
        let by_kind = synced_by_kind
            .into_iter()
            .map(|(event_kind, synced_count)| {
                let inserted_count = inserted_by_kind.get(&event_kind).copied().unwrap_or(0);
                (event_kind, build_kind_summary(synced_count, inserted_count))
            })
            .collect();

        (total_synced_count, total_inserted_count, by_kind)
    }
}

pub(crate) async fn load_existing_event_identities(
    pool: &PgPool,
    events: &[NormalizedEvent],
    adapter_label: &str,
) -> Result<HashSet<String>> {
    if events.is_empty() {
        return Ok(HashSet::new());
    }

    let event_identities = events
        .iter()
        .map(|event| event.event_identity.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query_scalar::<_, String>(
        r#"
        SELECT event_identity
        FROM normalized_events
        WHERE event_identity = ANY($1::TEXT[])
        "#,
    )
    .bind(event_identities)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load existing {adapter_label} event identities"))?;

    Ok(rows.into_iter().collect())
}

pub(crate) async fn upsert_normalized_events_with_counts(
    pool: &PgPool,
    events: &[NormalizedEvent],
    adapter_label: &str,
) -> Result<NormalizedEventSyncCounts> {
    let total_started = Instant::now();
    let count_started = Instant::now();
    let counts = count_normalized_event_sync(pool, events, adapter_label).await?;
    let count_existing_ms = count_started.elapsed().as_millis();
    let total_synced_count = counts.total_synced_count;
    let total_inserted_count = counts.total_inserted_count;
    let synced_by_kind = counts.synced_by_kind.clone();
    let inserted_by_kind = counts.inserted_by_kind.clone();

    let upsert_started = Instant::now();
    upsert_normalized_events(pool, events).await?;
    let upsert_ms = upsert_started.elapsed().as_millis();
    info!(
        service = "adapters",
        adapter = adapter_label,
        normalized_event_count = events.len(),
        total_synced_count,
        total_inserted_count,
        synced_by_kind = ?synced_by_kind,
        inserted_by_kind = ?inserted_by_kind,
        count_existing_ms,
        upsert_ms,
        elapsed_ms = total_started.elapsed().as_millis(),
        "adapter normalized-event persistence timing completed"
    );
    Ok(counts)
}

pub(crate) async fn upsert_normalized_events_in_chunks_with_counts(
    pool: &PgPool,
    events: &[NormalizedEvent],
    adapter_label: &str,
    chunk_size: usize,
) -> Result<NormalizedEventSyncCounts> {
    if chunk_size == 0 {
        bail!("normalized event upsert chunk size must be positive");
    }

    let total_started = Instant::now();
    let count_started = Instant::now();
    let counts = count_normalized_event_sync(pool, events, adapter_label).await?;
    let count_existing_ms = count_started.elapsed().as_millis();
    let total_synced_count = counts.total_synced_count;
    let total_inserted_count = counts.total_inserted_count;
    let synced_by_kind = counts.synced_by_kind.clone();
    let inserted_by_kind = counts.inserted_by_kind.clone();
    let mut upsert_ms = 0u128;
    let mut chunk_count = 0usize;
    for chunk in events.chunks(chunk_size) {
        chunk_count += 1;
        let upsert_started = Instant::now();
        upsert_normalized_events(pool, chunk).await?;
        upsert_ms += upsert_started.elapsed().as_millis();
    }
    info!(
        service = "adapters",
        adapter = adapter_label,
        normalized_event_count = events.len(),
        total_synced_count,
        total_inserted_count,
        synced_by_kind = ?synced_by_kind,
        inserted_by_kind = ?inserted_by_kind,
        chunk_size,
        chunk_count,
        count_existing_ms,
        upsert_ms,
        elapsed_ms = total_started.elapsed().as_millis(),
        "adapter chunked normalized-event persistence timing completed"
    );
    Ok(counts)
}

async fn count_normalized_event_sync(
    pool: &PgPool,
    events: &[NormalizedEvent],
    adapter_label: &str,
) -> Result<NormalizedEventSyncCounts> {
    let existing_event_identities =
        load_existing_event_identities(pool, events, adapter_label).await?;
    let inserted_by_kind = count_inserted_events_by_kind(events, &existing_event_identities);
    let synced_by_kind = count_events_by_kind(events);

    Ok(NormalizedEventSyncCounts {
        synced_by_kind,
        total_synced_count: events.len(),
        total_inserted_count: inserted_by_kind.values().sum(),
        inserted_by_kind,
    })
}

pub(crate) fn count_events_by_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

pub(crate) fn count_inserted_events_by_kind(
    events: &[NormalizedEvent],
    existing_event_identities: &HashSet<String>,
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events
        .iter()
        .filter(|event| !existing_event_identities.contains(&event.event_identity))
    {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}
