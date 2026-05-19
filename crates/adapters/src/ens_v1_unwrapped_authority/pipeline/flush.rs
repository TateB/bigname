use super::*;
use crate::normalized_event_support::count_events_by_kind;

const REPLAY_EVENT_FLUSH_BATCH_SIZE: usize = 20_000;

pub(super) async fn flush_staged_replay_events(
    pool: &PgPool,
    histories: &mut BTreeMap<String, NameHistory>,
    reverse_histories: &mut BTreeMap<String, ReverseClaimSourceHistory>,
    checkpoint_delta: &mut UnwrappedAuthorityReplayCheckpointDelta,
    flushed_events: &mut UnwrappedAuthorityReplayFlushedEvents,
) -> Result<usize> {
    let mut flushed_count = 0usize;
    let mut buffer = Vec::with_capacity(REPLAY_EVENT_FLUSH_BATCH_SIZE);

    let history_keys = histories.keys().cloned().collect::<Vec<_>>();
    for key in history_keys {
        if let Some(history) = histories.get_mut(&key) {
            if !history.events.is_empty() {
                checkpoint_delta.mark_history(key);
                buffer.append(&mut history.events);
            }
        }
        flushed_count += flush_replay_event_buffer(pool, &mut buffer, flushed_events).await?;
    }

    let reverse_history_keys = reverse_histories.keys().cloned().collect::<Vec<_>>();
    for key in reverse_history_keys {
        if let Some(history) = reverse_histories.get_mut(&key) {
            if !history.events.is_empty() {
                checkpoint_delta.mark_reverse_history(key);
                buffer.append(&mut history.events);
            }
        }
        flushed_count += flush_replay_event_buffer(pool, &mut buffer, flushed_events).await?;
    }

    flushed_count += flush_replay_event_buffer_now(pool, &mut buffer, flushed_events).await?;
    Ok(flushed_count)
}

async fn flush_replay_event_buffer(
    pool: &PgPool,
    buffer: &mut Vec<NormalizedEvent>,
    flushed_events: &mut UnwrappedAuthorityReplayFlushedEvents,
) -> Result<usize> {
    if buffer.len() < REPLAY_EVENT_FLUSH_BATCH_SIZE {
        return Ok(0);
    }
    flush_replay_event_buffer_now(pool, buffer, flushed_events).await
}

pub(super) async fn flush_replay_event_buffer_now(
    pool: &PgPool,
    buffer: &mut Vec<NormalizedEvent>,
    flushed_events: &mut UnwrappedAuthorityReplayFlushedEvents,
) -> Result<usize> {
    if buffer.is_empty() {
        return Ok(0);
    }

    let event_count = buffer.len();
    merge_event_kind_counts(&mut flushed_events.by_kind, count_events_by_kind(buffer));
    let inserted_count = upsert_normalized_events_count_only(pool, buffer).await?;
    flushed_events.total_count += event_count;
    flushed_events.inserted_count += inserted_count;
    buffer.clear();
    if buffer.capacity() > REPLAY_EVENT_FLUSH_BATCH_SIZE * 4 {
        buffer.shrink_to(REPLAY_EVENT_FLUSH_BATCH_SIZE);
    }
    Ok(event_count)
}

pub(super) fn merge_event_kind_counts(
    target: &mut BTreeMap<String, usize>,
    source: BTreeMap<String, usize>,
) {
    for (kind, count) in source {
        *target.entry(kind).or_insert(0) += count;
    }
}
