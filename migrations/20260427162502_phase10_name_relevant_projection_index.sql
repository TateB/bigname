-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_name_relevant_projection_idx
  ON normalized_events (
    logical_name_id,
    block_number ASC NULLS FIRST,
    log_index ASC NULLS LAST,
    event_identity ASC
  )
  WHERE logical_name_id IS NOT NULL
    AND canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
    );
