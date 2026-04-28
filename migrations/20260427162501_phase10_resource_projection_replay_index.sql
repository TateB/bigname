-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_resource_projection_replay_idx
  ON normalized_events (
    resource_id,
    block_number DESC NULLS LAST,
    chain_id ASC NULLS LAST,
    block_hash DESC NULLS LAST,
    transaction_hash DESC NULLS LAST,
    log_index DESC NULLS LAST,
    event_identity DESC
  )
  WHERE resource_id IS NOT NULL
    AND canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
    );
