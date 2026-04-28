-- no-transaction

-- Projection rebuilds walk canonical normalized events by stable surface and
-- resource anchors. Keep these point reads indexed so full current-state
-- rebuilds do not repeatedly scan the normalized event corpus.
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_name_projection_replay_idx
  ON normalized_events (
    logical_name_id,
    block_number DESC NULLS LAST,
    chain_id ASC NULLS LAST,
    block_hash DESC NULLS LAST,
    transaction_hash DESC NULLS LAST,
    log_index DESC NULLS LAST,
    event_identity DESC
  )
  WHERE logical_name_id IS NOT NULL
    AND canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
    );
