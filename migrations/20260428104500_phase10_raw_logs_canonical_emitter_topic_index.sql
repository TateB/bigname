-- no-transaction

-- Speed up scoped normalized replay lookups that need "this emitter emitted
-- this topic in this block interval" without scanning all logs for the emitter.
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_logs_canonical_emitter_topic_block_idx
  ON raw_logs (chain_id, emitting_address, (topics[1]), block_number, log_index)
  WHERE canonicality_state IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  );
