-- no-transaction

-- Support bounded raw-fact normalized-event replay cursor scans without a
-- whole-table raw_logs sort on large backfills.
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_logs_canonical_replay_position_idx
  ON raw_logs (chain_id, block_number, block_hash, transaction_index, log_index, raw_log_id)
  WHERE canonicality_state IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  );
