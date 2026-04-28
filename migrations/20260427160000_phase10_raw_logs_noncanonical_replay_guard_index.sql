CREATE INDEX IF NOT EXISTS raw_logs_noncanonical_replay_guard_idx
  ON raw_logs (chain_id, block_hash)
  WHERE canonicality_state NOT IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  );
