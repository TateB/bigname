-- no-transaction

-- ENSv1 unwrapped-authority replay resolves release boundaries by timestamp.
-- Keep those bounded timestamp seeks off raw_blocks table scans during
-- historical raw-fact replay.
CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_blocks_chain_timestamp_canonical_idx
  ON raw_blocks (chain_id, block_timestamp, block_number)
  INCLUDE (block_hash, canonicality_state)
  WHERE canonicality_state IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  );
