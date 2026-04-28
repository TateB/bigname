-- Recreate secondary raw-fact indexes that can be dropped during raw-only
-- historical backfill. Run these after the raw backfill finishes and before
-- normalized replay or audit-heavy API traffic.
--
-- These are intentionally not migrations: they are operational bootstrap
-- helpers for a database that already has the migrated schema.

CREATE INDEX CONCURRENTLY IF NOT EXISTS chain_lineage_by_number_idx
  ON public.chain_lineage USING btree (chain_id, block_number DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS chain_lineage_by_state_idx
  ON public.chain_lineage USING btree (chain_id, canonicality_state, block_number DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_blocks_by_number_idx
  ON public.raw_blocks USING btree (chain_id, block_number DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_blocks_by_state_idx
  ON public.raw_blocks USING btree (chain_id, canonicality_state, block_number DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_blocks_chain_timestamp_canonical_idx
  ON public.raw_blocks USING btree (chain_id, block_timestamp, block_number)
  INCLUDE (block_hash, canonicality_state)
  WHERE canonicality_state = ANY (
    ARRAY['canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state]
  );

CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_logs_by_emitter_idx
  ON public.raw_logs USING btree (chain_id, emitting_address, block_number DESC, log_index DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_logs_by_state_idx
  ON public.raw_logs USING btree (chain_id, canonicality_state, block_number DESC, log_index DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_logs_by_tx_idx
  ON public.raw_logs USING btree (chain_id, transaction_hash, log_index);

CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_logs_canonical_replay_position_idx
  ON public.raw_logs USING btree (
    chain_id,
    block_number,
    block_hash,
    transaction_index,
    log_index,
    raw_log_id
  )
  WHERE canonicality_state = ANY (
    ARRAY['canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state]
  );

CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_logs_noncanonical_replay_guard_idx
  ON public.raw_logs USING btree (chain_id, block_hash)
  WHERE canonicality_state <> ALL (
    ARRAY['canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state]
  );

CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_receipts_by_hash_idx
  ON public.raw_receipts USING btree (chain_id, transaction_hash);

CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_receipts_by_state_idx
  ON public.raw_receipts USING btree (
    chain_id,
    canonicality_state,
    block_number DESC,
    transaction_index DESC
  );

CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_transactions_by_hash_idx
  ON public.raw_transactions USING btree (chain_id, transaction_hash);

CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_transactions_by_state_idx
  ON public.raw_transactions USING btree (
    chain_id,
    canonicality_state,
    block_number DESC,
    transaction_index DESC
  );
