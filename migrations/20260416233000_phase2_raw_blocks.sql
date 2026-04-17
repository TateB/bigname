-- Phase 2 storage foundation: exact raw block facts for hash-scoped intake.

CREATE TABLE raw_blocks (
  raw_block_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chain_id TEXT NOT NULL,
  block_hash TEXT NOT NULL,
  parent_hash TEXT,
  block_number BIGINT NOT NULL CHECK (block_number >= 0),
  block_timestamp TIMESTAMPTZ NOT NULL,
  logs_bloom BYTEA,
  transactions_root TEXT,
  receipts_root TEXT,
  state_root TEXT,
  canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (chain_id, block_hash)
);

CREATE INDEX raw_blocks_by_number_idx
  ON raw_blocks (chain_id, block_number DESC);

CREATE INDEX raw_blocks_by_state_idx
  ON raw_blocks (chain_id, canonicality_state, block_number DESC);
