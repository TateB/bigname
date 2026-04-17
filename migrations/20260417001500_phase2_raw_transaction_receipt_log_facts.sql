-- Phase 2 storage foundation: exact raw transaction, receipt, and log facts.

CREATE TABLE raw_transactions (
  raw_transaction_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chain_id TEXT NOT NULL,
  block_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL CHECK (block_number >= 0),
  transaction_hash TEXT NOT NULL,
  transaction_index BIGINT NOT NULL CHECK (transaction_index >= 0),
  from_address TEXT NOT NULL,
  to_address TEXT,
  canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (chain_id, block_hash, transaction_index)
);

CREATE INDEX raw_transactions_by_hash_idx
  ON raw_transactions (chain_id, transaction_hash);

CREATE INDEX raw_transactions_by_state_idx
  ON raw_transactions (chain_id, canonicality_state, block_number DESC, transaction_index DESC);

CREATE TABLE raw_receipts (
  raw_receipt_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chain_id TEXT NOT NULL,
  block_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL CHECK (block_number >= 0),
  transaction_hash TEXT NOT NULL,
  transaction_index BIGINT NOT NULL CHECK (transaction_index >= 0),
  contract_address TEXT,
  status BOOLEAN,
  gas_used BIGINT CHECK (gas_used >= 0),
  cumulative_gas_used BIGINT CHECK (cumulative_gas_used >= 0),
  logs_bloom BYTEA,
  canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (chain_id, block_hash, transaction_index)
);

CREATE INDEX raw_receipts_by_hash_idx
  ON raw_receipts (chain_id, transaction_hash);

CREATE INDEX raw_receipts_by_state_idx
  ON raw_receipts (chain_id, canonicality_state, block_number DESC, transaction_index DESC);

CREATE TABLE raw_logs (
  raw_log_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chain_id TEXT NOT NULL,
  block_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL CHECK (block_number >= 0),
  transaction_hash TEXT NOT NULL,
  transaction_index BIGINT NOT NULL CHECK (transaction_index >= 0),
  log_index BIGINT NOT NULL CHECK (log_index >= 0),
  emitting_address TEXT NOT NULL,
  topics TEXT[] NOT NULL DEFAULT '{}',
  data BYTEA NOT NULL DEFAULT '\x',
  canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (chain_id, block_hash, log_index)
);

CREATE INDEX raw_logs_by_tx_idx
  ON raw_logs (chain_id, transaction_hash, log_index);

CREATE INDEX raw_logs_by_emitter_idx
  ON raw_logs (chain_id, emitting_address, block_number DESC, log_index DESC);

CREATE INDEX raw_logs_by_state_idx
  ON raw_logs (chain_id, canonicality_state, block_number DESC, log_index DESC);
