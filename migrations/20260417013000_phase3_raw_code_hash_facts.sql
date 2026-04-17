-- Phase 3 intake: exact raw code-hash observations for watched contracts.

CREATE TABLE raw_code_hashes (
  raw_code_hash_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chain_id TEXT NOT NULL,
  block_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL CHECK (block_number >= 0),
  contract_address TEXT NOT NULL,
  code_hash TEXT NOT NULL,
  code_byte_length BIGINT NOT NULL CHECK (code_byte_length >= 0),
  canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (chain_id, block_hash, contract_address)
);

CREATE INDEX raw_code_hashes_by_contract_idx
  ON raw_code_hashes (chain_id, contract_address, block_number DESC);

CREATE INDEX raw_code_hashes_by_state_idx
  ON raw_code_hashes (chain_id, canonicality_state, block_number DESC, contract_address);
