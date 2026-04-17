-- Phase 3 normalized-event foundation for adapter-owned replay inputs.

CREATE TABLE normalized_events (
  normalized_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_identity TEXT NOT NULL,
  namespace TEXT NOT NULL,
  logical_name_id TEXT,
  resource_id UUID,
  event_kind TEXT NOT NULL,
  source_family TEXT NOT NULL,
  manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
  source_manifest_id BIGINT REFERENCES manifest_versions (manifest_id) ON DELETE SET NULL,
  chain_id TEXT,
  block_number BIGINT CHECK (block_number IS NULL OR block_number >= 0),
  block_hash TEXT,
  transaction_hash TEXT,
  log_index BIGINT CHECK (log_index IS NULL OR log_index >= 0),
  raw_fact_ref JSONB NOT NULL DEFAULT '{}'::JSONB,
  derivation_kind TEXT NOT NULL,
  canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
  before_state JSONB NOT NULL DEFAULT '{}'::JSONB,
  after_state JSONB NOT NULL DEFAULT '{}'::JSONB,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (event_identity),
  CHECK ((block_hash IS NULL) = (block_number IS NULL)),
  CHECK (transaction_hash IS NOT NULL OR log_index IS NULL)
);

CREATE INDEX normalized_events_namespace_idx
  ON normalized_events (namespace, normalized_event_id DESC);

CREATE INDEX normalized_events_kind_idx
  ON normalized_events (event_kind, normalized_event_id DESC);

CREATE INDEX normalized_events_manifest_idx
  ON normalized_events (source_manifest_id, event_kind, normalized_event_id DESC)
  WHERE source_manifest_id IS NOT NULL;

CREATE INDEX normalized_events_chain_position_idx
  ON normalized_events (chain_id, block_number DESC, normalized_event_id DESC)
  WHERE block_number IS NOT NULL;
