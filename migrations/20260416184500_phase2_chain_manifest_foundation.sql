-- Phase 2 storage foundation: chain lineage plus manifest and discovery families.

CREATE TYPE canonicality_state AS ENUM (
  'observed',
  'canonical',
  'safe',
  'finalized',
  'orphaned'
);

CREATE TYPE manifest_rollout_status AS ENUM (
  'draft',
  'shadow',
  'active',
  'deprecated'
);

CREATE TYPE capability_support_status AS ENUM (
  'unsupported',
  'shadow',
  'supported'
);

CREATE TABLE chain_checkpoints (
  chain_id TEXT PRIMARY KEY,
  canonical_block_hash TEXT,
  canonical_block_number BIGINT,
  safe_block_hash TEXT,
  safe_block_number BIGINT,
  finalized_block_hash TEXT,
  finalized_block_number BIGINT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK ((canonical_block_hash IS NULL) = (canonical_block_number IS NULL)),
  CHECK ((safe_block_hash IS NULL) = (safe_block_number IS NULL)),
  CHECK ((finalized_block_hash IS NULL) = (finalized_block_number IS NULL))
);

CREATE TABLE chain_lineage (
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
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (chain_id, block_hash)
);

CREATE INDEX chain_lineage_by_number_idx
  ON chain_lineage (chain_id, block_number DESC);

CREATE INDEX chain_lineage_by_state_idx
  ON chain_lineage (chain_id, canonicality_state, block_number DESC);

CREATE TABLE manifest_versions (
  manifest_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
  namespace TEXT NOT NULL,
  source_family TEXT NOT NULL,
  chain TEXT NOT NULL,
  deployment_epoch TEXT NOT NULL,
  rollout_status manifest_rollout_status NOT NULL,
  normalizer_version TEXT NOT NULL,
  file_path TEXT NOT NULL,
  manifest_payload JSONB NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (namespace, source_family, chain, deployment_epoch, manifest_version),
  UNIQUE (file_path)
);

CREATE INDEX manifest_versions_lookup_idx
  ON manifest_versions (namespace, source_family, chain, rollout_status);

CREATE TABLE manifest_roots (
  manifest_root_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  manifest_id BIGINT NOT NULL REFERENCES manifest_versions (manifest_id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  address TEXT NOT NULL,
  code_hash TEXT,
  abi_ref TEXT,
  UNIQUE (manifest_id, name, address)
);

CREATE TABLE manifest_contracts (
  manifest_contract_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  manifest_id BIGINT NOT NULL REFERENCES manifest_versions (manifest_id) ON DELETE CASCADE,
  role TEXT NOT NULL,
  address TEXT NOT NULL,
  proxy_kind TEXT NOT NULL,
  implementation TEXT,
  UNIQUE (manifest_id, role, address)
);

CREATE TABLE manifest_capability_flags (
  manifest_id BIGINT NOT NULL REFERENCES manifest_versions (manifest_id) ON DELETE CASCADE,
  capability_name TEXT NOT NULL,
  status capability_support_status NOT NULL,
  notes TEXT,
  PRIMARY KEY (manifest_id, capability_name)
);

CREATE TABLE manifest_discovery_rules (
  manifest_discovery_rule_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  manifest_id BIGINT NOT NULL REFERENCES manifest_versions (manifest_id) ON DELETE CASCADE,
  edge_kind TEXT NOT NULL,
  from_role TEXT NOT NULL,
  admission TEXT NOT NULL,
  rule_payload JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE INDEX manifest_discovery_rules_manifest_idx
  ON manifest_discovery_rules (manifest_id);

CREATE TABLE discovery_edges (
  discovery_edge_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chain_id TEXT NOT NULL,
  edge_kind TEXT NOT NULL,
  from_address TEXT NOT NULL,
  to_address TEXT NOT NULL,
  discovery_source TEXT NOT NULL,
  source_manifest_id BIGINT REFERENCES manifest_versions (manifest_id) ON DELETE SET NULL,
  admission TEXT NOT NULL,
  active_from_block_number BIGINT CHECK (active_from_block_number IS NULL OR active_from_block_number >= 0),
  active_from_block_hash TEXT,
  active_to_block_number BIGINT CHECK (active_to_block_number IS NULL OR active_to_block_number >= 0),
  active_to_block_hash TEXT,
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX discovery_edges_lookup_idx
  ON discovery_edges (chain_id, from_address, edge_kind);
