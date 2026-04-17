-- Phase 3 manifests/discovery: replace address-keyed persistence with contract-instance-backed storage.

ALTER TABLE discovery_edges
  RENAME TO discovery_edges_legacy;

ALTER INDEX discovery_edges_lookup_idx
  RENAME TO discovery_edges_lookup_idx_legacy;

CREATE TABLE contract_instances (
  contract_instance_id UUID PRIMARY KEY,
  chain_id TEXT NOT NULL,
  contract_kind TEXT NOT NULL,
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX contract_instances_chain_kind_idx
  ON contract_instances (chain_id, contract_kind);

CREATE TABLE contract_instance_addresses (
  contract_instance_address_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  contract_instance_id UUID NOT NULL REFERENCES contract_instances (contract_instance_id) ON DELETE CASCADE,
  chain_id TEXT NOT NULL,
  address TEXT NOT NULL,
  admitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  deactivated_at TIMESTAMPTZ,
  active_from_block_number BIGINT CHECK (active_from_block_number IS NULL OR active_from_block_number >= 0),
  active_from_block_hash TEXT,
  active_to_block_number BIGINT CHECK (active_to_block_number IS NULL OR active_to_block_number >= 0),
  active_to_block_hash TEXT,
  source_manifest_id BIGINT REFERENCES manifest_versions (manifest_id) ON DELETE SET NULL,
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  CHECK (deactivated_at IS NULL OR deactivated_at >= admitted_at)
);

CREATE INDEX contract_instance_addresses_lookup_idx
  ON contract_instance_addresses (chain_id, address, admitted_at DESC);

CREATE UNIQUE INDEX contract_instance_addresses_active_instance_idx
  ON contract_instance_addresses (contract_instance_id)
  WHERE deactivated_at IS NULL;

CREATE UNIQUE INDEX contract_instance_addresses_active_address_idx
  ON contract_instance_addresses (chain_id, address)
  WHERE deactivated_at IS NULL;

CREATE TABLE manifest_contract_instances (
  manifest_contract_instance_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  manifest_id BIGINT NOT NULL REFERENCES manifest_versions (manifest_id) ON DELETE CASCADE,
  declaration_kind TEXT NOT NULL,
  declaration_name TEXT NOT NULL,
  contract_instance_id UUID NOT NULL REFERENCES contract_instances (contract_instance_id),
  declared_address TEXT NOT NULL,
  code_hash TEXT,
  abi_ref TEXT,
  role TEXT,
  proxy_kind TEXT,
  implementation_contract_instance_id UUID REFERENCES contract_instances (contract_instance_id),
  declared_implementation_address TEXT,
  UNIQUE (manifest_id, declaration_kind, declaration_name),
  CHECK (declaration_kind IN ('root', 'contract')),
  CHECK (
    (declaration_kind = 'root'
      AND role IS NULL
      AND proxy_kind IS NULL
      AND implementation_contract_instance_id IS NULL
      AND declared_implementation_address IS NULL)
    OR
    (declaration_kind = 'contract' AND role IS NOT NULL)
  )
);

CREATE INDEX manifest_contract_instances_manifest_idx
  ON manifest_contract_instances (manifest_id, declaration_kind, declaration_name);

CREATE INDEX manifest_contract_instances_instance_idx
  ON manifest_contract_instances (contract_instance_id);

CREATE TABLE discovery_edges (
  discovery_edge_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chain_id TEXT NOT NULL,
  edge_kind TEXT NOT NULL,
  from_contract_instance_id UUID NOT NULL REFERENCES contract_instances (contract_instance_id),
  to_contract_instance_id UUID NOT NULL REFERENCES contract_instances (contract_instance_id),
  discovery_source TEXT NOT NULL,
  source_manifest_id BIGINT REFERENCES manifest_versions (manifest_id) ON DELETE SET NULL,
  admission TEXT NOT NULL,
  admitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  deactivated_at TIMESTAMPTZ,
  active_from_block_number BIGINT CHECK (active_from_block_number IS NULL OR active_from_block_number >= 0),
  active_from_block_hash TEXT,
  active_to_block_number BIGINT CHECK (active_to_block_number IS NULL OR active_to_block_number >= 0),
  active_to_block_hash TEXT,
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  CHECK (deactivated_at IS NULL OR deactivated_at >= admitted_at)
);

CREATE INDEX discovery_edges_lookup_idx
  ON discovery_edges (chain_id, from_contract_instance_id, edge_kind);

CREATE INDEX discovery_edges_active_target_idx
  ON discovery_edges (chain_id, to_contract_instance_id, edge_kind)
  WHERE deactivated_at IS NULL;

CREATE TEMP TABLE contract_instance_backfill_map (
  chain_id TEXT NOT NULL,
  address TEXT NOT NULL,
  contract_instance_id UUID NOT NULL,
  contract_kind TEXT NOT NULL,
  PRIMARY KEY (chain_id, address)
) ON COMMIT DROP;

WITH addresses AS (
  SELECT
    mv.chain AS chain_id,
    lower(mr.address) AS address,
    'root'::TEXT AS contract_kind
  FROM manifest_versions mv
  JOIN manifest_roots mr ON mr.manifest_id = mv.manifest_id

  UNION

  SELECT
    mv.chain AS chain_id,
    lower(mc.address) AS address,
    'contract'::TEXT AS contract_kind
  FROM manifest_versions mv
  JOIN manifest_contracts mc ON mc.manifest_id = mv.manifest_id

  UNION

  SELECT
    mv.chain AS chain_id,
    lower(mc.implementation) AS address,
    'contract'::TEXT AS contract_kind
  FROM manifest_versions mv
  JOIN manifest_contracts mc ON mc.manifest_id = mv.manifest_id
  WHERE mc.implementation IS NOT NULL

  UNION

  SELECT
    de.chain_id AS chain_id,
    lower(de.from_address) AS address,
    'contract'::TEXT AS contract_kind
  FROM discovery_edges_legacy de

  UNION

  SELECT
    de.chain_id AS chain_id,
    lower(de.to_address) AS address,
    'contract'::TEXT AS contract_kind
  FROM discovery_edges_legacy de
)
INSERT INTO contract_instance_backfill_map (
  chain_id,
  address,
  contract_instance_id,
  contract_kind
)
SELECT
  chain_id,
  address,
  (
    substr(md5(chain_id || ':' || address), 1, 8) || '-' ||
    substr(md5(chain_id || ':' || address), 9, 4) || '-' ||
    substr(md5(chain_id || ':' || address), 13, 4) || '-' ||
    substr(md5(chain_id || ':' || address), 17, 4) || '-' ||
    substr(md5(chain_id || ':' || address), 21, 12)
  )::UUID AS contract_instance_id,
  CASE
    WHEN bool_or(contract_kind = 'root') THEN 'root'
    ELSE 'contract'
  END AS contract_kind
FROM addresses
GROUP BY chain_id, address;

INSERT INTO contract_instances (
  contract_instance_id,
  chain_id,
  contract_kind,
  provenance
)
SELECT
  contract_instance_id,
  chain_id,
  contract_kind,
  jsonb_build_object(
    'source', 'phase2_address_keyed_backfill',
    'address', address
  )
FROM contract_instance_backfill_map;

INSERT INTO contract_instance_addresses (
  contract_instance_id,
  chain_id,
  address,
  admitted_at,
  source_manifest_id,
  provenance
)
SELECT
  contract_instance_id,
  chain_id,
  address,
  now(),
  NULL,
  jsonb_build_object(
    'source', 'phase2_address_keyed_backfill'
  )
FROM contract_instance_backfill_map;

INSERT INTO manifest_contract_instances (
  manifest_id,
  declaration_kind,
  declaration_name,
  contract_instance_id,
  declared_address,
  code_hash,
  abi_ref
)
SELECT
  mr.manifest_id,
  'root',
  mr.name,
  map.contract_instance_id,
  lower(mr.address),
  mr.code_hash,
  mr.abi_ref
FROM manifest_roots mr
JOIN manifest_versions mv ON mv.manifest_id = mr.manifest_id
JOIN contract_instance_backfill_map map
  ON map.chain_id = mv.chain
 AND map.address = lower(mr.address);

INSERT INTO manifest_contract_instances (
  manifest_id,
  declaration_kind,
  declaration_name,
  contract_instance_id,
  declared_address,
  role,
  proxy_kind,
  implementation_contract_instance_id,
  declared_implementation_address
)
SELECT
  mc.manifest_id,
  'contract',
  mc.role,
  address_map.contract_instance_id,
  lower(mc.address),
  mc.role,
  mc.proxy_kind,
  implementation_map.contract_instance_id,
  lower(mc.implementation)
FROM manifest_contracts mc
JOIN manifest_versions mv ON mv.manifest_id = mc.manifest_id
JOIN contract_instance_backfill_map address_map
  ON address_map.chain_id = mv.chain
 AND address_map.address = lower(mc.address)
LEFT JOIN contract_instance_backfill_map implementation_map
  ON implementation_map.chain_id = mv.chain
 AND implementation_map.address = lower(mc.implementation);

INSERT INTO discovery_edges (
  chain_id,
  edge_kind,
  from_contract_instance_id,
  to_contract_instance_id,
  discovery_source,
  source_manifest_id,
  admission,
  admitted_at,
  active_from_block_number,
  active_from_block_hash,
  active_to_block_number,
  active_to_block_hash,
  provenance
)
SELECT
  de.chain_id,
  de.edge_kind,
  from_map.contract_instance_id,
  to_map.contract_instance_id,
  de.discovery_source,
  de.source_manifest_id,
  de.admission,
  de.observed_at,
  de.active_from_block_number,
  de.active_from_block_hash,
  de.active_to_block_number,
  de.active_to_block_hash,
  de.provenance
FROM discovery_edges_legacy de
JOIN contract_instance_backfill_map from_map
  ON from_map.chain_id = de.chain_id
 AND from_map.address = lower(de.from_address)
JOIN contract_instance_backfill_map to_map
  ON to_map.chain_id = de.chain_id
 AND to_map.address = lower(de.to_address);

DROP TABLE manifest_roots;
DROP TABLE manifest_contracts;
DROP TABLE discovery_edges_legacy;
