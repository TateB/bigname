-- Phase 6 projection foundation: declared record inventory and cache storage keyed by resource and version boundary.

CREATE TABLE record_inventory_current (
  resource_id UUID NOT NULL REFERENCES resources (resource_id),
  record_version_boundary_key TEXT NOT NULL,
  record_version_boundary JSONB NOT NULL DEFAULT '{}'::JSONB,
  enumeration_basis JSONB NOT NULL DEFAULT '{}'::JSONB,
  selectors JSONB NOT NULL DEFAULT '[]'::JSONB,
  explicit_gaps JSONB NOT NULL DEFAULT '[]'::JSONB,
  unsupported_families JSONB NOT NULL DEFAULT '[]'::JSONB,
  last_change JSONB,
  entries JSONB NOT NULL DEFAULT '[]'::JSONB,
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  coverage JSONB NOT NULL DEFAULT '{}'::JSONB,
  chain_positions JSONB NOT NULL DEFAULT '{}'::JSONB,
  canonicality_summary JSONB NOT NULL DEFAULT '{}'::JSONB,
  manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
  last_recomputed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (resource_id, record_version_boundary_key),
  CHECK (record_version_boundary_key <> '')
);
