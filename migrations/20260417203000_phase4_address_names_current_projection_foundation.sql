-- Phase 4 projection foundation: address-to-name current relation storage.

CREATE TABLE address_names_current (
  address TEXT NOT NULL,
  logical_name_id TEXT NOT NULL REFERENCES name_surfaces (logical_name_id),
  relation TEXT NOT NULL,
  namespace TEXT NOT NULL,
  canonical_display_name TEXT NOT NULL,
  normalized_name TEXT NOT NULL,
  namehash TEXT NOT NULL,
  surface_binding_id UUID NOT NULL REFERENCES surface_bindings (surface_binding_id),
  resource_id UUID NOT NULL REFERENCES resources (resource_id),
  token_lineage_id UUID REFERENCES token_lineages (token_lineage_id),
  binding_kind TEXT NOT NULL,
  provenance JSONB NOT NULL DEFAULT '{}'::JSONB,
  coverage JSONB NOT NULL DEFAULT '{}'::JSONB,
  chain_positions JSONB NOT NULL DEFAULT '{}'::JSONB,
  canonicality_summary JSONB NOT NULL DEFAULT '{}'::JSONB,
  manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
  last_recomputed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (address, logical_name_id, relation),
  CHECK (logical_name_id = namespace || ':' || normalized_name),
  CHECK (
    relation IN (
      'registrant',
      'token_holder',
      'effective_controller'
    )
  ),
  CHECK (
    binding_kind IN (
      'declared_registry_path',
      'linked_subregistry_path',
      'resolver_alias_path',
      'observed_wildcard_path',
      'migration_rebind',
      'observed_only'
    )
  )
);

CREATE INDEX address_names_current_address_sort_idx
  ON address_names_current (
    address,
    namespace,
    canonical_display_name,
    logical_name_id
  );
