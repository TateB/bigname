CREATE INDEX IF NOT EXISTS discovery_edges_active_from_endpoint_scope_idx
  ON public.discovery_edges USING btree (from_contract_instance_id, source_manifest_id)
  WHERE deactivated_at IS NULL
    AND edge_kind <> 'migration'::text;

CREATE INDEX IF NOT EXISTS discovery_edges_active_to_endpoint_scope_idx
  ON public.discovery_edges USING btree (to_contract_instance_id, source_manifest_id)
  WHERE deactivated_at IS NULL
    AND edge_kind <> 'migration'::text;

CREATE INDEX IF NOT EXISTS discovery_edges_active_transitive_parent_scope_idx
  ON public.discovery_edges USING btree (to_contract_instance_id, source_manifest_id)
  WHERE deactivated_at IS NULL
    AND edge_kind = 'transitive'::text
    AND admission = 'reachable_from_root'::text
    AND provenance ? 'propagated_role'::text;

CREATE INDEX IF NOT EXISTS discovery_edges_active_source_chain_from_scope_idx
  ON public.discovery_edges USING btree (discovery_source, chain_id, from_contract_instance_id)
  WHERE deactivated_at IS NULL;

CREATE INDEX IF NOT EXISTS discovery_edges_active_target_source_kind_idx
  ON public.discovery_edges USING btree (chain_id, to_contract_instance_id, source_manifest_id, edge_kind)
  WHERE deactivated_at IS NULL
    AND edge_kind <> 'migration'::text;

CREATE INDEX IF NOT EXISTS discovery_edges_active_source_target_kind_chain_idx
  ON public.discovery_edges USING btree (source_manifest_id, to_contract_instance_id, edge_kind, chain_id)
  WHERE deactivated_at IS NULL
    AND edge_kind <> 'migration'::text;

CREATE INDEX IF NOT EXISTS discovery_edges_active_source_target_resolver_idx
  ON public.discovery_edges USING btree (source_manifest_id, to_contract_instance_id, chain_id)
  WHERE deactivated_at IS NULL
    AND edge_kind = 'resolver'::text;

CREATE INDEX IF NOT EXISTS discovery_edges_active_source_target_nonresolver_idx
  ON public.discovery_edges USING btree (source_manifest_id, to_contract_instance_id, chain_id)
  WHERE deactivated_at IS NULL
    AND edge_kind <> 'migration'::text
    AND edge_kind <> 'resolver'::text;
