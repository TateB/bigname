CREATE INDEX IF NOT EXISTS discovery_edges_active_source_from_endpoint_idx
  ON discovery_edges (source_manifest_id, from_contract_instance_id)
  WHERE deactivated_at IS NULL
    AND edge_kind <> 'migration';

CREATE INDEX IF NOT EXISTS discovery_edges_active_source_to_endpoint_idx
  ON discovery_edges (source_manifest_id, to_contract_instance_id)
  WHERE deactivated_at IS NULL
    AND edge_kind <> 'migration';

CREATE INDEX IF NOT EXISTS manifest_versions_rollout_manifest_idx
  ON manifest_versions (rollout_status, manifest_id);

CREATE INDEX IF NOT EXISTS contract_instance_addresses_latest_instance_idx
  ON contract_instance_addresses (
    contract_instance_id,
    ((deactivated_at IS NULL)) DESC,
    admitted_at DESC
  )
  INCLUDE (chain_id, address);

CREATE INDEX IF NOT EXISTS discovery_edges_observation_point_idx
  ON discovery_edges (
    discovery_source,
    edge_kind,
    active_from_block_number,
    active_from_block_hash,
    ((provenance ->> 'observation_key'))
  );
