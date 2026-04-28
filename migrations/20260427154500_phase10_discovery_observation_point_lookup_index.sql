CREATE INDEX IF NOT EXISTS discovery_edges_observation_point_lookup_idx
  ON discovery_edges (
    discovery_source,
    ((provenance ->> 'observation_key')),
    active_from_block_number,
    active_from_block_hash,
    edge_kind
  );
