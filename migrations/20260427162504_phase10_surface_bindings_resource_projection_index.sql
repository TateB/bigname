-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS surface_bindings_resource_projection_replay_idx
  ON surface_bindings (
    resource_id,
    active_from ASC,
    active_to ASC NULLS LAST,
    logical_name_id ASC,
    surface_binding_id ASC
  )
  WHERE canonicality_state IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  );
