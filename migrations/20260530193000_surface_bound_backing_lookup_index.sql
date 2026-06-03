-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_surface_bound_backing_lookup_idx
ON normalized_events (
    logical_name_id,
    resource_id,
    (after_state->>'authority_key'),
    (after_state->>'active_from')
)
WHERE event_kind = 'SurfaceBound'
  AND canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  );
