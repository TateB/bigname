-- no-transaction

-- Record-inventory rebuilds read canonical resolver and record events by
-- resource in ascending event order. Keep that projection path off broad
-- name-oriented scans.
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_inventory_resource_replay_idx
  ON normalized_events (
    resource_id,
    block_number ASC,
    log_index ASC NULLS FIRST,
    normalized_event_id ASC
  )
  WHERE resource_id IS NOT NULL
    AND logical_name_id IS NOT NULL
    AND chain_id IS NOT NULL
    AND block_number IS NOT NULL
    AND block_hash IS NOT NULL
    AND derivation_kind IN (
      'ens_v1_unwrapped_authority',
      'ens_v2_resolver'
    )
    AND event_kind IN (
      'RecordChanged',
      'RecordVersionChanged',
      'ResolverChanged'
    )
    AND canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
    );
