CREATE INDEX IF NOT EXISTS normalized_events_reverse_claim_source_lookup_idx
  ON normalized_events (
    chain_id,
    (LOWER(after_state ->> 'reverse_node')),
    block_number DESC NULLS LAST,
    log_index DESC NULLS LAST,
    normalized_event_id DESC
  )
  WHERE event_kind = 'ReverseChanged'
    AND derivation_kind = 'ens_v1_reverse_claim'
    AND canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
    )
    AND after_state ->> 'reverse_node' IS NOT NULL
    AND after_state ->> 'reverse_node' <> ''
    AND after_state ->> 'address' IS NOT NULL
    AND after_state ->> 'address' <> ''
    AND after_state ->> 'coin_type' IS NOT NULL
    AND after_state ->> 'coin_type' <> ''
    AND after_state ->> 'reverse_name' IS NOT NULL
    AND after_state ->> 'reverse_name' <> '';
