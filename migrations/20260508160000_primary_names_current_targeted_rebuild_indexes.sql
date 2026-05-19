-- Primary-name point rebuilds are keyed by `(address, namespace, coin_type)`.
-- These expression indexes keep tuple refreshes bounded to the changed primary
-- claim instead of scanning all current reverse/name claim events.
CREATE INDEX IF NOT EXISTS normalized_events_primary_names_reverse_lookup_idx
    ON public.normalized_events (
        lower(after_state ->> 'address'),
        COALESCE(after_state ->> 'namespace', namespace),
        (after_state ->> 'coin_type'),
        block_number DESC NULLS LAST,
        log_index DESC NULLS LAST,
        normalized_event_id DESC
    )
    WHERE event_kind = 'ReverseChanged'
      AND after_state ->> 'address' IS NOT NULL
      AND after_state ->> 'address' <> ''
      AND COALESCE(after_state ->> 'namespace', namespace) IS NOT NULL
      AND COALESCE(after_state ->> 'namespace', namespace) <> ''
      AND after_state ->> 'coin_type' IS NOT NULL
      AND after_state ->> 'coin_type' <> ''
      AND canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      );

CREATE INDEX IF NOT EXISTS normalized_events_primary_names_claim_lookup_idx
    ON public.normalized_events (
        lower(after_state -> 'primary_claim_source' ->> 'address'),
        COALESCE(after_state -> 'primary_claim_source' ->> 'namespace', namespace),
        (after_state -> 'primary_claim_source' ->> 'coin_type'),
        block_number DESC NULLS LAST,
        log_index DESC NULLS LAST,
        normalized_event_id DESC
    )
    WHERE event_kind = 'RecordChanged'
      AND logical_name_id IS NULL
      AND resource_id IS NULL
      AND after_state ->> 'record_key' = 'name'
      AND after_state ? 'primary_claim_source'
      AND after_state -> 'primary_claim_source' ->> 'address' IS NOT NULL
      AND after_state -> 'primary_claim_source' ->> 'address' <> ''
      AND COALESCE(after_state -> 'primary_claim_source' ->> 'namespace', namespace) IS NOT NULL
      AND COALESCE(after_state -> 'primary_claim_source' ->> 'namespace', namespace) <> ''
      AND after_state -> 'primary_claim_source' ->> 'coin_type' IS NOT NULL
      AND after_state -> 'primary_claim_source' ->> 'coin_type' <> ''
      AND canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      );
