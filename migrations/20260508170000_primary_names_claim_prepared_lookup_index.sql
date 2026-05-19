-- The primary-name claim point query is prepared with tuple values as bind
-- parameters. Keep the partial predicate limited to static event-shape filters
-- so Postgres can prove the index is usable for the prepared statement.
CREATE INDEX IF NOT EXISTS normalized_events_primary_names_claim_prepared_lookup_idx
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
      AND canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      );
