-- Resolver point rebuilds are keyed by `(chain_id, resolver_address)`. The
-- worker first finds canonical logical/resource pairs that have ever pointed at
-- the requested resolver, then proves the latest resolver event for those pairs
-- still points there.
CREATE INDEX IF NOT EXISTS normalized_events_resolver_current_address_lookup_idx
    ON public.normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        logical_name_id,
        resource_id
    )
    WHERE event_kind = 'ResolverChanged'
      AND logical_name_id IS NOT NULL
      AND resource_id IS NOT NULL
      AND chain_id IS NOT NULL
      AND after_state ->> 'resolver' IS NOT NULL
      AND after_state ->> 'resolver' <> ''
      AND canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      );

CREATE INDEX IF NOT EXISTS normalized_events_resolver_current_pair_latest_idx
    ON public.normalized_events (
        chain_id,
        logical_name_id,
        resource_id,
        block_number DESC NULLS LAST,
        log_index DESC NULLS LAST,
        normalized_event_id DESC
    )
    WHERE event_kind = 'ResolverChanged'
      AND logical_name_id IS NOT NULL
      AND resource_id IS NOT NULL
      AND chain_id IS NOT NULL
      AND canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      );
