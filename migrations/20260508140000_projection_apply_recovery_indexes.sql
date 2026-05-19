-- Continuous projection apply reclaims old durable claims after a worker exits or
-- is replaced mid-apply. Keep that recovery lookup narrow so claimed rows do not
-- become invisible operational debt.
CREATE INDEX IF NOT EXISTS projection_invalidations_stale_claim_idx
    ON public.projection_invalidations (
        claimed_at,
        projection,
        projection_key
    )
    WHERE claim_token IS NOT NULL;

-- Address-name point rebuilds are driven by one address key. These indexes let
-- the worker find affected canonical logical names directly instead of scanning
-- every current surface binding for each address invalidation.
CREATE INDEX IF NOT EXISTS normalized_events_address_names_registrant_lookup_idx
    ON public.normalized_events (
        lower(after_state ->> 'registrant'),
        logical_name_id
    )
    WHERE logical_name_id IS NOT NULL
      AND event_kind = 'RegistrationGranted'
      AND after_state ->> 'registrant' IS NOT NULL
      AND after_state ->> 'registrant' <> ''
      AND canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      );

CREATE INDEX IF NOT EXISTS normalized_events_address_names_token_to_lookup_idx
    ON public.normalized_events (
        lower(after_state ->> 'to'),
        logical_name_id
    )
    WHERE logical_name_id IS NOT NULL
      AND event_kind = 'TokenControlTransferred'
      AND after_state ->> 'to' IS NOT NULL
      AND after_state ->> 'to' <> ''
      AND canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      );

CREATE INDEX IF NOT EXISTS normalized_events_address_names_owner_lookup_idx
    ON public.normalized_events (
        lower(after_state ->> 'owner'),
        logical_name_id
    )
    WHERE logical_name_id IS NOT NULL
      AND event_kind = 'AuthorityTransferred'
      AND after_state ->> 'owner' IS NOT NULL
      AND after_state ->> 'owner' <> ''
      AND canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      );
