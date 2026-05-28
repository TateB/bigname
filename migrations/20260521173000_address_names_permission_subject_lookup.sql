-- no-transaction

-- Address-name point rebuilds now treat current resource-control permissions as
-- effective-controller inputs. Keep subject lookup symmetric with the existing
-- registrant/token-owner/controller indexes so address invalidations do not
-- scan the full normalized-event stream.
CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_address_names_permission_subject_lookup_idx
    ON public.normalized_events (
        lower(after_state ->> 'subject'),
        logical_name_id
    )
    WHERE logical_name_id IS NOT NULL
      AND event_kind = 'PermissionChanged'
      AND after_state -> 'scope' ->> 'kind' = 'resource'
      AND after_state ->> 'subject' IS NOT NULL
      AND after_state ->> 'subject' <> ''
      AND canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      );
