-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_children_v1_labelhash_lookup_idx
    ON public.normalized_events (
        lower(after_state ->> 'labelhash'),
        namespace,
        chain_id,
        (after_state ->> 'parent_node')
    )
    WHERE event_kind = 'SubregistryChanged'
      AND derivation_kind = 'ens_v1_subregistry_changed'
      AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
      AND canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
      AND after_state ->> 'parent_node' IS NOT NULL
      AND after_state ->> 'child_node' IS NOT NULL
      AND after_state ->> 'labelhash' IS NOT NULL;
