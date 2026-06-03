-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_children_v1_child_lookup_idx
    ON public.normalized_events (
        namespace,
        chain_id,
        (after_state ->> 'child_node'),
        block_number DESC NULLS LAST,
        log_index DESC NULLS LAST,
        normalized_event_id DESC
    )
    WHERE event_kind = 'SubregistryChanged'
      AND derivation_kind = 'ens_v1_subregistry_changed'
      AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
      AND canonicality_state IN (
          'canonical'::public.canonicality_state,
          'safe'::public.canonicality_state,
          'finalized'::public.canonicality_state
      )
      AND after_state ->> 'parent_node' IS NOT NULL
      AND after_state ->> 'child_node' IS NOT NULL;
