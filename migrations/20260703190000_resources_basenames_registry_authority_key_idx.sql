-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS resources_basenames_registry_authority_key_idx
    ON public.resources ((provenance->>'authority_key'))
    WHERE chain_id = 'base-mainnet'
      AND canonicality_state IN (
          'canonical'::public.canonicality_state,
          'safe'::public.canonicality_state,
          'finalized'::public.canonicality_state
      )
      AND provenance->>'authority_kind' = 'registry_only'
      AND COALESCE(provenance->>'labelhash', '') <> '';
