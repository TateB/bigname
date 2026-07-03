-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS resources_basenames_registry_logical_labelhash_idx
    ON public.resources ((provenance->>'logical_name_id'), lower(provenance->>'labelhash'))
    WHERE chain_id = 'base-mainnet'
      AND canonicality_state IN (
          'canonical'::public.canonicality_state,
          'safe'::public.canonicality_state,
          'finalized'::public.canonicality_state
      )
      AND provenance->>'authority_kind' = 'registry_only'
      AND COALESCE(provenance->>'namehash', '') <> '';
