-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS resources_base_rederive_adapter_scope_idx
    ON public.resources (resource_id)
    WHERE chain_id = 'base-mainnet'
      AND provenance->>'adapter' = 'ens_v1_unwrapped_authority';
