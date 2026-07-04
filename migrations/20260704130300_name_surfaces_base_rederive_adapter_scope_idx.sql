-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS name_surfaces_base_rederive_adapter_scope_idx
    ON public.name_surfaces (logical_name_id)
    WHERE chain_id = 'base-mainnet'
      AND provenance->>'adapter' = 'ens_v1_unwrapped_authority';
