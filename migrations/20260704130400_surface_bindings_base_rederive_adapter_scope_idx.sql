-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS surface_bindings_base_rederive_adapter_scope_idx
    ON public.surface_bindings (surface_binding_id)
    WHERE chain_id = 'base-mainnet'
      AND provenance->>'adapter' = 'ens_v1_unwrapped_authority';
