-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS token_lineages_base_rederive_adapter_scope_idx
    ON public.token_lineages (token_lineage_id)
    WHERE chain_id = 'base-mainnet'
      AND provenance->>'adapter' = 'ens_v1_unwrapped_authority';
