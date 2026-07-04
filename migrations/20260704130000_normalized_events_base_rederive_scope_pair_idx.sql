-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_base_rederive_scope_pair_idx
    ON public.normalized_events (
        derivation_kind,
        source_family,
        block_number DESC,
        normalized_event_id
    )
    WHERE chain_id = 'base-mainnet'
      AND block_number IS NOT NULL
      AND block_hash IS NOT NULL;
