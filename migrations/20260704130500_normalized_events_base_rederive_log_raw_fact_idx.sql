-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_base_rederive_log_raw_fact_idx
    ON public.normalized_events (
        derivation_kind,
        source_family,
        block_number,
        block_hash,
        transaction_hash,
        log_index
    )
    WHERE chain_id = 'base-mainnet'
      AND block_number IS NOT NULL
      AND block_hash IS NOT NULL
      AND transaction_hash IS NOT NULL
      AND log_index IS NOT NULL;
