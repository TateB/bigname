-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS raw_logs_base_rederive_fact_emitter_idx
    ON public.raw_logs (
        chain_id,
        block_hash,
        transaction_hash,
        log_index,
        LOWER(emitting_address),
        block_number
    )
    WHERE chain_id = 'base-mainnet'
      AND canonicality_state IN (
          'canonical'::public.canonicality_state,
          'safe'::public.canonicality_state,
          'finalized'::public.canonicality_state
      );
