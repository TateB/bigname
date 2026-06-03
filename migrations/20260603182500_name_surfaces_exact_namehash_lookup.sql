-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS name_surfaces_exact_namehash_projection_idx
    ON public.name_surfaces (
        namehash,
        namespace,
        chain_id,
        logical_name_id
    )
    WHERE canonicality_state IN (
        'canonical'::public.canonicality_state,
        'safe'::public.canonicality_state,
        'finalized'::public.canonicality_state
    );
