-- no-transaction

-- Replace the first compact-feed covering indexes with variants that also
-- cover readable-universe checks and compact response metadata. This keeps
-- feed rows aligned with the reverse identity/count contract without forcing
-- heap reads for the displayed row.
DROP INDEX IF EXISTS public.address_names_current_identity_feed_compact_idx;
DROP INDEX IF EXISTS public.address_names_current_identity_claim_compact_idx;

CREATE INDEX IF NOT EXISTS address_names_current_identity_feed_compact_idx
    ON public.address_names_current (
        address,
        (
            CASE
                WHEN relation IN ('registrant', 'token_holder') THEN 0
                ELSE 1
            END
        ),
        normalized_name,
        namespace,
        namehash,
        logical_name_id
    )
    INCLUDE (
        relation,
        canonical_display_name,
        resource_id,
        surface_binding_id,
        token_lineage_id,
        chain_positions,
        coverage
    );

CREATE INDEX IF NOT EXISTS address_names_current_identity_claim_compact_idx
    ON public.address_names_current (
        address,
        namespace,
        normalized_name,
        relation
    )
    INCLUDE (
        logical_name_id,
        namehash,
        canonical_display_name,
        resource_id,
        surface_binding_id,
        token_lineage_id,
        chain_positions,
        coverage
    );
