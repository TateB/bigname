-- no-transaction

-- The compact feed route reads one display row per address and intentionally
-- avoids record-inventory or provenance hydration. Keep displayed fields,
-- readable-universe keys, and compact response metadata in covering indexes so
-- high-cardinality feed batches do not touch the row heap.
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
