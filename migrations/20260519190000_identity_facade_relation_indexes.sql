-- no-transaction

-- Identity forward lookups join address-name relation rows from a name-rooted
-- request. Keep this logical-name-led so single and batch name reads do not
-- scan the address-led relation indexes.
CREATE INDEX IF NOT EXISTS address_names_current_identity_logical_relation_idx
    ON public.address_names_current (
        logical_name_id,
        relation,
        address
    )
    INCLUDE (
        resource_id,
        surface_binding_id,
        token_lineage_id
    );

-- Reverse identity pagination is sorted by role/name for each address. This
-- index gives the façade a relation/name-led path for high-cardinality
-- address sets while keeping the current projection table as the source.
CREATE INDEX IF NOT EXISTS address_names_current_identity_reverse_sort_idx
    ON public.address_names_current (
        address,
        relation,
        normalized_name,
        namespace,
        namehash,
        logical_name_id
    )
    INCLUDE (
        resource_id,
        surface_binding_id,
        token_lineage_id
    );
