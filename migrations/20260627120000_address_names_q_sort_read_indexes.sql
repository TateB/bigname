-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS address_names_current_address_normalized_name_prefix_idx
    ON public.address_names_current (
        address,
        normalized_name text_pattern_ops,
        namespace,
        logical_name_id,
        resource_id
    );
