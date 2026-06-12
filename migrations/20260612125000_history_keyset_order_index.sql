-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_history_canonical_order_idx
ON public.normalized_events (
    block_number DESC NULLS LAST,
    chain_id ASC NULLS LAST,
    block_hash DESC NULLS LAST,
    transaction_hash DESC NULLS LAST,
    log_index DESC NULLS LAST,
    event_identity DESC
)
INCLUDE (
    normalized_event_id,
    namespace,
    logical_name_id,
    resource_id,
    event_kind
)
WHERE canonicality_state IN (
    'canonical'::public.canonicality_state,
    'safe'::public.canonicality_state,
    'finalized'::public.canonicality_state
);
