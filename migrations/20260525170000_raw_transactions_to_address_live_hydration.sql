CREATE TABLE event_silent_resolver_call_observations (
    event_silent_resolver_call_observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id TEXT NOT NULL,
    resolver_address TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    transaction_index BIGINT NOT NULL,
    canonicality_state canonicality_state DEFAULT 'observed'::canonicality_state NOT NULL,
    observed_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    CONSTRAINT event_silent_resolver_call_observations_block_number_check CHECK (block_number >= 0),
    CONSTRAINT event_silent_resolver_call_observations_transaction_index_check CHECK (transaction_index >= 0),
    CONSTRAINT event_silent_resolver_call_observations_resolver_check CHECK (resolver_address <> ''),
    CONSTRAINT event_silent_resolver_call_observations_transaction_hash_check CHECK (transaction_hash <> ''),
    UNIQUE (chain_id, block_hash, transaction_index)
);

CREATE INDEX event_silent_resolver_calls_by_latest_idx
    ON event_silent_resolver_call_observations (
        chain_id,
        LOWER(resolver_address),
        canonicality_state,
        block_number DESC,
        transaction_index DESC
    );

CREATE INDEX event_silent_resolver_calls_by_block_hash_idx
    ON event_silent_resolver_call_observations (chain_id, block_hash);
