CREATE TABLE public.normalized_replay_adapter_checkpoints (
    deployment_profile text NOT NULL,
    chain_id text NOT NULL,
    cursor_kind text NOT NULL,
    adapter text NOT NULL,
    checkpoint_scope text NOT NULL,
    replay_start_block_number bigint NOT NULL,
    replay_target_block_number bigint NOT NULL,
    last_block_number bigint,
    last_transaction_index bigint,
    last_log_index bigint,
    last_emitting_address text,
    staged_item_count bigint DEFAULT 0 NOT NULL,
    staged_aux_item_count bigint DEFAULT 0 NOT NULL,
    scanned_log_count bigint DEFAULT 0 NOT NULL,
    matched_log_count bigint DEFAULT 0 NOT NULL,
    status text DEFAULT 'running' NOT NULL,
    state_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    last_failure_reason text,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT normalized_replay_adapter_checkpoints_pkey PRIMARY KEY (
        deployment_profile,
        chain_id,
        cursor_kind,
        adapter,
        checkpoint_scope
    ),
    CONSTRAINT normalized_replay_adapter_checkpoints_range_check CHECK (
        replay_start_block_number >= 0
        AND replay_target_block_number >= replay_start_block_number
    ),
    CONSTRAINT normalized_replay_adapter_checkpoints_position_check CHECK (
        (
            last_block_number IS NULL
            AND last_transaction_index IS NULL
            AND last_log_index IS NULL
            AND last_emitting_address IS NULL
        )
        OR (
            last_block_number IS NOT NULL
            AND last_transaction_index IS NOT NULL
            AND last_log_index IS NOT NULL
            AND last_emitting_address IS NOT NULL
            AND last_block_number >= replay_start_block_number
            AND last_block_number <= replay_target_block_number
            AND last_transaction_index >= 0
            AND last_log_index >= 0
            AND last_emitting_address <> ''
        )
    ),
    CONSTRAINT normalized_replay_adapter_checkpoints_counts_check CHECK (
        staged_item_count >= 0
        AND staged_aux_item_count >= 0
        AND scanned_log_count >= 0
        AND matched_log_count >= 0
    ),
    CONSTRAINT normalized_replay_adapter_checkpoints_status_check CHECK (
        status = ANY (ARRAY['running'::text, 'stream_complete'::text, 'completed'::text])
    )
);

CREATE TABLE public.normalized_replay_adapter_checkpoint_items (
    deployment_profile text NOT NULL,
    chain_id text NOT NULL,
    cursor_kind text NOT NULL,
    adapter text NOT NULL,
    checkpoint_scope text NOT NULL,
    item_kind text NOT NULL,
    item_key text NOT NULL,
    item_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT normalized_replay_adapter_checkpoint_items_pkey PRIMARY KEY (
        deployment_profile,
        chain_id,
        cursor_kind,
        adapter,
        checkpoint_scope,
        item_kind,
        item_key
    ),
    CONSTRAINT normalized_replay_adapter_checkpoint_items_parent_fkey FOREIGN KEY (
        deployment_profile,
        chain_id,
        cursor_kind,
        adapter,
        checkpoint_scope
    ) REFERENCES public.normalized_replay_adapter_checkpoints (
        deployment_profile,
        chain_id,
        cursor_kind,
        adapter,
        checkpoint_scope
    ) ON DELETE CASCADE,
    CONSTRAINT normalized_replay_adapter_checkpoint_items_kind_check CHECK (item_kind <> ''),
    CONSTRAINT normalized_replay_adapter_checkpoint_items_key_check CHECK (item_key <> '')
);

CREATE INDEX normalized_replay_adapter_checkpoints_progress_idx
    ON public.normalized_replay_adapter_checkpoints (
        deployment_profile,
        chain_id,
        cursor_kind,
        status,
        replay_target_block_number,
        last_block_number,
        last_transaction_index,
        last_log_index
    );

CREATE INDEX normalized_replay_adapter_checkpoint_items_kind_idx
    ON public.normalized_replay_adapter_checkpoint_items (
        deployment_profile,
        chain_id,
        cursor_kind,
        adapter,
        checkpoint_scope,
        item_kind
    );

CREATE INDEX normalized_replay_checkpoint_items_latest_source_key_idx
    ON public.normalized_replay_adapter_checkpoint_items (
        deployment_profile,
        chain_id,
        cursor_kind,
        adapter,
        checkpoint_scope,
        (item_payload ->> 'discovery_source'),
        item_key
    )
    WHERE item_kind = 'latest_assignment';
