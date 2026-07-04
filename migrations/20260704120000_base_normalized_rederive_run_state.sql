CREATE TABLE public.base_normalized_rederive_runs (
    run_id text PRIMARY KEY,
    deployment_profile text NOT NULL,
    chain_id text NOT NULL,
    replay_target_block bigint NOT NULL,
    batch_size bigint NOT NULL,
    status text DEFAULT 'running'::text NOT NULL,
    current_step text DEFAULT 'address_names_current'::text NOT NULL,
    expected_counts jsonb NOT NULL,
    deleted_counts jsonb DEFAULT '{}'::jsonb NOT NULL,
    plan_snapshot jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT base_normalized_rederive_runs_batch_size_check CHECK (batch_size > 0),
    CONSTRAINT base_normalized_rederive_runs_chain_id_check CHECK (chain_id = 'base-mainnet'::text),
    CONSTRAINT base_normalized_rederive_runs_counts_object_check CHECK (
        jsonb_typeof(expected_counts) = 'object'::text
        AND jsonb_typeof(deleted_counts) = 'object'::text
        AND jsonb_typeof(plan_snapshot) = 'object'::text
    ),
    CONSTRAINT base_normalized_rederive_runs_status_check CHECK (
        status = ANY (ARRAY['running'::text, 'completed'::text])
    ),
    CONSTRAINT base_normalized_rederive_runs_step_check CHECK (
        current_step = ANY (ARRAY[
            'address_names_current'::text,
            'name_current'::text,
            'children_current'::text,
            'permissions_current'::text,
            'record_inventory_current'::text,
            'projection_normalized_event_changes'::text,
            'normalized_events'::text,
            'surface_bindings'::text,
            'resources'::text,
            'name_surfaces'::text,
            'token_lineages'::text,
            'final_replay_reset'::text,
            'completed'::text
        ])
    ),
    CONSTRAINT base_normalized_rederive_runs_completed_at_check CHECK (
        (status = 'completed'::text) = (completed_at IS NOT NULL)
    )
);

CREATE INDEX base_normalized_rederive_runs_status_idx
    ON public.base_normalized_rederive_runs (chain_id, status, updated_at);

CREATE TABLE public.base_normalized_rederive_run_batches (
    run_id text NOT NULL REFERENCES public.base_normalized_rederive_runs(run_id) ON DELETE CASCADE,
    batch_sequence bigint GENERATED ALWAYS AS IDENTITY,
    step text NOT NULL,
    range_start text,
    range_end text,
    row_count bigint NOT NULL,
    deleted_counts jsonb NOT NULL,
    completed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT base_normalized_rederive_run_batches_pkey PRIMARY KEY (run_id, batch_sequence),
    CONSTRAINT base_normalized_rederive_run_batches_count_check CHECK (row_count >= 0),
    CONSTRAINT base_normalized_rederive_run_batches_counts_object_check CHECK (
        jsonb_typeof(deleted_counts) = 'object'::text
    )
);

CREATE INDEX base_normalized_rederive_run_batches_step_idx
    ON public.base_normalized_rederive_run_batches (run_id, step, batch_sequence);
