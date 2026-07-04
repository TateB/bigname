ALTER TABLE public.base_normalized_rederive_runs
    DROP CONSTRAINT base_normalized_rederive_runs_status_check;

ALTER TABLE public.base_normalized_rederive_runs
    ADD CONSTRAINT base_normalized_rederive_runs_status_check CHECK (
        status = ANY (ARRAY['running'::text, 'completed'::text, 'aborted'::text])
    );

ALTER TABLE public.base_normalized_rederive_runs
    DROP CONSTRAINT base_normalized_rederive_runs_step_check;

ALTER TABLE public.base_normalized_rederive_runs
    ADD CONSTRAINT base_normalized_rederive_runs_step_check CHECK (
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
            'completed'::text,
            'aborted'::text
        ])
    );

ALTER TABLE public.base_normalized_rederive_runs
    DROP CONSTRAINT base_normalized_rederive_runs_completed_at_check;

ALTER TABLE public.base_normalized_rederive_runs
    ADD CONSTRAINT base_normalized_rederive_runs_completed_at_check CHECK (
        (status = 'completed'::text) = (completed_at IS NOT NULL)
    );
