ALTER TABLE public.normalized_replay_cursors
    DROP CONSTRAINT IF EXISTS normalized_replay_cursors_range_start_floor_block_number_check;

ALTER TABLE public.normalized_replay_cursors
    DROP COLUMN IF EXISTS range_start_floor_block_number;
