ALTER TABLE public.normalized_replay_cursors
    ADD COLUMN range_start_floor_block_number bigint;

ALTER TABLE public.normalized_replay_cursors
    ADD CONSTRAINT normalized_replay_cursors_range_start_floor_block_number_check
    CHECK (
        range_start_floor_block_number IS NULL
        OR range_start_floor_block_number >= 0
    );
