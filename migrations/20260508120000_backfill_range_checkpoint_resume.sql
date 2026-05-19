ALTER TABLE backfill_ranges
    DROP CONSTRAINT backfill_ranges_check1;

UPDATE backfill_ranges
SET checkpoint_block_number = range_start_block_number - 1
WHERE status = 'pending'::backfill_lifecycle_status
  AND attempt_count = 0
  AND checkpoint_block_number = range_start_block_number;

ALTER TABLE backfill_ranges
    ADD CONSTRAINT backfill_ranges_check1 CHECK (
        checkpoint_block_number >= range_start_block_number - 1
        AND checkpoint_block_number <= range_end_block_number
    );
