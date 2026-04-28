-- Phase 10 operational cursor for automatic raw-fact normalized-event replay.
--
-- This table persists indexer-owned replay progress only. It does not define
-- chain canonicality, does not replace backfill range checkpoints, and does
-- not own adapter normalized-event writes.

CREATE TABLE normalized_replay_cursors (
  deployment_profile TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  cursor_kind TEXT NOT NULL,
  range_start_block_number BIGINT NOT NULL CHECK (range_start_block_number >= 0),
  next_block_number BIGINT NOT NULL CHECK (next_block_number >= range_start_block_number),
  target_block_number BIGINT NOT NULL CHECK (target_block_number >= range_start_block_number),
  last_completed_block_number BIGINT CHECK (last_completed_block_number IS NULL OR last_completed_block_number >= range_start_block_number),
  last_selected_block_count BIGINT NOT NULL DEFAULT 0 CHECK (last_selected_block_count >= 0),
  last_canonical_raw_log_count BIGINT NOT NULL DEFAULT 0 CHECK (last_canonical_raw_log_count >= 0),
  last_scanned_raw_log_count BIGINT NOT NULL DEFAULT 0 CHECK (last_scanned_raw_log_count >= 0),
  last_matched_raw_log_count BIGINT NOT NULL DEFAULT 0 CHECK (last_matched_raw_log_count >= 0),
  last_normalized_event_synced_count BIGINT NOT NULL DEFAULT 0 CHECK (last_normalized_event_synced_count >= 0),
  last_normalized_event_inserted_count BIGINT NOT NULL DEFAULT 0 CHECK (last_normalized_event_inserted_count >= 0),
  last_replayed_at TIMESTAMPTZ,
  last_failure_reason TEXT,
  last_failure_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_profile, chain_id, cursor_kind),
  CHECK (next_block_number <= target_block_number + 1)
);

CREATE INDEX normalized_replay_cursors_progress_idx
  ON normalized_replay_cursors (deployment_profile, chain_id, cursor_kind, next_block_number, target_block_number);
