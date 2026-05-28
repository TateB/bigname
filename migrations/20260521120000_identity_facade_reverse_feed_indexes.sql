-- no-transaction

-- This migration version originally held narrower reverse-feed helper indexes.
-- The compact covering indexes in 20260521143000 now own that planner surface,
-- so fresh databases should not build a duplicate prefix pair here.
DROP INDEX IF EXISTS address_names_current_identity_feed_sort_idx;
DROP INDEX IF EXISTS address_names_current_identity_claim_name_idx;
