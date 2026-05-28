-- no-transaction

-- Development builds of the compact feed branch briefly created narrower
-- prefix-equivalent address_names_current indexes before adding the compact
-- covering pair. Drop them for already-migrated branch databases; fresh
-- databases only build the covering pair in 20260521143000.
DROP INDEX IF EXISTS address_names_current_identity_feed_sort_idx;
DROP INDEX IF EXISTS address_names_current_identity_claim_name_idx;
