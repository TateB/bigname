-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS chain_lineage_orphaned_identity_idx
ON chain_lineage (chain_id, block_hash)
WHERE canonicality_state = 'orphaned'::canonicality_state;
