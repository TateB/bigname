CREATE INDEX IF NOT EXISTS name_surfaces_lower_namehash_idx
    ON name_surfaces (lower(namehash))
    WHERE labelhashes[1] IS NOT NULL
      AND canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      );
