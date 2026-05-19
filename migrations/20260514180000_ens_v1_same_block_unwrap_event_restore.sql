-- Restore resolver/permission rows that precede a wrapper NameUnwrapped log in
-- the same block.
--
-- 20260514170000 repaired genuinely unbound recent events, but timestamp-only
-- binding checks are not enough for same-block boundaries. If a resolver log
-- precedes NameUnwrapped, the wrapper authority is still active for that log.

CREATE TEMP TABLE ens_v1_same_block_unwrap_event_restore AS
SELECT DISTINCT ON (event.normalized_event_id)
    event.normalized_event_id,
    event.event_kind,
    event.canonicality_state,
    binding.resource_id AS expected_resource_id
FROM public.normalized_events event
JOIN public.chain_lineage block
  ON block.chain_id = event.chain_id
 AND block.block_hash = event.block_hash
JOIN public.surface_bindings binding
  ON binding.logical_name_id = event.logical_name_id
 AND binding.active_to = block.block_timestamp
 AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.resources expected
  ON expected.resource_id = binding.resource_id
 AND expected.provenance->>'authority_kind' = 'wrapper'
JOIN public.raw_logs unwrap
  ON unwrap.chain_id = event.chain_id
 AND unwrap.block_hash = event.block_hash
 AND unwrap.log_index > event.log_index
 AND unwrap.topics[1] =
     '0xee2ba1195c65bcf218a83d874335c6bf9d9067b4c672f3c3bf16cf40de7586c4'
 AND unwrap.topics[2] = expected.provenance->>'namehash'
 AND unwrap.canonicality_state IN ('canonical', 'safe', 'finalized')
WHERE event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.observed_at >= TIMESTAMPTZ '2026-05-14 00:00:00+00'
  AND (
      (
          event.event_kind = 'ResolverChanged'
          AND event.resource_id IS NULL
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      )
      OR (
          event.event_kind = 'PermissionChanged'
          AND event.resource_id = binding.resource_id
          AND event.canonicality_state = 'orphaned'
      )
  )
ORDER BY event.normalized_event_id, unwrap.log_index;

UPDATE public.normalized_events event
SET resource_id = repair.expected_resource_id
FROM ens_v1_same_block_unwrap_event_restore repair
WHERE event.normalized_event_id = repair.normalized_event_id
  AND repair.event_kind = 'ResolverChanged'
  AND event.resource_id IS NULL;

UPDATE public.normalized_events event
SET canonicality_state = 'canonical'
FROM ens_v1_same_block_unwrap_event_restore repair
WHERE event.normalized_event_id = repair.normalized_event_id
  AND repair.event_kind = 'PermissionChanged'
  AND event.canonicality_state = 'orphaned';

INSERT INTO public.projection_normalized_event_changes (
    normalized_event_id,
    changed_at,
    change_kind,
    canonicality_state
)
SELECT
    normalized_event_id,
    now(),
    'canonicality_update',
    CASE
        WHEN event_kind = 'PermissionChanged'
        THEN 'canonical'::canonicality_state
        ELSE canonicality_state
    END
FROM ens_v1_same_block_unwrap_event_restore;
