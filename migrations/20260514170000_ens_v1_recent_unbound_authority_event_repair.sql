-- Repair recent ENSv1 unwrapped events that were written with a resource_id
-- even though no canonical surface binding is active at the event timestamp.
--
-- ResolverChanged remains a name-scoped event when there is no active
-- authority, so its resource_id must be NULL. Record and permission changes are
-- resource-authority facts and current replay omits them when there is no
-- active authority; stale rows from older live sync are orphaned.

CREATE TEMP TABLE ens_v1_recent_unbound_authority_events AS
SELECT
    event.normalized_event_id,
    event.event_kind,
    event.canonicality_state
FROM public.normalized_events event
JOIN public.chain_lineage block
  ON block.chain_id = event.chain_id
 AND block.block_hash = event.block_hash
WHERE event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND event.observed_at >= TIMESTAMPTZ '2026-05-14 00:00:00+00'
  AND event.logical_name_id IS NOT NULL
  AND event.resource_id IS NOT NULL
  AND event.event_kind IN (
      'ResolverChanged',
      'RecordChanged',
      'RecordVersionChanged',
      'PermissionChanged'
  )
  AND NOT EXISTS (
      SELECT 1
      FROM public.surface_bindings binding
      WHERE binding.logical_name_id = event.logical_name_id
        AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
        AND binding.active_from <= block.block_timestamp
        AND (
            binding.active_to IS NULL
            OR block.block_timestamp < binding.active_to
        )
  );

CREATE INDEX ens_v1_recent_unbound_authority_events_kind_idx
    ON ens_v1_recent_unbound_authority_events (
        event_kind,
        normalized_event_id
    );

UPDATE public.normalized_events event
SET resource_id = NULL
FROM ens_v1_recent_unbound_authority_events repair
WHERE event.normalized_event_id = repair.normalized_event_id
  AND repair.event_kind = 'ResolverChanged'
  AND event.resource_id IS NOT NULL;

UPDATE public.normalized_events event
SET canonicality_state = 'orphaned'
FROM ens_v1_recent_unbound_authority_events repair
WHERE event.normalized_event_id = repair.normalized_event_id
  AND repair.event_kind IN (
      'RecordChanged',
      'RecordVersionChanged',
      'PermissionChanged'
  );

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
    canonicality_state
FROM ens_v1_recent_unbound_authority_events;
