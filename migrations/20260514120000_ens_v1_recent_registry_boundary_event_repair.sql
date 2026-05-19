-- Repair ENSv1 resolver/permission rows that were written against a
-- registry-only authority before a later replay shortened that registry-only
-- surface binding.
--
-- The registry-only interval itself is real. Only facts whose block timestamp
-- falls after that interval's active_to should move back to the surface binding
-- that actually covers the event timestamp.

CREATE TEMP TABLE ens_v1_recent_registry_boundary_repointed_events AS
SELECT DISTINCT ON (event.normalized_event_id)
    event.normalized_event_id,
    event.canonicality_state,
    event.resource_id AS old_resource_id,
    expected_binding.resource_id AS expected_resource_id,
    old_binding.provenance->>'authority_key' AS old_authority_key,
    old_binding.provenance->>'authority_kind' AS old_authority_kind,
    expected_binding.provenance->>'authority_key' AS expected_authority_key,
    expected_binding.provenance->>'authority_kind' AS expected_authority_kind,
    event.before_state,
    before_revocation_state.repaired_before_state,
    event.after_state,
    after_revocation_state.repaired_after_state
FROM public.surface_bindings old_binding
JOIN public.resources old_resource
  ON old_resource.resource_id = old_binding.resource_id
 AND old_resource.provenance->>'authority_kind' = 'registry_only'
JOIN public.normalized_events event
  ON event.resource_id = old_binding.resource_id
 AND event.logical_name_id = old_binding.logical_name_id
JOIN public.chain_lineage event_block
  ON event_block.chain_id = event.chain_id
 AND event_block.block_hash = event.block_hash
JOIN public.surface_bindings expected_binding
  ON expected_binding.logical_name_id = event.logical_name_id
 AND expected_binding.resource_id <> event.resource_id
 AND expected_binding.active_from <= event_block.block_timestamp
 AND (
     expected_binding.active_to IS NULL
     OR expected_binding.active_to > event_block.block_timestamp
 )
 AND expected_binding.canonicality_state IN ('canonical', 'safe', 'finalized')
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN event.event_kind = 'PermissionChanged'
             AND event.before_state #>> '{grant_source,authority_key}' =
                 old_binding.provenance->>'authority_key'
            THEN jsonb_set(
                jsonb_set(
                    event.before_state,
                    '{grant_source,authority_key}',
                    to_jsonb(expected_binding.provenance->>'authority_key'),
                    false
                ),
                '{grant_source,authority_kind}',
                to_jsonb(expected_binding.provenance->>'authority_kind'),
                false
            )
            ELSE event.before_state
        END AS repaired_before_state
) before_grant_state
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN event.event_kind = 'PermissionChanged'
             AND before_grant_state.repaired_before_state
                 #>> '{revocation_source,authority_key}' =
                 old_binding.provenance->>'authority_key'
            THEN jsonb_set(
                jsonb_set(
                    before_grant_state.repaired_before_state,
                    '{revocation_source,authority_key}',
                    to_jsonb(expected_binding.provenance->>'authority_key'),
                    false
                ),
                '{revocation_source,authority_kind}',
                to_jsonb(expected_binding.provenance->>'authority_kind'),
                false
            )
            ELSE before_grant_state.repaired_before_state
        END AS repaired_before_state
) before_revocation_state
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN event.event_kind = 'PermissionChanged'
             AND event.after_state #>> '{grant_source,authority_key}' =
                 old_binding.provenance->>'authority_key'
            THEN jsonb_set(
                jsonb_set(
                    event.after_state,
                    '{grant_source,authority_key}',
                    to_jsonb(expected_binding.provenance->>'authority_key'),
                    false
                ),
                '{grant_source,authority_kind}',
                to_jsonb(expected_binding.provenance->>'authority_kind'),
                false
            )
            ELSE event.after_state
        END AS repaired_after_state
) after_grant_state
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN event.event_kind = 'PermissionChanged'
             AND after_grant_state.repaired_after_state
                 #>> '{revocation_source,authority_key}' =
                 old_binding.provenance->>'authority_key'
            THEN jsonb_set(
                jsonb_set(
                    after_grant_state.repaired_after_state,
                    '{revocation_source,authority_key}',
                    to_jsonb(expected_binding.provenance->>'authority_key'),
                    false
                ),
                '{revocation_source,authority_kind}',
                to_jsonb(expected_binding.provenance->>'authority_kind'),
                false
            )
            ELSE after_grant_state.repaired_after_state
        END AS repaired_after_state
) after_revocation_state
WHERE old_binding.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND old_binding.active_to IS NOT NULL
  AND old_binding.observed_at >= TIMESTAMPTZ '2026-05-14 00:00:00+00'
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.event_kind IN (
      'ResolverChanged',
      'RecordChanged',
      'RecordVersionChanged',
      'PermissionChanged'
  )
  AND (
      event.event_kind <> 'ResolverChanged'
      OR COALESCE(event.after_state->>'source_event', '') <>
          'AuthorityEpochChanged'
  )
  AND event.block_number >= old_binding.block_number
  AND event_block.block_timestamp >= old_binding.active_to
ORDER BY
    event.normalized_event_id,
    expected_binding.active_from DESC,
    expected_binding.surface_binding_id;

UPDATE public.normalized_events event
SET
    resource_id = repair.expected_resource_id,
    before_state = repair.repaired_before_state,
    after_state = repair.repaired_after_state
FROM ens_v1_recent_registry_boundary_repointed_events repair
WHERE event.normalized_event_id = repair.normalized_event_id
  AND event.resource_id = repair.old_resource_id
  AND event.before_state = repair.before_state
  AND event.after_state = repair.after_state;

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
FROM ens_v1_recent_registry_boundary_repointed_events;
