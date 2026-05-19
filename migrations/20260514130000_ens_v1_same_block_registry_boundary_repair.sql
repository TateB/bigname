-- Repair ENSv1 same-block authority-boundary rows written on the later
-- registrar resource even though their log_index precedes that registrar
-- resource's anchoring log.
--
-- Surface bindings are timestamp-shaped, but replay applies same-block logs in
-- log order. Rows before the registrar anchor in the boundary block still
-- belong to the previous registry-only authority.

CREATE TEMP TABLE ens_v1_same_block_registry_boundary_repointed_events AS
SELECT DISTINCT ON (event.normalized_event_id)
    event.normalized_event_id,
    event.canonicality_state,
    event.resource_id AS old_resource_id,
    previous_binding.resource_id AS expected_resource_id,
    current_binding.provenance->>'authority_key' AS old_authority_key,
    current_binding.provenance->>'authority_kind' AS old_authority_kind,
    previous_binding.provenance->>'authority_key' AS expected_authority_key,
    previous_binding.provenance->>'authority_kind' AS expected_authority_kind,
    event.before_state,
    before_revocation_state.repaired_before_state,
    event.after_state,
    after_revocation_state.repaired_after_state
FROM public.surface_bindings current_binding
JOIN public.resources current_resource
  ON current_resource.resource_id = current_binding.resource_id
 AND current_resource.provenance->>'authority_kind' = 'registrar'
 AND current_resource.provenance ? 'authority_key'
JOIN LATERAL (
    SELECT split_part(
        current_resource.provenance->>'authority_key',
        ':',
        6
    )::BIGINT AS boundary_log_index
) boundary ON TRUE
JOIN public.surface_bindings previous_binding
  ON previous_binding.logical_name_id = current_binding.logical_name_id
 AND previous_binding.active_to = current_binding.active_from
 AND previous_binding.resource_id <> current_binding.resource_id
 AND previous_binding.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.resources previous_resource
  ON previous_resource.resource_id = previous_binding.resource_id
 AND previous_resource.provenance->>'authority_kind' = 'registry_only'
JOIN public.normalized_events event
  ON event.resource_id = current_binding.resource_id
 AND event.logical_name_id = current_binding.logical_name_id
 AND event.block_hash = current_binding.block_hash
 AND event.log_index IS NOT NULL
 AND event.log_index < boundary.boundary_log_index
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN event.event_kind = 'PermissionChanged'
             AND event.before_state #>> '{grant_source,authority_key}' =
                 current_binding.provenance->>'authority_key'
            THEN jsonb_set(
                jsonb_set(
                    event.before_state,
                    '{grant_source,authority_key}',
                    to_jsonb(previous_binding.provenance->>'authority_key'),
                    false
                ),
                '{grant_source,authority_kind}',
                to_jsonb(previous_binding.provenance->>'authority_kind'),
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
                 current_binding.provenance->>'authority_key'
            THEN jsonb_set(
                jsonb_set(
                    before_grant_state.repaired_before_state,
                    '{revocation_source,authority_key}',
                    to_jsonb(previous_binding.provenance->>'authority_key'),
                    false
                ),
                '{revocation_source,authority_kind}',
                to_jsonb(previous_binding.provenance->>'authority_kind'),
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
                 current_binding.provenance->>'authority_key'
            THEN jsonb_set(
                jsonb_set(
                    event.after_state,
                    '{grant_source,authority_key}',
                    to_jsonb(previous_binding.provenance->>'authority_key'),
                    false
                ),
                '{grant_source,authority_kind}',
                to_jsonb(previous_binding.provenance->>'authority_kind'),
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
                 current_binding.provenance->>'authority_key'
            THEN jsonb_set(
                jsonb_set(
                    after_grant_state.repaired_after_state,
                    '{revocation_source,authority_key}',
                    to_jsonb(previous_binding.provenance->>'authority_key'),
                    false
                ),
                '{revocation_source,authority_kind}',
                to_jsonb(previous_binding.provenance->>'authority_kind'),
                false
            )
            ELSE after_grant_state.repaired_after_state
        END AS repaired_after_state
) after_revocation_state
WHERE current_binding.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND current_binding.observed_at >= TIMESTAMPTZ '2026-05-14 00:00:00+00'
  AND current_resource.provenance->>'authority_key' ~
      '^registrar:[^:]+:[0-9]+:0x[0-9a-fA-F]+:0x[0-9a-fA-F]+:[0-9]+$'
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
ORDER BY
    event.normalized_event_id,
    previous_binding.active_from DESC,
    previous_binding.surface_binding_id;

UPDATE public.normalized_events event
SET
    resource_id = repair.expected_resource_id,
    before_state = repair.repaired_before_state,
    after_state = repair.repaired_after_state
FROM ens_v1_same_block_registry_boundary_repointed_events repair
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
FROM ens_v1_same_block_registry_boundary_repointed_events;
