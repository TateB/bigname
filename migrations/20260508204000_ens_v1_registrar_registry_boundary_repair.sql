-- Repair ENSv1 registrar histories where a registry owner observation was
-- previously materialized as a registrar -> registry-only authority boundary
-- even though the registrar lease was still live and the registry owner
-- matched the current registrar owner.
--
-- Current replay keeps resolver-local facts on the registrar resource in that
-- window. These stale synthetic boundaries make replay disagree on resource_id
-- for later RecordChanged/RecordVersionChanged/ResolverChanged rows.

CREATE TEMP TABLE ens_v1_registrar_registry_boundary_repair_map AS
SELECT DISTINCT ON (epoch.normalized_event_id)
    epoch.logical_name_id,
    epoch.block_number AS boundary_block_number,
    epoch.block_hash AS boundary_block_hash,
    block.block_timestamp AS boundary_timestamp,
    epoch.resource_id AS old_resource_id,
    expected.resource_id AS expected_resource_id,
    old_binding.surface_binding_id AS old_surface_binding_id,
    expected_binding.surface_binding_id AS expected_surface_binding_id,
    epoch.before_state->>'authority_key' AS expected_authority_key,
    epoch.after_state->>'authority_key' AS old_authority_key,
    (expected.provenance->>'expiry')::BIGINT + 7776000 AS release_timestamp
FROM public.normalized_events epoch
JOIN public.chain_lineage block
  ON block.chain_id = epoch.chain_id
 AND block.block_hash = epoch.block_hash
JOIN public.resources old_resource
  ON old_resource.resource_id = epoch.resource_id
 AND old_resource.provenance->>'authority_kind' = 'registry_only'
 AND old_resource.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.resources expected
  ON expected.provenance->>'authority_kind' = 'registrar'
 AND expected.provenance->>'authority_key' = epoch.before_state->>'authority_key'
 AND expected.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.normalized_events permission
  ON permission.logical_name_id = epoch.logical_name_id
 AND permission.block_hash = epoch.block_hash
 AND permission.resource_id = epoch.resource_id
 AND permission.event_kind = 'PermissionChanged'
 AND permission.source_family = 'ens_v1_registry_l1'
 AND permission.after_state #>> '{grant_source,authority_key}' =
     epoch.after_state->>'authority_key'
 AND permission.after_state #>> '{scope,kind}' = 'resource'
 AND permission.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.surface_bindings old_binding
  ON old_binding.resource_id = epoch.resource_id
 AND old_binding.logical_name_id = epoch.logical_name_id
 AND old_binding.active_from = block.block_timestamp
 AND old_binding.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.surface_bindings expected_binding
  ON expected_binding.resource_id = expected.resource_id
 AND expected_binding.logical_name_id = epoch.logical_name_id
 AND expected_binding.active_to = block.block_timestamp
 AND expected_binding.canonicality_state IN ('canonical', 'safe', 'finalized')
WHERE epoch.derivation_kind = 'ens_v1_unwrapped_authority'
  AND epoch.chain_id = 'ethereum-mainnet'
  AND epoch.event_kind = 'AuthorityEpochChanged'
  AND epoch.source_family = 'ens_v1_registry_l1'
  AND epoch.before_state->>'authority_kind' = 'registrar'
  AND epoch.after_state->>'authority_kind' = 'registry_only'
  AND epoch.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND expected.provenance->>'expiry' ~ '^[0-9]+$'
  AND (expected.provenance->>'expiry')::BIGINT + 7776000 >
      EXTRACT(EPOCH FROM block.block_timestamp)::BIGINT
  AND lower(expected.provenance->>'registrant') =
      lower(permission.after_state->>'subject')
ORDER BY epoch.normalized_event_id, permission.normalized_event_id;

CREATE INDEX ens_v1_registrar_registry_boundary_repair_old_idx
    ON ens_v1_registrar_registry_boundary_repair_map (
        old_resource_id,
        logical_name_id,
        boundary_block_number
    );

CREATE INDEX ens_v1_registrar_registry_boundary_repair_boundary_idx
    ON ens_v1_registrar_registry_boundary_repair_map (
        boundary_block_hash,
        logical_name_id
    );

CREATE INDEX ens_v1_registrar_registry_boundary_repair_binding_idx
    ON ens_v1_registrar_registry_boundary_repair_map (
        old_surface_binding_id,
        expected_surface_binding_id
    );

CREATE TEMP TABLE ens_v1_registrar_registry_boundary_repointed_events AS
SELECT DISTINCT ON (event.normalized_event_id)
    event.normalized_event_id,
    event.canonicality_state,
    event.resource_id AS old_resource_id,
    repair.expected_resource_id,
    event.before_state,
    before_revocation_state.repaired_before_state,
    event.after_state,
    after_revocation_state.repaired_after_state
FROM ens_v1_registrar_registry_boundary_repair_map repair
JOIN public.normalized_events event
  ON event.resource_id = repair.old_resource_id
 AND event.logical_name_id = repair.logical_name_id
 AND event.block_number >= repair.boundary_block_number
JOIN public.chain_lineage event_block
  ON event_block.chain_id = event.chain_id
 AND event_block.block_hash = event.block_hash
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN event.event_kind = 'PermissionChanged'
             AND event.before_state #>> '{grant_source,authority_key}' =
                 repair.old_authority_key
            THEN jsonb_set(
                event.before_state,
                '{grant_source,authority_key}',
                to_jsonb(repair.expected_authority_key),
                false
            )
            ELSE event.before_state
        END AS repaired_before_state
) before_grant_state
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN event.event_kind = 'PermissionChanged'
             AND before_grant_state.repaired_before_state #>>
                 '{revocation_source,authority_key}' = repair.old_authority_key
            THEN jsonb_set(
                before_grant_state.repaired_before_state,
                '{revocation_source,authority_key}',
                to_jsonb(repair.expected_authority_key),
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
                 repair.old_authority_key
            THEN jsonb_set(
                event.after_state,
                '{grant_source,authority_key}',
                to_jsonb(repair.expected_authority_key),
                false
            )
            ELSE event.after_state
        END AS repaired_after_state
) after_grant_state
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN event.event_kind = 'PermissionChanged'
             AND after_grant_state.repaired_after_state #>>
                 '{revocation_source,authority_key}' = repair.old_authority_key
            THEN jsonb_set(
                after_grant_state.repaired_after_state,
                '{revocation_source,authority_key}',
                to_jsonb(repair.expected_authority_key),
                false
            )
            ELSE after_grant_state.repaired_after_state
        END AS repaired_after_state
) after_revocation_state
WHERE event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND event.event_kind IN (
      'ResolverChanged',
      'RecordChanged',
      'RecordVersionChanged',
      'PermissionChanged'
  )
  AND (
      event.event_kind <> 'ResolverChanged'
      OR COALESCE(event.after_state->>'source_event', '') <> 'AuthorityEpochChanged'
  )
  AND EXTRACT(EPOCH FROM event_block.block_timestamp)::BIGINT <
      repair.release_timestamp
  AND NOT (
      event.block_hash = repair.boundary_block_hash
      AND event.event_kind = 'PermissionChanged'
      AND event.after_state #>> '{grant_source,authority_key}' =
          repair.old_authority_key
  )
  AND (
      event.block_number > repair.boundary_block_number
      OR event.log_index IS NOT NULL
  )
ORDER BY event.normalized_event_id, repair.boundary_block_number DESC;

UPDATE public.normalized_events event
SET
    resource_id = repair.expected_resource_id,
    before_state = repair.repaired_before_state,
    after_state = repair.repaired_after_state
FROM ens_v1_registrar_registry_boundary_repointed_events repair
WHERE event.normalized_event_id = repair.normalized_event_id
  AND event.resource_id = repair.old_resource_id
  AND event.before_state = repair.before_state
  AND event.after_state = repair.after_state;

CREATE TEMP TABLE ens_v1_registrar_registry_boundary_orphaned_events AS
SELECT DISTINCT event.normalized_event_id, event.canonicality_state
FROM ens_v1_registrar_registry_boundary_repair_map repair
JOIN public.normalized_events event
  ON event.logical_name_id = repair.logical_name_id
 AND event.block_hash = repair.boundary_block_hash
WHERE event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND (
      (
          event.resource_id = repair.old_resource_id
          AND (
              event.event_kind IN ('SurfaceBound', 'AuthorityEpochChanged')
              OR (
                  event.event_kind = 'ResolverChanged'
                  AND event.after_state->>'source_event' = 'AuthorityEpochChanged'
              )
              OR (
                  event.event_kind = 'PermissionChanged'
                  AND event.after_state #>> '{grant_source,authority_key}' =
                      repair.old_authority_key
              )
          )
      )
      OR (
          event.resource_id = repair.expected_resource_id
          AND event.event_kind = 'SurfaceUnbound'
          AND event.after_state->>'authority_key' =
              repair.expected_authority_key
      )
  );

UPDATE public.normalized_events event
SET canonicality_state = 'orphaned'
FROM ens_v1_registrar_registry_boundary_orphaned_events repair
WHERE event.normalized_event_id = repair.normalized_event_id;

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
FROM ens_v1_registrar_registry_boundary_repointed_events
UNION ALL
SELECT
    normalized_event_id,
    now(),
    'canonicality_update',
    canonicality_state
FROM ens_v1_registrar_registry_boundary_orphaned_events;

UPDATE public.surface_bindings binding
SET canonicality_state = 'orphaned'
FROM ens_v1_registrar_registry_boundary_repair_map repair
WHERE binding.surface_binding_id = repair.old_surface_binding_id
  AND binding.canonicality_state IN ('canonical', 'safe', 'finalized');

UPDATE public.surface_bindings binding
SET active_to = old_binding.active_to
FROM ens_v1_registrar_registry_boundary_repair_map repair
JOIN public.surface_bindings old_binding
  ON old_binding.surface_binding_id = repair.old_surface_binding_id
WHERE binding.surface_binding_id = repair.expected_surface_binding_id
  AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND (
      binding.active_to IS DISTINCT FROM old_binding.active_to
      OR (
          binding.active_to IS NOT NULL
          AND old_binding.active_to IS NULL
      )
  );
