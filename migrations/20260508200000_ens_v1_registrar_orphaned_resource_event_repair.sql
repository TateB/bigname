-- Repair canonical events that still point at orphaned ENSv1 registrar
-- resources after 20260508130000.
--
-- The earlier repair orphaned synthetic registrar resources that had been
-- created for renewals of still-live registrar leases, but some later
-- canonical events on those resources remained. Replay can then preload the
-- orphaned resource from those later events and disagree with the corrected
-- renewal resource. Repoint real resource events to the prior canonical
-- registrar resource and orphan synthetic binding/transition scaffolding.

CREATE TEMP TABLE ens_v1_registrar_orphaned_resource_event_repair_map AS
WITH affected_resources AS (
    SELECT DISTINCT event.resource_id AS orphaned_resource_id
    FROM public.normalized_events event
    JOIN public.resources resource
      ON resource.resource_id = event.resource_id
    WHERE event.derivation_kind = 'ens_v1_unwrapped_authority'
      AND event.chain_id = 'ethereum-mainnet'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND resource.canonicality_state = 'orphaned'
      AND resource.provenance->>'authority_kind' = 'registrar'
),
resource_scope AS (
    SELECT
        resource.resource_id AS orphaned_resource_id,
        resource.chain_id,
        resource.block_hash,
        resource.block_number,
        resource.provenance->>'authority_key' AS old_authority_key,
        resource.provenance->>'logical_name_id' AS logical_name_id,
        resource.provenance->>'labelhash' AS labelhash,
        split_part(resource.provenance->>'authority_key', ':', 6)::BIGINT AS log_index
    FROM public.resources resource
    JOIN affected_resources affected
      ON affected.orphaned_resource_id = resource.resource_id
    WHERE resource.provenance->>'authority_kind' = 'registrar'
      AND resource.provenance ? 'authority_key'
)
SELECT DISTINCT ON (scope.orphaned_resource_id)
    scope.orphaned_resource_id,
    renewal.resource_id AS expected_resource_id,
    scope.old_authority_key,
    expected.provenance->>'authority_key' AS expected_authority_key
FROM resource_scope scope
JOIN public.normalized_events renewal
  ON renewal.chain_id = scope.chain_id
 AND renewal.block_hash = scope.block_hash
 AND renewal.log_index = scope.log_index
 AND renewal.logical_name_id = scope.logical_name_id
 AND renewal.event_kind = 'RegistrationRenewed'
 AND renewal.derivation_kind = 'ens_v1_unwrapped_authority'
 AND renewal.source_family = 'ens_v1_registrar_l1'
 AND renewal.canonicality_state IN ('canonical', 'safe', 'finalized')
 AND renewal.resource_id IS NOT NULL
 AND renewal.resource_id <> scope.orphaned_resource_id
JOIN public.resources expected
  ON expected.resource_id = renewal.resource_id
 AND expected.canonicality_state IN ('canonical', 'safe', 'finalized')
 AND expected.provenance->>'authority_kind' = 'registrar'
ORDER BY scope.orphaned_resource_id, renewal.normalized_event_id;

CREATE INDEX ens_v1_registrar_orphaned_resource_event_repair_map_old_idx
    ON ens_v1_registrar_orphaned_resource_event_repair_map (orphaned_resource_id);

CREATE TEMP TABLE ens_v1_registrar_orphaned_resource_repointed_events AS
SELECT
    event.normalized_event_id,
    event.canonicality_state,
    event.event_identity AS old_event_identity,
    CASE
        WHEN event.event_kind = 'RegistrationReleased'
        THEN replace(
            event.event_identity,
            repair.old_authority_key,
            repair.expected_authority_key
        )
        ELSE event.event_identity
    END AS repaired_event_identity,
    event.resource_id AS old_resource_id,
    repair.expected_resource_id,
    event.before_state,
    before_revocation_state.repaired_before_state,
    event.after_state,
    after_revocation_state.repaired_after_state
FROM ens_v1_registrar_orphaned_resource_event_repair_map repair
JOIN public.normalized_events event
  ON event.resource_id = repair.orphaned_resource_id
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN event.event_kind = 'PermissionChanged'
             AND event.before_state #>> '{grant_source,authority_key}' = repair.old_authority_key
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
             AND before_grant_state.repaired_before_state #>> '{revocation_source,authority_key}' =
                 repair.old_authority_key
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
             AND event.after_state #>> '{grant_source,authority_key}' = repair.old_authority_key
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
             AND after_grant_state.repaired_after_state #>> '{revocation_source,authority_key}' =
                 repair.old_authority_key
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
      'RegistrationRenewed',
      'ExpiryChanged',
      'TokenControlTransferred',
      'ResolverChanged',
      'RecordChanged',
      'RecordVersionChanged',
      'PermissionChanged',
      'RegistrationReleased'
  )
  AND (
      event.event_kind <> 'ResolverChanged'
      OR COALESCE(event.after_state->>'source_event', '') <> 'AuthorityEpochChanged'
  );

UPDATE public.normalized_events event
SET
    event_identity = repair.repaired_event_identity,
    resource_id = repair.expected_resource_id,
    before_state = repair.repaired_before_state,
    after_state = repair.repaired_after_state
FROM ens_v1_registrar_orphaned_resource_repointed_events repair
WHERE event.normalized_event_id = repair.normalized_event_id
  AND event.event_identity = repair.old_event_identity
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
FROM ens_v1_registrar_orphaned_resource_repointed_events;

UPDATE public.normalized_events event
SET canonicality_state = 'orphaned'
FROM ens_v1_registrar_orphaned_resource_event_repair_map repair
WHERE event.resource_id = repair.orphaned_resource_id
  AND event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND (
      event.event_kind IN (
          'RegistrationGranted',
          'SurfaceBound',
          'SurfaceUnbound',
          'AuthorityEpochChanged'
      )
      OR (
          event.event_kind = 'ResolverChanged'
          AND event.after_state->>'source_event' = 'AuthorityEpochChanged'
      )
  );

UPDATE public.surface_bindings binding
SET canonicality_state = 'orphaned'
FROM ens_v1_registrar_orphaned_resource_event_repair_map repair
WHERE binding.resource_id = repair.orphaned_resource_id
  AND binding.canonicality_state IN ('canonical', 'safe', 'finalized');
