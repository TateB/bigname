-- Repair ENSv1 registry-only permission rows that were historically written
-- on the registrar resource while carrying registry-only authority metadata.
--
-- Current replay emits those grants/revocations against the registry-only
-- resource and uses the registry-only authority key. The malformed rows block
-- normalized replay because their stable event identities match incoming
-- events but their resource_id and embedded grant source differ.

CREATE TEMP TABLE ens_v1_registry_only_permission_resource_repair AS
SELECT
    event.normalized_event_id,
    event.canonicality_state,
    event.resource_id AS old_resource_id,
    registry_resource.resource_id AS expected_resource_id,
    registry_resource.provenance->>'authority_key' AS expected_authority_key,
    event.before_state,
    before_revocation_state.repaired_before_state,
    event.after_state,
    after_revocation_state.repaired_after_state
FROM public.normalized_events event
JOIN public.resources registrar_resource
  ON registrar_resource.resource_id = event.resource_id
 AND registrar_resource.provenance->>'authority_kind' = 'registrar'
 AND registrar_resource.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.resources registry_resource
  ON registry_resource.chain_id = event.chain_id
 AND registry_resource.provenance->>'authority_kind' = 'registry_only'
 AND registry_resource.provenance->>'logical_name_id' =
     registrar_resource.provenance->>'logical_name_id'
 AND registry_resource.provenance->>'authority_key' =
     CONCAT(
         'registry-only:',
         event.chain_id,
         ':',
         registrar_resource.provenance->>'labelhash'
     )
 AND registry_resource.canonicality_state IN ('canonical', 'safe', 'finalized')
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN event.before_state #>> '{grant_source,authority_kind}' =
                 'registry_only'
             AND event.before_state #>> '{grant_source,authority_key}' LIKE
                 'registrar:%'
            THEN jsonb_set(
                event.before_state,
                '{grant_source,authority_key}',
                to_jsonb(registry_resource.provenance->>'authority_key'),
                false
            )
            ELSE event.before_state
        END AS repaired_before_state
) before_grant_state
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN before_grant_state.repaired_before_state #>>
                 '{revocation_source,authority_kind}' = 'registry_only'
             AND before_grant_state.repaired_before_state #>>
                 '{revocation_source,authority_key}' LIKE 'registrar:%'
            THEN jsonb_set(
                before_grant_state.repaired_before_state,
                '{revocation_source,authority_key}',
                to_jsonb(registry_resource.provenance->>'authority_key'),
                false
            )
            ELSE before_grant_state.repaired_before_state
        END AS repaired_before_state
) before_revocation_state
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN event.after_state #>> '{grant_source,authority_kind}' =
                 'registry_only'
             AND event.after_state #>> '{grant_source,authority_key}' LIKE
                 'registrar:%'
            THEN jsonb_set(
                event.after_state,
                '{grant_source,authority_key}',
                to_jsonb(registry_resource.provenance->>'authority_key'),
                false
            )
            ELSE event.after_state
        END AS repaired_after_state
) after_grant_state
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN after_grant_state.repaired_after_state #>>
                 '{revocation_source,authority_kind}' = 'registry_only'
             AND after_grant_state.repaired_after_state #>>
                 '{revocation_source,authority_key}' LIKE 'registrar:%'
            THEN jsonb_set(
                after_grant_state.repaired_after_state,
                '{revocation_source,authority_key}',
                to_jsonb(registry_resource.provenance->>'authority_key'),
                false
            )
            ELSE after_grant_state.repaired_after_state
        END AS repaired_after_state
) after_revocation_state
WHERE event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.event_kind = 'PermissionChanged'
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND (
      (
          event.before_state #>> '{grant_source,authority_kind}' =
              'registry_only'
          AND event.before_state #>> '{grant_source,authority_key}' LIKE
              'registrar:%'
      )
      OR (
          event.before_state #>> '{revocation_source,authority_kind}' =
              'registry_only'
          AND event.before_state #>> '{revocation_source,authority_key}'
              LIKE 'registrar:%'
      )
      OR (
          event.after_state #>> '{grant_source,authority_kind}' =
              'registry_only'
          AND event.after_state #>> '{grant_source,authority_key}' LIKE
              'registrar:%'
      )
      OR (
          event.after_state #>> '{revocation_source,authority_kind}' =
              'registry_only'
          AND event.after_state #>> '{revocation_source,authority_key}'
              LIKE 'registrar:%'
      )
  );

CREATE INDEX ens_v1_registry_only_permission_resource_repair_event_idx
    ON ens_v1_registry_only_permission_resource_repair (normalized_event_id);

UPDATE public.normalized_events event
SET
    resource_id = repair.expected_resource_id,
    before_state = repair.repaired_before_state,
    after_state = repair.repaired_after_state
FROM ens_v1_registry_only_permission_resource_repair repair
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
FROM ens_v1_registry_only_permission_resource_repair;
