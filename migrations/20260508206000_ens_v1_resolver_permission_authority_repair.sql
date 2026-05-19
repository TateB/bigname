-- Repair ENSv1 resolver permission rows whose authority metadata drifted from
-- their sibling ResolverChanged event.
--
-- ResolverChanged establishes the active resource for the resolver mutation.
-- The paired PermissionChanged rows must use that same resource and authority
-- key/kind for resolver-control grants and revocations.

CREATE TEMP TABLE ens_v1_resolver_permission_authority_repair AS
SELECT
    permission.normalized_event_id,
    permission.canonicality_state,
    permission.resource_id AS old_resource_id,
    resolver_event.resource_id AS expected_resource_id,
    resolver_resource.provenance->>'authority_key' AS expected_authority_key,
    resolver_resource.provenance->>'authority_kind' AS expected_authority_kind,
    permission.before_state,
    before_revocation_state.repaired_before_state,
    permission.after_state,
    after_revocation_state.repaired_after_state
FROM public.normalized_events permission
JOIN public.normalized_events resolver_event
  ON resolver_event.chain_id = permission.chain_id
 AND resolver_event.logical_name_id = permission.logical_name_id
 AND resolver_event.block_hash = permission.block_hash
 AND resolver_event.transaction_hash IS NOT DISTINCT FROM
     permission.transaction_hash
 AND resolver_event.log_index IS NOT DISTINCT FROM permission.log_index
 AND resolver_event.event_kind = 'ResolverChanged'
 AND resolver_event.derivation_kind = permission.derivation_kind
 AND resolver_event.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.resources resolver_resource
  ON resolver_resource.resource_id = resolver_event.resource_id
 AND resolver_resource.canonicality_state IN ('canonical', 'safe', 'finalized')
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN permission.before_state #>>
                 '{grant_source,source_event_kind}' = 'ResolverChanged'
            THEN jsonb_set(
                jsonb_set(
                    permission.before_state,
                    '{grant_source,authority_key}',
                    to_jsonb(resolver_resource.provenance->>'authority_key'),
                    false
                ),
                '{grant_source,authority_kind}',
                to_jsonb(resolver_resource.provenance->>'authority_kind'),
                false
            )
            ELSE permission.before_state
        END AS repaired_before_state
) before_grant_state
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN before_grant_state.repaired_before_state #>>
                 '{revocation_source,source_event_kind}' = 'ResolverChanged'
            THEN jsonb_set(
                jsonb_set(
                    before_grant_state.repaired_before_state,
                    '{revocation_source,authority_key}',
                    to_jsonb(resolver_resource.provenance->>'authority_key'),
                    false
                ),
                '{revocation_source,authority_kind}',
                to_jsonb(resolver_resource.provenance->>'authority_kind'),
                false
            )
            ELSE before_grant_state.repaired_before_state
        END AS repaired_before_state
) before_revocation_state
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN permission.after_state #>>
                 '{grant_source,source_event_kind}' = 'ResolverChanged'
            THEN jsonb_set(
                jsonb_set(
                    permission.after_state,
                    '{grant_source,authority_key}',
                    to_jsonb(resolver_resource.provenance->>'authority_key'),
                    false
                ),
                '{grant_source,authority_kind}',
                to_jsonb(resolver_resource.provenance->>'authority_kind'),
                false
            )
            ELSE permission.after_state
        END AS repaired_after_state
) after_grant_state
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN after_grant_state.repaired_after_state #>>
                 '{revocation_source,source_event_kind}' = 'ResolverChanged'
            THEN jsonb_set(
                jsonb_set(
                    after_grant_state.repaired_after_state,
                    '{revocation_source,authority_key}',
                    to_jsonb(resolver_resource.provenance->>'authority_key'),
                    false
                ),
                '{revocation_source,authority_kind}',
                to_jsonb(resolver_resource.provenance->>'authority_kind'),
                false
            )
            ELSE after_grant_state.repaired_after_state
        END AS repaired_after_state
) after_revocation_state
WHERE permission.derivation_kind = 'ens_v1_unwrapped_authority'
  AND permission.chain_id = 'ethereum-mainnet'
  AND permission.event_kind = 'PermissionChanged'
  AND permission.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND (
      permission.before_state #>> '{grant_source,source_event_kind}' =
          'ResolverChanged'
      OR permission.before_state #>> '{revocation_source,source_event_kind}' =
          'ResolverChanged'
      OR permission.after_state #>> '{grant_source,source_event_kind}' =
          'ResolverChanged'
      OR permission.after_state #>> '{revocation_source,source_event_kind}' =
          'ResolverChanged'
  )
  AND (
      permission.resource_id IS DISTINCT FROM resolver_event.resource_id
      OR before_revocation_state.repaired_before_state IS DISTINCT FROM
          permission.before_state
      OR after_revocation_state.repaired_after_state IS DISTINCT FROM
          permission.after_state
  );

CREATE INDEX ens_v1_resolver_permission_authority_repair_event_idx
    ON ens_v1_resolver_permission_authority_repair (normalized_event_id);

UPDATE public.normalized_events permission
SET
    resource_id = repair.expected_resource_id,
    before_state = repair.repaired_before_state,
    after_state = repair.repaired_after_state
FROM ens_v1_resolver_permission_authority_repair repair
WHERE permission.normalized_event_id = repair.normalized_event_id
  AND permission.resource_id = repair.old_resource_id
  AND permission.before_state = repair.before_state
  AND permission.after_state = repair.after_state;

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
FROM ens_v1_resolver_permission_authority_repair;
