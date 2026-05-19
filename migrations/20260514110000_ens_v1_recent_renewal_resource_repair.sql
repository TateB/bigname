-- Repair ENSv1 registrar renewal rows written after the earlier renewal
-- resource repairs had already run.
--
-- A live replay can still have stale rows when an older adapter image saw a
-- renewal without its prior registrar lease, synthesized a same-log registrar
-- resource, and then the corrected adapter later replays the same log against
-- the still-live prior registrar resource. Keep normalized-event replay strict:
-- repair the stale synthetic resource rows instead of accepting two resources
-- for one normalized-event identity.

CREATE TEMP TABLE ens_v1_recent_renewal_repair_renewals AS
WITH prior_repair AS (
    SELECT COALESCE(
        (
            SELECT installed_on
            FROM public._sqlx_migrations
            WHERE version = 20260508203000
        ),
        TIMESTAMPTZ 'epoch'
    ) AS installed_on
)
SELECT
    renewal.normalized_event_id,
    renewal.logical_name_id,
    renewal.resource_id AS old_resource_id,
    renewal.block_number,
    renewal.block_hash,
    renewal.transaction_hash,
    renewal.log_index,
    renewal.after_state->>'labelhash' AS labelhash,
    block.block_timestamp,
    old_resource.provenance->>'authority_key' AS old_authority_key
FROM public.normalized_events renewal
JOIN public.normalized_events granted
  ON granted.chain_id = renewal.chain_id
 AND granted.block_hash = renewal.block_hash
 AND granted.transaction_hash = renewal.transaction_hash
 AND granted.log_index = renewal.log_index
 AND granted.logical_name_id = renewal.logical_name_id
 AND granted.resource_id = renewal.resource_id
 AND granted.event_kind = 'RegistrationGranted'
 AND granted.derivation_kind = renewal.derivation_kind
 AND granted.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.resources old_resource
  ON old_resource.resource_id = renewal.resource_id
 AND old_resource.canonicality_state IN ('canonical', 'safe', 'finalized')
 AND old_resource.provenance->>'authority_kind' = 'registrar'
JOIN public.chain_lineage block
  ON block.chain_id = renewal.chain_id
 AND block.block_hash = renewal.block_hash
CROSS JOIN prior_repair
WHERE renewal.derivation_kind = 'ens_v1_unwrapped_authority'
  AND renewal.chain_id = 'ethereum-mainnet'
  AND renewal.event_kind = 'RegistrationRenewed'
  AND renewal.source_family = 'ens_v1_registrar_l1'
  AND renewal.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND renewal.resource_id IS NOT NULL
  AND renewal.after_state ? 'labelhash'
  AND old_resource.inserted_at >= prior_repair.installed_on
  AND old_resource.provenance->>'authority_key' = concat(
      'registrar:',
      renewal.chain_id,
      ':',
      renewal.source_manifest_id::TEXT,
      ':',
      renewal.after_state->>'labelhash',
      ':',
      renewal.block_hash,
      ':',
      renewal.log_index::TEXT
  );

CREATE INDEX ens_v1_recent_renewal_repair_renewals_name_idx
    ON ens_v1_recent_renewal_repair_renewals (
        logical_name_id,
        labelhash,
        block_number
    );

CREATE TEMP TABLE ens_v1_recent_renewal_repair_names AS
SELECT DISTINCT logical_name_id
FROM ens_v1_recent_renewal_repair_renewals;

CREATE TEMP TABLE ens_v1_recent_renewal_repair_resources AS
SELECT
    resource.resource_id,
    resource.block_number,
    resource.inserted_at,
    resource.provenance->>'logical_name_id' AS logical_name_id,
    lower(resource.provenance->>'labelhash') AS labelhash,
    (resource.provenance->>'expiry')::BIGINT AS expiry,
    resource.provenance->>'authority_key' AS authority_key
FROM public.resources resource
JOIN ens_v1_recent_renewal_repair_names names
  ON names.logical_name_id = resource.provenance->>'logical_name_id'
WHERE resource.chain_id = 'ethereum-mainnet'
  AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND resource.provenance->>'authority_kind' = 'registrar'
  AND resource.provenance ? 'expiry'
  AND resource.provenance->>'expiry' ~ '^-?[0-9]+$'
  AND resource.provenance ? 'authority_key';

CREATE INDEX ens_v1_recent_renewal_repair_resources_lookup_idx
    ON ens_v1_recent_renewal_repair_resources (
        logical_name_id,
        labelhash,
        block_number DESC,
        inserted_at DESC
    );

CREATE TEMP TABLE ens_v1_recent_renewal_repair_map AS
SELECT DISTINCT ON (candidate.old_resource_id)
    candidate.logical_name_id,
    candidate.old_resource_id,
    candidate.expected_resource_id,
    candidate.old_authority_key,
    candidate.expected_authority_key,
    candidate.prior_expiry,
    MIN(candidate.block_number) OVER (
        PARTITION BY candidate.old_resource_id
    ) AS min_block_number
FROM (
    SELECT DISTINCT ON (renewal.normalized_event_id)
        renewal.logical_name_id,
        renewal.old_resource_id,
        prior.resource_id AS expected_resource_id,
        renewal.old_authority_key,
        prior.authority_key AS expected_authority_key,
        prior.expiry AS prior_expiry,
        renewal.block_number,
        prior.block_number AS prior_block_number,
        prior.inserted_at AS prior_inserted_at
    FROM ens_v1_recent_renewal_repair_renewals renewal
    JOIN ens_v1_recent_renewal_repair_resources prior
      ON prior.logical_name_id = renewal.logical_name_id
     AND prior.labelhash = lower(renewal.labelhash)
     AND prior.resource_id <> renewal.old_resource_id
     AND prior.block_number < renewal.block_number
     AND prior.expiry + 7776000 >
         EXTRACT(EPOCH FROM renewal.block_timestamp)::BIGINT
    ORDER BY
        renewal.normalized_event_id,
        prior.block_number DESC,
        prior.inserted_at DESC
) candidate
ORDER BY
    candidate.old_resource_id,
    candidate.prior_block_number DESC,
    candidate.prior_inserted_at DESC;

CREATE INDEX ens_v1_recent_renewal_repair_map_old_idx
    ON ens_v1_recent_renewal_repair_map (old_resource_id);

CREATE TEMP TABLE ens_v1_recent_renewal_repointed_events AS
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
FROM ens_v1_recent_renewal_repair_map repair
JOIN public.normalized_events event
  ON event.resource_id = repair.old_resource_id
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN event.event_kind IN ('RegistrationRenewed', 'ExpiryChanged')
             AND event.before_state ? 'expiry'
            THEN jsonb_set(
                event.before_state,
                '{expiry}',
                to_jsonb(repair.prior_expiry),
                true
            )
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
             AND before_grant_state.repaired_before_state
                 #>> '{revocation_source,authority_key}' =
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
             AND after_grant_state.repaired_after_state
                 #>> '{revocation_source,authority_key}' =
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
  AND event.block_number >= repair.min_block_number
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
      OR COALESCE(event.after_state->>'source_event', '') <>
          'AuthorityEpochChanged'
  );

UPDATE public.normalized_events event
SET
    event_identity = repair.repaired_event_identity,
    resource_id = repair.expected_resource_id,
    before_state = repair.repaired_before_state,
    after_state = repair.repaired_after_state
FROM ens_v1_recent_renewal_repointed_events repair
WHERE event.normalized_event_id = repair.normalized_event_id
  AND event.event_identity = repair.old_event_identity
  AND event.resource_id = repair.old_resource_id
  AND event.before_state = repair.before_state
  AND event.after_state = repair.after_state;

CREATE TEMP TABLE ens_v1_recent_renewal_orphaned_events AS
SELECT
    event.normalized_event_id,
    event.canonicality_state
FROM ens_v1_recent_renewal_repair_map repair
JOIN public.normalized_events event
  ON event.resource_id = repair.old_resource_id
WHERE event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.block_number >= repair.min_block_number
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

UPDATE public.normalized_events event
SET canonicality_state = 'orphaned'
FROM ens_v1_recent_renewal_orphaned_events repair
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
FROM ens_v1_recent_renewal_repointed_events
UNION ALL
SELECT
    normalized_event_id,
    now(),
    'canonicality_update',
    canonicality_state
FROM ens_v1_recent_renewal_orphaned_events;

UPDATE public.surface_bindings binding
SET canonicality_state = 'orphaned'
FROM ens_v1_recent_renewal_repair_map repair
WHERE binding.resource_id = repair.old_resource_id
  AND binding.canonicality_state IN ('canonical', 'safe', 'finalized');

UPDATE public.surface_bindings binding
SET active_to = old_binding.active_to
FROM ens_v1_recent_renewal_repair_map repair
JOIN public.surface_bindings old_binding
  ON old_binding.resource_id = repair.old_resource_id
 AND old_binding.active_to IS NOT NULL
WHERE binding.resource_id = repair.expected_resource_id
  AND binding.logical_name_id = repair.logical_name_id
  AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND (
      binding.active_to IS NULL
      OR binding.active_to < old_binding.active_to
  );

UPDATE public.resources resource
SET canonicality_state = 'orphaned'
FROM ens_v1_recent_renewal_repair_map repair
WHERE resource.resource_id = repair.old_resource_id
  AND resource.canonicality_state IN ('canonical', 'safe', 'finalized');
