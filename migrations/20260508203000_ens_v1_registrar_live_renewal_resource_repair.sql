-- Repair ENSv1 registrar renewal rows whose expected prior registrar resource
-- was materialized after the first renewal-resource repair ran.
--
-- These rows have a canonical same-log RegistrationGranted scaffold on the
-- renewal log, but a now-known prior registrar resource was still live at the
-- renewal timestamp. Replay keeps the renewal on that prior resource and does
-- not keep the synthetic grant/boundary resource.

CREATE TEMP TABLE ens_v1_registrar_live_renewal_resource_repair_map AS
WITH renewal_candidates AS (
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
    WHERE renewal.derivation_kind = 'ens_v1_unwrapped_authority'
      AND renewal.chain_id = 'ethereum-mainnet'
      AND renewal.event_kind = 'RegistrationRenewed'
      AND renewal.source_family = 'ens_v1_registrar_l1'
      AND renewal.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND renewal.resource_id IS NOT NULL
      AND renewal.after_state ? 'labelhash'
),
prior_candidates AS (
    SELECT DISTINCT ON (renewal.normalized_event_id)
        renewal.logical_name_id,
        renewal.old_resource_id,
        prior.resource_id AS expected_resource_id,
        renewal.old_authority_key,
        prior.provenance->>'authority_key' AS expected_authority_key,
        COALESCE(
            event_prior.prior_expiry,
            raw_prior.prior_expiry,
            CASE
                WHEN prior.provenance->>'expiry' ~ '^-?[0-9]+$'
                THEN (prior.provenance->>'expiry')::BIGINT
                ELSE NULL
            END
        ) AS prior_expiry,
        renewal.block_number,
        renewal.block_timestamp
    FROM renewal_candidates renewal
    JOIN public.resources prior
      ON prior.chain_id = 'ethereum-mainnet'
     AND prior.resource_id <> renewal.old_resource_id
     AND prior.canonicality_state IN ('canonical', 'safe', 'finalized')
     AND prior.provenance->>'authority_kind' = 'registrar'
     AND prior.provenance->>'logical_name_id' = renewal.logical_name_id
     AND lower(prior.provenance->>'labelhash') = lower(renewal.labelhash)
     AND prior.block_number < renewal.block_number
    LEFT JOIN LATERAL (
        SELECT (event.after_state->>'expiry')::BIGINT AS prior_expiry
        FROM public.normalized_events event
        WHERE event.resource_id = prior.resource_id
          AND event.derivation_kind = 'ens_v1_unwrapped_authority'
          AND event.chain_id = 'ethereum-mainnet'
          AND event.event_kind IN (
              'RegistrationGranted',
              'RegistrationRenewed',
              'ExpiryChanged'
          )
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND event.after_state ? 'expiry'
          AND event.after_state->>'expiry' ~ '^-?[0-9]+$'
          AND (
              event.block_number < renewal.block_number
              OR (
                  event.block_number = renewal.block_number
                  AND COALESCE(event.log_index, -1) < COALESCE(renewal.log_index, -1)
              )
          )
        ORDER BY
            event.block_number DESC NULLS LAST,
            event.log_index DESC NULLS LAST,
            event.normalized_event_id DESC
        LIMIT 1
    ) event_prior ON TRUE
    LEFT JOIN LATERAL (
        SELECT CASE
            WHEN split_part(prior.provenance->>'authority_key', ':', 6) ~ '^[0-9]+$'
            THEN split_part(prior.provenance->>'authority_key', ':', 6)::BIGINT
            ELSE NULL
        END AS authority_log_index
    ) authority_key ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            (
                (get_byte(raw_log.data, 88)::BIGINT << 56)
                + (get_byte(raw_log.data, 89)::BIGINT << 48)
                + (get_byte(raw_log.data, 90)::BIGINT << 40)
                + (get_byte(raw_log.data, 91)::BIGINT << 32)
                + (get_byte(raw_log.data, 92)::BIGINT << 24)
                + (get_byte(raw_log.data, 93)::BIGINT << 16)
                + (get_byte(raw_log.data, 94)::BIGINT << 8)
                + get_byte(raw_log.data, 95)::BIGINT
            ) AS prior_expiry
        FROM public.raw_logs raw_log
        WHERE raw_log.chain_id = prior.chain_id
          AND raw_log.block_hash = prior.block_hash
          AND raw_log.log_index = authority_key.authority_log_index
          AND raw_log.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND octet_length(raw_log.data) >= 96
        LIMIT 1
    ) raw_prior ON TRUE
    WHERE prior.provenance ? 'authority_key'
      AND authority_key.authority_log_index IS NOT NULL
    ORDER BY renewal.normalized_event_id, prior.block_number DESC
)
SELECT
    logical_name_id,
    old_resource_id,
    expected_resource_id,
    old_authority_key,
    expected_authority_key,
    prior_expiry,
    MIN(block_number) AS min_block_number
FROM prior_candidates
WHERE prior_expiry IS NOT NULL
  AND prior_expiry + 7776000 > EXTRACT(EPOCH FROM block_timestamp)::BIGINT
GROUP BY
    logical_name_id,
    old_resource_id,
    expected_resource_id,
    old_authority_key,
    expected_authority_key,
    prior_expiry;

CREATE INDEX ens_v1_registrar_live_renewal_resource_repair_old_idx
    ON ens_v1_registrar_live_renewal_resource_repair_map (old_resource_id);

CREATE TEMP TABLE ens_v1_registrar_live_renewal_resource_repointed_events AS
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
FROM ens_v1_registrar_live_renewal_resource_repair_map repair
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
      OR COALESCE(event.after_state->>'source_event', '') <> 'AuthorityEpochChanged'
  );

UPDATE public.normalized_events event
SET
    event_identity = repair.repaired_event_identity,
    resource_id = repair.expected_resource_id,
    before_state = repair.repaired_before_state,
    after_state = repair.repaired_after_state
FROM ens_v1_registrar_live_renewal_resource_repointed_events repair
WHERE event.normalized_event_id = repair.normalized_event_id
  AND event.event_identity = repair.old_event_identity
  AND event.resource_id = repair.old_resource_id
  AND event.before_state = repair.before_state
  AND event.after_state = repair.after_state;

CREATE TEMP TABLE ens_v1_registrar_live_renewal_resource_orphaned_events AS
SELECT
    event.normalized_event_id,
    event.canonicality_state
FROM ens_v1_registrar_live_renewal_resource_repair_map repair
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
FROM ens_v1_registrar_live_renewal_resource_orphaned_events repair
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
FROM ens_v1_registrar_live_renewal_resource_repointed_events
UNION ALL
SELECT
    normalized_event_id,
    now(),
    'canonicality_update',
    canonicality_state
FROM ens_v1_registrar_live_renewal_resource_orphaned_events;

UPDATE public.surface_bindings binding
SET canonicality_state = 'orphaned'
FROM ens_v1_registrar_live_renewal_resource_repair_map repair
WHERE binding.resource_id = repair.old_resource_id
  AND binding.canonicality_state IN ('canonical', 'safe', 'finalized');

UPDATE public.surface_bindings binding
SET active_to = old_binding.active_to
FROM ens_v1_registrar_live_renewal_resource_repair_map repair
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
FROM ens_v1_registrar_live_renewal_resource_repair_map repair
WHERE resource.resource_id = repair.old_resource_id
  AND resource.canonicality_state IN ('canonical', 'safe', 'finalized');
