-- Repair ENSv1 registrar renewal rows written after the earlier renewal
-- before-state repair ran.
--
-- Current replay preloads the active registrar lease before a renewal, so the
-- renewal and paired expiry event must record the prior expiry in before_state.
-- Older live sync rows can have before_state.expiry equal to the renewed
-- expiry; strict normalized-event replay correctly rejects those stale rows.

CREATE TEMP TABLE ens_v1_recent_renewal_before_state_candidates AS
SELECT
    event.normalized_event_id,
    event.resource_id,
    event.chain_id,
    event.block_number,
    event.log_index,
    event.before_state,
    event.after_state,
    event.canonicality_state
FROM public.normalized_events event
WHERE event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.source_family = 'ens_v1_registrar_l1'
  AND event.event_kind IN ('RegistrationRenewed', 'ExpiryChanged')
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND event.observed_at >= TIMESTAMPTZ '2026-05-14 00:00:00+00'
  AND event.before_state ? 'expiry'
  AND event.after_state ? 'expiry'
  AND event.before_state->>'expiry' IS NOT NULL
  AND event.after_state->>'expiry' IS NOT NULL
  AND event.before_state->>'expiry' ~ '^-?[0-9]+$'
  AND event.after_state->>'expiry' ~ '^-?[0-9]+$'
  AND event.before_state->>'expiry' = event.after_state->>'expiry';

CREATE INDEX ens_v1_recent_renewal_before_state_candidates_idx
    ON ens_v1_recent_renewal_before_state_candidates (
        resource_id,
        chain_id,
        block_number,
        log_index
    );

CREATE TEMP TABLE ens_v1_recent_renewal_before_state_repair AS
SELECT DISTINCT ON (event.normalized_event_id)
    event.normalized_event_id,
    event.canonicality_state,
    event.before_state,
    jsonb_set(
        event.before_state,
        '{expiry}',
        to_jsonb(prior.prior_expiry),
        true
    ) AS repaired_before_state
FROM ens_v1_recent_renewal_before_state_candidates event
CROSS JOIN LATERAL (
    SELECT (prior.after_state->>'expiry')::BIGINT AS prior_expiry
    FROM public.normalized_events prior
    WHERE prior.resource_id = event.resource_id
      AND prior.chain_id = event.chain_id
      AND prior.derivation_kind = 'ens_v1_unwrapped_authority'
      AND prior.event_kind IN (
          'RegistrationGranted',
          'RegistrationRenewed',
          'ExpiryChanged'
      )
      AND prior.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND prior.after_state ? 'expiry'
      AND prior.after_state->>'expiry' IS NOT NULL
      AND prior.after_state->>'expiry' ~ '^-?[0-9]+$'
      AND (
          prior.block_number < event.block_number
          OR (
              prior.block_number = event.block_number
              AND COALESCE(prior.log_index, -1) < COALESCE(event.log_index, -1)
          )
      )
    ORDER BY
        prior.block_number DESC NULLS LAST,
        prior.log_index DESC NULLS LAST,
        prior.normalized_event_id DESC
    LIMIT 1
) prior
WHERE (event.before_state->>'expiry')::BIGINT <> prior.prior_expiry
ORDER BY event.normalized_event_id;

UPDATE public.normalized_events event
SET before_state = repair.repaired_before_state
FROM ens_v1_recent_renewal_before_state_repair repair
WHERE event.normalized_event_id = repair.normalized_event_id
  AND event.before_state = repair.before_state;

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
FROM ens_v1_recent_renewal_before_state_repair;
