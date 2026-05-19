-- Repair ENSv1 registrar renewal rows whose prior renewal raw log arrived
-- later than the already-normalized renewal row.
--
-- Historical sparse backfill can fill a gap with an earlier NameRenewed log.
-- When replay processes that gap and a later renewal in the same chunk, the
-- later row's before_state must reflect the newly visible prior raw renewal
-- expiry. Existing normalized history is not enough because the prior raw log
-- may not have a normalized row yet.

CREATE TEMP TABLE ens_v1_registrar_renewal_raw_prior_repair AS
WITH renewal_targets AS (
    SELECT
        renewal.normalized_event_id,
        renewal.chain_id,
        renewal.block_hash,
        renewal.transaction_hash,
        renewal.log_index,
        renewal.logical_name_id,
        renewal.resource_id,
        raw_prior.prior_expiry
    FROM public.normalized_events renewal
    CROSS JOIN LATERAL (
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
        WHERE raw_log.chain_id = renewal.chain_id
          AND raw_log.topics[1] =
              '0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae'
          AND lower(raw_log.topics[2]) = lower(renewal.after_state->>'labelhash')
          AND raw_log.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND octet_length(raw_log.data) >= 96
          AND raw_log.block_number >= 16925618
          AND (
              raw_log.block_number < renewal.block_number
              OR (
                  raw_log.block_number = renewal.block_number
                  AND raw_log.log_index < COALESCE(renewal.log_index, -1)
              )
          )
        ORDER BY
            raw_log.block_number DESC,
            raw_log.transaction_index DESC,
            raw_log.log_index DESC
        LIMIT 1
    ) raw_prior
    WHERE renewal.derivation_kind = 'ens_v1_unwrapped_authority'
      AND renewal.chain_id = 'ethereum-mainnet'
      AND renewal.source_family = 'ens_v1_registrar_l1'
      AND renewal.event_kind = 'RegistrationRenewed'
      AND renewal.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND renewal.block_number >= 16925618
      AND renewal.after_state ? 'labelhash'
      AND renewal.before_state ? 'expiry'
      AND renewal.before_state->>'expiry' IS NOT NULL
      AND renewal.before_state->>'expiry' ~ '^-?[0-9]+$'
      AND (renewal.before_state->>'expiry')::BIGINT <> raw_prior.prior_expiry
)
SELECT
    renewal.normalized_event_id,
    renewal.canonicality_state,
    renewal.before_state,
    jsonb_set(
        renewal.before_state,
        '{expiry}',
        to_jsonb(target.prior_expiry),
        true
    ) AS repaired_before_state
FROM renewal_targets target
JOIN public.normalized_events renewal
  ON renewal.normalized_event_id = target.normalized_event_id

UNION ALL

SELECT
    expiry.normalized_event_id,
    expiry.canonicality_state,
    expiry.before_state,
    jsonb_set(
        expiry.before_state,
        '{expiry}',
        to_jsonb(target.prior_expiry),
        true
    ) AS repaired_before_state
FROM renewal_targets target
JOIN public.normalized_events expiry
  ON expiry.chain_id = target.chain_id
 AND expiry.block_hash = target.block_hash
 AND expiry.transaction_hash = target.transaction_hash
 AND expiry.log_index = target.log_index
 AND expiry.logical_name_id = target.logical_name_id
 AND expiry.resource_id = target.resource_id
 AND expiry.derivation_kind = 'ens_v1_unwrapped_authority'
 AND expiry.event_kind = 'ExpiryChanged'
 AND expiry.canonicality_state IN ('canonical', 'safe', 'finalized')
WHERE expiry.before_state ? 'expiry'
  AND expiry.before_state->>'expiry' IS NOT NULL
  AND expiry.before_state->>'expiry' ~ '^-?[0-9]+$'
  AND (expiry.before_state->>'expiry')::BIGINT <> target.prior_expiry;

UPDATE public.normalized_events event
SET before_state = repair.repaired_before_state
FROM ens_v1_registrar_renewal_raw_prior_repair repair
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
FROM ens_v1_registrar_renewal_raw_prior_repair;
