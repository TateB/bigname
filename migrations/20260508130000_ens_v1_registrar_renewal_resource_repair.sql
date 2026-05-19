-- Repair ENSv1 registrar renewal rows written by selected/block-hash replay
-- before registrar leases were preloaded from prior resource provenance.
--
-- The old replay path could see a renewal without its still-active registrar
-- lease, synthesize a same-log RegistrationGranted resource, and then attach
-- RegistrationRenewed/ExpiryChanged rows to that synthetic resource. The
-- corrected replay keeps those renewal rows on the prior unexpired registrar
-- resource and does not emit the synthetic grant or authority-transition rows.

CREATE TEMP TABLE ens_v1_registrar_renewal_repair_renewals AS
SELECT
    renewal.normalized_event_id,
    renewal.logical_name_id,
    renewal.resource_id AS old_resource_id,
    renewal.block_number,
    renewal.block_hash,
    renewal.transaction_hash,
    renewal.log_index,
    block.block_timestamp,
    renewal.after_state->>'labelhash' AS labelhash
FROM public.normalized_events renewal
JOIN public.normalized_events granted
  ON granted.chain_id = renewal.chain_id
 AND granted.block_hash = renewal.block_hash
 AND granted.transaction_hash = renewal.transaction_hash
 AND granted.log_index = renewal.log_index
 AND granted.logical_name_id = renewal.logical_name_id
 AND granted.event_kind = 'RegistrationGranted'
 AND granted.derivation_kind = renewal.derivation_kind
 AND granted.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.chain_lineage block
  ON block.chain_id = renewal.chain_id
 AND block.block_hash = renewal.block_hash
WHERE renewal.derivation_kind = 'ens_v1_unwrapped_authority'
  AND renewal.chain_id = 'ethereum-mainnet'
  AND renewal.event_kind = 'RegistrationRenewed'
  AND renewal.source_family = 'ens_v1_registrar_l1'
  AND renewal.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND renewal.resource_id IS NOT NULL
  AND renewal.after_state ? 'labelhash';

CREATE INDEX ens_v1_registrar_renewal_repair_renewals_lookup_idx
    ON ens_v1_registrar_renewal_repair_renewals (
        logical_name_id,
        labelhash,
        block_number
    );

CREATE TEMP TABLE ens_v1_registrar_renewal_repair_names AS
SELECT DISTINCT logical_name_id
FROM ens_v1_registrar_renewal_repair_renewals;

CREATE TEMP TABLE ens_v1_registrar_renewal_repair_resources AS
SELECT
    resource.resource_id,
    resource.block_number,
    resource.provenance->>'logical_name_id' AS logical_name_id,
    resource.provenance->>'labelhash' AS labelhash,
    (resource.provenance->>'expiry')::BIGINT AS expiry
FROM public.resources resource
JOIN ens_v1_registrar_renewal_repair_names names
  ON names.logical_name_id = resource.provenance->>'logical_name_id'
WHERE resource.chain_id = 'ethereum-mainnet'
  AND resource.provenance->>'authority_kind' = 'registrar'
  AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND resource.provenance ? 'expiry';

CREATE INDEX ens_v1_registrar_renewal_repair_resources_lookup_idx
    ON ens_v1_registrar_renewal_repair_resources (
        logical_name_id,
        labelhash,
        block_number DESC
    );

CREATE TEMP TABLE ens_v1_registrar_renewal_repair_map AS
SELECT DISTINCT ON (candidate.old_resource_id)
    candidate.logical_name_id,
    candidate.old_resource_id,
    candidate.expected_resource_id,
    MIN(candidate.block_number) OVER (
        PARTITION BY candidate.old_resource_id
    ) AS min_block_number
FROM (
    SELECT DISTINCT ON (renewal.normalized_event_id)
        renewal.logical_name_id,
        renewal.old_resource_id,
        renewal.block_number,
        prior.resource_id AS expected_resource_id,
        prior.block_number AS prior_block_number
    FROM ens_v1_registrar_renewal_repair_renewals renewal
    JOIN ens_v1_registrar_renewal_repair_resources prior
      ON prior.logical_name_id = renewal.logical_name_id
     AND prior.labelhash = renewal.labelhash
     AND prior.resource_id <> renewal.old_resource_id
     AND prior.block_number < renewal.block_number
     AND prior.expiry + 7776000 > EXTRACT(EPOCH FROM renewal.block_timestamp)::BIGINT
    ORDER BY renewal.normalized_event_id, prior.block_number DESC
) candidate
ORDER BY candidate.old_resource_id, candidate.prior_block_number DESC;

CREATE INDEX ens_v1_registrar_renewal_repair_map_old_resource_idx
    ON ens_v1_registrar_renewal_repair_map (old_resource_id);

CREATE TEMP TABLE ens_v1_registrar_renewal_repair_repointed_events AS
SELECT
    event.normalized_event_id,
    event.canonicality_state
FROM public.normalized_events event
JOIN ens_v1_registrar_renewal_repair_map repair
  ON repair.old_resource_id = event.resource_id
WHERE event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.block_number >= repair.min_block_number
  AND event.event_kind IN ('RegistrationRenewed', 'ExpiryChanged')
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized');

UPDATE public.normalized_events event
SET resource_id = repair.expected_resource_id
FROM ens_v1_registrar_renewal_repair_map repair
WHERE repair.old_resource_id = event.resource_id
  AND event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.block_number >= repair.min_block_number
  AND event.event_kind IN ('RegistrationRenewed', 'ExpiryChanged')
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized');

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
FROM ens_v1_registrar_renewal_repair_repointed_events;

UPDATE public.normalized_events event
SET canonicality_state = 'orphaned'
FROM ens_v1_registrar_renewal_repair_map repair
WHERE repair.old_resource_id = event.resource_id
  AND event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.block_number >= repair.min_block_number
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND (
      event.event_kind IN (
          'RegistrationGranted',
          'SurfaceBound',
          'AuthorityEpochChanged'
      )
      OR (
          event.event_kind = 'ResolverChanged'
          AND event.after_state->>'source_event' = 'AuthorityEpochChanged'
      )
  );

UPDATE public.surface_bindings binding
SET canonicality_state = 'orphaned'
FROM ens_v1_registrar_renewal_repair_map repair
WHERE repair.old_resource_id = binding.resource_id
  AND binding.canonicality_state IN ('canonical', 'safe', 'finalized');

UPDATE public.resources resource
SET canonicality_state = 'orphaned'
FROM ens_v1_registrar_renewal_repair_map repair
WHERE repair.old_resource_id = resource.resource_id
  AND resource.canonicality_state IN ('canonical', 'safe', 'finalized');
