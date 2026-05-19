-- Repair ENSv1 wrapper wrap rows that were replayed with a registry-only
-- before-state even though the immediately preceding active authority is a
-- live registrar lease.
--
-- The current adapter keeps the registrar authority active until NameWrapped.
-- Historical same-transaction rows from the older registry-only interpretation
-- must be orphaned so projection replay sees the same authority transition
-- that normalized-event replay emits today.

CREATE TEMP TABLE ens_v1_wrapper_active_registrar_tokens AS
SELECT
    token.normalized_event_id,
    token.logical_name_id,
    token.chain_id,
    token.block_number,
    token.block_hash,
    token.transaction_hash,
    token.log_index,
    token.resource_id AS wrapper_resource_id,
    token.before_state,
    token.after_state,
    token.canonicality_state,
    block.block_timestamp
FROM public.normalized_events token
JOIN public.chain_lineage block
  ON block.chain_id = token.chain_id
 AND block.block_hash = token.block_hash
WHERE token.derivation_kind = 'ens_v1_unwrapped_authority'
  AND token.chain_id = 'ethereum-mainnet'
  AND token.event_kind = 'TokenControlTransferred'
  AND token.source_family = 'ens_v1_wrapper_l1'
  AND token.before_state->>'authority_kind' = 'registry_only'
  AND token.after_state->>'authority_kind' = 'wrapper'
  AND token.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND token.observed_at >= TIMESTAMPTZ '2026-05-14 00:00:00+00';

CREATE INDEX ens_v1_wrapper_active_registrar_tokens_lookup_idx
    ON ens_v1_wrapper_active_registrar_tokens (
        logical_name_id,
        block_number,
        block_hash,
        transaction_hash,
        log_index
    );

CREATE TEMP TABLE ens_v1_wrapper_active_registrar_repair_map AS
SELECT DISTINCT ON (token.normalized_event_id)
    token.normalized_event_id AS token_event_id,
    token.canonicality_state AS token_canonicality_state,
    token.logical_name_id,
    token.chain_id,
    token.block_number,
    token.block_hash,
    token.transaction_hash,
    token.log_index AS token_log_index,
    token.wrapper_resource_id,
    token.before_state AS token_before_state,
    jsonb_set(
        token.before_state,
        '{authority_kind}',
        to_jsonb('registrar'::TEXT),
        true
    ) AS repaired_token_before_state,
    previous_binding.resource_id AS registrar_resource_id
FROM ens_v1_wrapper_active_registrar_tokens token
JOIN public.surface_bindings previous_binding
  ON previous_binding.logical_name_id = token.logical_name_id
 AND previous_binding.active_to = token.block_timestamp
 AND previous_binding.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.resources previous_resource
  ON previous_resource.resource_id = previous_binding.resource_id
 AND previous_resource.provenance->>'authority_kind' = 'registrar'
 AND previous_resource.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.resources wrapper_resource
  ON wrapper_resource.resource_id = token.wrapper_resource_id
 AND wrapper_resource.provenance->>'authority_kind' = 'wrapper'
 AND wrapper_resource.canonicality_state IN ('canonical', 'safe', 'finalized')
ORDER BY
    token.normalized_event_id,
    previous_binding.active_from DESC,
    previous_binding.surface_binding_id;

CREATE INDEX ens_v1_wrapper_active_registrar_repair_lookup_idx
    ON ens_v1_wrapper_active_registrar_repair_map (
        logical_name_id,
        block_number,
        block_hash,
        transaction_hash,
        token_log_index
    );

UPDATE public.normalized_events token
SET before_state = repair.repaired_token_before_state
FROM ens_v1_wrapper_active_registrar_repair_map repair
WHERE token.normalized_event_id = repair.token_event_id
  AND token.before_state = repair.token_before_state;

CREATE TEMP TABLE ens_v1_wrapper_active_registrar_orphaned_events AS
SELECT DISTINCT
    event.normalized_event_id,
    event.canonicality_state
FROM ens_v1_wrapper_active_registrar_repair_map repair
JOIN public.normalized_events event
  ON event.logical_name_id = repair.logical_name_id
 AND event.block_number = repair.block_number
WHERE event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND event.normalized_event_id <> repair.token_event_id
  AND (
      (
          event.event_kind IN (
              'SurfaceBound',
              'SurfaceUnbound',
              'AuthorityEpochChanged'
          )
          AND (
              event.before_state->>'authority_kind' = 'registry_only'
              OR event.after_state->>'authority_kind' = 'registry_only'
          )
      )
      OR (
          event.event_kind = 'AuthorityTransferred'
          AND event.transaction_hash IS NOT DISTINCT FROM repair.transaction_hash
          AND event.log_index < repair.token_log_index
      )
      OR (
          event.event_kind = 'PermissionChanged'
          AND event.transaction_hash IS NOT DISTINCT FROM repair.transaction_hash
          AND event.log_index < repair.token_log_index
          AND (
              event.before_state #>> '{grant_source,source_event_kind}' =
                  'AuthorityTransferred'
              OR event.after_state #>> '{grant_source,source_event_kind}' =
                  'AuthorityTransferred'
              OR event.before_state #>> '{revocation_source,source_event_kind}' =
                  'AuthorityTransferred'
              OR event.after_state #>> '{revocation_source,source_event_kind}' =
                  'AuthorityTransferred'
          )
      )
  );

UPDATE public.normalized_events event
SET canonicality_state = 'orphaned'
FROM ens_v1_wrapper_active_registrar_orphaned_events repair
WHERE event.normalized_event_id = repair.normalized_event_id;

INSERT INTO public.projection_normalized_event_changes (
    normalized_event_id,
    changed_at,
    change_kind,
    canonicality_state
)
SELECT DISTINCT
    token_event_id,
    now(),
    'canonicality_update',
    token_canonicality_state
FROM ens_v1_wrapper_active_registrar_repair_map
UNION ALL
SELECT DISTINCT
    normalized_event_id,
    now(),
    'canonicality_update',
    canonicality_state
FROM ens_v1_wrapper_active_registrar_orphaned_events;
