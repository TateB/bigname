-- Repair ENSv1 wrapper wrap boundaries that were historically preceded by a
-- synthetic registry-only authority epoch even though the same transaction had
-- already moved the registrar owner to the registry owner.
--
-- Current replay keeps the registrar authority active until the wrapper
-- boundary in this converged-owner case. The stale registry-only epoch rows
-- make the NameWrapped TokenControlTransferred before_state disagree with
-- replay and leave extra registry-only grant rows in the projection stream.

CREATE TEMP TABLE ens_v1_wrapper_converged_registrar_tokens AS
SELECT
    token.normalized_event_id,
    token.logical_name_id,
    token.chain_id,
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
  AND token.canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX ens_v1_wrapper_converged_registrar_tokens_idx
    ON ens_v1_wrapper_converged_registrar_tokens (
        block_hash,
        transaction_hash,
        logical_name_id,
        log_index
    );

CREATE TEMP TABLE ens_v1_wrapper_converged_registrar_repair_map AS
SELECT DISTINCT ON (token.normalized_event_id)
    token.normalized_event_id AS token_event_id,
    token.canonicality_state AS token_canonicality_state,
    token.logical_name_id,
    token.chain_id,
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
    registry_grant.normalized_event_id AS registry_grant_event_id,
    registry_grant.canonicality_state AS registry_grant_canonicality_state,
    registry_grant.resource_id AS registry_resource_id,
    registry_resource.provenance->>'authority_key' AS registry_authority_key,
    registrar_binding.resource_id AS expected_registrar_resource_id,
    registrar_resource.provenance->>'authority_key' AS expected_registrar_authority_key
FROM ens_v1_wrapper_converged_registrar_tokens token
JOIN public.normalized_events registry_grant
  ON registry_grant.logical_name_id = token.logical_name_id
 AND registry_grant.block_hash = token.block_hash
 AND registry_grant.transaction_hash IS NOT DISTINCT FROM token.transaction_hash
 AND registry_grant.log_index < token.log_index
 AND registry_grant.event_kind = 'PermissionChanged'
 AND registry_grant.after_state #>> '{grant_source,authority_kind}' =
     'registry_only'
 AND registry_grant.after_state #>> '{scope,kind}' = 'resource'
 AND registry_grant.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.resources registry_resource
  ON registry_resource.resource_id = registry_grant.resource_id
 AND registry_resource.provenance->>'authority_kind' = 'registry_only'
JOIN public.surface_bindings registrar_binding
  ON registrar_binding.logical_name_id = token.logical_name_id
 AND registrar_binding.active_to = token.block_timestamp
 AND registrar_binding.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.resources registrar_resource
  ON registrar_resource.resource_id = registrar_binding.resource_id
 AND registrar_resource.provenance->>'authority_kind' = 'registrar'
 AND registrar_resource.canonicality_state IN ('canonical', 'safe', 'finalized')
JOIN public.raw_logs registrar_transfer
  ON registrar_transfer.chain_id = token.chain_id
 AND registrar_transfer.block_hash = token.block_hash
 AND registrar_transfer.transaction_hash = token.transaction_hash
 AND registrar_transfer.log_index < registry_grant.log_index
 AND registrar_transfer.topics[1] =
     '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
 AND lower('0x' || right(registrar_transfer.topics[3], 40)) =
     lower(registry_grant.after_state->>'subject')
 AND lower(registrar_transfer.topics[4]) =
     lower(registrar_resource.provenance->>'labelhash')
 AND registrar_transfer.canonicality_state IN ('canonical', 'safe', 'finalized')
ORDER BY
    token.normalized_event_id,
    registry_grant.log_index DESC,
    registrar_transfer.log_index DESC;

CREATE INDEX ens_v1_wrapper_converged_registrar_repair_event_idx
    ON ens_v1_wrapper_converged_registrar_repair_map (token_event_id);

CREATE INDEX ens_v1_wrapper_converged_registrar_repair_boundary_idx
    ON ens_v1_wrapper_converged_registrar_repair_map (
        block_hash,
        logical_name_id,
        wrapper_resource_id,
        registry_resource_id
    );

UPDATE public.normalized_events token
SET before_state = repair.repaired_token_before_state
FROM ens_v1_wrapper_converged_registrar_repair_map repair
WHERE token.normalized_event_id = repair.token_event_id
  AND token.before_state = repair.token_before_state;

CREATE TEMP TABLE ens_v1_wrapper_converged_registrar_orphaned_events AS
SELECT DISTINCT
    event.normalized_event_id,
    event.canonicality_state
FROM ens_v1_wrapper_converged_registrar_repair_map repair
JOIN public.normalized_events event
  ON event.logical_name_id = repair.logical_name_id
 AND event.block_hash = repair.block_hash
WHERE event.derivation_kind = 'ens_v1_unwrapped_authority'
  AND event.chain_id = 'ethereum-mainnet'
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND (
      event.normalized_event_id = repair.registry_grant_event_id
      OR (
          event.resource_id = repair.registry_resource_id
          AND event.event_kind IN (
              'SurfaceBound',
              'AuthorityEpochChanged',
              'AuthorityTransferred'
          )
      )
      OR (
          event.resource_id = repair.wrapper_resource_id
          AND event.event_kind = 'AuthorityEpochChanged'
          AND event.before_state->>'authority_kind' = 'registry_only'
          AND event.after_state->>'authority_kind' = 'wrapper'
      )
  );

UPDATE public.normalized_events event
SET canonicality_state = 'orphaned'
FROM ens_v1_wrapper_converged_registrar_orphaned_events repair
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
FROM ens_v1_wrapper_converged_registrar_repair_map
UNION ALL
SELECT DISTINCT
    normalized_event_id,
    now(),
    'canonicality_update',
    canonicality_state
FROM ens_v1_wrapper_converged_registrar_orphaned_events;
