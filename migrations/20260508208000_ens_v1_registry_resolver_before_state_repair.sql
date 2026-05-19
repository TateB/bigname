-- Repair ENSv1 registry ResolverChanged rows whose before_state was written
-- before replay could see an earlier canonical registry NewResolver log for
-- the same namehash.
--
-- During normalized replay, block-derived preimages are rebuilt before the
-- ENSv1 authority adapter runs. That can admit older wrapper/registry logs for
-- names that were previously unknown to the authority adapter. Existing later
-- ResolverChanged rows must therefore reflect the previous raw registry
-- resolver, not stale historical state.

CREATE TEMP TABLE ens_v1_registry_resolver_emitters AS
SELECT DISTINCT
    manifest.source_family,
    address.chain_id,
    lower(address.address) AS address,
    COALESCE(address.active_from_block_number, 0) AS active_from_block_number,
    COALESCE(address.active_to_block_number, 9223372036854775807) AS active_to_block_number
FROM public.contract_instance_addresses address
JOIN public.manifest_versions manifest
  ON manifest.manifest_id = address.source_manifest_id
WHERE address.deactivated_at IS NULL
  AND manifest.source_family = 'ens_v1_registry_l1';

CREATE INDEX ens_v1_registry_resolver_emitters_address_idx
    ON ens_v1_registry_resolver_emitters (
        chain_id,
        address,
        active_from_block_number,
        active_to_block_number
    );

CREATE TEMP TABLE ens_v1_registry_resolver_raw_state AS
SELECT
    raw.chain_id,
    emitter.source_family,
    raw.block_hash,
    raw.block_number,
    raw.transaction_hash,
    raw.transaction_index,
    raw.log_index,
    raw.raw_log_id,
    lower(raw.topics[2]) AS namehash,
    lower('0x' || right(encode(raw.data, 'hex'), 40)) AS resolver,
    format(
        'ens_v1_unwrapped_authority:ResolverChanged:resolver:%s:%s:%s',
        raw.block_hash,
        raw.transaction_hash,
        raw.log_index
    ) AS event_identity
FROM public.raw_logs raw
JOIN ens_v1_registry_resolver_emitters emitter
  ON emitter.chain_id = raw.chain_id
 AND emitter.address = lower(raw.emitting_address)
 AND raw.block_number BETWEEN emitter.active_from_block_number
     AND emitter.active_to_block_number
WHERE raw.topics[1] =
      '0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0'
  AND raw.chain_id = 'ethereum-mainnet'
  AND octet_length(raw.data) >= 20
  AND raw.canonicality_state IN ('canonical', 'safe', 'finalized');

CREATE INDEX ens_v1_registry_resolver_raw_state_event_idx
    ON ens_v1_registry_resolver_raw_state (event_identity);

CREATE INDEX ens_v1_registry_resolver_raw_state_prior_idx
    ON ens_v1_registry_resolver_raw_state (
        chain_id,
        source_family,
        namehash,
        block_number DESC,
        transaction_index DESC,
        log_index DESC,
        raw_log_id DESC
    );

CREATE TEMP TABLE ens_v1_registry_resolver_before_state_repair AS
SELECT
    resolver.normalized_event_id,
    resolver.canonicality_state,
    resolver.logical_name_id,
    resolver.chain_id,
    resolver.block_hash,
    resolver.transaction_hash,
    resolver.log_index,
    resolver.before_state,
    jsonb_set(
        resolver.before_state,
        '{resolver}',
        to_jsonb(prior_raw.resolver),
        true
    ) AS repaired_before_state,
    current_raw.resolver AS after_resolver,
    prior_raw.resolver AS prior_resolver
FROM ens_v1_registry_resolver_raw_state current_raw
JOIN public.normalized_events resolver
  ON resolver.event_identity = current_raw.event_identity
JOIN LATERAL (
    SELECT prior.resolver
    FROM ens_v1_registry_resolver_raw_state prior
    WHERE prior.chain_id = current_raw.chain_id
      AND prior.source_family = current_raw.source_family
      AND prior.namehash = current_raw.namehash
      AND (
          prior.block_number < current_raw.block_number
          OR (
              prior.block_number = current_raw.block_number
              AND (
                  prior.transaction_index < current_raw.transaction_index
                  OR (
                      prior.transaction_index = current_raw.transaction_index
                      AND prior.log_index < current_raw.log_index
                  )
              )
          )
      )
    ORDER BY
        prior.block_number DESC,
        prior.transaction_index DESC,
        prior.log_index DESC,
        prior.raw_log_id DESC
    LIMIT 1
) prior_raw ON TRUE
WHERE resolver.derivation_kind = 'ens_v1_unwrapped_authority'
  AND resolver.chain_id = current_raw.chain_id
  AND resolver.source_family = current_raw.source_family
  AND resolver.event_kind = 'ResolverChanged'
  AND resolver.transaction_hash IS NOT NULL
  AND resolver.log_index IS NOT NULL
  AND resolver.after_state ? 'namehash'
  AND resolver.after_state ? 'resolver'
  AND resolver.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND lower(resolver.before_state->>'resolver') IS DISTINCT FROM
      prior_raw.resolver;

CREATE INDEX ens_v1_registry_resolver_before_state_repair_event_idx
    ON ens_v1_registry_resolver_before_state_repair (normalized_event_id);

CREATE INDEX ens_v1_registry_resolver_before_state_repair_boundary_idx
    ON ens_v1_registry_resolver_before_state_repair (
        chain_id,
        block_hash,
        transaction_hash,
        log_index
    );

UPDATE public.normalized_events resolver
SET before_state = repair.repaired_before_state
FROM ens_v1_registry_resolver_before_state_repair repair
WHERE resolver.normalized_event_id = repair.normalized_event_id
  AND resolver.before_state = repair.before_state;

INSERT INTO public.projection_invalidations (
    projection,
    projection_key,
    key_payload,
    first_normalized_event_id,
    last_normalized_event_id,
    last_changed_at,
    invalidated_at
)
SELECT
    'resolver_current' AS projection,
    repair.chain_id || ':' || lower(repair.before_state ->> 'resolver')
        AS projection_key,
    jsonb_build_object(
        'chain_id', repair.chain_id,
        'resolver_address', lower(repair.before_state ->> 'resolver')
    ) AS key_payload,
    MIN(repair.normalized_event_id),
    MAX(repair.normalized_event_id),
    now(),
    now()
FROM ens_v1_registry_resolver_before_state_repair repair
WHERE repair.before_state ? 'resolver'
  AND repair.before_state ->> 'resolver' IS NOT NULL
  AND repair.before_state ->> 'resolver' <> ''
GROUP BY
    repair.chain_id,
    lower(repair.before_state ->> 'resolver')
ON CONFLICT (projection, projection_key)
DO UPDATE SET
    key_payload = EXCLUDED.key_payload,
    generation = projection_invalidations.generation + 1,
    first_normalized_event_id = LEAST(
        projection_invalidations.first_normalized_event_id,
        EXCLUDED.first_normalized_event_id
    ),
    last_normalized_event_id = GREATEST(
        projection_invalidations.last_normalized_event_id,
        EXCLUDED.last_normalized_event_id
    ),
    last_changed_at = GREATEST(
        projection_invalidations.last_changed_at,
        EXCLUDED.last_changed_at
    ),
    invalidated_at = EXCLUDED.invalidated_at,
    claim_token = NULL,
    claimed_at = NULL,
    last_failure_reason = NULL,
    last_failure_at = NULL;

CREATE TEMP TABLE ens_v1_registry_resolver_noop_permissions AS
SELECT DISTINCT
    permission.normalized_event_id,
    permission.canonicality_state
FROM ens_v1_registry_resolver_before_state_repair repair
JOIN public.normalized_events permission
  ON permission.derivation_kind = 'ens_v1_unwrapped_authority'
 AND permission.chain_id = repair.chain_id
 AND permission.logical_name_id = repair.logical_name_id
 AND permission.block_hash = repair.block_hash
 AND permission.transaction_hash IS NOT DISTINCT FROM repair.transaction_hash
 AND permission.log_index IS NOT DISTINCT FROM repair.log_index
 AND permission.event_kind = 'PermissionChanged'
 AND permission.canonicality_state IN ('canonical', 'safe', 'finalized')
WHERE repair.prior_resolver = repair.after_resolver
  AND (
      permission.before_state #>> '{grant_source,source_event_kind}' =
          'ResolverChanged'
      OR permission.before_state #>> '{revocation_source,source_event_kind}' =
          'ResolverChanged'
      OR permission.after_state #>> '{grant_source,source_event_kind}' =
          'ResolverChanged'
      OR permission.after_state #>> '{revocation_source,source_event_kind}' =
          'ResolverChanged'
  );

CREATE INDEX ens_v1_registry_resolver_noop_permissions_event_idx
    ON ens_v1_registry_resolver_noop_permissions (normalized_event_id);

UPDATE public.normalized_events permission
SET canonicality_state = 'orphaned'
FROM ens_v1_registry_resolver_noop_permissions repair
WHERE permission.normalized_event_id = repair.normalized_event_id;

CREATE TEMP TABLE ens_v1_registry_resolver_permission_scope_repair AS
SELECT DISTINCT
    permission.normalized_event_id,
    permission.canonicality_state,
    permission.event_identity AS old_event_identity,
    replace(
        permission.event_identity,
        ':resolver:' || permission_scope.resolver_address || ':',
        ':resolver:' || lower(repair.prior_resolver) || ':'
    ) AS repaired_event_identity,
    permission.before_state,
    jsonb_set(
        permission.before_state,
        '{scope,resolver_address}',
        to_jsonb(lower(repair.prior_resolver)),
        false
    ) AS repaired_before_state,
    permission.after_state,
    jsonb_set(
        permission.after_state,
        '{scope,resolver_address}',
        to_jsonb(lower(repair.prior_resolver)),
        false
    ) AS repaired_after_state
FROM ens_v1_registry_resolver_before_state_repair repair
JOIN public.normalized_events permission
  ON permission.derivation_kind = 'ens_v1_unwrapped_authority'
 AND permission.chain_id = repair.chain_id
 AND permission.logical_name_id = repair.logical_name_id
 AND permission.block_hash = repair.block_hash
 AND permission.transaction_hash IS NOT DISTINCT FROM repair.transaction_hash
 AND permission.log_index IS NOT DISTINCT FROM repair.log_index
 AND permission.event_kind = 'PermissionChanged'
 AND permission.canonicality_state IN ('canonical', 'safe', 'finalized')
CROSS JOIN LATERAL (
    SELECT COALESCE(
        NULLIF(lower(permission.after_state #>> '{scope,resolver_address}'), ''),
        NULLIF(lower(permission.before_state #>> '{scope,resolver_address}'), '')
    ) AS resolver_address
) permission_scope
WHERE repair.prior_resolver <> repair.after_resolver
  AND lower(repair.prior_resolver) <>
      '0x0000000000000000000000000000000000000000'
  AND permission_scope.resolver_address =
      lower(repair.before_state ->> 'resolver')
  AND permission_scope.resolver_address IS DISTINCT FROM
      lower(repair.prior_resolver)
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
  AND NOT EXISTS (
      SELECT 1
      FROM public.normalized_events existing
      WHERE existing.event_identity = replace(
          permission.event_identity,
          ':resolver:' || permission_scope.resolver_address || ':',
          ':resolver:' || lower(repair.prior_resolver) || ':'
      )
        AND existing.normalized_event_id <> permission.normalized_event_id
  );

CREATE INDEX ens_v1_registry_resolver_permission_scope_repair_event_idx
    ON ens_v1_registry_resolver_permission_scope_repair (normalized_event_id);

UPDATE public.normalized_events permission
SET
    event_identity = repair.repaired_event_identity,
    before_state = repair.repaired_before_state,
    after_state = repair.repaired_after_state
FROM ens_v1_registry_resolver_permission_scope_repair repair
WHERE permission.normalized_event_id = repair.normalized_event_id
  AND permission.event_identity = repair.old_event_identity
  AND permission.before_state = repair.before_state
  AND permission.after_state = repair.after_state;

CREATE TEMP TABLE ens_v1_registry_resolver_stale_permissions AS
SELECT DISTINCT
    permission.normalized_event_id,
    permission.canonicality_state
FROM ens_v1_registry_resolver_before_state_repair repair
JOIN public.normalized_events permission
  ON permission.derivation_kind = 'ens_v1_unwrapped_authority'
 AND permission.chain_id = repair.chain_id
 AND permission.logical_name_id = repair.logical_name_id
 AND permission.block_hash = repair.block_hash
 AND permission.transaction_hash IS NOT DISTINCT FROM repair.transaction_hash
 AND permission.log_index IS NOT DISTINCT FROM repair.log_index
 AND permission.event_kind = 'PermissionChanged'
 AND permission.canonicality_state IN ('canonical', 'safe', 'finalized')
CROSS JOIN LATERAL (
    SELECT COALESCE(
        NULLIF(lower(permission.after_state #>> '{scope,resolver_address}'), ''),
        NULLIF(lower(permission.before_state #>> '{scope,resolver_address}'), '')
    ) AS resolver_address
) permission_scope
WHERE repair.prior_resolver <> repair.after_resolver
  AND permission_scope.resolver_address IS NOT NULL
  AND permission_scope.resolver_address <> lower(repair.prior_resolver)
  AND permission_scope.resolver_address <> lower(repair.after_resolver)
  AND (
      permission.before_state #>> '{grant_source,source_event_kind}' =
          'ResolverChanged'
      OR permission.before_state #>> '{revocation_source,source_event_kind}' =
          'ResolverChanged'
      OR permission.after_state #>> '{grant_source,source_event_kind}' =
          'ResolverChanged'
      OR permission.after_state #>> '{revocation_source,source_event_kind}' =
          'ResolverChanged'
  );

CREATE INDEX ens_v1_registry_resolver_stale_permissions_event_idx
    ON ens_v1_registry_resolver_stale_permissions (normalized_event_id);

UPDATE public.normalized_events permission
SET canonicality_state = 'orphaned'
FROM ens_v1_registry_resolver_stale_permissions repair
WHERE permission.normalized_event_id = repair.normalized_event_id;

INSERT INTO public.projection_normalized_event_changes (
    normalized_event_id,
    changed_at,
    change_kind,
    canonicality_state
)
SELECT DISTINCT
    normalized_event_id,
    now(),
    'canonicality_update',
    canonicality_state
FROM ens_v1_registry_resolver_before_state_repair
UNION ALL
SELECT DISTINCT
    normalized_event_id,
    now(),
    'canonicality_update',
    canonicality_state
FROM ens_v1_registry_resolver_noop_permissions
UNION ALL
SELECT DISTINCT
    normalized_event_id,
    now(),
    'canonicality_update',
    canonicality_state
FROM ens_v1_registry_resolver_permission_scope_repair
UNION ALL
SELECT DISTINCT
    normalized_event_id,
    now(),
    'canonicality_update',
    canonicality_state
FROM ens_v1_registry_resolver_stale_permissions;
