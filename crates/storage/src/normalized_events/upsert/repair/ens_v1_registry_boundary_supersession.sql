WITH input AS (
    SELECT *
    FROM unnest(
        $1::TEXT[],
        $2::UUID[],
        $3::TEXT[],
        $4::BIGINT[],
        $5::TEXT[],
        $6::TEXT[],
        $7::TEXT[],
        $8::TEXT[],
        $9::TEXT[],
        $10::BIGINT[],
        $11::BIGINT[]
    ) AS input(
        event_identity,
        resource_id,
        logical_name_id,
        block_number,
        block_hash,
        event_kind,
        raw_fact_ref,
        before_state,
        after_state,
        manifest_version,
        source_manifest_id
    )
),
current_event AS (
    SELECT
        input.*,
        event.manifest_version AS current_manifest_version,
        event.source_manifest_id AS current_source_manifest_id
    FROM input
    JOIN normalized_events event
      ON event.event_identity = input.event_identity
     AND event.namespace = 'basenames'
     AND event.logical_name_id = input.logical_name_id
     AND event.resource_id = input.resource_id
     AND event.event_kind = input.event_kind
     AND event.source_family = 'basenames_base_registry'
     AND event.chain_id = 'base-mainnet'
     AND event.block_number = input.block_number
     AND event.block_hash = input.block_hash
     AND event.transaction_hash IS NULL
     AND event.log_index IS NULL
     AND event.raw_fact_ref IS NOT DISTINCT FROM input.raw_fact_ref::JSONB
     AND event.derivation_kind = 'ens_v1_unwrapped_authority'
     AND event.before_state IS NOT DISTINCT FROM input.before_state::JSONB
     AND event.after_state IS NOT DISTINCT FROM input.after_state::JSONB
     AND event.manifest_version = input.manifest_version
     AND event.source_manifest_id IS NOT DISTINCT FROM input.source_manifest_id
     AND event.canonicality_state IN (
         'canonical'::canonicality_state,
         'safe'::canonicality_state,
         'finalized'::canonicality_state
     )
),
anchor_candidates AS (
    SELECT DISTINCT
        stale.event_identity AS stale_event_identity,
        current_event.event_identity AS current_event_identity,
        current_event.event_kind,
        stale.manifest_version AS stale_manifest_version,
        stale.source_manifest_id AS stale_source_manifest_id,
        current_event.current_manifest_version,
        current_event.current_source_manifest_id,
        stale.resource_id AS stale_resource_id,
        current_event.resource_id AS current_resource_id,
        (
            new_resource.resource_id IS NOT NULL
            AND new_resource.chain_id = 'base-mainnet'
            AND new_resource.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
            AND new_resource.provenance->>'authority_kind' = 'registry_only'
            AND new_resource.provenance->>'logical_name_id' = current_event.logical_name_id
            AND COALESCE(new_resource.provenance->>'namehash', '') <> ''
            AND new_resource.provenance->>'authority_key' =
                concat('registry-only:', new_resource.chain_id, ':', new_resource.provenance->>'namehash')
            AND old_resource.resource_id IS NOT NULL
            AND old_resource.chain_id = 'base-mainnet'
            AND old_resource.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
            AND old_resource.provenance->>'authority_kind' = 'registry_only'
            AND old_resource.provenance->>'logical_name_id' = current_event.logical_name_id
            AND COALESCE(old_resource.provenance->>'labelhash', '') <> ''
            AND old_resource.provenance->>'authority_key' =
                concat('registry-only:', old_resource.chain_id, ':', old_resource.provenance->>'labelhash')
            AND old_resource.resource_id <> new_resource.resource_id
            AND old_resource.provenance->>'authority_key' IS DISTINCT FROM
                new_resource.provenance->>'authority_key'
            AND lower(old_resource.provenance->>'labelhash') =
                lower(new_resource.provenance->>'labelhash')
        ) AS resource_verified,
        (
            (
                current_event.event_kind = 'AuthorityEpochChanged'
                AND (
                    stale.before_state IS NOT DISTINCT FROM current_event.before_state::JSONB
                    OR (
                        stale.before_state - 'authority_key' =
                            current_event.before_state::JSONB - 'authority_key'
                        AND stale.before_state->>'authority_kind' = 'registry_only'
                        AND current_event.before_state::JSONB->>'authority_kind' = 'registry_only'
                        AND stale.before_state->>'authority_key' =
                            old_resource.provenance->>'authority_key'
                        AND current_event.before_state::JSONB->>'authority_key' =
                            new_resource.provenance->>'authority_key'
                    )
                )
                AND (
                    stale.after_state IS NOT DISTINCT FROM current_event.after_state::JSONB
                    OR (
                        stale.after_state - 'authority_key' =
                            current_event.after_state::JSONB - 'authority_key'
                        AND stale.after_state->>'authority_kind' = 'registry_only'
                        AND current_event.after_state::JSONB->>'authority_kind' = 'registry_only'
                        AND stale.after_state->>'authority_key' =
                            old_resource.provenance->>'authority_key'
                        AND current_event.after_state::JSONB->>'authority_key' =
                            new_resource.provenance->>'authority_key'
                    )
                    OR (
                        stale.after_state - 'authority_key' =
                            current_event.after_state::JSONB - 'authority_key' - 'registry_owner'
                        AND stale.after_state->>'authority_kind' = 'registry_only'
                        AND current_event.after_state::JSONB->>'authority_kind' = 'registry_only'
                        AND NOT (stale.after_state ? 'registry_owner')
                        AND current_event.after_state::JSONB->>'registry_owner' ~ '^0x[0-9a-f]{40}$'
                        AND stale.after_state->>'authority_key' =
                            old_resource.provenance->>'authority_key'
                        AND current_event.after_state::JSONB->>'authority_key' =
                            new_resource.provenance->>'authority_key'
                    )
                )
            )
            OR (
                current_event.event_kind = 'SurfaceBound'
                AND stale.before_state IS NOT DISTINCT FROM current_event.before_state::JSONB
                AND stale.after_state - 'authority_key' =
                    current_event.after_state::JSONB - 'authority_key'
                AND stale.after_state->>'authority_kind' = 'registry_only'
                AND current_event.after_state::JSONB->>'authority_kind' = 'registry_only'
                AND stale.after_state->>'authority_key' =
                    old_resource.provenance->>'authority_key'
                AND current_event.after_state::JSONB->>'authority_key' =
                    new_resource.provenance->>'authority_key'
            )
            OR (
                current_event.event_kind = 'SurfaceUnbound'
                AND (
                    stale.before_state IS NOT DISTINCT FROM current_event.before_state::JSONB
                    OR (
                        stale.before_state - 'authority_key' =
                            current_event.before_state::JSONB - 'authority_key'
                        AND stale.before_state->>'authority_kind' = 'registry_only'
                        AND current_event.before_state::JSONB->>'authority_kind' = 'registry_only'
                        AND stale.before_state->>'authority_key' =
                            old_resource.provenance->>'authority_key'
                        AND current_event.before_state::JSONB->>'authority_key' =
                            new_resource.provenance->>'authority_key'
                    )
                )
                AND stale.after_state - 'authority_key' =
                    current_event.after_state::JSONB - 'authority_key'
                AND stale.after_state->>'authority_kind' = 'registry_only'
                AND current_event.after_state::JSONB->>'authority_kind' = 'registry_only'
                AND stale.after_state->>'authority_key' =
                    old_resource.provenance->>'authority_key'
                AND current_event.after_state::JSONB->>'authority_key' =
                    new_resource.provenance->>'authority_key'
            )
            OR (
                current_event.event_kind = 'ResolverChanged'
                AND stale.after_state->>'source_event' = 'AuthorityEpochChanged'
                AND current_event.after_state::JSONB->>'source_event' = 'AuthorityEpochChanged'
                AND stale.before_state IS NOT DISTINCT FROM current_event.before_state::JSONB
                AND stale.after_state IS NOT DISTINCT FROM current_event.after_state::JSONB
            )
        ) AS state_verified
    FROM current_event
    LEFT JOIN resources new_resource
      ON new_resource.resource_id = current_event.resource_id
    JOIN normalized_events stale
      ON stale.event_identity <> current_event.event_identity
     AND stale.namespace = 'basenames'
     AND stale.logical_name_id = current_event.logical_name_id
     AND stale.event_kind = current_event.event_kind
     AND stale.source_family = 'basenames_base_registry'
     AND stale.chain_id = 'base-mainnet'
     AND stale.block_number = current_event.block_number
     AND stale.block_hash = current_event.block_hash
     AND stale.transaction_hash IS NULL
     AND stale.log_index IS NULL
     AND stale.raw_fact_ref IS NOT DISTINCT FROM current_event.raw_fact_ref::JSONB
     AND stale.derivation_kind = 'ens_v1_unwrapped_authority'
     AND stale.canonicality_state IN (
         'canonical'::canonicality_state,
         'safe'::canonicality_state,
         'finalized'::canonicality_state
     )
    LEFT JOIN resources old_resource
      ON old_resource.resource_id = stale.resource_id
),
manifest_mismatch AS (
    SELECT *
    FROM anchor_candidates
    WHERE stale_manifest_version <> current_manifest_version
       OR stale_source_manifest_id IS DISTINCT FROM current_source_manifest_id
),
resource_mismatch AS (
    SELECT *
    FROM anchor_candidates
    WHERE resource_verified IS NOT TRUE
),
state_mismatch AS (
    SELECT *
    FROM anchor_candidates
    WHERE resource_verified IS TRUE
      AND state_verified IS NOT TRUE
),
supersession_map AS (
    SELECT
        stale_event_identity,
        current_event_identity
    FROM anchor_candidates
    WHERE resource_verified IS TRUE
      AND state_verified IS TRUE
      AND stale_manifest_version = current_manifest_version
      AND stale_source_manifest_id IS NOT DISTINCT FROM current_source_manifest_id
),
updated AS (
    UPDATE normalized_events event
    SET
        canonicality_state = 'orphaned'::canonicality_state,
        observed_at = now()
    FROM supersession_map repair
    WHERE event.event_identity = repair.stale_event_identity
      AND event.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
    RETURNING event.event_identity
)
SELECT concat('superseded:', event_identity)
FROM updated
UNION ALL
SELECT concat(
    'manifest_mismatch:',
    stale_event_identity,
    ' (current_event_identity=',
    current_event_identity,
    ', stale_manifest_version=',
    stale_manifest_version::TEXT,
    ', current_manifest_version=',
    current_manifest_version::TEXT,
    ', stale_source_manifest_id=',
    COALESCE(stale_source_manifest_id::TEXT, 'NULL'),
    ', current_source_manifest_id=',
    COALESCE(current_source_manifest_id::TEXT, 'NULL'),
    ')'
)
FROM manifest_mismatch
UNION ALL
SELECT concat(
    'resource_mismatch:',
    stale_event_identity,
    ' (current_event_identity=',
    current_event_identity,
    ', event_kind=',
    event_kind,
    ', stale_resource_id=',
    stale_resource_id,
    ', current_resource_id=',
    current_resource_id,
    ')'
)
FROM resource_mismatch
UNION ALL
SELECT concat(
    'state_mismatch:',
    stale_event_identity,
    ' (current_event_identity=',
    current_event_identity,
    ', event_kind=',
    event_kind,
    ')'
)
FROM state_mismatch
