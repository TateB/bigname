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
        $11::BIGINT[],
        $12::TEXT[]
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
        source_manifest_id,
        source_family
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
     AND event.source_family = input.source_family
     AND event.source_family IN ('basenames_base_registry', 'basenames_base_registrar')
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
registrar_before_keys AS MATERIALIZED (
    SELECT DISTINCT
        stale.before_state->>'authority_key' AS stale_authority_key
    FROM current_event
    JOIN normalized_events stale
      ON stale.event_identity <> current_event.event_identity
     AND stale.namespace = 'basenames'
     AND stale.logical_name_id = current_event.logical_name_id
     AND stale.event_kind = current_event.event_kind
     AND stale.source_family = current_event.source_family
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
    WHERE current_event.source_family = 'basenames_base_registrar'
      AND current_event.event_kind = 'AuthorityEpochChanged'
      AND stale.before_state->>'authority_kind' = 'registry_only'
      AND COALESCE(stale.before_state->>'authority_key', '') <> ''
      AND stale.after_state->>'authority_kind' = 'registrar'
      AND current_event.after_state::JSONB->>'authority_kind' = 'registrar'
      AND stale.after_state->>'authority_key' =
          current_event.after_state::JSONB->>'authority_key'
),
registrar_legacy_registry_resources AS MATERIALIZED (
    SELECT
        resource.resource_id,
        resource.chain_id,
        resource.canonicality_state,
        resource.provenance,
        resource.provenance->>'authority_key' AS authority_key,
        lower(resource.provenance->>'labelhash') AS labelhash
    FROM resources resource
    JOIN registrar_before_keys candidate
      ON candidate.stale_authority_key = resource.provenance->>'authority_key'
    WHERE resource.chain_id = 'base-mainnet'
      AND resource.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
      AND resource.provenance->>'authority_kind' = 'registry_only'
      AND COALESCE(resource.provenance->>'labelhash', '') <> ''
      AND resource.provenance->>'authority_key' = concat(
          'registry-only:',
          resource.chain_id,
          ':',
          resource.provenance->>'labelhash'
      )
),
registrar_current_registry_resources AS MATERIALIZED (
    SELECT
        resource.resource_id,
        resource.chain_id,
        resource.canonicality_state,
        resource.provenance,
        legacy.authority_key AS stale_authority_key
    FROM resources resource
    JOIN registrar_legacy_registry_resources legacy
      ON resource.provenance->>'logical_name_id' =
          legacy.provenance->>'logical_name_id'
     AND lower(resource.provenance->>'labelhash') = legacy.labelhash
    WHERE resource.chain_id = 'base-mainnet'
      AND resource.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
      AND resource.provenance->>'authority_kind' = 'registry_only'
      AND COALESCE(resource.provenance->>'namehash', '') <> ''
      AND resource.provenance->>'authority_key' = concat(
          'registry-only:',
          resource.chain_id,
          ':',
          resource.provenance->>'namehash'
      )
),
anchor_candidates AS (
    SELECT DISTINCT
        stale.event_identity AS stale_event_identity,
        current_event.event_identity AS current_event_identity,
        current_event.event_kind,
        current_event.source_family,
        stale.manifest_version AS stale_manifest_version,
        stale.source_manifest_id AS stale_source_manifest_id,
        current_event.current_manifest_version,
        current_event.current_source_manifest_id,
        stale.resource_id AS stale_resource_id,
        current_event.resource_id AS current_resource_id,
        (
            (
                current_event.source_family = 'basenames_base_registry'
                AND (
                    current_event.event_kind IN (
                        'AuthorityEpochChanged',
                        'SurfaceBound',
                        'SurfaceUnbound'
                    )
                    OR (
                        current_event.event_kind = 'ResolverChanged'
                        AND stale.after_state->>'source_event' = 'AuthorityEpochChanged'
                        AND current_event.after_state::JSONB->>'source_event' =
                            'AuthorityEpochChanged'
                    )
                )
            )
            OR (
                current_event.source_family = 'basenames_base_registrar'
                AND current_event.event_kind = 'AuthorityEpochChanged'
                AND stale.before_state->>'authority_kind' = 'registry_only'
                AND COALESCE(stale.before_state->>'authority_key', '') <> ''
                AND stale.after_state->>'authority_kind' = 'registrar'
                AND current_event.after_state::JSONB->>'authority_kind' =
                    'registrar'
                AND stale.after_state->>'authority_key' =
                    current_event.after_state::JSONB->>'authority_key'
            )
        ) AS repair_candidate,
        (
            (
                current_event.source_family = 'basenames_base_registry'
                AND current_event_resource.resource_id IS NOT NULL
                AND current_event_resource.chain_id = 'base-mainnet'
                AND current_event_resource.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
                )
                AND current_event_resource.provenance->>'authority_kind' = 'registry_only'
                AND current_event_resource.provenance->>'logical_name_id' =
                    current_event.logical_name_id
                AND COALESCE(current_event_resource.provenance->>'namehash', '') <> ''
                AND current_event_resource.provenance->>'authority_key' =
                    concat(
                        'registry-only:',
                        current_event_resource.chain_id,
                        ':',
                        current_event_resource.provenance->>'namehash'
                    )
                AND stale_event_resource.resource_id IS NOT NULL
                AND stale_event_resource.chain_id = 'base-mainnet'
                AND stale_event_resource.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
                )
                AND stale_event_resource.provenance->>'authority_kind' = 'registry_only'
                AND stale_event_resource.provenance->>'logical_name_id' =
                    current_event.logical_name_id
                AND COALESCE(stale_event_resource.provenance->>'labelhash', '') <> ''
                AND stale_event_resource.provenance->>'authority_key' =
                    concat(
                        'registry-only:',
                        stale_event_resource.chain_id,
                        ':',
                        stale_event_resource.provenance->>'labelhash'
                    )
                AND stale_event_resource.resource_id <> current_event_resource.resource_id
                AND stale_event_resource.provenance->>'authority_key' IS DISTINCT FROM
                    current_event_resource.provenance->>'authority_key'
                AND lower(stale_event_resource.provenance->>'labelhash') =
                    lower(current_event_resource.provenance->>'labelhash')
            )
            OR (
                current_event.source_family = 'basenames_base_registrar'
                AND current_event.event_kind = 'AuthorityEpochChanged'
                AND stale.before_state->>'authority_kind' = 'registry_only'
                AND stale_registry_before_resource.resource_id IS NOT NULL
                AND stale_registry_before_resource.chain_id = 'base-mainnet'
                AND stale_registry_before_resource.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
                )
                AND stale_registry_before_resource.provenance->>'authority_kind' =
                    'registry_only'
                AND stale_registry_before_resource.provenance->>'logical_name_id' =
                    current_event.logical_name_id
                AND COALESCE(stale_registry_before_resource.provenance->>'labelhash', '') <> ''
                AND stale_registry_before_resource.provenance->>'authority_key' =
                    concat(
                        'registry-only:',
                        stale_registry_before_resource.chain_id,
                        ':',
                        stale_registry_before_resource.provenance->>'labelhash'
                    )
                AND current_registry_before_resource.resource_id IS NOT NULL
                AND current_registry_before_resource.chain_id = 'base-mainnet'
                AND current_registry_before_resource.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
                )
                AND current_registry_before_resource.provenance->>'authority_kind' =
                    'registry_only'
                AND current_registry_before_resource.provenance->>'logical_name_id' =
                    current_event.logical_name_id
                AND COALESCE(current_registry_before_resource.provenance->>'namehash', '') <> ''
                AND current_registry_before_resource.provenance->>'authority_key' =
                    concat(
                        'registry-only:',
                        current_registry_before_resource.chain_id,
                        ':',
                        current_registry_before_resource.provenance->>'namehash'
                    )
                AND stale_registry_before_resource.resource_id <>
                    current_registry_before_resource.resource_id
                AND lower(stale_registry_before_resource.provenance->>'labelhash') =
                    lower(current_registry_before_resource.provenance->>'labelhash')
                AND stale_event_resource.resource_id IS NOT NULL
                AND current_event_resource.resource_id IS NOT NULL
                AND stale_event_resource.resource_id = current_event_resource.resource_id
                AND current_event_resource.chain_id = 'base-mainnet'
                AND current_event_resource.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
                )
                AND current_event_resource.provenance->>'authority_kind' = 'registrar'
                AND current_event_resource.provenance->>'logical_name_id' =
                    current_event.logical_name_id
                AND current_event_resource.provenance->>'authority_key' =
                    current_event.after_state::JSONB->>'authority_key'
                AND lower(current_event_resource.provenance->>'labelhash') =
                    lower(stale_registry_before_resource.provenance->>'labelhash')
                AND stale.after_state->>'authority_kind' = 'registrar'
                AND current_event.after_state::JSONB->>'authority_kind' = 'registrar'
                AND stale.after_state->>'authority_key' =
                    current_event.after_state::JSONB->>'authority_key'
            )
        ) AS resource_verified,
        (
            (
                current_event.source_family = 'basenames_base_registry'
                AND (
                    (
                        current_event.event_kind = 'AuthorityEpochChanged'
                        AND (
                            stale.before_state IS NOT DISTINCT FROM
                                current_event.before_state::JSONB
                            OR (
                                stale.before_state - 'authority_key' =
                                    current_event.before_state::JSONB - 'authority_key'
                                AND stale.before_state->>'authority_kind' = 'registry_only'
                                AND current_event.before_state::JSONB->>'authority_kind' =
                                    'registry_only'
                                AND stale.before_state->>'authority_key' =
                                    stale_event_resource.provenance->>'authority_key'
                                AND current_event.before_state::JSONB->>'authority_key' =
                                    current_event_resource.provenance->>'authority_key'
                            )
                        )
                        AND (
                            stale.after_state IS NOT DISTINCT FROM
                                current_event.after_state::JSONB
                            OR (
                                stale.after_state - 'authority_key' =
                                    current_event.after_state::JSONB - 'authority_key'
                                AND stale.after_state->>'authority_kind' = 'registry_only'
                                AND current_event.after_state::JSONB->>'authority_kind' =
                                    'registry_only'
                                AND stale.after_state->>'authority_key' =
                                    stale_event_resource.provenance->>'authority_key'
                                AND current_event.after_state::JSONB->>'authority_key' =
                                    current_event_resource.provenance->>'authority_key'
                            )
                            OR (
                                stale.after_state - 'authority_key' =
                                    current_event.after_state::JSONB - 'authority_key' -
                                        'registry_owner'
                                AND stale.after_state->>'authority_kind' = 'registry_only'
                                AND current_event.after_state::JSONB->>'authority_kind' =
                                    'registry_only'
                                AND NOT (stale.after_state ? 'registry_owner')
                                AND current_event.after_state::JSONB->>'registry_owner' ~
                                    '^0x[0-9a-f]{40}$'
                                AND stale.after_state->>'authority_key' =
                                    stale_event_resource.provenance->>'authority_key'
                                AND current_event.after_state::JSONB->>'authority_key' =
                                    current_event_resource.provenance->>'authority_key'
                            )
                        )
                    )
                    OR (
                        current_event.event_kind = 'SurfaceBound'
                        AND stale.before_state IS NOT DISTINCT FROM
                            current_event.before_state::JSONB
                        AND stale.after_state - 'authority_key' =
                            current_event.after_state::JSONB - 'authority_key'
                        AND stale.after_state->>'authority_kind' = 'registry_only'
                        AND current_event.after_state::JSONB->>'authority_kind' =
                            'registry_only'
                        AND stale.after_state->>'authority_key' =
                            stale_event_resource.provenance->>'authority_key'
                        AND current_event.after_state::JSONB->>'authority_key' =
                            current_event_resource.provenance->>'authority_key'
                    )
                    OR (
                        current_event.event_kind = 'SurfaceUnbound'
                        AND (
                            stale.before_state IS NOT DISTINCT FROM
                                current_event.before_state::JSONB
                            OR (
                                stale.before_state - 'authority_key' =
                                    current_event.before_state::JSONB - 'authority_key'
                                AND stale.before_state->>'authority_kind' = 'registry_only'
                                AND current_event.before_state::JSONB->>'authority_kind' =
                                    'registry_only'
                                AND stale.before_state->>'authority_key' =
                                    stale_event_resource.provenance->>'authority_key'
                                AND current_event.before_state::JSONB->>'authority_key' =
                                    current_event_resource.provenance->>'authority_key'
                            )
                        )
                        AND stale.after_state - 'authority_key' =
                            current_event.after_state::JSONB - 'authority_key'
                        AND stale.after_state->>'authority_kind' = 'registry_only'
                        AND current_event.after_state::JSONB->>'authority_kind' =
                            'registry_only'
                        AND stale.after_state->>'authority_key' =
                            stale_event_resource.provenance->>'authority_key'
                        AND current_event.after_state::JSONB->>'authority_key' =
                            current_event_resource.provenance->>'authority_key'
                    )
                    OR (
                        current_event.event_kind = 'ResolverChanged'
                        AND stale.after_state->>'source_event' = 'AuthorityEpochChanged'
                        AND current_event.after_state::JSONB->>'source_event' =
                            'AuthorityEpochChanged'
                        AND stale.before_state IS NOT DISTINCT FROM
                            current_event.before_state::JSONB
                        AND stale.after_state IS NOT DISTINCT FROM
                            current_event.after_state::JSONB
                    )
                )
            )
            OR (
                current_event.source_family = 'basenames_base_registrar'
                AND current_event.event_kind = 'AuthorityEpochChanged'
                AND stale.before_state->>'authority_kind' = 'registry_only'
                AND stale.before_state->>'authority_key' =
                    stale_registry_before_resource.provenance->>'authority_key'
                AND stale.after_state IS NOT DISTINCT FROM current_event.after_state::JSONB
                AND (
                    (
                        current_event.before_state::JSONB->>'authority_kind' =
                            'registry_only'
                        AND current_event.before_state::JSONB - 'authority_key' =
                            stale.before_state - 'authority_key'
                        AND current_event.before_state::JSONB->>'authority_key' =
                            current_registry_before_resource.provenance->>'authority_key'
                    )
                    OR (
                        current_event.before_state::JSONB IS NOT DISTINCT FROM
                            jsonb_build_object('authority_kind', NULL, 'authority_key', NULL)
                        AND stale.before_state - 'authority_key' IS NOT DISTINCT FROM
                            jsonb_build_object('authority_kind', 'registry_only')
                    )
                )
            )
        ) AS state_verified
    FROM current_event
    LEFT JOIN resources current_event_resource
      ON current_event_resource.resource_id = current_event.resource_id
    JOIN normalized_events stale
      ON stale.event_identity <> current_event.event_identity
     AND stale.namespace = 'basenames'
     AND stale.logical_name_id = current_event.logical_name_id
     AND stale.event_kind = current_event.event_kind
     AND stale.source_family = current_event.source_family
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
    LEFT JOIN resources stale_event_resource
      ON stale_event_resource.resource_id = stale.resource_id
    LEFT JOIN registrar_legacy_registry_resources stale_registry_before_resource
      ON stale_registry_before_resource.authority_key =
         stale.before_state->>'authority_key'
    LEFT JOIN registrar_current_registry_resources current_registry_before_resource
      ON current_registry_before_resource.stale_authority_key =
         stale.before_state->>'authority_key'
),
supersession_map AS (
    SELECT
        stale_event_identity,
        current_event_identity
    FROM anchor_candidates
    WHERE repair_candidate IS TRUE
      AND resource_verified IS TRUE
      AND state_verified IS TRUE
      AND stale_manifest_version = current_manifest_version
      AND stale_source_manifest_id IS NOT DISTINCT FROM current_source_manifest_id
),
manifest_mismatch AS (
    SELECT *
    FROM anchor_candidates candidate
    WHERE repair_candidate IS TRUE
      AND (
          stale_manifest_version <> current_manifest_version
          OR stale_source_manifest_id IS DISTINCT FROM current_source_manifest_id
      )
      AND NOT EXISTS (
          SELECT 1
          FROM supersession_map repair
          WHERE repair.stale_event_identity = candidate.stale_event_identity
      )
),
resource_mismatch AS (
    SELECT *
    FROM anchor_candidates candidate
    WHERE repair_candidate IS TRUE
      AND resource_verified IS NOT TRUE
      AND NOT EXISTS (
          SELECT 1
          FROM supersession_map repair
          WHERE repair.stale_event_identity = candidate.stale_event_identity
      )
),
state_mismatch AS (
    SELECT *
    FROM anchor_candidates candidate
    WHERE repair_candidate IS TRUE
      AND resource_verified IS TRUE
      AND state_verified IS NOT TRUE
      AND NOT EXISTS (
          SELECT 1
          FROM supersession_map repair
          WHERE repair.stale_event_identity = candidate.stale_event_identity
      )
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
    ', source_family=',
    source_family,
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
    ', source_family=',
    source_family,
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
    ', source_family=',
    source_family,
    ', event_kind=',
    event_kind,
    ')'
)
FROM state_mismatch
