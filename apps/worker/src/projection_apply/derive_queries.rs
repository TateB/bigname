pub(super) const UPSERT_SUFFIX: &str = r#"
INSERT INTO projection_invalidations (
    projection,
    projection_key,
    key_payload,
    first_change_id,
    last_change_id,
    first_normalized_event_id,
    last_normalized_event_id,
    last_changed_at,
    invalidated_at
)
SELECT
    projection,
    projection_key,
    key_payload,
    MIN(change_id),
    MAX(change_id),
    MIN(normalized_event_id),
    MAX(normalized_event_id),
    MAX(changed_at),
    now()
FROM candidate_keys
WHERE projection_key IS NOT NULL
  AND btrim(projection_key) <> ''
GROUP BY projection, projection_key, key_payload
ON CONFLICT (projection, projection_key)
DO UPDATE SET
    key_payload = EXCLUDED.key_payload,
    generation = projection_invalidations.generation + 1,
    first_change_id = LEAST(
        projection_invalidations.first_change_id,
        EXCLUDED.first_change_id
    ),
    last_change_id = GREATEST(
        projection_invalidations.last_change_id,
        EXCLUDED.last_change_id
    ),
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
    last_failure_at = NULL
"#;

const NAME_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
WITH changed_events AS (
    SELECT ne.*, change.change_id, change.changed_at
    FROM projection_normalized_event_changes change
    JOIN normalized_events ne
      ON ne.normalized_event_id = change.normalized_event_id
    WHERE change.change_id > $1
      AND change.change_id <= $2
),
candidate_keys AS (
    SELECT
        'name_current'::TEXT AS projection,
        logical_name_id AS projection_key,
        jsonb_build_object('logical_name_id', logical_name_id) AS key_payload,
        normalized_event_id,
        change_id,
        changed_at
    FROM changed_events
    WHERE logical_name_id IS NOT NULL
)
"#;

const CHILDREN_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
WITH changed_events AS (
    SELECT ne.*, change.change_id, change.changed_at
    FROM projection_normalized_event_changes change
    JOIN normalized_events ne
      ON ne.normalized_event_id = change.normalized_event_id
    WHERE change.change_id > $1
      AND change.change_id <= $2
),
candidate_keys AS (
    SELECT
        'children_current'::TEXT AS projection,
        ne.logical_name_id AS projection_key,
        jsonb_build_object('parent_logical_name_id', ne.logical_name_id) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    WHERE ne.event_kind IN ('SubregistryChanged', 'ParentChanged')
      AND ne.logical_name_id IS NOT NULL

    UNION ALL

    SELECT
        'children_current'::TEXT AS projection,
        parent.logical_name_id AS projection_key,
        jsonb_build_object('parent_logical_name_id', parent.logical_name_id) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    JOIN name_surfaces parent
      ON parent.namehash = ne.after_state ->> 'parent_node'
    WHERE ne.event_kind = 'SubregistryChanged'

    UNION ALL

    SELECT
        'children_current'::TEXT AS projection,
        parent.logical_name_id AS projection_key,
        jsonb_build_object('parent_logical_name_id', parent.logical_name_id) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    JOIN name_surfaces child
      ON child.logical_name_id = ne.logical_name_id
    JOIN name_surfaces parent
      ON parent.namespace = child.namespace
     AND parent.chain_id = child.chain_id
     AND child.normalized_name LIKE '%.%'
     AND parent.normalized_name = substring(
         child.normalized_name FROM position('.' IN child.normalized_name) + 1
     )
    WHERE ne.event_kind IN (
        'RegistrationGranted',
        'RegistrationRenewed',
        'RegistrationReleased'
    )
)
"#;

const PERMISSIONS_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
WITH changed_events AS (
    SELECT ne.*, change.change_id, change.changed_at
    FROM projection_normalized_event_changes change
    JOIN normalized_events ne
      ON ne.normalized_event_id = change.normalized_event_id
    WHERE change.change_id > $1
      AND change.change_id <= $2
),
candidate_keys AS (
    SELECT
        'permissions_current'::TEXT AS projection,
        resource_id::TEXT AS projection_key,
        jsonb_build_object('resource_id', resource_id::TEXT) AS key_payload,
        normalized_event_id,
        change_id,
        changed_at
    FROM changed_events
    WHERE event_kind IN ('PermissionChanged', 'PermissionScopeChanged')
      AND resource_id IS NOT NULL
)
"#;

const RECORD_INVENTORY_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
WITH changed_events AS (
    SELECT ne.*, change.change_id, change.changed_at
    FROM projection_normalized_event_changes change
    JOIN normalized_events ne
      ON ne.normalized_event_id = change.normalized_event_id
    WHERE change.change_id > $1
      AND change.change_id <= $2
),
candidate_keys AS (
    SELECT
        'record_inventory_current'::TEXT AS projection,
        resource_id::TEXT AS projection_key,
        jsonb_build_object('resource_id', resource_id::TEXT) AS key_payload,
        normalized_event_id,
        change_id,
        changed_at
    FROM changed_events
    WHERE derivation_kind IN ('ens_v1_unwrapped_authority', 'ens_v2_resolver')
      AND event_kind IN ('RecordChanged', 'RecordVersionChanged', 'ResolverChanged')
      AND resource_id IS NOT NULL
      AND logical_name_id IS NOT NULL
      AND chain_id IS NOT NULL
      AND block_number IS NOT NULL
      AND block_hash IS NOT NULL
)
"#;

const RESOLVER_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
WITH changed_events AS (
    SELECT ne.*, change.change_id, change.changed_at
    FROM projection_normalized_event_changes change
    JOIN normalized_events ne
      ON ne.normalized_event_id = change.normalized_event_id
    WHERE change.change_id > $1
      AND change.change_id <= $2
),
candidate_keys AS (
    SELECT
        'resolver_current'::TEXT AS projection,
        ne.chain_id || ':' || lower(resolver.resolver_address) AS projection_key,
        jsonb_build_object(
            'chain_id', ne.chain_id,
            'resolver_address', lower(resolver.resolver_address)
        ) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    CROSS JOIN LATERAL (
        VALUES
            (ne.after_state ->> 'resolver'),
            (ne.before_state ->> 'resolver')
    ) AS resolver(resolver_address)
    WHERE ne.event_kind IN ('ResolverChanged', 'AliasChanged')
      AND ne.chain_id IS NOT NULL
      AND resolver.resolver_address IS NOT NULL
      AND resolver.resolver_address <> ''

    UNION ALL

    SELECT
        'resolver_current'::TEXT AS projection,
        scope.scope ->> 'chain_id'
            || ':' || lower(scope.scope ->> 'resolver_address') AS projection_key,
        jsonb_build_object(
            'chain_id', scope.scope ->> 'chain_id',
            'resolver_address', lower(scope.scope ->> 'resolver_address')
        ) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    CROSS JOIN LATERAL (
        VALUES
            (ne.after_state -> 'scope'),
            (ne.before_state -> 'scope')
    ) AS scope(scope)
    WHERE ne.event_kind IN ('PermissionChanged', 'PermissionScopeChanged')
      AND scope.scope ->> 'kind' = 'resolver'
      AND scope.scope ->> 'chain_id' IS NOT NULL
      AND scope.scope ->> 'resolver_address' IS NOT NULL
      AND scope.scope ->> 'resolver_address' <> ''
)
"#;

const ADDRESS_NAMES_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
WITH changed_events AS (
    SELECT ne.*, change.change_id, change.changed_at
    FROM projection_normalized_event_changes change
    JOIN normalized_events ne
      ON ne.normalized_event_id = change.normalized_event_id
    WHERE change.change_id > $1
      AND change.change_id <= $2
),
candidate_keys AS (
    SELECT
        'address_names_current'::TEXT AS projection,
        lower(address.address) AS projection_key,
        jsonb_build_object('address', lower(address.address)) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    CROSS JOIN LATERAL (
        VALUES
            (ne.after_state ->> 'registrant'),
            (ne.before_state ->> 'registrant'),
            (ne.after_state ->> 'to'),
            (ne.before_state ->> 'to'),
            (ne.after_state ->> 'owner'),
            (ne.before_state ->> 'owner')
    ) AS address(address)
    WHERE ne.event_kind IN (
        'RegistrationGranted',
        'TokenControlTransferred',
        'AuthorityTransferred',
        'AuthorityEpochChanged',
        'TokenRegenerated'
    )
      AND address.address IS NOT NULL
      AND address.address <> ''
)
"#;

const PRIMARY_NAMES_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
WITH changed_events AS (
    SELECT ne.*, change.change_id, change.changed_at
    FROM projection_normalized_event_changes change
    JOIN normalized_events ne
      ON ne.normalized_event_id = change.normalized_event_id
    WHERE change.change_id > $1
      AND change.change_id <= $2
),
candidate_keys AS (
    SELECT
        'primary_names_current'::TEXT AS projection,
        lower(tuple.address) || ':' || tuple.namespace || ':' || tuple.coin_type AS projection_key,
        jsonb_build_object(
            'address', lower(tuple.address),
            'namespace', tuple.namespace,
            'coin_type', tuple.coin_type
        ) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    CROSS JOIN LATERAL (
        VALUES
            (
                ne.after_state ->> 'address',
                COALESCE(ne.after_state ->> 'namespace', ne.namespace),
                ne.after_state ->> 'coin_type'
            ),
            (
                ne.before_state ->> 'address',
                COALESCE(ne.before_state ->> 'namespace', ne.namespace),
                ne.before_state ->> 'coin_type'
            )
    ) AS tuple(address, namespace, coin_type)
    WHERE ne.event_kind = 'ReverseChanged'
      AND tuple.address IS NOT NULL
      AND tuple.address <> ''
      AND tuple.namespace IS NOT NULL
      AND tuple.namespace <> ''
      AND tuple.coin_type IS NOT NULL
      AND tuple.coin_type <> ''

    UNION ALL

    SELECT
        'primary_names_current'::TEXT AS projection,
        lower(tuple.claim_source ->> 'address')
            || ':' || COALESCE(tuple.claim_source ->> 'namespace', ne.namespace)
            || ':' || (tuple.claim_source ->> 'coin_type') AS projection_key,
        jsonb_build_object(
            'address', lower(tuple.claim_source ->> 'address'),
            'namespace', COALESCE(tuple.claim_source ->> 'namespace', ne.namespace),
            'coin_type', tuple.claim_source ->> 'coin_type'
        ) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    CROSS JOIN LATERAL (
        VALUES
            (ne.after_state -> 'primary_claim_source'),
            (ne.before_state -> 'primary_claim_source')
    ) AS tuple(claim_source)
    WHERE ne.event_kind = 'RecordChanged'
      AND ne.logical_name_id IS NULL
      AND ne.resource_id IS NULL
      AND ne.after_state ->> 'record_key' = 'name'
      AND tuple.claim_source ->> 'address' IS NOT NULL
      AND tuple.claim_source ->> 'address' <> ''
      AND COALESCE(tuple.claim_source ->> 'namespace', ne.namespace) IS NOT NULL
      AND COALESCE(tuple.claim_source ->> 'namespace', ne.namespace) <> ''
      AND tuple.claim_source ->> 'coin_type' IS NOT NULL
      AND tuple.claim_source ->> 'coin_type' <> ''
)
"#;

pub(super) const INVALIDATION_QUERY_PREFIXES: &[&str] = &[
    NAME_CURRENT_INVALIDATIONS_PREFIX,
    CHILDREN_CURRENT_INVALIDATIONS_PREFIX,
    PERMISSIONS_CURRENT_INVALIDATIONS_PREFIX,
    RECORD_INVENTORY_CURRENT_INVALIDATIONS_PREFIX,
    RESOLVER_CURRENT_INVALIDATIONS_PREFIX,
    ADDRESS_NAMES_CURRENT_INVALIDATIONS_PREFIX,
    PRIMARY_NAMES_CURRENT_INVALIDATIONS_PREFIX,
];
