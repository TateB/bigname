pub(super) const RESOLVER_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
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
    JOIN normalized_events permission
      ON permission.resource_id = ne.resource_id
     AND permission.event_kind = 'PermissionChanged'
     AND permission.canonicality_state IN (
         'canonical'::canonicality_state,
         'safe'::canonicality_state,
         'finalized'::canonicality_state
     )
    CROSS JOIN LATERAL (
        VALUES
            (permission.after_state -> 'scope'),
            (permission.before_state -> 'scope')
    ) AS scope(scope)
    WHERE ne.event_kind = 'PermissionScopeChanged'
      AND ne.resource_id IS NOT NULL
      AND (
          ne.after_state -> 'scope' ->> 'kind' = 'resource'
          OR ne.before_state -> 'scope' ->> 'kind' = 'resource'
      )
      AND scope.scope ->> 'kind' = 'resolver'
      AND scope.scope ->> 'chain_id' IS NOT NULL
      AND scope.scope ->> 'resolver_address' IS NOT NULL
      AND scope.scope ->> 'resolver_address' <> ''
)
"#;
