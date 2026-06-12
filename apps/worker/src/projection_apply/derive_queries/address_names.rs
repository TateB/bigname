pub(super) const ADDRESS_NAMES_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
WITH changed_events AS (
    SELECT ne.*, change.change_id, change.changed_at
    FROM projection_normalized_event_changes change
    JOIN normalized_events ne
      ON ne.normalized_event_id = change.normalized_event_id
    WHERE change.change_id > $1
      AND change.change_id <= $2
),
resource_permission_changed_names AS (
    SELECT DISTINCT
        logical_name_id,
        normalized_event_id,
        change_id,
        changed_at
    FROM changed_events
    WHERE logical_name_id IS NOT NULL
      AND (
          (
              event_kind = 'PermissionChanged'
              AND (
                  after_state -> 'scope' ->> 'kind' = 'resource'
                  OR before_state -> 'scope' ->> 'kind' = 'resource'
              )
          )
          OR (
              event_kind = 'PermissionScopeChanged'
              AND resource_id IS NOT NULL
              AND (
                  after_state -> 'scope' ->> 'kind' = 'resource'
                  OR before_state -> 'scope' ->> 'kind' = 'resource'
              )
          )
      )
),
candidate_keys AS (
    SELECT
        'address_names_current'::TEXT AS projection,
        CASE
            WHEN ne.logical_name_id IS NOT NULL
            THEN lower(address.address) || ':' || ne.logical_name_id
            ELSE lower(address.address)
        END AS projection_key,
        jsonb_strip_nulls(jsonb_build_object(
            'address', lower(address.address),
            'logical_name_id', ne.logical_name_id
        )) AS key_payload,
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

    UNION ALL

    SELECT
        'address_names_current'::TEXT AS projection,
        CASE
            WHEN ne.logical_name_id IS NOT NULL
            THEN lower(address.address) || ':' || ne.logical_name_id
            ELSE lower(address.address)
        END AS projection_key,
        jsonb_strip_nulls(jsonb_build_object(
            'address', lower(address.address),
            'logical_name_id', ne.logical_name_id
        )) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    CROSS JOIN LATERAL (
        VALUES
            (ne.after_state ->> 'subject', ne.after_state -> 'scope'),
            (ne.before_state ->> 'subject', ne.before_state -> 'scope')
    ) AS address(address, scope)
    WHERE ne.event_kind = 'PermissionChanged'
      AND address.scope ->> 'kind' = 'resource'
      AND address.address IS NOT NULL
      AND address.address <> ''

    UNION ALL

    SELECT
        'address_names_current'::TEXT AS projection,
        lower(address.address) || ':' || ne.logical_name_id AS projection_key,
        jsonb_build_object(
            'address', lower(address.address),
            'logical_name_id', ne.logical_name_id
        ) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    JOIN normalized_events permission
      ON permission.resource_id = ne.resource_id
     AND permission.logical_name_id = ne.logical_name_id
     AND permission.event_kind = 'PermissionChanged'
     AND permission.canonicality_state IN (
         'canonical'::canonicality_state,
         'safe'::canonicality_state,
         'finalized'::canonicality_state
     )
    CROSS JOIN LATERAL (
        VALUES
            (permission.after_state ->> 'subject', permission.after_state -> 'scope'),
            (permission.before_state ->> 'subject', permission.before_state -> 'scope')
    ) AS address(address, scope)
    WHERE ne.event_kind = 'PermissionScopeChanged'
      AND ne.resource_id IS NOT NULL
      AND ne.logical_name_id IS NOT NULL
      AND (
          ne.after_state -> 'scope' ->> 'kind' = 'resource'
          OR ne.before_state -> 'scope' ->> 'kind' = 'resource'
      )
      AND address.scope ->> 'kind' = 'resource'
      AND address.address IS NOT NULL
      AND address.address <> ''

    UNION ALL

    SELECT
        'address_names_current'::TEXT AS projection,
        lower(fallback.address) || ':' || changed.logical_name_id AS projection_key,
        jsonb_build_object(
            'address', lower(fallback.address),
            'logical_name_id', changed.logical_name_id
        ) AS key_payload,
        changed.normalized_event_id,
        changed.change_id,
        changed.changed_at
    FROM resource_permission_changed_names changed
    JOIN normalized_events ne
      ON ne.logical_name_id = changed.logical_name_id
    CROSS JOIN LATERAL (
        VALUES
            (ne.after_state ->> 'registrant'),
            (ne.after_state ->> 'to')
    ) AS fallback(address)
    WHERE ne.event_kind IN ('RegistrationGranted', 'TokenControlTransferred')
      AND ne.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
      AND fallback.address IS NOT NULL
      AND fallback.address <> ''

    UNION ALL

    SELECT
        'address_names_current'::TEXT AS projection,
        lower(fallback.address) || ':' || changed.logical_name_id AS projection_key,
        jsonb_build_object(
            'address', lower(fallback.address),
            'logical_name_id', changed.logical_name_id
        ) AS key_payload,
        changed.normalized_event_id,
        changed.change_id,
        changed.changed_at
    FROM resource_permission_changed_names changed
    JOIN normalized_events ne
      ON ne.logical_name_id = changed.logical_name_id
    CROSS JOIN LATERAL (
        VALUES (ne.after_state ->> 'owner')
    ) AS fallback(address)
    WHERE ne.event_kind = 'AuthorityTransferred'
      AND ne.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
      AND fallback.address IS NOT NULL
      AND fallback.address <> ''
)
"#;
