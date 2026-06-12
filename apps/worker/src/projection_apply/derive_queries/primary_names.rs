pub(super) const PRIMARY_NAMES_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
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
    WHERE ne.event_kind = 'ResolverChanged'
      AND ne.logical_name_id IS NULL
      AND ne.resource_id IS NULL
      AND tuple.claim_source ->> 'address' IS NOT NULL
      AND tuple.claim_source ->> 'address' <> ''
      AND COALESCE(tuple.claim_source ->> 'namespace', ne.namespace) IS NOT NULL
      AND COALESCE(tuple.claim_source ->> 'namespace', ne.namespace) <> ''
      AND tuple.claim_source ->> 'coin_type' IS NOT NULL
      AND tuple.claim_source ->> 'coin_type' <> ''
)
"#;
