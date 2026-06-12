pub(super) const PERMISSIONS_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
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
    WHERE event_kind IN ('PermissionChanged', 'RootPermissionChanged', 'PermissionScopeChanged')
      AND resource_id IS NOT NULL
)
"#;
