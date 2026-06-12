pub(super) const CHILDREN_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
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
