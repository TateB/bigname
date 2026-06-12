pub(super) const RECORD_INVENTORY_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
WITH changed_events AS (
    SELECT ne.*, change.change_id, change.changed_at
    FROM projection_normalized_event_changes change
    JOIN normalized_events ne
      ON ne.normalized_event_id = change.normalized_event_id
    WHERE change.change_id > $1
      AND change.change_id <= $2
),
record_inventory_changed_events AS (
    SELECT *
    FROM changed_events
    WHERE derivation_kind IN ('ens_v1_unwrapped_authority', 'ens_v2_resolver')
      AND event_kind IN ('RecordChanged', 'RecordVersionChanged', 'ResolverChanged')
      AND (
          resource_id IS NOT NULL
          OR event_kind = 'ResolverChanged'
      )
      AND logical_name_id IS NOT NULL
      AND chain_id IS NOT NULL
      AND block_number IS NOT NULL
      AND block_hash IS NOT NULL
),
record_inventory_changed_names AS (
    SELECT DISTINCT logical_name_id
    FROM record_inventory_changed_events
),
target_resource_events AS (
    SELECT DISTINCT
        target.resource_id,
        target.logical_name_id,
        target.block_number,
        COALESCE(target.log_index, -1::BIGINT) AS log_index
    FROM normalized_events target
    JOIN record_inventory_changed_names changed_name
      ON changed_name.logical_name_id = target.logical_name_id
    JOIN resources resource
      ON resource.resource_id = target.resource_id
     AND resource.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
    WHERE target.derivation_kind IN ('ens_v1_unwrapped_authority', 'ens_v2_resolver')
      AND target.event_kind IN ('RecordChanged', 'RecordVersionChanged', 'ResolverChanged')
      AND target.resource_id IS NOT NULL
      AND target.logical_name_id IS NOT NULL
      AND target.chain_id IS NOT NULL
      AND target.block_number IS NOT NULL
      AND target.block_hash IS NOT NULL
      AND target.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
),
candidate_keys AS (
    SELECT
        'record_inventory_current'::TEXT AS projection,
        resource_id::TEXT AS projection_key,
        jsonb_build_object('resource_id', resource_id::TEXT) AS key_payload,
        normalized_event_id,
        change_id,
        changed_at
    FROM record_inventory_changed_events
    WHERE resource_id IS NOT NULL

    UNION ALL

    SELECT
        'record_inventory_current'::TEXT AS projection,
        target.resource_id::TEXT AS projection_key,
        jsonb_build_object('resource_id', target.resource_id::TEXT) AS key_payload,
        changed.normalized_event_id,
        changed.change_id,
        changed.changed_at
    FROM record_inventory_changed_events changed
    JOIN target_resource_events target
      ON target.logical_name_id = changed.logical_name_id
     AND (
         changed.resource_id IS NULL
         OR target.resource_id <> changed.resource_id
     )
     AND (
         target.block_number,
         target.log_index
     ) >= (
         changed.block_number,
         COALESCE(changed.log_index, -1::BIGINT)
     )
)
"#;
