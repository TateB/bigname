        WITH input AS (
            SELECT *
            FROM unnest(
                $1::TEXT[],
                $2::UUID[],
                $3::TEXT[],
                $4::TEXT[],
                $5::TEXT[],
                $6::TEXT[],
                $7::TEXT[]
            ) AS input(
                event_identity,
                resource_id,
                logical_name_id,
                event_kind,
                old_before_state,
                new_before_state,
                after_state
            )
        ),
        repair_map AS (
            SELECT input.*
            FROM input
            JOIN resources resource
              ON resource.resource_id = input.resource_id
             AND resource.chain_id = 'ethereum-mainnet'
             AND resource.canonicality_state IN (
                 'canonical'::canonicality_state,
                 'safe'::canonicality_state,
                 'finalized'::canonicality_state
             )
             AND resource.provenance->>'logical_name_id' = input.logical_name_id
             AND resource.provenance->>'authority_kind' IN (
                 'registrar',
                 'wrapper',
                 'registry_only'
             )
            WHERE (
                input.event_kind = 'AuthorityTransferred'
                AND input.old_before_state::JSONB - 'owner' =
                    input.new_before_state::JSONB - 'owner'
                AND (
                    (
                        jsonb_typeof(input.old_before_state::JSONB -> 'owner') =
                            'string'
                        AND btrim(input.old_before_state::JSONB ->> 'owner') <> ''
                        AND jsonb_typeof(input.new_before_state::JSONB -> 'owner') =
                            'string'
                        AND btrim(input.new_before_state::JSONB ->> 'owner') <> ''
                    )
                    OR (
                        jsonb_typeof(input.old_before_state::JSONB -> 'owner') =
                            'string'
                        AND btrim(input.old_before_state::JSONB ->> 'owner') <> ''
                        AND input.new_before_state::JSONB -> 'owner' = 'null'::JSONB
                    )
                    OR (
                        input.old_before_state::JSONB -> 'owner' = 'null'::JSONB
                        AND jsonb_typeof(input.new_before_state::JSONB -> 'owner') =
                            'string'
                        AND btrim(input.new_before_state::JSONB ->> 'owner') <> ''
                    )
                )
            )
            OR (
                input.event_kind = 'RecordVersionChanged'
                AND input.old_before_state::JSONB - 'record_version' =
                    input.new_before_state::JSONB - 'record_version'
                AND COALESCE(input.after_state::JSONB ->> 'record_version', '') ~
                    '^[0-9]+$'
                AND (
                    (
                        input.old_before_state::JSONB -> 'record_version' =
                            'null'::JSONB
                        AND COALESCE(
                            input.new_before_state::JSONB ->> 'record_version',
                            ''
                        ) ~ '^[0-9]+$'
                        AND (
                            input.new_before_state::JSONB ->> 'record_version'
                        )::BIGINT + 1 =
                            (input.after_state::JSONB ->> 'record_version')::BIGINT
                    )
                    OR (
                        input.new_before_state::JSONB -> 'record_version' =
                            'null'::JSONB
                        AND COALESCE(
                            input.old_before_state::JSONB ->> 'record_version',
                            ''
                        ) ~ '^[0-9]+$'
                        AND (
                            input.old_before_state::JSONB ->> 'record_version'
                        )::BIGINT + 1 =
                            (input.after_state::JSONB ->> 'record_version')::BIGINT
                    )
                )
            )
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                before_state = CASE
                    WHEN repair.event_kind = 'AuthorityTransferred'
                     AND jsonb_typeof(repair.old_before_state::JSONB -> 'owner') =
                         'string'
                     AND btrim(repair.old_before_state::JSONB ->> 'owner') <> ''
                     AND repair.new_before_state::JSONB -> 'owner' = 'null'::JSONB
                    THEN repair.old_before_state::JSONB
                    ELSE repair.new_before_state::JSONB
                END,
                observed_at = now()
            FROM repair_map repair
            WHERE event.event_identity = repair.event_identity
              AND event.resource_id = repair.resource_id
              AND event.event_kind = repair.event_kind
              AND event.before_state IS NOT DISTINCT FROM repair.old_before_state::JSONB
              AND event.after_state IS NOT DISTINCT FROM repair.after_state::JSONB
            RETURNING
                event.event_identity,
                event.normalized_event_id,
                event.canonicality_state,
                event.event_kind,
                event.resource_id
        ),
        queued_changes AS (
            INSERT INTO projection_normalized_event_changes (
                normalized_event_id,
                changed_at,
                change_kind,
                canonicality_state
            )
            SELECT
                normalized_event_id,
                now(),
                'canonicality_update',
                canonicality_state
            FROM updated
            RETURNING
                change_id,
                normalized_event_id,
                changed_at
        ),
        affected_resource_keys AS (
            SELECT
                'permissions_current'::TEXT AS projection,
                resource_id::TEXT AS projection_key,
                jsonb_build_object('resource_id', resource_id::TEXT) AS key_payload
            FROM updated
            WHERE event_kind = 'AuthorityTransferred'

            UNION ALL

            SELECT
                'record_inventory_current'::TEXT AS projection,
                resource_id::TEXT AS projection_key,
                jsonb_build_object('resource_id', resource_id::TEXT) AS key_payload
            FROM updated
            WHERE event_kind = 'RecordVersionChanged'
        ),
        queued_resource_invalidations AS (
            INSERT INTO projection_invalidations (
                projection,
                projection_key,
                key_payload,
                last_changed_at,
                invalidated_at
            )
            SELECT
                projection,
                projection_key,
                key_payload,
                now(),
                now()
            FROM affected_resource_keys
            WHERE projection_key IS NOT NULL
              AND btrim(projection_key) <> ''
            GROUP BY projection, projection_key, key_payload
            ON CONFLICT (projection, projection_key)
            DO UPDATE SET
                key_payload = EXCLUDED.key_payload,
                generation = projection_invalidations.generation + 1,
                last_changed_at = GREATEST(
                    projection_invalidations.last_changed_at,
                    EXCLUDED.last_changed_at
                ),
                invalidated_at = EXCLUDED.invalidated_at,
                claim_token = NULL,
                claimed_at = NULL,
                last_failure_reason = NULL,
                last_failure_at = NULL
            RETURNING projection_key
        )
        SELECT input.event_identity
        FROM input
        JOIN updated
          ON updated.event_identity = input.event_identity
