        WITH input AS (
            SELECT *
            FROM unnest(
                $1::TEXT[],
                $2::UUID[],
                $3::TEXT[],
                $4::TEXT[],
                $5::TEXT[],
                $6::TEXT[]
            ) AS input(
                event_identity,
                resource_id,
                logical_name_id,
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
             AND resource.provenance->>'authority_kind' = 'registrar'
             AND resource.provenance->>'logical_name_id' = input.logical_name_id
            WHERE input.old_before_state::JSONB - 'registrant' =
                  input.new_before_state::JSONB - 'registrant'
              AND input.old_before_state::JSONB ->> 'expiry' =
                  input.new_before_state::JSONB ->> 'expiry'
              AND COALESCE(input.old_before_state::JSONB ->> 'expiry', '') ~
                  '^-?[0-9]+$'
              AND COALESCE(input.old_before_state::JSONB ->> 'registrant', '') <> ''
              AND COALESCE(input.new_before_state::JSONB ->> 'registrant', '') <> ''
              AND input.after_state::JSONB ? 'released_at'
              AND input.after_state::JSONB ? 'labelhash'
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                before_state = repair.new_before_state::JSONB,
                observed_at = now()
            FROM repair_map repair
            WHERE event.event_identity = repair.event_identity
              AND event.resource_id = repair.resource_id
              AND event.logical_name_id = repair.logical_name_id
              AND event.event_kind = 'RegistrationReleased'
              AND event.source_family = 'ens_v1_registrar_l1'
              AND event.derivation_kind = 'ens_v1_unwrapped_authority'
              AND event.chain_id = 'ethereum-mainnet'
              AND event.before_state IS NOT DISTINCT FROM repair.old_before_state::JSONB
              AND event.after_state IS NOT DISTINCT FROM repair.after_state::JSONB
            RETURNING
                event.event_identity,
                event.normalized_event_id,
                event.canonicality_state
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
        )
        SELECT input.event_identity
        FROM input
        JOIN updated
          ON updated.event_identity = input.event_identity
