        WITH input AS (
            SELECT *
            FROM unnest(
                $1::TEXT[],
                $2::UUID[],
                $3::UUID[],
                $4::TEXT[],
                $5::BIGINT[],
                $6::TEXT[],
                $7::TEXT[],
                $8::BIGINT[],
                $9::TEXT[],
                $10::TEXT[],
                $11::TEXT[],
                $12::TEXT[],
                $13::TEXT[]
            ) AS input(
                event_identity,
                old_resource_id,
                new_resource_id,
                logical_name_id,
                block_number,
                block_hash,
                transaction_hash,
                log_index,
                event_kind,
                old_before_state,
                new_before_state,
                old_after_state,
                new_after_state
            )
        ),
        registration_input AS (
            SELECT *
            FROM unnest(
                $14::UUID[],
                $15::TEXT[],
                $16::TEXT[],
                $17::BIGINT[]
            ) AS registration(
                resource_id,
                block_hash,
                transaction_hash,
                log_index
            )
        ),
        repair_map AS (
            SELECT
                input.*,
                CASE
                    WHEN input.event_kind = 'AuthorityTransferred'
                     AND jsonb_typeof(input.old_before_state::JSONB -> 'owner') =
                         'string'
                     AND btrim(input.old_before_state::JSONB ->> 'owner') <> ''
                     AND input.new_before_state::JSONB -> 'owner' = 'null'::JSONB
                    THEN input.old_before_state::JSONB
                    ELSE input.new_before_state::JSONB
                END AS repaired_before_state,
                CASE
                    WHEN input.event_kind = 'RecordChanged'
                     AND input.old_before_state::JSONB IS NOT DISTINCT FROM
                         input.new_before_state::JSONB
                     AND input.old_after_state::JSONB - 'value' =
                         input.new_after_state::JSONB - 'value'
                     AND input.old_after_state::JSONB ->> 'record_family' = 'text'
                     AND input.new_after_state::JSONB ->> 'record_family' = 'text'
                     AND COALESCE(input.old_after_state::JSONB ->> 'record_key', '') LIKE
                         'text:%'
                     AND input.old_after_state::JSONB ->> 'record_key' =
                         input.new_after_state::JSONB ->> 'record_key'
                     AND COALESCE(input.old_after_state::JSONB ->> 'selector_key', '') <> ''
                     AND input.old_after_state::JSONB ->> 'selector_key' =
                         input.new_after_state::JSONB ->> 'selector_key'
                     AND (input.old_after_state::JSONB ? 'value')
                     AND NOT (input.new_after_state::JSONB ? 'value')
                    THEN input.old_after_state::JSONB
                    ELSE input.new_after_state::JSONB
                END AS repaired_after_state
            FROM input
            JOIN resources old_resource
              ON old_resource.resource_id = input.old_resource_id
             AND old_resource.chain_id = 'ethereum-mainnet'
             AND old_resource.canonicality_state IN (
                 'canonical'::canonicality_state,
                 'safe'::canonicality_state,
                 'finalized'::canonicality_state
             )
             AND old_resource.provenance->>'authority_kind' IN (
                 'registrar',
                 'wrapper',
                 'registry_only'
             )
             AND (
                 old_resource.provenance->>'logical_name_id' = input.logical_name_id
                 OR (
                     old_resource.provenance->>'authority_kind' = 'registry_only'
                     AND old_resource.provenance->>'logical_name_id' IS DISTINCT FROM
                         input.logical_name_id
                 )
             )
            LEFT JOIN resources new_resource
              ON new_resource.resource_id = input.new_resource_id
            WHERE (
                (
                    input.new_resource_id IS NULL
                    AND old_resource.provenance->>'authority_kind' IN ('registrar', 'wrapper')
                    AND old_resource.block_number > input.block_number
                )
                OR (
                    new_resource.resource_id IS NOT NULL
                    AND new_resource.resource_id <> old_resource.resource_id
                    AND new_resource.chain_id = 'ethereum-mainnet'
                    AND new_resource.canonicality_state IN (
                        'canonical'::canonicality_state,
                        'safe'::canonicality_state,
                        'finalized'::canonicality_state
                    )
                    AND new_resource.provenance->>'logical_name_id' = input.logical_name_id
                    AND (
                        lower(COALESCE(new_resource.provenance->>'labelhash', '')) =
                            lower(COALESCE(old_resource.provenance->>'labelhash', ''))
                        OR (
                            old_resource.provenance->>'authority_kind' = 'wrapper'
                            AND COALESCE(old_resource.provenance->>'labelhash', '') = ''
                            AND COALESCE(new_resource.provenance->>'labelhash', '') <> ''
                        )
                    )
                    AND (
                        (
                            new_resource.provenance->>'authority_kind' = 'registry_only'
                            AND new_resource.block_number <= input.block_number
                            AND (
                                (
                                    old_resource.provenance->>'authority_kind' IN ('registrar', 'wrapper')
                                    AND old_resource.block_number > input.block_number
                                )
                                OR (
                                    old_resource.provenance->>'authority_kind' = 'registry_only'
                                    AND old_resource.provenance->>'authority_key' IS DISTINCT FROM
                                        new_resource.provenance->>'authority_key'
                                )
                            )
                        )
                        OR (
                            new_resource.provenance->>'authority_kind' = 'registrar'
                            AND (
                                (
                                    old_resource.provenance->>'authority_kind' = 'registrar'
                                    AND new_resource.block_number <= input.block_number
                                )
                                OR (
                                    old_resource.provenance->>'authority_kind' = 'registry_only'
                                    AND input.block_hash <> ''
                                    AND input.transaction_hash <> ''
                                    AND input.log_index >= 0
                                    AND new_resource.block_number = input.block_number
                                    AND new_resource.block_hash = input.block_hash
                                    AND split_part(new_resource.provenance->>'authority_key', ':', 1) =
                                        'registrar'
                                    AND split_part(new_resource.provenance->>'authority_key', ':', 2) =
                                        'ethereum-mainnet'
                                    AND split_part(new_resource.provenance->>'authority_key', ':', 5) =
                                        input.block_hash
                                    AND split_part(new_resource.provenance->>'authority_key', ':', 6) ~
                                        '^[0-9]+$'
                                    AND (
                                        split_part(
                                            new_resource.provenance->>'authority_key',
                                            ':',
                                            6
                                        )::BIGINT
                                    ) > input.log_index
                                    AND EXISTS (
                                        SELECT 1
                                        FROM (
                                            SELECT
                                                event.resource_id,
                                                event.block_hash,
                                                event.transaction_hash,
                                                COALESCE(event.log_index, -1) AS log_index
                                            FROM normalized_events event
                                            WHERE event.resource_id = input.new_resource_id
                                              AND event.event_kind = 'RegistrationGranted'
                                              AND event.canonicality_state IN (
                                                  'canonical'::canonicality_state,
                                                  'safe'::canonicality_state,
                                                  'finalized'::canonicality_state
                                              )

                                            UNION ALL

                                            SELECT
                                                registration.resource_id,
                                                registration.block_hash,
                                                registration.transaction_hash,
                                                registration.log_index
                                            FROM registration_input registration
                                            WHERE registration.resource_id = input.new_resource_id
                                        ) registration
                                        WHERE registration.resource_id = input.new_resource_id
                                          AND registration.block_hash = input.block_hash
                                          AND registration.transaction_hash = input.transaction_hash
                                          AND registration.log_index > input.log_index
                                    )
                                )
                            )
                        )
                    )
                )
            )
              AND (
                 (
                     input.old_before_state::JSONB IS NOT DISTINCT FROM
                         input.new_before_state::JSONB
                     AND input.old_after_state::JSONB IS NOT DISTINCT FROM
                         input.new_after_state::JSONB
                 )
                 OR (
                     input.event_kind = 'AuthorityTransferred'
                     AND input.old_after_state::JSONB IS NOT DISTINCT FROM
                         input.new_after_state::JSONB
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
                     AND input.old_after_state::JSONB IS NOT DISTINCT FROM
                         input.new_after_state::JSONB
                     AND input.old_before_state::JSONB - 'record_version' =
                         input.new_before_state::JSONB - 'record_version'
                     AND COALESCE(input.new_after_state::JSONB ->> 'record_version', '') ~
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
                                 (
                                     input.new_after_state::JSONB ->> 'record_version'
                                 )::BIGINT
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
                                 (
                                     input.new_after_state::JSONB ->> 'record_version'
                                 )::BIGINT
                         )
                     )
                 )
                 OR (
                     input.event_kind = 'RecordChanged'
                     AND input.old_before_state::JSONB IS NOT DISTINCT FROM
                         input.new_before_state::JSONB
                     AND input.old_after_state::JSONB - 'value' =
                         input.new_after_state::JSONB - 'value'
                     AND input.old_after_state::JSONB ->> 'record_family' = 'text'
                     AND input.new_after_state::JSONB ->> 'record_family' = 'text'
                     AND COALESCE(input.old_after_state::JSONB ->> 'record_key', '') LIKE
                         'text:%'
                     AND input.old_after_state::JSONB ->> 'record_key' =
                         input.new_after_state::JSONB ->> 'record_key'
                     AND COALESCE(input.old_after_state::JSONB ->> 'selector_key', '') <> ''
                     AND input.old_after_state::JSONB ->> 'selector_key' =
                         input.new_after_state::JSONB ->> 'selector_key'
                     AND (
                         (
                             (input.old_after_state::JSONB ? 'value')
                             AND NOT (input.new_after_state::JSONB ? 'value')
                             AND jsonb_typeof(input.old_after_state::JSONB -> 'value') =
                                 'string'
                         )
                         OR (
                             NOT (input.old_after_state::JSONB ? 'value')
                             AND (input.new_after_state::JSONB ? 'value')
                             AND jsonb_typeof(input.new_after_state::JSONB -> 'value') =
                                 'string'
                         )
                     )
                 )
                 OR (
                     input.event_kind = 'PermissionChanged'
                     AND (
                         input.old_before_state::JSONB IS NOT DISTINCT FROM
                             input.new_before_state::JSONB
                         OR (
                             input.old_before_state::JSONB - 'grant_source' - 'revocation_source' =
                                 input.new_before_state::JSONB - 'grant_source' - 'revocation_source'
                             AND (
                                 input.old_before_state::JSONB -> 'grant_source' IS NOT DISTINCT FROM
                                     input.new_before_state::JSONB -> 'grant_source'
                                 OR (
                                     input.old_before_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_before_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_before_state::JSONB #>> '{grant_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_before_state::JSONB #>> '{grant_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_before_state::JSONB #>> '{grant_source,authority_kind}' =
                                         new_resource.provenance->>'authority_kind'
                                     AND input.new_before_state::JSONB #>> '{grant_source,authority_key}' =
                                         new_resource.provenance->>'authority_key'
                                     AND COALESCE(
                                         input.old_before_state::JSONB #>> '{grant_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_before_state::JSONB #>> '{grant_source,source_event_kind}' =
                                         input.new_before_state::JSONB #>> '{grant_source,source_event_kind}'
                                 )
                             )
                             AND (
                                 input.old_before_state::JSONB -> 'revocation_source' IS NOT DISTINCT FROM
                                     input.new_before_state::JSONB -> 'revocation_source'
                                 OR (
                                     input.old_before_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_before_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_before_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_before_state::JSONB #>> '{revocation_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_before_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         new_resource.provenance->>'authority_kind'
                                     AND input.new_before_state::JSONB #>> '{revocation_source,authority_key}' =
                                         new_resource.provenance->>'authority_key'
                                     AND COALESCE(
                                         input.old_before_state::JSONB #>> '{revocation_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_before_state::JSONB #>> '{revocation_source,source_event_kind}' =
                                         input.new_before_state::JSONB #>> '{revocation_source,source_event_kind}'
                                 )
                             )
                         )
                     )
                     AND (
                         input.old_after_state::JSONB IS NOT DISTINCT FROM
                             input.new_after_state::JSONB
                         OR (
                             input.old_after_state::JSONB - 'grant_source' - 'revocation_source' =
                                 input.new_after_state::JSONB - 'grant_source' - 'revocation_source'
                             AND (
                                 input.old_after_state::JSONB -> 'grant_source' IS NOT DISTINCT FROM
                                     input.new_after_state::JSONB -> 'grant_source'
                                 OR (
                                     input.old_after_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_after_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_after_state::JSONB #>> '{grant_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_after_state::JSONB #>> '{grant_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_after_state::JSONB #>> '{grant_source,authority_kind}' =
                                         new_resource.provenance->>'authority_kind'
                                     AND input.new_after_state::JSONB #>> '{grant_source,authority_key}' =
                                         new_resource.provenance->>'authority_key'
                                     AND COALESCE(
                                         input.old_after_state::JSONB #>> '{grant_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_after_state::JSONB #>> '{grant_source,source_event_kind}' =
                                         input.new_after_state::JSONB #>> '{grant_source,source_event_kind}'
                                 )
                             )
                             AND (
                                 input.old_after_state::JSONB -> 'revocation_source' IS NOT DISTINCT FROM
                                     input.new_after_state::JSONB -> 'revocation_source'
                                 OR (
                                     input.old_after_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_after_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_after_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_after_state::JSONB #>> '{revocation_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_after_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         new_resource.provenance->>'authority_kind'
                                     AND input.new_after_state::JSONB #>> '{revocation_source,authority_key}' =
                                         new_resource.provenance->>'authority_key'
                                     AND COALESCE(
                                         input.old_after_state::JSONB #>> '{revocation_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_after_state::JSONB #>> '{revocation_source,source_event_kind}' =
                                         input.new_after_state::JSONB #>> '{revocation_source,source_event_kind}'
                                 )
                             )
                         )
                     )
                 )
                 OR (
                     new_resource.resource_id IS NULL
                     AND input.event_kind = 'PermissionChanged'
                     AND (
                         input.old_before_state::JSONB IS NOT DISTINCT FROM
                             input.new_before_state::JSONB
                         OR (
                             input.old_before_state::JSONB - 'grant_source' - 'revocation_source' =
                                 input.new_before_state::JSONB - 'grant_source' - 'revocation_source'
                             AND (
                                 input.old_before_state::JSONB -> 'grant_source' IS NOT DISTINCT FROM
                                     input.new_before_state::JSONB -> 'grant_source'
                                 OR (
                                     input.old_before_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_before_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_before_state::JSONB #>> '{grant_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_before_state::JSONB #>> '{grant_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_before_state::JSONB #>> '{grant_source,authority_kind}' =
                                         'registry_only'
                                     AND input.new_before_state::JSONB #>> '{grant_source,authority_key}' LIKE
                                         'registry-only:ethereum-mainnet:%'
                                     AND COALESCE(
                                         input.old_before_state::JSONB #>> '{grant_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_before_state::JSONB #>> '{grant_source,source_event_kind}' =
                                         input.new_before_state::JSONB #>> '{grant_source,source_event_kind}'
                                 )
                             )
                             AND (
                                 input.old_before_state::JSONB -> 'revocation_source' IS NOT DISTINCT FROM
                                     input.new_before_state::JSONB -> 'revocation_source'
                                 OR (
                                     input.old_before_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_before_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_before_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_before_state::JSONB #>> '{revocation_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_before_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         'registry_only'
                                     AND input.new_before_state::JSONB #>> '{revocation_source,authority_key}' LIKE
                                         'registry-only:ethereum-mainnet:%'
                                     AND COALESCE(
                                         input.old_before_state::JSONB #>> '{revocation_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_before_state::JSONB #>> '{revocation_source,source_event_kind}' =
                                         input.new_before_state::JSONB #>> '{revocation_source,source_event_kind}'
                                 )
                             )
                         )
                     )
                     AND (
                         input.old_after_state::JSONB IS NOT DISTINCT FROM
                             input.new_after_state::JSONB
                         OR (
                             input.old_after_state::JSONB - 'grant_source' - 'revocation_source' =
                                 input.new_after_state::JSONB - 'grant_source' - 'revocation_source'
                             AND (
                                 input.old_after_state::JSONB -> 'grant_source' IS NOT DISTINCT FROM
                                     input.new_after_state::JSONB -> 'grant_source'
                                 OR (
                                     input.old_after_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_after_state::JSONB #>> '{grant_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_after_state::JSONB #>> '{grant_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_after_state::JSONB #>> '{grant_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_after_state::JSONB #>> '{grant_source,authority_kind}' =
                                         'registry_only'
                                     AND input.new_after_state::JSONB #>> '{grant_source,authority_key}' LIKE
                                         'registry-only:ethereum-mainnet:%'
                                     AND COALESCE(
                                         input.old_after_state::JSONB #>> '{grant_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_after_state::JSONB #>> '{grant_source,source_event_kind}' =
                                         input.new_after_state::JSONB #>> '{grant_source,source_event_kind}'
                                 )
                             )
                             AND (
                                 input.old_after_state::JSONB -> 'revocation_source' IS NOT DISTINCT FROM
                                     input.new_after_state::JSONB -> 'revocation_source'
                                 OR (
                                     input.old_after_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.new_after_state::JSONB #>> '{revocation_source,kind}' =
                                         'ens_v1_authority'
                                     AND input.old_after_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         old_resource.provenance->>'authority_kind'
                                     AND input.old_after_state::JSONB #>> '{revocation_source,authority_key}' =
                                         old_resource.provenance->>'authority_key'
                                     AND input.new_after_state::JSONB #>> '{revocation_source,authority_kind}' =
                                         'registry_only'
                                     AND input.new_after_state::JSONB #>> '{revocation_source,authority_key}' LIKE
                                         'registry-only:ethereum-mainnet:%'
                                     AND COALESCE(
                                         input.old_after_state::JSONB #>> '{revocation_source,source_event_kind}',
                                         ''
                                     ) <> ''
                                     AND input.old_after_state::JSONB #>> '{revocation_source,source_event_kind}' =
                                         input.new_after_state::JSONB #>> '{revocation_source,source_event_kind}'
                                 )
                             )
                         )
                     )
                 )
             )
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                resource_id = repair.new_resource_id,
                before_state = repair.repaired_before_state,
                after_state = repair.repaired_after_state,
                observed_at = now()
            FROM repair_map repair
            WHERE event.event_identity = repair.event_identity
              AND event.resource_id = repair.old_resource_id
              AND event.before_state IS NOT DISTINCT FROM repair.old_before_state::JSONB
              AND event.after_state IS NOT DISTINCT FROM repair.old_after_state::JSONB
            RETURNING
                event.event_identity,
                event.normalized_event_id,
                event.canonicality_state,
                event.event_kind,
                repair.old_resource_id,
                repair.new_resource_id
        ),
        already_repaired AS (
            SELECT event.event_identity
            FROM input
            JOIN normalized_events event
              ON event.event_identity = input.event_identity
            WHERE event.event_kind = input.event_kind
              AND event.resource_id IS NOT DISTINCT FROM input.new_resource_id
              AND event.before_state IS NOT DISTINCT FROM (
                  CASE
                      WHEN input.event_kind = 'AuthorityTransferred'
                       AND jsonb_typeof(input.old_before_state::JSONB -> 'owner') =
                           'string'
                       AND btrim(input.old_before_state::JSONB ->> 'owner') <> ''
                       AND input.new_before_state::JSONB -> 'owner' = 'null'::JSONB
                      THEN input.old_before_state::JSONB
                      ELSE input.new_before_state::JSONB
                  END
              )
              AND event.after_state IS NOT DISTINCT FROM (
                  CASE
                      WHEN input.event_kind = 'RecordChanged'
                       AND input.old_before_state::JSONB IS NOT DISTINCT FROM
                           input.new_before_state::JSONB
                       AND input.old_after_state::JSONB - 'value' =
                           input.new_after_state::JSONB - 'value'
                       AND input.old_after_state::JSONB ->> 'record_family' = 'text'
                       AND input.new_after_state::JSONB ->> 'record_family' = 'text'
                       AND COALESCE(input.old_after_state::JSONB ->> 'record_key', '') LIKE
                           'text:%'
                       AND input.old_after_state::JSONB ->> 'record_key' =
                           input.new_after_state::JSONB ->> 'record_key'
                       AND COALESCE(input.old_after_state::JSONB ->> 'selector_key', '') <> ''
                       AND input.old_after_state::JSONB ->> 'selector_key' =
                           input.new_after_state::JSONB ->> 'selector_key'
                       AND (input.old_after_state::JSONB ? 'value')
                       AND NOT (input.new_after_state::JSONB ? 'value')
                      THEN input.old_after_state::JSONB
                      ELSE input.new_after_state::JSONB
                  END
              )
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
            CROSS JOIN LATERAL (
                VALUES (old_resource_id), (new_resource_id)
            ) AS resource(resource_id)
            WHERE event_kind IN ('AuthorityTransferred', 'PermissionChanged')

            UNION ALL

            SELECT
                'record_inventory_current'::TEXT AS projection,
                resource_id::TEXT AS projection_key,
                jsonb_build_object('resource_id', resource_id::TEXT) AS key_payload
            FROM updated
            CROSS JOIN LATERAL (
                VALUES (old_resource_id), (new_resource_id)
            ) AS resource(resource_id)
            WHERE event_kind IN (
                'ResolverChanged',
                'RecordChanged',
                'RecordVersionChanged'
            )
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
        WHERE EXISTS (
            SELECT 1
            FROM updated
            WHERE updated.event_identity = input.event_identity
        )
        OR EXISTS (
            SELECT 1
            FROM already_repaired
            WHERE already_repaired.event_identity = input.event_identity
        )
