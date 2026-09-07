-- Preserve exact nonempty absence while retaining the existing function privileges.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.resolution_divergences') IS NULL THEN
        RETURN;
    END IF;
    EXECUTE $definition$
CREATE OR REPLACE FUNCTION bigname_phase.write_resolution_divergence(
    compared_resource_id uuid,
    compared_boundary_key text,
    compared_row_xmin text,
    requested_authoritative_chain_id text,
    requested_authoritative_block_number bigint,
    requested_authoritative_block_hash text,
    compared_execution_authority jsonb,
    requested_logical_name_id text,
    requested_resolver_chain_id text,
    requested_resolver_address text,
    requested_record_key text,
    compared_positions jsonb,
    live_answer jsonb,
    used_ccip_read boolean
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, bigname_phase, pg_temp
AS $$
DECLARE
    guard_status text;
    resolver_path jsonb;
    compared_entries jsonb;
    compared_provenance jsonb;
    compared_support_status text;
    selector_family text;
    selector_key text;
    indexed_entry jsonb;
    default_entry jsonb;
    indexed_status text;
    indexed_value jsonb;
    indexed_answer jsonb;
BEGIN
    IF used_ccip_read THEN
        RETURN 'ccip_skipped';
    END IF;

    IF compared_execution_authority ->> 'logical_name_id'
        IS DISTINCT FROM requested_logical_name_id
    THEN
        RETURN 'guard_rejected';
    END IF;

    guard_status := revalidate_resolution_lookup_state(
        requested_authoritative_chain_id,
        requested_authoritative_block_number,
        requested_authoritative_block_hash,
        compared_positions,
        compared_execution_authority,
        compared_resource_id,
        compared_boundary_key,
        compared_row_xmin
    );

    IF guard_status <> 'unchanged' THEN
        RETURN 'guard_rejected';
    END IF;

    CASE
        WHEN requested_record_key = 'avatar' THEN
            selector_family := 'avatar';
            selector_key := NULL;
        WHEN requested_record_key = 'contenthash' THEN
            selector_family := 'contenthash';
            selector_key := NULL;
        WHEN requested_record_key LIKE 'text:%'
            AND length(substr(requested_record_key, 6)) > 0
        THEN
            selector_family := 'text';
            selector_key := substr(requested_record_key, 6);
        WHEN requested_record_key ~ '^addr:(0|[1-9][0-9]*)$' THEN
            BEGIN
                selector_key := substr(requested_record_key, 6);
                IF selector_key::numeric > 18446744073709551615::numeric THEN
                    RETURN 'guard_rejected';
                END IF;
                selector_family := 'addr';
            EXCEPTION
                WHEN data_exception THEN
                    RETURN 'guard_rejected';
            END;
        ELSE
            RETURN 'guard_rejected';
    END CASE;

    SELECT inventory.entries,
           inventory.provenance,
           inventory.support_status,
           name.declared_summary #> '{topology,resolver_path}'
    INTO compared_entries, compared_provenance, compared_support_status, resolver_path
    FROM record_inventory_current AS inventory
    JOIN name_current AS name
      ON name.logical_name_id = requested_logical_name_id
     AND name.support_status = 'supported'
     AND name.declared_summary
            #> '{topology,version_boundaries,record_version_boundary}' =
         inventory.record_version_boundary
    WHERE inventory.resource_id = compared_resource_id
      AND inventory.record_version_boundary_key = compared_boundary_key
      AND inventory.xmin::text = compared_row_xmin
    FOR SHARE OF inventory, name;

    IF NOT FOUND
        OR jsonb_typeof(resolver_path) IS DISTINCT FROM 'array'
        OR jsonb_array_length(resolver_path) = 0
        OR resolver_path -> (jsonb_array_length(resolver_path) - 1)
                ->> 'chain_id' <> requested_resolver_chain_id
        OR lower(
            resolver_path -> (jsonb_array_length(resolver_path) - 1)
                ->> 'address'
        ) <> lower(requested_resolver_address)
    THEN
        RETURN 'guard_rejected';
    END IF;

    SELECT candidate.entry
    INTO indexed_entry
    FROM jsonb_array_elements(compared_entries)
        WITH ORDINALITY AS candidate(entry, ordinal)
    WHERE candidate.entry ->> 'record_key' = requested_record_key
       OR (
            candidate.entry ->> 'record_family' = selector_family
            AND (candidate.entry ->> 'selector_key')
                IS NOT DISTINCT FROM selector_key
       )
       OR (
            requested_record_key = 'avatar'
            AND candidate.entry ->> 'record_key' = 'text:avatar'
       )
    ORDER BY CASE
        WHEN candidate.entry ->> 'record_key' = 'text:avatar'
            AND requested_record_key = 'avatar'
        THEN 1
        ELSE 0
    END,
    candidate.ordinal
    LIMIT 1;

    IF (indexed_entry IS NULL OR indexed_entry ->> 'status' = 'not_found')
       AND NOT COALESCE(
           requested_record_key = 'addr:60'
           AND indexed_entry ->> 'status' = 'not_found'
           AND jsonb_typeof(compared_provenance -> 'exact_nonempty_not_found_record_keys') = 'array'
           AND compared_provenance -> 'exact_nonempty_not_found_record_keys'
               @> jsonb_build_array(requested_record_key),
           false
       )
       AND selector_family = 'addr'
       AND (
           selector_key = '60'
           OR selector_key::numeric BETWEEN 2147483649::numeric AND 4294967295::numeric
       )
       AND EXISTS (
           SELECT 1
           FROM jsonb_array_elements(COALESCE(
               compared_provenance -> 'read_rules', '[]'::jsonb
           )) rule
           WHERE rule ->> 'kind' = 'ensip19_default_address'
             AND rule ->> 'source_record_key' = 'addr:2147483648'
       )
    THEN
        IF compared_support_status <> 'supported' THEN
            indexed_entry := jsonb_build_object('status', 'unsupported');
        ELSE
            SELECT candidate.entry
            INTO default_entry
            FROM jsonb_array_elements(compared_entries)
                WITH ORDINALITY AS candidate(entry, ordinal)
            WHERE candidate.entry ->> 'record_key' = 'addr:2147483648'
               OR (
                    candidate.entry ->> 'record_family' = 'addr'
                    AND candidate.entry ->> 'selector_key' = '2147483648'
               )
            ORDER BY candidate.ordinal
            LIMIT 1;

            IF default_entry IS NULL THEN
                indexed_entry := jsonb_build_object('status', 'not_found');
            ELSIF default_entry ->> 'status' IN ('success', 'not_found') THEN
                -- Match the requested getter's verified decode. addr(bytes32) converts
                -- the coin-60 bytes to address(0); multicoin addr(bytes32,uint256)
                -- preserves non-empty bytes, including 20 zero bytes.
                -- (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)
                -- (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L685-L697 @ ens_v2_sepolia_20260629@ccaeb58)
                IF selector_key = '60'
                   AND default_entry ->> 'status' = 'success'
                   AND lower(COALESCE(
                       default_entry #>> '{value,value}',
                       default_entry #>> '{value,bytes}',
                       default_entry ->> 'value'
                   )) = '0x0000000000000000000000000000000000000000'
                THEN
                    indexed_entry := jsonb_build_object('status', 'not_found');
                ELSE
                    indexed_entry := default_entry;
                END IF;
            ELSE
                indexed_entry := jsonb_build_object('status', 'unsupported');
            END IF;
        END IF;
    ELSIF (indexed_entry IS NULL OR indexed_entry ->> 'status' = 'not_found')
          AND compared_support_status <> 'supported'
    THEN
        indexed_entry := jsonb_build_object('status', 'unsupported');
    END IF;

    IF indexed_entry IS NULL THEN
        indexed_answer := jsonb_build_object('status', 'not_found');
    ELSE
        indexed_status := CASE COALESCE(
            indexed_entry ->> 'status',
            'unsupported'
        )
            WHEN 'failed' THEN 'execution_failed'
            ELSE COALESCE(indexed_entry ->> 'status', 'unsupported')
        END;
        indexed_answer := jsonb_build_object('status', indexed_status);
        IF indexed_status = 'success' THEN
            indexed_value := COALESCE(
                indexed_entry #> '{value,value}',
                indexed_entry #> '{value,bytes}',
                indexed_entry -> 'value'
            );
            IF jsonb_typeof(indexed_value) = 'string' THEN
                indexed_answer := indexed_answer || jsonb_build_object(
                    'value',
                    CASE
                        WHEN selector_family = 'addr'
                            THEN lower(indexed_value #>> '{}')
                        ELSE indexed_value #>> '{}'
                    END
                );
            ELSE
                indexed_answer := jsonb_build_object('status', 'unsupported');
            END IF;
        END IF;
    END IF;

    IF indexed_answer = live_answer THEN
        UPDATE resolution_divergences
        SET cleared_at = GREATEST(statement_timestamp(), last_observed_at)
        WHERE logical_name_id = requested_logical_name_id
          AND resolver_chain_id = requested_resolver_chain_id
          AND lower(resolver_address) = lower(requested_resolver_address)
          AND request_kind_hash =
              public.digest(requested_record_key, 'sha256')
          AND request_kind = requested_record_key
          AND cleared_at IS NULL;
        IF FOUND THEN
            RETURN 'cleared';
        END IF;
        RETURN 'agreement';
    END IF;

    UPDATE resolution_divergences
    SET cleared_at = GREATEST(statement_timestamp(), last_observed_at)
    WHERE logical_name_id = requested_logical_name_id
      AND resolver_chain_id = requested_resolver_chain_id
      AND lower(resolver_address) = lower(requested_resolver_address)
      AND request_kind_hash =
          public.digest(requested_record_key, 'sha256')
      AND request_kind = requested_record_key
      AND observed_positions <> compared_positions
      AND cleared_at IS NULL;

    IF EXISTS (
        SELECT 1
        FROM resolution_divergences
        WHERE logical_name_id = requested_logical_name_id
          AND resolver_chain_id = requested_resolver_chain_id
          AND lower(resolver_address) = lower(requested_resolver_address)
          AND request_kind_hash =
              public.digest(requested_record_key, 'sha256')
          AND request_kind <> requested_record_key
    ) THEN
        RAISE EXCEPTION 'resolution divergence request-key hash collision'
            USING ERRCODE = '23514';
    END IF;

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    ) VALUES (
        requested_logical_name_id,
        requested_resolver_chain_id,
        lower(requested_resolver_address),
        requested_record_key,
        compared_positions,
        indexed_answer,
        live_answer
    )
    ON CONFLICT ON CONSTRAINT resolution_divergences_pkey DO UPDATE
    SET indexed_result = EXCLUDED.indexed_result,
        live_result = EXCLUDED.live_result,
        last_observed_at = GREATEST(
            resolution_divergences.last_observed_at,
            statement_timestamp()
        ),
        cleared_at = NULL
    WHERE resolution_divergences.request_kind = EXCLUDED.request_kind;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'resolution divergence request-key hash collision'
            USING ERRCODE = '23514';
    END IF;

    RETURN 'written';
END
$$;
$definition$;
END
$migration$;
