CREATE TABLE IF NOT EXISTS resolution_divergences (
    logical_name_id text NOT NULL
        REFERENCES name_surfaces (logical_name_id),
    resolver_chain_id text NOT NULL,
    resolver_address text NOT NULL,
    request_kind text NOT NULL,
    request_kind_hash bytea GENERATED ALWAYS AS (
        public.digest(request_kind, 'sha256')
    ) STORED,
    observed_positions jsonb NOT NULL,
    indexed_result jsonb NOT NULL,
    live_result jsonb NOT NULL,
    first_observed_at timestamptz NOT NULL DEFAULT now(),
    last_observed_at timestamptz NOT NULL DEFAULT now(),
    cleared_at timestamptz,
    PRIMARY KEY (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind_hash,
        observed_positions
    ),
    CHECK (btrim(resolver_chain_id) <> ''),
    CHECK (btrim(resolver_address) <> ''),
    CHECK (btrim(request_kind) <> ''),
    CHECK (jsonb_typeof(observed_positions) = 'object'),
    CHECK (observed_positions <> '{}'::jsonb),
    CHECK (indexed_result <> live_result),
    CHECK (last_observed_at >= first_observed_at),
    CONSTRAINT resolution_divergences_clearing_time_check
        CHECK (cleared_at IS NULL OR cleared_at >= last_observed_at)
);

CREATE INDEX IF NOT EXISTS resolution_divergences_active_name_idx
    ON resolution_divergences (logical_name_id, last_observed_at DESC)
    WHERE cleared_at IS NULL;

CREATE INDEX IF NOT EXISTS resolution_divergences_active_resolver_idx
    ON resolution_divergences (
        resolver_chain_id,
        lower(resolver_address),
        last_observed_at DESC
    )
    WHERE cleared_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS resolution_divergences_one_active_request_idx
    ON resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        lower(resolver_address),
        request_kind_hash
    )
    WHERE cleared_at IS NULL;

COMMENT ON INDEX resolution_divergences_one_active_request_idx IS
    'This bounded btree uses SHA-256 of the unbounded request key; writes retain and compare the original key so a digest collision fails closed.';

-- Keep serving-path row locks behind a narrow privilege boundary. The API role
-- receives EXECUTE on this function, not UPDATE on the guarded projection and
-- head tables. Both locks remain held by the caller's transaction.
CREATE OR REPLACE FUNCTION revalidate_resolution_lookup_state(
    requested_authoritative_chain_id text,
    requested_authoritative_block_number bigint,
    requested_authoritative_block_hash text,
    requested_observed_positions jsonb,
    compared_execution_authority jsonb,
    compared_resource_id uuid,
    compared_boundary_key text,
    compared_row_xmin text
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, bigname_phase, pg_temp
AS $$
DECLARE
    position_slot text;
    position_value jsonb;
    manifest_authority jsonb;
    compared_project_row_xmin text;
    compared_logical_name_id text;
    compared_name_row_xmin text;
BEGIN
    -- Keep this key aligned with SCHEMA_V2_MANIFEST_SYNC_LOCK in
    -- crates/manifests/src/schema_v2.rs. A shared transaction lock makes the
    -- captured active-or-shadow manifest selection stable through commit.
    PERFORM pg_advisory_xact_lock_shared(4776427281231725874);

    PERFORM 1
    FROM chain_heads
    WHERE chain_id = requested_authoritative_chain_id
      AND latest_block_number = requested_authoritative_block_number
      AND latest_block_hash = requested_authoritative_block_hash
    FOR SHARE;

    IF NOT FOUND THEN
        RETURN 'head_changed';
    END IF;

    IF jsonb_typeof(compared_execution_authority) IS DISTINCT FROM 'object'
    THEN
        RETURN 'invalid_comparison';
    END IF;

    compared_project_row_xmin :=
        compared_execution_authority ->> 'project_row_xmin';
    compared_logical_name_id :=
        compared_execution_authority ->> 'logical_name_id';
    compared_name_row_xmin :=
        compared_execution_authority ->> 'name_row_xmin';

    IF compared_project_row_xmin IS NULL
        OR btrim(compared_project_row_xmin) = ''
    THEN
        RETURN 'invalid_comparison';
    END IF;

    -- This lock is the projection-publication generation fence. Phase
    -- transitions change this row before a new projected generation is
    -- admitted. The advisory lock above separately fences manifest sync,
    -- including changes to admitted shadow execution declarations.
    PERFORM 1
    FROM chain_phase_state
    WHERE chain_id = requested_authoritative_chain_id
      AND phase_name = 'project'
      AND phase_status = 'completed'
      AND current_block_number = requested_authoritative_block_number
      AND current_block_hash = requested_authoritative_block_hash
      AND xmin::text = compared_project_row_xmin
    FOR SHARE;

    IF NOT FOUND THEN
        RETURN 'project_changed';
    END IF;

    IF jsonb_typeof(requested_observed_positions) IS DISTINCT FROM 'object'
        OR requested_observed_positions = '{}'::jsonb
    THEN
        RETURN 'position_changed';
    END IF;

    FOR position_slot, position_value IN
        SELECT key, value
        FROM jsonb_each(requested_observed_positions)
        ORDER BY key
    LOOP
        BEGIN
            PERFORM 1
            FROM chain_lineage
            WHERE chain_id = position_value ->> 'chain_id'
              AND block_hash = position_value ->> 'block_hash'
              AND block_number =
                  (position_value ->> 'block_number')::bigint
              AND block_timestamp =
                  (position_value ->> 'timestamp')::timestamptz
              AND canonicality_state IN (
                  'canonical',
                  'safe',
                  'finalized'
              )
            FOR SHARE;
        EXCEPTION
            WHEN data_exception THEN
                RETURN 'position_changed';
        END;

        IF NOT FOUND THEN
            RETURN 'position_changed';
        END IF;
    END LOOP;

    -- Match project publication order: name_current is locked before
    -- record_inventory_current. This prevents serving-path writes from
    -- deadlocking with a same-height projection swap.
    IF compared_logical_name_id IS NULL
        AND compared_name_row_xmin IS NULL
    THEN
        NULL;
    ELSIF compared_logical_name_id IS NULL
        OR compared_name_row_xmin IS NULL
    THEN
        RETURN 'invalid_comparison';
    ELSE
        PERFORM 1
        FROM name_current
        WHERE logical_name_id = compared_logical_name_id
          AND support_status = 'supported'
          AND xmin::text = compared_name_row_xmin
        FOR SHARE;

        IF NOT FOUND THEN
            RETURN 'name_changed';
        END IF;
    END IF;

    IF jsonb_typeof(
        compared_execution_authority -> 'manifest_authorities'
    ) IS DISTINCT FROM 'array'
        OR jsonb_array_length(
            compared_execution_authority -> 'manifest_authorities'
        ) = 0
    THEN
        RETURN 'invalid_comparison';
    END IF;

    FOR manifest_authority IN
        SELECT value
        FROM jsonb_array_elements(
            compared_execution_authority -> 'manifest_authorities'
        )
    LOOP
        PERFORM 1
        FROM manifest_versions AS manifest
        JOIN manifest_contract_instances AS declaration
          ON declaration.manifest_id = manifest.manifest_id
         AND declaration.chain_id = manifest.chain_id
        WHERE manifest.manifest_id::text =
                  manifest_authority ->> 'manifest_id'
          AND manifest.xmin::text =
                  manifest_authority ->> 'manifest_row_xmin'
          AND declaration.manifest_contract_instance_id::text =
                  manifest_authority ->> 'declaration_id'
          AND declaration.xmin::text =
                  manifest_authority ->> 'declaration_row_xmin'
          AND lower(declaration.declared_address) = lower(
                  manifest_authority ->> 'declared_address'
              );

        IF NOT FOUND THEN
            RETURN 'manifest_changed';
        END IF;
    END LOOP;

    IF compared_resource_id IS NULL
        AND compared_boundary_key IS NULL
        AND compared_row_xmin IS NULL
    THEN
        RETURN 'unchanged';
    END IF;

    IF compared_resource_id IS NULL
        OR compared_boundary_key IS NULL
        OR compared_row_xmin IS NULL
    THEN
        RETURN 'invalid_comparison';
    END IF;

    PERFORM 1
    FROM record_inventory_current
    WHERE resource_id = compared_resource_id
      AND record_version_boundary_key = compared_boundary_key
      AND xmin::text = compared_row_xmin
    FOR SHARE;

    IF NOT FOUND THEN
        RETURN 'record_changed';
    END IF;

    RETURN 'unchanged';
END
$$;

REVOKE ALL ON FUNCTION revalidate_resolution_lookup_state(
    text, bigint, text, jsonb, jsonb, uuid, text, text
) FROM PUBLIC;

-- Authorized by simplification-build-plan-20260730.md § B6, lines 100-104.
-- The compared projection row is locked until the caller's transaction ends.
CREATE OR REPLACE FUNCTION write_resolution_divergence(
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

REVOKE ALL ON FUNCTION write_resolution_divergence(
    uuid, text, text, text, bigint, text, jsonb, text, text, text,
    text, jsonb, jsonb, boolean
) FROM PUBLIC;

CREATE OR REPLACE FUNCTION validate_resolution_divergence_positions()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    position_slot text;
    position_value jsonb;
    position_chain_id text;
    position_block_hash text;
    position_block_number bigint;
    position_timestamp timestamptz;
BEGIN
    IF jsonb_typeof(NEW.observed_positions) <> 'object'
        OR NEW.observed_positions = '{}'::jsonb
    THEN
        RAISE EXCEPTION
            'observed positions must be a nonempty ChainPositions object'
            USING ERRCODE = '23514';
    END IF;

    FOR position_slot, position_value IN
        SELECT key, value
        FROM jsonb_each(NEW.observed_positions)
    LOOP
        IF btrim(position_slot) = ''
            OR jsonb_typeof(position_value)
                IS DISTINCT FROM 'object'
            OR jsonb_typeof(position_value -> 'chain_id')
                IS DISTINCT FROM 'string'
            OR jsonb_typeof(position_value -> 'block_hash')
                IS DISTINCT FROM 'string'
            OR jsonb_typeof(position_value -> 'block_number')
                IS DISTINCT FROM 'number'
            OR jsonb_typeof(position_value -> 'timestamp')
                IS DISTINCT FROM 'string'
            OR btrim(position_value ->> 'chain_id') = ''
            OR btrim(position_value ->> 'block_hash') = ''
            OR btrim(position_value ->> 'timestamp') = ''
            OR (position_value ->> 'block_number') !~ '^[0-9]+$'
        THEN
            RAISE EXCEPTION
                'observed position % is not a valid ChainPosition',
                position_slot
                USING ERRCODE = '23514';
        END IF;

        BEGIN
            position_chain_id := position_value ->> 'chain_id';
            position_block_hash := position_value ->> 'block_hash';
            position_block_number :=
                (position_value ->> 'block_number')::bigint;
            position_timestamp :=
                (position_value ->> 'timestamp')::timestamptz;
        EXCEPTION
            WHEN data_exception THEN
                RAISE EXCEPTION
                    'observed position % is not a valid ChainPosition',
                    position_slot
                    USING ERRCODE = '23514';
        END;

        IF NEW.cleared_at IS NULL THEN
            PERFORM 1
            FROM chain_lineage
            WHERE chain_id = position_chain_id
              AND block_hash = position_block_hash
              AND block_number = position_block_number
              AND block_timestamp = position_timestamp
              AND canonicality_state IN (
                  'canonical',
                  'safe',
                  'finalized'
              )
            FOR SHARE;

            IF NOT FOUND THEN
                RAISE EXCEPTION
                    'active resolution difference position % is not canonical',
                    position_slot
                    USING ERRCODE = '23503';
            END IF;
        END IF;
    END LOOP;

    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS resolution_divergences_validate_positions
    ON resolution_divergences;
CREATE TRIGGER resolution_divergences_validate_positions
BEFORE INSERT OR UPDATE ON resolution_divergences
FOR EACH ROW
EXECUTE FUNCTION validate_resolution_divergence_positions();

CREATE OR REPLACE FUNCTION retire_direct_divergences_for_null_resolver()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, bigname_phase, pg_temp
AS $$
DECLARE
    surface_on_ethereum_mainnet boolean;
BEGIN
    -- Rust lookup admission excludes resolver.status = 'unsupported', but
    -- retirement intentionally does not: once the exact resolver is null,
    -- prior direct-resolver observations are stale regardless of that status.
    IF NEW.namespace = 'ens'
        AND NEW.declared_summary -> 'resolver' ? 'chain_id'
        AND NEW.declared_summary -> 'resolver' ? 'address'
        AND NEW.declared_summary -> 'resolver' -> 'chain_id' = 'null'::jsonb
        AND NEW.declared_summary -> 'resolver' -> 'address' = 'null'::jsonb
    THEN
        EXECUTE format(
            'SELECT EXISTS (
                SELECT 1 FROM %I.name_surfaces
                WHERE logical_name_id = $1
                  AND chain_id = ''ethereum-mainnet''
            )',
            TG_TABLE_SCHEMA
        )
        INTO surface_on_ethereum_mainnet
        USING NEW.logical_name_id;

        IF surface_on_ethereum_mainnet THEN
            EXECUTE format(
                'UPDATE %I.resolution_divergences
                 SET cleared_at = GREATEST(statement_timestamp(), last_observed_at)
                 WHERE logical_name_id = $1
                   AND resolver_chain_id = ''ethereum-mainnet''
                   AND cleared_at IS NULL',
                TG_TABLE_SCHEMA
            )
            USING NEW.logical_name_id;
        END IF;
    END IF;
    RETURN NEW;
END
$$;

REVOKE ALL ON FUNCTION retire_direct_divergences_for_null_resolver()
    FROM PUBLIC;

DROP TRIGGER IF EXISTS name_current_retire_null_resolver_divergences
    ON name_current;
CREATE TRIGGER name_current_retire_null_resolver_divergences
AFTER INSERT OR UPDATE OF declared_summary ON name_current
FOR EACH ROW
EXECUTE FUNCTION retire_direct_divergences_for_null_resolver();

CREATE OR REPLACE FUNCTION clear_resolution_divergences_for_block()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE'
        OR (
            OLD.canonicality_state IN ('canonical', 'safe', 'finalized')
            AND NEW.canonicality_state
                NOT IN ('canonical', 'safe', 'finalized')
        )
    THEN
        UPDATE resolution_divergences AS divergence
        SET cleared_at =
            GREATEST(statement_timestamp(), divergence.last_observed_at)
        WHERE divergence.cleared_at IS NULL
          AND EXISTS (
              SELECT 1
              FROM jsonb_each(divergence.observed_positions)
                  AS position(slot, value)
              WHERE position.value ->> 'chain_id' = OLD.chain_id
                AND position.value ->> 'block_hash' = OLD.block_hash
                AND (position.value ->> 'block_number')::bigint =
                    OLD.block_number
          );
    END IF;

    RETURN NULL;
END
$$;

DROP TRIGGER IF EXISTS chain_lineage_clear_resolution_divergences
    ON chain_lineage;
CREATE TRIGGER chain_lineage_clear_resolution_divergences
AFTER UPDATE OF canonicality_state OR DELETE ON chain_lineage
FOR EACH ROW
EXECUTE FUNCTION clear_resolution_divergences_for_block();

COMMENT ON TABLE resolution_divergences IS
    'This table stores disagreements between indexed and live answers.';
COMMENT ON COLUMN resolution_divergences.logical_name_id IS
    'This value identifies the requested name.';
COMMENT ON COLUMN resolution_divergences.resolver_chain_id IS
    'This value identifies the resolver chain.';
COMMENT ON COLUMN resolution_divergences.resolver_address IS
    'This value is the resolver address.';
COMMENT ON COLUMN resolution_divergences.request_kind IS
    'This unbounded value identifies the requested record and is retained for exact equality checks.';
COMMENT ON COLUMN resolution_divergences.request_kind_hash IS
    'This fixed-width SHA-256 digest is the btree lookup key for request_kind; exact request_kind comparison makes digest collisions fail closed.';
COMMENT ON COLUMN resolution_divergences.observed_positions IS
    'This ChainPositions object identifies the compared canonical blocks.';
COMMENT ON COLUMN resolution_divergences.indexed_result IS
    'This value stores the indexed answer.';
COMMENT ON COLUMN resolution_divergences.live_result IS
    'This value stores the live answer.';
COMMENT ON COLUMN resolution_divergences.first_observed_at IS
    'This time records the first disagreement.';
COMMENT ON COLUMN resolution_divergences.last_observed_at IS
    'This time records the latest disagreement.';
COMMENT ON COLUMN resolution_divergences.cleared_at IS
    'This time records agreement restoration or guarded retirement after the exact resolver becomes null.';
COMMENT ON FUNCTION write_resolution_divergence(
    uuid, text, text, text, bigint, text, jsonb, text, text, text,
    text, jsonb, jsonb, boolean
) IS
    'Derives the indexed answer and mutates a direct-resolution disagreement only while its exact projected name, resolver, inventory row, head, and canonical positions are unchanged.';
COMMENT ON FUNCTION revalidate_resolution_lookup_state(
    text, bigint, text, jsonb, jsonb, uuid, text, text
) IS
    'Locks and revalidates the authoritative head, project generation, optional exact name and inventory rows, manifest declarations, and all observed canonical positions without granting the caller UPDATE on those relations.';
COMMENT ON FUNCTION retire_direct_divergences_for_null_resolver() IS
    'Retires active direct-resolver observations during projection publication when an ENS Mainnet exact resolver becomes null; it performs no live/indexed comparison.';
