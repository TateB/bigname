-- Materialize the compact reverse-identity feed winner per address, role set,
-- and primary-name coin type. The partner feed route needs to avoid live
-- canonicality/name_current joins for high-cardinality batches while still
-- returning the same readable universe as the canonical reverse collection.
CREATE TABLE IF NOT EXISTS public.address_names_current_identity_feed (
    address text NOT NULL,
    roles text NOT NULL,
    coin_type text NOT NULL DEFAULT '',
    logical_name_id text NOT NULL,
    namespace text NOT NULL,
    canonical_display_name text NOT NULL,
    normalized_name text NOT NULL,
    namehash text NOT NULL,
    chain_positions jsonb DEFAULT '{}'::jsonb NOT NULL,
    coverage jsonb DEFAULT '{}'::jsonb NOT NULL,
    is_primary boolean NOT NULL,
    relation_facets text[] NOT NULL,
    last_recomputed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT address_names_current_identity_feed_pkey
        PRIMARY KEY (address, roles, coin_type),
    CONSTRAINT address_names_current_identity_feed_roles_check CHECK (
        roles = ANY (ARRAY['owned'::text, 'managed'::text, 'both'::text])
    ),
    CONSTRAINT address_names_current_identity_feed_coin_type_check CHECK (
        coin_type = ''::text OR btrim(coin_type) <> ''::text
    ),
    CONSTRAINT address_names_current_identity_feed_relation_facets_check CHECK (
        cardinality(relation_facets) > 0
        AND relation_facets <@ ARRAY[
            'registrant'::text,
            'token_holder'::text,
            'effective_controller'::text
        ]
    )
);

CREATE INDEX IF NOT EXISTS address_names_current_identity_feed_lookup_idx
    ON public.address_names_current_identity_feed (address, roles, coin_type)
    INCLUDE (
        logical_name_id,
        namespace,
        canonical_display_name,
        normalized_name,
        namehash,
        chain_positions,
        coverage,
        is_primary,
        relation_facets
    );

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_candidate_rows(
    target_address text
) RETURNS TABLE (
    address text,
    roles text,
    coin_type text,
    logical_name_id text,
    namespace text,
    canonical_display_name text,
    normalized_name text,
    namehash text,
    chain_positions jsonb,
    coverage jsonb,
    is_primary boolean,
    relation_facets text[]
)
LANGUAGE sql
STABLE
AS $$
    WITH readable_rows AS (
        SELECT
            anc.address,
            anc.logical_name_id,
            anc.namespace,
            anc.canonical_display_name,
            anc.normalized_name,
            anc.namehash,
            anc.relation,
            anc.chain_positions,
            anc.coverage,
            CASE
                WHEN anc.relation IN ('registrant', 'token_holder') THEN 0::smallint
                ELSE 1::smallint
            END AS role_rank
        FROM public.address_names_current anc
        JOIN public.name_surfaces surface
          ON surface.logical_name_id = anc.logical_name_id
        JOIN public.resources resource
          ON resource.resource_id = anc.resource_id
        JOIN public.surface_bindings binding
          ON binding.surface_binding_id = anc.surface_binding_id
        LEFT JOIN public.token_lineages token_lineage
          ON token_lineage.token_lineage_id = anc.token_lineage_id
        JOIN public.name_current identity_nc
          ON identity_nc.logical_name_id = anc.logical_name_id
        JOIN public.name_surfaces identity_nc_surface
          ON identity_nc_surface.logical_name_id = identity_nc.logical_name_id
        LEFT JOIN public.resources identity_nc_resource
          ON identity_nc_resource.resource_id = identity_nc.resource_id
        LEFT JOIN public.surface_bindings identity_nc_binding
          ON identity_nc_binding.surface_binding_id = identity_nc.surface_binding_id
        LEFT JOIN public.token_lineages identity_nc_token_lineage
          ON identity_nc_token_lineage.token_lineage_id = identity_nc.token_lineage_id
        WHERE (target_address IS NULL OR anc.address = target_address)
          AND surface.canonicality_state IN (
              'canonical'::public.canonicality_state,
              'safe'::public.canonicality_state,
              'finalized'::public.canonicality_state
          )
          AND resource.canonicality_state IN (
              'canonical'::public.canonicality_state,
              'safe'::public.canonicality_state,
              'finalized'::public.canonicality_state
          )
          AND binding.canonicality_state IN (
              'canonical'::public.canonicality_state,
              'safe'::public.canonicality_state,
              'finalized'::public.canonicality_state
          )
          AND (
              anc.token_lineage_id IS NULL
              OR token_lineage.canonicality_state IN (
                  'canonical'::public.canonicality_state,
                  'safe'::public.canonicality_state,
                  'finalized'::public.canonicality_state
              )
          )
          AND identity_nc_surface.canonicality_state IN (
              'canonical'::public.canonicality_state,
              'safe'::public.canonicality_state,
              'finalized'::public.canonicality_state
          )
          AND (
              identity_nc.surface_binding_id IS NULL
              OR (
                  identity_nc_resource.canonicality_state IN (
                      'canonical'::public.canonicality_state,
                      'safe'::public.canonicality_state,
                      'finalized'::public.canonicality_state
                  )
                  AND identity_nc_binding.canonicality_state IN (
                      'canonical'::public.canonicality_state,
                      'safe'::public.canonicality_state,
                      'finalized'::public.canonicality_state
                  )
                  AND (
                      identity_nc.token_lineage_id IS NULL
                      OR identity_nc_token_lineage.canonicality_state IN (
                          'canonical'::public.canonicality_state,
                          'safe'::public.canonicality_state,
                          'finalized'::public.canonicality_state
                      )
                  )
              )
          )
    ),
    role_rows AS (
        SELECT
            rr.address,
            'owned'::text AS roles,
            rr.logical_name_id,
            rr.namespace,
            rr.canonical_display_name,
            rr.normalized_name,
            rr.namehash,
            rr.relation,
            rr.chain_positions,
            rr.coverage,
            rr.role_rank
        FROM readable_rows rr
        WHERE rr.relation IN ('registrant', 'token_holder')
        UNION ALL
        SELECT
            rr.address,
            'managed'::text AS roles,
            rr.logical_name_id,
            rr.namespace,
            rr.canonical_display_name,
            rr.normalized_name,
            rr.namehash,
            rr.relation,
            rr.chain_positions,
            rr.coverage,
            rr.role_rank
        FROM readable_rows rr
        WHERE rr.relation = 'effective_controller'
        UNION ALL
        SELECT
            rr.address,
            'both'::text AS roles,
            rr.logical_name_id,
            rr.namespace,
            rr.canonical_display_name,
            rr.normalized_name,
            rr.namehash,
            rr.relation,
            rr.chain_positions,
            rr.coverage,
            rr.role_rank
        FROM readable_rows rr
    ),
    facets AS (
        SELECT
            rr.address,
            rr.roles,
            rr.logical_name_id,
            ARRAY_AGG(
                rr.relation
                ORDER BY
                    CASE rr.relation
                        WHEN 'registrant' THEN 0
                        WHEN 'token_holder' THEN 1
                        WHEN 'effective_controller' THEN 2
                        ELSE 99
                    END
            ) AS relation_facets
        FROM role_rows rr
        GROUP BY rr.address, rr.roles, rr.logical_name_id
    ),
    fallback_ranked AS (
        SELECT
            rr.*,
            ROW_NUMBER() OVER (
                PARTITION BY rr.address, rr.roles
                ORDER BY
                    rr.role_rank ASC,
                    rr.normalized_name ASC,
                    rr.namespace ASC,
                    rr.namehash ASC,
                    rr.logical_name_id ASC
            ) AS row_rank
        FROM role_rows rr
    ),
    fallback_rows AS (
        SELECT
            ranked.address,
            ranked.roles,
            ''::text AS coin_type,
            ranked.logical_name_id,
            ranked.namespace,
            ranked.canonical_display_name,
            ranked.normalized_name,
            ranked.namehash,
            ranked.chain_positions,
            ranked.coverage,
            FALSE AS is_primary,
            facets.relation_facets
        FROM fallback_ranked ranked
        JOIN facets
          ON facets.address = ranked.address
         AND facets.roles = ranked.roles
         AND facets.logical_name_id = ranked.logical_name_id
        WHERE ranked.row_rank = 1
    ),
    primary_ranked AS (
        SELECT
            rr.*,
            pnc.coin_type,
            ROW_NUMBER() OVER (
                PARTITION BY rr.address, pnc.coin_type, rr.roles
                ORDER BY
                    rr.role_rank ASC,
                    rr.normalized_name ASC,
                    rr.namespace ASC,
                    rr.namehash ASC,
                    rr.logical_name_id ASC
            ) AS row_rank
        FROM role_rows rr
        JOIN public.primary_names_current pnc
          ON pnc.address = rr.address
         AND pnc.namespace = rr.namespace
         AND pnc.normalized_claim_name = rr.normalized_name
        WHERE pnc.claim_status = 'success'
          AND (target_address IS NULL OR pnc.address = target_address)
    ),
    primary_rows AS (
        SELECT
            ranked.address,
            ranked.roles,
            ranked.coin_type,
            ranked.logical_name_id,
            ranked.namespace,
            ranked.canonical_display_name,
            ranked.normalized_name,
            ranked.namehash,
            ranked.chain_positions,
            ranked.coverage,
            TRUE AS is_primary,
            facets.relation_facets
        FROM primary_ranked ranked
        JOIN facets
          ON facets.address = ranked.address
         AND facets.roles = ranked.roles
         AND facets.logical_name_id = ranked.logical_name_id
        WHERE ranked.row_rank = 1
    )
    SELECT * FROM fallback_rows
    UNION ALL
    SELECT * FROM primary_rows
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_recompute_address(
    target_address text
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF target_address IS NULL OR btrim(target_address) = '' THEN
        RETURN;
    END IF;

    PERFORM public.address_names_current_identity_counts_lock_address(target_address);

    DELETE FROM public.address_names_current_identity_feed
    WHERE address = target_address;

    INSERT INTO public.address_names_current_identity_feed (
        address,
        roles,
        coin_type,
        logical_name_id,
        namespace,
        canonical_display_name,
        normalized_name,
        namehash,
        chain_positions,
        coverage,
        is_primary,
        relation_facets,
        last_recomputed_at
    )
    SELECT
        candidate.address,
        candidate.roles,
        candidate.coin_type,
        candidate.logical_name_id,
        candidate.namespace,
        candidate.canonical_display_name,
        candidate.normalized_name,
        candidate.namehash,
        candidate.chain_positions,
        candidate.coverage,
        candidate.is_primary,
        candidate.relation_facets,
        now()
    FROM public.address_names_current_identity_feed_candidate_rows(target_address) candidate
    ON CONFLICT (address, roles, coin_type) DO UPDATE
    SET
        logical_name_id = EXCLUDED.logical_name_id,
        namespace = EXCLUDED.namespace,
        canonical_display_name = EXCLUDED.canonical_display_name,
        normalized_name = EXCLUDED.normalized_name,
        namehash = EXCLUDED.namehash,
        chain_positions = EXCLUDED.chain_positions,
        coverage = EXCLUDED.coverage,
        is_primary = EXCLUDED.is_primary,
        relation_facets = EXCLUDED.relation_facets,
        last_recomputed_at = EXCLUDED.last_recomputed_at;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_recompute_for_logical_names(
    target_logical_name_ids text[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT DISTINCT anc.address
        FROM public.address_names_current anc
        WHERE anc.logical_name_id = ANY(target_logical_name_ids)
        ORDER BY anc.address
    LOOP
        PERFORM public.address_names_current_identity_feed_recompute_address(target_address);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_recompute_for_resources(
    target_resource_ids uuid[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT DISTINCT anc.address
        FROM public.address_names_current anc
        LEFT JOIN public.name_current nc
          ON nc.logical_name_id = anc.logical_name_id
        WHERE anc.resource_id = ANY(target_resource_ids)
           OR nc.resource_id = ANY(target_resource_ids)
        ORDER BY anc.address
    LOOP
        PERFORM public.address_names_current_identity_feed_recompute_address(target_address);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_recompute_for_bindings(
    target_surface_binding_ids uuid[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT DISTINCT anc.address
        FROM public.address_names_current anc
        LEFT JOIN public.name_current nc
          ON nc.logical_name_id = anc.logical_name_id
        WHERE anc.surface_binding_id = ANY(target_surface_binding_ids)
           OR nc.surface_binding_id = ANY(target_surface_binding_ids)
        ORDER BY anc.address
    LOOP
        PERFORM public.address_names_current_identity_feed_recompute_address(target_address);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_recompute_for_token_lineage(
    target_token_lineage_ids uuid[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT DISTINCT anc.address
        FROM public.address_names_current anc
        LEFT JOIN public.name_current nc
          ON nc.logical_name_id = anc.logical_name_id
        WHERE anc.token_lineage_id = ANY(target_token_lineage_ids)
           OR nc.token_lineage_id = ANY(target_token_lineage_ids)
        ORDER BY anc.address
    LOOP
        PERFORM public.address_names_current_identity_feed_recompute_address(target_address);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_address_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM public.address_names_current_identity_feed_recompute_address(OLD.address);
        RETURN OLD;
    ELSIF TG_OP = 'INSERT' THEN
        PERFORM public.address_names_current_identity_feed_recompute_address(NEW.address);
        RETURN NEW;
    END IF;

    IF OLD.address <= NEW.address THEN
        PERFORM public.address_names_current_identity_feed_recompute_address(OLD.address);
        IF NEW.address IS DISTINCT FROM OLD.address THEN
            PERFORM public.address_names_current_identity_feed_recompute_address(NEW.address);
        END IF;
    ELSE
        PERFORM public.address_names_current_identity_feed_recompute_address(NEW.address);
        PERFORM public.address_names_current_identity_feed_recompute_address(OLD.address);
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS address_names_current_identity_feed_after_change
    ON public.address_names_current;

CREATE TRIGGER address_names_current_identity_feed_after_change
    AFTER INSERT OR UPDATE OR DELETE ON public.address_names_current
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_address_trigger();

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_primary_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM public.address_names_current_identity_feed_recompute_address(OLD.address);
        RETURN OLD;
    ELSIF TG_OP = 'INSERT' THEN
        PERFORM public.address_names_current_identity_feed_recompute_address(NEW.address);
        RETURN NEW;
    END IF;

    IF OLD.address <= NEW.address THEN
        PERFORM public.address_names_current_identity_feed_recompute_address(OLD.address);
        IF NEW.address IS DISTINCT FROM OLD.address THEN
            PERFORM public.address_names_current_identity_feed_recompute_address(NEW.address);
        END IF;
    ELSE
        PERFORM public.address_names_current_identity_feed_recompute_address(NEW.address);
        PERFORM public.address_names_current_identity_feed_recompute_address(OLD.address);
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS primary_names_current_identity_feed_after_change
    ON public.primary_names_current;

CREATE TRIGGER primary_names_current_identity_feed_after_change
    AFTER INSERT OR UPDATE OR DELETE ON public.primary_names_current
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_primary_trigger();

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_name_current_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_logical_names(
            ARRAY[OLD.logical_name_id]::text[]
        );
        RETURN OLD;
    ELSIF TG_OP = 'INSERT' THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_logical_names(
            ARRAY[NEW.logical_name_id]::text[]
        );
        RETURN NEW;
    END IF;

    PERFORM public.address_names_current_identity_feed_recompute_for_logical_names(
        ARRAY[OLD.logical_name_id, NEW.logical_name_id]::text[]
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS name_current_identity_feed_after_change
    ON public.name_current;

CREATE TRIGGER name_current_identity_feed_after_change
    AFTER INSERT
        OR UPDATE OF logical_name_id, resource_id, surface_binding_id, token_lineage_id
        OR DELETE
    ON public.name_current
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_name_current_trigger();

DROP TRIGGER IF EXISTS name_surfaces_identity_feed_after_change
    ON public.name_surfaces;

CREATE TRIGGER name_surfaces_identity_feed_after_change
    AFTER INSERT OR UPDATE OF canonicality_state OR DELETE ON public.name_surfaces
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_name_current_trigger();

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_resource_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_resources(
            ARRAY[OLD.resource_id]::uuid[]
        );
        RETURN OLD;
    ELSIF TG_OP = 'INSERT' THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_resources(
            ARRAY[NEW.resource_id]::uuid[]
        );
        RETURN NEW;
    END IF;

    PERFORM public.address_names_current_identity_feed_recompute_for_resources(
        ARRAY[OLD.resource_id, NEW.resource_id]::uuid[]
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS resources_identity_feed_after_change
    ON public.resources;

CREATE TRIGGER resources_identity_feed_after_change
    AFTER INSERT OR UPDATE OF canonicality_state OR DELETE ON public.resources
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_resource_trigger();

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_binding_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_bindings(
            ARRAY[OLD.surface_binding_id]::uuid[]
        );
        RETURN OLD;
    ELSIF TG_OP = 'INSERT' THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_bindings(
            ARRAY[NEW.surface_binding_id]::uuid[]
        );
        RETURN NEW;
    END IF;

    PERFORM public.address_names_current_identity_feed_recompute_for_bindings(
        ARRAY[OLD.surface_binding_id, NEW.surface_binding_id]::uuid[]
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS surface_bindings_identity_feed_after_change
    ON public.surface_bindings;

CREATE TRIGGER surface_bindings_identity_feed_after_change
    AFTER INSERT OR UPDATE OF canonicality_state OR DELETE ON public.surface_bindings
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_binding_trigger();

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_token_lineage_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_token_lineage(
            ARRAY[OLD.token_lineage_id]::uuid[]
        );
        RETURN OLD;
    ELSIF TG_OP = 'INSERT' THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_token_lineage(
            ARRAY[NEW.token_lineage_id]::uuid[]
        );
        RETURN NEW;
    END IF;

    PERFORM public.address_names_current_identity_feed_recompute_for_token_lineage(
        ARRAY[OLD.token_lineage_id, NEW.token_lineage_id]::uuid[]
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS token_lineages_identity_feed_after_change
    ON public.token_lineages;

CREATE TRIGGER token_lineages_identity_feed_after_change
    AFTER INSERT OR UPDATE OF canonicality_state OR DELETE ON public.token_lineages
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_token_lineage_trigger();

TRUNCATE TABLE public.address_names_current_identity_feed;

INSERT INTO public.address_names_current_identity_feed (
    address,
    roles,
    coin_type,
    logical_name_id,
    namespace,
    canonical_display_name,
    normalized_name,
    namehash,
    chain_positions,
    coverage,
    is_primary,
    relation_facets,
    last_recomputed_at
)
SELECT
    candidate.address,
    candidate.roles,
    candidate.coin_type,
    candidate.logical_name_id,
    candidate.namespace,
    candidate.canonical_display_name,
    candidate.normalized_name,
    candidate.namehash,
    candidate.chain_positions,
    candidate.coverage,
    candidate.is_primary,
    candidate.relation_facets,
    now()
FROM public.address_names_current_identity_feed_candidate_rows(NULL::text) candidate
ON CONFLICT (address, roles, coin_type) DO UPDATE
SET
    logical_name_id = EXCLUDED.logical_name_id,
    namespace = EXCLUDED.namespace,
    canonical_display_name = EXCLUDED.canonical_display_name,
    normalized_name = EXCLUDED.normalized_name,
    namehash = EXCLUDED.namehash,
    chain_positions = EXCLUDED.chain_positions,
    coverage = EXCLUDED.coverage,
    is_primary = EXCLUDED.is_primary,
    relation_facets = EXCLUDED.relation_facets,
    last_recomputed_at = EXCLUDED.last_recomputed_at;
