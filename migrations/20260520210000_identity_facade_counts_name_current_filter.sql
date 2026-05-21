CREATE OR REPLACE FUNCTION public.address_names_current_identity_row_readable(
    target_logical_name_id text,
    target_resource_id uuid,
    target_surface_binding_id uuid,
    target_token_lineage_id uuid
) RETURNS boolean
LANGUAGE sql
VOLATILE
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM public.name_surfaces surface
        JOIN public.resources resource
          ON resource.resource_id = target_resource_id
        JOIN public.surface_bindings binding
          ON binding.surface_binding_id = target_surface_binding_id
        LEFT JOIN public.token_lineages token_lineage
          ON token_lineage.token_lineage_id = target_token_lineage_id
        WHERE surface.logical_name_id = target_logical_name_id
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
              target_token_lineage_id IS NULL
              OR token_lineage.canonicality_state IN (
                  'canonical'::public.canonicality_state,
                  'safe'::public.canonicality_state,
                  'finalized'::public.canonicality_state
              )
          )
    )
    AND EXISTS (
        SELECT 1
        FROM public.name_current identity_nc
        JOIN public.name_surfaces identity_nc_surface
          ON identity_nc_surface.logical_name_id = identity_nc.logical_name_id
        LEFT JOIN public.resources identity_nc_resource
          ON identity_nc_resource.resource_id = identity_nc.resource_id
        LEFT JOIN public.surface_bindings identity_nc_binding
          ON identity_nc_binding.surface_binding_id = identity_nc.surface_binding_id
        LEFT JOIN public.token_lineages identity_nc_token_lineage
          ON identity_nc_token_lineage.token_lineage_id = identity_nc.token_lineage_id
        WHERE identity_nc.logical_name_id = target_logical_name_id
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
    )
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_visible_relation_count(
    target_address text,
    target_logical_name_id text,
    target_roles text
) RETURNS bigint
LANGUAGE sql
VOLATILE
AS $$
    SELECT COUNT(*)::bigint
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
    WHERE anc.address = target_address
      AND anc.logical_name_id = target_logical_name_id
      AND (
          target_roles = 'both'
          OR (target_roles = 'owned' AND anc.relation IN ('registrant', 'token_holder'))
          OR (target_roles = 'managed' AND anc.relation = 'effective_controller')
      )
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
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_recompute_address(
    target_address text
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM public.address_names_current_identity_counts_lock_address(target_address);

    DELETE FROM public.address_names_current_identity_counts
    WHERE address = target_address;

    WITH relation_groups AS (
        SELECT
            anc.address,
            anc.logical_name_id,
            BOOL_OR(anc.relation IN ('registrant', 'token_holder')) AS owned,
            BOOL_OR(anc.relation = 'effective_controller') AS managed
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
        WHERE anc.address = target_address
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
        GROUP BY anc.address, anc.logical_name_id
    ),
    counts AS (
        SELECT address, 'owned'::text AS roles, COUNT(*)::bigint AS total_count
        FROM relation_groups
        WHERE owned
        GROUP BY address
        UNION ALL
        SELECT address, 'managed'::text AS roles, COUNT(*)::bigint AS total_count
        FROM relation_groups
        WHERE managed
        GROUP BY address
        UNION ALL
        SELECT address, 'both'::text AS roles, COUNT(*)::bigint AS total_count
        FROM relation_groups
        GROUP BY address
    )
    INSERT INTO public.address_names_current_identity_counts (address, roles, total_count)
    SELECT address, roles, total_count
    FROM counts
    WHERE total_count > 0
    ON CONFLICT (address, roles) DO UPDATE
    SET
        total_count = EXCLUDED.total_count,
        updated_at = now();
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_recompute_for_name_current()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        FOR target_address IN
            SELECT DISTINCT anc.address
            FROM public.address_names_current anc
            WHERE anc.logical_name_id = OLD.logical_name_id
            ORDER BY anc.address
        LOOP
            PERFORM public.address_names_current_identity_counts_recompute_address(target_address);
        END LOOP;
    END IF;

    IF TG_OP IN ('INSERT', 'UPDATE')
       AND (TG_OP = 'INSERT' OR OLD.logical_name_id IS DISTINCT FROM NEW.logical_name_id) THEN
        FOR target_address IN
            SELECT DISTINCT anc.address
            FROM public.address_names_current anc
            WHERE anc.logical_name_id = NEW.logical_name_id
            ORDER BY anc.address
        LOOP
            PERFORM public.address_names_current_identity_counts_recompute_address(target_address);
        END LOOP;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_recompute_for_resource()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT DISTINCT anc.address
        FROM public.address_names_current anc
        LEFT JOIN public.name_current identity_nc
          ON identity_nc.logical_name_id = anc.logical_name_id
        WHERE anc.resource_id = NEW.resource_id
           OR identity_nc.resource_id = NEW.resource_id
        ORDER BY anc.address
    LOOP
        PERFORM public.address_names_current_identity_counts_recompute_address(target_address);
    END LOOP;

    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_recompute_for_binding()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT DISTINCT anc.address
        FROM public.address_names_current anc
        LEFT JOIN public.name_current identity_nc
          ON identity_nc.logical_name_id = anc.logical_name_id
        WHERE anc.surface_binding_id = NEW.surface_binding_id
           OR identity_nc.surface_binding_id = NEW.surface_binding_id
        ORDER BY anc.address
    LOOP
        PERFORM public.address_names_current_identity_counts_recompute_address(target_address);
    END LOOP;

    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_recompute_for_token_lineage()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT DISTINCT anc.address
        FROM public.address_names_current anc
        LEFT JOIN public.name_current identity_nc
          ON identity_nc.logical_name_id = anc.logical_name_id
        WHERE anc.token_lineage_id = NEW.token_lineage_id
           OR identity_nc.token_lineage_id = NEW.token_lineage_id
        ORDER BY anc.address
    LOOP
        PERFORM public.address_names_current_identity_counts_recompute_address(target_address);
    END LOOP;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS address_names_current_identity_counts_name_current
    ON public.name_current;

CREATE TRIGGER address_names_current_identity_counts_name_current
    AFTER INSERT OR DELETE OR UPDATE OF logical_name_id, surface_binding_id, resource_id, token_lineage_id
    ON public.name_current
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_counts_recompute_for_name_current();

TRUNCATE TABLE public.address_names_current_identity_counts;

WITH relation_groups AS (
    SELECT
        anc.address,
        anc.logical_name_id,
        BOOL_OR(anc.relation IN ('registrant', 'token_holder')) AS owned,
        BOOL_OR(anc.relation = 'effective_controller') AS managed
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
    WHERE surface.canonicality_state IN (
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
    GROUP BY anc.address, anc.logical_name_id
),
counts AS (
    SELECT address, 'owned'::text AS roles, COUNT(*)::bigint AS total_count
    FROM relation_groups
    WHERE owned
    GROUP BY address
    UNION ALL
    SELECT address, 'managed'::text AS roles, COUNT(*)::bigint AS total_count
    FROM relation_groups
    WHERE managed
    GROUP BY address
    UNION ALL
    SELECT address, 'both'::text AS roles, COUNT(*)::bigint AS total_count
    FROM relation_groups
    GROUP BY address
)
INSERT INTO public.address_names_current_identity_counts (address, roles, total_count)
SELECT address, roles, total_count
FROM counts
WHERE total_count > 0
ON CONFLICT (address, roles) DO UPDATE
SET
    total_count = EXCLUDED.total_count,
    updated_at = now();
