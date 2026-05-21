CREATE OR REPLACE FUNCTION public.address_names_current_identity_count_increment(
    target_address text,
    target_roles text
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.address_names_current_identity_counts (address, roles, total_count)
    VALUES (target_address, target_roles, 1)
    ON CONFLICT (address, roles) DO UPDATE
    SET
        total_count = public.address_names_current_identity_counts.total_count + 1,
        updated_at = now();
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_count_decrement(
    target_address text,
    target_roles text
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE public.address_names_current_identity_counts
    SET
        total_count = total_count - 1,
        updated_at = now()
    WHERE address = target_address
      AND roles = target_roles;

    DELETE FROM public.address_names_current_identity_counts
    WHERE address = target_address
      AND roles = target_roles
      AND total_count <= 0;
END;
$$;

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
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_apply_insert(
    target_address text,
    target_logical_name_id text,
    target_relation text,
    target_resource_id uuid,
    target_surface_binding_id uuid,
    target_token_lineage_id uuid
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT public.address_names_current_identity_row_readable(
        target_logical_name_id,
        target_resource_id,
        target_surface_binding_id,
        target_token_lineage_id
    ) THEN
        RETURN;
    END IF;

    IF public.address_names_current_identity_visible_relation_count(
        target_address,
        target_logical_name_id,
        'both'
    ) = 1 THEN
        PERFORM public.address_names_current_identity_count_increment(target_address, 'both');
    END IF;

    IF target_relation IN ('registrant', 'token_holder')
       AND public.address_names_current_identity_visible_relation_count(
           target_address,
           target_logical_name_id,
           'owned'
       ) = 1 THEN
        PERFORM public.address_names_current_identity_count_increment(target_address, 'owned');
    END IF;

    IF target_relation = 'effective_controller'
       AND public.address_names_current_identity_visible_relation_count(
           target_address,
           target_logical_name_id,
           'managed'
       ) = 1 THEN
        PERFORM public.address_names_current_identity_count_increment(target_address, 'managed');
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_apply_delete(
    target_address text,
    target_logical_name_id text,
    target_relation text,
    target_resource_id uuid,
    target_surface_binding_id uuid,
    target_token_lineage_id uuid
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT public.address_names_current_identity_row_readable(
        target_logical_name_id,
        target_resource_id,
        target_surface_binding_id,
        target_token_lineage_id
    ) THEN
        RETURN;
    END IF;

    IF public.address_names_current_identity_visible_relation_count(
        target_address,
        target_logical_name_id,
        'both'
    ) = 0 THEN
        PERFORM public.address_names_current_identity_count_decrement(target_address, 'both');
    END IF;

    IF target_relation IN ('registrant', 'token_holder')
       AND public.address_names_current_identity_visible_relation_count(
           target_address,
           target_logical_name_id,
           'owned'
       ) = 0 THEN
        PERFORM public.address_names_current_identity_count_decrement(target_address, 'owned');
    END IF;

    IF target_relation = 'effective_controller'
       AND public.address_names_current_identity_visible_relation_count(
           target_address,
           target_logical_name_id,
           'managed'
       ) = 0 THEN
        PERFORM public.address_names_current_identity_count_decrement(target_address, 'managed');
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_recompute_address(
    target_address text
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
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

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM public.address_names_current_identity_counts_apply_insert(
            NEW.address,
            NEW.logical_name_id,
            NEW.relation,
            NEW.resource_id,
            NEW.surface_binding_id,
            NEW.token_lineage_id
        );
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM public.address_names_current_identity_counts_apply_delete(
            OLD.address,
            OLD.logical_name_id,
            OLD.relation,
            OLD.resource_id,
            OLD.surface_binding_id,
            OLD.token_lineage_id
        );
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.address IS DISTINCT FROM NEW.address
            OR OLD.logical_name_id IS DISTINCT FROM NEW.logical_name_id
            OR OLD.relation IS DISTINCT FROM NEW.relation
            OR OLD.resource_id IS DISTINCT FROM NEW.resource_id
            OR OLD.surface_binding_id IS DISTINCT FROM NEW.surface_binding_id
            OR OLD.token_lineage_id IS DISTINCT FROM NEW.token_lineage_id THEN
            PERFORM public.address_names_current_identity_counts_recompute_address(OLD.address);
            IF OLD.address IS DISTINCT FROM NEW.address THEN
                PERFORM public.address_names_current_identity_counts_recompute_address(NEW.address);
            END IF;
        END IF;
        RETURN NEW;
    END IF;

    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS address_names_current_identity_counts_after_update
    ON public.address_names_current;

CREATE TRIGGER address_names_current_identity_counts_after_update
    AFTER UPDATE OF address, logical_name_id, relation, resource_id, surface_binding_id, token_lineage_id
    ON public.address_names_current
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_counts_trigger();

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_recompute_for_surface()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT DISTINCT anc.address
        FROM public.address_names_current anc
        WHERE anc.logical_name_id = NEW.logical_name_id
    LOOP
        PERFORM public.address_names_current_identity_counts_recompute_address(target_address);
    END LOOP;

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
        WHERE anc.resource_id = NEW.resource_id
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
        WHERE anc.surface_binding_id = NEW.surface_binding_id
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
        WHERE anc.token_lineage_id = NEW.token_lineage_id
    LOOP
        PERFORM public.address_names_current_identity_counts_recompute_address(target_address);
    END LOOP;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS address_names_current_identity_counts_surface_canonicality
    ON public.name_surfaces;
DROP TRIGGER IF EXISTS address_names_current_identity_counts_resource_canonicality
    ON public.resources;
DROP TRIGGER IF EXISTS address_names_current_identity_counts_binding_canonicality
    ON public.surface_bindings;
DROP TRIGGER IF EXISTS address_names_current_identity_counts_token_lineage_canonicality
    ON public.token_lineages;

CREATE TRIGGER address_names_current_identity_counts_surface_canonicality
    AFTER UPDATE OF canonicality_state ON public.name_surfaces
    FOR EACH ROW
    WHEN (OLD.canonicality_state IS DISTINCT FROM NEW.canonicality_state)
    EXECUTE FUNCTION public.address_names_current_identity_counts_recompute_for_surface();

CREATE TRIGGER address_names_current_identity_counts_resource_canonicality
    AFTER UPDATE OF canonicality_state ON public.resources
    FOR EACH ROW
    WHEN (OLD.canonicality_state IS DISTINCT FROM NEW.canonicality_state)
    EXECUTE FUNCTION public.address_names_current_identity_counts_recompute_for_resource();

CREATE TRIGGER address_names_current_identity_counts_binding_canonicality
    AFTER UPDATE OF canonicality_state ON public.surface_bindings
    FOR EACH ROW
    WHEN (OLD.canonicality_state IS DISTINCT FROM NEW.canonicality_state)
    EXECUTE FUNCTION public.address_names_current_identity_counts_recompute_for_binding();

CREATE TRIGGER address_names_current_identity_counts_token_lineage_canonicality
    AFTER UPDATE OF canonicality_state ON public.token_lineages
    FOR EACH ROW
    WHEN (OLD.canonicality_state IS DISTINCT FROM NEW.canonicality_state)
    EXECUTE FUNCTION public.address_names_current_identity_counts_recompute_for_token_lineage();

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
