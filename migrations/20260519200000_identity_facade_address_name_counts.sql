CREATE TABLE IF NOT EXISTS public.address_names_current_identity_counts (
    address text NOT NULL,
    roles text NOT NULL,
    total_count bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT address_names_current_identity_counts_pkey PRIMARY KEY (address, roles),
    CONSTRAINT address_names_current_identity_counts_roles_check CHECK (
        roles = ANY (ARRAY['owned'::text, 'managed'::text, 'both'::text])
    ),
    CONSTRAINT address_names_current_identity_counts_total_count_check CHECK (total_count >= 0)
);

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

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_apply_insert(
    target_address text,
    target_logical_name_id text,
    target_relation text
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF (
        SELECT COUNT(*)
        FROM public.address_names_current anc
        WHERE anc.address = target_address
          AND anc.logical_name_id = target_logical_name_id
    ) = 1 THEN
        PERFORM public.address_names_current_identity_count_increment(target_address, 'both');
    END IF;

    IF target_relation IN ('registrant', 'token_holder') AND (
        SELECT COUNT(*)
        FROM public.address_names_current anc
        WHERE anc.address = target_address
          AND anc.logical_name_id = target_logical_name_id
          AND anc.relation IN ('registrant', 'token_holder')
    ) = 1 THEN
        PERFORM public.address_names_current_identity_count_increment(target_address, 'owned');
    END IF;

    IF target_relation = 'effective_controller' AND (
        SELECT COUNT(*)
        FROM public.address_names_current anc
        WHERE anc.address = target_address
          AND anc.logical_name_id = target_logical_name_id
          AND anc.relation = 'effective_controller'
    ) = 1 THEN
        PERFORM public.address_names_current_identity_count_increment(target_address, 'managed');
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_apply_delete(
    target_address text,
    target_logical_name_id text,
    target_relation text
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.address_names_current anc
        WHERE anc.address = target_address
          AND anc.logical_name_id = target_logical_name_id
    ) THEN
        PERFORM public.address_names_current_identity_count_decrement(target_address, 'both');
    END IF;

    IF target_relation IN ('registrant', 'token_holder') AND NOT EXISTS (
        SELECT 1
        FROM public.address_names_current anc
        WHERE anc.address = target_address
          AND anc.logical_name_id = target_logical_name_id
          AND anc.relation IN ('registrant', 'token_holder')
    ) THEN
        PERFORM public.address_names_current_identity_count_decrement(target_address, 'owned');
    END IF;

    IF target_relation = 'effective_controller' AND NOT EXISTS (
        SELECT 1
        FROM public.address_names_current anc
        WHERE anc.address = target_address
          AND anc.logical_name_id = target_logical_name_id
          AND anc.relation = 'effective_controller'
    ) THEN
        PERFORM public.address_names_current_identity_count_decrement(target_address, 'managed');
    END IF;
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
            NEW.relation
        );
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM public.address_names_current_identity_counts_apply_delete(
            OLD.address,
            OLD.logical_name_id,
            OLD.relation
        );
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.address IS DISTINCT FROM NEW.address
            OR OLD.logical_name_id IS DISTINCT FROM NEW.logical_name_id
            OR OLD.relation IS DISTINCT FROM NEW.relation THEN
            PERFORM public.address_names_current_identity_counts_apply_delete(
                OLD.address,
                OLD.logical_name_id,
                OLD.relation
            );
            PERFORM public.address_names_current_identity_counts_apply_insert(
                NEW.address,
                NEW.logical_name_id,
                NEW.relation
            );
        END IF;
        RETURN NEW;
    END IF;

    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS address_names_current_identity_counts_after_insert
    ON public.address_names_current;
DROP TRIGGER IF EXISTS address_names_current_identity_counts_after_delete
    ON public.address_names_current;
DROP TRIGGER IF EXISTS address_names_current_identity_counts_after_update
    ON public.address_names_current;

CREATE TRIGGER address_names_current_identity_counts_after_insert
    AFTER INSERT ON public.address_names_current
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_counts_trigger();

CREATE TRIGGER address_names_current_identity_counts_after_delete
    AFTER DELETE ON public.address_names_current
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_counts_trigger();

CREATE TRIGGER address_names_current_identity_counts_after_update
    AFTER UPDATE OF address, logical_name_id, relation ON public.address_names_current
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_counts_trigger();

TRUNCATE TABLE public.address_names_current_identity_counts;

WITH relation_groups AS (
    SELECT
        address,
        logical_name_id,
        BOOL_OR(relation IN ('registrant', 'token_holder')) AS owned,
        BOOL_OR(relation = 'effective_controller') AS managed
    FROM public.address_names_current
    GROUP BY address, logical_name_id
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
