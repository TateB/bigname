CREATE OR REPLACE FUNCTION public.identity_canonicality_readable(
    state public.canonicality_state
) RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT state IN (
        'canonical'::public.canonicality_state,
        'safe'::public.canonicality_state,
        'finalized'::public.canonicality_state
    );
$$;

CREATE OR REPLACE FUNCTION public.identity_canonicality_readability_changed(
    old_state public.canonicality_state,
    new_state public.canonicality_state
) RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT public.identity_canonicality_readable(old_state)
        IS DISTINCT FROM public.identity_canonicality_readable(new_state);
$$;

CREATE OR REPLACE FUNCTION public.anc_counts_name_surfaces_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_logical_name_ids text[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.logical_name_id)
    INTO target_logical_name_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.logical_name_id = new_rows.logical_name_id
    WHERE public.identity_canonicality_readability_changed(
        old_rows.canonicality_state,
        new_rows.canonicality_state
    );

    IF target_logical_name_ids IS NOT NULL THEN
        PERFORM public.anc_identity_counts_recompute_logical_names(target_logical_name_ids);
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_counts_resources_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_resource_ids uuid[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.resource_id)
    INTO target_resource_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.resource_id = new_rows.resource_id
    WHERE public.identity_canonicality_readability_changed(
        old_rows.canonicality_state,
        new_rows.canonicality_state
    );

    IF target_resource_ids IS NOT NULL THEN
        PERFORM public.anc_identity_counts_recompute_resources(target_resource_ids);
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_counts_bindings_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_surface_binding_ids uuid[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.surface_binding_id)
    INTO target_surface_binding_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.surface_binding_id = new_rows.surface_binding_id
    WHERE old_rows.active_to IS DISTINCT FROM new_rows.active_to
       OR public.identity_canonicality_readability_changed(
           old_rows.canonicality_state,
           new_rows.canonicality_state
       );

    IF target_surface_binding_ids IS NOT NULL THEN
        PERFORM public.anc_identity_counts_recompute_bindings(target_surface_binding_ids);
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_counts_token_lineages_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_token_lineage_ids uuid[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.token_lineage_id)
    INTO target_token_lineage_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.token_lineage_id = new_rows.token_lineage_id
    WHERE public.identity_canonicality_readability_changed(
        old_rows.canonicality_state,
        new_rows.canonicality_state
    );

    IF target_token_lineage_ids IS NOT NULL THEN
        PERFORM public.anc_identity_counts_recompute_token_lineages(target_token_lineage_ids);
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_feed_name_surfaces_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_logical_name_ids text[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.logical_name_id)
    INTO target_logical_name_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.logical_name_id = new_rows.logical_name_id
    WHERE public.identity_canonicality_readability_changed(
        old_rows.canonicality_state,
        new_rows.canonicality_state
    );

    IF target_logical_name_ids IS NOT NULL THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_logical_names(
            target_logical_name_ids
        );
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_feed_resources_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_resource_ids uuid[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.resource_id)
    INTO target_resource_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.resource_id = new_rows.resource_id
    WHERE public.identity_canonicality_readability_changed(
        old_rows.canonicality_state,
        new_rows.canonicality_state
    );

    IF target_resource_ids IS NOT NULL THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_resources(
            target_resource_ids
        );
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_feed_bindings_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_surface_binding_ids uuid[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.surface_binding_id)
    INTO target_surface_binding_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.surface_binding_id = new_rows.surface_binding_id
    WHERE old_rows.active_to IS DISTINCT FROM new_rows.active_to
       OR public.identity_canonicality_readability_changed(
           old_rows.canonicality_state,
           new_rows.canonicality_state
       );

    IF target_surface_binding_ids IS NOT NULL THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_bindings(
            target_surface_binding_ids
        );
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_feed_token_lineages_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_token_lineage_ids uuid[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.token_lineage_id)
    INTO target_token_lineage_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.token_lineage_id = new_rows.token_lineage_id
    WHERE public.identity_canonicality_readability_changed(
        old_rows.canonicality_state,
        new_rows.canonicality_state
    );

    IF target_token_lineage_ids IS NOT NULL THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_token_lineage(
            target_token_lineage_ids
        );
    END IF;

    RETURN NULL;
END;
$$;

-- queue_surface_binding_repair_projection_invalidations intentionally remains ungated.
