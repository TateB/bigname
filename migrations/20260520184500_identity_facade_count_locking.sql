CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_lock_pair(
    target_address text,
    target_logical_name_id text
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(target_address), hashtext(target_logical_name_id));
END;
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
    PERFORM public.address_names_current_identity_counts_lock_pair(
        target_address,
        target_logical_name_id
    );

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
    PERFORM public.address_names_current_identity_counts_lock_pair(
        target_address,
        target_logical_name_id
    );

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

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    old_pair text;
    new_pair text;
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
            old_pair := OLD.address || chr(31) || OLD.logical_name_id;
            new_pair := NEW.address || chr(31) || NEW.logical_name_id;

            IF old_pair <= new_pair THEN
                PERFORM public.address_names_current_identity_counts_lock_pair(
                    OLD.address,
                    OLD.logical_name_id
                );
                IF old_pair IS DISTINCT FROM new_pair THEN
                    PERFORM public.address_names_current_identity_counts_lock_pair(
                        NEW.address,
                        NEW.logical_name_id
                    );
                END IF;
            ELSE
                PERFORM public.address_names_current_identity_counts_lock_pair(
                    NEW.address,
                    NEW.logical_name_id
                );
                PERFORM public.address_names_current_identity_counts_lock_pair(
                    OLD.address,
                    OLD.logical_name_id
                );
            END IF;

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
