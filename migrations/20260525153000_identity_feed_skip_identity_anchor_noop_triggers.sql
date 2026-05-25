DROP TRIGGER IF EXISTS name_surfaces_identity_feed_after_change
    ON public.name_surfaces;
DROP TRIGGER IF EXISTS resources_identity_feed_after_change
    ON public.resources;
DROP TRIGGER IF EXISTS surface_bindings_identity_feed_after_change
    ON public.surface_bindings;
DROP TRIGGER IF EXISTS token_lineages_identity_feed_after_change
    ON public.token_lineages;

DROP TRIGGER IF EXISTS name_surfaces_identity_feed_after_canonicality_update
    ON public.name_surfaces;
DROP TRIGGER IF EXISTS name_surfaces_identity_feed_after_delete
    ON public.name_surfaces;
DROP TRIGGER IF EXISTS resources_identity_feed_after_canonicality_update
    ON public.resources;
DROP TRIGGER IF EXISTS resources_identity_feed_after_delete
    ON public.resources;
DROP TRIGGER IF EXISTS surface_bindings_identity_feed_after_canonicality_update
    ON public.surface_bindings;
DROP TRIGGER IF EXISTS surface_bindings_identity_feed_after_delete
    ON public.surface_bindings;
DROP TRIGGER IF EXISTS token_lineages_identity_feed_after_canonicality_update
    ON public.token_lineages;
DROP TRIGGER IF EXISTS token_lineages_identity_feed_after_delete
    ON public.token_lineages;

CREATE TRIGGER name_surfaces_identity_feed_after_canonicality_update
    AFTER UPDATE OF canonicality_state ON public.name_surfaces
    FOR EACH ROW
    WHEN (OLD.canonicality_state IS DISTINCT FROM NEW.canonicality_state)
    EXECUTE FUNCTION public.address_names_current_identity_feed_name_current_trigger();

CREATE TRIGGER name_surfaces_identity_feed_after_delete
    AFTER DELETE ON public.name_surfaces
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_name_current_trigger();

CREATE TRIGGER resources_identity_feed_after_canonicality_update
    AFTER UPDATE OF canonicality_state ON public.resources
    FOR EACH ROW
    WHEN (OLD.canonicality_state IS DISTINCT FROM NEW.canonicality_state)
    EXECUTE FUNCTION public.address_names_current_identity_feed_resource_trigger();

CREATE TRIGGER resources_identity_feed_after_delete
    AFTER DELETE ON public.resources
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_resource_trigger();

CREATE TRIGGER surface_bindings_identity_feed_after_canonicality_update
    AFTER UPDATE OF canonicality_state ON public.surface_bindings
    FOR EACH ROW
    WHEN (OLD.canonicality_state IS DISTINCT FROM NEW.canonicality_state)
    EXECUTE FUNCTION public.address_names_current_identity_feed_binding_trigger();

CREATE TRIGGER surface_bindings_identity_feed_after_delete
    AFTER DELETE ON public.surface_bindings
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_binding_trigger();

CREATE TRIGGER token_lineages_identity_feed_after_canonicality_update
    AFTER UPDATE OF canonicality_state ON public.token_lineages
    FOR EACH ROW
    WHEN (OLD.canonicality_state IS DISTINCT FROM NEW.canonicality_state)
    EXECUTE FUNCTION public.address_names_current_identity_feed_token_lineage_trigger();

CREATE TRIGGER token_lineages_identity_feed_after_delete
    AFTER DELETE ON public.token_lineages
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_token_lineage_trigger();
