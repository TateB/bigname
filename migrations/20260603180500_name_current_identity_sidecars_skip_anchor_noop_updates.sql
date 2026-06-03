-- Per-name projection upserts restate the identity anchor columns even when
-- they only refresh metadata. Keep reverse-identity sidecars on true anchor
-- changes, inserts, and deletes; skip no-op UPDATE trigger executions.

DROP TRIGGER IF EXISTS address_names_current_identity_counts_name_current
    ON public.name_current;
DROP TRIGGER IF EXISTS address_names_current_identity_counts_name_current_insert_delete
    ON public.name_current;
DROP TRIGGER IF EXISTS address_names_current_identity_counts_name_current_update
    ON public.name_current;

CREATE TRIGGER address_names_current_identity_counts_name_current_insert_delete
    AFTER INSERT OR DELETE ON public.name_current
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_counts_recompute_for_name_current();

CREATE TRIGGER address_names_current_identity_counts_name_current_update
    AFTER UPDATE OF logical_name_id, surface_binding_id, resource_id, token_lineage_id
    ON public.name_current
    FOR EACH ROW
    WHEN (
        OLD.logical_name_id IS DISTINCT FROM NEW.logical_name_id
        OR OLD.surface_binding_id IS DISTINCT FROM NEW.surface_binding_id
        OR OLD.resource_id IS DISTINCT FROM NEW.resource_id
        OR OLD.token_lineage_id IS DISTINCT FROM NEW.token_lineage_id
    )
    EXECUTE FUNCTION public.address_names_current_identity_counts_recompute_for_name_current();

DROP TRIGGER IF EXISTS name_current_identity_feed_after_change
    ON public.name_current;
DROP TRIGGER IF EXISTS name_current_identity_feed_after_insert_delete
    ON public.name_current;
DROP TRIGGER IF EXISTS name_current_identity_feed_after_anchor_update
    ON public.name_current;

CREATE TRIGGER name_current_identity_feed_after_insert_delete
    AFTER INSERT OR DELETE ON public.name_current
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_name_current_trigger();

CREATE TRIGGER name_current_identity_feed_after_anchor_update
    AFTER UPDATE OF logical_name_id, resource_id, surface_binding_id, token_lineage_id
    ON public.name_current
    FOR EACH ROW
    WHEN (
        OLD.logical_name_id IS DISTINCT FROM NEW.logical_name_id
        OR OLD.resource_id IS DISTINCT FROM NEW.resource_id
        OR OLD.surface_binding_id IS DISTINCT FROM NEW.surface_binding_id
        OR OLD.token_lineage_id IS DISTINCT FROM NEW.token_lineage_id
    )
    EXECUTE FUNCTION public.address_names_current_identity_feed_name_current_trigger();
