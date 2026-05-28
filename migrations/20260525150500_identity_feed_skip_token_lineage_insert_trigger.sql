DROP TRIGGER IF EXISTS token_lineages_identity_feed_after_change
    ON public.token_lineages;

CREATE TRIGGER token_lineages_identity_feed_after_change
    AFTER UPDATE OF canonicality_state OR DELETE ON public.token_lineages
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_token_lineage_trigger();
