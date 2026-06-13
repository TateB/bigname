DROP TRIGGER IF EXISTS address_names_current_identity_feed_after_change
    ON public.address_names_current;
DROP TRIGGER IF EXISTS address_names_current_identity_feed_after_insert_delete
    ON public.address_names_current;
DROP TRIGGER IF EXISTS address_names_current_identity_feed_after_anchor_update
    ON public.address_names_current;

CREATE TRIGGER address_names_current_identity_feed_after_insert_delete
    AFTER INSERT OR DELETE ON public.address_names_current
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_address_trigger();

CREATE TRIGGER address_names_current_identity_feed_after_anchor_update
    AFTER UPDATE OF
        address,
        logical_name_id,
        namespace,
        canonical_display_name,
        normalized_name,
        namehash,
        relation,
        surface_binding_id,
        resource_id,
        token_lineage_id,
        chain_positions,
        coverage
    ON public.address_names_current
    FOR EACH ROW
    WHEN (
        OLD.address IS DISTINCT FROM NEW.address
        OR OLD.logical_name_id IS DISTINCT FROM NEW.logical_name_id
        OR OLD.namespace IS DISTINCT FROM NEW.namespace
        OR OLD.canonical_display_name IS DISTINCT FROM NEW.canonical_display_name
        OR OLD.normalized_name IS DISTINCT FROM NEW.normalized_name
        OR OLD.namehash IS DISTINCT FROM NEW.namehash
        OR OLD.relation IS DISTINCT FROM NEW.relation
        OR OLD.surface_binding_id IS DISTINCT FROM NEW.surface_binding_id
        OR OLD.resource_id IS DISTINCT FROM NEW.resource_id
        OR OLD.token_lineage_id IS DISTINCT FROM NEW.token_lineage_id
        OR OLD.chain_positions IS DISTINCT FROM NEW.chain_positions
        OR OLD.coverage IS DISTINCT FROM NEW.coverage
    )
    EXECUTE FUNCTION public.address_names_current_identity_feed_address_trigger();

DROP TRIGGER IF EXISTS primary_names_current_identity_feed_after_change
    ON public.primary_names_current;
DROP TRIGGER IF EXISTS primary_names_current_identity_feed_after_insert_delete
    ON public.primary_names_current;
DROP TRIGGER IF EXISTS primary_names_current_identity_feed_after_claim_update
    ON public.primary_names_current;

CREATE TRIGGER primary_names_current_identity_feed_after_insert_delete
    AFTER INSERT OR DELETE ON public.primary_names_current
    FOR EACH ROW
    EXECUTE FUNCTION public.address_names_current_identity_feed_primary_trigger();

CREATE TRIGGER primary_names_current_identity_feed_after_claim_update
    AFTER UPDATE OF
        address,
        namespace,
        coin_type,
        normalized_claim_name,
        claim_status
    ON public.primary_names_current
    FOR EACH ROW
    WHEN (
        OLD.address IS DISTINCT FROM NEW.address
        OR OLD.namespace IS DISTINCT FROM NEW.namespace
        OR OLD.coin_type IS DISTINCT FROM NEW.coin_type
        OR OLD.normalized_claim_name IS DISTINCT FROM NEW.normalized_claim_name
        OR OLD.claim_status IS DISTINCT FROM NEW.claim_status
    )
    EXECUTE FUNCTION public.address_names_current_identity_feed_primary_trigger();
