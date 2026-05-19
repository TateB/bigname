CREATE TABLE public.projection_normalized_event_changes (
    change_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    normalized_event_id BIGINT NOT NULL REFERENCES public.normalized_events(normalized_event_id),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    change_kind TEXT NOT NULL,
    canonicality_state public.canonicality_state NOT NULL,
    CONSTRAINT projection_normalized_event_changes_kind_check CHECK (
        change_kind IN ('insert', 'canonicality_update')
    )
);

CREATE INDEX projection_normalized_event_changes_event_idx
    ON public.projection_normalized_event_changes (normalized_event_id, change_id DESC);

-- Historical normalized-event catch-up is worker-owned. This migration only
-- installs the forward change log and trigger; bootstrap/full replay establishes
-- the historical projection baseline and seeds projection apply cursors.

CREATE FUNCTION public.record_projection_normalized_event_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO public.projection_normalized_event_changes (
            normalized_event_id,
            changed_at,
            change_kind,
            canonicality_state
        )
        VALUES (
            NEW.normalized_event_id,
            NEW.observed_at,
            'insert',
            NEW.canonicality_state
        );
        RETURN NEW;
    END IF;

    IF OLD.canonicality_state IS DISTINCT FROM NEW.canonicality_state THEN
        INSERT INTO public.projection_normalized_event_changes (
            normalized_event_id,
            changed_at,
            change_kind,
            canonicality_state
        )
        VALUES (
            NEW.normalized_event_id,
            NEW.observed_at,
            'canonicality_update',
            NEW.canonicality_state
        );
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER normalized_events_projection_change_trigger
AFTER INSERT OR UPDATE OF canonicality_state ON public.normalized_events
FOR EACH ROW
EXECUTE FUNCTION public.record_projection_normalized_event_change();

CREATE TABLE public.projection_apply_cursors (
    cursor_name TEXT PRIMARY KEY,
    last_change_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT projection_apply_cursors_last_change_check CHECK (last_change_id >= 0)
);

CREATE TABLE public.projection_invalidations (
    projection TEXT NOT NULL,
    projection_key TEXT NOT NULL,
    key_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    generation BIGINT NOT NULL DEFAULT 0,
    first_change_id BIGINT,
    last_change_id BIGINT,
    first_normalized_event_id BIGINT,
    last_normalized_event_id BIGINT,
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    invalidated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claim_token UUID,
    claimed_at TIMESTAMPTZ,
    attempt_count BIGINT NOT NULL DEFAULT 0,
    last_failure_reason TEXT,
    last_failure_at TIMESTAMPTZ,
    PRIMARY KEY (projection, projection_key),
    CONSTRAINT projection_invalidations_generation_check CHECK (generation >= 0),
    CONSTRAINT projection_invalidations_attempt_check CHECK (attempt_count >= 0),
    CONSTRAINT projection_invalidations_change_order_check CHECK (
        first_change_id IS NULL
        OR last_change_id IS NULL
        OR first_change_id <= last_change_id
    ),
    CONSTRAINT projection_invalidations_event_order_check CHECK (
        first_normalized_event_id IS NULL
        OR last_normalized_event_id IS NULL
        OR first_normalized_event_id <= last_normalized_event_id
    ),
    CONSTRAINT projection_invalidations_claim_pair_check CHECK (
        (claim_token IS NULL AND claimed_at IS NULL)
        OR (claim_token IS NOT NULL AND claimed_at IS NOT NULL)
    )
);

CREATE INDEX projection_invalidations_pending_idx
    ON public.projection_invalidations (
        projection,
        last_changed_at,
        projection_key
    )
    WHERE claim_token IS NULL;

CREATE INDEX projection_invalidations_claim_idx
    ON public.projection_invalidations (claim_token)
    WHERE claim_token IS NOT NULL;
