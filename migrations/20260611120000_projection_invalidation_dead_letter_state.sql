CREATE TYPE public.projection_invalidation_state AS ENUM (
    'pending',
    'dead_letter'
);

ALTER TABLE public.projection_invalidations
    ADD COLUMN state public.projection_invalidation_state
        NOT NULL
        DEFAULT 'pending'::public.projection_invalidation_state;

CREATE TABLE public.projection_invalidation_dead_letters (
    projection TEXT NOT NULL,
    projection_key TEXT NOT NULL,
    key_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    generation BIGINT NOT NULL,
    attempt_count BIGINT NOT NULL,
    first_change_id BIGINT,
    last_change_id BIGINT,
    first_normalized_event_id BIGINT,
    last_normalized_event_id BIGINT,
    last_changed_at TIMESTAMPTZ NOT NULL,
    invalidated_at TIMESTAMPTZ NOT NULL,
    claim_token UUID,
    claimed_at TIMESTAMPTZ,
    last_failure_reason TEXT NOT NULL,
    last_failure_at TIMESTAMPTZ NOT NULL,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    state public.projection_invalidation_state
        NOT NULL
        DEFAULT 'dead_letter'::public.projection_invalidation_state,
    PRIMARY KEY (projection, projection_key, generation),
    CONSTRAINT projection_invalidation_dead_letters_state_check CHECK (
        state = 'dead_letter'::public.projection_invalidation_state
    ),
    CONSTRAINT projection_invalidation_dead_letters_generation_check CHECK (generation >= 0),
    CONSTRAINT projection_invalidation_dead_letters_attempt_check CHECK (attempt_count >= 0),
    CONSTRAINT projection_invalidation_dead_letters_change_order_check CHECK (
        first_change_id IS NULL
        OR last_change_id IS NULL
        OR first_change_id <= last_change_id
    ),
    CONSTRAINT projection_invalidation_dead_letters_event_order_check CHECK (
        first_normalized_event_id IS NULL
        OR last_normalized_event_id IS NULL
        OR first_normalized_event_id <= last_normalized_event_id
    ),
    CONSTRAINT projection_invalidation_dead_letters_claim_pair_check CHECK (
        (claim_token IS NULL AND claimed_at IS NULL)
        OR (claim_token IS NOT NULL AND claimed_at IS NOT NULL)
    )
);

CREATE INDEX projection_invalidation_dead_letters_lookup_idx
    ON public.projection_invalidation_dead_letters (
        projection,
        projection_key,
        dead_lettered_at
    );

CREATE FUNCTION public.advance_projection_invalidation_generation_after_dead_letter()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    next_generation BIGINT;
BEGIN
    SELECT MAX(dead_letter.generation) + 1
    INTO next_generation
    FROM public.projection_invalidation_dead_letters dead_letter
    WHERE dead_letter.projection = NEW.projection
      AND dead_letter.projection_key = NEW.projection_key;

    IF next_generation IS NOT NULL
       AND NEW.generation < next_generation THEN
        NEW.generation = next_generation;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER projection_invalidations_dead_letter_generation_insert_trigger
BEFORE INSERT ON public.projection_invalidations
FOR EACH ROW
EXECUTE FUNCTION public.advance_projection_invalidation_generation_after_dead_letter();

CREATE FUNCTION public.reset_projection_invalidation_retry_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.generation IS DISTINCT FROM OLD.generation THEN
        NEW.attempt_count = 0;
        NEW.last_failure_reason = NULL;
        NEW.last_failure_at = NULL;
        NEW.state = 'pending'::public.projection_invalidation_state;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER projection_invalidations_generation_retry_reset_trigger
BEFORE UPDATE OF generation ON public.projection_invalidations
FOR EACH ROW
EXECUTE FUNCTION public.reset_projection_invalidation_retry_state();
