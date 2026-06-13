-- Canonicalize the addr selector identity boundary introduced on 2026-06-12.
-- Existing rows with leading-zero addr selectors were admitted before the API
-- and execution write gates agreed that addr:060 and addr:60 are the same
-- selector. Drop reusable verified-resolution cache outcomes while retaining
-- durable traces/steps and declared projection rows for audit/rebuild.

CREATE OR REPLACE FUNCTION pg_temp.bigname_noncanonical_addr_selector(record_key TEXT)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE
        WHEN record_key !~ '^addr:' THEN FALSE
        WHEN substring(record_key FROM 6) !~ '^[0-9]+$' THEN TRUE
        WHEN substring(record_key FROM 6) <> (substring(record_key FROM 6)::NUMERIC)::TEXT THEN TRUE
        WHEN substring(record_key FROM 6)::NUMERIC > 18446744073709551615::NUMERIC THEN TRUE
        ELSE FALSE
    END
$$;

DELETE FROM public.execution_cache_outcomes outcome
USING public.execution_traces trace
WHERE outcome.execution_trace_id = trace.execution_trace_id
  AND outcome.request_type = 'verified_resolution'
  AND (
    EXISTS (
      SELECT 1
      FROM regexp_matches(outcome.request_key, '^[^:]+:[^:]+:(.*)$') AS selector_part,
           string_to_table(selector_part[1], ',') AS record_key(value)
      WHERE pg_temp.bigname_noncanonical_addr_selector(record_key.value)
    )
    OR EXISTS (
      SELECT 1
      FROM jsonb_path_query(
          COALESCE(outcome.outcome_payload, '{}'::jsonb),
          '$.verified_queries[*].record_key'
      ) AS record_key
      WHERE pg_temp.bigname_noncanonical_addr_selector(record_key #>> '{}')
    )
    OR EXISTS (
      SELECT 1
      FROM jsonb_path_query(trace.request_metadata, '$.record_keys[*]') AS record_key
      WHERE pg_temp.bigname_noncanonical_addr_selector(record_key #>> '{}')
    )
    OR EXISTS (
      SELECT 1
      FROM jsonb_path_query(trace.request_metadata, '$.record_key') AS record_key
      WHERE pg_temp.bigname_noncanonical_addr_selector(record_key #>> '{}')
    )
    OR EXISTS (
      SELECT 1
      FROM jsonb_path_query(
          COALESCE(trace.final_payload, '{}'::jsonb),
          '$.verified_queries[*].record_key'
      ) AS record_key
      WHERE pg_temp.bigname_noncanonical_addr_selector(record_key #>> '{}')
    )
  );
