-- Phase 7 execution cache/outcome storage foundation: deterministic cache-keyed verified outcomes.

CREATE TABLE execution_cache_outcomes (
  execution_cache_key TEXT PRIMARY KEY,
  request_key TEXT NOT NULL,
  requested_chain_positions JSONB NOT NULL DEFAULT '[]'::JSONB,
  manifest_versions JSONB NOT NULL DEFAULT '[]'::JSONB,
  topology_version_boundary JSONB NOT NULL DEFAULT '{}'::JSONB,
  record_version_boundary JSONB NOT NULL DEFAULT '{}'::JSONB,
  execution_trace_id UUID NOT NULL REFERENCES execution_traces (execution_trace_id) ON DELETE CASCADE,
  request_type TEXT NOT NULL,
  namespace TEXT NOT NULL,
  outcome_payload JSONB,
  failure_payload JSONB,
  finished_at TIMESTAMPTZ NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (request_key <> ''),
  CHECK (
    jsonb_typeof(requested_chain_positions) = 'array'
    AND requested_chain_positions <> '[]'::JSONB
  ),
  CHECK (
    jsonb_typeof(manifest_versions) = 'array'
    AND manifest_versions <> '[]'::JSONB
  ),
  CHECK (
    jsonb_typeof(topology_version_boundary) = 'object'
    AND topology_version_boundary <> '{}'::JSONB
  ),
  CHECK (
    jsonb_typeof(record_version_boundary) = 'object'
    AND record_version_boundary <> '{}'::JSONB
  ),
  CHECK (outcome_payload IS NOT NULL OR failure_payload IS NOT NULL)
);

CREATE INDEX execution_cache_outcomes_execution_trace_idx
  ON execution_cache_outcomes (execution_trace_id);
