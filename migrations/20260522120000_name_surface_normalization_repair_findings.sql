CREATE TABLE name_surface_normalization_repair_findings (
    logical_name_id TEXT NOT NULL,
    expected_normalizer_version TEXT NOT NULL,
    finding_kind TEXT NOT NULL,
    current_normalizer_version TEXT NOT NULL,
    namespace TEXT NOT NULL,
    input_name TEXT NOT NULL,
    current_normalized_name TEXT NOT NULL,
    candidate_logical_name_id TEXT,
    candidate_normalized_name TEXT,
    error_message TEXT,
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (expected_normalizer_version, logical_name_id),
    CHECK (expected_normalizer_version <> ''),
    CHECK (finding_kind IN ('rejected', 'incompatible')),
    CHECK (
        finding_kind <> 'rejected'
        OR error_message IS NOT NULL
    ),
    CHECK (
        finding_kind <> 'incompatible'
        OR candidate_logical_name_id IS NOT NULL
    )
);

CREATE INDEX name_surface_normalization_repair_findings_kind_idx
    ON name_surface_normalization_repair_findings (
        expected_normalizer_version,
        finding_kind,
        logical_name_id
    );
