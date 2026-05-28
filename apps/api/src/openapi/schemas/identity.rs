use serde_json::json;
use sqlx::types::JsonValue;

use super::schema_ref;

pub(super) fn identity_status_schema() -> JsonValue {
    json!({
        "type": "string",
        "enum": ["success", "not_found", "unsupported", "stale", "unnormalizable_input"],
    })
}

pub(super) fn name_record_status_schema() -> JsonValue {
    json!({
        "type": "string",
        "enum": ["success", "not_found", "unsupported", "stale"],
    })
}

pub(super) fn indexing_status_response_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["status", "chains"],
        "properties": {
            "status": {
                "type": "string",
                "enum": ["ready", "degraded", "stale"],
            },
            "chains": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "required": [
                        "canonical_block",
                        "safe_block",
                        "finalized_block",
                        "latest_projected_block",
                        "latest_projected_timestamp",
                        "projection_lag_blocks",
                        "projection_lag_seconds",
                    ],
                    "properties": {
                        "canonical_block": { "type": ["integer", "null"] },
                        "safe_block": { "type": ["integer", "null"] },
                        "finalized_block": { "type": ["integer", "null"] },
                        "latest_projected_block": { "type": ["integer", "null"] },
                        "latest_projected_timestamp": {
                            "type": ["string", "null"],
                            "format": "date-time",
                        },
                        "projection_lag_blocks": { "type": ["integer", "null"] },
                        "projection_lag_seconds": { "type": ["integer", "null"] },
                    },
                },
            },
        },
    })
}

pub(super) fn public_status_response_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["data"],
        "properties": {
            "data": schema_ref("IndexingStatusResponse"),
        },
        "additionalProperties": false,
    })
}
