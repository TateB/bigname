use super::*;

pub(super) fn primary_name_claimed_result_schema() -> JsonValue {
    json!({
        "oneOf": [
            json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "success",
                    },
                    "name": {
                        "type": "string",
                    },
                    "provenance": schema_ref("JsonObject"),
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "not_found",
                    },
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "not_found",
                    },
                    "provenance": schema_ref("JsonObject"),
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "unsupported",
                    },
                    "unsupported_reason": {
                        "type": "string",
                    },
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "unsupported",
                    },
                    "provenance": schema_ref("JsonObject"),
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status", "raw_claim_name", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "invalid_name",
                    },
                    "raw_claim_name": {
                        "type": "string",
                    },
                    "provenance": schema_ref("JsonObject"),
                },
                "additionalProperties": false,
            }),
        ],
    })
}

pub(super) fn primary_name_verified_result_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["status"],
        "properties": {
            "status": {
                "type": "string",
            },
            "provenance": schema_ref("PrimaryNameVerifiedResultProvenance"),
        },
        "additionalProperties": true,
    })
}

pub(super) fn primary_name_verified_result_provenance_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["manifest_versions", "execution_trace_id"],
        "properties": {
            "manifest_versions": {
                "type": "array",
                "items": {},
            },
            "execution_trace_id": {
                "type": "string",
            },
        },
        "additionalProperties": false,
    })
}
