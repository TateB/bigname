use serde_json::json;
use sqlx::types::JsonValue;

use super::{nullable_ref_schema, schema_ref};

pub(super) fn identity_status_schema() -> JsonValue {
    json!({
        "type": "string",
        "enum": ["success", "not_found", "unsupported", "stale"],
    })
}

pub(super) fn identity_as_of_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["chain_positions", "as_of_timestamp"],
        "properties": {
            "chain_positions": {},
            "as_of_timestamp": {
                "type": ["string", "null"],
                "format": "date-time",
            },
        },
    })
}

pub(super) fn name_record_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": [
            "name",
            "namehash",
            "owner_address",
            "manager_address",
            "primary_address",
            "coin_type_addresses",
            "text_records",
            "resolver_address",
            "expiration",
            "token_id",
            "network",
            "as_of",
            "status",
            "unsupported_fields",
        ],
        "properties": {
            "name": { "type": "string" },
            "namehash": { "type": "string" },
            "owner_address": { "type": ["string", "null"] },
            "manager_address": { "type": ["string", "null"] },
            "primary_address": { "type": ["string", "null"] },
            "coin_type_addresses": {
                "type": "object",
                "additionalProperties": { "type": "string" },
            },
            "text_records": {
                "type": "object",
                "additionalProperties": { "type": "string" },
            },
            "resolver_address": { "type": ["string", "null"] },
            "expiration": { "type": ["integer", "null"] },
            "token_id": { "type": ["string", "null"] },
            "network": { "type": "string" },
            "as_of": schema_ref("IdentityAsOf"),
            "status": schema_ref("IdentityStatus"),
            "unsupported_fields": {
                "type": "array",
                "items": { "type": "string" },
            },
        },
    })
}

pub(super) fn reverse_name_record_schema() -> JsonValue {
    json!({
        "allOf": [
            schema_ref("NameRecord"),
            {
                "type": "object",
                "required": ["is_primary", "relation_facets"],
                "properties": {
                    "is_primary": { "type": "boolean" },
                    "relation_facets": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": [
                                "OWNED",
                                "MANAGED",
                                "REGISTRANT",
                                "EFFECTIVE_CONTROLLER"
                            ],
                        },
                    },
                },
            },
        ],
    })
}

pub(super) fn identity_pagination_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["has_more"],
        "properties": {
            "next_page_cursor": { "type": "string" },
            "total_count": {
                "type": "integer",
                "minimum": 0,
            },
            "has_more": { "type": "boolean" },
        },
    })
}

pub(super) fn identity_name_response_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["status", "record"],
        "properties": {
            "status": schema_ref("IdentityStatus"),
            "record": nullable_ref_schema("NameRecord"),
        },
    })
}

pub(super) fn forward_identity_batch_input_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["names"],
        "properties": {
            "names": {
                "type": "array",
                "items": { "type": "string" },
            },
        },
        "additionalProperties": false,
    })
}

pub(super) fn forward_identity_batch_response_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["results"],
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["input", "record", "status"],
                    "properties": {
                        "input": {
                            "type": "object",
                            "required": ["name"],
                            "properties": {
                                "name": { "type": "string" },
                            },
                        },
                        "record": nullable_ref_schema("NameRecord"),
                        "status": schema_ref("IdentityStatus"),
                    },
                },
            },
        },
    })
}

pub(super) fn reverse_names_input_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["address", "coin_type", "roles"],
        "properties": {
            "address": { "type": "string" },
            "coin_type": {
                "type": "integer",
                "minimum": 0,
            },
            "roles": {
                "type": "string",
                "enum": ["OWNED", "MANAGED", "BOTH"],
            },
        },
    })
}

pub(super) fn reverse_names_response_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["input", "records", "pagination"],
        "properties": {
            "input": schema_ref("ReverseNamesInput"),
            "records": {
                "type": "array",
                "items": schema_ref("ReverseNameRecord"),
            },
            "pagination": schema_ref("IdentityPagination"),
        },
    })
}

pub(super) fn reverse_identity_batch_input_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["inputs"],
        "properties": {
            "inputs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["address", "coin_type"],
                    "properties": {
                        "address": { "type": "string" },
                        "coin_type": {
                            "type": "integer",
                            "minimum": 0,
                        },
                        "roles": {
                            "type": "string",
                            "enum": ["OWNED", "MANAGED", "BOTH"],
                            "default": "BOTH",
                        },
                        "page_size": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": crate::MAX_PAGE_SIZE,
                        },
                        "page_cursor": { "type": "string" },
                    },
                    "additionalProperties": false,
                },
            },
        },
        "additionalProperties": false,
    })
}

pub(super) fn reverse_identity_batch_response_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["results"],
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [
                        "input",
                        "records",
                        "pagination",
                        "status"
                    ],
                    "properties": {
                        "input": schema_ref("ReverseNamesInput"),
                        "records": {
                            "type": "array",
                            "items": schema_ref("ReverseNameRecord"),
                        },
                        "pagination": schema_ref("IdentityPagination"),
                        "status": schema_ref("IdentityStatus"),
                    },
                },
            },
        },
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
