use serde_json::json;
use sqlx::types::JsonValue;

use super::{nullable_ref_schema, schema_ref};

pub(super) fn normalization_info_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["changed", "input_name", "reason"],
        "properties": {
            "changed": { "type": "boolean" },
            "input_name": { "type": "string" },
            "reason": {
                "type": "string",
                "enum": ["case_normalized", "invalid_normalized_name"],
            },
        },
        "additionalProperties": false,
    })
}

pub(super) fn native_identity_record_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["name", "namespace", "namehash", "network", "status"],
        "properties": {
            "name": { "type": "string" },
            "namespace": { "type": "string" },
            "namehash": { "type": "string" },
            "owner_address": { "type": "string" },
            "manager_address": { "type": "string" },
            "primary_address": { "type": "string" },
            "coin_type_addresses": {
                "type": "object",
                "additionalProperties": { "type": "string" },
            },
            "text_records": {
                "type": "object",
                "additionalProperties": { "type": "string" },
            },
            "resolver_address": { "type": "string" },
            "expiration": { "type": "integer" },
            "token_id": { "type": "string" },
            "network": { "type": "string" },
            "is_primary": { "type": "boolean" },
            "relation_facets": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": [
                        "owned",
                        "managed",
                        "registrant",
                        "effective_controller"
                    ],
                },
            },
            "status": schema_ref("NameRecordStatus"),
            "unsupported_fields": {
                "type": "array",
                "items": { "type": "string" },
            },
        },
        "additionalProperties": false,
    })
}

pub(super) fn identity_lookup_input_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["inputs"],
        "properties": {
            "profile": {
                "type": "string",
                "enum": ["feed", "detail", "shadow"],
                "default": "detail",
            },
            "namespace": {
                "type": "string",
                "enum": ["auto", "public", "ens", "basenames"],
                "default": "public",
            },
            "inputs": {
                "type": "array",
                "maxItems": 1000,
                "items": {
                    "type": "object",
                    "required": ["id", "kind"],
                    "properties": {
                        "id": { "type": "string" },
                        "kind": {
                            "type": "string",
                            "enum": ["name", "address"],
                        },
                        "name": { "type": "string" },
                        "address": { "type": "string" },
                        "coin_type": {
                            "type": "integer",
                            "minimum": 0,
                        },
                        "roles": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": [
                                    "owned",
                                    "managed",
                                    "registrant",
                                    "effective_controller",
                                    "both",
                                    "any"
                                ],
                            },
                        },
                        "page_size": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": crate::MAX_PAGE_SIZE,
                            "default": 1,
                        },
                        "cursor": { "type": ["string", "null"] },
                    },
                    "additionalProperties": false,
                },
            },
        },
        "additionalProperties": false,
    })
}

pub(super) fn identity_lookup_page_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["next_cursor", "total_count", "has_more"],
        "properties": {
            "next_cursor": { "type": ["string", "null"] },
            "total_count": {
                "type": ["integer", "null"],
                "minimum": 0,
            },
            "has_more": { "type": "boolean" },
        },
        "additionalProperties": false,
    })
}

pub(super) fn identity_lookup_response_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["results"],
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["id", "kind", "status", "input"],
                    "properties": {
                        "id": { "type": "string" },
                        "kind": {
                            "type": "string",
                            "enum": ["name", "address"],
                        },
                        "status": schema_ref("IdentityStatus"),
                        "input": {
                            "type": "object",
                            "properties": {
                                "name": { "type": "string" },
                                "address": { "type": "string" },
                                "coin_type": {
                                    "type": "integer",
                                    "minimum": 0,
                                },
                                "roles": {
                                    "type": "array",
                                    "items": { "type": "string" },
                                },
                            },
                            "additionalProperties": false,
                        },
                        "normalization": schema_ref("NormalizationInfo"),
                        "record": nullable_ref_schema("NativeIdentityRecord"),
                        "records": {
                            "type": "array",
                            "items": schema_ref("NativeIdentityRecord"),
                        },
                        "page": schema_ref("IdentityLookupPage"),
                    },
                    "additionalProperties": false,
                },
            },
        },
        "additionalProperties": false,
    })
}
