use serde_json::json;
use sqlx::types::JsonValue;

use crate::PUBLIC_NAMESPACES;

#[path = "schemas/primary_name.rs"]
mod primary_name;
#[path = "schemas/identity.rs"]
mod identity;
#[path = "schemas/identity_native.rs"]
mod identity_native;

use super::responses::{
    declared_response_schema, mixed_response_schema, paginated_declared_response_schema,
    primary_name_response_schema,
};
use identity::{
    identity_status_schema, indexing_status_response_schema, name_record_status_schema,
    public_status_response_schema,
};
use identity_native::{
    identity_lookup_input_schema, identity_lookup_page_schema, identity_lookup_response_schema,
    native_identity_record_schema, normalization_info_schema,
};
use primary_name::{
    primary_name_claimed_result_schema, primary_name_verified_result_provenance_schema,
    primary_name_verified_result_schema,
};

pub(super) fn openapi_components() -> JsonValue {
    json!({
        "schemas": {
            "JsonObject": json_object_schema(),
            "NullValue": json!({ "type": "null" }),
            "Consistency": json!({
                "type": "string",
                "enum": ["head", "safe", "finalized"],
            }),
            "Provenance": json!({
                "type": "object",
                "required": [
                    "normalized_event_ids",
                    "raw_fact_refs",
                    "manifest_versions",
                    "execution_trace_id",
                    "derivation_kind",
                ],
                "properties": {
                    "normalized_event_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "raw_fact_refs": {
                        "type": "array",
                        "items": {},
                    },
                    "manifest_versions": {
                        "type": "array",
                        "items": {},
                    },
                    "execution_trace_id": {
                        "type": ["string", "null"],
                    },
                    "derivation_kind": {
                        "type": "string",
                    },
                },
            }),
            "CoverageResponse": json!({
                "type": "object",
                "required": [
                    "status",
                    "exhaustiveness",
                    "source_classes_considered",
                    "enumeration_basis",
                    "unsupported_reason",
                ],
                "properties": {
                    "status": { "type": "string" },
                    "exhaustiveness": { "type": "string" },
                    "source_classes_considered": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "enumeration_basis": { "type": "string" },
                    "unsupported_reason": {
                        "type": ["string", "null"],
                    },
                },
            }),
            "ChainPositionResponse": json!({
                "type": "object",
                "required": ["chain_id", "block_number", "block_hash", "timestamp"],
                "properties": {
                    "chain_id": { "type": "string" },
                    "block_number": { "type": "integer" },
                    "block_hash": { "type": "string" },
                    "timestamp": {
                        "type": "string",
                        "format": "date-time",
                    },
                },
            }),
            "ChainPositions": json!({
                "type": "object",
                "additionalProperties": schema_ref("ChainPositionResponse"),
            }),
            "HistoryPageResponse": json!({
                "type": "object",
                "required": ["cursor", "next_cursor", "page_size", "sort"],
                "properties": {
                    "cursor": { "type": ["string", "null"] },
                    "next_cursor": { "type": ["string", "null"] },
                    "page_size": {
                        "type": "integer",
                        "minimum": 0,
                    },
                    "sort": { "type": "string" },
                },
            }),
            "ExactNameData": json!({
                "type": "object",
                "required": [
                    "logical_name_id",
                    "namespace",
                    "normalized_name",
                    "canonical_display_name",
                    "namehash",
                    "resource_id",
                    "token_lineage_id",
                    "binding_kind",
                ],
                "properties": {
                    "logical_name_id": { "type": "string" },
                    "namespace": { "type": "string" },
                    "normalized_name": { "type": "string" },
                    "canonical_display_name": { "type": "string" },
                    "namehash": { "type": "string" },
                    "resource_id": {
                        "type": ["string", "null"],
                        "format": "uuid",
                    },
                    "token_lineage_id": {
                        "type": ["string", "null"],
                        "format": "uuid",
                    },
                    "binding_kind": {
                        "type": ["string", "null"],
                    },
                },
            }),
            "ProfileNameData": json!({
                "type": "object",
                "required": ["name", "namespace", "namehash", "resource_id"],
                "properties": {
                    "name": { "type": "string" },
                    "namespace": { "type": "string" },
                    "namehash": { "type": "string" },
                    "resource_id": {
                        "type": ["string", "null"],
                        "format": "uuid",
                    },
                },
                "additionalProperties": false,
            }),
            "ResolverData": json!({
                "type": "object",
                "required": ["chain_id", "resolver_address"],
                "properties": {
                    "chain_id": { "type": "string" },
                    "resolver_address": { "type": "string" },
                },
            }),
            "PrimaryNameData": json!({
                "type": "object",
                "required": ["address", "namespace", "coin_type"],
                "properties": {
                    "address": { "type": "string" },
                    "namespace": {
                        "type": "string",
                        "enum": PUBLIC_NAMESPACES,
                    },
                    "coin_type": { "type": "string" },
                },
            }),
            "IdentityStatus": identity_status_schema(),
            "NameRecordStatus": name_record_status_schema(),
            "NativeIdentityRecord": native_identity_record_schema(),
            "NormalizationInfo": normalization_info_schema(),
            "IdentityLookupPage": identity_lookup_page_schema(),
            "IdentityLookupInput": identity_lookup_input_schema(),
            "IdentityLookupResponse": identity_lookup_response_schema(),
            "IndexingStatusResponse": indexing_status_response_schema(),
            "PublicStatusResponse": public_status_response_schema(),
            "PrimaryNameClaimedResult": primary_name_claimed_result_schema(),
            "PrimaryNameDeclaredState": json!({
                "type": "object",
                "required": ["claimed_primary_name"],
                "properties": {
                    "claimed_primary_name": schema_ref("PrimaryNameClaimedResult"),
                },
                "additionalProperties": false,
            }),
            "PrimaryNameVerifiedState": json!({
                "type": "object",
                "required": ["verified_primary_name"],
                "properties": {
                    "verified_primary_name": schema_ref("PrimaryNameVerifiedResult"),
                },
                "additionalProperties": false,
            }),
            "PrimaryNameVerifiedResult": primary_name_verified_result_schema(),
            "PrimaryNameVerifiedResultProvenance": primary_name_verified_result_provenance_schema(),
            "ExactNameResponse": declared_response_schema(
                schema_ref("ExactNameData"),
                schema_ref("JsonObject"),
            ),
            "ResolutionResponse": mixed_response_schema(schema_ref("ExactNameData")),
            "NameProfileResponse": mixed_response_schema(json!({
                "oneOf": [
                    schema_ref("ProfileNameData"),
                    schema_ref("ExactNameData"),
                ],
            })),
            "PrimaryNameResponse": primary_name_response_schema(),
            "CollectionResponse": paginated_declared_response_schema(
                json!({
                    "type": "array",
                    "items": schema_ref("JsonObject"),
                }),
                schema_ref("JsonObject"),
            ),
            "CompactMeta": compact_meta_schema(),
            "CompactDomainSummary": compact_domain_summary_schema(),
            "CompactRecordSummary": compact_record_summary_schema(),
            "CompactHistoryEvent": compact_history_event_schema(),
            "RoleRow": role_row_schema(),
            "ResourceLookupData": resource_lookup_data_schema(),
            "ResolverOverviewCompact": resolver_overview_compact_schema(),
            "CompactNamesResponse": compact_collection_response_schema(
                schema_ref("CompactDomainSummary"),
            ),
            "CompactNameRecordsResponse": compact_single_response_schema(
                schema_ref("CompactRecordSummary"),
            ),
            "CompactEventsResponse": compact_collection_response_schema(
                schema_ref("CompactHistoryEvent"),
            ),
            "CompactRolesResponse": compact_collection_response_schema(schema_ref("RoleRow")),
            "ResourceLookupResponse": compact_single_response_schema(
                schema_ref("ResourceLookupData"),
            ),
            "CompactResolverOverviewResponse": compact_single_response_schema(
                schema_ref("ResolverOverviewCompact"),
            ),
            "NamespaceData": json!({
                "type": "object",
                "required": ["namespace"],
                "properties": {
                    "namespace": {
                        "type": "string",
                        "enum": PUBLIC_NAMESPACES,
                    },
                },
            }),
            "NamespaceMetadataDeclaredState": json!({
                "type": "object",
                "required": [
                    "active_manifest_count",
                    "active_source_families",
                    "chains",
                    "normalizer_versions",
                ],
                "properties": {
                    "active_manifest_count": {
                        "type": "integer",
                        "minimum": 0,
                    },
                    "active_source_families": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "chains": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "normalizer_versions": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                },
            }),
            "NamespaceMetadataResponse": declared_response_schema(
                schema_ref("NamespaceData"),
                schema_ref("NamespaceMetadataDeclaredState"),
            ),
            "CapabilityFlag": json!({
                "type": "object",
                "required": ["status", "notes"],
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["unsupported", "shadow", "supported"],
                    },
                    "notes": {
                        "type": ["string", "null"],
                    },
                },
            }),
            "NamespaceManifestEntry": json!({
                "type": "object",
                "required": [
                    "manifest_version",
                    "source_family",
                    "chain",
                    "deployment_epoch",
                    "normalizer_version",
                    "capability_flags",
                ],
                "properties": {
                    "manifest_version": {
                        "type": "integer",
                        "minimum": 1,
                    },
                    "source_family": { "type": "string" },
                    "chain": { "type": "string" },
                    "deployment_epoch": { "type": "string" },
                    "normalizer_version": { "type": "string" },
                    "capability_flags": {
                        "type": "object",
                        "additionalProperties": schema_ref("CapabilityFlag"),
                    },
                },
            }),
            "NamespaceManifestsDeclaredState": json!({
                "type": "object",
                "required": ["manifests"],
                "properties": {
                    "manifests": {
                        "type": "array",
                        "items": schema_ref("NamespaceManifestEntry"),
                    },
                },
            }),
            "NamespaceManifestsResponse": declared_response_schema(
                schema_ref("NamespaceData"),
                schema_ref("NamespaceManifestsDeclaredState"),
            ),
            "HealthResponse": json!({
                "type": "object",
                "required": ["service", "phase", "status"],
                "properties": {
                    "service": { "type": "string" },
                    "phase": { "type": "string" },
                    "status": { "type": "string" },
                },
            }),
            "ErrorBody": json!({
                "type": "object",
                "required": ["code", "message", "details"],
                "properties": {
                    "code": { "type": "string" },
                    "message": { "type": "string" },
                    "details": {
                        "type": "object",
                        "additionalProperties": { "type": "string" },
                    },
                },
            }),
            "ErrorResponse": json!({
                "type": "object",
                "required": ["error"],
                "properties": {
                    "error": schema_ref("ErrorBody"),
                },
            }),
        },
    })
}
#[rustfmt::skip]
fn compact_collection_response_schema(item_schema: JsonValue) -> JsonValue {
    json!({ "type": "object", "required": ["data", "page"], "properties": { "data": { "type": "array", "items": item_schema }, "page": schema_ref("HistoryPageResponse"), "meta": schema_ref("CompactMeta") } })
}

#[rustfmt::skip]
fn compact_single_response_schema(data_schema: JsonValue) -> JsonValue {
    json!({ "type": "object", "required": ["data"], "properties": { "data": data_schema, "meta": schema_ref("CompactMeta") } })
}

#[rustfmt::skip]
fn compact_meta_schema() -> JsonValue {
    json!({ "type": "object", "properties": { "support_status": { "type": "string" }, "unsupported_filters": { "type": "array", "items": { "type": "string" } }, "unsupported_fields": { "type": "array", "items": { "type": "string" } }, "total_count": { "type": ["integer", "null"], "minimum": 0 }, "value_source": {}, "inventory_source": {} } })
}

#[rustfmt::skip]
fn compact_domain_summary_schema() -> JsonValue {
    compact_object_schema(&["namespace", "name", "normalized_name", "namehash"], &["namespace", "name", "normalized_name", "namehash", "labelhash", "token_id", "owner", "registrant", "created_at", "registration_date", "expiry_date", "resolver_address", "record_summaries", "subname_count", "record_count"])
}

#[rustfmt::skip]
fn compact_record_summary_schema() -> JsonValue {
    let fields = ["resolver_address", "text_records", "known_text_keys", "avatar", "content_hash", "coin_addresses"];
    compact_object_schema(&fields, &fields)
}

#[rustfmt::skip]
fn compact_history_event_schema() -> JsonValue {
    compact_object_schema(&["type", "name", "namespace", "block_number", "timestamp", "data"], &["type", "name", "namespace", "resource_id", "block_number", "timestamp", "transaction_hash", "log_index", "data"])
}

#[rustfmt::skip]
fn role_row_schema() -> JsonValue {
    compact_object_schema(&["account", "resource_id", "effective_powers"], &["account", "resource_hex", "resource_id", "name", "role_bitmap", "effective_powers"])
}

#[rustfmt::skip]
fn resource_lookup_data_schema() -> JsonValue {
    compact_object_schema(&["namespace", "name", "normalized_name", "resource_id", "resource_hex"], &["namespace", "name", "normalized_name", "resource_id", "resource_hex"])
}

#[rustfmt::skip]
fn resolver_overview_compact_schema() -> JsonValue {
    compact_object_schema(&["chain_id", "resolver_address", "counts"], &["chain_id", "resolver_address", "counts", "nodes", "aliases", "roles", "events"])
}

#[rustfmt::skip]
fn compact_object_schema(required: &[&str], fields: &[&str]) -> JsonValue {
    json!({ "type": "object", "required": required, "properties": compact_object_properties(fields) })
}

fn compact_object_properties(fields: &[&str]) -> JsonValue {
    let mut properties = serde_json::Map::new();
    for field in fields {
        properties.insert(
            (*field).to_owned(),
            json!({
                "type": ["object", "array", "string", "integer", "boolean", "null"],
            }),
        );
    }
    JsonValue::Object(properties)
}

pub(super) fn schema_ref(schema_name: &str) -> JsonValue {
    json!({
        "$ref": format!("#/components/schemas/{schema_name}"),
    })
}

pub(super) fn nullable_ref_schema(schema_name: &str) -> JsonValue {
    json!({
        "anyOf": [
            schema_ref(schema_name),
            schema_ref("NullValue"),
        ],
    })
}

fn json_object_schema() -> JsonValue {
    json!({
        "type": "object",
        "additionalProperties": true,
    })
}
