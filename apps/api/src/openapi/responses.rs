use serde_json::{Map as JsonMap, json};
use sqlx::types::JsonValue;

use super::schemas::{nullable_ref_schema, schema_ref};

pub(super) fn declared_response_schema(
    data_schema: JsonValue,
    declared_state_schema: JsonValue,
) -> JsonValue {
    json!({
        "type": "object",
        "required": [
            "data",
            "declared_state",
            "verified_state",
            "coverage",
            "chain_positions",
            "consistency",
            "last_updated",
        ],
        "properties": {
            "data": data_schema,
            "declared_state": declared_state_schema,
            "verified_state": schema_ref("NullValue"),
            "provenance": schema_ref("Provenance"),
            "coverage": schema_ref("CoverageResponse"),
            "chain_positions": schema_ref("ChainPositions"),
            "consistency": schema_ref("Consistency"),
            "last_updated": {
                "type": "string",
                "format": "date-time",
            },
        },
    })
}

pub(super) fn mixed_response_schema(data_schema: JsonValue) -> JsonValue {
    json!({
        "type": "object",
        "required": [
            "data",
            "declared_state",
            "verified_state",
        ],
        "properties": {
            "data": data_schema,
            "declared_state": {
                "type": ["object", "null"],
                "additionalProperties": true,
            },
            "verified_state": {
                "type": ["object", "null"],
                "additionalProperties": true,
            },
            "provenance": schema_ref("Provenance"),
            "coverage": schema_ref("CoverageResponse"),
            "chain_positions": schema_ref("ChainPositions"),
            "consistency": schema_ref("Consistency"),
            "last_updated": {
                "type": "string",
                "format": "date-time",
            },
        },
    })
}

pub(super) fn primary_name_response_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": [
            "data",
            "declared_state",
            "verified_state",
            "coverage",
            "chain_positions",
            "consistency",
            "last_updated",
        ],
        "properties": {
            "data": schema_ref("PrimaryNameData"),
            "declared_state": nullable_ref_schema("PrimaryNameDeclaredState"),
            "verified_state": nullable_ref_schema("PrimaryNameVerifiedState"),
            "provenance": schema_ref("Provenance"),
            "coverage": schema_ref("CoverageResponse"),
            "chain_positions": schema_ref("ChainPositions"),
            "consistency": schema_ref("Consistency"),
            "last_updated": {
                "type": "string",
                "format": "date-time",
            },
        },
    })
}
pub(super) fn paginated_declared_response_schema(
    data_schema: JsonValue,
    declared_state_schema: JsonValue,
) -> JsonValue {
    let mut schema = declared_response_schema(data_schema, declared_state_schema);
    let object = schema
        .as_object_mut()
        .expect("declared response schema must be an object");
    object
        .get_mut("required")
        .and_then(JsonValue::as_array_mut)
        .expect("declared response schema must define required fields")
        .push(JsonValue::String("page".to_owned()));
    object
        .get_mut("properties")
        .and_then(JsonValue::as_object_mut)
        .expect("declared response schema must define properties")
        .insert("page".to_owned(), schema_ref("HistoryPageResponse"));
    schema
}

pub(super) fn openapi_json_get_operation(
    operation_id: &'static str,
    summary: &'static str,
    tag: &'static str,
    parameters: Vec<JsonValue>,
    request_schema: Option<&'static str>,
    success_schema: &'static str,
    include_bad_request: bool,
    include_not_found: bool,
) -> JsonValue {
    let mut responses = JsonMap::new();
    responses.insert(
        "200".to_owned(),
        json_response("Successful response", success_schema),
    );
    if include_bad_request {
        responses.insert(
            "400".to_owned(),
            json_response("Invalid request", "ErrorResponse"),
        );
    }
    if include_not_found {
        responses.insert(
            "404".to_owned(),
            json_response("Requested resource was not found", "ErrorResponse"),
        );
    }
    responses.insert(
        "500".to_owned(),
        json_response("Internal error", "ErrorResponse"),
    );

    let mut operation = json!({
        "operationId": operation_id,
        "summary": summary,
        "tags": [tag],
        "parameters": parameters,
        "responses": JsonValue::Object(responses),
    });
    if let Some(request_schema) = request_schema {
        operation
            .as_object_mut()
            .expect("OpenAPI operation must be an object")
            .insert(
                "requestBody".to_owned(),
                json!({
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": schema_ref(request_schema),
                        },
                    },
                }),
            );
    }
    operation
}

fn json_response(description: &'static str, schema_name: &'static str) -> JsonValue {
    json!({
        "description": description,
        "content": {
            "application/json": {
                "schema": schema_ref(schema_name),
            },
        },
    })
}

pub(super) trait OpenApiOperationExt {
    fn with_bad_request_description(self, description: &'static str) -> JsonValue;
    fn with_conflict_response(self) -> JsonValue;
}

impl OpenApiOperationExt for JsonValue {
    fn with_bad_request_description(mut self, description: &'static str) -> JsonValue {
        insert_error_response(&mut self, "400", description);
        self
    }

    fn with_conflict_response(mut self) -> JsonValue {
        insert_error_response(&mut self, "409", "Snapshot conflict or stale projection");
        self
    }
}

fn insert_error_response(
    operation: &mut JsonValue,
    status: &'static str,
    description: &'static str,
) {
    operation
        .get_mut("responses")
        .and_then(JsonValue::as_object_mut)
        .expect("OpenAPI operation must expose responses")
        .insert(
            status.to_owned(),
            json_response(description, "ErrorResponse"),
        );
}
