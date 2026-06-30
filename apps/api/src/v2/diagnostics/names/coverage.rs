use axum::{
    Json,
    extract::{Path, State},
};
use serde_json::Value as JsonValue;

use crate::{AppState, responses::build_name_coverage_declared_state};

use super::{
    DiagnosticNameQueryParams, Envelope, V2Result, bind_diagnostic_path_name, diagnostic_envelope,
    resolve_diagnostic_name,
};

const MISSING_COVERAGE_UNSUPPORTED_REASON: &str = "name_coverage_unsupported_reason_missing";

pub(crate) async fn get_name_coverage_diagnostic(
    Path(input_name): Path<String>,
    params: DiagnosticNameQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<JsonValue>>> {
    let params = bind_diagnostic_path_name(input_name, params);
    let (row, selected_snapshot) = resolve_diagnostic_name(&state, &params).await?;
    let data = build_name_coverage_diagnostic_data(&row.coverage);

    diagnostic_envelope(data, &selected_snapshot)
}

fn build_name_coverage_diagnostic_data(coverage: &JsonValue) -> JsonValue {
    let mut data = build_name_coverage_declared_state(coverage);
    if data.get("status").and_then(JsonValue::as_str) == Some("unsupported")
        && data
            .get("unsupported_reason")
            .and_then(JsonValue::as_str)
            .filter(|value| !value.trim().is_empty())
            .is_none()
        && let Some(object) = data.as_object_mut()
    {
        object.insert(
            "unsupported_reason".to_owned(),
            JsonValue::String(MISSING_COVERAGE_UNSUPPORTED_REASON.to_owned()),
        );
    }
    data
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn coverage_builder_normalizes_declared_coverage() {
        let row = super::super::test_name_row();

        assert_eq!(
            build_name_coverage_declared_state(&row.coverage),
            json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["ens_v1_registry_l1"],
                "enumeration_basis": "exact_name",
                "unsupported_reason": null
            })
        );
    }

    #[test]
    fn coverage_builder_defaults_missing_fields_to_unsupported_shape() {
        assert_eq!(
            build_name_coverage_declared_state(&json!({})),
            json!({
                "status": "unsupported",
                "exhaustiveness": "not_applicable",
                "source_classes_considered": [],
                "enumeration_basis": "exact_name",
                "unsupported_reason": null
            })
        );
    }

    #[test]
    fn coverage_diagnostic_data_requires_unsupported_reason_for_unsupported_status() {
        assert_eq!(
            build_name_coverage_diagnostic_data(&json!({
                "status": "unsupported",
                "exhaustiveness": "not_applicable",
                "source_classes_considered": [],
                "enumeration_basis": "exact_name",
                "unsupported_reason": null
            })),
            json!({
                "status": "unsupported",
                "exhaustiveness": "not_applicable",
                "source_classes_considered": [],
                "enumeration_basis": "exact_name",
                "unsupported_reason": "name_coverage_unsupported_reason_missing"
            })
        );
    }
}
