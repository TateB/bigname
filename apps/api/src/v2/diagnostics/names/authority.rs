use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::NameCurrentRow;
use serde_json::{Value as JsonValue, json};

use crate::{AppState, responses::build_name_authority_control_explain_declared_state};

use super::{
    DiagnosticNameQueryParams, Envelope, V2Result, bind_diagnostic_path_name, diagnostic_envelope,
    resolve_diagnostic_name,
};

const PERMISSION_LINEAGE_UNSUPPORTED_REASON: &str =
    "permission_lineage_not_projected_on_name_current";

pub(crate) async fn get_name_authority_diagnostic(
    Path(input_name): Path<String>,
    params: DiagnosticNameQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<JsonValue>>> {
    let params = bind_diagnostic_path_name(input_name, params);
    let (row, selected_snapshot) = resolve_diagnostic_name(&state, &params).await?;
    let data = build_name_authority_diagnostic_data(&row);

    diagnostic_envelope(data, &selected_snapshot)
}

fn build_name_authority_diagnostic_data(row: &NameCurrentRow) -> JsonValue {
    let mut data = build_name_authority_control_explain_declared_state(row);
    let permission_lineage = projected_permission_lineage(row, &data)
        .unwrap_or_else(unsupported_permission_lineage_section);

    if let Some(object) = data.as_object_mut() {
        object.insert("permission_lineage".to_owned(), permission_lineage);
    }
    data
}

fn projected_permission_lineage(row: &NameCurrentRow, data: &JsonValue) -> Option<JsonValue> {
    [
        data.get("permission_lineage"),
        data.get("authority")
            .and_then(|authority| authority.get("permission_lineage")),
        row.declared_summary.get("permission_lineage"),
        row.declared_summary
            .get("authority")
            .and_then(|authority| authority.get("permission_lineage")),
    ]
    .into_iter()
    .flatten()
    .find(|value| value.is_object() || value.is_array())
    .cloned()
}

fn unsupported_permission_lineage_section() -> JsonValue {
    json!({
        "status": "unsupported",
        "unsupported_reason": PERMISSION_LINEAGE_UNSUPPORTED_REASON
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn authority_builder_projects_authority_fallback_and_control() {
        let row = super::super::test_name_row();

        assert_eq!(
            build_name_authority_control_explain_declared_state(&row),
            json!({
                "authority": {
                    "resource_id": "00000000-0000-0000-0000-000000002200",
                    "token_lineage_id": "00000000-0000-0000-0000-000000001100",
                    "binding_kind": "declared_registry_path"
                },
                "control": {
                    "registrant": "0x00000000000000000000000000000000000000aa",
                    "registry_owner": "0x00000000000000000000000000000000000000bb",
                    "latest_event_kind": "NameTransferred"
                }
            })
        );
    }

    #[test]
    fn authority_builder_keeps_projected_authority_section() {
        let mut row = super::super::test_name_row();
        row.declared_summary["authority"] = json!({
            "resource_id": "projected-resource",
            "permission_lineage": ["registry-owner"]
        });

        assert_eq!(
            build_name_authority_control_explain_declared_state(&row)["authority"],
            json!({
                "resource_id": "projected-resource",
                "permission_lineage": ["registry-owner"]
            })
        );
    }

    #[test]
    fn authority_diagnostic_data_marks_missing_permission_lineage_unsupported() {
        let row = super::super::test_name_row();

        assert_eq!(
            build_name_authority_diagnostic_data(&row)["permission_lineage"],
            json!({
                "status": "unsupported",
                "unsupported_reason": "permission_lineage_not_projected_on_name_current"
            })
        );
    }

    #[test]
    fn authority_diagnostic_data_reuses_projected_permission_lineage() {
        let mut row = super::super::test_name_row();
        row.declared_summary["authority"] = json!({
            "permission_lineage": ["registry-owner"]
        });

        assert_eq!(
            build_name_authority_diagnostic_data(&row)["permission_lineage"],
            json!(["registry-owner"])
        );
    }
}
