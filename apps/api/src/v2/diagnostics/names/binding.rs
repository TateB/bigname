use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::NameCurrentRow;
use serde_json::{Value as JsonValue, json};

use crate::{AppState, responses::build_name_surface_binding_explain_declared_state};

use super::{
    DiagnosticNameQueryParams, Envelope, V2Result, bind_diagnostic_path_name, diagnostic_envelope,
    resolve_diagnostic_name,
};

pub(crate) async fn get_name_binding_diagnostic(
    Path(input_name): Path<String>,
    params: DiagnosticNameQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<JsonValue>>> {
    let params = bind_diagnostic_path_name(input_name, params);
    let (row, selected_snapshot) = resolve_diagnostic_name(&state, &params).await?;
    let data = build_name_binding_diagnostic_data(&row);

    diagnostic_envelope(data, &selected_snapshot)
}

fn build_name_binding_diagnostic_data(row: &NameCurrentRow) -> JsonValue {
    let mut data = build_name_surface_binding_explain_declared_state(row);
    if let Some(object) = data.as_object_mut() {
        object.insert("anchors".to_owned(), binding_anchors(row));
    }
    data
}

fn binding_anchors(row: &NameCurrentRow) -> JsonValue {
    json!({
        "logical_name_id": &row.logical_name_id,
        "namehash": &row.namehash,
        "resource_id": row.resource_id.map(|value| value.to_string()),
        "token_lineage_id": row.token_lineage_id.map(|value| value.to_string())
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn binding_builder_projects_surface_binding_and_history() {
        let row = super::super::test_name_row();

        assert_eq!(
            build_name_surface_binding_explain_declared_state(&row),
            json!({
                "surface_binding": {
                    "surface_binding_id": "00000000-0000-0000-0000-000000003300",
                    "binding_kind": "declared_registry_path"
                },
                "history": {
                    "latest_event_kind": "NameTransferred"
                }
            })
        );
    }

    #[test]
    fn binding_builder_reports_unsupported_without_binding_summary() {
        let mut row = super::super::test_name_row();
        row.surface_binding_id = None;
        row.binding_kind = None;

        assert_eq!(
            build_name_surface_binding_explain_declared_state(&row)["surface_binding"],
            json!({
                "status": "unsupported",
                "unsupported_reason": "declared surface binding summary is not yet projected"
            })
        );
    }

    #[test]
    fn binding_diagnostic_data_adds_reconciliation_anchors() {
        let row = super::super::test_name_row();

        assert_eq!(
            build_name_binding_diagnostic_data(&row)["anchors"],
            json!({
                "logical_name_id": "ens:alice.eth",
                "namehash": "namehash:alice.eth",
                "resource_id": "00000000-0000-0000-0000-000000002200",
                "token_lineage_id": "00000000-0000-0000-0000-000000001100"
            })
        );
    }
}
