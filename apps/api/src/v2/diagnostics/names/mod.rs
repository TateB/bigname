use axum::{Json, extract::FromRequestParts, http::request::Parts};
use bigname_storage::{NameCurrentRow, SelectedSnapshot};
use serde::Deserialize;
use serde_json::{Map, Value as JsonValue};

use crate::{AppState, load_name_current_for_selected_snapshot, normalize_inferred_route_name};

use super::super::{
    Envelope, QueryParams, RawQueryParams, SnapshotReadResource, V2Error, V2Result,
    api_error_to_v2_for_resource, resolve_v2_snapshot_for, snapshot_meta,
    v2_exact_name_snapshot_scope_with_resolution_auxiliary,
};

mod authority;
mod binding;
mod coverage;
mod execution;
mod records;

pub(crate) use authority::get_name_authority_diagnostic;
pub(crate) use binding::get_name_binding_diagnostic;
pub(crate) use coverage::get_name_coverage_diagnostic;
pub(crate) use execution::get_name_execution_diagnostic;
pub(crate) use records::get_name_records_diagnostic;

const DIAGNOSTIC_NAME_QUERY_PARAMS: &[&str] = &["namespace", "at", "finality"];

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RawDiagnosticNameQueryParams {
    at: Option<String>,
    finality: Option<String>,
    namespace: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DiagnosticNameQueryParams {
    inner: QueryParams,
}

impl<S> FromRequestParts<S> for DiagnosticNameQueryParams
where
    S: Send + Sync,
{
    type Rejection = V2Error;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let raw = super::super::parse_raw_query_params_with_allowlist::<
            RawDiagnosticNameQueryParams,
            S,
        >(parts, state, DIAGNOSTIC_NAME_QUERY_PARAMS)
        .await?;
        Self::try_from(raw)
    }
}

impl TryFrom<RawDiagnosticNameQueryParams> for DiagnosticNameQueryParams {
    type Error = V2Error;

    fn try_from(raw: RawDiagnosticNameQueryParams) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: QueryParams::try_from(RawQueryParams {
                at: raw.at,
                finality: raw.finality,
                namespace: raw.namespace,
                ..RawQueryParams::default()
            })?,
        })
    }
}

async fn resolve_diagnostic_name(
    state: &AppState,
    params: &QueryParams,
) -> V2Result<(NameCurrentRow, SelectedSnapshot)> {
    resolve_diagnostic_name_with_resolution_auxiliary(state, params, false).await
}

async fn resolve_diagnostic_name_with_resolution_auxiliary(
    state: &AppState,
    params: &QueryParams,
    include_resolution_auxiliary: bool,
) -> V2Result<(NameCurrentRow, SelectedSnapshot)> {
    let input_name = params
        .name
        .as_deref()
        .ok_or_else(|| V2Error::internal_error("diagnostic name path parameter was not bound"))?;
    let normalized = normalize_inferred_route_name(input_name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());

    let scope = v2_exact_name_snapshot_scope_with_resolution_auxiliary(
        state,
        &namespace,
        params.at.as_ref(),
        include_resolution_auxiliary,
    )
    .await?;
    let selected_snapshot = resolve_v2_snapshot_for(
        &state.pool,
        &scope,
        params.at.as_ref(),
        params.finality,
        SnapshotReadResource::DiagnosticData,
    )
    .await?;
    let row = load_name_current_for_selected_snapshot(
        &state.pool,
        &namespace,
        &normalized.normalized_name,
        &selected_snapshot,
    )
    .await
    .map_err(|error| api_error_to_v2_for_resource(error, SnapshotReadResource::DiagnosticData))?;

    Ok((row, selected_snapshot))
}

fn bind_diagnostic_path_name(
    input_name: String,
    mut params: DiagnosticNameQueryParams,
) -> QueryParams {
    params.inner.name = Some(input_name);
    params.inner
}

fn diagnostic_envelope(
    data: JsonValue,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<Json<Envelope<JsonValue>>> {
    Ok(Json(Envelope {
        data,
        page: None,
        meta: snapshot_meta(selected_snapshot)?,
    }))
}

fn apply_diagnostics_dictionary_names(value: &mut JsonValue) -> V2Result<()> {
    match value {
        JsonValue::Object(object) => {
            apply_diagnostics_dictionary_object_names(object)?;
            for child in object.values_mut() {
                apply_diagnostics_dictionary_names(child)?;
            }
        }
        JsonValue::Array(items) => {
            for child in items {
                apply_diagnostics_dictionary_names(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn apply_diagnostics_dictionary_object_names(object: &mut Map<String, JsonValue>) -> V2Result<()> {
    rename_diagnostic_key(object, "normalized_name", "name")?;
    rename_diagnostic_key(object, "canonical_display_name", "display_name")?;
    rename_diagnostic_key(object, "resource_id", "registration_id")?;
    split_logical_name_id(object)?;
    Ok(())
}

fn rename_diagnostic_key(
    object: &mut Map<String, JsonValue>,
    source: &str,
    target: &str,
) -> V2Result<()> {
    if !object.contains_key(source) {
        return Ok(());
    }
    if object.contains_key(target) {
        return Err(diagnostics_dictionary_error());
    }
    let value = object
        .remove(source)
        .expect("source key existence checked above");
    object.insert(target.to_owned(), value);
    Ok(())
}

fn split_logical_name_id(object: &mut Map<String, JsonValue>) -> V2Result<()> {
    let Some(value) = object.get("logical_name_id") else {
        return Ok(());
    };
    if object.contains_key("namespace") || object.contains_key("name") {
        return Err(diagnostics_dictionary_error());
    }
    let Some((namespace, name)) = value.as_str().and_then(|value| value.split_once(':')) else {
        return Err(diagnostics_dictionary_error());
    };
    if namespace.is_empty() || name.is_empty() {
        return Err(diagnostics_dictionary_error());
    }
    let namespace = namespace.to_owned();
    let name = name.to_owned();
    object.remove("logical_name_id");
    object.insert("namespace".to_owned(), JsonValue::String(namespace));
    object.insert("name".to_owned(), JsonValue::String(name));
    Ok(())
}

fn diagnostics_dictionary_error() -> V2Error {
    V2Error::internal_error("failed to map diagnostics dictionary names")
}

#[cfg(test)]
fn test_name_row() -> NameCurrentRow {
    use serde_json::json;
    use sqlx::types::Uuid;

    NameCurrentRow {
        logical_name_id: "ens:alice.eth".to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "Alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "namehash:alice.eth".to_owned(),
        surface_binding_id: Some(Uuid::from_u128(0x3300)),
        resource_id: Some(Uuid::from_u128(0x2200)),
        token_lineage_id: Some(Uuid::from_u128(0x1100)),
        binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({
            "control": {
                "registrant": "0x00000000000000000000000000000000000000aa",
                "registry_owner": "0x00000000000000000000000000000000000000bb",
                "latest_event_kind": "NameTransferred"
            },
            "history": {
                "latest_event_kind": "NameTransferred"
            }
        }),
        provenance: json!({}),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ens_v1_registry_l1"],
            "enumeration_basis": "exact_name",
            "unsupported_reason": null
        }),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: bigname_storage::parse_rfc3339_utc_timestamp("2026-04-17T00:00:03Z")
            .expect("test timestamp must parse"),
    }
}

#[cfg(test)]
mod dictionary_tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn diagnostics_dictionary_mapper_keeps_pipeline_event_ids_and_renames_concepts() {
        let mut value = json!({
            "record_version_boundary": {
                "logical_name_id": "ens:alice.eth",
                "resource_id": "00000000-0000-0000-0000-000000002200",
                "normalized_event_id": 1200
            },
            "resolver_discovery_path": [
                {
                    "logical_name_id": "ens:alice.eth",
                    "canonical_display_name": "Alice.eth",
                    "resource_id": "00000000-0000-0000-0000-000000002200"
                }
            ]
        });

        apply_diagnostics_dictionary_names(&mut value).expect("diagnostics names must map");

        assert_eq!(
            value,
            json!({
                "record_version_boundary": {
                    "namespace": "ens",
                    "name": "alice.eth",
                    "registration_id": "00000000-0000-0000-0000-000000002200",
                    "normalized_event_id": 1200
                },
                "resolver_discovery_path": [
                    {
                        "namespace": "ens",
                        "name": "alice.eth",
                        "display_name": "Alice.eth",
                        "registration_id": "00000000-0000-0000-0000-000000002200"
                    }
                ]
            })
        );
    }

    #[test]
    fn diagnostics_dictionary_mapper_rejects_target_collisions() {
        let mut value = json!({
            "normalized_name": "alice.eth",
            "name": "existing.eth"
        });

        let error = apply_diagnostics_dictionary_names(&mut value)
            .expect_err("colliding dictionary rename must fail");

        assert_eq!(error.code(), crate::v2::ErrorCode::InternalError);
    }

    #[test]
    fn diagnostics_dictionary_mapper_rejects_logical_name_target_collisions() {
        let mut value = json!({
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens"
        });

        let error = apply_diagnostics_dictionary_names(&mut value)
            .expect_err("logical_name_id split collision must fail");

        assert_eq!(error.code(), crate::v2::ErrorCode::InternalError);
    }

    #[test]
    fn diagnostics_dictionary_mapper_rejects_malformed_logical_name_ids() {
        let mut value = json!({
            "logical_name_id": "alice.eth"
        });

        let error = apply_diagnostics_dictionary_names(&mut value)
            .expect_err("malformed logical_name_id must fail");

        assert_eq!(error.code(), crate::v2::ErrorCode::InternalError);
    }
}
