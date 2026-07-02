use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_manifests::{
    ActiveManifestVersion, CapabilityFlag, NamespaceManifestSnapshot,
    load_namespace_manifest_snapshot,
};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::{AppState, ensure_public_namespace};

use super::{Envelope, Meta, NoQueryParams, V2Error, V2Result, api_error_to_v2};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct DiagnosticNamespaceManifests {
    pub(crate) namespace: String,
    pub(crate) manifests: Vec<DiagnosticNamespaceManifest>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct DiagnosticNamespaceManifest {
    pub(crate) manifest_version: u64,
    pub(crate) source_family: String,
    pub(crate) chain: String,
    pub(crate) deployment_epoch: String,
    pub(crate) normalizer_version: String,
    pub(crate) capability_flags: BTreeMap<String, CapabilityFlag>,
}

pub(crate) async fn get_diagnostic_namespace_manifests(
    Path(namespace): Path<String>,
    _no_query: NoQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<DiagnosticNamespaceManifests>>> {
    ensure_public_namespace(&namespace).map_err(api_error_to_v2)?;

    let snapshot = load_namespace_manifest_snapshot(&state.pool, &namespace)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?load_error,
                "failed to load v2 diagnostic namespace manifests"
            );
            V2Error::internal_error(format!(
                "failed to load manifest snapshot for namespace {namespace}"
            ))
        })?;

    Ok(Json(Envelope {
        data: build_diagnostic_namespace_manifests(namespace, snapshot),
        page: None,
        meta: Meta::default(),
    }))
}

fn build_diagnostic_namespace_manifests(
    namespace: String,
    snapshot: NamespaceManifestSnapshot,
) -> DiagnosticNamespaceManifests {
    DiagnosticNamespaceManifests {
        namespace,
        manifests: snapshot.manifests.into_iter().map(Into::into).collect(),
    }
}

impl From<ActiveManifestVersion> for DiagnosticNamespaceManifest {
    fn from(value: ActiveManifestVersion) -> Self {
        Self {
            manifest_version: value.manifest_version,
            source_family: value.source_family,
            chain: value.chain,
            deployment_epoch: value.deployment_epoch,
            normalizer_version: value.normalizer_version,
            capability_flags: value.capability_flags,
        }
    }
}
