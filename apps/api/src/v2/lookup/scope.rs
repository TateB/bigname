use std::collections::BTreeSet;

use bigname_storage::{SnapshotPositionRequirement, SnapshotSelectionScope};
use tracing::error;

use crate::{AppState, PUBLIC_NAMESPACES};

use super::{super::chains::deployment_profile_for_slug, parse::ParsedNameLookup};
use crate::v2::{V2Error, V2Result, v2_exact_name_snapshot_scope};

pub(super) async fn lookup_snapshot_scope(
    state: &AppState,
    namespace: Option<&str>,
    name_inputs: &[ParsedNameLookup],
    has_address_inputs: bool,
) -> V2Result<Option<SnapshotSelectionScope>> {
    let has_valid_name_inputs = name_inputs.iter().any(|input| input.lookup.is_some());
    if !has_address_inputs && !has_valid_name_inputs {
        return Ok(None);
    }

    if let Some(namespace) = namespace {
        return v2_exact_name_snapshot_scope(state, namespace, None)
            .await
            .map(Some);
    }

    if has_address_inputs {
        return lookup_union_snapshot_scope(
            state,
            PUBLIC_NAMESPACES
                .iter()
                .map(|namespace| (*namespace).to_owned())
                .collect(),
        )
        .await
        .map(Some);
    }

    let namespaces = name_inputs
        .iter()
        .filter_map(parsed_name_lookup_namespace)
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();

    if namespaces.len() == 1 {
        let namespace = namespaces
            .iter()
            .next()
            .expect("length check ensures one namespace");
        return v2_exact_name_snapshot_scope(state, namespace, None)
            .await
            .map(Some);
    }

    let namespaces = if namespaces.is_empty() {
        PUBLIC_NAMESPACES
            .iter()
            .map(|namespace| (*namespace).to_owned())
            .collect()
    } else {
        namespaces.into_iter().collect()
    };
    lookup_union_snapshot_scope(state, namespaces)
        .await
        .map(Some)
}

async fn lookup_union_snapshot_scope(
    state: &AppState,
    namespaces: Vec<String>,
) -> V2Result<SnapshotSelectionScope> {
    let mut requirements = Vec::new();
    for namespace in namespaces {
        let scope = v2_exact_name_snapshot_scope(state, &namespace, None).await?;
        requirements.extend(scope.required_positions().iter().cloned());
    }
    validate_lookup_single_deployment_profile(&requirements)?;

    SnapshotSelectionScope::new(requirements, None).map_err(|error| {
        error!(
            service = "api",
            message = %error.message(),
            "failed to build v2 lookup snapshot scope"
        );
        V2Error::internal_error("failed to build lookup snapshot scope")
    })
}

fn parsed_name_lookup_namespace(input: &ParsedNameLookup) -> Option<&str> {
    input
        .lookup
        .as_ref()
        .and_then(|lookup| lookup.logical_name_id.split_once(':'))
        .map(|(namespace, _)| namespace)
}

fn validate_lookup_single_deployment_profile(
    requirements: &[SnapshotPositionRequirement],
) -> V2Result<()> {
    let mut profile = None;
    for requirement in requirements {
        let requirement_profile =
            deployment_profile_for_slug(&requirement.chain_id).ok_or_else(|| {
                V2Error::internal_error("snapshot scope contains an unregistered deployment chain")
            })?;
        if profile.is_some_and(|profile| profile != requirement_profile) {
            return Err(V2Error::conflict(
                "snapshot selector cannot form one canonical snapshot across deployment profiles",
            ));
        }
        profile = Some(requirement_profile);
    }

    Ok(())
}
