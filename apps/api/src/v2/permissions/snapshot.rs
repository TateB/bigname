use bigname_storage::NameCurrentRow;
use serde_json::Value;

use crate::{AppState, load_name_current_for_selected_snapshot};

use super::super::chains::{DeploymentProfile, deployment_profile_for_slug};
use super::super::{V2Error, V2Result};

pub(super) async fn load_name_row_for_snapshot(
    state: &AppState,
    namespace: &str,
    normalized_name: &str,
    selected_snapshot: &bigname_storage::SelectedSnapshot,
) -> V2Result<Option<NameCurrentRow>> {
    match load_name_current_for_selected_snapshot(
        &state.pool,
        namespace,
        normalized_name,
        selected_snapshot,
    )
    .await
    {
        Ok(row) => Ok(Some(row)),
        Err(error) if error.code == "not_found" => Ok(None),
        Err(error)
            if error.code == "stale"
                && name_filter_misses_selected_profile(
                    state,
                    namespace,
                    normalized_name,
                    selected_snapshot,
                )
                .await? =>
        {
            Ok(None)
        }
        Err(error) if error.code == "stale" => Err(V2Error::stale(
            "requested snapshot is not available for permissions",
        )),
        Err(_) => Err(V2Error::internal_error(format!(
            "failed to resolve current resource for name {namespace}/{normalized_name}"
        ))),
    }
}

async fn name_filter_misses_selected_profile(
    state: &AppState,
    namespace: &str,
    normalized_name: &str,
    selected_snapshot: &bigname_storage::SelectedSnapshot,
) -> V2Result<bool> {
    let logical_name_id = format!("{namespace}:{normalized_name}");
    let row = bigname_storage::load_name_current(&state.pool, &logical_name_id)
        .await
        .map_err(|_| {
            V2Error::internal_error(format!(
                "failed to resolve current resource for name {namespace}/{normalized_name}"
            ))
        })?;
    let Some(row) = row else {
        return Ok(true);
    };
    let Some(row_profiles) = deployment_profiles_from_chain_positions_value(&row.chain_positions)
    else {
        return Ok(false);
    };
    let selected_profiles = deployment_profiles_from_chain_ids(
        selected_snapshot
            .chain_positions
            .as_map()
            .values()
            .map(|position| position.chain_id.as_str()),
    )
    .unwrap_or_default();

    Ok(!row_profiles.is_empty()
        && !selected_profiles.is_empty()
        && row_profiles
            .iter()
            .all(|profile| !selected_profiles.contains(profile)))
}

fn deployment_profiles_from_chain_positions_value(
    chain_positions: &Value,
) -> Option<Vec<DeploymentProfile>> {
    let positions = chain_positions.as_object()?;
    deployment_profiles_from_chain_ids(positions.values().map(|position| {
        position
            .get("chain_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }))
}

fn deployment_profiles_from_chain_ids<'a>(
    chain_ids: impl IntoIterator<Item = &'a str>,
) -> Option<Vec<DeploymentProfile>> {
    let mut profiles = Vec::new();
    for chain_id in chain_ids {
        let profile = deployment_profile_for_slug(chain_id)?;
        if !profiles.contains(&profile) {
            profiles.push(profile);
        }
    }
    Some(profiles)
}
