mod lookup;
mod support;

use super::*;
pub(super) use lookup::identity_lookup;

pub(super) async fn public_status(
    State(state): State<AppState>,
) -> ApiResult<Json<PublicStatusResponse>> {
    Ok(Json(PublicStatusResponse {
        data: load_indexing_status_response(&state).await?,
    }))
}

async fn load_indexing_status_response(state: &AppState) -> ApiResult<IndexingStatusResponse> {
    let read = bigname_storage::load_indexing_status(&state.pool)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                error = ?load_error,
                "failed to load indexing status"
            );
            ApiError::internal_error("failed to load indexing status")
        })?;

    Ok(build_indexing_status_response(&read))
}
