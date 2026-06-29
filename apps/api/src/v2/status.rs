use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{FromRequestParts, State},
    http::request::Parts,
};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::AppState;

use super::{Envelope, Meta, OpsStatus, V2Error, V2Result, slug_to_numeric};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct StatusData {
    pub(crate) status: OpsStatus,
    pub(crate) chains: BTreeMap<String, ChainStatus>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ChainStatus {
    pub(crate) latest_block: Option<i64>,
    pub(crate) indexed_block: Option<i64>,
    pub(crate) safe_block: Option<i64>,
    pub(crate) finalized_block: Option<i64>,
    pub(crate) lag_blocks: Option<i64>,
    pub(crate) lag_seconds: Option<i64>,
    pub(crate) status: OpsStatus,
}

#[derive(Debug)]
pub(crate) struct NoQueryParams;

impl<S> FromRequestParts<S> for NoQueryParams
where
    S: Send + Sync,
{
    type Rejection = V2Error;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if parts.uri.query().is_some_and(|query| !query.is_empty()) {
            return Err(V2Error::invalid_input(
                "query parameters are not supported on this route",
            ));
        }

        Ok(Self)
    }
}

pub(crate) async fn get_status(
    _no_query: NoQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<StatusData>>> {
    let read = bigname_storage::load_indexing_status(&state.pool)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                error = ?load_error,
                "failed to load v2 indexing status"
            );
            V2Error::internal_error("failed to load indexing status")
        })?;

    Ok(Json(Envelope {
        data: build_status_data(&read)?,
        page: None,
        meta: Meta::default(),
    }))
}

fn build_status_data(read: &bigname_storage::IndexingStatusRead) -> V2Result<StatusData> {
    let mut chains = BTreeMap::new();
    for row in &read.chains {
        chains.insert(status_chain_key(&row.chain_id)?, build_chain_status(row));
    }

    Ok(StatusData {
        status: root_status(chains.values(), read.has_unscoped_pending_invalidations),
        chains,
    })
}

fn status_chain_key(storage_chain_id: &str) -> V2Result<String> {
    let numeric = slug_to_numeric(storage_chain_id).ok_or_else(|| {
        V2Error::internal_error(format!(
            "indexing status row uses unmapped chain_id {storage_chain_id}"
        ))
    })?;

    Ok(numeric.to_string())
}

fn build_chain_status(row: &bigname_storage::IndexingStatusChainRow) -> ChainStatus {
    let lag_blocks = row
        .canonical_block
        .zip(row.latest_projected_block)
        .map(|(canonical, projected)| canonical.saturating_sub(projected));
    let lag_seconds = row
        .canonical_timestamp
        .zip(row.latest_projected_timestamp)
        .map(|(canonical, projected)| (canonical - projected).whole_seconds().max(0));
    let status = chain_status(row.canonical_block, row.latest_projected_block, lag_blocks);

    ChainStatus {
        latest_block: row.canonical_block,
        indexed_block: row.latest_projected_block,
        safe_block: row.safe_block,
        finalized_block: row.finalized_block,
        lag_blocks,
        lag_seconds,
        status,
    }
}

fn chain_status(
    latest_block: Option<i64>,
    indexed_block: Option<i64>,
    lag_blocks: Option<i64>,
) -> OpsStatus {
    match (latest_block, indexed_block, lag_blocks) {
        (Some(_), Some(_), Some(0)) => OpsStatus::Ready,
        (Some(_), Some(_), Some(_)) => OpsStatus::Stale,
        _ => OpsStatus::Degraded,
    }
}

fn root_status<'a>(
    chains: impl Iterator<Item = &'a ChainStatus>,
    has_unscoped_pending_invalidations: bool,
) -> OpsStatus {
    let mut saw_degraded = has_unscoped_pending_invalidations;
    let mut saw_chain = false;

    for chain in chains {
        saw_chain = true;
        match chain.status {
            OpsStatus::Stale => return OpsStatus::Stale,
            OpsStatus::Degraded => saw_degraded = true,
            OpsStatus::Ready => {}
        }
    }

    if saw_degraded || !saw_chain {
        OpsStatus::Degraded
    } else {
        OpsStatus::Ready
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        http::{Request, StatusCode},
        response::IntoResponse,
    };
    use sqlx::types::time::OffsetDateTime;

    use super::*;

    #[test]
    fn status_data_maps_chains_by_numeric_chain_id_and_derives_chain_statuses() {
        let read = bigname_storage::IndexingStatusRead {
            chains: vec![
                row(
                    bigname_storage::ETHEREUM_MAINNET_CHAIN_ID,
                    Some(100),
                    Some(100),
                    Some(90),
                    Some(80),
                ),
                row(
                    bigname_storage::BASE_MAINNET_CHAIN_ID,
                    None,
                    Some(50),
                    Some(40),
                    Some(30),
                ),
                row("base-sepolia", Some(100), Some(95), Some(90), Some(80)),
            ],
            has_unscoped_pending_invalidations: false,
        };

        let data = build_status_data(&read).expect("known storage chain slugs must map");

        assert_eq!(data.status, OpsStatus::Stale);
        assert_eq!(
            data.chains.keys().collect::<Vec<_>>(),
            vec!["1", "8453", "84532"]
        );
        assert_eq!(data.chains["1"].status, OpsStatus::Ready);
        assert_eq!(data.chains["1"].latest_block, Some(100));
        assert_eq!(data.chains["1"].indexed_block, Some(100));
        assert_eq!(data.chains["1"].lag_blocks, Some(0));
        assert_eq!(data.chains["1"].lag_seconds, Some(0));
        assert_eq!(data.chains["8453"].status, OpsStatus::Degraded);
        assert_eq!(data.chains["8453"].lag_blocks, None);
        assert_eq!(data.chains["84532"].status, OpsStatus::Stale);
        assert_eq!(data.chains["84532"].lag_blocks, Some(5));
        assert_eq!(data.chains["84532"].lag_seconds, Some(50));
    }

    #[test]
    fn status_data_rejects_unmapped_storage_chain_slugs() {
        let read = bigname_storage::IndexingStatusRead {
            chains: vec![row(
                "future-mainnet",
                Some(100),
                Some(100),
                Some(90),
                Some(80),
            )],
            has_unscoped_pending_invalidations: false,
        };

        let error = build_status_data(&read).expect_err("unknown storage slug must fail loudly");

        assert_eq!(error.envelope().error.code, "internal_error");
    }

    #[test]
    fn root_status_aggregates_stale_degraded_invalidations_and_empty_reads() {
        let ready = ChainStatus {
            latest_block: Some(100),
            indexed_block: Some(100),
            safe_block: Some(90),
            finalized_block: Some(80),
            lag_blocks: Some(0),
            lag_seconds: Some(0),
            status: OpsStatus::Ready,
        };
        let degraded = ChainStatus {
            latest_block: None,
            indexed_block: Some(100),
            safe_block: Some(90),
            finalized_block: Some(80),
            lag_blocks: None,
            lag_seconds: None,
            status: OpsStatus::Degraded,
        };
        let stale = ChainStatus {
            latest_block: Some(100),
            indexed_block: Some(99),
            safe_block: Some(90),
            finalized_block: Some(80),
            lag_blocks: Some(1),
            lag_seconds: Some(10),
            status: OpsStatus::Stale,
        };

        assert_eq!(root_status([&ready].into_iter(), false), OpsStatus::Ready);
        assert_eq!(
            root_status([&ready, &degraded].into_iter(), false),
            OpsStatus::Degraded
        );
        assert_eq!(
            root_status([&ready, &stale, &degraded].into_iter(), false),
            OpsStatus::Stale
        );
        assert_eq!(root_status([&ready].into_iter(), true), OpsStatus::Degraded);
        assert_eq!(root_status([].into_iter(), false), OpsStatus::Degraded);
    }

    #[tokio::test]
    async fn no_query_params_rejects_status_controls_with_bad_request() {
        let request = Request::builder()
            .uri("/v2/status?finality=safe")
            .body(())
            .expect("request must build");
        let (mut parts, ()) = request.into_parts();

        let error = NoQueryParams::from_request_parts(&mut parts, &())
            .await
            .expect_err("status route must reject query params");
        let envelope = error.envelope();
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(envelope.error.code, "invalid_input");
    }

    fn row(
        chain_id: &str,
        canonical_block: Option<i64>,
        latest_projected_block: Option<i64>,
        safe_block: Option<i64>,
        finalized_block: Option<i64>,
    ) -> bigname_storage::IndexingStatusChainRow {
        bigname_storage::IndexingStatusChainRow {
            chain_id: chain_id.to_owned(),
            canonical_block,
            safe_block,
            finalized_block,
            canonical_timestamp: canonical_block.map(timestamp_for_block),
            latest_projected_block,
            latest_projected_timestamp: latest_projected_block.map(timestamp_for_block),
        }
    }

    fn timestamp_for_block(block: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(block * 10).expect("test timestamp must be valid")
    }
}
