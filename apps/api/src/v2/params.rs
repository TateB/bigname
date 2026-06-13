use axum::{
    extract::{FromRequestParts, Query},
    http::request::Parts,
};
use serde::Deserialize;

use super::{
    error::{V2Error, V2Result},
    vocab::Finality,
};

pub(crate) const DEFAULT_PAGE_SIZE: u64 = 50;
pub(crate) const MAX_PAGE_SIZE: u64 = 200;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub(crate) struct RawQueryParams {
    pub(crate) at: Option<String>,
    pub(crate) finality: Option<String>,
    pub(crate) source: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) include: Option<String>,
    pub(crate) sort: Option<String>,
    pub(crate) order: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QueryParams {
    pub(crate) at: Option<AtSelector>,
    pub(crate) finality: Finality,
    pub(crate) source: RequestSource,
    pub(crate) namespace: Option<String>,
    pub(crate) include: Vec<String>,
    pub(crate) sort: Option<String>,
    pub(crate) order: Option<SortOrder>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AtSelector {
    Timestamp(String),
    SnapshotToken(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestSource {
    Indexed,
    Verified,
    Auto,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SortOrder {
    Asc,
    Desc,
}

impl<S> FromRequestParts<S> for QueryParams
where
    S: Send + Sync,
{
    type Rejection = V2Error;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(raw) = Query::<RawQueryParams>::from_request_parts(parts, state)
            .await
            .map_err(|_| V2Error::invalid_input("query parameters are invalid"))?;
        Self::try_from(raw)
    }
}

impl TryFrom<RawQueryParams> for QueryParams {
    type Error = V2Error;

    fn try_from(raw: RawQueryParams) -> Result<Self, Self::Error> {
        Ok(Self {
            at: raw.at.as_deref().map(parse_at).transpose()?,
            finality: parse_finality(raw.finality.as_deref())?,
            source: parse_source(raw.source.as_deref())?,
            namespace: trim_to_option(raw.namespace),
            include: parse_include(raw.include),
            sort: trim_to_option(raw.sort),
            order: raw.order.as_deref().map(parse_order).transpose()?,
            cursor: trim_to_option(raw.cursor),
            page_size: parse_page_size(raw.page_size)?,
        })
    }
}

fn parse_at(value: &str) -> V2Result<AtSelector> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid_parameter("at"));
    }

    if bigname_storage::parse_rfc3339_utc_timestamp(value).is_ok() {
        return Ok(AtSelector::Timestamp(value.to_owned()));
    }

    if is_url_safe_opaque_token(value) {
        return Ok(AtSelector::SnapshotToken(value.to_owned()));
    }

    Err(invalid_parameter("at"))
}

fn parse_finality(value: Option<&str>) -> V2Result<Finality> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("latest") => Ok(Finality::Latest),
        Some("safe") => Ok(Finality::Safe),
        Some("finalized") => Ok(Finality::Finalized),
        Some(_) => Err(invalid_parameter("finality")),
    }
}

fn parse_source(value: Option<&str>) -> V2Result<RequestSource> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("indexed") => Ok(RequestSource::Indexed),
        Some("verified") => Ok(RequestSource::Verified),
        Some("auto") => Ok(RequestSource::Auto),
        Some(_) => Err(invalid_parameter("source")),
    }
}

fn parse_order(value: &str) -> V2Result<SortOrder> {
    match value.trim() {
        "asc" => Ok(SortOrder::Asc),
        "desc" => Ok(SortOrder::Desc),
        _ => Err(invalid_parameter("order")),
    }
}

fn parse_page_size(value: Option<u64>) -> V2Result<u64> {
    match value {
        None => Ok(DEFAULT_PAGE_SIZE),
        Some(value @ 1..=MAX_PAGE_SIZE) => Ok(value),
        Some(_) => Err(V2Error::invalid_input(format!(
            "page_size must be between 1 and {MAX_PAGE_SIZE}"
        ))),
    }
}

fn parse_include(value: Option<String>) -> Vec<String> {
    value
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn trim_to_option(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn is_url_safe_opaque_token(value: &str) -> bool {
    value.bytes().all(|byte| {
        matches!(
            byte,
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~'
        )
    })
}

fn invalid_parameter(parameter: &'static str) -> V2Error {
    V2Error::invalid_input(format!("{parameter} is invalid"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::error::ErrorCode;

    fn parse(raw: RawQueryParams) -> V2Result<QueryParams> {
        QueryParams::try_from(raw)
    }

    #[test]
    fn page_size_above_max_is_rejected() {
        let error = parse(RawQueryParams {
            page_size: Some(MAX_PAGE_SIZE + 1),
            ..RawQueryParams::default()
        })
        .expect_err("oversized page must fail");

        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn bad_finality_source_and_order_are_rejected() {
        for raw in [
            RawQueryParams {
                finality: Some("pending".to_owned()),
                ..RawQueryParams::default()
            },
            RawQueryParams {
                source: Some("both".to_owned()),
                ..RawQueryParams::default()
            },
            RawQueryParams {
                order: Some("sideways".to_owned()),
                ..RawQueryParams::default()
            },
        ] {
            let error = parse(raw).expect_err("bad enum value must fail");
            assert_eq!(error.code(), ErrorCode::InvalidInput);
        }
    }

    #[test]
    fn at_selector_classifies_rfc3339_timestamp() {
        let params = parse(RawQueryParams {
            at: Some("2026-06-10T00:00:00Z".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("timestamp at selector must parse");

        assert_eq!(
            params.at,
            Some(AtSelector::Timestamp("2026-06-10T00:00:00Z".to_owned()))
        );
    }

    #[test]
    fn at_selector_classifies_opaque_snapshot_token() {
        let params = parse(RawQueryParams {
            at: Some("snapshot_abc-123".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("snapshot token at selector must parse");

        assert_eq!(
            params.at,
            Some(AtSelector::SnapshotToken("snapshot_abc-123".to_owned()))
        );
    }

    #[test]
    fn invalid_at_selector_is_rejected() {
        let error = parse(RawQueryParams {
            at: Some("not a token".to_owned()),
            ..RawQueryParams::default()
        })
        .expect_err("invalid at selector must fail");

        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }
}
