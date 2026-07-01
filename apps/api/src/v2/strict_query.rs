use std::marker::PhantomData;

use axum::{
    extract::{FromRequestParts, Query},
    http::request::Parts,
};
use serde::de::DeserializeOwned;

use super::{QueryParams, RawQueryParams, V2Error, V2Result};

pub(crate) trait QueryParamAllowlist {
    const ALLOWED: &'static [&'static str];
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StrictQueryParams<A> {
    inner: QueryParams,
    _marker: PhantomData<A>,
}

impl<A> StrictQueryParams<A> {
    pub(crate) fn into_inner(self) -> QueryParams {
        self.inner
    }
}

impl<S, A> FromRequestParts<S> for StrictQueryParams<A>
where
    S: Send + Sync,
    A: QueryParamAllowlist + Send + Sync,
{
    type Rejection = V2Error;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self {
            inner: parse_raw_query_params_with_allowlist::<RawQueryParams, S>(
                parts,
                state,
                A::ALLOWED,
            )
            .await
            .and_then(QueryParams::try_from)?,
            _marker: PhantomData,
        })
    }
}

#[derive(Debug)]
pub(crate) struct NoQueryParams;

impl<S> FromRequestParts<S> for NoQueryParams
where
    S: Send + Sync,
{
    type Rejection = V2Error;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        reject_query_params_without_allowlist(parts)?;
        Ok(Self)
    }
}

pub(crate) async fn parse_raw_query_params_with_allowlist<T, S>(
    parts: &mut Parts,
    state: &S,
    allowed: &'static [&'static str],
) -> V2Result<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    reject_undocumented_query_params(parts, state, allowed).await?;
    let Query(raw) = Query::<T>::from_request_parts(parts, state)
        .await
        .map_err(|_| V2Error::invalid_input("query parameters are invalid"))?;
    Ok(raw)
}

async fn reject_undocumented_query_params<S>(
    parts: &mut Parts,
    state: &S,
    allowed: &'static [&'static str],
) -> V2Result<()>
where
    S: Send + Sync,
{
    if parts.uri.query().is_none_or(|query| query.is_empty()) {
        return Ok(());
    }
    if allowed.is_empty() {
        return Err(unsupported_query_params());
    }

    let Query(pairs) = Query::<Vec<(String, String)>>::from_request_parts(parts, state)
        .await
        .map_err(|_| V2Error::invalid_input("query parameters are invalid"))?;
    for (key, _) in pairs {
        if !allowed.contains(&key.as_str()) {
            return Err(V2Error::invalid_input(format!(
                "unknown query parameter: {key}"
            )));
        }
    }

    Ok(())
}

fn reject_query_params_without_allowlist(parts: &Parts) -> V2Result<()> {
    if parts.uri.query().is_some_and(|query| !query.is_empty()) {
        return Err(unsupported_query_params());
    }

    Ok(())
}

fn unsupported_query_params() -> V2Error {
    V2Error::invalid_input("query parameters are not supported on this route")
}
