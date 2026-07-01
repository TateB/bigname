use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{FromRequestParts, Query, State},
    http::request::Parts,
};
use bigname_storage::{
    NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentListFilter, NameCurrentListOrder,
    NameCurrentListRow, NameCurrentListSort, SnapshotSelectionScope,
};
use serde::{Deserialize, Serialize};

use crate::{AppState, PUBLIC_NAMESPACES};

use super::{
    AtSelector, CursorPayload, Envelope, Finality, Meta, Page, QueryParams, RawQueryParams,
    RegistrationStatus, V2Error, V2Result, as_of_meta, decode, encode, encode_at_token,
    format_timestamp, name_record::name_registration_fields, resolve_v2_snapshot,
    v2_exact_name_snapshot_scope,
};

const SEARCH_SORT: &str = "name_asc";
const Q_FILTER_KEY: &str = "q";
const MATCH_FILTER_KEY: &str = "match";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const NONE_FILTER_VALUE: &str = "";
const DISPLAY_NAME_CURSOR_KEY: &str = "display_name";
const NORMALIZED_NAME_CURSOR_KEY: &str = "normalized_name";
const NAMEHASH_CURSOR_KEY: &str = "namehash";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct SearchName {
    pub(crate) name: String,
    pub(crate) display_name: String,
    pub(crate) namespace: String,
    pub(crate) namehash: String,
    pub(crate) owner: Option<String>,
    pub(crate) registrant: Option<String>,
    pub(crate) registration_status: RegistrationStatus,
    pub(crate) registered_at: Option<String>,
    pub(crate) created_at: Option<String>,
    pub(crate) expires_at: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SearchQueryParams {
    pub(crate) at: Option<AtSelector>,
    pub(crate) finality: Finality,
    pub(crate) q: String,
    pub(crate) match_mode: SearchMatch,
    pub(crate) namespace: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SearchMatch {
    Prefix,
    Contains,
}

impl SearchMatch {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Prefix => "prefix",
            Self::Contains => "contains",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RawSearchQueryParams {
    q: Option<String>,
    #[serde(rename = "match")]
    match_mode: Option<String>,
    namespace: Option<String>,
    at: Option<String>,
    finality: Option<String>,
    cursor: Option<String>,
    page_size: Option<u64>,
}

impl<S> FromRequestParts<S> for SearchQueryParams
where
    S: Send + Sync,
{
    type Rejection = V2Error;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(raw) = Query::<RawSearchQueryParams>::from_request_parts(parts, state)
            .await
            .map_err(|_| V2Error::invalid_input("query parameters are invalid"))?;
        Self::try_from(raw)
    }
}

impl TryFrom<RawSearchQueryParams> for SearchQueryParams {
    type Error = V2Error;

    fn try_from(raw: RawSearchQueryParams) -> Result<Self, Self::Error> {
        let shared = QueryParams::try_from(RawQueryParams {
            at: raw.at,
            finality: raw.finality,
            namespace: raw.namespace,
            cursor: raw.cursor,
            page_size: raw.page_size,
            ..RawQueryParams::default()
        })?;

        if let Some(namespace) = shared.namespace.as_deref() {
            validate_namespace(namespace)?;
        }

        Ok(Self {
            at: shared.at,
            finality: shared.finality,
            q: parse_q(raw.q)?,
            match_mode: parse_match(raw.match_mode.as_deref())?,
            namespace: shared.namespace,
            cursor: shared.cursor,
            page_size: shared.page_size,
        })
    }
}

pub(crate) async fn get_search(
    params: SearchQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<SearchName>>>> {
    // Search is anchorless: `namespace` filters rows, while `at`/`finality`
    // select response metadata and cursor binding for the latest-row read.
    let scope = search_snapshot_scope(&state, params.namespace.as_deref()).await?;
    let selected_snapshot =
        resolve_v2_snapshot(&state.pool, &scope, params.at.as_ref(), params.finality).await?;
    let snapshot_token = encode_at_token(&selected_snapshot);
    let cursor_binding = SearchCursorBinding {
        q: &params.q,
        match_mode: params.match_mode,
        namespace: params.namespace.as_deref(),
        snapshot_token: &snapshot_token,
    };
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            search_storage_cursor(&payload, &cursor_binding)
        })
        .transpose()?;

    let filter = search_filter(&params);
    let storage_page =
        load_search_storage_page(&state, &filter, storage_cursor.as_ref(), params.page_size)
            .await?;

    let next_cursor = storage_page
        .next_cursor
        .as_ref()
        .map(|cursor| {
            search_cursor_payload(cursor, &cursor_binding).map(|payload| encode(&payload))
        })
        .transpose()?;
    let has_more = next_cursor.is_some();
    let data = storage_page.rows.iter().map(build_search_name).collect();
    let meta = Meta {
        as_of: Some(as_of_meta(&selected_snapshot)?),
        ..Meta::default()
    };

    Ok(Json(Envelope {
        data,
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count: None,
            has_more,
        }),
        meta,
    }))
}

pub(crate) fn build_search_name(row: &NameCurrentListRow) -> SearchName {
    let registration = name_registration_fields(Some(&row.row), &row.row.namespace);

    SearchName {
        name: row.row.normalized_name.clone(),
        display_name: row.row.canonical_display_name.clone(),
        namespace: row.row.namespace.clone(),
        namehash: row.row.namehash.clone(),
        owner: registration.owner,
        registrant: registration.registrant,
        registration_status: registration.registration_status,
        registered_at: row
            .registration_date
            .map(format_timestamp)
            .or(registration.registered_at),
        created_at: row
            .created_at
            .map(format_timestamp)
            .or(registration.created_at),
        expires_at: row
            .expiry_date
            .map(format_timestamp)
            .or(registration.expires_at),
    }
}

pub(crate) fn search_cursor_payload(
    cursor: &NameCurrentListCursor,
    binding: &SearchCursorBinding<'_>,
) -> V2Result<CursorPayload> {
    let NameCurrentListCursorValue::Name(display_name) = &cursor.sort_value else {
        return Err(V2Error::internal_error(
            "search pagination cursor must use name sort",
        ));
    };

    Ok(CursorPayload::new(
        SEARCH_SORT,
        cursor_filters(binding),
        BTreeMap::from([
            (DISPLAY_NAME_CURSOR_KEY.to_owned(), display_name.clone()),
            (NAMESPACE_FILTER_KEY.to_owned(), cursor.namespace.clone()),
            (
                NORMALIZED_NAME_CURSOR_KEY.to_owned(),
                cursor.normalized_name.clone(),
            ),
            (NAMEHASH_CURSOR_KEY.to_owned(), cursor.namehash.clone()),
        ]),
        Some(binding.snapshot_token.to_owned()),
    ))
}

pub(crate) fn search_storage_cursor(
    payload: &CursorPayload,
    binding: &SearchCursorBinding<'_>,
) -> V2Result<NameCurrentListCursor> {
    if payload.sort != SEARCH_SORT {
        return Err(invalid_search_cursor());
    }
    if payload.snapshot.as_deref() != Some(binding.snapshot_token) {
        return Err(invalid_search_cursor());
    }
    if payload.filters != cursor_filters(binding) {
        return Err(invalid_search_cursor());
    }
    if payload.last_item.len() != 4 {
        return Err(invalid_search_cursor());
    }

    Ok(NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name(cursor_value(
            payload,
            DISPLAY_NAME_CURSOR_KEY,
        )?),
        namespace: cursor_value(payload, NAMESPACE_FILTER_KEY)?,
        normalized_name: cursor_value(payload, NORMALIZED_NAME_CURSOR_KEY)?,
        namehash: cursor_value(payload, NAMEHASH_CURSOR_KEY)?,
    })
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SearchCursorBinding<'a> {
    pub(crate) q: &'a str,
    pub(crate) match_mode: SearchMatch,
    pub(crate) namespace: Option<&'a str>,
    pub(crate) snapshot_token: &'a str,
}

fn search_filter(params: &SearchQueryParams) -> NameCurrentListFilter {
    let mut filter = NameCurrentListFilter {
        namespace: params.namespace.clone(),
        namespaces: params.namespace.is_none().then(|| {
            PUBLIC_NAMESPACES
                .iter()
                .map(|namespace| (*namespace).to_owned())
                .collect()
        }),
        ..NameCurrentListFilter::default()
    };

    match params.match_mode {
        SearchMatch::Prefix => filter.prefix = Some(params.q.clone()),
        SearchMatch::Contains => filter.contains = Some(params.q.clone()),
    }

    filter
}

async fn search_snapshot_scope(
    state: &AppState,
    namespace: Option<&str>,
) -> V2Result<SnapshotSelectionScope> {
    let Some(namespace) = namespace else {
        let mut requirements = Vec::new();
        for namespace in PUBLIC_NAMESPACES {
            let scope = v2_exact_name_snapshot_scope(state, namespace).await?;
            requirements.extend(scope.required_positions().iter().cloned());
        }

        return SnapshotSelectionScope::new(requirements, None)
            .map_err(|error| V2Error::internal_error(error.message()));
    };

    v2_exact_name_snapshot_scope(state, namespace).await
}

async fn load_search_storage_page(
    state: &AppState,
    filter: &NameCurrentListFilter,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
) -> V2Result<bigname_storage::NameCurrentListPage> {
    bigname_storage::load_name_current_list_page(
        &state.pool,
        filter,
        NameCurrentListSort::Name,
        NameCurrentListOrder::Asc,
        cursor,
        page_size,
        false,
    )
    .await
    .map_err(|_| V2Error::internal_error("failed to load search results"))
}

fn cursor_filters(binding: &SearchCursorBinding<'_>) -> BTreeMap<String, String> {
    BTreeMap::from([
        (Q_FILTER_KEY.to_owned(), binding.q.to_owned()),
        (
            MATCH_FILTER_KEY.to_owned(),
            binding.match_mode.as_str().to_owned(),
        ),
        (
            NAMESPACE_FILTER_KEY.to_owned(),
            binding.namespace.unwrap_or(NONE_FILTER_VALUE).to_owned(),
        ),
    ])
}

fn parse_q(value: Option<String>) -> V2Result<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_lowercase())
        .ok_or_else(|| V2Error::invalid_input("q is required and must be non-empty"))
}

fn parse_match(value: Option<&str>) -> V2Result<SearchMatch> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("prefix") => Ok(SearchMatch::Prefix),
        Some("contains") => Ok(SearchMatch::Contains),
        Some(_) => Err(V2Error::invalid_input("match is invalid")),
    }
}

fn validate_namespace(namespace: &str) -> V2Result<()> {
    if PUBLIC_NAMESPACES.contains(&namespace) {
        Ok(())
    } else {
        Err(V2Error::invalid_input("namespace is invalid"))
    }
}

fn cursor_value(payload: &CursorPayload, key: &str) -> V2Result<String> {
    payload
        .last_item
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(invalid_search_cursor)
}

fn invalid_search_cursor() -> V2Error {
    V2Error::invalid_input("cursor must be a valid pagination cursor")
}

#[cfg(test)]
mod tests;
