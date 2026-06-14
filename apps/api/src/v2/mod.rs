#![allow(dead_code, unused_imports)]

mod chains;
mod cursor;
mod envelope;
mod error;
mod params;
mod router;
mod snapshots;
mod vocab;

pub(crate) use chains::{numeric_to_slug, slug_to_numeric};
pub(crate) use cursor::{Payload as CursorPayload, V2_CURSOR_VERSION, decode, encode};
pub(crate) use envelope::{AsOf, Envelope, Meta, Page};
pub(crate) use error::{ErrorBody, ErrorCode, ErrorEnvelope, V2Error, V2Result};
pub(crate) use params::{
    AtSelector, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, QueryParams, RawQueryParams, RequestSource,
    SortOrder,
};
pub(crate) use snapshots::{
    as_of_meta, consistency_for_finality, decode_at_token, encode_at_token, resolve_v2_snapshot,
};
pub(crate) use vocab::{
    Completeness, Finality, RegistrationStatus, Relation, Resolver, Source, Status,
};

use axum::Router;

use crate::state::AppState;

pub(crate) fn router() -> Router<AppState> {
    router::router()
}
