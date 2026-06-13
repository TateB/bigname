#![allow(dead_code, unused_imports)]

mod cursor;
mod envelope;
mod error;
mod params;
mod router;
mod vocab;

pub(crate) use cursor::{Payload as CursorPayload, V2_CURSOR_VERSION, decode, encode};
pub(crate) use envelope::{AsOf, Envelope, Meta, Page};
pub(crate) use error::{ErrorBody, ErrorCode, ErrorEnvelope, V2Error, V2Result};
pub(crate) use params::{
    AtSelector, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, QueryParams, RawQueryParams, RequestSource,
    SortOrder,
};
pub(crate) use vocab::{
    Completeness, Finality, RegistrationStatus, Relation, Resolver, Source, Status,
};

use axum::Router;

use crate::state::AppState;

pub(crate) fn router() -> Router<AppState> {
    router::router()
}
