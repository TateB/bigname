use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct HistoryPageResponse {
    pub(crate) cursor: Option<String>,
    pub(crate) next_cursor: Option<String>,
    pub(crate) page_size: u64,
    pub(crate) sort: String,
}

pub(crate) const DEFAULT_PAGE_SIZE: u64 = 50;
pub(crate) const MAX_PAGE_SIZE: u64 = 200;
pub(crate) const CURSOR_VERSION: u8 = 1;

#[derive(Clone, Debug)]
pub(crate) struct PaginationRequest {
    pub(crate) active: bool,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct CursorSpec {
    pub(crate) route: &'static str,
    pub(crate) anchor: String,
    pub(crate) sort: &'static str,
    pub(crate) filters: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CursorEnvelope {
    pub(crate) version: u8,
    pub(crate) route: String,
    pub(crate) anchor: String,
    pub(crate) sort: String,
    pub(crate) filters: BTreeMap<String, String>,
    pub(crate) item: BTreeMap<String, String>,
}

impl CursorSpec {
    pub(crate) fn envelope(&self, item: BTreeMap<String, String>) -> CursorEnvelope {
        CursorEnvelope {
            version: CURSOR_VERSION,
            route: self.route.to_owned(),
            anchor: self.anchor.clone(),
            sort: self.sort.to_owned(),
            filters: self.filters.clone(),
            item,
        }
    }
}
