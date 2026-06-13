use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::vocab::{Completeness, Source};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Envelope<T> {
    pub(crate) data: T,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) page: Option<Page>,
    pub(crate) meta: Meta,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Page {
    pub(crate) cursor: Option<String>,
    pub(crate) next_cursor: Option<String>,
    pub(crate) page_size: u64,
    pub(crate) total_count: Option<u64>,
    pub(crate) has_more: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Meta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) as_of: Option<BTreeMap<String, AsOf>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) completeness: Option<Completeness>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unsupported_fields: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unsupported_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source: Option<Source>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AsOf {
    pub(crate) block_number: u64,
    pub(crate) block_hash: String,
    pub(crate) timestamp: String,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn envelope_omits_absent_page_and_keeps_empty_meta() {
        let envelope = Envelope {
            data: json!({ "name": "nick.eth" }),
            page: None,
            meta: Meta::default(),
        };

        let value = serde_json::to_value(envelope).expect("envelope must serialize");

        assert_eq!(
            value,
            json!({
                "data": { "name": "nick.eth" },
                "meta": {}
            })
        );
        assert!(value.get("page").is_none());
    }

    #[test]
    fn page_serializes_nullable_total_count() {
        let envelope = Envelope {
            data: json!([]),
            page: Some(Page {
                cursor: Some("cursor-1".to_owned()),
                next_cursor: None,
                page_size: 50,
                total_count: None,
                has_more: false,
            }),
            meta: Meta::default(),
        };

        let value = serde_json::to_value(envelope).expect("envelope must serialize");

        assert_eq!(
            value["page"],
            json!({
                "cursor": "cursor-1",
                "next_cursor": null,
                "page_size": 50,
                "total_count": null,
                "has_more": false
            })
        );
    }
}
