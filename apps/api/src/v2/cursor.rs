use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::error::{V2Error, V2Result};

pub(crate) const V2_CURSOR_VERSION: u8 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Payload {
    pub(crate) version: u8,
    pub(crate) sort: String,
    pub(crate) filters: BTreeMap<String, String>,
    pub(crate) last_item: BTreeMap<String, String>,
    pub(crate) snapshot: Option<String>,
}

impl Payload {
    pub(crate) fn new(
        sort: impl Into<String>,
        filters: BTreeMap<String, String>,
        last_item: BTreeMap<String, String>,
        snapshot: Option<String>,
    ) -> Self {
        Self {
            version: V2_CURSOR_VERSION,
            sort: sort.into(),
            filters,
            last_item,
            snapshot,
        }
    }
}

pub(crate) fn encode(payload: &Payload) -> String {
    hex::encode(serde_json::to_vec(payload).expect("v2 cursor payload must serialize"))
}

pub(crate) fn decode(cursor: &str) -> V2Result<Payload> {
    let decoded = hex::decode(cursor).map_err(|_| invalid_cursor_error())?;
    let payload: Payload = serde_json::from_slice(&decoded).map_err(|_| invalid_cursor_error())?;

    if payload.version != V2_CURSOR_VERSION {
        return Err(invalid_cursor_error());
    }

    Ok(payload)
}

pub(crate) fn cursor_value(
    payload: &Payload,
    key: &str,
    invalid_cursor_error: impl Fn() -> V2Error,
) -> V2Result<String> {
    payload
        .last_item
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(invalid_cursor_error)
}

pub(crate) fn invalid_cursor_error() -> V2Error {
    V2Error::invalid_input("cursor must be a valid pagination cursor")
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    use crate::v2::error::ErrorCode;

    fn sample_payload() -> Payload {
        let filters = BTreeMap::from([
            ("namespace".to_owned(), "ens".to_owned()),
            ("order".to_owned(), "asc".to_owned()),
        ]);
        let last_item = BTreeMap::from([
            ("name".to_owned(), "nick.eth".to_owned()),
            ("registration_id".to_owned(), "reg-1".to_owned()),
        ]);

        Payload::new("name", filters, last_item, Some("snapshot-1".to_owned()))
    }

    #[test]
    fn cursor_round_trips_encoded_payload() {
        let payload = sample_payload();
        let encoded = encode(&payload);

        assert!(encoded.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(encoded, encoded.to_ascii_lowercase());
        assert_eq!(decode(&encoded).expect("cursor must decode"), payload);
    }

    #[test]
    fn cursor_decode_rejects_version_mismatch_as_invalid_input() {
        let mut payload = sample_payload();
        payload.version = V2_CURSOR_VERSION + 1;
        let encoded = hex::encode(serde_json::to_vec(&payload).expect("payload must serialize"));

        let error = decode(&encoded).expect_err("version mismatch must fail");

        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn cursor_decode_rejects_malformed_token_as_invalid_input() {
        let error = decode("not-a-hex-cursor").expect_err("malformed cursor must fail");

        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn cursor_payload_has_no_route_field() {
        let payload = sample_payload();
        let serialized = serde_json::to_value(payload).expect("payload must serialize");
        let Value::Object(object) = serialized else {
            panic!("payload must serialize as an object");
        };

        assert!(!object.contains_key("route"));
        assert!(object.contains_key("snapshot"));
    }
}
