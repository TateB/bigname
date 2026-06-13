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
    encode_base64_url_no_pad(
        &serde_json::to_vec(payload).expect("v2 cursor payload must serialize"),
    )
}

pub(crate) fn decode(cursor: &str) -> V2Result<Payload> {
    let decoded = decode_base64_url_no_pad(cursor).map_err(|_| invalid_cursor_error())?;
    let payload: Payload = serde_json::from_slice(&decoded).map_err(|_| invalid_cursor_error())?;

    if payload.version != V2_CURSOR_VERSION {
        return Err(invalid_cursor_error());
    }

    Ok(payload)
}

fn invalid_cursor_error() -> V2Error {
    V2Error::invalid_input("cursor must be a valid pagination cursor")
}

const BASE64_URL_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn encode_base64_url_no_pad(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity((bytes.len() * 4).div_ceil(3));

    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied();
        let third = chunk.get(2).copied();

        encoded.push(BASE64_URL_ALPHABET[(first >> 2) as usize] as char);
        encoded.push(
            BASE64_URL_ALPHABET[(((first & 0b0000_0011) << 4) | second.unwrap_or(0) >> 4) as usize]
                as char,
        );

        if let Some(second) = second {
            encoded.push(
                BASE64_URL_ALPHABET
                    [(((second & 0b0000_1111) << 2) | third.unwrap_or(0) >> 6) as usize]
                    as char,
            );
        }

        if let Some(third) = third {
            encoded.push(BASE64_URL_ALPHABET[(third & 0b0011_1111) as usize] as char);
        }
    }

    encoded
}

fn decode_base64_url_no_pad(value: &str) -> Result<Vec<u8>, ()> {
    if value.is_empty() || value.as_bytes().contains(&b'=') || value.len() % 4 == 1 {
        return Err(());
    }

    let values = value
        .bytes()
        .map(base64_url_value)
        .collect::<Option<Vec<_>>>()
        .ok_or(())?;
    let mut decoded = Vec::with_capacity(values.len() * 3 / 4);

    for chunk in values.chunks(4) {
        decoded.push((chunk[0] << 2) | (chunk[1] >> 4));

        match chunk.len() {
            2 => {
                if chunk[1] & 0b0000_1111 != 0 {
                    return Err(());
                }
            }
            3 => {
                if chunk[2] & 0b0000_0011 != 0 {
                    return Err(());
                }
                decoded.push((chunk[1] << 4) | (chunk[2] >> 2));
            }
            4 => {
                decoded.push((chunk[1] << 4) | (chunk[2] >> 2));
                decoded.push((chunk[2] << 6) | chunk[3]);
            }
            _ => return Err(()),
        }
    }

    Ok(decoded)
}

fn base64_url_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
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

        assert_eq!(decode(&encoded).expect("cursor must decode"), payload);
    }

    #[test]
    fn cursor_decode_rejects_version_mismatch_as_invalid_input() {
        let mut payload = sample_payload();
        payload.version = V2_CURSOR_VERSION + 1;
        let encoded = encode_base64_url_no_pad(
            &serde_json::to_vec(&payload).expect("payload must serialize"),
        );

        let error = decode(&encoded).expect_err("version mismatch must fail");

        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn cursor_decode_rejects_malformed_token_as_invalid_input() {
        let error = decode("not a base64url cursor").expect_err("malformed cursor must fail");

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
