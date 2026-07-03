use std::collections::BTreeMap;

use bigname_storage::{
    ChainPositions, SelectedSnapshot, SnapshotAt, SnapshotConsistency, SnapshotSelectionError,
    SnapshotSelectionErrorKind, SnapshotSelectionScope, SnapshotSelectorInput,
    parse_rfc3339_utc_timestamp, resolve_exact_name_snapshot_selection,
};
use serde_json::Value;
use sqlx::PgPool;
use tracing::{error, warn};

use crate::ApiError;

use super::{
    chains::slug_to_numeric,
    envelope::{AsOf, Meta},
    error::{V2Error, V2Result},
    params::AtSelector,
    vocab::Finality,
};

pub(crate) fn consistency_for_finality(finality: Finality) -> SnapshotConsistency {
    match finality {
        Finality::Latest => SnapshotConsistency::Head,
        Finality::Safe => SnapshotConsistency::Safe,
        Finality::Finalized => SnapshotConsistency::Finalized,
    }
}

pub(crate) fn encode_at_token(selected: &SelectedSnapshot) -> String {
    hex::encode(
        serde_json::to_vec(&selected.chain_positions_value())
            .expect("v2 snapshot positions must serialize"),
    )
}

pub(crate) fn decode_at_token(token: &str) -> V2Result<SnapshotAt> {
    let decoded = hex::decode(token).map_err(|_| invalid_at_error())?;
    let value: Value = serde_json::from_slice(&decoded).map_err(|_| invalid_at_error())?;
    let chain_positions = ChainPositions::from_value(&value).map_err(|_| invalid_at_error())?;

    Ok(SnapshotAt::ResolvedPositions(chain_positions))
}

pub(crate) fn as_of_meta(selected: &SelectedSnapshot) -> V2Result<BTreeMap<String, AsOf>> {
    let mut as_of = BTreeMap::new();

    for position in selected.chain_positions.as_map().values() {
        let numeric = slug_to_numeric(&position.chain_id).ok_or_else(|| {
            V2Error::internal_error(format!(
                "resolved snapshot position uses unmapped chain_id {}",
                position.chain_id
            ))
        })?;
        let position_value = position.to_value();
        let timestamp = position_value["timestamp"]
            .as_str()
            .ok_or_else(|| {
                V2Error::internal_error("resolved snapshot position did not format a timestamp")
            })?
            .to_owned();

        as_of.insert(
            numeric.to_string(),
            AsOf {
                block_number: position.block_number as u64,
                block_hash: position.block_hash.clone(),
                timestamp,
            },
        );
    }

    Ok(as_of)
}

pub(crate) fn snapshot_meta(selected: &SelectedSnapshot) -> V2Result<Meta> {
    Ok(Meta {
        as_of: Some(as_of_meta(selected)?),
        as_of_token: Some(encode_at_token(selected)),
        ..Meta::default()
    })
}

pub(crate) async fn resolve_v2_snapshot_for(
    pool: &PgPool,
    scope: &SnapshotSelectionScope,
    at: Option<&AtSelector>,
    finality: Finality,
    resource: SnapshotReadResource,
) -> V2Result<SelectedSnapshot> {
    let consistency = consistency_for_finality(finality);
    let at = match at {
        None => None,
        Some(AtSelector::Timestamp(timestamp)) => Some(SnapshotAt::Timestamp(
            parse_rfc3339_utc_timestamp(timestamp)
                .map_err(|error| map_snapshot_error_for_resource(error, resource))?,
        )),
        Some(AtSelector::SnapshotToken(token)) => Some(decode_at_token(token)?),
    };
    let input = SnapshotSelectorInput::new(at, None, consistency)
        .map_err(|error| map_snapshot_error_for_resource(error, resource))?;

    resolve_exact_name_snapshot_selection(pool, scope, &input)
        .await
        .map_err(|error| map_snapshot_error_for_resource(error, resource))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SnapshotReadResource {
    AddressHistory,
    AddressNames,
    DiagnosticData,
    Events,
    Name,
    NameHistory,
    NameRecords,
    Permissions,
    Resolver,
    Resource,
    Search,
    Subnames,
}

impl SnapshotReadResource {
    fn label(self) -> &'static str {
        match self {
            Self::AddressHistory => "address history",
            Self::AddressNames => "address names",
            Self::DiagnosticData => "diagnostic data",
            Self::Events => "events",
            Self::Name => "name",
            Self::NameHistory => "name history",
            Self::NameRecords => "name records",
            Self::Permissions => "permissions",
            Self::Resolver => "resolver",
            Self::Resource => "resource",
            Self::Search => "search results",
            Self::Subnames => "subnames",
        }
    }
}

pub(crate) fn api_error_to_v2(error: ApiError) -> V2Error {
    api_error_to_v2_for_resource(error, SnapshotReadResource::Resource)
}

pub(crate) fn api_error_to_v2_for_resource(
    error: ApiError,
    resource: SnapshotReadResource,
) -> V2Error {
    match error.code {
        "invalid_input" => V2Error::invalid_input(error.message),
        "not_found" => V2Error::not_found(error.message),
        "unsupported" => V2Error::unsupported(error.message),
        "stale" => {
            log_sanitized_api_error(&error, resource);
            V2Error::stale(stale_snapshot_message(resource))
        }
        "conflict" => {
            log_sanitized_api_error(&error, resource);
            V2Error::conflict(conflict_snapshot_message(resource))
        }
        _ => {
            log_sanitized_api_error(&error, resource);
            V2Error::internal_error(internal_api_message(resource))
        }
    }
}

pub(crate) fn sanitized_snapshot_internal_error(
    error: &SnapshotSelectionError,
    resource: SnapshotReadResource,
) -> V2Error {
    log_sanitized_snapshot_error(error, resource);
    V2Error::internal_error(internal_snapshot_message(resource))
}

fn map_snapshot_error_for_resource(
    error: SnapshotSelectionError,
    resource: SnapshotReadResource,
) -> V2Error {
    match error.kind() {
        SnapshotSelectionErrorKind::InvalidInput => V2Error::invalid_input(error.message()),
        SnapshotSelectionErrorKind::Conflict => {
            log_sanitized_snapshot_error(&error, resource);
            V2Error::conflict(conflict_snapshot_message(resource))
        }
        SnapshotSelectionErrorKind::Stale => {
            log_sanitized_snapshot_error(&error, resource);
            V2Error::stale(stale_snapshot_message(resource))
        }
        SnapshotSelectionErrorKind::InternalError => {
            sanitized_snapshot_internal_error(&error, resource)
        }
    }
}

fn invalid_at_error() -> V2Error {
    V2Error::invalid_input("at is invalid")
}

fn stale_snapshot_message(resource: SnapshotReadResource) -> String {
    format!(
        "requested snapshot is not available for {}",
        resource.label()
    )
}

fn conflict_snapshot_message(resource: SnapshotReadResource) -> String {
    format!(
        "requested snapshot cannot be resolved for {}",
        resource.label()
    )
}

fn internal_snapshot_message(resource: SnapshotReadResource) -> String {
    if resource == SnapshotReadResource::Resource {
        return "failed to serve v2 request".to_owned();
    }

    format!(
        "failed to select requested snapshot for {}",
        resource.label()
    )
}

fn internal_api_message(resource: SnapshotReadResource) -> String {
    if resource == SnapshotReadResource::Resource {
        return "failed to serve v2 request".to_owned();
    }

    format!("failed to load {}", resource.label())
}

fn log_sanitized_snapshot_error(error: &SnapshotSelectionError, resource: SnapshotReadResource) {
    match error.kind() {
        SnapshotSelectionErrorKind::InternalError => {
            error!(
                service = "api",
                resource = %resource.label(),
                kind = ?error.kind(),
                message = %error.message(),
                "sanitized v2 snapshot selection error"
            );
        }
        SnapshotSelectionErrorKind::Conflict | SnapshotSelectionErrorKind::Stale => {
            warn!(
                service = "api",
                resource = %resource.label(),
                kind = ?error.kind(),
                message = %error.message(),
                "sanitized v2 snapshot selection error"
            );
        }
        SnapshotSelectionErrorKind::InvalidInput => {}
    }
}

fn log_sanitized_api_error(error: &ApiError, resource: SnapshotReadResource) {
    if error.code == "internal_error" {
        error!(
            service = "api",
            resource = %resource.label(),
            status = %error.status,
            code = %error.code,
            message = %error.message,
            "sanitized v2 API error"
        );
    } else {
        warn!(
            service = "api",
            resource = %resource.label(),
            status = %error.status,
            code = %error.code,
            message = %error.message,
            "sanitized v2 API error"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bigname_storage::{
        ChainPosition, ChainPositions, ETHEREUM_MAINNET_CHAIN_ID, SelectedSnapshot, SnapshotAt,
        SnapshotConsistency, SnapshotSelectionError,
    };
    use serde_json::json;

    use super::*;
    use crate::v2::{ErrorCode, QueryParams, RawQueryParams};

    fn chain_position(slot: &str, chain_id: &str, block_hash: &str) -> ChainPosition {
        ChainPosition {
            slot: slot.to_owned(),
            chain_id: chain_id.to_owned(),
            block_number: 100,
            block_hash: block_hash.to_owned(),
            timestamp: bigname_storage::parse_rfc3339_utc_timestamp("2026-06-10T00:00:00Z")
                .unwrap(),
        }
    }

    fn selected_snapshot(chain_id: &str) -> SelectedSnapshot {
        SelectedSnapshot {
            chain_positions: ChainPositions::new(BTreeMap::from([(
                "ethereum".to_owned(),
                chain_position("ethereum", chain_id, "0xabc123"),
            )])),
            consistency: SnapshotConsistency::Head,
        }
    }

    #[test]
    fn finality_maps_to_snapshot_consistency() {
        assert_eq!(
            consistency_for_finality(Finality::Latest),
            SnapshotConsistency::Head
        );
        assert_eq!(
            consistency_for_finality(Finality::Safe),
            SnapshotConsistency::Safe
        );
        assert_eq!(
            consistency_for_finality(Finality::Finalized),
            SnapshotConsistency::Finalized
        );
    }

    #[test]
    fn at_token_round_trips_slot_native_chain_positions() {
        let selected = selected_snapshot(ETHEREUM_MAINNET_CHAIN_ID);
        let encoded = encode_at_token(&selected);

        assert!(
            encoded
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        );
        assert_eq!(encoded, encoded.to_ascii_lowercase());

        let params = QueryParams::try_from(RawQueryParams {
            at: Some(encoded.clone()),
            ..RawQueryParams::default()
        })
        .expect("hex token must parse as a v2 at selector");
        assert_eq!(params.at, Some(AtSelector::SnapshotToken(encoded.clone())));

        let decoded = decode_at_token(&encoded).expect("token must decode");
        assert_eq!(
            decoded,
            SnapshotAt::ResolvedPositions(selected.chain_positions)
        );
    }

    #[test]
    fn at_token_decode_rejects_malformed_tokens_as_invalid_input() {
        let error = decode_at_token("not-hex!!").expect_err("bad hex must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);

        let bad_shape =
            hex::encode(serde_json::to_vec(&json!({"not": "positions"})).expect("json encodes"));
        let error = decode_at_token(&bad_shape).expect_err("bad shape must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn as_of_meta_keys_positions_by_numeric_chain_id() {
        let selected = selected_snapshot(ETHEREUM_MAINNET_CHAIN_ID);
        let as_of = as_of_meta(&selected).expect("mainnet slug must map");

        assert_eq!(as_of.len(), 1);
        assert_eq!(
            as_of.get("1"),
            Some(&AsOf {
                block_number: 100,
                block_hash: "0xabc123".to_owned(),
                timestamp: "2026-06-10T00:00:00Z".to_owned(),
            })
        );
    }

    #[test]
    fn as_of_meta_rejects_unmapped_storage_slug_as_internal_error() {
        let selected = selected_snapshot("unknown-mainnet");
        let error = as_of_meta(&selected).expect_err("unknown slug must fail loudly");

        assert_eq!(error.code(), ErrorCode::InternalError);
    }

    #[test]
    fn snapshot_errors_map_to_v2_error_codes() {
        for (error, expected) in [
            (
                SnapshotSelectionError::invalid_input("snapshot error"),
                ErrorCode::InvalidInput,
            ),
            (
                SnapshotSelectionError::conflict("snapshot error"),
                ErrorCode::Conflict,
            ),
            (
                SnapshotSelectionError::stale("snapshot error"),
                ErrorCode::Stale,
            ),
            (
                SnapshotSelectionError::internal("snapshot error"),
                ErrorCode::InternalError,
            ),
        ] {
            let mapped = map_snapshot_error_for_resource(error, SnapshotReadResource::NameRecords);
            assert_eq!(mapped.code(), expected);
        }
    }

    #[test]
    fn snapshot_errors_sanitize_non_input_messages() {
        let invalid = map_snapshot_error_for_resource(
            SnapshotSelectionError::invalid_input("at is invalid"),
            SnapshotReadResource::NameRecords,
        );
        assert_eq!(invalid.envelope().error.message, "at is invalid");

        let stale = map_snapshot_error_for_resource(
            SnapshotSelectionError::stale("record_inventory_current projection does not match"),
            SnapshotReadResource::NameRecords,
        );
        assert_eq!(
            stale.envelope().error.message,
            "requested snapshot is not available for name records"
        );

        let conflict = map_snapshot_error_for_resource(
            SnapshotSelectionError::conflict("chain ethereum-mainnet has no checkpoint"),
            SnapshotReadResource::NameRecords,
        );
        assert_eq!(
            conflict.envelope().error.message,
            "requested snapshot cannot be resolved for name records"
        );

        let internal = map_snapshot_error_for_resource(
            SnapshotSelectionError::internal("failed to load chain_lineage: relation missing"),
            SnapshotReadResource::NameRecords,
        );
        assert_eq!(
            internal.envelope().error.message,
            "failed to select requested snapshot for name records"
        );
    }

    #[test]
    fn snapshot_token_path_does_not_need_numeric_registry_entries() {
        let selected = SelectedSnapshot {
            chain_positions: ChainPositions::new(BTreeMap::from([(
                "base".to_owned(),
                chain_position("base", "future-testnet", "0xdef456"),
            )])),
            consistency: SnapshotConsistency::Head,
        };
        let encoded = encode_at_token(&selected);
        let decoded = decode_at_token(&encoded).expect("token must remain storage-native");

        assert_eq!(
            decoded,
            SnapshotAt::ResolvedPositions(selected.chain_positions)
        );
        assert_eq!(slug_to_numeric("future-testnet"), None);
        assert_eq!(
            slug_to_numeric(bigname_storage::BASE_MAINNET_CHAIN_ID),
            Some(8453)
        );
    }
}
