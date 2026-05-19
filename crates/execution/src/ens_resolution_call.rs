use anyhow::Result;
use bigname_storage::{
    CanonicalityState, NameCurrentRow, RawCallSnapshot, SupportedVerifiedResolutionRecordKey,
    parse_supported_verified_resolution_record_key,
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::ens_resolution::{EnsResolutionRecord, ExecutionBlock};
use crate::ens_resolution_abi::{
    decode_selector_result, decode_universal_resolver_result, digest_json, hex_to_bytes,
    resolver_record_call, universal_resolver_call,
};
use crate::ens_resolution_ccip::{CcipReadSummary, follow_ccip_read};
use crate::rpc::{JsonRpcCallError, JsonRpcCallResult, JsonRpcHttpClient};
use crate::{ENS_UNIVERSAL_RESOLVER_ADDRESS, ETHEREUM_MAINNET_CHAIN_ID};

pub(super) struct SelectorCall {
    status: SelectorStatus,
    pub(super) request_hash: Option<String>,
    pub(super) response_hash: Option<String>,
    pub(super) raw_call_snapshot: Option<RawCallSnapshot>,
    pub(super) contract_call: Value,
    pub(super) universal_calldata: String,
    pub(super) resolver_selector: String,
    pub(super) block_selector: Value,
    pub(super) ccip_summary: Option<CcipReadSummary>,
}

enum SelectorStatus {
    Success { value: Value },
    NotFound { failure_reason: &'static str },
    ExecutionFailed { failure_reason: &'static str },
}

impl SelectorCall {
    pub(super) fn verified_query(&self, execution_trace_id: Uuid) -> Value {
        let mut query = match &self.status {
            SelectorStatus::Success { value } => json!({
                "status": "success",
                "value": value,
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string(),
                }
            }),
            SelectorStatus::NotFound { failure_reason } => json!({
                "status": "not_found",
                "failure_reason": failure_reason,
            }),
            SelectorStatus::ExecutionFailed { failure_reason } => json!({
                "status": "execution_failed",
                "failure_reason": failure_reason,
            }),
        };
        query["record_key"] = self
            .contract_call
            .get("record_key")
            .cloned()
            .unwrap_or(Value::Null);
        query
    }
}

pub(super) async fn execute_record_call(
    row: &NameCurrentRow,
    record: &EnsResolutionRecord,
    dns_name: &[u8],
    node: [u8; 32],
    block: &ExecutionBlock,
    rpc: &JsonRpcHttpClient,
    use_latest_block_tag: bool,
) -> Result<SelectorCall> {
    let selector = parse_supported_verified_resolution_record_key(&record.record_key)?;
    let resolver_call = resolver_record_call(&selector, &record.record_key, node)?;
    let universal_call = universal_resolver_call(dns_name, resolver_call.calldata());
    let universal_calldata_hex = universal_call.calldata_hex();
    let universal_selector = universal_call.selector_hex();
    let resolver_selector = resolver_call.selector_hex();
    let contract_call = json!({
        "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
        "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
        "selector": universal_selector,
        "record_key": record.record_key,
        "resolver_selector": resolver_selector,
    });
    let call = json!({
        "to": ENS_UNIVERSAL_RESOLVER_ADDRESS,
        "data": universal_calldata_hex,
    });
    let block_selector = if use_latest_block_tag {
        json!("latest")
    } else {
        json!({
            "blockHash": block.block_hash,
            "requireCanonical": true,
        })
    };

    let mut ccip_summary = None;
    let result = rpc
        .call("eth_call", vec![call, block_selector.clone()])
        .await?;
    let result = if use_latest_block_tag {
        match &result.result {
            Err(error) => match follow_ccip_read(rpc, error, &block_selector).await {
                Ok(Some(outcome)) => {
                    ccip_summary = Some(outcome.summary);
                    outcome.result
                }
                Ok(None) => result,
                Err(_) => result,
            },
            Ok(_) => result,
        }
    } else {
        result
    };
    selector_call_from_rpc_result(SelectorCallRpcContext {
        row,
        record,
        selector: &selector,
        result,
        block,
        contract_call,
        resolver_selector,
        universal_calldata: universal_calldata_hex,
        block_selector,
        persist_raw_call_snapshot: !use_latest_block_tag,
        ccip_summary,
    })
}

struct SelectorCallRpcContext<'a> {
    row: &'a NameCurrentRow,
    record: &'a EnsResolutionRecord,
    selector: &'a SupportedVerifiedResolutionRecordKey,
    result: JsonRpcCallResult,
    block: &'a ExecutionBlock,
    contract_call: Value,
    resolver_selector: String,
    universal_calldata: String,
    block_selector: Value,
    persist_raw_call_snapshot: bool,
    ccip_summary: Option<CcipReadSummary>,
}

fn selector_call_from_rpc_result(context: SelectorCallRpcContext<'_>) -> Result<SelectorCall> {
    let SelectorCallRpcContext {
        row,
        record,
        selector,
        result,
        block,
        contract_call,
        resolver_selector,
        universal_calldata,
        block_selector,
        persist_raw_call_snapshot,
        ccip_summary,
    } = context;
    let JsonRpcCallResult {
        request_payload,
        response_payload,
        result,
    } = result;
    let request_hash = digest_json(&request_payload);
    let response_hash = digest_json(&response_payload);
    let raw_call_snapshot = persist_raw_call_snapshot.then(|| RawCallSnapshot {
        chain_id: block.chain_id.clone(),
        block_hash: block.block_hash.clone(),
        block_number: block.block_number,
        request_hash: request_hash.clone(),
        request_payload,
        response_hash: response_hash.clone(),
        response_payload,
        canonicality_state: CanonicalityState::Canonical,
    });
    let status = match result {
        Ok(value) => decode_rpc_success(row, record, selector, value),
        Err(error) => Ok(decode_rpc_error(&error)),
    }?;

    Ok(SelectorCall {
        status,
        request_hash: Some(request_hash),
        response_hash: Some(response_hash),
        raw_call_snapshot,
        contract_call,
        universal_calldata,
        resolver_selector,
        block_selector,
        ccip_summary,
    })
}

fn decode_rpc_success(
    _row: &NameCurrentRow,
    record: &EnsResolutionRecord,
    selector: &SupportedVerifiedResolutionRecordKey,
    value: Value,
) -> Result<SelectorStatus> {
    let Some(hex) = value.as_str() else {
        return Ok(SelectorStatus::ExecutionFailed {
            failure_reason: "resolver_return_data_malformed",
        });
    };
    let return_data = match hex_to_bytes(hex) {
        Ok(value) => value,
        Err(_) => {
            return Ok(SelectorStatus::ExecutionFailed {
                failure_reason: "resolver_return_data_malformed",
            });
        }
    };
    let selector_return = match decode_universal_resolver_result(&return_data) {
        Ok(value) => value,
        Err(_) => {
            return Ok(SelectorStatus::ExecutionFailed {
                failure_reason: "resolver_return_data_malformed",
            });
        }
    };
    let decoded = match decode_selector_result(selector, &selector_return) {
        Ok(decoded) => decoded,
        Err(_) => {
            return Ok(SelectorStatus::ExecutionFailed {
                failure_reason: "resolver_return_data_malformed",
            });
        }
    };

    let Some(value) = decoded else {
        return Ok(SelectorStatus::NotFound {
            failure_reason: not_found_reason(selector),
        });
    };
    Ok(SelectorStatus::Success {
        value: selector_value(record, selector, value),
    })
}

fn decode_rpc_error(error: &JsonRpcCallError) -> SelectorStatus {
    let _ = error.code;
    let _ = &error.message;
    let _ = &error.data;
    SelectorStatus::ExecutionFailed {
        failure_reason: "resolver_call_failed",
    }
}

fn selector_value(
    _record: &EnsResolutionRecord,
    selector: &SupportedVerifiedResolutionRecordKey,
    value: String,
) -> Value {
    match selector {
        SupportedVerifiedResolutionRecordKey::Addr { coin_type } => json!({
            "coin_type": coin_type,
            "value": value,
        }),
        SupportedVerifiedResolutionRecordKey::Text
        | SupportedVerifiedResolutionRecordKey::Avatar
        | SupportedVerifiedResolutionRecordKey::Contenthash => json!({
            "value": value,
        }),
    }
}

fn not_found_reason(selector: &SupportedVerifiedResolutionRecordKey) -> &'static str {
    match selector {
        SupportedVerifiedResolutionRecordKey::Addr { .. } => "no_addr_record",
        SupportedVerifiedResolutionRecordKey::Text => "no_text_record",
        SupportedVerifiedResolutionRecordKey::Avatar => "no_avatar_record",
        SupportedVerifiedResolutionRecordKey::Contenthash => "no_contenthash_record",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_storage::ENS_NAMESPACE;
    use sqlx::types::time::OffsetDateTime;

    #[test]
    fn malformed_universal_resolver_success_is_selector_failure_for_latest_calls() -> Result<()> {
        let row = test_name_current_row();
        let record = EnsResolutionRecord::new("avatar", "text", Some("avatar".to_owned()));
        let selector = parse_supported_verified_resolution_record_key(&record.record_key)?;
        let block = ExecutionBlock {
            chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
            block_number: 21_000_000,
            block_hash: "0xabc123".to_owned(),
        };
        let selector_call = selector_call_from_rpc_result(SelectorCallRpcContext {
            row: &row,
            record: &record,
            selector: &selector,
            result: JsonRpcCallResult {
                request_payload: json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_call",
                    "params": [
                        {
                            "to": ENS_UNIVERSAL_RESOLVER_ADDRESS,
                            "data": "0x9061b923"
                        },
                        "latest"
                    ]
                }),
                response_payload: json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": "0x"
                }),
                result: Ok(json!("0x")),
            },
            block: &block,
            contract_call: json!({
                "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
                "selector": "0x9061b923",
                "record_key": "avatar",
                "resolver_selector": "0x59d1d43c"
            }),
            resolver_selector: "0x59d1d43c".to_owned(),
            universal_calldata: "0x9061b923".to_owned(),
            block_selector: json!("latest"),
            persist_raw_call_snapshot: false,
            ccip_summary: None,
        })?;

        assert_eq!(selector_call.block_selector, json!("latest"));
        assert!(selector_call.request_hash.is_some());
        assert!(selector_call.response_hash.is_some());
        assert!(selector_call.raw_call_snapshot.is_none());

        let verified_query = selector_call.verified_query(Uuid::from_u128(1));
        assert_eq!(
            verified_query.get("status").and_then(Value::as_str),
            Some("execution_failed")
        );
        assert_eq!(
            verified_query.get("failure_reason").and_then(Value::as_str),
            Some("resolver_return_data_malformed")
        );
        assert_eq!(
            verified_query.get("record_key").and_then(Value::as_str),
            Some("avatar")
        );
        Ok(())
    }

    fn test_name_current_row() -> NameCurrentRow {
        NameCurrentRow {
            logical_name_id: "ens:alice.eth".to_owned(),
            namespace: ENS_NAMESPACE.to_owned(),
            canonical_display_name: "alice.eth".to_owned(),
            normalized_name: "alice.eth".to_owned(),
            namehash: "namehash:alice.eth".to_owned(),
            surface_binding_id: None,
            resource_id: None,
            token_lineage_id: None,
            binding_kind: None,
            declared_summary: json!({}),
            provenance: json!({}),
            coverage: json!({}),
            chain_positions: json!({}),
            canonicality_summary: json!({}),
            manifest_version: 1,
            last_recomputed_at: OffsetDateTime::from_unix_timestamp(1)
                .expect("test timestamp must be valid"),
        }
    }
}
