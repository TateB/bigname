use std::sync::LazyLock;
use std::time::Duration;

use alloy_primitives::{Address, Bytes};
use alloy_sol_types::{SolCall, SolError, SolValue, sol};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::ens_resolution_abi::{hex_string, hex_to_bytes};
use crate::rpc::{JsonRpcCallError, JsonRpcCallResult, JsonRpcHttpClient};

const LOCAL_BATCH_GATEWAY_URL: &str = "x-batch-gateway:true";
const MAX_CCIP_REDIRECTS: usize = 4;
const MAX_GATEWAY_URLS: usize = 4;
const GATEWAY_TIMEOUT: Duration = Duration::from_millis(1500);

static GATEWAY_HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(GATEWAY_TIMEOUT)
        .build()
        .expect("CCIP gateway HTTP client configuration must be valid")
});

mod abi {
    use super::*;

    sol! {
        #[derive(Debug, PartialEq, Eq)]
        error OffchainLookup(
            address sender,
            string[] urls,
            bytes callData,
            bytes4 callbackFunction,
            bytes extraData
        );

        #[derive(Debug, PartialEq, Eq)]
        struct Request {
            address sender;
            string[] urls;
            bytes data;
        }

        function query(Request[] requests) external view returns (
            bool[] failures,
            bytes[] responses
        );
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CcipReadSummary {
    pub(crate) gateway_digests: Vec<String>,
    pub(crate) step_payloads: Vec<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CcipReadOutcome {
    pub(crate) result: JsonRpcCallResult,
    pub(crate) summary: CcipReadSummary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OffchainLookup {
    sender: String,
    urls: Vec<String>,
    call_data: Vec<u8>,
    callback_function: [u8; 4],
    extra_data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BatchGatewayRequest {
    sender: String,
    urls: Vec<String>,
    data: Vec<u8>,
}

pub(crate) async fn follow_ccip_read(
    rpc: &JsonRpcHttpClient,
    error: &JsonRpcCallError,
    block_selector: &Value,
) -> Result<Option<CcipReadOutcome>> {
    let Some(mut lookup) = offchain_lookup_from_rpc_error(error)? else {
        return Ok(None);
    };

    let mut gateway_digests = Vec::new();
    let mut step_payloads = Vec::new();
    for redirect_index in 0..MAX_CCIP_REDIRECTS {
        let gateway_response = fetch_ccip_gateway_response(&lookup)
            .await
            .with_context(|| {
                format!(
                    "failed to complete CCIP-Read gateway request at redirect {}",
                    redirect_index + 1
                )
            })?;
        let callback_calldata = ccip_callback_calldata(
            lookup.callback_function,
            &gateway_response.body,
            &lookup.extra_data,
        );
        let call = json!({
            "to": lookup.sender,
            "data": hex_string(&callback_calldata),
        });
        let callback_result = rpc
            .call("eth_call", vec![call, block_selector.clone()])
            .await
            .context("failed to execute CCIP-Read callback eth_call")?;
        gateway_digests.extend(gateway_response.gateway_digests);
        step_payloads.push(json!({
            "sender": lookup.sender,
            "gateway_count": lookup.urls.len(),
            "used_local_batch_gateway": gateway_response.used_local_batch_gateway,
            "response_bytes": gateway_response.body.len(),
        }));

        match &callback_result.result {
            Err(error) if redirect_index + 1 < MAX_CCIP_REDIRECTS => {
                if let Some(next_lookup) = offchain_lookup_from_rpc_error(error)? {
                    lookup = next_lookup;
                    continue;
                }
            }
            _ => {}
        }

        return Ok(Some(CcipReadOutcome {
            result: callback_result,
            summary: CcipReadSummary {
                gateway_digests,
                step_payloads,
            },
        }));
    }

    bail!("CCIP-Read exceeded maximum redirect depth");
}

struct GatewayFetchResult {
    body: Vec<u8>,
    gateway_digests: Vec<String>,
    used_local_batch_gateway: bool,
}

async fn fetch_ccip_gateway_response(lookup: &OffchainLookup) -> Result<GatewayFetchResult> {
    if lookup
        .urls
        .iter()
        .any(|url| url.eq_ignore_ascii_case(LOCAL_BATCH_GATEWAY_URL))
    {
        let requests = decode_batch_gateway_query(&lookup.call_data)
            .context("failed to decode ENSIP-21 batch gateway query")?;
        let results =
            futures_util::future::join_all(requests.into_iter().map(|request| async move {
                fetch_standard_gateway_response(&request.sender, &request.urls, &request.data).await
            }))
            .await;
        let mut failures = Vec::with_capacity(results.len());
        let mut responses = Vec::with_capacity(results.len());
        let mut gateway_digests = Vec::new();
        for result in results {
            match result {
                Ok(response) => {
                    failures.push(false);
                    gateway_digests.extend(response.gateway_digests);
                    responses.push(response.body);
                }
                Err(error) => {
                    failures.push(true);
                    responses.push(abi_error_string(&format!(
                        "CCIP gateway request failed: {error}"
                    )));
                }
            }
        }

        return Ok(GatewayFetchResult {
            body: abi_encode_bool_array_and_bytes_array(&failures, &responses),
            gateway_digests,
            used_local_batch_gateway: true,
        });
    }

    let response =
        fetch_standard_gateway_response(&lookup.sender, &lookup.urls, &lookup.call_data).await?;
    Ok(GatewayFetchResult {
        body: response.body,
        gateway_digests: response.gateway_digests,
        used_local_batch_gateway: false,
    })
}

async fn fetch_standard_gateway_response(
    sender: &str,
    urls: &[String],
    call_data: &[u8],
) -> Result<GatewayFetchResult> {
    let data = hex_string(call_data);
    let mut last_error = None;
    for url in urls
        .iter()
        .filter(|url| !url.eq_ignore_ascii_case(LOCAL_BATCH_GATEWAY_URL))
        .take(MAX_GATEWAY_URLS)
    {
        match fetch_one_gateway(&GATEWAY_HTTP_CLIENT, url, sender, &data).await {
            Ok(body) => {
                return Ok(GatewayFetchResult {
                    gateway_digests: vec![crate::ens_resolution_abi::digest_json(&json!({
                        "url": url,
                        "sender": sender,
                        "data": data,
                        "response_bytes": body.len(),
                    }))],
                    body,
                    used_local_batch_gateway: false,
                });
            }
            Err(error) => last_error = Some(error),
        }
    }

    match last_error {
        Some(error) => Err(error),
        None => bail!("CCIP-Read supplied no usable HTTP gateway URL"),
    }
}

async fn fetch_one_gateway(
    client: &reqwest::Client,
    url: &str,
    sender: &str,
    data: &str,
) -> Result<Vec<u8>> {
    let response = if url.contains("{data}") || url.contains("{sender}") {
        let url = url.replace("{sender}", sender).replace("{data}", data);
        client.get(&url).send().await
    } else {
        client
            .post(url)
            .json(&json!({
                "sender": sender,
                "data": data,
            }))
            .send()
            .await
    }
    .with_context(|| format!("failed to send CCIP gateway request to {url}"))?;

    let status = response.status();
    let body = response
        .bytes()
        .await
        .with_context(|| format!("failed to read CCIP gateway response body from {url}"))?;
    if !status.is_success() {
        bail!("CCIP gateway {url} returned HTTP {status}");
    }

    decode_gateway_response_body(&body)
        .with_context(|| format!("failed to decode CCIP gateway response body from {url}"))
}

fn decode_gateway_response_body(body: &[u8]) -> Result<Vec<u8>> {
    gateway_response_hex_payload(body)?.decode()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HexPayload {
    value: String,
}

impl HexPayload {
    fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    fn decode(&self) -> Result<Vec<u8>> {
        hex_to_bytes(&self.value)
    }
}

fn gateway_response_hex_payload(body: &[u8]) -> Result<HexPayload> {
    if let Ok(value) = serde_json::from_slice::<Value>(body)
        && let Some(payload) = gateway_json_hex_payload(&value)
    {
        return Ok(payload);
    }

    let text = std::str::from_utf8(body).context("gateway response is not UTF-8")?;
    Ok(HexPayload::new(text.trim()))
}

fn gateway_json_hex_payload(value: &Value) -> Option<HexPayload> {
    match value {
        Value::Object(object) => object
            .get("data")
            .and_then(Value::as_str)
            .map(HexPayload::new),
        Value::String(data) => Some(HexPayload::new(data.clone())),
        _ => None,
    }
}

fn offchain_lookup_from_rpc_error(error: &JsonRpcCallError) -> Result<Option<OffchainLookup>> {
    let Some(data) = error.data.as_ref().and_then(rpc_error_hex_data) else {
        return Ok(None);
    };
    let bytes = data.decode()?;
    decode_offchain_lookup_revert(&bytes)
}

pub(crate) fn rpc_error_contains_offchain_lookup(error: &JsonRpcCallError) -> Result<bool> {
    offchain_lookup_from_rpc_error(error).map(|lookup| lookup.is_some())
}

// JSON-RPC providers disagree on where revert data lives; keep this compatibility
// scan centralized so the accepted shapes stay deliberate.
fn rpc_error_hex_data(value: &Value) -> Option<HexPayload> {
    match value {
        Value::String(text) if text.starts_with("0x") => Some(HexPayload::new(text.as_str())),
        Value::Object(object) => object
            .get("data")
            .and_then(rpc_error_hex_data)
            .or_else(|| object.get("originalError").and_then(rpc_error_hex_data))
            .or_else(|| object.get("error").and_then(rpc_error_hex_data)),
        _ => None,
    }
}

fn decode_offchain_lookup_revert(bytes: &[u8]) -> Result<Option<OffchainLookup>> {
    if !bytes.starts_with(&abi::OffchainLookup::SELECTOR) {
        return Ok(None);
    }
    decode_offchain_lookup(bytes).map(Some)
}

fn decode_offchain_lookup(data: &[u8]) -> Result<OffchainLookup> {
    let decoded =
        abi::OffchainLookup::abi_decode_validate(data).context("OffchainLookup data malformed")?;
    Ok(OffchainLookup {
        sender: format_address(decoded.sender),
        urls: decoded.urls,
        call_data: decoded.callData.to_vec(),
        callback_function: decoded.callbackFunction.into(),
        extra_data: decoded.extraData.to_vec(),
    })
}

fn decode_batch_gateway_query(call_data: &[u8]) -> Result<Vec<BatchGatewayRequest>> {
    let decoded = decode_batch_gateway_query_call(call_data)?;
    Ok(decoded
        .requests
        .into_iter()
        .map(|request| BatchGatewayRequest {
            sender: format_address(request.sender),
            urls: request.urls,
            data: request.data.to_vec(),
        })
        .collect())
}

fn decode_batch_gateway_query_call(call_data: &[u8]) -> Result<abi::queryCall> {
    abi::queryCall::abi_decode(call_data).context("batch gateway query calldata malformed")
}

fn ccip_callback_calldata(
    callback_function: [u8; 4],
    response: &[u8],
    extra_data: &[u8],
) -> Vec<u8> {
    let mut calldata = Vec::new();
    calldata.extend_from_slice(&callback_function);
    calldata.extend_from_slice(
        &(
            Bytes::copy_from_slice(response),
            Bytes::copy_from_slice(extra_data),
        )
            .abi_encode_params(),
    );
    calldata
}

fn abi_encode_bool_array_and_bytes_array(failures: &[bool], responses: &[Vec<u8>]) -> Vec<u8> {
    let responses = responses
        .iter()
        .map(|response| Bytes::copy_from_slice(response))
        .collect::<Vec<_>>();
    (failures.to_vec(), responses).abi_encode_params()
}

fn abi_error_string(message: &str) -> Vec<u8> {
    alloy_sol_types::Revert::from(message).abi_encode()
}

fn format_address(address: Address) -> String {
    hex_string(address.as_slice())
}

#[cfg(test)]
#[path = "ens_resolution_ccip/tests.rs"]
mod tests;
