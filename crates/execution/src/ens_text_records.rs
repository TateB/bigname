use std::str::FromStr;

use alloy_primitives::{Address, Bytes};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result, bail};
use bigname_storage::SupportedVerifiedResolutionRecordKey;
use serde_json::{Value, json};

use crate::{
    ens_resolution_abi::{
        decode_selector_result, hex_string, hex_to_bytes, namehash, resolver_calldata,
    },
    rpc::{ChainRpcUrls, JsonRpcHttpClient},
};

pub const MULTICALL3_ADDRESS: &str = "0xcA11bde05977b3631167028862bE2a173976CA11";

mod abi {
    use super::*;

    sol! {
        #[derive(Debug, PartialEq, Eq)]
        struct Call3 {
            address target;
            bool allowFailure;
            bytes callData;
        }

        #[derive(Debug, PartialEq, Eq)]
        struct Result3 {
            bool success;
            bytes returnData;
        }

        function aggregate3(Call3[] calls) external payable returns (Result3[] returnData);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsTextRecordMulticallRequest {
    pub resolver_address: String,
    pub namehash: String,
    pub text_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsTextRecordMulticallBlock {
    pub block_number: i64,
    pub block_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnsTextRecordMulticallResult {
    Success { value: String },
    NotFound,
    Failed { message: String },
}

pub fn ens_namehash_hex(name: &str) -> Result<String> {
    namehash(name).map(|node| hex_string(&node))
}

pub async fn execute_ens_text_record_multicall(
    rpc_urls: &ChainRpcUrls,
    chain_id: &str,
    multicall3_address: &str,
    block: &EnsTextRecordMulticallBlock,
    requests: &[EnsTextRecordMulticallRequest],
) -> Result<Vec<EnsTextRecordMulticallResult>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    if block.block_hash.trim().is_empty() {
        bail!("ENS text record Multicall3 block hash must not be empty");
    }

    let rpc_url = rpc_urls
        .url_for(chain_id)
        .with_context(|| format!("missing chain RPC URL for {chain_id}"))?;
    let rpc = JsonRpcHttpClient::new(rpc_url)?;
    let multicall3 = parse_address(multicall3_address, "multicall3")?;
    let (calls, call_indices, mut results) = multicall_calls_for_text_requests(requests);
    if calls.is_empty() {
        return finalize_text_multicall_results(results);
    }
    let call_count = calls.len();
    let calldata = abi::aggregate3Call { calls }.abi_encode();
    let call = json!({
        "to": format_address(multicall3),
        "data": hex_string(&calldata),
    });
    let block_selector = block_selector(block);
    let call_result = rpc
        .call("eth_call", vec![call, block_selector])
        .await
        .with_context(|| {
            format!(
                "failed to execute ENS text record Multicall3 batch on {chain_id} block {} ({}) with {} calls",
                block.block_number,
                block.block_hash,
                requests.len()
            )
        })?;
    let return_hex = match call_result.result {
        Ok(Value::String(value)) => value,
        Ok(other) => bail!("ENS text record Multicall3 eth_call returned non-string JSON {other}"),
        Err(error) => bail!(
            "ENS text record Multicall3 eth_call failed: {}",
            error.message
        ),
    };
    let return_data = hex_to_bytes(&return_hex)
        .context("ENS text record Multicall3 return data is not valid hex")?;
    let decoded_results = decode_multicall_results(&return_data)?;
    if decoded_results.len() != call_count {
        bail!(
            "ENS text record Multicall3 returned {} outcomes for {} calls",
            decoded_results.len(),
            call_count
        );
    }
    for (request_index, result) in call_indices.into_iter().zip(decoded_results) {
        results[request_index] = Some(result);
    }
    finalize_text_multicall_results(results)
}

fn multicall_calls_for_text_requests(
    requests: &[EnsTextRecordMulticallRequest],
) -> (
    Vec<abi::Call3>,
    Vec<usize>,
    Vec<Option<EnsTextRecordMulticallResult>>,
) {
    let mut calls = Vec::with_capacity(requests.len());
    let mut call_indices = Vec::with_capacity(requests.len());
    let mut results = vec![None; requests.len()];
    for (index, request) in requests.iter().enumerate() {
        match multicall_call_for_text_request(request) {
            Ok(call) => {
                calls.push(call);
                call_indices.push(index);
            }
            Err(error) => {
                results[index] = Some(EnsTextRecordMulticallResult::Failed {
                    message: format!("failed to build resolver text call: {error:#}"),
                });
            }
        }
    }
    (calls, call_indices, results)
}

fn finalize_text_multicall_results(
    results: Vec<Option<EnsTextRecordMulticallResult>>,
) -> Result<Vec<EnsTextRecordMulticallResult>> {
    results
        .into_iter()
        .enumerate()
        .map(|(index, result)| {
            result.with_context(|| format!("missing ENS text record Multicall3 result {index}"))
        })
        .collect()
}

fn multicall_call_for_text_request(request: &EnsTextRecordMulticallRequest) -> Result<abi::Call3> {
    let target = parse_address(&request.resolver_address, "resolver")?;
    let node = parse_namehash(&request.namehash)?;
    let calldata = resolver_calldata(
        &SupportedVerifiedResolutionRecordKey::Text,
        &format!("text:{}", request.text_key),
        node,
    )?;

    Ok(abi::Call3 {
        target,
        allowFailure: true,
        callData: Bytes::copy_from_slice(&calldata),
    })
}

fn decode_multicall_results(return_data: &[u8]) -> Result<Vec<EnsTextRecordMulticallResult>> {
    let decoded = abi::aggregate3Call::abi_decode_returns(return_data)
        .context("ENS text record Multicall3 return data is malformed")?;
    decoded
        .into_iter()
        .map(|result| {
            if !result.success {
                return Ok(EnsTextRecordMulticallResult::Failed {
                    message: "resolver text call returned failure from Multicall3".to_owned(),
                });
            }

            let value = match decode_selector_result(
                &SupportedVerifiedResolutionRecordKey::Text,
                result.returnData.as_ref(),
            ) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(EnsTextRecordMulticallResult::Failed {
                        message: format!("resolver text call return data is malformed: {error:#}"),
                    });
                }
            };
            Ok(match value {
                Some(value) => EnsTextRecordMulticallResult::Success { value },
                None => EnsTextRecordMulticallResult::NotFound,
            })
        })
        .collect()
}

fn parse_address(value: &str, context: &str) -> Result<Address> {
    Address::from_str(value).with_context(|| format!("failed to parse {context} address {value}"))
}

fn parse_namehash(value: &str) -> Result<[u8; 32]> {
    let bytes = hex_to_bytes(value)
        .with_context(|| format!("ENS text record Multicall3 namehash {value} is invalid"))?;
    <[u8; 32]>::try_from(bytes.as_slice()).with_context(|| {
        format!("ENS text record Multicall3 namehash {value} must contain exactly 32 bytes")
    })
}

fn format_address(address: Address) -> String {
    hex_string(address.as_slice())
}

fn block_selector(block: &EnsTextRecordMulticallBlock) -> Value {
    json!({
        "blockHash": block.block_hash,
        "requireCanonical": true,
    })
}

#[cfg(test)]
mod tests {
    use alloy_sol_types::SolValue;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };

    use super::*;

    async fn spawn_mock_rpc(result: Value) -> Result<(String, JoinHandle<Result<Value>>)> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("failed to bind mock RPC listener")?;
        let url = format!("http://{}", listener.local_addr()?);
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept mock RPC request")?;
            let request_payload = read_http_json_body(&mut socket).await?;
            write_json_rpc_response(&mut socket, result).await?;
            Ok(request_payload)
        });

        Ok((url, handle))
    }

    async fn read_http_json_body(socket: &mut tokio::net::TcpStream) -> Result<Value> {
        let mut buffer = Vec::new();
        let mut scratch = [0_u8; 1024];
        let (body_start, content_length) = loop {
            let bytes_read = socket
                .read(&mut scratch)
                .await
                .context("failed to read mock RPC request")?;
            if bytes_read == 0 {
                bail!("mock RPC request closed before headers finished");
            }
            buffer.extend_from_slice(&scratch[..bytes_read]);
            if let Some(body_start) = find_header_end(&buffer) {
                let headers = std::str::from_utf8(&buffer[..body_start])
                    .context("mock RPC request headers were not utf8")?;
                let content_length = parse_content_length(headers)?;
                break (body_start, content_length);
            }
        };

        while buffer.len() < body_start + content_length {
            let bytes_read = socket
                .read(&mut scratch)
                .await
                .context("failed to read mock RPC request body")?;
            if bytes_read == 0 {
                bail!("mock RPC request closed before body finished");
            }
            buffer.extend_from_slice(&scratch[..bytes_read]);
        }

        serde_json::from_slice(&buffer[body_start..body_start + content_length])
            .context("failed to parse mock RPC request body")
    }

    fn find_header_end(buffer: &[u8]) -> Option<usize> {
        buffer
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|position| position + 4)
    }

    fn parse_content_length(headers: &str) -> Result<usize> {
        headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>())
            })
            .transpose()
            .context("mock RPC request content-length was invalid")?
            .with_context(|| "mock RPC request did not include content-length")
    }

    async fn write_json_rpc_response(
        socket: &mut tokio::net::TcpStream,
        result: Value,
    ) -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .context("failed to write mock RPC response")
    }

    async fn join_request(handle: JoinHandle<Result<Value>>) -> Result<Value> {
        handle
            .await
            .context("mock RPC task panicked or was cancelled")?
    }

    #[test]
    fn ens_namehash_hex_hashes_names() -> Result<()> {
        assert_eq!(
            ens_namehash_hex("eth")?,
            "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae"
        );
        Ok(())
    }

    #[test]
    fn decodes_multicall_text_results() -> Result<()> {
        let text_return = ("ipfs://avatar".to_owned(),).abi_encode_params();
        let empty_return = ("".to_owned(),).abi_encode_params();
        let malformed_return = [0xab, 0xcd];
        let encoded = (vec![
            abi::Result3 {
                success: true,
                returnData: Bytes::copy_from_slice(&text_return),
            },
            abi::Result3 {
                success: true,
                returnData: Bytes::copy_from_slice(&empty_return),
            },
            abi::Result3 {
                success: false,
                returnData: Bytes::new(),
            },
            abi::Result3 {
                success: true,
                returnData: Bytes::copy_from_slice(&malformed_return),
            },
        ],)
            .abi_encode_params();

        let results = decode_multicall_results(&encoded)?;
        assert_eq!(
            &results[..3],
            [
                EnsTextRecordMulticallResult::Success {
                    value: "ipfs://avatar".to_owned()
                },
                EnsTextRecordMulticallResult::NotFound,
                EnsTextRecordMulticallResult::Failed {
                    message: "resolver text call returned failure from Multicall3".to_owned()
                },
            ]
        );
        let EnsTextRecordMulticallResult::Failed { message } = &results[3] else {
            panic!("malformed resolver return data must be reported as a per-call failure");
        };
        assert!(
            message.starts_with("resolver text call return data is malformed:"),
            "{message}"
        );
        Ok(())
    }

    #[test]
    fn encodes_text_call_targets() -> Result<()> {
        let call = multicall_call_for_text_request(&EnsTextRecordMulticallRequest {
            resolver_address: "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41".to_owned(),
            namehash: ens_namehash_hex("taytems.eth")?,
            text_key: "avatar".to_owned(),
        })?;

        assert_eq!(
            hex_string(call.target.as_slice()),
            "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41"
        );
        assert_eq!(&call.callData[..4], [0x59, 0xd1, 0xd4, 0x3c]);
        assert!(!call.callData.is_empty());
        Ok(())
    }

    #[test]
    fn invalid_text_call_namehashes_fail_per_request() -> Result<()> {
        let requests = vec![
            EnsTextRecordMulticallRequest {
                resolver_address: "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41".to_owned(),
                namehash: ens_namehash_hex("taytems.eth")?,
                text_key: "avatar".to_owned(),
            },
            EnsTextRecordMulticallRequest {
                resolver_address: "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41".to_owned(),
                namehash: "not-a-namehash".to_owned(),
                text_key: "avatar".to_owned(),
            },
        ];

        let (calls, call_indices, partial_results) = multicall_calls_for_text_requests(&requests);
        assert_eq!(calls.len(), 1);
        assert_eq!(call_indices, vec![0]);
        assert!(partial_results[0].is_none());
        let Some(EnsTextRecordMulticallResult::Failed { message }) = &partial_results[1] else {
            panic!("invalid ENS namehash must become a failed per-request result");
        };
        assert!(
            message.contains("namehash not-a-namehash is invalid"),
            "{message}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn text_multicall_splices_request_build_failures() -> Result<()> {
        let encoded = (vec![abi::Result3 {
            success: true,
            returnData: Bytes::copy_from_slice(&("ipfs://avatar".to_owned(),).abi_encode_params()),
        }],)
            .abi_encode_params();
        let (rpc_url, handle) = spawn_mock_rpc(Value::String(hex_string(&encoded))).await?;
        let rpc_urls = ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

        let results = execute_ens_text_record_multicall(
            &rpc_urls,
            "ethereum-mainnet",
            MULTICALL3_ADDRESS,
            &EnsTextRecordMulticallBlock {
                block_number: 12_345,
                block_hash: "0xabc".to_owned(),
            },
            &[
                EnsTextRecordMulticallRequest {
                    resolver_address: "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41".to_owned(),
                    namehash: ens_namehash_hex("taytems.eth")?,
                    text_key: "avatar".to_owned(),
                },
                EnsTextRecordMulticallRequest {
                    resolver_address: "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41".to_owned(),
                    namehash: "0x1234".to_owned(),
                    text_key: "avatar".to_owned(),
                },
            ],
        )
        .await?;

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            EnsTextRecordMulticallResult::Success {
                value: "ipfs://avatar".to_owned()
            }
        );
        let EnsTextRecordMulticallResult::Failed { message } = &results[1] else {
            panic!("invalid ENS namehash must stay aligned with the original request");
        };
        assert!(
            message.contains("namehash 0x1234 must contain exactly 32 bytes"),
            "{message}"
        );

        let request = join_request(handle).await?;
        assert_eq!(request["method"], "eth_call");
        assert_eq!(
            request["params"][0]["to"],
            MULTICALL3_ADDRESS.to_lowercase()
        );
        assert_eq!(
            request["params"][1],
            json!({
                "blockHash": "0xabc",
                "requireCanonical": true,
            })
        );
        Ok(())
    }

    #[test]
    fn text_multicall_block_selector_is_hash_pinned() {
        let block = EnsTextRecordMulticallBlock {
            block_number: 12_345,
            block_hash: "0xabc".to_owned(),
        };

        assert_eq!(
            block_selector(&block),
            json!({
                "blockHash": "0xabc",
                "requireCanonical": true,
            })
        );
    }
}
