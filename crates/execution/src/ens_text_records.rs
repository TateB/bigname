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
    pub name: String,
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
    let calls = requests
        .iter()
        .map(multicall_call_for_text_request)
        .collect::<Result<Vec<_>>>()?;
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
    decode_multicall_results(&return_data)
}

fn multicall_call_for_text_request(request: &EnsTextRecordMulticallRequest) -> Result<abi::Call3> {
    let target = parse_address(&request.resolver_address, "resolver")?;
    let node = namehash(&request.name)
        .with_context(|| format!("failed to namehash ENS name {}", request.name))?;
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

    use super::*;

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
            name: "taytems.eth".to_owned(),
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
