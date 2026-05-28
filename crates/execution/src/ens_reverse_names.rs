use std::str::FromStr;

use alloy_primitives::{Address, B256, Bytes};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::{
    ens_resolution_abi::{hex_string, hex_to_bytes},
    rpc::{ChainRpcUrls, JsonRpcHttpClient},
};

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
        function name(bytes32 node) external view returns (string);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsReverseNameMulticallRequest {
    pub resolver_address: String,
    pub reverse_node: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsReverseNameMulticallBlock {
    pub block_number: i64,
    pub block_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnsReverseNameMulticallResult {
    Success { value: String },
    NotFound,
    Failed { message: String },
}

pub async fn execute_ens_reverse_name_multicall(
    rpc_urls: &ChainRpcUrls,
    chain_id: &str,
    multicall3_address: &str,
    block: &EnsReverseNameMulticallBlock,
    requests: &[EnsReverseNameMulticallRequest],
) -> Result<Vec<EnsReverseNameMulticallResult>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    if block.block_hash.trim().is_empty() {
        bail!("ENS reverse-name Multicall3 block hash must not be empty");
    }

    let rpc_url = rpc_urls
        .url_for(chain_id)
        .with_context(|| format!("missing chain RPC URL for {chain_id}"))?;
    let rpc = JsonRpcHttpClient::new(rpc_url)?;
    let multicall3 = parse_address(multicall3_address, "multicall3")?;
    let calls = requests
        .iter()
        .map(multicall_call_for_reverse_name_request)
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
                "failed to execute ENS reverse-name Multicall3 batch on {chain_id} block {} ({}) with {} calls",
                block.block_number,
                block.block_hash,
                requests.len()
            )
        })?;
    let return_hex = match call_result.result {
        Ok(Value::String(value)) => value,
        Ok(other) => bail!("ENS reverse-name Multicall3 eth_call returned non-string JSON {other}"),
        Err(error) => bail!(
            "ENS reverse-name Multicall3 eth_call failed: {}",
            error.message
        ),
    };
    let return_data = hex_to_bytes(&return_hex)
        .context("ENS reverse-name Multicall3 return data is not valid hex")?;
    decode_multicall_results(&return_data)
}

fn multicall_call_for_reverse_name_request(
    request: &EnsReverseNameMulticallRequest,
) -> Result<abi::Call3> {
    let target = parse_address(&request.resolver_address, "resolver")?;
    let node = parse_node(&request.reverse_node)?;
    let calldata = abi::nameCall { node }.abi_encode();

    Ok(abi::Call3 {
        target,
        allowFailure: true,
        callData: Bytes::copy_from_slice(&calldata),
    })
}

fn decode_multicall_results(return_data: &[u8]) -> Result<Vec<EnsReverseNameMulticallResult>> {
    let decoded = abi::aggregate3Call::abi_decode_returns(return_data)
        .context("ENS reverse-name Multicall3 return data is malformed")?;
    decoded
        .into_iter()
        .map(|result| {
            if !result.success {
                return Ok(EnsReverseNameMulticallResult::Failed {
                    message: "resolver name call returned failure from Multicall3".to_owned(),
                });
            }

            let value = match abi::nameCall::abi_decode_returns_validate(result.returnData.as_ref())
            {
                Ok(value) => value,
                Err(error) => {
                    return Ok(EnsReverseNameMulticallResult::Failed {
                        message: format!("resolver name call return data is malformed: {error:#}"),
                    });
                }
            };
            Ok(if value.is_empty() {
                EnsReverseNameMulticallResult::NotFound
            } else {
                EnsReverseNameMulticallResult::Success { value }
            })
        })
        .collect()
}

fn parse_node(value: &str) -> Result<B256> {
    let bytes = hex_to_bytes(value).with_context(|| format!("reverse node {value} is not hex"))?;
    if bytes.len() != 32 {
        bail!("reverse node {value} must be 32 bytes");
    }
    Ok(B256::from_slice(&bytes))
}

fn parse_address(value: &str, context: &str) -> Result<Address> {
    Address::from_str(value).with_context(|| format!("failed to parse {context} address {value}"))
}

fn format_address(address: Address) -> String {
    hex_string(address.as_slice())
}

fn block_selector(block: &EnsReverseNameMulticallBlock) -> Value {
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
    fn decodes_multicall_reverse_name_results() -> Result<()> {
        let name_return = ("vitalik.eth".to_owned(),).abi_encode_params();
        let empty_return = ("".to_owned(),).abi_encode_params();
        let malformed_return = [0xab, 0xcd];
        let encoded = (vec![
            abi::Result3 {
                success: true,
                returnData: Bytes::copy_from_slice(&name_return),
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
                EnsReverseNameMulticallResult::Success {
                    value: "vitalik.eth".to_owned()
                },
                EnsReverseNameMulticallResult::NotFound,
                EnsReverseNameMulticallResult::Failed {
                    message: "resolver name call returned failure from Multicall3".to_owned()
                },
            ]
        );
        let EnsReverseNameMulticallResult::Failed { message } = &results[3] else {
            panic!("malformed resolver return data must be reported as a per-call failure");
        };
        assert!(
            message.starts_with("resolver name call return data is malformed:"),
            "{message}"
        );
        Ok(())
    }

    #[test]
    fn encodes_reverse_name_call_targets() -> Result<()> {
        let call = multicall_call_for_reverse_name_request(&EnsReverseNameMulticallRequest {
            resolver_address: "0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned(),
            reverse_node: "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_owned(),
        })?;

        assert_eq!(
            hex_string(call.target.as_slice()),
            "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
        );
        assert_eq!(&call.callData[..4], [0x69, 0x1f, 0x34, 0x31]);
        assert!(!call.callData.is_empty());
        Ok(())
    }

    #[test]
    fn reverse_name_multicall_block_selector_is_hash_pinned() {
        let block = EnsReverseNameMulticallBlock {
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
