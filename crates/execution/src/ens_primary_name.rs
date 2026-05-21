use alloy_primitives::{Address, B256};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result, bail};
use bigname_storage::SupportedVerifiedResolutionRecordKey;
use serde_json::{Value, json};

use crate::ens_resolution_abi::{
    decode_selector_result, decode_universal_resolver_result, dns_encode_name, hex_string,
    hex_to_bytes, namehash, resolver_record_call, universal_resolver_call,
};
use crate::ens_resolution_ccip::follow_ccip_read;
use crate::rpc::{ChainRpcUrls, JsonRpcCallResult, JsonRpcHttpClient};
use crate::{ENS_REGISTRY_ADDRESS, ENS_UNIVERSAL_RESOLVER_ADDRESS, ETHEREUM_MAINNET_CHAIN_ID};

mod abi {
    use super::*;

    sol! {
        function resolver(bytes32 node) external view returns (address);
        function name(bytes32 node) external view returns (string);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OnDemandEnsPrimaryNameRequest<'a> {
    pub normalized_address: &'a str,
    pub chain_rpc_urls: &'a ChainRpcUrls,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OnDemandEnsPrimaryName {
    pub name: String,
    pub resolver_address: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OnDemandEnsPrimaryNameVerificationRequest<'a> {
    pub normalized_address: &'a str,
    pub normalized_name: &'a str,
    pub chain_rpc_urls: &'a ChainRpcUrls,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OnDemandEnsPrimaryNameVerification {
    pub resolved_address: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OnDemandEnsPrimaryNameErrorKind {
    Configuration,
    Execution,
}

#[derive(Debug)]
pub struct OnDemandEnsPrimaryNameError {
    kind: OnDemandEnsPrimaryNameErrorKind,
    message: String,
}

impl OnDemandEnsPrimaryNameError {
    fn configuration(message: impl Into<String>) -> Self {
        Self {
            kind: OnDemandEnsPrimaryNameErrorKind::Configuration,
            message: message.into(),
        }
    }

    fn execution(message: impl Into<String>) -> Self {
        Self {
            kind: OnDemandEnsPrimaryNameErrorKind::Execution,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> OnDemandEnsPrimaryNameErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for OnDemandEnsPrimaryNameError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for OnDemandEnsPrimaryNameError {}

pub async fn lookup_ens_reverse_primary_name(
    request: OnDemandEnsPrimaryNameRequest<'_>,
) -> std::result::Result<Option<OnDemandEnsPrimaryName>, OnDemandEnsPrimaryNameError> {
    let rpc = primary_name_rpc(request.chain_rpc_urls)?;

    lookup_ens_reverse_primary_name_with_rpc(&rpc, request.normalized_address).await
}

pub async fn verify_ens_primary_name_forward_address(
    request: OnDemandEnsPrimaryNameVerificationRequest<'_>,
) -> std::result::Result<OnDemandEnsPrimaryNameVerification, OnDemandEnsPrimaryNameError> {
    let rpc = primary_name_rpc(request.chain_rpc_urls)?;

    verify_ens_primary_name_forward_address_with_rpc(
        &rpc,
        request.normalized_address,
        request.normalized_name,
    )
    .await
}

fn primary_name_rpc(
    chain_rpc_urls: &ChainRpcUrls,
) -> std::result::Result<JsonRpcHttpClient, OnDemandEnsPrimaryNameError> {
    let Some(provider_url) = chain_rpc_urls.url_for(ETHEREUM_MAINNET_CHAIN_ID) else {
        return Err(OnDemandEnsPrimaryNameError::configuration(format!(
            "primary-name RPC provider for {ETHEREUM_MAINNET_CHAIN_ID} is not configured; set BIGNAME_API_CHAIN_RPC_URLS={ETHEREUM_MAINNET_CHAIN_ID}=<url>"
        )));
    };
    JsonRpcHttpClient::new(provider_url).map_err(|error| {
        OnDemandEnsPrimaryNameError::configuration(format!(
            "primary-name RPC provider for {ETHEREUM_MAINNET_CHAIN_ID} is invalid: {error}"
        ))
    })
}

async fn lookup_ens_reverse_primary_name_with_rpc(
    rpc: &JsonRpcHttpClient,
    normalized_address: &str,
) -> std::result::Result<Option<OnDemandEnsPrimaryName>, OnDemandEnsPrimaryNameError> {
    let reverse_node = reverse_node(normalized_address).map_err(|error| {
        OnDemandEnsPrimaryNameError::configuration(format!(
            "failed to build ENS reverse node for {normalized_address}: {error}"
        ))
    })?;
    let resolver_address = lookup_reverse_resolver(rpc, reverse_node).await?;
    if resolver_address == Address::ZERO {
        return Ok(None);
    }

    let name = lookup_reverse_name(rpc, resolver_address, reverse_node).await?;
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    Ok(Some(OnDemandEnsPrimaryName {
        name: trimmed.to_owned(),
        resolver_address: hex_string(resolver_address.as_slice()),
    }))
}

async fn verify_ens_primary_name_forward_address_with_rpc(
    rpc: &JsonRpcHttpClient,
    normalized_address: &str,
    normalized_name: &str,
) -> std::result::Result<OnDemandEnsPrimaryNameVerification, OnDemandEnsPrimaryNameError> {
    reverse_node(normalized_address).map_err(|error| {
        OnDemandEnsPrimaryNameError::configuration(format!(
            "failed to validate ENS primary-name verification address {normalized_address}: {error}"
        ))
    })?;

    let dns_name = dns_encode_name(normalized_name).map_err(|error| {
        OnDemandEnsPrimaryNameError::configuration(format!(
            "failed to DNS-encode ENS primary-name {normalized_name}: {error}"
        ))
    })?;
    let node = namehash(normalized_name).map_err(|error| {
        OnDemandEnsPrimaryNameError::configuration(format!(
            "failed to build ENS primary-name node for {normalized_name}: {error}"
        ))
    })?;
    let selector = SupportedVerifiedResolutionRecordKey::Addr {
        coin_type: "60".to_owned(),
    };
    let resolver_call = resolver_record_call(&selector, "addr:60", node).map_err(|error| {
        OnDemandEnsPrimaryNameError::configuration(format!(
            "failed to build ENS primary-name forward addr:60 call for {normalized_name}: {error}"
        ))
    })?;
    let universal_call = universal_resolver_call(&dns_name, resolver_call.calldata());
    let return_data = eth_call_following_ccip(
        rpc,
        ENS_UNIVERSAL_RESOLVER_ADDRESS,
        universal_call.calldata(),
    )
    .await?;
    let selector_return = decode_universal_resolver_result(&return_data).map_err(|error| {
        OnDemandEnsPrimaryNameError::execution(format!(
            "ENS Universal Resolver return data is malformed for {normalized_name}: {error}"
        ))
    })?;
    let resolved_address =
        decode_selector_result(&selector, &selector_return).map_err(|error| {
            OnDemandEnsPrimaryNameError::execution(format!(
                "ENS primary-name addr:60 return data is malformed for {normalized_name}: {error}"
            ))
        })?;

    Ok(OnDemandEnsPrimaryNameVerification { resolved_address })
}

fn reverse_node(normalized_address: &str) -> Result<[u8; 32]> {
    let label = normalized_address
        .strip_prefix("0x")
        .with_context(|| "normalized address must be 0x-prefixed")?;
    if label.len() != 40 || !label.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("normalized address must be a 20-byte hex address");
    }
    namehash(&format!("{label}.addr.reverse"))
}

async fn lookup_reverse_resolver(
    rpc: &JsonRpcHttpClient,
    reverse_node: [u8; 32],
) -> std::result::Result<Address, OnDemandEnsPrimaryNameError> {
    let calldata = abi::resolverCall {
        node: B256::from(reverse_node),
    }
    .abi_encode();
    let return_data = eth_call(rpc, ENS_REGISTRY_ADDRESS, &calldata).await?;
    abi::resolverCall::abi_decode_returns_validate(&return_data).map_err(|error| {
        OnDemandEnsPrimaryNameError::execution(format!(
            "ENS registry resolver(bytes32) return data is malformed: {error}"
        ))
    })
}

async fn lookup_reverse_name(
    rpc: &JsonRpcHttpClient,
    resolver_address: Address,
    reverse_node: [u8; 32],
) -> std::result::Result<String, OnDemandEnsPrimaryNameError> {
    let calldata = abi::nameCall {
        node: B256::from(reverse_node),
    }
    .abi_encode();
    let return_data = eth_call(rpc, &hex_string(resolver_address.as_slice()), &calldata).await?;
    abi::nameCall::abi_decode_returns_validate(&return_data).map_err(|error| {
        OnDemandEnsPrimaryNameError::execution(format!(
            "ENS reverse resolver name(bytes32) return data is malformed: {error}"
        ))
    })
}

async fn eth_call(
    rpc: &JsonRpcHttpClient,
    to: &str,
    calldata: &[u8],
) -> std::result::Result<Vec<u8>, OnDemandEnsPrimaryNameError> {
    let result = eth_call_result(rpc, to, calldata).await?;
    decode_eth_call_result(result)
}

async fn eth_call_following_ccip(
    rpc: &JsonRpcHttpClient,
    to: &str,
    calldata: &[u8],
) -> std::result::Result<Vec<u8>, OnDemandEnsPrimaryNameError> {
    let block_selector = Value::String("latest".to_owned());
    let result =
        eth_call_result_with_block_selector(rpc, to, calldata, block_selector.clone()).await?;
    let result = match &result.result {
        Err(error) => match follow_ccip_read(rpc, error, &block_selector).await {
            Ok(Some(outcome)) => outcome.result,
            Ok(None) | Err(_) => result,
        },
        Ok(_) => result,
    };
    decode_eth_call_result(result)
}

async fn eth_call_result(
    rpc: &JsonRpcHttpClient,
    to: &str,
    calldata: &[u8],
) -> std::result::Result<JsonRpcCallResult, OnDemandEnsPrimaryNameError> {
    eth_call_result_with_block_selector(rpc, to, calldata, Value::String("latest".to_owned())).await
}

async fn eth_call_result_with_block_selector(
    rpc: &JsonRpcHttpClient,
    to: &str,
    calldata: &[u8],
    block_selector: Value,
) -> std::result::Result<JsonRpcCallResult, OnDemandEnsPrimaryNameError> {
    rpc.call(
        "eth_call",
        vec![
            json!({
                "to": to,
                "data": hex_string(calldata),
            }),
            block_selector,
        ],
    )
    .await
    .map_err(|error| {
        OnDemandEnsPrimaryNameError::execution(format!(
            "failed to execute ENS primary-name RPC call: {error}"
        ))
    })
}

fn decode_eth_call_result(
    result: JsonRpcCallResult,
) -> std::result::Result<Vec<u8>, OnDemandEnsPrimaryNameError> {
    let value = result.result.map_err(|error| {
        OnDemandEnsPrimaryNameError::execution(format!(
            "ENS primary-name RPC call failed: {}",
            error.message
        ))
    })?;
    let hex_value = value.as_str().ok_or_else(|| {
        OnDemandEnsPrimaryNameError::execution("ENS primary-name RPC result was not a hex string")
    })?;
    hex_to_bytes(hex_value).map_err(|error| {
        OnDemandEnsPrimaryNameError::execution(format!(
            "ENS primary-name RPC result is malformed: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::hex;
    use alloy_sol_types::SolValue;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };

    async fn spawn_mock_rpc(
        responses: Vec<Value>,
    ) -> Result<(String, JoinHandle<Result<Vec<Value>>>)> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("failed to bind mock RPC listener")?;
        let url = format!("http://{}", listener.local_addr()?);
        let handle = tokio::spawn(async move {
            let mut requests = Vec::new();
            for response_result in responses {
                let (mut socket, _) = listener
                    .accept()
                    .await
                    .context("failed to accept mock RPC request")?;
                let request_payload = read_http_json_body(&mut socket).await?;
                requests.push(request_payload);
                write_json_rpc_response(&mut socket, response_result).await?;
            }
            Ok(requests)
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

    async fn join_requests(handle: JoinHandle<Result<Vec<Value>>>) -> Result<Vec<Value>> {
        handle
            .await
            .context("mock RPC task panicked or was cancelled")?
    }

    fn encode_universal_resolver_return(result: Vec<u8>, resolver_address: Address) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&abi_word_usize(64));
        let mut resolver_word = [0_u8; 32];
        resolver_word[12..].copy_from_slice(resolver_address.as_slice());
        encoded.extend_from_slice(&resolver_word);
        encoded.extend_from_slice(&abi_word_usize(result.len()));
        encoded.extend_from_slice(&result);
        let padding = (32 - (result.len() % 32)) % 32;
        encoded.extend(std::iter::repeat_n(0_u8, padding));
        encoded
    }

    fn abi_word_usize(value: usize) -> [u8; 32] {
        let mut word = [0_u8; 32];
        word[24..].copy_from_slice(&(value as u64).to_be_bytes());
        word
    }

    #[test]
    fn builds_expected_reverse_node() {
        let node = reverse_node("0x8e8db5ccef88cca9d624701db544989c996e3216")
            .expect("reverse node must build");

        assert_eq!(
            hex::encode(node),
            "658ecd2fe8aadf31c3ee6126e11967ff852cfd7592ef26c28e0b65c30e4e8628"
        );
    }

    #[test]
    fn rejects_non_normalized_reverse_node_address() {
        assert!(reverse_node("8e8db5ccef88cca9d624701db544989c996e3216").is_err());
        assert!(reverse_node("0xabc").is_err());
    }

    #[tokio::test]
    async fn lookup_ens_reverse_primary_name_executes_configured_rpc_calls() -> Result<()> {
        let resolver_address = "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
            .parse::<Address>()
            .context("resolver address must parse")?;
        let (rpc_url, handle) = spawn_mock_rpc(vec![
            Value::String(hex_string(&resolver_address.abi_encode())),
            Value::String(hex_string(&"taytems.eth".to_owned().abi_encode())),
        ])
        .await?;
        let chain_rpc_urls =
            ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

        let result = lookup_ens_reverse_primary_name(OnDemandEnsPrimaryNameRequest {
            normalized_address: "0x8e8db5ccef88cca9d624701db544989c996e3216",
            chain_rpc_urls: &chain_rpc_urls,
        })
        .await
        .expect("mock RPC lookup must succeed")
        .expect("mock RPC lookup must return a claim");

        assert_eq!(
            result,
            OnDemandEnsPrimaryName {
                name: "taytems.eth".to_owned(),
                resolver_address: "0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned(),
            }
        );

        let reverse_node = reverse_node("0x8e8db5ccef88cca9d624701db544989c996e3216")?;
        let expected_resolver_call = hex_string(
            &abi::resolverCall {
                node: B256::from(reverse_node),
            }
            .abi_encode(),
        );
        let expected_name_call = hex_string(
            &abi::nameCall {
                node: B256::from(reverse_node),
            }
            .abi_encode(),
        );

        let requests = join_requests(handle).await?;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["method"], "eth_call");
        assert_eq!(requests[0]["params"][0]["to"], ENS_REGISTRY_ADDRESS);
        assert_eq!(requests[0]["params"][0]["data"], expected_resolver_call);
        assert_eq!(requests[0]["params"][1], "latest");
        assert_eq!(requests[1]["method"], "eth_call");
        assert_eq!(
            requests[1]["params"][0]["to"],
            "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
        );
        assert_eq!(requests[1]["params"][0]["data"], expected_name_call);
        assert_eq!(requests[1]["params"][1], "latest");

        Ok(())
    }

    #[tokio::test]
    async fn lookup_ens_reverse_primary_name_returns_none_for_zero_resolver() -> Result<()> {
        let (rpc_url, handle) =
            spawn_mock_rpc(vec![Value::String(hex_string(&Address::ZERO.abi_encode()))]).await?;
        let chain_rpc_urls =
            ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

        let result = lookup_ens_reverse_primary_name(OnDemandEnsPrimaryNameRequest {
            normalized_address: "0x8e8db5ccef88cca9d624701db544989c996e3216",
            chain_rpc_urls: &chain_rpc_urls,
        })
        .await
        .expect("mock RPC lookup must not error");

        assert_eq!(result, None);
        assert_eq!(join_requests(handle).await?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn lookup_ens_reverse_primary_name_rejects_malformed_rpc_return() -> Result<()> {
        let (rpc_url, handle) = spawn_mock_rpc(vec![Value::String("0x1234".to_owned())]).await?;
        let chain_rpc_urls =
            ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

        let error = lookup_ens_reverse_primary_name(OnDemandEnsPrimaryNameRequest {
            normalized_address: "0x8e8db5ccef88cca9d624701db544989c996e3216",
            chain_rpc_urls: &chain_rpc_urls,
        })
        .await
        .expect_err("malformed RPC return must fail");

        assert_eq!(error.kind(), OnDemandEnsPrimaryNameErrorKind::Execution);
        assert_eq!(join_requests(handle).await?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn verify_ens_primary_name_forward_address_executes_universal_resolver_call() -> Result<()>
    {
        let resolver_address = "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
            .parse::<Address>()
            .context("resolver address must parse")?;
        let requested_address = "0x8e8db5ccef88cca9d624701db544989c996e3216"
            .parse::<Address>()
            .context("requested address must parse")?;
        let universal_return =
            encode_universal_resolver_return(requested_address.abi_encode(), resolver_address);
        let (rpc_url, handle) =
            spawn_mock_rpc(vec![Value::String(hex_string(&universal_return))]).await?;
        let chain_rpc_urls =
            ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

        let result =
            verify_ens_primary_name_forward_address(OnDemandEnsPrimaryNameVerificationRequest {
                normalized_address: "0x8e8db5ccef88cca9d624701db544989c996e3216",
                normalized_name: "taytems.eth",
                chain_rpc_urls: &chain_rpc_urls,
            })
            .await
            .expect("mock RPC verification must succeed");

        assert_eq!(
            result,
            OnDemandEnsPrimaryNameVerification {
                resolved_address: Some("0x8e8db5ccef88cca9d624701db544989c996e3216".to_owned()),
            }
        );

        let node = namehash("taytems.eth")?;
        let selector = SupportedVerifiedResolutionRecordKey::Addr {
            coin_type: "60".to_owned(),
        };
        let resolver_call = resolver_record_call(&selector, "addr:60", node)?;
        let universal_call =
            universal_resolver_call(&dns_encode_name("taytems.eth")?, resolver_call.calldata());
        let requests = join_requests(handle).await?;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["method"], "eth_call");
        assert_eq!(
            requests[0]["params"][0]["to"],
            ENS_UNIVERSAL_RESOLVER_ADDRESS
        );
        assert_eq!(
            requests[0]["params"][0]["data"],
            universal_call.calldata_hex()
        );
        assert_eq!(requests[0]["params"][1], "latest");
        Ok(())
    }

    #[tokio::test]
    async fn verify_ens_primary_name_forward_address_returns_none_for_zero_addr() -> Result<()> {
        let resolver_address = "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
            .parse::<Address>()
            .context("resolver address must parse")?;
        let universal_return =
            encode_universal_resolver_return(Address::ZERO.abi_encode(), resolver_address);
        let (rpc_url, handle) =
            spawn_mock_rpc(vec![Value::String(hex_string(&universal_return))]).await?;
        let chain_rpc_urls =
            ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

        let result =
            verify_ens_primary_name_forward_address(OnDemandEnsPrimaryNameVerificationRequest {
                normalized_address: "0x8e8db5ccef88cca9d624701db544989c996e3216",
                normalized_name: "taytems.eth",
                chain_rpc_urls: &chain_rpc_urls,
            })
            .await
            .expect("mock RPC verification must succeed");

        assert_eq!(
            result,
            OnDemandEnsPrimaryNameVerification {
                resolved_address: None,
            }
        );
        assert_eq!(join_requests(handle).await?.len(), 1);
        Ok(())
    }
}
