use alloy_primitives::{Address, B256};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result, bail};
use bigname_storage::SupportedVerifiedResolutionRecordKey;
use serde_json::{Value, json};

use crate::ens_resolution_abi::{
    decode_selector_result, decode_universal_resolver_result, dns_encode_name, hex_string,
    hex_to_bytes, namehash, resolver_record_call, universal_resolver_call,
};
use crate::ens_resolution_ccip::{follow_ccip_read, rpc_error_contains_offchain_lookup};
use crate::rpc::{ChainRpcUrls, JsonRpcCallError, JsonRpcCallResult, JsonRpcHttpClient};
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
pub struct EnsForwardAddressLookupRequest<'a> {
    pub normalized_name: &'a str,
    pub chain_rpc_urls: &'a ChainRpcUrls,
    pub block_number: i64,
    pub block_hash: &'a str,
    pub follow_ccip_read: bool,
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
    plain_execution_revert: bool,
    offchain_lookup_required: bool,
}

impl OnDemandEnsPrimaryNameError {
    fn configuration(message: impl Into<String>) -> Self {
        Self {
            kind: OnDemandEnsPrimaryNameErrorKind::Configuration,
            message: message.into(),
            plain_execution_revert: false,
            offchain_lookup_required: false,
        }
    }

    fn execution(message: impl Into<String>) -> Self {
        Self::execution_with_rpc_flags(message, false, false)
    }

    fn execution_with_rpc_flags(
        message: impl Into<String>,
        plain_execution_revert: bool,
        offchain_lookup_required: bool,
    ) -> Self {
        Self {
            kind: OnDemandEnsPrimaryNameErrorKind::Execution,
            message: message.into(),
            plain_execution_revert,
            offchain_lookup_required,
        }
    }

    pub const fn kind(&self) -> OnDemandEnsPrimaryNameErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub const fn is_plain_execution_revert(&self) -> bool {
        self.plain_execution_revert
    }

    pub const fn is_offchain_lookup_required(&self) -> bool {
        self.offchain_lookup_required
    }

    #[doc(hidden)]
    pub fn synthetic_execution_rpc_error_for_tests(
        message: impl Into<String>,
        plain_execution_revert: bool,
        offchain_lookup_required: bool,
    ) -> Self {
        Self::execution_with_rpc_flags(message, plain_execution_revert, offchain_lookup_required)
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

pub async fn lookup_ens_forward_address_at_block(
    request: EnsForwardAddressLookupRequest<'_>,
) -> std::result::Result<Option<String>, OnDemandEnsPrimaryNameError> {
    let rpc = primary_name_rpc(request.chain_rpc_urls)?;
    resolve_ens_primary_name_forward_address_with_rpc(
        &rpc,
        request.normalized_name,
        hash_pinned_block_selector(request.block_hash),
        request.follow_ccip_read,
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
    let resolved_address = resolve_ens_primary_name_forward_address_with_rpc(
        rpc,
        normalized_name,
        Value::String("latest".to_owned()),
        true,
    )
    .await?;

    Ok(OnDemandEnsPrimaryNameVerification { resolved_address })
}

async fn resolve_ens_primary_name_forward_address_with_rpc(
    rpc: &JsonRpcHttpClient,
    normalized_name: &str,
    block_selector: Value,
    follow_ccip: bool,
) -> std::result::Result<Option<String>, OnDemandEnsPrimaryNameError> {
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
    let return_data = if follow_ccip {
        eth_call_following_ccip_with_block_selector(
            rpc,
            ENS_UNIVERSAL_RESOLVER_ADDRESS,
            universal_call.calldata(),
            block_selector,
        )
        .await?
    } else {
        eth_call_with_block_selector(
            rpc,
            ENS_UNIVERSAL_RESOLVER_ADDRESS,
            universal_call.calldata(),
            block_selector,
        )
        .await?
    };
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

    Ok(resolved_address)
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

async fn eth_call_with_block_selector(
    rpc: &JsonRpcHttpClient,
    to: &str,
    calldata: &[u8],
    block_selector: Value,
) -> std::result::Result<Vec<u8>, OnDemandEnsPrimaryNameError> {
    let result = eth_call_result_with_block_selector(rpc, to, calldata, block_selector).await?;
    decode_eth_call_result(result)
}

async fn eth_call_following_ccip_with_block_selector(
    rpc: &JsonRpcHttpClient,
    to: &str,
    calldata: &[u8],
    block_selector: Value,
) -> std::result::Result<Vec<u8>, OnDemandEnsPrimaryNameError> {
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

fn hash_pinned_block_selector(block_hash: &str) -> Value {
    json!({
        "blockHash": block_hash,
        "requireCanonical": true,
    })
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
    let value = result.result.map_err(primary_name_rpc_call_error)?;
    let hex_value = value.as_str().ok_or_else(|| {
        OnDemandEnsPrimaryNameError::execution("ENS primary-name RPC result was not a hex string")
    })?;
    hex_to_bytes(hex_value).map_err(|error| {
        OnDemandEnsPrimaryNameError::execution(format!(
            "ENS primary-name RPC result is malformed: {error}"
        ))
    })
}

fn primary_name_rpc_call_error(error: JsonRpcCallError) -> OnDemandEnsPrimaryNameError {
    let offchain_lookup_required = match rpc_error_contains_offchain_lookup(&error) {
        Ok(value) => value,
        Err(decode_error) => {
            return OnDemandEnsPrimaryNameError::execution(format!(
                "ENS primary-name RPC call failed: {}; failed to inspect RPC revert data: {decode_error}",
                error.message
            ));
        }
    };
    let plain_execution_revert = error.message == "execution reverted" && !offchain_lookup_required;
    let offchain_context = if offchain_lookup_required {
        " (OffchainLookup required)"
    } else {
        ""
    };
    OnDemandEnsPrimaryNameError::execution_with_rpc_flags(
        format!(
            "ENS primary-name RPC call failed: {}{offchain_context}",
            error.message
        ),
        plain_execution_revert,
        offchain_lookup_required,
    )
}

#[cfg(test)]
#[path = "ens_primary_name/tests.rs"]
mod tests;
