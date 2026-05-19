use std::collections::BTreeMap;

use alloy_json_rpc::{
    Id, Request as JsonRpcRequest, RequestPacket, ResponsePacket, ResponsePayload,
};
use alloy_transport_http::Http;
use anyhow::{Context, Result, bail};
use reqwest::Url;
use serde_json::Value;
use tower::Service;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChainRpcUrls {
    urls: BTreeMap<String, String>,
}

impl ChainRpcUrls {
    pub fn from_entries(entries: &[String]) -> Result<Self> {
        let mut urls = BTreeMap::new();
        for entry in entries {
            for item in entry
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
            {
                let (chain, url) = item.split_once('=').with_context(|| {
                    format!("invalid chain RPC entry {item}; expected <chain>=<url>")
                })?;
                let chain = chain.trim();
                let url = url.trim();
                if chain.is_empty() || url.is_empty() {
                    bail!("invalid chain RPC entry {item}; expected non-empty <chain>=<url>");
                }
                if urls.insert(chain.to_owned(), url.to_owned()).is_some() {
                    bail!("duplicate chain RPC entry for {chain}");
                }
            }
        }

        Ok(Self { urls })
    }

    pub fn from_comma_delimited(value: &str) -> Result<Self> {
        Self::from_entries(&[value.to_owned()])
    }

    pub fn is_empty(&self) -> bool {
        self.urls.is_empty()
    }

    pub fn url_for(&self, chain_id: &str) -> Option<&str> {
        self.urls.get(chain_id).map(String::as_str)
    }
}

#[derive(Clone)]
pub(crate) struct JsonRpcHttpClient {
    transport: Http<reqwest::Client>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JsonRpcCallResult {
    pub(crate) request_payload: Value,
    pub(crate) response_payload: Value,
    pub(crate) result: std::result::Result<Value, JsonRpcCallError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JsonRpcCallError {
    pub(crate) code: Option<i64>,
    pub(crate) message: String,
    pub(crate) data: Option<Value>,
}

impl JsonRpcHttpClient {
    pub(crate) fn new(endpoint: &str) -> Result<Self> {
        let endpoint = endpoint
            .parse::<Url>()
            .with_context(|| format!("failed to parse RPC endpoint {endpoint}"))?;
        if endpoint.scheme() != "http" {
            bail!(
                "unsupported RPC endpoint scheme for {endpoint}; bootstrap on-demand execution currently supports only http:// URLs"
            );
        }

        Ok(Self {
            transport: Http::with_client(reqwest::Client::new(), endpoint),
        })
    }

    pub(crate) async fn call(&self, method: &str, params: Vec<Value>) -> Result<JsonRpcCallResult> {
        let request = JsonRpcRequest::new(method.to_owned(), Id::Number(1), params)
            .serialize()
            .context("failed to encode JSON-RPC request")?;
        let request_payload = serde_json::from_str(request.serialized().get())
            .context("failed to decode serialized JSON-RPC request")?;
        let (response_payload, result) = self.send_json_rpc_request(method, request).await?;

        Ok(JsonRpcCallResult {
            request_payload,
            response_payload,
            result,
        })
    }

    async fn send_json_rpc_request(
        &self,
        request_context: &str,
        request: alloy_json_rpc::SerializedRequest,
    ) -> Result<(Value, std::result::Result<Value, JsonRpcCallError>)> {
        let mut transport = self.transport.clone();
        let response = transport
            .call(RequestPacket::Single(request))
            .await
            .with_context(|| format!("failed to send JSON-RPC request for {request_context}"))?;
        let ResponsePacket::Single(response) = response else {
            bail!(
                "provider returned a batch response for single JSON-RPC request {request_context}"
            );
        };
        let response_payload =
            serde_json::to_value(&response).context("failed to encode JSON-RPC response")?;
        let result =
            match response.payload {
                ResponsePayload::Success(result) => Ok(raw_value_to_json(result.as_ref())
                    .context("failed to decode JSON-RPC result")?),
                ResponsePayload::Failure(error) => Err(JsonRpcCallError {
                    code: Some(error.code),
                    message: error.message.into_owned(),
                    data: error
                        .data
                        .as_deref()
                        .map(raw_value_to_json)
                        .transpose()
                        .context("failed to decode JSON-RPC error data")?,
                }),
            };
        Ok((response_payload, result))
    }
}

fn raw_value_to_json(value: &serde_json::value::RawValue) -> Result<Value> {
    serde_json::from_str(value.get()).context("failed to decode raw JSON value")
}
