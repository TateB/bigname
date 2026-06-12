use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use alloy_json_rpc::{
    Id, Request as JsonRpcRequest, RequestPacket, Response, ResponsePacket, ResponsePayload,
    SerializedRequest,
};
use anyhow::{Context, Result, bail};
use bytes::Bytes;
use reqwest::Url;
use serde_json::Value;
use tracing::warn;

use super::{
    JsonRpcProvider,
    payload_cache::{JsonRpcPayloadFingerprint, JsonRpcResultPayload},
};

const MAX_JSON_RPC_ATTEMPTS: usize = 5;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct JsonRpcBatchCall {
    pub(super) method: &'static str,
    pub(super) params: Vec<Value>,
}

impl JsonRpcProvider {
    pub(super) async fn fetch_json_rpc_result(
        &self,
        method: &str,
        params: Vec<Value>,
    ) -> Result<Option<Value>> {
        self.fetch_json_rpc_result_at_endpoint(&self.endpoint, method, params)
            .await
    }

    pub(super) async fn fetch_json_rpc_result_at_endpoint(
        &self,
        endpoint: &Url,
        method: &str,
        params: Vec<Value>,
    ) -> Result<Option<Value>> {
        Ok(self
            .fetch_json_rpc_result_with_payload_at_endpoint(endpoint, method, params)
            .await?
            .result)
    }

    pub(super) async fn fetch_json_rpc_result_with_payload(
        &self,
        method: &str,
        params: Vec<Value>,
    ) -> Result<JsonRpcResultPayload> {
        self.fetch_json_rpc_result_with_payload_at_endpoint(&self.endpoint, method, params)
            .await
    }

    async fn fetch_json_rpc_result_with_payload_at_endpoint(
        &self,
        endpoint: &Url,
        method: &str,
        params: Vec<Value>,
    ) -> Result<JsonRpcResultPayload> {
        for attempt in 0..MAX_JSON_RPC_ATTEMPTS {
            match self
                .fetch_json_rpc_result_with_payload_once_at_endpoint(
                    endpoint,
                    method,
                    params.clone(),
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(error)
                    if is_retryable_provider_error(&error)
                        && attempt + 1 < MAX_JSON_RPC_ATTEMPTS =>
                {
                    warn!(
                        service = "indexer",
                        component = "provider",
                        method,
                        attempt = attempt + 1,
                        max_attempts = MAX_JSON_RPC_ATTEMPTS,
                        error = %format!("{error:#}"),
                        "retrying transient JSON-RPC provider request"
                    );
                    sleep_json_rpc_backoff(attempt).await;
                }
                Err(error) => return Err(error),
            }
        }

        bail!("JSON-RPC request retry loop exited unexpectedly")
    }

    async fn fetch_json_rpc_result_with_payload_once_at_endpoint(
        &self,
        endpoint: &Url,
        method: &str,
        params: Vec<Value>,
    ) -> Result<JsonRpcResultPayload> {
        let request = json_rpc_request(method.to_owned(), 1, params)?;
        let body = self
            .send_json_rpc_payload_to_endpoint(endpoint, method, RequestPacket::Single(request))
            .await?;

        let fingerprint = JsonRpcPayloadFingerprint::for_body(&body)?;
        let response = serde_json::from_slice::<Response>(&body)
            .context("failed to decode JSON-RPC response")?;

        Ok(JsonRpcResultPayload {
            result: json_rpc_response_result(response, method)?,
            fingerprint,
        })
    }

    pub(super) async fn fetch_json_rpc_batch_results(
        &self,
        calls: Vec<JsonRpcBatchCall>,
    ) -> Result<Vec<Option<Value>>> {
        self.fetch_json_rpc_batch_results_at_endpoint(&self.endpoint, calls)
            .await
    }

    pub(super) async fn fetch_json_rpc_batch_results_at_endpoint(
        &self,
        endpoint: &Url,
        calls: Vec<JsonRpcBatchCall>,
    ) -> Result<Vec<Option<Value>>> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }

        match self
            .try_fetch_json_rpc_batch_results_at_endpoint(endpoint, &calls)
            .await
        {
            Ok(results) => Ok(results),
            Err(batch_error) => {
                if is_retryable_provider_error(&batch_error) {
                    return Err(batch_error).context(
                        "provider JSON-RPC batch exhausted retryable attempts; refusing immediate sequential fallback",
                    );
                }

                let mut results = Vec::with_capacity(calls.len());
                for call in calls {
                    let method = call.method;
                    let result = self
                        .fetch_json_rpc_result_at_endpoint(endpoint, method, call.params)
                        .await
                        .with_context(|| {
                            format!(
                                "provider JSON-RPC batch failed ({batch_error}); individual retry for {} also failed",
                                method
                            )
                        })?;
                    results.push(result);
                }
                Ok(results)
            }
        }
    }

    async fn try_fetch_json_rpc_batch_results_at_endpoint(
        &self,
        endpoint: &Url,
        calls: &[JsonRpcBatchCall],
    ) -> Result<Vec<Option<Value>>> {
        for attempt in 0..MAX_JSON_RPC_ATTEMPTS {
            match self
                .try_fetch_json_rpc_batch_results_once_at_endpoint(endpoint, calls)
                .await
            {
                Ok(results) => return Ok(results),
                Err(error)
                    if is_retryable_provider_error(&error)
                        && attempt + 1 < MAX_JSON_RPC_ATTEMPTS =>
                {
                    warn!(
                        service = "indexer",
                        component = "provider",
                        request_context = "batch",
                        attempt = attempt + 1,
                        max_attempts = MAX_JSON_RPC_ATTEMPTS,
                        error = %format!("{error:#}"),
                        "retrying transient JSON-RPC provider batch"
                    );
                    sleep_json_rpc_backoff(attempt).await;
                }
                Err(error) => return Err(error),
            }
        }

        bail!("JSON-RPC batch retry loop exited unexpectedly")
    }

    async fn try_fetch_json_rpc_batch_results_once_at_endpoint(
        &self,
        endpoint: &Url,
        calls: &[JsonRpcBatchCall],
    ) -> Result<Vec<Option<Value>>> {
        let payload = RequestPacket::Batch(
            calls
                .iter()
                .enumerate()
                .map(|(index, call)| {
                    json_rpc_request(
                        call.method.to_owned(),
                        (index + 1) as u64,
                        call.params.clone(),
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        );
        let body = self
            .send_json_rpc_payload_to_endpoint(endpoint, "batch", payload)
            .await?;
        let response_packet = serde_json::from_slice::<ResponsePacket>(&body)
            .context("failed to decode JSON-RPC batch response")?;
        let ResponsePacket::Batch(response_values) = response_packet else {
            bail!("expected JSON-RPC batch response array");
        };
        let expected_methods = calls
            .iter()
            .enumerate()
            .map(|(index, call)| ((index + 1) as i64, call.method))
            .collect::<BTreeMap<_, _>>();
        let mut results_by_id = BTreeMap::<i64, Option<Value>>::new();

        for response in response_values {
            let id = response_id(&response.id)?;
            let method = expected_methods
                .get(&id)
                .with_context(|| format!("provider returned unexpected JSON-RPC batch id {id}"))?;
            let result = json_rpc_response_result(response, method)?;
            if results_by_id.insert(id, result).is_some() {
                bail!("provider returned duplicate JSON-RPC batch response id {id}");
            }
        }

        let mut results = Vec::with_capacity(calls.len());
        for id in 1..=calls.len() as i64 {
            results.push(
                results_by_id
                    .remove(&id)
                    .with_context(|| format!("provider omitted JSON-RPC batch response id {id}"))?,
            );
        }
        if !results_by_id.is_empty() {
            bail!("provider returned extra JSON-RPC batch responses");
        }

        Ok(results)
    }

    async fn send_json_rpc_payload_to_endpoint(
        &self,
        endpoint: &Url,
        request_context: &str,
        payload: RequestPacket,
    ) -> Result<Bytes> {
        let started = Instant::now();
        let payload = payload
            .serialize()
            .context("failed to encode JSON-RPC request payload")?;
        let response = self
            .client
            .post(endpoint.clone())
            .header("content-type", "application/json")
            .body(payload.get().to_owned())
            .send()
            .await
            .with_context(|| format!("failed to send JSON-RPC request for {request_context}"))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .context("failed to read JSON-RPC response body")?;

        if !status.is_success() {
            let response_body = String::from_utf8_lossy(&body);
            bail!(
                "provider request for {request_context} failed with HTTP {status}: {response_body}"
            );
        }
        let elapsed_ms = started.elapsed().as_millis();
        if elapsed_ms >= 10_000 {
            warn!(
                service = "indexer",
                component = "provider",
                request_context,
                status = %status,
                response_bytes = body.len(),
                elapsed_ms,
                "slow JSON-RPC provider request completed"
            );
        }

        Ok(body)
    }
}

fn json_rpc_request(method: String, id: u64, params: Vec<Value>) -> Result<SerializedRequest> {
    JsonRpcRequest::new(method, Id::Number(id), params)
        .serialize()
        .context("failed to encode JSON-RPC request")
}

fn json_rpc_response_result(response: Response, method: &str) -> Result<Option<Value>> {
    match response.payload {
        ResponsePayload::Success(result) => {
            let value = raw_value_to_json(result.as_ref())?;
            Ok((!value.is_null()).then_some(value))
        }
        ResponsePayload::Failure(error) => {
            bail!(
                "provider returned JSON-RPC error for {method}: {}: {}",
                error.code,
                error.message
            )
        }
    }
}

fn response_id(id: &Id) -> Result<i64> {
    match id {
        Id::Number(value) => i64::try_from(*value).context("JSON-RPC response id overflows i64"),
        Id::String(_) | Id::None => bail!("missing or non-integer JSON-RPC response id"),
    }
}

fn raw_value_to_json(value: &serde_json::value::RawValue) -> Result<Value> {
    serde_json::from_str(value.get()).context("failed to decode raw JSON value")
}

pub(super) fn is_retryable_provider_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("http 429")
        || (message.contains("json-rpc error") && message.contains("-32005"))
        || message.contains("http 500")
        || message.contains("http 502")
        || message.contains("http 503")
        || message.contains("http 504")
        || message.contains("too many requests")
        || message.contains("rate limit")
        || message.contains("retry later")
        || message.contains("temporarily unavailable")
        || message.contains("service unavailable")
        || message.contains("bad gateway")
        || message.contains("gateway timeout")
        || message.contains("timed out")
        || message.contains("timeout")
        || message.contains("connection reset")
        || message.contains("connection closed")
}

async fn sleep_json_rpc_backoff(attempt: usize) {
    let millis = 250_u64.saturating_mul(1_u64 << attempt.min(4));
    tokio::time::sleep(Duration::from_millis(millis)).await;
}
