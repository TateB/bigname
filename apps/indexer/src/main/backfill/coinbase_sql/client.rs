use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::{StatusCode, header};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{auth::CoinbaseSqlAuth, rate_limit::CoinbaseSqlRateLimiter, rows::CoinbaseSqlLogRow};
use crate::backfill::CoinbaseSqlBackfillConfig;

const MAX_SQL_ATTEMPTS: usize = 5;

#[derive(Clone)]
pub(super) struct CoinbaseSqlClient {
    url: String,
    auth: CoinbaseSqlAuth,
    http: reqwest::Client,
    rate_limiter: CoinbaseSqlRateLimiter,
}

impl CoinbaseSqlClient {
    pub(super) fn new(
        url: &str,
        api_key_id_env: &str,
        api_key_secret_env: &str,
        config: &CoinbaseSqlBackfillConfig,
    ) -> Result<Self> {
        let parsed_url = validate_coinbase_sql_url(url)?;
        let auth = CoinbaseSqlAuth::from_env(
            api_key_id_env,
            api_key_secret_env,
            request_host_for_url(&parsed_url)?,
            request_path_for_url(&parsed_url),
        )?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.query_timeout_secs))
            .build()
            .context("failed to build Coinbase SQL HTTP client")?;
        Ok(Self {
            url: url.to_owned(),
            auth,
            http,
            rate_limiter: CoinbaseSqlRateLimiter::new(config.rate_limit_qps),
        })
    }

    pub(super) async fn run_query(&self, sql: &str) -> Result<CoinbaseSqlQueryResponse> {
        let mut retry_count = 0usize;
        for attempt in 0..MAX_SQL_ATTEMPTS {
            self.rate_limiter.wait().await;
            let bearer_token = self.auth.bearer_token()?;
            let response = self
                .http
                .post(&self.url)
                .bearer_auth(bearer_token)
                .header(header::CONTENT_TYPE, "application/json")
                .json(&json!({ "sql": sql }))
                .send()
                .await;

            match response {
                Ok(response) if response.status().is_success() => {
                    let body = response
                        .json::<CoinbaseSqlRunResponse>()
                        .await
                        .context("failed to decode Coinbase SQL response")?;
                    let rows = body
                        .result
                        .into_iter()
                        .map(CoinbaseSqlLogRow::from_value)
                        .collect::<Result<Vec<_>>>()?;
                    return Ok(CoinbaseSqlQueryResponse { rows, retry_count });
                }
                Ok(response)
                    if should_retry_status(response.status()) && attempt + 1 < MAX_SQL_ATTEMPTS =>
                {
                    retry_count += 1;
                    sleep_before_retry(response.headers(), attempt).await;
                }
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    bail!("Coinbase SQL request failed with status {status}: {body}");
                }
                Err(error) if should_retry_error(&error) && attempt + 1 < MAX_SQL_ATTEMPTS => {
                    retry_count += 1;
                    sleep_backoff(attempt).await;
                }
                Err(error) => {
                    return Err(error).context("Coinbase SQL request failed");
                }
            }
        }

        bail!("Coinbase SQL request exhausted retry attempts")
    }
}

fn validate_coinbase_sql_url(url: &str) -> Result<reqwest::Url> {
    let parsed = reqwest::Url::parse(url)
        .with_context(|| format!("failed to parse Coinbase SQL URL {url}"))?;
    if parsed.scheme() != "https" {
        bail!("Coinbase SQL URL must use https://; refusing to send bearer token to {url}");
    }

    Ok(parsed)
}

fn request_host_for_url(url: &reqwest::Url) -> Result<String> {
    let host = url
        .host_str()
        .with_context(|| format!("Coinbase SQL URL is missing a host: {url}"))?;
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    })
}

fn request_path_for_url(url: &reqwest::Url) -> String {
    let mut path = url.path().to_owned();
    if path.is_empty() {
        path.push('/');
    }
    if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
    }
    path
}

pub(super) struct CoinbaseSqlQueryResponse {
    pub(super) rows: Vec<CoinbaseSqlLogRow>,
    pub(super) retry_count: usize,
}

#[derive(Deserialize)]
struct CoinbaseSqlRunResponse {
    #[serde(default)]
    result: Vec<Value>,
}

fn should_retry_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::INTERNAL_SERVER_ERROR
        || status == StatusCode::BAD_GATEWAY
        || status == StatusCode::SERVICE_UNAVAILABLE
        || status == StatusCode::GATEWAY_TIMEOUT
}

fn should_retry_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request()
}

async fn sleep_before_retry(headers: &header::HeaderMap, attempt: usize) {
    if let Some(delay) = retry_after_delay(headers) {
        tokio::time::sleep(delay).await;
        return;
    }
    sleep_backoff(attempt).await;
}

async fn sleep_backoff(attempt: usize) {
    let millis = 250_u64.saturating_mul(1_u64 << attempt.min(4));
    tokio::time::sleep(Duration::from_millis(millis)).await;
}

fn retry_after_delay(headers: &header::HeaderMap) -> Option<Duration> {
    headers
        .get(header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backfill::{
        CoinbaseSqlValidationMode, DEFAULT_COINBASE_SQL_INITIAL_WINDOW_BLOCKS,
        DEFAULT_COINBASE_SQL_MAX_WINDOW_BLOCKS, DEFAULT_COINBASE_SQL_PAGE_LIMIT,
        DEFAULT_COINBASE_SQL_QUERY_CHAR_LIMIT, DEFAULT_COINBASE_SQL_QUERY_TIMEOUT_SECS,
        DEFAULT_COINBASE_SQL_RATE_LIMIT_QPS,
    };

    #[test]
    fn client_rejects_non_https_url_before_reading_secret_key() {
        let error = match CoinbaseSqlClient::new(
            "http://127.0.0.1:8080/sql",
            "BIGNAME_TEST_MISSING_COINBASE_KEY_ID",
            "BIGNAME_TEST_MISSING_COINBASE_KEY_SECRET",
            &test_config(),
        ) {
            Ok(_) => panic!("Coinbase SQL client must reject non-HTTPS URLs"),
            Err(error) => error,
        };

        assert!(format!("{error:#}").contains("must use https://"));
    }

    #[tokio::test]
    #[ignore = "requires live Coinbase CDP SQL credentials and consumes two read-only SQL API queries"]
    async fn live_query_authenticates_and_decodes_one_base_event() -> Result<()> {
        let client = CoinbaseSqlClient::new(
            "https://api.cdp.coinbase.com/platform/v2/data/query/run",
            "COINBASE_CDP_SQL_API_KEY_ID",
            "COINBASE_CDP_SQL_API_KEY_SECRET",
            &test_config(),
        )?;

        let seed_response = client
            .run_query(
                r#"SELECT
  block_number,
  block_hash,
  transaction_hash,
  0 AS transaction_index,
  log_index,
  address AS emitting_address,
  topics
FROM base.events
LIMIT 1"#,
            )
            .await?;
        assert_eq!(seed_response.rows.len(), 1);
        let seed = &seed_response.rows[0];
        let planned_sql = super::super::query::build_query(
            &super::super::query::CoinbaseSqlFilterPack {
                chain: "base-mainnet".to_owned(),
                from_block: seed.block_number,
                to_block: seed.block_number,
                addresses: vec![seed.emitting_address.clone()],
                topic0s: seed.topics.first().cloned().into_iter().collect(),
                scan_all_emitters: false,
                source_families: vec!["live_smoke".to_owned()],
            },
            None,
            10,
        )?;
        let response = client.run_query(&planned_sql).await?;

        assert!(
            response
                .rows
                .iter()
                .any(|row| row.block_number == seed.block_number
                    && row.block_hash == seed.block_hash
                    && row.transaction_hash == seed.transaction_hash
                    && row.emitting_address == seed.emitting_address)
        );
        Ok(())
    }

    fn test_config() -> CoinbaseSqlBackfillConfig {
        CoinbaseSqlBackfillConfig {
            initial_window_blocks: DEFAULT_COINBASE_SQL_INITIAL_WINDOW_BLOCKS,
            max_window_blocks: DEFAULT_COINBASE_SQL_MAX_WINDOW_BLOCKS,
            page_limit: DEFAULT_COINBASE_SQL_PAGE_LIMIT,
            sql_char_limit: DEFAULT_COINBASE_SQL_QUERY_CHAR_LIMIT,
            query_timeout_secs: DEFAULT_COINBASE_SQL_QUERY_TIMEOUT_SECS,
            rate_limit_qps: DEFAULT_COINBASE_SQL_RATE_LIMIT_QPS,
            validation_mode: CoinbaseSqlValidationMode::Full,
        }
    }
}
