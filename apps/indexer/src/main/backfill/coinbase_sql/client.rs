use std::{
    fs,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{auth::CoinbaseSqlAuth, rate_limit::CoinbaseSqlRateLimiter, rows::CoinbaseSqlLogRow};
use crate::backfill::CoinbaseSqlBackfillConfig;

const MAX_SQL_ATTEMPTS: usize = 5;
const COINBASE_SQL_USER_AGENT: &str = "bigname-indexer/0.1";

#[derive(Clone)]
pub(super) struct CoinbaseSqlClient {
    url: String,
    auth: CoinbaseSqlAuth,
    rate_limiter: CoinbaseSqlRateLimiter,
    query_timeout_secs: u64,
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
        Ok(Self {
            url: url.to_owned(),
            auth,
            rate_limiter: CoinbaseSqlRateLimiter::new(config.rate_limit_qps),
            query_timeout_secs: config.query_timeout_secs,
        })
    }

    pub(super) async fn run_query(&self, sql: &str) -> Result<CoinbaseSqlQueryResponse> {
        let mut retry_count = 0usize;
        for attempt in 0..MAX_SQL_ATTEMPTS {
            self.rate_limiter.wait().await;
            let bearer_token = self.auth.bearer_token()?;
            let response = self.run_curl_query(sql, &bearer_token).await;

            match response {
                Ok(response) if response.status.is_success() => {
                    let body = serde_json::from_str::<CoinbaseSqlRunResponse>(&response.body)
                        .context("failed to decode Coinbase SQL response")?;
                    let rows = body
                        .result
                        .into_iter()
                        .map(CoinbaseSqlLogRow::from_value)
                        .collect::<Result<Vec<_>>>()?;
                    return Ok(CoinbaseSqlQueryResponse { rows, retry_count });
                }
                Ok(response)
                    if should_retry_status(response.status) && attempt + 1 < MAX_SQL_ATTEMPTS =>
                {
                    retry_count += 1;
                    sleep_backoff(attempt).await;
                }
                Ok(response) => {
                    let status = response.status;
                    let body = truncate_error_body(&response.body);
                    bail!("Coinbase SQL request failed with status {status}: {body}");
                }
                Err(error) if attempt + 1 < MAX_SQL_ATTEMPTS => {
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

    async fn run_curl_query(
        &self,
        sql: &str,
        bearer_token: &str,
    ) -> Result<CoinbaseSqlHttpResponse> {
        let url = self.url.clone();
        let sql = sql.to_owned();
        let bearer_token = bearer_token.to_owned();
        let timeout_secs = self.query_timeout_secs;
        tokio::task::spawn_blocking(move || {
            run_curl_query_blocking(&url, &bearer_token, &sql, timeout_secs)
        })
        .await
        .context("Coinbase SQL curl task failed to join")?
    }
}

fn run_curl_query_blocking(
    url: &str,
    bearer_token: &str,
    sql: &str,
    timeout_secs: u64,
) -> Result<CoinbaseSqlHttpResponse> {
    let request_id = Uuid::new_v4().simple().to_string();
    let temp_dir = std::env::temp_dir();
    let config_path = temp_dir.join(format!("bigname-coinbase-sql-{request_id}.curl"));
    let body_path = temp_dir.join(format!("bigname-coinbase-sql-{request_id}.json"));

    let result = run_curl_query_with_files(
        url,
        bearer_token,
        sql,
        timeout_secs,
        &config_path,
        &body_path,
    );
    cleanup_temp_file(&config_path);
    cleanup_temp_file(&body_path);
    result
}

fn run_curl_query_with_files(
    url: &str,
    bearer_token: &str,
    sql: &str,
    timeout_secs: u64,
    config_path: &Path,
    body_path: &Path,
) -> Result<CoinbaseSqlHttpResponse> {
    let mut config_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(config_path)
        .with_context(|| {
            format!(
                "failed to create Coinbase SQL curl config {}",
                config_path.display()
            )
        })?;
    writeln!(config_file, "silent")?;
    writeln!(config_file, "show-error")?;
    writeln!(config_file, "request = \"POST\"")?;
    writeln!(config_file, "user-agent = \"{COINBASE_SQL_USER_AGENT}\"")?;
    writeln!(config_file, "url = \"{}\"", curl_config_escape(url))?;
    writeln!(
        config_file,
        "header = \"Authorization: Bearer {}\"",
        curl_config_escape(bearer_token)
    )?;
    writeln!(config_file, "header = \"Content-Type: application/json\"")?;
    drop(config_file);

    let request_body = serde_json::to_vec(&json!({ "sql": sql }))
        .context("failed to encode Coinbase SQL request body")?;
    let mut child = Command::new("curl")
        .arg("--config")
        .arg(config_path)
        .arg("--data-binary")
        .arg("@-")
        .arg("--output")
        .arg(body_path)
        .arg("--write-out")
        .arg("%{http_code}")
        .arg("--max-time")
        .arg(timeout_secs.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn curl for Coinbase SQL request; install curl in the runtime")?;

    child
        .stdin
        .take()
        .context("failed to open curl stdin for Coinbase SQL request")?
        .write_all(&request_body)
        .context("failed to write Coinbase SQL request body to curl")?;

    let output = child
        .wait_with_output()
        .context("failed to wait for Coinbase SQL curl request")?;
    let status_text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let body = fs::read_to_string(body_path).unwrap_or_default();
    if !output.status.success() && status_text.is_empty() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "Coinbase SQL curl request failed: {}",
            truncate_error_body(&stderr)
        );
    }
    let status_code = status_text.parse::<u16>().with_context(|| {
        format!("Coinbase SQL curl returned non-numeric HTTP status {status_text}")
    })?;
    let status = StatusCode::from_u16(status_code)
        .with_context(|| format!("Coinbase SQL curl returned invalid HTTP status {status_code}"))?;
    Ok(CoinbaseSqlHttpResponse { status, body })
}

fn curl_config_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn cleanup_temp_file(path: &PathBuf) {
    let _ = fs::remove_file(path);
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

struct CoinbaseSqlHttpResponse {
    status: StatusCode,
    body: String,
}

#[derive(Deserialize)]
struct CoinbaseSqlRunResponse {
    #[serde(default, deserialize_with = "deserialize_result_rows")]
    result: Vec<Value>,
}

fn deserialize_result_rows<'de, D>(deserializer: D) -> Result<Vec<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<Vec<Value>>::deserialize(deserializer)?.unwrap_or_default())
}

fn should_retry_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::INTERNAL_SERVER_ERROR
        || status == StatusCode::BAD_GATEWAY
        || status == StatusCode::SERVICE_UNAVAILABLE
        || status == StatusCode::GATEWAY_TIMEOUT
}

async fn sleep_backoff(attempt: usize) {
    let millis = 250_u64.saturating_mul(1_u64 << attempt.min(4));
    tokio::time::sleep(Duration::from_millis(millis)).await;
}

fn truncate_error_body(body: &str) -> String {
    const MAX_ERROR_BODY_CHARS: usize = 2_000;
    let mut truncated = body.chars().take(MAX_ERROR_BODY_CHARS).collect::<String>();
    if body.chars().count() > MAX_ERROR_BODY_CHARS {
        truncated.push_str("...");
    }
    truncated
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

    #[test]
    fn run_response_treats_null_result_as_empty() -> Result<()> {
        let response = serde_json::from_str::<CoinbaseSqlRunResponse>(
            r#"{"result":null,"metadata":{"rowCount":0}}"#,
        )?;

        assert!(response.result.is_empty());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires live Coinbase CDP SQL credentials and consumes one read-only SQL API query"]
    async fn live_query_executes_one_base_block_probe() -> Result<()> {
        let client = live_coinbase_sql_client()?;

        let planned_sql = super::super::query::build_query(
            &super::super::query::CoinbaseSqlFilterPack {
                chain: "base-mainnet".to_owned(),
                from_block: 46_954_187,
                to_block: 46_954_187,
                addresses: vec!["0xb94704422c2a1e396835a571837aa5ae53285a95".to_owned()],
                topic0s: Vec::new(),
                event_signatures: vec![
                    "NewOwner(bytes32,bytes32,address)".to_owned(),
                    "NewResolver(bytes32,address)".to_owned(),
                    "NewTTL(bytes32,uint64)".to_owned(),
                    "Transfer(bytes32,address)".to_owned(),
                ],
                scan_all_emitters: false,
                source_families: vec!["basenames_base_registry".to_owned()],
            },
            None,
            1,
        )?;
        let response = client.run_query(&planned_sql).await?;
        eprintln!(
            "Coinbase SQL one-block probe returned {} row(s)",
            response.rows.len()
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires live Coinbase CDP SQL credentials and an explicit real-row assertion opt-in"]
    async fn live_query_decodes_real_cdp_parameters_row_when_enabled() -> Result<()> {
        if std::env::var("BIGNAME_INDEXER_TEST_COINBASE_SQL_ASSERT_REAL_ROW")
            .ok()
            .as_deref()
            != Some("1")
        {
            eprintln!(
                "skipping real Coinbase SQL row assertion; set BIGNAME_INDEXER_TEST_COINBASE_SQL_ASSERT_REAL_ROW=1 to enable"
            );
            return Ok(());
        }

        let block_number = std::env::var("BIGNAME_INDEXER_TEST_COINBASE_SQL_ROW_BLOCK")
            .ok()
            .map(|value| value.parse::<i64>())
            .transpose()
            .context("BIGNAME_INDEXER_TEST_COINBASE_SQL_ROW_BLOCK must be an i64")?
            .unwrap_or(46_954_187);
        let address = std::env::var("BIGNAME_INDEXER_TEST_COINBASE_SQL_ROW_ADDRESS")
            .unwrap_or_else(|_| "0xb94704422c2a1e396835a571837aa5ae53285a95".to_owned());
        let event_signatures =
            std::env::var("BIGNAME_INDEXER_TEST_COINBASE_SQL_ROW_EVENT_SIGNATURES")
                .unwrap_or_else(|_| {
                    [
                        "NewOwner(bytes32,bytes32,address)",
                        "NewResolver(bytes32,address)",
                        "NewTTL(bytes32,uint64)",
                    ]
                    .join(",")
                })
                .split(',')
                .map(str::trim)
                .filter(|signature| !signature.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();

        let client = live_coinbase_sql_client()?;
        let planned_sql = super::super::query::build_query(
            &super::super::query::CoinbaseSqlFilterPack {
                chain: "base-mainnet".to_owned(),
                from_block: block_number,
                to_block: block_number,
                addresses: vec![address],
                topic0s: Vec::new(),
                event_signatures,
                scan_all_emitters: false,
                source_families: vec!["basenames_base_registry".to_owned()],
            },
            None,
            10,
        )?;
        let response = client.run_query(&planned_sql).await?;
        let decoded = response
            .rows
            .iter()
            .find(|row| row.event_signature.is_some() && !row.requires_validation_provider_data);
        assert!(
            decoded.is_some(),
            "expected at least one real Coinbase SQL decoded event row with string parameters at block {block_number}; got {} row(s)",
            response.rows.len()
        );
        Ok(())
    }

    fn live_coinbase_sql_client() -> Result<CoinbaseSqlClient> {
        CoinbaseSqlClient::new(
            "https://api.cdp.coinbase.com/platform/v2/data/query/run",
            "COINBASE_CDP_SQL_API_KEY_ID",
            "COINBASE_CDP_SQL_API_KEY_SECRET",
            &test_config(),
        )
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
