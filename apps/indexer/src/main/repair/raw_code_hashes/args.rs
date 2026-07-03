use anyhow::{Context, Result, ensure};
use bigname_storage::parse_rfc3339_utc_timestamp;
use sqlx::types::time::OffsetDateTime;

pub(super) fn parse_timestamp_arg(value: &str, label: &str) -> Result<OffsetDateTime> {
    parse_rfc3339_utc_timestamp(value)
        .map_err(|error| anyhow::anyhow!("{error}"))
        .with_context(|| format!("failed to parse {label} {value}"))
}

pub(super) fn parse_single_chain_source(entry: &str, label: &str) -> Result<(String, String)> {
    let (chain, value) = entry
        .split_once('=')
        .with_context(|| format!("invalid {label} {entry}; expected <chain>=<value>"))?;
    let chain = chain.trim();
    let value = value.trim();
    ensure!(
        !chain.is_empty() && !value.is_empty(),
        "invalid {label} {entry}; expected non-empty <chain>=<value>"
    );
    Ok((chain.to_owned(), value.to_owned()))
}
