use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

use super::*;

#[test]
fn provider_registry_parses_chain_rpc_urls() -> Result<()> {
    let registry = ProviderRegistry::from_chain_rpc_urls(&[
        "ethereum-mainnet=http://127.0.0.1:8545".to_owned(),
        "base-mainnet=http://127.0.0.1:9545".to_owned(),
    ])?;

    assert_eq!(registry.configured_chain_count(), 2);
    assert!(registry.provider_for("ethereum-mainnet").is_some());
    assert!(registry.provider_for("base-mainnet").is_some());
    assert!(registry.provider_for("optimism-mainnet").is_none());
    assert_eq!(
        registry.configured_chain_count_by_kind(ChainProviderKind::JsonRpc),
        2
    );
    assert_eq!(
        registry.configured_chain_count_by_kind(ChainProviderKind::RethDb),
        0
    );
    Ok(())
}

#[test]
fn provider_batch_item_limit_parser_defaults_and_caps_runtime_override() {
    assert_eq!(parse_provider_batch_item_limit(None), 32);
    assert_eq!(parse_provider_batch_item_limit(Some("")), 32);
    assert_eq!(parse_provider_batch_item_limit(Some("0")), 32);
    assert_eq!(parse_provider_batch_item_limit(Some("not-a-number")), 32);
    assert_eq!(parse_provider_batch_item_limit(Some("128")), 128);
    assert_eq!(parse_provider_batch_item_limit(Some("9999")), 256);

    assert_eq!(parse_provider_batch_request_concurrency(None), 1);
    assert_eq!(parse_provider_batch_request_concurrency(Some("")), 1);
    assert_eq!(parse_provider_batch_request_concurrency(Some("0")), 1);
    assert_eq!(
        parse_provider_batch_request_concurrency(Some("not-a-number")),
        1
    );
    assert_eq!(parse_provider_batch_request_concurrency(Some("4")), 4);
    assert_eq!(parse_provider_batch_request_concurrency(Some("9999")), 16);
}

#[cfg(feature = "reth-db")]
#[test]
fn provider_registry_parses_optional_reth_db_sources() -> Result<()> {
    let registry = ProviderRegistry::from_sources(
        &["ethereum-mainnet=http://127.0.0.1:8545".to_owned()],
        &["base-mainnet=/var/lib/reth/base".to_owned()],
    )?;

    assert_eq!(registry.configured_chain_count(), 2);
    assert_eq!(
        registry
            .provider_for("ethereum-mainnet")
            .expect("ethereum source must be configured")
            .kind(),
        ChainProviderKind::JsonRpc
    );
    assert_eq!(
        registry
            .provider_for("base-mainnet")
            .expect("base source must be configured")
            .kind(),
        ChainProviderKind::RethDb
    );
    assert_eq!(
        registry.configured_chain_count_by_kind(ChainProviderKind::JsonRpc),
        1
    );
    assert_eq!(
        registry.configured_chain_count_by_kind(ChainProviderKind::RethDb),
        1
    );
    Ok(())
}

#[cfg(not(feature = "reth-db"))]
#[test]
fn provider_registry_rejects_reth_db_sources_without_feature() {
    let error = match ProviderRegistry::from_sources(
        &["ethereum-mainnet=http://127.0.0.1:8545".to_owned()],
        &["base-mainnet=/var/lib/reth/base".to_owned()],
    ) {
        Ok(_) => panic!("Reth DB sources must require the reth-db feature"),
        Err(error) => error,
    };

    assert!(
        error.to_string().contains("--features reth-db"),
        "unexpected error: {error:#}"
    );
    assert!(
        error
            .to_string()
            .contains("BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn provider_registry_rejects_duplicate_chain_across_sources() {
    let error = match ProviderRegistry::from_sources(
        &["ethereum-mainnet=http://127.0.0.1:8545".to_owned()],
        &["ethereum-mainnet=/var/lib/reth/ethereum".to_owned()],
    ) {
        Ok(_) => panic!("a chain must not have two provider sources"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("duplicate provider source configuration for ethereum-mainnet"),
        "unexpected error: {error:#}"
    );
}

#[cfg(feature = "reth-db")]
#[tokio::test]
async fn reth_db_provider_source_fails_closed_for_unsupported_chain() -> Result<()> {
    let registry =
        ProviderRegistry::from_sources(&[], &["base-mainnet=/var/lib/reth/base".to_owned()])?;
    let provider = registry
        .provider_for("base-mainnet")
        .expect("Reth DB provider source must be configured");

    let error = provider
        .fetch_chain_heads()
        .await
        .expect_err("unsupported Reth DB chain must fail closed");

    assert!(
        error
            .to_string()
            .contains("Reth DB provider currently supports ethereum-mainnet only"),
        "unexpected error: {error:#}"
    );
    assert!(
        error.to_string().contains("configured chain base-mainnet"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[cfg(feature = "reth-db")]
#[tokio::test]
#[ignore = "requires BIGNAME_INDEXER_TEST_RETH_DB_DATADIR to point at a local Ethereum Mainnet Reth datadir"]
async fn reth_db_provider_reads_local_ethereum_mainnet_datadir() -> Result<()> {
    let datadir = std::env::var("BIGNAME_INDEXER_TEST_RETH_DB_DATADIR").context(
        "BIGNAME_INDEXER_TEST_RETH_DB_DATADIR must point at a local Ethereum Mainnet Reth datadir",
    )?;
    let registry = ProviderRegistry::from_sources(&[], &[format!("ethereum-mainnet={datadir}")])?;
    let provider = registry
        .provider_for("ethereum-mainnet")
        .expect("Reth DB provider source must be configured");

    let resolved = provider.fetch_block_hashes_by_numbers(&[0]).await?;
    assert_eq!(resolved.len(), 1);
    let genesis = provider
        .fetch_block_by_hash(&resolved[0].block_hash)
        .await?;
    assert_eq!(genesis.block_number, 0);

    let heads = provider.fetch_chain_heads().await?;
    assert!(heads.canonical.block_number > genesis.block_number);
    Ok(())
}

#[cfg(feature = "reth-db")]
#[tokio::test]
#[ignore = "requires BIGNAME_INDEXER_TEST_RETH_DB_DATADIR and BIGNAME_INDEXER_TEST_ETHEREUM_RPC_URL"]
async fn reth_db_provider_matches_json_rpc_for_local_blocks() -> Result<()> {
    let datadir = std::env::var("BIGNAME_INDEXER_TEST_RETH_DB_DATADIR").context(
        "BIGNAME_INDEXER_TEST_RETH_DB_DATADIR must point at a local Ethereum Mainnet Reth datadir",
    )?;
    let rpc_url = std::env::var("BIGNAME_INDEXER_TEST_ETHEREUM_RPC_URL")
        .context("BIGNAME_INDEXER_TEST_ETHEREUM_RPC_URL must point at an Ethereum Mainnet RPC")?;
    let block_numbers = std::env::var("BIGNAME_INDEXER_TEST_RETH_COMPARE_BLOCKS")
        .unwrap_or_else(|_| "0".to_owned())
        .split(',')
        .map(|value| {
            value
                .trim()
                .parse::<i64>()
                .with_context(|| format!("invalid Reth compare block number {value}"))
        })
        .collect::<Result<Vec<_>>>()?;
    assert!(
        !block_numbers.is_empty(),
        "Reth compare block list must not be empty"
    );

    let reth = RethDbProvider::new("ethereum-mainnet", &datadir)?;
    let rpc = JsonRpcProvider::new(&rpc_url)?;
    let reth_resolved = reth.fetch_block_hashes_by_numbers(&block_numbers).await?;
    let rpc_resolved = rpc.fetch_block_hashes_by_numbers(&block_numbers).await?;
    assert_eq!(reth_resolved, rpc_resolved);

    let reth_bundles = reth.fetch_block_bundles_by_hashes(&reth_resolved).await?;
    let rpc_bundles = rpc.fetch_block_bundles_by_hashes(&rpc_resolved).await?;
    assert_eq!(reth_bundles.len(), rpc_bundles.len());
    for (reth_bundle, rpc_bundle) in reth_bundles.iter().zip(&rpc_bundles) {
        assert_eq!(reth_bundle.block, rpc_bundle.block);
        assert_eq!(reth_bundle.transactions, rpc_bundle.transactions);
        assert_eq!(reth_bundle.receipts, rpc_bundle.receipts);
        assert_eq!(reth_bundle.logs, rpc_bundle.logs);
        assert!(
            reth_bundle.raw_payloads.is_empty(),
            "Reth DB bundles must not retain provider-local payload cache metadata"
        );
    }

    Ok(())
}

#[test]
fn provider_registry_accepts_ethereum_only_rpc_without_base_provider() -> Result<()> {
    let registry = ProviderRegistry::from_chain_rpc_urls(&[
        "ethereum-mainnet=http://127.0.0.1:8545".to_owned(),
    ])?;

    assert_eq!(registry.configured_chain_count(), 1);
    assert!(registry.provider_for("ethereum-mainnet").is_some());
    assert!(registry.provider_for("base-mainnet").is_none());
    registry.ensure_configured_chains_admitted(["base-mainnet", "ethereum-mainnet"].into_iter())?;
    Ok(())
}

#[test]
fn provider_registry_rejects_configured_chains_outside_admitted_set() -> Result<()> {
    let registry = ProviderRegistry::from_chain_rpc_urls(&[
        "ethereum-mainnet=http://127.0.0.1:8545".to_owned(),
        "optimism-mainnet=http://127.0.0.1:7545".to_owned(),
    ])?;

    let error = registry
        .ensure_configured_chains_admitted(["base-mainnet", "ethereum-mainnet"].into_iter())
        .expect_err("out-of-profile provider must be rejected");

    assert!(
        error.to_string().contains(
            "configured provider source chains outside selected/admitted runtime chain set: optimism-mainnet"
        ),
        "unexpected error: {error:#}"
    );
    assert!(
        error
            .to_string()
            .contains("admitted runtime chains: base-mainnet, ethereum-mainnet"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn provider_block_selection_formats_json_rpc_parameters() -> Result<()> {
    assert_eq!(
        ProviderBlockSelection::Number(42).json_rpc_parameter()?,
        json!("0x2a")
    );
    assert_eq!(
        ProviderBlockSelection::Tag(ProviderBlockTag::Safe).json_rpc_parameter()?,
        json!("safe")
    );
    assert_eq!(
        ProviderBlockSelection::Hash(
            "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned()
        )
        .json_rpc_parameter()?,
        json!({
            "blockHash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        })
    );

    let error = ProviderBlockSelection::Number(-1)
        .json_rpc_parameter()
        .expect_err("negative block selections must fail");
    assert!(
        error
            .to_string()
            .contains("provider block selection number cannot be negative: -1")
    );

    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_resolves_block_numbers_to_hashes() -> Result<()> {
    let requested_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push((method.to_owned(), params.clone()));

        let result = match method {
            "eth_getBlockByNumber" => {
                assert_eq!(params.first().and_then(Value::as_str), Some("0x2a"));
                assert_eq!(params.get(1), Some(&Value::Bool(false)));
                rpc_block_payload(requested_hash, ZERO_HASH, 42, None)
            }
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let block_hash = provider.fetch_block_hash_by_number(42).await?;
    assert_eq!(block_hash, requested_hash);

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].0, "eth_getBlockByNumber");

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_retries_transient_json_rpc_errors_before_failing_request() -> Result<()>
{
    let requested_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let attempts = Arc::new(AtomicUsize::new(0));
    let request_attempts = Arc::clone(&attempts);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let attempt = request_attempts.fetch_add(1, Ordering::Relaxed);
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert_eq!(method, "eth_getBlockByNumber");
        assert_eq!(params.first().and_then(Value::as_str), Some("0x2a"));

        if attempt == 0 {
            return json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32005,
                    "message": "too many requests; retry later"
                }
            });
        }

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": rpc_block_payload(requested_hash, ZERO_HASH, 42, None)
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let block_hash = provider.fetch_block_hash_by_number(42).await?;

    assert_eq!(block_hash, requested_hash);
    assert_eq!(attempts.load(Ordering::Relaxed), 2);

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_retries_transient_batch_errors_without_sequential_fallback() -> Result<()>
{
    let block_hash_42 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash_43 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let batch_attempts = Arc::new(AtomicUsize::new(0));
    let request_batch_sizes = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::clone(&batch_attempts);
    let batch_sizes = Arc::clone(&request_batch_sizes);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let batch = body
            .as_array()
            .expect("retryable batch errors must not fall back to single requests");
        batch_sizes
            .lock()
            .expect("request batch sizes must not be poisoned")
            .push(batch.len());
        let attempt = attempts.fetch_add(1, Ordering::Relaxed);

        if attempt == 0 {
            return Value::Array(
                batch
                    .iter()
                    .map(|request| {
                        json!({
                            "jsonrpc": "2.0",
                            "id": request.get("id").cloned().unwrap_or(Value::Null),
                            "error": {
                                "code": -32005,
                                "message": "too many requests; retry later"
                            }
                        })
                    })
                    .collect(),
            );
        }

        rpc_block_number_batch_response(&body, &[(42, block_hash_42), (43, block_hash_43)])
            .expect("second attempt must be a block-number batch")
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let resolved = provider.fetch_block_hashes_by_numbers(&[42, 43]).await?;

    assert_eq!(
        resolved
            .iter()
            .map(|block| (block.block_number, block.block_hash.as_str()))
            .collect::<Vec<_>>(),
        vec![(42, block_hash_42), (43, block_hash_43)]
    );
    assert_eq!(
        *request_batch_sizes
            .lock()
            .expect("request batch sizes must not be poisoned"),
        vec![2, 2]
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_retries_batch_error_code_32005_without_matching_text() -> Result<()> {
    let block_hash_42 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash_43 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let batch_attempts = Arc::new(AtomicUsize::new(0));
    let request_batch_sizes = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::clone(&batch_attempts);
    let batch_sizes = Arc::clone(&request_batch_sizes);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let batch = body
            .as_array()
            .expect("-32005 batch errors must not fall back to single requests");
        batch_sizes
            .lock()
            .expect("request batch sizes must not be poisoned")
            .push(batch.len());
        let attempt = attempts.fetch_add(1, Ordering::Relaxed);

        if attempt == 0 {
            return Value::Array(
                batch
                    .iter()
                    .map(|request| {
                        json!({
                            "jsonrpc": "2.0",
                            "id": request.get("id").cloned().unwrap_or(Value::Null),
                            "error": {
                                "code": -32005,
                                "message": "capacity exceeded"
                            }
                        })
                    })
                    .collect(),
            );
        }

        rpc_block_number_batch_response(&body, &[(42, block_hash_42), (43, block_hash_43)])
            .expect("second attempt must be a block-number batch")
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let resolved = provider.fetch_block_hashes_by_numbers(&[42, 43]).await?;

    assert_eq!(
        resolved
            .iter()
            .map(|block| (block.block_number, block.block_hash.as_str()))
            .collect::<Vec<_>>(),
        vec![(42, block_hash_42), (43, block_hash_43)]
    );
    assert_eq!(
        *request_batch_sizes
            .lock()
            .expect("request batch sizes must not be poisoned"),
        vec![2, 2]
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_accepts_hash_only_transactions_for_block_headers() -> Result<()> {
    let requested_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        let result = match method {
            "eth_getBlockByNumber" => {
                assert_eq!(params.first().and_then(Value::as_str), Some("0x2a"));
                assert_eq!(params.get(1), Some(&Value::Bool(false)));
                let mut block = rpc_block_payload(requested_hash, ZERO_HASH, 42, None);
                block["transactions"] =
                    json!(["0x1111111111111111111111111111111111111111111111111111111111111111"]);
                block
            }
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let block_hash = provider.fetch_block_hash_by_number(42).await?;
    assert_eq!(block_hash, requested_hash);

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_fetches_chain_heads_via_tag_hash_discovery() -> Result<()> {
    let canonical_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let canonical_parent = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let safe_hash = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    let safe_parent = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let response_for_request = |request: &Value| {
            let method = request
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let first_param = request
                .get("params")
                .and_then(Value::as_array)
                .and_then(|params| params.first())
                .and_then(Value::as_str)
                .unwrap_or_default();
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push((method.to_owned(), first_param.to_owned()));

            let result = match (method, first_param) {
                ("eth_getBlockByNumber", "latest") => json!({
                    "hash": canonical_hash.to_ascii_uppercase(),
                }),
                ("eth_getBlockByNumber", "safe") => json!({
                    "hash": safe_hash,
                }),
                ("eth_getBlockByNumber", "finalized") => json!({
                    "hash": safe_hash,
                }),
                ("eth_getBlockByHash", hash) if hash == canonical_hash => {
                    rpc_block_payload(canonical_hash, canonical_parent, 43, Some("0x0102"))
                }
                ("eth_getBlockByHash", hash) if hash == safe_hash => {
                    rpc_block_payload(safe_hash, safe_parent, 42, None)
                }
                _ => panic!("unexpected RPC request: {request}"),
            };

            json!({
                "jsonrpc": "2.0",
                "id": request.get("id").cloned().unwrap_or(Value::Null),
                "result": result
            })
        };

        if let Some(batch) = body.as_array() {
            Value::Array(batch.iter().map(response_for_request).collect())
        } else {
            response_for_request(&body)
        }
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let heads = provider.fetch_chain_heads().await?;
    assert_eq!(heads.canonical.block_number, 43);
    assert_eq!(
        heads.canonical.parent_hash,
        Some(canonical_parent.to_owned())
    );
    assert_eq!(heads.canonical.logs_bloom, Some(vec![0x01, 0x02]));
    assert_eq!(
        heads.safe.as_ref().map(|block| block.block_number),
        Some(42)
    );
    assert_eq!(
        heads
            .finalized
            .as_ref()
            .map(|block| block.block_hash.as_str()),
        Some(safe_hash)
    );

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(
        requests,
        vec![
            ("eth_getBlockByNumber".to_owned(), "latest".to_owned()),
            ("eth_getBlockByNumber".to_owned(), "safe".to_owned()),
            ("eth_getBlockByNumber".to_owned(), "finalized".to_owned()),
            ("eth_getBlockByHash".to_owned(), canonical_hash.to_owned()),
            ("eth_getBlockByHash".to_owned(), safe_hash.to_owned()),
        ]
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_degrades_safe_and_finalized_tag_errors_to_none() -> Result<()> {
    let canonical_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let canonical_parent = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let response_for_request = |request: &Value| {
            let method = request
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let first_param = request
                .get("params")
                .and_then(Value::as_array)
                .and_then(|params| params.first())
                .and_then(Value::as_str)
                .unwrap_or_default();

            match (method, first_param) {
                ("eth_getBlockByNumber", "latest") => json!({
                    "jsonrpc": "2.0",
                    "id": request.get("id").cloned().unwrap_or(Value::Null),
                    "result": {
                        "hash": canonical_hash,
                    }
                }),
                ("eth_getBlockByNumber", "safe" | "finalized") => json!({
                    "jsonrpc": "2.0",
                    "id": request.get("id").cloned().unwrap_or(Value::Null),
                    "error": {
                        "code": -32000,
                        "message": format!("unsupported block tag {first_param}")
                    }
                }),
                ("eth_getBlockByHash", hash) if hash == canonical_hash => json!({
                    "jsonrpc": "2.0",
                    "id": request.get("id").cloned().unwrap_or(Value::Null),
                    "result": rpc_block_payload(canonical_hash, canonical_parent, 43, None)
                }),
                _ => panic!("unexpected RPC request: {request}"),
            }
        };

        if let Some(batch) = body.as_array() {
            Value::Array(batch.iter().map(response_for_request).collect())
        } else {
            response_for_request(&body)
        }
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let heads = provider.fetch_chain_heads().await?;

    assert_eq!(heads.canonical.block_hash, canonical_hash);
    assert_eq!(heads.safe, None);
    assert_eq!(heads.finalized, None);

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_does_not_degrade_unrelated_checkpoint_tag_errors() -> Result<()> {
    let canonical_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let canonical_parent = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let response_for_request = |request: &Value| {
            let method = request
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let first_param = request
                .get("params")
                .and_then(Value::as_array)
                .and_then(|params| params.first())
                .and_then(Value::as_str)
                .unwrap_or_default();

            match (method, first_param) {
                ("eth_getBlockByNumber", "latest") => json!({
                    "jsonrpc": "2.0",
                    "id": request.get("id").cloned().unwrap_or(Value::Null),
                    "result": {
                        "hash": canonical_hash,
                    }
                }),
                ("eth_getBlockByNumber", "safe" | "finalized") => json!({
                    "jsonrpc": "2.0",
                    "id": request.get("id").cloned().unwrap_or(Value::Null),
                    "error": {
                        "code": -32000,
                        "message": "checkpoint database unavailable"
                    }
                }),
                ("eth_getBlockByHash", hash) if hash == canonical_hash => json!({
                    "jsonrpc": "2.0",
                    "id": request.get("id").cloned().unwrap_or(Value::Null),
                    "result": rpc_block_payload(canonical_hash, canonical_parent, 43, None)
                }),
                _ => panic!("unexpected RPC request: {request}"),
            }
        };

        if let Some(batch) = body.as_array() {
            Value::Array(batch.iter().map(response_for_request).collect())
        } else {
            response_for_request(&body)
        }
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let error = provider
        .fetch_chain_heads()
        .await
        .expect_err("unrelated checkpoint-tag errors must remain fatal");
    assert!(
        error
            .to_string()
            .contains("provider returned JSON-RPC error"),
        "unexpected error: {error:#}"
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_rejects_mismatched_hash_payloads() -> Result<()> {
    let requested_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let returned_hash = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let first_param = body
            .get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.first())
            .and_then(Value::as_str)
            .unwrap_or_default();

        let result = match (method, first_param) {
            ("eth_getBlockByHash", hash) if hash == requested_hash => {
                rpc_block_payload(returned_hash, ZERO_HASH, 43, None)
            }
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let error = provider
        .fetch_block_by_hash(&requested_hash.to_ascii_uppercase())
        .await
        .expect_err("mismatched hash payload must fail");
    assert!(
            error
                .to_string()
                .contains("provider returned block 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff for requested hash 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_fetches_code_observations_by_block_number() -> Result<()> {
    let contract_address = "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let proxy_address = "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let address = params
            .first()
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let block = params
            .get(1)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push((method.to_owned(), address.clone(), block.clone()));

        let result = match (method, address.as_str(), block.as_str()) {
            ("eth_getCode", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "0x2a") => {
                Value::String("0x6001600155".to_owned())
            }
            ("eth_getCode", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "0x2a") => {
                Value::String("0x".to_owned())
            }
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let observations = provider
        .fetch_code_observations_at_block(
            &[
                contract_address.to_owned(),
                proxy_address.to_owned(),
                contract_address.to_ascii_lowercase(),
            ],
            ProviderBlockSelection::Number(42),
        )
        .await?;

    assert_eq!(
        observations,
        vec![
            ProviderCodeObservation {
                address: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                code: vec![0x60, 0x01, 0x60, 0x01, 0x55],
            },
            ProviderCodeObservation {
                address: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                code: Vec::new(),
            },
            ProviderCodeObservation {
                address: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                code: vec![0x60, 0x01, 0x60, 0x01, 0x55],
            },
        ]
    );

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(
        requests,
        vec![
            (
                "eth_getCode".to_owned(),
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                "0x2a".to_owned(),
            ),
            (
                "eth_getCode".to_owned(),
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                "0x2a".to_owned(),
            ),
        ]
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_fetches_code_observations_by_tag() -> Result<()> {
    let contract_address = "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let address = params
            .first()
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let block = params
            .get(1)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push((method.to_owned(), address.clone(), block.clone()));

        let result = match (method, address.as_str(), block.as_str()) {
            ("eth_getCode", "0xcccccccccccccccccccccccccccccccccccccccc", "finalized") => {
                Value::String("0x600a600b".to_owned())
            }
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let observations = provider
        .fetch_code_observations_at_block(
            &[contract_address.to_owned()],
            ProviderBlockSelection::Tag(ProviderBlockTag::Finalized),
        )
        .await?;
    assert_eq!(
        observations,
        vec![ProviderCodeObservation {
            address: "0xcccccccccccccccccccccccccccccccccccccccc".to_owned(),
            code: vec![0x60, 0x0a, 0x60, 0x0b],
        }]
    );

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(
        requests,
        vec![(
            "eth_getCode".to_owned(),
            "0xcccccccccccccccccccccccccccccccccccccccc".to_owned(),
            "finalized".to_owned(),
        )]
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_retries_batched_code_items_after_item_error() -> Result<()> {
    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let address_one = "0x1111111111111111111111111111111111111111";
    let address_two = "0x2222222222222222222222222222222222222222";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        if let Some(batch) = body.as_array() {
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push(("batch".to_owned(), batch.len(), Vec::new()));
            assert_eq!(batch.len(), 2);
            return json!([
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {
                        "code": -32000,
                        "message": "temporary upstream item failure"
                    }
                },
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "result": "0xffff"
                }
            ]);
        }

        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push((method.to_owned(), 1, params.clone()));

        let result = match (method, params.first().and_then(Value::as_str)) {
            ("eth_getCode", Some(address)) if address == address_one => {
                Value::String("0x6001".to_owned())
            }
            ("eth_getCode", Some(address)) if address == address_two => {
                Value::String("0x6002".to_owned())
            }
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let observations = provider
        .fetch_code_observations_at_block_hashes(&[ProviderBlockCodeObservationRequest {
            block_hash: block_hash.to_owned(),
            addresses: vec![address_one.to_owned(), address_two.to_owned()],
        }])
        .await?;

    assert_eq!(
        observations,
        vec![ProviderBlockCodeObservations {
            block_hash: block_hash.to_owned(),
            observations: vec![
                ProviderCodeObservation {
                    address: address_one.to_owned(),
                    code: vec![0x60, 0x01],
                },
                ProviderCodeObservation {
                    address: address_two.to_owned(),
                    code: vec![0x60, 0x02],
                },
            ],
        }]
    );

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].0, "batch");
    assert_eq!(requests[0].1, 2);
    assert_eq!(requests[1].0, "eth_getCode");
    assert_eq!(
        requests[1].2.first().and_then(Value::as_str),
        Some(address_one)
    );
    assert_eq!(requests[2].0, "eth_getCode");
    assert_eq!(
        requests[2].2.first().and_then(Value::as_str),
        Some(address_two)
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_fetches_logs_by_block_range() -> Result<()> {
    let block_hash_42 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash_43 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let address_one = "0x1111111111111111111111111111111111111111";
    let address_two = "0x2222222222222222222222222222222222222222";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        if let Some(response) =
            rpc_block_number_batch_response(&body, &[(42, block_hash_42), (43, block_hash_43)])
        {
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push(body.clone());
            return response;
        }

        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(body.clone());
        assert_eq!(
            body.get("method").and_then(Value::as_str),
            Some("eth_getLogs")
        );
        let filter = body
            .get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.first())
            .and_then(Value::as_object)
            .expect("range log request must include a filter object");
        assert_eq!(
            filter.get("fromBlock").and_then(Value::as_str),
            Some("0x2a")
        );
        assert_eq!(filter.get("toBlock").and_then(Value::as_str), Some("0x2b"));
        assert_eq!(
            filter.get("address").and_then(Value::as_array),
            Some(&vec![
                Value::String(address_one.to_owned()),
                Value::String(address_two.to_owned())
            ])
        );
        assert!(!filter.contains_key("blockHash"));

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [
                rpc_log_payload(
                    "0x3333333333333333333333333333333333333333333333333333333333333333",
                    block_hash_43,
                    43,
                    1,
                    4,
                    address_two,
                    "0x4444444444444444444444444444444444444444444444444444444444444444",
                ),
                rpc_log_payload(
                    "0x5555555555555555555555555555555555555555555555555555555555555555",
                    block_hash_42,
                    42,
                    0,
                    2,
                    address_one,
                    "0x6666666666666666666666666666666666666666666666666666666666666666",
                )
            ]
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;
    let resolved_blocks = vec![
        ProviderResolvedBlock {
            block_number: 42,
            block_hash: block_hash_42.to_ascii_uppercase(),
        },
        ProviderResolvedBlock {
            block_number: 43,
            block_hash: block_hash_43.to_owned(),
        },
    ];
    let addresses = vec![address_one.to_owned(), address_two.to_ascii_uppercase()];

    let logs_by_block_number = provider
        .fetch_logs_by_block_range(&resolved_blocks, &addresses)
        .await?;

    assert_eq!(logs_by_block_number.len(), 2);
    assert_eq!(logs_by_block_number.get(&42).expect("block 42").len(), 1);
    assert_eq!(logs_by_block_number.get(&43).expect("block 43").len(), 1);
    assert_eq!(
        logs_by_block_number.get(&42).expect("block 42")[0].block_hash,
        block_hash_42
    );
    assert_eq!(
        logs_by_block_number.get(&43).expect("block 43")[0].address,
        address_two
    );
    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].get("method").and_then(Value::as_str),
        Some("eth_getLogs")
    );
    let revalidation_batch = requests[1]
        .as_array()
        .expect("post-range hash revalidation must be batched");
    assert_eq!(revalidation_batch.len(), 2);
    assert_eq!(
        revalidation_batch
            .iter()
            .map(|request| {
                request
                    .get("params")
                    .and_then(Value::as_array)
                    .and_then(|params| params.first())
                    .and_then(Value::as_str)
            })
            .collect::<Vec<_>>(),
        vec![Some("0x2a"), Some("0x2b")]
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_splits_logs_by_block_range_after_result_limit_error() -> Result<()> {
    let block_hash_42 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash_43 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let block_hash_44 = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let block_hash_45 = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let address = "0x1111111111111111111111111111111111111111";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        if let Some(response) = rpc_block_number_batch_response(
            &body,
            &[
                (42, block_hash_42),
                (43, block_hash_43),
                (44, block_hash_44),
                (45, block_hash_45),
            ],
        ) {
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push(body.clone());
            return response;
        }

        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(body.clone());
        assert_eq!(
            body.get("method").and_then(Value::as_str),
            Some("eth_getLogs")
        );
        let filter = body
            .get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.first())
            .and_then(Value::as_object)
            .expect("range log request must include a filter object");
        let from_block = filter
            .get("fromBlock")
            .and_then(Value::as_str)
            .expect("range log request must include fromBlock");
        let to_block = filter
            .get("toBlock")
            .and_then(Value::as_str)
            .expect("range log request must include toBlock");

        match (from_block, to_block) {
            ("0x2a", "0x2d") => json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32602,
                    "message": "query exceeds max results 20000, retry with the range 42-44"
                }
            }),
            ("0x2a", "0x2b") => json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [rpc_log_payload(
                    "0x3333333333333333333333333333333333333333333333333333333333333333",
                    block_hash_42,
                    42,
                    0,
                    2,
                    address,
                    "0x4444444444444444444444444444444444444444444444444444444444444444",
                )]
            }),
            ("0x2c", "0x2d") => json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [rpc_log_payload(
                    "0x5555555555555555555555555555555555555555555555555555555555555555",
                    block_hash_45,
                    45,
                    1,
                    3,
                    address,
                    "0x6666666666666666666666666666666666666666666666666666666666666666",
                )]
            }),
            _ => panic!("unexpected log range {from_block}..={to_block}"),
        }
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;
    let resolved_blocks = vec![
        ProviderResolvedBlock {
            block_number: 42,
            block_hash: block_hash_42.to_owned(),
        },
        ProviderResolvedBlock {
            block_number: 43,
            block_hash: block_hash_43.to_owned(),
        },
        ProviderResolvedBlock {
            block_number: 44,
            block_hash: block_hash_44.to_owned(),
        },
        ProviderResolvedBlock {
            block_number: 45,
            block_hash: block_hash_45.to_owned(),
        },
    ];
    let addresses = vec![address.to_owned()];

    let logs_by_block_number = provider
        .fetch_logs_by_block_range(&resolved_blocks, &addresses)
        .await?;

    assert_eq!(logs_by_block_number.get(&42).expect("block 42").len(), 1);
    assert_eq!(logs_by_block_number.get(&45).expect("block 45").len(), 1);
    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(requests.len(), 4);
    assert_eq!(
        requests
            .iter()
            .take(3)
            .map(|request| {
                let filter = request
                    .get("params")
                    .and_then(Value::as_array)
                    .and_then(|params| params.first())
                    .and_then(Value::as_object)
                    .expect("log request must include a filter object");
                (
                    filter.get("fromBlock").and_then(Value::as_str),
                    filter.get("toBlock").and_then(Value::as_str),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (Some("0x2a"), Some("0x2d")),
            (Some("0x2a"), Some("0x2b")),
            (Some("0x2c"), Some("0x2d")),
        ]
    );
    assert!(
        requests[3].as_array().is_some_and(|batch| batch.len() == 4),
        "successful split log lookup must still revalidate all block hashes"
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_splits_logs_after_alternate_result_limit_error() -> Result<()> {
    let block_hash_42 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash_43 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let address = "0x1111111111111111111111111111111111111111";

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        if let Some(response) =
            rpc_block_number_batch_response(&body, &[(42, block_hash_42), (43, block_hash_43)])
        {
            return response;
        }

        assert_eq!(
            body.get("method").and_then(Value::as_str),
            Some("eth_getLogs")
        );
        let filter = body
            .get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.first())
            .and_then(Value::as_object)
            .expect("range log request must include a filter object");
        let from_block = filter
            .get("fromBlock")
            .and_then(Value::as_str)
            .expect("range log request must include fromBlock");
        let to_block = filter
            .get("toBlock")
            .and_then(Value::as_str)
            .expect("range log request must include toBlock");

        match (from_block, to_block) {
            ("0x2a", "0x2b") => json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32602,
                    "message": "Log response size exceeded; use a smaller block range"
                }
            }),
            ("0x2a", "0x2a") => json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [rpc_log_payload(
                    "0x3333333333333333333333333333333333333333333333333333333333333333",
                    block_hash_42,
                    42,
                    0,
                    2,
                    address,
                    "0x4444444444444444444444444444444444444444444444444444444444444444",
                )]
            }),
            ("0x2b", "0x2b") => json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": []
            }),
            _ => panic!("unexpected log range {from_block}..={to_block}"),
        }
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;
    let resolved_blocks = vec![
        ProviderResolvedBlock {
            block_number: 42,
            block_hash: block_hash_42.to_owned(),
        },
        ProviderResolvedBlock {
            block_number: 43,
            block_hash: block_hash_43.to_owned(),
        },
    ];
    let addresses = vec![address.to_owned()];

    let logs_by_block_number = provider
        .fetch_logs_by_block_range(&resolved_blocks, &addresses)
        .await?;

    assert_eq!(logs_by_block_number.get(&42).expect("block 42").len(), 1);
    assert_eq!(logs_by_block_number.get(&43).expect("block 43").len(), 0);

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_rejects_empty_range_when_post_range_block_hash_drifts() -> Result<()> {
    let block_hash_42 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash_43 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let drifted_hash = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let address = "0x1111111111111111111111111111111111111111";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(body.clone());

        if let Some(response) =
            rpc_block_number_batch_response(&body, &[(42, drifted_hash), (43, block_hash_43)])
        {
            return response;
        }

        assert_eq!(
            body.get("method").and_then(Value::as_str),
            Some("eth_getLogs")
        );
        let filter = body
            .get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.first())
            .and_then(Value::as_object)
            .expect("range log request must include a filter object");
        assert_eq!(
            filter.get("fromBlock").and_then(Value::as_str),
            Some("0x2a")
        );
        assert_eq!(filter.get("toBlock").and_then(Value::as_str), Some("0x2b"));
        assert!(!filter.contains_key("blockHash"));

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": []
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;
    let resolved_blocks = vec![
        ProviderResolvedBlock {
            block_number: 42,
            block_hash: block_hash_42.to_owned(),
        },
        ProviderResolvedBlock {
            block_number: 43,
            block_hash: block_hash_43.to_owned(),
        },
    ];
    let addresses = vec![address.to_owned()];

    let error = provider
        .fetch_logs_by_block_range(&resolved_blocks, &addresses)
        .await
        .expect_err("empty drifted range must fail post-range block hash validation");

    assert!(
            error.to_string().contains(
                "provider block hash changed after range log lookup for block number 42: expected 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, got 0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            ),
            "unexpected error: {error:#}"
        );
    assert_eq!(
        requests
            .lock()
            .expect("request log must not be poisoned")
            .len(),
        2
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_rejects_mismatched_range_log_block_hash() -> Result<()> {
    let block_hash_42 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash_43 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let wrong_hash = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let address = "0x1111111111111111111111111111111111111111";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(body.clone());
        assert_eq!(
            body.get("method").and_then(Value::as_str),
            Some("eth_getLogs")
        );
        let filter = body
            .get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.first())
            .and_then(Value::as_object)
            .expect("range log request must include a filter object");
        assert_eq!(
            filter.get("fromBlock").and_then(Value::as_str),
            Some("0x2a")
        );
        assert_eq!(filter.get("toBlock").and_then(Value::as_str), Some("0x2b"));
        assert!(!filter.contains_key("blockHash"));

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [rpc_log_payload(
                "0x3333333333333333333333333333333333333333333333333333333333333333",
                wrong_hash,
                43,
                0,
                0,
                address,
                "0x4444444444444444444444444444444444444444444444444444444444444444",
            )]
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;
    let resolved_blocks = vec![
        ProviderResolvedBlock {
            block_number: 42,
            block_hash: block_hash_42.to_owned(),
        },
        ProviderResolvedBlock {
            block_number: 43,
            block_hash: block_hash_43.to_owned(),
        },
    ];
    let addresses = vec![address.to_owned()];

    let error = provider
        .fetch_logs_by_block_range(&resolved_blocks, &addresses)
        .await
        .expect_err("mismatched range log block hash must fail");

    assert!(
            error.to_string().contains(
                "provider returned log 0 for block 0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb with mismatched block hash 0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            ),
            "unexpected error: {error:#}"
        );
    assert_eq!(
        requests
            .lock()
            .expect("request log must not be poisoned")
            .len(),
        1
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_rejects_range_logs_for_unrequested_block_numbers() -> Result<()> {
    let block_hash_42 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash_43 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let block_hash_44 = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let address = "0x1111111111111111111111111111111111111111";

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        assert_eq!(
            body.get("method").and_then(Value::as_str),
            Some("eth_getLogs")
        );

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [rpc_log_payload(
                "0x3333333333333333333333333333333333333333333333333333333333333333",
                block_hash_44,
                44,
                0,
                0,
                address,
                "0x4444444444444444444444444444444444444444444444444444444444444444",
            )]
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;
    let resolved_blocks = vec![
        ProviderResolvedBlock {
            block_number: 42,
            block_hash: block_hash_42.to_owned(),
        },
        ProviderResolvedBlock {
            block_number: 43,
            block_hash: block_hash_43.to_owned(),
        },
    ];
    let addresses = vec![address.to_owned()];

    let error = provider
        .fetch_logs_by_block_range(&resolved_blocks, &addresses)
        .await
        .expect_err("range logs from unrequested blocks must fail");

    assert!(
        error
            .to_string()
            .contains("provider returned log 0 for unrequested block number 44"),
        "unexpected error: {error:#}"
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_rejects_invalid_log_range_requests() -> Result<()> {
    let provider = JsonRpcProvider::new("http://127.0.0.1:1")?;
    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let addresses = vec!["0x1111111111111111111111111111111111111111".to_owned()];

    let error = provider
        .fetch_logs_by_block_range(
            &[
                ProviderResolvedBlock {
                    block_number: 42,
                    block_hash: block_hash.to_owned(),
                },
                ProviderResolvedBlock {
                    block_number: 42,
                    block_hash: block_hash.to_owned(),
                },
            ],
            &addresses,
        )
        .await
        .expect_err("duplicate range block numbers must fail");
    assert!(
        error
            .to_string()
            .contains("provider log range requested duplicate block number 42"),
        "unexpected error: {error:#}"
    );

    let error = provider
        .fetch_logs_by_block_range(
            &[
                ProviderResolvedBlock {
                    block_number: 42,
                    block_hash: block_hash.to_owned(),
                },
                ProviderResolvedBlock {
                    block_number: 44,
                    block_hash: block_hash.to_owned(),
                },
            ],
            &addresses,
        )
        .await
        .expect_err("non-contiguous range block numbers must fail");
    assert!(
        error
            .to_string()
            .contains("provider log range requested non-contiguous block numbers"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_rejects_mismatched_batched_exact_log_block_hash() -> Result<()> {
    let block_hash_one = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash_two = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let wrong_hash = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let batch = body.as_array().expect("logs must be requested as a batch");
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(batch.clone());
        assert_eq!(batch.len(), 2);
        for (request, (expected_hash, expected_address)) in batch.iter().zip([
            (block_hash_one, "0x1111111111111111111111111111111111111111"),
            (block_hash_two, "0x3333333333333333333333333333333333333333"),
        ]) {
            assert_eq!(
                request.get("method").and_then(Value::as_str),
                Some("eth_getLogs")
            );
            let filter = request
                .get("params")
                .and_then(Value::as_array)
                .and_then(|params| params.first())
                .and_then(Value::as_object)
                .expect("batched log request must include a filter object");
            assert_eq!(
                filter.get("blockHash").and_then(Value::as_str),
                Some(expected_hash)
            );
            assert_eq!(
                filter.get("address").and_then(Value::as_array),
                Some(&vec![Value::String(expected_address.to_owned())])
            );
            assert!(!filter.contains_key("fromBlock"));
            assert!(!filter.contains_key("toBlock"));
        }

        json!([
            {
                "jsonrpc": "2.0",
                "id": 1,
                "result": [rpc_log_payload(
                    "0x1111111111111111111111111111111111111111111111111111111111111111",
                    block_hash_one,
                    42,
                    0,
                    0,
                    "0x1111111111111111111111111111111111111111",
                    "0x2222222222222222222222222222222222222222222222222222222222222222",
                )]
            },
            {
                "jsonrpc": "2.0",
                "id": 2,
                "result": [rpc_log_payload(
                    "0x3333333333333333333333333333333333333333333333333333333333333333",
                    wrong_hash,
                    43,
                    0,
                    0,
                    "0x3333333333333333333333333333333333333333",
                    "0x4444444444444444444444444444444444444444444444444444444444444444",
                )]
            }
        ])
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let error = provider
        .fetch_logs_by_block_hashes(&[
            ProviderBlockLogRequest {
                block_number: 42,
                block_hash: block_hash_one.to_owned(),
                addresses: vec!["0x1111111111111111111111111111111111111111".to_owned()],
            },
            ProviderBlockLogRequest {
                block_number: 43,
                block_hash: block_hash_two.to_owned(),
                addresses: vec!["0x3333333333333333333333333333333333333333".to_owned()],
            },
        ])
        .await
        .expect_err("mismatched batched log block hash must fail");
    assert!(
            error.to_string().contains(
                "provider returned log 0 for block 0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb with mismatched block hash 0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            ),
            "unexpected error: {error:#}"
        );
    assert_eq!(
        requests
            .lock()
            .expect("request log must not be poisoned")
            .len(),
        1
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_rejects_invalid_code_payloads() -> Result<()> {
    let contract_address = "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        let result = match method {
            "eth_getCode"
                if params.first().and_then(Value::as_str)
                    == Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") =>
            {
                Value::String("0x123".to_owned())
            }
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let error = provider
        .fetch_code_observations_at_block(
            &[contract_address.to_owned()],
            ProviderBlockSelection::Tag(ProviderBlockTag::Latest),
        )
        .await
        .expect_err("invalid code payload must fail");
    assert!(
        error
            .to_string()
            .contains("invalid hex byte string with odd length")
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_batches_block_bundles_without_logs() -> Result<()> {
    let block_hash_one = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash_two = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let tx_hash_one = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let tx_hash_two = "0x2222222222222222222222222222222222222222222222222222222222222222";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(body.clone());

        let batch = body.as_array().expect("request must be batched");
        let method = batch
            .first()
            .and_then(|request| request.get("method"))
            .and_then(Value::as_str)
            .expect("batch request must include a method");

        Value::Array(
            batch
                .iter()
                .map(|request| {
                    let params = request
                        .get("params")
                        .and_then(Value::as_array)
                        .expect("batch request must include params");
                    let block_hash = params
                        .first()
                        .and_then(Value::as_str)
                        .expect("batch request must include block hash");
                    let id = request.get("id").cloned().unwrap_or(Value::Null);
                    let result = match method {
                        "eth_getBlockByHash" => {
                            assert_eq!(params.get(1), Some(&Value::Bool(true)));
                            match block_hash {
                                hash if hash == block_hash_one => rpc_exact_block_payload(
                                    block_hash_one,
                                    ZERO_HASH,
                                    42,
                                    None,
                                    vec![rpc_transaction_payload(
                                        tx_hash_one,
                                        block_hash_one,
                                        42,
                                        0,
                                        "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                        Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                                    )],
                                ),
                                hash if hash == block_hash_two => rpc_exact_block_payload(
                                    block_hash_two,
                                    block_hash_one,
                                    43,
                                    None,
                                    vec![rpc_transaction_payload(
                                        tx_hash_two,
                                        block_hash_two,
                                        43,
                                        0,
                                        "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
                                        None,
                                    )],
                                ),
                                _ => panic!("unexpected block hash {block_hash}"),
                            }
                        }
                        "eth_getBlockReceipts" => match block_hash {
                            hash if hash == block_hash_one => {
                                Value::Array(vec![rpc_receipt_payload(
                                    tx_hash_one,
                                    block_hash_one,
                                    42,
                                    0,
                                    None,
                                )])
                            }
                            hash if hash == block_hash_two => {
                                Value::Array(vec![rpc_receipt_payload(
                                    tx_hash_two,
                                    block_hash_two,
                                    43,
                                    0,
                                    None,
                                )])
                            }
                            _ => panic!("unexpected receipt block hash {block_hash}"),
                        },
                        _ => panic!("unexpected batch method {method}"),
                    };

                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": result,
                    })
                })
                .collect(),
        )
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let bundles = provider
        .fetch_block_bundles_without_logs_by_hashes(&[
            ProviderResolvedBlock {
                block_number: 42,
                block_hash: block_hash_one.to_owned(),
            },
            ProviderResolvedBlock {
                block_number: 43,
                block_hash: block_hash_two.to_owned(),
            },
        ])
        .await?;

    assert_eq!(bundles.len(), 2);
    assert_eq!(bundles[0].block.block_hash, block_hash_one);
    assert_eq!(bundles[0].transactions[0].transaction_hash, tx_hash_one);
    assert_eq!(bundles[0].receipts[0].transaction_hash, tx_hash_one);
    assert!(bundles[0].logs.is_empty());
    assert!(bundles[0].raw_payloads.is_empty());
    assert_eq!(bundles[1].block.block_hash, block_hash_two);
    assert_eq!(bundles[1].receipts[0].transaction_hash, tx_hash_two);

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests
            .iter()
            .map(|request| {
                request
                    .as_array()
                    .and_then(|batch| batch.first())
                    .and_then(|request| request.get("method"))
                    .and_then(Value::as_str)
            })
            .collect::<Vec<_>>(),
        vec![Some("eth_getBlockByHash"), Some("eth_getBlockReceipts")]
    );
    assert!(
        requests
            .iter()
            .all(|request| { request.as_array().is_some_and(|batch| batch.len() == 2) })
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_batches_selected_transaction_receipt_pairs() -> Result<()> {
    let block_hash_one = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_hash_two = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let tx_hash_one = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let tx_hash_two = "0x2222222222222222222222222222222222222222222222222222222222222222";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(body.clone());

        let batch = body.as_array().expect("request must be batched");
        Value::Array(
            batch
                .iter()
                .map(|request| {
                    let method = request
                        .get("method")
                        .and_then(Value::as_str)
                        .expect("batch request must include a method");
                    let params = request
                        .get("params")
                        .and_then(Value::as_array)
                        .expect("batch request must include params");
                    let transaction_hash = params
                        .first()
                        .and_then(Value::as_str)
                        .expect("batch request must include transaction hash");
                    let result = match (method, transaction_hash) {
                        ("eth_getTransactionByHash", hash) if hash == tx_hash_one => {
                            rpc_transaction_payload(
                                tx_hash_one,
                                block_hash_one,
                                42,
                                0,
                                "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                            )
                        }
                        ("eth_getTransactionReceipt", hash) if hash == tx_hash_one => {
                            rpc_receipt_payload(tx_hash_one, block_hash_one, 42, 0, None)
                        }
                        ("eth_getTransactionByHash", hash) if hash == tx_hash_two => {
                            rpc_transaction_payload(
                                tx_hash_two,
                                block_hash_two,
                                43,
                                1,
                                "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
                                None,
                            )
                        }
                        ("eth_getTransactionReceipt", hash) if hash == tx_hash_two => {
                            rpc_receipt_payload(tx_hash_two, block_hash_two, 43, 1, None)
                        }
                        _ => panic!("unexpected selected transaction request: {request}"),
                    };

                    json!({
                        "jsonrpc": "2.0",
                        "id": request.get("id").cloned().unwrap_or(Value::Null),
                        "result": result,
                    })
                })
                .collect(),
        )
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let bundles = provider
        .fetch_transaction_receipt_pairs_by_hashes(&[
            ProviderTransactionReceiptRequest {
                transaction_hash: tx_hash_one.to_owned(),
                block_hash: block_hash_one.to_owned(),
                block_number: 42,
                transaction_index: 0,
            },
            ProviderTransactionReceiptRequest {
                transaction_hash: tx_hash_two.to_owned(),
                block_hash: block_hash_two.to_owned(),
                block_number: 43,
                transaction_index: 1,
            },
        ])
        .await?;

    assert_eq!(bundles.len(), 2);
    assert_eq!(bundles[0].transaction.transaction_hash, tx_hash_one);
    assert_eq!(bundles[0].receipt.transaction_hash, tx_hash_one);
    assert_eq!(bundles[1].transaction.transaction_hash, tx_hash_two);
    assert_eq!(bundles[1].receipt.transaction_index, 1);

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(requests.len(), 1);
    let batch = requests[0]
        .as_array()
        .expect("selected transaction requests must be batched");
    assert_eq!(batch.len(), 4);
    assert_eq!(
        batch
            .iter()
            .map(|request| request.get("method").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec![
            Some("eth_getTransactionByHash"),
            Some("eth_getTransactionReceipt"),
            Some("eth_getTransactionByHash"),
            Some("eth_getTransactionReceipt"),
        ]
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_falls_back_to_block_receipts_for_null_selected_receipt() -> Result<()> {
    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let parent_hash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let tx_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let unrelated_tx_hash = "0x2222222222222222222222222222222222222222222222222222222222222222";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let batch = body.as_array().expect("request must be batched");
        let methods = batch
            .iter()
            .map(|request| {
                request
                    .get("method")
                    .and_then(Value::as_str)
                    .expect("batch request must include a method")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(methods);

        Value::Array(
            batch
                .iter()
                .map(|request| {
                    let method = request
                        .get("method")
                        .and_then(Value::as_str)
                        .expect("batch request must include a method");
                    let params = request
                        .get("params")
                        .and_then(Value::as_array)
                        .expect("batch request must include params");
                    let result = match method {
                        "eth_getTransactionByHash" => {
                            assert_eq!(params.first().and_then(Value::as_str), Some(tx_hash));
                            rpc_transaction_payload(
                                tx_hash,
                                block_hash,
                                42,
                                7,
                                "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                            )
                        }
                        "eth_getTransactionReceipt" => {
                            assert_eq!(params.first().and_then(Value::as_str), Some(tx_hash));
                            Value::Null
                        }
                        "eth_getBlockByHash" => {
                            assert_eq!(params.first().and_then(Value::as_str), Some(block_hash));
                            assert_eq!(params.get(1), Some(&Value::Bool(true)));
                            rpc_exact_block_payload(
                                block_hash,
                                parent_hash,
                                42,
                                Some("0x0102"),
                                vec![
                                    rpc_transaction_payload(
                                        tx_hash,
                                        block_hash,
                                        42,
                                        7,
                                        "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                        Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                                    ),
                                    rpc_transaction_payload(
                                        unrelated_tx_hash,
                                        block_hash,
                                        42,
                                        8,
                                        "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
                                        None,
                                    ),
                                ],
                            )
                        }
                        "eth_getBlockReceipts" => {
                            assert_eq!(params.first().and_then(Value::as_str), Some(block_hash));
                            Value::Array(vec![rpc_receipt_payload(
                                tx_hash, block_hash, 42, 7, None,
                            )])
                        }
                        _ => panic!("unexpected selected transaction fallback request: {request}"),
                    };

                    json!({
                        "jsonrpc": "2.0",
                        "id": request.get("id").cloned().unwrap_or(Value::Null),
                        "result": result,
                    })
                })
                .collect(),
        )
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let bundles = provider
        .fetch_transaction_receipt_pairs_by_hashes(&[ProviderTransactionReceiptRequest {
            transaction_hash: tx_hash.to_owned(),
            block_hash: block_hash.to_owned(),
            block_number: 42,
            transaction_index: 7,
        }])
        .await?;

    assert_eq!(bundles.len(), 1);
    assert_eq!(bundles[0].transaction.transaction_hash, tx_hash);
    assert_eq!(bundles[0].receipt.transaction_hash, tx_hash);
    assert_eq!(bundles[0].receipt.transaction_index, 7);

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(
        requests,
        vec![
            vec![
                "eth_getTransactionByHash".to_owned(),
                "eth_getTransactionReceipt".to_owned(),
            ],
            vec!["eth_getBlockByHash".to_owned()],
            vec!["eth_getBlockReceipts".to_owned()],
        ]
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_retries_direct_receipt_when_block_fallback_omits_selected_receipt()
-> Result<()> {
    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let parent_hash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let tx_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let batch = body.as_array().expect("request must be batched");
        let methods = batch
            .iter()
            .map(|request| {
                request
                    .get("method")
                    .and_then(Value::as_str)
                    .expect("batch request must include a method")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        let request_index = {
            let mut requests = request_log
                .lock()
                .expect("request log must not be poisoned");
            let request_index = requests.len();
            requests.push(methods.clone());
            request_index
        };

        Value::Array(
            batch
                .iter()
                .map(|request| {
                    let method = request
                        .get("method")
                        .and_then(Value::as_str)
                        .expect("batch request must include a method");
                    let result = match (request_index, method) {
                        (0, "eth_getTransactionByHash") => rpc_transaction_payload(
                            tx_hash,
                            block_hash,
                            42,
                            7,
                            "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                        ),
                        (0, "eth_getTransactionReceipt") => Value::Null,
                        (1, "eth_getBlockByHash") => rpc_exact_block_payload(
                            block_hash,
                            parent_hash,
                            42,
                            Some("0x0102"),
                            vec![rpc_transaction_payload(
                                tx_hash,
                                block_hash,
                                42,
                                7,
                                "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                            )],
                        ),
                        (2, "eth_getBlockReceipts") => Value::Array(Vec::new()),
                        (_, "eth_getTransactionByHash") => rpc_transaction_payload(
                            tx_hash,
                            block_hash,
                            42,
                            7,
                            "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                        ),
                        (_, "eth_getTransactionReceipt") => {
                            rpc_receipt_payload(tx_hash, block_hash, 42, 7, None)
                        }
                        _ => panic!("unexpected selected transaction retry request: {request}"),
                    };

                    json!({
                        "jsonrpc": "2.0",
                        "id": request.get("id").cloned().unwrap_or(Value::Null),
                        "result": result,
                    })
                })
                .collect(),
        )
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let bundles = provider
        .fetch_transaction_receipt_pairs_by_hashes(&[ProviderTransactionReceiptRequest {
            transaction_hash: tx_hash.to_owned(),
            block_hash: block_hash.to_owned(),
            block_number: 42,
            transaction_index: 7,
        }])
        .await?;

    assert_eq!(bundles.len(), 1);
    assert_eq!(bundles[0].transaction.transaction_hash, tx_hash);
    assert_eq!(bundles[0].receipt.transaction_hash, tx_hash);
    assert_eq!(bundles[0].receipt.transaction_index, 7);

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(
        requests,
        vec![
            vec![
                "eth_getTransactionByHash".to_owned(),
                "eth_getTransactionReceipt".to_owned(),
            ],
            vec!["eth_getBlockByHash".to_owned()],
            vec!["eth_getBlockReceipts".to_owned()],
            vec![
                "eth_getTransactionByHash".to_owned(),
                "eth_getTransactionReceipt".to_owned(),
            ],
        ]
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_uses_receipt_fallback_endpoint_after_primary_omits_selected_receipt()
-> Result<()> {
    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let parent_hash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let tx_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let primary_requests = Arc::new(Mutex::new(Vec::new()));
    let primary_request_log = Arc::clone(&primary_requests);

    let (primary_url, primary_server) = spawn_json_rpc_server(Arc::new(move |body| {
        let batch = body.as_array().expect("request must be batched");
        let methods = batch
            .iter()
            .map(|request| {
                request
                    .get("method")
                    .and_then(Value::as_str)
                    .expect("batch request must include a method")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        let request_index = {
            let mut requests = primary_request_log
                .lock()
                .expect("request log must not be poisoned");
            let request_index = requests.len();
            requests.push(methods);
            request_index
        };

        Value::Array(
            batch
                .iter()
                .map(|request| {
                    let method = request
                        .get("method")
                        .and_then(Value::as_str)
                        .expect("batch request must include a method");
                    let result = match (request_index, method) {
                        (0, "eth_getTransactionByHash") => rpc_transaction_payload(
                            tx_hash,
                            block_hash,
                            42,
                            7,
                            "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                        ),
                        (0, "eth_getTransactionReceipt") => Value::Null,
                        (1, "eth_getBlockByHash") => rpc_exact_block_payload(
                            block_hash,
                            parent_hash,
                            42,
                            Some("0x0102"),
                            vec![rpc_transaction_payload(
                                tx_hash,
                                block_hash,
                                42,
                                7,
                                "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                            )],
                        ),
                        (2, "eth_getBlockReceipts") => Value::Array(Vec::new()),
                        (_, "eth_getTransactionByHash") => rpc_transaction_payload(
                            tx_hash,
                            block_hash,
                            42,
                            7,
                            "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                        ),
                        (_, "eth_getTransactionReceipt") => Value::Null,
                        _ => panic!("unexpected selected transaction fallback request: {request}"),
                    };

                    json!({
                        "jsonrpc": "2.0",
                        "id": request.get("id").cloned().unwrap_or(Value::Null),
                        "result": result,
                    })
                })
                .collect(),
        )
    }))
    .await?;
    let fallback_requests = Arc::new(Mutex::new(Vec::new()));
    let fallback_request_log = Arc::clone(&fallback_requests);
    let (fallback_url, fallback_server) = spawn_json_rpc_server(Arc::new(move |body| {
        let batch = body.as_array().expect("request must be batched");
        fallback_request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(
                batch
                    .iter()
                    .map(|request| {
                        request
                            .get("method")
                            .and_then(Value::as_str)
                            .expect("batch request must include a method")
                            .to_owned()
                    })
                    .collect::<Vec<_>>(),
            );

        Value::Array(
            batch
                .iter()
                .map(|request| {
                    let method = request
                        .get("method")
                        .and_then(Value::as_str)
                        .expect("batch request must include a method");
                    let result = match method {
                        "eth_getTransactionByHash" => rpc_transaction_payload(
                            tx_hash,
                            block_hash,
                            42,
                            7,
                            "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                        ),
                        "eth_getTransactionReceipt" => {
                            rpc_receipt_payload(tx_hash, block_hash, 42, 7, None)
                        }
                        _ => panic!("unexpected fallback provider request: {request}"),
                    };

                    json!({
                        "jsonrpc": "2.0",
                        "id": request.get("id").cloned().unwrap_or(Value::Null),
                        "result": result,
                    })
                })
                .collect(),
        )
    }))
    .await?;
    let provider = JsonRpcProvider::new_with_receipt_fallback(
        &primary_url,
        Some(reqwest::Url::parse(&fallback_url)?),
    )?;

    let bundles = provider
        .fetch_transaction_receipt_pairs_by_hashes(&[ProviderTransactionReceiptRequest {
            transaction_hash: tx_hash.to_owned(),
            block_hash: block_hash.to_owned(),
            block_number: 42,
            transaction_index: 7,
        }])
        .await?;

    assert_eq!(bundles.len(), 1);
    assert_eq!(bundles[0].transaction.transaction_hash, tx_hash);
    assert_eq!(bundles[0].receipt.transaction_hash, tx_hash);
    assert_eq!(bundles[0].receipt.transaction_index, 7);

    let fallback_requests = fallback_requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(
        fallback_requests,
        vec![vec![
            "eth_getTransactionByHash".to_owned(),
            "eth_getTransactionReceipt".to_owned(),
        ]]
    );

    primary_server.abort();
    fallback_server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_uses_receipt_fallback_endpoint_when_block_receipts_error() -> Result<()>
{
    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let parent_hash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let tx_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let primary_requests = Arc::new(Mutex::new(Vec::new()));
    let primary_request_log = Arc::clone(&primary_requests);

    let (primary_url, primary_server) = spawn_json_rpc_server(Arc::new(move |body| {
        let is_batch = body.is_array();
        let requests = body
            .as_array()
            .cloned()
            .unwrap_or_else(|| vec![body.clone()]);
        let methods = requests
            .iter()
            .map(|request| {
                request
                    .get("method")
                    .and_then(Value::as_str)
                    .expect("request must include a method")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        primary_request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(methods);

        let responses = requests
            .iter()
            .map(|request| {
                let method = request
                    .get("method")
                    .and_then(Value::as_str)
                    .expect("request must include a method");
                let id = request.get("id").cloned().unwrap_or(Value::Null);
                if method == "eth_getBlockReceipts" {
                    return json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {
                            "code": -32601,
                            "message": "Method not found",
                        },
                    });
                }

                let result = match method {
                    "eth_getTransactionByHash" => rpc_transaction_payload(
                        tx_hash,
                        block_hash,
                        42,
                        7,
                        "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                        Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                    ),
                    "eth_getTransactionReceipt" => Value::Null,
                    "eth_getBlockByHash" => {
                        let params = request
                            .get("params")
                            .and_then(Value::as_array)
                            .expect("block request must include params");
                        assert_eq!(params.first().and_then(Value::as_str), Some(block_hash));
                        assert_eq!(params.get(1), Some(&Value::Bool(true)));
                        rpc_exact_block_payload(
                            block_hash,
                            parent_hash,
                            42,
                            Some("0x0102"),
                            vec![rpc_transaction_payload(
                                tx_hash,
                                block_hash,
                                42,
                                7,
                                "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                            )],
                        )
                    }
                    _ => panic!("unexpected selected transaction fallback request: {request}"),
                };

                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": result,
                })
            })
            .collect::<Vec<_>>();
        if is_batch {
            Value::Array(responses)
        } else {
            responses
                .into_iter()
                .next()
                .expect("single response is present")
        }
    }))
    .await?;
    let fallback_requests = Arc::new(Mutex::new(Vec::new()));
    let fallback_request_log = Arc::clone(&fallback_requests);
    let (fallback_url, fallback_server) = spawn_json_rpc_server(Arc::new(move |body| {
        let batch = body.as_array().expect("request must be batched");
        fallback_request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(
                batch
                    .iter()
                    .map(|request| {
                        request
                            .get("method")
                            .and_then(Value::as_str)
                            .expect("batch request must include a method")
                            .to_owned()
                    })
                    .collect::<Vec<_>>(),
            );

        Value::Array(
            batch
                .iter()
                .map(|request| {
                    let method = request
                        .get("method")
                        .and_then(Value::as_str)
                        .expect("batch request must include a method");
                    let result = match method {
                        "eth_getTransactionByHash" => rpc_transaction_payload(
                            tx_hash,
                            block_hash,
                            42,
                            7,
                            "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                        ),
                        "eth_getTransactionReceipt" => {
                            rpc_receipt_payload(tx_hash, block_hash, 42, 7, None)
                        }
                        _ => panic!("unexpected fallback provider request: {request}"),
                    };

                    json!({
                        "jsonrpc": "2.0",
                        "id": request.get("id").cloned().unwrap_or(Value::Null),
                        "result": result,
                    })
                })
                .collect(),
        )
    }))
    .await?;
    let provider = JsonRpcProvider::new_with_receipt_fallback(
        &primary_url,
        Some(reqwest::Url::parse(&fallback_url)?),
    )?;

    let bundles = provider
        .fetch_transaction_receipt_pairs_by_hashes(&[ProviderTransactionReceiptRequest {
            transaction_hash: tx_hash.to_owned(),
            block_hash: block_hash.to_owned(),
            block_number: 42,
            transaction_index: 7,
        }])
        .await?;

    assert_eq!(bundles.len(), 1);
    assert_eq!(bundles[0].transaction.transaction_hash, tx_hash);
    assert_eq!(bundles[0].receipt.transaction_hash, tx_hash);
    assert_eq!(bundles[0].receipt.transaction_index, 7);

    let primary_requests = primary_requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert!(
        primary_requests
            .iter()
            .any(|methods| methods == &vec!["eth_getBlockReceipts".to_owned()]),
        "primary provider should attempt block receipts before fallback recovery"
    );
    let fallback_requests = fallback_requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(
        fallback_requests,
        vec![vec![
            "eth_getTransactionByHash".to_owned(),
            "eth_getTransactionReceipt".to_owned(),
        ]]
    );

    primary_server.abort();
    fallback_server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_fetches_exact_block_bundle_with_block_scoped_receipts() -> Result<()> {
    let requested_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let parent_hash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let tx_hash_one = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let tx_hash_two = "0x2222222222222222222222222222222222222222222222222222222222222222";
    let log_hash = "0x3333333333333333333333333333333333333333333333333333333333333333";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(method.to_owned());

        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let result = match method {
            "eth_getBlockByHash" => {
                assert_eq!(params.get(1), Some(&Value::Bool(true)));
                rpc_exact_block_payload(
                    requested_hash,
                    parent_hash,
                    43,
                    Some("0x0102"),
                    vec![
                        rpc_transaction_payload(
                            tx_hash_one,
                            requested_hash,
                            43,
                            0,
                            "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                        ),
                        rpc_transaction_payload(
                            tx_hash_two,
                            requested_hash,
                            43,
                            1,
                            "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
                            None,
                        ),
                    ],
                )
            }
            "eth_getLogs" => {
                let filter = params
                    .first()
                    .and_then(Value::as_object)
                    .expect("log filter must be an object");
                assert_eq!(
                    filter.get("blockHash").and_then(Value::as_str),
                    Some(requested_hash)
                );
                Value::Array(vec![
                    rpc_log_payload(
                        log_hash,
                        requested_hash,
                        43,
                        0,
                        0,
                        "0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD",
                        tx_hash_one,
                    ),
                    rpc_log_payload(
                        "0x4444444444444444444444444444444444444444444444444444444444444444",
                        requested_hash,
                        43,
                        1,
                        1,
                        "0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
                        tx_hash_two,
                    ),
                ])
            }
            "eth_getBlockReceipts" => Value::Array(vec![
                rpc_receipt_payload(
                    tx_hash_two,
                    requested_hash,
                    43,
                    1,
                    Some("0x9999999999999999999999999999999999999999"),
                ),
                rpc_receipt_payload(
                    tx_hash_one,
                    requested_hash,
                    43,
                    0,
                    Some("0x8888888888888888888888888888888888888888"),
                ),
            ]),
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let bundle = provider
        .fetch_block_bundle_by_hash(&requested_hash.to_ascii_uppercase())
        .await?;

    assert_eq!(bundle.block.block_hash, requested_hash);
    assert_eq!(bundle.block.parent_hash, Some(parent_hash.to_owned()));
    assert_eq!(bundle.transactions.len(), 2);
    assert_eq!(bundle.transactions[0].transaction_hash, tx_hash_one);
    assert_eq!(
        bundle.transactions[0].from,
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(
        bundle.transactions[0].to.as_deref(),
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    );
    assert_eq!(bundle.transactions[1].to, None);
    assert_eq!(bundle.logs.len(), 2);
    assert_eq!(
        bundle.logs[0].address,
        "0xdddddddddddddddddddddddddddddddddddddddd"
    );
    assert_eq!(bundle.logs[0].block_hash, requested_hash);
    assert_eq!(bundle.receipts.len(), 2);
    assert_eq!(bundle.receipts[0].transaction_hash, tx_hash_one);
    assert_eq!(
        bundle.receipts[0].contract_address.as_deref(),
        Some("0x8888888888888888888888888888888888888888")
    );
    assert_eq!(bundle.receipts[1].transaction_hash, tx_hash_two);
    assert_eq!(
        bundle.receipts[1].contract_address.as_deref(),
        Some("0x9999999999999999999999999999999999999999")
    );

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(
        requests,
        vec![
            "eth_getBlockByHash".to_owned(),
            "eth_getLogs".to_owned(),
            "eth_getBlockReceipts".to_owned(),
        ]
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_fetches_exact_block_bundle_with_receipt_fallback() -> Result<()> {
    let requested_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let parent_hash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let tx_hash_one = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let tx_hash_two = "0x2222222222222222222222222222222222222222222222222222222222222222";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(method.to_owned());

        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        match method {
            "eth_getBlockByHash" => {
                assert_eq!(params.get(1), Some(&Value::Bool(true)));
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": rpc_exact_block_payload(
                        requested_hash,
                        parent_hash,
                        43,
                        None,
                        vec![
                            rpc_transaction_payload(
                                tx_hash_one,
                                requested_hash,
                                43,
                                0,
                                "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                            ),
                            rpc_transaction_payload(
                                tx_hash_two,
                                requested_hash,
                                43,
                                1,
                                "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
                                None,
                            ),
                        ],
                    )
                })
            }
            "eth_getLogs" => {
                let filter = params
                    .first()
                    .and_then(Value::as_object)
                    .expect("log filter must be an object");
                assert_eq!(
                    filter.get("blockHash").and_then(Value::as_str),
                    Some(requested_hash)
                );
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": [rpc_log_payload(
                        "0x3333333333333333333333333333333333333333333333333333333333333333",
                        requested_hash,
                        43,
                        0,
                        0,
                        "0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD",
                        tx_hash_one,
                    )]
                })
            }
            "eth_getBlockReceipts" => json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32601,
                    "message": "method not found"
                }
            }),
            "eth_getTransactionReceipt"
                if params.first().and_then(Value::as_str) == Some(tx_hash_one) =>
            {
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": rpc_receipt_payload(
                        tx_hash_one,
                        requested_hash,
                        43,
                        0,
                        Some("0x8888888888888888888888888888888888888888"),
                    )
                })
            }
            "eth_getTransactionReceipt"
                if params.first().and_then(Value::as_str) == Some(tx_hash_two) =>
            {
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": rpc_receipt_payload(
                        tx_hash_two,
                        requested_hash,
                        43,
                        1,
                        Some("0x9999999999999999999999999999999999999999"),
                    )
                })
            }
            _ => panic!("unexpected RPC request: {body}"),
        }
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let bundle = provider.fetch_block_bundle_by_hash(requested_hash).await?;
    assert_eq!(bundle.block.block_hash, requested_hash);
    assert_eq!(bundle.logs.len(), 1);
    assert_eq!(bundle.receipts.len(), 2);
    assert_eq!(bundle.receipts[1].transaction_hash, tx_hash_two);
    assert_eq!(
        bundle.receipts[0].contract_address.as_deref(),
        Some("0x8888888888888888888888888888888888888888")
    );

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(
        requests,
        vec![
            "eth_getBlockByHash".to_owned(),
            "eth_getLogs".to_owned(),
            "eth_getBlockReceipts".to_owned(),
            "eth_getTransactionReceipt".to_owned(),
            "eth_getTransactionReceipt".to_owned(),
        ]
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn json_rpc_provider_rejects_mismatched_bundle_transaction_hashes() -> Result<()> {
    let requested_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let returned_hash = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    let tx_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(method.to_owned());

        let result = match method {
            "eth_getBlockByHash" => rpc_exact_block_payload(
                requested_hash,
                ZERO_HASH,
                43,
                None,
                vec![rpc_transaction_payload(
                    tx_hash,
                    returned_hash,
                    43,
                    0,
                    "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                    None,
                )],
            ),
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result
        })
    }))
    .await?;
    let provider = JsonRpcProvider::new(&url)?;

    let error = provider
        .fetch_block_bundle_by_hash(&requested_hash.to_ascii_uppercase())
        .await
        .expect_err("mismatched transaction block hashes must fail");
    assert!(
            error
                .to_string()
                .contains("provider returned transaction 0x1111111111111111111111111111111111111111111111111111111111111111 for block 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa with mismatched block hash 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
        );

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(requests, vec!["eth_getBlockByHash".to_owned()]);

    server.abort();
    Ok(())
}

fn rpc_block_payload(
    hash: &str,
    parent_hash: &str,
    block_number: i64,
    logs_bloom: Option<&str>,
) -> Value {
    let mut payload = json!({
        "hash": hash,
        "parentHash": parent_hash,
        "number": format!("0x{block_number:x}"),
        "timestamp": format!("0x{:x}", 0x65f2d150 + block_number),
    });
    if let Some(logs_bloom) = logs_bloom {
        payload["logsBloom"] = Value::String(logs_bloom.to_owned());
    }

    payload
}

fn rpc_exact_block_payload(
    hash: &str,
    parent_hash: &str,
    block_number: i64,
    logs_bloom: Option<&str>,
    transactions: Vec<Value>,
) -> Value {
    let mut payload = rpc_block_payload(hash, parent_hash, block_number, logs_bloom);
    payload["transactions"] = Value::Array(transactions);
    payload
}

fn rpc_transaction_payload(
    hash: &str,
    block_hash: &str,
    block_number: i64,
    transaction_index: i64,
    from: &str,
    to: Option<&str>,
) -> Value {
    json!({
        "hash": hash,
        "blockHash": block_hash,
        "blockNumber": format!("0x{block_number:x}"),
        "transactionIndex": format!("0x{transaction_index:x}"),
        "from": from,
        "to": to,
    })
}

fn rpc_receipt_payload(
    transaction_hash: &str,
    block_hash: &str,
    block_number: i64,
    transaction_index: i64,
    contract_address: Option<&str>,
) -> Value {
    json!({
        "transactionHash": transaction_hash,
        "blockHash": block_hash,
        "blockNumber": format!("0x{block_number:x}"),
        "transactionIndex": format!("0x{transaction_index:x}"),
        "contractAddress": contract_address,
        "status": "0x1",
        "cumulativeGasUsed": "0x5208",
        "gasUsed": "0x5208",
        "logsBloom": "0x0102",
    })
}

fn rpc_log_payload(
    log_hash: &str,
    block_hash: &str,
    block_number: i64,
    transaction_index: i64,
    log_index: i64,
    address: &str,
    transaction_hash: &str,
) -> Value {
    json!({
        "address": address,
        "blockHash": block_hash,
        "blockNumber": format!("0x{block_number:x}"),
        "data": "0xdeadbeef",
        "logIndex": format!("0x{log_index:x}"),
        "removed": false,
        "topics": [log_hash],
        "transactionHash": transaction_hash,
        "transactionIndex": format!("0x{transaction_index:x}"),
    })
}

fn rpc_block_number_batch_response(body: &Value, blocks: &[(i64, &str)]) -> Option<Value> {
    let batch = body.as_array()?;
    Some(Value::Array(
        batch
            .iter()
            .map(|request| {
                assert_eq!(
                    request.get("method").and_then(Value::as_str),
                    Some("eth_getBlockByNumber")
                );
                let params = request
                    .get("params")
                    .and_then(Value::as_array)
                    .expect("block-number request must include params");
                assert_eq!(params.get(1), Some(&Value::Bool(false)));
                let block_number = parse_hex_i64(
                    params
                        .first()
                        .and_then(Value::as_str)
                        .expect("block-number request must include a number"),
                )
                .expect("block-number request must include valid hex");
                let block_hash = blocks
                    .iter()
                    .find_map(|(candidate_number, candidate_hash)| {
                        (*candidate_number == block_number).then_some(*candidate_hash)
                    })
                    .unwrap_or_else(|| {
                        panic!("unexpected block-number revalidation request: {request}")
                    });

                json!({
                    "jsonrpc": "2.0",
                    "id": request.get("id").cloned().unwrap_or(Value::Null),
                    "result": rpc_block_payload(block_hash, ZERO_HASH, block_number, None),
                })
            })
            .collect(),
    ))
}

async fn spawn_json_rpc_server(
    handler: Arc<dyn Fn(Value) -> Value + Send + Sync>,
) -> Result<(String, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind JSON-RPC test server")?;
    let address = listener
        .local_addr()
        .context("failed to read JSON-RPC test server address")?;
    let url = format!("http://{address}");

    let server = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut header_end = None;
                let mut content_length = 0usize;

                loop {
                    let mut chunk = [0_u8; 4096];
                    let Ok(read) = stream.read(&mut chunk).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }
                    buffer.extend_from_slice(&chunk[..read]);

                    if header_end.is_none()
                        && let Some(index) = find_header_end(&buffer)
                    {
                        header_end = Some(index);
                        content_length = parse_content_length(&buffer[..index]).unwrap_or(0);
                    }

                    if let Some(index) = header_end
                        && buffer.len() >= index + 4 + content_length
                    {
                        let body = &buffer[index + 4..index + 4 + content_length];
                        let request_body = serde_json::from_slice::<Value>(body).unwrap();
                        let response_body = handler(request_body).to_string();
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                            response_body.len(),
                            response_body
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                        let _ = stream.shutdown().await;
                        return;
                    }
                }
            });
        }
    });

    Ok((url, server))
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_content_length(headers: &[u8]) -> Option<usize> {
    let headers = std::str::from_utf8(headers).ok()?;
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("content-length") {
            value.trim().parse().ok()
        } else {
            None
        }
    })
}
