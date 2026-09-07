use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy_primitives::{Address, Bytes, keccak256};
use alloy_sol_types::SolValue;
use anyhow::{Context, Result as AnyResult, bail};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions, raw_sql};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};

use crate::{
    BASENAMES_NAMESPACE, ChainRpcUrls, ENS_NAMESPACE, EnsPrimaryNameStatus, ErrorKind,
    LedgerAction, LookupEngine, LookupPosition, LookupRequest, LookupResponse, RecordSelector,
    abi::{dns_encode_name, hex_string, namehash},
    ccip::encode_offchain_lookup_for_test,
};

const ETHEREUM: &str = "ethereum-mainnet";
const BASE: &str = "base-mainnet";
const ETHEREUM_HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const ETHEREUM_LATER_HASH: &str =
    "0x3333333333333333333333333333333333333333333333333333333333333333";
const ETHEREUM_PRIOR_HASH: &str =
    "0x4444444444444444444444444444444444444444444444444444444444444444";
const BASE_HASH: &str = "0x2222222222222222222222222222222222222222222222222222222222222222";
const UNIVERSAL_RESOLVER: &str = "0xeeeeeeee14d718c2b47d9923deab1335e144eeee";
const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const BASE_L1_RESOLVER: &str = "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31";
const REPLACEMENT_UNIVERSAL_RESOLVER: &str = "0x2000000000000000000000000000000000000002";
const SCHEMA_V2_MANIFEST_SYNC_LOCK: i64 = 0x4249_474e_414d_4532;
const INDEXED_VALUE: &str = "https://indexed.example";
const LIVE_VALUE: &str = "https://live.example";

enum RpcResponse {
    Result(Value),
    Error {
        code: i64,
        message: String,
        data: Value,
    },
}

#[derive(Clone, Copy)]
enum FixtureKind {
    Ens,
    Basenames,
}

struct Fixture {
    database: TestDatabase,
    logical_name_id: String,
}

impl Fixture {
    fn pool(&self) -> &PgPool {
        self.database.pool()
    }

    async fn cleanup(self) -> AnyResult<()> {
        self.database.cleanup().await
    }
}

#[test]
fn lookup_request_canonicalizes_and_deduplicates_selectors() -> crate::Result<()> {
    let request = LookupRequest::new("ens:alice.eth", ["addr:060", "addr:60", "text:url"])?;
    assert_eq!(
        request.records,
        vec![
            RecordSelector::parse("addr:60")?,
            RecordSelector::parse("text:url")?,
        ]
    );
    Ok(())
}

#[tokio::test]
async fn record_lookup_bounds_provider_call_concurrency_for_two_hundred_selectors() -> AnyResult<()>
{
    const SELECTOR_COUNT: usize = 200;
    const EXPECTED_MAX_CONCURRENCY: usize = 16;

    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let request = LookupRequest::new(
        &fixture.logical_name_id,
        (0..SELECTOR_COUNT).map(|index| format!("text:key-{index}")),
    )?;
    let (rpc_url, rpc_handle) = spawn_peak_concurrency_rpc(SELECTOR_COUNT).await?;

    let response = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup(request)
        .await?;
    let peak = rpc_handle.await??;
    assert_eq!(response.records.len(), SELECTOR_COUNT);
    assert!(
        peak <= EXPECTED_MAX_CONCURRENCY,
        "one lookup opened {peak} concurrent provider calls"
    );

    fixture.cleanup().await?;
    Ok(())
}

#[test]
fn lookup_request_preserves_exact_text_selector_keys() -> crate::Result<()> {
    let request = LookupRequest::new("ens:alice.eth", ["text:url ", "text: "])?;
    assert_eq!(
        request.records,
        vec![
            RecordSelector {
                record_key: "text: ".to_owned(),
                record_family: "text".to_owned(),
                selector_key: Some(" ".to_owned()),
            },
            RecordSelector {
                record_key: "text:url ".to_owned(),
                record_family: "text".to_owned(),
                selector_key: Some("url ".to_owned()),
            },
        ]
    );
    Ok(())
}

#[test]
fn lookup_request_rejects_postgres_unsafe_text_selectors() {
    let error = LookupRequest::new("ens:alice.eth", ["text:a\0b"])
        .expect_err("NUL-bearing text selectors cannot cross the PostgreSQL boundary");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

#[test]
fn avatar_comparison_uses_the_text_avatar_inventory_entry() -> crate::Result<()> {
    let entries = json!([{
        "record_key": "text:avatar",
        "record_family": "text",
        "selector_key": "avatar",
        "status": "success",
        "value": { "kind": "text", "value": "ipfs://avatar" },
    }]);
    assert_eq!(
        crate::store::indexed_answer(&entries, &RecordSelector::parse("avatar")?),
        json!({ "status": "success", "value": "ipfs://avatar" })
    );
    Ok(())
}

#[tokio::test]
async fn rust_and_sql_indexed_answer_derivations_are_equivalent() -> AnyResult<()> {
    let oversized_text_key = "k".repeat(4_096);
    let oversized_record_key = format!("text:{oversized_text_key}");
    let read_rule = json!({"read_rules":[{
        "kind":"ensip19_default_address",
        "source_record_key":"addr:2147483648"
    }]});
    let cases = vec![
        (
            "nested value",
            "text:url".to_owned(),
            json!([{
                "record_key": "text:url",
                "record_family": "text",
                "selector_key": "url",
                "status": "success",
                "value": { "value": "https://value.example" },
            }]),
            json!({}),
        ),
        (
            "nested bytes",
            "contenthash".to_owned(),
            json!([{
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "status": "success",
                "value": { "bytes": "0xe3010170" },
            }]),
            json!({}),
        ),
        (
            "avatar exact entry preferred over text fallback",
            "avatar".to_owned(),
            json!([
                {
                    "record_key": "text:avatar",
                    "record_family": "text",
                    "selector_key": "avatar",
                    "status": "success",
                    "value": { "value": "ipfs://fallback" },
                },
                {
                    "record_key": "avatar",
                    "record_family": "avatar",
                    "selector_key": null,
                    "status": "success",
                    "value": { "value": "ipfs://preferred" },
                },
            ]),
            json!({}),
        ),
        (
            "address value lowercasing",
            "addr:60".to_owned(),
            json!([{
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "success",
                "value": { "bytes": "0xAbCdEf0123" },
            }]),
            json!({}),
        ),
        (
            "absent entry",
            "text:missing".to_owned(),
            json!([]),
            json!({}),
        ),
        (
            "null value",
            "text:null".to_owned(),
            json!([{
                "record_key": "text:null",
                "record_family": "text",
                "selector_key": "null",
                "status": "success",
                "value": null,
            }]),
            json!({}),
        ),
        (
            "oversized text key",
            oversized_record_key.clone(),
            json!([{
                "record_key": oversized_record_key,
                "record_family": "text",
                "selector_key": oversized_text_key,
                "status": "success",
                "value": { "value": "oversized-key-value" },
            }]),
            json!({}),
        ),
        (
            "legacy failed status alias",
            "text:failed".to_owned(),
            json!([{
                "record_key":"text:failed",
                "record_family":"text",
                "selector_key":"failed",
                "status":"failed",
                "failure_reason":"record_read_failed"
            }]),
            json!({}),
        ),
        (
            "empty exact address derives from default",
            "addr:60".to_owned(),
            json!([
                {
                    "record_key":"addr:60",
                    "record_family":"addr",
                    "selector_key":"60",
                    "status":"not_found"
                },
                {
                    "record_key":"addr:2147483648",
                    "record_family":"addr",
                    "selector_key":"2147483648",
                    "status":"success",
                    "value":{"encoding":"hex","bytes":"0x0000000000000000000000000000000000000def"}
                }
            ]),
            read_rule.clone(),
        ),
        (
            "empty default address derives not found",
            "addr:60".to_owned(),
            json!([{
                "record_key":"addr:2147483648",
                "record_family":"addr",
                "selector_key":"2147483648",
                "status":"not_found"
            }]),
            read_rule.clone(),
        ),
        (
            "zero20 default for legacy address derives not found",
            "addr:60".to_owned(),
            json!([{
                "record_key":"addr:2147483648",
                "record_family":"addr",
                "selector_key":"2147483648",
                "status":"success",
                "value":{"encoding":"hex","bytes":"0x0000000000000000000000000000000000000000"}
            }]),
            read_rule.clone(),
        ),
        (
            "zero20 default for EVM multicoin remains success",
            "addr:2147483649".to_owned(),
            json!([{
                "record_key":"addr:2147483648",
                "record_family":"addr",
                "selector_key":"2147483648",
                "status":"success",
                "value":{"encoding":"hex","bytes":"0x0000000000000000000000000000000000000000"}
            }]),
            read_rule,
        ),
    ]
    .into_iter()
    .map(|(name, record_key, entries, provenance)| {
        (
            name,
            record_key,
            entries,
            provenance,
            json!({"status":"projected"}),
            "supported",
        )
    })
    .chain([(
        "exact not found with non-authoritative coverage",
        "text:empty".to_owned(),
        json!([{
            "record_key":"text:empty",
            "record_family":"text",
            "selector_key":"empty",
            "status":"not_found"
        }]),
        json!({}),
        json!({
            "status":"unsupported",
            "unsupported_reason":"coverage_incomplete"
        }),
        "unsupported",
    )]);
    let marker_value = "0x2222222222222222222222222222222222222222";
    let marker_cases = [
        ("marked absence", Some("not_found"), json!(["addr:60"])),
        ("marked incomplete", Some("not_found"), json!(["addr:60"])),
        ("unmarked absence", Some("not_found"), json!([])),
        ("orphan marker", None, json!(["addr:60"])),
        ("exact success", Some("success"), json!(["addr:60"])),
        ("malformed marker", Some("not_found"), json!("addr:60")),
        ("unrelated marker", Some("not_found"), json!(["addr:61"])),
    ]
    .into_iter()
    .map(|(name, exact_status, marker)| {
        let mut entries = vec![json!({
            "record_key":"addr:2147483648", "record_family":"addr",
            "selector_key":"2147483648", "status":"success", "value":marker_value
        })];
        if let Some(status) = exact_status {
            let mut exact = json!({
                "record_key":"addr:60", "record_family":"addr",
                "selector_key":"60", "status":status
            });
            if status == "success" {
                exact["value"] = json!(marker_value);
            }
            entries.push(exact);
        }
        (
            name,
            "addr:60".to_owned(),
            json!(entries),
            json!({
                "read_rules":[{"kind":"ensip19_default_address",
                    "source_record_key":"addr:2147483648"}],
                "exact_nonempty_not_found_record_keys":marker
            }),
            json!({"status": if name == "marked incomplete" { "unsupported" } else { "projected" }}),
            if name == "marked incomplete" { "unsupported" } else { "supported" },
        )
    });
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;

    for (case_name, record_key, entries, provenance, coverage, support_status) in
        cases.chain(marker_cases)
    {
        let selector = RecordSelector::parse(&record_key)?;
        let rust_answer = bigname_domain::resolver_read::evaluate_indexed_record(
            &entries,
            &provenance,
            &coverage,
            &selector.record_key,
            &selector.record_family,
            selector.selector_key.as_deref(),
        )
        .comparison_value();
        sqlx::query(
            "UPDATE record_inventory_current
             SET entries = $1,
                 provenance = $2,
                 support_status = $3,
                 unsupported_reason = CASE WHEN $3 = 'unsupported'
                     THEN 'coverage_incomplete'
                     ELSE NULL
                 END",
        )
        .bind(&entries)
        .bind(&provenance)
        .bind(support_status)
        .execute(fixture.pool())
        .await?;
        let snapshot = crate::store::load_snapshot(
            fixture.pool(),
            &LookupRequest::new(&fixture.logical_name_id, [&record_key])?,
        )
        .await?;
        let (resource_id, boundary_key, row_xmin): (String, String, String) = sqlx::query_as(
            "SELECT resource_id::text, record_version_boundary_key, xmin::text
             FROM record_inventory_current",
        )
        .fetch_one(fixture.pool())
        .await?;
        let live_probe = json!({ "status": "derivation_probe", "case": case_name });
        let write_status: String = sqlx::query_scalar(
            "SELECT write_resolution_divergence(
                 $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, false
             )",
        )
        .bind(&resource_id)
        .bind(&boundary_key)
        .bind(&row_xmin)
        .bind(&snapshot.authoritative_position.chain_id)
        .bind(snapshot.authoritative_position.block_number)
        .bind(&snapshot.authoritative_position.block_hash)
        .bind(&snapshot.execution_authority)
        .bind(&snapshot.logical_name_id)
        .bind(&snapshot.resolver_chain_id)
        .bind(&snapshot.resolver_address)
        .bind(&record_key)
        .bind(&snapshot.revalidation_positions)
        .bind(&live_probe)
        .fetch_one(fixture.pool())
        .await?;
        assert_eq!(write_status, "written", "SQL probe failed for {case_name}");
        let sql_answer: Value = sqlx::query_scalar(
            "SELECT indexed_result
             FROM resolution_divergences
             WHERE request_kind = $1 AND cleared_at IS NULL",
        )
        .bind(&record_key)
        .fetch_one(fixture.pool())
        .await?;
        if provenance
            .get("exact_nonempty_not_found_record_keys")
            .is_some()
        {
            let expected = if case_name == "marked incomplete" {
                json!({"status":"unsupported"})
            } else if case_name == "marked absence" {
                json!({"status":"not_found"})
            } else {
                json!({"status":"success", "value":marker_value})
            };
            assert_eq!(sql_answer, expected, "SQL marker case: {case_name}");
            assert_eq!(rust_answer, expected, "Rust marker case: {case_name}");
        }
        assert_eq!(
            rust_answer, sql_answer,
            "derivation mismatch for {case_name}"
        );
        sqlx::query("DELETE FROM resolution_divergences")
            .execute(fixture.pool())
            .await?;
    }

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn empty_ensip19_default_agrees_with_live_miss_without_divergence() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(encoded_address_result(
        "0x0000000000000000000000000000000000000000",
    )?)])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE record_inventory_current
         SET selectors = $1, entries = $2, provenance = $3",
    )
    .bind(json!([{
        "record_key":"addr:2147483648",
        "record_family":"addr",
        "selector_key":"2147483648"
    }]))
    .bind(json!([{
        "record_key":"addr:2147483648",
        "record_family":"addr",
        "selector_key":"2147483648",
        "status":"not_found"
    }]))
    .bind(json!({"read_rules":[{
        "kind":"ensip19_default_address",
        "source_record_key":"addr:2147483648"
    }]}))
    .execute(fixture.pool())
    .await?;

    let outcome = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup(LookupRequest::new(&fixture.logical_name_id, ["addr:60"])?)
        .await?;
    assert_eq!(outcome.records[0].ledger_action, LedgerAction::None);
    assert_eq!(
        outcome.records[0].status,
        crate::LookupRecordStatus::NotFound
    );
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(requests.len(), 1);
    Ok(())
}

#[tokio::test]
async fn zero20_ensip19_default_agrees_with_legacy_live_miss_without_divergence() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(encoded_address_result(
        "0x0000000000000000000000000000000000000000",
    )?)])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE record_inventory_current
         SET selectors = $1, entries = $2, provenance = $3",
    )
    .bind(json!([{
        "record_key":"addr:2147483648",
        "record_family":"addr",
        "selector_key":"2147483648"
    }]))
    .bind(json!([{
        "record_key":"addr:2147483648",
        "record_family":"addr",
        "selector_key":"2147483648",
        "status":"success",
        "value":{"encoding":"hex","bytes":"0x0000000000000000000000000000000000000000"}
    }]))
    .bind(json!({"read_rules":[{
        "kind":"ensip19_default_address",
        "source_record_key":"addr:2147483648"
    }]}))
    .execute(fixture.pool())
    .await?;

    let outcome = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup(LookupRequest::new(&fixture.logical_name_id, ["addr:60"])?)
        .await?;
    assert_eq!(outcome.records[0].ledger_action, LedgerAction::None);
    assert_eq!(
        outcome.records[0].status,
        crate::LookupRecordStatus::NotFound
    );
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(requests.len(), 1);
    Ok(())
}

#[tokio::test]
async fn agreeing_resolution_without_active_divergence_writes_nothing() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(
        INDEXED_VALUE,
    ))])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let result = run_lookup(&fixture, &rpc_url).await;
    let outcome = finish_fixture(fixture, result).await?;

    assert_eq!(outcome.records[0].ledger_action, LedgerAction::None);
    assert_eq!(outcome.records[0].value, Some(json!(INDEXED_VALUE)));
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn restored_agreement_clears_the_matching_active_divergence() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(encoded_text_result(LIVE_VALUE)),
        RpcResponse::Result(encoded_text_result(INDEXED_VALUE)),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;

    let disagreement = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(disagreement.records[0].ledger_action, LedgerAction::Written);
    assert_eq!(ledger_count(fixture.pool()).await?, 1);

    let agreement = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(agreement.records[0].ledger_action, LedgerAction::Cleared);
    let (total, active, cleared): (i64, i64, i64) = sqlx::query_as(
        "SELECT count(*),
                count(*) FILTER (WHERE cleared_at IS NULL),
                count(*) FILTER (WHERE cleared_at IS NOT NULL)
         FROM resolution_divergences",
    )
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!((total, active, cleared), (1, 0, 1));

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(requests.len(), 2);
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn ensip19_derived_indexed_agreement_clears_a_prior_divergence() -> AnyResult<()> {
    let default_address = "0x0000000000000000000000000000000000000def";
    let different_address = "0x0000000000000000000000000000000000000abc";
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(encoded_address_result(different_address)?),
        RpcResponse::Result(encoded_address_result(default_address)?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE record_inventory_current
         SET selectors = $1, entries = $2, provenance = $3",
    )
    .bind(json!([{
        "record_key":"addr:2147483648",
        "record_family":"addr",
        "selector_key":"2147483648"
    }]))
    .bind(json!([{
        "record_key":"addr:2147483648",
        "record_family":"addr",
        "selector_key":"2147483648",
        "status":"success",
        "value":default_address
    }]))
    .bind(json!({"read_rules":[{
        "kind":"ensip19_default_address",
        "source_record_key":"addr:2147483648"
    }]}))
    .execute(fixture.pool())
    .await?;
    let request = || LookupRequest::new(&fixture.logical_name_id, ["addr:60"]);

    let disagreement = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup(request()?)
        .await?;
    assert_eq!(disagreement.records[0].ledger_action, LedgerAction::Written);

    let agreement = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup(request()?)
        .await?;
    assert_eq!(agreement.records[0].ledger_action, LedgerAction::Cleared);
    let active: i64 =
        sqlx::query_scalar("SELECT count(*) FROM resolution_divergences WHERE cleared_at IS NULL")
            .fetch_one(fixture.pool())
            .await?;
    assert_eq!(active, 0);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(requests.len(), 2);
    Ok(())
}

#[tokio::test]
async fn unadmitted_serving_position_fails_before_rpc_or_ledger_write() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup_at_positions(
            lookup_request(&fixture.logical_name_id)?,
            &[LookupPosition {
                chain_id: ETHEREUM.to_owned(),
                block_number: 9,
                block_hash: ETHEREUM_PRIOR_HASH.to_owned(),
                timestamp: "2026-08-03T00:00:00Z".to_owned(),
            }],
        )
        .await
        .expect_err("a stale serving snapshot must be rejected before execution");

    assert_eq!(error.kind(), ErrorKind::Stale);
    assert_eq!(
        error.message(),
        "lookup authoritative position is not present in the caller's admitted snapshot"
    );
    assert_eq!(ledger_count(fixture.pool()).await?, 0);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn disagreement_writes_one_ledger_row_with_answers_and_anchor() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let result = run_lookup(&fixture, &rpc_url).await;

    let response = result?;
    assert_eq!(response.records[0].ledger_action, LedgerAction::Written);
    let (count, indexed, live, positions): (i64, Value, Value, Value) = sqlx::query_as(
        r#"
        SELECT count(*) OVER (), indexed_result, live_result, observed_positions
        FROM resolution_divergences
        WHERE cleared_at IS NULL
        "#,
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(count, 1);
    assert_eq!(
        indexed,
        json!({ "status": "success", "value": INDEXED_VALUE })
    );
    assert_eq!(live, json!({ "status": "success", "value": LIVE_VALUE }));
    assert_eq!(positions["ethereum"]["block_hash"], ETHEREUM_HASH);
    assert_eq!(positions["ethereum"]["block_number"], 10);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn least_privileged_api_role_can_guard_and_write_only_through_functions() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let role_name = format!(
        "lookup_api_{}",
        fixture
            .database
            .database_name()
            .chars()
            .rev()
            .take(40)
            .collect::<String>()
            .chars()
            .rev()
            .collect::<String>()
    );
    let role = quote_identifier(&role_name);
    raw_sql(&format!(
        "CREATE ROLE {role}
             NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
             NOREPLICATION NOBYPASSRLS;
         GRANT {role} TO CURRENT_USER;
         GRANT TEMPORARY ON DATABASE {} TO {role};
         GRANT USAGE ON SCHEMA bigname_phase TO {role};
         GRANT SELECT ON TABLE
             bigname_phase.chain_heads,
             bigname_phase.chain_lineage,
             bigname_phase.chain_phase_state,
             bigname_phase.name_current,
             bigname_phase.name_surfaces,
             bigname_phase.resources,
             bigname_phase.surface_bindings,
             bigname_phase.token_lineages,
             bigname_phase.record_inventory_current,
             bigname_phase.manifest_versions,
             bigname_phase.manifest_contract_instances
         TO {role};
         GRANT EXECUTE ON FUNCTION bigname_phase.revalidate_resolution_lookup_state(
             text, bigint, text, jsonb, jsonb, uuid, text, text
         ) TO {role};
         GRANT EXECUTE ON FUNCTION bigname_phase.write_resolution_divergence(
             uuid, text, text, text, bigint, text, jsonb, text, text, text,
             text, jsonb, jsonb, boolean
         ) TO {role};",
        quote_identifier(fixture.database.database_name())
    ))
    .execute(fixture.pool())
    .await?;

    let connect_options = fixture
        .pool()
        .connect_options()
        .as_ref()
        .clone()
        .options([("role", role_name.as_str())]);
    let api_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(connect_options)
        .await?;

    let request = lookup_request(&fixture.logical_name_id)?;
    let snapshot = crate::store::load_snapshot(&api_pool, &request).await?;

    let mut api_connection = api_pool.acquire().await?;
    let (resource_id, boundary_key, row_xmin): (String, String, String) = sqlx::query_as(
        "SELECT resource_id::text, record_version_boundary_key, xmin::text
         FROM record_inventory_current",
    )
    .fetch_one(&mut *api_connection)
    .await?;
    raw_sql(
        "CREATE TEMP TABLE chain_heads (shadow text);
         CREATE TEMP TABLE chain_lineage (shadow text);
         CREATE TEMP TABLE record_inventory_current (shadow text);
         CREATE TEMP TABLE resolution_divergences (shadow text);",
    )
    .execute(&mut *api_connection)
    .await?;
    let positions = observed_position(10, ETHEREUM_HASH, "2026-08-03T00:00:00Z");
    let guard_status: String = sqlx::query_scalar(
        "SELECT bigname_phase.revalidate_resolution_lookup_state(
             $1, 10, $2, $3, $4, $5::uuid, $6, $7
         )",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_HASH)
    .bind(&positions)
    .bind(&snapshot.execution_authority)
    .bind(&resource_id)
    .bind(&boundary_key)
    .bind(&row_xmin)
    .fetch_one(&mut *api_connection)
    .await?;
    assert_eq!(guard_status, "unchanged");
    let indexed_answer = json!({ "status": "success", "value": INDEXED_VALUE });
    let writer_status: String = sqlx::query_scalar(
        "SELECT bigname_phase.write_resolution_divergence(
             $1::uuid, $2, $3, $4, 10, $5, $6, $7, $4, $8, $9, $10, $11, false
         )",
    )
    .bind(&resource_id)
    .bind(&boundary_key)
    .bind(&row_xmin)
    .bind(ETHEREUM)
    .bind(ETHEREUM_HASH)
    .bind(&snapshot.execution_authority)
    .bind(&fixture.logical_name_id)
    .bind("0x1000000000000000000000000000000000000001")
    .bind("text:url")
    .bind(&positions)
    .bind(&indexed_answer)
    .fetch_one(&mut *api_connection)
    .await?;
    assert_eq!(writer_status, "agreement");
    let forged_status: String = sqlx::query_scalar(
        "SELECT bigname_phase.write_resolution_divergence(
             $1::uuid, $2, $3, $4, 10, $5, $6, 'ens:forged', $4, $7, 'text:url',
             $8, $9, false
         )",
    )
    .bind(&resource_id)
    .bind(&boundary_key)
    .bind(&row_xmin)
    .bind(ETHEREUM)
    .bind(ETHEREUM_HASH)
    .bind(&snapshot.execution_authority)
    .bind("0x1000000000000000000000000000000000000001")
    .bind(&positions)
    .bind(&indexed_answer)
    .fetch_one(&mut *api_connection)
    .await?;
    assert_eq!(forged_status, "guard_rejected");
    raw_sql(
        "DROP TABLE pg_temp.chain_heads;
         DROP TABLE pg_temp.chain_lineage;
         DROP TABLE pg_temp.record_inventory_current;
         DROP TABLE pg_temp.resolution_divergences;",
    )
    .execute(&mut *api_connection)
    .await?;
    drop(api_connection);

    let direct_write_error = sqlx::query(
        "UPDATE record_inventory_current
         SET entries = entries
         WHERE false",
    )
    .execute(&api_pool)
    .await
    .expect_err("the API role must not update guarded projection rows directly");
    assert_eq!(
        direct_write_error
            .as_database_error()
            .and_then(|error| error.code().map(|code| code.into_owned()))
            .as_deref(),
        Some("42501")
    );

    let raw_read_error = sqlx::query("SELECT count(*) FROM raw_logs")
        .fetch_one(&api_pool)
        .await
        .expect_err("the API role must not read raw facts");
    assert_eq!(
        raw_read_error
            .as_database_error()
            .and_then(|error| error.code().map(|code| code.into_owned()))
            .as_deref(),
        Some("42501")
    );

    let response = lookup_engine(&api_pool, &rpc_url)?.lookup(request).await?;
    assert_eq!(response.records[0].ledger_action, LedgerAction::Written);
    assert_eq!(ledger_count(fixture.pool()).await?, 1);

    api_pool.close().await;
    raw_sql(&format!(
        "DROP OWNED BY {role};
         REVOKE {role} FROM CURRENT_USER;
         DROP ROLE {role};"
    ))
    .execute(fixture.pool())
    .await?;
    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn stable_projection_row_executes_at_caught_up_head() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(
        INDEXED_VALUE,
    ))])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    advance_head_and_project(fixture.pool()).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(response.records[0].ledger_action, LedgerAction::None);
    assert_eq!(response.records[0].value, Some(json!(INDEXED_VALUE)));
    assert_eq!(response.observed_positions["ethereum"]["block_number"], 10);
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_LATER_HASH);
    Ok(())
}

#[tokio::test]
async fn stable_projection_divergence_tracks_live_reorg_dependency() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    advance_head_and_project(fixture.pool()).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(response.records[0].ledger_action, LedgerAction::Written);
    assert_eq!(response.observed_positions["ethereum"]["block_number"], 10);
    let positions: Value = sqlx::query_scalar(
        "SELECT observed_positions FROM resolution_divergences WHERE cleared_at IS NULL",
    )
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(positions["indexed"]["block_number"], 10);
    assert_eq!(positions["indexed"]["block_hash"], ETHEREUM_HASH);
    assert_eq!(positions["live"]["block_number"], 11);
    assert_eq!(positions["live"]["block_hash"], ETHEREUM_LATER_HASH);

    let mut transaction = fixture.pool().begin().await?;
    sqlx::query("DELETE FROM chain_heads WHERE chain_id = $1")
        .bind(ETHEREUM)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_LATER_HASH)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    let (active, cleared): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE cleared_at IS NULL),
                count(*) FILTER (WHERE cleared_at IS NOT NULL)
         FROM resolution_divergences",
    )
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!((active, cleared), (0, 1));

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_LATER_HASH);
    Ok(())
}

#[tokio::test]
async fn lookup_is_stale_while_project_cursor_lags_the_head() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    advance_head(fixture.pool()).await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("lookup must wait for project to publish the newest processed head");
    assert_eq!(error.kind(), ErrorKind::Stale);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn divergence_shape_check_violation_is_non_retryable() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let answer = json!({ "status": "not_found" });
    let error = sqlx::query(
        r#"
        INSERT INTO resolution_divergences (
            logical_name_id, resolver_chain_id, resolver_address, request_kind,
            observed_positions, indexed_result, live_result
        ) VALUES ($1, $2, $3, 'text:url', $4, $5, $5)
        "#,
    )
    .bind(&fixture.logical_name_id)
    .bind(ETHEREUM)
    .bind("0x1000000000000000000000000000000000000001")
    .bind(observed_position(10, ETHEREUM_HASH, "2026-08-03T00:00:00Z"))
    .bind(answer)
    .execute(fixture.pool())
    .await
    .expect_err("equal answers must violate the divergence table shape");
    let error = crate::store::divergence_write_error(error);
    assert_eq!(error.kind(), ErrorKind::Database);

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn active_divergence_uniqueness_violation_is_concurrent_state() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, 9, '2026-08-02T23:59:59Z', 'canonical')",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_PRIOR_HASH)
    .execute(fixture.pool())
    .await?;
    for positions in [
        observed_position(10, ETHEREUM_HASH, "2026-08-03T00:00:00Z"),
        observed_position(9, ETHEREUM_PRIOR_HASH, "2026-08-02T23:59:59Z"),
    ] {
        let result = sqlx::query(
            r#"
            INSERT INTO resolution_divergences (
                logical_name_id, resolver_chain_id, resolver_address, request_kind,
                observed_positions, indexed_result, live_result
            ) VALUES (
                $1, $2, $3, 'text:url', $4,
                '{"status":"success","value":"indexed"}'::jsonb,
                '{"status":"success","value":"live"}'::jsonb
            )
            "#,
        )
        .bind(&fixture.logical_name_id)
        .bind(ETHEREUM)
        .bind("0x1000000000000000000000000000000000000001")
        .bind(positions)
        .execute(fixture.pool())
        .await;
        if let Err(error) = result {
            let error = crate::store::divergence_write_error(error);
            assert_eq!(error.kind(), ErrorKind::ConcurrentState);
            fixture.cleanup().await?;
            return Ok(());
        }
    }
    bail!("two active same-request divergences at different heads were accepted")
}

#[tokio::test]
async fn unsupported_resolver_inventory_does_not_block_live_lookup() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE record_inventory_current
         SET support_status = 'unsupported',
             unsupported_reason = 'resolver_family_unsupported'",
    )
    .execute(fixture.pool())
    .await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(response.records[0].value, Some(json!(LIVE_VALUE)));
    assert_eq!(response.records[0].ledger_action, LedgerAction::Written);
    assert_eq!(ledger_count(fixture.pool()).await?, 1);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn out_of_class_projected_topology_is_not_executed() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE name_current
         SET declared_summary = jsonb_set(
             jsonb_set(
                 declared_summary,
                 '{topology,subregistry_path}',
                 '[{\"logical_name_id\":\"ens:ancestor\"}]'::jsonb
             ),
             '{topology,resolver_path,0,logical_name_id}',
             to_jsonb('ens:ancestor'::text)
         )",
    )
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("linked-subregistry path must not execute");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn unsupported_basenames_topology_precedes_position_and_manifest_reads() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    sqlx::query(
        r#"UPDATE name_current
           SET declared_summary = jsonb_set(
               declared_summary,
               '{topology,alias}',
               '{"final_target":{"logical_name_id":"basenames:alias"},
                 "hops":[{"logical_name_id":"basenames:alias"}]}'::jsonb
           )"#,
    )
    .execute(fixture.pool())
    .await?;
    sqlx::query("DELETE FROM chain_heads")
        .execute(fixture.pool())
        .await?;
    sqlx::query("DELETE FROM manifest_versions")
        .execute(fixture.pool())
        .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("unsupported topology must be rejected before snapshot dependencies");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn wildcard_lookup_executes_without_an_indexed_record_row() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE name_current
         SET declared_summary = jsonb_set(
             jsonb_set(
                 declared_summary,
                 '{topology,wildcard}',
                 jsonb_build_object(
                     'source', jsonb_build_object('logical_name_id', 'ens:ancestor'),
                     'matched_labels', jsonb_build_array('alice')
                 )
             ),
             '{topology,resolver_path,0,logical_name_id}',
             to_jsonb('ens:ancestor'::text)
         )",
    )
    .execute(fixture.pool())
    .await?;
    sqlx::query("DELETE FROM record_inventory_current")
        .execute(fixture.pool())
        .await?;

    let result = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(result.records[0].value, Some(json!(LIVE_VALUE)));
    assert_eq!(result.records[0].ledger_action, LedgerAction::None);
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn null_exact_resolver_executes_without_record_boundary_or_ledger_comparison() -> AnyResult<()>
{
    let effective = "0x3000000000000000000000000000000000000003";
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(
        encoded_text_result_with_resolver(LIVE_VALUE, effective)?,
    )])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(response.records[0].value, Some(json!(LIVE_VALUE)));
    assert_eq!(response.records[0].ledger_action, LedgerAction::None);
    assert_eq!(
        response.resolver_address,
        "0x0000000000000000000000000000000000000000"
    );
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn null_exact_resolver_requires_one_effective_resolver_for_the_batch() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(encoded_text_result_with_resolver(
            "first",
            "0x3000000000000000000000000000000000000003",
        )?),
        RpcResponse::Result(encoded_text_result_with_resolver(
            "second",
            "0x4000000000000000000000000000000000000004",
        )?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;

    let error = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup(LookupRequest::new(
            &fixture.logical_name_id,
            ["avatar", "text:url"],
        )?)
        .await
        .expect_err("different effective resolvers must fail closed");
    assert_eq!(error.kind(), ErrorKind::Execution);
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn discovery_rejects_success_and_malformed_inner_from_different_resolvers() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(encoded_text_result_with_resolver(
            LIVE_VALUE,
            "0x3000000000000000000000000000000000000003",
        )?),
        RpcResponse::Result(encoded_malformed_text_result_with_resolver(
            "0x4000000000000000000000000000000000000004",
        )?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;

    let error = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup(LookupRequest::new(
            &fixture.logical_name_id,
            ["avatar", "text:url"],
        )?)
        .await
        .expect_err("malformed inner data from another resolver must fail closed");
    assert_eq!(error.kind(), ErrorKind::Execution);
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn discovery_rejects_resolver_not_found_and_malformed_inner_resolver() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Error {
            code: 3,
            message: "execution reverted".to_owned(),
            data: Value::String(resolver_not_found_revert(b"\x05alice\x03eth\0")),
        },
        RpcResponse::Result(encoded_malformed_text_result_with_resolver(
            "0x4000000000000000000000000000000000000004",
        )?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;

    let error = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup(LookupRequest::new(
            &fixture.logical_name_id,
            ["avatar", "text:url"],
        )?)
        .await
        .expect_err("resolver-not-found and a decoded resolver must fail closed");
    assert_eq!(error.kind(), ErrorKind::Execution);
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn discovery_keeps_malformed_inner_per_key_for_consistent_resolver() -> AnyResult<()> {
    let effective = "0x3000000000000000000000000000000000000003";
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(encoded_text_result_with_resolver(LIVE_VALUE, effective)?),
        RpcResponse::Result(encoded_malformed_text_result_with_resolver(effective)?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;

    let response = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup(LookupRequest::new(
            &fixture.logical_name_id,
            ["avatar", "text:url"],
        )?)
        .await?;
    assert!(
        response
            .records
            .iter()
            .any(|record| record.status == crate::LookupRecordStatus::Success)
    );
    assert!(
        response
            .records
            .iter()
            .any(|record| record.status == crate::LookupRecordStatus::ExecutionFailed)
    );
    assert!(
        response
            .records
            .iter()
            .all(|record| record.ledger_action == LedgerAction::None)
    );
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn resolver_not_found_envelopes_are_live_not_found_without_ledger_mutation() -> AnyResult<()>
{
    let revert = resolver_not_found_revert(b"\x05alice\x03eth\0");
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Error {
        code: 3,
        message: "execution reverted".to_owned(),
        data: json!({"originalError":{"data":revert}}),
    }])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(
        response.records[0].status,
        crate::LookupRecordStatus::NotFound
    );
    assert_eq!(
        response.records[0].failure_reason.as_deref(),
        Some("resolver_not_found")
    );
    assert_eq!(response.records[0].ledger_action, LedgerAction::None);
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn resolver_not_found_must_match_the_discovery_name() -> AnyResult<()> {
    let revert = resolver_not_found_revert(b"\x03bob\x03eth\0");
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Error {
        code: 3,
        message: "execution reverted".to_owned(),
        data: Value::String(revert),
    }])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(
        response.records[0].status,
        crate::LookupRecordStatus::ExecutionFailed
    );
    assert_eq!(ledger_count(fixture.pool()).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn projected_route_does_not_reclassify_resolver_not_found() -> AnyResult<()> {
    let revert = resolver_not_found_revert(b"\x05alice\x03eth\0");
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Error {
        code: 3,
        message: "execution reverted".to_owned(),
        data: Value::String(revert),
    }])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(
        response.records[0].status,
        crate::LookupRecordStatus::ExecutionFailed
    );
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn discovery_rejects_mixed_resolver_not_found_and_success() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Error {
            code: 3,
            message: "execution reverted".to_owned(),
            data: Value::String(resolver_not_found_revert(b"\x05alice\x03eth\0")),
        },
        RpcResponse::Result(encoded_text_result_with_resolver(
            LIVE_VALUE,
            "0x3000000000000000000000000000000000000003",
        )?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;

    let error = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup(LookupRequest::new(
            &fixture.logical_name_id,
            ["avatar", "text:url"],
        )?)
        .await
        .expect_err("one block cannot both lack and select an effective resolver");
    assert_eq!(error.kind(), ErrorKind::Execution);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn discovery_keeps_resolver_not_found_and_malformed_results_per_key() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Error {
            code: 3,
            message: "execution reverted".to_owned(),
            data: Value::String(resolver_not_found_revert(b"\x05alice\x03eth\0")),
        },
        RpcResponse::Result(Value::String("0x1234".to_owned())),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;

    let response = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup(LookupRequest::new(
            &fixture.logical_name_id,
            ["avatar", "text:url"],
        )?)
        .await?;
    assert!(response.records.iter().any(|record| {
        record.status == crate::LookupRecordStatus::NotFound
            && record.failure_reason.as_deref() == Some("resolver_not_found")
    }));
    assert!(
        response
            .records
            .iter()
            .any(|record| record.status == crate::LookupRecordStatus::ExecutionFailed)
    );
    assert!(
        response
            .records
            .iter()
            .all(|record| record.ledger_action == LedgerAction::None)
    );
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn null_exact_resolver_returns_mixed_values_and_empty_record_misses() -> AnyResult<()> {
    let effective = "0x3000000000000000000000000000000000000003";
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(encoded_text_result_with_resolver("", effective)?),
        RpcResponse::Result(encoded_text_result_with_resolver(LIVE_VALUE, effective)?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;

    let response = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup(LookupRequest::new(
            &fixture.logical_name_id,
            ["avatar", "text:url"],
        )?)
        .await?;
    assert!(response.records.iter().any(|record| {
        record.status == crate::LookupRecordStatus::Success
            && record.value == Some(json!(LIVE_VALUE))
    }));
    assert!(response.records.iter().any(|record| {
        record.status == crate::LookupRecordStatus::NotFound && record.value.is_none()
    }));
    assert!(
        response
            .records
            .iter()
            .all(|record| record.ledger_action == LedgerAction::None)
    );
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn null_exact_resolver_offchain_lookup_stays_unsupported_and_unpersisted() -> AnyResult<()> {
    let offchain_data = encode_offchain_lookup_for_test(
        Address::from_str(UNIVERSAL_RESOLVER)?,
        vec!["https://gateway.invalid/{data}".to_owned()],
        vec![0x12, 0x34],
        [0x01, 0x02, 0x03, 0x04],
        vec![0xab],
    );
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Error {
        code: 3,
        message: "execution reverted".to_owned(),
        data: Value::String(offchain_data),
    }])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(
        response.records[0].status,
        crate::LookupRecordStatus::Unsupported
    );
    assert_eq!(
        response.records[0].unsupported_reason.as_deref(),
        Some("offchain_lookup_required")
    );
    assert_eq!(response.records[0].ledger_action, LedgerAction::SkippedCcip);
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn resolver_clear_retires_old_direct_divergence_as_stale_evidence() -> AnyResult<()> {
    let effective = "0x3000000000000000000000000000000000000003";
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(encoded_text_result(LIVE_VALUE)),
        RpcResponse::Result(encoded_text_result_with_resolver(LIVE_VALUE, effective)?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let direct = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(direct.records[0].ledger_action, LedgerAction::Written);
    make_null_resolver_discovery(&fixture).await?;

    let (active, cleared): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE cleared_at IS NULL),
                count(*) FILTER (WHERE cleared_at IS NOT NULL)
         FROM resolution_divergences",
    )
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!((active, cleared), (0, 1));

    let discovery = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(discovery.records[0].ledger_action, LedgerAction::None);
    let (active, cleared): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE cleared_at IS NULL),
                count(*) FILTER (WHERE cleared_at IS NOT NULL)
         FROM resolution_divergences",
    )
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!((active, cleared), (0, 1));

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn schema_migration_preserves_and_retires_populated_null_resolver_evidence() -> AnyResult<()>
{
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let direct = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(direct.records[0].ledger_action, LedgerAction::Written);
    sqlx::raw_sql(
        "DROP TRIGGER name_current_retire_null_resolver_divergences ON name_current;
         DROP FUNCTION retire_direct_divergences_for_null_resolver();",
    )
    .execute(fixture.pool())
    .await?;
    make_null_resolver_discovery(&fixture).await?;
    assert_eq!(ledger_count(fixture.pool()).await?, 1);

    sqlx::raw_sql(include_str!(
        "../../../migrations/20260831120000_retire_direct_divergences_for_null_resolver.sql"
    ))
    .execute(fixture.pool())
    .await?;
    let (total, active): (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE cleared_at IS NULL)
         FROM resolution_divergences",
    )
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!((total, active), (1, 0));

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn null_exact_resolver_revalidates_name_state_without_a_comparison_row() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(
        encoded_text_result_with_resolver(
            LIVE_VALUE,
            "0x3000000000000000000000000000000000000003",
        )?,
    )])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;
    let update_pool = fixture.pool().clone();
    let logical_name_id = fixture.logical_name_id.clone();
    let result = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            sqlx::query("UPDATE name_current SET provenance = provenance || '{\"changed\":true}'")
                .execute(&update_pool)
                .await
                .expect("second session must replace the name row");
        })
        .await;
    assert_eq!(
        result.expect_err("name mutation must be stale").kind(),
        ErrorKind::ConcurrentState
    );
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn wildcard_lookup_rejects_a_concurrent_name_projection_change() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE name_current
         SET declared_summary = jsonb_set(
             jsonb_set(
                 declared_summary,
                 '{topology,wildcard}',
                 jsonb_build_object(
                     'source', jsonb_build_object('logical_name_id', 'ens:ancestor'),
                     'matched_labels', jsonb_build_array('alice')
                 )
             ),
             '{topology,resolver_path,0,logical_name_id}',
             to_jsonb('ens:ancestor'::text)
         )",
    )
    .execute(fixture.pool())
    .await?;
    sqlx::query("DELETE FROM record_inventory_current")
        .execute(fixture.pool())
        .await?;

    let pool = fixture.pool().clone();
    let update_pool = pool.clone();
    let logical_name_id = fixture.logical_name_id.clone();
    let result = lookup_engine(&pool, &rpc_url)?
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            sqlx::query("UPDATE name_current SET declared_summary = declared_summary")
                .execute(&update_pool)
                .await
                .expect("second session must replace the wildcard name row");
        })
        .await;

    let error = result.expect_err("wildcard execution must retain its exact projected topology");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(ledger_count(&pool).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn closed_surface_binding_is_not_readable() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET active_to = '2026-08-04T00:00:00Z'",
    )
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("closed surface binding must not be readable");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn event_linked_ownerless_name_loads_verified_snapshot_without_control_binding()
-> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_fixture_ownerless(&fixture).await?;

    let snapshot =
        crate::store::load_snapshot(fixture.pool(), &lookup_request(&fixture.logical_name_id)?)
            .await?;
    assert_eq!(snapshot.resolver_chain_id, ETHEREUM);
    assert_eq!(
        snapshot.indexed_answer(&RecordSelector::parse("text:url")?),
        Some(json!({"status":"success", "value":INDEXED_VALUE}))
    );
    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(response.records[0].value, Some(json!(LIVE_VALUE)));
    fixture.cleanup().await?;
    assert_hash_pinned(&join_rpc(rpc_handle).await?, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn basenames_event_linked_ownerless_name_reaches_verified_provider() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(
        encoded_basenames_text_result(LIVE_VALUE),
    )])
    .await?;
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    make_fixture_ownerless(&fixture).await?;
    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(response.records[0].value, Some(json!(LIVE_VALUE)));
    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn indexed_inventory_requires_readable_resource_lineage() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(
        INDEXED_VALUE,
    ))])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let losing_resource_id = "00000000-0000-0000-0000-000000000109";
    let losing_hash = "0x9999999999999999999999999999999999999999999999999999999999999999";

    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(response.records[0].value, Some(json!(INDEXED_VALUE)));

    sqlx::query(
        "INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, 10, '2026-08-03T00:00:00Z', 'orphaned')",
    )
    .bind(ETHEREUM)
    .bind(losing_hash)
    .execute(fixture.pool())
    .await?;
    sqlx::query(
        "INSERT INTO resources
            (resource_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1::uuid, $2, $3, 10, 'canonical')",
    )
    .bind(losing_resource_id)
    .bind(ETHEREUM)
    .bind(losing_hash)
    .execute(fixture.pool())
    .await?;

    let mut boundary: Value =
        sqlx::query_scalar("SELECT record_version_boundary FROM record_inventory_current LIMIT 1")
            .fetch_one(fixture.pool())
            .await?;
    boundary["resource_id"] = json!(losing_resource_id);
    sqlx::query(
        "UPDATE record_inventory_current
         SET resource_id = $1::uuid,
             record_version_boundary = $2",
    )
    .bind(losing_resource_id)
    .bind(&boundary)
    .execute(fixture.pool())
    .await?;
    sqlx::query(
        "UPDATE name_current
         SET declared_summary = jsonb_set(
             declared_summary,
             '{topology,version_boundaries,record_version_boundary}',
             $1
         )",
    )
    .bind(&boundary)
    .execute(fixture.pool())
    .await?;
    let row_local_state: String = sqlx::query_scalar(
        "SELECT canonicality_state::text FROM resources WHERE resource_id = $1::uuid",
    )
    .bind(losing_resource_id)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(row_local_state, "canonical");

    let error = run_lookup(&fixture, &rpc_url)
        .await
        .expect_err("orphaned inventory resource lineage must hide its projected record row");
    assert_eq!(error.kind(), ErrorKind::Unsupported);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn lookup_snapshot_requires_readable_token_lineage_lineage() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let token_lineage_id = "00000000-0000-0000-0000-000000000110";
    let losing_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    crate::store::load_snapshot(fixture.pool(), &lookup_request(&fixture.logical_name_id)?)
        .await
        .expect("winning identity anchors must load the lookup snapshot");

    sqlx::query(
        "INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, 10, '2026-08-03T00:00:00Z', 'orphaned')",
    )
    .bind(ETHEREUM)
    .bind(losing_hash)
    .execute(fixture.pool())
    .await?;
    sqlx::query(
        "INSERT INTO token_lineages
            (token_lineage_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1::uuid, $2, $3, 10, 'canonical')",
    )
    .bind(token_lineage_id)
    .bind(ETHEREUM)
    .bind(losing_hash)
    .execute(fixture.pool())
    .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = $1::uuid")
        .bind(token_lineage_id)
        .execute(fixture.pool())
        .await?;
    sqlx::query("UPDATE name_current SET token_lineage_id = $1::uuid")
        .bind(token_lineage_id)
        .execute(fixture.pool())
        .await?;

    let row_local_state: String = sqlx::query_scalar(
        "SELECT canonicality_state::text
         FROM token_lineages
         WHERE token_lineage_id = $1::uuid",
    )
    .bind(token_lineage_id)
    .fetch_one(fixture.pool())
    .await?;
    assert_eq!(row_local_state, "canonical");

    let error =
        crate::store::load_snapshot(fixture.pool(), &lookup_request(&fixture.logical_name_id)?)
            .await
            .expect_err("orphaned token lineage must hide the lookup snapshot");
    assert_eq!(error.kind(), ErrorKind::Unsupported);

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn projected_position_timestamp_must_match_canonical_lineage() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE name_current
         SET chain_positions = jsonb_set(
             chain_positions,
             '{ethereum,timestamp}',
             to_jsonb('2026-08-03T00:00:01Z'::text)
         )",
    )
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("projected position timestamp must identify canonical lineage");
    assert_eq!(error.kind(), ErrorKind::Stale);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn record_lookup_missing_selected_state_is_stale() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Error {
        code: -32000,
        message: "missing state".to_owned(),
        data: Value::Null,
    }])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;

    let error = run_lookup(&fixture, &rpc_url)
        .await
        .expect_err("missing selected state must be stale");
    assert_eq!(error.kind(), ErrorKind::Stale);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn record_lookup_transport_failure_is_not_stale() -> AnyResult<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;

    let error = run_lookup(&fixture, &rpc_url)
        .await
        .expect_err("provider transport failure must abort the lookup");
    assert_eq!(error.kind(), ErrorKind::Transport);
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn row_unchanged_guard_rejects_two_session_projection_modification() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let engine = lookup_engine(&pool, &rpc_url)?;
    let logical_name_id = fixture.logical_name_id.clone();
    let update_pool = pool.clone();
    let result = engine
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            sqlx::query(
                "UPDATE record_inventory_current
                 SET entries = jsonb_set(entries, '{0,value,value}', '\"https://concurrent.example\"')",
            )
            .execute(&update_pool)
            .await
            .expect("second session must update the compared projection row");
        })
        .await;

    let error = result.expect_err("stale projection token must reject the ledger write");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(ledger_count(&pool).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn invalidated_project_generation_is_stale_before_rpc() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = 'manifest-authority:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:test-invalidation'
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(ETHEREUM)
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("invalidated projected authority must fail before provider execution");
    assert_eq!(error.kind(), ErrorKind::Stale);
    assert_eq!(ledger_count(fixture.pool()).await?, 0);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn project_generation_change_during_rpc_rejects_the_lookup() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let update_pool = pool.clone();
    let logical_name_id = fixture.logical_name_id.clone();
    let result = lookup_engine(&pool, &rpc_url)?
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            sqlx::query(
                "UPDATE chain_phase_state
                 SET input_content_hash = 'manifest-authority:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:test-invalidation'
                 WHERE chain_id = $1 AND phase_name = 'project'",
            )
            .bind(ETHEREUM)
            .execute(&update_pool)
            .await
            .expect("second session must invalidate the project generation");
        })
        .await;

    let error = result.expect_err("lookup must reject a replaced project generation");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(ledger_count(&pool).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn null_exact_resolver_rejects_project_generation_change_during_rpc() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;
    let pool = fixture.pool().clone();
    let update_pool = pool.clone();
    let logical_name_id = fixture.logical_name_id.clone();
    let result = lookup_engine(&pool, &rpc_url)?
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            sqlx::query(
                "UPDATE chain_phase_state
                 SET input_content_hash = 'manifest-authority:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:test-invalidation'
                 WHERE chain_id = $1 AND phase_name = 'project'",
            )
            .bind(ETHEREUM)
            .execute(&update_pool)
            .await
            .expect("second session must invalidate the project generation");
        })
        .await;

    let error = result.expect_err("lookup must reject a replaced project generation");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(ledger_count(&pool).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn manifest_declaration_change_during_rpc_rejects_the_lookup() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let update_pool = pool.clone();
    let logical_name_id = fixture.logical_name_id.clone();
    let result = lookup_engine(&pool, &rpc_url)?
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            sqlx::query(
                "UPDATE manifest_contract_instances
                 SET declared_address = declared_address
                 WHERE role = 'universal_resolver'",
            )
            .execute(&update_pool)
            .await
            .expect("second session must replace the selected manifest declaration");
        })
        .await;

    let error = result.expect_err("lookup must retain the exact selected manifest declaration");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(ledger_count(&pool).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn null_exact_resolver_rejects_manifest_change_during_rpc() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;
    let pool = fixture.pool().clone();
    let update_pool = pool.clone();
    let logical_name_id = fixture.logical_name_id.clone();
    let result = lookup_engine(&pool, &rpc_url)?
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            sqlx::query(
                "UPDATE manifest_contract_instances
                 SET declared_address = declared_address
                 WHERE role = 'universal_resolver'",
            )
            .execute(&update_pool)
            .await
            .expect("second session must replace the selected manifest declaration");
        })
        .await;

    let error = result.expect_err("lookup must retain the exact selected manifest declaration");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(ledger_count(&pool).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn shadow_manifest_sync_is_serialized_with_lookup_revalidation() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE manifest_versions
         SET rollout_status = 'shadow',
             manifest_payload = jsonb_set(
                 manifest_payload,
                 '{capability_flags,verified_resolution,status}',
                 to_jsonb('shadow'::text)
             )
         WHERE source_family = 'ens_execution'",
    )
    .execute(fixture.pool())
    .await?;

    let pool = fixture.pool().clone();
    let sync_pool = pool.clone();
    let (manifest_changed_tx, manifest_changed_rx) = oneshot::channel();
    let sync = tokio::spawn(async move {
        let mut transaction = sync_pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(SCHEMA_V2_MANIFEST_SYNC_LOCK)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "UPDATE manifest_contract_instances
             SET declared_address = declared_address
             WHERE role = 'universal_resolver'",
        )
        .execute(&mut *transaction)
        .await?;
        let _ = manifest_changed_tx.send(());
        tokio::time::sleep(Duration::from_millis(250)).await;
        transaction.commit().await?;
        Ok::<(), anyhow::Error>(())
    });

    let logical_name_id = fixture.logical_name_id.clone();
    let result = lookup_engine(&pool, &rpc_url)?
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            manifest_changed_rx
                .await
                .expect("manifest sync must update the shadow declaration");
        })
        .await;
    sync.await.context("manifest sync task was cancelled")??;

    let error = result.expect_err("lookup must not commit across shadow manifest sync");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(
        error.message(),
        "lookup manifest authority changed while live lookup was running"
    );
    assert_eq!(ledger_count(&pool).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn projection_publication_lock_order_does_not_deadlock_lookup() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let publish_pool = pool.clone();
    let (name_locked_tx, name_locked_rx) = oneshot::channel();
    let publisher = tokio::spawn(async move {
        let mut transaction = publish_pool.begin().await?;
        sqlx::query("UPDATE name_current SET declared_summary = declared_summary")
            .execute(&mut *transaction)
            .await?;
        let _ = name_locked_tx.send(());
        tokio::time::sleep(Duration::from_millis(250)).await;
        sqlx::query("UPDATE record_inventory_current SET entries = entries")
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok::<(), anyhow::Error>(())
    });

    let logical_name_id = fixture.logical_name_id.clone();
    let result = lookup_engine(&pool, &rpc_url)?
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            name_locked_rx
                .await
                .expect("publication session must lock name_current");
        })
        .await;
    publisher
        .await
        .context("publication task was cancelled")??;

    let error = result.expect_err("same-height publication must replace the name-row token");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(
        error.message(),
        "projected name state changed while live lookup was running"
    );
    assert_eq!(ledger_count(&pool).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn head_change_between_read_and_commit_rejects_the_lookup() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let logical_name_id = fixture.logical_name_id.clone();
    let head_pool = pool.clone();
    let result = lookup_engine(&pool, &rpc_url)?
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            advance_head(&head_pool)
                .await
                .expect("second session must advance the readable head");
        })
        .await;

    let error = result.expect_err("lookup must revalidate the execution head through commit");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(ledger_count(&pool).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn null_exact_resolver_rejects_head_change_between_read_and_commit() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    make_null_resolver_discovery(&fixture).await?;
    let pool = fixture.pool().clone();
    let logical_name_id = fixture.logical_name_id.clone();
    let head_pool = pool.clone();
    let result = lookup_engine(&pool, &rpc_url)?
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            advance_head(&head_pool)
                .await
                .expect("second session must advance the readable head");
        })
        .await;

    let error = result.expect_err("lookup must revalidate the execution head through commit");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(ledger_count(&pool).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn reorg_between_read_and_write_rejects_and_leaves_no_active_row() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let engine = lookup_engine(&pool, &rpc_url)?;
    let logical_name_id = fixture.logical_name_id.clone();
    let reorg_pool = pool.clone();
    let result = engine
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            let mut transaction = reorg_pool.begin().await.expect("reorg session must begin");
            sqlx::query("DELETE FROM chain_heads WHERE chain_id = $1")
                .bind(ETHEREUM)
                .execute(&mut *transaction)
                .await
                .expect("reorg session must detach the old head");
            sqlx::query(
                "UPDATE chain_lineage SET canonicality_state = 'orphaned'
                 WHERE chain_id = $1 AND block_hash = $2",
            )
            .bind(ETHEREUM)
            .bind(ETHEREUM_HASH)
            .execute(&mut *transaction)
            .await
            .expect("reorg session must orphan the compared block");
            transaction
                .commit()
                .await
                .expect("reorg session must commit");
        })
        .await;

    let error = result.expect_err("orphaned lookup anchor must reject the ledger write");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(ledger_count(&pool).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn reorged_agreement_does_not_clear_an_older_active_divergence() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(
        INDEXED_VALUE,
    ))])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    sqlx::query(
        "INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, 9, '2026-08-02T23:59:59Z', 'canonical')",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_PRIOR_HASH)
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO resolution_divergences (
            logical_name_id,
            resolver_chain_id,
            resolver_address,
            request_kind,
            observed_positions,
            indexed_result,
            live_result
        ) VALUES (
            $1,
            $2,
            '0x1000000000000000000000000000000000000001',
            'text:url',
            $3,
            '{"status":"success","value":"https://older-indexed.example"}'::jsonb,
            '{"status":"success","value":"https://older-live.example"}'::jsonb
        )
        "#,
    )
    .bind(&fixture.logical_name_id)
    .bind(ETHEREUM)
    .bind(json!({
        "ethereum": {
            "chain_id": ETHEREUM,
            "block_number": 9,
            "block_hash": ETHEREUM_PRIOR_HASH,
            "timestamp": "2026-08-02T23:59:59Z",
        }
    }))
    .execute(&pool)
    .await?;

    let engine = lookup_engine(&pool, &rpc_url)?;
    let logical_name_id = fixture.logical_name_id.clone();
    let reorg_pool = pool.clone();
    let result = engine
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            let mut transaction = reorg_pool.begin().await.expect("reorg session must begin");
            sqlx::query("DELETE FROM chain_heads WHERE chain_id = $1")
                .bind(ETHEREUM)
                .execute(&mut *transaction)
                .await
                .expect("reorg session must detach the old head");
            sqlx::query(
                "UPDATE chain_lineage SET canonicality_state = 'orphaned'
                 WHERE chain_id = $1 AND block_hash = $2",
            )
            .bind(ETHEREUM)
            .bind(ETHEREUM_HASH)
            .execute(&mut *transaction)
            .await
            .expect("reorg session must orphan the agreement observation");
            transaction
                .commit()
                .await
                .expect("reorg session must commit");
        })
        .await;

    let error = result.expect_err("orphaned agreement must not clear an older divergence");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    let active_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM resolution_divergences WHERE cleared_at IS NULL")
            .fetch_one(&pool)
            .await?;
    assert_eq!(active_count, 1);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn successful_ccip_result_is_never_persisted() -> AnyResult<()> {
    let (gateway_url, gateway_handle) = spawn_gateway(vec![0xca, 0xfe]).await?;
    let sender = Address::from_str(BASE_L1_RESOLVER)?;
    let offchain_data = encode_offchain_lookup_for_test(
        sender,
        vec![gateway_url],
        vec![0x12, 0x34],
        [0x01, 0x02, 0x03, 0x04],
        vec![0xab],
    );
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Error {
            code: 3,
            message: "execution reverted".to_owned(),
            data: Value::String(offchain_data),
        },
        RpcResponse::Result(encoded_basenames_text_result(LIVE_VALUE)),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let result = run_lookup(&fixture, &rpc_url).await?;

    assert_eq!(result.records[0].value, Some(json!(LIVE_VALUE)));
    assert!(result.records[0].ccip_read);
    assert_eq!(result.records[0].ledger_action, LedgerAction::SkippedCcip);
    assert_eq!(ledger_count(&pool).await?, 0);
    assert_eq!(result.observed_positions["base"]["block_hash"], BASE_HASH);
    assert_eq!(
        result.observed_positions["ethereum"]["block_hash"],
        ETHEREUM_HASH
    );

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(requests.len(), 2);
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    gateway_handle
        .await
        .context("gateway task was cancelled")??;
    Ok(())
}

#[tokio::test]
async fn mixed_ccip_and_direct_batch_persists_only_the_direct_disagreement() -> AnyResult<()> {
    let (gateway_url, gateway_handle) = spawn_gateway(vec![0xca, 0xfe]).await?;
    let offchain_data = encode_offchain_lookup_for_test(
        Address::from_str(BASE_L1_RESOLVER)?,
        vec![gateway_url],
        vec![0x12, 0x34],
        [0x01, 0x02, 0x03, 0x04],
        vec![0xab],
    );
    let (rpc_url, rpc_handle) = spawn_mixed_ccip_rpc(offchain_data).await?;
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    let request = LookupRequest::new(&fixture.logical_name_id, ["avatar", "text:url"])?;

    let response = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup(request)
        .await?;
    let avatar = response
        .records
        .iter()
        .find(|record| record.record_key == "avatar")
        .context("mixed lookup omitted avatar")?;
    assert!(avatar.ccip_read);
    assert_eq!(avatar.ledger_action, LedgerAction::SkippedCcip);
    let url = response
        .records
        .iter()
        .find(|record| record.record_key == "text:url")
        .context("mixed lookup omitted text:url")?;
    assert!(!url.ccip_read);
    assert_eq!(url.value, Some(json!(LIVE_VALUE)));
    assert_eq!(url.ledger_action, LedgerAction::Written);
    let request_kinds: Vec<String> = sqlx::query_scalar(
        "SELECT request_kind FROM resolution_divergences WHERE cleared_at IS NULL",
    )
    .fetch_all(fixture.pool())
    .await?;
    assert_eq!(request_kinds, vec!["text:url"]);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(requests.len(), 3);
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    gateway_handle
        .await
        .context("gateway task was cancelled")??;
    Ok(())
}

#[tokio::test]
async fn ccip_result_rejects_a_concurrent_inventory_change() -> AnyResult<()> {
    let (gateway_url, gateway_handle) = spawn_gateway(vec![0xca, 0xfe]).await?;
    let sender = Address::from_str(BASE_L1_RESOLVER)?;
    let offchain_data = encode_offchain_lookup_for_test(
        sender,
        vec![gateway_url],
        vec![0x12, 0x34],
        [0x01, 0x02, 0x03, 0x04],
        vec![0xab],
    );
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Error {
            code: 3,
            message: "execution reverted".to_owned(),
            data: Value::String(offchain_data),
        },
        RpcResponse::Result(encoded_basenames_text_result(LIVE_VALUE)),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let engine = lookup_engine(&pool, &rpc_url)?;
    let logical_name_id = fixture.logical_name_id.clone();
    let update_pool = pool.clone();
    let result = engine
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            sqlx::query(
                "UPDATE record_inventory_current
                 SET entries = jsonb_set(
                     entries,
                     '{0,value,value}',
                     '\"https://concurrent.example\"'
                 )",
            )
            .execute(&update_pool)
            .await
            .expect("second session must update the projection row");
        })
        .await;

    let error = result.expect_err("CCIP execution must retain its indexed serving snapshot");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(ledger_count(&pool).await?, 0);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    gateway_handle
        .await
        .context("gateway task was cancelled")??;
    Ok(())
}

#[tokio::test]
async fn basenames_uses_projected_auxiliary_execution_position() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(
        encoded_basenames_text_result(INDEXED_VALUE),
    )])
    .await?;
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    sqlx::query(
        "INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, 11, '2026-08-03T00:00:01Z', 'canonical')",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_LATER_HASH)
    .execute(fixture.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_heads
         SET latest_block_hash = $2, latest_block_number = 11
         WHERE chain_id = $1",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_LATER_HASH)
    .execute(fixture.pool())
    .await?;

    let response = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup_at_positions(
            lookup_request(&fixture.logical_name_id)?,
            &[
                LookupPosition {
                    chain_id: BASE.to_owned(),
                    block_number: 10,
                    block_hash: BASE_HASH.to_owned(),
                    timestamp: "2026-08-03T00:00:00Z".to_owned(),
                },
                LookupPosition {
                    chain_id: ETHEREUM.to_owned(),
                    block_number: 11,
                    block_hash: ETHEREUM_LATER_HASH.to_owned(),
                    timestamp: "2026-08-03T00:00:01Z".to_owned(),
                },
            ],
        )
        .await?;
    assert_eq!(
        response.observed_positions["ethereum"]["block_hash"],
        ETHEREUM_HASH
    );
    assert_eq!(response.execution_position.block_hash, ETHEREUM_HASH);
    assert_eq!(response.execution_position.block_number, 10);
    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn basenames_rejects_newer_or_same_height_incompatible_execution_positions_before_rpc()
-> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    for (block_number, block_hash) in [(9, ETHEREUM_PRIOR_HASH), (10, ETHEREUM_PRIOR_HASH)] {
        let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
            .lookup_at_positions(
                lookup_request(&fixture.logical_name_id)?,
                &[
                    LookupPosition {
                        chain_id: BASE.to_owned(),
                        block_number: 10,
                        block_hash: BASE_HASH.to_owned(),
                        timestamp: "2026-08-03T00:00:00Z".to_owned(),
                    },
                    LookupPosition {
                        chain_id: ETHEREUM.to_owned(),
                        block_number,
                        block_hash: block_hash.to_owned(),
                        timestamp: "2026-08-03T00:00:00Z".to_owned(),
                    },
                ],
            )
            .await
            .expect_err("an execution position outside the admitted snapshot must be stale");
        assert_eq!(error.kind(), ErrorKind::Stale);
        assert_eq!(
            error.message(),
            "lookup execution position is not compatible with the caller's admitted snapshot"
        );
    }
    assert_eq!(ledger_count(fixture.pool()).await?, 0);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn basenames_shadow_execution_manifest_is_not_authority() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE manifest_versions
         SET rollout_status = 'shadow',
             manifest_payload = jsonb_set(
                 manifest_payload,
                 '{capability_flags,verified_resolution,status}',
                 to_jsonb('shadow'::text)
             )
         WHERE source_family = 'basenames_execution'",
    )
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("shadow Basenames execution manifest must not execute");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn basenames_v1_execution_manifest_is_not_authority() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE manifest_versions
         SET manifest_version = 1
         WHERE source_family = 'basenames_execution'",
    )
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("Basenames execution manifest v1 must not execute");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn manifest_entrypoint_prefers_the_highest_started_role_declaration() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(
        INDEXED_VALUE,
    ))])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions WHERE source_family = 'ens_execution'",
    )
    .fetch_one(fixture.pool())
    .await?;
    sqlx::query(
        "UPDATE manifest_contract_instances
         SET start_block_number = 1
         WHERE manifest_id = $1 AND role = 'universal_resolver'",
    )
    .bind(manifest_id)
    .execute(fixture.pool())
    .await?;
    sqlx::query(
        "INSERT INTO contract_instances
            (contract_instance_id, chain_id, contract_kind)
         VALUES ('00000000-0000-0000-0000-000000000105'::uuid, $1, 'contract')",
    )
    .bind(ETHEREUM)
    .execute(fixture.pool())
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances
            (manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind,
             start_block_number)
         VALUES ($1, $2, 'contract', 'replacement_universal_resolver',
                 '00000000-0000-0000-0000-000000000105'::uuid, $3,
                 'universal_resolver', 'none', 9)",
    )
    .bind(manifest_id)
    .bind(ETHEREUM)
    .bind(REPLACEMENT_UNIVERSAL_RESOLVER)
    .execute(fixture.pool())
    .await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(response.entrypoint_address, REPLACEMENT_UNIVERSAL_RESOLVER);
    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(
        requests[0]["params"][0]["to"],
        REPLACEMENT_UNIVERSAL_RESOLVER
    );
    Ok(())
}

#[tokio::test]
async fn ens_execution_manifest_without_resolution_capability_is_not_authority() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE manifest_versions
         SET rollout_status = 'shadow',
             manifest_payload = '{\"capability_flags\": {}}'::jsonb
         WHERE source_family = 'ens_execution'",
    )
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("ENS execution without an explicit capability must not execute");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn unsupported_active_ens_manifest_does_not_fall_back_to_shadow() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE manifest_versions
         SET rollout_status = 'shadow',
             manifest_payload = jsonb_set(
                 manifest_payload,
                 '{capability_flags,verified_resolution,status}',
                 to_jsonb('shadow'::text)
             )
         WHERE source_family = 'ens_execution'",
    )
    .execute(fixture.pool())
    .await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (2, $1, 'ens_execution', $2, 'test', 'active', 'test',
                 'test/ens/ens_execution-v2.toml', '{\"capability_flags\": {}}'::jsonb)
         RETURNING manifest_id",
    )
    .bind(ENS_NAMESPACE)
    .bind(ETHEREUM)
    .fetch_one(fixture.pool())
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances
            (manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind)
         VALUES ($1, $2, 'contract', 'universal_resolver', $3::uuid, $4,
                 'universal_resolver', 'none')",
    )
    .bind(manifest_id)
    .bind(ETHEREUM)
    .bind("00000000-0000-0000-0000-000000000103")
    .bind(UNIVERSAL_RESOLVER)
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("an active manifest without the capability must not fall back to shadow");
    assert_eq!(error.kind(), ErrorKind::Unsupported);

    sqlx::query(
        "UPDATE manifest_versions
         SET manifest_payload = jsonb_build_object(
                 'capability_flags', jsonb_build_object(
                     'verified_resolution', jsonb_build_object('status', 'supported')
                 )
             )
         WHERE manifest_id = $1",
    )
    .bind(manifest_id)
    .execute(fixture.pool())
    .await?;
    sqlx::query(
        "UPDATE manifest_contract_instances
         SET role = 'replacement_resolver'
         WHERE manifest_id = $1",
    )
    .bind(manifest_id)
    .execute(fixture.pool())
    .await?;
    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("an active manifest without the role must not fall back to shadow");
    assert_eq!(error.kind(), ErrorKind::Unsupported);

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn primary_name_lookup_uses_manifest_entrypoints_and_readable_head() -> AnyResult<()> {
    let target = "0x8e8db5ccef88cca9d624701db544989c996e3216";
    let reverse_resolver = "0xa2c122be93b0074270ebee7f6b7292c7deb45047";
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(Value::String(hex_string(
            &Address::from_str(reverse_resolver)?.abi_encode(),
        ))),
        RpcResponse::Result(Value::String(hex_string(&"alice.eth".abi_encode()))),
        RpcResponse::Result(encoded_address_result(target)?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;
    sqlx::query(
        "UPDATE manifest_versions
         SET manifest_payload = '{}'::jsonb
         WHERE source_family = 'ens_v1_registry_l1'",
    )
    .execute(fixture.pool())
    .await?;
    let result = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup_ens_primary_name(target)
        .await?;

    assert_eq!(result.status, EnsPrimaryNameStatus::Success);
    assert_eq!(result.name.as_deref(), Some("alice.eth"));
    assert_eq!(result.normalized_name.as_deref(), Some("alice.eth"));
    assert_eq!(result.forward_address.as_deref(), Some(target));
    assert_eq!(
        result.reverse_resolver_address.as_deref(),
        Some(reverse_resolver)
    );
    assert!(!result.ccip_read);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(requests[0]["params"][0]["to"], ENS_REGISTRY);
    assert_eq!(requests[1]["params"][0]["to"], reverse_resolver);
    assert_eq!(requests[2]["params"][0]["to"], UNIVERSAL_RESOLVER);
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn primary_name_revalidates_its_position_after_live_calls() -> AnyResult<()> {
    let target = "0x8e8db5ccef88cca9d624701db544989c996e3216";
    let reverse_resolver = "0xa2c122be93b0074270ebee7f6b7292c7deb45047";
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(Value::String(hex_string(
            &Address::from_str(reverse_resolver)?.abi_encode(),
        ))),
        RpcResponse::Result(Value::String(hex_string(&"alice.eth".abi_encode()))),
        RpcResponse::Result(encoded_address_result(target)?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;
    let pool = fixture.pool().clone();
    let update_pool = pool.clone();
    let result = lookup_engine(&pool, &rpc_url)?
        .lookup_ens_primary_name_with_before_revalidate(target, move || async move {
            advance_head(&update_pool)
                .await
                .expect("second session must advance the readable head");
        })
        .await;

    let error = result.expect_err("primary-name lookup must reject its replaced readable head");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn primary_name_rejects_a_project_generation_change_after_live_calls() -> AnyResult<()> {
    let target = "0x8e8db5ccef88cca9d624701db544989c996e3216";
    let reverse_resolver = "0xa2c122be93b0074270ebee7f6b7292c7deb45047";
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(Value::String(hex_string(
            &Address::from_str(reverse_resolver)?.abi_encode(),
        ))),
        RpcResponse::Result(Value::String(hex_string(&"alice.eth".abi_encode()))),
        RpcResponse::Result(encoded_address_result(target)?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;
    let pool = fixture.pool().clone();
    let update_pool = pool.clone();
    let result = lookup_engine(&pool, &rpc_url)?
        .lookup_ens_primary_name_with_before_revalidate(target, move || async move {
            sqlx::query(
                "UPDATE chain_phase_state
                 SET input_content_hash = 'manifest-authority:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:test-invalidation'
                 WHERE chain_id = $1 AND phase_name = 'project'",
            )
            .bind(ETHEREUM)
            .execute(&update_pool)
            .await
            .expect("second session must invalidate primary-name authority");
        })
        .await;

    let error = result.expect_err("primary-name lookup must retain its project generation");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn primary_name_missing_forward_address_is_not_found() -> AnyResult<()> {
    let target = "0x8e8db5ccef88cca9d624701db544989c996e3216";
    let reverse_resolver = "0xa2c122be93b0074270ebee7f6b7292c7deb45047";
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(Value::String(hex_string(
            &Address::from_str(reverse_resolver)?.abi_encode(),
        ))),
        RpcResponse::Result(Value::String(hex_string(&"alice.eth".abi_encode()))),
        RpcResponse::Result(encoded_address_result(
            "0x0000000000000000000000000000000000000000",
        )?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;

    let result = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup_ens_primary_name(target)
        .await?;
    assert_eq!(result.status, EnsPrimaryNameStatus::NotFound);
    assert_eq!(result.forward_address, None);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn primary_name_does_not_reclassify_resolver_not_found() -> AnyResult<()> {
    let target = "0x8e8db5ccef88cca9d624701db544989c996e3216";
    let reverse_resolver = "0xa2c122be93b0074270ebee7f6b7292c7deb45047";
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(Value::String(hex_string(
            &Address::from_str(reverse_resolver)?.abi_encode(),
        ))),
        RpcResponse::Result(Value::String(hex_string(&"alice.eth".abi_encode()))),
        RpcResponse::Error {
            code: 3,
            message: "execution reverted".to_owned(),
            data: Value::String(resolver_not_found_revert(b"\x05alice\x03eth\0")),
        },
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;

    let result = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup_ens_primary_name(target)
        .await?;
    assert_eq!(result.status, EnsPrimaryNameStatus::ExecutionFailed);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn primary_name_selected_block_error_is_stale() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Error {
        code: -32000,
        message: "header not found".to_owned(),
        data: Value::Null,
    }])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;

    let error = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup_ens_primary_name("0x8e8db5ccef88cca9d624701db544989c996e3216")
        .await
        .expect_err("unavailable selected block must be stale");
    assert_eq!(error.kind(), ErrorKind::Stale);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn primary_name_missing_selected_state_is_stale() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Error {
        code: -32000,
        message: "missing state".to_owned(),
        data: Value::Null,
    }])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;

    let error = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup_ens_primary_name("0x8e8db5ccef88cca9d624701db544989c996e3216")
        .await
        .expect_err("missing selected state must be stale");
    assert_eq!(error.kind(), ErrorKind::Stale);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn primary_name_configured_response_timeout_is_in_band() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_hanging_rpc().await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;
    let rpc_urls = ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={rpc_url}")])?
        .with_http_timeouts(Duration::from_millis(50), Duration::from_millis(150))?;

    let result = LookupEngine::new(fixture.pool().clone(), rpc_urls)
        .lookup_ens_primary_name("0x8e8db5ccef88cca9d624701db544989c996e3216")
        .await?;
    assert_eq!(result.status, EnsPrimaryNameStatus::ExecutionFailed);
    assert_eq!(
        result.failure_reason.as_deref(),
        Some("resolver_call_failed")
    );

    rpc_handle.abort();
    fixture.cleanup().await?;
    Ok(())
}

async fn finish_fixture(
    fixture: Fixture,
    result: crate::Result<LookupResponse>,
) -> AnyResult<LookupResponse> {
    let count = ledger_count(fixture.pool()).await?;
    fixture.cleanup().await?;
    let response = result?;
    assert_eq!(count, 0);
    Ok(response)
}

fn lookup_engine(pool: &PgPool, rpc_url: &str) -> AnyResult<LookupEngine> {
    let rpc_urls = ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={rpc_url}")])?;
    Ok(LookupEngine::new(pool.clone(), rpc_urls))
}

async fn run_lookup(fixture: &Fixture, rpc_url: &str) -> crate::Result<LookupResponse> {
    lookup_engine(fixture.pool(), rpc_url)
        .map_err(|error| crate::LookupError::configuration(error.to_string()))?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
}

fn lookup_request(logical_name_id: &str) -> crate::Result<LookupRequest> {
    LookupRequest::new(logical_name_id, ["text:url"])
}

async fn make_fixture_ownerless(fixture: &Fixture) -> AnyResult<()> {
    sqlx::query(
        "UPDATE name_current
         SET serving_resource_id = resource_id,
             surface_binding_id = NULL,
             resource_id = NULL,
             token_lineage_id = NULL,
             binding_kind = NULL",
    )
    .execute(fixture.pool())
    .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = NULL")
        .execute(fixture.pool())
        .await?;
    Ok(())
}

async fn ledger_count(pool: &PgPool) -> AnyResult<i64> {
    sqlx::query_scalar("SELECT count(*) FROM resolution_divergences")
        .fetch_one(pool)
        .await
        .context("failed to count resolution divergences")
}

async fn make_null_resolver_discovery(fixture: &Fixture) -> AnyResult<()> {
    sqlx::query(
        r#"
        UPDATE name_current
        SET declared_summary = jsonb_set(
                declared_summary #- '{topology}',
                '{resolver}',
                '{"chain_id":null,"address":null,"latest_event_kind":"ResolverChanged"}'::jsonb
            )
        WHERE logical_name_id = $1
        "#,
    )
    .bind(&fixture.logical_name_id)
    .execute(fixture.pool())
    .await?;
    sqlx::query("DELETE FROM record_inventory_current")
        .execute(fixture.pool())
        .await?;
    Ok(())
}

async fn setup_fixture(kind: FixtureKind, indexed_value: &str) -> AnyResult<Fixture> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("bigname_lookup").pool_max_connections(6))
            .await?;
    apply_baseline(database.pool()).await?;
    let (namespace, name, resolver_chain, resolver_hash, entrypoint, role, source_family) =
        match kind {
            FixtureKind::Ens => (
                ENS_NAMESPACE,
                "alice.eth",
                ETHEREUM,
                ETHEREUM_HASH,
                UNIVERSAL_RESOLVER,
                "universal_resolver",
                "ens_execution",
            ),
            FixtureKind::Basenames => (
                BASENAMES_NAMESPACE,
                "alice.base.eth",
                BASE,
                BASE_HASH,
                BASE_L1_RESOLVER,
                "l1_resolver",
                "basenames_execution",
            ),
        };
    seed_heads(database.pool(), kind).await?;
    seed_manifest(
        database.pool(),
        namespace,
        source_family,
        role,
        entrypoint,
        "00000000-0000-0000-0000-000000000103",
    )
    .await?;
    let namehash = hex_string(&namehash(name)?);
    let logical_name_id = format!("{namespace}:{namehash}");
    let dns_name = dns_encode_name(name)?;
    let resource_id = "00000000-0000-0000-0000-000000000101";
    let binding_id = "00000000-0000-0000-0000-000000000102";
    let resolver_address = "0x1000000000000000000000000000000000000001";
    let inventory_positions = json!({
        "target_block_number": 10,
        "target_block_hash": resolver_hash,
    });
    let resolver_slot = if resolver_chain == ETHEREUM {
        "ethereum"
    } else {
        "base"
    };
    let mut name_positions = json!({
        (resolver_slot): {
            "chain_id": resolver_chain,
            "block_number": 10,
            "block_hash": resolver_hash,
            "timestamp": "2026-08-03T00:00:00Z",
        },
    });
    if matches!(kind, FixtureKind::Basenames) {
        name_positions["ethereum"] = json!({
            "chain_id": ETHEREUM,
            "block_number": 10,
            "block_hash": ETHEREUM_HASH,
            "timestamp": "2026-08-03T00:00:00Z",
        });
    }
    let boundary = json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id,
        "normalized_event_id": 1,
        "event_kind": "ResolverChanged",
        "chain_position": {
            "chain_id": resolver_chain,
            "block_number": 10,
            "block_hash": resolver_hash,
            "timestamp": "2026-08-03T00:00:00Z",
        },
    });
    let transport = match kind {
        FixtureKind::Ens => json!({
            "source_chain_id": null,
            "target_chain_id": null,
            "contract_address": null,
            "latest_event_kind": null,
        }),
        FixtureKind::Basenames => json!({
            "source_chain_id": BASE,
            "target_chain_id": ETHEREUM,
            "contract_address": entrypoint,
            "latest_event_kind": "ResolverChanged",
        }),
    };
    let topology = json!({
        "registry_path": [],
        "subregistry_path": [],
        "resolver_path": [{
            "logical_name_id": logical_name_id,
            "resource_id": resource_id,
            "chain_id": resolver_chain,
            "address": resolver_address,
        }],
        "wildcard": { "source": null, "matched_labels": [] },
        "alias": { "final_target": null, "hops": [] },
        "version_boundaries": { "record_version_boundary": boundary },
        "transport": transport,
    });
    seed_identity_and_projection(
        database.pool(),
        namespace,
        name,
        &logical_name_id,
        &namehash,
        &dns_name,
        resolver_chain,
        resolver_hash,
        resource_id,
        binding_id,
        &topology,
        &boundary,
        &name_positions,
        &inventory_positions,
        indexed_value,
    )
    .await?;

    Ok(Fixture {
        database,
        logical_name_id,
    })
}

async fn apply_baseline(pool: &PgPool) -> AnyResult<()> {
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await?;
    let mut transaction = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *transaction)
        .await?;
    raw_sql(&format!(
        "ALTER DATABASE {} SET search_path TO bigname_phase, public",
        quote_identifier(&database_name)
    ))
    .execute(&mut *transaction)
    .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await?;
    for script in [
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;

    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        connections.push(pool.acquire().await?);
    }
    for connection in &mut connections {
        sqlx::query("SET search_path TO bigname_phase, public")
            .execute(&mut **connection)
            .await?;
    }
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!(r#""{}""#, identifier.replace('"', r#""""#))
}

async fn seed_heads(pool: &PgPool, kind: FixtureKind) -> AnyResult<()> {
    sqlx::query(
        "INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, 10, '2026-08-03T00:00:00Z', 'canonical')",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, 10)",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_HASH)
    .execute(pool)
    .await?;
    seed_project_state(pool, ETHEREUM, ETHEREUM_HASH).await?;
    if matches!(kind, FixtureKind::Basenames) {
        sqlx::query(
            "INSERT INTO chain_lineage
                (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
             VALUES ($1, $2, 10, '2026-08-03T00:00:00Z', 'canonical')",
        )
        .bind(BASE)
        .bind(BASE_HASH)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
             VALUES ($1, $2, 10)",
        )
        .bind(BASE)
        .bind(BASE_HASH)
        .execute(pool)
        .await?;
        seed_project_state(pool, BASE, BASE_HASH).await?;
    }
    Ok(())
}

async fn seed_project_state(pool: &PgPool, chain_id: &str, block_hash: &str) -> AnyResult<()> {
    sqlx::query(
        "INSERT INTO chain_phase_state
            (chain_id, phase_name, phase_status, current_block_number, current_block_hash,
             target_block_number, target_block_hash, input_content_hash, started_at, finished_at)
         VALUES ($1, 'project', 'completed', 10, $2, 10, $2, $3, now(), now())",
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    Ok(())
}

async fn advance_head(pool: &PgPool) -> AnyResult<()> {
    sqlx::query(
        "INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, 11, '2026-08-03T00:00:01Z', 'canonical')",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_LATER_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE chain_heads
         SET latest_block_hash = $2, latest_block_number = 11
         WHERE chain_id = $1",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_LATER_HASH)
    .execute(pool)
    .await?;
    Ok(())
}

async fn advance_head_and_project(pool: &PgPool) -> AnyResult<()> {
    advance_head(pool).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET current_block_number = 11, current_block_hash = $2,
             target_block_number = 11, target_block_hash = $2
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_LATER_HASH)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_manifest(
    pool: &PgPool,
    namespace: &str,
    source_family: &str,
    role: &str,
    address: &str,
    contract_id: &str,
) -> AnyResult<()> {
    let manifest_version = if source_family == "basenames_execution" {
        2
    } else {
        1
    };
    sqlx::query(
        "INSERT INTO contract_instances
            (contract_instance_id, chain_id, contract_kind)
         VALUES ($1::uuid, $2, 'contract')",
    )
    .bind(contract_id)
    .bind(ETHEREUM)
    .execute(pool)
    .await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES ($5, $1, $2, $3, 'test', 'active', 'test', $4,
                 jsonb_build_object('capability_flags', jsonb_build_object(
                     'verified_resolution', jsonb_build_object('status', 'supported'))))
         RETURNING manifest_id",
    )
    .bind(namespace)
    .bind(source_family)
    .bind(ETHEREUM)
    .bind(format!("test/{namespace}/{source_family}.toml"))
    .bind(manifest_version)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances
            (manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind)
         VALUES ($1, $2, 'contract', $3, $4::uuid, $5, $3, 'none')",
    )
    .bind(manifest_id)
    .bind(ETHEREUM)
    .bind(role)
    .bind(contract_id)
    .bind(address)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn seed_identity_and_projection(
    pool: &PgPool,
    namespace: &str,
    name: &str,
    logical_name_id: &str,
    namehash: &str,
    dns_name: &[u8],
    chain_id: &str,
    block_hash: &str,
    resource_id: &str,
    binding_id: &str,
    topology: &Value,
    boundary: &Value,
    name_positions: &Value,
    inventory_positions: &Value,
    indexed_value: &str,
) -> AnyResult<()> {
    sqlx::query(
        "INSERT INTO resources
            (resource_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1::uuid, $2, $3, 10, 'canonical')",
    )
    .bind(resource_id)
    .bind(chain_id)
    .bind(block_hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces
            (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, $2, $3, ARRAY[$3], $4, $5, ARRAY[$5], 'test', 'active',
                 $6, $7, 10, 'canonical')",
    )
    .bind(logical_name_id)
    .bind(namespace)
    .bind(name)
    .bind(dns_name)
    .bind(namehash)
    .bind(chain_id)
    .bind(block_hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings
            (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path',
                 CASE WHEN $2 LIKE 'basenames:%' THEN 'basenames' ELSE 'ens_v1' END,
                 '2026-08-03T00:00:00Z', $4, $5, 10, 'canonical')",
    )
    .bind(binding_id)
    .bind(logical_name_id)
    .bind(resource_id)
    .bind(chain_id)
    .bind(block_hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_current
            (logical_name_id, namespace, raw_name, namehash, surface_binding_id,
             resource_id, binding_kind, declared_summary, support_status,
             provenance, chain_positions, canonicality_summary, manifest_version)
         VALUES ($1, $2, $3, $4, $5::uuid, $6::uuid, 'declared_registry_path',
                 jsonb_build_object('topology', $7::jsonb), 'supported', '{}', $8,
                 jsonb_build_object('state', 'canonical'), 1)",
    )
    .bind(logical_name_id)
    .bind(namespace)
    .bind(name)
    .bind(namehash)
    .bind(binding_id)
    .bind(resource_id)
    .bind(topology)
    .bind(name_positions)
    .execute(pool)
    .await?;
    let selectors = json!([{
        "record_key": "text:url",
        "record_family": "text",
        "selector_key": "url",
    }]);
    let entries = json!([{
        "record_key": "text:url",
        "record_family": "text",
        "selector_key": "url",
        "status": "success",
        "value": { "kind": "text", "value": indexed_value },
    }]);
    sqlx::query(
        "INSERT INTO record_inventory_current
            (resource_id, record_version_boundary_key, record_version_boundary,
             selectors, unsupported_families, entries, support_status, provenance,
             chain_positions, canonicality_summary, manifest_version)
         VALUES ($1::uuid, 'boundary-1', $2, $3, '[]', $4, 'supported', '{}', $5,
                 jsonb_build_object('state', 'canonical'), 1)",
    )
    .bind(resource_id)
    .bind(boundary)
    .bind(selectors)
    .bind(entries)
    .bind(inventory_positions)
    .execute(pool)
    .await?;
    Ok(())
}

fn encoded_text_result(value: &str) -> Value {
    let record_result = (value.to_owned(),).abi_encode_params();
    let universal_result = (Bytes::from(record_result), Address::ZERO).abi_encode_params();
    Value::String(hex_string(&universal_result))
}

fn encoded_text_result_with_resolver(value: &str, resolver: &str) -> AnyResult<Value> {
    let record_result = (value.to_owned(),).abi_encode_params();
    let universal_result =
        (Bytes::from(record_result), Address::from_str(resolver)?).abi_encode_params();
    Ok(Value::String(hex_string(&universal_result)))
}

fn encoded_malformed_text_result_with_resolver(resolver: &str) -> AnyResult<Value> {
    let universal_result =
        (Bytes::from(vec![0x12, 0x34]), Address::from_str(resolver)?).abi_encode_params();
    Ok(Value::String(hex_string(&universal_result)))
}

fn resolver_not_found_revert(name: &[u8]) -> String {
    let mut encoded = keccak256("ResolverNotFound(bytes)").as_slice()[..4].to_vec();
    encoded.extend((Bytes::copy_from_slice(name),).abi_encode_params());
    hex_string(&encoded)
}

fn observed_position(block_number: i64, block_hash: &str, timestamp: &str) -> Value {
    json!({
        "ethereum": {
            "chain_id": ETHEREUM,
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": timestamp,
        }
    })
}

fn encoded_basenames_text_result(value: &str) -> Value {
    let record_result = (value.to_owned(),).abi_encode_params();
    let l1_resolver_result = (Bytes::from(record_result),).abi_encode_params();
    Value::String(hex_string(&l1_resolver_result))
}

fn encoded_address_result(address: &str) -> AnyResult<Value> {
    let record_result = Address::from_str(address)?.abi_encode();
    let universal_result = (Bytes::from(record_result), Address::ZERO).abi_encode_params();
    Ok(Value::String(hex_string(&universal_result)))
}

async fn spawn_mock_rpc(
    responses: Vec<RpcResponse>,
) -> AnyResult<(String, JoinHandle<AnyResult<Vec<Value>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut requests = Vec::with_capacity(responses.len());
        for response in responses {
            let (mut socket, _) = listener.accept().await?;
            requests.push(read_http_json_body(&mut socket).await?);
            write_rpc_response(&mut socket, response).await?;
        }
        Ok(requests)
    });
    Ok((url, handle))
}

async fn spawn_peak_concurrency_rpc(
    request_count: usize,
) -> AnyResult<(String, JoinHandle<AnyResult<usize>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut tasks = tokio::task::JoinSet::<AnyResult<()>>::new();
        for _ in 0..request_count {
            let (mut socket, _) = listener.accept().await?;
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            tasks.spawn(async move {
                read_http_json_body(&mut socket).await?;
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(25)).await;
                write_rpc_response(
                    &mut socket,
                    RpcResponse::Result(encoded_text_result(LIVE_VALUE)),
                )
                .await?;
                active.fetch_sub(1, Ordering::SeqCst);
                AnyResult::Ok(())
            });
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok(peak.load(Ordering::SeqCst))
    });
    Ok((url, handle))
}

async fn spawn_mixed_ccip_rpc(
    offchain_data: String,
) -> AnyResult<(String, JoinHandle<AnyResult<Vec<Value>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut requests = Vec::with_capacity(3);
        for _ in 0..3 {
            let (mut socket, _) = listener.accept().await?;
            let request = read_http_json_body(&mut socket).await?;
            let calldata = request["params"][0]["data"].as_str().unwrap_or_default();
            let response = if calldata.contains("617661746172") {
                RpcResponse::Error {
                    code: 3,
                    message: "execution reverted".to_owned(),
                    data: Value::String(offchain_data.clone()),
                }
            } else if calldata.contains("75726c") {
                RpcResponse::Result(encoded_basenames_text_result(LIVE_VALUE))
            } else {
                RpcResponse::Result(encoded_basenames_text_result("ipfs://ccip-avatar"))
            };
            requests.push(request);
            write_rpc_response(&mut socket, response).await?;
        }
        Ok(requests)
    });
    Ok((url, handle))
}

async fn spawn_gateway(response: Vec<u8>) -> AnyResult<(String, JoinHandle<AnyResult<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        read_http_json_body(&mut socket).await?;
        let body = json!({ "data": hex_string(&response) }).to_string();
        write_http_response(&mut socket, &body).await
    });
    Ok((url, handle))
}

async fn spawn_hanging_rpc() -> AnyResult<(String, JoinHandle<AnyResult<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        read_http_json_body(&mut socket).await?;
        std::future::pending::<()>().await;
        #[allow(unreachable_code)]
        Ok(())
    });
    Ok((url, handle))
}

async fn read_http_json_body(socket: &mut TcpStream) -> AnyResult<Value> {
    let mut buffer = Vec::new();
    let mut scratch = [0_u8; 1024];
    let (body_start, content_length) = loop {
        let read = socket.read(&mut scratch).await?;
        if read == 0 {
            bail!("HTTP request closed before its headers completed");
        }
        buffer.extend_from_slice(&scratch[..read]);
        if let Some(body_start) = buffer
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|position| position + 4)
        {
            let headers = std::str::from_utf8(&buffer[..body_start])?;
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>())
                })
                .transpose()?
                .context("HTTP request omitted content-length")?;
            break (body_start, length);
        }
    };
    while buffer.len() < body_start + content_length {
        let read = socket.read(&mut scratch).await?;
        if read == 0 {
            bail!("HTTP request closed before its body completed");
        }
        buffer.extend_from_slice(&scratch[..read]);
    }
    serde_json::from_slice(&buffer[body_start..body_start + content_length])
        .context("HTTP request body was not JSON")
}

async fn write_rpc_response(socket: &mut TcpStream, response: RpcResponse) -> AnyResult<()> {
    let payload = match response {
        RpcResponse::Result(result) => json!({ "jsonrpc": "2.0", "id": 1, "result": result }),
        RpcResponse::Error {
            code,
            message,
            data,
        } => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": code, "message": message, "data": data },
        }),
    };
    write_http_response(socket, &payload.to_string()).await
}

async fn write_http_response(socket: &mut TcpStream, body: &str) -> AnyResult<()> {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket.write_all(response.as_bytes()).await?;
    Ok(())
}

async fn join_rpc(handle: JoinHandle<AnyResult<Vec<Value>>>) -> AnyResult<Vec<Value>> {
    handle.await.context("mock RPC task was cancelled")?
}

fn assert_hash_pinned(requests: &[Value], expected_hash: &str) {
    assert!(!requests.is_empty());
    for request in requests {
        assert_eq!(request["method"], "eth_call");
        assert_eq!(request["params"][1]["blockHash"], expected_hash);
        assert_eq!(request["params"][1]["requireCanonical"], true);
    }
}
