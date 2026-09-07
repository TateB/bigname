use anyhow::{Context, Result};
use bigname_domain::resolver_read::{IndexedRecordStatus, evaluate_indexed_record};
use bigname_project::{BatchOutcome, BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const RESOURCE: &str = "68200000-0000-0000-0000-000000000100";
const BINDING: &str = "68200000-0000-0000-0000-000000000101";
const REGISTRY_INSTANCE: &str = "68200000-0000-0000-0000-000000000102";
const RESOLVER_INSTANCE: &str = "68200000-0000-0000-0000-000000000103";
const NODE: &str = "0x6820000000000000000000000000000000000000000000000000000000000000";
const RESOLVER: &str = "0x6820000000000000000000000000000000000060";
const ZERO20: &str = "0x0000000000000000000000000000000000000000";
const ZERO32: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
const NONZERO20: &str = "0x1111111111111111111111111111111111111111";
const LATER20: &str = "0x2222222222222222222222222222222222222222";

#[derive(Clone, Debug)]
struct FixtureEvent {
    block: i64,
    log_index: i64,
    transaction: i64,
    after_state: Value,
}

#[derive(Clone, Debug)]
struct Case {
    id: &'static str,
    namespace: &'static str,
    chain: &'static str,
    pointer_source_family: &'static str,
    event_source_family: &'static str,
    authority_arm: &'static str,
    events: Vec<FixtureEvent>,
    expected_key: &'static str,
    expected_status: IndexedRecordStatus,
    expected_value: Option<&'static str>,
}

#[derive(Clone, Copy, Debug)]
enum Execution {
    FromZero,
    PerBlock,
    TwoByTwo,
    Idempotent,
    RedoZeroBlock,
}

#[tokio::test]
async fn default_fallback_tracks_exact_replacement_and_version_reset() -> Result<()> {
    for state in ["missing", "empty", "nonzero", "version_reset"] {
        let mut fixture = composed_fixture(true)?;
        fixture.events = vec![scalar(10, 1, 10, "AddressChanged", "2147483648", LATER20)];
        if state != "missing" {
            fixture.events.extend([
                scalar(11, 1, 11, "AddressChanged", "60", ZERO20),
                scalar(11, 2, 11, "AddrChanged", "60", ZERO20),
            ]);
        }
        if matches!(state, "empty" | "nonzero") {
            let mut empty = flat(12, "60", "0x");
            empty.transaction = 12;
            fixture
                .events
                .extend([empty, scalar(12, 2, 12, "AddrChanged", "60", ZERO20)]);
        }
        if state == "nonzero" {
            fixture
                .events
                .push(scalar(13, 1, 13, "AddrChanged", "60", NONZERO20));
        }
        if state == "version_reset" {
            fixture.events.extend([
                event(13, 0, 13, json!({"node":NODE, "record_version":"1"})),
                scalar(13, 1, 13, "AddressChanged", "2147483648", LATER20),
            ]);
        }
        fixture
            .events
            .push(scalar(13, 9, 13, "AddressChanged", "61", NONZERO20));
        let mut previous = None;
        for execution in [
            Execution::FromZero,
            Execution::PerBlock,
            Execution::TwoByTwo,
            Execution::Idempotent,
            Execution::RedoZeroBlock,
        ] {
            let (database, row) = project_case_with_database(&fixture, 13, execution).await?;
            assert_marker(&row, false);
            let answer = evaluate_indexed_record(
                &row["entries"],
                &row["provenance"],
                &row["provenance"]["coverage"],
                "addr:60",
                "addr",
                Some("60"),
            );
            assert_eq!(
                answer.status,
                IndexedRecordStatus::Success,
                "{state}: {answer:?}"
            );
            assert_eq!(
                answer.value,
                Some(json!(if state == "nonzero" {
                    NONZERO20
                } else {
                    LATER20
                }))
            );
            assert_eq!(answer.derivation.is_some(), state != "nonzero");
            let exact = row["entries"]
                .as_array()
                .context("produced entries")?
                .iter()
                .find(|entry| entry["record_key"] == "addr:60");
            if state == "empty" {
                assert_eq!(
                    exact.context("retained empty exact entry")?["status"],
                    "not_found"
                );
            } else if state != "nonzero" {
                assert!(exact.is_none(), "{state} retained an exact entry: {row}");
            }
            if state == "version_reset" {
                assert_eq!(
                    row["record_version_boundary"]["event_kind"],
                    "RecordVersionChanged"
                );
            }
            if let Some(previous) = &previous {
                assert_eq!(&row, previous, "{state} under {execution:?} drifted");
            }
            previous = Some(row);
            database.cleanup().await?;
        }
    }
    Ok(())
}

fn assert_marker(row: &Value, marked: bool) {
    let marker = row["provenance"].get("exact_nonempty_not_found_record_keys");
    assert_eq!(marker.cloned(), marked.then(|| json!(["addr:60"])));
}

#[tokio::test]
async fn composed_project_lookup_persists_written_cleared_none() -> Result<()> {
    use bigname_lookup::{ChainRpcUrls, LedgerAction, LookupEngine, LookupRequest};
    let mut fixture = composed_fixture(true)?;
    fixture.id = "zero_default_persistence";
    let (database, row) = project_case_with_database(&fixture, 13, Execution::FromZero).await?;
    let pool = database.pool();
    let outcome = run_window(pool, 13, 13, 13, Some(13), RunMode::Normal).await?;
    let name: Value = sqlx::query_scalar("SELECT to_jsonb(name) FROM name_current name")
        .fetch_one(pool)
        .await?;
    assert_eq!(name["serving_resource_id"], RESOURCE);
    assert_eq!(
        name["declared_summary"]["topology"]["version_boundaries"]["record_version_boundary"],
        row["record_version_boundary"]
    );
    let logical_name = name["logical_name_id"]
        .as_str()
        .context("produced logical name")?;
    assert_eq!(
        logical_name,
        format!("ens:{}", bigname_lookup::ens_namehash_hex("zero.fixture")?)
    );
    raw_sql(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES ('68200000-0000-0000-0000-000000000104', 'ethereum-mainnet', 'contract');
        WITH manifest AS (
            INSERT INTO manifest_versions (
                manifest_version, namespace, source_family, chain_id, deployment_label,
                rollout_status, normalizer_version, file_path, manifest_payload)
            VALUES (1, 'ens', 'ens_execution', 'ethereum-mainnet', 'fixture', 'active',
                'fixture', 'fixture/execution.toml',
                '{"capability_flags":{"verified_resolution":{"status":"supported"}}}')
            RETURNING manifest_id
        )
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind)
        SELECT manifest_id, 'ethereum-mainnet', 'contract', 'universal_resolver',
            '68200000-0000-0000-0000-000000000104',
            '0x6820000000000000000000000000000000000061', 'universal_resolver', 'none'
        FROM manifest;
    "#,
    )
    .execute(pool)
    .await?;
    // This serving envelope is fixture metadata, not phase-runner completion evidence.
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
        SELECT chain_id, block_hash, block_number FROM chain_lineage
        WHERE block_number = $1 AND block_hash = $2",
    )
    .bind(outcome.current.number)
    .bind(&outcome.current.hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_phase_state (chain_id, phase_name, phase_status,
        current_block_number, current_block_hash, target_block_number, target_block_hash,
        input_content_hash, started_at, finished_at)
        VALUES ('ethereum-mainnet', 'project', 'completed', $1, $2, $1, $2, $3, now(), now())",
    )
    .bind(outcome.current.number)
    .bind(&outcome.current.hash)
    .bind(bigname_test_support::INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    let mut retained_id = None;
    for (value, action, active) in [
        (NONZERO20, LedgerAction::Written, 1_i64),
        (ZERO20, LedgerAction::Cleared, 0),
        (ZERO20, LedgerAction::None, 0),
    ] {
        let (url, server) = address_rpc(value).await?;
        let rpc = ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={url}")])?;
        let response = LookupEngine::new(pool.clone(), rpc)
            .lookup(LookupRequest::new(logical_name, ["addr:60"])?)
            .await?;
        server.await??;
        eprintln!(
            "Project lookup database {} returned {response:?}",
            database.database_name()
        );
        let ledger: (i64, i64) = sqlx::query_as(
            "SELECT count(*),
            count(*) FILTER (WHERE cleared_at IS NULL) FROM resolution_divergences
            WHERE logical_name_id = $1 AND request_kind = 'addr:60'",
        )
        .bind(logical_name)
        .fetch_one(pool)
        .await?;
        let row_id: Value = sqlx::query_scalar(
            "SELECT jsonb_build_array(logical_name_id, resolver_chain_id, resolver_address,
                encode(request_kind_hash, 'hex'), observed_positions, first_observed_at)
            FROM resolution_divergences
            WHERE logical_name_id = $1 AND request_kind = 'addr:60'",
        )
        .bind(logical_name)
        .fetch_one(pool)
        .await?;
        if let Some(previous) = &retained_id {
            assert_eq!(&row_id, previous, "the durable row identity changed");
        }
        retained_id = Some(row_id);
        assert_eq!(response.records[0].ledger_action, action);
        assert_eq!(
            ledger,
            (1, active),
            "the real writer must retain one durable observation"
        );
    }
    database.cleanup().await?;
    Ok(())
}

async fn address_rpc(value: &str) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let encoded = format!(
        "0x{:064x}{:0>64}{:064x}{:0>64}",
        64,
        &RESOLVER[2..],
        32,
        &value[2..]
    );
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut data = Vec::new();
        let request: Value = loop {
            let mut chunk = [0_u8; 4096];
            let count = socket.read(&mut chunk).await?;
            anyhow::ensure!(count > 0, "RPC request ended before its JSON body");
            data.extend_from_slice(&chunk[..count]);
            if let Some(end) = data.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&data[..end])?;
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim())
                    })
                    .context("RPC content length")?
                    .parse()?;
                if data.len() >= end + 4 + length {
                    break serde_json::from_slice(&data[end + 4..end + 4 + length])?;
                }
            }
        };
        assert_eq!(request["method"], "eth_call");
        assert_eq!(request["params"][1]["blockHash"], block_hash(13));
        let body = json!({"jsonrpc":"2.0", "id":request["id"], "result":encoded}).to_string();
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
        Ok(())
    });
    Ok((url, handle))
}

#[tokio::test]
async fn composed_zero_default_remains_exact_not_found() -> Result<()> {
    let mut previous = None;
    for execution in [
        Execution::FromZero,
        Execution::PerBlock,
        Execution::TwoByTwo,
        Execution::Idempotent,
        Execution::RedoZeroBlock,
    ] {
        let fixture = composed_fixture(true)?;
        let (database, row) = project_case_with_database(&fixture, 13, execution).await?;
        assert_eq!(row["support_status"], "supported");
        assert_eq!(
            row["provenance"]["coverage"],
            json!({"status":"projected", "exhaustiveness":"not_asserted"})
        );
        assert_eq!(
            row["provenance"]["read_rules"],
            json!([{
                "kind":"ensip19_default_address", "source_record_key":"addr:2147483648"
            }])
        );
        let entries = row["entries"].as_array().context("produced entries")?;
        let exact = entries
            .iter()
            .find(|entry| entry["record_key"] == "addr:60")
            .context("produced exact entry")?;
        assert_eq!(exact["status"], "not_found");
        assert!(exact.get("value").is_none());
        let default = entries
            .iter()
            .find(|entry| entry["record_key"] == "addr:2147483648")
            .context("produced default entry")?;
        assert_eq!(default["status"], "success");
        assert_eq!(default["value"], LATER20);
        let answer = evaluate_indexed_record(
            &row["entries"],
            &row["provenance"],
            &row["provenance"]["coverage"],
            "addr:60",
            "addr",
            Some("60"),
        );
        eprintln!(
            "Composed Project database {} under {execution:?}: {row}; answer: {answer:?}",
            database.database_name()
        );
        assert_eq!(answer.status, IndexedRecordStatus::NotFound, "{answer:?}");
        assert_eq!(answer.value, None);
        assert_eq!(answer.derivation, None);
        assert_marker(&row, true);
        if let Some(previous) = &previous {
            assert_eq!(&row, previous, "{execution:?} projection drift");
        }
        previous = Some(row);
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn composed_zero_without_feature_creates_no_read_rule() -> Result<()> {
    let fixture = composed_fixture(false)?;
    let (database, row) = project_case_with_database(&fixture, 13, Execution::FromZero).await?;
    assert_eq!(row["provenance"]["read_rules"], json!([]));
    assert_marker(&row, true);
    assert_entry_and_answer(&row, "addr:60", IndexedRecordStatus::NotFound, None);
    database.cleanup().await?;
    Ok(())
}

fn composed_fixture(feature: bool) -> Result<Case> {
    let mut fixture = case("ens_v1_addr60_set_zero")?.clone();
    fixture.id = if feature {
        "zero_default_feature"
    } else {
        "zero_default_no_feature"
    };
    fixture.events.insert(
        0,
        scalar(10, 1, 10, "AddressChanged", "2147483648", LATER20),
    );
    Ok(fixture)
}

#[tokio::test]
async fn exact_zero_addr60_projects_not_found_on_v1_arms() -> Result<()> {
    let actual = observations(&["ens_v1_addr60_set_zero", "basenames_addr60_set_zero"]).await?;
    assert_eq!(
        actual,
        json!([
            {"case":"ens_v1_addr60_set_zero","entry":{"record_family":"addr","record_key":"addr:60","selector_key":"60","status":"not_found"},"answer":{"status":"not_found","value":null}},
            {"case":"basenames_addr60_set_zero","entry":{"record_family":"addr","record_key":"addr:60","selector_key":"60","status":"not_found"},"answer":{"status":"not_found","value":null}}
        ])
    );
    Ok(())
}

#[tokio::test]
async fn legacy_addrchanged_zero_projects_not_found() -> Result<()> {
    let actual = observations(&[
        "ens_v1_legacy_addrchanged_zero",
        "ens_v1_legacy_nested_addrchanged_zero",
    ])
    .await?;
    assert_eq!(
        actual,
        json!([
            {"case":"ens_v1_legacy_addrchanged_zero","entry":{"record_family":"addr","record_key":"addr:60","selector_key":"60","status":"not_found"},"answer":{"status":"not_found","value":null}},
            {"case":"ens_v1_legacy_nested_addrchanged_zero","entry":{"record_family":"addr","record_key":"addr:60","selector_key":"60","status":"not_found"},"answer":{"status":"not_found","value":null}}
        ])
    );
    Ok(())
}

#[tokio::test]
async fn set_zero_set_observes_absence_then_latest_value() -> Result<()> {
    let fixture = case("ens_v1_addr60_set_zero_set")?;
    let zero = project_case_at(fixture, 12, Execution::FromZero).await?;
    assert_entry_and_answer(&zero, "addr:60", IndexedRecordStatus::NotFound, None);
    assert_terminal(fixture).await?;
    Ok(())
}

#[tokio::test]
async fn zero20_classification_is_coin60_and_authority_arm_scoped() -> Result<()> {
    let actual = observations(&[
        "ens_v1_addr60_set_zero",
        "ens_v1_coin0_zero20_control",
        "ens_v1_addr60_zero32_control",
        "ens_v1_addr60_nonzero20_control",
        "ens_v2_declared_v1_resolver_zero20_control",
    ])
    .await?;
    assert_eq!(
        actual,
        json!([
            {"case":"ens_v1_addr60_set_zero","entry":{"record_family":"addr","record_key":"addr:60","selector_key":"60","status":"not_found"},"answer":{"status":"not_found","value":null}},
            {"case":"ens_v1_coin0_zero20_control","entry":{"record_family":"addr","record_key":"addr:0","selector_key":"0","status":"success","value":ZERO20},"answer":{"status":"success","value":ZERO20}},
            {"case":"ens_v1_addr60_zero32_control","entry":{"record_family":"addr","record_key":"addr:60","selector_key":"60","status":"success","value":ZERO32},"answer":{"status":"success","value":ZERO32}},
            {"case":"ens_v1_addr60_nonzero20_control","entry":{"record_family":"addr","record_key":"addr:60","selector_key":"60","status":"success","value":NONZERO20},"answer":{"status":"success","value":NONZERO20}},
            {"case":"ens_v2_declared_v1_resolver_zero20_control","entry":{"record_family":"addr","record_key":"addr:60","selector_key":"60","status":"success","value":ZERO20},"answer":{"status":"success","value":ZERO20}}
        ])
    );
    Ok(())
}

#[tokio::test]
async fn zero_address_projection_converges_across_replay_modes() -> Result<()> {
    let fixture = case("ens_v1_addr60_set_zero")?;
    let from_zero = project_case_at(fixture, 13, Execution::FromZero).await?;
    assert_entry_and_answer(&from_zero, "addr:60", IndexedRecordStatus::NotFound, None);
    for execution in [
        Execution::PerBlock,
        Execution::TwoByTwo,
        Execution::Idempotent,
        Execution::RedoZeroBlock,
    ] {
        let replayed = project_case_at(fixture, 13, execution).await?;
        assert_eq!(replayed, from_zero, "{execution:?} projection drift");
    }
    Ok(())
}

async fn assert_terminal(fixture: &Case) -> Result<()> {
    let row = project_case_at(fixture, 13, Execution::FromZero).await?;
    assert_entry_and_answer(
        &row,
        fixture.expected_key,
        fixture.expected_status,
        fixture.expected_value,
    );
    Ok(())
}

async fn observations(ids: &[&str]) -> Result<Value> {
    let mut observed = Vec::new();
    for id in ids {
        let fixture = case(id)?;
        let row = project_case_at(fixture, 13, Execution::FromZero).await?;
        assert_marker(
            &row,
            fixture.expected_status == IndexedRecordStatus::NotFound,
        );
        let entry = row["entries"]
            .as_array()
            .and_then(|entries| {
                entries
                    .iter()
                    .find(|entry| entry["record_key"] == fixture.expected_key)
            })
            .with_context(|| format!("{} projected record entry missing from {row}", fixture.id))?;
        let answer = evaluate_indexed_record(
            &row["entries"],
            &row["provenance"],
            &json!({"status":"projected"}),
            fixture.expected_key,
            entry["record_family"].as_str().context("record family")?,
            entry["selector_key"].as_str(),
        );
        observed.push(json!({
            "case": fixture.id,
            "entry": entry,
            "answer": {"status":answer.status.as_str(), "value":answer.value}
        }));
    }
    Ok(Value::Array(observed))
}

fn assert_entry_and_answer(
    row: &Value,
    key: &str,
    status: IndexedRecordStatus,
    value: Option<&str>,
) {
    let entry = row["entries"]
        .as_array()
        .and_then(|entries| entries.iter().find(|entry| entry["record_key"] == key))
        .expect("projected record entry");
    assert_eq!(entry["status"], status.as_str());
    assert_eq!(entry.get("value").cloned(), value.map(|value| json!(value)));
    let answer = evaluate_indexed_record(
        &row["entries"],
        &row["provenance"],
        &json!({"status":"projected"}),
        key,
        entry["record_family"].as_str().expect("record family"),
        entry["selector_key"].as_str(),
    );
    assert_eq!(answer.status, status);
    assert_eq!(answer.value, value.map(|value| json!(value)));
}

fn case(id: &str) -> Result<&'static Case> {
    cases()
        .iter()
        .find(|fixture| fixture.id == id)
        .with_context(|| format!("missing zero-address fixture {id}"))
}

fn cases() -> &'static [Case] {
    static CASES: std::sync::OnceLock<Vec<Case>> = std::sync::OnceLock::new();
    CASES.get_or_init(|| {
        vec![
            fixture(
                "ens_v1_addr60_set_zero",
                "ens",
                "ethereum-mainnet",
                "ens_v1_registry_l1",
                "ens_v1_resolver_l1",
                "ens_v1",
                vec![
                    scalar(11, 1, 1, "AddressChanged", "60", NONZERO20),
                    scalar(11, 2, 1, "AddrChanged", "60", NONZERO20),
                    scalar(12, 1, 2, "AddrChanged", "60", NONZERO20),
                    scalar(13, 1, 3, "AddressChanged", "60", ZERO20),
                    scalar(13, 2, 3, "AddrChanged", "60", ZERO20),
                ],
                "addr:60",
                IndexedRecordStatus::NotFound,
                None,
            ),
            fixture(
                "basenames_addr60_set_zero",
                "basenames",
                "base-mainnet",
                "basenames_base_registry",
                "basenames_base_resolver",
                "basenames",
                vec![
                    scalar(11, 1, 1, "AddressChanged", "60", NONZERO20),
                    scalar(11, 2, 1, "AddrChanged", "60", NONZERO20),
                    scalar(13, 1, 3, "AddressChanged", "60", ZERO20),
                    scalar(13, 2, 3, "AddrChanged", "60", ZERO20),
                ],
                "addr:60",
                IndexedRecordStatus::NotFound,
                None,
            ),
            fixture(
                "ens_v1_legacy_addrchanged_zero",
                "ens",
                "ethereum-mainnet",
                "ens_v1_registrar_l1",
                "ens_v1_resolver_l1",
                "ens_v1",
                vec![scalar(11, 1, 1, "AddrChanged", "60", ZERO20)],
                "addr:60",
                IndexedRecordStatus::NotFound,
                None,
            ),
            fixture(
                "ens_v1_legacy_nested_addrchanged_zero",
                "ens",
                "ethereum-mainnet",
                "ens_v1_wrapper_l1",
                "ens_v1_resolver_l1",
                "ens_v1",
                vec![nested(11, 1, 1, "AddrChanged", "60", ZERO20)],
                "addr:60",
                IndexedRecordStatus::NotFound,
                None,
            ),
            fixture(
                "ens_v1_addr60_set_zero_set",
                "ens",
                "ethereum-mainnet",
                "ens_v1_registry_l1",
                "ens_v1_resolver_l1",
                "ens_v1",
                vec![
                    scalar(11, 1, 1, "AddrChanged", "60", NONZERO20),
                    scalar(12, 1, 2, "AddressChanged", "60", ZERO20),
                    scalar(12, 2, 2, "AddrChanged", "60", ZERO20),
                    scalar(13, 1, 3, "AddrChanged", "60", LATER20),
                ],
                "addr:60",
                IndexedRecordStatus::Success,
                Some(LATER20),
            ),
            fixture(
                "ens_v1_coin0_zero20_control",
                "ens",
                "ethereum-mainnet",
                "ens_v1_registry_l1",
                "ens_v1_resolver_l1",
                "ens_v1",
                vec![flat(11, "0", ZERO20)],
                "addr:0",
                IndexedRecordStatus::Success,
                Some(ZERO20),
            ),
            fixture(
                "ens_v1_addr60_zero32_control",
                "ens",
                "ethereum-mainnet",
                "ens_v1_registry_l1",
                "ens_v1_resolver_l1",
                "ens_v1",
                vec![flat(11, "60", ZERO32)],
                "addr:60",
                IndexedRecordStatus::Success,
                Some(ZERO32),
            ),
            fixture(
                "ens_v1_addr60_nonzero20_control",
                "ens",
                "ethereum-mainnet",
                "ens_v1_registry_l1",
                "ens_v1_resolver_l1",
                "ens_v1",
                vec![scalar(11, 1, 1, "AddrChanged", "60", NONZERO20)],
                "addr:60",
                IndexedRecordStatus::Success,
                Some(NONZERO20),
            ),
            fixture(
                "ens_v2_declared_v1_resolver_zero20_control",
                "ens",
                "ethereum-mainnet",
                "ens_v2_registry_l1",
                "ens_v1_resolver_l1",
                "ens_v2",
                vec![scalar(11, 1, 1, "AddrChanged", "60", ZERO20)],
                "addr:60",
                IndexedRecordStatus::Success,
                Some(ZERO20),
            ),
        ]
    })
}

#[allow(clippy::too_many_arguments)]
fn fixture(
    id: &'static str,
    namespace: &'static str,
    chain: &'static str,
    pointer_source_family: &'static str,
    event_source_family: &'static str,
    authority_arm: &'static str,
    events: Vec<FixtureEvent>,
    expected_key: &'static str,
    expected_status: IndexedRecordStatus,
    expected_value: Option<&'static str>,
) -> Case {
    Case {
        id,
        namespace,
        chain,
        pointer_source_family,
        event_source_family,
        authority_arm,
        events,
        expected_key,
        expected_status,
        expected_value,
    }
}

fn scalar(
    block: i64,
    log_index: i64,
    transaction: i64,
    source_event: &str,
    coin: &str,
    value: &str,
) -> FixtureEvent {
    event(
        block,
        log_index,
        transaction,
        json!({
            "node": NODE, "resolver": RESOLVER, "record_key": format!("addr:{coin}"),
            "record_family": "addr", "selector_key": coin, "source_event": source_event,
            "value": value
        }),
    )
}

fn nested(
    block: i64,
    log_index: i64,
    transaction: i64,
    source_event: &str,
    coin: &str,
    value: &str,
) -> FixtureEvent {
    event(
        block,
        log_index,
        transaction,
        json!({
            "node": NODE, "resolver": RESOLVER, "record_key": format!("addr:{coin}"),
            "record_family": "addr", "selector_key": coin, "source_event": source_event,
            "value": {"encoding":"hex", "bytes":value}
        }),
    )
}

fn flat(block: i64, coin: &str, value: &str) -> FixtureEvent {
    event(
        block,
        1,
        1,
        json!({
            "node": NODE, "resolver": RESOLVER, "record_key": format!("addr:{coin}"),
            "record_family": "addr", "selector_key": coin, "source_event": "AddressChanged",
            "coin_type": coin, "address_bytes_hex": value, "value_retained": false
        }),
    )
}

fn event(block: i64, log_index: i64, transaction: i64, after_state: Value) -> FixtureEvent {
    FixtureEvent {
        block,
        log_index,
        transaction,
        after_state,
    }
}

async fn project_case_at(fixture: &Case, target: i64, execution: Execution) -> Result<Value> {
    let (database, row) = project_case_with_database(fixture, target, execution).await?;
    database.cleanup().await?;
    Ok(row)
}

async fn project_case_with_database(
    fixture: &Case,
    target: i64,
    execution: Execution,
) -> Result<(TestDatabase, Value)> {
    let (database, pool) =
        database(&format!("{}_{}", fixture.id, execution_name(execution))).await?;
    seed(&pool, fixture).await?;
    let before = normalized_snapshot(&pool).await?;
    match execution {
        Execution::FromZero => {
            run_window(&pool, target, 0, target, None, RunMode::Normal).await?;
        }
        Execution::PerBlock => {
            run_window(&pool, 10, 0, 10, None, RunMode::Normal).await?;
            for block in 11..=target {
                run_window(&pool, block, block, block, Some(block - 1), RunMode::Normal).await?;
            }
        }
        Execution::TwoByTwo => {
            run_window(&pool, 11, 0, 11, None, RunMode::Normal).await?;
            run_window(&pool, target, 12, target, Some(11), RunMode::Normal).await?;
        }
        Execution::Idempotent => {
            run_window(&pool, 12, 0, 12, None, RunMode::Normal).await?;
            run_window(&pool, target, 13, target, Some(12), RunMode::Normal).await?;
            run_window(&pool, target, 13, target, Some(12), RunMode::Normal).await?;
        }
        Execution::RedoZeroBlock => {
            run_window(&pool, target, 0, target, None, RunMode::Normal).await?;
            run_window(&pool, target, 13, 13, Some(target), RunMode::Redo).await?;
        }
    }
    assert_eq!(
        normalized_snapshot(&pool).await?,
        before,
        "normalized rows changed"
    );
    let raw_count: i64 = sqlx::query_scalar("SELECT count(*) FROM raw_logs")
        .fetch_one(&pool)
        .await?;
    assert_eq!(raw_count, 0, "Project wrote raw facts");
    let row: Value = sqlx::query_scalar(
        "SELECT to_jsonb(row) - 'last_recomputed_at' - 'inserted_at' \
         FROM record_inventory_current row WHERE resource_id = $1::uuid",
    )
    .bind(RESOURCE)
    .fetch_one(&pool)
    .await?;
    Ok((database, row))
}

fn execution_name(execution: Execution) -> &'static str {
    match execution {
        Execution::FromZero => "from_zero",
        Execution::PerBlock => "per_block",
        Execution::TwoByTwo => "two_by_two",
        Execution::Idempotent => "idempotent",
        Execution::RedoZeroBlock => "redo_zero",
    }
}

async fn run_window(
    pool: &PgPool,
    target_block: i64,
    affected_from_block: i64,
    affected_to_block: i64,
    resume_current: Option<i64>,
    mode: RunMode,
) -> Result<BatchOutcome> {
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: sqlx::query_scalar("SELECT chain_id FROM chain_lineage LIMIT 1")
                .fetch_one(pool)
                .await?,
            target_block,
            affected_from_block,
            affected_to_block,
            resume_current: resume_current.map(|number| Marker {
                number,
                hash: block_hash(number),
            }),
            mode,
        })
        .await?;
    assert!(outcome.complete);
    assert_eq!(outcome.current, outcome.target);
    assert_eq!(outcome.target.number, target_block);
    assert_eq!(outcome.target.hash, block_hash(target_block));
    Ok(outcome)
}

async fn normalized_snapshot(pool: &PgPool) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(event) ORDER BY normalized_event_id), '[]') \
         FROM normalized_events event",
    )
    .fetch_one(pool)
    .await?)
}

async fn seed(pool: &PgPool, fixture: &Case) -> Result<()> {
    let persistence = fixture.id == "zero_default_persistence";
    let node = if persistence {
        bigname_lookup::ens_namehash_hex("zero.fixture")?
    } else {
        NODE.to_owned()
    };
    let name = if persistence {
        "zero.fixture".to_owned()
    } else {
        format!("{}.fixture", fixture.id)
    };
    let dns = if persistence {
        b"\x04zero\x07fixture\0".as_slice()
    } else {
        b"\0".as_slice()
    };
    for number in 10..=13 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($4),'canonical')")
            .bind(fixture.chain).bind(block_hash(number)).bind(number).bind(1_800_000_000 + number).execute(pool).await?;
    }
    let payload = json!({"deployment_epoch":"fixture","contracts":[{
        "role":"resolver", "address":RESOLVER, "proxy_kind":"none", "start_block":0,
        "read_features": if matches!(fixture.id, "zero_default_feature" | "zero_default_persistence") {
            json!(["ensip19_default_address"])
        } else { json!([]) }
    }]});
    let manifest_id: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,$1,$2,$3,'fixture','active','fixture',$4,$5) RETURNING manifest_id")
        .bind(fixture.namespace).bind(fixture.event_source_family).bind(fixture.chain)
        .bind(format!("fixture/{}.toml", fixture.id)).bind(&payload).fetch_one(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state) VALUES ($1,$2,'SourceManifestUpdated',$3,1,$4,$5,'manifest_sync','canonical',$6)")
        .bind(format!("manifest:{}", fixture.id)).bind(fixture.namespace).bind(fixture.event_source_family)
        .bind(manifest_id).bind(fixture.chain).bind(json!({"rollout_status":"active","normalizer_version":"fixture","manifest_payload":payload})).execute(pool).await?;
    if fixture.pointer_source_family.starts_with("ens_v2_") {
        seed_ens_v2_resolver_admission(pool, fixture).await?;
    }
    let labelhashes: Vec<_> = name
        .split('.')
        .map(|label| format!("{:#x}", alloy_primitives::keccak256(label)))
        .collect();
    let logical_name_id = format!("{}:{node}", fixture.namespace);
    sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,$2,$3,string_to_array($3,'.'),$7,$4,$8,'fixture','active',$5,$6,10,'canonical')")
        .bind(&logical_name_id).bind(fixture.namespace).bind(&name)
        .bind(&node).bind(fixture.chain).bind(block_hash(10)).bind(dns).bind(labelhashes).execute(pool).await?;
    sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,10,'canonical')")
        .bind(RESOURCE).bind(fixture.chain).bind(block_hash(10)).execute(pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,active_to,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path',$4,to_timestamp(1800000010),CASE WHEN $7 THEN to_timestamp(1800000011) END,$5,$6,10,'canonical')")
        .bind(BINDING).bind(&logical_name_id).bind(RESOURCE).bind(fixture.authority_arm)
        .bind(fixture.chain).bind(block_hash(10)).bind(persistence).execute(pool).await?;
    insert_event(
        pool,
        fixture,
        "pointer",
        Some(&logical_name_id),
        Some(RESOURCE),
        "ResolverChanged",
        fixture.pointer_source_family,
        None,
        10,
        0,
        0,
        json!({"node":NODE,"resolver":RESOLVER}),
    )
    .await?;
    if fixture.pointer_source_family.starts_with("ens_v2_") {
        insert_event(
            pool,
            fixture,
            "resolver-alias",
            None,
            None,
            "AliasChanged",
            fixture.event_source_family,
            Some(manifest_id),
            10,
            1,
            0,
            json!({"resolver":RESOLVER,"active":false}),
        )
        .await?;
    }
    if persistence {
        insert_event(
            pool,
            fixture,
            "ownerless",
            Some(&logical_name_id),
            Some(RESOURCE),
            "AuthorityTransferred",
            fixture.pointer_source_family,
            None,
            11,
            0,
            11,
            json!({"node":node, "owner_getter":ZERO20, "owner_getter_reason":"zero_owner"}),
        )
        .await?;
    }
    for (index, event) in fixture.events.iter().enumerate() {
        insert_event(
            pool,
            fixture,
            &format!("record-{index}"),
            None,
            None,
            if event.after_state.get("record_version").is_some() {
                "RecordVersionChanged"
            } else {
                "RecordChanged"
            },
            fixture.event_source_family,
            Some(manifest_id),
            event.block,
            event.log_index,
            event.transaction,
            event.after_state.clone(),
        )
        .await?;
    }
    Ok(())
}

async fn seed_ens_v2_resolver_admission(pool: &PgPool, fixture: &Case) -> Result<()> {
    let registry_manifest_id: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,$1,$2,$3,'fixture-v2','active','fixture',$4,'{}') RETURNING manifest_id")
        .bind(fixture.namespace).bind(fixture.pointer_source_family).bind(fixture.chain)
        .bind(format!("fixture/{}-v2.toml", fixture.id)).fetch_one(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state) VALUES ($1,$2,'SourceManifestUpdated',$3,1,$4,$5,'manifest_sync','canonical',$6)")
        .bind(format!("manifest:{}:v2", fixture.id)).bind(fixture.namespace)
        .bind(fixture.pointer_source_family).bind(registry_manifest_id).bind(fixture.chain)
        .bind(json!({"rollout_status":"active","normalizer_version":"fixture","manifest_payload":{}}))
        .execute(pool).await?;
    for instance in [REGISTRY_INSTANCE, RESOLVER_INSTANCE] {
        sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind) VALUES ($1::uuid,$2,'contract')")
            .bind(instance).bind(fixture.chain).execute(pool).await?;
    }
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,active_from_block_number,active_from_block_hash,source_manifest_id) VALUES ($1::uuid,$2,$3,10,$4,$5)")
        .bind(RESOLVER_INSTANCE).bind(fixture.chain).bind(RESOLVER).bind(block_hash(10))
        .bind(registry_manifest_id).execute(pool).await?;
    sqlx::query("INSERT INTO discovery_edges (chain_id,edge_kind,from_contract_instance_id,to_contract_instance_id,discovery_source,admission_basis,source_manifest_id,active_from_block_number,active_from_block_hash,canonicality_state) VALUES ($1,'resolver',$2::uuid,$3::uuid,'fixture','fixture',$4,10,$5,'canonical')")
        .bind(fixture.chain).bind(REGISTRY_INSTANCE).bind(RESOLVER_INSTANCE)
        .bind(registry_manifest_id).bind(block_hash(10)).execute(pool).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_event(
    pool: &PgPool,
    fixture: &Case,
    identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<&str>,
    event_kind: &str,
    source_family: &str,
    manifest_id: Option<i64>,
    block: i64,
    log_index: i64,
    transaction: i64,
    mut after_state: Value,
) -> Result<()> {
    if fixture.id == "zero_default_persistence" {
        after_state["node"] = json!(bigname_lookup::ens_namehash_hex("zero.fixture")?);
    }
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref) VALUES ($1,$2,$3,$4::uuid,$5,$6,1,$7,$8,$9,$10,$11,0,$12,'ens_v1_unwrapped_authority','canonical',$13,$14)")
        .bind(format!("{}:{identity}:{block}:{log_index}", fixture.id)).bind(fixture.namespace)
        .bind(logical_name_id).bind(resource_id).bind(event_kind).bind(source_family).bind(manifest_id)
        .bind(fixture.chain).bind(block).bind(block_hash(block))
        .bind(format!("0x{transaction:064x}")).bind(log_index).bind(after_state)
        .bind(json!({"emitting_address":RESOLVER})).execute(pool).await?;
    Ok(())
}

fn block_hash(number: i64) -> String {
    format!("0x{number:064x}")
}

async fn database(name: &str) -> Result<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new(format!("zero_addr_{name}")).pool_max_connections(1),
    )
    .await?;
    let pool = database.pool().clone();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut transaction = pool.begin().await?;
    raw_sql(&format!("CREATE SCHEMA bigname_phase; ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public; SET LOCAL search_path TO bigname_phase, public", database_name.replace('"', "\"\""))).execute(&mut *transaction).await?;
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
    Ok((database, pool))
}
