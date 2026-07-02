use std::collections::BTreeMap;

use anyhow::{Context, Result};
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
    response::Response,
};
use bigname_storage::{
    CanonicalityState, ChainCheckpointUpdate, ChainLineageBlock, ChainPosition, ChainPositions,
    CheckpointBlockRef, NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentRow,
    NameSurface, Resource, SelectedSnapshot, SnapshotConsistency, SurfaceBinding,
    SurfaceBindingKind, TokenLineage, advance_chain_checkpoints, upsert_chain_lineage_blocks,
    upsert_name_current_rows, upsert_name_surfaces, upsert_resources, upsert_surface_bindings,
    upsert_token_lineages,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::types::{
    Uuid,
    time::{OffsetDateTime, UtcOffset},
};
use tower::ServiceExt;

use crate::v2::ErrorCode;

use super::*;

const DIVERGENT_REGISTRY_OWNER: &str = "0x0000000000000000000000000000000000000d01";
const DIVERGENT_CONTROL_OWNER: &str = "0x0000000000000000000000000000000000000d02";
const DIVERGENT_REGISTRATION_REGISTRANT: &str = "0x0000000000000000000000000000000000000d03";
const DIVERGENT_CONTROL_REGISTRANT: &str = "0x0000000000000000000000000000000000000d04";

#[test]
fn search_cursor_payload_round_trips_name_cursor() {
    let cursor = NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name("alpha.eth".to_owned()),
        namespace: "ens".to_owned(),
        normalized_name: "alpha.eth".to_owned(),
        namehash: "node:alpha.eth".to_owned(),
    };
    let binding = cursor_binding("al", SearchMatch::Prefix, Some("ens"), "snapshot-1");
    let payload = search_cursor_payload(&cursor, &binding).expect("name cursor must encode");

    assert_eq!(
        search_storage_cursor(&payload, &binding).expect("cursor must decode"),
        cursor
    );
    assert_eq!(payload.sort, SEARCH_SORT);
    assert_eq!(payload.filters[Q_FILTER_KEY], "al");
    assert_eq!(payload.filters[MATCH_FILTER_KEY], "prefix");
    assert_eq!(payload.filters[NAMESPACE_FILTER_KEY], "ens");
}

#[test]
fn search_cursor_rejects_cross_filter_match_namespace_or_snapshot() {
    let cursor = NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name("alpha.eth".to_owned()),
        namespace: "ens".to_owned(),
        normalized_name: "alpha.eth".to_owned(),
        namehash: "node:alpha.eth".to_owned(),
    };
    let binding = cursor_binding("al", SearchMatch::Prefix, Some("ens"), "snapshot-1");

    let mut payload = search_cursor_payload(&cursor, &binding).expect("name cursor must encode");
    payload
        .filters
        .insert(Q_FILTER_KEY.to_owned(), "be".to_owned());
    assert!(search_storage_cursor(&payload, &binding).is_err());

    let mut payload = search_cursor_payload(&cursor, &binding).expect("name cursor must encode");
    payload
        .filters
        .insert(MATCH_FILTER_KEY.to_owned(), "contains".to_owned());
    assert!(search_storage_cursor(&payload, &binding).is_err());

    let mut payload = search_cursor_payload(&cursor, &binding).expect("name cursor must encode");
    payload
        .filters
        .insert(NAMESPACE_FILTER_KEY.to_owned(), "basenames".to_owned());
    assert!(search_storage_cursor(&payload, &binding).is_err());

    let mut payload = search_cursor_payload(&cursor, &binding).expect("name cursor must encode");
    payload.snapshot = Some("snapshot-2".to_owned());
    assert!(search_storage_cursor(&payload, &binding).is_err());

    let mut payload = search_cursor_payload(&cursor, &binding).expect("name cursor must encode");
    payload.sort = "name_desc".to_owned();
    assert!(search_storage_cursor(&payload, &binding).is_err());
}

#[test]
fn search_cursor_payload_rejects_non_name_storage_cursor() {
    let cursor = NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Timestamp(None),
        namespace: "ens".to_owned(),
        normalized_name: "alpha.eth".to_owned(),
        namehash: "node:alpha.eth".to_owned(),
    };
    let binding = cursor_binding("al", SearchMatch::Prefix, Some("ens"), "snapshot-1");

    let error =
        search_cursor_payload(&cursor, &binding).expect_err("non-name cursor must not encode");

    assert_eq!(error.code(), ErrorCode::InternalError);
}

#[test]
fn search_query_requires_q_and_parses_match_controls() {
    let parsed = SearchQueryParams::try_from(RawSearchQueryParams {
        q: Some(" AL ".to_owned()),
        ..RawSearchQueryParams::default()
    })
    .expect("default search params must parse");
    assert_eq!(parsed.q, "al");
    assert_eq!(parsed.match_mode, SearchMatch::Prefix);

    let contains = SearchQueryParams::try_from(RawSearchQueryParams {
        q: Some("ha".to_owned()),
        match_mode: Some("contains".to_owned()),
        ..RawSearchQueryParams::default()
    })
    .expect("contains match must parse");
    assert_eq!(contains.match_mode, SearchMatch::Contains);

    assert!(
        SearchQueryParams::try_from(RawSearchQueryParams {
            q: None,
            ..RawSearchQueryParams::default()
        })
        .is_err()
    );
    assert!(
        SearchQueryParams::try_from(RawSearchQueryParams {
            q: Some(" ".to_owned()),
            ..RawSearchQueryParams::default()
        })
        .is_err()
    );
    assert!(
        SearchQueryParams::try_from(RawSearchQueryParams {
            q: Some("al".to_owned()),
            match_mode: Some("suffix".to_owned()),
            ..RawSearchQueryParams::default()
        })
        .is_err()
    );
}

#[tokio::test]
async fn v2_search_prefix_returns_record_rows() -> Result<()> {
    let database = SearchDatabase::new().await?;
    seed_search_fixture(&database).await?;

    let payload = search_payload(&database, "/v2/search?q=al&namespace=ens").await?;

    assert_eq!(payload["page"]["page_size"], json!(50));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 110,
            "block_hash": "0xeth110",
            "timestamp": "2026-04-17T00:01:50Z"
        })
    );

    let data = payload["data"].as_array().expect("data must be an array");
    assert_eq!(names(data), vec!["alpha.eth", "alpine.eth"]);
    assert_eq!(data[0]["display_name"], json!("alpha.eth"));
    assert_eq!(data[0]["namespace"], json!("ens"));
    assert_eq!(data[0]["namehash"], json!("node:alpha.eth"));
    assert_eq!(
        data[0]["owner"],
        json!("0x00000000000000000000000000000000000000a1")
    );
    assert_eq!(
        data[0]["registrant"],
        json!("0x00000000000000000000000000000000000000a2")
    );
    assert_eq!(data[0]["registration_status"], json!("active"));
    assert_eq!(data[0]["registered_at"], json!("2024-01-02T00:00:00Z"));
    assert_eq!(data[0]["created_at"], json!("2023-01-02T00:00:00Z"));
    assert_eq!(data[0]["expires_at"], json!("2027-01-02T00:00:00Z"));
    assert!(data[0].get("relations").is_none());
    assert!(data[0].get("is_primary").is_none());
    assert!(data[0].get("role_summary").is_none());
    assert!(data[0].get("labelhash").is_none());
    assert!(data[0].get("subname_count").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_uses_dictionary_owner_and_registrant_precedence() -> Result<()> {
    let database = SearchDatabase::new().await?;
    seed_search_fixture(&database).await?;

    let payload = search_payload(&database, "/v2/search?q=precedence&namespace=ens").await?;
    let data = payload["data"].as_array().expect("data must be an array");

    assert_eq!(names(data), vec!["precedence.eth"]);
    assert_eq!(data[0]["owner"], json!(DIVERGENT_CONTROL_OWNER));
    assert_eq!(
        data[0]["registrant"],
        json!(DIVERGENT_REGISTRATION_REGISTRANT)
    );
    assert_eq!(data[0]["registration_status"], json!("active"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_match_modes_and_q_validation() -> Result<()> {
    let database = SearchDatabase::new().await?;
    seed_search_fixture(&database).await?;

    let default_payload = search_payload(&database, "/v2/search?q=amm&namespace=ens").await?;
    assert_eq!(
        names(default_payload["data"].as_array().unwrap()),
        Vec::<&str>::new()
    );

    let contains_payload =
        search_payload(&database, "/v2/search?q=amm&match=contains&namespace=ens").await?;
    assert_eq!(
        names(contains_payload["data"].as_array().unwrap()),
        vec!["gamma.eth"]
    );

    for uri in [
        "/v2/search?namespace=ens",
        "/v2/search?q=&namespace=ens",
        "/v2/search?q=al&match=suffix&namespace=ens",
    ] {
        let response = search_response(&database, uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(
            read_json(response).await?["error"]["code"],
            json!("invalid_input")
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_lowercases_q_and_filters_namespace() -> Result<()> {
    let database = SearchDatabase::new().await?;
    seed_search_fixture(&database).await?;

    let uppercase = search_payload(&database, "/v2/search?q=AL&namespace=ens").await?;
    assert_eq!(
        names(uppercase["data"].as_array().unwrap()),
        vec!["alpha.eth", "alpine.eth"]
    );

    let all_namespaces = search_payload(&database, "/v2/search?q=alpha").await?;
    assert_eq!(
        names(all_namespaces["data"].as_array().unwrap()),
        vec!["alpha.base.eth", "alpha.eth"]
    );
    assert!(all_namespaces["meta"]["as_of"].get("1").is_some());
    assert!(all_namespaces["meta"]["as_of"].get("8453").is_some());

    let basenames = search_payload(&database, "/v2/search?q=alpha&namespace=basenames").await?;
    assert_eq!(
        names(basenames["data"].as_array().unwrap()),
        vec!["alpha.base.eth"]
    );
    assert_eq!(
        basenames["meta"]["as_of"]["8453"],
        json!({
            "block_number": 210,
            "block_hash": "0xbase210",
            "timestamp": "2026-04-17T00:03:30Z"
        })
    );

    let unknown = search_response(&database, "/v2/search?q=alpha&namespace=unknown").await?;
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json(unknown).await?["error"]["code"],
        json!("invalid_input")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_escapes_like_metacharacters() -> Result<()> {
    let database = SearchDatabase::new().await?;
    seed_search_fixture(&database).await?;

    let percent = search_payload(&database, "/v2/search?q=percent%25&namespace=ens").await?;
    assert_eq!(
        names(percent["data"].as_array().unwrap()),
        vec!["percent%name.eth"]
    );

    let underscore = search_payload(&database, "/v2/search?q=under_&namespace=ens").await?;
    assert_eq!(
        names(underscore["data"].as_array().unwrap()),
        vec!["under_score.eth"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_paginates_without_overlap_or_gap() -> Result<()> {
    let database = SearchDatabase::new().await?;
    seed_search_fixture(&database).await?;

    let first = search_payload(&database, "/v2/search?q=a&page_size=1").await?;
    assert_eq!(
        names(first["data"].as_array().unwrap()),
        vec!["alpha.base.eth"]
    );
    assert_eq!(first["page"]["has_more"], json!(true));
    let next_cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("first page must include next cursor");

    let second = search_payload(
        &database,
        &format!("/v2/search?q=a&page_size=1&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(second["page"]["cursor"], json!(next_cursor));
    assert_eq!(names(second["data"].as_array().unwrap()), vec!["alpha.eth"]);
    assert_eq!(second["page"]["has_more"], json!(true));

    let third_cursor = second["page"]["next_cursor"]
        .as_str()
        .expect("second page must include next cursor");
    let third = search_payload(
        &database,
        &format!("/v2/search?q=a&page_size=1&cursor={third_cursor}"),
    )
    .await?;
    assert_eq!(
        names(third["data"].as_array().unwrap()),
        vec!["alpine.base.eth"]
    );
    assert_eq!(third["page"]["has_more"], json!(true));

    let final_cursor = third["page"]["next_cursor"]
        .as_str()
        .expect("third page must include next cursor");
    let final_page = search_payload(
        &database,
        &format!("/v2/search?q=a&page_size=1&cursor={final_cursor}"),
    )
    .await?;
    assert_eq!(
        names(final_page["data"].as_array().unwrap()),
        vec!["alpine.eth"]
    );
    assert_eq!(final_page["page"]["has_more"], json!(false));
    assert_eq!(final_page["page"]["next_cursor"], Value::Null);

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_rejects_cursor_anchor_changes() -> Result<()> {
    let database = SearchDatabase::new().await?;
    seed_search_fixture(&database).await?;

    let first = search_payload(&database, "/v2/search?q=al&namespace=ens&page_size=1").await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("first page must include cursor");

    for uri in [
        format!("/v2/search?q=ga&namespace=ens&page_size=1&cursor={cursor}"),
        format!("/v2/search?q=al&match=contains&namespace=ens&page_size=1&cursor={cursor}"),
        format!("/v2/search?q=al&namespace=basenames&page_size=1&cursor={cursor}"),
        format!("/v2/search?q=al&namespace=ens&finality=finalized&page_size=1&cursor={cursor}"),
    ] {
        let response = search_response(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(
            read_json(response).await?["error"]["code"],
            json!("invalid_input")
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_snapshot_selectors_unknown_params_and_empty_matches() -> Result<()> {
    let database = SearchDatabase::new().await?;
    seed_search_fixture(&database).await?;

    let safe = search_payload(&database, "/v2/search?q=al&namespace=ens&finality=safe").await?;
    assert_eq!(
        safe["meta"]["as_of"]["1"],
        json!({
            "block_number": 109,
            "block_hash": "0xeth109",
            "timestamp": "2026-04-17T00:01:49Z"
        })
    );

    let snapshot_token = database
        .snapshot_token("ens", "2026-04-17T00:01:48Z")
        .await?;
    let pinned = search_payload(
        &database,
        &format!("/v2/search?q=al&namespace=ens&at={snapshot_token}"),
    )
    .await?;
    assert_eq!(
        pinned["meta"]["as_of"]["1"],
        json!({
            "block_number": 108,
            "block_hash": "0xeth108",
            "timestamp": "2026-04-17T00:01:48Z"
        })
    );

    let unknown = search_response(&database, "/v2/search?q=al&namespace=ens&sort=name").await?;
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json(unknown).await?["error"]["code"],
        json!("invalid_input")
    );

    let empty = search_payload(&database, "/v2/search?q=nomatch").await?;
    assert_eq!(empty["data"], json!([]));
    assert_eq!(empty["page"]["has_more"], json!(false));
    assert_eq!(empty["page"]["next_cursor"], Value::Null);

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_union_at_replay_rejects_single_name_scope() -> Result<()> {
    let database = SearchDatabase::new().await?;
    seed_search_fixture(&database).await?;

    let first = search_payload(&database, "/v2/search?q=alpha").await?;
    assert_eq!(
        names(first["data"].as_array().unwrap()),
        vec!["alpha.base.eth", "alpha.eth"]
    );
    assert_eq!(
        first["meta"]["as_of"]["1"],
        json!({
            "block_number": 110,
            "block_hash": "0xeth110",
            "timestamp": "2026-04-17T00:01:50Z"
        })
    );
    assert_eq!(
        first["meta"]["as_of"]["8453"],
        json!({
            "block_number": 210,
            "block_hash": "0xbase210",
            "timestamp": "2026-04-17T00:03:30Z"
        })
    );
    assert!(first["meta"]["as_of"].get("11155111").is_none());

    let replay_at = search_union_at_token_from_meta_as_of(&first)?;
    let replay = search_payload(&database, &format!("/v2/search?q=alpha&at={replay_at}")).await?;
    assert_eq!(replay["meta"]["as_of"], first["meta"]["as_of"]);
    assert_eq!(replay["data"], first["data"]);

    let rejected =
        search_response(&database, &format!("/v2/names/alpha.eth?at={replay_at}")).await?;
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    let error = read_json(rejected).await?;
    assert_eq!(error["error"]["code"], json!("invalid_input"));
    assert_eq!(
        error["error"]["message"],
        json!("unsupported snapshot position slot base")
    );

    seed_chain(
        &database,
        "ethereum-sepolia",
        "0xsepolia",
        300,
        310,
        1_776_384_300,
    )
    .await?;
    let sepolia_at = sepolia_snapshot_token();
    let mixed_profile =
        search_response(&database, &format!("/v2/search?q=alpha&at={sepolia_at}")).await?;
    assert_eq!(mixed_profile.status(), StatusCode::CONFLICT);
    let error = read_json(mixed_profile).await?;
    assert_eq!(error["error"]["code"], json!("conflict"));
    assert_eq!(
        error["error"]["message"],
        json!("snapshot selector cannot form one canonical snapshot across deployment profiles")
    );

    database.cleanup().await
}

fn cursor_binding<'a>(
    q: &'a str,
    match_mode: SearchMatch,
    namespace: Option<&'a str>,
    snapshot_token: &'a str,
) -> SearchCursorBinding<'a> {
    SearchCursorBinding {
        q,
        match_mode,
        namespace,
        snapshot_token,
    }
}

fn names(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .map(|row| row["name"].as_str().expect("row must include name"))
        .collect()
}

fn sepolia_snapshot_token() -> String {
    encode_at_token(&SelectedSnapshot {
        chain_positions: ChainPositions::new(BTreeMap::from([(
            "ethereum-sepolia".to_owned(),
            snapshot_position(
                "ethereum-sepolia",
                "ethereum-sepolia",
                308,
                "0xsepolia308",
                1_776_384_308,
            ),
        )])),
        consistency: SnapshotConsistency::Head,
    })
}

fn search_union_at_token_from_meta_as_of(payload: &Value) -> Result<String> {
    Ok(encode_at_token(&SelectedSnapshot {
        chain_positions: ChainPositions::new(BTreeMap::from([
            snapshot_position_from_meta_as_of(payload, "1", "ethereum", "ethereum-mainnet")?,
            snapshot_position_from_meta_as_of(payload, "8453", "base", "base-mainnet")?,
        ])),
        consistency: SnapshotConsistency::Head,
    }))
}

fn snapshot_position_from_meta_as_of(
    payload: &Value,
    numeric_chain_id: &str,
    slot: &str,
    chain_id: &str,
) -> Result<(String, ChainPosition)> {
    let as_of = payload
        .pointer(&format!("/meta/as_of/{numeric_chain_id}"))
        .with_context(|| format!("response must include meta.as_of[{numeric_chain_id}]"))?;
    let block_number = as_of
        .get("block_number")
        .and_then(Value::as_i64)
        .context("meta.as_of block_number must be an i64")?;
    let block_hash = as_of
        .get("block_hash")
        .and_then(Value::as_str)
        .context("meta.as_of block_hash must be a string")?;
    let timestamp = as_of
        .get("timestamp")
        .and_then(Value::as_str)
        .context("meta.as_of timestamp must be a string")?;

    Ok((
        slot.to_owned(),
        ChainPosition {
            slot: slot.to_owned(),
            chain_id: chain_id.to_owned(),
            block_number,
            block_hash: block_hash.to_owned(),
            timestamp: bigname_storage::parse_rfc3339_utc_timestamp(timestamp)
                .map_err(|error| anyhow::anyhow!("{error}"))?,
        },
    ))
}

fn snapshot_position(
    slot: &str,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    unix_seconds: i64,
) -> ChainPosition {
    ChainPosition {
        slot: slot.to_owned(),
        chain_id: chain_id.to_owned(),
        block_number,
        block_hash: block_hash.to_owned(),
        timestamp: timestamp(unix_seconds),
    }
}

async fn search_payload(database: &SearchDatabase, uri: &str) -> Result<Value> {
    let response = search_response(database, uri).await?;
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
    read_json(response).await
}

async fn search_response(database: &SearchDatabase, uri: &str) -> Result<Response> {
    search_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 search request failed")
}

fn search_router(state: crate::AppState) -> Router {
    crate::v2::router().with_state(state)
}

async fn read_json(response: Response) -> Result<Value> {
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .context("failed to read response body")?;
    serde_json::from_slice(&body).context("failed to decode response JSON")
}

struct SearchDatabase {
    database: TestDatabase,
}

impl SearchDatabase {
    async fn new() -> Result<Self> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("bigname_api_v2_search_test")
                .admin_database_from_url()
                .pool_max_connections(1)
                .parse_context("failed to parse database URL for v2 search tests")
                .admin_connect_context("failed to connect admin pool for v2 search tests")
                .pool_connect_context("failed to connect v2 search test pool"),
            &bigname_storage::MIGRATOR,
            "failed to apply migrations for v2 search tests",
        )
        .await?;

        Ok(Self { database })
    }

    fn pool(&self) -> &sqlx::PgPool {
        self.database.pool()
    }

    fn app_state(&self) -> crate::AppState {
        crate::AppState {
            phase: "test",
            pool: self.pool().clone(),
            chain_rpc_urls: bigname_execution::ChainRpcUrls::default(),
        }
    }

    async fn snapshot_token(&self, namespace: &str, at: &str) -> Result<String> {
        let state = self.app_state();
        let at_selector = AtSelector::Timestamp(at.to_owned());
        let scope = v2_exact_name_snapshot_scope(&state, namespace, Some(&at_selector))
            .await
            .map_err(|error| anyhow::anyhow!("{:?}", error.envelope()))?;
        let selected =
            resolve_v2_snapshot(self.pool(), &scope, Some(&at_selector), Finality::Latest)
                .await
                .map_err(|error| anyhow::anyhow!("{:?}", error.envelope()))?;

        Ok(encode_at_token(&selected))
    }

    async fn cleanup(self) -> Result<()> {
        self.database.cleanup().await
    }
}

async fn seed_search_fixture(database: &SearchDatabase) -> Result<()> {
    seed_chain(
        database,
        "ethereum-mainnet",
        "0xeth",
        100,
        110,
        1_776_384_100,
    )
    .await?;
    seed_chain(database, "base-mainnet", "0xbase", 200, 210, 1_776_384_200).await?;

    let specs = search_specs();
    upsert_name_surfaces(
        database.pool(),
        &specs.iter().map(name_surface).collect::<Vec<_>>(),
    )
    .await?;
    upsert_token_lineages(
        database.pool(),
        &specs.iter().map(token_lineage).collect::<Vec<_>>(),
    )
    .await?;
    upsert_resources(
        database.pool(),
        &specs.iter().map(resource).collect::<Vec<_>>(),
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &specs.iter().map(surface_binding).collect::<Vec<_>>(),
    )
    .await?;
    upsert_name_current_rows(
        database.pool(),
        &specs.iter().map(name_current_row).collect::<Vec<_>>(),
    )
    .await?;

    Ok(())
}

async fn seed_chain(
    database: &SearchDatabase,
    chain_id: &str,
    hash_prefix: &str,
    start_block: i64,
    end_block: i64,
    start_timestamp: i64,
) -> Result<()> {
    let mut parent_hash = None;
    let mut blocks = Vec::new();
    for block_number in start_block..=end_block {
        let block_hash = format!("{hash_prefix}{block_number}");
        blocks.push(ChainLineageBlock {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.clone(),
            parent_hash: parent_hash.clone(),
            block_number,
            block_timestamp: timestamp(start_timestamp + block_number - start_block),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Finalized,
        });
        parent_hash = Some(block_hash);
    }
    upsert_chain_lineage_blocks(database.pool(), &blocks).await?;

    advance_chain_checkpoints(
        database.pool(),
        &ChainCheckpointUpdate {
            chain_id: chain_id.to_owned(),
            canonical: Some(block_ref(hash_prefix, end_block)),
            safe: Some(block_ref(hash_prefix, end_block - 1)),
            finalized: Some(block_ref(hash_prefix, end_block - 2)),
        },
    )
    .await?;

    Ok(())
}

fn block_ref(hash_prefix: &str, block_number: i64) -> CheckpointBlockRef {
    CheckpointBlockRef {
        block_hash: format!("{hash_prefix}{block_number}"),
        block_number,
    }
}

fn search_specs() -> Vec<SearchSpec> {
    macro_rules! spec {
        (
            $namespace:literal,
            $name:literal,
            $namehash:literal,
            $id:literal,
            $owner:literal,
            $registrant:literal,
            $registered_at:literal,
            $created_at:literal,
            $expires_at:literal $(,)?
        ) => {
            SearchSpec {
                namespace: $namespace,
                name: $name,
                namehash: $namehash,
                id: $id,
                owner: $owner,
                control_owner: None,
                registrant: $registrant,
                control_registrant: None,
                registered_at: $registered_at,
                created_at: $created_at,
                expires_at: $expires_at,
            }
        };
    }

    vec![
        spec!(
            "ens",
            "alpha.eth",
            "node:alpha.eth",
            0xa100,
            "0x00000000000000000000000000000000000000a1",
            "0x00000000000000000000000000000000000000a2",
            "2024-01-02T00:00:00Z",
            "2023-01-02T00:00:00Z",
            "2027-01-02T00:00:00Z",
        ),
        spec!(
            "ens",
            "alpine.eth",
            "node:alpine.eth",
            0xa200,
            "0x0000000000000000000000000000000000000a21",
            "0x0000000000000000000000000000000000000a22",
            "2024-02-02T00:00:00Z",
            "2023-02-02T00:00:00Z",
            "2027-02-02T00:00:00Z",
        ),
        spec!(
            "ens",
            "gamma.eth",
            "node:gamma.eth",
            0xa300,
            "0x0000000000000000000000000000000000000a31",
            "0x0000000000000000000000000000000000000a32",
            "2024-03-02T00:00:00Z",
            "2023-03-02T00:00:00Z",
            "2027-03-02T00:00:00Z",
        ),
        spec!(
            "ens",
            "percent%name.eth",
            "node:percent-percent-name.eth",
            0xa400,
            "0x0000000000000000000000000000000000000a41",
            "0x0000000000000000000000000000000000000a42",
            "2024-04-02T00:00:00Z",
            "2023-04-02T00:00:00Z",
            "2027-04-02T00:00:00Z",
        ),
        spec!(
            "ens",
            "percentxname.eth",
            "node:percentxname.eth",
            0xa500,
            "0x0000000000000000000000000000000000000a51",
            "0x0000000000000000000000000000000000000a52",
            "2024-05-02T00:00:00Z",
            "2023-05-02T00:00:00Z",
            "2027-05-02T00:00:00Z",
        ),
        spec!(
            "ens",
            "under_score.eth",
            "node:under-score.eth",
            0xa600,
            "0x0000000000000000000000000000000000000a61",
            "0x0000000000000000000000000000000000000a62",
            "2024-06-02T00:00:00Z",
            "2023-06-02T00:00:00Z",
            "2027-06-02T00:00:00Z",
        ),
        spec!(
            "ens",
            "underxscore.eth",
            "node:underxscore.eth",
            0xa700,
            "0x0000000000000000000000000000000000000a71",
            "0x0000000000000000000000000000000000000a72",
            "2024-07-02T00:00:00Z",
            "2023-07-02T00:00:00Z",
            "2027-07-02T00:00:00Z",
        ),
        SearchSpec {
            namespace: "ens",
            name: "precedence.eth",
            namehash: "node:precedence.eth",
            id: 0xd100,
            owner: DIVERGENT_REGISTRY_OWNER,
            control_owner: Some(DIVERGENT_CONTROL_OWNER),
            registrant: DIVERGENT_REGISTRATION_REGISTRANT,
            control_registrant: Some(DIVERGENT_CONTROL_REGISTRANT),
            registered_at: "2024-07-03T00:00:00Z",
            created_at: "2023-07-03T00:00:00Z",
            expires_at: "2027-07-03T00:00:00Z",
        },
        spec!(
            "basenames",
            "alpha.base.eth",
            "node:alpha.base.eth",
            0xb100,
            "0x0000000000000000000000000000000000000b11",
            "0x0000000000000000000000000000000000000b12",
            "2024-08-02T00:00:00Z",
            "2023-08-02T00:00:00Z",
            "2027-08-02T00:00:00Z",
        ),
        spec!(
            "basenames",
            "alpine.base.eth",
            "node:alpine.base.eth",
            0xb200,
            "0x0000000000000000000000000000000000000b21",
            "0x0000000000000000000000000000000000000b22",
            "2024-08-03T00:00:00Z",
            "2023-08-03T00:00:00Z",
            "2027-08-03T00:00:00Z",
        ),
        spec!(
            "internal",
            "alpha.internal",
            "node:alpha.internal",
            0xc100,
            "0x0000000000000000000000000000000000000c11",
            "0x0000000000000000000000000000000000000c12",
            "2024-09-02T00:00:00Z",
            "2023-09-02T00:00:00Z",
            "2027-09-02T00:00:00Z",
        ),
    ]
}

#[derive(Clone)]
struct SearchSpec {
    namespace: &'static str,
    name: &'static str,
    namehash: &'static str,
    id: u128,
    owner: &'static str,
    control_owner: Option<&'static str>,
    registrant: &'static str,
    control_registrant: Option<&'static str>,
    registered_at: &'static str,
    created_at: &'static str,
    expires_at: &'static str,
}

impl SearchSpec {
    fn logical_name_id(&self) -> String {
        format!("{}:{}", self.namespace, self.name)
    }

    fn chain_id(&self) -> &'static str {
        match self.namespace {
            "basenames" => "base-mainnet",
            _ => "ethereum-mainnet",
        }
    }

    fn slot(&self) -> &'static str {
        match self.namespace {
            "basenames" => "base",
            _ => "ethereum",
        }
    }

    fn block_hash(&self) -> String {
        match self.namespace {
            "basenames" => "0xbase208".to_owned(),
            _ => "0xeth108".to_owned(),
        }
    }

    fn block_number(&self) -> i64 {
        match self.namespace {
            "basenames" => 208,
            _ => 108,
        }
    }

    fn block_timestamp(&self) -> &'static str {
        match self.namespace {
            "basenames" => "2026-04-17T00:03:28Z",
            _ => "2026-04-17T00:01:48Z",
        }
    }
}

fn name_surface(spec: &SearchSpec) -> NameSurface {
    NameSurface {
        logical_name_id: spec.logical_name_id(),
        namespace: spec.namespace.to_owned(),
        input_name: spec.name.to_owned(),
        canonical_display_name: spec.name.to_owned(),
        normalized_name: spec.name.to_owned(),
        dns_encoded_name: spec.name.as_bytes().to_vec(),
        namehash: spec.namehash.to_owned(),
        labelhashes: vec![format!("labelhash:{}", spec.name)],
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: spec.chain_id().to_owned(),
        block_hash: spec.block_hash(),
        block_number: spec.block_number(),
        provenance: json!({"seed": "v2_search_surface"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn token_lineage(spec: &SearchSpec) -> TokenLineage {
    TokenLineage {
        token_lineage_id: token_lineage_id(spec),
        chain_id: spec.chain_id().to_owned(),
        block_hash: spec.block_hash(),
        block_number: spec.block_number(),
        provenance: json!({"seed": "v2_search_token_lineage"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn resource(spec: &SearchSpec) -> Resource {
    Resource {
        resource_id: resource_id(spec),
        token_lineage_id: Some(token_lineage_id(spec)),
        chain_id: spec.chain_id().to_owned(),
        block_hash: spec.block_hash(),
        block_number: spec.block_number(),
        provenance: json!({"seed": "v2_search_resource"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn surface_binding(spec: &SearchSpec) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id: surface_binding_id(spec),
        logical_name_id: spec.logical_name_id(),
        resource_id: resource_id(spec),
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: timestamp(1_713_312_300),
        active_to: None,
        chain_id: spec.chain_id().to_owned(),
        block_hash: spec.block_hash(),
        block_number: spec.block_number(),
        provenance: json!({"seed": "v2_search_binding"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn name_current_row(spec: &SearchSpec) -> NameCurrentRow {
    NameCurrentRow {
        logical_name_id: spec.logical_name_id(),
        namespace: spec.namespace.to_owned(),
        canonical_display_name: spec.name.to_owned(),
        normalized_name: spec.name.to_owned(),
        namehash: spec.namehash.to_owned(),
        surface_binding_id: Some(surface_binding_id(spec)),
        resource_id: Some(resource_id(spec)),
        token_lineage_id: Some(token_lineage_id(spec)),
        binding_kind: Some(SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar",
                "registrant": spec.registrant,
                "registered_at": spec.registered_at,
                "created_at": spec.created_at,
                "expiry": spec.expires_at
            },
            "control": {
                "registry_owner": spec.owner,
                "owner": spec.control_owner,
                "registrant": spec.control_registrant.unwrap_or(spec.registrant),
                "expiry": spec.expires_at
            }
        }),
        provenance: json!({"seed": "v2_search_name_current"}),
        coverage: json!({}),
        chain_positions: chain_positions(spec),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_713_312_400),
    }
}

fn resource_id(spec: &SearchSpec) -> Uuid {
    Uuid::from_u128(spec.id)
}

fn token_lineage_id(spec: &SearchSpec) -> Uuid {
    Uuid::from_u128(spec.id + 1)
}

fn surface_binding_id(spec: &SearchSpec) -> Uuid {
    Uuid::from_u128(spec.id + 2)
}

fn chain_positions(spec: &SearchSpec) -> Value {
    let mut positions = serde_json::Map::new();
    positions.insert(
        spec.slot().to_owned(),
        json!({
            "chain_id": spec.chain_id(),
            "block_number": spec.block_number(),
            "block_hash": spec.block_hash(),
            "timestamp": spec.block_timestamp()
        }),
    );
    Value::Object(positions)
}

fn timestamp(unix_seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(unix_seconds)
        .expect("test timestamp must be in range")
        .to_offset(UtcOffset::UTC)
}
