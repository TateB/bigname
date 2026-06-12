use anyhow::Result;
use bigname_storage::{
    CanonicalityState, ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, ExecutionTraceStep,
    NormalizedEvent, PrimaryNameClaimStatus, PrimaryNameCurrentRow, load_execution_outcome,
    load_primary_name_current, load_primary_name_current_snapshot, upsert_execution_outcome,
    upsert_execution_trace, upsert_normalized_events, upsert_primary_name_current_rows,
};
use serde_json::{Value, json};
use sqlx::types::{Uuid, time::OffsetDateTime};

use super::super::{PrimaryNamesCurrentRebuildSummary, rebuild_primary_names_current};

use super::support::{
    TestDatabase, expected_claim_provenance, reverse_changed_event, reverse_linked_name_event,
};

#[tokio::test]
async fn full_rebuild_projects_declared_claim_status_rows() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event(
                "reverse-a-60-canonical",
                "0x0000000000000000000000000000000000000aAa",
                "60",
                100,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_changed_event(
                "reverse-a-60-finalized",
                "0x0000000000000000000000000000000000000aaa",
                "60",
                101,
                0,
                CanonicalityState::Finalized,
            ),
            reverse_changed_event(
                "reverse-a-61-safe",
                "0x0000000000000000000000000000000000000aaa",
                "61",
                102,
                0,
                CanonicalityState::Safe,
            ),
            reverse_changed_event(
                "reverse-b-60-canonical",
                "0x0000000000000000000000000000000000000bbb",
                "60",
                103,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_changed_event(
                "reverse-orphaned",
                "0x0000000000000000000000000000000000000ccc",
                "60",
                104,
                0,
                CanonicalityState::Orphaned,
            ),
            NormalizedEvent {
                event_identity: "not-reverse".to_owned(),
                event_kind: "ResolverChanged".to_owned(),
                ..reverse_changed_event(
                    "not-reverse-base",
                    "0x0000000000000000000000000000000000000ddd",
                    "60",
                    105,
                    0,
                    CanonicalityState::Canonical,
                )
            },
            reverse_linked_name_event(
                "record-a-60-success",
                "0x0000000000000000000000000000000000000aaa",
                "60",
                Some("Alice.eth"),
                201,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_linked_name_event(
                "record-b-60-invalid",
                "0x0000000000000000000000000000000000000bbb",
                "60",
                Some("alice..eth"),
                202,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let summary = rebuild_primary_names_current(database.pool(), None, None, None).await?;
    assert_eq!(
        summary,
        PrimaryNamesCurrentRebuildSummary {
            requested_tuple_count: 3,
            upserted_row_count: 3,
            deleted_row_count: 0,
            success_row_count: 1,
            not_found_row_count: 1,
            invalid_name_row_count: 1,
        }
    );

    assert_eq!(
        load_primary_name_current(
            database.pool(),
            "0x0000000000000000000000000000000000000aaa",
            "ens",
            "60",
        )
        .await?,
        Some(PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000aaa".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: expected_claim_provenance(
                "0x0000000000000000000000000000000000000aaa",
                "60",
                101,
                PrimaryNameClaimStatus::Success,
                Some(201),
            ),
        })
    );
    assert_eq!(
        load_primary_name_current_snapshot(
            database.pool(),
            "0x0000000000000000000000000000000000000aaa",
            "ens",
            "60",
        )
        .await?
        .map(|snapshot| snapshot.normalized_claim_name),
        Some(Some("alice.eth".to_owned()))
    );
    assert_eq!(
        load_primary_name_current(
            database.pool(),
            "0x0000000000000000000000000000000000000aaa",
            "ens",
            "61",
        )
        .await?,
        Some(PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000aaa".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "61".to_owned(),
            claim_status: PrimaryNameClaimStatus::NotFound,
            raw_claim_name: None,
            claim_provenance: expected_claim_provenance(
                "0x0000000000000000000000000000000000000aaa",
                "61",
                102,
                PrimaryNameClaimStatus::NotFound,
                None,
            ),
        })
    );
    assert_eq!(
        load_primary_name_current_snapshot(
            database.pool(),
            "0x0000000000000000000000000000000000000aaa",
            "ens",
            "61",
        )
        .await?
        .map(|snapshot| snapshot.normalized_claim_name),
        Some(None)
    );
    assert_eq!(
        load_primary_name_current(
            database.pool(),
            "0x0000000000000000000000000000000000000bbb",
            "ens",
            "60",
        )
        .await?,
        Some(PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000bbb".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::InvalidName,
            raw_claim_name: Some("alice..eth".to_owned()),
            claim_provenance: expected_claim_provenance(
                "0x0000000000000000000000000000000000000bbb",
                "60",
                103,
                PrimaryNameClaimStatus::InvalidName,
                Some(202),
            ),
        })
    );
    assert_eq!(
        load_primary_name_current_snapshot(
            database.pool(),
            "0x0000000000000000000000000000000000000bbb",
            "ens",
            "60",
        )
        .await?
        .map(|snapshot| snapshot.normalized_claim_name),
        Some(None)
    );
    assert!(
        load_primary_name_current(
            database.pool(),
            "0x0000000000000000000000000000000000000ccc",
            "ens",
            "60",
        )
        .await?
        .is_none()
    );
    assert!(
        load_primary_name_current(
            database.pool(),
            "0x0000000000000000000000000000000000000ddd",
            "ens",
            "60",
        )
        .await?
        .is_none()
    );

    database.cleanup().await
}

#[tokio::test]
async fn targeted_rebuild_deletes_stale_tuple_when_no_reverse_event_exists() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_primary_name_current_rows(
        database.pool(),
        &[PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: json!({
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
            }),
        }],
    )
    .await?;

    let summary = rebuild_primary_names_current(
        database.pool(),
        Some("0x0000000000000000000000000000000000000abc"),
        Some("ens"),
        Some("60"),
    )
    .await?;
    assert_eq!(
        summary,
        PrimaryNamesCurrentRebuildSummary {
            requested_tuple_count: 1,
            upserted_row_count: 0,
            deleted_row_count: 1,
            success_row_count: 0,
            not_found_row_count: 0,
            invalid_name_row_count: 0,
        }
    );
    assert!(
        load_primary_name_current(
            database.pool(),
            "0x0000000000000000000000000000000000000abc",
            "ens",
            "60",
        )
        .await?
        .is_none()
    );

    database.cleanup().await
}

#[tokio::test]
async fn targeted_rebuild_invalidates_verified_primary_cache_on_claim_create_update_and_removal()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    let create_outcome = seed_verified_primary_outcome(
        &database,
        Uuid::from_u128(0x5e710000000000000000000000010001),
        address,
        "60",
        1_717_180_001,
    )
    .await?;
    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event(
                "reverse-a-60-create",
                address,
                "60",
                300,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_linked_name_event(
                "record-a-60-create",
                address,
                "60",
                Some("alice.eth"),
                301,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    rebuild_primary_names_current(database.pool(), Some(address), Some("ens"), Some("60")).await?;
    assert_eq!(
        load_execution_outcome(database.pool(), &create_outcome.cache_key).await?,
        None
    );

    let update_outcome = seed_verified_primary_outcome(
        &database,
        Uuid::from_u128(0x5e710000000000000000000000010002),
        address,
        "60",
        1_717_180_002,
    )
    .await?;
    upsert_normalized_events(
        database.pool(),
        &[reverse_linked_name_event(
            "record-a-60-update",
            address,
            "60",
            Some("alice..eth"),
            302,
            0,
            CanonicalityState::Canonical,
        )],
    )
    .await?;

    rebuild_primary_names_current(database.pool(), Some(address), Some("ens"), Some("60")).await?;
    assert_eq!(
        load_execution_outcome(database.pool(), &update_outcome.cache_key).await?,
        None
    );

    let removal_outcome = seed_verified_primary_outcome(
        &database,
        Uuid::from_u128(0x5e710000000000000000000000010003),
        address,
        "60",
        1_717_180_003,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE normalized_events
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE LOWER(after_state->>'address') = $1
           OR LOWER(after_state->'primary_claim_source'->>'address') = $1
        "#,
    )
    .bind(address)
    .execute(database.pool())
    .await?;

    rebuild_primary_names_current(database.pool(), Some(address), Some("ens"), Some("60")).await?;
    assert_eq!(
        load_execution_outcome(database.pool(), &removal_outcome.cache_key).await?,
        None
    );
    assert!(
        load_primary_name_current(database.pool(), address, "ens", "60")
            .await?
            .is_none()
    );

    database.cleanup().await
}

#[tokio::test]
async fn targeted_rebuild_preserves_verified_primary_cache_when_claim_row_is_unchanged()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event(
                "reverse-a-60-noop",
                address,
                "60",
                309,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_linked_name_event(
                "record-a-60-noop",
                address,
                "60",
                Some("alice.eth"),
                310,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), Some(address), Some("ens"), Some("60")).await?;

    let outcome = seed_verified_primary_outcome(
        &database,
        Uuid::from_u128(0x5e710000000000000000000000010004),
        address,
        "60",
        1_717_180_004,
    )
    .await?;

    rebuild_primary_names_current(database.pool(), Some(address), Some("ens"), Some("60")).await?;

    assert_eq!(
        load_execution_outcome(database.pool(), &outcome.cache_key).await?,
        Some(outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn full_rebuild_invalidates_changed_verified_primary_cache_without_touching_unchanged_tuples()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let target = "0x0000000000000000000000000000000000000abc";
    let sibling = "0x0000000000000000000000000000000000000def";

    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event(
                "full-reverse-target",
                target,
                "60",
                310,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_linked_name_event(
                "full-record-target-old",
                target,
                "60",
                Some("alice.eth"),
                311,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_changed_event(
                "full-reverse-sibling",
                sibling,
                "60",
                320,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_linked_name_event(
                "full-record-sibling",
                sibling,
                "60",
                Some("bob.eth"),
                321,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let target_outcome = seed_verified_primary_outcome(
        &database,
        Uuid::from_u128(0x5e710000000000000000000000020001),
        target,
        "60",
        1_717_180_101,
    )
    .await?;
    let sibling_outcome = seed_verified_primary_outcome(
        &database,
        Uuid::from_u128(0x5e710000000000000000000000020002),
        sibling,
        "60",
        1_717_180_102,
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[reverse_linked_name_event(
            "full-record-target-new",
            target,
            "60",
            Some("alice..eth"),
            312,
            0,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    assert_eq!(
        load_execution_outcome(database.pool(), &target_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &sibling_outcome.cache_key).await?,
        Some(sibling_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn targeted_rebuild_projects_invalid_name_from_latest_reverse_linked_observation()
-> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event(
                "reverse-a-60",
                "0x0000000000000000000000000000000000000abc",
                "60",
                300,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_linked_name_event(
                "record-a-60-old-success",
                "0x0000000000000000000000000000000000000abc",
                "60",
                Some("alice.eth"),
                301,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_linked_name_event(
                "record-a-60-new-invalid",
                "0x0000000000000000000000000000000000000abc",
                "60",
                Some("alice..eth"),
                302,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let summary = rebuild_primary_names_current(
        database.pool(),
        Some("0x0000000000000000000000000000000000000abc"),
        Some("ens"),
        Some("60"),
    )
    .await?;
    assert_eq!(
        summary,
        PrimaryNamesCurrentRebuildSummary {
            requested_tuple_count: 1,
            upserted_row_count: 1,
            deleted_row_count: 0,
            success_row_count: 0,
            not_found_row_count: 0,
            invalid_name_row_count: 1,
        }
    );
    assert_eq!(
        load_primary_name_current(
            database.pool(),
            "0x0000000000000000000000000000000000000abc",
            "ens",
            "60",
        )
        .await?,
        Some(PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::InvalidName,
            raw_claim_name: Some("alice..eth".to_owned()),
            claim_provenance: expected_claim_provenance(
                "0x0000000000000000000000000000000000000abc",
                "60",
                300,
                PrimaryNameClaimStatus::InvalidName,
                Some(302),
            ),
        })
    );
    assert_eq!(
        load_primary_name_current_snapshot(
            database.pool(),
            "0x0000000000000000000000000000000000000abc",
            "ens",
            "60",
        )
        .await?
        .map(|snapshot| snapshot.normalized_claim_name),
        Some(None)
    );

    database.cleanup().await
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

fn verified_primary_request_key(address: &str, coin_type: &str) -> String {
    format!("ens:{}:{coin_type}", address.to_ascii_lowercase())
}

async fn seed_verified_primary_outcome(
    database: &TestDatabase,
    execution_trace_id: Uuid,
    address: &str,
    coin_type: &str,
    finished_at: i64,
) -> Result<ExecutionOutcome> {
    let trace = verified_primary_trace(execution_trace_id, address, coin_type, finished_at);
    let outcome = verified_primary_outcome(&trace);
    upsert_execution_trace(database.pool(), &trace).await?;
    upsert_execution_outcome(database.pool(), &outcome).await?;
    Ok(outcome)
}

fn verified_primary_trace(
    execution_trace_id: Uuid,
    address: &str,
    coin_type: &str,
    finished_at: i64,
) -> ExecutionTrace {
    let normalized_address = address.to_ascii_lowercase();
    let request_key = verified_primary_request_key(&normalized_address, coin_type);
    ExecutionTrace {
        execution_trace_id,
        request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
        request_key: request_key.clone(),
        namespace: "ens".to_owned(),
        chain_context: json!({
            "requested_positions": verified_primary_requested_chain_positions(),
        }),
        manifest_context: json!({
            "manifest_versions": verified_primary_manifest_versions(),
        }),
        contracts_called: json!([]),
        gateway_digests: json!([]),
        final_payload: Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "ens:alice.eth",
                    "namespace": "ens",
                    "normalized_name": "alice.eth",
                    "canonical_display_name": "alice.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123"
                }
            }
        })),
        failure_payload: None,
        request_metadata: json!({
            "normalized_address": normalized_address,
            "namespace": "ens",
            "coin_type": coin_type,
        }),
        finished_at: Some(timestamp(finished_at)),
        steps: vec![ExecutionTraceStep {
            step_index: 0,
            step_kind: "load_primary_name_claim".to_owned(),
            input_digest: Some("sha256:primary-claim-input".to_owned()),
            output_digest: Some("sha256:primary-claim-output".to_owned()),
            latency_ms: Some(2),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xprimary",
                    "block_number": 21_000_010,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "address": address,
                "coin_type": coin_type
            }),
        }],
    }
}

fn verified_primary_outcome(trace: &ExecutionTrace) -> ExecutionOutcome {
    ExecutionOutcome {
        cache_key: ExecutionCacheKey {
            request_key: trace.request_key.clone(),
            requested_chain_positions: verified_primary_requested_chain_positions(),
            manifest_versions: verified_primary_manifest_versions(),
            topology_version_boundary: verified_primary_boundary("ens:alice.eth"),
            record_version_boundary: verified_primary_boundary("ens:alice.eth"),
        },
        execution_trace_id: trace.execution_trace_id,
        request_type: trace.request_type.clone(),
        namespace: trace.namespace.clone(),
        outcome_payload: trace.final_payload.clone(),
        failure_payload: None,
        finished_at: trace.finished_at.expect("test trace must finish"),
    }
}

fn verified_primary_requested_chain_positions() -> Value {
    json!([{
        "chain_id": "ethereum-mainnet",
        "block_number": 21_000_010,
        "block_hash": "0xprimary",
    }])
}

fn verified_primary_manifest_versions() -> Value {
    json!([{
        "source_family": "ens_execution",
        "manifest_version": 3,
    }])
}

fn verified_primary_boundary(logical_name_id: &str) -> Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": "00000000-0000-0000-0000-00000000b001",
        "normalized_event_id": null,
        "event_kind": null,
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_010,
            "block_hash": "0xprimary",
            "timestamp": "2026-04-17T00:00:10Z"
        }
    })
}
