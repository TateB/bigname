use std::{
    sync::{Arc, Condvar, Mutex, mpsc},
    time::Duration,
};

use anyhow::{Context, Result};
use bigname_execution::{
    ENS_UNIVERSAL_RESOLVER_ADDRESS, ETHEREUM_MAINNET_CHAIN_ID,
    PersistEnsVerifiedPrimaryNameRequest, persist_ens_verified_primary_name,
};
use bigname_storage::{
    CanonicalityState, ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, ExecutionTraceStep,
    NormalizedEvent, PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot,
    load_execution_outcome, load_primary_name_current, load_primary_name_current_snapshot,
    upsert_execution_outcome, upsert_execution_trace, upsert_normalized_events,
    upsert_primary_name_current_rows, upsert_primary_name_current_snapshots,
};
use serde_json::{Value, json};
use sqlx::types::{Uuid, time::OffsetDateTime};

use super::super::projection::test_hooks;
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
async fn targeted_rebuild_serializes_claim_publish_with_verified_primary_producer() -> Result<()> {
    struct HookState {
        reached: bool,
        release: bool,
    }

    let database = TestDatabase::new_with_max_connections(12).await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let old_snapshot = PrimaryNameCurrentSnapshot {
        row: PrimaryNameCurrentRow {
            address: address.to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: json!({}),
        },
        normalized_claim_name: Some("alice.eth".to_owned()),
    };
    upsert_primary_name_current_snapshots(database.pool(), &[old_snapshot])
        .await
        .context("failed to seed old primary-name anchor for serialization test")?;

    let seeded_request = persistable_verified_primary_request(
        Uuid::from_u128(0x5e7100000000000000000000000100a1),
        address,
        "60",
        "alice.eth",
        1_717_180_101,
    );
    persist_ens_verified_primary_name(database.pool(), &seeded_request)
        .await
        .context("failed to seed old verified-primary outcome through producer")?;
    assert!(
        load_execution_outcome(database.pool(), &seeded_request.outcome.cache_key)
            .await?
            .is_some(),
        "seeded verified-primary outcome must exist before claim-change rebuild"
    );

    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event(
                "reverse-a-60-serialize",
                address,
                "60",
                330,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_linked_name_event(
                "record-a-60-serialize",
                address,
                "60",
                Some("bob.eth"),
                331,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let hook_state = Arc::new((
        Mutex::new(HookState {
            reached: false,
            release: false,
        }),
        Condvar::new(),
    ));
    let hook_state_for_hook = Arc::clone(&hook_state);
    let _hook_guard = test_hooks::install_targeted_rebuild_after_invalidation_hook(Arc::new(
        move |hook_address, hook_namespace, hook_coin_type| {
            if hook_address != address || hook_namespace != "ens" || hook_coin_type != "60" {
                return;
            }
            let (lock, condvar) = &*hook_state_for_hook;
            let mut state = lock
                .lock()
                .expect("targeted rebuild hook state mutex poisoned");
            state.reached = true;
            condvar.notify_all();
            while !state.release {
                state = condvar
                    .wait(state)
                    .expect("targeted rebuild hook state mutex poisoned while waiting");
            }
        },
    ));

    let worker_pool = database.pool().clone();
    let worker_address = address.to_owned();
    let worker_thread = std::thread::spawn(move || -> Result<PrimaryNamesCurrentRebuildSummary> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to build primary-name rebuild test runtime")?;
        runtime
            .block_on(rebuild_primary_names_current(
                &worker_pool,
                Some(&worker_address),
                Some("ens"),
                Some("60"),
            ))
            .context("targeted primary-name rebuild failed in serialization test")
    });

    let hook_state_for_wait = Arc::clone(&hook_state);
    tokio::task::spawn_blocking(move || -> Result<()> {
        let (lock, condvar) = &*hook_state_for_wait;
        let mut state = lock
            .lock()
            .expect("targeted rebuild hook state mutex poisoned");
        while !state.reached {
            state = condvar
                .wait(state)
                .expect("targeted rebuild hook state mutex poisoned while waiting");
        }
        Ok(())
    })
    .await
    .context("targeted rebuild hook wait task panicked")??;
    let stale_request = persistable_verified_primary_request(
        Uuid::from_u128(0x5e7100000000000000000000000100a2),
        address,
        "60",
        "alice.eth",
        1_717_180_102,
    );
    let stale_cache_key = stale_request.outcome.cache_key.clone();
    let (producer_tx, producer_rx) = mpsc::channel();
    let producer_pool = database.pool().clone();
    let producer_thread = std::thread::spawn(move || {
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| anyhow::anyhow!(error))
            .and_then(|runtime| {
                runtime
                    .block_on(persist_ens_verified_primary_name(
                        &producer_pool,
                        &stale_request,
                    ))
                    .map_err(|error| anyhow::anyhow!(error))
            })
            .map_err(|error| format!("{error:#}"));
        producer_tx
            .send(result)
            .expect("producer result receiver must stay open");
    });
    let early_producer_result = producer_rx.recv_timeout(Duration::from_secs(2)).ok();

    {
        let (lock, condvar) = &*hook_state;
        let mut state = lock
            .lock()
            .expect("targeted rebuild hook state mutex poisoned");
        state.release = true;
        condvar.notify_all();
    }

    let worker_summary = tokio::task::spawn_blocking(move || {
        worker_thread
            .join()
            .map_err(|_| anyhow::anyhow!("primary-name rebuild test thread panicked"))?
    })
    .await
    .context("primary-name rebuild join task panicked")??;
    assert_eq!(worker_summary.upserted_row_count, 1);
    let producer_result = match early_producer_result {
        Some(result) => result,
        None => producer_rx
            .recv_timeout(Duration::from_secs(5))
            .context("producer did not finish after targeted rebuild publish")?,
    };
    producer_thread
        .join()
        .map_err(|_| anyhow::anyhow!("verified-primary producer test thread panicked"))?;

    assert!(
        producer_result.is_err()
            || load_execution_outcome(database.pool(), &stale_cache_key)
                .await?
                .is_none(),
        "stale producer outcome survived claim-change publish: {producer_result:?}"
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &stale_cache_key).await?,
        None,
        "claim-change publish must not leave stale verified-primary outcome reusable"
    );
    assert_eq!(
        load_primary_name_current_snapshot(database.pool(), address, "ens", "60")
            .await?
            .and_then(|snapshot| snapshot.normalized_claim_name),
        Some("bob.eth".to_owned())
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

fn persistable_verified_primary_request(
    execution_trace_id: Uuid,
    address: &str,
    coin_type: &str,
    verified_name: &str,
    finished_at: i64,
) -> PersistEnsVerifiedPrimaryNameRequest {
    let mut trace = verified_primary_trace(execution_trace_id, address, coin_type, finished_at);
    trace.contracts_called = json!([{
        "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
        "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
        "selector": "0x9061b923",
    }]);
    trace.steps.push(ExecutionTraceStep {
        step_index: 1,
        step_kind: "call_universal_resolver".to_owned(),
        input_digest: Some("sha256:primary-forward-input".to_owned()),
        output_digest: Some("sha256:primary-forward-output".to_owned()),
        latency_ms: Some(8),
        canonicality_dependency: json!({
            ETHEREUM_MAINNET_CHAIN_ID: {
                "block_hash": "0xprimary",
                "block_number": 21_000_010,
                "state": "finalized"
            }
        }),
        step_payload: json!({
            "name": verified_name,
            "coin_type": coin_type
        }),
    });
    let outcome = verified_primary_outcome(&trace);
    trace.request_metadata["cache_identity"] = json!({
        "requested_chain_positions": outcome.cache_key.requested_chain_positions.clone(),
        "manifest_versions": outcome.cache_key.manifest_versions.clone(),
        "topology_version_boundary": outcome.cache_key.topology_version_boundary.clone(),
        "record_version_boundary": outcome.cache_key.record_version_boundary.clone(),
    });

    PersistEnsVerifiedPrimaryNameRequest { trace, outcome }
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
