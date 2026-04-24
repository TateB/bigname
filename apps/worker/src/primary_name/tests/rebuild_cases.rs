use anyhow::Result;
use bigname_storage::{
    CanonicalityState, NormalizedEvent, PrimaryNameClaimStatus, PrimaryNameCurrentRow,
    load_primary_name_current, load_primary_name_current_snapshot, upsert_normalized_events,
    upsert_primary_name_current_rows,
};
use serde_json::json;

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
