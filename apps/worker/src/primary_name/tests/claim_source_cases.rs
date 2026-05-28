use anyhow::Result;
use bigname_storage::{
    CanonicalityState, PrimaryNameClaimStatus, PrimaryNameCurrentRow, load_primary_name_current,
    load_primary_name_current_snapshot, upsert_normalized_events,
};

use super::super::{PrimaryNamesCurrentRebuildSummary, rebuild_primary_names_current};

use super::support::{
    TestDatabase, expected_claim_provenance, reverse_changed_event, reverse_linked_name_event,
};

#[tokio::test]
async fn targeted_rebuild_rejects_invalid_claim_name_source() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event(
                "reverse-a-60",
                "0x0000000000000000000000000000000000000aaa",
                "60",
                101,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_linked_name_event(
                "record-a-60-invalid-name",
                "0x0000000000000000000000000000000000000aaa",
                "60",
                Some("Ni\u{200d}ck.eth"),
                201,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let summary = rebuild_primary_names_current(
        database.pool(),
        Some("0x0000000000000000000000000000000000000aaa"),
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
            "0x0000000000000000000000000000000000000aaa",
            "ens",
            "60",
        )
        .await?,
        Some(PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000aaa".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::InvalidName,
            raw_claim_name: Some("Ni\u{200d}ck.eth".to_owned()),
            claim_provenance: expected_claim_provenance(
                "0x0000000000000000000000000000000000000aaa",
                "60",
                101,
                PrimaryNameClaimStatus::InvalidName,
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
        Some(None)
    );

    database.cleanup().await
}

#[tokio::test]
async fn targeted_rebuild_projects_declared_claim_name_source_for_success_rows() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event(
                "reverse-a-60",
                "0x0000000000000000000000000000000000000abc",
                "60",
                250,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_linked_name_event(
                "record-a-60-success",
                "0x0000000000000000000000000000000000000abc",
                "60",
                Some("alice.eth"),
                251,
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
            success_row_count: 1,
            not_found_row_count: 0,
            invalid_name_row_count: 0,
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
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: expected_claim_provenance(
                "0x0000000000000000000000000000000000000abc",
                "60",
                250,
                PrimaryNameClaimStatus::Success,
                Some(251),
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
        Some(Some("alice.eth".to_owned()))
    );

    database.cleanup().await
}

#[tokio::test]
async fn targeted_rebuild_keeps_primary_claim_source_hook_for_not_found_rows() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event(
                "reverse-a-60",
                "0x0000000000000000000000000000000000000abc",
                "60",
                400,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_linked_name_event(
                "record-a-60-empty",
                "0x0000000000000000000000000000000000000abc",
                "60",
                None,
                401,
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
            not_found_row_count: 1,
            invalid_name_row_count: 0,
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
            claim_status: PrimaryNameClaimStatus::NotFound,
            raw_claim_name: None,
            claim_provenance: expected_claim_provenance(
                "0x0000000000000000000000000000000000000abc",
                "60",
                400,
                PrimaryNameClaimStatus::NotFound,
                Some(401),
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

#[tokio::test]
async fn targeted_rebuild_treats_blank_claim_name_source_as_not_found() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event(
                "reverse-a-60",
                "0x0000000000000000000000000000000000000def",
                "60",
                500,
                0,
                CanonicalityState::Canonical,
            ),
            reverse_linked_name_event(
                "record-a-60-blank",
                "0x0000000000000000000000000000000000000def",
                "60",
                Some("   "),
                501,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let summary = rebuild_primary_names_current(
        database.pool(),
        Some("0x0000000000000000000000000000000000000def"),
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
            not_found_row_count: 1,
            invalid_name_row_count: 0,
        }
    );
    assert_eq!(
        load_primary_name_current(
            database.pool(),
            "0x0000000000000000000000000000000000000def",
            "ens",
            "60",
        )
        .await?,
        Some(PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000def".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::NotFound,
            raw_claim_name: None,
            claim_provenance: expected_claim_provenance(
                "0x0000000000000000000000000000000000000def",
                "60",
                500,
                PrimaryNameClaimStatus::NotFound,
                Some(501),
            ),
        })
    );

    database.cleanup().await
}
