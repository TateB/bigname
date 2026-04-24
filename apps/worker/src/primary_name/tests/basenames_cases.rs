use anyhow::Result;
use bigname_storage::{
    CanonicalityState, PrimaryNameClaimStatus, PrimaryNameCurrentRow, load_primary_name_current,
    load_primary_name_current_snapshot, upsert_normalized_events,
};

use super::super::{PrimaryNamesCurrentRebuildSummary, rebuild_primary_names_current};

use super::support::{
    BASENAMES_NAMESPACE, TestDatabase, basenames_expected_claim_provenance,
    basenames_reverse_changed_event, basenames_reverse_linked_name_event,
};

#[tokio::test]
async fn full_rebuild_projects_basenames_claim_name_from_base_resolver_observation() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    upsert_normalized_events(
        database.pool(),
        &[
            basenames_reverse_changed_event(
                "basenames-reverse-a-60",
                address,
                "60",
                500,
                0,
                CanonicalityState::Canonical,
            ),
            basenames_reverse_linked_name_event(
                "basenames-record-a-60-success",
                address,
                "60",
                Some("alice.base.eth"),
                501,
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
            requested_tuple_count: 1,
            upserted_row_count: 1,
            deleted_row_count: 0,
            success_row_count: 1,
            not_found_row_count: 0,
            invalid_name_row_count: 0,
        }
    );
    assert_eq!(
        load_primary_name_current(database.pool(), address, BASENAMES_NAMESPACE, "60").await?,
        Some(PrimaryNameCurrentRow {
            address: address.to_owned(),
            namespace: BASENAMES_NAMESPACE.to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: basenames_expected_claim_provenance(
                address,
                "60",
                500,
                PrimaryNameClaimStatus::Success,
                Some(501),
            ),
        })
    );
    assert_eq!(
        load_primary_name_current_snapshot(database.pool(), address, BASENAMES_NAMESPACE, "60")
            .await?
            .map(|snapshot| snapshot.normalized_claim_name),
        Some(Some("alice.base.eth".to_owned()))
    );

    database.cleanup().await
}

#[tokio::test]
async fn targeted_rebuild_projects_basenames_claim_name_from_base_resolver_observation()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000def";

    upsert_normalized_events(
        database.pool(),
        &[
            basenames_reverse_changed_event(
                "basenames-reverse-b-60",
                address,
                "60",
                600,
                0,
                CanonicalityState::Canonical,
            ),
            basenames_reverse_linked_name_event(
                "basenames-record-b-60-success",
                address,
                "60",
                Some("bob.base.eth"),
                601,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let summary = rebuild_primary_names_current(
        database.pool(),
        Some(address),
        Some(BASENAMES_NAMESPACE),
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
        load_primary_name_current(database.pool(), address, BASENAMES_NAMESPACE, "60").await?,
        Some(PrimaryNameCurrentRow {
            address: address.to_owned(),
            namespace: BASENAMES_NAMESPACE.to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: basenames_expected_claim_provenance(
                address,
                "60",
                600,
                PrimaryNameClaimStatus::Success,
                Some(601),
            ),
        })
    );
    assert_eq!(
        load_primary_name_current_snapshot(database.pool(), address, BASENAMES_NAMESPACE, "60")
            .await?
            .map(|snapshot| snapshot.normalized_claim_name),
        Some(Some("bob.base.eth".to_owned()))
    );

    database.cleanup().await
}
