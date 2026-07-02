use bigname_storage::PrimaryNameCurrentRow;
use serde_json::json;

use crate::{PersistedPrimaryNameVerifiedReadback, v2::ErrorCode};

use super::*;

#[test]
fn builder_returns_indexed_then_verified_answers_for_both_sources() {
    let lookup_state = PrimaryNameLookupState {
        tuple_state: PrimaryNameTupleState::TuplePresent(PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: json!({}),
        }),
        normalized_claim_name: Some("alice.eth".to_owned()),
        on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
        on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
        persisted_verified: Some(PersistedPrimaryNameVerifiedReadback {
            verified_primary_name: json!({
                "status": "mismatch",
                "name": {
                    "logical_name_id": "ens:alice.eth",
                    "normalized_name": "alice.eth",
                    "resource_id": "00000000-0000-0000-0000-000000000456"
                },
                "failure_reason": "resolved_target_mismatch"
            }),
            provenance: json!({}),
            finished_at: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
        }),
    };

    let response = build_primary_name(
        "0x0000000000000000000000000000000000000abc".to_owned(),
        "ens".to_owned(),
        60,
        PrimaryNameSourceSelection::Both,
        &lookup_state,
    )
    .expect("primary-name response must build");

    assert_eq!(
        response.answers,
        vec![
            PrimaryNameAnswer::named(Source::Indexed, Status::Ok, "alice.eth"),
            PrimaryNameAnswer {
                failure_reason: Some("resolved_target_mismatch".to_owned()),
                ..PrimaryNameAnswer::named(Source::Verified, Status::Mismatch, "alice.eth")
            },
        ]
    );
    assert_eq!(
        response.verification,
        Some(PrimaryNameVerification {
            status: Status::Mismatch,
            name: Some("alice.eth".to_owned()),
            unsupported_reason: None,
            failure_reason: Some("resolved_target_mismatch".to_owned()),
        })
    );
}

#[test]
fn builder_narrows_answers_to_requested_source() {
    let lookup_state = PrimaryNameLookupState {
        tuple_state: PrimaryNameTupleState::TupleMissing,
        normalized_claim_name: None,
        on_demand_claim: OnDemandPrimaryNameClaimState::NotFound,
        on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
        persisted_verified: None,
    };

    let indexed = build_primary_name(
        "0x0000000000000000000000000000000000000abc".to_owned(),
        "ens".to_owned(),
        60,
        PrimaryNameSourceSelection::Indexed,
        &lookup_state,
    )
    .expect("indexed primary-name response must build");
    let verified = build_primary_name(
        "0x0000000000000000000000000000000000000abc".to_owned(),
        "ens".to_owned(),
        60,
        PrimaryNameSourceSelection::Verified,
        &lookup_state,
    )
    .expect("verified primary-name response must build");

    assert_eq!(
        indexed.answers,
        vec![PrimaryNameAnswer::new(Source::Indexed, Status::NotFound)]
    );
    assert_eq!(
        verified.answers,
        vec![PrimaryNameAnswer::new(Source::Verified, Status::NotFound)]
    );
    assert_eq!(indexed.verification, None);
    assert_eq!(verified.verification, None);
}

#[test]
fn verified_primary_name_unsupported_reason_is_required_and_mapped() {
    let lookup_state = PrimaryNameLookupState {
        tuple_state: PrimaryNameTupleState::TupleMissing,
        normalized_claim_name: None,
        on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
        on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
        persisted_verified: None,
    };

    let missing = verified_answer_from_value(
        &json!({
            "status": "unsupported"
        }),
        &lookup_state,
    )
    .expect("missing unsupported reason must map to product default");
    assert_eq!(missing.status, Status::Unsupported);
    assert_eq!(
        missing.unsupported_reason,
        Some("unsupported_reason_missing".to_owned())
    );

    let mapped = verified_answer_from_value(
        &json!({
            "status": "unsupported",
            "unsupported_reason": "ensv2_exact_name_profile_shadow"
        }),
        &lookup_state,
    )
    .expect("known stored reason must map to product vocabulary");
    assert_eq!(
        mapped.unsupported_reason,
        Some("exact_name_profile_not_supported".to_owned())
    );
}

#[test]
fn verified_primary_name_rejects_unmapped_pipeline_reason() {
    let lookup_state = PrimaryNameLookupState {
        tuple_state: PrimaryNameTupleState::TupleMissing,
        normalized_claim_name: None,
        on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
        on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
        persisted_verified: None,
    };

    let error = verified_answer_from_value(
        &json!({
            "status": "unsupported",
            "unsupported_reason": "primary_names_current_projection_missing"
        }),
        &lookup_state,
    )
    .expect_err("pipeline vocabulary must fail closed");

    assert_eq!(error.code(), ErrorCode::InternalError);
    assert_eq!(
        error.envelope().error.message,
        "failed to map primary-name reason vocabulary"
    );
}

#[test]
fn primary_name_params_default_tuple_and_source_subset() {
    let defaulted = PrimaryNameQueryParams::try_from(RawQueryParams::default())
        .expect("default query must parse");
    assert_eq!(defaulted.namespace, "ens");
    assert_eq!(defaulted.coin_type, "60");
    assert_eq!(defaulted.source, PrimaryNameSourceSelection::Both);

    let indexed = PrimaryNameQueryParams::try_from(RawQueryParams {
        namespace: Some("ens".to_owned()),
        coin_type: Some("060".to_owned()),
        source: Some("indexed".to_owned()),
        ..RawQueryParams::default()
    })
    .expect("indexed primary-name source must parse");
    assert_eq!(indexed.coin_type, "60");
    assert_eq!(indexed.source, PrimaryNameSourceSelection::Indexed);

    let verified = PrimaryNameQueryParams::try_from(RawQueryParams {
        source: Some("verified".to_owned()),
        ..RawQueryParams::default()
    })
    .expect("verified primary-name source must parse");
    assert_eq!(verified.source, PrimaryNameSourceSelection::Verified);
}

#[test]
fn primary_name_params_reject_auto_and_snapshot_controls() {
    for raw in [
        RawQueryParams {
            source: Some("auto".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            at: Some("2026-06-10T00:00:00Z".to_owned()),
            ..RawQueryParams::default()
        },
        RawQueryParams {
            finality: Some("safe".to_owned()),
            ..RawQueryParams::default()
        },
    ] {
        let error =
            PrimaryNameQueryParams::try_from(raw).expect_err("bad primary-name query must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }
}
