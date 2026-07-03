use super::*;

fn row(raw_code_hash_id: i64, address: &str, code_hash: &str) -> RawCodeHashCorrectionCandidate {
    RawCodeHashCorrectionCandidate {
        raw_code_hash_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0x{raw_code_hash_id:064x}"),
        block_number: raw_code_hash_id,
        contract_address: address.to_owned(),
        code_hash: code_hash.to_owned(),
        code_byte_length: 32,
    }
}

fn derived(code_hash: &str) -> DerivedCodeHash {
    derived_with_len(code_hash, 31)
}

fn derived_with_len(code_hash: &str, code_byte_length: i64) -> DerivedCodeHash {
    DerivedCodeHash {
        code_hash: code_hash.to_owned(),
        code_byte_length,
    }
}

fn verified_update(
    raw_code_hash_id: i64,
    address: &str,
    block_number: i64,
) -> VerifiedCorrectionUpdate {
    VerifiedCorrectionUpdate {
        update: RawCodeHashCorrectionUpdate {
            raw_code_hash_id,
            stored_code_hash: "0x1111111111111111111111111111111111111111111111111111111111111111"
                .to_owned(),
            stored_code_byte_length: 32,
            corrected_code_hash:
                "0x2222222222222222222222222222222222222222222222222222222222222222".to_owned(),
            corrected_code_byte_length: 30,
        },
        block_hash: format!("0x{raw_code_hash_id:064x}"),
        block_number,
        contract_address: address.to_owned(),
    }
}

#[test]
fn classification_samples_at_least_one_percent_and_each_address() -> Result<()> {
    let variants = BTreeMap::new();
    let mut accumulator = ClassificationAccumulator::new(&variants, 1.0)?;
    for index in 0_i64..101 {
        let address = if index < 100 {
            "0x0000000000000000000000000000000000000001"
        } else {
            "0x0000000000000000000000000000000000000002"
        };
        accumulator.observe(
            &row(
                index + 1,
                address,
                "0x1111111111111111111111111111111111111111111111111111111111111111",
            ),
            &derived("0x2222222222222222222222222222222222222222222222222222222222222222"),
        )?;
    }

    let outcome = accumulator.finish();
    assert_eq!(outcome.scanned_count, 101);
    assert_eq!(outcome.samples.len(), 2);
    assert!(
        outcome
            .samples
            .iter()
            .any(|sample| sample.contract_address.ends_with("0002"))
    );
    Ok(())
}

#[test]
fn classification_flags_hash_outside_multi_variant_family() -> Result<()> {
    let address = "0x0000000000000000000000000000000000000001".to_owned();
    let variants = BTreeMap::from([(
        address.clone(),
        RawCodeHashAddressVariant {
            contract_address: address.clone(),
            code_hashes: vec![
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_owned(),
                "0x2222222222222222222222222222222222222222222222222222222222222222".to_owned(),
            ],
            row_count: 2,
        },
    )]);
    let mut accumulator = ClassificationAccumulator::new(&variants, 1.0)?;
    accumulator.observe(
        &row(
            1,
            &address,
            "0x1111111111111111111111111111111111111111111111111111111111111111",
        ),
        &derived("0x3333333333333333333333333333333333333333333333333333333333333333"),
    )?;

    let outcome = accumulator.finish();
    assert_eq!(outcome.unexpected_variant_count, 1);
    assert_eq!(outcome.unexpected_variant_examples.len(), 1);
    Ok(())
}

#[test]
fn classification_samples_every_out_of_family_variant_row() -> Result<()> {
    let address = "0x0000000000000000000000000000000000000001".to_owned();
    let variants = BTreeMap::from([(
        address.clone(),
        RawCodeHashAddressVariant {
            contract_address: address.clone(),
            code_hashes: vec![
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_owned(),
                "0x2222222222222222222222222222222222222222222222222222222222222222".to_owned(),
            ],
            row_count: 2,
        },
    )]);
    let mut accumulator = ClassificationAccumulator::new(&variants, 1.0)?;
    accumulator.observe(
        &row(
            1,
            &address,
            "0x1111111111111111111111111111111111111111111111111111111111111111",
        ),
        &derived("0x2222222222222222222222222222222222222222222222222222222222222222"),
    )?;
    accumulator.observe(
        &row(
            2,
            &address,
            "0x1111111111111111111111111111111111111111111111111111111111111111",
        ),
        &derived("0x3333333333333333333333333333333333333333333333333333333333333333"),
    )?;

    let outcome = accumulator.finish();
    assert!(
        outcome
            .samples
            .iter()
            .any(|sample| sample.raw_code_hash_id == 2)
    );
    Ok(())
}

#[test]
fn classification_retains_verified_update_plan_for_write() -> Result<()> {
    let variants = BTreeMap::new();
    let mut accumulator = ClassificationAccumulator::new(&variants, 1.0)?;
    accumulator.observe(
        &row(
            1,
            "0x0000000000000000000000000000000000000001",
            "0x1111111111111111111111111111111111111111111111111111111111111111",
        ),
        &derived("0x2222222222222222222222222222222222222222222222222222222222222222"),
    )?;

    let outcome = accumulator.finish();
    assert_eq!(
        outcome.updates,
        vec![VerifiedCorrectionUpdate {
            update: RawCodeHashCorrectionUpdate {
                raw_code_hash_id: 1,
                stored_code_hash:
                    "0x1111111111111111111111111111111111111111111111111111111111111111".to_owned(),
                stored_code_byte_length: 32,
                corrected_code_hash:
                    "0x2222222222222222222222222222222222222222222222222222222222222222".to_owned(),
                corrected_code_byte_length: 31,
            },
            block_hash: "0x0000000000000000000000000000000000000000000000000000000000000001"
                .to_owned(),
            block_number: 1,
            contract_address: "0x0000000000000000000000000000000000000001".to_owned(),
        }]
    );
    Ok(())
}

#[test]
fn proof_spot_check_samples_use_most_recent_correctable_row_per_address() {
    let address_one = "0x0000000000000000000000000000000000000001";
    let address_two = "0x0000000000000000000000000000000000000002";
    let updates = vec![
        verified_update(1, address_one, 10),
        verified_update(2, address_one, 30),
        verified_update(3, address_two, 20),
    ];

    let samples = proof_spot_check_samples(&updates);

    assert_eq!(
        samples,
        vec![
            CorrectionSampleRow {
                raw_code_hash_id: 2,
                block_hash: "0x0000000000000000000000000000000000000000000000000000000000000002"
                    .to_owned(),
                block_number: 30,
                contract_address: address_one.to_owned(),
                rederived_code_hash:
                    "0x2222222222222222222222222222222222222222222222222222222222222222".to_owned(),
                rederived_code_byte_length: 30,
            },
            CorrectionSampleRow {
                raw_code_hash_id: 3,
                block_hash: "0x0000000000000000000000000000000000000000000000000000000000000003"
                    .to_owned(),
                block_number: 20,
                contract_address: address_two.to_owned(),
                rederived_code_hash:
                    "0x2222222222222222222222222222222222222222222222222222222222222222".to_owned(),
                rederived_code_byte_length: 30,
            },
        ]
    );
}

#[test]
fn classification_refuses_empty_derived_code_for_non_empty_stored_row() -> Result<()> {
    let variants = BTreeMap::new();
    let mut accumulator = ClassificationAccumulator::new(&variants, 1.0)?;

    let error = accumulator
        .observe(
            &row(
                1,
                "0x0000000000000000000000000000000000000001",
                "0x1111111111111111111111111111111111111111111111111111111111111111",
            ),
            &derived_with_len(
                "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
                0,
            ),
        )
        .expect_err("empty re-derived code for non-empty stored row must fail");

    assert!(
        error.to_string().contains("re-derived empty code"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn batch_accounting_rejects_drift() {
    let error = ensure_batch_accounted(
        &RawCodeHashCorrectionBatchOutcome {
            requested_count: 2,
            corrected_count: 1,
            already_correct_count: 0,
            conflicting_count: 0,
        },
        0,
    )
    .expect_err("unaccounted batch row must fail");

    assert!(
        error.to_string().contains("batch accounting drift"),
        "unexpected error: {error:#}"
    );
}
