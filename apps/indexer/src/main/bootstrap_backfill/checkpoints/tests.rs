use super::*;
use serde_json::json;

const SOURCE_IDENTITY_HASH: &str = "fnv1a64:1111111111111111";
const OTHER_SOURCE_IDENTITY_HASH: &str = "fnv1a64:2222222222222222";
const DEFAULT_ADDRESS: &str = "0x0000000000000000000000000000000000000abc";
const OTHER_ADDRESS: &str = "0x0000000000000000000000000000000000000def";

#[test]
fn target_checkpoint_walks_contiguous_jobs_for_same_target() -> Result<()> {
    let target_id = "00000000-0000-0000-0000-000000000001";
    let other_target_id = "00000000-0000-0000-0000-000000000002";
    let expected_source_identity =
        checkpoint_source_identity(1, 30, &[target_id], SOURCE_IDENTITY_HASH);
    let rows = vec![
        checkpoint_row_with_hash(1, 10, &[target_id], OTHER_SOURCE_IDENTITY_HASH),
        checkpoint_row_with_hash(
            11,
            20,
            &[target_id, other_target_id],
            OTHER_SOURCE_IDENTITY_HASH,
        ),
    ];

    assert_eq!(
        contiguous_bootstrap_target_checkpoint(
            rows,
            BackfillBlockRange::new(1, 30)?,
            &expected_source_identity,
            target_id,
        )?,
        Some(20)
    );
    Ok(())
}

#[test]
fn target_checkpoint_stops_at_coverage_gap() -> Result<()> {
    let target_id = "00000000-0000-0000-0000-000000000001";
    let expected_source_identity =
        checkpoint_source_identity(1, 30, &[target_id], SOURCE_IDENTITY_HASH);
    let rows = vec![
        checkpoint_row_with_hash(1, 10, &[target_id], OTHER_SOURCE_IDENTITY_HASH),
        checkpoint_row_with_hash(12, 20, &[target_id], OTHER_SOURCE_IDENTITY_HASH),
    ];

    assert_eq!(
        contiguous_bootstrap_target_checkpoint(
            rows,
            BackfillBlockRange::new(1, 30)?,
            &expected_source_identity,
            target_id,
        )?,
        Some(10)
    );
    Ok(())
}

#[test]
fn segment_checkpoint_ignores_non_contiguous_parallel_range_progress() -> Result<()> {
    let target_id = "00000000-0000-0000-0000-000000000001";
    let target_ids = BTreeSet::from([target_id.to_owned()]);
    let expected_source_identity =
        checkpoint_source_identity(1, 40, &[target_id], SOURCE_IDENTITY_HASH);
    let rows = vec![
        checkpoint_row_with_hash(1, 10, &[target_id], OTHER_SOURCE_IDENTITY_HASH),
        checkpoint_row_with_hash(21, 30, &[target_id], OTHER_SOURCE_IDENTITY_HASH),
    ];

    assert_eq!(
        contiguous_bootstrap_segment_checkpoint(
            rows,
            BackfillBlockRange::new(1, 40)?,
            &expected_source_identity,
            &target_ids,
        )?,
        Some(10)
    );
    Ok(())
}

#[test]
fn target_checkpoint_ignores_incompatible_source_identity() -> Result<()> {
    let target_id = "00000000-0000-0000-0000-000000000001";
    let expected_source_identity =
        checkpoint_source_identity(1, 30, &[target_id], SOURCE_IDENTITY_HASH);
    let rows = vec![
        checkpoint_row_with_address(1, 30, &[target_id], OTHER_ADDRESS),
        checkpoint_row(1, 10, &[target_id]),
    ];

    assert_eq!(
        contiguous_bootstrap_target_checkpoint(
            rows,
            BackfillBlockRange::new(1, 30)?,
            &expected_source_identity,
            target_id,
        )?,
        Some(10)
    );
    Ok(())
}

#[test]
fn segment_checkpoint_matches_legacy_full_identity_with_compact_expected() -> Result<()> {
    let target_id = "00000000-0000-0000-0000-000000000001";
    let target_ids = BTreeSet::from([target_id.to_owned()]);
    let legacy_source_identity =
        checkpoint_source_identity(1, 30, &[target_id], SOURCE_IDENTITY_HASH);
    let expected_source_identity =
        compact_selected_targets_source_identity(&legacy_source_identity)?;
    let selected_targets = legacy_source_identity
        .get("selected_targets")
        .and_then(Value::as_array)
        .expect("legacy source identity has selected targets");
    let expected_digest = selected_targets_digest(selected_targets)?;
    assert_eq!(
        expected_source_identity
            .get("selected_targets_digest")
            .and_then(Value::as_str),
        Some(expected_digest.as_str()),
        "bootstrap compact identity must use the same canonical keccak digest shape as storage backfill validation"
    );
    let rows = vec![checkpoint_row_with_source_identity(
        1,
        10,
        legacy_source_identity,
    )];

    assert_eq!(
        contiguous_bootstrap_segment_checkpoint(
            rows,
            BackfillBlockRange::new(1, 30)?,
            &expected_source_identity,
            &target_ids,
        )?,
        Some(10)
    );
    Ok(())
}

#[test]
fn segment_checkpoint_matches_generic_resolver_scan_identity() -> Result<()> {
    let registry_id = "00000000-0000-0000-0000-000000000001";
    let resolver_id = "00000000-0000-0000-0000-000000000002";
    let target_ids = BTreeSet::from([registry_id.to_owned(), resolver_id.to_owned()]);
    let expected_source_identity = checkpoint_generic_resolver_source_identity(
        1,
        30,
        &[registry_id],
        &[resolver_id],
        SOURCE_IDENTITY_HASH,
    );
    let rows = vec![checkpoint_row_with_source_identity(
        1,
        10,
        checkpoint_generic_resolver_source_identity(
            1,
            10,
            &[registry_id],
            &[resolver_id],
            OTHER_SOURCE_IDENTITY_HASH,
        ),
    )];

    assert_eq!(
        contiguous_bootstrap_segment_checkpoint(
            rows,
            BackfillBlockRange::new(1, 30)?,
            &expected_source_identity,
            &target_ids,
        )?,
        Some(10)
    );
    Ok(())
}

#[test]
fn target_checkpoint_matches_generic_resolver_scan_in_mixed_job() -> Result<()> {
    let registry_id = "00000000-0000-0000-0000-000000000001";
    let resolver_id = "00000000-0000-0000-0000-000000000002";
    let expected_source_identity = checkpoint_generic_resolver_source_identity(
        1,
        30,
        &[],
        &[resolver_id],
        SOURCE_IDENTITY_HASH,
    );
    let rows = vec![checkpoint_row_with_source_identity(
        1,
        10,
        checkpoint_generic_resolver_source_identity(
            1,
            10,
            &[registry_id],
            &[resolver_id],
            OTHER_SOURCE_IDENTITY_HASH,
        ),
    )];

    assert_eq!(
        contiguous_bootstrap_target_checkpoint(
            rows,
            BackfillBlockRange::new(1, 30)?,
            &expected_source_identity,
            resolver_id,
        )?,
        Some(10)
    );
    Ok(())
}

fn checkpoint_row(
    range_start_block_number: i64,
    checkpoint_block_number: i64,
    target_ids: &[&str],
) -> BootstrapTargetCheckpointRow {
    checkpoint_row_with_hash(
        range_start_block_number,
        checkpoint_block_number,
        target_ids,
        SOURCE_IDENTITY_HASH,
    )
}

fn checkpoint_row_with_hash(
    range_start_block_number: i64,
    checkpoint_block_number: i64,
    target_ids: &[&str],
    source_identity_hash: &str,
) -> BootstrapTargetCheckpointRow {
    checkpoint_row_with_source_identity(
        range_start_block_number,
        checkpoint_block_number,
        checkpoint_source_identity(
            range_start_block_number,
            checkpoint_block_number,
            target_ids,
            source_identity_hash,
        ),
    )
}

fn checkpoint_row_with_address(
    range_start_block_number: i64,
    checkpoint_block_number: i64,
    target_ids: &[&str],
    address: &str,
) -> BootstrapTargetCheckpointRow {
    checkpoint_row_with_source_identity(
        range_start_block_number,
        checkpoint_block_number,
        checkpoint_source_identity_with_address(
            range_start_block_number,
            checkpoint_block_number,
            target_ids,
            OTHER_SOURCE_IDENTITY_HASH,
            address,
        ),
    )
}

fn checkpoint_row_with_source_identity(
    range_start_block_number: i64,
    checkpoint_block_number: i64,
    source_identity: Value,
) -> BootstrapTargetCheckpointRow {
    BootstrapTargetCheckpointRow {
        range_start_block_number,
        checkpoint_block_number,
        source_identity,
    }
}

fn checkpoint_source_identity(
    effective_from_block: i64,
    effective_to_block: i64,
    target_ids: &[&str],
    source_identity_hash: &str,
) -> Value {
    checkpoint_source_identity_with_address(
        effective_from_block,
        effective_to_block,
        target_ids,
        source_identity_hash,
        DEFAULT_ADDRESS,
    )
}

fn checkpoint_source_identity_with_address(
    effective_from_block: i64,
    effective_to_block: i64,
    target_ids: &[&str],
    source_identity_hash: &str,
    address: &str,
) -> Value {
    json!({
        "selector_kind": "watched_target_set",
        "source_family": null,
        "source_identity_hash": source_identity_hash,
        "requested_watched_targets": target_ids
            .iter()
            .map(|target_id| json!({ "contract_instance_id": target_id }))
            .collect::<Vec<_>>(),
        "selected_targets": target_ids
            .iter()
            .map(|target_id| json!({
                "source_family": "ens_v1_registry_l1",
                "contract_instance_id": target_id,
                "address": address,
                "effective_from_block": effective_from_block,
                "effective_to_block": effective_to_block
            }))
            .collect::<Vec<_>>()
    })
}

fn compact_selected_targets_source_identity(source_identity: &Value) -> Result<Value> {
    let selected_targets = source_identity
        .get("selected_targets")
        .and_then(Value::as_array)
        .expect("legacy source identity has selected targets");
    let selected_targets_digest = selected_targets_digest(selected_targets)?;

    Ok(json!({
        "selector_kind": source_identity.get("selector_kind"),
        "source_family": source_identity.get("source_family"),
        "source_identity_hash": source_identity.get("source_identity_hash"),
        "requested_watched_targets": source_identity.get("requested_watched_targets"),
        "selected_target_count": selected_targets.len(),
        "selected_targets_digest_algorithm": "keccak256",
        "selected_targets_digest": selected_targets_digest,
        "selected_targets_sample": {
            "first": selected_targets.first(),
            "last": selected_targets.last(),
        },
        "source_identity_payload_format": "selected_targets_digest_v1"
    }))
}

fn selected_targets_digest(selected_targets: &[Value]) -> Result<String> {
    Ok(format!(
        "keccak256:{}",
        alloy_primitives::keccak256(serde_json::to_vec(&canonical_json_value(Value::Array(
            selected_targets.to_vec(),
        )))?)
    ))
}

fn canonical_json_value(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(canonical_json_value).collect()),
        Value::Object(fields) => {
            let mut fields = fields
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect::<Vec<_>>();
            fields.sort_by(|left, right| left.0.cmp(&right.0));

            let mut sorted = serde_json::Map::new();
            for (key, value) in fields {
                sorted.insert(key, value);
            }
            Value::Object(sorted)
        }
        value => value,
    }
}

fn checkpoint_generic_resolver_source_identity(
    effective_from_block: i64,
    effective_to_block: i64,
    selected_target_ids: &[&str],
    generic_target_ids: &[&str],
    source_identity_hash: &str,
) -> Value {
    let requested_target_ids = selected_target_ids
        .iter()
        .chain(generic_target_ids.iter())
        .copied()
        .collect::<Vec<_>>();
    json!({
        "selector_kind": "watched_target_set",
        "source_family": null,
        "source_identity_hash": source_identity_hash,
        "requested_watched_targets": requested_target_ids
            .iter()
            .map(|target_id| json!({ "contract_instance_id": target_id }))
            .collect::<Vec<_>>(),
        "selected_targets": selected_target_ids
            .iter()
            .map(|target_id| json!({
                "source_family": "ens_v1_registry_l1",
                "contract_instance_id": target_id,
                "address": DEFAULT_ADDRESS,
                "effective_from_block": effective_from_block,
                "effective_to_block": effective_to_block
            }))
            .collect::<Vec<_>>(),
        "generic_topic_scans": [
            {
                "source_family": "ens_v1_resolver_l1",
                "source_identity_payload_format": "generic_resolver_event_topics_v1"
            }
        ],
        "source_identity_payload_format": "selected_targets_with_generic_topic_scans_v1"
    })
}
