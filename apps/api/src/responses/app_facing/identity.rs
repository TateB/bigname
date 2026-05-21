fn build_name_record_response_for_coin_type(
    record: &bigname_storage::IdentityNameRecordRow,
    primary_coin_type: &str,
    corrected_input_normalization: bool,
) -> NameRecordResponse {
    let coin_type_addresses = identity_coin_type_addresses(record.record_inventory_current.as_ref());
    let text_records = identity_text_records(record.record_inventory_current.as_ref());
    let mut unsupported_fields = identity_unsupported_fields(record);
    let owner_address = identity_relation_subject(
        &record.relations,
        &[bigname_storage::AddressNameRelation::TokenHolder],
    )
    .or_else(|| identity_json_address(&record.row.declared_summary, &[&["control", "owner"]]))
    .or_else(|| {
        identity_json_address(&record.row.declared_summary, &[&["control", "registry_owner"]])
    });
    let manager_address = identity_relation_subject(
        &record.relations,
        &[bigname_storage::AddressNameRelation::EffectiveController],
    );
    let resolver_address =
        identity_json_address(&record.row.declared_summary, &[&["resolver", "address"]]);
    let expiration = identity_json_timestamp(&record.row.declared_summary, &[
        &["registration", "expiry_date"],
        &["registration", "expiry"],
        &["control", "expiry_date"],
        &["control", "expiry"],
    ]);
    let token_id = identity_json_string(&record.row.declared_summary, &[
        &["authority", "token_id"],
        &["registration", "token_id"],
        &["registration", "upstream_resource"],
        &["control", "token_id"],
    ])
    .or_else(|| identity_labelhash_token_id(&record.row));

    if owner_address.is_none() {
        unsupported_fields.insert("owner_address".to_owned());
    }
    if manager_address.is_none() {
        unsupported_fields.insert("manager_address".to_owned());
    }
    if expiration.is_none() {
        unsupported_fields.insert("expiration".to_owned());
    }
    if token_id.is_none() {
        unsupported_fields.insert("token_id".to_owned());
    }

    NameRecordResponse {
        name: record.row.canonical_display_name.clone(),
        normalized_name: record.row.normalized_name.clone(),
        corrected_input_normalization,
        namehash: record.row.namehash.clone(),
        owner_address,
        manager_address,
        primary_address: coin_type_addresses.get(primary_coin_type).cloned(),
        coin_type_addresses,
        text_records,
        resolver_address,
        expiration,
        token_id,
        network: identity_network(&record.row),
        as_of: IdentityAsOfResponse {
            chain_positions: build_chain_positions_from_values(
                std::iter::once(&record.row.chain_positions).chain(
                    record
                        .record_inventory_current
                        .as_ref()
                        .map(|inventory| &inventory.chain_positions),
                ),
            ),
            as_of_timestamp: Some(identity_as_of_timestamp(record)),
        },
        status: identity_record_status(&record.row),
        unsupported_fields: unsupported_fields.into_iter().collect(),
    }
}

pub(crate) fn build_indexing_status_response(
    read: &bigname_storage::IndexingStatusRead,
) -> IndexingStatusResponse {
    let chains = read
        .chains
        .iter()
        .map(|row| {
            let projection_lag_blocks = row
                .canonical_block
                .zip(row.latest_projected_block)
                .map(|(canonical, projected)| canonical.saturating_sub(projected));
            let projection_lag_seconds = row
                .canonical_timestamp
                .zip(row.latest_projected_timestamp)
                .map(|(canonical, projected)| (canonical - projected).whole_seconds().max(0));
            (
                row.chain_id.clone(),
                IndexingStatusChainResponse {
                    canonical_block: row.canonical_block,
                    safe_block: row.safe_block,
                    finalized_block: row.finalized_block,
                    latest_projected_block: row.latest_projected_block,
                    latest_projected_timestamp: row.latest_projected_timestamp.map(format_timestamp),
                    projection_lag_blocks,
                    projection_lag_seconds,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    IndexingStatusResponse {
        status: indexing_status_label(chains.values(), read.has_unscoped_pending_invalidations),
        chains,
    }
}

fn identity_coin_type_addresses(
    inventory: Option<&bigname_storage::IdentityRecordInventoryRow>,
) -> BTreeMap<String, String> {
    identity_success_record_entries(inventory, "addr")
        .filter_map(|entry| {
            let coin_type = string_field(provenance_field(entry, "selector_key")).or_else(|| {
                provenance_field(entry, "value")
                    .and_then(|value| provenance_field(value, "coin_type"))
                    .and_then(value_to_string)
            })?;
            let value = identity_record_value_string(entry)?;
            Some((coin_type, value))
        })
        .collect()
}

fn identity_text_records(
    inventory: Option<&bigname_storage::IdentityRecordInventoryRow>,
) -> BTreeMap<String, String> {
    let mut records = BTreeMap::new();
    for entry in identity_success_record_entries(inventory, "text") {
        let Some(key) = string_field(provenance_field(entry, "selector_key")).or_else(|| {
            provenance_field(entry, "value")
                .and_then(|value| provenance_field(value, "key"))
                .and_then(value_to_string)
        }) else {
            continue;
        };
        if let Some(value) = identity_record_value_string(entry) {
            records.insert(key, value);
        }
    }
    for entry in identity_success_record_entries(inventory, "avatar") {
        if let Some(value) = identity_record_value_string(entry) {
            records.insert("avatar".to_owned(), value);
        }
    }
    records
}

fn identity_success_record_entries<'a>(
    inventory: Option<&'a bigname_storage::IdentityRecordInventoryRow>,
    record_family: &'static str,
) -> impl Iterator<Item = &'a JsonValue> {
    inventory
        .and_then(|inventory| inventory.entries.as_array())
        .into_iter()
        .flatten()
        .filter(move |entry| {
            string_field(provenance_field(entry, "record_family")).as_deref()
                == Some(record_family)
                && string_field(provenance_field(entry, "status")).as_deref() == Some("success")
        })
}

fn identity_record_value_string(entry: &JsonValue) -> Option<String> {
    let value = provenance_field(entry, "value")?;
    provenance_field(value, "value")
        .and_then(value_to_string)
        .or_else(|| value_to_string(value))
}

fn identity_labelhash_token_id(row: &bigname_storage::IdentityNameCurrentRow) -> Option<String> {
    if !identity_supports_labelhash_token_id(row) {
        return None;
    }

    let labelhash = row.labelhash.as_deref()?;
    let hex = labelhash.strip_prefix("0x").unwrap_or(labelhash);
    alloy_primitives::U256::from_str_radix(hex, 16)
        .ok()
        .map(|value| value.to_string())
}

fn identity_supports_labelhash_token_id(row: &bigname_storage::IdentityNameCurrentRow) -> bool {
    row.namespace == "ens"
        && row.labelhash_count == Some(2)
        && row.normalized_name.ends_with(".eth")
        && row.normalized_name.split('.').count() == 2
}

fn identity_unsupported_fields(
    record: &bigname_storage::IdentityNameRecordRow,
) -> BTreeSet<String> {
    let mut fields = BTreeSet::new();
    let Some(inventory) = record.record_inventory_current.as_ref() else {
        fields.insert("coin_type_addresses".to_owned());
        fields.insert("primary_address".to_owned());
        fields.insert("text_records".to_owned());
        return fields;
    };

    for family in inventory
        .unsupported_families
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|family| string_field(provenance_field(family, "record_family")))
    {
        match family.as_str() {
            "addr" => {
                fields.insert("coin_type_addresses".to_owned());
                fields.insert("primary_address".to_owned());
            }
            "text" | "avatar" => {
                fields.insert("text_records".to_owned());
            }
            _ => {}
        }
    }
    fields
}

fn identity_relation_subject(
    relations: &[bigname_storage::IdentityAddressRelationRow],
    accepted: &[bigname_storage::AddressNameRelation],
) -> Option<String> {
    let subjects = relations
        .iter()
        .filter(|relation| accepted.contains(&relation.relation))
        .map(|relation| relation.address.clone())
        .collect::<BTreeSet<_>>();

    if subjects.len() == 1 {
        subjects.into_iter().next()
    } else {
        None
    }
}

fn identity_json_string(value: &JsonValue, paths: &[&[&str]]) -> Option<String> {
    paths
        .iter()
        .find_map(|path| json_path(value, path).and_then(value_to_string))
        .filter(|value| !value.trim().is_empty())
}

fn identity_json_address(value: &JsonValue, paths: &[&[&str]]) -> Option<String> {
    identity_json_string(value, paths).map(|value| value.to_ascii_lowercase())
}

fn identity_json_timestamp(value: &JsonValue, paths: &[&[&str]]) -> Option<i64> {
    for path in paths {
        let Some(value) = json_path(value, path) else {
            continue;
        };
        if let Some(value) = value.as_i64() {
            return Some(value);
        }
        let Some(text) = value.as_str().filter(|value| !value.trim().is_empty()) else {
            continue;
        };
        if let Ok(value) = text.parse::<i64>() {
            return Some(value);
        }
        if let Ok(value) = parse_rfc3339_utc_timestamp(text) {
            return Some(value.unix_timestamp());
        }
    }
    None
}

fn json_path<'a>(mut value: &'a JsonValue, path: &[&str]) -> Option<&'a JsonValue> {
    for key in path {
        value = provenance_field(value, key)?;
    }
    Some(value)
}

fn identity_network(row: &bigname_storage::IdentityNameCurrentRow) -> String {
    identity_network_from_parts(&row.namespace, &row.chain_positions)
}

fn identity_network_from_parts(namespace: &str, chain_positions: &JsonValue) -> String {
    match namespace {
        "basenames" if identity_has_chain_position(chain_positions, "base-sepolia") => {
            "base-sepolia".to_owned()
        }
        "basenames" => "base".to_owned(),
        "ens" if identity_has_chain_position(chain_positions, "ethereum-sepolia") => {
            "ethereum-sepolia".to_owned()
        }
        "ens" => "ethereum".to_owned(),
        namespace => namespace.to_owned(),
    }
}

fn identity_has_chain_position(chain_positions: &JsonValue, chain_id: &str) -> bool {
    chain_positions
        .as_object()
        .into_iter()
        .flatten()
        .any(|(slot, value)| {
            slot == chain_id
                || string_field(provenance_field(value, "chain_id")).as_deref() == Some(chain_id)
        })
}

fn identity_record_status(row: &bigname_storage::IdentityNameCurrentRow) -> String {
    identity_record_status_from_coverage(&row.coverage)
}

fn identity_record_status_from_coverage(coverage: &JsonValue) -> String {
    match string_field(provenance_field(coverage, "status")).as_deref() {
        Some("stale") => "stale".to_owned(),
        Some("unsupported") => "unsupported".to_owned(),
        _ => "success".to_owned(),
    }
}

fn identity_as_of_timestamp(record: &bigname_storage::IdentityNameRecordRow) -> String {
    std::iter::once(record.row.last_recomputed_at)
        .chain(
            record
                .record_inventory_current
                .as_ref()
                .map(|inventory| inventory.last_recomputed_at),
        )
        .max()
        .map(format_timestamp)
        .unwrap_or_else(|| format_timestamp(OffsetDateTime::now_utc()))
}

fn indexing_status_label<'a>(
    chains: impl Iterator<Item = &'a IndexingStatusChainResponse>,
    has_unscoped_pending_invalidations: bool,
) -> String {
    let mut saw_degraded = has_unscoped_pending_invalidations;
    let mut saw_chain = false;
    for chain in chains {
        saw_chain = true;
        if chain.canonical_block.is_none() || chain.latest_projected_block.is_none() {
            saw_degraded = true;
            continue;
        }
        if chain.projection_lag_blocks.unwrap_or_default() > 0 {
            return "stale".to_owned();
        }
    }

    if saw_degraded || !saw_chain {
        "degraded".to_owned()
    } else {
        "ready".to_owned()
    }
}
