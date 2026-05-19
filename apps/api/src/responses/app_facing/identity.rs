pub(crate) fn build_identity_name_response(
    record: Option<&bigname_storage::IdentityNameRecordRow>,
) -> IdentityNameResponse {
    match record {
        Some(record) => {
            let record = build_name_record_response(record);
            IdentityNameResponse {
                status: record.status.clone(),
                record: Some(record),
            }
        }
        None => IdentityNameResponse {
            status: "not_found".to_owned(),
            record: None,
        },
    }
}

pub(crate) fn build_name_record_response(
    record: &bigname_storage::IdentityNameRecordRow,
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
    ]);

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
        namehash: record.row.namehash.clone(),
        owner_address,
        manager_address,
        primary_address: coin_type_addresses.get("60").cloned(),
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

pub(crate) fn build_reverse_name_record_response(
    record: &bigname_storage::ReverseIdentityRecordRow,
) -> ReverseNameRecordResponse {
    let is_primary = record.primary_name.as_ref().is_some_and(|primary| {
        primary.claim_status == bigname_storage::PrimaryNameClaimStatus::Success
            && primary.normalized_claim_name.as_deref()
                == Some(record.name_record.row.normalized_name.as_str())
    });

    ReverseNameRecordResponse {
        record: build_name_record_response(&record.name_record),
        is_primary,
        relation_facets: identity_relation_facets(&record.relation_facets),
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
        status: indexing_status_label(chains.values()),
        chains,
    }
}

fn identity_coin_type_addresses(
    inventory: Option<&bigname_storage::RecordInventoryCurrentRow>,
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
    inventory: Option<&bigname_storage::RecordInventoryCurrentRow>,
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
    inventory: Option<&'a bigname_storage::RecordInventoryCurrentRow>,
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

fn identity_network(row: &NameCurrentRow) -> String {
    match row.namespace.as_str() {
        "basenames" => "base".to_owned(),
        "ens" => "ethereum".to_owned(),
        namespace => namespace.to_owned(),
    }
}

fn identity_record_status(row: &NameCurrentRow) -> String {
    match string_field(provenance_field(&row.coverage, "status")).as_deref() {
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

fn identity_relation_facets(relations: &[bigname_storage::AddressNameRelation]) -> Vec<String> {
    let has_owned = relations.iter().any(|relation| {
        matches!(
            relation,
            bigname_storage::AddressNameRelation::Registrant
                | bigname_storage::AddressNameRelation::TokenHolder
        )
    });
    let has_managed = relations
        .iter()
        .any(|relation| matches!(relation, bigname_storage::AddressNameRelation::EffectiveController));
    let has_registrant = relations
        .iter()
        .any(|relation| matches!(relation, bigname_storage::AddressNameRelation::Registrant));
    let has_effective_controller = relations
        .iter()
        .any(|relation| matches!(relation, bigname_storage::AddressNameRelation::EffectiveController));

    [
        (has_owned, "OWNED"),
        (has_managed, "MANAGED"),
        (has_registrant, "REGISTRANT"),
        (has_effective_controller, "EFFECTIVE_CONTROLLER"),
    ]
    .into_iter()
    .filter_map(|(present, label)| present.then(|| label.to_owned()))
    .collect()
}

fn indexing_status_label<'a>(
    chains: impl Iterator<Item = &'a IndexingStatusChainResponse>,
) -> String {
    let mut saw_degraded = false;
    for chain in chains {
        if chain.canonical_block.is_none() || chain.latest_projected_block.is_none() {
            saw_degraded = true;
            continue;
        }
        if chain.projection_lag_blocks.unwrap_or_default() > 0 {
            return "stale".to_owned();
        }
    }

    if saw_degraded {
        "degraded".to_owned()
    } else {
        "ready".to_owned()
    }
}
