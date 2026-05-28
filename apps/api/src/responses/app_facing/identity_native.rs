pub(crate) fn build_native_identity_name_record_response(
    record: &bigname_storage::IdentityNameRecordRow,
) -> NativeIdentityRecordResponse {
    build_native_identity_record_response_for_coin_type(
        record,
        "60",
        None,
        native_identity_relation_facets_from_rows(&record.relations),
    )
}

pub(crate) fn build_native_identity_name_feed_record_response(
    record: &bigname_storage::IdentityNameRecordRow,
) -> NativeIdentityRecordResponse {
    NativeIdentityRecordResponse {
        name: record.row.normalized_name.clone(),
        namespace: record.row.namespace.clone(),
        namehash: record.row.namehash.clone(),
        owner_address: None,
        manager_address: None,
        primary_address: None,
        coin_type_addresses: BTreeMap::new(),
        text_records: BTreeMap::new(),
        resolver_address: None,
        expiration: None,
        token_id: None,
        network: identity_network(&record.row),
        is_primary: None,
        relation_facets: native_identity_relation_facets_from_rows(&record.relations),
        status: identity_record_status(&record.row),
        unsupported_fields: Vec::new(),
    }
}

pub(crate) fn build_native_reverse_identity_record_response(
    record: &bigname_storage::ReverseIdentityRecordRow,
) -> NativeIdentityRecordResponse {
    build_native_identity_record_response_for_coin_type(
        &record.name_record,
        &record.requested_coin_type,
        Some(reverse_identity_is_primary_response(record)),
        native_identity_relation_facets(&record.relation_facets),
    )
}

pub(crate) fn build_native_identity_feed_record_response(
    record: &bigname_storage::ReverseIdentityFeedRecordRow,
) -> NativeIdentityRecordResponse {
    NativeIdentityRecordResponse {
        name: record.normalized_name.clone(),
        namespace: record.namespace.clone(),
        namehash: record.namehash.clone(),
        owner_address: None,
        manager_address: None,
        primary_address: None,
        coin_type_addresses: BTreeMap::new(),
        text_records: BTreeMap::new(),
        resolver_address: None,
        expiration: None,
        token_id: None,
        network: identity_network_from_parts(&record.namespace, &record.chain_positions),
        is_primary: Some(record.is_primary),
        relation_facets: native_identity_relation_facets(&record.relation_facets),
        status: identity_record_status_from_coverage(&record.coverage),
        unsupported_fields: Vec::new(),
    }
}

fn build_native_identity_record_response_for_coin_type(
    record: &bigname_storage::IdentityNameRecordRow,
    primary_coin_type: &str,
    is_primary: Option<bool>,
    relation_facets: Vec<String>,
) -> NativeIdentityRecordResponse {
    let record_response = build_name_record_response_for_coin_type(record, primary_coin_type, false);
    NativeIdentityRecordResponse {
        name: record.row.normalized_name.clone(),
        namespace: record.row.namespace.clone(),
        namehash: record_response.namehash,
        owner_address: record_response.owner_address,
        manager_address: record_response.manager_address,
        primary_address: record_response.primary_address,
        coin_type_addresses: record_response.coin_type_addresses,
        text_records: record_response.text_records,
        resolver_address: record_response.resolver_address,
        expiration: record_response.expiration,
        token_id: record_response.token_id,
        network: record_response.network,
        is_primary,
        relation_facets,
        status: record_response.status,
        unsupported_fields: record_response.unsupported_fields,
    }
}

fn reverse_identity_is_primary_response(
    record: &bigname_storage::ReverseIdentityRecordRow,
) -> bool {
    record.primary_name.as_ref().is_some_and(|primary| {
        primary.claim_status == bigname_storage::PrimaryNameClaimStatus::Success
            && primary.normalized_claim_name.as_deref()
                == Some(record.name_record.row.normalized_name.as_str())
    })
}

fn native_identity_relation_facets_from_rows(
    relations: &[bigname_storage::IdentityAddressRelationRow],
) -> Vec<String> {
    let relations = relations
        .iter()
        .map(|relation| relation.relation)
        .collect::<Vec<_>>();
    native_identity_relation_facets(&relations)
}

fn native_identity_relation_facets(
    relations: &[bigname_storage::AddressNameRelation],
) -> Vec<String> {
    let has_owned = relations.iter().any(|relation| {
        matches!(
            relation,
            bigname_storage::AddressNameRelation::Registrant
                | bigname_storage::AddressNameRelation::TokenHolder
        )
    });
    let has_managed = relations.iter().any(|relation| {
        matches!(
            relation,
            bigname_storage::AddressNameRelation::EffectiveController
        )
    });
    let has_registrant = relations.iter().any(|relation| {
        matches!(relation, bigname_storage::AddressNameRelation::Registrant)
    });
    let has_effective_controller = relations.iter().any(|relation| {
        matches!(
            relation,
            bigname_storage::AddressNameRelation::EffectiveController
        )
    });

    [
        (has_owned, "owned"),
        (has_managed, "managed"),
        (has_registrant, "registrant"),
        (has_effective_controller, "effective_controller"),
    ]
    .into_iter()
    .filter_map(|(present, label)| present.then(|| label.to_owned()))
    .collect()
}
