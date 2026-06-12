use super::*;

#[path = "observation/resolver_records.rs"]
mod resolver_records;

pub(super) fn build_authority_observations(
    raw_log: &AuthorityRawLogRow,
    event_topics: &AuthorityEventTopics,
) -> Result<Vec<AuthorityObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(Vec::new());
    };
    let profile = authority_profile_for_source_family(&raw_log.source_family);

    if matches!(profile, Some(profile) if profile.wrapper_source_family() == Some(raw_log.source_family.as_str()))
        && event_topics.matches(TRANSFER_BATCH_SIGNATURE, topic0)?
    {
        return decode_wrapper_transfer_batch_data(raw_log)?
            .into_iter()
            .enumerate()
            .map(|(index, transfer)| {
                Ok(AuthorityObservation::WrapperTokenTransferred(
                    WrapperTokenTransferObservation {
                        namehash: normalize_hex_32(&transfer.namehash)?,
                        from_address: transfer.from_address,
                        to_address: transfer.to_address,
                        value: transfer.value,
                        transfer_index: Some(
                            i64::try_from(index).context("TransferBatch index exceeds i64")?,
                        ),
                        reference: raw_log.reference(),
                    },
                ))
            })
            .collect();
    }

    Ok(build_authority_observation(raw_log, event_topics)?
        .into_iter()
        .collect())
}

pub(super) fn build_authority_observation(
    raw_log: &AuthorityRawLogRow,
    event_topics: &AuthorityEventTopics,
) -> Result<Option<AuthorityObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    let profile = authority_profile_for_source_family(&raw_log.source_family);

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registrar_source_family())
    {
        if let Some(registration) =
            decode_registrar_name_registered_data(raw_log, topic0, event_topics)?
        {
            if !can_observe_registrar_label(&registration.label) {
                return Ok(None);
            }
            let labelhash = normalize_hex_32(
                raw_log
                    .topics
                    .get(1)
                    .context("NameRegistered log is missing indexed labelhash")?,
            )?;
            let Ok(observed) = profile
                .context("registrar observation is missing an authority profile")?
                .observe_name(&registration.label, &raw_log.normalizer_version)
            else {
                return Ok(None);
            };
            let observed_labelhash = observed
                .labelhashes
                .first()
                .context("observed registrar name is missing labelhash")?;
            if !observed_labelhash.eq_ignore_ascii_case(&labelhash) {
                return Ok(None);
            }
            let registrant = normalize_topic_address(
                raw_log
                    .topics
                    .get(2)
                    .context("NameRegistered log is missing indexed owner")?,
            )?;
            return Ok(Some(AuthorityObservation::RegistrationGranted(
                NameRegistrationObservation {
                    label: registration.label,
                    labelhash,
                    registrant,
                    expiry: OffsetDateTime::from_unix_timestamp(registration.expiry)
                        .context("NameRegistered expiry is not a valid unix timestamp")?,
                    reference: raw_log.reference(),
                },
            )));
        }
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registrar_source_family())
    {
        if let Some(renewal) = decode_registrar_name_renewed_data(raw_log, topic0, event_topics)? {
            if !can_observe_registrar_label(&renewal.label) {
                return Ok(None);
            }
            let labelhash = normalize_hex_32(
                raw_log
                    .topics
                    .get(1)
                    .context("NameRenewed log is missing indexed labelhash")?,
            )?;
            let Ok(observed) = profile
                .context("registrar renewal observation is missing an authority profile")?
                .observe_name(&renewal.label, &raw_log.normalizer_version)
            else {
                return Ok(None);
            };
            let observed_labelhash = observed
                .labelhashes
                .first()
                .context("observed renewed registrar name is missing labelhash")?;
            if !observed_labelhash.eq_ignore_ascii_case(&labelhash) {
                return Ok(None);
            }
            return Ok(Some(AuthorityObservation::RegistrationRenewed(
                NameRenewalObservation {
                    label: renewal.label,
                    labelhash,
                    expiry: OffsetDateTime::from_unix_timestamp(renewal.expiry)
                        .context("NameRenewed expiry is not a valid unix timestamp")?,
                    reference: raw_log.reference(),
                },
            )));
        }
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registrar_source_family())
        && event_topics.matches(TRANSFER_SIGNATURE, topic0)?
    {
        if raw_log.topics.len() < 4 {
            bail!("Transfer log is missing indexed topics");
        }
        return Ok(Some(AuthorityObservation::TokenTransferred(
            TokenTransferObservation {
                labelhash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(3)
                        .context("Transfer topic3 is missing token id")?,
                )?,
                from_address: normalize_topic_address(
                    raw_log
                        .topics
                        .get(1)
                        .context("Transfer topic1 is missing from address")?,
                )?,
                to_address: normalize_topic_address(
                    raw_log
                        .topics
                        .get(2)
                        .context("Transfer topic2 is missing to address")?,
                )?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registry_source_family())
        && event_topics.matches(NEW_OWNER_SIGNATURE, topic0)?
    {
        let parent_node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NewOwner log is missing parent node")?,
        )?;
        let labelhash = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("NewOwner log is missing indexed labelhash")?,
        )?;
        let namehash = child_namehash_hex(&parent_node, &labelhash)?;
        return Ok(Some(AuthorityObservation::RegistryOwnerChanged(
            RegistryOwnerObservation {
                parent_node: Some(parent_node),
                labelhash,
                namehash: Some(namehash),
                owner: decode_owner_address(&raw_log.data)?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registry_source_family())
        && event_topics.matches(REGISTRY_TRANSFER_SIGNATURE, topic0)?
    {
        return Ok(Some(AuthorityObservation::RegistryOwnerChanged(
            RegistryOwnerObservation {
                parent_node: None,
                labelhash: String::new(),
                namehash: Some(normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("Transfer log is missing indexed node")?,
                )?),
                owner: decode_owner_address(&raw_log.data)?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registry_source_family())
        && event_topics.matches(NEW_RESOLVER_SIGNATURE, topic0)?
    {
        return Ok(Some(AuthorityObservation::ResolverChanged(
            ResolverObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("NewResolver log is missing indexed node")?,
                )?,
                resolver: decode_owner_address(&raw_log.data)?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && event_topics.is_text_changed_topic0(&raw_log.source_family, topic0)?
    {
        let Some(text_record) = decode_text_record_change(raw_log, event_topics)? else {
            return Ok(None);
        };
        let Some(namehash) = normalize_resolver_topic(raw_log.topics.get(1)) else {
            return Ok(None);
        };
        let selector = text_record_selector(&text_record.selector_key);
        return Ok(Some(AuthorityObservation::RecordChanged(
            RecordChangeObservation {
                namehash,
                resolver: raw_log.emitting_address.clone(),
                selector,
                value: text_record.value.map(Value::String),
                raw_name: None,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && event_topics.matches(NAME_CHANGED_SIGNATURE, topic0)?
    {
        let Some(name) = decode_name_changed_data(raw_log) else {
            return Ok(None);
        };
        let Some(namehash) = normalize_resolver_topic(raw_log.topics.get(1)) else {
            return Ok(None);
        };
        return Ok(Some(AuthorityObservation::RecordChanged(
            RecordChangeObservation {
                namehash,
                resolver: raw_log.emitting_address.clone(),
                selector: RecordSelector {
                    record_key: "name".to_owned(),
                    record_family: "name".to_owned(),
                    selector_key: None,
                },
                value: Some(Value::String(name.clone())),
                raw_name: Some(name),
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && event_topics.matches(ADDR_CHANGED_SIGNATURE, topic0)?
    {
        let Some(address) = decode_addr_changed_data(raw_log) else {
            return Ok(None);
        };
        let Some(namehash) = normalize_resolver_topic(raw_log.topics.get(1)) else {
            return Ok(None);
        };
        return Ok(Some(AuthorityObservation::RecordChanged(
            RecordChangeObservation {
                namehash,
                resolver: raw_log.emitting_address.clone(),
                selector: RecordSelector {
                    record_key: format!("addr:{ENS_NATIVE_COIN_TYPE}"),
                    record_family: "addr".to_owned(),
                    selector_key: Some(ENS_NATIVE_COIN_TYPE.to_owned()),
                },
                value: Some(Value::String(address)),
                raw_name: None,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && event_topics.matches(ADDRESS_CHANGED_SIGNATURE, topic0)?
    {
        let Some(address_change) = decode_address_changed_data(raw_log) else {
            return Ok(None);
        };
        let value =
            resolver_address_record_value(address_change.coin_type, &address_change.address_bytes);
        let Some(namehash) = normalize_resolver_topic(raw_log.topics.get(1)) else {
            return Ok(None);
        };
        return Ok(Some(AuthorityObservation::RecordChanged(
            RecordChangeObservation {
                namehash,
                resolver: raw_log.emitting_address.clone(),
                selector: RecordSelector {
                    record_key: format!("addr:{}", address_change.coin_type),
                    record_family: "addr".to_owned(),
                    selector_key: Some(address_change.coin_type.to_string()),
                },
                value: Some(value),
                raw_name: None,
                reference: raw_log.reference(),
            },
        )));
    }

    if let Some(observation) =
        resolver_records::build_ens_v1_generic_record_observation(raw_log, topic0, event_topics)?
    {
        return Ok(Some(observation));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && event_topics.matches(VERSION_CHANGED_SIGNATURE, topic0)?
    {
        let Some(namehash) = normalize_resolver_topic(raw_log.topics.get(1)) else {
            return Ok(None);
        };
        let Some(record_version) = decode_version_changed_data(raw_log) else {
            return Ok(None);
        };
        return Ok(Some(AuthorityObservation::RecordVersionChanged(
            RecordVersionObservation {
                namehash,
                resolver: raw_log.emitting_address.clone(),
                record_version,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if profile.wrapper_source_family() == Some(raw_log.source_family.as_str()))
        && event_topics.matches(NAME_WRAPPED_SIGNATURE, topic0)?
    {
        let decoded = decode_wrapper_name_wrapped_data(raw_log)?;
        let Ok(name) = observe_dns_encoded_name_with_reference(
            &decoded.dns_name,
            &raw_log.reference(),
            &raw_log.normalizer_version,
        ) else {
            return Ok(None);
        };
        if !decoded.namehash.eq_ignore_ascii_case(&name.namehash) {
            return Ok(None);
        }
        return Ok(Some(AuthorityObservation::WrapperNameWrapped(
            WrapperNameWrappedObservation {
                name,
                owner: decoded.owner,
                fuses: decoded.fuses,
                expiry: OffsetDateTime::from_unix_timestamp(decoded.expiry)
                    .context("NameWrapped expiry is not a valid unix timestamp")?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if profile.wrapper_source_family() == Some(raw_log.source_family.as_str()))
        && event_topics.matches(NAME_UNWRAPPED_SIGNATURE, topic0)?
    {
        return Ok(Some(AuthorityObservation::WrapperNameUnwrapped(
            WrapperNameUnwrappedObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("NameUnwrapped log is missing indexed node")?,
                )?,
                owner: decode_owner_address(&raw_log.data)?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if profile.wrapper_source_family() == Some(raw_log.source_family.as_str()))
        && event_topics.matches(FUSES_SET_SIGNATURE, topic0)?
    {
        let decoded = decode_wrapper_fuses_set_data(raw_log)?;
        return Ok(Some(AuthorityObservation::WrapperFusesSet(
            WrapperFusesObservation {
                namehash: normalize_hex_32(&decoded.namehash)?,
                fuses: decoded.fuses,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if profile.wrapper_source_family() == Some(raw_log.source_family.as_str()))
        && event_topics.matches(EXPIRY_EXTENDED_SIGNATURE, topic0)?
    {
        let decoded = decode_wrapper_expiry_extended_data(raw_log)?;
        return Ok(Some(AuthorityObservation::WrapperExpiryExtended(
            WrapperExpiryObservation {
                namehash: normalize_hex_32(&decoded.namehash)?,
                expiry: OffsetDateTime::from_unix_timestamp(decoded.expiry)
                    .context("ExpiryExtended expiry is not a valid unix timestamp")?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if profile.wrapper_source_family() == Some(raw_log.source_family.as_str()))
        && event_topics.matches(TRANSFER_SINGLE_SIGNATURE, topic0)?
    {
        let transfer = decode_wrapper_transfer_single_data(raw_log)?;
        return Ok(Some(AuthorityObservation::WrapperTokenTransferred(
            WrapperTokenTransferObservation {
                namehash: normalize_hex_32(&transfer.namehash)?,
                from_address: transfer.from_address,
                to_address: transfer.to_address,
                value: transfer.value,
                transfer_index: None,
                reference: raw_log.reference(),
            },
        )));
    }

    Ok(None)
}

pub(super) fn decode_text_record_change(
    raw_log: &AuthorityRawLogRow,
    event_topics: &AuthorityEventTopics,
) -> Result<Option<EnsV1TextRecordChange>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    let source_family = raw_log.source_family.as_str();
    if !event_topics.is_text_changed_topic0(source_family, topic0)? {
        return Ok(None);
    }
    let Some(text_change) = decode_text_changed_data(source_family, raw_log, event_topics)? else {
        return Ok(None);
    };
    if text_change.key.trim().is_empty() {
        return Ok(None);
    }
    if text_change.indexed_key_hash != keccak256_hex(text_change.key.as_bytes()) {
        return Ok(None);
    }
    Ok(Some(EnsV1TextRecordChange {
        record_key: format!("text:{}", text_change.key),
        record_family: "text".to_owned(),
        selector_key: text_change.key,
        value: text_change.value,
    }))
}

fn normalize_resolver_topic(topic: Option<&String>) -> Option<String> {
    normalize_hex_32(topic?).ok()
}

fn resolver_address_record_value(coin_type: i64, address_bytes: &[u8]) -> Value {
    let hex_value = hex_string(address_bytes);
    if coin_type.to_string() == ENS_NATIVE_COIN_TYPE && address_bytes.len() == 20 {
        return Value::String(hex_value);
    }

    serde_json::json!({
        "encoding": "hex",
        "bytes": hex_value,
    })
}

fn text_record_selector(key: &str) -> RecordSelector {
    RecordSelector {
        record_key: format!("text:{key}"),
        record_family: "text".to_owned(),
        selector_key: Some(key.to_owned()),
    }
}

pub(super) fn observation_labelhash(observation: &AuthorityObservation) -> String {
    match observation {
        AuthorityObservation::RegistrationGranted(value) => value.labelhash.clone(),
        AuthorityObservation::RegistrationRenewed(value) => value.labelhash.clone(),
        AuthorityObservation::TokenTransferred(value) => value.labelhash.clone(),
        AuthorityObservation::RegistryOwnerChanged(value) => value.labelhash.clone(),
        AuthorityObservation::WrapperNameWrapped(value) => value
            .name
            .labelhashes
            .first()
            .cloned()
            .expect("wrapper name observation must include a first labelhash"),
        AuthorityObservation::ResolverChanged(_)
        | AuthorityObservation::RecordChanged(_)
        | AuthorityObservation::RecordVersionChanged(_)
        | AuthorityObservation::WrapperNameUnwrapped(_)
        | AuthorityObservation::WrapperFusesSet(_)
        | AuthorityObservation::WrapperExpiryExtended(_)
        | AuthorityObservation::WrapperTokenTransferred(_) => {
            unreachable!("resolver observations must be resolved by namehash before use")
        }
    }
}

pub(super) fn observation_namehash(observation: &AuthorityObservation) -> Option<&str> {
    match observation {
        AuthorityObservation::ResolverChanged(value) => Some(&value.namehash),
        AuthorityObservation::RecordChanged(value) => Some(&value.namehash),
        AuthorityObservation::RecordVersionChanged(value) => Some(&value.namehash),
        AuthorityObservation::WrapperNameUnwrapped(value) => Some(&value.namehash),
        AuthorityObservation::WrapperFusesSet(value) => Some(&value.namehash),
        AuthorityObservation::WrapperExpiryExtended(value) => Some(&value.namehash),
        AuthorityObservation::WrapperTokenTransferred(value) => Some(&value.namehash),
        AuthorityObservation::RegistryOwnerChanged(value) => value.namehash.as_deref(),
        _ => None,
    }
}

pub(super) fn observation_reference(observation: &AuthorityObservation) -> &ObservationRef {
    match observation {
        AuthorityObservation::RegistrationGranted(value) => &value.reference,
        AuthorityObservation::RegistrationRenewed(value) => &value.reference,
        AuthorityObservation::TokenTransferred(value) => &value.reference,
        AuthorityObservation::RegistryOwnerChanged(value) => &value.reference,
        AuthorityObservation::ResolverChanged(value) => &value.reference,
        AuthorityObservation::RecordChanged(value) => &value.reference,
        AuthorityObservation::RecordVersionChanged(value) => &value.reference,
        AuthorityObservation::WrapperNameWrapped(value) => &value.reference,
        AuthorityObservation::WrapperNameUnwrapped(value) => &value.reference,
        AuthorityObservation::WrapperFusesSet(value) => &value.reference,
        AuthorityObservation::WrapperExpiryExtended(value) => &value.reference,
        AuthorityObservation::WrapperTokenTransferred(value) => &value.reference,
    }
}
