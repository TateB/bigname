use super::*;

pub(super) fn build_authority_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    let profile = authority_profile_for_source_family(&raw_log.source_family);

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registrar_source_family())
        && registrar_name_registered_expiry_word_start(topic0).is_some()
    {
        let expiry_word_start = registrar_name_registered_expiry_word_start(topic0)
            .expect("checked registrar NameRegistered topic must have an expiry word");
        let label = decode_first_dynamic_string(&raw_log.data)?;
        let labelhash = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameRegistered log is missing indexed labelhash")?,
        )?;
        let observed = profile
            .context("registrar observation is missing an authority profile")?
            .observe_name(&label, &raw_log.normalizer_version)?;
        let observed_labelhash = observed
            .labelhashes
            .first()
            .context("observed registrar name is missing labelhash")?;
        if !observed_labelhash.eq_ignore_ascii_case(&labelhash) {
            bail!("NameRegistered labelhash does not match decoded label");
        }
        let registrant = normalize_topic_address(
            raw_log
                .topics
                .get(2)
                .context("NameRegistered log is missing indexed owner")?,
        )?;
        let expiry = abi_word_to_i64(
            raw_log
                .data
                .get(expiry_word_start..expiry_word_start + 32)
                .context("NameRegistered data is missing expiry word")?,
        )?;
        return Ok(Some(AuthorityObservation::RegistrationGranted(
            NameRegistrationObservation {
                label,
                labelhash,
                registrant,
                expiry: OffsetDateTime::from_unix_timestamp(expiry)
                    .context("NameRegistered expiry is not a valid unix timestamp")?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registrar_source_family())
        && registrar_name_renewed_expiry_word_start(topic0).is_some()
    {
        let expiry_word_start = registrar_name_renewed_expiry_word_start(topic0)
            .expect("checked registrar NameRenewed topic must have an expiry word");
        let label = decode_first_dynamic_string(&raw_log.data)?;
        let labelhash = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameRenewed log is missing indexed labelhash")?,
        )?;
        let observed = profile
            .context("registrar renewal observation is missing an authority profile")?
            .observe_name(&label, &raw_log.normalizer_version)?;
        let observed_labelhash = observed
            .labelhashes
            .first()
            .context("observed renewed registrar name is missing labelhash")?;
        if !observed_labelhash.eq_ignore_ascii_case(&labelhash) {
            bail!("NameRenewed labelhash does not match decoded label");
        }
        let expiry = abi_word_to_i64(
            raw_log
                .data
                .get(expiry_word_start..expiry_word_start + 32)
                .context("NameRenewed data is missing expiry word")?,
        )?;
        return Ok(Some(AuthorityObservation::RegistrationRenewed(
            NameRenewalObservation {
                label,
                labelhash,
                expiry: OffsetDateTime::from_unix_timestamp(expiry)
                    .context("NameRenewed expiry is not a valid unix timestamp")?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registrar_source_family())
        && topic0.eq_ignore_ascii_case(&transfer_topic0())
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
        && topic0.eq_ignore_ascii_case(&new_owner_topic0())
    {
        let parent_node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NewOwner log is missing parent node")?,
        )?;
        if parent_node
            != profile
                .context("registry observation is missing an authority profile")?
                .root_node()
        {
            return Ok(None);
        }
        return Ok(Some(AuthorityObservation::RegistryOwnerChanged(
            RegistryOwnerObservation {
                labelhash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(2)
                        .context("NewOwner log is missing indexed labelhash")?,
                )?,
                owner: decode_owner_address(&raw_log.data)?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registry_source_family())
        && topic0.eq_ignore_ascii_case(&new_resolver_topic0())
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
        && topic0.eq_ignore_ascii_case(&text_changed_topic0())
    {
        let key = decode_first_dynamic_string(&raw_log.data)?;
        let value = decode_second_dynamic_string_if_present(&raw_log.data)?;
        let indexed_key_hash = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("TextChanged log is missing indexed key hash")?,
        )?;
        if indexed_key_hash != keccak256_hex(key.as_bytes()) {
            bail!("TextChanged indexed key hash does not match decoded key");
        }
        return Ok(Some(AuthorityObservation::RecordChanged(
            RecordChangeObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("TextChanged log is missing indexed node")?,
                )?,
                resolver: raw_log.emitting_address.clone(),
                selector: RecordSelector {
                    record_key: "text".to_owned(),
                    record_family: "text".to_owned(),
                    selector_key: None,
                },
                value: value.map(Value::String),
                raw_name: None,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && topic0.eq_ignore_ascii_case(&name_changed_topic0())
    {
        let name = decode_first_dynamic_string(&raw_log.data)?;
        return Ok(Some(AuthorityObservation::RecordChanged(
            RecordChangeObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("NameChanged log is missing indexed node")?,
                )?,
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
        && topic0.eq_ignore_ascii_case(&addr_changed_topic0())
    {
        let address = decode_owner_address(&raw_log.data)?;
        return Ok(Some(AuthorityObservation::RecordChanged(
            RecordChangeObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("AddrChanged log is missing indexed node")?,
                )?,
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
        && topic0.eq_ignore_ascii_case(&address_changed_topic0())
    {
        let coin_type = abi_word_to_i64(
            raw_log
                .data
                .get(..32)
                .context("AddressChanged log is missing coin type")?,
        )?;
        let address_bytes = decode_nth_dynamic_bytes(&raw_log.data, 1)?;
        let value = resolver_address_record_value(coin_type, &address_bytes);
        return Ok(Some(AuthorityObservation::RecordChanged(
            RecordChangeObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("AddressChanged log is missing indexed node")?,
                )?,
                resolver: raw_log.emitting_address.clone(),
                selector: RecordSelector {
                    record_key: format!("addr:{coin_type}"),
                    record_family: "addr".to_owned(),
                    selector_key: Some(coin_type.to_string()),
                },
                value: Some(value),
                raw_name: None,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && topic0.eq_ignore_ascii_case(&version_changed_topic0())
    {
        return Ok(Some(AuthorityObservation::RecordVersionChanged(
            RecordVersionObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("VersionChanged log is missing indexed node")?,
                )?,
                resolver: raw_log.emitting_address.clone(),
                record_version: abi_word_to_i64(
                    raw_log
                        .data
                        .get(..32)
                        .context("VersionChanged log is missing record version")?,
                )?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if profile.wrapper_source_family() == Some(raw_log.source_family.as_str()))
        && topic0.eq_ignore_ascii_case(&name_wrapped_topic0())
    {
        let dns_name = decode_first_dynamic_bytes(&raw_log.data)?;
        let name = observe_dns_encoded_name_with_reference(
            &dns_name,
            &raw_log.reference(),
            &raw_log.normalizer_version,
        )?;
        let indexed_node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameWrapped log is missing indexed node")?,
        )?;
        if !indexed_node.eq_ignore_ascii_case(&name.namehash) {
            bail!("NameWrapped indexed node does not match decoded DNS name");
        }
        let owner = decode_owner_address(
            raw_log
                .data
                .get(32..64)
                .context("NameWrapped data is missing owner word")?,
        )?;
        let fuses = abi_word_to_i64(
            raw_log
                .data
                .get(64..96)
                .context("NameWrapped data is missing fuses word")?,
        )?;
        let expiry = abi_word_to_i64(
            raw_log
                .data
                .get(96..128)
                .context("NameWrapped data is missing expiry word")?,
        )?;
        return Ok(Some(AuthorityObservation::WrapperNameWrapped(
            WrapperNameWrappedObservation {
                name,
                owner,
                fuses,
                expiry: OffsetDateTime::from_unix_timestamp(expiry)
                    .context("NameWrapped expiry is not a valid unix timestamp")?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if profile.wrapper_source_family() == Some(raw_log.source_family.as_str()))
        && topic0.eq_ignore_ascii_case(&name_unwrapped_topic0())
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
        && topic0.eq_ignore_ascii_case(&fuses_set_topic0())
    {
        return Ok(Some(AuthorityObservation::WrapperFusesSet(
            WrapperFusesObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("FusesSet log is missing indexed node")?,
                )?,
                fuses: abi_word_to_i64(
                    raw_log
                        .data
                        .get(..32)
                        .context("FusesSet data is missing fuses word")?,
                )?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if profile.wrapper_source_family() == Some(raw_log.source_family.as_str()))
        && topic0.eq_ignore_ascii_case(&expiry_extended_topic0())
    {
        let expiry = abi_word_to_i64(
            raw_log
                .data
                .get(..32)
                .context("ExpiryExtended data is missing expiry word")?,
        )?;
        return Ok(Some(AuthorityObservation::WrapperExpiryExtended(
            WrapperExpiryObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("ExpiryExtended log is missing indexed node")?,
                )?,
                expiry: OffsetDateTime::from_unix_timestamp(expiry)
                    .context("ExpiryExtended expiry is not a valid unix timestamp")?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if profile.wrapper_source_family() == Some(raw_log.source_family.as_str()))
        && topic0.eq_ignore_ascii_case(&transfer_single_topic0())
    {
        let namehash = normalize_hex_32(&hex_string(
            raw_log
                .data
                .get(..32)
                .context("TransferSingle data is missing token id word")?,
        ))?;
        let value = abi_word_to_i64(
            raw_log
                .data
                .get(32..64)
                .context("TransferSingle data is missing value word")?,
        )?;
        return Ok(Some(AuthorityObservation::WrapperTokenTransferred(
            WrapperTokenTransferObservation {
                namehash,
                from_address: normalize_topic_address(
                    raw_log
                        .topics
                        .get(2)
                        .context("TransferSingle topic2 is missing from address")?,
                )?,
                to_address: normalize_topic_address(
                    raw_log
                        .topics
                        .get(3)
                        .context("TransferSingle topic3 is missing to address")?,
                )?,
                value,
                reference: raw_log.reference(),
            },
        )));
    }

    Ok(None)
}

fn decode_second_dynamic_string_if_present(data: &[u8]) -> Result<Option<String>> {
    if data.len() < 64 {
        return Ok(None);
    }

    let first_offset = abi_word_to_usize(&data[..32]).context("invalid TextChanged ABI offset")?;
    if first_offset < 64 {
        return Ok(None);
    }

    decode_nth_dynamic_string(data, 1).map(Some)
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
        _ => None,
    }
}
