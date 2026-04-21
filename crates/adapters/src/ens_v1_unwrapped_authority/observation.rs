fn build_authority_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    let profile = authority_profile_for_source_family(&raw_log.source_family);

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registrar_source_family())
        && topic0.eq_ignore_ascii_case(&name_registered_topic0())
    {
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
                .get(64..96)
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
        && topic0.eq_ignore_ascii_case(&name_renewed_topic0())
    {
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
                .get(64..96)
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
                raw_name: None,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && topic0.eq_ignore_ascii_case(&name_changed_topic0())
    {
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
                raw_name: Some(decode_first_dynamic_string(&raw_log.data)?),
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && topic0.eq_ignore_ascii_case(&addr_changed_topic0())
    {
        decode_owner_address(&raw_log.data)?;
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
        decode_nth_dynamic_bytes(&raw_log.data, 1)?;
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

    Ok(None)
}

impl AuthorityRawLogRow {
    fn reference(&self) -> ObservationRef {
        ObservationRef {
            chain_id: self.chain_id.clone(),
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            block_timestamp: self.block_timestamp,
            transaction_hash: Some(self.transaction_hash.clone()),
            transaction_index: Some(self.transaction_index),
            log_index: Some(self.log_index),
            canonicality_state: self.canonicality_state,
            namespace: self.namespace.clone(),
            source_manifest_id: self.source_manifest_id,
            source_family: self.source_family.clone(),
            manifest_version: self.manifest_version,
        }
    }
}

impl CanonicalBlockIndex {
    fn first_block_at_or_after(
        &self,
        timestamp: OffsetDateTime,
        namespace: &str,
    ) -> Option<BoundaryRef> {
        self.blocks
            .iter()
            .find(|block| block.block_timestamp >= timestamp)
            .map(|block| BoundaryRef {
                chain_id: block.chain_id.clone(),
                block_hash: block.block_hash.clone(),
                block_number: block.block_number,
                block_timestamp: block.block_timestamp,
                canonicality_state: block.canonicality_state,
                namespace: namespace.to_owned(),
            })
    }
}

async fn load_canonical_blocks(pool: &PgPool, chain: &str) -> Result<Vec<RawBlockSnapshot>> {
    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_blocks
        WHERE chain_id = $1
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY block_number
        "#,
    )
    .bind(chain)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load canonical raw blocks for chain {chain}"))?;

    rows.into_iter()
        .map(|row| {
            Ok(RawBlockSnapshot {
                chain_id: row.try_get("chain_id").context("missing chain_id")?,
                block_hash: row.try_get("block_hash").context("missing block_hash")?,
                block_number: row
                    .try_get("block_number")
                    .context("missing block_number")?,
                block_timestamp: row
                    .try_get("block_timestamp")
                    .context("missing block_timestamp")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")
                        .context("missing canonicality_state")?,
                )?,
            })
        })
        .collect()
}

async fn load_authority_raw_logs(
    pool: &PgPool,
    chain: &str,
    active_emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
) -> Result<Vec<AuthorityRawLogRow>> {
    let emitters_by_address = active_emitters
        .iter()
        .cloned()
        .map(|emitter| (emitter.address.clone(), emitter))
        .collect::<HashMap<_, _>>();
    let watched_addresses = emitters_by_address.keys().cloned().collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        SELECT
            rl.chain_id AS chain_id,
            rl.block_hash AS block_hash,
            rl.block_number AS block_number,
            rb.block_timestamp AS block_timestamp,
            rl.transaction_hash AS transaction_hash,
            rl.transaction_index AS transaction_index,
            rl.log_index AS log_index,
            rl.emitting_address AS emitting_address,
            rl.topics AS topics,
            rl.data AS data,
            rl.canonicality_state::TEXT AS canonicality_state
        FROM raw_logs rl
        JOIN raw_blocks rb
          ON rb.chain_id = rl.chain_id
         AND rb.block_hash = rl.block_hash
        WHERE rl.chain_id = $1
          AND lower(rl.emitting_address) = ANY($2::TEXT[])
          AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
          AND rl.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY rl.block_number, rl.transaction_index, rl.log_index
        "#,
    )
    .bind(chain)
    .bind(&watched_addresses)
    .bind(restrict_to_block_hashes)
    .bind(block_hashes)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load ENSv1 unwrapped authority raw logs for chain {chain}")
    })?;

    rows.into_iter()
        .map(|row| {
            let address = row
                .try_get::<String, _>("emitting_address")
                .context("missing emitting_address")?
                .to_ascii_lowercase();
            let emitter = emitters_by_address.get(&address).with_context(|| {
                format!("missing active emitter metadata for chain {chain} address {address}")
            })?;
            Ok(AuthorityRawLogRow {
                chain_id: row.try_get("chain_id").context("missing chain_id")?,
                block_hash: row.try_get("block_hash").context("missing block_hash")?,
                block_number: row
                    .try_get("block_number")
                    .context("missing block_number")?,
                block_timestamp: row
                    .try_get("block_timestamp")
                    .context("missing block_timestamp")?,
                transaction_hash: row
                    .try_get("transaction_hash")
                    .context("missing transaction_hash")?,
                transaction_index: row
                    .try_get("transaction_index")
                    .context("missing transaction_index")?,
                log_index: row.try_get("log_index").context("missing log_index")?,
                emitting_address: address,
                topics: row.try_get("topics").context("missing topics")?,
                data: row.try_get("data").context("missing data")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")
                        .context("missing canonicality_state")?,
                )?,
                source_manifest_id: emitter.source_manifest_id,
                namespace: emitter.namespace.clone(),
                source_family: emitter.source_family.clone(),
                manifest_version: emitter.manifest_version,
                normalizer_version: emitter.normalizer_version.clone(),
            })
        })
        .collect()
}

async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .context("failed to load watched contracts for ENSv1 unwrapped authority attribution")?;
    let watched_contracts = watched_contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .collect::<Vec<_>>();
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let manifest_ids = watched_contracts
        .iter()
        .map(|contract| {
            contract.source_manifest_id.with_context(|| {
                format!(
                    "watched contract {} on {} is missing source_manifest_id",
                    contract.address, contract.chain
                )
            })
        })
        .collect::<Result<HashSet<_>>>()?
        .into_iter()
        .collect::<Vec<_>>();

    let active_manifests = load_active_manifest_metadata(pool, &manifest_ids).await?;
    let mut emitters_by_address = HashMap::<String, ActiveEmitter>::new();
    for watched_contract in watched_contracts {
        let Some(source_manifest_id) = watched_contract.source_manifest_id else {
            continue;
        };
        let Some(manifest) = active_manifests.get(&source_manifest_id) else {
            continue;
        };
        if manifest.source_family != SOURCE_FAMILY_ENS_V1_REGISTRAR_L1
            && manifest.source_family != SOURCE_FAMILY_ENS_V1_REGISTRY_L1
            && manifest.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1
            && manifest.source_family != SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
            && manifest.source_family != SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
            && manifest.source_family != SOURCE_FAMILY_BASENAMES_BASE_RESOLVER
        {
            continue;
        }

        let candidate = ActiveEmitter {
            address: watched_contract.address,
            source_manifest_id,
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            manifest_version: manifest.manifest_version,
            normalizer_version: manifest.normalizer_version.clone(),
            source_rank: source_rank(watched_contract.source),
        };

        match emitters_by_address.get(&candidate.address) {
            Some(current) if !candidate_precedes(&candidate, current) => {}
            _ => {
                emitters_by_address.insert(candidate.address.clone(), candidate);
            }
        }
    }

    let mut emitters = emitters_by_address.into_values().collect::<Vec<_>>();
    emitters.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then(left.source_rank.cmp(&right.source_rank))
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
    });
    Ok(emitters)
}

async fn load_active_manifest_metadata(
    pool: &PgPool,
    manifest_ids: &[i64],
) -> Result<HashMap<i64, ActiveManifestMetadata>> {
    let rows = sqlx::query(
        r#"
        SELECT manifest_id, chain, namespace, source_family, manifest_version, normalizer_version
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND manifest_id = ANY($1::BIGINT[])
        "#,
    )
    .bind(manifest_ids)
    .fetch_all(pool)
    .await
    .context("failed to load active manifest metadata for ENSv1 unwrapped authority")?;

    rows.into_iter()
        .map(|row| {
            let manifest = ActiveManifestMetadata {
                manifest_id: row.try_get("manifest_id").context("missing manifest_id")?,
                chain: row.try_get("chain").context("missing chain")?,
                namespace: row.try_get("namespace").context("missing namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("missing source_family")?,
                manifest_version: row
                    .try_get("manifest_version")
                    .context("missing manifest_version")?,
                normalizer_version: row
                    .try_get("normalizer_version")
                    .context("missing normalizer_version")?,
            };
            Ok((manifest.manifest_id, manifest))
        })
        .collect()
}

fn source_rank(source: WatchedContractSource) -> i32 {
    match source {
        WatchedContractSource::ManifestRoot => 0,
        WatchedContractSource::ManifestContract => 1,
        WatchedContractSource::DiscoveryEdge => 2,
    }
}

fn candidate_precedes(candidate: &ActiveEmitter, current: &ActiveEmitter) -> bool {
    (candidate.source_rank, candidate.source_manifest_id)
        < (current.source_rank, current.source_manifest_id)
}

fn authority_profile_for_source_family(source_family: &str) -> Option<AuthorityProfile> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1
        | SOURCE_FAMILY_ENS_V1_REGISTRY_L1
        | SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => Some(AuthorityProfile::Ens),
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
        | SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
        | SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some(AuthorityProfile::Basenames),
        _ => None,
    }
}

fn observe_registrar_name_with_reference(
    label: &str,
    reference: &ObservationRef,
    normalizer_version: &str,
) -> Result<NameMetadata> {
    authority_profile_for_source_family(&reference.source_family)
        .with_context(|| {
            format!(
                "unsupported authority source family {}",
                reference.source_family
            )
        })?
        .observe_name(label, normalizer_version)
}

fn observe_registrar_name_with_version(
    label: &str,
    profile: AuthorityProfile,
    normalizer_version: &str,
) -> Result<NameMetadata> {
    if label.is_empty() {
        bail!("registrar label must not be empty");
    }
    let normalized_label = label.to_ascii_lowercase();
    let (normalized_name, input_name, parent_labels) = match profile {
        AuthorityProfile::Ens => (
            format!("{normalized_label}.eth"),
            format!("{label}.eth"),
            vec![b"eth".to_vec()],
        ),
        AuthorityProfile::Basenames => (
            format!("{normalized_label}.base.eth"),
            format!("{label}.base.eth"),
            vec![b"base".to_vec(), b"eth".to_vec()],
        ),
    };
    let label_length =
        u8::try_from(normalized_label.len()).context("registrar label exceeds DNS length")?;
    let dns_capacity = 2
        + normalized_label.len()
        + parent_labels
            .iter()
            .map(|label| 1 + label.len())
            .sum::<usize>();
    let mut dns_name = Vec::with_capacity(dns_capacity);
    dns_name.push(label_length);
    dns_name.extend_from_slice(normalized_label.as_bytes());
    for label in &parent_labels {
        dns_name
            .push(u8::try_from(label.len()).context("registrar suffix label exceeds DNS length")?);
        dns_name.extend_from_slice(label);
    }
    dns_name.push(0);
    let mut namehash_labels = Vec::with_capacity(1 + parent_labels.len());
    namehash_labels.push(normalized_label.as_bytes().to_vec());
    namehash_labels.extend(parent_labels.iter().cloned());
    let mut labelhashes = Vec::with_capacity(namehash_labels.len());
    for label in &namehash_labels {
        labelhashes.push(keccak256_hex(label));
    }
    Ok(NameMetadata {
        namespace: profile.namespace().to_owned(),
        logical_name_id: format!("{}:{normalized_name}", profile.namespace()),
        input_name: input_name.clone(),
        canonical_display_name: normalized_name.clone(),
        normalized_name: normalized_name.clone(),
        dns_encoded_name: dns_name.clone(),
        namehash: namehash_hex(&namehash_labels),
        labelhashes,
        normalizer_version: normalizer_version.to_owned(),
    })
}

#[cfg(test)]
fn observe_registrar_eth_name_with_version(
    label: &str,
    normalizer_version: &str,
) -> Result<NameMetadata> {
    observe_registrar_name_with_version(label, AuthorityProfile::Ens, normalizer_version)
}

fn decode_first_dynamic_string(data: &[u8]) -> Result<String> {
    String::from_utf8(decode_first_dynamic_bytes(data)?)
        .context("dynamic string payload is not valid UTF-8")
}

fn decode_first_dynamic_bytes(data: &[u8]) -> Result<Vec<u8>> {
    decode_nth_dynamic_bytes(data, 0)
}

fn decode_nth_dynamic_bytes(data: &[u8], parameter_index: usize) -> Result<Vec<u8>> {
    let offset_start = parameter_index
        .checked_mul(32)
        .context("dynamic ABI parameter index overflowed")?;
    if data.len() < 64 {
        bail!("event data is too short to decode a dynamic bytes parameter");
    }
    let offset = word_to_usize(
        data.get(offset_start..offset_start + 32)
            .context("event data is missing dynamic bytes offset")?,
    )
    .context("invalid ABI offset")?;
    if data.len() < offset + 32 {
        bail!("event data is missing dynamic bytes length");
    }
    let byte_length = word_to_usize(&data[offset..offset + 32]).context("invalid ABI length")?;
    let bytes_start = offset + 32;
    let bytes_end = bytes_start + byte_length;
    if data.len() < bytes_end {
        bail!("event data does not contain the full dynamic bytes payload");
    }
    Ok(data[bytes_start..bytes_end].to_vec())
}

fn word_to_usize(word: &[u8]) -> Result<usize> {
    if word.len() != 32 {
        bail!("ABI word must be 32 bytes");
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word exceeds supported usize width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    usize::try_from(u64::from_be_bytes(bytes)).context("ABI word does not fit in usize")
}

fn abi_word_to_i64(word: &[u8]) -> Result<i64> {
    if word.len() != 32 {
        bail!("ABI word must be 32 bytes");
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word exceeds supported i64 width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    i64::try_from(u64::from_be_bytes(bytes)).context("ABI word does not fit in i64")
}

fn normalize_hex_32(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    let normalized = if normalized.starts_with("0x") {
        normalized
    } else {
        format!("0x{normalized}")
    };
    if normalized.len() != 66 {
        bail!("expected 32-byte hex value, got {normalized}");
    }
    Ok(normalized)
}

fn decode_owner_address(data: &[u8]) -> Result<String> {
    let word = data
        .get(..32)
        .context("owner address payload is missing the first ABI word")?;
    let mut output = String::from("0x");
    for byte in &word[12..32] {
        output.push_str(&format!("{byte:02x}"));
    }
    Ok(output)
}

fn normalize_topic_address(value: &str) -> Result<String> {
    let normalized = normalize_hex_32(value)?;
    Ok(format!("0x{}", &normalized[26..]))
}

fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown canonicality_state value {value}"),
    }
}

fn deterministic_uuid(seed: &str) -> Uuid {
    let mut digest = Keccak256::new();
    digest.update(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn keccak256_hex(bytes: &[u8]) -> String {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    hex_string(&digest)
}

fn namehash_hex(labels: &[Vec<u8>]) -> String {
    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let label_hash = {
            let mut hasher = Keccak256::new();
            hasher.update(label);
            let digest = hasher.finalize();
            let mut output = [0u8; 32];
            output.copy_from_slice(&digest);
            output
        };
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(&label_hash);
        let mut hasher = Keccak256::new();
        hasher.update(combined);
        node.copy_from_slice(&hasher.finalize());
    }
    hex_string(&node)
}

fn eth_node() -> String {
    namehash_hex(&[b"eth".to_vec()])
}

fn base_eth_node() -> String {
    namehash_hex(&[b"base".to_vec(), b"eth".to_vec()])
}

fn name_registered_topic0() -> String {
    keccak256_hex(NAME_REGISTERED_SIGNATURE.as_bytes())
}

fn name_renewed_topic0() -> String {
    keccak256_hex(NAME_RENEWED_SIGNATURE.as_bytes())
}

fn transfer_topic0() -> String {
    keccak256_hex(TRANSFER_SIGNATURE.as_bytes())
}

fn new_owner_topic0() -> String {
    keccak256_hex(NEW_OWNER_SIGNATURE.as_bytes())
}

fn new_resolver_topic0() -> String {
    keccak256_hex(NEW_RESOLVER_SIGNATURE.as_bytes())
}

fn name_changed_topic0() -> String {
    keccak256_hex(NAME_CHANGED_SIGNATURE.as_bytes())
}

fn addr_changed_topic0() -> String {
    keccak256_hex(ADDR_CHANGED_SIGNATURE.as_bytes())
}

fn address_changed_topic0() -> String {
    keccak256_hex(ADDRESS_CHANGED_SIGNATURE.as_bytes())
}

fn text_changed_topic0() -> String {
    keccak256_hex(TEXT_CHANGED_SIGNATURE.as_bytes())
}

fn version_changed_topic0() -> String {
    keccak256_hex(VERSION_CHANGED_SIGNATURE.as_bytes())
}

fn hex_string(bytes: &[u8]) -> String {
    let mut output = String::from("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}
