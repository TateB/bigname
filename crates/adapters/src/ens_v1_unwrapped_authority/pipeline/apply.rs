use super::super::*;

mod preload;

pub(super) use preload::{name_intro_positions_for_raw_logs, preload_name_metadata_for_raw_logs};

pub(super) fn resolver_profile_fact_nodes(
    raw_logs: &[AuthorityRawLogRow],
    event_topics: &AuthorityEventTopics,
) -> Result<Vec<String>> {
    let mut nodes = BTreeSet::<String>::new();
    for raw_log in raw_logs {
        let Some(topic0) = raw_log.topics.first() else {
            continue;
        };
        if resolver_fact_families_for_topic0(&raw_log.source_family, topic0, event_topics)?
            .is_empty()
        {
            continue;
        }
        let Some(node) = raw_log.topics.get(1) else {
            continue;
        };
        nodes.insert(normalize_hex_32(node)?);
    }
    Ok(nodes.into_iter().collect())
}

pub(super) fn apply_authority_raw_log(
    raw_log: &AuthorityRawLogRow,
    histories: &mut BTreeMap<String, NameHistory>,
    reverse_histories: &mut BTreeMap<String, ReverseClaimSourceHistory>,
    known_names_by_namehash: &mut HashMap<String, NameMetadata>,
    known_name_refs_by_namehash: &mut HashMap<String, ObservationRef>,
    namehash_to_labelhash: &mut HashMap<String, String>,
    pending_namehash_observations: &mut HashMap<String, Vec<AuthorityObservation>>,
    same_tx_name_intro_positions: &HashMap<String, Vec<RawLogPosition>>,
    migrated_registry_nodes: &mut MigratedRegistryNodes,
    reverse_claim_sources: &HashMap<String, ReverseClaimSource>,
    resolver_profile_gate: &ResolverProfileGate,
    block_index: &CanonicalBlockIndex,
    event_topics: &AuthorityEventTopics,
    mut checkpoint_delta: Option<&mut UnwrappedAuthorityReplayCheckpointDelta>,
) -> Result<bool> {
    let migration_guard = registry_migration_guard_action(raw_log, event_topics)?;
    if migration_guard.suppressed_by(migrated_registry_nodes) {
        return Ok(false);
    }

    if resolver_profile_gate.rejects_resolver_local_fact(raw_log, event_topics)? {
        if let Some(node) = migration_guard.mark_migrated_node() {
            if migrated_registry_nodes.insert(node.to_owned())
                && let Some(delta) = checkpoint_delta.as_deref_mut()
            {
                delta.mark_migrated_node(node);
            }
        }
        return Ok(false);
    }
    let observation = build_authority_observation(raw_log, event_topics)?;
    if let Some(node) = migration_guard.mark_migrated_node() {
        if migrated_registry_nodes.insert(node.to_owned())
            && let Some(delta) = checkpoint_delta.as_deref_mut()
        {
            delta.mark_migrated_node(node);
        }
    }
    let Some(observation) = observation else {
        return Ok(false);
    };

    apply_authority_observation(
        observation,
        histories,
        reverse_histories,
        known_names_by_namehash,
        known_name_refs_by_namehash,
        namehash_to_labelhash,
        pending_namehash_observations,
        same_tx_name_intro_positions,
        reverse_claim_sources,
        block_index,
        checkpoint_delta.as_deref_mut(),
    )?;
    Ok(true)
}

fn apply_authority_observation(
    observation: AuthorityObservation,
    histories: &mut BTreeMap<String, NameHistory>,
    reverse_histories: &mut BTreeMap<String, ReverseClaimSourceHistory>,
    known_names_by_namehash: &mut HashMap<String, NameMetadata>,
    known_name_refs_by_namehash: &mut HashMap<String, ObservationRef>,
    namehash_to_labelhash: &mut HashMap<String, String>,
    pending_namehash_observations: &mut HashMap<String, Vec<AuthorityObservation>>,
    same_tx_name_intro_positions: &HashMap<String, Vec<RawLogPosition>>,
    reverse_claim_sources: &HashMap<String, ReverseClaimSource>,
    block_index: &CanonicalBlockIndex,
    mut checkpoint_delta: Option<&mut UnwrappedAuthorityReplayCheckpointDelta>,
) -> Result<()> {
    if let Some(name) = learn_record_raw_name_preimage(
        &observation,
        reverse_claim_sources,
        known_names_by_namehash,
        known_name_refs_by_namehash,
        namehash_to_labelhash,
        checkpoint_delta.as_deref_mut(),
    ) && let Some(pending) = pending_namehash_observations.remove(&name.namehash)
    {
        if let Some(delta) = checkpoint_delta.as_deref_mut() {
            delta.mark_pending_observations(name.namehash.clone());
        }
        let labelhash = name
            .labelhashes
            .first()
            .cloned()
            .context("learned name preimage is missing a first labelhash")?;
        let name_ref = known_name_refs_by_namehash.get(&name.namehash).cloned();
        for pending_observation in pending {
            apply_authority_observation_for_history_key(
                pending_observation,
                &name.namehash,
                &labelhash,
                Some(name.clone()),
                name_ref.clone(),
                histories,
                known_names_by_namehash,
                known_name_refs_by_namehash,
                namehash_to_labelhash,
                pending_namehash_observations,
                same_tx_name_intro_positions,
                block_index,
                checkpoint_delta.as_deref_mut(),
            )?;
        }
    }

    if let Some(namehash) = observation_namehash(&observation) {
        let normalized_namehash = namehash.to_ascii_lowercase();
        let defer_to_same_tx_intro = should_defer_preloaded_namehash_observation(
            &observation,
            same_tx_name_intro_positions,
            histories,
            namehash_to_labelhash,
        );
        if !defer_to_same_tx_intro
            && let Some(labelhash) = namehash_to_labelhash.get(&normalized_namehash).cloned()
        {
            let known_name = known_names_by_namehash.get(&normalized_namehash).cloned();
            let known_name_ref = known_name_refs_by_namehash
                .get(&normalized_namehash)
                .cloned();
            return apply_authority_observation_for_history_key(
                observation,
                &normalized_namehash,
                &labelhash,
                known_name,
                known_name_ref,
                histories,
                known_names_by_namehash,
                known_name_refs_by_namehash,
                namehash_to_labelhash,
                pending_namehash_observations,
                same_tx_name_intro_positions,
                block_index,
                checkpoint_delta.as_deref_mut(),
            );
        } else if !defer_to_same_tx_intro
            && let Some(claim_source) = reverse_claim_sources.get(&normalized_namehash).cloned()
        {
            let history = reverse_histories
                .entry(normalized_namehash.clone())
                .or_insert_with(|| ReverseClaimSourceHistory {
                    claim_source,
                    current_resolver: None,
                    current_record_version: None,
                    events: Vec::new(),
                });
            apply_reverse_claim_source_observation(history, observation)?;
            if let Some(delta) = checkpoint_delta.as_deref_mut() {
                delta.mark_reverse_history(normalized_namehash);
            }
            return Ok(());
        } else {
            pending_namehash_observations
                .entry(normalized_namehash.clone())
                .or_default()
                .push(observation);
            if let Some(delta) = checkpoint_delta.as_deref_mut() {
                delta.mark_pending_observations(normalized_namehash);
            }
            return Ok(());
        }
    } else {
        let LabelhashObservationTarget {
            history_key,
            labelhash,
            known_name,
            known_name_ref,
        } = labelhash_observation_target(&observation, known_names_by_namehash)?;
        return apply_authority_observation_for_history_key(
            observation,
            &history_key,
            &labelhash,
            known_name,
            known_name_ref,
            histories,
            known_names_by_namehash,
            known_name_refs_by_namehash,
            namehash_to_labelhash,
            pending_namehash_observations,
            same_tx_name_intro_positions,
            block_index,
            checkpoint_delta.as_deref_mut(),
        );
    }
}

struct LabelhashObservationTarget {
    history_key: String,
    labelhash: String,
    known_name: Option<NameMetadata>,
    known_name_ref: Option<ObservationRef>,
}

fn labelhash_observation_target(
    observation: &AuthorityObservation,
    known_names_by_namehash: &HashMap<String, NameMetadata>,
) -> Result<LabelhashObservationTarget> {
    match observation {
        AuthorityObservation::RegistrationGranted(value) => {
            let name = observe_registrar_name_with_reference(
                &value.label,
                &value.reference,
                ENS_NORMALIZER_VERSION,
            )?;
            Ok(LabelhashObservationTarget {
                history_key: name.namehash.clone(),
                labelhash: value.labelhash.clone(),
                known_name: Some(name),
                known_name_ref: Some(value.reference.clone()),
            })
        }
        AuthorityObservation::RegistrationRenewed(value) => {
            let name = observe_registrar_name_with_reference(
                &value.label,
                &value.reference,
                ENS_NORMALIZER_VERSION,
            )?;
            Ok(LabelhashObservationTarget {
                history_key: name.namehash.clone(),
                labelhash: value.labelhash.clone(),
                known_name: Some(name),
                known_name_ref: Some(value.reference.clone()),
            })
        }
        AuthorityObservation::TokenTransferred(value) => {
            let history_key = registrar_child_namehash(&value.reference, &value.labelhash)?;
            Ok(LabelhashObservationTarget {
                known_name: known_names_by_namehash.get(&history_key).cloned(),
                known_name_ref: None,
                history_key,
                labelhash: value.labelhash.clone(),
            })
        }
        AuthorityObservation::RegistryOwnerChanged(value) => {
            let history_key = registrar_child_namehash(&value.reference, &value.labelhash)?;
            Ok(LabelhashObservationTarget {
                known_name: known_names_by_namehash.get(&history_key).cloned(),
                known_name_ref: None,
                history_key,
                labelhash: value.labelhash.clone(),
            })
        }
        AuthorityObservation::WrapperNameWrapped(value) => {
            let labelhash = value
                .name
                .labelhashes
                .first()
                .cloned()
                .context("wrapper name observation must include a first labelhash")?;
            Ok(LabelhashObservationTarget {
                history_key: value.name.namehash.clone(),
                labelhash,
                known_name: Some(value.name.clone()),
                known_name_ref: Some(value.reference.clone()),
            })
        }
        AuthorityObservation::ResolverChanged(_)
        | AuthorityObservation::RecordChanged(_)
        | AuthorityObservation::RecordVersionChanged(_)
        | AuthorityObservation::WrapperNameUnwrapped(_)
        | AuthorityObservation::WrapperFusesSet(_)
        | AuthorityObservation::WrapperExpiryExtended(_)
        | AuthorityObservation::WrapperTokenTransferred(_) => {
            unreachable!("namehash observations must be resolved before use")
        }
    }
}

fn registrar_child_namehash(reference: &ObservationRef, labelhash: &str) -> Result<String> {
    let profile =
        authority_profile_for_source_family(&reference.source_family).with_context(|| {
            format!(
                "unsupported authority source family {}",
                reference.source_family
            )
        })?;
    child_namehash_hex(&profile.root_node(), labelhash)
}

fn apply_authority_observation_for_history_key(
    observation: AuthorityObservation,
    history_key: &str,
    labelhash: &str,
    known_name: Option<NameMetadata>,
    known_name_ref: Option<ObservationRef>,
    histories: &mut BTreeMap<String, NameHistory>,
    known_names_by_namehash: &mut HashMap<String, NameMetadata>,
    known_name_refs_by_namehash: &mut HashMap<String, ObservationRef>,
    namehash_to_labelhash: &mut HashMap<String, String>,
    pending_namehash_observations: &mut HashMap<String, Vec<AuthorityObservation>>,
    same_tx_name_intro_positions: &HashMap<String, Vec<RawLogPosition>>,
    block_index: &CanonicalBlockIndex,
    mut checkpoint_delta: Option<&mut UnwrappedAuthorityReplayCheckpointDelta>,
) -> Result<()> {
    if let Some(delta) = checkpoint_delta.as_deref_mut() {
        delta.mark_history(history_key);
    }
    let history = histories
        .entry(history_key.to_owned())
        .or_insert_with(|| NameHistory {
            name: known_name.clone(),
            labelhash: labelhash.to_owned(),
            first_name_ref: known_name_ref.clone(),
            current_registration: None,
            superseded_registration: None,
            current_wrapper_key: None,
            wrapper_authorities: BTreeMap::new(),
            current_registry_owner: None,
            current_resolver: None,
            current_record_version: None,
            open_binding: None,
            bindings: Vec::new(),
            events: Vec::new(),
            registry_resource_anchor: None,
            latest_registry_owner_ref: None,
            latest_registry_owner_before_registration: None,
        });
    if history.name.is_none() {
        history.name = known_name;
        if let Some(reference) = known_name_ref.clone() {
            history.first_name_ref.get_or_insert(reference);
        }
    }

    apply_observation(history, observation, block_index)?;
    let learned_name = history.name.clone();
    if let Some(name) = learned_name {
        namehash_to_labelhash.insert(name.namehash.clone(), labelhash.to_owned());
        if let Some(delta) = checkpoint_delta.as_deref_mut() {
            delta.mark_namehash_labelhash(name.namehash.clone());
            delta.mark_known_name(name.namehash.clone());
            if known_name_ref.is_some() {
                delta.mark_known_name_ref(name.namehash.clone());
            }
        }
        known_names_by_namehash
            .entry(name.namehash.clone())
            .or_insert_with(|| name.clone());
        if let Some(pending) = pending_namehash_observations.remove(&name.namehash) {
            if let Some(delta) = checkpoint_delta.as_deref_mut() {
                delta.mark_pending_observations(name.namehash.clone());
            }
            let name_ref = known_name_refs_by_namehash.get(&name.namehash).cloned();
            for pending_observation in pending {
                apply_authority_observation_for_history_key(
                    pending_observation,
                    &name.namehash,
                    labelhash,
                    Some(name.clone()),
                    name_ref.clone(),
                    histories,
                    known_names_by_namehash,
                    known_name_refs_by_namehash,
                    namehash_to_labelhash,
                    pending_namehash_observations,
                    same_tx_name_intro_positions,
                    block_index,
                    checkpoint_delta.as_deref_mut(),
                )?;
            }
        }
    }
    Ok(())
}

fn learn_record_raw_name_preimage(
    observation: &AuthorityObservation,
    reverse_claim_sources: &HashMap<String, ReverseClaimSource>,
    known_names_by_namehash: &mut HashMap<String, NameMetadata>,
    known_name_refs_by_namehash: &mut HashMap<String, ObservationRef>,
    namehash_to_labelhash: &mut HashMap<String, String>,
    mut checkpoint_delta: Option<&mut UnwrappedAuthorityReplayCheckpointDelta>,
) -> Option<NameMetadata> {
    let AuthorityObservation::RecordChanged(event) = observation else {
        return None;
    };
    if event.selector.record_key != "name" {
        return None;
    }
    if !reverse_claim_sources.contains_key(&event.namehash) {
        return None;
    }
    let raw_name = event.raw_name.as_deref()?;
    let name = observe_text_name_with_reference(raw_name, &event.reference, ENS_NORMALIZER_VERSION)
        .ok()?;
    let labelhash = name.labelhashes.first()?.clone();
    namehash_to_labelhash
        .entry(name.namehash.clone())
        .or_insert(labelhash);
    known_name_refs_by_namehash
        .entry(name.namehash.clone())
        .or_insert_with(|| event.reference.clone());
    known_names_by_namehash
        .entry(name.namehash.clone())
        .or_insert_with(|| name.clone());
    if let Some(delta) = checkpoint_delta.as_deref_mut() {
        delta.mark_namehash_labelhash(name.namehash.clone());
        delta.mark_known_name_ref(name.namehash.clone());
        delta.mark_known_name(name.namehash.clone());
    }
    Some(name)
}

fn should_defer_preloaded_namehash_observation(
    observation: &AuthorityObservation,
    same_tx_name_intro_positions: &HashMap<String, Vec<RawLogPosition>>,
    histories: &BTreeMap<String, NameHistory>,
    namehash_to_labelhash: &HashMap<String, String>,
) -> bool {
    let Some(namehash) = observation_namehash(observation) else {
        return false;
    };
    let Some(position) = observation_raw_log_position(observation) else {
        return false;
    };
    let normalized_namehash = namehash.to_ascii_lowercase();
    let has_later_same_tx_intro = same_tx_name_intro_positions
        .get(&normalized_namehash)
        .is_some_and(|positions| {
            positions.iter().any(|intro| {
                intro.block_hash == position.block_hash
                    && intro.transaction_hash == position.transaction_hash
                    && position.log_index < intro.log_index
            })
        });
    if !has_later_same_tx_intro {
        return false;
    }
    if namehash_to_labelhash.contains_key(&normalized_namehash)
        && let Some(history) = histories.get(&normalized_namehash)
        && history_has_authority_at_observation(history, observation)
    {
        return false;
    }
    true
}

fn observation_raw_log_position(observation: &AuthorityObservation) -> Option<RawLogPosition> {
    let reference = observation_reference(observation);
    Some(RawLogPosition {
        block_hash: reference.block_hash.clone(),
        transaction_hash: reference.transaction_hash.clone()?,
        log_index: reference.log_index?,
    })
}

fn observation_reference(observation: &AuthorityObservation) -> &ObservationRef {
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

fn history_has_authority_at_observation(
    history: &NameHistory,
    observation: &AuthorityObservation,
) -> bool {
    let reference = observation_reference(observation);
    if history.current_wrapper_key.is_some() {
        return active_anchor_for_history(history, &reference.chain_id).is_some();
    }
    if let Some(registration) = history.current_registration.as_ref() {
        if registration
            .release_ref
            .as_ref()
            .is_some_and(|release_ref| release_ref.block_timestamp <= reference.block_timestamp)
        {
            return registry_anchor_for_history(history, &reference.chain_id, &history.labelhash)
                .is_some();
        }
        return true;
    }
    if registry_anchor_for_history(history, &reference.chain_id, &history.labelhash).is_none() {
        return false;
    }
    registry_authority_started_before_observation(history, reference)
}

fn registry_authority_started_before_observation(
    history: &NameHistory,
    reference: &ObservationRef,
) -> bool {
    history
        .registry_resource_anchor
        .as_ref()
        .is_some_and(|anchor| anchor.block_number < reference.block_number)
        || nonzero_address(history.current_resolver.as_deref()).is_some()
}
