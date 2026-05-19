use super::*;

pub(super) fn selected_registrar_event_identities(
    raw_logs: &[AuthorityRawLogRow],
    event_topics: &AuthorityEventTopics,
) -> Result<Vec<String>> {
    let mut earliest_selected_position_by_namehash =
        HashMap::<String, SelectedReplayPosition>::new();
    for raw_log in raw_logs {
        let Some(observation) = build_authority_observation(raw_log, event_topics)? else {
            continue;
        };
        let Some(namehash) = selected_replay_observation_namehash(&observation)? else {
            continue;
        };
        let position = SelectedReplayPosition::from(raw_log);
        earliest_selected_position_by_namehash
            .entry(namehash)
            .and_modify(|existing| {
                if position < *existing {
                    *existing = position;
                }
            })
            .or_insert(position);
    }

    let mut identities = BTreeSet::<String>::new();
    for raw_log in raw_logs {
        let Some(observation) = build_authority_observation(raw_log, event_topics)? else {
            continue;
        };
        let Some(namehash) = selected_replay_observation_namehash(&observation)? else {
            continue;
        };
        let position = SelectedReplayPosition::from(raw_log);
        if earliest_selected_position_by_namehash
            .get(&namehash)
            .is_some_and(|earliest| position > *earliest)
        {
            continue;
        }
        match observation {
            AuthorityObservation::RegistrationRenewed(_) => {
                identities.insert(raw_log_event_identity(
                    raw_log,
                    EVENT_KIND_REGISTRATION_RENEWED,
                    "renewal",
                ));
                identities.insert(raw_log_event_identity(
                    raw_log,
                    EVENT_KIND_EXPIRY_CHANGED,
                    "expiry",
                ));
            }
            AuthorityObservation::TokenTransferred(_) => {
                identities.insert(raw_log_event_identity(
                    raw_log,
                    EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
                    "token-transfer",
                ));
            }
            _ => {}
        }
    }
    Ok(identities.into_iter().collect())
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SelectedReplayPosition {
    block_number: i64,
    transaction_index: i64,
    log_index: i64,
}

impl From<&AuthorityRawLogRow> for SelectedReplayPosition {
    fn from(raw_log: &AuthorityRawLogRow) -> Self {
        Self {
            block_number: raw_log.block_number,
            transaction_index: raw_log.transaction_index,
            log_index: raw_log.log_index,
        }
    }
}

fn selected_replay_observation_namehash(
    observation: &AuthorityObservation,
) -> Result<Option<String>> {
    if let Some(namehash) = observation_namehash(observation) {
        return Ok(Some(namehash.to_ascii_lowercase()));
    }

    match observation {
        AuthorityObservation::RegistrationGranted(value) => Ok(Some(
            observe_registrar_name_with_reference(
                &value.label,
                &value.reference,
                ENS_NORMALIZER_VERSION,
            )?
            .namehash,
        )),
        AuthorityObservation::RegistrationRenewed(value) => Ok(Some(
            observe_registrar_name_with_reference(
                &value.label,
                &value.reference,
                ENS_NORMALIZER_VERSION,
            )?
            .namehash,
        )),
        AuthorityObservation::TokenTransferred(value) => Ok(Some(
            registrar_child_namehash_for_reference(&value.reference, &value.labelhash)?,
        )),
        AuthorityObservation::RegistryOwnerChanged(value) => Ok(Some(
            registrar_child_namehash_for_reference(&value.reference, &value.labelhash)?,
        )),
        AuthorityObservation::WrapperNameWrapped(value) => {
            Ok(Some(value.name.namehash.to_ascii_lowercase()))
        }
        AuthorityObservation::ResolverChanged(_)
        | AuthorityObservation::RecordChanged(_)
        | AuthorityObservation::RecordVersionChanged(_)
        | AuthorityObservation::WrapperNameUnwrapped(_)
        | AuthorityObservation::WrapperFusesSet(_)
        | AuthorityObservation::WrapperExpiryExtended(_)
        | AuthorityObservation::WrapperTokenTransferred(_) => Ok(None),
    }
}

fn registrar_child_namehash_for_reference(
    reference: &ObservationRef,
    labelhash: &str,
) -> Result<String> {
    let profile =
        authority_profile_for_source_family(&reference.source_family).with_context(|| {
            format!(
                "unsupported authority source family {}",
                reference.source_family
            )
        })?;
    child_namehash_hex(&profile.root_node(), labelhash)
}

#[cfg(test)]
mod tests;
