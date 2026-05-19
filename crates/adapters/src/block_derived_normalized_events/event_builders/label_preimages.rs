use anyhow::{Context, Result, bail};
use bigname_storage::NormalizedEvent;

use super::{
    LabelRegistered, LabelReserved, NameRegistered_0, NameRegistered_1, NameRenewed_0,
    NameRenewed_1, ParentUpdated, build_preimage_observed_normalized_event, decode_event_log,
    log_context,
};
use crate::block_derived_normalized_events::constants::{
    ENS_V1_NAME_REGISTERED_SIGNATURE, ENS_V1_NAME_RENEWED_SIGNATURE,
    ENS_V2_NAME_REGISTERED_SIGNATURE, ENS_V2_NAME_RENEWED_SIGNATURE, LABEL_REGISTERED_SIGNATURE,
    LABEL_RESERVED_SIGNATURE, PARENT_UPDATED_SIGNATURE, SOURCE_EVENT_LABEL_REGISTERED,
    SOURCE_EVENT_LABEL_RESERVED, SOURCE_EVENT_NAME_REGISTERED, SOURCE_EVENT_NAME_RENEWED,
    SOURCE_EVENT_PARENT_UPDATED, SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
    SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V2_ROOT_L1,
};
use crate::block_derived_normalized_events::event_topics::PreimageObservedEventTopics;
use crate::block_derived_normalized_events::preimage_observation::{
    can_observe_dns_label, observe_registrar_eth_name, observe_single_label,
};
use crate::block_derived_normalized_events::types::{PreimageObservation, WatchedRawLogRow};

#[derive(Clone, Copy)]
struct LabelPreimageSpec {
    source_event: &'static str,
    signature: &'static str,
    observation_kind: LabelPreimageObservationKind,
    derivation_context: &'static str,
    indexed_labelhash_topic: Option<usize>,
    missing_labelhash_context: Option<&'static str>,
}

#[derive(Clone, Copy)]
enum LabelPreimageObservationKind {
    RegistrarEthName,
    SingleLabel,
}

const ENS_V1_REGISTRAR_LABEL_PREIMAGE_EVENTS: &[LabelPreimageSpec] = &[
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_NAME_REGISTERED,
        signature: ENS_V1_NAME_REGISTERED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::RegistrarEthName,
        derivation_context: "failed to derive registrar .eth preimage",
        indexed_labelhash_topic: Some(1),
        missing_labelhash_context: Some("registrar observation is missing the explicit labelhash"),
    },
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_NAME_RENEWED,
        signature: ENS_V1_NAME_RENEWED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::RegistrarEthName,
        derivation_context: "failed to derive registrar .eth preimage",
        indexed_labelhash_topic: Some(1),
        missing_labelhash_context: Some("registrar observation is missing the explicit labelhash"),
    },
];

const ENS_V2_REGISTRY_LABEL_PREIMAGE_EVENTS: &[LabelPreimageSpec] = &[
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_LABEL_REGISTERED,
        signature: LABEL_REGISTERED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::SingleLabel,
        derivation_context: "failed to derive ENSv2 registry label preimage",
        indexed_labelhash_topic: Some(2),
        missing_labelhash_context: Some(
            "ENSv2 registry observation is missing the explicit labelhash",
        ),
    },
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_LABEL_RESERVED,
        signature: LABEL_RESERVED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::SingleLabel,
        derivation_context: "failed to derive ENSv2 registry label preimage",
        indexed_labelhash_topic: Some(2),
        missing_labelhash_context: Some(
            "ENSv2 registry observation is missing the explicit labelhash",
        ),
    },
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_PARENT_UPDATED,
        signature: PARENT_UPDATED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::SingleLabel,
        derivation_context: "failed to derive ENSv2 registry parent label preimage",
        indexed_labelhash_topic: None,
        missing_labelhash_context: None,
    },
];

const ENS_V2_REGISTRAR_LABEL_PREIMAGE_EVENTS: &[LabelPreimageSpec] = &[
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_NAME_REGISTERED,
        signature: ENS_V2_NAME_REGISTERED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::RegistrarEthName,
        derivation_context: "failed to derive ENSv2 registrar .eth preimage",
        indexed_labelhash_topic: None,
        missing_labelhash_context: None,
    },
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_NAME_RENEWED,
        signature: ENS_V2_NAME_RENEWED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::RegistrarEthName,
        derivation_context: "failed to derive ENSv2 registrar .eth preimage",
        indexed_labelhash_topic: None,
        missing_labelhash_context: None,
    },
];

pub(super) fn build_registrar_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    event_topics: &PreimageObservedEventTopics,
) -> Result<Vec<NormalizedEvent>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(Vec::new());
    };
    try_build_label_preimage_observed_events(
        raw_log,
        event_topics,
        topic0,
        ENS_V1_REGISTRAR_LABEL_PREIMAGE_EVENTS,
    )
    .map(Option::unwrap_or_default)
}

pub(super) fn try_build_ens_v2_registry_label_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    event_topics: &PreimageObservedEventTopics,
    topic0: &str,
) -> Result<Option<Vec<NormalizedEvent>>> {
    try_build_label_preimage_observed_events(
        raw_log,
        event_topics,
        topic0,
        ENS_V2_REGISTRY_LABEL_PREIMAGE_EVENTS,
    )
}

pub(super) fn try_build_ens_v2_registrar_label_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    event_topics: &PreimageObservedEventTopics,
    topic0: &str,
) -> Result<Option<Vec<NormalizedEvent>>> {
    try_build_label_preimage_observed_events(
        raw_log,
        event_topics,
        topic0,
        ENS_V2_REGISTRAR_LABEL_PREIMAGE_EVENTS,
    )
}

fn try_build_label_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    event_topics: &PreimageObservedEventTopics,
    topic0: &str,
    specs: &[LabelPreimageSpec],
) -> Result<Option<Vec<NormalizedEvent>>> {
    for spec in specs {
        if event_topics.matches(raw_log, spec.signature, topic0)? {
            return build_label_preimage_observed_events(raw_log, spec).map(Some);
        }
    }
    Ok(None)
}

fn build_label_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    spec: &LabelPreimageSpec,
) -> Result<Vec<NormalizedEvent>> {
    let Some(label) = decode_observable_event_label(raw_log, spec.signature) else {
        return Ok(Vec::new());
    };
    let observation = spec
        .observation_kind
        .observe(&label)
        .with_context(|| log_context(raw_log, spec.derivation_context))?;

    validate_indexed_labelhash(raw_log, spec, &observation)?;

    Ok(vec![build_preimage_observed_normalized_event(
        raw_log,
        spec.source_event,
        observation,
        None,
    )])
}

impl LabelPreimageObservationKind {
    fn observe(self, label: &str) -> Result<PreimageObservation> {
        match self {
            Self::RegistrarEthName => observe_registrar_eth_name(label),
            Self::SingleLabel => observe_single_label(label),
        }
    }
}

fn validate_indexed_labelhash(
    raw_log: &WatchedRawLogRow,
    spec: &LabelPreimageSpec,
    observation: &PreimageObservation,
) -> Result<()> {
    let Some(indexed_labelhash_topic) = spec.indexed_labelhash_topic else {
        return Ok(());
    };
    let observed_labelhash = observation.labelhashes.first().context(
        spec.missing_labelhash_context
            .unwrap_or("preimage observation is missing the explicit labelhash"),
    )?;
    if let Some(indexed_labelhash) = raw_log.topics.get(indexed_labelhash_topic)
        && !indexed_labelhash.eq_ignore_ascii_case(observed_labelhash)
    {
        bail!(
            "{} indexed labelhash {} does not match decoded labelhash {} for chain {} block {} log {}",
            spec.source_event,
            indexed_labelhash,
            observed_labelhash,
            raw_log.chain_id,
            raw_log.block_hash,
            raw_log.log_index
        );
    }
    Ok(())
}

fn decode_observable_event_label(raw_log: &WatchedRawLogRow, signature: &str) -> Option<String> {
    let label = decode_event_label(raw_log, signature)?;
    if can_observe_dns_label(&label) {
        Some(label)
    } else {
        None
    }
}

fn decode_event_label(raw_log: &WatchedRawLogRow, signature: &str) -> Option<String> {
    match (raw_log.source_family.as_str(), signature) {
        (SOURCE_FAMILY_ENS_V1_REGISTRAR_L1, ENS_V1_NAME_REGISTERED_SIGNATURE) => {
            decode_event_log::<NameRegistered_0>(raw_log, "ENSv1 NameRegistered log is malformed")
                .ok()
                .map(|event| event.name)
        }
        (SOURCE_FAMILY_ENS_V1_REGISTRAR_L1, ENS_V1_NAME_RENEWED_SIGNATURE) => {
            decode_event_log::<NameRenewed_0>(raw_log, "ENSv1 NameRenewed log is malformed")
                .ok()
                .map(|event| event.name)
        }
        (
            SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
            LABEL_REGISTERED_SIGNATURE,
        ) => decode_event_log::<LabelRegistered>(raw_log, "LabelRegistered log is malformed")
            .ok()
            .map(|event| event.label),
        (
            SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
            LABEL_RESERVED_SIGNATURE,
        ) => decode_event_log::<LabelReserved>(raw_log, "LabelReserved log is malformed")
            .ok()
            .map(|event| event.label),
        (
            SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
            PARENT_UPDATED_SIGNATURE,
        ) => decode_event_log::<ParentUpdated>(raw_log, "ParentUpdated log is malformed")
            .ok()
            .map(|event| event.label),
        (SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, ENS_V2_NAME_REGISTERED_SIGNATURE) => {
            decode_event_log::<NameRegistered_1>(raw_log, "ENSv2 NameRegistered log is malformed")
                .ok()
                .map(|event| event.label)
        }
        (SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, ENS_V2_NAME_RENEWED_SIGNATURE) => {
            decode_event_log::<NameRenewed_1>(raw_log, "ENSv2 NameRenewed log is malformed")
                .ok()
                .map(|event| event.label)
        }
        _ => None,
    }
}
