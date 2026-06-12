use anyhow::{Context, Result, bail};
use bigname_storage::NormalizedEvent;

use super::build_preimage_observed_normalized_event;
use crate::block_derived_normalized_events::constants::{
    BASENAMES_NAME_REGISTERED_SIGNATURE, BASENAMES_NAME_RENEWED_SIGNATURE,
    ENS_V1_NAME_REGISTERED_SIGNATURE, ENS_V1_NAME_RENEWED_SIGNATURE,
    ENS_V1_UNWRAPPED_NAME_REGISTERED_SIGNATURE, ENS_V1_UNWRAPPED_NAME_RENEWED_SIGNATURE,
    ENS_V1_WRAPPED_NAME_REGISTERED_SIGNATURE, ENS_V2_NAME_REGISTERED_SIGNATURE,
    ENS_V2_NAME_RENEWED_SIGNATURE, LABEL_REGISTERED_SIGNATURE, LABEL_RESERVED_SIGNATURE,
    PARENT_UPDATED_SIGNATURE, SOURCE_EVENT_LABEL_REGISTERED, SOURCE_EVENT_LABEL_RESERVED,
    SOURCE_EVENT_NAME_REGISTERED, SOURCE_EVENT_NAME_RENEWED, SOURCE_EVENT_PARENT_UPDATED,
    SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR, SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
    SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V2_ROOT_L1,
};
use crate::block_derived_normalized_events::event_topics::PreimageObservedEventTopics;
use crate::block_derived_normalized_events::preimage_observation::{
    can_observe_dns_label, observe_registrar_base_name, observe_registrar_eth_name,
    observe_single_label,
};
use crate::block_derived_normalized_events::types::{PreimageObservation, WatchedRawLogRow};

#[derive(Clone, Copy)]
struct LabelPreimageSpec {
    source_event: &'static str,
    signature: &'static str,
    observation_kind: LabelPreimageObservationKind,
    indexed_labelhash_topic: Option<usize>,
    missing_labelhash_context: Option<&'static str>,
}

#[derive(Clone, Copy)]
enum LabelPreimageObservationKind {
    RegistrarEnsName,
    RegistrarBaseName,
    SingleLabel,
}

const ENS_V1_REGISTRAR_LABEL_PREIMAGE_EVENTS: &[LabelPreimageSpec] = &[
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_NAME_REGISTERED,
        signature: ENS_V1_NAME_REGISTERED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::RegistrarEnsName,
        indexed_labelhash_topic: Some(1),
        missing_labelhash_context: Some("registrar observation is missing the explicit labelhash"),
    },
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_NAME_REGISTERED,
        signature: ENS_V1_WRAPPED_NAME_REGISTERED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::RegistrarEnsName,
        indexed_labelhash_topic: Some(1),
        missing_labelhash_context: Some("registrar observation is missing the explicit labelhash"),
    },
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_NAME_REGISTERED,
        signature: ENS_V1_UNWRAPPED_NAME_REGISTERED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::RegistrarEnsName,
        indexed_labelhash_topic: Some(1),
        missing_labelhash_context: Some("registrar observation is missing the explicit labelhash"),
    },
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_NAME_RENEWED,
        signature: ENS_V1_NAME_RENEWED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::RegistrarEnsName,
        indexed_labelhash_topic: Some(1),
        missing_labelhash_context: Some("registrar observation is missing the explicit labelhash"),
    },
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_NAME_RENEWED,
        signature: ENS_V1_UNWRAPPED_NAME_RENEWED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::RegistrarEnsName,
        indexed_labelhash_topic: Some(1),
        missing_labelhash_context: Some("registrar observation is missing the explicit labelhash"),
    },
];

const BASENAMES_REGISTRAR_LABEL_PREIMAGE_EVENTS: &[LabelPreimageSpec] = &[
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_NAME_REGISTERED,
        signature: BASENAMES_NAME_REGISTERED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::RegistrarBaseName,
        indexed_labelhash_topic: Some(1),
        missing_labelhash_context: Some(
            "Basenames registrar observation is missing the explicit labelhash",
        ),
    },
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_NAME_RENEWED,
        signature: BASENAMES_NAME_RENEWED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::RegistrarBaseName,
        indexed_labelhash_topic: Some(1),
        missing_labelhash_context: Some(
            "Basenames registrar observation is missing the explicit labelhash",
        ),
    },
];

const ENS_V2_REGISTRY_LABEL_PREIMAGE_EVENTS: &[LabelPreimageSpec] = &[
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_LABEL_REGISTERED,
        signature: LABEL_REGISTERED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::SingleLabel,
        indexed_labelhash_topic: Some(2),
        missing_labelhash_context: Some(
            "ENSv2 registry observation is missing the explicit labelhash",
        ),
    },
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_LABEL_RESERVED,
        signature: LABEL_RESERVED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::SingleLabel,
        indexed_labelhash_topic: Some(2),
        missing_labelhash_context: Some(
            "ENSv2 registry observation is missing the explicit labelhash",
        ),
    },
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_PARENT_UPDATED,
        signature: PARENT_UPDATED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::SingleLabel,
        indexed_labelhash_topic: None,
        missing_labelhash_context: None,
    },
];

const ENS_V2_REGISTRAR_LABEL_PREIMAGE_EVENTS: &[LabelPreimageSpec] = &[
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_NAME_REGISTERED,
        signature: ENS_V2_NAME_REGISTERED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::RegistrarEnsName,
        indexed_labelhash_topic: None,
        missing_labelhash_context: None,
    },
    LabelPreimageSpec {
        source_event: SOURCE_EVENT_NAME_RENEWED,
        signature: ENS_V2_NAME_RENEWED_SIGNATURE,
        observation_kind: LabelPreimageObservationKind::RegistrarEnsName,
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
        match raw_log.source_family.as_str() {
            SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 => ENS_V1_REGISTRAR_LABEL_PREIMAGE_EVENTS,
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR => BASENAMES_REGISTRAR_LABEL_PREIMAGE_EVENTS,
            _ => &[],
        },
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
    let Ok(observation) = spec.observation_kind.observe(&label) else {
        return Ok(Vec::new());
    };

    if !indexed_labelhash_matches(raw_log, spec, &observation)? {
        return Ok(Vec::new());
    }

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
            Self::RegistrarEnsName => observe_registrar_eth_name(label),
            Self::RegistrarBaseName => observe_registrar_base_name(label),
            Self::SingleLabel => observe_single_label(label),
        }
    }
}

fn indexed_labelhash_matches(
    raw_log: &WatchedRawLogRow,
    spec: &LabelPreimageSpec,
    observation: &PreimageObservation,
) -> Result<bool> {
    let Some(indexed_labelhash_topic) = spec.indexed_labelhash_topic else {
        return Ok(true);
    };
    let observed_labelhash = observation.labelhashes.first().context(
        spec.missing_labelhash_context
            .unwrap_or("preimage observation is missing the explicit labelhash"),
    )?;
    let Some(indexed_labelhash) = raw_log.topics.get(indexed_labelhash_topic) else {
        return Ok(false);
    };
    if !indexed_labelhash.eq_ignore_ascii_case(observed_labelhash) {
        return Ok(false);
    }
    Ok(true)
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
        (
            SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
            ENS_V1_NAME_REGISTERED_SIGNATURE
            | ENS_V1_WRAPPED_NAME_REGISTERED_SIGNATURE
            | ENS_V1_UNWRAPPED_NAME_REGISTERED_SIGNATURE
            | ENS_V1_NAME_RENEWED_SIGNATURE
            | ENS_V1_UNWRAPPED_NAME_RENEWED_SIGNATURE,
        )
        | (
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
            BASENAMES_NAME_REGISTERED_SIGNATURE | BASENAMES_NAME_RENEWED_SIGNATURE,
        )
        | (
            SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
            LABEL_REGISTERED_SIGNATURE | LABEL_RESERVED_SIGNATURE | PARENT_UPDATED_SIGNATURE,
        )
        | (
            SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
            ENS_V2_NAME_REGISTERED_SIGNATURE | ENS_V2_NAME_RENEWED_SIGNATURE,
        ) => decode_first_abi_string(&raw_log.data).ok(),
        _ => None,
    }
}

fn decode_first_abi_string(data: &[u8]) -> Result<String> {
    let offset = abi_word_usize(
        data.get(..32)
            .context("label-bearing event data is missing string offset")?,
    )?;
    let length_offset = offset
        .checked_add(32)
        .context("label-bearing event string offset overflowed")?;
    let length = abi_word_usize(
        data.get(offset..length_offset)
            .context("label-bearing event data is missing string length")?,
    )?;
    let end = length_offset
        .checked_add(length)
        .context("label-bearing event string length overflowed")?;
    let bytes = data
        .get(length_offset..end)
        .context("label-bearing event data is shorter than declared string length")?;
    String::from_utf8(bytes.to_vec()).context("label-bearing event label is not valid utf-8")
}

fn abi_word_usize(word: &[u8]) -> Result<usize> {
    if word.len() != 32 {
        bail!("ABI word must be 32 bytes");
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word is too large for this decoder");
    }
    let mut value = [0u8; 8];
    value.copy_from_slice(&word[24..]);
    Ok(u64::from_be_bytes(value) as usize)
}
