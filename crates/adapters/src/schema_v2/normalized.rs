use serde_json::json;

use super::{
    catalog::Selected,
    common::{derivation_kind, raw_fact_ref},
    manifest::{ManifestProvenance, ManifestSource},
    model::{BatchOutput, NormalizedEvent, RawBlockInput, RawLogInput},
    protocol::EventDraft,
    seam::PREIMAGE_OBSERVATION_EVENT_KIND,
    state::State,
    state_key::interpreter_state_key,
};

pub(super) fn materialize(
    selected: &Selected,
    raw: &RawLogInput,
    events: Vec<EventDraft>,
    state: &mut State,
    output: &mut BatchOutput,
) {
    materialize_for_source(&selected.source, raw, events, state, output);
}

pub(super) fn materialize_for_source(
    source: &ManifestSource,
    raw: &RawLogInput,
    events: Vec<EventDraft>,
    state: &mut State,
    output: &mut BatchOutput,
) {
    let provenance = ManifestProvenance::from_source(source);
    materialize_for_provenance(&provenance, raw, events, state, output);
}

pub(super) fn materialize_for_provenance(
    source: &ManifestProvenance,
    raw: &RawLogInput,
    events: Vec<EventDraft>,
    state: &mut State,
    output: &mut BatchOutput,
) {
    for (ordinal, draft) in events.into_iter().enumerate() {
        let before_state_explicit = draft.explicit_before.is_some();
        let before_state = state.transition(
            &source.namespace,
            draft.logical_name_id.as_deref(),
            draft.resource_id,
            &draft.event_kind,
            &source.source_family,
            &draft.state_scope,
            draft.explicit_before,
            draft.after_state.clone(),
        );
        let derivation = derivation_kind(&source.source_family, &draft.event_kind);
        let mut source_ref = raw_fact_ref(raw);
        source_ref
            .as_object_mut()
            .expect("raw fact reference is an object")
            .insert(
                "state_scope".to_owned(),
                serde_json::Value::String(draft.state_scope.clone()),
            );
        source_ref
            .as_object_mut()
            .expect("raw fact reference is an object")
            .insert(
                "interpreter_state_key".to_owned(),
                serde_json::Value::String(interpreter_state_key(
                    &source.namespace,
                    draft.logical_name_id.as_deref(),
                    draft.resource_id,
                    &draft.event_kind,
                    &source.source_family,
                    &draft.state_scope,
                )),
            );
        output.normalized_events.push(NormalizedEvent {
            event_identity: raw_log_event_identity(
                &source.source_family,
                source.manifest_id,
                raw,
                &draft.event_kind,
                &draft.identity_suffix,
                ordinal,
            ),
            namespace: source.namespace.clone(),
            logical_name_id: draft.logical_name_id,
            resource_id: draft.resource_id,
            event_kind: draft.event_kind,
            source_family: source.source_family.clone(),
            manifest_version: source.manifest_version,
            source_manifest_id: Some(source.manifest_id),
            chain_id: raw.chain_id.clone(),
            block_number: Some(raw.block_number),
            block_hash: Some(raw.block_hash.clone()),
            transaction_hash: Some(raw.transaction_hash.clone()),
            transaction_index: Some(raw.transaction_index),
            log_index: Some(raw.log_index),
            raw_fact_ref: source_ref,
            derivation_kind: derivation.to_owned(),
            canonicality_state: raw.canonicality_state.clone(),
            before_state,
            after_state: draft.after_state,
            migration_correlation_ids: Vec::new(),
            consumer_visibility: "activated".to_owned(),
            before_state_explicit,
        });
    }
}

pub(super) fn raw_log_event_identity(
    source_family: &str,
    source_manifest_id: i64,
    raw: &RawLogInput,
    event_kind: &str,
    identity_suffix: &str,
    ordinal: usize,
) -> String {
    let derivation = derivation_kind(source_family, event_kind);
    format!(
        "{derivation}:{source_manifest_id}:{}:{}:{}:{}:{identity_suffix}:{ordinal}",
        raw.chain_id, raw.block_hash, raw.transaction_hash, raw.log_index,
    )
}

#[rustfmt::skip]
pub(super) fn materialize_boundary(
    source: &ManifestSource,
    block: &RawBlockInput,
    events: Vec<EventDraft>,
    state: &mut State,
    output: &mut BatchOutput,
) {
    for (ordinal, draft) in events.into_iter().enumerate() {
        let derivation = derivation_kind(&source.source_family, &draft.event_kind);
        let identity_prefix = format!("{derivation}:{}:{}:{}:{}:", source.manifest_id, block.chain_id, block.block_hash, draft.identity_suffix);
        let ordinal = ordinal + output.normalized_events.iter().filter(|event| event.event_identity.starts_with(&identity_prefix)).count();
        let before_state_explicit = draft.explicit_before.is_some();
        let state_key = interpreter_state_key(
            &source.namespace,
            draft.logical_name_id.as_deref(),
            draft.resource_id,
            &draft.event_kind,
            &source.source_family,
            &draft.state_scope,
        );
        let before_state = state.transition(
            &source.namespace,
            draft.logical_name_id.as_deref(),
            draft.resource_id,
            &draft.event_kind,
            &source.source_family,
            &draft.state_scope,
            draft.explicit_before,
            draft.after_state.clone(),
        );
        output.normalized_events.push(NormalizedEvent {
            event_identity: format!(
                "{derivation}:{}:{}:{}:{}:{ordinal}",
                source.manifest_id, block.chain_id, block.block_hash, draft.identity_suffix,
            ),
            namespace: source.namespace.clone(),
            logical_name_id: draft.logical_name_id,
            resource_id: draft.resource_id,
            event_kind: draft.event_kind,
            source_family: source.source_family.clone(),
            manifest_version: source.manifest_version,
            source_manifest_id: Some(source.manifest_id),
            chain_id: block.chain_id.clone(),
            block_number: Some(block.block_number),
            block_hash: Some(block.block_hash.clone()),
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            raw_fact_ref: json!({
                "kind":"raw_block",
                "chain_id":block.chain_id,
                "block_hash":block.block_hash,
                "block_number":block.block_number,
                "block_timestamp":block.block_timestamp.unix_timestamp(),
                "state_scope":draft.state_scope,
                "interpreter_state_key":state_key,
            }),
            derivation_kind: derivation.to_owned(),
            canonicality_state: block.canonicality_state.clone(),
            before_state,
            after_state: draft.after_state,
            migration_correlation_ids: Vec::new(),
            consumer_visibility: "activated".to_owned(),
            before_state_explicit,
        });
    }
}

pub(super) fn preimage_event(
    selected: &Selected,
    raw: &RawLogInput,
    logical_name_id: Option<String>,
    identity_suffix: &str,
    after_state: serde_json::Value,
) -> NormalizedEvent {
    let state_scope = match (
        after_state
            .get("resolver")
            .and_then(serde_json::Value::as_str),
        after_state
            .get("upstream_resource")
            .and_then(serde_json::Value::as_str),
    ) {
        (Some(resolver), Some(resource)) => {
            format!("{}:preimage:{resource}", resolver.to_ascii_lowercase())
        }
        _ => format!(
            "{}:preimage:{}",
            raw.emitting_address.to_ascii_lowercase(),
            after_state
                .get("namehash")
                .or_else(|| after_state.get("labelhash"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or(identity_suffix)
        ),
    };
    let mut source_ref = raw_fact_ref(raw);
    source_ref
        .as_object_mut()
        .expect("raw fact reference is an object")
        .insert(
            "state_scope".to_owned(),
            serde_json::Value::String(state_scope.clone()),
        );
    source_ref
        .as_object_mut()
        .expect("raw fact reference is an object")
        .insert(
            "interpreter_state_key".to_owned(),
            serde_json::Value::String(interpreter_state_key(
                &selected.source.namespace,
                logical_name_id.as_deref(),
                None,
                PREIMAGE_OBSERVATION_EVENT_KIND,
                &selected.source.source_family,
                &state_scope,
            )),
        );
    NormalizedEvent {
        event_identity: format!(
            "raw_log_preimage_observation:{}:{}:{}:{}:{}:{identity_suffix}",
            selected.source.manifest_id,
            raw.chain_id,
            raw.block_hash,
            raw.transaction_hash,
            raw.log_index,
        ),
        namespace: selected.source.namespace.clone(),
        logical_name_id,
        resource_id: None,
        event_kind: PREIMAGE_OBSERVATION_EVENT_KIND.to_owned(),
        source_family: selected.source.source_family.clone(),
        manifest_version: selected.source.manifest_version,
        source_manifest_id: Some(selected.source.manifest_id),
        chain_id: raw.chain_id.clone(),
        block_number: Some(raw.block_number),
        block_hash: Some(raw.block_hash.clone()),
        transaction_hash: Some(raw.transaction_hash.clone()),
        transaction_index: Some(raw.transaction_index),
        log_index: Some(raw.log_index),
        raw_fact_ref: source_ref,
        derivation_kind: "raw_log_preimage_observation".to_owned(),
        canonicality_state: raw.canonicality_state.clone(),
        before_state: json!({}),
        after_state,
        migration_correlation_ids: Vec::new(),
        consumer_visibility: "activated".to_owned(),
        // Preimage observations never enter the interpreter state stream; their empty before is
        // fixed, so the re-thread must leave it alone.
        before_state_explicit: true,
    }
}

pub(super) fn boundary_preimage_event(
    source: &ManifestSource,
    block: &RawBlockInput,
    logical_name_id: Option<String>,
    identity_suffix: &str,
    after_state: serde_json::Value,
) -> NormalizedEvent {
    let state_scope = format!("raw-block:preimage:{identity_suffix}");
    let state_key = interpreter_state_key(
        &source.namespace,
        logical_name_id.as_deref(),
        None,
        PREIMAGE_OBSERVATION_EVENT_KIND,
        &source.source_family,
        &state_scope,
    );
    NormalizedEvent {
        event_identity: format!(
            "raw_block_preimage_observation:{}:{}:{}:{identity_suffix}",
            source.manifest_id, block.chain_id, block.block_hash,
        ),
        namespace: source.namespace.clone(),
        logical_name_id,
        resource_id: None,
        event_kind: PREIMAGE_OBSERVATION_EVENT_KIND.to_owned(),
        source_family: source.source_family.clone(),
        manifest_version: source.manifest_version,
        source_manifest_id: Some(source.manifest_id),
        chain_id: block.chain_id.clone(),
        block_number: Some(block.block_number),
        block_hash: Some(block.block_hash.clone()),
        transaction_hash: None,
        transaction_index: None,
        log_index: None,
        raw_fact_ref: json!({
            "kind":"raw_block",
            "chain_id":block.chain_id,
            "block_hash":block.block_hash,
            "block_number":block.block_number,
            "block_timestamp":block.block_timestamp.unix_timestamp(),
            "state_scope":state_scope,
            "interpreter_state_key":state_key,
        }),
        derivation_kind: "raw_block_preimage_observation".to_owned(),
        canonicality_state: block.canonicality_state.clone(),
        before_state: json!({}),
        after_state,
        migration_correlation_ids: Vec::new(),
        consumer_visibility: "activated".to_owned(),
        before_state_explicit: true,
    }
}
