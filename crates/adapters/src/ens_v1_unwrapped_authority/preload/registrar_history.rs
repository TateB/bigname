use super::support::*;
use super::*;

pub(in crate::ens_v1_unwrapped_authority) fn empty_preloaded_history(
    labelhash: String,
    name: Option<NameMetadata>,
) -> NameHistory {
    let namehash = name
        .as_ref()
        .map(|name| name.namehash.clone())
        .unwrap_or_default();
    NameHistory {
        name,
        namehash,
        labelhash,
        first_name_ref: None,
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
    }
}

pub(in crate::ens_v1_unwrapped_authority) fn preload_registrar_history(
    history: &mut NameHistory,
    provenance: &Value,
    binding_ref: &BoundaryRef,
    surface_binding_id: Uuid,
    binding_active_to: Option<OffsetDateTime>,
    registrar_state: Option<&PreloadedRegistrarState>,
    block_index: &CanonicalBlockIndex,
) -> Result<()> {
    let provenance_authority_key = provenance_string(provenance, "authority_key")?;
    let authority_key = registrar_state
        .and_then(|state| state.authority_key.as_deref())
        .unwrap_or(&provenance_authority_key)
        .to_owned();
    let labelhash = registrar_labelhash_from_provenance_or_authority_key(
        provenance,
        &authority_key,
        &history.labelhash,
    );
    let expiry = if let Some(expiry) = registrar_state.and_then(|state| state.expiry) {
        expiry
    } else {
        let expiry =
            registrar_expiry_from_provenance_or_binding_end(provenance, binding_active_to)?;
        OffsetDateTime::from_unix_timestamp(expiry)
            .context("preloaded registrar expiry is not a valid unix timestamp")?
    };
    let registrant = registrar_state
        .and_then(|state| state.registrant.as_deref())
        .or_else(|| provenance.get("registrant").and_then(Value::as_str))
        .unwrap_or(ZERO_ADDRESS)
        .to_owned();
    let source_manifest_id = manifest_id_from_authority_key(&authority_key).unwrap_or(0);
    let source_family = default_registrar_source_family(&binding_ref.namespace).to_owned();
    let start_ref = registrar_state
        .and_then(|state| state.start_ref.clone())
        .unwrap_or_else(|| {
            observation_ref_from_boundary(
                binding_ref,
                Some(source_family),
                Some(source_manifest_id),
                log_index_from_authority_key(&authority_key),
            )
        });
    let lease = RegistrationLease {
        authority_key,
        labelhash,
        registrant,
        expiry,
        release_ref: block_index
            .first_block_after(release_after_grace(expiry)?, &binding_ref.namespace),
        start_ref,
    };
    let anchor = build_registrar_anchor(&lease);
    history.current_registration = Some(lease);
    history.superseded_registration = None;
    history.open_binding = Some(OpenBinding {
        surface_binding_id,
        authority: anchor,
        active_from: binding_ref.block_timestamp,
        anchor_ref: binding_ref.clone(),
    });
    Ok(())
}

pub(in crate::ens_v1_unwrapped_authority) fn preload_selected_registrar_lease(
    history: &mut NameHistory,
    registrar_state: Option<&PreloadedRegistrarState>,
    block_index: &CanonicalBlockIndex,
) -> Result<()> {
    if history.current_registration.is_some() || history.superseded_registration.is_some() {
        return Ok(());
    }
    if let Some(lease) = preloaded_registrar_lease(history, registrar_state, block_index)? {
        history.current_registration = Some(lease);
        history.superseded_registration = None;
    }

    Ok(())
}

pub(in crate::ens_v1_unwrapped_authority) fn preload_superseded_registrar_lease(
    history: &mut NameHistory,
    registrar_state: Option<&PreloadedRegistrarState>,
    block_index: &CanonicalBlockIndex,
) -> Result<()> {
    if history.current_registration.is_some() || history.superseded_registration.is_some() {
        return Ok(());
    }
    if let Some(lease) = preloaded_registrar_lease(history, registrar_state, block_index)? {
        history.current_registration = None;
        history.superseded_registration = Some(lease);
    }

    Ok(())
}

fn preloaded_registrar_lease(
    history: &NameHistory,
    registrar_state: Option<&PreloadedRegistrarState>,
    block_index: &CanonicalBlockIndex,
) -> Result<Option<RegistrationLease>> {
    let Some(state) = registrar_state else {
        return Ok(None);
    };
    let (Some(authority_key), Some(expiry), Some(start_ref)) = (
        state.authority_key.as_ref(),
        state.expiry,
        state.start_ref.as_ref(),
    ) else {
        return Ok(None);
    };

    let labelhash = state
        .labelhash
        .clone()
        .or_else(|| registrar_labelhash_from_authority_key(authority_key))
        .unwrap_or_else(|| history.labelhash.clone());
    let registrant = state
        .registrant
        .clone()
        .unwrap_or_else(|| ZERO_ADDRESS.to_owned());
    Ok(Some(RegistrationLease {
        authority_key: authority_key.clone(),
        labelhash,
        registrant,
        expiry,
        release_ref: block_index
            .first_block_after(release_after_grace(expiry)?, &start_ref.namespace),
        start_ref: start_ref.clone(),
    }))
}

pub(super) fn registrar_labelhash_from_provenance_or_authority_key(
    provenance: &Value,
    authority_key: &str,
    history_labelhash: &str,
) -> String {
    provenance
        .get("labelhash")
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_lowercase())
        .or_else(|| registrar_labelhash_from_authority_key(authority_key))
        .unwrap_or_else(|| history_labelhash.to_owned())
}

pub(super) fn registrar_expiry_from_provenance_or_binding_end(
    provenance: &Value,
    binding_active_to: Option<OffsetDateTime>,
) -> Result<i64> {
    if let Some(expiry) = provenance.get("expiry").and_then(Value::as_i64) {
        return Ok(expiry);
    }
    if let Some(released_at) = provenance.get("released_at").and_then(Value::as_i64) {
        return released_at
            .checked_sub(ENS_GRACE_PERIOD_SECS)
            .context("preloaded registrar released_at cannot be converted to expiry");
    }
    if let Some(active_to) = binding_active_to {
        return active_to
            .unix_timestamp()
            .checked_sub(ENS_GRACE_PERIOD_SECS)
            .context("preloaded registrar binding end cannot be converted to expiry");
    }

    bail!("preloaded authority provenance is missing integer expiry");
}

pub(in crate::ens_v1_unwrapped_authority) fn registrar_labelhash_from_authority_key(
    authority_key: &str,
) -> Option<String> {
    let mut parts = authority_key.split(':');
    if parts.next()? != "registrar" {
        return None;
    }
    let _chain = parts.next()?;
    let _manifest_id = parts.next()?;
    let labelhash = parts.next()?;
    if !labelhash.starts_with("0x") {
        return None;
    }
    Some(labelhash.to_ascii_lowercase())
}
