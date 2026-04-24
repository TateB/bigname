use anyhow::{Result, bail};

use super::types::NormalizedEvent;

pub(super) fn validate_normalized_event(event: &NormalizedEvent) -> Result<()> {
    if event.event_identity.is_empty() {
        bail!("normalized event has empty event_identity");
    }
    if event.namespace.is_empty() {
        bail!(
            "normalized event {} has empty namespace",
            event.event_identity
        );
    }
    if event.event_kind.is_empty() {
        bail!(
            "normalized event {} has empty event_kind",
            event.event_identity
        );
    }
    if event.source_family.is_empty() {
        bail!(
            "normalized event {} has empty source_family",
            event.event_identity
        );
    }
    if event.derivation_kind.is_empty() {
        bail!(
            "normalized event {} has empty derivation_kind",
            event.event_identity
        );
    }
    if event.manifest_version <= 0 {
        bail!(
            "normalized event {} has non-positive manifest_version {}",
            event.event_identity,
            event.manifest_version
        );
    }
    if event.block_number.is_some() != event.block_hash.is_some() {
        bail!(
            "normalized event {} must set block_number and block_hash together",
            event.event_identity
        );
    }
    if let Some(block_number) = event.block_number
        && block_number < 0
    {
        bail!(
            "normalized event {} has negative block_number {}",
            event.event_identity,
            block_number
        );
    }
    if let Some(log_index) = event.log_index {
        if log_index < 0 {
            bail!(
                "normalized event {} has negative log_index {}",
                event.event_identity,
                log_index
            );
        }
        if event.transaction_hash.is_none() {
            bail!(
                "normalized event {} has log_index without transaction_hash",
                event.event_identity
            );
        }
    }

    Ok(())
}
