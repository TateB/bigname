use alloy_sol_types::sol;
use anyhow::{Context, Result};

use crate::adapter_manifest::ActiveManifestEventTopic0sBySignature;
pub(super) use crate::ens_v2_common::{hex_string, normalize_address};
use crate::evm_abi::{
    address_hex, decode_event_log, hex_string as prefixed_hex_string, u256_word_hex,
};

use super::{
    ABI_EVENT_NAME_REGISTERED_SIGNATURE, ABI_EVENT_NAME_RENEWED_SIGNATURE,
    raw_logs::RegistrarRawLogRow,
};

sol! {
    #[derive(Debug)]
    event NameRegistered(
        uint256 indexed tokenId,
        string label,
        address owner,
        address subregistry,
        address resolver,
        uint64 duration,
        address paymentToken,
        bytes32 referrer,
        uint256 base,
        uint256 premium
    );

    #[derive(Debug)]
    event NameRenewed(
        uint256 indexed tokenId,
        string label,
        uint64 duration,
        uint64 newExpiry,
        address paymentToken,
        bytes32 referrer,
        uint256 base
    );
}

pub(super) enum RegistrarObservation {
    NameRegistered {
        token_id: String,
        label: String,
        owner: String,
        subregistry: String,
        resolver: String,
        duration: i64,
        payment_token: String,
        referrer: String,
        base: String,
        premium: String,
    },
    NameRenewed {
        token_id: String,
        label: String,
        duration: i64,
        new_expiry: i64,
        payment_token: String,
        referrer: String,
        base: String,
    },
}

pub(super) fn build_registrar_observation(
    raw_log: &RegistrarRawLogRow,
    event_topics: &ActiveManifestEventTopic0sBySignature,
) -> Result<Option<RegistrarObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if event_topics.matches(ABI_EVENT_NAME_REGISTERED_SIGNATURE, topic0)? {
        let event = decode_event_log::<NameRegistered>(
            &raw_log.topics,
            &raw_log.data,
            "NameRegistered log is malformed",
        )?;
        return Ok(Some(RegistrarObservation::NameRegistered {
            token_id: u256_word_hex(event.tokenId),
            label: event.label,
            owner: address_hex(event.owner),
            subregistry: address_hex(event.subregistry),
            resolver: address_hex(event.resolver),
            duration: i64::try_from(event.duration)
                .context("NameRegistered duration exceeds i64")?,
            payment_token: address_hex(event.paymentToken),
            referrer: prefixed_hex_string(event.referrer.as_slice()),
            base: u256_word_hex(event.base),
            premium: u256_word_hex(event.premium),
        }));
    }

    if event_topics.matches(ABI_EVENT_NAME_RENEWED_SIGNATURE, topic0)? {
        let event = decode_event_log::<NameRenewed>(
            &raw_log.topics,
            &raw_log.data,
            "NameRenewed log is malformed",
        )?;
        return Ok(Some(RegistrarObservation::NameRenewed {
            token_id: u256_word_hex(event.tokenId),
            label: event.label,
            duration: i64::try_from(event.duration).context("NameRenewed duration exceeds i64")?,
            new_expiry: i64::try_from(event.newExpiry).context("NameRenewed expiry exceeds i64")?,
            payment_token: address_hex(event.paymentToken),
            referrer: prefixed_hex_string(event.referrer.as_slice()),
            base: u256_word_hex(event.base),
        }));
    }

    Ok(None)
}
