use alloy_sol_types::sol_data::{Address as SolAddress, FixedBytes, String as SolString, Uint};
use anyhow::{Context, Result};

pub(super) use crate::ens_v2_common::{hex_string, keccak_signature_hex, normalize_address};
use crate::evm_abi::{
    abi_decode_params, address_hex, hex_string as prefixed_hex_string, normalize_hex_32,
    u256_word_hex,
};

use super::{NAME_REGISTERED_SIGNATURE, NAME_RENEWED_SIGNATURE, raw_logs::RegistrarRawLogRow};

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
) -> Result<Option<RegistrarObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAME_REGISTERED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameRegistered missing tokenId topic")?,
        )?;
        let (label, owner, subregistry, resolver, duration, payment_token, referrer, base, premium) =
            abi_decode_params::<(
                SolString,
                SolAddress,
                SolAddress,
                SolAddress,
                Uint<64>,
                SolAddress,
                FixedBytes<32>,
                Uint<256>,
                Uint<256>,
            )>(&raw_log.data, "NameRegistered data is malformed")?;
        return Ok(Some(RegistrarObservation::NameRegistered {
            token_id,
            label,
            owner: address_hex(owner),
            subregistry: address_hex(subregistry),
            resolver: address_hex(resolver),
            duration: i64::try_from(duration).context("NameRegistered duration exceeds i64")?,
            payment_token: address_hex(payment_token),
            referrer: prefixed_hex_string(referrer.as_slice()),
            base: u256_word_hex(base),
            premium: u256_word_hex(premium),
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAME_RENEWED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameRenewed missing tokenId topic")?,
        )?;
        let (label, duration, new_expiry, payment_token, referrer, base) =
            abi_decode_params::<(
                SolString,
                Uint<64>,
                Uint<64>,
                SolAddress,
                FixedBytes<32>,
                Uint<256>,
            )>(&raw_log.data, "NameRenewed data is malformed")?;
        return Ok(Some(RegistrarObservation::NameRenewed {
            token_id,
            label,
            duration: i64::try_from(duration).context("NameRenewed duration exceeds i64")?,
            new_expiry: i64::try_from(new_expiry).context("NameRenewed expiry exceeds i64")?,
            payment_token: address_hex(payment_token),
            referrer: prefixed_hex_string(referrer.as_slice()),
            base: u256_word_hex(base),
        }));
    }

    Ok(None)
}
