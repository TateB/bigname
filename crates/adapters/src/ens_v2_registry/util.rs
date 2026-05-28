use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::{Uuid, time::OffsetDateTime};

pub(super) use crate::ens_v2_common::{hex_string, keccak256_bytes, normalize_address};
pub(super) use crate::evm_abi::namehash_bytes;

use super::{constants::ZERO_ADDRESS, types::ObservationRef};

pub(super) fn event_position_timestamp(reference: &ObservationRef) -> OffsetDateTime {
    let offset_micros = reference
        .transaction_index
        .saturating_mul(1_000)
        .saturating_add(reference.log_index.max(0));
    reference.block_timestamp + Duration::from_micros(offset_micros.max(0) as u64)
}

pub(super) fn null_if_zero_address(value: &str) -> Value {
    if normalize_address(value) == ZERO_ADDRESS {
        Value::Null
    } else {
        Value::String(normalize_address(value))
    }
}

pub(super) fn dns_encode(labels: &[Vec<u8>]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    for label in labels {
        let length = u8::try_from(label.len()).context("label exceeds DNS label length")?;
        if length == 0 {
            bail!("empty label is not encodable");
        }
        output.push(length);
        output.extend_from_slice(label);
    }
    output.push(0);
    Ok(output)
}

pub(super) fn deterministic_uuid(seed: &str) -> Uuid {
    let digest = keccak256_bytes(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}
