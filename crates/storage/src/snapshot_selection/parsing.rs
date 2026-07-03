use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

use super::chain_position::{ChainPosition, ChainPositions, SnapshotSelectionScope};
use super::error::{SnapshotSelectionError, SnapshotSelectionResult};
use crate::evm_primitives::normalize_evm_b256;

pub(super) fn parse_explicit_chain_positions_json(
    raw: &str,
    scope: &SnapshotSelectionScope,
) -> SnapshotSelectionResult<ChainPositions> {
    let value = serde_json::from_str::<Value>(raw).map_err(|error| {
        SnapshotSelectionError::invalid_input(format!(
            "chain_positions must be one JSON object: {error}"
        ))
    })?;
    reject_duplicate_top_level_slots(raw)?;
    let positions = decode_chain_positions_value(&value, "chain_positions")?;
    positions.validate_scope(scope)?;
    Ok(positions)
}

pub(super) fn decode_chain_positions_value(
    value: &Value,
    field_name: &str,
) -> SnapshotSelectionResult<ChainPositions> {
    let object = value.as_object().ok_or_else(|| {
        SnapshotSelectionError::invalid_input(format!("{field_name} must be a JSON object"))
    })?;
    let mut positions = BTreeMap::new();
    for (slot, position) in object {
        if slot.trim().is_empty() {
            return Err(SnapshotSelectionError::invalid_input(format!(
                "{field_name} contains an empty position slot"
            )));
        }
        positions.insert(
            slot.clone(),
            decode_chain_position_value(field_name, slot, position)?,
        );
    }
    Ok(ChainPositions::new(positions))
}

fn decode_chain_position_value(
    field_name: &str,
    slot: &str,
    value: &Value,
) -> SnapshotSelectionResult<ChainPosition> {
    let object = value.as_object().ok_or_else(|| {
        SnapshotSelectionError::invalid_input(format!("{field_name}.{slot} must be an object"))
    })?;
    let chain_id = required_string_field(object, "chain_id", field_name, slot)?;
    let block_hash = normalize_evm_b256(&required_string_field(
        object,
        "block_hash",
        field_name,
        slot,
    )?);
    let block_number = object
        .get("block_number")
        .and_then(Value::as_i64)
        .ok_or_else(|| {
            SnapshotSelectionError::invalid_input(format!(
                "{field_name}.{slot}.block_number must be an integer"
            ))
        })?;
    if block_number < 0 {
        return Err(SnapshotSelectionError::invalid_input(format!(
            "{field_name}.{slot}.block_number must not be negative"
        )));
    }
    let timestamp = parse_rfc3339_utc_timestamp(&required_string_field(
        object,
        "timestamp",
        field_name,
        slot,
    )?)?;

    Ok(ChainPosition {
        slot: slot.to_owned(),
        chain_id,
        block_number,
        block_hash,
        timestamp,
    })
}

fn required_string_field(
    object: &serde_json::Map<String, Value>,
    key: &str,
    field_name: &str,
    slot: &str,
) -> SnapshotSelectionResult<String> {
    let value = object.get(key).and_then(Value::as_str).ok_or_else(|| {
        SnapshotSelectionError::invalid_input(format!("{field_name}.{slot}.{key} must be a string"))
    })?;
    if value.trim().is_empty() {
        return Err(SnapshotSelectionError::invalid_input(format!(
            "{field_name}.{slot}.{key} must not be empty"
        )));
    }
    Ok(value.to_owned())
}

fn reject_duplicate_top_level_slots(raw: &str) -> SnapshotSelectionResult<()> {
    let slots = top_level_object_keys(raw)?;
    let mut seen = BTreeSet::new();
    for slot in slots {
        if !seen.insert(slot.clone()) {
            return Err(SnapshotSelectionError::invalid_input(format!(
                "chain_positions repeats position slot {slot}"
            )));
        }
    }
    Ok(())
}

fn top_level_object_keys(raw: &str) -> SnapshotSelectionResult<Vec<String>> {
    let mut index = skip_whitespace(raw, 0);
    if raw.as_bytes().get(index) != Some(&b'{') {
        return Err(SnapshotSelectionError::invalid_input(
            "chain_positions must be one JSON object",
        ));
    }
    index += 1;

    let mut keys = Vec::new();
    loop {
        index = skip_whitespace(raw, index);
        match raw.as_bytes().get(index) {
            Some(b'}') => {
                index += 1;
                break;
            }
            Some(b'"') => {}
            _ => {
                return Err(SnapshotSelectionError::invalid_input(
                    "chain_positions object keys must be strings",
                ));
            }
        }

        let (key, next_index) = read_json_string(raw, index)?;
        keys.push(key);
        index = skip_whitespace(raw, next_index);
        if raw.as_bytes().get(index) != Some(&b':') {
            return Err(SnapshotSelectionError::invalid_input(
                "chain_positions object key must be followed by ':'",
            ));
        }
        index = skip_json_value(raw, skip_whitespace(raw, index + 1))?;
        index = skip_whitespace(raw, index);
        match raw.as_bytes().get(index) {
            Some(b',') => index += 1,
            Some(b'}') => {
                index += 1;
                break;
            }
            _ => {
                return Err(SnapshotSelectionError::invalid_input(
                    "chain_positions object entries must be separated by ','",
                ));
            }
        }
    }

    if skip_whitespace(raw, index) != raw.len() {
        return Err(SnapshotSelectionError::invalid_input(
            "chain_positions must contain exactly one JSON object",
        ));
    }

    Ok(keys)
}

fn read_json_string(raw: &str, start: usize) -> SnapshotSelectionResult<(String, usize)> {
    let end = skip_json_string(raw, start)?;
    let decoded = serde_json::from_str::<String>(&raw[start..end]).map_err(|error| {
        SnapshotSelectionError::invalid_input(format!(
            "chain_positions object key is not a valid JSON string: {error}"
        ))
    })?;
    Ok((decoded, end))
}

fn skip_json_string(raw: &str, start: usize) -> SnapshotSelectionResult<usize> {
    if raw.as_bytes().get(start) != Some(&b'"') {
        return Err(SnapshotSelectionError::invalid_input(
            "expected JSON string",
        ));
    }
    let mut escaped = false;
    let mut index = start + 1;
    while let Some(byte) = raw.as_bytes().get(index) {
        if escaped {
            escaped = false;
            index += 1;
            continue;
        }
        match byte {
            b'\\' => {
                escaped = true;
                index += 1;
            }
            b'"' => return Ok(index + 1),
            _ => index += 1,
        }
    }
    Err(SnapshotSelectionError::invalid_input(
        "unterminated JSON string",
    ))
}

fn skip_json_value(raw: &str, start: usize) -> SnapshotSelectionResult<usize> {
    if start >= raw.len() {
        return Err(SnapshotSelectionError::invalid_input(
            "chain_positions object value is missing",
        ));
    }
    let mut depth = 0_i32;
    let mut index = start;
    while let Some(byte) = raw.as_bytes().get(index) {
        match byte {
            b'"' => index = skip_json_string(raw, index)?,
            b'{' | b'[' => {
                depth += 1;
                index += 1;
            }
            b'}' => {
                if depth == 0 {
                    return Ok(index);
                }
                depth -= 1;
                index += 1;
            }
            b']' => {
                if depth == 0 {
                    return Err(SnapshotSelectionError::invalid_input(
                        "unexpected ']' in chain_positions object",
                    ));
                }
                depth -= 1;
                index += 1;
            }
            b',' if depth == 0 => return Ok(index),
            _ => index += 1,
        }
    }
    Ok(index)
}

fn skip_whitespace(raw: &str, mut index: usize) -> usize {
    while matches!(
        raw.as_bytes().get(index),
        Some(b' ' | b'\n' | b'\r' | b'\t')
    ) {
        index += 1;
    }
    index
}

pub fn parse_rfc3339_utc_timestamp(value: &str) -> SnapshotSelectionResult<OffsetDateTime> {
    if value.len() != 20
        || !matches!(value.as_bytes().get(4), Some(b'-'))
        || !matches!(value.as_bytes().get(7), Some(b'-'))
        || !matches!(value.as_bytes().get(10), Some(b'T'))
        || !matches!(value.as_bytes().get(13), Some(b':'))
        || !matches!(value.as_bytes().get(16), Some(b':'))
        || !matches!(value.as_bytes().get(19), Some(b'Z'))
    {
        return Err(SnapshotSelectionError::invalid_input(format!(
            "timestamp {value} must use RFC 3339 UTC seconds format"
        )));
    }

    let year = parse_digits_i32(value, 0, 4, "year")?;
    let month = parse_digits_u8(value, 5, 7, "month")?;
    let day = parse_digits_u8(value, 8, 10, "day")?;
    let hour = parse_digits_u8(value, 11, 13, "hour")?;
    let minute = parse_digits_u8(value, 14, 16, "minute")?;
    let second = parse_digits_u8(value, 17, 19, "second")?;

    validate_date_parts(value, year, month, day)?;
    if hour > 23 || minute > 59 || second > 59 {
        Err(SnapshotSelectionError::invalid_input(format!(
            "timestamp {value} has invalid time"
        )))
    } else {
        let days = days_from_civil(year, month, day);
        let seconds = days
            .checked_mul(86_400)
            .and_then(|value| value.checked_add(i64::from(hour) * 3_600))
            .and_then(|value| value.checked_add(i64::from(minute) * 60))
            .and_then(|value| value.checked_add(i64::from(second)))
            .ok_or_else(|| {
                SnapshotSelectionError::invalid_input(format!(
                    "timestamp {value} is outside the supported range"
                ))
            })?;
        OffsetDateTime::from_unix_timestamp(seconds).map_err(|_| {
            SnapshotSelectionError::invalid_input(format!(
                "timestamp {value} is outside the supported range"
            ))
        })
    }
}

fn parse_digits_i32(
    value: &str,
    start: usize,
    end: usize,
    part: &str,
) -> SnapshotSelectionResult<i32> {
    value[start..end]
        .parse::<i32>()
        .map_err(|_| SnapshotSelectionError::invalid_input(format!("timestamp has invalid {part}")))
}

fn parse_digits_u8(
    value: &str,
    start: usize,
    end: usize,
    part: &str,
) -> SnapshotSelectionResult<u8> {
    value[start..end]
        .parse::<u8>()
        .map_err(|_| SnapshotSelectionError::invalid_input(format!("timestamp has invalid {part}")))
}

fn validate_date_parts(value: &str, year: i32, month: u8, day: u8) -> SnapshotSelectionResult<()> {
    if !(1..=12).contains(&month) {
        return Err(SnapshotSelectionError::invalid_input(format!(
            "timestamp {value} has invalid month"
        )));
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return Err(SnapshotSelectionError::invalid_input(format!(
            "timestamp {value} has invalid date"
        )));
    }
    Ok(())
}

fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u8, day: u8) -> i64 {
    let adjusted_year = i64::from(year) - i64::from(month <= 2);
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let year_of_era = adjusted_year - era * 400;
    let month = i64::from(month);
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}
