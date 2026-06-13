use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};

use crate::{
    execution::ExecutionCacheKey, name_current::NameCurrentRow,
    record_inventory::RecordInventoryCurrentRow,
};

use super::{
    boundaries::resolution_verified_support_boundary,
    record_keys::canonical_resolution_record_key,
    support_classes::{
        VerifiedResolutionRecord, VerifiedResolutionRequestedChainPosition, array_or_empty,
        json_field, json_string_field, resolution_projection_chain_position_from_value,
    },
};

pub fn build_resolution_execution_cache_key<R>(
    row: &NameCurrentRow,
    records: &[R],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    chain_positions: Value,
) -> Result<ExecutionCacheKey>
where
    R: VerifiedResolutionRecord,
{
    let manifest_versions = array_or_empty(json_field(&row.provenance, "manifest_versions"));
    if manifest_versions
        .as_array()
        .is_none_or(|items| items.is_empty())
    {
        bail!(
            "resolution execution explain requires non-empty manifest_versions provenance for {}",
            row.logical_name_id
        );
    }

    let support_boundary = resolution_verified_support_boundary(row, record_inventory_row)
        .with_context(|| {
            format!(
                "resolution execution explain requires a supported topology boundary for {}",
                row.logical_name_id
            )
        })?;

    Ok(ExecutionCacheKey {
        request_key: normalized_resolution_request_key(
            &row.namespace,
            &row.normalized_name,
            records,
        ),
        requested_chain_positions: build_resolution_requested_chain_positions(&chain_positions)?,
        manifest_versions,
        topology_version_boundary: support_boundary.topology_version_boundary,
        record_version_boundary: support_boundary.record_version_boundary,
    })
}

pub fn normalized_resolution_request_key<R>(
    namespace: &str,
    normalized_name: &str,
    records: &[R],
) -> String
where
    R: VerifiedResolutionRecord,
{
    let mut record_keys = records
        .iter()
        .map(|record| canonical_resolution_record_key(record.record_key()))
        .collect::<Vec<_>>();
    format_normalized_resolution_request_key(namespace, normalized_name, &mut record_keys)
}

pub fn normalized_resolution_request_key_from_record_keys(
    namespace: &str,
    normalized_name: &str,
    record_keys: &[String],
) -> String {
    let mut normalized_record_keys = record_keys
        .iter()
        .map(|record_key| canonical_resolution_record_key(record_key))
        .collect::<Vec<_>>();
    format_normalized_resolution_request_key(
        namespace,
        normalized_name,
        &mut normalized_record_keys,
    )
}

pub fn build_resolution_requested_chain_positions(chain_positions: &Value) -> Result<Value> {
    let positions = chain_positions
        .as_object()
        .context("resolution execution explain requires chain_positions")?
        .values()
        .filter_map(resolution_projection_chain_position_from_value)
        .map(|position| {
            let mut value = Map::new();
            value.insert("chain_id".to_owned(), Value::String(position.chain_id));
            value.insert(
                "block_number".to_owned(),
                Value::Number(position.block_number.into()),
            );
            value.insert("block_hash".to_owned(), Value::String(position.block_hash));
            Value::Object(value)
        })
        .collect::<Vec<_>>();

    if positions.is_empty() {
        bail!("resolution execution explain requires at least one chain position");
    }

    let mut positions = positions;
    positions.sort_by(|left, right| {
        json_string_field(json_field(left, "chain_id"))
            .cmp(&json_string_field(json_field(right, "chain_id")))
            .then(
                json_field(left, "block_number")
                    .and_then(Value::as_i64)
                    .cmp(&json_field(right, "block_number").and_then(Value::as_i64)),
            )
            .then(
                json_string_field(json_field(left, "block_hash"))
                    .cmp(&json_string_field(json_field(right, "block_hash"))),
            )
    });

    Ok(Value::Array(positions))
}

pub fn resolution_requested_chain_positions_from_projection(
    chain_positions: &Value,
) -> Result<Vec<VerifiedResolutionRequestedChainPosition>> {
    let chain_positions = chain_positions
        .as_object()
        .context("projected chain_positions must be a JSON object")?;
    let mut positions = chain_positions
        .values()
        .filter_map(resolution_projection_chain_position_from_value)
        .map(|position| VerifiedResolutionRequestedChainPosition {
            chain_id: position.chain_id,
            block_number: position.block_number,
            block_hash: position.block_hash,
        })
        .collect::<Vec<_>>();

    if positions.is_empty() {
        bail!("projected chain_positions must include at least one chain position");
    }

    positions.sort_by(|left, right| {
        left.chain_id
            .cmp(&right.chain_id)
            .then(left.block_number.cmp(&right.block_number))
            .then(left.block_hash.cmp(&right.block_hash))
    });
    Ok(positions)
}

fn format_normalized_resolution_request_key(
    namespace: &str,
    normalized_name: &str,
    record_keys: &mut [String],
) -> String {
    record_keys.sort_unstable();
    format!("{namespace}:{normalized_name}:{}", record_keys.join(","))
}

#[cfg(test)]
mod tests {
    use super::normalized_resolution_request_key_from_record_keys;

    #[test]
    fn resolution_request_key_canonicalizes_addr_coin_type_text() {
        let canonical = normalized_resolution_request_key_from_record_keys(
            "ens",
            "alice.eth",
            &["addr:60".to_owned()],
        );
        let padded = normalized_resolution_request_key_from_record_keys(
            "ens",
            "alice.eth",
            &["addr:060".to_owned()],
        );

        assert_eq!(padded, canonical);
        assert_eq!(padded, "ens:alice.eth:addr:60");
    }
}
