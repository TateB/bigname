use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    execution::ExecutionCacheKey, identity::SurfaceBindingKind, name_current::NameCurrentRow,
    record_inventory::RecordInventoryCurrentRow,
};

pub const ENS_NAMESPACE: &str = "ens";
pub const BASENAMES_NAMESPACE: &str = "basenames";
pub const BASE_MAINNET_CHAIN_ID: &str = "base-mainnet";
pub const ETHEREUM_MAINNET_CHAIN_ID: &str = "ethereum-mainnet";
pub const BASENAMES_L1_RESOLVER_ADDRESS: &str = "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31";

pub trait VerifiedResolutionRecord {
    fn record_key(&self) -> &str;
    fn record_family(&self) -> &str;
    fn selector_key(&self) -> Option<&str>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SupportedVerifiedResolutionRecordKey {
    Addr { coin_type: String },
    Avatar,
    Contenthash,
    Text,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerifiedResolutionPathClass {
    Direct,
    AliasOnly,
    WildcardDerived,
    BasenamesTransportDirect,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedResolutionSupportBoundary {
    pub path_class: VerifiedResolutionPathClass,
    pub topology_version_boundary: Value,
    pub record_version_boundary: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedResolutionRequestedChainPosition {
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolutionProjectionChainPosition {
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: String,
}

pub fn parse_supported_verified_resolution_record_key(
    record_key: &str,
) -> Result<SupportedVerifiedResolutionRecordKey> {
    if let Some(coin_type) = record_key.strip_prefix("addr:")
        && !coin_type.is_empty()
        && coin_type.as_bytes().iter().all(u8::is_ascii_digit)
    {
        return Ok(SupportedVerifiedResolutionRecordKey::Addr {
            coin_type: coin_type.to_owned(),
        });
    }

    if record_key == "contenthash" {
        return Ok(SupportedVerifiedResolutionRecordKey::Contenthash);
    }

    if record_key == "avatar" {
        return Ok(SupportedVerifiedResolutionRecordKey::Avatar);
    }

    if let Some(text_key) = record_key.strip_prefix("text:")
        && !text_key.is_empty()
    {
        return Ok(SupportedVerifiedResolutionRecordKey::Text);
    }

    bail!(
        "ENS direct-path verified resolution only supports addr:<coin_type>, avatar, contenthash, and text:<key> selectors, found {}",
        record_key
    );
}

pub fn supported_resolution_verified_lookup_records<R>(records: &[R]) -> Vec<R>
where
    R: VerifiedResolutionRecord + Clone,
{
    records
        .iter()
        .filter(|record| supports_resolution_verified_lookup_record(*record))
        .cloned()
        .collect()
}

pub fn supported_resolution_verified_readback_records<R>(
    row: &NameCurrentRow,
    records: &[R],
) -> Vec<R>
where
    R: VerifiedResolutionRecord + Clone,
{
    records
        .iter()
        .filter(|record| {
            supports_resolution_verified_lookup_record(*record)
                || (resolution_supports_avatar_readback(row, None)
                    && is_resolution_avatar_record(*record))
        })
        .cloned()
        .collect()
}

pub fn supports_resolution_verified_lookup_record(record: &impl VerifiedResolutionRecord) -> bool {
    match record.record_family() {
        "addr" => record
            .selector_key()
            .is_some_and(|selector| selector.as_bytes().iter().all(u8::is_ascii_digit)),
        "contenthash" => record.record_key() == "contenthash" && record.selector_key().is_none(),
        "text" => record.selector_key().is_some(),
        _ => false,
    }
}

pub fn is_resolution_avatar_record(record: &impl VerifiedResolutionRecord) -> bool {
    record.record_key() == "avatar"
        && record.record_family() == "avatar"
        && record.selector_key().is_none()
}

pub fn resolution_execution_cache_lookup_records<R>(row: &NameCurrentRow, records: &[R]) -> Vec<R>
where
    R: VerifiedResolutionRecord + Clone,
{
    if !resolution_supports_avatar_readback(row, None) {
        return records.to_vec();
    }

    let lookup_records = records
        .iter()
        .filter(|record| !is_resolution_avatar_record(*record))
        .cloned()
        .collect::<Vec<_>>();

    if lookup_records.is_empty() || lookup_records.len() == records.len() {
        records.to_vec()
    } else {
        lookup_records
    }
}

pub fn resolution_supports_avatar_readback(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> bool {
    resolution_verified_support_boundary(row, record_inventory_row).is_some()
}

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
        .map(|record| record.record_key().to_owned())
        .collect::<Vec<_>>();
    format_normalized_resolution_request_key(namespace, normalized_name, &mut record_keys)
}

pub fn normalized_resolution_request_key_from_record_keys(
    namespace: &str,
    normalized_name: &str,
    record_keys: &[String],
) -> String {
    let mut normalized_record_keys = record_keys.to_vec();
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

pub fn resolution_record_inventory_lookup_key(row: &NameCurrentRow) -> Option<(Uuid, Value)> {
    Some((
        row.resource_id?,
        build_supported_resolution_declared_boundary(row)?,
    ))
}

pub fn resolution_record_inventory_lookup_key_for_revalidation(
    row: &NameCurrentRow,
) -> Result<Option<(Uuid, Value)>> {
    if let Some(lookup) = projected_record_inventory_lookup_key_for_revalidation(row)? {
        return Ok(Some(lookup));
    }

    let Some(record_version_boundary) =
        build_supported_resolution_declared_boundary_for_revalidation(row)
    else {
        return Ok(None);
    };
    let resource_id = row
        .resource_id
        .with_context(|| "supported resolution revalidation requires resource_id".to_owned())?;
    Ok(Some((resource_id, record_version_boundary)))
}

pub fn resolution_record_version_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<Value> {
    record_inventory_row
        .map(|record_inventory_row| record_inventory_row.record_version_boundary.clone())
        .or_else(|| build_supported_resolution_declared_boundary(row))
}

pub fn resolution_record_version_boundary_for_revalidation(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<Value> {
    record_inventory_row
        .map(|row| row.record_version_boundary.clone())
        .or_else(|| build_supported_resolution_declared_boundary_for_revalidation(row))
}

pub fn record_version_boundary_has_pointer(record_version_boundary: &Value) -> bool {
    json_field(record_version_boundary, "normalized_event_id").is_some_and(|value| !value.is_null())
        && json_field(record_version_boundary, "event_kind").is_some_and(|value| !value.is_null())
}

pub fn projected_resolution_topology(summary: &Value) -> Option<Value> {
    json_field(summary, "topology")
        .filter(|value| value.is_object())
        .cloned()
}

pub fn projected_resolution_boundaries_from_topology(topology: &Value) -> Result<(Value, Value)> {
    let version_boundaries = json_field(topology, "version_boundaries")
        .with_context(|| "projected topology must include version_boundaries".to_owned())?;
    Ok((
        json_field(version_boundaries, "topology_version_boundary")
            .cloned()
            .with_context(|| {
                "projected topology must include version_boundaries.topology_version_boundary"
                    .to_owned()
            })?,
        json_field(version_boundaries, "record_version_boundary")
            .cloned()
            .with_context(|| {
                "projected topology must include version_boundaries.record_version_boundary"
                    .to_owned()
            })?,
    ))
}

pub fn resolution_verified_support_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<VerifiedResolutionSupportBoundary> {
    if !matches!(row.namespace.as_str(), ENS_NAMESPACE | BASENAMES_NAMESPACE) {
        return None;
    }

    if let Some(projected_topology) = projected_resolution_topology(&row.declared_summary) {
        let version_boundaries = json_field(&projected_topology, "version_boundaries")?;
        let topology_version_boundary =
            json_field(version_boundaries, "topology_version_boundary")?.clone();
        let record_version_boundary =
            json_field(version_boundaries, "record_version_boundary")?.clone();
        match row.namespace.as_str() {
            ENS_NAMESPACE
                if !boundary_chain_id_matches(
                    &topology_version_boundary,
                    ETHEREUM_MAINNET_CHAIN_ID,
                ) || !boundary_chain_id_matches(
                    &record_version_boundary,
                    ETHEREUM_MAINNET_CHAIN_ID,
                ) =>
            {
                return None;
            }
            BASENAMES_NAMESPACE if !row_has_basenames_supported_chain_positions(row) => {
                return None;
            }
            ENS_NAMESPACE | BASENAMES_NAMESPACE => {}
            _ => return None,
        }
        let path_class = classify_supported_resolution_topology(
            &row.namespace,
            &row.logical_name_id,
            &projected_topology,
        )?;
        return Some(VerifiedResolutionSupportBoundary {
            path_class,
            topology_version_boundary,
            record_version_boundary,
        });
    }

    let topology_version_boundary = match row.namespace.as_str() {
        ENS_NAMESPACE => build_supported_resolution_verified_boundary(row)?,
        BASENAMES_NAMESPACE => return None,
        _ => return None,
    };
    let record_version_boundary = resolution_record_version_boundary(row, record_inventory_row)
        .or_else(|| Some(topology_version_boundary.clone()))?;
    let path_class = match row.binding_kind {
        Some(SurfaceBindingKind::ResolverAliasPath) => VerifiedResolutionPathClass::AliasOnly,
        _ => VerifiedResolutionPathClass::Direct,
    };

    Some(VerifiedResolutionSupportBoundary {
        path_class,
        topology_version_boundary,
        record_version_boundary,
    })
}

pub fn try_resolution_verified_support_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Result<Option<VerifiedResolutionSupportBoundary>> {
    if !matches!(row.namespace.as_str(), ENS_NAMESPACE | BASENAMES_NAMESPACE) {
        return Ok(None);
    }

    if let Some(projected_topology) = projected_resolution_topology(&row.declared_summary) {
        let version_boundaries = json_field(&projected_topology, "version_boundaries")
            .with_context(|| "projected topology must include version_boundaries".to_owned())?;
        let topology_version_boundary = json_field(version_boundaries, "topology_version_boundary")
            .cloned()
            .with_context(|| {
                "projected topology must include version_boundaries.topology_version_boundary"
                    .to_owned()
            })?;
        let record_version_boundary = json_field(version_boundaries, "record_version_boundary")
            .cloned()
            .with_context(|| {
                "projected topology must include version_boundaries.record_version_boundary"
                    .to_owned()
            })?;
        match row.namespace.as_str() {
            ENS_NAMESPACE
                if !boundary_chain_id_matches(
                    &topology_version_boundary,
                    ETHEREUM_MAINNET_CHAIN_ID,
                ) || !boundary_chain_id_matches(
                    &record_version_boundary,
                    ETHEREUM_MAINNET_CHAIN_ID,
                ) =>
            {
                return Ok(None);
            }
            BASENAMES_NAMESPACE
                if !row_has_basenames_supported_chain_positions_for_revalidation(row) =>
            {
                return Ok(None);
            }
            ENS_NAMESPACE | BASENAMES_NAMESPACE => {}
            _ => return Ok(None),
        }
        let path_class = try_classify_supported_resolution_topology(
            &row.namespace,
            &row.logical_name_id,
            &projected_topology,
        )?;
        return Ok(Some(VerifiedResolutionSupportBoundary {
            path_class,
            topology_version_boundary,
            record_version_boundary,
        }));
    }

    let Some(topology_version_boundary) = (match row.namespace.as_str() {
        ENS_NAMESPACE => build_supported_resolution_declared_boundary_for_revalidation(row),
        BASENAMES_NAMESPACE => None,
        _ => None,
    }) else {
        return Ok(None);
    };
    let record_version_boundary =
        resolution_record_version_boundary_for_revalidation(row, record_inventory_row)
            .unwrap_or_else(|| topology_version_boundary.clone());
    let path_class = match row.binding_kind {
        Some(SurfaceBindingKind::ResolverAliasPath) => VerifiedResolutionPathClass::AliasOnly,
        _ => VerifiedResolutionPathClass::Direct,
    };

    Ok(Some(VerifiedResolutionSupportBoundary {
        path_class,
        topology_version_boundary,
        record_version_boundary,
    }))
}

pub fn classify_supported_resolution_topology(
    namespace: &str,
    logical_name_id: &str,
    topology: &Value,
) -> Option<VerifiedResolutionPathClass> {
    if summary_is_unsupported(Some(topology)) {
        return None;
    }

    let resolver_logical_name_id = resolution_topology_resolver_logical_name_id(topology)?;
    let alias_present = resolution_topology_alias_is_present(topology).ok()?;
    let wildcard_source_logical_name_id = resolution_topology_wildcard_state(topology).ok()?;
    let transport_is_null = resolution_topology_transport_is_null(topology);

    if namespace == BASENAMES_NAMESPACE {
        if !transport_is_null {
            return resolution_topology_subregistry_path_is_empty(topology)
                .then_some(())
                .filter(|_| resolver_logical_name_id == logical_name_id)
                .filter(|_| !alias_present)
                .filter(|_| wildcard_source_logical_name_id.is_none())
                .filter(|_| {
                    resolution_topology_transport_matches_basenames_supported_class(topology)
                })
                .map(|_| VerifiedResolutionPathClass::BasenamesTransportDirect);
        }
        return None;
    }

    if !transport_is_null {
        return None;
    }

    if wildcard_source_logical_name_id.is_some() {
        if alias_present || !resolution_topology_subregistry_path_is_empty(topology) {
            return None;
        }
        return (resolver_logical_name_id == wildcard_source_logical_name_id?)
            .then_some(VerifiedResolutionPathClass::WildcardDerived);
    }

    if resolver_logical_name_id != logical_name_id {
        return None;
    }

    if alias_present {
        Some(VerifiedResolutionPathClass::AliasOnly)
    } else {
        Some(VerifiedResolutionPathClass::Direct)
    }
}

pub fn try_classify_supported_resolution_topology(
    namespace: &str,
    logical_name_id: &str,
    topology: &Value,
) -> Result<VerifiedResolutionPathClass> {
    if summary_is_unsupported(Some(topology)) {
        bail!("projected topology is unsupported");
    }

    let resolver_logical_name_id = resolution_topology_resolver_logical_name_id(topology)
        .with_context(|| {
            "projected topology must include resolver_path[0].logical_name_id".to_owned()
        })?;
    let alias_present = resolution_topology_alias_is_present(topology)?;
    let wildcard_source_logical_name_id = resolution_topology_wildcard_state(topology)?;
    let transport_is_null = resolution_topology_transport_is_null(topology);

    if namespace == BASENAMES_NAMESPACE {
        if transport_is_null {
            bail!("projected Basenames topology must include supported transport detail");
        }
        if !resolution_topology_subregistry_path_is_empty(topology) {
            bail!("projected Basenames topology must keep subregistry_path empty");
        }
        if resolver_logical_name_id != logical_name_id {
            bail!("projected Basenames topology must anchor resolver_path[0] to the request name");
        }
        if alias_present {
            bail!("projected Basenames topology must keep alias detail empty");
        }
        if wildcard_source_logical_name_id.is_some() {
            bail!("projected Basenames topology must keep wildcard detail empty");
        }
        if !resolution_topology_transport_matches_basenames_supported_class(topology) {
            bail!("projected Basenames topology transport is outside the supported class");
        }
        return Ok(VerifiedResolutionPathClass::BasenamesTransportDirect);
    }

    if !transport_is_null {
        bail!("projected ENS topology must keep transport detail null");
    }

    if let Some(wildcard_source_logical_name_id) = wildcard_source_logical_name_id {
        if alias_present || !resolution_topology_subregistry_path_is_empty(topology) {
            bail!(
                "projected wildcard-derived ENS topology must keep alias detail empty and subregistry_path empty"
            );
        }
        if resolver_logical_name_id != wildcard_source_logical_name_id {
            bail!(
                "projected wildcard-derived ENS topology must anchor resolver_path[0] to wildcard.source.logical_name_id"
            );
        }
        return Ok(VerifiedResolutionPathClass::WildcardDerived);
    }

    if resolver_logical_name_id != logical_name_id {
        bail!("projected ENS topology must anchor resolver_path[0] to the request name");
    }

    if alias_present {
        Ok(VerifiedResolutionPathClass::AliasOnly)
    } else {
        Ok(VerifiedResolutionPathClass::Direct)
    }
}

pub fn row_has_basenames_supported_chain_positions(row: &NameCurrentRow) -> bool {
    let Some(chain_positions) = row.chain_positions.as_object() else {
        return false;
    };

    let mut saw_base = false;
    let mut saw_ethereum = false;
    for position in chain_positions.values() {
        match resolution_projection_chain_position_from_value(position)
            .map(|position| position.chain_id)
        {
            Some(chain_id) if chain_id == BASE_MAINNET_CHAIN_ID => saw_base = true,
            Some(chain_id) if chain_id == ETHEREUM_MAINNET_CHAIN_ID => saw_ethereum = true,
            Some(_) | None => {}
        }
    }

    saw_base && saw_ethereum
}

fn format_normalized_resolution_request_key(
    namespace: &str,
    normalized_name: &str,
    record_keys: &mut [String],
) -> String {
    record_keys.sort_unstable();
    format!("{namespace}:{normalized_name}:{}", record_keys.join(","))
}

fn build_supported_resolution_verified_boundary(row: &NameCurrentRow) -> Option<Value> {
    if row.namespace != ENS_NAMESPACE
        || !matches!(
            row.binding_kind,
            Some(SurfaceBindingKind::DeclaredRegistryPath | SurfaceBindingKind::ResolverAliasPath)
        )
        || row.resource_id.is_none()
    {
        return None;
    }

    let chain_position = build_resolution_boundary_chain_position(row)?;
    if chain_position.chain_id != ETHEREUM_MAINNET_CHAIN_ID {
        return None;
    }

    Some(build_resolution_version_boundary(row, &chain_position))
}

fn build_supported_resolution_declared_boundary(row: &NameCurrentRow) -> Option<Value> {
    let binding_supported = match row.namespace.as_str() {
        ENS_NAMESPACE => matches!(
            row.binding_kind,
            Some(SurfaceBindingKind::DeclaredRegistryPath | SurfaceBindingKind::ResolverAliasPath)
        ),
        BASENAMES_NAMESPACE => row.binding_kind == Some(SurfaceBindingKind::DeclaredRegistryPath),
        _ => false,
    };
    if !binding_supported || row.resource_id.is_none() {
        return None;
    }

    let chain_position = build_resolution_boundary_chain_position(row)?;
    match row.namespace.as_str() {
        ENS_NAMESPACE if chain_position.chain_id == ETHEREUM_MAINNET_CHAIN_ID => {}
        BASENAMES_NAMESPACE if chain_position.chain_id == BASE_MAINNET_CHAIN_ID => {}
        _ => return None,
    }

    Some(build_resolution_version_boundary(row, &chain_position))
}

fn build_supported_resolution_declared_boundary_for_revalidation(
    row: &NameCurrentRow,
) -> Option<Value> {
    let binding_supported = match row.namespace.as_str() {
        ENS_NAMESPACE => matches!(
            row.binding_kind,
            Some(SurfaceBindingKind::DeclaredRegistryPath | SurfaceBindingKind::ResolverAliasPath)
        ),
        BASENAMES_NAMESPACE => row.binding_kind == Some(SurfaceBindingKind::DeclaredRegistryPath),
        _ => false,
    };
    if !binding_supported || row.resource_id.is_none() {
        return None;
    }

    let chain_position = build_resolution_boundary_chain_position(row)?;
    match row.namespace.as_str() {
        ENS_NAMESPACE if chain_position.chain_id == ETHEREUM_MAINNET_CHAIN_ID => {}
        BASENAMES_NAMESPACE if chain_position.chain_id == BASE_MAINNET_CHAIN_ID => {}
        _ => return None,
    }

    Some(build_resolution_version_boundary(row, &chain_position))
}

fn build_resolution_boundary_chain_position(
    row: &NameCurrentRow,
) -> Option<ResolutionProjectionChainPosition> {
    let chain_positions = row.chain_positions.as_object()?;
    if row.namespace == BASENAMES_NAMESPACE
        && let Some(position) = chain_positions
            .values()
            .filter_map(resolution_projection_chain_position_from_value)
            .find(|position| position.chain_id == BASE_MAINNET_CHAIN_ID)
    {
        return Some(position);
    }

    chain_positions
        .get("ethereum")
        .and_then(resolution_projection_chain_position_from_value)
        .or_else(|| {
            let mut parsed = chain_positions
                .values()
                .filter_map(resolution_projection_chain_position_from_value);
            let first = parsed.next()?;
            parsed.next().is_none().then_some(first)
        })
}

fn build_resolution_version_boundary(
    row: &NameCurrentRow,
    chain_position: &ResolutionProjectionChainPosition,
) -> Value {
    let mut boundary = Map::new();
    boundary.insert(
        "logical_name_id".to_owned(),
        Value::String(row.logical_name_id.clone()),
    );
    boundary.insert(
        "resource_id".to_owned(),
        row.resource_id
            .map(|value| Value::String(value.to_string()))
            .unwrap_or(Value::Null),
    );
    boundary.insert("normalized_event_id".to_owned(), Value::Null);
    boundary.insert("event_kind".to_owned(), Value::Null);
    boundary.insert(
        "chain_position".to_owned(),
        Value::Object(chain_position_value(chain_position)),
    );
    Value::Object(boundary)
}

fn boundary_chain_id_matches(boundary: &Value, expected_chain_id: &str) -> bool {
    json_field(boundary, "chain_position")
        .and_then(|chain_position| json_string_field(json_field(chain_position, "chain_id")))
        .is_some_and(|chain_id| chain_id == expected_chain_id)
}

fn chain_position_value(position: &ResolutionProjectionChainPosition) -> Map<String, Value> {
    let mut value = Map::new();
    value.insert(
        "chain_id".to_owned(),
        Value::String(position.chain_id.clone()),
    );
    value.insert(
        "block_number".to_owned(),
        Value::Number(position.block_number.into()),
    );
    value.insert(
        "block_hash".to_owned(),
        Value::String(position.block_hash.clone()),
    );
    value.insert(
        "timestamp".to_owned(),
        Value::String(position.timestamp.clone()),
    );
    value
}

fn projected_record_inventory_lookup_key_for_revalidation(
    row: &NameCurrentRow,
) -> Result<Option<(Uuid, Value)>> {
    let Some(projected_topology) = projected_resolution_topology(&row.declared_summary) else {
        return Ok(None);
    };

    let version_boundaries =
        json_field(&projected_topology, "version_boundaries").with_context(|| {
            format!(
                "projected topology for logical_name_id {} must include version_boundaries",
                row.logical_name_id
            )
        })?;
    let record_version_boundary = json_field(version_boundaries, "record_version_boundary")
        .cloned()
        .with_context(|| {
            format!(
                "projected topology for logical_name_id {} must include version_boundaries.record_version_boundary",
                row.logical_name_id
            )
        })?;
    let resource_id = json_field(&record_version_boundary, "resource_id")
        .and_then(Value::as_str)
        .with_context(|| {
            format!(
                "projected topology record_version_boundary for logical_name_id {} must include resource_id",
                row.logical_name_id
            )
        })?;
    let resource_id = Uuid::parse_str(resource_id).with_context(|| {
        format!(
            "projected topology record_version_boundary for logical_name_id {} must include a valid UUID resource_id",
            row.logical_name_id
        )
    })?;

    Ok(Some((resource_id, record_version_boundary)))
}

fn row_has_basenames_supported_chain_positions_for_revalidation(row: &NameCurrentRow) -> bool {
    row_has_basenames_supported_chain_positions(row)
}

fn resolution_topology_resolver_logical_name_id(topology: &Value) -> Option<String> {
    json_field(topology, "resolver_path")
        .and_then(Value::as_array)
        .and_then(|resolver_path| resolver_path.first())
        .and_then(|hop| json_string_field(json_field(hop, "logical_name_id")))
}

fn resolution_topology_alias_is_present(topology: &Value) -> Result<bool> {
    let alias = json_field(topology, "alias")
        .with_context(|| "projected topology must include alias".to_owned())?;
    let final_target_present =
        !matches!(json_field(alias, "final_target"), None | Some(Value::Null));
    let hops = json_field(alias, "hops")
        .and_then(Value::as_array)
        .with_context(|| "projected topology alias must include hops".to_owned())?;
    let hops_present = !hops.is_empty();
    if final_target_present != hops_present {
        bail!("projected topology alias must set final_target and non-empty hops together");
    }
    Ok(final_target_present)
}

fn resolution_topology_wildcard_state(topology: &Value) -> Result<Option<String>> {
    let wildcard = json_field(topology, "wildcard")
        .with_context(|| "projected topology must include wildcard".to_owned())?;
    let matched_labels = json_field(wildcard, "matched_labels")
        .and_then(Value::as_array)
        .with_context(|| "projected topology wildcard must include matched_labels".to_owned())?;
    let source = json_field(wildcard, "source");

    match source {
        None | Some(Value::Null) => {
            if matched_labels.is_empty() {
                Ok(None)
            } else {
                bail!("projected topology wildcard with null source must keep matched_labels empty")
            }
        }
        Some(_) if matched_labels.is_empty() => {
            bail!(
                "projected topology wildcard must keep matched_labels non-empty when source is present"
            )
        }
        Some(source) => Ok(Some(
            json_string_field(json_field(source, "logical_name_id")).with_context(|| {
                "projected topology wildcard source must include logical_name_id".to_owned()
            })?,
        )),
    }
}

fn resolution_topology_subregistry_path_is_empty(topology: &Value) -> bool {
    json_field(topology, "subregistry_path")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
}

fn resolution_topology_transport_is_null(topology: &Value) -> bool {
    let Some(transport) = json_field(topology, "transport") else {
        return true;
    };

    for field_name in [
        "source_chain_id",
        "target_chain_id",
        "contract_address",
        "latest_event_kind",
    ] {
        if !matches!(json_field(transport, field_name), None | Some(Value::Null)) {
            return false;
        }
    }

    true
}

fn resolution_topology_transport_matches_basenames_supported_class(topology: &Value) -> bool {
    let Some(transport) = json_field(topology, "transport").and_then(Value::as_object) else {
        return false;
    };
    if transport.iter().any(|(field_name, value)| {
        !matches!(
            field_name.as_str(),
            "source_chain_id" | "target_chain_id" | "contract_address" | "latest_event_kind"
        ) && !value.is_null()
    }) {
        return false;
    }
    json_string_field(transport.get("source_chain_id"))
        .is_some_and(|value| value == BASE_MAINNET_CHAIN_ID)
        && json_string_field(transport.get("target_chain_id"))
            .is_some_and(|value| value == ETHEREUM_MAINNET_CHAIN_ID)
        && json_string_field(transport.get("contract_address"))
            .is_some_and(|value| value.eq_ignore_ascii_case(BASENAMES_L1_RESOLVER_ADDRESS))
}

fn resolution_projection_chain_position_from_value(
    value: &Value,
) -> Option<ResolutionProjectionChainPosition> {
    Some(ResolutionProjectionChainPosition {
        chain_id: json_string_field(json_field(value, "chain_id"))?,
        block_number: json_field(value, "block_number")?.as_i64()?,
        block_hash: json_string_field(json_field(value, "block_hash"))?,
        timestamp: json_string_field(json_field(value, "timestamp"))?,
    })
}

fn array_or_empty(value: Option<&Value>) -> Value {
    value
        .and_then(Value::as_array)
        .map(|items| Value::Array(items.clone()))
        .unwrap_or_else(|| Value::Array(Vec::new()))
}

fn summary_is_unsupported(section: Option<&Value>) -> bool {
    matches!(
        json_string_field(section.and_then(|value| json_field(value, "status"))).as_deref(),
        Some("unsupported")
    ) && json_string_field(section.and_then(|value| json_field(value, "unsupported_reason")))
        .is_some()
}

fn json_field<'a>(value: &'a Value, field_name: &str) -> Option<&'a Value> {
    value.as_object()?.get(field_name)
}

fn json_string_field(value: Option<&Value>) -> Option<String> {
    value?.as_str().map(str::to_owned)
}
