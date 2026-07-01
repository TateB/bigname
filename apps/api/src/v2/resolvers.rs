use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentListFilter, NameCurrentListOrder,
    NameCurrentListRow, NameCurrentListSort, ResolverCurrentRow, SnapshotPositionRequirement,
    SnapshotSelectionScope,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::AppState;

use super::{
    CursorPayload, Envelope, Meta, NameRecord, Page, QueryParams, V2Error, V2Result,
    api_error_to_v2, as_of_meta, build_name_record, decode, encode, encode_at_token,
    format_timestamp, name_record, numeric_to_slug, resolve_v2_snapshot,
    vocab::{Completeness, Status},
};

const BOUND_NAMES_SORT: NameCurrentListSort = NameCurrentListSort::Name;
const BOUND_NAMES_ORDER: NameCurrentListOrder = NameCurrentListOrder::Asc;
const BOUND_NAMES_SORT_TOKEN: &str = "name_asc";
const CHAIN_ID_FILTER_KEY: &str = "chain_id";
const RESOLVER_FILTER_KEY: &str = "resolver";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const SORT_VALUE_CURSOR_KEY: &str = "sort_value";
const CURSOR_NAMESPACE_KEY: &str = "namespace";
const NORMALIZED_NAME_CURSOR_KEY: &str = "normalized_name";
const NAMEHASH_CURSOR_KEY: &str = "namehash";
const NONE_FILTER_VALUE: &str = "";
const RESOLVER_SECTIONS: [(&str, &str, &str); 4] = [
    ("nodes", "nodes", "bindings"),
    ("aliases", "aliases", "aliases"),
    ("roles", "role_holders", "role_holders"),
    ("events", "events", "event_summary"),
];

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct ResolverOverview {
    pub(crate) chain_id: u64,
    pub(crate) address: String,
    pub(crate) counts: BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) nodes: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) aliases: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) roles: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) events: Option<Value>,
    pub(crate) bound_names: BoundNames,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct BoundNames {
    pub(crate) data: Vec<NameRecord>,
    pub(crate) page: Page,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResolverOverviewInclude {
    nodes: bool,
    aliases: bool,
    roles: bool,
    events: bool,
}

impl ResolverOverviewInclude {
    fn all() -> Self {
        Self {
            nodes: true,
            aliases: true,
            roles: true,
            events: true,
        }
    }

    fn empty() -> Self {
        Self {
            nodes: false,
            aliases: false,
            roles: false,
            events: false,
        }
    }

    fn requests(self, section: &str) -> bool {
        match section {
            "nodes" => self.nodes,
            "aliases" => self.aliases,
            "roles" => self.roles,
            "events" => self.events,
            _ => false,
        }
    }
}

pub(crate) async fn get_resolver(
    Path((chain_id, address)): Path<(String, String)>,
    params: QueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<ResolverOverview>>> {
    let (numeric_chain_id, chain_id_slug) = parse_numeric_chain_id(&chain_id)?;
    let normalized_address =
        crate::parse_evm_address(&address, "address").map_err(api_error_to_v2)?;
    let include = resolver_overview_include(&params.include)?;

    let row = bigname_storage::load_resolver_current(
        &state.pool,
        chain_id_slug,
        &normalized_address,
    )
    .await
    .map_err(|_| {
        V2Error::internal_error(format!(
            "failed to load resolver data for chain_id {chain_id_slug} address {normalized_address}"
        ))
    })?;
    let Some(row) = row else {
        return Err(V2Error::not_found(format!(
            "resolver {normalized_address} was not found on chain {numeric_chain_id}"
        )));
    };

    let scope = resolver_snapshot_scope(chain_id_slug)?;
    let selected_snapshot =
        resolve_v2_snapshot(&state.pool, &scope, params.at.as_ref(), params.finality).await?;
    let snapshot_token = encode_at_token(&selected_snapshot);
    let cursor_binding = BoundNamesCursorBinding {
        chain_id: numeric_chain_id,
        resolver_address: &normalized_address,
        namespace: params.namespace.as_deref(),
        sort: BOUND_NAMES_SORT_TOKEN,
        snapshot_token: &snapshot_token,
    };
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            bound_names_storage_cursor(&payload, &cursor_binding)
        })
        .transpose()?;

    let filter = NameCurrentListFilter {
        namespace: params.namespace.clone(),
        resolver: Some(normalized_address.clone()),
        ..NameCurrentListFilter::default()
    };
    let (bound_name_rows, storage_next_cursor) = load_bound_name_rows(
        &state.pool,
        &filter,
        storage_cursor.as_ref(),
        params.page_size,
        numeric_chain_id,
        &normalized_address,
    )
    .await?;

    let next_cursor = storage_next_cursor
        .as_ref()
        .map(|cursor| encode(&bound_names_cursor_payload(cursor, &cursor_binding)));
    let has_more = next_cursor.is_some();
    let bound_names = BoundNames {
        data: bound_name_rows
            .iter()
            .map(|row| build_bound_name_record(row, numeric_chain_id))
            .collect(),
        page: Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count: None,
            has_more,
        },
    };
    let mut meta = Meta {
        as_of: Some(as_of_meta(&selected_snapshot)?),
        ..Meta::default()
    };
    apply_resolver_support_meta(&mut meta, &row, include);
    let data = build_resolver_overview(row, numeric_chain_id, include, bound_names);

    Ok(Json(Envelope {
        data,
        page: None,
        meta,
    }))
}

pub(crate) fn build_resolver_overview(
    row: ResolverCurrentRow,
    chain_id: u64,
    include: ResolverOverviewInclude,
    bound_names: BoundNames,
) -> ResolverOverview {
    let mut counts = BTreeMap::new();
    let mut nodes = None;
    let mut aliases = None;
    let mut roles = None;
    let mut events = None;

    for (field_key, count_key, summary_key) in RESOLVER_SECTIONS {
        let section_summary = resolver_overview_summary(&row, summary_key);
        if let Some(count) = section_summary.and_then(projected_section_count) {
            counts.insert(count_key.to_owned(), count);
        }

        if include.requests(field_key) {
            let items = section_summary
                .and_then(|summary| projected_section_items(summary, field_key))
                .unwrap_or(Value::Null);
            match field_key {
                "nodes" => nodes = Some(items),
                "aliases" => aliases = Some(items),
                "roles" => roles = Some(items),
                "events" => events = Some(items),
                _ => {}
            }
        }
    }

    ResolverOverview {
        chain_id,
        address: row.resolver_address,
        counts,
        nodes,
        aliases,
        roles,
        events,
        bound_names,
    }
}

pub(crate) fn build_bound_name_record(row: &NameCurrentListRow, chain_id: u64) -> NameRecord {
    let mut record = build_name_record(&row.row, None, Some(chain_id), Status::Ok);
    let registration = name_record::name_registration_fields(Some(&row.row), &row.row.namespace);

    record.token_id = row.token_id.clone().or(record.token_id);
    record.owner = registration.owner;
    record.registrant = registration.registrant;
    record.registered_at = row
        .registration_date
        .map(format_timestamp)
        .or(registration.registered_at);
    record.created_at = row
        .created_at
        .map(format_timestamp)
        .or(registration.created_at);
    record.expires_at = row
        .expiry_date
        .map(format_timestamp)
        .or(registration.expires_at);
    record.registration_status = name_record::classify_registration_status(
        &row.row.namespace,
        name_record::declared_registration(&row.row.declared_summary),
        record.owner.as_deref(),
        has_name_binding(&row.row),
    );
    record
}

async fn load_bound_name_rows(
    pool: &sqlx::PgPool,
    filter: &NameCurrentListFilter,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
    chain_id: u64,
    resolver_address: &str,
) -> V2Result<(Vec<NameCurrentListRow>, Option<NameCurrentListCursor>)> {
    let target_len = page_size as usize;
    let scan_size = page_size.max(50);
    let mut rows = Vec::with_capacity(target_len);
    let mut page_cursor = cursor.cloned();
    let mut last_match_cursor = None;

    loop {
        let storage_page = bigname_storage::load_name_current_list_page(
            pool,
            filter,
            BOUND_NAMES_SORT,
            BOUND_NAMES_ORDER,
            page_cursor.as_ref(),
            scan_size,
            false,
        )
        .await
        .map_err(|_| {
            V2Error::internal_error(format!(
                "failed to load bound names for resolver {resolver_address} on chain {chain_id}"
            ))
        })?;
        let storage_has_more = storage_page.next_cursor.is_some();

        for row in storage_page.rows {
            let row_cursor = bound_name_cursor_from_row(&row);
            if bound_name_row_matches_chain(&row, chain_id) {
                if rows.len() == target_len {
                    return Ok((rows, last_match_cursor));
                }
                last_match_cursor = Some(row_cursor.clone());
                rows.push(row);
            }
            page_cursor = Some(row_cursor);
        }
        if !storage_has_more {
            return Ok((rows, None));
        }
    }
}

fn apply_resolver_support_meta(
    meta: &mut Meta,
    row: &ResolverCurrentRow,
    include: ResolverOverviewInclude,
) {
    let mut fields = Vec::new();
    let mut reason = None;
    let requested_count = RESOLVER_SECTIONS
        .iter()
        .filter(|(field_key, _, _)| include.requests(field_key))
        .count();

    for (field_key, _, summary_key) in RESOLVER_SECTIONS {
        if !include.requests(field_key) {
            continue;
        }
        let summary = resolver_overview_summary(row, summary_key);
        if summary.is_none_or(|summary| !summary_is_supported(summary)) {
            fields.push(field_key.to_owned());
            reason = reason.or_else(|| {
                summary
                    .and_then(|summary| summary.get("unsupported_reason"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
        }
    }

    if !fields.is_empty() {
        meta.completeness = Some(if fields.len() == requested_count {
            Completeness::Unsupported
        } else {
            Completeness::Partial
        });
        meta.unsupported_fields = Some(fields);
        meta.unsupported_reason = reason;
    }
}

fn bound_name_cursor_from_row(row: &NameCurrentListRow) -> NameCurrentListCursor {
    NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name(row.row.canonical_display_name.clone()),
        namespace: row.row.namespace.clone(),
        normalized_name: row.row.normalized_name.clone(),
        namehash: row.row.namehash.clone(),
    }
}

pub(crate) fn resolver_overview_include(include: &[String]) -> V2Result<ResolverOverviewInclude> {
    let mut parsed = ResolverOverviewInclude::empty();
    let mut saw_value = false;

    for value in include {
        saw_value = true;
        match value.as_str() {
            "nodes" => parsed.nodes = true,
            "aliases" => parsed.aliases = true,
            "roles" => parsed.roles = true,
            "events" => parsed.events = true,
            _ => {
                return Err(V2Error::invalid_input(
                    "include must contain only nodes, aliases, roles, or events",
                ));
            }
        }
    }

    Ok(if saw_value {
        parsed
    } else {
        ResolverOverviewInclude::all()
    })
}

pub(crate) fn bound_names_cursor_payload(
    cursor: &NameCurrentListCursor,
    binding: &BoundNamesCursorBinding<'_>,
) -> CursorPayload {
    CursorPayload::new(
        binding.sort,
        BTreeMap::from([
            (CHAIN_ID_FILTER_KEY.to_owned(), binding.chain_id.to_string()),
            (
                RESOLVER_FILTER_KEY.to_owned(),
                binding.resolver_address.to_owned(),
            ),
            (
                NAMESPACE_FILTER_KEY.to_owned(),
                option_filter(binding.namespace),
            ),
        ]),
        BTreeMap::from([
            (SORT_VALUE_CURSOR_KEY.to_owned(), cursor_sort_value(cursor)),
            (CURSOR_NAMESPACE_KEY.to_owned(), cursor.namespace.clone()),
            (
                NORMALIZED_NAME_CURSOR_KEY.to_owned(),
                cursor.normalized_name.clone(),
            ),
            (NAMEHASH_CURSOR_KEY.to_owned(), cursor.namehash.clone()),
        ]),
        Some(binding.snapshot_token.to_owned()),
    )
}

pub(crate) fn bound_names_storage_cursor(
    payload: &CursorPayload,
    binding: &BoundNamesCursorBinding<'_>,
) -> V2Result<NameCurrentListCursor> {
    let expected_chain_id = binding.chain_id.to_string();
    let expected_namespace = option_filter(binding.namespace);
    if payload.sort != binding.sort {
        return Err(invalid_bound_names_cursor());
    }
    if payload.snapshot.as_deref() != Some(binding.snapshot_token) {
        return Err(invalid_bound_names_cursor());
    }
    if payload.filters.len() != 3
        || payload.filters.get(CHAIN_ID_FILTER_KEY).map(String::as_str)
            != Some(expected_chain_id.as_str())
        || payload.filters.get(RESOLVER_FILTER_KEY).map(String::as_str)
            != Some(binding.resolver_address)
        || payload
            .filters
            .get(NAMESPACE_FILTER_KEY)
            .map(String::as_str)
            != Some(expected_namespace.as_str())
    {
        return Err(invalid_bound_names_cursor());
    }
    if payload.last_item.len() != 4 {
        return Err(invalid_bound_names_cursor());
    }

    Ok(NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name(cursor_nonempty_value(
            payload,
            SORT_VALUE_CURSOR_KEY,
        )?),
        namespace: cursor_nonempty_value(payload, CURSOR_NAMESPACE_KEY)?,
        normalized_name: cursor_nonempty_value(payload, NORMALIZED_NAME_CURSOR_KEY)?,
        namehash: cursor_nonempty_value(payload, NAMEHASH_CURSOR_KEY)?,
    })
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BoundNamesCursorBinding<'a> {
    pub(crate) chain_id: u64,
    pub(crate) resolver_address: &'a str,
    pub(crate) namespace: Option<&'a str>,
    pub(crate) sort: &'a str,
    pub(crate) snapshot_token: &'a str,
}

fn parse_numeric_chain_id(value: &str) -> V2Result<(u64, &'static str)> {
    let value = value.trim();
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_chain_id());
    }

    let chain_id = value.parse::<u64>().map_err(|_| invalid_chain_id())?;
    let slug = numeric_to_slug(chain_id).ok_or_else(invalid_chain_id)?;

    Ok((chain_id, slug))
}

fn invalid_chain_id() -> V2Error {
    V2Error::invalid_input("chain_id must be a supported numeric EVM chain id")
}

fn resolver_snapshot_scope(chain_id_slug: &str) -> V2Result<SnapshotSelectionScope> {
    SnapshotSelectionScope::new(
        vec![SnapshotPositionRequirement::new(
            chain_id_slug.to_owned(),
            chain_id_slug.to_owned(),
        )],
        Some(chain_id_slug.to_owned()),
    )
    .map_err(|error| V2Error::internal_error(error.message().to_owned()))
}

fn resolver_overview_summary<'a>(
    row: &'a ResolverCurrentRow,
    summary_key: &str,
) -> Option<&'a Value> {
    row.declared_summary
        .get(summary_key)
        .filter(|value| value.is_object())
}

fn projected_section_count(summary: &Value) -> Option<u64> {
    if !summary_is_supported(summary) {
        return None;
    }

    summary.get("count").and_then(Value::as_u64).or_else(|| {
        summary
            .get("items")
            .and_then(Value::as_array)
            .map(|items| items.len() as u64)
    })
}

fn projected_section_items(summary: &Value, field_key: &str) -> Option<Value> {
    if !summary_is_supported(summary) {
        return None;
    }

    summary
        .get("items")
        .and_then(Value::as_array)
        .map(|items| match field_key {
            "nodes" | "aliases" => {
                Value::Array(items.iter().map(compact_resolver_binding_item).collect())
            }
            _ => Value::Array(items.clone()),
        })
}

fn compact_resolver_binding_item(item: &Value) -> Value {
    let mut compact = Map::new();
    if let Some(logical_name_id) = item.get("logical_name_id").and_then(Value::as_str)
        && let Some((namespace, _)) = logical_name_id.split_once(':')
    {
        insert_optional_string(&mut compact, "namespace", Some(namespace.to_owned()));
    }
    insert_optional_string(
        &mut compact,
        "name",
        item_string(item, "canonical_display_name"),
    );
    insert_optional_string(
        &mut compact,
        "normalized_name",
        item_string(item, "normalized_name"),
    );
    insert_optional_string(&mut compact, "namehash", item_string(item, "namehash"));
    Value::Object(compact)
}

fn item_string(item: &Value, key: &str) -> Option<String> {
    item.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn insert_optional_string(object: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        object.insert(key.to_owned(), Value::String(value));
    }
}

fn summary_is_supported(summary: &Value) -> bool {
    summary.get("status").and_then(Value::as_str) == Some("supported")
}

fn cursor_sort_value(cursor: &NameCurrentListCursor) -> String {
    match &cursor.sort_value {
        NameCurrentListCursorValue::Name(value) => value.clone(),
        NameCurrentListCursorValue::Timestamp(_) => String::new(),
    }
}

fn cursor_nonempty_value(payload: &CursorPayload, key: &str) -> V2Result<String> {
    payload
        .last_item
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(invalid_bound_names_cursor)
}

fn invalid_bound_names_cursor() -> V2Error {
    V2Error::invalid_input("cursor must be a valid pagination cursor")
}

fn option_filter(value: Option<&str>) -> String {
    value.unwrap_or(NONE_FILTER_VALUE).to_owned()
}

fn bound_name_row_matches_chain(row: &NameCurrentListRow, chain_id: u64) -> bool {
    name_record::resolver(&row.row.declared_summary)
        .is_some_and(|resolver| resolver.chain_id == chain_id)
}

fn has_name_binding(row: &bigname_storage::NameCurrentRow) -> bool {
    row.surface_binding_id.is_some() || row.resource_id.is_some() || row.binding_kind.is_some()
}
