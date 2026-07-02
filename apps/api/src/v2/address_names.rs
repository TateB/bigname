use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    AddressNameCurrentEntry, AddressNameRelation, AddressNamesCurrentDedupe,
    AddressNamesCurrentOrder, AddressNamesCurrentSort, AddressNamesCurrentSortedCursor,
    AddressNamesCurrentSortedCursorValue, NameCurrentRow, PermissionsCurrentRow,
    PrimaryNameClaimStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::types::{
    Uuid,
    time::{OffsetDateTime, UtcOffset},
};

use crate::AppState;

use super::{
    AddressNamesDedupe, AddressNamesSort, CursorPayload, Envelope, Meta, Page, QueryParamAllowlist,
    QueryParams, RegistrationStatus, Relation, SortOrder, StrictQueryParams, V2Error, V2Result,
    api_error_to_v2, as_of_meta, decode, encode, encode_at_token,
    name_record::name_registration_fields, permission_powers_value, permission_scope_value,
    resolve_v2_snapshot, v2_exact_name_snapshot_scope,
};

const ADDRESS_NAMES_SORT_NAME: &str = "name";
const ADDRESS_NAMES_SORT_EXPIRES_AT: &str = "expires_at";
const ADDRESS_NAMES_SORT_REGISTERED_AT: &str = "registered_at";
const ADDRESS_FILTER_KEY: &str = "address";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const RELATION_FILTER_KEY: &str = "relation";
const DEDUPE_FILTER_KEY: &str = "dedupe";
const Q_FILTER_KEY: &str = "q";
const ORDER_FILTER_KEY: &str = "order";
const SORT_KIND_CURSOR_KEY: &str = "sort_kind";
const SORT_VALUE_CURSOR_KEY: &str = "sort_value";
const LOGICAL_NAME_ID_CURSOR_KEY: &str = "logical_name_id";
const RESOURCE_ID_CURSOR_KEY: &str = "resource_id";
const SORT_KIND_NAME: &str = "name";
const SORT_KIND_TIMESTAMP_NULL: &str = "timestamp_null";
const SORT_KIND_TIMESTAMP_VALUE: &str = "timestamp_value";
const NONE_FILTER_VALUE: &str = "";

pub(crate) struct AddressNamesQueryParams;

impl QueryParamAllowlist for AddressNamesQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "namespace",
        "at",
        "finality",
        "relation",
        "q",
        "sort",
        "order",
        "dedupe",
        "include",
        "cursor",
        "page_size",
    ];
}

pub(crate) type AddressNamesQuery = StrictQueryParams<AddressNamesQueryParams>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct AddressName {
    pub(crate) name: String,
    pub(crate) display_name: String,
    pub(crate) namespace: String,
    pub(crate) namehash: String,
    pub(crate) owner: Option<String>,
    pub(crate) registrant: Option<String>,
    pub(crate) registration_status: RegistrationStatus,
    pub(crate) registered_at: Option<String>,
    pub(crate) created_at: Option<String>,
    pub(crate) expires_at: Option<String>,
    pub(crate) relations: Vec<Relation>,
    pub(crate) is_primary: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) role_summary: Option<Vec<AddressNameRoleSummary>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AddressNameRoleSummary {
    pub(crate) address: String,
    pub(crate) grants: Vec<AddressNameGrant>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AddressNameGrant {
    pub(crate) grant_scope: Value,
    pub(crate) powers: Value,
}

pub(crate) async fn get_address_names(
    Path(address): Path<String>,
    params: AddressNamesQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<AddressName>>>> {
    let params = params.into_inner();
    let normalized_address =
        crate::parse_evm_address(&address, "address").map_err(api_error_to_v2)?;
    let namespace_filter = params.namespace.clone();
    let snapshot_namespace = namespace_filter.as_deref().unwrap_or("ens");
    let include_role_summary = address_names_include_role_summary(&params.include)?;
    let storage_relation = params.relation.map(relation_to_storage);
    let storage_dedupe = dedupe_to_storage(params.dedupe);
    let storage_sort = sort_to_storage(params.sort);
    let storage_order = order_to_storage(params.order);
    let normalized_q = params.q.as_deref().map(str::to_lowercase);

    let scope =
        v2_exact_name_snapshot_scope(&state, snapshot_namespace, params.at.as_ref()).await?;
    let selected_snapshot =
        resolve_v2_snapshot(&state.pool, &scope, params.at.as_ref(), params.finality).await?;
    let snapshot_token = encode_at_token(&selected_snapshot);
    let cursor_binding = AddressNamesCursorBinding {
        address: &normalized_address,
        namespace: namespace_filter.as_deref(),
        relation: params.relation,
        dedupe: params.dedupe,
        q: normalized_q.as_deref(),
        sort: params.sort,
        order: params.order,
        snapshot_token: &snapshot_token,
    };
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            address_names_storage_cursor(&payload, &cursor_binding)
        })
        .transpose()?;

    let storage_page = bigname_storage::load_address_names_current_page_sorted(
        &state.pool,
        &normalized_address,
        namespace_filter.as_deref(),
        storage_relation,
        storage_dedupe,
        normalized_q.as_deref(),
        storage_sort,
        storage_order,
        storage_cursor.as_ref(),
        params.page_size,
    )
    .await
    .map_err(|error| {
        if storage_cursor.is_some()
            && error
                .to_string()
                .contains("page cursor does not match a grouped entry")
        {
            return V2Error::invalid_input("cursor must be a valid pagination cursor");
        }
        V2Error::internal_error(format!(
            "failed to load address names for {normalized_address}"
        ))
    })?;

    let logical_name_ids = storage_page
        .entries
        .iter()
        .map(|entry| entry.logical_name_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let name_rows =
        bigname_storage::load_name_current_by_logical_name_ids(&state.pool, &logical_name_ids)
            .await
            .map_err(|_| {
                V2Error::internal_error(format!(
                    "failed to load address-name registration summaries for {normalized_address}"
                ))
            })?;
    let primary_name = bigname_storage::load_primary_name_current_snapshot(
        &state.pool,
        &normalized_address,
        snapshot_namespace,
        "60",
    )
    .await
    .map_err(|_| {
        V2Error::internal_error(format!(
            "failed to load primary name for address {normalized_address}"
        ))
    })?
    .filter(|snapshot| snapshot.row.claim_status == PrimaryNameClaimStatus::Success)
    .and_then(|snapshot| {
        snapshot
            .normalized_claim_name
            .map(|name| name.trim().to_owned())
            .filter(|name| !name.is_empty())
    });
    let permissions_by_resource = if include_role_summary {
        let resource_ids = storage_page
            .entries
            .iter()
            .map(|entry| entry.resource_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        bigname_storage::load_permissions_current_by_resource_ids(&state.pool, &resource_ids)
            .await
            .map_err(|_| {
                V2Error::internal_error(format!(
                    "failed to load address-name role summaries for {normalized_address}"
                ))
            })?
    } else {
        std::collections::BTreeMap::new()
    };

    let next_cursor = storage_page
        .next_cursor
        .as_ref()
        .map(|cursor| encode(&address_names_cursor_payload(cursor, &cursor_binding)));
    let has_more = next_cursor.is_some();
    let data = storage_page
        .entries
        .iter()
        .map(|entry| {
            let role_summary = if include_role_summary {
                Some(build_address_name_role_summary(
                    permissions_by_resource
                        .get(&entry.resource_id)
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                )?)
            } else {
                None
            };
            Ok(build_address_name(
                entry,
                name_rows.get(&entry.logical_name_id),
                primary_name.as_deref(),
                role_summary,
            ))
        })
        .collect::<V2Result<Vec<_>>>()?;
    let meta = Meta {
        as_of: Some(as_of_meta(&selected_snapshot)?),
        ..Meta::default()
    };

    Ok(Json(Envelope {
        data,
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count: None,
            has_more,
        }),
        meta,
    }))
}

pub(crate) fn build_address_name(
    entry: &AddressNameCurrentEntry,
    name_row: Option<&NameCurrentRow>,
    primary_name: Option<&str>,
    role_summary: Option<Vec<AddressNameRoleSummary>>,
) -> AddressName {
    let registration = name_registration_fields(name_row, &entry.namespace);

    AddressName {
        name: entry.normalized_name.clone(),
        display_name: entry.canonical_display_name.clone(),
        namespace: entry.namespace.clone(),
        namehash: entry.namehash.clone(),
        owner: registration.owner,
        registrant: registration.registrant,
        registration_status: registration.registration_status,
        registered_at: registration.registered_at,
        created_at: registration.created_at,
        expires_at: registration.expires_at,
        relations: entry
            .relations
            .iter()
            .copied()
            .map(relation_from_storage)
            .collect(),
        is_primary: primary_name == Some(entry.normalized_name.as_str()),
        role_summary,
    }
}

pub(crate) fn relation_to_storage(relation: Relation) -> AddressNameRelation {
    match relation {
        Relation::Owner => AddressNameRelation::TokenHolder,
        Relation::Manager => AddressNameRelation::EffectiveController,
        Relation::Registrant => AddressNameRelation::Registrant,
    }
}

pub(crate) fn relation_from_storage(relation: AddressNameRelation) -> Relation {
    match relation {
        AddressNameRelation::TokenHolder => Relation::Owner,
        AddressNameRelation::EffectiveController => Relation::Manager,
        AddressNameRelation::Registrant => Relation::Registrant,
    }
}

pub(crate) fn dedupe_to_storage(dedupe: AddressNamesDedupe) -> AddressNamesCurrentDedupe {
    match dedupe {
        AddressNamesDedupe::Name => AddressNamesCurrentDedupe::Surface,
        AddressNamesDedupe::Registration => AddressNamesCurrentDedupe::Resource,
    }
}

pub(crate) fn sort_to_storage(sort: AddressNamesSort) -> AddressNamesCurrentSort {
    match sort {
        AddressNamesSort::Name => AddressNamesCurrentSort::Name,
        AddressNamesSort::ExpiresAt => AddressNamesCurrentSort::ExpiresAt,
        AddressNamesSort::RegisteredAt => AddressNamesCurrentSort::RegisteredAt,
    }
}

pub(crate) fn order_to_storage(order: SortOrder) -> AddressNamesCurrentOrder {
    match order {
        SortOrder::Asc => AddressNamesCurrentOrder::Asc,
        SortOrder::Desc => AddressNamesCurrentOrder::Desc,
    }
}

pub(crate) fn build_address_name_role_summary(
    rows: &[PermissionsCurrentRow],
) -> V2Result<Vec<AddressNameRoleSummary>> {
    let mut subjects = BTreeMap::<String, Vec<&PermissionsCurrentRow>>::new();

    for row in rows {
        subjects.entry(row.subject.clone()).or_default().push(row);
    }

    subjects
        .into_iter()
        .map(|(address, mut rows)| {
            rows.sort_by(|left, right| left.scope.storage_key().cmp(&right.scope.storage_key()));
            Ok(AddressNameRoleSummary {
                address,
                grants: rows
                    .into_iter()
                    .map(|row| {
                        Ok(AddressNameGrant {
                            grant_scope: permission_scope_value(&row.scope)?,
                            powers: permission_powers_value(&row.effective_powers)?,
                        })
                    })
                    .collect::<V2Result<Vec<_>>>()?,
            })
        })
        .collect()
}

pub(crate) fn address_names_cursor_payload(
    cursor: &AddressNamesCurrentSortedCursor,
    binding: &AddressNamesCursorBinding<'_>,
) -> CursorPayload {
    CursorPayload::new(
        binding.sort.as_str(),
        BTreeMap::from([
            (ADDRESS_FILTER_KEY.to_owned(), binding.address.to_owned()),
            (
                NAMESPACE_FILTER_KEY.to_owned(),
                option_filter(binding.namespace),
            ),
            (
                RELATION_FILTER_KEY.to_owned(),
                binding
                    .relation
                    .map(Relation::as_str)
                    .unwrap_or(NONE_FILTER_VALUE)
                    .to_owned(),
            ),
            (
                DEDUPE_FILTER_KEY.to_owned(),
                binding.dedupe.as_str().to_owned(),
            ),
            (Q_FILTER_KEY.to_owned(), option_filter(binding.q)),
            (
                ORDER_FILTER_KEY.to_owned(),
                binding.order.as_str().to_owned(),
            ),
        ]),
        cursor_last_item(cursor),
        Some(binding.snapshot_token.to_owned()),
    )
}

pub(crate) fn address_names_storage_cursor(
    payload: &CursorPayload,
    binding: &AddressNamesCursorBinding<'_>,
) -> V2Result<AddressNamesCurrentSortedCursor> {
    if payload.sort != binding.sort.as_str() {
        return Err(invalid_address_names_cursor());
    }
    if payload.snapshot.as_deref() != Some(binding.snapshot_token) {
        return Err(invalid_address_names_cursor());
    }
    if payload.filters.len() != 6
        || payload.filters.get(ADDRESS_FILTER_KEY).map(String::as_str) != Some(binding.address)
        || payload
            .filters
            .get(NAMESPACE_FILTER_KEY)
            .map(String::as_str)
            != Some(option_filter(binding.namespace).as_str())
        || payload.filters.get(RELATION_FILTER_KEY).map(String::as_str)
            != Some(
                binding
                    .relation
                    .map(Relation::as_str)
                    .unwrap_or(NONE_FILTER_VALUE),
            )
        || payload.filters.get(DEDUPE_FILTER_KEY).map(String::as_str)
            != Some(binding.dedupe.as_str())
        || payload.filters.get(Q_FILTER_KEY).map(String::as_str)
            != Some(option_filter(binding.q).as_str())
        || payload.filters.get(ORDER_FILTER_KEY).map(String::as_str) != Some(binding.order.as_str())
    {
        return Err(invalid_address_names_cursor());
    }
    if payload.last_item.len() != 4 {
        return Err(invalid_address_names_cursor());
    }

    let sort_value = cursor_sort_value(payload, binding.sort)?;
    let logical_name_id = cursor_nonempty_value(payload, LOGICAL_NAME_ID_CURSOR_KEY)?;
    let resource_id = Uuid::parse_str(&cursor_nonempty_value(payload, RESOURCE_ID_CURSOR_KEY)?)
        .map_err(|_| invalid_address_names_cursor())?;

    Ok(AddressNamesCurrentSortedCursor {
        sort_value,
        logical_name_id,
        resource_id,
    })
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AddressNamesCursorBinding<'a> {
    pub(crate) address: &'a str,
    pub(crate) namespace: Option<&'a str>,
    pub(crate) relation: Option<Relation>,
    pub(crate) dedupe: AddressNamesDedupe,
    pub(crate) q: Option<&'a str>,
    pub(crate) sort: AddressNamesSort,
    pub(crate) order: SortOrder,
    pub(crate) snapshot_token: &'a str,
}

fn cursor_last_item(cursor: &AddressNamesCurrentSortedCursor) -> BTreeMap<String, String> {
    let (sort_kind, sort_value) = match &cursor.sort_value {
        AddressNamesCurrentSortedCursorValue::Name(value) => {
            (SORT_KIND_NAME.to_owned(), value.clone())
        }
        AddressNamesCurrentSortedCursorValue::Timestamp(None) => {
            (SORT_KIND_TIMESTAMP_NULL.to_owned(), String::new())
        }
        AddressNamesCurrentSortedCursorValue::Timestamp(Some(value)) => (
            SORT_KIND_TIMESTAMP_VALUE.to_owned(),
            format_timestamp(*value),
        ),
    };

    BTreeMap::from([
        (SORT_KIND_CURSOR_KEY.to_owned(), sort_kind),
        (SORT_VALUE_CURSOR_KEY.to_owned(), sort_value),
        (
            LOGICAL_NAME_ID_CURSOR_KEY.to_owned(),
            cursor.logical_name_id.clone(),
        ),
        (
            RESOURCE_ID_CURSOR_KEY.to_owned(),
            cursor.resource_id.to_string(),
        ),
    ])
}

fn cursor_sort_value(
    payload: &CursorPayload,
    sort: AddressNamesSort,
) -> V2Result<AddressNamesCurrentSortedCursorValue> {
    let sort_kind = cursor_nonempty_value(payload, SORT_KIND_CURSOR_KEY)?;
    let sort_value = payload
        .last_item
        .get(SORT_VALUE_CURSOR_KEY)
        .cloned()
        .ok_or_else(invalid_address_names_cursor)?;

    match (sort, sort_kind.as_str()) {
        (AddressNamesSort::Name, SORT_KIND_NAME) if !sort_value.trim().is_empty() => {
            Ok(AddressNamesCurrentSortedCursorValue::Name(sort_value))
        }
        (
            AddressNamesSort::ExpiresAt | AddressNamesSort::RegisteredAt,
            SORT_KIND_TIMESTAMP_NULL,
        ) if sort_value.is_empty() => Ok(AddressNamesCurrentSortedCursorValue::Timestamp(None)),
        (
            AddressNamesSort::ExpiresAt | AddressNamesSort::RegisteredAt,
            SORT_KIND_TIMESTAMP_VALUE,
        ) if !sort_value.trim().is_empty() => {
            let value = bigname_storage::parse_rfc3339_utc_timestamp(&sort_value)
                .map_err(|_| invalid_address_names_cursor())?;
            Ok(AddressNamesCurrentSortedCursorValue::Timestamp(Some(value)))
        }
        _ => Err(invalid_address_names_cursor()),
    }
}

fn cursor_nonempty_value(payload: &CursorPayload, key: &str) -> V2Result<String> {
    payload
        .last_item
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(invalid_address_names_cursor)
}

fn invalid_address_names_cursor() -> V2Error {
    V2Error::invalid_input("cursor must be a valid pagination cursor")
}

fn address_names_include_role_summary(include: &[String]) -> V2Result<bool> {
    let mut include_role_summary = false;
    for value in include {
        match value.as_str() {
            "role_summary" => include_role_summary = true,
            _ => {
                return Err(V2Error::invalid_input(
                    "include must contain only role_summary",
                ));
            }
        }
    }
    Ok(include_role_summary)
}

fn option_filter(value: Option<&str>) -> String {
    value.unwrap_or(NONE_FILTER_VALUE).to_owned()
}

fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

#[cfg(test)]
mod tests;
