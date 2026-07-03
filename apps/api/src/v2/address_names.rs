use std::collections::{BTreeMap, BTreeSet};

use crate::AppState;
use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    AddressNameCurrentEntry, AddressNameRelation, AddressNamesCurrentDedupe,
    AddressNamesCurrentOrder, AddressNamesCurrentSort, NameCurrentRow, PermissionsCurrentRow,
    PrimaryNameClaimStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::cursor::invalid_cursor_error;
use super::{
    AddressNamesDedupe, AddressNamesSort, Envelope, Page, QueryParamAllowlist, RegistrationStatus,
    Relation, RelationSet, SnapshotReadResource, SortOrder, StrictQueryParams, V2Error, V2Result,
    api_error_to_v2, decode, encode, encode_at_token, name_record::name_registration_fields,
    permission_powers_value, permission_scope_value, resolve_v2_snapshot_for, snapshot_meta,
    v2_exact_name_snapshot_scope,
};

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use self::cursor::{
    ADDRESS_FILTER_KEY, ORDER_FILTER_KEY, SORT_KIND_CURSOR_KEY, SORT_KIND_NAME,
    SORT_KIND_TIMESTAMP_NULL, SORT_KIND_TIMESTAMP_VALUE, SORT_VALUE_CURSOR_KEY,
};
pub(crate) use self::cursor::{
    AddressNamesCursorBinding, address_names_cursor_payload, address_names_storage_cursor,
};

mod cursor;

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registrant: Option<String>,
    pub(crate) registration_status: RegistrationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registered_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at: Option<String>,
    pub(crate) relations: Vec<Relation>,
    pub(crate) is_primary: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) record_count: Option<u64>,
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
    let storage_relations = params
        .relation
        .as_ref()
        .map(relation_set_to_storage)
        .unwrap_or_default();
    let storage_relations = (!storage_relations.is_empty()).then_some(storage_relations.as_slice());
    let storage_dedupe = dedupe_to_storage(params.dedupe);
    let storage_sort = sort_to_storage(params.sort);
    let storage_order = order_to_storage(params.order);
    let normalized_q = params.q.as_deref().map(str::to_lowercase);

    let scope =
        v2_exact_name_snapshot_scope(&state, snapshot_namespace, params.at.as_ref()).await?;
    let selected_snapshot = resolve_v2_snapshot_for(
        &state.pool,
        &scope,
        params.at.as_ref(),
        params.finality,
        SnapshotReadResource::AddressNames,
    )
    .await?;
    let snapshot_token = encode_at_token(&selected_snapshot);
    let cursor_binding = AddressNamesCursorBinding {
        address: &normalized_address,
        namespace: namespace_filter.as_deref(),
        relation: params.relation.as_ref(),
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

    let storage_page = bigname_storage::load_address_names_current_page_sorted_for_relations(
        &state.pool,
        &normalized_address,
        namespace_filter.as_deref(),
        storage_relations,
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
            return invalid_cursor_error();
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
    let primary_names_by_namespace = load_primary_names_by_namespace(
        &state.pool,
        &normalized_address,
        storage_page
            .entries
            .iter()
            .map(|entry| entry.namespace.as_str()),
    )
    .await?;
    let role_resource_ids = include_role_summary.then(|| {
        storage_page
            .entries
            .iter()
            .map(|entry| entry.resource_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    });
    let permissions_by_resource = if let Some(resource_ids) = role_resource_ids.as_deref() {
        bigname_storage::load_permissions_current_by_resource_ids(&state.pool, resource_ids)
            .await
            .map_err(|_| {
                V2Error::internal_error(format!(
                    "failed to load address-name role summaries for {normalized_address}"
                ))
            })?
    } else {
        std::collections::BTreeMap::new()
    };
    let record_counts_by_name = if include_role_summary {
        load_address_name_record_counts(&state.pool, &storage_page.entries, &name_rows)
            .await
            .map_err(|_| {
                V2Error::internal_error(format!(
                    "failed to load address-name record counts for {normalized_address}"
                ))
            })?
    } else {
        BTreeMap::new()
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
                primary_names_by_namespace
                    .get(&entry.namespace)
                    .and_then(Option::as_deref),
                record_counts_by_name.get(&entry.logical_name_id).copied(),
                role_summary,
            ))
        })
        .collect::<V2Result<Vec<_>>>()?;
    let meta = snapshot_meta(&selected_snapshot)?;

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

async fn load_primary_names_by_namespace<'a>(
    pool: &sqlx::PgPool,
    address: &str,
    namespaces: impl Iterator<Item = &'a str>,
) -> V2Result<BTreeMap<String, Option<String>>> {
    let namespaces = namespaces.collect::<BTreeSet<_>>();
    let mut primary_names = BTreeMap::new();
    for namespace in namespaces {
        let primary_name =
            bigname_storage::load_primary_name_current_snapshot(pool, address, namespace, "60")
                .await
                .map_err(|_| {
                    V2Error::internal_error(format!(
                        "failed to load primary name for address {address}"
                    ))
                })?
                .filter(|snapshot| snapshot.row.claim_status == PrimaryNameClaimStatus::Success)
                .and_then(|snapshot| {
                    snapshot
                        .normalized_claim_name
                        .map(|name| name.trim().to_owned())
                        .filter(|name| !name.is_empty())
                });
        primary_names.insert(namespace.to_owned(), primary_name);
    }
    Ok(primary_names)
}

async fn load_address_name_record_counts(
    pool: &sqlx::PgPool,
    entries: &[AddressNameCurrentEntry],
    name_rows: &BTreeMap<String, NameCurrentRow>,
) -> anyhow::Result<BTreeMap<String, u64>> {
    let mut logical_name_ids = Vec::new();
    let mut keys = Vec::new();
    for entry in entries {
        let Some(name_row) = name_rows.get(&entry.logical_name_id) else {
            continue;
        };
        let Some((resource_id, boundary)) =
            bigname_storage::resolution_record_inventory_lookup_key_any_chain(name_row)
        else {
            continue;
        };
        logical_name_ids.push(entry.logical_name_id.clone());
        keys.push((resource_id, boundary));
    }

    let counts =
        bigname_storage::count_record_inventory_selectors_by_lookup_keys(pool, &keys).await?;
    Ok(logical_name_ids
        .into_iter()
        .zip(counts)
        .filter_map(|(logical_name_id, count)| count.map(|count| (logical_name_id, count)))
        .collect())
}

pub(crate) fn build_address_name(
    entry: &AddressNameCurrentEntry,
    name_row: Option<&NameCurrentRow>,
    primary_name: Option<&str>,
    record_count: Option<u64>,
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
        record_count,
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

pub(crate) fn relation_set_to_storage(relation_set: &RelationSet) -> Vec<AddressNameRelation> {
    relation_set
        .as_slice()
        .iter()
        .copied()
        .map(relation_to_storage)
        .collect()
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

#[cfg(test)]
mod tests;
