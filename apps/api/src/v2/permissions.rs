use std::collections::BTreeMap;

use axum::{Json, extract::State};
use bigname_storage::{PermissionsCurrentAccountResourceCursor, PermissionsCurrentRow};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{AppState, normalize_inferred_route_name};

use super::{
    AddressNameGrant, CursorPayload, Envelope, Meta, Page, QueryParamAllowlist, QueryParams,
    StrictQueryParams, V2Error, V2Result, as_of_meta, decode, encode, encode_at_token,
    permission_powers_value, permission_scope_value, resolve_v2_snapshot,
    v2_exact_name_snapshot_scope,
};

#[path = "permissions/lineage.rs"]
mod lineage;
use lineage::permission_lineage;
#[path = "permissions/snapshot.rs"]
mod snapshot;
use snapshot::load_name_row_for_snapshot;

const PERMISSIONS_SORT: &str = "address_registration_scope_asc";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const ADDRESS_FILTER_KEY: &str = "address";
const REGISTRATION_ID_FILTER_KEY: &str = "registration_id";
const INCLUDE_FILTER_KEY: &str = "include";
const SUBJECT_CURSOR_KEY: &str = "subject";
const RESOURCE_ID_CURSOR_KEY: &str = "resource_id";
const SCOPE_CURSOR_KEY: &str = "scope";

pub(crate) struct PermissionsQueryParams;

impl QueryParamAllowlist for PermissionsQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "name",
        "registration_id",
        "address",
        "namespace",
        "at",
        "finality",
        "include",
        "cursor",
        "page_size",
    ];
}

pub(crate) type PermissionsQuery = StrictQueryParams<PermissionsQueryParams>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct PermissionRow {
    pub(crate) address: String,
    #[serde(flatten)]
    pub(crate) grant: AddressNameGrant,
    pub(crate) registration_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) lineage: Option<PermissionLineage>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct PermissionLineage {
    pub(crate) grant: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) revocation: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) inheritance_path: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) transfer_behavior: Option<Value>,
}

#[derive(Debug)]
struct ResolvedPermissionsFilter {
    subject: Option<String>,
    resource_id: Option<Uuid>,
    known_empty: bool,
    cursor_filters: BTreeMap<String, String>,
}

#[derive(Debug)]
struct NormalizedNameFilter {
    namespace: String,
    normalized_name: String,
}

#[derive(Debug)]
struct PermissionsFilterInputs {
    namespace: String,
    name_filter: Option<NormalizedNameFilter>,
    requested_resource_id: Option<Uuid>,
}

pub(crate) async fn get_permissions(
    params: PermissionsQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<PermissionRow>>>> {
    let params = params.into_inner();
    let include_lineage = permissions_include_lineage(&params.include)?;
    let filter_inputs = permissions_filter_inputs(&params)?;

    let scope =
        v2_exact_name_snapshot_scope(&state, &filter_inputs.namespace, params.at.as_ref()).await?;
    let selected_snapshot =
        resolve_v2_snapshot(&state.pool, &scope, params.at.as_ref(), params.finality).await?;
    let resolved = resolve_permissions_filter(
        &state,
        &params,
        include_lineage,
        &filter_inputs,
        &selected_snapshot,
    )
    .await?;
    let snapshot_token = encode_at_token(&selected_snapshot);
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            permissions_storage_cursor(&payload, &resolved.cursor_filters, &snapshot_token)
        })
        .transpose()?;

    if resolved.known_empty {
        return empty_permissions_response(&params, &selected_snapshot);
    }

    let storage_page = bigname_storage::load_permissions_current_account_resource_page(
        &state.pool,
        resolved.subject.as_deref(),
        resolved.resource_id,
        storage_cursor.as_ref(),
        params.page_size,
    )
    .await
    .map_err(|_| V2Error::internal_error("failed to load permissions"))?;

    let resource_ids = storage_page
        .rows
        .iter()
        .map(|row| row.resource_id)
        .collect::<Vec<_>>();
    let current_names =
        bigname_storage::load_current_names_by_resource_ids(&state.pool, &resource_ids)
            .await
            .map_err(|_| V2Error::internal_error("failed to load permission names"))?;
    let next_cursor = storage_page.next_cursor.as_ref().map(|cursor| {
        encode(&permissions_cursor_payload(
            cursor,
            &resolved.cursor_filters,
            &snapshot_token,
        ))
    });
    let has_more = next_cursor.is_some();
    let data = storage_page
        .rows
        .iter()
        .map(|row| build_permission_row(row, current_names.get(&row.resource_id), include_lineage))
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

fn empty_permissions_response(
    params: &QueryParams,
    selected_snapshot: &bigname_storage::SelectedSnapshot,
) -> V2Result<Json<Envelope<Vec<PermissionRow>>>> {
    let meta = Meta {
        as_of: Some(as_of_meta(selected_snapshot)?),
        ..Meta::default()
    };

    Ok(Json(Envelope {
        data: Vec::new(),
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor: None,
            page_size: params.page_size,
            total_count: None,
            has_more: false,
        }),
        meta,
    }))
}

fn permissions_filter_inputs(params: &QueryParams) -> V2Result<PermissionsFilterInputs> {
    let name_filter = normalized_name_filter(params)?;
    if name_filter.is_none() && params.registration_id.is_none() && params.address.is_none() {
        return Err(V2Error::invalid_input(
            "at least one of name, registration_id, or address is required",
        ));
    }

    let requested_resource_id = params
        .registration_id
        .as_deref()
        .map(|registration_id| {
            Uuid::parse_str(registration_id)
                .map_err(|_| V2Error::invalid_input("registration_id must be a UUID"))
        })
        .transpose()?;

    let namespace = name_filter
        .as_ref()
        .map(|name_filter| name_filter.namespace.clone())
        .or_else(|| params.namespace.clone())
        .unwrap_or_else(|| "ens".to_owned());

    Ok(PermissionsFilterInputs {
        namespace,
        name_filter,
        requested_resource_id,
    })
}

async fn resolve_permissions_filter(
    state: &AppState,
    params: &QueryParams,
    include_lineage: bool,
    inputs: &PermissionsFilterInputs,
    selected_snapshot: &bigname_storage::SelectedSnapshot,
) -> V2Result<ResolvedPermissionsFilter> {
    let resolved_name_row = match inputs.name_filter.as_ref() {
        Some(name_filter) => Some(
            load_name_row_for_snapshot(
                state,
                &name_filter.namespace,
                &name_filter.normalized_name,
                selected_snapshot,
            )
            .await?,
        ),
        None => None,
    };
    let name_resource_id = resolved_name_row
        .as_ref()
        .and_then(|row| row.as_ref())
        .and_then(|row| row.resource_id);

    if let (Some(requested), Some(resolved)) = (inputs.requested_resource_id, name_resource_id)
        && requested != resolved
    {
        return Err(V2Error::unsupported("conflicting registration filters"));
    }

    let namespace = inputs.namespace.clone();
    let resource_id = inputs.requested_resource_id.or(name_resource_id);
    let known_empty = inputs.name_filter.is_some() && name_resource_id.is_none();
    let mut cursor_filters = BTreeMap::new();
    if params.namespace.is_some() || inputs.name_filter.is_some() {
        cursor_filters.insert(NAMESPACE_FILTER_KEY.to_owned(), namespace.clone());
    }
    if let Some(address) = params.address.as_ref() {
        cursor_filters.insert(ADDRESS_FILTER_KEY.to_owned(), address.clone());
    }
    if let Some(resource_id) = resource_id {
        cursor_filters.insert(
            REGISTRATION_ID_FILTER_KEY.to_owned(),
            resource_id.to_string(),
        );
    }
    if include_lineage {
        cursor_filters.insert(INCLUDE_FILTER_KEY.to_owned(), "lineage".to_owned());
    }

    Ok(ResolvedPermissionsFilter {
        subject: params.address.clone(),
        resource_id,
        known_empty,
        cursor_filters,
    })
}

fn normalized_name_filter(params: &QueryParams) -> V2Result<Option<NormalizedNameFilter>> {
    let Some(name) = params.name.as_deref() else {
        return Ok(None);
    };
    let normalized = normalize_inferred_route_name(name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());

    Ok(Some(NormalizedNameFilter {
        namespace,
        normalized_name: normalized.normalized_name.to_owned(),
    }))
}

pub(crate) fn build_permission_row(
    row: &PermissionsCurrentRow,
    name: Option<&String>,
    include_lineage: bool,
) -> V2Result<PermissionRow> {
    Ok(PermissionRow {
        address: row.subject.clone(),
        grant: AddressNameGrant {
            grant_scope: permission_scope_value(&row.scope)?,
            powers: permission_powers_value(&row.effective_powers)?,
        },
        registration_id: row.resource_id.to_string(),
        name: name.cloned(),
        lineage: include_lineage
            .then(|| permission_lineage(row))
            .transpose()?,
    })
}

fn permissions_cursor_payload(
    cursor: &PermissionsCurrentAccountResourceCursor,
    filters: &BTreeMap<String, String>,
    snapshot_token: &str,
) -> CursorPayload {
    CursorPayload::new(
        PERMISSIONS_SORT,
        filters.clone(),
        BTreeMap::from([
            (SUBJECT_CURSOR_KEY.to_owned(), cursor.subject.clone()),
            (
                RESOURCE_ID_CURSOR_KEY.to_owned(),
                cursor.resource_id.to_string(),
            ),
            (SCOPE_CURSOR_KEY.to_owned(), cursor.scope.clone()),
        ]),
        Some(snapshot_token.to_owned()),
    )
}

fn permissions_storage_cursor(
    payload: &CursorPayload,
    expected_filters: &BTreeMap<String, String>,
    snapshot_token: &str,
) -> V2Result<PermissionsCurrentAccountResourceCursor> {
    if payload.sort != PERMISSIONS_SORT {
        return Err(invalid_permissions_cursor());
    }
    if payload.snapshot.as_deref() != Some(snapshot_token) {
        return Err(invalid_permissions_cursor());
    }
    if &payload.filters != expected_filters {
        return Err(invalid_permissions_cursor());
    }
    if payload.last_item.len() != 3 {
        return Err(invalid_permissions_cursor());
    }

    let resource_id = cursor_value(payload, RESOURCE_ID_CURSOR_KEY)?
        .parse::<Uuid>()
        .map_err(|_| invalid_permissions_cursor())?;

    Ok(PermissionsCurrentAccountResourceCursor {
        subject: cursor_value(payload, SUBJECT_CURSOR_KEY)?,
        resource_id,
        scope: cursor_value(payload, SCOPE_CURSOR_KEY)?,
    })
}

fn cursor_value(payload: &CursorPayload, key: &str) -> V2Result<String> {
    payload
        .last_item
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(invalid_permissions_cursor)
}

fn invalid_permissions_cursor() -> V2Error {
    V2Error::invalid_input("cursor must be a valid pagination cursor")
}

fn permissions_include_lineage(include: &[String]) -> V2Result<bool> {
    let mut include_lineage = false;
    for value in include {
        match value.as_str() {
            "lineage" => include_lineage = true,
            _ => return Err(V2Error::invalid_input("include must contain only lineage")),
        }
    }
    Ok(include_lineage)
}

#[cfg(test)]
mod tests {
    use bigname_storage::{PermissionScope, PermissionsCurrentAccountResourceCursor};
    use serde_json::json;
    use sqlx::types::time::OffsetDateTime;

    use super::*;

    const ADDRESS: &str = "0x00000000000000000000000000000000000000aa";
    const REGISTRATION_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn sample_storage_cursor() -> PermissionsCurrentAccountResourceCursor {
        PermissionsCurrentAccountResourceCursor {
            subject: ADDRESS.to_owned(),
            resource_id: Uuid::parse_str(REGISTRATION_ID).expect("uuid literal must parse"),
            scope: "resource".to_owned(),
        }
    }

    fn sample_filters() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("address".to_owned(), ADDRESS.to_owned()),
            ("include".to_owned(), "lineage".to_owned()),
            ("registration_id".to_owned(), REGISTRATION_ID.to_owned()),
        ])
    }

    fn sample_permissions_row(
        inheritance_path: Value,
        transfer_behavior: Value,
    ) -> PermissionsCurrentRow {
        PermissionsCurrentRow {
            resource_id: Uuid::parse_str(REGISTRATION_ID).expect("uuid literal must parse"),
            subject: ADDRESS.to_owned(),
            scope: PermissionScope::Resolver {
                chain_id: "ethereum-mainnet".to_owned(),
                resolver_address: "0x0000000000000000000000000000000000000ABC".to_owned(),
            },
            effective_powers: json!(["set_resolver"]),
            grant_source: json!({
                "kind": "raw_log",
                "source_event": "EACRolesChanged",
                "upstream_resource": "root",
                "root_resource": true,
                "changed_powers": ["set_resolver"],
                "resolver_contract_instance_id": "00000000-0000-0000-0000-000000000010"
            }),
            revocation_source: Some(json!({
                "kind": "raw_log",
                "source_event": "EACRolesChanged",
                "upstream_resource": "root",
                "root_resource": true,
                "changed_powers": ["set_resolver"],
                "resolver_contract_instance_id": "00000000-0000-0000-0000-000000000011"
            })),
            inheritance_path,
            transfer_behavior,
            provenance: json!({}),
            coverage: json!({}),
            chain_positions: json!({}),
            canonicality_summary: json!({}),
            manifest_version: 1,
            last_recomputed_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn permissions_cursor_payload_round_trips_storage_cursor() {
        let cursor = sample_storage_cursor();
        let filters = sample_filters();
        let payload = permissions_cursor_payload(&cursor, &filters, "snapshot-1");

        assert_eq!(payload.filters, filters);
        assert_eq!(
            permissions_storage_cursor(&payload, &sample_filters(), "snapshot-1")
                .expect("cursor must decode"),
            cursor
        );
    }

    #[test]
    fn permissions_cursor_rejects_wrong_sort_snapshot_or_filters() {
        let cursor = sample_storage_cursor();
        let filters = sample_filters();

        let mut payload = permissions_cursor_payload(&cursor, &filters, "snapshot-1");
        payload.sort = "name".to_owned();
        assert!(permissions_storage_cursor(&payload, &filters, "snapshot-1").is_err());

        let mut payload = permissions_cursor_payload(&cursor, &filters, "snapshot-1");
        payload.snapshot = Some("snapshot-2".to_owned());
        assert!(permissions_storage_cursor(&payload, &filters, "snapshot-1").is_err());

        let mut payload = permissions_cursor_payload(&cursor, &filters, "snapshot-1");
        payload
            .filters
            .insert("namespace".to_owned(), "ens".to_owned());
        assert!(permissions_storage_cursor(&payload, &filters, "snapshot-1").is_err());

        let mut payload = permissions_cursor_payload(&cursor, &filters, "snapshot-1");
        payload.filters.remove("address");
        assert!(permissions_storage_cursor(&payload, &filters, "snapshot-1").is_err());
    }

    #[test]
    fn build_permission_row_maps_scope_powers_name_and_lineage() {
        let row = sample_permissions_row(
            json!([{
                "kind": "resolver_root_fallback",
                "chain_id": "ethereum-mainnet",
                "resolver_address": "0x0000000000000000000000000000000000000ABC",
                "upstream_resource": "root"
            }]),
            Value::Null,
        );
        let name = "alice.eth".to_owned();
        let mapped =
            build_permission_row(&row, Some(&name), true).expect("known storage chain id must map");

        assert_eq!(mapped.address, ADDRESS);
        assert_eq!(mapped.registration_id, REGISTRATION_ID);
        assert_eq!(mapped.name, Some("alice.eth".to_owned()));
        assert_eq!(mapped.grant.powers, json!(["set_resolver"]));
        assert_eq!(
            mapped.grant.grant_scope,
            json!({
                "kind": "resolver",
                "detail": {
                    "resolver": {
                        "chain_id": 1,
                        "address": "0x0000000000000000000000000000000000000abc"
                    }
                }
            })
        );
        assert_eq!(
            mapped.lineage,
            Some(PermissionLineage {
                grant: json!({"kind": "event"}),
                revocation: Some(json!({
                    "kind": "event"
                })),
                inheritance_path: Some(json!([{
                    "kind": "resolver_root_fallback",
                    "resolver": {
                        "chain_id": 1,
                        "address": "0x0000000000000000000000000000000000000abc"
                    }
                }])),
                transfer_behavior: None,
            })
        );
    }

    #[test]
    fn lineage_omits_absent_optional_members() {
        let mut row = sample_permissions_row(json!([]), Value::Null);
        row.revocation_source = None;
        let mapped =
            build_permission_row(&row, None, true).expect("known storage chain id must map");
        let lineage = mapped.lineage.expect("lineage must be present");

        assert_eq!(mapped.name, None);
        assert_eq!(lineage.grant, json!({"kind": "event"}));
        assert_eq!(lineage.revocation, None);
        assert_eq!(lineage.inheritance_path, None);
        assert_eq!(lineage.transfer_behavior, None);
    }
}
