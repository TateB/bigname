use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
};
use tracing::error;

use crate::AppState;

use super::{Envelope, Meta, Page, Status, V2Error, V2Result, encode};

mod build;
mod cursor;
mod dto;
mod head;
mod parse;

use build::{
    build_forward_detail_record, build_forward_feed_record, build_reverse_detail_record,
    build_reverse_feed_record, lookup_address_status,
};
use cursor::{LookupReverseCursorBinding, lookup_reverse_cursor_payload, reverse_identity_sort};
use dto::{LookupInput, LookupKind, LookupRecord, LookupRequest, LookupResult};
use head::{load_served_head_meta, served_head_token};
use parse::{
    LookupProfile, LookupQueryParams, ParsedAddressLookup, ParsedNameLookup,
    ensure_lookup_batch_limit, parse_address_input, parse_lookup_json_body, parse_lookup_namespace,
    parse_lookup_profile, parse_name_input,
};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReverseStorageKey {
    address: String,
    coin_type: u64,
    roles: bigname_storage::ReverseIdentityRoles,
    page_size: u64,
    cursor: Option<ReverseCursorKey>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReverseCursorKey {
    is_primary: bool,
    role_rank: i16,
    normalized_name: String,
    namespace: String,
    namehash: String,
}

pub(crate) async fn get_lookup(
    _query: LookupQueryParams,
    State(state): State<AppState>,
    body: Result<Json<LookupRequest>, JsonRejection>,
) -> V2Result<Json<Envelope<Vec<LookupResult>>>> {
    let body = parse_lookup_json_body(body)?;
    ensure_lookup_batch_limit(body.inputs.len())?;
    let profile = parse_lookup_profile(body.profile.as_deref())?;
    let namespace = parse_lookup_namespace(body.namespace.as_deref())?;
    let served_head = load_served_head_meta(&state.pool).await?;
    let served_head_token = served_head_token(&served_head)?;

    let mut name_inputs = Vec::new();
    let mut address_inputs = Vec::new();
    for (index, item) in body.inputs.iter().enumerate() {
        match item {
            LookupInput::Name(input) => {
                name_inputs.push(parse_name_input(index, input, namespace)?);
            }
            LookupInput::Address(input) => {
                if namespace.is_some() {
                    return Err(V2Error::invalid_input(
                        "namespace is not supported for address lookup inputs",
                    ));
                }
                address_inputs.push(parse_address_input(index, input, &served_head_token)?);
            }
        }
    }

    let mut results = vec![None; body.inputs.len()];
    render_name_lookup_results(&state, profile, &name_inputs, &mut results).await?;
    match profile {
        LookupProfile::Feed => {
            render_feed_lookup_results(&state, &address_inputs, &mut results).await?;
        }
        LookupProfile::Detail => {
            render_detail_lookup_results(&state, &address_inputs, &served_head_token, &mut results)
                .await?;
        }
    }

    let data = results
        .into_iter()
        .map(|result| result.expect("every parsed lookup input must render a result"))
        .collect::<Vec<_>>();

    Ok(Json(Envelope {
        data,
        page: None,
        meta: Meta {
            as_of: Some(served_head),
            ..Meta::default()
        },
    }))
}

async fn render_name_lookup_results(
    state: &AppState,
    profile: LookupProfile,
    inputs: &[ParsedNameLookup],
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    let logical_name_ids = inputs
        .iter()
        .filter_map(|input| {
            input
                .lookup
                .as_ref()
                .map(|lookup| lookup.logical_name_id.clone())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let records = load_name_records(state, profile, &logical_name_ids).await?;

    for input in inputs {
        let (status, record) = match input.lookup.as_ref() {
            None => (Status::InvalidName, None),
            Some(lookup) => match records.get(&lookup.logical_name_id) {
                Some(record) => {
                    let record = match profile {
                        LookupProfile::Feed => build_forward_feed_record(record),
                        LookupProfile::Detail => build_forward_detail_record(record),
                    };
                    (record.status, Some(record))
                }
                None => (Status::NotFound, None),
            },
        };
        results[input.index] = Some(LookupResult {
            input: input.input.clone(),
            kind: LookupKind::Name,
            status,
            normalization: input.normalization.clone(),
            record,
            records: None,
            page: None,
        });
    }

    Ok(())
}

async fn load_name_records(
    state: &AppState,
    profile: LookupProfile,
    logical_name_ids: &[String],
) -> V2Result<BTreeMap<String, bigname_storage::IdentityNameRecordRow>> {
    let records = match profile {
        LookupProfile::Feed => {
            bigname_storage::load_identity_name_feed_records_by_names(&state.pool, logical_name_ids)
                .await
        }
        LookupProfile::Detail => {
            bigname_storage::load_identity_records_by_names(&state.pool, logical_name_ids).await
        }
    }
    .map_err(|load_error| {
        error!(
            service = "api",
            input_count = logical_name_ids.len(),
            profile = ?profile,
            error = ?load_error,
            "failed to load v2 lookup name records"
        );
        V2Error::internal_error("failed to load lookup name records")
    })?;

    Ok(records
        .into_iter()
        .map(|record| (record.row.logical_name_id.clone(), record))
        .collect())
}

async fn render_feed_lookup_results(
    state: &AppState,
    inputs: &[ParsedAddressLookup],
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    let storage_inputs = inputs
        .iter()
        .map(|input| {
            (
                (input.address.clone(), input.coin_type, input.roles),
                bigname_storage::ReverseIdentityFeedInput {
                    address: input.address.clone(),
                    coin_type: input.coin_type.to_string(),
                    roles: input.roles,
                },
            )
        })
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    let groups = bigname_storage::load_reverse_identity_feed_records(&state.pool, &storage_inputs)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                input_count = inputs.len(),
                error = ?load_error,
                "failed to load v2 lookup reverse feed records"
            );
            V2Error::internal_error("failed to load lookup reverse feed records")
        })?
        .into_iter()
        .map(|group| {
            (
                (
                    group.input.address.clone(),
                    group.input.coin_type.parse::<u64>().unwrap_or_default(),
                    group.input.roles,
                ),
                group,
            )
        })
        .collect::<BTreeMap<_, _>>();

    for input in inputs {
        let group = groups.get(&(input.address.clone(), input.coin_type, input.roles));
        let records = group
            .and_then(|group| group.record.as_ref())
            .map(build_reverse_feed_record)
            .into_iter()
            .collect::<Vec<_>>();
        let status = lookup_address_status(&records);
        let total_count = Some(group.map(|group| group.total_count).unwrap_or(0));
        results[input.index] = Some(address_lookup_result(
            input,
            records,
            None,
            total_count,
            false,
            status,
        ));
    }

    Ok(())
}

async fn render_detail_lookup_results(
    state: &AppState,
    inputs: &[ParsedAddressLookup],
    served_head_token: &str,
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    let storage_inputs = deduped_reverse_storage_inputs(inputs);
    let groups = bigname_storage::load_reverse_identity_records(&state.pool, &storage_inputs)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                input_count = inputs.len(),
                error = ?load_error,
                "failed to load v2 lookup reverse detail records"
            );
            V2Error::internal_error("failed to load lookup reverse detail records")
        })?
        .into_iter()
        .map(|group| (reverse_group_key(&group), group))
        .collect::<BTreeMap<_, _>>();

    for input in inputs {
        let key = ReverseStorageKey::from(input);
        let mut entries = groups
            .get(&key)
            .map(|group| group.entries.clone())
            .unwrap_or_default();
        let total_count = groups
            .get(&key)
            .and_then(|group| group.total_count)
            .or(Some(0));
        let has_more = groups.get(&key).is_some_and(|group| group.has_more);
        entries.sort_by(reverse_identity_sort);
        let binding = LookupReverseCursorBinding {
            served_head: served_head_token,
            address: &input.address,
            coin_type: input.coin_type,
            relation: input.relation,
        };
        let next_cursor = if has_more {
            entries
                .last()
                .map(|record| encode(&lookup_reverse_cursor_payload(record, &binding)))
        } else {
            None
        };
        let records = entries
            .iter()
            .map(build_reverse_detail_record)
            .collect::<Vec<_>>();
        let status = lookup_address_status(&records);
        results[input.index] = Some(address_lookup_result(
            input,
            records,
            next_cursor,
            total_count,
            has_more,
            status,
        ));
    }

    Ok(())
}

fn address_lookup_result(
    input: &ParsedAddressLookup,
    records: Vec<LookupRecord>,
    next_cursor: Option<String>,
    total_count: Option<u64>,
    has_more: bool,
    status: Status,
) -> LookupResult {
    LookupResult {
        input: input.input.clone(),
        kind: LookupKind::Address,
        status,
        normalization: None,
        record: None,
        records: Some(records),
        page: Some(Page {
            cursor: input.page_cursor_token.clone(),
            next_cursor,
            page_size: input.page_size,
            total_count,
            has_more,
        }),
    }
}

fn deduped_reverse_storage_inputs(
    inputs: &[ParsedAddressLookup],
) -> Vec<bigname_storage::ReverseIdentityStorageInput> {
    inputs
        .iter()
        .map(ReverseStorageKey::from)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|key| bigname_storage::ReverseIdentityStorageInput {
            address: key.address,
            coin_type: key.coin_type.to_string(),
            roles: key.roles,
            page_size: key.page_size as i64,
            cursor: key.cursor.map(bigname_storage::ReverseIdentityCursor::from),
        })
        .collect()
}

fn reverse_group_key(group: &bigname_storage::ReverseIdentityGroup) -> ReverseStorageKey {
    ReverseStorageKey {
        address: group.input.address.clone(),
        coin_type: group.input.coin_type.parse::<u64>().unwrap_or_default(),
        roles: group.input.roles,
        page_size: group.input.page_size as u64,
        cursor: group.input.cursor.clone().map(ReverseCursorKey::from),
    }
}

impl From<&ParsedAddressLookup> for ReverseStorageKey {
    fn from(value: &ParsedAddressLookup) -> Self {
        Self {
            address: value.address.clone(),
            coin_type: value.coin_type,
            roles: value.roles,
            page_size: value.page_size,
            cursor: value.page_cursor.clone().map(ReverseCursorKey::from),
        }
    }
}

impl From<bigname_storage::ReverseIdentityCursor> for ReverseCursorKey {
    fn from(value: bigname_storage::ReverseIdentityCursor) -> Self {
        Self {
            is_primary: value.is_primary,
            role_rank: value.role_rank,
            normalized_name: value.normalized_name,
            namespace: value.namespace,
            namehash: value.namehash,
        }
    }
}

impl From<ReverseCursorKey> for bigname_storage::ReverseIdentityCursor {
    fn from(value: ReverseCursorKey) -> Self {
        Self {
            is_primary: value.is_primary,
            role_rank: value.role_rank,
            normalized_name: value.normalized_name,
            namespace: value.namespace,
            namehash: value.namehash,
        }
    }
}
